//! Real socket pairs, no VM. Every assertion is about bytes that actually
//! crossed between two guest ends, so a passing test means the fabric forwards
//! rather than that a decision function returned the right enum.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::net::UnixDatagram as StdUnixDatagram;
use std::time::Duration;

use super::*;
use crate::environment_switch::DropReason;

const IPV4: u16 = 0x0800;

fn mac(last: u8) -> MacAddress {
    MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, last])
}

fn frame(destination: MacAddress, source: MacAddress, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&destination.bytes());
    bytes.extend_from_slice(&source.bytes());
    bytes.extend_from_slice(&IPV4.to_be_bytes());
    bytes.extend_from_slice(body);
    bytes
}

/// A guest end as a Machine would use it: blocking, with a read deadline so a
/// frame that never arrives fails the test instead of hanging it.
///
/// The descriptor arrives non-blocking, because tokio sets that on both ends of
/// the pair it created. A read timeout means nothing on a non-blocking socket,
/// so clearing the flag is what makes these tests actually wait for delivery
/// rather than observe an immediate EAGAIN and call it silence.
fn guest(socket: OwnedFd) -> StdUnixDatagram {
    let socket = StdUnixDatagram::from(socket);
    socket.set_nonblocking(false).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    socket
}

fn recv(socket: &StdUnixDatagram) -> Option<Vec<u8>> {
    let mut buffer = vec![0_u8; 2048];
    match socket.recv(&mut buffer) {
        Ok(read) => Some(buffer[..read].to_vec()),
        Err(_) => None,
    }
}

/// Nothing arrived within a short window. Used where the point is that a frame
/// was NOT delivered, so the wait is deliberately short and the failure is a
/// received frame rather than a timeout.
fn expect_silence(socket: &StdUnixDatagram) {
    socket
        .set_read_timeout(Some(Duration::from_millis(250)))
        .unwrap();
    let mut buffer = vec![0_u8; 2048];
    if let Ok(read) = socket.recv(&mut buffer) {
        panic!("expected no delivery, received {read} bytes");
    }
}

fn started(count: u8) -> (NetworkSwitch, Vec<StdUnixDatagram>) {
    let members: Vec<(PortId, MacAddress)> = (1..=count)
        .map(|index| (PortId(u32::from(index)), mac(index)))
        .collect();
    let (switch, guests) = NetworkSwitch::start("declared", members).unwrap();
    let sockets = guests.into_iter().map(|port| guest(port.socket)).collect();
    (switch, sockets)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frame_crosses_from_one_machine_to_the_one_it_addresses() {
    let (mut switch, guests) = started(3);
    guests[0].send(&frame(mac(3), mac(1), b"payload")).unwrap();
    assert_eq!(
        recv(&guests[2]).as_deref(),
        Some(frame(mac(3), mac(1), b"payload").as_slice()),
        "the addressed Machine receives the frame unchanged"
    );
    // The Machine that was not addressed receives nothing.
    expect_silence(&guests[1]);
    let receipt = switch.shutdown().await.unwrap();
    assert_eq!(receipt.frames_read, 1);
    assert_eq!(receipt.frames_delivered, 1);
    assert_eq!(receipt.undeliverable, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spoofed_source_never_reaches_the_wire() {
    // The anti-spoof rule is only worth anything if it holds on real sockets:
    // this is the frame a guest would send to claim a sibling's address.
    let (mut switch, guests) = started(3);
    guests[0].send(&frame(mac(3), mac(2), b"stolen")).unwrap();
    expect_silence(&guests[2]);
    expect_silence(&guests[1]);
    let receipt = switch.shutdown().await.unwrap();
    assert_eq!(receipt.frames_read, 1);
    assert_eq!(receipt.frames_delivered, 0);
    assert_eq!(
        receipt
            .counters
            .dropped
            .get(&DropReason::SourceAddressNotAssigned),
        Some(&1)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_broadcast_reaches_every_other_machine_and_not_the_sender() {
    let (mut switch, guests) = started(3);
    let sent = frame(MacAddress::BROADCAST, mac(2), b"who-has");
    guests[1].send(&sent).unwrap();
    assert_eq!(recv(&guests[0]).as_deref(), Some(sent.as_slice()));
    assert_eq!(recv(&guests[2]).as_deref(), Some(sent.as_slice()));
    expect_silence(&guests[1]);
    let receipt = switch.shutdown().await.unwrap();
    assert_eq!(receipt.frames_delivered, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn two_networks_with_identical_addressing_deliver_nothing_to_each_other() {
    // Sibling Environments must be unable to reach one another even with
    // identical addressing. Each switch owns its own sockets, so a frame in one
    // has no path into the other; this asserts that on real descriptors.
    let (mut left, left_guests) = started(3);
    let (mut right, right_guests) = started(3);
    left_guests[0]
        .send(&frame(mac(3), mac(1), b"left"))
        .unwrap();
    assert_eq!(
        recv(&left_guests[2]).as_deref(),
        Some(frame(mac(3), mac(1), b"left").as_slice())
    );
    for socket in &right_guests {
        expect_silence(socket);
    }
    let right_receipt = right.shutdown().await.unwrap();
    assert_eq!(
        right_receipt.frames_read, 0,
        "the sibling network saw nothing"
    );
    let left_receipt = left.shutdown().await.unwrap();
    assert_eq!(left_receipt.frames_delivered, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_port_and_an_address_are_each_refused_twice_before_any_socket_exists() {
    // Refusing before creating descriptors is why the fabric is built first.
    let duplicate_port =
        NetworkSwitch::start("declared", [(PortId(1), mac(1)), (PortId(1), mac(2))]);
    assert!(matches!(
        duplicate_port,
        Err(SwitchError::Fabric(FabricError::PortAlreadyAttached(1)))
    ));
    let duplicate_address =
        NetworkSwitch::start("declared", [(PortId(1), mac(1)), (PortId(2), mac(1))]);
    assert!(matches!(
        duplicate_address,
        Err(SwitchError::Fabric(FabricError::AddressAlreadyAssigned(
            _,
            1
        )))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_reports_once_and_refuses_a_second_call() {
    let (mut switch, guests) = started(2);
    assert_eq!(switch.ports().len(), 2);
    guests[0].send(&frame(mac(2), mac(1), b"x")).unwrap();
    assert!(recv(&guests[1]).is_some());
    assert_eq!(switch.shutdown().await.unwrap().frames_delivered, 1);
    assert!(matches!(
        switch.shutdown().await,
        Err(SwitchError::AlreadyStopped)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_network_starts_stops_and_forwards_nothing() {
    let (mut switch, guests) = started(0);
    assert!(guests.is_empty());
    assert_eq!(switch.shutdown().await.unwrap(), SwitchShutdown::default());
}

#[tokio::test(flavor = "multi_thread")]
async fn guest_ends_carry_the_addresses_the_switch_assigned() {
    let members = [
        (PortId(7), MacAddress::derive("env", "machine-a", "net")),
        (PortId(9), MacAddress::derive("env", "machine-b", "net")),
    ];
    let (mut switch, ports) = NetworkSwitch::start("declared", members).unwrap();
    assert_eq!(ports.len(), 2);
    for (port, expected) in ports.iter().zip(members.iter()) {
        assert_eq!((port.port, port.address), *expected);
        // The descriptor handed out is real and usable.
        assert!(port.socket.as_raw_fd() >= 0);
    }
    assert_eq!(switch.ports().get(&PortId(7)), Some(&members[0].1));
    switch.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frame_a_machine_sends_after_shutdown_is_not_forwarded() {
    let (mut switch, guests) = started(2);
    let receipt = switch.shutdown().await.unwrap();
    assert_eq!(receipt.frames_read, 0);
    // The host end is gone with the task, so the send either fails outright or
    // is never forwarded. Either is correct; delivery is not.
    let _ = guests[0].send(&frame(mac(2), mac(1), b"late"));
    expect_silence(&guests[1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frame_larger_than_a_machine_will_read_is_still_one_datagram() {
    // Datagram framing is what makes the forwarding decision total: a read
    // yields one whole frame or nothing, never a split of two.
    let (mut switch, guests) = started(2);
    let body = vec![0x5a_u8; 1400];
    let sent = frame(mac(2), mac(1), &body);
    guests[0].send(&sent).unwrap();
    let received = recv(&guests[1]).unwrap();
    assert_eq!(received.len(), sent.len());
    assert_eq!(received, sent);
    switch.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_machine_whose_guest_end_is_closed_becomes_a_dead_port_and_stalls_nothing() {
    // A port whose guest end is gone is the shape a caller produces by minting
    // a port and never attaching it to a VM. It must degrade to a counted drop:
    // one Machine that cannot receive may not stall the frames between the
    // others, and it may not take the switch down.
    let (mut switch, mut guests) = started(3);
    let closed = guests.remove(2);
    drop(closed);

    guests[0]
        .send(&frame(mac(3), mac(1), b"to-the-dead"))
        .unwrap();
    // The live pair still carries traffic after the failed delivery.
    guests[0]
        .send(&frame(mac(2), mac(1), b"to-the-living"))
        .unwrap();
    assert_eq!(
        recv(&guests[1]).as_deref(),
        Some(frame(mac(2), mac(1), b"to-the-living").as_slice()),
        "a dead port must not stall the frames between the Machines that remain"
    );

    let receipt = switch.shutdown().await.unwrap();
    assert_eq!(receipt.frames_read, 2);
    assert_eq!(receipt.frames_delivered, 1);
    assert_eq!(
        receipt.undeliverable, 1,
        "the frame for the closed Machine is counted, not retried and not lost silently"
    );
    // The fabric still resolved it: the address is assigned, so this is a
    // delivery failure, not a forwarding decision.
    assert_eq!(receipt.counters.unicast_forwarded, 2);
    assert!(receipt.counters.dropped.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_port_added_to_a_running_fabric_carries_frames_both_ways() {
    // What a fork needs: its parent is already forwarding and must not be
    // disturbed, and the new Machine has to be reachable in both directions the
    // moment it is attached.
    let (mut switch, guests) = started(2);
    guests[0].send(&frame(mac(2), mac(1), b"before")).unwrap();
    assert_eq!(
        recv(&guests[1]).as_deref(),
        Some(frame(mac(2), mac(1), b"before").as_slice())
    );

    let added = switch.add_port(PortId(3), mac(3)).await.unwrap();
    assert_eq!(added.port, PortId(3));
    assert_eq!(added.address, mac(3));
    assert_eq!(switch.ports().len(), 3);
    let joined = guest(added.socket);

    // Reaching the newcomer.
    guests[0].send(&frame(mac(3), mac(1), b"to-fork")).unwrap();
    assert_eq!(
        recv(&joined).as_deref(),
        Some(frame(mac(3), mac(1), b"to-fork").as_slice()),
        "a port attached to a running switch receives"
    );
    // And the newcomer reaching a Machine that was already there.
    joined.send(&frame(mac(1), mac(3), b"from-fork")).unwrap();
    assert_eq!(
        recv(&guests[0]).as_deref(),
        Some(frame(mac(1), mac(3), b"from-fork").as_slice()),
        "a port attached to a running switch also sends"
    );
    // The Machine that was already running kept working throughout, which is
    // the whole reason the fabric is not rebuilt to add a port.
    guests[0].send(&frame(mac(2), mac(1), b"after")).unwrap();
    assert_eq!(
        recv(&guests[1]).as_deref(),
        Some(frame(mac(2), mac(1), b"after").as_slice())
    );

    let receipt = switch.shutdown().await.unwrap();
    assert_eq!(receipt.frames_read, 4);
    assert_eq!(receipt.frames_delivered, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_port_the_running_fabric_refuses_leaves_it_exactly_as_it_was() {
    // The fabric's rules do not weaken because it is already forwarding: a
    // duplicate port or a repeated address is refused, the caller is told which,
    // and the switch keeps carrying frames for everyone already on it.
    let (mut switch, guests) = started(2);

    let duplicate_port = switch.add_port(PortId(1), mac(9)).await;
    assert!(
        matches!(
            duplicate_port,
            Err(SwitchError::Fabric(FabricError::PortAlreadyAttached(1)))
        ),
        "{duplicate_port:?}"
    );
    let duplicate_address = switch.add_port(PortId(4), mac(1)).await;
    assert!(
        matches!(
            duplicate_address,
            Err(SwitchError::Fabric(FabricError::AddressAlreadyAssigned(
                _,
                1
            )))
        ),
        "{duplicate_address:?}"
    );
    assert_eq!(switch.ports().len(), 2, "a refused port is not recorded");

    guests[0].send(&frame(mac(2), mac(1), b"still")).unwrap();
    assert_eq!(
        recv(&guests[1]).as_deref(),
        Some(frame(mac(2), mac(1), b"still").as_slice()),
        "a refusal does not disturb the running fabric"
    );
    switch.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_switch_that_can_still_be_added_to_still_stops() {
    // Retaining the control and frame senders means the frame channel never
    // closes on its own. Shutdown does not depend on it -- the task selects the
    // stop signal first -- and this is the test that says so, because a switch
    // that never stops is a leaked daemon on this host.
    let (mut switch, guests) = started(2);
    switch.add_port(PortId(7), mac(7)).await.unwrap();
    drop(guests);
    let receipt = tokio::time::timeout(Duration::from_secs(5), switch.shutdown())
        .await
        .expect("a switch holding its own senders still observes the stop signal")
        .unwrap();
    assert_eq!(receipt.frames_delivered, 0);
}
