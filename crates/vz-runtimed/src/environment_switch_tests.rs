//! Every rule is decided on synthetic frames, so a passing test means the rule
//! holds rather than that a running switch happened not to misbehave.
#![allow(clippy::unwrap_used)]

use super::*;

const IPV4: u16 = 0x0800;
const ARP: u16 = 0x0806;

fn mac(last: u8) -> MacAddress {
    MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, last])
}

fn frame(destination: MacAddress, source: MacAddress, ethertype: u16) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEADER_LEN + 4);
    bytes.extend_from_slice(&destination.bytes());
    bytes.extend_from_slice(&source.bytes());
    bytes.extend_from_slice(&ethertype.to_be_bytes());
    bytes.extend_from_slice(b"body");
    bytes
}

/// Three Machines on one network, ports 1..=3 holding addresses ..:01..:03.
fn network() -> Fabric {
    let mut fabric = Fabric::new();
    for index in 1..=3_u8 {
        fabric.attach(PortId(u32::from(index)), mac(index)).unwrap();
    }
    fabric
}

#[test]
fn unicast_reaches_exactly_the_port_holding_the_destination() {
    let mut fabric = network();
    let sent = frame(mac(3), mac(1), IPV4);
    assert_eq!(
        fabric.forward(PortId(1), &sent),
        Disposition::Unicast(PortId(3))
    );
    assert_eq!(fabric.counters().unicast_forwarded, 1);
    assert_eq!(fabric.counters().dropped_total(), 0);
}

#[test]
fn a_guest_cannot_claim_a_siblings_address() {
    // The whole reason addresses are assigned and never learned: on a learning
    // bridge this frame installs port 1 as the owner of the sibling's address
    // and every later frame for that sibling is delivered to the impostor.
    let mut fabric = network();
    let spoofed = frame(mac(3), mac(2), IPV4);
    assert_eq!(
        fabric.forward(PortId(1), &spoofed),
        Disposition::Drop(DropReason::SourceAddressNotAssigned)
    );
    // The sibling's address still resolves to the sibling.
    let honest = frame(mac(3), mac(2), IPV4);
    assert_eq!(
        fabric.forward(PortId(2), &honest),
        Disposition::Unicast(PortId(3))
    );
    assert_eq!(
        fabric
            .counters()
            .dropped
            .get(&DropReason::SourceAddressNotAssigned),
        Some(&1)
    );
}

#[test]
fn unknown_unicast_is_dropped_and_never_flooded() {
    // A bridge floods this to every port, which hands one Machine's traffic to
    // every other Machine whenever a destination is momentarily unknown.
    let mut fabric = network();
    let sent = frame(mac(9), mac(1), IPV4);
    assert_eq!(
        fabric.forward(PortId(1), &sent),
        Disposition::Drop(DropReason::DestinationUnknown)
    );
    assert_eq!(fabric.counters().group_forwarded, 0);
}

#[test]
fn broadcast_and_multicast_reach_every_other_member_and_never_the_sender() {
    let mut fabric = network();
    for destination in [
        MacAddress::BROADCAST,
        MacAddress::new([0x01, 0, 0x5e, 0, 0, 1]),
    ] {
        let sent = frame(destination, mac(2), ARP);
        assert_eq!(
            fabric.forward(PortId(2), &sent),
            Disposition::Group(vec![PortId(1), PortId(3)]),
            "group frame to {destination}"
        );
    }
    assert_eq!(fabric.counters().group_forwarded, 2);
}

#[test]
fn a_group_address_is_refused_as_a_source_and_as_a_port_assignment() {
    let mut fabric = network();
    let sent = frame(mac(3), MacAddress::BROADCAST, IPV4);
    assert_eq!(
        fabric.forward(PortId(1), &sent),
        Disposition::Drop(DropReason::SourceIsGroupAddress)
    );
    assert_eq!(
        fabric.attach(PortId(4), MacAddress::BROADCAST),
        Err(FabricError::GroupAddressAssigned(MacAddress::BROADCAST))
    );
}

#[test]
fn only_the_ethertypes_this_fabric_carries_are_forwarded() {
    let mut fabric = network();
    for permitted in [IPV4, ARP] {
        let sent = frame(mac(3), mac(1), permitted);
        assert_eq!(
            fabric.forward(PortId(1), &sent),
            Disposition::Unicast(PortId(3))
        );
    }
    // IPv6 and 802.1Q are refused: no part of the topology contract declares
    // what they would carry, so forwarding them would carry undeclared traffic.
    for refused in [0x86DD_u16, 0x8100, 0x88CC, 0x0000] {
        let sent = frame(mac(3), mac(1), refused);
        assert_eq!(
            fabric.forward(PortId(1), &sent),
            Disposition::Drop(DropReason::EthertypeNotPermitted),
            "ethertype {refused:#06x}"
        );
    }
}

#[test]
fn a_frame_too_short_to_have_addresses_is_dropped() {
    let mut fabric = network();
    let full = frame(mac(3), mac(1), IPV4);
    for length in 0..HEADER_LEN {
        assert_eq!(
            fabric.forward(PortId(1), &full[..length]),
            Disposition::Drop(DropReason::ShortFrame),
            "length {length}"
        );
    }
    // Exactly a header and nothing else is still a decidable frame.
    assert_eq!(
        fabric.forward(PortId(1), &full[..HEADER_LEN]),
        Disposition::Unicast(PortId(3))
    );
}

#[test]
fn a_frame_from_a_port_this_network_never_attached_is_dropped() {
    let mut fabric = network();
    let sent = frame(mac(3), mac(1), IPV4);
    assert_eq!(
        fabric.forward(PortId(77), &sent),
        Disposition::Drop(DropReason::UnknownIngressPort)
    );
}

#[test]
fn two_networks_using_the_same_addresses_forward_nothing_between_them() {
    // Sibling Environments are required to be unable to resolve or route to one
    // another even with identical addressing. Here that is structural: each
    // network is its own set of ports, so a frame entering one is only ever
    // decided against that one's table.
    let mut left = network();
    let mut right = network();
    let sent = frame(mac(3), mac(1), IPV4);
    assert_eq!(
        left.forward(PortId(1), &sent),
        Disposition::Unicast(PortId(3))
    );
    assert_eq!(
        right.counters(),
        &Counters::default(),
        "the sibling network saw nothing"
    );

    // Detaching every port on the left leaves the right's identical addressing
    // untouched, so the two tables were never one table.
    for index in 1..=3_u32 {
        left.detach(PortId(index)).unwrap();
    }
    assert_eq!(
        left.forward(PortId(1), &sent),
        Disposition::Drop(DropReason::UnknownIngressPort)
    );
    assert_eq!(
        right.forward(PortId(1), &sent),
        Disposition::Unicast(PortId(3))
    );
}

#[test]
fn a_detached_address_stops_resolving_to_the_machine_that_is_gone() {
    let mut fabric = network();
    assert_eq!(fabric.detach(PortId(3)), Ok(mac(3)));
    assert_eq!(fabric.address_of(PortId(3)), None);
    let sent = frame(mac(3), mac(1), IPV4);
    assert_eq!(
        fabric.forward(PortId(1), &sent),
        Disposition::Drop(DropReason::DestinationUnknown)
    );
    // A group frame now reaches only the members that remain.
    let broadcast = frame(MacAddress::BROADCAST, mac(1), ARP);
    assert_eq!(
        fabric.forward(PortId(1), &broadcast),
        Disposition::Group(vec![PortId(2)])
    );
    assert_eq!(
        fabric.detach(PortId(3)),
        Err(FabricError::PortNotAttached(3))
    );
}

#[test]
fn a_port_and_an_address_can_each_be_attached_only_once() {
    let mut fabric = network();
    assert_eq!(
        fabric.attach(PortId(1), mac(9)),
        Err(FabricError::PortAlreadyAttached(1))
    );
    assert_eq!(
        fabric.attach(PortId(9), mac(2)),
        Err(FabricError::AddressAlreadyAssigned(mac(2), 2))
    );
    // The refusals changed nothing.
    assert_eq!(fabric.address_of(PortId(1)), Some(mac(1)));
    let sent = frame(mac(2), mac(1), IPV4);
    assert_eq!(
        fabric.forward(PortId(1), &sent),
        Disposition::Unicast(PortId(2))
    );
}

#[test]
fn a_frame_addressed_to_its_own_sender_is_not_delivered_back() {
    let mut fabric = network();
    let sent = frame(mac(1), mac(1), IPV4);
    assert_eq!(
        fabric.forward(PortId(1), &sent),
        Disposition::Drop(DropReason::DestinationUnknown)
    );
}

#[test]
fn every_refused_frame_is_counted_under_exactly_one_reason() {
    let mut fabric = network();
    let cases: [(&[u8], DropReason); 5] = [
        (&[], DropReason::ShortFrame),
        (&[0_u8; HEADER_LEN], DropReason::SourceAddressNotAssigned),
        (&[0xff_u8; HEADER_LEN], DropReason::SourceIsGroupAddress),
        (&[], DropReason::ShortFrame),
        (&[0_u8; 4], DropReason::ShortFrame),
    ];
    for (bytes, expected) in cases {
        assert_eq!(
            fabric.forward(PortId(1), bytes),
            Disposition::Drop(expected)
        );
    }
    let unknown = frame(mac(9), mac(1), IPV4);
    fabric.forward(PortId(1), &unknown);
    assert_eq!(fabric.counters().dropped_total(), 6);
    assert_eq!(
        fabric.counters().dropped.get(&DropReason::ShortFrame),
        Some(&3)
    );
    assert_eq!(fabric.counters().unicast_forwarded, 0);
    assert_eq!(fabric.counters().group_forwarded, 0);
}

// --- Derived addressing ----------------------------------------------------

#[test]
fn a_derived_address_is_stable_for_one_attachment() {
    // A restored VM's NIC has to match the address its saved guest already
    // believes it has, and the switch has to expect the same address the guest
    // configures without either telling the other.
    let first = MacAddress::derive("env-a", "machine-1", "net-private");
    let again = MacAddress::derive("env-a", "machine-1", "net-private");
    assert_eq!(first, again);
}

#[test]
fn a_derived_address_is_a_locally_administered_station_address() {
    for (environment, machine, network) in [
        ("env-a", "machine-1", "net-private"),
        ("", "", ""),
        ("e", "m", "n"),
    ] {
        let address = MacAddress::derive(environment, machine, network);
        assert!(
            address.is_locally_administered(),
            "{address} must be locally administered; no OUI is registered for these"
        );
        assert!(
            !address.is_group(),
            "{address} must not be a group address; the fabric refuses one as a source"
        );
    }
}

#[test]
fn attachments_differing_in_any_one_identifier_get_different_addresses() {
    let base = MacAddress::derive("env-a", "machine-1", "net-private");
    for (environment, machine, network) in [
        ("env-b", "machine-1", "net-private"),
        ("env-a", "machine-2", "net-private"),
        ("env-a", "machine-1", "net-other"),
    ] {
        assert_ne!(
            base,
            MacAddress::derive(environment, machine, network),
            "{environment}/{machine}/{network} collided with the base attachment"
        );
    }
}

#[test]
fn identifiers_cannot_be_re_split_to_produce_one_address() {
    // Concatenating without lengths would make ("ab", "c", "d") and
    // ("a", "bc", "d") the same input, so two attachments would be handed one
    // address and the fabric would refuse to attach the second.
    assert_ne!(
        MacAddress::derive("ab", "c", "d"),
        MacAddress::derive("a", "bc", "d")
    );
    assert_ne!(
        MacAddress::derive("a", "b", "cd"),
        MacAddress::derive("a", "bc", "d")
    );
}

#[test]
fn derived_addresses_attach_to_one_fabric_without_collision() {
    let mut fabric = Fabric::new();
    for index in 1..=8_u32 {
        let address = MacAddress::derive("env-a", &format!("machine-{index}"), "net-private");
        assert_eq!(fabric.attach(PortId(index), address), Ok(()));
    }
    let sender = MacAddress::derive("env-a", "machine-1", "net-private");
    let target = MacAddress::derive("env-a", "machine-5", "net-private");
    let sent = frame(target, sender, IPV4);
    assert_eq!(
        fabric.forward(PortId(1), &sent),
        Disposition::Unicast(PortId(5))
    );
}
