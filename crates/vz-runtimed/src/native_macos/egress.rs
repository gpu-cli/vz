//! External egress for a native macOS Machine that declares `egress: allowed`.
//!
//! A Linux Machine's `allowed` is Apple's shared NAT attachment
//! (`vz_oci_macos::runtime::stack_vm::external_nic_required`). A native Machine
//! does not take that path, because the shared vmnet segment is one network
//! for every VM on the host: two Machines in different Environments that both
//! held a NAT NIC could address each other directly, and the product contract
//! forbids that segment as an authorization boundary and requires separate
//! Environments to be default-deny.
//!
//! A native Machine with `allowed` is instead given one more NIC whose other
//! end is a datagram socket held by this daemon and by nothing else. Behind it
//! runs a userspace stack (`translator`) that terminates the guest's TCP
//! connections and DNS queries and re-originates them from the host, after
//! `policy::admit` has refused everything that is not the public Internet. The
//! link is point-to-point: no other Machine, in this Environment or another,
//! holds a port on it, and the only thing on its far side is a process that
//! never forwards a guest packet. That is what makes "two Machines with egress
//! cannot reach each other" structural rather than a filter that a root user
//! in the guest could undo.
//!
//! `offline`, the default, is no link at all: no NIC, no socket, no task.

pub(crate) mod policy;
pub(crate) mod translator;

use std::net::Ipv4Addr;
use std::os::fd::OwnedFd;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use vz_runtime_contract::EgressPolicy;

use crate::environment_switch::MacAddress;
use translator::{EgressReceipt, Translator, TranslatorConfig};

/// The link's addresses. Every link uses the same ones, which is safe because
/// each link is its own two-member segment, and they sit in `198.18.0.0/15`
/// (RFC 2544 benchmarking space), which `policy::admit` refuses as a
/// destination, so no link can be used to reach another's.
pub(crate) const GATEWAY: Ipv4Addr = Ipv4Addr::new(198, 18, 0, 1);
pub(crate) const GUEST: Ipv4Addr = Ipv4Addr::new(198, 18, 0, 2);
pub(crate) const PREFIX: u8 = 24;
pub(crate) const MTU: u32 = 1500;
/// The translator's own link-layer address. Fixed for the same reason the IP
/// addresses are: no two links share a segment.
const GATEWAY_MAC: MacAddress = MacAddress::new([0x02, 0x76, 0x7a, 0x65, 0x67, 0x01]);
const GUEST_MAC_DOMAIN: &[u8] = b"vz.native.egress.guest.mac.v1\n";

/// Whether this Machine's declaration asks for an egress link at all.
///
/// The whole of `offline`'s enforcement for a native Machine: a Machine without
/// a link has no NIC that leads anywhere off its declared fabric ports, which
/// no rule inside the guest can change.
pub(crate) fn link_required(policy: EgressPolicy) -> bool {
    matches!(policy, EgressPolicy::Allowed)
}

/// Whether a declared fabric network's range overlaps the link's.
///
/// The guest would then hold two on-link routes for one range and send some of
/// its Environment's traffic to the translator, so such a Machine is refused
/// rather than booted with a routing table that depends on interface order.
pub(crate) fn overlaps_link(address: Ipv4Addr, prefix: u8) -> bool {
    let shared = prefix.min(PREFIX);
    let mask = u32::MAX.checked_shl(u32::from(32 - shared)).unwrap_or(0);
    u32::from(address) & mask == u32::from(GUEST) & mask
}

/// The guest end of a link: what becomes the Machine's NIC.
pub(crate) struct GuestLink {
    pub mac: String,
    pub socket: OwnedFd,
}

/// The daemon end of a link. Stopping it, or dropping it, ends the translator
/// and every host connection it opened.
#[must_use = "an egress link ends when its handle is dropped"]
pub(crate) struct EgressLink {
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<EgressReceipt>>,
}

impl std::fmt::Debug for EgressLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EgressLink").finish_non_exhaustive()
    }
}

impl EgressLink {
    /// Open a link for one Machine and start its translator.
    ///
    /// `machine` is the Machine's exact VM reservation, so the guest NIC's
    /// address is stable across boots of the same Machine and distinct between
    /// Machines.
    pub(crate) fn open(machine: &str) -> std::io::Result<(GuestLink, Self)> {
        let (host, guest) = std::os::unix::net::UnixDatagram::pair()?;
        crate::environment_switch::runtime::size_buffers(&host)?;
        crate::environment_switch::runtime::size_buffers(&guest)?;
        host.set_nonblocking(true)?;
        let host = tokio::net::UnixDatagram::from_std(host)?;
        let translator = Translator::new(TranslatorConfig {
            machine: machine.to_string(),
            mac: GATEWAY_MAC,
            address: GATEWAY,
            prefix: PREFIX,
            mtu: MTU,
        })
        .map_err(std::io::Error::other)?;
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(translator.run(host, stopped));
        tracing::info!(machine, guest = %GUEST, gateway = %GATEWAY, "native Machine egress link started");
        Ok((
            GuestLink {
                mac: guest_mac(machine).to_string(),
                socket: OwnedFd::from(guest),
            },
            Self {
                stop: Some(stop),
                task: Some(task),
            },
        ))
    }

    /// Stop the translator and report what it carried.
    pub(crate) async fn shutdown(&mut self) -> Option<EgressReceipt> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.task.take()?.await.ok()
    }
}

impl Drop for EgressLink {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

/// The guest program that addresses the egress NIC, and the exact stdout it
/// prints once the guest is using the link.
///
/// Unlike a fabric port, which readiness addresses with `ifconfig` on every
/// Up, the egress NIC is given a persistent network service: macOS creates one
/// for a new Ethernet interface by itself, and an interface with a service is
/// one configd manages, so an address set with `ifconfig` alone would be
/// replaced. The service is set to the link's fixed addresses with the
/// translator as router and resolver, which also survives a restart inside the
/// guest. The NIC is found by its MAC, for the reasons `fabric` gives.
///
/// Success is read back from the guest's routing table and resolver
/// configuration, not from `networksetup` exiting zero: configd applies a
/// service change asynchronously, and the link is only in use once the
/// default route and the first resolver are the translator's.
pub(crate) fn configure_program(mac: &str) -> (String, String) {
    let mac = mac.to_ascii_lowercase();
    let match_by_mac = super::fabric::MATCH_INTERFACE_BY_MAC_AWK;
    let program = format!(
        r#"set -eu
iface=$(/sbin/ifconfig -a | /usr/bin/awk -v want='{mac}' '{match_by_mac}')
ns=/usr/sbin/networksetup
service=$($ns -listnetworkserviceorder | /usr/bin/awk -v dev="$iface" '
  /^\([0-9*]+\) / {{ name = $0; sub(/^\([0-9*]+\) /, "", name); next }}
  /^\(Hardware Port: / && index($0, "Device: " dev ")") {{ print name; exit }}')
if [ -z "$service" ]; then
  port=$($ns -listallhardwareports | /usr/bin/awk -v dev="$iface" '
    /^Hardware Port: / {{ port = substr($0, 16) }}
    /^Device: / && $2 == dev {{ print port; exit }}')
  [ -n "$port" ] || {{ echo "no hardware port for $iface" >&2; exit 1; }}
  service='vz egress'
  $ns -createnetworkservice "$service" "$port" >/dev/null
fi
$ns -setnetworkserviceenabled "$service" on >/dev/null
$ns -setmanual "$service" '{GUEST}' '{netmask}' '{GATEWAY}' >/dev/null
$ns -setdnsservers "$service" '{GATEWAY}' >/dev/null
$ns -setv6off "$service" >/dev/null
tries=0
while :; do
  address=$(/sbin/ifconfig "$iface" inet 2>/dev/null | /usr/bin/awk '$1 == "inet" {{ print $2; exit }}')
  router=$(/sbin/route -n get default 2>/dev/null | /usr/bin/awk '$1 == "gateway:" {{ print $2; exit }}')
  resolver=$(/usr/sbin/scutil --dns | /usr/bin/awk '$1 == "nameserver[0]" {{ print $3; exit }}')
  if [ "$address" = '{GUEST}' ] && [ "$router" = '{GATEWAY}' ] && [ "$resolver" = '{GATEWAY}' ]; then
    break
  fi
  tries=$((tries + 1))
  if [ "$tries" -ge 150 ]; then
    echo "egress link on $iface did not come up: address=$address router=$router resolver=$resolver" >&2
    exit 1
  fi
  /bin/sleep 0.2
done
echo "$address $router $resolver"
"#,
        netmask = Ipv4Addr::from(u32::MAX << (32 - u32::from(PREFIX))),
    );
    (program, format!("{GUEST} {GATEWAY} {GATEWAY}\n"))
}

/// The guest NIC's link-layer address, derived from the Machine so that a
/// Machine that comes back presents the address its guest already configured.
pub(crate) fn guest_mac(machine: &str) -> MacAddress {
    use sha2::{Digest, Sha256};
    let digest = Sha256::new()
        .chain_update(GUEST_MAC_DOMAIN)
        .chain_update(machine.as_bytes())
        .finalize();
    let mut bytes = [0_u8; 6];
    bytes.copy_from_slice(&digest[..6]);
    // Locally administered, individual.
    bytes[0] = (bytes[0] | 0x02) & !0x01;
    MacAddress::new(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The gate itself: `offline` (and the default, which is `offline`) gets no
    /// link, `allowed` gets one.
    #[test]
    fn only_allowed_egress_opens_a_link() {
        assert!(!link_required(EgressPolicy::Offline));
        assert!(!link_required(EgressPolicy::default()));
        assert!(link_required(EgressPolicy::Allowed));
    }

    #[test]
    fn a_guest_mac_is_stable_per_machine_distinct_between_and_a_station_address() {
        let first = guest_mac("vm-a");
        assert_eq!(first, guest_mac("vm-a"));
        assert_ne!(first, guest_mac("vm-b"));
        assert!(first.is_locally_administered());
        assert!(!first.is_group());
        assert_ne!(first, GATEWAY_MAC);
    }

    #[test]
    fn a_fabric_range_that_covers_the_link_is_detected() {
        assert!(overlaps_link(Ipv4Addr::new(198, 18, 0, 0), 24));
        assert!(overlaps_link(Ipv4Addr::new(198, 16, 0, 0), 12));
        assert!(overlaps_link(Ipv4Addr::new(198, 18, 0, 0), 30));
        assert!(!overlaps_link(Ipv4Addr::new(198, 18, 1, 0), 24));
        assert!(!overlaps_link(Ipv4Addr::new(10, 0, 0, 0), 8));
    }

    /// Two Machines that both have egress share nothing: what one guest sends
    /// reaches only its own translator, which resets every connection towards
    /// a non-public address, and the other guest's NIC never sees a frame.
    #[tokio::test]
    async fn two_links_cannot_reach_each_other() {
        use smoltcp::wire::{EthernetFrame, EthernetProtocol, IpProtocol, Ipv4Packet, TcpPacket};
        let (first, mut first_link) =
            EgressLink::open("vm-first").unwrap_or_else(|e| unreachable!("{e}"));
        let (second, mut second_link) =
            EgressLink::open("vm-second").unwrap_or_else(|e| unreachable!("{e}"));
        let nonblocking = |socket: OwnedFd| {
            let socket = std::os::unix::net::UnixDatagram::from(socket);
            socket
                .set_nonblocking(true)
                .unwrap_or_else(|e| unreachable!("{e}"));
            tokio::net::UnixDatagram::from_std(socket).unwrap_or_else(|e| unreachable!("{e}"))
        };
        let first = nonblocking(first.socket);
        let second = nonblocking(second.socket);
        // The second guest's only address is the one this guest holds itself,
        // so it is local to the first guest and never reaches the wire. What
        // can reach the wire is the rest of the link's range and the shared
        // vmnet segment a NAT-attached Machine of another Environment sits on.
        // The gateway's MAC is fixed, so the guest needs no ARP to address it.
        let guest = guest_mac("vm-first");
        let mut resets = 0;
        for destination in [GATEWAY, Ipv4Addr::new(192, 168, 64, 2)] {
            let syn = translator::tests::syn_frame(
                guest.bytes(),
                GATEWAY_MAC.bytes(),
                GUEST.octets(),
                destination.octets(),
                8080,
            );
            first
                .send(&syn)
                .await
                .unwrap_or_else(|e| unreachable!("{e}"));
            let mut buffer = vec![0_u8; 2048];
            loop {
                let read = tokio::time::timeout(Duration::from_secs(5), first.recv(&mut buffer))
                    .await
                    .unwrap_or_else(|_| unreachable!("the translator answers the SYN"))
                    .unwrap_or_else(|e| unreachable!("{e}"));
                if let Some(reply) =
                    translator::tests::arp_reply_for(&buffer[..read], guest.bytes())
                {
                    // The translator asks where the guest is before answering
                    // it, and drops the segment that prompted the question, so
                    // the guest answers and retransmits as a real one would.
                    for frame in [&reply, &syn] {
                        first
                            .send(frame)
                            .await
                            .unwrap_or_else(|e| unreachable!("{e}"));
                    }
                    continue;
                }
                let frame = EthernetFrame::new_unchecked(&buffer[..read]);
                if frame.ethertype() != EthernetProtocol::Ipv4 {
                    continue;
                }
                let ip = Ipv4Packet::new_unchecked(frame.payload());
                if ip.next_header() != IpProtocol::Tcp {
                    continue;
                }
                if TcpPacket::new_unchecked(ip.payload()).rst() {
                    resets += 1;
                }
                break;
            }
        }
        assert_eq!(
            resets, 2,
            "every connection towards a non-public address is reset"
        );
        let mut quiet = vec![0_u8; 2048];
        assert!(
            tokio::time::timeout(Duration::from_millis(500), second.recv(&mut quiet))
                .await
                .is_err(),
            "nothing the first guest sent reached the second guest's NIC"
        );
        let first_receipt = first_link
            .shutdown()
            .await
            .unwrap_or_else(|| unreachable!());
        let second_receipt = second_link
            .shutdown()
            .await
            .unwrap_or_else(|| unreachable!());
        // Every SYN is judged, a retransmitted one included.
        assert!(first_receipt.connections_refused >= 2);
        assert_eq!(first_receipt.connections_opened, 0);
        assert_eq!(second_receipt, EgressReceipt::default());
    }

    /// A link ends when its handle does: the translator stops and reports.
    #[tokio::test]
    async fn a_link_stops_when_asked_and_reports_what_it_carried() {
        let (guest, mut link) = EgressLink::open("vm-test").unwrap_or_else(|e| unreachable!("{e}"));
        assert_eq!(guest.mac, guest_mac("vm-test").to_string());
        let receipt = link.shutdown().await.unwrap_or_else(|| unreachable!());
        assert_eq!(receipt, EgressReceipt::default());
    }
}
