//! Which destinations a native Machine's egress link may open a connection to.
//!
//! The product contract makes external egress "deny-first" and says enabling it
//! "does not authorize host imports, LAN access, control-plane access, or
//! cross-Environment traffic" (`docs/developer-environments.md`, Network
//! topology contract). This is where that sentence is enforced for a native
//! macOS Machine: every connection the translator opens on a guest's behalf is
//! opened by this process, from the host's own network stack, so a destination
//! the host can reach is one the guest can reach unless this refuses it.
//!
//! What it refuses is therefore everything that is not the public Internet:
//! the host itself (loopback and every address its interfaces hold), the LAN
//! and every other private range, link-local, carrier-grade NAT, the shared
//! vmnet segment other VMs sit on (it is private space), multicast and
//! broadcast, and the ranges reserved for documentation and benchmarking — the
//! last of which holds the egress link's own addresses, so one Machine's link
//! can never be a route to another Machine's.

use std::net::Ipv4Addr;

/// Why a destination was refused, kept so a refusal can be logged with a cause
/// rather than as a bare drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// `0.0.0.0/8`, `127.0.0.0/8`: this host, or no host at all.
    ThisHost,
    /// An address one of the host's own interfaces holds.
    HostInterface,
    /// RFC 1918 space: a LAN, a VPN, or the shared vmnet NAT segment.
    Private,
    /// `169.254.0.0/16`.
    LinkLocal,
    /// `100.64.0.0/10`, carrier-grade NAT and overlay networks built on it.
    SharedAddressSpace,
    /// Multicast, limited broadcast and the reserved `240.0.0.0/4` block.
    NotUnicast,
    /// IETF protocol assignments, documentation and benchmarking ranges, the
    /// last of which holds every egress link's own addresses.
    Reserved,
}

/// The one decision: may a guest's connection to `destination` be opened?
///
/// `host_addresses` are the IPv4 addresses the host's interfaces hold right
/// now. They are refused by identity rather than by range because a host with
/// a public address would otherwise expose every service it binds on that
/// address to its guests, which is a host import the Environment never declared.
pub(crate) fn admit(destination: Ipv4Addr, host_addresses: &[Ipv4Addr]) -> Result<(), Refusal> {
    let [a, b, c, _] = destination.octets();
    if a == 0 || destination.is_loopback() {
        return Err(Refusal::ThisHost);
    }
    if destination.is_private() {
        return Err(Refusal::Private);
    }
    if destination.is_link_local() {
        return Err(Refusal::LinkLocal);
    }
    if a == 100 && (b & 0xc0) == 64 {
        return Err(Refusal::SharedAddressSpace);
    }
    if destination.is_multicast() || destination.is_broadcast() || a >= 240 {
        return Err(Refusal::NotUnicast);
    }
    let reserved = (a == 192 && b == 0 && c == 0)
        || destination.is_documentation()
        || (a == 198 && (b & 0xfe) == 18);
    if reserved {
        return Err(Refusal::Reserved);
    }
    if host_addresses.contains(&destination) {
        return Err(Refusal::HostInterface);
    }
    Ok(())
}

/// Every IPv4 address this host's interfaces hold right now.
///
/// Read per decision rather than once at link start, because a laptop's
/// addresses change as it moves between networks, and an address it acquired
/// after the link started is exactly as much the host as one it held before.
pub(crate) fn host_addresses() -> Vec<Ipv4Addr> {
    let Ok(interfaces) = nix::ifaddrs::getifaddrs() else {
        return Vec::new();
    };
    interfaces
        .filter_map(|interface| {
            let address = interface.address?;
            let address = address.as_sockaddr_in()?;
            Some(address.ip())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admitted(address: [u8; 4]) -> Result<(), Refusal> {
        admit(Ipv4Addr::from(address), &[])
    }

    #[test]
    fn public_unicast_destinations_are_admitted() {
        for address in [
            [142, 250, 72, 16],  // a public web host
            [1, 1, 1, 1],        // a public resolver
            [8, 8, 8, 8],        // another
            [172, 15, 255, 255], // just below 172.16/12
            [172, 32, 0, 1],     // just above it
            [100, 63, 255, 255], // just below 100.64/10
            [100, 128, 0, 1],    // just above it
            [198, 17, 255, 255], // just below 198.18/15
            [198, 20, 0, 1],     // just above it
        ] {
            assert_eq!(admitted(address), Ok(()), "{address:?} is public");
        }
    }

    #[test]
    fn the_host_itself_is_refused() {
        assert_eq!(admitted([127, 0, 0, 1]), Err(Refusal::ThisHost));
        assert_eq!(admitted([127, 9, 9, 9]), Err(Refusal::ThisHost));
        assert_eq!(admitted([0, 0, 0, 0]), Err(Refusal::ThisHost));
    }

    /// An address the host's own interface holds is the host, however public it
    /// is. Without this a host with a public address would serve every
    /// wildcard listener it has to every guest with egress.
    #[test]
    fn an_address_the_host_holds_is_refused_even_when_public() {
        let public = Ipv4Addr::new(203, 0, 114, 7);
        assert_eq!(admit(public, &[]), Ok(()));
        assert_eq!(admit(public, &[public]), Err(Refusal::HostInterface));
    }

    /// The LAN, VPNs and the shared vmnet segment every NAT-attached VM sits on
    /// are all private space. Refusing it is what keeps egress from being a
    /// path to another Environment's Linux Machine on `192.168.64.0/24` or to
    /// anything on the host's LAN.
    #[test]
    fn private_space_including_the_shared_vmnet_segment_is_refused() {
        for address in [
            [10, 0, 0, 1],
            [10, 255, 255, 254],
            [172, 16, 0, 1],
            [172, 31, 255, 254],
            [192, 168, 1, 1],
            [192, 168, 64, 2],
        ] {
            assert_eq!(admitted(address), Err(Refusal::Private), "{address:?}");
        }
    }

    #[test]
    fn link_local_and_shared_address_space_are_refused() {
        assert_eq!(admitted([169, 254, 169, 254]), Err(Refusal::LinkLocal));
        assert_eq!(admitted([100, 64, 0, 1]), Err(Refusal::SharedAddressSpace));
        assert_eq!(
            admitted([100, 100, 100, 100]),
            Err(Refusal::SharedAddressSpace)
        );
        assert_eq!(
            admitted([100, 127, 255, 254]),
            Err(Refusal::SharedAddressSpace)
        );
    }

    #[test]
    fn non_unicast_destinations_are_refused() {
        assert_eq!(admitted([224, 0, 0, 251]), Err(Refusal::NotUnicast));
        assert_eq!(admitted([239, 255, 255, 250]), Err(Refusal::NotUnicast));
        assert_eq!(admitted([255, 255, 255, 255]), Err(Refusal::NotUnicast));
        assert_eq!(admitted([240, 0, 0, 1]), Err(Refusal::NotUnicast));
    }

    /// Every egress link uses the same addresses inside `198.18.0.0/15`, so a
    /// connection towards that range is never a connection to the Internet and
    /// would otherwise be the one place two Machines' links could meet.
    #[test]
    fn reserved_ranges_including_every_link_address_are_refused() {
        for address in [
            [198, 18, 0, 1],
            [198, 18, 0, 2],
            [198, 19, 255, 254],
            [192, 0, 0, 8],
            [192, 0, 2, 1],
            [198, 51, 100, 1],
            [203, 0, 113, 1],
        ] {
            assert_eq!(admitted(address), Err(Refusal::Reserved), "{address:?}");
        }
    }

    #[test]
    fn this_host_reports_its_own_loopback_address() {
        assert!(host_addresses().contains(&Ipv4Addr::LOCALHOST));
    }
}
