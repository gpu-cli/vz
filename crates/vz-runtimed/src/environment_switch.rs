//! The Ethernet fabric one Environment network owns.
//!
//! Each Machine attached to a network holds one end of a datagram socket and the
//! runtime holds the other, so a network is a set of sockets this process
//! forwards between rather than a segment on the host. Two Environments cannot
//! reach one another even when their networks use identical addresses, because
//! there is no shared segment to reach across: an Environment's frames only ever
//! enter its own fabric.
//!
//! This module is the forwarding decision alone. It performs no I/O, owns no
//! socket and starts no task, so every rule below is decided on synthetic frames
//! in tests rather than inferred from a running switch.
//!
//! Two decisions are worth stating, because both differ from an ordinary bridge
//! and both are the reason this exists:
//!
//! A port's address is assigned when the Machine is admitted and is never
//! learned from traffic. A learning bridge installs whatever source address it
//! sees, so any guest could claim a sibling's address by sending one frame and
//! receive that sibling's traffic from then on. Here a frame whose source is not
//! the address assigned to its ingress port is dropped and counted.
//!
//! Unicast to an address no port holds is dropped, never flooded. A bridge
//! floods unknown unicast to every port, which would hand one Machine's traffic
//! to every other Machine on the network whenever a destination is momentarily
//! unknown.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};
use thiserror::Error;

/// Domain separator for derived addresses, so this derivation can never collide
/// with another use of the same identifiers under a different hash purpose.
const ADDRESS_DERIVATION_DOMAIN: &[u8] = b"vz.environment.network.attachment.mac.v1\n";

/// Destination, source and ethertype: the fixed part of an Ethernet II header.
/// VLAN tags are not accepted, so this length is exact rather than a minimum.
pub const HEADER_LEN: usize = 14;

/// Ethertypes this fabric carries.
///
/// The fabric exists to carry declared TCP endpoints between Machines, which
/// needs IPv4 and the ARP that resolves it. Everything else is refused rather
/// than forwarded unexamined, so a guest cannot use the fabric as a general
/// layer-2 channel to a sibling. IPv6 is absent because no part of the topology
/// contract assigns IPv6 addresses; admitting it would forward traffic no
/// declaration covers.
const PERMITTED_ETHERTYPES: [u16; 2] = [0x0800, 0x0806];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FabricError {
    #[error("port {0} is already attached to this network")]
    PortAlreadyAttached(u32),
    #[error("address {0} is already assigned to port {1} on this network")]
    AddressAlreadyAssigned(MacAddress, u32),
    #[error("port {0} is not attached to this network")]
    PortNotAttached(u32),
    #[error("a group address cannot be assigned to a port: {0}")]
    GroupAddressAssigned(MacAddress),
}

/// One Machine's attachment to one network. The daemon maps these to Machines;
/// the fabric only needs them to be distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PortId(pub u32);

impl std::fmt::Display for PortId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A 48-bit Ethernet address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MacAddress([u8; 6]);

impl MacAddress {
    pub const BROADCAST: Self = Self([0xff; 6]);

    pub const fn new(bytes: [u8; 6]) -> Self {
        Self(bytes)
    }

    pub const fn bytes(&self) -> [u8; 6] {
        self.0
    }

    /// Whether this is a group address: broadcast, or any multicast.
    ///
    /// The low bit of the first octet is the individual/group bit, so this is one
    /// test rather than a broadcast comparison plus a multicast range check.
    pub const fn is_group(&self) -> bool {
        self.0[0] & 1 == 1
    }

    /// Whether the locally administered bit is set, as every derived address is.
    pub const fn is_locally_administered(&self) -> bool {
        self.0[0] & 2 == 2
    }

    /// The address a Machine presents on one network, derived from the identity
    /// of that attachment.
    ///
    /// It has to be derived rather than generated, for two reasons that pull the
    /// same way. The switch addresses a guest by its MAC and refuses any frame
    /// whose source is not the address it assigned, so the address the guest
    /// configures and the address the switch expects must agree without either
    /// telling the other. And a restored VM's NIC must match the address its
    /// saved guest already believes it has, which a fresh random address on every
    /// boot cannot do.
    ///
    /// The two administered bits are then forced rather than taken from the hash:
    /// the locally administered bit is set because no registered OUI was
    /// assigned for these, and the group bit is cleared because a station address
    /// is never a group address and the fabric refuses one as a source.
    pub fn derive(environment_id: &str, machine_id: &str, network_id: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(ADDRESS_DERIVATION_DOMAIN);
        // Length-prefixed, so no pair of identifiers can be re-split to produce
        // the same input and hand two attachments one address.
        for field in [environment_id, machine_id, network_id] {
            hasher.update(u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
            hasher.update(field.as_bytes());
        }
        let digest = hasher.finalize();
        let mut bytes = [0_u8; 6];
        bytes.copy_from_slice(&digest[..6]);
        bytes[0] = (bytes[0] | 2) & 0xfe;
        Self(bytes)
    }
}

impl std::fmt::Display for MacAddress {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let [a, b, c, d, e, f] = self.0;
        write!(formatter, "{a:02x}:{b:02x}:{c:02x}:{d:02x}:{e:02x}:{f:02x}")
    }
}

/// Why a frame was not forwarded. Every drop has exactly one of these, so the
/// counters account for every frame the fabric refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DropReason {
    /// Shorter than an Ethernet header, so it has no addresses to judge.
    ShortFrame,
    /// Arrived on a port this network has not attached.
    UnknownIngressPort,
    /// The source is not the address assigned to the ingress port.
    SourceAddressNotAssigned,
    /// A group address can only be a destination, never a source.
    SourceIsGroupAddress,
    /// Not one of the ethertypes this fabric carries.
    EthertypeNotPermitted,
    /// Unicast to an address no port on this network holds. Never flooded.
    DestinationUnknown,
}

/// What the fabric decided to do with one frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    /// Deliver to exactly this port.
    Unicast(PortId),
    /// Deliver to every attached port except the one it arrived on.
    Group(Vec<PortId>),
    /// Deliver nowhere, for this reason.
    Drop(DropReason),
}

/// Per-rule accounting. The denial matrix is proven from these, so a refused
/// frame is visible as a number rather than only as an absence of delivery.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counters {
    pub unicast_forwarded: u64,
    pub group_forwarded: u64,
    pub dropped: BTreeMap<DropReason, u64>,
}

impl Counters {
    /// Every frame the fabric refused, for any reason.
    pub fn dropped_total(&self) -> u64 {
        self.dropped.values().sum()
    }
}

/// The fixed part of an Ethernet II header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub destination: MacAddress,
    pub source: MacAddress,
    pub ethertype: u16,
}

impl FrameHeader {
    /// Read the header, or `None` when the frame is too short to have one.
    pub fn parse(frame: &[u8]) -> Option<Self> {
        let header: &[u8; HEADER_LEN] = frame.get(..HEADER_LEN)?.try_into().ok()?;
        let mut destination = [0_u8; 6];
        let mut source = [0_u8; 6];
        destination.copy_from_slice(&header[0..6]);
        source.copy_from_slice(&header[6..12]);
        Some(Self {
            destination: MacAddress(destination),
            source: MacAddress(source),
            ethertype: u16::from_be_bytes([header[12], header[13]]),
        })
    }
}

/// One network's ports and the decisions it makes about their frames.
#[derive(Debug, Default)]
pub struct Fabric {
    ports: BTreeMap<PortId, MacAddress>,
    by_address: BTreeMap<MacAddress, PortId>,
    counters: Counters,
}

impl Fabric {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a Machine's port with the address it was assigned.
    ///
    /// Both the port and the address must be new to this network. A repeated
    /// address would make delivery ambiguous, and silently preferring one port
    /// is how a sibling's traffic ends up somewhere it was never declared to go.
    pub fn attach(&mut self, port: PortId, address: MacAddress) -> Result<(), FabricError> {
        if address.is_group() {
            return Err(FabricError::GroupAddressAssigned(address));
        }
        if self.ports.contains_key(&port) {
            return Err(FabricError::PortAlreadyAttached(port.0));
        }
        if let Some(existing) = self.by_address.get(&address) {
            return Err(FabricError::AddressAlreadyAssigned(address, existing.0));
        }
        self.ports.insert(port, address);
        self.by_address.insert(address, port);
        Ok(())
    }

    /// Detach a port, so frames to its address become unknown rather than
    /// continuing to resolve to a Machine that is gone.
    pub fn detach(&mut self, port: PortId) -> Result<MacAddress, FabricError> {
        let address = self
            .ports
            .remove(&port)
            .ok_or(FabricError::PortNotAttached(port.0))?;
        self.by_address.remove(&address);
        Ok(address)
    }

    /// The address assigned to a port, if it is attached.
    pub fn address_of(&self, port: PortId) -> Option<MacAddress> {
        self.ports.get(&port).copied()
    }

    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    /// Decide where one frame arriving on `ingress` goes, and count the decision.
    pub fn forward(&mut self, ingress: PortId, frame: &[u8]) -> Disposition {
        let decision = self.decide(ingress, frame);
        match &decision {
            Disposition::Unicast(_) => self.counters.unicast_forwarded += 1,
            Disposition::Group(_) => self.counters.group_forwarded += 1,
            Disposition::Drop(reason) => {
                *self.counters.dropped.entry(*reason).or_default() += 1;
            }
        }
        decision
    }

    fn decide(&self, ingress: PortId, frame: &[u8]) -> Disposition {
        let Some(assigned) = self.ports.get(&ingress).copied() else {
            return Disposition::Drop(DropReason::UnknownIngressPort);
        };
        let Some(header) = FrameHeader::parse(frame) else {
            return Disposition::Drop(DropReason::ShortFrame);
        };
        if header.source.is_group() {
            return Disposition::Drop(DropReason::SourceIsGroupAddress);
        }
        if header.source != assigned {
            return Disposition::Drop(DropReason::SourceAddressNotAssigned);
        }
        if !PERMITTED_ETHERTYPES.contains(&header.ethertype) {
            return Disposition::Drop(DropReason::EthertypeNotPermitted);
        }
        if header.destination.is_group() {
            return Disposition::Group(
                self.ports
                    .keys()
                    .copied()
                    .filter(|port| *port != ingress)
                    .collect(),
            );
        }
        match self.by_address.get(&header.destination) {
            // A frame addressed back to its own sender is not delivery; it is a
            // loop, and the sender already has it.
            Some(port) if *port == ingress => Disposition::Drop(DropReason::DestinationUnknown),
            Some(port) => Disposition::Unicast(*port),
            None => Disposition::Drop(DropReason::DestinationUnknown),
        }
    }
}

pub mod runtime;

#[cfg(test)]
#[path = "environment_switch_tests.rs"]
mod tests;
