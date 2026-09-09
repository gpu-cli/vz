//! What the Environment's edge accepts, decided before the stack sees a byte.
//!
//! The edge is default-deny. Everything that reaches its port is one of four
//! things — an ARP exchange, a query to its resolver, a connection to its
//! ingress listener, or a reply on a translation it opened itself — and
//! anything else is dropped and counted with the rule that dropped it.
//!
//! It is stated as a filter in front of the stack rather than as the absence of
//! a listener, because those are not the same claim. A closed port answers with
//! a reset, which tells a guest that something is there and what it is not; a
//! dropped frame tells it nothing and, more importantly, means the stack never
//! parsed it. A criterion that asks for a firewall is asking for the first
//! thing, and a test can only see the difference if the rule exists somewhere
//! to be read.
//!
//! Two rules here are not about ports at all and matter more than the ones that
//! are. The edge accepts nothing addressed to anyone but itself, so there is no
//! transit through it and no Machine can use it as a route to a sibling it was
//! not declared a path to. And it accepts nothing sourced outside its own
//! network's range, so a guest cannot present an off-fabric source address to
//! an edge that translates addresses for a living.
//!
//! Nothing here does I/O and nothing here holds a socket: a frame is bytes in
//! and a verdict out.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use crate::environment_switch::HEADER_LEN as ETHERNET_HEADER_LEN;

const ETHERTYPE_IPV4: u16 = 0x0800;
const ETHERTYPE_ARP: u16 = 0x0806;
const PROTOCOL_TCP: u8 = 6;
const PROTOCOL_UDP: u8 = 17;

/// The resolver's port, and the ingress listener's.
pub const DNS_PORT: u16 = 53;
pub const INGRESS_PORT: u16 = 443;

/// The class one accepted frame belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Admitted {
    /// Address resolution for the edge's own address.
    Arp,
    /// A query to the Environment's resolver.
    Resolver,
    /// A connection to the edge's TLS ingress listener.
    Ingress,
    /// A reply on a translation the edge opened towards a declared origin.
    Translated,
}

/// Why a frame was refused. One variant per rule, so a counter names the rule
/// and not merely the fact that something was dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Refused {
    /// Not IPv4 and not ARP. The fabric already refuses these; the edge does
    /// not assume that and repeats the rule.
    UnsupportedEthertype,
    /// Too short, or a header that does not describe the bytes that follow.
    Malformed,
    /// A source address outside this network's range.
    ForeignSource,
    /// Addressed to something other than the edge. The edge is not a route.
    NotTheEdge,
    /// A fragment. A filter that read ports from the first fragment alone could
    /// be walked past by sending the rest.
    Fragmented,
    /// IPv4 carrying neither TCP nor UDP.
    UnservedProtocol,
    /// TCP or UDP to a port the edge does not serve.
    UnservedPort,
    /// A reply on a translated port, from an address that translation was never
    /// opened towards.
    UnexpectedOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Accept(Admitted),
    Drop(Refused),
}

/// Every rule the edge applies, and what applying them has done so far.
#[derive(Debug)]
pub struct Firewall {
    address: Ipv4Addr,
    network: u32,
    mask: u32,
    /// Whether the edge publishes an ingress listener at all. An Environment
    /// that declared a public-like network and no `https` endpoint has an edge
    /// that resolves names and accepts no connections, and the listener rule
    /// must not admit traffic to a listener that was never opened.
    ingress: bool,
    /// Ephemeral local ports the edge has opened towards a declared origin, and
    /// the origin each was opened towards. This is the translation table: the
    /// edge's reply path exists only for a flow the edge itself started.
    translated: BTreeMap<u16, Ipv4Addr>,
    accepted: BTreeMap<Admitted, u64>,
    refused: BTreeMap<Refused, u64>,
}

/// What the edge's filter did, reported with the running edge.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FirewallCounters {
    pub accepted: BTreeMap<Admitted, u64>,
    pub refused: BTreeMap<Refused, u64>,
}

impl Firewall {
    pub fn new(address: Ipv4Addr, prefix: u8, ingress: bool) -> Self {
        let mask = if prefix == 0 {
            0
        } else {
            u32::MAX << (32 - u32::from(prefix.min(32)))
        };
        Self {
            address,
            network: u32::from(address) & mask,
            mask,
            ingress,
            translated: BTreeMap::new(),
            accepted: BTreeMap::new(),
            refused: BTreeMap::new(),
        }
    }

    /// Record that the edge has opened a translation from `port` towards
    /// `origin`, so replies on it are admitted and nothing else on it is.
    pub fn open_translation(&mut self, port: u16, origin: Ipv4Addr) {
        self.translated.insert(port, origin);
    }

    /// Close one translation. A port that is no longer translated is refused
    /// again immediately, rather than staying open until something reuses it.
    pub fn close_translation(&mut self, port: u16) {
        self.translated.remove(&port);
    }

    pub fn counters(&self) -> FirewallCounters {
        FirewallCounters {
            accepted: self.accepted.clone(),
            refused: self.refused.clone(),
        }
    }

    /// Judge one Ethernet frame and count the judgement.
    pub fn judge(&mut self, frame: &[u8]) -> Verdict {
        let verdict = self.decide(frame);
        match verdict {
            Verdict::Accept(class) => *self.accepted.entry(class).or_default() += 1,
            Verdict::Drop(reason) => *self.refused.entry(reason).or_default() += 1,
        }
        verdict
    }

    fn decide(&self, frame: &[u8]) -> Verdict {
        if frame.len() < ETHERNET_HEADER_LEN {
            return Verdict::Drop(Refused::Malformed);
        }
        let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
        if ethertype == ETHERTYPE_ARP {
            return Verdict::Accept(Admitted::Arp);
        }
        if ethertype != ETHERTYPE_IPV4 {
            return Verdict::Drop(Refused::UnsupportedEthertype);
        }
        let packet = &frame[ETHERNET_HEADER_LEN..];
        if packet.len() < 20 || packet[0] >> 4 != 4 {
            return Verdict::Drop(Refused::Malformed);
        }
        let header_len = usize::from(packet[0] & 0x0f) * 4;
        let total_len = usize::from(u16::from_be_bytes([packet[2], packet[3]]));
        if header_len < 20 || packet.len() < header_len || total_len < header_len {
            return Verdict::Drop(Refused::Malformed);
        }
        // The frame may be padded to the Ethernet minimum, so it may be longer
        // than the packet claims; it may never be shorter, because then the
        // ports the rules below read are not the ports that will be delivered.
        if packet.len() < total_len {
            return Verdict::Drop(Refused::Malformed);
        }
        let flags_and_offset = u16::from_be_bytes([packet[6], packet[7]]);
        if flags_and_offset & 0x2000 != 0 || flags_and_offset & 0x1fff != 0 {
            return Verdict::Drop(Refused::Fragmented);
        }
        let source = Ipv4Addr::from([packet[12], packet[13], packet[14], packet[15]]);
        let destination = Ipv4Addr::from([packet[16], packet[17], packet[18], packet[19]]);
        if u32::from(source) & self.mask != self.network {
            return Verdict::Drop(Refused::ForeignSource);
        }
        if destination != self.address {
            return Verdict::Drop(Refused::NotTheEdge);
        }
        let protocol = packet[9];
        let payload = &packet[header_len..total_len];
        if payload.len() < 4 {
            return Verdict::Drop(Refused::Malformed);
        }
        let destination_port = u16::from_be_bytes([payload[2], payload[3]]);
        match protocol {
            PROTOCOL_UDP if destination_port == DNS_PORT => Verdict::Accept(Admitted::Resolver),
            PROTOCOL_TCP if destination_port == INGRESS_PORT && self.ingress => {
                Verdict::Accept(Admitted::Ingress)
            }
            PROTOCOL_TCP | PROTOCOL_UDP => match self.translated.get(&destination_port) {
                Some(origin) if *origin == source => Verdict::Accept(Admitted::Translated),
                Some(_) => Verdict::Drop(Refused::UnexpectedOrigin),
                None => Verdict::Drop(Refused::UnservedPort),
            },
            _ => Verdict::Drop(Refused::UnservedProtocol),
        }
    }
}
