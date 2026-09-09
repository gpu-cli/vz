//! What one Environment's declared networks become, decided before any socket.
//!
//! `NetworkSwitch::start` fixes a network's membership at construction and the
//! guest end of every port must exist before its Machine's VM is created, so
//! the whole fabric has to be decided in one pass over the persisted
//! Environment rather than discovered Machine by Machine during the boot loop.
//! This module is that pass. It performs no I/O, opens no socket and starts no
//! task, so every rule below is decided on persisted records in tests rather
//! than inferred from a running Environment.
//!
//! Addresses are derived, never leased. There is no DHCP server and no lease
//! state, for the same reason the MAC derivation exists: a Machine that stops
//! and comes back up must present the address its switch already expects and
//! the address its saved guest already believes it has, and a fresh assignment
//! on every boot cannot do that. Every address here is a pure function of
//! identifiers that outlive the Machine — the Environment, the network and the
//! attachment — so the same persisted Environment plans to the same fabric on
//! every Up.

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use sha2::{Digest, Sha256};
use thiserror::Error;
use vz_runtime_contract::{
    EndpointId, EnvironmentInstance, MachineId, MachineInstance, MachineProfile,
    NetworkAttachmentId, NetworkId, NetworkKind, OperatingSystem,
};

use super::{MacAddress, PortId};

/// Domain separators, so neither derivation can collide with the other or with
/// the MAC derivation over the same identifiers.
const SUBNET_DERIVATION_DOMAIN: &[u8] = b"vz.environment.network.subnet.v1\n";
const HOST_DERIVATION_DOMAIN: &[u8] = b"vz.environment.network.attachment.host.v1\n";

/// The MTU every fabric port is sized for. One value for the whole fabric,
/// because a port whose MTU disagrees with its peers drops full-sized frames on
/// one path only, which reads as an application fault rather than a
/// configuration one.
pub const FABRIC_MTU: u32 = 1500;

/// Offset 0 is the subnet address and the last offset is its broadcast address;
/// neither can be a host. Offset 1 is reserved and never assigned to a Machine:
/// `NetworkKind::SimulatedPublic` is defined as this private fabric plus
/// external egress, which will need a per-Environment gateway on the fabric,
/// and a gateway that had to take an offset already assigned would move a
/// Machine's address — exactly what deriving rather than leasing exists to
/// prevent.
const GATEWAY_OFFSET: u32 = 1;
const FIRST_HOST_OFFSET: u32 = GATEWAY_OFFSET + 1;

/// A network that declares no CIDR gets a /24 derived inside this block. RFC1918
/// space, and a /24 because the fabric is bounded by the 128-Machine Environment
/// limit long before it is bounded by addresses.
const DERIVED_SUBNET_PREFIX: u8 = 24;
const DERIVED_SUBNET_BLOCK: Ipv4Cidr = Ipv4Cidr {
    base: u32::from_be_bytes([10, 0, 0, 0]),
    prefix: 8,
};
/// How many derived /24s the probe will try before giving up. The whole block,
/// so exhaustion means the Environment really has no room rather than that the
/// probe was short.
const DERIVED_SUBNET_PROBE_LIMIT: u32 = 1 << 16;

/// Prefix lengths a fabric range may have. Shorter than /8 is more address
/// space than the 128-Machine Environment bound could ever use and makes the
/// range arithmetic overflow-prone; longer than /30 leaves no offset for a
/// Machine once the subnet, gateway and broadcast addresses are set aside.
const MIN_PREFIX: u8 = 8;
const MAX_PREFIX: u8 = 30;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FabricPlanError {
    #[error(
        "network `{network}` is SimulatedPublic, which is this private fabric plus external egress; no egress path off an Environment fabric exists yet, so it cannot be applied"
    )]
    EgressNotImplemented { network: String },
    #[error("network `{network}` declares cidr `{cidr}`: {reason}")]
    InvalidCidr {
        network: String,
        cidr: String,
        reason: String,
    },
    #[error(
        "networks `{first}` and `{second}` cover overlapping address ranges, so a Machine on both could not route between them"
    )]
    OverlappingNetworks { first: String, second: String },
    #[error("network `{network}` has no free address range left to derive")]
    SubnetExhausted { network: String },
    #[error(
        "network `{network}` has room for {capacity} Machines but {attached} are attached to it"
    )]
    NetworkTooSmall {
        network: String,
        capacity: u32,
        attached: usize,
    },
    #[error("attachment `{attachment}` names {kind} `{id}`, which this Environment does not have")]
    DanglingAttachment {
        attachment: String,
        kind: &'static str,
        id: String,
    },
    #[error(
        "Machine `{machine}` is a {profile:?} {os:?} Machine and cannot hold an Environment-network port"
    )]
    UnsupportedMachine {
        machine: String,
        profile: MachineProfile,
        os: OperatingSystem,
    },
    #[error("endpoint `{endpoint}` names {kind} `{id}`, which this Environment does not have")]
    DanglingEndpoint {
        endpoint: String,
        kind: &'static str,
        id: String,
    },
    #[error(
        "endpoint `{endpoint}` is on network `{network}`, but its Machine has no port on that network, so the endpoint has no address to resolve to"
    )]
    UnattachedEndpoint { endpoint: String, network: String },
    #[error(
        "endpoints `{first}` and `{second}` both resolve the name `{name}`, which would make it resolve to two different Machines"
    )]
    AmbiguousEndpointName {
        name: String,
        first: String,
        second: String,
    },
}

/// An IPv4 network range: the base address and its prefix length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ipv4Cidr {
    base: u32,
    prefix: u8,
}

impl Ipv4Cidr {
    /// Read `A.B.C.D/len`, refusing anything a fabric cannot address.
    ///
    /// The base must have its host bits clear, because `10.0.0.5/24` names a
    /// host and not a range and silently rounding it to `10.0.0.0/24` would
    /// accept two different declarations as the same network.
    pub fn parse(text: &str) -> Result<Self, String> {
        let (address, prefix) = text
            .split_once('/')
            .ok_or_else(|| "expected `A.B.C.D/len`".to_string())?;
        let address: Ipv4Addr = address
            .parse()
            .map_err(|_| format!("`{address}` is not an IPv4 address"))?;
        let prefix: u8 = prefix
            .parse()
            .map_err(|_| format!("`{prefix}` is not a prefix length"))?;
        if !(MIN_PREFIX..=MAX_PREFIX).contains(&prefix) {
            return Err(format!(
                "prefix /{prefix} is outside /{MIN_PREFIX}..=/{MAX_PREFIX}; shorter ranges are larger than any Environment can use and longer ones leave no assignable host address"
            ));
        }
        let base = u32::from(address);
        let host_mask = host_mask(prefix);
        if base & host_mask != 0 {
            return Err(format!(
                "`{address}` has host bits set; declare the range base address"
            ));
        }
        Ok(Self { base, prefix })
    }

    /// How wide this range is. A port carries this alongside its own address,
    /// because an address without its prefix does not tell the guest which
    /// peers are on-link.
    pub const fn prefix(&self) -> u8 {
        self.prefix
    }

    /// How many addresses in this range may be assigned to a Machine.
    pub const fn host_capacity(&self) -> u32 {
        // A /30 holds four addresses: subnet, gateway, one host, broadcast.
        (host_mask(self.prefix) + 1) - FIRST_HOST_OFFSET - 1
    }

    /// The address at `offset` within this range.
    const fn address_at(&self, offset: u32) -> Ipv4Addr {
        Ipv4Addr::from_bits(self.base | (offset & host_mask(self.prefix)))
    }

    /// Whether two ranges cover any address in common.
    const fn overlaps(&self, other: &Self) -> bool {
        let mask = if self.prefix <= other.prefix {
            !host_mask(self.prefix)
        } else {
            !host_mask(other.prefix)
        };
        self.base & mask == other.base & mask
    }
}

impl std::fmt::Display for Ipv4Cidr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}/{}",
            Ipv4Addr::from_bits(self.base),
            self.prefix
        )
    }
}

const fn host_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        u32::MAX
    } else {
        u32::MAX >> prefix
    }
}

/// One Machine's port on one network, decided but not yet created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricPort {
    pub port: PortId,
    pub attachment_id: NetworkAttachmentId,
    pub machine_id: MachineId,
    pub mac: MacAddress,
    /// The host address this attachment holds on its network. Nothing configures
    /// it in the guest yet; the kernel-cmdline channel that does is a separate
    /// adapter, and this is the value it will carry.
    pub address: Ipv4Addr,
}

/// One declared endpoint, resolved to the address it names.
///
/// An endpoint is declaration and resolution only: this says which name answers
/// with which address, and says nothing about whether anything is listening on
/// the port behind it. Nothing here binds a listener, probes the port or waits
/// for one, so an Up that produced this cannot be read as evidence that the
/// service exists yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricEndpoint {
    pub endpoint_id: EndpointId,
    /// The name this endpoint answers to, already defaulted.
    pub name: String,
    /// The fabric address of the Machine that owns the endpoint, taken from
    /// that Machine's port on this same network rather than derived a second
    /// time — one derivation, so a name cannot resolve to an address the switch
    /// does not actually forward to.
    pub address: Ipv4Addr,
}

/// One network's switch, its range, every port it will be constructed with, and
/// every name that resolves on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkPlan {
    pub network_id: NetworkId,
    pub name: String,
    pub cidr: Ipv4Cidr,
    pub ports: Vec<FabricPort>,
    /// The endpoints declared on this network, in resolved-name order.
    ///
    /// Endpoints hang off the network rather than off the Environment because
    /// reachability does: a Machine with no port on this network cannot reach
    /// any address in this range, and resolving a name to an address that
    /// Machine provably cannot route to would turn a clear "unknown host" into
    /// a connection that hangs. So a name is published to exactly the Machines
    /// that hold a port here.
    pub endpoints: Vec<FabricEndpoint>,
}

impl NetworkPlan {
    /// The membership `NetworkSwitch::start` is constructed with.
    pub fn members(&self) -> Vec<(PortId, MacAddress)> {
        self.ports
            .iter()
            .map(|port| (port.port, port.mac))
            .collect()
    }

    /// The `name -> address` pairs a Machine on this network resolves.
    pub fn hosts(&self) -> Vec<(String, Ipv4Addr)> {
        self.endpoints
            .iter()
            .map(|endpoint| (endpoint.name.clone(), endpoint.address))
            .collect()
    }
}

/// Every switch one Environment needs, in a fixed order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FabricPlan {
    pub networks: Vec<NetworkPlan>,
}

impl FabricPlan {
    pub fn is_empty(&self) -> bool {
        self.networks.is_empty()
    }

    /// Every Machine that holds at least one port in this plan.
    pub fn attached_machines(&self) -> BTreeSet<MachineId> {
        self.networks
            .iter()
            .flat_map(|network| network.ports.iter().map(|port| port.machine_id.clone()))
            .collect()
    }
}

/// Decide the whole fabric of one persisted Environment.
///
/// Networks are visited in `network_id` order and each network's ports in
/// `attachment_id` order, so the plan depends only on what is persisted and
/// never on the order the definition happened to list things in.
pub fn plan_environment_fabric(
    environment: &EnvironmentInstance,
) -> Result<FabricPlan, FabricPlanError> {
    let mut networks: Vec<_> = environment.networks.iter().collect();
    networks.sort_by(|left, right| left.network_id.as_str().cmp(right.network_id.as_str()));
    for network in &networks {
        if network.kind == NetworkKind::SimulatedPublic {
            return Err(FabricPlanError::EgressNotImplemented {
                network: network.name.clone(),
            });
        }
    }

    let machines: BTreeMap<&str, &MachineInstance> = environment
        .machines
        .iter()
        .map(|machine| (machine.machine_id.as_str(), machine))
        .collect();
    let network_ids: BTreeSet<&str> = networks
        .iter()
        .map(|network| network.network_id.as_str())
        .collect();
    let mut attachments_by_network: BTreeMap<&str, Vec<_>> = BTreeMap::new();
    for attachment in &environment.network_attachments {
        let Some(machine) = machines.get(attachment.machine_id.as_str()) else {
            return Err(FabricPlanError::DanglingAttachment {
                attachment: attachment.attachment_id.to_string(),
                kind: "Machine",
                id: attachment.machine_id.to_string(),
            });
        };
        // Only a Developer Linux Machine can hold a fabric port: a Hardened or
        // native-target Machine has no NIC a switch could attach to. The
        // topology contract already refuses such a declaration; refusing it
        // again here keeps the runtime independently safe rather than trusting
        // that every persisted record passed through that validation.
        if machine.profile != MachineProfile::Developer
            || machine.target.os != OperatingSystem::Linux
        {
            return Err(FabricPlanError::UnsupportedMachine {
                machine: machine.name.clone(),
                profile: machine.profile,
                os: machine.target.os,
            });
        }
        if !network_ids.contains(attachment.network_id.as_str()) {
            return Err(FabricPlanError::DanglingAttachment {
                attachment: attachment.attachment_id.to_string(),
                kind: "network",
                id: attachment.network_id.to_string(),
            });
        }
        attachments_by_network
            .entry(attachment.network_id.as_str())
            .or_default()
            .push(attachment);
    }

    let ranges = resolve_ranges(environment, &networks)?;

    let mut planned = Vec::with_capacity(networks.len());
    for network in networks {
        let cidr = ranges[network.network_id.as_str()];
        let mut attachments = attachments_by_network
            .remove(network.network_id.as_str())
            .unwrap_or_default();
        attachments.sort_by(|left, right| {
            left.attachment_id
                .as_str()
                .cmp(right.attachment_id.as_str())
        });
        let capacity = cidr.host_capacity();
        if u64::try_from(attachments.len()).unwrap_or(u64::MAX) > u64::from(capacity) {
            return Err(FabricPlanError::NetworkTooSmall {
                network: network.name.clone(),
                capacity,
                attached: attachments.len(),
            });
        }
        let mut taken = BTreeSet::new();
        let mut ports = Vec::with_capacity(attachments.len());
        for (index, attachment) in attachments.iter().enumerate() {
            let offset = assign_host_offset(
                environment.environment_id.as_str(),
                network.network_id.as_str(),
                attachment.attachment_id.as_str(),
                capacity,
                &mut taken,
            )
            .ok_or_else(|| FabricPlanError::NetworkTooSmall {
                network: network.name.clone(),
                capacity,
                attached: attachments.len(),
            })?;
            ports.push(FabricPort {
                // Port numbers are dense and ordered rather than derived: they
                // are private to one switch's forwarding table and never leave
                // this process, so nothing outside needs them to be stable.
                port: PortId(u32::try_from(index).unwrap_or(u32::MAX)),
                attachment_id: attachment.attachment_id.clone(),
                machine_id: attachment.machine_id.clone(),
                mac: MacAddress::derive(
                    environment.environment_id.as_str(),
                    attachment.machine_id.as_str(),
                    network.network_id.as_str(),
                ),
                address: cidr.address_at(offset),
            });
        }
        planned.push(NetworkPlan {
            network_id: network.network_id.clone(),
            name: network.name.clone(),
            cidr,
            ports,
            endpoints: Vec::new(),
        });
    }
    resolve_endpoints(environment, &mut planned)?;
    Ok(FabricPlan { networks: planned })
}

/// Give every declared endpoint the address of the Machine that owns it.
///
/// This runs after the ports are decided rather than beside them because an
/// endpoint resolves to a port's address and cannot be answered before that
/// address exists. It adds no address of its own: an endpoint whose Machine has
/// no port on the endpoint's network is refused rather than given one, because
/// minting an address here would put a Machine on a network its declaration
/// never attached it to.
fn resolve_endpoints(
    environment: &EnvironmentInstance,
    planned: &mut [NetworkPlan],
) -> Result<(), FabricPlanError> {
    let mut endpoints: Vec<_> = environment.endpoints.iter().collect();
    endpoints.sort_by(|left, right| left.endpoint_id.as_str().cmp(right.endpoint_id.as_str()));

    // One name, one address, per Environment. Two endpoints that resolve the
    // same name are refused rather than ordered, for the same reason two
    // attachments may not hold one address: whichever line of `/etc/hosts` a
    // resolver happened to read first would decide which Machine the name meant,
    // and the declaration would have said nothing about which that is.
    let mut claimed: BTreeMap<&str, &str> = BTreeMap::new();
    let mut resolved: BTreeMap<usize, Vec<FabricEndpoint>> = BTreeMap::new();
    for endpoint in endpoints {
        let name = endpoint.resolved_hostname();
        if let Some(first) = claimed.insert(name, endpoint.name.as_str()) {
            return Err(FabricPlanError::AmbiguousEndpointName {
                name: name.to_string(),
                first: first.to_string(),
                second: endpoint.name.clone(),
            });
        }
        let Some((index, network)) = planned
            .iter()
            .enumerate()
            .find(|(_, network)| network.network_id == endpoint.network_id)
        else {
            return Err(FabricPlanError::DanglingEndpoint {
                endpoint: endpoint.name.clone(),
                kind: "network",
                id: endpoint.network_id.to_string(),
            });
        };
        if !environment
            .machines
            .iter()
            .any(|machine| machine.machine_id == endpoint.machine_id)
        {
            return Err(FabricPlanError::DanglingEndpoint {
                endpoint: endpoint.name.clone(),
                kind: "Machine",
                id: endpoint.machine_id.to_string(),
            });
        }
        let port = network
            .ports
            .iter()
            .find(|port| port.machine_id == endpoint.machine_id)
            .ok_or_else(|| FabricPlanError::UnattachedEndpoint {
                endpoint: endpoint.name.clone(),
                network: network.name.clone(),
            })?;
        resolved.entry(index).or_default().push(FabricEndpoint {
            endpoint_id: endpoint.endpoint_id.clone(),
            name: name.to_string(),
            address: port.address,
        });
    }

    for (index, network) in planned.iter_mut().enumerate() {
        let mut network_endpoints = resolved.remove(&index).unwrap_or_default();
        network_endpoints.sort_by(|left, right| left.name.cmp(&right.name));
        network.endpoints = network_endpoints;
    }
    Ok(())
}

/// Give every network a range: the declared one where there is one, otherwise a
/// derived one that overlaps nothing already resolved.
///
/// Declared ranges are resolved first and as a whole, so a derived range can
/// never be handed a block a later declaration also claims. Two declared ranges
/// that overlap are refused rather than probed away: the definition asked for
/// both, and quietly moving one would hide the contradiction.
fn resolve_ranges<'network>(
    environment: &EnvironmentInstance,
    networks: &[&'network vz_runtime_contract::NetworkInstance],
) -> Result<BTreeMap<&'network str, Ipv4Cidr>, FabricPlanError> {
    let mut resolved: BTreeMap<&str, Ipv4Cidr> = BTreeMap::new();
    let mut taken: Vec<(&str, Ipv4Cidr)> = Vec::new();
    for network in networks {
        let Some(declared) = network.cidr.as_deref() else {
            continue;
        };
        let cidr = Ipv4Cidr::parse(declared).map_err(|reason| FabricPlanError::InvalidCidr {
            network: network.name.clone(),
            cidr: declared.to_string(),
            reason,
        })?;
        if let Some((other, _)) = taken.iter().find(|(_, other)| other.overlaps(&cidr)) {
            return Err(FabricPlanError::OverlappingNetworks {
                first: (*other).to_string(),
                second: network.name.clone(),
            });
        }
        taken.push((network.name.as_str(), cidr));
        resolved.insert(network.network_id.as_str(), cidr);
    }
    for network in networks {
        if network.cidr.is_some() {
            continue;
        }
        let seed = derive_u32(
            SUBNET_DERIVATION_DOMAIN,
            &[
                environment.environment_id.as_str(),
                network.network_id.as_str(),
            ],
        );
        let span = DERIVED_SUBNET_PROBE_LIMIT;
        let base = seed % span;
        let mut chosen = None;
        for probe in 0..span {
            let candidate = Ipv4Cidr {
                base: DERIVED_SUBNET_BLOCK.base
                    | (((base + probe) % span) << (32 - DERIVED_SUBNET_PREFIX)),
                prefix: DERIVED_SUBNET_PREFIX,
            };
            if !taken.iter().any(|(_, other)| other.overlaps(&candidate)) {
                chosen = Some(candidate);
                break;
            }
        }
        let candidate = chosen.ok_or_else(|| FabricPlanError::SubnetExhausted {
            network: network.name.clone(),
        })?;
        taken.push((network.name.as_str(), candidate));
        resolved.insert(network.network_id.as_str(), candidate);
    }
    Ok(resolved)
}

/// The host offset one attachment holds, derived and then probed forward.
///
/// The derivation alone cannot be the answer: two attachments on one network
/// can hash into the same offset, and handing both the same address would make
/// delivery ambiguous in exactly the way the fabric refuses. The rule is a
/// linear probe forward from the derived offset, wrapping within the range, over
/// attachments visited in `attachment_id` order. That keeps the result a
/// function of the persisted records alone: the same Environment yields the same
/// assignment on every Up, and an attachment whose derived offset is free — the
/// ordinary case — is never moved by a sibling.
///
/// `None` means every offset in the range is already assigned, which the caller
/// has already refused by comparing the attachment count against the capacity;
/// it is returned rather than asserted so this function stays total.
fn assign_host_offset(
    environment_id: &str,
    network_id: &str,
    attachment_id: &str,
    capacity: u32,
    taken: &mut BTreeSet<u32>,
) -> Option<u32> {
    let seed = derive_u32(
        HOST_DERIVATION_DOMAIN,
        &[environment_id, network_id, attachment_id],
    );
    let base = seed % capacity;
    (0..capacity)
        .map(|probe| FIRST_HOST_OFFSET + ((base + probe) % capacity))
        .find(|offset| taken.insert(*offset))
}

/// A domain-separated, length-prefixed digest of identifiers, reduced to 32 bits.
///
/// Length-prefixed for the same reason the MAC derivation is: no pair of
/// identifiers may be re-split into another pair that hashes the same and hands
/// two attachments one address.
fn derive_u32(domain: &[u8], fields: &[&str]) -> u32 {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for field in fields {
        hasher.update(u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(field.as_bytes());
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&digest[..4]);
    u32::from_be_bytes(bytes)
}

#[cfg(test)]
#[path = "plan_tests.rs"]
mod tests;
