//! Resolve declared host imports into authenticated per-Machine relays.
//!
//! An import is the mirror of an export and the harder half of criterion 7. An
//! export makes one Machine port reachable from the host; an import makes one
//! *host* loopback service reachable from one authorized Machine, and the
//! product contract's rule is:
//!
//! > Host imports require exact authenticated Environment/Machine grants to a
//! > declared host-loopback service, independently of external egress. NAT
//! > aliases and wildcard/LAN listeners are not authorization.
//!
//! This module is the admission and join half; the transport is
//! `vz_oci_macos`'s per-Machine vsock terminator and the guest agent's loopback
//! listeners. What matters here is which declarations Up will serve at all, and
//! how a persisted import identity is joined back to the ports it was declared
//! with — [`HostImportInstance`] deliberately stores only identity, exactly as
//! [`HostExportInstance`](vz_runtime_contract::HostExportInstance) does, because
//! the bound ports are runtime state.
//!
//! **Where the authorization lives.** Not in the port, and not in the address:
//!
//! * The relay port is one vsock port on one Machine's socket device. Reaching
//!   it proves which Machine you are and nothing else.
//! * Each declaration gets its own 32-byte credential, minted here, for one
//!   Machine and one boot. It is handed to that Machine's agent over its own
//!   private agent channel and never persisted, so a stopped Machine's
//!   credential cannot be replayed against its successor.
//! * The host destination never leaves the host. A grant's `host_port` is
//!   removed by [`vz::host_import::HostImportGrant::guest_view`] before the
//!   grant crosses to the guest, so no guest-selected host destination exists
//!   to be denied — the wire format cannot express one.
//!
//! **Ordering.** Like an export relay and unlike a switch port, an import relay
//! is not fixed at `LinuxVm::create`: it is installed after the boot, because
//! its guest half is an agent RPC and the agent is not running before the boot.
//! What must happen before the first boot is the refusal of a declaration Up
//! cannot serve, so that a Machine is never started for a request that then
//! fails.

use std::collections::{BTreeMap, BTreeSet};

use ring::rand::SecureRandom;
use vz::host_import::{CREDENTIAL_BYTES, HostImportGrant, MAX_NAME_BYTES};
use vz_runtime_contract::{
    EnvironmentSpec, HostImportInstance, HostImportSpec, MachineId, MachineInstance,
    MachineProfile, OperatingSystem, TransportProtocol,
};

/// Why a declared host import could not be applied.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HostImportError {
    #[error(
        "host import `{import}` names Machine `{machine}`, which the Environment does not declare"
    )]
    UnknownMachine { import: String, machine: String },
    #[error(
        "host import `{import}` names Machine `{machine}`, which is not a Developer Linux Machine; only a Developer Linux Machine carries the guest agent an import's loopback listener needs"
    )]
    UnsupportedMachine { import: String, machine: String },
    #[error(
        "host import `{import}` declares host port 0; an import terminates against an exact declared host-loopback service, and port 0 names none"
    )]
    ZeroHostPort { import: String },
    #[error(
        "host import `{import}` declares guest port 0; the guest would bind a kernel-chosen port nothing granted"
    )]
    ZeroGuestPort { import: String },
    #[error(
        "host import `{import}` has a {length}-byte name; the authenticated open frame carries at most {MAX_NAME_BYTES}"
    )]
    NameTooLong { import: String, length: usize },
    #[error("two host imports are named `{import}`; one name resolves to one host service")]
    DuplicateName { import: String },
    #[error(
        "host imports `{first}` and `{second}` both bind guest loopback port {port} on Machine `{machine}`; one guest port carries at most one import"
    )]
    DuplicateGuestPort {
        first: String,
        second: String,
        machine: String,
        port: u16,
    },
    #[error(
        "persisted host import `{import}` has no matching declaration in the project definition"
    )]
    UndeclaredInstance { import: String },
    #[error("declared host import `{import}` has no persisted instance in this Environment")]
    MissingInstance { import: String },
    #[error(
        "persisted host import `{import}` names Machine id `{machine_id}`, which this Environment does not hold"
    )]
    UnknownInstanceMachine { import: String, machine_id: String },
    #[error("host import `{import}` could not be given a credential: {reason}")]
    CredentialUnavailable { import: String, reason: String },
}

/// One import joined from its persisted identity to its declared ports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedHostImport {
    /// The declared name, carried so a refusal names the declaration rather
    /// than an opaque identifier. It is also what the open frame carries.
    pub name: String,
    pub machine_id: MachineId,
    /// Loopback port the guest agent binds inside that Machine.
    pub guest_port: u16,
    /// Loopback port on the host this import terminates against. Host-only:
    /// it is dropped before a grant crosses to the guest.
    pub host_port: u16,
}

/// Refuse a declared import Up cannot serve, before any durable effect.
///
/// Called from `validate_supported`, so every rule here is proved on the
/// definition alone and mutates nothing. The rules are the ones whose violation
/// would otherwise produce a Machine that silently lacks the boundary its
/// definition asked for, or a boundary wider than the one declared.
pub fn refuse_unsupported_host_imports(spec: &EnvironmentSpec) -> Result<(), HostImportError> {
    let mut names: BTreeSet<&str> = BTreeSet::new();
    let mut by_guest_port: BTreeMap<(&str, u16), &str> = BTreeMap::new();
    for import in &spec.host_imports {
        // `TransportProtocol` has exactly one variant and the JSON schema pins
        // `protocol` to `tcp`, so a non-TCP import is unconstructible rather
        // than refused. Stating the dependency makes adding UDP to the enum
        // fail to compile here instead of silently reaching a TCP-only relay.
        let _: bool = protocol_is_relayable(import.protocol);
        if import.name.len() > MAX_NAME_BYTES {
            return Err(HostImportError::NameTooLong {
                import: import.name.clone(),
                length: import.name.len(),
            });
        }
        if !names.insert(import.name.as_str()) {
            return Err(HostImportError::DuplicateName {
                import: import.name.clone(),
            });
        }
        if import.host_port == 0 {
            return Err(HostImportError::ZeroHostPort {
                import: import.name.clone(),
            });
        }
        let guest_port = guest_port_of(import);
        if guest_port == 0 {
            return Err(HostImportError::ZeroGuestPort {
                import: import.name.clone(),
            });
        }
        let Some(machine) = spec
            .machines
            .iter()
            .find(|machine| machine.name == import.machine)
        else {
            return Err(HostImportError::UnknownMachine {
                import: import.name.clone(),
                machine: import.machine.clone(),
            });
        };
        // A native macOS Machine's agent has no import listener, and Hardened is
        // the restricted profile that declares none of this topology. Admitting
        // either would start a Machine whose declared import silently never
        // exists — which is the failure mode this whole criterion is about.
        if machine.target.os != OperatingSystem::Linux
            || machine.profile != MachineProfile::Developer
        {
            return Err(HostImportError::UnsupportedMachine {
                import: import.name.clone(),
                machine: import.machine.clone(),
            });
        }
        // Two imports on one Machine's loopback port: the second would shadow
        // the first, and the guest process reaching that port would have no way
        // to tell which host service it got. Two imports on *different* Machines
        // may share a guest port, and two imports may share a host port: those
        // are two grants to one service, which is exactly what a declaration
        // per Machine means.
        if let Some(previous) =
            by_guest_port.insert((import.machine.as_str(), guest_port), import.name.as_str())
        {
            return Err(HostImportError::DuplicateGuestPort {
                first: previous.to_string(),
                second: import.name.clone(),
                machine: import.machine.clone(),
                port: guest_port,
            });
        }
    }
    Ok(())
}

/// The guest loopback port a declaration binds. Absent reuses `host_port`.
const fn guest_port_of(import: &HostImportSpec) -> u16 {
    match import.guest_port {
        Some(port) => port,
        None => import.host_port,
    }
}

/// Whether the relay can carry this protocol.
///
/// The relay is `copy_bidirectional` over a vsock stream, which is a stream
/// protocol.
const fn protocol_is_relayable(protocol: TransportProtocol) -> bool {
    match protocol {
        TransportProtocol::Tcp => true,
    }
}

/// Join persisted import identities to their declared ports.
///
/// The persisted instance is the authority on *which* import exists and *which*
/// Machine owns it, because that is what Delete accounts for. The definition is
/// the authority on the ports, because the instance deliberately does not store
/// them. A name present on one side and absent on the other is refused rather
/// than skipped: a skipped instance is an owned resource nothing reclaims, and
/// a skipped declaration is a boundary the caller asked for and did not get.
pub fn resolve_environment_host_imports(
    spec: &EnvironmentSpec,
    machines: &[MachineInstance],
    instances: &[HostImportInstance],
) -> Result<Vec<ResolvedHostImport>, HostImportError> {
    let declared: BTreeMap<&str, &HostImportSpec> = spec
        .host_imports
        .iter()
        .map(|import| (import.name.as_str(), import))
        .collect();
    let mut resolved = Vec::with_capacity(instances.len());
    for instance in instances {
        let Some(import) = declared.get(instance.name.as_str()) else {
            return Err(HostImportError::UndeclaredInstance {
                import: instance.name.clone(),
            });
        };
        if import.host_port == 0 {
            return Err(HostImportError::ZeroHostPort {
                import: import.name.clone(),
            });
        }
        // The instance's Machine and the declaration's Machine must be the same
        // Machine. `verify_environment_matches_definition` already refuses drift
        // between them; repeating it here keeps this join from depending on
        // having been called after that one — and this is the join that decides
        // which Machine's relay a host service becomes reachable from.
        let Some(machine) = machines
            .iter()
            .find(|machine| machine.machine_id == instance.machine_id)
        else {
            return Err(HostImportError::UnknownInstanceMachine {
                import: instance.name.clone(),
                machine_id: instance.machine_id.to_string(),
            });
        };
        if machine.name != import.machine {
            return Err(HostImportError::UnknownMachine {
                import: import.name.clone(),
                machine: import.machine.clone(),
            });
        }
        resolved.push(ResolvedHostImport {
            name: import.name.clone(),
            machine_id: instance.machine_id.clone(),
            guest_port: guest_port_of(import),
            host_port: import.host_port,
        });
    }
    for import in &spec.host_imports {
        if !instances
            .iter()
            .any(|instance| instance.name == import.name)
        {
            return Err(HostImportError::MissingInstance {
                import: import.name.clone(),
            });
        }
    }
    // A stable order so two Ups of one definition present an identical
    // installation request.
    resolved.sort_by(|left, right| {
        (
            &left.machine_id,
            left.guest_port,
            left.host_port,
            &left.name,
        )
            .cmp(&(
                &right.machine_id,
                right.guest_port,
                right.host_port,
                &right.name,
            ))
    });
    Ok(resolved)
}

/// Mint one boot's grants, grouped by the Machine that will hold them.
///
/// The credential is minted here and nowhere else, from the OS CSPRNG, and is
/// never persisted. Two consequences are deliberate:
///
/// * a stopped Machine's credential cannot be replayed against the Machine that
///   replaces it, because the replacement's credentials are new; and
/// * two Machines that declare the same import *name* hold different secrets,
///   so one Machine's credential cannot open the other's grant even though the
///   name matches.
pub fn boot_import_grants(
    resolved: &[ResolvedHostImport],
) -> Result<BTreeMap<MachineId, Vec<HostImportGrant>>, HostImportError> {
    let random = ring::rand::SystemRandom::new();
    let mut grouped: BTreeMap<MachineId, Vec<HostImportGrant>> = BTreeMap::new();
    for import in resolved {
        let mut credential = [0u8; CREDENTIAL_BYTES];
        random
            .fill(&mut credential)
            .map_err(|error| HostImportError::CredentialUnavailable {
                import: import.name.clone(),
                reason: error.to_string(),
            })?;
        grouped
            .entry(import.machine_id.clone())
            .or_default()
            .push(HostImportGrant {
                name: import.name.clone(),
                guest_port: import.guest_port,
                host_port: import.host_port,
                credential,
            });
    }
    Ok(grouped)
}

#[cfg(test)]
#[path = "host_imports/tests.rs"]
mod tests;
