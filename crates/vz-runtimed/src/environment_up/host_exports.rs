//! Resolve declared host exports into loopback-only port relays.
//!
//! An export is one Machine port made reachable from the host, and nothing
//! else. The product contract's hard rule is that a NAT alias or a wildcard
//! listener is never an authorization, so the design keeps the host destination
//! out of every declaration and out of every wire format:
//!
//! * `HostExportSpec` has no host-address field at all
//!   (`vz-runtime-contract/src/types/topology.rs:382-398`), so `host_port` only
//!   chooses which loopback port is bound, never which interface.
//! * The listener that carries an export is
//!   `crates/vz-oci-macos/src/runtime/networking.rs:92`, which binds
//!   `("127.0.0.1", host_port)` unconditionally. No code path in the workspace
//!   binds a wildcard or LAN address for a port relay.
//! * The guest half is `PortForwardOpen` with an EMPTY `target_service`, which
//!   `forward_grants::destination` resolves to the guest's own `127.0.0.1`
//!   (`crates/vz-guest-agent/src/forward_grants.rs:114-121`). The host never
//!   sends an address, so an export cannot be pointed at a third party even by
//!   a compromised host-side caller.
//!
//! What this module adds is the admission half: which declarations Up will
//! serve at all, and the join from persisted export identities back to the
//! declared ports, because `HostExportInstance` deliberately stores only
//! identity — "the bound loopback port is runtime state" (`topology.rs:756`).
//!
//! **Ordering.** Unlike a switch port or a VirtioFS share, a port relay is not
//! fixed at `LinuxVm::create`; `start_port_forwarding` starts it inside the boot
//! (`stack_vm.rs:1835`). So the mapping does not have to be minted before the
//! boot loop the way `install_environment_fabric` mints switch ports. What does
//! have to happen before the boot loop is the refusal of a declaration Up cannot
//! serve and the collision proof, because a Machine that booted before the
//! collision was discovered is an effect admitted for a request that then failed.

use std::collections::{BTreeMap, BTreeSet};

use vz_runtime_contract::{
    EnvironmentSpec, HostExportInstance, HostExportSpec, MachineId, MachineInstance,
    MachineProfile, OperatingSystem, PortMapping, PortProtocol, TransportProtocol,
};

/// Why a declared host export could not be applied.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HostExportError {
    #[error(
        "host export `{export}` requests a dynamically allocated loopback port, which this Up cannot report back to the caller; declare an exact `host_port`"
    )]
    DynamicPortUnsupported { export: String },
    #[error(
        "host export `{export}` names Machine `{machine}`, which the Environment does not declare"
    )]
    UnknownMachine { export: String, machine: String },
    #[error(
        "host export `{export}` names Machine `{machine}`, which is not a Developer Linux Machine; only a Developer Linux Machine carries the guest relay an export needs"
    )]
    UnsupportedMachine { export: String, machine: String },
    #[error(
        "host exports `{first}` and `{second}` both declare host loopback port {port}; one loopback port carries at most one export"
    )]
    DuplicateHostPort {
        first: String,
        second: String,
        port: u16,
    },
    #[error(
        "host loopback port {port} for export `{export}` is already held on this host: {reason}"
    )]
    HostPortUnavailable {
        export: String,
        port: u16,
        reason: String,
    },
    #[error(
        "persisted host export `{export}` has no matching declaration in the project definition"
    )]
    UndeclaredInstance { export: String },
    #[error("declared host export `{export}` has no persisted instance in this Environment")]
    MissingInstance { export: String },
    #[error(
        "persisted host export `{export}` names Machine id `{machine_id}`, which this Environment does not hold"
    )]
    UnknownInstanceMachine { export: String, machine_id: String },
}

/// One export joined from its persisted identity to its declared ports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedHostExport {
    /// The declared export name, carried so a collision names the declaration
    /// rather than an opaque identifier.
    pub name: String,
    pub machine_id: MachineId,
    pub mapping: PortMapping,
}

/// Refuse a declared export Up cannot serve, before any durable effect.
///
/// Called from `validate_supported`, so every rule here is proved on the
/// definition alone and mutates nothing. The rules are the ones whose violation
/// would otherwise produce a Machine that silently lacks the boundary its
/// definition asked for: a dynamic port nothing can report, a Machine with no
/// relay, and two exports fighting over one loopback port.
pub fn refuse_unsupported_host_exports(spec: &EnvironmentSpec) -> Result<(), HostExportError> {
    let mut by_host_port: BTreeMap<u16, &str> = BTreeMap::new();
    for export in &spec.host_exports {
        let Some(host_port) = export.host_port else {
            return Err(HostExportError::DynamicPortUnsupported {
                export: export.name.clone(),
            });
        };
        // `TransportProtocol` has exactly one variant and the JSON schema pins
        // `protocol` to `tcp`, so a non-TCP export is unconstructible rather
        // than refused. `protocol_is_relayable` states the dependency so that
        // adding UDP to the enum fails to compile here instead of silently
        // reaching a TCP-only relay. `host_export_protocol_is_tcp_only` pins it.
        let _: bool = protocol_is_relayable(export.protocol);
        let Some(machine) = spec
            .machines
            .iter()
            .find(|machine| machine.name == export.machine)
        else {
            return Err(HostExportError::UnknownMachine {
                export: export.name.clone(),
                machine: export.machine.clone(),
            });
        };
        // A native macOS Machine is booted with no ports at all: the native arm
        // of `boot_or_inspect_machine` takes no `PortMapping`
        // (`machine_runtime_activation.rs:205-226`). Hardened is the restricted
        // profile that declares none of this topology. Admitting either would
        // start a Machine whose declared export silently never exists.
        if machine.target.os != OperatingSystem::Linux
            || machine.profile != MachineProfile::Developer
        {
            return Err(HostExportError::UnsupportedMachine {
                export: export.name.clone(),
                machine: export.machine.clone(),
            });
        }
        if let Some(previous) = by_host_port.insert(host_port, export.name.as_str()) {
            return Err(HostExportError::DuplicateHostPort {
                first: previous.to_string(),
                second: export.name.clone(),
                port: host_port,
            });
        }
    }
    Ok(())
}

/// Whether the relay can carry this protocol.
///
/// The relay is `copy_bidirectional` over a `PortForward` stream, which is a
/// stream protocol; `start_port_forwarding` refuses anything else outright
/// (`crates/vz-oci-macos/src/runtime/networking.rs:82-90`).
const fn protocol_is_relayable(protocol: TransportProtocol) -> bool {
    match protocol {
        TransportProtocol::Tcp => true,
    }
}

/// Join persisted export identities to their declared ports.
///
/// The persisted instance is the authority on *which* export exists and *which*
/// Machine owns it, because that is what Delete accounts for. The definition is
/// the authority on the ports, because the instance deliberately does not store
/// them. A name present on one side and absent on the other is refused rather
/// than skipped: a skipped instance is an owned resource nothing reclaims, and a
/// skipped declaration is a boundary the caller asked for and did not get.
pub fn resolve_environment_host_exports(
    spec: &EnvironmentSpec,
    machines: &[MachineInstance],
    instances: &[HostExportInstance],
) -> Result<Vec<ResolvedHostExport>, HostExportError> {
    let declared: BTreeMap<&str, &HostExportSpec> = spec
        .host_exports
        .iter()
        .map(|export| (export.name.as_str(), export))
        .collect();
    let mut resolved = Vec::with_capacity(instances.len());
    for instance in instances {
        let Some(export) = declared.get(instance.name.as_str()) else {
            return Err(HostExportError::UndeclaredInstance {
                export: instance.name.clone(),
            });
        };
        let Some(host_port) = export.host_port else {
            return Err(HostExportError::DynamicPortUnsupported {
                export: export.name.clone(),
            });
        };
        // The instance's Machine and the declaration's Machine must be the same
        // Machine. `verify_environment_matches_definition` already refuses drift
        // between them; repeating it here keeps this join from depending on
        // having been called after that one.
        let Some(machine) = machines
            .iter()
            .find(|machine| machine.machine_id == instance.machine_id)
        else {
            return Err(HostExportError::UnknownInstanceMachine {
                export: instance.name.clone(),
                machine_id: instance.machine_id.to_string(),
            });
        };
        if machine.name != export.machine {
            return Err(HostExportError::UnknownMachine {
                export: export.name.clone(),
                machine: export.machine.clone(),
            });
        }
        resolved.push(ResolvedHostExport {
            name: export.name.clone(),
            machine_id: instance.machine_id.clone(),
            mapping: PortMapping {
                host: host_port,
                container: export.machine_port,
                protocol: match export.protocol {
                    TransportProtocol::Tcp => PortProtocol::Tcp,
                },
                // Never an address. An empty service is the guest's own
                // loopback, which is exactly what a Machine port is.
                target_service: None,
            },
        });
    }
    for export in &spec.host_exports {
        if !instances
            .iter()
            .any(|instance| instance.name == export.name)
        {
            return Err(HostExportError::MissingInstance {
                export: export.name.clone(),
            });
        }
    }
    // A stable order so two Ups of one definition present an identical boot
    // request; `require_matching_shared_vm_boot_request` compares them by value
    // and treats any drift as a conflict.
    resolved.sort_by(|left, right| {
        (&left.machine_id, left.mapping.host, left.mapping.container).cmp(&(
            &right.machine_id,
            right.mapping.host,
            right.mapping.container,
        ))
    });
    Ok(resolved)
}

/// Group resolved exports into the per-Machine boot argument.
pub fn boot_port_mappings(
    resolved: &[ResolvedHostExport],
) -> BTreeMap<MachineId, Vec<PortMapping>> {
    let mut grouped: BTreeMap<MachineId, Vec<PortMapping>> = BTreeMap::new();
    for export in resolved {
        grouped
            .entry(export.machine_id.clone())
            .or_default()
            .push(export.mapping.clone());
    }
    grouped
}

/// Prove every export port this Up will newly bind is free, before any boot.
///
/// `start_port_forwarding` binds inside the boot, so without this a collision
/// with a *sibling Environment* (which the definition cannot see, and so the
/// static duplicate check in `refuse_unsupported_host_exports` cannot catch) is
/// discovered only after an earlier Machine has already started.
///
/// The probe binds and immediately drops. That is exclusive on macOS: neither
/// `std::net::TcpListener::bind` nor tokio's wrapper sets `SO_REUSEPORT`, and
/// `SO_REUSEADDR` alone does not permit a second live TCP listener on the same
/// address and port. So a successful probe means no other live listener holds it.
///
/// Ports belonging to `already_booted` Machines are skipped, because those
/// listeners are held by this daemon's own live relay; probing them would fail a
/// re-Up of a running Environment against itself.
///
/// This proves a fact that can change, not a reservation: a foreign process may
/// take the port between the probe and the boot. That residual still fails
/// closed, because the real bind in `start_port_forwarding` then fails and the
/// boot fails with it. What the probe buys is that the ordinary collision fails
/// before any Machine of this Up has been started.
pub async fn probe_exportable_host_ports(
    resolved: &[ResolvedHostExport],
    already_booted: &BTreeSet<MachineId>,
) -> Result<(), HostExportError> {
    for export in resolved {
        if already_booted.contains(&export.machine_id) {
            continue;
        }
        match tokio::net::TcpListener::bind(("127.0.0.1", export.mapping.host)).await {
            Ok(listener) => drop(listener),
            Err(error) => {
                return Err(HostExportError::HostPortUnavailable {
                    export: export.name.clone(),
                    port: export.mapping.host,
                    reason: error.to_string(),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "host_exports/tests.rs"]
mod tests;
