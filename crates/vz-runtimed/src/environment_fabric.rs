//! Turning one Environment's planned fabric into running switches and the guest
//! ends its Machines boot attached to.
//!
//! This is the half that had no caller: `EnvironmentSwitches` could own a switch
//! and Stop and Delete could reclaim one, but nothing started one and nothing
//! minted a Machine a port, so every Machine booted with its default network
//! alone regardless of what its Environment declared.
//!
//! The ordering is not incidental. `NetworkSwitch::start` fixes a network's
//! membership when it is constructed, and a Machine's guest descriptor has to
//! exist before its VM is created, so the whole fabric is installed in one call
//! before the per-Machine boot loop rather than a port at a time inside it. That
//! is also why a Machine cannot be added to a network whose switch is already
//! running: there is no such operation, and pretending otherwise by restarting
//! the switch would disconnect every Machine already on it.

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;
use tracing::info;
use vz_oci_macos::{DeclaredAttachment, DeclaredHost, SharedVmAttachment};
use vz_runtime_contract::{EnvironmentInstance, MachineId, ResourceOwner};

use crate::RuntimeDaemon;
use crate::environment_gateway::{EnvironmentGateway, GatewayError};
use crate::environment_runtime_controller::EnvironmentControllerLease;
use crate::environment_switch::plan::{FABRIC_MTU, FabricPlanError, plan_environment_fabric};
use crate::environment_switch::registry::SwitchRegistryError;
use crate::environment_switch::runtime::{NetworkSwitch, SwitchError};

#[derive(Debug, Error)]
pub enum EnvironmentFabricError {
    #[error(transparent)]
    Plan(#[from] FabricPlanError),
    #[error(transparent)]
    Registry(#[from] SwitchRegistryError),
    #[error("network `{network}`: {error}")]
    Switch { network: String, error: SwitchError },
    #[error("network `{network}` port for Machine {machine_id}: {error}")]
    Port {
        network: String,
        machine_id: MachineId,
        error: vz_oci_macos::MacosOciError,
    },
    #[error("network `{network}` edge: {error}")]
    Gateway {
        network: String,
        error: GatewayError,
    },
    /// The Environment's running fabric is not the fabric it now needs, and no
    /// operation can reconcile the difference without disconnecting Machines.
    #[error("Environment fabric conflict: {0}")]
    Conflict(String),
}

fn conflict(message: impl Into<String>) -> EnvironmentFabricError {
    EnvironmentFabricError::Conflict(message.into())
}

/// Every guest end this Up minted, by the Machine that must boot holding it.
///
/// A Machine absent from the map has no declared attachment, or is already
/// running and already holds the ends it was minted.
pub(crate) type MintedAttachments = BTreeMap<MachineId, Vec<SharedVmAttachment>>;

/// Where an Environment edge's certificate authority is published, under the
/// daemon's runtime directory: `<root>/<environment id>/<network id>/authority.pem`.
pub const EDGE_ANCHOR_ROOT: &str = "environment-edges";

impl RuntimeDaemon {
    /// Start every switch this Environment's definition asks for and mint one
    /// port per attached Machine, before any Machine boots.
    ///
    /// `live` names the Machines this Up will reuse rather than boot. A running
    /// Machine already holds the guest end of its ports, and a port minted for
    /// it now would reach nothing, so the two admissible states are exact: the
    /// Environment has no switches and no attached Machine is running, in which
    /// case the whole fabric is created here; or the Environment already has
    /// exactly the switches this plan wants and every attached Machine is
    /// already running, in which case nothing is minted and the running fabric
    /// stands. Anything between those is a partially-established fabric that
    /// cannot be completed — a switch cannot gain a port after construction —
    /// and is refused with what it found rather than silently under-attached.
    pub(crate) async fn install_environment_fabric(
        &self,
        lease: &EnvironmentControllerLease,
        environment: &EnvironmentInstance,
        live: &BTreeSet<MachineId>,
    ) -> Result<MintedAttachments, EnvironmentFabricError> {
        let plan = plan_environment_fabric(environment)?;
        let owner = ResourceOwner {
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: None,
        };
        let installed: BTreeSet<String> = self
            .environment_switches
            .networks(&environment.environment_id)
            .await
            .into_iter()
            .collect();
        let wanted: BTreeSet<String> = plan
            .networks
            .iter()
            .map(|network| network.network_id.to_string())
            .collect();
        let attached = plan.attached_machines();
        let running: BTreeSet<&MachineId> =
            attached.iter().filter(|id| live.contains(id)).collect();

        if !installed.is_empty() {
            if installed != wanted {
                return Err(conflict(format!(
                    "this Environment already owns switches for {installed:?} but its definition now wants {wanted:?}; a running switch cannot be re-membered"
                )));
            }
            if running.len() != attached.len() {
                return Err(conflict(format!(
                    "this Environment's switches are already running, so no further port can be minted, but {} of its {} attached Machines are not",
                    attached.len() - running.len(),
                    attached.len()
                )));
            }
            return Ok(MintedAttachments::new());
        }
        if !running.is_empty() {
            return Err(conflict(format!(
                "{} attached Machines are already running without a fabric, and a running Machine cannot be given a switch port",
                running.len()
            )));
        }

        // Every Environment's trust anchors live under one root in the daemon's
        // own runtime directory, so an Environment's client can be handed the
        // one certificate that verifies its edge without the daemon publishing
        // anything else about the Environment.
        let anchor_root = self.runtime_data_dir().join(EDGE_ANCHOR_ROOT);
        let mut minted = MintedAttachments::new();
        for network in &plan.networks {
            let (switch, guests) =
                NetworkSwitch::start(&network.name, network.members()).map_err(|error| {
                    EnvironmentFabricError::Switch {
                        network: network.name.clone(),
                        error,
                    }
                })?;
            // Matched by port number rather than by position: the guest ends
            // come back as a list, and a mis-paired descriptor would give a
            // Machine a NIC the switch has assigned to a different address,
            // which the fabric would then drop every frame from.
            let mut guests: BTreeMap<_, _> = guests
                .into_iter()
                .map(|guest| (guest.port, guest.socket))
                .collect();
            // The edge's port is claimed before any Machine's, and by the
            // daemon rather than by a VM. It is the same kind of port on the
            // same switch; what differs is only who holds the guest end.
            let gateway = match &network.gateway {
                Some(planned) => {
                    let socket = guests.remove(&planned.port).ok_or_else(|| {
                        conflict(format!(
                            "switch for network `{}` returned no guest end for its edge port {}",
                            network.name, planned.port
                        ))
                    })?;
                    EnvironmentGateway::start(
                        environment.environment_id.as_str(),
                        network,
                        socket,
                        &anchor_root,
                    )
                    .map_err(|error| EnvironmentFabricError::Gateway {
                        network: network.name.clone(),
                        error,
                    })?
                }
                None => None,
            };
            let resolver = gateway.as_ref().map(EnvironmentGateway::address);
            for port in &network.ports {
                let socket = guests.remove(&port.port).ok_or_else(|| {
                    conflict(format!(
                        "switch for network `{}` returned no guest end for port {}",
                        network.name, port.port
                    ))
                })?;
                let attachment = SharedVmAttachment::new(
                    DeclaredAttachment {
                        network_id: network.network_id.to_string(),
                        mac: port.mac.to_string(),
                        ipv4: port.address,
                        prefix: network.cidr.prefix(),
                        // A private fabric has no route off itself, so there
                        // is no gateway to name and offset one stays empty. A
                        // public-like one names its edge, which is running by
                        // the time this descriptor is built: a guest is never
                        // pointed at an address nothing answers on.
                        gateway: resolver,
                        dns: resolver,
                        mtu: FABRIC_MTU,
                        // Every Machine on this network gets the same table,
                        // including the Machine that owns an endpoint: a service
                        // that reaches a sibling by its declared name must be
                        // able to reach itself by its own, or the name would
                        // mean one thing from outside and nothing from inside.
                        hosts: network
                            .hosts()
                            .into_iter()
                            .map(|(name, address)| DeclaredHost { name, address })
                            .collect(),
                    },
                    socket,
                )
                .map_err(|error| EnvironmentFabricError::Port {
                    network: network.name.clone(),
                    machine_id: port.machine_id.clone(),
                    error,
                })?;
                minted
                    .entry(port.machine_id.clone())
                    .or_default()
                    .push(attachment);
            }
            // Ownership last, and with the edge, because the two are
            // reclaimed as one: a switch installed before its edge existed
            // could be stopped by a concurrent Stop while the edge it does not
            // yet own kept running on a port that had gone.
            self.environment_switches
                .install(lease, &owner, network.network_id.as_str(), switch, gateway)
                .await?;
            info!(
                environment_id = %environment.environment_id,
                network = %network.name,
                network_id = %network.network_id,
                cidr = %network.cidr,
                ports = network.ports.len(),
                edge = ?resolver,
                "started Environment network switch"
            );
        }
        Ok(minted)
    }
}

#[cfg(test)]
#[path = "environment_fabric_tests.rs"]
mod tests;
