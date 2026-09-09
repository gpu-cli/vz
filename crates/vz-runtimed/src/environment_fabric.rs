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

        let mut minted = MintedAttachments::new();
        for network in &plan.networks {
            let (switch, guests) =
                NetworkSwitch::start(&network.name, network.members()).map_err(|error| {
                    EnvironmentFabricError::Switch {
                        network: network.name.clone(),
                        error,
                    }
                })?;
            // Ownership before the descriptors are handed out: a switch this
            // Environment does not own is one Stop and Delete cannot reclaim.
            self.environment_switches
                .install(lease, &owner, network.network_id.as_str(), switch)
                .await?;
            // Matched by port number rather than by position: the guest ends
            // come back as a list, and a mis-paired descriptor would give a
            // Machine a NIC the switch has assigned to a different address,
            // which the fabric would then drop every frame from.
            let mut guests: BTreeMap<_, _> = guests
                .into_iter()
                .map(|guest| (guest.port, guest.socket))
                .collect();
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
                        // is no gateway to name. The offset one address is
                        // reserved for the one `NetworkKind::SimulatedPublic`
                        // will need, but nothing occupies it until an egress
                        // path exists, and pointing a guest at an address no
                        // one answers on would be worse than no route at all.
                        gateway: None,
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
            info!(
                environment_id = %environment.environment_id,
                network = %network.name,
                network_id = %network.network_id,
                cidr = %network.cidr,
                ports = network.ports.len(),
                "started Environment network switch"
            );
        }
        Ok(minted)
    }
}

#[cfg(test)]
#[path = "environment_fabric_tests.rs"]
mod tests;
