//! Which Environment owns which running switch.
//!
//! A switch is not a free-standing task: it belongs to exactly one Environment,
//! it must be stopped before that Environment's Machines are released, and no
//! other Environment may reach it. This registry is where that ownership lives,
//! following the same discipline as the per-Machine Docker endpoint: every
//! mutation is fenced by the Environment lease that authorized it, the registry
//! binds to one controller for its life, and teardown is joined and reported
//! rather than aborted.
//!
//! A switch is keyed by Environment and network, because an Environment may
//! declare several networks and a Machine may be on more than one of them.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use thiserror::Error;
use tokio::sync::Mutex;
use vz_runtime_contract::{EnvironmentId, ResourceOwner};

use super::runtime::{GuestPort, NetworkSwitch, SwitchShutdown};
use super::{MacAddress, PortId};
use crate::environment_gateway::{EdgeShutdown, EnvironmentGateway};
use crate::environment_runtime_controller::EnvironmentControllerLease;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SwitchRegistryError {
    #[error("switch registry: {0}")]
    Conflict(String),
}

fn conflict(message: impl Into<String>) -> SwitchRegistryError {
    SwitchRegistryError::Conflict(message.into())
}

/// What stopping one Environment's switches did, network by network.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnvironmentSwitchShutdown {
    pub networks: BTreeMap<String, SwitchShutdown>,
    /// What each network's edge decided while it ran, for the networks that had
    /// one. Reported beside the switch's own counters rather than folded into
    /// them: a frame the edge's filter refused never reached the switch, and a
    /// frame the switch refused never reached the edge, so one number could not
    /// be attributed to either.
    pub edges: BTreeMap<String, EdgeShutdown>,
}

#[derive(Default)]
struct Registry {
    /// Bound on first use, so a second controller cannot adopt switches this
    /// daemon's controller established.
    controller: Option<std::sync::Arc<()>>,
    switches: BTreeMap<EnvironmentId, BTreeMap<String, Installed>>,
}

/// One network's switch and, where the network declared one, its edge.
///
/// They are installed and reclaimed together because the edge holds the guest
/// end of one of that switch's ports: a switch stopped while its edge still ran
/// would present to the edge as a network that silently stopped forwarding,
/// and an edge left running after its switch is gone owns a socket whose peer
/// no longer exists.
struct Installed {
    switch: NetworkSwitch,
    gateway: Option<EnvironmentGateway>,
}

/// Every Environment's running switches, owned by the daemon.
#[derive(Default)]
pub struct EnvironmentSwitches {
    registry: Mutex<Registry>,
}

impl EnvironmentSwitches {
    /// Place a started switch under its Environment's ownership.
    ///
    /// Refuses a second switch for the same network, because two switches on one
    /// network would each hold half its ports and neither would forward between
    /// them, which looks like packet loss rather than like the mistake it is.
    pub async fn install(
        &self,
        lease: &EnvironmentControllerLease,
        owner: &ResourceOwner,
        network_id: &str,
        switch: NetworkSwitch,
        gateway: Option<EnvironmentGateway>,
    ) -> Result<(), SwitchRegistryError> {
        lease
            .require_owner(owner)
            .map_err(|error| conflict(error.to_string()))?;
        require_named(network_id)?;
        let mut registry = self.registry.lock().await;
        bind_controller(&mut registry.controller, lease.controller_identity())?;
        let networks = registry
            .switches
            .entry(owner.environment_id.clone())
            .or_default();
        if networks.contains_key(network_id) {
            return Err(conflict(format!(
                "network `{network_id}` already has a switch in this Environment"
            )));
        }
        networks.insert(network_id.to_string(), Installed { switch, gateway });
        Ok(())
    }

    /// The networks this Environment currently has a switch for.
    pub async fn networks(&self, environment_id: &EnvironmentId) -> Vec<String> {
        self.registry
            .lock()
            .await
            .switches
            .get(environment_id)
            .map(|networks| networks.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Stop every switch this Environment owns and report what each did.
    ///
    /// Removing them from the registry before joining is what makes this safe to
    /// call once: a second call finds nothing and reports nothing, rather than
    /// joining a task that is already gone.
    /// Attach one port to a network's switch that is already forwarding, and
    /// report the edge address that network resolves through.
    ///
    /// This is how a Machine joins an Environment whose fabric is already up --
    /// a fork, whose parent is running and holding its own port. The guest end
    /// comes back for that Machine's NIC; the edge address comes back with it
    /// because the caller has to build the same attachment descriptor the fresh
    /// path builds, and a Machine pointed at a gateway address different from
    /// its siblings' would be on the same wire with a different idea of how to
    /// leave it.
    pub async fn add_port(
        &self,
        lease: &EnvironmentControllerLease,
        owner: &ResourceOwner,
        network_id: &str,
        port: PortId,
        address: MacAddress,
    ) -> Result<(GuestPort, Option<Ipv4Addr>), SwitchRegistryError> {
        lease
            .require_owner(owner)
            .map_err(|error| conflict(error.to_string()))?;
        require_named(network_id)?;
        let mut registry = self.registry.lock().await;
        bind_controller(&mut registry.controller, lease.controller_identity())?;
        let installed = registry
            .switches
            .get_mut(&owner.environment_id)
            .and_then(|networks| networks.get_mut(network_id))
            .ok_or_else(|| {
                conflict(format!(
                    "Environment `{}` has no running switch for network `{network_id}`",
                    owner.environment_id
                ))
            })?;
        let guest = installed
            .switch
            .add_port(port, address)
            .await
            .map_err(|error| conflict(format!("network `{network_id}`: {error}")))?;
        Ok((
            guest,
            installed.gateway.as_ref().map(EnvironmentGateway::address),
        ))
    }

    pub async fn stop(
        &self,
        lease: &EnvironmentControllerLease,
        owner: &ResourceOwner,
    ) -> Result<EnvironmentSwitchShutdown, SwitchRegistryError> {
        lease
            .require_owner(owner)
            .map_err(|error| conflict(error.to_string()))?;
        let taken = {
            let mut registry = self.registry.lock().await;
            bind_controller(&mut registry.controller, lease.controller_identity())?;
            registry
                .switches
                .remove(&owner.environment_id)
                .unwrap_or_default()
        };
        let mut receipt = EnvironmentSwitchShutdown::default();
        for (network_id, mut installed) in taken {
            // The edge first, and joined before the switch is touched: it is a
            // port on that switch, and the order is the same one the boot loop
            // uses in reverse.
            if let Some(gateway) = installed.gateway.as_mut() {
                let stopped = gateway
                    .shutdown()
                    .await
                    .map_err(|error| conflict(format!("network `{network_id}` edge: {error}")))?;
                receipt.edges.insert(network_id.clone(), stopped);
            }
            let stopped = installed
                .switch
                .shutdown()
                .await
                .map_err(|error| conflict(format!("network `{network_id}`: {error}")))?;
            receipt.networks.insert(network_id, stopped);
        }
        Ok(receipt)
    }
}

fn require_named(network_id: &str) -> Result<(), SwitchRegistryError> {
    if network_id.trim().is_empty() || network_id.len() > 128 {
        return Err(conflict("network id must be 1..=128 non-blank characters"));
    }
    Ok(())
}

fn bind_controller(
    bound: &mut Option<std::sync::Arc<()>>,
    provided: &std::sync::Arc<()>,
) -> Result<(), SwitchRegistryError> {
    match bound {
        Some(bound) if !std::sync::Arc::ptr_eq(bound, provided) => Err(conflict(
            "switch registry belongs to another Environment controller",
        )),
        Some(_) => Ok(()),
        None => {
            *bound = Some(std::sync::Arc::clone(provided));
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "registry_tests.rs"]
mod tests;
