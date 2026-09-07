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

use thiserror::Error;
use tokio::sync::Mutex;
use vz_runtime_contract::{EnvironmentId, ResourceOwner};

use super::runtime::{NetworkSwitch, SwitchShutdown};
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
}

#[derive(Default)]
struct Registry {
    /// Bound on first use, so a second controller cannot adopt switches this
    /// daemon's controller established.
    controller: Option<std::sync::Arc<()>>,
    switches: BTreeMap<EnvironmentId, BTreeMap<String, NetworkSwitch>>,
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
        networks.insert(network_id.to_string(), switch);
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
        for (network_id, mut switch) in taken {
            let stopped = switch
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
