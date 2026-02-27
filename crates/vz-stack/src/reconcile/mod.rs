//! Stack reconciliation: diff planner and ordered executor.
//!
//! The [`apply`] function compares desired [`StackSpec`] against
//! observed state, computes a deterministic action plan, and
//! persists all state transitions. Actions are ordered by service
//! dependency graph (topological sort with name-based tie-break).

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};

use crate::error::StackError;
use crate::events::StackEvent;
use crate::health::{DependencyCheck, HealthStatus, check_dependencies};
use crate::spec::{ServiceSpec, StackSpec};
use crate::state_store::{ServiceObservedState, ServicePhase, StateStore};
use crate::volume;

/// Compute a deterministic digest of all config-affecting fields for a service.
///
/// Any change to image, command, entrypoint, environment, working_dir, user,
/// ports, capabilities, privileged mode, sysctls, hostname, or mounts will
/// produce a different digest, triggering a `ServiceRecreate`.
mod planning;
mod topo;

use self::planning::{compute_actions_with_mount_digests, service_config_digest};

#[cfg(test)]
mod tests;

/// A reconciliation action to converge observed state toward desired state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Create and start a new service.
    ServiceCreate {
        /// Service name.
        service_name: String,
    },
    /// Recreate a service whose configuration changed.
    ServiceRecreate {
        /// Service name.
        service_name: String,
    },
    /// Remove a service that is no longer in the desired spec.
    ServiceRemove {
        /// Service name.
        service_name: String,
    },
}

impl Action {
    /// Service name this action targets.
    pub fn service_name(&self) -> &str {
        match self {
            Self::ServiceCreate { service_name }
            | Self::ServiceRecreate { service_name }
            | Self::ServiceRemove { service_name } => service_name,
        }
    }
}

/// Compute a deterministic hash of an action list for identity tracking.
///
/// Two action lists that contain the same sequence of action kinds and
/// service names produce the same hash, enabling callers to detect
/// whether a resumed session matches the original plan.
pub fn compute_actions_hash(actions: &[Action]) -> String {
    let mut hasher = DefaultHasher::new();
    for action in actions {
        match action {
            Action::ServiceCreate { service_name } => {
                "create".hash(&mut hasher);
                service_name.hash(&mut hasher);
            }
            Action::ServiceRecreate { service_name } => {
                "recreate".hash(&mut hasher);
                service_name.hash(&mut hasher);
            }
            Action::ServiceRemove { service_name } => {
                "remove".hash(&mut hasher);
                service_name.hash(&mut hasher);
            }
        }
    }
    format!("{:016x}", hasher.finish())
}

/// Result of an [`apply`] call.
#[derive(Debug, Clone, Default)]
pub struct ApplyResult {
    /// Actions that were planned (and would be executed by a real runtime).
    ///
    /// This is the reconciler's explicit convergence claim for the round:
    /// if this list is empty and no services are deferred, reconcile has no
    /// further work for the current desired/observed state.
    pub actions: Vec<Action>,
    /// Services deferred because their dependencies are not ready.
    ///
    /// Deferred services are part of the convergence claim and must be empty
    /// before the orchestrator can declare the stack converged.
    pub deferred: Vec<DeferredService>,
}

/// A service whose creation was deferred due to unready dependencies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredService {
    /// Service name that was deferred.
    pub service_name: String,
    /// Dependencies that are not yet ready.
    pub waiting_on: Vec<String>,
}

/// Persist desired state, compute action plan, and update observed state.
///
/// The reconciler:
/// 1. Persists the desired spec in the state store.
/// 2. Loads current observed state.
/// 3. Computes a deterministic, dependency-ordered action plan.
/// 4. Gates service creation on dependency readiness.
/// 5. Updates observed state for each action (create/remove).
/// 6. Emits lifecycle events for observability.
///
/// Services whose dependencies are not ready are deferred and
/// reported in [`ApplyResult::deferred`]. Re-applying the same
/// spec after dependencies become ready will create them. This makes
/// `apply` idempotent and restart-safe: convergence is driven by the
/// persisted desired/observed state and deterministic action planning.
pub fn apply(
    spec: &StackSpec,
    store: &StateStore,
    health_statuses: &HashMap<String, HealthStatus>,
) -> Result<ApplyResult, StackError> {
    // 1. Load previous desired state (for reverse-dep teardown ordering).
    let previous_desired = store.load_desired_state(&spec.name)?;
    let previous_config_digests: HashMap<String, String> = previous_desired
        .as_ref()
        .map(|stack| {
            stack
                .services
                .iter()
                .map(|svc| (svc.name.clone(), service_config_digest(svc)))
                .collect()
        })
        .unwrap_or_default();
    let desired_service_map: HashMap<&str, &ServiceSpec> = spec
        .services
        .iter()
        .map(|svc| (svc.name.as_str(), svc))
        .collect();

    // 2. Persist desired state.
    store.save_desired_state(&spec.name, spec)?;

    // 3. Emit start event.
    store.emit_event(
        &spec.name,
        &StackEvent::StackApplyStarted {
            stack_name: spec.name.clone(),
            services_count: spec.services.len(),
        },
    )?;

    // 4. Load current observed state.
    let observed = store.load_observed_state(&spec.name)?;
    let stored_mount_digests = store.load_service_mount_digests(&spec.name)?;

    // 5. Compute action plan with dependency gating.
    let (actions, deferred) = compute_actions_with_mount_digests(
        &spec.services,
        &observed,
        health_statuses,
        previous_desired.as_ref().map(|s| s.services.as_slice()),
        &stored_mount_digests,
    );

    // 5. Emit events for deferred services.
    for d in &deferred {
        store.emit_event(
            &spec.name,
            &StackEvent::DependencyBlocked {
                stack_name: spec.name.clone(),
                service_name: d.service_name.clone(),
                waiting_on: d.waiting_on.clone(),
            },
        )?;
    }

    // 6. Execute action plan (update observed state).
    let mut succeeded = 0;
    let failed = 0;
    for action in &actions {
        match action {
            Action::ServiceCreate { service_name } => {
                if let Some(service) = desired_service_map.get(service_name.as_str()) {
                    let digest = service_config_digest(service);
                    store.save_service_mount_digest(&spec.name, service_name, &digest)?;
                }
                store.save_observed_state(
                    &spec.name,
                    &ServiceObservedState {
                        service_name: service_name.clone(),
                        phase: ServicePhase::Pending,
                        container_id: None,
                        last_error: None,
                        ready: false,
                    },
                )?;
                store.emit_event(
                    &spec.name,
                    &StackEvent::ServiceCreating {
                        stack_name: spec.name.clone(),
                        service_name: service_name.clone(),
                    },
                )?;
                succeeded += 1;
            }
            Action::ServiceRecreate { service_name } => {
                let desired_digest = desired_service_map
                    .get(service_name.as_str())
                    .map(|service| service_config_digest(service))
                    .unwrap_or_default();
                let previous_digest = stored_mount_digests
                    .get(service_name)
                    .cloned()
                    .or_else(|| previous_config_digests.get(service_name).cloned());
                store.emit_event(
                    &spec.name,
                    &StackEvent::MountTopologyRecreateRequired {
                        stack_name: spec.name.clone(),
                        service_name: service_name.clone(),
                        previous_digest,
                        desired_digest: desired_digest.clone(),
                    },
                )?;
                // Preserve the existing container_id so the executor can
                // stop + remove the old container before creating the new one.
                let existing_cid = observed
                    .iter()
                    .find(|o| o.service_name == *service_name)
                    .and_then(|o| o.container_id.clone());
                store.save_observed_state(
                    &spec.name,
                    &ServiceObservedState {
                        service_name: service_name.clone(),
                        phase: ServicePhase::Pending,
                        container_id: existing_cid,
                        last_error: None,
                        ready: false,
                    },
                )?;
                store.save_service_mount_digest(&spec.name, service_name, &desired_digest)?;
                store.emit_event(
                    &spec.name,
                    &StackEvent::ServiceCreating {
                        stack_name: spec.name.clone(),
                        service_name: service_name.clone(),
                    },
                )?;
                succeeded += 1;
            }
            Action::ServiceRemove { service_name } => {
                store.delete_service_mount_digest(&spec.name, service_name)?;
                store.save_observed_state(
                    &spec.name,
                    &ServiceObservedState {
                        service_name: service_name.clone(),
                        phase: ServicePhase::Stopped,
                        container_id: None,
                        last_error: None,
                        ready: false,
                    },
                )?;
                store.emit_event(
                    &spec.name,
                    &StackEvent::ServiceStopped {
                        stack_name: spec.name.clone(),
                        service_name: service_name.clone(),
                        exit_code: 0,
                    },
                )?;
                succeeded += 1;
            }
        }
    }

    // 7. Emit completion event.
    store.emit_event(
        &spec.name,
        &StackEvent::StackApplyCompleted {
            stack_name: spec.name.clone(),
            succeeded,
            failed,
        },
    )?;

    Ok(ApplyResult { actions, deferred })
}
