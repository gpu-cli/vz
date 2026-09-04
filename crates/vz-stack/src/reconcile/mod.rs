//! Stack reconciliation: diff planner and ordered executor.
//!
//! The [`apply`] function compares desired [`StackSpec`] against
//! observed state, computes a deterministic action plan, and
//! persists all state transitions. Actions are ordered by service
//! dependency graph (topological sort with name-based tie-break).

use std::collections::{HashMap, HashSet, VecDeque};

use sha2::{Digest, Sha256};

use crate::error::StackError;
use crate::events::StackEvent;
use crate::health::{DependencyCheck, HealthStatus, check_dependencies};
use crate::spec::{ServiceSpec, StackSpec};
use crate::state_store::{ServiceObservedState, ServicePhase, ServiceReplicaKey, StateStore};

/// Compute a deterministic digest of all config-affecting fields for a service.
///
/// The versioned digest covers the canonical normalized service activation
/// projection. Replica count is topology, not per-replica runtime config.
mod planning;
mod topo;

use self::planning::compute_actions_with_mount_digests;
pub use self::planning::service_config_digest;

#[cfg(test)]
mod tests;

/// A reconciliation action to converge observed state toward desired state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Create and start a new service.
    ServiceCreate {
        /// Exact service replica.
        target: ServiceReplicaKey,
    },
    /// Recreate a service whose configuration changed.
    ServiceRecreate {
        /// Exact service replica.
        target: ServiceReplicaKey,
    },
    /// Remove a service that is no longer in the desired spec.
    ServiceRemove {
        /// Exact service replica.
        target: ServiceReplicaKey,
    },
}

impl Action {
    /// Service name this action targets.
    pub fn service_name(&self) -> &str {
        match self {
            Self::ServiceCreate { target }
            | Self::ServiceRecreate { target }
            | Self::ServiceRemove { target } => &target.service_name,
        }
    }

    /// Exact replica targeted by this action.
    pub fn target(&self) -> &ServiceReplicaKey {
        match self {
            Self::ServiceCreate { target }
            | Self::ServiceRecreate { target }
            | Self::ServiceRemove { target } => target,
        }
    }
}

/// Compute a deterministic hash of an action list for identity tracking.
///
/// The versioned, length-framed digest covers action order, kind, exact base
/// service name, and non-zero replica index.
pub fn compute_actions_hash(actions: &[Action]) -> String {
    fn frame(hasher: &mut Sha256, value: &[u8]) {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }

    let mut hasher = Sha256::new();
    frame(&mut hasher, b"vz-reconcile-actions-v1");
    frame(&mut hasher, &(actions.len() as u64).to_be_bytes());
    for action in actions {
        let (kind, target) = match action {
            Action::ServiceCreate { target } => (b"create".as_slice(), target),
            Action::ServiceRecreate { target } => (b"recreate".as_slice(), target),
            Action::ServiceRemove { target } => (b"remove".as_slice(), target),
        };
        frame(&mut hasher, kind);
        frame(&mut hasher, target.service_name.as_bytes());
        frame(&mut hasher, &target.index().to_be_bytes());
    }
    format!("vzrah1-sha256:{:x}", hasher.finalize())
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

/// Compute an apply plan without persisting desired/observed state or events.
pub fn plan_apply(
    spec: &StackSpec,
    store: &StateStore,
    health_statuses: &HashMap<String, HealthStatus>,
) -> Result<ApplyResult, StackError> {
    let previous_desired = store.load_desired_state(&spec.name)?;
    let observed = store.load_observed_state(&spec.name)?;
    let stored_mount_digests = store.load_service_mount_digests(&spec.name)?;
    let (actions, deferred) = compute_actions_with_mount_digests(
        &spec.services,
        &observed,
        health_statuses,
        previous_desired
            .as_ref()
            .map(|stack| stack.services.as_slice()),
        &stored_mount_digests,
    )?;
    Ok(ApplyResult { actions, deferred })
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
    )?;

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
            Action::ServiceCreate { target } => {
                let service_name = &target.service_name;
                if let Some(service) = desired_service_map.get(service_name.as_str()) {
                    let digest = service_config_digest(service);
                    store.save_service_mount_digest(&spec.name, service_name, &digest)?;
                }
                let failed_create_ownership = observed
                    .iter()
                    .find(|state| state.replica == *target)
                    .and_then(|state| state.failed_create_ownership.clone());
                let existing_cid = observed
                    .iter()
                    .find(|state| state.replica == *target)
                    .and_then(|state| state.container_id.clone());
                store.save_observed_state(
                    &spec.name,
                    &ServiceObservedState {
                        replica: target.clone(),
                        applied_config_digest: None,
                        phase: ServicePhase::Pending,
                        container_id: existing_cid,
                        failed_create_ownership,
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
            Action::ServiceRecreate { target } => {
                let service_name = &target.service_name;
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
                    .find(|o| o.replica == *target)
                    .and_then(|o| o.container_id.clone());
                let failed_create_ownership = observed
                    .iter()
                    .find(|o| o.replica == *target)
                    .and_then(|o| o.failed_create_ownership.clone());
                store.save_observed_state(
                    &spec.name,
                    &ServiceObservedState {
                        replica: target.clone(),
                        applied_config_digest: None,
                        phase: ServicePhase::Pending,
                        container_id: existing_cid,
                        failed_create_ownership,
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
            Action::ServiceRemove { target } => {
                let service_name = &target.service_name;
                store.delete_service_mount_digest(&spec.name, service_name)?;
                let existing_cid = observed
                    .iter()
                    .find(|state| state.replica == *target)
                    .and_then(|state| state.container_id.clone());
                let failed_create_ownership = observed
                    .iter()
                    .find(|state| state.replica == *target)
                    .and_then(|state| state.failed_create_ownership.clone());
                store.save_observed_state(
                    &spec.name,
                    &ServiceObservedState {
                        replica: target.clone(),
                        applied_config_digest: None,
                        phase: ServicePhase::Stopped,
                        // Preserve the opaque runtime ID until the executor has
                        // actually completed stop + remove. This also makes a
                        // crash between planning and execution retryable.
                        container_id: existing_cid,
                        failed_create_ownership,
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
