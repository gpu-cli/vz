//! Daemon-owned live Linux Machine sessions, not a persisted-state recovery guess.
//!
//! Registration retains the exact activation and its original Runtime. Stop
//! drains an optional exact-activation endpoint before releasing the VM reader.
//! Missing registrations after restart are uncertain, never `AlreadyAbsent`.
//! This trusted controller API does not authorize RPCs or acknowledge journals.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{OwnedMutexGuard, watch};
use vz_oci_macos::MacosRuntimeBackend;
use vz_runtime_contract::{
    EnvironmentLifecycleKind, EnvironmentLifecycleOperation, MachineId, ResourceOwner,
    STACK_RUNTIME_SHUTDOWN_REQUEST_SCHEMA_VERSION, StackRuntimeIdentity,
    StackRuntimeShutdownOutcome, StackRuntimeShutdownRequest,
};

use crate::environment_runtime_controller::{EnvironmentControllerLease, EnvironmentStateStore};
use crate::machine_docker_endpoint::{MachineDockerEndpoint, MachineDockerEndpointShutdown};
use crate::machine_execution_activity::{MachineExecutionActivities, MachineExecutionActivity};
use crate::machine_runtime_activation::MachineRuntimeActivation;
use crate::machine_runtime_registry::{
    MachineRuntimeEntry, MachineRuntimeRegistry, MachineRuntimeStoreLease,
    MachineStoreDeletePreflight,
};

#[derive(Debug, Error)]
#[error("Machine live-session ownership: {0}")]
pub struct MachineLiveSessionError(String);

fn error(value: impl ToString) -> MachineLiveSessionError {
    MachineLiveSessionError(value.to_string())
}

/// Physical teardown proof only. The caller must separately acknowledge the
/// exact durable Machine step and finish the Environment lifecycle operation.
#[derive(Debug, Clone, Serialize)]
pub struct MachineSessionStopReceipt {
    pub owner: ResourceOwner,
    pub operation_id: String,
    pub generation: u64,
    pub runtime_identity: StackRuntimeIdentity,
    pub endpoint: Option<MachineDockerEndpointShutdown>,
    pub outcome: StackRuntimeShutdownOutcome,
}

type StopResult = Result<MachineSessionStopReceipt, String>;
type RetainedFence = Arc<Mutex<Option<Arc<OwnedMutexGuard<()>>>>>;

struct StopAttempt {
    operation: EnvironmentLifecycleOperation,
    result: watch::Receiver<Option<Arc<StopResult>>>,
    // An uncertain timeout/error keeps the Environment fenced, even after the
    // result has been delivered. It requires explicit recovery, not a retry
    // that might cross unfinished backend effects.
    _fence: RetainedFence,
}

struct LiveResources {
    entry: Arc<MachineRuntimeEntry<MacosRuntimeBackend>>,
    activation: Arc<MachineRuntimeActivation>,
    endpoint: Option<MachineDockerEndpoint>,
}

struct Session {
    owner: ResourceOwner,
    identity: StackRuntimeIdentity,
    configuration_digest: String,
    original_entry: Weak<MachineRuntimeEntry<MacosRuntimeBackend>>,
    resources: Mutex<Option<LiveResources>>,
    // Keep the original Runtime/store lock even after a failed or cancelled
    // backend future. Only a positive teardown receipt releases this anchor.
    retained_entry: Mutex<Option<Arc<MachineRuntimeEntry<MacosRuntimeBackend>>>>,
    attempt: Mutex<Option<StopAttempt>>,
    executions: Arc<MachineExecutionActivities>,
}

/// Sealed pre-Delete absence authority. A missing map entry is not this proof.
pub(crate) struct MachineDeleteAbsentAdmission {
    environment: vz_runtime_contract::EnvironmentInstance,
    machine_id: MachineId,
    authority: DeleteAdmissionAuthority,
    current_delete: Option<EnvironmentLifecycleOperation>,
    controller: Arc<()>,
    _fence: Arc<OwnedMutexGuard<()>>,
}

enum DeleteAdmissionAuthority {
    Absent(Box<DeleteAbsenceAuthority>),
    Retired(Box<DeleteQuiescenceEvidence>),
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum DeleteAbsenceAuthority {
    PositiveStop {
        operation: EnvironmentLifecycleOperation,
    },
    BootNotDispatched {
        proof: vz_stack::MachineBootNonDispatchProof,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum DeleteQuiescenceAuthority {
    Drained {
        runtime_identity: StackRuntimeIdentity,
        endpoint: Option<DeleteEndpointProof>,
        outcome: StackRuntimeShutdownOutcome,
    },
    Absent {
        authority: DeleteAbsenceAuthority,
    },
    AcknowledgedDelete,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteEndpointProof {
    accepted_connections: u64,
    completed_connections: u64,
    cancelled_connections: u64,
    failed_connections: u64,
    active_connections: usize,
    socket_removed: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteQuiescenceEvidence {
    schema_version: u32,
    owner: ResourceOwner,
    configuration_digest: String,
    operation: EnvironmentLifecycleOperation,
    authority: DeleteQuiescenceAuthority,
}

/// Only this module can mint filesystem-deletion authority. The retained
/// controller guard fences new work through the registry's destructive phase.
pub(crate) struct MachineDeleteQuiescence {
    proof: DeleteQuiescenceEvidence,
    evidence: serde_json::Value,
    original_entry: Option<Weak<MachineRuntimeEntry<MacosRuntimeBackend>>>,
    original_lease: Option<Weak<MachineRuntimeStoreLease>>,
    _fence: Arc<OwnedMutexGuard<()>>,
}

impl MachineDeleteQuiescence {
    /// Synthetic runtime-free authority for registry filesystem unit tests,
    /// never a persisted-state or physical-backend verification substitute.
    #[cfg(test)]
    pub(crate) fn for_runtime_free_test(
        claim: &MachineStoreDeletePreflight,
        operation: &EnvironmentLifecycleOperation,
        lease: &EnvironmentControllerLease,
    ) -> Result<Self, MachineLiveSessionError> {
        lease.require_owner(claim.owner()).map_err(error)?;
        let machine = claim
            .owner()
            .machine_id
            .as_ref()
            .ok_or_else(|| error("Machine required"))?;
        if delete_step(operation, machine)?.status
            != vz_runtime_contract::LifecycleStepStatus::Succeeded
        {
            return Err(error("test quiescence requires acknowledged Delete"));
        }
        let proof = match claim.quiescence_evidence() {
            Some(raw) => serde_json::from_value(raw.clone()).map_err(error)?,
            None => DeleteQuiescenceEvidence {
                schema_version: 1,
                owner: claim.owner().clone(),
                configuration_digest: claim.configuration_digest().into(),
                operation: operation.clone(),
                authority: DeleteQuiescenceAuthority::AcknowledgedDelete,
            },
        };
        let token = Self {
            evidence: serde_json::to_value(&proof).map_err(error)?,
            proof,
            original_entry: None,
            original_lease: claim.lease().map(Arc::downgrade),
            _fence: lease.retained_guard(),
        };
        token.require_store(claim, operation)?;
        Ok(token)
    }

    pub(crate) fn evidence(&self) -> serde_json::Value {
        self.evidence.clone()
    }

    pub(crate) fn runtime_entry_address(&self) -> Option<usize> {
        self.original_entry
            .as_ref()
            .map(|entry| entry.as_ptr() as usize)
    }

    pub(crate) fn require_store(
        &self,
        claim: &MachineStoreDeletePreflight,
        operation: &EnvironmentLifecycleOperation,
    ) -> Result<(), MachineLiveSessionError> {
        let machine = self
            .proof
            .owner
            .machine_id
            .as_ref()
            .ok_or_else(|| error("Machine owner required"))?;
        if claim.owner() != &self.proof.owner
            || claim.configuration_digest() != self.proof.configuration_digest
            || self.proof.owner.project_id != operation.project_id
            || self.proof.owner.environment_id != operation.environment_id
            || !same_stop_request(&self.proof.operation, operation, machine)
        {
            return Err(error(
                "Delete token does not authorize this store or operation",
            ));
        }
        claim.matches_operation(operation).map_err(error)?;
        match (&self.original_lease, claim.lease()) {
            (Some(expected), Some(actual)) if expected.ptr_eq(&Arc::downgrade(actual)) => {}
            (None, None) if claim.quiescence_evidence() == Some(&self.evidence) => {}
            _ => return Err(error("Delete store lease changed after quiescence")),
        }
        if claim
            .quiescence_evidence()
            .is_some_and(|value| value != &self.evidence)
        {
            return Err(error(
                "Delete intent's positive quiescence evidence changed",
            ));
        }
        Ok(())
    }
}

pub(crate) struct MachineExecutionAdmission {
    pub activation: Arc<MachineRuntimeActivation>,
    pub activity: Arc<MachineExecutionActivity>,
}

#[derive(Default)]
struct Sessions {
    controller: Option<Arc<()>>,
    machines: HashMap<MachineId, Arc<Session>>,
    retired: HashMap<MachineId, RetiredDelete>,
}

struct RetiredDelete {
    // Captured before removing the session. A failed database acknowledgement
    // must not discard already-established positive physical quiescence.
    proof: DeleteQuiescenceEvidence,
    original_entry: Option<Weak<MachineRuntimeEntry<MacosRuntimeBackend>>>,
    original_lease: Option<Weak<MachineRuntimeStoreLease>>,
}

/// One registry belongs to one daemon/controller. A session must be registered
/// before publishing its activation or exposing its endpoint to host clients.
#[derive(Default)]
pub struct MachineLiveSessions {
    sessions: Mutex<Sessions>,
}

impl MachineLiveSessions {
    /// Resolve missing-session authority before beginning Delete. This proof
    /// says nothing about disks, contexts, or runtime-store absence.
    pub(crate) fn prepare_delete_absence<S: EnvironmentStateStore>(
        &self,
        lease: &EnvironmentControllerLease,
        state: &S,
        environment: &vz_runtime_contract::EnvironmentInstance,
        machine_id: &MachineId,
    ) -> Result<Option<MachineDeleteAbsentAdmission>, MachineLiveSessionError> {
        let owner = ResourceOwner {
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(machine_id.clone()),
        };
        lease.require_owner(&owner).map_err(error)?;
        let mut sessions = self.sessions.lock().map_err(error)?;
        require_controller(&mut sessions.controller, lease.controller_identity())?;
        let current = load_delete_environment(state, &owner)?;
        if &current != environment {
            return Err(error("Delete preflight snapshot changed"));
        }
        if let Some(session) = sessions.machines.get(machine_id) {
            if session.owner != owner {
                return Err(error("Delete preflight session is foreign"));
            }
            let attempt = session.attempt.lock().map_err(error)?;
            if let Some(attempt) = attempt.as_ref() {
                positive_session_receipt(session, attempt)?;
                let prior = state
                    .access(|store| {
                        store.load_environment_lifecycle(attempt.operation.operation_id.as_str())
                    })
                    .map_err(error)?
                    .ok_or_else(|| error("Delete preflight original teardown journal absent"))?;
                if !same_stop_request(&prior, &attempt.operation, machine_id) {
                    return Err(error(
                        "Delete preflight original teardown authority changed",
                    ));
                }
                if prior.kind == EnvironmentLifecycleKind::Delete {
                    if prior.generation != environment.lifecycle_generation
                        || environment.active_operation_id.as_ref() != Some(&prior.operation_id)
                    {
                        return Err(error("Delete preflight has an unrelated teardown attempt"));
                    }
                } else {
                    require_positive_stop(environment, machine_id, &prior)?;
                }
            } else {
                session
                    .resources
                    .lock()
                    .map_err(error)?
                    .as_ref()
                    .ok_or_else(|| error("Delete preflight has no exact live resources"))?
                    .entry
                    .validate_current()
                    .map_err(error)?;
            }
            let machine = environment
                .machines
                .iter()
                .find(|row| &row.machine_id == machine_id)
                .ok_or_else(|| error("Delete preflight Machine absent"))?;
            if let Some(identity) = &machine.runtime_identity {
                if serde_json::from_str::<StackRuntimeIdentity>(&identity.opaque_id)
                    .map_err(error)?
                    != session.identity
                {
                    return Err(error(
                        "Delete preflight runtime identity differs from original session",
                    ));
                }
            } else if machine.incarnation.is_some() {
                return Err(error("Delete preflight runtime identity missing"));
            }
            return Ok(None);
        }
        let machine = environment
            .machines
            .iter()
            .find(|machine| &machine.machine_id == machine_id)
            .ok_or_else(|| error("Delete preflight Machine absent"))?;
        let prior = state
            .access(|store| {
                store.load_environment_lifecycle_at_generation(
                    &environment.environment_id,
                    environment.lifecycle_generation,
                )
            })
            .map_err(error)?;
        if let Some(prior) = prior.as_ref() {
            if prior.kind == EnvironmentLifecycleKind::Delete {
                let step = delete_step(prior, machine_id)?;
                if step.status == vz_runtime_contract::LifecycleStepStatus::Succeeded {
                    require_acknowledged_delete(environment, prior, machine_id)?;
                    return Ok(None);
                }
                require_delete_journal(state, prior, machine_id)?;
                require_delete_store_fence(state, prior, machine_id, &owner)?;
                if let Some(retired) = sessions.retired.get(machine_id) {
                    require_retired_identity(retired, &owner, prior, machine_id)?;
                    return Ok(Some(MachineDeleteAbsentAdmission {
                        environment: environment.clone(),
                        machine_id: machine_id.clone(),
                        authority: DeleteAdmissionAuthority::Retired(Box::new(
                            retired.proof.clone(),
                        )),
                        current_delete: Some(prior.clone()),
                        controller: Arc::clone(lease.controller_identity()),
                        _fence: lease.retained_guard(),
                    }));
                }
                if step.initial_state != vz_runtime_contract::MachineState::Stopped {
                    let proof = state
                        .access(|store| {
                            store.require_machine_boot_non_dispatch(environment, machine_id)
                        })
                        .map_err(error)?
                        .ok_or_else(|| {
                            error("unacknowledged Delete lost its original live VM authority")
                        })?;
                    return Ok(Some(MachineDeleteAbsentAdmission {
                        environment: environment.clone(),
                        machine_id: machine_id.clone(),
                        authority: DeleteAdmissionAuthority::Absent(Box::new(
                            DeleteAbsenceAuthority::BootNotDispatched { proof },
                        )),
                        current_delete: Some(prior.clone()),
                        controller: Arc::clone(lease.controller_identity()),
                        _fence: lease.retained_guard(),
                    }));
                }
                let generation = prior
                    .generation
                    .checked_sub(1)
                    .ok_or_else(|| error("Delete predecessor generation absent"))?;
                let stop = state
                    .access(|store| {
                        store.load_environment_lifecycle_at_generation(
                            &environment.environment_id,
                            generation,
                        )
                    })
                    .map_err(error)?
                    .ok_or_else(|| error("pending Delete has no prior positive Stop"))?;
                // Validate the retained Machine against the authoritative prior
                // Stop, not against an invented pre-Delete persisted snapshot.
                let mut stop_view = environment.clone();
                stop_view.lifecycle_generation = generation;
                stop_view.active_operation_id = None;
                require_positive_stop(&stop_view, machine_id, &stop)?;
                return Ok(Some(MachineDeleteAbsentAdmission {
                    environment: environment.clone(),
                    machine_id: machine_id.clone(),
                    authority: DeleteAdmissionAuthority::Absent(Box::new(
                        DeleteAbsenceAuthority::PositiveStop { operation: stop },
                    )),
                    current_delete: Some(prior.clone()),
                    controller: Arc::clone(lease.controller_identity()),
                    _fence: lease.retained_guard(),
                }));
            }
        }
        let authority = if machine.state == vz_runtime_contract::MachineState::Stopped {
            let prior =
                prior.ok_or_else(|| error("Stopped Machine has no positive Stop journal"))?;
            require_positive_stop(environment, machine_id, &prior)?;
            DeleteAbsenceAuthority::PositiveStop { operation: prior }
        } else {
            let proof = state
                .access(|store| store.require_machine_boot_non_dispatch(environment, machine_id))
                .map_err(error)?
                .ok_or_else(|| {
                    error("missing session has no positive VM non-dispatch authority")
                })?;
            DeleteAbsenceAuthority::BootNotDispatched { proof }
        };
        Ok(Some(MachineDeleteAbsentAdmission {
            environment: environment.clone(),
            machine_id: machine_id.clone(),
            authority: DeleteAdmissionAuthority::Absent(Box::new(authority)),
            current_delete: None,
            controller: Arc::clone(lease.controller_identity()),
            _fence: lease.retained_guard(),
        }))
    }

    /// Retire the exact session before acknowledging the Delete Machine step.
    /// The returned sealed token remains fenced through context/store cleanup.
    #[allow(clippy::too_many_arguments)] // Explicit independent authority inputs.
    pub(crate) fn retire_for_delete<S: EnvironmentStateStore>(
        &self,
        lease: &EnvironmentControllerLease,
        state: &S,
        operation: &EnvironmentLifecycleOperation,
        machine_id: &MachineId,
        claim: &MachineStoreDeletePreflight,
        absence: Option<MachineDeleteAbsentAdmission>,
    ) -> Result<MachineDeleteQuiescence, MachineLiveSessionError> {
        let owner = ResourceOwner {
            project_id: operation.project_id.clone(),
            environment_id: operation.environment_id.clone(),
            machine_id: Some(machine_id.clone()),
        };
        lease.require_owner(&owner).map_err(error)?;
        if claim.owner() != &owner {
            return Err(error("Delete store preflight owner mismatch"));
        }
        claim.matches_operation(operation).map_err(error)?;
        let mut sessions = self.sessions.lock().map_err(error)?;
        require_controller(&mut sessions.controller, lease.controller_identity())?;
        let environment = require_delete_journal(state, operation, machine_id)?;
        let (proof, original_entry) = if let Some(session) = sessions.machines.get(machine_id) {
            if absence.is_some()
                || claim.quiescence_evidence().is_some()
                || session.owner != owner
                || session.configuration_digest != claim.configuration_digest()
            {
                return Err(error(
                    "Delete retirement conflicts with retained live-session authority",
                ));
            }
            require_delete_fence(state, operation, machine_id, session)?;
            // An observer or task holding this exact session has not finished
            // handing off. Do not race it or infer completion from map removal.
            if Arc::strong_count(session) != 1 {
                return Err(error("Delete session still has active observers"));
            }
            let attempt = session.attempt.lock().map_err(error)?;
            let attempt = attempt
                .as_ref()
                .ok_or_else(|| error("Delete session has not been drained"))?;
            if !same_stop_request(&attempt.operation, operation, machine_id) {
                return Err(error("Delete session was drained under another operation"));
            }
            let receipt = positive_session_receipt(session, attempt)?;
            let endpoint = receipt.endpoint.map(|endpoint| DeleteEndpointProof {
                accepted_connections: endpoint.accepted_connections,
                completed_connections: endpoint.completed_connections,
                cancelled_connections: endpoint.cancelled_connections,
                failed_connections: endpoint.failed_connections,
                active_connections: endpoint.active_connections,
                socket_removed: endpoint.socket_removed,
            });
            (
                DeleteQuiescenceEvidence {
                    schema_version: 1,
                    owner: owner.clone(),
                    configuration_digest: claim.configuration_digest().into(),
                    operation: operation.clone(),
                    authority: DeleteQuiescenceAuthority::Drained {
                        runtime_identity: receipt.runtime_identity,
                        endpoint,
                        outcome: receipt.outcome,
                    },
                },
                Some(session.original_entry.clone()),
            )
        } else if let Some(raw) = claim.quiescence_evidence() {
            require_acknowledged_delete(&environment, operation, machine_id)?;
            let proof: DeleteQuiescenceEvidence =
                serde_json::from_value(raw.clone()).map_err(error)?;
            validate_quiescence_evidence(&proof, claim, operation, &environment, machine_id)?;
            (proof, None)
        } else if let Some(absence) = absence {
            if !Arc::ptr_eq(&absence.controller, lease.controller_identity())
                || absence.machine_id != *machine_id
                || absence.environment.project_id != operation.project_id
                || absence.environment.environment_id != operation.environment_id
                || absence.environment.definition_digest != operation.definition_digest
                || match &absence.current_delete {
                    Some(admitted) => {
                        !same_stop_request(admitted, operation, machine_id)
                            || absence.environment.lifecycle_generation != operation.generation
                    }
                    None => {
                        absence.environment.lifecycle_generation.checked_add(1)
                            != Some(operation.generation)
                    }
                }
            {
                return Err(error("Delete absence admission is stale or foreign"));
            }
            let previous = absence
                .environment
                .machines
                .iter()
                .find(|machine| &machine.machine_id == machine_id)
                .ok_or_else(|| error("Delete admitted Machine absent"))?;
            let step = delete_step(operation, machine_id)?;
            if previous.incarnation != step.expected_incarnation
                || previous.state != step.initial_state
            {
                return Err(error("Delete absence admission incarnation/state changed"));
            }
            require_delete_store_fence(state, operation, machine_id, &owner)?;
            match absence.authority {
                DeleteAdmissionAuthority::Absent(authority) => (
                    DeleteQuiescenceEvidence {
                        schema_version: 1,
                        owner: owner.clone(),
                        configuration_digest: claim.configuration_digest().into(),
                        operation: operation.clone(),
                        authority: DeleteQuiescenceAuthority::Absent {
                            authority: *authority,
                        },
                    },
                    None,
                ),
                DeleteAdmissionAuthority::Retired(proof) => {
                    let retired = sessions
                        .retired
                        .get(machine_id)
                        .ok_or_else(|| error("positive retired Delete authority disappeared"))?;
                    require_retired_identity(retired, &owner, operation, machine_id)?;
                    if retired.proof.configuration_digest != claim.configuration_digest()
                        || !matches!((&retired.original_lease, claim.lease()), (Some(expected), Some(actual)) if expected.ptr_eq(&Arc::downgrade(actual)))
                        || serde_json::to_value(&retired.proof).map_err(error)?
                            != serde_json::to_value(&proof).map_err(error)?
                    {
                        return Err(error(
                            "retired Delete evidence or exact store lease changed",
                        ));
                    }
                    (*proof, retired.original_entry.clone())
                }
            }
        } else {
            require_acknowledged_delete(&environment, operation, machine_id)?;
            (
                DeleteQuiescenceEvidence {
                    schema_version: 1,
                    owner: owner.clone(),
                    configuration_digest: claim.configuration_digest().into(),
                    operation: operation.clone(),
                    authority: DeleteQuiescenceAuthority::AcknowledgedDelete,
                },
                None,
            )
        };
        validate_quiescence_evidence(&proof, claim, operation, &environment, machine_id)?;
        let evidence = serde_json::to_value(&proof).map_err(error)?;
        let original_entry = original_entry.or_else(|| {
            sessions
                .retired
                .get(machine_id)
                .filter(|retired| {
                    retired.proof.owner == owner
                        && retired.proof.configuration_digest == claim.configuration_digest()
                        && same_stop_request(&retired.proof.operation, operation, machine_id)
                })
                .and_then(|retired| retired.original_entry.clone())
        });
        let token = MachineDeleteQuiescence {
            proof,
            evidence,
            original_entry,
            original_lease: claim.lease().map(Arc::downgrade),
            _fence: lease.retained_guard(),
        };
        token.require_store(claim, operation)?;
        // Publish the sealed evidence before map retirement and before the
        // caller's fallible database acknowledgement. Weak references retain
        // identity, not a Runtime reader or another store-lease owner.
        sessions.retired.insert(
            machine_id.clone(),
            RetiredDelete {
                proof: token.proof.clone(),
                original_entry: token.original_entry.clone(),
                original_lease: token.original_lease.clone(),
            },
        );
        sessions.machines.remove(machine_id);
        Ok(token)
    }

    /// Resolve every sibling before Up effects. Only fresh never-started or
    /// positively stopped Machines may lack an original activation.
    pub(crate) fn activations_for_up(
        &self,
        lease: &EnvironmentControllerLease,
        environment: &vz_runtime_contract::EnvironmentInstance,
        non_dispatched: &std::collections::BTreeSet<MachineId>,
    ) -> Result<HashMap<MachineId, Arc<MachineRuntimeActivation>>, MachineLiveSessionError> {
        let mut sessions = self.sessions.lock().map_err(error)?;
        require_controller(&mut sessions.controller, lease.controller_identity())?;
        let mut result = HashMap::new();
        for machine in &environment.machines {
            let owner = ResourceOwner {
                project_id: environment.project_id.clone(),
                environment_id: environment.environment_id.clone(),
                machine_id: Some(machine.machine_id.clone()),
            };
            lease.require_owner(&owner).map_err(error)?;
            let fresh = environment.lifecycle_generation == 0
                && machine.incarnation.is_none()
                && machine.runtime_identity.is_none();
            let Some(session) = sessions.machines.get(&machine.machine_id) else {
                if non_dispatched.contains(&machine.machine_id) {
                    continue;
                }
                if fresh || machine.state == vz_runtime_contract::MachineState::Stopped {
                    continue;
                }
                return Err(error(
                    "Up cannot reconstruct an unknown previously active Machine after restart",
                ));
            };
            if session.owner != owner {
                return Err(error("Up session owner mismatch"));
            }
            let attempt = session.attempt.lock().map_err(error)?;
            let resources = session.resources.lock().map_err(error)?;
            if non_dispatched.contains(&machine.machine_id) {
                if resources.is_some()
                    || !attempt.as_ref().is_some_and(|attempt| {
                        attempt
                            .result
                            .borrow()
                            .as_ref()
                            .is_some_and(|result| result.is_ok())
                    })
                {
                    return Err(error(
                        "boot non-dispatch proof conflicts with retained session effects",
                    ));
                }
                continue;
            }
            if machine.state == vz_runtime_contract::MachineState::Stopped {
                if resources.is_some()
                    || !attempt.as_ref().is_some_and(|attempt| {
                        attempt
                            .result
                            .borrow()
                            .as_ref()
                            .is_some_and(|result| result.is_ok())
                    })
                {
                    return Err(error(
                        "Up cannot restart a Machine with uncertain Stop effects",
                    ));
                }
                continue;
            }
            if attempt.is_some() {
                return Err(error("Up session is owned by a Stop attempt"));
            }
            let resources = resources
                .as_ref()
                .ok_or_else(|| error("Up session has no original activation"))?;
            resources.entry.validate_current().map_err(error)?;
            if let Some(identity) = &machine.runtime_identity {
                let persisted: StackRuntimeIdentity =
                    serde_json::from_str(&identity.opaque_id).map_err(error)?;
                if persisted != session.identity {
                    return Err(error(
                        "Up persisted runtime identity differs from live owner",
                    ));
                }
            } else if machine.incarnation.is_some() {
                return Err(error("Up incarnation has no persisted runtime identity"));
            }
            result.insert(
                machine.machine_id.clone(),
                Arc::clone(&resources.activation),
            );
        }
        Ok(result)
    }

    /// Install a private endpoint only onto its already registered exact boot.
    pub(crate) fn attach_docker_endpoint(
        &self,
        lease: &EnvironmentControllerLease,
        activation: &Arc<MachineRuntimeActivation>,
        endpoint: &mut Option<MachineDockerEndpoint>,
    ) -> Result<(), MachineLiveSessionError> {
        lease.require_owner(activation.owner()).map_err(error)?;
        let mut sessions = self.sessions.lock().map_err(error)?;
        require_controller(&mut sessions.controller, lease.controller_identity())?;
        let machine = activation
            .owner()
            .machine_id
            .as_ref()
            .ok_or_else(|| error("Machine owner required"))?;
        let session = sessions
            .machines
            .get(machine)
            .ok_or_else(|| error("endpoint has no registered owner"))?;
        if session.attempt.lock().map_err(error)?.is_some() {
            return Err(error("endpoint owner is stopping"));
        }
        let mut resources = session.resources.lock().map_err(error)?;
        let resources = resources
            .as_mut()
            .ok_or_else(|| error("endpoint owner resources absent"))?;
        if !Arc::ptr_eq(&resources.activation, activation)
            || resources.endpoint.is_some()
            || !endpoint
                .as_ref()
                .is_some_and(|endpoint| endpoint.belongs_to(activation))
        {
            return Err(error(
                "endpoint is duplicate or belongs to a different activation object",
            ));
        }
        resources.endpoint = endpoint.take();
        Ok(())
    }

    pub(crate) fn docker_endpoint_path(
        &self,
        lease: &EnvironmentControllerLease,
        activation: &Arc<MachineRuntimeActivation>,
    ) -> Result<Option<std::path::PathBuf>, MachineLiveSessionError> {
        lease.require_owner(activation.owner()).map_err(error)?;
        let mut sessions = self.sessions.lock().map_err(error)?;
        require_controller(&mut sessions.controller, lease.controller_identity())?;
        let machine = activation
            .owner()
            .machine_id
            .as_ref()
            .ok_or_else(|| error("Machine owner required"))?;
        let session = sessions
            .machines
            .get(machine)
            .ok_or_else(|| error("endpoint has no registered owner"))?;
        let resources = session.resources.lock().map_err(error)?;
        let resources = resources
            .as_ref()
            .ok_or_else(|| error("endpoint owner resources absent"))?;
        if !Arc::ptr_eq(&resources.activation, activation) {
            return Err(error("endpoint activation mismatch"));
        }
        Ok(resources
            .endpoint
            .as_ref()
            .map(|endpoint| endpoint.socket_path().to_path_buf()))
    }
    /// Acquire the exact registered incarnation while the shared controller
    /// fence excludes Stop admission. No Runtime reconstruction or name lookup.
    pub(crate) fn admit_execution(
        &self,
        lease: &EnvironmentControllerLease,
        scope: &vz_runtime_contract::MachineExecutionScope,
    ) -> Result<MachineExecutionAdmission, MachineLiveSessionError> {
        scope.validate().map_err(error)?;
        let owner = ResourceOwner {
            project_id: scope.project_id.clone(),
            environment_id: scope.environment_id.clone(),
            machine_id: Some(scope.machine_id.clone()),
        };
        lease.require_owner(&owner).map_err(error)?;
        let mut sessions = self.sessions.lock().map_err(error)?;
        require_controller(&mut sessions.controller, lease.controller_identity())?;
        let session = sessions.machines.get(&scope.machine_id).ok_or_else(|| {
            error("Machine has no authoritative live session; restart recovery is uncertain")
        })?;
        let identity: StackRuntimeIdentity =
            serde_json::from_str(&scope.runtime_identity.opaque_id).map_err(error)?;
        if session.owner != owner
            || session.identity != identity
            || session.attempt.lock().map_err(error)?.is_some()
        {
            return Err(error(
                "Machine execution owner/runtime differs or Stop is already admitted",
            ));
        }
        let resources = session.resources.lock().map_err(error)?;
        let resources = resources
            .as_ref()
            .ok_or_else(|| error("Machine live resources unavailable"))?;
        resources.entry.validate_current().map_err(error)?;
        let activity = session
            .executions
            .register(&scope.execution_id)
            .map_err(error)?;
        Ok(MachineExecutionAdmission {
            activation: Arc::clone(&resources.activation),
            activity,
        })
    }
    /// Read-only all-sibling admission before a new durable Stop begins.
    /// Persisted stopped Machines need no reconstructed backend; every other
    /// Machine requires its original registered live activation.
    pub fn preflight_stop(
        &self,
        lease: &EnvironmentControllerLease,
        environment: &vz_runtime_contract::EnvironmentInstance,
    ) -> Result<(), MachineLiveSessionError> {
        self.preflight_stop_replay(lease, environment, None)
    }

    /// Permit journal repair only from this registry's exact successful receipt.
    pub fn preflight_stop_replay(
        &self,
        lease: &EnvironmentControllerLease,
        environment: &vz_runtime_contract::EnvironmentInstance,
        operation: Option<&EnvironmentLifecycleOperation>,
    ) -> Result<(), MachineLiveSessionError> {
        self.preflight_stop_with_non_dispatch(lease, environment, operation, &Default::default())
    }

    /// The set is supplied only after same-fence durable proof validation by
    /// the topology controller. No public caller may assert absence by ID.
    pub(crate) fn preflight_stop_with_non_dispatch(
        &self,
        lease: &EnvironmentControllerLease,
        environment: &vz_runtime_contract::EnvironmentInstance,
        operation: Option<&EnvironmentLifecycleOperation>,
        non_dispatched: &std::collections::BTreeSet<MachineId>,
    ) -> Result<(), MachineLiveSessionError> {
        let mut sessions = self.sessions.lock().map_err(error)?;
        require_controller(&mut sessions.controller, lease.controller_identity())?;
        for machine in &environment.machines {
            let owner = ResourceOwner {
                project_id: environment.project_id.clone(),
                environment_id: environment.environment_id.clone(),
                machine_id: Some(machine.machine_id.clone()),
            };
            lease.require_owner(&owner).map_err(error)?;
            let session = sessions.machines.get(&machine.machine_id);
            if non_dispatched.contains(&machine.machine_id) {
                if let Some(session) = session {
                    if session.owner != owner || session.resources.lock().map_err(error)?.is_some()
                    {
                        return Err(error(
                            "boot non-dispatch proof conflicts with a live/foreign session",
                        ));
                    }
                    if !session
                        .attempt
                        .lock()
                        .map_err(error)?
                        .as_ref()
                        .is_some_and(|attempt| {
                            attempt
                                .result
                                .borrow()
                                .as_ref()
                                .is_some_and(|result| result.is_ok())
                        })
                    {
                        return Err(error(
                            "boot non-dispatch proof cannot override uncertain session teardown",
                        ));
                    }
                }
                continue;
            }
            if machine.state == vz_runtime_contract::MachineState::Stopped {
                if let Some(session) = session {
                    if session.owner != owner || session.resources.lock().map_err(error)?.is_some()
                    {
                        return Err(error("stopped Machine still has a live or foreign session"));
                    }
                    let attempt = session.attempt.lock().map_err(error)?;
                    if !attempt.as_ref().is_some_and(|attempt| {
                        attempt
                            .result
                            .borrow()
                            .as_ref()
                            .is_some_and(|result| result.is_ok())
                    }) {
                        return Err(error(
                            "stopped Machine has an uncertain live-session teardown",
                        ));
                    }
                }
                continue;
            }
            let session = session.ok_or_else(|| {
                error(format!(
                    "Machine {} has no authoritative live session; restart recovery is uncertain",
                    machine.machine_id
                ))
            })?;
            if session.owner != owner {
                return Err(error("Machine session is foreign"));
            }
            let attempt = session.attempt.lock().map_err(error)?;
            if let Some(attempt) = attempt.as_ref() {
                if successful_replay(attempt, operation, &machine.machine_id)
                    && session.resources.lock().map_err(error)?.is_none()
                {
                    continue;
                }
                return Err(error(
                    "Machine session is foreign or already belongs to a Stop attempt",
                ));
            }
            let resources = session.resources.lock().map_err(error)?;
            let resources = resources
                .as_ref()
                .ok_or_else(|| error("Machine live resources are absent"))?;
            resources.entry.validate_current().map_err(error)?;
            if let Some(persisted) = &machine.runtime_identity {
                let identity: StackRuntimeIdentity =
                    serde_json::from_str(&persisted.opaque_id).map_err(error)?;
                if identity != session.identity {
                    return Err(error(
                        "persisted Machine activation differs from registered runtime",
                    ));
                }
            } else if machine.incarnation.is_some() {
                return Err(error("persisted incarnation has no exact runtime identity"));
            }
        }
        Ok(())
    }

    /// Transfer authoritative live ownership to the daemon. Duplicate entries,
    /// including an equal owner with a different Runtime, are rejected. The
    /// endpoint must originate from this pointer-identical activation.
    pub fn register(
        &self,
        lease: &EnvironmentControllerLease,
        activation: Arc<MachineRuntimeActivation>,
        endpoint: &mut Option<MachineDockerEndpoint>,
    ) -> Result<(), MachineLiveSessionError> {
        let owner = activation.owner().clone();
        lease.require_owner(&owner).map_err(error)?;
        let identity = activation.runtime_identity().clone();
        let reservation =
            MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(&owner).map_err(error)?;
        if identity.stack_id != reservation.resource_id {
            return Err(error(
                "activation identity does not name its exact owned VM",
            ));
        }
        if endpoint
            .as_ref()
            .is_some_and(|endpoint| !endpoint.belongs_to(&activation))
        {
            return Err(error("endpoint belongs to another activation object"));
        }
        let machine = owner
            .machine_id
            .clone()
            .ok_or_else(|| error("Machine owner required"))?;
        let mut sessions = self.sessions.lock().map_err(error)?;
        require_controller(&mut sessions.controller, lease.controller_identity())?;
        require_available_session(&sessions, &owner, &identity)?;
        let retained_entry = Arc::clone(activation.entry());
        let configuration_digest = retained_entry.configuration_digest().to_owned();
        let original_entry = Arc::downgrade(&retained_entry);
        let resources = LiveResources {
            entry: Arc::clone(activation.entry()),
            activation,
            endpoint: endpoint.take(),
        };
        sessions.machines.insert(
            machine,
            Arc::new(Session {
                owner,
                identity,
                configuration_digest,
                original_entry,
                resources: Mutex::new(Some(resources)),
                retained_entry: Mutex::new(Some(retained_entry)),
                attempt: Mutex::new(None),
                executions: Arc::new(MachineExecutionActivities::default()),
            }),
        );
        Ok(())
    }

    /// Stop the registered exact boot under the persisted Stop generation.
    /// Caller cancellation only drops observation: the owned task keeps its
    /// Environment fence and result. A duplicate exact request observes that
    /// task; it never starts a second teardown. Unknown sessions fail closed.
    pub async fn stop<S: EnvironmentStateStore>(
        &self,
        lease: &EnvironmentControllerLease,
        state: &S,
        operation: &EnvironmentLifecycleOperation,
        machine_id: &MachineId,
        timeout: Duration,
    ) -> Result<MachineSessionStopReceipt, MachineLiveSessionError> {
        self.stop_for_kind(
            lease,
            state,
            operation,
            machine_id,
            timeout,
            EnvironmentLifecycleKind::Stop,
        )
        .await
    }

    /// Drain one exact registered boot for a pending durable Delete step.
    /// Delete keeps its `None` Machine target; physical quiescence does not
    /// acknowledge deletion or authorize removal of another Machine's state.
    pub(crate) async fn stop_for_delete<S: EnvironmentStateStore>(
        &self,
        lease: &EnvironmentControllerLease,
        state: &S,
        operation: &EnvironmentLifecycleOperation,
        machine_id: &MachineId,
        timeout: Duration,
    ) -> Result<MachineSessionStopReceipt, MachineLiveSessionError> {
        self.stop_for_kind(
            lease,
            state,
            operation,
            machine_id,
            timeout,
            EnvironmentLifecycleKind::Delete,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)] // Stop and Delete share only the physical drain.
    async fn stop_for_kind<S: EnvironmentStateStore>(
        &self,
        lease: &EnvironmentControllerLease,
        state: &S,
        operation: &EnvironmentLifecycleOperation,
        machine_id: &MachineId,
        timeout: Duration,
        kind: EnvironmentLifecycleKind,
    ) -> Result<MachineSessionStopReceipt, MachineLiveSessionError> {
        if timeout.is_zero() || timeout > Duration::from_secs(300) {
            return Err(error(
                "Stop timeout must be greater than zero and at most 300 seconds",
            ));
        }
        let owner = ResourceOwner {
            project_id: operation.project_id.clone(),
            environment_id: operation.environment_id.clone(),
            machine_id: Some(machine_id.clone()),
        };
        lease.require_owner(&owner).map_err(error)?;
        if operation.kind != kind {
            return Err(error(
                "live-session teardown requires its exact durable operation kind",
            ));
        }
        let session = {
            let mut sessions = self.sessions.lock().map_err(error)?;
            require_controller(&mut sessions.controller, lease.controller_identity())?;
            sessions.machines.get(machine_id).cloned().ok_or_else(|| {
                error("no authoritative live session; restart recovery is uncertain")
            })?
        };
        if session.owner != owner {
            return Err(error(
                "registered session belongs to another Environment or Project",
            ));
        }
        let receiver = {
            let mut attempt = session.attempt.lock().map_err(error)?;
            if kind == EnvironmentLifecycleKind::Delete {
                require_delete_fence(state, operation, machine_id, &session)?;
            }
            if let Some(existing) = attempt.as_ref() {
                if !same_stop_request(&existing.operation, operation, machine_id) {
                    if kind != EnvironmentLifecycleKind::Delete {
                        return Err(error("another Stop request owns the live session"));
                    }
                    let receipt =
                        rebind_completed_stop(state, operation, machine_id, &session, existing)?;
                    let (_, receiver) = watch::channel(Some(Arc::new(Ok(receipt))));
                    *attempt = Some(StopAttempt {
                        operation: operation.clone(),
                        result: receiver.clone(),
                        _fence: Arc::new(Mutex::new(None)),
                    });
                    receiver
                } else {
                    existing.result.clone()
                }
            } else {
                let step = operation
                    .machine_steps
                    .iter()
                    .find(|step| &step.machine_id == machine_id)
                    .ok_or_else(|| error("Machine is not in the Stop operation"))?;
                let records = [
                    MachineRuntimeRegistry::<MacosRuntimeBackend>::reservation(&owner)
                        .map_err(error)?,
                    MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(&owner)
                        .map_err(error)?,
                ];
                let (environment, _) = state
                    .access(|store| {
                        store.require_current_machine_lifecycle_fence(operation, step, &records)
                    })
                    .map_err(error)?;
                let machine = environment
                    .machines
                    .iter()
                    .find(|machine| &machine.machine_id == machine_id)
                    .ok_or_else(|| error("Machine disappeared from fenced Environment"))?;
                if let Some(persisted) = &machine.runtime_identity {
                    let exact: StackRuntimeIdentity =
                        serde_json::from_str(&persisted.opaque_id).map_err(error)?;
                    if exact != session.identity {
                        return Err(error(
                            "persisted Machine activation differs from registered runtime",
                        ));
                    }
                } else if machine.incarnation.is_some() {
                    return Err(error("persisted incarnation has no exact runtime identity"));
                }
                // Failed Up may have an owned, registered boot that never
                // advertised Ready. Its registration still proves the exact
                // original runtime; absent persisted evidence never authorizes
                // constructing a new Runtime or selecting a VM by name.
                let resources = session
                    .resources
                    .lock()
                    .map_err(error)?
                    .take()
                    .ok_or_else(|| error("live session resources were already consumed"))?;
                let operation = operation.clone();
                let owner = owner.clone();
                let identity = session.identity.clone();
                let fence = Arc::new(Mutex::new(Some(lease.retained_guard())));
                let (sender, receiver) = watch::channel(None);
                *attempt = Some(StopAttempt {
                    operation: operation.clone(),
                    result: receiver.clone(),
                    _fence: Arc::clone(&fence),
                });
                let retained_session = Arc::clone(&session);
                tokio::spawn(run_owned_stop(sender, fence, timeout, async move {
                    retained_session.executions.cancel_and_drain().await?;
                    let result = stop_resources(resources, owner, identity, operation).await;
                    if result.is_ok() {
                        retained_session
                            .retained_entry
                            .lock()
                            .map_err(|_| "original Runtime anchor poisoned".to_string())?
                            .take();
                    }
                    result
                }));
                receiver
            }
        };
        observe_stop(receiver).await
    }
}

fn require_retired_identity(
    retired: &RetiredDelete,
    owner: &ResourceOwner,
    operation: &EnvironmentLifecycleOperation,
    machine: &MachineId,
) -> Result<(), MachineLiveSessionError> {
    if retired.proof.schema_version != 1
        || &retired.proof.owner != owner
        || !same_stop_request(&retired.proof.operation, operation, machine)
    {
        return Err(error(
            "retired Delete authority belongs to another owner or request",
        ));
    }
    Ok(())
}

fn load_delete_environment<S: EnvironmentStateStore>(
    state: &S,
    owner: &ResourceOwner,
) -> Result<vz_runtime_contract::EnvironmentInstance, MachineLiveSessionError> {
    state
        .access(|store| store.load_project_state_snapshot(owner.project_id.as_str()))
        .map_err(error)?
        .ok_or_else(|| error("Delete Project disappeared"))?
        .environments
        .into_iter()
        .find(|environment| environment.environment_id == owner.environment_id)
        .ok_or_else(|| error("Delete Environment disappeared"))
}

fn delete_step<'a>(
    operation: &'a EnvironmentLifecycleOperation,
    machine: &MachineId,
) -> Result<&'a vz_runtime_contract::MachineLifecycleStep, MachineLiveSessionError> {
    if operation.kind != EnvironmentLifecycleKind::Delete {
        return Err(error("Delete operation required"));
    }
    let step = operation
        .machine_steps
        .iter()
        .find(|step| &step.machine_id == machine)
        .ok_or_else(|| error("Machine absent from Delete operation"))?;
    if step.target_state.is_some() {
        return Err(error("Delete Machine target must remain None"));
    }
    Ok(step)
}

fn require_delete_journal<S: EnvironmentStateStore>(
    state: &S,
    operation: &EnvironmentLifecycleOperation,
    machine: &MachineId,
) -> Result<vz_runtime_contract::EnvironmentInstance, MachineLiveSessionError> {
    let step = delete_step(operation, machine)?;
    operation.validate_structure().map_err(error)?;
    let owner = ResourceOwner {
        project_id: operation.project_id.clone(),
        environment_id: operation.environment_id.clone(),
        machine_id: Some(machine.clone()),
    };
    let environment = load_delete_environment(state, &owner)?;
    let actual = state
        .access(|store| store.load_environment_lifecycle(operation.operation_id.as_str()))
        .map_err(error)?
        .ok_or_else(|| error("Delete journal absent"))?;
    actual
        .validate_against_environment(&environment)
        .map_err(error)?;
    if !same_stop_request(&actual, operation, machine)
        || actual.status != vz_runtime_contract::EnvironmentLifecycleStatus::Running
        || actual
            .machine_steps
            .iter()
            .find(|row| &row.machine_id == machine)
            != Some(step)
        || environment.active_operation_id.as_ref() != Some(&operation.operation_id)
        || environment.lifecycle_generation != operation.generation
    {
        return Err(error("Delete journal is stale, foreign, or not active"));
    }
    let current = environment
        .machines
        .iter()
        .find(|row| &row.machine_id == machine)
        .ok_or_else(|| error("Delete Machine disappeared"))?;
    if current.incarnation != step.expected_incarnation {
        return Err(error("Delete Machine incarnation changed"));
    }
    Ok(environment)
}

fn require_acknowledged_delete(
    environment: &vz_runtime_contract::EnvironmentInstance,
    operation: &EnvironmentLifecycleOperation,
    machine: &MachineId,
) -> Result<(), MachineLiveSessionError> {
    let step = delete_step(operation, machine)?;
    if environment.project_id != operation.project_id
        || environment.environment_id != operation.environment_id
        || environment.definition_digest != operation.definition_digest
        || environment.lifecycle_generation != operation.generation
        || environment.active_operation_id.as_ref() != Some(&operation.operation_id)
        || operation.status != vz_runtime_contract::EnvironmentLifecycleStatus::Running
        || step.status != vz_runtime_contract::LifecycleStepStatus::Succeeded
        || !environment
            .machines
            .iter()
            .any(|row| &row.machine_id == machine && row.incarnation == step.expected_incarnation)
    {
        return Err(error(
            "missing session has no current acknowledged Delete quiescence",
        ));
    }
    Ok(())
}

fn require_positive_stop(
    environment: &vz_runtime_contract::EnvironmentInstance,
    machine: &MachineId,
    operation: &EnvironmentLifecycleOperation,
) -> Result<(), MachineLiveSessionError> {
    let current = environment
        .machines
        .iter()
        .find(|row| &row.machine_id == machine)
        .ok_or_else(|| error("positive Stop Machine absent"))?;
    let step = operation
        .machine_steps
        .iter()
        .find(|row| &row.machine_id == machine)
        .ok_or_else(|| error("positive Stop step absent"))?;
    if environment.active_operation_id.is_some()
        || environment.project_id != operation.project_id
        || environment.environment_id != operation.environment_id
        || environment.definition_digest != operation.definition_digest
        || environment.lifecycle_generation != operation.generation
        || operation.kind != EnvironmentLifecycleKind::Stop
        || !matches!(
            operation.status,
            vz_runtime_contract::EnvironmentLifecycleStatus::Succeeded
                | vz_runtime_contract::EnvironmentLifecycleStatus::Failed
        )
        || current.state != vz_runtime_contract::MachineState::Stopped
        || step.status != vz_runtime_contract::LifecycleStepStatus::Succeeded
        || step.target_state != Some(vz_runtime_contract::MachineState::Stopped)
        || step.expected_incarnation != current.incarnation
    {
        return Err(error("Stopped state is not an exact positive Stop journal"));
    }
    Ok(())
}

fn require_delete_store_fence<S: EnvironmentStateStore>(
    state: &S,
    operation: &EnvironmentLifecycleOperation,
    machine: &MachineId,
    owner: &ResourceOwner,
) -> Result<(), MachineLiveSessionError> {
    let records =
        [MachineRuntimeRegistry::<MacosRuntimeBackend>::reservation(owner).map_err(error)?];
    let (_, actual) = state
        .access(|store| {
            store.require_current_machine_lifecycle_fence(
                operation,
                delete_step(operation, machine)
                    .map_err(|error| vz_stack::StackError::InvalidSpec(error.to_string()))?,
                &records,
            )
        })
        .map_err(error)?;
    if !same_stop_request(&actual, operation, machine) {
        return Err(error("Delete store fence differs from request"));
    }
    Ok(())
}

fn validate_quiescence_evidence(
    proof: &DeleteQuiescenceEvidence,
    claim: &MachineStoreDeletePreflight,
    operation: &EnvironmentLifecycleOperation,
    environment: &vz_runtime_contract::EnvironmentInstance,
    machine: &MachineId,
) -> Result<(), MachineLiveSessionError> {
    let step = delete_step(operation, machine)?;
    proof.operation.validate_structure().map_err(error)?;
    if proof.schema_version != 1
        || &proof.owner != claim.owner()
        || proof.owner.machine_id.as_ref() != Some(machine)
        || proof.owner.project_id != operation.project_id
        || proof.owner.environment_id != operation.environment_id
        || proof.configuration_digest != claim.configuration_digest()
        || !same_stop_request(&proof.operation, operation, machine)
    {
        return Err(error("persisted Delete quiescence scope changed"));
    }
    match &proof.authority {
        DeleteQuiescenceAuthority::Drained {
            runtime_identity,
            endpoint,
            outcome,
        } => {
            let current = environment
                .machines
                .iter()
                .find(|row| &row.machine_id == machine)
                .ok_or_else(|| error("Delete Machine absent"))?;
            if let Some(identity) = &current.runtime_identity {
                if serde_json::from_str::<StackRuntimeIdentity>(&identity.opaque_id)
                    .map_err(error)?
                    != *runtime_identity
                {
                    return Err(error("Delete quiescence runtime changed"));
                }
            } else if current.incarnation.is_some() {
                return Err(error("Delete runtime identity missing"));
            }
            let reservation =
                MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(&proof.owner)
                    .map_err(error)?;
            if runtime_identity.stack_id != reservation.resource_id
                || matches!(
                    outcome,
                    StackRuntimeShutdownOutcome::ReplacementPresent { .. }
                )
                || endpoint.as_ref().is_some_and(|row| {
                    !row.socket_removed
                        || row.active_connections != 0
                        || row
                            .completed_connections
                            .checked_add(row.cancelled_connections)
                            .and_then(|sum| sum.checked_add(row.failed_connections))
                            != Some(row.accepted_connections)
                })
            {
                return Err(error("Delete proof retains uncertain VM/endpoint effects"));
            }
        }
        DeleteQuiescenceAuthority::Absent { authority } => match authority {
            DeleteAbsenceAuthority::PositiveStop { operation: prior } => {
                let prior_step = prior
                    .machine_steps
                    .iter()
                    .find(|row| &row.machine_id == machine)
                    .ok_or_else(|| error("Stop proof step absent"))?;
                if prior.kind != EnvironmentLifecycleKind::Stop
                    || prior.project_id != operation.project_id
                    || prior.environment_id != operation.environment_id
                    || prior.definition_digest != operation.definition_digest
                    || prior.generation.checked_add(1) != Some(operation.generation)
                    || !matches!(
                        prior.status,
                        vz_runtime_contract::EnvironmentLifecycleStatus::Succeeded
                            | vz_runtime_contract::EnvironmentLifecycleStatus::Failed
                    )
                    || prior_step.status != vz_runtime_contract::LifecycleStepStatus::Succeeded
                    || prior_step.target_state != Some(vz_runtime_contract::MachineState::Stopped)
                    || prior_step.expected_incarnation != step.expected_incarnation
                    || step.initial_state != vz_runtime_contract::MachineState::Stopped
                {
                    return Err(error("Delete positive Stop authority changed"));
                }
            }
            DeleteAbsenceAuthority::BootNotDispatched { proof } => {
                if proof.schema_version != 1
                    || proof.project_id != operation.project_id
                    || proof.environment_id != operation.environment_id
                    || &proof.machine_id != machine
                    || proof.definition_digest != operation.definition_digest
                    || proof.generation.checked_add(1) != Some(operation.generation)
                    || proof.expected_incarnation != step.expected_incarnation
                {
                    return Err(error("Delete non-dispatch authority changed"));
                }
            }
        },
        DeleteQuiescenceAuthority::AcknowledgedDelete => {
            require_acknowledged_delete(environment, operation, machine)?
        }
    }
    Ok(())
}

fn require_delete_fence<S: EnvironmentStateStore>(
    state: &S,
    operation: &EnvironmentLifecycleOperation,
    machine_id: &MachineId,
    session: &Session,
) -> Result<(), MachineLiveSessionError> {
    if operation.kind != EnvironmentLifecycleKind::Delete {
        return Err(error(
            "Delete quiescence requires a durable Delete operation",
        ));
    }
    let step = operation
        .machine_steps
        .iter()
        .find(|step| &step.machine_id == machine_id)
        .ok_or_else(|| error("Machine absent from Delete operation"))?;
    if step.target_state.is_some() {
        return Err(error("Delete Machine target must remain None"));
    }
    let records = [
        MachineRuntimeRegistry::<MacosRuntimeBackend>::reservation(&session.owner)
            .map_err(error)?,
        MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(&session.owner)
            .map_err(error)?,
    ];
    let (environment, actual) = state
        .access(|store| store.require_current_machine_lifecycle_fence(operation, step, &records))
        .map_err(error)?;
    if !same_stop_request(&actual, operation, machine_id) {
        return Err(error("Delete request differs from authoritative journal"));
    }
    let machine = environment
        .machines
        .iter()
        .find(|machine| &machine.machine_id == machine_id)
        .ok_or_else(|| error("Delete Machine disappeared"))?;
    if machine.incarnation != step.expected_incarnation {
        return Err(error("Delete expected incarnation changed"));
    }
    if let Some(persisted) = &machine.runtime_identity {
        let identity: StackRuntimeIdentity =
            serde_json::from_str(&persisted.opaque_id).map_err(error)?;
        if identity != session.identity {
            return Err(error(
                "Delete runtime identity differs from exact live owner",
            ));
        }
    } else if machine.incarnation.is_some() {
        return Err(error(
            "Delete incarnation has no persisted runtime identity",
        ));
    }
    Ok(())
}

fn positive_session_receipt(
    session: &Session,
    attempt: &StopAttempt,
) -> Result<MachineSessionStopReceipt, MachineLiveSessionError> {
    if session.resources.lock().map_err(error)?.is_some()
        || session.retained_entry.lock().map_err(error)?.is_some()
        || attempt._fence.lock().map_err(error)?.is_some()
    {
        return Err(error(
            "Delete cannot retire retained live or uncertain resources",
        ));
    }
    let receipt = attempt
        .result
        .borrow()
        .as_ref()
        .ok_or_else(|| error("teardown has no terminal proof"))?
        .as_ref()
        .clone()
        .map_err(error)?;
    if receipt.owner != session.owner
        || receipt.runtime_identity != session.identity
        || receipt.operation_id != attempt.operation.operation_id.to_string()
        || receipt.generation != attempt.operation.generation
        || matches!(
            receipt.outcome,
            StackRuntimeShutdownOutcome::ReplacementPresent { .. }
        )
        || receipt
            .endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint.active_connections != 0 || !endpoint.socket_removed)
    {
        return Err(error(
            "teardown receipt does not prove this exact session quiescent",
        ));
    }
    Ok(receipt)
}

fn rebind_completed_stop<S: EnvironmentStateStore>(
    state: &S,
    delete: &EnvironmentLifecycleOperation,
    machine: &MachineId,
    session: &Session,
    attempt: &StopAttempt,
) -> Result<MachineSessionStopReceipt, MachineLiveSessionError> {
    let prior = state
        .access(|store| store.load_environment_lifecycle(attempt.operation.operation_id.as_str()))
        .map_err(error)?
        .ok_or_else(|| error("original Stop journal disappeared"))?;
    let previous_step = prior
        .machine_steps
        .iter()
        .find(|step| &step.machine_id == machine)
        .ok_or_else(|| error("original Stop has no exact Machine step"))?;
    let next_step = delete
        .machine_steps
        .iter()
        .find(|step| &step.machine_id == machine)
        .ok_or_else(|| error("Delete has no exact Machine step"))?;
    if prior.kind != EnvironmentLifecycleKind::Stop
        || !same_stop_request(&prior, &attempt.operation, machine)
        || prior.generation.checked_add(1) != Some(delete.generation)
        || prior.project_id != delete.project_id
        || prior.environment_id != delete.environment_id
        || prior.definition_digest != delete.definition_digest
        || !matches!(
            prior.status,
            vz_runtime_contract::EnvironmentLifecycleStatus::Succeeded
                | vz_runtime_contract::EnvironmentLifecycleStatus::Failed
        )
        || previous_step.status != vz_runtime_contract::LifecycleStepStatus::Succeeded
        || previous_step.target_state != Some(vz_runtime_contract::MachineState::Stopped)
        || previous_step.expected_incarnation != next_step.expected_incarnation
        || next_step.initial_state != vz_runtime_contract::MachineState::Stopped
    {
        return Err(error(
            "Delete cannot reuse a foreign, unresolved, or stale Stop attempt",
        ));
    }
    let old = positive_session_receipt(session, attempt)?;
    Ok(MachineSessionStopReceipt {
        owner: old.owner,
        operation_id: delete.operation_id.to_string(),
        generation: delete.generation,
        runtime_identity: old.runtime_identity,
        endpoint: old.endpoint,
        outcome: StackRuntimeShutdownOutcome::AlreadyAbsent,
    })
}

fn require_available_session(
    sessions: &Sessions,
    owner: &ResourceOwner,
    identity: &StackRuntimeIdentity,
) -> Result<(), MachineLiveSessionError> {
    let machine = owner
        .machine_id
        .as_ref()
        .ok_or_else(|| error("Machine owner required"))?;
    if let Some(existing) = sessions.machines.get(machine) {
        let attempt = existing.attempt.lock().map_err(error)?;
        let stopped = attempt.as_ref().is_some_and(|attempt| {
            attempt
                .result
                .borrow()
                .as_ref()
                .is_some_and(|result| result.is_ok())
        });
        if !stopped || existing.owner != *owner || existing.identity == *identity {
            return Err(error(
                "Machine already has a live, uncertain, or identical session",
            ));
        }
    }
    if sessions
        .machines
        .values()
        .any(|session| session.identity == *identity)
    {
        return Err(error(
            "runtime incarnation is already owned by another session",
        ));
    }
    Ok(())
}

fn require_controller(
    bound: &mut Option<Arc<()>>,
    provided: &Arc<()>,
) -> Result<(), MachineLiveSessionError> {
    if let Some(bound) = bound {
        if !Arc::ptr_eq(bound, provided) {
            return Err(error(
                "session registry belongs to another Environment controller",
            ));
        }
    } else {
        *bound = Some(Arc::clone(provided));
    }
    Ok(())
}

fn same_stop_request(
    left: &EnvironmentLifecycleOperation,
    right: &EnvironmentLifecycleOperation,
    machine: &MachineId,
) -> bool {
    let step = |operation: &EnvironmentLifecycleOperation| {
        operation
            .machine_steps
            .iter()
            .find(|step| &step.machine_id == machine)
            .map(|step| {
                (
                    step.initial_state,
                    step.target_state,
                    step.expected_incarnation.clone(),
                )
            })
    };
    left.schema_version == right.schema_version
        && left.operation_id == right.operation_id
        && left.project_id == right.project_id
        && left.environment_id == right.environment_id
        && left.kind == right.kind
        && left.generation == right.generation
        && left.request_id == right.request_id
        && left.idempotency_key == right.idempotency_key
        && left.request_hash == right.request_hash
        && left.definition_digest == right.definition_digest
        && left.initial_state == right.initial_state
        && left.requested_target == right.requested_target
        && step(left).is_some()
        && step(left) == step(right)
}

fn successful_replay(
    attempt: &StopAttempt,
    operation: Option<&EnvironmentLifecycleOperation>,
    machine: &MachineId,
) -> bool {
    operation.is_some_and(|operation| same_stop_request(&attempt.operation, operation, machine))
        && attempt
            .result
            .borrow()
            .as_ref()
            .is_some_and(|result| result.is_ok())
}

async fn stop_resources(
    resources: LiveResources,
    owner: ResourceOwner,
    identity: StackRuntimeIdentity,
    operation: EnvironmentLifecycleOperation,
) -> StopResult {
    let LiveResources {
        entry,
        activation,
        endpoint,
    } = resources;
    let endpoint = match endpoint {
        Some(endpoint) => {
            let receipt = endpoint
                .shutdown()
                .await
                .map_err(|error| error.to_string())?;
            if receipt.active_connections != 0 || !receipt.socket_removed {
                return Err("endpoint did not complete exact joined teardown".into());
            }
            Some(receipt)
        }
        None => None,
    };
    // The original Runtime must outlive releasing this lifecycle reader.
    // Any externally retained activation reader causes a bounded timeout;
    // timeout is uncertainty, not evidence of physical absence.
    drop(activation);
    let outcome = entry
        .runtime()
        .inner()
        .shutdown_shared_vm_exact(&StackRuntimeShutdownRequest {
            schema_version: STACK_RUNTIME_SHUTDOWN_REQUEST_SCHEMA_VERSION,
            operation_id: operation.operation_id.to_string(),
            expected: identity.clone(),
        })
        .await
        .map_err(|error| error.to_string())?;
    if matches!(
        outcome,
        StackRuntimeShutdownOutcome::ReplacementPresent { .. }
    ) {
        return Err("original Runtime now contains a replacement incarnation; preserved".into());
    }
    Ok(MachineSessionStopReceipt {
        owner,
        operation_id: operation.operation_id.to_string(),
        generation: operation.generation,
        runtime_identity: identity,
        endpoint,
        outcome,
    })
}

async fn run_owned_stop(
    sender: watch::Sender<Option<Arc<StopResult>>>,
    fence: RetainedFence,
    timeout: Duration,
    effect: impl Future<Output = StopResult>,
) {
    let result = tokio::time::timeout(timeout, effect)
        .await
        .unwrap_or_else(|_| {
            Err("Stop timed out; physical state uncertain and Environment fence retained".into())
        });
    if result.is_ok() {
        match fence.lock() {
            Ok(mut fence) => {
                fence.take();
            }
            Err(_) => {
                sender.send_replace(Some(Arc::new(Err("Stop fence poisoned".into()))));
                return;
            }
        }
    }
    sender.send_replace(Some(Arc::new(result)));
}

async fn observe_stop(
    mut receiver: watch::Receiver<Option<Arc<StopResult>>>,
) -> Result<MachineSessionStopReceipt, MachineLiveSessionError> {
    loop {
        if let Some(result) = receiver.borrow().as_ref() {
            return result.as_ref().clone().map_err(error);
        }
        receiver
            .changed()
            .await
            .map_err(|_| error("owned Stop task ended without a receipt"))?;
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::environment_runtime_controller::EnvironmentRuntimeController;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;
    use vz_runtime_contract::{
        EnvironmentId, EnvironmentLifecycleStatus, EnvironmentState, LifecycleOperationId,
        LifecycleStepStatus, MachineLifecycleStep, MachineState, ProjectId,
        TOPOLOGY_SCHEMA_VERSION,
    };

    fn owner() -> ResourceOwner {
        ResourceOwner {
            project_id: ProjectId::generate(),
            environment_id: EnvironmentId::generate(),
            machine_id: Some(MachineId::generate()),
        }
    }

    fn receipt(owner: ResourceOwner) -> MachineSessionStopReceipt {
        MachineSessionStopReceipt {
            owner,
            operation_id: "op_owned_stop_test".into(),
            generation: 1,
            runtime_identity: StackRuntimeIdentity::new("vm-test").unwrap(),
            endpoint: None,
            outcome: StackRuntimeShutdownOutcome::Stopped,
        }
    }

    fn operation(owner: &ResourceOwner) -> EnvironmentLifecycleOperation {
        EnvironmentLifecycleOperation {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            operation_id: LifecycleOperationId::generate(),
            project_id: owner.project_id.clone(),
            environment_id: owner.environment_id.clone(),
            kind: EnvironmentLifecycleKind::Stop,
            generation: 1,
            request_id: "req-owned-stop".into(),
            idempotency_key: "idem-owned-stop".into(),
            request_hash: "sha256:owned-stop".into(),
            definition_digest: "sha256:definition".into(),
            initial_state: EnvironmentState::Failed,
            requested_target: EnvironmentState::Stopped,
            status: EnvironmentLifecycleStatus::Running,
            machine_steps: vec![MachineLifecycleStep {
                machine_id: owner.machine_id.clone().unwrap(),
                initial_state: MachineState::Failed,
                target_state: Some(MachineState::Stopped),
                expected_incarnation: None,
                resulting_incarnation: None,
                resulting_activation: None,
                status: LifecycleStepStatus::Pending,
                failure_reason: None,
            }],
            cleanup_steps: vec![],
            created_at: 1,
            updated_at: 1,
            completed_at: None,
        }
    }

    fn delete_fixture() -> (
        vz_runtime_contract::EnvironmentInstance,
        EnvironmentLifecycleOperation,
        EnvironmentLifecycleOperation,
    ) {
        let owner = owner();
        let mut stop = operation(&owner);
        stop.status = EnvironmentLifecycleStatus::Succeeded;
        stop.completed_at = Some(2);
        stop.machine_steps[0].status = LifecycleStepStatus::Succeeded;
        let machine = vz_runtime_contract::MachineInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            machine_id: owner.machine_id.clone().unwrap(),
            environment_id: owner.environment_id.clone(),
            name: "main".into(),
            profile: vz_runtime_contract::MachineProfile::Hardened,
            target: vz_runtime_contract::TargetSpec {
                os: vz_runtime_contract::OperatingSystem::Linux,
                arch: vz_runtime_contract::Architecture::Aarch64,
                image: "test".into(),
                version: None,
                channel: None,
                digest: None,
            },
            resources: Default::default(),
            requested_capabilities: Default::default(),
            negotiated_capabilities: Default::default(),
            backend: None,
            incarnation: None,
            runtime_identity: None,
            docker_context: None,
            state: MachineState::Stopped,
            legacy_sandbox_id: None,
        };
        let environment = vz_runtime_contract::EnvironmentInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: owner.project_id.clone(),
            environment_id: owner.environment_id.clone(),
            name: "test".into(),
            definition_digest: stop.definition_digest.clone(),
            state: EnvironmentState::Stopped,
            lifecycle_generation: stop.generation,
            active_operation_id: None,
            bindings: vec![],
            machines: vec![machine],
            networks: vec![],
            endpoints: vec![],
            ownership: vec![],
            legacy_migration: None,
            created_at: 1,
            updated_at: 2,
        };
        let mut delete = operation(&owner);
        delete.operation_id = LifecycleOperationId::generate();
        delete.kind = EnvironmentLifecycleKind::Delete;
        delete.generation = 2;
        delete.initial_state = EnvironmentState::Stopped;
        delete.requested_target = EnvironmentState::Deleted;
        delete.machine_steps[0].initial_state = MachineState::Stopped;
        delete.machine_steps[0].target_state = None;
        (environment, stop, delete)
    }

    // Runtime-free host fixture. Passive teardown receipts below test the
    // ownership handoff only; no VM or endpoint execution is claimed.
    fn recovery_fixture(
        up: bool,
    ) -> (
        tempfile::TempDir,
        vz_stack::StateStore,
        MachineRuntimeRegistry<MacosRuntimeBackend>,
        EnvironmentLifecycleOperation,
        ResourceOwner,
    ) {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::Builder::new()
            .prefix("vz-del-retry-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let root_path = root.path().canonicalize().unwrap();
        let store = vz_stack::StateStore::open(&root_path.join("state.db")).unwrap();
        let definition: vz_runtime_contract::ProjectDefinition = serde_json::from_value(serde_json::json!({
            "schema_version":1,"project_id":ProjectId::generate(),"name":"delete-retry",
            "environment":{"schema_version":1,"machines":[{"schema_version":1,"name":"app","profile":"hardened",
                "target":{"os":"linux","arch":"aarch64","image":"fixture"}}]}
        })).unwrap();
        let mut environment = definition.instantiate_environment("test", 1).unwrap();
        if !up {
            environment.state = EnvironmentState::Failed;
            environment.machines[0].state = MachineState::Failed;
        }
        let owner = ResourceOwner {
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(environment.machines[0].machine_id.clone()),
        };
        let reservation =
            MachineRuntimeRegistry::<MacosRuntimeBackend>::reservation(&owner).unwrap();
        environment.ownership.push(reservation.clone());
        environment
            .ownership
            .push(MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(&owner).unwrap());
        store
            .save_project_state(&vz_runtime_contract::ProjectState {
                schema_version: 1,
                definition,
                environments: vec![environment],
            })
            .unwrap();
        let registry_path = root_path.join("registry");
        std::fs::create_dir(&registry_path).unwrap();
        std::fs::set_permissions(&registry_path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let registry = MachineRuntimeRegistry::new(registry_path).unwrap();
        registry
            .acquire_store(
                &owner,
                &reservation,
                Some(&format!("sha256:{}", "b".repeat(64))),
                crate::machine_runtime_registry::MachineRuntimeAdmission::CreateOrOpen,
            )
            .unwrap();
        let operation = store
            .begin_environment_lifecycle(
                owner.environment_id.as_str(),
                if up {
                    EnvironmentLifecycleKind::Up
                } else {
                    EnvironmentLifecycleKind::Delete
                },
                "recovery-request",
                "recovery-key",
                &format!("sha256:{}", "c".repeat(64)),
                2,
            )
            .unwrap();
        (root, store, registry, operation, owner)
    }

    #[tokio::test]
    async fn retired_positive_receipt_survives_failed_ack_and_exact_pending_retry() {
        let (_root, state, registry, operation, owner) = recovery_fixture(false);
        let machine = owner.machine_id.as_ref().unwrap();
        let controller =
            crate::environment_runtime_controller::EnvironmentRuntimeController::default();
        let lease = controller
            .acquire(&owner.project_id, &owner.environment_id)
            .await
            .unwrap();
        let reservation =
            MachineRuntimeRegistry::<MacosRuntimeBackend>::reservation(&owner).unwrap();
        let claim = registry.preflight_delete(&owner, &reservation).unwrap();
        let sessions = MachineLiveSessions::default();
        let identity = StackRuntimeIdentity::new(
            MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(&owner)
                .unwrap()
                .resource_id,
        )
        .unwrap();
        let mut session = passive(owner.clone(), identity.clone());
        Arc::get_mut(&mut session).unwrap().configuration_digest =
            claim.configuration_digest().into();
        let receipt = MachineSessionStopReceipt {
            owner: owner.clone(),
            operation_id: operation.operation_id.to_string(),
            generation: operation.generation,
            runtime_identity: identity,
            endpoint: None,
            outcome: StackRuntimeShutdownOutcome::Stopped,
        };
        let (_, receiver) = watch::channel(Some(Arc::new(Ok(receipt))));
        *session.attempt.lock().unwrap() = Some(StopAttempt {
            operation: operation.clone(),
            result: receiver,
            _fence: Arc::new(Mutex::new(None)),
        });
        sessions
            .sessions
            .lock()
            .unwrap()
            .machines
            .insert(machine.clone(), session);
        let token = sessions
            .retire_for_delete(&lease, &state, &operation, machine, &claim, None)
            .unwrap();
        let evidence = token.evidence();
        assert!(
            !sessions
                .sessions
                .lock()
                .unwrap()
                .machines
                .contains_key(machine)
        );
        // Inject acknowledgement loss: discard observation/lease without ever
        // updating the durable pending step, then retry under a fresh guard.
        drop(token);
        drop(lease);
        let lease = tokio::time::timeout(
            Duration::from_secs(1),
            controller.acquire(&owner.project_id, &owner.environment_id),
        )
        .await
        .unwrap()
        .unwrap();
        let environment = load_delete_environment(&state, &owner).unwrap();
        let admission = sessions
            .prepare_delete_absence(&lease, &state, &environment, machine)
            .unwrap()
            .unwrap();
        let replay = sessions
            .retire_for_delete(&lease, &state, &operation, machine, &claim, Some(admission))
            .unwrap();
        assert_eq!(replay.evidence(), evidence);
        assert_eq!(
            state
                .load_environment_lifecycle(operation.operation_id.as_str())
                .unwrap()
                .unwrap()
                .machine_steps[0]
                .status,
            LifecycleStepStatus::Pending
        );
        let step = &operation.machine_steps[0];
        state
            .acknowledge_environment_machine_step(
                &vz_runtime_contract::MachineLifecycleStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    machine_id: machine.clone(),
                    initial_state: step.initial_state,
                    target_state: None,
                    expected_incarnation: step.expected_incarnation.clone(),
                    resulting_incarnation: None,
                    resulting_activation: None,
                    result: vz_runtime_contract::LifecycleStepResult::Succeeded,
                },
                3,
            )
            .unwrap();
    }

    #[tokio::test]
    async fn restarted_pending_delete_uses_only_exact_unconsumed_failed_up_proof() {
        for consumed in [false, true] {
            let (root, state, registry, up, owner) = recovery_fixture(true);
            let machine = owner.machine_id.as_ref().unwrap();
            state
                .record_machine_boot_non_dispatch(&up, machine)
                .unwrap();
            if consumed {
                state
                    .consume_machine_boot_non_dispatch(&up, machine)
                    .unwrap();
            }
            let step = &up.machine_steps[0];
            state
                .acknowledge_environment_machine_step(
                    &vz_runtime_contract::MachineLifecycleStepAcknowledgement {
                        operation_id: up.operation_id.clone(),
                        generation: up.generation,
                        machine_id: machine.clone(),
                        initial_state: step.initial_state,
                        target_state: step.target_state,
                        expected_incarnation: None,
                        resulting_incarnation: None,
                        resulting_activation: None,
                        result: vz_runtime_contract::LifecycleStepResult::Failed {
                            reason: "fixture boot not dispatched".into(),
                        },
                    },
                    3,
                )
                .unwrap();
            state
                .finish_environment_lifecycle(up.operation_id.as_str(), up.generation, 4)
                .unwrap();
            let delete = state
                .begin_environment_lifecycle(
                    owner.environment_id.as_str(),
                    EnvironmentLifecycleKind::Delete,
                    "delete-restart-request",
                    "delete-restart-key",
                    &format!("sha256:{}", "d".repeat(64)),
                    5,
                )
                .unwrap();
            drop(state);
            let state = vz_stack::StateStore::open(&root.path().join("state.db")).unwrap();
            let sessions = MachineLiveSessions::default();
            let controller =
                crate::environment_runtime_controller::EnvironmentRuntimeController::default();
            let lease = controller
                .acquire(&owner.project_id, &owner.environment_id)
                .await
                .unwrap();
            let current = load_delete_environment(&state, &owner).unwrap();
            let result = sessions.prepare_delete_absence(&lease, &state, &current, machine);
            if consumed {
                assert!(result.is_err());
                continue;
            }
            let admission = result.unwrap().unwrap();
            let claim = registry
                .preflight_delete(
                    &owner,
                    &MachineRuntimeRegistry::<MacosRuntimeBackend>::reservation(&owner).unwrap(),
                )
                .unwrap();
            let token = sessions
                .retire_for_delete(&lease, &state, &delete, machine, &claim, Some(admission))
                .unwrap();
            assert_eq!(token.evidence()["authority"]["kind"], "absent");
            assert_eq!(
                token.evidence()["authority"]["authority"]["kind"],
                "boot_not_dispatched"
            );
            assert!(token.runtime_entry_address().is_none());
        }
    }

    #[test]
    fn delete_requires_none_target_and_preserves_strict_stop_kind() {
        let (_, _, mut delete) = delete_fixture();
        let machine = delete.machine_steps[0].machine_id.clone();
        assert!(delete_step(&delete, &machine).is_ok());
        delete.machine_steps[0].target_state = Some(MachineState::Stopped);
        assert!(delete_step(&delete, &machine).is_err());
        delete.machine_steps[0].target_state = None;
        delete.kind = EnvironmentLifecycleKind::Stop;
        assert!(delete_step(&delete, &machine).is_err());
    }

    #[test]
    fn absent_stopped_machine_needs_exact_successful_stop_not_state_alone() {
        let (environment, stop, _) = delete_fixture();
        let machine = environment.machines[0].machine_id.clone();
        assert!(require_positive_stop(&environment, &machine, &stop).is_ok());
        for mutate in 0..8 {
            let mut env = environment.clone();
            let mut prior = stop.clone();
            match mutate {
                0 => env.lifecycle_generation += 1,
                1 => env.active_operation_id = Some(LifecycleOperationId::generate()),
                2 => env.project_id = ProjectId::generate(),
                3 => env.definition_digest.push('x'),
                4 => prior.machine_steps[0].status = LifecycleStepStatus::Failed,
                5 => prior.status = EnvironmentLifecycleStatus::Running,
                6 => prior.kind = EnvironmentLifecycleKind::Up,
                7 => env.machines[0].state = MachineState::Failed,
                _ => unreachable!(),
            }
            assert!(
                require_positive_stop(&env, &machine, &prior).is_err(),
                "mutation {mutate}"
            );
        }
        let mut sibling_failure = stop.clone();
        sibling_failure.status = EnvironmentLifecycleStatus::Failed;
        assert!(require_positive_stop(&environment, &machine, &sibling_failure).is_ok());
    }

    #[test]
    fn acknowledged_delete_replay_requires_current_owner_generation_and_step() {
        let (mut environment, _, mut delete) = delete_fixture();
        let machine = environment.machines[0].machine_id.clone();
        environment.lifecycle_generation = delete.generation;
        environment.active_operation_id = Some(delete.operation_id.clone());
        assert!(require_acknowledged_delete(&environment, &delete, &machine).is_err());
        delete.machine_steps[0].status = LifecycleStepStatus::Succeeded;
        assert!(require_acknowledged_delete(&environment, &delete, &machine).is_ok());
        for mutate in 0..7 {
            let mut env = environment.clone();
            let mut op = delete.clone();
            match mutate {
                0 => env.lifecycle_generation += 1,
                1 => env.active_operation_id = None,
                2 => env.active_operation_id = Some(LifecycleOperationId::generate()),
                3 => env.project_id = ProjectId::generate(),
                4 => op.status = EnvironmentLifecycleStatus::Failed,
                5 => op.machine_steps[0].status = LifecycleStepStatus::Running,
                6 => op.machine_steps[0].target_state = Some(MachineState::Stopped),
                _ => unreachable!(),
            }
            assert!(
                require_acknowledged_delete(&env, &op, &machine).is_err(),
                "mutation {mutate}"
            );
        }
    }

    #[test]
    fn delete_positive_receipt_rejects_unknown_replacement_and_changed_identity() {
        let owner = owner();
        let session = passive(
            owner.clone(),
            StackRuntimeIdentity::new("vm-exact").unwrap(),
        );
        let op = operation(&owner);
        let good = MachineSessionStopReceipt {
            owner: owner.clone(),
            operation_id: op.operation_id.to_string(),
            generation: op.generation,
            runtime_identity: session.identity.clone(),
            endpoint: Some(MachineDockerEndpointShutdown {
                socket_removed: true,
                ..Default::default()
            }),
            outcome: StackRuntimeShutdownOutcome::Stopped,
        };
        let (sender, receiver) = watch::channel(Some(Arc::new(Ok(good.clone()))));
        let attempt = StopAttempt {
            operation: op,
            result: receiver,
            _fence: Arc::new(Mutex::new(None)),
        };
        assert!(positive_session_receipt(&session, &attempt).is_ok());
        for mutate in 0..7 {
            let mut changed = good.clone();
            match mutate {
                0 => changed.owner.project_id = ProjectId::generate(),
                1 => changed.generation += 1,
                2 => changed.operation_id.push('x'),
                3 => changed.runtime_identity = StackRuntimeIdentity::new("vm-other").unwrap(),
                4 => changed.endpoint.as_mut().unwrap().active_connections = 1,
                5 => changed.endpoint.as_mut().unwrap().socket_removed = false,
                6 => {
                    changed.outcome = StackRuntimeShutdownOutcome::ReplacementPresent {
                        current: StackRuntimeIdentity::new("vm-replacement").unwrap(),
                    }
                }
                _ => unreachable!(),
            }
            sender.send_replace(Some(Arc::new(Ok(changed))));
            assert!(
                positive_session_receipt(&session, &attempt).is_err(),
                "mutation {mutate}"
            );
        }
        sender.send_replace(None);
        assert!(positive_session_receipt(&session, &attempt).is_err());
        sender.send_replace(Some(Arc::new(Err("uncertain".into()))));
        assert!(positive_session_receipt(&session, &attempt).is_err());
    }

    #[tokio::test]
    async fn delete_positive_receipt_cannot_release_an_unresolved_fence() {
        let owner = owner();
        let session = passive(
            owner.clone(),
            StackRuntimeIdentity::new("vm-exact").unwrap(),
        );
        let op = operation(&owner);
        let receipt = MachineSessionStopReceipt {
            owner,
            operation_id: op.operation_id.to_string(),
            generation: op.generation,
            runtime_identity: session.identity.clone(),
            endpoint: None,
            outcome: StackRuntimeShutdownOutcome::AlreadyAbsent,
        };
        let (_, receiver) = watch::channel(Some(Arc::new(Ok(receipt))));
        let guard = Arc::new(Arc::new(tokio::sync::Mutex::new(())).lock_owned().await);
        let attempt = StopAttempt {
            operation: op,
            result: receiver,
            _fence: Arc::new(Mutex::new(Some(guard))),
        };
        assert!(positive_session_receipt(&session, &attempt).is_err());
        attempt._fence.lock().unwrap().take();
        assert!(positive_session_receipt(&session, &attempt).is_ok());
    }

    #[tokio::test]
    async fn unknown_session_is_uncertain_without_state_mutation_or_runtime_construction() {
        struct NoStateAccess;
        impl EnvironmentStateStore for NoStateAccess {
            fn access<T>(
                &self,
                _: impl FnOnce(&vz_stack::StateStore) -> Result<T, vz_stack::StackError>,
            ) -> Result<T, vz_stack::StackError> {
                panic!("an unknown live session must fail before state/backend access")
            }
        }
        let owner = owner();
        let controller = EnvironmentRuntimeController::default();
        let lease = controller
            .acquire(&owner.project_id, &owner.environment_id)
            .await
            .unwrap();
        let sessions = MachineLiveSessions::default();
        let error = sessions
            .stop(
                &lease,
                &NoStateAccess,
                &operation(&owner),
                owner.machine_id.as_ref().unwrap(),
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("restart recovery is uncertain"));
    }

    #[test]
    fn replay_ignores_progress_but_not_changed_authority() {
        let owner = owner();
        let first = operation(&owner);
        let machine = owner.machine_id.as_ref().unwrap();
        let mut progressed = first.clone();
        progressed.machine_steps[0].status = LifecycleStepStatus::Succeeded;
        progressed.updated_at = 2;
        assert!(same_stop_request(&first, &progressed, machine));
        progressed.generation += 1;
        assert!(!same_stop_request(&first, &progressed, machine));
        progressed = first.clone();
        progressed.request_hash.push_str("-different");
        assert!(!same_stop_request(&first, &progressed, machine));
    }

    #[test]
    fn replacement_requires_positive_old_stop_receipt_and_same_owner() {
        let owner = owner();
        let identity = StackRuntimeIdentity::new("vm-first").unwrap();
        let next = StackRuntimeIdentity::new("vm-first").unwrap();
        let record = passive(owner.clone(), identity.clone());
        let (sender, receiver) = watch::channel(Some(Arc::new(Err("uncertain".into()))));
        *record.attempt.lock().unwrap() = Some(StopAttempt {
            operation: operation(&owner),
            result: receiver,
            _fence: Arc::new(Mutex::new(None)),
        });
        let mut sessions = Sessions::default();
        sessions
            .machines
            .insert(owner.machine_id.clone().unwrap(), record);
        assert!(require_available_session(&sessions, &owner, &next).is_err());
        sender.send_replace(Some(Arc::new(Ok(receipt(owner.clone())))));
        assert!(require_available_session(&sessions, &owner, &next).is_ok());
        assert!(require_available_session(&sessions, &owner, &identity).is_err());
        assert!(
            require_available_session(
                &sessions,
                &ResourceOwner {
                    environment_id: EnvironmentId::generate(),
                    ..owner
                },
                &next
            )
            .is_err()
        );
    }

    #[test]
    fn journal_repair_requires_exact_successful_physical_receipt() {
        let owner = owner();
        let operation = operation(&owner);
        let machine = owner.machine_id.as_ref().unwrap();
        let (sender, receiver) = watch::channel(None);
        let attempt = StopAttempt {
            operation: operation.clone(),
            result: receiver,
            _fence: Arc::new(Mutex::new(None)),
        };
        assert!(!successful_replay(&attempt, Some(&operation), machine));
        sender.send_replace(Some(Arc::new(Err("uncertain".into()))));
        assert!(!successful_replay(&attempt, Some(&operation), machine));
        sender.send_replace(Some(Arc::new(Ok(receipt(owner.clone())))));
        assert!(successful_replay(&attempt, Some(&operation), machine));
        assert!(!successful_replay(&attempt, None, machine));
        let mut changed = operation.clone();
        changed.generation += 1;
        assert!(!successful_replay(&attempt, Some(&changed), machine));
        let mut progressed = operation.clone();
        progressed.updated_at += 1;
        progressed.machine_steps[0].status = LifecycleStepStatus::Succeeded;
        assert!(successful_replay(&attempt, Some(&progressed), machine));
    }

    // A passive record exercises the same admission decision as production,
    // without manufacturing an activation or claiming a physical VM test.
    fn passive(owner: ResourceOwner, identity: StackRuntimeIdentity) -> Arc<Session> {
        Arc::new(Session {
            owner,
            identity,
            configuration_digest: "sha256:passive-test".into(),
            original_entry: Weak::new(),
            resources: Mutex::new(None),
            retained_entry: Mutex::new(None),
            attempt: Mutex::new(None),
            executions: Arc::new(MachineExecutionActivities::default()),
        })
    }

    #[test]
    fn duplicate_machine_or_runtime_identity_cannot_be_adopted() {
        let first = owner();
        let identity = StackRuntimeIdentity::new("vm-first").unwrap();
        let mut sessions = Sessions::default();
        sessions.machines.insert(
            first.machine_id.clone().unwrap(),
            passive(first.clone(), identity.clone()),
        );
        assert!(require_available_session(&sessions, &first, &identity).is_err());
        assert!(
            require_available_session(
                &sessions,
                &first,
                &StackRuntimeIdentity::new("vm-first").unwrap()
            )
            .is_err()
        );
        let foreign = ResourceOwner {
            environment_id: EnvironmentId::generate(),
            ..first.clone()
        };
        assert!(
            require_available_session(
                &sessions,
                &foreign,
                &StackRuntimeIdentity::new("vm-foreign").unwrap()
            )
            .is_err()
        );
        let sibling = ResourceOwner {
            machine_id: Some(MachineId::generate()),
            ..first
        };
        assert!(require_available_session(&sessions, &sibling, &identity).is_err());
        assert!(
            require_available_session(
                &sessions,
                &sibling,
                &StackRuntimeIdentity::new("vm-sibling").unwrap()
            )
            .is_ok()
        );
    }

    #[test]
    fn controller_provenance_is_pointer_identity_not_an_equal_scope() {
        let first = Arc::new(());
        let other = Arc::new(());
        let mut bound = None;
        require_controller(&mut bound, &first).unwrap();
        require_controller(&mut bound, &Arc::clone(&first)).unwrap();
        assert!(require_controller(&mut bound, &other).is_err());
    }

    #[tokio::test]
    async fn owner_scope_rejects_foreign_environment_and_project() {
        let controller = EnvironmentRuntimeController::default();
        let first = owner();
        let lease = controller
            .acquire(&first.project_id, &first.environment_id)
            .await
            .unwrap();
        lease.require_owner(&first).unwrap();
        assert!(
            lease
                .require_owner(&ResourceOwner {
                    project_id: ProjectId::generate(),
                    ..first.clone()
                })
                .is_err()
        );
        assert!(
            lease
                .require_owner(&ResourceOwner {
                    environment_id: EnvironmentId::generate(),
                    ..first
                })
                .is_err()
        );
    }

    #[tokio::test]
    async fn cancelled_observer_does_not_cancel_effect_or_lose_replay() {
        let controller = EnvironmentRuntimeController::default();
        let owner = owner();
        let lease = controller
            .acquire(&owner.project_id, &owner.environment_id)
            .await
            .unwrap();
        let fence = Arc::new(Mutex::new(Some(lease.retained_guard())));
        let (sender, receiver) = watch::channel(None);
        let (release, wait) = oneshot::channel();
        let calls = Arc::new(AtomicUsize::new(0));
        let effect_calls = Arc::clone(&calls);
        let result = receipt(owner.clone());
        let task = tokio::spawn(run_owned_stop(
            sender,
            Arc::clone(&fence),
            Duration::from_secs(5),
            async move {
                effect_calls.fetch_add(1, Ordering::SeqCst);
                wait.await.unwrap();
                Ok(result)
            },
        ));
        let observer = tokio::spawn(observe_stop(receiver.clone()));
        observer.abort();
        drop(lease);
        assert!(fence.lock().unwrap().is_some());
        release.send(()).unwrap();
        let (first, replay) = tokio::join!(observe_stop(receiver.clone()), observe_stop(receiver));
        assert_eq!(
            first.unwrap().runtime_identity,
            replay.unwrap().runtime_identity
        );
        task.await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(fence.lock().unwrap().is_none());
        let _reopened = tokio::time::timeout(
            Duration::from_secs(1),
            controller.acquire(&owner.project_id, &owner.environment_id),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn timeout_is_bounded_uncertainty_and_retains_environment_fence() {
        let controller = EnvironmentRuntimeController::default();
        let owner = owner();
        let lease = controller
            .acquire(&owner.project_id, &owner.environment_id)
            .await
            .unwrap();
        let fence = Arc::new(Mutex::new(Some(lease.retained_guard())));
        let (sender, receiver) = watch::channel(None);
        drop(lease);
        let task = tokio::spawn(run_owned_stop(
            sender,
            Arc::clone(&fence),
            Duration::from_millis(1),
            std::future::pending(),
        ));
        let failure = observe_stop(receiver.clone())
            .await
            .unwrap_err()
            .to_string();
        assert!(failure.contains("uncertain"));
        assert_eq!(
            failure,
            observe_stop(receiver).await.unwrap_err().to_string()
        );
        task.await.unwrap();
        assert!(fence.lock().unwrap().is_some());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(1),
                controller.acquire(&owner.project_id, &owner.environment_id)
            )
            .await
            .is_err()
        );
        // A sibling Environment is not blocked by this uncertainty.
        let _sibling = tokio::time::timeout(
            Duration::from_secs(1),
            controller.acquire(&owner.project_id, &EnvironmentId::generate()),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn backend_failure_retains_fence_and_is_not_absence_success() {
        let controller = EnvironmentRuntimeController::default();
        let owner = owner();
        let lease = controller
            .acquire(&owner.project_id, &owner.environment_id)
            .await
            .unwrap();
        let fence = Arc::new(Mutex::new(Some(lease.retained_guard())));
        let (sender, receiver) = watch::channel(None);
        run_owned_stop(sender, Arc::clone(&fence), Duration::from_secs(1), async {
            Err("replacement preserved".into())
        })
        .await;
        assert!(
            observe_stop(receiver)
                .await
                .unwrap_err()
                .to_string()
                .contains("replacement preserved")
        );
        assert!(fence.lock().unwrap().is_some());
    }
}
