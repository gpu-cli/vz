//! Daemon-owned live Linux Machine sessions, not a persisted-state recovery guess.
//!
//! Registration retains the exact activation and its original Runtime. Stop
//! drains an optional exact-activation endpoint before releasing the VM reader.
//! Missing registrations after restart are uncertain, never `AlreadyAbsent`.
//! This trusted controller API does not authorize RPCs or acknowledge journals.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
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
use crate::machine_runtime_registry::{MachineRuntimeEntry, MachineRuntimeRegistry};

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
    resources: Mutex<Option<LiveResources>>,
    // Keep the original Runtime/store lock even after a failed or cancelled
    // backend future. Only a positive teardown receipt releases this anchor.
    retained_entry: Mutex<Option<Arc<MachineRuntimeEntry<MacosRuntimeBackend>>>>,
    attempt: Mutex<Option<StopAttempt>>,
    executions: Arc<MachineExecutionActivities>,
}

pub(crate) struct MachineExecutionAdmission {
    pub activation: Arc<MachineRuntimeActivation>,
    pub activity: Arc<MachineExecutionActivity>,
}

#[derive(Default)]
struct Sessions {
    controller: Option<Arc<()>>,
    machines: HashMap<MachineId, Arc<Session>>,
}

/// One registry belongs to one daemon/controller. A session must be registered
/// before publishing its activation or exposing its endpoint to host clients.
#[derive(Default)]
pub struct MachineLiveSessions {
    sessions: Mutex<Sessions>,
}

impl MachineLiveSessions {
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
        if operation.kind != EnvironmentLifecycleKind::Stop {
            return Err(error("live-session Stop requires a durable Stop operation"));
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
            if let Some(existing) = attempt.as_ref() {
                if !same_stop_request(&existing.operation, operation, machine_id) {
                    return Err(error("another Stop request owns the live session"));
                }
                existing.result.clone()
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
