//! Daemon-owned, streamed Stop of a selected Linux Environment.
//!
//! Admission is read-only until every sibling has an authoritative live owner.
//! Client cancellation ends observation only. The retained task owns the
//! Environment fence, physical teardown and durable acknowledgements.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use vz_runtime_contract::{
    Architecture, EnvironmentInstance, EnvironmentLifecycleKind, EnvironmentLifecycleOperation,
    EnvironmentLifecycleStatus, EnvironmentSelectionContext, LifecycleStepResult,
    LifecycleStepStatus, MachineError, MachineErrorCode, MachineLifecycleStepAcknowledgement,
    OperatingSystem, OwnedResourceKind, PolicyDecision, ProjectId, RequestMetadata,
    TopologyAuthorization, TopologyOperation,
};
use vz_stack::StackError;

use crate::RuntimeDaemon;
use crate::environment_runtime_controller::EnvironmentControllerLease;

const MAX_MACHINES: usize = 128;

#[derive(Debug, Clone)]
pub struct StopEnvironmentInput {
    pub project_id: ProjectId,
    pub selection: EnvironmentSelectionContext,
    pub metadata: RequestMetadata,
    pub machine_timeout: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct StopEnvironmentProgress {
    pub schema_version: u32,
    pub request_id: String,
    pub sequence: u64,
    pub operation: EnvironmentLifecycleOperation,
    pub terminal: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<MachineError>,
}

fn failure(
    input: &StopEnvironmentInput,
    code: MachineErrorCode,
    message: impl Into<String>,
) -> MachineError {
    MachineError::new(
        code,
        message.into(),
        input.metadata.request_id.clone(),
        BTreeMap::from([
            ("project_id".into(), input.project_id.to_string()),
            ("operation".into(), "stop_environment".into()),
        ]),
    )
}

fn state_error(input: &StopEnvironmentInput, error: StackError) -> MachineError {
    error.to_machine_error(&input.metadata)
}

impl RuntimeDaemon {
    /// Real topology Stop; never invokes the legacy sandbox/VM manager.
    /// Current physical adapter supports Linux-on-Apple-silicon only. Other
    /// targets and unhandled live topology resources fail before journal writes.
    pub async fn stop_environment(
        self: &Arc<Self>,
        input: StopEnvironmentInput,
    ) -> Result<mpsc::Receiver<Result<StopEnvironmentProgress, MachineError>>, MachineError> {
        validate_input(&input)?;
        let initial = self.selected_stop_environment(&input)?;
        self.authorize_environment_stop(&input, &initial)?;
        // Receipts are read-only. Failed physical effects intentionally retain
        // their fence, so observing an exact terminal receipt must not need it.
        let lease_project_id = input.project_id.clone();
        let acquire =
            self.acquire_environment_controller(&lease_project_id, &initial.environment_id);
        tokio::pin!(acquire);
        let deadline = tokio::time::sleep(Duration::from_secs(30));
        tokio::pin!(deadline);
        let lease = loop {
            if let Some(operation) = self.existing_stop_request(&input, &initial)?
                && terminal(&operation)
            {
                let selected = self.selected_stop_environment(&input)?;
                if selected.environment_id != operation.environment_id {
                    return Err(failure(
                        &input,
                        MachineErrorCode::StateConflict,
                        "Environment selection changed while awaiting the Stop receipt",
                    ));
                }
                self.authorize_stop_scope(
                    &input,
                    TopologyAuthorization {
                        operation: TopologyOperation::Stop,
                        project_id: operation.project_id.clone(),
                        environment_id: operation.environment_id.clone(),
                        machine_ids: operation
                            .machine_steps
                            .iter()
                            .map(|step| step.machine_id.clone())
                            .collect(),
                        definition_digest: operation.definition_digest.clone(),
                    },
                )?;
                let (sender, receiver) = mpsc::channel(1);
                publish(
                    &sender,
                    &input,
                    &operation,
                    0,
                    true,
                    terminal_error(&input, &operation),
                );
                return Ok(receiver);
            }
            tokio::select! {
                result = &mut acquire => break result.map_err(|error| failure(&input, MachineErrorCode::StateConflict, error.to_string()))?,
                () = &mut deadline => return Err(failure(&input, MachineErrorCode::Timeout,
                    "Environment Stop admission exceeded 30 seconds; no new operation admitted")),
                () = tokio::time::sleep(Duration::from_millis(100)) => {},
            }
        };
        let environment = self.selected_stop_environment(&input)?;
        if environment.environment_id != initial.environment_id
            || environment.definition_digest != initial.definition_digest
            || environment
                .machines
                .iter()
                .map(|machine| &machine.machine_id)
                .collect::<Vec<_>>()
                != initial
                    .machines
                    .iter()
                    .map(|machine| &machine.machine_id)
                    .collect::<Vec<_>>()
        {
            return Err(failure(
                &input,
                MachineErrorCode::StateConflict,
                "selected Environment changed during Stop admission; retry with unchanged request identity",
            ));
        }
        let request_id = input.metadata.request_id.as_deref().unwrap_or_default();
        let idempotency_key = input
            .metadata
            .idempotency_key
            .as_deref()
            .unwrap_or_default();
        let request_hash = request_hash(&input, &environment)?;
        // Policy may be revoked while another controller holds the fence.
        self.authorize_environment_stop(&input, &environment)?;
        let existing = self.existing_stop_request(&input, &environment)?;
        if existing
            .as_ref()
            .is_none_or(|operation| !terminal(operation))
        {
            validate_supported_topology(&input, &environment)?;
            let non_dispatched = self
                .with_state_store(|store| {
                    let mut ids = std::collections::BTreeSet::new();
                    for machine in &environment.machines {
                        if store
                            .require_machine_boot_non_dispatch(&environment, &machine.machine_id)?
                            .is_some()
                        {
                            ids.insert(machine.machine_id.clone());
                        }
                    }
                    Ok(ids)
                })
                .map_err(|error| state_error(&input, error))?;
            self.machine_live_sessions()
                .preflight_stop_with_non_dispatch(
                    &lease,
                    &environment,
                    existing.as_ref(),
                    &non_dispatched,
                )
                .map_err(|error| {
                    failure(&input, MachineErrorCode::StateConflict, error.to_string())
                })?;
            for machine in &environment.machines {
                if machine.state == vz_runtime_contract::MachineState::Stopped {
                    continue;
                }
                let owner = vz_runtime_contract::ResourceOwner {
                    project_id: environment.project_id.clone(),
                    environment_id: environment.environment_id.clone(),
                    machine_id: Some(machine.machine_id.clone()),
                };
                let records = [
                    crate::machine_runtime_registry::MachineRuntimeRegistry::<
                        crate::machine_backend::MachineBackendRuntime,
                    >::reservation(&owner)
                    .map_err(|error| {
                        failure(&input, MachineErrorCode::StateConflict, error.to_string())
                    })?,
                    crate::machine_runtime_registry::MachineRuntimeEntry::<
                        crate::machine_backend::MachineBackendRuntime,
                    >::vm_reservation(&owner)
                    .map_err(|error| {
                        failure(&input, MachineErrorCode::StateConflict, error.to_string())
                    })?,
                ];
                for record in records {
                    self.with_state_store(|store| store.require_owned_resource(&record))
                        .map_err(|error| state_error(&input, error))?;
                }
            }
        }
        // begin performs exact immutable replay validation atomically; an
        // existing global key cannot be rebound to another Environment/request.
        let operation = self
            .with_state_store(|store| {
                store.begin_environment_lifecycle(
                    environment.environment_id.as_str(),
                    EnvironmentLifecycleKind::Stop,
                    request_id,
                    idempotency_key,
                    &request_hash,
                    crate::current_unix_secs(),
                )
            })
            .map_err(|error| state_error(&input, error))?;
        let (sender, receiver) = mpsc::channel(MAX_MACHINES + 2);
        let daemon = Arc::clone(self);
        // No await separates the durable begin from installing the owned task.
        tokio::spawn(async move {
            daemon
                .drive_environment_stop(input, lease, operation, sender)
                .await;
        });
        Ok(receiver)
    }

    fn existing_stop_request(
        &self,
        input: &StopEnvironmentInput,
        environment: &EnvironmentInstance,
    ) -> Result<Option<EnvironmentLifecycleOperation>, MachineError> {
        let existing = self
            .with_state_store(|store| {
                store.load_environment_lifecycle_by_idempotency_key(
                    input
                        .metadata
                        .idempotency_key
                        .as_deref()
                        .unwrap_or_default(),
                )
            })
            .map_err(|error| state_error(input, error))?;
        if let Some(operation) = &existing
            && (operation.kind != EnvironmentLifecycleKind::Stop
                || operation.project_id != input.project_id
                || operation.environment_id != environment.environment_id
                || operation.request_id != input.metadata.request_id.as_deref().unwrap_or_default()
                || operation.request_hash != request_hash(input, environment)?)
        {
            return Err(failure(
                input,
                MachineErrorCode::StateConflict,
                "idempotency key belongs to a different immutable Stop request",
            ));
        }
        Ok(existing)
    }

    fn selected_stop_environment(
        &self,
        input: &StopEnvironmentInput,
    ) -> Result<EnvironmentInstance, MachineError> {
        let project = self
            .with_state_store(|store| store.load_project_state_snapshot(input.project_id.as_str()))
            .map_err(|error| state_error(input, error))?
            .ok_or_else(|| failure(input, MachineErrorCode::NotFound, "Project not found"))?;
        let selection = project
            .resolve_environment(&input.selection)
            .map_err(|error| state_error(input, error.into()))?;
        project
            .environments
            .into_iter()
            .find(|environment| environment.environment_id == selection.environment_id)
            .ok_or_else(|| {
                failure(
                    input,
                    MachineErrorCode::StateConflict,
                    "selected Environment disappeared",
                )
            })
    }

    fn authorize_environment_stop(
        &self,
        input: &StopEnvironmentInput,
        environment: &EnvironmentInstance,
    ) -> Result<(), MachineError> {
        let mut machine_ids = environment
            .machines
            .iter()
            .map(|machine| machine.machine_id.clone())
            .collect::<Vec<_>>();
        machine_ids.sort();
        let scope = TopologyAuthorization {
            operation: TopologyOperation::Stop,
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_ids,
            definition_digest: environment.definition_digest.clone(),
        };
        self.authorize_stop_scope(input, scope)
    }

    fn authorize_stop_scope(
        &self,
        input: &StopEnvironmentInput,
        scope: TopologyAuthorization,
    ) -> Result<(), MachineError> {
        match self.policy_hook.evaluate_topology(&scope, &input.metadata) {
            Ok(PolicyDecision::Allow) => Ok(()),
            Ok(PolicyDecision::Deny { reason }) => {
                Err(failure(input, MachineErrorCode::PolicyDenied, reason))
            }
            Err(error) => Err(failure(
                input,
                MachineErrorCode::BackendUnavailable,
                format!("topology policy evaluation failed: {error}"),
            )),
        }
    }

    async fn drive_environment_stop(
        &self,
        input: StopEnvironmentInput,
        lease: EnvironmentControllerLease,
        mut operation: EnvironmentLifecycleOperation,
        sender: mpsc::Sender<Result<StopEnvironmentProgress, MachineError>>,
    ) {
        let mut sequence = 0;
        if terminal(&operation) {
            publish(
                &sender,
                &input,
                &operation,
                sequence,
                true,
                terminal_error(&input, &operation),
            );
            return;
        }
        publish(&sender, &input, &operation, sequence, false, None);
        for step in operation.machine_steps.clone() {
            if step.status == LifecycleStepStatus::Succeeded
                || step.status == LifecycleStepStatus::Failed
            {
                continue;
            }
            let non_dispatched = self.with_state_store(|store| {
                let project = store
                    .load_project_state_snapshot(operation.project_id.as_str())?
                    .ok_or_else(|| StackError::Machine {
                        code: MachineErrorCode::StateConflict,
                        message: "Stop Project disappeared".into(),
                    })?;
                let environment = project
                    .environments
                    .iter()
                    .find(|environment| environment.environment_id == operation.environment_id)
                    .ok_or_else(|| StackError::Machine {
                        code: MachineErrorCode::StateConflict,
                        message: "Stop Environment disappeared".into(),
                    })?;
                store
                    .require_machine_boot_non_dispatch(environment, &step.machine_id)
                    .map(|proof| proof.is_some())
            });
            let result = match non_dispatched {
                Ok(true) => Ok(()),
                Err(error) => Err(error.to_string()),
                Ok(false) => self
                    .machine_live_sessions()
                    .stop(
                        &lease,
                        &self.state_store,
                        &operation,
                        &step.machine_id,
                        input.machine_timeout,
                    )
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string()),
            };
            let acknowledgement = MachineLifecycleStepAcknowledgement {
                operation_id: operation.operation_id.clone(),
                generation: operation.generation,
                machine_id: step.machine_id.clone(),
                initial_state: step.initial_state,
                target_state: step.target_state,
                expected_incarnation: step.expected_incarnation,
                resulting_incarnation: None,
                resulting_activation: None,
                result: match result {
                    Ok(_) => LifecycleStepResult::Succeeded,
                    Err(error) => LifecycleStepResult::Failed {
                        reason: error.to_string(),
                    },
                },
            };
            match self.with_state_store(|store| {
                store.acknowledge_environment_machine_step(
                    &acknowledgement,
                    crate::current_unix_secs(),
                )
            }) {
                Ok(updated) => operation = updated,
                Err(error) => {
                    tracing::error!(operation_id = %operation.operation_id, machine_id = %step.machine_id, %error,
                        "Stop physical receipt could not be acknowledged; exact journal repair is required");
                    let _ = sender.try_send(Err(state_error(&input, error)));
                    return;
                }
            }
            sequence += 1;
            publish(&sender, &input, &operation, sequence, false, None);
        }
        match self.with_state_store(|store| {
            store.finish_environment_lifecycle(
                operation.operation_id.as_str(),
                operation.generation,
                crate::current_unix_secs(),
            )
        }) {
            Ok(finished) => {
                sequence += 1;
                publish(
                    &sender,
                    &input,
                    &finished,
                    sequence,
                    true,
                    terminal_error(&input, &finished),
                );
            }
            Err(error) => {
                tracing::error!(operation_id = %operation.operation_id, %error,
                    "Stop journal could not be finalized; exact replay is required");
                let _ = sender.try_send(Err(state_error(&input, error)));
            }
        }
    }
}

fn terminal(operation: &EnvironmentLifecycleOperation) -> bool {
    matches!(
        operation.status,
        EnvironmentLifecycleStatus::Succeeded | EnvironmentLifecycleStatus::Failed
    )
}

fn terminal_error(
    input: &StopEnvironmentInput,
    operation: &EnvironmentLifecycleOperation,
) -> Option<MachineError> {
    (operation.status == EnvironmentLifecycleStatus::Failed).then(|| {
        failure(
            input,
            MachineErrorCode::BackendUnavailable,
            operation
                .machine_steps
                .iter()
                .filter_map(|step| {
                    step.failure_reason
                        .as_ref()
                        .map(|reason| format!("{}: {reason}", step.machine_id))
                })
                .collect::<Vec<_>>()
                .join("; "),
        )
    })
}

fn publish(
    sender: &mpsc::Sender<Result<StopEnvironmentProgress, MachineError>>,
    input: &StopEnvironmentInput,
    operation: &EnvironmentLifecycleOperation,
    sequence: u64,
    terminal: bool,
    error: Option<MachineError>,
) {
    // At most initial + 128 Machine updates + terminal are produced. The queue
    // holds the whole bounded stream, so a slow/disconnected observer cannot
    // stall effects or make us discard a terminal event behind backpressure.
    let _ = sender.try_send(Ok(StopEnvironmentProgress {
        schema_version: 1,
        request_id: input.metadata.request_id.clone().unwrap_or_default(),
        sequence,
        operation: operation.clone(),
        terminal,
        error,
    }));
}

fn validate_input(input: &StopEnvironmentInput) -> Result<(), MachineError> {
    for value in [&input.metadata.request_id, &input.metadata.idempotency_key] {
        if !value.as_ref().is_some_and(|value| {
            !value.trim().is_empty()
                && value.trim() == value
                && value.len() <= 256
                && !value.chars().any(char::is_control)
        }) {
            return Err(failure(
                input,
                MachineErrorCode::ValidationError,
                "Stop requires nonempty request_id and idempotency_key of at most 256 bytes without control characters or surrounding whitespace",
            ));
        }
    }
    if input.machine_timeout.is_zero() || input.machine_timeout > Duration::from_secs(300) {
        return Err(failure(
            input,
            MachineErrorCode::ValidationError,
            "machine_timeout_millis must be in 1..300000",
        ));
    }
    Ok(())
}

fn validate_supported_topology(
    input: &StopEnvironmentInput,
    environment: &EnvironmentInstance,
) -> Result<(), MachineError> {
    let supported = environment.machines.len() <= MAX_MACHINES
        && environment.legacy_migration.is_none()
        && environment.networks.is_empty()
        && environment.endpoints.is_empty()
        && environment.machines.iter().all(|machine| {
            matches!(
                machine.target.os,
                OperatingSystem::Linux | OperatingSystem::Macos
            ) && machine.target.arch == Architecture::Aarch64
        })
        && environment
            .ownership
            .iter()
            .all(|record| match &record.resource_kind {
                OwnedResourceKind::Machine
                | OwnedResourceKind::Incarnation
                | OwnedResourceKind::Disk
                | OwnedResourceKind::DockerContext => true,
                OwnedResourceKind::Other(kind) => {
                    kind == "machine_runtime_store" || kind == "runtime_vm"
                }
                _ => false,
            });
    if !supported {
        return Err(failure(
            input,
            MachineErrorCode::UnsupportedOperation,
            "Stop supports up to 128 owned Linux/native macOS ARM64 Machines and registered Linux Docker endpoints; additional topology resources remain unsupported",
        ));
    }
    Ok(())
}

fn request_hash(
    input: &StopEnvironmentInput,
    environment: &EnvironmentInstance,
) -> Result<String, MachineError> {
    let bytes = serde_json::to_vec(&(
        &input.project_id,
        &environment.environment_id,
        &input.selection,
        input.machine_timeout.as_millis(),
    ))
    .map_err(|error| failure(input, MachineErrorCode::ValidationError, error.to_string()))?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
#[path = "environment_stop_tests.rs"]
mod tests;
