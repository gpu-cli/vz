//! Exact-owner Environment deletion. Admission prepares every resource before
//! effects; a retained controller drives quiescence, cleanup and tombstoning.
//! Disconnect ends observation, never the admitted deletion.
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::watch;
use vz_runtime_contract::{
    Architecture, EnvironmentInstance, EnvironmentLifecycleKind, EnvironmentLifecycleOperation,
    EnvironmentLifecycleStatus, EnvironmentSelectionContext, EnvironmentTombstone,
    LifecycleStepResult, LifecycleStepStatus, MachineError, MachineErrorCode, MachineId,
    MachineLifecycleStepAcknowledgement, OperatingSystem, OwnedResourceKind,
    OwnershipCleanupStepAcknowledgement, OwnershipRecord, PolicyDecision, ProjectId,
    RequestMetadata, ResourceOwner, TopologyAuthorization, TopologyOperation,
};

use crate::RuntimeDaemon;
use crate::environment_runtime_controller::EnvironmentControllerLease;
use crate::machine_docker_config::ManagedMachineDockerConfig;
use crate::machine_docker_context::{
    ManagedMachineDockerContext, PreparedMachineDockerContextDelete,
};
use crate::machine_docker_endpoint::MachineDockerEndpoint;
use crate::machine_live_sessions::{MachineDeleteAbsentAdmission, MachineDeleteQuiescence};
use crate::machine_runtime_registry::{
    MachineRuntimeEntry, MachineRuntimeRegistry, MachineStoreDeletePreflight,
};

#[derive(Debug, Clone)]
pub struct DeleteEnvironmentInput {
    pub project_id: ProjectId,
    pub selection: EnvironmentSelectionContext,
    pub metadata: RequestMetadata,
    pub machine_timeout: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeleteEnvironmentProgress {
    pub schema_version: u32,
    pub request_id: String,
    pub sequence: u64,
    pub operation: EnvironmentLifecycleOperation,
    pub terminal: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<MachineError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tombstone: Option<EnvironmentTombstone>,
}

type Progress = Result<DeleteEnvironmentProgress, MachineError>;

struct PreparedDeleteMachine {
    id: MachineId,
    store: MachineStoreDeletePreflight,
    context: Option<PreparedMachineDockerContextDelete>,
    absent: Option<MachineDeleteAbsentAdmission>,
    quiescence: Option<MachineDeleteQuiescence>,
}

fn failure(
    input: &DeleteEnvironmentInput,
    code: MachineErrorCode,
    message: impl Into<String>,
) -> MachineError {
    MachineError::new(
        code,
        message.into(),
        input.metadata.request_id.clone(),
        BTreeMap::from([
            ("project_id".into(), input.project_id.to_string()),
            ("operation".into(), "delete_environment".into()),
        ]),
    )
}

fn conflict(input: &DeleteEnvironmentInput, error: impl std::fmt::Display) -> MachineError {
    failure(input, MachineErrorCode::StateConflict, error.to_string())
}

fn request_hash(
    input: &DeleteEnvironmentInput,
    environment: &vz_runtime_contract::EnvironmentId,
) -> Result<String, MachineError> {
    vz_runtime_contract::environment_delete_request_hash(
        &input.project_id,
        environment,
        &input.selection,
        input.machine_timeout.as_millis(),
    )
    .map_err(|error| conflict(input, error))
}

fn progress(
    input: &DeleteEnvironmentInput,
    operation: &EnvironmentLifecycleOperation,
    sequence: u64,
    tombstone: Option<EnvironmentTombstone>,
) -> Progress {
    Ok(DeleteEnvironmentProgress {
        schema_version: 1,
        request_id: input.metadata.request_id.clone().unwrap_or_default(),
        sequence,
        operation: operation.clone(),
        terminal: tombstone.is_some() || operation.status == EnvironmentLifecycleStatus::Blocked,
        error: (operation.status == EnvironmentLifecycleStatus::Blocked).then(|| {
            failure(
                input,
                MachineErrorCode::BackendUnavailable,
                "Delete is blocked; exact retained ownership reconciliation is required",
            )
        }),
        tombstone,
    })
}

impl RuntimeDaemon {
    pub async fn delete_environment(
        self: &Arc<Self>,
        input: DeleteEnvironmentInput,
    ) -> Result<watch::Receiver<Progress>, MachineError> {
        validate_input(&input)?;
        // Resolve immutable request replay BEFORE resolving a now-absent or
        // reused human name/workspace binding. No new target can inherit a key.
        if let Some(terminal) = self.completed_delete(&input)? {
            return Ok(terminal);
        }
        let original = self.selected_delete(&input)?;
        self.authorize_delete(&input, &original)?;
        let lease = tokio::time::timeout(
            Duration::from_secs(30),
            self.acquire_environment_controller(&input.project_id, &original.environment_id),
        )
        .await
        .map_err(|_| {
            failure(
                &input,
                MachineErrorCode::Timeout,
                "Delete admission exceeded 30 seconds; no new operation admitted",
            )
        })?
        .map_err(|error| conflict(&input, error))?;
        if let Some(terminal) = self.completed_delete(&input)? {
            return Ok(terminal);
        }
        let environment = self.selected_delete(&input)?;
        if environment != original {
            return Err(conflict(
                &input,
                "Delete selection changed during admission",
            ));
        }
        self.authorize_delete(&input, &environment)?;
        validate_supported(&input, &environment)?;
        let existing = self.existing_delete(&input)?;
        if existing
            .as_ref()
            .is_some_and(|op| op.environment_id != environment.environment_id)
        {
            return Err(conflict(
                &input,
                "Delete key belongs to another selected Environment",
            ));
        }
        if let Some(operation) = &existing {
            operation
                .validate_against_environment(&environment)
                .map_err(|e| conflict(&input, e))?;
        }
        let mut prepared = Vec::new();
        for machine in &environment.machines {
            let owner = ResourceOwner {
                project_id: environment.project_id.clone(),
                environment_id: environment.environment_id.clone(),
                machine_id: Some(machine.machine_id.clone()),
            };
            let reservation =
                MachineRuntimeRegistry::<crate::machine_backend::MachineBackendRuntime>::reservation(&owner)
                    .map_err(|e| conflict(&input, e))?;
            self.with_state_store(|store| store.require_owned_resource(&reservation))
                .map_err(|e| e.to_machine_error(&input.metadata))?;
            let store = self
                .machine_runtime_registry()
                .preflight_delete(&owner, &reservation)
                .map_err(|e| conflict(&input, e))?;
            if let Some(id) = store.delete_operation_id() {
                if existing
                    .as_ref()
                    .is_none_or(|operation| &operation.operation_id != id)
                {
                    return Err(conflict(
                        &input,
                        "Machine store belongs to a different Delete intent",
                    ));
                }
            }
            let context = if let Some(lease) = store
                .lease()
                .filter(|_| machine.target.os == OperatingSystem::Linux)
            {
                let socket =
                    MachineDockerEndpoint::socket_path_for(&self.config.runtime_data_dir, &owner)
                        .map_err(|e| conflict(&input, e))?;
                // Private configuration is authorized by the retained Machine
                // store, never by an ambient path or the public descriptor.
                // Only old DEV claims retain the original shared-config path
                // admission; their host credentials are never owned/deleted.
                let config_dir = match ManagedMachineDockerConfig::open_existing(Arc::clone(lease))
                    .map_err(|e| conflict(&input, e))?
                {
                    Some(config) => config.path().to_path_buf(),
                    None => docker_config_dir(&input)?,
                };
                ManagedMachineDockerContext::prepare_existing_delete(
                    Arc::clone(lease),
                    machine.docker_context.as_ref(),
                    &config_dir,
                    &socket,
                )
                .map_err(|e| conflict(&input, e))?
            } else {
                None
            };
            let absent = if store.quiescence_evidence().is_some() {
                None
            } else {
                self.machine_live_sessions()
                    .prepare_delete_absence(
                        &lease,
                        &self.state_store,
                        &environment,
                        &machine.machine_id,
                    )
                    .map_err(|e| conflict(&input, e))?
            };
            prepared.push(PreparedDeleteMachine {
                id: machine.machine_id.clone(),
                store,
                context,
                absent,
                quiescence: None,
            });
        }
        // All policy, ownership and physical path checks preceded this begin.
        // Controller serialization spans every subsequent effect and receipt.
        self.authorize_delete(&input, &environment)?;
        let hash = request_hash(&input, &environment.environment_id)?;
        let operation = self
            .with_state_store(|store| {
                store.begin_environment_lifecycle(
                    environment.environment_id.as_str(),
                    EnvironmentLifecycleKind::Delete,
                    input.metadata.request_id.as_deref().unwrap_or_default(),
                    input
                        .metadata
                        .idempotency_key
                        .as_deref()
                        .unwrap_or_default(),
                    &hash,
                    crate::current_unix_secs(),
                )
            })
            .map_err(|e| e.to_machine_error(&input.metadata))?;
        let (sender, receiver) = watch::channel(progress(&input, &operation, 0, None));
        let daemon = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(error) = daemon
                .drive_delete(&input, &lease, operation, prepared, &sender)
                .await
            {
                // The durable active Delete remains authoritative. No tombstone
                // or successful cleanup is fabricated after uncertain effects.
                let _ = sender.send_replace(Err(error));
            }
        });
        Ok(receiver)
    }

    fn existing_delete(
        &self,
        input: &DeleteEnvironmentInput,
    ) -> Result<Option<EnvironmentLifecycleOperation>, MachineError> {
        let operation = self
            .with_state_store(|store| {
                store.load_environment_lifecycle_by_idempotency_key(
                    input
                        .metadata
                        .idempotency_key
                        .as_deref()
                        .unwrap_or_default(),
                )
            })
            .map_err(|e| e.to_machine_error(&input.metadata))?;
        if let Some(operation) = &operation {
            if operation.kind != EnvironmentLifecycleKind::Delete
                || operation.project_id != input.project_id
                || operation.request_id != input.metadata.request_id.as_deref().unwrap_or_default()
                || operation.request_hash != request_hash(input, &operation.environment_id)?
            {
                return Err(conflict(
                    input,
                    "idempotency key belongs to a different immutable Delete request",
                ));
            }
        }
        Ok(operation)
    }

    fn completed_delete(
        &self,
        input: &DeleteEnvironmentInput,
    ) -> Result<Option<watch::Receiver<Progress>>, MachineError> {
        let Some(operation) = self.existing_delete(input)? else {
            return Ok(None);
        };
        if operation.status != EnvironmentLifecycleStatus::Succeeded
            && operation.status != EnvironmentLifecycleStatus::Blocked
        {
            return Ok(None);
        }
        self.authorize_delete_operation(input, &operation)?;
        if operation.status == EnvironmentLifecycleStatus::Blocked {
            let (_, receiver) = watch::channel(progress(input, &operation, 0, None));
            return Ok(Some(receiver));
        }
        let tombstone = self
            .with_state_store(|store| {
                store.load_environment_tombstone(operation.environment_id.as_str())
            })
            .map_err(|e| e.to_machine_error(&input.metadata))?
            .ok_or_else(|| conflict(input, "completed Delete omitted its durable tombstone"))?;
        tombstone
            .validate_for_operation(&operation)
            .map_err(|e| conflict(input, e))?;
        let (_, receiver) = watch::channel(progress(input, &operation, 0, Some(tombstone)));
        Ok(Some(receiver))
    }

    fn selected_delete(
        &self,
        input: &DeleteEnvironmentInput,
    ) -> Result<EnvironmentInstance, MachineError> {
        let project = self
            .with_state_store(|store| store.load_project_state_snapshot(input.project_id.as_str()))
            .map_err(|e| e.to_machine_error(&input.metadata))?
            .ok_or_else(|| failure(input, MachineErrorCode::NotFound, "Project not found"))?;
        let selection = project
            .resolve_environment(&input.selection)
            .map_err(|e| conflict(input, e))?;
        project
            .environments
            .into_iter()
            .find(|environment| environment.environment_id == selection.environment_id)
            .ok_or_else(|| conflict(input, "selected Environment disappeared"))
    }

    fn authorize_delete(
        &self,
        input: &DeleteEnvironmentInput,
        environment: &EnvironmentInstance,
    ) -> Result<(), MachineError> {
        self.authorize_delete_scope(
            input,
            environment.project_id.clone(),
            environment.environment_id.clone(),
            environment
                .machines
                .iter()
                .map(|m| m.machine_id.clone())
                .collect(),
            environment.definition_digest.clone(),
        )
    }

    fn authorize_delete_operation(
        &self,
        input: &DeleteEnvironmentInput,
        operation: &EnvironmentLifecycleOperation,
    ) -> Result<(), MachineError> {
        self.authorize_delete_scope(
            input,
            operation.project_id.clone(),
            operation.environment_id.clone(),
            operation
                .machine_steps
                .iter()
                .map(|m| m.machine_id.clone())
                .collect(),
            operation.definition_digest.clone(),
        )
    }

    fn authorize_delete_scope(
        &self,
        input: &DeleteEnvironmentInput,
        project_id: ProjectId,
        environment_id: vz_runtime_contract::EnvironmentId,
        mut machine_ids: Vec<MachineId>,
        definition_digest: String,
    ) -> Result<(), MachineError> {
        machine_ids.sort();
        let scope = TopologyAuthorization {
            operation: TopologyOperation::Delete,
            project_id,
            environment_id,
            machine_ids,
            definition_digest,
        };
        match self.policy_hook.evaluate_topology(&scope, &input.metadata) {
            Ok(PolicyDecision::Allow) => Ok(()),
            Ok(PolicyDecision::Deny { reason }) => {
                Err(failure(input, MachineErrorCode::PolicyDenied, reason))
            }
            Err(error) => Err(failure(
                input,
                MachineErrorCode::BackendUnavailable,
                error.to_string(),
            )),
        }
    }

    async fn drive_delete(
        self: &Arc<Self>,
        input: &DeleteEnvironmentInput,
        lease: &EnvironmentControllerLease,
        mut operation: EnvironmentLifecycleOperation,
        mut machines: Vec<PreparedDeleteMachine>,
        sender: &watch::Sender<Progress>,
    ) -> Result<(), MachineError> {
        let mut sequence = 0;
        for machine in &mut machines {
            self.authorize_delete_operation(input, &operation)?;
            let step = operation
                .machine_steps
                .iter()
                .find(|step| step.machine_id == machine.id)
                .ok_or_else(|| conflict(input, "Delete Machine step missing"))?
                .clone();
            if step.status != LifecycleStepStatus::Succeeded {
                if step.status != LifecycleStepStatus::Pending
                    && step.status != LifecycleStepStatus::Running
                {
                    return Err(conflict(
                        input,
                        "failed Delete Machine step needs explicit reconciliation",
                    ));
                }
                if machine.absent.is_none() {
                    self.machine_live_sessions()
                        .stop_for_delete(
                            lease,
                            &self.state_store,
                            &operation,
                            &machine.id,
                            input.machine_timeout,
                        )
                        .await
                        .map_err(|e| conflict(input, e))?;
                }
            }
            machine.quiescence = Some(
                self.machine_live_sessions()
                    .retire_for_delete(
                        lease,
                        &self.state_store,
                        &operation,
                        &machine.id,
                        &machine.store,
                        machine.absent.take(),
                    )
                    .map_err(|e| conflict(input, e))?,
            );
            if step.status != LifecycleStepStatus::Succeeded {
                operation = self
                    .with_state_store(|store| {
                        store.acknowledge_environment_machine_step(
                            &MachineLifecycleStepAcknowledgement {
                                operation_id: operation.operation_id.clone(),
                                generation: operation.generation,
                                machine_id: step.machine_id,
                                initial_state: step.initial_state,
                                target_state: step.target_state,
                                expected_incarnation: step.expected_incarnation,
                                resulting_incarnation: None,
                                resulting_activation: None,
                                result: LifecycleStepResult::Succeeded,
                            },
                            crate::current_unix_secs(),
                        )
                    })
                    .map_err(|e| e.to_machine_error(&input.metadata))?;
            }
            sequence += 1;
            let _ = sender.send_replace(progress(input, &operation, sequence, None));
        }
        // No owned storage is removed until every Machine is positively quiet.
        for machine in &mut machines {
            self.authorize_delete_operation(input, &operation)?;
            if let Some(mut context) = machine.context.take() {
                context
                    .remove_exact(&operation)
                    .map_err(|e| conflict(input, e))?;
            }
        }
        for machine in machines {
            self.authorize_delete_operation(input, &operation)?;
            let quiescence = machine
                .quiescence
                .ok_or_else(|| conflict(input, "Delete omitted positive quiescence"))?;
            let daemon = Arc::clone(self);
            let exact_operation = operation.clone();
            // The blocking walker owns its token/fence until completion. Never
            // timeout/drop it on client observation loss or strand its effects.
            let receipt = tokio::task::spawn_blocking(move || {
                daemon
                    .machine_runtime_registry()
                    .begin_delete(machine.store, &exact_operation, quiescence)?
                    .remove()
            })
            .await
            .map_err(|e| conflict(input, e))?
            .map_err(|e| conflict(input, e))?;
            if !receipt.store_removed
                || receipt.owner.machine_id.as_ref() != Some(&machine.id)
                || receipt.operation_id != operation.operation_id
                || receipt.generation != operation.generation
            {
                return Err(conflict(
                    input,
                    "Machine store deletion receipt changed authority",
                ));
            }
            for step in operation
                .cleanup_steps
                .clone()
                .into_iter()
                .filter(|step| step.ownership.machine_id.as_ref() == Some(&machine.id))
            {
                operation = self
                    .with_state_store(|store| {
                        store.acknowledge_environment_cleanup_step(
                            &OwnershipCleanupStepAcknowledgement {
                                operation_id: operation.operation_id.clone(),
                                generation: operation.generation,
                                ownership: step.ownership,
                                result: LifecycleStepResult::Succeeded,
                            },
                            crate::current_unix_secs(),
                        )
                    })
                    .map_err(|e| e.to_machine_error(&input.metadata))?;
            }
            sequence += 1;
            let _ = sender.send_replace(progress(input, &operation, sequence, None));
        }
        let (finished, tombstone) = self
            .with_state_store(|store| {
                store.finish_environment_delete(
                    operation.operation_id.as_str(),
                    operation.generation,
                    crate::current_unix_secs(),
                )
            })
            .map_err(|e| e.to_machine_error(&input.metadata))?;
        tombstone
            .validate_for_operation(&finished)
            .map_err(|e| conflict(input, e))?;
        let _ = sender.send_replace(progress(input, &finished, sequence + 1, Some(tombstone)));
        Ok(())
    }
}

fn validate_input(input: &DeleteEnvironmentInput) -> Result<(), MachineError> {
    for text in [&input.metadata.request_id, &input.metadata.idempotency_key] {
        if !text.as_ref().is_some_and(|text| {
            !text.is_empty()
                && text.len() <= 256
                && text.trim() == text
                && !text.chars().any(char::is_control)
        }) {
            return Err(failure(
                input,
                MachineErrorCode::ValidationError,
                "Delete requires stable bounded request and idempotency IDs",
            ));
        }
    }
    if input.machine_timeout.is_zero() || input.machine_timeout > Duration::from_secs(300) {
        return Err(failure(
            input,
            MachineErrorCode::ValidationError,
            "Delete Machine timeout must be in 1..300000 milliseconds",
        ));
    }
    Ok(())
}

fn docker_config_dir(input: &DeleteEnvironmentInput) -> Result<PathBuf, MachineError> {
    let path = std::env::var_os("VZ_DOCKER_CONFIG")
        .or_else(|| std::env::var_os("DOCKER_CONFIG"))
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".docker")))
        .ok_or_else(|| conflict(input, "Docker configuration path is unavailable"))?;
    if !path.is_absolute() {
        return Err(conflict(
            input,
            "Docker configuration path must be absolute",
        ));
    }
    Ok(path)
}

fn validate_supported(
    input: &DeleteEnvironmentInput,
    environment: &EnvironmentInstance,
) -> Result<(), MachineError> {
    if environment.machines.is_empty()
        || environment.machines.len() > 128
        || environment.ownership.len() > 4096
        || environment.legacy_migration.is_some()
        || !environment.networks.is_empty()
        || !environment.endpoints.is_empty()
        || environment.machines.iter().any(|m| {
            !matches!(m.target.os, OperatingSystem::Linux | OperatingSystem::Macos)
                || m.target.arch != Architecture::Aarch64
        })
    {
        return Err(failure(
            input,
            MachineErrorCode::UnsupportedOperation,
            "Delete supports registered Linux/native macOS ARM64 Machines; additional topology resource adapters remain unsupported",
        ));
    }
    let mut expected = Vec::<OwnershipRecord>::new();
    for machine in &environment.machines {
        let owner = ResourceOwner {
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(machine.machine_id.clone()),
        };
        expected.push(OwnershipRecord {
            schema_version: 1,
            resource_kind: OwnedResourceKind::Machine,
            resource_id: machine.machine_id.to_string(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(machine.machine_id.clone()),
        });
        expected.push(
            MachineRuntimeRegistry::<crate::machine_backend::MachineBackendRuntime>::reservation(
                &owner,
            )
            .map_err(|e| conflict(input, e))?,
        );
        expected.push(
            MachineRuntimeEntry::<crate::machine_backend::MachineBackendRuntime>::vm_reservation(
                &owner,
            )
            .map_err(|e| conflict(input, e))?,
        );
        if let Some(incarnation) = &machine.incarnation {
            expected.push(OwnershipRecord {
                schema_version: 1,
                resource_kind: OwnedResourceKind::Incarnation,
                resource_id: incarnation.incarnation_id.to_string(),
                environment_id: environment.environment_id.clone(),
                machine_id: Some(machine.machine_id.clone()),
            });
        }
        if let Some(context) = &machine.docker_context {
            context.validate().map_err(|e| conflict(input, e))?;
            if context.owner != owner {
                return Err(conflict(input, "Delete context is foreign"));
            }
            expected.push(OwnershipRecord {
                schema_version: 1,
                resource_kind: OwnedResourceKind::DockerContext,
                resource_id: context.name.clone(),
                environment_id: environment.environment_id.clone(),
                machine_id: Some(machine.machine_id.clone()),
            });
        }
    }
    if expected.len() != environment.ownership.len()
        || expected
            .iter()
            .any(|record| !environment.ownership.contains(record))
    {
        return Err(failure(
            input,
            MachineErrorCode::UnsupportedOperation,
            "Delete ownership graph contains missing, unknown, or unsupported physical resources; no effects admitted",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "environment_delete_tests.rs"]
mod tests;
