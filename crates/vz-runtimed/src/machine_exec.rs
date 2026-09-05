//! Retained, exact-session ordinary Machine execution. No legacy Run fallback.
//!
//! This foundation admits already Ready, authoritatively owned Linux Machines.
//! Automatic dependency reconciliation and native Machine adapters remain open.

use crate::{
    RuntimeDaemon, machine_execution_activity::MachineExecutionActivity,
    machine_runtime_activation::MachineRuntimeActivation,
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use vz_runtime_contract::*;

mod supervisor;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct MachineExecInput {
    pub project_id: ProjectId,
    pub selection: EnvironmentSelectionContext,
    pub machine: Option<String>,
    pub process_machine_id: Option<MachineId>,
    pub metadata: RequestMetadata,
    pub spec: MachineExecutionSpec,
}

#[derive(Debug, Clone)]
pub enum MachineExecControl {
    Stdin(Vec<u8>),
    StdinEof,
    Signal(i32),
    Resize(MachineExecutionTerminal),
    Cancel,
}

#[derive(Debug, Clone)]
pub struct MachineExecControlFrame {
    pub request_id: String,
    pub idempotency_key: String,
    pub execution_id: String,
    pub sequence: u64,
    pub control: MachineExecControl,
}

#[derive(Debug, Clone)]
pub enum MachineExecPayload {
    Ready,
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Receipt(Box<MachineExecutionReceipt>),
}

#[derive(Debug, Clone)]
pub struct MachineExecEvent {
    pub scope: MachineExecutionScope,
    pub sequence: u64,
    pub replayed: bool,
    pub payload: MachineExecPayload,
}

fn failure(
    input: &MachineExecInput,
    code: MachineErrorCode,
    message: impl Into<String>,
) -> MachineError {
    MachineError::new(
        code,
        message.into(),
        input.metadata.request_id.clone(),
        BTreeMap::from([
            ("project_id".into(), input.project_id.to_string()),
            ("operation".into(), "exec_machine".into()),
        ]),
    )
}
fn state_error(input: &MachineExecInput, error: vz_stack::StackError) -> MachineError {
    error.to_machine_error(&input.metadata)
}
fn valid_id(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        !value.is_empty()
            && value.len() <= 256
            && value.trim() == value
            && !value.chars().any(char::is_control)
    })
}

fn select_machine(
    input: &MachineExecInput,
    environment: &EnvironmentInstance,
) -> Result<MachineInstance, MachineError> {
    let candidates: Vec<_> = if let Some(selector) = &input.machine {
        if !valid_id(Some(selector)) {
            return Err(failure(
                input,
                MachineErrorCode::ValidationError,
                "invalid explicit Machine selector",
            ));
        }
        environment
            .machines
            .iter()
            .filter(|machine| machine.machine_id.as_str() == selector || machine.name == *selector)
            .collect()
    } else if let Some(id) = &input.process_machine_id {
        environment
            .machines
            .iter()
            .filter(|machine| machine.machine_id == *id)
            .collect()
    } else {
        environment.machines.iter().collect()
    };
    match candidates.as_slice() {
        [machine] => Ok((*machine).clone()),
        [] => Err(failure(
            input,
            MachineErrorCode::NotFound,
            "no Machine matches the selected Environment and Machine selectors",
        )),
        _ => Err(failure(
            input,
            MachineErrorCode::ValidationError,
            format!(
                "Machine selection is ambiguous; specify --machine (candidates: {})",
                candidates
                    .iter()
                    .take(32)
                    .map(|machine| format!("{} ({})", machine.name, machine.machine_id))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )),
    }
}

fn request_hash(
    input: &MachineExecInput,
    environment: &EnvironmentInstance,
    machine: &MachineInstance,
) -> Result<String, MachineError> {
    input
        .spec
        .request_hash(
            &input.project_id,
            &environment.environment_id,
            &machine.machine_id,
        )
        .map_err(|error| failure(input, MachineErrorCode::ValidationError, error))
}

impl RuntimeDaemon {
    fn select_execution(
        &self,
        input: &MachineExecInput,
    ) -> Result<(EnvironmentInstance, MachineInstance), MachineError> {
        let project = self
            .with_state_store(|store| store.load_project_state_snapshot(input.project_id.as_str()))
            .map_err(|error| state_error(input, error))?
            .ok_or_else(|| failure(input, MachineErrorCode::NotFound, "Project not found"))?;
        let selection = project
            .resolve_environment(&input.selection)
            .map_err(|error| state_error(input, error.into()))?;
        let environment = project
            .environments
            .into_iter()
            .find(|environment| environment.environment_id == selection.environment_id)
            .ok_or_else(|| {
                failure(
                    input,
                    MachineErrorCode::StateConflict,
                    "selected Environment disappeared",
                )
            })?;
        let machine = select_machine(input, &environment)?;
        Ok((environment, machine))
    }

    fn authorize_execution(
        &self,
        input: &MachineExecInput,
        environment: &EnvironmentInstance,
        machine: &MachineInstance,
        digest: &str,
    ) -> Result<(), MachineError> {
        let scope = TopologyAuthorization {
            operation: TopologyOperation::Exec,
            project_id: input.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_ids: vec![machine.machine_id.clone()],
            definition_digest: digest.into(),
        };
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

    fn replay_execution(
        &self,
        input: &MachineExecInput,
        environment: &EnvironmentInstance,
        machine: &MachineInstance,
    ) -> Result<Option<MachineExecutionReceipt>, MachineError> {
        let receipt = self
            .with_state_store(|store| {
                store.load_machine_execution(
                    input
                        .metadata
                        .idempotency_key
                        .as_deref()
                        .unwrap_or_default(),
                )
            })
            .map_err(|error| state_error(input, error))?;
        if let Some(receipt) = &receipt {
            if receipt.scope.project_id != input.project_id
                || receipt.scope.environment_id != environment.environment_id
                || receipt.scope.machine_id != machine.machine_id
                || receipt.scope.request_id
                    != input.metadata.request_id.as_deref().unwrap_or_default()
                || receipt.scope.request_hash != request_hash(input, environment, machine)?
            {
                return Err(failure(
                    input,
                    MachineErrorCode::StateConflict,
                    "idempotency key belongs to a different immutable Machine execution request",
                ));
            }
            self.authorize_execution(
                input,
                environment,
                machine,
                &receipt.scope.definition_digest,
            )?;
            if !matches!(
                receipt.state,
                MachineExecutionState::Completed | MachineExecutionState::Quiesced
            ) {
                return Err(failure(
                    input,
                    MachineErrorCode::StateConflict,
                    "exact execution remains active or uncertain; duplicate effects and reconstructed runtime fallback are prohibited",
                ));
            }
        }
        Ok(receipt)
    }

    /// Admit a real supervised process. The task survives response cancellation
    /// only to cancel/reap that process or retain explicit uncertainty ownership.
    pub async fn exec_machine(
        self: &Arc<Self>,
        input: MachineExecInput,
        controls: mpsc::Receiver<Result<MachineExecControlFrame, MachineError>>,
    ) -> Result<mpsc::Receiver<Result<MachineExecEvent, MachineError>>, MachineError> {
        input
            .spec
            .validate()
            .map_err(|error| failure(&input, MachineErrorCode::ValidationError, error))?;
        if !valid_id(input.metadata.request_id.as_deref())
            || !valid_id(input.metadata.idempotency_key.as_deref())
        {
            return Err(failure(
                &input,
                MachineErrorCode::ValidationError,
                "bounded request_id and idempotency_key are required",
            ));
        }
        let (initial, initial_machine) = self.select_execution(&input)?;
        self.authorize_execution(
            &input,
            &initial,
            &initial_machine,
            &initial.definition_digest,
        )?;
        let (sender, receiver) = mpsc::channel(64);
        if let Some(receipt) = self.replay_execution(&input, &initial, &initial_machine)? {
            let _ = sender.try_send(Ok(MachineExecEvent {
                scope: receipt.scope.clone(),
                sequence: 0,
                replayed: true,
                payload: MachineExecPayload::Receipt(Box::new(receipt)),
            }));
            return Ok(receiver);
        }
        let lease = tokio::time::timeout(
            Duration::from_secs(30),
            self.acquire_environment_controller(&input.project_id, &initial.environment_id),
        )
        .await
        .map_err(|_| {
            failure(
                &input,
                MachineErrorCode::Timeout,
                "Machine execution admission exceeded 30 seconds; no process admitted",
            )
        })?
        .map_err(|error| failure(&input, MachineErrorCode::StateConflict, error.to_string()))?;
        let (environment, machine) = self.select_execution(&input)?;
        if environment.environment_id != initial.environment_id
            || machine.machine_id != initial_machine.machine_id
            || environment.definition_digest != initial.definition_digest
        {
            return Err(failure(
                &input,
                MachineErrorCode::StateConflict,
                "Machine selection changed while awaiting admission",
            ));
        }
        self.authorize_execution(
            &input,
            &environment,
            &machine,
            &environment.definition_digest,
        )?;
        if let Some(receipt) = self.replay_execution(&input, &environment, &machine)? {
            let _ = sender.try_send(Ok(MachineExecEvent {
                scope: receipt.scope.clone(),
                sequence: 0,
                replayed: true,
                payload: MachineExecPayload::Receipt(Box::new(receipt)),
            }));
            return Ok(receiver);
        }
        if machine.target.os != OperatingSystem::Linux
            || machine.target.arch != Architecture::Aarch64
        {
            return Err(failure(
                &input,
                MachineErrorCode::UnsupportedOperation,
                "supervised Machine Exec currently supports owned Linux-on-Apple-silicon sessions only; no native or host fallback",
            ));
        }
        if machine.state != MachineState::Ready || environment.active_operation_id.is_some() {
            return Err(failure(
                &input,
                MachineErrorCode::StateConflict,
                "Machine is not Ready outside an active lifecycle operation; automatic Up reconciliation is not yet available",
            ));
        }
        if !machine
            .negotiated_capabilities
            .contains(MachineCapability::PosixExec)
            || (input.spec.terminal.is_some()
                && !machine
                    .negotiated_capabilities
                    .contains(MachineCapability::PosixPty))
        {
            return Err(failure(
                &input,
                MachineErrorCode::UnsupportedOperation,
                "selected Machine has not negotiated the requested execution/terminal capability",
            ));
        }
        let request_id = input.metadata.request_id.clone().unwrap_or_default();
        let idempotency_key = input.metadata.idempotency_key.clone().unwrap_or_default();
        let scope = MachineExecutionScope {
            schema_version: 1,
            execution_id: format!("mex_{:x}", Sha256::digest(idempotency_key.as_bytes())),
            request_id,
            idempotency_key,
            request_hash: request_hash(&input, &environment, &machine)?,
            project_id: input.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: machine.machine_id.clone(),
            environment_generation: environment.lifecycle_generation,
            incarnation: machine.incarnation.clone().ok_or_else(|| {
                failure(
                    &input,
                    MachineErrorCode::StateConflict,
                    "Ready Machine has no exact incarnation",
                )
            })?,
            runtime_identity: machine.runtime_identity.clone().ok_or_else(|| {
                failure(
                    &input,
                    MachineErrorCode::StateConflict,
                    "Ready Machine has no exact runtime identity",
                )
            })?,
            definition_digest: environment.definition_digest.clone(),
        };
        let admission = self
            .machine_live_sessions()
            .admit_execution(&lease, &scope)
            .map_err(|error| failure(&input, MachineErrorCode::StateConflict, error.to_string()))?;
        let now = crate::current_unix_secs();
        let receipt = MachineExecutionReceipt {
            scope,
            state: MachineExecutionState::Admitted,
            exit_code: None,
            failure: None,
            output_replay_available: false,
            created_at: now,
            updated_at: now,
        };
        if let Err(error) = self.with_state_store(|store| store.claim_machine_execution(&receipt)) {
            drop(admission.activation);
            admission.activity.complete();
            return Err(state_error(&input, error));
        }
        // No await separates durable admission and retained supervision. The
        // Environment fence can now release: its live session owns this activity.
        let daemon = Arc::clone(self);
        tokio::spawn(supervisor::drive(
            daemon,
            input,
            receipt,
            admission.activation,
            admission.activity,
            controls,
            sender,
        ));
        drop(lease);
        Ok(receiver)
    }
}
