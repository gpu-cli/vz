//! Durable, narrowly scoped VM-boot non-dispatch authority.
//!
//! This does not certify absence of disks, pinned artifacts, runtime stores, or
//! other resources. Callers hold the Environment controller lock across recording,
//! consumption, and dispatch. A consumed record is deliberately never re-armed
//! within the same operation, even if the caller crashed before actually booting.
use super::StateStore;
use crate::StackError;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use vz_runtime_contract::{
    EnvironmentId, EnvironmentInstance, EnvironmentLifecycleKind, EnvironmentLifecycleOperation,
    EnvironmentLifecycleStatus, LifecycleOperationId, LifecycleStepStatus, MachineErrorCode,
    MachineId, MachineIncarnation, MachineLifecycleStep, MachineState, ProjectId,
};

/// Exact authority that VM boot has not been dispatched for this Up attempt.
/// It makes no assertion about non-VM resources or earlier incarnation history.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MachineBootNonDispatchProof {
    pub schema_version: u32,
    pub project_id: ProjectId,
    pub environment_id: EnvironmentId,
    pub machine_id: MachineId,
    pub operation_id: LifecycleOperationId,
    pub generation: u64,
    pub definition_digest: String,
    pub request_id: String,
    pub request_hash: String,
    pub idempotency_key: String,
    pub initial_state: MachineState,
    pub expected_incarnation: Option<MachineIncarnation>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    proof: MachineBootNonDispatchProof,
    consumed: bool,
}

fn conflict(message: &str) -> StackError {
    StackError::Machine {
        code: MachineErrorCode::StateConflict,
        message: format!("VM boot non-dispatch proof: {message}"),
    }
}

fn key(environment: &EnvironmentId, machine: &MachineId) -> Result<String, StackError> {
    Ok(format!(
        "machine-boot-non-dispatch-v1/{:x}",
        Sha256::digest(serde_json::to_vec(&(environment, machine))?)
    ))
}

fn step<'a>(
    operation: &'a EnvironmentLifecycleOperation,
    machine: &MachineId,
) -> Result<&'a MachineLifecycleStep, StackError> {
    operation
        .machine_steps
        .iter()
        .find(|step| &step.machine_id == machine)
        .ok_or_else(|| conflict("Machine absent from operation"))
}

fn proof_for(
    operation: &EnvironmentLifecycleOperation,
    step: &MachineLifecycleStep,
) -> MachineBootNonDispatchProof {
    MachineBootNonDispatchProof {
        schema_version: 1,
        project_id: operation.project_id.clone(),
        environment_id: operation.environment_id.clone(),
        machine_id: step.machine_id.clone(),
        operation_id: operation.operation_id.clone(),
        generation: operation.generation,
        definition_digest: operation.definition_digest.clone(),
        request_id: operation.request_id.clone(),
        request_hash: operation.request_hash.clone(),
        idempotency_key: operation.idempotency_key.clone(),
        initial_state: step.initial_state,
        expected_incarnation: step.expected_incarnation.clone(),
    }
}

impl StateStore {
    fn boot_record(
        &self,
        environment: &EnvironmentId,
        machine: &MachineId,
    ) -> Result<Option<Record>, StackError> {
        self.get_control_metadata(&key(environment, machine)?)?
            .map(|value| serde_json::from_str(&value).map_err(Into::into))
            .transpose()
    }

    fn write_boot_record(&self, record: &Record) -> Result<(), StackError> {
        self.conn.execute(
            "INSERT INTO control_metadata (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![
                key(&record.proof.environment_id, &record.proof.machine_id)?,
                serde_json::to_string(record)?
            ],
        )?;
        Ok(())
    }

    fn boot_operation_at_generation(
        &self,
        environment: &EnvironmentId,
        generation: u64,
    ) -> Result<Option<EnvironmentLifecycleOperation>, StackError> {
        let id: Option<String> = self.conn.query_row(
            "SELECT operation_id FROM environment_lifecycle_operations WHERE environment_id = ?1 AND generation = ?2",
            params![environment.as_str(), generation], |row| row.get(0),
        ).optional()?;
        id.map(|id| {
            self.load_environment_lifecycle(&id)?
                .ok_or_else(|| conflict("journal disappeared"))
        })
        .transpose()
    }

    fn boot_pending_up(
        &self,
        expected: &EnvironmentLifecycleOperation,
        machine: &MachineId,
    ) -> Result<EnvironmentInstance, StackError> {
        let environment = self
            .load_environment_instance(expected.environment_id.as_str())?
            .ok_or_else(|| conflict("Environment absent"))?;
        let actual = self
            .load_environment_lifecycle(expected.operation_id.as_str())?
            .ok_or_else(|| conflict("Up journal absent"))?;
        actual.validate_against_environment(&environment)?;
        // Compare immutable request identity and the exact Machine step, allowing
        // already-acknowledged siblings to advance independently.
        if actual.kind != EnvironmentLifecycleKind::Up
            || actual.status != EnvironmentLifecycleStatus::Running
            || environment.active_operation_id.as_ref() != Some(&actual.operation_id)
            || environment.lifecycle_generation != actual.generation
            || proof_for(&actual, step(&actual, machine)?)
                != proof_for(expected, step(expected, machine)?)
            || step(&actual, machine)? != step(expected, machine)?
            || !matches!(
                step(&actual, machine)?.status,
                LifecycleStepStatus::Pending | LifecycleStepStatus::Running
            )
            || expected.kind != EnvironmentLifecycleKind::Up
            || expected.status != EnvironmentLifecycleStatus::Running
        {
            return Err(conflict("not the exact current pending Up fence"));
        }
        Ok(environment)
    }

    fn validate_boot_record(
        &self,
        record: &Record,
        environment: &EnvironmentInstance,
        machine: &MachineId,
    ) -> Result<EnvironmentLifecycleOperation, StackError> {
        let proof = &record.proof;
        let original = self
            .load_environment_lifecycle(proof.operation_id.as_str())?
            .ok_or_else(|| conflict("original Up journal absent"))?;
        let machine_state = environment
            .machines
            .iter()
            .find(|item| &item.machine_id == machine)
            .ok_or_else(|| conflict("Machine absent from Environment"))?;
        if proof.schema_version != 1
            || &proof.machine_id != machine
            || proof.project_id != environment.project_id
            || proof.environment_id != environment.environment_id
            || proof.definition_digest != environment.definition_digest
            || original.kind != EnvironmentLifecycleKind::Up
            || proof_for(&original, step(&original, machine)?) != *proof
            || machine_state.incarnation != proof.expected_incarnation
            || matches!(
                original.status,
                EnvironmentLifecycleStatus::Superseded | EnvironmentLifecycleStatus::Succeeded
            )
        {
            return Err(conflict("foreign, changed, or superseded authority"));
        }
        Ok(original)
    }

    fn boot_failed_predecessor(
        &self,
        record: &Record,
        environment: &EnvironmentInstance,
        next: &EnvironmentLifecycleOperation,
        machine: &MachineId,
    ) -> Result<bool, StackError> {
        let original = self.validate_boot_record(record, environment, machine)?;
        let previous_step = step(&original, machine)?;
        let next_step = step(next, machine)?;
        Ok(!record.consumed
            && original.status == EnvironmentLifecycleStatus::Failed
            && previous_step.status == LifecycleStepStatus::Failed
            && original.generation.checked_add(1) == Some(next.generation)
            && environment.lifecycle_generation == next.generation
            && next_step.initial_state == MachineState::Failed
            && next_step.expected_incarnation == record.proof.expected_incarnation
            && next.project_id == original.project_id
            && next.environment_id == original.environment_id
            && next.definition_digest == original.definition_digest
            && next.status == EnvironmentLifecycleStatus::Running
            && matches!(
                next_step.status,
                LifecycleStepStatus::Pending | LifecycleStepStatus::Running
            )
            && environment.active_operation_id.as_ref() == Some(&next.operation_id))
    }

    /// Record positive authority before boot. Only fresh first-Up Machines, an
    /// exact preceding positively acknowledged Stop, or an unconsumed immediately
    /// preceding failed-Up proof qualify. Generic Failed/absent-runtime does not.
    pub fn record_machine_boot_non_dispatch(
        &self,
        operation: &EnvironmentLifecycleOperation,
        machine: &MachineId,
    ) -> Result<MachineBootNonDispatchProof, StackError> {
        self.with_immediate_transaction(|store| {
            let environment = store.boot_pending_up(operation, machine)?;
            let machine_step = step(operation, machine)?;
            let proof = proof_for(operation, machine_step);
            let existing = store.boot_record(&environment.environment_id, machine)?;
            if let Some(record) = &existing {
                if record.proof.operation_id == operation.operation_id {
                    store.validate_boot_record(record, &environment, machine)?;
                    if record.consumed {
                        return Err(conflict("attempt already consumed; dispatch is uncertain"));
                    }
                    if record.proof != proof {
                        return Err(conflict("same-attempt authority changed"));
                    }
                    return Ok(proof);
                }
            }
            let instance = environment
                .machines
                .iter()
                .find(|item| &item.machine_id == machine)
                .ok_or_else(|| conflict("Machine absent"))?;
            let fresh = operation.generation == 1
                && machine_step.initial_state == MachineState::Creating
                && instance.incarnation.is_none()
                && instance.runtime_identity.is_none()
                && instance.legacy_sandbox_id.is_none()
                && existing.is_none();
            let predecessor = operation
                .generation
                .checked_sub(1)
                .map(|generation| {
                    store.boot_operation_at_generation(&environment.environment_id, generation)
                })
                .transpose()?
                .flatten();
            let stopped = if let Some(previous) = &predecessor {
                let previous_step = step(previous, machine)?;
                previous.kind == EnvironmentLifecycleKind::Stop
                    && matches!(
                        previous.status,
                        EnvironmentLifecycleStatus::Succeeded | EnvironmentLifecycleStatus::Failed
                    )
                    && previous.project_id == operation.project_id
                    && previous.definition_digest == operation.definition_digest
                    && previous_step.status == LifecycleStepStatus::Succeeded
                    && previous_step.target_state == Some(MachineState::Stopped)
                    && previous_step.expected_incarnation == machine_step.expected_incarnation
                    && machine_step.initial_state == MachineState::Stopped
                    && instance.state == MachineState::Stopped
            } else {
                false
            };
            let transferred = if let Some(record) = &existing {
                // A positive later Stop is independent authority; do not try to
                // reinterpret an old consumed/superseded boot attempt as absence.
                !stopped
                    && store.boot_failed_predecessor(record, &environment, operation, machine)?
            } else {
                false
            };
            if !fresh && !stopped && !transferred {
                return Err(conflict(
                    "no positive never-dispatched or stopped authority",
                ));
            }
            store.write_boot_record(&Record {
                proof: proof.clone(),
                consumed: false,
            })?;
            Ok(proof)
        })
    }

    /// Durably remove usable proof BEFORE dispatch. Retain a consumed tombstone
    /// so retries cannot re-arm this attempt. Missing/already-consumed rejects.
    pub fn consume_machine_boot_non_dispatch(
        &self,
        operation: &EnvironmentLifecycleOperation,
        machine: &MachineId,
    ) -> Result<(), StackError> {
        self.with_immediate_transaction(|store| {
            let environment = store.boot_pending_up(operation, machine)?;
            let mut record = store
                .boot_record(&environment.environment_id, machine)?
                .ok_or_else(|| conflict("proof missing before dispatch"))?;
            store.validate_boot_record(&record, &environment, machine)?;
            if record.consumed || record.proof != proof_for(operation, step(operation, machine)?) {
                return Err(conflict(
                    "proof already consumed or belongs to another attempt",
                ));
            }
            record.consumed = true;
            store.write_boot_record(&record)
        })
    }

    /// Read exact boot non-dispatch authority in one snapshot. Accept the
    /// original current Up or its immediate current Stop/Up successor only.
    /// This is not an effects lease; callers retain controller serialization.
    pub fn require_machine_boot_non_dispatch(
        &self,
        environment: &EnvironmentInstance,
        machine: &MachineId,
    ) -> Result<Option<MachineBootNonDispatchProof>, StackError> {
        let transaction = self.conn.unchecked_transaction()?;
        let current = self
            .load_environment_instance(environment.environment_id.as_str())?
            .ok_or_else(|| conflict("Environment absent"))?;
        if &current != environment {
            return Err(conflict("Environment snapshot changed"));
        }
        let result = if let Some(record) = self.boot_record(&environment.environment_id, machine)? {
            // Consumed is absence of authority, not evidence of an absent VM.
            if record.consumed {
                None
            } else {
                let original = self.validate_boot_record(&record, environment, machine)?;
                let original_step = step(&original, machine)?;
                let same = environment.lifecycle_generation == original.generation
                    && ((original.status == EnvironmentLifecycleStatus::Failed
                        && original_step.status == LifecycleStepStatus::Failed
                        && environment.active_operation_id.is_none())
                        || (original.status == EnvironmentLifecycleStatus::Running
                            && matches!(
                                original_step.status,
                                LifecycleStepStatus::Pending | LifecycleStepStatus::Running
                            )
                            && environment.active_operation_id.as_ref()
                                == Some(&original.operation_id)));
                let successor = if let Some(id) = &environment.active_operation_id {
                    let next = self
                        .load_environment_lifecycle(id.as_str())?
                        .ok_or_else(|| conflict("active journal missing"))?;
                    matches!(
                        next.kind,
                        EnvironmentLifecycleKind::Stop | EnvironmentLifecycleKind::Up
                    ) && self.boot_failed_predecessor(&record, environment, &next, machine)?
                } else {
                    false
                };
                let stopped = self
                    .boot_operation_at_generation(
                        &environment.environment_id,
                        environment.lifecycle_generation,
                    )?
                    .is_some_and(|current| {
                        current.kind == EnvironmentLifecycleKind::Stop
                            && original.generation.checked_add(1) == Some(current.generation)
                            && current.project_id == original.project_id
                            && current.definition_digest == original.definition_digest
                            && matches!(
                                current.status,
                                EnvironmentLifecycleStatus::Running
                                    | EnvironmentLifecycleStatus::Succeeded
                                    | EnvironmentLifecycleStatus::Failed
                            )
                            && step(&current, machine).is_ok_and(|step| {
                                step.status == LifecycleStepStatus::Succeeded
                                    && step.expected_incarnation
                                        == record.proof.expected_incarnation
                                    && step.target_state == Some(MachineState::Stopped)
                            })
                            && environment.machines.iter().any(|item| {
                                &item.machine_id == machine && item.state == MachineState::Stopped
                            })
                    });
                if !same && !successor && !stopped {
                    return Err(conflict(
                        "proof is stale for the current lifecycle generation",
                    ));
                }
                if stopped { None } else { Some(record.proof) }
            }
        } else {
            None
        };
        transaction.commit()?;
        Ok(result)
    }
}

#[cfg(test)]
#[path = "machine_boot_non_dispatch_tests.rs"]
mod tests;
