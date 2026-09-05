//! Non-expiring Up request identity, atomically reserved with fresh instances.
use super::{StateStore, topology::EnvironmentUpReservation};
use crate::StackError;
use rusqlite::params;
use sha2::{Digest, Sha256};
use vz_runtime_contract::{
    EnvironmentInstance, EnvironmentSelectionContext, EnvironmentUpAdmission,
    EnvironmentUpCompletion, MachineErrorCode, ProjectDefinition,
};

#[cfg(test)]
#[path = "environment_up_tests.rs"]
mod tests;

fn conflict(message: impl Into<String>) -> StackError {
    StackError::Machine {
        code: MachineErrorCode::StateConflict,
        message: message.into(),
    }
}
fn key(kind: &str, idempotency_key: &str) -> String {
    format!(
        "environment-up-{kind}-v1/{:x}",
        Sha256::digest(idempotency_key.as_bytes())
    )
}

impl StateStore {
    pub fn load_environment_up_admission(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<EnvironmentUpAdmission>, StackError> {
        let Some(value) = self.get_control_metadata(&key("admission", idempotency_key))? else {
            return Ok(None);
        };
        let admission: EnvironmentUpAdmission = serde_json::from_str(&value)?;
        admission.validate().map_err(conflict)?;
        if admission.idempotency_key != idempotency_key {
            return Err(conflict(
                "Up admission key does not match immutable receipt",
            ));
        }
        Ok(Some(admission))
    }

    /// Authorize the exact selected/prospective IDs before insertion. First
    /// default creation and its idempotency identity commit together; a crash
    /// before lifecycle begin cannot cause a new unbound sibling on retry.
    #[allow(clippy::too_many_arguments)]
    pub fn reserve_environment_up_admission(
        &self,
        definition: &ProjectDefinition,
        selection: &EnvironmentSelectionContext,
        request_id: &str,
        idempotency_key: &str,
        request_hash: &str,
        now: u64,
        authorize: impl Fn(&EnvironmentInstance) -> Result<(), StackError>,
    ) -> Result<EnvironmentUpAdmission, StackError> {
        definition
            .validate()
            .map_err(|error| conflict(error.to_string()))?;
        let digest = definition
            .digest()
            .map_err(|error| conflict(error.to_string()))?;
        self.with_immediate_transaction(|store| {
            if let Some(existing) = store.load_environment_up_admission(idempotency_key)? {
                if existing.project_id != definition.project_id
                    || existing.definition_digest != digest
                    || existing.request_id != request_id
                    || existing.request_hash != request_hash
                    || existing.workspace_key != selection.workspace_key
                {
                    return Err(conflict(
                        "Up idempotency key belongs to a different immutable request",
                    ));
                }
                let environment = store
                    .load_environment_instance(existing.environment_id.as_str())?
                    .ok_or_else(|| conflict("reserved Up Environment no longer exists"))?;
                let mut machines = environment
                    .machines
                    .iter()
                    .map(|machine| machine.machine_id.clone())
                    .collect::<Vec<_>>();
                machines.sort();
                if environment.project_id != existing.project_id
                    || environment.definition_digest != existing.definition_digest
                    || machines != existing.machine_ids
                {
                    return Err(conflict("reserved Up ownership changed after admission"));
                }
                authorize(&environment)?;
                return Ok(existing);
            }
            if store
                .load_environment_lifecycle_by_idempotency_key(idempotency_key)?
                .is_some()
            {
                return Err(conflict(
                    "lifecycle key already exists without this exact Up admission",
                ));
            }
            let reservation = store.resolve_or_reserve_environment_for_up_in_transaction(
                definition, selection, now, &authorize,
            )?;
            let environment = match reservation {
                EnvironmentUpReservation::Existing { environment, .. }
                | EnvironmentUpReservation::Created { environment } => environment,
            };
            let mut machine_ids = environment
                .machines
                .iter()
                .map(|machine| machine.machine_id.clone())
                .collect::<Vec<_>>();
            machine_ids.sort();
            let admission = EnvironmentUpAdmission {
                schema_version: 1,
                project_id: definition.project_id.clone(),
                environment_id: environment.environment_id,
                machine_ids,
                definition_digest: digest.clone(),
                request_id: request_id.into(),
                idempotency_key: idempotency_key.into(),
                request_hash: request_hash.into(),
                workspace_key: selection.workspace_key.clone(),
                created_at: now,
            };
            admission.validate().map_err(conflict)?;
            store.conn.execute(
                "INSERT INTO control_metadata(key,value) VALUES (?1,?2)",
                params![
                    key("admission", idempotency_key),
                    serde_json::to_string(&admission)?
                ],
            )?;
            Ok(admission)
        })
    }

    pub fn load_environment_up_completion(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<EnvironmentUpCompletion>, StackError> {
        let Some(value) = self.get_control_metadata(&key("completion", idempotency_key))? else {
            return Ok(None);
        };
        let completion: EnvironmentUpCompletion = serde_json::from_str(&value)?;
        completion.validate().map_err(conflict)?;
        if self
            .load_environment_up_admission(idempotency_key)?
            .as_ref()
            != Some(&completion.admission)
        {
            return Err(conflict(
                "Up completion no longer matches its immutable admission",
            ));
        }
        Ok(Some(completion))
    }

    pub fn finish_environment_up_admission(
        &self,
        completion: &EnvironmentUpCompletion,
    ) -> Result<(), StackError> {
        completion.validate().map_err(conflict)?;
        self.with_immediate_transaction(|store| {
            if store
                .load_environment_up_admission(&completion.admission.idempotency_key)?
                .as_ref()
                != Some(&completion.admission)
            {
                return Err(conflict("Up completion changed immutable admission"));
            }
            if let Some(previous) =
                store.load_environment_up_completion(&completion.admission.idempotency_key)?
            {
                return if previous == *completion {
                    Ok(())
                } else {
                    Err(conflict("terminal Up receipt is immutable"))
                };
            }
            if let Some(operation) = &completion.operation {
                if store
                    .load_environment_lifecycle(operation.operation_id.as_str())?
                    .as_ref()
                    != Some(operation)
                {
                    return Err(conflict(
                        "Up completion does not match the current durable lifecycle journal",
                    ));
                }
            }
            if let Some(binding) = &completion.workspace_binding {
                let environment = store
                    .load_environment_instance(completion.admission.environment_id.as_str())?
                    .ok_or_else(|| conflict("Up completion Environment no longer exists"))?;
                if !environment.bindings.iter().any(|stored| stored == binding) {
                    return Err(conflict(
                        "Up completion binding does not match an exact durable workspace binding",
                    ));
                }
            }
            store.conn.execute(
                "INSERT INTO control_metadata(key,value) VALUES (?1,?2)",
                params![
                    key("completion", &completion.admission.idempotency_key),
                    serde_json::to_string(completion)?
                ],
            )?;
            Ok(())
        })
    }
}
