//! Atomic Up creation/admission identity and durable completion receipts.

use crate::{
    EnvironmentId, EnvironmentLifecycleKind, EnvironmentLifecycleOperation,
    EnvironmentLifecycleStatus, MachineError, MachineId, ProjectId, WorkspaceBinding,
};
use serde::{Deserialize, Serialize};

/// Immutable intent reserved in the same transaction as a newly named instance.
/// It survives a crash before lifecycle begin, so an unbound default-creation
/// retry resolves its exact original Environment instead of creating a sibling.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentUpAdmission {
    pub schema_version: u32,
    pub project_id: ProjectId,
    pub environment_id: EnvironmentId,
    pub machine_ids: Vec<MachineId>,
    pub definition_digest: String,
    pub request_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub workspace_key: Option<String>,
    pub created_at: u64,
}

impl EnvironmentUpAdmission {
    pub fn validate(&self) -> Result<(), String> {
        ProjectId::new(self.project_id.to_string()).map_err(|error| error.to_string())?;
        EnvironmentId::new(self.environment_id.to_string()).map_err(|error| error.to_string())?;
        if self.schema_version != 1 || self.machine_ids.is_empty() || self.machine_ids.len() > 128 {
            return Err("invalid Up admission schema or bounded Machine inventory".into());
        }
        let mut sorted = self.machine_ids.clone();
        sorted.sort();
        sorted.dedup();
        if sorted != self.machine_ids {
            return Err("Up admission Machine inventory must be sorted and unique".into());
        }
        for machine in &self.machine_ids {
            MachineId::new(machine.to_string()).map_err(|error| error.to_string())?;
        }
        for value in [&self.request_id, &self.idempotency_key] {
            if value.is_empty()
                || value.len() > 256
                || value.trim() != value
                || value.chars().any(char::is_control)
            {
                return Err("invalid Up request or idempotency identity".into());
            }
        }
        for digest in [&self.definition_digest, &self.request_hash] {
            if !digest.strip_prefix("sha256:").is_some_and(|hash| {
                hash.len() == 64
                    && hash
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            }) {
                return Err("Up digest must be canonical SHA-256".into());
            }
        }
        if self.workspace_key.as_ref().is_some_and(|key| {
            key.is_empty()
                || key.len() > 512
                || key.trim() != key
                || key.chars().any(char::is_control)
        }) {
            return Err("invalid opaque Up workspace key".into());
        }
        Ok(())
    }
}

/// Entire requested Up outcome, including the success-only workspace binding.
/// A succeeded Machine lifecycle with a binding error is not an Up success.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentUpCompletion {
    pub admission: EnvironmentUpAdmission,
    pub operation: Option<EnvironmentLifecycleOperation>,
    pub workspace_binding: Option<WorkspaceBinding>,
    pub error: Option<MachineError>,
    pub completed_at: u64,
}
impl EnvironmentUpCompletion {
    pub fn validate(&self) -> Result<(), String> {
        self.admission.validate()?;
        if self.completed_at < self.admission.created_at {
            return Err("Up completion predates admission".into());
        }
        if let Some(operation) = &self.operation {
            operation
                .validate_structure()
                .map_err(|error| error.to_string())?;
            let mut machines = operation
                .machine_steps
                .iter()
                .map(|step| step.machine_id.clone())
                .collect::<Vec<_>>();
            machines.sort();
            if operation.kind != EnvironmentLifecycleKind::Up
                || operation.project_id != self.admission.project_id
                || operation.environment_id != self.admission.environment_id
                || operation.definition_digest != self.admission.definition_digest
                || operation.request_id != self.admission.request_id
                || operation.idempotency_key != self.admission.idempotency_key
                || operation.request_hash != self.admission.request_hash
                || machines != self.admission.machine_ids
            {
                return Err("Up completion changed immutable operation ownership".into());
            }
            if self.error.is_none() && operation.status != EnvironmentLifecycleStatus::Succeeded {
                return Err("Up success requires a succeeded durable lifecycle".into());
            }
        } else if self.error.is_none() {
            return Err("Up success requires a durable lifecycle receipt".into());
        }
        if let Some(error) = &self.error {
            if error.request_id.as_deref() != Some(self.admission.request_id.as_str())
                || error.message.is_empty()
                || error.message.len() > 8192
            {
                return Err("Up failure correlation or diagnostic bound mismatch".into());
            }
        }
        if let Some(binding) = &self.workspace_binding {
            if self.error.is_some() {
                return Err(
                    "failed Up completion cannot publish a success-only workspace binding".into(),
                );
            }
            if binding.project_id != self.admission.project_id
                || binding.environment_id != self.admission.environment_id
                || self.admission.workspace_key.as_deref() != Some(binding.workspace_key.as_str())
            {
                return Err("Up workspace binding changed its exact owner or token".into());
            }
        }
        if self.error.is_none()
            && self.admission.workspace_key.is_some()
            && self.workspace_binding.is_none()
        {
            return Err("Up success omitted the requested workspace binding".into());
        }
        Ok(())
    }
}
