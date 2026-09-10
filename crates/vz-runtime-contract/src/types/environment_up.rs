//! Atomic Up creation/admission identity and durable completion receipts.

use crate::{
    EnvironmentId, EnvironmentLifecycleKind, EnvironmentLifecycleOperation,
    EnvironmentLifecycleStatus, MachineError, MachineId, ProjectId, WorkspaceBinding,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Desired Up input; path_hint is diagnostic and excluded from mutation identity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentUpRequest {
    pub definition: super::ProjectDefinition,
    pub selection: super::EnvironmentSelectionContext,
    pub path_hint: Option<String>,
    /// Authoritative canonical absolute worktree root for workspace projections.
    ///
    /// This is deliberately NOT `path_hint`. `path_hint` is diagnostic and
    /// excluded from the mutation identity, so authorizing a host share from it
    /// would let two Ups with different roots share one idempotency key. This
    /// field is authorizing and enters `request_hash`, which is what makes the
    /// resolved projection path part of the mutation identity: the resolved
    /// path is a function of this root and the definition's declared relative
    /// `source_path`, and both are hashed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<String>,
    pub timeout_millis: u64,
    /// Fork one existing Machine of the selected Environment instead of only
    /// reconciling the definition.
    ///
    /// Authorizing, and therefore hashed: two Ups that differ only in which
    /// Machine they fork are different mutations, and sharing an idempotency
    /// key between them would let a replay return the wrong fork's receipt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork: Option<MachineForkRequest>,
}

/// `vz up --fork-from <machine> --as <machine>@<label>`.
///
/// Fork is a flag on Up rather than a sixth verb because the public lifecycle
/// has exactly five, and the agent that drives this speaks the CLI.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MachineForkRequest {
    /// Name or immutable ID of the Machine to seed from.
    pub fork_from: String,
    /// The fork's full address, `<machine>@<label>`.
    ///
    /// Required rather than derived from the worktree, because the caller must
    /// name what it will later target: an agent that cannot predict the address
    /// cannot address it. The CLI supplies the default when the caller omits it.
    pub fork_as: String,
}

impl MachineForkRequest {
    /// The parsed address, and the label it carries.
    ///
    /// Refuses an address whose Machine half disagrees with `fork_from` when
    /// `fork_from` is a name rather than an ID: `--fork-from backend --as
    /// api@feat-x` names two different Machines and is a mistake worth catching
    /// before anything is minted.
    pub fn resolve(&self) -> Result<(super::MachineForkAddress, String), String> {
        let address =
            super::MachineForkAddress::parse(&self.fork_as).map_err(|error| error.to_string())?;
        let label = address.label.clone().ok_or_else(|| {
            format!(
                "`{}` is not a fork address; expected `<machine>@<label>`",
                self.fork_as
            )
        })?;
        if self.fork_from.trim().is_empty() {
            return Err("fork source Machine must not be empty".into());
        }
        Ok((address, label))
    }
}

impl EnvironmentUpRequest {
    pub fn request_hash(&self) -> Result<String, String> {
        self.definition
            .validate()
            .map_err(|error| error.to_string())?;
        if !(1_000..=3_600_000).contains(&self.timeout_millis) {
            return Err("Up deadline must be 1..3600 seconds".into());
        }
        let mut selection = self.selection.clone();
        if selection.explicit.is_some() {
            selection.process_environment_id = None;
        }
        if let Some(fork) = &self.fork {
            fork.resolve()?;
        }
        // `fork` is appended to the hashed tuple rather than inserted, so an Up
        // that forks nothing hashes exactly as it did before forking existed and
        // every persisted receipt stays replayable.
        let bytes = serde_json::to_vec(&(
            &self.definition,
            selection,
            &self.workspace_root,
            self.timeout_millis,
            &self.fork,
        ))
        .map_err(|error| error.to_string())?;
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }
}

/// Coalescing snapshots have strictly increasing sequence numbers. Intermediate
/// snapshots may be omitted by a slow observer; the terminal receipt may not.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentUpProgress {
    pub schema_version: u32,
    pub sequence: u64,
    pub admission: EnvironmentUpAdmission,
    pub phase: String,
    pub operation: Option<EnvironmentLifecycleOperation>,
    pub completion: Option<EnvironmentUpCompletion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preparation: Option<EnvironmentPreparationProgress>,
}

/// Bounded preparation progress independent of lifecycle phase. Units are local
/// to each label; clients can always render completed/total as a progress bar.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentPreparationProgress {
    pub label: String,
    pub completed: u64,
    pub total: u64,
}
impl EnvironmentPreparationProgress {
    pub fn validate(&self) -> Result<(), String> {
        if self.label.is_empty()
            || self.label.len() > 128
            || self.label.chars().any(char::is_control)
            || self.total == 0
            || self.completed > self.total
        {
            return Err("invalid bounded preparation progress".into());
        }
        Ok(())
    }
}

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
