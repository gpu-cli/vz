//! Exact-Machine execution contracts, independent of legacy sandbox containers.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{EnvironmentId, MachineId, MachineIncarnation, MachineRuntimeIdentity, ProjectId};

/// Requested process attributes. No host paths or implicit shell interpretation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MachineExecutionSpec {
    pub argv: Vec<String>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    pub working_directory: Option<String>,
    pub user: Option<String>,
    pub terminal: Option<MachineExecutionTerminal>,
    pub timeout_millis: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MachineExecutionTerminal {
    pub rows: u16,
    pub columns: u16,
}

impl MachineExecutionSpec {
    /// Canonical semantic request digest shared by admission and its client.
    /// Selector spelling is not identity; resolved Project/Environment/Machine
    /// identity and every process attribute are bound into the digest.
    pub fn request_hash(
        &self,
        project: &ProjectId,
        environment: &EnvironmentId,
        machine: &MachineId,
    ) -> Result<String, String> {
        use sha2::{Digest, Sha256};
        self.validate()?;
        let bytes = serde_json::to_vec(&(1u32, project, environment, machine, self))
            .map_err(|error| error.to_string())?;
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.argv.is_empty()
            || self.argv.len() > 256
            || self.argv[0].is_empty()
            || !(1..=86_400_000).contains(&self.timeout_millis)
            || self.environment.len() > 256
            || self
                .terminal
                .is_some_and(|terminal| terminal.rows == 0 || terminal.columns == 0)
        {
            return Err(
                "invalid execution argv, timeout, environment or terminal dimensions".into(),
            );
        }
        let mut bytes = 0usize;
        for value in self
            .argv
            .iter()
            .chain(self.environment.keys())
            .chain(self.environment.values())
            .chain(self.working_directory.iter())
            .chain(self.user.iter())
        {
            if value.contains('\0') || value.len() > 65_536 {
                return Err("execution argument exceeds bound or contains NUL".into());
            }
            bytes += value.len();
        }
        if bytes > 1_048_576
            || self
                .environment
                .keys()
                .any(|key| key.is_empty() || key.contains('='))
        {
            return Err("execution input exceeds bound or has invalid environment key".into());
        }
        if self
            .working_directory
            .as_ref()
            .is_some_and(|directory| !directory.starts_with('/'))
        {
            return Err("execution working directory must be absolute inside the Machine".into());
        }
        Ok(())
    }
}

/// Immutable ownership and request identity pinned on every output frame.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MachineExecutionScope {
    pub schema_version: u32,
    pub execution_id: String,
    pub request_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub project_id: ProjectId,
    pub environment_id: EnvironmentId,
    pub machine_id: MachineId,
    pub environment_generation: u64,
    pub incarnation: MachineIncarnation,
    pub runtime_identity: MachineRuntimeIdentity,
    pub definition_digest: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MachineExecutionState {
    Admitted,
    Completed,
    /// Positive no-live-work proof; command side effects and exit status may be unknown.
    Quiesced,
    /// No positive terminal/reap proof. This is never permission to retry effects.
    Uncertain,
}

/// Durable admission/terminal record. Historical output is not replayed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MachineExecutionReceipt {
    pub scope: MachineExecutionScope,
    pub state: MachineExecutionState,
    pub exit_code: Option<i32>,
    pub failure: Option<String>,
    pub output_replay_available: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

impl MachineExecutionScope {
    /// Validate the structural comparison token, not a backend authority grant.
    pub fn validate(&self) -> Result<(), String> {
        ProjectId::new(self.project_id.to_string()).map_err(|error| error.to_string())?;
        EnvironmentId::new(self.environment_id.to_string()).map_err(|error| error.to_string())?;
        MachineId::new(self.machine_id.to_string()).map_err(|error| error.to_string())?;
        if self.schema_version != 1
            || self.incarnation.schema_version != 1
            || self.runtime_identity.schema_version != 1
            || self.environment_generation == 0
            || self.incarnation.machine_id != self.machine_id
            || self.incarnation.generation == 0
        {
            return Err("invalid Machine execution ownership/generation".into());
        }
        crate::MachineIncarnationId::new(self.incarnation.incarnation_id.to_string())
            .map_err(|error| error.to_string())?;
        for value in [&self.execution_id, &self.request_id, &self.idempotency_key] {
            if value.is_empty()
                || value.len() > 256
                || value.trim() != value
                || value.chars().any(char::is_control)
            {
                return Err("invalid Machine execution request identity".into());
            }
        }
        for value in [&self.request_hash, &self.definition_digest] {
            let Some(digest) = value.strip_prefix("sha256:") else {
                return Err("execution digest must be SHA-256".into());
            };
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err("execution digest must be canonical SHA-256".into());
            }
        }
        if self.runtime_identity.opaque_id.is_empty()
            || self.runtime_identity.opaque_id.len() > 4096
        {
            return Err("invalid Machine execution runtime identity".into());
        }
        Ok(())
    }
}

impl MachineExecutionReceipt {
    pub fn validate(&self) -> Result<(), String> {
        self.scope.validate()?;
        if self.updated_at < self.created_at
            || self.output_replay_available
            || self
                .exit_code
                .is_some_and(|code| !(0..=255).contains(&code))
            || self
                .failure
                .as_ref()
                .is_some_and(|failure| failure.is_empty() || failure.len() > 8192)
        {
            return Err("invalid Machine execution receipt".into());
        }
        let valid = match self.state {
            MachineExecutionState::Admitted => {
                self.exit_code.is_none()
                    && self.failure.is_none()
                    && self.updated_at == self.created_at
            }
            MachineExecutionState::Completed => self.exit_code.is_some(),
            MachineExecutionState::Quiesced => self.exit_code.is_none() && self.failure.is_some(),
            MachineExecutionState::Uncertain => self.exit_code.is_none() && self.failure.is_some(),
        };
        if !valid {
            return Err("execution state contradicts terminal proof".into());
        }
        Ok(())
    }
}
