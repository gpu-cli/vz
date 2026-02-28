use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::ContractInvariantError;

/// Checkpoint class variants.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointClass {
    /// Filesystem-focused quick checkpoint.
    FsQuick,
    /// Full VM state checkpoint.
    VmFull,
}

/// Metadata that describes replay/restore guarantees for a checkpoint class.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointClassMetadata {
    /// Includes writable layer and volume state.
    pub includes_filesystem_state: bool,
    /// Includes live memory pages.
    pub includes_memory_state: bool,
    /// Includes CPU register and virtual device state.
    pub includes_cpu_and_device_state: bool,
}

impl CheckpointClass {
    /// Metadata semantics for this checkpoint class.
    pub const fn metadata(self) -> CheckpointClassMetadata {
        match self {
            CheckpointClass::FsQuick => CheckpointClassMetadata {
                includes_filesystem_state: true,
                includes_memory_state: false,
                includes_cpu_and_device_state: false,
            },
            CheckpointClass::VmFull => CheckpointClassMetadata {
                includes_filesystem_state: true,
                includes_memory_state: true,
                includes_cpu_and_device_state: true,
            },
        }
    }
}

/// Checkpoint lifecycle states.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointState {
    /// Checkpoint is being created.
    Creating,
    /// Checkpoint is ready for restore/fork.
    Ready,
    /// Checkpoint operation failed.
    Failed,
}

impl CheckpointState {
    fn can_transition_to(self, next: CheckpointState) -> bool {
        matches!(
            (self, next),
            (CheckpointState::Creating, CheckpointState::Ready)
                | (CheckpointState::Creating, CheckpointState::Failed)
        )
    }
}

/// Structured compatibility metadata captured for a checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CheckpointCompatibilityMetadata {
    /// Backend/runtime implementation identifier.
    pub backend_id: String,
    /// Backend build or semantic version.
    pub backend_version: String,
    /// Runtime V2 contract/runtime version.
    pub runtime_version: String,
    /// Guest artifact versions (kernel, initramfs, agent, etc).
    pub guest_artifact_versions: BTreeMap<String, String>,
    /// VM/container config digest.
    pub config_hash: String,
    /// Host markers relevant for compatibility gating.
    pub host_compatibility_markers: BTreeMap<String, String>,
}

impl CheckpointCompatibilityMetadata {
    /// Whether all required top-level compatibility fields are present.
    pub fn is_complete(&self) -> bool {
        !(self.backend_id.is_empty()
            || self.backend_version.is_empty()
            || self.runtime_version.is_empty()
            || self.config_hash.is_empty())
    }
}

/// Restorable runtime state capture.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    /// Checkpoint identifier.
    pub checkpoint_id: String,
    /// Owning sandbox identifier.
    pub sandbox_id: String,
    /// Optional parent checkpoint lineage ID.
    pub parent_checkpoint_id: Option<String>,
    /// Checkpoint class.
    pub class: CheckpointClass,
    /// Current checkpoint state.
    pub state: CheckpointState,
    /// Creation timestamp in unix epoch seconds.
    pub created_at: u64,
    /// Compatibility fingerprint for restore safety checks.
    pub compatibility_fingerprint: String,
}

impl Checkpoint {
    /// Transition to a new checkpoint state if allowed.
    pub fn transition_to(&mut self, next: CheckpointState) -> Result<(), ContractInvariantError> {
        if self.state == next {
            return Ok(());
        }

        if !self.state.can_transition_to(next) {
            return Err(ContractInvariantError::CheckpointStateTransition {
                checkpoint_id: self.checkpoint_id.clone(),
                from: self.state,
                to: next,
            });
        }

        self.state = next;
        Ok(())
    }
}

/// Complete metadata envelope persisted for checkpoint lineage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointMetadata {
    /// Core checkpoint record.
    pub checkpoint: Checkpoint,
    /// Class semantics snapshot for auditability.
    pub class_metadata: CheckpointClassMetadata,
    /// Structured compatibility details used by restore validation.
    pub compatibility: CheckpointCompatibilityMetadata,
}

impl CheckpointMetadata {
    /// Build metadata from a checkpoint and compatibility payload.
    pub fn new(checkpoint: Checkpoint, compatibility: CheckpointCompatibilityMetadata) -> Self {
        Self {
            class_metadata: checkpoint.class.metadata(),
            checkpoint,
            compatibility,
        }
    }
}

/// In-memory lineage catalog keyed by checkpoint id.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CheckpointLineageStore {
    /// Checkpoint metadata records by checkpoint id.
    pub checkpoints: BTreeMap<String, CheckpointMetadata>,
}

impl CheckpointLineageStore {
    /// Register a checkpoint metadata record and enforce parent lineage existence.
    pub fn register(&mut self, metadata: CheckpointMetadata) -> Result<(), ContractInvariantError> {
        let checkpoint_id = metadata.checkpoint.checkpoint_id.clone();
        if self.checkpoints.contains_key(&checkpoint_id) {
            return Err(ContractInvariantError::CheckpointAlreadyExists { checkpoint_id });
        }

        if let Some(parent_checkpoint_id) = metadata.checkpoint.parent_checkpoint_id.clone() {
            if !self.checkpoints.contains_key(&parent_checkpoint_id) {
                return Err(ContractInvariantError::CheckpointParentNotFound {
                    checkpoint_id,
                    parent_checkpoint_id,
                });
            }
        }

        self.checkpoints.insert(checkpoint_id, metadata);
        Ok(())
    }

    /// Retrieve a checkpoint metadata record by id.
    pub fn get(&self, checkpoint_id: &str) -> Option<&CheckpointMetadata> {
        self.checkpoints.get(checkpoint_id)
    }

    /// List checkpoint records for a sandbox ordered by create timestamp.
    pub fn list_for_sandbox(&self, sandbox_id: &str) -> Vec<CheckpointMetadata> {
        let mut records: Vec<_> = self
            .checkpoints
            .values()
            .filter(|record| record.checkpoint.sandbox_id == sandbox_id)
            .cloned()
            .collect();
        records.sort_by(|lhs, rhs| {
            lhs.checkpoint
                .created_at
                .cmp(&rhs.checkpoint.created_at)
                .then_with(|| {
                    lhs.checkpoint
                        .checkpoint_id
                        .cmp(&rhs.checkpoint.checkpoint_id)
                })
        });
        records
    }

    /// List direct children for a parent checkpoint id.
    pub fn children_of(&self, parent_checkpoint_id: &str) -> Vec<CheckpointMetadata> {
        let mut records: Vec<_> = self
            .checkpoints
            .values()
            .filter(|record| {
                record.checkpoint.parent_checkpoint_id.as_deref() == Some(parent_checkpoint_id)
            })
            .cloned()
            .collect();
        records.sort_by(|lhs, rhs| {
            lhs.checkpoint
                .checkpoint_id
                .cmp(&rhs.checkpoint.checkpoint_id)
        });
        records
    }
}
