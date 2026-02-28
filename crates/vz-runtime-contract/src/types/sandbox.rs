use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::ContractInvariantError;

/// Sandbox backend identifier.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SandboxBackend {
    /// Apple Virtualization.framework backend.
    MacosVz,
    /// Linux Firecracker backend.
    LinuxFirecracker,
    /// Future or custom backend identifier.
    Other(String),
}

/// Sandbox resource/network specification.
pub const SANDBOX_LABEL_BASE_IMAGE_REF: &str = "vz.sandbox.base_image_ref";
/// Canonical sandbox label key for main container selection.
pub const SANDBOX_LABEL_MAIN_CONTAINER: &str = "vz.sandbox.main_container";
/// Canonical sandbox label key for project workspace directory mount.
pub const SANDBOX_LABEL_PROJECT_DIR: &str = "project_dir";
/// Canonical sandbox label key for spaces mode selection.
pub const SANDBOX_LABEL_SPACE_MODE: &str = "vz.space.mode";
/// Canonical sandbox label value indicating spaces mode is required.
pub const SANDBOX_SPACE_MODE_REQUIRED: &str = "required";
/// Canonical sandbox label key for the checked-in space config path.
pub const SANDBOX_LABEL_SPACE_CONFIG_PATH: &str = "vz.space.config_path";
/// Canonical sandbox label key prefix mapping space secret names to external env sources.
pub const SANDBOX_LABEL_SPACE_SECRET_ENV_PREFIX: &str = "vz.space.secret.env.";
/// Canonical sandbox label key describing how base image defaulting was applied.
pub const SANDBOX_LABEL_BASE_IMAGE_DEFAULT_SOURCE: &str = "vz.sandbox.base_image_default_source";
/// Canonical sandbox label key describing how main container defaulting was applied.
pub const SANDBOX_LABEL_MAIN_CONTAINER_DEFAULT_SOURCE: &str =
    "vz.sandbox.main_container_default_source";

/// Sandbox resource/network specification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SandboxSpec {
    /// Optional CPU core limit.
    pub cpus: Option<u8>,
    /// Optional memory limit in MB.
    pub memory_mb: Option<u64>,
    /// Optional default image reference for sandbox startup workload.
    pub base_image_ref: Option<String>,
    /// Optional main workload/container identifier for sandbox startup.
    pub main_container: Option<String>,
    /// Logical network profile identifier.
    pub network_profile: Option<String>,
    /// Volume attachments to surface in the sandbox.
    pub volume_mounts: Vec<SandboxVolumeMount>,
}

/// Reference to a volume attachment in sandbox spec.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SandboxVolumeMount {
    /// Referenced volume identifier.
    pub volume_id: String,
    /// Target path mounted inside workloads.
    pub target: String,
    /// Read-only attachment flag.
    pub read_only: bool,
}

/// Sandbox lifecycle states.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SandboxState {
    /// Sandbox resources are being provisioned.
    Creating,
    /// Sandbox is available for lease/workload operations.
    Ready,
    /// Sandbox is accepting no new work and is draining.
    Draining,
    /// Sandbox has been terminated and cannot be resumed.
    Terminated,
    /// Sandbox failed irrecoverably.
    Failed,
}

impl SandboxState {
    /// Whether this state accepts no further transitions.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Terminated | Self::Failed)
    }

    fn can_transition_to(self, next: SandboxState) -> bool {
        matches!(
            (self, next),
            (SandboxState::Creating, SandboxState::Ready)
                | (SandboxState::Creating, SandboxState::Failed)
                | (SandboxState::Ready, SandboxState::Draining)
                | (SandboxState::Ready, SandboxState::Failed)
                | (SandboxState::Draining, SandboxState::Terminated)
                | (SandboxState::Draining, SandboxState::Failed)
        )
    }
}

/// Isolated runtime boundary that owns resources and lifecycle lineage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Sandbox {
    /// Sandbox identifier.
    pub sandbox_id: String,
    /// Backend implementation serving this sandbox.
    pub backend: SandboxBackend,
    /// Sandbox resource specification.
    pub spec: SandboxSpec,
    /// Current sandbox state.
    pub state: SandboxState,
    /// Unix epoch seconds of sandbox creation.
    pub created_at: u64,
    /// Unix epoch seconds of last update.
    pub updated_at: u64,
    /// Free-form metadata labels.
    pub labels: BTreeMap<String, String>,
}

impl Sandbox {
    /// Validate that the sandbox currently permits opening a new lease.
    pub fn ensure_can_open_lease(&self) -> Result<(), ContractInvariantError> {
        if self.state != SandboxState::Ready {
            return Err(ContractInvariantError::LeaseRequiresReadySandbox {
                sandbox_id: self.sandbox_id.clone(),
                state: self.state,
            });
        }

        Ok(())
    }

    /// Transition to a new sandbox state if allowed.
    pub fn transition_to(&mut self, next: SandboxState) -> Result<(), ContractInvariantError> {
        if self.state == next {
            return Ok(());
        }

        if !self.state.can_transition_to(next) {
            return Err(ContractInvariantError::SandboxStateTransition {
                sandbox_id: self.sandbox_id.clone(),
                from: self.state,
                to: next,
            });
        }

        self.state = next;
        Ok(())
    }
}

/// Lease lifecycle states.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    /// Lease is being established.
    Opening,
    /// Lease is healthy and can accept new work.
    Active,
    /// Lease expired due to TTL timeout.
    Expired,
    /// Lease was closed explicitly.
    Closed,
    /// Lease failed irrecoverably.
    Failed,
}

impl LeaseState {
    /// Whether this state is terminal for new work submission.
    pub const fn is_terminal_for_work(self) -> bool {
        matches!(self, Self::Expired | Self::Closed | Self::Failed)
    }

    fn can_transition_to(self, next: LeaseState) -> bool {
        matches!(
            (self, next),
            (LeaseState::Opening, LeaseState::Active)
                | (LeaseState::Opening, LeaseState::Expired)
                | (LeaseState::Opening, LeaseState::Failed)
                | (LeaseState::Active, LeaseState::Expired)
                | (LeaseState::Active, LeaseState::Closed)
                | (LeaseState::Active, LeaseState::Failed)
        )
    }
}

/// Time-bounded access grant for sandbox operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lease {
    /// Lease identifier.
    pub lease_id: String,
    /// Associated sandbox identifier.
    pub sandbox_id: String,
    /// Lease duration in seconds.
    pub ttl_secs: u64,
    /// Last heartbeat timestamp in unix epoch seconds.
    pub last_heartbeat_at: u64,
    /// Current lease state.
    pub state: LeaseState,
}

impl Lease {
    /// Validate that this lease can accept new work.
    pub fn ensure_can_submit_work(&self, operation: &str) -> Result<(), ContractInvariantError> {
        if self.state != LeaseState::Active {
            return Err(ContractInvariantError::WorkRequiresActiveLease {
                lease_id: self.lease_id.clone(),
                state: self.state,
                operation: operation.to_string(),
            });
        }

        Ok(())
    }

    /// Transition to a new lease state if allowed.
    pub fn transition_to(&mut self, next: LeaseState) -> Result<(), ContractInvariantError> {
        if self.state == next {
            return Ok(());
        }

        if !self.state.can_transition_to(next) {
            return Err(ContractInvariantError::LeaseStateTransition {
                lease_id: self.lease_id.clone(),
                from: self.state,
                to: next,
            });
        }

        self.state = next;
        Ok(())
    }
}
