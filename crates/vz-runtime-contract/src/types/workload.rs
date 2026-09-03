use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{ContractInvariantError, MountAccess, PortProtocol};

/// Container-level resource requests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ContainerResources {
    /// Requested CPU cores.
    pub cpus: Option<u8>,
    /// Requested memory limit in MB.
    pub memory_mb: Option<u64>,
}

/// Reference to a volume attached to a container.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerMount {
    /// Referenced volume identifier.
    pub volume_id: String,
    /// Mount target path in container filesystem.
    pub target: String,
    /// Mount access mode.
    pub access_mode: MountAccess,
}

/// Container runtime specification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ContainerSpec {
    /// Init command and arguments.
    pub cmd: Vec<String>,
    /// Environment variable key/value mapping.
    pub env: BTreeMap<String, String>,
    /// Working directory.
    pub cwd: Option<String>,
    /// User identity.
    pub user: Option<String>,
    /// Volume mount references.
    pub mounts: Vec<ContainerMount>,
    /// Requested resources.
    pub resources: ContainerResources,
    /// Attached network domain IDs.
    pub network_attachments: Vec<String>,
}

/// Container lifecycle states.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContainerState {
    /// Metadata created.
    Created,
    /// Transitioning into running state.
    Starting,
    /// Actively running.
    Running,
    /// Graceful stop in progress.
    Stopping,
    /// Exited with status code.
    Exited,
    /// Failed before a clean exit.
    Failed,
    /// Container removed and no longer addressable.
    Removed,
}

impl ContainerState {
    /// Whether this state is terminal.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Exited | Self::Failed | Self::Removed)
    }

    fn can_transition_to(self, next: ContainerState) -> bool {
        matches!(
            (self, next),
            (ContainerState::Created, ContainerState::Starting)
                | (ContainerState::Created, ContainerState::Removed)
                | (ContainerState::Created, ContainerState::Failed)
                | (ContainerState::Starting, ContainerState::Running)
                | (ContainerState::Starting, ContainerState::Failed)
                | (ContainerState::Starting, ContainerState::Removed)
                | (ContainerState::Running, ContainerState::Stopping)
                | (ContainerState::Running, ContainerState::Exited)
                | (ContainerState::Running, ContainerState::Failed)
                | (ContainerState::Stopping, ContainerState::Exited)
                | (ContainerState::Stopping, ContainerState::Failed)
                | (ContainerState::Exited, ContainerState::Removed)
                | (ContainerState::Failed, ContainerState::Removed)
        )
    }
}

/// Runtime V2 container record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Container {
    /// Container identifier.
    pub container_id: String,
    /// Owning sandbox identifier.
    pub sandbox_id: String,
    /// Immutable source image digest.
    pub image_digest: String,
    /// Runtime specification used to create the workload.
    pub container_spec: ContainerSpec,
    /// Current lifecycle state.
    pub state: ContainerState,
    /// Creation timestamp in unix epoch seconds.
    pub created_at: u64,
    /// Start timestamp, when started.
    pub started_at: Option<u64>,
    /// End timestamp, when terminal.
    pub ended_at: Option<u64>,
}

impl Container {
    /// Validate that an exec operation can run in this container.
    pub fn ensure_can_exec(&self) -> Result<(), ContractInvariantError> {
        if self.state != ContainerState::Running {
            return Err(ContractInvariantError::ExecRequiresRunningContainer {
                container_id: self.container_id.clone(),
                state: self.state,
            });
        }

        Ok(())
    }

    /// Transition to a new container state if allowed.
    pub fn transition_to(&mut self, next: ContainerState) -> Result<(), ContractInvariantError> {
        if self.state == next {
            return Ok(());
        }

        if !self.state.can_transition_to(next) {
            return Err(ContractInvariantError::ContainerStateTransition {
                container_id: self.container_id.clone(),
                from: self.state,
                to: next,
            });
        }

        self.state = next;
        Ok(())
    }
}

/// Execution request details for running command inside a container.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ExecutionSpec {
    /// Executable path and arguments.
    pub cmd: Vec<String>,
    /// Optional arg list override.
    pub args: Vec<String>,
    /// Environment overrides.
    pub env_override: BTreeMap<String, String>,
    /// Pseudo-terminal mode.
    pub pty: bool,
    /// Optional timeout in seconds.
    pub timeout_secs: Option<u64>,
}

/// Execution lifecycle states.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    /// Command accepted and waiting to start.
    Queued,
    /// Command currently executing.
    Running,
    /// Command exited naturally.
    Exited,
    /// Command failed unexpectedly.
    Failed,
    /// Command canceled by caller.
    Canceled,
}

impl ExecutionState {
    /// Whether this state is terminal.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Exited | Self::Failed | Self::Canceled)
    }

    fn can_transition_to(self, next: ExecutionState) -> bool {
        matches!(
            (self, next),
            (ExecutionState::Queued, ExecutionState::Running)
                | (ExecutionState::Queued, ExecutionState::Failed)
                | (ExecutionState::Queued, ExecutionState::Canceled)
                | (ExecutionState::Running, ExecutionState::Exited)
                | (ExecutionState::Running, ExecutionState::Failed)
                | (ExecutionState::Running, ExecutionState::Canceled)
        )
    }
}

/// Execution record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Execution {
    /// Execution identifier.
    pub execution_id: String,
    /// Target container identifier.
    pub container_id: String,
    /// Requested execution parameters.
    pub exec_spec: ExecutionSpec,
    /// Current state.
    pub state: ExecutionState,
    /// Exit code for completed commands.
    pub exit_code: Option<i32>,
    /// Start timestamp when running/terminal.
    pub started_at: Option<u64>,
    /// End timestamp once terminal.
    pub ended_at: Option<u64>,
}

impl Execution {
    /// Validate execution metadata consistency.
    pub fn ensure_lifecycle_consistency(&self) -> Result<(), ContractInvariantError> {
        if let (Some(started), Some(ended)) = (self.started_at, self.ended_at) {
            if ended < started {
                return Err(ContractInvariantError::ExecutionLifecycleInconsistency {
                    execution_id: self.execution_id.clone(),
                    details: "end time cannot precede start time".to_string(),
                });
            }
        }

        match self.state {
            ExecutionState::Queued => {
                if self.started_at.is_some() || self.ended_at.is_some() || self.exit_code.is_some()
                {
                    return Err(ContractInvariantError::ExecutionLifecycleInconsistency {
                        execution_id: self.execution_id.clone(),
                        details: "queued executions cannot include start/end/exit metadata"
                            .to_string(),
                    });
                }
            }
            ExecutionState::Running => {
                if self.started_at.is_none() || self.ended_at.is_some() || self.exit_code.is_some()
                {
                    return Err(ContractInvariantError::ExecutionLifecycleInconsistency {
                        execution_id: self.execution_id.clone(),
                        details: "running executions require start time and no terminal metadata"
                            .to_string(),
                    });
                }
            }
            ExecutionState::Exited => {
                if self.started_at.is_none() || self.ended_at.is_none() || self.exit_code.is_none()
                {
                    return Err(ContractInvariantError::ExecutionLifecycleInconsistency {
                        execution_id: self.execution_id.clone(),
                        details: "exited executions require start/end times and exit code"
                            .to_string(),
                    });
                }
            }
            ExecutionState::Failed | ExecutionState::Canceled => {
                if self.started_at.is_none() || self.ended_at.is_none() {
                    return Err(ContractInvariantError::ExecutionLifecycleInconsistency {
                        execution_id: self.execution_id.clone(),
                        details: "terminal executions require start/end times".to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Transition to a new execution state if allowed.
    pub fn transition_to(&mut self, next: ExecutionState) -> Result<(), ContractInvariantError> {
        if self.state == next {
            return Ok(());
        }

        if !self.state.can_transition_to(next) {
            return Err(ContractInvariantError::ExecutionStateTransition {
                execution_id: self.execution_id.clone(),
                from: self.state,
                to: next,
            });
        }

        self.state = next;
        Ok(())
    }
}

/// Volume backing type.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VolumeType {
    /// Bind mount from host.
    Bind,
    /// Persistent named volume.
    Named,
    /// Ephemeral volume.
    Ephemeral,
    /// Secret material volume.
    Secret,
}

/// Persistent or ephemeral storage attachment unit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Volume {
    /// Volume identifier.
    pub volume_id: String,
    /// Owning sandbox identifier.
    pub sandbox_id: String,
    /// Volume backing type.
    pub volume_type: VolumeType,
    /// Source path, ref, or provider key.
    pub source: String,
    /// Mount target path.
    pub target: String,
    /// Access mode.
    pub access_mode: MountAccess,
}

/// Network domain lifecycle states.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NetworkDomainState {
    /// Network domain is provisioning.
    Creating,
    /// Network domain is operational.
    Ready,
    /// Network domain is draining connections.
    Draining,
    /// Network domain has terminated.
    Terminated,
    /// Network domain failed.
    Failed,
}

/// Published port details within a network domain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishedPort {
    /// Host-side listener port.
    pub host_port: u16,
    /// Container-side target port.
    pub container_port: u16,
    /// Transport protocol.
    pub protocol: PortProtocol,
}

/// Isolated network scope for sandbox or stack workloads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkDomain {
    /// Network domain identifier.
    pub network_id: String,
    /// Associated sandbox identifier.
    pub sandbox_id: Option<String>,
    /// Associated stack identifier.
    pub stack_id: Option<String>,
    /// Current network state.
    pub state: NetworkDomainState,
    /// DNS zone suffix used inside this domain.
    pub dns_zone: String,
    /// Published ingress ports.
    pub published_ports: Vec<PublishedPort>,
}

impl NetworkDomain {
    /// Validate exactly one scope owner is set (sandbox or stack).
    pub fn has_valid_scope(&self) -> bool {
        self.sandbox_id.is_some() ^ self.stack_id.is_some()
    }
}
