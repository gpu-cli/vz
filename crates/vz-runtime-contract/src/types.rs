//! Backend-neutral runtime types shared across all container backends.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// ── Port mapping ──────────────────────────────────────────────────

/// Port mapping protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum PortProtocol {
    /// TCP stream forwarding.
    #[default]
    Tcp,
    /// UDP datagram forwarding.
    Udp,
}

impl PortProtocol {
    /// Protocol name as a lowercase string.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

/// Host-to-container port mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortMapping {
    /// Host port to listen on.
    pub host: u16,
    /// Container port to forward to.
    pub container: u16,
    /// Forwarding protocol.
    pub protocol: PortProtocol,
    /// Target host/IP inside the runtime for port forwarding.
    ///
    /// In stack mode with per-service networking, this is the service IP
    /// (e.g., `172.20.0.2`). When `None`, defaults to `127.0.0.1`.
    pub target_host: Option<String>,
}

// ── Mount specification ───────────────────────────────────────────

/// Mount type for container volume bindings.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum MountType {
    /// Bind mount from host to container.
    #[default]
    Bind,
    /// Ephemeral tmpfs mount inside the container.
    Tmpfs,
    /// Named volume backed by a persistent disk image inside the VM.
    ///
    /// The volume data lives at `/run/vz-oci/volumes/{volume_name}` on
    /// an ext4 block device attached to the guest VM.
    Volume {
        /// Volume name (used as the subdirectory on the disk image).
        volume_name: String,
    },
}

/// Access mode for container mounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum MountAccess {
    /// Read-write access (default).
    #[default]
    ReadWrite,
    /// Read-only access.
    ReadOnly,
}

/// A volume/bind mount specification for a container run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountSpec {
    /// Host source path (required for bind mounts, ignored for tmpfs).
    pub source: Option<PathBuf>,
    /// Container destination path (absolute).
    pub target: PathBuf,
    /// Mount type.
    pub mount_type: MountType,
    /// Access mode.
    pub access: MountAccess,
    /// For file bind mounts: subpath within the VirtioFS-shared parent directory.
    ///
    /// The VirtioFS share exposes the parent dir; the OCI bundle bind-mounts
    /// `/mnt/vz-mount-{idx}/{subpath}` to the container target.
    pub subpath: Option<String>,
}

// ── Run configuration ─────────────────────────────────────────────

/// Per-run options for a container.
#[derive(Debug, Clone, Default)]
pub struct RunConfig {
    /// Command to execute inside the container.
    pub cmd: Vec<String>,
    /// Optional working directory.
    pub working_dir: Option<String>,
    /// Environment variables.
    pub env: Vec<(String, String)>,
    /// Optional user to run as.
    pub user: Option<String>,
    /// Host-to-container port mappings.
    pub ports: Vec<PortMapping>,
    /// Volume/bind mount specifications.
    pub mounts: Vec<MountSpec>,
    /// Optional CPU cores override.
    pub cpus: Option<u8>,
    /// Optional memory limit in MB.
    pub memory_mb: Option<u64>,
    /// Optional network enable override.
    pub network_enabled: Option<bool>,
    /// Optional exec timeout override.
    pub timeout: Option<Duration>,
    /// Optional explicit container identifier.
    ///
    /// When unset, the runtime generates a unique ID.
    pub container_id: Option<String>,
    /// Optional init process command used for container create/start.
    ///
    /// If unset, the resolved run command is used for init.
    pub init_process: Option<Vec<String>>,
    /// Additional OCI runtime-spec annotations.
    pub oci_annotations: Vec<(String, String)>,
    /// Extra `/etc/hosts` entries as `(hostname, ip)` pairs.
    pub extra_hosts: Vec<(String, String)>,
    /// Path to an existing network namespace for the container to join.
    pub network_namespace_path: Option<String>,
    /// CPU quota in microseconds per `cpu_period` for cgroup CPU throttling.
    pub cpu_quota: Option<i64>,
    /// CPU CFS period in microseconds (default: 100000 = 100ms).
    pub cpu_period: Option<u64>,
    /// Redirect container stdout/stderr to log files for later retrieval.
    pub capture_logs: bool,
    // ── Security fields ──────────────────────────────────────────
    /// Additional Linux capabilities to add to the container.
    pub cap_add: Vec<String>,
    /// Linux capabilities to drop from the container defaults.
    pub cap_drop: Vec<String>,
    /// Run the container in privileged mode (all capabilities).
    pub privileged: bool,
    /// Mount the container root filesystem as read-only.
    pub read_only_rootfs: bool,
    /// Kernel parameters to set inside the container (sysctl).
    pub sysctls: Vec<(String, String)>,
    // ── Resource extensions ──────────────────────────────────────
    /// Per-process resource limits (name, soft, hard).
    pub ulimits: Vec<(String, u64, u64)>,
    /// Maximum number of PIDs in the container.
    pub pids_limit: Option<i64>,
    // ── Container identity ───────────────────────────────────────
    /// Container hostname override.
    pub hostname: Option<String>,
    /// Container domain name.
    pub domainname: Option<String>,
    // ── Stop lifecycle ──────────────────────────────────────────────
    /// Signal to send for graceful stop (e.g., "SIGQUIT"). Default: SIGTERM.
    pub stop_signal: Option<String>,
    /// Seconds to wait after stop signal before SIGKILL. Default: 10.
    pub stop_grace_period_secs: Option<u64>,
    // ── Shared VM mount support ──────────────────────────────────
    /// Offset added to VirtioFS mount tag indices in shared VM mode.
    ///
    /// In a shared VM, multiple containers share one set of VirtioFS
    /// shares. Each container's bind mounts are assigned a global index
    /// starting at this offset so tags don't collide between services
    /// (e.g., service A gets `vz-mount-0`, service B gets `vz-mount-2`).
    pub mount_tag_offset: usize,
}

// ── Exec configuration ────────────────────────────────────────────

/// Options for executing a command in an already-running container.
#[derive(Debug, Clone, Default)]
pub struct ExecConfig {
    /// Command and arguments to execute.
    pub cmd: Vec<String>,
    /// Optional working directory inside the container.
    pub working_dir: Option<String>,
    /// Environment variables for the process.
    pub env: Vec<(String, String)>,
    /// Optional user to run as inside the container.
    pub user: Option<String>,
    /// Optional exec timeout override.
    pub timeout: Option<Duration>,
}

// ── Output types ──────────────────────────────────────────────────

/// Output from a command executed inside a container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecOutput {
    /// Exit code of the command (0 = success).
    pub exit_code: i32,
    /// Standard output collected as a string.
    pub stdout: String,
    /// Standard error collected as a string.
    pub stderr: String,
}

// ── Container state ───────────────────────────────────────────────

/// Runtime status for a tracked container.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ContainerStatus {
    /// Container metadata created, but execution hasn't started yet.
    Created,
    /// Container is currently running.
    Running,
    /// Container exited with an exit code.
    Stopped {
        /// Exit code from the container command.
        exit_code: i32,
    },
}

/// Container metadata record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerInfo {
    /// Container identifier.
    pub id: String,
    /// Original image reference used for creation.
    pub image: String,
    /// Resolved image digest identifier.
    pub image_id: String,
    /// Container lifecycle status.
    pub status: ContainerStatus,
    /// Unix epoch seconds when metadata was created.
    pub created_unix_secs: u64,
    /// Unix epoch seconds when the container was started, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_unix_secs: Option<u64>,
    /// Unix epoch seconds when the container stopped, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped_unix_secs: Option<u64>,
    /// Assembled rootfs path for this container, when known.
    pub rootfs_path: Option<PathBuf>,
    /// Host process ID currently managing this container, if running.
    pub host_pid: Option<u32>,
}

impl ContainerInfo {
    /// Verify the lifecycle timestamps are internally consistent for this status.
    pub fn ensure_lifecycle_consistency(&self) -> Result<(), ContractInvariantError> {
        match &self.status {
            ContainerStatus::Created => {
                if self.started_unix_secs.is_some() {
                    return Err(
                        self.lifecycle_error("created containers must not report a start time")
                    );
                }
                if self.stopped_unix_secs.is_some() {
                    return Err(
                        self.lifecycle_error("created containers must not report a stop time")
                    );
                }
            }
            ContainerStatus::Running => {
                let started = match self.started_unix_secs {
                    Some(val) => val,
                    None => {
                        return Err(
                            self.lifecycle_error("running containers must record a start time")
                        );
                    }
                };
                if self.stopped_unix_secs.is_some() {
                    return Err(
                        self.lifecycle_error("running containers must not report a stop time")
                    );
                }
                if started < self.created_unix_secs {
                    return Err(self.lifecycle_error("start time cannot precede create time"));
                }
            }
            ContainerStatus::Stopped { .. } => {
                let started = match self.started_unix_secs {
                    Some(val) => val,
                    None => {
                        return Err(
                            self.lifecycle_error("stopped containers must record a start time")
                        );
                    }
                };
                let stopped = match self.stopped_unix_secs {
                    Some(val) => val,
                    None => {
                        return Err(
                            self.lifecycle_error("stopped containers must record a stop time")
                        );
                    }
                };
                if started > stopped {
                    return Err(self.lifecycle_error("stop time cannot precede start time"));
                }
                if started < self.created_unix_secs {
                    return Err(self.lifecycle_error("start time cannot precede create time"));
                }
            }
        }

        Ok(())
    }

    fn lifecycle_error(&self, details: &str) -> ContractInvariantError {
        ContractInvariantError::LifecycleInconsistency {
            container_id: self.id.clone(),
            details: details.to_string(),
        }
    }
}

/// Contract invariants that must hold consistently for runtime data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractInvariantError {
    /// Container lifecycle timestamps are inconsistent with the reported status.
    LifecycleInconsistency {
        container_id: String,
        details: String,
    },
    /// Shared VM phase transitions violated the allowed state machine.
    SharedVmPhaseTransition {
        from: SharedVmPhase,
        to: SharedVmPhase,
    },
    /// Sandbox state transition was invalid.
    SandboxStateTransition {
        sandbox_id: String,
        from: SandboxState,
        to: SandboxState,
    },
    /// Lease state transition was invalid.
    LeaseStateTransition {
        lease_id: String,
        from: LeaseState,
        to: LeaseState,
    },
    /// New leases can only be created when the sandbox is ready.
    LeaseRequiresReadySandbox {
        sandbox_id: String,
        state: SandboxState,
    },
    /// New work can only be submitted on active leases.
    WorkRequiresActiveLease {
        lease_id: String,
        state: LeaseState,
        operation: String,
    },
    /// Container state transition was invalid.
    ContainerStateTransition {
        container_id: String,
        from: ContainerState,
        to: ContainerState,
    },
    /// Exec operations require a running container.
    ExecRequiresRunningContainer {
        container_id: String,
        state: ContainerState,
    },
    /// Build state transition was invalid.
    BuildStateTransition {
        build_id: String,
        from: BuildState,
        to: BuildState,
    },
    /// Build record fields are inconsistent with the reported state.
    BuildLifecycleInconsistency { build_id: String, details: String },
    /// Execution state transition was invalid.
    ExecutionStateTransition {
        execution_id: String,
        from: ExecutionState,
        to: ExecutionState,
    },
    /// Execution record fields are inconsistent with the reported state.
    ExecutionLifecycleInconsistency {
        execution_id: String,
        details: String,
    },
    /// Checkpoint state transition was invalid.
    CheckpointStateTransition {
        checkpoint_id: String,
        from: CheckpointState,
        to: CheckpointState,
    },
    /// Checkpoint identifier already exists in lineage metadata.
    CheckpointAlreadyExists { checkpoint_id: String },
    /// Checkpoint parent is missing from lineage metadata.
    CheckpointParentNotFound {
        checkpoint_id: String,
        parent_checkpoint_id: String,
    },
    /// Image digest invariants were violated.
    ImageDigestInvariant { image_ref: String, details: String },
    /// Receipt event ranges must be ordered.
    ReceiptEventRangeInvalid {
        receipt_id: String,
        start_event_id: u64,
        end_event_id: u64,
    },
}

impl fmt::Display for ContractInvariantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContractInvariantError::LifecycleInconsistency {
                container_id,
                details,
            } => write!(
                f,
                "Lifecycle invariant violated for container {}: {}",
                container_id, details
            ),
            ContractInvariantError::SharedVmPhaseTransition { from, to } => write!(
                f,
                "Invalid shared VM phase transition from {:?} to {:?}",
                from, to
            ),
            ContractInvariantError::SandboxStateTransition {
                sandbox_id,
                from,
                to,
            } => write!(
                f,
                "Invalid sandbox state transition for {} from {:?} to {:?}",
                sandbox_id, from, to
            ),
            ContractInvariantError::LeaseStateTransition { lease_id, from, to } => write!(
                f,
                "Invalid lease state transition for {} from {:?} to {:?}",
                lease_id, from, to
            ),
            ContractInvariantError::LeaseRequiresReadySandbox { sandbox_id, state } => write!(
                f,
                "Sandbox {} must be ready to open a lease (state: {:?})",
                sandbox_id, state
            ),
            ContractInvariantError::WorkRequiresActiveLease {
                lease_id,
                state,
                operation,
            } => write!(
                f,
                "Lease {} must be active for {} (state: {:?})",
                lease_id, operation, state
            ),
            ContractInvariantError::ContainerStateTransition {
                container_id,
                from,
                to,
            } => write!(
                f,
                "Invalid container state transition for {} from {:?} to {:?}",
                container_id, from, to
            ),
            ContractInvariantError::ExecRequiresRunningContainer {
                container_id,
                state,
            } => write!(
                f,
                "Container {} must be running for exec (state: {:?})",
                container_id, state
            ),
            ContractInvariantError::BuildStateTransition { build_id, from, to } => write!(
                f,
                "Invalid build state transition for {} from {:?} to {:?}",
                build_id, from, to
            ),
            ContractInvariantError::BuildLifecycleInconsistency { build_id, details } => write!(
                f,
                "Build lifecycle invariant violated for {}: {}",
                build_id, details
            ),
            ContractInvariantError::ExecutionStateTransition {
                execution_id,
                from,
                to,
            } => write!(
                f,
                "Invalid execution state transition for {} from {:?} to {:?}",
                execution_id, from, to
            ),
            ContractInvariantError::ExecutionLifecycleInconsistency {
                execution_id,
                details,
            } => write!(
                f,
                "Execution lifecycle invariant violated for {}: {}",
                execution_id, details
            ),
            ContractInvariantError::CheckpointStateTransition {
                checkpoint_id,
                from,
                to,
            } => write!(
                f,
                "Invalid checkpoint state transition for {} from {:?} to {:?}",
                checkpoint_id, from, to
            ),
            ContractInvariantError::CheckpointAlreadyExists { checkpoint_id } => write!(
                f,
                "Checkpoint {} already exists in lineage metadata",
                checkpoint_id
            ),
            ContractInvariantError::CheckpointParentNotFound {
                checkpoint_id,
                parent_checkpoint_id,
            } => write!(
                f,
                "Checkpoint {} references missing parent {}",
                checkpoint_id, parent_checkpoint_id
            ),
            ContractInvariantError::ImageDigestInvariant { image_ref, details } => write!(
                f,
                "Image digest invariant violated for {}: {}",
                image_ref, details
            ),
            ContractInvariantError::ReceiptEventRangeInvalid {
                receipt_id,
                start_event_id,
                end_event_id,
            } => write!(
                f,
                "Receipt {} has invalid event range [{}..={}]",
                receipt_id, start_event_id, end_event_id
            ),
        }
    }
}

impl std::error::Error for ContractInvariantError {}

/// Runtime phases for a shared stack VM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedVmPhase {
    /// No shared VM is currently booted.
    Shutdown,
    /// A shared VM is in the process of booting.
    Booting,
    /// A shared VM has booted and is available for containers.
    Ready,
    /// The shared VM is in the process of shutting down.
    ShuttingDown,
}

impl SharedVmPhase {
    fn can_transition_to(self, next: SharedVmPhase) -> bool {
        matches!(
            (self, next),
            (SharedVmPhase::Shutdown, SharedVmPhase::Booting)
                | (SharedVmPhase::Booting, SharedVmPhase::Ready)
                | (SharedVmPhase::Ready, SharedVmPhase::ShuttingDown)
                | (SharedVmPhase::ShuttingDown, SharedVmPhase::Shutdown)
        )
    }
}

/// Tracks shared VM phases and validates transitions.
#[derive(Debug, Clone)]
pub struct SharedVmPhaseTracker {
    phase: SharedVmPhase,
}

impl Default for SharedVmPhaseTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedVmPhaseTracker {
    /// Start tracking from the shutdown phase.
    pub fn new() -> Self {
        Self {
            phase: SharedVmPhase::Shutdown,
        }
    }

    /// Current known shared VM phase.
    pub fn phase(&self) -> SharedVmPhase {
        self.phase
    }

    /// Attempt to transition to a new phase, returning an error if invalid.
    pub fn transition_to(&mut self, next: SharedVmPhase) -> Result<(), ContractInvariantError> {
        if self.phase == next {
            return Ok(());
        }

        if !self.phase.can_transition_to(next) {
            return Err(ContractInvariantError::SharedVmPhaseTransition {
                from: self.phase,
                to: next,
            });
        }

        self.phase = next;
        Ok(())
    }
}

/// Backend capability flags used by callers to branch behavior deterministically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RuntimeCapabilities {
    /// Supports fs-focused quick checkpoints.
    pub fs_quick_checkpoint: bool,
    /// Supports full VM checkpoints (RAM/CPU/device state).
    pub vm_full_checkpoint: bool,
    /// Supports checkpoint fork into a new sandbox lineage.
    pub checkpoint_fork: bool,
    /// Supports Docker command compatibility adapter.
    pub docker_compat: bool,
    /// Supports Compose adapter semantics.
    pub compose_adapter: bool,
    /// Supports build cache export/import semantics.
    pub build_cache_export: bool,
    /// Supports GPU passthrough for workloads.
    pub gpu_passthrough: bool,
    /// Supports runtime live-resize operations.
    pub live_resize: bool,
    /// Supports shared sandbox/VM orchestration for multi-service stacks.
    pub shared_vm: bool,
    /// Supports stack network setup/teardown APIs.
    pub stack_networking: bool,
    /// Supports runtime log retrieval for created containers.
    pub container_logs: bool,
}

impl RuntimeCapabilities {
    /// Baseline capabilities used by current stack-enabled backends.
    pub const fn stack_baseline() -> Self {
        Self {
            fs_quick_checkpoint: false,
            vm_full_checkpoint: false,
            checkpoint_fork: false,
            docker_compat: false,
            compose_adapter: true,
            build_cache_export: false,
            gpu_passthrough: false,
            live_resize: false,
            shared_vm: true,
            stack_networking: true,
            container_logs: true,
        }
    }
}

// ── Isolation levels ─────────────────────────────────────────────

/// Isolation level supported by a runtime backend.
///
/// Backends expose the strongest isolation they provide. Callers can
/// query a backend's isolation level to make scheduling, security, and
/// resource decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    /// Full hardware-virtualised isolation (e.g., Virtualization.framework VM).
    Full,
    /// OCI-runtime container isolation (namespaces + cgroups + seccomp).
    Container,
    /// Lightweight namespace-only isolation (no cgroup/seccomp enforcement).
    ///
    /// Provides filesystem, PID, network, and user separation without the
    /// overhead of a full OCI runtime or VM. Suitable for trusted workloads
    /// that need process separation but not a full security boundary.
    Namespace,
    /// No isolation — direct host execution.
    None,
}

impl IsolationLevel {
    /// Human-readable label for diagnostics and reporting.
    pub fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Container => "container",
            Self::Namespace => "namespace",
            Self::None => "none",
        }
    }

    /// Whether this level provides at least namespace-level separation.
    pub fn has_namespace_isolation(self) -> bool {
        matches!(self, Self::Full | Self::Container | Self::Namespace)
    }

    /// Whether this level provides cgroup and seccomp enforcement.
    pub fn has_container_isolation(self) -> bool {
        matches!(self, Self::Full | Self::Container)
    }

    /// Whether this level provides full VM-based isolation.
    pub fn has_vm_isolation(self) -> bool {
        matches!(self, Self::Full)
    }
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::Full
    }
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Configuration for Linux namespace isolation.
///
/// Controls which namespaces are created for a lightweight
/// namespace-only isolation mode. Each field enables or disables the
/// corresponding `clone(2)` / `unshare(2)` namespace flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceConfig {
    /// Create a new user namespace (`CLONE_NEWUSER`).
    pub user: bool,
    /// Create a new network namespace (`CLONE_NEWNET`).
    pub net: bool,
    /// Create a new PID namespace (`CLONE_NEWPID`).
    pub pid: bool,
    /// Create a new mount namespace (`CLONE_NEWNS`).
    pub mnt: bool,
    /// Create a new IPC namespace (`CLONE_NEWIPC`).
    pub ipc: bool,
    /// Create a new UTS namespace (`CLONE_NEWUTS`).
    pub uts: bool,
}

impl NamespaceConfig {
    /// All namespaces enabled.
    pub const ALL: Self = Self {
        user: true,
        net: true,
        pid: true,
        mnt: true,
        ipc: true,
        uts: true,
    };

    /// No namespaces enabled (host execution).
    pub const NONE: Self = Self {
        user: false,
        net: false,
        pid: false,
        mnt: false,
        ipc: false,
        uts: false,
    };

    /// Count of enabled namespaces.
    pub fn enabled_count(self) -> usize {
        [self.user, self.net, self.pid, self.mnt, self.ipc, self.uts]
            .iter()
            .filter(|&&v| v)
            .count()
    }
}

/// Sensible default namespace configuration.
///
/// Enables PID, mount, IPC, and UTS namespaces for basic process
/// separation. Network and user namespaces are disabled by default
/// because they require additional setup (veth pairs, UID mapping).
pub fn default_namespace_config() -> NamespaceConfig {
    NamespaceConfig {
        user: false,
        net: false,
        pid: true,
        mnt: true,
        ipc: true,
        uts: true,
    }
}

impl Default for NamespaceConfig {
    fn default() -> Self {
        default_namespace_config()
    }
}

// ── Runtime V2 canonical domain model ────────────────────────────

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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SandboxSpec {
    /// Optional CPU core limit.
    pub cpus: Option<u8>,
    /// Optional memory limit in MB.
    pub memory_mb: Option<u64>,
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

/// Immutable image reference resolved by digest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Image {
    /// User-provided image reference (tag or digest).
    pub image_ref: String,
    /// Resolved immutable image digest.
    pub resolved_digest: String,
    /// Target platform identifier.
    pub platform: String,
    /// Source registry name/host.
    pub source_registry: String,
    /// Pull completion timestamp in unix epoch seconds.
    pub pulled_at: u64,
}

impl Image {
    /// Validate digest immutability expectations for runtime execution.
    pub fn ensure_digest_immutable(&self) -> Result<(), ContractInvariantError> {
        if !self.resolved_digest.starts_with("sha256:") {
            return Err(ContractInvariantError::ImageDigestInvariant {
                image_ref: self.image_ref.clone(),
                details: "resolved digest must use sha256:<hex> form".to_string(),
            });
        }

        if self.resolved_digest.len() <= "sha256:".len() {
            return Err(ContractInvariantError::ImageDigestInvariant {
                image_ref: self.image_ref.clone(),
                details: "resolved digest must include digest bytes".to_string(),
            });
        }

        Ok(())
    }
}

/// Build request details.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BuildSpec {
    /// Build context URI or path.
    pub context: String,
    /// Optional Dockerfile path in the context.
    pub dockerfile: Option<String>,
    /// Build arguments supplied to the builder.
    pub args: BTreeMap<String, String>,
}

/// Build lifecycle states.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    /// Build accepted but not yet started.
    Queued,
    /// Build is currently running.
    Running,
    /// Build completed successfully.
    Succeeded,
    /// Build completed with failure.
    Failed,
    /// Build canceled before completion.
    Canceled,
}

impl BuildState {
    /// Whether this state is terminal.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }

    fn can_transition_to(self, next: BuildState) -> bool {
        matches!(
            (self, next),
            (BuildState::Queued, BuildState::Running)
                | (BuildState::Queued, BuildState::Canceled)
                | (BuildState::Queued, BuildState::Failed)
                | (BuildState::Running, BuildState::Succeeded)
                | (BuildState::Running, BuildState::Failed)
                | (BuildState::Running, BuildState::Canceled)
        )
    }
}

/// Asynchronous image build operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Build {
    /// Build identifier.
    pub build_id: String,
    /// Sandbox where build executes.
    pub sandbox_id: String,
    /// Build request specification.
    pub build_spec: BuildSpec,
    /// Current build state.
    pub state: BuildState,
    /// Resulting image digest, available on success.
    pub result_digest: Option<String>,
    /// Build start timestamp in unix epoch seconds.
    pub started_at: u64,
    /// Build end timestamp when terminal.
    pub ended_at: Option<u64>,
}

impl Build {
    /// Validate core build invariants against current state.
    pub fn ensure_lifecycle_consistency(&self) -> Result<(), ContractInvariantError> {
        if let Some(ended_at) = self.ended_at {
            if ended_at < self.started_at {
                return Err(ContractInvariantError::BuildLifecycleInconsistency {
                    build_id: self.build_id.clone(),
                    details: "end time cannot precede start time".to_string(),
                });
            }
        }

        match self.state {
            BuildState::Succeeded => {
                if self.result_digest.is_none() {
                    return Err(ContractInvariantError::BuildLifecycleInconsistency {
                        build_id: self.build_id.clone(),
                        details: "successful builds must include a result digest".to_string(),
                    });
                }
                if self.ended_at.is_none() {
                    return Err(ContractInvariantError::BuildLifecycleInconsistency {
                        build_id: self.build_id.clone(),
                        details: "successful builds must include an end time".to_string(),
                    });
                }
            }
            BuildState::Failed | BuildState::Canceled => {
                if self.ended_at.is_none() {
                    return Err(ContractInvariantError::BuildLifecycleInconsistency {
                        build_id: self.build_id.clone(),
                        details: "terminal builds must include an end time".to_string(),
                    });
                }
            }
            BuildState::Queued | BuildState::Running => {
                if self.ended_at.is_some() {
                    return Err(ContractInvariantError::BuildLifecycleInconsistency {
                        build_id: self.build_id.clone(),
                        details: "non-terminal builds cannot include an end time".to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Transition to a new build state if allowed.
    pub fn transition_to(&mut self, next: BuildState) -> Result<(), ContractInvariantError> {
        if self.state == next {
            return Ok(());
        }

        if !self.state.can_transition_to(next) {
            return Err(ContractInvariantError::BuildStateTransition {
                build_id: self.build_id.clone(),
                from: self.state,
                to: next,
            });
        }

        self.state = next;
        Ok(())
    }
}

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

/// Event stream scope.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventScope {
    /// Sandbox-scoped event.
    Sandbox,
    /// Lease-scoped event.
    Lease,
    /// Build-scoped event.
    Build,
    /// Container-scoped event.
    Container,
    /// Execution-scoped event.
    Execution,
    /// Checkpoint-scoped event.
    Checkpoint,
    /// System-scoped event.
    System,
}

/// Append-only runtime operation event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    /// Monotonic event identifier in stream.
    pub event_id: u64,
    /// Event timestamp in unix epoch seconds.
    pub ts: u64,
    /// Event scope class.
    pub scope: EventScope,
    /// Scoped entity identifier.
    pub scope_id: String,
    /// Event type identifier.
    pub event_type: String,
    /// Structured payload fields.
    pub payload: BTreeMap<String, String>,
    /// Optional trace identifier.
    pub trace_id: Option<String>,
}

/// Receipt result classification.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptResultClassification {
    /// Request completed successfully.
    Success,
    /// Request failed validation.
    ValidationError,
    /// Request failed due to policy.
    PolicyDenied,
    /// Request failed due to state conflict.
    StateConflict,
    /// Request failed due to timeout.
    Timeout,
    /// Request failed with internal runtime error.
    InternalError,
}

/// Inclusive event range linked to a receipt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventRange {
    /// First event ID included.
    pub start_event_id: u64,
    /// Last event ID included.
    pub end_event_id: u64,
}

/// Immutable operation summary for audit/replay metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    /// Receipt identifier.
    pub receipt_id: String,
    /// Scope class for the operation.
    pub scope: EventScope,
    /// Scoped entity identifier.
    pub scope_id: String,
    /// Hash of request payload/input.
    pub request_hash: String,
    /// Optional policy hash evaluated during the request.
    pub policy_hash: Option<String>,
    /// Result classification.
    pub result_classification: ReceiptResultClassification,
    /// Artifact references emitted by the operation.
    pub artifacts: Vec<String>,
    /// Structured resource usage summary.
    pub resource_summary: BTreeMap<String, String>,
    /// Event range associated with this operation.
    pub event_range: EventRange,
}

impl Receipt {
    /// Validate that receipt event range ordering is correct.
    pub fn ensure_event_range_ordered(&self) -> Result<(), ContractInvariantError> {
        if self.event_range.start_event_id > self.event_range.end_event_id {
            return Err(ContractInvariantError::ReceiptEventRangeInvalid {
                receipt_id: self.receipt_id.clone(),
                start_event_id: self.event_range.start_event_id,
                end_event_id: self.event_range.end_event_id,
            });
        }

        Ok(())
    }
}

/// Backend-declared runtime capability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// Supports full VM checkpoint captures.
    VmFullCheckpoint,
    /// Supports checkpoint fork semantics.
    CheckpointFork,
    /// Supports Docker compatibility adapter.
    DockerCompat,
    /// Supports Compose adapter semantics.
    ComposeAdapter,
    /// Supports build cache export/import.
    BuildCacheExport,
    /// Supports GPU passthrough.
    GpuPassthrough,
    /// Supports fs-focused quick checkpoints.
    FsQuickCheckpoint,
    /// Supports shared multi-service VM mode.
    SharedVm,
    /// Supports stack network setup/teardown APIs.
    StackNetworking,
    /// Supports runtime log retrieval.
    ContainerLogs,
    /// Supports live resize operations.
    LiveResize,
}

impl RuntimeCapabilities {
    /// Convert bool flags to a stable capability list.
    pub fn to_capability_list(self) -> Vec<Capability> {
        let mut list = Vec::new();
        if self.vm_full_checkpoint {
            list.push(Capability::VmFullCheckpoint);
        }
        if self.checkpoint_fork {
            list.push(Capability::CheckpointFork);
        }
        if self.docker_compat {
            list.push(Capability::DockerCompat);
        }
        if self.compose_adapter {
            list.push(Capability::ComposeAdapter);
        }
        if self.build_cache_export {
            list.push(Capability::BuildCacheExport);
        }
        if self.gpu_passthrough {
            list.push(Capability::GpuPassthrough);
        }
        if self.fs_quick_checkpoint {
            list.push(Capability::FsQuickCheckpoint);
        }
        if self.shared_vm {
            list.push(Capability::SharedVm);
        }
        if self.stack_networking {
            list.push(Capability::StackNetworking);
        }
        if self.container_logs {
            list.push(Capability::ContainerLogs);
        }
        if self.live_resize {
            list.push(Capability::LiveResize);
        }
        list
    }
}

/// Canonical Runtime V2 operation surface.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeOperation {
    /// Create a new sandbox.
    CreateSandbox,
    /// Get sandbox details by identifier.
    GetSandbox,
    /// Terminate an existing sandbox.
    TerminateSandbox,
    /// Open a lease for sandbox operations.
    OpenLease,
    /// Heartbeat an existing lease.
    HeartbeatLease,
    /// Close an existing lease.
    CloseLease,
    /// Resolve an image reference to immutable digest.
    ResolveImage,
    /// Pull an image reference.
    PullImage,
    /// Start asynchronous build operation.
    StartBuild,
    /// Get build status/details.
    GetBuild,
    /// Stream build events.
    StreamBuildEvents,
    /// Cancel a running build.
    CancelBuild,
    /// Create a container.
    CreateContainer,
    /// Start a created container.
    StartContainer,
    /// Stop a running container.
    StopContainer,
    /// Remove a container.
    RemoveContainer,
    /// Retrieve container logs.
    GetContainerLogs,
    /// Execute command in container.
    ExecContainer,
    /// Write stdin to running exec.
    WriteExecStdin,
    /// Signal running exec.
    SignalExec,
    /// Resize PTY for running exec.
    ResizeExecPty,
    /// Cancel running exec.
    CancelExec,
    /// Create checkpoint.
    CreateCheckpoint,
    /// Restore checkpoint.
    RestoreCheckpoint,
    /// Fork checkpoint into new lineage.
    ForkCheckpoint,
    /// Create new volume.
    CreateVolume,
    /// Attach volume to workload.
    AttachVolume,
    /// Detach volume from workload.
    DetachVolume,
    /// Create isolated network domain.
    CreateNetworkDomain,
    /// Publish ingress port.
    PublishPort,
    /// Connect container to network domain.
    ConnectContainer,
    /// List events from a cursor.
    ListEvents,
    /// Get immutable operation receipt.
    GetReceipt,
    /// Query backend capabilities.
    GetCapabilities,
}

impl RuntimeOperation {
    /// All required Runtime V2 operations.
    pub const ALL: [RuntimeOperation; 34] = [
        RuntimeOperation::CreateSandbox,
        RuntimeOperation::GetSandbox,
        RuntimeOperation::TerminateSandbox,
        RuntimeOperation::OpenLease,
        RuntimeOperation::HeartbeatLease,
        RuntimeOperation::CloseLease,
        RuntimeOperation::ResolveImage,
        RuntimeOperation::PullImage,
        RuntimeOperation::StartBuild,
        RuntimeOperation::GetBuild,
        RuntimeOperation::StreamBuildEvents,
        RuntimeOperation::CancelBuild,
        RuntimeOperation::CreateContainer,
        RuntimeOperation::StartContainer,
        RuntimeOperation::StopContainer,
        RuntimeOperation::RemoveContainer,
        RuntimeOperation::GetContainerLogs,
        RuntimeOperation::ExecContainer,
        RuntimeOperation::WriteExecStdin,
        RuntimeOperation::SignalExec,
        RuntimeOperation::ResizeExecPty,
        RuntimeOperation::CancelExec,
        RuntimeOperation::CreateCheckpoint,
        RuntimeOperation::RestoreCheckpoint,
        RuntimeOperation::ForkCheckpoint,
        RuntimeOperation::CreateVolume,
        RuntimeOperation::AttachVolume,
        RuntimeOperation::DetachVolume,
        RuntimeOperation::CreateNetworkDomain,
        RuntimeOperation::PublishPort,
        RuntimeOperation::ConnectContainer,
        RuntimeOperation::ListEvents,
        RuntimeOperation::GetReceipt,
        RuntimeOperation::GetCapabilities,
    ];

    /// Whether this operation requires an idempotency key for retries.
    pub const fn requires_idempotency_key(self) -> bool {
        matches!(
            self,
            RuntimeOperation::CreateSandbox
                | RuntimeOperation::OpenLease
                | RuntimeOperation::PullImage
                | RuntimeOperation::StartBuild
                | RuntimeOperation::CreateContainer
                | RuntimeOperation::ExecContainer
                | RuntimeOperation::CreateCheckpoint
                | RuntimeOperation::ForkCheckpoint
        )
    }

    /// Canonical idempotency key prefix for this operation, if required.
    pub const fn idempotency_key_prefix(self) -> Option<&'static str> {
        match self {
            RuntimeOperation::CreateSandbox => Some("create_sandbox"),
            RuntimeOperation::OpenLease => Some("open_lease"),
            RuntimeOperation::PullImage => Some("pull_image"),
            RuntimeOperation::StartBuild => Some("start_build"),
            RuntimeOperation::CreateContainer => Some("create_container"),
            RuntimeOperation::ExecContainer => Some("exec_container"),
            RuntimeOperation::CreateCheckpoint => Some("create_checkpoint"),
            RuntimeOperation::ForkCheckpoint => Some("fork_checkpoint"),
            _ => None,
        }
    }

    /// Canonical operation name.
    pub const fn as_str(self) -> &'static str {
        match self {
            RuntimeOperation::CreateSandbox => "create_sandbox",
            RuntimeOperation::GetSandbox => "get_sandbox",
            RuntimeOperation::TerminateSandbox => "terminate_sandbox",
            RuntimeOperation::OpenLease => "open_lease",
            RuntimeOperation::HeartbeatLease => "heartbeat_lease",
            RuntimeOperation::CloseLease => "close_lease",
            RuntimeOperation::ResolveImage => "resolve_image",
            RuntimeOperation::PullImage => "pull_image",
            RuntimeOperation::StartBuild => "start_build",
            RuntimeOperation::GetBuild => "get_build",
            RuntimeOperation::StreamBuildEvents => "stream_build_events",
            RuntimeOperation::CancelBuild => "cancel_build",
            RuntimeOperation::CreateContainer => "create_container",
            RuntimeOperation::StartContainer => "start_container",
            RuntimeOperation::StopContainer => "stop_container",
            RuntimeOperation::RemoveContainer => "remove_container",
            RuntimeOperation::GetContainerLogs => "get_container_logs",
            RuntimeOperation::ExecContainer => "exec_container",
            RuntimeOperation::WriteExecStdin => "write_exec_stdin",
            RuntimeOperation::SignalExec => "signal_exec",
            RuntimeOperation::ResizeExecPty => "resize_exec_pty",
            RuntimeOperation::CancelExec => "cancel_exec",
            RuntimeOperation::CreateCheckpoint => "create_checkpoint",
            RuntimeOperation::RestoreCheckpoint => "restore_checkpoint",
            RuntimeOperation::ForkCheckpoint => "fork_checkpoint",
            RuntimeOperation::CreateVolume => "create_volume",
            RuntimeOperation::AttachVolume => "attach_volume",
            RuntimeOperation::DetachVolume => "detach_volume",
            RuntimeOperation::CreateNetworkDomain => "create_network_domain",
            RuntimeOperation::PublishPort => "publish_port",
            RuntimeOperation::ConnectContainer => "connect_container",
            RuntimeOperation::ListEvents => "list_events",
            RuntimeOperation::GetReceipt => "get_receipt",
            RuntimeOperation::GetCapabilities => "get_capabilities",
        }
    }
}

// ── Image types ───────────────────────────────────────────────────

/// Cached image reference and manifest identifier pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageInfo {
    /// Human-readable image reference, for example `ubuntu:latest`.
    pub reference: String,
    /// Image identifier used by stored manifests/configs (digest form).
    pub image_id: String,
}

/// Summary of a local image prune pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneResult {
    /// Number of stale reference mappings that were removed.
    pub removed_refs: usize,
    /// Number of manifest JSON files removed.
    pub removed_manifests: usize,
    /// Number of config JSON files removed.
    pub removed_configs: usize,
    /// Number of unpacked layer directories removed.
    pub removed_layer_dirs: usize,
}

// ── Network types ─────────────────────────────────────────────────

/// Per-service network configuration for stack networking.
///
/// Each entry represents one service on one network. A service that belongs
/// to multiple custom networks will have multiple `NetworkServiceConfig`
/// entries (one per network), each with a different `network_name` and
/// subnet-specific `addr`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkServiceConfig {
    /// Service name.
    pub name: String,
    /// IP address assigned to this service (CIDR, e.g., `"172.20.0.2/24"`).
    pub addr: String,
    /// Logical network this entry belongs to (e.g., `"default"`, `"frontend"`).
    pub network_name: String,
}

/// Aggregate resource hints for sizing a shared stack VM.
///
/// When multiple services define CPU/memory limits, the stack executor
/// computes an aggregate and passes it to the runtime backend so the
/// shared VM gets enough CPU cores and memory.
#[derive(Debug, Clone, Default)]
pub struct StackResourceHint {
    /// Suggested CPU cores for the VM (max of all service limits, ceiling).
    pub cpus: Option<u8>,
    /// Suggested memory in MB for the VM (sum of all service limits).
    pub memory_mb: Option<u64>,
    /// Host directories to share as VirtioFS mounts inside the VM.
    ///
    /// Each entry is `(tag, host_path, read_only)`. The tag is used as the
    /// VirtioFS mount tag and the init script mounts it at `/mnt/{tag}`.
    /// Named volumes and bind mounts from all services are collected here
    /// so the shared VM can set them up at boot time (VirtioFS shares are
    /// static and must be configured before the VM starts).
    pub volume_mounts: Vec<StackVolumeMount>,
    /// Optional path to a disk image to attach as a VirtioBlock device.
    ///
    /// Used for persistent named volumes: the image contains an ext4
    /// filesystem mounted at `/run/vz-oci/volumes` inside the guest VM.
    pub disk_image_path: Option<PathBuf>,
}

/// A host directory to expose inside the shared VM via VirtioFS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackVolumeMount {
    /// VirtioFS mount tag (e.g., `"vz-mount-0"`).
    pub tag: String,
    /// Absolute path on the host.
    pub host_path: std::path::PathBuf,
    /// Whether the mount is read-only.
    pub read_only: bool,
}

/// Container log output.
#[derive(Debug, Clone, Default)]
pub struct ContainerLogs {
    /// Combined stdout/stderr output.
    pub output: String,
}
