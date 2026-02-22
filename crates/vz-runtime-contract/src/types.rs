//! Backend-neutral runtime types shared across all container backends.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// ── Port mapping ──────────────────────────────────────────────────

/// Port mapping protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
}

/// Access mode for container mounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
