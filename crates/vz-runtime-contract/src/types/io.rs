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
    // ── Network ─────────────────────────────────────────────────
    /// Share the host (VM) network namespace instead of creating an isolated one.
    ///
    /// When true, the container does not get its own network namespace and
    /// uses the VM's NAT network directly. This gives the container internet
    /// access via the VM's DHCP-assigned IP.
    pub share_host_network: bool,
    // ── Shared VM mount support ──────────────────────────────────
    /// Offset added to VirtioFS mount tag indices in shared VM mode.
    ///
    /// In a shared VM, multiple containers share one set of VirtioFS
    /// shares. Each container's bind mounts are assigned a global index
    /// starting at this offset so tags don't collide between services
    /// (e.g., service A gets `vz-mount-0`, service B gets `vz-mount-2`).
    pub mount_tag_offset: usize,
    // ── Setup commands ──────────────────────────────────────────
    /// Shell commands to run once to establish the container environment.
    ///
    /// Setup commands install packages, create files, or perform other
    /// one-time initialization. The runtime commits the resulting
    /// filesystem state so subsequent executions start from the
    /// post-setup environment without re-running setup.
    ///
    /// Each entry is executed as a separate `sh -c` invocation inside
    /// the container, in order. If any setup command fails, the run
    /// fails without committing.
    pub setup_commands: Vec<String>,
}

// ── Exec configuration ────────────────────────────────────────────

/// Options for executing a command in an already-running container.
#[derive(Debug, Clone, Default)]
pub struct ExecConfig {
    /// Optional daemon-side execution identity used to bind backend
    /// control operations to an active exec session.
    pub execution_id: Option<String>,
    /// Command and arguments to execute.
    pub cmd: Vec<String>,
    /// Optional working directory inside the container.
    pub working_dir: Option<String>,
    /// Environment variables for the process.
    pub env: Vec<(String, String)>,
    /// Optional user to run as inside the container.
    pub user: Option<String>,
    /// Allocate an interactive PTY for the process.
    pub pty: bool,
    /// Optional initial terminal rows when PTY is enabled.
    pub term_rows: Option<u16>,
    /// Optional initial terminal cols when PTY is enabled.
    pub term_cols: Option<u16>,
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
