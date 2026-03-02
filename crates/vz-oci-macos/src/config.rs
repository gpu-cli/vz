use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

pub use vz_image::Auth;

// Re-export shared types from the runtime contract.
pub use vz_runtime_contract::{MountAccess, MountSpec, MountType, PortMapping, PortProtocol};

/// Runtime backend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeBackend {
    /// Linux OCI image backend (`vz-linux`).
    Linux,
    /// macOS sandbox backend (`vz-sandbox`).
    MacOS,
}

/// How the container command is executed inside the VM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionMode {
    /// Execute host-chosen command directly via guest agent `Exec` request.
    #[default]
    GuestExec,
    /// Future path: run OCI workload lifecycle inside the guest using an OCI runtime.
    OciRuntime,
}

/// Supported OCI runtime implementation for guest lifecycle operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OciRuntimeKind {
    /// Use `youki` inside the guest.
    #[default]
    Youki,
}

impl OciRuntimeKind {
    /// Expected runtime binary filename for this runtime kind.
    pub fn binary_name(self) -> &'static str {
        match self {
            Self::Youki => "youki",
        }
    }
}

/// Top-level runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// Base data directory for runtime metadata and caches.
    pub data_dir: PathBuf,
    /// Registry authentication strategy for image pulls.
    pub auth: Auth,
    /// Optional install/cache directory for kernel artifacts.
    pub linux_install_dir: Option<PathBuf>,
    /// Optional predownloaded bundle directory for kernel artifacts.
    pub linux_bundle_dir: Option<PathBuf>,
    /// OCI runtime implementation used for guest lifecycle operations.
    pub guest_oci_runtime: OciRuntimeKind,
    /// Optional host path override for the guest OCI runtime binary (`youki`).
    ///
    /// When unset, runtime provisioning uses the pinned artifact from
    /// `vz-linux::ensure_kernel_with_options`.
    pub guest_oci_runtime_path: Option<PathBuf>,
    /// Optional guest state directory used for OCI runtime state and bundles.
    ///
    /// When unset, the runtime defaults to `/run/vz-oci`.
    pub guest_state_dir: Option<PathBuf>,
    /// Require exact guest-agent version match in kernel artifact metadata.
    pub require_exact_agent_version: bool,
    /// Default CPU cores per Linux container VM.
    pub default_cpus: u8,
    /// Default memory per Linux container VM in MB.
    pub default_memory_mb: u64,
    /// Enable default networking for Linux container VMs.
    pub default_network_enabled: bool,
    /// Timeout waiting for guest agent readiness.
    pub agent_ready_timeout: Duration,
    /// Timeout for primary container command execution.
    pub exec_timeout: Duration,
}

fn env_duration_secs(key: &str, default_secs: u64) -> Duration {
    let parsed = std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_secs);
    Duration::from_secs(parsed)
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("~/.vz/oci"),
            auth: Auth::default(),
            linux_install_dir: None,
            linux_bundle_dir: None,
            guest_oci_runtime: OciRuntimeKind::default(),
            guest_oci_runtime_path: None,
            guest_state_dir: None,
            require_exact_agent_version: true,
            default_cpus: 2,
            default_memory_mb: 512,
            default_network_enabled: true,
            // Cold boots on loaded hosts can exceed 8s; keep this conservative
            // and allow overrides via `VZ_AGENT_READY_TIMEOUT_SECS`.
            agent_ready_timeout: env_duration_secs("VZ_AGENT_READY_TIMEOUT_SECS", 20),
            exec_timeout: Duration::from_secs(30),
        }
    }
}

/// Per-run options for a Linux rootfs-backed container VM.
#[derive(Debug, Clone, Default)]
pub struct RunConfig {
    /// Command to execute inside the guest.
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
    /// Optional CPU override.
    pub cpus: Option<u8>,
    /// Optional memory override in MB.
    pub memory_mb: Option<u64>,
    /// Optional network enable override.
    pub network_enabled: Option<bool>,
    /// Optional serial log file path for this run.
    pub serial_log_file: Option<PathBuf>,
    /// Optional exec timeout override.
    pub timeout: Option<Duration>,
    /// How to execute the workload for this run.
    pub execution_mode: ExecutionMode,
    /// Optional explicit container identifier.
    ///
    /// When unset, the runtime generates a unique ID.
    pub container_id: Option<String>,
    /// Optional OCI init process command used for container create/start.
    ///
    /// If unset, the resolved run command is used for init.
    pub init_process: Option<Vec<String>>,
    /// Additional OCI runtime-spec annotations for this run.
    pub oci_annotations: Vec<(String, String)>,
    /// Extra `/etc/hosts` entries as `(hostname, ip)` pairs.
    ///
    /// When non-empty, the runtime generates an `/etc/hosts` file in the
    /// OCI bundle directory and bind-mounts it into the container. This
    /// enables inter-service hostname resolution without a DNS server.
    pub extra_hosts: Vec<(String, String)>,
    /// Path to an existing network namespace for the container to join.
    ///
    /// When set, the OCI bundle's `linux.namespaces` section includes a
    /// network namespace entry with this path (e.g., `/var/run/netns/svc-web`),
    /// causing the container to join the existing netns rather than creating
    /// a new one.
    pub network_namespace_path: Option<String>,
    /// CPU quota in microseconds per `cpu_period` for cgroup CPU throttling.
    ///
    /// For example, `cpus: 0.5` → quota=50000, period=100000.
    pub cpu_quota: Option<i64>,
    /// CPU CFS period in microseconds (default: 100000 = 100ms).
    pub cpu_period: Option<u64>,
    /// Redirect container stdout/stderr to log files for later retrieval.
    ///
    /// When `true`, the OCI process args are wrapped with shell redirection
    /// to capture stdout/stderr to `/var/log/vz-oci/{stdout,stderr}.log`.
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
    pub sysctls: HashMap<String, String>,
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
    pub mount_tag_offset: usize,
}

/// Options for executing a command in an already-running container.
#[derive(Debug, Clone, Default)]
pub struct ExecConfig {
    /// Optional daemon-side execution identity used for runtime
    /// interactive control session binding.
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
