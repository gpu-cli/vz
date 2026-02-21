use std::path::PathBuf;
use std::time::Duration;

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
    /// Protocol name used by the guest wire protocol.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

/// Host-to-container port mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortMapping {
    /// Host port to listen on.
    pub host: u16,
    /// Container guest port to forward to.
    pub container: u16,
    /// Forwarding protocol.
    pub protocol: PortProtocol,
}

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
#[derive(Debug, Clone)]
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
            agent_ready_timeout: Duration::from_secs(8),
            exec_timeout: Duration::from_secs(30),
        }
    }
}

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
}

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

/// Configuration for booting a shared VM that hosts multiple containers in a
/// stack.
///
/// Unlike [`RunConfig`] (which boots one VM per container), a stack VM is
/// created once and reused for all services in the stack. Individual
/// containers are then created inside it via
/// [`Runtime::create_container_in_stack`](crate::Runtime::create_container_in_stack).
#[derive(Debug, Clone)]
pub struct StackVmConfig {
    /// Number of virtual CPUs.
    pub cpus: u8,
    /// Memory allocation in megabytes.
    pub memory_mb: u64,
    /// Whether networking is enabled in the VM.
    pub network_enabled: bool,
    /// Host-to-guest port mappings (combined from all services).
    pub ports: Vec<PortMapping>,
    /// Optional serial log file path.
    pub serial_log_file: Option<PathBuf>,
    /// Path to the VM's root filesystem directory.
    ///
    /// For a stack VM this should be a base Linux rootfs that contains
    /// the guest agent and OCI runtime. Individual service rootfs
    /// directories are shared as additional VirtioFS mounts.
    pub rootfs_dir: PathBuf,
    /// Additional VirtioFS bind mounts to share with the guest
    /// (e.g., named volumes, project directories).
    pub extra_mounts: Vec<MountSpec>,
}

/// Registry authentication used when pulling OCI images.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Auth {
    /// Access the registry anonymously.
    #[default]
    Anonymous,
    /// Authenticate to the registry with username and password.
    Basic {
        /// Registry username.
        username: String,
        /// Registry password.
        password: String,
    },
    /// Load credentials from the local Docker credential configuration.
    DockerConfig,
}
