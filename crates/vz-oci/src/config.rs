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
            require_exact_agent_version: true,
            default_cpus: 2,
            default_memory_mb: 512,
            default_network_enabled: true,
            agent_ready_timeout: Duration::from_secs(8),
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
