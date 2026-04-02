#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use thiserror::Error;
use tonic::Request;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;
use vz_runtime_contract::{RequestMetadata, RuntimeCapabilities};
use vz_runtime_proto::runtime_v2;
use vz_runtime_translate::{request_metadata_to_proto, runtime_capabilities_from_proto};

use crate::transport::{connect_channel, status_to_client_error};

mod build;
mod checkpoint;
mod container;
mod events;
mod execution;
mod files;
mod image;
mod linux_vm;
mod sandbox;
mod stack;
mod stream_completion;
mod transport;

#[cfg(test)]
mod tests;

/// Runtime daemon client result type.
pub type Result<T> = std::result::Result<T, DaemonClientError>;

/// Typed failure classes for runtime daemon client lifecycle and RPC operations.
#[derive(Debug, Error)]
pub enum DaemonClientError {
    #[error("daemon unavailable at {socket_path}: {reason}")]
    Unavailable {
        socket_path: PathBuf,
        reason: String,
    },
    #[error(
        "daemon startup timed out after {timeout_secs}s at {socket_path}; last_error={last_error}"
    )]
    StartupTimeout {
        socket_path: PathBuf,
        timeout_secs: u64,
        last_error: String,
    },
    #[error("daemon binary not found at {path}")]
    BinaryNotFound { path: PathBuf },
    #[error("failed to resolve current executable path: {source}")]
    ResolveCurrentExecutable {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to spawn daemon {path}: {source}")]
    SpawnFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("daemon version mismatch: daemon={daemon_version}, client={client_version}")]
    IncompatibleVersion {
        daemon_version: String,
        client_version: String,
    },
    #[error("daemon protocol mismatch: {reason}")]
    IncompatibleProtocol { reason: String },
    #[error("transport error: {0}")]
    Transport(#[from] Box<tonic::transport::Error>),
    #[error("grpc status error: {0}")]
    Grpc(#[from] Box<tonic::Status>),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Connection and startup policy for `DaemonClient`.
#[derive(Debug, Clone)]
pub struct DaemonClientConfig {
    /// Runtime daemon UDS path.
    pub socket_path: PathBuf,
    /// Optional daemon binary override.
    pub daemon_binary: Option<PathBuf>,
    /// Whether to spawn daemon if not currently reachable.
    pub auto_spawn: bool,
    /// Max wall-clock time for connection lifecycle completion.
    pub startup_timeout: Duration,
    /// Per-attempt socket connection timeout.
    pub connect_timeout: Duration,
    /// Per-attempt capabilities handshake timeout.
    pub request_timeout: Duration,
    /// Retry backoff floor between attempts.
    pub retry_backoff: Duration,
    /// Retry backoff ceiling between attempts.
    pub max_retry_backoff: Duration,
    /// Expected daemon version (exact match) when set.
    pub expected_daemon_version: Option<String>,
    /// Optional state-store path passed during daemon spawn.
    pub state_store_path: Option<PathBuf>,
    /// Optional runtime data directory passed during daemon spawn.
    pub runtime_data_dir: Option<PathBuf>,
}

impl Default for DaemonClientConfig {
    fn default() -> Self {
        Self {
            socket_path: PathBuf::from(".vz-runtime/runtimed.sock"),
            daemon_binary: None,
            auto_spawn: true,
            startup_timeout: Duration::from_secs(6),
            connect_timeout: Duration::from_millis(400),
            request_timeout: Duration::from_millis(800),
            retry_backoff: Duration::from_millis(40),
            max_retry_backoff: Duration::from_millis(320),
            expected_daemon_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            state_store_path: None,
            runtime_data_dir: None,
        }
    }
}

/// Capability handshake snapshot returned by daemon readiness probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonHandshake {
    pub daemon_id: String,
    pub daemon_version: String,
    pub backend_name: String,
    pub started_at_unix_secs: u64,
    pub request_id: String,
    pub capabilities: RuntimeCapabilities,
}

/// Reusable Runtime V2 daemon client.
#[derive(Debug, Clone)]
pub struct DaemonClient {
    config: DaemonClientConfig,
    handshake: DaemonHandshake,
    sandbox_client: runtime_v2::sandbox_service_client::SandboxServiceClient<Channel>,
    lease_client: runtime_v2::lease_service_client::LeaseServiceClient<Channel>,
    container_client: runtime_v2::container_service_client::ContainerServiceClient<Channel>,
    image_client: runtime_v2::image_service_client::ImageServiceClient<Channel>,
    build_client: runtime_v2::build_service_client::BuildServiceClient<Channel>,
    execution_client: runtime_v2::execution_service_client::ExecutionServiceClient<Channel>,
    checkpoint_client: runtime_v2::checkpoint_service_client::CheckpointServiceClient<Channel>,
    linux_vm_client: runtime_v2::linux_vm_service_client::LinuxVmServiceClient<Channel>,
    event_client: runtime_v2::event_service_client::EventServiceClient<Channel>,
    receipt_client: runtime_v2::receipt_service_client::ReceiptServiceClient<Channel>,
    stack_client: runtime_v2::stack_service_client::StackServiceClient<Channel>,
    file_client: runtime_v2::file_service_client::FileServiceClient<Channel>,
    capability_client: runtime_v2::capability_service_client::CapabilityServiceClient<Channel>,
}

impl DaemonClient {
    fn ensure_metadata(metadata: &mut Option<runtime_v2::RequestMetadata>) {
        if metadata.is_none() {
            *metadata = Some(request_metadata_to_proto(&RequestMetadata::default()));
        }
    }

    /// Connect with default config (auto-spawn enabled).
    pub async fn connect() -> Result<Self> {
        Self::connect_with_config(DaemonClientConfig::default()).await
    }

    /// Connect with explicit lifecycle config.
    ///
    /// When `auto_spawn` is enabled and the running daemon has a different
    /// version than this client, the stale daemon is stopped and a fresh
    /// one is spawned automatically.
    pub async fn connect_with_config(config: DaemonClientConfig) -> Result<Self> {
        let deadline = Instant::now() + config.startup_timeout;
        let mut backoff = config.retry_backoff;
        let mut spawned = false;
        let mut restarted_for_version = false;

        loop {
            match Self::connect_once(&config).await {
                Ok(client) => return Ok(client),
                Err(error) => {
                    // Version mismatch: kill the stale daemon and respawn (once).
                    if let DaemonClientError::IncompatibleVersion {
                        ref daemon_version,
                        ref client_version,
                    } = error
                    {
                        if config.auto_spawn && !restarted_for_version {
                            tracing::warn!(
                                daemon = %daemon_version,
                                client = %client_version,
                                "daemon version mismatch — restarting daemon"
                            );
                            Self::stop_running_daemon(&config.socket_path);
                            Self::spawn_daemon(&config)?;
                            restarted_for_version = true;
                            spawned = true;
                            backoff = config.retry_backoff;
                            tokio::time::sleep(backoff).await;
                            continue;
                        }
                        return Err(error);
                    }

                    if matches!(error, DaemonClientError::IncompatibleProtocol { .. }) {
                        return Err(error);
                    }

                    let last_error = error.to_string();

                    if config.auto_spawn && !spawned {
                        Self::spawn_daemon(&config)?;
                        spawned = true;
                    } else if !config.auto_spawn && Instant::now() >= deadline {
                        return Err(DaemonClientError::StartupTimeout {
                            socket_path: config.socket_path.clone(),
                            timeout_secs: config.startup_timeout.as_secs(),
                            last_error: last_error.clone(),
                        });
                    }

                    if Instant::now() >= deadline {
                        return Err(DaemonClientError::StartupTimeout {
                            socket_path: config.socket_path.clone(),
                            timeout_secs: config.startup_timeout.as_secs(),
                            last_error,
                        });
                    }

                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff.saturating_mul(2), config.max_retry_backoff);
                }
            }
        }
    }

    /// Reconnect using the same config.
    pub async fn reconnect(&self) -> Result<Self> {
        Self::connect_with_config(self.config.clone()).await
    }

    /// Socket path bound to this client connection policy.
    pub fn socket_path(&self) -> &Path {
        &self.config.socket_path
    }

    /// Last handshake snapshot.
    pub fn handshake(&self) -> &DaemonHandshake {
        &self.handshake
    }

    /// Perform a fresh capabilities handshake and update cached metadata.
    pub async fn refresh_handshake(&mut self) -> Result<&DaemonHandshake> {
        let handshake =
            handshake_via_capabilities(&self.config, &mut self.capability_client).await?;
        self.handshake = handshake;
        Ok(&self.handshake)
    }
    async fn connect_once(config: &DaemonClientConfig) -> Result<Self> {
        let channel = connect_channel(&config.socket_path, config.connect_timeout).await?;
        let sandbox_client =
            runtime_v2::sandbox_service_client::SandboxServiceClient::new(channel.clone());
        let lease_client =
            runtime_v2::lease_service_client::LeaseServiceClient::new(channel.clone());
        let container_client =
            runtime_v2::container_service_client::ContainerServiceClient::new(channel.clone());
        let image_client =
            runtime_v2::image_service_client::ImageServiceClient::new(channel.clone());
        let build_client =
            runtime_v2::build_service_client::BuildServiceClient::new(channel.clone());
        let execution_client =
            runtime_v2::execution_service_client::ExecutionServiceClient::new(channel.clone());
        let checkpoint_client =
            runtime_v2::checkpoint_service_client::CheckpointServiceClient::new(channel.clone());
        let linux_vm_client =
            runtime_v2::linux_vm_service_client::LinuxVmServiceClient::new(channel.clone());
        let event_client =
            runtime_v2::event_service_client::EventServiceClient::new(channel.clone());
        let receipt_client =
            runtime_v2::receipt_service_client::ReceiptServiceClient::new(channel.clone());
        let stack_client =
            runtime_v2::stack_service_client::StackServiceClient::new(channel.clone());
        let file_client = runtime_v2::file_service_client::FileServiceClient::new(channel.clone());
        let mut capability_client =
            runtime_v2::capability_service_client::CapabilityServiceClient::new(channel);
        let handshake = handshake_via_capabilities(config, &mut capability_client).await?;

        Ok(Self {
            config: config.clone(),
            handshake,
            sandbox_client,
            lease_client,
            container_client,
            image_client,
            build_client,
            execution_client,
            checkpoint_client,
            linux_vm_client,
            event_client,
            receipt_client,
            stack_client,
            file_client,
            capability_client,
        })
    }

    /// Best-effort stop of the daemon bound to the given socket path.
    ///
    /// Reads the PID file next to the socket (or falls back to lsof) and
    /// sends SIGTERM via the `kill` command. The socket file is removed so
    /// the next spawn gets a clean bind.
    fn stop_running_daemon(socket_path: &Path) {
        let pid_path = socket_path.with_extension("pid");
        let pid = std::fs::read_to_string(&pid_path)
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok());

        if let Some(pid) = pid {
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            let _ = std::fs::remove_file(&pid_path);
        } else {
            // Fallback: find the daemon process via lsof on the socket.
            if let Ok(output) = Command::new("lsof")
                .arg("-t")
                .arg(socket_path)
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .output()
            {
                for line in String::from_utf8_lossy(&output.stdout).lines() {
                    if let Ok(pid) = line.trim().parse::<u32>() {
                        let _ = Command::new("kill")
                            .arg("-TERM")
                            .arg(pid.to_string())
                            .stdout(Stdio::null())
                            .stderr(Stdio::null())
                            .status();
                    }
                }
            }
        }

        // Remove stale socket so spawn_daemon gets a clean bind.
        let _ = std::fs::remove_file(socket_path);

        // Brief pause for process cleanup.
        std::thread::sleep(Duration::from_millis(200));
    }

    fn spawn_daemon(config: &DaemonClientConfig) -> Result<()> {
        let binary = resolve_daemon_binary(config)?;
        if !binary.exists() {
            return Err(DaemonClientError::BinaryNotFound { path: binary });
        }

        if let Some(parent) = config.socket_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        if let Some(state_store_path) = &config.state_store_path
            && let Some(parent) = state_store_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        if let Some(runtime_data_dir) = &config.runtime_data_dir {
            std::fs::create_dir_all(runtime_data_dir)?;
        }

        // Direct daemon stderr to a log file for `vz logs` support.
        let log_file_path = config.socket_path.with_extension("log");
        let stderr_target = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file_path)
            .map(Stdio::from)
            .unwrap_or_else(|_| Stdio::null());

        let mut command = Command::new(&binary);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(stderr_target)
            .arg("--socket-path")
            .arg(&config.socket_path);

        if let Some(state_store_path) = &config.state_store_path {
            command
                .arg("--state-store-path")
                .arg(state_store_path.as_os_str());
        }
        if let Some(runtime_data_dir) = &config.runtime_data_dir {
            command
                .arg("--runtime-data-dir")
                .arg(runtime_data_dir.as_os_str());
        }

        command
            .spawn()
            .map_err(|source| DaemonClientError::SpawnFailed {
                path: binary,
                source,
            })?;
        Ok(())
    }
}

async fn handshake_via_capabilities(
    config: &DaemonClientConfig,
    capability_client: &mut runtime_v2::capability_service_client::CapabilityServiceClient<Channel>,
) -> Result<DaemonHandshake> {
    let response = tokio::time::timeout(
        config.request_timeout,
        capability_client.get_capabilities(Request::new(runtime_v2::GetCapabilitiesRequest {
            metadata: Some(request_metadata_to_proto(&RequestMetadata::default())),
        })),
    )
    .await
    .map_err(|_| DaemonClientError::Unavailable {
        socket_path: config.socket_path.clone(),
        reason: format!(
            "get_capabilities timed out after {}ms",
            config.request_timeout.as_millis()
        ),
    })?
    .map_err(|status| status_to_client_error(&config.socket_path, status))?;

    handshake_from_response(config, response)
}

fn handshake_from_response(
    config: &DaemonClientConfig,
    response: tonic::Response<runtime_v2::GetCapabilitiesResponse>,
) -> Result<DaemonHandshake> {
    let headers = response.metadata();
    let daemon_id = required_header(headers, "x-vz-runtimed-id")?;
    let daemon_version = required_header(headers, "x-vz-runtimed-version")?;
    let backend_name = required_header(headers, "x-vz-runtimed-backend")?;
    let started_at_unix_secs = required_header(headers, "x-vz-runtimed-started-at")?
        .parse::<u64>()
        .map_err(|error| DaemonClientError::IncompatibleProtocol {
            reason: format!("invalid x-vz-runtimed-started-at header: {error}"),
        })?;

    if let Some(expected) = &config.expected_daemon_version
        && daemon_version != *expected
    {
        return Err(DaemonClientError::IncompatibleVersion {
            daemon_version,
            client_version: expected.clone(),
        });
    }

    let response = response.into_inner();
    if response.request_id.trim().is_empty() {
        return Err(DaemonClientError::IncompatibleProtocol {
            reason: "capabilities response missing request_id".to_string(),
        });
    }

    let capabilities =
        runtime_capabilities_from_proto(&response.capabilities).map_err(|source| {
            DaemonClientError::IncompatibleProtocol {
                reason: format!("invalid capabilities payload: {source}"),
            }
        })?;

    Ok(DaemonHandshake {
        daemon_id,
        daemon_version,
        backend_name,
        started_at_unix_secs,
        request_id: response.request_id,
        capabilities,
    })
}

fn required_header(headers: &MetadataMap, name: &'static str) -> Result<String> {
    let value = headers
        .get(name)
        .ok_or_else(|| DaemonClientError::IncompatibleProtocol {
            reason: format!("missing required metadata header `{name}`"),
        })?;

    value
        .to_str()
        .map(str::to_string)
        .map_err(|error| DaemonClientError::IncompatibleProtocol {
            reason: format!("invalid metadata header `{name}`: {error}"),
        })
}

fn resolve_daemon_binary(config: &DaemonClientConfig) -> Result<PathBuf> {
    if let Some(path) = &config.daemon_binary {
        return Ok(path.clone());
    }

    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_vz-runtimed") {
        return Ok(PathBuf::from(path));
    }

    let current_exe = std::env::current_exe()
        .map_err(|source| DaemonClientError::ResolveCurrentExecutable { source })?;
    let mut sibling = current_exe.clone();
    sibling.set_file_name("vz-runtimed");
    if sibling.exists() {
        return Ok(sibling);
    }

    // During cargo test, current executable often lives in target/*/deps.
    // Try the parent bin directory as a fallback (target/*/vz-runtimed).
    if current_exe
        .parent()
        .and_then(|parent| parent.file_name())
        .is_some_and(|name| name == "deps")
        && let Some(parent_bin_dir) = current_exe.parent().and_then(|parent| parent.parent())
    {
        let candidate = parent_bin_dir.join("vz-runtimed");
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    Ok(sibling)
}
