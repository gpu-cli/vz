#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use hyper_util::rt::TokioIo;
use thiserror::Error;
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::{Code, Request, Status};
use tower::service_fn;
use vz_runtime_contract::{RequestMetadata, RuntimeCapabilities};
use vz_runtime_proto::runtime_v2;
use vz_runtime_translate::{request_metadata_to_proto, runtime_capabilities_from_proto};

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
    Transport(#[from] tonic::transport::Error),
    #[error("grpc status error: {0}")]
    Grpc(#[from] tonic::Status),
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
    pub async fn connect_with_config(config: DaemonClientConfig) -> Result<Self> {
        let deadline = Instant::now() + config.startup_timeout;
        let mut backoff = config.retry_backoff;
        let mut spawned = false;

        loop {
            match Self::connect_once(&config).await {
                Ok(client) => return Ok(client),
                Err(error) => {
                    if matches!(
                        error,
                        DaemonClientError::IncompatibleVersion { .. }
                            | DaemonClientError::IncompatibleProtocol { .. }
                    ) {
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

    /// Call Runtime V2 `CreateSandbox`.
    pub async fn create_sandbox(
        &mut self,
        request: runtime_v2::CreateSandboxRequest,
    ) -> Result<runtime_v2::SandboxResponse> {
        let response = self.create_sandbox_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CreateSandbox` and preserve gRPC response metadata.
    pub async fn create_sandbox_with_metadata(
        &mut self,
        mut request: runtime_v2::CreateSandboxRequest,
    ) -> Result<tonic::Response<runtime_v2::SandboxResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .create_sandbox(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetSandbox`.
    pub async fn get_sandbox(
        &mut self,
        request: runtime_v2::GetSandboxRequest,
    ) -> Result<runtime_v2::SandboxResponse> {
        let response = self.get_sandbox_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetSandbox` and preserve gRPC response metadata.
    pub async fn get_sandbox_with_metadata(
        &mut self,
        mut request: runtime_v2::GetSandboxRequest,
    ) -> Result<tonic::Response<runtime_v2::SandboxResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .get_sandbox(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListSandboxes`.
    pub async fn list_sandboxes(
        &mut self,
        request: runtime_v2::ListSandboxesRequest,
    ) -> Result<runtime_v2::ListSandboxesResponse> {
        let response = self.list_sandboxes_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListSandboxes` and preserve gRPC response metadata.
    pub async fn list_sandboxes_with_metadata(
        &mut self,
        mut request: runtime_v2::ListSandboxesRequest,
    ) -> Result<tonic::Response<runtime_v2::ListSandboxesResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .list_sandboxes(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `TerminateSandbox`.
    pub async fn terminate_sandbox(
        &mut self,
        request: runtime_v2::TerminateSandboxRequest,
    ) -> Result<runtime_v2::SandboxResponse> {
        let response = self.terminate_sandbox_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `TerminateSandbox` and preserve gRPC response metadata.
    pub async fn terminate_sandbox_with_metadata(
        &mut self,
        mut request: runtime_v2::TerminateSandboxRequest,
    ) -> Result<tonic::Response<runtime_v2::SandboxResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .terminate_sandbox(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `OpenSandboxShell` as a server stream.
    pub async fn open_sandbox_shell(
        &mut self,
        mut request: runtime_v2::OpenSandboxShellRequest,
    ) -> Result<tonic::Streaming<runtime_v2::OpenSandboxShellEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .open_sandbox_shell(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CloseSandboxShell` as a server stream.
    pub async fn close_sandbox_shell(
        &mut self,
        mut request: runtime_v2::CloseSandboxShellRequest,
    ) -> Result<tonic::Streaming<runtime_v2::CloseSandboxShellEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .close_sandbox_shell(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Convenience helper for `CreateSandbox`.
    pub async fn create_sandbox_for_stack(&mut self, stack_name: impl Into<String>) -> Result<()> {
        let request = runtime_v2::CreateSandboxRequest {
            metadata: Some(request_metadata_to_proto(&RequestMetadata::default())),
            stack_name: stack_name.into(),
            cpus: 0,
            memory_mb: 0,
            labels: HashMap::new(),
        };
        let _ = self.create_sandbox(request).await?;
        Ok(())
    }

    /// Call Runtime V2 `OpenLease`.
    pub async fn open_lease(
        &mut self,
        request: runtime_v2::OpenLeaseRequest,
    ) -> Result<runtime_v2::LeaseResponse> {
        let response = self.open_lease_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `OpenLease` and preserve gRPC response metadata.
    pub async fn open_lease_with_metadata(
        &mut self,
        mut request: runtime_v2::OpenLeaseRequest,
    ) -> Result<tonic::Response<runtime_v2::LeaseResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.lease_client
            .open_lease(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetLease`.
    pub async fn get_lease(
        &mut self,
        request: runtime_v2::GetLeaseRequest,
    ) -> Result<runtime_v2::LeaseResponse> {
        let response = self.get_lease_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetLease` and preserve gRPC response metadata.
    pub async fn get_lease_with_metadata(
        &mut self,
        mut request: runtime_v2::GetLeaseRequest,
    ) -> Result<tonic::Response<runtime_v2::LeaseResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.lease_client
            .get_lease(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListLeases`.
    pub async fn list_leases(
        &mut self,
        request: runtime_v2::ListLeasesRequest,
    ) -> Result<runtime_v2::ListLeasesResponse> {
        let response = self.list_leases_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListLeases` and preserve gRPC response metadata.
    pub async fn list_leases_with_metadata(
        &mut self,
        mut request: runtime_v2::ListLeasesRequest,
    ) -> Result<tonic::Response<runtime_v2::ListLeasesResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.lease_client
            .list_leases(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `HeartbeatLease`.
    pub async fn heartbeat_lease(
        &mut self,
        request: runtime_v2::HeartbeatLeaseRequest,
    ) -> Result<runtime_v2::LeaseResponse> {
        let response = self.heartbeat_lease_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `HeartbeatLease` and preserve gRPC response metadata.
    pub async fn heartbeat_lease_with_metadata(
        &mut self,
        mut request: runtime_v2::HeartbeatLeaseRequest,
    ) -> Result<tonic::Response<runtime_v2::LeaseResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.lease_client
            .heartbeat_lease(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CloseLease`.
    pub async fn close_lease(
        &mut self,
        request: runtime_v2::CloseLeaseRequest,
    ) -> Result<runtime_v2::LeaseResponse> {
        let response = self.close_lease_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CloseLease` and preserve gRPC response metadata.
    pub async fn close_lease_with_metadata(
        &mut self,
        mut request: runtime_v2::CloseLeaseRequest,
    ) -> Result<tonic::Response<runtime_v2::LeaseResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.lease_client
            .close_lease(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CreateContainer`.
    pub async fn create_container(
        &mut self,
        request: runtime_v2::CreateContainerRequest,
    ) -> Result<runtime_v2::ContainerResponse> {
        let response = self.create_container_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CreateContainer` and preserve gRPC response metadata.
    pub async fn create_container_with_metadata(
        &mut self,
        mut request: runtime_v2::CreateContainerRequest,
    ) -> Result<tonic::Response<runtime_v2::ContainerResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.container_client
            .create_container(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetContainer`.
    pub async fn get_container(
        &mut self,
        request: runtime_v2::GetContainerRequest,
    ) -> Result<runtime_v2::ContainerResponse> {
        let response = self.get_container_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetContainer` and preserve gRPC response metadata.
    pub async fn get_container_with_metadata(
        &mut self,
        mut request: runtime_v2::GetContainerRequest,
    ) -> Result<tonic::Response<runtime_v2::ContainerResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.container_client
            .get_container(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListContainers`.
    pub async fn list_containers(
        &mut self,
        request: runtime_v2::ListContainersRequest,
    ) -> Result<runtime_v2::ListContainersResponse> {
        let response = self.list_containers_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListContainers` and preserve gRPC response metadata.
    pub async fn list_containers_with_metadata(
        &mut self,
        mut request: runtime_v2::ListContainersRequest,
    ) -> Result<tonic::Response<runtime_v2::ListContainersResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.container_client
            .list_containers(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `RemoveContainer`.
    pub async fn remove_container(
        &mut self,
        request: runtime_v2::RemoveContainerRequest,
    ) -> Result<runtime_v2::ContainerResponse> {
        let response = self.remove_container_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `RemoveContainer` and preserve gRPC response metadata.
    pub async fn remove_container_with_metadata(
        &mut self,
        mut request: runtime_v2::RemoveContainerRequest,
    ) -> Result<tonic::Response<runtime_v2::ContainerResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.container_client
            .remove_container(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetImage`.
    pub async fn get_image(
        &mut self,
        request: runtime_v2::GetImageRequest,
    ) -> Result<runtime_v2::ImageResponse> {
        let response = self.get_image_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetImage` and preserve gRPC response metadata.
    pub async fn get_image_with_metadata(
        &mut self,
        mut request: runtime_v2::GetImageRequest,
    ) -> Result<tonic::Response<runtime_v2::ImageResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.image_client
            .get_image(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListImages`.
    pub async fn list_images(
        &mut self,
        request: runtime_v2::ListImagesRequest,
    ) -> Result<runtime_v2::ListImagesResponse> {
        let response = self.list_images_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListImages` and preserve gRPC response metadata.
    pub async fn list_images_with_metadata(
        &mut self,
        mut request: runtime_v2::ListImagesRequest,
    ) -> Result<tonic::Response<runtime_v2::ListImagesResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.image_client
            .list_images(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `PullImage` as a server stream.
    pub async fn pull_image(
        &mut self,
        mut request: runtime_v2::PullImageRequest,
    ) -> Result<tonic::Streaming<runtime_v2::PullImageEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.image_client
            .pull_image(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `PruneImages` as a server stream.
    pub async fn prune_images(
        &mut self,
        mut request: runtime_v2::PruneImagesRequest,
    ) -> Result<tonic::Streaming<runtime_v2::PruneImagesEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.image_client
            .prune_images(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListEvents`.
    pub async fn list_events(
        &mut self,
        request: runtime_v2::ListEventsRequest,
    ) -> Result<runtime_v2::ListEventsResponse> {
        let response = self.list_events_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListEvents` and preserve gRPC response metadata.
    pub async fn list_events_with_metadata(
        &mut self,
        mut request: runtime_v2::ListEventsRequest,
    ) -> Result<tonic::Response<runtime_v2::ListEventsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.event_client
            .list_events(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `StreamEvents`.
    pub async fn stream_events(
        &mut self,
        mut request: runtime_v2::StreamEventsRequest,
    ) -> Result<tonic::Streaming<runtime_v2::RuntimeEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.event_client
            .stream_events(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetReceipt`.
    pub async fn get_receipt(
        &mut self,
        request: runtime_v2::GetReceiptRequest,
    ) -> Result<runtime_v2::ReceiptResponse> {
        let response = self.get_receipt_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetReceipt` and preserve gRPC response metadata.
    pub async fn get_receipt_with_metadata(
        &mut self,
        mut request: runtime_v2::GetReceiptRequest,
    ) -> Result<tonic::Response<runtime_v2::ReceiptResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.receipt_client
            .get_receipt(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ApplyStack`.
    pub async fn apply_stack(
        &mut self,
        request: runtime_v2::ApplyStackRequest,
    ) -> Result<runtime_v2::ApplyStackResponse> {
        let response = self.apply_stack_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ApplyStack` and preserve gRPC response metadata.
    pub async fn apply_stack_with_metadata(
        &mut self,
        mut request: runtime_v2::ApplyStackRequest,
    ) -> Result<tonic::Response<runtime_v2::ApplyStackResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .apply_stack(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `TeardownStack`.
    pub async fn teardown_stack(
        &mut self,
        request: runtime_v2::TeardownStackRequest,
    ) -> Result<runtime_v2::TeardownStackResponse> {
        let response = self.teardown_stack_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `TeardownStack` and preserve gRPC response metadata.
    pub async fn teardown_stack_with_metadata(
        &mut self,
        mut request: runtime_v2::TeardownStackRequest,
    ) -> Result<tonic::Response<runtime_v2::TeardownStackResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .teardown_stack(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetStackStatus`.
    pub async fn get_stack_status(
        &mut self,
        request: runtime_v2::GetStackStatusRequest,
    ) -> Result<runtime_v2::GetStackStatusResponse> {
        let response = self.get_stack_status_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetStackStatus` and preserve gRPC response metadata.
    pub async fn get_stack_status_with_metadata(
        &mut self,
        mut request: runtime_v2::GetStackStatusRequest,
    ) -> Result<tonic::Response<runtime_v2::GetStackStatusResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .get_stack_status(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListStackEvents`.
    pub async fn list_stack_events(
        &mut self,
        request: runtime_v2::ListStackEventsRequest,
    ) -> Result<runtime_v2::ListStackEventsResponse> {
        let response = self.list_stack_events_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListStackEvents` and preserve gRPC response metadata.
    pub async fn list_stack_events_with_metadata(
        &mut self,
        mut request: runtime_v2::ListStackEventsRequest,
    ) -> Result<tonic::Response<runtime_v2::ListStackEventsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .list_stack_events(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetStackLogs`.
    pub async fn get_stack_logs(
        &mut self,
        request: runtime_v2::GetStackLogsRequest,
    ) -> Result<runtime_v2::GetStackLogsResponse> {
        let response = self.get_stack_logs_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetStackLogs` and preserve gRPC response metadata.
    pub async fn get_stack_logs_with_metadata(
        &mut self,
        mut request: runtime_v2::GetStackLogsRequest,
    ) -> Result<tonic::Response<runtime_v2::GetStackLogsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .get_stack_logs(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `StopStackService`.
    pub async fn stop_stack_service(
        &mut self,
        request: runtime_v2::StackServiceActionRequest,
    ) -> Result<runtime_v2::StackServiceActionResponse> {
        let response = self.stop_stack_service_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `StopStackService` and preserve gRPC response metadata.
    pub async fn stop_stack_service_with_metadata(
        &mut self,
        mut request: runtime_v2::StackServiceActionRequest,
    ) -> Result<tonic::Response<runtime_v2::StackServiceActionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .stop_stack_service(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `StartStackService`.
    pub async fn start_stack_service(
        &mut self,
        request: runtime_v2::StackServiceActionRequest,
    ) -> Result<runtime_v2::StackServiceActionResponse> {
        let response = self.start_stack_service_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `StartStackService` and preserve gRPC response metadata.
    pub async fn start_stack_service_with_metadata(
        &mut self,
        mut request: runtime_v2::StackServiceActionRequest,
    ) -> Result<tonic::Response<runtime_v2::StackServiceActionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .start_stack_service(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `RestartStackService`.
    pub async fn restart_stack_service(
        &mut self,
        request: runtime_v2::StackServiceActionRequest,
    ) -> Result<runtime_v2::StackServiceActionResponse> {
        let response = self.restart_stack_service_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `RestartStackService` and preserve gRPC response metadata.
    pub async fn restart_stack_service_with_metadata(
        &mut self,
        mut request: runtime_v2::StackServiceActionRequest,
    ) -> Result<tonic::Response<runtime_v2::StackServiceActionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .restart_stack_service(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CreateStackRunContainer`.
    pub async fn create_stack_run_container(
        &mut self,
        request: runtime_v2::StackRunContainerRequest,
    ) -> Result<runtime_v2::StackRunContainerResponse> {
        let response = self
            .create_stack_run_container_with_metadata(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CreateStackRunContainer` and preserve gRPC response metadata.
    pub async fn create_stack_run_container_with_metadata(
        &mut self,
        mut request: runtime_v2::StackRunContainerRequest,
    ) -> Result<tonic::Response<runtime_v2::StackRunContainerResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .create_stack_run_container(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `RemoveStackRunContainer`.
    pub async fn remove_stack_run_container(
        &mut self,
        request: runtime_v2::StackRunContainerRequest,
    ) -> Result<runtime_v2::StackRunContainerResponse> {
        let response = self
            .remove_stack_run_container_with_metadata(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `RemoveStackRunContainer` and preserve gRPC response metadata.
    pub async fn remove_stack_run_container_with_metadata(
        &mut self,
        mut request: runtime_v2::StackRunContainerRequest,
    ) -> Result<tonic::Response<runtime_v2::StackRunContainerResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .remove_stack_run_container(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ReadFile`.
    pub async fn read_file(
        &mut self,
        request: runtime_v2::ReadFileRequest,
    ) -> Result<runtime_v2::ReadFileResponse> {
        let response = self.read_file_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ReadFile` and preserve gRPC response metadata.
    pub async fn read_file_with_metadata(
        &mut self,
        mut request: runtime_v2::ReadFileRequest,
    ) -> Result<tonic::Response<runtime_v2::ReadFileResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .read_file(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `WriteFile`.
    pub async fn write_file(
        &mut self,
        request: runtime_v2::WriteFileRequest,
    ) -> Result<runtime_v2::WriteFileResponse> {
        let response = self.write_file_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `WriteFile` and preserve gRPC response metadata.
    pub async fn write_file_with_metadata(
        &mut self,
        mut request: runtime_v2::WriteFileRequest,
    ) -> Result<tonic::Response<runtime_v2::WriteFileResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .write_file(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListFiles`.
    pub async fn list_files(
        &mut self,
        request: runtime_v2::ListFilesRequest,
    ) -> Result<runtime_v2::ListFilesResponse> {
        let response = self.list_files_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListFiles` and preserve gRPC response metadata.
    pub async fn list_files_with_metadata(
        &mut self,
        mut request: runtime_v2::ListFilesRequest,
    ) -> Result<tonic::Response<runtime_v2::ListFilesResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .list_files(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `MakeDir`.
    pub async fn make_dir(
        &mut self,
        request: runtime_v2::MakeDirRequest,
    ) -> Result<runtime_v2::FileMutationResponse> {
        let response = self.make_dir_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `MakeDir` and preserve gRPC response metadata.
    pub async fn make_dir_with_metadata(
        &mut self,
        mut request: runtime_v2::MakeDirRequest,
    ) -> Result<tonic::Response<runtime_v2::FileMutationResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .make_dir(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `RemovePath`.
    pub async fn remove_path(
        &mut self,
        request: runtime_v2::RemovePathRequest,
    ) -> Result<runtime_v2::FileMutationResponse> {
        let response = self.remove_path_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `RemovePath` and preserve gRPC response metadata.
    pub async fn remove_path_with_metadata(
        &mut self,
        mut request: runtime_v2::RemovePathRequest,
    ) -> Result<tonic::Response<runtime_v2::FileMutationResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .remove_path(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `MovePath`.
    pub async fn move_path(
        &mut self,
        request: runtime_v2::MovePathRequest,
    ) -> Result<runtime_v2::FileMutationResponse> {
        let response = self.move_path_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `MovePath` and preserve gRPC response metadata.
    pub async fn move_path_with_metadata(
        &mut self,
        mut request: runtime_v2::MovePathRequest,
    ) -> Result<tonic::Response<runtime_v2::FileMutationResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .move_path(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CopyPath`.
    pub async fn copy_path(
        &mut self,
        request: runtime_v2::CopyPathRequest,
    ) -> Result<runtime_v2::FileMutationResponse> {
        let response = self.copy_path_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CopyPath` and preserve gRPC response metadata.
    pub async fn copy_path_with_metadata(
        &mut self,
        mut request: runtime_v2::CopyPathRequest,
    ) -> Result<tonic::Response<runtime_v2::FileMutationResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .copy_path(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ChmodPath`.
    pub async fn chmod_path(
        &mut self,
        request: runtime_v2::ChmodPathRequest,
    ) -> Result<runtime_v2::FileMutationResponse> {
        let response = self.chmod_path_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ChmodPath` and preserve gRPC response metadata.
    pub async fn chmod_path_with_metadata(
        &mut self,
        mut request: runtime_v2::ChmodPathRequest,
    ) -> Result<tonic::Response<runtime_v2::FileMutationResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .chmod_path(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ChownPath`.
    pub async fn chown_path(
        &mut self,
        request: runtime_v2::ChownPathRequest,
    ) -> Result<runtime_v2::FileMutationResponse> {
        let response = self.chown_path_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ChownPath` and preserve gRPC response metadata.
    pub async fn chown_path_with_metadata(
        &mut self,
        mut request: runtime_v2::ChownPathRequest,
    ) -> Result<tonic::Response<runtime_v2::FileMutationResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .chown_path(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `StartBuild`.
    pub async fn start_build(
        &mut self,
        request: runtime_v2::StartBuildRequest,
    ) -> Result<runtime_v2::BuildResponse> {
        let response = self.start_build_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `StartBuild` and preserve gRPC response metadata.
    pub async fn start_build_with_metadata(
        &mut self,
        mut request: runtime_v2::StartBuildRequest,
    ) -> Result<tonic::Response<runtime_v2::BuildResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.build_client
            .start_build(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetBuild`.
    pub async fn get_build(
        &mut self,
        request: runtime_v2::GetBuildRequest,
    ) -> Result<runtime_v2::BuildResponse> {
        let response = self.get_build_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetBuild` and preserve gRPC response metadata.
    pub async fn get_build_with_metadata(
        &mut self,
        mut request: runtime_v2::GetBuildRequest,
    ) -> Result<tonic::Response<runtime_v2::BuildResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.build_client
            .get_build(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListBuilds`.
    pub async fn list_builds(
        &mut self,
        request: runtime_v2::ListBuildsRequest,
    ) -> Result<runtime_v2::ListBuildsResponse> {
        let response = self.list_builds_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListBuilds` and preserve gRPC response metadata.
    pub async fn list_builds_with_metadata(
        &mut self,
        mut request: runtime_v2::ListBuildsRequest,
    ) -> Result<tonic::Response<runtime_v2::ListBuildsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.build_client
            .list_builds(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CancelBuild`.
    pub async fn cancel_build(
        &mut self,
        request: runtime_v2::CancelBuildRequest,
    ) -> Result<runtime_v2::BuildResponse> {
        let response = self.cancel_build_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CancelBuild` and preserve gRPC response metadata.
    pub async fn cancel_build_with_metadata(
        &mut self,
        mut request: runtime_v2::CancelBuildRequest,
    ) -> Result<tonic::Response<runtime_v2::BuildResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.build_client
            .cancel_build(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CreateExecution`.
    pub async fn create_execution(
        &mut self,
        request: runtime_v2::CreateExecutionRequest,
    ) -> Result<runtime_v2::ExecutionResponse> {
        let response = self.create_execution_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CreateExecution` and preserve gRPC response metadata.
    pub async fn create_execution_with_metadata(
        &mut self,
        mut request: runtime_v2::CreateExecutionRequest,
    ) -> Result<tonic::Response<runtime_v2::ExecutionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .create_execution(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetExecution`.
    pub async fn get_execution(
        &mut self,
        request: runtime_v2::GetExecutionRequest,
    ) -> Result<runtime_v2::ExecutionResponse> {
        let response = self.get_execution_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetExecution` and preserve gRPC response metadata.
    pub async fn get_execution_with_metadata(
        &mut self,
        mut request: runtime_v2::GetExecutionRequest,
    ) -> Result<tonic::Response<runtime_v2::ExecutionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .get_execution(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListExecutions`.
    pub async fn list_executions(
        &mut self,
        request: runtime_v2::ListExecutionsRequest,
    ) -> Result<runtime_v2::ListExecutionsResponse> {
        let response = self.list_executions_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListExecutions` and preserve gRPC response metadata.
    pub async fn list_executions_with_metadata(
        &mut self,
        mut request: runtime_v2::ListExecutionsRequest,
    ) -> Result<tonic::Response<runtime_v2::ListExecutionsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .list_executions(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CancelExecution`.
    pub async fn cancel_execution(
        &mut self,
        request: runtime_v2::CancelExecutionRequest,
    ) -> Result<runtime_v2::ExecutionResponse> {
        let response = self.cancel_execution_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CancelExecution` and preserve gRPC response metadata.
    pub async fn cancel_execution_with_metadata(
        &mut self,
        mut request: runtime_v2::CancelExecutionRequest,
    ) -> Result<tonic::Response<runtime_v2::ExecutionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .cancel_execution(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `StreamExecOutput`.
    pub async fn stream_exec_output(
        &mut self,
        mut request: runtime_v2::StreamExecOutputRequest,
    ) -> Result<tonic::Streaming<runtime_v2::ExecOutputEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .stream_exec_output(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `WriteExecStdin`.
    pub async fn write_exec_stdin(
        &mut self,
        request: runtime_v2::WriteExecStdinRequest,
    ) -> Result<runtime_v2::ExecutionResponse> {
        let response = self.write_exec_stdin_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `WriteExecStdin` and preserve gRPC response metadata.
    pub async fn write_exec_stdin_with_metadata(
        &mut self,
        mut request: runtime_v2::WriteExecStdinRequest,
    ) -> Result<tonic::Response<runtime_v2::ExecutionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .write_exec_stdin(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ResizeExecPty`.
    pub async fn resize_exec_pty(
        &mut self,
        request: runtime_v2::ResizeExecPtyRequest,
    ) -> Result<runtime_v2::ExecutionResponse> {
        let response = self.resize_exec_pty_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ResizeExecPty` and preserve gRPC response metadata.
    pub async fn resize_exec_pty_with_metadata(
        &mut self,
        mut request: runtime_v2::ResizeExecPtyRequest,
    ) -> Result<tonic::Response<runtime_v2::ExecutionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .resize_exec_pty(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `SignalExec`.
    pub async fn signal_exec(
        &mut self,
        request: runtime_v2::SignalExecRequest,
    ) -> Result<runtime_v2::ExecutionResponse> {
        let response = self.signal_exec_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `SignalExec` and preserve gRPC response metadata.
    pub async fn signal_exec_with_metadata(
        &mut self,
        mut request: runtime_v2::SignalExecRequest,
    ) -> Result<tonic::Response<runtime_v2::ExecutionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .signal_exec(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CreateCheckpoint`.
    pub async fn create_checkpoint(
        &mut self,
        request: runtime_v2::CreateCheckpointRequest,
    ) -> Result<runtime_v2::CheckpointResponse> {
        let response = self.create_checkpoint_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CreateCheckpoint` and preserve gRPC response metadata.
    pub async fn create_checkpoint_with_metadata(
        &mut self,
        mut request: runtime_v2::CreateCheckpointRequest,
    ) -> Result<tonic::Response<runtime_v2::CheckpointResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .create_checkpoint(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetCheckpoint`.
    pub async fn get_checkpoint(
        &mut self,
        request: runtime_v2::GetCheckpointRequest,
    ) -> Result<runtime_v2::CheckpointResponse> {
        let response = self.get_checkpoint_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetCheckpoint` and preserve gRPC response metadata.
    pub async fn get_checkpoint_with_metadata(
        &mut self,
        mut request: runtime_v2::GetCheckpointRequest,
    ) -> Result<tonic::Response<runtime_v2::CheckpointResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .get_checkpoint(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListCheckpoints`.
    pub async fn list_checkpoints(
        &mut self,
        request: runtime_v2::ListCheckpointsRequest,
    ) -> Result<runtime_v2::ListCheckpointsResponse> {
        let response = self.list_checkpoints_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListCheckpoints` and preserve gRPC response metadata.
    pub async fn list_checkpoints_with_metadata(
        &mut self,
        mut request: runtime_v2::ListCheckpointsRequest,
    ) -> Result<tonic::Response<runtime_v2::ListCheckpointsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .list_checkpoints(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `RestoreCheckpoint`.
    pub async fn restore_checkpoint(
        &mut self,
        request: runtime_v2::RestoreCheckpointRequest,
    ) -> Result<runtime_v2::CheckpointResponse> {
        let response = self.restore_checkpoint_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `RestoreCheckpoint` and preserve gRPC response metadata.
    pub async fn restore_checkpoint_with_metadata(
        &mut self,
        mut request: runtime_v2::RestoreCheckpointRequest,
    ) -> Result<tonic::Response<runtime_v2::CheckpointResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .restore_checkpoint(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ForkCheckpoint`.
    pub async fn fork_checkpoint(
        &mut self,
        request: runtime_v2::ForkCheckpointRequest,
    ) -> Result<runtime_v2::CheckpointResponse> {
        let response = self.fork_checkpoint_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ForkCheckpoint` and preserve gRPC response metadata.
    pub async fn fork_checkpoint_with_metadata(
        &mut self,
        mut request: runtime_v2::ForkCheckpointRequest,
    ) -> Result<tonic::Response<runtime_v2::CheckpointResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .fork_checkpoint(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
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
            event_client,
            receipt_client,
            stack_client,
            file_client,
            capability_client,
        })
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

        let mut command = Command::new(&binary);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
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

async fn connect_channel(socket_path: &Path, timeout: Duration) -> Result<Channel> {
    if !socket_path.exists() {
        return Err(DaemonClientError::Unavailable {
            socket_path: socket_path.to_path_buf(),
            reason: "socket does not exist".to_string(),
        });
    }

    let socket_path_buf = socket_path.to_path_buf();
    let endpoint = Endpoint::try_from("http://[::]:50051")
        .map_err(|error| DaemonClientError::Unavailable {
            socket_path: socket_path.to_path_buf(),
            reason: format!("invalid endpoint: {error}"),
        })?
        .connect_timeout(timeout);

    endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path_buf.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .map_err(|error| DaemonClientError::Unavailable {
            socket_path: socket_path.to_path_buf(),
            reason: error.to_string(),
        })
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

fn status_to_client_error(socket_path: &Path, status: Status) -> DaemonClientError {
    if matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::Unknown
    ) {
        return DaemonClientError::Unavailable {
            socket_path: socket_path.to_path_buf(),
            reason: status.to_string(),
        };
    }
    DaemonClientError::Grpc(status)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::Notify;
    use vz_runtimed::{RuntimeDaemon, RuntimedConfig, serve_runtime_uds_with_shutdown};

    use super::*;

    struct RunningDaemon {
        shutdown: Arc<Notify>,
        task: tokio::task::JoinHandle<std::result::Result<(), vz_runtimed::RuntimedServerError>>,
    }

    impl RunningDaemon {
        async fn stop(self) {
            self.shutdown.notify_waiters();
            let join = tokio::time::timeout(Duration::from_secs(5), self.task)
                .await
                .expect("server join timeout")
                .expect("server task join failed");
            assert!(join.is_ok());
        }
    }

    async fn wait_for_socket(path: &Path) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            if path.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("socket was not created in time: {}", path.display());
    }

    async fn start_daemon(config: RuntimedConfig) -> RunningDaemon {
        let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
        let shutdown = Arc::new(Notify::new());
        let shutdown_task = shutdown.clone();
        let socket_path = config.socket_path.clone();
        let task = tokio::spawn(async move {
            serve_runtime_uds_with_shutdown(daemon, socket_path, async move {
                shutdown_task.notified().await;
            })
            .await
        });
        wait_for_socket(&config.socket_path).await;
        RunningDaemon { shutdown, task }
    }

    fn runtimed_config(tmp: &tempfile::TempDir) -> RuntimedConfig {
        RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        }
    }

    fn client_config(tmp: &tempfile::TempDir, auto_spawn: bool) -> DaemonClientConfig {
        let daemon = runtimed_config(tmp);
        DaemonClientConfig {
            socket_path: daemon.socket_path,
            auto_spawn,
            startup_timeout: Duration::from_secs(3),
            connect_timeout: Duration::from_millis(300),
            request_timeout: Duration::from_millis(500),
            retry_backoff: Duration::from_millis(30),
            max_retry_backoff: Duration::from_millis(120),
            ..DaemonClientConfig::default()
        }
    }

    #[tokio::test]
    async fn connect_retries_until_daemon_cold_start_is_ready() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let config = runtimed_config(&tmp);
        let socket_path = config.socket_path.clone();

        let shutdown = Arc::new(Notify::new());
        let shutdown_task = shutdown.clone();
        let server = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(220)).await;
            let daemon = Arc::new(RuntimeDaemon::start(config).expect("daemon start"));
            serve_runtime_uds_with_shutdown(daemon, socket_path, async move {
                shutdown_task.notified().await;
            })
            .await
        });

        let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
            .await
            .expect("client should connect after delayed startup");
        assert!(!client.handshake().daemon_id.is_empty());

        let error = client
            .create_sandbox(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "   ".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels: HashMap::new(),
            })
            .await
            .expect_err("empty stack name should fail validation");
        assert!(matches!(
            error,
            DaemonClientError::Grpc(status) if status.code() == Code::InvalidArgument
        ));

        shutdown.notify_waiters();
        let result = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("server join timeout")
            .expect("server task join failed");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn reconnect_after_daemon_restart_yields_new_handshake() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let daemon_config = runtimed_config(&tmp);
        let first = start_daemon(daemon_config.clone()).await;

        let client = DaemonClient::connect_with_config(client_config(&tmp, false))
            .await
            .expect("client connect");
        let first_request_id = client.handshake().request_id.clone();

        first.stop().await;

        let second = start_daemon(daemon_config).await;
        let mut reconnected = client.reconnect().await.expect("client reconnect");
        let second_request_id = reconnected.handshake().request_id.clone();
        assert_ne!(first_request_id, second_request_id);

        let error = reconnected
            .create_sandbox(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels: HashMap::new(),
            })
            .await
            .expect_err("empty stack name should fail validation");
        assert!(matches!(
            error,
            DaemonClientError::Grpc(status) if status.code() == Code::InvalidArgument
        ));

        second.stop().await;
    }

    #[tokio::test]
    async fn version_mismatch_returns_incompatible_version() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let daemon = start_daemon(runtimed_config(&tmp)).await;

        let mut config = client_config(&tmp, false);
        config.expected_daemon_version = Some("999.999.999".to_string());
        let error = match DaemonClient::connect_with_config(config).await {
            Ok(_) => panic!("mismatch should fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            DaemonClientError::IncompatibleVersion { .. }
        ));

        daemon.stop().await;
    }
}
