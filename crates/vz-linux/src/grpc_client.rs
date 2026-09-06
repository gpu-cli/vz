//! gRPC-based guest agent client.
//!
//! Provides the host-side client for communicating with the guest agent
//! over gRPC/protobuf. The gRPC channel runs over vsock via a custom
//! tonic connector.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::debug;
use vz::Vm;
use vz::protocol::{ExecOutput, OciContainerState};
use vz_agent_proto::{
    AllocateExecRequestRequest, CancelExecRequest, CancelExecResponse, ContainerExecTarget,
    ContainerGeneration, DockerEnsureEvent, DockerEnsureRequest, ExecRequest as ProtoExecRequest,
    NetworkSetupRequest, NetworkTeardownRequest, OciCreateRequest, OciDeleteRequest,
    OciKillRequest, OciStartRequest, OciStateRequest, PingRequest, PortForwardFrame,
    PortForwardOpen, ReconcileExecRequest, ReconcileExecResponse, ResizeExecPtyRequest,
    ResourceStatsRequest, ResourceStatsResponse, SignalRequest, StdinCloseRequest,
    StdinWriteRequest, SystemInfoRequest, SystemInfoResponse,
    TransportMetadata as ProtoTransportMetadata, agent_service_client::AgentServiceClient,
    exec_event, network_service_client::NetworkServiceClient, oci_service_client::OciServiceClient,
    port_forward_frame,
};
use vz_runtime_contract::{
    CheckpointClass, RequestMetadata as ContractRequestMetadata, RuntimeCapabilities,
    RuntimeOperation,
    ensure_checkpoint_class_supported as contract_ensure_checkpoint_class_supported,
};

use crate::LinuxError;

/// Default gRPC agent port (matches [`vz::protocol::AGENT_PORT`]).
const GRPC_AGENT_PORT: u32 = 7424;
/// BusyBox command path used to set env vars for `buildctl`.
const GUEST_BUSYBOX_BINARY: &str = "/bin/busybox";
/// Guest path where BuildKit tooling is mounted.
const GUEST_BUILDCTL_BINARY: &str = "/mnt/buildkit-bin/buildctl";

/// Timeout for establishing the vsock connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const ALLOCATE_EXEC_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

const EXEC_DISPATCH_PENDING: u8 = 0;
const EXEC_DISPATCH_AUTHORIZED: u8 = 1;
const EXEC_DISPATCH_CANCELLED: u8 = 2;

/// Per-request linearization gate between connection preflight and the exact
/// container Exec RPC send.
///
/// Cancellation or deadline expiry that closes a pending gate proves that no
/// Exec RPC was dispatched. Once dispatch authorization wins, the request ID
/// is instead ambiguous until readiness or reconciliation proves its outcome.
#[derive(Clone, Debug)]
pub struct ContainerExecDispatchGate {
    state: Arc<AtomicU8>,
    deadline: tokio::time::Instant,
}

impl ContainerExecDispatchGate {
    pub fn new(deadline: tokio::time::Instant) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(EXEC_DISPATCH_PENDING)),
            deadline,
        }
    }

    /// Prevent dispatch if the gate is still pending.
    ///
    /// Returns `true` only when this call proved the request was never
    /// authorized for dispatch. `false` means dispatch already owns the
    /// request identity and must be reconciled on an uncertain outcome.
    pub fn cancel_before_dispatch(&self) -> bool {
        self.state
            .compare_exchange(
                EXEC_DISPATCH_PENDING,
                EXEC_DISPATCH_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
            || self.state.load(Ordering::Acquire) == EXEC_DISPATCH_CANCELLED
    }

    fn authorize_dispatch(&self) -> bool {
        if tokio::time::Instant::now() >= self.deadline {
            self.cancel_before_dispatch();
        }
        self.state
            .compare_exchange(
                EXEC_DISPATCH_PENDING,
                EXEC_DISPATCH_AUTHORIZED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

fn exec_control_debug_enabled() -> bool {
    std::env::var("VZ_LINUX_EXEC_CONTROL_DEBUG")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Options for guest command execution.
#[derive(Debug, Clone, Default)]
pub struct ExecOptions {
    /// Optional working directory inside the guest.
    pub working_dir: Option<String>,
    /// Environment variables for the process.
    pub env: Vec<(String, String)>,
    /// Optional user to run as.
    pub user: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ExecTerminal {
    allocate_pty: bool,
    rows: u32,
    cols: u32,
}

fn build_exec_request(
    command: String,
    args: Vec<String>,
    options: ExecOptions,
    container_target: Option<String>,
    metadata: ProtoTransportMetadata,
    terminal: ExecTerminal,
) -> ProtoExecRequest {
    ProtoExecRequest {
        command,
        args,
        working_dir: options.working_dir.unwrap_or_default(),
        env: options.env.into_iter().collect(),
        user: options.user.unwrap_or_default(),
        metadata: Some(metadata),
        allocate_pty: terminal.allocate_pty,
        term_rows: terminal.rows,
        term_cols: terminal.cols,
        container_target: container_target.map(|container_id| ContainerExecTarget { container_id }),
        supervised_machine: false,
    }
}

fn retired_oci_exec_error() -> LinuxError {
    LinuxError::Protocol(
        "legacy OciService.Exec is retired; use supervised AgentService.Exec stream collection"
            .to_string(),
    )
}

/// Options for OCI exec requests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OciExecOptions {
    /// Environment variables for the process.
    pub env: Vec<(String, String)>,
    /// Optional working directory inside the container.
    pub cwd: Option<String>,
    /// Optional user identity inside the container.
    pub user: Option<String>,
}

/// gRPC-based guest agent client.
///
/// Wraps three tonic service clients that share a single vsock-backed
/// gRPC channel to the guest agent:
///
/// - [`AgentServiceClient`] -- ping, system info, exec, port forward
/// - [`OciServiceClient`] -- container lifecycle
/// - [`NetworkServiceClient`] -- network namespace management
pub struct GrpcAgentClient {
    /// Agent service client (ping, system info, resource stats, exec, port forward).
    agent: AgentServiceClient<Channel>,
    /// OCI container lifecycle client.
    oci: OciServiceClient<Channel>,
    /// Network namespace management client.
    network: NetworkServiceClient<Channel>,
    /// Monotonic request sequence used to mint request IDs.
    next_request_sequence: u64,
}

/// Failure while establishing a container exec's addressable ready state.
///
/// `Definite` means the request was rejected before a guest process could remain
/// live. `Ambiguous` means transport or protocol state prevented the host from
/// proving that no process was registered, so callers must retain lifecycle
/// authority rather than treating the start as finished.
#[derive(Debug)]
pub enum ContainerExecStartError {
    /// The guest definitively rejected the request without leaving live work.
    Definite(LinuxError),
    /// The host cannot prove whether the guest registered live work.
    Ambiguous(LinuxError),
}

impl ContainerExecStartError {
    /// Consume the classification and return the underlying error.
    pub fn into_inner(self) -> LinuxError {
        match self {
            Self::Definite(error) | Self::Ambiguous(error) => error,
        }
    }

    /// Whether lifecycle ownership must be retained after this failure.
    pub fn is_ambiguous(&self) -> bool {
        matches!(self, Self::Ambiguous(_))
    }
}

impl std::fmt::Display for ContainerExecStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Definite(error) | Self::Ambiguous(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ContainerExecStartError {}

fn classify_exec_rpc_status(status: tonic::Status) -> ContainerExecStartError {
    // A tonic Status does not prove where the server stopped: even an
    // application-looking code can race transport loss after registration.
    // Only an authenticated in-stream rejection frame is classified definite.
    ContainerExecStartError::Ambiguous(LinuxError::from(status))
}

async fn inject_container_exec_response_loss(command: &str) -> bool {
    static INJECTED: AtomicBool = AtomicBool::new(false);

    let Ok(expected) = std::env::var("VZ_TEST_DROP_CONTAINER_EXEC_RESPONSE_BEFORE_READY_COMMAND")
    else {
        return false;
    };
    if expected != command {
        return false;
    }
    if INJECTED.swap(true, Ordering::AcqRel) {
        return false;
    }
    let dwell_ms = std::env::var("VZ_TEST_DROP_CONTAINER_EXEC_RESPONSE_DWELL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1000)
        .min(30_000);
    tokio::time::sleep(Duration::from_millis(dwell_ms)).await;
    true
}

fn validate_reconcile_exec_response(
    response: &ReconcileExecResponse,
    expected_request_id: &str,
) -> Result<(), LinuxError> {
    use vz_agent_proto::reconcile_exec_response::Outcome;

    if response.exec_request_id != expected_request_id {
        return Err(LinuxError::Protocol(format!(
            "exec reconciliation request mismatch: expected `{expected_request_id}`, got `{}`",
            response.exec_request_id
        )));
    }
    match Outcome::try_from(response.outcome) {
        Ok(Outcome::FencedNeverStarted)
            if response.exec_id == 0 && response.exit_code == 0 && !response.forced => {}
        Ok(Outcome::TerminalReaped)
            if response.exec_id > 0 && (0..=255).contains(&response.exit_code) => {}
        Ok(Outcome::StaleUnknown)
            if response.exec_id == 0 && response.exit_code == 0 && !response.forced => {}
        Err(_) | Ok(Outcome::Unspecified) => {
            return Err(LinuxError::Protocol(format!(
                "exec reconciliation returned unknown outcome {}",
                response.outcome
            )));
        }
        Ok(outcome) => {
            return Err(LinuxError::Protocol(format!(
                "exec reconciliation returned invalid fields for {outcome:?}: exec_id={}, exit_code={}, forced={}",
                response.exec_id, response.exit_code, response.forced
            )));
        }
    }
    Ok(())
}

fn validate_allocated_exec_request_id(request_id: &str) -> Result<(), LinuxError> {
    let (encoded_boot_id, sequence) = request_id
        .strip_prefix("exec_req_")
        .and_then(|value| value.split_once('_'))
        .ok_or_else(|| {
            LinuxError::Protocol("allocated exec request ID has an invalid shape".to_string())
        })?;
    let boot_id = uuid::Uuid::parse_str(encoded_boot_id).map_err(|_| {
        LinuxError::Protocol("allocated exec request ID has an invalid boot UUID".to_string())
    })?;
    if encoded_boot_id.len() != 36
        || boot_id.get_version_num() != 4
        || boot_id.get_variant() != uuid::Variant::RFC4122
        || boot_id.to_string() != encoded_boot_id
        || sequence.len() != 16
        || !sequence
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || u64::from_str_radix(sequence, 16)
            .ok()
            .filter(|value| *value != 0)
            .is_none()
    {
        return Err(LinuxError::Protocol(
            "allocated exec request ID is not a canonical boot ticket".to_string(),
        ));
    }
    Ok(())
}

fn validate_exec_event_metadata(
    last_sequence: &mut u64,
    expected_request_id: &mut Option<String>,
    sequence: u64,
    request_id: &str,
) -> Result<(), LinuxError> {
    if sequence == 0 {
        return Err(LinuxError::Protocol(
            "exec event omitted required sequence".to_string(),
        ));
    }
    if sequence <= *last_sequence {
        return Err(LinuxError::Protocol(format!(
            "exec event ordering violation: got sequence {sequence} after {last_sequence}"
        )));
    }
    *last_sequence = sequence;

    if request_id.is_empty() {
        return Err(LinuxError::Protocol(
            "exec event omitted required request_id".to_string(),
        ));
    }
    if let Some(expected) = expected_request_id {
        if expected != request_id {
            return Err(LinuxError::Protocol(format!(
                "exec request_id mismatch: expected `{expected}`, got `{request_id}`"
            )));
        }
    } else {
        *expected_request_id = Some(request_id.to_string());
    }

    Ok(())
}

impl GrpcAgentClient {
    /// Connect only to this guest's already-provisioned private Engine socket.
    pub async fn docker_forward(&mut self) -> Result<crate::GrpcDockerStream, LinuxError> {
        use vz_agent_proto::{DockerForwardFrame, DockerForwardOpen, docker_forward_frame::Frame};
        let (sender, receiver) = mpsc::channel(8);
        sender
            .send(DockerForwardFrame {
                frame: Some(Frame::Open(DockerForwardOpen {
                    metadata: Some(self.next_transport_metadata(None)),
                })),
            })
            .await
            .map_err(|_| LinuxError::Protocol("Docker open channel closed".to_string()))?;
        let handshake = async {
            let mut inbound = self
                .agent
                .docker_forward(tokio_stream::wrappers::ReceiverStream::new(receiver))
                .await?
                .into_inner();
            match inbound.message().await? {
                Some(DockerForwardFrame {
                    frame: Some(Frame::Connected(_)),
                }) => Ok(crate::GrpcDockerStream::new(inbound, sender)),
                _ => Err(LinuxError::Protocol(
                    "Docker relay did not acknowledge connection".to_string(),
                )),
            }
        };
        tokio::time::timeout(CONNECT_TIMEOUT, handshake)
            .await
            .map_err(|_| LinuxError::Protocol("Docker relay connection timed out".to_string()))?
    }

    /// Runtime capability declaration for this gRPC guest path.
    pub fn advertised_runtime_capabilities() -> RuntimeCapabilities {
        vz_runtime_contract::canonical_backend_capabilities(
            &vz_runtime_contract::SandboxBackend::LinuxFirecracker,
        )
    }

    /// Enforce checkpoint class capability gating before guest operations.
    pub fn ensure_checkpoint_class_supported_for_guest(
        class: CheckpointClass,
        operation: RuntimeOperation,
    ) -> Result<(), LinuxError> {
        contract_ensure_checkpoint_class_supported(
            Self::advertised_runtime_capabilities(),
            class,
            operation,
        )
        .map_err(|err| LinuxError::Protocol(err.to_string()))
    }

    /// Establish a gRPC channel over vsock to the guest agent.
    ///
    /// Connects to the given VM's vsock device on `port` (default 7424)
    /// and creates all three service clients over the shared channel.
    ///
    /// Accepts `Arc<Vm>` because the vsock connector closure must
    /// own a reference to the VM across reconnections.
    pub async fn connect(vm: Arc<Vm>, port: u32) -> Result<Self, LinuxError> {
        let channel = create_vsock_channel(vm, port).await?;

        Ok(Self {
            agent: AgentServiceClient::new(channel.clone()),
            oci: OciServiceClient::new(channel.clone()),
            network: NetworkServiceClient::new(channel),
            next_request_sequence: 0,
        })
    }

    fn next_transport_metadata(
        &mut self,
        operation: Option<RuntimeOperation>,
    ) -> ProtoTransportMetadata {
        self.next_request_sequence = self.next_request_sequence.saturating_add(1);
        let (request_id, idempotency_key) = vz_runtime_contract::transport_metadata_for_sequence(
            self.next_request_sequence,
            operation,
        );
        let normalized = ContractRequestMetadata::new(Some(request_id), idempotency_key);

        ProtoTransportMetadata {
            request_id: normalized.request_id.unwrap_or_default(),
            idempotency_key: normalized.idempotency_key.unwrap_or_default(),
        }
    }

    fn container_exec_metadata(&mut self, request_id: String) -> ProtoTransportMetadata {
        let mut metadata = self.next_transport_metadata(Some(RuntimeOperation::ExecContainer));
        metadata.request_id = request_id.clone();
        metadata.idempotency_key = format!("exec_container:{request_id}");
        metadata
    }

    /// Allocate a guest-incarnation-bound request ticket. Allocation cannot
    /// spawn work, so any transport failure is a definite pre-dispatch error.
    pub async fn prepare_container_exec_request(&mut self) -> Result<String, LinuxError> {
        let metadata = self.next_transport_metadata(None);
        let response = tokio::time::timeout(
            ALLOCATE_EXEC_REQUEST_TIMEOUT,
            self.agent
                .allocate_exec_request(AllocateExecRequestRequest {
                    metadata: Some(metadata),
                }),
        )
        .await
        .map_err(|_| {
            LinuxError::Protocol(format!(
                "exec request allocation timed out after {:.3}s before dispatch",
                ALLOCATE_EXEC_REQUEST_TIMEOUT.as_secs_f64()
            ))
        })??
        .into_inner();
        validate_allocated_exec_request_id(&response.exec_request_id)?;
        Ok(response.exec_request_id)
    }

    /// Allocate a single-use ticket for supervised execution in this Machine.
    pub async fn prepare_machine_exec_request(&mut self) -> Result<String, LinuxError> {
        self.prepare_container_exec_request().await
    }

    /// Start ticketed execution in the Machine, without any OCI target.
    /// `pty` contains `(rows, cols)`; `None` selects separate output pipes.
    /// Ambiguous errors require reconciliation of this exact request ticket.
    #[allow(clippy::too_many_arguments)]
    pub async fn exec_machine_stream_ready_for_request(
        &mut self,
        dispatch_gate: ContainerExecDispatchGate,
        request_id: String,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
        pty: Option<(u32, u32)>,
    ) -> Result<(GrpcExecStream, u64), ContainerExecStartError> {
        validate_allocated_exec_request_id(&request_id)
            .map_err(ContainerExecStartError::Definite)?;
        let metadata = ProtoTransportMetadata {
            request_id: request_id.clone(),
            idempotency_key: format!("exec_machine:{request_id}"),
        };
        let terminal = pty.map_or_else(ExecTerminal::default, |(rows, cols)| ExecTerminal {
            allocate_pty: true,
            rows,
            cols,
        });
        let mut request = build_exec_request(command, args, options, None, metadata, terminal);
        request.supervised_machine = true;
        if !dispatch_gate.authorize_dispatch() {
            return Err(ContainerExecStartError::Definite(LinuxError::Protocol(
                "Machine exec was cancelled or timed out before dispatch".to_string(),
            )));
        }
        let response = self
            .agent
            .exec(request)
            .await
            .map_err(classify_exec_rpc_status)?;
        GrpcExecStream::new(response.into_inner(), Some(request_id))
            .wait_machine_ready()
            .await
    }

    /// Fence or cancel/reap an ambiguously-started exec by its prepared request ID.
    pub async fn reconcile_exec_request(
        &mut self,
        exec_request_id: String,
    ) -> Result<ReconcileExecResponse, LinuxError> {
        let metadata = self.next_transport_metadata(None);
        let response = self
            .agent
            .reconcile_exec(ReconcileExecRequest {
                exec_request_id: exec_request_id.clone(),
                metadata: Some(metadata),
            })
            .await?
            .into_inner();
        validate_reconcile_exec_response(&response, &exec_request_id)?;
        Ok(response)
    }

    /// Establish a gRPC channel using the default agent port.
    pub async fn connect_default(vm: Arc<Vm>) -> Result<Self, LinuxError> {
        Self::connect(vm, GRPC_AGENT_PORT).await
    }

    /// Health-check ping.
    pub async fn ping(&mut self) -> Result<(), LinuxError> {
        self.agent.ping(PingRequest {}).await?;
        Ok(())
    }

    /// Query guest system information.
    pub async fn system_info(&mut self) -> Result<SystemInfoResponse, LinuxError> {
        let response = self.agent.system_info(SystemInfoRequest {}).await?;
        Ok(response.into_inner())
    }

    /// Query guest resource usage statistics.
    pub async fn resource_stats(&mut self) -> Result<ResourceStatsResponse, LinuxError> {
        let response = self.agent.resource_stats(ResourceStatsRequest {}).await?;
        Ok(response.into_inner())
    }

    /// Stream ordered shutdown and persistent-filesystem closure for the exact
    /// request. A missing terminal receipt is not permission to power-stop.
    pub async fn shutdown_docker_stream(
        &mut self,
        request_id: String,
    ) -> Result<tonic::Streaming<vz_agent_proto::DockerShutdownEvent>, LinuxError> {
        Ok(self
            .agent
            .shutdown_docker(vz_agent_proto::DockerShutdownRequest { request_id })
            .await?
            .into_inner())
    }

    /// Explicitly trigger lazy Docker facade supervision and stream startup progress.
    ///
    /// This hook is intentionally separate from all native OCI calls. The host
    /// Docker socket proxy invokes it on first facade use.
    pub async fn ensure_docker_stream(
        &mut self,
    ) -> Result<tonic::Streaming<DockerEnsureEvent>, LinuxError> {
        let metadata = self.next_transport_metadata(None);
        let response = self
            .agent
            .ensure_docker(DockerEnsureRequest {
                metadata: Some(metadata),
            })
            .await?;
        Ok(response.into_inner())
    }

    /// Execute a command in the guest and return a streaming handle.
    ///
    /// Unlike [`exec`](Self::exec), this does not buffer the output.
    /// Returns a [`GrpcExecStream`] that yields [`vz::protocol::ExecEvent`]
    /// values matching the legacy protocol API.
    pub async fn exec_stream(
        &mut self,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
    ) -> Result<GrpcExecStream, LinuxError> {
        self.exec_stream_direct(command, args, options).await
    }

    async fn exec_stream_direct(
        &mut self,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
    ) -> Result<GrpcExecStream, LinuxError> {
        let metadata = self.next_transport_metadata(Some(RuntimeOperation::ExecContainer));
        let expected_request_id = if metadata.request_id.is_empty() {
            None
        } else {
            Some(metadata.request_id.clone())
        };

        let request = build_exec_request(
            command,
            args,
            options,
            None,
            metadata,
            ExecTerminal::default(),
        );

        let response = self.agent.exec(request).await?;
        Ok(GrpcExecStream::new(
            response.into_inner(),
            expected_request_id,
        ))
    }

    /// Start a container-targeted pipe exec using an identity the caller owns.
    /// On an ambiguous result, the caller must reconcile this exact request ID
    /// while retaining the VM and any lifecycle/admission authority.
    pub async fn exec_container_stream_ready_for_request(
        &mut self,
        dispatch_gate: ContainerExecDispatchGate,
        request_id: String,
        container_id: String,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
    ) -> Result<(GrpcExecStream, u64, ContainerGeneration), ContainerExecStartError> {
        let command_for_fault = command.clone();
        let metadata = self.container_exec_metadata(request_id.clone());
        let expected_request_id = Some(request_id);
        let request = build_exec_request(
            command,
            args,
            options,
            Some(container_id.clone()),
            metadata,
            ExecTerminal::default(),
        );
        if !dispatch_gate.authorize_dispatch() {
            return Err(ContainerExecStartError::Definite(LinuxError::Protocol(
                "container exec was cancelled or timed out before dispatch".to_string(),
            )));
        }
        let response = self
            .agent
            .exec(request)
            .await
            .map_err(classify_exec_rpc_status)?;
        if inject_container_exec_response_loss(&command_for_fault).await {
            drop(response);
            return Err(ContainerExecStartError::Ambiguous(LinuxError::Protocol(
                "test-injected container exec response loss before readiness".to_string(),
            )));
        }
        GrpcExecStream::new(response.into_inner(), expected_request_id)
            .wait_container_ready(&container_id)
            .await
    }

    /// Execute `buildctl` inside the guest and collect output.
    pub async fn buildctl(&mut self, args: Vec<String>) -> Result<ExecOutput, LinuxError> {
        self.buildctl_with_options(args, ExecOptions::default())
            .await
    }

    /// Execute `buildctl` inside the guest with explicit execution options.
    pub async fn buildctl_with_options(
        &mut self,
        args: Vec<String>,
        options: ExecOptions,
    ) -> Result<ExecOutput, LinuxError> {
        let (command, args) = buildctl_guest_command(args);
        let stream = self.exec_stream(command, args, options).await?;
        Ok(stream.collect().await)
    }

    /// Execute `buildctl` inside the guest and stream output events.
    pub async fn buildctl_stream(
        &mut self,
        args: Vec<String>,
    ) -> Result<GrpcExecStream, LinuxError> {
        self.buildctl_stream_with_options(args, ExecOptions::default())
            .await
    }

    /// Execute `buildctl` inside the guest with explicit options and streamed output.
    pub async fn buildctl_stream_with_options(
        &mut self,
        args: Vec<String>,
        options: ExecOptions,
    ) -> Result<GrpcExecStream, LinuxError> {
        let (command, args) = buildctl_guest_command(args);
        self.exec_stream(command, args, options).await
    }

    /// Create an OCI container from a prepared bundle.
    pub async fn oci_create(&mut self, id: String, bundle_path: String) -> Result<(), LinuxError> {
        let metadata = self.next_transport_metadata(Some(RuntimeOperation::CreateContainer));
        self.oci
            .create(OciCreateRequest {
                container_id: id,
                bundle_path,
                metadata: Some(metadata),
            })
            .await?;
        Ok(())
    }

    /// Start a previously created OCI container.
    pub async fn oci_start(&mut self, id: String) -> Result<(), LinuxError> {
        let metadata = self.next_transport_metadata(Some(RuntimeOperation::StartContainer));
        self.oci
            .start(OciStartRequest {
                container_id: id,
                metadata: Some(metadata),
            })
            .await?;
        Ok(())
    }

    /// Query runtime state for an OCI container.
    pub async fn oci_state(&mut self, id: String) -> Result<OciContainerState, LinuxError> {
        let debug = exec_control_debug_enabled();
        let metadata = self.next_transport_metadata(None);
        let request_id = metadata.request_id.clone();
        if debug {
            debug!(
                "[vz-linux grpc-client] oci_state rpc start container_id={} request_id={}",
                id, request_id
            );
        }
        let response_result = self
            .oci
            .state(OciStateRequest {
                container_id: id.clone(),
                metadata: Some(metadata),
            })
            .await;
        if debug {
            match &response_result {
                Ok(response) => {
                    let state = response.get_ref();
                    debug!(
                        "[vz-linux grpc-client] oci_state rpc complete container_id={} request_id={} status={} pid={}",
                        id, request_id, state.status, state.pid
                    );
                }
                Err(error) => debug!(
                    "[vz-linux grpc-client] oci_state rpc failed container_id={} request_id={} error={error}",
                    id, request_id
                ),
            }
        }
        let response = response_result?;
        let state = response.into_inner();
        Ok(OciContainerState {
            id: state.container_id,
            status: state.status,
            pid: if state.pid > 0 { Some(state.pid) } else { None },
            bundle_path: if state.bundle_path.is_empty() {
                None
            } else {
                Some(state.bundle_path)
            },
        })
    }

    /// Legacy OCI unary exec is retired because it cannot expose supervised
    /// process identity or terminal cleanup receipts.
    #[deprecated(note = "use exec_container_stream_ready and collect the supervised stream")]
    pub async fn oci_exec(
        &mut self,
        _id: String,
        _command: String,
        _args: Vec<String>,
        _options: OciExecOptions,
    ) -> Result<ExecOutput, LinuxError> {
        Err(retired_oci_exec_error())
    }

    /// Send a signal to a running OCI container.
    pub async fn oci_kill(&mut self, id: String, signal: String) -> Result<(), LinuxError> {
        let metadata = self.next_transport_metadata(Some(RuntimeOperation::StopContainer));
        self.oci
            .kill(OciKillRequest {
                container_id: id,
                signal,
                metadata: Some(metadata),
            })
            .await?;
        Ok(())
    }

    /// Delete an OCI container from runtime state.
    pub async fn oci_delete(&mut self, id: String, force: bool) -> Result<(), LinuxError> {
        let metadata = self.next_transport_metadata(Some(RuntimeOperation::RemoveContainer));
        self.oci
            .delete(OciDeleteRequest {
                container_id: id,
                force,
                metadata: Some(metadata),
            })
            .await?;
        Ok(())
    }

    /// Set up per-service network isolation inside a shared stack VM.
    pub async fn network_setup(
        &mut self,
        stack_id: String,
        services: Vec<vz_agent_proto::NetworkServiceConfig>,
    ) -> Result<(), LinuxError> {
        let metadata = self.next_transport_metadata(Some(RuntimeOperation::CreateNetworkDomain));
        self.network
            .setup(NetworkSetupRequest {
                stack_id,
                services,
                metadata: Some(metadata),
            })
            .await?;
        Ok(())
    }

    /// Tear down per-service network resources.
    pub async fn network_teardown(
        &mut self,
        stack_id: String,
        service_names: Vec<String>,
    ) -> Result<(), LinuxError> {
        let metadata = self.next_transport_metadata(None);
        self.network
            .teardown(NetworkTeardownRequest {
                stack_id,
                service_names,
                metadata: Some(metadata),
            })
            .await?;
        Ok(())
    }

    /// Open a bidirectional port forward stream to a guest-local target.
    ///
    /// Returns a [`GrpcPortForwardStream`] that implements
    /// [`tokio::io::AsyncRead`] + [`tokio::io::AsyncWrite`], suitable for
    /// use with [`tokio::io::copy_bidirectional`].
    pub async fn port_forward(
        &mut self,
        target_port: u16,
        protocol: &str,
        target_host: Option<&str>,
    ) -> Result<GrpcPortForwardStream, LinuxError> {
        let (tx, rx) = mpsc::channel::<PortForwardFrame>(64);

        // Send the open frame as the first message.
        let metadata = self.next_transport_metadata(None);
        let open_frame = PortForwardFrame {
            frame: Some(port_forward_frame::Frame::Open(PortForwardOpen {
                target_port: u32::from(target_port),
                protocol: protocol.to_string(),
                target_host: target_host.unwrap_or_default().to_string(),
                metadata: Some(metadata),
            })),
        };
        tx.send(open_frame).await.map_err(|_| {
            LinuxError::Protocol("failed to send port forward open frame".to_string())
        })?;

        let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);
        let response = self.agent.port_forward(outbound).await?;
        let inbound = response.into_inner();

        Ok(GrpcPortForwardStream::new(inbound, tx))
    }

    /// Write data to a running exec's stdin.
    pub async fn stdin_write(&mut self, exec_id: u64, data: &[u8]) -> Result<(), LinuxError> {
        let debug = exec_control_debug_enabled();
        if debug {
            debug!(
                "[vz-linux grpc-client] stdin_write rpc start exec_id={exec_id} bytes={}",
                data.len()
            );
        }
        let metadata = self.next_transport_metadata(None);
        let rpc_result = self
            .agent
            .stdin_write(StdinWriteRequest {
                exec_id,
                data: data.to_vec(),
                metadata: Some(metadata),
            })
            .await;
        if debug {
            match &rpc_result {
                Ok(_) => {
                    debug!("[vz-linux grpc-client] stdin_write rpc complete exec_id={exec_id}")
                }
                Err(error) => debug!(
                    "[vz-linux grpc-client] stdin_write rpc failed exec_id={exec_id} error={error}"
                ),
            }
        }
        rpc_result?;
        Ok(())
    }

    /// Send a signal to a running exec process.
    pub async fn signal(&mut self, exec_id: u64, signal: i32) -> Result<(), LinuxError> {
        let metadata = self.next_transport_metadata(None);
        self.agent
            .signal(SignalRequest {
                exec_id,
                signal,
                metadata: Some(metadata),
            })
            .await?;
        Ok(())
    }

    /// Cancel an exec and wait for the guest to report terminal/reaped state.
    pub async fn cancel_exec(&mut self, exec_id: u64) -> Result<CancelExecResponse, LinuxError> {
        let metadata = self.next_transport_metadata(None);
        let response = self
            .agent
            .cancel_exec(CancelExecRequest {
                exec_id,
                metadata: Some(metadata),
            })
            .await?;
        Ok(response.into_inner())
    }

    /// Close a running exec's stdin.
    pub async fn stdin_close(&mut self, exec_id: u64) -> Result<(), LinuxError> {
        let metadata = self.next_transport_metadata(None);
        self.agent
            .stdin_close(StdinCloseRequest {
                exec_id,
                metadata: Some(metadata),
            })
            .await?;
        Ok(())
    }

    /// Resize the PTY window for a running interactive exec session.
    pub async fn resize_exec_pty(
        &mut self,
        exec_id: u64,
        rows: u32,
        cols: u32,
    ) -> Result<(), LinuxError> {
        let metadata = self.next_transport_metadata(None);
        self.agent
            .resize_exec_pty(ResizeExecPtyRequest {
                exec_id,
                rows,
                cols,
                metadata: Some(metadata),
            })
            .await?;
        Ok(())
    }

    /// Execute a command with PTY allocation and return a streaming handle + exec_id.
    ///
    /// Unlike [`exec_stream`](Self::exec_stream), this allocates a PTY on the guest
    /// and returns the exec_id needed for stdin_write, signal, and resize_exec_pty.
    pub async fn exec_stream_interactive(
        &mut self,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
        rows: u32,
        cols: u32,
    ) -> Result<(GrpcExecStream, u64), LinuxError> {
        self.exec_stream_interactive_with_target(
            command, args, options, None, None, None, rows, cols,
        )
        .await
        .map_err(ContainerExecStartError::into_inner)
    }

    #[allow(clippy::too_many_arguments)]
    async fn exec_stream_interactive_with_target(
        &mut self,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
        container_target: Option<String>,
        prepared_request_id: Option<String>,
        dispatch_gate: Option<ContainerExecDispatchGate>,
        rows: u32,
        cols: u32,
    ) -> Result<(GrpcExecStream, u64), ContainerExecStartError> {
        let debug = exec_control_debug_enabled();
        let metadata = match (container_target.is_some(), prepared_request_id) {
            (true, Some(request_id)) => self.container_exec_metadata(request_id),
            (true, None) => {
                return Err(ContainerExecStartError::Definite(LinuxError::Protocol(
                    "container PTY exec requires an explicit request identity".to_string(),
                )));
            }
            (false, Some(_)) => {
                return Err(ContainerExecStartError::Definite(LinuxError::Protocol(
                    "ordinary PTY exec must not carry a container request identity".to_string(),
                )));
            }
            (false, None) => self.next_transport_metadata(Some(RuntimeOperation::ExecContainer)),
        };
        let request_id = metadata.request_id.clone();
        let expected_request_id = if metadata.request_id.is_empty() {
            None
        } else {
            Some(metadata.request_id.clone())
        };
        let command_debug = command.clone();
        let args_debug = args.clone();
        if debug {
            debug!(
                "[vz-linux grpc-client] exec_stream_interactive rpc start request_id={} command={:?} args={:?} rows={} cols={}",
                request_id, command_debug, args_debug, rows, cols
            );
        }

        let expected_container_id = container_target.clone();
        let request = build_exec_request(
            command,
            args,
            options,
            container_target,
            metadata,
            ExecTerminal {
                allocate_pty: true,
                rows,
                cols,
            },
        );

        if let Some(dispatch_gate) = dispatch_gate
            && !dispatch_gate.authorize_dispatch()
        {
            return Err(ContainerExecStartError::Definite(LinuxError::Protocol(
                "container PTY exec was cancelled or timed out before dispatch".to_string(),
            )));
        }

        let response_result =
            tokio::time::timeout(std::time::Duration::from_secs(10), self.agent.exec(request))
                .await;
        if debug {
            match &response_result {
                Ok(Ok(_)) => debug!(
                    "[vz-linux grpc-client] exec_stream_interactive rpc accepted request_id={}",
                    request_id
                ),
                Ok(Err(error)) => debug!(
                    "[vz-linux grpc-client] exec_stream_interactive rpc failed request_id={} error={error}",
                    request_id
                ),
                Err(_) => debug!(
                    "[vz-linux grpc-client] exec_stream_interactive rpc timeout waiting for headers request_id={}",
                    request_id
                ),
            }
        }
        let response = match response_result {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                return Err(if expected_container_id.is_some() {
                    classify_exec_rpc_status(error)
                } else {
                    ContainerExecStartError::Definite(error.into())
                });
            }
            Err(_) => {
                let error = LinuxError::Protocol(
                    "timeout waiting for interactive exec RPC headers from guest".to_string(),
                );
                return Err(if expected_container_id.is_some() {
                    ContainerExecStartError::Ambiguous(error)
                } else {
                    ContainerExecStartError::Definite(error)
                });
            }
        };
        if expected_container_id.is_some()
            && inject_container_exec_response_loss(&command_debug).await
        {
            drop(response);
            return Err(ContainerExecStartError::Ambiguous(LinuxError::Protocol(
                "test-injected container exec response loss before readiness".to_string(),
            )));
        }
        let inner_stream = response.into_inner();

        let interactive_result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            GrpcExecStream::new_interactive(
                inner_stream,
                expected_request_id,
                expected_container_id.as_deref(),
            ),
        )
        .await;
        let (stream, exec_id) = match interactive_result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                if debug {
                    debug!(
                        "[vz-linux grpc-client] exec_stream_interactive initial event error request_id={} error={error}",
                        request_id
                    );
                }
                return Err(error);
            }
            Err(_) => {
                if debug {
                    debug!(
                        "[vz-linux grpc-client] exec_stream_interactive initial event timeout request_id={}",
                        request_id
                    );
                }
                let error = LinuxError::Protocol(
                    "timeout waiting for initial exec event from guest".to_string(),
                );
                return Err(if expected_container_id.is_some() {
                    ContainerExecStartError::Ambiguous(error)
                } else {
                    ContainerExecStartError::Definite(error)
                });
            }
        };
        if debug {
            debug!(
                "[vz-linux grpc-client] exec_stream_interactive ready request_id={} exec_id={}",
                request_id, exec_id
            );
        }

        Ok((stream, exec_id))
    }

    /// Start a container-targeted PTY exec using an identity the caller owns.
    /// Ambiguous results carry no release proof; the caller must reconcile the
    /// supplied request ID before releasing lifecycle authority.
    #[allow(clippy::too_many_arguments)]
    pub async fn exec_container_stream_interactive_ready_for_request(
        &mut self,
        dispatch_gate: ContainerExecDispatchGate,
        request_id: String,
        container_id: String,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
        rows: u32,
        cols: u32,
    ) -> Result<(GrpcExecStream, u64, ContainerGeneration), ContainerExecStartError> {
        let (stream, exec_id) = self
            .exec_stream_interactive_with_target(
                command,
                args,
                options,
                Some(container_id),
                Some(request_id),
                Some(dispatch_gate),
                rows,
                cols,
            )
            .await?;
        let generation = stream.container_generation.clone().ok_or_else(|| {
            ContainerExecStartError::Ambiguous(LinuxError::Protocol(
                "container PTY exec omitted ready generation".to_string(),
            ))
        })?;
        Ok((stream, exec_id, generation))
    }
}

/// A stream of exec events from a gRPC-based command execution.
///
/// Yields [`vz::protocol::ExecEvent`] values (Stdout, Stderr, Exit).
pub struct GrpcExecStream {
    inner: tonic::Streaming<vz_agent_proto::ExecEvent>,
    done: bool,
    last_sequence: u64,
    expected_request_id: Option<String>,
    /// Buffered first proto event consumed during interactive session setup.
    buffered_first: Option<vz_agent_proto::ExecEvent>,
    container_generation: Option<ContainerGeneration>,
    /// Container-ready streams pin every subsequent frame to this logical exec.
    expected_exec_id: Option<u64>,
}

enum ExecStreamReadError {
    Protocol(LinuxError),
    Transport(LinuxError),
}

fn decode_exec_stream_event(
    last_sequence: &mut u64,
    expected_request_id: &mut Option<String>,
    expected_exec_id: Option<u64>,
    proto_event: vz_agent_proto::ExecEvent,
) -> Result<Option<vz::protocol::ExecEvent>, LinuxError> {
    if let Some(expected_exec_id) = expected_exec_id
        && (proto_event.exec_id == 0 || proto_event.exec_id != expected_exec_id)
    {
        return Err(LinuxError::Protocol(format!(
            "container exec_id mismatch: expected {expected_exec_id}, got {}",
            proto_event.exec_id
        )));
    }
    validate_exec_event_metadata(
        last_sequence,
        expected_request_id,
        proto_event.sequence,
        proto_event.request_id.as_str(),
    )?;
    match proto_event.event {
        Some(exec_event::Event::Stdout(data)) => Ok(Some(vz::protocol::ExecEvent::Stdout(data))),
        Some(exec_event::Event::Stderr(data)) => Ok(Some(vz::protocol::ExecEvent::Stderr(data))),
        Some(exec_event::Event::ExitCode(code)) => Ok(Some(vz::protocol::ExecEvent::Exit(code))),
        Some(exec_event::Event::Error(detail)) => Err(LinuxError::Protocol(format!(
            "exec stream reported an error: {detail}"
        ))),
        Some(exec_event::Event::ContainerReady(_)) => Err(LinuxError::Protocol(
            "exec stream repeated container readiness".to_string(),
        )),
        Some(exec_event::Event::MachineReady(_)) => Err(LinuxError::Protocol(
            "exec stream repeated Machine readiness".to_string(),
        )),
        None => Ok(None),
    }
}

impl ExecStreamReadError {
    fn error(self) -> LinuxError {
        match self {
            Self::Protocol(error) | Self::Transport(error) => error,
        }
    }
}

#[derive(Default)]
struct CheckedExecCollection {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl CheckedExecCollection {
    fn accept(
        &mut self,
        event: Result<Option<vz::protocol::ExecEvent>, LinuxError>,
    ) -> Result<Option<ExecOutput>, LinuxError> {
        match event? {
            Some(vz::protocol::ExecEvent::Stdout(data)) => self.stdout.extend(data),
            Some(vz::protocol::ExecEvent::Stderr(data)) => self.stderr.extend(data),
            Some(vz::protocol::ExecEvent::Exit(exit_code)) => {
                return Ok(Some(ExecOutput {
                    exit_code,
                    stdout: String::from_utf8_lossy(&self.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&self.stderr).into_owned(),
                }));
            }
            None => {
                return Err(LinuxError::Protocol(
                    "exec stream ended without a guest-reported exit code".to_string(),
                ));
            }
        }
        Ok(None)
    }
}

fn require_machine_ready(event: &vz_agent_proto::ExecEvent) -> Result<(), LinuxError> {
    if event.exec_id == 0
        || event.sequence != 1
        || event.request_id.is_empty()
        || !matches!(event.event, Some(exec_event::Event::MachineReady(_)))
    {
        return Err(LinuxError::Protocol(
            "Machine exec omitted typed readiness or its control identity".to_string(),
        ));
    }
    Ok(())
}

fn require_container_ready(
    event: &vz_agent_proto::ExecEvent,
    expected_container_id: &str,
) -> Result<ContainerGeneration, LinuxError> {
    let exec_event::Event::ContainerReady(ready) = event.event.as_ref().ok_or_else(|| {
        LinuxError::Protocol("container exec omitted its readiness event".to_string())
    })?
    else {
        return Err(LinuxError::Protocol(
            "container exec produced output before readiness".to_string(),
        ));
    };
    let generation = ready.generation.clone().ok_or_else(|| {
        LinuxError::Protocol("container readiness omitted generation identity".to_string())
    })?;
    let complete_objects = generation
        .cgroup
        .as_ref()
        .is_some_and(|identity| identity.inode != 0)
        && generation
            .root
            .as_ref()
            .is_some_and(|identity| identity.inode != 0)
        && generation.namespaces.as_ref().is_some_and(|namespaces| {
            [
                namespaces.mount.as_ref(),
                namespaces.network.as_ref(),
                namespaces.pid.as_ref(),
                namespaces.ipc.as_ref(),
                namespaces.uts.as_ref(),
            ]
            .into_iter()
            .all(|identity| identity.is_some_and(|identity| identity.inode != 0))
        });
    if generation.container_id != expected_container_id
        || event.exec_id == 0
        || generation.init_pid == 0
        || generation.init_start_time == 0
        || generation.cgroup_path.is_empty()
        || !complete_objects
    {
        return Err(LinuxError::Protocol(
            "container readiness contained an incomplete or mismatched generation".to_string(),
        ));
    }
    Ok(generation)
}

fn definite_initial_exec_rejection(
    event: &vz_agent_proto::ExecEvent,
    expected_request_id: &str,
) -> Option<ContainerExecStartError> {
    match event.event.as_ref() {
        Some(exec_event::Event::Error(detail))
            if event.sequence == 1
                && event.request_id == expected_request_id
                && event.exec_id == 0 =>
        {
            Some(ContainerExecStartError::Definite(LinuxError::Protocol(
                format!("exec stream reported an error: {detail}"),
            )))
        }
        _ => None,
    }
}

impl GrpcExecStream {
    async fn wait_machine_ready(mut self) -> Result<(Self, u64), ContainerExecStartError> {
        let first = self
            .inner
            .message()
            .await
            .map_err(|error| ContainerExecStartError::Ambiguous(error.into()))?
            .ok_or_else(|| {
                ContainerExecStartError::Ambiguous(LinuxError::Protocol(
                    "Machine exec stream ended before readiness".to_string(),
                ))
            })?;
        validate_exec_event_metadata(
            &mut self.last_sequence,
            &mut self.expected_request_id,
            first.sequence,
            first.request_id.as_str(),
        )
        .map_err(ContainerExecStartError::Ambiguous)?;
        if let Some(rejection) = definite_initial_exec_rejection(
            &first,
            self.expected_request_id.as_deref().unwrap_or_default(),
        ) {
            return Err(rejection);
        }
        require_machine_ready(&first).map_err(ContainerExecStartError::Ambiguous)?;
        let exec_id = first.exec_id;
        self.expected_exec_id = Some(exec_id);
        Ok((self, exec_id))
    }

    async fn next_checked_inner(
        &mut self,
    ) -> Result<Option<vz::protocol::ExecEvent>, ExecStreamReadError> {
        if self.done {
            return Ok(None);
        }

        loop {
            let next_event = if let Some(buffered) = self.buffered_first.take() {
                Ok(Some(buffered))
            } else {
                self.inner.message().await
            };

            match next_event {
                Ok(Some(proto_event)) => {
                    let decoded = decode_exec_stream_event(
                        &mut self.last_sequence,
                        &mut self.expected_request_id,
                        self.expected_exec_id,
                        proto_event,
                    );
                    match decoded {
                        Ok(Some(event @ vz::protocol::ExecEvent::Exit(_))) => {
                            self.done = true;
                            return Ok(Some(event));
                        }
                        Ok(Some(event)) => return Ok(Some(event)),
                        Ok(None) => continue,
                        Err(error) => {
                            self.done = true;
                            return Err(ExecStreamReadError::Protocol(error));
                        }
                    }
                }
                Ok(None) => {
                    self.done = true;
                    return Ok(None);
                }
                Err(error) => {
                    self.done = true;
                    return Err(ExecStreamReadError::Transport(error.into()));
                }
            }
        }
    }

    /// Read the next exec event without conflating transport or protocol
    /// failures with a guest-reported terminal status.
    pub async fn next_checked(&mut self) -> Result<Option<vz::protocol::ExecEvent>, LinuxError> {
        self.next_checked_inner()
            .await
            .map_err(ExecStreamReadError::error)
    }

    /// Wrap a tonic streaming response.
    fn new(
        inner: tonic::Streaming<vz_agent_proto::ExecEvent>,
        expected_request_id: Option<String>,
    ) -> Self {
        Self {
            inner,
            done: false,
            last_sequence: 0,
            expected_request_id,
            buffered_first: None,
            container_generation: None,
            expected_exec_id: None,
        }
    }

    /// Create a new interactive exec stream, extracting exec_id from the first event.
    ///
    /// Returns the stream and the exec_id for subsequent stdin_write/resize operations.
    pub async fn new_interactive(
        mut inner: tonic::Streaming<vz_agent_proto::ExecEvent>,
        expected_request_id: Option<String>,
        expected_container_id: Option<&str>,
    ) -> Result<(Self, u64), ContainerExecStartError> {
        // Read the first event to extract exec_id.
        let first = inner
            .message()
            .await
            .map_err(|error| ContainerExecStartError::Ambiguous(error.into()))?
            .ok_or_else(|| {
                ContainerExecStartError::Ambiguous(LinuxError::Protocol(
                    "interactive exec stream empty".to_string(),
                ))
            })?;

        let mut stream = Self::new(inner, expected_request_id);
        if let Some(container_id) = expected_container_id {
            validate_exec_event_metadata(
                &mut stream.last_sequence,
                &mut stream.expected_request_id,
                first.sequence,
                first.request_id.as_str(),
            )
            .map_err(ContainerExecStartError::Ambiguous)?;
            let expected_request_id = stream.expected_request_id.as_deref().unwrap_or_default();
            if let Some(rejection) = definite_initial_exec_rejection(&first, expected_request_id) {
                return Err(rejection);
            }
            if first.exec_id == 0 {
                return Err(ContainerExecStartError::Ambiguous(LinuxError::Protocol(
                    "interactive container exec missing exec_id in first event".to_string(),
                )));
            }
            stream.container_generation = Some(
                require_container_ready(&first, container_id)
                    .map_err(ContainerExecStartError::Ambiguous)?,
            );
            let exec_id = first.exec_id;
            stream.expected_exec_id = Some(exec_id);
            Ok((stream, exec_id))
        } else {
            let exec_id = first.exec_id;
            if exec_id == 0 {
                return Err(ContainerExecStartError::Definite(LinuxError::Protocol(
                    "interactive exec missing exec_id in first event".to_string(),
                )));
            }
            stream.buffered_first = Some(first);
            Ok((stream, exec_id))
        }
    }

    async fn wait_container_ready(
        mut self,
        expected_container_id: &str,
    ) -> Result<(Self, u64, ContainerGeneration), ContainerExecStartError> {
        let first = self
            .inner
            .message()
            .await
            .map_err(|error| ContainerExecStartError::Ambiguous(error.into()))?
            .ok_or_else(|| {
                ContainerExecStartError::Ambiguous(LinuxError::Protocol(
                    "container exec stream ended before readiness".to_string(),
                ))
            })?;
        validate_exec_event_metadata(
            &mut self.last_sequence,
            &mut self.expected_request_id,
            first.sequence,
            first.request_id.as_str(),
        )
        .map_err(ContainerExecStartError::Ambiguous)?;
        let expected_request_id = self.expected_request_id.as_deref().unwrap_or_default();
        if let Some(rejection) = definite_initial_exec_rejection(&first, expected_request_id) {
            return Err(rejection);
        }
        let exec_id = first.exec_id;
        let generation = require_container_ready(&first, expected_container_id)
            .map_err(ContainerExecStartError::Ambiguous)?;
        self.container_generation = Some(generation.clone());
        self.expected_exec_id = Some(exec_id);
        Ok((self, exec_id, generation))
    }

    /// Exact guest-observed container generation pinned by this exec.
    pub fn container_generation(&self) -> Option<&ContainerGeneration> {
        self.container_generation.as_ref()
    }

    /// Read the next event from the stream.
    ///
    /// Returns `None` after the command has exited (after yielding
    /// [`ExecEvent::Exit`](vz::protocol::ExecEvent::Exit)).
    pub async fn next(&mut self) -> Option<vz::protocol::ExecEvent> {
        match self.next_checked_inner().await {
            Ok(event) => event,
            Err(ExecStreamReadError::Protocol(_)) => Some(vz::protocol::ExecEvent::Exit(-1)),
            Err(ExecStreamReadError::Transport(_)) => None,
        }
    }

    /// Collect all remaining events into an [`ExecOutput`].
    ///
    /// This legacy adapter loses protocol/transport diagnostics. Runtime
    /// decisions must use [`Self::collect_checked`] instead.
    pub async fn collect(mut self) -> ExecOutput {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut exit_code = -1;

        while let Some(event) = self.next().await {
            match event {
                vz::protocol::ExecEvent::Stdout(data) => stdout.extend_from_slice(&data),
                vz::protocol::ExecEvent::Stderr(data) => stderr.extend_from_slice(&data),
                vz::protocol::ExecEvent::Exit(code) => exit_code = code,
            }
        }

        ExecOutput {
            exit_code,
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        }
    }

    /// Collect a genuine guest terminal result without replacing spawn,
    /// protocol, transport, or truncated-stream failures with exit code -1.
    pub async fn collect_checked(mut self) -> Result<ExecOutput, LinuxError> {
        let mut collected = CheckedExecCollection::default();
        loop {
            if let Some(output) = collected.accept(self.next_checked().await)? {
                return Ok(output);
            }
        }
    }
}

/// A bidirectional port forward stream over gRPC.
///
/// Implements [`tokio::io::AsyncRead`] and [`tokio::io::AsyncWrite`] so it
/// can be used with [`tokio::io::copy_bidirectional`].
pub struct GrpcPortForwardStream {
    /// Inbound gRPC stream (data from guest).
    inbound: tonic::Streaming<PortForwardFrame>,
    /// Outbound sender (data to guest).
    outbound: mpsc::Sender<PortForwardFrame>,
    /// Buffered data from the most recent inbound frame.
    read_buf: Vec<u8>,
    /// Current read position within `read_buf`.
    read_pos: usize,
}

impl GrpcPortForwardStream {
    fn new(
        inbound: tonic::Streaming<PortForwardFrame>,
        outbound: mpsc::Sender<PortForwardFrame>,
    ) -> Self {
        Self {
            inbound,
            outbound,
            read_buf: Vec::new(),
            read_pos: 0,
        }
    }
}

impl tokio::io::AsyncRead for GrpcPortForwardStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        // If we have buffered data, return it first.
        if this.read_pos < this.read_buf.len() {
            let remaining = &this.read_buf[this.read_pos..];
            let to_copy = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..to_copy]);
            this.read_pos += to_copy;
            if this.read_pos >= this.read_buf.len() {
                this.read_buf.clear();
                this.read_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // Poll the inbound stream for the next frame.
        let message_future = this.inbound.message();
        tokio::pin!(message_future);
        match message_future.poll(cx) {
            Poll::Ready(Ok(Some(frame))) => {
                if let Some(port_forward_frame::Frame::Data(data)) = frame.frame {
                    let to_copy = data.len().min(buf.remaining());
                    buf.put_slice(&data[..to_copy]);
                    if to_copy < data.len() {
                        this.read_buf = data;
                        this.read_pos = to_copy;
                    }
                    Poll::Ready(Ok(()))
                } else {
                    // Non-data frame (e.g., Open) — treat as EOF.
                    Poll::Ready(Ok(()))
                }
            }
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(())), // Stream ended.
            Poll::Ready(Err(e)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                format!("gRPC port forward read error: {e}"),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl tokio::io::AsyncWrite for GrpcPortForwardStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let frame = PortForwardFrame {
            frame: Some(port_forward_frame::Frame::Data(buf.to_vec())),
        };
        let send_future = self.outbound.send(frame);
        tokio::pin!(send_future);
        match send_future.poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(buf.len())),
            Poll::Ready(Err(_)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "gRPC port forward channel closed",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Create a tonic [`Channel`] that connects over vsock to the guest.
///
/// Uses [`Endpoint::connect_with_connector`] with a custom service
/// function that opens a vsock connection and wraps it with
/// [`hyper_util::rt::TokioIo`] to satisfy hyper's I/O trait bounds.
async fn create_vsock_channel(vm: Arc<Vm>, port: u32) -> Result<Channel, LinuxError> {
    let channel = Endpoint::try_from("http://[::]:50051")
        .map_err(|e| LinuxError::Protocol(format!("failed to create gRPC endpoint: {e}")))?
        .connect_timeout(CONNECT_TIMEOUT)
        .connect_with_connector(service_fn(move |_: Uri| {
            let vm = Arc::clone(&vm);
            async move {
                let stream = vm.vsock_connect(port).await.map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        format!("vsock connect failed: {e}"),
                    )
                })?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .map_err(|e| LinuxError::GrpcTransport(Box::new(e)))?;

    Ok(channel)
}

fn buildctl_guest_command(args: Vec<String>) -> (String, Vec<String>) {
    let mut command_args = vec![
        "env".to_string(),
        "HOME=/root".to_string(),
        "DOCKER_CONFIG=/root/.docker".to_string(),
        GUEST_BUILDCTL_BINARY.to_string(),
    ];
    command_args.extend(args);
    (GUEST_BUSYBOX_BINARY.to_string(), command_args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_ready_requires_its_own_first_frame_and_control_identity() {
        let ready = vz_agent_proto::ExecEvent {
            event: Some(exec_event::Event::MachineReady(
                vz_agent_proto::MachineExecReady {},
            )),
            exec_id: 42,
            sequence: 1,
            request_id: "ticket".to_string(),
        };
        assert!(require_machine_ready(&ready).is_ok());
        for invalid in [
            vz_agent_proto::ExecEvent {
                exec_id: 0,
                ..ready.clone()
            },
            vz_agent_proto::ExecEvent {
                sequence: 2,
                ..ready.clone()
            },
            vz_agent_proto::ExecEvent {
                request_id: String::new(),
                ..ready.clone()
            },
            vz_agent_proto::ExecEvent {
                event: Some(exec_event::Event::Stdout(vec![])),
                ..ready.clone()
            },
            vz_agent_proto::ExecEvent {
                event: Some(exec_event::Event::ContainerReady(
                    vz_agent_proto::ContainerExecReady {
                        generation: Some(ready_generation("not-a-Machine")),
                    },
                )),
                ..ready.clone()
            },
        ] {
            assert!(require_machine_ready(&invalid).is_err());
        }
        assert!(require_container_ready(&ready, "not-a-container").is_err());
    }

    #[test]
    fn machine_checked_stream_pins_identity_and_rejects_repeated_ready() {
        let event = |exec_id, request_id: &str, event| vz_agent_proto::ExecEvent {
            exec_id,
            request_id: request_id.to_string(),
            sequence: 2,
            event: Some(event),
        };
        for invalid in [
            event(43, "ticket", exec_event::Event::Stdout(vec![0, 255])),
            event(42, "other", exec_event::Event::ExitCode(0)),
            event(
                42,
                "ticket",
                exec_event::Event::MachineReady(vz_agent_proto::MachineExecReady {}),
            ),
        ] {
            assert!(
                decode_exec_stream_event(
                    &mut 1,
                    &mut Some("ticket".to_string()),
                    Some(42),
                    invalid
                )
                .is_err()
            );
        }
        let decoded = decode_exec_stream_event(
            &mut 1,
            &mut Some("ticket".to_string()),
            Some(42),
            event(42, "ticket", exec_event::Event::Stdout(vec![0, 255])),
        );
        assert!(
            matches!(decoded, Ok(Some(vz::protocol::ExecEvent::Stdout(bytes))) if bytes == [0,255])
        );
    }

    #[tokio::test]
    async fn cancellation_during_preflight_closes_dispatch_gate_before_send() {
        let gate =
            ContainerExecDispatchGate::new(tokio::time::Instant::now() + Duration::from_secs(1));
        let task_gate = gate.clone();
        let (release_preflight, preflight_blocked) = tokio::sync::oneshot::channel();
        let start = tokio::spawn(async move {
            let _ = preflight_blocked.await;
            task_gate.authorize_dispatch()
        });

        assert!(gate.cancel_before_dispatch());
        assert!(release_preflight.send(()).is_ok());
        assert!(
            matches!(start.await, Ok(false)),
            "cancelled request reached send gate"
        );
    }

    #[test]
    fn dispatch_authorization_owns_racing_cancellation() {
        let gate =
            ContainerExecDispatchGate::new(tokio::time::Instant::now() + Duration::from_secs(1));
        assert!(gate.authorize_dispatch());
        assert!(
            !gate.cancel_before_dispatch(),
            "authorized request must remain under request-ID reconciliation"
        );
        assert!(!gate.authorize_dispatch(), "dispatch is one-shot");
    }

    #[tokio::test]
    async fn expired_dispatch_gate_cannot_authorize_send() {
        let gate = ContainerExecDispatchGate::new(tokio::time::Instant::now());
        assert!(!gate.authorize_dispatch());
        assert!(gate.cancel_before_dispatch());
    }

    fn ready_generation(container_id: &str) -> ContainerGeneration {
        let object = || vz_agent_proto::KernelObjectIdentity {
            device: 8,
            inode: 42,
        };
        ContainerGeneration {
            container_id: container_id.to_string(),
            init_pid: 4242,
            init_start_time: 123_456,
            cgroup_path: "/youki/web".to_string(),
            cgroup: Some(object()),
            namespaces: Some(vz_agent_proto::ContainerNamespaceIdentity {
                mount: Some(object()),
                network: Some(object()),
                pid: Some(object()),
                ipc: Some(object()),
                uts: Some(object()),
            }),
            root: Some(object()),
        }
    }

    #[test]
    fn typed_container_readiness_requires_exact_complete_generation() {
        let generation = ready_generation("web");
        let event = vz_agent_proto::ExecEvent {
            event: Some(exec_event::Event::ContainerReady(
                vz_agent_proto::ContainerExecReady {
                    generation: Some(generation.clone()),
                },
            )),
            sequence: 1,
            request_id: "req-ready".to_string(),
            exec_id: 99,
        };
        assert_eq!(require_container_ready(&event, "web").unwrap(), generation);
        assert!(require_container_ready(&event, "replacement").is_err());

        let output_before_ready = vz_agent_proto::ExecEvent {
            event: Some(exec_event::Event::Stdout(Vec::new())),
            ..event.clone()
        };
        assert!(require_container_ready(&output_before_ready, "web").is_err());

        let missing_exec_identity = vz_agent_proto::ExecEvent {
            exec_id: 0,
            ..event
        };
        assert!(require_container_ready(&missing_exec_identity, "web").is_err());
    }

    #[test]
    fn cancel_receipt_retains_normalized_terminal_status() {
        let receipt = CancelExecResponse {
            exit_code: 143,
            forced: false,
        };
        assert_eq!(receipt.exit_code, 128 + 15);
        assert!(!receipt.forced);

        let forced = CancelExecResponse {
            exit_code: 137,
            forced: true,
        };
        assert_eq!(forced.exit_code, 128 + 9);
        assert!(forced.forced);
    }

    #[test]
    fn grpc_agent_port_matches_protocol_agent_port() {
        assert_eq!(GRPC_AGENT_PORT, vz::protocol::AGENT_PORT);
    }

    #[test]
    fn buildctl_guest_command_wraps_busybox_and_env() {
        let (command, args) = buildctl_guest_command(vec![
            "--addr".to_string(),
            "tcp://127.0.0.1:8372".to_string(),
            "debug".to_string(),
            "workers".to_string(),
        ]);

        assert_eq!(command, GUEST_BUSYBOX_BINARY);
        assert_eq!(args[0], "env");
        assert_eq!(args[1], "HOME=/root");
        assert_eq!(args[2], "DOCKER_CONFIG=/root/.docker");
        assert_eq!(args[3], GUEST_BUILDCTL_BINARY);
        assert_eq!(args[4], "--addr");
        assert_eq!(args[6], "debug");
    }

    #[test]
    fn ordinary_exec_request_has_no_container_target_and_preserves_argv() {
        let request = build_exec_request(
            "printf".to_string(),
            vec!["%s".to_string(), "a; $b ' c".to_string()],
            ExecOptions::default(),
            None,
            ProtoTransportMetadata::default(),
            ExecTerminal::default(),
        );

        assert!(request.container_target.is_none());
        assert_eq!(request.command, "printf");
        assert_eq!(request.args, ["%s", "a; $b ' c"]);
    }

    #[test]
    fn pipe_and_pty_requests_carry_the_same_typed_container_target() {
        let options = ExecOptions {
            working_dir: Some("/workspace".to_string()),
            env: vec![("MODE".to_string(), "test".to_string())],
            ..ExecOptions::default()
        };
        let pipe = build_exec_request(
            "/bin/tool".to_string(),
            vec!["--literal=$HOME;echo".to_string()],
            options.clone(),
            Some("machine-web".to_string()),
            ProtoTransportMetadata::default(),
            ExecTerminal::default(),
        );
        let pty = build_exec_request(
            "/bin/tool".to_string(),
            vec!["--literal=$HOME;echo".to_string()],
            options,
            Some("machine-web".to_string()),
            ProtoTransportMetadata::default(),
            ExecTerminal {
                allocate_pty: true,
                rows: 33,
                cols: 101,
            },
        );

        assert_eq!(pipe.container_target, pty.container_target);
        assert_eq!(
            pipe.container_target
                .as_ref()
                .map(|target| target.container_id.as_str()),
            Some("machine-web")
        );
        assert_eq!(pipe.command, pty.command);
        assert_eq!(pipe.args, pty.args);
        assert_eq!(pipe.working_dir, pty.working_dir);
        assert_eq!(pipe.env, pty.env);
        assert!(!pipe.allocate_pty);
        assert!(pty.allocate_pty);
    }

    #[test]
    fn legacy_unary_oci_exec_is_retired_fail_closed() {
        let message = retired_oci_exec_error().to_string();
        assert!(message.contains("OciService.Exec is retired"));
        assert!(message.contains("supervised AgentService.Exec"));
    }

    #[test]
    fn advertised_runtime_capabilities_gate_vm_full() {
        let capabilities = GrpcAgentClient::advertised_runtime_capabilities();
        assert!(capabilities.fs_quick_checkpoint);
        assert!(capabilities.checkpoint_fork);
        assert!(!capabilities.vm_full_checkpoint);
        assert!(!capabilities.docker_compat);
        assert!(capabilities.compose_adapter);
        assert!(!capabilities.gpu_passthrough);
        assert!(!capabilities.live_resize);
        assert!(capabilities.shared_vm);
        assert!(capabilities.stack_networking);
        assert!(capabilities.container_logs);
        vz_runtime_contract::validate_backend_adapter_contract_surface().unwrap();
        vz_runtime_contract::validate_backend_adapter_parity(capabilities).unwrap();
    }

    #[test]
    fn ensure_checkpoint_class_supported_for_guest_rejects_vm_full() {
        let err = GrpcAgentClient::ensure_checkpoint_class_supported_for_guest(
            CheckpointClass::VmFull,
            RuntimeOperation::CreateCheckpoint,
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("vm_full_checkpoint"));
        assert!(message.contains("create_checkpoint"));
    }

    #[test]
    fn validate_exec_event_metadata_accepts_monotonic_sequence() {
        let mut last_sequence = 0;
        let mut expected_request_id = Some("req_1".to_string());
        validate_exec_event_metadata(&mut last_sequence, &mut expected_request_id, 1, "req_1")
            .unwrap();
        validate_exec_event_metadata(&mut last_sequence, &mut expected_request_id, 2, "req_1")
            .unwrap();
        assert_eq!(last_sequence, 2);
    }

    #[test]
    fn validate_exec_event_metadata_rejects_out_of_order_sequence() {
        let mut last_sequence = 2;
        let mut expected_request_id = Some("req_1".to_string());
        let err =
            validate_exec_event_metadata(&mut last_sequence, &mut expected_request_id, 2, "req_1")
                .unwrap_err();
        assert!(err.to_string().contains("ordering violation"));
    }

    #[test]
    fn validate_exec_event_metadata_rejects_request_id_mismatch() {
        let mut last_sequence = 1;
        let mut expected_request_id = Some("req_1".to_string());
        let err =
            validate_exec_event_metadata(&mut last_sequence, &mut expected_request_id, 2, "req_2")
                .unwrap_err();
        assert!(err.to_string().contains("request_id mismatch"));
    }

    #[test]
    fn validate_exec_event_metadata_rejects_missing_revision_five_fields() {
        let mut last_sequence = 0;
        let mut expected_request_id = Some("req_1".to_string());
        let zero_sequence =
            validate_exec_event_metadata(&mut last_sequence, &mut expected_request_id, 0, "req_1")
                .unwrap_err();
        assert!(zero_sequence.to_string().contains("required sequence"));

        let empty_request_id =
            validate_exec_event_metadata(&mut last_sequence, &mut expected_request_id, 1, "")
                .unwrap_err();
        assert!(empty_request_id.to_string().contains("required request_id"));
    }

    #[test]
    fn allocated_container_exec_request_ids_require_canonical_boot_ticket() {
        let valid = "exec_req_00000000-0000-4000-8000-000000000001_0000000000000001";
        validate_allocated_exec_request_id(valid).unwrap();
        for invalid in [
            "exec_req_00000000-0000-4000-8000-000000000001",
            "exec_req_00000000-0000-4000-8000-000000000001_0000000000000000",
            "exec_req_00000000-0000-4000-8000-000000000001_000000000000000g",
            "exec_req_00000000-0000-4000-0000-000000000001_0000000000000001",
            "exec_req_00000000-0000-4000-8000-000000000001_1",
        ] {
            assert!(validate_allocated_exec_request_id(invalid).is_err());
        }
    }

    #[test]
    fn reconciliation_response_requires_exact_normalized_proof_fields() {
        use vz_agent_proto::reconcile_exec_response::Outcome;

        let request_id = "exec_req_00000000-0000-4000-8000-000000000004_0000000000000001";
        let fenced = ReconcileExecResponse {
            outcome: Outcome::FencedNeverStarted as i32,
            exec_request_id: request_id.to_string(),
            exec_id: 0,
            exit_code: 0,
            forced: false,
        };
        validate_reconcile_exec_response(&fenced, request_id).unwrap();

        let terminal = ReconcileExecResponse {
            outcome: Outcome::TerminalReaped as i32,
            exec_request_id: request_id.to_string(),
            exec_id: 42,
            exit_code: 137,
            forced: true,
        };
        validate_reconcile_exec_response(&terminal, request_id).unwrap();

        for invalid in [
            ReconcileExecResponse {
                exec_id: 1,
                ..fenced.clone()
            },
            ReconcileExecResponse {
                forced: true,
                ..fenced.clone()
            },
            ReconcileExecResponse {
                exec_id: 0,
                ..terminal.clone()
            },
            ReconcileExecResponse {
                exit_code: -1,
                ..terminal.clone()
            },
            ReconcileExecResponse {
                exit_code: 256,
                ..terminal.clone()
            },
            ReconcileExecResponse {
                exec_request_id: "different".to_string(),
                ..terminal
            },
        ] {
            assert!(validate_reconcile_exec_response(&invalid, request_id).is_err());
        }
    }

    #[test]
    fn response_loss_hook_consumes_only_after_exact_command_match() {
        let source = include_str!("grpc_client.rs");
        let hook = source
            .split_once("async fn inject_container_exec_response_loss")
            .unwrap()
            .1
            .split_once("fn validate_reconcile_exec_response")
            .unwrap()
            .0;
        assert!(hook.contains("expected != command"));
        assert!(hook.contains("VZ_TEST_DROP_CONTAINER_EXEC_RESPONSE_DWELL_MS"));
        assert!(hook.find("expected != command") < hook.find("INJECTED.swap"));
    }

    #[test]
    fn checked_collection_preserves_spawn_and_transport_errors_without_synthetic_exit() {
        let mut collection = CheckedExecCollection::default();
        let rejected = vz_agent_proto::ExecEvent {
            event: Some(exec_event::Event::Error("exec rejected before spawn: failed to spawn process: No such file or directory (os error 2)".into())),
            sequence: 1,
            request_id: "req_1".into(),
            exec_id: 0,
        };
        let decoded = decode_exec_stream_event(&mut 0, &mut Some("req_1".into()), None, rejected);
        let error = collection.accept(decoded).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("No such file or directory (os error 2)")
        );
        let error = collection
            .accept(Err(
                tonic::Status::unavailable("exact transport reset").into()
            ))
            .unwrap_err();
        assert!(error.to_string().contains("exact transport reset"));
    }

    #[test]
    fn checked_collection_requires_exit_after_output_and_preserves_real_nonzero_status() {
        use vz::protocol::ExecEvent;
        let mut collection = CheckedExecCollection::default();
        assert!(
            collection
                .accept(Ok(None))
                .unwrap_err()
                .to_string()
                .contains("without a guest-reported exit")
        );
        assert!(
            collection
                .accept(Ok(Some(ExecEvent::Stdout(b"partial".to_vec()))))
                .unwrap()
                .is_none()
        );
        assert!(collection.accept(Ok(None)).is_err());
        assert!(
            collection
                .accept(Ok(Some(ExecEvent::Stderr(b"stderr".to_vec()))))
                .unwrap()
                .is_none()
        );
        let result = collection
            .accept(Ok(Some(ExecEvent::Exit(17))))
            .unwrap()
            .unwrap();
        assert_eq!(result.exit_code, 17);
        assert_eq!(result.stdout, "partial");
        assert_eq!(result.stderr, "stderr");
    }

    #[test]
    fn exec_start_status_classification_is_conservative() {
        for status in [
            tonic::Status::invalid_argument("server rejection without stage proof"),
            tonic::Status::unavailable("transport lost"),
            tonic::Status::cancelled("caller lost"),
            tonic::Status::internal("server state unknown"),
        ] {
            assert!(classify_exec_rpc_status(status).is_ambiguous());
        }
    }

    #[test]
    fn only_exact_first_authenticated_error_frame_is_a_definite_start_rejection() {
        let rejected = vz_agent_proto::ExecEvent {
            event: Some(exec_event::Event::Error("spawn failed".to_string())),
            sequence: 1,
            request_id: "req_1".to_string(),
            exec_id: 0,
        };
        assert!(matches!(
            definite_initial_exec_rejection(&rejected, "req_1"),
            Some(ContainerExecStartError::Definite(_))
        ));

        let addressed_error = vz_agent_proto::ExecEvent {
            exec_id: 41,
            ..rejected.clone()
        };
        assert!(definite_initial_exec_rejection(&addressed_error, "req_1").is_none());

        let wrong_sequence = vz_agent_proto::ExecEvent {
            sequence: 2,
            ..rejected.clone()
        };
        assert!(definite_initial_exec_rejection(&wrong_sequence, "req_1").is_none());

        let wrong_request = vz_agent_proto::ExecEvent {
            request_id: "req_other".to_string(),
            ..rejected.clone()
        };
        assert!(definite_initial_exec_rejection(&wrong_request, "req_1").is_none());

        let malformed_ready = vz_agent_proto::ExecEvent {
            event: Some(exec_event::Event::ContainerReady(
                vz_agent_proto::ContainerExecReady {
                    generation: Some(ready_generation("web")),
                },
            )),
            ..rejected
        };
        assert!(definite_initial_exec_rejection(&malformed_ready, "req_1").is_none());
    }

    #[test]
    fn checked_exec_decode_never_synthesizes_terminal_status_for_protocol_failure() {
        let event = vz_agent_proto::ExecEvent {
            event: Some(exec_event::Event::Error("launcher lost".to_string())),
            sequence: 1,
            request_id: "req_1".to_string(),
            exec_id: 41,
        };
        let mut last_sequence = 0;
        let mut expected_request_id = Some("req_1".to_string());
        let error =
            decode_exec_stream_event(&mut last_sequence, &mut expected_request_id, None, event)
                .unwrap_err();
        assert!(error.to_string().contains("launcher lost"));

        let repeated_ready = vz_agent_proto::ExecEvent {
            event: Some(exec_event::Event::ContainerReady(
                vz_agent_proto::ContainerExecReady {
                    generation: Some(ready_generation("web")),
                },
            )),
            sequence: 2,
            request_id: "req_1".to_string(),
            exec_id: 41,
        };
        let error = decode_exec_stream_event(
            &mut last_sequence,
            &mut expected_request_id,
            None,
            repeated_ready,
        )
        .unwrap_err();
        assert!(error.to_string().contains("repeated container readiness"));
    }

    #[test]
    fn checked_exec_decode_preserves_only_genuine_exit_frames_as_terminal() {
        let event = vz_agent_proto::ExecEvent {
            event: Some(exec_event::Event::ExitCode(143)),
            sequence: 1,
            request_id: "req_1".to_string(),
            exec_id: 41,
        };
        let mut last_sequence = 0;
        let mut expected_request_id = Some("req_1".to_string());
        assert_eq!(
            decode_exec_stream_event(&mut last_sequence, &mut expected_request_id, None, event)
                .unwrap(),
            Some(vz::protocol::ExecEvent::Exit(143))
        );
    }

    #[test]
    fn container_exec_decode_rejects_missing_or_mismatched_exec_identity() {
        for (observed, expected_fragment) in [(0, "got 0"), (42, "got 42")] {
            let event = vz_agent_proto::ExecEvent {
                event: Some(exec_event::Event::ExitCode(0)),
                sequence: 2,
                request_id: "req_1".to_string(),
                exec_id: observed,
            };
            let mut last_sequence = 1;
            let mut expected_request_id = Some("req_1".to_string());
            let error = decode_exec_stream_event(
                &mut last_sequence,
                &mut expected_request_id,
                Some(41),
                event,
            )
            .unwrap_err();
            assert!(error.to_string().contains(expected_fragment));
            assert_eq!(
                last_sequence, 1,
                "identity must fail before ordering state changes"
            );
        }
    }

    #[test]
    fn container_exec_decode_accepts_matching_terminal_identity() {
        let event = vz_agent_proto::ExecEvent {
            event: Some(exec_event::Event::ExitCode(143)),
            sequence: 2,
            request_id: "req_1".to_string(),
            exec_id: 41,
        };
        let mut last_sequence = 1;
        let mut expected_request_id = Some("req_1".to_string());
        assert_eq!(
            decode_exec_stream_event(
                &mut last_sequence,
                &mut expected_request_id,
                Some(41),
                event,
            )
            .unwrap(),
            Some(vz::protocol::ExecEvent::Exit(143))
        );
    }

    #[test]
    fn ordinary_exec_decode_does_not_require_container_exec_identity() {
        let event = vz_agent_proto::ExecEvent {
            event: Some(exec_event::Event::Stdout(b"ordinary".to_vec())),
            sequence: 1,
            request_id: "req_ordinary".to_string(),
            exec_id: 0,
        };
        let mut last_sequence = 0;
        let mut expected_request_id = Some("req_ordinary".to_string());
        assert_eq!(
            decode_exec_stream_event(&mut last_sequence, &mut expected_request_id, None, event)
                .unwrap(),
            Some(vz::protocol::ExecEvent::Stdout(b"ordinary".to_vec()))
        );
    }

    #[test]
    fn transport_parity_grpc_metadata_generation_is_stable_for_matrixed_operations() {
        let mut expected_sequence = 0u64;
        for entry in vz_runtime_contract::PRIMITIVE_CONFORMANCE_MATRIX {
            if !entry.grpc_metadata {
                continue;
            }

            let (expected_request_id, expected_key) =
                vz_runtime_contract::transport_metadata_for_sequence(
                    expected_sequence,
                    Some(entry.operation),
                );
            expected_sequence = expected_sequence.saturating_add(1);

            let expected_prefix = entry
                .operation
                .idempotency_key_prefix()
                .map(|prefix| format!("{prefix}:{expected_request_id}"));
            assert_eq!(expected_key, expected_prefix);

            assert_eq!(
                expected_request_id,
                format!("req_{:016x}", expected_sequence),
                "request id sequence mismatch for {}",
                entry.operation.as_str()
            );
        }

        assert!(expected_sequence > 0);
    }
}
