//! gRPC-based guest agent client.
//!
//! Provides the host-side client for communicating with the guest agent
//! over gRPC/protobuf. The gRPC channel runs over vsock via a custom
//! tonic connector.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::debug;
use vz::Vm;
use vz::protocol::{ExecOutput, OciContainerState};
use vz_agent_proto::{
    ContainerExecTarget, ContainerGeneration, DockerEnsureEvent, DockerEnsureRequest,
    ExecRequest as ProtoExecRequest, NetworkSetupRequest, NetworkTeardownRequest, OciCreateRequest,
    OciDeleteRequest, OciExecRequest, OciKillRequest, OciStartRequest, OciStateRequest,
    PingRequest, PortForwardFrame, PortForwardOpen, ResizeExecPtyRequest, ResourceStatsRequest,
    ResourceStatsResponse, SignalRequest, StdinCloseRequest, StdinWriteRequest, SystemInfoRequest,
    SystemInfoResponse, TransportMetadata as ProtoTransportMetadata,
    agent_service_client::AgentServiceClient, exec_event,
    network_service_client::NetworkServiceClient, oci_service_client::OciServiceClient,
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
    }
}

fn build_oci_exec_request(
    id: String,
    command: String,
    args: Vec<String>,
    options: OciExecOptions,
    metadata: ProtoTransportMetadata,
) -> OciExecRequest {
    OciExecRequest {
        container_id: id,
        command,
        args,
        env: options.env.into_iter().collect(),
        working_dir: options.cwd.unwrap_or_default(),
        user: options.user.unwrap_or_default(),
        metadata: Some(metadata),
    }
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

fn validate_exec_event_metadata(
    last_sequence: &mut u64,
    expected_request_id: &mut Option<String>,
    sequence: u64,
    request_id: &str,
) -> Result<(), LinuxError> {
    if sequence > 0 {
        if sequence <= *last_sequence {
            return Err(LinuxError::Protocol(format!(
                "exec event ordering violation: got sequence {sequence} after {last_sequence}"
            )));
        }
        *last_sequence = sequence;
    }

    if !request_id.is_empty() {
        if let Some(expected) = expected_request_id {
            if expected != request_id {
                return Err(LinuxError::Protocol(format!(
                    "exec request_id mismatch: expected `{expected}`, got `{request_id}`"
                )));
            }
        } else {
            *expected_request_id = Some(request_id.to_string());
        }
    }

    Ok(())
}

impl GrpcAgentClient {
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
        self.exec_stream_with_target(command, args, options, None)
            .await
    }

    async fn exec_stream_with_target(
        &mut self,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
        container_target: Option<String>,
    ) -> Result<GrpcExecStream, LinuxError> {
        let metadata = self.next_transport_metadata(Some(RuntimeOperation::ExecContainer));
        let expected_request_id = if metadata.request_id.is_empty() {
            None
        } else {
            Some(metadata.request_id.clone())
        };

        let expected_container_id = container_target.clone();
        let request = build_exec_request(
            command,
            args,
            options,
            container_target,
            metadata,
            ExecTerminal::default(),
        );

        let response = self.agent.exec(request).await?;
        let stream = GrpcExecStream::new(response.into_inner(), expected_request_id);
        if let Some(container_id) = expected_container_id {
            stream
                .wait_container_ready(&container_id)
                .await
                .map(|(stream, _)| stream)
        } else {
            Ok(stream)
        }
    }

    /// Execute a raw command inside a running OCI container and stream output.
    ///
    /// Container identity is sent as a typed target. Namespace and cgroup
    /// resolution are deliberately guest-owned.
    pub async fn exec_container_stream(
        &mut self,
        container_id: String,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
    ) -> Result<GrpcExecStream, LinuxError> {
        self.exec_stream_with_target(command, args, options, Some(container_id))
            .await
    }

    /// Start a container-targeted pipe exec and return its exact pinned guest
    /// generation. Completion means the inner process successfully exec'd.
    pub async fn exec_container_stream_ready(
        &mut self,
        container_id: String,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
    ) -> Result<(GrpcExecStream, ContainerGeneration), LinuxError> {
        let metadata = self.next_transport_metadata(Some(RuntimeOperation::ExecContainer));
        let expected_request_id =
            (!metadata.request_id.is_empty()).then(|| metadata.request_id.clone());
        let request = build_exec_request(
            command,
            args,
            options,
            Some(container_id.clone()),
            metadata,
            ExecTerminal::default(),
        );
        let response = self.agent.exec(request).await?;
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

    /// Execute through the OCI service's bounded unary compatibility RPC.
    pub async fn oci_exec(
        &mut self,
        id: String,
        command: String,
        args: Vec<String>,
        options: OciExecOptions,
    ) -> Result<ExecOutput, LinuxError> {
        let metadata = self.next_transport_metadata(Some(RuntimeOperation::ExecContainer));
        let response = self
            .oci
            .exec(build_oci_exec_request(id, command, args, options, metadata))
            .await?
            .into_inner();
        Ok(ExecOutput {
            exit_code: response.exit_code,
            stdout: response.stdout,
            stderr: response.stderr,
        })
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
        self.exec_stream_interactive_with_target(command, args, options, None, rows, cols)
            .await
    }

    async fn exec_stream_interactive_with_target(
        &mut self,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
        container_target: Option<String>,
        rows: u32,
        cols: u32,
    ) -> Result<(GrpcExecStream, u64), LinuxError> {
        let debug = exec_control_debug_enabled();
        let metadata = self.next_transport_metadata(Some(RuntimeOperation::ExecContainer));
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
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                return Err(LinuxError::Protocol(
                    "timeout waiting for interactive exec RPC headers from guest".to_string(),
                ));
            }
        };
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
                return Err(LinuxError::Protocol(
                    "timeout waiting for initial exec event from guest".to_string(),
                ));
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

    /// Execute a raw command inside a running OCI container with a PTY.
    pub async fn exec_container_stream_interactive(
        &mut self,
        container_id: String,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
        rows: u32,
        cols: u32,
    ) -> Result<(GrpcExecStream, u64), LinuxError> {
        self.exec_stream_interactive_with_target(
            command,
            args,
            options,
            Some(container_id),
            rows,
            cols,
        )
        .await
    }

    /// Start a container-targeted PTY exec and return its exec ID plus exact
    /// pinned guest generation.
    pub async fn exec_container_stream_interactive_ready(
        &mut self,
        container_id: String,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
        rows: u32,
        cols: u32,
    ) -> Result<(GrpcExecStream, u64, ContainerGeneration), LinuxError> {
        let (stream, exec_id) = self
            .exec_stream_interactive_with_target(
                command,
                args,
                options,
                Some(container_id),
                rows,
                cols,
            )
            .await?;
        let generation = stream.container_generation.clone().ok_or_else(|| {
            LinuxError::Protocol("container PTY exec omitted ready generation".to_string())
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

impl GrpcExecStream {
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
        }
    }

    /// Create a new interactive exec stream, extracting exec_id from the first event.
    ///
    /// Returns the stream and the exec_id for subsequent stdin_write/resize operations.
    pub async fn new_interactive(
        mut inner: tonic::Streaming<vz_agent_proto::ExecEvent>,
        expected_request_id: Option<String>,
        expected_container_id: Option<&str>,
    ) -> Result<(Self, u64), LinuxError> {
        // Read the first event to extract exec_id.
        let first = inner
            .message()
            .await?
            .ok_or_else(|| LinuxError::Protocol("interactive exec stream empty".to_string()))?;

        let exec_id = first.exec_id;
        if exec_id == 0 {
            return Err(LinuxError::Protocol(
                "interactive exec missing exec_id in first event".to_string(),
            ));
        }

        let mut stream = Self::new(inner, expected_request_id);
        if let Some(container_id) = expected_container_id {
            validate_exec_event_metadata(
                &mut stream.last_sequence,
                &mut stream.expected_request_id,
                first.sequence,
                first.request_id.as_str(),
            )?;
            stream.container_generation = Some(require_container_ready(&first, container_id)?);
        } else {
            stream.buffered_first = Some(first);
        }

        // Buffer ordinary guest PTY correlation frames. Container readiness is
        // protocol control data and is deliberately not exposed as stdout.

        Ok((stream, exec_id))
    }

    async fn wait_container_ready(
        mut self,
        expected_container_id: &str,
    ) -> Result<(Self, ContainerGeneration), LinuxError> {
        let first = self.inner.message().await?.ok_or_else(|| {
            LinuxError::Protocol("container exec stream ended before readiness".to_string())
        })?;
        validate_exec_event_metadata(
            &mut self.last_sequence,
            &mut self.expected_request_id,
            first.sequence,
            first.request_id.as_str(),
        )?;
        let generation = require_container_ready(&first, expected_container_id)?;
        self.container_generation = Some(generation.clone());
        Ok((self, generation))
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
        if self.done {
            return None;
        }

        loop {
            // Return the buffered first event if present (from interactive setup).
            let next_event = if let Some(buffered) = self.buffered_first.take() {
                Ok(Some(buffered))
            } else {
                self.inner.message().await
            };

            match next_event {
                Ok(Some(proto_event)) => {
                    if validate_exec_event_metadata(
                        &mut self.last_sequence,
                        &mut self.expected_request_id,
                        proto_event.sequence,
                        proto_event.request_id.as_str(),
                    )
                    .is_err()
                    {
                        self.done = true;
                        return Some(vz::protocol::ExecEvent::Exit(-1));
                    }
                    match proto_event.event {
                        Some(exec_event::Event::Stdout(data)) => {
                            return Some(vz::protocol::ExecEvent::Stdout(data));
                        }
                        Some(exec_event::Event::Stderr(data)) => {
                            return Some(vz::protocol::ExecEvent::Stderr(data));
                        }
                        Some(exec_event::Event::ExitCode(code)) => {
                            self.done = true;
                            return Some(vz::protocol::ExecEvent::Exit(code));
                        }
                        Some(exec_event::Event::Error(_)) => {
                            self.done = true;
                            return Some(vz::protocol::ExecEvent::Exit(-1));
                        }
                        Some(exec_event::Event::ContainerReady(_)) => {
                            self.done = true;
                            return Some(vz::protocol::ExecEvent::Exit(-1));
                        }
                        None => {
                            // Empty event frame, skip.
                            continue;
                        }
                    }
                }
                Ok(None) | Err(_) => {
                    self.done = true;
                    return None;
                }
            }
        }
    }

    /// Collect all remaining events into an [`ExecOutput`].
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
            ..event
        };
        assert!(require_container_ready(&output_before_ready, "web").is_err());
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
    fn unary_oci_request_preserves_raw_command_options_and_identity() {
        let request = build_oci_exec_request(
            "web".to_string(),
            "/bin/printf".to_string(),
            vec!["%s".to_string(), "$HOME;still-argv".to_string()],
            OciExecOptions {
                env: vec![("PATH".to_string(), "/bin".to_string())],
                cwd: Some("/workspace".to_string()),
                user: Some("1000:1000".to_string()),
            },
            ProtoTransportMetadata::default(),
        );

        assert_eq!(request.container_id, "web");
        assert_eq!(request.command, "/bin/printf");
        assert_eq!(request.args, ["%s", "$HOME;still-argv"]);
        assert_eq!(request.working_dir, "/workspace");
        assert_eq!(request.env.get("PATH").map(String::as_str), Some("/bin"));
        assert_eq!(request.user, "1000:1000");
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
