//! gRPC-based guest agent client.
//!
//! Provides the host-side client for communicating with the guest agent
//! over gRPC/protobuf. The gRPC channel runs over vsock via a custom
//! tonic connector.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use vz::Vm;
use vz::protocol::{ExecOutput, OciContainerState, OciExecResult};
use vz_agent_proto::{
    ExecRequest as ProtoExecRequest, NetworkSetupRequest, NetworkTeardownRequest, OciCreateRequest,
    OciDeleteRequest, OciExecRequest, OciKillRequest, OciStartRequest, OciStateRequest,
    PingRequest, PortForwardFrame, PortForwardOpen, ResourceStatsRequest, ResourceStatsResponse,
    SystemInfoRequest, SystemInfoResponse, agent_service_client::AgentServiceClient, exec_event,
    network_service_client::NetworkServiceClient, oci_service_client::OciServiceClient,
    port_forward_frame,
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
}

impl GrpcAgentClient {
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
        })
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

    /// Execute a command in the guest and collect all output.
    ///
    /// Sends an `Exec` RPC and consumes the server-streamed response,
    /// assembling stdout/stderr chunks into a single [`ExecOutput`].
    pub async fn exec(
        &mut self,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
    ) -> Result<ExecOutput, LinuxError> {
        let env = options.env.into_iter().collect::<HashMap<String, String>>();

        let request = ProtoExecRequest {
            command,
            args,
            working_dir: options.working_dir.unwrap_or_default(),
            env,
            user: options.user.unwrap_or_default(),
        };

        let response = self.agent.exec(request).await?;
        let mut stream = response.into_inner();

        let mut stdout_bytes = Vec::new();
        let mut stderr_bytes = Vec::new();

        loop {
            match stream.message().await? {
                Some(event) => match event.event {
                    Some(exec_event::Event::Stdout(data)) => {
                        stdout_bytes.extend_from_slice(&data);
                    }
                    Some(exec_event::Event::Stderr(data)) => {
                        stderr_bytes.extend_from_slice(&data);
                    }
                    Some(exec_event::Event::ExitCode(code)) => {
                        return Ok(ExecOutput {
                            exit_code: code,
                            stdout: String::from_utf8_lossy(&stdout_bytes).into_owned(),
                            stderr: String::from_utf8_lossy(&stderr_bytes).into_owned(),
                        });
                    }
                    Some(exec_event::Event::Error(msg)) => {
                        return Err(LinuxError::Protocol(format!(
                            "guest exec failed to start: {msg}"
                        )));
                    }
                    None => {
                        // Empty event frame, skip.
                    }
                },
                None => {
                    // Stream ended without an exit code.
                    return Err(LinuxError::Protocol(
                        "exec stream ended without exit code".to_string(),
                    ));
                }
            }
        }
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
        let env = options.env.into_iter().collect::<HashMap<String, String>>();

        let request = ProtoExecRequest {
            command,
            args,
            working_dir: options.working_dir.unwrap_or_default(),
            env,
            user: options.user.unwrap_or_default(),
        };

        let response = self.agent.exec(request).await?;
        Ok(GrpcExecStream::new(response.into_inner()))
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
        self.exec(command, args, options).await
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
        self.oci
            .create(OciCreateRequest {
                container_id: id,
                bundle_path,
            })
            .await?;
        Ok(())
    }

    /// Start a previously created OCI container.
    pub async fn oci_start(&mut self, id: String) -> Result<(), LinuxError> {
        self.oci.start(OciStartRequest { container_id: id }).await?;
        Ok(())
    }

    /// Query runtime state for an OCI container.
    pub async fn oci_state(&mut self, id: String) -> Result<OciContainerState, LinuxError> {
        let response = self.oci.state(OciStateRequest { container_id: id }).await?;
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

    /// Execute a command inside a running OCI container.
    pub async fn oci_exec(
        &mut self,
        id: String,
        command: String,
        args: Vec<String>,
        options: OciExecOptions,
    ) -> Result<OciExecResult, LinuxError> {
        let env = options.env.into_iter().collect::<HashMap<String, String>>();

        let response = self
            .oci
            .exec(OciExecRequest {
                container_id: id,
                command,
                args,
                env,
                working_dir: options.cwd.unwrap_or_default(),
                user: options.user.unwrap_or_default(),
            })
            .await?;
        let result = response.into_inner();
        Ok(OciExecResult {
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
        })
    }

    /// Send a signal to a running OCI container.
    pub async fn oci_kill(&mut self, id: String, signal: String) -> Result<(), LinuxError> {
        self.oci
            .kill(OciKillRequest {
                container_id: id,
                signal,
            })
            .await?;
        Ok(())
    }

    /// Delete an OCI container from runtime state.
    pub async fn oci_delete(&mut self, id: String, force: bool) -> Result<(), LinuxError> {
        self.oci
            .delete(OciDeleteRequest {
                container_id: id,
                force,
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
        self.network
            .setup(NetworkSetupRequest { stack_id, services })
            .await?;
        Ok(())
    }

    /// Tear down per-service network resources.
    pub async fn network_teardown(
        &mut self,
        stack_id: String,
        service_names: Vec<String>,
    ) -> Result<(), LinuxError> {
        self.network
            .teardown(NetworkTeardownRequest {
                stack_id,
                service_names,
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
        let open_frame = PortForwardFrame {
            frame: Some(port_forward_frame::Frame::Open(PortForwardOpen {
                target_port: u32::from(target_port),
                protocol: protocol.to_string(),
                target_host: target_host.unwrap_or_default().to_string(),
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
}

/// A stream of exec events from a gRPC-based command execution.
///
/// Yields [`vz::protocol::ExecEvent`] values (Stdout, Stderr, Exit).
pub struct GrpcExecStream {
    inner: tonic::Streaming<vz_agent_proto::ExecEvent>,
    done: bool,
}

impl GrpcExecStream {
    /// Wrap a tonic streaming response.
    fn new(inner: tonic::Streaming<vz_agent_proto::ExecEvent>) -> Self {
        Self { inner, done: false }
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
            match self.inner.message().await {
                Ok(Some(proto_event)) => match proto_event.event {
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
                    None => {
                        // Empty event frame, skip.
                        continue;
                    }
                },
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
}
