//! gRPC-based guest agent client.
//!
//! Provides the same public API as [`crate::agent`] but communicates
//! with the guest agent over gRPC/protobuf instead of JSON framing.
//! The gRPC channel runs over vsock via a custom tonic connector.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use vz::Vm;
use vz::protocol::{ExecOutput, OciContainerState, OciExecResult};
use vz_agent_proto::{
    ExecRequest as ProtoExecRequest, NetworkSetupRequest, NetworkTeardownRequest, OciCreateRequest,
    OciDeleteRequest, OciExecRequest, OciKillRequest, OciStartRequest, OciStateRequest,
    PingRequest, ResourceStatsRequest, ResourceStatsResponse, SystemInfoRequest,
    SystemInfoResponse, agent_service_client::AgentServiceClient, exec_event,
    network_service_client::NetworkServiceClient, oci_service_client::OciServiceClient,
};

use crate::LinuxError;
use crate::agent::{ExecOptions, OciExecOptions};

/// Default gRPC agent port (matches [`vz::protocol::AGENT_PORT`]).
const GRPC_AGENT_PORT: u32 = 7424;

/// Timeout for establishing the vsock connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// gRPC-based guest agent client.
///
/// Wraps three tonic service clients that share a single vsock-backed
/// gRPC channel to the guest agent:
///
/// - [`AgentServiceClient`] -- ping, system info, exec
/// - [`OciServiceClient`] -- container lifecycle
/// - [`NetworkServiceClient`] -- network namespace management
pub struct GrpcAgentClient {
    /// Agent service client (ping, system info, resource stats, exec).
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

    /// Execute a command in the guest and return the streaming response.
    ///
    /// Unlike [`exec`](Self::exec), this does not buffer the output.
    /// The caller receives a [`tonic::Streaming`] of
    /// [`ExecEvent`](vz_agent_proto::ExecEvent) messages and can
    /// process them incrementally.
    pub async fn exec_stream(
        &mut self,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
    ) -> Result<tonic::Streaming<vz_agent_proto::ExecEvent>, LinuxError> {
        let env = options.env.into_iter().collect::<HashMap<String, String>>();

        let request = ProtoExecRequest {
            command,
            args,
            working_dir: options.working_dir.unwrap_or_default(),
            env,
            user: options.user.unwrap_or_default(),
        };

        let response = self.agent.exec(request).await?;
        Ok(response.into_inner())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_agent_port_matches_protocol_agent_port() {
        assert_eq!(GRPC_AGENT_PORT, vz::protocol::AGENT_PORT);
    }
}
