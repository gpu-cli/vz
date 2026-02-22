//! gRPC service implementations for the guest agent.
//!
//! Each service struct holds shared state (process table, etc.) and delegates
//! to the existing handler logic in the parent module. This bridges from
//! protobuf request/response types to the underlying handler functions.

// tonic::Status is the canonical error type for all gRPC service methods;
// its size is dictated by the tonic crate and cannot be reduced here.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

#[cfg(target_os = "linux")]
use tracing::error;

use vz_agent_proto::*;

use crate::process_table::ProcessTable;

// ── Shared state passed to all service impls ────────────────────────

/// Shared state accessible by all gRPC service implementations.
#[derive(Clone)]
pub struct SharedState {
    /// Process table for tracking spawned child processes.
    pub process_table: Arc<Mutex<ProcessTable>>,
}

// ── AgentService ────────────────────────────────────────────────────

/// gRPC implementation of the `AgentService` trait.
pub struct AgentServiceImpl {
    state: SharedState,
}

impl AgentServiceImpl {
    /// Create a new `AgentServiceImpl` with the given shared state.
    pub fn new(state: SharedState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl agent_service_server::AgentService for AgentServiceImpl {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        info!("grpc: ping");
        Ok(Response::new(PingResponse {}))
    }

    async fn system_info(
        &self,
        _request: Request<SystemInfoRequest>,
    ) -> Result<Response<SystemInfoResponse>, Status> {
        let (cpu_count, memory_bytes, disk_free_bytes, os_version) = crate::collect_system_info()
            .map_err(|e| {
            warn!(error = %e, "grpc: system_info failed");
            Status::internal(format!("system info failed: {e}"))
        })?;

        Ok(Response::new(SystemInfoResponse {
            cpu_count,
            memory_bytes,
            disk_free_bytes,
            os_version,
        }))
    }

    async fn resource_stats(
        &self,
        _request: Request<ResourceStatsRequest>,
    ) -> Result<Response<ResourceStatsResponse>, Status> {
        let stats = crate::collect_resource_stats().map_err(|e| {
            warn!(error = %e, "grpc: resource_stats failed");
            Status::internal(format!("resource stats failed: {e}"))
        })?;

        Ok(Response::new(ResourceStatsResponse {
            cpu_usage_percent: stats.cpu_usage_percent,
            memory_used_bytes: stats.memory_used_bytes,
            memory_total_bytes: stats.memory_total_bytes,
            disk_used_bytes: stats.disk_used_bytes,
            disk_total_bytes: stats.disk_total_bytes,
            process_count: stats.process_count,
            load_average: stats.load_average.to_vec(),
        }))
    }

    type ExecStream = ReceiverStream<Result<ExecEvent, Status>>;

    async fn exec(
        &self,
        request: Request<ExecRequest>,
    ) -> Result<Response<Self::ExecStream>, Status> {
        use tokio::io::AsyncReadExt;

        let req = request.into_inner();
        let env: Vec<(String, String)> = req.env.into_iter().collect();
        let working_dir = if req.working_dir.is_empty() {
            None
        } else {
            Some(req.working_dir)
        };
        let user = if req.user.is_empty() {
            None
        } else {
            Some(req.user)
        };

        let spawn_result = if let Some(ref username) = user {
            crate::spawn_as_user(
                username,
                &req.command,
                &req.args,
                working_dir.as_deref(),
                &env,
            )
        } else {
            crate::spawn_direct(&req.command, &req.args, working_dir.as_deref(), &env)
        };

        let mut child = match spawn_result {
            Ok(child) => child,
            Err(e) => {
                warn!(command = %req.command, error = %e, "grpc: exec spawn failed");
                // Return a stream with a single error event.
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                let _ = tx
                    .send(Ok(ExecEvent {
                        event: Some(exec_event::Event::Error(e.to_string())),
                    }))
                    .await;
                return Ok(Response::new(ReceiverStream::new(rx)));
            }
        };

        info!(command = %req.command, args = ?req.args, "grpc: process spawned");

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stdin = child.stdin.take();

        // Generate exec_id from the child PID (or a fallback).
        let exec_id = child.id().unwrap_or(0) as u64;

        {
            let mut table = self.state.process_table.lock().await;
            table.insert(exec_id, child, stdin);
        }

        // Channel for streaming ExecEvents back to the client.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ExecEvent, Status>>(64);

        let process_table = self.state.process_table.clone();

        // Spawn stdout reader.
        let stdout_tx = tx.clone();
        let stdout_handle = tokio::spawn(async move {
            if let Some(mut stdout) = stdout {
                let mut buf = vec![0u8; 65536];
                loop {
                    match stdout.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if stdout_tx
                                .send(Ok(ExecEvent {
                                    event: Some(exec_event::Event::Stdout(buf[..n].to_vec())),
                                }))
                                .await
                                .is_err()
                            {
                                break; // Client disconnected.
                            }
                        }
                        Err(e) => {
                            warn!(exec_id, error = %e, "grpc: stdout read error");
                            break;
                        }
                    }
                }
            }
        });

        // Spawn stderr reader.
        let stderr_tx = tx.clone();
        let stderr_handle = tokio::spawn(async move {
            if let Some(mut stderr) = stderr {
                let mut buf = vec![0u8; 65536];
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if stderr_tx
                                .send(Ok(ExecEvent {
                                    event: Some(exec_event::Event::Stderr(buf[..n].to_vec())),
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(e) => {
                            warn!(exec_id, error = %e, "grpc: stderr read error");
                            break;
                        }
                    }
                }
            }
        });

        // Spawn exit watcher.
        let exit_tx = tx;
        let exit_table = process_table;
        tokio::spawn(async move {
            // Wait for child exit before draining pipes.
            let exit_code = {
                let mut table = exit_table.lock().await;
                if let Some(entry) = table.get_mut(exec_id) {
                    match entry.child.wait().await {
                        Ok(status) => status.code().unwrap_or(-1),
                        Err(e) => {
                            warn!(exec_id, error = %e, "grpc: wait error");
                            -1
                        }
                    }
                } else {
                    -1
                }
            };

            // Brief window for remaining stdout/stderr data.
            let _ = tokio::time::timeout(std::time::Duration::from_millis(500), async {
                let _ = stdout_handle.await;
                let _ = stderr_handle.await;
            })
            .await;

            info!(exec_id, exit_code, "grpc: process exited");

            let _ = exit_tx
                .send(Ok(ExecEvent {
                    event: Some(exec_event::Event::ExitCode(exit_code)),
                }))
                .await;

            // Remove from process table.
            {
                let mut table = exit_table.lock().await;
                table.remove(exec_id);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn stdin_write(
        &self,
        request: Request<StdinWriteRequest>,
    ) -> Result<Response<StdinWriteResponse>, Status> {
        use tokio::io::AsyncWriteExt;

        let req = request.into_inner();
        let mut table = self.state.process_table.lock().await;

        let entry = table
            .get_mut(req.exec_id)
            .ok_or_else(|| Status::not_found(format!("process {} not found", req.exec_id)))?;

        let stdin = entry
            .stdin
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("stdin already closed"))?;

        stdin
            .write_all(&req.data)
            .await
            .map_err(|e| Status::internal(format!("stdin write failed: {e}")))?;

        Ok(Response::new(StdinWriteResponse {}))
    }

    async fn stdin_close(
        &self,
        request: Request<StdinCloseRequest>,
    ) -> Result<Response<StdinCloseResponse>, Status> {
        let req = request.into_inner();
        let mut table = self.state.process_table.lock().await;

        if let Some(entry) = table.get_mut(req.exec_id) {
            entry.stdin = None;
            info!(exec_id = req.exec_id, "grpc: stdin closed");
        } else {
            warn!(
                exec_id = req.exec_id,
                "grpc: stdin close: process not found"
            );
        }

        Ok(Response::new(StdinCloseResponse {}))
    }

    async fn signal(
        &self,
        request: Request<SignalRequest>,
    ) -> Result<Response<SignalResponse>, Status> {
        let req = request.into_inner();
        let table = self.state.process_table.lock().await;

        if let Some(entry) = table.get(req.exec_id) {
            if let Some(pid) = entry.pid() {
                info!(
                    exec_id = req.exec_id,
                    pid,
                    signal = req.signal,
                    "grpc: sending signal"
                );
                // SAFETY: kill is a standard POSIX function.
                unsafe {
                    libc::kill(pid, req.signal);
                }
            }
        } else {
            warn!(exec_id = req.exec_id, "grpc: signal: process not found");
        }

        Ok(Response::new(SignalResponse {}))
    }

    type PortForwardStream = ReceiverStream<Result<PortForwardFrame, Status>>;

    async fn port_forward(
        &self,
        request: Request<tonic::Streaming<PortForwardFrame>>,
    ) -> Result<Response<Self::PortForwardStream>, Status> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut inbound = request.into_inner();

        // First frame must be PortForwardOpen.
        let first_frame = inbound
            .message()
            .await
            .map_err(|e| Status::internal(format!("failed to read first frame: {e}")))?
            .ok_or_else(|| Status::invalid_argument("empty port forward stream"))?;

        let open = match first_frame.frame {
            Some(port_forward_frame::Frame::Open(open)) => open,
            _ => {
                return Err(Status::invalid_argument(
                    "first frame must be PortForwardOpen",
                ));
            }
        };

        if open.protocol != "tcp" {
            return Err(Status::invalid_argument(format!(
                "unsupported protocol: {}",
                open.protocol
            )));
        }

        let host = if open.target_host.is_empty() {
            "127.0.0.1"
        } else {
            &open.target_host
        };
        let port = open.target_port as u16;

        let target = crate::connect_port_forward_target(host, port)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to {host}:{port}: {e}")))?;

        let (mut target_reader, mut target_writer) = target.into_split();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<PortForwardFrame, Status>>(64);

        // Task: read from target TCP socket, send as gRPC data frames.
        let reader_tx = tx.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                match target_reader.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if reader_tx
                            .send(Ok(PortForwardFrame {
                                frame: Some(port_forward_frame::Frame::Data(buf[..n].to_vec())),
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "grpc: port forward target read error");
                        break;
                    }
                }
            }
        });

        // Task: read gRPC data frames from client, write to target TCP socket.
        tokio::spawn(async move {
            while let Ok(Some(frame)) = inbound.message().await {
                if let Some(port_forward_frame::Frame::Data(data)) = frame.frame {
                    if let Err(e) = target_writer.write_all(&data).await {
                        warn!(error = %e, "grpc: port forward target write error");
                        break;
                    }
                }
            }
            // Client stream ended; shut down the target write side.
            let _ = target_writer.shutdown().await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

// ── OciService ──────────────────────────────────────────────────────

/// gRPC implementation of the `OciService` trait.
///
/// OCI lifecycle requests are unsupported in this guest agent binary.
/// All methods return `UNIMPLEMENTED` status.
pub struct OciServiceImpl;

#[tonic::async_trait]
impl oci_service_server::OciService for OciServiceImpl {
    async fn create(
        &self,
        _request: Request<OciCreateRequest>,
    ) -> Result<Response<OciCreateResponse>, Status> {
        Err(Status::unimplemented(oci_unsupported_message()))
    }

    async fn start(
        &self,
        _request: Request<OciStartRequest>,
    ) -> Result<Response<OciStartResponse>, Status> {
        Err(Status::unimplemented(oci_unsupported_message()))
    }

    async fn state(
        &self,
        _request: Request<OciStateRequest>,
    ) -> Result<Response<OciStateResponse>, Status> {
        Err(Status::unimplemented(oci_unsupported_message()))
    }

    async fn exec(
        &self,
        _request: Request<OciExecRequest>,
    ) -> Result<Response<OciExecResponse>, Status> {
        Err(Status::unimplemented(oci_unsupported_message()))
    }

    async fn kill(
        &self,
        _request: Request<OciKillRequest>,
    ) -> Result<Response<OciKillResponse>, Status> {
        Err(Status::unimplemented(oci_unsupported_message()))
    }

    async fn delete(
        &self,
        _request: Request<OciDeleteRequest>,
    ) -> Result<Response<OciDeleteResponse>, Status> {
        Err(Status::unimplemented(oci_unsupported_message()))
    }
}

/// Generate the unsupported OCI message for this platform.
fn oci_unsupported_message() -> String {
    format!(
        "OCI lifecycle requests are unsupported by vz-guest-agent on {} guests",
        std::env::consts::OS
    )
}

// ── NetworkService ──────────────────────────────────────────────────

/// gRPC implementation of the `NetworkService` trait.
pub struct NetworkServiceImpl;

#[tonic::async_trait]
impl network_service_server::NetworkService for NetworkServiceImpl {
    async fn setup(
        &self,
        request: Request<NetworkSetupRequest>,
    ) -> Result<Response<NetworkSetupResponse>, Status> {
        let req = request.into_inner();
        do_network_setup(&req.stack_id, &req.services)
    }

    async fn teardown(
        &self,
        request: Request<NetworkTeardownRequest>,
    ) -> Result<Response<NetworkTeardownResponse>, Status> {
        let req = request.into_inner();
        do_network_teardown(&req.stack_id, &req.service_names).await
    }
}

#[cfg(target_os = "linux")]
fn do_network_setup(
    stack_id: &str,
    services: &[vz_agent_proto::NetworkServiceConfig],
) -> Result<Response<NetworkSetupResponse>, Status> {
    // Convert proto NetworkServiceConfig to vz protocol NetworkServiceConfig.
    let vz_services: Vec<vz::protocol::NetworkServiceConfig> = services
        .iter()
        .map(|s| vz::protocol::NetworkServiceConfig {
            name: s.name.clone(),
            addr: s.addr.clone(),
            network_name: s.network_name.clone(),
        })
        .collect();

    match crate::network::setup_stack_network(stack_id, &vz_services) {
        Ok(()) => {
            info!(stack_id = %stack_id, services = services.len(), "grpc: network setup complete");
            Ok(Response::new(NetworkSetupResponse {}))
        }
        Err(e) => {
            error!(stack_id = %stack_id, error = %e, "grpc: network setup failed");
            Err(Status::internal(format!("network setup failed: {e}")))
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn do_network_setup(
    _stack_id: &str,
    _services: &[vz_agent_proto::NetworkServiceConfig],
) -> Result<Response<NetworkSetupResponse>, Status> {
    Err(Status::unimplemented("network setup requires Linux"))
}

#[cfg(target_os = "linux")]
async fn do_network_teardown(
    stack_id: &str,
    service_names: &[String],
) -> Result<Response<NetworkTeardownResponse>, Status> {
    let stack_id_owned = stack_id.to_string();
    let service_names_owned = service_names.to_vec();

    let result = tokio::task::spawn_blocking(move || {
        crate::network::teardown_stack_network(&stack_id_owned, &service_names_owned)
    })
    .await;

    match result {
        Ok(Ok(())) => {
            info!(stack_id = %stack_id, "grpc: network teardown complete");
            Ok(Response::new(NetworkTeardownResponse {}))
        }
        Ok(Err(e)) => {
            error!(stack_id = %stack_id, error = %e, "grpc: network teardown failed");
            Err(Status::internal(format!("network teardown failed: {e}")))
        }
        Err(e) => {
            error!(stack_id = %stack_id, error = %e, "grpc: network teardown task panicked");
            Err(Status::internal(format!("task panicked: {e}")))
        }
    }
}

#[cfg(not(target_os = "linux"))]
async fn do_network_teardown(
    _stack_id: &str,
    _service_names: &[String],
) -> Result<Response<NetworkTeardownResponse>, Status> {
    Err(Status::unimplemented("network teardown requires Linux"))
}
