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

/// Path to the youki OCI runtime binary (delivered via VirtioFS).
#[cfg(target_os = "linux")]
const YOUKI_BIN: &str = "/run/vz-oci/bin/youki";

/// Root directory for youki container state.
#[cfg(target_os = "linux")]
const YOUKI_ROOT: &str = "/run/vz-oci/state";

/// gRPC implementation of the `OciService` trait.
///
/// On Linux guests, delegates to the youki OCI runtime for container
/// lifecycle management. On other platforms, returns `UNIMPLEMENTED`.
pub struct OciServiceImpl;

#[cfg(target_os = "linux")]
#[tonic::async_trait]
impl oci_service_server::OciService for OciServiceImpl {
    async fn create(
        &self,
        request: Request<OciCreateRequest>,
    ) -> Result<Response<OciCreateResponse>, Status> {
        let req = request.into_inner();
        info!(container_id = %req.container_id, bundle_path = %req.bundle_path, "oci: create");

        // Patch the OCI config to work in the minimal guest VM kernel.
        let config_path = format!("{}/config.json", &req.bundle_path);
        match patch_oci_config(&config_path).await {
            Ok(()) => info!(container_id = %req.container_id, "oci: config patched for guest VM"),
            Err(e) => error!(container_id = %req.container_id, error = %e, "oci: failed to patch config"),
        }

        // Log bundle config for diagnostics.
        match tokio::fs::read_to_string(&config_path).await {
            Ok(config) => info!(container_id = %req.container_id, config = %config, "oci: bundle config"),
            Err(e) => error!(container_id = %req.container_id, error = %e, "oci: failed to read bundle config"),
        }

        run_youki(&["create", "--bundle", &req.bundle_path, &req.container_id]).await?;
        Ok(Response::new(OciCreateResponse {}))
    }

    async fn start(
        &self,
        request: Request<OciStartRequest>,
    ) -> Result<Response<OciStartResponse>, Status> {
        let req = request.into_inner();
        info!(container_id = %req.container_id, "oci: start");

        run_youki(&["start", &req.container_id]).await?;
        Ok(Response::new(OciStartResponse {}))
    }

    async fn state(
        &self,
        request: Request<OciStateRequest>,
    ) -> Result<Response<OciStateResponse>, Status> {
        let req = request.into_inner();

        let output = run_youki_output(&["state", &req.container_id], YOUKI_LIFECYCLE_TIMEOUT).await?;
        let state: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| Status::internal(format!("failed to parse youki state: {e}")))?;

        Ok(Response::new(OciStateResponse {
            container_id: state["id"].as_str().unwrap_or("").to_string(),
            status: state["status"].as_str().unwrap_or("unknown").to_string(),
            pid: state["pid"].as_u64().unwrap_or(0) as u32,
            bundle_path: state["bundle"].as_str().unwrap_or("").to_string(),
        }))
    }

    async fn exec(
        &self,
        request: Request<OciExecRequest>,
    ) -> Result<Response<OciExecResponse>, Status> {
        let req = request.into_inner();
        info!(container_id = %req.container_id, command = %req.command, "oci: exec");

        // Youki 0.5.7 exec doesn't properly enter the container's mount
        // namespace, causing commands to see the initramfs instead of the
        // container rootfs. Work around this by using nsenter: get the init
        // PID from `youki state`, then nsenter into its namespaces.
        let state_output = run_youki_output(
            &["state", &req.container_id],
            YOUKI_LIFECYCLE_TIMEOUT,
        )
        .await?;
        let state: serde_json::Value = serde_json::from_slice(&state_output.stdout)
            .map_err(|e| Status::internal(format!("failed to parse youki state: {e}")))?;
        let pid = state["pid"]
            .as_u64()
            .ok_or_else(|| Status::internal("youki state missing pid field"))?;

        let mut nsenter_args: Vec<String> = vec![
            "nsenter".into(),
            "--mount".into(),
            "--net".into(),
            format!("--root=/proc/{pid}/root"),
            format!("--target={pid}"),
            "--".into(),
            "env".into(),
        ];

        // Always set a standard PATH so commands like pg_isready are found.
        let has_path = req.env.keys().any(|k| k == "PATH");
        if !has_path {
            nsenter_args.push(
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".into(),
            );
        }

        for (key, value) in &req.env {
            nsenter_args.push(format!("{key}={value}"));
        }

        nsenter_args.push(req.command);
        nsenter_args.extend(req.args);

        info!(pid = pid, args = ?nsenter_args, "oci: exec via nsenter");

        let mut cmd = tokio::process::Command::new(&nsenter_args[0]);
        for arg in &nsenter_args[1..] {
            cmd.arg(arg);
        }
        if !req.working_dir.is_empty() {
            cmd.current_dir(&req.working_dir);
        }
        cmd.kill_on_drop(true);

        let output = match tokio::time::timeout(YOUKI_EXEC_TIMEOUT, cmd.output()).await {
            Ok(Ok(output)) => output,
            Ok(Err(e)) => {
                return Err(Status::internal(format!("failed to execute nsenter: {e}")));
            }
            Err(_) => {
                return Err(Status::internal(format!(
                    "oci exec timed out after {}s",
                    YOUKI_EXEC_TIMEOUT.as_secs()
                )));
            }
        };

        Ok(Response::new(OciExecResponse {
            exit_code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        }))
    }

    async fn kill(
        &self,
        request: Request<OciKillRequest>,
    ) -> Result<Response<OciKillResponse>, Status> {
        let req = request.into_inner();
        info!(container_id = %req.container_id, signal = %req.signal, "oci: kill");

        run_youki(&["kill", &req.container_id, &req.signal]).await?;
        Ok(Response::new(OciKillResponse {}))
    }

    async fn delete(
        &self,
        request: Request<OciDeleteRequest>,
    ) -> Result<Response<OciDeleteResponse>, Status> {
        let req = request.into_inner();
        info!(container_id = %req.container_id, force = req.force, "oci: delete");

        if req.force {
            run_youki(&["delete", "--force", &req.container_id]).await?;
        } else {
            run_youki(&["delete", &req.container_id]).await?;
        }
        Ok(Response::new(OciDeleteResponse {}))
    }
}

/// Timeout for youki lifecycle commands (create, start, kill, delete).
#[cfg(target_os = "linux")]
const YOUKI_LIFECYCLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Timeout for youki exec commands.
#[cfg(target_os = "linux")]
const YOUKI_EXEC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Ensure the youki state directory exists.
#[cfg(target_os = "linux")]
fn ensure_youki_state_dir() {
    let _ = std::fs::create_dir_all(YOUKI_ROOT);
}

/// Run a youki lifecycle command (create, start, kill, delete) and check for
/// success. Uses null stdio to avoid blocking on long-lived child processes
/// that inherit pipe FDs.
#[cfg(target_os = "linux")]
async fn run_youki(args: &[&str]) -> Result<(), Status> {
    ensure_youki_state_dir();
    let _ = std::fs::create_dir_all(YOUKI_LOG_DIR);

    let subcmd = args.first().unwrap_or(&"unknown");
    let container_id = args.last().unwrap_or(&"unknown");
    let log_file = format!("{YOUKI_LOG_DIR}/{container_id}-{subcmd}.log");

    let mut cmd = tokio::process::Command::new(YOUKI_BIN);
    cmd.arg("--root").arg(YOUKI_ROOT);
    cmd.arg("--log").arg(&log_file);
    cmd.kill_on_drop(true);
    // Lifecycle commands (create, start) fork child processes that inherit
    // pipe FDs. Using null stdio ensures wait() returns as soon as the
    // youki parent exits, without blocking on the init process.
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());
    for arg in args {
        cmd.arg(arg);
    }

    let cmd_desc = format!("youki {}", args.join(" "));
    info!(cmd = %cmd_desc, log_file = %log_file, "executing youki command");

    let mut child = cmd.spawn().map_err(|e| {
        error!(cmd = %cmd_desc, error = %e, "failed to spawn youki");
        Status::internal(format!("failed to execute youki: {e}"))
    })?;

    let status = match tokio::time::timeout(YOUKI_LIFECYCLE_TIMEOUT, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(e)) => {
            error!(cmd = %cmd_desc, error = %e, "failed to wait for youki");
            dump_youki_log(&log_file).await;
            return Err(Status::internal(format!("youki {subcmd} failed: {e}")));
        }
        Err(_) => {
            error!(cmd = %cmd_desc, timeout_secs = YOUKI_LIFECYCLE_TIMEOUT.as_secs(), "youki command timed out");
            dump_youki_log(&log_file).await;
            return Err(Status::internal(format!(
                "{cmd_desc} timed out after {}s",
                YOUKI_LIFECYCLE_TIMEOUT.as_secs()
            )));
        }
    };

    if !status.success() {
        let youki_log = tokio::fs::read_to_string(&log_file).await.unwrap_or_default();
        error!(command = %subcmd, log = %youki_log, "youki command failed");
        // Include the last few lines of the youki log in the error response
        // so the host can surface them without needing VM access.
        let log_tail: String = youki_log
            .lines()
            .rev()
            .take(10)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n");
        let exit_code = status.code().unwrap_or(-1);
        return Err(Status::internal(if log_tail.is_empty() {
            format!("youki {subcmd} failed (exit {exit_code}): no log output")
        } else {
            format!("youki {subcmd} failed (exit {exit_code}): {log_tail}")
        }));
    }

    Ok(())
}

/// Directory for youki log files.
#[cfg(target_os = "linux")]
const YOUKI_LOG_DIR: &str = "/run/vz-oci/logs";

/// Run a youki command and return the raw output (success or failure).
#[cfg(target_os = "linux")]
async fn run_youki_output(
    args: &[&str],
    timeout: std::time::Duration,
) -> Result<std::process::Output, Status> {
    ensure_youki_state_dir();
    let _ = std::fs::create_dir_all(YOUKI_LOG_DIR);

    // Generate a unique log file for this invocation.
    let subcmd = args.first().unwrap_or(&"unknown");
    let container_id = args.last().unwrap_or(&"unknown");
    let log_file = format!("{YOUKI_LOG_DIR}/{container_id}-{subcmd}.log");

    let mut cmd = tokio::process::Command::new(YOUKI_BIN);
    cmd.arg("--root").arg(YOUKI_ROOT);
    cmd.arg("--log").arg(&log_file);
    cmd.kill_on_drop(true);
    for arg in args {
        cmd.arg(arg);
    }

    let cmd_desc = format!("youki {}", args.join(" "));
    info!(cmd = %cmd_desc, log_file = %log_file, "executing youki command");

    match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(e)) => {
            error!(cmd = %cmd_desc, error = %e, "failed to execute youki");
            dump_youki_log(&log_file).await;
            Err(Status::internal(format!("failed to execute youki: {e}")))
        }
        Err(_) => {
            error!(cmd = %cmd_desc, timeout_secs = timeout.as_secs(), "youki command timed out");
            dump_youki_log(&log_file).await;
            Err(Status::internal(format!("{cmd_desc} timed out after {}s", timeout.as_secs())))
        }
    }
}

/// Patch OCI config.json to be compatible with the minimal guest VM kernel.
///
/// The guest VM runs a stripped kernel that may lack certain filesystem types
/// (e.g. mqueue, cgroup v1). This function removes or adjusts mounts that
/// would cause youki to fail or hang.
#[cfg(target_os = "linux")]
async fn patch_oci_config(config_path: &str) -> Result<(), Status> {
    let content = tokio::fs::read_to_string(config_path)
        .await
        .map_err(|e| Status::internal(format!("read config.json: {e}")))?;

    let mut config: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| Status::internal(format!("parse config.json: {e}")))?;

    // Remove mounts with filesystem types not available in the minimal kernel.
    // Only keep types known to work: proc, tmpfs, bind, overlay.
    // Types that hang or fail: mqueue (CONFIG_POSIX_MQUEUE), devpts, sysfs,
    // cgroup/cgroup2 — these can cause youki to hang during container init.
    if let Some(mounts) = config.pointer_mut("/mounts").and_then(|v| v.as_array_mut()) {
        let supported_types = ["proc", "tmpfs", "bind"];
        mounts.retain(|m| {
            let typ = m.get("type").and_then(|t| t.as_str()).unwrap_or("");
            if !supported_types.contains(&typ) {
                tracing::info!(mount_type = typ, "stripping unsupported mount type from OCI config");
                false
            } else {
                true
            }
        });
    }

    // Strip maskedPaths, readonlyPaths, and unsupported namespaces — the
    // minimal VM kernel doesn't support all namespace types youki tries to
    // unshare, and masked/readonly paths reference /proc and /sys paths that
    // may not exist, causing youki to hang.
    if let Some(linux) = config.pointer_mut("/linux") {
        if let Some(obj) = linux.as_object_mut() {
            if obj.remove("maskedPaths").is_some() {
                tracing::info!("stripped maskedPaths from OCI config");
            }
            if obj.remove("readonlyPaths").is_some() {
                tracing::info!("stripped readonlyPaths from OCI config");
            }
            // Strip unsupported namespaces but preserve mount and network.
            // The host-side bundle already strips PID/IPC/UTS/cgroup, but
            // older bundles or third-party configs may still include them.
            // Network namespaces MUST be preserved — multi-service stacks
            // use per-service netns (e.g. /var/run/netns/svc-web) for
            // container network isolation and service discovery.
            if let Some(namespaces) = obj.get_mut("namespaces").and_then(|v| v.as_array_mut()) {
                let before = namespaces.len();
                namespaces.retain(|ns| {
                    let typ = ns.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    matches!(typ, "mount" | "network")
                });
                let stripped = before - namespaces.len();
                if stripped > 0 {
                    tracing::info!(stripped, "stripped unsupported namespaces from OCI config");
                }
            }
        }
    }

    let patched = serde_json::to_string_pretty(&config)
        .map_err(|e| Status::internal(format!("serialize config.json: {e}")))?;

    tokio::fs::write(config_path, patched)
        .await
        .map_err(|e| Status::internal(format!("write config.json: {e}")))?;

    Ok(())
}

/// Read and log the contents of a youki log file for diagnostics.
#[cfg(target_os = "linux")]
async fn dump_youki_log(path: &str) {
    match tokio::fs::read_to_string(path).await {
        Ok(contents) if !contents.is_empty() => {
            error!(log_file = %path, contents = %contents, "youki log file contents");
        }
        Ok(_) => {
            warn!(log_file = %path, "youki log file is empty");
        }
        Err(e) => {
            warn!(log_file = %path, error = %e, "could not read youki log file");
        }
    }
}

#[cfg(not(target_os = "linux"))]
#[tonic::async_trait]
impl oci_service_server::OciService for OciServiceImpl {
    async fn create(
        &self,
        _request: Request<OciCreateRequest>,
    ) -> Result<Response<OciCreateResponse>, Status> {
        Err(Status::unimplemented("OCI lifecycle requires Linux guest"))
    }

    async fn start(
        &self,
        _request: Request<OciStartRequest>,
    ) -> Result<Response<OciStartResponse>, Status> {
        Err(Status::unimplemented("OCI lifecycle requires Linux guest"))
    }

    async fn state(
        &self,
        _request: Request<OciStateRequest>,
    ) -> Result<Response<OciStateResponse>, Status> {
        Err(Status::unimplemented("OCI lifecycle requires Linux guest"))
    }

    async fn exec(
        &self,
        _request: Request<OciExecRequest>,
    ) -> Result<Response<OciExecResponse>, Status> {
        Err(Status::unimplemented("OCI lifecycle requires Linux guest"))
    }

    async fn kill(
        &self,
        _request: Request<OciKillRequest>,
    ) -> Result<Response<OciKillResponse>, Status> {
        Err(Status::unimplemented("OCI lifecycle requires Linux guest"))
    }

    async fn delete(
        &self,
        _request: Request<OciDeleteRequest>,
    ) -> Result<Response<OciDeleteResponse>, Status> {
        Err(Status::unimplemented("OCI lifecycle requires Linux guest"))
    }
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
    let vz_services: Vec<::vz::protocol::NetworkServiceConfig> = services
        .iter()
        .map(|s| ::vz::protocol::NetworkServiceConfig {
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
