//! gRPC service implementations for the guest agent.
//!
//! Each service struct holds shared state (process table, etc.) and delegates
//! to the existing handler logic in the parent module. This bridges from
//! protobuf request/response types to the underlying handler functions.

// tonic::Status is the canonical error type for all gRPC service methods;
// its size is dictated by the tonic crate and cannot be reduced here.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

#[cfg(target_os = "linux")]
use tracing::error;

use vz_agent_proto::*;

use crate::process_table::ProcessTable;

// ── PTY handle tracking ─────────────────────────────────────────

/// Holds the writer and master PTY for a PTY session, supporting
/// stdin writes and terminal resizing.
struct PtyMasterHandle {
    writer: Box<dyn std::io::Write + Send>,
    master: Box<dyn portable_pty::MasterPty + Send>,
}

static PTY_HANDLES: OnceLock<StdMutex<HashMap<u64, PtyMasterHandle>>> = OnceLock::new();

fn pty_handles() -> &'static StdMutex<HashMap<u64, PtyMasterHandle>> {
    PTY_HANDLES.get_or_init(|| StdMutex::new(HashMap::new()))
}

// ── Shared state passed to all service impls ────────────────────────

/// Shared state accessible by all gRPC service implementations.
#[derive(Clone)]
pub struct SharedState {
    /// Process table for tracking spawned child processes.
    pub process_table: Arc<Mutex<ProcessTable>>,
}

#[derive(Clone)]
struct ExecOrderContext {
    sender: tokio::sync::mpsc::Sender<Result<ExecEvent, Status>>,
    gate: Arc<Mutex<()>>,
    sequence: Arc<AtomicU64>,
    request_id: String,
}

impl ExecOrderContext {
    fn new(
        sender: tokio::sync::mpsc::Sender<Result<ExecEvent, Status>>,
        request_id: String,
    ) -> Self {
        Self {
            sender,
            gate: Arc::new(Mutex::new(())),
            sequence: Arc::new(AtomicU64::new(0)),
            request_id,
        }
    }
}

static EXEC_ORDER_CONTEXTS: OnceLock<StdMutex<HashMap<u64, ExecOrderContext>>> = OnceLock::new();

fn exec_order_contexts() -> &'static StdMutex<HashMap<u64, ExecOrderContext>> {
    EXEC_ORDER_CONTEXTS.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn with_exec_order_contexts<R>(f: impl FnOnce(&mut HashMap<u64, ExecOrderContext>) -> R) -> R {
    let mut guard = exec_order_contexts()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    f(&mut guard)
}

fn register_exec_order_context(exec_id: u64, context: ExecOrderContext) {
    with_exec_order_contexts(|contexts| {
        contexts.insert(exec_id, context);
    });
}

fn lookup_exec_order_context(exec_id: u64) -> Option<ExecOrderContext> {
    with_exec_order_contexts(|contexts| contexts.get(&exec_id).cloned())
}

fn remove_exec_order_context(exec_id: u64) {
    with_exec_order_contexts(|contexts| {
        contexts.remove(&exec_id);
    });
}

fn generated_request_id(prefix: &str) -> String {
    static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
    let seq = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{seq:016x}")
}

fn request_id_from_metadata(metadata: Option<&TransportMetadata>, prefix: &str) -> String {
    metadata
        .and_then(|metadata| {
            let trimmed = metadata.request_id.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .unwrap_or_else(|| generated_request_id(prefix))
}

async fn send_ordered_exec_event(exec_id: u64, event: exec_event::Event) -> Result<u64, ()> {
    send_ordered_exec_event_with_id(exec_id, event, 0).await
}

/// Send an ordered exec event with an explicit exec_id field in the event.
/// Used for PTY sessions where the client needs the exec_id for correlation.
async fn send_ordered_exec_event_with_id(
    exec_id: u64,
    event: exec_event::Event,
    event_exec_id: u64,
) -> Result<u64, ()> {
    let Some(context) = lookup_exec_order_context(exec_id) else {
        return Err(());
    };
    let _guard = context.gate.lock().await;
    let sequence = context.sequence.fetch_add(1, Ordering::Relaxed) + 1;
    context
        .sender
        .send(Ok(ExecEvent {
            event: Some(event),
            sequence,
            request_id: context.request_id.clone(),
            exec_id: event_exec_id,
        }))
        .await
        .map_err(|_| ())?;
    Ok(sequence)
}

async fn mark_ordered_control(exec_id: u64, operation: &str) -> Option<u64> {
    let context = lookup_exec_order_context(exec_id)?;
    let _guard = context.gate.lock().await;
    let sequence = context.sequence.fetch_add(1, Ordering::Relaxed) + 1;
    debug!(
        exec_id,
        sequence, operation, "grpc: exec control op ordered"
    );
    Some(sequence)
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

    /// Pipe-based exec (the original non-PTY path). Spawns a child process with
    /// piped stdin/stdout/stderr and streams output events back to the client.
    async fn exec_pipe(
        &self,
        req: ExecRequest,
        request_id: String,
    ) -> Result<Response<ReceiverStream<Result<ExecEvent, Status>>>, Status> {
        use tokio::io::AsyncReadExt;

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
                warn!(request_id = %request_id, command = %req.command, error = %e, "grpc: exec spawn failed");
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                let _ = tx
                    .send(Ok(ExecEvent {
                        event: Some(exec_event::Event::Error(e.to_string())),
                        sequence: 1,
                        request_id: request_id.clone(),
                        exec_id: 0,
                    }))
                    .await;
                return Ok(Response::new(ReceiverStream::new(rx)));
            }
        };

        info!(request_id = %request_id, command = %req.command, args = ?req.args, "grpc: process spawned");

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stdin = child.stdin.take();
        let exec_id = child.id().unwrap_or(0) as u64;

        {
            let mut table = self.state.process_table.lock().await;
            table.insert(exec_id, child, stdin);
        }

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ExecEvent, Status>>(64);
        register_exec_order_context(exec_id, ExecOrderContext::new(tx.clone(), request_id));

        let process_table = self.state.process_table.clone();

        let stdout_handle = tokio::spawn(async move {
            if let Some(mut stdout) = stdout {
                let mut buf = vec![0u8; 65536];
                loop {
                    match stdout.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            match send_ordered_exec_event(
                                exec_id,
                                exec_event::Event::Stdout(buf[..n].to_vec()),
                            )
                            .await
                            {
                                Ok(sequence) => {
                                    debug!(exec_id, sequence, bytes = n, "grpc: stdout chunk");
                                }
                                Err(_) => break,
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

        let stderr_handle = tokio::spawn(async move {
            if let Some(mut stderr) = stderr {
                let mut buf = vec![0u8; 65536];
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            match send_ordered_exec_event(
                                exec_id,
                                exec_event::Event::Stderr(buf[..n].to_vec()),
                            )
                            .await
                            {
                                Ok(sequence) => {
                                    debug!(exec_id, sequence, bytes = n, "grpc: stderr chunk");
                                }
                                Err(_) => break,
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

        let exit_table = process_table;
        tokio::spawn(async move {
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

            let _ = tokio::time::timeout(std::time::Duration::from_millis(500), async {
                let _ = stdout_handle.await;
                let _ = stderr_handle.await;
            })
            .await;

            info!(exec_id, exit_code, "grpc: process exited");

            if let Ok(sequence) =
                send_ordered_exec_event(exec_id, exec_event::Event::ExitCode(exit_code)).await
            {
                debug!(exec_id, sequence, "grpc: exit event");
            }

            {
                let mut table = exit_table.lock().await;
                table.remove(exec_id);
            }
            remove_exec_order_context(exec_id);
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// PTY-based exec. Allocates a pseudo-terminal via `portable-pty` and spawns
    /// the child process with the slave side as its controlling terminal. Output
    /// is read from the master and streamed as stdout events.
    async fn exec_pty(
        &self,
        req: ExecRequest,
        request_id: String,
    ) -> Result<Response<ReceiverStream<Result<ExecEvent, Status>>>, Status> {
        use portable_pty::{CommandBuilder, PtySize, native_pty_system};
        use std::io::Read;

        let rows = if req.term_rows == 0 { 24 } else { req.term_rows };
        let cols = if req.term_cols == 0 { 80 } else { req.term_cols };

        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows: rows as u16,
                cols: cols as u16,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| Status::internal(format!("openpty failed: {e}")))?;

        let mut cmd = CommandBuilder::new(&req.command);
        cmd.args(&req.args);

        if !req.working_dir.is_empty() {
            cmd.cwd(&req.working_dir);
        }

        cmd.env("TERM", "xterm-256color");
        for (key, value) in &req.env {
            cmd.env(key, value);
        }

        let child = pair.slave.spawn_command(cmd).map_err(|e| {
            warn!(command = %req.command, error = %e, "grpc: pty exec spawn failed");
            Status::internal(format!("failed to spawn PTY process: {e}"))
        })?;

        // Drop slave — only the child uses it.
        drop(pair.slave);

        let exec_id = child.process_id().unwrap_or(0) as u64;
        info!(
            request_id = %request_id, exec_id, command = %req.command,
            args = ?req.args, rows, cols, "grpc: pty process spawned"
        );

        // Get reader (cloned handle) and writer from the master.
        let mut reader = pair.master.try_clone_reader().map_err(|e| {
            Status::internal(format!("failed to clone PTY reader: {e}"))
        })?;
        let writer = pair.master.take_writer().map_err(|e| {
            Status::internal(format!("failed to take PTY writer: {e}"))
        })?;

        // Store master + writer for stdin_write and resize operations.
        {
            let mut handles = pty_handles().lock().unwrap_or_else(|p| p.into_inner());
            handles.insert(exec_id, PtyMasterHandle {
                writer,
                master: pair.master,
            });
        }

        // Insert child into process table (no stdin pipe — we use PTY writer).
        {
            let mut table = self.state.process_table.lock().await;
            // portable-pty Child isn't tokio-compatible, so we wrap it in the
            // process table as a waitable entry below instead.
            table.insert_pty(exec_id, child);
        }

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ExecEvent, Status>>(64);
        register_exec_order_context(exec_id, ExecOrderContext::new(tx.clone(), request_id));

        // Send the first event with exec_id so the client can correlate.
        if let Err(()) = send_ordered_exec_event_with_id(
            exec_id,
            exec_event::Event::Stdout(Vec::new()),
            exec_id,
        )
        .await
        {
            warn!(exec_id, "grpc: failed to send initial pty exec event");
        }

        // Spawn blocking reader task. portable-pty gives us a synchronous Read,
        // so we read in a blocking thread and forward chunks as exec events.
        let reader_exec_id = exec_id;
        let pty_reader_handle = tokio::task::spawn_blocking(move || {
            let mut buf = vec![0u8; 65536];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        let data = buf[..n].to_vec();
                        let rt = tokio::runtime::Handle::current();
                        match rt.block_on(send_ordered_exec_event(
                            reader_exec_id,
                            exec_event::Event::Stdout(data),
                        )) {
                            Ok(sequence) => {
                                debug!(exec_id = reader_exec_id, sequence, bytes = n, "grpc: pty stdout chunk");
                            }
                            Err(_) => break,
                        }
                    }
                    Err(e) => {
                        // EIO is expected when the slave side closes (child exited).
                        if e.raw_os_error() != Some(libc::EIO) {
                            warn!(exec_id = reader_exec_id, error = %e, "grpc: pty read error");
                        }
                        break;
                    }
                }
            }
        });

        // Spawn exit watcher for the PTY session.
        let exit_table = self.state.process_table.clone();
        tokio::spawn(async move {
            let exit_code = {
                let mut table = exit_table.lock().await;
                table.wait_pty(exec_id).await
            };

            // Brief window for remaining PTY output.
            let _ = tokio::time::timeout(
                std::time::Duration::from_millis(500),
                pty_reader_handle,
            )
            .await;

            info!(exec_id, exit_code, "grpc: pty process exited");

            if let Ok(sequence) =
                send_ordered_exec_event(exec_id, exec_event::Event::ExitCode(exit_code)).await
            {
                debug!(exec_id, sequence, "grpc: pty exit event");
            }

            // Clean up: remove from process table, PTY handles, and order context.
            {
                let mut table = exit_table.lock().await;
                table.remove(exec_id);
            }
            {
                let mut handles = pty_handles().lock().unwrap_or_else(|p| p.into_inner());
                handles.remove(&exec_id);
            }
            remove_exec_order_context(exec_id);
        });

        Ok(Response::new(ReceiverStream::new(rx)))
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
        let req = request.into_inner();
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "exec");

        if req.allocate_pty {
            self.exec_pty(req, request_id).await
        } else {
            self.exec_pipe(req, request_id).await
        }
    }

    async fn stdin_write(
        &self,
        request: Request<StdinWriteRequest>,
    ) -> Result<Response<StdinWriteResponse>, Status> {
        use tokio::io::AsyncWriteExt;

        let req = request.into_inner();
        if let Some(sequence) = mark_ordered_control(req.exec_id, "stdin_write").await {
            debug!(
                exec_id = req.exec_id,
                sequence,
                bytes = req.data.len(),
                "grpc: stdin write ordered"
            );
        }

        // For PTY sessions, write to the master PTY writer.
        {
            let mut handles = pty_handles().lock().unwrap_or_else(|p| p.into_inner());
            if let Some(handle) = handles.get_mut(&req.exec_id) {
                use std::io::Write;
                handle
                    .writer
                    .write_all(&req.data)
                    .map_err(|e| Status::internal(format!("pty write failed: {e}")))?;
                return Ok(Response::new(StdinWriteResponse {}));
            }
        }

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
        if let Some(sequence) = mark_ordered_control(req.exec_id, "stdin_close").await {
            debug!(exec_id = req.exec_id, sequence, "grpc: stdin close ordered");
        }
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
        if let Some(sequence) = mark_ordered_control(req.exec_id, "signal").await {
            debug!(
                exec_id = req.exec_id,
                sequence,
                signal = req.signal,
                "grpc: signal ordered"
            );
        }
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

    async fn resize_exec_pty(
        &self,
        request: Request<ResizeExecPtyRequest>,
    ) -> Result<Response<ResizeExecPtyResponse>, Status> {
        use portable_pty::PtySize;

        let req = request.into_inner();
        let mut handles = pty_handles().lock().unwrap_or_else(|p| p.into_inner());
        let handle = handles
            .get_mut(&req.exec_id)
            .ok_or_else(|| Status::not_found(format!("no PTY for exec {}", req.exec_id)))?;

        handle
            .master
            .resize(PtySize {
                rows: req.rows as u16,
                cols: req.cols as u16,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| Status::internal(format!("pty resize failed: {e}")))?;

        info!(exec_id = req.exec_id, rows = req.rows, cols = req.cols, "grpc: pty resized");
        Ok(Response::new(ResizeExecPtyResponse {}))
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
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "oci-create");
        info!(
            request_id = %request_id,
            container_id = %req.container_id,
            bundle_path = %req.bundle_path,
            "oci: create"
        );

        // Patch the OCI config to work in the minimal guest VM kernel.
        let config_path = format!("{}/config.json", &req.bundle_path);
        match patch_oci_config(&config_path).await {
            Ok(()) => info!(container_id = %req.container_id, "oci: config patched for guest VM"),
            Err(e) => {
                error!(container_id = %req.container_id, error = %e, "oci: failed to patch config")
            }
        }

        // Log bundle config for diagnostics.
        match tokio::fs::read_to_string(&config_path).await {
            Ok(config) => {
                info!(container_id = %req.container_id, config = %config, "oci: bundle config")
            }
            Err(e) => {
                error!(container_id = %req.container_id, error = %e, "oci: failed to read bundle config")
            }
        }

        run_youki(&["create", "--bundle", &req.bundle_path, &req.container_id]).await?;
        Ok(Response::new(OciCreateResponse {}))
    }

    async fn start(
        &self,
        request: Request<OciStartRequest>,
    ) -> Result<Response<OciStartResponse>, Status> {
        let req = request.into_inner();
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "oci-start");
        info!(request_id = %request_id, container_id = %req.container_id, "oci: start");

        run_youki(&["start", &req.container_id]).await?;
        Ok(Response::new(OciStartResponse {}))
    }

    async fn state(
        &self,
        request: Request<OciStateRequest>,
    ) -> Result<Response<OciStateResponse>, Status> {
        let req = request.into_inner();
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "oci-state");
        debug!(request_id = %request_id, container_id = %req.container_id, "oci: state");

        let output =
            run_youki_output(&["state", &req.container_id], YOUKI_LIFECYCLE_TIMEOUT).await?;
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
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "oci-exec");
        info!(
            request_id = %request_id,
            container_id = %req.container_id,
            command = %req.command,
            "oci: exec"
        );

        // Youki 0.5.7 exec doesn't properly enter the container's mount
        // namespace, causing commands to see the initramfs instead of the
        // container rootfs. Work around this by using nsenter: get the init
        // PID from `youki state`, then nsenter into its namespaces.
        let state_output =
            run_youki_output(&["state", &req.container_id], YOUKI_LIFECYCLE_TIMEOUT).await?;
        let state: serde_json::Value = serde_json::from_slice(&state_output.stdout)
            .map_err(|e| Status::internal(format!("failed to parse youki state: {e}")))?;
        let pid = state["pid"]
            .as_u64()
            .ok_or_else(|| Status::internal("youki state missing pid field"))?;

        let mut nsenter_args: Vec<String> = vec![
            format!("--mount=/proc/{pid}/ns/mnt"),
            format!("--net=/proc/{pid}/ns/net"),
            format!("--pid=/proc/{pid}/ns/pid"),
            format!("--ipc=/proc/{pid}/ns/ipc"),
            format!("--uts=/proc/{pid}/ns/uts"),
            format!("--root=/proc/{pid}/root"),
        ];
        if !req.working_dir.is_empty() {
            nsenter_args.push(format!("--wd={}", req.working_dir));
        }
        nsenter_args.push("--".into());

        nsenter_args.push(req.command.clone());
        nsenter_args.extend(req.args.clone());

        info!(pid = pid, args = ?nsenter_args, "oci: exec via nsenter");

        let mut cmd = tokio::process::Command::new("nsenter");
        for arg in nsenter_args {
            cmd.arg(arg);
        }

        cmd.env_clear();
        let has_path = req.env.keys().any(|k| k == "PATH");
        if !has_path {
            cmd.env(
                "PATH",
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            );
        }
        cmd.envs(&req.env);
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
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "oci-kill");
        info!(
            request_id = %request_id,
            container_id = %req.container_id,
            signal = %req.signal,
            "oci: kill"
        );

        run_youki(&["kill", &req.container_id, &req.signal]).await?;
        Ok(Response::new(OciKillResponse {}))
    }

    async fn delete(
        &self,
        request: Request<OciDeleteRequest>,
    ) -> Result<Response<OciDeleteResponse>, Status> {
        let req = request.into_inner();
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "oci-delete");
        info!(
            request_id = %request_id,
            container_id = %req.container_id,
            force = req.force,
            "oci: delete"
        );

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
        let youki_log = tokio::fs::read_to_string(&log_file)
            .await
            .unwrap_or_default();
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
            Err(Status::internal(format!(
                "{cmd_desc} timed out after {}s",
                timeout.as_secs()
            )))
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
                tracing::info!(
                    mount_type = typ,
                    "stripping unsupported mount type from OCI config"
                );
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
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "network-setup");
        debug!(
            request_id = %request_id,
            stack_id = %req.stack_id,
            services = req.services.len(),
            "grpc: network setup request"
        );
        do_network_setup(&req.stack_id, &req.services)
    }

    async fn teardown(
        &self,
        request: Request<NetworkTeardownRequest>,
    ) -> Result<Response<NetworkTeardownResponse>, Status> {
        let req = request.into_inner();
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "network-teardown");
        debug!(
            request_id = %request_id,
            stack_id = %req.stack_id,
            services = req.service_names.len(),
            "grpc: network teardown request"
        );
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn test_exec_id() -> u64 {
        static NEXT_EXEC_ID: AtomicU64 = AtomicU64::new(10_000);
        NEXT_EXEC_ID.fetch_add(1, Ordering::Relaxed)
    }

    #[tokio::test]
    async fn exec_order_sequence_is_monotonic_across_control_ops() {
        let exec_id = test_exec_id();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<ExecEvent, Status>>(8);
        register_exec_order_context(exec_id, ExecOrderContext::new(tx, "req-test".to_string()));

        let first_event = match send_ordered_exec_event(
            exec_id,
            exec_event::Event::Stdout(b"a".to_vec()),
        )
        .await
        {
            Ok(sequence) => sequence,
            Err(()) => panic!("first event should send"),
        };
        let control = match mark_ordered_control(exec_id, "stdin_close").await {
            Some(sequence) => sequence,
            None => panic!("control op should be ordered"),
        };
        let second_event = match send_ordered_exec_event(
            exec_id,
            exec_event::Event::Stderr(b"b".to_vec()),
        )
        .await
        {
            Ok(sequence) => sequence,
            Err(()) => panic!("second event should send"),
        };

        assert_eq!(first_event, 1);
        assert_eq!(control, 2);
        assert_eq!(second_event, 3);

        let first = rx.recv().await;
        assert!(matches!(
            first,
            Some(Ok(ExecEvent {
                sequence: 1,
                request_id,
                event: Some(exec_event::Event::Stdout(_)),
                ..
            })) if request_id == "req-test"
        ));
        let second = rx.recv().await;
        assert!(matches!(
            second,
            Some(Ok(ExecEvent {
                sequence: 3,
                request_id,
                event: Some(exec_event::Event::Stderr(_)),
                ..
            })) if request_id == "req-test"
        ));

        remove_exec_order_context(exec_id);
    }

    #[tokio::test]
    async fn ordered_send_and_control_require_registered_exec_context() {
        let exec_id = test_exec_id();
        remove_exec_order_context(exec_id);

        let sent = send_ordered_exec_event(exec_id, exec_event::Event::ExitCode(0)).await;
        assert!(sent.is_err());
        let control = mark_ordered_control(exec_id, "signal").await;
        assert!(control.is_none());
    }
}
