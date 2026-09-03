//! gRPC service implementations for the guest agent.
//!
//! Each service struct holds shared state (process table, etc.) and delegates
//! to the existing handler logic in the parent module. This bridges from
//! protobuf request/response types to the underlying handler functions.

// tonic::Status is the canonical error type for all gRPC service methods;
// its size is dictated by the tonic crate and cannot be reduced here.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
#[cfg(any(target_os = "linux", test))]
use std::path::{Path, PathBuf};
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

static PTY_HANDLES: OnceLock<StdMutex<HashMap<u64, Arc<StdMutex<PtyMasterHandle>>>>> =
    OnceLock::new();

fn pty_handles() -> &'static StdMutex<HashMap<u64, Arc<StdMutex<PtyMasterHandle>>>> {
    PTY_HANDLES.get_or_init(|| StdMutex::new(HashMap::new()))
}

#[cfg(target_os = "linux")]
fn devpts_is_mounted() -> bool {
    let Ok(mounts) = std::fs::read_to_string("/proc/mounts") else {
        return false;
    };
    mounts.lines().any(|line| {
        let mut fields = line.split_whitespace();
        let _source = fields.next();
        let target = fields.next();
        let fs_type = fields.next();
        target == Some("/dev/pts") && fs_type == Some("devpts")
    })
}

#[cfg(target_os = "linux")]
fn ensure_devpts_ready() -> Result<(), Status> {
    use std::ffi::CString;
    use std::os::unix::fs::symlink;
    use std::path::Path;

    std::fs::create_dir_all("/dev/pts")
        .map_err(|error| Status::internal(format!("failed to create /dev/pts: {error}")))?;

    if !devpts_is_mounted() {
        let source = CString::new("devpts")
            .map_err(|error| Status::internal(format!("invalid devpts source: {error}")))?;
        let target = CString::new("/dev/pts")
            .map_err(|error| Status::internal(format!("invalid devpts target: {error}")))?;
        let fs_type = CString::new("devpts")
            .map_err(|error| Status::internal(format!("invalid devpts fs type: {error}")))?;
        let data = CString::new("ptmxmode=0666,mode=0620")
            .map_err(|error| Status::internal(format!("invalid devpts mount options: {error}")))?;
        let mount_result = unsafe {
            libc::mount(
                source.as_ptr(),
                target.as_ptr(),
                fs_type.as_ptr(),
                0,
                data.as_ptr().cast(),
            )
        };
        if mount_result != 0 {
            let mount_error = std::io::Error::last_os_error();
            if mount_error.raw_os_error() != Some(libc::EBUSY) {
                return Err(Status::internal(format!(
                    "failed to mount devpts at /dev/pts: {mount_error}"
                )));
            }
        }
    }

    let ptmx_path = Path::new("/dev/ptmx");
    if !ptmx_path.exists() {
        symlink("pts/ptmx", ptmx_path).map_err(|error| {
            Status::internal(format!("failed to create /dev/ptmx symlink: {error}"))
        })?;
    }

    Ok(())
}

// ── Shared state passed to all service impls ────────────────────────

/// Shared state accessible by all gRPC service implementations.
#[derive(Clone)]
pub struct SharedState {
    /// Process table for tracking spawned child processes.
    pub process_table: Arc<Mutex<ProcessTable>>,
    /// Docker facade supervisor. Construction is inert; `EnsureDocker` starts it.
    pub docker_supervisor: Arc<crate::docker::DockerSupervisor>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedAgentExec {
    command: String,
    args: Vec<String>,
    spawn_working_dir: Option<String>,
    spawn_user: Option<String>,
    spawn_environment: Vec<(String, String)>,
    clear_environment: bool,
    container_targeted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContainerExecProcessSpec {
    trampoline: crate::container_exec::TrampolineCommand,
    environment: Vec<(String, String)>,
}

const DEFAULT_CONTAINER_EXEC_PATH: &str =
    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

fn normalized_container_environment(
    environment: &HashMap<String, String>,
) -> Result<Vec<(String, String)>, Status> {
    let mut normalized = environment
        .iter()
        .map(|(key, value)| {
            if key.is_empty() || key.contains('=') || key.contains('\0') || value.contains('\0') {
                return Err(Status::invalid_argument(
                    "container exec environment contains an invalid key or NUL byte",
                ));
            }
            Ok((key.clone(), value.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !environment.contains_key("PATH") {
        normalized.push(("PATH".to_string(), DEFAULT_CONTAINER_EXEC_PATH.to_string()));
    }
    normalized.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(normalized)
}

fn normalized_container_exec(
    container_id: &str,
    command: &str,
    args: &[String],
    working_dir: Option<&str>,
    user: Option<&str>,
    environment: &HashMap<String, String>,
) -> Result<ContainerExecProcessSpec, Status> {
    let environment = normalized_container_environment(environment)?;
    let trampoline = crate::container_exec::prepare_trampoline(
        container_id,
        command,
        args,
        working_dir,
        user,
        environment.iter().any(|(key, _)| key == "SHELL"),
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(ContainerExecProcessSpec {
        trampoline,
        environment,
    })
}

fn prepare_agent_exec(req: &ExecRequest) -> Result<PreparedAgentExec, Status> {
    if let Some(target) = &req.container_target {
        let spec = normalized_container_exec(
            &target.container_id,
            &req.command,
            &req.args,
            (!req.working_dir.is_empty()).then_some(req.working_dir.as_str()),
            (!req.user.is_empty()).then_some(req.user.as_str()),
            &req.env,
        )?;
        return Ok(PreparedAgentExec {
            command: spec.trampoline.program,
            args: spec.trampoline.args,
            // Cwd and user are applied inside the pinned container root and
            // namespaces, never to the trampoline in the guest namespace.
            spawn_working_dir: None,
            spawn_user: None,
            spawn_environment: spec.environment,
            clear_environment: true,
            container_targeted: true,
        });
    }

    Ok(PreparedAgentExec {
        command: req.command.clone(),
        args: req.args.clone(),
        spawn_working_dir: (!req.working_dir.is_empty()).then(|| req.working_dir.clone()),
        spawn_user: (!req.user.is_empty()).then(|| req.user.clone()),
        spawn_environment: req
            .env
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        clear_environment: false,
        container_targeted: false,
    })
}

#[cfg(any(target_os = "linux", test))]
fn prepare_oci_exec(req: &OciExecRequest) -> Result<ContainerExecProcessSpec, Status> {
    normalized_container_exec(
        &req.container_id,
        &req.command,
        &req.args,
        (!req.working_dir.is_empty()).then_some(req.working_dir.as_str()),
        (!req.user.is_empty()).then_some(req.user.as_str()),
        &req.env,
    )
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

        let launch = prepare_agent_exec(&req)?;
        let spawn_result = if let Some(ref username) = launch.spawn_user {
            crate::spawn_as_user(
                username,
                &launch.command,
                &launch.args,
                launch.spawn_working_dir.as_deref(),
                &launch.spawn_environment,
                launch.clear_environment,
            )
        } else {
            crate::spawn_direct(
                &launch.command,
                &launch.args,
                launch.spawn_working_dir.as_deref(),
                &launch.spawn_environment,
                launch.clear_environment,
            )
        };

        let mut child = match spawn_result {
            Ok(child) => child,
            Err(e) => {
                warn!(request_id = %request_id, command = %launch.command, error = %e, "grpc: exec spawn failed");
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

        info!(request_id = %request_id, command = %launch.command, args = ?launch.args, container_targeted = launch.container_targeted, "grpc: process spawned");

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
            // Never hold the global process table lock while waiting for process exit.
            // Otherwise a slow/hung non-PTY command can block unrelated PTY exec setup.
            let exit_code = loop {
                let poll = {
                    let mut table = exit_table.lock().await;
                    let Some(entry) = table.get_mut(exec_id) else {
                        break -1;
                    };
                    match entry.child.try_wait() {
                        Ok(Some(status)) => Some(status.code().unwrap_or(-1)),
                        Ok(None) => None,
                        Err(error) => {
                            warn!(exec_id, error = %error, "grpc: wait error");
                            Some(-1)
                        }
                    }
                };
                if let Some(code) = poll {
                    break code;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
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

        info!(
            request_id = %request_id,
            command = %req.command,
            args = ?req.args,
            "grpc: pty exec request received"
        );

        let launch = prepare_agent_exec(&req)?;
        let rows = if req.term_rows == 0 {
            24
        } else {
            req.term_rows
        };
        let cols = if req.term_cols == 0 {
            80
        } else {
            req.term_cols
        };

        #[cfg(target_os = "linux")]
        ensure_devpts_ready()?;

        let pty_system = native_pty_system();
        info!(
            request_id = %request_id,
            rows,
            cols,
            "grpc: opening PTY pair"
        );
        let pair = pty_system
            .openpty(PtySize {
                rows: rows as u16,
                cols: cols as u16,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| Status::internal(format!("openpty failed: {e}")))?;
        info!(request_id = %request_id, "grpc: PTY pair opened");

        let mut cmd = CommandBuilder::new(&launch.command);
        cmd.args(&launch.args);

        if let Some(working_dir) = &launch.spawn_working_dir {
            cmd.cwd(working_dir);
        }

        if launch.clear_environment {
            cmd.env_clear();
        } else {
            cmd.env("TERM", "xterm-256color");
        }
        for (key, value) in &launch.spawn_environment {
            cmd.env(key, value);
        }

        info!(
            request_id = %request_id,
            command = %launch.command,
            "grpc: spawning PTY process"
        );
        let child = pair.slave.spawn_command(cmd).map_err(|e| {
            warn!(command = %launch.command, error = %e, "grpc: pty exec spawn failed");
            Status::internal(format!("failed to spawn PTY process: {e}"))
        })?;
        info!(request_id = %request_id, "grpc: PTY process spawned");

        // Drop slave — only the child uses it.
        drop(pair.slave);

        let exec_id = child.process_id().unwrap_or(0) as u64;
        info!(
            request_id = %request_id, exec_id, command = %launch.command,
            args = ?launch.args, rows, cols, container_targeted = launch.container_targeted,
            "grpc: pty process spawned"
        );

        // Get reader (cloned handle) and writer from the master.
        let mut reader = pair
            .master
            .try_clone_reader()
            .map_err(|e| Status::internal(format!("failed to clone PTY reader: {e}")))?;
        let writer = pair
            .master
            .take_writer()
            .map_err(|e| Status::internal(format!("failed to take PTY writer: {e}")))?;

        // Store master + writer for stdin_write and resize operations.
        {
            let mut handles = pty_handles().lock().unwrap_or_else(|p| p.into_inner());
            handles.insert(
                exec_id,
                Arc::new(StdMutex::new(PtyMasterHandle {
                    writer,
                    master: pair.master,
                })),
            );
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
        info!(exec_id, "grpc: sending initial PTY exec event");
        if let Err(()) =
            send_ordered_exec_event_with_id(exec_id, exec_event::Event::Stdout(Vec::new()), exec_id)
                .await
        {
            warn!(exec_id, "grpc: failed to send initial pty exec event");
        }
        info!(exec_id, "grpc: initial PTY exec event sent");

        // Spawn blocking reader task. portable-pty gives us a synchronous Read,
        // so we read in a blocking thread and forward chunks as exec events.
        let reader_exec_id = exec_id;
        let (reader_done_tx, mut reader_done_rx) = tokio::sync::oneshot::channel::<()>();
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
                                debug!(
                                    exec_id = reader_exec_id,
                                    sequence,
                                    bytes = n,
                                    "grpc: pty stdout chunk"
                                );
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
            let _ = reader_done_tx.send(());
        });

        // Spawn exit watcher for the PTY session.
        let exit_table = self.state.process_table.clone();
        tokio::spawn(async move {
            let child = {
                let mut table = exit_table.lock().await;
                table.take_pty(exec_id)
            };

            let mut wait_handle = child.map(|mut child| {
                tokio::task::spawn_blocking(move || match child.wait() {
                    Ok(status) => status.exit_code() as i32,
                    Err(_) => -1,
                })
            });

            let exit_code = if let Some(wait_handle) = wait_handle.as_mut() {
                tokio::select! {
                    result = &mut *wait_handle => {
                        result.unwrap_or(-1)
                    }
                    _ = &mut reader_done_rx => {
                        match tokio::time::timeout(
                            std::time::Duration::from_secs(2),
                            &mut *wait_handle,
                        )
                        .await
                        {
                            Ok(result) => result.unwrap_or(-1),
                            Err(_) => {
                                warn!(
                                    exec_id,
                                    "grpc: pty reader closed but wait() did not resolve; forcing exit event"
                                );
                                wait_handle.abort();
                                -1
                            }
                        }
                    }
                }
            } else {
                -1
            };

            // Brief window for remaining PTY output.
            let _ = tokio::time::timeout(std::time::Duration::from_millis(500), pty_reader_handle)
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
            agent_protocol_revision: vz_agent_proto::AGENT_PROTOCOL_REVISION,
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

    type EnsureDockerStream = ReceiverStream<Result<DockerEnsureEvent, Status>>;

    async fn exec(
        &self,
        request: Request<ExecRequest>,
    ) -> Result<Response<Self::ExecStream>, Status> {
        let req = request.into_inner();
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "exec");
        info!(
            request_id = %request_id,
            allocate_pty = req.allocate_pty,
            command = %req.command,
            "grpc: exec request routing"
        );

        if req.allocate_pty {
            self.exec_pty(req, request_id).await
        } else {
            self.exec_pipe(req, request_id).await
        }
    }

    async fn ensure_docker(
        &self,
        request: Request<DockerEnsureRequest>,
    ) -> Result<Response<Self::EnsureDockerStream>, Status> {
        let request = request.into_inner();
        let request_id = request_id_from_metadata(request.metadata.as_ref(), "ensure-docker");
        let supervisor = Arc::clone(&self.state.docker_supervisor);
        let (sender, receiver) = tokio::sync::mpsc::channel(8);

        tokio::spawn(async move {
            use docker_ensure_event::Stage;

            if sender
                .send(Ok(DockerEnsureEvent {
                    stage: Stage::Validating as i32,
                    message: "Validating persistent Docker facade artifacts and mounts".to_string(),
                    socket_path: String::new(),
                }))
                .await
                .is_err()
            {
                return;
            }

            if let Err(error) = supervisor.ensure_started().await {
                warn!(request_id = %request_id, %error, "Docker facade startup validation failed");
                let _ = sender
                    .send(Err(Status::failed_precondition(format!(
                        "Docker facade startup failed: {error}"
                    ))))
                    .await;
                return;
            }

            for (stage, message) in [
                (Stage::Starting, "Guest-agent Docker supervision started"),
                (
                    Stage::Waiting,
                    "Waiting for the guest Docker Engine API socket",
                ),
            ] {
                if sender
                    .send(Ok(DockerEnsureEvent {
                        stage: stage as i32,
                        message: message.to_string(),
                        socket_path: String::new(),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }

            match supervisor.wait_ready().await {
                Ok(socket_path) => {
                    info!(request_id = %request_id, socket_path, "Docker facade ready");
                    let _ = sender
                        .send(Ok(DockerEnsureEvent {
                            stage: Stage::Ready as i32,
                            message: "Docker facade ready".to_string(),
                            socket_path: socket_path.to_string(),
                        }))
                        .await;
                }
                Err(error) => {
                    warn!(request_id = %request_id, %error, "Docker facade startup failed");
                    let _ = sender
                        .send(Err(Status::failed_precondition(format!(
                            "Docker facade startup failed: {error}"
                        ))))
                        .await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
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
        let pty_handle = {
            let handles = pty_handles().lock().unwrap_or_else(|p| p.into_inner());
            handles.get(&req.exec_id).cloned()
        };
        if let Some(handle) = pty_handle {
            let data = req.data.clone();
            tokio::task::spawn_blocking(move || -> Result<(), Status> {
                use std::io::Write;
                let mut guard = handle.lock().unwrap_or_else(|p| p.into_inner());
                guard
                    .writer
                    .write_all(&data)
                    .map_err(|e| Status::internal(format!("pty write failed: {e}")))
            })
            .await
            .map_err(|error| Status::internal(format!("pty write task failed: {error}")))??;
            return Ok(Response::new(StdinWriteResponse {}));
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
        let handle = {
            let handles = pty_handles().lock().unwrap_or_else(|p| p.into_inner());
            handles.get(&req.exec_id).cloned()
        }
        .ok_or_else(|| Status::not_found(format!("no PTY for exec {}", req.exec_id)))?;

        let rows = req.rows as u16;
        let cols = req.cols as u16;
        tokio::task::spawn_blocking(move || -> Result<(), Status> {
            let guard = handle.lock().unwrap_or_else(|p| p.into_inner());
            guard
                .master
                .resize(PtySize {
                    rows,
                    cols,
                    pixel_width: 0,
                    pixel_height: 0,
                })
                .map_err(|e| Status::internal(format!("pty resize failed: {e}")))
        })
        .await
        .map_err(|error| Status::internal(format!("pty resize task failed: {error}")))??;

        info!(
            exec_id = req.exec_id,
            rows = req.rows,
            cols = req.cols,
            "grpc: pty resized"
        );
        Ok(Response::new(ResizeExecPtyResponse {}))
    }
}

// ── OciService ──────────────────────────────────────────────────────

/// Path to the youki OCI runtime binary (delivered via VirtioFS).
#[cfg(target_os = "linux")]
const YOUKI_BIN: &str = crate::container_exec::YOUKI_BIN;

/// Root directory for youki container state.
#[cfg(target_os = "linux")]
const YOUKI_ROOT: &str = crate::container_exec::YOUKI_ROOT;

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
                info!(container_id = %req.container_id, config = %config, "oci: bundle config");

                let mountinfo = tokio::fs::read_to_string("/proc/self/mountinfo")
                    .await
                    .unwrap_or_else(|error| format!("<unavailable: {error}>"));
                let diagnostic = inspect_oci_rootfs(&req.bundle_path, &config, &mountinfo)
                    .map_err(|diagnostic| {
                        error!(container_id = %req.container_id, %diagnostic, "oci: rootfs preflight failed");
                        Status::failed_precondition(diagnostic)
                    })?;
                info!(
                    container_id = %req.container_id,
                    configured_rootfs = %diagnostic.configured.display(),
                    resolved_rootfs = %diagnostic.resolved.display(),
                    canonical_rootfs = %diagnostic.canonical.display(),
                    mountinfo = %diagnostic.mountinfo,
                    "oci: rootfs preflight passed"
                );

                ensure_youki_user_namespace_procfs(Path::new("/proc/self/uid_map")).map_err(
                    |kernel_diagnostic| {
                        let diagnostic = format!(
                            "{kernel_diagnostic}; config.root.path={} canonical={} mountinfo={}",
                            diagnostic.configured.display(),
                            diagnostic.canonical.display(),
                            diagnostic.mountinfo
                        );
                        error!(container_id = %req.container_id, %diagnostic, "oci: youki kernel preflight failed");
                        Status::failed_precondition(diagnostic)
                    },
                )?;
            }
            Err(e) => {
                error!(container_id = %req.container_id, error = %e, "oci: failed to read bundle config");
                return Err(Status::failed_precondition(format!(
                    "OCI bundle preflight failed: cannot read {config_path}: {e}"
                )));
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

        let spec = prepare_oci_exec(&req)?;
        let trampoline = spec.trampoline;
        info!(args = ?trampoline.args, "oci: exec via verified container trampoline");

        let mut cmd = tokio::process::Command::new(&trampoline.program);
        cmd.args(&trampoline.args);

        cmd.env_clear();
        cmd.envs(spec.environment);
        cmd.kill_on_drop(true);

        let output = match tokio::time::timeout(YOUKI_EXEC_TIMEOUT, cmd.output()).await {
            Ok(Ok(output)) => output,
            Ok(Err(e)) => {
                return Err(Status::internal(format!(
                    "failed to execute container command: {e}"
                )));
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

        let mut id_components = Path::new(&req.container_id).components();
        let state_name = match (id_components.next(), id_components.next()) {
            (Some(std::path::Component::Normal(name)), None) => name,
            _ => return Err(Status::invalid_argument("invalid OCI container ID")),
        };
        let state_path = Path::new(YOUKI_ROOT).join(state_name);
        if req.force && !state_path.exists() {
            debug!(
                request_id = %request_id,
                container_id = %req.container_id,
                "oci: forced delete is already complete"
            );
            return Ok(Response::new(OciDeleteResponse {}));
        }

        if req.force {
            let delete = run_youki(&["delete", "--force", &req.container_id]).await;
            if delete.is_err() && !state_path.exists() {
                // youki may report failure after another cleanup wins the race.
                // Exact absence from its configured state root makes forced
                // delete idempotently complete; transport and genuine cleanup
                // failures retain state and continue to surface.
                debug!(
                    request_id = %request_id,
                    container_id = %req.container_id,
                    "oci: forced delete completed concurrently"
                );
            } else {
                delete?;
            }
        } else {
            run_youki(&["delete", &req.container_id]).await?;
        }
        Ok(Response::new(OciDeleteResponse {}))
    }
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, PartialEq, Eq)]
struct OciRootfsDiagnostic {
    configured: PathBuf,
    resolved: PathBuf,
    canonical: PathBuf,
    mountinfo: String,
}

/// Resolve the OCI rootfs exactly as youki does and retain the governing
/// mountinfo entry. This turns otherwise context-free ENOENT failures into an
/// actionable guest diagnostic before youki forks its init process.
#[cfg(any(target_os = "linux", test))]
fn inspect_oci_rootfs(
    bundle_path: &str,
    config_json: &str,
    mountinfo: &str,
) -> Result<OciRootfsDiagnostic, String> {
    let config: serde_json::Value = serde_json::from_str(config_json)
        .map_err(|error| format!("OCI rootfs preflight failed: invalid config.json: {error}"))?;
    let configured = config
        .pointer("/root/path")
        .and_then(serde_json::Value::as_str)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| "OCI rootfs preflight failed: config.root.path is missing".to_string())?;
    let resolved = if configured.is_absolute() {
        configured.clone()
    } else {
        Path::new(bundle_path).join(&configured)
    };
    let governing_mount = governing_mountinfo_entry(mountinfo, &resolved)
        .unwrap_or_else(|| "<no matching mountinfo entry>".to_string());
    let canonical = std::fs::canonicalize(&resolved).map_err(|error| {
        format!(
            "OCI rootfs preflight failed: config.root.path={} resolved={} cannot be canonicalized: {error}; mountinfo={governing_mount}",
            configured.display(),
            resolved.display()
        )
    })?;
    if !canonical.is_dir() {
        return Err(format!(
            "OCI rootfs preflight failed: config.root.path={} canonical={} is not a directory; mountinfo={governing_mount}",
            configured.display(),
            canonical.display()
        ));
    }

    Ok(OciRootfsDiagnostic {
        configured,
        resolved,
        canonical,
        mountinfo: governing_mount,
    })
}

#[cfg(any(target_os = "linux", test))]
fn governing_mountinfo_entry(mountinfo: &str, path: &Path) -> Option<String> {
    mountinfo
        .lines()
        .filter_map(|line| {
            let mount_point = line.split_whitespace().nth(4)?;
            let mount_point = Path::new(mount_point);
            path.starts_with(mount_point)
                .then(|| (mount_point.components().count(), line.to_string()))
        })
        .max_by_key(|(depth, _)| *depth)
        .map(|(_, line)| line)
}

/// Youki 0.5.7 unconditionally probes this procfs file from its init process.
/// It is absent when CONFIG_USER_NS is disabled, which youki reports only as
/// `io error: ENOENT` and can easily be mistaken for a missing OCI rootfs.
#[cfg(any(target_os = "linux", test))]
fn ensure_youki_user_namespace_procfs(uid_map_path: &Path) -> Result<(), String> {
    std::fs::read_to_string(uid_map_path)
        .map(|_| ())
        .map_err(|error| {
            format!(
                "youki kernel preflight failed: {} is unavailable: {error}; the guest kernel must enable CONFIG_USER_NS=y",
                uid_map_path.display()
            )
        })
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
    if *subcmd == "create" {
        cmd.arg("--log-level").arg("debug");
    }
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
/// (e.g. mqueue and cgroup v1). This function removes or adjusts mounts that
/// would cause youki to fail or hang while preserving the cgroup v2 surface
/// required for OCI resource enforcement.
#[cfg(target_os = "linux")]
async fn patch_oci_config(config_path: &str) -> Result<(), Status> {
    let content = tokio::fs::read_to_string(config_path)
        .await
        .map_err(|e| Status::internal(format!("read config.json: {e}")))?;

    let mut config: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| Status::internal(format!("parse config.json: {e}")))?;

    normalize_oci_config(&mut config);

    let patched = serde_json::to_string_pretty(&config)
        .map_err(|e| Status::internal(format!("serialize config.json: {e}")))?;

    tokio::fs::write(config_path, patched)
        .await
        .map_err(|e| Status::internal(format!("write config.json: {e}")))?;

    Ok(())
}

/// Normalize an OCI config for the guest kernel without discarding the
/// read-only cgroup v2 view used to inspect and enforce container resources.
#[cfg(any(target_os = "linux", test))]
fn normalize_oci_config(config: &mut serde_json::Value) {
    // Remove mounts with filesystem types not available in the minimal kernel.
    // cgroup2 is explicitly supported and must retain the read-only options
    // supplied by the host bundle. Types that still hang or fail include
    // mqueue (CONFIG_POSIX_MQUEUE), devpts, sysfs, and cgroup v1.
    if let Some(mounts) = config.pointer_mut("/mounts").and_then(|v| v.as_array_mut()) {
        let supported_types = ["proc", "tmpfs", "bind", "cgroup2"];
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
            // Strip unsupported namespaces but preserve mount, network, and
            // cgroup. The cgroup namespace makes the container's delegated
            // cgroup appear at the root of its read-only cgroup2 mount.
            // Network namespaces MUST be preserved — multi-service stacks
            // use per-service netns (e.g. /var/run/netns/svc-web) for
            // container network isolation and service discovery.
            if let Some(namespaces) = obj.get_mut("namespaces").and_then(|v| v.as_array_mut()) {
                let before = namespaces.len();
                namespaces.retain(|ns| {
                    let typ = ns.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    matches!(typ, "mount" | "network" | "cgroup")
                });
                let stripped = before - namespaces.len();
                if stripped > 0 {
                    tracing::info!(stripped, "stripped unsupported namespaces from OCI config");
                }
            }
        }
    }
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
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::ffi::OsString;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn test_exec_id() -> u64 {
        static NEXT_EXEC_ID: AtomicU64 = AtomicU64::new(10_000);
        NEXT_EXEC_ID.fetch_add(1, Ordering::Relaxed)
    }

    fn exec_request(container_id: Option<&str>, allocate_pty: bool) -> ExecRequest {
        ExecRequest {
            command: "/bin/printf".to_string(),
            args: vec!["%s".to_string(), "$HOME;not-a-shell".to_string()],
            working_dir: "/workspace".to_string(),
            env: [("MODE".to_string(), "test".to_string())]
                .into_iter()
                .collect(),
            user: String::new(),
            metadata: None,
            allocate_pty,
            term_rows: 24,
            term_cols: 80,
            container_target: container_id.map(|container_id| ContainerExecTarget {
                container_id: container_id.to_string(),
            }),
        }
    }

    #[test]
    fn ordinary_guest_exec_remains_direct_and_does_not_select_trampoline() {
        let prepared = prepare_agent_exec(&exec_request(None, false)).unwrap();
        assert!(!prepared.container_targeted);
        assert_eq!(prepared.command, "/bin/printf");
        assert_eq!(prepared.args, ["%s", "$HOME;not-a-shell"]);
        assert_eq!(prepared.spawn_working_dir.as_deref(), Some("/workspace"));
        assert!(!crate::container_exec::is_trampoline_request(
            &prepared
                .args
                .iter()
                .cloned()
                .map(OsString::from)
                .collect::<Vec<_>>()
        ));
    }

    #[test]
    fn pipe_and_pty_container_requests_route_through_one_trampoline() {
        let pipe = prepare_agent_exec(&exec_request(Some("web"), false)).unwrap();
        let pty = prepare_agent_exec(&exec_request(Some("web"), true)).unwrap();
        assert!(pipe.container_targeted);
        assert_eq!(pipe, pty);
        assert_eq!(pipe.command, "/proc/self/exe");
        assert!(crate::container_exec::is_trampoline_request(
            &pipe
                .args
                .iter()
                .cloned()
                .map(OsString::from)
                .collect::<Vec<_>>()
        ));
        assert!(pipe.spawn_working_dir.is_none());
        assert!(pipe.spawn_user.is_none());
        assert!(pipe.clear_environment);
        assert_eq!(
            pipe.spawn_environment,
            [
                ("MODE".to_string(), "test".to_string()),
                ("PATH".to_string(), DEFAULT_CONTAINER_EXEC_PATH.to_string())
            ]
        );
    }

    #[test]
    fn unary_oci_exec_uses_the_same_trampoline_and_preserves_argv() {
        let agent = prepare_agent_exec(&exec_request(Some("web"), false)).unwrap();
        let unary = prepare_oci_exec(&OciExecRequest {
            container_id: "web".to_string(),
            command: "/bin/printf".to_string(),
            args: vec!["%s".to_string(), "$HOME;not-a-shell".to_string()],
            env: [("MODE".to_string(), "test".to_string())]
                .into_iter()
                .collect(),
            working_dir: "/workspace".to_string(),
            user: String::new(),
            metadata: None,
        })
        .unwrap();

        assert_eq!(agent.command, unary.trampoline.program);
        assert_eq!(agent.args, unary.trampoline.args);
        assert_eq!(agent.spawn_environment, unary.environment);
    }

    #[test]
    fn every_container_exec_adapter_has_one_exact_environment_and_user_spec() {
        let mut request = exec_request(Some("web"), false);
        request.user = "dev:builders".to_string();
        request
            .env
            .insert("PATH".to_string(), "/custom/bin".to_string());
        request.env.insert("Z_LAST".to_string(), "last".to_string());

        let pipe = prepare_agent_exec(&request).unwrap();
        request.allocate_pty = true;
        let pty = prepare_agent_exec(&request).unwrap();
        let unary = prepare_oci_exec(&OciExecRequest {
            container_id: "web".to_string(),
            command: request.command,
            args: request.args,
            env: request.env,
            working_dir: request.working_dir,
            user: request.user,
            metadata: None,
        })
        .unwrap();

        assert_eq!(pipe, pty);
        assert_eq!(pipe.args, unary.trampoline.args);
        assert_eq!(pipe.spawn_environment, unary.environment);
        assert_eq!(
            pipe.spawn_environment,
            [
                ("MODE".to_string(), "test".to_string()),
                ("PATH".to_string(), "/custom/bin".to_string()),
                ("Z_LAST".to_string(), "last".to_string()),
            ]
        );
    }

    #[test]
    fn container_exec_environment_rejects_invalid_entries() {
        for key in ["", "BAD=KEY", "BAD\0KEY"] {
            let environment = [(key.to_string(), "value".to_string())]
                .into_iter()
                .collect();
            assert!(normalized_container_environment(&environment).is_err());
        }
        let environment = [("KEY".to_string(), "bad\0value".to_string())]
            .into_iter()
            .collect();
        assert!(normalized_container_environment(&environment).is_err());
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

    #[test]
    fn oci_rootfs_preflight_reports_canonical_path_and_governing_mount() {
        let bundle = tempfile::tempdir().expect("create bundle");
        let rootfs = bundle.path().join("rootfs");
        std::fs::create_dir(&rootfs).expect("create rootfs");
        let config = r#"{"root":{"path":"rootfs"}}"#;
        let mountinfo = format!(
            "20 1 0:1 / / rw - rootfs rootfs rw\n21 20 0:2 / {} rw - tmpfs tmpfs rw",
            bundle.path().display()
        );

        let diagnostic = inspect_oci_rootfs(
            bundle.path().to_str().expect("UTF-8 bundle path"),
            config,
            &mountinfo,
        )
        .expect("valid rootfs");

        assert_eq!(diagnostic.configured, PathBuf::from("rootfs"));
        assert_eq!(diagnostic.resolved, rootfs);
        assert_eq!(diagnostic.canonical, rootfs.canonicalize().unwrap());
        assert!(diagnostic.mountinfo.contains(" 0:2 "));
    }

    #[test]
    fn oci_rootfs_preflight_preserves_path_and_mountinfo_on_enoent() {
        let bundle = tempfile::tempdir().expect("create bundle");
        let config = r#"{"root":{"path":"missing"}}"#;
        let mountinfo = "20 1 0:1 / / rw - rootfs rootfs rw";

        let error = inspect_oci_rootfs(
            bundle.path().to_str().expect("UTF-8 bundle path"),
            config,
            mountinfo,
        )
        .expect_err("missing rootfs must fail");

        assert!(error.contains("config.root.path=missing"));
        assert!(error.contains("cannot be canonicalized"));
        assert!(error.contains("mountinfo=20 1 0:1 / / rw"));
    }

    #[test]
    fn youki_kernel_preflight_names_user_namespace_requirement() {
        let temp = tempfile::tempdir().expect("create tempdir");
        let error = ensure_youki_user_namespace_procfs(&temp.path().join("uid_map"))
            .expect_err("missing uid_map must fail");

        assert!(error.contains("CONFIG_USER_NS=y"));
        assert!(error.contains("uid_map"));
    }

    #[test]
    fn oci_normalization_preserves_read_only_cgroup2_mount_and_namespace() {
        let mut config = serde_json::json!({
            "mounts": [
                { "destination": "/proc", "type": "proc", "source": "proc" },
                {
                    "destination": "/sys/fs/cgroup",
                    "type": "cgroup2",
                    "source": "cgroup2",
                    "options": ["nosuid", "noexec", "nodev", "relatime", "ro"]
                },
                { "destination": "/sys", "type": "sysfs", "source": "sysfs" }
            ],
            "linux": {
                "maskedPaths": ["/proc/kcore"],
                "readonlyPaths": ["/proc/sys"],
                "resources": {
                    "cpu": {
                        "quota": 50000,
                        "period": 100000
                    }
                },
                "namespaces": [
                    { "type": "mount" },
                    { "type": "network" },
                    { "type": "cgroup" },
                    { "type": "pid" }
                ]
            }
        });
        let resources_before = config["linux"]["resources"].clone();

        normalize_oci_config(&mut config);

        let mounts = config["mounts"].as_array().expect("mounts");
        assert_eq!(mounts.len(), 2);
        let cgroup2 = mounts
            .iter()
            .find(|mount| mount["type"] == "cgroup2")
            .expect("cgroup2 mount must survive normalization");
        assert_eq!(cgroup2["destination"], "/sys/fs/cgroup");
        assert_eq!(
            cgroup2["options"],
            serde_json::json!(["nosuid", "noexec", "nodev", "relatime", "ro"])
        );

        let namespace_types: Vec<&str> = config["linux"]["namespaces"]
            .as_array()
            .expect("namespaces")
            .iter()
            .map(|namespace| namespace["type"].as_str().expect("namespace type"))
            .collect();
        assert_eq!(namespace_types, ["mount", "network", "cgroup"]);
        assert_eq!(config["linux"]["resources"], resources_before);
        assert!(config["linux"].get("maskedPaths").is_none());
        assert!(config["linux"].get("readonlyPaths").is_none());
    }
}
