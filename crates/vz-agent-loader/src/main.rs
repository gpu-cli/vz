//! vz-agent-loader: minimal bootstrap binary for macOS/Linux VMs.
//!
//! Installed once via the patch/delta system with root ownership.
//! Starts at boot as a LaunchDaemon, listens on vsock port 7420,
//! and executes/supervises other binaries on host command.
//!
//! Design: this binary must be maximally stable. No gRPC, no protobuf,
//! no complex dependencies. Line-delimited JSON over raw vsock.

#![allow(unsafe_code)]

mod vsock;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use vz_agent_loader_client::protocol::*;
use vsock::VsockListener;

/// A tracked child process.
struct TrackedChild {
    exec_id: String,
    pid: u32,
    binary: String,
    keep_alive: bool,
    args: Vec<String>,
    env: Vec<String>,
}

/// Shared state across connections.
struct LoaderState {
    children: HashMap<String, TrackedChild>,
    next_id: u64,
    started_at: Instant,
}

impl LoaderState {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            next_id: 1,
            started_at: Instant::now(),
        }
    }

    fn alloc_exec_id(&mut self) -> String {
        let id = self.next_id;
        self.next_id += 1;
        format!("e{id}")
    }
}

type SharedState = Arc<Mutex<LoaderState>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let port = std::env::var("VZ_LOADER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(LOADER_PORT);

    let bind_timeout_secs: u64 = std::env::var("VZ_LOADER_BIND_TIMEOUT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    info!(port, "vz-agent-loader starting");

    let listener = bind_with_retry(port, bind_timeout_secs).await?;
    info!(port, "listening on vsock");

    let state: SharedState = Arc::new(Mutex::new(LoaderState::new()));

    // Auto-start services from the startup manifest.
    start_manifest_services(&state).await;

    loop {
        match listener.accept().await {
            Ok(stream) => {
                info!("host connected");
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, state).await {
                        warn!(error = %e, "connection handler error");
                    }
                    info!("host disconnected");
                });
            }
            Err(e) => {
                error!(error = %e, "accept failed");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn bind_with_retry(
    port: u32,
    timeout_secs: u64,
) -> Result<VsockListener, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);
    let mut attempts = 0u32;

    loop {
        attempts = attempts.saturating_add(1);
        match VsockListener::bind(port) {
            Ok(listener) => {
                if attempts > 1 {
                    info!(
                        attempts,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "vsock bind succeeded after retry"
                    );
                }
                return Ok(listener);
            }
            Err(err) => {
                if started.elapsed() >= timeout {
                    return Err(format!(
                        "failed to bind vsock:{port} after {attempts} attempts: {err}"
                    )
                    .into());
                }
                warn!(attempts, error = %err, "vsock bind failed, retrying");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
}

async fn handle_connection(
    stream: vsock::VsockStream,
    state: SharedState,
) -> Result<(), Box<dyn std::error::Error>> {
    let (reader, mut writer) = tokio::io::split(stream);
    let mut lines = BufReader::new(reader).lines();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<Request>(&line) {
            Ok(request) => handle_request(request, &state).await,
            Err(e) => Response::Error(ErrorResponse {
                code: "parse_error".to_string(),
                message: format!("invalid request: {e}"),
            }),
        };

        let mut json = serde_json::to_string(&response)?;
        json.push('\n');
        writer.write_all(json.as_bytes()).await?;
        writer.flush().await?;
    }

    Ok(())
}

async fn handle_request(request: Request, state: &SharedState) -> Response {
    match request {
        Request::Ping => handle_ping(state).await,
        Request::Exec(req) => handle_exec(req, state).await,
        Request::List => handle_list(state).await,
        Request::Kill(req) => handle_kill(req, state).await,
        Request::Register(req) => handle_register(req, state).await,
        Request::Unregister(req) => handle_unregister(req, state).await,
    }
}

// ── Startup manifest ───────────────────────────────────────────────

fn load_manifest() -> StartupManifest {
    let path = std::path::Path::new(STARTUP_MANIFEST_PATH);
    if !path.exists() {
        return StartupManifest::default();
    }
    match std::fs::read_to_string(path) {
        Ok(contents) => match serde_json::from_str(&contents) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "failed to parse startup manifest, using empty");
                StartupManifest::default()
            }
        },
        Err(e) => {
            warn!(error = %e, "failed to read startup manifest, using empty");
            StartupManifest::default()
        }
    }
}

fn save_manifest(manifest: &StartupManifest) -> Result<(), String> {
    let path = std::path::Path::new(STARTUP_MANIFEST_PATH);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create manifest dir: {e}"))?;
    }
    let json = serde_json::to_string_pretty(manifest)
        .map_err(|e| format!("failed to serialize manifest: {e}"))?;
    std::fs::write(path, json).map_err(|e| format!("failed to write manifest: {e}"))
}

async fn start_manifest_services(state: &SharedState) {
    let manifest = load_manifest();
    if manifest.services.is_empty() {
        info!("no services in startup manifest");
        return;
    }

    info!(count = manifest.services.len(), "starting services from manifest");
    for service in &manifest.services {
        let req = ExecRequest {
            binary: service.binary.clone(),
            args: service.args.clone(),
            env: service.env.clone(),
            keep_alive: service.keep_alive,
        };
        match handle_exec(req, state).await {
            Response::ExecOk(ok) => {
                info!(
                    name = %service.name,
                    exec_id = %ok.exec_id,
                    pid = ok.pid,
                    "manifest service started"
                );
            }
            Response::Error(e) => {
                error!(
                    name = %service.name,
                    code = %e.code,
                    message = %e.message,
                    "manifest service failed to start"
                );
            }
            _ => {}
        }
    }
}

async fn handle_register(req: RegisterRequest, state: &SharedState) -> Response {
    // Update the manifest on disk.
    let mut manifest = load_manifest();
    manifest.services.retain(|s| s.name != req.service.name);
    manifest.services.push(req.service.clone());
    if let Err(e) = save_manifest(&manifest) {
        return Response::Error(ErrorResponse {
            code: "manifest_write_failed".to_string(),
            message: e,
        });
    }

    info!(name = %req.service.name, "service registered in startup manifest");

    // Optionally start immediately.
    if req.start_now {
        let exec_req = ExecRequest {
            binary: req.service.binary.clone(),
            args: req.service.args.clone(),
            env: req.service.env.clone(),
            keep_alive: req.service.keep_alive,
        };
        match handle_exec(exec_req, state).await {
            Response::ExecOk(ok) => Response::RegisterOk(RegisterOkResponse {
                name: req.service.name,
                exec_id: Some(ok.exec_id),
                pid: Some(ok.pid),
            }),
            Response::Error(e) => Response::Error(e),
            _ => Response::RegisterOk(RegisterOkResponse {
                name: req.service.name,
                exec_id: None,
                pid: None,
            }),
        }
    } else {
        Response::RegisterOk(RegisterOkResponse {
            name: req.service.name,
            exec_id: None,
            pid: None,
        })
    }
}

async fn handle_unregister(req: UnregisterRequest, state: &SharedState) -> Response {
    // Remove from manifest.
    let mut manifest = load_manifest();
    let before = manifest.services.len();
    manifest.services.retain(|s| s.name != req.name);
    if manifest.services.len() == before {
        return Response::Error(ErrorResponse {
            code: "not_found".to_string(),
            message: format!("service '{}' not found in manifest", req.name),
        });
    }
    if let Err(e) = save_manifest(&manifest) {
        return Response::Error(ErrorResponse {
            code: "manifest_write_failed".to_string(),
            message: e,
        });
    }

    info!(name = %req.name, "service unregistered from startup manifest");

    // Optionally kill the running service.
    if req.stop {
        let st = state.lock().await;
        // Find children matching the service name's binary.
        let matching: Vec<String> = st
            .children
            .values()
            .filter(|c| c.binary.ends_with(&req.name) || c.exec_id == req.name)
            .map(|c| c.exec_id.clone())
            .collect();
        drop(st);

        for exec_id in matching {
            let kill_req = KillRequest {
                exec_id,
                signal: 15,
            };
            let _ = handle_kill(kill_req, state).await;
        }
    }

    Response::UnregisterOk
}

async fn handle_ping(state: &SharedState) -> Response {
    let st = state.lock().await;
    Response::Pong(PongResponse {
        version: PROTOCOL_VERSION,
        loader_version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: st.started_at.elapsed().as_secs(),
    })
}

async fn handle_exec(req: ExecRequest, state: &SharedState) -> Response {
    let path = std::path::Path::new(&req.binary);
    if !path.is_absolute() {
        return Response::Error(ErrorResponse {
            code: "invalid_path".to_string(),
            message: "binary must be an absolute path".to_string(),
        });
    }

    if !path.exists() {
        return Response::Error(ErrorResponse {
            code: "not_found".to_string(),
            message: format!("binary not found: {}", req.binary),
        });
    }

    let mut cmd = Command::new(&req.binary);
    cmd.args(&req.args);

    for kv in &req.env {
        if let Some((k, v)) = kv.split_once('=') {
            cmd.env(k, v);
        }
    }

    // Detach stdio — the child runs independently.
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());

    match cmd.spawn() {
        Ok(child) => {
            let pid = child.id().unwrap_or(0);
            let exec_id = {
                let mut st = state.lock().await;
                let eid = st.alloc_exec_id();
                st.children.insert(
                    eid.clone(),
                    TrackedChild {
                        exec_id: eid.clone(),
                        pid,
                        binary: req.binary.clone(),
                        keep_alive: req.keep_alive,
                        args: req.args.clone(),
                        env: req.env.clone(),
                    },
                );
                eid
            };

            let state_clone = state.clone();
            let exec_id_clone = exec_id.clone();
            tokio::spawn(async move {
                wait_and_supervise(child, exec_id_clone, state_clone).await;
            });

            info!(exec_id = %exec_id, pid, binary = %req.binary, "child started");
            Response::ExecOk(ExecOkResponse { exec_id, pid })
        }
        Err(e) => {
            error!(binary = %req.binary, error = %e, "spawn failed");
            Response::Error(ErrorResponse {
                code: "spawn_failed".to_string(),
                message: format!("failed to start {}: {e}", req.binary),
            })
        }
    }
}

async fn wait_and_supervise(
    mut child: tokio::process::Child,
    exec_id: String,
    state: SharedState,
) {
    let status = child.wait().await;
    let (exit_code, signal) = match &status {
        Ok(s) => {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                (s.code(), s.signal())
            }
            #[cfg(not(unix))]
            {
                (s.code(), None)
            }
        }
        Err(_) => (None, None),
    };

    // Check if we should restart.
    let restart_info = {
        let st = state.lock().await;
        st.children.get(&exec_id).and_then(|c| {
            if c.keep_alive {
                Some((c.binary.clone(), c.args.clone(), c.env.clone()))
            } else {
                None
            }
        })
    };

    if let Some((binary, args, env)) = restart_info {
        info!(exec_id = %exec_id, exit_code, signal, "child exited, restarting (keep_alive)");

        // Brief delay to avoid tight crash loops.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let mut cmd = Command::new(&binary);
        cmd.args(&args);
        for kv in &env {
            if let Some((k, v)) = kv.split_once('=') {
                cmd.env(k, v);
            }
        }
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());

        match cmd.spawn() {
            Ok(new_child) => {
                let new_pid = new_child.id().unwrap_or(0);
                {
                    let mut st = state.lock().await;
                    if let Some(entry) = st.children.get_mut(&exec_id) {
                        entry.pid = new_pid;
                    }
                }
                info!(exec_id = %exec_id, pid = new_pid, "child restarted");

                // Recurse to keep supervising.
                Box::pin(wait_and_supervise(new_child, exec_id, state)).await;
            }
            Err(e) => {
                error!(exec_id = %exec_id, error = %e, "failed to restart child, giving up");
                let mut st = state.lock().await;
                st.children.remove(&exec_id);
            }
        }
    } else {
        info!(exec_id = %exec_id, exit_code, signal, "child exited");
        let mut st = state.lock().await;
        st.children.remove(&exec_id);
    }
}

async fn handle_list(state: &SharedState) -> Response {
    let st = state.lock().await;
    let children = st
        .children
        .values()
        .map(|c| ChildInfo {
            exec_id: c.exec_id.clone(),
            pid: c.pid,
            binary: c.binary.clone(),
            keep_alive: c.keep_alive,
        })
        .collect();
    Response::ListOk(ListOkResponse { children })
}

async fn handle_kill(req: KillRequest, state: &SharedState) -> Response {
    let pid = {
        let st = state.lock().await;
        match st.children.get(&req.exec_id) {
            Some(c) => c.pid as i32,
            None => {
                return Response::Error(ErrorResponse {
                    code: "not_found".to_string(),
                    message: format!("exec_id {} not found", req.exec_id),
                });
            }
        }
    };

    // SAFETY: kill() is a standard POSIX function.
    let ret = unsafe { libc::kill(pid, req.signal) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        return Response::Error(ErrorResponse {
            code: "kill_failed".to_string(),
            message: format!("kill({pid}, {}) failed: {err}", req.signal),
        });
    }

    info!(exec_id = %req.exec_id, pid, signal = req.signal, "sent signal");
    Response::KillOk
}
