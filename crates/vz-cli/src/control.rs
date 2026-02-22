//! Unix socket control channel for communicating with running VMs.
//!
//! `vz run` starts a control server on `~/.vz/run/<name>.sock`.
//! Other commands (exec, save, stop) connect to this socket.

use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, info, warn};

use crate::registry;

// ---------------------------------------------------------------------------
// Wire format: length-prefixed JSON
// ---------------------------------------------------------------------------

/// Maximum control frame size: 4 MiB.
const MAX_CONTROL_FRAME: usize = 4 * 1024 * 1024;

/// Read a length-prefixed JSON frame from a reader.
async fn read_control_frame<T: serde::de::DeserializeOwned>(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> anyhow::Result<T> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_CONTROL_FRAME {
        anyhow::bail!("control frame too large: {len} bytes (max {MAX_CONTROL_FRAME})");
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    let msg: T = serde_json::from_slice(&payload)?;
    Ok(msg)
}

/// Write a length-prefixed JSON frame to a writer.
async fn write_control_frame<T: serde::Serialize>(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    msg: &T,
) -> anyhow::Result<()> {
    let json = serde_json::to_vec(msg)?;
    if json.len() > MAX_CONTROL_FRAME {
        anyhow::bail!(
            "control frame too large: {} bytes (max {MAX_CONTROL_FRAME})",
            json.len()
        );
    }
    let len = (json.len() as u32).to_le_bytes();
    writer.write_all(&len).await?;
    writer.write_all(&json).await?;
    writer.flush().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Protocol types
// ---------------------------------------------------------------------------

/// Request sent to a running VM's control socket.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ControlRequest {
    /// Execute a command in the VM.
    Exec {
        command: Vec<String>,
        user: Option<String>,
        workdir: Option<String>,
    },
    /// Save VM state to disk.
    Save { path: String, stop_after: bool },
    /// Stop the VM.
    Stop { force: bool },
    /// Query VM status.
    Status,
}

/// Response from the control server.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ControlResponse {
    /// Result of command execution.
    ExecResult {
        exit_code: i32,
        stdout: String,
        stderr: String,
    },
    /// Save completed.
    SaveComplete { path: String },
    /// VM stopped.
    Stopped,
    /// VM status.
    Status { state: String, pid: u32 },
    /// Error.
    Error { message: String },
}

// ---------------------------------------------------------------------------
// Client helpers
// ---------------------------------------------------------------------------

/// Get the control socket path for a named VM.
pub fn socket_path(name: &str) -> PathBuf {
    registry::vz_home().join("run").join(format!("{name}.sock"))
}

/// Connect to a running VM's control socket.
pub async fn connect(name: &str) -> anyhow::Result<UnixStream> {
    let path = socket_path(name);
    if !path.exists() {
        anyhow::bail!(
            "VM '{name}' is not running (no control socket at {})",
            path.display()
        );
    }
    let stream = UnixStream::connect(&path).await?;
    Ok(stream)
}

/// Send a control request and receive the response.
pub async fn request(
    stream: &mut UnixStream,
    req: &ControlRequest,
) -> anyhow::Result<ControlResponse> {
    let (mut reader, mut writer) = stream.split();
    write_control_frame(&mut writer, req).await?;
    let resp: ControlResponse = read_control_frame(&mut reader).await?;
    Ok(resp)
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Start the control server for a running VM.
///
/// Accepts connections, reads `ControlRequest`, dispatches to handler.
/// Returns when the server shuts down (e.g., VM stopped).
///
/// `vm_stopped` is notified when a Stop control request succeeds, so the
/// main `vz run` loop can exit without waiting for Ctrl+C.
pub async fn serve(
    name: &str,
    vm: Arc<vz::Vm>,
    vm_stopped: Arc<tokio::sync::Notify>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let path = socket_path(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Remove stale socket
    let _ = std::fs::remove_file(&path);

    let listener = UnixListener::bind(&path)?;
    info!(path = %path.display(), "control server listening");

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _)) => {
                        let vm = vm.clone();
                        let stopped = vm_stopped.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_control_connection(stream, vm, stopped).await {
                                warn!(error = %e, "control connection error");
                            }
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "control accept error");
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("control server shutting down");
                    break;
                }
            }
        }
    }

    // Clean up socket
    let _ = std::fs::remove_file(&path);
    Ok(())
}

async fn handle_control_connection(
    stream: UnixStream,
    vm: Arc<vz::Vm>,
    vm_stopped: Arc<tokio::sync::Notify>,
) -> anyhow::Result<()> {
    let (mut reader, mut writer) = stream.into_split();

    let req: ControlRequest = read_control_frame(&mut reader).await?;
    debug!(?req, "control request received");

    let is_stop = matches!(&req, ControlRequest::Stop { .. });
    let is_save_and_stop = matches!(
        &req,
        ControlRequest::Save {
            stop_after: true,
            ..
        }
    );

    let resp = match req {
        ControlRequest::Exec {
            command,
            user,
            workdir,
        } => handle_control_exec(Arc::clone(&vm), command, user, workdir).await,
        ControlRequest::Save { path, stop_after } => {
            handle_control_save(&vm, &path, stop_after).await
        }
        ControlRequest::Stop { force } => handle_control_stop(&vm, force).await,
        ControlRequest::Status => ControlResponse::Status {
            state: format!("{:?}", vm.state()),
            pid: std::process::id(),
        },
    };

    write_control_frame(&mut writer, &resp).await?;

    // If the VM was stopped (either via Stop or Save+stop), notify the main loop
    if (is_stop || is_save_and_stop)
        && matches!(
            &resp,
            ControlResponse::Stopped | ControlResponse::SaveComplete { .. }
        )
    {
        vm_stopped.notify_one();
    }

    Ok(())
}

async fn handle_control_exec(
    vm: Arc<vz::Vm>,
    command: Vec<String>,
    user: Option<String>,
    workdir: Option<String>,
) -> ControlResponse {
    use vz_linux::grpc_client::{ExecOptions, GrpcAgentClient};

    if command.is_empty() {
        return ControlResponse::Error {
            message: "empty command".to_string(),
        };
    }

    let mut client = match GrpcAgentClient::connect(vm, vz::protocol::AGENT_PORT).await {
        Ok(c) => c,
        Err(e) => {
            return ControlResponse::Error {
                message: format!("failed to connect to guest agent: {e}"),
            };
        }
    };

    let cmd = command[0].clone();
    let args: Vec<String> = command[1..].to_vec();

    let options = ExecOptions {
        working_dir: workdir,
        env: Vec::new(),
        user,
    };

    match client.exec(cmd, args, options).await {
        Ok(output) => ControlResponse::ExecResult {
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        },
        Err(e) => ControlResponse::Error {
            message: format!("exec failed: {e}"),
        },
    }
}

async fn handle_control_save(vm: &vz::Vm, path: &str, stop_after: bool) -> ControlResponse {
    let save_path = std::path::Path::new(path);

    // Remove existing state file (Virtualization.framework won't overwrite)
    if save_path.exists() {
        if let Err(e) = std::fs::remove_file(save_path) {
            return ControlResponse::Error {
                message: format!("failed to remove existing state file: {e}"),
            };
        }
    }

    // Pause first (required for save)
    if let Err(e) = vm.pause().await {
        return ControlResponse::Error {
            message: format!("failed to pause VM: {e}"),
        };
    }

    // Save state
    if let Err(e) = vm.save_state(save_path).await {
        // Try to resume on save failure
        let _ = vm.resume().await;
        return ControlResponse::Error {
            message: format!("failed to save VM state: {e}"),
        };
    }

    if stop_after {
        // Leave VM paused instead of calling vm.stop() — stop() flushes buffers
        // to disk, which makes the save file inconsistent with the disk state.
        // The vm_stopped notify will cause the main process to exit cleanly,
        // which drops the VM without additional disk writes.
    } else {
        let _ = vm.resume().await;
    }

    ControlResponse::SaveComplete {
        path: path.to_string(),
    }
}

async fn handle_control_stop(vm: &vz::Vm, force: bool) -> ControlResponse {
    let result = if force {
        vm.stop().await
    } else {
        vm.request_stop().await
    };

    match result {
        Ok(()) => ControlResponse::Stopped,
        Err(e) => ControlResponse::Error {
            message: format!("failed to stop VM: {e}"),
        },
    }
}
