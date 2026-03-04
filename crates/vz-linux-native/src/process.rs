//! OCI runtime process management.
//!
//! Shells out to an OCI runtime binary (youki, runc) for container
//! lifecycle operations. Handles command construction, output parsing,
//! and error mapping.

use std::path::Path;
use std::time::Duration;

use serde::Deserialize;
use tokio::process::Command;
use tracing::{debug, warn};

use crate::error::LinuxNativeError;

/// Parsed output from an OCI runtime state command.
#[derive(Debug, Clone, Deserialize)]
pub struct OciState {
    pub id: String,
    pub status: String,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default, alias = "bundlePath", alias = "bundle_path")]
    pub bundle: Option<String>,
}

/// Output from running an OCI runtime command.
#[derive(Debug)]
pub struct ProcessOutput {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Create a container from an OCI bundle.
///
/// Runs: `<runtime> --root <state_dir> create <id> --bundle <bundle_path>`
pub(crate) async fn oci_create(
    runtime_binary: &str,
    container_id: &str,
    bundle_path: &Path,
    state_dir: &Path,
) -> Result<(), LinuxNativeError> {
    let state_dir_s = state_dir.to_string_lossy();
    let bundle_path_s = bundle_path.to_string_lossy();
    let output = run_runtime_command(
        runtime_binary,
        &[
            "--root",
            &state_dir_s,
            "create",
            container_id,
            "--bundle",
            &bundle_path_s,
        ],
        None,
    )
    .await?;

    if output.exit_code != 0 {
        return Err(LinuxNativeError::InvalidConfig(format!(
            "create failed (exit {}): {}",
            output.exit_code,
            output.stderr.trim()
        )));
    }

    Ok(())
}

/// Start a created container.
///
/// Runs: `<runtime> --root <state_dir> start <id>`
pub(crate) async fn oci_start(
    runtime_binary: &str,
    container_id: &str,
    state_dir: &Path,
) -> Result<(), LinuxNativeError> {
    let state_dir_s = state_dir.to_string_lossy();
    let output = run_runtime_command(
        runtime_binary,
        &["--root", &state_dir_s, "start", container_id],
        None,
    )
    .await?;

    if output.exit_code != 0 {
        return Err(LinuxNativeError::InvalidConfig(format!(
            "start failed (exit {}): {}",
            output.exit_code,
            output.stderr.trim()
        )));
    }

    Ok(())
}

/// Get container state.
///
/// Runs: `<runtime> --root <state_dir> state <id>`
pub(crate) async fn oci_state(
    runtime_binary: &str,
    container_id: &str,
    state_dir: &Path,
) -> Result<OciState, LinuxNativeError> {
    let state_dir_s = state_dir.to_string_lossy();
    let output = run_runtime_command(
        runtime_binary,
        &["--root", &state_dir_s, "state", container_id],
        None,
    )
    .await?;

    if output.exit_code != 0 {
        return Err(LinuxNativeError::ContainerNotFound {
            id: container_id.to_string(),
        });
    }

    let state: OciState = serde_json::from_str(output.stdout.trim())?;
    Ok(state)
}

/// Options for `oci_exec`.
pub(crate) struct ExecOptions<'a> {
    pub runtime_binary: &'a str,
    pub container_id: &'a str,
    pub state_dir: &'a Path,
    pub cmd: &'a [String],
    pub env: &'a [(String, String)],
    pub cwd: Option<&'a str>,
    pub user: Option<&'a str>,
    pub timeout: Option<Duration>,
}

/// Execute a command in a running container.
///
/// Runs: `<runtime> exec [--cwd <dir>] [--env K=V]* [--user U] <id> <cmd> [args]`
pub(crate) async fn oci_exec(opts: ExecOptions<'_>) -> Result<ProcessOutput, LinuxNativeError> {
    let ExecOptions {
        runtime_binary,
        container_id,
        state_dir,
        cmd,
        env,
        cwd,
        user,
        timeout,
    } = opts;
    // --root is a global flag and must come before the subcommand.
    let mut args: Vec<String> = vec![
        "--root".to_string(),
        state_dir.to_string_lossy().into_owned(),
        "exec".to_string(),
    ];

    if let Some(cwd) = cwd {
        args.push("--cwd".to_string());
        args.push(cwd.to_string());
    }

    // Always include PATH if not already specified.
    let has_path = env.iter().any(|(k, _)| k == "PATH");
    if !has_path {
        args.push("--env".to_string());
        args.push("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string());
    }

    for (key, value) in env {
        args.push("--env".to_string());
        args.push(format!("{key}={value}"));
    }

    if let Some(user) = user {
        args.push("--user".to_string());
        args.push(user.to_string());
    }

    args.push(container_id.to_string());
    args.extend(cmd.iter().cloned());

    let args_ref: Vec<&str> = args.iter().map(String::as_str).collect();
    run_runtime_command(runtime_binary, &args_ref, timeout).await
}

/// Send a signal to a container.
///
/// Runs: `<runtime> --root <state_dir> kill <id> <signal>`
pub(crate) async fn oci_kill(
    runtime_binary: &str,
    container_id: &str,
    state_dir: &Path,
    signal: &str,
) -> Result<(), LinuxNativeError> {
    let state_dir_s = state_dir.to_string_lossy();
    let output = run_runtime_command(
        runtime_binary,
        &["--root", &state_dir_s, "kill", container_id, signal],
        None,
    )
    .await?;

    if output.exit_code != 0 {
        debug!(
            container_id,
            signal,
            exit_code = output.exit_code,
            stderr = %output.stderr.trim(),
            "kill returned non-zero (container may already be stopped)"
        );
    }

    Ok(())
}

/// Delete a container.
///
/// Runs: `<runtime> --root <state_dir> delete [--force] <id>`
pub(crate) async fn oci_delete(
    runtime_binary: &str,
    container_id: &str,
    state_dir: &Path,
    force: bool,
) -> Result<(), LinuxNativeError> {
    let state_dir_str = state_dir.to_string_lossy().into_owned();
    let mut args = vec!["--root", &*state_dir_str, "delete"];

    if force {
        args.push("--force");
    }
    args.push(container_id);

    let output = run_runtime_command(runtime_binary, &args, None).await?;

    if output.exit_code != 0 {
        warn!(
            container_id,
            exit_code = output.exit_code,
            stderr = %output.stderr.trim(),
            "delete returned non-zero"
        );
    }

    Ok(())
}

/// Run an OCI runtime command and capture output.
///
/// IMPORTANT: We wait for the child process to exit BEFORE draining
/// stdout/stderr pipes. OCI runtimes like youki fork child processes
/// (container init) that inherit pipe file descriptors. Using
/// `wait_with_output()` would block forever because the forked
/// init process (`sleep infinity`) keeps the pipes open.
async fn run_runtime_command(
    runtime_binary: &str,
    args: &[&str],
    timeout: Option<Duration>,
) -> Result<ProcessOutput, LinuxNativeError> {
    debug!(
        binary = runtime_binary,
        ?args,
        "running OCI runtime command"
    );

    let mut cmd = Command::new(runtime_binary);
    cmd.args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let mut child = cmd.spawn().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            LinuxNativeError::RuntimeBinaryNotFound {
                path: runtime_binary.to_string(),
            }
        } else {
            LinuxNativeError::Io(e)
        }
    })?;

    // Take ownership of stdout/stderr before waiting, so we can drain
    // them independently after the process exits.
    let mut stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();

    // Wait for the child process to exit first.
    let status = if let Some(timeout) = timeout {
        match tokio::time::timeout(timeout, child.wait()).await {
            Ok(result) => result?,
            Err(_) => {
                // Kill the child on timeout.
                let _ = child.kill().await;
                return Ok(ProcessOutput {
                    exit_code: 124, // timeout exit code convention
                    stdout: String::new(),
                    stderr: "command timed out".to_string(),
                });
            }
        }
    } else {
        child.wait().await?
    };

    // Now drain pipes. The main process has exited, so its pipe ends
    // are closed. Forked children may still hold them open, so read
    // with a short timeout to avoid blocking forever.
    let drain_timeout = Duration::from_millis(500);

    let stdout = if let Some(ref mut pipe) = stdout_pipe {
        drain_pipe(pipe, drain_timeout).await
    } else {
        String::new()
    };

    let stderr = if let Some(ref mut pipe) = stderr_pipe {
        drain_pipe(pipe, drain_timeout).await
    } else {
        String::new()
    };

    let exit_code = status.code().unwrap_or(128);

    debug!(
        exit_code,
        stdout_len = stdout.len(),
        stderr_len = stderr.len(),
        "OCI runtime command completed"
    );

    Ok(ProcessOutput {
        exit_code,
        stdout,
        stderr,
    })
}

/// Drain a pipe with a timeout, returning whatever was read.
async fn drain_pipe<R: tokio::io::AsyncRead + Unpin>(pipe: &mut R, timeout: Duration) -> String {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(timeout, pipe.read_to_end(&mut buf)).await;
    String::from_utf8_lossy(&buf).into_owned()
}
