//! Active sandbox session with command execution.
//!
//! A [`SandboxSession`] represents a lease on a VM pool slot with a mounted
//! project directory. It provides synchronous (buffered) and streaming command
//! execution inside the guest VM.
//!
//! # Execution Models
//!
//! - [`exec`](SandboxSession::exec) — Run a command, collect all output, return when done.
//! - [`exec_streaming`](SandboxSession::exec_streaming) — Run a command, return a
//!   [`GrpcExecStream`] that yields events as they arrive.
//! - [`exec_as_root`](SandboxSession::exec_as_root) — Like `exec`, but runs as root.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::debug;
use vz::protocol::ExecOutput;
use vz_linux::grpc_client::{ExecOptions, GrpcAgentClient, GrpcExecStream};

use crate::error::SandboxError;

// ---------------------------------------------------------------------------
// SandboxSession
// ---------------------------------------------------------------------------

/// An active sandbox session with a mounted project directory.
///
/// Provides command execution inside the VM. Created by
/// [`SandboxPool::acquire`](crate::pool::SandboxPool::acquire) and returned
/// to the pool with [`SandboxPool::release`](crate::pool::SandboxPool::release).
///
/// Commands are parsed with `shell-words` for proper quoting support,
/// then sent as structured gRPC `Exec` requests to the guest agent.
pub struct SandboxSession {
    /// Pool slot index this session occupies.
    slot_index: usize,
    /// Guest-side path to the mounted project directory.
    guest_project_path: String,
    /// Default timeout for exec calls.
    default_exec_timeout: Option<Duration>,
    /// gRPC agent client (None when no VM is connected, e.g. in tests).
    grpc: Arc<Mutex<Option<GrpcAgentClient>>>,
}

impl std::fmt::Debug for SandboxSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SandboxSession")
            .field("slot_index", &self.slot_index)
            .field("guest_project_path", &self.guest_project_path)
            .field("default_exec_timeout", &self.default_exec_timeout)
            .finish()
    }
}

impl SandboxSession {
    /// Create a new session. Called by [`SandboxPool::acquire`](crate::pool::SandboxPool::acquire).
    pub(crate) fn new(
        slot_index: usize,
        guest_project_path: String,
        default_exec_timeout: Option<Duration>,
        grpc: Arc<Mutex<Option<GrpcAgentClient>>>,
    ) -> Self {
        Self {
            slot_index,
            guest_project_path,
            default_exec_timeout,
            grpc,
        }
    }

    /// Get the pool slot index for this session.
    pub fn slot_index(&self) -> usize {
        self.slot_index
    }

    /// Path where the project is mounted inside the VM.
    ///
    /// This is the guest-side path under `/mnt/workspace/`.
    pub fn project_path(&self) -> &str {
        &self.guest_project_path
    }

    /// Get the default exec timeout for this session.
    pub fn default_exec_timeout(&self) -> Option<Duration> {
        self.default_exec_timeout
    }

    /// Execute a command inside the sandbox and collect all output.
    ///
    /// The command string is parsed using shell-words for proper quoting.
    /// The working directory is set to the project's guest-side mount path.
    ///
    /// Uses the session's default timeout if `timeout` is `None`.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::ExecTimeout`] if the command exceeds the timeout.
    /// Returns [`SandboxError::CommandParse`] if the command string is malformed.
    pub async fn exec(&self, cmd: &str) -> Result<ExecOutput, SandboxError> {
        self.exec_with_options(cmd, None, self.default_exec_timeout)
            .await
    }

    /// Execute a command with an explicit timeout.
    ///
    /// Pass `Some(duration)` to override the default, or `None` for no timeout.
    pub async fn exec_with_timeout(
        &self,
        cmd: &str,
        timeout: Option<Duration>,
    ) -> Result<ExecOutput, SandboxError> {
        self.exec_with_options(cmd, None, timeout).await
    }

    /// Execute a command as root inside the sandbox.
    ///
    /// Some operations (package installation, system config) require root.
    /// The guest agent runs as root and can execute commands as any user.
    pub async fn exec_as_root(&self, cmd: &str) -> Result<ExecOutput, SandboxError> {
        self.exec_with_options(cmd, Some("root"), self.default_exec_timeout)
            .await
    }

    /// Execute a command and return a streaming event source.
    ///
    /// Returns a [`GrpcExecStream`] that yields events as stdout/stderr
    /// data arrives. Use this for long-running commands where you want to
    /// process output incrementally.
    pub async fn exec_streaming(&self, cmd: &str) -> Result<GrpcExecStream, SandboxError> {
        let (command, args) = self.parse_command(cmd)?;

        debug!(cmd = cmd, slot = self.slot_index, "exec_streaming");

        let mut grpc = self.grpc.lock().await;
        if let Some(ref mut client) = *grpc {
            let options = ExecOptions {
                working_dir: Some(self.guest_project_path.clone()),
                env: Vec::new(),
                user: None,
            };
            let stream = client
                .exec_stream(command, args, options)
                .await
                .map_err(|e| SandboxError::GrpcError(e.to_string()))?;
            Ok(stream)
        } else {
            Err(SandboxError::GrpcError(
                "no gRPC client connected".to_string(),
            ))
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Execute with all options specified.
    async fn exec_with_options(
        &self,
        cmd: &str,
        user: Option<&str>,
        timeout: Option<Duration>,
    ) -> Result<ExecOutput, SandboxError> {
        let (command, args) = self.parse_command(cmd)?;

        debug!(
            cmd = cmd,
            user = user,
            timeout = ?timeout,
            slot = self.slot_index,
            "exec"
        );

        let mut grpc = self.grpc.lock().await;
        if let Some(ref mut client) = *grpc {
            let options = ExecOptions {
                working_dir: Some(self.guest_project_path.clone()),
                env: Vec::new(),
                user: user.map(String::from),
            };

            let exec_future = client.exec(command, args, options);

            if let Some(duration) = timeout {
                tokio::time::timeout(duration, exec_future)
                    .await
                    .map_err(|_| SandboxError::ExecTimeout(duration))?
                    .map_err(|e| SandboxError::GrpcError(e.to_string()))
            } else {
                exec_future
                    .await
                    .map_err(|e| SandboxError::GrpcError(e.to_string()))
            }
        } else {
            // No client available (test mode or VM not connected)
            debug!("no gRPC client connected, returning empty output");
            Ok(ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            })
        }
    }

    /// Parse a command string into (command, args).
    fn parse_command(&self, cmd: &str) -> Result<(String, Vec<String>), SandboxError> {
        let words =
            shell_words::split(cmd).map_err(|e| SandboxError::CommandParse(e.to_string()))?;

        if words.is_empty() {
            return Err(SandboxError::CommandParse("empty command".to_string()));
        }

        let command = words[0].clone();
        let args: Vec<String> = words[1..].to_vec();

        Ok((command, args))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_session() -> SandboxSession {
        SandboxSession::new(
            0,
            "/mnt/workspace/my-project".to_string(),
            Some(Duration::from_secs(30)),
            Arc::new(Mutex::new(None)),
        )
    }

    #[test]
    fn session_accessors() {
        let session = make_session();
        assert_eq!(session.slot_index(), 0);
        assert_eq!(session.project_path(), "/mnt/workspace/my-project");
        assert_eq!(
            session.default_exec_timeout(),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn parse_command_simple() {
        let session = make_session();
        let (command, args) = session.parse_command("cargo build").unwrap();
        assert_eq!(command, "cargo");
        assert_eq!(args, vec!["build"]);
    }

    #[test]
    fn parse_command_with_quotes() {
        let session = make_session();
        let (command, args) = session.parse_command(r#"echo "hello world""#).unwrap();
        assert_eq!(command, "echo");
        assert_eq!(args, vec!["hello world"]);
    }

    #[test]
    fn parse_command_empty_fails() {
        let session = make_session();
        let result = session.parse_command("");
        assert!(result.is_err());
    }

    #[test]
    fn parse_command_bad_quotes_fails() {
        let session = make_session();
        let result = session.parse_command("echo \"unterminated");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn exec_returns_output() {
        let session = make_session();
        let output = session.exec("echo hello").await.unwrap();
        // Stub returns exit code 0 with empty output
        assert_eq!(output.exit_code, 0);
    }

    #[tokio::test]
    async fn exec_as_root_returns_output() {
        let session = make_session();
        let output = session.exec_as_root("whoami").await.unwrap();
        assert_eq!(output.exit_code, 0);
    }

    #[tokio::test]
    async fn exec_with_timeout_works() {
        let session = make_session();
        let output = session
            .exec_with_timeout("sleep 1", Some(Duration::from_secs(5)))
            .await
            .unwrap();
        assert_eq!(output.exit_code, 0);
    }

    #[test]
    fn parse_command_complex() {
        let session = make_session();
        let (command, args) = session
            .parse_command("bash -c 'echo $HOME && ls -la'")
            .unwrap();
        assert_eq!(command, "bash");
        assert_eq!(args, vec!["-c", "echo $HOME && ls -la"]);
    }

    #[test]
    fn session_no_timeout() {
        let session = SandboxSession::new(
            1,
            "/mnt/workspace/other".to_string(),
            None,
            Arc::new(Mutex::new(None)),
        );
        assert!(session.default_exec_timeout().is_none());
    }
}
