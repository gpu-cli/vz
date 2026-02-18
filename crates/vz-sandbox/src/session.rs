//! Active sandbox session with command execution.
//!
//! A [`SandboxSession`] represents a lease on a VM pool slot with a mounted
//! project directory. It provides synchronous (buffered) and streaming command
//! execution inside the guest VM.
//!
//! # Execution Models
//!
//! - [`exec`](SandboxSession::exec) — Run a command, collect all output, return when done.
//! - [`exec_streaming`](SandboxSession::exec_streaming) — Run a command, return an
//!   [`ExecStream`] that yields [`ExecEvent`]s as they arrive.
//! - [`exec_as_root`](SandboxSession::exec_as_root) — Like `exec`, but runs as root.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::debug;
use vz::protocol::{Channel, ExecOutput, ExecStream, Request, Response};

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
/// then sent as structured [`Request::Exec`] messages to the guest agent.
pub struct SandboxSession {
    /// Pool slot index this session occupies.
    slot_index: usize,
    /// Guest-side path to the mounted project directory.
    guest_project_path: String,
    /// Default timeout for exec calls.
    default_exec_timeout: Option<Duration>,
    /// Shared atomic counter for generating unique exec IDs.
    next_exec_id: Arc<AtomicU64>,
    /// Channel to the guest agent (None when no VM is connected, e.g. in tests).
    channel: Option<Arc<Channel<Request, Response>>>,
}

impl std::fmt::Debug for SandboxSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SandboxSession")
            .field("slot_index", &self.slot_index)
            .field("guest_project_path", &self.guest_project_path)
            .field("default_exec_timeout", &self.default_exec_timeout)
            .field("channel", &self.channel.as_ref().map(|_| "connected"))
            .finish()
    }
}

impl SandboxSession {
    /// Create a new session. Called by [`SandboxPool::acquire`](crate::pool::SandboxPool::acquire).
    pub(crate) fn new(
        slot_index: usize,
        guest_project_path: String,
        default_exec_timeout: Option<Duration>,
        next_exec_id: Arc<AtomicU64>,
        channel: Option<Arc<Channel<Request, Response>>>,
    ) -> Self {
        Self {
            slot_index,
            guest_project_path,
            default_exec_timeout,
            next_exec_id,
            channel,
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
    /// Returns an [`ExecStream`] that yields [`ExecEvent`]s as stdout/stderr
    /// data arrives. Use this for long-running commands where you want to
    /// process output incrementally.
    ///
    /// # Example (conceptual)
    ///
    /// ```rust,ignore
    /// let mut stream = session.exec_streaming("cargo build 2>&1").await?;
    /// while let Some(event) = stream.next().await {
    ///     match event {
    ///         ExecEvent::Stdout(data) => print!("{}", String::from_utf8_lossy(&data)),
    ///         ExecEvent::Stderr(data) => eprint!("{}", String::from_utf8_lossy(&data)),
    ///         ExecEvent::Exit(code) => println!("exited with {code}"),
    ///     }
    /// }
    /// ```
    pub async fn exec_streaming(&self, cmd: &str) -> Result<ExecStream, SandboxError> {
        let (request, exec_id) = self.build_exec_request(cmd, None)?;

        debug!(
            exec_id = exec_id,
            cmd = cmd,
            slot = self.slot_index,
            "exec_streaming"
        );

        if let Some(ref channel) = self.channel {
            channel.send(&request).await?;
        }

        Ok(ExecStream::new(exec_id, self.channel.clone()))
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
        let (request, exec_id) = self.build_exec_request(cmd, user)?;

        debug!(
            exec_id = exec_id,
            cmd = cmd,
            user = user,
            timeout = ?timeout,
            slot = self.slot_index,
            "exec"
        );

        if let Some(ref channel) = self.channel {
            channel.send(&request).await?;

            let collect_future = async {
                let mut stdout = Vec::new();
                let mut stderr = Vec::new();
                let exit_code = loop {
                    let resp = channel.recv().await?;
                    match resp {
                        Response::Stdout { data, exec_id: eid } if eid == exec_id => {
                            stdout.extend_from_slice(&data);
                        }
                        Response::Stderr { data, exec_id: eid } if eid == exec_id => {
                            stderr.extend_from_slice(&data);
                        }
                        Response::ExitCode { code, exec_id: eid } if eid == exec_id => {
                            break code;
                        }
                        Response::ExecError { error, .. } => {
                            return Err(SandboxError::Channel(vz::protocol::ChannelError::Io(
                                std::io::Error::other(error),
                            )));
                        }
                        _ => {} // Ignore responses for other exec_ids
                    }
                };

                Ok(ExecOutput {
                    exit_code,
                    stdout: String::from_utf8_lossy(&stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&stderr).into_owned(),
                })
            };

            if let Some(duration) = timeout {
                tokio::time::timeout(duration, collect_future)
                    .await
                    .map_err(|_| SandboxError::ExecTimeout(duration))?
            } else {
                collect_future.await
            }
        } else {
            // No channel available (test mode or VM not connected)
            debug!(exec_id, "no channel connected, returning empty output");
            Ok(ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            })
        }
    }

    /// Parse a command string and build an Exec request.
    fn build_exec_request(
        &self,
        cmd: &str,
        user: Option<&str>,
    ) -> Result<(Request, u64), SandboxError> {
        let words =
            shell_words::split(cmd).map_err(|e| SandboxError::CommandParse(e.to_string()))?;

        if words.is_empty() {
            return Err(SandboxError::CommandParse("empty command".to_string()));
        }

        let command = words[0].clone();
        let args: Vec<String> = words[1..].to_vec();

        let exec_id = self.next_exec_id.fetch_add(1, Ordering::Relaxed);

        let request = Request::Exec {
            id: exec_id,
            command,
            args,
            working_dir: Some(self.guest_project_path.clone()),
            env: vec![],
            user: user.map(String::from),
        };

        Ok((request, exec_id))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use vz::protocol::ExecEvent;

    fn make_session() -> SandboxSession {
        let counter = Arc::new(AtomicU64::new(1));
        SandboxSession::new(
            0,
            "/mnt/workspace/my-project".to_string(),
            Some(Duration::from_secs(30)),
            counter,
            None,
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
    fn build_exec_request_simple() {
        let session = make_session();
        let (request, exec_id) = session.build_exec_request("cargo build", None).unwrap();
        assert_eq!(exec_id, 1);
        if let Request::Exec {
            id,
            command,
            args,
            working_dir,
            user,
            ..
        } = request
        {
            assert_eq!(id, 1);
            assert_eq!(command, "cargo");
            assert_eq!(args, vec!["build"]);
            assert_eq!(working_dir, Some("/mnt/workspace/my-project".to_string()));
            assert!(user.is_none());
        } else {
            panic!("expected Exec request");
        }
    }

    #[test]
    fn build_exec_request_with_quotes() {
        let session = make_session();
        let (request, _) = session
            .build_exec_request(r#"echo "hello world""#, None)
            .unwrap();
        if let Request::Exec { command, args, .. } = request {
            assert_eq!(command, "echo");
            assert_eq!(args, vec!["hello world"]);
        } else {
            panic!("expected Exec request");
        }
    }

    #[test]
    fn build_exec_request_with_user() {
        let session = make_session();
        let (request, _) = session
            .build_exec_request("apt install git", Some("root"))
            .unwrap();
        if let Request::Exec { user, .. } = request {
            assert_eq!(user, Some("root".to_string()));
        } else {
            panic!("expected Exec request");
        }
    }

    #[test]
    fn build_exec_request_empty_fails() {
        let session = make_session();
        let result = session.build_exec_request("", None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SandboxError::CommandParse(_)));
    }

    #[test]
    fn build_exec_request_bad_quotes_fails() {
        let session = make_session();
        let result = session.build_exec_request("echo \"unterminated", None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SandboxError::CommandParse(_)));
    }

    #[test]
    fn exec_ids_increment() {
        let session = make_session();
        let (_, id1) = session.build_exec_request("cmd1", None).unwrap();
        let (_, id2) = session.build_exec_request("cmd2", None).unwrap();
        let (_, id3) = session.build_exec_request("cmd3", None).unwrap();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
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
    async fn exec_streaming_returns_stream() {
        let session = make_session();
        let mut stream = session.exec_streaming("cargo build").await.unwrap();
        assert!(stream.exec_id() > 0);

        // Stub yields Exit(0) immediately
        let event = stream.next().await;
        assert_eq!(event, Some(ExecEvent::Exit(0)));

        // After exit, next() returns None
        let event = stream.next().await;
        assert!(event.is_none());
    }

    #[tokio::test]
    async fn exec_stream_collect() {
        let session = make_session();
        let stream = session.exec_streaming("ls").await.unwrap();
        let output = stream.collect().await.unwrap();
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
    fn build_exec_request_complex_command() {
        let session = make_session();
        let (request, _) = session
            .build_exec_request("bash -c 'echo $HOME && ls -la'", None)
            .unwrap();
        if let Request::Exec { command, args, .. } = request {
            assert_eq!(command, "bash");
            assert_eq!(args, vec!["-c", "echo $HOME && ls -la"]);
        } else {
            panic!("expected Exec request");
        }
    }

    #[test]
    fn session_no_timeout() {
        let counter = Arc::new(AtomicU64::new(1));
        let session =
            SandboxSession::new(1, "/mnt/workspace/other".to_string(), None, counter, None);
        assert!(session.default_exec_timeout().is_none());
    }
}
