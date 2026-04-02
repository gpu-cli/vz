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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::debug;
use vz::protocol::ExecOutput;
use vz_linux::grpc_client::{ExecOptions, GrpcAgentClient, GrpcExecStream};

use crate::error::SandboxError;

const DEFAULT_EXEC_USER: &str = "dev";
const AGENT_UNAVAILABLE_ERROR: &str = "sandbox guest agent unavailable: no gRPC client connected";

// ---------------------------------------------------------------------------
// SandboxSession
// ---------------------------------------------------------------------------

/// Runtime lifecycle class for a pinned workload in a lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ContainerLifecycleClass {
    /// Long-lived interactive container.
    Workspace,
    /// Long-lived service container.
    Service,
    /// One-off short-lived command container.
    Ephemeral,
}

impl fmt::Display for ContainerLifecycleClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Workspace => "workspace",
            Self::Service => "service",
            Self::Ephemeral => "ephemeral",
        };
        write!(f, "{value}")
    }
}

/// An active sandbox session with a mounted project directory.
///
/// Provides command execution inside the VM. Created by
/// [`SandboxPool::acquire`](crate::pool::SandboxPool::acquire) and returned
/// to the pool with [`SandboxPool::release`](crate::pool::SandboxPool::release).
///
/// Commands are parsed with `shell-words` for proper quoting support,
/// then sent as structured gRPC `Exec` requests to the guest agent.
pub struct SandboxSession {
    /// Lease identity for this session.
    lease_id: String,
    /// Pool slot index this session occupies.
    slot_index: usize,
    /// Guest-side path to the mounted project directory.
    guest_project_path: String,
    /// Default timeout for exec calls.
    default_exec_timeout: Option<Duration>,
    /// Default environment variables included on every exec request.
    default_env: Vec<(String, String)>,
    /// gRPC agent client (None when no VM is connected, e.g. in tests).
    grpc: Arc<Mutex<Option<GrpcAgentClient>>>,
    /// Active pinned workloads keyed by workload ID.
    pinned_workloads: StdMutex<BTreeMap<String, ContainerLifecycleClass>>,
}

impl std::fmt::Debug for SandboxSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SandboxSession")
            .field("lease_id", &self.lease_id)
            .field("slot_index", &self.slot_index)
            .field("guest_project_path", &self.guest_project_path)
            .field("default_exec_timeout", &self.default_exec_timeout)
            .field("default_env_len", &self.default_env.len())
            .finish()
    }
}

impl SandboxSession {
    /// Create a new session. Called by [`SandboxPool::acquire`](crate::pool::SandboxPool::acquire).
    pub(crate) fn new(
        lease_id: String,
        slot_index: usize,
        guest_project_path: String,
        default_exec_timeout: Option<Duration>,
        default_env: Vec<(String, String)>,
        grpc: Arc<Mutex<Option<GrpcAgentClient>>>,
    ) -> Self {
        Self {
            lease_id,
            slot_index,
            guest_project_path,
            default_exec_timeout,
            default_env,
            grpc,
            pinned_workloads: StdMutex::new(BTreeMap::new()),
        }
    }

    /// Lease identifier for this session.
    pub fn lease_id(&self) -> &str {
        &self.lease_id
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

    /// Default environment variables injected into every exec request.
    pub fn default_env(&self) -> &[(String, String)] {
        &self.default_env
    }

    /// Pin a workload to this lease.
    ///
    /// Returns the previous class if the workload was already pinned.
    pub fn pin_workload(
        &self,
        workload_id: impl Into<String>,
        class: ContainerLifecycleClass,
    ) -> Option<ContainerLifecycleClass> {
        self.pinned_workloads_lock()
            .insert(workload_id.into(), class)
    }

    /// Remove a workload pin from this lease.
    ///
    /// Returns the removed class when the workload existed.
    pub fn unpin_workload(&self, workload_id: &str) -> Option<ContainerLifecycleClass> {
        self.pinned_workloads_lock().remove(workload_id)
    }

    /// Snapshot active pinned workloads as `(workload_id, class)` tuples.
    pub fn pinned_workloads(&self) -> Vec<(String, ContainerLifecycleClass)> {
        self.pinned_workloads_lock()
            .iter()
            .map(|(workload_id, class)| (workload_id.clone(), *class))
            .collect()
    }

    /// Number of active pinned workloads.
    pub fn pinned_workload_count(&self) -> usize {
        self.pinned_workloads_lock().len()
    }

    /// Whether any pinned workloads remain active for this lease.
    pub fn has_pinned_workloads(&self) -> bool {
        !self.pinned_workloads_lock().is_empty()
    }

    /// Execute a command inside the sandbox and collect all output.
    ///
    /// The command string is parsed using shell-words for proper quoting.
    /// Commands run as the sandbox's default user (`"dev"`) unless an explicit
    /// user is requested.
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
                env: self.default_env.clone(),
                user: Some(DEFAULT_EXEC_USER.to_string()),
            };
            let stream = client
                .exec_stream(command, args, options)
                .await
                .map_err(|e| SandboxError::GrpcError(e.to_string()))?;
            Ok(stream)
        } else {
            Err(SandboxError::GrpcError(AGENT_UNAVAILABLE_ERROR.to_string()))
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
            user = resolve_exec_user(user),
            timeout = ?timeout,
            slot = self.slot_index,
            "exec"
        );

        let mut grpc = self.grpc.lock().await;
        if let Some(ref mut client) = *grpc {
            let options = ExecOptions {
                working_dir: Some(self.guest_project_path.clone()),
                env: self.default_env.clone(),
                user: Some(resolve_exec_user(user).to_string()),
            };

            let exec_future = async {
                let stream = client.exec_stream(command, args, options).await?;
                Ok::<_, vz_linux::LinuxError>(stream.collect().await)
            };

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
            // No client available (VM missing/disconnected). Never fake success.
            debug!("no gRPC client connected, command was not executed");
            Err(SandboxError::GrpcError(AGENT_UNAVAILABLE_ERROR.to_string()))
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

    fn pinned_workloads_lock(
        &self,
    ) -> std::sync::MutexGuard<'_, BTreeMap<String, ContainerLifecycleClass>> {
        self.pinned_workloads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn resolve_exec_user(user: Option<&str>) -> &str {
    user.unwrap_or(DEFAULT_EXEC_USER)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_session() -> SandboxSession {
        SandboxSession::new(
            "lease-1".to_string(),
            0,
            "/mnt/workspace/my-project".to_string(),
            Some(Duration::from_secs(30)),
            Vec::new(),
            Arc::new(Mutex::new(None)),
        )
    }

    #[test]
    fn session_accessors() {
        let session = make_session();
        assert_eq!(session.lease_id(), "lease-1");
        assert_eq!(session.slot_index(), 0);
        assert_eq!(session.project_path(), "/mnt/workspace/my-project");
        assert_eq!(
            session.default_exec_timeout(),
            Some(Duration::from_secs(30))
        );
        assert!(session.default_env().is_empty());
    }

    #[test]
    fn session_pin_and_unpin_workloads() {
        let session = make_session();
        assert_eq!(session.pinned_workload_count(), 0);
        assert!(!session.has_pinned_workloads());

        assert_eq!(
            session.pin_workload("workspace-main", ContainerLifecycleClass::Workspace),
            None
        );
        assert_eq!(
            session.pin_workload("svc-db", ContainerLifecycleClass::Service),
            None
        );
        assert!(session.has_pinned_workloads());
        assert_eq!(session.pinned_workload_count(), 2);
        assert_eq!(
            session.pinned_workloads(),
            vec![
                ("svc-db".to_string(), ContainerLifecycleClass::Service),
                (
                    "workspace-main".to_string(),
                    ContainerLifecycleClass::Workspace,
                ),
            ],
        );

        assert_eq!(
            session.unpin_workload("workspace-main"),
            Some(ContainerLifecycleClass::Workspace)
        );
        assert_eq!(session.pinned_workload_count(), 1);
        assert_eq!(session.unpin_workload("missing-workload"), None);
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
    async fn exec_without_grpc_client_returns_error() {
        let session = make_session();
        let err = session.exec("echo hello").await.unwrap_err();
        assert!(matches!(err, SandboxError::GrpcError(_)));
        assert!(
            err.to_string().contains("guest agent unavailable"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn exec_as_root_without_grpc_client_returns_error() {
        let session = make_session();
        let err = session.exec_as_root("whoami").await.unwrap_err();
        assert!(matches!(err, SandboxError::GrpcError(_)));
    }

    #[tokio::test]
    async fn exec_with_timeout_without_grpc_client_returns_error() {
        let session = make_session();
        let err = session
            .exec_with_timeout("sleep 1", Some(Duration::from_secs(5)))
            .await
            .unwrap_err();
        assert!(matches!(err, SandboxError::GrpcError(_)));
    }

    #[tokio::test]
    async fn exec_streaming_without_grpc_client_returns_error() {
        let session = make_session();
        let result = session.exec_streaming("echo hello").await;
        assert!(matches!(result, Err(SandboxError::GrpcError(_))));
    }

    #[test]
    fn resolve_exec_user_defaults_to_dev() {
        assert_eq!(resolve_exec_user(None), "dev");
    }

    #[test]
    fn resolve_exec_user_preserves_explicit_override() {
        assert_eq!(resolve_exec_user(Some("root")), "root");
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
            "lease-2".to_string(),
            1,
            "/mnt/workspace/other".to_string(),
            None,
            vec![(
                "VZ_SANDBOX_BASE_IMAGE_REF".to_string(),
                "alpine:3.20".to_string(),
            )],
            Arc::new(Mutex::new(None)),
        );
        assert!(session.default_exec_timeout().is_none());
        assert_eq!(session.default_env().len(), 1);
    }
}
