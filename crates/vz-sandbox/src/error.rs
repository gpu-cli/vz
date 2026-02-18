//! Sandbox-specific error types.

use std::path::PathBuf;
use std::time::Duration;

/// Errors from sandbox pool and session operations.
#[derive(Debug, thiserror::Error)]
pub enum SandboxError {
    /// Command execution timed out.
    #[error("exec timed out after {0:?}")]
    ExecTimeout(Duration),

    /// All VMs in the pool are currently in use.
    #[error("pool exhausted: all VMs are in use")]
    PoolExhausted,

    /// The project directory is not under the configured workspace mount.
    #[error("project dir {} is not under workspace mount {}", .0.display(), .1.display())]
    ProjectOutsideWorkspace(PathBuf, PathBuf),

    /// The guest agent could not be reached after retries.
    #[error("guest agent unreachable after {attempts} attempts")]
    AgentUnreachable {
        /// Number of connection attempts made.
        attempts: u32,
    },

    /// The handshake with the guest agent failed.
    #[error("handshake failed: {0}")]
    HandshakeFailed(String),

    /// A VM-level error from the vz crate.
    #[error(transparent)]
    Vm(#[from] vz::VzError),

    /// A VM operation error with context.
    #[error("VM error: {0}")]
    VmError(String),

    /// A channel communication error.
    #[error("channel error: {0}")]
    Channel(#[from] crate::channel::ChannelError),

    /// An underlying I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Command parsing error.
    #[error("command parse error: {0}")]
    CommandParse(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_timeout_display() {
        let err = SandboxError::ExecTimeout(Duration::from_secs(30));
        assert_eq!(err.to_string(), "exec timed out after 30s");
    }

    #[test]
    fn pool_exhausted_display() {
        let err = SandboxError::PoolExhausted;
        assert_eq!(err.to_string(), "pool exhausted: all VMs are in use");
    }

    #[test]
    fn project_outside_workspace_display() {
        let err = SandboxError::ProjectOutsideWorkspace(
            PathBuf::from("/tmp/evil"),
            PathBuf::from("/Users/dev/workspace"),
        );
        let msg = err.to_string();
        assert!(msg.contains("/tmp/evil"));
        assert!(msg.contains("/Users/dev/workspace"));
    }

    #[test]
    fn agent_unreachable_display() {
        let err = SandboxError::AgentUnreachable { attempts: 3 };
        assert_eq!(err.to_string(), "guest agent unreachable after 3 attempts");
    }

    #[test]
    fn handshake_failed_display() {
        let err = SandboxError::HandshakeFailed("version mismatch".to_string());
        assert_eq!(err.to_string(), "handshake failed: version mismatch");
    }
}
