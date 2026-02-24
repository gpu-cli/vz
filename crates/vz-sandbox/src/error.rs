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

    /// Lease release was denied because pinned workloads are still active.
    #[error("cannot release lease {lease_id}: active pinned workloads {active_workloads:?}")]
    LeaseReleaseDenied {
        /// Lease identity associated with the release attempt.
        lease_id: String,
        /// Workloads still pinned to the lease (`<workload-id>:<class>`).
        active_workloads: Vec<String>,
    },

    /// Checkpoint metadata store file is malformed or unreadable as JSON.
    #[error("checkpoint catalog at {} is invalid: {reason}", path.display())]
    CheckpointCatalogCorrupt {
        /// Catalog file path.
        path: PathBuf,
        /// Parse/validation reason.
        reason: String,
    },

    /// Checkpoint metadata store could not be persisted.
    #[error("failed to persist checkpoint catalog at {}: {reason}", path.display())]
    CheckpointCatalogPersistence {
        /// Catalog file path.
        path: PathBuf,
        /// Persistence error details.
        reason: String,
    },

    /// Checkpoint lineage invariants were violated.
    #[error("checkpoint lineage violation: {0}")]
    CheckpointLineageViolation(String),

    /// A VM-level error from the vz crate.
    #[error(transparent)]
    Vm(#[from] vz::VzError),

    /// A VM operation error with context.
    #[error("VM error: {0}")]
    VmError(String),

    /// A gRPC communication error.
    #[error("grpc error: {0}")]
    GrpcError(String),

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

    #[test]
    fn lease_release_denied_display() {
        let err = SandboxError::LeaseReleaseDenied {
            lease_id: "lease-0-1".to_string(),
            active_workloads: vec![
                "workspace-main:workspace".to_string(),
                "svc-db:service".to_string(),
            ],
        };
        let msg = err.to_string();
        assert!(msg.contains("lease-0-1"));
        assert!(msg.contains("workspace-main:workspace"));
        assert!(msg.contains("svc-db:service"));
    }

    #[test]
    fn checkpoint_catalog_corrupt_display() {
        let err = SandboxError::CheckpointCatalogCorrupt {
            path: PathBuf::from("/tmp/checkpoint-lineage.json"),
            reason: "expected map at line 1 column 1".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("/tmp/checkpoint-lineage.json"));
        assert!(msg.contains("expected map"));
    }

    #[test]
    fn checkpoint_catalog_persistence_display() {
        let err = SandboxError::CheckpointCatalogPersistence {
            path: PathBuf::from("/tmp/checkpoint-lineage.json"),
            reason: "permission denied".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("/tmp/checkpoint-lineage.json"));
        assert!(msg.contains("permission denied"));
    }

    #[test]
    fn checkpoint_lineage_violation_display() {
        let err = SandboxError::CheckpointLineageViolation(
            "Checkpoint child references missing parent root".to_string(),
        );
        assert_eq!(
            err.to_string(),
            "checkpoint lineage violation: Checkpoint child references missing parent root"
        );
    }
}
