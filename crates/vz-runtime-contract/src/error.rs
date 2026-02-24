//! Backend-neutral runtime error type.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Stable machine-readable error codes for Runtime V2 operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MachineErrorCode {
    /// Input failed validation or violated schema constraints.
    ValidationError,
    /// Referenced entity was not found.
    NotFound,
    /// Requested transition conflicts with current state.
    StateConflict,
    /// Policy explicitly denied the requested operation.
    PolicyDenied,
    /// Operation exceeded timeout/deadline.
    Timeout,
    /// Backend was unavailable or transport/storage was disrupted.
    BackendUnavailable,
    /// Operation unsupported by current backend/capabilities.
    UnsupportedOperation,
    /// Unexpected internal runtime failure.
    InternalError,
}

impl MachineErrorCode {
    /// All stable machine-readable codes.
    pub const ALL: [MachineErrorCode; 8] = [
        MachineErrorCode::ValidationError,
        MachineErrorCode::NotFound,
        MachineErrorCode::StateConflict,
        MachineErrorCode::PolicyDenied,
        MachineErrorCode::Timeout,
        MachineErrorCode::BackendUnavailable,
        MachineErrorCode::UnsupportedOperation,
        MachineErrorCode::InternalError,
    ];

    /// Canonical snake_case code string.
    pub const fn as_str(self) -> &'static str {
        match self {
            MachineErrorCode::ValidationError => "validation_error",
            MachineErrorCode::NotFound => "not_found",
            MachineErrorCode::StateConflict => "state_conflict",
            MachineErrorCode::PolicyDenied => "policy_denied",
            MachineErrorCode::Timeout => "timeout",
            MachineErrorCode::BackendUnavailable => "backend_unavailable",
            MachineErrorCode::UnsupportedOperation => "unsupported_operation",
            MachineErrorCode::InternalError => "internal_error",
        }
    }
}

impl std::fmt::Display for MachineErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Errors returned by [`RuntimeBackend`](crate::RuntimeBackend) operations.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// Invalid runtime or run configuration.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// Container not found by ID.
    #[error("container not found: {id}")]
    ContainerNotFound {
        /// The container ID that was looked up.
        id: String,
    },

    /// Image not found or pull failed.
    #[error("image not found: {reference}")]
    ImageNotFound {
        /// The image reference that was looked up.
        reference: String,
    },

    /// Image pull failed.
    #[error("pull failed for {reference}: {reason}")]
    PullFailed {
        /// The image reference that failed.
        reference: String,
        /// Reason for the failure.
        reason: String,
    },

    /// Container lifecycle operation failed.
    #[error("container {id}: {reason}")]
    ContainerFailed {
        /// Container identifier.
        id: String,
        /// Reason for the failure.
        reason: String,
    },

    /// Exec operation failed.
    #[error("exec failed in container {id}: {reason}")]
    ExecFailed {
        /// Container identifier.
        id: String,
        /// Reason for the failure.
        reason: String,
    },

    /// Backend cannot perform the requested operation on this host/backend.
    #[error("unsupported operation `{operation}`: {reason}")]
    UnsupportedOperation {
        /// Operation name, for example `network_setup` or `vm_full_checkpoint`.
        operation: String,
        /// Actionable reason this operation is unsupported.
        reason: String,
    },

    /// Policy hook explicitly denied the requested operation.
    #[error("policy denied `{operation}`: {reason}")]
    PolicyDenied {
        /// Operation name denied by policy.
        operation: String,
        /// Actionable denial reason.
        reason: String,
    },

    /// Rootfs directory is missing or invalid.
    #[error("invalid rootfs: {path}")]
    InvalidRootfs {
        /// Rootfs path that failed validation.
        path: PathBuf,
    },

    /// Filesystem or I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Backend-specific error preserved for diagnostics.
    ///
    /// Backends wrap their native errors here so callers can inspect
    /// the source chain for backend-specific details.
    #[error("{message}")]
    Backend {
        /// Human-readable error description.
        message: String,
        /// Backend-specific error source.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl RuntimeError {
    /// Stable machine-readable code for this runtime error.
    pub fn machine_code(&self) -> MachineErrorCode {
        fn reason_looks_like_timeout(reason: &str) -> bool {
            let reason_lc = reason.to_ascii_lowercase();
            reason_lc.contains("timeout")
                || reason_lc.contains("timed out")
                || reason_lc.contains("deadline")
        }

        match self {
            RuntimeError::InvalidConfig(_) | RuntimeError::InvalidRootfs { .. } => {
                MachineErrorCode::ValidationError
            }
            RuntimeError::ContainerNotFound { .. } | RuntimeError::ImageNotFound { .. } => {
                MachineErrorCode::NotFound
            }
            RuntimeError::PullFailed { reason, .. } if reason_looks_like_timeout(reason) => {
                MachineErrorCode::Timeout
            }
            RuntimeError::PullFailed { .. } => MachineErrorCode::BackendUnavailable,
            RuntimeError::ContainerFailed { reason, .. }
            | RuntimeError::ExecFailed { reason, .. }
                if reason_looks_like_timeout(reason) =>
            {
                MachineErrorCode::Timeout
            }
            RuntimeError::ContainerFailed { .. } | RuntimeError::ExecFailed { .. } => {
                MachineErrorCode::StateConflict
            }
            RuntimeError::PolicyDenied { .. } => MachineErrorCode::PolicyDenied,
            RuntimeError::UnsupportedOperation { .. } => MachineErrorCode::UnsupportedOperation,
            RuntimeError::Io(_) => MachineErrorCode::BackendUnavailable,
            RuntimeError::Backend { .. } => MachineErrorCode::InternalError,
        }
    }
}
