//! Backend-neutral runtime error type.

use std::path::PathBuf;

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
