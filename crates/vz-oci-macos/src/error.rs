/// Errors produced by the macOS OCI runtime backend.
use std::path::PathBuf;

use oci_spec::OciSpecError;

#[derive(Debug, thiserror::Error)]
pub enum MacosOciError {
    /// Invalid runtime or run configuration.
    #[error("invalid runtime config: {0}")]
    InvalidConfig(String),

    /// Rootfs directory is missing or invalid.
    #[error("rootfs directory is invalid: {path}")]
    InvalidRootfs {
        /// Rootfs path that failed validation.
        path: PathBuf,
    },

    /// Linux VM backend error.
    #[error(transparent)]
    Linux(#[from] vz_linux::LinuxError),

    /// Requested execution session is not active.
    #[error("execution session not found: {execution_id}")]
    ExecutionSessionNotFound {
        /// Daemon execution identifier.
        execution_id: String,
    },

    /// Interactive execution control is unsupported for the session.
    #[error("execution control unsupported for `{operation}`: {reason}")]
    ExecutionControlUnsupported {
        /// Operation name.
        operation: String,
        /// Actionable unsupported reason.
        reason: String,
    },

    /// The selected execution strategy is not yet implemented.
    #[error("execution mode '{mode}' is not yet supported")]
    UnsupportedExecutionMode {
        /// Requested execution strategy name.
        mode: String,
    },

    /// Runtime-spec generation or serialization failed.
    #[error(transparent)]
    RuntimeSpec(#[from] OciSpecError),

    /// Image store or pull error.
    #[error(transparent)]
    Image(#[from] vz_image::ImageError),

    /// Storage operation failed.
    #[error("storage operation failed: {0}")]
    Storage(#[from] std::io::Error),
}

/// Convert platform-agnostic `OciError` into `MacosOciError`.
///
/// This allows `?` propagation when calling `vz_oci` bundle and container_store
/// functions from the macOS runtime.
impl From<vz_oci::OciError> for MacosOciError {
    fn from(e: vz_oci::OciError) -> Self {
        match e {
            vz_oci::OciError::InvalidConfig(msg) => Self::InvalidConfig(msg),
            vz_oci::OciError::InvalidRootfs { path } => Self::InvalidRootfs { path },
            vz_oci::OciError::RuntimeSpec(e) => Self::RuntimeSpec(e),
            vz_oci::OciError::Image(e) => Self::Image(e),
            vz_oci::OciError::Storage(e) => Self::Storage(e),
        }
    }
}
