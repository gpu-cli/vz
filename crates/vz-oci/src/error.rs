use std::path::PathBuf;

/// Errors produced by `vz-oci` runtime operations.
use oci_spec::OciSpecError;

#[derive(Debug, thiserror::Error)]
pub enum OciError {
    /// Invalid runtime or run configuration.
    #[error("invalid runtime config: {0}")]
    InvalidConfig(String),

    /// Rootfs directory is missing or invalid.
    #[error("rootfs directory is invalid: {path}")]
    InvalidRootfs {
        /// Rootfs path that failed validation.
        path: PathBuf,
    },

    /// Linux backend error.
    #[error(transparent)]
    Linux(#[from] vz_linux::LinuxError),

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
