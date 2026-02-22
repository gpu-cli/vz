use std::path::PathBuf;

/// Errors produced by platform-agnostic OCI operations (bundle generation, storage).
use oci_spec::OciSpecError;

#[derive(Debug, thiserror::Error)]
pub enum OciError {
    /// Invalid configuration.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// Rootfs directory is missing or invalid.
    #[error("rootfs directory is invalid: {path}")]
    InvalidRootfs {
        /// Rootfs path that failed validation.
        path: PathBuf,
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
