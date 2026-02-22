/// Errors produced by `vz-linux-native` backend operations.
#[derive(Debug, thiserror::Error)]
pub enum LinuxNativeError {
    /// Invalid configuration.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// The OCI runtime binary could not be found.
    #[error("OCI runtime binary not found at '{path}'")]
    RuntimeBinaryNotFound {
        /// Path that was searched.
        path: String,
    },

    /// Rootfs directory is missing or invalid.
    #[error("rootfs directory is invalid: {path}")]
    InvalidRootfs {
        /// Rootfs path that failed validation.
        path: std::path::PathBuf,
    },

    /// Container was not found by ID.
    #[error("container not found: {id}")]
    ContainerNotFound {
        /// Container ID.
        id: String,
    },

    /// An OCI runtime-spec generation or serialization failure.
    #[error(transparent)]
    RuntimeSpec(#[from] oci_spec::OciSpecError),

    /// Filesystem or I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Image store or pull error.
    #[error(transparent)]
    Image(#[from] vz_image::ImageError),

}

/// Convert platform-agnostic `OciError` into `LinuxNativeError`.
impl From<vz_oci::OciError> for LinuxNativeError {
    fn from(e: vz_oci::OciError) -> Self {
        match e {
            vz_oci::OciError::InvalidConfig(msg) => Self::InvalidConfig(msg),
            vz_oci::OciError::InvalidRootfs { path } => Self::InvalidRootfs { path },
            vz_oci::OciError::RuntimeSpec(e) => Self::RuntimeSpec(e),
            vz_oci::OciError::Image(e) => Self::Image(e),
            vz_oci::OciError::Storage(e) => Self::Io(e),
        }
    }
}
