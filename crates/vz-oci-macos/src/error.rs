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

    /// A caller-selected container ID is already owned by another lifecycle.
    #[error("container already exists: {id}")]
    ContainerAlreadyExists { id: String },

    /// A generation-qualified cleanup proof no longer owns the container ID.
    #[error("container generation ownership mismatch for {id}: {reason}")]
    ContainerOwnershipMismatch {
        /// Container identifier named by the stale or foreign proof.
        id: String,
        /// Concrete mismatch details for diagnostics.
        reason: String,
    },

    /// No shared-runtime boot exists for the exact Docker-readiness request.
    #[error("shared runtime is absent for stack: {stack_id}")]
    SharedRuntimeAbsent {
        /// Stable stack selector that had no active boot.
        stack_id: String,
    },

    /// A reusable stack selector now names a replacement runtime boot.
    #[error(
        "shared runtime identity mismatch for stack {stack_id}: expected {expected_incarnation_id}, found {current_incarnation_id}"
    )]
    SharedRuntimeIdentityMismatch {
        /// Stable stack selector shared by the stale and current boots.
        stack_id: String,
        /// Incarnation authorized by the caller.
        expected_incarnation_id: String,
        /// Incarnation currently registered in the runtime.
        current_incarnation_id: String,
    },

    /// Container metadata or generation was not found.
    #[error("container not found: {id}")]
    ContainerNotFound { id: String },

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

    /// The backend cannot perform the requested operation without degrading its semantics.
    #[error("unsupported operation `{operation}`: {reason}")]
    UnsupportedOperation {
        /// Stable operation name from the runtime contract.
        operation: String,
        /// Actionable reason the operation is unsupported.
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

    /// Docker facade binary provisioning or validation failed.
    #[error(transparent)]
    DockerArtifacts(#[from] vz_oci::DockerArtifactError),

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
