use std::path::PathBuf;

/// Errors produced by `vz-linux`.
#[derive(Debug, thiserror::Error)]
pub enum LinuxError {
    /// Invalid Linux VM config value.
    #[error("invalid linux vm config: {0}")]
    InvalidConfig(String),

    /// Missing `$HOME` when resolving default install paths.
    #[error("home directory is not set (cannot resolve ~/.vz/linux)")]
    HomeDirectoryUnavailable,

    /// Kernel artifacts are missing and no bundle path was provided.
    #[error(
        "missing Linux kernel artifacts in {dir}; expected vmlinux, initramfs.img, youki, version.json"
    )]
    MissingKernelArtifacts {
        /// Directory where artifacts were expected.
        dir: PathBuf,
    },

    /// Installed or bundled artifacts have an unexpected version.
    #[error("kernel artifact version mismatch: expected agent {expected}, found {found}")]
    VersionMismatch {
        /// Expected agent version (crate version).
        expected: String,
        /// Found agent version in `version.json`.
        found: String,
    },

    /// Kernel artifact metadata is missing protocol revision information.
    #[error(
        "kernel artifact metadata missing agent protocol revision; expected revision {expected}. Rebuild artifacts to refresh version.json"
    )]
    MissingProtocolRevision {
        /// Host-required protocol revision.
        expected: u32,
    },

    /// Kernel artifact metadata protocol revision does not match host expectations.
    #[error(
        "kernel artifact protocol revision mismatch: expected {expected}, found {found}; rebuild guest artifacts"
    )]
    ProtocolRevisionMismatch {
        /// Host-required protocol revision.
        expected: u32,
        /// Artifact-declared protocol revision.
        found: u32,
    },

    /// Artifact file digest did not match `version.json` checksum.
    #[error(
        "kernel artifact checksum mismatch for {artifact} at {path}: expected {expected}, found {found}"
    )]
    ArtifactChecksumMismatch {
        /// Artifact identifier (`vmlinux`, `initramfs.img`, or `youki`).
        artifact: String,
        /// Artifact path that failed validation.
        path: String,
        /// Expected SHA256 hex digest from `version.json`.
        expected: String,
        /// Computed SHA256 hex digest of artifact bytes.
        found: String,
    },

    /// Guest responded with a non-Linux OS identifier.
    #[error("guest agent reported unexpected os: {0}")]
    UnexpectedGuestOs(String),

    /// Guest protocol revision does not match host expectations.
    #[error(
        "guest agent protocol revision mismatch: expected {expected}, found {found}; rebuild guest artifacts (e.g. `make -C linux initramfs`) and ensure runtime uses updated bundle"
    )]
    GuestProtocolRevisionMismatch {
        /// Host-required protocol revision.
        expected: u32,
        /// Guest-reported protocol revision.
        found: u32,
    },

    /// Guest agent did not become reachable in time.
    #[error("guest agent unreachable after {attempts} attempts: {last_error}")]
    AgentUnreachable {
        /// Number of connection attempts made.
        attempts: u32,
        /// Last observed connection/handshake error.
        last_error: String,
    },

    /// Handshake or ping response was invalid.
    #[error("agent protocol error: {0}")]
    Protocol(String),

    /// Wrapped VM/runtime error from `vz`.
    #[error(transparent)]
    Vm(#[from] vz::VzError),

    /// Wrapped filesystem I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Wrapped JSON parsing/serialization error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// gRPC transport or status error from the guest agent.
    #[error("grpc error: {0}")]
    Grpc(Box<tonic::Status>),

    /// gRPC transport-level connection error.
    #[error("grpc transport error: {0}")]
    GrpcTransport(Box<tonic::transport::Error>),
}

impl From<tonic::Status> for LinuxError {
    fn from(status: tonic::Status) -> Self {
        Self::Grpc(Box::new(status))
    }
}

impl From<tonic::transport::Error> for LinuxError {
    fn from(err: tonic::transport::Error) -> Self {
        Self::GrpcTransport(Box::new(err))
    }
}
