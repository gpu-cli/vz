use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use docker_credential::CredentialRetrievalError;
use vz_image::ImageId;
use vz_linux::LinuxError;

pub use crate::buildkit_rawjson::{
    BuildkitPosition, BuildkitProgressGroup, BuildkitRange, BuildkitSolveStatus,
    BuildkitSourceInfo, BuildkitVertex, BuildkitVertexLog, BuildkitVertexStatus,
    BuildkitVertexWarning,
};

mod artifacts;
mod common;
mod manager;
mod pipeline;
mod proxy;

#[cfg(test)]
mod tests;

pub use manager::BuildManager;
pub use pipeline::{build_image, build_image_with_events, cache_disk_usage, cache_prune};
pub use proxy::{create_buildkit_channel, start_unix_proxy};

#[cfg(test)]
pub(crate) use artifacts::import_oci_tar_to_store;
#[cfg(test)]
pub(crate) use manager::{BuildEventSink, BuildPipeline, BuildPipelineFuture};
#[cfg(test)]
pub(crate) use pipeline::{
    docker_auth_keys_for_registry, parse_dockerfile_registries, parse_dockerfile_syntax_registry,
    registries_for_build,
};

const BUILDKIT_VERSION: &str = "0.19.0";
const BUILDKITD_BINARY: &str = "buildkitd";
const BUILDKIT_RUNC_BINARY: &str = "buildkit-runc";
const BUILDCTL_BINARY: &str = "buildctl";
const VERSION_FILE: &str = "version.json";
const BUILD_OUTPUT_ARCHIVE: &str = "image.tar";
const BUILDKITD_ADDR: &str = "tcp://127.0.0.1:8372";
const BUILDKIT_SETUP_TIMEOUT: Duration = Duration::from_secs(90);
const BUILDKIT_BUILD_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const BUILDKIT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);
const BUILDKIT_RUNC_GUEST_PATH: &str = "/tmp/runc";
const BUILDKIT_AUTH_TAG: &str = "buildkit-auth";
const BUILDKIT_AUTH_GUEST_DIR: &str = "/mnt/buildkit-auth";
const BUILDKIT_AUTH_GUEST_CONFIG: &str = "/mnt/buildkit-auth/config.json";
const BUILDKIT_SNAPSHOTTER: &str = "overlayfs";
const BUILDKIT_CACHE_KEEP_DURATION: &str = "168h";
const BUILDKIT_CACHE_KEEP_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const BUILDKIT_CACHE_DISK_IMAGE: &str = "cache.img";
const BUILDKIT_CACHE_DISK_SIZE_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const BUILDKIT_VM_MEMORY_MB: u64 = 8192;

/// Destination for built image output.
#[derive(Debug, Clone, Default)]
pub enum BuildOutput {
    /// Import built image directly into local vz image store.
    #[default]
    VzStore,
    /// Push built image to registry.
    RegistryPush,
    /// Write OCI tar archive to host path.
    OciTar {
        /// Destination path for generated archive.
        dest: PathBuf,
    },
}

/// Build progress rendering mode passed to buildctl.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BuildProgress {
    /// Buildctl picks plain vs tty based on terminal detection.
    #[default]
    Auto,
    /// Always print plain logs.
    Plain,
    /// Always print tty progress UI.
    Tty,
    /// Stream machine-readable status objects (one JSON object per line).
    RawJson,
}

impl BuildProgress {
    fn as_buildctl_value(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Plain => "plain",
            Self::Tty => "tty",
            Self::RawJson => "rawjson",
        }
    }
}

/// Output stream source for BuildKit log chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildLogStream {
    Stdout,
    Stderr,
}

/// Event emitted while running a BuildKit build.
#[derive(Debug, Clone)]
pub enum BuildEvent {
    /// Lifecycle status update (VM boot, import stage, etc.).
    Status { message: String },
    /// Raw output bytes from buildctl.
    Output {
        stream: BuildLogStream,
        chunk: Vec<u8>,
    },
    /// Parsed BuildKit solve status from `--progress=rawjson`.
    SolveStatus { status: BuildkitSolveStatus },
    /// Rawjson decode failure for a single output line.
    RawJsonDecodeError { line: String, error: String },
}

/// Request for a Dockerfile build executed by BuildKit.
#[derive(Debug, Clone)]
pub struct BuildRequest {
    /// Host directory used as Docker build context.
    pub context_dir: PathBuf,
    /// Dockerfile path. Relative paths are resolved against `context_dir`.
    pub dockerfile: PathBuf,
    /// Image reference (for local tag and/or registry push).
    pub tag: String,
    /// Optional multi-stage target name.
    pub target: Option<String>,
    /// Optional cache sources (for example registry references).
    pub cache_from: Vec<String>,
    /// Build-time key/value arguments.
    pub build_args: BTreeMap<String, String>,
    /// Build secrets forwarded to BuildKit (`id=...,src=...`).
    pub secrets: Vec<String>,
    /// Disable BuildKit cache for this build.
    pub no_cache: bool,
    /// Output destination mode.
    pub output: BuildOutput,
    /// Progress rendering mode.
    pub progress: BuildProgress,
}

/// Successful BuildKit execution result.
#[derive(Debug, Clone)]
pub struct BuildResult {
    /// Stored image manifest digest when imported into local store.
    pub image_id: Option<ImageId>,
    /// Resolved image reference.
    pub tag: String,
    /// Path to emitted archive when output mode writes to disk.
    pub output_path: Option<PathBuf>,
    /// Whether the image was pushed to a registry.
    pub pushed: bool,
}

/// Build manager failures.
#[derive(Debug, thiserror::Error)]
pub enum BuildManagerError {
    /// The requested build is unknown.
    #[error("build not found: {build_id}")]
    BuildNotFound {
        /// Missing build identifier.
        build_id: String,
    },
    /// Idempotency key was reused with a different normalized request.
    #[error(
        "idempotency key '{key}' conflicts with existing request (build_id={existing_build_id})"
    )]
    IdempotencyConflict {
        /// Idempotency key provided by caller.
        key: String,
        /// Existing build ID associated with the key.
        existing_build_id: String,
    },
    /// Failed to normalize request data used for idempotency matching.
    #[error("failed to normalize build request: {details}")]
    RequestNormalization {
        /// Serialization/normalization details.
        details: String,
    },
    /// Wrapped build pipeline failure.
    #[error(transparent)]
    Buildkit(#[from] BuildkitError),
}

/// Options for `buildctl prune` cache command.
#[derive(Debug, Clone, Default)]
pub struct CachePruneOptions {
    /// Remove all cache entries.
    pub all: bool,
    /// Keep cache newer than this duration (for example `24h`).
    pub keep_duration: Option<String>,
    /// Keep this much storage (for example `5GB`).
    pub keep_storage: Option<String>,
}

/// BuildKit integration errors.
#[derive(Debug, thiserror::Error)]
pub enum BuildkitError {
    /// Invalid user-provided build configuration.
    #[error("invalid build configuration: {0}")]
    InvalidConfig(String),

    /// HOME is unavailable when resolving `~/.vz/buildkit`.
    #[error("home directory is not set (cannot resolve ~/.vz/buildkit)")]
    HomeDirectoryUnavailable,

    /// Guest-side setup command failed.
    #[error("guest command failed ({command}) with exit code {exit_code}: {stderr}\n{stdout}")]
    GuestCommandFailed {
        /// Command label for diagnostics.
        command: String,
        /// Exit code returned by the guest command.
        exit_code: i32,
        /// Captured stdout.
        stdout: String,
        /// Captured stderr.
        stderr: String,
    },

    /// BuildKit solve or cache command failed.
    #[error("buildctl command failed with exit code {exit_code}: {stderr}\n{stdout}")]
    BuildFailed {
        /// Exit code returned by buildctl.
        exit_code: i32,
        /// Captured stdout.
        stdout: String,
        /// Captured stderr.
        stderr: String,
    },

    /// OCI layout import encountered invalid or unsupported data.
    #[error("invalid OCI image layout: {0}")]
    InvalidOciLayout(String),

    /// Blob digest did not match expected descriptor digest.
    #[error("blob digest mismatch for {digest}: expected {expected}, found {found}")]
    DigestMismatch {
        /// Digest identifier from descriptor.
        digest: String,
        /// Expected hash component.
        expected: String,
        /// Computed hash component.
        found: String,
    },

    /// Unsupported digest algorithm in OCI descriptor.
    #[error("unsupported digest algorithm '{algorithm}' in {digest}")]
    UnsupportedDigestAlgorithm {
        /// Full digest string.
        digest: String,
        /// Algorithm prefix (before colon).
        algorithm: String,
    },

    /// Wrapped Linux guest orchestration error.
    #[error(transparent)]
    Linux(#[from] LinuxError),

    /// Wrapped filesystem I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Wrapped JSON parse/serialization error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Wrapped HTTP download error.
    #[error(transparent)]
    Http(#[from] reqwest::Error),

    /// Docker credential helper lookup failed.
    #[error("failed to resolve docker credentials for registry '{registry}': {source}")]
    CredentialLookup {
        /// Registry host used for credential lookup.
        registry: String,
        /// Underlying credential helper failure.
        #[source]
        source: CredentialRetrievalError,
    },
}
