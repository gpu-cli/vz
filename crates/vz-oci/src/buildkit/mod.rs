//! BuildKit artifact provisioning support.
//!
//! Downloads and validates pinned BuildKit binaries for linux/arm64 and
//! installs only the runtime binaries needed by the BuildKit VM.

mod artifacts;
mod auth;
mod client;
mod filesync;
mod output;
mod progress;
mod session_tunnel;

pub use artifacts::{
    BUILDKIT_VERSION, BuildkitArtifacts, BuildkitError, BuildkitVersionMetadata,
    ensure_buildkit_artifacts,
};
pub use auth::{DockerAuthError, DockerAuthProvider, ResolvedRegistryCredential};
pub use client::{
    BuildClient, BuildClientError, BuildOutput, BuildProgressStream, BuildRequest, BuildResult,
    BuildSession, SecretSpec, SshSpec,
};
pub use filesync::{FileSyncError, FileSyncService, LocalFileSync};
pub use output::{
    BuildOutputError, BuildOutputResult, import_oci_tar_to_store, materialize_build_output,
};
pub use progress::{BuildLogStream, BuildProgress, BuildProgressMapper};
