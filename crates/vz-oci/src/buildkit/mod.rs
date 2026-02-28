//! BuildKit artifact provisioning support.
//!
//! Downloads and validates pinned BuildKit binaries for linux/arm64 and
//! installs only the runtime binaries needed by the BuildKit VM.

mod artifacts;
mod client;

pub use artifacts::{
    BUILDKIT_VERSION, BuildkitArtifacts, BuildkitError, BuildkitVersionMetadata,
    ensure_buildkit_artifacts,
};
pub use client::{
    BuildClient, BuildClientError, BuildOutput, BuildRequest, BuildResult, BuildSession,
    SecretSpec, SshSpec,
};
