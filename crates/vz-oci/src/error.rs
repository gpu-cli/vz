use std::path::PathBuf;

/// Errors produced by `vz-oci` runtime operations.
use std::io;

use docker_credential::CredentialRetrievalError;
use oci_distribution::errors::OciDistributionError;

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

    /// Could not parse image reference string.
    #[error("invalid image reference '{reference}': {reason}")]
    ImageReferenceParse {
        /// The provided image reference string.
        reference: String,
        /// Parse failure reason.
        reason: String,
    },

    /// Failed to parse an image configuration payload.
    #[error("failed to parse image config: {0}")]
    ImageConfigParse(#[from] serde_json::Error),

    /// Credential lookup failed.
    #[error("failed to resolve image registry credentials: {0}")]
    CredentialFailure(String),

    /// Docker credential helper reported an error.
    #[error("failed to read docker credentials: {0}")]
    CredentialLookup(#[from] CredentialRetrievalError),

    /// Registry pull operation failed.
    #[error("registry pull failed for image {reference}: {source}")]
    Pull {
        /// The image reference used for the pull.
        reference: String,
        /// Underlying registry error.
        #[source]
        source: OciDistributionError,
    },

    /// Image platform is not supported by this runtime.
    #[error("image '{reference}' does not have a linux/arm64 variant: {details}")]
    PlatformMismatch {
        /// The image reference used for the pull.
        reference: String,
        /// Additional information from resolver or registry.
        details: String,
    },

    /// The registry returned image data that did not match its advertised digest.
    #[error("digest mismatch for '{digest}': expected '{expected}', got '{actual}'")]
    DigestMismatch {
        /// Blob descriptor digest (for example `sha256:...`).
        digest: String,
        expected: String,
        actual: String,
    },

    /// The registry returned a digest algorithm this runtime does not verify.
    #[error("unsupported digest algorithm '{algorithm}'")]
    UnsupportedDigestAlgorithm {
        /// Algorithm name.
        algorithm: String,
    },

    /// Storage operation failed while pulling or writing image data.
    #[error("image store operation failed: {0}")]
    Storage(#[from] io::Error),

    /// User requested an explicit authentication mode that is currently unsupported.
    #[error("unsupported authentication mode: {0}")]
    AuthenticationUnsupported(String),
}
