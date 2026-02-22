use std::io;

use docker_credential::CredentialRetrievalError;
use oci_distribution::errors::OciDistributionError;

/// Errors produced by image store and pull operations.
#[derive(Debug, thiserror::Error)]
pub enum ImageError {
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

    /// Image platform is not supported by this runtime.
    #[error("image '{reference}' does not have a matching platform variant: {details}")]
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

    /// Registry pull operation failed.
    #[error("registry pull failed for image {reference}: {source}")]
    Pull {
        /// The image reference used for the pull.
        reference: String,
        /// Underlying registry error.
        #[source]
        source: OciDistributionError,
    },

    /// Docker credential helper reported an error.
    #[error("failed to read docker credentials: {0}")]
    CredentialLookup(#[from] CredentialRetrievalError),

    /// Credential lookup failed.
    #[error("failed to resolve image registry credentials: {0}")]
    CredentialFailure(String),

    /// User requested an explicit authentication mode that is currently unsupported.
    #[error("unsupported authentication mode: {0}")]
    AuthenticationUnsupported(String),

    /// Storage operation failed while pulling or writing image data.
    #[error("image store operation failed: {0}")]
    Storage(#[from] io::Error),

    /// Invalid runtime or image configuration.
    #[error("invalid image config: {0}")]
    InvalidConfig(String),
}
