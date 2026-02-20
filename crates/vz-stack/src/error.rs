/// Errors produced by `vz-stack` operations.
#[derive(Debug, thiserror::Error)]
pub enum StackError {
    /// State store operation failed.
    #[error("state store error: {0}")]
    Store(#[from] rusqlite::Error),

    /// Serialization or deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Invalid stack specification.
    #[error("invalid stack spec: {0}")]
    InvalidSpec(String),

    /// Network backend operation failed.
    #[error("network error: {0}")]
    Network(String),

    /// Compose YAML parsing failed.
    #[error("compose parse error: {0}")]
    ComposeParse(String),

    /// Compose file uses an unsupported feature.
    #[error("unsupported compose feature `{feature}`: {reason}")]
    ComposeUnsupportedFeature {
        /// The unsupported key or feature name.
        feature: String,
        /// Actionable message explaining why and what to do instead.
        reason: String,
    },

    /// Compose file validation failed.
    #[error("compose validation error: {0}")]
    ComposeValidation(String),

    /// Filesystem operation failed (volume create/remove).
    #[error("volume IO error: {0}")]
    VolumeIo(#[from] std::io::Error),
}
