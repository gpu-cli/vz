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
}
