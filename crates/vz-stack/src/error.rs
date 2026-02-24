use vz_runtime_contract::MachineErrorCode;

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

    /// Stable machine-classified error with actionable message.
    #[error("{code}: {message}")]
    Machine {
        /// Stable machine-readable code.
        code: MachineErrorCode,
        /// Human-readable context string.
        message: String,
    },
}

impl StackError {
    /// Stable machine-readable code aligned with Runtime V2 taxonomy.
    pub fn machine_code(&self) -> MachineErrorCode {
        fn message_looks_like_timeout(message: &str) -> bool {
            let msg = message.to_ascii_lowercase();
            msg.contains("timeout") || msg.contains("timed out") || msg.contains("deadline")
        }

        fn message_looks_like_not_found(message: &str) -> bool {
            let msg = message.to_ascii_lowercase();
            msg.contains("not found") || msg.contains("no such")
        }

        match self {
            StackError::Store(_) | StackError::Serialization(_) | StackError::VolumeIo(_) => {
                MachineErrorCode::InternalError
            }
            StackError::InvalidSpec(_)
            | StackError::ComposeParse(_)
            | StackError::ComposeValidation(_) => MachineErrorCode::ValidationError,
            StackError::ComposeUnsupportedFeature { .. } => MachineErrorCode::UnsupportedOperation,
            StackError::Network(message)
                if message.starts_with("unsupported_operation:")
                    || message.contains("unsupported operation") =>
            {
                MachineErrorCode::UnsupportedOperation
            }
            StackError::Network(message) if message_looks_like_timeout(message) => {
                MachineErrorCode::Timeout
            }
            StackError::Network(message) if message_looks_like_not_found(message) => {
                MachineErrorCode::NotFound
            }
            StackError::Network(_) => MachineErrorCode::BackendUnavailable,
            StackError::Machine { code, .. } => *code,
        }
    }
}
