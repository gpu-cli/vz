use std::collections::BTreeMap;

use vz_runtime_contract::{
    MachineError, MachineErrorCode, MachineErrorEnvelope, RequestMetadata, RuntimeError,
};

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
    #[error("unsupported_operation: surface=compose; feature={feature}; reason={reason}")]
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

    fn machine_details(&self) -> BTreeMap<String, String> {
        let mut details = BTreeMap::new();
        match self {
            StackError::Store(error) => {
                details.insert("reason".to_string(), error.to_string());
            }
            StackError::Serialization(error) => {
                details.insert("reason".to_string(), error.to_string());
            }
            StackError::InvalidSpec(message)
            | StackError::Network(message)
            | StackError::ComposeParse(message)
            | StackError::ComposeValidation(message) => {
                details.insert("reason".to_string(), message.clone());
            }
            StackError::ComposeUnsupportedFeature { feature, reason } => {
                details.insert("feature".to_string(), feature.clone());
                details.insert("reason".to_string(), reason.clone());
            }
            StackError::VolumeIo(error) => {
                details.insert("reason".to_string(), error.to_string());
            }
            StackError::Machine { message, .. } => {
                details.insert("reason".to_string(), message.clone());
            }
        }
        details
    }

    /// Convert a stack error into the shared machine-error payload.
    pub fn to_machine_error(&self, metadata: &RequestMetadata) -> MachineError {
        MachineError::new(
            self.machine_code(),
            self.to_string(),
            metadata.request_id.clone(),
            self.machine_details(),
        )
    }

    /// Convert a stack error into the shared transport error envelope.
    pub fn to_machine_error_envelope(&self, metadata: &RequestMetadata) -> MachineErrorEnvelope {
        MachineErrorEnvelope::new(self.to_machine_error(metadata))
    }
}

impl From<RuntimeError> for StackError {
    fn from(error: RuntimeError) -> Self {
        StackError::Machine {
            code: error.machine_code(),
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_error_conversion_preserves_machine_code() {
        let stack_error = StackError::from(RuntimeError::UnsupportedOperation {
            operation: "create_checkpoint".to_string(),
            reason: "missing fs_quick_checkpoint capability".to_string(),
        });

        assert_eq!(
            stack_error.machine_code(),
            MachineErrorCode::UnsupportedOperation
        );
        assert!(matches!(stack_error, StackError::Machine { .. }));
    }

    #[test]
    fn machine_error_envelope_propagates_request_id_and_details() {
        let metadata = RequestMetadata::from_optional_refs(Some("req_77"), None);
        let stack_error = StackError::ComposeUnsupportedFeature {
            feature: "deploy.mode".to_string(),
            reason: "replicated mode is not supported".to_string(),
        };

        let envelope = stack_error.to_machine_error_envelope(&metadata);
        assert_eq!(envelope.error.code, MachineErrorCode::UnsupportedOperation);
        assert_eq!(envelope.error.request_id.as_deref(), Some("req_77"));
        assert_eq!(
            envelope.error.details.get("feature").map(String::as_str),
            Some("deploy.mode")
        );
        assert_eq!(
            envelope.error.details.get("reason").map(String::as_str),
            Some("replicated mode is not supported")
        );
    }

    #[test]
    fn compose_unsupported_message_prefix_is_stable() {
        let stack_error = StackError::ComposeUnsupportedFeature {
            feature: "services.web.networks.frontend.aliases".to_string(),
            reason: "network attachment options are not supported".to_string(),
        };

        let message = stack_error.to_string();
        assert!(message.starts_with("unsupported_operation:"));
        assert!(message.contains("surface=compose"));
        assert!(message.contains("feature=services.web.networks.frontend.aliases"));
    }
}
