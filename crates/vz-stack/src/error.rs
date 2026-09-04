use std::collections::BTreeMap;

use vz_runtime_contract::{
    MachineError, MachineErrorCode, MachineErrorEnvelope, RequestMetadata, RuntimeError,
    TopologyLifecycleError, TopologyResolutionError, runtime_error_machine_error,
};

/// Owner recorded for a physical/runtime resource key that could not be reserved.
#[derive(Debug, thiserror::Error)]
#[error(
    "owned resource collision: kind={resource_kind}; resource_id={resource_id}; existing_environment_id={existing_environment_id}; existing_machine_id={existing_machine_id:?}"
)]
pub struct OwnedResourceCollisionError {
    pub resource_kind: String,
    pub resource_id: String,
    pub existing_environment_id: String,
    pub existing_machine_id: Option<String>,
}

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

    /// A Developer Environment selector did not resolve uniquely.
    #[error(transparent)]
    TopologyResolution(Box<TopologyResolutionError>),

    /// A durable Developer Environment lifecycle operation was rejected.
    #[error(transparent)]
    TopologyLifecycle(Box<TopologyLifecycleError>),

    /// A physical/runtime resource key is already reserved by another owner.
    #[error(transparent)]
    OwnedResourceCollision(Box<OwnedResourceCollisionError>),

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

    /// Runtime failure retained in typed form so action transports preserve
    /// its variant-specific structured details.
    #[error(transparent)]
    Runtime(RuntimeError),

    /// Fully classified machine error retained across stack execution.
    #[error("{code}: {message}", code = .0.code, message = .0.message)]
    TypedMachine(MachineError),
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
            StackError::TopologyResolution(error) => match error.as_ref() {
                TopologyResolutionError::NotFound { .. } => MachineErrorCode::NotFound,
                TopologyResolutionError::Ambiguous { .. }
                | TopologyResolutionError::SelectionRequired { .. } => {
                    MachineErrorCode::StateConflict
                }
                TopologyResolutionError::InvalidSelector { .. } => {
                    MachineErrorCode::ValidationError
                }
            },
            StackError::TopologyLifecycle(error) => match error.as_ref() {
                TopologyLifecycleError::MachineStepNotFound { .. } => MachineErrorCode::NotFound,
                TopologyLifecycleError::InvalidOperation { .. } => {
                    MachineErrorCode::ValidationError
                }
                TopologyLifecycleError::InvalidTransition { .. }
                | TopologyLifecycleError::OperationConflict { .. }
                | TopologyLifecycleError::GenerationMismatch { .. }
                | TopologyLifecycleError::OperationMismatch { .. }
                | TopologyLifecycleError::MachineStepMismatch { .. }
                | TopologyLifecycleError::OwnershipStepMismatch { .. }
                | TopologyLifecycleError::OperationIncomplete { .. }
                | TopologyLifecycleError::OperationFailed { .. }
                | TopologyLifecycleError::DeleteRequired { .. }
                | TopologyLifecycleError::DeletedEnvironmentIsNotLive { .. } => {
                    MachineErrorCode::StateConflict
                }
            },
            StackError::OwnedResourceCollision(_) => MachineErrorCode::StateConflict,
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
            StackError::Runtime(error) => error.machine_code(),
            StackError::TypedMachine(error) => error.code,
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
            StackError::TopologyResolution(error) => {
                details.insert("reason".to_string(), error.to_string());
            }
            StackError::TopologyLifecycle(error) => {
                details.insert("reason".to_string(), error.to_string());
            }
            StackError::OwnedResourceCollision(error) => {
                details.insert("resource_kind".to_string(), error.resource_kind.clone());
                details.insert("resource_id".to_string(), error.resource_id.clone());
                details.insert(
                    "existing_environment_id".to_string(),
                    error.existing_environment_id.clone(),
                );
                if let Some(machine_id) = &error.existing_machine_id {
                    details.insert("existing_machine_id".to_string(), machine_id.clone());
                }
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
            StackError::Runtime(error) => {
                details.insert("reason".to_string(), error.to_string());
            }
            StackError::TypedMachine(error) => details.extend(error.details.clone()),
        }
        details
    }

    /// Convert a stack error into the shared machine-error payload.
    pub fn to_machine_error(&self, metadata: &RequestMetadata) -> MachineError {
        if let StackError::Runtime(error) = self {
            return runtime_error_machine_error(error, metadata);
        }
        if let StackError::TypedMachine(error) = self {
            let mut error = error.clone();
            if metadata.request_id.is_some() {
                error.request_id = metadata.request_id.clone();
            }
            return error;
        }
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

impl From<TopologyResolutionError> for StackError {
    fn from(error: TopologyResolutionError) -> Self {
        Self::TopologyResolution(Box::new(error))
    }
}

impl From<TopologyLifecycleError> for StackError {
    fn from(error: TopologyLifecycleError) -> Self {
        Self::TopologyLifecycle(Box::new(error))
    }
}

impl From<RuntimeError> for StackError {
    fn from(error: RuntimeError) -> Self {
        StackError::Runtime(error)
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
        assert!(matches!(stack_error, StackError::Runtime(_)));
    }

    #[test]
    fn topology_lifecycle_errors_preserve_stable_machine_codes() {
        let conflict = StackError::from(TopologyLifecycleError::GenerationMismatch {
            operation_id: "lop_example".to_string(),
            expected: 4,
            found: 3,
        });
        assert_eq!(conflict.machine_code(), MachineErrorCode::StateConflict);

        let not_found = StackError::from(TopologyLifecycleError::MachineStepNotFound {
            operation_id: "lop_example".to_string(),
            machine_id: "mch_missing".to_string(),
        });
        assert_eq!(not_found.machine_code(), MachineErrorCode::NotFound);

        let invalid = StackError::from(TopologyLifecycleError::InvalidOperation {
            reason: "empty request hash".to_string(),
        });
        assert_eq!(invalid.machine_code(), MachineErrorCode::ValidationError);
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

    #[test]
    fn topology_resolution_and_resource_conflicts_use_state_aware_machine_codes() {
        assert_eq!(
            StackError::from(TopologyResolutionError::NotFound {
                kind: "environment".to_string(),
                selector: "env_missing".to_string(),
            })
            .machine_code(),
            MachineErrorCode::NotFound
        );
        assert_eq!(
            StackError::from(TopologyResolutionError::selection_required(
                "environment",
                "workspace",
                [],
            ))
            .machine_code(),
            MachineErrorCode::StateConflict
        );
        assert_eq!(
            StackError::from(TopologyResolutionError::InvalidSelector {
                kind: "environment".to_string(),
                selector: "".to_string(),
                reason: "selector is blank".to_string(),
            })
            .machine_code(),
            MachineErrorCode::ValidationError
        );
        assert_eq!(
            StackError::OwnedResourceCollision(Box::new(OwnedResourceCollisionError {
                resource_kind: "disk".to_string(),
                resource_id: "disk-1".to_string(),
                existing_environment_id: "env_owner".to_string(),
                existing_machine_id: None,
            }))
            .machine_code(),
            MachineErrorCode::StateConflict
        );
    }
}
