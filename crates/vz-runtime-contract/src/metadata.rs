use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{MachineErrorCode, RuntimeError, RuntimeOperation};

/// Opaque metadata key/value pairs passed through runtime integrations.
pub type RuntimePassthroughMetadata = BTreeMap<String, String>;

/// Generic runtime extension points that may be provided by integrators.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeExtensionPoint {
    PolicyHook,
    EventSink,
    MetadataPassthrough,
}

impl RuntimeExtensionPoint {
    pub const ALL: [RuntimeExtensionPoint; 3] = [
        RuntimeExtensionPoint::PolicyHook,
        RuntimeExtensionPoint::EventSink,
        RuntimeExtensionPoint::MetadataPassthrough,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            RuntimeExtensionPoint::PolicyHook => "policy_hook",
            RuntimeExtensionPoint::EventSink => "event_sink",
            RuntimeExtensionPoint::MetadataPassthrough => "metadata_passthrough",
        }
    }
}

/// Stable extension failure classes mapped into runtime errors.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeExtensionFailureKind {
    PolicyDenied,
    Transport,
    InvalidMetadata,
}

impl RuntimeExtensionFailureKind {
    pub const ALL: [RuntimeExtensionFailureKind; 3] = [
        RuntimeExtensionFailureKind::PolicyDenied,
        RuntimeExtensionFailureKind::Transport,
        RuntimeExtensionFailureKind::InvalidMetadata,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            RuntimeExtensionFailureKind::PolicyDenied => "policy_denied",
            RuntimeExtensionFailureKind::Transport => "transport",
            RuntimeExtensionFailureKind::InvalidMetadata => "invalid_metadata",
        }
    }
}

/// Map extension failures into stable runtime taxonomy variants.
pub fn map_runtime_extension_failure(
    extension: RuntimeExtensionPoint,
    operation: &str,
    kind: RuntimeExtensionFailureKind,
    reason: impl Into<String>,
) -> RuntimeError {
    let operation = {
        let trimmed = operation.trim();
        if trimmed.is_empty() {
            "unknown_operation".to_string()
        } else {
            trimmed.to_string()
        }
    };
    let reason = normalize_required_reason(reason.into());
    let extension_name = extension.as_str();

    match kind {
        RuntimeExtensionFailureKind::PolicyDenied => RuntimeError::PolicyDenied {
            operation,
            reason: format!("extension={extension_name}; reason={reason}"),
        },
        RuntimeExtensionFailureKind::Transport => RuntimeError::Io(std::io::Error::other(format!(
            "extension_failure: extension={extension_name}; operation={operation}; kind={}; reason={reason}",
            kind.as_str()
        ))),
        RuntimeExtensionFailureKind::InvalidMetadata => RuntimeError::InvalidConfig(format!(
            "extension_failure: extension={extension_name}; operation={operation}; kind={}; reason={reason}",
            kind.as_str()
        )),
    }
}

/// Policy hook decision for an operation preflight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow,
    Deny { reason: String },
}

/// Generic policy extension hook for runtime operations.
pub trait RuntimePolicyHook: Send + Sync {
    fn evaluate(
        &self,
        operation: RuntimeOperation,
        metadata: &RequestMetadata,
    ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>>;
}

/// Enforce a policy hook decision with stable error taxonomy mapping.
pub fn enforce_runtime_policy_hook(
    hook: &dyn RuntimePolicyHook,
    operation: RuntimeOperation,
    metadata: &RequestMetadata,
) -> Result<(), RuntimeError> {
    match hook.evaluate(operation, metadata) {
        Ok(PolicyDecision::Allow) => Ok(()),
        Ok(PolicyDecision::Deny { reason }) => Err(map_runtime_extension_failure(
            RuntimeExtensionPoint::PolicyHook,
            operation.as_str(),
            RuntimeExtensionFailureKind::PolicyDenied,
            reason,
        )),
        Err(error) => Err(map_runtime_extension_failure(
            RuntimeExtensionPoint::PolicyHook,
            operation.as_str(),
            RuntimeExtensionFailureKind::Transport,
            error.to_string(),
        )),
    }
}

/// Structured request metadata propagated across transports.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestMetadata {
    /// Transport-stable request identifier (for logs/tracing/client correlation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Stable idempotency key for mutation retries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Optional trace identifier for cross-system event correlation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    /// Opaque metadata labels propagated to extensions.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub passthrough: RuntimePassthroughMetadata,
}

impl RequestMetadata {
    /// Build metadata while normalizing empty fields to `None`.
    pub fn new(request_id: Option<String>, idempotency_key: Option<String>) -> Self {
        Self {
            request_id: normalize_optional_metadata_field(request_id),
            idempotency_key: normalize_optional_metadata_field(idempotency_key),
            trace_id: None,
            passthrough: BTreeMap::new(),
        }
    }

    /// Build metadata from optional borrowed values.
    pub fn from_optional_refs(request_id: Option<&str>, idempotency_key: Option<&str>) -> Self {
        Self::new(
            request_id.map(ToOwned::to_owned),
            idempotency_key.map(ToOwned::to_owned),
        )
    }

    /// Attach optional trace identifier metadata.
    pub fn with_trace_id(mut self, trace_id: Option<String>) -> Self {
        self.trace_id = normalize_optional_metadata_field(trace_id);
        self
    }

    /// Attach passthrough metadata with normalization and validation.
    pub fn with_passthrough(
        mut self,
        operation: RuntimeOperation,
        passthrough: RuntimePassthroughMetadata,
    ) -> Result<Self, RuntimeError> {
        self.passthrough = normalize_passthrough_metadata(operation, passthrough)?;
        Ok(self)
    }
}

fn normalize_optional_metadata_field(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn normalize_required_reason(reason: String) -> String {
    let trimmed = reason.trim();
    if trimmed.is_empty() {
        "unspecified extension failure".to_string()
    } else {
        trimmed.to_string()
    }
}

fn normalize_required_metadata_field(
    value: String,
    extension: RuntimeExtensionPoint,
    operation: RuntimeOperation,
    field_name: &str,
) -> Result<String, RuntimeError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(map_runtime_extension_failure(
            extension,
            operation.as_str(),
            RuntimeExtensionFailureKind::InvalidMetadata,
            format!("{field_name} cannot be empty"),
        ));
    }

    Ok(trimmed.to_string())
}

fn validate_passthrough_key(
    operation: RuntimeOperation,
    key: &str,
) -> Result<String, RuntimeError> {
    let normalized = normalize_required_metadata_field(
        key.to_string(),
        RuntimeExtensionPoint::MetadataPassthrough,
        operation,
        "passthrough key",
    )?;
    if normalized.starts_with("vz.") {
        return Err(map_runtime_extension_failure(
            RuntimeExtensionPoint::MetadataPassthrough,
            operation.as_str(),
            RuntimeExtensionFailureKind::InvalidMetadata,
            format!("passthrough key `{normalized}` uses reserved `vz.` prefix"),
        ));
    }
    Ok(normalized)
}

/// Normalize and validate passthrough metadata fields for an operation.
pub fn normalize_passthrough_metadata(
    operation: RuntimeOperation,
    passthrough: RuntimePassthroughMetadata,
) -> Result<RuntimePassthroughMetadata, RuntimeError> {
    let mut normalized = BTreeMap::new();
    for (key, value) in passthrough {
        let key = validate_passthrough_key(operation, &key)?;
        let value = value.trim().to_string();
        normalized.insert(key, value);
    }
    Ok(normalized)
}

/// Validate request metadata requirements for a runtime operation.
pub fn validate_request_metadata_for_operation(
    operation: RuntimeOperation,
    metadata: &RequestMetadata,
) -> Result<(), RuntimeError> {
    if operation.requires_idempotency_key() && metadata.idempotency_key.is_none() {
        return Err(RuntimeError::InvalidConfig(format!(
            "operation `{}` requires idempotency_key metadata",
            operation.as_str()
        )));
    }
    if metadata
        .trace_id
        .as_ref()
        .is_some_and(|trace| trace.trim().is_empty())
    {
        return Err(map_runtime_extension_failure(
            RuntimeExtensionPoint::MetadataPassthrough,
            operation.as_str(),
            RuntimeExtensionFailureKind::InvalidMetadata,
            "trace_id cannot be empty",
        ));
    }
    for key in metadata.passthrough.keys() {
        let _ = validate_passthrough_key(operation, key)?;
    }

    Ok(())
}

/// Structured machine-error detail map used across transports.
pub type MachineErrorDetails = BTreeMap<String, String>;

/// Stable machine-error payload emitted by all transports.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MachineError {
    /// Stable machine-readable code.
    pub code: MachineErrorCode,
    /// Human-readable diagnostic message.
    pub message: String,
    /// Correlated request identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Optional structured details.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub details: MachineErrorDetails,
}

impl MachineError {
    /// Build a machine-error payload.
    pub fn new(
        code: MachineErrorCode,
        message: String,
        request_id: Option<String>,
        details: MachineErrorDetails,
    ) -> Self {
        Self {
            code,
            message,
            request_id: normalize_optional_metadata_field(request_id),
            details,
        }
    }
}

/// Transport-stable machine-error response envelope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MachineErrorEnvelope {
    /// Wrapped machine error.
    pub error: MachineError,
}

impl MachineErrorEnvelope {
    /// Build an envelope from a machine-error payload.
    pub fn new(error: MachineError) -> Self {
        Self { error }
    }
}

fn runtime_error_details(error: &RuntimeError) -> MachineErrorDetails {
    let mut details = BTreeMap::new();
    match error {
        RuntimeError::InvalidConfig(reason) => {
            details.insert("reason".to_string(), reason.clone());
        }
        RuntimeError::ContainerNotFound { id } => {
            details.insert("container_id".to_string(), id.clone());
        }
        RuntimeError::ImageNotFound { reference } => {
            details.insert("image_reference".to_string(), reference.clone());
        }
        RuntimeError::PullFailed { reference, reason } => {
            details.insert("image_reference".to_string(), reference.clone());
            details.insert("reason".to_string(), reason.clone());
        }
        RuntimeError::ContainerFailed { id, reason } | RuntimeError::ExecFailed { id, reason } => {
            details.insert("container_id".to_string(), id.clone());
            details.insert("reason".to_string(), reason.clone());
        }
        RuntimeError::UnsupportedOperation { operation, reason } => {
            details.insert("operation".to_string(), operation.clone());
            details.insert("reason".to_string(), reason.clone());
        }
        RuntimeError::PolicyDenied { operation, reason } => {
            details.insert("operation".to_string(), operation.clone());
            details.insert("reason".to_string(), reason.clone());
        }
        RuntimeError::InvalidRootfs { path } => {
            details.insert("path".to_string(), path.display().to_string());
        }
        RuntimeError::Io(error) => {
            details.insert("reason".to_string(), error.to_string());
        }
        RuntimeError::Backend { message, source } => {
            details.insert("message".to_string(), message.clone());
            details.insert("source".to_string(), source.to_string());
        }
    }
    details
}

/// Convert a runtime error into a transport-stable machine-error payload.
pub fn runtime_error_machine_error(
    error: &RuntimeError,
    metadata: &RequestMetadata,
) -> MachineError {
    MachineError::new(
        error.machine_code(),
        error.to_string(),
        metadata.request_id.clone(),
        runtime_error_details(error),
    )
}

/// Convert a runtime error into a transport-stable error envelope.
pub fn runtime_error_machine_envelope(
    error: &RuntimeError,
    metadata: &RequestMetadata,
) -> MachineErrorEnvelope {
    MachineErrorEnvelope::new(runtime_error_machine_error(error, metadata))
}
