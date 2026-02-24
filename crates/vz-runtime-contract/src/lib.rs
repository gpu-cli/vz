//! Backend-neutral runtime contract for vz container backends.
//!
//! This crate defines the [`RuntimeBackend`] trait and shared types that
//! both the macOS (Virtualization.framework) and Linux-native backends
//! implement. Callers depend only on this contract, making the backend
//! selection transparent.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub mod error;
pub mod selection;
pub mod types;

pub use error::{MachineErrorCode, RuntimeError};
pub use selection::{HostBackend, ResolvedBackend};
pub use types::{
    Build, BuildSpec, BuildState, Capability, Checkpoint, CheckpointClass, CheckpointClassMetadata,
    CheckpointCompatibilityMetadata, CheckpointLineageStore, CheckpointMetadata, CheckpointState,
    Container, ContainerInfo, ContainerLogs, ContainerMount, ContainerResources, ContainerSpec,
    ContainerState, ContainerStatus, ContractInvariantError, Event, EventRange, EventScope,
    ExecConfig, ExecOutput, Execution, ExecutionSpec, ExecutionState, Image, ImageInfo, Lease,
    LeaseState, MountAccess, MountSpec, MountType, NetworkDomain, NetworkDomainState,
    NetworkServiceConfig, PortMapping, PortProtocol, PruneResult, PublishedPort, Receipt,
    ReceiptResultClassification, RunConfig, RuntimeCapabilities, RuntimeOperation, Sandbox,
    SandboxBackend, SandboxSpec, SandboxState, SandboxVolumeMount, SharedVmPhase,
    SharedVmPhaseTracker, StackResourceHint, StackVolumeMount, Volume, VolumeType,
};

/// Canonical Runtime V2 operation surface expected from implementations.
pub const REQUIRED_RUNTIME_OPERATIONS: &[RuntimeOperation] = &RuntimeOperation::ALL;

/// Required idempotent mutation paths and their canonical operation names.
pub const REQUIRED_IDEMPOTENT_MUTATIONS: &[RuntimeOperation] = &[
    RuntimeOperation::CreateSandbox,
    RuntimeOperation::OpenLease,
    RuntimeOperation::PullImage,
    RuntimeOperation::StartBuild,
    RuntimeOperation::CreateContainer,
    RuntimeOperation::ExecContainer,
    RuntimeOperation::CreateCheckpoint,
    RuntimeOperation::ForkCheckpoint,
];

/// Docker-compat command set supported by the Runtime V2 translation shim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DockerShimCommand {
    Run,
    Exec,
    Ps,
    Logs,
    Pull,
    Build,
    Stop,
    Rm,
}

impl DockerShimCommand {
    /// V1 command coverage set in canonical order.
    pub const V1_ALL: [DockerShimCommand; 8] = [
        DockerShimCommand::Run,
        DockerShimCommand::Exec,
        DockerShimCommand::Ps,
        DockerShimCommand::Logs,
        DockerShimCommand::Pull,
        DockerShimCommand::Build,
        DockerShimCommand::Stop,
        DockerShimCommand::Rm,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            DockerShimCommand::Run => "run",
            DockerShimCommand::Exec => "exec",
            DockerShimCommand::Ps => "ps",
            DockerShimCommand::Logs => "logs",
            DockerShimCommand::Pull => "pull",
            DockerShimCommand::Build => "build",
            DockerShimCommand::Stop => "stop",
            DockerShimCommand::Rm => "rm",
        }
    }

    /// Canonical Runtime V2 operation mapped from this shim command.
    ///
    /// `None` indicates a read-only shim command handled via backend listing
    /// and not yet represented by a dedicated Runtime V2 operation enum variant.
    pub const fn runtime_operation(self) -> Option<RuntimeOperation> {
        match self {
            DockerShimCommand::Run => Some(RuntimeOperation::CreateContainer),
            DockerShimCommand::Exec => Some(RuntimeOperation::ExecContainer),
            DockerShimCommand::Ps => None,
            DockerShimCommand::Logs => Some(RuntimeOperation::GetContainerLogs),
            DockerShimCommand::Pull => Some(RuntimeOperation::PullImage),
            DockerShimCommand::Build => Some(RuntimeOperation::StartBuild),
            DockerShimCommand::Stop => Some(RuntimeOperation::StopContainer),
            DockerShimCommand::Rm => Some(RuntimeOperation::RemoveContainer),
        }
    }
}

/// Runtime operations every backend adapter must preserve with shared semantics.
///
/// This is the backend-facing subset of [`REQUIRED_RUNTIME_OPERATIONS`].
pub const REQUIRED_BACKEND_ADAPTER_OPERATIONS: &[RuntimeOperation] = &[
    RuntimeOperation::CreateSandbox,
    RuntimeOperation::TerminateSandbox,
    RuntimeOperation::CreateContainer,
    RuntimeOperation::StartContainer,
    RuntimeOperation::StopContainer,
    RuntimeOperation::RemoveContainer,
    RuntimeOperation::GetContainerLogs,
    RuntimeOperation::ExecContainer,
    RuntimeOperation::WriteExecStdin,
    RuntimeOperation::SignalExec,
    RuntimeOperation::ResizeExecPty,
    RuntimeOperation::CancelExec,
    RuntimeOperation::CreateCheckpoint,
    RuntimeOperation::RestoreCheckpoint,
    RuntimeOperation::ForkCheckpoint,
    RuntimeOperation::AttachVolume,
    RuntimeOperation::DetachVolume,
    RuntimeOperation::CreateNetworkDomain,
    RuntimeOperation::ConnectContainer,
    RuntimeOperation::PublishPort,
    RuntimeOperation::GetCapabilities,
];

/// Canonical capability matrix fields that may vary across backends.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackendCapabilityMatrix {
    pub fs_quick_checkpoint: bool,
    pub vm_full_checkpoint: bool,
    pub checkpoint_fork: bool,
    pub docker_compat: bool,
    pub compose_adapter: bool,
    pub gpu_passthrough: bool,
    pub live_resize: bool,
}

impl BackendCapabilityMatrix {
    /// Stable field names exposed by the Runtime V2 backend capability matrix.
    pub const FIELD_NAMES: [&'static str; 7] = [
        "fs_quick_checkpoint",
        "vm_full_checkpoint",
        "checkpoint_fork",
        "docker_compat",
        "compose_adapter",
        "gpu_passthrough",
        "live_resize",
    ];

    pub const fn from_runtime_capabilities(capabilities: RuntimeCapabilities) -> Self {
        Self {
            fs_quick_checkpoint: capabilities.fs_quick_checkpoint,
            vm_full_checkpoint: capabilities.vm_full_checkpoint,
            checkpoint_fork: capabilities.checkpoint_fork,
            docker_compat: capabilities.docker_compat,
            compose_adapter: capabilities.compose_adapter,
            gpu_passthrough: capabilities.gpu_passthrough,
            live_resize: capabilities.live_resize,
        }
    }
}

/// Project backend capabilities into the canonical backend matrix shape.
pub const fn backend_capability_matrix(
    capabilities: RuntimeCapabilities,
) -> BackendCapabilityMatrix {
    BackendCapabilityMatrix::from_runtime_capabilities(capabilities)
}

/// Canonical Runtime V2 capability surface for first-party backend adapters.
pub fn canonical_backend_capabilities(backend: &SandboxBackend) -> RuntimeCapabilities {
    let mut capabilities = RuntimeCapabilities::stack_baseline();
    match backend {
        SandboxBackend::MacosVz | SandboxBackend::LinuxFirecracker => {
            capabilities.fs_quick_checkpoint = true;
            capabilities.vm_full_checkpoint = false;
            capabilities.checkpoint_fork = true;
        }
        SandboxBackend::Other(_) => {}
    }
    capabilities
}

/// Validate backend adapter operation parity rules that are independent of capabilities.
pub fn validate_backend_adapter_contract_surface() -> Result<(), RuntimeError> {
    for operation in REQUIRED_BACKEND_ADAPTER_OPERATIONS {
        if operation.requires_idempotency_key() && operation.idempotency_key_prefix().is_none() {
            return Err(RuntimeError::InvalidConfig(format!(
                "backend adapter operation `{}` requires idempotency key metadata",
                operation.as_str()
            )));
        }
    }

    Ok(())
}

/// Validate backend adapter capability parity requirements shared across runtimes.
pub fn validate_backend_adapter_parity(
    capabilities: RuntimeCapabilities,
) -> Result<(), RuntimeError> {
    let matrix = backend_capability_matrix(capabilities);
    if !matrix.fs_quick_checkpoint {
        return Err(RuntimeError::UnsupportedOperation {
            operation: RuntimeOperation::CreateCheckpoint.as_str().to_string(),
            reason: "backend parity requires fs_quick_checkpoint baseline".to_string(),
        });
    }
    if !matrix.checkpoint_fork {
        return Err(RuntimeError::UnsupportedOperation {
            operation: RuntimeOperation::ForkCheckpoint.as_str().to_string(),
            reason: "backend parity requires checkpoint_fork baseline".to_string(),
        });
    }
    if !capabilities.shared_vm {
        return Err(RuntimeError::UnsupportedOperation {
            operation: RuntimeOperation::CreateContainer.as_str().to_string(),
            reason: "backend parity requires shared_vm baseline".to_string(),
        });
    }
    if !capabilities.stack_networking {
        return Err(RuntimeError::UnsupportedOperation {
            operation: RuntimeOperation::CreateNetworkDomain.as_str().to_string(),
            reason: "backend parity requires stack_networking baseline".to_string(),
        });
    }
    if !capabilities.container_logs {
        return Err(RuntimeError::UnsupportedOperation {
            operation: RuntimeOperation::GetContainerLogs.as_str().to_string(),
            reason: "backend parity requires container_logs baseline".to_string(),
        });
    }

    Ok(())
}

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

/// Validate checkpoint restore compatibility constraints.
///
/// Returns `RuntimeError::InvalidConfig` with explicit mismatch details when
/// fingerprint or compatibility metadata constraints are violated.
///
/// Returns `RuntimeError::UnsupportedOperation` when checkpoint class semantics
/// would degrade without explicit caller acknowledgement.
pub fn validate_checkpoint_restore_compatibility(
    metadata: &CheckpointMetadata,
    expected_fingerprint: &str,
    expected_compatibility: Option<&CheckpointCompatibilityMetadata>,
    expected_class: CheckpointClass,
    allow_class_degradation: bool,
) -> Result<(), RuntimeError> {
    let actual_fingerprint = metadata.checkpoint.compatibility_fingerprint.as_str();
    if actual_fingerprint != expected_fingerprint {
        return Err(RuntimeError::InvalidConfig(format!(
            "checkpoint {} compatibility fingerprint mismatch: expected `{expected_fingerprint}`, got `{actual_fingerprint}`",
            metadata.checkpoint.checkpoint_id
        )));
    }

    let Some(expected) = expected_compatibility else {
        return Ok(());
    };

    let actual = &metadata.compatibility;
    let mut mismatches = Vec::new();
    if actual.backend_id != expected.backend_id {
        mismatches.push(format!(
            "backend_id expected `{}`, got `{}`",
            expected.backend_id, actual.backend_id
        ));
    }
    if actual.backend_version != expected.backend_version {
        mismatches.push(format!(
            "backend_version expected `{}`, got `{}`",
            expected.backend_version, actual.backend_version
        ));
    }
    if actual.runtime_version != expected.runtime_version {
        mismatches.push(format!(
            "runtime_version expected `{}`, got `{}`",
            expected.runtime_version, actual.runtime_version
        ));
    }
    if actual.config_hash != expected.config_hash {
        mismatches.push(format!(
            "config_hash expected `{}`, got `{}`",
            expected.config_hash, actual.config_hash
        ));
    }
    if actual.guest_artifact_versions != expected.guest_artifact_versions {
        mismatches.push("guest_artifact_versions differ".to_string());
    }
    if actual.host_compatibility_markers != expected.host_compatibility_markers {
        mismatches.push("host_compatibility_markers differ".to_string());
    }

    if mismatches.is_empty() {
        // Continue to class validation below.
    } else {
        return Err(RuntimeError::InvalidConfig(format!(
            "checkpoint {} is incompatible for restore: {}",
            metadata.checkpoint.checkpoint_id,
            mismatches.join("; ")
        )));
    }

    let actual_class = metadata.checkpoint.class;
    if actual_class == expected_class {
        return Ok(());
    }

    let is_degradation = matches!(
        (expected_class, actual_class),
        (CheckpointClass::VmFull, CheckpointClass::FsQuick)
    );
    if is_degradation && allow_class_degradation {
        return Ok(());
    }

    let expected_label = checkpoint_class_label(expected_class);
    let actual_label = checkpoint_class_label(actual_class);
    let reason = if is_degradation {
        format!(
            "checkpoint class degradation for restore_checkpoint: expected `{expected_label}`, got `{actual_label}`; set allow_class_degradation=true to acknowledge fallback"
        )
    } else {
        format!(
            "checkpoint class mismatch for restore_checkpoint: expected `{expected_label}`, got `{actual_label}`"
        )
    };

    Err(RuntimeError::UnsupportedOperation {
        operation: RuntimeOperation::RestoreCheckpoint.as_str().to_string(),
        reason,
    })
}

fn checkpoint_class_label(class: CheckpointClass) -> &'static str {
    match class {
        CheckpointClass::FsQuick => "fs_quick",
        CheckpointClass::VmFull => "vm_full",
    }
}

/// Validate checkpoint-class capability gating for an operation.
pub fn ensure_checkpoint_class_supported(
    capabilities: RuntimeCapabilities,
    class: CheckpointClass,
    operation: RuntimeOperation,
) -> Result<(), RuntimeError> {
    let supported = match class {
        CheckpointClass::FsQuick => capabilities.fs_quick_checkpoint,
        CheckpointClass::VmFull => capabilities.vm_full_checkpoint,
    };
    if supported {
        return Ok(());
    }

    let missing_capability = match class {
        CheckpointClass::FsQuick => "fs_quick_checkpoint",
        CheckpointClass::VmFull => "vm_full_checkpoint",
    };
    Err(RuntimeError::UnsupportedOperation {
        operation: operation.as_str().to_string(),
        reason: format!("missing {missing_capability} capability"),
    })
}

/// Workspace-oriented runtime manager that routes stack operations
/// through backend capabilities with deterministic fallback behavior.
pub struct WorkspaceRuntimeManager<B: RuntimeBackend> {
    backend: B,
}

impl<B: RuntimeBackend> WorkspaceRuntimeManager<B> {
    /// Create a new runtime manager over a concrete backend.
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    /// Access the wrapped backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Consume the manager and return the wrapped backend.
    pub fn into_inner(self) -> B {
        self.backend
    }

    /// Backend name for diagnostics.
    pub fn name(&self) -> &'static str {
        self.backend.name()
    }

    /// Capability snapshot.
    pub fn capabilities(&self) -> RuntimeCapabilities {
        self.backend.capabilities()
    }

    /// Pull an image reference and return resolved image id.
    pub async fn pull_image(&self, image: &str) -> Result<String, RuntimeError> {
        self.backend.pull(image).await
    }

    /// Create a standalone container.
    pub async fn create_container(
        &self,
        image: &str,
        config: RunConfig,
    ) -> Result<String, RuntimeError> {
        self.backend.create_container(image, config).await
    }

    /// Execute command inside a running container.
    pub async fn exec_container(
        &self,
        id: &str,
        config: ExecConfig,
    ) -> Result<ExecOutput, RuntimeError> {
        self.backend.exec_container(id, config).await
    }

    /// Stop a running container.
    pub async fn stop_container(
        &self,
        id: &str,
        force: bool,
        signal: Option<&str>,
        grace_period: Option<std::time::Duration>,
    ) -> Result<ContainerInfo, RuntimeError> {
        self.backend
            .stop_container(id, force, signal, grace_period)
            .await
    }

    /// Remove a container.
    pub async fn remove_container(&self, id: &str) -> Result<(), RuntimeError> {
        self.backend.remove_container(id).await
    }

    /// Fetch persisted container logs if supported by backend.
    pub fn container_logs(&self, container_id: &str) -> Result<ContainerLogs, RuntimeError> {
        self.backend.logs(container_id)
    }

    /// Ensure stack runtime environment is prepared.
    ///
    /// Transitional behavior: when `shared_vm` is unsupported this is a no-op
    /// and stack services fall back to plain container primitives.
    pub async fn ensure_stack_runtime(
        &self,
        stack_id: &str,
        ports: Vec<PortMapping>,
        resources: StackResourceHint,
    ) -> Result<(), RuntimeError> {
        if self.capabilities().shared_vm {
            self.backend
                .boot_shared_vm(stack_id, ports, resources)
                .await?;
        }
        Ok(())
    }

    /// Create a stack service container.
    ///
    /// If shared runtime capability is present, route through backend stack
    /// create path; otherwise fall back to plain `create_container`.
    pub async fn create_stack_container(
        &self,
        stack_id: &str,
        image: &str,
        config: RunConfig,
    ) -> Result<String, RuntimeError> {
        if self.capabilities().shared_vm {
            self.backend
                .create_container_in_stack(stack_id, image, config)
                .await
        } else {
            self.backend.create_container(image, config).await
        }
    }

    /// Configure stack service networking when capability is available.
    pub async fn setup_stack_network(
        &self,
        stack_id: &str,
        services: Vec<NetworkServiceConfig>,
    ) -> Result<(), RuntimeError> {
        let caps = self.capabilities();
        if caps.shared_vm && caps.stack_networking {
            self.backend.network_setup(stack_id, services).await?;
        }
        Ok(())
    }

    /// Tear down stack service networking when capability is available.
    pub async fn teardown_stack_network(
        &self,
        stack_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), RuntimeError> {
        let caps = self.capabilities();
        if caps.shared_vm && caps.stack_networking {
            self.backend
                .network_teardown(stack_id, service_names)
                .await?;
        }
        Ok(())
    }

    /// Shut down stack runtime environment when capability is available.
    pub async fn shutdown_stack_runtime(&self, stack_id: &str) -> Result<(), RuntimeError> {
        if self.capabilities().shared_vm {
            self.backend.shutdown_shared_vm(stack_id).await?;
        }
        Ok(())
    }

    /// Whether stack runtime is currently active.
    pub fn has_stack_runtime(&self, stack_id: &str) -> bool {
        if !self.capabilities().shared_vm {
            return false;
        }
        self.backend.has_shared_vm(stack_id)
    }
}

/// Backend-neutral container runtime trait.
///
/// Each host platform provides an implementation of this trait. The
/// [`vz-oci`] facade holds a backend and delegates lifecycle operations
/// to it, keeping callers (`vz-stack`, `vz-cli`) unaware of the
/// underlying platform.
///
/// # Async Methods
///
/// Lifecycle methods are `async` because they may involve network I/O
/// (image pulls), IPC (guest agent communication on macOS), or
/// process management (OCI runtime invocation on Linux).
pub trait RuntimeBackend: Send + Sync {
    /// Human-readable backend name for diagnostics.
    fn name(&self) -> &'static str;

    /// Capability flags for this backend/runtime implementation.
    ///
    /// Callers must check these flags before invoking capability-gated flows
    /// and return deterministic `unsupported_operation` diagnostics when false.
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities::default()
    }

    // ── Image operations ──────────────────────────────────────────

    /// Pull an image from a registry and return its image ID (digest).
    fn pull(&self, image: &str) -> impl Future<Output = Result<String, RuntimeError>>;

    /// List locally cached images.
    fn images(&self) -> Result<Vec<ImageInfo>, RuntimeError>;

    /// Remove unreferenced images and layers.
    fn prune_images(&self) -> Result<PruneResult, RuntimeError>;

    // ── Container lifecycle ───────────────────────────────────────

    /// Pull image (if needed), run a command, and return output.
    ///
    /// This is the "one-shot" convenience path. Implementations may
    /// create a container, start it, wait for the command to finish,
    /// and clean up.
    fn run(
        &self,
        image: &str,
        config: RunConfig,
    ) -> impl Future<Output = Result<ExecOutput, RuntimeError>>;

    /// Create a container from an image and return its container ID.
    fn create_container(
        &self,
        image: &str,
        config: RunConfig,
    ) -> impl Future<Output = Result<String, RuntimeError>>;

    /// Execute a command in an already-running container.
    fn exec_container(
        &self,
        id: &str,
        config: ExecConfig,
    ) -> impl Future<Output = Result<ExecOutput, RuntimeError>>;

    /// Stop a running container.
    ///
    /// `signal` overrides the default stop signal (SIGTERM).
    /// `grace_period` overrides the default grace period before SIGKILL escalation.
    fn stop_container(
        &self,
        id: &str,
        force: bool,
        signal: Option<&str>,
        grace_period: Option<std::time::Duration>,
    ) -> impl Future<Output = Result<ContainerInfo, RuntimeError>>;

    /// Remove a stopped container and clean up its resources.
    fn remove_container(&self, id: &str) -> impl Future<Output = Result<(), RuntimeError>>;

    /// List all tracked containers.
    fn list_containers(&self) -> Result<Vec<ContainerInfo>, RuntimeError>;

    // ── Stack / multi-container support ───────────────────────────

    /// Boot a shared runtime environment for multi-container stacks.
    ///
    /// On macOS this boots a shared Linux VM. On Linux-native this may
    /// set up a shared network bridge. Returns `Ok(())` if already booted.
    fn boot_shared_vm(
        &self,
        _stack_id: &str,
        _ports: Vec<PortMapping>,
        _resources: StackResourceHint,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async { Ok(()) }
    }

    /// Create a container within a shared stack environment.
    ///
    /// Default implementation delegates to [`create_container`](Self::create_container).
    fn create_container_in_stack(
        &self,
        _stack_id: &str,
        image: &str,
        config: RunConfig,
    ) -> impl Future<Output = Result<String, RuntimeError>> {
        self.create_container(image, config)
    }

    /// Set up per-service networking within a stack.
    fn network_setup(
        &self,
        _stack_id: &str,
        _services: Vec<NetworkServiceConfig>,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async { Ok(()) }
    }

    /// Tear down per-service networking within a stack.
    fn network_teardown(
        &self,
        _stack_id: &str,
        _service_names: Vec<String>,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async { Ok(()) }
    }

    /// Shut down a shared stack runtime environment.
    fn shutdown_shared_vm(
        &self,
        _stack_id: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async { Ok(()) }
    }

    /// Check if a shared stack environment is currently booted.
    fn has_shared_vm(&self, _stack_id: &str) -> bool {
        false
    }

    /// Retrieve captured logs from a container.
    fn logs(&self, _container_id: &str) -> Result<ContainerLogs, RuntimeError> {
        Ok(ContainerLogs::default())
    }

    // ── Build operations ──────────────────────────────────────────

    /// Start an asynchronous build.
    fn start_build(
        &self,
        _sandbox_id: &str,
        _build_spec: BuildSpec,
        _idempotency_key: Option<String>,
    ) -> impl Future<Output = Result<Build, RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::StartBuild.as_str().to_string(),
                reason: "build operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Get build status/details.
    fn get_build(&self, _build_id: &str) -> impl Future<Output = Result<Build, RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::GetBuild.as_str().to_string(),
                reason: "build operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Stream historical build events for a build ID.
    fn stream_build_events(
        &self,
        _build_id: &str,
        _after_event_id: Option<u64>,
    ) -> impl Future<Output = Result<Vec<Event>, RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::StreamBuildEvents.as_str().to_string(),
                reason: "build operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Cancel an in-flight build.
    fn cancel_build(&self, _build_id: &str) -> impl Future<Output = Result<Build, RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::CancelBuild.as_str().to_string(),
                reason: "build operations are not supported by this backend".to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::future::{Future, ready};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Wake, Waker};

    fn unsupported(operation: &str) -> RuntimeError {
        RuntimeError::UnsupportedOperation {
            operation: operation.to_string(),
            reason: "test stub".to_string(),
        }
    }

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn poll_immediate<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let waker = Waker::from(Arc::new(NoopWaker));
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);

        match Future::poll(future.as_mut(), &mut cx) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("future unexpectedly pending"),
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum StubPolicyMode {
        Allow,
        Deny,
        Fail,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("{0}")]
    struct StubPolicyError(&'static str);

    #[derive(Debug, Clone, Copy)]
    struct StubPolicyHook {
        mode: StubPolicyMode,
    }

    impl RuntimePolicyHook for StubPolicyHook {
        fn evaluate(
            &self,
            _operation: RuntimeOperation,
            _metadata: &RequestMetadata,
        ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
            match self.mode {
                StubPolicyMode::Allow => Ok(PolicyDecision::Allow),
                StubPolicyMode::Deny => Ok(PolicyDecision::Deny {
                    reason: "blocked by test policy".to_string(),
                }),
                StubPolicyMode::Fail => Err(Box::new(StubPolicyError("policy backend offline"))),
            }
        }
    }

    #[derive(Debug, Default)]
    struct StubBackend;

    impl RuntimeBackend for StubBackend {
        fn name(&self) -> &'static str {
            "stub"
        }

        fn pull(&self, _image: &str) -> impl Future<Output = Result<String, RuntimeError>> {
            ready(Err(unsupported("pull")))
        }

        fn images(&self) -> Result<Vec<ImageInfo>, RuntimeError> {
            Err(unsupported("images"))
        }

        fn prune_images(&self) -> Result<PruneResult, RuntimeError> {
            Err(unsupported("prune_images"))
        }

        fn run(
            &self,
            _image: &str,
            _config: RunConfig,
        ) -> impl Future<Output = Result<ExecOutput, RuntimeError>> {
            ready(Err(unsupported("run")))
        }

        fn create_container(
            &self,
            _image: &str,
            _config: RunConfig,
        ) -> impl Future<Output = Result<String, RuntimeError>> {
            ready(Err(unsupported("create_container")))
        }

        fn exec_container(
            &self,
            _id: &str,
            _config: ExecConfig,
        ) -> impl Future<Output = Result<ExecOutput, RuntimeError>> {
            ready(Err(unsupported("exec_container")))
        }

        fn stop_container(
            &self,
            _id: &str,
            _force: bool,
            _signal: Option<&str>,
            _grace_period: Option<std::time::Duration>,
        ) -> impl Future<Output = Result<ContainerInfo, RuntimeError>> {
            ready(Err(unsupported("stop_container")))
        }

        fn remove_container(&self, _id: &str) -> impl Future<Output = Result<(), RuntimeError>> {
            ready(Err(unsupported("remove_container")))
        }

        fn list_containers(&self) -> Result<Vec<ContainerInfo>, RuntimeError> {
            Err(unsupported("list_containers"))
        }
    }

    #[derive(Debug)]
    struct ManagerRoutingBackend {
        capabilities: RuntimeCapabilities,
        calls: Mutex<Vec<String>>,
    }

    impl ManagerRoutingBackend {
        fn new(capabilities: RuntimeCapabilities) -> Self {
            Self {
                capabilities,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn record(&self, call: &str) {
            self.calls.lock().unwrap().push(call.to_string());
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl RuntimeBackend for ManagerRoutingBackend {
        fn name(&self) -> &'static str {
            "manager-routing"
        }

        fn capabilities(&self) -> RuntimeCapabilities {
            self.capabilities
        }

        fn pull(&self, _image: &str) -> impl Future<Output = Result<String, RuntimeError>> {
            self.record("pull");
            ready(Ok("sha256:test".to_string()))
        }

        fn images(&self) -> Result<Vec<ImageInfo>, RuntimeError> {
            Ok(Vec::new())
        }

        fn prune_images(&self) -> Result<PruneResult, RuntimeError> {
            Ok(PruneResult {
                removed_refs: 0,
                removed_manifests: 0,
                removed_configs: 0,
                removed_layer_dirs: 0,
            })
        }

        fn run(
            &self,
            _image: &str,
            _config: RunConfig,
        ) -> impl Future<Output = Result<ExecOutput, RuntimeError>> {
            self.record("run");
            ready(Ok(ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            }))
        }

        fn create_container(
            &self,
            _image: &str,
            _config: RunConfig,
        ) -> impl Future<Output = Result<String, RuntimeError>> {
            self.record("create_container");
            ready(Ok("ctr-plain".to_string()))
        }

        fn exec_container(
            &self,
            _id: &str,
            _config: ExecConfig,
        ) -> impl Future<Output = Result<ExecOutput, RuntimeError>> {
            self.record("exec_container");
            ready(Ok(ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            }))
        }

        fn stop_container(
            &self,
            _id: &str,
            _force: bool,
            _signal: Option<&str>,
            _grace_period: Option<std::time::Duration>,
        ) -> impl Future<Output = Result<ContainerInfo, RuntimeError>> {
            self.record("stop_container");
            ready(Err(unsupported("stop_container")))
        }

        fn remove_container(&self, _id: &str) -> impl Future<Output = Result<(), RuntimeError>> {
            self.record("remove_container");
            ready(Ok(()))
        }

        fn list_containers(&self) -> Result<Vec<ContainerInfo>, RuntimeError> {
            Ok(Vec::new())
        }

        fn boot_shared_vm(
            &self,
            _stack_id: &str,
            _ports: Vec<PortMapping>,
            _resources: StackResourceHint,
        ) -> impl Future<Output = Result<(), RuntimeError>> {
            self.record("boot_shared_vm");
            ready(Ok(()))
        }

        fn create_container_in_stack(
            &self,
            _stack_id: &str,
            _image: &str,
            _config: RunConfig,
        ) -> impl Future<Output = Result<String, RuntimeError>> {
            self.record("create_container_in_stack");
            ready(Ok("ctr-stack".to_string()))
        }

        fn network_setup(
            &self,
            _stack_id: &str,
            _services: Vec<NetworkServiceConfig>,
        ) -> impl Future<Output = Result<(), RuntimeError>> {
            self.record("network_setup");
            ready(Ok(()))
        }
    }

    /// Verify the trait is object-safe enough for our usage pattern.
    /// We use `impl RuntimeBackend` (static dispatch) not `dyn RuntimeBackend`,
    /// but this test documents that the types compile correctly.
    #[test]
    fn contract_types_are_constructible() {
        let _run = RunConfig::default();
        let _exec = ExecConfig::default();
        let _output = ExecOutput {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        };
        let _info = ContainerInfo {
            id: "test".to_string(),
            image: "img".to_string(),
            image_id: "sha256:abc".to_string(),
            status: ContainerStatus::Created,
            created_unix_secs: 0,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: None,
        };
        let _img = ImageInfo {
            reference: "ubuntu:latest".to_string(),
            image_id: "sha256:abc".to_string(),
        };
        let _prune = PruneResult {
            removed_refs: 0,
            removed_manifests: 0,
            removed_configs: 0,
            removed_layer_dirs: 0,
        };
        let _port = PortMapping {
            host: 8080,
            container: 80,
            protocol: PortProtocol::Tcp,
            target_host: None,
        };
        let _mount = MountSpec {
            source: None,
            target: std::path::PathBuf::from("/data"),
            mount_type: MountType::Tmpfs,
            access: MountAccess::ReadWrite,
            subpath: None,
        };
        let _net = NetworkServiceConfig {
            name: "web".to_string(),
            addr: "172.20.0.2".to_string(),
            network_name: "default".to_string(),
        };
        let _logs = ContainerLogs::default();
        let _capabilities = RuntimeCapabilities::default();
        let _stack_capabilities = RuntimeCapabilities::stack_baseline();
        let _sandbox_spec = SandboxSpec {
            cpus: Some(2),
            memory_mb: Some(4096),
            network_profile: Some("default".to_string()),
            volume_mounts: vec![SandboxVolumeMount {
                volume_id: "vol-1".to_string(),
                target: "/data".to_string(),
                read_only: false,
            }],
        };
        let _sandbox = Sandbox {
            sandbox_id: "sbx-1".to_string(),
            backend: SandboxBackend::MacosVz,
            spec: _sandbox_spec.clone(),
            state: SandboxState::Ready,
            created_at: 10,
            updated_at: 11,
            labels: BTreeMap::new(),
        };
        let _lease = Lease {
            lease_id: "lease-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            ttl_secs: 60,
            last_heartbeat_at: 20,
            state: LeaseState::Active,
        };
        let _image = Image {
            image_ref: "alpine:latest".to_string(),
            resolved_digest: "sha256:abc".to_string(),
            platform: "linux/amd64".to_string(),
            source_registry: "docker.io".to_string(),
            pulled_at: 30,
        };
        let _build = Build {
            build_id: "b-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            build_spec: BuildSpec {
                context: ".".to_string(),
                dockerfile: Some("Dockerfile".to_string()),
                args: BTreeMap::new(),
            },
            state: BuildState::Queued,
            result_digest: None,
            started_at: 40,
            ended_at: None,
        };
        let _container = Container {
            container_id: "ctr-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            image_digest: "sha256:abc".to_string(),
            container_spec: ContainerSpec {
                cmd: vec!["sleep".to_string(), "1".to_string()],
                env: BTreeMap::new(),
                cwd: None,
                user: None,
                mounts: vec![ContainerMount {
                    volume_id: "vol-1".to_string(),
                    target: "/work".to_string(),
                    access_mode: MountAccess::ReadWrite,
                }],
                resources: ContainerResources::default(),
                network_attachments: vec!["net-1".to_string()],
            },
            state: ContainerState::Created,
            created_at: 50,
            started_at: None,
            ended_at: None,
        };
        let _execution = Execution {
            execution_id: "exec-1".to_string(),
            container_id: "ctr-1".to_string(),
            exec_spec: ExecutionSpec {
                cmd: vec!["echo".to_string()],
                args: vec!["hello".to_string()],
                env_override: BTreeMap::new(),
                pty: false,
                timeout_secs: Some(10),
            },
            state: ExecutionState::Queued,
            exit_code: None,
            started_at: None,
            ended_at: None,
        };
        let _volume = Volume {
            volume_id: "vol-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            volume_type: VolumeType::Named,
            source: "named://vol-1".to_string(),
            target: "/data".to_string(),
            access_mode: MountAccess::ReadWrite,
        };
        let _network = NetworkDomain {
            network_id: "net-1".to_string(),
            sandbox_id: Some("sbx-1".to_string()),
            stack_id: None,
            state: NetworkDomainState::Ready,
            dns_zone: "sandbox.local".to_string(),
            published_ports: vec![PublishedPort {
                host_port: 8080,
                container_port: 80,
                protocol: PortProtocol::Tcp,
            }],
        };
        let _checkpoint = Checkpoint {
            checkpoint_id: "ckpt-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Creating,
            created_at: 60,
            compatibility_fingerprint: "linux-amd64".to_string(),
        };
        let _checkpoint_class_metadata = CheckpointClass::FsQuick.metadata();
        let _checkpoint_compatibility = CheckpointCompatibilityMetadata {
            backend_id: "macos-vz".to_string(),
            backend_version: "0.1.0".to_string(),
            runtime_version: "2".to_string(),
            guest_artifact_versions: BTreeMap::from([("agent".to_string(), "1.2.3".to_string())]),
            config_hash: "sha256:abc".to_string(),
            host_compatibility_markers: BTreeMap::from([(
                "host.os".to_string(),
                "macos-15".to_string(),
            )]),
        };
        let _checkpoint_metadata =
            CheckpointMetadata::new(_checkpoint.clone(), _checkpoint_compatibility);
        let _checkpoint_store = CheckpointLineageStore::default();
        let _event = Event {
            event_id: 1,
            ts: 70,
            scope: EventScope::Sandbox,
            scope_id: "sbx-1".to_string(),
            event_type: "sandbox.ready".to_string(),
            payload: BTreeMap::new(),
            trace_id: Some("trace-1".to_string()),
        };
        let _receipt = Receipt {
            receipt_id: "r-1".to_string(),
            scope: EventScope::Sandbox,
            scope_id: "sbx-1".to_string(),
            request_hash: "req".to_string(),
            policy_hash: None,
            result_classification: ReceiptResultClassification::Success,
            artifacts: vec![],
            resource_summary: BTreeMap::new(),
            event_range: EventRange {
                start_event_id: 1,
                end_event_id: 1,
            },
        };
        let _capability = Capability::ComposeAdapter;
    }

    #[test]
    fn default_build_operations_return_unsupported_operation() {
        let backend = StubBackend;

        let start_error = poll_immediate(backend.start_build(
            "sandbox-1",
            BuildSpec::default(),
            Some("idem-1".to_string()),
        ))
        .unwrap_err();
        let get_error = poll_immediate(backend.get_build("build-1")).unwrap_err();
        let stream_error =
            poll_immediate(backend.stream_build_events("build-1", Some(10))).unwrap_err();
        let cancel_error = poll_immediate(backend.cancel_build("build-1")).unwrap_err();

        for (error, operation) in [
            (start_error, RuntimeOperation::StartBuild.as_str()),
            (get_error, RuntimeOperation::GetBuild.as_str()),
            (stream_error, RuntimeOperation::StreamBuildEvents.as_str()),
            (cancel_error, RuntimeOperation::CancelBuild.as_str()),
        ] {
            match error {
                RuntimeError::UnsupportedOperation { operation: got, .. } => {
                    assert_eq!(got, operation);
                }
                other => panic!("expected unsupported operation error, got: {other:?}"),
            }
        }
    }

    #[test]
    fn workspace_runtime_manager_routes_stack_create_with_shared_runtime() {
        let backend = ManagerRoutingBackend::new(RuntimeCapabilities::stack_baseline());
        let manager = WorkspaceRuntimeManager::new(backend);

        let created = poll_immediate(manager.create_stack_container(
            "stack-1",
            "nginx:latest",
            RunConfig::default(),
        ))
        .unwrap();

        assert_eq!(created, "ctr-stack");
        assert_eq!(manager.backend().calls(), vec!["create_container_in_stack"]);
    }

    #[test]
    fn workspace_runtime_manager_falls_back_to_plain_create_when_shared_disabled() {
        let mut caps = RuntimeCapabilities::stack_baseline();
        caps.shared_vm = false;
        let backend = ManagerRoutingBackend::new(caps);
        let manager = WorkspaceRuntimeManager::new(backend);

        let created = poll_immediate(manager.create_stack_container(
            "stack-1",
            "nginx:latest",
            RunConfig::default(),
        ))
        .unwrap();

        assert_eq!(created, "ctr-plain");
        assert_eq!(manager.backend().calls(), vec!["create_container"]);
    }

    #[test]
    fn workspace_runtime_manager_skips_network_setup_without_capability() {
        let mut caps = RuntimeCapabilities::stack_baseline();
        caps.stack_networking = false;
        let backend = ManagerRoutingBackend::new(caps);
        let manager = WorkspaceRuntimeManager::new(backend);

        poll_immediate(manager.setup_stack_network("stack-1", Vec::new())).unwrap();

        assert!(manager.backend().calls().is_empty());
    }

    #[test]
    fn checkpoint_lineage_store_enforces_parent_and_duplicates() {
        let mut store = CheckpointLineageStore::default();
        let compatibility = CheckpointCompatibilityMetadata {
            backend_id: "linux-native".to_string(),
            backend_version: "0.1.0".to_string(),
            runtime_version: "2".to_string(),
            guest_artifact_versions: BTreeMap::new(),
            config_hash: "sha256:config".to_string(),
            host_compatibility_markers: BTreeMap::new(),
        };
        assert!(compatibility.is_complete());

        let root = CheckpointMetadata::new(
            Checkpoint {
                checkpoint_id: "ckpt-root".to_string(),
                sandbox_id: "sbx-1".to_string(),
                parent_checkpoint_id: None,
                class: CheckpointClass::FsQuick,
                state: CheckpointState::Ready,
                created_at: 1,
                compatibility_fingerprint: "fingerprint-1".to_string(),
            },
            compatibility.clone(),
        );
        assert_eq!(
            root.class_metadata,
            CheckpointClassMetadata {
                includes_filesystem_state: true,
                includes_memory_state: false,
                includes_cpu_and_device_state: false,
            }
        );
        store.register(root).unwrap();

        let child = CheckpointMetadata::new(
            Checkpoint {
                checkpoint_id: "ckpt-child".to_string(),
                sandbox_id: "sbx-2".to_string(),
                parent_checkpoint_id: Some("ckpt-root".to_string()),
                class: CheckpointClass::VmFull,
                state: CheckpointState::Ready,
                created_at: 2,
                compatibility_fingerprint: "fingerprint-2".to_string(),
            },
            compatibility.clone(),
        );
        store.register(child).unwrap();

        assert_eq!(store.children_of("ckpt-root").len(), 1);
        assert_eq!(store.list_for_sandbox("sbx-2").len(), 1);

        let missing_parent = CheckpointMetadata::new(
            Checkpoint {
                checkpoint_id: "ckpt-missing-parent".to_string(),
                sandbox_id: "sbx-3".to_string(),
                parent_checkpoint_id: Some("does-not-exist".to_string()),
                class: CheckpointClass::FsQuick,
                state: CheckpointState::Creating,
                created_at: 3,
                compatibility_fingerprint: "fingerprint-3".to_string(),
            },
            compatibility.clone(),
        );
        assert!(matches!(
            store.register(missing_parent),
            Err(ContractInvariantError::CheckpointParentNotFound { .. })
        ));

        let duplicate = CheckpointMetadata::new(
            Checkpoint {
                checkpoint_id: "ckpt-root".to_string(),
                sandbox_id: "sbx-1".to_string(),
                parent_checkpoint_id: None,
                class: CheckpointClass::FsQuick,
                state: CheckpointState::Ready,
                created_at: 4,
                compatibility_fingerprint: "fingerprint-4".to_string(),
            },
            compatibility,
        );
        assert!(matches!(
            store.register(duplicate),
            Err(ContractInvariantError::CheckpointAlreadyExists { .. })
        ));
    }

    #[test]
    fn validate_checkpoint_restore_compatibility_accepts_matching_metadata() {
        let compatibility = CheckpointCompatibilityMetadata {
            backend_id: "macos-vz".to_string(),
            backend_version: "0.1.0".to_string(),
            runtime_version: "2".to_string(),
            guest_artifact_versions: BTreeMap::from([(
                "guest-agent".to_string(),
                "1.0.0".to_string(),
            )]),
            config_hash: "sha256:cfg".to_string(),
            host_compatibility_markers: BTreeMap::from([(
                "host.cpu".to_string(),
                "apple-silicon".to_string(),
            )]),
        };
        let metadata = CheckpointMetadata::new(
            Checkpoint {
                checkpoint_id: "ckpt-1".to_string(),
                sandbox_id: "sbx-1".to_string(),
                parent_checkpoint_id: None,
                class: CheckpointClass::FsQuick,
                state: CheckpointState::Ready,
                created_at: 10,
                compatibility_fingerprint: "fp-1".to_string(),
            },
            compatibility.clone(),
        );

        validate_checkpoint_restore_compatibility(
            &metadata,
            "fp-1",
            Some(&compatibility),
            CheckpointClass::FsQuick,
            false,
        )
        .unwrap();
    }

    #[test]
    fn validate_checkpoint_restore_compatibility_rejects_mismatch() {
        let metadata = CheckpointMetadata::new(
            Checkpoint {
                checkpoint_id: "ckpt-2".to_string(),
                sandbox_id: "sbx-1".to_string(),
                parent_checkpoint_id: None,
                class: CheckpointClass::VmFull,
                state: CheckpointState::Ready,
                created_at: 11,
                compatibility_fingerprint: "fp-actual".to_string(),
            },
            CheckpointCompatibilityMetadata {
                backend_id: "linux-native".to_string(),
                backend_version: "0.1.0".to_string(),
                runtime_version: "2".to_string(),
                guest_artifact_versions: BTreeMap::new(),
                config_hash: "sha256:cfg-a".to_string(),
                host_compatibility_markers: BTreeMap::new(),
            },
        );

        let err = validate_checkpoint_restore_compatibility(
            &metadata,
            "fp-expected",
            Some(&CheckpointCompatibilityMetadata {
                backend_id: "macos-vz".to_string(),
                backend_version: "0.1.0".to_string(),
                runtime_version: "2".to_string(),
                guest_artifact_versions: BTreeMap::new(),
                config_hash: "sha256:cfg-b".to_string(),
                host_compatibility_markers: BTreeMap::new(),
            }),
            CheckpointClass::VmFull,
            false,
        )
        .unwrap_err();

        match err {
            RuntimeError::InvalidConfig(message) => {
                assert!(message.contains("compatibility fingerprint mismatch"));
            }
            other => panic!("expected invalid config error, got: {other:?}"),
        }

        let err = validate_checkpoint_restore_compatibility(
            &metadata,
            "fp-actual",
            Some(&CheckpointCompatibilityMetadata {
                backend_id: "macos-vz".to_string(),
                backend_version: "0.1.0".to_string(),
                runtime_version: "2".to_string(),
                guest_artifact_versions: BTreeMap::new(),
                config_hash: "sha256:cfg-b".to_string(),
                host_compatibility_markers: BTreeMap::new(),
            }),
            CheckpointClass::VmFull,
            false,
        )
        .unwrap_err();

        match err {
            RuntimeError::InvalidConfig(message) => {
                assert!(message.contains("incompatible for restore"));
                assert!(message.contains("backend_id"));
                assert!(message.contains("config_hash"));
            }
            other => panic!("expected invalid config error, got: {other:?}"),
        }
    }

    #[test]
    fn validate_checkpoint_restore_compatibility_rejects_class_degradation_without_ack() {
        let metadata = CheckpointMetadata::new(
            Checkpoint {
                checkpoint_id: "ckpt-3".to_string(),
                sandbox_id: "sbx-1".to_string(),
                parent_checkpoint_id: None,
                class: CheckpointClass::FsQuick,
                state: CheckpointState::Ready,
                created_at: 12,
                compatibility_fingerprint: "fp-3".to_string(),
            },
            CheckpointCompatibilityMetadata {
                backend_id: "macos-vz".to_string(),
                backend_version: "0.1.0".to_string(),
                runtime_version: "2".to_string(),
                guest_artifact_versions: BTreeMap::new(),
                config_hash: "sha256:cfg".to_string(),
                host_compatibility_markers: BTreeMap::new(),
            },
        );

        let err = validate_checkpoint_restore_compatibility(
            &metadata,
            "fp-3",
            Some(&metadata.compatibility),
            CheckpointClass::VmFull,
            false,
        )
        .unwrap_err();
        match err {
            RuntimeError::UnsupportedOperation { operation, reason } => {
                assert_eq!(operation, RuntimeOperation::RestoreCheckpoint.as_str());
                assert!(reason.contains("degradation"));
                assert!(reason.contains("allow_class_degradation=true"));
            }
            other => panic!("expected unsupported operation error, got: {other:?}"),
        }
    }

    #[test]
    fn validate_checkpoint_restore_compatibility_allows_class_degradation_with_ack() {
        let metadata = CheckpointMetadata::new(
            Checkpoint {
                checkpoint_id: "ckpt-4".to_string(),
                sandbox_id: "sbx-1".to_string(),
                parent_checkpoint_id: None,
                class: CheckpointClass::FsQuick,
                state: CheckpointState::Ready,
                created_at: 13,
                compatibility_fingerprint: "fp-4".to_string(),
            },
            CheckpointCompatibilityMetadata {
                backend_id: "macos-vz".to_string(),
                backend_version: "0.1.0".to_string(),
                runtime_version: "2".to_string(),
                guest_artifact_versions: BTreeMap::new(),
                config_hash: "sha256:cfg".to_string(),
                host_compatibility_markers: BTreeMap::new(),
            },
        );

        validate_checkpoint_restore_compatibility(
            &metadata,
            "fp-4",
            Some(&metadata.compatibility),
            CheckpointClass::VmFull,
            true,
        )
        .unwrap();
    }

    #[test]
    fn ensure_checkpoint_class_supported_rejects_missing_vm_full_capability() {
        let mut capabilities = RuntimeCapabilities::stack_baseline();
        capabilities.fs_quick_checkpoint = true;
        capabilities.vm_full_checkpoint = false;

        let err = ensure_checkpoint_class_supported(
            capabilities,
            CheckpointClass::VmFull,
            RuntimeOperation::CreateCheckpoint,
        )
        .unwrap_err();

        match err {
            RuntimeError::UnsupportedOperation { operation, reason } => {
                assert_eq!(operation, RuntimeOperation::CreateCheckpoint.as_str());
                assert!(reason.contains("vm_full_checkpoint"));
            }
            other => panic!("expected unsupported operation error, got: {other:?}"),
        }
    }

    #[test]
    fn ensure_checkpoint_class_supported_allows_enabled_class_capability() {
        let mut capabilities = RuntimeCapabilities::stack_baseline();
        capabilities.fs_quick_checkpoint = true;

        ensure_checkpoint_class_supported(
            capabilities,
            CheckpointClass::FsQuick,
            RuntimeOperation::RestoreCheckpoint,
        )
        .unwrap();
    }

    #[test]
    fn lifecycle_consistency_checks() {
        let mut info = ContainerInfo {
            id: "c1".to_string(),
            image: "img".to_string(),
            image_id: "sha256:abc".to_string(),
            status: ContainerStatus::Running,
            created_unix_secs: 0,
            started_unix_secs: Some(1),
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: None,
        };

        assert!(info.ensure_lifecycle_consistency().is_ok());

        info.started_unix_secs = None;
        assert!(matches!(
            info.ensure_lifecycle_consistency(),
            Err(ContractInvariantError::LifecycleInconsistency { .. })
        ));

        info.status = ContainerStatus::Stopped { exit_code: 0 };
        info.created_unix_secs = 2;
        info.started_unix_secs = Some(1);
        info.stopped_unix_secs = Some(3);
        assert!(matches!(
            info.ensure_lifecycle_consistency(),
            Err(ContractInvariantError::LifecycleInconsistency { .. })
        ));
    }

    #[test]
    fn shared_vm_phase_transitions() {
        let mut tracker = SharedVmPhaseTracker::new();
        assert_eq!(tracker.phase(), SharedVmPhase::Shutdown);

        tracker.transition_to(SharedVmPhase::Booting).unwrap();
        tracker.transition_to(SharedVmPhase::Ready).unwrap();
        tracker.transition_to(SharedVmPhase::ShuttingDown).unwrap();
        tracker.transition_to(SharedVmPhase::Shutdown).unwrap();

        assert!(matches!(
            tracker.transition_to(SharedVmPhase::Ready),
            Err(ContractInvariantError::SharedVmPhaseTransition { .. })
        ));
    }

    #[test]
    fn sandbox_and_lease_state_invariants() {
        let mut sandbox = Sandbox {
            sandbox_id: "s-1".to_string(),
            backend: SandboxBackend::LinuxFirecracker,
            spec: SandboxSpec::default(),
            state: SandboxState::Creating,
            created_at: 0,
            updated_at: 0,
            labels: BTreeMap::new(),
        };

        assert!(matches!(
            sandbox.ensure_can_open_lease(),
            Err(ContractInvariantError::LeaseRequiresReadySandbox { .. })
        ));

        sandbox.transition_to(SandboxState::Ready).unwrap();
        sandbox.ensure_can_open_lease().unwrap();
        sandbox.transition_to(SandboxState::Draining).unwrap();
        sandbox.transition_to(SandboxState::Terminated).unwrap();
        assert!(matches!(
            sandbox.transition_to(SandboxState::Ready),
            Err(ContractInvariantError::SandboxStateTransition { .. })
        ));

        let mut lease = Lease {
            lease_id: "l-1".to_string(),
            sandbox_id: "s-1".to_string(),
            ttl_secs: 30,
            last_heartbeat_at: 1,
            state: LeaseState::Opening,
        };
        assert!(matches!(
            lease.ensure_can_submit_work("create_container"),
            Err(ContractInvariantError::WorkRequiresActiveLease { .. })
        ));
        lease.transition_to(LeaseState::Active).unwrap();
        lease.ensure_can_submit_work("create_container").unwrap();
        lease.transition_to(LeaseState::Closed).unwrap();
        assert!(matches!(
            lease.ensure_can_submit_work("create_container"),
            Err(ContractInvariantError::WorkRequiresActiveLease { .. })
        ));
        assert!(matches!(
            lease.transition_to(LeaseState::Active),
            Err(ContractInvariantError::LeaseStateTransition { .. })
        ));
    }

    #[test]
    fn container_and_execution_state_invariants() {
        let mut container = Container {
            container_id: "c-1".to_string(),
            sandbox_id: "s-1".to_string(),
            image_digest: "sha256:abc".to_string(),
            container_spec: ContainerSpec::default(),
            state: ContainerState::Created,
            created_at: 1,
            started_at: None,
            ended_at: None,
        };

        assert!(matches!(
            container.ensure_can_exec(),
            Err(ContractInvariantError::ExecRequiresRunningContainer { .. })
        ));
        container.transition_to(ContainerState::Starting).unwrap();
        container.transition_to(ContainerState::Running).unwrap();
        container.ensure_can_exec().unwrap();
        container.transition_to(ContainerState::Stopping).unwrap();
        container.transition_to(ContainerState::Exited).unwrap();
        assert!(matches!(
            container.ensure_can_exec(),
            Err(ContractInvariantError::ExecRequiresRunningContainer { .. })
        ));
        container.transition_to(ContainerState::Removed).unwrap();
        assert!(matches!(
            container.transition_to(ContainerState::Running),
            Err(ContractInvariantError::ContainerStateTransition { .. })
        ));

        let mut execution = Execution {
            execution_id: "e-1".to_string(),
            container_id: "c-1".to_string(),
            exec_spec: ExecutionSpec::default(),
            state: ExecutionState::Queued,
            exit_code: None,
            started_at: None,
            ended_at: None,
        };
        execution.ensure_lifecycle_consistency().unwrap();
        execution.transition_to(ExecutionState::Running).unwrap();
        execution.started_at = Some(2);
        execution.ensure_lifecycle_consistency().unwrap();
        execution.transition_to(ExecutionState::Exited).unwrap();
        execution.ended_at = Some(3);
        execution.exit_code = Some(0);
        execution.ensure_lifecycle_consistency().unwrap();
        assert!(matches!(
            execution.transition_to(ExecutionState::Running),
            Err(ContractInvariantError::ExecutionStateTransition { .. })
        ));
    }

    #[test]
    fn build_receipt_and_capability_invariants() {
        let mut build = Build {
            build_id: "b-1".to_string(),
            sandbox_id: "s-1".to_string(),
            build_spec: BuildSpec::default(),
            state: BuildState::Queued,
            result_digest: None,
            started_at: 1,
            ended_at: None,
        };
        build.ensure_lifecycle_consistency().unwrap();
        build.transition_to(BuildState::Running).unwrap();
        build.transition_to(BuildState::Succeeded).unwrap();
        build.result_digest = Some("sha256:abcd".to_string());
        build.ended_at = Some(2);
        build.ensure_lifecycle_consistency().unwrap();
        assert!(matches!(
            build.transition_to(BuildState::Running),
            Err(ContractInvariantError::BuildStateTransition { .. })
        ));

        let image = Image {
            image_ref: "alpine:latest".to_string(),
            resolved_digest: "sha256:abcd".to_string(),
            platform: "linux/amd64".to_string(),
            source_registry: "docker.io".to_string(),
            pulled_at: 1,
        };
        image.ensure_digest_immutable().unwrap();

        let bad_image = Image {
            image_ref: "alpine:latest".to_string(),
            resolved_digest: "latest".to_string(),
            platform: "linux/amd64".to_string(),
            source_registry: "docker.io".to_string(),
            pulled_at: 1,
        };
        assert!(matches!(
            bad_image.ensure_digest_immutable(),
            Err(ContractInvariantError::ImageDigestInvariant { .. })
        ));

        let receipt = Receipt {
            receipt_id: "r-1".to_string(),
            scope: EventScope::Sandbox,
            scope_id: "s-1".to_string(),
            request_hash: "req".to_string(),
            policy_hash: None,
            result_classification: ReceiptResultClassification::Success,
            artifacts: vec![],
            resource_summary: BTreeMap::new(),
            event_range: EventRange {
                start_event_id: 10,
                end_event_id: 11,
            },
        };
        receipt.ensure_event_range_ordered().unwrap();

        let bad_receipt = Receipt {
            event_range: EventRange {
                start_event_id: 12,
                end_event_id: 11,
            },
            ..receipt
        };
        assert!(matches!(
            bad_receipt.ensure_event_range_ordered(),
            Err(ContractInvariantError::ReceiptEventRangeInvalid { .. })
        ));

        let list = RuntimeCapabilities::stack_baseline().to_capability_list();
        assert!(list.contains(&Capability::ComposeAdapter));
        assert!(list.contains(&Capability::SharedVm));
        assert!(list.contains(&Capability::StackNetworking));
    }

    #[test]
    fn required_operations_and_idempotency_surface_match_contract() {
        assert_eq!(REQUIRED_RUNTIME_OPERATIONS.len(), 34);
        assert_eq!(
            RuntimeOperation::ALL.len(),
            REQUIRED_RUNTIME_OPERATIONS.len()
        );
        assert_eq!(REQUIRED_IDEMPOTENT_MUTATIONS.len(), 8);

        for operation in REQUIRED_RUNTIME_OPERATIONS {
            assert_eq!(
                operation.requires_idempotency_key(),
                operation.idempotency_key_prefix().is_some()
            );
        }

        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::CreateSandbox));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::OpenLease));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::PullImage));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::StartBuild));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::CreateContainer));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::ExecContainer));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::CreateCheckpoint));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::ForkCheckpoint));

        assert!(!RuntimeOperation::GetReceipt.requires_idempotency_key());
        assert!(!RuntimeOperation::ListEvents.requires_idempotency_key());
        assert_eq!(
            RuntimeOperation::CreateSandbox.idempotency_key_prefix(),
            Some("create_sandbox")
        );
        assert_eq!(
            RuntimeOperation::GetCapabilities.idempotency_key_prefix(),
            None
        );
    }

    #[test]
    fn docker_shim_v1_command_mapping_is_stable() {
        assert_eq!(DockerShimCommand::V1_ALL.len(), 8);
        assert_eq!(DockerShimCommand::Run.as_str(), "run");
        assert_eq!(
            DockerShimCommand::Run.runtime_operation(),
            Some(RuntimeOperation::CreateContainer)
        );
        assert_eq!(
            DockerShimCommand::Exec.runtime_operation(),
            Some(RuntimeOperation::ExecContainer)
        );
        assert_eq!(DockerShimCommand::Ps.runtime_operation(), None);
        assert_eq!(
            DockerShimCommand::Logs.runtime_operation(),
            Some(RuntimeOperation::GetContainerLogs)
        );
        assert_eq!(
            DockerShimCommand::Pull.runtime_operation(),
            Some(RuntimeOperation::PullImage)
        );
        assert_eq!(
            DockerShimCommand::Build.runtime_operation(),
            Some(RuntimeOperation::StartBuild)
        );
        assert_eq!(
            DockerShimCommand::Stop.runtime_operation(),
            Some(RuntimeOperation::StopContainer)
        );
        assert_eq!(
            DockerShimCommand::Rm.runtime_operation(),
            Some(RuntimeOperation::RemoveContainer)
        );
    }

    #[test]
    fn required_backend_adapter_operations_are_subset_of_runtime_surface() {
        assert!(!REQUIRED_BACKEND_ADAPTER_OPERATIONS.is_empty());
        for operation in REQUIRED_BACKEND_ADAPTER_OPERATIONS {
            assert!(REQUIRED_RUNTIME_OPERATIONS.contains(operation));
        }
        assert!(REQUIRED_BACKEND_ADAPTER_OPERATIONS.contains(&RuntimeOperation::CreateSandbox));
        assert!(REQUIRED_BACKEND_ADAPTER_OPERATIONS.contains(&RuntimeOperation::ExecContainer));
        assert!(REQUIRED_BACKEND_ADAPTER_OPERATIONS.contains(&RuntimeOperation::GetCapabilities));
    }

    #[test]
    fn canonical_backend_capabilities_share_same_matrix_shape() {
        let macos = canonical_backend_capabilities(&SandboxBackend::MacosVz);
        let linux = canonical_backend_capabilities(&SandboxBackend::LinuxFirecracker);
        assert_eq!(
            backend_capability_matrix(macos),
            backend_capability_matrix(linux)
        );

        let matrix = backend_capability_matrix(macos);
        assert!(matrix.fs_quick_checkpoint);
        assert!(!matrix.vm_full_checkpoint);
        assert!(matrix.checkpoint_fork);
        assert!(!matrix.docker_compat);
        assert!(matrix.compose_adapter);
        assert!(!matrix.gpu_passthrough);
        assert!(!matrix.live_resize);
        assert_eq!(
            BackendCapabilityMatrix::FIELD_NAMES,
            [
                "fs_quick_checkpoint",
                "vm_full_checkpoint",
                "checkpoint_fork",
                "docker_compat",
                "compose_adapter",
                "gpu_passthrough",
                "live_resize",
            ]
        );
    }

    #[test]
    fn backend_adapter_contract_surface_has_valid_idempotency_mapping() {
        validate_backend_adapter_contract_surface().unwrap();
    }

    #[test]
    fn backend_adapter_parity_validates_required_capability_baseline() {
        let capabilities = canonical_backend_capabilities(&SandboxBackend::MacosVz);
        validate_backend_adapter_parity(capabilities).unwrap();

        let mut missing_checkpoint = capabilities;
        missing_checkpoint.fs_quick_checkpoint = false;
        let err = validate_backend_adapter_parity(missing_checkpoint).unwrap_err();
        match err {
            RuntimeError::UnsupportedOperation { operation, reason } => {
                assert_eq!(operation, RuntimeOperation::CreateCheckpoint.as_str());
                assert!(reason.contains("fs_quick_checkpoint"));
            }
            other => panic!("expected unsupported operation error, got: {other:?}"),
        }

        let mut missing_network = capabilities;
        missing_network.stack_networking = false;
        let err = validate_backend_adapter_parity(missing_network).unwrap_err();
        match err {
            RuntimeError::UnsupportedOperation { operation, reason } => {
                assert_eq!(operation, RuntimeOperation::CreateNetworkDomain.as_str());
                assert!(reason.contains("stack_networking"));
            }
            other => panic!("expected unsupported operation error, got: {other:?}"),
        }
    }

    #[test]
    fn request_metadata_validation_enforces_required_idempotency_keys() {
        let metadata =
            RequestMetadata::from_optional_refs(Some(" req-1 "), Some(" create_container:abc "))
                .with_trace_id(Some(" trace-7 ".to_string()))
                .with_passthrough(
                    RuntimeOperation::CreateContainer,
                    BTreeMap::from([(" customer ".to_string(), " west ".to_string())]),
                )
                .unwrap();
        assert_eq!(metadata.request_id.as_deref(), Some("req-1"));
        assert_eq!(
            metadata.idempotency_key.as_deref(),
            Some("create_container:abc")
        );
        assert_eq!(metadata.trace_id.as_deref(), Some("trace-7"));
        assert_eq!(
            metadata.passthrough.get("customer").map(String::as_str),
            Some("west")
        );

        validate_request_metadata_for_operation(RuntimeOperation::CreateContainer, &metadata)
            .unwrap();
        validate_request_metadata_for_operation(RuntimeOperation::GetReceipt, &metadata).unwrap();

        let missing = RequestMetadata::default();
        let err =
            validate_request_metadata_for_operation(RuntimeOperation::CreateContainer, &missing)
                .unwrap_err();
        assert!(matches!(err, RuntimeError::InvalidConfig(_)));
        assert!(err.to_string().contains("create_container"));
    }

    #[test]
    fn metadata_passthrough_rejects_reserved_keys() {
        let err = RequestMetadata::default()
            .with_passthrough(
                RuntimeOperation::CreateContainer,
                BTreeMap::from([("vz.internal".to_string(), "1".to_string())]),
            )
            .unwrap_err();

        assert!(matches!(err, RuntimeError::InvalidConfig(_)));
        assert!(err.to_string().contains("metadata_passthrough"));
        assert!(err.to_string().contains("reserved `vz.` prefix"));
    }

    #[test]
    fn runtime_extension_failure_mapping_is_stable() {
        let denied = map_runtime_extension_failure(
            RuntimeExtensionPoint::PolicyHook,
            RuntimeOperation::CreateContainer.as_str(),
            RuntimeExtensionFailureKind::PolicyDenied,
            "no quota",
        );
        assert_eq!(denied.machine_code(), MachineErrorCode::PolicyDenied);
        assert!(denied.to_string().contains("extension=policy_hook"));

        let transport = map_runtime_extension_failure(
            RuntimeExtensionPoint::EventSink,
            "stack.emit_event",
            RuntimeExtensionFailureKind::Transport,
            "sink closed",
        );
        assert_eq!(
            transport.machine_code(),
            MachineErrorCode::BackendUnavailable
        );
        assert!(transport.to_string().contains("extension_failure:"));
        assert!(transport.to_string().contains("extension=event_sink"));
        assert!(transport.to_string().contains("operation=stack.emit_event"));

        let invalid = map_runtime_extension_failure(
            RuntimeExtensionPoint::MetadataPassthrough,
            RuntimeOperation::CreateContainer.as_str(),
            RuntimeExtensionFailureKind::InvalidMetadata,
            "key cannot be empty",
        );
        assert_eq!(invalid.machine_code(), MachineErrorCode::ValidationError);
        assert!(invalid.to_string().contains("kind=invalid_metadata"));
    }

    #[test]
    fn runtime_policy_hook_maps_allow_deny_and_transport_errors() {
        let metadata = RequestMetadata::from_optional_refs(Some("req-7"), None);

        let allow_hook = StubPolicyHook {
            mode: StubPolicyMode::Allow,
        };
        enforce_runtime_policy_hook(&allow_hook, RuntimeOperation::CreateContainer, &metadata)
            .unwrap();

        let deny_hook = StubPolicyHook {
            mode: StubPolicyMode::Deny,
        };
        let deny =
            enforce_runtime_policy_hook(&deny_hook, RuntimeOperation::CreateContainer, &metadata)
                .unwrap_err();
        assert_eq!(deny.machine_code(), MachineErrorCode::PolicyDenied);
        assert!(deny.to_string().contains("blocked by test policy"));

        let fail_hook = StubPolicyHook {
            mode: StubPolicyMode::Fail,
        };
        let transport =
            enforce_runtime_policy_hook(&fail_hook, RuntimeOperation::CreateContainer, &metadata)
                .unwrap_err();
        assert_eq!(
            transport.machine_code(),
            MachineErrorCode::BackendUnavailable
        );
        assert!(transport.to_string().contains("operation=create_container"));
    }

    #[test]
    fn runtime_error_machine_envelope_carries_request_id_and_details() {
        let metadata = RequestMetadata::from_optional_refs(Some("req_123"), None);
        let error = RuntimeError::UnsupportedOperation {
            operation: "restore_checkpoint".to_string(),
            reason: "missing vm_full_checkpoint capability".to_string(),
        };

        let envelope = runtime_error_machine_envelope(&error, &metadata);
        assert_eq!(envelope.error.code, MachineErrorCode::UnsupportedOperation);
        assert_eq!(envelope.error.request_id.as_deref(), Some("req_123"));
        assert_eq!(
            envelope.error.details.get("operation").map(String::as_str),
            Some("restore_checkpoint")
        );
        assert_eq!(
            envelope.error.details.get("reason").map(String::as_str),
            Some("missing vm_full_checkpoint capability")
        );
    }

    #[test]
    fn runtime_error_machine_codes_are_stable() {
        assert_eq!(
            MachineErrorCode::ALL.map(MachineErrorCode::as_str),
            [
                "validation_error",
                "not_found",
                "state_conflict",
                "policy_denied",
                "timeout",
                "backend_unavailable",
                "unsupported_operation",
                "internal_error",
            ]
        );

        assert_eq!(
            RuntimeError::InvalidConfig("bad".to_string()).machine_code(),
            MachineErrorCode::ValidationError
        );
        assert_eq!(
            RuntimeError::ContainerNotFound {
                id: "c1".to_string()
            }
            .machine_code(),
            MachineErrorCode::NotFound
        );
        assert_eq!(
            RuntimeError::ContainerFailed {
                id: "c1".to_string(),
                reason: "already stopped".to_string(),
            }
            .machine_code(),
            MachineErrorCode::StateConflict
        );
        assert_eq!(
            RuntimeError::PullFailed {
                reference: "img:latest".to_string(),
                reason: "network timeout".to_string(),
            }
            .machine_code(),
            MachineErrorCode::Timeout
        );
        assert_eq!(
            RuntimeError::UnsupportedOperation {
                operation: "fork_checkpoint".to_string(),
                reason: "missing checkpoint_fork capability".to_string(),
            }
            .machine_code(),
            MachineErrorCode::UnsupportedOperation
        );
        assert_eq!(
            RuntimeError::PolicyDenied {
                operation: "create_container".to_string(),
                reason: "extension=policy_hook; reason=test".to_string(),
            }
            .machine_code(),
            MachineErrorCode::PolicyDenied
        );
        assert_eq!(
            RuntimeError::Backend {
                message: "agent unavailable".to_string(),
                source: Box::new(std::io::Error::other("dial failed")),
            }
            .machine_code(),
            MachineErrorCode::InternalError
        );
    }

    #[test]
    fn runtime_surface_forbids_product_domain_primitives() {
        const FORBIDDEN: [&str; 5] = [
            "identity_provider",
            "memory_provider",
            "tool_gateway",
            "mission",
            "workflow",
        ];

        let mut labels = Vec::new();
        labels.extend(RuntimeOperation::ALL.map(RuntimeOperation::as_str));
        labels.extend(MachineErrorCode::ALL.map(MachineErrorCode::as_str));
        labels.extend(RuntimeExtensionPoint::ALL.map(RuntimeExtensionPoint::as_str));
        labels.extend(DockerShimCommand::V1_ALL.map(DockerShimCommand::as_str));

        for label in labels {
            let normalized = label.to_ascii_lowercase();
            for forbidden in FORBIDDEN {
                assert!(
                    !normalized.contains(forbidden),
                    "runtime surface label `{label}` must not contain forbidden primitive `{forbidden}`"
                );
            }
        }
    }

    #[test]
    fn port_protocol_as_str() {
        assert_eq!(PortProtocol::Tcp.as_str(), "tcp");
        assert_eq!(PortProtocol::Udp.as_str(), "udp");
    }

    #[test]
    fn runtime_error_display() {
        let err = RuntimeError::ContainerNotFound {
            id: "abc".to_string(),
        };
        assert_eq!(err.to_string(), "container not found: abc");

        let err = RuntimeError::PullFailed {
            reference: "ubuntu:latest".to_string(),
            reason: "network timeout".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "pull failed for ubuntu:latest: network timeout"
        );

        let err = RuntimeError::UnsupportedOperation {
            operation: "network_setup".to_string(),
            reason: "missing stack_networking capability".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "unsupported operation `network_setup`: missing stack_networking capability"
        );
    }
}
