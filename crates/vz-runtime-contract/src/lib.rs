//! Backend-neutral runtime contract for vz container backends.
//!
//! This crate defines the [`RuntimeBackend`] trait and shared types that
//! both the macOS (Virtualization.framework) and Linux-native backends
//! implement. Callers depend only on this contract, making the backend
//! selection transparent.

pub mod error;
pub mod selection;
pub mod types;

mod backend;
mod checkpoint;
mod conformance;
mod metadata;

#[cfg(test)]
mod tests;

pub use backend::{RuntimeBackend, WorkspaceRuntimeManager};
pub use checkpoint::{
    ensure_checkpoint_class_supported, validate_checkpoint_restore_compatibility,
};
pub use conformance::{
    BackendCapabilityMatrix, DockerShimCommand, OpenApiPrimitiveSurface,
    PRIMITIVE_CONFORMANCE_MATRIX, PrimitiveConformanceEntry, REQUIRED_BACKEND_ADAPTER_OPERATIONS,
    REQUIRED_IDEMPOTENT_MUTATIONS, REQUIRED_RUNTIME_OPERATIONS, backend_capability_matrix,
    canonical_backend_capabilities, transport_metadata_for_sequence,
    validate_backend_adapter_contract_surface, validate_backend_adapter_parity,
};
pub use error::{MachineErrorCode, RuntimeError};
pub use metadata::{
    MachineError, MachineErrorDetails, MachineErrorEnvelope, PolicyDecision, RequestMetadata,
    RuntimeExtensionFailureKind, RuntimeExtensionPoint, RuntimePassthroughMetadata,
    RuntimePolicyHook, enforce_runtime_policy_hook, map_runtime_extension_failure,
    normalize_passthrough_metadata, runtime_error_machine_envelope, runtime_error_machine_error,
    validate_request_metadata_for_operation,
};
pub use selection::{HostBackend, ResolvedBackend};
pub use types::{
    Build, BuildSpec, BuildState, Capability, Checkpoint, CheckpointClass, CheckpointClassMetadata,
    CheckpointCompatibilityMetadata, CheckpointLineageStore, CheckpointMetadata, CheckpointState,
    Container, ContainerInfo, ContainerLogs, ContainerMount, ContainerResources, ContainerSpec,
    ContainerState, ContainerStatus, ContractInvariantError, Event, EventRange, EventScope,
    ExecConfig, ExecOutput, Execution, ExecutionSpec, ExecutionState, Image, ImageInfo,
    IsolationLevel, Lease, LeaseState, MountAccess, MountSpec, MountType, NamespaceConfig,
    NetworkDomain, NetworkDomainState, NetworkServiceConfig, PortMapping, PortProtocol,
    PruneResult, PublishedPort, Receipt, ReceiptResultClassification, RunConfig,
    RuntimeCapabilities, RuntimeOperation, SANDBOX_LABEL_BASE_IMAGE_DEFAULT_SOURCE,
    SANDBOX_LABEL_BASE_IMAGE_REF, SANDBOX_LABEL_MAIN_CONTAINER,
    SANDBOX_LABEL_MAIN_CONTAINER_DEFAULT_SOURCE, SANDBOX_LABEL_PROJECT_DIR,
    SANDBOX_LABEL_SPACE_CONFIG_PATH, SANDBOX_LABEL_SPACE_LIFECYCLE, SANDBOX_LABEL_SPACE_MODE,
    SANDBOX_LABEL_SPACE_SECRET_ENV_PREFIX, SANDBOX_SPACE_LIFECYCLE_EPHEMERAL,
    SANDBOX_SPACE_LIFECYCLE_PERSISTENT, SANDBOX_SPACE_MODE_REQUIRED, Sandbox, SandboxBackend,
    SandboxSpec, SandboxState, SandboxVolumeMount, SharedVmPhase, SharedVmPhaseTracker,
    StackResourceHint, StackVolumeMount, Volume, VolumeType, default_namespace_config,
};
