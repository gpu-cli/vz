//! Stack control plane for multi-service OCI workloads.
//!
//! Provides a typed [`StackSpec`] model, a durable SQLite-backed
//! [`StateStore`], and an [`apply`] entrypoint that persists desired
//! and observed state for idempotent reconciliation.

#![forbid(unsafe_code)]

#[cfg(test)]
mod chaos_tests;
mod compose;
mod convert;
mod error;
mod events;
mod executor;
mod health;
mod image_policy;
mod network;
mod orchestrator;
mod reconcile;
mod restart;
mod spec;
mod state_store;
mod volume;

pub use compose::{
    ComposeBuildSpec, collect_compose_build_specs, collect_compose_build_specs_with_dir,
    expand_variables, parse_compose, parse_compose_with_dir, parse_env_file_content,
};
pub use convert::service_to_run_config;
pub use error::{OwnedResourceCollisionError, StackError};
pub use events::{
    EventRecord, FnStackEventSink, StackEvent, StackEventSink, StackEventSinkError,
    emit_event_to_sink,
};
pub use executor::{
    ActionOutcomeResult, ContainerLogs, ContainerRuntime, ExecutionResult, IndexedActionOutcome,
    LogLine, LogStream, PortTracker, ReconcileActionKind, StackExecutor,
};
pub use health::{
    DependencyCheck, HealthPollResult, HealthPoller, HealthStatus, check_dependencies,
    is_service_ready,
};
pub use image_policy::{
    ImagePolicy, ImageRefKind, PolicyViolation, ViolationKind, classify_image_ref,
    validate_image_reference, validate_stack_images,
};
pub use network::{
    PortConflict, PublishedPort, detect_port_conflicts, ports_changed, resolve_ports,
};
pub use orchestrator::{OrchestrationConfig, OrchestrationResult, RoundReport, StackOrchestrator};
pub use reconcile::{
    Action, ApplyResult, DeferredService, ExpectedJournalHead, ReplicaPrecondition, apply,
    compute_actions_hash, plan_apply, service_config_digest,
};
pub use restart::{RestartTracker, compute_restart_targets};
pub use spec::{
    DependencyCondition, HealthCheckSpec, LoggingConfig, MountSpec, NetworkSpec, PortSpec,
    ResourcesSpec, RestartPolicy, SecretDef, SecretSource, ServiceDependency, ServiceKind,
    ServiceSecretRef, ServiceSpec, StackSpec, VolumeSpec,
};
pub use state_store::{
    AllocatorSnapshot, CheckpointGcReport, CheckpointRetentionPolicy, CheckpointRetentionState,
    DriftFinding, DriftSeverity, EnvironmentUpReservation, IDEMPOTENCY_TTL_SECS, IdempotencyRecord,
    ImageRecord, Receipt, ReceiptGcReport, ReceiptRetentionPolicy, ReceiptRetentionState,
    ReconcileActionClaim, ReconcileAuditEntry, ReconcileBatchCommit, ReconcileSession,
    ReconcileSessionStatus, RetentionGcReason, ServiceObservedState, ServicePhase,
    ServiceReplicaKey, StackContainerCreateIntent, StackContainerCreateSelector,
    StackContainerCreateStatus, StackContainerGenerationBinding, StackContainerRecoveryDisposition,
    StackContainerRecoveryRecord, StackWorkloadOwner, StateStore, StateStorePragmas,
};
pub use volume::{
    ResolvedMount, ResolvedMountKind, SkippedMount, VolumeManager, mounts_changed,
    orphaned_volumes, referenced_volume_names, resolve_mounts,
};
