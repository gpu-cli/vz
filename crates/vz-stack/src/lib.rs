//! Stack control plane for multi-service OCI workloads.
//!
//! Provides a typed [`StackSpec`] model, a durable SQLite-backed
//! [`StateStore`], and an [`apply`] entrypoint that persists desired
//! and observed state for idempotent reconciliation.

#![forbid(unsafe_code)]

mod compose;
mod convert;
mod error;
mod events;
mod executor;
mod health;
mod network;
mod orchestrator;
mod reconcile;
mod restart;
mod spec;
mod state_store;
mod volume;

pub use compose::{expand_variables, parse_compose, parse_compose_with_dir, parse_env_file_content};
pub use convert::service_to_run_config;
pub use error::StackError;
pub use events::{EventRecord, StackEvent};
pub use executor::{ContainerLogs, ContainerRuntime, ExecutionResult, PortTracker, StackExecutor};
pub use health::{
    DependencyCheck, HealthPollResult, HealthPoller, HealthStatus, check_dependencies,
    is_service_ready,
};
pub use network::{
    GvproxyBackend, GvproxyConfig, NetworkBackend, NetworkHandle, PortConflict, PublishedPort,
    detect_port_conflicts, locate_gvproxy, ports_changed, resolve_ports,
};
pub use orchestrator::{OrchestrationConfig, OrchestrationResult, RoundReport, StackOrchestrator};
pub use reconcile::{Action, ApplyResult, DeferredService, apply};
pub use restart::{RestartTracker, compute_restarts};
pub use spec::{
    HealthCheckSpec, MountSpec, NetworkSpec, PortSpec, ResourcesSpec, RestartPolicy, ServiceSpec,
    StackSpec, VolumeSpec,
};
pub use state_store::{ServiceObservedState, ServicePhase, StateStore};
pub use volume::{
    ResolvedMount, ResolvedMountKind, VolumeManager, mounts_changed, orphaned_volumes,
    referenced_volume_names, resolve_mounts,
};
