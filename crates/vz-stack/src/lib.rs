//! Stack control plane for multi-service OCI workloads.
//!
//! Provides a typed [`StackSpec`] model, a durable SQLite-backed
//! [`StateStore`], and an [`apply`] entrypoint that persists desired
//! and observed state for idempotent reconciliation.

#![forbid(unsafe_code)]

mod error;
mod events;
mod network;
mod reconcile;
mod spec;
mod state_store;
mod volume;

pub use error::StackError;
pub use events::{EventRecord, StackEvent};
pub use network::{
    GvproxyBackend, GvproxyConfig, NetworkBackend, NetworkHandle, PortConflict, PublishedPort,
    detect_port_conflicts, locate_gvproxy, ports_changed, resolve_ports,
};
pub use reconcile::{Action, ApplyResult, apply};
pub use spec::{
    HealthCheckSpec, MountSpec, NetworkSpec, PortSpec, ResourcesSpec, RestartPolicy, ServiceSpec,
    StackSpec, VolumeSpec,
};
pub use state_store::{ServiceObservedState, ServicePhase, StateStore};
pub use volume::{
    ResolvedMount, ResolvedMountKind, mounts_changed, orphaned_volumes, referenced_volume_names,
    resolve_mounts,
};
