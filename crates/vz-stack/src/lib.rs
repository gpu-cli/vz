//! Stack control plane for multi-service OCI workloads.
//!
//! Provides a typed [`StackSpec`] model, a durable SQLite-backed
//! [`StateStore`], and an [`apply`] entrypoint that persists desired
//! and observed state for idempotent reconciliation.

#![forbid(unsafe_code)]

mod error;
mod events;
mod reconcile;
mod spec;
mod state_store;

pub use error::StackError;
pub use events::{EventRecord, StackEvent};
pub use reconcile::{ApplyResult, apply};
pub use spec::{
    HealthCheckSpec, MountSpec, NetworkSpec, PortSpec, ResourcesSpec, RestartPolicy, ServiceSpec,
    StackSpec, VolumeSpec,
};
pub use state_store::{ServiceObservedState, ServicePhase, StateStore};
