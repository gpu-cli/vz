//! Backend-neutral runtime types shared across all container backends.
//!
//! Module ownership and extension rules:
//! - Add new types to the domain module that owns the lifecycle/state machine.
//! - Keep cross-domain references at the boundary type level only.
//! - Preserve stable serde/prost-facing fields when extending existing records.
//! - Keep `ContractInvariantError` as the shared invariant surface across domains.

use std::fmt;

mod build;
mod checkpoint;
mod container_legacy;
mod events;
mod io;
mod isolation;
mod operations;
mod sandbox;
mod shared_vm;
mod space_cache;
mod space_cache_trust;
mod stack;
mod workload;

pub use self::build::*;
pub use self::checkpoint::*;
pub use self::container_legacy::*;
pub use self::events::*;
pub use self::io::*;
pub use self::isolation::*;
pub use self::operations::*;
pub use self::sandbox::*;
pub use self::shared_vm::*;
pub use self::space_cache::*;
pub use self::space_cache_trust::*;
pub use self::stack::*;
pub use self::workload::*;

/// Contract invariants that must hold consistently for runtime data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractInvariantError {
    /// Container lifecycle timestamps are inconsistent with the reported status.
    LifecycleInconsistency {
        container_id: String,
        details: String,
    },
    /// Shared VM phase transitions violated the allowed state machine.
    SharedVmPhaseTransition {
        from: SharedVmPhase,
        to: SharedVmPhase,
    },
    /// Sandbox state transition was invalid.
    SandboxStateTransition {
        sandbox_id: String,
        from: SandboxState,
        to: SandboxState,
    },
    /// Lease state transition was invalid.
    LeaseStateTransition {
        lease_id: String,
        from: LeaseState,
        to: LeaseState,
    },
    /// New leases can only be created when the sandbox is ready.
    LeaseRequiresReadySandbox {
        sandbox_id: String,
        state: SandboxState,
    },
    /// New work can only be submitted on active leases.
    WorkRequiresActiveLease {
        lease_id: String,
        state: LeaseState,
        operation: String,
    },
    /// Container state transition was invalid.
    ContainerStateTransition {
        container_id: String,
        from: ContainerState,
        to: ContainerState,
    },
    /// Exec operations require a running container.
    ExecRequiresRunningContainer {
        container_id: String,
        state: ContainerState,
    },
    /// Build state transition was invalid.
    BuildStateTransition {
        build_id: String,
        from: BuildState,
        to: BuildState,
    },
    /// Build record fields are inconsistent with the reported state.
    BuildLifecycleInconsistency { build_id: String, details: String },
    /// Execution state transition was invalid.
    ExecutionStateTransition {
        execution_id: String,
        from: ExecutionState,
        to: ExecutionState,
    },
    /// Execution record fields are inconsistent with the reported state.
    ExecutionLifecycleInconsistency {
        execution_id: String,
        details: String,
    },
    /// Checkpoint state transition was invalid.
    CheckpointStateTransition {
        checkpoint_id: String,
        from: CheckpointState,
        to: CheckpointState,
    },
    /// Checkpoint identifier already exists in lineage metadata.
    CheckpointAlreadyExists { checkpoint_id: String },
    /// Checkpoint parent is missing from lineage metadata.
    CheckpointParentNotFound {
        checkpoint_id: String,
        parent_checkpoint_id: String,
    },
    /// Image digest invariants were violated.
    ImageDigestInvariant { image_ref: String, details: String },
    /// Receipt event ranges must be ordered.
    ReceiptEventRangeInvalid {
        receipt_id: String,
        start_event_id: u64,
        end_event_id: u64,
    },
}

impl fmt::Display for ContractInvariantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContractInvariantError::LifecycleInconsistency {
                container_id,
                details,
            } => write!(
                f,
                "Lifecycle invariant violated for container {}: {}",
                container_id, details
            ),
            ContractInvariantError::SharedVmPhaseTransition { from, to } => write!(
                f,
                "Invalid shared VM phase transition from {:?} to {:?}",
                from, to
            ),
            ContractInvariantError::SandboxStateTransition {
                sandbox_id,
                from,
                to,
            } => write!(
                f,
                "Invalid sandbox state transition for {} from {:?} to {:?}",
                sandbox_id, from, to
            ),
            ContractInvariantError::LeaseStateTransition { lease_id, from, to } => write!(
                f,
                "Invalid lease state transition for {} from {:?} to {:?}",
                lease_id, from, to
            ),
            ContractInvariantError::LeaseRequiresReadySandbox { sandbox_id, state } => write!(
                f,
                "Sandbox {} must be ready to open a lease (state: {:?})",
                sandbox_id, state
            ),
            ContractInvariantError::WorkRequiresActiveLease {
                lease_id,
                state,
                operation,
            } => write!(
                f,
                "Lease {} must be active for {} (state: {:?})",
                lease_id, operation, state
            ),
            ContractInvariantError::ContainerStateTransition {
                container_id,
                from,
                to,
            } => write!(
                f,
                "Invalid container state transition for {} from {:?} to {:?}",
                container_id, from, to
            ),
            ContractInvariantError::ExecRequiresRunningContainer {
                container_id,
                state,
            } => write!(
                f,
                "Container {} must be running for exec (state: {:?})",
                container_id, state
            ),
            ContractInvariantError::BuildStateTransition { build_id, from, to } => write!(
                f,
                "Invalid build state transition for {} from {:?} to {:?}",
                build_id, from, to
            ),
            ContractInvariantError::BuildLifecycleInconsistency { build_id, details } => write!(
                f,
                "Build lifecycle invariant violated for {}: {}",
                build_id, details
            ),
            ContractInvariantError::ExecutionStateTransition {
                execution_id,
                from,
                to,
            } => write!(
                f,
                "Invalid execution state transition for {} from {:?} to {:?}",
                execution_id, from, to
            ),
            ContractInvariantError::ExecutionLifecycleInconsistency {
                execution_id,
                details,
            } => write!(
                f,
                "Execution lifecycle invariant violated for {}: {}",
                execution_id, details
            ),
            ContractInvariantError::CheckpointStateTransition {
                checkpoint_id,
                from,
                to,
            } => write!(
                f,
                "Invalid checkpoint state transition for {} from {:?} to {:?}",
                checkpoint_id, from, to
            ),
            ContractInvariantError::CheckpointAlreadyExists { checkpoint_id } => write!(
                f,
                "Checkpoint {} already exists in lineage metadata",
                checkpoint_id
            ),
            ContractInvariantError::CheckpointParentNotFound {
                checkpoint_id,
                parent_checkpoint_id,
            } => write!(
                f,
                "Checkpoint {} references missing parent {}",
                checkpoint_id, parent_checkpoint_id
            ),
            ContractInvariantError::ImageDigestInvariant { image_ref, details } => write!(
                f,
                "Image digest invariant violated for {}: {}",
                image_ref, details
            ),
            ContractInvariantError::ReceiptEventRangeInvalid {
                receipt_id,
                start_event_id,
                end_event_id,
            } => write!(
                f,
                "Receipt {} has invalid event range [{}..={}]",
                receipt_id, start_event_id, end_event_id
            ),
        }
    }
}

impl std::error::Error for ContractInvariantError {}
