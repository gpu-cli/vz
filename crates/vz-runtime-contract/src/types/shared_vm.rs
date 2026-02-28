use serde::{Deserialize, Serialize};

use super::ContractInvariantError;

/// Runtime phases for a shared stack VM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedVmPhase {
    /// No shared VM is currently booted.
    Shutdown,
    /// A shared VM is in the process of booting.
    Booting,
    /// A shared VM has booted and is available for containers.
    Ready,
    /// The shared VM is in the process of shutting down.
    ShuttingDown,
}

impl SharedVmPhase {
    fn can_transition_to(self, next: SharedVmPhase) -> bool {
        matches!(
            (self, next),
            (SharedVmPhase::Shutdown, SharedVmPhase::Booting)
                | (SharedVmPhase::Booting, SharedVmPhase::Ready)
                | (SharedVmPhase::Ready, SharedVmPhase::ShuttingDown)
                | (SharedVmPhase::ShuttingDown, SharedVmPhase::Shutdown)
        )
    }
}

/// Tracks shared VM phases and validates transitions.
#[derive(Debug, Clone)]
pub struct SharedVmPhaseTracker {
    phase: SharedVmPhase,
}

impl Default for SharedVmPhaseTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedVmPhaseTracker {
    /// Start tracking from the shutdown phase.
    pub fn new() -> Self {
        Self {
            phase: SharedVmPhase::Shutdown,
        }
    }

    /// Current known shared VM phase.
    pub fn phase(&self) -> SharedVmPhase {
        self.phase
    }

    /// Attempt to transition to a new phase, returning an error if invalid.
    pub fn transition_to(&mut self, next: SharedVmPhase) -> Result<(), ContractInvariantError> {
        if self.phase == next {
            return Ok(());
        }

        if !self.phase.can_transition_to(next) {
            return Err(ContractInvariantError::SharedVmPhaseTransition {
                from: self.phase,
                to: next,
            });
        }

        self.phase = next;
        Ok(())
    }
}

/// Backend capability flags used by callers to branch behavior deterministically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RuntimeCapabilities {
    /// Supports fs-focused quick checkpoints.
    pub fs_quick_checkpoint: bool,
    /// Supports full VM checkpoints (RAM/CPU/device state).
    pub vm_full_checkpoint: bool,
    /// Supports checkpoint fork into a new sandbox lineage.
    pub checkpoint_fork: bool,
    /// Supports Docker command compatibility adapter.
    pub docker_compat: bool,
    /// Supports Compose adapter semantics.
    pub compose_adapter: bool,
    /// Supports build cache export/import semantics.
    pub build_cache_export: bool,
    /// Supports GPU passthrough for workloads.
    pub gpu_passthrough: bool,
    /// Supports runtime live-resize operations.
    pub live_resize: bool,
    /// Supports shared sandbox/VM orchestration for multi-service stacks.
    pub shared_vm: bool,
    /// Supports stack network setup/teardown APIs.
    pub stack_networking: bool,
    /// Supports runtime log retrieval for created containers.
    pub container_logs: bool,
}

impl RuntimeCapabilities {
    /// Baseline capabilities used by current stack-enabled backends.
    pub const fn stack_baseline() -> Self {
        Self {
            fs_quick_checkpoint: false,
            vm_full_checkpoint: false,
            checkpoint_fork: false,
            docker_compat: false,
            compose_adapter: true,
            build_cache_export: false,
            gpu_passthrough: false,
            live_resize: false,
            shared_vm: true,
            stack_networking: true,
            container_logs: true,
        }
    }
}
