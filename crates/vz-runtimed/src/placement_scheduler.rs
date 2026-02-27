use std::collections::BTreeMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use vz_runtime_contract::{
    ContainerState, MachineError, MachineErrorCode, RuntimeCapabilities, SandboxState,
};
use vz_stack::{StackError, StateStore};

pub(crate) const DEFAULT_MAX_ACTIVE_SANDBOXES: usize = 128;
pub(crate) const DEFAULT_MAX_ACTIVE_CONTAINERS: usize = 2048;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PlacementSnapshot {
    pub active_sandboxes: usize,
    pub active_containers: usize,
    pub active_executions: usize,
    pub updated_at_unix_secs: u64,
}

pub(crate) struct PlacementScheduler {
    snapshot: RwLock<PlacementSnapshot>,
    max_active_sandboxes: AtomicUsize,
    max_active_containers: AtomicUsize,
}

impl Default for PlacementScheduler {
    fn default() -> Self {
        Self {
            snapshot: RwLock::new(PlacementSnapshot::default()),
            max_active_sandboxes: AtomicUsize::new(DEFAULT_MAX_ACTIVE_SANDBOXES),
            max_active_containers: AtomicUsize::new(DEFAULT_MAX_ACTIVE_CONTAINERS),
        }
    }
}

impl PlacementScheduler {
    pub(crate) fn refresh(
        &self,
        store: &StateStore,
        now_unix_secs: u64,
    ) -> Result<PlacementSnapshot, StackError> {
        let active_sandboxes = store
            .list_sandboxes()?
            .into_iter()
            .filter(|sandbox| {
                !matches!(
                    sandbox.state,
                    SandboxState::Terminated | SandboxState::Failed
                )
            })
            .count();
        let active_containers = store
            .list_containers()?
            .into_iter()
            .filter(|container| !matches!(container.state, ContainerState::Removed))
            .count();
        let active_executions = store
            .list_executions()?
            .into_iter()
            .filter(|execution| !execution.state.is_terminal())
            .count();

        let snapshot = PlacementSnapshot {
            active_sandboxes,
            active_containers,
            active_executions,
            updated_at_unix_secs: now_unix_secs,
        };
        let mut guard = self.snapshot.write().map_err(|_| StackError::Machine {
            code: MachineErrorCode::InternalError,
            message: "placement scheduler snapshot lock poisoned".to_string(),
        })?;
        *guard = snapshot;
        Ok(snapshot)
    }

    pub(crate) fn snapshot(&self) -> Result<PlacementSnapshot, StackError> {
        self.snapshot
            .read()
            .map(|guard| *guard)
            .map_err(|_| StackError::Machine {
                code: MachineErrorCode::InternalError,
                message: "placement scheduler snapshot lock poisoned".to_string(),
            })
    }

    pub(crate) fn evaluate_create_sandbox(
        &self,
        capabilities: RuntimeCapabilities,
        request_id: &str,
    ) -> Result<(), MachineError> {
        if !capabilities.shared_vm {
            return Err(MachineError::new(
                MachineErrorCode::UnsupportedOperation,
                "placement denied: backend does not support shared_vm sandboxes".to_string(),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ));
        }

        let snapshot = self
            .snapshot()
            .map_err(|error| internal_scheduler_machine_error(error, request_id))?;
        let max_sandboxes = self.max_active_sandboxes.load(Ordering::Relaxed);
        if snapshot.active_sandboxes >= max_sandboxes {
            let mut details = BTreeMap::new();
            details.insert(
                "active_sandboxes".to_string(),
                snapshot.active_sandboxes.to_string(),
            );
            details.insert(
                "max_active_sandboxes".to_string(),
                max_sandboxes.to_string(),
            );
            return Err(MachineError::new(
                MachineErrorCode::BackendUnavailable,
                format!(
                    "placement denied: active sandbox pressure {}/{}",
                    snapshot.active_sandboxes, max_sandboxes
                ),
                Some(request_id.to_string()),
                details,
            ));
        }

        Ok(())
    }

    pub(crate) fn evaluate_create_container(
        &self,
        capabilities: RuntimeCapabilities,
        request_id: &str,
    ) -> Result<(), MachineError> {
        if !capabilities.shared_vm {
            return Err(MachineError::new(
                MachineErrorCode::UnsupportedOperation,
                "placement denied: backend does not support shared_vm containers".to_string(),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ));
        }

        let snapshot = self
            .snapshot()
            .map_err(|error| internal_scheduler_machine_error(error, request_id))?;
        let max_containers = self.max_active_containers.load(Ordering::Relaxed);
        if snapshot.active_containers >= max_containers {
            let mut details = BTreeMap::new();
            details.insert(
                "active_containers".to_string(),
                snapshot.active_containers.to_string(),
            );
            details.insert(
                "max_active_containers".to_string(),
                max_containers.to_string(),
            );
            return Err(MachineError::new(
                MachineErrorCode::BackendUnavailable,
                format!(
                    "placement denied: active container pressure {}/{}",
                    snapshot.active_containers, max_containers
                ),
                Some(request_id.to_string()),
                details,
            ));
        }

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_limits_for_test(&self, max_sandboxes: usize, max_containers: usize) {
        self.max_active_sandboxes
            .store(max_sandboxes, Ordering::Relaxed);
        self.max_active_containers
            .store(max_containers, Ordering::Relaxed);
    }
}

fn internal_scheduler_machine_error(error: StackError, request_id: &str) -> MachineError {
    let mut details = BTreeMap::new();
    details.insert("reason".to_string(), error.to_string());
    MachineError::new(
        MachineErrorCode::InternalError,
        format!("placement scheduler failed: {error}"),
        Some(request_id.to_string()),
        details,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_pressure_denies_when_limit_reached() {
        let scheduler = PlacementScheduler::default();
        scheduler.set_limits_for_test(2, 10);
        {
            let mut guard = scheduler.snapshot.write().expect("snapshot lock");
            *guard = PlacementSnapshot {
                active_sandboxes: 2,
                active_containers: 0,
                active_executions: 0,
                updated_at_unix_secs: 1,
            };
        }

        let error = scheduler
            .evaluate_create_sandbox(RuntimeCapabilities::stack_baseline(), "req-pressure")
            .expect_err("sandbox placement should be denied");
        assert_eq!(error.code, MachineErrorCode::BackendUnavailable);
    }
}
