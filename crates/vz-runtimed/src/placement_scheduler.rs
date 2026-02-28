use std::collections::BTreeMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use vz_runtime_contract::{
    ContainerState, MachineError, MachineErrorCode, RuntimeCapabilities, SandboxBackend,
    SandboxState,
};
use vz_stack::{StackError, StateStore};

pub(crate) const DEFAULT_MAX_ACTIVE_SANDBOXES: usize = 128;
pub(crate) const DEFAULT_MAX_ACTIVE_CONTAINERS: usize = 2048;

const ADAPTIVE_SANDBOX_EXECUTION_DIVISOR: usize = 8;
const ADAPTIVE_CONTAINER_EXECUTION_DIVISOR: usize = 2;

#[derive(Debug, Clone, Default)]
pub(crate) struct PlacementSnapshot {
    pub active_sandboxes: usize,
    pub active_containers: usize,
    pub active_executions: usize,
    pub active_sandboxes_by_backend: BTreeMap<String, usize>,
    pub active_containers_by_backend: BTreeMap<String, usize>,
    pub active_executions_by_backend: BTreeMap<String, usize>,
    pub updated_at_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackendPlacementCandidate {
    pub backend_id: String,
    pub capabilities: RuntimeCapabilities,
    pub is_available: bool,
}

impl BackendPlacementCandidate {
    pub(crate) fn available(
        backend_id: impl Into<String>,
        capabilities: RuntimeCapabilities,
    ) -> Self {
        Self {
            backend_id: backend_id.into(),
            capabilities,
            is_available: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementDecision {
    pub backend_id: String,
    pub score: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlacementOperation {
    CreateSandbox,
    CreateContainer,
}

impl PlacementOperation {
    fn label(self) -> &'static str {
        match self {
            Self::CreateSandbox => "create_sandbox",
            Self::CreateContainer => "create_container",
        }
    }

    fn resource_label(self) -> &'static str {
        match self {
            Self::CreateSandbox => "sandbox",
            Self::CreateContainer => "container",
        }
    }

    fn max_limit_detail_key(self) -> &'static str {
        match self {
            Self::CreateSandbox => "max_active_sandboxes",
            Self::CreateContainer => "max_active_containers",
        }
    }

    fn adaptive_limit_detail_key(self) -> &'static str {
        match self {
            Self::CreateSandbox => "adaptive_max_active_sandboxes",
            Self::CreateContainer => "adaptive_max_active_containers",
        }
    }

    fn active_detail_key(self) -> &'static str {
        match self {
            Self::CreateSandbox => "active_sandboxes",
            Self::CreateContainer => "active_containers",
        }
    }
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
        let sandboxes = store.list_sandboxes()?;
        let mut sandbox_backend_by_id = BTreeMap::new();
        let mut active_sandboxes = 0;
        let mut active_sandboxes_by_backend = BTreeMap::new();
        for sandbox in &sandboxes {
            let backend_id = backend_id_from_sandbox_backend(&sandbox.backend);
            sandbox_backend_by_id.insert(sandbox.sandbox_id.clone(), backend_id.clone());
            if matches!(
                sandbox.state,
                SandboxState::Terminated | SandboxState::Failed
            ) {
                continue;
            }
            active_sandboxes += 1;
            increment_backend_count(&mut active_sandboxes_by_backend, &backend_id);
        }

        let containers = store.list_containers()?;
        let mut container_backend_by_id = BTreeMap::new();
        let mut active_containers = 0;
        let mut active_containers_by_backend = BTreeMap::new();
        for container in &containers {
            let backend_id = sandbox_backend_by_id
                .get(&container.sandbox_id)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            container_backend_by_id.insert(container.container_id.clone(), backend_id.clone());
            if matches!(container.state, ContainerState::Removed) {
                continue;
            }
            active_containers += 1;
            increment_backend_count(&mut active_containers_by_backend, &backend_id);
        }

        let executions = store.list_executions()?;
        let mut active_executions = 0;
        let mut active_executions_by_backend = BTreeMap::new();
        for execution in &executions {
            if execution.state.is_terminal() {
                continue;
            }
            active_executions += 1;
            let backend_id = container_backend_by_id
                .get(&execution.container_id)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            increment_backend_count(&mut active_executions_by_backend, &backend_id);
        }

        let snapshot = PlacementSnapshot {
            active_sandboxes,
            active_containers,
            active_executions,
            active_sandboxes_by_backend,
            active_containers_by_backend,
            active_executions_by_backend,
            updated_at_unix_secs: now_unix_secs,
        };
        let mut guard = self.snapshot.write().map_err(|_| StackError::Machine {
            code: MachineErrorCode::InternalError,
            message: "placement scheduler snapshot lock poisoned".to_string(),
        })?;
        *guard = snapshot.clone();
        Ok(snapshot)
    }

    pub(crate) fn snapshot(&self) -> Result<PlacementSnapshot, StackError> {
        self.snapshot
            .read()
            .map(|guard| guard.clone())
            .map_err(|_| StackError::Machine {
                code: MachineErrorCode::InternalError,
                message: "placement scheduler snapshot lock poisoned".to_string(),
            })
    }

    pub(crate) fn evaluate_create_sandbox(
        &self,
        candidates: &[BackendPlacementCandidate],
        request_id: &str,
    ) -> Result<PlacementDecision, MachineError> {
        self.evaluate(PlacementOperation::CreateSandbox, candidates, request_id)
    }

    pub(crate) fn evaluate_create_container(
        &self,
        candidates: &[BackendPlacementCandidate],
        request_id: &str,
    ) -> Result<PlacementDecision, MachineError> {
        self.evaluate(PlacementOperation::CreateContainer, candidates, request_id)
    }

    fn evaluate(
        &self,
        operation: PlacementOperation,
        candidates: &[BackendPlacementCandidate],
        request_id: &str,
    ) -> Result<PlacementDecision, MachineError> {
        if candidates.is_empty() {
            return Err(MachineError::new(
                MachineErrorCode::InternalError,
                format!(
                    "placement scheduler requires at least one backend candidate for {}",
                    operation.label()
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ));
        }

        let snapshot = self
            .snapshot()
            .map_err(|error| internal_scheduler_machine_error(error, request_id))?;
        let base_limit = match operation {
            PlacementOperation::CreateSandbox => self.max_active_sandboxes.load(Ordering::Relaxed),
            PlacementOperation::CreateContainer => {
                self.max_active_containers.load(Ordering::Relaxed)
            }
        };

        let mut available_candidates = 0usize;
        let mut unsupported_candidates = 0usize;
        let mut saturated_candidates = 0usize;
        let mut backend_statuses = Vec::with_capacity(candidates.len());
        let mut best: Option<PlacementDecision> = None;

        for candidate in candidates {
            if !candidate.is_available {
                backend_statuses.push(format!("{}:unavailable", candidate.backend_id));
                continue;
            }
            available_candidates += 1;

            if !candidate.capabilities.shared_vm {
                unsupported_candidates += 1;
                backend_statuses.push(format!("{}:unsupported_shared_vm", candidate.backend_id));
                continue;
            }

            let active_count = match operation {
                PlacementOperation::CreateSandbox => candidate_backend_count(
                    &snapshot.active_sandboxes_by_backend,
                    &candidate.backend_id,
                    snapshot.active_sandboxes,
                    candidates.len(),
                ),
                PlacementOperation::CreateContainer => candidate_backend_count(
                    &snapshot.active_containers_by_backend,
                    &candidate.backend_id,
                    snapshot.active_containers,
                    candidates.len(),
                ),
            };
            let active_executions = candidate_backend_count(
                &snapshot.active_executions_by_backend,
                &candidate.backend_id,
                snapshot.active_executions,
                candidates.len(),
            );

            let adaptive_limit = adaptive_capacity_limit(base_limit, active_executions, operation);
            if adaptive_limit == 0 || active_count >= adaptive_limit {
                saturated_candidates += 1;
                backend_statuses.push(format!(
                    "{}:saturated({}/{})",
                    candidate.backend_id, active_count, adaptive_limit
                ));
                continue;
            }

            let score = candidate_score(active_count, adaptive_limit, active_executions);
            backend_statuses.push(format!("{}:score({score})", candidate.backend_id));
            if best.as_ref().is_none_or(|current| score > current.score) {
                best = Some(PlacementDecision {
                    backend_id: candidate.backend_id.clone(),
                    score,
                });
            }
        }

        if let Some(decision) = best {
            return Ok(decision);
        }

        let mut details = BTreeMap::new();
        details.insert("candidate_count".to_string(), candidates.len().to_string());
        details.insert(
            "available_candidates".to_string(),
            available_candidates.to_string(),
        );
        details.insert(
            "unsupported_candidates".to_string(),
            unsupported_candidates.to_string(),
        );
        details.insert(
            "saturated_candidates".to_string(),
            saturated_candidates.to_string(),
        );
        details.insert("backend_statuses".to_string(), backend_statuses.join(","));
        details.insert(
            operation.max_limit_detail_key().to_string(),
            base_limit.to_string(),
        );
        details.insert(
            "active_executions".to_string(),
            snapshot.active_executions.to_string(),
        );

        if available_candidates == 0 {
            return Err(MachineError::new(
                MachineErrorCode::BackendUnavailable,
                format!(
                    "placement denied: no available backend candidates for {}",
                    operation.label()
                ),
                Some(request_id.to_string()),
                details,
            ));
        }

        if unsupported_candidates == available_candidates {
            return Err(MachineError::new(
                MachineErrorCode::UnsupportedOperation,
                format!(
                    "placement denied: available backend candidates do not support {} with shared_vm",
                    operation.label()
                ),
                Some(request_id.to_string()),
                details,
            ));
        }

        let adaptive_limit =
            adaptive_capacity_limit(base_limit, snapshot.active_executions, operation);
        details.insert(
            operation.active_detail_key().to_string(),
            match operation {
                PlacementOperation::CreateSandbox => snapshot.active_sandboxes,
                PlacementOperation::CreateContainer => snapshot.active_containers,
            }
            .to_string(),
        );
        details.insert(
            operation.adaptive_limit_detail_key().to_string(),
            adaptive_limit.to_string(),
        );
        Err(MachineError::new(
            MachineErrorCode::BackendUnavailable,
            format!(
                "placement denied: {} capacity exhausted across backend candidates",
                operation.resource_label()
            ),
            Some(request_id.to_string()),
            details,
        ))
    }

    #[cfg(test)]
    pub(crate) fn set_limits_for_test(&self, max_sandboxes: usize, max_containers: usize) {
        self.max_active_sandboxes
            .store(max_sandboxes, Ordering::Relaxed);
        self.max_active_containers
            .store(max_containers, Ordering::Relaxed);
    }
}

fn backend_id_from_sandbox_backend(backend: &SandboxBackend) -> String {
    match backend {
        SandboxBackend::MacosVz => "macos-vz".to_string(),
        SandboxBackend::LinuxFirecracker => "linux-firecracker".to_string(),
        SandboxBackend::Other(value) => value.clone(),
    }
}

fn increment_backend_count(counts: &mut BTreeMap<String, usize>, backend_id: &str) {
    counts
        .entry(backend_id.to_string())
        .and_modify(|value| *value += 1)
        .or_insert(1);
}

fn candidate_backend_count(
    counts: &BTreeMap<String, usize>,
    backend_id: &str,
    fallback_single_backend_total: usize,
    candidate_count: usize,
) -> usize {
    counts
        .get(backend_id)
        .copied()
        .or_else(|| {
            if candidate_count == 1 {
                Some(fallback_single_backend_total)
            } else {
                None
            }
        })
        .unwrap_or(0)
}

fn adaptive_capacity_limit(
    base_limit: usize,
    active_executions: usize,
    operation: PlacementOperation,
) -> usize {
    if base_limit == 0 {
        return 0;
    }
    let divisor = match operation {
        PlacementOperation::CreateSandbox => ADAPTIVE_SANDBOX_EXECUTION_DIVISOR,
        PlacementOperation::CreateContainer => ADAPTIVE_CONTAINER_EXECUTION_DIVISOR,
    };
    let penalty = active_executions / divisor;
    base_limit.saturating_sub(penalty).max(1)
}

fn candidate_score(active_count: usize, adaptive_limit: usize, active_executions: usize) -> i64 {
    let headroom = adaptive_limit.saturating_sub(active_count) as i64;
    let utilization_penalty = if adaptive_limit == 0 {
        100
    } else {
        ((active_count as u128 * 100) / adaptive_limit as u128) as i64
    };
    let execution_penalty = active_executions as i64;
    headroom * 100 - utilization_penalty - execution_penalty
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

    fn baseline_candidate(backend_id: &str) -> BackendPlacementCandidate {
        BackendPlacementCandidate::available(
            backend_id.to_string(),
            RuntimeCapabilities::stack_baseline(),
        )
    }

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
                active_sandboxes_by_backend: BTreeMap::from([("macos-vz".to_string(), 2)]),
                active_containers_by_backend: BTreeMap::new(),
                active_executions_by_backend: BTreeMap::new(),
                updated_at_unix_secs: 1,
            };
        }

        let error = scheduler
            .evaluate_create_sandbox(&[baseline_candidate("macos-vz")], "req-pressure")
            .expect_err("sandbox placement should be denied");
        assert_eq!(error.code, MachineErrorCode::BackendUnavailable);
    }

    #[test]
    fn scheduler_prefers_backend_with_higher_adaptive_headroom() {
        let scheduler = PlacementScheduler::default();
        scheduler.set_limits_for_test(10, 100);
        {
            let mut guard = scheduler.snapshot.write().expect("snapshot lock");
            *guard = PlacementSnapshot {
                active_sandboxes: 8,
                active_containers: 0,
                active_executions: 4,
                active_sandboxes_by_backend: BTreeMap::from([
                    ("macos-vz".to_string(), 7),
                    ("linux-firecracker".to_string(), 1),
                ]),
                active_containers_by_backend: BTreeMap::new(),
                active_executions_by_backend: BTreeMap::from([
                    ("macos-vz".to_string(), 3),
                    ("linux-firecracker".to_string(), 1),
                ]),
                updated_at_unix_secs: 1,
            };
        }

        let decision = scheduler
            .evaluate_create_sandbox(
                &[
                    baseline_candidate("macos-vz"),
                    baseline_candidate("linux-firecracker"),
                ],
                "req-score",
            )
            .expect("placement decision");
        assert_eq!(decision.backend_id, "linux-firecracker");
    }

    #[test]
    fn adaptive_limits_reduce_sandbox_capacity_when_execution_pressure_is_high() {
        let scheduler = PlacementScheduler::default();
        scheduler.set_limits_for_test(4, 100);
        {
            let mut guard = scheduler.snapshot.write().expect("snapshot lock");
            *guard = PlacementSnapshot {
                active_sandboxes: 3,
                active_containers: 0,
                active_executions: 16,
                active_sandboxes_by_backend: BTreeMap::from([("macos-vz".to_string(), 3)]),
                active_containers_by_backend: BTreeMap::new(),
                active_executions_by_backend: BTreeMap::from([("macos-vz".to_string(), 16)]),
                updated_at_unix_secs: 1,
            };
        }

        let error = scheduler
            .evaluate_create_sandbox(&[baseline_candidate("macos-vz")], "req-adaptive")
            .expect_err("adaptive pressure should deny placement");
        assert_eq!(error.code, MachineErrorCode::BackendUnavailable);
        assert_eq!(
            error.details.get("adaptive_max_active_sandboxes"),
            Some(&"2".to_string())
        );
    }

    #[test]
    fn unavailable_candidates_are_skipped_when_another_backend_is_healthy() {
        let scheduler = PlacementScheduler::default();
        {
            let mut guard = scheduler.snapshot.write().expect("snapshot lock");
            *guard = PlacementSnapshot {
                active_sandboxes: 1,
                active_containers: 0,
                active_executions: 0,
                active_sandboxes_by_backend: BTreeMap::from([("linux-firecracker".to_string(), 1)]),
                active_containers_by_backend: BTreeMap::new(),
                active_executions_by_backend: BTreeMap::new(),
                updated_at_unix_secs: 1,
            };
        }

        let unavailable = BackendPlacementCandidate {
            backend_id: "macos-vz".to_string(),
            capabilities: RuntimeCapabilities::stack_baseline(),
            is_available: false,
        };
        let decision = scheduler
            .evaluate_create_sandbox(
                &[unavailable, baseline_candidate("linux-firecracker")],
                "req-unavailable",
            )
            .expect("healthy backend should still be selected");
        assert_eq!(decision.backend_id, "linux-firecracker");
    }
}
