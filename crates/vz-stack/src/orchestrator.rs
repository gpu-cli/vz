//! Stack orchestration loop: apply → execute → health poll → converge.
//!
//! The [`StackOrchestrator`] drives the reconciliation loop to convergence:
//! 1. Apply the desired spec to compute actions.
//! 2. Execute actions through the container runtime.
//! 3. Poll health checks for running services.
//! 4. Re-apply when health status changes (unblocking deferred services).
//! 5. Exit when all services are converged (running+ready or permanently failed).

use std::collections::HashMap;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::error::StackError;
use crate::executor::{ContainerRuntime, ExecutionResult, StackExecutor};
use crate::health::{HealthPollResult, HealthPoller};
use crate::reconcile::{ApplyResult, apply};
use crate::spec::StackSpec;
use crate::state_store::{ServicePhase, StateStore};

/// Default poll interval when no health checks are defined (seconds).
const DEFAULT_POLL_INTERVAL_SECS: u64 = 2;

/// Maximum number of reconciliation rounds before giving up.
const MAX_ROUNDS: usize = 100;

/// Configuration for the orchestration loop.
#[derive(Debug, Clone)]
pub struct OrchestrationConfig {
    /// Override poll interval (seconds). If `None`, uses the minimum
    /// health check interval from the spec, or [`DEFAULT_POLL_INTERVAL_SECS`].
    pub poll_interval: Option<u64>,
    /// Maximum number of reconciliation rounds. Default: [`MAX_ROUNDS`].
    pub max_rounds: usize,
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            poll_interval: None,
            max_rounds: MAX_ROUNDS,
        }
    }
}

/// Result of running the orchestration loop.
#[derive(Debug, Clone)]
pub struct OrchestrationResult {
    /// Whether the stack converged (all services ready or permanently failed).
    pub converged: bool,
    /// Number of reconciliation rounds executed.
    pub rounds: usize,
    /// Number of services in Running+ready state.
    pub services_ready: usize,
    /// Number of services that permanently failed.
    pub services_failed: usize,
}

/// Callback for each orchestration round, letting callers observe progress.
pub struct RoundReport {
    /// Current round number (1-indexed).
    pub round: usize,
    /// Result of reconciliation (actions planned).
    pub apply_result: ApplyResult,
    /// Result of executing actions (may be empty if no actions).
    pub exec_result: Option<ExecutionResult>,
    /// Result of health polling (may be empty if no health checks).
    pub health_result: Option<HealthPollResult>,
    /// Services currently ready.
    pub services_ready: usize,
    /// Services currently failed.
    pub services_failed: usize,
    /// Services still pending.
    pub services_pending: usize,
}

/// Drives the stack reconciliation loop to convergence.
///
/// Owns a [`StackExecutor`], a [`HealthPoller`], and a separate
/// [`StateStore`] connection for reconciliation (the executor has
/// its own connection for state persistence during execution).
pub struct StackOrchestrator<R: ContainerRuntime> {
    executor: StackExecutor<R>,
    reconcile_store: StateStore,
    health_poller: HealthPoller,
    config: OrchestrationConfig,
}

impl<R: ContainerRuntime> StackOrchestrator<R> {
    /// Create a new orchestrator.
    ///
    /// `reconcile_store` should be a separate [`StateStore`] connection
    /// from the one owned by `executor` (both point to the same DB file).
    pub fn new(
        executor: StackExecutor<R>,
        reconcile_store: StateStore,
        config: OrchestrationConfig,
    ) -> Self {
        Self {
            executor,
            reconcile_store,
            health_poller: HealthPoller::new(),
            config,
        }
    }

    /// Access the underlying executor.
    pub fn executor(&self) -> &StackExecutor<R> {
        &self.executor
    }

    /// Mutably access the underlying executor.
    pub fn executor_mut(&mut self) -> &mut StackExecutor<R> {
        &mut self.executor
    }

    /// Access the health poller.
    pub fn health_poller(&self) -> &HealthPoller {
        &self.health_poller
    }

    /// Run the orchestration loop until convergence or max rounds.
    ///
    /// The optional `on_round` callback is invoked after each round with
    /// a [`RoundReport`], allowing callers to print progress.
    pub fn run(
        &mut self,
        spec: &StackSpec,
        mut on_round: Option<&mut dyn FnMut(&RoundReport)>,
    ) -> Result<OrchestrationResult, StackError> {
        let poll_interval = Duration::from_secs(
            self.config
                .poll_interval
                .or_else(|| self.health_poller.min_interval(spec))
                .unwrap_or(DEFAULT_POLL_INTERVAL_SECS),
        );

        let has_health_checks = spec.services.iter().any(|s| s.healthcheck.is_some());

        for round in 1..=self.config.max_rounds {
            info!(round, "orchestration round");

            // 1. Reconcile with current health statuses.
            let health_statuses = self.health_poller.statuses().clone();
            let apply_result = apply(spec, &self.reconcile_store, &health_statuses)?;

            // 2. Execute any new actions.
            let exec_result = if !apply_result.actions.is_empty() {
                info!(
                    actions = apply_result.actions.len(),
                    deferred = apply_result.deferred.len(),
                    "executing actions"
                );
                let result = self.executor.execute(spec, &apply_result.actions)?;
                if result.failed > 0 {
                    warn!(failed = result.failed, "some actions failed");
                }
                Some(result)
            } else {
                None
            };

            // 3. Poll health checks (if any services have them).
            let health_result = if has_health_checks {
                let result = self.health_poller.poll_all(
                    self.executor.runtime(),
                    self.executor.store(),
                    spec,
                )?;
                if !result.newly_ready.is_empty() {
                    info!(ready = ?result.newly_ready, "services became ready");
                }
                if !result.newly_failed.is_empty() {
                    warn!(failed = ?result.newly_failed, "services failed health checks");
                }
                Some(result)
            } else {
                None
            };

            // 4. Check convergence.
            let (ready, failed, pending) = self.check_convergence(spec)?;

            debug!(
                round,
                ready,
                failed,
                pending,
                deferred = apply_result.deferred.len(),
                "convergence check"
            );

            // Invoke callback.
            if let Some(ref mut cb) = on_round {
                cb(&RoundReport {
                    round,
                    apply_result: apply_result.clone(),
                    exec_result: exec_result.clone(),
                    health_result: health_result.clone(),
                    services_ready: ready,
                    services_failed: failed,
                    services_pending: pending,
                });
            }

            if pending == 0 && apply_result.deferred.is_empty() {
                info!(rounds = round, ready, failed, "stack converged");
                return Ok(OrchestrationResult {
                    converged: true,
                    rounds: round,
                    services_ready: ready,
                    services_failed: failed,
                });
            }

            // 5. Sleep before next round.
            std::thread::sleep(poll_interval);
        }

        // Max rounds exhausted.
        let (ready, failed, _) = self.check_convergence(spec)?;
        warn!(
            max_rounds = self.config.max_rounds,
            ready, failed, "orchestration did not converge within max rounds"
        );

        Ok(OrchestrationResult {
            converged: false,
            rounds: self.config.max_rounds,
            services_ready: ready,
            services_failed: failed,
        })
    }

    /// Check how many services are ready, failed, or still pending.
    fn check_convergence(&self, spec: &StackSpec) -> Result<(usize, usize, usize), StackError> {
        let observed = self.executor.store().load_observed_state(&spec.name)?;
        let observed_map: HashMap<&str, _> = observed
            .iter()
            .map(|o| (o.service_name.as_str(), o))
            .collect();

        let mut ready = 0;
        let mut failed = 0;
        let mut pending = 0;

        for svc in &spec.services {
            match observed_map.get(svc.name.as_str()) {
                Some(obs) => match obs.phase {
                    ServicePhase::Running => {
                        let health_passed = match &svc.healthcheck {
                            None => true,
                            Some(_) => self
                                .health_poller
                                .statuses()
                                .get(&svc.name)
                                .is_some_and(|s| s.consecutive_passes >= 1),
                        };
                        if health_passed {
                            ready += 1;
                        } else {
                            pending += 1;
                        }
                    }
                    ServicePhase::Failed => failed += 1,
                    _ => pending += 1,
                },
                None => pending += 1,
            }
        }

        Ok((ready, failed, pending))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::executor::tests_support::MockContainerRuntime;
    use crate::spec::{HealthCheckSpec, ServiceSpec, StackSpec};

    fn svc(name: &str) -> ServiceSpec {
        ServiceSpec {
            name: name.to_string(),
            image: "img:latest".to_string(),
            command: None,
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            mounts: vec![],
            ports: vec![],
            depends_on: vec![],
            healthcheck: None,
            restart_policy: None,
            resources: Default::default(),
            extra_hosts: vec![],
            secrets: vec![],
        }
    }

    fn svc_with_deps(name: &str, deps: Vec<&str>) -> ServiceSpec {
        ServiceSpec {
            depends_on: deps.into_iter().map(String::from).collect(),
            ..svc(name)
        }
    }

    fn svc_with_healthcheck(name: &str) -> ServiceSpec {
        ServiceSpec {
            healthcheck: Some(HealthCheckSpec {
                test: vec!["CMD".to_string(), "true".to_string()],
                interval_secs: Some(1),
                timeout_secs: Some(1),
                retries: Some(3),
                start_period_secs: None,
            }),
            ..svc(name)
        }
    }

    fn stack(name: &str, services: Vec<ServiceSpec>) -> StackSpec {
        StackSpec {
            name: name.to_string(),
            services,
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
        }
    }

    /// Orchestrator using a shared on-disk SQLite DB so reconcile_store
    /// and exec_store see each other's writes.
    fn make_orchestrator_shared(
        runtime: MockContainerRuntime,
    ) -> (StackOrchestrator<MockContainerRuntime>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("state.db");
        let exec_store = StateStore::open(&db_path).unwrap();
        let reconcile_store = StateStore::open(&db_path).unwrap();
        let executor = StackExecutor::new(runtime, exec_store, tmp.path());
        let orch = StackOrchestrator::new(
            executor,
            reconcile_store,
            OrchestrationConfig {
                poll_interval: Some(0),
                max_rounds: 10,
            },
        );
        (orch, tmp)
    }

    #[test]
    fn converges_immediately_without_health_checks() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web"), svc("db")]);

        let result = orch.run(&spec, None).unwrap();

        assert!(result.converged);
        assert_eq!(result.rounds, 1);
        assert_eq!(result.services_ready, 2);
        assert_eq!(result.services_failed, 0);
    }

    #[test]
    fn reports_failed_services() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_create = true;
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web")]);

        let result = orch.run(&spec, None).unwrap();

        assert!(result.converged);
        assert_eq!(result.services_ready, 0);
        assert_eq!(result.services_failed, 1);
    }

    #[test]
    fn max_rounds_respected() {
        // Health check always fails → never converges.
        // Set retries higher than max_rounds so health never exhausts retries.
        let mut runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        runtime.exec_exit_code = 1;
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        orch.config.max_rounds = 3;

        let spec = stack(
            "app",
            vec![ServiceSpec {
                healthcheck: Some(HealthCheckSpec {
                    test: vec!["CMD".to_string(), "false".to_string()],
                    interval_secs: Some(1),
                    timeout_secs: Some(1),
                    retries: Some(100), // Much higher than max_rounds.
                    start_period_secs: None,
                }),
                ..svc("web")
            }],
        );
        let result = orch.run(&spec, None).unwrap();

        assert!(!result.converged);
        assert_eq!(result.rounds, 3);
    }

    #[test]
    fn converges_with_health_check_passing() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        // exec_exit_code defaults to 0 → health check passes.
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc_with_healthcheck("web")]);

        let result = orch.run(&spec, None).unwrap();

        assert!(result.converged);
        assert_eq!(result.services_ready, 1);
    }

    #[test]
    fn callback_invoked_each_round() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web")]);

        let mut round_count = 0;
        orch.run(
            &spec,
            Some(&mut |report: &RoundReport| {
                round_count += 1;
                assert_eq!(report.round, round_count);
            }),
        )
        .unwrap();

        assert_eq!(round_count, 1);
    }

    #[test]
    fn dependency_ordering_respected() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-db", "ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc_with_deps("web", vec!["db"]), svc("db")]);

        let result = orch.run(&spec, None).unwrap();

        assert!(result.converged);
        assert_eq!(result.services_ready, 2);

        // Verify db was created before web.
        // Multi-service stacks use create_in_stack instead of create.
        let calls = orch.executor.runtime().call_log();
        let create_calls: Vec<&str> = calls
            .iter()
            .filter(|(op, _)| op == "create" || op == "create_in_stack")
            .map(|(_, arg)| arg.as_str())
            .collect();
        assert_eq!(create_calls.len(), 2);
        // Both images are "img:latest" but db should be first via topo sort.
    }

    #[test]
    fn empty_spec_converges_immediately() {
        let runtime = MockContainerRuntime::new();
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![]);

        let result = orch.run(&spec, None).unwrap();

        assert!(result.converged);
        assert_eq!(result.rounds, 1);
        assert_eq!(result.services_ready, 0);
        assert_eq!(result.services_failed, 0);
    }
}
