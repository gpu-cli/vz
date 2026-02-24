//! Health and dependency gating for service readiness.
//!
//! Evaluates whether services are ready based on their lifecycle
//! phase, health check configuration, and health check results.
//! Provides dependency readiness checking so the reconciler can
//! defer service creation until all dependencies are satisfied.
//!
//! The [`HealthPoller`] runs one health check cycle across all
//! running services, updating observed state and emitting events.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tracing::{debug, info};

use crate::error::StackError;
use crate::events::StackEvent;
use crate::executor::ContainerRuntime;
use crate::spec::{DependencyCondition, HealthCheckSpec, ServiceSpec, StackSpec};
use crate::state_store::{ServiceObservedState, ServicePhase, StateStore};

/// Result of checking whether a service's dependencies are satisfied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DependencyCheck {
    /// All dependencies are ready.
    Ready,
    /// Some dependencies are not yet ready.
    Blocked {
        /// Names of dependencies that are not ready.
        waiting_on: Vec<String>,
    },
}

/// Health status for a service's health check executions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthStatus {
    /// Service name.
    pub service_name: String,
    /// Number of consecutive passed health checks.
    pub consecutive_passes: u32,
    /// Number of consecutive failed health checks.
    pub consecutive_failures: u32,
    /// When the last health check was executed.
    pub last_check: Option<Instant>,
}

impl HealthStatus {
    /// Create a new health status with zero counts.
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
            consecutive_passes: 0,
            consecutive_failures: 0,
            last_check: None,
        }
    }

    /// Record a passed health check.
    pub fn record_pass(&mut self) {
        self.consecutive_passes += 1;
        self.consecutive_failures = 0;
    }

    /// Record a failed health check.
    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.consecutive_passes = 0;
    }
}

/// Evaluate whether a service should be considered ready.
///
/// A service is ready when:
/// - It is in the `Running` phase, AND
/// - Either no health check is defined, OR the health check
///   has at least one consecutive pass.
pub fn is_service_ready(
    observed: &ServiceObservedState,
    healthcheck: Option<&HealthCheckSpec>,
    health_status: Option<&HealthStatus>,
) -> bool {
    // Must be Running to be ready.
    if observed.phase != ServicePhase::Running {
        return false;
    }

    // If the observed state already has `ready: true` and there's no
    // health check, that's sufficient.
    let Some(_spec) = healthcheck else {
        // No health check — running means ready.
        return true;
    };

    // Has a health check — need at least one consecutive pass.
    match health_status {
        Some(status) => status.consecutive_passes >= 1,
        None => false,
    }
}

/// Check if any dependency blocks creation of this service.
///
/// The reconciler uses topological sort to order actions within a
/// single apply batch, so dependencies that are not yet observed
/// (being created in the same batch) do NOT block. A dependency
/// only blocks when:
///
/// - It is in a terminal state (`Failed` / `Stopped`).
/// - The condition is `service_healthy` and the health check has
///   not yet passed.
/// - The condition is `service_completed_successfully` and the
///   service has not exited with code 0.
///
/// With the default `service_started` condition, a running service
/// is considered ready regardless of health check status — matching
/// Docker Compose semantics.
pub fn check_dependencies(
    service: &ServiceSpec,
    observed: &[ServiceObservedState],
    all_services: &[ServiceSpec],
    health_statuses: &HashMap<String, HealthStatus>,
) -> DependencyCheck {
    if service.depends_on.is_empty() {
        return DependencyCheck::Ready;
    }

    let observed_map: HashMap<&str, &ServiceObservedState> = observed
        .iter()
        .map(|o| (o.service_name.as_str(), o))
        .collect();

    let spec_map: HashMap<&str, &ServiceSpec> =
        all_services.iter().map(|s| (s.name.as_str(), s)).collect();

    let mut waiting_on = Vec::new();

    for dep in &service.depends_on {
        let dep_obs = observed_map.get(dep.service.as_str());
        let dep_spec = spec_map.get(dep.service.as_str());
        let dep_health = health_statuses.get(&dep.service);

        let blocked = match dep_obs {
            None => {
                // Not yet created — topo sort handles ordering within the batch.
                false
            }
            Some(obs) => match obs.phase {
                // Terminal states block dependent creation.
                ServicePhase::Failed | ServicePhase::Stopped => true,
                // Running: behaviour depends on the condition.
                ServicePhase::Running => match dep.condition {
                    DependencyCondition::ServiceStarted => {
                        // Running is sufficient — don't check health.
                        false
                    }
                    DependencyCondition::ServiceHealthy => {
                        // Must have a passing health check.
                        let healthcheck = dep_spec.and_then(|s| s.healthcheck.as_ref());
                        match healthcheck {
                            None => false, // No health check defined = ready.
                            Some(hc) => !is_service_ready(obs, Some(hc), dep_health),
                        }
                    }
                    DependencyCondition::ServiceCompletedSuccessfully => {
                        // Running means not completed yet → blocked.
                        true
                    }
                },
                // Pending/Creating/Stopping — in progress, don't block.
                _ => false,
            },
        };

        if blocked {
            waiting_on.push(dep.service.clone());
        }
    }

    if waiting_on.is_empty() {
        DependencyCheck::Ready
    } else {
        waiting_on.sort();
        DependencyCheck::Blocked { waiting_on }
    }
}

/// Resolve Docker health check command convention.
///
/// Docker uses a prefix to indicate how the command should be executed:
/// - `["CMD", "arg1", "arg2", ...]` → execute directly: `["arg1", "arg2", ...]`
/// - `["CMD-SHELL", "cmd string"]` → execute via shell: `["/bin/sh", "-c", "cmd string"]`
/// - Anything else → execute as-is
fn resolve_healthcheck_command(test: &[String]) -> Vec<String> {
    match test.first().map(|s| s.as_str()) {
        Some("CMD") => test[1..].to_vec(),
        Some("CMD-SHELL") => {
            let shell_cmd = test[1..].join(" ");
            vec!["/bin/sh".to_string(), "-c".to_string(), shell_cmd]
        }
        _ => test.to_vec(),
    }
}

/// Default health check interval when not specified (30s).
const DEFAULT_INTERVAL_SECS: u64 = 30;
/// Default health check timeout when not specified (30s).
const DEFAULT_TIMEOUT_SECS: u64 = 30;
/// Default health check retries threshold when not specified.
const DEFAULT_RETRIES: u32 = 3;

/// Polls health checks for running services in a stack.
///
/// Call [`poll_all`](HealthPoller::poll_all) periodically (at the
/// smallest configured interval) to run one cycle of health checks.
/// The poller respects `start_period_secs` grace periods and marks
/// services as `Failed` when consecutive failures exceed the
/// `retries` threshold.
pub struct HealthPoller {
    /// Health status per service name.
    statuses: HashMap<String, HealthStatus>,
    /// When each service was first observed as Running (for start_period grace).
    start_times: HashMap<String, Instant>,
}

/// Result of a single health poll cycle.
#[derive(Debug, Clone, Default)]
pub struct HealthPollResult {
    /// Services that became ready this cycle.
    pub newly_ready: Vec<String>,
    /// Services that exceeded retries and were marked failed.
    pub newly_failed: Vec<String>,
    /// Number of health checks executed.
    pub checks_run: usize,
}

impl HealthPoller {
    /// Create a new poller with no tracked state.
    pub fn new() -> Self {
        Self {
            statuses: HashMap::new(),
            start_times: HashMap::new(),
        }
    }

    /// Access the current health statuses (keyed by service name).
    pub fn statuses(&self) -> &HashMap<String, HealthStatus> {
        &self.statuses
    }

    /// Compute the minimum poll interval across all health-checked
    /// services in the spec, in seconds. Returns `None` if no
    /// services have health checks.
    pub fn min_interval(&self, spec: &StackSpec) -> Option<u64> {
        spec.services
            .iter()
            .filter_map(|s| s.healthcheck.as_ref())
            .map(|hc| hc.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS))
            .min()
    }

    /// Run one health check cycle for all running services with
    /// health checks.
    ///
    /// For each service that is Running and has a `HealthCheckSpec`:
    /// - Skips if still within the `start_period_secs` grace window.
    /// - Executes the health check command via `runtime.exec()`.
    /// - Records pass/fail in [`HealthStatus`].
    /// - On first pass: sets `observed.ready = true` and emits
    ///   [`StackEvent::HealthCheckPassed`].
    /// - On consecutive failures exceeding `retries`: marks service
    ///   as `Failed` and emits [`StackEvent::HealthCheckFailed`].
    pub fn poll_all<R: ContainerRuntime>(
        &mut self,
        runtime: &R,
        store: &StateStore,
        spec: &StackSpec,
    ) -> Result<HealthPollResult, StackError> {
        let observed = store.load_observed_state(&spec.name)?;
        let observed_map: HashMap<&str, &ServiceObservedState> = observed
            .iter()
            .map(|o| (o.service_name.as_str(), o))
            .collect();

        let mut result = HealthPollResult::default();
        let now = Instant::now();

        for svc in &spec.services {
            let Some(hc) = &svc.healthcheck else {
                continue;
            };

            let Some(obs) = observed_map.get(svc.name.as_str()) else {
                continue;
            };

            // Only check Running services.
            if obs.phase != ServicePhase::Running {
                continue;
            }

            let Some(ref container_id) = obs.container_id else {
                continue;
            };

            // Track when we first saw this service running.
            let start_time = *self.start_times.entry(svc.name.clone()).or_insert(now);

            // Respect start_period grace.
            let start_period = hc.start_period_secs.unwrap_or(0);
            let elapsed = now.duration_since(start_time).as_secs();
            if elapsed < start_period {
                debug!(
                    service = %svc.name,
                    remaining = start_period - elapsed,
                    "within start period grace, skipping health check"
                );
                continue;
            }

            // Respect the health check interval — skip if we checked
            // this service too recently.
            let interval = Duration::from_secs(hc.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS));
            {
                let status = self
                    .statuses
                    .entry(svc.name.clone())
                    .or_insert_with(|| HealthStatus::new(&svc.name));
                if let Some(last) = status.last_check {
                    if now.duration_since(last) < interval {
                        continue;
                    }
                }
            }

            // Execute health check command with timeout enforcement.
            // Docker convention: ["CMD", "arg1", ...] → exec directly,
            // ["CMD-SHELL", "cmd"] → exec through /bin/sh -c.
            let cmd = resolve_healthcheck_command(&hc.test);
            let timeout = Duration::from_secs(hc.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS));
            debug!(service = %svc.name, cmd = ?cmd, timeout_secs = timeout.as_secs(), "running health check");

            let exit_code = {
                let (tx, rx) = std::sync::mpsc::channel();
                let cid = container_id.clone();
                let cmd_clone = cmd.clone();
                std::thread::scope(|s| {
                    s.spawn(|| {
                        let result = runtime.exec(&cid, &cmd_clone);
                        let _ = tx.send(result);
                    });
                    match rx.recv_timeout(timeout) {
                        Ok(Ok(code)) => {
                            if code != 0 {
                                debug!(service = %svc.name, exit_code = code, cmd = ?cmd, "health check returned non-zero");
                            }
                            (code, None)
                        }
                        Ok(Err(e)) => {
                            debug!(service = %svc.name, error = %e, "health check exec failed");
                            (1, Some(format!("exec error: {e}")))
                        }
                        Err(_) => {
                            debug!(service = %svc.name, timeout_secs = timeout.as_secs(), "health check timed out");
                            (1, Some("timed out".to_string()))
                        }
                    }
                })
            };

            let status = self
                .statuses
                .entry(svc.name.clone())
                .or_insert_with(|| HealthStatus::new(&svc.name));
            status.last_check = Some(now);

            result.checks_run += 1;

            let (code, exec_error) = exit_code;
            if code == 0 {
                let was_ready = status.consecutive_passes >= 1;
                status.record_pass();

                if !was_ready {
                    // First pass — mark ready.
                    info!(service = %svc.name, "health check passed, service ready");
                    store.save_observed_state(
                        &spec.name,
                        &ServiceObservedState {
                            service_name: svc.name.clone(),
                            phase: ServicePhase::Running,
                            container_id: Some(container_id.clone()),
                            last_error: None,
                            ready: true,
                        },
                    )?;
                    store.emit_event(
                        &spec.name,
                        &StackEvent::HealthCheckPassed {
                            stack_name: spec.name.clone(),
                            service_name: svc.name.clone(),
                        },
                    )?;
                    result.newly_ready.push(svc.name.clone());
                }
            } else {
                status.record_failure();

                let retries = hc.retries.unwrap_or(DEFAULT_RETRIES);

                // Build error message with command and exit code or error
                let error_msg = if let Some(err) = exec_error {
                    format!("{} → {}", cmd.join(" "), err)
                } else {
                    format!("{} → exit code {}", cmd.join(" "), code)
                };

                // Emit event for every failure.
                store.emit_event(
                    &spec.name,
                    &StackEvent::HealthCheckFailed {
                        stack_name: spec.name.clone(),
                        service_name: svc.name.clone(),
                        attempt: status.consecutive_failures,
                        error: error_msg,
                    },
                )?;

                if status.consecutive_failures >= retries {
                    // Retries exhausted — mark unhealthy but keep running.
                    // Docker Compose semantics: container stays running, health
                    // checks continue indefinitely, and a future pass can
                    // promote the service back to healthy/ready.

                    // Read container output from VM-level log directory.
                    let log_output = match runtime.logs(container_id) {
                        Ok(logs) if !logs.output.is_empty() => {
                            let lines: Vec<&str> = logs.output.lines().rev().take(30).collect();
                            let lines: Vec<&str> = lines.into_iter().rev().collect();
                            lines.join("\n")
                        }
                        Ok(_) => "(no output captured)".to_string(),
                        Err(e) => format!("(logs error: {e})"),
                    };
                    debug!(
                        service = %svc.name,
                        failures = status.consecutive_failures,
                        retries,
                        container_output = %log_output,
                        "health check retries exhausted, service unhealthy (will keep checking)"
                    );
                    result.newly_failed.push(svc.name.clone());
                    // Reset counter so we keep polling on subsequent cycles.
                    status.consecutive_failures = 0;
                } else {
                    debug!(
                        service = %svc.name,
                        failures = status.consecutive_failures,
                        retries,
                        "health check failed, will retry"
                    );
                }
            }
        }

        Ok(result)
    }

    /// Clear tracked state for a service (e.g., when it is removed).
    pub fn clear(&mut self, service_name: &str) {
        self.statuses.remove(service_name);
        self.start_times.remove(service_name);
    }

    /// Clear all tracked state.
    pub fn clear_all(&mut self) {
        self.statuses.clear();
        self.start_times.clear();
    }
}

impl Default for HealthPoller {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::spec::ServiceDependency;
    use std::collections::HashMap;

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
            networks: vec![],
            cap_add: vec![],
            cap_drop: vec![],
            privileged: false,
            read_only: false,
            sysctls: std::collections::HashMap::new(),
            ulimits: vec![],
            container_name: None,
            hostname: None,
            domainname: None,
            labels: std::collections::HashMap::new(),
            stop_signal: None,
            stop_grace_period_secs: None,
        }
    }

    fn svc_with_deps(name: &str, deps: Vec<&str>) -> ServiceSpec {
        ServiceSpec {
            depends_on: deps.into_iter().map(ServiceDependency::started).collect(),
            ..svc(name)
        }
    }

    fn svc_with_healthy_deps(name: &str, deps: Vec<&str>) -> ServiceSpec {
        ServiceSpec {
            depends_on: deps.into_iter().map(ServiceDependency::healthy).collect(),
            ..svc(name)
        }
    }

    fn svc_with_healthcheck(name: &str) -> ServiceSpec {
        ServiceSpec {
            healthcheck: Some(HealthCheckSpec {
                test: vec![
                    "CMD".to_string(),
                    "curl".to_string(),
                    "localhost".to_string(),
                ],
                interval_secs: Some(5),
                timeout_secs: Some(3),
                retries: Some(3),
                start_period_secs: None,
            }),
            ..svc(name)
        }
    }

    fn obs(name: &str, phase: ServicePhase) -> ServiceObservedState {
        ServiceObservedState {
            service_name: name.to_string(),
            phase,
            container_id: None,
            last_error: None,
            ready: false,
        }
    }

    fn obs_ready(name: &str) -> ServiceObservedState {
        ServiceObservedState {
            phase: ServicePhase::Running,
            ready: true,
            ..obs(name, ServicePhase::Running)
        }
    }

    // ── is_service_ready ──

    #[test]
    fn ready_when_running_no_healthcheck() {
        let observed = obs("web", ServicePhase::Running);
        assert!(is_service_ready(&observed, None, None));
    }

    #[test]
    fn not_ready_when_pending() {
        let observed = obs("web", ServicePhase::Pending);
        assert!(!is_service_ready(&observed, None, None));
    }

    #[test]
    fn not_ready_when_creating() {
        let observed = obs("web", ServicePhase::Creating);
        assert!(!is_service_ready(&observed, None, None));
    }

    #[test]
    fn not_ready_when_stopped() {
        let observed = obs("web", ServicePhase::Stopped);
        assert!(!is_service_ready(&observed, None, None));
    }

    #[test]
    fn not_ready_when_failed() {
        let observed = obs("web", ServicePhase::Failed);
        assert!(!is_service_ready(&observed, None, None));
    }

    #[test]
    fn ready_with_healthcheck_passing() {
        let observed = obs("web", ServicePhase::Running);
        let spec = HealthCheckSpec {
            test: vec!["CMD".to_string(), "true".to_string()],
            interval_secs: None,
            timeout_secs: None,
            retries: None,
            start_period_secs: None,
        };
        let mut status = HealthStatus::new("web");
        status.record_pass();

        assert!(is_service_ready(&observed, Some(&spec), Some(&status)));
    }

    #[test]
    fn not_ready_with_healthcheck_no_status() {
        let observed = obs("web", ServicePhase::Running);
        let spec = HealthCheckSpec {
            test: vec!["CMD".to_string(), "true".to_string()],
            interval_secs: None,
            timeout_secs: None,
            retries: None,
            start_period_secs: None,
        };

        assert!(!is_service_ready(&observed, Some(&spec), None));
    }

    #[test]
    fn not_ready_with_healthcheck_only_failures() {
        let observed = obs("web", ServicePhase::Running);
        let spec = HealthCheckSpec {
            test: vec!["CMD".to_string(), "true".to_string()],
            interval_secs: None,
            timeout_secs: None,
            retries: None,
            start_period_secs: None,
        };
        let mut status = HealthStatus::new("web");
        status.record_failure();
        status.record_failure();

        assert!(!is_service_ready(&observed, Some(&spec), Some(&status)));
    }

    // ── HealthStatus ──

    #[test]
    fn health_status_pass_resets_failures() {
        let mut status = HealthStatus::new("web");
        status.record_failure();
        status.record_failure();
        assert_eq!(status.consecutive_failures, 2);
        assert_eq!(status.consecutive_passes, 0);

        status.record_pass();
        assert_eq!(status.consecutive_passes, 1);
        assert_eq!(status.consecutive_failures, 0);
    }

    #[test]
    fn health_status_failure_resets_passes() {
        let mut status = HealthStatus::new("web");
        status.record_pass();
        status.record_pass();
        assert_eq!(status.consecutive_passes, 2);

        status.record_failure();
        assert_eq!(status.consecutive_failures, 1);
        assert_eq!(status.consecutive_passes, 0);
    }

    // ── check_dependencies ──

    #[test]
    fn no_deps_always_ready() {
        let service = svc("web");
        let result = check_dependencies(&service, &[], &[], &HashMap::new());
        assert_eq!(result, DependencyCheck::Ready);
    }

    #[test]
    fn dep_not_created_is_not_blocked() {
        // Not-yet-created deps don't block — topo sort handles ordering.
        let service = svc_with_deps("web", vec!["db"]);
        let all_services = vec![svc("db"), service.clone()];

        let result = check_dependencies(&service, &[], &all_services, &HashMap::new());
        assert_eq!(result, DependencyCheck::Ready);
    }

    #[test]
    fn dep_running_no_healthcheck_is_ready() {
        let service = svc_with_deps("web", vec!["db"]);
        let all_services = vec![svc("db"), service.clone()];
        let observed = vec![obs("db", ServicePhase::Running)];

        let result = check_dependencies(&service, &observed, &all_services, &HashMap::new());
        assert_eq!(result, DependencyCheck::Ready);
    }

    #[test]
    fn dep_pending_is_not_blocked() {
        // Pending deps don't block — topo sort handles ordering.
        let service = svc_with_deps("web", vec!["db"]);
        let all_services = vec![svc("db"), service.clone()];
        let observed = vec![obs("db", ServicePhase::Pending)];

        let result = check_dependencies(&service, &observed, &all_services, &HashMap::new());
        assert_eq!(result, DependencyCheck::Ready);
    }

    #[test]
    fn dep_running_with_healthcheck_service_started_is_ready() {
        // service_started condition: running is sufficient, healthcheck irrelevant.
        let service = svc_with_deps("web", vec!["db"]);
        let all_services = vec![svc_with_healthcheck("db"), service.clone()];
        let observed = vec![obs("db", ServicePhase::Running)];

        // No health status — but condition is service_started, so not blocked.
        let result = check_dependencies(&service, &observed, &all_services, &HashMap::new());
        assert_eq!(result, DependencyCheck::Ready);
    }

    #[test]
    fn dep_running_with_healthcheck_service_healthy_blocks() {
        // service_healthy condition: must wait for health check to pass.
        let service = svc_with_healthy_deps("web", vec!["db"]);
        let all_services = vec![svc_with_healthcheck("db"), service.clone()];
        let observed = vec![obs("db", ServicePhase::Running)];

        // No health status means health check hasn't passed yet → blocked.
        let result = check_dependencies(&service, &observed, &all_services, &HashMap::new());
        assert_eq!(
            result,
            DependencyCheck::Blocked {
                waiting_on: vec!["db".to_string()]
            }
        );
    }

    #[test]
    fn dep_running_with_healthcheck_passing_service_healthy_is_ready() {
        // service_healthy condition + health check passed → ready.
        let service = svc_with_healthy_deps("web", vec!["db"]);
        let all_services = vec![svc_with_healthcheck("db"), service.clone()];
        let observed = vec![obs("db", ServicePhase::Running)];

        let mut statuses = HashMap::new();
        let mut db_status = HealthStatus::new("db");
        db_status.record_pass();
        statuses.insert("db".to_string(), db_status);

        let result = check_dependencies(&service, &observed, &all_services, &statuses);
        assert_eq!(result, DependencyCheck::Ready);
    }

    #[test]
    fn multiple_deps_one_failed_blocks() {
        let service = svc_with_deps("app", vec!["db", "cache"]);
        let all_services = vec![svc("db"), svc("cache"), service.clone()];
        let observed = vec![
            obs("db", ServicePhase::Running),
            obs("cache", ServicePhase::Failed),
        ];

        let result = check_dependencies(&service, &observed, &all_services, &HashMap::new());
        assert_eq!(
            result,
            DependencyCheck::Blocked {
                waiting_on: vec!["cache".to_string()]
            }
        );
    }

    #[test]
    fn multiple_deps_all_running_is_ready() {
        let service = svc_with_deps("app", vec!["db", "cache"]);
        let all_services = vec![svc("db"), svc("cache"), service.clone()];
        let observed = vec![
            obs("db", ServicePhase::Running),
            obs("cache", ServicePhase::Running),
        ];

        let result = check_dependencies(&service, &observed, &all_services, &HashMap::new());
        assert_eq!(result, DependencyCheck::Ready);
    }

    #[test]
    fn chain_deps_not_created_are_not_blocked() {
        // app → api → db. Nothing running. Topo sort handles ordering.
        let db = svc("db");
        let api = svc_with_deps("api", vec!["db"]);
        let app = svc_with_deps("app", vec!["api"]);
        let all = vec![db, api.clone(), app.clone()];

        // No observed state — all deps unblocked (topo sort handles order).
        let api_check = check_dependencies(&api, &[], &all, &HashMap::new());
        assert_eq!(api_check, DependencyCheck::Ready);

        let app_check = check_dependencies(&app, &[], &all, &HashMap::new());
        assert_eq!(app_check, DependencyCheck::Ready);
    }

    #[test]
    fn chain_dep_failed_blocks_dependents() {
        // api → db. db is Failed → blocks api.
        let db = svc("db");
        let api = svc_with_deps("api", vec!["db"]);
        let all = vec![db, api.clone()];
        let observed = vec![obs("db", ServicePhase::Failed)];

        let result = check_dependencies(&api, &observed, &all, &HashMap::new());
        assert_eq!(
            result,
            DependencyCheck::Blocked {
                waiting_on: vec!["db".to_string()]
            }
        );
    }

    #[test]
    fn dep_failed_is_blocked() {
        let service = svc_with_deps("web", vec!["db"]);
        let all_services = vec![svc("db"), service.clone()];
        let observed = vec![obs("db", ServicePhase::Failed)];

        let result = check_dependencies(&service, &observed, &all_services, &HashMap::new());
        assert_eq!(
            result,
            DependencyCheck::Blocked {
                waiting_on: vec!["db".to_string()]
            }
        );
    }

    #[test]
    fn dep_stopped_is_blocked() {
        let service = svc_with_deps("web", vec!["db"]);
        let all_services = vec![svc("db"), service.clone()];
        let observed = vec![obs("db", ServicePhase::Stopped)];

        let result = check_dependencies(&service, &observed, &all_services, &HashMap::new());
        assert_eq!(
            result,
            DependencyCheck::Blocked {
                waiting_on: vec!["db".to_string()]
            }
        );
    }

    // ── Readiness field on observed state ──

    #[test]
    fn observed_state_ready_field_defaults_false() {
        let json = r#"{"service_name":"web","phase":"Running"}"#;
        let state: ServiceObservedState = serde_json::from_str(json).unwrap();
        assert!(!state.ready);
    }

    #[test]
    fn observed_state_ready_field_round_trip() {
        let state = obs_ready("web");
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: ServiceObservedState = serde_json::from_str(&json).unwrap();
        assert!(deserialized.ready);
    }

    // ── HealthPoller tests ──

    use crate::executor::tests_support::MockContainerRuntime;
    use crate::spec::StackSpec;

    fn make_hc_spec(retries: Option<u32>) -> HealthCheckSpec {
        HealthCheckSpec {
            test: vec![
                "CMD".to_string(),
                "curl".to_string(),
                "localhost".to_string(),
            ],
            // Use 0 interval so tests can call poll_all repeatedly without delay.
            interval_secs: Some(0),
            timeout_secs: Some(3),
            retries,
            start_period_secs: None,
        }
    }

    fn stack_with_hc(name: &str, services: Vec<ServiceSpec>) -> StackSpec {
        StackSpec {
            name: name.to_string(),
            services,
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
            disk_size_mb: None,
        }
    }

    fn running_obs(name: &str, container_id: &str) -> ServiceObservedState {
        ServiceObservedState {
            service_name: name.to_string(),
            phase: ServicePhase::Running,
            container_id: Some(container_id.to_string()),
            last_error: None,
            ready: false,
        }
    }

    #[test]
    fn poller_pass_marks_service_ready() {
        let runtime = MockContainerRuntime::new();
        let store = StateStore::in_memory().unwrap();
        let spec = stack_with_hc(
            "app",
            vec![ServiceSpec {
                healthcheck: Some(make_hc_spec(Some(3))),
                ..svc("web")
            }],
        );

        store
            .save_observed_state("app", &running_obs("web", "ctr-1"))
            .unwrap();

        let mut poller = HealthPoller::new();
        let result = poller.poll_all(&runtime, &store, &spec).unwrap();

        assert_eq!(result.checks_run, 1);
        assert_eq!(result.newly_ready, vec!["web".to_string()]);
        assert!(result.newly_failed.is_empty());

        // Observed state should have ready=true.
        let observed = store.load_observed_state("app").unwrap();
        let web = observed.iter().find(|o| o.service_name == "web").unwrap();
        assert!(web.ready);
        assert_eq!(web.phase, ServicePhase::Running);

        // Event emitted.
        let events = store.load_events("app").unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::HealthCheckPassed { .. }))
        );
    }

    #[test]
    fn poller_failure_emits_event_without_failing_service() {
        let mut runtime = MockContainerRuntime::new();
        runtime.exec_exit_code = 1;
        let store = StateStore::in_memory().unwrap();
        let spec = stack_with_hc(
            "app",
            vec![ServiceSpec {
                healthcheck: Some(make_hc_spec(Some(3))),
                ..svc("web")
            }],
        );

        store
            .save_observed_state("app", &running_obs("web", "ctr-1"))
            .unwrap();

        let mut poller = HealthPoller::new();
        let result = poller.poll_all(&runtime, &store, &spec).unwrap();

        assert_eq!(result.checks_run, 1);
        assert!(result.newly_ready.is_empty());
        assert!(result.newly_failed.is_empty()); // Not yet at retries threshold.

        // HealthCheckFailed event emitted.
        let events = store.load_events("app").unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::HealthCheckFailed { .. }))
        );

        // Service still Running.
        let observed = store.load_observed_state("app").unwrap();
        let web = observed.iter().find(|o| o.service_name == "web").unwrap();
        assert_eq!(web.phase, ServicePhase::Running);
    }

    #[test]
    fn poller_retries_exhausted_marks_unhealthy_but_keeps_running() {
        let mut runtime = MockContainerRuntime::new();
        runtime.exec_exit_code = 1;
        let store = StateStore::in_memory().unwrap();
        let spec = stack_with_hc(
            "app",
            vec![ServiceSpec {
                healthcheck: Some(make_hc_spec(Some(2))), // 2 retries
                ..svc("web")
            }],
        );

        store
            .save_observed_state("app", &running_obs("web", "ctr-1"))
            .unwrap();

        let mut poller = HealthPoller::new();

        // First failure — not yet at threshold.
        let r1 = poller.poll_all(&runtime, &store, &spec).unwrap();
        assert!(r1.newly_failed.is_empty());

        // Second failure — hits retries=2 threshold → newly_failed reported.
        let r2 = poller.poll_all(&runtime, &store, &spec).unwrap();
        assert_eq!(r2.newly_failed, vec!["web".to_string()]);

        // Service stays Running (Docker semantics: unhealthy != killed).
        let observed = store.load_observed_state("app").unwrap();
        let web = observed.iter().find(|o| o.service_name == "web").unwrap();
        assert_eq!(web.phase, ServicePhase::Running);

        // Counter is reset so health checks continue.
        assert_eq!(poller.statuses()["web"].consecutive_failures, 0);

        // A subsequent pass can still mark the service healthy.
        runtime.exec_exit_code = 0;
        let r3 = poller.poll_all(&runtime, &store, &spec).unwrap();
        assert_eq!(r3.newly_ready, vec!["web".to_string()]);
    }

    #[test]
    fn poller_skips_non_running_services() {
        let runtime = MockContainerRuntime::new();
        let store = StateStore::in_memory().unwrap();
        let spec = stack_with_hc(
            "app",
            vec![ServiceSpec {
                healthcheck: Some(make_hc_spec(Some(3))),
                ..svc("web")
            }],
        );

        // Service is Pending, not Running.
        store
            .save_observed_state(
                "app",
                &ServiceObservedState {
                    service_name: "web".to_string(),
                    phase: ServicePhase::Pending,
                    container_id: None,
                    last_error: None,
                    ready: false,
                },
            )
            .unwrap();

        let mut poller = HealthPoller::new();
        let result = poller.poll_all(&runtime, &store, &spec).unwrap();
        assert_eq!(result.checks_run, 0);
    }

    #[test]
    fn poller_skips_services_without_healthcheck() {
        let runtime = MockContainerRuntime::new();
        let store = StateStore::in_memory().unwrap();
        // No healthcheck on the service.
        let spec = stack_with_hc("app", vec![svc("web")]);

        store
            .save_observed_state("app", &running_obs("web", "ctr-1"))
            .unwrap();

        let mut poller = HealthPoller::new();
        let result = poller.poll_all(&runtime, &store, &spec).unwrap();
        assert_eq!(result.checks_run, 0);
    }

    #[test]
    fn poller_pass_after_failures_resets_and_becomes_ready() {
        let mut runtime = MockContainerRuntime::new();
        runtime.exec_exit_code = 1;
        let store = StateStore::in_memory().unwrap();
        let spec = stack_with_hc(
            "app",
            vec![ServiceSpec {
                healthcheck: Some(make_hc_spec(Some(5))),
                ..svc("web")
            }],
        );

        store
            .save_observed_state("app", &running_obs("web", "ctr-1"))
            .unwrap();

        let mut poller = HealthPoller::new();

        // Two failures.
        poller.poll_all(&runtime, &store, &spec).unwrap();
        poller.poll_all(&runtime, &store, &spec).unwrap();
        assert_eq!(poller.statuses()["web"].consecutive_failures, 2);

        // Now a pass.
        runtime.exec_exit_code = 0;
        let result = poller.poll_all(&runtime, &store, &spec).unwrap();
        assert_eq!(result.newly_ready, vec!["web".to_string()]);
        assert_eq!(poller.statuses()["web"].consecutive_passes, 1);
        assert_eq!(poller.statuses()["web"].consecutive_failures, 0);
    }

    #[test]
    fn poller_clear_removes_service_state() {
        let mut poller = HealthPoller::new();
        poller
            .statuses
            .insert("web".to_string(), HealthStatus::new("web"));
        poller.start_times.insert("web".to_string(), Instant::now());

        poller.clear("web");
        assert!(!poller.statuses().contains_key("web"));
        assert!(!poller.start_times.contains_key("web"));
    }

    #[test]
    fn poller_min_interval_returns_smallest() {
        let poller = HealthPoller::new();
        let spec = stack_with_hc(
            "app",
            vec![
                ServiceSpec {
                    healthcheck: Some(HealthCheckSpec {
                        test: vec!["CMD".to_string()],
                        interval_secs: Some(30),
                        timeout_secs: None,
                        retries: None,
                        start_period_secs: None,
                    }),
                    ..svc("slow")
                },
                ServiceSpec {
                    healthcheck: Some(HealthCheckSpec {
                        test: vec!["CMD".to_string()],
                        interval_secs: Some(5),
                        timeout_secs: None,
                        retries: None,
                        start_period_secs: None,
                    }),
                    ..svc("fast")
                },
            ],
        );
        assert_eq!(poller.min_interval(&spec), Some(5));
    }

    #[test]
    fn poller_min_interval_none_when_no_healthchecks() {
        let poller = HealthPoller::new();
        let spec = stack_with_hc("app", vec![svc("web")]);
        assert_eq!(poller.min_interval(&spec), None);
    }

    #[test]
    fn poller_exec_error_treated_as_failure() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_exec = true;
        let store = StateStore::in_memory().unwrap();
        let spec = stack_with_hc(
            "app",
            vec![ServiceSpec {
                healthcheck: Some(make_hc_spec(Some(3))),
                ..svc("web")
            }],
        );

        store
            .save_observed_state("app", &running_obs("web", "ctr-1"))
            .unwrap();

        let mut poller = HealthPoller::new();
        let result = poller.poll_all(&runtime, &store, &spec).unwrap();

        assert_eq!(result.checks_run, 1);
        assert!(result.newly_ready.is_empty());
        assert_eq!(poller.statuses()["web"].consecutive_failures, 1);
    }

    #[test]
    fn poller_timeout_treated_as_failure() {
        use std::time::Duration;

        let mut runtime = MockContainerRuntime::new();
        // Exec sleeps for 2s, but health check timeout is 1s.
        runtime.exec_delay = Some(Duration::from_secs(2));
        let store = StateStore::in_memory().unwrap();

        let hc = HealthCheckSpec {
            test: vec!["CMD".to_string(), "slow-cmd".to_string()],
            interval_secs: Some(5),
            timeout_secs: Some(1),
            retries: Some(3),
            start_period_secs: None,
        };
        let spec = stack_with_hc(
            "app",
            vec![ServiceSpec {
                healthcheck: Some(hc),
                ..svc("web")
            }],
        );

        store
            .save_observed_state("app", &running_obs("web", "ctr-1"))
            .unwrap();

        let mut poller = HealthPoller::new();
        let result = poller.poll_all(&runtime, &store, &spec).unwrap();

        assert_eq!(result.checks_run, 1);
        assert!(result.newly_ready.is_empty());
        assert_eq!(poller.statuses()["web"].consecutive_failures, 1);

        // Event should be emitted for the failure.
        let events = store.load_events("app").unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::HealthCheckFailed { .. }))
        );
    }
}
