//! Health and dependency gating for service readiness.
//!
//! Evaluates whether services are ready based on their lifecycle
//! phase, health check configuration, and health check results.
//! Provides dependency readiness checking so the reconciler can
//! defer service creation until all dependencies are satisfied.

use std::collections::HashMap;

use crate::spec::{HealthCheckSpec, ServiceSpec};
use crate::state_store::{ServiceObservedState, ServicePhase};

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
}

impl HealthStatus {
    /// Create a new health status with zero counts.
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
            consecutive_passes: 0,
            consecutive_failures: 0,
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
/// - It is `Running` with a health check that has not yet passed.
///
/// This means a fresh deployment creates all services in one
/// topo-sorted pass, while health-checked dependencies gate their
/// dependents across apply cycles.
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

    for dep_name in &service.depends_on {
        let dep_obs = observed_map.get(dep_name.as_str());
        let dep_spec = spec_map.get(dep_name.as_str());
        let dep_health = health_statuses.get(dep_name);

        let blocked = match dep_obs {
            None => {
                // Not yet created — topo sort handles ordering within the batch.
                false
            }
            Some(obs) => match obs.phase {
                // Terminal states block dependent creation.
                ServicePhase::Failed | ServicePhase::Stopped => true,
                // Running: block only if there's a health check that hasn't passed.
                ServicePhase::Running => {
                    let healthcheck = dep_spec.and_then(|s| s.healthcheck.as_ref());
                    match healthcheck {
                        None => false, // Running + no health check = ready.
                        Some(hc) => !is_service_ready(obs, Some(hc), dep_health),
                    }
                }
                // Pending/Creating/Stopping — in progress, don't block.
                _ => false,
            },
        };

        if blocked {
            waiting_on.push(dep_name.clone());
        }
    }

    if waiting_on.is_empty() {
        DependencyCheck::Ready
    } else {
        waiting_on.sort();
        DependencyCheck::Blocked { waiting_on }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
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
    fn dep_running_with_healthcheck_failing_is_blocked() {
        let service = svc_with_deps("web", vec!["db"]);
        let all_services = vec![svc_with_healthcheck("db"), service.clone()];
        let observed = vec![obs("db", ServicePhase::Running)];

        // No health status means health check hasn't passed yet.
        let result = check_dependencies(&service, &observed, &all_services, &HashMap::new());
        assert_eq!(
            result,
            DependencyCheck::Blocked {
                waiting_on: vec!["db".to_string()]
            }
        );
    }

    #[test]
    fn dep_running_with_healthcheck_passing_is_ready() {
        let service = svc_with_deps("web", vec!["db"]);
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
}
