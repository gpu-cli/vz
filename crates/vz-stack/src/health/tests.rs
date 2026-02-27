#![allow(clippy::unwrap_used)]

use super::poller::build_health_check_error;
use super::*;
use crate::spec::{ServiceDependency, ServiceKind};
use std::collections::HashMap;

fn svc(name: &str) -> ServiceSpec {
    ServiceSpec {
        name: name.to_string(),
        kind: ServiceKind::Service,
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
        expose: vec![],
        stdin_open: false,
        tty: false,
        logging: None,
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

fn svc_with_completed_deps(name: &str, deps: Vec<&str>) -> ServiceSpec {
    ServiceSpec {
        depends_on: deps
            .into_iter()
            .map(|dep| ServiceDependency {
                service: dep.to_string(),
                condition: DependencyCondition::ServiceCompletedSuccessfully,
            })
            .collect(),
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
fn dep_not_created_is_blocked() {
    let service = svc_with_deps("web", vec!["db"]);
    let all_services = vec![svc("db"), service.clone()];

    let result = check_dependencies(&service, &[], &all_services, &HashMap::new());
    assert_eq!(
        result,
        DependencyCheck::Blocked {
            waiting_on: vec!["db".to_string()]
        }
    );
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
fn dep_pending_is_blocked() {
    let service = svc_with_deps("web", vec!["db"]);
    let all_services = vec![svc("db"), service.clone()];
    let observed = vec![obs("db", ServicePhase::Pending)];

    let result = check_dependencies(&service, &observed, &all_services, &HashMap::new());
    assert_eq!(
        result,
        DependencyCheck::Blocked {
            waiting_on: vec!["db".to_string()]
        }
    );
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
fn chain_deps_not_created_are_blocked() {
    // app → api → db. Nothing observed, both should be blocked.
    let db = svc("db");
    let api = svc_with_deps("api", vec!["db"]);
    let app = svc_with_deps("app", vec!["api"]);
    let all = vec![db, api.clone(), app.clone()];

    let api_check = check_dependencies(&api, &[], &all, &HashMap::new());
    assert_eq!(
        api_check,
        DependencyCheck::Blocked {
            waiting_on: vec!["db".to_string()]
        }
    );

    let app_check = check_dependencies(&app, &[], &all, &HashMap::new());
    assert_eq!(
        app_check,
        DependencyCheck::Blocked {
            waiting_on: vec!["api".to_string()]
        }
    );
}

#[test]
fn dep_completed_successfully_requires_stopped_without_error() {
    let service = svc_with_completed_deps("web", vec!["job"]);
    let all_services = vec![svc("job"), service.clone()];

    let running = vec![obs("job", ServicePhase::Running)];
    assert_eq!(
        check_dependencies(&service, &running, &all_services, &HashMap::new()),
        DependencyCheck::Blocked {
            waiting_on: vec!["job".to_string()]
        }
    );

    let mut stopped_ok = obs("job", ServicePhase::Stopped);
    stopped_ok.last_error = None;
    assert_eq!(
        check_dependencies(&service, &[stopped_ok], &all_services, &HashMap::new()),
        DependencyCheck::Ready
    );

    let mut stopped_err = obs("job", ServicePhase::Stopped);
    stopped_err.last_error = Some("exit code 1".to_string());
    assert_eq!(
        check_dependencies(&service, &[stopped_err], &all_services, &HashMap::new()),
        DependencyCheck::Blocked {
            waiting_on: vec!["job".to_string()]
        }
    );
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

// ── build_health_check_error tests ──

#[test]
fn build_health_check_error_basic_exit_code() {
    let msg = build_health_check_error("curl localhost:8080", 1, None, "", "", 1, 3);
    assert!(msg.contains("curl localhost:8080"));
    assert!(msg.contains("exit code 1"));
    assert!(msg.contains("[1/3]"));
}

#[test]
fn build_health_check_error_with_exec_error() {
    let msg = build_health_check_error(
        "curl localhost:8080",
        1,
        Some("exec error: foo"),
        "",
        "",
        2,
        3,
    );
    assert!(msg.contains("exec error: foo"));
    assert!(!msg.contains("exit code"));
    assert!(msg.contains("[2/3]"));
}

#[test]
fn build_health_check_error_with_stderr() {
    let msg = build_health_check_error("pg_isready", 2, None, "", "connection refused\n", 1, 5);
    assert!(msg.contains("exit code 2"));
    assert!(msg.contains("(stderr: connection refused)"));
    assert!(msg.contains("[1/5]"));
}

#[test]
fn build_health_check_error_with_stdout_when_no_stderr() {
    let msg = build_health_check_error("check.sh", 1, None, "NOT OK\n", "", 1, 3);
    assert!(msg.contains("(stdout: NOT OK)"));
}

#[test]
fn build_health_check_error_prefers_stderr_over_stdout() {
    let msg = build_health_check_error("check.sh", 1, None, "stdout stuff", "stderr stuff", 1, 3);
    assert!(msg.contains("stderr: stderr stuff"));
    assert!(!msg.contains("stdout stuff"));
}

#[test]
fn build_health_check_error_truncates_long_stderr() {
    let long_stderr = "x".repeat(200);
    let msg = build_health_check_error("cmd", 1, None, "", &long_stderr, 1, 3);
    assert!(msg.contains("..."));
}

// ── Concurrent health check timeout/failure cascade tests ──

#[test]
fn multiple_services_timeout_simultaneously() {
    use std::time::Duration;

    let mut runtime = MockContainerRuntime::new();
    // Both services will time out (exec sleeps 2s, timeout is 1s).
    runtime.exec_delay = Some(Duration::from_secs(2));
    let store = StateStore::in_memory().unwrap();

    let hc = || HealthCheckSpec {
        test: vec!["CMD".to_string(), "slow-cmd".to_string()],
        interval_secs: Some(0),
        timeout_secs: Some(1),
        retries: Some(3),
        start_period_secs: None,
    };

    let spec = stack_with_hc(
        "app",
        vec![
            ServiceSpec {
                healthcheck: Some(hc()),
                ..svc("web")
            },
            ServiceSpec {
                healthcheck: Some(hc()),
                ..svc("api")
            },
        ],
    );

    store
        .save_observed_state("app", &running_obs("web", "ctr-web"))
        .unwrap();
    store
        .save_observed_state("app", &running_obs("api", "ctr-api"))
        .unwrap();

    let mut poller = HealthPoller::new();
    let result = poller.poll_all(&runtime, &store, &spec).unwrap();

    // Both services should have been checked (not blocked by each other).
    assert_eq!(result.checks_run, 2, "both services should be polled");
    assert!(result.newly_ready.is_empty());

    // Both should have 1 consecutive failure each.
    assert_eq!(poller.statuses()["web"].consecutive_failures, 1);
    assert_eq!(poller.statuses()["api"].consecutive_failures, 1);
}

#[test]
fn timeouts_count_toward_retry_exhaustion() {
    use std::time::Duration;

    let mut runtime = MockContainerRuntime::new();
    runtime.exec_delay = Some(Duration::from_secs(2));
    let store = StateStore::in_memory().unwrap();

    let spec = stack_with_hc(
        "app",
        vec![ServiceSpec {
            healthcheck: Some(HealthCheckSpec {
                test: vec!["CMD".to_string(), "slow-cmd".to_string()],
                interval_secs: Some(0),
                timeout_secs: Some(1),
                retries: Some(2), // Exhausts after 2 failures.
                start_period_secs: None,
            }),
            ..svc("web")
        }],
    );

    store
        .save_observed_state("app", &running_obs("web", "ctr-web"))
        .unwrap();

    let mut poller = HealthPoller::new();

    // Round 1: first timeout failure.
    let r1 = poller.poll_all(&runtime, &store, &spec).unwrap();
    assert_eq!(r1.checks_run, 1);
    assert!(r1.newly_failed.is_empty(), "not yet exhausted retries");
    assert_eq!(poller.statuses()["web"].consecutive_failures, 1);

    // Round 2: second timeout failure — retries exhausted.
    let r2 = poller.poll_all(&runtime, &store, &spec).unwrap();
    assert_eq!(r2.checks_run, 1);
    assert_eq!(
        r2.newly_failed.len(),
        1,
        "should report web as newly failed after 2 consecutive timeouts"
    );
    assert_eq!(r2.newly_failed[0], "web");

    // Consecutive failures counter resets after exhaustion (Docker semantics:
    // service keeps running, health checks continue).
    assert_eq!(poller.statuses()["web"].consecutive_failures, 0);
}

#[test]
fn exec_error_counted_as_failure_toward_exhaustion() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_exec = true;
    let store = StateStore::in_memory().unwrap();

    let spec = stack_with_hc(
        "app",
        vec![ServiceSpec {
            healthcheck: Some(HealthCheckSpec {
                test: vec!["CMD".to_string(), "check".to_string()],
                interval_secs: Some(0),
                timeout_secs: Some(3),
                retries: Some(2),
                start_period_secs: None,
            }),
            ..svc("web")
        }],
    );

    store
        .save_observed_state("app", &running_obs("web", "ctr-web"))
        .unwrap();

    let mut poller = HealthPoller::new();

    // Round 1: exec error.
    let r1 = poller.poll_all(&runtime, &store, &spec).unwrap();
    assert_eq!(poller.statuses()["web"].consecutive_failures, 1);
    assert!(r1.newly_failed.is_empty());

    // Round 2: exec error again — retries exhausted.
    let r2 = poller.poll_all(&runtime, &store, &spec).unwrap();
    assert_eq!(r2.newly_failed.len(), 1);
    assert_eq!(r2.newly_failed[0], "web");
}

#[test]
fn service_recovers_after_retry_exhaustion() {
    let mut runtime = MockContainerRuntime::new();
    runtime.exec_exit_code = 1; // Health check fails.
    let store = StateStore::in_memory().unwrap();

    let spec = stack_with_hc(
        "app",
        vec![ServiceSpec {
            healthcheck: Some(HealthCheckSpec {
                test: vec!["CMD".to_string(), "check".to_string()],
                interval_secs: Some(0),
                timeout_secs: Some(3),
                retries: Some(1), // Exhausts after 1 failure.
                start_period_secs: None,
            }),
            ..svc("web")
        }],
    );

    store
        .save_observed_state("app", &running_obs("web", "ctr-web"))
        .unwrap();

    let mut poller = HealthPoller::new();

    // Round 1: health check fails → retries exhausted.
    let r1 = poller.poll_all(&runtime, &store, &spec).unwrap();
    assert_eq!(r1.newly_failed.len(), 1);

    // Now health check passes.
    runtime.exec_exit_code = 0;

    // Round 2: should recover and mark service ready.
    let r2 = poller.poll_all(&runtime, &store, &spec).unwrap();
    assert_eq!(
        r2.newly_ready.len(),
        1,
        "service should recover after retries exhaustion"
    );
    assert_eq!(r2.newly_ready[0], "web");
    assert_eq!(poller.statuses()["web"].consecutive_passes, 1);
    assert_eq!(poller.statuses()["web"].consecutive_failures, 0);
}
