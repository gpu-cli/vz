#![allow(clippy::unwrap_used)]

use super::planning::{compute_actions, compute_actions_with_mount_digests, service_config_digest};
use super::*;
use crate::spec::{MountSpec, ServiceDependency, ServiceKind, StackSpec};

fn svc(name: &str, image: &str) -> ServiceSpec {
    ServiceSpec {
        name: name.to_string(),
        kind: ServiceKind::Service,
        image: image.to_string(),
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
        sysctls: HashMap::new(),
        ulimits: vec![],
        container_name: None,
        hostname: None,
        domainname: None,
        labels: HashMap::new(),
        stop_signal: None,
        stop_grace_period_secs: None,
        expose: vec![],
        stdin_open: false,
        tty: false,
        logging: None,
    }
}

fn svc_with_deps(name: &str, image: &str, deps: Vec<&str>) -> ServiceSpec {
    ServiceSpec {
        depends_on: deps.into_iter().map(ServiceDependency::started).collect(),
        ..svc(name, image)
    }
}

fn svc_with_healthy_deps(name: &str, image: &str, deps: Vec<&str>) -> ServiceSpec {
    ServiceSpec {
        depends_on: deps.into_iter().map(ServiceDependency::healthy).collect(),
        ..svc(name, image)
    }
}

fn svc_with_healthcheck(name: &str, image: &str) -> ServiceSpec {
    use crate::spec::HealthCheckSpec;
    ServiceSpec {
        healthcheck: Some(HealthCheckSpec {
            test: vec!["CMD".to_string(), "true".to_string()],
            interval_secs: Some(5),
            timeout_secs: Some(3),
            retries: Some(3),
            start_period_secs: None,
        }),
        ..svc(name, image)
    }
}

fn spec(name: &str, services: Vec<ServiceSpec>) -> StackSpec {
    StackSpec {
        name: name.to_string(),
        services,
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    }
}

fn no_health() -> HashMap<String, HealthStatus> {
    HashMap::new()
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

fn obs_running(name: &str) -> ServiceObservedState {
    ServiceObservedState {
        service_name: name.to_string(),
        phase: ServicePhase::Running,
        container_id: Some(format!("ctr-{name}")),
        last_error: None,
        ready: true,
    }
}

// ── Diff planner tests ──

#[test]
fn compute_actions_creates_new_services() {
    let desired = vec![svc("web", "nginx:latest"), svc("db", "postgres:16")];
    let observed = vec![];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(actions.len(), 2);
    assert!(deferred.is_empty());
    assert!(
        actions
            .iter()
            .all(|a| matches!(a, Action::ServiceCreate { .. }))
    );
}

#[test]
fn compute_actions_noop_when_converged() {
    let desired = vec![svc("web", "nginx:latest")];
    let observed = vec![obs_running("web")];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    assert!(actions.is_empty());
}

#[test]
fn compute_actions_recreates_running_service_when_mounts_change() {
    let desired = vec![ServiceSpec {
        mounts: vec![MountSpec::Bind {
            source: "/workspace/new".to_string(),
            target: "/workspace".to_string(),
            read_only: false,
        }],
        ..svc("web", "nginx:latest")
    }];
    let previous = vec![ServiceSpec {
        mounts: vec![MountSpec::Bind {
            source: "/workspace/old".to_string(),
            target: "/workspace".to_string(),
            read_only: false,
        }],
        ..svc("web", "nginx:latest")
    }];
    let observed = vec![obs_running("web")];

    let (actions, deferred) =
        compute_actions(&desired, &observed, &no_health(), Some(previous.as_slice()));
    assert_eq!(
        actions,
        vec![Action::ServiceRecreate {
            service_name: "web".to_string(),
        }]
    );
    assert!(deferred.is_empty());
}

#[test]
fn compute_actions_recreates_running_service_when_persisted_digest_changes() {
    let desired = vec![ServiceSpec {
        mounts: vec![MountSpec::Bind {
            source: "/workspace/new".to_string(),
            target: "/workspace".to_string(),
            read_only: false,
        }],
        ..svc("web", "nginx:latest")
    }];
    let observed = vec![obs_running("web")];
    // The stored digest is the full config digest of the old service.
    let old_service = ServiceSpec {
        mounts: vec![MountSpec::Bind {
            source: "/workspace/old".to_string(),
            target: "/workspace".to_string(),
            read_only: false,
        }],
        ..svc("web", "nginx:latest")
    };
    let mut observed_config_digests = HashMap::new();
    observed_config_digests.insert("web".to_string(), service_config_digest(&old_service));

    let (actions, deferred) = compute_actions_with_mount_digests(
        &desired,
        &observed,
        &no_health(),
        None,
        &observed_config_digests,
    );
    assert_eq!(
        actions,
        vec![Action::ServiceRecreate {
            service_name: "web".to_string(),
        }]
    );
    assert!(deferred.is_empty());
}

#[test]
fn compute_actions_keeps_running_service_when_mounts_match_previous_spec() {
    let desired = vec![ServiceSpec {
        mounts: vec![MountSpec::Bind {
            source: "/workspace/src".to_string(),
            target: "/workspace".to_string(),
            read_only: false,
        }],
        ..svc("web", "nginx:latest")
    }];
    let previous = vec![ServiceSpec {
        mounts: vec![MountSpec::Bind {
            source: "/workspace/src".to_string(),
            target: "/workspace".to_string(),
            read_only: false,
        }],
        ..svc("web", "nginx:latest")
    }];
    let observed = vec![obs_running("web")];

    let (actions, deferred) =
        compute_actions(&desired, &observed, &no_health(), Some(previous.as_slice()));
    assert!(actions.is_empty());
    assert!(deferred.is_empty());
}

#[test]
fn compute_actions_removes_extra_services() {
    let desired = vec![svc("web", "nginx:latest")];
    let observed = vec![obs_running("web"), obs_running("old-svc")];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0],
        Action::ServiceRemove {
            service_name: "old-svc".to_string()
        }
    );
}

#[test]
fn compute_actions_recreates_pending_services() {
    let desired = vec![svc("web", "nginx:latest")];
    let observed = vec![obs("web", ServicePhase::Pending)];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0],
        Action::ServiceCreate {
            service_name: "web".to_string()
        }
    );
}

#[test]
fn compute_actions_recreates_failed_services() {
    let desired = vec![svc("web", "nginx:latest")];
    let observed = vec![ServiceObservedState {
        last_error: Some("oom".to_string()),
        ..obs("web", ServicePhase::Failed)
    }];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0],
        Action::ServiceCreate {
            service_name: "web".to_string()
        }
    );
}

#[test]
fn compute_actions_recreates_stopped_services() {
    let desired = vec![svc("web", "nginx:latest")];
    let observed = vec![obs("web", ServicePhase::Stopped)];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(actions.len(), 1);
}

// ── Dependency ordering tests ──

#[test]
fn topo_sort_respects_depends_on() {
    let desired = vec![
        svc_with_deps("web", "nginx:latest", vec!["db"]),
        svc("db", "postgres:16"),
    ];
    let observed = vec![];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);
    let names: Vec<&str> = actions.iter().map(|a| a.service_name()).collect();

    // Strict dependency gating: only db is actionable in the first pass.
    assert_eq!(names, vec!["db"]);
    assert_eq!(deferred.len(), 1);
    assert_eq!(deferred[0].service_name, "web");
}

#[test]
fn topo_sort_chain_dependency() {
    let desired = vec![
        svc_with_deps("app", "myapp:latest", vec!["api"]),
        svc_with_deps("api", "api:latest", vec!["db"]),
        svc("db", "postgres:16"),
    ];
    let observed = vec![];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);
    let names: Vec<&str> = actions.iter().map(|a| a.service_name()).collect();

    // First pass only schedules the root dependency.
    assert_eq!(names, vec!["db"]);
    let mut deferred_names: Vec<&str> = deferred
        .iter()
        .map(|entry| entry.service_name.as_str())
        .collect();
    deferred_names.sort();
    assert_eq!(deferred_names, vec!["api", "app"]);
}

#[test]
fn topo_sort_name_tiebreak_is_deterministic() {
    // Three services with no dependencies — should sort by name.
    let desired = vec![
        svc("charlie", "img:1"),
        svc("alpha", "img:1"),
        svc("bravo", "img:1"),
    ];
    let observed = vec![];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    let names: Vec<&str> = actions.iter().map(|a| a.service_name()).collect();
    assert_eq!(names, vec!["alpha", "bravo", "charlie"]);
}

#[test]
fn topo_sort_is_deterministic_across_calls() {
    let desired = vec![
        svc_with_deps("web", "nginx:latest", vec!["api"]),
        svc_with_deps("api", "api:latest", vec!["db", "cache"]),
        svc("db", "postgres:16"),
        svc("cache", "redis:7"),
    ];
    let observed = vec![];

    let (run1, _) = compute_actions(&desired, &observed, &no_health(), None);
    let (run2, _) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(run1, run2);
}

#[test]
fn topo_sort_removes_dependents_before_dependencies() {
    // When removing, dependents should be removed before dependencies.
    let desired = vec![]; // remove everything
    let observed = vec![obs_running("web"), obs_running("db")];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(actions.len(), 2);
    // Both are removes, sorted by name since no deps in this case.
    assert!(
        actions
            .iter()
            .all(|a| matches!(a, Action::ServiceRemove { .. }))
    );
}

// ── Dependency gating tests ──

#[test]
fn dep_gating_no_healthcheck_waits_for_started_dependency() {
    let desired = vec![
        svc("db", "postgres:16"),
        svc_with_deps("web", "nginx:latest", vec!["db"]),
    ];
    let observed = vec![];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);

    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].service_name(), "db");
    assert_eq!(deferred.len(), 1);
    assert_eq!(deferred[0].service_name, "web");
}

#[test]
fn dep_gating_failed_dep_blocks() {
    // db is Failed → web is deferred.
    let desired = vec![
        svc("db", "postgres:16"),
        svc_with_deps("web", "nginx:latest", vec!["db"]),
    ];
    let observed = vec![obs("db", ServicePhase::Failed)];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);

    // db gets ServiceCreate (recreate from Failed), web is deferred.
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].service_name(), "db");
    assert_eq!(deferred.len(), 1);
    assert_eq!(deferred[0].service_name, "web");
}

#[test]
fn dep_gating_service_started_ignores_healthcheck() {
    // db has a health check and is Running but not yet healthy.
    // With service_started condition (default), web should NOT be blocked.
    let desired = vec![
        svc_with_healthcheck("db", "postgres:16"),
        svc_with_deps("web", "nginx:latest", vec!["db"]),
    ];
    let observed = vec![obs_running("db")];

    // No health status — but condition is service_started, so web is unblocked.
    // db is Running (no action), web is not yet created → ServiceCreate.
    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);

    assert!(deferred.is_empty());
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].service_name(), "web");
}

#[test]
fn dep_gating_service_healthy_blocks_until_healthy() {
    // db has a health check and is Running but not yet healthy.
    // With service_healthy condition, web should be blocked.
    let desired = vec![
        svc_with_healthcheck("db", "postgres:16"),
        svc_with_healthy_deps("web", "nginx:latest", vec!["db"]),
    ];
    let observed = vec![obs_running("db")];

    // No health status → health check hasn't passed → web is deferred.
    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);

    assert!(actions.is_empty()); // db is Running, no action needed.
    assert_eq!(deferred.len(), 1);
    assert_eq!(deferred[0].service_name, "web");
}

#[test]
fn dep_gating_service_healthy_unblocks_when_healthy() {
    // db has a health check and is Running + healthy.
    // With service_healthy condition, web should be unblocked.
    let desired = vec![
        svc_with_healthcheck("db", "postgres:16"),
        svc_with_healthy_deps("web", "nginx:latest", vec!["db"]),
    ];
    let observed = vec![obs_running("db")];

    let mut health = HashMap::new();
    let mut db_health = HealthStatus::new("db");
    db_health.record_pass();
    health.insert("db".to_string(), db_health);

    let (actions, deferred) = compute_actions(&desired, &observed, &health, None);

    // web should now be created.
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].service_name(), "web");
    assert!(deferred.is_empty());
}

#[test]
fn dep_gating_chain_defers_until_dependencies_started() {
    // app → api → db. Only db can start in the first pass.
    let desired = vec![
        svc("db", "postgres:16"),
        svc_with_deps("api", "api:latest", vec!["db"]),
        svc_with_deps("app", "myapp:latest", vec!["api"]),
    ];
    let observed = vec![];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);

    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].service_name(), "db");
    assert_eq!(deferred.len(), 2);
    assert_eq!(deferred[0].service_name, "api");
    assert_eq!(deferred[1].service_name, "app");
}

#[test]
fn dep_gating_no_deps_always_proceeds() {
    let desired = vec![svc("web", "nginx:latest")];
    let observed = vec![];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);

    assert_eq!(actions.len(), 1);
    assert!(deferred.is_empty());
}

// ── Apply integration tests ──

#[test]
fn apply_creates_new_services() {
    let store = StateStore::in_memory().unwrap();
    let s = spec("myapp", vec![svc("web", "nginx:latest")]);

    let result = apply(&s, &store, &no_health()).unwrap();
    assert_eq!(result.actions.len(), 1);
    assert_eq!(
        result.actions[0],
        Action::ServiceCreate {
            service_name: "web".to_string()
        }
    );

    let observed = store.load_observed_state("myapp").unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].phase, ServicePhase::Pending);
}

#[test]
fn apply_is_idempotent_when_running() {
    let store = StateStore::in_memory().unwrap();
    let s = spec("myapp", vec![svc("web", "nginx:latest")]);

    // First apply creates the service.
    apply(&s, &store, &no_health()).unwrap();

    // Simulate the service becoming Running.
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-1".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    // Second apply should produce no actions.
    let result = apply(&s, &store, &no_health()).unwrap();
    assert!(result.actions.is_empty());
}

#[test]
fn apply_removes_deleted_services() {
    let store = StateStore::in_memory().unwrap();

    // Start with two services.
    let s1 = spec(
        "myapp",
        vec![svc("web", "nginx:latest"), svc("db", "postgres:16")],
    );
    apply(&s1, &store, &no_health()).unwrap();

    // Simulate both Running.
    for name in &["web", "db"] {
        store
            .save_observed_state("myapp", &obs_running(name))
            .unwrap();
    }

    // Remove db from the spec.
    let s2 = spec("myapp", vec![svc("web", "nginx:latest")]);
    let result = apply(&s2, &store, &no_health()).unwrap();

    assert_eq!(result.actions.len(), 1);
    assert_eq!(
        result.actions[0],
        Action::ServiceRemove {
            service_name: "db".to_string()
        }
    );
}

#[test]
fn apply_emits_events_for_actions() {
    let store = StateStore::in_memory().unwrap();
    let s = spec("myapp", vec![svc("web", "nginx:latest")]);

    apply(&s, &store, &no_health()).unwrap();

    let events = store.load_events("myapp").unwrap();
    // Started + ServiceCreating + Completed.
    assert_eq!(events.len(), 3);
    assert!(matches!(events[0], StackEvent::StackApplyStarted { .. }));
    assert!(matches!(events[1], StackEvent::ServiceCreating { .. }));
    assert!(matches!(events[2], StackEvent::StackApplyCompleted { .. }));
}

#[test]
fn apply_emits_mount_topology_recreate_required_before_service_creating() {
    let store = StateStore::in_memory().unwrap();

    let s1 = spec(
        "myapp",
        vec![ServiceSpec {
            mounts: vec![MountSpec::Bind {
                source: "/workspace/old".to_string(),
                target: "/workspace".to_string(),
                read_only: false,
            }],
            ..svc("web", "nginx:latest")
        }],
    );
    apply(&s1, &store, &no_health()).unwrap();
    store
        .save_observed_state("myapp", &obs_running("web"))
        .unwrap();

    let s2 = spec(
        "myapp",
        vec![ServiceSpec {
            mounts: vec![MountSpec::Bind {
                source: "/workspace/new".to_string(),
                target: "/workspace".to_string(),
                read_only: false,
            }],
            ..svc("web", "nginx:latest")
        }],
    );
    let result = apply(&s2, &store, &no_health()).unwrap();
    assert_eq!(
        result.actions,
        vec![Action::ServiceRecreate {
            service_name: "web".to_string(),
        }]
    );

    let events = store.load_events("myapp").unwrap();
    let recreate_idx = events
        .iter()
        .position(|event| {
            matches!(
                event,
                StackEvent::MountTopologyRecreateRequired { service_name, .. }
                    if service_name == "web"
            )
        })
        .unwrap();
    let creating_idx = events
            .iter()
            .rposition(
                |event| matches!(event, StackEvent::ServiceCreating { service_name, .. } if service_name == "web"),
            )
            .unwrap();
    assert!(recreate_idx < creating_idx);

    let digests = store.load_service_mount_digests("myapp").unwrap();
    assert_eq!(
        digests.get("web"),
        Some(&service_config_digest(&s2.services[0]))
    );
}

#[test]
fn apply_with_healthcheck_gating_service_healthy() {
    let store = StateStore::in_memory().unwrap();
    // db has a health check; web depends on db with service_healthy condition.
    let s = spec(
        "myapp",
        vec![
            svc_with_healthcheck("db", "postgres:16"),
            svc_with_healthy_deps("web", "nginx:latest", vec!["db"]),
        ],
    );

    // First apply: only db is created, web waits for dependency readiness.
    let r1 = apply(&s, &store, &no_health()).unwrap();
    assert_eq!(r1.actions.len(), 1);
    assert_eq!(r1.actions[0].service_name(), "db");
    assert_eq!(r1.deferred.len(), 1);
    assert_eq!(r1.deferred[0].service_name, "web");

    // Simulate db Running but health check NOT yet passing.
    store
        .save_observed_state("myapp", &obs_running("db"))
        .unwrap();

    // With no health pass, web is still deferred.
    let r2 = apply(&s, &store, &no_health()).unwrap();
    assert!(r2.actions.is_empty());
    assert_eq!(r2.deferred.len(), 1);
    assert_eq!(r2.deferred[0].service_name, "web");

    // Apply with db healthy: web should be created.
    let mut health = HashMap::new();
    let mut db_health = HealthStatus::new("db");
    db_health.record_pass();
    health.insert("db".to_string(), db_health);

    let r3 = apply(&s, &store, &health).unwrap();
    assert_eq!(r3.actions.len(), 1);
    assert_eq!(r3.actions[0].service_name(), "web");
    assert!(r3.deferred.is_empty());
}

#[test]
fn apply_with_healthcheck_service_started_ignores_health() {
    let store = StateStore::in_memory().unwrap();
    // db has a health check; web depends on db with default (service_started) condition.
    let s = spec(
        "myapp",
        vec![
            svc_with_healthcheck("db", "postgres:16"),
            svc_with_deps("web", "nginx:latest", vec!["db"]),
        ],
    );

    // Simulate db Running but health check NOT yet passing.
    store
        .save_observed_state("myapp", &obs_running("db"))
        .unwrap();
    store
        .save_observed_state("myapp", &obs("web", ServicePhase::Stopped))
        .unwrap();

    // Apply with no health status: web should NOT be deferred because
    // service_started doesn't care about health checks.
    let r = apply(&s, &store, &no_health()).unwrap();
    assert_eq!(r.actions.len(), 1);
    assert_eq!(r.actions[0].service_name(), "web");
    assert!(r.deferred.is_empty());
}

#[test]
fn apply_emits_dependency_blocked_events() {
    let store = StateStore::in_memory().unwrap();
    // db is Failed, web depends on db → web is deferred.
    store
        .save_observed_state("myapp", &obs("db", ServicePhase::Failed))
        .unwrap();

    let s = spec(
        "myapp",
        vec![
            svc("db", "postgres:16"),
            svc_with_deps("web", "nginx:latest", vec!["db"]),
        ],
    );

    let result = apply(&s, &store, &no_health()).unwrap();
    assert_eq!(result.deferred.len(), 1);

    let events = store.load_events("myapp").unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::DependencyBlocked { .. }))
    );
}

#[test]
fn apply_determinism_proof() {
    let store1 = StateStore::in_memory().unwrap();
    let store2 = StateStore::in_memory().unwrap();
    let s = spec(
        "myapp",
        vec![svc("db", "postgres:16"), svc("cache", "redis:7")],
    );

    let r1 = apply(&s, &store1, &no_health()).unwrap();
    let r2 = apply(&s, &store2, &no_health()).unwrap();
    assert_eq!(r1.actions, r2.actions);
}

#[test]
fn apply_empty_spec_produces_no_actions() {
    let store = StateStore::in_memory().unwrap();
    let s = spec("myapp", vec![]);

    let result = apply(&s, &store, &no_health()).unwrap();
    assert!(result.actions.is_empty());
}

// ── Reverse-dependency teardown ordering tests ──

#[test]
fn teardown_removes_dependents_before_dependencies() {
    // Chain: C depends on B depends on A.
    // On teardown (empty desired), removal order should be C, B, A.
    let a = svc("a", "img:1");
    let b = svc_with_deps("b", "img:1", vec!["a"]);
    let c = svc_with_deps("c", "img:1", vec!["b"]);
    let previous = vec![a, b, c];

    // All three are currently running.
    let observed = vec![obs_running("a"), obs_running("b"), obs_running("c")];

    // Empty desired = full teardown, with previous deps for ordering.
    let (actions, deferred) = compute_actions(&[], &observed, &no_health(), Some(&previous));

    assert!(deferred.is_empty());
    assert_eq!(actions.len(), 3);
    // All should be removes.
    assert!(
        actions
            .iter()
            .all(|a| matches!(a, Action::ServiceRemove { .. }))
    );
    // Order: c first (depends on b), then b (depends on a), then a.
    let names: Vec<&str> = actions.iter().map(|a| a.service_name()).collect();
    assert_eq!(names, vec!["c", "b", "a"]);
}

#[test]
fn teardown_via_apply_uses_previous_spec_for_ordering() {
    let store = StateStore::in_memory().unwrap();

    // Set up a stack with A -> B -> C dependency chain.
    let a = svc("a", "img:1");
    let b = svc_with_deps("b", "img:1", vec!["a"]);
    let c = svc_with_deps("c", "img:1", vec!["b"]);
    let s = spec("myapp", vec![a, b, c]);

    // First apply creates all three.
    apply(&s, &store, &no_health()).unwrap();

    // Simulate all Running.
    for name in &["a", "b", "c"] {
        store
            .save_observed_state("myapp", &obs_running(name))
            .unwrap();
    }

    // Teardown: empty spec.
    let empty = spec("myapp", vec![]);
    let result = apply(&empty, &store, &no_health()).unwrap();

    assert_eq!(result.actions.len(), 3);
    let names: Vec<&str> = result.actions.iter().map(|a| a.service_name()).collect();
    // Dependents removed first: c, b, a.
    assert_eq!(names, vec!["c", "b", "a"]);
}

#[test]
fn teardown_without_previous_spec_falls_back_to_alphabetical() {
    // No previous spec stored, so no dep info for removals.
    let observed = vec![obs_running("a"), obs_running("b"), obs_running("c")];
    let (actions, _) = compute_actions(&[], &observed, &no_health(), None);

    let names: Vec<&str> = actions.iter().map(|a| a.service_name()).collect();
    // Falls back to alphabetical without dependency info.
    assert_eq!(names, vec!["a", "b", "c"]);
}

// ── Action hash tests ──

#[test]
fn actions_hash_deterministic_same_input() {
    let actions = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
    ];

    let hash1 = compute_actions_hash(&actions);
    let hash2 = compute_actions_hash(&actions);
    assert_eq!(hash1, hash2);
    assert_eq!(hash1.len(), 16, "hash should be 16 hex characters");
}

#[test]
fn actions_hash_differs_for_different_actions() {
    let a = vec![Action::ServiceCreate {
        service_name: "db".to_string(),
    }];
    let b = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    assert_ne!(compute_actions_hash(&a), compute_actions_hash(&b));
}

#[test]
fn actions_hash_differs_for_different_kinds() {
    let a = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];
    let b = vec![Action::ServiceRecreate {
        service_name: "web".to_string(),
    }];
    let c = vec![Action::ServiceRemove {
        service_name: "web".to_string(),
    }];

    let hash_a = compute_actions_hash(&a);
    let hash_b = compute_actions_hash(&b);
    let hash_c = compute_actions_hash(&c);

    assert_ne!(hash_a, hash_b);
    assert_ne!(hash_b, hash_c);
    assert_ne!(hash_a, hash_c);
}

#[test]
fn actions_hash_order_matters() {
    let a = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
    ];
    let b = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];

    assert_ne!(compute_actions_hash(&a), compute_actions_hash(&b));
}

#[test]
fn actions_hash_empty_list() {
    let hash = compute_actions_hash(&[]);
    assert_eq!(hash.len(), 16);
    // Empty list should still produce a valid hash.
}

// ── Replica-aware reconciliation tests ──

fn svc_with_replicas(name: &str, image: &str, replicas: u32) -> ServiceSpec {
    use crate::spec::ResourcesSpec;
    ServiceSpec {
        resources: ResourcesSpec {
            replicas,
            ..Default::default()
        },
        ..svc(name, image)
    }
}

#[test]
fn compute_actions_creates_replicated_service() {
    // replicas=3, no observed → ServiceCreate for the base name.
    let desired = vec![svc_with_replicas("web", "nginx:latest", 3)];
    let observed = vec![];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0],
        Action::ServiceCreate {
            service_name: "web".to_string()
        }
    );
    assert!(deferred.is_empty());
}

#[test]
fn compute_actions_noop_for_converged_replicas() {
    // replicas=3, all 3 Running → no actions.
    let desired = vec![svc_with_replicas("web", "nginx:latest", 3)];
    let observed = vec![
        obs_running("web"),
        obs_running("web-2"),
        obs_running("web-3"),
    ];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);
    assert!(actions.is_empty());
    assert!(deferred.is_empty());
}

#[test]
fn compute_actions_scale_up() {
    // observed has only "web" Running, desired replicas=3 → ServiceCreate.
    let desired = vec![svc_with_replicas("web", "nginx:latest", 3)];
    let observed = vec![obs_running("web")];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0],
        Action::ServiceCreate {
            service_name: "web".to_string()
        }
    );
    assert!(deferred.is_empty());
}

#[test]
fn compute_actions_scale_down() {
    // observed has "web"+"web-2"+"web-3", desired replicas=1 → remove web-2, web-3.
    let desired = vec![svc_with_replicas("web", "nginx:latest", 1)];
    let observed = vec![
        obs_running("web"),
        obs_running("web-2"),
        obs_running("web-3"),
    ];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    let mut remove_names: Vec<&str> = actions
        .iter()
        .filter_map(|a| match a {
            Action::ServiceRemove { service_name } => Some(service_name.as_str()),
            _ => None,
        })
        .collect();
    remove_names.sort();
    assert_eq!(remove_names, vec!["web-2", "web-3"]);
}

#[test]
fn compute_actions_removes_all_replicas() {
    // desired empty, observed has all 3 replicas → remove all.
    let desired: Vec<ServiceSpec> = vec![];
    let observed = vec![
        obs_running("web"),
        obs_running("web-2"),
        obs_running("web-3"),
    ];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(actions.len(), 3);
    assert!(
        actions
            .iter()
            .all(|a| matches!(a, Action::ServiceRemove { .. }))
    );
}

#[test]
fn compute_actions_does_not_remove_replicas_of_running_service() {
    // Replica names like "web-2" should NOT be removed when the service
    // specifies replicas >= 2. This was the original bug.
    let desired = vec![svc_with_replicas("web", "nginx:latest", 3)];
    let observed = vec![
        obs_running("web"),
        obs_running("web-2"),
        obs_running("web-3"),
    ];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    let removes: Vec<&str> = actions
        .iter()
        .filter_map(|a| match a {
            Action::ServiceRemove { service_name } => Some(service_name.as_str()),
            _ => None,
        })
        .collect();
    assert!(removes.is_empty(), "should not remove any running replicas");
}
