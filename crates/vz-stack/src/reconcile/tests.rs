#![allow(clippy::unwrap_used)]

use super::planning::{compute_actions, compute_actions_with_mount_digests, service_config_digest};
use super::*;
use crate::spec::{MountSpec, ServiceDependency, ServiceKind, StackSpec};
use crate::state_store::{StackContainerCreateSelector, StackContainerGenerationBinding};

fn authoritative_store(stack_id: &str) -> StateStore {
    let store = StateStore::in_memory().unwrap();
    crate::reconcile::install_test_planning_authority(&store, stack_id);
    store
}

fn create_selector(
    store: &StateStore,
    stack_id: &str,
    service: &ServiceSpec,
    replica_index: u32,
) -> StackContainerCreateSelector {
    let project = store
        .load_project_state("prj_stack_unit_fixture")
        .unwrap()
        .unwrap();
    let environment = &project.environments[0];
    let machine = &environment.machines[0];
    StackContainerCreateSelector {
        project_id: environment.project_id.clone(),
        environment_id: environment.environment_id.clone(),
        machine_id: machine.machine_id.clone(),
        machine_incarnation_id: machine.incarnation.as_ref().unwrap().incarnation_id.clone(),
        environment_generation: environment.lifecycle_generation,
        stack_id: stack_id.to_string(),
        service_name: service.name.clone(),
        replica_index,
        requested_container_id: format!("ctr-{stack_id}-{}-{replica_index}", service.name),
        definition_digest: environment.definition_digest.clone(),
        action_digest: format!("test-action-{stack_id}-{}-{replica_index}", service.name),
        applied_config_digest: service_config_digest(service),
    }
}

fn publish_running_generation(
    store: &StateStore,
    stack_id: &str,
    service: &ServiceSpec,
    replica_index: u32,
) -> vz_runtime_contract::ContainerGenerationOwnership {
    let selector = create_selector(store, stack_id, service, replica_index);
    let (intent, existing) = store
        .resolve_or_begin_stack_container_create(&selector, 10)
        .unwrap();
    assert!(existing.is_none());
    let ownership = vz_runtime_contract::ContainerGenerationOwnership {
        container_id: selector.requested_container_id,
        generation: u64::from(replica_index),
        stack_id: stack_id.to_string(),
        scope: Some(Box::new(intent.scope.clone())),
    };
    store
        .bind_stack_container_generation(&StackContainerGenerationBinding {
            reservation_id: intent.scope.reservation_id.clone(),
            service_name: service.name.clone(),
            ownership: ownership.clone(),
            bound_at: 11,
        })
        .unwrap();
    store
        .publish_stack_container_create_success(&intent.scope.reservation_id, true, 12)
        .unwrap();
    ownership
}

fn publish_bound_create_failure(
    store: &StateStore,
    stack_id: &str,
    service: &ServiceSpec,
    replica_index: u32,
) -> vz_runtime_contract::ContainerGenerationOwnership {
    let selector = create_selector(store, stack_id, service, replica_index);
    let (intent, existing) = store
        .resolve_or_begin_stack_container_create(&selector, 10)
        .unwrap();
    assert!(existing.is_none());
    let ownership = vz_runtime_contract::ContainerGenerationOwnership {
        container_id: selector.requested_container_id,
        generation: u64::from(replica_index),
        stack_id: stack_id.to_string(),
        scope: Some(Box::new(intent.scope.clone())),
    };
    store
        .bind_stack_container_generation(&StackContainerGenerationBinding {
            reservation_id: intent.scope.reservation_id.clone(),
            service_name: service.name.clone(),
            ownership: ownership.clone(),
            bound_at: 11,
        })
        .unwrap();
    store
        .publish_stack_container_create_failure(&intent.scope.reservation_id, "create failed", 12)
        .unwrap();
    ownership
}

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
        replica: crate::state_store::ServiceReplicaKey::first(name.to_string()).unwrap(),
        applied_config_digest: None,
        phase,
        container_id: None,
        failed_create_ownership: None,
        last_error: None,
        ready: false,
    }
}

fn obs_running(name: &str) -> ServiceObservedState {
    ServiceObservedState {
        replica: crate::state_store::ServiceReplicaKey::first(name.to_string()).unwrap(),
        applied_config_digest: None,
        phase: ServicePhase::Running,
        container_id: Some(format!("ctr-{name}")),
        failed_create_ownership: None,
        last_error: None,
        ready: true,
    }
}

fn obs_running_replica(name: &str, replica_index: u32) -> ServiceObservedState {
    ServiceObservedState {
        replica: crate::state_store::ServiceReplicaKey::new(name, replica_index).unwrap(),
        applied_config_digest: None,
        phase: ServicePhase::Running,
        container_id: Some(format!("ctr-{name}-{replica_index}")),
        failed_create_ownership: None,
        last_error: None,
        ready: true,
    }
}

fn obs_running_for(service: &ServiceSpec, replica_index: u32) -> ServiceObservedState {
    ServiceObservedState {
        replica: crate::state_store::ServiceReplicaKey::new(&service.name, replica_index).unwrap(),
        applied_config_digest: Some(service_config_digest(service)),
        phase: ServicePhase::Running,
        container_id: Some(format!("ctr-{}-{replica_index}", service.name)),
        failed_create_ownership: None,
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
    let observed = vec![obs_running_for(&desired[0], 1)];

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
    let observed = vec![obs_running_for(&previous[0], 1)];

    let (actions, deferred) =
        compute_actions(&desired, &observed, &no_health(), Some(previous.as_slice()));
    assert_eq!(
        actions,
        vec![Action::ServiceRecreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
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
    let mut observed = vec![obs_running("web")];
    // The stored digest is the full config digest of the old service.
    let old_service = ServiceSpec {
        mounts: vec![MountSpec::Bind {
            source: "/workspace/old".to_string(),
            target: "/workspace".to_string(),
            read_only: false,
        }],
        ..svc("web", "nginx:latest")
    };
    observed[0].applied_config_digest = Some(service_config_digest(&old_service));
    let mut observed_config_digests = HashMap::new();
    observed_config_digests.insert("web".to_string(), service_config_digest(&old_service));

    let (actions, deferred) = compute_actions_with_mount_digests(
        &desired,
        &observed,
        &no_health(),
        None,
        &observed_config_digests,
    )
    .unwrap();
    let actions = actions
        .into_iter()
        .map(|draft| draft.into_action(crate::reconcile::test_replica_precondition()))
        .collect::<Vec<_>>();
    assert_eq!(
        actions,
        vec![Action::ServiceRecreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
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
    let observed = vec![obs_running_for(&desired[0], 1)];

    let (actions, deferred) =
        compute_actions(&desired, &observed, &no_health(), Some(previous.as_slice()));
    assert!(actions.is_empty());
    assert!(deferred.is_empty());
}

#[test]
fn compute_actions_removes_extra_services() {
    let desired = vec![svc("web", "nginx:latest")];
    let observed = vec![obs_running_for(&desired[0], 1), obs_running("old-svc")];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0],
        Action::ServiceRemove {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("old-svc".to_string()).unwrap(),
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
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
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
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
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
    let observed = vec![obs_running_for(&desired[0], 1)];

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
    let observed = vec![obs_running_for(&desired[0], 1)];

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
    let observed = vec![obs_running_for(&desired[0], 1)];

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
    let store = authoritative_store("myapp");
    let s = spec("myapp", vec![svc("web", "nginx:latest")]);

    let result = apply(&s, &store, &no_health()).unwrap();
    assert_eq!(result.actions.len(), 1);
    assert!(matches!(
        &result.actions[0],
        Action::ServiceCreate { target, precondition }
            if target == &crate::state_store::ServiceReplicaKey::first("web").unwrap()
                && matches!(precondition.journal_head(), ExpectedJournalHead::NeverJournaled)
                && precondition.workload().stack_id == "myapp"
    ));

    let observed = store.load_observed_state("myapp").unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].phase, ServicePhase::Pending);
}

#[test]
fn apply_is_idempotent_when_running() {
    let store = authoritative_store("myapp");
    let s = spec("myapp", vec![svc("web", "nginx:latest")]);

    // First apply creates the service.
    apply(&s, &store, &no_health()).unwrap();

    // Publish the service Running through exact journal/binding authority.
    publish_running_generation(&store, "myapp", &s.services[0], 1);

    // Second apply should produce no actions.
    let result = apply(&s, &store, &no_health()).unwrap();
    assert!(result.actions.is_empty());
}

#[test]
fn apply_removes_deleted_services() {
    let store = authoritative_store("myapp");

    // Start with two services.
    let s1 = spec(
        "myapp",
        vec![svc("web", "nginx:latest"), svc("db", "postgres:16")],
    );
    apply(&s1, &store, &no_health()).unwrap();

    // Publish both Running through the exact journal/binding authority.
    for service in &s1.services {
        publish_running_generation(&store, "myapp", service, 1);
    }
    let observed_before = store.load_observed_state("myapp").unwrap();

    // Remove db from the spec.
    let s2 = spec("myapp", vec![svc("web", "nginx:latest")]);
    let result = apply(&s2, &store, &no_health()).unwrap();

    assert_eq!(result.actions.len(), 1);
    assert!(matches!(
        &result.actions[0],
        Action::ServiceRemove { target, precondition }
            if target == &crate::state_store::ServiceReplicaKey::first("db").unwrap()
                && matches!(
                    precondition.journal_head(),
                    ExpectedJournalHead::Exact { ownership: Some(_), .. }
                )
                && precondition.workload().stack_id == "myapp"
    ));
    let observed = store.load_observed_state("myapp").unwrap();
    assert_eq!(observed, observed_before);
    let db = observed
        .iter()
        .find(|state| state.replica.service_name == "db")
        .unwrap();
    assert_eq!(db.container_id.as_deref(), Some("ctr-myapp-db-1"));
}

#[test]
fn apply_preserves_unproven_failed_ids_for_cleanup_quarantine() {
    for container_name in [None, Some("global-explicit-id")] {
        let store = authoritative_store("myapp");
        let mut service = svc("web", "nginx:latest");
        service.container_name = container_name.map(str::to_string);
        store
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    replica: crate::state_store::ServiceReplicaKey::first("web".to_string())
                        .unwrap(),
                    applied_config_digest: None,
                    phase: ServicePhase::Failed,
                    container_id: Some("unproven-id".to_string()),
                    failed_create_ownership: None,
                    last_error: Some("create failed".to_string()),
                    ready: false,
                },
            )
            .unwrap();

        let error = apply(&spec("myapp", vec![service]), &store, &no_health()).unwrap_err();
        assert!(error.to_string().contains("without an exact journal head"));
        let observed = store.load_observed_state("myapp").unwrap();
        assert_eq!(observed[0].container_id.as_deref(), Some("unproven-id"));
        assert!(observed[0].failed_create_ownership.is_none());
    }
}

#[test]
fn apply_preserves_successful_runtime_ownership_for_recreate() {
    let store = authoritative_store("myapp");
    let original = svc("web", "nginx:latest");
    store
        .save_desired_state("myapp", &spec("myapp", vec![original.clone()]))
        .unwrap();
    let ownership = publish_running_generation(&store, "myapp", &original, 1);
    let observed_before = store.load_observed_state("myapp").unwrap();

    let result = apply(
        &spec("myapp", vec![svc("web", "different:latest")]),
        &store,
        &no_health(),
    )
    .unwrap();

    assert_eq!(result.actions.len(), 1);
    assert!(matches!(result.actions[0], Action::ServiceRecreate { .. }));
    let observed = store.load_observed_state("myapp").unwrap();
    assert_eq!(observed, observed_before);
    assert_eq!(
        observed[0].container_id.as_deref(),
        Some(ownership.container_id.as_str())
    );
    assert_eq!(observed[0].failed_create_ownership, Some(ownership));
}

#[test]
fn apply_preserves_runtime_ownership_independent_of_container_name() {
    for container_name in [None, Some("global-explicit-id")] {
        let store = authoritative_store("myapp");
        let mut service = svc("web", "nginx:latest");
        service.container_name = container_name.map(str::to_string);
        let ownership = publish_bound_create_failure(&store, "myapp", &service, 1);
        let observed_before = store.load_observed_state("myapp").unwrap();

        let result = apply(&spec("myapp", vec![service]), &store, &no_health()).unwrap();
        assert_eq!(result.actions.len(), 1);
        let observed = store.load_observed_state("myapp").unwrap();
        assert_eq!(observed, observed_before);
        assert_eq!(
            observed[0].container_id.as_deref(),
            Some(ownership.container_id.as_str())
        );
        assert_eq!(observed[0].failed_create_ownership, Some(ownership));
    }
}

#[test]
fn apply_emits_events_for_actions() {
    let store = authoritative_store("myapp");
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
fn apply_exact_mount_recreate_preserves_observed_until_claimed_execution() {
    let store = authoritative_store("myapp");

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
    publish_running_generation(&store, "myapp", &s1.services[0], 1);
    let observed_before = store.load_observed_state("myapp").unwrap();
    let events_before = store.load_events("myapp").unwrap();
    let recreate_event_count_before = events_before
        .iter()
        .filter(|event| {
            matches!(
                event,
                StackEvent::MountTopologyRecreateRequired { service_name, .. }
                    if service_name == "web"
            )
        })
        .count();
    let creating_event_count_before = events_before
        .iter()
        .filter(|event| {
            matches!(
                event,
                StackEvent::ServiceCreating { service_name, .. }
                    if service_name == "web"
            )
        })
        .count();

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
    assert!(matches!(
        result.actions.as_slice(),
        [Action::ServiceRecreate { target, precondition }]
            if target == &crate::state_store::ServiceReplicaKey::first("web").unwrap()
                && matches!(
                    precondition.journal_head(),
                    ExpectedJournalHead::Exact { ownership: Some(_), .. }
                )
                && precondition.workload().stack_id == "myapp"
    ));

    assert_eq!(store.load_observed_state("myapp").unwrap(), observed_before);
    let events = store.load_events("myapp").unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    StackEvent::MountTopologyRecreateRequired { service_name, .. }
                        if service_name == "web"
                )
            })
            .count(),
        recreate_event_count_before + 1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    StackEvent::ServiceCreating { service_name, .. }
                        if service_name == "web"
                )
            })
            .count(),
        creating_event_count_before
    );

    let digests = store.load_service_mount_digests("myapp").unwrap();
    assert_eq!(
        digests.get("web"),
        Some(&service_config_digest(&s1.services[0]))
    );
}

#[test]
fn apply_with_healthcheck_gating_service_healthy() {
    let store = authoritative_store("myapp");
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

    // Publish db Running but leave its health check NOT yet passing.
    publish_running_generation(&store, "myapp", &s.services[0], 1);

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
    let store = authoritative_store("myapp");
    // db has a health check; web depends on db with default (service_started) condition.
    let s = spec(
        "myapp",
        vec![
            svc_with_healthcheck("db", "postgres:16"),
            svc_with_deps("web", "nginx:latest", vec!["db"]),
        ],
    );

    // Publish db Running but leave its health check NOT yet passing.
    publish_running_generation(&store, "myapp", &s.services[0], 1);
    // Apply with no health status: web should NOT be deferred because
    // service_started doesn't care about health checks.
    let r = apply(&s, &store, &no_health()).unwrap();
    assert_eq!(r.actions.len(), 1);
    assert_eq!(r.actions[0].service_name(), "web");
    assert!(r.deferred.is_empty());
}

#[test]
fn apply_emits_dependency_blocked_events() {
    let store = authoritative_store("myapp");
    // db is absent, so web is deferred while db itself is planned for creation.
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
    let store1 = authoritative_store("myapp");
    let store2 = authoritative_store("myapp");
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
    let store = authoritative_store("myapp");
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
    let store = authoritative_store("myapp");

    // Set up a stack with A -> B -> C dependency chain.
    let a = svc("a", "img:1");
    let b = svc_with_deps("b", "img:1", vec!["a"]);
    let c = svc_with_deps("c", "img:1", vec!["b"]);
    let s = spec("myapp", vec![a, b, c]);

    // First apply creates all three.
    apply(&s, &store, &no_health()).unwrap();

    // Publish all Running through exact journal/binding authority.
    for service in &s.services {
        publish_running_generation(&store, "myapp", service, 1);
    }
    let observed_before = store.load_observed_state("myapp").unwrap();

    // Teardown: empty spec.
    let empty = spec("myapp", vec![]);
    let result = apply(&empty, &store, &no_health()).unwrap();

    assert_eq!(result.actions.len(), 3);
    let names: Vec<&str> = result.actions.iter().map(|a| a.service_name()).collect();
    // Dependents removed first: c, b, a.
    assert_eq!(names, vec!["c", "b", "a"]);
    assert_eq!(store.load_observed_state("myapp").unwrap(), observed_before);
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
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
    ];

    let hash1 = compute_actions_hash(&actions);
    let hash2 = compute_actions_hash(&actions);
    assert_eq!(hash1, hash2);
    assert!(hash1.starts_with("vzrah2-sha256:"));
    assert_eq!(hash1.len(), "vzrah2-sha256:".len() + 64);
}

#[test]
fn actions_hash_differs_for_different_actions() {
    let a = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
    }];
    let b = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    assert_ne!(compute_actions_hash(&a), compute_actions_hash(&b));
}

#[test]
fn actions_hash_differs_for_different_kinds() {
    let a = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    let b = vec![Action::ServiceRecreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    let c = vec![Action::ServiceRemove {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
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
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
    ];
    let b = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];

    assert_ne!(compute_actions_hash(&a), compute_actions_hash(&b));
}

#[test]
fn actions_hash_empty_list() {
    let hash = compute_actions_hash(&[]);
    assert!(hash.starts_with("vzrah2-sha256:"));
    assert_eq!(hash.len(), "vzrah2-sha256:".len() + 64);
    // Empty list should still produce a valid hash.
}

fn action_hash_workload(
    project_id: &str,
    environment_id: &str,
    machine_id: &str,
    incarnation_id: &str,
    stack_id: &str,
) -> MachineWorkloadScope {
    MachineWorkloadScope {
        schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
        project_id: vz_runtime_contract::ProjectId::new(project_id).unwrap(),
        environment_id: vz_runtime_contract::EnvironmentId::new(environment_id).unwrap(),
        machine_id: vz_runtime_contract::MachineId::new(machine_id).unwrap(),
        machine_incarnation_id: vz_runtime_contract::MachineIncarnationId::new(incarnation_id)
            .unwrap(),
        stack_id: stack_id.to_string(),
    }
}

fn action_hash_for_precondition(precondition: ReplicaPrecondition) -> String {
    compute_actions_hash(&[Action::ServiceCreate {
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        precondition,
    }])
}

#[test]
fn actions_hash_covers_topology_and_journal_precondition_identity() {
    let base_workload = action_hash_workload(
        "prj_hash_a",
        "env_hash_a",
        "mch_hash_a",
        "inc_hash_a",
        "hash-stack-a",
    );
    let base = ReplicaPrecondition::new(
        base_workload.clone(),
        7,
        ExpectedJournalHead::NeverJournaled,
    )
    .unwrap();
    let base_hash = action_hash_for_precondition(base);

    for changed_workload in [
        action_hash_workload(
            "prj_hash_b",
            "env_hash_a",
            "mch_hash_a",
            "inc_hash_a",
            "hash-stack-a",
        ),
        action_hash_workload(
            "prj_hash_a",
            "env_hash_b",
            "mch_hash_a",
            "inc_hash_a",
            "hash-stack-a",
        ),
        action_hash_workload(
            "prj_hash_a",
            "env_hash_a",
            "mch_hash_b",
            "inc_hash_a",
            "hash-stack-a",
        ),
        action_hash_workload(
            "prj_hash_a",
            "env_hash_a",
            "mch_hash_a",
            "inc_hash_b",
            "hash-stack-a",
        ),
        action_hash_workload(
            "prj_hash_a",
            "env_hash_a",
            "mch_hash_a",
            "inc_hash_a",
            "hash-stack-b",
        ),
    ] {
        let changed =
            ReplicaPrecondition::new(changed_workload, 7, ExpectedJournalHead::NeverJournaled)
                .unwrap();
        assert_ne!(base_hash, action_hash_for_precondition(changed));
    }

    let changed_generation = ReplicaPrecondition::new(
        base_workload.clone(),
        8,
        ExpectedJournalHead::NeverJournaled,
    )
    .unwrap();
    assert_ne!(base_hash, action_hash_for_precondition(changed_generation));

    let unbound = ReplicaPrecondition::new(
        base_workload.clone(),
        7,
        ExpectedJournalHead::exact("reservation-a", 1, None).unwrap(),
    )
    .unwrap();
    let changed_reservation = ReplicaPrecondition::new(
        base_workload.clone(),
        7,
        ExpectedJournalHead::exact("reservation-b", 1, None).unwrap(),
    )
    .unwrap();
    let changed_service_generation = ReplicaPrecondition::new(
        base_workload.clone(),
        7,
        ExpectedJournalHead::exact("reservation-a", 2, None).unwrap(),
    )
    .unwrap();
    assert_ne!(base_hash, action_hash_for_precondition(unbound.clone()));
    assert_ne!(
        action_hash_for_precondition(unbound.clone()),
        action_hash_for_precondition(changed_reservation)
    );
    assert_ne!(
        action_hash_for_precondition(unbound),
        action_hash_for_precondition(changed_service_generation)
    );

    let scope = base_workload
        .container_generation_scope("reservation-a")
        .unwrap();
    let ownership = ContainerGenerationOwnership {
        container_id: "ctr-a".to_string(),
        generation: 3,
        stack_id: base_workload.stack_id.clone(),
        scope: Some(Box::new(scope)),
    };
    let bound = ReplicaPrecondition::new(
        base_workload.clone(),
        7,
        ExpectedJournalHead::exact("reservation-a", 1, Some(ownership.clone())).unwrap(),
    )
    .unwrap();
    let mut changed_container = ownership.clone();
    changed_container.container_id = "ctr-b".to_string();
    let mut changed_runtime_generation = ownership;
    changed_runtime_generation.generation = 4;
    for changed_ownership in [changed_container, changed_runtime_generation] {
        let changed = ReplicaPrecondition::new(
            base_workload.clone(),
            7,
            ExpectedJournalHead::exact("reservation-a", 1, Some(changed_ownership)).unwrap(),
        )
        .unwrap();
        assert_ne!(
            action_hash_for_precondition(bound.clone()),
            action_hash_for_precondition(changed)
        );
    }
}

#[test]
fn replica_precondition_rejects_unsafe_journal_identity_and_unknown_fields() {
    for reservation_id in [
        " leading",
        "trailing ",
        "has space",
        "line\nbreak",
        "nul\0byte",
        "unicode\u{2003}space",
    ] {
        assert!(ExpectedJournalHead::exact(reservation_id, 1, None).is_err());
    }
    assert!(ExpectedJournalHead::exact("reservation", 0, None).is_err());

    let precondition = ReplicaPrecondition::new(
        action_hash_workload(
            "prj_serde",
            "env_serde",
            "mch_serde",
            "inc_serde",
            "serde-stack",
        ),
        0,
        ExpectedJournalHead::NeverJournaled,
    )
    .unwrap();
    let mut value = serde_json::to_value(&precondition).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("unexpected".to_string(), serde_json::json!(true));
    assert!(serde_json::from_value::<ReplicaPrecondition>(value).is_err());

    let mut head = serde_json::to_value(ExpectedJournalHead::NeverJournaled).unwrap();
    head.as_object_mut()
        .unwrap()
        .insert("unexpected".to_string(), serde_json::json!(true));
    assert!(serde_json::from_value::<ExpectedJournalHead>(head).is_err());

    let bound = crate::reconcile::test_replica_precondition_for_stack("serde-stack");
    let mut legacy_scope: serde_json::Value = serde_json::to_value(&bound).unwrap();
    legacy_scope["journal_head"]["ownership"]["scope"]["machine_incarnation_id"] =
        serde_json::Value::Null;
    let error = serde_json::from_value::<ReplicaPrecondition>(legacy_scope).unwrap_err();
    assert!(error.to_string().contains("machine incarnation"));
}

#[test]
fn actions_hash_distinguishes_exact_replica_identity() {
    let second_api_replica = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::new("api", 2).unwrap(),
    }];
    let similarly_decorated_base = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::new("api-2", 1).unwrap(),
    }];

    assert_ne!(
        compute_actions_hash(&second_api_replica),
        compute_actions_hash(&similarly_decorated_base)
    );
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
    // replicas=3, no observed → one exact create per replica.
    let desired = vec![svc_with_replicas("web", "nginx:latest", 3)];
    let observed = vec![];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(actions.len(), 3);
    assert_eq!(
        actions,
        (1..=3)
            .map(|replica_index| Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::new("web", replica_index).unwrap(),
            })
            .collect::<Vec<_>>()
    );
    assert!(deferred.is_empty());
}

#[test]
fn compute_actions_noop_for_converged_replicas() {
    // replicas=3, all 3 Running → no actions.
    let desired = vec![svc_with_replicas("web", "nginx:latest", 3)];
    let observed = vec![
        obs_running_for(&desired[0], 1),
        obs_running_for(&desired[0], 2),
        obs_running_for(&desired[0], 3),
    ];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);
    assert!(actions.is_empty());
    assert!(deferred.is_empty());
}

#[test]
fn explicit_container_name_does_not_replace_observed_service_identity() {
    let mut service = svc_with_replicas("web", "nginx:latest", 2);
    service.container_name = Some("globally-explicit-web".to_string());
    let observed = vec![obs_running_for(&service, 1), obs_running_for(&service, 2)];

    let (actions, deferred) = compute_actions(&[service], &observed, &no_health(), None);

    assert!(actions.is_empty());
    assert!(deferred.is_empty());
}

#[test]
fn compute_actions_scale_up() {
    // observed has only "web" Running, desired replicas=3 → ServiceCreate.
    let desired = vec![svc_with_replicas("web", "nginx:latest", 3)];
    let observed = vec![obs_running_for(&desired[0], 1)];

    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(actions.len(), 2);
    assert_eq!(
        actions,
        vec![
            Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::new("web", 2).unwrap(),
            },
            Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::new("web", 3).unwrap(),
            },
        ]
    );
    assert!(deferred.is_empty());
}

#[test]
fn compute_actions_scale_down() {
    // observed has "web"+"web-2"+"web-3", desired replicas=1 → remove web-2, web-3.
    let desired = vec![svc_with_replicas("web", "nginx:latest", 1)];
    let observed = vec![
        obs_running("web"),
        obs_running_replica("web", 2),
        obs_running_replica("web", 3),
    ];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    let mut remove_indices: Vec<u32> = actions
        .iter()
        .filter_map(|a| match a {
            Action::ServiceRemove { target, .. } => Some(target.index()),
            _ => None,
        })
        .collect();
    remove_indices.sort_unstable();
    assert_eq!(remove_indices, vec![2, 3]);
}

#[test]
fn compute_actions_removes_all_replicas() {
    // desired empty, observed has all 3 replicas → remove all.
    let desired: Vec<ServiceSpec> = vec![];
    let observed = vec![
        obs_running("web"),
        obs_running_replica("web", 2),
        obs_running_replica("web", 3),
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
        obs_running_for(&desired[0], 1),
        obs_running_for(&desired[0], 2),
        obs_running_for(&desired[0], 3),
    ];

    let (actions, _) = compute_actions(&desired, &observed, &no_health(), None);
    let removes: Vec<&str> = actions
        .iter()
        .filter_map(|a| match a {
            Action::ServiceRemove { target, .. } => Some(target.service_name.as_str()),
            _ => None,
        })
        .collect();
    assert!(removes.is_empty(), "should not remove any running replicas");
    assert!(actions.is_empty());
}

#[test]
fn compute_actions_creates_only_missing_middle_replica() {
    let desired = vec![svc_with_replicas("api", "api:v1", 3)];
    let observed = vec![
        obs_running_for(&desired[0], 1),
        obs_running_for(&desired[0], 3),
    ];
    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), None);
    assert_eq!(
        actions,
        vec![Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: ServiceReplicaKey::new("api", 2).unwrap(),
        }]
    );
    assert!(deferred.is_empty());
}

#[test]
fn compute_actions_recreates_each_stale_config_replica() {
    let old = svc_with_replicas("api", "api:v1", 3);
    let desired = vec![svc_with_replicas("api", "api:v2", 3)];
    let observed = (1..=3)
        .map(|index| obs_running_for(&old, index))
        .collect::<Vec<_>>();
    let (actions, deferred) = compute_actions(&desired, &observed, &no_health(), Some(&[old]));
    assert_eq!(
        actions,
        (1..=3)
            .map(|index| Action::ServiceRecreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: ServiceReplicaKey::new("api", index).unwrap(),
            })
            .collect::<Vec<_>>()
    );
    assert!(deferred.is_empty());
}

#[test]
fn planner_keeps_suffix_ambiguous_exact_targets_distinct() {
    let desired = vec![
        svc_with_replicas("api", "api:v1", 2),
        svc("api-2", "worker:v1"),
    ];
    let (actions, _) = compute_actions(&desired, &[], &no_health(), None);
    assert!(
        actions
            .iter()
            .any(|action| { action.target() == &ServiceReplicaKey::new("api", 2).unwrap() })
    );
    assert!(
        actions
            .iter()
            .any(|action| { action.target() == &ServiceReplicaKey::new("api-2", 1).unwrap() })
    );
    assert_eq!(actions.len(), 3);
}

#[test]
fn planner_rejects_cycle_even_when_all_replicas_are_running() {
    let desired = vec![
        svc_with_deps("a", "a:v1", vec!["b"]),
        svc_with_deps("b", "b:v1", vec!["a"]),
    ];
    let observed = vec![
        obs_running_for(&desired[0], 1),
        obs_running_for(&desired[1], 1),
    ];
    let error = compute_actions_with_mount_digests(
        &desired,
        &observed,
        &no_health(),
        None,
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("dependency cycle"));
}

#[test]
fn planner_rejects_cyclic_teardown_instead_of_returning_partial_actions() {
    let previous = vec![
        svc_with_deps("a", "a:v1", vec!["b"]),
        svc_with_deps("b", "b:v1", vec!["a"]),
    ];
    let observed = vec![obs_running("a"), obs_running("b")];
    let error = compute_actions_with_mount_digests(
        &[],
        &observed,
        &no_health(),
        Some(&previous),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("dependency cycle"));
}

#[test]
fn targeted_action_planner_preserves_exact_replica_two_identity() {
    let store = authoritative_store("targeted");
    let target = ServiceReplicaKey::new("api", 2).unwrap();

    let action = store
        .plan_targeted_action("targeted", TargetedActionKind::Create, target.clone())
        .unwrap();

    assert!(matches!(
        action,
        Action::ServiceCreate {
            target: planned,
            precondition,
        } if planned == target
            && precondition.workload().stack_id == "targeted"
            && precondition.journal_head() == &ExpectedJournalHead::NeverJournaled
    ));
    assert!(store.load_observed_state("targeted").unwrap().is_empty());
}

#[test]
fn targeted_action_planner_rejects_missing_remove_and_recreate() {
    let store = authoritative_store("targeted-missing");
    let target = ServiceReplicaKey::first("api").unwrap();

    for kind in [TargetedActionKind::Remove, TargetedActionKind::Recreate] {
        let error = store
            .plan_targeted_action("targeted-missing", kind, target.clone())
            .unwrap_err();
        assert!(error.to_string().contains("missing replica `api`"));
    }
    assert!(
        store
            .load_observed_state("targeted-missing")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn targeted_action_planner_captures_exact_running_head_without_mutation() {
    let store = authoritative_store("targeted-running");
    let service = svc_with_replicas("api", "api:v1", 2);
    let ownership = publish_running_generation(&store, "targeted-running", &service, 2);
    let before = store.load_observed_state("targeted-running").unwrap();

    let action = store
        .plan_targeted_action(
            "targeted-running",
            TargetedActionKind::Remove,
            ServiceReplicaKey::new("api", 2).unwrap(),
        )
        .unwrap();

    assert!(matches!(
        action,
        Action::ServiceRemove {
            target,
            precondition,
        } if target == ServiceReplicaKey::new("api", 2).unwrap()
            && matches!(
                precondition.journal_head(),
                ExpectedJournalHead::Exact {
                    ownership: Some(actual),
                    ..
                } if actual == &ownership
            )
    ));
    assert_eq!(
        store.load_observed_state("targeted-running").unwrap(),
        before
    );
}
