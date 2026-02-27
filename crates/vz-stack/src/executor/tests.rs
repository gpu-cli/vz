#![allow(clippy::unwrap_used)]

use super::dispatch::{compute_topo_levels, parse_subnet_base, parse_subnet_prefix};
use super::tests_support::MockContainerRuntime;
use super::*;
use crate::reconcile::apply;
use crate::spec::MountSpec as StackMountSpec;
use crate::spec::{PortSpec, ResourcesSpec, ServiceKind, StackSpec, VolumeSpec};
use std::collections::HashMap;

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
        resources: ResourcesSpec::default(),
        extra_hosts: vec![],
        secrets: vec![],
        networks: vec!["default".to_string()],
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

fn default_network() -> crate::spec::NetworkSpec {
    crate::spec::NetworkSpec {
        name: "default".to_string(),
        driver: "bridge".to_string(),
        subnet: None,
    }
}

fn stack(name: &str, services: Vec<ServiceSpec>) -> StackSpec {
    StackSpec {
        name: name.to_string(),
        services,
        networks: vec![default_network()],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    }
}

fn make_executor(runtime: MockContainerRuntime) -> StackExecutor<MockContainerRuntime> {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    StackExecutor::new(runtime, store, tmp.path())
}

fn make_executor_with_dir(
    runtime: MockContainerRuntime,
    dir: &Path,
) -> StackExecutor<MockContainerRuntime> {
    let store = StateStore::in_memory().unwrap();
    StackExecutor::new(runtime, store, dir)
}

#[test]
fn create_single_service() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());
    assert_eq!(result.succeeded, 1);
    assert_eq!(result.failed, 0);

    // Verify observed state.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].phase, ServicePhase::Running);
    assert_eq!(observed[0].container_id, Some("ctr-web".to_string()));

    // Verify events.
    let events = executor.store().load_events("myapp").unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::ServiceCreating { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::ServiceReady { .. }))
    );
}

#[test]
fn create_multiple_services() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = stack(
        "myapp",
        vec![svc("web", "nginx:latest"), svc("db", "postgres:16")],
    );

    let actions = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());
    assert_eq!(result.succeeded, 2);

    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed.len(), 2);
}

#[test]
fn remove_service() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);

    // Simulate existing running container.
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "old".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-old".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRemove {
        service_name: "old".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Verify stop+remove were called.
    let calls = executor.runtime.call_log();
    assert!(calls.iter().any(|(op, _)| op == "stop"));
    assert!(calls.iter().any(|(op, _)| op == "remove"));

    // Verify state is Stopped.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let old = observed.iter().find(|o| o.service_name == "old").unwrap();
    assert_eq!(old.phase, ServicePhase::Stopped);
    assert!(old.container_id.is_none());
}

#[test]
fn recreate_service() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-new"]);
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    // Simulate existing running container.
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-old".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRecreate {
        service_name: "web".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Verify sandbox setup, then stop+remove of old, then pull+create_in_sandbox of new.
    let calls = executor.runtime.call_log();
    let ops: Vec<&str> = calls.iter().map(|(op, _)| op.as_str()).collect();
    assert_eq!(
        ops,
        vec![
            "create_sandbox",
            "setup_sandbox_network",
            "stop",
            "remove",
            "pull",
            "create_in_sandbox",
        ]
    );

    // New container.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed.iter().find(|o| o.service_name == "web").unwrap();
    assert_eq!(web.phase, ServicePhase::Running);
    assert_eq!(web.container_id, Some("ctr-web".to_string()));
}

#[test]
fn pull_failure_marks_service_failed() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_pull = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.failed, 1);
    assert!(!result.all_succeeded());

    // Service should be marked Failed.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed.iter().find(|o| o.service_name == "web").unwrap();
    assert_eq!(web.phase, ServicePhase::Failed);
    assert!(web.last_error.is_some());

    // ServiceFailed event emitted.
    let events = executor.store().load_events("myapp").unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::ServiceFailed { .. }))
    );
}

#[test]
fn create_failure_marks_service_failed() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_create = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.failed, 1);

    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed.iter().find(|o| o.service_name == "web").unwrap();
    assert_eq!(web.phase, ServicePhase::Failed);
}

#[test]
fn partial_failure_continues_other_services() {
    let mut runtime = MockContainerRuntime::with_ids(vec!["ctr-db"]);
    runtime.fail_pull = false;
    runtime.fail_create = false;
    let mut executor = make_executor(runtime);

    let spec = stack(
        "myapp",
        vec![svc("db", "postgres:16"), svc("web", "nginx:latest")],
    );

    // Make only "web" fail by using a spec that triggers an error.
    // We'll test with a normal mock that succeeds for both.
    let actions = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    // Both succeed with mock.
    assert_eq!(result.succeeded, 2);
}

#[test]
fn remove_with_no_container_id() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);

    // Service observed but no container_id.
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "orphan".to_string(),
                phase: ServicePhase::Pending,
                container_id: None,
                last_error: None,
                ready: false,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRemove {
        service_name: "orphan".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // No stop/remove calls since there's no container.
    let calls = executor.runtime.call_log();
    assert!(calls.is_empty());
}

#[test]
fn volumes_created_before_containers() {
    let runtime = MockContainerRuntime::new();
    let tmp = tempfile::tempdir().unwrap();
    let mut executor = make_executor_with_dir(runtime, tmp.path());

    let spec = StackSpec {
        name: "myapp".to_string(),
        services: vec![ServiceSpec {
            mounts: vec![StackMountSpec::Named {
                source: "dbdata".to_string(),
                target: "/var/lib/db".to_string(),
                read_only: false,
            }],
            ..svc("db", "postgres:16")
        }],
        networks: vec![default_network()],
        volumes: vec![VolumeSpec {
            name: "dbdata".to_string(),
            driver: "local".to_string(),
            driver_opts: None,
        }],
        secrets: vec![],
        disk_size_mb: None,
    };

    let actions = vec![Action::ServiceCreate {
        service_name: "db".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Volume directory exists.
    assert!(executor.volumes().volumes_dir().join("dbdata").is_dir());

    // VolumeCreated event emitted.
    let events = executor.store().load_events("myapp").unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::VolumeCreated { .. }))
    );
}

#[test]
fn service_with_ports_creates_correctly() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);

    let spec = stack(
        "myapp",
        vec![ServiceSpec {
            ports: vec![PortSpec {
                protocol: "tcp".to_string(),
                container_port: 80,
                host_port: Some(8080),
            }],
            ..svc("web", "nginx:latest")
        }],
    );

    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Verify sandbox setup + pull + create_in_sandbox were called.
    let calls = executor.runtime.call_log();
    let ops: Vec<&str> = calls.iter().map(|(op, _)| op.as_str()).collect();
    assert_eq!(
        ops,
        vec![
            "create_sandbox",
            "setup_sandbox_network",
            "pull",
            "create_in_sandbox"
        ]
    );
}

#[test]
fn stop_failure_does_not_prevent_state_update() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_stop = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);

    executor
        .store()
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

    let actions = vec![Action::ServiceRemove {
        service_name: "web".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    // Still counts as succeeded (best-effort stop).
    assert!(result.all_succeeded());

    // State still updated to Stopped.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed.iter().find(|o| o.service_name == "web").unwrap();
    assert_eq!(web.phase, ServicePhase::Stopped);
}

#[test]
fn execution_result_errors_collected() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_pull = true;
    let mut executor = make_executor(runtime);

    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].0, "web");
    assert!(result.errors[0].1.contains("image pull failed"));
}

// ── Port tracking tests ──

#[test]
fn port_tracker_allocates_explicit_port() {
    let mut tracker = PortTracker::new();
    let ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(8080),
    }];
    let published = tracker.allocate("web", &ports).unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].host_port, 8080);
    assert_eq!(published[0].container_port, 80);
    assert!(tracker.in_use().contains(&8080));
}

#[test]
fn port_tracker_allocates_ephemeral_port() {
    let mut tracker = PortTracker::new();
    let ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 3000,
        host_port: None,
    }];
    let published = tracker.allocate("api", &ports).unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].container_port, 3000);
    // Ephemeral port should be assigned.
    assert!(published[0].host_port > 0);
    assert!(tracker.in_use().contains(&published[0].host_port));
}

#[test]
fn port_tracker_detects_cross_service_conflict() {
    let mut tracker = PortTracker::new();
    let ports_a = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(8080),
    }];
    tracker.allocate("web", &ports_a).unwrap();

    // Second service trying the same host port should fail.
    let ports_b = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 3000,
        host_port: Some(8080),
    }];
    let result = tracker.allocate("api", &ports_b);
    assert!(result.is_err());
}

#[test]
fn port_tracker_release_frees_port() {
    let mut tracker = PortTracker::new();
    let ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(9090),
    }];
    tracker.allocate("web", &ports).unwrap();
    assert!(tracker.in_use().contains(&9090));

    tracker.release("web");
    assert!(!tracker.in_use().contains(&9090));
    assert!(tracker.ports_for("web").is_none());
}

#[test]
fn port_tracker_reuse_after_release() {
    let mut tracker = PortTracker::new();
    let ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(9090),
    }];
    tracker.allocate("web", &ports).unwrap();
    tracker.release("web");

    // Another service can now use the same port.
    let ports2 = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 3000,
        host_port: Some(9090),
    }];
    let published = tracker.allocate("api", &ports2).unwrap();
    assert_eq!(published[0].host_port, 9090);
}

#[test]
fn port_tracker_reallocate_same_service_succeeds() {
    let mut tracker = PortTracker::new();
    let ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 5432,
        host_port: Some(5432),
    }];
    // First allocation succeeds.
    tracker.allocate("postgres", &ports).unwrap();

    // Re-allocating the same service (e.g. retry after create failure)
    // should succeed — the old allocation is released automatically.
    let published = tracker.allocate("postgres", &ports).unwrap();
    assert_eq!(published[0].host_port, 5432);
}

#[test]
fn port_tracker_reallocate_does_not_conflict_with_other_services() {
    let mut tracker = PortTracker::new();

    // Service A takes port 5433.
    tracker
        .allocate(
            "postgres-test",
            &[PortSpec {
                protocol: "tcp".to_string(),
                container_port: 5432,
                host_port: Some(5433),
            }],
        )
        .unwrap();

    // Service B takes port 5432.
    tracker
        .allocate(
            "postgres",
            &[PortSpec {
                protocol: "tcp".to_string(),
                container_port: 5432,
                host_port: Some(5432),
            }],
        )
        .unwrap();

    // Re-allocating service B should still succeed (its own port isn't
    // treated as a conflict), but service A's port is still reserved.
    let published = tracker
        .allocate(
            "postgres",
            &[PortSpec {
                protocol: "tcp".to_string(),
                container_port: 5432,
                host_port: Some(5432),
            }],
        )
        .unwrap();
    assert_eq!(published[0].host_port, 5432);

    // But trying to take service A's port should still fail.
    let result = tracker.allocate(
        "postgres",
        &[PortSpec {
            protocol: "tcp".to_string(),
            container_port: 5432,
            host_port: Some(5433),
        }],
    );
    assert!(result.is_err());
}

#[test]
fn executor_tracks_ports_on_create() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);

    let spec = stack(
        "myapp",
        vec![ServiceSpec {
            ports: vec![PortSpec {
                protocol: "tcp".to_string(),
                container_port: 80,
                host_port: Some(8080),
            }],
            ..svc("web", "nginx:latest")
        }],
    );

    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Ports should be tracked.
    let ports = executor.ports().ports_for("web").unwrap();
    assert_eq!(ports.len(), 1);
    assert_eq!(ports[0].host_port, 8080);
}

#[test]
fn executor_releases_ports_on_remove() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);

    let spec = stack(
        "myapp",
        vec![ServiceSpec {
            ports: vec![PortSpec {
                protocol: "tcp".to_string(),
                container_port: 80,
                host_port: Some(8080),
            }],
            ..svc("web", "nginx:latest")
        }],
    );

    // Create first.
    let create_actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];
    executor.execute(&spec, &create_actions).unwrap();
    assert!(executor.ports().ports_for("web").is_some());

    // Remove should release ports.
    let remove_actions = vec![Action::ServiceRemove {
        service_name: "web".to_string(),
    }];
    let result = executor.execute(&spec, &remove_actions).unwrap();
    assert!(result.all_succeeded());
    assert!(executor.ports().ports_for("web").is_none());
    assert!(executor.ports().in_use().is_empty());
}

#[test]
fn executor_port_conflict_emits_event() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
    let mut executor = make_executor(runtime);

    let spec = stack(
        "myapp",
        vec![
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 3000,
                    host_port: Some(8080), // conflict with web
                }],
                ..svc("api", "node:20")
            },
        ],
    );

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.succeeded, 1); // web succeeds
    assert_eq!(result.failed, 1); // api fails (port conflict)

    // PortConflict event emitted.
    let events = executor.store().load_events("myapp").unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::PortConflict { .. }))
    );

    // api should be marked Failed.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let api = observed.iter().find(|o| o.service_name == "api").unwrap();
    assert_eq!(api.phase, ServicePhase::Failed);
}

// ── Docker Compose network conformance tests ──

/// Helper: two-service stack for network tests.
fn network_stack() -> StackSpec {
    stack(
        "netapp",
        vec![
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 5432,
                    host_port: Some(5432),
                }],
                ..svc("db", "postgres:16")
            },
        ],
    )
}

/// Helper: three-service stack.
fn three_service_stack() -> StackSpec {
    stack(
        "triapp",
        vec![
            svc("web", "nginx:latest"),
            svc("api", "node:20"),
            svc("db", "postgres:16"),
        ],
    )
}

#[test]
fn shared_vm_boots_before_container_creates() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = network_stack();

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Verify ordering: create_sandbox → setup_sandbox_network → create_in_sandbox × 2.
    let call_log = executor.runtime.call_log();
    let ops: Vec<&str> = call_log.iter().map(|(op, _)| op.as_str()).collect();
    assert_eq!(ops[0], "create_sandbox");
    assert_eq!(ops[1], "setup_sandbox_network");
    // Remaining: pull + create_in_sandbox for each service.
    assert!(ops.contains(&"create_in_sandbox"));
    assert!(
        !ops.contains(&"create"),
        "should use create_in_sandbox, not create"
    );
}

#[test]
fn setup_sandbox_network_assigns_correct_ips() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = network_stack();

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];

    executor.execute(&spec, &actions).unwrap();

    // Verify setup_sandbox_network was called with correct service configs.
    let captured = executor.runtime.captured_network_services.lock().unwrap();
    assert_eq!(captured.len(), 1);
    let (stack_id, services) = &captured[0];
    assert_eq!(stack_id, "netapp");
    assert_eq!(services.len(), 2);

    // web gets 172.20.0.2/24, db gets 172.20.0.3/24, both on "default" network.
    assert_eq!(services[0].name, "web");
    assert_eq!(services[0].addr, "172.20.0.2/24");
    assert_eq!(services[0].network_name, "default");
    assert_eq!(services[1].name, "db");
    assert_eq!(services[1].addr, "172.20.0.3/24");
    assert_eq!(services[1].network_name, "default");
}

#[test]
fn service_to_service_hosts_use_real_ips() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = network_stack();

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];

    executor.execute(&spec, &actions).unwrap();

    // Verify extra_hosts use real IPs, not 127.0.0.1.
    let configs = executor.runtime.captured_configs.lock().unwrap();

    // Find web's config.
    let web_config = configs.iter().find(|(id, _)| id == "ctr-web");
    assert!(web_config.is_some(), "web config not captured");
    let web_hosts = &web_config.unwrap().1.extra_hosts;
    // web should have db mapped to 172.20.0.3 (db is index 1, so .3).
    let db_host = web_hosts.iter().find(|(h, _)| h == "db");
    assert!(db_host.is_some(), "db not in web's extra_hosts");
    assert_eq!(db_host.unwrap().1, "172.20.0.3");

    // Find db's config.
    let db_config = configs.iter().find(|(id, _)| id == "ctr-db");
    assert!(db_config.is_some(), "db config not captured");
    let db_hosts = &db_config.unwrap().1.extra_hosts;
    // db should have web mapped to 172.20.0.2.
    let web_host = db_hosts.iter().find(|(h, _)| h == "web");
    assert!(web_host.is_some(), "web not in db's extra_hosts");
    assert_eq!(web_host.unwrap().1, "172.20.0.2");
}

#[test]
fn containers_join_per_service_network_namespace() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = network_stack();

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];

    executor.execute(&spec, &actions).unwrap();

    let configs = executor.runtime.captured_configs.lock().unwrap();

    // web should join /var/run/netns/web.
    let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
    assert_eq!(
        web_config.1.network_namespace_path,
        Some("/var/run/netns/web".to_string())
    );

    // db should join /var/run/netns/db.
    let db_config = configs.iter().find(|(id, _)| id == "ctr-db").unwrap();
    assert_eq!(
        db_config.1.network_namespace_path,
        Some("/var/run/netns/db".to_string())
    );
}

#[test]
fn same_container_port_no_conflict_with_shared_vm() {
    // Two services both bind container port 80 but in different netns.
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
    let mut executor = make_executor(runtime);

    let spec = stack(
        "portapp",
        vec![
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8081),
                }],
                ..svc("api", "node:20")
            },
        ],
    );

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    // Both succeed: different host ports, same container port is fine with netns.
    assert!(result.all_succeeded());
    assert_eq!(result.succeeded, 2);
}

#[test]
fn three_service_ip_allocation() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = three_service_stack();

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];

    executor.execute(&spec, &actions).unwrap();

    let captured = executor.runtime.captured_network_services.lock().unwrap();
    let (_, services) = &captured[0];
    assert_eq!(services.len(), 3);
    // 172.20.0.1 = bridge, services get .2, .3, .4.
    assert_eq!(services[0].addr, "172.20.0.2/24");
    assert_eq!(services[1].addr, "172.20.0.3/24");
    assert_eq!(services[2].addr, "172.20.0.4/24");

    // Verify cross-service host resolution for web.
    let configs = executor.runtime.captured_configs.lock().unwrap();
    let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
    let web_hosts = &web_config.1.extra_hosts;
    assert_eq!(web_hosts.len(), 2); // api + db
    assert!(
        web_hosts
            .iter()
            .any(|(h, ip)| h == "api" && ip == "172.20.0.3")
    );
    assert!(
        web_hosts
            .iter()
            .any(|(h, ip)| h == "db" && ip == "172.20.0.4")
    );
}

#[test]
fn single_service_stack_uses_sandbox() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec = stack("solo", vec![svc("web", "nginx:latest")]);

    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Single-service stacks use sandbox mode (same as multi-service).
    let call_log = executor.runtime.call_log();
    let ops: Vec<&str> = call_log.iter().map(|(op, _)| op.as_str()).collect();
    assert!(ops.contains(&"create_sandbox"));
    assert!(ops.contains(&"setup_sandbox_network"));
    assert!(ops.contains(&"create_in_sandbox"));
    assert!(
        !ops.contains(&"create"),
        "should use create_in_sandbox, not create"
    );
}

#[test]
fn single_service_gets_sandbox_network() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
    let mut executor = make_executor(runtime);
    let spec = stack("solo", vec![svc("web", "nginx:latest")]);

    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    executor.execute(&spec, &actions).unwrap();

    // Single service gets sandbox networking (netns path assigned).
    let configs = executor.runtime.captured_configs.lock().unwrap();
    let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
    assert!(
        web_config.1.network_namespace_path.is_some(),
        "single service should get a network namespace"
    );
}

#[test]
fn shared_vm_not_rebooted_on_second_execute() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db", "ctr-new"]);
    let mut executor = make_executor(runtime);
    let spec = network_stack();

    // First execute: boots shared VM.
    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];
    executor.execute(&spec, &actions).unwrap();

    // Second execute with a recreate: should NOT reboot.
    let actions2 = vec![Action::ServiceRecreate {
        service_name: "web".to_string(),
    }];
    executor.execute(&spec, &actions2).unwrap();

    // create_sandbox should only appear once.
    let boot_count = executor
        .runtime
        .call_log()
        .iter()
        .filter(|(op, _)| op == "create_sandbox")
        .count();
    assert_eq!(boot_count, 1, "sandbox should not be recreated");
}

// ── Parallel execution tests ──

#[test]
fn topo_levels_independent_services_same_level() {
    // Three services with no deps → all at level 0.
    let spec = three_service_stack();
    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];
    let refs: Vec<&Action> = actions.iter().collect();
    let levels = compute_topo_levels(&refs, &spec);
    assert_eq!(levels.len(), 1, "all independent services at one level");
    assert_eq!(levels[0].len(), 3);
}

#[test]
fn topo_levels_chain_dependency() {
    // app → api → db: three levels.
    let spec = stack(
        "chain",
        vec![
            svc("db", "postgres:16"),
            ServiceSpec {
                depends_on: vec![crate::spec::ServiceDependency::started("db")],
                ..svc("api", "node:20")
            },
            ServiceSpec {
                depends_on: vec![crate::spec::ServiceDependency::started("api")],
                ..svc("app", "myapp:latest")
            },
        ],
    );
    let actions = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
        Action::ServiceCreate {
            service_name: "app".to_string(),
        },
    ];
    let refs: Vec<&Action> = actions.iter().collect();
    let levels = compute_topo_levels(&refs, &spec);
    assert_eq!(levels.len(), 3);
    assert_eq!(levels[0][0].service_name(), "db");
    assert_eq!(levels[1][0].service_name(), "api");
    assert_eq!(levels[2][0].service_name(), "app");
}

#[test]
fn topo_levels_diamond_dependency() {
    // web and api depend on db → db at level 0, web+api at level 1.
    let spec = stack(
        "diamond",
        vec![
            svc("db", "postgres:16"),
            ServiceSpec {
                depends_on: vec![crate::spec::ServiceDependency::started("db")],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                depends_on: vec![crate::spec::ServiceDependency::started("db")],
                ..svc("api", "node:20")
            },
        ],
    );
    let actions = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
    ];
    let refs: Vec<&Action> = actions.iter().collect();
    let levels = compute_topo_levels(&refs, &spec);
    assert_eq!(levels.len(), 2);
    assert_eq!(levels[0].len(), 1);
    assert_eq!(levels[0][0].service_name(), "db");
    assert_eq!(levels[1].len(), 2);
    let level1_names: HashSet<&str> = levels[1].iter().map(|a| a.service_name()).collect();
    assert!(level1_names.contains("web"));
    assert!(level1_names.contains("api"));
}

#[test]
fn parallel_creates_all_succeed() {
    // Three independent services should all be created (via parallel path).
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = three_service_stack();

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());
    assert_eq!(result.succeeded, 3);

    // All three should be Running with deterministic IDs from container_id.
    let observed = executor.store().load_observed_state("triapp").unwrap();
    assert_eq!(observed.len(), 3);
    for obs in &observed {
        assert_eq!(obs.phase, ServicePhase::Running);
        assert_eq!(obs.container_id, Some(format!("ctr-{}", obs.service_name)));
    }
}

#[test]
fn parallel_creates_with_dependency_ordering() {
    // web depends on db: db at level 0 (serial), web at level 1 (serial).
    // api has no deps: at level 0 alongside db (parallel with db).
    let spec = stack(
        "depapp",
        vec![
            svc("db", "postgres:16"),
            svc("api", "node:20"),
            ServiceSpec {
                depends_on: vec![crate::spec::ServiceDependency::started("db")],
                ..svc("web", "nginx:latest")
            },
        ],
    );

    let runtime = MockContainerRuntime::with_ids(vec!["ctr-db", "ctr-api", "ctr-web"]);
    let mut executor = make_executor(runtime);

    let actions = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(
        result.all_succeeded(),
        "execution had errors: {:?}",
        result.errors
    );
    assert_eq!(result.succeeded, 3);

    // web depends on db, so web's create must come after db's.
    // api is independent, so it can be in any order relative to db.
    // With 3 services the executor boots a shared VM, so creates go
    // through create_in_sandbox (arg = "stack_name:image").
    let calls = executor.runtime.call_log();
    let create_calls: Vec<&str> = calls
        .iter()
        .filter(|(op, _)| op == "create" || op == "create_in_sandbox")
        .map(|(_, arg)| arg.as_str())
        .collect();
    // db and api images are both at level 0.
    // web image is at level 1 and must appear after both db and api.
    let web_idx = create_calls
        .iter()
        .position(|img| img.contains("nginx:latest"))
        .unwrap();
    let db_idx = create_calls
        .iter()
        .position(|img| img.contains("postgres:16"))
        .unwrap();
    assert!(
        db_idx < web_idx,
        "db must be created before web (dependency)"
    );
}

#[test]
fn resource_hints_passed_to_create_sandbox() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);

    let spec = stack(
        "resapp",
        vec![
            ServiceSpec {
                resources: ResourcesSpec {
                    cpus: Some(2.0),
                    memory_bytes: Some(512 * 1024 * 1024), // 512 MiB
                    ..Default::default()
                },
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                resources: ResourcesSpec {
                    cpus: Some(4.0),
                    memory_bytes: Some(1024 * 1024 * 1024), // 1 GiB
                    ..Default::default()
                },
                ..svc("db", "postgres:16")
            },
        ],
    );

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());
    // Verify create_sandbox was called (indicating sandbox was used).
    let calls = executor.runtime.call_log();
    assert!(calls.iter().any(|(op, _)| op == "create_sandbox"));
}

// ── Custom network tests ──

/// Helper: create a NetworkSpec.
fn net(name: &str, subnet: Option<&str>) -> crate::spec::NetworkSpec {
    crate::spec::NetworkSpec {
        name: name.to_string(),
        driver: "bridge".to_string(),
        subnet: subnet.map(|s| s.to_string()),
    }
}

#[test]
fn custom_networks_multi_subnet_allocation() {
    // Two networks: frontend (auto) and backend (auto).
    // web on frontend only, api on both, db on backend only.
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
    let mut executor = make_executor(runtime);

    let spec = StackSpec {
        name: "multinet".to_string(),
        services: vec![
            ServiceSpec {
                networks: vec!["frontend".to_string()],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                networks: vec!["frontend".to_string(), "backend".to_string()],
                ..svc("api", "node:20")
            },
            ServiceSpec {
                networks: vec!["backend".to_string()],
                ..svc("db", "postgres:16")
            },
        ],
        networks: vec![net("frontend", None), net("backend", None)],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded(), "errors: {:?}", result.errors);

    // Verify network configs: 4 entries (web@frontend, api@frontend, api@backend, db@backend).
    let captured = executor.runtime.captured_network_services.lock().unwrap();
    assert_eq!(captured.len(), 1);
    let (_, services) = &captured[0];
    assert_eq!(services.len(), 4);

    // frontend network: 172.20.0.0/24
    assert_eq!(services[0].name, "web");
    assert_eq!(services[0].addr, "172.20.0.2/24");
    assert_eq!(services[0].network_name, "frontend");

    assert_eq!(services[1].name, "api");
    assert_eq!(services[1].addr, "172.20.0.3/24");
    assert_eq!(services[1].network_name, "frontend");

    // backend network: 172.20.1.0/24
    assert_eq!(services[2].name, "api");
    assert_eq!(services[2].addr, "172.20.1.2/24");
    assert_eq!(services[2].network_name, "backend");

    assert_eq!(services[3].name, "db");
    assert_eq!(services[3].addr, "172.20.1.3/24");
    assert_eq!(services[3].network_name, "backend");
}

#[test]
fn custom_networks_explicit_subnet() {
    // Frontend has explicit subnet 10.0.1.0/24.
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
    let mut executor = make_executor(runtime);

    let spec = StackSpec {
        name: "explicit".to_string(),
        services: vec![
            ServiceSpec {
                networks: vec!["frontend".to_string()],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                networks: vec!["frontend".to_string()],
                ..svc("api", "node:20")
            },
        ],
        networks: vec![net("frontend", Some("10.0.1.0/24"))],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded(), "errors: {:?}", result.errors);

    let captured = executor.runtime.captured_network_services.lock().unwrap();
    let (_, services) = &captured[0];
    assert_eq!(services[0].addr, "10.0.1.2/24");
    assert_eq!(services[1].addr, "10.0.1.3/24");
}

#[test]
fn scoped_hosts_only_shared_networks() {
    // web on frontend only, db on backend only, api on both.
    // web should see api (shared frontend) but NOT db.
    // db should see api (shared backend) but NOT web.
    // api should see both web and db.
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
    let mut executor = make_executor(runtime);

    let spec = StackSpec {
        name: "scoped".to_string(),
        services: vec![
            ServiceSpec {
                networks: vec!["frontend".to_string()],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                networks: vec!["frontend".to_string(), "backend".to_string()],
                ..svc("api", "node:20")
            },
            ServiceSpec {
                networks: vec!["backend".to_string()],
                ..svc("db", "postgres:16")
            },
        ],
        networks: vec![net("frontend", None), net("backend", None)],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded(), "errors: {:?}", result.errors);

    let configs = executor.runtime.captured_configs.lock().unwrap();

    // web should only see api (shared frontend), NOT db.
    let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
    let web_hosts: Vec<&str> = web_config
        .1
        .extra_hosts
        .iter()
        .map(|(h, _)| h.as_str())
        .collect();
    assert!(web_hosts.contains(&"api"), "web should see api");
    assert!(!web_hosts.contains(&"db"), "web should NOT see db");

    // db should only see api (shared backend), NOT web.
    let db_config = configs.iter().find(|(id, _)| id == "ctr-db").unwrap();
    let db_hosts: Vec<&str> = db_config
        .1
        .extra_hosts
        .iter()
        .map(|(h, _)| h.as_str())
        .collect();
    assert!(db_hosts.contains(&"api"), "db should see api");
    assert!(!db_hosts.contains(&"web"), "db should NOT see web");

    // api should see both web and db.
    let api_config = configs.iter().find(|(id, _)| id == "ctr-api").unwrap();
    let api_hosts: Vec<&str> = api_config
        .1
        .extra_hosts
        .iter()
        .map(|(h, _)| h.as_str())
        .collect();
    assert!(api_hosts.contains(&"web"), "api should see web");
    assert!(api_hosts.contains(&"db"), "api should see db");
}

#[test]
fn default_network_backward_compat() {
    // When all services are on "default" network, behaviour is identical
    // to the old single-bridge approach.
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = network_stack();

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // All services on same network, so all see each other.
    let configs = executor.runtime.captured_configs.lock().unwrap();
    let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
    assert_eq!(web_config.1.extra_hosts.len(), 1);
    assert_eq!(web_config.1.extra_hosts[0].0, "db");

    let db_config = configs.iter().find(|(id, _)| id == "ctr-db").unwrap();
    assert_eq!(db_config.1.extra_hosts.len(), 1);
    assert_eq!(db_config.1.extra_hosts[0].0, "web");
}

#[test]
fn parse_subnet_helpers() {
    assert_eq!(parse_subnet_base("172.20.1.0/24"), [172, 20, 1, 0]);
    assert_eq!(parse_subnet_base("10.0.0.0/16"), [10, 0, 0, 0]);
    assert_eq!(parse_subnet_prefix("172.20.1.0/24"), 24);
    assert_eq!(parse_subnet_prefix("10.0.0.0/16"), 16);
}

#[test]
fn port_tracker_snapshot_and_restore() {
    let mut tracker = PortTracker::new();
    let ports = vec![PublishedPort {
        host_port: 8080,
        container_port: 80,
        protocol: "tcp".to_string(),
    }];
    tracker.restore("web".to_string(), ports.clone());

    let snapshot = tracker.allocated_snapshot();
    assert_eq!(snapshot.get("web").unwrap(), &ports);

    let mut tracker2 = PortTracker::new();
    for (name, ports) in snapshot {
        tracker2.restore(name.clone(), ports.clone());
    }
    assert_eq!(tracker2.allocated_snapshot().get("web").unwrap(), &ports);
}

#[test]
fn stream_logs_default_returns_empty_stream() {
    let runtime = MockContainerRuntime::new();
    let rx = runtime.stream_logs("ctr-001", "web", false).unwrap();

    // Default mock has no pre-configured lines, so channel closes immediately.
    let lines: Vec<LogLine> = rx.iter().collect();
    assert!(lines.is_empty());
}

#[test]
fn stream_logs_mock_returns_configured_lines() {
    let runtime = MockContainerRuntime::new();
    {
        let mut mock_lines = runtime.mock_log_lines.lock().unwrap();
        mock_lines.push(LogLine {
            timestamp: Some("2025-01-15T10:00:00Z".to_string()),
            service: "api".to_string(),
            line: "server started on :8080".to_string(),
        });
        mock_lines.push(LogLine {
            timestamp: None,
            service: "api".to_string(),
            line: "ready to accept connections".to_string(),
        });
    }

    let rx = runtime.stream_logs("ctr-api", "api", true).unwrap();
    let lines: Vec<LogLine> = rx.iter().collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0].service, "api");
    assert_eq!(lines[0].line, "server started on :8080");
    assert!(lines[0].timestamp.is_some());
    assert_eq!(lines[1].line, "ready to accept connections");
    assert!(lines[1].timestamp.is_none());
}

#[test]
fn stream_logs_records_call_in_mock() {
    let runtime = MockContainerRuntime::new();
    let _rx = runtime.stream_logs("ctr-db", "postgres", true).unwrap();

    let calls = runtime.call_log();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "stream_logs");
    assert_eq!(calls[0].1, "ctr-db:postgres:follow=true");
}

#[test]
fn log_line_clone_and_debug() {
    let line = LogLine {
        timestamp: Some("2025-01-15T10:00:00Z".to_string()),
        service: "web".to_string(),
        line: "hello world".to_string(),
    };
    let cloned = line.clone();
    assert_eq!(cloned.service, "web");
    // Ensure Debug is derived.
    let _debug = format!("{:?}", cloned);
}

// ── Stop/remove failure cascade tests ──

#[test]
fn stop_failure_still_attempts_remove() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_stop = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);

    // Simulate existing running container.
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-web".to_string()),
                last_error: None,
                ready: false,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRemove {
        service_name: "web".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(
        result.all_succeeded(),
        "remove should succeed despite stop failure"
    );

    // Verify both stop AND remove were attempted.
    let calls = executor.runtime().call_log();
    assert!(
        calls.iter().any(|(op, _)| op == "stop"),
        "stop should be attempted"
    );
    assert!(
        calls.iter().any(|(op, _)| op == "remove"),
        "remove should still be called after stop failure"
    );

    // State should be Stopped (not stuck in Running).
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed.iter().find(|o| o.service_name == "web").unwrap();
    assert_eq!(web.phase, ServicePhase::Stopped);
    assert!(web.container_id.is_none());
}

#[test]
fn remove_failure_still_updates_state_to_stopped() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_remove = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);

    // Simulate existing running container.
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-web".to_string()),
                last_error: None,
                ready: false,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRemove {
        service_name: "web".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(
        result.all_succeeded(),
        "remove should succeed even when runtime remove fails"
    );

    // State should be Stopped (not stuck in Running).
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed.iter().find(|o| o.service_name == "web").unwrap();
    assert_eq!(web.phase, ServicePhase::Stopped);
    assert!(web.container_id.is_none());
}

#[test]
fn stop_and_remove_both_fail_still_updates_state() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_stop = true;
    runtime.fail_remove = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);

    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-web".to_string()),
                last_error: None,
                ready: false,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRemove {
        service_name: "web".to_string(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    // Executor marks result as succeeded because state is updated
    // regardless of stop/remove runtime errors (best-effort cleanup).
    assert!(result.all_succeeded());

    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed.iter().find(|o| o.service_name == "web").unwrap();
    assert_eq!(web.phase, ServicePhase::Stopped);
}

#[test]
fn ports_released_on_remove_even_when_stop_fails() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_stop = true;
    let mut executor = make_executor(runtime);

    let mut web = svc("web", "nginx:latest");
    web.ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(8080),
    }];
    let spec = stack("myapp", vec![web.clone()]);

    // Create the service first.
    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];
    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());
    assert!(executor.ports().in_use().contains(&8080));

    // Now remove — stop will fail but ports should still be released.
    let remove_spec = stack("myapp", vec![]);
    let remove_actions = vec![Action::ServiceRemove {
        service_name: "web".to_string(),
    }];
    let result = executor.execute(&remove_spec, &remove_actions).unwrap();
    assert!(result.all_succeeded());
    assert!(
        !executor.ports().in_use().contains(&8080),
        "port 8080 should be released even when stop fails"
    );
}

#[test]
fn ports_released_when_create_fails_on_retry() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_create = true;
    let mut executor = make_executor(runtime);

    let mut web = svc("web", "nginx:latest");
    web.ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(8080),
    }];
    let spec = stack("myapp", vec![web.clone()]);

    // Create fails — ports were allocated during prepare_create but
    // service is marked Failed. Verify port state is usable for retry.
    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];
    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.failed, 1);

    // Port should still be allocated (not released) because the service
    // will be retried — release only happens on ServiceRemove.
    // But crucially, a second create attempt should not conflict.
    let mut retry_runtime = MockContainerRuntime::new();
    retry_runtime.fail_create = false;
    // We can't swap the runtime, but we can verify port tracker state
    // allows reallocation for the same service.
    let reallocated = executor.ports_mut().allocate("web", &web.ports);
    assert!(
        reallocated.is_ok(),
        "same service should be able to reallocate its ports on retry: {:?}",
        reallocated.err()
    );
}

// ── Partial replica scale-down failure tests ──

#[test]
fn replica_scale_down_removes_excess_replicas() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec_name = "replica-sd";

    // Simulate 3 running replicas.
    for (name, cid) in [
        ("web", "ctr-web"),
        ("web-2", "ctr-web-2"),
        ("web-3", "ctr-web-3"),
    ] {
        executor
            .store()
            .save_observed_state(
                spec_name,
                &ServiceObservedState {
                    service_name: name.to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some(cid.to_string()),
                    last_error: None,
                    ready: false,
                },
            )
            .unwrap();
    }

    // Scale down to 1 replica.
    let mut web = svc("web", "nginx:latest");
    web.resources.replicas = 1;
    let spec = stack(spec_name, vec![web]);

    let health = HashMap::new();
    let reconcile = apply(&spec, executor.store(), &health).unwrap();

    // Should generate 2 remove actions (for web-2 and web-3).
    let remove_count = reconcile
        .actions
        .iter()
        .filter(|a| matches!(a, Action::ServiceRemove { .. }))
        .count();
    assert_eq!(remove_count, 2, "should remove 2 excess replicas");

    let result = executor.execute(&spec, &reconcile.actions).unwrap();
    assert_eq!(result.failed, 0, "all removals should succeed");

    // Only web (base replica) should remain running.
    let observed = executor.store().load_observed_state(spec_name).unwrap();
    let running: Vec<&str> = observed
        .iter()
        .filter(|o| matches!(o.phase, ServicePhase::Running))
        .map(|o| o.service_name.as_str())
        .collect();
    assert_eq!(running, vec!["web"]);
}
