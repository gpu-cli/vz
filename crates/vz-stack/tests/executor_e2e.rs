//! End-to-end stack integration tests.
//!
//! Exercises the full pipeline: parse compose YAML → reconcile →
//! execute via mock runtime → verify container state, health checks,
//! restart policies, and port allocation.

#![allow(clippy::unwrap_used)]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use vz_stack::{
    Action, ContainerRuntime, HealthPoller, RestartTracker, ServiceObservedState, ServicePhase,
    StackError, StackEvent, StackExecutor, StackSpec, StateStore, compute_restarts, parse_compose,
};

// ── Mock runtime for integration tests ───────────────────────────

struct MockRuntime {
    container_ids: Vec<String>,
    exec_exit_code: Cell<i32>,
    calls: RefCell<Vec<(String, String)>>,
    create_counter: Cell<usize>,
}

impl MockRuntime {
    fn new(ids: Vec<&str>) -> Self {
        Self {
            container_ids: ids.into_iter().map(String::from).collect(),
            exec_exit_code: Cell::new(0),
            calls: RefCell::new(Vec::new()),
            create_counter: Cell::new(0),
        }
    }

    fn call_log(&self) -> Vec<(String, String)> {
        self.calls.borrow().clone()
    }

    fn set_exec_exit_code(&self, code: i32) {
        self.exec_exit_code.set(code);
    }
}

impl ContainerRuntime for MockRuntime {
    fn pull(&self, image: &str) -> Result<String, StackError> {
        self.calls.borrow_mut().push(("pull".into(), image.into()));
        Ok(format!("sha256:{image}"))
    }

    fn create(&self, image: &str, _config: vz_oci::RunConfig) -> Result<String, StackError> {
        self.calls
            .borrow_mut()
            .push(("create".into(), image.into()));
        let idx = self.create_counter.get();
        let id = self.container_ids[idx % self.container_ids.len()].clone();
        self.create_counter.set(idx + 1);
        Ok(id)
    }

    fn stop(&self, container_id: &str) -> Result<(), StackError> {
        self.calls
            .borrow_mut()
            .push(("stop".into(), container_id.into()));
        Ok(())
    }

    fn remove(&self, container_id: &str) -> Result<(), StackError> {
        self.calls
            .borrow_mut()
            .push(("remove".into(), container_id.into()));
        Ok(())
    }

    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError> {
        self.calls.borrow_mut().push((
            "exec".into(),
            format!("{container_id}:{}", command.join(" ")),
        ));
        Ok(self.exec_exit_code.get())
    }
}

// ── Helpers ──────────────────────────────────────────────────────

// ── Full pipeline: parse → reconcile → execute ──────────────────

const SIMPLE_COMPOSE: &str = r#"
services:
  web:
    image: nginx:latest
    ports:
      - "8080:80"

  api:
    image: node:20
    ports:
      - "3000:3000"
    depends_on:
      - web
"#;

#[test]
fn full_pipeline_parse_apply_execute() {
    let spec = parse_compose(SIMPLE_COMPOSE, "myapp").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(&dir.path().join("state.db")).unwrap();

    // Step 1: Reconcile to get actions.
    let health = HashMap::new();
    let result = vz_stack::apply(&spec, &store, &health).unwrap();
    assert_eq!(result.actions.len(), 2);

    // Step 2: Execute actions through mock runtime.
    let runtime = MockRuntime::new(vec!["ctr-web", "ctr-api"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());

    let exec_result = executor.execute(&spec, &result.actions).unwrap();
    assert!(exec_result.all_succeeded());
    assert_eq!(exec_result.succeeded, 2);

    // Step 3: Verify observed state.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed.len(), 2);

    let web = observed.iter().find(|o| o.service_name == "web").unwrap();
    assert_eq!(web.phase, ServicePhase::Running);
    assert_eq!(web.container_id, Some("ctr-web".to_string()));

    let api = observed.iter().find(|o| o.service_name == "api").unwrap();
    assert_eq!(api.phase, ServicePhase::Running);
    assert_eq!(api.container_id, Some("ctr-api".to_string()));

    // Step 4: Verify events emitted.
    // Note: apply() also emits ServiceCreating events, so we get 2 from apply + 2 from executor = 4.
    // ServiceReady events are only emitted by the executor (2 total).
    let events = executor.store().load_events("myapp").unwrap();
    let creating_count = events
        .iter()
        .filter(|e| matches!(e, StackEvent::ServiceCreating { .. }))
        .count();
    assert_eq!(creating_count, 4);

    let ready_count = events
        .iter()
        .filter(|e| matches!(e, StackEvent::ServiceReady { .. }))
        .count();
    assert_eq!(ready_count, 2);
}

#[test]
fn full_pipeline_up_then_down() {
    let spec = parse_compose(SIMPLE_COMPOSE, "myapp").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(&dir.path().join("state.db")).unwrap();

    // UP: apply + execute.
    let health = HashMap::new();
    let up_result = vz_stack::apply(&spec, &store, &health).unwrap();

    let runtime = MockRuntime::new(vec!["ctr-web", "ctr-api"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());
    executor.execute(&spec, &up_result.actions).unwrap();

    // Verify running.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert!(observed.iter().all(|o| o.phase == ServicePhase::Running));

    // DOWN: construct remove actions directly (bypassing apply, which
    // would clear container_ids before the executor can call stop/remove).
    let empty = StackSpec {
        name: "myapp".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
    };
    let down_actions = vec![
        Action::ServiceRemove {
            service_name: "web".to_string(),
        },
        Action::ServiceRemove {
            service_name: "api".to_string(),
        },
    ];

    let exec_result = executor.execute(&empty, &down_actions).unwrap();
    assert!(exec_result.all_succeeded());

    // Verify stopped.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert!(
        observed.iter().all(|o| o.phase == ServicePhase::Stopped),
        "all services should be stopped: {observed:?}"
    );

    // Verify runtime calls include stop + remove.
    let calls = executor.runtime().call_log();
    let stop_count = calls.iter().filter(|(op, _)| op == "stop").count();
    let remove_count = calls.iter().filter(|(op, _)| op == "remove").count();
    assert_eq!(stop_count, 2);
    assert_eq!(remove_count, 2);
}

// ── Health check integration ────────────────────────────────────

const HEALTHCHECK_COMPOSE: &str = r#"
services:
  db:
    image: postgres:16
    healthcheck:
      test: ["CMD", "pg_isready"]
      interval: 5s
      retries: 2

  app:
    image: myapp:latest
    depends_on:
      - db
"#;

#[test]
fn health_check_gates_dependent_service() {
    let spec = parse_compose(HEALTHCHECK_COMPOSE, "hc-test").unwrap();
    let dir = tempfile::tempdir().unwrap();

    // Initial apply creates both (topo sort handles fresh deploy).
    let store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let health = HashMap::new();
    let result = vz_stack::apply(&spec, &store, &health).unwrap();
    assert_eq!(result.actions.len(), 2);

    // Execute: both start running.
    let runtime = MockRuntime::new(vec!["ctr-db", "ctr-app"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());
    executor.execute(&spec, &result.actions).unwrap();

    // Health check: db returns healthy.
    let mut poller = HealthPoller::new();
    let poll_result = poller
        .poll_all(executor.runtime(), executor.store(), &spec)
        .unwrap();
    assert_eq!(poll_result.newly_ready, vec!["db".to_string()]);

    // Verify db is now ready.
    let observed = executor.store().load_observed_state("hc-test").unwrap();
    let db = observed.iter().find(|o| o.service_name == "db").unwrap();
    assert!(db.ready);
}

#[test]
fn health_check_failure_marks_service_failed() {
    let spec = parse_compose(HEALTHCHECK_COMPOSE, "hc-fail").unwrap();
    let dir = tempfile::tempdir().unwrap();

    let store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let health = HashMap::new();
    let result = vz_stack::apply(&spec, &store, &health).unwrap();

    let runtime = MockRuntime::new(vec!["ctr-db", "ctr-app"]);
    runtime.set_exec_exit_code(1); // Health checks will fail.

    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());
    executor.execute(&spec, &result.actions).unwrap();

    // Poll twice with failures (retries=2).
    let mut poller = HealthPoller::new();
    poller
        .poll_all(executor.runtime(), executor.store(), &spec)
        .unwrap();
    let poll2 = poller
        .poll_all(executor.runtime(), executor.store(), &spec)
        .unwrap();

    assert_eq!(poll2.newly_failed, vec!["db".to_string()]);

    // db should be marked Failed.
    let observed = executor.store().load_observed_state("hc-fail").unwrap();
    let db = observed.iter().find(|o| o.service_name == "db").unwrap();
    assert_eq!(db.phase, ServicePhase::Failed);
}

// ── Restart policy integration ──────────────────────────────────

const RESTART_COMPOSE: &str = r#"
services:
  worker:
    image: worker:latest
    restart: always

  cron:
    image: cron:latest
    restart: "no"
"#;

#[test]
fn restart_policy_generates_actions_for_failed_services() {
    let spec = parse_compose(RESTART_COMPOSE, "restart-test").unwrap();
    let dir = tempfile::tempdir().unwrap();

    let store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let health = HashMap::new();
    let result = vz_stack::apply(&spec, &store, &health).unwrap();

    let runtime = MockRuntime::new(vec!["ctr-worker", "ctr-cron"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());
    executor.execute(&spec, &result.actions).unwrap();

    // Simulate both services failing.
    let observed_states = vec![
        ServiceObservedState {
            service_name: "worker".to_string(),
            phase: ServicePhase::Failed,
            container_id: None,
            last_error: Some("crash".to_string()),
            ready: false,
        },
        ServiceObservedState {
            service_name: "cron".to_string(),
            phase: ServicePhase::Failed,
            container_id: None,
            last_error: Some("crash".to_string()),
            ready: false,
        },
    ];

    for obs in &observed_states {
        executor
            .store()
            .save_observed_state("restart-test", obs)
            .unwrap();
    }

    // Compute restarts.
    let tracker = RestartTracker::new();
    let restart_actions = compute_restarts(&spec, &observed_states, &tracker);

    // Only worker should restart (policy=always). cron has policy=no.
    assert_eq!(restart_actions.len(), 1);
    assert!(matches!(
        &restart_actions[0],
        Action::ServiceCreate { service_name } if service_name == "worker"
    ));
}

#[test]
fn restart_with_max_retries_stops_after_limit() {
    let compose = r#"
services:
  worker:
    image: worker:latest
    restart: on-failure:2
"#;

    let spec = parse_compose(compose, "retry-test").unwrap();

    let observed = vec![ServiceObservedState {
        service_name: "worker".to_string(),
        phase: ServicePhase::Failed,
        container_id: None,
        last_error: Some("crash".to_string()),
        ready: false,
    }];

    let mut tracker = RestartTracker::new();

    // First restart: ok.
    let r1 = compute_restarts(&spec, &observed, &tracker);
    assert_eq!(r1.len(), 1);
    tracker.record_restart("worker");

    // Second restart: ok.
    let r2 = compute_restarts(&spec, &observed, &tracker);
    assert_eq!(r2.len(), 1);
    tracker.record_restart("worker");

    // Third restart: blocked (max_retries=2).
    let r3 = compute_restarts(&spec, &observed, &tracker);
    assert!(r3.is_empty());
}

// ── Port allocation integration ─────────────────────────────────

const PORT_COMPOSE: &str = r#"
services:
  web:
    image: nginx:latest
    ports:
      - "8080:80"

  api:
    image: node:20
    ports:
      - "3000:3000"
"#;

#[test]
fn port_allocation_tracked_through_lifecycle() {
    let spec = parse_compose(PORT_COMPOSE, "port-test").unwrap();
    let dir = tempfile::tempdir().unwrap();

    let store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let health = HashMap::new();
    let result = vz_stack::apply(&spec, &store, &health).unwrap();

    let runtime = MockRuntime::new(vec!["ctr-web", "ctr-api"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());
    executor.execute(&spec, &result.actions).unwrap();

    // Verify ports are tracked.
    let web_ports = executor.ports().ports_for("web").unwrap();
    assert_eq!(web_ports.len(), 1);
    assert_eq!(web_ports[0].host_port, 8080);
    assert_eq!(web_ports[0].container_port, 80);

    let api_ports = executor.ports().ports_for("api").unwrap();
    assert_eq!(api_ports.len(), 1);
    assert_eq!(api_ports[0].host_port, 3000);

    // Down: remove services and verify ports released.
    let empty = StackSpec {
        name: "port-test".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
    };
    let down = vz_stack::apply(&empty, &store, &health).unwrap();
    executor.execute(&empty, &down.actions).unwrap();

    assert!(executor.ports().ports_for("web").is_none());
    assert!(executor.ports().ports_for("api").is_none());
    assert!(executor.ports().in_use().is_empty());
}

// ── Volume lifecycle ────────────────────────────────────────────

const VOLUME_COMPOSE: &str = r#"
services:
  db:
    image: postgres:16
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
"#;

#[test]
fn volumes_created_and_used_in_full_pipeline() {
    let spec = parse_compose(VOLUME_COMPOSE, "vol-test").unwrap();
    let dir = tempfile::tempdir().unwrap();

    let store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let health = HashMap::new();
    let result = vz_stack::apply(&spec, &store, &health).unwrap();

    let runtime = MockRuntime::new(vec!["ctr-db"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());
    let exec_result = executor.execute(&spec, &result.actions).unwrap();
    assert!(exec_result.all_succeeded());

    // Volume directory should exist.
    assert!(executor.volumes().volumes_dir().join("pgdata").is_dir());

    // VolumeCreated event emitted.
    let events = executor.store().load_events("vol-test").unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::VolumeCreated { .. }))
    );
}

// ── Idempotent re-apply ─────────────────────────────────────────

#[test]
fn re_apply_after_execution_is_idempotent() {
    let spec = parse_compose(SIMPLE_COMPOSE, "idem-test").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(&dir.path().join("state.db")).unwrap();

    // First apply + execute.
    let health = HashMap::new();
    let result = vz_stack::apply(&spec, &store, &health).unwrap();

    let runtime = MockRuntime::new(vec!["ctr-web", "ctr-api"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());
    executor.execute(&spec, &result.actions).unwrap();

    // Second apply: should produce no actions since services are Running.
    let result2 = vz_stack::apply(&spec, &store, &health).unwrap();
    assert!(
        result2.actions.is_empty(),
        "second apply should be idempotent after execution: {:?}",
        result2.actions
    );
}
