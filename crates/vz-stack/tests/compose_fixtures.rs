//! Compose compatibility fixture tests.
//!
//! Validates canonical multi-service compose files through the full
//! pipeline: parse → reconcile → verify actions and state. These
//! tests run without a real container runtime — they verify the
//! control plane produces the correct plan.

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;

use vz_stack::{
    Action, HealthStatus, ServiceDependency, ServiceObservedState, ServicePhase, StackSpec,
    StateStore, parse_compose,
};

// ── Fixture: web + redis ──────────────────────────────────────────

const WEB_REDIS_COMPOSE: &str = r#"
services:
  web:
    image: myapp:latest
    ports:
      - "8080:80"
    depends_on:
      redis:
        condition: service_healthy
    environment:
      REDIS_URL: redis://redis:6379
    restart: on-failure:3

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 5

volumes:
  redis-data:
"#;

#[test]
fn web_redis_parse_produces_valid_spec() {
    let spec = parse_compose(WEB_REDIS_COMPOSE, "web-redis").unwrap();

    assert_eq!(spec.name, "web-redis");
    assert_eq!(spec.services.len(), 2);
    assert_eq!(spec.volumes.len(), 1);

    // Services are sorted by name.
    let redis = &spec.services[0];
    let web = &spec.services[1];

    assert_eq!(redis.name, "redis");
    assert_eq!(redis.image, "redis:7-alpine");
    assert!(redis.healthcheck.is_some());
    assert!(redis.depends_on.is_empty());

    assert_eq!(web.name, "web");
    assert_eq!(web.image, "myapp:latest");
    assert_eq!(web.depends_on, vec![ServiceDependency::healthy("redis")]);
    assert_eq!(
        web.environment.get("REDIS_URL").unwrap(),
        "redis://redis:6379"
    );

    // Volume defined.
    assert_eq!(spec.volumes[0].name, "redis-data");
    assert_eq!(spec.volumes[0].driver, "local");
}

#[test]
fn web_redis_initial_apply_creates_both_services() {
    let spec = parse_compose(WEB_REDIS_COMPOSE, "web-redis").unwrap();
    let (store, _dir) = open_temp_store();
    let health = HashMap::new();

    let result = vz_stack::apply(&spec, &store, &health).unwrap();

    // Strict dependency gating: only redis is created in the first pass.
    let created: Vec<&str> = result
        .actions
        .iter()
        .filter_map(|a| match a {
            Action::ServiceCreate { service_name } => Some(service_name.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        created.contains(&"redis"),
        "redis should be created: {created:?}"
    );
    assert_eq!(created, vec!["redis"]);
    assert_eq!(result.deferred.len(), 1);
    assert_eq!(result.deferred[0].service_name, "web");
}

#[test]
fn web_redis_second_apply_is_idempotent_when_all_running() {
    let spec = parse_compose(WEB_REDIS_COMPOSE, "web-redis").unwrap();
    let (store, _dir) = open_temp_store();
    let health = HashMap::new();

    // First apply.
    let _first = vz_stack::apply(&spec, &store, &health).unwrap();

    // Simulate runtime: both services now running.
    simulate_running(&store, "web-redis", &["redis", "web"]);

    // Redis health check passes.
    let mut health = HashMap::new();
    let mut redis_health = HealthStatus::new("redis");
    redis_health.record_pass();
    health.insert("redis".to_string(), redis_health);

    // Second apply should be idempotent — no actions needed.
    let second = vz_stack::apply(&spec, &store, &health).unwrap();
    assert!(
        second.actions.is_empty(),
        "second apply should be idempotent when all running: {:?}",
        second.actions
    );
}

#[test]
fn web_redis_redis_health_gates_web_when_redis_running_but_unhealthy() {
    let spec = parse_compose(WEB_REDIS_COMPOSE, "web-redis").unwrap();
    let (store, _dir) = open_temp_store();
    let health = HashMap::new();

    // First apply creates both.
    let _first = vz_stack::apply(&spec, &store, &health).unwrap();

    // Simulate: redis running but health check failing, web not yet created.
    store
        .save_observed_state(
            "web-redis",
            &ServiceObservedState {
                service_name: "redis".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-redis".to_string()),
                last_error: None,
                ready: false,
            },
        )
        .unwrap();

    // No health status for redis = health check hasn't passed.
    let health = HashMap::new();

    // Re-apply: web should be deferred because redis has a health check that hasn't passed.
    let result = vz_stack::apply(&spec, &store, &health).unwrap();

    let deferred_names: Vec<&str> = result
        .deferred
        .iter()
        .map(|d| d.service_name.as_str())
        .collect();

    assert!(
        deferred_names.contains(&"web"),
        "web should be deferred waiting on redis health: deferred={deferred_names:?}"
    );
}

#[test]
fn web_redis_redis_health_unblocks_web_when_healthy() {
    let spec = parse_compose(WEB_REDIS_COMPOSE, "web-redis").unwrap();
    let (store, _dir) = open_temp_store();
    let health = HashMap::new();

    // First apply.
    let _first = vz_stack::apply(&spec, &store, &health).unwrap();

    // Simulate: redis running and healthy.
    store
        .save_observed_state(
            "web-redis",
            &ServiceObservedState {
                service_name: "redis".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-redis".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let mut health = HashMap::new();
    let mut redis_health = HealthStatus::new("redis");
    redis_health.record_pass();
    health.insert("redis".to_string(), redis_health);

    // Re-apply: web should now be created (not deferred).
    let result = vz_stack::apply(&spec, &store, &health).unwrap();

    let created: Vec<&str> = result
        .actions
        .iter()
        .filter_map(|a| match a {
            Action::ServiceCreate { service_name } => Some(service_name.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        created.contains(&"web"),
        "web should be created now that redis is healthy: actions={:?}, deferred={:?}",
        result.actions,
        result.deferred
    );
    assert!(result.deferred.is_empty());
}

#[test]
fn web_redis_teardown_removes_both_services() {
    let spec = parse_compose(WEB_REDIS_COMPOSE, "web-redis").unwrap();
    let (store, _dir) = open_temp_store();
    let health = HashMap::new();

    // Apply to create services.
    let _first = vz_stack::apply(&spec, &store, &health).unwrap();
    simulate_running(&store, "web-redis", &["redis", "web"]);

    // Teardown: apply empty spec.
    let empty = StackSpec {
        name: "web-redis".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let result = vz_stack::apply(&empty, &store, &health).unwrap();

    let removed: Vec<&str> = result
        .actions
        .iter()
        .filter_map(|a| match a {
            Action::ServiceRemove { service_name } => Some(service_name.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(removed.len(), 2, "both services should be removed");
    assert!(removed.contains(&"redis"), "redis should be removed");
    assert!(removed.contains(&"web"), "web should be removed");
}

// ── Fixture: web + postgres + redis ───────────────────────────────

const WEB_PG_REDIS_COMPOSE: &str = r#"
services:
  web:
    image: myapp:latest
    ports:
      - "8080:80"
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      DATABASE_URL: postgresql://postgres:5432/app
      REDIS_URL: redis://redis:6379

  postgres:
    image: postgres:16-alpine
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: app
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secret
    volumes:
      - pg-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app"]
      interval: 5s
      timeout: 3s
      retries: 5
      start_period: 10s

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 3

volumes:
  pg-data:
  redis-data:
"#;

#[test]
fn web_pg_redis_parse_produces_valid_spec() {
    let spec = parse_compose(WEB_PG_REDIS_COMPOSE, "fullstack").unwrap();

    assert_eq!(spec.name, "fullstack");
    assert_eq!(spec.services.len(), 3);
    assert_eq!(spec.volumes.len(), 2);

    // Services sorted by name.
    let postgres = spec.services.iter().find(|s| s.name == "postgres").unwrap();
    let redis = spec.services.iter().find(|s| s.name == "redis").unwrap();
    let web = spec.services.iter().find(|s| s.name == "web").unwrap();

    assert_eq!(postgres.image, "postgres:16-alpine");
    assert!(postgres.healthcheck.is_some());
    let pg_hc = postgres.healthcheck.as_ref().unwrap();
    assert_eq!(pg_hc.start_period_secs, Some(10));
    assert_eq!(pg_hc.interval_secs, Some(5));

    assert_eq!(redis.image, "redis:7-alpine");
    assert!(redis.healthcheck.is_some());

    assert_eq!(web.depends_on.len(), 2);
    assert!(
        web.depends_on
            .contains(&ServiceDependency::healthy("postgres"))
    );
    assert!(
        web.depends_on
            .contains(&ServiceDependency::healthy("redis"))
    );

    // Volumes sorted by name.
    let vol_names: Vec<&str> = spec.volumes.iter().map(|v| v.name.as_str()).collect();
    assert_eq!(vol_names, vec!["pg-data", "redis-data"]);
}

#[test]
fn web_pg_redis_initial_apply_creates_all_three() {
    let spec = parse_compose(WEB_PG_REDIS_COMPOSE, "fullstack").unwrap();
    let (store, _dir) = open_temp_store();
    let health = HashMap::new();

    let result = vz_stack::apply(&spec, &store, &health).unwrap();

    let created: Vec<&str> = result
        .actions
        .iter()
        .filter_map(|a| match a {
            Action::ServiceCreate { service_name } => Some(service_name.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        created.len(),
        2,
        "only dependencies should be created first"
    );
    assert!(created.contains(&"postgres"));
    assert!(created.contains(&"redis"));
    assert_eq!(result.deferred.len(), 1);
    assert_eq!(result.deferred[0].service_name, "web");
}

#[test]
fn web_pg_redis_health_gates_web_on_both_deps() {
    let spec = parse_compose(WEB_PG_REDIS_COMPOSE, "fullstack").unwrap();
    let (store, _dir) = open_temp_store();
    let health = HashMap::new();

    // First apply.
    let _first = vz_stack::apply(&spec, &store, &health).unwrap();

    // Simulate: postgres running but unhealthy, redis running and healthy.
    store
        .save_observed_state(
            "fullstack",
            &ServiceObservedState {
                service_name: "postgres".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-pg".to_string()),
                last_error: None,
                ready: false,
            },
        )
        .unwrap();
    store
        .save_observed_state(
            "fullstack",
            &ServiceObservedState {
                service_name: "redis".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-redis".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let mut health = HashMap::new();
    let mut redis_h = HealthStatus::new("redis");
    redis_h.record_pass();
    health.insert("redis".to_string(), redis_h);
    // postgres: no health status (check hasn't passed).

    let result = vz_stack::apply(&spec, &store, &health).unwrap();

    let deferred_names: Vec<&str> = result
        .deferred
        .iter()
        .map(|d| d.service_name.as_str())
        .collect();

    assert!(
        deferred_names.contains(&"web"),
        "web should be deferred waiting on postgres: {deferred_names:?}"
    );
}

#[test]
fn web_pg_redis_all_healthy_converges() {
    let spec = parse_compose(WEB_PG_REDIS_COMPOSE, "fullstack").unwrap();
    let (store, _dir) = open_temp_store();
    let health = HashMap::new();

    // First apply.
    let _first = vz_stack::apply(&spec, &store, &health).unwrap();

    // Simulate all running and healthy.
    simulate_running(&store, "fullstack", &["postgres", "redis", "web"]);

    let mut health = HashMap::new();
    for name in &["postgres", "redis"] {
        let mut h = HealthStatus::new(name);
        h.record_pass();
        health.insert(name.to_string(), h);
    }

    let result = vz_stack::apply(&spec, &store, &health).unwrap();
    assert!(
        result.actions.is_empty(),
        "converged state should produce no actions: {:?}",
        result.actions
    );
    assert!(result.deferred.is_empty());
}

#[test]
fn web_pg_redis_teardown_removes_all_three() {
    let spec = parse_compose(WEB_PG_REDIS_COMPOSE, "fullstack").unwrap();
    let (store, _dir) = open_temp_store();
    let health = HashMap::new();

    let _first = vz_stack::apply(&spec, &store, &health).unwrap();
    simulate_running(&store, "fullstack", &["postgres", "redis", "web"]);

    let empty = StackSpec {
        name: "fullstack".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let result = vz_stack::apply(&empty, &store, &health).unwrap();

    let removed: Vec<&str> = result
        .actions
        .iter()
        .filter_map(|a| match a {
            Action::ServiceRemove { service_name } => Some(service_name.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(removed.len(), 3, "all three services should be removed");
    assert!(removed.contains(&"web"));
    assert!(removed.contains(&"postgres"));
    assert!(removed.contains(&"redis"));
}

// ── Event pipeline tests ──────────────────────────────────────────

#[test]
fn web_redis_apply_emits_events() {
    let spec = parse_compose(WEB_REDIS_COMPOSE, "web-redis").unwrap();
    let (store, _dir) = open_temp_store();
    let health = HashMap::new();

    let _result = vz_stack::apply(&spec, &store, &health).unwrap();

    let events = store.load_event_records("web-redis").unwrap();
    assert!(!events.is_empty(), "apply should emit events for fixture");

    // Should have at least a start event and creating events.
    let event_types: Vec<String> = events
        .iter()
        .map(|e| {
            serde_json::to_value(&e.event).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();

    assert!(
        event_types.contains(&"stack_apply_started".to_string()),
        "should emit stack_apply_started: {event_types:?}"
    );
    assert!(
        event_types.contains(&"service_creating".to_string()),
        "should emit service_creating: {event_types:?}"
    );
    assert!(
        event_types.contains(&"stack_apply_completed".to_string()),
        "should emit stack_apply_completed: {event_types:?}"
    );
}

#[test]
fn web_redis_event_streaming_since() {
    let spec = parse_compose(WEB_REDIS_COMPOSE, "web-redis").unwrap();
    let (store, _dir) = open_temp_store();
    let health = HashMap::new();

    // First apply.
    let _first = vz_stack::apply(&spec, &store, &health).unwrap();
    let events_after_first = store.load_event_records("web-redis").unwrap();
    let cursor = events_after_first.last().unwrap().id;

    // Simulate running, then re-apply.
    simulate_running(&store, "web-redis", &["redis", "web"]);

    let mut health2 = HashMap::new();
    let mut redis_h = HealthStatus::new("redis");
    redis_h.record_pass();
    health2.insert("redis".to_string(), redis_h);

    let _second = vz_stack::apply(&spec, &store, &health2).unwrap();

    // Events since cursor should only include second apply events.
    let new_events = store.load_events_since("web-redis", cursor).unwrap();
    assert!(
        !new_events.is_empty(),
        "should have events from second apply"
    );
    assert!(
        new_events[0].id > cursor,
        "new events should be after cursor"
    );
}

// ── Determinism proof ─────────────────────────────────────────────

#[test]
fn web_pg_redis_deterministic_across_runs() {
    let spec = parse_compose(WEB_PG_REDIS_COMPOSE, "fullstack").unwrap();

    let mut action_sequences: Vec<Vec<String>> = Vec::new();

    for _ in 0..5 {
        let (store, _dir) = open_temp_store();
        let health = HashMap::new();
        let result = vz_stack::apply(&spec, &store, &health).unwrap();
        let names: Vec<String> = result
            .actions
            .iter()
            .map(|a| a.service_name().to_string())
            .collect();
        action_sequences.push(names);
    }

    // All runs should produce the same sequence.
    for (i, seq) in action_sequences.iter().enumerate().skip(1) {
        assert_eq!(
            &action_sequences[0], seq,
            "run 0 and run {i} produced different action sequences"
        );
    }
}

#[test]
fn compose_build_fixture_derives_image_and_dependency_condition() {
    let yaml = r#"
services:
  api:
    build:
      context: .
      dockerfile: Dockerfile
    depends_on:
      db:
        condition: service_healthy
  db:
    image: postgres:16
    healthcheck:
      test: ["CMD", "pg_isready"]
"#;

    let spec = parse_compose(yaml, "build-fixture").unwrap();
    let api = spec.services.iter().find(|s| s.name == "api").unwrap();
    assert_eq!(api.image, "api:latest");
    assert_eq!(api.depends_on, vec![ServiceDependency::healthy("db")]);
}

#[test]
fn compose_fixture_unsupported_network_attachment_is_stable() {
    let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      frontend:
        aliases:
          - web-local
networks:
  frontend:
"#;

    let err = parse_compose(yaml, "unsupported-fixture").unwrap_err();
    let message = err.to_string();
    assert!(message.starts_with("unsupported_operation:"));
    assert!(message.contains("surface=compose"));
    assert!(message.contains("services.web.networks.frontend.aliases"));
}

// ── Helpers ────────────────────────────────────────────────────────

fn open_temp_store() -> (StateStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(&dir.path().join("state.db")).unwrap();
    (store, dir)
}

fn simulate_running(store: &StateStore, stack_name: &str, services: &[&str]) {
    for name in services {
        store
            .save_observed_state(
                stack_name,
                &ServiceObservedState {
                    service_name: name.to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some(format!("ctr-{name}")),
                    last_error: None,
                    ready: false,
                },
            )
            .unwrap();
    }
}
