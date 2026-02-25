//! Criterion benchmarks for [`StateStore`] operations.
//!
//! Establishes performance baselines for the critical SQLite-backed
//! state store. Run with:
//! ```bash
//! cd crates && cargo bench -p vz-stack
//! ```

use std::collections::HashMap;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use vz_stack::{
    IDEMPOTENCY_TTL_SECS, IdempotencyRecord, NetworkSpec, ServiceKind, ServiceObservedState,
    ServicePhase, ServiceSpec, StackEvent, StackSpec, StateStore,
};

// ── Helpers ─────────────────────────────────────────────────────────

fn minimal_service(name: &str) -> ServiceSpec {
    ServiceSpec {
        name: name.to_string(),
        kind: ServiceKind::Service,
        image: format!("{name}:latest"),
        command: None,
        entrypoint: None,
        environment: HashMap::from([("PORT".to_string(), "80".to_string())]),
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
    }
}

fn sample_spec(name: &str, n_services: usize) -> StackSpec {
    let services: Vec<ServiceSpec> = (0..n_services)
        .map(|i| minimal_service(&format!("svc-{i}")))
        .collect();
    StackSpec {
        name: name.to_string(),
        services,
        networks: vec![NetworkSpec {
            name: "default".to_string(),
            driver: "bridge".to_string(),
            subnet: None,
        }],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    }
}

// ── Benchmarks ──────────────────────────────────────────────────────

fn bench_save_desired_state(c: &mut Criterion) {
    let spec = sample_spec("bench-app", 5);
    c.bench_function("state_store/save_desired_state", |b| {
        b.iter_batched(
            || StateStore::in_memory().expect("in_memory store"),
            |store| {
                store
                    .save_desired_state("bench-app", &spec)
                    .expect("save_desired_state");
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_load_desired_state(c: &mut Criterion) {
    let spec = sample_spec("bench-app", 5);
    c.bench_function("state_store/load_desired_state", |b| {
        b.iter_batched(
            || {
                let store = StateStore::in_memory().expect("in_memory store");
                store
                    .save_desired_state("bench-app", &spec)
                    .expect("save_desired_state");
                store
            },
            |store| {
                store
                    .load_desired_state("bench-app")
                    .expect("load_desired_state");
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_save_observed_state(c: &mut Criterion) {
    c.bench_function("state_store/save_observed_state", |b| {
        b.iter_batched(
            || StateStore::in_memory().expect("in_memory store"),
            |store| {
                let state = ServiceObservedState {
                    service_name: "web".to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some("ctr-abc".to_string()),
                    last_error: None,
                    ready: true,
                };
                store
                    .save_observed_state("bench-app", &state)
                    .expect("save_observed_state");
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_load_observed_state(c: &mut Criterion) {
    c.bench_function("state_store/load_observed_state", |b| {
        b.iter_batched(
            || {
                let store = StateStore::in_memory().expect("in_memory store");
                for i in 0..10 {
                    let state = ServiceObservedState {
                        service_name: format!("svc-{i}"),
                        phase: ServicePhase::Running,
                        container_id: Some(format!("ctr-{i}")),
                        last_error: None,
                        ready: true,
                    };
                    store
                        .save_observed_state("bench-app", &state)
                        .expect("save");
                }
                store
            },
            |store| {
                store
                    .load_observed_state("bench-app")
                    .expect("load_observed_state");
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_emit_event(c: &mut Criterion) {
    c.bench_function("state_store/emit_event", |b| {
        b.iter_batched(
            || StateStore::in_memory().expect("in_memory store"),
            |store| {
                store
                    .emit_event(
                        "bench-app",
                        &StackEvent::ServiceCreating {
                            stack_name: "bench-app".to_string(),
                            service_name: "web".to_string(),
                        },
                    )
                    .expect("emit_event");
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_event_insert_1000(c: &mut Criterion) {
    c.bench_function("state_store/event_insert_1000", |b| {
        b.iter_batched(
            || StateStore::in_memory().expect("in_memory store"),
            |store| {
                for i in 0..1000 {
                    store
                        .emit_event(
                            "bench-app",
                            &StackEvent::ServiceCreating {
                                stack_name: "bench-app".to_string(),
                                service_name: format!("svc-{i}"),
                            },
                        )
                        .expect("emit_event");
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_event_query_with_cursor(c: &mut Criterion) {
    c.bench_function("state_store/event_query_cursor_1000", |b| {
        b.iter_batched(
            || {
                let store = StateStore::in_memory().expect("in_memory store");
                for i in 0..1000 {
                    store
                        .emit_event(
                            "bench-app",
                            &StackEvent::ServiceCreating {
                                stack_name: "bench-app".to_string(),
                                service_name: format!("svc-{i}"),
                            },
                        )
                        .expect("emit_event");
                }
                store
            },
            |store| {
                // Query from midpoint cursor
                let _records = store
                    .load_events_since_limited("bench-app", 500, 100)
                    .expect("load_events_since_limited");
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_idempotency_key_lookup(c: &mut Criterion) {
    c.bench_function("state_store/idempotency_key_lookup", |b| {
        b.iter_batched(
            || {
                let store = StateStore::in_memory().expect("in_memory store");
                for i in 0..100 {
                    let record = IdempotencyRecord {
                        key: format!("key-{i}"),
                        operation: "create_sandbox".to_string(),
                        request_hash: format!("hash-{i}"),
                        response_json: r#"{"sandbox_id":"sb-1"}"#.to_string(),
                        status_code: 201,
                        created_at: 1_700_000_000,
                        expires_at: 1_700_000_000 + IDEMPOTENCY_TTL_SECS,
                    };
                    store
                        .save_idempotency_result(&record)
                        .expect("save_idempotency");
                }
                store
            },
            |store| {
                let _result = store
                    .find_idempotency_result("key-50")
                    .expect("find_idempotency");
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_large_desired_state_round_trip(c: &mut Criterion) {
    let spec = sample_spec("large-app", 50);
    c.bench_function("state_store/large_spec_50_services_round_trip", |b| {
        b.iter_batched(
            || StateStore::in_memory().expect("in_memory store"),
            |store| {
                store.save_desired_state("large-app", &spec).expect("save");
                let _loaded = store.load_desired_state("large-app").expect("load");
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    benches,
    bench_save_desired_state,
    bench_load_desired_state,
    bench_save_observed_state,
    bench_load_observed_state,
    bench_emit_event,
    bench_event_insert_1000,
    bench_event_query_with_cursor,
    bench_idempotency_key_lookup,
    bench_large_desired_state_round_trip,
);
criterion_main!(benches);
