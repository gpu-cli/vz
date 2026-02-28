#![allow(clippy::unwrap_used)]

use super::*;
use crate::spec::{NetworkSpec, ServiceKind, ServiceSpec, VolumeSpec};
use std::collections::HashMap;

fn sample_spec() -> StackSpec {
    StackSpec {
        name: "myapp".to_string(),
        services: vec![
            ServiceSpec {
                name: "web".to_string(),
                kind: ServiceKind::Service,
                image: "nginx:latest".to_string(),
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
                expose: vec![],
                stdin_open: false,
                tty: false,
                logging: None,
            },
            ServiceSpec {
                name: "db".to_string(),
                kind: ServiceKind::Service,
                image: "postgres:16".to_string(),
                command: None,
                entrypoint: None,
                environment: HashMap::from([(
                    "POSTGRES_PASSWORD".to_string(),
                    "secret".to_string(),
                )]),
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
            },
        ],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    }
}

#[test]
fn desired_state_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let spec = sample_spec();

    store.save_desired_state("myapp", &spec).unwrap();
    let loaded = store.load_desired_state("myapp").unwrap();
    assert_eq!(loaded, Some(spec));
}

#[test]
fn desired_state_missing_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.load_desired_state("nonexistent").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn desired_state_upsert_replaces() {
    let store = StateStore::in_memory().unwrap();
    let spec1 = sample_spec();

    store.save_desired_state("myapp", &spec1).unwrap();

    let spec2 = StackSpec {
        name: "myapp".to_string(),
        services: vec![],
        networks: vec![NetworkSpec {
            name: "net1".to_string(),
            driver: "bridge".to_string(),
            subnet: None,
        }],
        volumes: vec![VolumeSpec {
            name: "vol1".to_string(),
            driver: "local".to_string(),
            driver_opts: None,
        }],
        secrets: vec![],
        disk_size_mb: None,
    };

    store.save_desired_state("myapp", &spec2).unwrap();
    let loaded = store.load_desired_state("myapp").unwrap().unwrap();
    assert_eq!(loaded, spec2);
    assert!(loaded.services.is_empty());
}

#[test]
fn service_mount_digest_round_trip_and_delete() {
    let store = StateStore::in_memory().unwrap();

    store
        .save_service_mount_digest("myapp", "web", "digest-web-v1")
        .unwrap();
    store
        .save_service_mount_digest("myapp", "db", "digest-db-v1")
        .unwrap();

    let digests = store.load_service_mount_digests("myapp").unwrap();
    assert_eq!(digests.len(), 2);
    assert_eq!(digests.get("web"), Some(&"digest-web-v1".to_string()));
    assert_eq!(digests.get("db"), Some(&"digest-db-v1".to_string()));

    store
        .save_service_mount_digest("myapp", "web", "digest-web-v2")
        .unwrap();
    let digests = store.load_service_mount_digests("myapp").unwrap();
    assert_eq!(digests.get("web"), Some(&"digest-web-v2".to_string()));

    store.delete_service_mount_digest("myapp", "db").unwrap();
    let digests = store.load_service_mount_digests("myapp").unwrap();
    assert_eq!(digests.len(), 1);
    assert!(digests.get("db").is_none());
}

#[test]
fn reconcile_progress_round_trip_and_clear() {
    let store = StateStore::in_memory().unwrap();
    let actions = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
    ];

    store
        .save_reconcile_progress("myapp", "op-1", &actions, 0)
        .unwrap();

    let progress = store.load_reconcile_progress("myapp").unwrap().unwrap();
    assert_eq!(progress.operation_id, "op-1");
    assert_eq!(progress.next_action_index, 0);
    assert_eq!(progress.actions, actions);

    store
        .save_reconcile_progress("myapp", "op-1", &progress.actions, 1)
        .unwrap();
    let updated = store.load_reconcile_progress("myapp").unwrap().unwrap();
    assert_eq!(updated.next_action_index, 1);
    assert_eq!(updated.actions.len(), 2);

    store.clear_reconcile_progress("myapp").unwrap();
    assert!(store.load_reconcile_progress("myapp").unwrap().is_none());
}

#[test]
fn observed_state_round_trip() {
    let store = StateStore::in_memory().unwrap();

    let state1 = ServiceObservedState {
        service_name: "web".to_string(),
        phase: ServicePhase::Running,
        container_id: Some("ctr-abc".to_string()),
        last_error: None,
        ready: true,
    };

    let state2 = ServiceObservedState {
        service_name: "db".to_string(),
        phase: ServicePhase::Pending,
        container_id: None,
        last_error: None,
        ready: false,
    };

    store.save_observed_state("myapp", &state1).unwrap();
    store.save_observed_state("myapp", &state2).unwrap();

    let states = store.load_observed_state("myapp").unwrap();
    assert_eq!(states.len(), 2);
    assert!(states.iter().any(|s| s.service_name == "web"));
    assert!(states.iter().any(|s| s.service_name == "db"));
}

#[test]
fn resolve_service_tty_for_container_returns_desired_service_tty() {
    let store = StateStore::in_memory().unwrap();

    let mut spec = sample_spec();
    spec.services[0].tty = true;
    store.save_desired_state("myapp", &spec).unwrap();
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-web-1".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let resolved = store
        .resolve_service_tty_for_container("ctr-web-1")
        .unwrap();
    assert_eq!(resolved, Some(true));
}

#[test]
fn resolve_service_tty_for_container_returns_none_when_unmapped() {
    let store = StateStore::in_memory().unwrap();
    store.save_desired_state("myapp", &sample_spec()).unwrap();
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-web-1".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let resolved_missing = store
        .resolve_service_tty_for_container("ctr-missing")
        .unwrap();
    assert!(resolved_missing.is_none());
}

#[test]
fn resolve_service_exec_pty_default_for_container_uses_stdin_open_or_tty() {
    let store = StateStore::in_memory().unwrap();

    let mut spec = sample_spec();
    spec.services[0].tty = false;
    spec.services[0].stdin_open = true;
    store.save_desired_state("myapp", &spec).unwrap();
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-web-stdin".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let resolved = store
        .resolve_service_exec_pty_default_for_container("ctr-web-stdin")
        .unwrap();
    assert_eq!(resolved, Some(true));
}

#[test]
fn resolve_service_exec_pty_default_for_container_returns_none_when_unmapped() {
    let store = StateStore::in_memory().unwrap();
    store.save_desired_state("myapp", &sample_spec()).unwrap();
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-web-1".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let resolved_missing = store
        .resolve_service_exec_pty_default_for_container("ctr-missing")
        .unwrap();
    assert!(resolved_missing.is_none());
}

#[test]
fn observed_state_upsert_updates_service() {
    let store = StateStore::in_memory().unwrap();

    let initial = ServiceObservedState {
        service_name: "web".to_string(),
        phase: ServicePhase::Creating,
        container_id: None,
        last_error: None,
        ready: false,
    };

    store.save_observed_state("myapp", &initial).unwrap();

    let updated = ServiceObservedState {
        service_name: "web".to_string(),
        phase: ServicePhase::Running,
        container_id: Some("ctr-xyz".to_string()),
        last_error: None,
        ready: true,
    };

    store.save_observed_state("myapp", &updated).unwrap();

    let states = store.load_observed_state("myapp").unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].phase, ServicePhase::Running);
    assert_eq!(states[0].container_id, Some("ctr-xyz".to_string()));
}

#[test]
fn observed_state_empty_returns_empty_vec() {
    let store = StateStore::in_memory().unwrap();
    let states = store.load_observed_state("empty").unwrap();
    assert!(states.is_empty());
}

#[test]
fn health_poller_state_round_trip_and_clear() {
    let store = StateStore::in_memory().unwrap();
    let mut state = HashMap::new();
    state.insert(
        "web".to_string(),
        HealthPollState {
            service_name: "web".to_string(),
            consecutive_passes: 2,
            consecutive_failures: 1,
            last_check_millis: Some(1_700_000_000_000),
            start_time_millis: Some(1_700_000_000_123),
        },
    );

    store.save_health_poller_state("myapp", &state).unwrap();
    let loaded = store.load_health_poller_state("myapp").unwrap();
    assert_eq!(loaded.get("web").unwrap(), state.get("web").unwrap());

    store.clear_health_poller_state("myapp").unwrap();
    let cleared = store.load_health_poller_state("myapp").unwrap();
    assert!(cleared.is_empty());
}

#[test]
fn events_emit_and_load() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 2,
            },
        )
        .unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyCompleted {
                stack_name: "myapp".to_string(),
                succeeded: 2,
                failed: 0,
            },
        )
        .unwrap();

    let events = store.load_events("myapp").unwrap();
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0], StackEvent::StackApplyStarted { .. }));
    assert!(matches!(events[1], StackEvent::StackApplyCompleted { .. }));
}

#[test]
fn events_empty_returns_empty_vec() {
    let store = StateStore::in_memory().unwrap();
    let events = store.load_events("empty").unwrap();
    assert!(events.is_empty());
}

#[test]
fn events_scoped_by_stack_name() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "app1",
            &StackEvent::StackApplyStarted {
                stack_name: "app1".to_string(),
                services_count: 1,
            },
        )
        .unwrap();

    store
        .emit_event(
            "app2",
            &StackEvent::StackApplyStarted {
                stack_name: "app2".to_string(),
                services_count: 5,
            },
        )
        .unwrap();

    let app1_events = store.load_events("app1").unwrap();
    assert_eq!(app1_events.len(), 1);
    let app2_events = store.load_events("app2").unwrap();
    assert_eq!(app2_events.len(), 1);
}

#[test]
fn multiple_stacks_isolated() {
    let store = StateStore::in_memory().unwrap();

    let spec1 = StackSpec {
        name: "app1".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let spec2 = StackSpec {
        name: "app2".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };

    store.save_desired_state("app1", &spec1).unwrap();
    store.save_desired_state("app2", &spec2).unwrap();

    let loaded1 = store.load_desired_state("app1").unwrap().unwrap();
    let loaded2 = store.load_desired_state("app2").unwrap().unwrap();

    assert_eq!(loaded1.name, "app1");
    assert_eq!(loaded2.name, "app2");
}

// ── B17: Event pipeline tests ──

#[test]
fn event_records_include_id_and_timestamp() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::ServiceCreating {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
        )
        .unwrap();

    let records = store.load_event_records("myapp").unwrap();
    assert_eq!(records.len(), 1);
    assert!(records[0].id > 0);
    assert!(!records[0].created_at.is_empty());
    assert_eq!(records[0].stack_name, "myapp");
    assert!(matches!(
        records[0].event,
        StackEvent::ServiceCreating { .. }
    ));
}

#[test]
fn load_events_since_returns_only_newer_events() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 1,
            },
        )
        .unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::ServiceCreating {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
        )
        .unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::ServiceReady {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
                runtime_id: "ctr-1".to_string(),
            },
        )
        .unwrap();

    let all = store.load_event_records("myapp").unwrap();
    assert_eq!(all.len(), 3);

    // Stream from after the first event.
    let cursor = all[0].id;
    let newer = store.load_events_since("myapp", cursor).unwrap();
    assert_eq!(newer.len(), 2);
    assert!(matches!(newer[0].event, StackEvent::ServiceCreating { .. }));
    assert!(matches!(newer[1].event, StackEvent::ServiceReady { .. }));

    // Stream from after the second event.
    let cursor2 = newer[0].id;
    let newest = store.load_events_since("myapp", cursor2).unwrap();
    assert_eq!(newest.len(), 1);
    assert!(matches!(newest[0].event, StackEvent::ServiceReady { .. }));
}

#[test]
fn load_events_since_with_zero_returns_all() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 1,
            },
        )
        .unwrap();

    let all = store.load_events_since("myapp", 0).unwrap();
    assert_eq!(all.len(), 1);
}

#[test]
fn load_events_since_with_future_cursor_returns_empty() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 1,
            },
        )
        .unwrap();

    let empty = store.load_events_since("myapp", 999_999).unwrap();
    assert!(empty.is_empty());
}

#[test]
fn load_events_since_limited_applies_limit_and_order() {
    let store = StateStore::in_memory().unwrap();
    for index in 0..3 {
        store
            .emit_event(
                "myapp",
                &StackEvent::ServiceCreating {
                    stack_name: "myapp".to_string(),
                    service_name: format!("svc-{index}"),
                },
            )
            .unwrap();
    }

    let first_page = store.load_events_since_limited("myapp", 0, 2).unwrap();
    assert_eq!(first_page.len(), 2);
    assert!(first_page[0].id < first_page[1].id);

    let second_page = store
        .load_events_since_limited("myapp", first_page[1].id, 2)
        .unwrap();
    assert_eq!(second_page.len(), 1);
    assert!(second_page[0].id > first_page[1].id);
}

#[test]
fn event_count_returns_correct_total() {
    let store = StateStore::in_memory().unwrap();

    assert_eq!(store.event_count("myapp").unwrap(), 0);

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 1,
            },
        )
        .unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyCompleted {
                stack_name: "myapp".to_string(),
                succeeded: 1,
                failed: 0,
            },
        )
        .unwrap();

    assert_eq!(store.event_count("myapp").unwrap(), 2);
    assert_eq!(store.event_count("other").unwrap(), 0);
}

#[test]
fn event_records_ids_are_monotonically_increasing() {
    let store = StateStore::in_memory().unwrap();

    for i in 0..5 {
        store
            .emit_event(
                "myapp",
                &StackEvent::ServiceCreating {
                    stack_name: "myapp".to_string(),
                    service_name: format!("svc-{i}"),
                },
            )
            .unwrap();
    }

    let records = store.load_event_records("myapp").unwrap();
    assert_eq!(records.len(), 5);
    for window in records.windows(2) {
        assert!(window[1].id > window[0].id);
    }
}

#[test]
fn new_event_variants_persist_and_load() {
    let store = StateStore::in_memory().unwrap();

    let events = vec![
        StackEvent::ServiceStopping {
            stack_name: "myapp".to_string(),
            service_name: "web".to_string(),
        },
        StackEvent::ServiceStopped {
            stack_name: "myapp".to_string(),
            service_name: "web".to_string(),
            exit_code: 137,
        },
        StackEvent::PortConflict {
            stack_name: "myapp".to_string(),
            service_name: "web".to_string(),
            port: 8080,
        },
        StackEvent::VolumeCreated {
            stack_name: "myapp".to_string(),
            volume_name: "dbdata".to_string(),
        },
        StackEvent::StackDestroyed {
            stack_name: "myapp".to_string(),
        },
    ];

    for event in &events {
        store.emit_event("myapp", event).unwrap();
    }

    let loaded = store.load_events("myapp").unwrap();
    assert_eq!(loaded, events);
}

// ── Real-time event streaming tests ──

#[test]
fn emit_event_sends_to_channel() {
    use std::sync::mpsc;

    let mut store = StateStore::in_memory().unwrap();
    let (tx, rx) = mpsc::channel();
    store.set_event_sender(tx);

    store
        .emit_event(
            "test",
            &StackEvent::StackDestroyed {
                stack_name: "test".to_string(),
            },
        )
        .unwrap();

    let received = rx.try_recv().unwrap();
    assert!(matches!(received, StackEvent::StackDestroyed { .. }));
}

#[test]
fn emit_event_without_sender_works() {
    let store = StateStore::in_memory().unwrap();
    // No sender set — should not error.
    store
        .emit_event(
            "test",
            &StackEvent::StackDestroyed {
                stack_name: "test".to_string(),
            },
        )
        .unwrap();
}

#[test]
fn emit_event_ignores_dropped_receiver() {
    use std::sync::mpsc;

    let mut store = StateStore::in_memory().unwrap();
    let (tx, rx) = mpsc::channel();
    store.set_event_sender(tx);

    // Drop the receiver so sends fail.
    drop(rx);

    // Should not error even though receiver is gone.
    store
        .emit_event(
            "test",
            &StackEvent::StackDestroyed {
                stack_name: "test".to_string(),
            },
        )
        .unwrap();

    // Event should still be persisted to SQLite.
    let events = store.load_events("test").unwrap();
    assert_eq!(events.len(), 1);
}

// ── Event compaction tests ──

fn emit_n_events(store: &StateStore, stack_name: &str, n: usize) {
    for i in 0..n {
        store
            .emit_event(
                stack_name,
                &StackEvent::ServiceCreating {
                    stack_name: stack_name.to_string(),
                    service_name: format!("svc-{i}"),
                },
            )
            .unwrap();
    }
}

#[test]
fn compact_events_by_count_keeps_recent() {
    let store = StateStore::in_memory().unwrap();
    emit_n_events(&store, "myapp", 20);

    assert_eq!(store.event_count("myapp").unwrap(), 20);

    let deleted = store.compact_events_by_count("myapp", 10).unwrap();
    assert_eq!(deleted, 10);
    assert_eq!(store.event_count("myapp").unwrap(), 10);

    // The kept events should be the most recent 10 (IDs 11..=20).
    let records = store.load_event_records("myapp").unwrap();
    assert_eq!(records.len(), 10);
    // Verify ordering is ascending by id and that the oldest kept is > 10.
    assert!(records[0].id > 10);
}

#[test]
fn compact_events_by_count_noop_when_under_limit() {
    let store = StateStore::in_memory().unwrap();
    emit_n_events(&store, "myapp", 5);

    let deleted = store.compact_events_by_count("myapp", 10).unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(store.event_count("myapp").unwrap(), 5);
}

#[test]
fn compact_events_by_count_scoped_to_stack() {
    let store = StateStore::in_memory().unwrap();
    emit_n_events(&store, "app-a", 15);
    emit_n_events(&store, "app-b", 5);

    let deleted = store.compact_events_by_count("app-a", 10).unwrap();
    assert_eq!(deleted, 5);
    assert_eq!(store.event_count("app-a").unwrap(), 10);
    // app-b is untouched.
    assert_eq!(store.event_count("app-b").unwrap(), 5);
}

#[test]
fn compact_events_by_age_deletes_old() {
    let store = StateStore::in_memory().unwrap();
    emit_n_events(&store, "myapp", 5);

    // Back-date all events to 2 hours ago so they are clearly old.
    store
        .conn
        .execute(
            "UPDATE events SET created_at = datetime('now', '-7200 seconds') WHERE stack_name = 'myapp'",
            [],
        )
        .unwrap();

    // Delete events older than 1 hour (3600 seconds). All 5 should be removed.
    let deleted = store.compact_events("myapp", 3600).unwrap();
    assert_eq!(deleted, 5);
    assert_eq!(store.event_count("myapp").unwrap(), 0);
}

#[test]
fn compact_events_by_age_keeps_recent() {
    let store = StateStore::in_memory().unwrap();
    emit_n_events(&store, "myapp", 5);

    // With a generous window (1 hour), nothing should be deleted
    // because the events were just created.
    let deleted = store.compact_events("myapp", 3600).unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(store.event_count("myapp").unwrap(), 5);
}

#[test]
fn compact_events_by_age_partial_delete() {
    let store = StateStore::in_memory().unwrap();
    emit_n_events(&store, "myapp", 5);

    // Back-date 3 events to 2 hours ago, leave 2 at current time.
    store
        .conn
        .execute(
            "UPDATE events SET created_at = datetime('now', '-7200 seconds')
             WHERE stack_name = 'myapp' AND id IN (
                 SELECT id FROM events WHERE stack_name = 'myapp' ORDER BY id ASC LIMIT 3
             )",
            [],
        )
        .unwrap();

    let deleted = store.compact_events("myapp", 3600).unwrap();
    assert_eq!(deleted, 3);
    assert_eq!(store.event_count("myapp").unwrap(), 2);
}

#[test]
fn compact_events_default_applies_both_policies() {
    let store = StateStore::in_memory().unwrap();
    // Emit more than the default max (10,000).
    emit_n_events(&store, "myapp", 10_050);
    assert_eq!(store.event_count("myapp").unwrap(), 10_050);

    let deleted = store.compact_events_default("myapp").unwrap();
    // Age-based deletes 0 (all recent), count-based deletes 50.
    assert_eq!(deleted, 50);
    assert_eq!(store.event_count("myapp").unwrap(), 10_000);
}

#[test]
fn event_count_empty_stack() {
    let store = StateStore::in_memory().unwrap();
    assert_eq!(store.event_count("nonexistent").unwrap(), 0);
}

#[test]
fn compact_events_empty_stack() {
    let store = StateStore::in_memory().unwrap();
    let deleted = store.compact_events("nonexistent", 0).unwrap();
    assert_eq!(deleted, 0);
    let deleted = store.compact_events_by_count("nonexistent", 10).unwrap();
    assert_eq!(deleted, 0);
}

// ── Sandbox persistence tests ──

fn sample_sandbox(id: &str, stack_name: &str) -> Sandbox {
    use std::collections::BTreeMap;
    let mut labels = BTreeMap::new();
    labels.insert("stack_name".to_string(), stack_name.to_string());
    Sandbox {
        sandbox_id: id.to_string(),
        backend: SandboxBackend::MacosVz,
        spec: SandboxSpec::default(),
        state: SandboxState::Creating,
        created_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        labels,
    }
}

#[test]
fn sandbox_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let sandbox = sample_sandbox("sb-1", "myapp");

    store.save_sandbox(&sandbox).unwrap();
    let loaded = store.load_sandbox("sb-1").unwrap().unwrap();
    assert_eq!(loaded, sandbox);
}

#[test]
fn sandbox_for_stack_lookup() {
    let store = StateStore::in_memory().unwrap();
    let sandbox = sample_sandbox("sb-2", "myapp");

    store.save_sandbox(&sandbox).unwrap();
    let loaded = store.load_sandbox_for_stack("myapp").unwrap().unwrap();
    assert_eq!(loaded.sandbox_id, "sb-2");
}

#[test]
fn sandbox_list_returns_all() {
    let store = StateStore::in_memory().unwrap();
    let sb1 = sample_sandbox("sb-a", "app1");
    let mut sb2 = sample_sandbox("sb-b", "app2");
    sb2.created_at = 1_700_000_001;

    store.save_sandbox(&sb1).unwrap();
    store.save_sandbox(&sb2).unwrap();

    let all = store.list_sandboxes().unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn sandbox_delete_removes() {
    let store = StateStore::in_memory().unwrap();
    let sandbox = sample_sandbox("sb-del", "myapp");

    store.save_sandbox(&sandbox).unwrap();
    store.delete_sandbox("sb-del").unwrap();
    let loaded = store.load_sandbox("sb-del").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn sandbox_upsert_updates_state() {
    let store = StateStore::in_memory().unwrap();
    let mut sandbox = sample_sandbox("sb-up", "myapp");

    store.save_sandbox(&sandbox).unwrap();

    sandbox.state = SandboxState::Ready;
    sandbox.updated_at = 1_700_000_100;
    store.save_sandbox(&sandbox).unwrap();

    let loaded = store.load_sandbox("sb-up").unwrap().unwrap();
    assert_eq!(loaded.state, SandboxState::Ready);
    assert_eq!(loaded.updated_at, 1_700_000_100);
}

#[test]
fn allocator_state_round_trip() {
    let store = StateStore::in_memory().unwrap();

    let snapshot = AllocatorSnapshot {
        ports: HashMap::from([(
            "web".to_string(),
            vec![PublishedPort {
                protocol: "tcp".to_string(),
                container_port: 80,
                host_port: 8080,
            }],
        )]),
        service_ips: HashMap::from([("web".to_string(), "10.0.0.2".to_string())]),
        mount_tag_offsets: HashMap::from([("web".to_string(), 3)]),
    };

    store.save_allocator_state("myapp", &snapshot).unwrap();
    let loaded = store.load_allocator_state("myapp").unwrap().unwrap();
    assert_eq!(loaded, snapshot);
}

// ── Reconcile session tests ──

fn sample_session(id: &str, stack: &str) -> ReconcileSession {
    ReconcileSession {
        session_id: id.to_string(),
        stack_name: stack.to_string(),
        operation_id: "op-1".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: "abcdef0123456789".to_string(),
        next_action_index: 0,
        total_actions: 2,
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    }
}

fn sample_actions() -> Vec<Action> {
    vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
    ]
}

#[test]
fn reconcile_session_create_and_load_active() {
    let store = StateStore::in_memory().unwrap();
    let session = sample_session("rs-1", "myapp");
    let actions = sample_actions();

    store.create_reconcile_session(&session, &actions).unwrap();

    let loaded = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(loaded.session_id, "rs-1");
    assert_eq!(loaded.stack_name, "myapp");
    assert_eq!(loaded.status, ReconcileSessionStatus::Active);
    assert_eq!(loaded.actions_hash, "abcdef0123456789");
    assert_eq!(loaded.next_action_index, 0);
    assert_eq!(loaded.total_actions, 2);
}

#[test]
fn reconcile_session_update_progress() {
    let store = StateStore::in_memory().unwrap();
    let session = sample_session("rs-2", "myapp");
    store
        .create_reconcile_session(&session, &sample_actions())
        .unwrap();

    store
        .update_reconcile_session_progress("rs-2", 1, &ReconcileSessionStatus::Active)
        .unwrap();

    let loaded = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(loaded.next_action_index, 1);
    assert_eq!(loaded.status, ReconcileSessionStatus::Active);
}

#[test]
fn reconcile_session_complete() {
    let store = StateStore::in_memory().unwrap();
    let session = sample_session("rs-3", "myapp");
    store
        .create_reconcile_session(&session, &sample_actions())
        .unwrap();

    store.complete_reconcile_session("rs-3").unwrap();

    // Active load should return None since it's completed now.
    let active = store.load_active_reconcile_session("myapp").unwrap();
    assert!(active.is_none());

    // List should show it as completed.
    let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Completed);
    assert!(sessions[0].completed_at.is_some());
}

#[test]
fn reconcile_session_fail() {
    let store = StateStore::in_memory().unwrap();
    let session = sample_session("rs-4", "myapp");
    store
        .create_reconcile_session(&session, &sample_actions())
        .unwrap();

    store.fail_reconcile_session("rs-4").unwrap();

    let active = store.load_active_reconcile_session("myapp").unwrap();
    assert!(active.is_none());

    let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Failed);
    assert!(sessions[0].completed_at.is_some());
}

#[test]
fn reconcile_session_supersede_active() {
    let store = StateStore::in_memory().unwrap();

    let session1 = sample_session("rs-5", "myapp");
    store
        .create_reconcile_session(&session1, &sample_actions())
        .unwrap();

    let count = store.supersede_active_sessions("myapp").unwrap();
    assert_eq!(count, 1);

    let active = store.load_active_reconcile_session("myapp").unwrap();
    assert!(active.is_none());

    let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Superseded);
}

#[test]
fn reconcile_session_list_respects_limit_and_ordering() {
    let store = StateStore::in_memory().unwrap();

    for i in 0..5 {
        let mut session = sample_session(&format!("rs-{i}"), "myapp");
        session.started_at = 1_700_000_000 + i as u64;
        session.updated_at = session.started_at;
        store.complete_reconcile_session(&format!("rs-{i}")).ok();
        store
            .create_reconcile_session(&session, &sample_actions())
            .unwrap();
        store
            .complete_reconcile_session(&format!("rs-{i}"))
            .unwrap();
    }

    let all = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(all.len(), 5);
    // Ordered by started_at DESC.
    assert!(all[0].started_at >= all[1].started_at);

    let limited = store.list_reconcile_sessions("myapp", 2).unwrap();
    assert_eq!(limited.len(), 2);
}

#[test]
fn reconcile_session_no_active_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let active = store.load_active_reconcile_session("nonexistent").unwrap();
    assert!(active.is_none());
}

#[test]
fn reconcile_session_stacks_are_isolated() {
    let store = StateStore::in_memory().unwrap();

    let s1 = sample_session("rs-a1", "app1");
    let s2 = sample_session("rs-b1", "app2");
    store
        .create_reconcile_session(&s1, &sample_actions())
        .unwrap();
    store
        .create_reconcile_session(&s2, &sample_actions())
        .unwrap();

    let active1 = store
        .load_active_reconcile_session("app1")
        .unwrap()
        .unwrap();
    assert_eq!(active1.session_id, "rs-a1");

    let active2 = store
        .load_active_reconcile_session("app2")
        .unwrap()
        .unwrap();
    assert_eq!(active2.session_id, "rs-b1");

    // Supersede only app1.
    store.supersede_active_sessions("app1").unwrap();
    assert!(
        store
            .load_active_reconcile_session("app1")
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_active_reconcile_session("app2")
            .unwrap()
            .is_some()
    );
}

// ── Idempotency key persistence tests ──

fn sample_idempotency_record(key: &str) -> IdempotencyRecord {
    IdempotencyRecord {
        key: key.to_string(),
        operation: "create_sandbox".to_string(),
        request_hash: "abc123".to_string(),
        response_json: r#"{"sandbox_id":"sbx-1"}"#.to_string(),
        status_code: 201,
        created_at: 1_700_000_000,
        expires_at: 1_700_000_000 + IDEMPOTENCY_TTL_SECS,
    }
}

#[test]
fn idempotency_save_and_find_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let record = sample_idempotency_record("ik-1");

    store.save_idempotency_result(&record).unwrap();
    let loaded = store.find_idempotency_result("ik-1").unwrap().unwrap();
    assert_eq!(loaded.key, "ik-1");
    assert_eq!(loaded.operation, "create_sandbox");
    assert_eq!(loaded.request_hash, "abc123");
    assert_eq!(loaded.response_json, r#"{"sandbox_id":"sbx-1"}"#);
    assert_eq!(loaded.status_code, 201);
    assert_eq!(loaded.created_at, 1_700_000_000);
    assert_eq!(loaded.expires_at, 1_700_000_000 + IDEMPOTENCY_TTL_SECS);
}

#[test]
fn idempotency_missing_key_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.find_idempotency_result("nonexistent").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn idempotency_cleanup_removes_expired_keys() {
    let store = StateStore::in_memory().unwrap();

    // Record with expires_at in the past (epoch 0 + TTL = ~1 day).
    let expired = IdempotencyRecord {
        key: "ik-expired".to_string(),
        operation: "create_sandbox".to_string(),
        request_hash: "hash1".to_string(),
        response_json: "{}".to_string(),
        status_code: 201,
        created_at: 0,
        expires_at: 1, // Far in the past
    };
    store.save_idempotency_result(&expired).unwrap();

    // Record with expires_at far in the future.
    let fresh = IdempotencyRecord {
        key: "ik-fresh".to_string(),
        operation: "create_sandbox".to_string(),
        request_hash: "hash2".to_string(),
        response_json: "{}".to_string(),
        status_code: 201,
        created_at: 1_700_000_000,
        expires_at: u64::MAX / 2, // Far in the future
    };
    store.save_idempotency_result(&fresh).unwrap();

    let deleted = store.cleanup_expired_idempotency_keys().unwrap();
    assert_eq!(deleted, 1);

    // Expired key is gone.
    assert!(
        store
            .find_idempotency_result("ik-expired")
            .unwrap()
            .is_none()
    );
    // Fresh key is still present.
    assert!(store.find_idempotency_result("ik-fresh").unwrap().is_some());
}

// ── Lease persistence tests ──

fn sample_lease(id: &str, sandbox_id: &str) -> Lease {
    Lease {
        lease_id: id.to_string(),
        sandbox_id: sandbox_id.to_string(),
        ttl_secs: 300,
        last_heartbeat_at: 1_700_000_000,
        state: LeaseState::Opening,
    }
}

#[test]
fn lease_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let lease = sample_lease("ls-1", "sb-1");

    store.save_lease(&lease).unwrap();
    let loaded = store.load_lease("ls-1").unwrap().unwrap();
    assert_eq!(loaded, lease);
}

#[test]
fn lease_list_for_sandbox() {
    let store = StateStore::in_memory().unwrap();
    let lease1 = sample_lease("ls-a", "sb-1");
    let lease2 = sample_lease("ls-b", "sb-1");
    let lease3 = sample_lease("ls-c", "sb-2");

    store.save_lease(&lease1).unwrap();
    store.save_lease(&lease2).unwrap();
    store.save_lease(&lease3).unwrap();

    let sb1_leases = store.list_leases_for_sandbox("sb-1").unwrap();
    assert_eq!(sb1_leases.len(), 2);

    let sb2_leases = store.list_leases_for_sandbox("sb-2").unwrap();
    assert_eq!(sb2_leases.len(), 1);
}

#[test]
fn lease_list_returns_all() {
    let store = StateStore::in_memory().unwrap();
    let lease1 = sample_lease("ls-x", "sb-1");
    let lease2 = sample_lease("ls-y", "sb-2");

    store.save_lease(&lease1).unwrap();
    store.save_lease(&lease2).unwrap();

    let all = store.list_leases().unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn lease_delete_removes() {
    let store = StateStore::in_memory().unwrap();
    let lease = sample_lease("ls-del", "sb-1");

    store.save_lease(&lease).unwrap();
    store.delete_lease("ls-del").unwrap();
    let loaded = store.load_lease("ls-del").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn lease_upsert_updates_state() {
    let store = StateStore::in_memory().unwrap();
    let mut lease = sample_lease("ls-up", "sb-1");

    store.save_lease(&lease).unwrap();

    lease.state = LeaseState::Active;
    lease.last_heartbeat_at = 1_700_000_100;
    store.save_lease(&lease).unwrap();

    let loaded = store.load_lease("ls-up").unwrap().unwrap();
    assert_eq!(loaded.state, LeaseState::Active);
    assert_eq!(loaded.last_heartbeat_at, 1_700_000_100);
}

// ── Execution persistence tests ──

fn sample_execution(id: &str, container_id: &str) -> Execution {
    Execution {
        execution_id: id.to_string(),
        container_id: container_id.to_string(),
        exec_spec: ExecutionSpec {
            cmd: vec!["echo".to_string(), "hello".to_string()],
            args: vec![],
            env_override: std::collections::BTreeMap::new(),
            pty: false,
            timeout_secs: None,
        },
        state: ExecutionState::Queued,
        exit_code: None,
        started_at: None,
        ended_at: None,
    }
}

#[test]
fn execution_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let execution = sample_execution("exec-1", "ctr-abc");

    store.save_execution(&execution).unwrap();
    let loaded = store.load_execution("exec-1").unwrap().unwrap();
    assert_eq!(loaded.execution_id, "exec-1");
    assert_eq!(loaded.container_id, "ctr-abc");
    assert_eq!(loaded.state, ExecutionState::Queued);
    assert_eq!(loaded.exec_spec.cmd, vec!["echo", "hello"]);
}

#[test]
fn execution_list_returns_all() {
    let store = StateStore::in_memory().unwrap();
    store
        .save_execution(&sample_execution("exec-a", "ctr-1"))
        .unwrap();
    store
        .save_execution(&sample_execution("exec-b", "ctr-2"))
        .unwrap();

    let all = store.list_executions().unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn execution_list_for_container() {
    let store = StateStore::in_memory().unwrap();
    store
        .save_execution(&sample_execution("exec-a", "ctr-1"))
        .unwrap();
    store
        .save_execution(&sample_execution("exec-b", "ctr-1"))
        .unwrap();
    store
        .save_execution(&sample_execution("exec-c", "ctr-2"))
        .unwrap();

    let for_ctr1 = store.list_executions_for_container("ctr-1").unwrap();
    assert_eq!(for_ctr1.len(), 2);
    assert!(for_ctr1.iter().all(|e| e.container_id == "ctr-1"));

    let for_ctr2 = store.list_executions_for_container("ctr-2").unwrap();
    assert_eq!(for_ctr2.len(), 1);
}

#[test]
fn execution_delete_removes() {
    let store = StateStore::in_memory().unwrap();
    store
        .save_execution(&sample_execution("exec-del", "ctr-1"))
        .unwrap();
    store.delete_execution("exec-del").unwrap();
    let loaded = store.load_execution("exec-del").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn execution_upsert_updates_state() {
    let store = StateStore::in_memory().unwrap();
    let mut execution = sample_execution("exec-up", "ctr-1");

    store.save_execution(&execution).unwrap();

    execution.state = ExecutionState::Running;
    execution.started_at = Some(1_700_000_000);
    store.save_execution(&execution).unwrap();

    let loaded = store.load_execution("exec-up").unwrap().unwrap();
    assert_eq!(loaded.state, ExecutionState::Running);
    assert_eq!(loaded.started_at, Some(1_700_000_000));
}

#[test]
fn execution_missing_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.load_execution("nonexistent").unwrap();
    assert!(loaded.is_none());
}

// ── Checkpoint persistence tests ──

fn sample_checkpoint(id: &str, sandbox_id: &str) -> Checkpoint {
    Checkpoint {
        checkpoint_id: id.to_string(),
        sandbox_id: sandbox_id.to_string(),
        parent_checkpoint_id: None,
        class: CheckpointClass::FsQuick,
        state: CheckpointState::Creating,
        created_at: 1_700_000_000,
        compatibility_fingerprint: "fp-abc123".to_string(),
    }
}

#[test]
fn checkpoint_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let checkpoint = sample_checkpoint("ckpt-1", "sb-1");

    store.save_checkpoint(&checkpoint).unwrap();
    let loaded = store.load_checkpoint("ckpt-1").unwrap().unwrap();
    assert_eq!(loaded, checkpoint);
}

#[test]
fn checkpoint_missing_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.load_checkpoint("nonexistent").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn checkpoint_upsert_updates_state() {
    let store = StateStore::in_memory().unwrap();
    let mut checkpoint = sample_checkpoint("ckpt-up", "sb-1");

    store.save_checkpoint(&checkpoint).unwrap();

    checkpoint.state = CheckpointState::Ready;
    store.save_checkpoint(&checkpoint).unwrap();

    let loaded = store.load_checkpoint("ckpt-up").unwrap().unwrap();
    assert_eq!(loaded.state, CheckpointState::Ready);
}

#[test]
fn checkpoint_list_returns_all_ordered() {
    let store = StateStore::in_memory().unwrap();
    let ckpt1 = sample_checkpoint("ckpt-a", "sb-1");
    let mut ckpt2 = sample_checkpoint("ckpt-b", "sb-2");
    ckpt2.created_at = 1_700_000_001;

    store.save_checkpoint(&ckpt1).unwrap();
    store.save_checkpoint(&ckpt2).unwrap();

    let all = store.list_checkpoints().unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].checkpoint_id, "ckpt-a");
    assert_eq!(all[1].checkpoint_id, "ckpt-b");
}

#[test]
fn checkpoint_list_for_sandbox_filters() {
    let store = StateStore::in_memory().unwrap();
    let ckpt1 = sample_checkpoint("ckpt-1", "sb-1");
    let ckpt2 = sample_checkpoint("ckpt-2", "sb-2");
    let mut ckpt3 = sample_checkpoint("ckpt-3", "sb-1");
    ckpt3.created_at = 1_700_000_001;

    store.save_checkpoint(&ckpt1).unwrap();
    store.save_checkpoint(&ckpt2).unwrap();
    store.save_checkpoint(&ckpt3).unwrap();

    let sb1 = store.list_checkpoints_for_sandbox("sb-1").unwrap();
    assert_eq!(sb1.len(), 2);
    assert!(sb1.iter().all(|c| c.sandbox_id == "sb-1"));
}

#[test]
fn checkpoint_children_returns_direct_children() {
    let store = StateStore::in_memory().unwrap();
    let parent = sample_checkpoint("ckpt-parent", "sb-1");
    let mut child1 = sample_checkpoint("ckpt-child1", "sb-2");
    child1.parent_checkpoint_id = Some("ckpt-parent".to_string());
    let mut child2 = sample_checkpoint("ckpt-child2", "sb-3");
    child2.parent_checkpoint_id = Some("ckpt-parent".to_string());
    child2.created_at = 1_700_000_001;
    let unrelated = sample_checkpoint("ckpt-other", "sb-4");

    store.save_checkpoint(&parent).unwrap();
    store.save_checkpoint(&child1).unwrap();
    store.save_checkpoint(&child2).unwrap();
    store.save_checkpoint(&unrelated).unwrap();

    let children = store.list_checkpoint_children("ckpt-parent").unwrap();
    assert_eq!(children.len(), 2);
    assert!(
        children
            .iter()
            .all(|c| c.parent_checkpoint_id.as_deref() == Some("ckpt-parent"))
    );
}

#[test]
fn checkpoint_delete_removes() {
    let store = StateStore::in_memory().unwrap();
    let checkpoint = sample_checkpoint("ckpt-del", "sb-1");

    store.save_checkpoint(&checkpoint).unwrap();
    store.delete_checkpoint("ckpt-del").unwrap();
    let loaded = store.load_checkpoint("ckpt-del").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn checkpoint_file_entries_round_trip_replaces_and_orders_by_path() {
    let store = StateStore::in_memory().unwrap();
    let checkpoint = sample_checkpoint("ckpt-files", "sb-1");
    store.save_checkpoint(&checkpoint).unwrap();

    store
        .replace_checkpoint_file_entries(
            "ckpt-files",
            &[
                CheckpointFileEntry {
                    path: "z.txt".to_string(),
                    digest_sha256: "digest-z".to_string(),
                    size: 3,
                },
                CheckpointFileEntry {
                    path: "a.txt".to_string(),
                    digest_sha256: "digest-a".to_string(),
                    size: 1,
                },
            ],
        )
        .unwrap();

    let loaded = store.load_checkpoint_file_entries("ckpt-files").unwrap();
    let paths: Vec<_> = loaded.iter().map(|entry| entry.path.as_str()).collect();
    assert_eq!(paths, vec!["a.txt", "z.txt"]);

    store
        .replace_checkpoint_file_entries(
            "ckpt-files",
            &[CheckpointFileEntry {
                path: "m.txt".to_string(),
                digest_sha256: "digest-m".to_string(),
                size: 2,
            }],
        )
        .unwrap();
    let replaced = store.load_checkpoint_file_entries("ckpt-files").unwrap();
    assert_eq!(replaced.len(), 1);
    assert_eq!(replaced[0].path, "m.txt");
}

#[test]
fn checkpoint_delete_cascades_checkpoint_file_entries() {
    let store = StateStore::in_memory().unwrap();
    let checkpoint = sample_checkpoint("ckpt-del-files", "sb-1");
    store.save_checkpoint(&checkpoint).unwrap();
    store
        .replace_checkpoint_file_entries(
            "ckpt-del-files",
            &[CheckpointFileEntry {
                path: "artifact.bin".to_string(),
                digest_sha256: "digest-artifact".to_string(),
                size: 42,
            }],
        )
        .unwrap();

    store.delete_checkpoint("ckpt-del-files").unwrap();
    let loaded = store
        .load_checkpoint_file_entries("ckpt-del-files")
        .unwrap();
    assert!(loaded.is_empty());
}

#[test]
fn checkpoint_null_parent_round_trips() {
    let store = StateStore::in_memory().unwrap();
    let checkpoint = sample_checkpoint("ckpt-null-parent", "sb-1");
    assert!(checkpoint.parent_checkpoint_id.is_none());

    store.save_checkpoint(&checkpoint).unwrap();
    let loaded = store.load_checkpoint("ckpt-null-parent").unwrap().unwrap();
    assert!(loaded.parent_checkpoint_id.is_none());
}

#[test]
fn checkpoint_vm_full_class_persists() {
    let store = StateStore::in_memory().unwrap();
    let mut checkpoint = sample_checkpoint("ckpt-vm", "sb-1");
    checkpoint.class = CheckpointClass::VmFull;

    store.save_checkpoint(&checkpoint).unwrap();
    let loaded = store.load_checkpoint("ckpt-vm").unwrap().unwrap();
    assert_eq!(loaded.class, CheckpointClass::VmFull);
}

#[test]
fn checkpoint_retention_tag_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let checkpoint = sample_checkpoint("ckpt-tagged", "sb-1");
    store.save_checkpoint(&checkpoint).unwrap();

    store
        .save_checkpoint_retention_tag("ckpt-tagged", "pre-session")
        .unwrap();
    let loaded = store
        .load_checkpoint_retention_tag("ckpt-tagged")
        .unwrap()
        .unwrap();
    assert_eq!(loaded, "pre-session");

    store
        .delete_checkpoint_retention_tag("ckpt-tagged")
        .unwrap();
    assert!(
        store
            .load_checkpoint_retention_tag("ckpt-tagged")
            .unwrap()
            .is_none()
    );
}

#[test]
fn checkpoint_gc_respects_tags_and_is_idempotent() {
    let store = StateStore::in_memory().unwrap();

    let mut old_age = sample_checkpoint("ckpt-age", "sb-1");
    old_age.created_at = 10;
    let mut old_count = sample_checkpoint("ckpt-count", "sb-1");
    old_count.created_at = 61;
    let mut tagged = sample_checkpoint("ckpt-tagged", "sb-1");
    tagged.created_at = 20;
    let mut newest = sample_checkpoint("ckpt-keep", "sb-1");
    newest.created_at = 62;

    store.save_checkpoint(&old_age).unwrap();
    store.save_checkpoint(&old_count).unwrap();
    store.save_checkpoint(&tagged).unwrap();
    store.save_checkpoint(&newest).unwrap();
    store
        .save_checkpoint_retention_tag("ckpt-tagged", "golden")
        .unwrap();

    let policy = CheckpointRetentionPolicy {
        max_untagged_count: 1,
        max_age_secs: 40,
    };
    let state_map = store.checkpoint_retention_state_map(policy, 100).unwrap();
    assert_eq!(
        state_map.get("ckpt-age").and_then(|s| s.gc_reason),
        Some(RetentionGcReason::AgeLimit)
    );
    assert_eq!(
        state_map.get("ckpt-count").and_then(|s| s.gc_reason),
        Some(RetentionGcReason::CountLimit)
    );
    assert_eq!(state_map.get("ckpt-tagged").and_then(|s| s.gc_reason), None);
    assert_eq!(
        state_map.get("ckpt-tagged").map(|s| s.protected),
        Some(true)
    );

    let report = store
        .compact_checkpoints_with_policy_at(policy, 100)
        .unwrap();
    assert_eq!(report.deleted_by_age, vec!["ckpt-age".to_string()]);
    assert_eq!(report.deleted_by_count, vec!["ckpt-count".to_string()]);

    let remaining_ids: Vec<_> = store
        .list_checkpoints()
        .unwrap()
        .into_iter()
        .map(|checkpoint| checkpoint.checkpoint_id)
        .collect();
    assert_eq!(
        remaining_ids,
        vec!["ckpt-tagged".to_string(), "ckpt-keep".to_string()]
    );

    let second = store
        .compact_checkpoints_with_policy_at(policy, 100)
        .unwrap();
    assert!(second.is_empty());
}

// ── Receipt persistence tests (from agent-a03881b1) ──

fn sample_receipt(receipt_id: &str, entity_id: &str) -> Receipt {
    Receipt {
        receipt_id: receipt_id.to_string(),
        operation: "create_sandbox".to_string(),
        entity_id: entity_id.to_string(),
        entity_type: "sandbox".to_string(),
        request_id: "req-1".to_string(),
        status: "completed".to_string(),
        created_at: 1_700_000_000,
        metadata: serde_json::Value::Object(serde_json::Map::new()),
    }
}

#[test]
fn receipt_save_and_load() {
    let store = StateStore::in_memory().unwrap();
    let receipt = sample_receipt("rcp-1", "sbx-1");

    store.save_receipt(&receipt).unwrap();
    let loaded = store.load_receipt("rcp-1").unwrap().unwrap();
    assert_eq!(loaded.receipt_id, "rcp-1");
    assert_eq!(loaded.operation, "create_sandbox");
    assert_eq!(loaded.entity_id, "sbx-1");
    assert_eq!(loaded.entity_type, "sandbox");
    assert_eq!(loaded.request_id, "req-1");
    assert_eq!(loaded.status, "completed");
    assert_eq!(loaded.created_at, 1_700_000_000);
}

#[test]
fn receipt_load_missing_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.load_receipt("nonexistent").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn receipt_load_by_request_id() {
    let store = StateStore::in_memory().unwrap();
    let receipt = sample_receipt("rcp-2", "sbx-2");

    store.save_receipt(&receipt).unwrap();
    let loaded = store.load_receipt_by_request_id("req-1").unwrap().unwrap();
    assert_eq!(loaded.receipt_id, "rcp-2");
}

#[test]
fn receipt_load_by_request_id_missing_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.load_receipt_by_request_id("nonexistent").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn receipt_list_for_entity() {
    let store = StateStore::in_memory().unwrap();

    let r1 = Receipt {
        receipt_id: "rcp-a".to_string(),
        operation: "create_sandbox".to_string(),
        entity_id: "sbx-1".to_string(),
        entity_type: "sandbox".to_string(),
        request_id: "req-a".to_string(),
        status: "completed".to_string(),
        created_at: 1_700_000_000,
        metadata: serde_json::Value::Object(serde_json::Map::new()),
    };
    let r2 = Receipt {
        receipt_id: "rcp-b".to_string(),
        operation: "terminate_sandbox".to_string(),
        entity_id: "sbx-1".to_string(),
        entity_type: "sandbox".to_string(),
        request_id: "req-b".to_string(),
        status: "completed".to_string(),
        created_at: 1_700_000_001,
        metadata: serde_json::Value::Object(serde_json::Map::new()),
    };
    let r3 = Receipt {
        receipt_id: "rcp-c".to_string(),
        operation: "create_sandbox".to_string(),
        entity_id: "sbx-2".to_string(),
        entity_type: "sandbox".to_string(),
        request_id: "req-c".to_string(),
        status: "completed".to_string(),
        created_at: 1_700_000_002,
        metadata: serde_json::Value::Object(serde_json::Map::new()),
    };

    store.save_receipt(&r1).unwrap();
    store.save_receipt(&r2).unwrap();
    store.save_receipt(&r3).unwrap();

    let sbx1_receipts = store.list_receipts_for_entity("sandbox", "sbx-1").unwrap();
    assert_eq!(sbx1_receipts.len(), 2);
    assert_eq!(sbx1_receipts[0].receipt_id, "rcp-a");
    assert_eq!(sbx1_receipts[1].receipt_id, "rcp-b");

    let sbx2_receipts = store.list_receipts_for_entity("sandbox", "sbx-2").unwrap();
    assert_eq!(sbx2_receipts.len(), 1);
    assert_eq!(sbx2_receipts[0].receipt_id, "rcp-c");

    let empty = store.list_receipts_for_entity("lease", "ls-1").unwrap();
    assert!(empty.is_empty());
}

#[test]
fn receipt_upsert_updates() {
    let store = StateStore::in_memory().unwrap();
    let mut receipt = sample_receipt("rcp-upsert", "sbx-1");
    receipt.status = "pending".to_string();
    store.save_receipt(&receipt).unwrap();

    receipt.status = "completed".to_string();
    store.save_receipt(&receipt).unwrap();

    let loaded = store.load_receipt("rcp-upsert").unwrap().unwrap();
    assert_eq!(loaded.status, "completed");
}

#[test]
fn receipt_gc_applies_age_then_count_and_is_idempotent() {
    let store = StateStore::in_memory().unwrap();

    let mut r1 = sample_receipt("rcp-age", "sbx-1");
    r1.created_at = 10;
    let mut r2 = sample_receipt("rcp-count", "sbx-1");
    r2.created_at = 20;
    let mut r3 = sample_receipt("rcp-keep", "sbx-1");
    r3.created_at = 30;

    store.save_receipt(&r1).unwrap();
    store.save_receipt(&r2).unwrap();
    store.save_receipt(&r3).unwrap();

    let policy = ReceiptRetentionPolicy {
        max_count: 1,
        max_age_secs: 60,
    };
    let state_map = store.receipt_retention_state_map(policy, 70).unwrap();
    assert_eq!(
        state_map.get("rcp-age").and_then(|s| s.gc_reason),
        Some(RetentionGcReason::AgeLimit)
    );
    assert_eq!(
        state_map.get("rcp-count").and_then(|s| s.gc_reason),
        Some(RetentionGcReason::CountLimit)
    );
    assert_eq!(state_map.get("rcp-keep").and_then(|s| s.gc_reason), None);

    let report = store.compact_receipts_with_policy_at(policy, 70).unwrap();
    assert_eq!(report.deleted_by_age, vec!["rcp-age".to_string()]);
    assert_eq!(report.deleted_by_count, vec!["rcp-count".to_string()]);

    let remaining_ids: Vec<_> = store
        .list_receipts()
        .unwrap()
        .into_iter()
        .map(|receipt| receipt.receipt_id)
        .collect();
    assert_eq!(remaining_ids, vec!["rcp-keep".to_string()]);

    let second = store.compact_receipts_with_policy_at(policy, 70).unwrap();
    assert!(second.is_empty());
}

// ── Scoped event listing tests (from agent-a03881b1) ──

#[test]
fn events_by_scope_filters_on_type_prefix() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::SandboxCreating {
                stack_name: "myapp".to_string(),
                sandbox_id: "sb-1".to_string(),
            },
        )
        .unwrap();
    store
        .emit_event(
            "myapp",
            &StackEvent::LeaseOpened {
                sandbox_id: "sb-1".to_string(),
                lease_id: "ls-1".to_string(),
            },
        )
        .unwrap();
    store
        .emit_event(
            "myapp",
            &StackEvent::SandboxReady {
                stack_name: "myapp".to_string(),
                sandbox_id: "sb-1".to_string(),
            },
        )
        .unwrap();
    store
        .emit_event(
            "myapp",
            &StackEvent::ExecutionQueued {
                container_id: "ctr-1".to_string(),
                execution_id: "exec-1".to_string(),
            },
        )
        .unwrap();

    let sandbox_events = store
        .load_events_by_scope("myapp", "sandbox_", None, 100)
        .unwrap();
    assert_eq!(sandbox_events.len(), 2);

    let lease_events = store
        .load_events_by_scope("myapp", "lease_", None, 100)
        .unwrap();
    assert_eq!(lease_events.len(), 1);

    let exec_events = store
        .load_events_by_scope("myapp", "execution_", None, 100)
        .unwrap();
    assert_eq!(exec_events.len(), 1);
}

#[test]
fn events_by_scope_respects_cursor_and_limit() {
    let store = StateStore::in_memory().unwrap();

    for i in 0..5 {
        store
            .emit_event(
                "myapp",
                &StackEvent::SandboxCreating {
                    stack_name: "myapp".to_string(),
                    sandbox_id: format!("sb-{i}"),
                },
            )
            .unwrap();
    }

    let first_page = store
        .load_events_by_scope("myapp", "sandbox_", None, 2)
        .unwrap();
    assert_eq!(first_page.len(), 2);

    let cursor = first_page.last().map(|r| r.id);
    let second_page = store
        .load_events_by_scope("myapp", "sandbox_", cursor, 2)
        .unwrap();
    assert_eq!(second_page.len(), 2);

    // IDs should be strictly greater than the cursor
    assert!(second_page[0].id > first_page[1].id);
}

#[test]
fn events_by_scope_empty_scope_returns_nothing() {
    let store = StateStore::in_memory().unwrap();
    store
        .emit_event(
            "myapp",
            &StackEvent::SandboxCreating {
                stack_name: "myapp".to_string(),
                sandbox_id: "sb-1".to_string(),
            },
        )
        .unwrap();

    let events = store
        .load_events_by_scope("myapp", "nonexistent_", None, 100)
        .unwrap();
    assert!(events.is_empty());
}

// ── Build persistence tests (from agent-af0c4a41) ──

fn sample_build(id: &str, sandbox_id: &str) -> Build {
    Build {
        build_id: id.to_string(),
        sandbox_id: sandbox_id.to_string(),
        build_spec: BuildSpec {
            context: "/tmp/ctx".to_string(),
            dockerfile: Some("Dockerfile".to_string()),
            target: None,
            args: std::collections::BTreeMap::new(),
            cache_from: Vec::new(),
            image_tag: None,
        },
        state: BuildState::Queued,
        result_digest: None,
        started_at: 1_700_000_000,
        ended_at: None,
    }
}

#[test]
fn build_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let build = sample_build("bld-1", "sb-1");

    store.save_build(&build).unwrap();
    let loaded = store.load_build("bld-1").unwrap().unwrap();
    assert_eq!(loaded.build_id, "bld-1");
    assert_eq!(loaded.sandbox_id, "sb-1");
    assert_eq!(loaded.state, BuildState::Queued);
    assert_eq!(loaded.build_spec.context, "/tmp/ctx");
}

#[test]
fn build_list_returns_all() {
    let store = StateStore::in_memory().unwrap();
    store.save_build(&sample_build("bld-a", "sb-1")).unwrap();
    store.save_build(&sample_build("bld-b", "sb-2")).unwrap();

    let all = store.list_builds().unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn build_list_for_sandbox() {
    let store = StateStore::in_memory().unwrap();
    store.save_build(&sample_build("bld-a", "sb-1")).unwrap();
    store.save_build(&sample_build("bld-b", "sb-1")).unwrap();
    store.save_build(&sample_build("bld-c", "sb-2")).unwrap();

    let for_sb1 = store.list_builds_for_sandbox("sb-1").unwrap();
    assert_eq!(for_sb1.len(), 2);
    assert!(for_sb1.iter().all(|b| b.sandbox_id == "sb-1"));

    let for_sb2 = store.list_builds_for_sandbox("sb-2").unwrap();
    assert_eq!(for_sb2.len(), 1);
}

#[test]
fn build_delete_removes() {
    let store = StateStore::in_memory().unwrap();
    store.save_build(&sample_build("bld-del", "sb-1")).unwrap();
    store.delete_build("bld-del").unwrap();
    let loaded = store.load_build("bld-del").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn build_upsert_updates_state() {
    let store = StateStore::in_memory().unwrap();
    let mut build = sample_build("bld-up", "sb-1");

    store.save_build(&build).unwrap();

    build.state = BuildState::Running;
    store.save_build(&build).unwrap();

    let loaded = store.load_build("bld-up").unwrap().unwrap();
    assert_eq!(loaded.state, BuildState::Running);
}

#[test]
fn build_missing_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.load_build("nonexistent").unwrap();
    assert!(loaded.is_none());
}

// ── Phase 1 validation tests (from agent-a80ffa89) ──

#[test]
fn phase1_validation_health_state_persistence_round_trip() {
    let store = StateStore::in_memory().unwrap();

    let mut original_state = HashMap::new();
    original_state.insert(
        "web".to_string(),
        HealthPollState {
            service_name: "web".to_string(),
            consecutive_passes: 5,
            consecutive_failures: 0,
            last_check_millis: Some(1_700_000_000_000),
            start_time_millis: Some(1_700_000_000_123),
        },
    );
    original_state.insert(
        "db".to_string(),
        HealthPollState {
            service_name: "db".to_string(),
            consecutive_passes: 0,
            consecutive_failures: 3,
            last_check_millis: Some(1_700_000_001_000),
            start_time_millis: Some(1_700_000_000_456),
        },
    );

    // Save to store.
    store
        .save_health_poller_state("myapp", &original_state)
        .unwrap();

    // Load from a fresh perspective (same store, simulating reload).
    let loaded = store.load_health_poller_state("myapp").unwrap();

    assert_eq!(loaded.len(), 2);
    assert_eq!(
        loaded.get("web").unwrap(),
        original_state.get("web").unwrap()
    );
    assert_eq!(loaded.get("db").unwrap(), original_state.get("db").unwrap());

    // Verify counters survived the round-trip.
    let web = loaded.get("web").unwrap();
    assert_eq!(web.consecutive_passes, 5);
    assert_eq!(web.consecutive_failures, 0);

    let db = loaded.get("db").unwrap();
    assert_eq!(db.consecutive_passes, 0);
    assert_eq!(db.consecutive_failures, 3);
}

#[test]
fn phase1_validation_allocator_state_persistence_round_trip() {
    let store = StateStore::in_memory().unwrap();

    let mut ports = HashMap::new();
    ports.insert(
        "web".to_string(),
        vec![PublishedPort {
            host_port: 8080,
            container_port: 80,
            protocol: "tcp".to_string(),
        }],
    );
    ports.insert(
        "db".to_string(),
        vec![PublishedPort {
            host_port: 5432,
            container_port: 5432,
            protocol: "tcp".to_string(),
        }],
    );

    let mut service_ips = HashMap::new();
    service_ips.insert("web".to_string(), "10.0.0.2".to_string());
    service_ips.insert("db".to_string(), "10.0.0.3".to_string());

    let mut mount_tag_offsets = HashMap::new();
    mount_tag_offsets.insert("web".to_string(), 0);
    mount_tag_offsets.insert("db".to_string(), 3);

    let snapshot = AllocatorSnapshot {
        ports: ports.clone(),
        service_ips: service_ips.clone(),
        mount_tag_offsets: mount_tag_offsets.clone(),
    };

    store.save_allocator_state("myapp", &snapshot).unwrap();

    // Reload and verify all fields.
    let loaded = store.load_allocator_state("myapp").unwrap().unwrap();
    assert_eq!(loaded.ports, ports);
    assert_eq!(loaded.service_ips, service_ips);
    assert_eq!(loaded.mount_tag_offsets, mount_tag_offsets);

    // Verify specific port allocations survived.
    let web_ports = loaded.ports.get("web").unwrap();
    assert_eq!(web_ports.len(), 1);
    assert_eq!(web_ports[0].host_port, 8080);
    assert_eq!(web_ports[0].container_port, 80);

    // Verify IPs survived.
    assert_eq!(loaded.service_ips.get("web"), Some(&"10.0.0.2".to_string()));
    assert_eq!(loaded.service_ips.get("db"), Some(&"10.0.0.3".to_string()));

    // Verify mount tag offsets survived.
    assert_eq!(loaded.mount_tag_offsets.get("web"), Some(&0));
    assert_eq!(loaded.mount_tag_offsets.get("db"), Some(&3));
}

#[test]
fn phase1_validation_reconcile_session_lifecycle() {
    let store = StateStore::in_memory().unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
    ];

    let session = ReconcileSession {
        session_id: "rs-1000".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-1".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: "hash-abc".to_string(),
        next_action_index: 0,
        total_actions: 2,
        started_at: now,
        updated_at: now,
        completed_at: None,
    };

    // Create session.
    store.create_reconcile_session(&session, &actions).unwrap();

    // Load active session.
    let loaded = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(loaded.session_id, "rs-1000");
    assert_eq!(loaded.status, ReconcileSessionStatus::Active);
    assert_eq!(loaded.next_action_index, 0);

    // Update progress.
    store
        .update_reconcile_session_progress("rs-1000", 1, &ReconcileSessionStatus::Active)
        .unwrap();

    let updated = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(updated.next_action_index, 1);
    assert_eq!(updated.status, ReconcileSessionStatus::Active);

    // Complete session.
    store.complete_reconcile_session("rs-1000").unwrap();

    // Active session should now be gone.
    let none = store.load_active_reconcile_session("myapp").unwrap();
    assert!(none.is_none());

    // List sessions should show completed.
    let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Completed);
    assert!(sessions[0].completed_at.is_some());
}

#[test]
fn phase1_validation_reconcile_session_supersession() {
    let store = StateStore::in_memory().unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    // Create first session.
    let session1 = ReconcileSession {
        session_id: "rs-first".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-1".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: "hash-1".to_string(),
        next_action_index: 0,
        total_actions: 1,
        started_at: now,
        updated_at: now,
        completed_at: None,
    };
    store.create_reconcile_session(&session1, &actions).unwrap();

    // Supersede active sessions for the stack.
    let superseded_count = store.supersede_active_sessions("myapp").unwrap();
    assert_eq!(superseded_count, 1);

    // Old session should be superseded.
    let old_active = store.load_active_reconcile_session("myapp").unwrap();
    assert!(old_active.is_none());

    let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Superseded);

    // Create new session for same stack.
    let session2 = ReconcileSession {
        session_id: "rs-second".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-2".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: "hash-2".to_string(),
        next_action_index: 0,
        total_actions: 1,
        started_at: now + 1,
        updated_at: now + 1,
        completed_at: None,
    };
    store.create_reconcile_session(&session2, &actions).unwrap();

    // New session is active.
    let active = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(active.session_id, "rs-second");
}

#[test]
fn phase1_validation_event_cursor_coherence_after_simulated_restart() {
    let store = StateStore::in_memory().unwrap();

    // Emit a batch of events (simulating pre-restart state).
    let events_batch1 = vec![
        StackEvent::StackApplyStarted {
            stack_name: "myapp".to_string(),
            services_count: 2,
        },
        StackEvent::ServiceCreating {
            stack_name: "myapp".to_string(),
            service_name: "web".to_string(),
        },
        StackEvent::ServiceReady {
            stack_name: "myapp".to_string(),
            service_name: "web".to_string(),
            runtime_id: "ctr-1".to_string(),
        },
    ];

    for event in &events_batch1 {
        store.emit_event("myapp", event).unwrap();
    }

    // Record the cursor (simulating what a consumer would save before restart).
    let all_records = store.load_event_records("myapp").unwrap();
    assert_eq!(all_records.len(), 3);
    let cursor = all_records[1].id; // After ServiceCreating

    // Emit more events (simulating post-restart activity).
    let events_batch2 = vec![
        StackEvent::ServiceCreating {
            stack_name: "myapp".to_string(),
            service_name: "db".to_string(),
        },
        StackEvent::ServiceReady {
            stack_name: "myapp".to_string(),
            service_name: "db".to_string(),
            runtime_id: "ctr-2".to_string(),
        },
        StackEvent::StackApplyCompleted {
            stack_name: "myapp".to_string(),
            succeeded: 2,
            failed: 0,
        },
    ];

    for event in &events_batch2 {
        store.emit_event("myapp", event).unwrap();
    }

    // Load events since cursor (simulating restart recovery).
    let since_cursor = store.load_events_since("myapp", cursor).unwrap();

    // Should get: ServiceReady(web), ServiceCreating(db), ServiceReady(db), StackApplyCompleted
    assert_eq!(since_cursor.len(), 4);

    // Verify ordering: IDs must be strictly monotonically increasing.
    for window in since_cursor.windows(2) {
        assert!(
            window[1].id > window[0].id,
            "event IDs must be monotonically increasing"
        );
    }

    // All events since cursor must have id > cursor.
    for record in &since_cursor {
        assert!(record.id > cursor);
    }

    // Verify completeness: total events = batch1 + batch2.
    let total = store.load_event_records("myapp").unwrap();
    assert_eq!(total.len(), 6);

    // Verify cursor-based loading gives exact complement.
    let from_start = store.load_events_since("myapp", 0).unwrap();
    assert_eq!(from_start.len(), 6);
}

// ── Phase 2: Schema/version migration tests (from agent-a80ffa89) ──

#[test]
fn phase2_control_metadata_crud() {
    let store = StateStore::in_memory().unwrap();

    // Read non-existent key.
    assert!(store.get_control_metadata("nonexistent").unwrap().is_none());

    // Set and read.
    store
        .set_control_metadata("test_key", "test_value")
        .unwrap();
    let value = store.get_control_metadata("test_key").unwrap().unwrap();
    assert_eq!(value, "test_value");

    // Update (upsert).
    store
        .set_control_metadata("test_key", "updated_value")
        .unwrap();
    let value = store.get_control_metadata("test_key").unwrap().unwrap();
    assert_eq!(value, "updated_value");
}

#[test]
fn phase2_schema_version_defaults_to_1() {
    let store = StateStore::in_memory().unwrap();
    let version = store.schema_version().unwrap();
    assert_eq!(version, 1);
}

#[test]
fn phase2_schema_version_set_and_get() {
    let store = StateStore::in_memory().unwrap();

    store.set_schema_version(2).unwrap();
    assert_eq!(store.schema_version().unwrap(), 2);

    store.set_schema_version(42).unwrap();
    assert_eq!(store.schema_version().unwrap(), 42);
}

#[test]
fn phase2_created_at_metadata_set_on_init() {
    let store = StateStore::in_memory().unwrap();
    let created_at = store.get_control_metadata("created_at").unwrap();
    assert!(created_at.is_some());
    // Should be a parseable integer.
    let secs: u64 = created_at.unwrap().parse().unwrap();
    assert!(secs > 0);
}

#[test]
fn phase2_multiple_metadata_keys_independent() {
    let store = StateStore::in_memory().unwrap();

    store.set_control_metadata("key_a", "value_a").unwrap();
    store.set_control_metadata("key_b", "value_b").unwrap();

    assert_eq!(
        store.get_control_metadata("key_a").unwrap().unwrap(),
        "value_a"
    );
    assert_eq!(
        store.get_control_metadata("key_b").unwrap().unwrap(),
        "value_b"
    );

    // Updating one doesn't affect the other.
    store.set_control_metadata("key_a", "new_a").unwrap();
    assert_eq!(
        store.get_control_metadata("key_a").unwrap().unwrap(),
        "new_a"
    );
    assert_eq!(
        store.get_control_metadata("key_b").unwrap().unwrap(),
        "value_b"
    );
}

// ── Phase 3: Startup drift verification tests (from agent-a80ffa89) ──

#[test]
fn phase3_drift_desired_without_observed() {
    let store = StateStore::in_memory().unwrap();

    // Save desired state but no observed state.
    store.save_desired_state("myapp", &sample_spec()).unwrap();

    let findings = store.verify_startup_drift("myapp").unwrap();
    assert!(
        findings.iter().any(
            |f| f.category == "desired_state" && f.description.contains("without observations")
        ),
        "expected desired_state drift finding, got: {findings:?}"
    );
}

#[test]
fn phase3_drift_orphaned_observed_state() {
    let store = StateStore::in_memory().unwrap();

    // Save desired state with only "web" service.
    let mut spec = sample_spec();
    spec.services.retain(|s| s.name == "web");
    store.save_desired_state("myapp", &spec).unwrap();

    // Save observed state for "web" (expected) and "cache" (orphaned).
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
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "cache".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-2".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let findings = store.verify_startup_drift("myapp").unwrap();
    let orphaned: Vec<_> = findings
        .iter()
        .filter(|f| f.category == "observed_state" && f.description.contains("cache"))
        .collect();
    assert_eq!(orphaned.len(), 1);
    assert!(matches!(orphaned[0].severity, DriftSeverity::Warning));
}

#[test]
fn phase3_drift_stale_reconcile_session() {
    let store = StateStore::in_memory().unwrap();

    // Create an active session with updated_at far in the past (> 5 min ago).
    let old_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 600; // 10 minutes ago

    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    let session = ReconcileSession {
        session_id: "rs-stale".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-stale".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: "hash-stale".to_string(),
        next_action_index: 0,
        total_actions: 1,
        started_at: old_time,
        updated_at: old_time,
        completed_at: None,
    };
    store.create_reconcile_session(&session, &actions).unwrap();

    let findings = store.verify_startup_drift("myapp").unwrap();
    let stale: Vec<_> = findings
        .iter()
        .filter(|f| f.category == "reconcile" && f.description.contains("stale"))
        .collect();
    assert_eq!(stale.len(), 1);
    assert!(matches!(stale[0].severity, DriftSeverity::Warning));
}

#[test]
fn phase3_drift_orphaned_health_state() {
    let store = StateStore::in_memory().unwrap();

    // Save health state but no desired state.
    let mut health = HashMap::new();
    health.insert(
        "web".to_string(),
        HealthPollState {
            service_name: "web".to_string(),
            consecutive_passes: 1,
            consecutive_failures: 0,
            last_check_millis: Some(1_700_000_000_000),
            start_time_millis: None,
        },
    );
    store.save_health_poller_state("myapp", &health).unwrap();

    let findings = store.verify_startup_drift("myapp").unwrap();
    let orphaned: Vec<_> = findings
        .iter()
        .filter(|f| f.category == "health" && f.description.contains("orphaned"))
        .collect();
    assert_eq!(orphaned.len(), 1);
    assert!(matches!(orphaned[0].severity, DriftSeverity::Info));
}

#[test]
fn phase3_drift_clean_state_returns_no_findings() {
    let store = StateStore::in_memory().unwrap();

    // Save desired state with matching observed state.
    store.save_desired_state("myapp", &sample_spec()).unwrap();
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
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                service_name: "db".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-2".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let findings = store.verify_startup_drift("myapp").unwrap();
    assert!(
        findings.is_empty(),
        "expected no drift findings in clean state, got: {findings:?}"
    );
}

#[test]
fn phase3_drift_nonexistent_stack_returns_no_findings() {
    let store = StateStore::in_memory().unwrap();
    let findings = store.verify_startup_drift("nonexistent").unwrap();
    assert!(findings.is_empty());
}

#[test]
fn phase3_drift_finding_serialization_round_trip() {
    let finding = DriftFinding {
        category: "observed_state".to_string(),
        description: "orphaned service".to_string(),
        severity: DriftSeverity::Warning,
    };

    let json = serde_json::to_string(&finding).unwrap();
    let loaded: DriftFinding = serde_json::from_str(&json).unwrap();
    assert_eq!(loaded.category, "observed_state");
    assert_eq!(loaded.description, "orphaned service");
    assert!(matches!(loaded.severity, DriftSeverity::Warning));
}

#[test]
fn phase3_drift_event_emission() {
    let store = StateStore::in_memory().unwrap();

    // Create a drift finding and emit as event.
    let finding = DriftFinding {
        category: "desired_state".to_string(),
        description: "desired state without observations".to_string(),
        severity: DriftSeverity::Warning,
    };

    let event = StackEvent::DriftDetected {
        stack_name: "myapp".to_string(),
        category: finding.category.clone(),
        description: finding.description.clone(),
        severity: finding.severity.as_str().to_string(),
    };

    store.emit_event("myapp", &event).unwrap();

    let events = store.load_events("myapp").unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], StackEvent::DriftDetected { .. }));
}

// ── Part 1: Audit log CRUD tests (vz-v2n.3.1) ──

fn make_audit_entry(
    session_id: &str,
    stack_name: &str,
    action_index: usize,
    action_kind: &str,
    service_name: &str,
) -> ReconcileAuditEntry {
    ReconcileAuditEntry {
        id: 0, // auto-generated on insert
        session_id: session_id.to_string(),
        stack_name: stack_name.to_string(),
        action_index,
        action_kind: action_kind.to_string(),
        service_name: service_name.to_string(),
        action_hash: format!("hash-{action_index}"),
        status: "started".to_string(),
        started_at: 1_700_000_000 + action_index as u64,
        completed_at: None,
        error_message: None,
    }
}

#[test]
fn audit_log_start_and_load() {
    let store = StateStore::in_memory().unwrap();

    let entry = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
    let id = store.log_reconcile_action_start(&entry).unwrap();
    assert!(id > 0);

    let log = store.load_audit_log_for_session("sess-1").unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].session_id, "sess-1");
    assert_eq!(log[0].action_kind, "service_create");
    assert_eq!(log[0].service_name, "web");
    assert_eq!(log[0].status, "started");
    assert!(log[0].completed_at.is_none());
    assert!(log[0].error_message.is_none());
}

#[test]
fn audit_log_complete_success() {
    let store = StateStore::in_memory().unwrap();

    let entry = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
    let id = store.log_reconcile_action_start(&entry).unwrap();
    store.log_reconcile_action_complete(id, None).unwrap();

    let log = store.load_audit_log_for_session("sess-1").unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].status, "completed");
    assert!(log[0].completed_at.is_some());
    assert!(log[0].error_message.is_none());
}

#[test]
fn audit_log_complete_failure() {
    let store = StateStore::in_memory().unwrap();

    let entry = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
    let id = store.log_reconcile_action_start(&entry).unwrap();
    store
        .log_reconcile_action_complete(id, Some("container start failed"))
        .unwrap();

    let log = store.load_audit_log_for_session("sess-1").unwrap();
    assert_eq!(log[0].status, "failed");
    assert!(log[0].completed_at.is_some());
    assert_eq!(
        log[0].error_message.as_deref(),
        Some("container start failed")
    );
}

#[test]
fn audit_log_multiple_entries_ordered_by_action_index() {
    let store = StateStore::in_memory().unwrap();

    // Insert out of order to verify ORDER BY
    let e2 = make_audit_entry("sess-1", "myapp", 2, "service_remove", "cache");
    let e0 = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
    let e1 = make_audit_entry("sess-1", "myapp", 1, "service_create", "db");

    store.log_reconcile_action_start(&e2).unwrap();
    store.log_reconcile_action_start(&e0).unwrap();
    store.log_reconcile_action_start(&e1).unwrap();

    let log = store.load_audit_log_for_session("sess-1").unwrap();
    assert_eq!(log.len(), 3);
    assert_eq!(log[0].action_index, 0);
    assert_eq!(log[1].action_index, 1);
    assert_eq!(log[2].action_index, 2);
}

#[test]
fn audit_log_scoped_by_session() {
    let store = StateStore::in_memory().unwrap();

    let e1 = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
    let e2 = make_audit_entry("sess-2", "myapp", 0, "service_create", "api");

    store.log_reconcile_action_start(&e1).unwrap();
    store.log_reconcile_action_start(&e2).unwrap();

    let log1 = store.load_audit_log_for_session("sess-1").unwrap();
    assert_eq!(log1.len(), 1);
    assert_eq!(log1[0].service_name, "web");

    let log2 = store.load_audit_log_for_session("sess-2").unwrap();
    assert_eq!(log2.len(), 1);
    assert_eq!(log2[0].service_name, "api");
}

#[test]
fn audit_log_recent_by_stack() {
    let store = StateStore::in_memory().unwrap();

    for i in 0..5 {
        let entry = make_audit_entry(
            &format!("sess-{i}"),
            "myapp",
            0,
            "service_create",
            &format!("svc-{i}"),
        );
        store.log_reconcile_action_start(&entry).unwrap();
    }

    // Other stack should not appear
    let other = make_audit_entry("sess-other", "otherapp", 0, "service_create", "web");
    store.log_reconcile_action_start(&other).unwrap();

    let recent = store.load_recent_audit_log("myapp", 3).unwrap();
    assert_eq!(recent.len(), 3);
    // Newest first (DESC)
    assert!(recent[0].id > recent[1].id);
    assert!(recent[1].id > recent[2].id);
}

#[test]
fn audit_log_empty_session_returns_empty() {
    let store = StateStore::in_memory().unwrap();
    let log = store.load_audit_log_for_session("nonexistent").unwrap();
    assert!(log.is_empty());
}

// ── Part 2: Recovery fault-injection tests (vz-v2n.3.2) ──

#[test]
fn recovery_crash_during_apply_actions_partially_persisted() {
    let store = StateStore::in_memory().unwrap();

    // Create session with 3 actions
    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "cache".to_string(),
        },
    ];
    let session = ReconcileSession {
        session_id: "rs-crash-1".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-1".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 3,
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store.create_reconcile_session(&session, &actions).unwrap();

    // Action 0: started + completed
    let e0 = make_audit_entry("rs-crash-1", "myapp", 0, "service_create", "web");
    let id0 = store.log_reconcile_action_start(&e0).unwrap();
    store.log_reconcile_action_complete(id0, None).unwrap();

    // Action 1: started + completed
    let e1 = make_audit_entry("rs-crash-1", "myapp", 1, "service_create", "db");
    let id1 = store.log_reconcile_action_start(&e1).unwrap();
    store.log_reconcile_action_complete(id1, None).unwrap();

    // Action 2: started but NOT completed (crash simulation)
    let e2 = make_audit_entry("rs-crash-1", "myapp", 2, "service_create", "cache");
    store.log_reconcile_action_start(&e2).unwrap();

    // Update progress to reflect that we were partway through
    store
        .update_reconcile_session_progress("rs-crash-1", 2, &ReconcileSessionStatus::Active)
        .unwrap();

    // Verify: session is still active (crash recovery)
    let active = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(active.session_id, "rs-crash-1");
    assert_eq!(active.status, ReconcileSessionStatus::Active);

    // Verify: audit log shows 2 completed, 1 started
    let log = store.load_audit_log_for_session("rs-crash-1").unwrap();
    assert_eq!(log.len(), 3);
    assert_eq!(log[0].status, "completed");
    assert_eq!(log[1].status, "completed");
    assert_eq!(log[2].status, "started"); // crash point

    // Verify: next_action_index points to the right place
    assert_eq!(active.next_action_index, 2);
}

#[test]
fn recovery_restart_with_partial_batch_resumes_from_cursor() {
    let store = StateStore::in_memory().unwrap();

    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "cache".to_string(),
        },
    ];
    let session = ReconcileSession {
        session_id: "rs-resume-1".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-2".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 3,
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store.create_reconcile_session(&session, &actions).unwrap();

    // Complete action 0, advance cursor
    let e0 = make_audit_entry("rs-resume-1", "myapp", 0, "service_create", "web");
    let id0 = store.log_reconcile_action_start(&e0).unwrap();
    store.log_reconcile_action_complete(id0, None).unwrap();
    store
        .update_reconcile_session_progress("rs-resume-1", 1, &ReconcileSessionStatus::Active)
        .unwrap();

    // Simulate restart: load active session
    let resumed = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(resumed.next_action_index, 1);
    assert_eq!(resumed.total_actions, 3);

    // Verify remaining actions via audit log
    let log = store.load_audit_log_for_session("rs-resume-1").unwrap();
    let completed_count = log.iter().filter(|e| e.status == "completed").count();
    assert_eq!(completed_count, 1);
    // Remaining = total - cursor
    let remaining = resumed.total_actions - resumed.next_action_index;
    assert_eq!(remaining, 2);
}

#[test]
fn recovery_crash_during_health_polling_state_preserved() {
    let store = StateStore::in_memory().unwrap();

    let mut health_state = HashMap::new();
    health_state.insert(
        "web".to_string(),
        HealthPollState {
            service_name: "web".to_string(),
            consecutive_passes: 3,
            consecutive_failures: 0,
            last_check_millis: Some(1_700_000_000_000),
            start_time_millis: Some(1_700_000_000_100),
        },
    );
    health_state.insert(
        "db".to_string(),
        HealthPollState {
            service_name: "db".to_string(),
            consecutive_passes: 1,
            consecutive_failures: 2,
            last_check_millis: Some(1_700_000_000_500),
            start_time_millis: Some(1_700_000_000_200),
        },
    );
    store
        .save_health_poller_state("myapp", &health_state)
        .unwrap();

    // Simulate crash: just reload from store (in-memory is still there)
    let restored = store.load_health_poller_state("myapp").unwrap();
    assert_eq!(restored.len(), 2);
    let web = restored.get("web").unwrap();
    assert_eq!(web.consecutive_passes, 3);
    assert_eq!(web.consecutive_failures, 0);
    let db = restored.get("db").unwrap();
    assert_eq!(db.consecutive_passes, 1);
    assert_eq!(db.consecutive_failures, 2);
}

#[test]
fn recovery_port_conflict_replay_after_restart() {
    let store = StateStore::in_memory().unwrap();

    let mut ports = HashMap::new();
    ports.insert(
        "web".to_string(),
        vec![PublishedPort {
            host_port: 8080,
            container_port: 80,
            protocol: "tcp".to_string(),
        }],
    );
    ports.insert(
        "api".to_string(),
        vec![PublishedPort {
            host_port: 3000,
            container_port: 3000,
            protocol: "tcp".to_string(),
        }],
    );
    let snapshot = AllocatorSnapshot {
        ports: ports.clone(),
        service_ips: HashMap::from([
            ("web".to_string(), "10.0.0.2".to_string()),
            ("api".to_string(), "10.0.0.3".to_string()),
        ]),
        mount_tag_offsets: HashMap::from([("web".to_string(), 0), ("api".to_string(), 1)]),
    };
    store.save_allocator_state("myapp", &snapshot).unwrap();

    // Simulate restart: reload
    let restored = store.load_allocator_state("myapp").unwrap().unwrap();
    assert_eq!(restored.ports, snapshot.ports);
    assert_eq!(restored.service_ips, snapshot.service_ips);
    assert_eq!(restored.mount_tag_offsets, snapshot.mount_tag_offsets);
}

#[test]
fn recovery_dependency_blocked_replay_after_restart() {
    let store = StateStore::in_memory().unwrap();

    let spec = StackSpec {
        name: "myapp".to_string(),
        services: vec![
            ServiceSpec {
                name: "web".to_string(),
                kind: ServiceKind::Service,
                image: "nginx:latest".to_string(),
                depends_on: vec![crate::spec::ServiceDependency {
                    service: "db".to_string(),
                    condition: crate::spec::DependencyCondition::ServiceHealthy,
                }],
                command: None,
                entrypoint: None,
                environment: HashMap::new(),
                working_dir: None,
                user: None,
                mounts: vec![],
                ports: vec![],
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
            },
            ServiceSpec {
                name: "db".to_string(),
                kind: ServiceKind::Service,
                image: "postgres:16".to_string(),
                depends_on: vec![],
                command: None,
                entrypoint: None,
                environment: HashMap::new(),
                working_dir: None,
                user: None,
                mounts: vec![],
                ports: vec![],
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
            },
        ],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    store.save_desired_state("myapp", &spec).unwrap();

    // Simulate restart: reload desired state and verify dependencies
    let restored = store.load_desired_state("myapp").unwrap().unwrap();
    assert_eq!(restored.services.len(), 2);
    let web = restored.services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(web.depends_on.len(), 1);
    assert_eq!(web.depends_on[0].service, "db");
    assert_eq!(
        web.depends_on[0].condition,
        crate::spec::DependencyCondition::ServiceHealthy
    );
}

#[test]
fn recovery_superseded_session_cleanup() {
    let store = StateStore::in_memory().unwrap();

    // First session
    let actions1 = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];
    let session1 = ReconcileSession {
        session_id: "rs-old-1".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-old".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::compute_actions_hash(&actions1),
        next_action_index: 0,
        total_actions: 1,
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store
        .create_reconcile_session(&session1, &actions1)
        .unwrap();

    // Audit entries for old session
    let e_old = make_audit_entry("rs-old-1", "myapp", 0, "service_create", "web");
    store.log_reconcile_action_start(&e_old).unwrap();

    // Supersede the old session
    let superseded_count = store.supersede_active_sessions("myapp").unwrap();
    assert_eq!(superseded_count, 1);

    // Create new session
    let actions2 = vec![Action::ServiceRecreate {
        service_name: "web".to_string(),
    }];
    let session2 = ReconcileSession {
        session_id: "rs-new-1".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-new".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::compute_actions_hash(&actions2),
        next_action_index: 0,
        total_actions: 1,
        started_at: 1_700_001_000,
        updated_at: 1_700_001_000,
        completed_at: None,
    };
    store
        .create_reconcile_session(&session2, &actions2)
        .unwrap();

    // Verify old audit entries are still queryable
    let old_log = store.load_audit_log_for_session("rs-old-1").unwrap();
    assert_eq!(old_log.len(), 1);
    assert_eq!(old_log[0].service_name, "web");

    // Verify only new session is active
    let active = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(active.session_id, "rs-new-1");

    // Verify old session is superseded
    let all_sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(all_sessions.len(), 2);
    let old_sess = all_sessions
        .iter()
        .find(|s| s.session_id == "rs-old-1")
        .unwrap();
    assert_eq!(old_sess.status, ReconcileSessionStatus::Superseded);
}

// ── Part 3: Phase 3 recovery proof validation (vz-v2n.3.3) ──

#[test]
fn phase3_validation_full_recovery_lifecycle() {
    let store = StateStore::in_memory().unwrap();

    // 1. Create stack with desired state
    let spec = sample_spec();
    store.save_desired_state("myapp", &spec).unwrap();

    // 2. Create reconcile session with 3 actions
    let actions = vec![
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceRemove {
            service_name: "old-svc".to_string(),
        },
    ];
    let session = ReconcileSession {
        session_id: "rs-full-1".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-full".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 3,
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store.create_reconcile_session(&session, &actions).unwrap();

    // 3. Log action starts and completions with audit entries
    for (idx, action) in actions.iter().enumerate() {
        let kind = match action {
            Action::ServiceCreate { .. } => "service_create",
            Action::ServiceRecreate { .. } => "service_recreate",
            Action::ServiceRemove { .. } => "service_remove",
        };
        let entry = make_audit_entry("rs-full-1", "myapp", idx, kind, action.service_name());
        let id = store.log_reconcile_action_start(&entry).unwrap();
        store.log_reconcile_action_complete(id, None).unwrap();
        store
            .update_reconcile_session_progress(
                "rs-full-1",
                idx + 1,
                &ReconcileSessionStatus::Active,
            )
            .unwrap();
    }

    // 4. Mark session completed
    store.complete_reconcile_session("rs-full-1").unwrap();

    // 5. Verify: audit log is complete and ordered
    let log = store.load_audit_log_for_session("rs-full-1").unwrap();
    assert_eq!(log.len(), 3);
    for (idx, entry) in log.iter().enumerate() {
        assert_eq!(entry.action_index, idx);
        assert_eq!(entry.status, "completed");
        assert!(entry.completed_at.is_some());
    }
    assert_eq!(log[0].action_kind, "service_create");
    assert_eq!(log[0].service_name, "web");
    assert_eq!(log[1].action_kind, "service_create");
    assert_eq!(log[1].service_name, "db");
    assert_eq!(log[2].action_kind, "service_remove");
    assert_eq!(log[2].service_name, "old-svc");

    // 6. Verify: session has correct completed_at
    let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    let completed_sess = sessions
        .iter()
        .find(|s| s.session_id == "rs-full-1")
        .unwrap();
    assert_eq!(completed_sess.status, ReconcileSessionStatus::Completed);
    assert!(completed_sess.completed_at.is_some());

    // 7. Create second session (simulating next apply)
    store.supersede_active_sessions("myapp").unwrap(); // no-op: already completed
    let actions2 = vec![Action::ServiceRecreate {
        service_name: "web".to_string(),
    }];
    let session2 = ReconcileSession {
        session_id: "rs-full-2".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-full-2".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::compute_actions_hash(&actions2),
        next_action_index: 0,
        total_actions: 1,
        started_at: 1_700_001_000,
        updated_at: 1_700_001_000,
        completed_at: None,
    };
    store
        .create_reconcile_session(&session2, &actions2)
        .unwrap();

    // 8. Verify: old session is completed (not superseded since it was already done),
    //    new session is active
    let all = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(all.len(), 2);
    let old = all.iter().find(|s| s.session_id == "rs-full-1").unwrap();
    assert_eq!(old.status, ReconcileSessionStatus::Completed);
    let new = all.iter().find(|s| s.session_id == "rs-full-2").unwrap();
    assert_eq!(new.status, ReconcileSessionStatus::Active);

    // 9. Verify: drift check returns clean for correct state
    //    Save observed state matching desired state
    for svc in &spec.services {
        store
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    service_name: svc.name.clone(),
                    phase: ServicePhase::Running,
                    container_id: Some(format!("ctr-{}", svc.name)),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();
    }
    let findings = store.verify_startup_drift("myapp").unwrap();
    // The only finding should be about the active session (if stale).
    // Since the new session was just created, no stale session warning.
    // Both desired services have observed state, so no orphan warnings.
    let non_stale: Vec<_> = findings
        .iter()
        .filter(|f| f.category != "reconcile")
        .collect();
    assert!(
        non_stale.is_empty(),
        "unexpected drift findings: {non_stale:?}"
    );
}

// ── Part 4: Phase 2 schema/drift validation (vz-v2n.2.3) ──

#[test]
fn phase2_validation_schema_version_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test-state.db");

    {
        let store = StateStore::open(&db_path).unwrap();
        store.set_schema_version(2).unwrap();
        assert_eq!(store.schema_version().unwrap(), 2);
    }
    // Drop store (close connection), reopen
    {
        let store = StateStore::open(&db_path).unwrap();
        assert_eq!(store.schema_version().unwrap(), 2);
    }
}

#[test]
fn phase2_validation_drift_desired_without_observed() {
    let store = StateStore::in_memory().unwrap();

    // Save desired state, don't save observed state
    let spec = sample_spec();
    store.save_desired_state("myapp", &spec).unwrap();

    let findings = store.verify_startup_drift("myapp").unwrap();
    let desired_drift: Vec<_> = findings
        .iter()
        .filter(|f| f.category == "desired_state")
        .collect();
    assert_eq!(desired_drift.len(), 1);
    assert!(
        desired_drift[0]
            .description
            .contains("desired state without observations")
    );
    assert_eq!(desired_drift[0].severity, DriftSeverity::Warning);
}

#[test]
fn phase2_validation_drift_orphaned_observed() {
    let store = StateStore::in_memory().unwrap();

    // Save desired state (only "web" and "db")
    let spec = sample_spec();
    store.save_desired_state("myapp", &spec).unwrap();

    // Save observed state for services including one not in desired state
    for name in &["web", "db", "orphaned-svc"] {
        store
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    service_name: name.to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some(format!("ctr-{name}")),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();
    }

    let findings = store.verify_startup_drift("myapp").unwrap();
    let orphaned: Vec<_> = findings
        .iter()
        .filter(|f| f.category == "observed_state")
        .collect();
    assert_eq!(orphaned.len(), 1);
    assert!(
        orphaned[0]
            .description
            .contains("orphaned observed state for service 'orphaned-svc'")
    );
    assert_eq!(orphaned[0].severity, DriftSeverity::Warning);
}

#[test]
fn phase2_validation_event_queries_after_migration() {
    let store = StateStore::in_memory().unwrap();

    // Emit events
    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 2,
            },
        )
        .unwrap();
    store
        .emit_event(
            "myapp",
            &StackEvent::ServiceCreating {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
        )
        .unwrap();

    // Verify load_events works
    let events = store.load_events("myapp").unwrap();
    assert_eq!(events.len(), 2);

    // Verify load_events_since works
    let records = store.load_event_records("myapp").unwrap();
    let since = store.load_events_since("myapp", records[0].id).unwrap();
    assert_eq!(since.len(), 1);
    assert!(matches!(since[0].event, StackEvent::ServiceCreating { .. }));

    // Set schema version and verify queries still work
    store.set_schema_version(3).unwrap();
    assert_eq!(store.schema_version().unwrap(), 3);

    let events_after = store.load_events("myapp").unwrap();
    assert_eq!(events_after.len(), 2);

    let since_after = store.load_events_since("myapp", records[0].id).unwrap();
    assert_eq!(since_after.len(), 1);
}

// ── Capacity and regression tests (vz-lbg) ─────────────────────

fn make_service(name: &str) -> ServiceSpec {
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
        expose: vec![],
        stdin_open: false,
        tty: false,
        logging: None,
    }
}

/// Insert 10,000 events into a single stack and verify that cursor-based
/// queries remain performant (complete within a generous wall-clock bound).
#[test]
fn capacity_10k_events_query_performance() {
    let store = StateStore::in_memory().unwrap();

    // Insert 10,000 events.
    let start_insert = std::time::Instant::now();
    for i in 0..10_000 {
        store
            .emit_event(
                "perf-app",
                &StackEvent::ServiceCreating {
                    stack_name: "perf-app".to_string(),
                    service_name: format!("svc-{i}"),
                },
            )
            .unwrap();
    }
    let insert_elapsed = start_insert.elapsed();
    // Generous bound: 10,000 inserts should complete within 10 seconds on CI.
    assert!(
        insert_elapsed.as_secs() < 10,
        "10,000 event inserts took {insert_elapsed:?} (>10s budget)"
    );

    // Count should be exact.
    assert_eq!(store.event_count("perf-app").unwrap(), 10_000);

    // Cursor-based query from midpoint should be fast.
    let start_query = std::time::Instant::now();
    let page = store
        .load_events_since_limited("perf-app", 5000, 100)
        .unwrap();
    let query_elapsed = start_query.elapsed();
    assert_eq!(page.len(), 100);
    // Query should complete in well under 1 second.
    assert!(
        query_elapsed.as_millis() < 1000,
        "cursor query after 10k events took {query_elapsed:?} (>1s budget)"
    );

    // Full-table scan should also be bounded.
    let start_all = std::time::Instant::now();
    let _all_records = store.load_event_records("perf-app").unwrap();
    let all_elapsed = start_all.elapsed();
    assert!(
        all_elapsed.as_secs() < 5,
        "full load of 10k event records took {all_elapsed:?} (>5s budget)"
    );
}

/// Verify that 100 concurrent stacks maintain isolation and perform
/// adequately for save/load operations.
#[test]
fn capacity_100_concurrent_stacks_isolation() {
    let store = StateStore::in_memory().unwrap();

    let start = std::time::Instant::now();

    // Create 100 stacks, each with a unique spec.
    for i in 0..100 {
        let name = format!("stack-{i}");
        let spec = StackSpec {
            name: name.clone(),
            services: vec![make_service(&format!("svc-{i}"))],
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
            disk_size_mb: None,
        };
        store.save_desired_state(&name, &spec).unwrap();

        // Emit a couple events per stack.
        store
            .emit_event(
                &name,
                &StackEvent::StackApplyStarted {
                    stack_name: name.clone(),
                    services_count: 1,
                },
            )
            .unwrap();
        store
            .emit_event(
                &name,
                &StackEvent::StackApplyCompleted {
                    stack_name: name.clone(),
                    succeeded: 1,
                    failed: 0,
                },
            )
            .unwrap();

        // Save observed state.
        store
            .save_observed_state(
                &name,
                &ServiceObservedState {
                    service_name: format!("svc-{i}"),
                    phase: ServicePhase::Running,
                    container_id: Some(format!("ctr-{i}")),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();
    }

    let setup_elapsed = start.elapsed();
    assert!(
        setup_elapsed.as_secs() < 10,
        "setting up 100 stacks took {setup_elapsed:?} (>10s budget)"
    );

    // Verify isolation: each stack has its own events.
    for i in 0..100 {
        let name = format!("stack-{i}");
        let events = store.load_events(&name).unwrap();
        assert_eq!(events.len(), 2, "stack-{i} should have exactly 2 events");

        let observed = store.load_observed_state(&name).unwrap();
        assert_eq!(
            observed.len(),
            1,
            "stack-{i} should have exactly 1 observed state"
        );
        assert_eq!(observed[0].service_name, format!("svc-{i}"));
    }

    // Verify load for a random stack in the middle is fast.
    let start_load = std::time::Instant::now();
    let loaded = store.load_desired_state("stack-50").unwrap().unwrap();
    let load_elapsed = start_load.elapsed();
    assert_eq!(loaded.name, "stack-50");
    assert!(
        load_elapsed.as_millis() < 100,
        "loading stack-50 among 100 stacks took {load_elapsed:?} (>100ms budget)"
    );
}

/// Verify that a large desired state (50+ services) round-trips
/// correctly through save/load with acceptable performance.
#[test]
fn capacity_large_desired_state_50_services() {
    let store = StateStore::in_memory().unwrap();

    let services: Vec<ServiceSpec> = (0..50).map(|i| make_service(&format!("svc-{i}"))).collect();
    let spec = StackSpec {
        name: "large-app".to_string(),
        services,
        networks: vec![NetworkSpec {
            name: "default".to_string(),
            driver: "bridge".to_string(),
            subnet: None,
        }],
        volumes: vec![VolumeSpec {
            name: "data".to_string(),
            driver: "local".to_string(),
            driver_opts: None,
        }],
        secrets: vec![],
        disk_size_mb: Some(20480),
    };

    let start = std::time::Instant::now();
    store.save_desired_state("large-app", &spec).unwrap();
    let loaded = store.load_desired_state("large-app").unwrap().unwrap();
    let elapsed = start.elapsed();

    assert_eq!(loaded, spec);
    assert_eq!(loaded.services.len(), 50);
    assert!(
        elapsed.as_millis() < 500,
        "large spec (50 services) save+load took {elapsed:?} (>500ms budget)"
    );

    // Upsert to verify update path is also performant.
    let start_upsert = std::time::Instant::now();
    store.save_desired_state("large-app", &spec).unwrap();
    let upsert_elapsed = start_upsert.elapsed();
    assert!(
        upsert_elapsed.as_millis() < 500,
        "large spec upsert took {upsert_elapsed:?} (>500ms budget)"
    );
}

/// Regression: 1,000 event inserts must complete within 500ms.
#[test]
fn regression_1000_event_inserts_under_500ms() {
    let store = StateStore::in_memory().unwrap();

    let start = std::time::Instant::now();
    for i in 0..1_000 {
        store
            .emit_event(
                "regression-app",
                &StackEvent::ServiceCreating {
                    stack_name: "regression-app".to_string(),
                    service_name: format!("svc-{i}"),
                },
            )
            .unwrap();
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 500,
        "1,000 event inserts took {elapsed:?} — exceeds 500ms regression gate"
    );
}

/// Regression: idempotency key lookup among 500 keys must be under 50ms.
#[test]
fn regression_idempotency_lookup_under_50ms() {
    let store = StateStore::in_memory().unwrap();

    for i in 0..500 {
        let record = IdempotencyRecord {
            key: format!("idem-key-{i}"),
            operation: "create_sandbox".to_string(),
            request_hash: format!("hash-{i}"),
            response_json: r#"{"sandbox_id":"sb-1"}"#.to_string(),
            status_code: 201,
            created_at: 1_700_000_000,
            expires_at: 1_700_000_000 + IDEMPOTENCY_TTL_SECS,
        };
        store.save_idempotency_result(&record).unwrap();
    }

    let start = std::time::Instant::now();
    let result = store.find_idempotency_result("idem-key-250").unwrap();
    let elapsed = start.elapsed();

    assert!(result.is_some());
    assert!(
        elapsed.as_millis() < 50,
        "idempotency lookup among 500 keys took {elapsed:?} — exceeds 50ms regression gate"
    );
}

/// Regression: saving and loading observed state for 20 services
/// must complete within 200ms.
#[test]
fn regression_observed_state_20_services_under_200ms() {
    let store = StateStore::in_memory().unwrap();

    let start = std::time::Instant::now();
    for i in 0..20 {
        let state = ServiceObservedState {
            service_name: format!("svc-{i}"),
            phase: ServicePhase::Running,
            container_id: Some(format!("ctr-{i}")),
            last_error: None,
            ready: true,
        };
        store.save_observed_state("regression-app", &state).unwrap();
    }
    let loaded = store.load_observed_state("regression-app").unwrap();
    let elapsed = start.elapsed();

    assert_eq!(loaded.len(), 20);
    assert!(
        elapsed.as_millis() < 200,
        "20 observed state save+load took {elapsed:?} — exceeds 200ms regression gate"
    );
}

// ── Migration compatibility tests (vz-4g0) ──

/// Verify that the `control_metadata` table stores a detectable schema version
/// on first init, and that it can be read back as the expected v1 value.
#[test]
fn migration_v1_schema_detectable() {
    let store = StateStore::in_memory().unwrap();

    // Schema version must be present and equal to "1" after initial init.
    let version_str = store
        .get_control_metadata("schema_version")
        .unwrap()
        .expect("schema_version should be set on first init");
    assert_eq!(version_str, "1");

    // The typed accessor must agree.
    assert_eq!(store.schema_version().unwrap(), 1);

    // created_at must also be set.
    assert!(
        store.get_control_metadata("created_at").unwrap().is_some(),
        "created_at should be populated on first init"
    );
}

/// Pre-populate a database with v1 format data, then re-run `init_schema`
/// (which would add any new tables in a migration scenario). Verify that
/// previously-stored data is still readable.
#[test]
fn migration_old_data_readable_after_schema_update() {
    // Open an in-memory store — this runs init_schema once.
    let store = StateStore::in_memory().unwrap();

    // Pre-populate with v1 data: desired_state, observed_state, events.
    let spec = sample_spec();
    store.save_desired_state("myapp", &spec).unwrap();

    let obs = ServiceObservedState {
        service_name: "web".to_string(),
        phase: ServicePhase::Running,
        container_id: Some("ctr-web-001".to_string()),
        last_error: None,
        ready: true,
    };
    store.save_observed_state("myapp", &obs).unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::ServiceCreating {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
        )
        .unwrap();

    // Simulate a "migration" by running init_schema again — this calls
    // CREATE TABLE IF NOT EXISTS for every table, including any new ones.
    store.init_schema().unwrap();

    // Verify old data is still readable after the schema re-init.
    let loaded_spec = store.load_desired_state("myapp").unwrap().unwrap();
    assert_eq!(loaded_spec, spec);

    let loaded_obs = store.load_observed_state("myapp").unwrap();
    assert_eq!(loaded_obs.len(), 1);
    assert_eq!(loaded_obs[0].service_name, "web");
    assert_eq!(loaded_obs[0].phase, ServicePhase::Running);

    let loaded_events = store.load_events("myapp").unwrap();
    assert_eq!(loaded_events.len(), 1);

    // Schema version must not have been overwritten by re-init
    // (INSERT OR IGNORE preserves original value).
    assert_eq!(store.schema_version().unwrap(), 1);
}

/// Verify that all existing queries continue to work correctly after new
/// tables are added to the schema. This exercises the full query surface
/// against a freshly-initialized store.
#[test]
fn migration_new_tables_dont_break_old_queries() {
    let store = StateStore::in_memory().unwrap();

    // Exercise every major query path to ensure none are broken.
    // Desired state
    assert!(store.load_desired_state("nonexistent").unwrap().is_none());
    store.save_desired_state("s1", &sample_spec()).unwrap();
    assert!(store.load_desired_state("s1").unwrap().is_some());

    // Observed state
    assert!(store.load_observed_state("s1").unwrap().is_empty());
    let obs = ServiceObservedState {
        service_name: "svc".to_string(),
        phase: ServicePhase::Pending,
        container_id: None,
        last_error: None,
        ready: false,
    };
    store.save_observed_state("s1", &obs).unwrap();
    assert_eq!(store.load_observed_state("s1").unwrap().len(), 1);

    // Events
    store
        .emit_event(
            "s1",
            &StackEvent::StackApplyStarted {
                stack_name: "s1".to_string(),
                services_count: 1,
            },
        )
        .unwrap();
    assert_eq!(store.load_events("s1").unwrap().len(), 1);

    // Control metadata
    store.set_control_metadata("test_k", "test_v").unwrap();
    assert_eq!(
        store.get_control_metadata("test_k").unwrap().unwrap(),
        "test_v"
    );

    // Mount digests
    store
        .save_service_mount_digest("s1", "svc", "abc123")
        .unwrap();
    let digests = store.load_service_mount_digests("s1").unwrap();
    assert_eq!(digests.get("svc").unwrap(), "abc123");

    // Reconcile progress
    let actions = vec![Action::ServiceCreate {
        service_name: "svc".to_string(),
    }];
    store
        .save_reconcile_progress("s1", "op-1", &actions, 0)
        .unwrap();
    let progress = store.load_reconcile_progress("s1").unwrap().unwrap();
    assert_eq!(progress.operation_id, "op-1");

    // Checkpoint state (via entity CRUD)
    let checkpoint = Checkpoint {
        checkpoint_id: "ckpt-1".to_string(),
        sandbox_id: "sbx-1".to_string(),
        parent_checkpoint_id: None,
        class: CheckpointClass::FsQuick,
        state: CheckpointState::Ready,
        created_at: 1_700_000_000,
        compatibility_fingerprint: "fp-abc".to_string(),
    };
    store.save_checkpoint(&checkpoint).unwrap();
    let loaded = store.load_checkpoint("ckpt-1").unwrap().unwrap();
    assert_eq!(loaded.checkpoint_id, "ckpt-1");

    // Schema version still intact.
    assert_eq!(store.schema_version().unwrap(), 1);
}

/// Serialize and deserialize a checkpoint through the state store,
/// verifying no data loss in the round trip.
#[test]
fn checkpoint_format_round_trip_stability() {
    let store = StateStore::in_memory().unwrap();

    let original = Checkpoint {
        checkpoint_id: "ckpt-roundtrip-001".to_string(),
        sandbox_id: "sbx-roundtrip".to_string(),
        parent_checkpoint_id: Some("ckpt-parent-000".to_string()),
        class: CheckpointClass::VmFull,
        state: CheckpointState::Ready,
        created_at: 1_700_100_200,
        compatibility_fingerprint: "fp-sha256-deadbeef".to_string(),
    };

    store.save_checkpoint(&original).unwrap();
    let loaded = store
        .load_checkpoint("ckpt-roundtrip-001")
        .unwrap()
        .unwrap();

    assert_eq!(loaded.checkpoint_id, original.checkpoint_id);
    assert_eq!(loaded.sandbox_id, original.sandbox_id);
    assert_eq!(loaded.parent_checkpoint_id, original.parent_checkpoint_id);
    assert_eq!(loaded.class, original.class);
    assert_eq!(loaded.state, original.state);
    assert_eq!(loaded.created_at, original.created_at);
    assert_eq!(
        loaded.compatibility_fingerprint,
        original.compatibility_fingerprint
    );

    // Also test FsQuick class with no parent.
    let original_fs = Checkpoint {
        checkpoint_id: "ckpt-fs-001".to_string(),
        sandbox_id: "sbx-fs".to_string(),
        parent_checkpoint_id: None,
        class: CheckpointClass::FsQuick,
        state: CheckpointState::Creating,
        created_at: 1_700_200_300,
        compatibility_fingerprint: "fp-sha256-cafebabe".to_string(),
    };
    store.save_checkpoint(&original_fs).unwrap();
    let loaded_fs = store.load_checkpoint("ckpt-fs-001").unwrap().unwrap();

    assert_eq!(loaded_fs.checkpoint_id, original_fs.checkpoint_id);
    assert_eq!(loaded_fs.sandbox_id, original_fs.sandbox_id);
    assert_eq!(loaded_fs.parent_checkpoint_id, None);
    assert_eq!(loaded_fs.class, CheckpointClass::FsQuick);
    assert_eq!(loaded_fs.state, CheckpointState::Creating);
}

/// Verify that old event JSON formats (v1 tagged enums) can still be
/// deserialized after code evolution. This guards against accidental
/// serde tag or field renames.
#[test]
fn event_format_backward_compat() {
    // These are the canonical v1 JSON shapes — if serde(rename) or
    // serde(tag) attributes change, this test will catch it.
    let v1_event_jsons = vec![
        r#"{"type":"stack_apply_started","stack_name":"app","services_count":2}"#,
        r#"{"type":"stack_apply_completed","stack_name":"app","succeeded":2,"failed":0}"#,
        r#"{"type":"stack_apply_failed","stack_name":"app","error":"boom"}"#,
        r#"{"type":"service_creating","stack_name":"app","service_name":"web"}"#,
        r#"{"type":"service_ready","stack_name":"app","service_name":"web","runtime_id":"ctr-001"}"#,
        r#"{"type":"service_stopped","stack_name":"app","service_name":"web","exit_code":0}"#,
        r#"{"type":"service_failed","stack_name":"app","service_name":"web","error":"crash"}"#,
        r#"{"type":"stack_destroyed","stack_name":"app"}"#,
    ];

    for (i, json_str) in v1_event_jsons.iter().enumerate() {
        let parsed: Result<StackEvent, _> = serde_json::from_str(json_str);
        assert!(
            parsed.is_ok(),
            "v1 event JSON at index {i} failed to deserialize: {} — input: {json_str}",
            parsed.unwrap_err()
        );

        // Re-serialize and re-deserialize to verify stability.
        let re_serialized = serde_json::to_string(&parsed.unwrap()).unwrap();
        let re_parsed: StackEvent = serde_json::from_str(&re_serialized).unwrap();
        let _ = re_parsed; // Just verify it doesn't panic.
    }

    // Also verify that events stored in the DB can be loaded back.
    let store = StateStore::in_memory().unwrap();
    for json_str in &v1_event_jsons {
        // Directly insert raw JSON into the events table to simulate
        // events written by an older version.
        store
            .conn
            .execute(
                "INSERT INTO events (stack_name, event_json) VALUES ('compat', ?1)",
                params![*json_str],
            )
            .unwrap();
    }
    let loaded = store.load_events("compat").unwrap();
    assert_eq!(loaded.len(), v1_event_jsons.len());
}

#[test]
fn with_immediate_transaction_rolls_back_on_error() {
    let store = StateStore::in_memory().unwrap();

    let _: Result<(), StackError> = store.with_immediate_transaction(|tx| {
        let sandbox = Sandbox {
            sandbox_id: "sbx-rollback".to_string(),
            backend: SandboxBackend::MacosVz,
            spec: SandboxSpec::default(),
            state: SandboxState::Ready,
            created_at: 1,
            updated_at: 1,
            labels: std::collections::BTreeMap::new(),
        };
        tx.save_sandbox(&sandbox)?;
        Err(StackError::InvalidSpec("force rollback".to_string()))
    });

    assert!(store.load_sandbox("sbx-rollback").unwrap().is_none());
}

#[test]
fn daemon_pragmas_busy_timeout_waits_through_write_lock_contention() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("state.db");

    let contender_store =
        StateStore::open_with_pragmas(&db_path, StateStorePragmas::daemon_defaults()).unwrap();

    let (lock_started_tx, lock_started_rx) = std::sync::mpsc::channel();
    let db_path_for_lock_holder = db_path.clone();
    let lock_holder = std::thread::spawn(move || {
        let lock_holder_store = StateStore::open_with_pragmas(
            &db_path_for_lock_holder,
            StateStorePragmas::daemon_defaults(),
        )
        .unwrap();

        lock_holder_store
            .with_immediate_transaction(|tx| {
                tx.save_sandbox(&Sandbox {
                    sandbox_id: "sbx-lock-holder".to_string(),
                    backend: SandboxBackend::MacosVz,
                    spec: SandboxSpec::default(),
                    state: SandboxState::Ready,
                    created_at: 1,
                    updated_at: 1,
                    labels: std::collections::BTreeMap::new(),
                })?;

                lock_started_tx.send(()).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(300));
                Ok(())
            })
            .unwrap();
    });

    lock_started_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("lock holder should enter transaction");

    let start = std::time::Instant::now();
    contender_store
        .with_immediate_transaction(|tx| {
            tx.save_sandbox(&Sandbox {
                sandbox_id: "sbx-contender".to_string(),
                backend: SandboxBackend::MacosVz,
                spec: SandboxSpec::default(),
                state: SandboxState::Ready,
                created_at: 2,
                updated_at: 2,
                labels: std::collections::BTreeMap::new(),
            })?;
            Ok(())
        })
        .unwrap();
    let elapsed = start.elapsed();

    lock_holder.join().expect("lock holder thread should join");

    assert!(
        elapsed >= std::time::Duration::from_millis(200),
        "contender transaction should wait for lock release (elapsed={elapsed:?})"
    );
    assert!(
        contender_store
            .load_sandbox("sbx-lock-holder")
            .unwrap()
            .is_some()
    );
    assert!(
        contender_store
            .load_sandbox("sbx-contender")
            .unwrap()
            .is_some()
    );
}
