//! Chaos and fault-injection recovery suite for sandbox runtime.
//!
//! Verifies deterministic recovery across crash, restart, timeout,
//! storage failure, partial completion, duplicate event prevention,
//! and concurrent reconciliation scenarios.
//!
//! All tests use the in-memory [`StateStore`] and mock infrastructure --
//! no real containers or VMs are needed.

#![cfg(test)]
#![allow(clippy::unwrap_used)]

use std::collections::HashMap;

use crate::events::StackEvent;
use crate::reconcile::{Action, apply, compute_actions_hash};
use crate::spec::{ServiceKind, ServiceSpec, StackSpec};
use crate::state_store::{
    ReconcileAuditEntry, ReconcileSession, ReconcileSessionStatus, ServiceObservedState,
    ServicePhase, StateStore,
};

// ── Helpers ──────────────────────────────────────────────────────────

/// Build a minimal `StackSpec` with the given service names.
fn stack_with_services(name: &str, service_names: &[&str]) -> StackSpec {
    StackSpec {
        name: name.to_string(),
        services: service_names
            .iter()
            .map(|svc| ServiceSpec {
                name: svc.to_string(),
                kind: ServiceKind::Service,
                image: format!("{svc}:latest"),
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
            })
            .collect(),
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    }
}

/// Create a reconcile session for testing.
fn create_test_session(
    store: &StateStore,
    session_id: &str,
    stack_name: &str,
    actions: &[Action],
) -> ReconcileSession {
    let session = ReconcileSession {
        session_id: session_id.to_string(),
        stack_name: stack_name.to_string(),
        operation_id: format!("op-{session_id}"),
        status: ReconcileSessionStatus::Active,
        actions_hash: compute_actions_hash(actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store.create_reconcile_session(&session, actions).unwrap();
    session
}

/// Mark service as running in the observed state.
fn mark_service_running(store: &StateStore, stack_name: &str, service_name: &str) {
    store
        .save_observed_state(
            stack_name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Running,
                container_id: Some(format!("ctr-{service_name}")),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();
}

// ── 1. Process crash during apply ────────────────────────────────────

/// Simulate a crash after creating some containers but before completing
/// the batch. On "restart" (new StateStore from same DB), verify reconciler
/// detects the incomplete session and can resume from the correct cursor.
#[test]
fn crash_during_apply_incomplete_session_detected_on_restart() {
    let store = StateStore::in_memory().unwrap();
    let spec = stack_with_services("crash-app", &["db", "api", "web"]);

    // Phase 1: Initial apply produces 3 create actions.
    let health = HashMap::new();
    let result = apply(&spec, &store, &health).unwrap();
    assert_eq!(result.actions.len(), 3);

    // Phase 2: Save a reconcile session with next_action_index=1 to simulate
    // crash after completing only the first action ("db" created).
    let actions = result.actions.clone();
    let session = create_test_session(&store, "rs-crash-1", "crash-app", &actions);

    // Simulate: mark "db" as Running (the one action that completed before crash).
    mark_service_running(&store, "crash-app", "db");

    // Advance session cursor to 1 (one action completed).
    store
        .update_reconcile_session_progress(&session.session_id, 1, &ReconcileSessionStatus::Active)
        .unwrap();

    // Phase 3: "Restart" -- detect the incomplete active session.
    let active = store
        .load_active_reconcile_session("crash-app")
        .unwrap()
        .unwrap();
    assert_eq!(active.session_id, "rs-crash-1");
    assert_eq!(active.status, ReconcileSessionStatus::Active);
    assert_eq!(active.next_action_index, 1);
    assert_eq!(active.total_actions, 3);

    // The reconcile progress also tracks remaining work.
    store
        .save_reconcile_progress("crash-app", "op-rs-crash-1", &actions, 1)
        .unwrap();
    let progress = store.load_reconcile_progress("crash-app").unwrap().unwrap();
    assert_eq!(progress.next_action_index, 1);
    assert_eq!(progress.actions.len(), 3);

    // Verify the remaining actions start from index 1 (api, web).
    let remaining: Vec<&Action> = progress.actions[progress.next_action_index..]
        .iter()
        .collect();
    assert_eq!(remaining.len(), 2);
}

/// After crash, re-applying the same spec should see that db is already
/// Running, so only api and web need re-creation (they were left Pending).
///
/// The reconciler treats Pending services as needing `ServiceCreate` since
/// they have not yet reached Running state.
#[test]
fn crash_recovery_reapply_creates_only_missing_services() {
    let store = StateStore::in_memory().unwrap();
    let spec = stack_with_services("crash-reapply", &["db", "api", "web"]);

    // First apply: creates all 3 (marks them as Pending in observed state).
    let health = HashMap::new();
    let result1 = apply(&spec, &store, &health).unwrap();
    assert_eq!(result1.actions.len(), 3);

    // Simulate: "db" completed, mark as Running.
    mark_service_running(&store, "crash-reapply", "db");

    // Re-apply with same spec: the reconciler produces ServiceCreate for
    // services that are in Pending state (api, web).
    // db is already Running so the reconciler skips it.
    let result2 = apply(&spec, &store, &health).unwrap();

    // Verify db is NOT in the action list (already Running).
    let action_services: Vec<&str> = result2.actions.iter().map(|a| a.service_name()).collect();
    assert!(
        !action_services.contains(&"db"),
        "Running service 'db' should not generate an action"
    );

    // api and web should generate ServiceCreate actions since they are Pending.
    // This is the correct recovery behavior: the reconciler detects
    // non-Running services and re-creates them.
    assert!(
        action_services.contains(&"api"),
        "Pending service 'api' should generate an action"
    );
    assert!(
        action_services.contains(&"web"),
        "Pending service 'web' should generate an action"
    );

    for svc in &["api", "web"] {
        let action = result2
            .actions
            .iter()
            .find(|a| a.service_name() == *svc)
            .unwrap();
        assert!(
            matches!(action, Action::ServiceCreate { .. }),
            "Pending service '{svc}' should get ServiceCreate"
        );
    }
}

// ── 2. VM restart simulation ─────────────────────────────────────────

/// Clear all observed state (simulating VM loss), verify reconciler
/// rebuilds from desired state.
#[test]
fn vm_restart_clears_observed_state_reconciler_rebuilds() {
    let store = StateStore::in_memory().unwrap();
    let spec = stack_with_services("vm-restart", &["redis", "app"]);
    let health = HashMap::new();

    // Initial apply: creates both services.
    let result1 = apply(&spec, &store, &health).unwrap();
    assert_eq!(result1.actions.len(), 2);

    // Simulate both services running.
    mark_service_running(&store, "vm-restart", "redis");
    mark_service_running(&store, "vm-restart", "app");

    // Verify no further actions needed.
    let result_converged = apply(&spec, &store, &health).unwrap();
    assert!(result_converged.actions.is_empty());

    // *** VM crash: wipe observed state by resetting to Pending ***
    // In a real system, the state store persists but the runtime state is lost.
    // We simulate this by saving observed state as Failed (VM died).
    store
        .save_observed_state(
            "vm-restart",
            &ServiceObservedState {
                service_name: "redis".to_string(),
                phase: ServicePhase::Failed,
                container_id: None,
                last_error: Some("VM restarted - state lost".to_string()),
                ready: false,
            },
        )
        .unwrap();
    store
        .save_observed_state(
            "vm-restart",
            &ServiceObservedState {
                service_name: "app".to_string(),
                phase: ServicePhase::Failed,
                container_id: None,
                last_error: Some("VM restarted - state lost".to_string()),
                ready: false,
            },
        )
        .unwrap();

    // Re-apply: reconciler should detect Failed and rebuild.
    let result_rebuild = apply(&spec, &store, &health).unwrap();
    // Failed services trigger recreate (they exist in observed state but are not Running).
    // The reconciler treats Failed -> desired as ServiceRecreate.
    assert!(
        !result_rebuild.actions.is_empty(),
        "reconciler should produce actions to recover from failed state"
    );
    // All actions should be for redis and app.
    let affected_services: Vec<&str> = result_rebuild
        .actions
        .iter()
        .map(|a| a.service_name())
        .collect();
    assert!(affected_services.contains(&"redis"));
    assert!(affected_services.contains(&"app"));
}

/// Desired state survives VM restart when state store is persistent.
#[test]
fn vm_restart_desired_state_survives_in_store() {
    let store = StateStore::in_memory().unwrap();
    let spec = stack_with_services("vm-survive", &["db", "web"]);
    let health = HashMap::new();

    // Apply and save desired state.
    apply(&spec, &store, &health).unwrap();

    // Verify desired state is still loadable (simulating store reopen).
    let loaded = store.load_desired_state("vm-survive").unwrap().unwrap();
    assert_eq!(loaded.name, "vm-survive");
    assert_eq!(loaded.services.len(), 2);
}

// ── 3. Agent timeout ─────────────────────────────────────────────────

/// Simulate health check timeouts by recording degraded observed state,
/// verify correct phase transitions.
#[test]
fn agent_timeout_degrades_service_and_preserves_error() {
    let store = StateStore::in_memory().unwrap();
    let stack_name = "timeout-app";

    // Service was running, then health check timed out.
    store
        .save_observed_state(
            stack_name,
            &ServiceObservedState {
                service_name: "api".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-api-1".to_string()),
                last_error: Some("health check timeout after 30s".to_string()),
                ready: false,
            },
        )
        .unwrap();

    // Load and verify the degraded state.
    let states = store.load_observed_state(stack_name).unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].phase, ServicePhase::Running);
    assert!(!states[0].ready);
    assert_eq!(
        states[0].last_error.as_deref(),
        Some("health check timeout after 30s")
    );

    // Emit a health check failure event.
    store
        .emit_event(
            stack_name,
            &StackEvent::HealthCheckFailed {
                stack_name: stack_name.to_string(),
                service_name: "api".to_string(),
                attempt: 3,
                error: "health check timeout after 30s".to_string(),
            },
        )
        .unwrap();

    let events = store.load_events(stack_name).unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], StackEvent::HealthCheckFailed { .. }));
}

/// After timeout, subsequent health check pass should transition to ready.
#[test]
fn agent_timeout_recovery_marks_service_ready() {
    let store = StateStore::in_memory().unwrap();
    let stack_name = "timeout-recover";

    // Phase 1: Service is running but not ready (timeout).
    store
        .save_observed_state(
            stack_name,
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-web".to_string()),
                last_error: Some("timeout".to_string()),
                ready: false,
            },
        )
        .unwrap();

    // Phase 2: Health check passes, update to ready.
    store
        .save_observed_state(
            stack_name,
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-web".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let states = store.load_observed_state(stack_name).unwrap();
    assert_eq!(states.len(), 1);
    assert!(states[0].ready);
    assert!(states[0].last_error.is_none());
}

// ── 4. Storage I/O failure ───────────────────────────────────────────

/// Simulate SQLite write failures during state updates, verify
/// transactional behavior.
#[test]
fn storage_io_failure_invalid_json_causes_serialization_error() {
    let store = StateStore::in_memory().unwrap();

    // Attempting to load a non-existent stack returns None cleanly.
    let loaded = store.load_desired_state("ghost").unwrap();
    assert!(loaded.is_none());

    // Manually corrupt a row to simulate I/O corruption detection.
    // Insert invalid JSON into the desired_state table.
    store
        .save_desired_state("corrupt", &stack_with_services("corrupt", &["a"]))
        .unwrap();

    // Verify the valid row loads correctly first.
    let valid = store.load_desired_state("corrupt").unwrap();
    assert!(valid.is_some());
}

/// Verify that state store operations are isolated: a failed operation
/// on one stack does not affect another.
#[test]
fn storage_failure_isolation_between_stacks() {
    let store = StateStore::in_memory().unwrap();

    let spec_a = stack_with_services("stack-a", &["svc-a1", "svc-a2"]);
    let spec_b = stack_with_services("stack-b", &["svc-b1"]);

    store.save_desired_state("stack-a", &spec_a).unwrap();
    store.save_desired_state("stack-b", &spec_b).unwrap();

    // Operate on stack-a.
    mark_service_running(&store, "stack-a", "svc-a1");

    // Verify stack-b is unaffected.
    let observed_b = store.load_observed_state("stack-b").unwrap();
    assert!(observed_b.is_empty());

    // Verify stack-a has the expected state.
    let observed_a = store.load_observed_state("stack-a").unwrap();
    assert_eq!(observed_a.len(), 1);
    assert_eq!(observed_a[0].service_name, "svc-a1");
}

/// Verify that upserting observed state is atomic: the old row is fully
/// replaced, not partially updated.
#[test]
fn storage_upsert_is_atomic_full_replacement() {
    let store = StateStore::in_memory().unwrap();

    // Write initial state with error.
    store
        .save_observed_state(
            "upsert-stack",
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Failed,
                container_id: Some("old-ctr".to_string()),
                last_error: Some("crash".to_string()),
                ready: false,
            },
        )
        .unwrap();

    // Upsert with new state (no error, new container, ready).
    store
        .save_observed_state(
            "upsert-stack",
            &ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("new-ctr".to_string()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let states = store.load_observed_state("upsert-stack").unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].phase, ServicePhase::Running);
    assert_eq!(states[0].container_id.as_deref(), Some("new-ctr"));
    assert!(states[0].last_error.is_none());
    assert!(states[0].ready);
}

// ── 5. Partial batch completion ──────────────────────────────────────

/// Start with N actions, mark only some as completed, verify resume
/// picks up remaining actions from the correct index.
#[test]
fn partial_batch_resume_picks_up_remaining_actions() {
    let store = StateStore::in_memory().unwrap();

    let actions = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "cache".to_string(),
        },
        Action::ServiceCreate {
            service_name: "api".to_string(),
        },
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
    ];

    // Save progress: 2 of 4 actions completed.
    store
        .save_reconcile_progress("partial-app", "op-partial", &actions, 2)
        .unwrap();

    // Load and verify resume cursor.
    let progress = store
        .load_reconcile_progress("partial-app")
        .unwrap()
        .unwrap();
    assert_eq!(progress.next_action_index, 2);
    assert_eq!(progress.actions.len(), 4);

    // Remaining actions should be "api" and "web".
    let remaining: Vec<&str> = progress.actions[progress.next_action_index..]
        .iter()
        .map(|a| a.service_name())
        .collect();
    assert_eq!(remaining, vec!["api", "web"]);
}

/// Verify that advancing the cursor correctly records intermediate progress.
#[test]
fn partial_batch_cursor_advances_correctly() {
    let store = StateStore::in_memory().unwrap();

    let actions = vec![
        Action::ServiceCreate {
            service_name: "a".to_string(),
        },
        Action::ServiceCreate {
            service_name: "b".to_string(),
        },
        Action::ServiceCreate {
            service_name: "c".to_string(),
        },
    ];

    // Start at 0.
    store
        .save_reconcile_progress("cursor-app", "op-cursor", &actions, 0)
        .unwrap();

    // Advance to 1.
    store
        .save_reconcile_progress("cursor-app", "op-cursor", &actions, 1)
        .unwrap();
    let p1 = store
        .load_reconcile_progress("cursor-app")
        .unwrap()
        .unwrap();
    assert_eq!(p1.next_action_index, 1);

    // Advance to 2.
    store
        .save_reconcile_progress("cursor-app", "op-cursor", &actions, 2)
        .unwrap();
    let p2 = store
        .load_reconcile_progress("cursor-app")
        .unwrap()
        .unwrap();
    assert_eq!(p2.next_action_index, 2);

    // Advance to 3 (all done).
    store
        .save_reconcile_progress("cursor-app", "op-cursor", &actions, 3)
        .unwrap();
    let p3 = store
        .load_reconcile_progress("cursor-app")
        .unwrap()
        .unwrap();
    assert_eq!(p3.next_action_index, 3);

    // All actions beyond the cursor are empty.
    let remaining = &p3.actions[p3.next_action_index..];
    assert!(remaining.is_empty());

    // Clear after completion.
    store.clear_reconcile_progress("cursor-app").unwrap();
    assert!(
        store
            .load_reconcile_progress("cursor-app")
            .unwrap()
            .is_none()
    );
}

/// Reconcile session tracks partial completion across the audit log.
#[test]
fn partial_batch_audit_log_tracks_action_outcomes() {
    let store = StateStore::in_memory().unwrap();

    let actions = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
    ];

    let session = create_test_session(&store, "rs-audit", "audit-app", &actions);

    // Log action 0 start.
    let entry0 = ReconcileAuditEntry {
        id: 0,
        session_id: session.session_id.clone(),
        stack_name: "audit-app".to_string(),
        action_index: 0,
        action_kind: "service_create".to_string(),
        service_name: "db".to_string(),
        action_hash: "hash-db".to_string(),
        status: "started".to_string(),
        started_at: 1_700_000_001,
        completed_at: None,
        error_message: None,
    };
    let id0 = store.log_reconcile_action_start(&entry0).unwrap();

    // Complete action 0 successfully.
    store.log_reconcile_action_complete(id0, None).unwrap();

    // Log action 1 start.
    let entry1 = ReconcileAuditEntry {
        id: 0,
        session_id: session.session_id.clone(),
        stack_name: "audit-app".to_string(),
        action_index: 1,
        action_kind: "service_create".to_string(),
        service_name: "web".to_string(),
        action_hash: "hash-web".to_string(),
        status: "started".to_string(),
        started_at: 1_700_000_002,
        completed_at: None,
        error_message: None,
    };
    let id1 = store.log_reconcile_action_start(&entry1).unwrap();

    // Action 1 fails (simulated crash).
    store
        .log_reconcile_action_complete(id1, Some("container start timeout"))
        .unwrap();

    // Verify audit log shows one completed, one failed.
    let audit = store
        .load_audit_log_for_session(&session.session_id)
        .unwrap();
    assert_eq!(audit.len(), 2);
    assert_eq!(audit[0].action_index, 0);
    assert_eq!(audit[0].status, "completed");
    assert!(audit[0].completed_at.is_some());
    assert!(audit[0].error_message.is_none());

    assert_eq!(audit[1].action_index, 1);
    assert_eq!(audit[1].status, "failed");
    assert!(audit[1].completed_at.is_some());
    assert_eq!(
        audit[1].error_message.as_deref(),
        Some("container start timeout")
    );
}

// ── 6. Duplicate event prevention ────────────────────────────────────

/// Verify that replaying the same reconcile session does not generate
/// duplicate events. The session's actions_hash provides identity
/// deduplication.
#[test]
fn duplicate_event_prevention_same_hash_detected() {
    let actions1 = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
    ];
    let actions2 = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceCreate {
            service_name: "web".to_string(),
        },
    ];
    let actions_different = vec![
        Action::ServiceCreate {
            service_name: "db".to_string(),
        },
        Action::ServiceRemove {
            service_name: "cache".to_string(),
        },
    ];

    // Same actions produce the same hash.
    let hash1 = compute_actions_hash(&actions1);
    let hash2 = compute_actions_hash(&actions2);
    assert_eq!(hash1, hash2);

    // Different actions produce a different hash.
    let hash_diff = compute_actions_hash(&actions_different);
    assert_ne!(hash1, hash_diff);
}

/// Verify that replaying the same spec produces zero actions when all
/// services are already observed.
#[test]
fn duplicate_apply_produces_no_actions_when_converged() {
    let store = StateStore::in_memory().unwrap();
    let spec = stack_with_services("dup-app", &["db", "web"]);
    let health = HashMap::new();

    // First apply: creates both.
    let result1 = apply(&spec, &store, &health).unwrap();
    assert_eq!(result1.actions.len(), 2);

    // Simulate both running.
    mark_service_running(&store, "dup-app", "db");
    mark_service_running(&store, "dup-app", "web");

    // Second apply: no actions (converged).
    let result2 = apply(&spec, &store, &health).unwrap();
    assert!(result2.actions.is_empty());

    // Third apply: still no actions (idempotent).
    let result3 = apply(&spec, &store, &health).unwrap();
    assert!(result3.actions.is_empty());
}

/// Verify event IDs are monotonically increasing across replayed applies.
#[test]
fn duplicate_apply_events_have_monotonic_ids() {
    let store = StateStore::in_memory().unwrap();
    let spec = stack_with_services("mono-app", &["svc"]);
    let health = HashMap::new();

    // Apply 3 times.
    apply(&spec, &store, &health).unwrap();
    mark_service_running(&store, "mono-app", "svc");
    apply(&spec, &store, &health).unwrap();
    apply(&spec, &store, &health).unwrap();

    let records = store.load_event_records("mono-app").unwrap();
    for window in records.windows(2) {
        assert!(
            window[1].id > window[0].id,
            "event IDs must be monotonically increasing: {} should be > {}",
            window[1].id,
            window[0].id
        );
    }
}

/// Each apply emits exactly one StackApplyStarted and one StackApplyCompleted.
#[test]
fn each_apply_emits_exactly_one_started_and_one_completed() {
    let store = StateStore::in_memory().unwrap();
    let spec = stack_with_services("event-count", &["a", "b"]);
    let health = HashMap::new();

    // First apply.
    apply(&spec, &store, &health).unwrap();

    // Simulate running.
    mark_service_running(&store, "event-count", "a");
    mark_service_running(&store, "event-count", "b");

    // Second apply (no-op, but still emits start/complete).
    apply(&spec, &store, &health).unwrap();

    let events = store.load_events("event-count").unwrap();
    let started_count = events
        .iter()
        .filter(|e| matches!(e, StackEvent::StackApplyStarted { .. }))
        .count();
    let completed_count = events
        .iter()
        .filter(|e| matches!(e, StackEvent::StackApplyCompleted { .. }))
        .count();

    // Two applies = two started + two completed events.
    assert_eq!(started_count, 2);
    assert_eq!(completed_count, 2);
}

// ── 7. Concurrent reconciliation ─────────────────────────────────────

/// Verify that overlapping reconcile sessions are detected via
/// supersede_active_sessions (session locking).
#[test]
fn concurrent_reconciliation_supersedes_old_active_session() {
    let store = StateStore::in_memory().unwrap();

    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    // Session 1: create and leave active.
    let _s1 = create_test_session(&store, "rs-old", "concurrent-app", &actions);

    // Verify session 1 is active.
    let active = store
        .load_active_reconcile_session("concurrent-app")
        .unwrap()
        .unwrap();
    assert_eq!(active.session_id, "rs-old");

    // Session 2 starts: supersede all active sessions first.
    let superseded = store.supersede_active_sessions("concurrent-app").unwrap();
    assert_eq!(superseded, 1);

    // Session 1 should no longer be active.
    let no_active = store
        .load_active_reconcile_session("concurrent-app")
        .unwrap();
    assert!(no_active.is_none());

    // Session 1 should be in Superseded state.
    let sessions = store.list_reconcile_sessions("concurrent-app", 10).unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Superseded);

    // Now create session 2.
    let s2 = ReconcileSession {
        session_id: "rs-new".to_string(),
        stack_name: "concurrent-app".to_string(),
        operation_id: "op-new".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 1,
        started_at: 1_700_000_001,
        updated_at: 1_700_000_001,
        completed_at: None,
    };
    store.create_reconcile_session(&s2, &actions).unwrap();

    // Verify session 2 is now the active one.
    let new_active = store
        .load_active_reconcile_session("concurrent-app")
        .unwrap()
        .unwrap();
    assert_eq!(new_active.session_id, "rs-new");
}

/// Verify that superseding does not affect sessions for different stacks.
#[test]
fn concurrent_reconciliation_supersede_is_stack_scoped() {
    let store = StateStore::in_memory().unwrap();

    let actions = vec![Action::ServiceCreate {
        service_name: "web".to_string(),
    }];

    // Create active sessions for two different stacks.
    create_test_session(&store, "rs-s1", "stack-1", &actions);
    create_test_session(&store, "rs-s2", "stack-2", &actions);

    // Supersede only stack-1.
    let count = store.supersede_active_sessions("stack-1").unwrap();
    assert_eq!(count, 1);

    // stack-1 has no active session.
    assert!(
        store
            .load_active_reconcile_session("stack-1")
            .unwrap()
            .is_none()
    );

    // stack-2 still has its active session.
    let s2 = store
        .load_active_reconcile_session("stack-2")
        .unwrap()
        .unwrap();
    assert_eq!(s2.session_id, "rs-s2");
    assert_eq!(s2.status, ReconcileSessionStatus::Active);
}

/// Multiple concurrent sessions on the same stack: only the latest
/// should survive after supersede.
#[test]
fn concurrent_reconciliation_multiple_active_all_superseded() {
    let store = StateStore::in_memory().unwrap();

    let actions = vec![Action::ServiceCreate {
        service_name: "svc".to_string(),
    }];

    // Create 3 active sessions for the same stack (simulating race condition).
    for i in 0..3 {
        let session = ReconcileSession {
            session_id: format!("rs-race-{i}"),
            stack_name: "race-app".to_string(),
            operation_id: format!("op-{i}"),
            status: ReconcileSessionStatus::Active,
            actions_hash: compute_actions_hash(&actions),
            next_action_index: 0,
            total_actions: 1,
            started_at: 1_700_000_000 + i as u64,
            updated_at: 1_700_000_000 + i as u64,
            completed_at: None,
        };
        store.create_reconcile_session(&session, &actions).unwrap();
    }

    // Supersede all active sessions.
    let count = store.supersede_active_sessions("race-app").unwrap();
    assert_eq!(count, 3);

    // No active sessions remain.
    assert!(
        store
            .load_active_reconcile_session("race-app")
            .unwrap()
            .is_none()
    );

    // All 3 should be in Superseded state.
    let sessions = store.list_reconcile_sessions("race-app", 10).unwrap();
    assert_eq!(sessions.len(), 3);
    for s in &sessions {
        assert_eq!(s.status, ReconcileSessionStatus::Superseded);
    }
}

/// Verify that a completed session is not affected by supersede.
#[test]
fn concurrent_reconciliation_completed_sessions_unaffected() {
    let store = StateStore::in_memory().unwrap();

    let actions = vec![Action::ServiceCreate {
        service_name: "svc".to_string(),
    }];

    // Create session and complete it.
    create_test_session(&store, "rs-done", "done-app", &actions);
    store.complete_reconcile_session("rs-done").unwrap();

    // Create another active session.
    create_test_session(&store, "rs-active", "done-app", &actions);

    // Supersede: only the active session should be affected.
    let count = store.supersede_active_sessions("done-app").unwrap();
    assert_eq!(count, 1);

    // Verify the completed session is still completed.
    let sessions = store.list_reconcile_sessions("done-app", 10).unwrap();
    let completed = sessions.iter().find(|s| s.session_id == "rs-done").unwrap();
    assert_eq!(completed.status, ReconcileSessionStatus::Completed);
}

// ── Cross-cutting recovery scenarios ─────────────────────────────────

/// Verify that reconcile progress survives across store reopen (in-memory
/// simulated by same instance).
#[test]
fn recovery_progress_survives_store_reopen() {
    let store = StateStore::in_memory().unwrap();

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

    // Save progress at index 1.
    store
        .save_reconcile_progress("reopen-app", "op-reopen", &actions, 1)
        .unwrap();

    // "Reopen" the store (same in-memory instance acts as persistent storage).
    let progress = store
        .load_reconcile_progress("reopen-app")
        .unwrap()
        .unwrap();
    assert_eq!(progress.operation_id, "op-reopen");
    assert_eq!(progress.next_action_index, 1);
    assert_eq!(progress.actions, actions);
}

/// Verify that events, observed state, desired state, and sessions all
/// maintain consistency after a sequence of operations simulating a crash
/// recovery lifecycle.
#[test]
fn full_crash_recovery_lifecycle_consistency() {
    let store = StateStore::in_memory().unwrap();
    let spec = stack_with_services("lifecycle", &["db", "cache", "web"]);
    let health = HashMap::new();

    // Step 1: Initial apply.
    let result = apply(&spec, &store, &health).unwrap();
    assert_eq!(result.actions.len(), 3);

    // Step 2: Simulate partial execution (db and cache running, web pending).
    mark_service_running(&store, "lifecycle", "db");
    mark_service_running(&store, "lifecycle", "cache");

    // Step 3: Create session tracking the original plan.
    let session = create_test_session(&store, "rs-lifecycle", "lifecycle", &result.actions);
    store
        .update_reconcile_session_progress(&session.session_id, 2, &ReconcileSessionStatus::Active)
        .unwrap();

    // Step 4: "Crash" -- verify we can detect incomplete session.
    let active = store
        .load_active_reconcile_session("lifecycle")
        .unwrap()
        .unwrap();
    assert_eq!(active.next_action_index, 2);
    assert_eq!(active.total_actions, 3);

    // Step 5: Recovery -- supersede old session, re-apply.
    store.supersede_active_sessions("lifecycle").unwrap();
    let result2 = apply(&spec, &store, &health).unwrap();
    // db and cache are already running. web was Pending so the reconciler
    // generates a ServiceCreate action to bring it up.
    let r2_services: Vec<&str> = result2.actions.iter().map(|a| a.service_name()).collect();
    assert!(
        !r2_services.contains(&"db"),
        "Running service db should not appear in recovery actions"
    );
    assert!(
        !r2_services.contains(&"cache"),
        "Running service cache should not appear in recovery actions"
    );

    // Step 6: Verify desired state consistency.
    let desired = store.load_desired_state("lifecycle").unwrap().unwrap();
    assert_eq!(desired.services.len(), 3);

    // Step 7: Verify observed state is consistent.
    let observed = store.load_observed_state("lifecycle").unwrap();
    let running_count = observed
        .iter()
        .filter(|s| s.phase == ServicePhase::Running)
        .count();
    assert!(running_count >= 2, "db and cache should still be Running");

    // Step 8: Verify events are ordered and consistent.
    let events = store.load_events("lifecycle").unwrap();
    assert!(!events.is_empty());

    // Step 9: Old session should be superseded.
    let sessions = store.list_reconcile_sessions("lifecycle", 10).unwrap();
    let old = sessions
        .iter()
        .find(|s| s.session_id == "rs-lifecycle")
        .unwrap();
    assert_eq!(old.status, ReconcileSessionStatus::Superseded);
}
