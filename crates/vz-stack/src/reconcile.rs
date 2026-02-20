//! Stack reconciliation entrypoint.
//!
//! The [`apply`] function persists desired state, emits lifecycle
//! events, and (in future beads) computes and executes an action plan
//! to converge observed state toward the desired spec.

use crate::error::StackError;
use crate::events::StackEvent;
use crate::spec::StackSpec;
use crate::state_store::{ServiceObservedState, ServicePhase, StateStore};

/// Result of an [`apply`] call.
#[derive(Debug, Clone, Default)]
pub struct ApplyResult {
    /// Number of services whose observed state was initialized or unchanged.
    pub services_synced: usize,
}

/// Persist desired state and reconcile observed state for a stack.
///
/// In this initial implementation the reconciler performs a no-op pass:
/// it stores the desired spec, initializes observed state for any new
/// services, and emits lifecycle events. The actual action planner and
/// executor are implemented in later beads.
pub fn apply(spec: &StackSpec, store: &StateStore) -> Result<ApplyResult, StackError> {
    // 1. Persist new desired state.
    store.save_desired_state(&spec.name, spec)?;

    // 2. Emit start event.
    store.emit_event(
        &spec.name,
        &StackEvent::StackApplyStarted {
            stack_name: spec.name.clone(),
            services_count: spec.services.len(),
        },
    )?;

    // 3. Load current observed state.
    let observed = store.load_observed_state(&spec.name)?;

    // 4. Ensure every service has an observed state row.
    let mut synced = 0;
    for svc in &spec.services {
        let exists = observed.iter().any(|o| o.service_name == svc.name);
        if !exists {
            store.save_observed_state(
                &spec.name,
                &ServiceObservedState {
                    service_name: svc.name.clone(),
                    phase: ServicePhase::Pending,
                    container_id: None,
                    last_error: None,
                },
            )?;
        }
        synced += 1;
    }

    // 5. Emit completion event (no-op: zero actions executed).
    store.emit_event(
        &spec.name,
        &StackEvent::StackApplyCompleted {
            stack_name: spec.name.clone(),
            succeeded: synced,
            failed: 0,
        },
    )?;

    Ok(ApplyResult {
        services_synced: synced,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::spec::{ServiceSpec, StackSpec};
    use std::collections::HashMap;

    fn two_service_spec() -> StackSpec {
        StackSpec {
            name: "testapp".to_string(),
            services: vec![
                ServiceSpec {
                    name: "web".to_string(),
                    image: "nginx:latest".to_string(),
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
                },
                ServiceSpec {
                    name: "db".to_string(),
                    image: "postgres:16".to_string(),
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
                },
            ],
            networks: vec![],
            volumes: vec![],
        }
    }

    #[test]
    fn apply_persists_desired_state() {
        let store = StateStore::in_memory().unwrap();
        let spec = two_service_spec();

        apply(&spec, &store).unwrap();

        let loaded = store.load_desired_state("testapp").unwrap();
        assert_eq!(loaded, Some(spec));
    }

    #[test]
    fn apply_initializes_observed_state_for_services() {
        let store = StateStore::in_memory().unwrap();
        let spec = two_service_spec();

        let result = apply(&spec, &store).unwrap();
        assert_eq!(result.services_synced, 2);

        let observed = store.load_observed_state("testapp").unwrap();
        assert_eq!(observed.len(), 2);
        assert!(observed.iter().all(|s| s.phase == ServicePhase::Pending));
    }

    #[test]
    fn apply_is_idempotent() {
        let store = StateStore::in_memory().unwrap();
        let spec = two_service_spec();

        apply(&spec, &store).unwrap();
        apply(&spec, &store).unwrap();

        // Desired state is the same.
        let loaded = store.load_desired_state("testapp").unwrap();
        assert_eq!(loaded, Some(spec));

        // Observed state still has 2 services (no duplicates).
        let observed = store.load_observed_state("testapp").unwrap();
        assert_eq!(observed.len(), 2);
    }

    #[test]
    fn apply_emits_start_and_completed_events() {
        let store = StateStore::in_memory().unwrap();
        let spec = two_service_spec();

        apply(&spec, &store).unwrap();

        let events = store.load_events("testapp").unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0],
            StackEvent::StackApplyStarted {
                services_count: 2,
                ..
            }
        ));
        assert!(matches!(
            events[1],
            StackEvent::StackApplyCompleted {
                succeeded: 2,
                failed: 0,
                ..
            }
        ));
    }

    #[test]
    fn apply_twice_emits_events_for_each_call() {
        let store = StateStore::in_memory().unwrap();
        let spec = two_service_spec();

        apply(&spec, &store).unwrap();
        apply(&spec, &store).unwrap();

        let events = store.load_events("testapp").unwrap();
        // Each apply emits 2 events (started + completed).
        assert_eq!(events.len(), 4);
    }

    #[test]
    fn apply_does_not_overwrite_existing_observed_state() {
        let store = StateStore::in_memory().unwrap();
        let spec = two_service_spec();

        apply(&spec, &store).unwrap();

        // Simulate reconciler updating "web" to Running.
        store
            .save_observed_state(
                "testapp",
                &ServiceObservedState {
                    service_name: "web".to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some("ctr-web".to_string()),
                    last_error: None,
                },
            )
            .unwrap();

        // Re-apply — should not overwrite Running state.
        apply(&spec, &store).unwrap();

        let observed = store.load_observed_state("testapp").unwrap();
        let web = observed.iter().find(|s| s.service_name == "web").unwrap();
        assert_eq!(web.phase, ServicePhase::Running);
        assert_eq!(web.container_id, Some("ctr-web".to_string()));
    }

    #[test]
    fn apply_empty_spec() {
        let store = StateStore::in_memory().unwrap();
        let spec = StackSpec {
            name: "empty".to_string(),
            services: vec![],
            networks: vec![],
            volumes: vec![],
        };

        let result = apply(&spec, &store).unwrap();
        assert_eq!(result.services_synced, 0);

        let events = store.load_events("empty").unwrap();
        assert_eq!(events.len(), 2);
    }
}
