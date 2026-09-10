//! The persisted Machine-scoped Delete: begin, acknowledge, finish, and the
//! things that must not happen underneath it.
//!
//! The identity half of a fork Delete — `delete_exact_machine_fork` — is proved
//! next door. What these tests prove is the half that makes it reachable: a
//! durable lifecycle operation that fences one Machine's teardown without
//! declaring the Environment `Deleting`, without taking its
//! `active_operation_id`, and without letting anything else decide this
//! Environment's ownership until it finishes.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use vz_runtime_contract::{
    EnvironmentLifecycleKind, EnvironmentLifecycleStatus, LifecycleStepResult,
    MachineLifecycleStepAcknowledgement, OwnershipCleanupStepAcknowledgement,
};

use super::tests::fixture;

/// Acknowledge every step of a begun scoped Delete, the way the daemon does.
fn acknowledge_every_step(
    store: &StateStore,
    operation: &vz_runtime_contract::EnvironmentLifecycleOperation,
    now: u64,
) -> vz_runtime_contract::EnvironmentLifecycleOperation {
    let mut current = operation.clone();
    for step in operation.machine_steps.clone() {
        current = store
            .acknowledge_environment_machine_step(
                &MachineLifecycleStepAcknowledgement {
                    operation_id: current.operation_id.clone(),
                    generation: current.generation,
                    machine_id: step.machine_id.clone(),
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: step.expected_incarnation.clone(),
                    resulting_incarnation: None,
                    resulting_activation: None,
                    result: LifecycleStepResult::Succeeded,
                },
                now,
            )
            .unwrap();
    }
    for step in operation.cleanup_steps.clone() {
        current = store
            .acknowledge_environment_cleanup_step(
                &OwnershipCleanupStepAcknowledgement {
                    operation_id: current.operation_id.clone(),
                    generation: current.generation,
                    ownership: step.ownership.clone(),
                    result: LifecycleStepResult::Succeeded,
                },
                now,
            )
            .unwrap();
    }
    current
}

#[test]
fn a_machine_scoped_delete_reclaims_exactly_its_fork_and_leaves_the_environment_serving() {
    let (_root, store, environment) = fixture();
    let environment_id = environment.environment_id.to_string();
    let parent = environment.machines[0].machine_id.clone();
    let doomed = store
        .fork_machine_in_environment(&environment_id, &parent, "feat-x", 9)
        .unwrap();
    let survivor = store
        .fork_machine_in_environment(&environment_id, &parent, "feat-y", 10)
        .unwrap();
    let before = store
        .load_environment_instance(&environment_id)
        .unwrap()
        .unwrap();

    let operation = store
        .begin_machine_lifecycle_delete(
            &environment_id,
            doomed.machine_id(),
            "req-fork-delete",
            "idem-fork-delete",
            &format!("sha256:{}", "c".repeat(64)),
            11,
        )
        .unwrap();
    assert_eq!(operation.machine_scope.as_ref(), Some(doomed.machine_id()));
    assert_eq!(operation.kind, EnvironmentLifecycleKind::Delete);
    assert_eq!(operation.status, EnvironmentLifecycleStatus::Running);

    // Mid-operation, the Environment is exactly where it was, minus nothing:
    // its state, its active operation and all three Machines are untouched.
    let during = store
        .load_environment_instance(&environment_id)
        .unwrap()
        .unwrap();
    assert_eq!(during.state, before.state);
    assert_eq!(during.active_operation_id, None);
    assert_eq!(during.lifecycle_generation, operation.generation);
    assert_eq!(during.machines.len(), 3);

    let acknowledged = acknowledge_every_step(&store, &operation, 12);
    let (finished, remaining) = store
        .finish_machine_delete(
            acknowledged.operation_id.as_str(),
            acknowledged.generation,
            13,
        )
        .unwrap();
    assert_eq!(finished.status, EnvironmentLifecycleStatus::Succeeded);
    assert!(finished.completed_at.is_some());

    // Exactly one Machine gone. The parent and the sibling keep every row they
    // had, which is the half a cascade gone wrong would quietly break.
    let names: Vec<_> = remaining
        .machines
        .iter()
        .map(|machine| machine.name.clone())
        .collect();
    assert_eq!(remaining.machines.len(), 2);
    assert!(names.contains(&"backend".to_string()));
    assert!(names.contains(&"backend@feat-y".to_string()));
    assert_eq!(remaining.state, before.state);
    assert_eq!(remaining.active_operation_id, None);
    for (table, column) in [
        ("machine_instances", "machine_id"),
        ("environment_network_attachments", "machine_id"),
        ("environment_machine_egress", "machine_id"),
        ("topology_ownership", "machine_id"),
    ] {
        let rows: usize = store
            .conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE {column} = ?1"),
                rusqlite::params![doomed.machine_id().as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 0, "`{table}` still holds the reclaimed fork");
    }
    let survivor_rows: usize = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM topology_ownership WHERE machine_id = ?1",
            rusqlite::params![survivor.machine_id().as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(survivor_rows, survivor.ownership.len());
    // No tombstone: the Environment identity was never retired.
    assert!(
        store
            .load_environment_tombstone(&environment_id)
            .unwrap()
            .is_none()
    );

    // Replay after a lost response answers from the durable journal rather than
    // attempting the reclamation a second time.
    let (replayed, _) = store
        .finish_machine_delete(finished.operation_id.as_str(), finished.generation, 14)
        .unwrap();
    assert_eq!(replayed, finished);
}

#[test]
fn an_unfinished_machine_scope_blocks_every_new_lifecycle_operation() {
    let (_root, store, environment) = fixture();
    let environment_id = environment.environment_id.to_string();
    let parent = environment.machines[0].machine_id.clone();
    let first = store
        .fork_machine_in_environment(&environment_id, &parent, "feat-x", 9)
        .unwrap();
    let second = store
        .fork_machine_in_environment(&environment_id, &parent, "feat-y", 10)
        .unwrap();
    let operation = store
        .begin_machine_lifecycle_delete(
            &environment_id,
            first.machine_id(),
            "req-fork-delete",
            "idem-fork-delete",
            &format!("sha256:{}", "c".repeat(64)),
            11,
        )
        .unwrap();

    // A scoped operation holds no `active_operation_id`, so nothing would stop
    // an Up from taking the next generation and stranding this teardown with the
    // fork stopped and its rows still claiming it. This is what stops it.
    for kind in [
        EnvironmentLifecycleKind::Up,
        EnvironmentLifecycleKind::Stop,
        EnvironmentLifecycleKind::Delete,
    ] {
        let error = store
            .begin_environment_lifecycle(
                &environment_id,
                kind,
                "req-other",
                &format!("idem-other-{kind:?}"),
                &format!("sha256:{}", "d".repeat(64)),
                12,
            )
            .unwrap_err();
        assert!(
            format!("{error}").contains("lifecycle operation"),
            "{kind:?} was not refused: {error}"
        );
    }
    // And so does a second fork's reclamation.
    assert!(
        store
            .begin_machine_lifecycle_delete(
                &environment_id,
                second.machine_id(),
                "req-second",
                "idem-second",
                &format!("sha256:{}", "e".repeat(64)),
                12,
            )
            .is_err()
    );
    let during = store
        .load_environment_instance(&environment_id)
        .unwrap()
        .unwrap();
    assert_eq!(during.machines.len(), 3, "nothing was reclaimed");
    assert_eq!(during.lifecycle_generation, operation.generation);

    // Once it finishes, the Environment admits new operations again.
    let acknowledged = acknowledge_every_step(&store, &operation, 13);
    store
        .finish_machine_delete(
            acknowledged.operation_id.as_str(),
            acknowledged.generation,
            14,
        )
        .unwrap();
    let up = store
        .begin_environment_lifecycle(
            &environment_id,
            EnvironmentLifecycleKind::Up,
            "req-after",
            "idem-after",
            &format!("sha256:{}", "f".repeat(64)),
            15,
        )
        .unwrap();
    assert_eq!(up.generation, operation.generation + 1);
    assert_eq!(
        up.machine_steps.len(),
        2,
        "the parent and the surviving fork"
    );
}

#[test]
fn a_machine_scoped_delete_replays_only_for_the_machine_it_named() {
    let (_root, store, environment) = fixture();
    let environment_id = environment.environment_id.to_string();
    let parent = environment.machines[0].machine_id.clone();
    let first = store
        .fork_machine_in_environment(&environment_id, &parent, "feat-x", 9)
        .unwrap();
    let second = store
        .fork_machine_in_environment(&environment_id, &parent, "feat-y", 10)
        .unwrap();
    let hash = format!("sha256:{}", "c".repeat(64));
    let operation = store
        .begin_machine_lifecycle_delete(
            &environment_id,
            first.machine_id(),
            "req-fork-delete",
            "idem-fork-delete",
            &hash,
            11,
        )
        .unwrap();

    // The same immutable request replays to the same journal.
    let replayed = store
        .begin_machine_lifecycle_delete(
            &environment_id,
            first.machine_id(),
            "req-fork-delete",
            "idem-fork-delete",
            &hash,
            12,
        )
        .unwrap();
    assert_eq!(replayed, operation);

    // The same key naming a different Machine is a different request, and
    // answering it with this journal would reclaim the wrong fork.
    assert!(
        store
            .begin_machine_lifecycle_delete(
                &environment_id,
                second.machine_id(),
                "req-fork-delete",
                "idem-fork-delete",
                &hash,
                13,
            )
            .is_err()
    );
    let after = store
        .load_environment_instance(&environment_id)
        .unwrap()
        .unwrap();
    assert_eq!(after.machines.len(), 3);
}

#[test]
fn a_declared_machine_never_opens_a_scoped_delete_and_changes_nothing() {
    let (_root, store, environment) = fixture();
    let environment_id = environment.environment_id.to_string();
    let parent = environment.machines[0].machine_id.clone();
    let generation_before = environment.lifecycle_generation;

    assert!(
        store
            .begin_machine_lifecycle_delete(
                &environment_id,
                &parent,
                "req-declared",
                "idem-declared",
                &format!("sha256:{}", "c".repeat(64)),
                9,
            )
            .is_err()
    );
    let after = store
        .load_environment_instance(&environment_id)
        .unwrap()
        .unwrap();
    assert_eq!(after.machines.len(), 1);
    assert_eq!(after.lifecycle_generation, generation_before);
    let operations: usize = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM environment_lifecycle_operations WHERE environment_id = ?1",
            rusqlite::params![environment_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(operations, 0, "a refused scope must persist no journal");
}

#[test]
fn an_unfinished_scoped_delete_cannot_be_finished_and_removes_nothing() {
    let (_root, store, environment) = fixture();
    let environment_id = environment.environment_id.to_string();
    let parent = environment.machines[0].machine_id.clone();
    let fork = store
        .fork_machine_in_environment(&environment_id, &parent, "feat-x", 9)
        .unwrap();
    let operation = store
        .begin_machine_lifecycle_delete(
            &environment_id,
            fork.machine_id(),
            "req-fork-delete",
            "idem-fork-delete",
            &format!("sha256:{}", "c".repeat(64)),
            10,
        )
        .unwrap();

    // Not every step has succeeded, so nothing is reclaimed and no journal goes
    // terminal: an operation that finished here would be claiming a teardown
    // that never happened.
    assert!(
        store
            .finish_machine_delete(operation.operation_id.as_str(), operation.generation, 11)
            .is_err()
    );
    // A stale generation is refused too.
    assert!(
        store
            .finish_machine_delete(
                operation.operation_id.as_str(),
                operation.generation + 1,
                11
            )
            .is_err()
    );
    let after = store
        .load_environment_instance(&environment_id)
        .unwrap()
        .unwrap();
    assert_eq!(after.machines.len(), 2);
    assert_eq!(
        store
            .load_environment_lifecycle(operation.operation_id.as_str())
            .unwrap()
            .unwrap()
            .status,
        EnvironmentLifecycleStatus::Running
    );
}
