#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use vz_runtime_contract::{
    EnvironmentLifecycleKind, EnvironmentSelectionContext, LifecycleStepResult, MachineIncarnation,
    MachineIncarnationId, MachineLifecycleStepAcknowledgement, MachineState, OwnedResourceKind,
    ProjectDefinition, ProjectId, TOPOLOGY_SCHEMA_VERSION,
};

/// One Linux Machine on one private network, reachable, admitted through the
/// ordinary Up reservation so the rows under test were written the way
/// production writes them.
pub(super) fn fixture() -> (tempfile::TempDir, StateStore, EnvironmentInstance) {
    let root = tempfile::tempdir().unwrap();
    let store = StateStore::open(&root.path().join("state.db")).unwrap();
    let definition: ProjectDefinition = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "project_id": ProjectId::generate(),
        "name": "fork",
        "environment": {
            "schema_version": 1,
            "machines": [{
                "schema_version": 1,
                "name": "backend",
                "profile": "developer",
                "target": {"os": "linux", "arch": "aarch64", "image": "vz-linux-appliance"},
                "networks": ["private"],
                "egress": "allowed"
            }],
            "networks": [{
                "schema_version": 1, "name": "private", "kind": "private",
                "cidr": "10.42.0.0/24"
            }]
        }
    }))
    .unwrap();
    let admission = store
        .reserve_environment_up_admission(
            &definition,
            &EnvironmentSelectionContext {
                workspace_key: Some("worktree-opaque".into()),
                ..Default::default()
            },
            "req-up",
            "idem-up",
            &format!("sha256:{}", "a".repeat(64)),
            1,
            |_| Ok(()),
        )
        .unwrap();
    let environment = store
        .load_environment_instance(admission.environment_id.as_str())
        .unwrap()
        .unwrap();
    (root, store, environment)
}

/// The same fixture, driven all the way to `Ready` the way Up drives it.
///
/// This is the state a fork is actually taken in and the one the other fixtures
/// never reach: `instantiate_environment` yields a Creating Environment whose
/// Machines are all Creating, so every fork test that starts there exercises the
/// one lifecycle state nobody forks from. A warm parent -- the only kind worth
/// forking -- is by definition in a Ready Environment.
pub(super) fn ready_fixture() -> (tempfile::TempDir, StateStore, EnvironmentInstance) {
    let (root, store, environment) = fixture();
    let up = store
        .begin_environment_lifecycle(
            environment.environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "req-ready",
            "idem-ready",
            "sha256:ready",
            10,
        )
        .unwrap();
    for step in up.machine_steps.clone() {
        // A never-booted Machine has no incarnation to expect; a successful Up
        // is exactly the event that mints its first one, which is why the step
        // carries `expected_incarnation: None` and the acknowledgement supplies
        // the result the backend produced.
        let incarnation = MachineIncarnation {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            incarnation_id: MachineIncarnationId::new("inc_ready_fixture_up").unwrap(),
            machine_id: step.machine_id.clone(),
            generation: up.generation,
            created_at: 10,
        };
        store
            .acknowledge_environment_machine_step(
                &MachineLifecycleStepAcknowledgement {
                    operation_id: up.operation_id.clone(),
                    generation: up.generation,
                    machine_id: step.machine_id,
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: step.expected_incarnation.clone(),
                    resulting_incarnation: Some(incarnation.clone()),
                    resulting_activation: Some(crate::state_store::tests::test_activation(
                        incarnation,
                    )),
                    result: LifecycleStepResult::Succeeded,
                },
                10,
            )
            .unwrap();
    }
    store
        .finish_environment_lifecycle(up.operation_id.as_str(), up.generation, 10)
        .unwrap();
    let environment = store
        .load_environment_instance(environment.environment_id.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(environment.state, EnvironmentState::Ready);
    (root, store, environment)
}

#[test]
fn a_warm_ready_environment_can_be_forked() {
    // The gate found `vz up --fork-from` refusing every Environment that had
    // finished coming up: `Ready` demanded that EVERY Machine be Ready, and a
    // fork is minted Creating because it has not booted. Forking was therefore
    // impossible in the only state anyone forks from, and 3248 unit tests
    // agreed it worked, because they all forked a Creating Environment.
    let (_root, store, environment) = ready_fixture();
    let parent = environment.machines[0].machine_id.clone();
    let plan = store
        .fork_machine_in_environment(environment.environment_id.as_str(), &parent, "feature", 11)
        .unwrap();
    assert_eq!(plan.machine.state, MachineState::Creating);
    assert_eq!(plan.machine.name, "backend@feature");

    // The Environment stays Ready while the fork boots. That is the point: a
    // fork is a runtime object, absent from the definition, and sibling
    // worktrees must not see the shared Environment leave Ready because someone
    // else took a copy.
    let after = store
        .load_environment_instance(environment.environment_id.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(after.state, EnvironmentState::Ready);
    assert_eq!(
        forks_in(&store, environment.environment_id.as_str()),
        vec!["backend@feature"]
    );
}

#[test]
fn a_declared_machine_that_is_not_ready_still_refuses_ready() {
    // The exemption is exactly and only for forks. Scoping the invariant to
    // declared Machines must not turn it off: `Ready` is still a claim that
    // every Machine the project declared is up, and that claim is what makes
    // Ready worth reporting at all.
    let (_root, _store, environment) = ready_fixture();
    let mut broken = environment.clone();
    broken.machines[0].state = MachineState::Creating;
    let error = broken.validate().unwrap_err().to_string();
    assert!(
        error.contains("Ready requires every declared Machine to be Ready"),
        "{error}"
    );
}

fn forks_in(store: &StateStore, environment_id: &str) -> Vec<String> {
    let environment = store
        .load_environment_instance(environment_id)
        .unwrap()
        .unwrap();
    let mut names: Vec<_> = environment
        .machines
        .iter()
        .filter(|machine| machine.fork.is_some())
        .map(|machine| machine.name.clone())
        .collect();
    names.sort();
    names
}

#[test]
fn forking_adds_one_machine_with_its_own_rows_and_leaves_the_parent_untouched() {
    let (_root, store, environment) = fixture();
    let parent = environment.machines[0].machine_id.clone();
    let parent_attachment = environment.network_attachments[0].clone();

    let plan = store
        .fork_machine_in_environment(environment.environment_id.as_str(), &parent, "feat-x", 9)
        .unwrap();
    let forked = plan.machine_id().clone();

    let after = store
        .load_environment_instance(environment.environment_id.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(after.machines.len(), 2);
    assert_eq!(
        forks_in(&store, environment.environment_id.as_str()),
        vec!["backend@feat-x".to_string()]
    );

    // The parent is provably unaffected: same identity, same attachment, same
    // address inputs. A fork that moved its parent's address would break every
    // sibling that had already resolved it.
    let parent_after = after
        .machines
        .iter()
        .find(|machine| machine.machine_id == parent)
        .unwrap();
    assert_eq!(parent_after.name, "backend");
    assert!(parent_after.fork.is_none());
    assert!(after.network_attachments.contains(&parent_attachment));

    // The fork's attachment is new, on the same network. This is what re-derives
    // its address without an allocator: `assign_host_offset` keys on
    // `[environment_id, network_id, attachment_id]`.
    let fork_attachment = after
        .network_attachments
        .iter()
        .find(|attachment| attachment.machine_id == forked)
        .unwrap();
    assert_eq!(fork_attachment.network_id, parent_attachment.network_id);
    assert_ne!(
        fork_attachment.attachment_id,
        parent_attachment.attachment_id
    );

    // Rows, not just the snapshot: the projections a query would read.
    let rows: usize = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM machine_instances WHERE environment_id = ?1",
            rusqlite::params![environment.environment_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 2);
    let owned: usize = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM topology_ownership WHERE machine_id = ?1",
            rusqlite::params![forked.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(owned, plan.ownership.len());
}

#[test]
fn a_second_fork_of_the_same_parent_is_a_sibling_with_a_different_address_input() {
    let (_root, store, environment) = fixture();
    let parent = environment.machines[0].machine_id.clone();
    let first = store
        .fork_machine_in_environment(environment.environment_id.as_str(), &parent, "feat-x", 9)
        .unwrap();
    let second = store
        .fork_machine_in_environment(environment.environment_id.as_str(), &parent, "feat-y", 10)
        .unwrap();

    assert_ne!(first.machine_id(), second.machine_id());
    assert_ne!(
        first.network_attachments[0].attachment_id,
        second.network_attachments[0].attachment_id
    );
    assert_eq!(
        first.network_attachments[0].network_id,
        second.network_attachments[0].network_id
    );
    assert_eq!(
        forks_in(&store, environment.environment_id.as_str()),
        vec!["backend@feat-x".to_string(), "backend@feat-y".to_string()]
    );

    // The same label twice is refused rather than made unique: an agent that
    // asked for `backend@feat-x` must get the one it named, never a second copy.
    assert!(
        store
            .fork_machine_in_environment(environment.environment_id.as_str(), &parent, "feat-x", 11)
            .is_err()
    );
}

#[test]
fn deleting_one_fork_reclaims_exactly_it_and_never_a_sibling_or_its_parent() {
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
    let released: Vec<_> = before
        .ownership
        .iter()
        .filter(|record| record.machine_id.as_ref() == Some(doomed.machine_id()))
        .cloned()
        .collect();
    let after = store
        .delete_exact_machine_fork(&environment_id, doomed.machine_id(), &released, 11)
        .unwrap();

    assert_eq!(after.machines.len(), 2);
    assert!(
        after
            .machines
            .iter()
            .any(|machine| machine.machine_id == parent)
    );
    assert!(
        after
            .machines
            .iter()
            .any(|machine| machine.machine_id == *survivor.machine_id())
    );
    // The sibling keeps its attachment, and therefore its address.
    assert!(after.network_attachments.iter().any(
        |attachment| attachment.attachment_id == survivor.network_attachments[0].attachment_id
    ));

    for (table, column) in [
        ("machine_instances", "machine_id"),
        ("environment_network_attachments", "machine_id"),
        ("environment_machine_egress", "machine_id"),
        ("topology_ownership", "machine_id"),
    ] {
        let remaining: usize = store
            .conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE {column} = ?1"),
                rusqlite::params![doomed.machine_id().as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 0, "`{table}` still holds the deleted fork");
    }
    // And the survivor's rows are all still there, which is the half a cascade
    // gone wrong would quietly break.
    let survivor_rows: usize = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM topology_ownership WHERE machine_id = ?1",
            rusqlite::params![survivor.machine_id().as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(survivor_rows, survivor.ownership.len());
}

#[test]
fn a_declared_machine_is_never_deletable_on_its_own() {
    let (_root, store, environment) = fixture();
    let environment_id = environment.environment_id.to_string();
    let parent = environment.machines[0].machine_id.clone();
    let released: Vec<_> = environment
        .ownership
        .iter()
        .filter(|record| record.machine_id.as_ref() == Some(&parent))
        .cloned()
        .collect();

    // `vz delete --machine backend` on a declared Machine would leave the
    // Environment permanently unable to instantiate its own definition.
    let error = store
        .delete_exact_machine_fork(&environment_id, &parent, &released, 9)
        .unwrap_err();
    assert!(matches!(
        error,
        StackError::Machine {
            code: MachineErrorCode::UnsupportedOperation,
            ..
        }
    ));
    assert_eq!(
        store
            .load_environment_instance(&environment_id)
            .unwrap()
            .unwrap()
            .machines
            .len(),
        1
    );
}

#[test]
fn a_fork_delete_that_does_not_match_the_released_set_exactly_removes_nothing() {
    let (_root, store, environment) = fixture();
    let environment_id = environment.environment_id.to_string();
    let parent = environment.machines[0].machine_id.clone();
    let fork = store
        .fork_machine_in_environment(&environment_id, &parent, "feat-x", 9)
        .unwrap();

    let before = store
        .load_environment_instance(&environment_id)
        .unwrap()
        .unwrap();
    let mut short: Vec<_> = before
        .ownership
        .iter()
        .filter(|record| record.machine_id.as_ref() == Some(fork.machine_id()))
        .cloned()
        .collect();
    // Dropping the fork record from the released set means the caller has not
    // proved it released everything. The store refuses rather than removing what
    // it was told about and leaking the rest.
    short.retain(|record| record.resource_kind != OwnedResourceKind::MachineFork);
    assert!(
        store
            .delete_exact_machine_fork(&environment_id, fork.machine_id(), &short, 10)
            .is_err()
    );
    assert_eq!(
        store
            .load_environment_instance(&environment_id)
            .unwrap()
            .unwrap()
            .machines
            .len(),
        2,
        "a refused fork delete must remove nothing"
    );
}
