//! The Machine-scoped lifecycle operation that reclaims exactly one fork.
//!
//! Reclaiming one fork needs a persisted operation to fence its effects on, and
//! an Environment-wide one would declare the whole Environment `Deleting` while
//! its declared Machines are still serving. These tests pin the shape that says
//! otherwise: one Machine step, that Machine's ownership, an Environment whose
//! state and active operation are untouched, and a generation that still moves
//! so nothing else can run underneath it.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;

use crate::types::machine_fork::MachineId;
use crate::types::topology::{
    EnvironmentInstance, EnvironmentLifecycleKind, EnvironmentLifecycleOperation,
    EnvironmentLifecycleStatus, EnvironmentState, LifecycleOperationId, LifecycleStepResult,
    LifecycleStepStatus, MachineLifecycleStepAcknowledgement, OwnershipCleanupStepAcknowledgement,
    TopologyLifecycleError,
};

use super::tests::definition;

/// An Environment holding its declared Machine and two forks of it.
fn environment_with_two_forks() -> (EnvironmentInstance, MachineId, MachineId, MachineId) {
    let definition = definition();
    let mut environment = definition.instantiate_environment("agent", 7).unwrap();
    let parent = environment.machines[0].machine_id.clone();
    let first = environment.plan_machine_fork(&parent, "feat-x").unwrap();
    let first_id = first.machine_id().clone();
    first.apply(&mut environment);
    let second = environment.plan_machine_fork(&parent, "feat-y").unwrap();
    let second_id = second.machine_id().clone();
    second.apply(&mut environment);
    environment.validate().unwrap();
    (environment, parent, first_id, second_id)
}

fn scoped_delete(
    environment: &EnvironmentInstance,
    machine_id: &MachineId,
) -> Result<EnvironmentLifecycleOperation, TopologyLifecycleError> {
    EnvironmentLifecycleOperation::plan_machine_delete(
        environment,
        machine_id,
        LifecycleOperationId::generate(),
        "req-fork-delete",
        "idem-fork-delete",
        format!("sha256:{}", "b".repeat(64)),
        11,
    )
}

/// Drive a begun scoped Delete to the point where it may finish.
fn acknowledge_every_step(
    operation: &mut EnvironmentLifecycleOperation,
    environment: &mut EnvironmentInstance,
) {
    for step in operation.machine_steps.clone() {
        operation
            .apply_machine_step_acknowledgement(
                environment,
                &MachineLifecycleStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    machine_id: step.machine_id.clone(),
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: step.expected_incarnation.clone(),
                    resulting_incarnation: None,
                    resulting_activation: None,
                    result: LifecycleStepResult::Succeeded,
                },
                12,
            )
            .unwrap();
    }
    for step in operation.cleanup_steps.clone() {
        operation
            .apply_cleanup_step_acknowledgement(
                environment,
                &OwnershipCleanupStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    ownership: step.ownership.clone(),
                    result: LifecycleStepResult::Succeeded,
                },
                12,
            )
            .unwrap();
    }
}

#[test]
fn a_machine_scoped_delete_plans_one_step_and_only_that_machines_ownership() {
    let (environment, parent, forked, sibling) = environment_with_two_forks();
    let operation = scoped_delete(&environment, &forked).unwrap();

    assert_eq!(operation.machine_scope.as_ref(), Some(&forked));
    assert_eq!(operation.machine_steps.len(), 1);
    assert_eq!(operation.machine_steps[0].machine_id, forked);
    assert_eq!(operation.kind, EnvironmentLifecycleKind::Delete);
    // The Environment is not being deleted, so the operation requests the state
    // it started in rather than `Deleted`.
    assert_eq!(operation.requested_target, environment.state);
    assert_eq!(operation.initial_state, environment.state);
    assert!(!operation.cleanup_steps.is_empty());
    for step in &operation.cleanup_steps {
        assert_eq!(step.ownership.machine_id.as_ref(), Some(&forked));
    }
    // Every record the fork holds, and none of anyone else's.
    let planned: BTreeSet<_> = operation
        .cleanup_steps
        .iter()
        .map(|step| &step.ownership)
        .collect();
    let held: BTreeSet<_> = environment
        .ownership
        .iter()
        .filter(|record| record.machine_id.as_ref() == Some(&forked))
        .collect();
    assert_eq!(planned, held);
    for other in [&parent, &sibling] {
        assert!(
            !operation
                .cleanup_steps
                .iter()
                .any(|step| step.ownership.machine_id.as_ref() == Some(other))
        );
    }
}

#[test]
fn a_declared_machine_can_never_open_a_machine_scoped_delete() {
    let (environment, parent, _, _) = environment_with_two_forks();
    // Subtracting a declared Machine would leave the Environment permanently
    // unable to instantiate its own definition.
    assert!(matches!(
        scoped_delete(&environment, &parent),
        Err(TopologyLifecycleError::InvalidOperation { .. })
    ));
    assert!(matches!(
        scoped_delete(&environment, &MachineId::generate()),
        Err(TopologyLifecycleError::MachineStepNotFound { .. })
    ));
}

#[test]
fn a_scoped_begin_moves_only_the_generation_and_never_attaches() {
    let (mut environment, _, forked, _) = environment_with_two_forks();
    let state_before = environment.state;
    let generation_before = environment.lifecycle_generation;
    let mut operation = scoped_delete(&environment, &forked).unwrap();

    // The Environment-wide begin is refused outright: attaching would move the
    // Environment to `Deleting` while its declared Machine is still serving.
    assert!(matches!(
        operation.clone().begin(&mut environment.clone(), 12),
        Err(TopologyLifecycleError::InvalidOperation { .. })
    ));

    operation.begin_machine_scope(&mut environment, 12).unwrap();
    assert_eq!(operation.status, EnvironmentLifecycleStatus::Running);
    assert_eq!(environment.state, state_before);
    assert_eq!(environment.active_operation_id, None);
    assert_eq!(environment.lifecycle_generation, generation_before + 1);
    assert_eq!(environment.lifecycle_generation, operation.generation);
    assert_eq!(environment.machines.len(), 3, "no Machine left yet");
    environment.validate().unwrap();
    operation
        .validate_against_environment(&environment)
        .unwrap();
    assert!(operation.fences_environment(&environment));

    // And the fence it holds is exactly "this generation, and nobody attached".
    let mut attached = environment.clone();
    attached.state = EnvironmentState::Reconciling;
    attached.active_operation_id = Some(LifecycleOperationId::generate());
    assert!(!operation.fences_environment(&attached));
    let mut advanced = environment.clone();
    advanced.lifecycle_generation += 1;
    assert!(!operation.fences_environment(&advanced));
}

#[test]
fn a_scoped_delete_finishes_without_a_tombstone_and_leaves_the_environment_where_it_was() {
    let (mut environment, _, forked, _) = environment_with_two_forks();
    let state_before = environment.state;
    let mut operation = scoped_delete(&environment, &forked).unwrap();
    operation.begin_machine_scope(&mut environment, 12).unwrap();

    // Incomplete is incomplete: nothing terminal before every step succeeds.
    assert!(matches!(
        operation.clone().finish_machine_delete(&environment, 13),
        Err(TopologyLifecycleError::OperationIncomplete { .. })
    ));

    acknowledge_every_step(&mut operation, &mut environment);
    // The Environment-wide finish refuses a scoped operation: a tombstone
    // retires an Environment identity, and this one was never retired.
    assert!(matches!(
        operation.clone().finish_delete(&environment, 13),
        Err(TopologyLifecycleError::DeleteRequired { .. })
    ));
    assert_eq!(operation.final_environment_state().unwrap(), state_before);

    operation.finish_machine_delete(&environment, 13).unwrap();
    assert_eq!(operation.status, EnvironmentLifecycleStatus::Succeeded);
    assert!(operation.completed_at.is_some());
    assert_eq!(environment.state, state_before);
    assert_eq!(environment.active_operation_id, None);
    operation.validate_structure().unwrap();
}

#[test]
fn a_scoped_operation_is_refused_unless_it_is_exactly_one_machines_teardown() {
    let (environment, parent, forked, sibling) = environment_with_two_forks();
    let valid = scoped_delete(&environment, &forked).unwrap();

    // A second Machine step.
    let mut two_steps = valid.clone();
    let mut extra = two_steps.machine_steps[0].clone();
    extra.machine_id = sibling.clone();
    extra.expected_incarnation = None;
    two_steps.machine_steps.push(extra);
    two_steps
        .machine_steps
        .sort_by(|left, right| left.machine_id.cmp(&right.machine_id));
    assert!(two_steps.validate_structure().is_err());

    // One step, for the wrong Machine.
    let mut wrong_step = valid.clone();
    wrong_step.machine_steps[0].machine_id = parent.clone();
    assert!(wrong_step.validate_structure().is_err());

    // Ownership belonging to a sibling, or to the Environment itself.
    let mut foreign = valid.clone();
    foreign.cleanup_steps[0].ownership.machine_id = Some(sibling.clone());
    assert!(foreign.validate_structure().is_err());
    let mut environment_scoped = valid.clone();
    environment_scoped.cleanup_steps[0].ownership.machine_id = None;
    assert!(environment_scoped.validate_structure().is_err());

    // A scope on anything but a Delete, and a target the Environment never asked
    // for: a scoped operation ends the Environment where it began it.
    let mut scoped_up = valid.clone();
    scoped_up.kind = EnvironmentLifecycleKind::Up;
    assert!(scoped_up.validate_structure().is_err());
    let mut deleted_target = valid.clone();
    deleted_target.requested_target = EnvironmentState::Deleted;
    assert!(deleted_target.validate_structure().is_err());
    valid.validate_structure().unwrap();
}

#[test]
fn a_scoped_operation_never_validates_against_an_environment_running_another_one() {
    let (mut environment, _, forked, _) = environment_with_two_forks();
    let mut operation = scoped_delete(&environment, &forked).unwrap();
    operation.begin_machine_scope(&mut environment, 12).unwrap();

    // Someone else took the Environment: this operation's effects are no longer
    // fenced, and every acknowledgement it makes must fail rather than land.
    let mut taken = environment.clone();
    taken.state = EnvironmentState::Reconciling;
    taken.lifecycle_generation += 1;
    taken.active_operation_id = Some(LifecycleOperationId::generate());
    assert!(operation.validate_against_environment(&taken).is_err());
    let step = operation.machine_steps[0].clone();
    assert!(
        operation
            .clone()
            .apply_machine_step_acknowledgement(
                &mut taken,
                &MachineLifecycleStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    machine_id: step.machine_id.clone(),
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: step.expected_incarnation.clone(),
                    resulting_incarnation: None,
                    resulting_activation: None,
                    result: LifecycleStepResult::Succeeded,
                },
                13,
            )
            .is_err()
    );
    // And the Machine it names must still be there.
    let mut without_fork = environment.clone();
    without_fork
        .machines
        .retain(|machine| machine.machine_id != forked);
    without_fork
        .network_attachments
        .retain(|attachment| attachment.machine_id != forked);
    without_fork
        .egress
        .retain(|egress| egress.machine_id != forked);
    without_fork
        .ownership
        .retain(|record| record.machine_id.as_ref() != Some(&forked));
    assert!(matches!(
        operation.validate_against_environment(&without_fork),
        Err(TopologyLifecycleError::MachineStepNotFound { .. })
    ));
    assert_eq!(
        operation.machine_steps[0].status,
        LifecycleStepStatus::Pending
    );
}
