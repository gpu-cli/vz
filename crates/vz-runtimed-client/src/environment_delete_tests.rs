//! Offline protocol tests. Fixtures use the real aggregate planner and exact acknowledgements.
#![allow(clippy::unwrap_used)]

use super::*;
use vz_runtime_contract::*;

const MACHINE_TIMEOUT_MILLIS: u64 = 60_000;

fn selection() -> EnvironmentSelectionContext {
    EnvironmentSelectionContext {
        explicit: Some(EnvironmentSelector::NameOrId("test".into())),
        ..Default::default()
    }
}

fn running_delete() -> (EnvironmentInstance, EnvironmentLifecycleOperation) {
    let machines = ["alpha", "beta"].map(|name| {
        serde_json::json!({
            "schema_version": 1, "name": name, "profile": "developer",
            "target": {"os": "linux", "arch": "aarch64", "image": "vz-linux-appliance"},
            "resources": {"cpus": 2, "memory_mb": 4096}
        })
    });
    let definition: ProjectDefinition = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "project_id": ProjectId::generate(),
        "name": "delete-validator",
        "environment": {
            "schema_version": 1,
            "machines": machines
        }
    }))
    .unwrap();
    let mut environment = definition.instantiate_environment("test", 100).unwrap();
    let mut operation = EnvironmentLifecycleOperation::plan(
        &environment,
        LifecycleOperationId::generate(),
        EnvironmentLifecycleKind::Delete,
        "delete-request",
        "delete-idempotency",
        environment_delete_request_hash(
            &environment.project_id,
            &environment.environment_id,
            &selection(),
            u128::from(MACHINE_TIMEOUT_MILLIS),
        )
        .unwrap(),
        101,
    )
    .unwrap();
    operation.begin(&mut environment, 102).unwrap();
    (environment, operation)
}

fn acknowledge_machines(
    environment: &mut EnvironmentInstance,
    operation: &mut EnvironmentLifecycleOperation,
) {
    for step in operation.machine_steps.clone() {
        operation
            .apply_machine_step_acknowledgement(
                environment,
                &MachineLifecycleStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    machine_id: step.machine_id,
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: step.expected_incarnation,
                    resulting_incarnation: None,
                    resulting_activation: None,
                    result: LifecycleStepResult::Succeeded,
                },
                103,
            )
            .unwrap();
    }
}

fn acknowledge_cleanup(
    environment: &EnvironmentInstance,
    operation: &mut EnvironmentLifecycleOperation,
    fail: bool,
) {
    for (index, step) in operation.cleanup_steps.clone().into_iter().enumerate() {
        operation
            .apply_cleanup_step_acknowledgement(
                environment,
                &OwnershipCleanupStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    ownership: step.ownership,
                    result: if fail && index == 0 {
                        LifecycleStepResult::Failed {
                            reason: "exact owned context removal is unproven".into(),
                        }
                    } else {
                        LifecycleStepResult::Succeeded
                    },
                },
                104,
            )
            .unwrap();
    }
}

fn successful_delete() -> (
    EnvironmentLifecycleOperation,
    EnvironmentLifecycleOperation,
    EnvironmentTombstone,
) {
    let (mut environment, mut operation) = running_delete();
    let initial = operation.clone();
    acknowledge_machines(&mut environment, &mut operation);
    acknowledge_cleanup(&environment, &mut operation, false);
    let tombstone = operation.finish_delete(&environment, 105).unwrap();
    tombstone.validate_for_operation(&operation).unwrap();
    (initial, operation, tombstone)
}

fn validator(operation: &EnvironmentLifecycleOperation) -> DeleteValidator {
    DeleteValidator {
        project_id: operation.project_id.clone(),
        request_id: operation.request_id.clone(),
        idempotency_key: operation.idempotency_key.clone(),
        expected_environment: None,
        selection: selection(),
        machine_timeout_millis: MACHINE_TIMEOUT_MILLIS,
        scope: None,
        last_sequence: None,
        terminal: false,
    }
}

fn frame(
    operation: &EnvironmentLifecycleOperation,
    sequence: u64,
) -> runtime_v2::DeleteEnvironmentEvent {
    operation.validate_structure().unwrap();
    runtime_v2::DeleteEnvironmentEvent {
        schema_version: 1,
        request_id: operation.request_id.clone(),
        sequence,
        operation: Some(vz_runtime_translate::environment_lifecycle_operation_to_proto(operation)),
        terminal: false,
        error: None,
        tombstone: None,
    }
}

fn success_frame(
    operation: &EnvironmentLifecycleOperation,
    tombstone: &EnvironmentTombstone,
    sequence: u64,
) -> runtime_v2::DeleteEnvironmentEvent {
    let mut wire = frame(operation, sequence);
    wire.terminal = true;
    wire.tombstone = Some(vz_runtime_translate::environment_tombstone_to_proto(
        tombstone,
    ));
    wire
}

fn correlated_error(operation: &EnvironmentLifecycleOperation) -> runtime_v2::ErrorDetail {
    vz_runtime_translate::machine_error_to_proto_detail(&MachineError::new(
        MachineErrorCode::BackendUnavailable,
        "owned context removal is unproven".into(),
        Some(operation.request_id.clone()),
        Default::default(),
    ))
}

fn assert_rejected(validator: &mut DeleteValidator, wire: runtime_v2::DeleteEnvironmentEvent) {
    assert!(matches!(
        validator.event(wire),
        Err(DaemonClientError::IncompatibleProtocol { .. })
    ));
}

#[test]
fn coalesced_sequence_gaps_and_real_progress_preserve_scope() {
    let (mut environment, mut operation) = running_delete();
    let mut validation = validator(&operation);
    validation.event(frame(&operation, 17)).unwrap();
    acknowledge_machines(&mut environment, &mut operation);
    validation.event(frame(&operation, 41)).unwrap();
    acknowledge_cleanup(&environment, &mut operation, false);
    validation.event(frame(&operation, 900)).unwrap();
    let tombstone = operation.finish_delete(&environment, 105).unwrap();
    let event = validation
        .event(success_frame(&operation, &tombstone, 999))
        .unwrap();
    assert_eq!(event.operation, operation);
    assert_eq!(event.tombstone, Some(tombstone));
    assert!(event.terminal);
    assert!(event.error.is_none());
    assert_eq!(event.request_id, operation.request_id);
    assert_eq!(event.sequence, 999);
}

#[test]
fn first_observation_may_already_be_a_complete_coalesced_receipt() {
    let (initial, operation, tombstone) = successful_delete();
    let mut validation = validator(&initial);
    assert!(
        validation
            .event(success_frame(&operation, &tombstone, 29))
            .unwrap()
            .terminal
    );
    for sequence in [0, 29, 30, u64::MAX] {
        assert_rejected(
            &mut validation,
            success_frame(&operation, &tombstone, sequence),
        );
    }
}

#[test]
fn duplicate_and_backwards_sequence_are_rejected_without_advancing_state() {
    let (_, operation) = running_delete();
    let mut validation = validator(&operation);
    validation.event(frame(&operation, 20)).unwrap();
    for sequence in [20, 19, 0] {
        assert_rejected(&mut validation, frame(&operation, sequence));
        assert_eq!(validation.last_sequence, Some(20));
        assert!(!validation.terminal);
    }
    validation.event(frame(&operation, 21)).unwrap();
}

#[test]
fn first_frame_requires_exact_request_project_idempotency_and_schema() {
    let (_, operation) = running_delete();
    for case in 0..6 {
        let mut wire = frame(&operation, 0);
        match case {
            0 => wire.schema_version = 2,
            1 => wire.request_id = "other-request".into(),
            2 => wire.operation.as_mut().unwrap().request_id = "other-request".into(),
            3 => wire.operation.as_mut().unwrap().idempotency_key = "other-idempotency".into(),
            4 => wire.operation.as_mut().unwrap().project_id = ProjectId::generate().to_string(),
            5 => wire.operation = None,
            _ => unreachable!(),
        }
        let mut validation = validator(&operation);
        assert_rejected(&mut validation, wire);
        assert!(validation.scope.is_none());
        assert!(validation.last_sequence.is_none());
        validation.event(frame(&operation, 0)).unwrap();
    }
}

#[test]
fn first_frame_rejects_other_canonical_selector_timeout_or_workspace_hash() {
    let (_, operation) = running_delete();
    for case in 0..4 {
        let mut changed = operation.clone();
        let mut other_selection = selection();
        let mut other_timeout = MACHINE_TIMEOUT_MILLIS;
        match case {
            0 => changed.request_hash = format!("sha256:{}", "f".repeat(64)),
            1 => other_selection.explicit = Some(EnvironmentSelector::NameOrId("other".into())),
            2 => other_timeout += 1,
            3 => other_selection.workspace_key = Some("foreign-worktree-token".into()),
            _ => unreachable!(),
        }
        if case != 0 {
            changed.request_hash = environment_delete_request_hash(
                &changed.project_id,
                &changed.environment_id,
                &other_selection,
                u128::from(other_timeout),
            )
            .unwrap();
        }
        changed.validate_structure().unwrap();
        let mut validation = validator(&operation);
        assert_rejected(&mut validation, frame(&changed, 0));
        assert!(validation.scope.is_none() && validation.last_sequence.is_none());
        validation.event(frame(&operation, 0)).unwrap();
    }
}

#[test]
fn first_terminal_frame_cannot_bypass_exact_outbound_timeout() {
    let (initial, operation, tombstone) = successful_delete();
    let mut validation = validator(&initial);
    validation.machine_timeout_millis += 1;
    assert_rejected(&mut validation, success_frame(&operation, &tombstone, 1));
    assert!(validation.scope.is_none() && !validation.terminal);
    validation.machine_timeout_millis = MACHINE_TIMEOUT_MILLIS;
    validation
        .event(success_frame(&operation, &tombstone, 1))
        .unwrap();
}

#[test]
fn outbound_selection_normalization_keeps_name_or_id_and_original_workspace() {
    let mut request = runtime_v2::DeleteEnvironmentRequest {
        metadata: None,
        project_id: ProjectId::generate().to_string(),
        environment: Some("env_looks_like_id_but_is_a_name".into()),
        process_environment_id: Some("ignored invalid process ID".into()),
        workspace_key: Some("original-worktree-token".into()),
        machine_timeout_millis: MACHINE_TIMEOUT_MILLIS,
    };
    let normalized = request_selection(&request).unwrap();
    assert_eq!(
        normalized.explicit,
        Some(EnvironmentSelector::NameOrId(
            "env_looks_like_id_but_is_a_name".into()
        ))
    );
    assert!(normalized.process_environment_id.is_none());
    assert_eq!(normalized.workspace_key, request.workspace_key);
    let (_, mut operation) = running_delete();
    let mut validation = validator(&operation);
    validation.selection = normalized;
    operation.request_hash = environment_delete_request_hash(
        &operation.project_id,
        &operation.environment_id,
        &validation.selection,
        u128::from(MACHINE_TIMEOUT_MILLIS),
    )
    .unwrap();
    validation.event(frame(&operation, 0)).unwrap();
    request.environment = None;
    assert!(request_selection(&request).is_err());
    let exact = EnvironmentId::generate();
    request.process_environment_id = Some(exact.to_string());
    assert_eq!(
        request_selection(&request).unwrap().process_environment_id,
        Some(exact)
    );
}

#[test]
fn explicit_process_environment_rejects_foreign_first_frame() {
    let (_, operation) = running_delete();
    let mut validation = validator(&operation);
    validation.expected_environment = Some(EnvironmentId::generate());
    assert_rejected(&mut validation, frame(&operation, 0));
    validation.expected_environment = Some(operation.environment_id.clone());
    validation.event(frame(&operation, 0)).unwrap();
}

#[test]
fn structurally_valid_other_lifecycle_kind_is_not_a_delete_receipt() {
    let (environment, operation) = running_delete();
    let mut source = environment;
    source.state = EnvironmentState::Creating;
    source.active_operation_id = None;
    let other = EnvironmentLifecycleOperation::plan(
        &source,
        LifecycleOperationId::generate(),
        EnvironmentLifecycleKind::Up,
        &operation.request_id,
        &operation.idempotency_key,
        &operation.request_hash,
        103,
    )
    .unwrap();
    assert_rejected(&mut validator(&operation), frame(&other, 0));
}

#[test]
fn later_frames_cannot_change_operation_or_exact_ownership_plan() {
    let (_, original) = running_delete();
    for case in 0..10 {
        let mut changed = original.clone();
        match case {
            0 => changed.operation_id = LifecycleOperationId::generate(),
            1 => changed.generation += 1,
            2 => changed.request_hash.push_str("-foreign"),
            3 => changed.definition_digest.push_str("-foreign"),
            4 => changed.created_at -= 1,
            5 => changed.machine_steps[0].initial_state = MachineState::Failed,
            6 => changed
                .cleanup_steps
                .last_mut()
                .unwrap()
                .ownership
                .resource_id
                .push_str("-foreign"),
            7 => changed.cleanup_steps.pop().map(|_| ()).unwrap(),
            8 => {
                changed.cleanup_steps[0].ownership.machine_id =
                    Some(changed.machine_steps[1].machine_id.clone())
            }
            9 => {
                changed.environment_id = EnvironmentId::generate();
                for step in &mut changed.cleanup_steps {
                    step.ownership.environment_id = changed.environment_id.clone();
                }
            }
            _ => unreachable!(),
        }
        // These mutations are individually valid contracts: rejection must bind
        // the original observation, not merely reject malformed protobuf data.
        changed.validate_structure().unwrap();
        let mut validation = validator(&original);
        validation.event(frame(&original, 3)).unwrap();
        assert_rejected(&mut validation, frame(&changed, 4));
        assert_eq!(validation.last_sequence, Some(3));
    }
}

#[test]
fn success_requires_exact_tombstone_and_terminal_without_error() {
    let (initial, operation, tombstone) = successful_delete();
    for case in 0..3 {
        let mut wire = success_frame(&operation, &tombstone, 8);
        match case {
            0 => wire.tombstone = None,
            1 => wire.terminal = false,
            2 => wire.error = Some(correlated_error(&operation)),
            _ => unreachable!(),
        }
        let mut validation = validator(&initial);
        validation.event(frame(&initial, 0)).unwrap();
        assert_rejected(&mut validation, wire);
        assert!(!validation.terminal);
        validation
            .event(success_frame(&operation, &tombstone, 8))
            .unwrap();
    }
}

#[test]
fn tombstone_foreign_scope_digest_and_completion_timestamp_are_rejected() {
    let (initial, operation, tombstone) = successful_delete();
    for case in 0..10 {
        let mut changed = tombstone.clone();
        match case {
            0 => changed.project_id = ProjectId::generate(),
            1 => changed.environment_id = EnvironmentId::generate(),
            2 => changed.delete_operation_id = LifecycleOperationId::generate(),
            3 => changed.lifecycle_generation += 1,
            4 => changed.definition_digest.push_str("-foreign"),
            5 => changed.ownership_digest = format!("sha256:{}", "0".repeat(64)),
            6 => changed.deleted_at -= 1,
            7 => changed.deleted_at += 1,
            8 => changed.deleted_at = 0,
            9 => changed.schema_version = 2,
            _ => unreachable!(),
        }
        assert_rejected(
            &mut validator(&initial),
            success_frame(&operation, &changed, 1),
        );
    }
}

#[test]
fn running_operation_cannot_claim_terminal_error_or_tombstone() {
    let (initial, _, tombstone) = successful_delete();
    for case in 0..3 {
        let mut wire = frame(&initial, 0);
        match case {
            0 => wire.terminal = true,
            1 => wire.error = Some(correlated_error(&initial)),
            2 => {
                wire.tombstone = Some(vz_runtime_translate::environment_tombstone_to_proto(
                    &tombstone,
                ))
            }
            _ => unreachable!(),
        }
        assert_rejected(&mut validator(&initial), wire);
    }
}

#[test]
fn blocked_delete_is_a_correlated_failed_observation_not_a_tombstone() {
    let (mut environment, mut operation) = running_delete();
    let initial = operation.clone();
    acknowledge_machines(&mut environment, &mut operation);
    acknowledge_cleanup(&environment, &mut operation, true);
    assert_eq!(operation.status, EnvironmentLifecycleStatus::Blocked);
    assert!(operation.completed_at.is_none());
    let mut wire = frame(&operation, 40);
    wire.terminal = true;
    wire.error = Some(correlated_error(&operation));
    let mut validation = validator(&initial);
    validation.event(frame(&initial, 0)).unwrap();
    let event = validation.event(wire.clone()).unwrap();
    assert_eq!(event.operation, operation);
    assert_eq!(
        event.error.unwrap().request_id.as_deref(),
        Some(initial.request_id.as_str())
    );
    assert!(event.tombstone.is_none());
    assert!(event.terminal);
    assert_rejected(&mut validation, wire);
}

#[test]
fn blocked_terminal_requires_exact_error_correlation_and_no_tombstone() {
    let (mut environment, mut operation) = running_delete();
    let initial = operation.clone();
    acknowledge_machines(&mut environment, &mut operation);
    acknowledge_cleanup(&environment, &mut operation, true);
    let (_, _, tombstone) = successful_delete();
    for case in 0..5 {
        let mut wire = frame(&operation, 20);
        wire.terminal = true;
        wire.error = Some(correlated_error(&operation));
        match case {
            0 => wire.error = None,
            1 => wire.error.as_mut().unwrap().request_id = "other-request".into(),
            2 => wire.error.as_mut().unwrap().request_id.clear(),
            3 => wire.terminal = false,
            4 => {
                wire.tombstone = Some(vz_runtime_translate::environment_tombstone_to_proto(
                    &tombstone,
                ))
            }
            _ => unreachable!(),
        }
        assert_rejected(&mut validator(&initial), wire);
    }
}
