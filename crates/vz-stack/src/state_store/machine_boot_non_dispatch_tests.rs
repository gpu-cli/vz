#![allow(clippy::unwrap_used)]
use super::*;
use vz_runtime_contract::*;

fn fixture() -> (tempfile::TempDir, StateStore, EnvironmentLifecycleOperation) {
    let directory = tempfile::tempdir().unwrap();
    let store = StateStore::open(&directory.path().join("state.db")).unwrap();
    let definition: ProjectDefinition = serde_json::from_value(serde_json::json!({
        "schema_version": 1, "project_id": ProjectId::generate(), "name": "boot-proof",
        "environment": {"schema_version": 1, "machines": [
            {"schema_version": 1,"name":"a","profile":"hardened","target":{"os":"linux","arch":"aarch64","image":"fixture"}},
            {"schema_version": 1,"name":"b","profile":"hardened","target":{"os":"linux","arch":"aarch64","image":"fixture"}}
        ]}
    })).unwrap();
    let admission = store
        .reserve_environment_up_admission(
            &definition,
            &EnvironmentSelectionContext::default(),
            "request-1",
            "key-1",
            &hash(),
            1,
            |_| Ok(()),
        )
        .unwrap();
    let operation = begin(
        &store,
        &admission.environment_id,
        EnvironmentLifecycleKind::Up,
        1,
    );
    (directory, store, operation)
}

fn hash() -> String {
    format!("sha256:{}", "a".repeat(64))
}
fn begin(
    store: &StateStore,
    environment: &EnvironmentId,
    kind: EnvironmentLifecycleKind,
    n: u64,
) -> EnvironmentLifecycleOperation {
    store
        .begin_environment_lifecycle(
            environment.as_str(),
            kind,
            &format!("request-{n}"),
            &format!("key-{n}"),
            &hash(),
            n + 1,
        )
        .unwrap()
}
fn environment(
    store: &StateStore,
    operation: &EnvironmentLifecycleOperation,
) -> EnvironmentInstance {
    store
        .load_environment_instance(operation.environment_id.as_str())
        .unwrap()
        .unwrap()
}
fn acknowledge(
    store: &StateStore,
    operation: &EnvironmentLifecycleOperation,
    machine: &MachineId,
    result: LifecycleStepResult,
) {
    let step = step(operation, machine).unwrap();
    store
        .acknowledge_environment_machine_step(
            &MachineLifecycleStepAcknowledgement {
                operation_id: operation.operation_id.clone(),
                generation: operation.generation,
                machine_id: machine.clone(),
                initial_state: step.initial_state,
                target_state: step.target_state,
                expected_incarnation: step.expected_incarnation.clone(),
                resulting_incarnation: None,
                resulting_activation: None,
                result,
            },
            10,
        )
        .unwrap();
}
fn fail(store: &StateStore, operation: &EnvironmentLifecycleOperation) {
    for step in &operation.machine_steps {
        acknowledge(
            store,
            operation,
            &step.machine_id,
            LifecycleStepResult::Failed {
                reason: "injected before dispatch".into(),
            },
        );
    }
    store
        .finish_environment_lifecycle(operation.operation_id.as_str(), operation.generation, 11)
        .unwrap();
}

#[test]
fn armed_proof_survives_restart_and_consumption_cannot_be_rearmed() {
    let (directory, store, operation) = fixture();
    let machine = &operation.machine_steps[0].machine_id;
    let proof = store
        .record_machine_boot_non_dispatch(&operation, machine)
        .unwrap();
    assert_eq!(
        store
            .record_machine_boot_non_dispatch(&operation, machine)
            .unwrap(),
        proof
    );
    drop(store);
    let store = StateStore::open(&directory.path().join("state.db")).unwrap();
    assert_eq!(
        store
            .require_machine_boot_non_dispatch(&environment(&store, &operation), machine)
            .unwrap(),
        Some(proof)
    );
    store
        .consume_machine_boot_non_dispatch(&operation, machine)
        .unwrap();
    drop(store);
    let store = StateStore::open(&directory.path().join("state.db")).unwrap();
    assert!(
        store
            .require_machine_boot_non_dispatch(&environment(&store, &operation), machine)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .record_machine_boot_non_dispatch(&operation, machine)
            .is_err()
    );
    assert!(
        store
            .consume_machine_boot_non_dispatch(&operation, machine)
            .is_err()
    );
}

#[test]
fn partial_dispatch_preserves_only_unbooted_sibling_authority_through_stop() {
    let (_directory, store, operation) = fixture();
    let a = &operation.machine_steps[0].machine_id;
    let b = &operation.machine_steps[1].machine_id;
    store
        .record_machine_boot_non_dispatch(&operation, a)
        .unwrap();
    store
        .record_machine_boot_non_dispatch(&operation, b)
        .unwrap();
    store
        .consume_machine_boot_non_dispatch(&operation, a)
        .unwrap();
    fail(&store, &operation);
    let failed = environment(&store, &operation);
    assert!(
        store
            .require_machine_boot_non_dispatch(&failed, a)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .require_machine_boot_non_dispatch(&failed, b)
            .unwrap()
            .is_some()
    );
    let stop = begin(
        &store,
        &operation.environment_id,
        EnvironmentLifecycleKind::Stop,
        2,
    );
    let stopping = environment(&store, &stop);
    assert!(
        store
            .require_machine_boot_non_dispatch(&stopping, a)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .require_machine_boot_non_dispatch(&stopping, b)
            .unwrap()
            .is_some()
    );
    assert!(store.require_machine_boot_non_dispatch(&failed, b).is_err());
    assert!(
        store
            .consume_machine_boot_non_dispatch(&operation, b)
            .is_err()
    );
    acknowledge(&store, &stop, b, LifecycleStepResult::Succeeded);
    assert!(
        store
            .require_machine_boot_non_dispatch(&environment(&store, &stop), b)
            .unwrap()
            .is_none()
    );
}

#[test]
fn failed_up_proof_transfers_once_to_immediate_retry() {
    let (_directory, store, operation) = fixture();
    let machine = &operation.machine_steps[0].machine_id;
    store
        .record_machine_boot_non_dispatch(&operation, machine)
        .unwrap();
    fail(&store, &operation);
    let next = begin(
        &store,
        &operation.environment_id,
        EnvironmentLifecycleKind::Up,
        2,
    );
    assert!(
        store
            .require_machine_boot_non_dispatch(&environment(&store, &next), machine)
            .unwrap()
            .is_some()
    );
    let proof = store
        .record_machine_boot_non_dispatch(&next, machine)
        .unwrap();
    assert_eq!(proof.generation, 2);
    assert_eq!(proof.operation_id, next.operation_id);
    assert!(
        store
            .consume_machine_boot_non_dispatch(&operation, machine)
            .is_err()
    );
    store
        .consume_machine_boot_non_dispatch(&next, machine)
        .unwrap();
    fail(&store, &next);
    let third = begin(
        &store,
        &operation.environment_id,
        EnvironmentLifecycleKind::Up,
        3,
    );
    assert!(
        store
            .record_machine_boot_non_dispatch(&third, machine)
            .is_err()
    );
}

#[test]
fn failed_without_proof_is_never_inferred_absent() {
    let (_directory, store, operation) = fixture();
    let machine = &operation.machine_steps[0].machine_id;
    assert!(
        store
            .consume_machine_boot_non_dispatch(&operation, machine)
            .is_err()
    );
    fail(&store, &operation);
    assert!(
        store
            .require_machine_boot_non_dispatch(&environment(&store, &operation), machine)
            .unwrap()
            .is_none()
    );
    let next = begin(
        &store,
        &operation.environment_id,
        EnvironmentLifecycleKind::Up,
        2,
    );
    assert!(
        store
            .record_machine_boot_non_dispatch(&next, machine)
            .is_err()
    );
}

#[test]
fn exact_positive_stop_is_new_authority_even_after_consumption() {
    let (_directory, store, operation) = fixture();
    let machine = &operation.machine_steps[0].machine_id;
    store
        .record_machine_boot_non_dispatch(&operation, machine)
        .unwrap();
    store
        .consume_machine_boot_non_dispatch(&operation, machine)
        .unwrap();
    fail(&store, &operation);
    let stop = begin(
        &store,
        &operation.environment_id,
        EnvironmentLifecycleKind::Stop,
        2,
    );
    // This acknowledgment represents independent positive physical Stop evidence,
    // not a conclusion derived from the consumed non-dispatch record.
    for item in &stop.machine_steps {
        acknowledge(
            &store,
            &stop,
            &item.machine_id,
            LifecycleStepResult::Succeeded,
        );
    }
    store
        .finish_environment_lifecycle(stop.operation_id.as_str(), stop.generation, 12)
        .unwrap();
    let next = begin(
        &store,
        &operation.environment_id,
        EnvironmentLifecycleKind::Up,
        3,
    );
    assert_eq!(
        store
            .record_machine_boot_non_dispatch(&next, machine)
            .unwrap()
            .generation,
        3
    );
}

#[test]
fn altered_request_owner_generation_and_machine_reject_without_writes() {
    let (_directory, store, operation) = fixture();
    let machine = &operation.machine_steps[0].machine_id;
    store
        .record_machine_boot_non_dispatch(&operation, machine)
        .unwrap();
    let before = store.total_changes_for_test();
    let mut variants = vec![];
    let mut changed = operation.clone();
    changed.request_hash = format!("sha256:{}", "b".repeat(64));
    variants.push(changed);
    let mut changed = operation.clone();
    changed.request_id.push('x');
    variants.push(changed);
    let mut changed = operation.clone();
    changed.project_id = ProjectId::generate();
    variants.push(changed);
    let mut changed = operation.clone();
    changed.definition_digest = format!("sha256:{}", "b".repeat(64));
    variants.push(changed);
    let mut changed = operation.clone();
    changed.generation += 1;
    variants.push(changed);
    for changed in variants {
        assert!(
            store
                .record_machine_boot_non_dispatch(&changed, machine)
                .is_err()
        );
        assert!(
            store
                .consume_machine_boot_non_dispatch(&changed, machine)
                .is_err()
        );
    }
    assert!(
        store
            .record_machine_boot_non_dispatch(&operation, &MachineId::generate())
            .is_err()
    );
    assert_eq!(store.total_changes_for_test(), before);
    assert!(
        store
            .require_machine_boot_non_dispatch(&environment(&store, &operation), machine)
            .unwrap()
            .is_some()
    );
}

#[test]
fn copied_corrupt_or_foreign_record_fails_closed() {
    let (_directory, store, operation) = fixture();
    let a = &operation.machine_steps[0].machine_id;
    let b = &operation.machine_steps[1].machine_id;
    let proof = store
        .record_machine_boot_non_dispatch(&operation, a)
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO control_metadata(key,value) VALUES(?1,?2)",
            params![
                key(&operation.environment_id, b).unwrap(),
                serde_json::to_string(&Record {
                    proof,
                    consumed: false
                })
                .unwrap()
            ],
        )
        .unwrap();
    assert!(
        store
            .require_machine_boot_non_dispatch(&environment(&store, &operation), b)
            .is_err()
    );
    store
        .conn
        .execute(
            "UPDATE control_metadata SET value = 'invalid' WHERE key = ?1",
            params![key(&operation.environment_id, a).unwrap()],
        )
        .unwrap();
    assert!(
        store
            .require_machine_boot_non_dispatch(&environment(&store, &operation), a)
            .is_err()
    );
}

#[test]
fn delete_supersedes_non_dispatch_authority() {
    let (_directory, store, operation) = fixture();
    let machine = &operation.machine_steps[0].machine_id;
    store
        .record_machine_boot_non_dispatch(&operation, machine)
        .unwrap();
    let delete = begin(
        &store,
        &operation.environment_id,
        EnvironmentLifecycleKind::Delete,
        2,
    );
    assert!(
        store
            .require_machine_boot_non_dispatch(&environment(&store, &delete), machine)
            .is_err()
    );
    assert!(
        store
            .record_machine_boot_non_dispatch(&operation, machine)
            .is_err()
    );
    assert!(
        store
            .consume_machine_boot_non_dispatch(&operation, machine)
            .is_err()
    );
}

#[test]
fn changed_current_generation_cannot_authorize_successor() {
    let (_directory, store, operation) = fixture();
    let machine = &operation.machine_steps[0].machine_id;
    store
        .record_machine_boot_non_dispatch(&operation, machine)
        .unwrap();
    fail(&store, &operation);
    let stop = begin(
        &store,
        &operation.environment_id,
        EnvironmentLifecycleKind::Stop,
        2,
    );
    let record = store
        .boot_record(&operation.environment_id, machine)
        .unwrap()
        .unwrap();
    let mut current = environment(&store, &stop);
    assert!(
        store
            .boot_failed_predecessor(&record, &current, &stop, machine)
            .unwrap()
    );
    current.lifecycle_generation += 1;
    assert!(
        !store
            .boot_failed_predecessor(&record, &current, &stop, machine)
            .unwrap()
    );
    assert!(
        store
            .require_machine_boot_non_dispatch(&current, machine)
            .is_err()
    );
}

#[test]
fn unconsumed_failed_proof_does_not_block_positive_stop_then_up() {
    let (_directory, store, operation) = fixture();
    let machine = &operation.machine_steps[0].machine_id;
    store
        .record_machine_boot_non_dispatch(&operation, machine)
        .unwrap();
    fail(&store, &operation);
    let stop = begin(
        &store,
        &operation.environment_id,
        EnvironmentLifecycleKind::Stop,
        2,
    );
    for item in &stop.machine_steps {
        acknowledge(
            &store,
            &stop,
            &item.machine_id,
            LifecycleStepResult::Succeeded,
        );
    }
    store
        .finish_environment_lifecycle(stop.operation_id.as_str(), stop.generation, 12)
        .unwrap();
    assert!(
        store
            .require_machine_boot_non_dispatch(&environment(&store, &stop), machine)
            .unwrap()
            .is_none()
    );
    let next = begin(
        &store,
        &operation.environment_id,
        EnvironmentLifecycleKind::Up,
        3,
    );
    let proof = store
        .record_machine_boot_non_dispatch(&next, machine)
        .unwrap();
    assert_eq!(proof.generation, 3);
    store
        .consume_machine_boot_non_dispatch(&next, machine)
        .unwrap();
}

#[test]
fn failed_up_delete_successor_recovers_only_unconsumed_proof_after_restart() {
    let (directory, store, up) = fixture();
    let a = up.machine_steps[0].machine_id.clone();
    let b = up.machine_steps[1].machine_id.clone();
    store.record_machine_boot_non_dispatch(&up, &a).unwrap();
    let proof = store.record_machine_boot_non_dispatch(&up, &b).unwrap();
    store.consume_machine_boot_non_dispatch(&up, &a).unwrap();
    fail(&store, &up);
    let delete = begin(
        &store,
        &up.environment_id,
        EnvironmentLifecycleKind::Delete,
        2,
    );
    assert!(
        delete
            .machine_steps
            .iter()
            .all(|step| step.target_state.is_none())
    );
    drop(store);
    let store = StateStore::open(&directory.path().join("state.db")).unwrap();
    let current = environment(&store, &delete);
    let before = store.total_changes_for_test();
    assert_eq!(
        store
            .require_machine_boot_non_dispatch(&current, &b)
            .unwrap(),
        Some(proof)
    );
    assert!(
        store
            .require_machine_boot_non_dispatch(&current, &a)
            .unwrap()
            .is_none()
    );
    assert!(store.record_machine_boot_non_dispatch(&delete, &b).is_err());
    assert!(
        store
            .consume_machine_boot_non_dispatch(&delete, &b)
            .is_err()
    );
    assert!(store.consume_machine_boot_non_dispatch(&up, &b).is_err());
    assert_eq!(store.total_changes_for_test(), before);
    acknowledge(&store, &delete, &b, LifecycleStepResult::Succeeded);
    // Acknowledged Delete uses its own positive quiescence receipt, not a
    // silently broadened pending non-dispatch capability.
    assert!(
        store
            .require_machine_boot_non_dispatch(&environment(&store, &delete), &b)
            .is_err()
    );
}

#[test]
fn delete_successor_rejects_missing_and_foreign_original_proof_without_writes() {
    let (_directory, store, up) = fixture();
    let a = &up.machine_steps[0].machine_id;
    let b = &up.machine_steps[1].machine_id;
    let proof = store.record_machine_boot_non_dispatch(&up, a).unwrap();
    fail(&store, &up);
    let delete = begin(
        &store,
        &up.environment_id,
        EnvironmentLifecycleKind::Delete,
        2,
    );
    let current = environment(&store, &delete);
    assert!(
        store
            .require_machine_boot_non_dispatch(&current, b)
            .unwrap()
            .is_none()
    );
    for mutation in 0..5 {
        let mut changed = proof.clone();
        match mutation {
            0 => changed.project_id = ProjectId::generate(),
            1 => changed.machine_id = b.clone(),
            2 => changed.generation += 1,
            3 => changed.request_hash = format!("sha256:{}", "c".repeat(64)),
            4 => changed.definition_digest = format!("sha256:{}", "c".repeat(64)),
            _ => unreachable!(),
        }
        store
            .conn
            .execute(
                "UPDATE control_metadata SET value=?1 WHERE key=?2",
                params![
                    serde_json::to_string(&Record {
                        proof: changed,
                        consumed: false
                    })
                    .unwrap(),
                    key(&up.environment_id, a).unwrap()
                ],
            )
            .unwrap();
        let before = store.total_changes_for_test();
        assert!(
            store
                .require_machine_boot_non_dispatch(&current, a)
                .is_err(),
            "mutation {mutation}"
        );
        assert_eq!(store.total_changes_for_test(), before);
    }
}

#[test]
fn failed_up_proof_does_not_skip_an_intervening_lifecycle_to_delete() {
    let (_directory, store, up) = fixture();
    let machine = &up.machine_steps[0].machine_id;
    store
        .record_machine_boot_non_dispatch(&up, machine)
        .unwrap();
    fail(&store, &up);
    let intervening = begin(
        &store,
        &up.environment_id,
        EnvironmentLifecycleKind::Stop,
        2,
    );
    fail(&store, &intervening);
    let delete = begin(
        &store,
        &up.environment_id,
        EnvironmentLifecycleKind::Delete,
        3,
    );
    assert!(
        store
            .require_machine_boot_non_dispatch(&environment(&store, &delete), machine)
            .is_err()
    );
}
