#![allow(clippy::unwrap_used)]
use super::*;
use vz_runtime_contract::*;

fn fixture() -> (Validator, MachineExecutionScope) {
    let spec = MachineExecutionSpec {
        argv: vec!["/bin/sh".into(), "-c".into(), "exit 7".into()],
        environment: Default::default(),
        working_directory: None,
        user: None,
        terminal: None,
        timeout_millis: 1000,
    };
    let project = ProjectId::generate();
    let environment = EnvironmentId::generate();
    let machine = MachineId::generate();
    let scope = MachineExecutionScope {
        schema_version: 1,
        execution_id: "mex_fixture".into(),
        request_id: "req_fixture".into(),
        idempotency_key: "idem_fixture".into(),
        request_hash: spec.request_hash(&project, &environment, &machine).unwrap(),
        project_id: project.clone(),
        environment_id: environment.clone(),
        machine_id: machine.clone(),
        environment_generation: 1,
        incarnation: MachineIncarnation {
            schema_version: 1,
            incarnation_id: MachineIncarnationId::generate(),
            machine_id: machine.clone(),
            generation: 1,
            created_at: 1,
        },
        runtime_identity: MachineRuntimeIdentity {
            schema_version: 1,
            opaque_id: "fixture-original".into(),
        },
        definition_digest: format!("sha256:{}", "a".repeat(64)),
    };
    let validation = Validator {
        spec,
        project,
        environment: Some(environment),
        machine: Some(machine),
        metadata: runtime_v2::RequestMetadata {
            request_id: scope.request_id.clone(),
            idempotency_key: scope.idempotency_key.clone(),
            trace_id: String::new(),
        },
        scope: None,
        sequence: 0,
        ready: false,
        terminal: false,
    };
    (validation, scope)
}
fn event(
    scope: &MachineExecutionScope,
    sequence: u64,
    payload: runtime_v2::machine_exec_event::Payload,
) -> runtime_v2::MachineExecEvent {
    runtime_v2::MachineExecEvent {
        schema_version: 1,
        scope: Some(vz_runtime_translate::machine_execution_scope_to_proto(
            scope,
        )),
        sequence,
        replayed: false,
        payload: Some(payload),
    }
}

#[test]
fn checked_machine_stream_preserves_binary_output_and_nonzero_exit() {
    use runtime_v2::machine_exec_event::Payload;
    let (mut validator, scope) = fixture();
    validator
        .event(event(&scope, 0, Payload::Ready(true)))
        .unwrap();
    let output = validator
        .event(event(&scope, 1, Payload::Stdout(vec![0, 255, 10])))
        .unwrap();
    assert!(matches!(output.output,MachineExecOutput::Stdout(bytes) if bytes==[0,255,10]));
    validator
        .event(event(&scope, 2, Payload::Stderr(vec![9])))
        .unwrap();
    let receipt = MachineExecutionReceipt {
        scope: scope.clone(),
        state: MachineExecutionState::Completed,
        exit_code: Some(7),
        failure: None,
        output_replay_available: false,
        created_at: 2,
        updated_at: 3,
    };
    let terminal = event(
        &scope,
        3,
        Payload::Receipt(vz_runtime_translate::machine_execution_receipt_to_proto(
            &receipt,
        )),
    );
    assert!(
        matches!(validator.event(terminal.clone()).unwrap().output,MachineExecOutput::Receipt(value) if value.exit_code==Some(7))
    );
    assert!(validator.event(terminal).is_err());
}

#[test]
fn checked_machine_stream_pins_all_immutable_scope_fields() {
    use runtime_v2::machine_exec_event::Payload;
    for mutation in 0..8 {
        let (mut validator, scope) = fixture();
        validator
            .event(event(&scope, 0, Payload::Ready(true)))
            .unwrap();
        let mut changed = scope.clone();
        match mutation {
            0 => changed.environment_generation += 1,
            1 => changed.runtime_identity.opaque_id.push('x'),
            2 => changed.incarnation.incarnation_id = MachineIncarnationId::generate(),
            3 => changed.definition_digest = format!("sha256:{}", "b".repeat(64)),
            4 => changed.execution_id.push('x'),
            5 => changed.request_id.push('x'),
            6 => changed.idempotency_key.push('x'),
            _ => changed.request_hash = format!("sha256:{}", "b".repeat(64)),
        }
        assert!(
            validator
                .event(event(&changed, 1, Payload::Stdout(vec![1])))
                .is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn checked_machine_stream_rejects_wrong_first_owner_and_process_attributes() {
    use runtime_v2::machine_exec_event::Payload;
    for mutation in 0..3 {
        let (mut validator, mut scope) = fixture();
        match mutation {
            0 => scope.environment_id = EnvironmentId::generate(),
            1 => {
                scope.machine_id = MachineId::generate();
                scope.incarnation.machine_id = scope.machine_id.clone();
            }
            _ => validator.spec.argv.push("changed".into()),
        }
        if mutation < 2 {
            scope.request_hash = validator
                .spec
                .request_hash(&scope.project_id, &scope.environment_id, &scope.machine_id)
                .unwrap();
        }
        assert!(
            validator
                .event(event(&scope, 0, Payload::Ready(true)))
                .is_err()
        );
    }
}

#[test]
fn checked_machine_stream_rejects_output_before_ready_and_invalid_replay() {
    use runtime_v2::machine_exec_event::Payload;
    let (mut validator, scope) = fixture();
    assert!(
        validator
            .event(event(&scope, 0, Payload::Stdout(vec![1])))
            .is_err()
    );
    let receipt = MachineExecutionReceipt {
        scope: scope.clone(),
        state: MachineExecutionState::Uncertain,
        exit_code: None,
        failure: Some("unknown".into()),
        output_replay_available: false,
        created_at: 1,
        updated_at: 2,
    };
    let mut replay = event(
        &scope,
        0,
        Payload::Receipt(vz_runtime_translate::machine_execution_receipt_to_proto(
            &receipt,
        )),
    );
    replay.replayed = true;
    assert!(validator.event(replay).is_err());
}
