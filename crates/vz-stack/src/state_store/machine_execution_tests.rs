#![allow(clippy::unwrap_used)]
use super::*;
use vz_runtime_contract::*;

fn fixture() -> (tempfile::TempDir, StateStore, MachineExecutionReceipt) {
    let directory = tempfile::tempdir().unwrap();
    let store = StateStore::open(&directory.path().join("state.db")).unwrap();
    let definition = ProjectDefinition {
        schema_version: 1,
        project_id: ProjectId::generate(),
        name: "exec-test".into(),
        environment: EnvironmentSpec {
            schema_version: 1,
            default_machine: None,
            machines: vec![MachineSpec {
                schema_version: 1,
                name: "worker".into(),
                profile: MachineProfile::Hardened,
                target: TargetSpec {
                    os: OperatingSystem::Linux,
                    arch: Architecture::Aarch64,
                    image: "fixture".into(),
                    version: None,
                    channel: None,
                    digest: None,
                },
                resources: MachineResources::default(),
                requested_capabilities: CapabilitySet::new([MachineCapability::PosixExec]),
                workspace: None,
            }],
            networks: vec![],
            endpoints: vec![],
        },
    };
    let mut environment = definition.instantiate_environment("selected", 1).unwrap();
    environment.state = EnvironmentState::Ready;
    environment.lifecycle_generation = 1;
    let machine = &mut environment.machines[0];
    machine.state = MachineState::Ready;
    machine.backend = Some(MachineBackend::MacosVirtualizationLinux);
    machine.negotiated_capabilities = CapabilitySet::new([MachineCapability::PosixExec]);
    machine.incarnation = Some(MachineIncarnation {
        schema_version: 1,
        incarnation_id: MachineIncarnationId::generate(),
        machine_id: machine.machine_id.clone(),
        generation: 1,
        created_at: 1,
    });
    machine.runtime_identity = Some(MachineRuntimeIdentity {
        schema_version: 1,
        opaque_id: "fixture-original-runtime".into(),
    });
    let receipt = MachineExecutionReceipt {
        scope: MachineExecutionScope {
            schema_version: 1,
            execution_id: "mex_fixture".into(),
            request_id: "req-fixture".into(),
            idempotency_key: "idem-fixture".into(),
            request_hash: format!("sha256:{}", "a".repeat(64)),
            project_id: definition.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: machine.machine_id.clone(),
            environment_generation: 1,
            incarnation: machine.incarnation.clone().unwrap(),
            runtime_identity: machine.runtime_identity.clone().unwrap(),
            definition_digest: environment.definition_digest.clone(),
        },
        state: MachineExecutionState::Admitted,
        exit_code: None,
        failure: None,
        output_replay_available: false,
        created_at: 2,
        updated_at: 2,
    };
    environment.ownership.push(OwnershipRecord {
        schema_version: 1,
        resource_kind: OwnedResourceKind::Incarnation,
        resource_id: receipt.scope.incarnation.incarnation_id.to_string(),
        environment_id: environment.environment_id.clone(),
        machine_id: Some(receipt.scope.machine_id.clone()),
    });
    store
        .save_project_state(&ProjectState {
            schema_version: 1,
            definition,
            environments: vec![environment],
        })
        .unwrap();
    (directory, store, receipt)
}

#[test]
fn execution_claim_is_nonexpiring_and_survives_restart_without_duplicate_effect_authority() {
    let (root, store, receipt) = fixture();
    store.claim_machine_execution(&receipt).unwrap();
    assert!(store.claim_machine_execution(&receipt).is_err());
    drop(store);
    let reopened = StateStore::open(&root.path().join("state.db")).unwrap();
    assert_eq!(
        reopened.load_machine_execution("idem-fixture").unwrap(),
        Some(receipt.clone())
    );
    assert!(reopened.claim_machine_execution(&receipt).is_err());
}

#[test]
fn terminal_receipt_is_exact_and_immutable() {
    let (_root, store, receipt) = fixture();
    store.claim_machine_execution(&receipt).unwrap();
    let mut terminal = receipt.clone();
    terminal.state = MachineExecutionState::Completed;
    terminal.exit_code = Some(7);
    terminal.updated_at = 3;
    store
        .finish_machine_execution(&receipt.scope, &terminal)
        .unwrap();
    store
        .finish_machine_execution(&receipt.scope, &terminal)
        .unwrap();
    let mut changed = terminal.clone();
    changed.exit_code = Some(0);
    assert!(
        store
            .finish_machine_execution(&receipt.scope, &changed)
            .is_err()
    );
    changed = terminal;
    changed.scope.machine_id = MachineId::generate();
    assert!(
        store
            .finish_machine_execution(&receipt.scope, &changed)
            .is_err()
    );
}

#[test]
fn stale_generation_and_tampered_ledger_fail_closed() {
    let (_root, store, receipt) = fixture();
    let mut stale = receipt.clone();
    stale.scope.environment_generation += 1;
    assert!(store.claim_machine_execution(&stale).is_err());
    stale = receipt.clone();
    stale.scope.incarnation.incarnation_id = MachineIncarnationId::generate();
    assert!(store.claim_machine_execution(&stale).is_err());
    store.claim_machine_execution(&receipt).unwrap();
    store
        .set_control_metadata(&key("idem-fixture"), "{}")
        .unwrap();
    assert!(store.load_machine_execution("idem-fixture").is_err());
    assert!(store.claim_machine_execution(&receipt).is_err());
}
