#![allow(clippy::unwrap_used)]
use super::*;

pub(super) fn fixture() -> (EnvironmentInstance, MachineExecInput, MachineExecutionScope) {
    let definition = ProjectDefinition {
        schema_version: 1,
        project_id: ProjectId::generate(),
        name: "execution-tests".into(),
        environment: EnvironmentSpec {
            schema_version: 1,
            machines: ["app", "worker"]
                .into_iter()
                .map(|name| MachineSpec {
                    schema_version: 1,
                    name: name.into(),
                    profile: MachineProfile::Hardened,
                    target: TargetSpec {
                        os: OperatingSystem::Linux,
                        arch: Architecture::Aarch64,
                        image: "fixture".into(),
                        version: None,
                        channel: None,
                        digest: None,
                    },
                    resources: Default::default(),
                    requested_capabilities: CapabilitySet::new([MachineCapability::PosixExec]),
                    workspace: None,
                })
                .collect(),
            networks: vec![],
            endpoints: vec![],
        },
    };
    let environment = definition.instantiate_environment("selected", 1).unwrap();
    let machine = environment.machines[0].machine_id.clone();
    let input = MachineExecInput {
        project_id: definition.project_id.clone(),
        selection: EnvironmentSelectionContext {
            explicit: Some(EnvironmentSelector::NameOrId("selected".into())),
            ..Default::default()
        },
        machine: Some("app".into()),
        process_machine_id: None,
        metadata: RequestMetadata::new(Some("req-exec".into()), Some("idem-exec".into())),
        spec: MachineExecutionSpec {
            argv: vec!["/bin/sh".into()],
            environment: Default::default(),
            working_directory: None,
            user: None,
            terminal: None,
            timeout_millis: 1000,
        },
    };
    let scope = MachineExecutionScope {
        schema_version: 1,
        execution_id: "mex_fixture".into(),
        request_id: "req-exec".into(),
        idempotency_key: "idem-exec".into(),
        request_hash: input
            .spec
            .request_hash(&input.project_id, &environment.environment_id, &machine)
            .unwrap(),
        project_id: definition.project_id,
        environment_id: environment.environment_id.clone(),
        machine_id: machine.clone(),
        environment_generation: 1,
        incarnation: MachineIncarnation {
            schema_version: 1,
            incarnation_id: MachineIncarnationId::generate(),
            machine_id: machine,
            generation: 1,
            created_at: 1,
        },
        runtime_identity: MachineRuntimeIdentity {
            schema_version: 1,
            opaque_id: "original-exact-fixture".into(),
        },
        definition_digest: environment.definition_digest.clone(),
    };
    (environment, input, scope)
}

#[test]
fn explicit_machine_precedence_and_cross_environment_process_ids_fail_closed() {
    let (environment, mut input, _) = fixture();
    input.process_machine_id = Some(MachineId::generate());
    assert_eq!(select_machine(&input, &environment).unwrap().name, "app");
    input.machine = None;
    assert!(select_machine(&input, &environment).is_err());
    input.process_machine_id = None;
    assert!(select_machine(&input, &environment).is_err());
    input.machine = Some(String::new());
    assert!(select_machine(&input, &environment).is_err());
}

#[test]
fn request_digest_binds_every_process_attribute_and_resolved_owner() {
    let (environment, input, _) = fixture();
    let machine = &environment.machines[0];
    let hash = request_hash(&input, &environment, machine).unwrap();
    for mutation in 0..6 {
        let mut changed = input.clone();
        match mutation {
            0 => changed.spec.argv.push("changed".into()),
            1 => changed.spec.working_directory = Some("/tmp".into()),
            2 => changed
                .spec
                .environment
                .insert("X".into(), "Y".into())
                .map(|_| ())
                .unwrap_or(()),
            3 => changed.spec.user = Some("1000".into()),
            4 => changed.spec.timeout_millis += 1,
            _ => {
                changed.spec.terminal = Some(MachineExecutionTerminal {
                    rows: 24,
                    columns: 80,
                })
            }
        }
        assert_ne!(hash, request_hash(&changed, &environment, machine).unwrap());
    }
    assert_ne!(
        hash,
        request_hash(&input, &environment, &environment.machines[1]).unwrap()
    );
}

#[test]
fn scope_and_receipts_never_forge_exit_for_unknown_history() {
    let (_, _, scope) = fixture();
    let mut receipt = MachineExecutionReceipt {
        scope,
        state: MachineExecutionState::Quiesced,
        exit_code: None,
        failure: Some("no live work; history unknown".into()),
        output_replay_available: false,
        created_at: 1,
        updated_at: 2,
    };
    receipt.validate().unwrap();
    receipt.exit_code = Some(0);
    assert!(receipt.validate().is_err());
    receipt.exit_code = None;
    receipt.state = MachineExecutionState::Completed;
    assert!(receipt.validate().is_err());
    receipt.state = MachineExecutionState::Uncertain;
    receipt.output_replay_available = true;
    assert!(receipt.validate().is_err());
}

fn daemon_fixture(
    hook: Option<Arc<dyn RuntimePolicyHook>>,
) -> (
    tempfile::TempDir,
    Arc<RuntimeDaemon>,
    MachineExecInput,
    MachineExecutionReceipt,
) {
    let (mut environment, input, scope) = fixture();
    let definition = ProjectDefinition {
        schema_version: 1,
        project_id: input.project_id.clone(),
        name: "execution-tests".into(),
        environment: EnvironmentSpec {
            schema_version: 1,
            machines: environment
                .machines
                .iter()
                .map(|machine| MachineSpec {
                    schema_version: 1,
                    name: machine.name.clone(),
                    profile: machine.profile,
                    target: machine.target.clone(),
                    resources: machine.resources.clone(),
                    requested_capabilities: machine.requested_capabilities.clone(),
                    workspace: None,
                })
                .collect(),
            networks: vec![],
            endpoints: vec![],
        },
    };
    environment.state = EnvironmentState::Failed;
    environment.lifecycle_generation = 1;
    for machine in &mut environment.machines {
        machine.state = MachineState::Stopped;
    }
    let selected = &mut environment.machines[0];
    selected.state = MachineState::Ready;
    selected.backend = Some(MachineBackend::MacosVirtualizationLinux);
    selected.negotiated_capabilities = CapabilitySet::new([MachineCapability::PosixExec]);
    selected.incarnation = Some(scope.incarnation.clone());
    selected.runtime_identity = Some(scope.runtime_identity.clone());
    environment.ownership.push(OwnershipRecord {
        schema_version: 1,
        resource_kind: OwnedResourceKind::Incarnation,
        resource_id: scope.incarnation.incarnation_id.to_string(),
        environment_id: scope.environment_id.clone(),
        machine_id: Some(scope.machine_id.clone()),
    });
    let root = tempfile::tempdir().unwrap();
    let config = crate::RuntimedConfig {
        state_store_path: root.path().join("state.db"),
        runtime_data_dir: root.path().join("runtime"),
        socket_path: root.path().join("daemon.sock"),
    };
    vz_stack::StateStore::open(&config.state_store_path)
        .unwrap()
        .save_project_state(&ProjectState {
            schema_version: 1,
            definition,
            environments: vec![environment],
        })
        .unwrap();
    let daemon = Arc::new(match hook {
        Some(hook) => RuntimeDaemon::start_with_policy_hook(config, hook, None).unwrap(),
        None => RuntimeDaemon::start(config).unwrap(),
    });
    let receipt = MachineExecutionReceipt {
        scope,
        state: MachineExecutionState::Admitted,
        exit_code: None,
        failure: None,
        output_replay_available: false,
        created_at: 2,
        updated_at: 2,
    };
    (root, daemon, input, receipt)
}

#[tokio::test]
async fn persisted_ready_after_restart_never_reconstructs_or_claims_unknown_runtime() {
    let (_root, daemon, input, receipt) = daemon_fixture(None);
    let (_sender, controls) = mpsc::channel(1);
    let error = daemon.exec_machine(input, controls).await.unwrap_err();
    assert_eq!(error.code, MachineErrorCode::StateConflict);
    assert_eq!(error.request_id.as_deref(), Some("req-exec"));
    assert!(
        daemon
            .with_state_store(|store| store.load_machine_execution(&receipt.scope.idempotency_key))
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn exact_terminal_replay_is_read_only_and_changed_process_is_rejected() {
    let (_root, daemon, input, mut receipt) = daemon_fixture(None);
    daemon
        .with_state_store(|store| store.claim_machine_execution(&receipt))
        .unwrap();
    receipt.state = MachineExecutionState::Completed;
    receipt.exit_code = Some(7);
    receipt.updated_at = 3;
    daemon
        .with_state_store(|store| store.finish_machine_execution(&receipt.scope, &receipt))
        .unwrap();
    let (_sender, controls) = mpsc::channel(1);
    let mut events = daemon.exec_machine(input.clone(), controls).await.unwrap();
    let event = events.recv().await.unwrap().unwrap();
    assert!(event.replayed);
    assert!(matches!(event.payload,MachineExecPayload::Receipt(value) if *value==receipt));
    assert!(events.recv().await.is_none());
    let mut changed = input;
    changed.spec.argv.push("changed".into());
    let (_sender, controls) = mpsc::channel(1);
    assert_eq!(
        daemon
            .exec_machine(changed, controls)
            .await
            .unwrap_err()
            .code,
        MachineErrorCode::StateConflict
    );
}

#[tokio::test]
async fn admitted_or_uncertain_restart_receipt_cannot_authorize_duplicate_execution() {
    let (_root, daemon, input, mut receipt) = daemon_fixture(None);
    daemon
        .with_state_store(|store| store.claim_machine_execution(&receipt))
        .unwrap();
    for uncertain in [false, true] {
        if uncertain {
            receipt.state = MachineExecutionState::Uncertain;
            receipt.failure = Some("unknown original guest outcome".into());
            receipt.updated_at = 3;
            daemon
                .with_state_store(|store| store.finish_machine_execution(&receipt.scope, &receipt))
                .unwrap();
        }
        let (_sender, controls) = mpsc::channel(1);
        assert_eq!(
            daemon
                .exec_machine(input.clone(), controls)
                .await
                .unwrap_err()
                .code,
            MachineErrorCode::StateConflict
        );
    }
}

struct SandboxOnly;
impl RuntimePolicyHook for SandboxOnly {
    fn evaluate(
        &self,
        _: RuntimeOperation,
        _: &RequestMetadata,
    ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
        Ok(PolicyDecision::Allow)
    }
}
#[tokio::test]
async fn sandbox_policy_does_not_authorize_machine_execution() {
    let (_root, daemon, input, receipt) = daemon_fixture(Some(Arc::new(SandboxOnly)));
    let (_sender, controls) = mpsc::channel(1);
    assert_eq!(
        daemon.exec_machine(input, controls).await.unwrap_err().code,
        MachineErrorCode::PolicyDenied
    );
    assert!(
        daemon
            .with_state_store(|store| store.load_machine_execution(&receipt.scope.idempotency_key))
            .unwrap()
            .is_none()
    );
}
