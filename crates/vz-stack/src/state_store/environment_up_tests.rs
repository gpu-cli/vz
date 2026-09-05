#![allow(clippy::unwrap_used)]
use super::*;
use vz_runtime_contract::*;

fn fixture() -> (
    tempfile::TempDir,
    StateStore,
    ProjectDefinition,
    EnvironmentSelectionContext,
) {
    let root = tempfile::tempdir().unwrap();
    let store = StateStore::open(&root.path().join("state.db")).unwrap();
    let definition:ProjectDefinition=serde_json::from_value(serde_json::json!({"schema_version":1,"project_id":ProjectId::generate(),"name":"up-admission","environment":{"schema_version":1,"machines":[{"schema_version":1,"name":"app","profile":"hardened","target":{"os":"linux","arch":"aarch64","image":"fixture"}}]}})).unwrap();
    (
        root,
        store,
        definition,
        EnvironmentSelectionContext {
            workspace_key: Some("worktree-opaque-one".into()),
            ..Default::default()
        },
    )
}
fn hash() -> String {
    format!("sha256:{}", "a".repeat(64))
}

#[test]
fn denied_creation_leaves_no_project_environment_or_admission() {
    let (_root, store, definition, selection) = fixture();
    let error = store
        .reserve_environment_up_admission(
            &definition,
            &selection,
            "req-up",
            "idem-up",
            &hash(),
            1,
            |candidate| {
                assert_eq!(candidate.project_id, definition.project_id);
                assert_eq!(candidate.name, "default");
                assert_eq!(candidate.machines.len(), 1);
                Err(StackError::Machine {
                    code: MachineErrorCode::PolicyDenied,
                    message: "deny exact prospective owner".into(),
                })
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        StackError::Machine {
            code: MachineErrorCode::PolicyDenied,
            ..
        }
    ));
    assert!(
        store
            .load_project_state(definition.project_id.as_str())
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_environment_up_admission("idem-up")
            .unwrap()
            .is_none()
    );
}

#[test]
fn first_default_creation_and_request_identity_commit_together_across_restart() {
    let (root, store, definition, selection) = fixture();
    let observed = std::sync::Mutex::new(None);
    let first = store
        .reserve_environment_up_admission(
            &definition,
            &selection,
            "req-up",
            "idem-up",
            &hash(),
            1,
            |candidate| {
                *observed.lock().unwrap() = Some(candidate.environment_id.clone());
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(
        *observed.lock().unwrap(),
        Some(first.environment_id.clone())
    );
    drop(store);
    let store = StateStore::open(&root.path().join("state.db")).unwrap();
    let replay = store
        .reserve_environment_up_admission(
            &definition,
            &selection,
            "req-up",
            "idem-up",
            &hash(),
            2,
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(replay, first);
    let project = store
        .load_project_state(definition.project_id.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(project.environments.len(), 1);
    assert_eq!(project.environments[0].name, "default");
    assert_eq!(project.environments[0].lifecycle_generation, 0);
    // Without this exact durable request, the unbound worktree cannot adopt it.
    assert!(
        store
            .reserve_environment_up_admission(
                &definition,
                &selection,
                "new-request",
                "new-key",
                &hash(),
                3,
                |_| Ok(())
            )
            .is_err()
    );
}

#[test]
fn exact_replay_reauthorizes_and_rejects_changed_request_or_tampered_record() {
    let (_root, store, definition, selection) = fixture();
    store
        .reserve_environment_up_admission(
            &definition,
            &selection,
            "req-up",
            "idem-up",
            &hash(),
            1,
            |_| Ok(()),
        )
        .unwrap();
    assert!(
        store
            .reserve_environment_up_admission(
                &definition,
                &selection,
                "req-up",
                "idem-up",
                &hash(),
                2,
                |_| Err(conflict("policy revoked"))
            )
            .is_err()
    );
    assert!(
        store
            .reserve_environment_up_admission(
                &definition,
                &selection,
                "changed",
                "idem-up",
                &hash(),
                2,
                |_| Ok(())
            )
            .is_err()
    );
    let changed_hash = format!("sha256:{}", "b".repeat(64));
    assert!(
        store
            .reserve_environment_up_admission(
                &definition,
                &selection,
                "req-up",
                "idem-up",
                &changed_hash,
                2,
                |_| Ok(())
            )
            .is_err()
    );
    store
        .set_control_metadata(&key("admission", "idem-up"), "{}")
        .unwrap();
    assert!(store.load_environment_up_admission("idem-up").is_err());
    assert!(
        store
            .reserve_environment_up_admission(
                &definition,
                &selection,
                "req-up",
                "idem-up",
                &hash(),
                2,
                |_| Ok(())
            )
            .is_err()
    );
}

#[test]
fn explicit_names_allow_multiple_instances_without_overwriting_siblings() {
    let (_root, store, definition, mut selection) = fixture();
    let mut ids = Vec::new();
    for name in ["alpha", "beta"] {
        selection.explicit = Some(EnvironmentSelector::NameOrId(name.into()));
        let admission = store
            .reserve_environment_up_admission(
                &definition,
                &selection,
                name,
                name,
                &hash(),
                1,
                |_| Ok(()),
            )
            .unwrap();
        ids.push(admission.environment_id);
    }
    assert_ne!(ids[0], ids[1]);
    assert_eq!(
        store
            .load_project_state(definition.project_id.as_str())
            .unwrap()
            .unwrap()
            .environments
            .len(),
        2
    );
}

#[test]
fn pre_lifecycle_failure_is_durable_and_cannot_be_forged_into_success() {
    let (_root, store, definition, selection) = fixture();
    let admission = store
        .reserve_environment_up_admission(
            &definition,
            &selection,
            "req-up",
            "idem-up",
            &hash(),
            1,
            |_| Ok(()),
        )
        .unwrap();
    let completion = EnvironmentUpCompletion {
        admission,
        operation: None,
        workspace_binding: None,
        error: Some(MachineError::new(
            MachineErrorCode::UnsupportedOperation,
            "verified target unavailable".into(),
            Some("req-up".into()),
            Default::default(),
        )),
        completed_at: 2,
    };
    store.finish_environment_up_admission(&completion).unwrap();
    store.finish_environment_up_admission(&completion).unwrap();
    assert_eq!(
        store.load_environment_up_completion("idem-up").unwrap(),
        Some(completion.clone())
    );
    let mut forged = completion;
    forged.error = None;
    assert!(store.finish_environment_up_admission(&forged).is_err());
}

#[test]
fn invalid_request_rolls_back_even_a_prospective_authorized_creation() {
    let (_root, store, definition, selection) = fixture();
    assert!(
        store
            .reserve_environment_up_admission(
                &definition,
                &selection,
                "invalid\nrequest",
                "idem-up",
                &hash(),
                1,
                |_| Ok(())
            )
            .is_err()
    );
    assert!(
        store
            .load_project_state(definition.project_id.as_str())
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_environment_up_admission("idem-up")
            .unwrap()
            .is_none()
    );
}

#[test]
fn successful_completion_requires_the_exact_persisted_workspace_binding() {
    let (_root, store, definition, selection) = fixture();
    let admission = store
        .reserve_environment_up_admission(
            &definition,
            &selection,
            "req-up",
            "idem-up",
            &hash(),
            1,
            |_| Ok(()),
        )
        .unwrap();
    let operation = store
        .begin_environment_lifecycle(
            admission.environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "req-up",
            "idem-up",
            &hash(),
            2,
        )
        .unwrap();
    for step in &operation.machine_steps {
        let incarnation = MachineIncarnation {
            schema_version: 1,
            incarnation_id: MachineIncarnationId::generate(),
            machine_id: step.machine_id.clone(),
            generation: 1,
            created_at: 2,
        };
        store
            .acknowledge_environment_machine_step(
                &MachineLifecycleStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    machine_id: step.machine_id.clone(),
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: step.expected_incarnation.clone(),
                    resulting_incarnation: Some(incarnation.clone()),
                    resulting_activation: Some(MachineActivationEvidence {
                        schema_version: 1,
                        backend: MachineBackend::MacosVirtualizationLinux,
                        negotiated_capabilities: CapabilitySet::new([MachineCapability::PosixExec]),
                        runtime_identity: MachineRuntimeIdentity {
                            schema_version: 1,
                            opaque_id: "unit-test-only-runtime".into(),
                        },
                        incarnation,
                    }),
                    result: LifecycleStepResult::Succeeded,
                },
                3,
            )
            .unwrap();
    }
    let operation = store
        .finish_environment_lifecycle(operation.operation_id.as_str(), operation.generation, 4)
        .unwrap();
    let binding = WorkspaceBinding {
        schema_version: 1,
        binding_id: WorkspaceBindingId::generate(),
        project_id: admission.project_id.clone(),
        environment_id: admission.environment_id.clone(),
        name: "source".into(),
        workspace_key: selection.workspace_key.unwrap(),
        path_hint: None,
    };
    let mut completion = EnvironmentUpCompletion {
        admission,
        operation: Some(operation),
        workspace_binding: Some(binding.clone()),
        error: None,
        completed_at: 5,
    };
    // Matching owner/token alone does not prove that a binding was committed.
    assert!(completion.validate().is_ok());
    assert!(store.finish_environment_up_admission(&completion).is_err());
    let persisted = store.refresh_workspace_binding(&binding, 5).unwrap();
    completion.workspace_binding.as_mut().unwrap().path_hint = Some("/fabricated".into());
    assert!(store.finish_environment_up_admission(&completion).is_err());
    completion.workspace_binding = Some(persisted);
    let mut failed = completion.clone();
    failed.error = Some(MachineError::new(
        MachineErrorCode::StateConflict,
        "post-lifecycle failure".into(),
        Some("req-up".into()),
        Default::default(),
    ));
    assert!(failed.validate().is_err());
    store.finish_environment_up_admission(&completion).unwrap();
    assert_eq!(
        store.load_environment_up_completion("idem-up").unwrap(),
        Some(completion)
    );
}
