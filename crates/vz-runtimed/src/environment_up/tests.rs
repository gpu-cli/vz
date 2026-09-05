#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::RuntimedConfig;

fn fixture() -> (
    tempfile::TempDir,
    Arc<RuntimeDaemon>,
    EnvironmentUpRequest,
    RequestMetadata,
) {
    let root = tempfile::Builder::new()
        .prefix("vz-up-")
        .tempdir_in("/private/tmp")
        .unwrap();
    let daemon = Arc::new(
        RuntimeDaemon::start(RuntimedConfig {
            state_store_path: root.path().join("state.db"),
            runtime_data_dir: root.path().join("r"),
            socket_path: root.path().join("d.sock"),
        })
        .unwrap(),
    );
    let definition=serde_json::from_value(serde_json::json!({"schema_version":1,"project_id":ProjectId::generate(),"name":"up-tests","environment":{"schema_version":1,"machines":[
        {"schema_version":1,"name":"app","profile":"developer","target":{"os":"linux","arch":"aarch64","image":"vz-linux-appliance","digest":format!("sha256:{}","a".repeat(64))}}
    ]}})).unwrap();
    (
        root,
        daemon,
        EnvironmentUpRequest {
            definition,
            selection: EnvironmentSelectionContext {
                workspace_key: Some("opaque-worktree".into()),
                ..Default::default()
            },
            path_hint: None,
            timeout_millis: 5000,
        },
        RequestMetadata::new(Some("req-up-test".into()), Some("idem-up-test".into())),
    )
}
async fn terminal(mut receiver: watch::Receiver<EnvironmentUpProgress>) -> EnvironmentUpCompletion {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(completion) = receiver.borrow_and_update().completion.clone() {
                return completion;
            }
            receiver.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn empty_verified_catalog_cannot_boot_or_publish_ready_even_in_test_backend_build() {
    let (_root, daemon, request, metadata) = fixture();
    let completion = terminal(
        daemon
            .up_environment(request.clone(), metadata)
            .await
            .unwrap(),
    )
    .await;
    assert!(completion.error.is_some());
    assert!(completion.operation.is_none());
    let project = daemon
        .with_state_store(|store| store.load_project_state(request.definition.project_id.as_str()))
        .unwrap()
        .unwrap();
    assert_eq!(project.environments.len(), 1);
    let environment = &project.environments[0];
    assert_eq!(environment.state, EnvironmentState::Creating);
    assert!(
        environment
            .ownership
            .iter()
            .all(|record| record.resource_kind == OwnedResourceKind::Machine)
    );
    assert!(
        environment
            .machines
            .iter()
            .all(|machine| machine.runtime_identity.is_none()
                && machine.incarnation.is_none()
                && machine.state != MachineState::Ready)
    );
    assert!(environment.bindings.is_empty());
}

#[tokio::test]
async fn concurrent_exact_retries_and_disconnected_observer_keep_one_durable_admission() {
    let (_root, daemon, request, metadata) = fixture();
    let first = daemon
        .up_environment(request.clone(), metadata.clone())
        .await
        .unwrap();
    let admission = first.borrow().admission.clone();
    drop(first);
    let completion = terminal(
        daemon
            .up_environment(request.clone(), metadata.clone())
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(completion.admission, admission);
    let replay = terminal(
        daemon
            .up_environment(request.clone(), metadata.clone())
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(completion, replay);
    let mut changed = request;
    changed.timeout_millis += 1;
    assert_eq!(
        daemon
            .up_environment(changed, metadata)
            .await
            .unwrap_err()
            .code,
        MachineErrorCode::StateConflict
    );
}

#[tokio::test]
async fn unsupported_topology_and_invalid_ids_reject_before_project_creation() {
    let (_root, daemon, mut request, mut metadata) = fixture();
    request.definition.environment.machines[0].workspace = Some(WorkspaceProjection {
        binding: "source".into(),
        target_path: "/src".into(),
        mode: WorkspaceProjectionMode::ReadOnly,
    });
    assert_eq!(
        daemon
            .up_environment(request.clone(), metadata.clone())
            .await
            .unwrap_err()
            .code,
        MachineErrorCode::UnsupportedOperation
    );
    request.definition.environment.machines[0].workspace = None;
    metadata.request_id = Some("bad\nrequest".into());
    assert_eq!(
        daemon
            .up_environment(request.clone(), metadata)
            .await
            .unwrap_err()
            .code,
        MachineErrorCode::ValidationError
    );
    assert!(
        daemon
            .with_state_store(
                |store| store.load_project_state(request.definition.project_id.as_str())
            )
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn failed_admission_releases_controller_without_fabricating_live_session() {
    let (_root, daemon, request, metadata) = fixture();
    let completion = terminal(daemon.up_environment(request, metadata).await.unwrap()).await;
    let lease = tokio::time::timeout(
        Duration::from_secs(1),
        daemon.acquire_environment_controller(
            &completion.admission.project_id,
            &completion.admission.environment_id,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    drop(lease);
}

#[test]
fn request_identity_ignores_diagnostic_path_and_shadowed_process_selector() {
    let (_root, _daemon, mut request, _metadata) = fixture();
    request.selection.explicit = Some(EnvironmentSelector::NameOrId("named".into()));
    let hash = request.request_hash().unwrap();
    request.path_hint = Some("/moved/worktree".into());
    request.selection.process_environment_id = Some(EnvironmentId::generate());
    assert_eq!(hash, request.request_hash().unwrap());
    request.selection.workspace_key = Some("different-opaque-token".into());
    assert_ne!(hash, request.request_hash().unwrap());
}

#[tokio::test]
async fn deadline_does_not_abort_owned_readiness_or_publish_late_success() {
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = Arc::clone(&completed);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(10);
    let result = readiness::await_readiness(
        async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            observed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        deadline,
        &RequestMetadata::new(Some("req-deadline".into()), Some("key".into())),
    )
    .await;
    assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(result.unwrap_err().code, MachineErrorCode::Timeout);
}

struct DenyUp;
impl RuntimePolicyHook for DenyUp {
    fn evaluate(
        &self,
        _operation: RuntimeOperation,
        _metadata: &RequestMetadata,
    ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
        Ok(PolicyDecision::Allow)
    }
    fn evaluate_topology(
        &self,
        scope: &TopologyAuthorization,
        _metadata: &RequestMetadata,
    ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
        assert_eq!(scope.operation, TopologyOperation::Up);
        assert!(!scope.machine_ids.is_empty());
        Ok(PolicyDecision::Deny {
            reason: "deny exact prospective Machine ownership".into(),
        })
    }
}
#[tokio::test]
async fn exact_topology_policy_denial_precedes_project_creation() {
    let (_root, _unused, request, metadata) = fixture();
    let root = tempfile::tempdir().unwrap();
    let daemon = Arc::new(
        RuntimeDaemon::start_with_policy_hook(
            RuntimedConfig {
                state_store_path: root.path().join("state.db"),
                runtime_data_dir: root.path().join("runtime"),
                socket_path: root.path().join("d.sock"),
            },
            Arc::new(DenyUp),
            None,
        )
        .unwrap(),
    );
    let error = daemon
        .up_environment(request.clone(), metadata)
        .await
        .unwrap_err();
    assert_eq!(error.code, MachineErrorCode::PolicyDenied);
    assert!(
        daemon
            .with_state_store(
                |store| store.load_project_state(request.definition.project_id.as_str())
            )
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn stop_accounts_for_exact_failed_up_non_dispatch_without_reconstructing_a_vm() {
    let (_root, daemon, request, metadata) = fixture();
    let admission = daemon
        .with_state_store(|store| {
            store.reserve_environment_up_admission(
                &request.definition,
                &request.selection,
                metadata.request_id.as_deref().unwrap(),
                metadata.idempotency_key.as_deref().unwrap(),
                &request.request_hash().unwrap(),
                1,
                |_| Ok(()),
            )
        })
        .unwrap();
    let owner = ResourceOwner {
        project_id: admission.project_id.clone(),
        environment_id: admission.environment_id.clone(),
        machine_id: Some(admission.machine_ids[0].clone()),
    };
    let records=[crate::machine_runtime_registry::MachineRuntimeRegistry::<vz_oci_macos::MacosRuntimeBackend>::reservation(&owner).unwrap(),
        crate::machine_runtime_registry::MachineRuntimeEntry::<vz_oci_macos::MacosRuntimeBackend>::vm_reservation(&owner).unwrap()];
    daemon
        .with_state_store(|store| {
            for record in &records {
                store.reserve_owned_resource(record, 1)?;
            }
            Ok(())
        })
        .unwrap();
    let operation = daemon
        .with_state_store(|store| {
            store.begin_environment_lifecycle(
                admission.environment_id.as_str(),
                EnvironmentLifecycleKind::Up,
                &admission.request_id,
                &admission.idempotency_key,
                &admission.request_hash,
                2,
            )
        })
        .unwrap();
    daemon
        .with_state_store(|store| {
            store.record_machine_boot_non_dispatch(&operation, &admission.machine_ids[0])
        })
        .unwrap();
    let step = &operation.machine_steps[0];
    daemon
        .with_state_store(|store| {
            store.acknowledge_environment_machine_step(
                &MachineLifecycleStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    machine_id: step.machine_id.clone(),
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: None,
                    resulting_incarnation: None,
                    resulting_activation: None,
                    result: LifecycleStepResult::Failed {
                        reason: "deadline before any VM dispatch".into(),
                    },
                },
                3,
            )
        })
        .unwrap();
    daemon
        .with_state_store(|store| {
            store.finish_environment_lifecycle(
                operation.operation_id.as_str(),
                operation.generation,
                4,
            )
        })
        .unwrap();
    let mut stream = daemon
        .stop_environment(crate::environment_stop::StopEnvironmentInput {
            project_id: admission.project_id.clone(),
            selection: EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::Id(admission.environment_id.clone())),
                ..Default::default()
            },
            metadata: RequestMetadata::new(Some("stop-request".into()), Some("stop-key".into())),
            machine_timeout: Duration::from_secs(1),
        })
        .await
        .unwrap();
    let mut receipt = None;
    while let Some(event) = stream.recv().await {
        let event = event.unwrap();
        if event.terminal {
            receipt = Some(event);
        }
    }
    let receipt = receipt.unwrap();
    assert!(receipt.error.is_none());
    assert_eq!(
        receipt.operation.status,
        EnvironmentLifecycleStatus::Succeeded
    );
    let environment = daemon
        .with_state_store(|store| store.load_project_state_snapshot(admission.project_id.as_str()))
        .unwrap()
        .unwrap()
        .environments
        .remove(0);
    assert_eq!(environment.state, EnvironmentState::Stopped);
    // The boot proof is not absence/deletion of stores or pinned state.
    for record in records {
        daemon
            .with_state_store(|store| store.require_owned_resource(&record))
            .unwrap();
    }
}
