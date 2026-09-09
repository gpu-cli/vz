#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::RuntimedConfig;

fn definition() -> ProjectDefinition {
    serde_json::from_value(serde_json::json!({"schema_version":1,"project_id":ProjectId::generate(),"name":"up-tests","environment":{"schema_version":1,"machines":[
        {"schema_version":1,"name":"app","profile":"developer","target":{"os":"linux","arch":"aarch64","image":"vz-linux-appliance","digest":format!("sha256:{}","a".repeat(64))}}
    ]}})).unwrap()
}

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
    (
        root,
        daemon,
        EnvironmentUpRequest {
            workspace_root: None,
            definition: definition(),
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
async fn declared_host_relays_and_egress_reject_before_project_creation() {
    // These are declarable records with no adapter behind them yet. Admitting
    // one would start a Machine that silently lacks the boundary its definition
    // asks for, so Up must refuse and create no project.
    for mutate in [
        (|request: &mut EnvironmentUpRequest| {
            request
                .definition
                .environment
                .host_exports
                .push(HostExportSpec {
                    schema_version: 1,
                    name: "api".into(),
                    machine: request.definition.environment.machines[0].name.clone(),
                    protocol: TransportProtocol::Tcp,
                    machine_port: 8080,
                    host_port: None,
                });
        }) as fn(&mut EnvironmentUpRequest),
        |request: &mut EnvironmentUpRequest| {
            request
                .definition
                .environment
                .host_imports
                .push(HostImportSpec {
                    schema_version: 1,
                    name: "db".into(),
                    machine: request.definition.environment.machines[0].name.clone(),
                    protocol: TransportProtocol::Tcp,
                    host_port: 5432,
                    guest_port: None,
                    alias: None,
                });
        },
        |request: &mut EnvironmentUpRequest| {
            request.definition.environment.machines[0].egress = EgressPolicy::Allowed;
        },
    ] {
        let (_root, daemon, mut request, metadata) = fixture();
        mutate(&mut request);
        assert_eq!(
            daemon
                .up_environment(request.clone(), metadata)
                .await
                .unwrap_err()
                .code,
            MachineErrorCode::UnsupportedOperation
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
}

/// Attach one Developer Linux Machine to one declared private network and give
/// it a declared endpoint. This is the smallest topology the whole vz-9vv epic
/// exists to serve.
fn declare_private_fabric(request: &mut EnvironmentUpRequest, kind: NetworkKind) {
    request.definition.environment.networks.push(NetworkSpec {
        schema_version: 1,
        name: "private".into(),
        kind,
        cidr: None,
    });
    request.definition.environment.machines[0]
        .networks
        .push("private".into());
    request.definition.environment.endpoints.push(EndpointSpec {
        schema_version: 1,
        name: "api".into(),
        machine: request.definition.environment.machines[0].name.clone(),
        network: "private".into(),
        protocol: EndpointProtocol::Tcp,
        port: 8080,
        hostname: None,
    });
}

/// The admitted half of the boundary vz-9vv.7 moved.
///
/// This replaces the test that pinned declared networks and endpoints as
/// refused. It exists for the same reason — to make the gate move by decision
/// rather than by accident — but now pins where the gate actually stands: a
/// private network, an endpoint on it and a read/write workspace projection are
/// admitted, and their instances and ownership edges are persisted for Stop and
/// Delete to reconcile.
///
/// Admission is all this asserts. This build has no verified image, so the Up
/// still terminates in a preparation failure before `install_environment_fabric`
/// or the workspace binding reservation ever run; a switch actually coming up is
/// hardware evidence, not a unit-test claim.
#[tokio::test]
async fn declared_networks_endpoints_and_workspaces_are_admitted_and_persisted() {
    let (root, daemon, mut request, metadata) = fixture();
    request.workspace_root = Some(root.path().to_string_lossy().into_owned());
    declare_private_fabric(&mut request, NetworkKind::Private);
    request.definition.environment.machines[0].workspace = Some(WorkspaceProjection {
        binding: "source".into(),
        source_path: ".".into(),
        target_path: "/workspace".into(),
        mode: WorkspaceProjectionMode::ReadWrite,
    });
    let completion = terminal(
        daemon
            .up_environment(request.clone(), metadata)
            .await
            .unwrap(),
    )
    .await;
    assert!(completion.error.is_some());
    let project = daemon
        .with_state_store(|store| store.load_project_state(request.definition.project_id.as_str()))
        .unwrap()
        .expect("a declared fabric is now admitted, so its project exists");
    let environment = &project.environments[0];
    assert_eq!(environment.networks.len(), 1);
    assert_eq!(environment.networks[0].kind, NetworkKind::Private);
    assert_eq!(environment.endpoints.len(), 1);
    assert_eq!(environment.endpoints[0].port, 8080);
    assert_eq!(environment.network_attachments.len(), 1);
    assert_eq!(
        environment.network_attachments[0].machine_id,
        environment.machines[0].machine_id
    );
    // `authorize_up` admitted at all only because these edges matched the
    // instances exactly; assert they are the edges Delete will look for.
    for kind in [
        OwnedResourceKind::Network,
        OwnedResourceKind::Endpoint,
        OwnedResourceKind::NetworkAttachment,
    ] {
        assert_eq!(
            environment
                .ownership
                .iter()
                .filter(|record| record.resource_kind == kind)
                .count(),
            1,
            "exactly one {kind:?} ownership edge"
        );
    }
}

/// `authorize_up`'s ownership guard, exercised directly on the graph.
///
/// Admitting declared fabric is only sound while every fabric ownership edge
/// names a persisted instance and every instance carries exactly one edge. An
/// edge with no instance is the unaccounted resource `environment_delete`
/// refuses to reclaim, so an Up that admitted one would boot every Machine of an
/// Environment that could then never be deleted. Kinds with no adapter behind
/// them stay refused whatever else the graph holds.
#[test]
fn fabric_ownership_is_admitted_only_when_it_matches_the_persisted_instances() {
    fn code(environment: &EnvironmentInstance) -> MachineErrorCode {
        match authorize_ownership(environment).unwrap_err() {
            StackError::Machine { code, .. } => code,
            other => panic!("expected a Machine error, got {other:?}"),
        }
    }
    let mut request = EnvironmentUpRequest {
        workspace_root: None,
        definition: definition(),
        selection: EnvironmentSelectionContext::default(),
        path_hint: None,
        timeout_millis: 5000,
    };
    declare_private_fabric(&mut request, NetworkKind::Private);
    let environment = request
        .definition
        .instantiate_environment("default", 0)
        .unwrap();
    authorize_ownership(&environment).expect("minted fabric ownership matches its instances");

    let mut orphaned = environment.clone();
    orphaned.networks.clear();
    assert_eq!(code(&orphaned), MachineErrorCode::UnsupportedOperation);

    let mut unaccounted = environment.clone();
    unaccounted
        .ownership
        .retain(|record| record.resource_kind != OwnedResourceKind::Endpoint);
    assert_eq!(code(&unaccounted), MachineErrorCode::UnsupportedOperation);

    let mut duplicated = environment.clone();
    let repeated = duplicated
        .ownership
        .iter()
        .find(|record| record.resource_kind == OwnedResourceKind::NetworkAttachment)
        .unwrap()
        .clone();
    duplicated.ownership.push(repeated);
    assert_eq!(code(&duplicated), MachineErrorCode::UnsupportedOperation);

    for kind in [
        OwnedResourceKind::HostExport,
        OwnedResourceKind::HostImport,
        OwnedResourceKind::Socket,
        OwnedResourceKind::PortRange,
        OwnedResourceKind::Credential,
        OwnedResourceKind::Fault,
        OwnedResourceKind::LegacySandbox,
        OwnedResourceKind::Other("some_unimplemented_adapter".into()),
    ] {
        let mut adapterless = environment.clone();
        let environment_id = adapterless.environment_id.clone();
        adapterless.ownership.push(OwnershipRecord {
            schema_version: 1,
            resource_kind: kind.clone(),
            resource_id: "resource".into(),
            environment_id,
            machine_id: None,
        });
        assert_eq!(
            code(&adapterless),
            MachineErrorCode::UnsupportedOperation,
            "{kind:?} has no adapter and must stay refused"
        );
    }
}

/// The refused half of the same boundary: declarations no adapter implements
/// must still be rejected, and rejected before any project row exists, so an Up
/// that cannot be served never leaves state behind.
#[tokio::test]
async fn declarations_without_adapters_still_reject_before_project_creation() {
    for mutate in [
        // No egress path off a private fabric exists, and the shared vmnet NAT
        // segment is disqualified by the contract, so nothing can serve this
        // until vz-9vv.6 builds a per-Environment gateway.
        (|request: &mut EnvironmentUpRequest| {
            declare_private_fabric(request, NetworkKind::SimulatedPublic);
        }) as fn(&mut EnvironmentUpRequest),
        // No directory-tree copy primitive and no `OwnedResourceKind` variant,
        // so Delete could neither reclaim nor account for a snapshot.
        |request: &mut EnvironmentUpRequest| {
            request.definition.environment.machines[0].workspace = Some(WorkspaceProjection {
                binding: "source".into(),
                source_path: ".".into(),
                target_path: "/workspace".into(),
                mode: WorkspaceProjectionMode::Snapshot,
            });
        },
        // The share is carried by a `vz-mount-{N}` VirtioFS tag that only
        // `linux/initramfs/init` bind-mounts, and the native macOS backend is
        // handed no resource hint at all, so the projection would silently
        // never appear.
        |request: &mut EnvironmentUpRequest| {
            request.definition.environment.machines[0].target.os = OperatingSystem::Macos;
            request.definition.environment.machines[0].workspace = Some(WorkspaceProjection {
                binding: "source".into(),
                source_path: ".".into(),
                target_path: "/workspace".into(),
                mode: WorkspaceProjectionMode::ReadOnly,
            });
        },
        // Hardened is the restricted profile and declares none of this
        // topology, matching the contract's refusal of network attachments.
        |request: &mut EnvironmentUpRequest| {
            request.definition.environment.machines[0].profile = MachineProfile::Hardened;
            request.definition.environment.machines[0].workspace = Some(WorkspaceProjection {
                binding: "source".into(),
                source_path: ".".into(),
                target_path: "/workspace".into(),
                mode: WorkspaceProjectionMode::ReadOnly,
            });
        },
    ] {
        let (root, daemon, mut request, metadata) = fixture();
        request.workspace_root = Some(root.path().to_string_lossy().into_owned());
        mutate(&mut request);
        assert_eq!(
            daemon
                .up_environment(request.clone(), metadata)
                .await
                .unwrap_err()
                .code,
            MachineErrorCode::UnsupportedOperation
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
}

#[tokio::test(flavor = "multi_thread")]
async fn unsupported_topology_and_invalid_ids_reject_before_project_creation() {
    let (_root, daemon, mut request, mut metadata) = fixture();
    // A workspace projection whose declared source path escapes the worktree
    // root is refused by the contract before Up ever reserves a project.
    request.definition.environment.machines[0].workspace = Some(WorkspaceProjection {
        source_path: "../outside".to_string(),
        binding: "source".into(),
        target_path: "/src".into(),
        mode: WorkspaceProjectionMode::ReadOnly,
    });
    assert!(
        daemon
            .up_environment(request.clone(), metadata.clone())
            .await
            .is_err()
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

#[tokio::test]
async fn context_ownership_authorization_requires_the_exact_persisted_machine_descriptor() {
    let (_root, daemon, request, metadata) = fixture();
    let completion = terminal(
        daemon
            .up_environment(request, metadata.clone())
            .await
            .unwrap(),
    )
    .await;
    let project = daemon
        .with_state_store(|store| {
            store.load_project_state(completion.admission.project_id.as_str())
        })
        .unwrap()
        .unwrap();
    let mut environment = project.environments[0].clone();
    let machine = &environment.machines[0];
    let context = MachineDockerContextDescriptor {
        schema_version: 1,
        owner: ResourceOwner {
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(machine.machine_id.clone()),
        },
        name: "exact-machine-context".into(),
        endpoint: "unix:///private/tmp/exact-unused.sock".into(),
        config_dir: "/private/tmp/exact-client".into(),
        engine_id: "exact-engine".into(),
        incarnation_id: MachineIncarnationId::generate(),
        incarnation_generation: 1,
    };
    let record = OwnershipRecord {
        schema_version: 1,
        resource_kind: OwnedResourceKind::DockerContext,
        resource_id: context.name.clone(),
        environment_id: context.owner.environment_id.clone(),
        machine_id: context.owner.machine_id.clone(),
    };
    environment.ownership.push(record);
    assert!(daemon.authorize_up(&metadata, &environment).is_err());
    environment.machines[0].docker_context = Some(context.clone());
    daemon.authorize_up(&metadata, &environment).unwrap();
    let last = environment.ownership.len() - 1;
    for variant in 0..4 {
        let mut changed = environment.clone();
        match variant {
            0 => changed.ownership[last].resource_id = "foreign-context".into(),
            1 => changed.ownership[last].machine_id = Some(MachineId::generate()),
            2 => changed.ownership[last].environment_id = EnvironmentId::generate(),
            _ => {
                changed.machines[0]
                    .docker_context
                    .as_mut()
                    .unwrap()
                    .owner
                    .project_id = ProjectId::generate()
            }
        }
        assert!(daemon.authorize_up(&metadata, &changed).is_err());
    }
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
    let records =
        [
            crate::machine_runtime_registry::MachineRuntimeRegistry::<
                vz_oci_macos::MacosRuntimeBackend,
            >::reservation(&owner)
            .unwrap(),
            crate::machine_runtime_registry::MachineRuntimeEntry::<
                crate::machine_backend::MachineBackendRuntime,
            >::vm_reservation(&owner)
            .unwrap(),
        ];
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

/// Two Machines projecting one writable host source are refused at admission,
/// before any project row or workspace binding exists.
///
/// The supervisor reserves a durable `WorkspaceBinding` before it resolves
/// shares, so a refusal that only happened at resolution would already have
/// mutated state. This asserts the earlier refusal directly: the state store
/// still has no project for the definition afterwards.
#[tokio::test]
async fn a_writable_host_source_shared_by_two_machines_rejects_before_project_creation() {
    for (label, first_mode, second_mode, second_source) in [
        (
            "two writers of one source",
            WorkspaceProjectionMode::ReadWrite,
            WorkspaceProjectionMode::ReadWrite,
            "src",
        ),
        (
            "a writer and a reader of one source",
            WorkspaceProjectionMode::ReadWrite,
            WorkspaceProjectionMode::ReadOnly,
            "src",
        ),
        (
            "a writer of the root containing another writer's subtree",
            WorkspaceProjectionMode::ReadWrite,
            WorkspaceProjectionMode::ReadWrite,
            ".",
        ),
    ] {
        let (root, daemon, mut request, metadata) = fixture();
        request.workspace_root = Some(root.path().to_string_lossy().into_owned());
        let machines = &mut request.definition.environment.machines;
        machines[0].workspace = Some(WorkspaceProjection {
            binding: "source".into(),
            source_path: "src".into(),
            target_path: "/workspace".into(),
            mode: first_mode,
        });
        let mut second = machines[0].clone();
        second.name = "app-two".into();
        second.workspace = Some(WorkspaceProjection {
            binding: "source".into(),
            source_path: second_source.into(),
            target_path: "/workspace".into(),
            mode: second_mode,
        });
        machines.push(second);
        let error = daemon
            .up_environment(request.clone(), metadata)
            .await
            .unwrap_err();
        assert_eq!(
            error.code,
            MachineErrorCode::ValidationError,
            "{label} must be refused"
        );
        assert!(
            error.message.contains("writable host source"),
            "{label}: refusal must say why: {}",
            error.message
        );
        assert!(
            daemon
                .with_state_store(
                    |store| store.load_project_state(request.definition.project_id.as_str())
                )
                .unwrap()
                .is_none(),
            "{label}: refused before any project row exists"
        );
    }
}

/// The control for the rule above: two Machines READING one host source is an
/// ordinary declaration and must still be admitted. Without this the admission
/// rule could pass by refusing every shared source rather than every writer.
#[tokio::test]
async fn two_read_only_projections_of_one_source_are_still_admitted() {
    let (root, daemon, mut request, metadata) = fixture();
    request.workspace_root = Some(root.path().to_string_lossy().into_owned());
    let machines = &mut request.definition.environment.machines;
    machines[0].workspace = Some(WorkspaceProjection {
        binding: "source".into(),
        source_path: "src".into(),
        target_path: "/workspace".into(),
        mode: WorkspaceProjectionMode::ReadOnly,
    });
    let mut second = machines[0].clone();
    second.name = "app-two".into();
    machines.push(second);
    // `up_environment` returns once admission has decided; the boot runs
    // behind the returned progress stream. So `Ok` here is precisely the
    // statement that admission accepted two readers of one source.
    daemon
        .up_environment(request, metadata)
        .await
        .expect("two read-only readers of one source must pass admission");
}
