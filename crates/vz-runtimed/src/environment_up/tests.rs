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
async fn declared_egress_rejects_before_project_creation() {
    // A declarable record with no adapter behind it. Admitting one would start
    // a Machine that silently lacks the boundary its definition asks for, so Up
    // must refuse and create no project.
    //
    // Host EXPORTS and IMPORTS are no longer in this list. An export is carried
    // by `start_port_forwarding`'s loopback-only listener; an import by the
    // per-Machine vsock terminator and the guest agent's loopback listeners,
    // with a per-declaration credential. `authorize_ownership` accounts for
    // both, and the admitted cases are asserted by
    // `a_fixed_port_host_export_is_admitted_and_persisted` and
    // `a_declared_host_import_is_admitted_and_persisted` below. What remains
    // refused about either is only the shape no surface can serve, which the
    // `..._cannot_serve_is_refused_for_its_own_named_reason` tests cover one
    // case at a time.
    for mutate in [(|request: &mut EnvironmentUpRequest| {
        request.definition.environment.machines[0].egress = EgressPolicy::Allowed;
    }) as fn(&mut EnvironmentUpRequest)]
    {
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

/// Declare one host import on a Developer Linux Machine.
fn declare_host_import(
    request: &mut EnvironmentUpRequest,
    name: &str,
    host_port: u16,
    guest_port: Option<u16>,
) {
    request
        .definition
        .environment
        .host_imports
        .push(HostImportSpec {
            schema_version: 1,
            name: name.into(),
            machine: request.definition.environment.machines[0].name.clone(),
            protocol: TransportProtocol::Tcp,
            host_port,
            guest_port,
            alias: None,
        });
}

/// The admitted half of criterion 7's import clauses.
///
/// This is the positive every import denial is measured against. Without it,
/// "an import is refused" would be indistinguishable from "imports are not
/// implemented", which is exactly what the blanket refusal this replaced meant.
/// The Up still fails afterwards, because the test backend cannot boot a
/// Machine — what is asserted is that the failure is no longer an admission
/// refusal, that the import instance and its Machine-scoped ownership edge were
/// persisted (that edge is what `environment_delete` demands before it will
/// reclaim anything), and that the join the boot loop performs resolves the
/// persisted instance to exactly one grant.
#[tokio::test]
async fn a_declared_host_import_is_admitted_and_persisted() {
    let (_root, daemon, mut request, metadata) = fixture();
    declare_host_import(&mut request, "db", 5432, Some(15432));
    let completion = terminal(
        daemon
            .up_environment(request.clone(), metadata)
            .await
            .unwrap(),
    )
    .await;
    // The Up does not succeed in this fixture; it must not fail at admission.
    assert!(completion.error.is_some());
    let project = daemon
        .with_state_store(|store| store.load_project_state(request.definition.project_id.as_str()))
        .unwrap()
        .expect("a declared import is now admitted, so its project exists");
    let environment = &project.environments[0];
    assert_eq!(environment.host_imports.len(), 1);
    assert_eq!(environment.host_imports[0].name, "db");
    assert_eq!(
        environment.host_imports[0].machine_id,
        environment.machines[0].machine_id
    );
    let edges = environment
        .ownership
        .iter()
        .filter(|record| record.resource_kind == OwnedResourceKind::HostImport)
        .collect::<Vec<_>>();
    assert_eq!(edges.len(), 1, "exactly one HostImport ownership edge");
    assert_eq!(
        edges[0].resource_id,
        environment.host_imports[0].import_id.to_string()
    );
    assert_eq!(
        edges[0].machine_id.as_ref(),
        Some(&environment.machines[0].machine_id),
        "the edge must be Machine-scoped or Delete dispatches its cleanup with no store"
    );
    let resolved = super::host_imports::resolve_environment_host_imports(
        &request.definition.environment,
        &environment.machines,
        &environment.host_imports,
    )
    .expect("the persisted import joins its declaration");
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].host_port, 5432);
    assert_eq!(resolved[0].guest_port, 15432);
    assert_eq!(resolved[0].machine_id, environment.machines[0].machine_id);
    // The grant that would be installed carries a secret and, in its guest
    // projection, no host destination at all.
    let grants = super::host_imports::boot_import_grants(&resolved).expect("credentials");
    let machine_grants = grants
        .get(&environment.machines[0].machine_id)
        .expect("grants for the declared Machine");
    assert_eq!(machine_grants.len(), 1);
    assert_eq!(machine_grants[0].host_port, 5432);
    assert_ne!(machine_grants[0].credential, [0u8; 32]);
}

/// Each import shape Up cannot serve is refused by its own name, before any
/// project row exists. Paired with the admitted case above, so each refusal is
/// evidence about that shape rather than about imports as a whole.
///
/// Four of these are refused by the portable definition itself rather than by
/// this Up, exactly as two of the export cases are: `EnvironmentSpec::validate`
/// (`vz-runtime-contract/src/types/topology.rs`) already refuses port 0 on
/// either side, a repeated declaration name, and an import naming a Machine the
/// Environment does not declare, so they never reach
/// `refuse_unsupported_host_imports`. Asserting the message actually produced,
/// rather than the one this Up would have produced, is the point; that module's
/// own rule for each is exercised directly by its unit tests. The
/// duplicate-guest-port refusal below has no counterpart in the contract -- one
/// Machine's loopback port carrying two imports is a runtime fact -- and is
/// this module's alone.
#[tokio::test]
async fn an_import_this_up_cannot_serve_is_refused_for_its_own_named_reason() {
    for (mutate, expected) in [
        // Port 0 names no host service; the relay would have nothing to dial.
        (
            (|request: &mut EnvironmentUpRequest| {
                declare_host_import(request, "db", 0, None);
            }) as fn(&mut EnvironmentUpRequest),
            "invalid host_import.host_port",
        ),
        // Guest port 0 asks the kernel to pick a port nothing granted.
        (
            |request: &mut EnvironmentUpRequest| {
                declare_host_import(request, "db", 5432, Some(0));
            },
            "invalid host_import.guest_port",
        ),
        // Two declarations of one name: one name resolves to one host service,
        // and the open frame carries only the name.
        (
            |request: &mut EnvironmentUpRequest| {
                declare_host_import(request, "db", 5432, Some(15432));
                declare_host_import(request, "db", 5433, Some(15433));
            },
            "duplicate host_import_name",
        ),
        // Two declarations on one guest loopback port: the second would shadow
        // the first and a guest process could not tell which it reached.
        (
            |request: &mut EnvironmentUpRequest| {
                declare_host_import(request, "db", 5432, Some(15432));
                declare_host_import(request, "cache", 6379, Some(15432));
            },
            "both bind guest loopback port",
        ),
        // A Machine the Environment does not declare.
        (
            |request: &mut EnvironmentUpRequest| {
                request
                    .definition
                    .environment
                    .host_imports
                    .push(HostImportSpec {
                        schema_version: 1,
                        name: "db".into(),
                        machine: "absent".into(),
                        protocol: TransportProtocol::Tcp,
                        host_port: 5432,
                        guest_port: None,
                        alias: None,
                    });
            },
            "host_import.machine",
        ),
    ] {
        let (_root, daemon, mut request, metadata) = fixture();
        mutate(&mut request);
        let error = daemon
            .up_environment(request.clone(), metadata)
            .await
            .unwrap_err();
        assert!(
            error.message.contains(expected),
            "expected a refusal naming {expected:?}, got {:?}",
            error.message
        );
        assert!(
            daemon
                .with_state_store(
                    |store| store.load_project_state(request.definition.project_id.as_str())
                )
                .unwrap()
                .is_none(),
            "a refused import must leave no project row behind"
        );
    }
}

/// Declare one fixed-port host export on a Developer Linux Machine.
fn declare_host_export(request: &mut EnvironmentUpRequest, name: &str, host_port: u16) {
    request
        .definition
        .environment
        .host_exports
        .push(HostExportSpec {
            schema_version: 1,
            name: name.into(),
            machine: request.definition.environment.machines[0].name.clone(),
            protocol: TransportProtocol::Tcp,
            machine_port: 8080,
            host_port: Some(host_port),
        });
}

/// The admitted half of criterion 7's export clauses.
///
/// This is the positive every export denial below is measured against: without
/// it, "an export is refused" would be indistinguishable from "exports are not
/// implemented", which is what the refusal this replaced actually meant. The Up
/// still fails afterwards, because the test backend cannot boot a Machine — what
/// is asserted is that the failure is no longer an admission refusal, and that
/// the export instance and its ownership edge were persisted, because that edge
/// is what `environment_delete` will demand before it will reclaim anything.
#[tokio::test]
async fn a_fixed_port_host_export_is_admitted_and_persisted() {
    let (_root, daemon, mut request, metadata) = fixture();
    declare_host_export(&mut request, "api", 18080);
    let completion = terminal(
        daemon
            .up_environment(request.clone(), metadata)
            .await
            .unwrap(),
    )
    .await;
    // The Up does not succeed in this fixture; it must not fail at admission.
    assert!(completion.error.is_some());
    let project = daemon
        .with_state_store(|store| store.load_project_state(request.definition.project_id.as_str()))
        .unwrap()
        .expect("a fixed-port export is now admitted, so its project exists");
    let environment = &project.environments[0];
    assert_eq!(environment.host_exports.len(), 1);
    assert_eq!(environment.host_exports[0].name, "api");
    assert_eq!(
        environment.host_exports[0].machine_id,
        environment.machines[0].machine_id
    );
    let edges = environment
        .ownership
        .iter()
        .filter(|record| record.resource_kind == OwnedResourceKind::HostExport)
        .collect::<Vec<_>>();
    assert_eq!(edges.len(), 1, "exactly one HostExport ownership edge");
    assert_eq!(
        edges[0].resource_id,
        environment.host_exports[0].export_id.to_string()
    );
    assert_eq!(
        edges[0].machine_id.as_ref(),
        Some(&environment.machines[0].machine_id),
        "the edge must be Machine-scoped or Delete dispatches its cleanup with no store"
    );
    // The join the boot loop performs, on the persisted instances rather than a
    // restatement of them: one loopback mapping, no destination address.
    let resolved = super::host_exports::resolve_environment_host_exports(
        &request.definition.environment,
        &environment.machines,
        &environment.host_exports,
    )
    .expect("the persisted export joins its declaration");
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].mapping.host, 18080);
    assert_eq!(resolved[0].mapping.container, 8080);
    assert_eq!(resolved[0].mapping.target_service, None);
}

/// Each export shape Up cannot serve is refused by its own name, before any
/// project row exists. Paired with the admitted case above, so each refusal is
/// evidence about that shape rather than about exports as a whole.
///
/// Two of these are refused by the portable definition itself rather than by
/// this Up: `validate_machine_network_support`
/// (`vz-runtime-contract/src/types/topology.rs`) already refuses an export on a
/// Hardened or non-Linux Machine -- a native macOS Machine may now hold an
/// Environment-fabric attachment, but host exports and imports are the host
/// half of the boundary and stay Linux-only -- so they arrive as
/// `ValidationError` and never reach `refuse_unsupported_host_exports`. That
/// module's own matching rule is therefore the second of two and is exercised
/// directly by its unit tests; asserting the code actually produced, rather than
/// the one this Up would have produced, is the point.
#[tokio::test]
async fn an_export_this_up_cannot_serve_is_refused_for_its_own_named_reason() {
    for (mutate, code, expected) in [
        // Nothing reports a dynamically allocated loopback port back to the
        // caller, so the caller could not use the export it asked for.
        (
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
            MachineErrorCode::UnsupportedOperation,
            "dynamically allocated loopback port",
        ),
        // The native macOS arm of `boot_or_inspect_machine` is handed no
        // `PortMapping` at all, so the listener would silently never exist.
        (
            |request: &mut EnvironmentUpRequest| {
                request.definition.environment.machines[0].target.os = OperatingSystem::Macos;
                declare_host_export(request, "api", 18080);
            },
            MachineErrorCode::ValidationError,
            "native Macos target cannot declare host exports",
        ),
        // Hardened is the restricted profile and declares none of this topology.
        (
            |request: &mut EnvironmentUpRequest| {
                request.definition.environment.machines[0].profile = MachineProfile::Hardened;
                declare_host_export(request, "api", 18080);
            },
            MachineErrorCode::ValidationError,
            "Hardened Machines cannot declare host exports",
        ),
        // Two exports on one loopback port is a definition defect, refused
        // before any Machine of the Environment has been started.
        (
            |request: &mut EnvironmentUpRequest| {
                declare_host_export(request, "api", 18080);
                declare_host_export(request, "web", 18080);
            },
            MachineErrorCode::UnsupportedOperation,
            "one loopback port carries at most one export",
        ),
    ] {
        let (_root, daemon, mut request, metadata) = fixture();
        mutate(&mut request);
        let error = daemon
            .up_environment(request.clone(), metadata)
            .await
            .unwrap_err();
        assert_eq!(
            error.code, code,
            "case `{expected}` produced {:?}: {}",
            error.code, error.message
        );
        assert!(
            error.message.contains(expected),
            "expected a refusal naming `{expected}`, got {:?}",
            error.message
        );
        assert!(
            daemon
                .with_state_store(
                    |store| store.load_project_state(request.definition.project_id.as_str())
                )
                .unwrap()
                .is_none(),
            "a refused export must leave no project row behind"
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

/// The public-like half of the same boundary.
///
/// `simulated_public` was refused outright until the Environment edge existed;
/// it is now admitted and persisted exactly like a private network, and what
/// distinguishes it is what the fabric then starts on it rather than whether Up
/// will take it. The endpoint is declared `https` because the edge terminates
/// that and only that; the plan refuses anything else on a public-like network
/// rather than publishing a name behind a listener that does not exist.
#[tokio::test]
async fn a_public_like_network_is_admitted_and_persisted_like_a_private_one() {
    let (root, daemon, mut request, metadata) = fixture();
    request.workspace_root = Some(root.path().to_string_lossy().into_owned());
    declare_private_fabric(&mut request, NetworkKind::SimulatedPublic);
    request.definition.environment.endpoints[0].protocol = EndpointProtocol::Https;
    request.definition.environment.endpoints[0].hostname = Some("api.shop.test".into());
    let completion = terminal(
        daemon
            .up_environment(request.clone(), metadata)
            .await
            .unwrap(),
    )
    .await;
    // As with the private case, this build has no verified image, so the Up
    // still ends in a preparation failure long before a switch or an edge is
    // started. Admission is all this asserts; the edge actually carrying
    // traffic is proved over a real switch in `environment_gateway_tests`.
    assert!(completion.error.is_some());
    let project = daemon
        .with_state_store(|store| store.load_project_state(request.definition.project_id.as_str()))
        .unwrap()
        .expect("a public-like fabric is admitted, so its project exists");
    let environment = &project.environments[0];
    assert_eq!(environment.networks.len(), 1);
    assert_eq!(environment.networks[0].kind, NetworkKind::SimulatedPublic);
    assert_eq!(environment.endpoints.len(), 1);
    assert_eq!(
        environment.endpoints[0].hostname.as_deref(),
        Some("api.shop.test")
    );
    assert_eq!(environment.network_attachments.len(), 1);
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

    // `HostExport` now has an adapter, so it is admitted the way the fabric
    // kinds are: only when the record names a persisted export instance. The
    // positive and its negative are asserted together, because an admitted kind
    // whose set comparison were dropped would be a leak Delete could not reclaim.
    let mut exported = EnvironmentUpRequest {
        workspace_root: None,
        definition: definition(),
        selection: EnvironmentSelectionContext::default(),
        path_hint: None,
        timeout_millis: 5000,
    };
    exported
        .definition
        .environment
        .host_exports
        .push(HostExportSpec {
            schema_version: 1,
            name: "api".into(),
            machine: exported.definition.environment.machines[0].name.clone(),
            protocol: TransportProtocol::Tcp,
            machine_port: 8080,
            host_port: Some(18080),
        });
    let exported = exported
        .definition
        .instantiate_environment("default", 0)
        .unwrap();
    assert_eq!(exported.host_exports.len(), 1);
    authorize_ownership(&exported).expect("a minted host export matches its instance");
    let mut unaccounted_export = exported.clone();
    unaccounted_export.host_exports.clear();
    assert_eq!(
        code(&unaccounted_export),
        MachineErrorCode::UnsupportedOperation,
        "an export ownership edge with no instance behind it must stay refused"
    );
    let mut duplicated_export = exported.clone();
    let repeated_export = duplicated_export
        .ownership
        .iter()
        .find(|record| record.resource_kind == OwnedResourceKind::HostExport)
        .unwrap()
        .clone();
    duplicated_export.ownership.push(repeated_export);
    assert_eq!(
        code(&duplicated_export),
        MachineErrorCode::UnsupportedOperation,
        "a repeated export identity must be refused, never deduplicated"
    );

    for kind in [
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
    // `HostImport` now has an adapter, so it leaves that list — but an edge
    // with no persisted instance behind it must still be refused, exactly as an
    // unaccounted export edge is. Otherwise Up would boot an Environment whose
    // import Delete could never reclaim.
    let mut unaccounted_import = environment.clone();
    let environment_id = unaccounted_import.environment_id.clone();
    let machine_id = unaccounted_import.machines[0].machine_id.clone();
    unaccounted_import.ownership.push(OwnershipRecord {
        schema_version: 1,
        resource_kind: OwnedResourceKind::HostImport,
        resource_id: "hmp_unaccounted".into(),
        environment_id,
        machine_id: Some(machine_id),
    });
    assert_eq!(
        code(&unaccounted_import),
        MachineErrorCode::UnsupportedOperation,
        "an import ownership edge with no instance behind it must stay refused"
    );
}

/// The refused half of the same boundary: declarations no adapter implements
/// must still be rejected, and rejected before any project row exists, so an Up
/// that cannot be served never leaves state behind.
/// Declare a second Developer Linux Machine so a volume can name two.
fn add_sibling_machine(request: &mut EnvironmentUpRequest, name: &str) {
    let mut sibling = request.definition.environment.machines[0].clone();
    sibling.name = name.to_string();
    sibling.workspace = None;
    request.definition.environment.machines.push(sibling);
}

/// One writable block volume attached to two Machines: the declaration the
/// workspace-and-storage policy refuses.
fn declare_writable_block_volume(request: &mut EnvironmentUpRequest, first: &str, second: &str) {
    if !request
        .definition
        .environment
        .machines
        .iter()
        .any(|machine| machine.name == second)
    {
        add_sibling_machine(request, second);
    }
    request.definition.environment.volumes = vec![VolumeSpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        name: "data".to_string(),
        kind: VolumeKind::Block,
        size_bytes: Some(16 * 1024 * 1024),
        consistency: None,
        attachments: vec![
            VolumeAttachment {
                machine: first.to_string(),
                target_path: "/data".to_string(),
                mode: VolumeAccessMode::ReadWrite,
            },
            VolumeAttachment {
                machine: second.to_string(),
                target_path: "/data".to_string(),
                mode: VolumeAccessMode::ReadOnly,
            },
        ],
    }];
}

/// One shared cache attached to the named Machines.
fn declare_shared_cache(request: &mut EnvironmentUpRequest, first: &str, second: &str) {
    let mut attachments = vec![VolumeAttachment {
        machine: first.to_string(),
        target_path: "/cache".to_string(),
        mode: VolumeAccessMode::ReadWrite,
    }];
    if second != first {
        if !request
            .definition
            .environment
            .machines
            .iter()
            .any(|machine| machine.name == second)
        {
            add_sibling_machine(request, second);
        }
        attachments.push(VolumeAttachment {
            machine: second.to_string(),
            target_path: "/cache".to_string(),
            mode: VolumeAccessMode::ReadWrite,
        });
    }
    request.definition.environment.volumes = vec![VolumeSpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        name: "cache".to_string(),
        kind: VolumeKind::SharedCache,
        size_bytes: None,
        consistency: Some(SharedCacheConsistency {
            model: SharedCacheConsistencyModel::BoundedStaleness,
            staleness_bound_millis: 2_000,
        }),
        attachments,
    }];
}

#[tokio::test]
async fn declarations_without_adapters_still_reject_before_project_creation() {
    for (mutate, expected) in [
        // `simulated_public` is no longer here: criterion 6 built the
        // per-Environment gateway, and `snapshot` is gone for the same reason
        // -- criterion 17 implemented it once `clonefile(2)` was shown to
        // clone a hierarchy. What remains are declarations whose carrier
        // genuinely does not exist.
        //
        // A volume is carried by the same VirtioFS/virtio-block hint the native
        // macOS backend is never handed, so a volume on a native Machine would
        // boot storage that silently never appears.
        (
            (|request: &mut EnvironmentUpRequest| {
                request.definition.environment.machines[0].target.os = OperatingSystem::Macos;
                declare_shared_cache(request, "app", "app");
            }) as fn(&mut EnvironmentUpRequest),
            MachineErrorCode::UnsupportedOperation,
        ),
        // The share is carried by a `vz-mount-{N}` VirtioFS tag that only
        // `linux/initramfs/init` bind-mounts, and the native macOS backend is
        // handed no resource hint at all, so the projection would silently
        // never appear.
        (
            |request: &mut EnvironmentUpRequest| {
                request.definition.environment.machines[0].target.os = OperatingSystem::Macos;
                request.definition.environment.machines[0].workspace = Some(WorkspaceProjection {
                    binding: "source".into(),
                    source_path: ".".into(),
                    target_path: "/workspace".into(),
                    mode: WorkspaceProjectionMode::ReadOnly,
                });
            },
            MachineErrorCode::UnsupportedOperation,
        ),
        // Hardened is the restricted profile and declares none of this
        // topology, matching the contract's refusal of network attachments.
        (
            |request: &mut EnvironmentUpRequest| {
                request.definition.environment.machines[0].profile = MachineProfile::Hardened;
                request.definition.environment.machines[0].workspace = Some(WorkspaceProjection {
                    binding: "source".into(),
                    source_path: ".".into(),
                    target_path: "/workspace".into(),
                    mode: WorkspaceProjectionMode::ReadOnly,
                });
            },
            MachineErrorCode::UnsupportedOperation,
        ),
        // The criterion-17 refusal, asserted here for its ORDERING rather than
        // its message: a writable block volume on two Machines must be refused
        // before `reserve_environment_up_admission` runs, and the project-state
        // assertion below is what proves nothing was written. Unlike the cases
        // above this is a `ValidationError`, because the product contract
        // forbids the declaration outright rather than the runtime lacking an
        // adapter for it -- it will still be refused when every adapter exists.
        (
            |request: &mut EnvironmentUpRequest| {
                declare_writable_block_volume(request, "app", "sibling");
            },
            MachineErrorCode::ValidationError,
        ),
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
            expected
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
