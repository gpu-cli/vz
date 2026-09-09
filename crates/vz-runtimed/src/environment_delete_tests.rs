//! Host controller/filesystem tests, not physical VM quiescence evidence.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use std::fs;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::process::Command;
use std::sync::Mutex;

use crate::RuntimedConfig;
use crate::machine_runtime_registry::MachineRuntimeAdmission;
use vz_runtime_contract::{
    CapabilitySet, EndpointProtocol, EndpointSpec, EnvironmentSelector, EnvironmentSpec,
    EnvironmentState, MachineProfile, MachineResources, MachineSpec, MachineState, NetworkKind,
    NetworkSpec, ProjectDefinition, ProjectState, RuntimeOperation, RuntimePolicyHook,
    TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};

#[derive(Default)]
struct DeleteOnlyPolicy {
    scopes: Mutex<Vec<TopologyAuthorization>>,
}

impl RuntimePolicyHook for DeleteOnlyPolicy {
    fn evaluate(
        &self,
        _: RuntimeOperation,
        _: &RequestMetadata,
    ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
        Ok(PolicyDecision::Deny {
            reason: "no legacy authority".into(),
        })
    }

    fn evaluate_topology(
        &self,
        scope: &TopologyAuthorization,
        _: &RequestMetadata,
    ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
        self.scopes.lock().unwrap().push(scope.clone());
        Ok(if scope.operation == TopologyOperation::Delete {
            PolicyDecision::Allow
        } else {
            PolicyDecision::Deny {
                reason: "Delete only".into(),
            }
        })
    }
}

struct LegacyOnlyPolicy;
impl RuntimePolicyHook for LegacyOnlyPolicy {
    fn evaluate(
        &self,
        _: RuntimeOperation,
        _: &RequestMetadata,
    ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
        Ok(PolicyDecision::Allow)
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    daemon: Arc<RuntimeDaemon>,
    initial: ProjectState,
    stores: BTreeMap<MachineId, PathBuf>,
}

impl Fixture {
    fn new(policy: Arc<dyn RuntimePolicyHook>) -> Self {
        Self::build(policy, false, false)
    }

    fn with_extra_ownership(policy: Arc<dyn RuntimePolicyHook>, extra: bool) -> Self {
        Self::build(policy, extra, false)
    }

    /// A fixture whose definition declares one network, one endpoint and one
    /// attachment per Machine, so `instantiate_environment` emits the three
    /// fabric ownership kinds Delete must expect and reclaim.
    fn networked(policy: Arc<dyn RuntimePolicyHook>) -> Self {
        Self::build(policy, false, true)
    }

    fn build(policy: Arc<dyn RuntimePolicyHook>, extra: bool, networked: bool) -> Self {
        let root = tempfile::Builder::new()
            .prefix("vz-del-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let root_path = root.path().canonicalize().unwrap();
        let runtime = root_path.join("r");
        fs::DirBuilder::new().mode(0o700).create(&runtime).unwrap();
        let config = RuntimedConfig {
            state_store_path: root_path.join("state.db"),
            runtime_data_dir: runtime,
            socket_path: root_path.join("d.sock"),
        };
        let definition = ProjectDefinition {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: ProjectId::generate(),
            name: "delete-tests".into(),
            environment: EnvironmentSpec {
                host_exports: Vec::new(),
                host_imports: Vec::new(),
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                default_machine: None,
                machines: ["app", "worker"]
                    .map(|name| MachineSpec {
                        networks: if networked {
                            vec!["private".to_string()]
                        } else {
                            Vec::new()
                        },
                        egress: Default::default(),
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        name: name.into(),
                        // Hardened Machines may not declare network attachments.
                        profile: if networked {
                            MachineProfile::Developer
                        } else {
                            MachineProfile::Hardened
                        },
                        target: TargetSpec {
                            os: OperatingSystem::Linux,
                            arch: Architecture::Aarch64,
                            image: "vz-linux-appliance".into(),
                            version: None,
                            channel: None,
                            digest: None,
                        },
                        resources: MachineResources::default(),
                        requested_capabilities: CapabilitySet::default(),
                        workspace: None,
                    })
                    .to_vec(),
                networks: if networked {
                    vec![NetworkSpec {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        name: "private".into(),
                        kind: NetworkKind::Private,
                        cidr: None,
                    }]
                } else {
                    vec![]
                },
                endpoints: if networked {
                    vec![EndpointSpec {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        name: "api".into(),
                        machine: "app".into(),
                        network: "private".into(),
                        protocol: EndpointProtocol::Tcp,
                        port: 8080,
                        hostname: None,
                    }]
                } else {
                    vec![]
                },
            },
        };
        let mut project = ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            environments: ["first", "sibling"]
                .map(|name| definition.instantiate_environment(name, 1).unwrap())
                .to_vec(),
            definition,
        };
        for environment in &mut project.environments {
            environment.state = EnvironmentState::Failed;
            for machine in &mut environment.machines {
                machine.state = MachineState::Failed;
                let owner = ResourceOwner {
                    project_id: environment.project_id.clone(),
                    environment_id: environment.environment_id.clone(),
                    machine_id: Some(machine.machine_id.clone()),
                };
                environment.ownership.push(
                    MachineRuntimeRegistry::<vz_oci_macos::MacosRuntimeBackend>::reservation(
                        &owner,
                    )
                    .unwrap(),
                );
                environment.ownership.push(
                    MachineRuntimeEntry::<crate::machine_backend::MachineBackendRuntime>::vm_reservation(
                        &owner,
                    )
                    .unwrap(),
                );
            }
        }
        if extra {
            let environment = &mut project.environments[0];
            environment.ownership.push(OwnershipRecord {
                schema_version: 1,
                resource_kind: OwnedResourceKind::Other("unimplemented-host-service".into()),
                resource_id: "foreign-cleanup-adapter-required".into(),
                environment_id: environment.environment_id.clone(),
                machine_id: Some(environment.machines[0].machine_id.clone()),
            });
        }
        let store = vz_stack::StateStore::open(&config.state_store_path).unwrap();
        store.save_project_state(&project).unwrap();
        // These fixture-issued acknowledgements establish controller authority
        // for deliberately runtime-free stores, not that a VM was tested.
        for environment in &project.environments {
            let operation = store
                .begin_environment_lifecycle(
                    environment.environment_id.as_str(),
                    EnvironmentLifecycleKind::Stop,
                    &format!("req-stop-{}", environment.name),
                    &format!("idem-stop-{}", environment.name),
                    &format!("sha256:{}", "a".repeat(64)),
                    2,
                )
                .unwrap();
            acknowledge_machines(&store, operation.clone(), 3);
            store
                .finish_environment_lifecycle(
                    operation.operation_id.as_str(),
                    operation.generation,
                    4,
                )
                .unwrap();
        }
        drop(store);
        let daemon = Arc::new(RuntimeDaemon::start_with_policy_hook(config, policy, None).unwrap());
        let initial = daemon
            .with_state_store(|store| {
                store.load_project_state_snapshot(project.definition.project_id.as_str())
            })
            .unwrap()
            .unwrap();
        let mut stores = BTreeMap::new();
        for environment in &initial.environments {
            for machine in &environment.machines {
                let owner = ResourceOwner {
                    project_id: environment.project_id.clone(),
                    environment_id: environment.environment_id.clone(),
                    machine_id: Some(machine.machine_id.clone()),
                };
                let reservation =
                    MachineRuntimeRegistry::<vz_oci_macos::MacosRuntimeBackend>::reservation(
                        &owner,
                    )
                    .unwrap();
                let lease = daemon
                    .machine_runtime_registry()
                    .acquire_store(
                        &owner,
                        &reservation,
                        Some(&format!("sha256:{}", "b".repeat(64))),
                        MachineRuntimeAdmission::CreateOrOpen,
                    )
                    .unwrap();
                let marker = lease.data_path().join("fixture-persistence");
                fs::write(&marker, machine.machine_id.as_str()).unwrap();
                fs::set_permissions(&marker, fs::Permissions::from_mode(0o600)).unwrap();
                stores.insert(machine.machine_id.clone(), lease.data_path().to_path_buf());
            }
        }
        Self {
            _root: root,
            daemon,
            initial,
            stores,
        }
    }

    fn input(&self) -> DeleteEnvironmentInput {
        DeleteEnvironmentInput {
            project_id: self.initial.definition.project_id.clone(),
            selection: EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::NameOrId("first".into())),
                ..Default::default()
            },
            metadata: RequestMetadata::new(
                Some("req-delete-test".into()),
                Some("idem-delete-test".into()),
            ),
            machine_timeout: Duration::from_secs(1),
        }
    }

    fn snapshot(&self) -> ProjectState {
        self.daemon
            .with_state_store(|store| {
                store.load_project_state_snapshot(self.initial.definition.project_id.as_str())
            })
            .unwrap()
            .unwrap()
    }

    fn first(&self) -> &EnvironmentInstance {
        self.initial
            .environments
            .iter()
            .find(|environment| environment.name == "first")
            .unwrap()
    }

    fn assert_no_delete(&self, before: &ProjectState, files: &BTreeMap<PathBuf, Vec<u8>>) {
        assert_eq!(&self.snapshot(), before);
        assert_eq!(&tree(&self.daemon.config.runtime_data_dir), files);
        assert!(
            self.daemon
                .with_state_store(
                    |store| store.load_environment_lifecycle_by_idempotency_key("idem-delete-test")
                )
                .unwrap()
                .is_none()
        );
    }
}

fn acknowledge_machines(
    store: &vz_stack::StateStore,
    mut operation: EnvironmentLifecycleOperation,
    now: u64,
) -> EnvironmentLifecycleOperation {
    for step in operation.machine_steps.clone() {
        operation = store
            .acknowledge_environment_machine_step(
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
                now,
            )
            .unwrap();
    }
    operation
}

fn tree(path: &std::path::Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(
        root: &std::path::Path,
        path: &std::path::Path,
        result: &mut BTreeMap<PathBuf, Vec<u8>>,
    ) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).unwrap();
            assert!(!metadata.file_type().is_symlink());
            if metadata.is_dir() {
                result.insert(path.strip_prefix(root).unwrap().to_path_buf(), vec![]);
                visit(root, &path, result);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(path, path, &mut result);
    result
}

// Process-local environment isolation, without unsafe set_var in a threaded
// test binary. Re-exec only this exact host unit test, never Cargo or a daemon.
fn isolated(test: &str) -> bool {
    if std::env::var("VZ_DELETE_HOST_TEST_CHILD").ok().as_deref() == Some(test) {
        return false;
    }
    let config = tempfile::Builder::new()
        .prefix("vz-del-config-")
        .tempdir_in("/private/tmp")
        .unwrap();
    fs::set_permissions(config.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let name = format!("environment_delete::tests::{test}");
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &name, "--nocapture", "--test-threads=1"])
        .env("VZ_DELETE_HOST_TEST_CHILD", test)
        .env("VZ_DOCKER_CONFIG", config.path())
        .env_remove("DOCKER_CONFIG")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "isolated host test failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed; 0 ignored"),
        "exact isolated test was not executed"
    );
    true
}

async fn terminal(
    mut receiver: watch::Receiver<Progress>,
) -> Result<DeleteEnvironmentProgress, MachineError> {
    tokio::time::timeout(Duration::from_secs(10), async move {
        loop {
            let current = receiver.borrow_and_update().clone()?;
            if current.terminal {
                return Ok(current);
            }
            receiver
                .changed()
                .await
                .expect("Delete must publish a terminal result before closing");
        }
    })
    .await
    .expect("bounded host Delete completion")
}

#[tokio::test]
async fn legacy_authority_defaults_to_delete_denied_before_journal_or_filesystem_changes() {
    let fixture = Fixture::new(Arc::new(LegacyOnlyPolicy));
    let files = tree(&fixture.daemon.config.runtime_data_dir);
    let error = fixture
        .daemon
        .delete_environment(fixture.input())
        .await
        .unwrap_err();
    assert_eq!(error.code, MachineErrorCode::PolicyDenied);
    assert_eq!(error.request_id.as_deref(), Some("req-delete-test"));
    fixture.assert_no_delete(&fixture.initial, &files);
}

#[tokio::test]
async fn stale_explicit_selector_never_falls_back_to_valid_process_environment() {
    let fixture = Fixture::new(Arc::new(DeleteOnlyPolicy::default()));
    let files = tree(&fixture.daemon.config.runtime_data_dir);
    let mut input = fixture.input();
    input.selection.explicit = Some(EnvironmentSelector::Id(
        vz_runtime_contract::EnvironmentId::generate(),
    ));
    input.selection.process_environment_id = Some(fixture.first().environment_id.clone());
    assert_eq!(
        fixture
            .daemon
            .delete_environment(input)
            .await
            .unwrap_err()
            .code,
        MachineErrorCode::StateConflict
    );
    fixture.assert_no_delete(&fixture.initial, &files);
}

#[tokio::test]
async fn changed_idempotent_delete_input_cannot_retarget_or_change_timeout() {
    let fixture = Fixture::new(Arc::new(DeleteOnlyPolicy::default()));
    let input = fixture.input();
    fixture
        .daemon
        .with_state_store(|store| {
            store.begin_environment_lifecycle(
                fixture.first().environment_id.as_str(),
                EnvironmentLifecycleKind::Delete,
                input.metadata.request_id.as_deref().unwrap(),
                input.metadata.idempotency_key.as_deref().unwrap(),
                &request_hash(&input, &fixture.first().environment_id).unwrap(),
                10,
            )
        })
        .unwrap();
    let before = fixture.snapshot();
    let files = tree(&fixture.daemon.config.runtime_data_dir);
    for variation in 0..3 {
        let mut changed = input.clone();
        match variation {
            0 => changed.machine_timeout += Duration::from_secs(1),
            1 => changed.selection.explicit = Some(EnvironmentSelector::NameOrId("sibling".into())),
            2 => changed.metadata.request_id = Some("req-different".into()),
            _ => unreachable!(),
        }
        assert_eq!(
            fixture
                .daemon
                .delete_environment(changed)
                .await
                .unwrap_err()
                .code,
            MachineErrorCode::StateConflict
        );
        assert_eq!(fixture.snapshot(), before);
        assert_eq!(tree(&fixture.daemon.config.runtime_data_dir), files);
    }
}

#[tokio::test]
async fn unsupported_owned_resource_fails_all_sibling_preflight_without_effects() {
    let fixture = Fixture::with_extra_ownership(Arc::new(DeleteOnlyPolicy::default()), true);
    let before = fixture.snapshot();
    let files = tree(&fixture.daemon.config.runtime_data_dir);
    assert_eq!(
        fixture
            .daemon
            .delete_environment(fixture.input())
            .await
            .unwrap_err()
            .code,
        MachineErrorCode::UnsupportedOperation
    );
    fixture.assert_no_delete(&before, &files);
}

#[tokio::test]
async fn foreign_store_owner_fails_all_machine_preflight_before_any_cleanup() {
    let fixture = Fixture::new(Arc::new(DeleteOnlyPolicy::default()));
    let selected = fixture.first();
    let sibling = fixture
        .initial
        .environments
        .iter()
        .find(|e| e.name == "sibling")
        .unwrap();
    let selected_store = fixture.stores[&selected.machines[1].machine_id]
        .parent()
        .unwrap();
    let sibling_store = fixture.stores[&sibling.machines[0].machine_id]
        .parent()
        .unwrap();
    fs::write(
        selected_store.join("owner.json"),
        fs::read(sibling_store.join("owner.json")).unwrap(),
    )
    .unwrap();
    let before = fixture.snapshot();
    let files = tree(&fixture.daemon.config.runtime_data_dir);
    assert_eq!(
        fixture
            .daemon
            .delete_environment(fixture.input())
            .await
            .unwrap_err()
            .code,
        MachineErrorCode::StateConflict
    );
    fixture.assert_no_delete(&before, &files);
}

#[tokio::test]
async fn dropped_observer_does_not_cancel_admitted_delete() {
    if isolated("dropped_observer_does_not_cancel_admitted_delete") {
        return;
    }
    let fixture = Fixture::new(Arc::new(DeleteOnlyPolicy::default()));
    let receiver = fixture
        .daemon
        .delete_environment(fixture.input())
        .await
        .unwrap();
    let admitted = receiver.borrow().as_ref().unwrap().operation.clone();
    drop(receiver);
    let tombstone = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(tombstone) = fixture
                .daemon
                .with_state_store(|store| {
                    store.load_environment_tombstone(admitted.environment_id.as_str())
                })
                .unwrap()
            {
                break tombstone;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retained Delete supervisor must complete without an observer");
    assert_eq!(tombstone.delete_operation_id, admitted.operation_id);
    for machine in &fixture.first().machines {
        assert!(!fixture.stores[&machine.machine_id].exists());
    }
    let replay = terminal(
        fixture
            .daemon
            .delete_environment(fixture.input())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(replay.tombstone, Some(tombstone));
}

#[tokio::test]
async fn stopped_controller_deletes_only_owned_stores_and_preserves_sibling() {
    if isolated("stopped_controller_deletes_only_owned_stores_and_preserves_sibling") {
        return;
    }
    let policy = Arc::new(DeleteOnlyPolicy::default());
    let fixture = Fixture::new(policy.clone());
    let sibling = fixture
        .initial
        .environments
        .iter()
        .find(|environment| environment.name == "sibling")
        .unwrap()
        .clone();
    let docker_config = PathBuf::from(std::env::var_os("VZ_DOCKER_CONFIG").unwrap());
    let config_before = tree(&docker_config);
    let outcome = terminal(
        fixture
            .daemon
            .delete_environment(fixture.input())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(outcome.terminal && outcome.error.is_none());
    assert_eq!(
        outcome.operation.status,
        EnvironmentLifecycleStatus::Succeeded
    );
    assert!(
        outcome
            .operation
            .machine_steps
            .iter()
            .all(
                |step| step.status == LifecycleStepStatus::Succeeded && step.target_state.is_none()
            )
    );
    assert!(
        outcome
            .operation
            .cleanup_steps
            .iter()
            .all(|step| step.status == LifecycleStepStatus::Succeeded)
    );
    outcome
        .tombstone
        .as_ref()
        .unwrap()
        .validate_for_operation(&outcome.operation)
        .unwrap();
    assert_eq!(fixture.snapshot().environments, vec![sibling.clone()]);
    for machine in &fixture.first().machines {
        assert!(!fixture.stores[&machine.machine_id].exists());
    }
    for machine in &sibling.machines {
        assert_eq!(
            fs::read(fixture.stores[&machine.machine_id].join("fixture-persistence")).unwrap(),
            machine.machine_id.as_str().as_bytes()
        );
    }
    assert_eq!(tree(&docker_config), config_before);
    let scopes = policy.scopes.lock().unwrap();
    assert!(!scopes.is_empty());
    let mut expected = fixture
        .first()
        .machines
        .iter()
        .map(|machine| machine.machine_id.clone())
        .collect::<Vec<_>>();
    expected.sort();
    assert!(
        scopes
            .iter()
            .all(|scope| scope.operation == TopologyOperation::Delete
                && scope.project_id == fixture.initial.definition.project_id
                && scope.environment_id == fixture.first().environment_id
                && scope.machine_ids == expected
                && scope.definition_digest == fixture.first().definition_digest)
    );
}

/// Every declared-fabric ownership kind belongs to Delete's expected set, so a
/// networked Environment is admitted, its switch is reclaimed, and every cleanup
/// step -- including the Environment-scoped network -- is acknowledged.
#[tokio::test(flavor = "multi_thread")]
async fn declared_fabric_is_expected_and_delete_reclaims_switches_and_records() {
    if isolated("declared_fabric_is_expected_and_delete_reclaims_switches_and_records") {
        return;
    }
    let fixture = Fixture::networked(Arc::new(DeleteOnlyPolicy::default()));
    let selected = fixture.first().clone();
    let sibling = fixture
        .initial
        .environments
        .iter()
        .find(|environment| environment.name == "sibling")
        .unwrap()
        .clone();
    let kinds = selected
        .ownership
        .iter()
        .map(|record| record.resource_kind.clone())
        .collect::<Vec<_>>();
    for kind in [
        OwnedResourceKind::Network,
        OwnedResourceKind::Endpoint,
        OwnedResourceKind::NetworkAttachment,
    ] {
        assert!(kinds.contains(&kind), "fixture must emit {kind:?}");
    }
    validate_supported(&fixture.input(), &selected).unwrap();

    let owner = ResourceOwner {
        project_id: selected.project_id.clone(),
        environment_id: selected.environment_id.clone(),
        machine_id: None,
    };
    let lease = fixture
        .daemon
        .acquire_environment_controller(&owner.project_id, &owner.environment_id)
        .await
        .unwrap();
    let members = selected
        .machines
        .iter()
        .enumerate()
        .map(|(index, machine)| {
            (
                crate::environment_switch::PortId(index as u32 + 1),
                crate::environment_switch::MacAddress::derive(
                    selected.environment_id.as_str(),
                    machine.machine_id.as_str(),
                    "private",
                ),
            )
        })
        .collect::<Vec<_>>();
    let (switch, guests) =
        crate::environment_switch::runtime::NetworkSwitch::start("declared", members).unwrap();
    drop(guests);
    fixture
        .daemon
        .environment_switches()
        .install(&lease, &owner, "private", switch, None)
        .await
        .unwrap();
    drop(lease);

    let outcome = terminal(
        fixture
            .daemon
            .delete_environment(fixture.input())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(outcome.terminal && outcome.error.is_none());
    assert_eq!(
        outcome.operation.status,
        EnvironmentLifecycleStatus::Succeeded
    );
    // The Environment-scoped network step has no Machine to be acknowledged
    // with, so a Delete that never acknowledged it would hang here instead.
    assert!(
        outcome
            .operation
            .cleanup_steps
            .iter()
            .any(
                |step| step.ownership.resource_kind == OwnedResourceKind::Network
                    && step.ownership.machine_id.is_none()
            )
    );
    assert!(
        outcome
            .operation
            .cleanup_steps
            .iter()
            .all(|step| step.status == LifecycleStepStatus::Succeeded)
    );
    outcome
        .tombstone
        .as_ref()
        .unwrap()
        .validate_for_operation(&outcome.operation)
        .unwrap();
    assert!(
        fixture
            .daemon
            .environment_switches()
            .networks(&selected.environment_id)
            .await
            .is_empty(),
        "Delete must reclaim every switch it owned"
    );
    assert_eq!(fixture.snapshot().environments, vec![sibling]);
}

/// Adding three kinds to the expected set is not a widening of it: every kind
/// outside the set is still an unaccounted resource and a hard refusal.
#[test]
fn ownership_outside_the_expected_set_is_still_refused() {
    let fixture = Fixture::networked(Arc::new(DeleteOnlyPolicy::default()));
    for kind in [
        OwnedResourceKind::Disk,
        OwnedResourceKind::Socket,
        OwnedResourceKind::HostExport,
        OwnedResourceKind::HostImport,
        OwnedResourceKind::PortRange,
        OwnedResourceKind::Credential,
        OwnedResourceKind::Fault,
        OwnedResourceKind::LegacySandbox,
        OwnedResourceKind::Other("unimplemented-adapter".into()),
    ] {
        let mut environment = fixture.first().clone();
        environment.ownership.push(OwnershipRecord {
            schema_version: 1,
            resource_kind: kind.clone(),
            resource_id: "unaccounted".into(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(environment.machines[0].machine_id.clone()),
        });
        assert_eq!(
            validate_supported(&fixture.input(), &environment)
                .unwrap_err()
                .code,
            MachineErrorCode::UnsupportedOperation,
            "{kind:?} must remain unaccounted for by Delete"
        );
    }
}

/// Delete's expected set must account for a host export, or an Environment that
/// declared one could never be deleted.
///
/// This is the accounted half of `ownership_outside_the_expected_set_is_still_refused`
/// above, which still refuses an export record with no instance behind it. Both
/// have to hold together: without the positive, "Delete refuses an export" would
/// be indistinguishable from "Delete never learned about exports", which is what
/// it meant before `authorize_ownership` began admitting them at Up. Delete would
/// then have refused every Environment Up had just successfully created.
#[test]
fn an_accounted_host_export_is_expected_and_an_unaccounted_one_is_not() {
    let fixture = Fixture::networked(Arc::new(DeleteOnlyPolicy::default()));
    let mut accounted = fixture.first().clone();
    let machine_id = accounted.machines[0].machine_id.clone();
    let export = vz_runtime_contract::HostExportInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        export_id: vz_runtime_contract::HostExportId::generate(),
        environment_id: accounted.environment_id.clone(),
        machine_id: machine_id.clone(),
        name: "api".into(),
    };
    let record = OwnershipRecord {
        schema_version: 1,
        resource_kind: OwnedResourceKind::HostExport,
        resource_id: export.export_id.to_string(),
        environment_id: accounted.environment_id.clone(),
        machine_id: Some(machine_id.clone()),
    };
    accounted.host_exports.push(export.clone());
    accounted.ownership.push(record.clone());
    validate_supported(&fixture.input(), &accounted)
        .expect("an export with its ownership edge is exactly accounted for");

    // The record without its instance: the unaccounted resource the whole check
    // exists to refuse.
    let mut orphan_record = accounted.clone();
    orphan_record.host_exports.clear();
    assert_eq!(
        validate_supported(&fixture.input(), &orphan_record)
            .unwrap_err()
            .code,
        MachineErrorCode::UnsupportedOperation
    );

    // The instance without its record: nothing would ever reclaim the listener.
    let mut orphan_instance = accounted.clone();
    orphan_instance
        .ownership
        .retain(|candidate| candidate.resource_kind != OwnedResourceKind::HostExport);
    assert_eq!(
        validate_supported(&fixture.input(), &orphan_instance)
            .unwrap_err()
            .code,
        MachineErrorCode::UnsupportedOperation
    );

    // An export attributed to a Machine outside this Environment: its cleanup
    // step is dispatched with that Machine's store, so it could never run.
    let mut foreign = accounted.clone();
    let absent = MachineId::generate();
    foreign.host_exports[0].machine_id = absent.clone();
    assert_eq!(
        validate_supported(&fixture.input(), &foreign)
            .unwrap_err()
            .code,
        MachineErrorCode::StateConflict
    );

    // A repeated export identity is refused, never deduplicated.
    let mut duplicated = accounted.clone();
    duplicated.host_exports.push(export);
    duplicated.ownership.push(record);
    assert_eq!(
        validate_supported(&fixture.input(), &duplicated)
            .unwrap_err()
            .code,
        MachineErrorCode::StateConflict
    );
}

/// The expected set is derived from the persisted instances, so a fabric
/// ownership record without its instance, and an instance without its record,
/// are both refused rather than silently reclaimed.
#[test]
fn fabric_ownership_and_instances_must_correspond_exactly() {
    let fixture = Fixture::networked(Arc::new(DeleteOnlyPolicy::default()));
    validate_supported(&fixture.input(), fixture.first()).unwrap();

    // One Network ownership record with no NetworkInstance to justify it: the
    // set no longer balances, so nothing is admitted.
    let mut orphan_record = fixture.first().clone();
    orphan_record.networks.clear();
    orphan_record.endpoints.clear();
    orphan_record.network_attachments.clear();
    orphan_record.ownership.retain(|record| {
        !matches!(
            record.resource_kind,
            OwnedResourceKind::Endpoint | OwnedResourceKind::NetworkAttachment
        )
    });
    assert_eq!(
        validate_supported(&fixture.input(), &orphan_record)
            .unwrap_err()
            .code,
        MachineErrorCode::UnsupportedOperation
    );

    // An endpoint left pointing at a network that is gone is refused before the
    // set is even compared, because its cleanup could never be attributed.
    let mut dangling_network = fixture.first().clone();
    dangling_network.networks.clear();
    assert_eq!(
        validate_supported(&fixture.input(), &dangling_network)
            .unwrap_err()
            .code,
        MachineErrorCode::StateConflict
    );

    let mut orphan_instance = fixture.first().clone();
    orphan_instance
        .ownership
        .retain(|record| record.resource_kind != OwnedResourceKind::NetworkAttachment);
    assert_eq!(
        validate_supported(&fixture.input(), &orphan_instance)
            .unwrap_err()
            .code,
        MachineErrorCode::UnsupportedOperation
    );

    // An endpoint attributed to a Machine outside this Environment could never
    // have its cleanup step dispatched, so it is refused before any effect.
    let mut foreign_endpoint = fixture.first().clone();
    let foreign = MachineId::generate();
    foreign_endpoint.endpoints[0].machine_id = foreign.clone();
    for record in &mut foreign_endpoint.ownership {
        if record.resource_kind == OwnedResourceKind::Endpoint {
            record.machine_id = Some(foreign.clone());
        }
    }
    assert_eq!(
        validate_supported(&fixture.input(), &foreign_endpoint)
            .unwrap_err()
            .code,
        MachineErrorCode::StateConflict
    );
}

/// The expected-ownership comparison is only exact while `expected` holds no
/// duplicate, and that property is enforced rather than assumed. Two persisted
/// instances minting one identical record would otherwise let the length match
/// an ownership list that carries an unaccounted resource in the spare slot.
#[test]
fn a_duplicated_expected_record_cannot_absorb_an_unaccounted_resource() {
    let fixture = Fixture::networked(Arc::new(DeleteOnlyPolicy::default()));
    let mut environment = fixture.first().clone();
    // Two NetworkInstances with one identity: `expected` gains a second,
    // identical Network record without the ownership list growing.
    let duplicate = environment.networks[0].clone();
    environment.networks.push(duplicate);
    // The slot that duplicate would otherwise account for, filled by a kind
    // Delete has no adapter for.
    environment.ownership.push(OwnershipRecord {
        schema_version: 1,
        resource_kind: OwnedResourceKind::Fault,
        resource_id: "unaccounted-behind-a-duplicate".into(),
        environment_id: environment.environment_id.clone(),
        machine_id: Some(environment.machines[0].machine_id.clone()),
    });
    let error = validate_supported(&fixture.input(), &environment)
        .expect_err("a duplicated expected record must never admit an unaccounted resource");
    assert_eq!(error.code, MachineErrorCode::StateConflict);
}

/// A duplicate in the persisted ownership list is state corruption, not a
/// resource Delete may reclaim twice.
#[test]
fn a_duplicated_ownership_record_is_refused() {
    let fixture = Fixture::networked(Arc::new(DeleteOnlyPolicy::default()));
    let mut environment = fixture.first().clone();
    let duplicate = environment
        .ownership
        .iter()
        .find(|record| record.resource_kind == OwnedResourceKind::Network)
        .unwrap()
        .clone();
    environment.ownership.push(duplicate);
    assert_eq!(
        validate_supported(&fixture.input(), &environment)
            .unwrap_err()
            .code,
        MachineErrorCode::UnsupportedOperation
    );
}

/// Seed only genuine lifecycle transitions. Cleanup acknowledgements here are
/// fixture inputs for replay tests, not evidence that any VM or disk was removed.
fn acknowledge_delete_fixture(fixture: &Fixture, finish: bool) -> EnvironmentLifecycleOperation {
    let input = fixture.input();
    fixture
        .daemon
        .with_state_store(|store| {
            let operation = store.begin_environment_lifecycle(
                fixture.first().environment_id.as_str(),
                EnvironmentLifecycleKind::Delete,
                input.metadata.request_id.as_deref().unwrap(),
                input.metadata.idempotency_key.as_deref().unwrap(),
                &request_hash(&input, &fixture.first().environment_id).unwrap(),
                10,
            )?;
            let mut operation = acknowledge_machines(store, operation, 11);
            for step in operation.cleanup_steps.clone() {
                operation = store.acknowledge_environment_cleanup_step(
                    &OwnershipCleanupStepAcknowledgement {
                        operation_id: operation.operation_id.clone(),
                        generation: operation.generation,
                        ownership: step.ownership,
                        result: LifecycleStepResult::Succeeded,
                    },
                    12,
                )?;
            }
            assert_eq!(operation.status, EnvironmentLifecycleStatus::Running);
            assert!(operation.completed_at.is_none());
            if finish {
                Ok(store
                    .finish_environment_delete(
                        operation.operation_id.as_str(),
                        operation.generation,
                        13,
                    )?
                    .0)
            } else {
                Ok(operation)
            }
        })
        .unwrap()
}

#[tokio::test]
async fn completed_tombstone_replay_pins_original_identity_after_human_name_reuse() {
    let fixture = Fixture::new(Arc::new(DeleteOnlyPolicy::default()));
    let original = acknowledge_delete_fixture(&fixture, true);
    let replacement = fixture
        .daemon
        .with_state_store(|store| {
            store.resolve_or_reserve_environment_for_up(
                &fixture.initial.definition,
                &fixture.input().selection,
                20,
            )
        })
        .unwrap();
    let replacement = match replacement {
        vz_stack::EnvironmentUpReservation::Created { environment } => environment,
        _ => panic!("deleted name must reserve a new Environment identity"),
    };
    assert_ne!(replacement.environment_id, original.environment_id);
    let before = fixture.snapshot();
    let files = tree(&fixture.daemon.config.runtime_data_dir);
    let replay = terminal(
        fixture
            .daemon
            .delete_environment(fixture.input())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(replay.operation, original);
    assert_eq!(
        replay.tombstone.as_ref().unwrap().environment_id,
        original.environment_id
    );
    assert_ne!(
        replay.tombstone.as_ref().unwrap().environment_id,
        replacement.environment_id
    );
    assert_eq!(fixture.snapshot(), before);
    assert_eq!(tree(&fixture.daemon.config.runtime_data_dir), files);
}

#[tokio::test]
async fn all_acknowledgements_remain_running_and_replay_finishes_genuine_tombstone() {
    if isolated("all_acknowledgements_remain_running_and_replay_finishes_genuine_tombstone") {
        return;
    }
    let fixture = Fixture::new(Arc::new(DeleteOnlyPolicy::default()));
    let active = acknowledge_delete_fixture(&fixture, false);
    assert_eq!(active.status, EnvironmentLifecycleStatus::Running);
    assert!(
        fixture
            .daemon
            .with_state_store(
                |store| store.load_environment_tombstone(active.environment_id.as_str())
            )
            .unwrap()
            .is_none()
    );
    let result = terminal(
        fixture
            .daemon
            .delete_environment(fixture.input())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(result.operation.operation_id, active.operation_id);
    assert_eq!(result.operation.generation, active.generation);
    assert_eq!(
        result.operation.status,
        EnvironmentLifecycleStatus::Succeeded
    );
    result
        .tombstone
        .unwrap()
        .validate_for_operation(&result.operation)
        .unwrap();
}
