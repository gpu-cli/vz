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
    CapabilitySet, EnvironmentSelector, EnvironmentSpec, EnvironmentState, MachineProfile,
    MachineResources, MachineSpec, MachineState, ProjectDefinition, ProjectState, RuntimeOperation,
    RuntimePolicyHook, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
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
        Self::with_extra_ownership(policy, false)
    }

    fn with_extra_ownership(policy: Arc<dyn RuntimePolicyHook>, extra: bool) -> Self {
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
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                default_machine: None,
                machines: ["app", "worker"]
                    .map(|name| MachineSpec {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        name: name.into(),
                        profile: MachineProfile::Hardened,
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
                networks: vec![],
                endpoints: vec![],
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
