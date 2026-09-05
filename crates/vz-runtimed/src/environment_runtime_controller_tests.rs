#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::future::Future;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Waker};

use tempfile::TempDir;
use vz_runtime_contract::{
    Architecture, CapabilitySet, EnvironmentSpec, HostSpec, MachineCapability, MachineProfile,
    MachineResources, MachineSpec, OperatingSystem, ProjectDefinition, TOPOLOGY_SCHEMA_VERSION,
    TargetSpec,
};

use super::*;
use crate::machine_target_resolver::{LINUX_APPLIANCE_IMAGE, MachineTargetCatalog};

fn project_id(value: &str) -> ProjectId {
    ProjectId::new(value).expect("valid test Project ID")
}

fn environment_id(value: &str) -> EnvironmentId {
    EnvironmentId::new(value).expect("valid test Environment ID")
}

// Poll the actual acquisition future to register its waiter. A timing-based
// sleep could pass without ever trying to acquire the contended lock.
fn assert_pending<F: Future>(future: Pin<&mut F>) {
    let mut context = Context::from_waker(Waker::noop());
    assert!(future.poll(&mut context).is_pending());
}

#[tokio::test]
async fn a_lease_cannot_impersonate_another_daemon_controller() {
    let first = EnvironmentRuntimeController::default();
    let second = EnvironmentRuntimeController::default();
    let project = project_id("prj_controller_provenance");
    let environment = environment_id("env_controller_provenance");
    let lease = first.acquire(&project, &environment).await.unwrap();
    first.require_own_lease(&lease).unwrap();
    assert!(second.require_own_lease(&lease).is_err());
}

#[tokio::test]
async fn same_environment_serializes_even_with_another_claimed_project() {
    let controller = EnvironmentRuntimeController::default();
    let first_project = project_id("prj_controller_one");
    let other_project = project_id("prj_controller_other");
    let environment = environment_id("env_controller_shared");
    let first = controller
        .acquire(&first_project, &environment)
        .await
        .unwrap();
    let mut other = Box::pin(controller.acquire(&other_project, &environment));
    assert_pending(other.as_mut());

    drop(first);
    let second = tokio::time::timeout(Duration::from_secs(1), other)
        .await
        .expect("the serialized waiter must progress after release")
        .unwrap();
    let mut third = Box::pin(controller.acquire(&first_project, &environment));
    assert_pending(third.as_mut());
    drop(second);
    let _third = tokio::time::timeout(Duration::from_secs(1), third)
        .await
        .expect("the second lease must retain the same Environment fence")
        .unwrap();
}

#[tokio::test]
async fn distinct_environments_can_be_held_concurrently() {
    let controller = EnvironmentRuntimeController::default();
    let project = project_id("prj_controller_parallel");
    let first_id = environment_id("env_controller_first");
    let second_id = environment_id("env_controller_second");
    let first = controller.acquire(&project, &first_id).await.unwrap();
    let second = tokio::time::timeout(
        Duration::from_secs(1),
        controller.acquire(&project, &second_id),
    )
    .await
    .expect("an unrelated Environment must not wait for the first")
    .unwrap();
    let mut first_waiter = Box::pin(controller.acquire(&project, &first_id));
    let mut second_waiter = Box::pin(controller.acquire(&project, &second_id));
    assert_pending(first_waiter.as_mut());
    assert_pending(second_waiter.as_mut());
    drop(first);
    let _first_reopened = tokio::time::timeout(Duration::from_secs(1), first_waiter)
        .await
        .unwrap()
        .unwrap();
    assert_pending(second_waiter.as_mut());
    drop(second);
    let _second_reopened = tokio::time::timeout(Duration::from_secs(1), second_waiter)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn cancelling_waiters_does_not_fork_or_strand_the_environment_lock() {
    let controller = EnvironmentRuntimeController::default();
    let project = project_id("prj_controller_cancel");
    let environment = environment_id("env_controller_cancel");
    let holder = controller.acquire(&project, &environment).await.unwrap();
    let mut cancelled = Box::pin(controller.acquire(&project, &environment));
    assert_pending(cancelled.as_mut());
    let mut survivor = Box::pin(controller.acquire(&project, &environment));
    assert_pending(survivor.as_mut());
    drop(cancelled);
    // Acquiring again causes the weak-entry pruning path to run while both a
    // holder and a queued waiter still own this exact lock.
    let mut newcomer = Box::pin(controller.acquire(&project, &environment));
    assert_pending(newcomer.as_mut());
    drop(holder);
    let survivor = tokio::time::timeout(Duration::from_secs(1), survivor)
        .await
        .expect("cancelling the first waiter cannot strand the next")
        .unwrap();
    assert_pending(newcomer.as_mut());
    drop(survivor);
    let newcomer = tokio::time::timeout(Duration::from_secs(1), newcomer)
        .await
        .unwrap()
        .unwrap();
    drop(newcomer);
    let _after_last_drop = controller.acquire(&project, &environment).await.unwrap();
}

struct Fixture {
    _temp: TempDir,
    state: StateStore,
    registry: MachineRuntimeRegistry<MacosRuntimeBackend>,
    registry_root: PathBuf,
    resolver: MachineTargetResolver,
    project_id: ProjectId,
}

impl Fixture {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let project_id = project_id("prj_controller_prepare");
        let definition = ProjectDefinition {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: project_id.clone(),
            name: "controller-prepare".into(),
            environment: EnvironmentSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                default_machine: None,
                machines: vec![MachineSpec {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    name: "main".into(),
                    profile: MachineProfile::Developer,
                    target: TargetSpec {
                        os: OperatingSystem::Linux,
                        arch: Architecture::Aarch64,
                        image: LINUX_APPLIANCE_IMAGE.into(),
                        version: Some("0.4.0-test".into()),
                        channel: None,
                        digest: Some(format!("sha256:{}", "a".repeat(64))),
                    },
                    resources: MachineResources::default(),
                    requested_capabilities: CapabilitySet::new([MachineCapability::PosixExec]),
                    workspace: None,
                }],
                networks: Vec::new(),
                endpoints: Vec::new(),
            },
        };
        let environment = definition.instantiate_environment("tests", 100).unwrap();
        let state = StateStore::open(&root.join("topology.db")).unwrap();
        state
            .save_project_state(&ProjectState {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                definition,
                environments: vec![environment],
            })
            .unwrap();
        let registry_root = root.join("registry");
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&registry_root)
            .unwrap();
        let registry = MachineRuntimeRegistry::new(registry_root.clone()).unwrap();
        let resolver = MachineTargetResolver::new(
            HostSpec {
                os: OperatingSystem::Macos,
                arch: Architecture::Aarch64,
            },
            MachineTargetCatalog::default(),
        )
        .unwrap();
        Self {
            _temp: temp,
            state,
            registry,
            registry_root,
            resolver,
            project_id,
        }
    }

    fn snapshot(&self) -> ProjectState {
        self.state
            .load_project_state_snapshot(self.project_id.as_str())
            .unwrap()
            .unwrap()
    }

    fn assert_unchanged(&self, before: &ProjectState) {
        assert_eq!(&self.snapshot(), before);
        assert_eq!(fs::read_dir(&self.registry_root).unwrap().count(), 0);
    }
}

fn assert_state_conflict(
    result: Result<PreparedEnvironmentMachines, EnvironmentRuntimeControllerError>,
) {
    assert!(matches!(
        result,
        Err(EnvironmentRuntimeControllerError::State(
            StackError::Machine {
                code: MachineErrorCode::StateConflict,
                ..
            }
        ))
    ));
}

#[tokio::test]
async fn prepare_rejects_wrong_owner_before_reservations_or_filesystem_effects() {
    let fixture = Fixture::new();
    let before = fixture.snapshot();
    let expected = &before.environments[0];
    let controller = EnvironmentRuntimeController::default();
    for (project, environment) in [
        (
            project_id("prj_controller_wrong"),
            expected.environment_id.clone(),
        ),
        (
            expected.project_id.clone(),
            environment_id("env_controller_wrong"),
        ),
    ] {
        let lease = controller.acquire(&project, &environment).await.unwrap();
        assert_state_conflict(
            lease
                .prepare(
                    &fixture.state,
                    &fixture.registry,
                    &fixture.resolver,
                    expected,
                    101,
                )
                .await,
        );
        fixture.assert_unchanged(&before);
    }
}

#[tokio::test]
async fn prepare_rejects_stale_snapshot_before_reservations_or_filesystem_effects() {
    let fixture = Fixture::new();
    let before = fixture.snapshot();
    let mut stale = before.environments[0].clone();
    stale.updated_at += 1;
    let controller = EnvironmentRuntimeController::default();
    let lease = controller
        .acquire(&stale.project_id, &stale.environment_id)
        .await
        .unwrap();
    assert_state_conflict(
        lease
            .prepare(
                &fixture.state,
                &fixture.registry,
                &fixture.resolver,
                &stale,
                102,
            )
            .await,
    );
    fixture.assert_unchanged(&before);
}

#[tokio::test]
async fn fresh_catalog_rejection_precedes_reservations_and_filesystem_effects() {
    let fixture = Fixture::new();
    let before = fixture.snapshot();
    let expected = &before.environments[0];
    let controller = EnvironmentRuntimeController::default();
    let lease = controller
        .acquire(&expected.project_id, &expected.environment_id)
        .await
        .unwrap();
    let result = lease
        .prepare(
            &fixture.state,
            &fixture.registry,
            &fixture.resolver,
            expected,
            101,
        )
        .await;
    assert!(matches!(
        result,
        Err(EnvironmentRuntimeControllerError::Resolution(
            TargetResolutionError::TargetNotFound { .. }
        ))
    ));
    fixture.assert_unchanged(&before);
    assert!(
        fixture
            .state
            .load_current_environment_lifecycle(expected.environment_id.as_str())
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn post_begin_recovery_refuses_missing_store_without_catalog_or_creation() {
    let fixture = Fixture::new();
    let initial = fixture.snapshot();
    let environment = &initial.environments[0];
    for machine in &environment.machines {
        let owner = ResourceOwner {
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(machine.machine_id.clone()),
        };
        for record in reservations(&owner).unwrap() {
            fixture.state.reserve_owned_resource(&record, 101).unwrap();
        }
    }
    let operation = fixture
        .state
        .begin_environment_lifecycle(
            environment.environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "req-controller-recovery",
            "idem-controller-recovery",
            "sha256:controller-recovery",
            102,
        )
        .unwrap();
    let before = fixture.snapshot();
    let expected = &before.environments[0];
    assert_eq!(expected.lifecycle_generation, 1);
    assert!(expected.active_operation_id.is_some());
    let controller = EnvironmentRuntimeController::default();
    let lease = controller
        .acquire(&expected.project_id, &expected.environment_id)
        .await
        .unwrap();
    let result = lease
        .prepare(
            &fixture.state,
            &fixture.registry,
            &fixture.resolver,
            expected,
            103,
        )
        .await;
    // Reaching ExistingOnly's missing-store error despite an empty catalog
    // proves recovery did not fall back to resolving or provisioning a target.
    assert!(matches!(
        result,
        Err(EnvironmentRuntimeControllerError::Registry(
            MachineRuntimeRegistryError::NotFound(_)
        ))
    ));
    fixture.assert_unchanged(&before);
    assert_eq!(
        fixture
            .state
            .load_current_environment_lifecycle(expected.environment_id.as_str())
            .unwrap(),
        Some(operation)
    );
}
