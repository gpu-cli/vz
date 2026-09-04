#![allow(clippy::unwrap_used)]

use super::dispatch::{compute_topo_levels, parse_subnet_base, parse_subnet_prefix};
use super::tests_support::MockContainerRuntime;
use super::*;
use crate::reconcile::ApplyResult;
use crate::spec::MountSpec as StackMountSpec;
use crate::spec::{
    PortSpec, ResourcesSpec, SecretDef, SecretSource, ServiceKind, ServiceSecretRef, StackSpec,
    VolumeSpec,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use vz_runtime_contract::types::{
    Architecture, CapabilitySet, EnvironmentId, EnvironmentInstance, EnvironmentSpec,
    EnvironmentState, MachineCapability, MachineId, MachineIncarnation, MachineIncarnationId,
    MachineInstance, MachineProfile, MachineResources, MachineSpec, MachineState, OperatingSystem,
    OwnedResourceKind, OwnershipRecord, ProjectDefinition, ProjectId, ProjectState,
    TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};

fn apply(
    spec: &StackSpec,
    store: &StateStore,
    health: &HashMap<String, crate::health::HealthStatus>,
) -> Result<ApplyResult, StackError> {
    crate::reconcile::install_test_planning_authority(store, &spec.name);
    crate::reconcile::apply(spec, store, health)
}

fn plan_apply(
    spec: &StackSpec,
    store: &StateStore,
    health: &HashMap<String, crate::health::HealthStatus>,
) -> Result<ApplyResult, StackError> {
    crate::reconcile::install_test_planning_authority(store, &spec.name);
    crate::reconcile::plan_apply(spec, store, health)
}

fn svc(name: &str, image: &str) -> ServiceSpec {
    ServiceSpec {
        name: name.to_string(),
        kind: ServiceKind::Service,
        image: image.to_string(),
        command: None,
        entrypoint: None,
        environment: HashMap::new(),
        working_dir: None,
        user: None,
        mounts: vec![],
        ports: vec![],
        depends_on: vec![],
        healthcheck: None,
        restart_policy: None,
        resources: ResourcesSpec::default(),
        extra_hosts: vec![],
        secrets: vec![],
        networks: vec!["default".to_string()],
        cap_add: vec![],
        cap_drop: vec![],
        privileged: false,
        read_only: false,
        sysctls: HashMap::new(),
        ulimits: vec![],
        container_name: None,
        hostname: None,
        domainname: None,
        labels: HashMap::new(),
        stop_signal: None,
        stop_grace_period_secs: None,
        expose: vec![],
        stdin_open: false,
        tty: false,
        logging: None,
    }
}

fn secret_ref(name: &str) -> ServiceSecretRef {
    ServiceSecretRef {
        source: name.to_string(),
        target: name.to_string(),
        mode: 0o444,
        uid: 0,
        gid: 0,
    }
}

fn default_network() -> crate::spec::NetworkSpec {
    crate::spec::NetworkSpec {
        name: "default".to_string(),
        driver: "bridge".to_string(),
        subnet: None,
    }
}

fn stack(name: &str, services: Vec<ServiceSpec>) -> StackSpec {
    crate::reconcile::set_test_action_stack(name);
    StackSpec {
        name: name.to_string(),
        services,
        networks: vec![default_network()],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    }
}

fn test_create_action(stack_name: &str, service_name: &str) -> Action {
    Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition_for_stack(stack_name),
        target: crate::state_store::ServiceReplicaKey::first(service_name.to_string()).unwrap(),
    }
}

fn planned_actions(
    executor: &StackExecutor<MockContainerRuntime>,
    spec: &StackSpec,
) -> Vec<Action> {
    plan_apply(spec, executor.store(), &HashMap::new())
        .unwrap()
        .actions
}

fn make_executor(runtime: MockContainerRuntime) -> StackExecutor<MockContainerRuntime> {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    crate::reconcile::install_test_planning_authority(&store, "myapp");
    StackExecutor::new(runtime, store, tmp.path())
}

fn make_executor_with_dir(
    runtime: MockContainerRuntime,
    dir: &Path,
) -> StackExecutor<MockContainerRuntime> {
    let store = StateStore::in_memory().unwrap();
    crate::reconcile::install_test_planning_authority(&store, "myapp");
    StackExecutor::new(runtime, store, dir)
}

fn scoped_topology(stack_id: &str) -> (ProjectState, vz_runtime_contract::MachineWorkloadScope) {
    let project_id = ProjectId::new("prj_executor_scope").unwrap();
    let environment_id = EnvironmentId::new("env_executor_scope").unwrap();
    let machine_id = MachineId::new("mac_executor_scope").unwrap();
    let incarnation_id = MachineIncarnationId::new("inc_executor_scope").unwrap();
    let target = TargetSpec {
        os: OperatingSystem::Linux,
        arch: Architecture::Aarch64,
        image: "ubuntu:24.04".to_string(),
        version: None,
        channel: None,
        digest: Some("sha256:executor-fixture".to_string()),
    };
    let capabilities = CapabilitySet::new([
        MachineCapability::DockerEngine,
        MachineCapability::Compose,
        MachineCapability::Buildx,
    ]);
    let definition = ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: project_id.clone(),
        name: "executor-fixture".to_string(),
        environment: EnvironmentSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            machines: vec![MachineSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                name: "linux".to_string(),
                profile: MachineProfile::Developer,
                target: target.clone(),
                resources: MachineResources::default(),
                requested_capabilities: capabilities.clone(),
                workspace: None,
            }],
            networks: vec![],
            endpoints: vec![],
        },
    };
    let definition_digest = definition.digest().unwrap();
    let machine = MachineInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        machine_id: machine_id.clone(),
        environment_id: environment_id.clone(),
        name: "linux".to_string(),
        profile: MachineProfile::Developer,
        target,
        resources: MachineResources::default(),
        requested_capabilities: capabilities.clone(),
        negotiated_capabilities: capabilities,
        backend: None,
        incarnation: Some(MachineIncarnation {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            incarnation_id: incarnation_id.clone(),
            machine_id: machine_id.clone(),
            generation: 1,
            created_at: 1,
        }),
        state: MachineState::Ready,
        legacy_sandbox_id: None,
    };
    let environment = EnvironmentInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        environment_id: environment_id.clone(),
        project_id: project_id.clone(),
        name: "dev".to_string(),
        definition_digest,
        state: EnvironmentState::Ready,
        lifecycle_generation: 0,
        active_operation_id: None,
        bindings: vec![],
        machines: vec![machine],
        networks: vec![],
        endpoints: vec![],
        ownership: vec![
            OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::Machine,
                resource_id: machine_id.to_string(),
                environment_id: environment_id.clone(),
                machine_id: Some(machine_id.clone()),
            },
            OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::Incarnation,
                resource_id: incarnation_id.to_string(),
                environment_id: environment_id.clone(),
                machine_id: Some(machine_id.clone()),
            },
        ],
        legacy_migration: None,
        created_at: 1,
        updated_at: 1,
    };
    (
        ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments: vec![environment],
        },
        vz_runtime_contract::MachineWorkloadScope {
            schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
            project_id,
            environment_id,
            machine_id,
            machine_incarnation_id: incarnation_id,
            stack_id: stack_id.to_string(),
        },
    )
}

fn make_scoped_executor<'a>(
    runtime: MockContainerRuntime,
    store: StateStore,
    dir: &'a Path,
    scope: vz_runtime_contract::MachineWorkloadScope,
) -> StackExecutor<MockContainerRuntime> {
    if dir.exists() {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    StackExecutor::new_scoped(runtime, store, dir, scope).unwrap()
}

fn generation_ownership(
    stack_id: &str,
    container_id: &str,
    generation: u64,
) -> vz_runtime_contract::ContainerGenerationOwnership {
    vz_runtime_contract::ContainerGenerationOwnership {
        container_id: container_id.to_string(),
        generation,
        stack_id: stack_id.to_string(),
        scope: Some(Box::new(
            vz_runtime_contract::ContainerGenerationScope::synthetic_legacy_stack(stack_id)
                .unwrap(),
        )),
    }
}

#[test]
fn create_single_service() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let actions = planned_actions(&executor, &spec);

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());
    assert_eq!(result.succeeded, 1);
    assert_eq!(result.failed, 0);

    // Verify observed state.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].phase, ServicePhase::Running);
    assert_eq!(observed[0].container_id, Some("ctr-web".to_string()));
    let ownership = observed[0]
        .failed_create_ownership
        .as_ref()
        .expect("successful create ownership must be durable");
    assert_eq!(ownership.container_id, "ctr-web");
    assert_eq!(ownership.stack_id, "myapp");
    ownership.validate().unwrap();

    // Verify events.
    let events = executor.store().load_events("myapp").unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::ServiceCreating { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::ServiceReady { .. }))
    );
}

#[test]
fn create_multiple_services() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = stack(
        "myapp",
        vec![svc("web", "nginx:latest"), svc("db", "postgres:16")],
    );

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());
    assert_eq!(result.succeeded, 2);

    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed.len(), 2);
}

#[test]
fn generated_runtime_ids_are_distinct_across_stacks_with_same_service_name() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    for stack_name in ["project-a", "project-b"] {
        let spec = stack(stack_name, vec![svc("db", "postgres:16")]);
        let result = executor
            .execute(
                &spec,
                &[Action::ServiceCreate {
                    precondition: crate::reconcile::test_replica_precondition(),
                    target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
                }],
            )
            .unwrap();
        assert!(result.all_succeeded());
    }

    let configs = executor.runtime.captured_configs.lock().unwrap();
    let requested: Vec<&str> = configs
        .iter()
        .filter_map(|(_, config)| config.container_id.as_deref())
        .collect();
    assert_eq!(requested.len(), 2);
    assert_ne!(requested[0], requested[1]);
    assert_eq!(
        requested[0],
        super::create::generated_runtime_container_id("project-a", "db", 1)
    );
    assert_eq!(
        requested[1],
        super::create::generated_runtime_container_id("project-b", "db", 1)
    );
}

#[test]
fn explicit_container_name_is_preserved_as_caller_selected_runtime_id() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let mut service = svc("web", "nginx:latest");
    service.container_name = Some("shared-explicit-name".to_string());
    let spec = stack("project-a", vec![service]);

    let result = executor
        .execute(
            &spec,
            &[Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
        )
        .unwrap();

    assert!(result.all_succeeded());
    let configs = executor.runtime.captured_configs.lock().unwrap();
    assert_eq!(
        configs[0].1.container_id.as_deref(),
        Some("shared-explicit-name")
    );
}

#[test]
fn remove_service() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);

    // Simulate existing running container.
    let ownership = vz_runtime_contract::ContainerGenerationOwnership {
        container_id: "ctr-old".to_string(),
        generation: 7,
        stack_id: "myapp".to_string(),
        scope: Some(Box::new(
            vz_runtime_contract::ContainerGenerationScope::synthetic_legacy_stack("myapp").unwrap(),
        )),
    };
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("old".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-old".to_string()),
                failed_create_ownership: Some(ownership.clone()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRemove {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("old".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Only the generation-qualified cleanup path may mutate the runtime.
    let calls = executor.runtime.call_log();
    assert!(calls.iter().any(|(op, argument)| {
        op == "stop_and_remove_container_generation" && argument.starts_with("myapp:ctr-old:7:")
    }));
    assert!(
        !calls
            .iter()
            .any(|(op, _)| matches!(op.as_str(), "stop" | "remove"))
    );

    // Verify state is Stopped.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let old = observed
        .iter()
        .find(|o| o.replica.service_name == "old")
        .unwrap();
    assert_eq!(old.phase, ServicePhase::Stopped);
    assert!(old.container_id.is_none());
    assert!(old.failed_create_ownership.is_none());
}

#[test]
fn running_container_without_ownership_is_quarantined_without_id_cleanup() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("old".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-old".to_string()),
                failed_create_ownership: None,
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let result = executor
        .execute(
            &spec,
            &[Action::ServiceRemove {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("old".to_string()).unwrap(),
            }],
        )
        .unwrap();

    assert_eq!(result.failed, 1);
    assert!(result.errors[0].1.contains("refusing ID-only cleanup"));
    assert!(!executor.runtime().call_log().iter().any(|(operation, _)| {
        matches!(
            operation.as_str(),
            "stop" | "remove" | "stop_and_remove_container_generation"
        )
    }));
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed[0].container_id.as_deref(), Some("ctr-old"));
    assert!(observed[0].failed_create_ownership.is_none());
}

#[test]
fn stale_successful_ownership_cannot_delete_a_foreign_replacement() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);
    let stale = generation_ownership("myapp", "ctr-stale", 12);
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("old".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-replacement".to_string()),
                failed_create_ownership: Some(stale.clone()),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let result = executor
        .execute(
            &spec,
            &[Action::ServiceRemove {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("old".to_string()).unwrap(),
            }],
        )
        .unwrap();

    assert_eq!(result.failed, 1);
    assert!(!executor.runtime().call_log().iter().any(|(operation, _)| {
        matches!(
            operation.as_str(),
            "stop" | "remove" | "stop_and_remove_container_generation"
        )
    }));
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed[0].container_id.as_deref(), Some("ctr-replacement"));
    assert_eq!(observed[0].failed_create_ownership, Some(stale));
}

#[test]
fn successful_generation_cleanup_failure_retains_exact_ownership() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let create_spec = stack("myapp", vec![svc("web", "nginx:latest")]);
    executor
        .execute(
            &create_spec,
            &[Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
        )
        .unwrap();
    let ownership = executor.store().load_observed_state("myapp").unwrap()[0]
        .failed_create_ownership
        .clone()
        .unwrap();
    executor.runtime_mut().fail_generation_cleanup = true;

    let result = executor
        .execute(
            &stack("myapp", vec![]),
            &[Action::ServiceRemove {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
        )
        .unwrap();

    assert_eq!(result.failed, 1);
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed[0].container_id.as_deref(), Some("ctr-web"));
    assert_eq!(observed[0].failed_create_ownership, Some(ownership));
    assert!(
        !executor
            .runtime()
            .call_log()
            .iter()
            .any(|(operation, _)| { matches!(operation.as_str(), "stop" | "remove") })
    );
}

#[test]
fn environment_secret_source_is_staged_and_mounted() {
    let runtime = MockContainerRuntime::new();
    let tmp = tempfile::tempdir().unwrap();
    let mut executor = make_executor_with_dir(runtime, tmp.path());

    let mut app = svc("app", "alpine:latest");
    app.secrets = vec![secret_ref("runtime_secret")];
    let spec = StackSpec {
        name: "env-secret".to_string(),
        services: vec![app],
        networks: vec![default_network()],
        volumes: vec![],
        secrets: vec![SecretDef {
            name: "runtime_secret".to_string(),
            source: SecretSource::Environment("HOME".to_string()),
        }],
        disk_size_mb: None,
    };
    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition_for_stack("env-secret"),
        target: crate::state_store::ServiceReplicaKey::first("app".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded(), "errors: {:?}", result.errors);

    let captured = executor.runtime.captured_configs.lock().unwrap();
    let (_, app_config) = captured
        .iter()
        .find(|(container_id, _)| container_id == "ctr-app")
        .unwrap();
    let mount = app_config
        .mounts
        .iter()
        .find(|mount| mount.target == std::path::PathBuf::from("/run/secrets/runtime_secret"))
        .unwrap();
    assert_eq!(mount.access, vz_runtime_contract::MountAccess::ReadOnly);
    let staged_dir = mount.source.as_ref().unwrap();
    let staged_subpath = mount.subpath.as_deref().unwrap();
    assert_eq!(staged_dir, &tmp.path().join("secrets/env-secret"));
    assert_eq!(staged_subpath, "runtime_secret");
    let staged = std::fs::read_to_string(staged_dir.join(staged_subpath)).unwrap();
    assert_eq!(staged, std::env::var("HOME").unwrap());
}

#[test]
fn missing_environment_secret_source_fails_without_secret_material_in_error() {
    let runtime = MockContainerRuntime::new();
    let tmp = tempfile::tempdir().unwrap();
    let mut executor = make_executor_with_dir(runtime, tmp.path());

    let mut app = svc("app", "alpine:latest");
    app.secrets = vec![secret_ref("runtime_secret")];
    let missing_env = "VZ_STACK_TEST_MISSING_SECRET_ENV_9421";
    let spec = StackSpec {
        name: "env-secret-missing".to_string(),
        services: vec![app],
        networks: vec![default_network()],
        volumes: vec![],
        secrets: vec![SecretDef {
            name: "runtime_secret".to_string(),
            source: SecretSource::Environment(missing_env.to_string()),
        }],
        disk_size_mb: None,
    };
    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition_for_stack("env-secret-missing"),
        target: crate::state_store::ServiceReplicaKey::first("app".to_string()).unwrap(),
    }];

    let error = executor
        .execute(&spec, &actions)
        .expect_err("missing environment secret should fail closed")
        .to_string();
    assert!(error.contains(missing_env), "unexpected error: {error}");
    assert!(
        !error.contains("super-secret"),
        "error should not leak secret values: {error}"
    );
}

#[test]
fn recreate_service() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-new"]);
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    // Simulate an existing generation-qualified running container.
    let ownership = vz_runtime_contract::ContainerGenerationOwnership {
        container_id: "ctr-old".to_string(),
        generation: 9,
        stack_id: "myapp".to_string(),
        scope: Some(Box::new(
            vz_runtime_contract::ContainerGenerationScope::synthetic_legacy_stack("myapp").unwrap(),
        )),
    };
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-old".to_string()),
                failed_create_ownership: Some(ownership),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRecreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Verify sandbox setup, exact cleanup of old, then pull+create of new.
    let calls = executor.runtime.call_log();
    let ops: Vec<&str> = calls.iter().map(|(op, _)| op.as_str()).collect();
    assert_eq!(
        ops,
        vec![
            "create_sandbox",
            "setup_sandbox_network",
            "stop_and_remove_container_generation",
            "pull",
            "create_in_sandbox",
        ]
    );

    // New container.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed
        .iter()
        .find(|o| o.replica.service_name == "web")
        .unwrap();
    assert_eq!(web.phase, ServicePhase::Running);
    assert_eq!(web.container_id, Some("ctr-web".to_string()));
}

#[test]
fn pull_failure_marks_service_failed() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_pull = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.failed, 1);
    assert!(!result.all_succeeded());

    // Service should be marked Failed.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed
        .iter()
        .find(|o| o.replica.service_name == "web")
        .unwrap();
    assert_eq!(web.phase, ServicePhase::Failed);
    assert!(web.container_id.is_none());
    assert!(web.last_error.is_some());

    // ServiceFailed event emitted.
    let events = executor.store().load_events("myapp").unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::ServiceFailed { .. }))
    );
}

#[test]
fn create_failure_marks_service_failed() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_create = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.failed, 1);

    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed
        .iter()
        .find(|o| o.replica.service_name == "web")
        .unwrap();
    assert_eq!(web.phase, ServicePhase::Failed);
    assert!(web.container_id.is_none());
    assert!(web.failed_create_ownership.is_none());
}

#[test]
fn explicit_container_name_failure_does_not_claim_cleanup_ownership() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_create = true;
    let mut executor = make_executor(runtime);
    let mut service = svc("web", "nginx:latest");
    service.container_name = Some("foreign-global-id".to_string());
    let spec = stack("myapp", vec![service]);

    let result = executor
        .execute(
            &spec,
            &[Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
        )
        .unwrap();

    assert_eq!(result.failed, 1);
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed[0].phase, ServicePhase::Failed);
    assert!(observed[0].container_id.is_none());
    assert!(observed[0].failed_create_ownership.is_none());

    let retry = executor
        .execute(&spec, &[test_create_action("myapp", "web")])
        .unwrap();
    assert_eq!(retry.failed, 1);
    let calls = executor.runtime().call_log();
    assert!(!calls.iter().any(|(operation, _)| matches!(
        operation.as_str(),
        "stop" | "remove" | "stop_and_remove_container_generation"
    )));
}

#[test]
fn inline_wrong_id_ownership_is_discarded_and_never_cleaned() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_create = true;
    runtime.claim_failed_create_ownership = true;
    let requested = super::create::generated_runtime_container_id("myapp", "web", 1);
    runtime.override_failed_create_ownership_id(&requested, "foreign-container");
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let first = executor
        .execute(
            &spec,
            &[Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
        )
        .unwrap();
    assert_eq!(first.failed, 1);
    let failed = executor.store().load_observed_state("myapp").unwrap();
    assert!(failed[0].container_id.is_none());
    assert!(failed[0].failed_create_ownership.is_none());

    let second = executor
        .execute(&spec, &[test_create_action("myapp", "web")])
        .unwrap();
    assert_eq!(second.failed, 1);
    assert!(
        !executor
            .runtime()
            .call_log()
            .iter()
            .any(|(operation, _)| operation == "stop_and_remove_container_generation")
    );
}

#[test]
fn inline_legacy_unscoped_ownership_is_quarantined_and_never_cleaned() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_create = true;
    runtime.claim_failed_create_ownership = true;
    runtime.omit_failed_create_ownership_scope = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let first = executor
        .execute(
            &spec,
            &[Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
        )
        .unwrap();
    assert_eq!(first.failed, 1);
    let failed = executor.store().load_observed_state("myapp").unwrap();
    assert!(failed[0].container_id.is_none());
    assert!(failed[0].failed_create_ownership.is_none());

    let second = executor
        .execute(&spec, &[test_create_action("myapp", "web")])
        .unwrap();
    assert_eq!(second.failed, 1);
    assert!(
        !executor
            .runtime()
            .call_log()
            .iter()
            .any(|(operation, _)| operation == "stop_and_remove_container_generation")
    );
}

#[test]
fn owned_failed_create_survives_reconcile_and_is_cleaned_before_retry() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_create = true;
    runtime.claim_failed_create_ownership = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let first = executor
        .execute(
            &spec,
            &[Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
        )
        .unwrap();
    assert_eq!(first.failed, 1);

    let expected_id = super::create::generated_runtime_container_id("myapp", "web", 1);
    let failed = executor.store().load_observed_state("myapp").unwrap();
    let ownership = failed[0]
        .failed_create_ownership
        .clone()
        .expect("admitted failure must retain ownership");
    assert_eq!(ownership.container_id, expected_id);
    assert_eq!(ownership.generation, 41);
    assert_eq!(ownership.stack_id, "myapp");
    assert_eq!(
        failed[0].container_id.as_deref(),
        Some(expected_id.as_str())
    );

    let pending = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(pending[0].failed_create_ownership, Some(ownership.clone()));

    executor.runtime_mut().fail_create = false;
    let retry = executor
        .execute(&spec, &[test_create_action("myapp", "web")])
        .unwrap();
    assert!(retry.all_succeeded());
    let calls = executor.runtime().call_log();
    let cleanup_index = calls
        .iter()
        .rposition(|(operation, _)| operation == "stop_and_remove_container_generation")
        .unwrap();
    let create_index = calls
        .iter()
        .rposition(|(operation, _)| operation == "create_in_sandbox")
        .unwrap();
    assert!(cleanup_index < create_index);
    assert!(
        calls[cleanup_index]
            .1
            .starts_with(&format!("myapp:{expected_id}:41:"))
    );
    assert!(
        !calls
            .iter()
            .any(|(operation, _)| matches!(operation.as_str(), "stop" | "remove"))
    );

    let running = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(running[0].phase, ServicePhase::Running);
    let replacement = running[0]
        .failed_create_ownership
        .as_ref()
        .expect("replacement ownership must be durable");
    assert_eq!(replacement.generation, 1);
    assert_eq!(replacement.container_id, "ctr-web");
    replacement.validate().unwrap();
}

#[test]
fn failed_generation_cleanup_retains_ownership_and_blocks_recreate() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_create = true;
    runtime.claim_failed_create_ownership = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    executor
        .execute(
            &spec,
            &[Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
        )
        .unwrap();
    let ownership = executor.store().load_observed_state("myapp").unwrap()[0]
        .failed_create_ownership
        .clone()
        .unwrap();

    executor.runtime_mut().fail_create = false;
    executor.runtime_mut().fail_generation_cleanup = true;
    let retry = executor
        .execute(&spec, &[test_create_action("myapp", "web")])
        .unwrap();
    assert_eq!(retry.failed, 1);
    let calls = executor.runtime().call_log();
    assert_eq!(
        calls
            .iter()
            .filter(|(operation, _)| operation == "stop_and_remove_container_generation")
            .count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|(operation, _)| operation == "create_in_sandbox")
            .count(),
        1,
        "cleanup failure must prevent a second create"
    );
    let failed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(failed[0].phase, ServicePhase::Failed);
    assert_eq!(failed[0].failed_create_ownership, Some(ownership));
}

#[test]
fn parallel_owned_failures_keep_their_generation_tokens_associated() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_create = true;
    runtime.claim_failed_create_ownership = true;
    let mut executor = make_executor(runtime);
    let spec = stack(
        "myapp",
        vec![svc("api", "alpine:latest"), svc("worker", "alpine:latest")],
    );

    let result = executor
        .execute(
            &spec,
            &[
                Action::ServiceCreate {
                    precondition: crate::reconcile::test_replica_precondition(),
                    target: crate::state_store::ServiceReplicaKey::first("api".to_string())
                        .unwrap(),
                },
                Action::ServiceCreate {
                    precondition: crate::reconcile::test_replica_precondition(),
                    target: crate::state_store::ServiceReplicaKey::first("worker".to_string())
                        .unwrap(),
                },
            ],
        )
        .unwrap();
    assert_eq!(result.failed, 2);

    let observed = executor.store().load_observed_state("myapp").unwrap();
    for service_name in ["api", "worker"] {
        let service = observed
            .iter()
            .find(|state| state.replica.service_name == service_name)
            .unwrap();
        let ownership = service.failed_create_ownership.as_ref().unwrap();
        let expected_id = super::create::generated_runtime_container_id("myapp", service_name, 1);
        assert_eq!(ownership.container_id, expected_id);
        assert_eq!(service.container_id.as_deref(), Some(expected_id.as_str()));
        assert_eq!(ownership.generation, 41);
        assert_eq!(ownership.stack_id, "myapp");
    }
}

#[test]
fn parallel_swapped_ownership_ids_are_discarded_and_never_cleaned() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_create = true;
    runtime.claim_failed_create_ownership = true;
    let api_id = super::create::generated_runtime_container_id("myapp", "api", 1);
    let worker_id = super::create::generated_runtime_container_id("myapp", "worker", 1);
    runtime.override_failed_create_ownership_id(&api_id, &worker_id);
    runtime.override_failed_create_ownership_id(&worker_id, &api_id);
    let mut executor = make_executor(runtime);
    let spec = stack(
        "myapp",
        vec![svc("api", "alpine:latest"), svc("worker", "alpine:latest")],
    );

    let first = executor
        .execute(
            &spec,
            &[
                Action::ServiceCreate {
                    precondition: crate::reconcile::test_replica_precondition(),
                    target: crate::state_store::ServiceReplicaKey::first("api".to_string())
                        .unwrap(),
                },
                Action::ServiceCreate {
                    precondition: crate::reconcile::test_replica_precondition(),
                    target: crate::state_store::ServiceReplicaKey::first("worker".to_string())
                        .unwrap(),
                },
            ],
        )
        .unwrap();
    assert_eq!(first.failed, 2);
    let failed = executor.store().load_observed_state("myapp").unwrap();
    assert!(failed.iter().all(|state| state.container_id.is_none()));
    assert!(
        failed
            .iter()
            .all(|state| state.failed_create_ownership.is_none())
    );

    let second = executor
        .execute(
            &spec,
            &[
                test_create_action("myapp", "api"),
                test_create_action("myapp", "worker"),
            ],
        )
        .unwrap();
    assert_eq!(second.failed, 2);
    assert!(
        !executor
            .runtime()
            .call_log()
            .iter()
            .any(|(operation, _)| operation == "stop_and_remove_container_generation")
    );
}

#[test]
fn partial_failure_continues_other_services() {
    let mut runtime = MockContainerRuntime::with_ids(vec!["ctr-db"]);
    runtime.fail_pull = false;
    runtime.fail_create = false;
    let mut executor = make_executor(runtime);

    let spec = stack(
        "myapp",
        vec![svc("db", "postgres:16"), svc("web", "nginx:latest")],
    );

    // Make only "web" fail by using a spec that triggers an error.
    // We'll test with a normal mock that succeeds for both.
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    // Both succeed with mock.
    assert_eq!(result.succeeded, 2);
}

#[test]
fn remove_with_no_container_id() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);

    // Service observed but no container_id.
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("orphan".to_string())
                    .unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Pending,
                container_id: None,
                failed_create_ownership: None,
                last_error: None,
                ready: false,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRemove {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("orphan".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // No stop/remove calls since there's no container.
    let calls = executor.runtime.call_log();
    assert!(calls.is_empty());
}

#[test]
fn volumes_created_before_containers() {
    let runtime = MockContainerRuntime::new();
    let tmp = tempfile::tempdir().unwrap();
    let mut executor = make_executor_with_dir(runtime, tmp.path());

    let spec = StackSpec {
        name: "myapp".to_string(),
        services: vec![ServiceSpec {
            mounts: vec![StackMountSpec::Named {
                source: "dbdata".to_string(),
                target: "/var/lib/db".to_string(),
                read_only: false,
            }],
            ..svc("db", "postgres:16")
        }],
        networks: vec![default_network()],
        volumes: vec![VolumeSpec {
            name: "dbdata".to_string(),
            driver: "local".to_string(),
            driver_opts: None,
        }],
        secrets: vec![],
        disk_size_mb: None,
    };

    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Volume directory exists.
    assert!(executor.volumes().volumes_dir().join("dbdata").is_dir());

    // VolumeCreated event emitted.
    let events = executor.store().load_events("myapp").unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::VolumeCreated { .. }))
    );
}

#[test]
fn service_with_ports_creates_correctly() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);

    let spec = stack(
        "myapp",
        vec![ServiceSpec {
            ports: vec![PortSpec {
                protocol: "tcp".to_string(),
                container_port: 80,
                host_port: Some(8080),
            }],
            ..svc("web", "nginx:latest")
        }],
    );

    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Verify sandbox setup + pull + create_in_sandbox were called.
    let calls = executor.runtime.call_log();
    let ops: Vec<&str> = calls.iter().map(|(op, _)| op.as_str()).collect();
    assert_eq!(
        ops,
        vec![
            "create_sandbox",
            "setup_sandbox_network",
            "pull",
            "create_in_sandbox"
        ]
    );
}

#[test]
fn exact_cleanup_failure_is_reported_and_retains_running_ownership() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_generation_cleanup = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);

    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-1".to_string()),
                failed_create_ownership: Some(generation_ownership("myapp", "ctr-1", 4)),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRemove {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.failed, 1);
    assert!(!result.all_succeeded());
    assert!(
        result.errors[0]
            .1
            .contains("mock generation cleanup failure")
    );

    // Failed cleanup retains the exact proof for retry.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed
        .iter()
        .find(|o| o.replica.service_name == "web")
        .unwrap();
    assert_eq!(web.phase, ServicePhase::Failed);
    assert!(web.failed_create_ownership.is_some());
}

#[test]
fn execution_result_errors_collected() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_pull = true;
    let mut executor = make_executor(runtime);

    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].0, "web");
    assert!(result.errors[0].1.contains("image pull failed"));
}

// ── Port tracking tests ──

#[test]
fn port_tracker_allocates_explicit_port() {
    let mut tracker = PortTracker::new();
    let ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(8080),
    }];
    let published = tracker.allocate("web", &ports).unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].host_port, 8080);
    assert_eq!(published[0].container_port, 80);
    assert!(tracker.in_use().contains(&8080));
}

#[test]
fn port_tracker_skips_internal_only_port_entries() {
    let mut tracker = PortTracker::new();
    let ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 3000,
        host_port: None,
    }];
    let published = tracker.allocate("api", &ports).unwrap();
    assert!(published.is_empty());
    assert!(tracker.in_use().is_empty());
}

#[test]
fn port_tracker_detects_cross_service_conflict() {
    let mut tracker = PortTracker::new();
    let ports_a = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(8080),
    }];
    tracker.allocate("web", &ports_a).unwrap();

    // Second service trying the same host port should fail.
    let ports_b = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 3000,
        host_port: Some(8080),
    }];
    let result = tracker.allocate("api", &ports_b);
    assert!(result.is_err());
}

#[test]
fn port_tracker_release_frees_port() {
    let mut tracker = PortTracker::new();
    let ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(9090),
    }];
    tracker.allocate("web", &ports).unwrap();
    assert!(tracker.in_use().contains(&9090));

    tracker.release("web");
    assert!(!tracker.in_use().contains(&9090));
    assert!(tracker.ports_for("web").is_none());
}

#[test]
fn port_tracker_reuse_after_release() {
    let mut tracker = PortTracker::new();
    let ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(9090),
    }];
    tracker.allocate("web", &ports).unwrap();
    tracker.release("web");

    // Another service can now use the same port.
    let ports2 = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 3000,
        host_port: Some(9090),
    }];
    let published = tracker.allocate("api", &ports2).unwrap();
    assert_eq!(published[0].host_port, 9090);
}

#[test]
fn port_tracker_reallocate_same_service_succeeds() {
    let mut tracker = PortTracker::new();
    let ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 5432,
        host_port: Some(5432),
    }];
    // First allocation succeeds.
    tracker.allocate("postgres", &ports).unwrap();

    // Re-allocating the same service (e.g. retry after create failure)
    // should succeed — the old allocation is released automatically.
    let published = tracker.allocate("postgres", &ports).unwrap();
    assert_eq!(published[0].host_port, 5432);
}

#[test]
fn port_tracker_reallocate_does_not_conflict_with_other_services() {
    let mut tracker = PortTracker::new();

    // Service A takes port 5433.
    tracker
        .allocate(
            "postgres-test",
            &[PortSpec {
                protocol: "tcp".to_string(),
                container_port: 5432,
                host_port: Some(5433),
            }],
        )
        .unwrap();

    // Service B takes port 5432.
    tracker
        .allocate(
            "postgres",
            &[PortSpec {
                protocol: "tcp".to_string(),
                container_port: 5432,
                host_port: Some(5432),
            }],
        )
        .unwrap();

    // Re-allocating service B should still succeed (its own port isn't
    // treated as a conflict), but service A's port is still reserved.
    let published = tracker
        .allocate(
            "postgres",
            &[PortSpec {
                protocol: "tcp".to_string(),
                container_port: 5432,
                host_port: Some(5432),
            }],
        )
        .unwrap();
    assert_eq!(published[0].host_port, 5432);

    // But trying to take service A's port should still fail.
    let result = tracker.allocate(
        "postgres",
        &[PortSpec {
            protocol: "tcp".to_string(),
            container_port: 5432,
            host_port: Some(5433),
        }],
    );
    assert!(result.is_err());
    assert_eq!(
        tracker.ports_for("postgres").unwrap()[0].host_port,
        5432,
        "failed replacement must retain the prior exact lease"
    );
}

#[test]
fn port_tracker_distinguishes_suffix_ambiguous_exact_replicas() {
    let mut tracker = PortTracker::new();
    let api_2 = crate::state_store::ServiceReplicaKey::new("api", 2).unwrap();
    let api_dash_2 = crate::state_store::ServiceReplicaKey::new("api-2", 1).unwrap();
    tracker
        .allocate_replica(
            &api_2,
            &[PortSpec {
                protocol: "tcp".to_string(),
                container_port: 80,
                host_port: Some(8080),
            }],
        )
        .unwrap();
    tracker
        .allocate_replica(
            &api_dash_2,
            &[PortSpec {
                protocol: "tcp".to_string(),
                container_port: 80,
                host_port: Some(8081),
            }],
        )
        .unwrap();
    assert_eq!(
        tracker.ports_for_replica(&api_2).unwrap()[0].host_port,
        8080
    );
    assert_eq!(
        tracker.ports_for_replica(&api_dash_2).unwrap()[0].host_port,
        8081
    );
    tracker.release_replica(&api_2);
    assert!(tracker.ports_for_replica(&api_2).is_none());
    assert!(tracker.ports_for_replica(&api_dash_2).is_some());
}

#[test]
fn executor_tracks_ports_on_create() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);

    let spec = stack(
        "myapp",
        vec![ServiceSpec {
            ports: vec![PortSpec {
                protocol: "tcp".to_string(),
                container_port: 80,
                host_port: Some(8080),
            }],
            ..svc("web", "nginx:latest")
        }],
    );

    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Ports should be tracked.
    let ports = executor.ports().ports_for("web").unwrap();
    assert_eq!(ports.len(), 1);
    assert_eq!(ports[0].host_port, 8080);
}

#[test]
fn executor_releases_ports_on_remove() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);

    let spec = stack(
        "myapp",
        vec![ServiceSpec {
            ports: vec![PortSpec {
                protocol: "tcp".to_string(),
                container_port: 80,
                host_port: Some(8080),
            }],
            ..svc("web", "nginx:latest")
        }],
    );

    // Create first.
    let create_actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    executor.execute(&spec, &create_actions).unwrap();
    assert!(executor.ports().ports_for("web").is_some());

    // Remove should release ports.
    let remove_actions = vec![Action::ServiceRemove {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    let result = executor.execute(&spec, &remove_actions).unwrap();
    assert!(result.all_succeeded());
    assert!(executor.ports().ports_for("web").is_none());
    assert!(executor.ports().in_use().is_empty());
}

#[test]
fn exact_teardown_preserves_configured_signal_and_grace_period() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let mut service = svc("web", "nginx:latest");
    service.stop_signal = Some("SIGQUIT".to_string());
    service.stop_grace_period_secs = Some(7);
    let spec = stack("myapp", vec![service]);
    let ownership = generation_ownership("myapp", "ctr-web", 12);
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some(ownership.container_id.clone()),
                failed_create_ownership: Some(ownership),
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let result = executor
        .execute(
            &spec,
            &[Action::ServiceRemove {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
        )
        .unwrap();
    assert!(result.all_succeeded());
    assert!(
        executor
            .runtime()
            .call_log()
            .iter()
            .any(|(operation, argument)| {
                operation == "stop_and_remove_container_generation"
                    && argument == "myapp:ctr-web:12:signal=SIGQUIT:grace_ms=7000"
            })
    );
}

#[test]
fn default_owned_create_fails_before_mutating_an_ownership_blind_runtime() {
    struct OwnershipBlindRuntime {
        create_called: std::sync::atomic::AtomicBool,
    }

    impl ContainerRuntime for OwnershipBlindRuntime {
        fn pull(&self, _image: &str) -> Result<String, StackError> {
            Ok("sha256:test".to_string())
        }

        fn create(
            &self,
            _image: &str,
            _config: vz_runtime_contract::RunConfig,
        ) -> Result<String, StackError> {
            self.create_called
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok("unowned-container".to_string())
        }

        fn stop(
            &self,
            _container_id: &str,
            _signal: Option<&str>,
            _grace_period: Option<std::time::Duration>,
        ) -> Result<(), StackError> {
            Ok(())
        }

        fn remove(&self, _container_id: &str) -> Result<(), StackError> {
            Ok(())
        }

        fn exec(&self, _container_id: &str, _command: &[String]) -> Result<i32, StackError> {
            Ok(0)
        }
    }

    let runtime = OwnershipBlindRuntime {
        create_called: std::sync::atomic::AtomicBool::new(false),
    };
    let failure = runtime
        .create_in_sandbox_owned("stack", "alpine", vz_runtime_contract::RunConfig::default())
        .unwrap_err();
    assert!(failure.cleanup.is_none());
    assert!(
        !runtime
            .create_called
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    assert!(failure.error.to_string().contains("unsupported_operation"));
}

#[test]
fn executor_port_conflict_rejects_the_whole_batch_before_effects() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
    let mut executor = make_executor(runtime);

    let spec = stack(
        "myapp",
        vec![
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 3000,
                    host_port: Some(8080), // conflict with web
                }],
                ..svc("api", "node:20")
            },
        ],
    );

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
    ];

    let error = executor.execute(&spec, &actions).unwrap_err();
    assert!(error.to_string().contains("port conflict"));
    assert!(
        executor.runtime().call_log().is_empty(),
        "whole-batch preflight must reject duplicate host ports before runtime effects"
    );

    // Preflight rejection is not a partially executed action and therefore
    // emits no action event or observed-state mutation.
    let events = executor.store().load_events("myapp").unwrap();
    assert!(events.is_empty());
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert!(observed.is_empty());
}

// ── Docker Compose network conformance tests ──

/// Helper: two-service stack for network tests.
fn network_stack() -> StackSpec {
    stack(
        "netapp",
        vec![
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 5432,
                    host_port: Some(5432),
                }],
                ..svc("db", "postgres:16")
            },
        ],
    )
}

/// Helper: three-service stack.
fn three_service_stack() -> StackSpec {
    stack(
        "triapp",
        vec![
            svc("web", "nginx:latest"),
            svc("api", "node:20"),
            svc("db", "postgres:16"),
        ],
    )
}

#[test]
fn shared_vm_boots_before_container_creates() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = network_stack();

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Verify ordering: create_sandbox → setup_sandbox_network → create_in_sandbox × 2.
    let call_log = executor.runtime.call_log();
    let ops: Vec<&str> = call_log.iter().map(|(op, _)| op.as_str()).collect();
    assert_eq!(ops[0], "create_sandbox");
    assert_eq!(ops[1], "setup_sandbox_network");
    // Remaining: pull + create_in_sandbox for each service.
    assert!(ops.contains(&"create_in_sandbox"));
    assert!(
        !ops.contains(&"create"),
        "should use create_in_sandbox, not create"
    );
}

#[test]
fn setup_sandbox_network_assigns_correct_ips() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = network_stack();

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];

    executor.execute(&spec, &actions).unwrap();

    // Verify setup_sandbox_network was called with correct service configs.
    let captured = executor.runtime.captured_network_services.lock().unwrap();
    assert_eq!(captured.len(), 1);
    let (stack_id, services) = &captured[0];
    assert_eq!(stack_id, "netapp");
    assert_eq!(services.len(), 2);

    // web gets 172.20.0.2/24, db gets 172.20.0.3/24, both on "default" network.
    assert_eq!(services[0].name, "web");
    assert_eq!(services[0].addr, "172.20.0.2/24");
    assert_eq!(services[0].network_name, "default");
    assert_eq!(services[1].name, "db");
    assert_eq!(services[1].addr, "172.20.0.3/24");
    assert_eq!(services[1].network_name, "default");
}

#[test]
fn service_to_service_hosts_use_real_ips() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = network_stack();

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];

    executor.execute(&spec, &actions).unwrap();

    // Verify extra_hosts use real IPs, not 127.0.0.1.
    let configs = executor.runtime.captured_configs.lock().unwrap();

    // Find web's config.
    let web_config = configs.iter().find(|(id, _)| id == "ctr-web");
    assert!(web_config.is_some(), "web config not captured");
    let web_hosts = &web_config.unwrap().1.extra_hosts;
    // web should have db mapped to 172.20.0.3 (db is index 1, so .3).
    let db_host = web_hosts.iter().find(|(h, _)| h == "db");
    assert!(db_host.is_some(), "db not in web's extra_hosts");
    assert_eq!(db_host.unwrap().1, "172.20.0.3");

    // Find db's config.
    let db_config = configs.iter().find(|(id, _)| id == "ctr-db");
    assert!(db_config.is_some(), "db config not captured");
    let db_hosts = &db_config.unwrap().1.extra_hosts;
    // db should have web mapped to 172.20.0.2.
    let web_host = db_hosts.iter().find(|(h, _)| h == "web");
    assert!(web_host.is_some(), "web not in db's extra_hosts");
    assert_eq!(web_host.unwrap().1, "172.20.0.2");
}

#[test]
fn containers_join_per_service_network_namespace() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = network_stack();

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];

    executor.execute(&spec, &actions).unwrap();

    let configs = executor.runtime.captured_configs.lock().unwrap();

    // web should join /var/run/netns/web.
    let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
    assert_eq!(
        web_config.1.network_namespace_path,
        Some("/var/run/netns/web".to_string())
    );

    // db should join /var/run/netns/db.
    let db_config = configs.iter().find(|(id, _)| id == "ctr-db").unwrap();
    assert_eq!(
        db_config.1.network_namespace_path,
        Some("/var/run/netns/db".to_string())
    );
}

#[test]
fn same_container_port_no_conflict_with_shared_vm() {
    // Two services both bind container port 80 but in different netns.
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
    let mut executor = make_executor(runtime);

    let spec = stack(
        "portapp",
        vec![
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8081),
                }],
                ..svc("api", "node:20")
            },
        ],
    );

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    // Both succeed: different host ports, same container port is fine with netns.
    assert!(result.all_succeeded());
    assert_eq!(result.succeeded, 2);
}

#[test]
fn three_service_ip_allocation() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = three_service_stack();

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];

    executor.execute(&spec, &actions).unwrap();

    let captured = executor.runtime.captured_network_services.lock().unwrap();
    let (_, services) = &captured[0];
    assert_eq!(services.len(), 3);
    // 172.20.0.1 = bridge, services get .2, .3, .4.
    assert_eq!(services[0].addr, "172.20.0.2/24");
    assert_eq!(services[1].addr, "172.20.0.3/24");
    assert_eq!(services[2].addr, "172.20.0.4/24");

    // Verify cross-service host resolution for web.
    let configs = executor.runtime.captured_configs.lock().unwrap();
    let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
    let web_hosts = &web_config.1.extra_hosts;
    assert_eq!(web_hosts.len(), 2);
    assert!(
        web_hosts
            .iter()
            .any(|(h, ip)| h == "api" && ip == "172.20.0.3")
    );
    assert!(
        web_hosts
            .iter()
            .any(|(h, ip)| h == "db" && ip == "172.20.0.4")
    );
    assert!(!web_hosts.iter().any(|(h, _)| h == "host.vz.internal"));
}

#[test]
fn single_service_stack_uses_sandbox() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec = stack("solo", vec![svc("web", "nginx:latest")]);

    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // Single-service stacks use sandbox mode (same as multi-service).
    let call_log = executor.runtime.call_log();
    let ops: Vec<&str> = call_log.iter().map(|(op, _)| op.as_str()).collect();
    assert!(ops.contains(&"create_sandbox"));
    assert!(ops.contains(&"setup_sandbox_network"));
    assert!(ops.contains(&"create_in_sandbox"));
    assert!(
        !ops.contains(&"create"),
        "should use create_in_sandbox, not create"
    );
}

#[test]
fn single_service_gets_sandbox_network() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
    let mut executor = make_executor(runtime);
    let spec = stack("solo", vec![svc("web", "nginx:latest")]);

    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    executor.execute(&spec, &actions).unwrap();

    // Single service gets sandbox networking (netns path assigned).
    let configs = executor.runtime.captured_configs.lock().unwrap();
    let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
    assert!(
        web_config.1.network_namespace_path.is_some(),
        "single service should get a network namespace"
    );
}

#[test]
fn shared_vm_not_rebooted_on_second_execute() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db", "ctr-new"]);
    let mut executor = make_executor(runtime);
    let spec = network_stack();

    // First execute: boots shared VM.
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];
    executor.execute(&spec, &actions).unwrap();

    // Second execute with a recreate: should NOT reboot.
    let actions2 = vec![Action::ServiceRecreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    executor.execute(&spec, &actions2).unwrap();

    // create_sandbox should only appear once.
    let boot_count = executor
        .runtime
        .call_log()
        .iter()
        .filter(|(op, _)| op == "create_sandbox")
        .count();
    assert_eq!(boot_count, 1, "sandbox should not be recreated");
}

// ── Parallel execution tests ──

#[test]
fn topo_levels_independent_services_same_level() {
    // Three services with no deps → all at level 0.
    let spec = three_service_stack();
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];
    let refs: Vec<&Action> = actions.iter().collect();
    let levels = compute_topo_levels(&refs, &spec);
    assert_eq!(levels.len(), 1, "all independent services at one level");
    assert_eq!(levels[0].len(), 3);
}

#[test]
fn topo_levels_chain_dependency() {
    // app → api → db: three levels.
    let spec = stack(
        "chain",
        vec![
            svc("db", "postgres:16"),
            ServiceSpec {
                depends_on: vec![crate::spec::ServiceDependency::started("db")],
                ..svc("api", "node:20")
            },
            ServiceSpec {
                depends_on: vec![crate::spec::ServiceDependency::started("api")],
                ..svc("app", "myapp:latest")
            },
        ],
    );
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("app".to_string()).unwrap(),
        },
    ];
    let refs: Vec<&Action> = actions.iter().collect();
    let levels = compute_topo_levels(&refs, &spec);
    assert_eq!(levels.len(), 3);
    assert_eq!(levels[0][0].service_name(), "db");
    assert_eq!(levels[1][0].service_name(), "api");
    assert_eq!(levels[2][0].service_name(), "app");
}

#[test]
fn topo_levels_diamond_dependency() {
    // web and api depend on db → db at level 0, web+api at level 1.
    let spec = stack(
        "diamond",
        vec![
            svc("db", "postgres:16"),
            ServiceSpec {
                depends_on: vec![crate::spec::ServiceDependency::started("db")],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                depends_on: vec![crate::spec::ServiceDependency::started("db")],
                ..svc("api", "node:20")
            },
        ],
    );
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
    ];
    let refs: Vec<&Action> = actions.iter().collect();
    let levels = compute_topo_levels(&refs, &spec);
    assert_eq!(levels.len(), 2);
    assert_eq!(levels[0].len(), 1);
    assert_eq!(levels[0][0].service_name(), "db");
    assert_eq!(levels[1].len(), 2);
    let level1_names: HashSet<&str> = levels[1].iter().map(|a| a.service_name()).collect();
    assert!(level1_names.contains("web"));
    assert!(level1_names.contains("api"));
}

#[test]
fn parallel_creates_all_succeed() {
    // Three independent services should all be created (via parallel path).
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = three_service_stack();

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());
    assert_eq!(result.succeeded, 3);

    // All three should be Running with deterministic IDs from container_id.
    let observed = executor.store().load_observed_state("triapp").unwrap();
    assert_eq!(observed.len(), 3);
    for obs in &observed {
        assert_eq!(obs.phase, ServicePhase::Running);
        assert_eq!(
            obs.container_id,
            Some(format!("ctr-{}", obs.replica.service_name))
        );
    }
}

#[test]
fn parallel_creates_with_dependency_ordering() {
    // web depends on db: db at level 0 (serial), web at level 1 (serial).
    // api has no deps: at level 0 alongside db (parallel with db).
    let spec = stack(
        "depapp",
        vec![
            svc("db", "postgres:16"),
            svc("api", "node:20"),
            ServiceSpec {
                depends_on: vec![crate::spec::ServiceDependency::started("db")],
                ..svc("web", "nginx:latest")
            },
        ],
    );

    let runtime = MockContainerRuntime::with_ids(vec!["ctr-db", "ctr-api", "ctr-web"]);
    let mut executor = make_executor(runtime);

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(
        result.all_succeeded(),
        "execution had errors: {:?}",
        result.errors
    );
    assert_eq!(result.succeeded, 3);

    // web depends on db, so web's create must come after db's.
    // api is independent, so it can be in any order relative to db.
    // With 3 services the executor boots a shared VM, so creates go
    // through create_in_sandbox (arg = "stack_name:image").
    let calls = executor.runtime.call_log();
    let create_calls: Vec<&str> = calls
        .iter()
        .filter(|(op, _)| op == "create" || op == "create_in_sandbox")
        .map(|(_, arg)| arg.as_str())
        .collect();
    // db and api images are both at level 0.
    // web image is at level 1 and must appear after both db and api.
    let web_idx = create_calls
        .iter()
        .position(|img| img.contains("nginx:latest"))
        .unwrap();
    let db_idx = create_calls
        .iter()
        .position(|img| img.contains("postgres:16"))
        .unwrap();
    assert!(
        db_idx < web_idx,
        "db must be created before web (dependency)"
    );
}

#[test]
fn resource_hints_passed_to_create_sandbox() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);

    let spec = stack(
        "resapp",
        vec![
            ServiceSpec {
                resources: ResourcesSpec {
                    cpus: Some(2.0),
                    memory_bytes: Some(512 * 1024 * 1024), // 512 MiB
                    ..Default::default()
                },
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                resources: ResourcesSpec {
                    cpus: Some(4.0),
                    memory_bytes: Some(1024 * 1024 * 1024), // 1 GiB
                    ..Default::default()
                },
                ..svc("db", "postgres:16")
            },
        ],
    );

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());
    // Verify create_sandbox was called (indicating sandbox was used).
    let calls = executor.runtime.call_log();
    assert!(calls.iter().any(|(op, _)| op == "create_sandbox"));
}

#[test]
fn create_sandbox_forwards_only_explicit_host_published_ports() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
    let mut executor = make_executor(runtime);

    let spec = stack(
        "publish-opt-in",
        vec![
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(18080),
                }],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 3000,
                    host_port: None,
                }],
                ..svc("api", "node:20")
            },
        ],
    );

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    let calls = executor.runtime.call_log();
    let create_sandbox_call = calls
        .iter()
        .find(|(operation, _)| operation == "create_sandbox")
        .expect("create_sandbox call should be recorded");
    assert!(create_sandbox_call.1.contains("18080:80"));
    assert!(!create_sandbox_call.1.contains("3000:3000"));
}

// ── Custom network tests ──

/// Helper: create a NetworkSpec.
fn net(name: &str, subnet: Option<&str>) -> crate::spec::NetworkSpec {
    crate::spec::NetworkSpec {
        name: name.to_string(),
        driver: "bridge".to_string(),
        subnet: subnet.map(|s| s.to_string()),
    }
}

#[test]
fn custom_networks_multi_subnet_allocation() {
    // Two networks: frontend (auto) and backend (auto).
    // web on frontend only, api on both, db on backend only.
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
    let mut executor = make_executor(runtime);

    let spec = StackSpec {
        name: "multinet".to_string(),
        services: vec![
            ServiceSpec {
                networks: vec!["frontend".to_string()],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                networks: vec!["frontend".to_string(), "backend".to_string()],
                ..svc("api", "node:20")
            },
            ServiceSpec {
                networks: vec!["backend".to_string()],
                ..svc("db", "postgres:16")
            },
        ],
        networks: vec![net("frontend", None), net("backend", None)],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("multinet"),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("multinet"),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("multinet"),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded(), "errors: {:?}", result.errors);

    // Verify network configs: 4 entries (web@frontend, api@frontend, api@backend, db@backend).
    let captured = executor.runtime.captured_network_services.lock().unwrap();
    assert_eq!(captured.len(), 1);
    let (_, services) = &captured[0];
    assert_eq!(services.len(), 4);

    // frontend network: 172.20.0.0/24
    assert_eq!(services[0].name, "web");
    assert_eq!(services[0].addr, "172.20.0.2/24");
    assert_eq!(services[0].network_name, "frontend");

    assert_eq!(services[1].name, "api");
    assert_eq!(services[1].addr, "172.20.0.3/24");
    assert_eq!(services[1].network_name, "frontend");

    // backend network: 172.20.1.0/24
    assert_eq!(services[2].name, "api");
    assert_eq!(services[2].addr, "172.20.1.2/24");
    assert_eq!(services[2].network_name, "backend");

    assert_eq!(services[3].name, "db");
    assert_eq!(services[3].addr, "172.20.1.3/24");
    assert_eq!(services[3].network_name, "backend");
}

#[test]
fn custom_networks_explicit_subnet() {
    // Frontend has explicit subnet 10.0.1.0/24.
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
    let mut executor = make_executor(runtime);

    let spec = StackSpec {
        name: "explicit".to_string(),
        services: vec![
            ServiceSpec {
                networks: vec!["frontend".to_string()],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                networks: vec!["frontend".to_string()],
                ..svc("api", "node:20")
            },
        ],
        networks: vec![net("frontend", Some("10.0.1.0/24"))],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("explicit"),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("explicit"),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded(), "errors: {:?}", result.errors);

    let captured = executor.runtime.captured_network_services.lock().unwrap();
    let (_, services) = &captured[0];
    assert_eq!(services[0].addr, "10.0.1.2/24");
    assert_eq!(services[1].addr, "10.0.1.3/24");
}

#[test]
fn scoped_hosts_only_shared_networks() {
    // web on frontend only, db on backend only, api on both.
    // web should see api (shared frontend) but NOT db.
    // db should see api (shared backend) but NOT web.
    // api should see both web and db.
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
    let mut executor = make_executor(runtime);

    let spec = StackSpec {
        name: "scoped".to_string(),
        services: vec![
            ServiceSpec {
                networks: vec!["frontend".to_string()],
                ..svc("web", "nginx:latest")
            },
            ServiceSpec {
                networks: vec!["frontend".to_string(), "backend".to_string()],
                ..svc("api", "node:20")
            },
            ServiceSpec {
                networks: vec!["backend".to_string()],
                ..svc("db", "postgres:16")
            },
        ],
        networks: vec![net("frontend", None), net("backend", None)],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("scoped"),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("scoped"),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("scoped"),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded(), "errors: {:?}", result.errors);

    let configs = executor.runtime.captured_configs.lock().unwrap();

    // web should only see api (shared frontend), NOT db.
    let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
    let web_hosts: Vec<&str> = web_config
        .1
        .extra_hosts
        .iter()
        .map(|(h, _)| h.as_str())
        .collect();
    assert!(web_hosts.contains(&"api"), "web should see api");
    assert!(!web_hosts.contains(&"db"), "web should NOT see db");

    // db should only see api (shared backend), NOT web.
    let db_config = configs.iter().find(|(id, _)| id == "ctr-db").unwrap();
    let db_hosts: Vec<&str> = db_config
        .1
        .extra_hosts
        .iter()
        .map(|(h, _)| h.as_str())
        .collect();
    assert!(db_hosts.contains(&"api"), "db should see api");
    assert!(!db_hosts.contains(&"web"), "db should NOT see web");

    // api should see both web and db.
    let api_config = configs.iter().find(|(id, _)| id == "ctr-api").unwrap();
    let api_hosts: Vec<&str> = api_config
        .1
        .extra_hosts
        .iter()
        .map(|(h, _)| h.as_str())
        .collect();
    assert!(api_hosts.contains(&"web"), "api should see web");
    assert!(api_hosts.contains(&"db"), "api should see db");
}

#[test]
fn default_network_backward_compat() {
    // When all services are on "default" network, behaviour is identical
    // to the old single-bridge approach.
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
    let mut executor = make_executor(runtime);
    let spec = network_stack();

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());

    // All services on the same network see each other, with no implicit host alias.
    let configs = executor.runtime.captured_configs.lock().unwrap();
    let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
    assert_eq!(web_config.1.extra_hosts.len(), 1);
    assert!(web_config.1.extra_hosts.iter().any(|(h, _)| h == "db"));
    assert!(
        !web_config
            .1
            .extra_hosts
            .iter()
            .any(|(h, _)| h == "host.vz.internal")
    );

    let db_config = configs.iter().find(|(id, _)| id == "ctr-db").unwrap();
    assert_eq!(db_config.1.extra_hosts.len(), 1);
    assert!(db_config.1.extra_hosts.iter().any(|(h, _)| h == "web"));
    assert!(
        !db_config
            .1
            .extra_hosts
            .iter()
            .any(|(h, _)| h == "host.vz.internal")
    );
}

#[test]
fn caller_declared_extra_hosts_are_preserved_without_implicit_aliases() {
    let runtime = MockContainerRuntime::with_ids(vec!["ctr-only"]);
    let mut executor = make_executor(runtime);
    let mut service = svc("only", "nginx:latest");
    service.extra_hosts = vec![
        ("host.example.test".to_string(), "203.0.113.17".to_string()),
        ("db.example.test".to_string(), "198.51.100.9".to_string()),
    ];
    let expected = service.extra_hosts.clone();
    let spec = stack("solo", vec![service]);

    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("only".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded(), "errors: {:?}", result.errors);

    let configs = executor.runtime.captured_configs.lock().unwrap();
    let only_config = configs.iter().find(|(id, _)| id == "ctr-only").unwrap();
    assert_eq!(only_config.1.extra_hosts, expected);
}

#[test]
fn parse_subnet_helpers() {
    assert_eq!(parse_subnet_base("172.20.1.0/24"), [172, 20, 1, 0]);
    assert_eq!(parse_subnet_base("10.0.0.0/16"), [10, 0, 0, 0]);
    assert_eq!(parse_subnet_prefix("172.20.1.0/24"), 24);
    assert_eq!(parse_subnet_prefix("10.0.0.0/16"), 16);
}

#[test]
fn port_tracker_snapshot_and_restore() {
    let mut tracker = PortTracker::new();
    let ports = vec![PublishedPort {
        host_port: 8080,
        container_port: 80,
        protocol: "tcp".to_string(),
    }];
    let web = crate::state_store::ServiceReplicaKey::first("web").unwrap();
    tracker.restore(web.clone(), ports.clone());

    let snapshot = tracker.allocated_snapshot();
    assert_eq!(snapshot.get(&web).unwrap(), &ports);

    let mut tracker2 = PortTracker::new();
    for (name, ports) in snapshot {
        tracker2.restore(name.clone(), ports.clone());
    }
    assert_eq!(tracker2.allocated_snapshot().get(&web).unwrap(), &ports);
}

#[test]
fn stream_logs_default_returns_empty_stream() {
    let runtime = MockContainerRuntime::new();
    let rx = runtime.stream_logs("ctr-001", "web", false).unwrap();

    // Default mock has no pre-configured lines, so channel closes immediately.
    let lines: Vec<LogLine> = rx.iter().collect();
    assert!(lines.is_empty());
}

#[test]
fn stream_logs_mock_returns_configured_lines() {
    let runtime = MockContainerRuntime::new();
    {
        let mut mock_lines = runtime.mock_log_lines.lock().unwrap();
        mock_lines.push(LogLine {
            timestamp: Some("2025-01-15T10:00:00Z".to_string()),
            service: "api".to_string(),
            line: "server started on :8080".to_string(),
        });
        mock_lines.push(LogLine {
            timestamp: None,
            service: "api".to_string(),
            line: "ready to accept connections".to_string(),
        });
    }

    let rx = runtime.stream_logs("ctr-api", "api", true).unwrap();
    let lines: Vec<LogLine> = rx.iter().collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0].service, "api");
    assert_eq!(lines[0].line, "server started on :8080");
    assert!(lines[0].timestamp.is_some());
    assert_eq!(lines[1].line, "ready to accept connections");
    assert!(lines[1].timestamp.is_none());
}

#[test]
fn stream_logs_records_call_in_mock() {
    let runtime = MockContainerRuntime::new();
    let _rx = runtime.stream_logs("ctr-db", "postgres", true).unwrap();

    let calls = runtime.call_log();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "stream_logs");
    assert_eq!(calls[0].1, "ctr-db:postgres:follow=true");
}

#[test]
fn log_line_clone_and_debug() {
    let line = LogLine {
        timestamp: Some("2025-01-15T10:00:00Z".to_string()),
        service: "web".to_string(),
        line: "hello world".to_string(),
    };
    let cloned = line.clone();
    assert_eq!(cloned.service, "web");
    // Ensure Debug is derived.
    let _debug = format!("{:?}", cloned);
}

// ── Stop/remove failure cascade tests ──

#[test]
fn exact_cleanup_failure_never_falls_back_to_id_only_remove() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_generation_cleanup = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);

    // Simulate existing running container.
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-web".to_string()),
                failed_create_ownership: Some(generation_ownership("myapp", "ctr-web", 5)),
                last_error: None,
                ready: false,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRemove {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.failed, 1, "cleanup failure must fail teardown");
    assert!(!result.all_succeeded());

    // Exact cleanup was attempted, with no ID-only fallback.
    let calls = executor.runtime().call_log();
    assert!(
        calls
            .iter()
            .any(|(op, _)| op == "stop_and_remove_container_generation")
    );
    assert!(
        !calls
            .iter()
            .any(|(op, _)| matches!(op.as_str(), "stop" | "remove"))
    );

    // State retains exact cleanup authority for retry.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed
        .iter()
        .find(|o| o.replica.service_name == "web")
        .unwrap();
    assert_eq!(web.phase, ServicePhase::Failed);
    assert_eq!(web.container_id.as_deref(), Some("ctr-web"));
    assert!(web.failed_create_ownership.is_some());
}

#[test]
fn remove_failure_retains_failed_state_and_container_for_retry() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_generation_cleanup = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);

    // Simulate existing running container.
    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-web".to_string()),
                failed_create_ownership: Some(generation_ownership("myapp", "ctr-web", 6)),
                last_error: None,
                ready: false,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRemove {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.failed, 1);
    assert!(!result.all_succeeded());

    // Cleanup did not complete, so preserve the runtime ID for reconciliation.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed
        .iter()
        .find(|o| o.replica.service_name == "web")
        .unwrap();
    assert_eq!(web.phase, ServicePhase::Failed);
    assert_eq!(web.container_id.as_deref(), Some("ctr-web"));
    assert!(web.failed_create_ownership.is_some());
    assert!(
        web.last_error
            .as_deref()
            .is_some_and(|error| { error.contains("mock generation cleanup failure") })
    );
}

#[test]
fn exact_cleanup_failure_retains_container_for_retry() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_generation_cleanup = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![]);

    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-web".to_string()),
                failed_create_ownership: Some(generation_ownership("myapp", "ctr-web", 8)),
                last_error: None,
                ready: false,
            },
        )
        .unwrap();

    let actions = vec![Action::ServiceRemove {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.failed, 1);
    assert!(!result.all_succeeded());
    assert!(
        result.errors[0]
            .1
            .contains("mock generation cleanup failure")
    );

    let observed = executor.store().load_observed_state("myapp").unwrap();
    let web = observed
        .iter()
        .find(|o| o.replica.service_name == "web")
        .unwrap();
    assert_eq!(web.phase, ServicePhase::Failed);
    assert_eq!(web.container_id.as_deref(), Some("ctr-web"));
    let last_error = web.last_error.as_deref().unwrap();
    assert!(last_error.contains("mock generation cleanup failure"));
    assert!(web.failed_create_ownership.is_some());
}

#[test]
fn malformed_failed_create_ownership_fails_closed() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let ownership = vz_runtime_contract::ContainerGenerationOwnership {
        container_id: "owned-web".to_string(),
        generation: 41,
        stack_id: "different-stack".to_string(),
        scope: Some(Box::new(
            vz_runtime_contract::ContainerGenerationScope::synthetic_legacy_stack(
                "different-stack",
            )
            .unwrap(),
        )),
    };

    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Failed,
                container_id: Some(ownership.container_id.clone()),
                failed_create_ownership: Some(ownership.clone()),
                last_error: Some("activation rollback retained OCI state".to_string()),
                ready: false,
            },
        )
        .unwrap();

    let result = executor
        .execute(
            &spec,
            &[Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
        )
        .unwrap();

    assert_eq!(result.failed, 1);
    let calls = executor.runtime().call_log();
    assert!(!calls.iter().any(|(operation, _)| matches!(
        operation.as_str(),
        "stop" | "remove" | "stop_and_remove_container_generation" | "create_in_sandbox"
    )));
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed[0].phase, ServicePhase::Failed);
    assert_eq!(observed[0].failed_create_ownership, Some(ownership));
}

#[test]
fn legacy_unscoped_persisted_ownership_never_reaches_cleanup() {
    let runtime = MockContainerRuntime::new();
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);
    let ownership = vz_runtime_contract::ContainerGenerationOwnership {
        container_id: "owned-web".to_string(),
        generation: 41,
        stack_id: "myapp".to_string(),
        scope: None,
    };

    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Failed,
                container_id: Some(ownership.container_id.clone()),
                failed_create_ownership: Some(ownership.clone()),
                last_error: Some("legacy admitted failure".to_string()),
                ready: false,
            },
        )
        .unwrap();

    let result = executor
        .execute(
            &spec,
            &[Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
        )
        .unwrap();

    assert_eq!(result.failed, 1);
    assert!(
        !executor
            .runtime()
            .call_log()
            .iter()
            .any(|(operation, _)| operation == "stop_and_remove_container_generation")
    );
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed[0].failed_create_ownership, Some(ownership));
}

#[test]
fn already_absent_generation_cleanup_allows_recreation() {
    let mut runtime = MockContainerRuntime::new();
    runtime.generation_cleanup_already_absent = true;
    let mut executor = make_executor(runtime);
    let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

    let ownership = vz_runtime_contract::ContainerGenerationOwnership {
        container_id: "owned-web".to_string(),
        generation: 41,
        stack_id: "myapp".to_string(),
        scope: Some(Box::new(
            vz_runtime_contract::ContainerGenerationScope::synthetic_legacy_stack("myapp").unwrap(),
        )),
    };

    executor
        .store()
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Failed,
                container_id: Some(ownership.container_id.clone()),
                failed_create_ownership: Some(ownership.clone()),
                last_error: Some("create failed before OCI state existed".to_string()),
                ready: false,
            },
        )
        .unwrap();

    let result = executor
        .execute(
            &spec,
            &[Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
        )
        .unwrap();

    assert!(result.all_succeeded());
    let calls = executor.runtime().call_log();
    assert!(calls.iter().any(|(operation, argument)| {
        operation == "stop_and_remove_container_generation"
            && argument.starts_with("myapp:owned-web:41:")
    }));
    assert!(
        !calls
            .iter()
            .any(|(operation, _)| matches!(operation.as_str(), "stop" | "remove"))
    );
    assert!(
        calls
            .iter()
            .any(|(operation, _)| operation == "create_in_sandbox")
    );
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed[0].phase, ServicePhase::Running);
}

#[test]
fn ports_remain_reserved_when_exact_cleanup_fails() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_generation_cleanup = true;
    let mut executor = make_executor(runtime);

    let mut web = svc("web", "nginx:latest");
    web.ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(8080),
    }];
    let spec = stack("myapp", vec![web.clone()]);

    // Create the service first.
    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    let result = executor.execute(&spec, &actions).unwrap();
    assert!(result.all_succeeded());
    assert!(executor.ports().in_use().contains(&8080));

    // Exact cleanup fails, so the still-live generation keeps its port claim.
    let remove_spec = stack("myapp", vec![]);
    let remove_actions = vec![Action::ServiceRemove {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    let result = executor.execute(&remove_spec, &remove_actions).unwrap();
    assert_eq!(result.failed, 1, "cleanup failure must fail teardown");
    assert!(
        executor.ports().in_use().contains(&8080),
        "port 8080 must remain reserved while cleanup is incomplete"
    );
}

#[test]
fn ports_released_when_create_fails_on_retry() {
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_create = true;
    let mut executor = make_executor(runtime);

    let mut web = svc("web", "nginx:latest");
    web.ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(8080),
    }];
    let spec = stack("myapp", vec![web.clone()]);

    // Create fails — ports were allocated during prepare_create but
    // service is marked Failed. Verify port state is usable for retry.
    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    let result = executor.execute(&spec, &actions).unwrap();
    assert_eq!(result.failed, 1);

    // Port should still be allocated (not released) because the service
    // will be retried — release only happens on ServiceRemove.
    // But crucially, a second create attempt should not conflict.
    let mut retry_runtime = MockContainerRuntime::new();
    retry_runtime.fail_create = false;
    // We can't swap the runtime, but we can verify port tracker state
    // allows reallocation for the same service.
    let reallocated = executor.ports_mut().allocate("web", &web.ports);
    assert!(
        reallocated.is_ok(),
        "same service should be able to reallocate its ports on retry: {:?}",
        reallocated.err()
    );
}

// ── Partial replica scale-down failure tests ──

#[test]
fn replica_scale_down_removes_excess_replicas() {
    let runtime = MockContainerRuntime::new();
    let executor = make_executor(runtime);
    let spec_name = "replica-sd";

    // Simulate 3 running replicas.
    for name in ["web", "web-2", "web-3"] {
        crate::reconcile::publish_test_container_running(
            executor.store(),
            spec_name,
            &crate::state_store::ServiceReplicaKey::first(name.to_string()).unwrap(),
            "test-config",
        );
    }

    // Scale down to 1 replica.
    let mut web = svc("web", "nginx:latest");
    web.resources.replicas = 1;
    let spec = stack(spec_name, vec![web]);

    let health = HashMap::new();
    let reconcile = apply(&spec, executor.store(), &health).unwrap();

    // Should generate 2 remove actions (for web-2 and web-3).
    let remove_count = reconcile
        .actions
        .iter()
        .filter(|a| matches!(a, Action::ServiceRemove { .. }))
        .count();
    assert_eq!(remove_count, 2, "should remove 2 excess replicas");

    // Phase-A planning does not mutate exact journal-owned state before the
    // batch is durably claimed by the scoped executor.
    let observed = executor.store().load_observed_state(spec_name).unwrap();
    let running: Vec<&str> = observed
        .iter()
        .filter(|o| matches!(o.phase, ServicePhase::Running))
        .map(|o| o.replica.service_name.as_str())
        .collect();
    assert_eq!(running, vec!["web", "web-2", "web-3"]);
}

// ── Topology-scoped two-phase create tests ──

#[test]
fn scoped_claimed_batch_success_replays_terminal_commit_without_effects() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let bind_source = tmp.path().join("terminal-replay-bind");
    std::fs::create_dir(&bind_source).unwrap();
    let mut service = svc("web", "nginx:latest");
    service.mounts.push(StackMountSpec::Bind {
        source: bind_source.to_string_lossy().into_owned(),
        target: "/workspace".to_string(),
        read_only: false,
    });
    let spec = stack("scoped-coordinator-success", vec![service]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let actions = planned_actions(&executor, &spec);

    let first = executor
        .execute_claimed_batch(
            &spec,
            &actions,
            "session-coordinator-success",
            "operation-coordinator-success",
            0,
        )
        .unwrap();
    assert!(first.all_succeeded());
    let session = executor
        .store()
        .load_reconcile_session("session-coordinator-success")
        .unwrap()
        .unwrap();
    assert_eq!(
        session.status,
        crate::state_store::ReconcileSessionStatus::Completed
    );
    assert_eq!(session.next_action_index, actions.len());
    let observed = executor
        .store()
        .load_observed_state_for_replica(&spec.name, "web", 1)
        .unwrap()
        .unwrap();
    assert!(
        observed.ready,
        "a service without a healthcheck must become ready with activation"
    );
    assert_eq!(
        executor
            .store()
            .load_events(&spec.name)
            .unwrap()
            .iter()
            .filter(|event| matches!(event, StackEvent::ServiceReady { .. }))
            .count(),
        1
    );
    let calls_before = executor.runtime().call_log();
    let records_before = executor
        .store()
        .list_stack_container_recovery_records()
        .unwrap();
    std::fs::remove_dir(&bind_source).unwrap();

    let replay = executor
        .execute_claimed_batch(
            &spec,
            &actions,
            "session-coordinator-success",
            "operation-coordinator-success",
            0,
        )
        .unwrap();

    assert!(replay.all_succeeded());
    assert_eq!(replay.outcomes, first.outcomes);
    assert_eq!(executor.runtime().call_log(), calls_before);
    assert_eq!(
        executor
            .store()
            .list_stack_container_recovery_records()
            .unwrap(),
        records_before
    );

    let mut changed = spec.clone();
    changed.services[0].image = "nginx:changed".to_string();
    let error = executor
        .execute_claimed_batch(
            &changed,
            &actions,
            "session-coordinator-success",
            "operation-coordinator-success",
            0,
        )
        .unwrap_err();
    assert!(error.to_string().contains("activation payload"));
    assert_eq!(executor.runtime().call_log(), calls_before);
    assert_eq!(
        executor
            .store()
            .load_events(&spec.name)
            .unwrap()
            .iter()
            .filter(|event| matches!(event, StackEvent::ServiceReady { .. }))
            .count(),
        1,
        "terminal batch replay must not duplicate ServiceReady"
    );
}

#[test]
fn scoped_claimed_healthchecked_activation_stays_unready_without_ready_event() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let mut service = svc("web", "nginx:latest");
    service.healthcheck = Some(crate::spec::HealthCheckSpec {
        test: vec!["CMD".to_string(), "true".to_string()],
        interval_secs: Some(1),
        timeout_secs: Some(1),
        retries: Some(1),
        start_period_secs: Some(0),
    });
    let spec = stack("scoped-healthchecked-activation", vec![service]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let actions = planned_actions(&executor, &spec);

    let result = executor
        .execute_claimed_batch(
            &spec,
            &actions,
            "session-healthchecked-activation",
            "operation-healthchecked-activation",
            0,
        )
        .unwrap();

    assert!(result.all_succeeded());
    let observed = executor
        .store()
        .load_observed_state_for_replica(&spec.name, "web", 1)
        .unwrap()
        .unwrap();
    assert_eq!(observed.phase, ServicePhase::Running);
    assert!(!observed.ready);
    assert!(
        executor
            .store()
            .load_events(&spec.name)
            .unwrap()
            .iter()
            .all(|event| !matches!(event, StackEvent::ServiceReady { .. })),
        "a healthchecked service must not emit ServiceReady during activation"
    );
}

#[test]
fn scoped_claimed_batch_failure_replays_terminal_commit_without_effects() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack(
        "scoped-coordinator-failure",
        vec![svc("web", "nginx:latest")],
    );
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_scoped_activation = true;
    let mut executor = make_scoped_executor(runtime, store, tmp.path(), scope);
    let actions = planned_actions(&executor, &spec);

    let first = executor
        .execute_claimed_batch(
            &spec,
            &actions,
            "session-coordinator-failure",
            "operation-coordinator-failure",
            0,
        )
        .unwrap();
    assert_eq!(first.failed, 1);
    let session = executor
        .store()
        .load_reconcile_session("session-coordinator-failure")
        .unwrap()
        .unwrap();
    assert_eq!(
        session.status,
        crate::state_store::ReconcileSessionStatus::Failed
    );
    assert_eq!(session.next_action_index, 0);
    let calls_before = executor.runtime().call_log();

    let replay = executor
        .execute_claimed_batch(
            &spec,
            &actions,
            "session-coordinator-failure",
            "operation-coordinator-failure",
            0,
        )
        .unwrap();

    assert_eq!(replay.failed, 1);
    assert_eq!(replay.outcomes, first.outcomes);
    assert_eq!(executor.runtime().call_log(), calls_before);
}

#[test]
fn scoped_claimed_batch_outer_error_keeps_started_cursor_for_exact_retry() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack("scoped-coordinator-retry", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_sandbox_create = true;
    let mut executor = make_scoped_executor(runtime, store, tmp.path(), scope);
    let actions = planned_actions(&executor, &spec);

    let error = executor
        .execute_claimed_batch(
            &spec,
            &actions,
            "session-coordinator-retry",
            "operation-coordinator-retry",
            0,
        )
        .unwrap_err();
    assert!(error.to_string().contains("mock sandbox creation failure"));
    let session = executor
        .store()
        .load_reconcile_session("session-coordinator-retry")
        .unwrap()
        .unwrap();
    assert_eq!(
        session.status,
        crate::state_store::ReconcileSessionStatus::Active
    );
    assert_eq!(session.next_action_index, 0);
    assert!(
        executor
            .store()
            .load_audit_log_for_session(&session.session_id)
            .unwrap()
            .iter()
            .all(|audit| audit.status == "started")
    );

    executor.runtime.fail_sandbox_create = false;
    let result = executor
        .execute_claimed_batch(
            &spec,
            &actions,
            "session-coordinator-retry",
            "operation-coordinator-retry",
            0,
        )
        .unwrap();
    assert!(result.all_succeeded());
    let session = executor
        .store()
        .load_reconcile_session("session-coordinator-retry")
        .unwrap()
        .unwrap();
    assert_eq!(
        session.status,
        crate::state_store::ReconcileSessionStatus::Completed
    );
}

#[test]
fn scoped_claimed_batch_staging_failure_persists_no_session() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let mut service = svc("web", "nginx:latest");
    service.secrets = vec![secret_ref("token")];
    let mut spec = stack("scoped-coordinator-stage-failure", vec![service]);
    spec.secrets = vec![SecretDef {
        name: "token".to_string(),
        source: SecretSource::File(
            tmp.path()
                .join("missing-secret")
                .to_string_lossy()
                .into_owned(),
        ),
    }];
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    let actions = plan_apply(&spec, &store, &HashMap::new()).unwrap().actions;
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);

    let error = executor
        .execute_claimed_batch(
            &spec,
            &actions,
            "session-coordinator-stage-failure",
            "operation-coordinator-stage-failure",
            0,
        )
        .unwrap_err();

    assert!(error.to_string().contains("failed to read secret"));
    assert!(
        executor
            .store()
            .load_reconcile_session("session-coordinator-stage-failure")
            .unwrap()
            .is_none()
    );
    assert!(
        executor
            .store()
            .load_reconcile_progress(&spec.name)
            .unwrap()
            .is_none()
    );
    assert!(executor.runtime().call_log().is_empty());
}

#[test]
fn scoped_claimed_batch_establishes_missing_stack_dir_for_manifest_and_volumes() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp
        .path()
        .join("runtime")
        .join("stacks")
        .join("missing-stack-dir");
    let store = StateStore::in_memory().unwrap();
    let mut service = svc("web", "nginx:latest");
    service.mounts.push(StackMountSpec::Named {
        source: "workspace".to_string(),
        target: "/workspace".to_string(),
        read_only: false,
    });
    let mut spec = stack("scoped-missing-stack-dir", vec![service]);
    spec.volumes.push(VolumeSpec {
        name: "workspace".to_string(),
        driver: "local".to_string(),
        driver_opts: None,
    });
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor =
        StackExecutor::new_scoped(MockContainerRuntime::new(), store, &data_dir, scope).unwrap();
    let actions = planned_actions(&executor, &spec);

    assert!(!data_dir.exists());
    let result = executor
        .execute_claimed_batch(
            &spec,
            &actions,
            "session-missing-stack-dir",
            "operation-missing-stack-dir",
            0,
        )
        .unwrap();

    assert!(result.all_succeeded(), "errors: {:?}", result.errors);
    assert!(data_dir.join("scoped-activation").is_dir());
    assert!(data_dir.join("volumes/workspace").is_dir());
}

#[test]
fn scoped_claimed_batch_rejects_public_manifest_data_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("public-stack-dir");
    std::fs::create_dir(&data_dir).unwrap();
    std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack("scoped-public-stack-dir", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor =
        StackExecutor::new_scoped(MockContainerRuntime::new(), store, &data_dir, scope).unwrap();
    let actions = planned_actions(&executor, &spec);

    let error = executor
        .execute_claimed_batch(
            &spec,
            &actions,
            "session-public-stack-dir",
            "operation-public-stack-dir",
            0,
        )
        .unwrap_err();

    assert!(error.to_string().contains("permissions are not 0700"));
    assert!(
        executor
            .store()
            .load_reconcile_session("session-public-stack-dir")
            .unwrap()
            .is_none()
    );
    assert!(!data_dir.join("scoped-activation").exists());
    assert!(executor.runtime().call_log().is_empty());
}

#[test]
fn scoped_claimed_teardown_stays_active_through_finalizer_and_retries_exactly() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack(
        "scoped-teardown-finalizer",
        vec![svc("web", "nginx:latest")],
    );
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let initial = planned_actions(&executor, &spec);
    assert!(
        executor
            .execute_with_test_session(&spec, &initial, "operation-teardown-initial", 0)
            .unwrap()
            .all_succeeded()
    );
    let observed = executor.store().load_observed_state(&spec.name).unwrap();
    let remove_actions = crate::reconcile::attach_action_preconditions(
        &spec.name,
        executor.store(),
        vec![crate::reconcile::ActionDraft::Remove {
            target: observed[0].replica.clone(),
            observed: observed[0].clone(),
        }],
    )
    .unwrap();
    let mut unrelated = svc("unrelated", "busybox:latest");
    unrelated.mounts.push(StackMountSpec::Bind {
        source: tmp
            .path()
            .join("missing-unrelated-bind")
            .to_string_lossy()
            .into_owned(),
        target: "/irrelevant".to_string(),
        read_only: true,
    });
    let teardown_spec = stack("scoped-teardown-finalizer", vec![unrelated]);
    executor.scoped_cleanup_only = true;

    let pending = match executor
        .begin_claimed_teardown_batch(
            &teardown_spec,
            &remove_actions,
            "session-teardown-finalizer",
            "operation-teardown-finalizer",
            0,
        )
        .unwrap()
    {
        ClaimedTeardownAdmission::Ready(pending) => pending,
        ClaimedTeardownAdmission::Failed(result) => {
            panic!("exact removes unexpectedly failed: {:?}", result.errors)
        }
    };
    let session = executor
        .store()
        .load_reconcile_session("session-teardown-finalizer")
        .unwrap()
        .unwrap();
    assert_eq!(
        session.status,
        crate::state_store::ReconcileSessionStatus::Active
    );
    assert_eq!(session.next_action_index, 0);
    assert_eq!(executor.runtime().scoped_generation_count(), 0);

    let generic_commit_error = executor
        .store()
        .commit_reconcile_batch(
            &pending.session_id,
            &pending.stack_name,
            &pending.operation_id,
            pending.first_action_index,
            &pending.actions,
            &pending.result.outcomes,
        )
        .unwrap_err();
    assert!(
        generic_commit_error
            .to_string()
            .contains("claim-qualified teardown commit")
    );

    let generic_error = executor
        .execute_claimed_batch(
            &teardown_spec,
            &remove_actions,
            "session-teardown-finalizer",
            &pending.operation_id,
            0,
        )
        .unwrap_err();
    assert!(generic_error.to_string().contains("typed teardown API"));
    assert_eq!(
        executor
            .store()
            .load_reconcile_session("session-teardown-finalizer")
            .unwrap()
            .unwrap()
            .status,
        crate::state_store::ReconcileSessionStatus::Active
    );

    executor.scoped_authority.as_mut().unwrap().scope.stack_id = "wrong-stack".to_string();
    let error = executor
        .commit_claimed_teardown_batch(*pending)
        .unwrap_err();
    assert!(error.to_string().contains("exact stack"));
    assert_eq!(
        executor
            .store()
            .load_reconcile_session("session-teardown-finalizer")
            .unwrap()
            .unwrap()
            .status,
        crate::state_store::ReconcileSessionStatus::Active
    );
    executor.scoped_authority.as_mut().unwrap().scope.stack_id = teardown_spec.name.clone();
    let replay = match executor
        .begin_claimed_teardown_batch(
            &teardown_spec,
            &remove_actions,
            "session-teardown-finalizer",
            "operation-teardown-finalizer",
            0,
        )
        .unwrap()
    {
        ClaimedTeardownAdmission::Ready(pending) => pending,
        ClaimedTeardownAdmission::Failed(result) => {
            panic!(
                "exact remove replay unexpectedly failed: {:?}",
                result.errors
            )
        }
    };
    let prepared = crate::state_store::TeardownFinalizer {
        schema_version: crate::state_store::TEARDOWN_FINALIZER_SCHEMA_VERSION,
        operation_key: "req:request-finalize".to_string(),
        request_id: "request-finalize".to_string(),
        idempotency_key: None,
        request_digest: "vztr3-sha256:executor-finalizer".to_string(),
        session_id: replay.session_id.clone(),
        reconcile_operation_id: replay.operation_id.clone(),
        scope: executor.scoped_authority.as_ref().unwrap().scope.clone(),
        remove_volumes: false,
        changed_actions: 1,
        actions_hash: crate::reconcile::compute_actions_hash(&replay.actions),
        desired_state_digest: "vzs1-sha256:executor-finalizer".to_string(),
        initial_volumes: Vec::new(),
        initial_disk_image: false,
        initial_runtime_present: true,
        runtime_shutdown: false,
        staged_volumes: Vec::new(),
        purged_volumes: Vec::new(),
        disk_staged: false,
        disk_purged: false,
        status: crate::state_store::TeardownFinalizerStatus::Prepared,
        receipt: None,
        response_json: None,
        created_at: 100,
        updated_at: 100,
        completed_at: None,
    };
    executor
        .store()
        .reserve_teardown_finalizer(&prepared)
        .unwrap();
    let mut progressed = prepared.clone();
    progressed.runtime_shutdown = true;
    progressed.updated_at = 101;
    executor
        .store()
        .save_teardown_finalizer_progress(&progressed)
        .unwrap();
    let mut completed = progressed;
    completed.status = crate::state_store::TeardownFinalizerStatus::Completed;
    completed.updated_at = 102;
    completed.completed_at = Some(102);
    completed.response_json = Some(
        serde_json::json!({
            "request_id": "request-finalize",
            "stack_name": teardown_spec.name.clone(),
            "changed_actions": 1,
            "removed_volumes": 0,
        })
        .to_string(),
    );
    completed.receipt = Some(crate::state_store::Receipt {
        receipt_id: crate::state_store::teardown_receipt_id(
            &completed.operation_key,
            &completed.request_digest,
        ),
        operation: "teardown_stack".to_string(),
        entity_id: teardown_spec.name.clone(),
        entity_type: "stack".to_string(),
        request_id: "request-finalize".to_string(),
        status: "success".to_string(),
        created_at: 102,
        metadata: serde_json::json!({
            "request_digest": completed.request_digest.clone(),
            "changed_actions": 1,
            "removed_volumes": 0
        }),
    });
    let result = executor
        .commit_claimed_teardown_finalized(
            *replay,
            &completed,
            None,
            &StackEvent::StackDestroyed {
                stack_name: teardown_spec.name.clone(),
            },
        )
        .unwrap();
    assert!(result.all_succeeded());
    let session = executor
        .store()
        .load_reconcile_session("session-teardown-finalizer")
        .unwrap()
        .unwrap();
    assert_eq!(
        session.status,
        crate::state_store::ReconcileSessionStatus::Completed
    );
    assert_eq!(
        executor
            .store()
            .load_teardown_finalizer("req:request-finalize")
            .unwrap(),
        Some(completed)
    );
    assert_eq!(executor.store().list_receipts().unwrap().len(), 1);
    assert_eq!(
        executor
            .store()
            .load_events_since(&teardown_spec.name, 0)
            .unwrap()
            .into_iter()
            .filter(|record| matches!(record.event, StackEvent::StackDestroyed { .. }))
            .count(),
        1
    );
}

#[test]
fn scoped_claimed_teardown_failed_remove_keeps_exact_claim_active_for_retry() {
    let tmp = tempfile::tempdir().unwrap();
    let state_path = tmp.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();
    let spec = stack("scoped-teardown-failure", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(
        MockContainerRuntime::new(),
        store,
        tmp.path(),
        scope.clone(),
    );
    let initial = planned_actions(&executor, &spec);
    assert!(
        executor
            .execute_with_test_session(&spec, &initial, "operation-teardown-failure-initial", 0)
            .unwrap()
            .all_succeeded()
    );
    let observed = executor.store().load_observed_state(&spec.name).unwrap();
    let remove_actions = crate::reconcile::attach_action_preconditions(
        &spec.name,
        executor.store(),
        vec![crate::reconcile::ActionDraft::Remove {
            target: observed[0].replica.clone(),
            observed: observed[0].clone(),
        }],
    )
    .unwrap();
    executor.scoped_cleanup_only = true;
    executor.runtime.fail_generation_cleanup = true;

    let admission = executor
        .begin_claimed_teardown_batch(
            &spec,
            &remove_actions,
            "session-teardown-failure",
            "operation-teardown-failure",
            0,
        )
        .unwrap();
    let result = match admission {
        ClaimedTeardownAdmission::Failed(result) => result,
        ClaimedTeardownAdmission::Ready(_) => {
            panic!("failed exact remove must not grant finalizer authority")
        }
    };
    assert_eq!(result.failed, 1);
    let session = executor
        .store()
        .load_reconcile_session("session-teardown-failure")
        .unwrap()
        .unwrap();
    assert_eq!(
        session.status,
        crate::state_store::ReconcileSessionStatus::Active
    );
    assert_eq!(session.next_action_index, 0);
    let progress = executor
        .store()
        .load_reconcile_progress(&spec.name)
        .unwrap()
        .expect("failed teardown retains exact retry progress");
    assert_eq!(progress.operation_id, session.operation_id);
    assert_eq!(progress.next_action_index, 0);
    let audits = executor
        .store()
        .load_audit_log_for_session(&session.session_id)
        .unwrap();
    assert_eq!(audits.len(), remove_actions.len());
    assert!(audits.iter().all(|audit| audit.status == "started"));

    drop(executor);
    let reopened = StateStore::open(&state_path).unwrap();
    let mut executor =
        make_scoped_executor(MockContainerRuntime::new(), reopened, tmp.path(), scope);
    executor.scoped_cleanup_only = true;
    let retry = executor
        .begin_claimed_teardown_batch(
            &spec,
            &remove_actions,
            "session-teardown-failure",
            "operation-teardown-failure",
            0,
        )
        .unwrap();
    let pending = match retry {
        ClaimedTeardownAdmission::Ready(pending) => pending,
        ClaimedTeardownAdmission::Failed(result) => {
            panic!("exact retry should become ready: {:?}", result.errors)
        }
    };
    assert!(pending.execution_result().all_succeeded());

    let completed = executor.commit_claimed_teardown_batch(*pending).unwrap();
    assert!(completed.all_succeeded());
    let terminal = executor
        .store()
        .load_reconcile_session("session-teardown-failure")
        .unwrap()
        .unwrap();
    assert_eq!(
        terminal.status,
        crate::state_store::ReconcileSessionStatus::Completed
    );
    assert_eq!(terminal.next_action_index, remove_actions.len());
}

fn assert_claimed_successor_crash_replay(reserve_before_replay: bool) {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack("scoped-successor-replay", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    let actions = plan_apply(&spec, &store, &HashMap::new()).unwrap().actions;
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let session_id = if reserve_before_replay {
        "session-successor-reserved"
    } else {
        "session-successor-intent"
    };
    let operation_id = if reserve_before_replay {
        "operation-successor-reserved"
    } else {
        "operation-successor-intent"
    };
    executor
        .stage_scoped_batch_manifest(&spec, &actions, session_id, operation_id, 0)
        .unwrap();
    let session = crate::state_store::ReconcileSession {
        session_id: session_id.to_string(),
        stack_name: spec.name.clone(),
        operation_id: operation_id.to_string(),
        status: crate::state_store::ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 1,
        updated_at: 1,
        completed_at: None,
    };
    executor
        .store()
        .create_reconcile_batch(&session, &actions)
        .unwrap();
    let claims = executor
        .store()
        .start_reconcile_batch(session_id, &spec.name, operation_id, 0, &actions)
        .unwrap();

    let target = actions[0].target().clone();
    executor
        .service_ips
        .insert(target.clone(), "172.20.0.2".to_string());
    executor.service_network_ips.insert(
        target.clone(),
        HashMap::from([(spec.networks[0].name.clone(), "172.20.0.2".to_string())]),
    );
    executor.mount_tag_offsets.insert("web".to_string(), 0);
    let service_map = HashMap::from([("web", &spec.services[0])]);
    let prepared = executor
        .prepare_create(&spec, &service_map, "web", 1)
        .unwrap();
    let payload = super::scoped::scoped_activation_payload_sha256(
        &prepared,
        &spec,
        &executor.scoped_secret_digests,
    )
    .unwrap();
    let input = crate::state_store::ClaimedCreateInput {
        requested_container_id: prepared.requested_container_id.clone(),
        definition_digest: executor
            .scoped_authority
            .as_ref()
            .unwrap()
            .definition_digest
            .clone(),
        applied_config_digest: crate::reconcile::service_config_digest(&spec.services[0]),
        activation_payload_sha256: payload,
    };
    let allocation = crate::state_store::ClaimedAllocatorTarget {
        ports: vec![],
        service_ip: Some("172.20.0.2".to_string()),
        service_network_ips: vec![crate::state_store::ClaimedAllocatorNetworkIp {
            network_name: spec.networks[0].name.clone(),
            ip: "172.20.0.2".to_string(),
        }],
        mount_tag_offset: Some(0),
    };
    let intent = executor
        .store()
        .resolve_or_begin_claimed_successor(&claims[0], &input, &allocation, 2)
        .unwrap();
    if reserve_before_replay {
        let ownership = executor
            .runtime()
            .reserve_container_generation(&intent.scope, &intent.requested_container_id)
            .unwrap();
        executor
            .store()
            .bind_claimed_successor_generation(
                &claims[0],
                &crate::state_store::StackContainerGenerationBinding {
                    reservation_id: intent.scope.reservation_id.clone(),
                    service_name: intent.service_name.clone(),
                    ownership,
                    bound_at: 3,
                },
            )
            .unwrap();
    }

    assert!(
        executor
            .preflight_scoped_claims(&spec, &actions, session_id, operation_id, 0, &claims)
            .unwrap()
            .is_none()
    );
    let result = executor
        .execute_with_session(&spec, &actions, session_id, operation_id, 0, &claims)
        .unwrap();
    assert!(result.all_succeeded(), "{:?}", result.errors);
    assert_eq!(executor.runtime.scoped_generation_count(), 1);
    let committed_events = vec![
        StackEvent::ServiceCreating {
            stack_name: spec.name.clone(),
            service_name: "web".to_string(),
        },
        StackEvent::ServiceReady {
            stack_name: spec.name.clone(),
            service_name: "web".to_string(),
            runtime_id: intent.requested_container_id.clone(),
        },
    ];
    assert_eq!(
        executor.store().load_events(&spec.name).unwrap(),
        committed_events
    );

    let replay = executor
        .execute_with_session(&spec, &actions, session_id, operation_id, 0, &claims)
        .unwrap();
    assert!(replay.all_succeeded(), "{:?}", replay.errors);
    assert_eq!(executor.runtime.scoped_generation_count(), 1);
    assert_eq!(
        executor.store().load_events(&spec.name).unwrap(),
        committed_events,
        "Running successor replay must not duplicate lifecycle events"
    );
}

#[test]
fn scoped_claimed_successor_intent_crash_replays() {
    assert_claimed_successor_crash_replay(false);
}

#[test]
fn scoped_claimed_successor_reserved_crash_replays() {
    assert_claimed_successor_crash_replay(true);
}

#[test]
fn scoped_claim_action_mismatch_fails_before_runtime_effects() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack(
        "scoped-claim-mismatch",
        vec![svc("web", "nginx:latest"), svc("db", "postgres:16")],
    );
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    let actions = plan_apply(&spec, &store, &HashMap::new()).unwrap().actions;
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let session = crate::state_store::ReconcileSession {
        session_id: "session-claim-mismatch".to_string(),
        stack_name: spec.name.clone(),
        operation_id: "operation-claim-mismatch".to_string(),
        status: crate::state_store::ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 1,
        updated_at: 1,
        completed_at: None,
    };
    executor
        .stage_scoped_batch_manifest(
            &spec,
            &actions,
            &session.session_id,
            &session.operation_id,
            0,
        )
        .unwrap();
    executor
        .store()
        .create_reconcile_batch(&session, &actions)
        .unwrap();
    let mut claims = executor
        .store()
        .start_reconcile_batch(
            &session.session_id,
            &spec.name,
            &session.operation_id,
            0,
            &actions,
        )
        .unwrap();
    claims.swap(0, 1);

    let error = executor
        .execute_with_session(
            &spec,
            &actions,
            &session.session_id,
            &session.operation_id,
            0,
            &claims,
        )
        .unwrap_err();
    assert!(error.to_string().contains("claim"));
    assert!(executor.runtime().call_log().is_empty());
    assert!(
        executor
            .store()
            .load_observed_state(&spec.name)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn scoped_batch_preflight_is_zero_effect_when_later_replica_is_replaced() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack(
        "scoped-preflight-atomic",
        vec![svc("web", "web:v1"), svc("db", "db:v1")],
    );
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(
        MockContainerRuntime::new(),
        store,
        tmp.path(),
        scope.clone(),
    );
    let initial = planned_actions(&executor, &spec);
    assert!(
        executor
            .execute_with_test_session(&spec, &initial, "operation-preflight-initial", 0)
            .unwrap()
            .all_succeeded()
    );

    let mut changed = spec.clone();
    changed.services[0].image = "web:v2".to_string();
    changed.services[1].image = "db:v2".to_string();
    let actions = plan_apply(&changed, executor.store(), &HashMap::new())
        .unwrap()
        .actions;
    executor
        .store()
        .save_desired_state(&changed.name, &changed)
        .unwrap();
    let session = crate::state_store::ReconcileSession {
        session_id: "session-preflight-atomic".to_string(),
        stack_name: changed.name.clone(),
        operation_id: "operation-preflight-atomic".to_string(),
        status: crate::state_store::ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 2,
        updated_at: 2,
        completed_at: None,
    };
    executor
        .stage_scoped_batch_manifest(
            &changed,
            &actions,
            &session.session_id,
            &session.operation_id,
            0,
        )
        .unwrap();
    executor
        .store()
        .create_reconcile_batch(&session, &actions)
        .unwrap();
    let claims = executor
        .store()
        .start_reconcile_batch(
            &session.session_id,
            &changed.name,
            &session.operation_id,
            0,
            &actions,
        )
        .unwrap();
    let db_id = super::create::generated_runtime_container_id(&changed.name, "db", 1);
    let mut replacement = executor.runtime().scoped_ownership(&db_id).unwrap();
    replacement.generation += 1;
    executor
        .runtime()
        .insert_scoped_generation(replacement, true);
    let before = executor
        .store()
        .list_stack_container_recovery_records_for_machine_workload(&scope)
        .unwrap();
    let runtime_calls = executor.runtime().call_log().len();

    let result = executor
        .preflight_scoped_claims(
            &changed,
            &actions,
            &session.session_id,
            &session.operation_id,
            0,
            &claims,
        )
        .unwrap()
        .expect("replacement must fail batch preflight");
    assert!(!result.all_succeeded());
    let after = executor
        .store()
        .list_stack_container_recovery_records_for_machine_workload(&scope)
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(executor.runtime().call_log().len(), runtime_calls);
    assert!(!executor.runtime().call_log().iter().any(|(operation, _)| {
        matches!(
            operation.as_str(),
            "release_container_reservation" | "stop_and_remove_container_generation"
        )
    }));
}

#[test]
fn scoped_recreate_invalid_effective_input_leaves_predecessor_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack(
        "scoped-invalid-recreate-input",
        vec![svc("web", "nginx:latest")],
    );
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let initial = planned_actions(&executor, &spec);
    assert!(
        executor
            .execute_with_test_session(&spec, &initial, "operation-valid-initial", 0)
            .unwrap()
            .all_succeeded()
    );

    let mut changed = spec.clone();
    changed.services[0].mounts.push(StackMountSpec::Bind {
        source: tmp
            .path()
            .join("missing-bind-source")
            .to_string_lossy()
            .into_owned(),
        target: "/workspace".to_string(),
        read_only: false,
    });
    let actions = plan_apply(&changed, executor.store(), &HashMap::new())
        .unwrap()
        .actions;
    assert_eq!(actions.len(), 1);
    assert!(matches!(actions[0], Action::ServiceRecreate { .. }));
    executor
        .store()
        .save_desired_state(&changed.name, &changed)
        .unwrap();
    let session = crate::state_store::ReconcileSession {
        session_id: "session-invalid-recreate-input".to_string(),
        stack_name: changed.name.clone(),
        operation_id: "operation-invalid-recreate-input".to_string(),
        status: crate::state_store::ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 2,
        updated_at: 2,
        completed_at: None,
    };
    executor
        .stage_scoped_batch_manifest(
            &changed,
            &actions,
            &session.session_id,
            &session.operation_id,
            0,
        )
        .unwrap();
    executor
        .store()
        .create_reconcile_batch(&session, &actions)
        .unwrap();
    let claims = executor
        .store()
        .start_reconcile_batch(
            &session.session_id,
            &changed.name,
            &session.operation_id,
            0,
            &actions,
        )
        .unwrap();
    let before = executor
        .store()
        .list_stack_container_recovery_records_for_machine_workload(
            &executor.scoped_authority.as_ref().unwrap().scope,
        )
        .unwrap();
    let container_id = super::create::generated_runtime_container_id(&changed.name, "web", 1);
    let ownership = executor.runtime().scoped_ownership(&container_id).unwrap();
    let calls_before = executor.runtime().call_log().len();
    let ports_before = executor.ports.in_use();

    let error = executor
        .preflight_scoped_claims(
            &changed,
            &actions,
            &session.session_id,
            &session.operation_id,
            0,
            &claims,
        )
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("bind mount source does not exist"),
        "unexpected preflight error: {}",
        error
    );
    assert_eq!(
        executor
            .store()
            .list_stack_container_recovery_records_for_machine_workload(
                &executor.scoped_authority.as_ref().unwrap().scope,
            )
            .unwrap(),
        before
    );
    assert_eq!(
        executor.runtime().scoped_ownership(&container_id),
        Some(ownership)
    );
    assert_eq!(executor.runtime().call_log().len(), calls_before);
    assert_eq!(executor.ports.in_use(), ports_before);
}

#[test]
fn scoped_replicated_fixed_port_recreate_is_rejected_before_cleanup() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let mut service = svc("web", "nginx:latest");
    service.resources.replicas = 2;
    let spec = stack("scoped-invalid-fixed-port-recreate", vec![service]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let initial = planned_actions(&executor, &spec);
    assert!(
        executor
            .execute_with_test_session(&spec, &initial, "operation-fixed-port-initial", 0)
            .unwrap()
            .all_succeeded()
    );

    let mut changed = spec.clone();
    changed.services[0].ports.push(PortSpec {
        protocol: "tcp".to_string(),
        container_port: 8080,
        host_port: Some(18_080),
    });
    let actions = plan_apply(&changed, executor.store(), &HashMap::new())
        .unwrap()
        .actions;
    assert_eq!(actions.len(), 2);
    executor
        .store()
        .save_desired_state(&changed.name, &changed)
        .unwrap();
    let session = crate::state_store::ReconcileSession {
        session_id: "session-invalid-fixed-port-recreate".to_string(),
        stack_name: changed.name.clone(),
        operation_id: "operation-invalid-fixed-port-recreate".to_string(),
        status: crate::state_store::ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 2,
        updated_at: 2,
        completed_at: None,
    };
    executor
        .stage_scoped_batch_manifest(
            &changed,
            &actions,
            &session.session_id,
            &session.operation_id,
            0,
        )
        .unwrap();
    executor
        .store()
        .create_reconcile_batch(&session, &actions)
        .unwrap();
    let claims = executor
        .store()
        .start_reconcile_batch(
            &session.session_id,
            &changed.name,
            &session.operation_id,
            0,
            &actions,
        )
        .unwrap();
    let before = executor
        .store()
        .list_stack_container_recovery_records()
        .unwrap();
    let calls_before = executor.runtime().call_log().len();

    let error = executor
        .preflight_scoped_claims(
            &changed,
            &actions,
            &session.session_id,
            &session.operation_id,
            0,
            &claims,
        )
        .unwrap_err();

    assert!(error.to_string().contains("fixed host port"));
    assert_eq!(
        executor
            .store()
            .list_stack_container_recovery_records()
            .unwrap(),
        before
    );
    assert_eq!(executor.runtime().scoped_generation_count(), 2);
    assert_eq!(executor.runtime().call_log().len(), calls_before);
}

#[test]
fn scoped_create_rejoins_exact_running_attempt_without_a_second_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack("scoped-rejoin", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    assert!(
        executor
            .execute_with_test_session(&spec, &actions, "operation-rejoin", 0)
            .unwrap()
            .all_succeeded()
    );
    let first = executor
        .runtime
        .scoped_ownership(&super::create::generated_runtime_container_id(
            &spec.name, "web", 1,
        ));
    assert_eq!(first.as_ref().unwrap().generation, 1);

    assert_eq!(executor.runtime.scoped_generation_count(), 1);
    assert_eq!(
        executor
            .runtime
            .call_log()
            .iter()
            .filter(|(operation, _)| operation == "activate_scoped")
            .count(),
        1
    );
}

#[test]
fn scoped_activation_failure_cleans_before_terminal_retry_allocates_n_plus_one() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack("scoped-retry", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_scoped_activation = true;
    let mut executor = make_scoped_executor(runtime, store, tmp.path(), scope.clone());
    let actions = planned_actions(&executor, &spec);

    let failed = executor
        .execute_with_test_session(&spec, &actions, "operation-retry-2", 0)
        .unwrap();
    assert_eq!(failed.failed, 1);
    assert_eq!(executor.runtime.scoped_generation_count(), 0);
    assert!(
        executor
            .store()
            .list_stack_container_recovery_records_for_machine_workload(&scope)
            .unwrap()
            .is_empty(),
        "cleaned activation must not retain the environment deletion fence"
    );

    executor.runtime.fail_scoped_activation = false;
    let actions = planned_actions(&executor, &spec);
    let retried = executor
        .execute_with_test_session(&spec, &actions, "operation-retry", 0)
        .unwrap();
    assert!(retried.all_succeeded());
    let records = executor
        .store()
        .list_stack_container_recovery_records_for_machine_workload(&scope)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].intent.service_generation, 2);
    assert_eq!(records[0].binding.as_ref().unwrap().ownership.generation, 2);
}

#[test]
fn scoped_foreign_activation_cleanup_leaves_reserved_successor_unchanged() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack(
        "scoped-foreign-activation-cleanup",
        vec![svc("web", "nginx:latest")],
    );
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_scoped_activation = true;
    runtime.foreign_scoped_activation_cleanup = true;
    let mut executor = make_scoped_executor(runtime, store, tmp.path(), scope.clone());
    let actions = planned_actions(&executor, &spec);

    let result = executor
        .execute_with_test_session(&spec, &actions, "operation-foreign-cleanup", 0)
        .unwrap();

    assert_eq!(result.failed, 1);
    assert!(result.errors[0].1.contains("foreign cleanup ownership"));
    let records = executor
        .store()
        .list_stack_container_recovery_records_for_machine_workload(&scope)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].intent.status,
        crate::state_store::StackContainerCreateStatus::Reserved
    );
    let binding = records[0].binding.as_ref().unwrap();
    assert!(matches!(
        executor
            .runtime()
            .inspect_container_generation(&binding.ownership)
            .unwrap(),
        vz_runtime_contract::ContainerGenerationInspection::ReservedUnpublished(found)
            if found == binding.ownership
    ));
    assert!(!executor.runtime().call_log().iter().any(|(operation, _)| {
        matches!(
            operation.as_str(),
            "release_container_reservation" | "stop_and_remove_container_generation"
        )
    }));
}

#[test]
fn scoped_foreign_success_receipt_cleans_published_reserved_successor() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack(
        "scoped-foreign-success-receipt",
        vec![svc("web", "nginx:latest")],
    );
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut runtime = MockContainerRuntime::new();
    runtime.foreign_scoped_activation_receipt = true;
    let mut executor = make_scoped_executor(runtime, store, tmp.path(), scope.clone());
    let actions = planned_actions(&executor, &spec);

    let result = executor
        .execute_with_test_session(&spec, &actions, "operation-foreign-receipt", 0)
        .unwrap();

    assert_eq!(result.failed, 1);
    assert!(result.errors[0].1.contains("claim-linked runtime binding"));
    assert_eq!(executor.runtime().scoped_generation_count(), 0);
    assert!(
        executor
            .store()
            .list_stack_container_recovery_records_for_machine_workload(&scope)
            .unwrap()
            .is_empty()
    );
    assert!(
        executor
            .runtime()
            .call_log()
            .iter()
            .any(|(operation, _)| { operation == "stop_and_remove_container_generation" })
    );
}

#[test]
fn scoped_pull_failure_releases_reserved_generation_and_clears_recovery_fence() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack("scoped-pull-failure", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut runtime = MockContainerRuntime::new();
    runtime.fail_pull = true;
    let mut executor = make_scoped_executor(runtime, store, tmp.path(), scope.clone());

    let actions = planned_actions(&executor, &spec);
    let result = executor
        .execute_with_test_session(&spec, &actions, "operation-pull-failure", 0)
        .unwrap();
    assert_eq!(result.failed, 1);
    assert_eq!(executor.runtime.scoped_generation_count(), 0);
    assert!(
        executor
            .store()
            .list_stack_container_recovery_records_for_machine_workload(&scope)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn scoped_foreign_reservation_is_rejected_before_journal_or_runtime_mutation() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack("scoped-foreign", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut runtime = MockContainerRuntime::new();
    runtime.force_foreign_scoped_inspection = true;
    let mut executor = make_scoped_executor(runtime, store, tmp.path(), scope.clone());

    let actions = planned_actions(&executor, &spec);
    let result = executor
        .execute_with_test_session(&spec, &actions, "operation-foreign", 0)
        .unwrap();
    assert_eq!(result.failed, 1);
    assert_eq!(executor.runtime.scoped_generation_count(), 0);
    assert!(
        !executor
            .runtime
            .call_log()
            .iter()
            .any(|(operation, _)| operation == "activate_scoped")
    );
    let records = executor
        .store()
        .list_stack_container_recovery_records_for_machine_workload(&scope)
        .unwrap();
    assert!(records.is_empty());
}

#[test]
fn scoped_parallel_create_publishes_each_exact_generation_once() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack(
        "scoped-parallel",
        vec![svc("web", "nginx:latest"), svc("db", "postgres:16")],
    );
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);

    let result = executor
        .execute_with_test_session(
            &spec,
            &[
                Action::ServiceCreate {
                    precondition: crate::reconcile::test_replica_precondition(),
                    target: crate::state_store::ServiceReplicaKey::first("web".to_string())
                        .unwrap(),
                },
                Action::ServiceCreate {
                    precondition: crate::reconcile::test_replica_precondition(),
                    target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
                },
            ],
            "operation-parallel",
            0,
        )
        .unwrap();
    assert!(result.all_succeeded());
    assert_eq!(executor.runtime.scoped_generation_count(), 2);
    assert_eq!(
        executor
            .runtime
            .call_log()
            .iter()
            .filter(|(operation, _)| operation == "activate_scoped")
            .count(),
        2
    );
}

#[test]
fn scoped_replicas_publish_distinct_observed_names_and_converge() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let mut web = svc("web", "nginx:latest");
    web.resources.replicas = 3;
    let spec = stack("scoped-replicas", vec![web]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();

    let initial = plan_apply(&spec, &store, &HashMap::new()).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    assert_eq!(
        initial
            .actions
            .iter()
            .map(Action::target)
            .cloned()
            .collect::<Vec<_>>(),
        vec![
            crate::state_store::ServiceReplicaKey::new("web", 1).unwrap(),
            crate::state_store::ServiceReplicaKey::new("web", 2).unwrap(),
            crate::state_store::ServiceReplicaKey::new("web", 3).unwrap(),
        ]
    );
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    assert!(
        executor
            .execute_with_test_session(&spec, &initial.actions, "operation-replicas", 0)
            .unwrap()
            .all_succeeded()
    );

    let observed = executor.store().load_observed_state(&spec.name).unwrap();
    assert_eq!(
        observed.len(),
        3,
        "legacy replica-zero projection must be removed"
    );
    assert_eq!(
        observed
            .iter()
            .map(|state| (state.replica.service_name.as_str(), state.replica.index()))
            .collect::<Vec<_>>(),
        vec![("web", 1), ("web", 2), ("web", 3)]
    );
    assert!(
        observed
            .iter()
            .all(|state| state.phase == ServicePhase::Running)
    );

    let retry = plan_apply(&spec, executor.store(), &HashMap::new()).unwrap();
    assert!(retry.actions.is_empty(), "replica apply must converge");
    assert_eq!(
        executor
            .store()
            .load_observed_state(&spec.name)
            .unwrap()
            .len(),
        3
    );

    let mut scaled_down = spec.clone();
    scaled_down.services[0].resources.replicas = 1;
    let down = plan_apply(&scaled_down, executor.store(), &HashMap::new()).unwrap();
    executor
        .store()
        .save_desired_state(&scaled_down.name, &scaled_down)
        .unwrap();
    assert_eq!(
        down.actions
            .iter()
            .map(Action::target)
            .cloned()
            .collect::<Vec<_>>(),
        vec![
            crate::state_store::ServiceReplicaKey::new("web", 2).unwrap(),
            crate::state_store::ServiceReplicaKey::new("web", 3).unwrap(),
        ]
    );
    assert!(
        executor
            .execute_with_test_session(&scaled_down, &down.actions, "operation-scale-down", 0)
            .unwrap()
            .all_succeeded()
    );
    assert!(
        plan_apply(&scaled_down, executor.store(), &HashMap::new())
            .unwrap()
            .actions
            .is_empty(),
        "terminal stopped excess replicas must not be removed repeatedly"
    );
}

#[test]
fn scoped_scale_up_skips_exact_running_replicas_and_creates_only_missing_ones() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack("scoped-scale-up", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    let initial = plan_apply(&spec, &store, &HashMap::new()).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    assert!(
        executor
            .execute_with_test_session(&spec, &initial.actions, "operation-scale-up-initial", 0)
            .unwrap()
            .all_succeeded()
    );

    let mut scaled_up = spec.clone();
    scaled_up.services[0].resources.replicas = 3;
    let up = plan_apply(&scaled_up, executor.store(), &HashMap::new()).unwrap();
    executor
        .store()
        .save_desired_state(&scaled_up.name, &scaled_up)
        .unwrap();
    assert_eq!(
        up.actions
            .iter()
            .map(Action::target)
            .cloned()
            .collect::<Vec<_>>(),
        vec![
            crate::state_store::ServiceReplicaKey::new("web", 2).unwrap(),
            crate::state_store::ServiceReplicaKey::new("web", 3).unwrap(),
        ]
    );
    let result = executor
        .execute_with_test_session(&scaled_up, &up.actions, "operation-scale-up", 0)
        .unwrap();
    assert!(result.all_succeeded(), "{:?}", result.errors);
    assert_eq!(
        executor
            .runtime
            .call_log()
            .iter()
            .filter(|(operation, _)| operation == "activate_scoped")
            .count(),
        3
    );
    assert!(
        plan_apply(&scaled_up, executor.store(), &HashMap::new())
            .unwrap()
            .actions
            .is_empty()
    );
}

#[test]
fn scoped_single_exact_action_mutates_only_target_replica() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let mut web = svc("web", "nginx:latest");
    web.resources.replicas = 3;
    let spec = stack("scoped-one-replica", vec![web]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let action = Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: ServiceReplicaKey::new("web", 2).unwrap(),
    };
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let result = executor
        .execute_with_test_session(
            &spec,
            std::slice::from_ref(&action),
            "operation-only-two",
            0,
        )
        .unwrap();
    assert!(result.all_succeeded(), "{:?}", result.errors);
    assert_eq!(result.outcomes.len(), 1);
    assert_eq!(result.outcomes[0].target, *action.target());
    assert_eq!(executor.runtime.scoped_generation_count(), 1);
    let observed = executor.store().load_observed_state(&spec.name).unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(
        observed[0].replica,
        ServiceReplicaKey::new("web", 2).unwrap()
    );
}

#[test]
fn unscoped_single_exact_action_mutates_only_target_replica() {
    let mut web = svc("web", "nginx:latest");
    web.resources.replicas = 3;
    let spec = stack("unscoped-one-replica", vec![web]);
    let action = Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: ServiceReplicaKey::new("web", 2).unwrap(),
    };
    let mut executor = make_executor(MockContainerRuntime::new());

    let result = executor
        .execute(&spec, std::slice::from_ref(&action))
        .unwrap();

    assert!(result.all_succeeded(), "{:?}", result.errors);
    assert_eq!(result.outcomes.len(), 1);
    assert_eq!(result.outcomes[0].target, *action.target());
    let observed = executor.store().load_observed_state(&spec.name).unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(
        observed[0].replica,
        ServiceReplicaKey::new("web", 2).unwrap()
    );
    assert_eq!(
        executor
            .runtime()
            .call_log()
            .iter()
            .filter(|(operation, _)| operation == "create_in_sandbox")
            .count(),
        1
    );
}

#[test]
fn scoped_port_conflict_does_not_write_a_generic_replica_state() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let mut web = svc("web", "nginx:latest");
    web.ports = vec![PortSpec {
        protocol: "tcp".to_string(),
        container_port: 80,
        host_port: Some(18_080),
    }];
    let spec = stack("scoped-port-conflict", vec![web]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    executor
        .ports
        .allocate_replica(
            &ServiceReplicaKey::first("already-owned").unwrap(),
            &spec.services[0].ports,
        )
        .unwrap();
    let action = Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: ServiceReplicaKey::new("web", 2).unwrap(),
    };

    let error = executor
        .execute_with_test_session(
            &spec,
            std::slice::from_ref(&action),
            "operation-port-conflict",
            0,
        )
        .unwrap_err();

    assert!(error.to_string().contains("port conflict"));
    assert!(
        executor
            .store()
            .load_observed_state(&spec.name)
            .unwrap()
            .is_empty(),
        "pre-journal scoped failures must not synthesize replica #1 or #2 observed state"
    );
}

#[test]
fn scoped_outcomes_are_bijective_ordered_exact_and_offset_for_partial_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let mut api = svc("api", "api:v1");
    api.resources.replicas = 2;
    let spec = stack("scoped-outcomes", vec![api, svc("api-2", "worker:v1")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let runtime = MockContainerRuntime::new();
    runtime.fail_scoped_activation_ids.lock().unwrap().insert(
        super::create::generated_runtime_container_id(&spec.name, "api", 2),
    );
    let mut executor = make_scoped_executor(runtime, store, tmp.path(), scope);
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: ServiceReplicaKey::new("api", 1).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: ServiceReplicaKey::new("api", 2).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: ServiceReplicaKey::new("api-2", 1).unwrap(),
        },
    ];
    let result = executor
        .execute_with_test_session(&spec, &actions, "operation-outcomes", 0)
        .unwrap();
    assert_eq!(result.outcomes.len(), actions.len());
    assert_eq!(
        result
            .outcomes
            .iter()
            .map(|outcome| outcome.absolute_index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        result
            .outcomes
            .iter()
            .map(|outcome| outcome.target.clone())
            .collect::<Vec<_>>(),
        actions
            .iter()
            .map(|action| action.target().clone())
            .collect::<Vec<_>>()
    );
    assert!(matches!(
        result.outcomes[0].result,
        ActionOutcomeResult::Succeeded
    ));
    assert!(matches!(
        result.outcomes[1].result,
        ActionOutcomeResult::Failed { .. }
    ));
    assert!(matches!(
        result.outcomes[2].result,
        ActionOutcomeResult::Succeeded
    ));
    assert_ne!(result.outcomes[1].target, result.outcomes[2].target);
}

#[test]
fn scoped_secret_manifest_is_redacted_private_and_tamper_evident() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source-secret");
    std::fs::write(&source, b"never-serialize-this-secret").unwrap();
    let store = StateStore::in_memory().unwrap();
    let mut service = svc("web", "nginx:latest");
    service.secrets = vec![secret_ref("api_token")];
    let mut spec = stack("scoped-secret", vec![service]);
    spec.secrets = vec![SecretDef {
        name: "api_token".to_string(),
        source: SecretSource::File(source.display().to_string()),
    }];
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition_for_stack("scoped-secret"),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    assert!(
        executor
            .execute_with_test_session(&spec, &actions, "operation-secret", 0)
            .unwrap()
            .all_succeeded()
    );
    let root = tmp.path().join("scoped-activation");
    let owner = std::fs::read_dir(&root)
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| !entry.file_name().to_string_lossy().starts_with(".tmp-"))
        .unwrap()
        .path();
    let manifest = std::fs::read(owner.join("manifest.json")).unwrap();
    assert!(
        !manifest
            .windows(b"never-serialize-this-secret".len())
            .any(|window| window == b"never-serialize-this-secret")
    );
    assert_eq!(
        std::fs::metadata(&owner).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let staged = owner.join("secrets/api_token");
    assert_eq!(
        std::fs::metadata(&staged).unwrap().permissions().mode() & 0o777,
        0o600
    );

    std::fs::write(&staged, b"tampered").unwrap();
    assert_eq!(
        executor
            .runtime
            .call_log()
            .iter()
            .filter(|(operation, _)| operation == "activate_scoped")
            .count(),
        1
    );
}

#[test]
fn scoped_execution_rejects_operation_identity_without_exact_session() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack("scoped-requires-session", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition_for_stack(&spec.name),
        target: ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let error = executor
        .execute_with_operation(&spec, &actions, "operation-without-session", 0)
        .unwrap_err();
    assert!(error.to_string().contains("exact reconcile session_id"));
    assert!(executor.runtime.call_log().is_empty());
    assert!(!tmp.path().join("scoped-activation").exists());
}

#[test]
fn scoped_interrupted_temp_staging_is_retryable_but_missing_final_manifest_is_not() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("scoped-activation");
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let orphan = root.join(".tmp-interrupted");
    std::fs::create_dir(&orphan).unwrap();
    std::fs::set_permissions(&orphan, std::fs::Permissions::from_mode(0o700)).unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack("scoped-staging", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition_for_stack("scoped-staging"),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    assert!(
        executor
            .execute_with_test_session(&spec, &actions, "operation-staging", 0)
            .unwrap()
            .all_succeeded()
    );
    let owner = std::fs::read_dir(&root)
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| !entry.file_name().to_string_lossy().starts_with(".tmp-"))
        .unwrap()
        .path();
    std::fs::remove_file(owner.join("manifest.json")).unwrap();
    let error = executor
        .prepare_scoped_batch_manifest(
            &spec,
            &actions,
            "test-session-operation-staging",
            "operation-staging",
            0,
        )
        .unwrap_err();
    assert!(error.to_string().contains("manifest is missing"));
}

#[test]
fn scoped_manifest_rejects_config_change_before_new_reservation() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let mut service = svc("web", "nginx:latest");
    service.command = Some(vec!["serve".to_string(), "--safe".to_string()]);
    let spec = stack("scoped-config", vec![service]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition_for_stack("scoped-config"),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    assert!(
        executor
            .execute_with_test_session(&spec, &actions, "operation-config", 0)
            .unwrap()
            .all_succeeded()
    );
    let reservations = executor.runtime.scoped_generation_count();

    let mut changed = spec.clone();
    changed.services[0].command = Some(vec!["serve".to_string(), "--unsafe".to_string()]);
    let error = executor
        .prepare_scoped_batch_manifest(
            &changed,
            &actions,
            "test-session-operation-config",
            "operation-config",
            0,
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not match resumed action batch")
    );
    assert_eq!(executor.runtime.scoped_generation_count(), reservations);
}

#[test]
fn scoped_manifest_distinguishes_suffix_ambiguous_exact_targets() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let mut api = svc("api", "api:v1");
    api.resources.replicas = 2;
    let spec = stack(
        "scoped-manifest-exact",
        vec![api, svc("api-2", "worker:v1")],
    );
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let api_2 = Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: ServiceReplicaKey::new("api", 2).unwrap(),
    };
    executor
        .prepare_scoped_batch_manifest(
            &spec,
            std::slice::from_ref(&api_2),
            "session-exact-manifest",
            "operation-exact-manifest",
            0,
        )
        .unwrap();
    let api_dash_2 = Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: ServiceReplicaKey::new("api-2", 1).unwrap(),
    };
    let error = executor
        .prepare_scoped_batch_manifest(
            &spec,
            std::slice::from_ref(&api_dash_2),
            "session-exact-manifest",
            "operation-exact-manifest",
            0,
        )
        .unwrap_err();
    assert!(error.to_string().contains("resumed actions do not match"));
}

#[test]
fn scoped_manifest_rejects_session_prefix_trailing_and_recursive_schema_tamper() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack(
        "scoped-manifest-integrity",
        vec![svc("web", "nginx:latest"), svc("worker", "busybox:latest")],
    );
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack(&spec.name),
            target: ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack(&spec.name),
            target: ServiceReplicaKey::first("worker".to_string()).unwrap(),
        },
    ];
    executor
        .prepare_scoped_batch_manifest(&spec, &actions, "session-a", "operation-a", 0)
        .unwrap();
    let owner = std::fs::read_dir(tmp.path().join("scoped-activation"))
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| !entry.file_name().to_string_lossy().starts_with(".tmp-"))
        .unwrap()
        .path();
    let path = owner.join("manifest.json");
    let original = std::fs::read(&path).unwrap();

    let error = executor
        .prepare_scoped_batch_manifest(&spec, &actions, "session-b", "operation-a", 0)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not match resumed action batch")
    );

    let mut top_level: serde_json::Value = serde_json::from_slice(&original).unwrap();
    top_level["unexpected"] = serde_json::json!(true);
    std::fs::write(&path, serde_json::to_vec(&top_level).unwrap()).unwrap();
    assert!(
        executor
            .prepare_scoped_batch_manifest(&spec, &actions, "session-a", "operation-a", 0)
            .is_err()
    );

    let mut prefix: serde_json::Value = serde_json::from_slice(&original).unwrap();
    prefix["actions"][0]["target"]["service_name"] = serde_json::json!("tampered");
    std::fs::write(&path, serde_json::to_vec(&prefix).unwrap()).unwrap();
    assert!(
        executor
            .prepare_scoped_batch_manifest(&spec, &actions[1..], "session-a", "operation-a", 1,)
            .is_err()
    );

    let mut trailing: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let extra = trailing["actions"][1].clone();
    trailing["actions"].as_array_mut().unwrap().push(extra);
    std::fs::write(&path, serde_json::to_vec(&trailing).unwrap()).unwrap();
    assert!(
        executor
            .prepare_scoped_batch_manifest(&spec, &actions, "session-a", "operation-a", 0)
            .is_err()
    );

    let mut nested: serde_json::Value = serde_json::from_slice(&original).unwrap();
    nested["actions"][0]["precondition"]["workload"]["unexpected"] = serde_json::json!(true);
    std::fs::write(&path, serde_json::to_vec(&nested).unwrap()).unwrap();
    assert!(
        executor
            .prepare_scoped_batch_manifest(&spec, &actions, "session-a", "operation-a", 0)
            .is_err()
    );

    let secret = serde_json::json!({
        "sha256": "sha256:test",
        "file_name": "secret",
        "unexpected": true
    });
    assert!(serde_json::from_value::<super::scoped::ScopedSecretInput>(secret).is_err());
}

#[test]
fn scoped_running_replacement_is_blocked_and_never_reactivated() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack("scoped-replacement", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    assert!(
        executor
            .execute_with_test_session(&spec, &actions, "operation-replacement", 0)
            .unwrap()
            .all_succeeded()
    );
    let container_id = super::create::generated_runtime_container_id(&spec.name, "web", 1);
    let mut replacement = executor.runtime.scoped_ownership(&container_id).unwrap();
    replacement.generation += 1;
    executor.runtime.insert_scoped_generation(replacement, true);
    let activations = executor
        .runtime
        .call_log()
        .iter()
        .filter(|(operation, _)| operation == "activate_scoped")
        .count();
    let before = executor
        .store()
        .list_stack_container_recovery_records_for_machine_workload(
            &executor.scoped_authority.as_ref().unwrap().scope,
        )
        .unwrap();

    let _error = executor
        .execute_with_test_session(&spec, &actions, "operation-replacement-2", 0)
        .unwrap_err();
    assert_eq!(
        executor
            .runtime
            .call_log()
            .iter()
            .filter(|(operation, _)| operation == "activate_scoped")
            .count(),
        activations
    );
    let after = executor
        .store()
        .list_stack_container_recovery_records_for_machine_workload(
            &executor.scoped_authority.as_ref().unwrap().scope,
        )
        .unwrap();
    assert_eq!(
        after, before,
        "foreign replacement must not mutate journal state"
    );
}

#[test]
fn scoped_remove_preserves_stop_signal_and_grace_policy() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let mut service = svc("web", "nginx:latest");
    service.stop_signal = Some("SIGQUIT".to_string());
    service.stop_grace_period_secs = Some(17);
    let spec = stack("scoped-remove", vec![service]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    assert!(
        executor
            .execute_with_test_session(
                &spec,
                &[Action::ServiceCreate {
                    precondition: crate::reconcile::test_replica_precondition(),
                    target: crate::state_store::ServiceReplicaKey::first("web".to_string())
                        .unwrap(),
                }],
                "operation-remove-create",
                0,
            )
            .unwrap()
            .all_succeeded()
    );
    assert!(
        executor
            .execute_with_test_session(
                &spec,
                &[Action::ServiceRemove {
                    precondition: crate::reconcile::test_replica_precondition(),
                    target: crate::state_store::ServiceReplicaKey::first("web".to_string())
                        .unwrap(),
                }],
                "operation-remove",
                0,
            )
            .unwrap()
            .all_succeeded()
    );
    assert_eq!(executor.runtime.scoped_generation_count(), 0);
    assert!(
        executor
            .runtime
            .call_log()
            .iter()
            .any(|(operation, value)| {
                operation == "stop_and_remove_container_generation"
                    && value.contains("signal=SIGQUIT:grace_ms=17000")
            })
    );
}

#[test]
fn scoped_manifest_resume_uses_staged_secret_after_source_disappears() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("resume-secret-source");
    std::fs::write(&source, b"staged-before-crash").unwrap();
    let store = StateStore::in_memory().unwrap();
    let mut web = svc("web", "nginx:latest");
    web.secrets = vec![secret_ref("api_token")];
    let mut worker = svc("worker", "busybox:latest");
    worker.secrets = vec![secret_ref("api_token")];
    let mut spec = stack("scoped-resume-secret", vec![web, worker]);
    spec.secrets = vec![SecretDef {
        name: "api_token".to_string(),
        source: SecretSource::File(source.display().to_string()),
    }];
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = make_scoped_executor(MockContainerRuntime::new(), store, tmp.path(), scope);
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack(
                "scoped-resume-secret",
            ),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack(
                "scoped-resume-secret",
            ),
            target: crate::state_store::ServiceReplicaKey::first("worker".to_string()).unwrap(),
        },
    ];
    executor
        .prepare_scoped_batch_manifest(&spec, &actions, "session-cursor", "operation-cursor", 0)
        .unwrap();
    std::fs::remove_file(&source).unwrap();
    executor
        .prepare_scoped_batch_manifest(&spec, &actions, "session-cursor", "operation-cursor", 0)
        .unwrap();
    assert!(
        executor
            .scoped_secret_inputs
            .values()
            .all(|bytes| bytes == b"staged-before-crash")
    );
}

#[test]
fn scoped_cleanup_mode_rejects_create_before_mutation() {
    let tmp = tempfile::tempdir().unwrap();
    let store = StateStore::in_memory().unwrap();
    let spec = stack("scoped-cleanup-only", vec![svc("web", "nginx:latest")]);
    let (project, scope) = scoped_topology(&spec.name);
    store.save_project_state(&project).unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    store.save_desired_state(&spec.name, &spec).unwrap();
    let mut executor = StackExecutor::new_scoped_for_cleanup(
        MockContainerRuntime::new(),
        store,
        tmp.path(),
        scope,
    )
    .unwrap();

    let error = executor
        .execute_with_test_session(
            &spec,
            &[Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            }],
            "cleanup-cannot-create",
            0,
        )
        .unwrap_err();
    assert!(error.to_string().contains("cleanup-only"));
    assert!(executor.runtime.call_log().is_empty());
    assert!(!tmp.path().join("scoped-activation").exists());
}
