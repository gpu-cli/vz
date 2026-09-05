#![allow(clippy::unwrap_used)]
use super::*;
use vz_runtime_contract::*;

fn fixture() -> (tempfile::TempDir, StateStore, EnvironmentInstance) {
    let root = tempfile::tempdir().unwrap();
    let store = StateStore::open(&root.path().join("state.db")).unwrap();
    let definition:ProjectDefinition=serde_json::from_value(serde_json::json!({"schema_version":1,"project_id":ProjectId::generate(),"name":"context-store","environment":{"schema_version":1,"machines":[{"schema_version":1,"name":"app","profile":"developer","target":{"os":"linux","arch":"aarch64","image":"fixture"}}]}})).unwrap();
    let environment = definition.instantiate_environment("one", 1).unwrap();
    store
        .save_project_state(&ProjectState {
            schema_version: 1,
            definition,
            environments: vec![environment.clone()],
        })
        .unwrap();
    (root, store, environment)
}
fn acknowledgement(
    operation: &EnvironmentLifecycleOperation,
    name: &str,
) -> MachineLifecycleStepAcknowledgement {
    let step = &operation.machine_steps[0];
    let incarnation = MachineIncarnation {
        schema_version: 1,
        incarnation_id: MachineIncarnationId::generate(),
        machine_id: step.machine_id.clone(),
        generation: 1,
        created_at: 2,
    };
    let context = MachineDockerContextDescriptor {
        schema_version: 1,
        owner: ResourceOwner {
            project_id: operation.project_id.clone(),
            environment_id: operation.environment_id.clone(),
            machine_id: Some(step.machine_id.clone()),
        },
        name: name.into(),
        endpoint: "unix:///private/machine.sock".into(),
        config_dir: "/private/client".into(),
        engine_id: "engine-one".into(),
        incarnation_id: incarnation.incarnation_id.clone(),
        incarnation_generation: 1,
    };
    MachineLifecycleStepAcknowledgement {
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
            negotiated_capabilities: CapabilitySet::new([
                MachineCapability::DockerEngine,
                MachineCapability::Compose,
                MachineCapability::Buildx,
            ]),
            runtime_identity: MachineRuntimeIdentity {
                schema_version: 1,
                opaque_id: "exact-context-runtime".into(),
            },
            incarnation,
            docker_context: Some(context),
        }),
        result: LifecycleStepResult::Succeeded,
    }
}

#[test]
fn docker_context_ownership_and_activation_survive_sqlite_restart() {
    let (root, store, environment) = fixture();
    let operation = store
        .begin_environment_lifecycle(
            environment.environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "request",
            "key",
            "sha256:fixture",
            2,
        )
        .unwrap();
    let ack = acknowledgement(&operation, "context-one");
    store.acknowledge_environment_machine_step(&ack, 3).unwrap();
    store
        .finish_environment_lifecycle(operation.operation_id.as_str(), operation.generation, 4)
        .unwrap();
    drop(store);
    let store = StateStore::open(&root.path().join("state.db")).unwrap();
    let actual = store
        .load_environment_instance(environment.environment_id.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(
        actual.machines[0].docker_context,
        ack.resulting_activation.unwrap().docker_context
    );
    let row = actual
        .ownership
        .iter()
        .find(|row| row.resource_kind == OwnedResourceKind::DockerContext)
        .unwrap();
    assert_eq!(row.resource_id, "context-one");
    assert_eq!(row.machine_id, Some(actual.machines[0].machine_id.clone()));
    assert_eq!(store.require_owned_resource(row).unwrap(), *row);
}

#[test]
fn docker_context_foreign_name_collision_rolls_back_incarnation_and_activation() {
    let (_root, store, environment) = fixture();
    let project = store
        .load_project_state(environment.project_id.as_str())
        .unwrap()
        .unwrap();
    let mut definition = project.definition;
    definition.project_id = ProjectId::generate();
    let foreign = definition.instantiate_environment("foreign", 1).unwrap();
    store
        .save_project_state(&ProjectState {
            schema_version: 1,
            definition,
            environments: vec![foreign.clone()],
        })
        .unwrap();
    let reserved = OwnershipRecord {
        schema_version: 1,
        resource_kind: OwnedResourceKind::DockerContext,
        resource_id: "collision".into(),
        environment_id: foreign.environment_id.clone(),
        machine_id: Some(foreign.machines[0].machine_id.clone()),
    };
    store.reserve_owned_resource(&reserved, 2).unwrap();
    let operation = store
        .begin_environment_lifecycle(
            environment.environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "request",
            "key",
            "sha256:fixture",
            3,
        )
        .unwrap();
    let before = store
        .load_environment_instance(environment.environment_id.as_str())
        .unwrap()
        .unwrap();
    assert!(
        store
            .acknowledge_environment_machine_step(&acknowledgement(&operation, "collision"), 4)
            .is_err()
    );
    assert_eq!(
        store
            .load_environment_instance(environment.environment_id.as_str())
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(store.require_owned_resource(&reserved).unwrap(), reserved);
    assert_eq!(
        store
            .load_environment_lifecycle(operation.operation_id.as_str())
            .unwrap()
            .unwrap(),
        operation
    );
}
