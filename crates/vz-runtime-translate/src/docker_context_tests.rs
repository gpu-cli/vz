#![allow(clippy::unwrap_used)]
use super::*;

fn machine() -> MachineInstance {
    let definition: ProjectDefinition=serde_json::from_value(serde_json::json!({"schema_version":1,"project_id":ProjectId::generate(),"name":"context-wire","environment":{"schema_version":1,"machines":[{"schema_version":1,"name":"app","profile":"developer","target":{"os":"linux","arch":"aarch64","image":"fixture"}}]}})).unwrap();
    let environment = definition.instantiate_environment("one", 1).unwrap();
    let mut machine = environment.machines[0].clone();
    let incarnation = MachineIncarnation {
        schema_version: 1,
        incarnation_id: MachineIncarnationId::generate(),
        machine_id: machine.machine_id.clone(),
        generation: 1,
        created_at: 2,
    };
    machine.docker_context = Some(MachineDockerContextDescriptor {
        schema_version: 1,
        owner: ResourceOwner {
            project_id: environment.project_id,
            environment_id: environment.environment_id,
            machine_id: Some(machine.machine_id.clone()),
        },
        name: "vz-context".into(),
        endpoint: "unix:///private/machine.sock".into(),
        config_dir: "/private/client".into(),
        engine_id: "exact-engine".into(),
        incarnation_id: incarnation.incarnation_id.clone(),
        incarnation_generation: 1,
    });
    machine.incarnation = Some(incarnation);
    machine.runtime_identity = Some(MachineRuntimeIdentity {
        schema_version: 1,
        opaque_id: "exact".into(),
    });
    machine.backend = Some(MachineBackend::MacosVirtualizationLinux);
    machine.negotiated_capabilities = CapabilitySet::new([
        MachineCapability::DockerEngine,
        MachineCapability::Compose,
        MachineCapability::Buildx,
    ]);
    machine.state = MachineState::Ready;
    machine.validate().unwrap();
    machine
}

#[test]
fn docker_context_machine_activation_and_stopped_roundtrip() {
    let mut machine = machine();
    assert_eq!(
        machine_instance_from_proto(&machine_instance_to_proto(&machine)).unwrap(),
        machine
    );
    let evidence = MachineActivationEvidence {
        schema_version: 1,
        backend: machine.backend.clone().unwrap(),
        negotiated_capabilities: machine.negotiated_capabilities.clone(),
        runtime_identity: machine.runtime_identity.clone().unwrap(),
        incarnation: machine.incarnation.clone().unwrap(),
        docker_context: machine.docker_context.clone(),
    };
    assert_eq!(
        machine_activation_evidence_from_proto(&machine_activation_evidence_to_proto(&evidence))
            .unwrap(),
        evidence
    );
    machine.state = MachineState::Stopped;
    assert_eq!(
        machine_instance_from_proto(&machine_instance_to_proto(&machine)).unwrap(),
        machine
    );
}

#[test]
fn docker_context_wire_rejects_missing_owner_wrong_incarnation_and_non_developer() {
    let original = machine_instance_to_proto(&machine());
    for change in 0..7 {
        let mut wire = original.clone();
        let descriptor = wire.docker_context.as_mut().unwrap();
        match change {
            0 => descriptor.machine_id.clear(),
            1 => descriptor.environment_id = EnvironmentId::generate().to_string(),
            2 => descriptor.incarnation_id = MachineIncarnationId::generate().to_string(),
            3 => descriptor.incarnation_generation += 1,
            4 => descriptor.endpoint.clear(),
            5 => wire.profile = runtime_v2::MachineProfile::Hardened as i32,
            _ => descriptor.schema_version = 0,
        }
        assert!(machine_instance_from_proto(&wire).is_err());
    }
}

#[test]
fn docker_context_optional_absence_is_preserved_in_wire_and_json() {
    let mut machine = machine();
    machine.docker_context = None;
    let wire = machine_instance_to_proto(&machine);
    assert!(wire.docker_context.is_none());
    let decoded = machine_instance_from_proto(&wire).unwrap();
    assert!(
        serde_json::to_value(decoded)
            .unwrap()
            .get("docker_context")
            .is_none()
    );
}
