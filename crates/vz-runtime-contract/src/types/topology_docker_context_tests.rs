#![allow(clippy::unwrap_used)]
use super::*;

fn fixture() -> (EnvironmentInstance, MachineActivationEvidence) {
    let definition: ProjectDefinition = serde_json::from_value(serde_json::json!({
        "schema_version":1,"project_id":ProjectId::generate(),"name":"docker-context",
        "environment":{"schema_version":1,"machines":[{"schema_version":1,"name":"app","profile":"developer","target":{"os":"linux","arch":"aarch64","image":"fixture"}}]}
    })).unwrap();
    let environment = definition.instantiate_environment("one", 1).unwrap();
    let machine = &environment.machines[0];
    let incarnation = MachineIncarnation {
        schema_version: 1,
        incarnation_id: MachineIncarnationId::generate(),
        machine_id: machine.machine_id.clone(),
        generation: 1,
        created_at: 2,
    };
    let context = MachineDockerContextDescriptor {
        schema_version: 1,
        owner: ResourceOwner {
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(machine.machine_id.clone()),
        },
        name: "vz-machine-context".into(),
        endpoint: "unix:///private/exact.sock".into(),
        config_dir: "/private/client-config".into(),
        engine_id: "engine-exact".into(),
        incarnation_id: incarnation.incarnation_id.clone(),
        incarnation_generation: 1,
    };
    let evidence = MachineActivationEvidence {
        schema_version: 1,
        backend: MachineBackend::MacosVirtualizationLinux,
        negotiated_capabilities: CapabilitySet::new([
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ]),
        runtime_identity: MachineRuntimeIdentity {
            schema_version: 1,
            opaque_id: "exact-runtime".into(),
        },
        incarnation,
        docker_context: Some(context),
    };
    (environment, evidence)
}
fn up(
    environment: &mut EnvironmentInstance,
    evidence: MachineActivationEvidence,
) -> Result<EnvironmentLifecycleOperation, TopologyLifecycleError> {
    let mut operation = EnvironmentLifecycleOperation::plan(
        environment,
        LifecycleOperationId::generate(),
        EnvironmentLifecycleKind::Up,
        "request",
        "key",
        "sha256:fixture",
        2,
    )?;
    operation.begin(environment, 2)?;
    let step = &operation.machine_steps[0];
    let acknowledgement = MachineLifecycleStepAcknowledgement {
        operation_id: operation.operation_id.clone(),
        generation: operation.generation,
        machine_id: step.machine_id.clone(),
        initial_state: step.initial_state,
        target_state: step.target_state,
        expected_incarnation: step.expected_incarnation.clone(),
        resulting_incarnation: Some(evidence.incarnation.clone()),
        resulting_activation: Some(evidence),
        result: LifecycleStepResult::Succeeded,
    };
    operation.apply_machine_step_acknowledgement(environment, &acknowledgement, 3)?;
    operation.finish_live_transition(environment, 4)?;
    Ok(operation)
}

#[test]
fn docker_context_roundtrips_and_stop_preserves_exact_logical_identity() {
    let (mut environment, evidence) = fixture();
    let context = evidence.docker_context.clone();
    up(&mut environment, evidence).unwrap();
    environment.validate().unwrap();
    assert_eq!(environment.machines[0].docker_context, context);
    let encoded = serde_json::to_vec(&environment).unwrap();
    assert_eq!(
        serde_json::from_slice::<EnvironmentInstance>(&encoded).unwrap(),
        environment
    );
    let mut stop = EnvironmentLifecycleOperation::plan(
        &environment,
        LifecycleOperationId::generate(),
        EnvironmentLifecycleKind::Stop,
        "stop",
        "stop-key",
        "sha256:stop",
        5,
    )
    .unwrap();
    stop.begin(&mut environment, 5).unwrap();
    let step = &stop.machine_steps[0];
    stop.apply_machine_step_acknowledgement(
        &mut environment,
        &MachineLifecycleStepAcknowledgement {
            operation_id: stop.operation_id.clone(),
            generation: stop.generation,
            machine_id: step.machine_id.clone(),
            initial_state: step.initial_state,
            target_state: step.target_state,
            expected_incarnation: step.expected_incarnation.clone(),
            resulting_incarnation: None,
            resulting_activation: None,
            result: LifecycleStepResult::Succeeded,
        },
        6,
    )
    .unwrap();
    stop.finish_live_transition(&mut environment, 7).unwrap();
    environment.validate().unwrap();
    assert_eq!(environment.machines[0].state, MachineState::Stopped);
    assert_eq!(environment.machines[0].docker_context, context);
}

#[test]
fn docker_context_foreign_owner_or_incarnation_never_activates() {
    for change in 0..5 {
        let (mut environment, mut evidence) = fixture();
        let context = evidence.docker_context.as_mut().unwrap();
        match change {
            0 => context.owner.project_id = ProjectId::generate(),
            1 => context.owner.environment_id = EnvironmentId::generate(),
            2 => context.owner.machine_id = Some(MachineId::generate()),
            3 => context.incarnation_id = MachineIncarnationId::generate(),
            _ => context.incarnation_generation += 1,
        }
        assert!(up(&mut environment, evidence).is_err());
        assert!(environment.machines[0].docker_context.is_none());
        assert_ne!(environment.machines[0].state, MachineState::Ready);
    }
}

#[test]
fn docker_context_is_forbidden_for_hardened_and_native_machines() {
    for native in [false, true] {
        let (mut environment, mut evidence) = fixture();
        evidence.negotiated_capabilities = CapabilitySet::default();
        if native {
            environment.machines[0].target.os = OperatingSystem::Macos;
            evidence.backend = MachineBackend::MacosNative;
        } else {
            environment.machines[0].profile = MachineProfile::Hardened;
        }
        assert!(
            validate_activation_evidence_for_machine(&environment.machines[0], &evidence).is_err()
        );
    }
}

#[test]
fn docker_context_cannot_infer_required_capabilities() {
    let (mut environment, mut evidence) = fixture();
    evidence.negotiated_capabilities = CapabilitySet::new([MachineCapability::PosixExec]);
    assert!(up(&mut environment, evidence).is_err());
    assert!(
        !environment.machines[0]
            .negotiated_capabilities
            .contains(MachineCapability::DockerEngine)
    );
}

#[test]
fn docker_context_owner_and_bounds_validate_without_host_path_reinterpretation() {
    let (_, evidence) = fixture();
    let context = evidence.docker_context.unwrap();
    for change in 0..7 {
        let mut invalid = context.clone();
        match change {
            0 => invalid.owner.machine_id = None,
            1 => invalid.name.clear(),
            2 => invalid.endpoint = "x".repeat(4097),
            3 => invalid.config_dir.push('\n'),
            4 => invalid.engine_id = " ".into(),
            5 => invalid.incarnation_generation = 0,
            _ => invalid.schema_version = 99,
        }
        assert!(invalid.validate().is_err());
    }
    let mut windows = context;
    windows.endpoint = "npipe:////./pipe/exact-machine".into();
    windows.config_dir = "C:\\Users\\fixture\\private-config".into();
    windows.validate().unwrap();
}

#[test]
fn absent_docker_context_keeps_historical_json_bytes_unchanged() {
    let (environment, mut evidence) = fixture();
    let machine = &environment.machines[0];
    let machine_json = serde_json::to_value(machine).unwrap();
    assert!(machine_json.get("docker_context").is_none());
    assert_eq!(
        serde_json::from_value::<MachineInstance>(machine_json.clone()).unwrap(),
        *machine
    );
    assert_eq!(
        serde_json::to_value(
            serde_json::from_value::<MachineInstance>(machine_json.clone()).unwrap()
        )
        .unwrap(),
        machine_json
    );
    evidence.docker_context = None;
    let json = serde_json::to_value(&evidence).unwrap();
    assert!(json.get("docker_context").is_none());
    assert_eq!(
        serde_json::from_value::<MachineActivationEvidence>(json).unwrap(),
        evidence
    );
}

#[test]
fn docker_context_project_tampering_rejected_by_aggregate_and_journal() {
    let (mut environment, evidence) = fixture();
    let mut operation = up(&mut environment, evidence).unwrap();
    environment.machines[0]
        .docker_context
        .as_mut()
        .unwrap()
        .owner
        .project_id = ProjectId::generate();
    assert!(environment.validate().is_err());
    operation.machine_steps[0]
        .resulting_activation
        .as_mut()
        .unwrap()
        .docker_context
        .as_mut()
        .unwrap()
        .owner
        .project_id = ProjectId::generate();
    assert!(operation.validate_structure().is_err());
}

#[test]
fn docker_context_requires_exact_ownership_row_and_cannot_be_dropped_or_renamed() {
    let (mut environment, evidence) = fixture();
    up(&mut environment, evidence.clone()).unwrap();
    assert!(
        environment
            .ownership
            .iter()
            .any(|row| row.resource_kind == OwnedResourceKind::DockerContext
                && row.resource_id == evidence.docker_context.as_ref().unwrap().name)
    );
    let mut missing = environment.clone();
    missing
        .ownership
        .retain(|row| row.resource_kind != OwnedResourceKind::DockerContext);
    assert!(missing.validate().is_err());
    for rename in [false, true] {
        let mut changed = evidence.clone();
        if rename {
            changed.docker_context.as_mut().unwrap().name = "different-context".into();
        } else {
            changed.docker_context = None;
        }
        let mut candidate = environment.clone();
        assert!(up(&mut candidate, changed).is_err());
        assert_eq!(
            candidate.machines[0].docker_context,
            environment.machines[0].docker_context
        );
    }
}

#[test]
fn docker_context_terminal_replay_compares_engine_endpoint_and_configuration() {
    let (mut environment, evidence) = fixture();
    let operation = up(&mut environment, evidence.clone()).unwrap();
    for field in 0..3 {
        let mut changed = evidence.clone();
        let context = changed.docker_context.as_mut().unwrap();
        match field {
            0 => context.engine_id = "other-engine".into(),
            1 => context.endpoint = "unix:///other.sock".into(),
            _ => context.config_dir = "/other/client".into(),
        }
        let step = &operation.machine_steps[0];
        let ack = MachineLifecycleStepAcknowledgement {
            operation_id: operation.operation_id.clone(),
            generation: operation.generation,
            machine_id: step.machine_id.clone(),
            initial_state: step.initial_state,
            target_state: step.target_state,
            expected_incarnation: step.expected_incarnation.clone(),
            resulting_incarnation: Some(changed.incarnation.clone()),
            resulting_activation: Some(changed),
            result: LifecycleStepResult::Succeeded,
        };
        let mut replay = operation.clone();
        let mut candidate = environment.clone();
        assert!(
            replay
                .apply_machine_step_acknowledgement(&mut candidate, &ack, 5)
                .is_err()
        );
        assert_eq!(candidate, environment);
    }
}
