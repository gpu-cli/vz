#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;

use vz_runtime_contract::{
    Checkpoint, CheckpointClass, CheckpointCompatibilityMetadata, CheckpointMetadata,
    DockerShimCommand, MachineErrorCode, RequestMetadata, RuntimeError, RuntimeOperation,
    SandboxBackend, SharedVmPhase, SharedVmPhaseTracker, canonical_backend_capabilities,
    runtime_error_machine_envelope, validate_backend_adapter_parity,
    validate_checkpoint_restore_compatibility, validate_request_metadata_for_operation,
};
use vz_stack::{DependencyCondition, parse_compose};

#[test]
fn quality_gate_state_machine_invariants_are_enforced() {
    let mut tracker = SharedVmPhaseTracker::new();
    tracker.transition_to(SharedVmPhase::Booting).unwrap();
    tracker.transition_to(SharedVmPhase::Ready).unwrap();

    let err = tracker.transition_to(SharedVmPhase::Booting).unwrap_err();
    assert!(
        err.to_string().contains("shared VM phase transition"),
        "unexpected transition error: {err}"
    );
}

#[test]
fn quality_gate_backend_contract_baseline_holds() {
    let linux_caps = canonical_backend_capabilities(&SandboxBackend::LinuxFirecracker);
    validate_backend_adapter_parity(linux_caps).unwrap();

    let macos_caps = canonical_backend_capabilities(&SandboxBackend::MacosVz);
    validate_backend_adapter_parity(macos_caps).unwrap();
}

#[test]
fn quality_gate_transport_parity_machine_envelope_shape() {
    let metadata = RequestMetadata::from_optional_refs(Some("req-quality"), None);
    let error = RuntimeError::UnsupportedOperation {
        operation: "create_checkpoint".to_string(),
        reason: "missing vm_full_checkpoint capability".to_string(),
    };

    let envelope = runtime_error_machine_envelope(&error, &metadata);
    assert_eq!(envelope.error.code, MachineErrorCode::UnsupportedOperation);
    assert_eq!(envelope.error.request_id.as_deref(), Some("req-quality"));
    assert_eq!(
        envelope.error.details.get("operation").map(String::as_str),
        Some("create_checkpoint")
    );
}

#[test]
fn quality_gate_idempotency_negative_missing_key_is_rejected() {
    let metadata = RequestMetadata::default();
    let err = validate_request_metadata_for_operation(RuntimeOperation::CreateContainer, &metadata)
        .unwrap_err();

    assert!(matches!(err, RuntimeError::InvalidConfig(_)));
    assert!(err.to_string().contains("idempotency_key"));
}

#[test]
fn quality_gate_checkpoint_compatibility_mismatch_fails() {
    let compatibility = CheckpointCompatibilityMetadata {
        backend_id: "linux-native".to_string(),
        backend_version: "0.1.0".to_string(),
        runtime_version: "2".to_string(),
        guest_artifact_versions: BTreeMap::new(),
        config_hash: "sha256:cfg".to_string(),
        host_compatibility_markers: BTreeMap::new(),
    };
    let metadata = CheckpointMetadata::new(
        Checkpoint {
            checkpoint_id: "ckpt-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: vz_runtime_contract::CheckpointState::Ready,
            created_at: 10,
            compatibility_fingerprint: "fp-a".to_string(),
        },
        compatibility.clone(),
    );

    let err = validate_checkpoint_restore_compatibility(
        &metadata,
        "fp-b",
        Some(&compatibility),
        CheckpointClass::FsQuick,
        false,
    )
    .unwrap_err();

    assert!(matches!(err, RuntimeError::InvalidConfig(_)));
    assert!(
        err.to_string()
            .contains("compatibility fingerprint mismatch")
    );
}

#[test]
fn quality_gate_docker_compose_shim_mapping_is_stable() {
    assert_eq!(
        DockerShimCommand::Build.runtime_operation(),
        Some(RuntimeOperation::StartBuild)
    );
    assert_eq!(
        DockerShimCommand::Exec.runtime_operation(),
        Some(RuntimeOperation::ExecContainer)
    );

    let compose = r#"
services:
  db:
    image: postgres:16
    healthcheck:
      test: [\"CMD\", \"pg_isready\", \"-U\", \"postgres\"]
  web:
    image: nginx:latest
    depends_on:
      db:
        condition: service_healthy
"#;

    let spec = parse_compose(compose, "quality-gate-stack").unwrap();
    let web = spec
        .services
        .iter()
        .find(|service| service.name == "web")
        .unwrap();
    assert_eq!(web.depends_on.len(), 1);
    assert_eq!(web.depends_on[0].service, "db");
    assert_eq!(
        web.depends_on[0].condition,
        DependencyCondition::ServiceHealthy
    );
}
