//! Runtime V2 backend conformance checks shared across CI lanes.
//!
//! These tests codify Linux, macOS, and cross-backend parity expectations
//! without requiring full VM lifecycle integration.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;

use vz_runtime_contract::{
    Checkpoint, CheckpointClass, CheckpointCompatibilityMetadata, CheckpointMetadata,
    CheckpointState, RuntimeError, SandboxBackend, backend_capability_matrix,
    canonical_backend_capabilities, validate_backend_adapter_contract_surface,
    validate_backend_adapter_parity, validate_checkpoint_restore_compatibility,
};

fn compatibility(version: &str) -> CheckpointCompatibilityMetadata {
    CheckpointCompatibilityMetadata {
        backend_id: "macos-vz".to_string(),
        backend_version: version.to_string(),
        runtime_version: "2".to_string(),
        guest_artifact_versions: BTreeMap::new(),
        config_hash: "sha256:cfg".to_string(),
        host_compatibility_markers: BTreeMap::new(),
    }
}

fn checkpoint_metadata(
    checkpoint_id: &str,
    class: CheckpointClass,
    fingerprint: &str,
) -> CheckpointMetadata {
    CheckpointMetadata::new(
        Checkpoint {
            checkpoint_id: checkpoint_id.to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: None,
            class,
            state: CheckpointState::Ready,
            created_at: 1,
            compatibility_fingerprint: fingerprint.to_string(),
        },
        compatibility("0.1.0"),
    )
}

#[test]
fn backend_conformance_cross_backend_contract_surface_is_stable() {
    validate_backend_adapter_contract_surface().unwrap();
}

#[test]
fn backend_conformance_cross_backend_capability_matrix_matches_plan() {
    let macos = canonical_backend_capabilities(&SandboxBackend::MacosVz);
    let linux = canonical_backend_capabilities(&SandboxBackend::LinuxFirecracker);

    validate_backend_adapter_parity(macos).unwrap();
    validate_backend_adapter_parity(linux).unwrap();
    assert_eq!(
        backend_capability_matrix(macos),
        backend_capability_matrix(linux)
    );
}

#[test]
fn backend_conformance_cross_backend_restore_requires_explicit_class_degradation_ack() {
    let metadata = checkpoint_metadata("ckpt-root", CheckpointClass::FsQuick, "fp-root");

    let err = validate_checkpoint_restore_compatibility(
        &metadata,
        "fp-root",
        Some(&metadata.compatibility),
        CheckpointClass::VmFull,
        false,
    )
    .unwrap_err();
    match err {
        RuntimeError::UnsupportedOperation { operation, reason } => {
            assert_eq!(operation, "restore_checkpoint");
            assert!(reason.contains("degradation"));
        }
        other => panic!("expected unsupported operation, got: {other:?}"),
    }

    validate_checkpoint_restore_compatibility(
        &metadata,
        "fp-root",
        Some(&metadata.compatibility),
        CheckpointClass::VmFull,
        true,
    )
    .unwrap();
}

#[test]
fn backend_conformance_linux_capability_profile_is_explicit() {
    let capabilities = canonical_backend_capabilities(&SandboxBackend::LinuxFirecracker);
    let matrix = backend_capability_matrix(capabilities);

    assert!(matrix.fs_quick_checkpoint);
    assert!(!matrix.vm_full_checkpoint);
    assert!(matrix.checkpoint_fork);
    assert!(!matrix.docker_compat);
    assert!(matrix.compose_adapter);
    assert!(!matrix.gpu_passthrough);
    assert!(!matrix.live_resize);
}

#[cfg(target_os = "macos")]
#[test]
fn backend_conformance_macos_capability_profile_matches_contract() {
    use tempfile::tempdir;
    use vz_oci_macos::RuntimeConfig;

    let tmp = tempdir().unwrap();
    let runtime = vz_oci_macos::Runtime::new(RuntimeConfig {
        data_dir: tmp.path().join("backend-conformance"),
        ..RuntimeConfig::default()
    });

    let capabilities = runtime.checkpoint_capabilities();
    validate_backend_adapter_parity(capabilities).unwrap();
    assert_eq!(
        backend_capability_matrix(capabilities),
        backend_capability_matrix(canonical_backend_capabilities(&SandboxBackend::MacosVz))
    );
}
