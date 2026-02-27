use crate::{
    CheckpointClass, CheckpointCompatibilityMetadata, CheckpointMetadata, RuntimeCapabilities,
    RuntimeError, RuntimeOperation,
};

/// Validate checkpoint restore compatibility constraints.
///
/// Returns `RuntimeError::InvalidConfig` with explicit mismatch details when
/// fingerprint or compatibility metadata constraints are violated.
///
/// Returns `RuntimeError::UnsupportedOperation` when checkpoint class semantics
/// would degrade without explicit caller acknowledgement.
pub fn validate_checkpoint_restore_compatibility(
    metadata: &CheckpointMetadata,
    expected_fingerprint: &str,
    expected_compatibility: Option<&CheckpointCompatibilityMetadata>,
    expected_class: CheckpointClass,
    allow_class_degradation: bool,
) -> Result<(), RuntimeError> {
    let actual_fingerprint = metadata.checkpoint.compatibility_fingerprint.as_str();
    if actual_fingerprint != expected_fingerprint {
        return Err(RuntimeError::InvalidConfig(format!(
            "checkpoint {} compatibility fingerprint mismatch: expected `{expected_fingerprint}`, got `{actual_fingerprint}`",
            metadata.checkpoint.checkpoint_id
        )));
    }

    let Some(expected) = expected_compatibility else {
        return Ok(());
    };

    let actual = &metadata.compatibility;
    let mut mismatches = Vec::new();
    if actual.backend_id != expected.backend_id {
        mismatches.push(format!(
            "backend_id expected `{}`, got `{}`",
            expected.backend_id, actual.backend_id
        ));
    }
    if actual.backend_version != expected.backend_version {
        mismatches.push(format!(
            "backend_version expected `{}`, got `{}`",
            expected.backend_version, actual.backend_version
        ));
    }
    if actual.runtime_version != expected.runtime_version {
        mismatches.push(format!(
            "runtime_version expected `{}`, got `{}`",
            expected.runtime_version, actual.runtime_version
        ));
    }
    if actual.config_hash != expected.config_hash {
        mismatches.push(format!(
            "config_hash expected `{}`, got `{}`",
            expected.config_hash, actual.config_hash
        ));
    }
    if actual.guest_artifact_versions != expected.guest_artifact_versions {
        mismatches.push("guest_artifact_versions differ".to_string());
    }
    if actual.host_compatibility_markers != expected.host_compatibility_markers {
        mismatches.push("host_compatibility_markers differ".to_string());
    }

    if mismatches.is_empty() {
        // Continue to class validation below.
    } else {
        return Err(RuntimeError::InvalidConfig(format!(
            "checkpoint {} is incompatible for restore: {}",
            metadata.checkpoint.checkpoint_id,
            mismatches.join("; ")
        )));
    }

    let actual_class = metadata.checkpoint.class;
    if actual_class == expected_class {
        return Ok(());
    }

    let is_degradation = matches!(
        (expected_class, actual_class),
        (CheckpointClass::VmFull, CheckpointClass::FsQuick)
    );
    if is_degradation && allow_class_degradation {
        return Ok(());
    }

    let expected_label = checkpoint_class_label(expected_class);
    let actual_label = checkpoint_class_label(actual_class);
    let reason = if is_degradation {
        format!(
            "checkpoint class degradation for restore_checkpoint: expected `{expected_label}`, got `{actual_label}`; set allow_class_degradation=true to acknowledge fallback"
        )
    } else {
        format!(
            "checkpoint class mismatch for restore_checkpoint: expected `{expected_label}`, got `{actual_label}`"
        )
    };

    Err(RuntimeError::UnsupportedOperation {
        operation: RuntimeOperation::RestoreCheckpoint.as_str().to_string(),
        reason,
    })
}

fn checkpoint_class_label(class: CheckpointClass) -> &'static str {
    match class {
        CheckpointClass::FsQuick => "fs_quick",
        CheckpointClass::VmFull => "vm_full",
    }
}

/// Validate checkpoint-class capability gating for an operation.
pub fn ensure_checkpoint_class_supported(
    capabilities: RuntimeCapabilities,
    class: CheckpointClass,
    operation: RuntimeOperation,
) -> Result<(), RuntimeError> {
    let supported = match class {
        CheckpointClass::FsQuick => capabilities.fs_quick_checkpoint,
        CheckpointClass::VmFull => capabilities.vm_full_checkpoint,
    };
    if supported {
        return Ok(());
    }

    let missing_capability = match class {
        CheckpointClass::FsQuick => "fs_quick_checkpoint",
        CheckpointClass::VmFull => "vm_full_checkpoint",
    };
    Err(RuntimeError::UnsupportedOperation {
        operation: operation.as_str().to_string(),
        reason: format!("missing {missing_capability} capability"),
    })
}
