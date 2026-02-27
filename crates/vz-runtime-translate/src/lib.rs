#![forbid(unsafe_code)]

//! Deterministic translation between Runtime V2 proto messages and runtime-domain types.
//!
//! Semantics:
//! - Unknown enum/state/capability strings are rejected with [`TranslationError`].
//! - Proto fields that encode optional values as empty strings/zero numbers are normalized
//!   back into `Option` domain fields.
//! - Event payloads are encoded/decoded through a stable JSON envelope in
//!   `runtime_v2::RuntimeEvent.event_json`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use vz_runtime_contract::{
    Build, BuildSpec, BuildState, Checkpoint, CheckpointClass, CheckpointState, Container,
    ContainerSpec, ContainerState, Event, EventScope, Execution, ExecutionSpec, ExecutionState,
    Lease, LeaseState, MachineError, MachineErrorCode, RequestMetadata, RuntimeCapabilities,
    SANDBOX_LABEL_BASE_IMAGE_REF, SANDBOX_LABEL_MAIN_CONTAINER, Sandbox, SandboxBackend,
    SandboxSpec, SandboxState,
};
use vz_runtime_proto::runtime_v2;

/// Conversion failures between Runtime V2 wire messages and domain entities.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TranslationError {
    #[error("invalid enum value for `{field}`: `{value}`")]
    InvalidEnumValue { field: &'static str, value: String },
    #[error("invalid value for `{field}`: `{value}`")]
    InvalidValue { field: &'static str, value: String },
    #[error("duplicate capability entry: `{name}`")]
    DuplicateCapability { name: String },
    #[error("unknown capability entry: `{name}`")]
    UnknownCapability { name: String },
    #[error("invalid json for `{field}`: {details}")]
    InvalidJson {
        field: &'static str,
        details: String,
    },
}

/// Convert domain request metadata into wire metadata.
pub fn request_metadata_to_proto(metadata: &RequestMetadata) -> runtime_v2::RequestMetadata {
    runtime_v2::RequestMetadata {
        request_id: metadata.request_id.clone().unwrap_or_default(),
        idempotency_key: metadata.idempotency_key.clone().unwrap_or_default(),
        trace_id: metadata.trace_id.clone().unwrap_or_default(),
    }
}

/// Convert wire request metadata into domain metadata.
pub fn request_metadata_from_proto(metadata: &runtime_v2::RequestMetadata) -> RequestMetadata {
    RequestMetadata::from_optional_refs(
        normalize_optional_wire_field(&metadata.request_id).as_deref(),
        normalize_optional_wire_field(&metadata.idempotency_key).as_deref(),
    )
    .with_trace_id(normalize_optional_wire_field(&metadata.trace_id))
}

/// Convert a domain machine error into wire error detail.
pub fn machine_error_to_proto_detail(error: &MachineError) -> runtime_v2::ErrorDetail {
    runtime_v2::ErrorDetail {
        code: error.code.as_str().to_string(),
        message: error.message.clone(),
        request_id: error.request_id.clone().unwrap_or_default(),
    }
}

/// Convert a wire error detail into a domain machine error.
pub fn machine_error_from_proto_detail(
    detail: &runtime_v2::ErrorDetail,
) -> Result<MachineError, TranslationError> {
    let code = machine_error_code_from_wire(&detail.code)?;
    Ok(MachineError::new(
        code,
        detail.message.clone(),
        normalize_optional_wire_field(&detail.request_id),
        BTreeMap::new(),
    ))
}

/// Convert domain sandbox into wire payload.
pub fn sandbox_to_proto_payload(sandbox: &Sandbox) -> runtime_v2::SandboxPayload {
    let mut labels = sandbox.labels.clone();
    if let Some(base_image_ref) = sandbox.spec.base_image_ref.as_ref() {
        labels
            .entry(SANDBOX_LABEL_BASE_IMAGE_REF.to_string())
            .or_insert_with(|| base_image_ref.clone());
    }
    if let Some(main_container) = sandbox.spec.main_container.as_ref() {
        labels
            .entry(SANDBOX_LABEL_MAIN_CONTAINER.to_string())
            .or_insert_with(|| main_container.clone());
    }

    runtime_v2::SandboxPayload {
        sandbox_id: sandbox.sandbox_id.clone(),
        backend: sandbox_backend_to_wire(&sandbox.backend).to_string(),
        state: sandbox_state_to_wire(sandbox.state).to_string(),
        cpus: sandbox.spec.cpus.map(u32::from).unwrap_or_default(),
        memory_mb: sandbox.spec.memory_mb.unwrap_or_default(),
        created_at: sandbox.created_at,
        updated_at: sandbox.updated_at,
        labels: btree_to_hash_map(&labels),
    }
}

/// Convert wire sandbox payload into domain sandbox.
pub fn sandbox_from_proto_payload(
    payload: &runtime_v2::SandboxPayload,
) -> Result<Sandbox, TranslationError> {
    let cpus = if payload.cpus == 0 {
        None
    } else {
        Some(
            u8::try_from(payload.cpus).map_err(|_| TranslationError::InvalidValue {
                field: "sandbox.cpus",
                value: payload.cpus.to_string(),
            })?,
        )
    };
    let labels = hash_to_btree_map(&payload.labels);
    let base_image_ref = labels
        .get(SANDBOX_LABEL_BASE_IMAGE_REF)
        .and_then(|value| normalize_optional_wire_field(value));
    let main_container = labels
        .get(SANDBOX_LABEL_MAIN_CONTAINER)
        .and_then(|value| normalize_optional_wire_field(value));

    Ok(Sandbox {
        sandbox_id: payload.sandbox_id.clone(),
        backend: sandbox_backend_from_wire(&payload.backend),
        spec: SandboxSpec {
            cpus,
            memory_mb: none_if_zero(payload.memory_mb),
            base_image_ref,
            main_container,
            network_profile: None,
            volume_mounts: Vec::new(),
        },
        state: sandbox_state_from_wire(&payload.state)?,
        created_at: payload.created_at,
        updated_at: payload.updated_at,
        labels,
    })
}

/// Convert domain lease into wire payload.
pub fn lease_to_proto_payload(lease: &Lease) -> runtime_v2::LeasePayload {
    runtime_v2::LeasePayload {
        lease_id: lease.lease_id.clone(),
        sandbox_id: lease.sandbox_id.clone(),
        ttl_secs: lease.ttl_secs,
        last_heartbeat_at: lease.last_heartbeat_at,
        state: lease_state_to_wire(lease.state).to_string(),
    }
}

/// Convert wire lease payload into domain lease.
pub fn lease_from_proto_payload(
    payload: &runtime_v2::LeasePayload,
) -> Result<Lease, TranslationError> {
    Ok(Lease {
        lease_id: payload.lease_id.clone(),
        sandbox_id: payload.sandbox_id.clone(),
        ttl_secs: payload.ttl_secs,
        last_heartbeat_at: payload.last_heartbeat_at,
        state: lease_state_from_wire(&payload.state)?,
    })
}

/// Convert domain container into wire payload.
pub fn container_to_proto_payload(container: &Container) -> runtime_v2::ContainerPayload {
    runtime_v2::ContainerPayload {
        container_id: container.container_id.clone(),
        sandbox_id: container.sandbox_id.clone(),
        image_digest: container.image_digest.clone(),
        state: container_state_to_wire(container.state).to_string(),
        created_at: container.created_at,
        started_at: container.started_at.unwrap_or_default(),
        ended_at: container.ended_at.unwrap_or_default(),
    }
}

/// Convert wire container payload into domain container.
pub fn container_from_proto_payload(
    payload: &runtime_v2::ContainerPayload,
) -> Result<Container, TranslationError> {
    Ok(Container {
        container_id: payload.container_id.clone(),
        sandbox_id: payload.sandbox_id.clone(),
        image_digest: payload.image_digest.clone(),
        container_spec: ContainerSpec::default(),
        state: container_state_from_wire(&payload.state)?,
        created_at: payload.created_at,
        started_at: none_if_zero(payload.started_at),
        ended_at: none_if_zero(payload.ended_at),
    })
}

/// Convert domain execution into wire payload.
pub fn execution_to_proto_payload(execution: &Execution) -> runtime_v2::ExecutionPayload {
    runtime_v2::ExecutionPayload {
        execution_id: execution.execution_id.clone(),
        container_id: execution.container_id.clone(),
        state: execution_state_to_wire(execution.state).to_string(),
        exit_code: execution.exit_code.unwrap_or_default(),
        started_at: execution.started_at.unwrap_or_default(),
        ended_at: execution.ended_at.unwrap_or_default(),
    }
}

/// Convert wire execution payload into domain execution.
pub fn execution_from_proto_payload(
    payload: &runtime_v2::ExecutionPayload,
) -> Result<Execution, TranslationError> {
    let state = execution_state_from_wire(&payload.state)?;
    let exit_code = match state {
        ExecutionState::Queued | ExecutionState::Running => None,
        ExecutionState::Exited => Some(payload.exit_code),
        ExecutionState::Failed | ExecutionState::Canceled => {
            if payload.exit_code == 0 {
                None
            } else {
                Some(payload.exit_code)
            }
        }
    };

    Ok(Execution {
        execution_id: payload.execution_id.clone(),
        container_id: payload.container_id.clone(),
        exec_spec: ExecutionSpec::default(),
        state,
        exit_code,
        started_at: none_if_zero(payload.started_at),
        ended_at: none_if_zero(payload.ended_at),
    })
}

/// Convert domain checkpoint into wire payload.
pub fn checkpoint_to_proto_payload(checkpoint: &Checkpoint) -> runtime_v2::CheckpointPayload {
    runtime_v2::CheckpointPayload {
        checkpoint_id: checkpoint.checkpoint_id.clone(),
        sandbox_id: checkpoint.sandbox_id.clone(),
        parent_checkpoint_id: checkpoint.parent_checkpoint_id.clone().unwrap_or_default(),
        checkpoint_class: checkpoint_class_to_wire(checkpoint.class).to_string(),
        state: checkpoint_state_to_wire(checkpoint.state).to_string(),
        compatibility_fingerprint: checkpoint.compatibility_fingerprint.clone(),
        created_at: checkpoint.created_at,
    }
}

/// Convert wire checkpoint payload into domain checkpoint.
pub fn checkpoint_from_proto_payload(
    payload: &runtime_v2::CheckpointPayload,
) -> Result<Checkpoint, TranslationError> {
    Ok(Checkpoint {
        checkpoint_id: payload.checkpoint_id.clone(),
        sandbox_id: payload.sandbox_id.clone(),
        parent_checkpoint_id: normalize_optional_wire_field(&payload.parent_checkpoint_id),
        class: checkpoint_class_from_wire(&payload.checkpoint_class)?,
        state: checkpoint_state_from_wire(&payload.state)?,
        created_at: payload.created_at,
        compatibility_fingerprint: payload.compatibility_fingerprint.clone(),
    })
}

/// Convert domain build into wire payload.
pub fn build_to_proto_payload(build: &Build) -> runtime_v2::BuildPayload {
    runtime_v2::BuildPayload {
        build_id: build.build_id.clone(),
        sandbox_id: build.sandbox_id.clone(),
        state: build_state_to_wire(build.state).to_string(),
        result_digest: build.result_digest.clone().unwrap_or_default(),
        started_at: build.started_at,
        ended_at: build.ended_at.unwrap_or_default(),
    }
}

/// Convert wire build payload into domain build.
pub fn build_from_proto_payload(
    payload: &runtime_v2::BuildPayload,
) -> Result<Build, TranslationError> {
    Ok(Build {
        build_id: payload.build_id.clone(),
        sandbox_id: payload.sandbox_id.clone(),
        build_spec: BuildSpec::default(),
        state: build_state_from_wire(&payload.state)?,
        result_digest: normalize_optional_wire_field(&payload.result_digest),
        started_at: payload.started_at,
        ended_at: none_if_zero(payload.ended_at),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct WireRuntimeEvent {
    scope: String,
    scope_id: String,
    event_type: String,
    payload: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_id: Option<String>,
    ts: u64,
}

/// Convert domain runtime event into wire runtime event payload.
pub fn event_to_proto_runtime_event(
    event: &Event,
) -> Result<runtime_v2::RuntimeEvent, TranslationError> {
    let id = i64::try_from(event.event_id).map_err(|_| TranslationError::InvalidValue {
        field: "event.event_id",
        value: event.event_id.to_string(),
    })?;

    let envelope = WireRuntimeEvent {
        scope: event_scope_to_wire(event.scope).to_string(),
        scope_id: event.scope_id.clone(),
        event_type: event.event_type.clone(),
        payload: event.payload.clone(),
        trace_id: event.trace_id.clone(),
        ts: event.ts,
    };

    let event_json =
        serde_json::to_string(&envelope).map_err(|error| TranslationError::InvalidJson {
            field: "runtime_event.event_json",
            details: error.to_string(),
        })?;

    Ok(runtime_v2::RuntimeEvent {
        id,
        // Keep legacy field populated for compatibility surfaces.
        stack_name: event.scope_id.clone(),
        created_at: event.ts.to_string(),
        event_json,
    })
}

/// Convert wire runtime event payload into domain runtime event.
pub fn event_from_proto_runtime_event(
    event: &runtime_v2::RuntimeEvent,
) -> Result<Event, TranslationError> {
    if event.id < 0 {
        return Err(TranslationError::InvalidValue {
            field: "runtime_event.id",
            value: event.id.to_string(),
        });
    }

    let created_at_ts = parse_u64_string_field("runtime_event.created_at", &event.created_at)?;
    let envelope: WireRuntimeEvent =
        serde_json::from_str(&event.event_json).map_err(|error| TranslationError::InvalidJson {
            field: "runtime_event.event_json",
            details: error.to_string(),
        })?;

    let scope_id = if envelope.scope_id.trim().is_empty() {
        event.stack_name.clone()
    } else {
        envelope.scope_id
    };

    if scope_id.trim().is_empty() {
        return Err(TranslationError::InvalidValue {
            field: "runtime_event.scope_id",
            value: String::new(),
        });
    }

    Ok(Event {
        event_id: event.id as u64,
        ts: if envelope.ts == 0 {
            created_at_ts
        } else {
            envelope.ts
        },
        scope: event_scope_from_wire(&envelope.scope)?,
        scope_id,
        event_type: envelope.event_type,
        payload: envelope.payload,
        trace_id: normalize_optional_owned(envelope.trace_id),
    })
}

/// Convert runtime capability flags into wire capability entries.
pub fn runtime_capabilities_to_proto(
    capabilities: RuntimeCapabilities,
) -> Vec<runtime_v2::Capability> {
    vec![
        proto_capability("vm_full_checkpoint", capabilities.vm_full_checkpoint),
        proto_capability("checkpoint_fork", capabilities.checkpoint_fork),
        proto_capability("docker_compat", capabilities.docker_compat),
        proto_capability("compose_adapter", capabilities.compose_adapter),
        proto_capability("build_cache_export", capabilities.build_cache_export),
        proto_capability("gpu_passthrough", capabilities.gpu_passthrough),
        proto_capability("fs_quick_checkpoint", capabilities.fs_quick_checkpoint),
        proto_capability("shared_vm", capabilities.shared_vm),
        proto_capability("stack_networking", capabilities.stack_networking),
        proto_capability("container_logs", capabilities.container_logs),
        proto_capability("live_resize", capabilities.live_resize),
    ]
}

/// Convert wire capability entries into runtime capability flags.
pub fn runtime_capabilities_from_proto(
    capabilities: &[runtime_v2::Capability],
) -> Result<RuntimeCapabilities, TranslationError> {
    let mut seen = BTreeSet::new();
    let mut result = RuntimeCapabilities::default();

    for capability in capabilities {
        let name = capability.name.trim();
        if name.is_empty() {
            return Err(TranslationError::InvalidValue {
                field: "capability.name",
                value: capability.name.clone(),
            });
        }

        if !seen.insert(name.to_string()) {
            return Err(TranslationError::DuplicateCapability {
                name: name.to_string(),
            });
        }

        match name {
            "vm_full_checkpoint" => result.vm_full_checkpoint = capability.enabled,
            "checkpoint_fork" => result.checkpoint_fork = capability.enabled,
            "docker_compat" => result.docker_compat = capability.enabled,
            "compose_adapter" => result.compose_adapter = capability.enabled,
            "build_cache_export" => result.build_cache_export = capability.enabled,
            "gpu_passthrough" => result.gpu_passthrough = capability.enabled,
            "fs_quick_checkpoint" => result.fs_quick_checkpoint = capability.enabled,
            "shared_vm" => result.shared_vm = capability.enabled,
            "stack_networking" => result.stack_networking = capability.enabled,
            "container_logs" => result.container_logs = capability.enabled,
            "live_resize" => result.live_resize = capability.enabled,
            other => {
                return Err(TranslationError::UnknownCapability {
                    name: other.to_string(),
                });
            }
        }
    }

    Ok(result)
}

fn machine_error_code_from_wire(code: &str) -> Result<MachineErrorCode, TranslationError> {
    match code {
        "validation_error" => Ok(MachineErrorCode::ValidationError),
        "not_found" => Ok(MachineErrorCode::NotFound),
        "state_conflict" => Ok(MachineErrorCode::StateConflict),
        "policy_denied" => Ok(MachineErrorCode::PolicyDenied),
        "timeout" => Ok(MachineErrorCode::Timeout),
        "backend_unavailable" => Ok(MachineErrorCode::BackendUnavailable),
        "unsupported_operation" => Ok(MachineErrorCode::UnsupportedOperation),
        "internal_error" => Ok(MachineErrorCode::InternalError),
        other => Err(TranslationError::InvalidEnumValue {
            field: "error.code",
            value: other.to_string(),
        }),
    }
}

fn sandbox_backend_to_wire(backend: &SandboxBackend) -> &str {
    match backend {
        SandboxBackend::MacosVz => "macos_vz",
        SandboxBackend::LinuxFirecracker => "linux_firecracker",
        SandboxBackend::Other(name) => name.as_str(),
    }
}

fn sandbox_backend_from_wire(backend: &str) -> SandboxBackend {
    match backend {
        "macos_vz" => SandboxBackend::MacosVz,
        "linux_firecracker" => SandboxBackend::LinuxFirecracker,
        other if other.trim().is_empty() => SandboxBackend::Other("unknown".to_string()),
        other => SandboxBackend::Other(other.to_string()),
    }
}

fn sandbox_state_to_wire(state: SandboxState) -> &'static str {
    match state {
        SandboxState::Creating => "creating",
        SandboxState::Ready => "ready",
        SandboxState::Draining => "draining",
        SandboxState::Terminated => "terminated",
        SandboxState::Failed => "failed",
    }
}

fn sandbox_state_from_wire(state: &str) -> Result<SandboxState, TranslationError> {
    match state {
        "creating" => Ok(SandboxState::Creating),
        "ready" => Ok(SandboxState::Ready),
        "draining" => Ok(SandboxState::Draining),
        "terminated" => Ok(SandboxState::Terminated),
        "failed" => Ok(SandboxState::Failed),
        other => Err(TranslationError::InvalidEnumValue {
            field: "sandbox.state",
            value: other.to_string(),
        }),
    }
}

fn lease_state_to_wire(state: LeaseState) -> &'static str {
    match state {
        LeaseState::Opening => "opening",
        LeaseState::Active => "active",
        LeaseState::Expired => "expired",
        LeaseState::Closed => "closed",
        LeaseState::Failed => "failed",
    }
}

fn lease_state_from_wire(state: &str) -> Result<LeaseState, TranslationError> {
    match state {
        "opening" => Ok(LeaseState::Opening),
        "active" => Ok(LeaseState::Active),
        "expired" => Ok(LeaseState::Expired),
        "closed" => Ok(LeaseState::Closed),
        "failed" => Ok(LeaseState::Failed),
        other => Err(TranslationError::InvalidEnumValue {
            field: "lease.state",
            value: other.to_string(),
        }),
    }
}

fn container_state_to_wire(state: ContainerState) -> &'static str {
    match state {
        ContainerState::Created => "created",
        ContainerState::Starting => "starting",
        ContainerState::Running => "running",
        ContainerState::Stopping => "stopping",
        ContainerState::Exited => "exited",
        ContainerState::Failed => "failed",
        ContainerState::Removed => "removed",
    }
}

fn container_state_from_wire(state: &str) -> Result<ContainerState, TranslationError> {
    match state {
        "created" => Ok(ContainerState::Created),
        "starting" => Ok(ContainerState::Starting),
        "running" => Ok(ContainerState::Running),
        "stopping" => Ok(ContainerState::Stopping),
        "exited" => Ok(ContainerState::Exited),
        "failed" => Ok(ContainerState::Failed),
        "removed" => Ok(ContainerState::Removed),
        other => Err(TranslationError::InvalidEnumValue {
            field: "container.state",
            value: other.to_string(),
        }),
    }
}

fn execution_state_to_wire(state: ExecutionState) -> &'static str {
    match state {
        ExecutionState::Queued => "queued",
        ExecutionState::Running => "running",
        ExecutionState::Exited => "exited",
        ExecutionState::Failed => "failed",
        ExecutionState::Canceled => "canceled",
    }
}

fn execution_state_from_wire(state: &str) -> Result<ExecutionState, TranslationError> {
    match state {
        "queued" => Ok(ExecutionState::Queued),
        "running" => Ok(ExecutionState::Running),
        "exited" => Ok(ExecutionState::Exited),
        "failed" => Ok(ExecutionState::Failed),
        "canceled" => Ok(ExecutionState::Canceled),
        other => Err(TranslationError::InvalidEnumValue {
            field: "execution.state",
            value: other.to_string(),
        }),
    }
}

fn checkpoint_class_to_wire(class: CheckpointClass) -> &'static str {
    match class {
        CheckpointClass::FsQuick => "fs_quick",
        CheckpointClass::VmFull => "vm_full",
    }
}

fn checkpoint_class_from_wire(class: &str) -> Result<CheckpointClass, TranslationError> {
    match class {
        "fs_quick" => Ok(CheckpointClass::FsQuick),
        "vm_full" => Ok(CheckpointClass::VmFull),
        other => Err(TranslationError::InvalidEnumValue {
            field: "checkpoint.class",
            value: other.to_string(),
        }),
    }
}

fn checkpoint_state_to_wire(state: CheckpointState) -> &'static str {
    match state {
        CheckpointState::Creating => "creating",
        CheckpointState::Ready => "ready",
        CheckpointState::Failed => "failed",
    }
}

fn checkpoint_state_from_wire(state: &str) -> Result<CheckpointState, TranslationError> {
    match state {
        "creating" => Ok(CheckpointState::Creating),
        "ready" => Ok(CheckpointState::Ready),
        "failed" => Ok(CheckpointState::Failed),
        other => Err(TranslationError::InvalidEnumValue {
            field: "checkpoint.state",
            value: other.to_string(),
        }),
    }
}

fn build_state_to_wire(state: BuildState) -> &'static str {
    match state {
        BuildState::Queued => "queued",
        BuildState::Running => "running",
        BuildState::Succeeded => "succeeded",
        BuildState::Failed => "failed",
        BuildState::Canceled => "canceled",
    }
}

fn build_state_from_wire(state: &str) -> Result<BuildState, TranslationError> {
    match state {
        "queued" => Ok(BuildState::Queued),
        "running" => Ok(BuildState::Running),
        "succeeded" => Ok(BuildState::Succeeded),
        "failed" => Ok(BuildState::Failed),
        "canceled" => Ok(BuildState::Canceled),
        other => Err(TranslationError::InvalidEnumValue {
            field: "build.state",
            value: other.to_string(),
        }),
    }
}

fn event_scope_to_wire(scope: EventScope) -> &'static str {
    match scope {
        EventScope::Sandbox => "sandbox",
        EventScope::Lease => "lease",
        EventScope::Build => "build",
        EventScope::Container => "container",
        EventScope::Execution => "execution",
        EventScope::Checkpoint => "checkpoint",
        EventScope::System => "system",
    }
}

fn event_scope_from_wire(scope: &str) -> Result<EventScope, TranslationError> {
    match scope {
        "sandbox" => Ok(EventScope::Sandbox),
        "lease" => Ok(EventScope::Lease),
        "build" => Ok(EventScope::Build),
        "container" => Ok(EventScope::Container),
        "execution" => Ok(EventScope::Execution),
        "checkpoint" => Ok(EventScope::Checkpoint),
        "system" => Ok(EventScope::System),
        other => Err(TranslationError::InvalidEnumValue {
            field: "event.scope",
            value: other.to_string(),
        }),
    }
}

fn proto_capability(name: &str, enabled: bool) -> runtime_v2::Capability {
    runtime_v2::Capability {
        name: name.to_string(),
        enabled,
    }
}

fn parse_u64_string_field(field: &'static str, value: &str) -> Result<u64, TranslationError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(0);
    }

    trimmed
        .parse::<u64>()
        .map_err(|_| TranslationError::InvalidValue {
            field,
            value: value.to_string(),
        })
}

fn normalize_optional_wire_field(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn normalize_optional_owned(value: Option<String>) -> Option<String> {
    value.and_then(|value| normalize_optional_wire_field(&value))
}

fn none_if_zero(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

fn btree_to_hash_map(
    input: &BTreeMap<String, String>,
) -> std::collections::HashMap<String, String> {
    input.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn hash_to_btree_map(
    input: &std::collections::HashMap<String, String>,
) -> BTreeMap<String, String> {
    input.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request_metadata() -> RequestMetadata {
        RequestMetadata::new(Some(" req-1 ".to_string()), Some(" idem-1 ".to_string()))
            .with_trace_id(Some(" trace-1 ".to_string()))
    }

    #[test]
    fn request_metadata_round_trip_normalizes_fields() {
        let metadata = sample_request_metadata();
        let proto = request_metadata_to_proto(&metadata);
        let decoded = request_metadata_from_proto(&proto);

        assert_eq!(decoded.request_id.as_deref(), Some("req-1"));
        assert_eq!(decoded.idempotency_key.as_deref(), Some("idem-1"));
        assert_eq!(decoded.trace_id.as_deref(), Some("trace-1"));
        assert!(decoded.passthrough.is_empty());
    }

    #[test]
    fn request_metadata_from_proto_drops_empty_values() {
        let proto = runtime_v2::RequestMetadata {
            request_id: " ".to_string(),
            idempotency_key: "".to_string(),
            trace_id: "\n".to_string(),
        };

        let decoded = request_metadata_from_proto(&proto);
        assert!(decoded.request_id.is_none());
        assert!(decoded.idempotency_key.is_none());
        assert!(decoded.trace_id.is_none());
    }

    #[test]
    fn machine_error_code_round_trip_for_all_known_values() {
        for code in MachineErrorCode::ALL {
            let error = MachineError::new(
                code,
                "oops".to_string(),
                Some("req-2".to_string()),
                BTreeMap::new(),
            );
            let proto = machine_error_to_proto_detail(&error);
            let decoded = machine_error_from_proto_detail(&proto).expect("decode should succeed");
            assert_eq!(decoded.code, code);
            assert_eq!(decoded.message, "oops");
            assert_eq!(decoded.request_id.as_deref(), Some("req-2"));
            assert!(decoded.details.is_empty());
        }
    }

    #[test]
    fn machine_error_unknown_code_is_rejected() {
        let detail = runtime_v2::ErrorDetail {
            code: "made_up".to_string(),
            message: "x".to_string(),
            request_id: String::new(),
        };

        let err = machine_error_from_proto_detail(&detail).expect_err("should reject unknown code");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "error.code",
                value: "made_up".to_string(),
            }
        );
    }

    #[test]
    fn sandbox_payload_round_trip_preserves_representable_fields() {
        let sandbox = Sandbox {
            sandbox_id: "sbx-1".to_string(),
            backend: SandboxBackend::MacosVz,
            spec: SandboxSpec {
                cpus: Some(4),
                memory_mb: Some(4096),
                base_image_ref: Some("alpine:3.20".to_string()),
                main_container: Some("workspace-main".to_string()),
                network_profile: None,
                volume_mounts: Vec::new(),
            },
            state: SandboxState::Ready,
            created_at: 11,
            updated_at: 12,
            labels: BTreeMap::from([
                ("env".to_string(), "dev".to_string()),
                (
                    SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
                    "alpine:3.20".to_string(),
                ),
                (
                    SANDBOX_LABEL_MAIN_CONTAINER.to_string(),
                    "workspace-main".to_string(),
                ),
            ]),
        };

        let payload = sandbox_to_proto_payload(&sandbox);
        let decoded = sandbox_from_proto_payload(&payload).expect("sandbox decode should succeed");
        assert_eq!(decoded, sandbox);
    }

    #[test]
    fn sandbox_payload_rejects_unknown_state() {
        let payload = runtime_v2::SandboxPayload {
            sandbox_id: "sbx".to_string(),
            backend: "macos_vz".to_string(),
            state: "booting".to_string(),
            cpus: 0,
            memory_mb: 0,
            created_at: 0,
            updated_at: 0,
            labels: std::collections::HashMap::new(),
        };

        let err = sandbox_from_proto_payload(&payload).expect_err("unknown state should fail");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "sandbox.state",
                value: "booting".to_string(),
            }
        );
    }

    #[test]
    fn lease_payload_round_trip() {
        let lease = Lease {
            lease_id: "lease-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            ttl_secs: 60,
            last_heartbeat_at: 123,
            state: LeaseState::Active,
        };

        let payload = lease_to_proto_payload(&lease);
        let decoded = lease_from_proto_payload(&payload).expect("lease decode should succeed");
        assert_eq!(decoded, lease);
    }

    #[test]
    fn lease_payload_rejects_unknown_state() {
        let payload = runtime_v2::LeasePayload {
            lease_id: "lease".to_string(),
            sandbox_id: "sbx".to_string(),
            ttl_secs: 10,
            last_heartbeat_at: 1,
            state: "paused".to_string(),
        };

        let err = lease_from_proto_payload(&payload).expect_err("unknown state should fail");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "lease.state",
                value: "paused".to_string(),
            }
        );
    }

    #[test]
    fn container_payload_round_trip() {
        let container = Container {
            container_id: "ctr-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            image_digest: "sha256:abc".to_string(),
            container_spec: ContainerSpec::default(),
            state: ContainerState::Running,
            created_at: 10,
            started_at: Some(11),
            ended_at: None,
        };

        let payload = container_to_proto_payload(&container);
        let decoded =
            container_from_proto_payload(&payload).expect("container decode should succeed");
        assert_eq!(decoded, container);
    }

    #[test]
    fn execution_payload_round_trip_for_exited_state() {
        let execution = Execution {
            execution_id: "exec-1".to_string(),
            container_id: "ctr-1".to_string(),
            exec_spec: ExecutionSpec::default(),
            state: ExecutionState::Exited,
            exit_code: Some(0),
            started_at: Some(20),
            ended_at: Some(30),
        };

        let payload = execution_to_proto_payload(&execution);
        let decoded =
            execution_from_proto_payload(&payload).expect("execution decode should succeed");
        assert_eq!(decoded, execution);
    }

    #[test]
    fn execution_payload_rejects_unknown_state() {
        let payload = runtime_v2::ExecutionPayload {
            execution_id: "exec".to_string(),
            container_id: "ctr".to_string(),
            state: "completed".to_string(),
            exit_code: 0,
            started_at: 0,
            ended_at: 0,
        };

        let err = execution_from_proto_payload(&payload).expect_err("unknown state should fail");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "execution.state",
                value: "completed".to_string(),
            }
        );
    }

    #[test]
    fn checkpoint_payload_round_trip() {
        let checkpoint = Checkpoint {
            checkpoint_id: "ckpt-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: Some("ckpt-parent".to_string()),
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Ready,
            created_at: 44,
            compatibility_fingerprint: "kernel-6.1".to_string(),
        };

        let payload = checkpoint_to_proto_payload(&checkpoint);
        let decoded =
            checkpoint_from_proto_payload(&payload).expect("checkpoint decode should succeed");
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    fn checkpoint_payload_rejects_unknown_class() {
        let payload = runtime_v2::CheckpointPayload {
            checkpoint_id: "ckpt".to_string(),
            sandbox_id: "sbx".to_string(),
            parent_checkpoint_id: String::new(),
            checkpoint_class: "snapshot".to_string(),
            state: "ready".to_string(),
            compatibility_fingerprint: String::new(),
            created_at: 0,
        };

        let err = checkpoint_from_proto_payload(&payload).expect_err("unknown class should fail");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "checkpoint.class",
                value: "snapshot".to_string(),
            }
        );
    }

    #[test]
    fn checkpoint_payload_rejects_unknown_state() {
        let payload = runtime_v2::CheckpointPayload {
            checkpoint_id: "ckpt".to_string(),
            sandbox_id: "sbx".to_string(),
            parent_checkpoint_id: String::new(),
            checkpoint_class: "fs_quick".to_string(),
            state: "restoring".to_string(),
            compatibility_fingerprint: String::new(),
            created_at: 0,
        };

        let err = checkpoint_from_proto_payload(&payload).expect_err("unknown state should fail");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "checkpoint.state",
                value: "restoring".to_string(),
            }
        );
    }

    #[test]
    fn build_payload_round_trip() {
        let build = Build {
            build_id: "build-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            build_spec: BuildSpec::default(),
            state: BuildState::Succeeded,
            result_digest: Some("sha256:def".to_string()),
            started_at: 70,
            ended_at: Some(80),
        };

        let payload = build_to_proto_payload(&build);
        let decoded = build_from_proto_payload(&payload).expect("build decode should succeed");
        assert_eq!(decoded, build);
    }

    #[test]
    fn build_payload_rejects_unknown_state() {
        let payload = runtime_v2::BuildPayload {
            build_id: "build".to_string(),
            sandbox_id: "sbx".to_string(),
            state: "done".to_string(),
            result_digest: String::new(),
            started_at: 0,
            ended_at: 0,
        };

        let err = build_from_proto_payload(&payload).expect_err("unknown state should fail");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "build.state",
                value: "done".to_string(),
            }
        );
    }

    #[test]
    fn runtime_event_round_trip() {
        let event = Event {
            event_id: 42,
            ts: 1_700_000_001,
            scope: EventScope::Container,
            scope_id: "ctr-1".to_string(),
            event_type: "container.started".to_string(),
            payload: BTreeMap::from([("key".to_string(), "value".to_string())]),
            trace_id: Some("trace-1".to_string()),
        };

        let wire = event_to_proto_runtime_event(&event).expect("event encode should succeed");
        let decoded = event_from_proto_runtime_event(&wire).expect("event decode should succeed");
        assert_eq!(decoded, event);
    }

    #[test]
    fn runtime_event_rejects_negative_identifier() {
        let wire = runtime_v2::RuntimeEvent {
            id: -1,
            stack_name: "stack".to_string(),
            created_at: "0".to_string(),
            event_json: "{}".to_string(),
        };

        let err = event_from_proto_runtime_event(&wire).expect_err("negative id should fail");
        assert_eq!(
            err,
            TranslationError::InvalidValue {
                field: "runtime_event.id",
                value: "-1".to_string(),
            }
        );
    }

    #[test]
    fn runtime_event_rejects_invalid_json() {
        let wire = runtime_v2::RuntimeEvent {
            id: 1,
            stack_name: "stack".to_string(),
            created_at: "1700".to_string(),
            event_json: "not-json".to_string(),
        };

        let err = event_from_proto_runtime_event(&wire).expect_err("invalid json should fail");
        match err {
            TranslationError::InvalidJson {
                field: "runtime_event.event_json",
                ..
            } => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn runtime_capabilities_round_trip() {
        let capabilities = RuntimeCapabilities {
            fs_quick_checkpoint: true,
            vm_full_checkpoint: false,
            checkpoint_fork: true,
            docker_compat: true,
            compose_adapter: true,
            build_cache_export: false,
            gpu_passthrough: false,
            live_resize: true,
            shared_vm: true,
            stack_networking: true,
            container_logs: true,
        };

        let wire = runtime_capabilities_to_proto(capabilities);
        let decoded =
            runtime_capabilities_from_proto(&wire).expect("capability decode should succeed");
        assert_eq!(decoded, capabilities);
    }

    #[test]
    fn runtime_capabilities_reject_unknown_capability() {
        let wire = vec![runtime_v2::Capability {
            name: "future_capability".to_string(),
            enabled: true,
        }];

        let err =
            runtime_capabilities_from_proto(&wire).expect_err("unknown capability should fail");
        assert_eq!(
            err,
            TranslationError::UnknownCapability {
                name: "future_capability".to_string(),
            }
        );
    }

    #[test]
    fn runtime_capabilities_reject_duplicate_entries() {
        let wire = vec![
            runtime_v2::Capability {
                name: "docker_compat".to_string(),
                enabled: true,
            },
            runtime_v2::Capability {
                name: "docker_compat".to_string(),
                enabled: false,
            },
        ];

        let err =
            runtime_capabilities_from_proto(&wire).expect_err("duplicate capability should fail");
        assert_eq!(
            err,
            TranslationError::DuplicateCapability {
                name: "docker_compat".to_string(),
            }
        );
    }
}
