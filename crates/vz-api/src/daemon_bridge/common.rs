use super::*;
pub(crate) fn normalize_optional_wire_field(value: &str) -> Option<String> {
    let raw = value.trim();
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

pub(crate) fn sandbox_payload_from_runtime_proto(
    payload: runtime_v2::SandboxPayload,
) -> SandboxPayload {
    let labels: BTreeMap<String, String> = payload.labels.into_iter().collect();
    let base_image_ref = labels
        .get(SANDBOX_LABEL_BASE_IMAGE_REF)
        .and_then(|value| normalize_optional_wire_field(value));
    let main_container = labels
        .get(SANDBOX_LABEL_MAIN_CONTAINER)
        .and_then(|value| normalize_optional_wire_field(value));

    SandboxPayload {
        sandbox_id: payload.sandbox_id,
        backend: payload.backend,
        state: payload.state,
        cpus: if payload.cpus == 0 {
            None
        } else {
            Some(payload.cpus as u8)
        },
        memory_mb: if payload.memory_mb == 0 {
            None
        } else {
            Some(payload.memory_mb)
        },
        base_image_ref,
        main_container,
        created_at: payload.created_at,
        updated_at: payload.updated_at,
        labels,
    }
}

pub(crate) fn lease_payload_from_runtime_proto(payload: runtime_v2::LeasePayload) -> LeasePayload {
    LeasePayload {
        lease_id: payload.lease_id,
        sandbox_id: payload.sandbox_id,
        ttl_secs: payload.ttl_secs,
        last_heartbeat_at: payload.last_heartbeat_at,
        state: payload.state,
    }
}

pub(crate) fn stack_service_status_from_runtime_proto(
    payload: runtime_v2::StackServiceStatus,
) -> StackServiceStatusPayload {
    StackServiceStatusPayload {
        service_name: payload.service_name,
        phase: payload.phase,
        ready: payload.ready,
        container_id: payload.container_id,
        last_error: payload.last_error,
    }
}

pub(crate) fn build_payload_from_runtime_proto(payload: runtime_v2::BuildPayload) -> BuildPayload {
    BuildPayload {
        build_id: payload.build_id,
        sandbox_id: payload.sandbox_id,
        state: payload.state,
        result_digest: if payload.result_digest.trim().is_empty() {
            None
        } else {
            Some(payload.result_digest)
        },
        started_at: payload.started_at,
        ended_at: if payload.ended_at == 0 {
            None
        } else {
            Some(payload.ended_at)
        },
    }
}

pub(crate) fn execution_payload_from_runtime_proto(
    payload: runtime_v2::ExecutionPayload,
) -> ExecutionPayload {
    ExecutionPayload {
        execution_id: payload.execution_id,
        container_id: payload.container_id,
        state: payload.state,
        exit_code: if payload.exit_code == 0 {
            None
        } else {
            Some(payload.exit_code)
        },
        started_at: if payload.started_at == 0 {
            None
        } else {
            Some(payload.started_at)
        },
        ended_at: if payload.ended_at == 0 {
            None
        } else {
            Some(payload.ended_at)
        },
    }
}

pub(crate) fn checkpoint_payload_from_runtime_proto(
    payload: runtime_v2::CheckpointPayload,
) -> CheckpointPayload {
    CheckpointPayload {
        checkpoint_id: payload.checkpoint_id,
        sandbox_id: payload.sandbox_id,
        parent_checkpoint_id: if payload.parent_checkpoint_id.trim().is_empty() {
            None
        } else {
            Some(payload.parent_checkpoint_id)
        },
        class: payload.checkpoint_class,
        state: payload.state,
        compatibility_fingerprint: payload.compatibility_fingerprint,
        created_at: payload.created_at,
        retention_tag: normalize_optional_wire_field(&payload.retention_tag),
        retention_protected: payload.retention_protected,
        retention_gc_reason: normalize_optional_wire_field(&payload.retention_gc_reason),
        retention_expires_at: if payload.retention_expires_at == 0 {
            None
        } else {
            Some(payload.retention_expires_at)
        },
    }
}

pub(crate) fn container_payload_from_runtime_proto(
    payload: runtime_v2::ContainerPayload,
) -> ContainerPayload {
    ContainerPayload {
        container_id: payload.container_id,
        sandbox_id: payload.sandbox_id,
        image_digest: payload.image_digest,
        state: payload.state,
        created_at: payload.created_at,
        started_at: if payload.started_at == 0 {
            None
        } else {
            Some(payload.started_at)
        },
        ended_at: if payload.ended_at == 0 {
            None
        } else {
            Some(payload.ended_at)
        },
    }
}

pub(crate) fn api_event_record_from_runtime_proto(
    event: runtime_v2::RuntimeEvent,
) -> ApiEventRecord {
    let event_value = serde_json::from_str::<serde_json::Value>(&event.event_json)
        .unwrap_or_else(|_| serialization_error_value());
    ApiEventRecord {
        id: event.id,
        stack_name: event.stack_name,
        created_at: event.created_at,
        event: event_value,
    }
}

pub(crate) fn image_payload_from_runtime_proto(payload: runtime_v2::ImagePayload) -> ImagePayload {
    ImagePayload {
        image_ref: payload.image_ref,
        resolved_digest: payload.resolved_digest,
        platform: payload.platform,
        source_registry: payload.source_registry,
        pulled_at: payload.pulled_at,
    }
}

pub(crate) fn receipt_payload_from_runtime_proto(
    payload: runtime_v2::ReceiptPayload,
) -> ReceiptPayload {
    let metadata = serde_json::from_str::<serde_json::Value>(&payload.metadata_json)
        .unwrap_or_else(|_| empty_json_object_value());
    ReceiptPayload {
        receipt_id: payload.receipt_id,
        operation: payload.operation,
        entity_id: payload.entity_id,
        entity_type: payload.entity_type,
        request_id: payload.request_id,
        status: payload.status,
        created_at: payload.created_at,
        metadata,
        retention_expires_at: if payload.retention_expires_at == 0 {
            payload.created_at
        } else {
            payload.retention_expires_at
        },
        retention_gc_reason: normalize_optional_wire_field(&payload.retention_gc_reason),
        retention_policy: normalize_optional_wire_field(&payload.retention_policy)
            .unwrap_or_else(|| "bounded_age_count".to_string()),
    }
}

pub(crate) fn file_entry_payload_from_runtime_proto(
    entry: runtime_v2::FileEntry,
) -> FileEntryPayload {
    FileEntryPayload {
        path: entry.path,
        is_dir: entry.is_dir,
        size: entry.size,
        modified_at: entry.modified_at,
    }
}

pub(crate) fn daemon_status_to_http_response(
    status: tonic::Status,
    fallback_request_id: &str,
) -> Response {
    let request_id = extract_request_id_from_status_message(&status)
        .unwrap_or_else(|| fallback_request_id.to_string());
    let status_message = status.message().to_string();
    let lowered_message = status_message.to_ascii_lowercase();
    let (http_status, code) = match status.code() {
        tonic::Code::InvalidArgument => (StatusCode::BAD_REQUEST, "invalid_request"),
        tonic::Code::NotFound => (StatusCode::NOT_FOUND, "not_found"),
        tonic::Code::FailedPrecondition => {
            if lowered_message.contains("idempotency key conflict") {
                (StatusCode::CONFLICT, "idempotency_conflict")
            } else if lowered_message.contains("checkpoint")
                && lowered_message.contains("not in ready state")
            {
                (StatusCode::CONFLICT, "checkpoint_not_ready")
            } else if lowered_message.contains("is not active") {
                (StatusCode::UNPROCESSABLE_ENTITY, "invalid_state")
            } else if lowered_message.contains("is in terminal state")
                || lowered_message.contains("is not running")
            {
                (StatusCode::CONFLICT, "invalid_state")
            } else {
                (StatusCode::CONFLICT, "state_conflict")
            }
        }
        tonic::Code::PermissionDenied => (StatusCode::FORBIDDEN, "policy_denied"),
        tonic::Code::DeadlineExceeded => (StatusCode::GATEWAY_TIMEOUT, "timeout"),
        tonic::Code::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "backend_unavailable"),
        tonic::Code::Unimplemented => (StatusCode::NOT_IMPLEMENTED, "unsupported_operation"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
    };
    json_error_response(http_status, code, &status_message, &request_id)
}

pub(crate) fn extract_request_id_from_status_message(status: &tonic::Status) -> Option<String> {
    let message = status.message();
    let marker = "request_id=";
    let idx = message.find(marker)?;
    let start = idx + marker.len();
    let tail = &message[start..];
    let end = tail.find(char::is_whitespace).unwrap_or(tail.len());
    let request_id = tail[..end].trim().to_string();
    if request_id.is_empty() {
        None
    } else {
        Some(request_id)
    }
}

pub(crate) fn daemon_client_config(state: &ApiState) -> DaemonClientConfig {
    let mut config = DaemonClientConfig::default();
    config.auto_spawn = state.daemon_auto_spawn;
    config.state_store_path = Some(state.state_store_path.clone());

    if let Some(socket_path) = &state.daemon_socket_path {
        config.socket_path = socket_path.clone();
    }

    if let Some(runtime_data_dir) = &state.daemon_runtime_data_dir {
        config.runtime_data_dir = Some(runtime_data_dir.clone());
    }

    if config.runtime_data_dir.is_none() {
        if state.daemon_socket_path.is_some() {
            if let Some(socket_parent) = config.socket_path.parent()
                && !socket_parent.as_os_str().is_empty()
            {
                config.runtime_data_dir = Some(socket_parent.to_path_buf());
            }
        } else if let Some(parent) = state.state_store_path.parent()
            && !parent.as_os_str().is_empty()
        {
            config.runtime_data_dir = Some(parent.join(".vz-runtime"));
        }
    }

    if state.daemon_socket_path.is_none()
        && let Some(runtime_data_dir) = &config.runtime_data_dir
    {
        config.socket_path = runtime_data_dir.join("runtimed.sock");
    }

    config
}

pub(crate) fn daemon_request_metadata(
    request_id: &str,
    idempotency_key: Option<String>,
) -> runtime_v2::RequestMetadata {
    runtime_v2::RequestMetadata {
        request_id: request_id.to_string(),
        idempotency_key: idempotency_key.unwrap_or_default(),
        trace_id: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_request_metadata_preserves_request_id_and_idempotency_key() {
        let metadata = daemon_request_metadata("req-42", Some("idem-42".to_string()));

        assert_eq!(metadata.request_id, "req-42");
        assert_eq!(metadata.idempotency_key, "idem-42");
        assert_eq!(metadata.trace_id, "");
    }

    #[test]
    fn daemon_request_metadata_uses_empty_idempotency_key_when_absent() {
        let metadata = daemon_request_metadata("req-99", None);

        assert_eq!(metadata.request_id, "req-99");
        assert_eq!(metadata.idempotency_key, "");
        assert_eq!(metadata.trace_id, "");
    }
}
