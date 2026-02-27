use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};

pub(super) fn normalize_optional_wire_field(value: &str) -> Option<String> {
    let raw = value.trim();
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

pub(super) fn sandbox_payload_from_runtime_proto(
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

pub(super) fn lease_payload_from_runtime_proto(payload: runtime_v2::LeasePayload) -> LeasePayload {
    LeasePayload {
        lease_id: payload.lease_id,
        sandbox_id: payload.sandbox_id,
        ttl_secs: payload.ttl_secs,
        last_heartbeat_at: payload.last_heartbeat_at,
        state: payload.state,
    }
}

pub(super) fn stack_service_status_from_runtime_proto(
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

pub(super) fn build_payload_from_runtime_proto(payload: runtime_v2::BuildPayload) -> BuildPayload {
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

pub(super) fn execution_payload_from_runtime_proto(
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

pub(super) fn checkpoint_payload_from_runtime_proto(
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
    }
}

pub(super) fn container_payload_from_runtime_proto(
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

pub(super) fn api_event_record_from_runtime_proto(
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

pub(super) fn image_payload_from_runtime_proto(payload: runtime_v2::ImagePayload) -> ImagePayload {
    ImagePayload {
        image_ref: payload.image_ref,
        resolved_digest: payload.resolved_digest,
        platform: payload.platform,
        source_registry: payload.source_registry,
        pulled_at: payload.pulled_at,
    }
}

pub(super) fn receipt_payload_from_runtime_proto(
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
    }
}

pub(super) fn file_entry_payload_from_runtime_proto(
    entry: runtime_v2::FileEntry,
) -> FileEntryPayload {
    FileEntryPayload {
        path: entry.path,
        is_dir: entry.is_dir,
        size: entry.size,
        modified_at: entry.modified_at,
    }
}

pub(super) fn daemon_status_to_http_response(
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

pub(super) fn extract_request_id_from_status_message(status: &tonic::Status) -> Option<String> {
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

pub(super) fn daemon_client_config(state: &ApiState) -> DaemonClientConfig {
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

pub(super) fn daemon_request_metadata(
    request_id: &str,
    idempotency_key: Option<String>,
) -> runtime_v2::RequestMetadata {
    runtime_v2::RequestMetadata {
        request_id: request_id.to_string(),
        idempotency_key: idempotency_key.unwrap_or_default(),
        trace_id: String::new(),
    }
}

pub(super) async fn try_create_sandbox_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: CreateSandboxRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let idempotency_key = extract_idempotency_key(headers);
    let stack_name = match body.stack_name.clone() {
        Some(stack_name) => stack_name,
        None => {
            if let Some(key) = idempotency_key.as_deref() {
                let normalized = key.trim();
                if !normalized.is_empty() {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    normalized.hash(&mut hasher);
                    format!("sbx-idem-{:016x}", hasher.finish())
                } else {
                    format!("sbx-{}", Uuid::new_v4())
                }
            } else {
                format!("sbx-{}", Uuid::new_v4())
            }
        }
    };
    let mut labels: HashMap<String, String> = body.labels.into_iter().collect();
    if let Some(original_stack_name) = body.stack_name.clone() {
        labels.insert("stack_name".to_string(), original_stack_name);
    }
    if let Some(base_image_ref) = body.base_image_ref.as_deref().map(str::trim)
        && !base_image_ref.is_empty()
    {
        labels.insert(
            SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
            base_image_ref.to_string(),
        );
    }
    if let Some(main_container) = body.main_container.as_deref().map(str::trim)
        && !main_container.is_empty()
    {
        labels.insert(
            SANDBOX_LABEL_MAIN_CONTAINER.to_string(),
            main_container.to_string(),
        );
    }

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::CreateSandboxRequest {
        metadata: Some(daemon_request_metadata(request_id, idempotency_key)),
        stack_name,
        cpus: body.cpus.unwrap_or(0) as u32,
        memory_mb: body.memory_mb.unwrap_or(0),
        labels,
    };

    match client.create_sandbox_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let Some(payload) = grpc_response.sandbox else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing sandbox payload",
                    request_id,
                ));
            };

            let mut response = (
                StatusCode::CREATED,
                Json(SandboxResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    sandbox: sandbox_payload_from_runtime_proto(payload),
                }),
            )
                .into_response();

            if let Some(receipt_id) = receipt_id
                && let Ok(value) = axum::http::HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }

            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_get_sandbox_via_daemon(
    state: &ApiState,
    sandbox_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::GetSandboxRequest {
        sandbox_id: sandbox_id.to_string(),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };

    match client.get_sandbox(grpc_request).await {
        Ok(grpc_response) => {
            let Some(payload) = grpc_response.sandbox else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing sandbox payload",
                    request_id,
                ));
            };
            Some(
                (
                    StatusCode::OK,
                    Json(SandboxResponse {
                        request_id: if grpc_response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            grpc_response.request_id
                        },
                        sandbox: sandbox_payload_from_runtime_proto(payload),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_list_sandboxes_via_daemon(
    state: &ApiState,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::ListSandboxesRequest {
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.list_sandboxes(grpc_request).await {
        Ok(grpc_response) => Some(
            (
                StatusCode::OK,
                Json(SandboxListResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    sandboxes: grpc_response
                        .sandboxes
                        .into_iter()
                        .map(sandbox_payload_from_runtime_proto)
                        .collect(),
                }),
            )
                .into_response(),
        ),
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_terminate_sandbox_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    sandbox_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::TerminateSandboxRequest {
        sandbox_id: sandbox_id.to_string(),
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
    };
    match client.terminate_sandbox_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let Some(payload) = grpc_response.sandbox else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing sandbox payload",
                    request_id,
                ));
            };

            let mut response = (
                StatusCode::OK,
                Json(SandboxResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    sandbox: sandbox_payload_from_runtime_proto(payload),
                }),
            )
                .into_response();

            if let Some(receipt_id) = receipt_id
                && let Ok(value) = axum::http::HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) enum StackServiceActionKind {
    Stop,
    Start,
    Restart,
}

pub(super) async fn try_apply_stack_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: ApplyStackRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::ApplyStackRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        stack_name: body.stack_name,
        compose_yaml: body.compose_yaml,
        compose_dir: body.compose_dir,
        dry_run: body.dry_run.unwrap_or(false),
        detach: body.detach.unwrap_or(false),
    };

    match client.apply_stack_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let mut response = (
                StatusCode::OK,
                Json(ApplyStackResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    stack: ApplyStackPayload {
                        stack_name: grpc_response.stack_name,
                        changed_actions: grpc_response.changed_actions,
                        converged: grpc_response.converged,
                        services_ready: grpc_response.services_ready,
                        services_failed: grpc_response.services_failed,
                        services: grpc_response
                            .services
                            .into_iter()
                            .map(stack_service_status_from_runtime_proto)
                            .collect(),
                    },
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = axum::http::HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_teardown_stack_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: TeardownStackRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::TeardownStackRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        stack_name: body.stack_name,
        dry_run: body.dry_run.unwrap_or(false),
        remove_volumes: body.remove_volumes.unwrap_or(false),
    };

    match client.teardown_stack_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let mut response = (
                StatusCode::OK,
                Json(TeardownStackResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    stack: TeardownStackPayload {
                        stack_name: grpc_response.stack_name,
                        changed_actions: grpc_response.changed_actions,
                        removed_volumes: grpc_response.removed_volumes,
                    },
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = axum::http::HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_get_stack_status_via_daemon(
    state: &ApiState,
    stack_name: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    match client
        .get_stack_status(runtime_v2::GetStackStatusRequest {
            metadata: Some(daemon_request_metadata(request_id, None)),
            stack_name: stack_name.to_string(),
        })
        .await
    {
        Ok(grpc_response) => Some(
            (
                StatusCode::OK,
                Json(StackStatusResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    stack_name: grpc_response.stack_name,
                    services: grpc_response
                        .services
                        .into_iter()
                        .map(stack_service_status_from_runtime_proto)
                        .collect(),
                }),
            )
                .into_response(),
        ),
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_list_stack_events_via_daemon(
    state: &ApiState,
    stack_name: &str,
    query: &StackEventsQuery,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let limit = query
        .limit
        .unwrap_or(state.default_event_page_size)
        .clamp(1, MAX_EVENT_PAGE_SIZE);
    let after = query.after.unwrap_or(0);

    match client
        .list_stack_events(runtime_v2::ListStackEventsRequest {
            metadata: Some(daemon_request_metadata(request_id, None)),
            stack_name: stack_name.to_string(),
            after,
            limit: limit as u32,
        })
        .await
    {
        Ok(grpc_response) => Some(
            (
                StatusCode::OK,
                Json(StackEventsResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    events: grpc_response
                        .events
                        .into_iter()
                        .map(api_event_record_from_runtime_proto)
                        .collect(),
                    next_cursor: grpc_response.next_cursor,
                }),
            )
                .into_response(),
        ),
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_get_stack_logs_via_daemon(
    state: &ApiState,
    stack_name: &str,
    query: &StackLogsQuery,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    match client
        .get_stack_logs(runtime_v2::GetStackLogsRequest {
            metadata: Some(daemon_request_metadata(request_id, None)),
            stack_name: stack_name.to_string(),
            service: query.service.clone().unwrap_or_default(),
            tail: query.tail.unwrap_or(0),
        })
        .await
    {
        Ok(grpc_response) => Some(
            (
                StatusCode::OK,
                Json(StackLogsResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    stack_name: grpc_response.stack_name,
                    logs: grpc_response
                        .logs
                        .into_iter()
                        .map(|log| StackServiceLogPayload {
                            service_name: log.service_name,
                            output: log.output,
                        })
                        .collect(),
                }),
            )
                .into_response(),
        ),
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_stack_service_action_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    stack_name: &str,
    service_name: &str,
    request_id: &str,
    action: StackServiceActionKind,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::StackServiceActionRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        stack_name: stack_name.to_string(),
        service_name: service_name.to_string(),
    };

    let grpc_response = match action {
        StackServiceActionKind::Stop => client.stop_stack_service_with_metadata(grpc_request).await,
        StackServiceActionKind::Start => {
            client.start_stack_service_with_metadata(grpc_request).await
        }
        StackServiceActionKind::Restart => {
            client
                .restart_stack_service_with_metadata(grpc_request)
                .await
        }
    };

    match grpc_response {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let Some(service) = grpc_response.service else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing stack service payload",
                    request_id,
                ));
            };
            let mut response = (
                StatusCode::OK,
                Json(StackServiceActionResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    action: StackServiceActionPayload {
                        stack_name: grpc_response.stack_name,
                        service: stack_service_status_from_runtime_proto(service),
                    },
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = axum::http::HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_create_stack_run_container_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: StackRunContainerRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    match client
        .create_stack_run_container_with_metadata(runtime_v2::StackRunContainerRequest {
            metadata: Some(daemon_request_metadata(
                request_id,
                extract_idempotency_key(headers),
            )),
            stack_name: body.stack_name,
            service_name: body.service_name,
            run_service_name: body.run_service_name.unwrap_or_default(),
        })
        .await
    {
        Ok(grpc_response) => {
            let grpc_response = grpc_response.into_inner();
            Some(
                (
                    StatusCode::OK,
                    Json(StackRunContainerResponse {
                        request_id: if grpc_response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            grpc_response.request_id
                        },
                        run_container: StackRunContainerPayload {
                            stack_name: grpc_response.stack_name,
                            service_name: grpc_response.service_name,
                            run_service_name: grpc_response.run_service_name,
                            container_id: grpc_response.container_id,
                        },
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_remove_stack_run_container_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: StackRunContainerRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    match client
        .remove_stack_run_container_with_metadata(runtime_v2::StackRunContainerRequest {
            metadata: Some(daemon_request_metadata(
                request_id,
                extract_idempotency_key(headers),
            )),
            stack_name: body.stack_name,
            service_name: body.service_name,
            run_service_name: body.run_service_name.unwrap_or_default(),
        })
        .await
    {
        Ok(grpc_response) => {
            let grpc_response = grpc_response.into_inner();
            Some(
                (
                    StatusCode::OK,
                    Json(StackRunContainerResponse {
                        request_id: if grpc_response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            grpc_response.request_id
                        },
                        run_container: StackRunContainerPayload {
                            stack_name: grpc_response.stack_name,
                            service_name: grpc_response.service_name,
                            run_service_name: grpc_response.run_service_name,
                            container_id: grpc_response.container_id,
                        },
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_open_sandbox_shell_via_daemon(
    state: &ApiState,
    sandbox_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let mut stream = match client
        .open_sandbox_shell(runtime_v2::OpenSandboxShellRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
    {
        Ok(stream) => stream,
        Err(DaemonClientError::Grpc(status)) => {
            return Some(daemon_status_to_http_response(status, request_id));
        }
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let mut completion: Option<runtime_v2::OpenSandboxShellResponse> = None;
    loop {
        match stream.message().await {
            Ok(Some(event)) => {
                if let Some(runtime_v2::open_sandbox_shell_event::Payload::Completion(done)) =
                    event.payload
                {
                    completion = Some(done);
                }
            }
            Ok(None) => break,
            Err(status) => return Some(daemon_status_to_http_response(status, request_id)),
        }
    }

    let completion = match completion {
        Some(completion) => completion,
        None => {
            return Some(json_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "daemon open_sandbox_shell stream ended without completion",
                request_id,
            ));
        }
    };

    Some(
        (
            StatusCode::OK,
            Json(OpenSandboxShellResponse {
                request_id: if completion.request_id.trim().is_empty() {
                    request_id.to_string()
                } else {
                    completion.request_id
                },
                shell: OpenSandboxShellPayload {
                    sandbox_id: completion.sandbox_id,
                    container_id: completion.container_id,
                    cmd: completion.cmd,
                    args: completion.args,
                    execution_id: completion.execution_id,
                },
            }),
        )
            .into_response(),
    )
}

pub(super) async fn try_close_sandbox_shell_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    sandbox_id: &str,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: CloseSandboxShellRequest =
        if raw_body.is_empty() || raw_body.iter().all(u8::is_ascii_whitespace) {
            CloseSandboxShellRequest { execution_id: None }
        } else {
            match serde_json::from_slice(raw_body) {
                Ok(body) => body,
                Err(error) => {
                    return Some(json_error_response(
                        StatusCode::BAD_REQUEST,
                        "invalid_request",
                        &format!("invalid JSON body: {error}"),
                        request_id,
                    ));
                }
            }
        };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let mut stream = match client
        .close_sandbox_shell(runtime_v2::CloseSandboxShellRequest {
            sandbox_id: sandbox_id.to_string(),
            execution_id: body.execution_id.unwrap_or_default(),
            metadata: Some(daemon_request_metadata(
                request_id,
                extract_idempotency_key(headers),
            )),
        })
        .await
    {
        Ok(stream) => stream,
        Err(DaemonClientError::Grpc(status)) => {
            return Some(daemon_status_to_http_response(status, request_id));
        }
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let mut completion: Option<runtime_v2::CloseSandboxShellResponse> = None;
    loop {
        match stream.message().await {
            Ok(Some(event)) => {
                if let Some(runtime_v2::close_sandbox_shell_event::Payload::Completion(done)) =
                    event.payload
                {
                    completion = Some(done);
                }
            }
            Ok(None) => break,
            Err(status) => return Some(daemon_status_to_http_response(status, request_id)),
        }
    }

    let completion = match completion {
        Some(completion) => completion,
        None => {
            return Some(json_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "daemon close_sandbox_shell stream ended without completion",
                request_id,
            ));
        }
    };

    Some(
        (
            StatusCode::OK,
            Json(CloseSandboxShellResponse {
                request_id: if completion.request_id.trim().is_empty() {
                    request_id.to_string()
                } else {
                    completion.request_id
                },
                shell: CloseSandboxShellPayload {
                    sandbox_id: completion.sandbox_id,
                    execution_id: completion.execution_id,
                },
            }),
        )
            .into_response(),
    )
}

pub(super) async fn try_open_lease_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: OpenLeaseRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::OpenLeaseRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        sandbox_id: body.sandbox_id,
        ttl_secs: body.ttl_secs.unwrap_or(0),
    };

    match client.open_lease_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let Some(payload) = grpc_response.lease else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing lease payload",
                    request_id,
                ));
            };

            let mut response = (
                StatusCode::CREATED,
                Json(LeaseResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    lease: lease_payload_from_runtime_proto(payload),
                }),
            )
                .into_response();

            if let Some(receipt_id) = receipt_id
                && let Ok(value) = axum::http::HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_list_leases_via_daemon(
    state: &ApiState,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::ListLeasesRequest {
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.list_leases(grpc_request).await {
        Ok(grpc_response) => Some(
            (
                StatusCode::OK,
                Json(LeaseListResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    leases: grpc_response
                        .leases
                        .into_iter()
                        .map(lease_payload_from_runtime_proto)
                        .collect(),
                }),
            )
                .into_response(),
        ),
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_get_lease_via_daemon(
    state: &ApiState,
    lease_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::GetLeaseRequest {
        lease_id: lease_id.to_string(),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.get_lease(grpc_request).await {
        Ok(grpc_response) => {
            let Some(payload) = grpc_response.lease else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing lease payload",
                    request_id,
                ));
            };
            Some(
                (
                    StatusCode::OK,
                    Json(LeaseResponse {
                        request_id: if grpc_response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            grpc_response.request_id
                        },
                        lease: lease_payload_from_runtime_proto(payload),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_close_lease_via_daemon(
    state: &ApiState,
    lease_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::CloseLeaseRequest {
        lease_id: lease_id.to_string(),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.close_lease_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let Some(payload) = grpc_response.lease else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing lease payload",
                    request_id,
                ));
            };
            let mut response = (
                StatusCode::OK,
                Json(LeaseResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    lease: lease_payload_from_runtime_proto(payload),
                }),
            )
                .into_response();

            if let Some(receipt_id) = receipt_id
                && let Ok(value) = axum::http::HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_heartbeat_lease_via_daemon(
    state: &ApiState,
    lease_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::HeartbeatLeaseRequest {
        lease_id: lease_id.to_string(),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.heartbeat_lease(grpc_request).await {
        Ok(grpc_response) => {
            let Some(payload) = grpc_response.lease else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing lease payload",
                    request_id,
                ));
            };
            Some(
                (
                    StatusCode::OK,
                    Json(LeaseResponse {
                        request_id: if grpc_response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            grpc_response.request_id
                        },
                        lease: lease_payload_from_runtime_proto(payload),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_start_build_via_daemon(
    state: &ApiState,
    body: &StartBuildRequest,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::StartBuildRequest {
        metadata: Some(daemon_request_metadata(request_id, None)),
        sandbox_id: body.sandbox_id.clone(),
        context: body.context.clone(),
        dockerfile: body.dockerfile.clone().unwrap_or_default(),
        args: body
            .args
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect::<HashMap<_, _>>(),
    };

    match client.start_build_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let Some(payload) = grpc_response.build else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing build payload",
                    request_id,
                ));
            };

            let mut response = (
                StatusCode::CREATED,
                Json(BuildResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    build: build_payload_from_runtime_proto(payload),
                }),
            )
                .into_response();

            if let Some(receipt_id) = receipt_id
                && let Ok(value) = axum::http::HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_list_builds_via_daemon(
    state: &ApiState,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::ListBuildsRequest {
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.list_builds(grpc_request).await {
        Ok(grpc_response) => Some(
            (
                StatusCode::OK,
                Json(BuildListResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    builds: grpc_response
                        .builds
                        .into_iter()
                        .map(build_payload_from_runtime_proto)
                        .collect(),
                }),
            )
                .into_response(),
        ),
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_get_build_via_daemon(
    state: &ApiState,
    build_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::GetBuildRequest {
        build_id: build_id.to_string(),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.get_build(grpc_request).await {
        Ok(grpc_response) => {
            let Some(payload) = grpc_response.build else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing build payload",
                    request_id,
                ));
            };
            Some(
                (
                    StatusCode::OK,
                    Json(BuildResponse {
                        request_id: if grpc_response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            grpc_response.request_id
                        },
                        build: build_payload_from_runtime_proto(payload),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_cancel_build_via_daemon(
    state: &ApiState,
    build_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::CancelBuildRequest {
        build_id: build_id.to_string(),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.cancel_build_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let Some(payload) = grpc_response.build else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing build payload",
                    request_id,
                ));
            };

            let mut response = (
                StatusCode::OK,
                Json(BuildResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    build: build_payload_from_runtime_proto(payload),
                }),
            )
                .into_response();

            if let Some(receipt_id) = receipt_id
                && let Ok(value) = axum::http::HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_create_execution_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: CreateExecutionRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::CreateExecutionRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        container_id: body.container_id,
        cmd: body.cmd,
        args: body.args.unwrap_or_default(),
        env_override: body
            .env_override
            .unwrap_or_default()
            .into_iter()
            .collect::<HashMap<_, _>>(),
        timeout_secs: body.timeout_secs.unwrap_or(0),
        pty_mode: match body.pty_mode.unwrap_or(ExecutionPtyMode::Inherit) {
            ExecutionPtyMode::Inherit => {
                runtime_v2::create_execution_request::PtyMode::Inherit as i32
            }
            ExecutionPtyMode::Enabled => {
                runtime_v2::create_execution_request::PtyMode::Enabled as i32
            }
            ExecutionPtyMode::Disabled => {
                runtime_v2::create_execution_request::PtyMode::Disabled as i32
            }
        },
    };

    match client.create_execution_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let Some(payload) = grpc_response.execution else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing execution payload",
                    request_id,
                ));
            };

            let mut response = (
                StatusCode::CREATED,
                Json(ExecutionResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    execution: execution_payload_from_runtime_proto(payload),
                }),
            )
                .into_response();

            if let Some(receipt_id) = receipt_id
                && let Ok(value) = axum::http::HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_list_executions_via_daemon(
    state: &ApiState,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::ListExecutionsRequest {
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.list_executions(grpc_request).await {
        Ok(grpc_response) => Some(
            (
                StatusCode::OK,
                Json(ExecutionListResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    executions: grpc_response
                        .executions
                        .into_iter()
                        .map(execution_payload_from_runtime_proto)
                        .collect(),
                }),
            )
                .into_response(),
        ),
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_get_execution_via_daemon(
    state: &ApiState,
    execution_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::GetExecutionRequest {
        execution_id: execution_id.to_string(),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.get_execution(grpc_request).await {
        Ok(grpc_response) => {
            let Some(payload) = grpc_response.execution else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing execution payload",
                    request_id,
                ));
            };
            Some(
                (
                    StatusCode::OK,
                    Json(ExecutionResponse {
                        request_id: if grpc_response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            grpc_response.request_id
                        },
                        execution: execution_payload_from_runtime_proto(payload),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_cancel_execution_via_daemon(
    state: &ApiState,
    execution_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::CancelExecutionRequest {
        execution_id: execution_id.to_string(),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.cancel_execution_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let Some(payload) = grpc_response.execution else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing execution payload",
                    request_id,
                ));
            };
            let mut response = (
                StatusCode::OK,
                Json(ExecutionResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    execution: execution_payload_from_runtime_proto(payload),
                }),
            )
                .into_response();

            if let Some(receipt_id) = receipt_id
                && let Ok(value) = axum::http::HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_resize_execution_via_daemon(
    state: &ApiState,
    execution_id: &str,
    body: &ResizeExecRequest,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::ResizeExecPtyRequest {
        execution_id: execution_id.to_string(),
        cols: u32::from(body.cols),
        rows: u32::from(body.rows),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.resize_exec_pty(grpc_request).await {
        Ok(grpc_response) => {
            let Some(payload) = grpc_response.execution else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing execution payload",
                    request_id,
                ));
            };
            Some(
                (
                    StatusCode::OK,
                    Json(ExecutionResponse {
                        request_id: if grpc_response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            grpc_response.request_id
                        },
                        execution: execution_payload_from_runtime_proto(payload),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_write_execution_stdin_via_daemon(
    state: &ApiState,
    execution_id: &str,
    body: &WriteExecStdinRequest,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::WriteExecStdinRequest {
        execution_id: execution_id.to_string(),
        data: body.data.as_bytes().to_vec(),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.write_exec_stdin(grpc_request).await {
        Ok(grpc_response) => {
            let Some(payload) = grpc_response.execution else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing execution payload",
                    request_id,
                ));
            };
            Some(
                (
                    StatusCode::OK,
                    Json(ExecutionResponse {
                        request_id: if grpc_response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            grpc_response.request_id
                        },
                        execution: execution_payload_from_runtime_proto(payload),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_signal_execution_via_daemon(
    state: &ApiState,
    execution_id: &str,
    body: &SignalExecRequest,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::SignalExecRequest {
        execution_id: execution_id.to_string(),
        signal: body.signal.clone(),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.signal_exec(grpc_request).await {
        Ok(grpc_response) => {
            let Some(payload) = grpc_response.execution else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing execution payload",
                    request_id,
                ));
            };
            Some(
                (
                    StatusCode::OK,
                    Json(ExecutionResponse {
                        request_id: if grpc_response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            grpc_response.request_id
                        },
                        execution: execution_payload_from_runtime_proto(payload),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_create_checkpoint_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: CreateCheckpointRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let checkpoint_class = match body.class.as_deref().unwrap_or("fs_quick") {
        "fs_quick" | "fs-quick" => "fs_quick".to_string(),
        "vm_full" | "vm-full" => "vm_full".to_string(),
        other => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_checkpoint_class",
                &format!("unknown checkpoint class: {other}"),
                request_id,
            ));
        }
    };
    if checkpoint_class == "fs_quick" && !state.capabilities.fs_quick_checkpoint {
        return Some(json_error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_checkpoint_class",
            "filesystem quick checkpoints are not supported by configured runtime capabilities",
            request_id,
        ));
    }
    if checkpoint_class == "vm_full" && !state.capabilities.vm_full_checkpoint {
        return Some(json_error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_checkpoint_class",
            "VM full checkpoints are not supported by configured runtime capabilities",
            request_id,
        ));
    }

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::CreateCheckpointRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        sandbox_id: body.sandbox_id,
        checkpoint_class,
        compatibility_fingerprint: body
            .compatibility_fingerprint
            .unwrap_or_else(|| "unset".to_string()),
    };
    match client.create_checkpoint_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_string());
            let grpc_response = grpc_response.into_inner();
            let Some(payload) = grpc_response.checkpoint else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing checkpoint payload",
                    request_id,
                ));
            };

            let mut response = (
                StatusCode::CREATED,
                Json(CheckpointResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    checkpoint: checkpoint_payload_from_runtime_proto(payload),
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_list_checkpoints_via_daemon(
    state: &ApiState,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::ListCheckpointsRequest {
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.list_checkpoints(grpc_request).await {
        Ok(grpc_response) => Some(
            (
                StatusCode::OK,
                Json(CheckpointListResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    checkpoints: grpc_response
                        .checkpoints
                        .into_iter()
                        .map(checkpoint_payload_from_runtime_proto)
                        .collect(),
                }),
            )
                .into_response(),
        ),
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_get_checkpoint_via_daemon(
    state: &ApiState,
    checkpoint_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::GetCheckpointRequest {
        checkpoint_id: checkpoint_id.to_string(),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.get_checkpoint(grpc_request).await {
        Ok(grpc_response) => {
            let Some(payload) = grpc_response.checkpoint else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing checkpoint payload",
                    request_id,
                ));
            };
            Some(
                (
                    StatusCode::OK,
                    Json(CheckpointResponse {
                        request_id: if grpc_response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            grpc_response.request_id
                        },
                        checkpoint: checkpoint_payload_from_runtime_proto(payload),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_restore_checkpoint_via_daemon(
    state: &ApiState,
    checkpoint_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::RestoreCheckpointRequest {
        checkpoint_id: checkpoint_id.to_string(),
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.restore_checkpoint(grpc_request).await {
        Ok(grpc_response) => {
            let Some(payload) = grpc_response.checkpoint else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing checkpoint payload",
                    request_id,
                ));
            };
            let checkpoint = checkpoint_payload_from_runtime_proto(payload);
            let compatibility_fingerprint = if checkpoint.compatibility_fingerprint.is_empty()
                || checkpoint.compatibility_fingerprint == "unset"
            {
                None
            } else {
                Some(checkpoint.compatibility_fingerprint.clone())
            };
            Some(
                (
                    StatusCode::OK,
                    Json(RestoreCheckpointResponse {
                        request_id: if grpc_response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            grpc_response.request_id
                        },
                        checkpoint,
                        compatibility_fingerprint,
                        restore_note: "Backend-level restore is delegated to the runtime; fingerprint validation is the caller's responsibility".to_string(),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_fork_checkpoint_via_daemon(
    state: &ApiState,
    checkpoint_id: &str,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: ForkCheckpointRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::ForkCheckpointRequest {
        checkpoint_id: checkpoint_id.to_string(),
        new_sandbox_id: body.new_sandbox_id.unwrap_or_default(),
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
    };
    match client.fork_checkpoint_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_string());
            let grpc_response = grpc_response.into_inner();
            let Some(payload) = grpc_response.checkpoint else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing checkpoint payload",
                    request_id,
                ));
            };
            let mut response = (
                StatusCode::CREATED,
                Json(CheckpointResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    checkpoint: checkpoint_payload_from_runtime_proto(payload),
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_list_checkpoint_children_via_daemon(
    state: &ApiState,
    checkpoint_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    // Match prior API behavior: return 404 when parent checkpoint does not exist.
    let parent = match client
        .get_checkpoint(runtime_v2::GetCheckpointRequest {
            checkpoint_id: checkpoint_id.to_string(),
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
    {
        Ok(response) => response.checkpoint,
        Err(DaemonClientError::Grpc(status)) => {
            return Some(daemon_status_to_http_response(status, request_id));
        }
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };
    if parent.is_none() {
        return Some(json_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "daemon returned missing checkpoint payload",
            request_id,
        ));
    }

    match client
        .list_checkpoints(runtime_v2::ListCheckpointsRequest {
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
    {
        Ok(response) => {
            let checkpoints = response
                .checkpoints
                .into_iter()
                .map(checkpoint_payload_from_runtime_proto)
                .filter(|checkpoint| {
                    checkpoint.parent_checkpoint_id.as_deref() == Some(checkpoint_id)
                })
                .collect();
            Some(
                (
                    StatusCode::OK,
                    Json(CheckpointListResponse {
                        request_id: if response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            response.request_id
                        },
                        checkpoints,
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_list_events_via_daemon(
    state: &ApiState,
    stack_name: &str,
    query: &EventsQuery,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let limit = query
        .limit
        .unwrap_or(state.default_event_page_size)
        .clamp(1, MAX_EVENT_PAGE_SIZE);
    let after = query.after.unwrap_or(0);

    match client
        .list_events(runtime_v2::ListEventsRequest {
            stack_name: stack_name.to_string(),
            after,
            limit: limit as u32,
            scope: query.scope.clone().unwrap_or_default(),
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
    {
        Ok(response) => {
            let events = response
                .events
                .into_iter()
                .map(api_event_record_from_runtime_proto)
                .collect();
            Some(
                (
                    StatusCode::OK,
                    Json(EventsResponse {
                        request_id: if response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            response.request_id
                        },
                        events,
                        next_cursor: response.next_cursor,
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn open_event_stream_via_daemon(
    state: &ApiState,
    stack_name: &str,
    query: &EventsQuery,
    request_id: &str,
) -> Result<tonic::Streaming<runtime_v2::RuntimeEvent>, Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Err(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    client
        .stream_events(runtime_v2::StreamEventsRequest {
            stack_name: stack_name.to_string(),
            after: query.after.unwrap_or(0),
            scope: query.scope.clone().unwrap_or_default(),
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
        .map_err(|error| match error {
            DaemonClientError::Grpc(status) => daemon_status_to_http_response(status, request_id),
            other => json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &other.to_string(),
                request_id,
            ),
        })
}

pub(super) async fn open_execution_output_stream_via_daemon(
    state: &ApiState,
    execution_id: &str,
    request_id: &str,
) -> Result<tonic::Streaming<runtime_v2::ExecOutputEvent>, Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Err(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    client
        .stream_exec_output(runtime_v2::StreamExecOutputRequest {
            execution_id: execution_id.to_string(),
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
        .map_err(|error| match error {
            DaemonClientError::Grpc(status) => daemon_status_to_http_response(status, request_id),
            other => json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &other.to_string(),
                request_id,
            ),
        })
}

pub(super) async fn try_list_images_via_daemon(
    state: &ApiState,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    match client
        .list_images(runtime_v2::ListImagesRequest {
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
    {
        Ok(response) => {
            let images = response
                .images
                .into_iter()
                .map(image_payload_from_runtime_proto)
                .collect();
            Some(
                (
                    StatusCode::OK,
                    Json(ImageListResponse {
                        request_id: if response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            response.request_id
                        },
                        images,
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_get_image_via_daemon(
    state: &ApiState,
    image_ref: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    match client
        .get_image(runtime_v2::GetImageRequest {
            image_ref: image_ref.to_string(),
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
    {
        Ok(response) => {
            let Some(image) = response.image else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing image payload",
                    request_id,
                ));
            };
            Some(
                (
                    StatusCode::OK,
                    Json(ImageResponse {
                        request_id: if response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            response.request_id
                        },
                        image: image_payload_from_runtime_proto(image),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_pull_image_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: PullImageRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let mut stream = match client
        .pull_image(runtime_v2::PullImageRequest {
            image_ref: body.image_ref,
            metadata: Some(daemon_request_metadata(
                request_id,
                extract_idempotency_key(headers),
            )),
        })
        .await
    {
        Ok(stream) => stream,
        Err(DaemonClientError::Grpc(status)) => {
            return Some(daemon_status_to_http_response(status, request_id));
        }
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let mut completion = None;
    loop {
        match stream.message().await {
            Ok(Some(event)) => {
                if let Some(runtime_v2::pull_image_event::Payload::Completion(done)) = event.payload
                {
                    completion = Some(done);
                }
            }
            Ok(None) => break,
            Err(status) => return Some(daemon_status_to_http_response(status, request_id)),
        }
    }

    let Some(done) = completion else {
        return Some(json_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "daemon pull_image stream ended without completion",
            request_id,
        ));
    };
    let Some(image) = done.image else {
        return Some(json_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "daemon returned missing image payload",
            request_id,
        ));
    };
    let receipt_id = normalize_optional_wire_field(&done.receipt_id);
    Some(
        (
            StatusCode::OK,
            Json(PullImageResponse {
                request_id: if done.request_id.trim().is_empty() {
                    request_id.to_string()
                } else {
                    done.request_id
                },
                image: image_payload_from_runtime_proto(image),
                receipt_id,
            }),
        )
            .into_response(),
    )
}

pub(super) async fn try_prune_images_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let mut stream = match client
        .prune_images(runtime_v2::PruneImagesRequest {
            metadata: Some(daemon_request_metadata(
                request_id,
                extract_idempotency_key(headers),
            )),
        })
        .await
    {
        Ok(stream) => stream,
        Err(DaemonClientError::Grpc(status)) => {
            return Some(daemon_status_to_http_response(status, request_id));
        }
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let mut completion = None;
    loop {
        match stream.message().await {
            Ok(Some(event)) => {
                if let Some(runtime_v2::prune_images_event::Payload::Completion(done)) =
                    event.payload
                {
                    completion = Some(done);
                }
            }
            Ok(None) => break,
            Err(status) => return Some(daemon_status_to_http_response(status, request_id)),
        }
    }

    let Some(done) = completion else {
        return Some(json_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "daemon prune_images stream ended without completion",
            request_id,
        ));
    };
    Some(
        (
            StatusCode::OK,
            Json(PruneImagesResponse {
                request_id: if done.request_id.trim().is_empty() {
                    request_id.to_string()
                } else {
                    done.request_id
                },
                removed_refs: done.removed_refs,
                removed_manifests: done.removed_manifests,
                removed_configs: done.removed_configs,
                removed_layer_dirs: done.removed_layer_dirs,
                remaining_images: done.remaining_images,
                receipt_id: normalize_optional_wire_field(&done.receipt_id),
            }),
        )
            .into_response(),
    )
}

pub(super) async fn try_get_receipt_via_daemon(
    state: &ApiState,
    receipt_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    match client
        .get_receipt(runtime_v2::GetReceiptRequest {
            receipt_id: receipt_id.to_string(),
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
    {
        Ok(response) => {
            let Some(receipt) = response.receipt else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing receipt payload",
                    request_id,
                ));
            };
            Some(
                (
                    StatusCode::OK,
                    Json(ReceiptResponse {
                        request_id: if response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            response.request_id
                        },
                        receipt: receipt_payload_from_runtime_proto(receipt),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_create_container_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: CreateContainerRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::CreateContainerRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        sandbox_id: body.sandbox_id,
        image_digest: body.image_digest.unwrap_or_default(),
        cmd: body.cmd.unwrap_or_default(),
        env: body.env.unwrap_or_default().into_iter().collect(),
        cwd: body.cwd.unwrap_or_default(),
        user: body.user.unwrap_or_default(),
    };

    match client.create_container_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_string());
            let grpc_response = grpc_response.into_inner();
            let Some(payload) = grpc_response.container else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing container payload",
                    request_id,
                ));
            };

            let mut response = (
                StatusCode::CREATED,
                Json(ContainerResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    container: container_payload_from_runtime_proto(payload),
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_list_containers_via_daemon(
    state: &ApiState,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    match client
        .list_containers(runtime_v2::ListContainersRequest {
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
    {
        Ok(response) => {
            let containers = response
                .containers
                .into_iter()
                .map(container_payload_from_runtime_proto)
                .collect();
            Some(
                (
                    StatusCode::OK,
                    Json(ContainerListResponse {
                        request_id: if response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            response.request_id
                        },
                        containers,
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_get_container_via_daemon(
    state: &ApiState,
    container_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    match client
        .get_container(runtime_v2::GetContainerRequest {
            container_id: container_id.to_string(),
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
    {
        Ok(response) => {
            let Some(payload) = response.container else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing container payload",
                    request_id,
                ));
            };
            Some(
                (
                    StatusCode::OK,
                    Json(ContainerResponse {
                        request_id: if response.request_id.trim().is_empty() {
                            request_id.to_string()
                        } else {
                            response.request_id
                        },
                        container: container_payload_from_runtime_proto(payload),
                    }),
                )
                    .into_response(),
            )
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_remove_container_via_daemon(
    state: &ApiState,
    container_id: &str,
    request_id: &str,
) -> Option<Response> {
    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    match client
        .remove_container_with_metadata(runtime_v2::RemoveContainerRequest {
            container_id: container_id.to_string(),
            metadata: Some(daemon_request_metadata(request_id, None)),
        })
        .await
    {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_string());
            let grpc_response = grpc_response.into_inner();
            let Some(payload) = grpc_response.container else {
                return Some(json_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "daemon returned missing container payload",
                    request_id,
                ));
            };

            let mut response = (
                StatusCode::OK,
                Json(ContainerResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    container: container_payload_from_runtime_proto(payload),
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_read_file_via_daemon(
    state: &ApiState,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: ReadFileRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::ReadFileRequest {
        metadata: Some(daemon_request_metadata(request_id, None)),
        sandbox_id: body.sandbox_id,
        path: body.path,
        offset: body.offset.unwrap_or(0),
        limit: body.limit.unwrap_or(0),
    };

    match client.read_file(grpc_request).await {
        Ok(response) => Some(
            (
                StatusCode::OK,
                Json(ReadFileResponse {
                    request_id: if response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        response.request_id
                    },
                    data_base64: BASE64_STANDARD.encode(response.data),
                    truncated: response.truncated,
                }),
            )
                .into_response(),
        ),
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_write_file_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: WriteFileRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let decoded = match BASE64_STANDARD.decode(body.data_base64.trim()) {
        Ok(decoded) => decoded,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("data_base64 must be valid base64: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::WriteFileRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        sandbox_id: body.sandbox_id,
        path: body.path,
        data: decoded,
        append: body.append.unwrap_or(false),
        create_parents: body.create_parents.unwrap_or(false),
    };

    match client.write_file_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let mut response = (
                StatusCode::OK,
                Json(WriteFileResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    bytes_written: grpc_response.bytes_written,
                }),
            )
                .into_response();

            if let Some(receipt_id) = receipt_id
                && let Ok(value) = HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_list_files_via_daemon(
    state: &ApiState,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: ListFilesRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::ListFilesRequest {
        metadata: Some(daemon_request_metadata(request_id, None)),
        sandbox_id: body.sandbox_id,
        path: body.path.unwrap_or_default(),
        recursive: body.recursive.unwrap_or(false),
        limit: body.limit.unwrap_or(0),
    };

    match client.list_files(grpc_request).await {
        Ok(response) => Some(
            (
                StatusCode::OK,
                Json(ListFilesResponse {
                    request_id: if response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        response.request_id
                    },
                    entries: response
                        .entries
                        .into_iter()
                        .map(file_entry_payload_from_runtime_proto)
                        .collect(),
                }),
            )
                .into_response(),
        ),
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_make_dir_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: MakeDirRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::MakeDirRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        sandbox_id: body.sandbox_id,
        path: body.path,
        parents: body.parents.unwrap_or(false),
    };

    match client.make_dir_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let mut response = (
                StatusCode::OK,
                Json(FileMutationResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    path: grpc_response.path,
                    status: grpc_response.status,
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_remove_path_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: RemovePathRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::RemovePathRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        sandbox_id: body.sandbox_id,
        path: body.path,
        recursive: body.recursive.unwrap_or(false),
    };

    match client.remove_path_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let mut response = (
                StatusCode::OK,
                Json(FileMutationResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    path: grpc_response.path,
                    status: grpc_response.status,
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_move_path_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: MovePathRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::MovePathRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        sandbox_id: body.sandbox_id,
        src_path: body.src_path,
        dst_path: body.dst_path,
        overwrite: body.overwrite.unwrap_or(false),
    };

    match client.move_path_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let mut response = (
                StatusCode::OK,
                Json(FileMutationResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    path: grpc_response.path,
                    status: grpc_response.status,
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_copy_path_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: CopyPathRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::CopyPathRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        sandbox_id: body.sandbox_id,
        src_path: body.src_path,
        dst_path: body.dst_path,
        overwrite: body.overwrite.unwrap_or(false),
    };

    match client.copy_path_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let mut response = (
                StatusCode::OK,
                Json(FileMutationResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    path: grpc_response.path,
                    status: grpc_response.status,
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_chmod_path_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: ChmodPathRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::ChmodPathRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        sandbox_id: body.sandbox_id,
        path: body.path,
        mode: body.mode,
    };

    match client.chmod_path_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let mut response = (
                StatusCode::OK,
                Json(FileMutationResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    path: grpc_response.path,
                    status: grpc_response.status,
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}

pub(super) async fn try_chown_path_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: ChownPathRequest = match serde_json::from_slice(raw_body) {
        Ok(body) => body,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                request_id,
            ));
        }
    };

    let mut client = match DaemonClient::connect_with_config(daemon_client_config(state)).await {
        Ok(client) => client,
        Err(error) => {
            return Some(json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "daemon_unavailable",
                &error.to_string(),
                request_id,
            ));
        }
    };

    let grpc_request = runtime_v2::ChownPathRequest {
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
        sandbox_id: body.sandbox_id,
        path: body.path,
        uid: body.uid,
        gid: body.gid,
    };

    match client.chown_path_with_metadata(grpc_request).await {
        Ok(grpc_response) => {
            let receipt_id = grpc_response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let grpc_response = grpc_response.into_inner();
            let mut response = (
                StatusCode::OK,
                Json(FileMutationResponse {
                    request_id: if grpc_response.request_id.trim().is_empty() {
                        request_id.to_string()
                    } else {
                        grpc_response.request_id
                    },
                    path: grpc_response.path,
                    status: grpc_response.status,
                }),
            )
                .into_response();
            if let Some(receipt_id) = receipt_id
                && let Ok(value) = HeaderValue::from_str(&receipt_id)
            {
                response.headers_mut().insert("x-receipt-id", value);
            }
            Some(response)
        }
        Err(DaemonClientError::Grpc(status)) => {
            Some(daemon_status_to_http_response(status, request_id))
        }
        Err(error) => Some(json_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "daemon_unavailable",
            &error.to_string(),
            request_id,
        )),
    }
}
