use super::*;
pub(crate) async fn try_create_checkpoint_via_daemon(
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
        retention_tag: body.retention_tag.unwrap_or_default(),
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

pub(crate) async fn try_list_checkpoints_via_daemon(
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

pub(crate) async fn try_get_checkpoint_via_daemon(
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

pub(crate) async fn try_restore_checkpoint_via_daemon(
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
                        restore_note:
                            "Restore is enforced by vz-runtimed runtime compatibility checks."
                                .to_string(),
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

pub(crate) async fn try_fork_checkpoint_via_daemon(
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

pub(crate) async fn try_export_checkpoint_via_daemon(
    state: &ApiState,
    checkpoint_id: &str,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: ExportCheckpointRequest = match serde_json::from_slice(raw_body) {
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

    let grpc_request = runtime_v2::ExportCheckpointRequest {
        checkpoint_id: checkpoint_id.to_string(),
        stream_path: body.stream_path,
        metadata: Some(daemon_request_metadata(request_id, None)),
    };
    match client.export_checkpoint(grpc_request).await {
        Ok(completion) => Some(
            (
                StatusCode::OK,
                Json(ExportCheckpointResponse {
                    request_id: request_id.to_string(),
                    checkpoint_id: completion.checkpoint_id,
                    stream_path: completion.stream_path,
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

pub(crate) async fn try_import_checkpoint_via_daemon(
    state: &ApiState,
    headers: &HeaderMap,
    raw_body: &[u8],
    request_id: &str,
) -> Option<Response> {
    let body: ImportCheckpointRequest = match serde_json::from_slice(raw_body) {
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

    let grpc_request = runtime_v2::ImportCheckpointRequest {
        sandbox_id: body.sandbox_id,
        stream_path: body.stream_path,
        checkpoint_class,
        compatibility_fingerprint: body
            .compatibility_fingerprint
            .unwrap_or_else(|| "unset".to_string()),
        retention_tag: body.retention_tag.unwrap_or_default(),
        metadata: Some(daemon_request_metadata(
            request_id,
            extract_idempotency_key(headers),
        )),
    };
    match client.import_checkpoint_with_metadata(grpc_request).await {
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

pub(crate) async fn try_list_checkpoint_children_via_daemon(
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
