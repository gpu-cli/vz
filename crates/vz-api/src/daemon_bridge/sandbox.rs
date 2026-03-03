use super::*;
pub(crate) async fn try_create_sandbox_via_daemon(
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
    let project_dir = body.project_dir.trim();
    if project_dir.is_empty() {
        return Some(json_error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "project_dir is required and cannot be empty",
            request_id,
        ));
    }
    let mut labels: HashMap<String, String> = body.labels.into_iter().collect();
    labels.insert(
        SANDBOX_LABEL_PROJECT_DIR.to_string(),
        project_dir.to_string(),
    );
    labels.insert(
        SANDBOX_LABEL_SPACE_MODE.to_string(),
        SANDBOX_SPACE_MODE_REQUIRED.to_string(),
    );
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

pub(crate) async fn try_get_sandbox_via_daemon(
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

pub(crate) async fn try_list_sandboxes_via_daemon(
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

pub(crate) async fn try_terminate_sandbox_via_daemon(
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

pub(crate) async fn try_open_sandbox_shell_via_daemon(
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
                    break;
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

pub(crate) async fn try_close_sandbox_shell_via_daemon(
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
                    break;
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
