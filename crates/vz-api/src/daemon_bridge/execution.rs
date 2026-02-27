use super::*;
pub(crate) async fn try_create_execution_via_daemon(
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

pub(crate) async fn try_list_executions_via_daemon(
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

pub(crate) async fn try_get_execution_via_daemon(
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

pub(crate) async fn try_cancel_execution_via_daemon(
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

pub(crate) async fn try_resize_execution_via_daemon(
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

pub(crate) async fn try_write_execution_stdin_via_daemon(
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

pub(crate) async fn try_signal_execution_via_daemon(
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

pub(crate) async fn open_execution_output_stream_via_daemon(
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
