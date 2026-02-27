use super::*;
pub(crate) enum StackServiceActionKind {
    Stop,
    Start,
    Restart,
}

pub(crate) async fn try_apply_stack_via_daemon(
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

pub(crate) async fn try_teardown_stack_via_daemon(
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

pub(crate) async fn try_get_stack_status_via_daemon(
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

pub(crate) async fn try_list_stack_events_via_daemon(
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

pub(crate) async fn try_get_stack_logs_via_daemon(
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

pub(crate) async fn try_stack_service_action_via_daemon(
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

pub(crate) async fn try_create_stack_run_container_via_daemon(
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

pub(crate) async fn try_remove_stack_run_container_via_daemon(
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
