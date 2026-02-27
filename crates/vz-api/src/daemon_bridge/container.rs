use super::*;
pub(crate) async fn try_create_container_via_daemon(
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

pub(crate) async fn try_list_containers_via_daemon(
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

pub(crate) async fn try_get_container_via_daemon(
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

pub(crate) async fn try_remove_container_via_daemon(
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
