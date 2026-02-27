use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
pub(crate) async fn try_read_file_via_daemon(
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

pub(crate) async fn try_write_file_via_daemon(
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

pub(crate) async fn try_list_files_via_daemon(
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

pub(crate) async fn try_make_dir_via_daemon(
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

pub(crate) async fn try_remove_path_via_daemon(
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

pub(crate) async fn try_move_path_via_daemon(
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

pub(crate) async fn try_copy_path_via_daemon(
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

pub(crate) async fn try_chmod_path_via_daemon(
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

pub(crate) async fn try_chown_path_via_daemon(
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
