use super::*;
pub(crate) async fn try_list_images_via_daemon(
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

pub(crate) async fn try_get_image_via_daemon(
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

pub(crate) async fn try_pull_image_via_daemon(
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

pub(crate) async fn try_prune_images_via_daemon(
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

pub(crate) async fn try_get_receipt_via_daemon(
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
