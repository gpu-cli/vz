use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct StreamErrorBody {
    code: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct SseStreamErrorPayload {
    request_id: String,
    error: StreamErrorBody,
}

#[derive(Debug, Serialize)]
struct WsStreamErrorPayload {
    error: StreamErrorBody,
}

fn serialize_json_or_marker(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| serialization_error_value().to_string())
}

pub(super) async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(openapi_document())
}

pub(super) async fn metrics_prometheus(State(state): State<ApiState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
        )],
        state.observability.render_prometheus(),
    )
}

pub(super) async fn capabilities(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let request_id = request_id_from_headers(&headers);
    let capabilities = state.capabilities.to_capability_list();
    Json(CapabilitiesResponse {
        request_id,
        capabilities,
    })
}

pub(super) async fn apply_stack(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_apply_stack_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "stack operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn teardown_stack(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_teardown_stack_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "stack operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn get_stack_status(
    State(state): State<ApiState>,
    Path(stack_name): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_get_stack_status_via_daemon(&state, &stack_name, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "stack operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn list_stack_events(
    State(state): State<ApiState>,
    Path(stack_name): Path<String>,
    Query(query): Query<StackEventsQuery>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_list_stack_events_via_daemon(&state, &stack_name, &query, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "stack operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn get_stack_logs(
    State(state): State<ApiState>,
    Path(stack_name): Path<String>,
    Query(query): Query<StackLogsQuery>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_get_stack_logs_via_daemon(&state, &stack_name, &query, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "stack operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn stop_stack_service(
    State(state): State<ApiState>,
    Path((stack_name, service_name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_stack_service_action_via_daemon(
        &state,
        &headers,
        &stack_name,
        &service_name,
        &request_id,
        StackServiceActionKind::Stop,
    )
    .await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "stack operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn start_stack_service(
    State(state): State<ApiState>,
    Path((stack_name, service_name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_stack_service_action_via_daemon(
        &state,
        &headers,
        &stack_name,
        &service_name,
        &request_id,
        StackServiceActionKind::Start,
    )
    .await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "stack operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn restart_stack_service(
    State(state): State<ApiState>,
    Path((stack_name, service_name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_stack_service_action_via_daemon(
        &state,
        &headers,
        &stack_name,
        &service_name,
        &request_id,
        StackServiceActionKind::Restart,
    )
    .await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "stack operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn create_stack_run_container(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_create_stack_run_container_via_daemon(&state, &headers, raw_body.as_ref(), &request_id)
            .await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "stack operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn remove_stack_run_container(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_remove_stack_run_container_via_daemon(&state, &headers, raw_body.as_ref(), &request_id)
            .await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "stack operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn list_events(
    State(state): State<ApiState>,
    Path(stack_name): Path<String>,
    Query(query): Query<EventsQuery>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_list_events_via_daemon(&state, &stack_name, &query, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "event operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn stream_events_sse(
    State(state): State<ApiState>,
    Path(stack_name): Path<String>,
    Query(query): Query<EventsQuery>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let mut stream =
        match open_event_stream_via_daemon(&state, &stack_name, &query, &request_id).await {
            Ok(stream) => stream,
            Err(response) => return response,
        };
    let keep_alive = state.event_poll_interval;
    let stream_request_id = request_id.clone();

    let sse_stream = stream! {
        loop {
            match stream.message().await {
                Ok(Some(runtime_event)) => {
                    let event = api_event_record_from_runtime_proto(runtime_event);
                    let payload = serialize_json_or_marker(&event);
                    yield Ok::<Event, Infallible>(Event::default().id(event.id.to_string()).data(payload));
                }
                Ok(None) => break,
                Err(status) => {
                    let payload = SseStreamErrorPayload {
                        request_id: stream_request_id.clone(),
                        error: StreamErrorBody {
                            code: "backend_unavailable".to_string(),
                            message: status.message().to_string(),
                        },
                    };
                    yield Ok::<Event, Infallible>(
                        Event::default().event("error").data(serialize_json_or_marker(&payload))
                    );
                    break;
                }
            }
        }
    };

    Sse::new(sse_stream)
        .keep_alive(KeepAlive::new().interval(keep_alive))
        .into_response()
}

pub(super) async fn stream_events_ws(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
    Path(stack_name): Path<String>,
    Query(query): Query<EventsQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let request_id = request_id_from_headers(&headers);
    let stream = match open_event_stream_via_daemon(&state, &stack_name, &query, &request_id).await
    {
        Ok(stream) => stream,
        Err(response) => return response,
    };

    ws.on_upgrade(move |socket| forward_events_to_websocket(socket, stream))
}

pub(super) async fn forward_events_to_websocket(
    mut socket: WebSocket,
    mut stream: tonic::Streaming<runtime_v2::RuntimeEvent>,
) {
    loop {
        match stream.message().await {
            Ok(Some(runtime_event)) => {
                let event = api_event_record_from_runtime_proto(runtime_event);
                let payload = serialize_json_or_marker(&event);
                if socket.send(Message::Text(payload.into())).await.is_err() {
                    return;
                }
            }
            Ok(None) => return,
            Err(status) => {
                let payload = WsStreamErrorPayload {
                    error: StreamErrorBody {
                        code: "backend_unavailable".to_string(),
                        message: status.message().to_string(),
                    },
                };
                let _ = socket
                    .send(Message::Text(serialize_json_or_marker(&payload).into()))
                    .await;
                return;
            }
        }
    }
}

pub(super) async fn create_sandbox(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_create_sandbox_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "sandbox operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn list_sandboxes(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);

    if let Some(response) = try_list_sandboxes_via_daemon(&state, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "sandbox operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn get_sandbox(
    State(state): State<ApiState>,
    Path(sandbox_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);

    if let Some(response) = try_get_sandbox_via_daemon(&state, &sandbox_id, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "sandbox operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn terminate_sandbox(
    State(state): State<ApiState>,
    Path(sandbox_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);

    if let Some(response) =
        try_terminate_sandbox_via_daemon(&state, &headers, &sandbox_id, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "sandbox operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn open_sandbox_shell(
    State(state): State<ApiState>,
    Path(sandbox_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);

    if let Some(response) =
        try_open_sandbox_shell_via_daemon(&state, &sandbox_id, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "sandbox shell operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn close_sandbox_shell(
    State(state): State<ApiState>,
    Path(sandbox_id): Path<String>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_close_sandbox_shell_via_daemon(
        &state,
        &headers,
        &sandbox_id,
        raw_body.as_ref(),
        &request_id,
    )
    .await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "sandbox shell operations require vz-runtimed daemon",
        &request_id,
    )
}

// ── Lease handlers ──

pub(super) async fn open_lease(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_open_lease_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "lease operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn list_leases(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_list_leases_via_daemon(&state, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "lease operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn get_lease(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_get_lease_via_daemon(&state, &lease_id, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "lease operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn close_lease(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_close_lease_via_daemon(&state, &lease_id, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "lease operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn heartbeat_lease(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_heartbeat_lease_via_daemon(&state, &lease_id, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "lease operations require vz-runtimed daemon",
        &request_id,
    )
}

// ── Execution handlers ──

pub(super) async fn create_execution(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_create_execution_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "execution operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn list_executions(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_list_executions_via_daemon(&state, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "execution operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn get_execution(
    State(state): State<ApiState>,
    Path(execution_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_get_execution_via_daemon(&state, &execution_id, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "execution operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn cancel_execution(
    State(state): State<ApiState>,
    Path(execution_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_cancel_execution_via_daemon(&state, &execution_id, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "execution operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn resize_exec(
    State(state): State<ApiState>,
    Path(execution_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ResizeExecRequest>,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_resize_execution_via_daemon(&state, &execution_id, &body, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "execution operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn write_exec_stdin(
    State(state): State<ApiState>,
    Path(execution_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<WriteExecStdinRequest>,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_write_execution_stdin_via_daemon(&state, &execution_id, &body, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "execution operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn signal_exec(
    State(state): State<ApiState>,
    Path(execution_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<SignalExecRequest>,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_signal_execution_via_daemon(&state, &execution_id, &body, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "execution operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn stream_execution_output_sse(
    State(state): State<ApiState>,
    Path(execution_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let mut stream =
        match open_execution_output_stream_via_daemon(&state, &execution_id, &request_id).await {
            Ok(stream) => stream,
            Err(response) => return response,
        };
    let keep_alive = state.event_poll_interval;
    let stream_request_id = request_id.clone();

    let sse_stream = stream! {
        loop {
            match stream.message().await {
                Ok(Some(runtime_event)) => {
                    let payload = match runtime_event.payload {
                        Some(runtime_v2::exec_output_event::Payload::Stdout(chunk)) => {
                            ExecutionOutputStreamEventPayload {
                                sequence: runtime_event.sequence,
                                event: "stdout".to_string(),
                                data_base64: Some(BASE64_STANDARD.encode(chunk)),
                                exit_code: None,
                                error: None,
                            }
                        }
                        Some(runtime_v2::exec_output_event::Payload::Stderr(chunk)) => {
                            ExecutionOutputStreamEventPayload {
                                sequence: runtime_event.sequence,
                                event: "stderr".to_string(),
                                data_base64: Some(BASE64_STANDARD.encode(chunk)),
                                exit_code: None,
                                error: None,
                            }
                        }
                        Some(runtime_v2::exec_output_event::Payload::ExitCode(code)) => {
                            ExecutionOutputStreamEventPayload {
                                sequence: runtime_event.sequence,
                                event: "exit_code".to_string(),
                                data_base64: None,
                                exit_code: Some(code),
                                error: None,
                            }
                        }
                        Some(runtime_v2::exec_output_event::Payload::Error(message)) => {
                            ExecutionOutputStreamEventPayload {
                                sequence: runtime_event.sequence,
                                event: "error".to_string(),
                                data_base64: None,
                                exit_code: None,
                                error: Some(message),
                            }
                        }
                        None => continue,
                    };
                    let payload_json = serialize_json_or_marker(&payload);
                    yield Ok::<Event, Infallible>(
                        Event::default()
                            .id(payload.sequence.to_string())
                            .event(payload.event.clone())
                            .data(payload_json),
                    );
                }
                Ok(None) => break,
                Err(status) => {
                    let payload = SseStreamErrorPayload {
                        request_id: stream_request_id.clone(),
                        error: StreamErrorBody {
                            code: "backend_unavailable".to_string(),
                            message: status.message().to_string(),
                        },
                    };
                    yield Ok::<Event, Infallible>(
                        Event::default().event("error").data(serialize_json_or_marker(&payload))
                    );
                    break;
                }
            }
        }
    };

    Sse::new(sse_stream)
        .keep_alive(KeepAlive::new().interval(keep_alive))
        .into_response()
}

// ── Checkpoint handlers ──

pub(super) async fn create_checkpoint(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_create_checkpoint_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "checkpoint operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn list_checkpoints(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_list_checkpoints_via_daemon(&state, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "checkpoint operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn get_checkpoint(
    State(state): State<ApiState>,
    Path(checkpoint_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_get_checkpoint_via_daemon(&state, &checkpoint_id, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "checkpoint operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn restore_checkpoint(
    State(state): State<ApiState>,
    Path(checkpoint_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_restore_checkpoint_via_daemon(&state, &checkpoint_id, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "checkpoint operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn fork_checkpoint(
    State(state): State<ApiState>,
    Path(checkpoint_id): Path<String>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_fork_checkpoint_via_daemon(
        &state,
        &checkpoint_id,
        &headers,
        raw_body.as_ref(),
        &request_id,
    )
    .await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "checkpoint operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn list_checkpoint_children(
    State(state): State<ApiState>,
    Path(checkpoint_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_list_checkpoint_children_via_daemon(&state, &checkpoint_id, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "checkpoint operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn export_checkpoint(
    State(state): State<ApiState>,
    Path(checkpoint_id): Path<String>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_export_checkpoint_via_daemon(&state, &checkpoint_id, raw_body.as_ref(), &request_id)
            .await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "checkpoint operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn import_checkpoint(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_import_checkpoint_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "checkpoint operations require vz-runtimed daemon",
        &request_id,
    )
}

// ── Container handlers ──

pub(super) async fn create_container(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_create_container_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "container operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn list_containers(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_list_containers_via_daemon(&state, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "container operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn get_container(
    State(state): State<ApiState>,
    Path(container_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_get_container_via_daemon(&state, &container_id, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "container operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn remove_container(
    State(state): State<ApiState>,
    Path(container_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_remove_container_via_daemon(&state, &container_id, &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "container operations require vz-runtimed daemon",
        &request_id,
    )
}

// ── Image handlers ──

pub(super) async fn list_images(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_list_images_via_daemon(&state, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "image operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn get_image(
    State(state): State<ApiState>,
    Path(image_ref): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_get_image_via_daemon(&state, &image_ref, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "image operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn pull_image(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_pull_image_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "image operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn prune_images(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_prune_images_via_daemon(&state, &headers, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "image operations require vz-runtimed daemon",
        &request_id,
    )
}

// ── Build handlers ──

pub(super) async fn start_build(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let body: StartBuildRequest = match serde_json::from_slice(&raw_body) {
        Ok(body) => body,
        Err(error) => {
            return json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {error}"),
                &request_id,
            );
        }
    };
    if let Some(response) = try_start_build_via_daemon(&state, &body, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "build operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn list_builds(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_list_builds_via_daemon(&state, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "build operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn get_build(
    State(state): State<ApiState>,
    Path(build_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_get_build_via_daemon(&state, &build_id, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "build operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn cancel_build(
    State(state): State<ApiState>,
    Path(build_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_cancel_build_via_daemon(&state, &build_id, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "build operations require vz-runtimed daemon",
        &request_id,
    )
}

// ── Receipt handler ──

pub(super) async fn get_receipt(
    State(state): State<ApiState>,
    Path(receipt_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_get_receipt_via_daemon(&state, &receipt_id, &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "receipt operations require vz-runtimed daemon",
        &request_id,
    )
}

// ── File handlers ──

pub(super) async fn read_file(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_read_file_via_daemon(&state, raw_body.as_ref(), &request_id).await {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "file operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn write_file(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_write_file_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "file operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn list_files(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) = try_list_files_via_daemon(&state, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "file operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn make_dir(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_make_dir_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "file operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn remove_path(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_remove_path_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "file operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn move_path(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_move_path_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "file operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn copy_path(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_copy_path_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "file operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn chmod_path(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_chmod_path_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "file operations require vz-runtimed daemon",
        &request_id,
    )
}

pub(super) async fn chown_path(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    if let Some(response) =
        try_chown_path_via_daemon(&state, &headers, raw_body.as_ref(), &request_id).await
    {
        return response;
    }
    json_error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unavailable",
        "file operations require vz-runtimed daemon",
        &request_id,
    )
}
