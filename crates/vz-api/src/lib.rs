//! OpenAPI/SSE/WebSocket transport adapter for Runtime V2.

#![forbid(unsafe_code)]

use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_stream::stream;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use vz_runtime_contract::{
    Capability, MachineErrorEnvelope, RequestMetadata, RuntimeCapabilities, RuntimeError,
    runtime_error_machine_envelope,
};
use vz_stack::{EventRecord, StackError, StateStore};

const DEFAULT_EVENT_PAGE_SIZE: usize = 100;
const MAX_EVENT_PAGE_SIZE: usize = 1000;
const DEFAULT_EVENT_POLL_INTERVAL: Duration = Duration::from_millis(250);

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// API adapter configuration.
#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// SQLite state-store path used for event reads.
    pub state_store_path: PathBuf,
    /// Runtime capabilities advertised by this API surface.
    pub capabilities: RuntimeCapabilities,
    /// Poll interval for SSE/WebSocket event adapters.
    pub event_poll_interval: Duration,
    /// Default event page size for `/v1/events`.
    pub default_event_page_size: usize,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            state_store_path: PathBuf::from("stack-state.db"),
            capabilities: RuntimeCapabilities::default(),
            event_poll_interval: DEFAULT_EVENT_POLL_INTERVAL,
            default_event_page_size: DEFAULT_EVENT_PAGE_SIZE,
        }
    }
}

#[derive(Debug, Clone)]
struct ApiState {
    state_store_path: PathBuf,
    capabilities: RuntimeCapabilities,
    event_poll_interval: Duration,
    default_event_page_size: usize,
}

impl From<ApiConfig> for ApiState {
    fn from(config: ApiConfig) -> Self {
        Self {
            state_store_path: config.state_store_path,
            capabilities: config.capabilities,
            event_poll_interval: config.event_poll_interval,
            default_event_page_size: config.default_event_page_size.clamp(1, MAX_EVENT_PAGE_SIZE),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Copy)]
struct EventsQuery {
    after: Option<i64>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ApiEventRecord {
    /// Monotonic cursor value.
    pub id: i64,
    /// Owning stack name.
    pub stack_name: String,
    /// SQLite event timestamp.
    pub created_at: String,
    /// Serialized stack event payload.
    pub event: serde_json::Value,
}

impl From<EventRecord> for ApiEventRecord {
    fn from(record: EventRecord) -> Self {
        Self {
            id: record.id,
            stack_name: record.stack_name,
            created_at: record.created_at,
            event: serde_json::to_value(record.event)
                .unwrap_or_else(|_| json!({ "type": "serialization_error" })),
        }
    }
}

#[derive(Debug, Serialize)]
struct EventsResponse {
    request_id: String,
    events: Vec<ApiEventRecord>,
    next_cursor: i64,
}

#[derive(Debug, Serialize)]
struct CapabilitiesResponse {
    request_id: String,
    capabilities: Vec<Capability>,
}

fn generated_request_id() -> String {
    let sequence = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    format!("req_{sequence:016x}")
}

fn request_id_from_headers(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(generated_request_id)
}

fn stack_error_response(status: StatusCode, error: StackError, request_id: String) -> Response {
    let metadata = RequestMetadata::from_optional_refs(Some(request_id.as_str()), None);
    let envelope = MachineErrorEnvelope::new(error.to_machine_error(&metadata));
    (status, Json(envelope)).into_response()
}

fn runtime_error_response(status: StatusCode, error: RuntimeError, request_id: String) -> Response {
    let metadata = RequestMetadata::from_optional_refs(Some(request_id.as_str()), None);
    let envelope = runtime_error_machine_envelope(&error, &metadata);
    (status, Json(envelope)).into_response()
}

fn unsupported_operation_response(operation: &'static str, request_id: String) -> Response {
    runtime_error_response(
        StatusCode::NOT_IMPLEMENTED,
        RuntimeError::UnsupportedOperation {
            operation: operation.to_string(),
            reason: "openapi adapter path not wired yet".to_string(),
        },
        request_id,
    )
}

fn read_events(
    state: &ApiState,
    stack_name: &str,
    after: i64,
    limit: usize,
) -> Result<Vec<EventRecord>, StackError> {
    let store = StateStore::open(&state.state_store_path)?;
    store.load_events_since_limited(stack_name, after, limit)
}

async fn openapi_json() -> Json<serde_json::Value> {
    Json(openapi_document())
}

async fn capabilities(State(state): State<ApiState>, headers: HeaderMap) -> impl IntoResponse {
    let request_id = request_id_from_headers(&headers);
    let capabilities = state.capabilities.to_capability_list();
    Json(CapabilitiesResponse {
        request_id,
        capabilities,
    })
}

async fn list_events(
    State(state): State<ApiState>,
    Path(stack_name): Path<String>,
    Query(query): Query<EventsQuery>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let after = query.after.unwrap_or(0);
    let limit = query
        .limit
        .unwrap_or(state.default_event_page_size)
        .clamp(1, MAX_EVENT_PAGE_SIZE);

    let records = match read_events(&state, &stack_name, after, limit) {
        Ok(records) => records,
        Err(error) => {
            return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, error, request_id);
        }
    };
    let next_cursor = records.last().map(|record| record.id).unwrap_or(after);
    let events = records.into_iter().map(ApiEventRecord::from).collect();
    (
        StatusCode::OK,
        Json(EventsResponse {
            request_id,
            events,
            next_cursor,
        }),
    )
        .into_response()
}

async fn stream_events_sse(
    State(state): State<ApiState>,
    Path(stack_name): Path<String>,
    Query(query): Query<EventsQuery>,
    headers: HeaderMap,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let request_id = request_id_from_headers(&headers);
    let mut cursor = query.after.unwrap_or(0);
    let limit = query
        .limit
        .unwrap_or(state.default_event_page_size)
        .clamp(1, MAX_EVENT_PAGE_SIZE);
    let poll_interval = state.event_poll_interval;
    let adapter_state = state.clone();
    let adapter_stack_name = stack_name.clone();

    let event_stream = stream! {
        loop {
            match read_events(&adapter_state, &adapter_stack_name, cursor, limit) {
                Ok(records) => {
                    for record in records {
                        cursor = record.id;
                        let payload = serde_json::to_string(&ApiEventRecord::from(record))
                            .unwrap_or_else(|_| "{\"type\":\"serialization_error\"}".to_string());
                        yield Ok(Event::default()
                            .event("stack_event")
                            .id(cursor.to_string())
                            .data(payload));
                    }
                }
                Err(error) => {
                    let metadata = RequestMetadata::from_optional_refs(Some(request_id.as_str()), None);
                    let envelope = MachineErrorEnvelope::new(error.to_machine_error(&metadata));
                    let payload = serde_json::to_string(&envelope)
                        .unwrap_or_else(|_| "{\"error\":{\"code\":\"internal_error\"}}".to_string());
                    yield Ok(Event::default().event("error").data(payload));
                    break;
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
    };

    Sse::new(event_stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(10))
            .text("keep-alive"),
    )
}

async fn stream_events_ws(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
    Path(stack_name): Path<String>,
    Query(query): Query<EventsQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let request_id = request_id_from_headers(&headers);
    ws.on_upgrade(move |socket| ws_event_loop(socket, state, stack_name, query, request_id))
}

async fn ws_event_loop(
    mut socket: WebSocket,
    state: ApiState,
    stack_name: String,
    query: EventsQuery,
    request_id: String,
) {
    let mut cursor = query.after.unwrap_or(0);
    let limit = query
        .limit
        .unwrap_or(state.default_event_page_size)
        .clamp(1, MAX_EVENT_PAGE_SIZE);

    loop {
        match read_events(&state, &stack_name, cursor, limit) {
            Ok(records) => {
                for record in records {
                    cursor = record.id;
                    let payload = serde_json::to_string(&ApiEventRecord::from(record))
                        .unwrap_or_else(|_| "{\"type\":\"serialization_error\"}".to_string());
                    if socket.send(Message::Text(payload.into())).await.is_err() {
                        return;
                    }
                }
            }
            Err(error) => {
                let metadata = RequestMetadata::from_optional_refs(Some(request_id.as_str()), None);
                let envelope = MachineErrorEnvelope::new(error.to_machine_error(&metadata));
                let payload = serde_json::to_string(&envelope)
                    .unwrap_or_else(|_| "{\"error\":{\"code\":\"internal_error\"}}".to_string());
                let _ = socket.send(Message::Text(payload.into())).await;
                return;
            }
        }
        tokio::time::sleep(state.event_poll_interval).await;
    }
}

async fn unsupported_sandboxes(headers: HeaderMap) -> Response {
    unsupported_operation_response("sandboxes", request_id_from_headers(&headers))
}

async fn unsupported_leases(headers: HeaderMap) -> Response {
    unsupported_operation_response("leases", request_id_from_headers(&headers))
}

async fn unsupported_images(headers: HeaderMap) -> Response {
    unsupported_operation_response("images", request_id_from_headers(&headers))
}

async fn unsupported_builds(headers: HeaderMap) -> Response {
    unsupported_operation_response("builds", request_id_from_headers(&headers))
}

async fn unsupported_containers(headers: HeaderMap) -> Response {
    unsupported_operation_response("containers", request_id_from_headers(&headers))
}

async fn unsupported_executions(headers: HeaderMap) -> Response {
    unsupported_operation_response("executions", request_id_from_headers(&headers))
}

async fn unsupported_checkpoints(headers: HeaderMap) -> Response {
    unsupported_operation_response("checkpoints", request_id_from_headers(&headers))
}

async fn unsupported_receipts(headers: HeaderMap) -> Response {
    unsupported_operation_response("receipts", request_id_from_headers(&headers))
}

/// Build the Runtime V2 API router.
pub fn router(config: ApiConfig) -> Router {
    let state: ApiState = config.into();
    Router::new()
        .route("/openapi.json", get(openapi_json))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/events/{stack_name}", get(list_events))
        .route("/v1/events/{stack_name}/stream", get(stream_events_sse))
        .route("/v1/events/{stack_name}/ws", get(stream_events_ws))
        .route("/v1/sandboxes", any(unsupported_sandboxes))
        .route("/v1/leases", any(unsupported_leases))
        .route("/v1/images", any(unsupported_images))
        .route("/v1/builds", any(unsupported_builds))
        .route("/v1/containers", any(unsupported_containers))
        .route("/v1/executions", any(unsupported_executions))
        .route("/v1/checkpoints", any(unsupported_checkpoints))
        .route("/v1/receipts", any(unsupported_receipts))
        .with_state(state)
}

/// Static OpenAPI 3.1 surface for Runtime V2 external transport.
pub fn openapi_document() -> serde_json::Value {
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "vz Runtime API",
            "version": "v1",
            "description": "Runtime V2 OpenAPI surface with SSE/WebSocket event adapters."
        },
        "paths": {
            "/v1/sandboxes": {},
            "/v1/leases": {},
            "/v1/images": {},
            "/v1/builds": {},
            "/v1/containers": {},
            "/v1/executions": {},
            "/v1/checkpoints": {},
            "/v1/events/{stack_name}": {},
            "/v1/events/{stack_name}/stream": {},
            "/v1/events/{stack_name}/ws": {},
            "/v1/receipts": {},
            "/v1/capabilities": {}
        }
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use std::collections::BTreeSet;
    use tempfile::tempdir;
    use tower::ServiceExt;
    use vz_stack::StackEvent;

    fn test_config(state_store_path: PathBuf) -> ApiConfig {
        ApiConfig {
            state_store_path,
            capabilities: RuntimeCapabilities {
                fs_quick_checkpoint: true,
                checkpoint_fork: true,
                ..RuntimeCapabilities::default()
            },
            event_poll_interval: Duration::from_millis(10),
            default_event_page_size: 2,
        }
    }

    fn sample_openapi_path(path: &str) -> String {
        match path {
            "/v1/events/{stack_name}" => "/v1/events/runtime-conformance-stack".to_string(),
            _ => path.to_string(),
        }
    }

    #[test]
    fn openapi_document_contains_required_paths() {
        let document = openapi_document();
        let paths = document["paths"].as_object().unwrap();
        assert!(paths.contains_key("/v1/sandboxes"));
        assert!(paths.contains_key("/v1/leases"));
        assert!(paths.contains_key("/v1/images"));
        assert!(paths.contains_key("/v1/builds"));
        assert!(paths.contains_key("/v1/containers"));
        assert!(paths.contains_key("/v1/executions"));
        assert!(paths.contains_key("/v1/checkpoints"));
        assert!(paths.contains_key("/v1/events/{stack_name}"));
        assert!(paths.contains_key("/v1/events/{stack_name}/stream"));
        assert!(paths.contains_key("/v1/events/{stack_name}/ws"));
        assert!(paths.contains_key("/v1/receipts"));
        assert!(paths.contains_key("/v1/capabilities"));
    }

    #[tokio::test]
    async fn capabilities_endpoint_returns_runtime_capabilities() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        StateStore::open(&state_path).unwrap();

        let app = router(test_config(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/capabilities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(payload["request_id"].as_str().is_some());
        let capabilities = payload["capabilities"].as_array().unwrap();
        assert!(capabilities.contains(&serde_json::Value::String(
            "fs_quick_checkpoint".to_string()
        )));
        assert!(capabilities.contains(&serde_json::Value::String("checkpoint_fork".to_string())));
    }

    #[tokio::test]
    async fn events_endpoint_respects_cursor_and_limit() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();
        for index in 0..3 {
            store
                .emit_event(
                    "my-stack",
                    &StackEvent::ServiceCreating {
                        stack_name: "my-stack".to_string(),
                        service_name: format!("svc-{index}"),
                    },
                )
                .unwrap();
        }

        let app = router(test_config(state_path.clone()));
        let first_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events/my-stack?after=0&limit=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_response.status(), StatusCode::OK);

        let first_payload: serde_json::Value = serde_json::from_slice(
            &to_bytes(first_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first_payload["events"].as_array().unwrap().len(), 2);
        let next_cursor = first_payload["next_cursor"].as_i64().unwrap();

        let second_response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/my-stack?after={next_cursor}&limit=2"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second_response.status(), StatusCode::OK);
        let second_payload: serde_json::Value = serde_json::from_slice(
            &to_bytes(second_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second_payload["events"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn transport_parity_error_codes_match_runtime_contract() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        StateStore::open(&state_path).unwrap();

        let app = router(test_config(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/containers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let envelope: MachineErrorEnvelope = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            envelope.error.code,
            vz_runtime_contract::MachineErrorCode::UnsupportedOperation
        );
    }

    #[tokio::test]
    async fn transport_parity_event_cursor_matches_state_store_slice() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();
        for index in 0..4 {
            store
                .emit_event(
                    "my-stack",
                    &StackEvent::ServiceCreating {
                        stack_name: "my-stack".to_string(),
                        service_name: format!("svc-{index}"),
                    },
                )
                .unwrap();
        }
        let expected = store.load_events_since_limited("my-stack", 0, 3).unwrap();
        let expected_ids: Vec<i64> = expected.iter().map(|record| record.id).collect();

        let app = router(test_config(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/my-stack?after=0&limit=3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let ids: Vec<i64> = payload["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, expected_ids);
    }

    #[test]
    fn transport_parity_openapi_and_grpc_surface_require_metadata_and_ordering() {
        let exec_request = vz_agent_proto::ExecRequest::default();
        assert!(exec_request.metadata.is_none());
        let oci_create_request = vz_agent_proto::OciCreateRequest::default();
        assert!(oci_create_request.metadata.is_none());

        let exec_event = vz_agent_proto::ExecEvent::default();
        assert_eq!(exec_event.sequence, 0);
        assert!(exec_event.request_id.is_empty());

        let document = openapi_document();
        let paths = document["paths"].as_object().unwrap();
        assert!(paths.contains_key("/v1/executions"));
        assert!(paths.contains_key("/v1/events/{stack_name}/stream"));
        assert!(paths.contains_key("/v1/events/{stack_name}/ws"));
    }

    #[tokio::test]
    async fn transport_parity_openapi_matrix_paths_match_contract() {
        let document = openapi_document();
        let paths = document["paths"].as_object().unwrap();
        let mut matrix_paths = BTreeSet::new();

        for entry in vz_runtime_contract::PRIMITIVE_CONFORMANCE_MATRIX {
            if let Some(surface) = entry.openapi {
                assert!(!surface.path.is_empty());
                assert!(surface.path.starts_with('/'));
                assert!(!surface.surface.is_empty());
                assert!(
                    paths.contains_key(surface.path),
                    "missing OpenAPI path `{}` for `{}`",
                    surface.path,
                    entry.operation.as_str()
                );
                matrix_paths.insert(surface.path);
            }
        }

        assert!(!matrix_paths.is_empty());
    }

    #[tokio::test]
    async fn transport_parity_openapi_surface_errors_match_runtime_operation_labels() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        StateStore::open(&state_path).unwrap();

        let app = router(test_config(state_path));

        for entry in vz_runtime_contract::PRIMITIVE_CONFORMANCE_MATRIX {
            let Some(surface) = entry.openapi else {
                continue;
            };

            let request = Request::builder()
                .uri(sample_openapi_path(surface.path))
                .body(Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            let status = response.status();

            if status == StatusCode::NOT_IMPLEMENTED {
                let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                let envelope: MachineErrorEnvelope = serde_json::from_slice(&body).unwrap();

                assert_eq!(
                    envelope.error.code,
                    vz_runtime_contract::MachineErrorCode::UnsupportedOperation
                );
                assert_eq!(
                    envelope.error.details.get("operation").map(String::as_str),
                    Some(surface.surface),
                    "matrix operation mismatch for `{}` at `{}`",
                    entry.operation.as_str(),
                    surface.path
                );
                continue;
            }

            if status == StatusCode::OK
                && matches!(surface.path, "/v1/capabilities" | "/v1/events/{stack_name}")
            {
                continue;
            }

            panic!(
                "unexpected matrix API status for `{}` at `{}`: {status}",
                entry.operation.as_str(),
                surface.path
            );
        }
    }
}
