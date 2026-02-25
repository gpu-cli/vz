//! OpenAPI/SSE/WebSocket transport adapter for Runtime V2.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_stream::stream;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;
use vz_runtime_contract::{
    Capability, Checkpoint, CheckpointClass, CheckpointState, Execution, ExecutionSpec,
    ExecutionState, Lease, LeaseState, MachineErrorEnvelope, RequestMetadata, RuntimeCapabilities,
    RuntimeError, Sandbox, SandboxBackend, SandboxSpec, SandboxState,
    runtime_error_machine_envelope,
};
use vz_stack::{EventRecord, IDEMPOTENCY_TTL_SECS, IdempotencyRecord, StackError, StateStore};

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

#[derive(Debug, Deserialize)]
struct CreateSandboxRequest {
    #[serde(default)]
    stack_name: Option<String>,
    #[serde(default)]
    cpus: Option<u8>,
    #[serde(default)]
    memory_mb: Option<u64>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct SandboxPayload {
    sandbox_id: String,
    backend: String,
    state: String,
    cpus: Option<u8>,
    memory_mb: Option<u64>,
    created_at: u64,
    updated_at: u64,
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct SandboxResponse {
    request_id: String,
    sandbox: SandboxPayload,
}

#[derive(Debug, Serialize)]
struct SandboxListResponse {
    request_id: String,
    sandboxes: Vec<SandboxPayload>,
}

fn sandbox_to_payload(s: &Sandbox) -> SandboxPayload {
    SandboxPayload {
        sandbox_id: s.sandbox_id.clone(),
        backend: serde_json::to_string(&s.backend)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        state: serde_json::to_string(&s.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        cpus: s.spec.cpus,
        memory_mb: s.spec.memory_mb,
        created_at: s.created_at,
        updated_at: s.updated_at,
        labels: s.labels.clone(),
    }
}

// ── Lease types ──

#[derive(Debug, Deserialize)]
struct OpenLeaseRequest {
    sandbox_id: String,
    #[serde(default)]
    ttl_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct LeasePayload {
    lease_id: String,
    sandbox_id: String,
    ttl_secs: u64,
    last_heartbeat_at: u64,
    state: String,
}

#[derive(Debug, Serialize)]
struct LeaseResponse {
    request_id: String,
    lease: LeasePayload,
}

#[derive(Debug, Serialize)]
struct LeaseListResponse {
    request_id: String,
    leases: Vec<LeasePayload>,
}

fn lease_to_payload(l: &Lease) -> LeasePayload {
    LeasePayload {
        lease_id: l.lease_id.clone(),
        sandbox_id: l.sandbox_id.clone(),
        ttl_secs: l.ttl_secs,
        last_heartbeat_at: l.last_heartbeat_at,
        state: serde_json::to_string(&l.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
    }
}

// ── Execution types ──

#[derive(Debug, Deserialize)]
struct CreateExecutionRequest {
    container_id: String,
    cmd: Vec<String>,
    #[serde(default)]
    args: Option<Vec<String>>,
    #[serde(default)]
    env_override: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pty: Option<bool>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ExecutionPayload {
    execution_id: String,
    container_id: String,
    state: String,
    exit_code: Option<i32>,
    started_at: Option<u64>,
    ended_at: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ExecutionResponse {
    request_id: String,
    execution: ExecutionPayload,
}

#[derive(Debug, Serialize)]
struct ExecutionListResponse {
    request_id: String,
    executions: Vec<ExecutionPayload>,
}

fn execution_to_payload(e: &Execution) -> ExecutionPayload {
    ExecutionPayload {
        execution_id: e.execution_id.clone(),
        container_id: e.container_id.clone(),
        state: serde_json::to_string(&e.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        exit_code: e.exit_code,
        started_at: e.started_at,
        ended_at: e.ended_at,
    }
}

// ── Checkpoint types ──

#[derive(Debug, Deserialize)]
struct CreateCheckpointRequest {
    sandbox_id: String,
    #[serde(default)]
    class: Option<String>,
    #[serde(default)]
    compatibility_fingerprint: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ForkCheckpointRequest {
    #[serde(default)]
    new_sandbox_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct CheckpointPayload {
    checkpoint_id: String,
    sandbox_id: String,
    parent_checkpoint_id: Option<String>,
    class: String,
    state: String,
    compatibility_fingerprint: String,
    created_at: u64,
}

#[derive(Debug, Serialize)]
struct CheckpointResponse {
    request_id: String,
    checkpoint: CheckpointPayload,
}

#[derive(Debug, Serialize)]
struct CheckpointListResponse {
    request_id: String,
    checkpoints: Vec<CheckpointPayload>,
}

fn checkpoint_to_payload(c: &Checkpoint) -> CheckpointPayload {
    CheckpointPayload {
        checkpoint_id: c.checkpoint_id.clone(),
        sandbox_id: c.sandbox_id.clone(),
        parent_checkpoint_id: c.parent_checkpoint_id.clone(),
        class: serde_json::to_string(&c.class)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        state: serde_json::to_string(&c.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        compatibility_fingerprint: c.compatibility_fingerprint.clone(),
        created_at: c.created_at,
    }
}

fn json_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
) -> Response {
    (
        status,
        Json(json!({
            "error": {
                "code": code,
                "message": message,
                "request_id": request_id
            }
        })),
    )
        .into_response()
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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

/// Extract idempotency key from request headers.
fn extract_idempotency_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Compute a hash of the request body for conflict detection.
fn compute_request_hash(body: &[u8]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    body.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Check idempotency key, return cached response or `None` to proceed.
///
/// When the key matches a prior request with the same body hash, the
/// original response is replayed. When the key matches but the body
/// hash differs a 409 Conflict is returned.
fn check_idempotency(
    store: &StateStore,
    key: &str,
    _operation: &str,
    request_hash: &str,
    request_id: &str,
) -> Option<Response> {
    match store.find_idempotency_result(key) {
        Ok(Some(record)) => {
            if record.request_hash == request_hash {
                // Return cached response
                let status = StatusCode::from_u16(record.status_code)
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                let body: serde_json::Value =
                    serde_json::from_str(&record.response_json).unwrap_or_default();
                Some((status, Json(body)).into_response())
            } else {
                // Conflict -- same key, different request
                Some(json_error_response(
                    StatusCode::CONFLICT,
                    "idempotency_conflict",
                    "Idempotency key already used with different request parameters",
                    request_id,
                ))
            }
        }
        Ok(None) => None, // Proceed with execution
        Err(_) => None,   // On error, proceed without idempotency
    }
}

/// Persist an idempotency result after a successful operation.
fn save_idempotency(
    store: &StateStore,
    key: &str,
    operation: &str,
    request_hash: &str,
    status_code: u16,
    response_json: &str,
) {
    let now = now_epoch_secs();
    let record = IdempotencyRecord {
        key: key.to_string(),
        operation: operation.to_string(),
        request_hash: request_hash.to_string(),
        response_json: response_json.to_string(),
        status_code,
        created_at: now,
        expires_at: now + IDEMPOTENCY_TTL_SECS,
    };
    // Best-effort save; failure here does not block the response.
    let _ = store.save_idempotency_result(&record);
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

async fn create_sandbox(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let idempotency_key = extract_idempotency_key(&headers);
    let request_hash = compute_request_hash(&raw_body);

    // Check for a cached idempotent response.
    if let Some(ref key) = idempotency_key {
        if let Some(cached) =
            check_idempotency(&store, key, "create_sandbox", &request_hash, &request_id)
        {
            return cached;
        }
    }

    let body: CreateSandboxRequest = match serde_json::from_slice(&raw_body) {
        Ok(b) => b,
        Err(e) => {
            return json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("invalid JSON body: {e}"),
                &request_id,
            );
        }
    };

    let now = now_epoch_secs();
    let sandbox = Sandbox {
        sandbox_id: format!("sbx-{}", Uuid::new_v4()),
        backend: SandboxBackend::MacosVz,
        spec: SandboxSpec {
            cpus: body.cpus,
            memory_mb: body.memory_mb,
            ..SandboxSpec::default()
        },
        state: SandboxState::Creating,
        created_at: now,
        updated_at: now,
        labels: body.labels,
    };

    if let Some(ref stack_name) = body.stack_name {
        let mut labels = sandbox.labels.clone();
        labels.insert("stack_name".to_string(), stack_name.clone());
        let sandbox = Sandbox {
            labels,
            ..sandbox.clone()
        };
        if let Err(e) = store.save_sandbox(&sandbox) {
            return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
        }
        let response = SandboxResponse {
            request_id,
            sandbox: sandbox_to_payload(&sandbox),
        };
        if let Some(ref key) = idempotency_key {
            if let Ok(json) = serde_json::to_string(&response) {
                save_idempotency(
                    &store,
                    key,
                    "create_sandbox",
                    &request_hash,
                    StatusCode::CREATED.as_u16(),
                    &json,
                );
            }
        }
        return (StatusCode::CREATED, Json(response)).into_response();
    }

    if let Err(e) = store.save_sandbox(&sandbox) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    let response = SandboxResponse {
        request_id,
        sandbox: sandbox_to_payload(&sandbox),
    };
    if let Some(ref key) = idempotency_key {
        if let Ok(json) = serde_json::to_string(&response) {
            save_idempotency(
                &store,
                key,
                "create_sandbox",
                &request_hash,
                StatusCode::CREATED.as_u16(),
                &json,
            );
        }
    }
    (StatusCode::CREATED, Json(response)).into_response()
}

async fn list_sandboxes(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let sandboxes = match store.list_sandboxes() {
        Ok(list) => list,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    (
        StatusCode::OK,
        Json(SandboxListResponse {
            request_id,
            sandboxes: sandboxes.iter().map(sandbox_to_payload).collect(),
        }),
    )
        .into_response()
}

async fn get_sandbox(
    State(state): State<ApiState>,
    Path(sandbox_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    match store.load_sandbox(&sandbox_id) {
        Ok(Some(sandbox)) => (
            StatusCode::OK,
            Json(SandboxResponse {
                request_id,
                sandbox: sandbox_to_payload(&sandbox),
            }),
        )
            .into_response(),
        Ok(None) => json_error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            &format!("sandbox {sandbox_id} not found"),
            &request_id,
        ),
        Err(e) => stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    }
}

async fn terminate_sandbox(
    State(state): State<ApiState>,
    Path(sandbox_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let idempotency_key = extract_idempotency_key(&headers);
    // For DELETE, the sandbox_id is the request identity.
    let request_hash = compute_request_hash(sandbox_id.as_bytes());

    // Check for a cached idempotent response.
    if let Some(ref key) = idempotency_key {
        if let Some(cached) =
            check_idempotency(&store, key, "terminate_sandbox", &request_hash, &request_id)
        {
            return cached;
        }
    }

    let mut sandbox = match store.load_sandbox(&sandbox_id) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return json_error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                &format!("sandbox {sandbox_id} not found"),
                &request_id,
            );
        }
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    if sandbox.state.is_terminal() {
        let response = SandboxResponse {
            request_id,
            sandbox: sandbox_to_payload(&sandbox),
        };
        if let Some(ref key) = idempotency_key {
            if let Ok(json) = serde_json::to_string(&response) {
                save_idempotency(
                    &store,
                    key,
                    "terminate_sandbox",
                    &request_hash,
                    StatusCode::OK.as_u16(),
                    &json,
                );
            }
        }
        return (StatusCode::OK, Json(response)).into_response();
    }

    // Transition based on current state:
    // Creating -> Failed (can't go directly to Terminated)
    // Ready -> Draining -> Terminated
    // Draining -> Terminated
    match sandbox.state {
        SandboxState::Creating => {
            let _ = sandbox.transition_to(SandboxState::Failed);
        }
        SandboxState::Ready => {
            let _ = sandbox.transition_to(SandboxState::Draining);
            let _ = sandbox.transition_to(SandboxState::Terminated);
        }
        SandboxState::Draining => {
            let _ = sandbox.transition_to(SandboxState::Terminated);
        }
        _ => {}
    }

    sandbox.updated_at = now_epoch_secs();

    if let Err(e) = store.save_sandbox(&sandbox) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    let response = SandboxResponse {
        request_id,
        sandbox: sandbox_to_payload(&sandbox),
    };
    if let Some(ref key) = idempotency_key {
        if let Ok(json) = serde_json::to_string(&response) {
            save_idempotency(
                &store,
                key,
                "terminate_sandbox",
                &request_hash,
                StatusCode::OK.as_u16(),
                &json,
            );
        }
    }
    (StatusCode::OK, Json(response)).into_response()
}

// ── Lease handlers ──

async fn open_lease(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<OpenLeaseRequest>,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let now = now_epoch_secs();
    let ttl_secs = body.ttl_secs.unwrap_or(300);

    let mut lease = Lease {
        lease_id: format!("ls-{}", Uuid::new_v4()),
        sandbox_id: body.sandbox_id,
        ttl_secs,
        last_heartbeat_at: now,
        state: LeaseState::Opening,
    };

    if let Err(e) = lease.transition_to(LeaseState::Active) {
        return json_error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "state_transition_failed",
            &e.to_string(),
            &request_id,
        );
    }

    if let Err(e) = store.save_lease(&lease) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    (
        StatusCode::CREATED,
        Json(LeaseResponse {
            request_id,
            lease: lease_to_payload(&lease),
        }),
    )
        .into_response()
}

async fn list_leases(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let leases = match store.list_leases() {
        Ok(list) => list,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    (
        StatusCode::OK,
        Json(LeaseListResponse {
            request_id,
            leases: leases.iter().map(lease_to_payload).collect(),
        }),
    )
        .into_response()
}

async fn get_lease(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    match store.load_lease(&lease_id) {
        Ok(Some(lease)) => (
            StatusCode::OK,
            Json(LeaseResponse {
                request_id,
                lease: lease_to_payload(&lease),
            }),
        )
            .into_response(),
        Ok(None) => json_error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            &format!("lease {lease_id} not found"),
            &request_id,
        ),
        Err(e) => stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    }
}

async fn close_lease(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let mut lease = match store.load_lease(&lease_id) {
        Ok(Some(l)) => l,
        Ok(None) => {
            return json_error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                &format!("lease {lease_id} not found"),
                &request_id,
            );
        }
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    if lease.state == LeaseState::Closed {
        return (
            StatusCode::OK,
            Json(LeaseResponse {
                request_id,
                lease: lease_to_payload(&lease),
            }),
        )
            .into_response();
    }

    if let Err(e) = lease.transition_to(LeaseState::Closed) {
        return json_error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "state_transition_failed",
            &e.to_string(),
            &request_id,
        );
    }

    if let Err(e) = store.save_lease(&lease) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    (
        StatusCode::OK,
        Json(LeaseResponse {
            request_id,
            lease: lease_to_payload(&lease),
        }),
    )
        .into_response()
}

async fn heartbeat_lease(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let mut lease = match store.load_lease(&lease_id) {
        Ok(Some(l)) => l,
        Ok(None) => {
            return json_error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                &format!("lease {lease_id} not found"),
                &request_id,
            );
        }
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    if lease.state != LeaseState::Active {
        return json_error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_state",
            &format!("lease {lease_id} is not active"),
            &request_id,
        );
    }

    lease.last_heartbeat_at = now_epoch_secs();

    if let Err(e) = store.save_lease(&lease) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    (
        StatusCode::OK,
        Json(LeaseResponse {
            request_id,
            lease: lease_to_payload(&lease),
        }),
    )
        .into_response()
}

// ── Execution handlers ──

async fn create_execution(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<CreateExecutionRequest>,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let execution = Execution {
        execution_id: format!("exec-{}", Uuid::new_v4()),
        container_id: body.container_id,
        exec_spec: ExecutionSpec {
            cmd: body.cmd,
            args: body.args.unwrap_or_default(),
            env_override: body.env_override.unwrap_or_default(),
            pty: body.pty.unwrap_or(false),
            timeout_secs: body.timeout_secs,
        },
        state: ExecutionState::Queued,
        exit_code: None,
        started_at: None,
        ended_at: None,
    };

    if let Err(e) = store.save_execution(&execution) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    (
        StatusCode::CREATED,
        Json(ExecutionResponse {
            request_id,
            execution: execution_to_payload(&execution),
        }),
    )
        .into_response()
}

async fn list_executions(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let executions = match store.list_executions() {
        Ok(list) => list,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    (
        StatusCode::OK,
        Json(ExecutionListResponse {
            request_id,
            executions: executions.iter().map(execution_to_payload).collect(),
        }),
    )
        .into_response()
}

async fn get_execution(
    State(state): State<ApiState>,
    Path(execution_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    match store.load_execution(&execution_id) {
        Ok(Some(execution)) => (
            StatusCode::OK,
            Json(ExecutionResponse {
                request_id,
                execution: execution_to_payload(&execution),
            }),
        )
            .into_response(),
        Ok(None) => json_error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            &format!("execution {execution_id} not found"),
            &request_id,
        ),
        Err(e) => stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    }
}

async fn cancel_execution(
    State(state): State<ApiState>,
    Path(execution_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let mut execution = match store.load_execution(&execution_id) {
        Ok(Some(e)) => e,
        Ok(None) => {
            return json_error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                &format!("execution {execution_id} not found"),
                &request_id,
            );
        }
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    if execution.state.is_terminal() {
        return (
            StatusCode::OK,
            Json(ExecutionResponse {
                request_id,
                execution: execution_to_payload(&execution),
            }),
        )
            .into_response();
    }

    let now = now_epoch_secs();
    if execution.started_at.is_none() {
        execution.started_at = Some(now);
    }
    execution.ended_at = Some(now);
    let _ = execution.transition_to(ExecutionState::Canceled);

    if let Err(e) = store.save_execution(&execution) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    (
        StatusCode::OK,
        Json(ExecutionResponse {
            request_id,
            execution: execution_to_payload(&execution),
        }),
    )
        .into_response()
}

// ── Checkpoint handlers ──

async fn create_checkpoint(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<CreateCheckpointRequest>,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let class_str = body.class.as_deref().unwrap_or("fs_quick");
    let class: CheckpointClass = match class_str {
        "fs_quick" => CheckpointClass::FsQuick,
        "vm_full" => CheckpointClass::VmFull,
        other => {
            return json_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_checkpoint_class",
                &format!("unknown checkpoint class: {other}"),
                &request_id,
            );
        }
    };

    let fingerprint = body
        .compatibility_fingerprint
        .unwrap_or_else(|| "unset".to_string());

    let now = now_epoch_secs();
    let mut checkpoint = Checkpoint {
        checkpoint_id: format!("ckpt-{}", Uuid::new_v4()),
        sandbox_id: body.sandbox_id,
        parent_checkpoint_id: None,
        class,
        state: CheckpointState::Creating,
        created_at: now,
        compatibility_fingerprint: fingerprint,
    };

    if let Err(e) = store.save_checkpoint(&checkpoint) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    // Transition to Ready immediately (in a real implementation this would
    // happen asynchronously after the checkpoint data is captured).
    if let Err(_contract_err) = checkpoint.transition_to(CheckpointState::Ready) {
        return json_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "state_transition_failed",
            "failed to transition checkpoint to ready",
            &request_id,
        );
    }
    if let Err(e) = store.save_checkpoint(&checkpoint) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    (
        StatusCode::CREATED,
        Json(CheckpointResponse {
            request_id,
            checkpoint: checkpoint_to_payload(&checkpoint),
        }),
    )
        .into_response()
}

async fn list_checkpoints(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let checkpoints = match store.list_checkpoints() {
        Ok(list) => list,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    (
        StatusCode::OK,
        Json(CheckpointListResponse {
            request_id,
            checkpoints: checkpoints.iter().map(checkpoint_to_payload).collect(),
        }),
    )
        .into_response()
}

async fn get_checkpoint(
    State(state): State<ApiState>,
    Path(checkpoint_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    match store.load_checkpoint(&checkpoint_id) {
        Ok(Some(checkpoint)) => (
            StatusCode::OK,
            Json(CheckpointResponse {
                request_id,
                checkpoint: checkpoint_to_payload(&checkpoint),
            }),
        )
            .into_response(),
        Ok(None) => json_error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            &format!("checkpoint {checkpoint_id} not found"),
            &request_id,
        ),
        Err(e) => stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    }
}

async fn restore_checkpoint(
    State(state): State<ApiState>,
    Path(checkpoint_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let checkpoint = match store.load_checkpoint(&checkpoint_id) {
        Ok(Some(c)) => c,
        Ok(None) => {
            return json_error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                &format!("checkpoint {checkpoint_id} not found"),
                &request_id,
            );
        }
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    if checkpoint.state != CheckpointState::Ready {
        return json_error_response(
            StatusCode::CONFLICT,
            "checkpoint_not_ready",
            &format!("checkpoint {checkpoint_id} is not in ready state"),
            &request_id,
        );
    }

    // In a real implementation, this would trigger the actual restore operation.
    // For now, return the checkpoint as acknowledgement.
    (
        StatusCode::OK,
        Json(CheckpointResponse {
            request_id,
            checkpoint: checkpoint_to_payload(&checkpoint),
        }),
    )
        .into_response()
}

async fn fork_checkpoint(
    State(state): State<ApiState>,
    Path(checkpoint_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ForkCheckpointRequest>,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let parent = match store.load_checkpoint(&checkpoint_id) {
        Ok(Some(c)) => c,
        Ok(None) => {
            return json_error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                &format!("checkpoint {checkpoint_id} not found"),
                &request_id,
            );
        }
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    if parent.state != CheckpointState::Ready {
        return json_error_response(
            StatusCode::CONFLICT,
            "checkpoint_not_ready",
            &format!("checkpoint {checkpoint_id} is not in ready state"),
            &request_id,
        );
    }

    let new_sandbox_id = body
        .new_sandbox_id
        .unwrap_or_else(|| format!("sbx-{}", Uuid::new_v4()));
    let now = now_epoch_secs();

    let mut forked = Checkpoint {
        checkpoint_id: format!("ckpt-{}", Uuid::new_v4()),
        sandbox_id: new_sandbox_id,
        parent_checkpoint_id: Some(parent.checkpoint_id.clone()),
        class: parent.class,
        state: CheckpointState::Creating,
        created_at: now,
        compatibility_fingerprint: parent.compatibility_fingerprint.clone(),
    };

    if let Err(e) = store.save_checkpoint(&forked) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    // Transition to Ready.
    if let Err(_contract_err) = forked.transition_to(CheckpointState::Ready) {
        return json_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "state_transition_failed",
            "failed to transition forked checkpoint to ready",
            &request_id,
        );
    }
    if let Err(e) = store.save_checkpoint(&forked) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    (
        StatusCode::CREATED,
        Json(CheckpointResponse {
            request_id,
            checkpoint: checkpoint_to_payload(&forked),
        }),
    )
        .into_response()
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
        .route("/v1/sandboxes", get(list_sandboxes).post(create_sandbox))
        .route(
            "/v1/sandboxes/{sandbox_id}",
            get(get_sandbox).delete(terminate_sandbox),
        )
        .route("/v1/leases", get(list_leases).post(open_lease))
        .route("/v1/leases/{lease_id}", get(get_lease).delete(close_lease))
        .route("/v1/leases/{lease_id}/heartbeat", post(heartbeat_lease))
        .route(
            "/v1/executions",
            get(list_executions).post(create_execution),
        )
        .route(
            "/v1/executions/{execution_id}",
            get(get_execution).delete(cancel_execution),
        )
        .route(
            "/v1/checkpoints",
            get(list_checkpoints).post(create_checkpoint),
        )
        .route("/v1/checkpoints/{checkpoint_id}", get(get_checkpoint))
        .route(
            "/v1/checkpoints/{checkpoint_id}/restore",
            post(restore_checkpoint),
        )
        .route(
            "/v1/checkpoints/{checkpoint_id}/fork",
            post(fork_checkpoint),
        )
        .route("/v1/images", any(unsupported_images))
        .route("/v1/builds", any(unsupported_builds))
        .route("/v1/containers", any(unsupported_containers))
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
                && matches!(
                    surface.path,
                    "/v1/capabilities"
                        | "/v1/events/{stack_name}"
                        | "/v1/sandboxes"
                        | "/v1/leases"
                        | "/v1/executions"
                        | "/v1/checkpoints"
                )
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

    fn test_router() -> (Router, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        StateStore::open(&state_path).unwrap();
        let app = router(test_config(state_path));
        (app, temp_dir)
    }

    #[tokio::test]
    async fn sandbox_create_returns_201() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sandboxes")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"cpus": 2, "memory_mb": 1024}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let sandbox_id = payload["sandbox"]["sandbox_id"].as_str().unwrap();
        assert!(sandbox_id.starts_with("sbx-"), "id should start with sbx-");
        assert_eq!(payload["sandbox"]["state"].as_str().unwrap(), "creating");
        assert_eq!(payload["sandbox"]["cpus"].as_u64().unwrap(), 2);
        assert_eq!(payload["sandbox"]["memory_mb"].as_u64().unwrap(), 1024);
    }

    #[tokio::test]
    async fn sandbox_list_empty() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/sandboxes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let sandboxes = payload["sandboxes"].as_array().unwrap();
        assert!(sandboxes.is_empty());
    }

    #[tokio::test]
    async fn sandbox_get_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/sandboxes/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn sandbox_create_then_get() {
        let (app, _dir) = test_router();

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sandboxes")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"cpus": 4}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);

        let create_body = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let create_payload: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
        let sandbox_id = create_payload["sandbox"]["sandbox_id"]
            .as_str()
            .unwrap()
            .to_string();

        let get_response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sandboxes/{sandbox_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);

        let get_body = to_bytes(get_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let get_payload: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
        assert_eq!(
            get_payload["sandbox"]["sandbox_id"].as_str().unwrap(),
            sandbox_id
        );
        assert_eq!(get_payload["sandbox"]["cpus"].as_u64().unwrap(), 4);
    }

    #[tokio::test]
    async fn sandbox_terminate() {
        let (app, _dir) = test_router();

        // Create a sandbox
        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sandboxes")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_response.status(), StatusCode::CREATED);

        let create_body = to_bytes(create_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let create_payload: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
        let sandbox_id = create_payload["sandbox"]["sandbox_id"]
            .as_str()
            .unwrap()
            .to_string();

        // Terminate sandbox (Creating -> Failed since can't go directly to Terminated)
        let delete_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/sandboxes/{sandbox_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_response.status(), StatusCode::OK);

        let delete_body = to_bytes(delete_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let delete_payload: serde_json::Value = serde_json::from_slice(&delete_body).unwrap();
        let state = delete_payload["sandbox"]["state"].as_str().unwrap();
        assert!(
            state == "failed" || state == "terminated",
            "expected terminal state, got {state}"
        );

        // GET should still return it in terminal state
        let get_response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sandboxes/{sandbox_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);

        let get_body = to_bytes(get_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let get_payload: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
        let final_state = get_payload["sandbox"]["state"].as_str().unwrap();
        assert!(
            final_state == "failed" || final_state == "terminated",
            "expected terminal state after GET, got {final_state}"
        );
    }
}
