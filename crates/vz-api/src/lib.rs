//! OpenAPI/SSE/WebSocket transport adapter for Runtime V2.

#![forbid(unsafe_code)]
#![recursion_limit = "256"]

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
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;
use vz_runtime_contract::{
    Build, BuildSpec, BuildState, Capability, Checkpoint, CheckpointClass, CheckpointState,
    Container, ContainerSpec, ContainerState, Execution, ExecutionSpec, ExecutionState, Lease,
    LeaseState, MachineErrorEnvelope, RequestMetadata, RuntimeCapabilities, Sandbox,
    SandboxBackend, SandboxSpec, SandboxState,
};
use vz_stack::{
    EventRecord, IDEMPOTENCY_TTL_SECS, IdempotencyRecord, ImageRecord, Receipt, StackError,
    StackEvent, StateStore,
};

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

#[derive(Debug, Deserialize, Clone)]
struct EventsQuery {
    after: Option<i64>,
    limit: Option<usize>,
    scope: Option<String>,
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

/// Request body for `POST /v1/executions/{execution_id}/resize`.
#[derive(Debug, Deserialize)]
struct ResizeExecRequest {
    cols: u16,
    rows: u16,
}

/// Request body for `POST /v1/executions/{execution_id}/signal`.
#[derive(Debug, Deserialize)]
struct SignalExecRequest {
    signal: String,
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
struct RestoreCheckpointResponse {
    request_id: String,
    checkpoint: CheckpointPayload,
    #[serde(skip_serializing_if = "Option::is_none")]
    compatibility_fingerprint: Option<String>,
    restore_note: String,
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

// ── Container types ──

#[derive(Debug, Deserialize)]
struct CreateContainerRequest {
    sandbox_id: String,
    image_digest: String,
    #[serde(default)]
    cmd: Option<Vec<String>>,
    #[serde(default)]
    env: Option<BTreeMap<String, String>>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    user: Option<String>,
}

#[derive(Debug, Serialize)]
struct ContainerPayload {
    container_id: String,
    sandbox_id: String,
    image_digest: String,
    state: String,
    created_at: u64,
    started_at: Option<u64>,
    ended_at: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ContainerResponse {
    request_id: String,
    container: ContainerPayload,
}

#[derive(Debug, Serialize)]
struct ContainerListResponse {
    request_id: String,
    containers: Vec<ContainerPayload>,
}

fn container_to_payload(c: &Container) -> ContainerPayload {
    ContainerPayload {
        container_id: c.container_id.clone(),
        sandbox_id: c.sandbox_id.clone(),
        image_digest: c.image_digest.clone(),
        state: serde_json::to_string(&c.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        created_at: c.created_at,
        started_at: c.started_at,
        ended_at: c.ended_at,
    }
}

// ── Image types ──

#[derive(Debug, Serialize)]
struct ImagePayload {
    image_ref: String,
    resolved_digest: String,
    platform: String,
    source_registry: String,
    pulled_at: u64,
}

#[derive(Debug, Serialize)]
struct ImageResponse {
    request_id: String,
    image: ImagePayload,
}

#[derive(Debug, Serialize)]
struct ImageListResponse {
    request_id: String,
    images: Vec<ImagePayload>,
}

fn image_to_payload(i: &ImageRecord) -> ImagePayload {
    ImagePayload {
        image_ref: i.image_ref.clone(),
        resolved_digest: i.resolved_digest.clone(),
        platform: i.platform.clone(),
        source_registry: i.source_registry.clone(),
        pulled_at: i.pulled_at,
    }
}

// ── Receipt types ──

#[derive(Debug, Serialize)]
struct ReceiptPayload {
    receipt_id: String,
    operation: String,
    entity_id: String,
    entity_type: String,
    request_id: String,
    status: String,
    created_at: u64,
    metadata: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ReceiptResponse {
    request_id: String,
    receipt: ReceiptPayload,
}

fn receipt_to_payload(r: &Receipt) -> ReceiptPayload {
    ReceiptPayload {
        receipt_id: r.receipt_id.clone(),
        operation: r.operation.clone(),
        entity_id: r.entity_id.clone(),
        entity_type: r.entity_type.clone(),
        request_id: r.request_id.clone(),
        status: r.status.clone(),
        created_at: r.created_at,
        metadata: r.metadata.clone(),
    }
}

// ── Build types ──

#[derive(Debug, Deserialize)]
struct StartBuildRequest {
    sandbox_id: String,
    context: String,
    #[serde(default)]
    dockerfile: Option<String>,
    #[serde(default)]
    args: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Serialize)]
struct BuildPayload {
    build_id: String,
    sandbox_id: String,
    state: String,
    result_digest: Option<String>,
    started_at: u64,
    ended_at: Option<u64>,
}

#[derive(Debug, Serialize)]
struct BuildResponse {
    request_id: String,
    build: BuildPayload,
}

#[derive(Debug, Serialize)]
struct BuildListResponse {
    request_id: String,
    builds: Vec<BuildPayload>,
}

fn build_to_payload(b: &Build) -> BuildPayload {
    BuildPayload {
        build_id: b.build_id.clone(),
        sandbox_id: b.sandbox_id.clone(),
        state: serde_json::to_string(&b.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        result_digest: b.result_digest.clone(),
        started_at: b.started_at,
        ended_at: b.ended_at,
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

/// Create and save a receipt for a completed mutating operation, returning the receipt ID.
///
/// Failures are best-effort; the caller should not block the response on receipt persistence.
fn emit_receipt(
    store: &StateStore,
    operation: &str,
    entity_id: &str,
    entity_type: &str,
    request_id: &str,
) -> Option<String> {
    let receipt_id = format!("rcp-{}", Uuid::new_v4());
    let receipt = Receipt {
        receipt_id: receipt_id.clone(),
        operation: operation.to_string(),
        entity_id: entity_id.to_string(),
        entity_type: entity_type.to_string(),
        request_id: request_id.to_string(),
        status: "completed".to_string(),
        created_at: now_epoch_secs(),
        metadata: serde_json::Value::Object(serde_json::Map::new()),
    };
    match store.save_receipt(&receipt) {
        Ok(()) => Some(receipt_id),
        Err(_) => None,
    }
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

fn read_events_scoped(
    state: &ApiState,
    stack_name: &str,
    scope: &str,
    after: i64,
    limit: usize,
) -> Result<Vec<EventRecord>, StackError> {
    let store = StateStore::open(&state.state_store_path)?;
    store.load_events_by_scope(stack_name, scope, Some(after), limit)
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

    let records = if let Some(ref scope) = query.scope {
        read_events_scoped(&state, &stack_name, scope, after, limit)
    } else {
        read_events(&state, &stack_name, after, limit)
    };

    let records = match records {
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
        let receipt_id = emit_receipt(
            &store,
            "create_sandbox",
            &sandbox.sandbox_id,
            "sandbox",
            &request_id,
        );
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
        let mut resp = (StatusCode::CREATED, Json(response)).into_response();
        if let Some(ref rid) = receipt_id {
            if let Ok(val) = axum::http::HeaderValue::from_str(rid) {
                resp.headers_mut().insert("x-receipt-id", val);
            }
        }
        return resp;
    }

    if let Err(e) = store.save_sandbox(&sandbox) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    let receipt_id = emit_receipt(
        &store,
        "create_sandbox",
        &sandbox.sandbox_id,
        "sandbox",
        &request_id,
    );
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
    let mut resp = (StatusCode::CREATED, Json(response)).into_response();
    if let Some(ref rid) = receipt_id {
        if let Ok(val) = axum::http::HeaderValue::from_str(rid) {
            resp.headers_mut().insert("x-receipt-id", val);
        }
    }
    resp
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

    let receipt_id = emit_receipt(
        &store,
        "terminate_sandbox",
        &sandbox.sandbox_id,
        "sandbox",
        &request_id,
    );
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
    let mut resp = (StatusCode::OK, Json(response)).into_response();
    if let Some(ref rid) = receipt_id {
        if let Ok(val) = axum::http::HeaderValue::from_str(rid) {
            resp.headers_mut().insert("x-receipt-id", val);
        }
    }
    resp
}

// ── Lease handlers ──

async fn open_lease(
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

    if let Some(ref key) = idempotency_key {
        if let Some(cached) =
            check_idempotency(&store, key, "open_lease", &request_hash, &request_id)
        {
            return cached;
        }
    }

    let body: OpenLeaseRequest = match serde_json::from_slice(&raw_body) {
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

    let receipt_id = emit_receipt(&store, "open_lease", &lease.lease_id, "lease", &request_id);
    let response = LeaseResponse {
        request_id,
        lease: lease_to_payload(&lease),
    };
    if let Some(ref key) = idempotency_key {
        if let Ok(json) = serde_json::to_string(&response) {
            save_idempotency(
                &store,
                key,
                "open_lease",
                &request_hash,
                StatusCode::CREATED.as_u16(),
                &json,
            );
        }
    }
    let mut resp = (StatusCode::CREATED, Json(response)).into_response();
    if let Some(ref rid) = receipt_id {
        if let Ok(val) = axum::http::HeaderValue::from_str(rid) {
            resp.headers_mut().insert("x-receipt-id", val);
        }
    }
    resp
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

    let receipt_id = emit_receipt(&store, "close_lease", &lease.lease_id, "lease", &request_id);
    let response = LeaseResponse {
        request_id,
        lease: lease_to_payload(&lease),
    };
    let mut resp = (StatusCode::OK, Json(response)).into_response();
    if let Some(ref rid) = receipt_id {
        if let Ok(val) = axum::http::HeaderValue::from_str(rid) {
            resp.headers_mut().insert("x-receipt-id", val);
        }
    }
    resp
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
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let idempotency_key = extract_idempotency_key(&headers);
    let request_hash = compute_request_hash(&raw_body);

    if let Some(ref key) = idempotency_key {
        if let Some(cached) =
            check_idempotency(&store, key, "create_execution", &request_hash, &request_id)
        {
            return cached;
        }
    }

    let body: CreateExecutionRequest = match serde_json::from_slice(&raw_body) {
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

    let receipt_id = emit_receipt(
        &store,
        "create_execution",
        &execution.execution_id,
        "execution",
        &request_id,
    );
    let response = ExecutionResponse {
        request_id,
        execution: execution_to_payload(&execution),
    };
    if let Some(ref key) = idempotency_key {
        if let Ok(json) = serde_json::to_string(&response) {
            save_idempotency(
                &store,
                key,
                "create_execution",
                &request_hash,
                StatusCode::CREATED.as_u16(),
                &json,
            );
        }
    }
    let mut resp = (StatusCode::CREATED, Json(response)).into_response();
    if let Some(ref rid) = receipt_id {
        if let Ok(val) = axum::http::HeaderValue::from_str(rid) {
            resp.headers_mut().insert("x-receipt-id", val);
        }
    }
    resp
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

    // Emit ExecutionCanceled event for observability.
    let _ = store.emit_event(
        "api",
        &vz_stack::StackEvent::ExecutionCanceled {
            execution_id: execution.execution_id.clone(),
        },
    );

    let receipt_id = emit_receipt(
        &store,
        "cancel_execution",
        &execution.execution_id,
        "execution",
        &request_id,
    );
    let response = ExecutionResponse {
        request_id,
        execution: execution_to_payload(&execution),
    };
    let mut resp = (StatusCode::OK, Json(response)).into_response();
    if let Some(ref rid) = receipt_id {
        if let Ok(val) = axum::http::HeaderValue::from_str(rid) {
            resp.headers_mut().insert("x-receipt-id", val);
        }
    }
    resp
}

async fn resize_exec(
    State(state): State<ApiState>,
    Path(execution_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ResizeExecRequest>,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let execution = match store.load_execution(&execution_id) {
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

    if execution.state != ExecutionState::Running {
        return json_error_response(
            StatusCode::CONFLICT,
            "invalid_state",
            &format!(
                "execution {execution_id} is not running (current state: {})",
                serde_json::to_string(&execution.state)
                    .unwrap_or_default()
                    .trim_matches('"')
            ),
            &request_id,
        );
    }

    if !state.capabilities.live_resize {
        return json_error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_operation",
            "PTY resize is not supported by the current backend",
            &request_id,
        );
    }

    // Emit resize event for observability.
    let _ = store.emit_event(
        "api",
        &vz_stack::StackEvent::ExecutionResized {
            execution_id: execution_id.clone(),
            cols: body.cols,
            rows: body.rows,
        },
    );

    (
        StatusCode::OK,
        Json(ExecutionResponse {
            request_id,
            execution: execution_to_payload(&execution),
        }),
    )
        .into_response()
}

async fn signal_exec(
    State(state): State<ApiState>,
    Path(execution_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<SignalExecRequest>,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let execution = match store.load_execution(&execution_id) {
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
        return json_error_response(
            StatusCode::CONFLICT,
            "invalid_state",
            &format!(
                "execution {execution_id} is in terminal state (current state: {})",
                serde_json::to_string(&execution.state)
                    .unwrap_or_default()
                    .trim_matches('"')
            ),
            &request_id,
        );
    }

    // Emit signal event for observability.
    let _ = store.emit_event(
        "api",
        &vz_stack::StackEvent::ExecutionSignaled {
            execution_id: execution_id.clone(),
            signal: body.signal,
        },
    );

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
    raw_body: axum::body::Bytes,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let idempotency_key = extract_idempotency_key(&headers);
    let request_hash = compute_request_hash(&raw_body);

    if let Some(ref key) = idempotency_key {
        if let Some(cached) =
            check_idempotency(&store, key, "create_checkpoint", &request_hash, &request_id)
        {
            return cached;
        }
    }

    let body: CreateCheckpointRequest = match serde_json::from_slice(&raw_body) {
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

    // Capability gating: reject checkpoint classes not supported by the current backend.
    match class {
        CheckpointClass::VmFull if !state.capabilities.vm_full_checkpoint => {
            return json_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "unsupported_checkpoint_class",
                "VM full checkpoints are not supported by the current backend",
                &request_id,
            );
        }
        CheckpointClass::FsQuick if !state.capabilities.fs_quick_checkpoint => {
            return json_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "unsupported_checkpoint_class",
                "Filesystem quick checkpoints are not supported by the current backend",
                &request_id,
            );
        }
        _ => {}
    }

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

    let receipt_id = emit_receipt(
        &store,
        "create_checkpoint",
        &checkpoint.checkpoint_id,
        "checkpoint",
        &request_id,
    );
    let response = CheckpointResponse {
        request_id,
        checkpoint: checkpoint_to_payload(&checkpoint),
    };
    if let Some(ref key) = idempotency_key {
        if let Ok(json) = serde_json::to_string(&response) {
            save_idempotency(
                &store,
                key,
                "create_checkpoint",
                &request_hash,
                StatusCode::CREATED.as_u16(),
                &json,
            );
        }
    }
    let mut resp = (StatusCode::CREATED, Json(response)).into_response();
    if let Some(ref rid) = receipt_id {
        if let Ok(val) = axum::http::HeaderValue::from_str(rid) {
            resp.headers_mut().insert("x-receipt-id", val);
        }
    }
    resp
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

    // In a real implementation, this would trigger the actual restore operation
    // at the backend layer. Return the checkpoint with fingerprint metadata so
    // callers can perform their own compatibility validation.
    let fingerprint = if checkpoint.compatibility_fingerprint.is_empty()
        || checkpoint.compatibility_fingerprint == "unset"
    {
        None
    } else {
        Some(checkpoint.compatibility_fingerprint.clone())
    };

    (
        StatusCode::OK,
        Json(RestoreCheckpointResponse {
            request_id,
            checkpoint: checkpoint_to_payload(&checkpoint),
            compatibility_fingerprint: fingerprint,
            restore_note: "Backend-level restore is delegated to the runtime; fingerprint validation is the caller's responsibility".to_string(),
        }),
    )
        .into_response()
}

async fn fork_checkpoint(
    State(state): State<ApiState>,
    Path(checkpoint_id): Path<String>,
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

    if let Some(ref key) = idempotency_key {
        if let Some(cached) =
            check_idempotency(&store, key, "fork_checkpoint", &request_hash, &request_id)
        {
            return cached;
        }
    }

    let body: ForkCheckpointRequest = match serde_json::from_slice(&raw_body) {
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

    // Emit CheckpointForked event (best-effort; failure does not block the response).
    let _ = store.emit_event(
        "default",
        &StackEvent::CheckpointForked {
            parent_checkpoint_id: parent.checkpoint_id.clone(),
            new_checkpoint_id: forked.checkpoint_id.clone(),
            new_sandbox_id: forked.sandbox_id.clone(),
        },
    );

    emit_receipt(
        &store,
        "fork_checkpoint",
        &forked.checkpoint_id,
        "checkpoint",
        &request_id,
    );

    let response = CheckpointResponse {
        request_id,
        checkpoint: checkpoint_to_payload(&forked),
    };
    if let Some(ref key) = idempotency_key {
        if let Ok(json) = serde_json::to_string(&response) {
            save_idempotency(
                &store,
                key,
                "fork_checkpoint",
                &request_hash,
                StatusCode::CREATED.as_u16(),
                &json,
            );
        }
    }
    (StatusCode::CREATED, Json(response)).into_response()
}

async fn list_checkpoint_children(
    State(state): State<ApiState>,
    Path(checkpoint_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    // Verify the parent checkpoint exists.
    match store.load_checkpoint(&checkpoint_id) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return json_error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                &format!("checkpoint {checkpoint_id} not found"),
                &request_id,
            );
        }
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    }

    let children = match store.list_checkpoint_children(&checkpoint_id) {
        Ok(list) => list,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    (
        StatusCode::OK,
        Json(CheckpointListResponse {
            request_id,
            checkpoints: children.iter().map(checkpoint_to_payload).collect(),
        }),
    )
        .into_response()
}

// ── Container handlers ──

async fn create_container(
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

    if let Some(ref key) = idempotency_key {
        if let Some(cached) =
            check_idempotency(&store, key, "create_container", &request_hash, &request_id)
        {
            return cached;
        }
    }

    let body: CreateContainerRequest = match serde_json::from_slice(&raw_body) {
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
    let container = Container {
        container_id: format!("ctr-{}", Uuid::new_v4()),
        sandbox_id: body.sandbox_id,
        image_digest: body.image_digest,
        container_spec: ContainerSpec {
            cmd: body.cmd.unwrap_or_default(),
            env: body.env.unwrap_or_default(),
            cwd: body.cwd,
            user: body.user,
            ..ContainerSpec::default()
        },
        state: ContainerState::Created,
        created_at: now,
        started_at: None,
        ended_at: None,
    };

    if let Err(e) = store.save_container(&container) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    let response = ContainerResponse {
        request_id,
        container: container_to_payload(&container),
    };
    if let Some(ref key) = idempotency_key {
        if let Ok(json) = serde_json::to_string(&response) {
            save_idempotency(
                &store,
                key,
                "create_container",
                &request_hash,
                StatusCode::CREATED.as_u16(),
                &json,
            );
        }
    }
    (StatusCode::CREATED, Json(response)).into_response()
}

async fn list_containers(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let containers = match store.list_containers() {
        Ok(list) => list,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    (
        StatusCode::OK,
        Json(ContainerListResponse {
            request_id,
            containers: containers.iter().map(container_to_payload).collect(),
        }),
    )
        .into_response()
}

async fn get_container(
    State(state): State<ApiState>,
    Path(container_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    match store.load_container(&container_id) {
        Ok(Some(container)) => (
            StatusCode::OK,
            Json(ContainerResponse {
                request_id,
                container: container_to_payload(&container),
            }),
        )
            .into_response(),
        Ok(None) => json_error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            &format!("container {container_id} not found"),
            &request_id,
        ),
        Err(e) => stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    }
}

async fn remove_container(
    State(state): State<ApiState>,
    Path(container_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let mut container = match store.load_container(&container_id) {
        Ok(Some(c)) => c,
        Ok(None) => {
            return json_error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                &format!("container {container_id} not found"),
                &request_id,
            );
        }
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    if container.state == ContainerState::Removed {
        return (
            StatusCode::OK,
            Json(ContainerResponse {
                request_id,
                container: container_to_payload(&container),
            }),
        )
            .into_response();
    }

    // Transition to Removed, going through intermediary states if needed.
    let _ = container.transition_to(ContainerState::Removed);

    if let Err(e) = store.save_container(&container) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    (
        StatusCode::OK,
        Json(ContainerResponse {
            request_id,
            container: container_to_payload(&container),
        }),
    )
        .into_response()
}

// ── Image handlers ──

async fn list_images(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let images = match store.list_images() {
        Ok(list) => list,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    (
        StatusCode::OK,
        Json(ImageListResponse {
            request_id,
            images: images.iter().map(image_to_payload).collect(),
        }),
    )
        .into_response()
}

async fn get_image(
    State(state): State<ApiState>,
    Path(image_ref): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    // axum's Path extractor already percent-decodes the parameter.
    match store.load_image(&image_ref) {
        Ok(Some(image)) => (
            StatusCode::OK,
            Json(ImageResponse {
                request_id,
                image: image_to_payload(&image),
            }),
        )
            .into_response(),
        Ok(None) => json_error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            &format!("image {image_ref} not found"),
            &request_id,
        ),
        Err(e) => stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    }
}

// ── Build handlers ──

async fn start_build(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<StartBuildRequest>,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let now = now_epoch_secs();
    let build = Build {
        build_id: format!("bld-{}", Uuid::new_v4()),
        sandbox_id: body.sandbox_id,
        build_spec: BuildSpec {
            context: body.context,
            dockerfile: body.dockerfile,
            args: body.args.unwrap_or_default(),
        },
        state: BuildState::Queued,
        result_digest: None,
        started_at: now,
        ended_at: None,
    };

    if let Err(e) = store.save_build(&build) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    (
        StatusCode::CREATED,
        Json(BuildResponse {
            request_id,
            build: build_to_payload(&build),
        }),
    )
        .into_response()
}

async fn list_builds(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let builds = match store.list_builds() {
        Ok(list) => list,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    (
        StatusCode::OK,
        Json(BuildListResponse {
            request_id,
            builds: builds.iter().map(build_to_payload).collect(),
        }),
    )
        .into_response()
}

async fn get_build(
    State(state): State<ApiState>,
    Path(build_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    match store.load_build(&build_id) {
        Ok(Some(build)) => (
            StatusCode::OK,
            Json(BuildResponse {
                request_id,
                build: build_to_payload(&build),
            }),
        )
            .into_response(),
        Ok(None) => json_error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            &format!("build {build_id} not found"),
            &request_id,
        ),
        Err(e) => stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    }
}

async fn cancel_build(
    State(state): State<ApiState>,
    Path(build_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    let mut build = match store.load_build(&build_id) {
        Ok(Some(b)) => b,
        Ok(None) => {
            return json_error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                &format!("build {build_id} not found"),
                &request_id,
            );
        }
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    if build.state.is_terminal() {
        return (
            StatusCode::OK,
            Json(BuildResponse {
                request_id,
                build: build_to_payload(&build),
            }),
        )
            .into_response();
    }

    let now = now_epoch_secs();
    build.ended_at = Some(now);
    let _ = build.transition_to(BuildState::Canceled);

    if let Err(e) = store.save_build(&build) {
        return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id);
    }

    (
        StatusCode::OK,
        Json(BuildResponse {
            request_id,
            build: build_to_payload(&build),
        }),
    )
        .into_response()
}

// ── Receipt handler ──

async fn get_receipt(
    State(state): State<ApiState>,
    Path(receipt_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id_from_headers(&headers);
    let store = match StateStore::open(&state.state_store_path) {
        Ok(s) => s,
        Err(e) => return stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    };

    match store.load_receipt(&receipt_id) {
        Ok(Some(receipt)) => (
            StatusCode::OK,
            Json(ReceiptResponse {
                request_id,
                receipt: receipt_to_payload(&receipt),
            }),
        )
            .into_response(),
        Ok(None) => json_error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            &format!("receipt {receipt_id} not found"),
            &request_id,
        ),
        Err(e) => stack_error_response(StatusCode::INTERNAL_SERVER_ERROR, e, request_id),
    }
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
        .route("/v1/executions/{execution_id}/resize", post(resize_exec))
        .route("/v1/executions/{execution_id}/signal", post(signal_exec))
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
        .route(
            "/v1/checkpoints/{checkpoint_id}/children",
            get(list_checkpoint_children),
        )
        .route(
            "/v1/containers",
            get(list_containers).post(create_container),
        )
        .route(
            "/v1/containers/{container_id}",
            get(get_container).delete(remove_container),
        )
        .route("/v1/images", get(list_images))
        .route("/v1/images/{image_ref}", get(get_image))
        .route("/v1/builds", get(list_builds).post(start_build))
        .route("/v1/builds/{build_id}", get(get_build).delete(cancel_build))
        .route("/v1/receipts/{receipt_id}", get(get_receipt))
        .with_state(state)
}

/// Static OpenAPI 3.1 surface for Runtime V2 external transport.
pub fn openapi_document() -> serde_json::Value {
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "vz Runtime V2 API",
            "version": "2.0.0-alpha",
            "description": "Container runtime API with sandbox lifecycle, lease management, execution dispatch, checkpoint/restore, and real-time event streaming via SSE and WebSocket."
        },
        "paths": {
            "/openapi.json": {
                "get": {
                    "operationId": "getOpenApiDocument",
                    "summary": "Return this OpenAPI 3.1 schema document",
                    "responses": {
                        "200": {
                            "description": "OpenAPI 3.1 JSON document",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/capabilities": {
                "get": {
                    "operationId": "getCapabilities",
                    "summary": "List runtime capabilities advertised by this API surface",
                    "responses": {
                        "200": {
                            "description": "Capabilities list",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/CapabilitiesResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/events/{stack_name}": {
                "get": {
                    "operationId": "listEvents",
                    "summary": "Paginated event log for a stack",
                    "parameters": [
                        { "$ref": "#/components/parameters/StackName" },
                        {
                            "name": "after",
                            "in": "query",
                            "description": "Return events with id strictly greater than this cursor",
                            "required": false,
                            "schema": { "type": "integer", "format": "int64", "default": 0 }
                        },
                        {
                            "name": "limit",
                            "in": "query",
                            "description": "Maximum number of events to return (1..1000)",
                            "required": false,
                            "schema": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 100 }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Paginated event list",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/EventsResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/events/{stack_name}/stream": {
                "get": {
                    "operationId": "streamEventsSse",
                    "summary": "Server-Sent Events stream of stack events",
                    "parameters": [
                        { "$ref": "#/components/parameters/StackName" },
                        {
                            "name": "after",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "integer", "format": "int64", "default": 0 }
                        },
                        {
                            "name": "limit",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 100 }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "SSE event stream",
                            "content": {
                                "text/event-stream": {
                                    "schema": { "type": "string" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/events/{stack_name}/ws": {
                "get": {
                    "operationId": "streamEventsWs",
                    "summary": "WebSocket stream of stack events",
                    "parameters": [
                        { "$ref": "#/components/parameters/StackName" },
                        {
                            "name": "after",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "integer", "format": "int64", "default": 0 }
                        },
                        {
                            "name": "limit",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 100 }
                        }
                    ],
                    "responses": {
                        "101": {
                            "description": "WebSocket upgrade"
                        }
                    }
                }
            },
            "/v1/sandboxes": {
                "post": {
                    "operationId": "createSandbox",
                    "summary": "Create a new sandbox",
                    "parameters": [
                        { "$ref": "#/components/parameters/IdempotencyKey" },
                        { "$ref": "#/components/parameters/RequestId" }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/CreateSandboxRequest" }
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Sandbox created",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/SandboxResponse" }
                                }
                            }
                        },
                        "400": {
                            "description": "Invalid request body",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "409": {
                            "description": "Idempotency conflict",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                },
                "get": {
                    "operationId": "listSandboxes",
                    "summary": "List all sandboxes",
                    "responses": {
                        "200": {
                            "description": "Sandbox list",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/SandboxListResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/sandboxes/{sandbox_id}": {
                "get": {
                    "operationId": "getSandbox",
                    "summary": "Get a sandbox by ID",
                    "parameters": [
                        { "$ref": "#/components/parameters/SandboxId" }
                    ],
                    "responses": {
                        "200": {
                            "description": "Sandbox details",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/SandboxResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Sandbox not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                },
                "delete": {
                    "operationId": "terminateSandbox",
                    "summary": "Terminate a sandbox",
                    "parameters": [
                        { "$ref": "#/components/parameters/SandboxId" },
                        { "$ref": "#/components/parameters/IdempotencyKey" }
                    ],
                    "responses": {
                        "200": {
                            "description": "Sandbox terminated",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/SandboxResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Sandbox not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "409": {
                            "description": "Idempotency conflict",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/leases": {
                "post": {
                    "operationId": "openLease",
                    "summary": "Open a new lease on a sandbox",
                    "parameters": [
                        { "$ref": "#/components/parameters/IdempotencyKey" },
                        { "$ref": "#/components/parameters/RequestId" }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/OpenLeaseRequest" }
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Lease opened",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/LeaseResponse" }
                                }
                            }
                        },
                        "422": {
                            "description": "State transition failed",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                },
                "get": {
                    "operationId": "listLeases",
                    "summary": "List all leases",
                    "responses": {
                        "200": {
                            "description": "Lease list",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/LeaseListResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/leases/{lease_id}": {
                "get": {
                    "operationId": "getLease",
                    "summary": "Get a lease by ID",
                    "parameters": [
                        { "$ref": "#/components/parameters/LeaseId" }
                    ],
                    "responses": {
                        "200": {
                            "description": "Lease details",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/LeaseResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Lease not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                },
                "delete": {
                    "operationId": "closeLease",
                    "summary": "Close a lease",
                    "parameters": [
                        { "$ref": "#/components/parameters/LeaseId" },
                        { "$ref": "#/components/parameters/IdempotencyKey" }
                    ],
                    "responses": {
                        "200": {
                            "description": "Lease closed",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/LeaseResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Lease not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "422": {
                            "description": "State transition failed",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/leases/{lease_id}/heartbeat": {
                "post": {
                    "operationId": "heartbeatLease",
                    "summary": "Send a heartbeat to keep a lease active",
                    "parameters": [
                        { "$ref": "#/components/parameters/LeaseId" },
                        { "$ref": "#/components/parameters/IdempotencyKey" }
                    ],
                    "responses": {
                        "200": {
                            "description": "Heartbeat acknowledged",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/LeaseResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Lease not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "422": {
                            "description": "Lease is not active",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/executions": {
                "post": {
                    "operationId": "createExecution",
                    "summary": "Create a new execution in a container",
                    "parameters": [
                        { "$ref": "#/components/parameters/IdempotencyKey" },
                        { "$ref": "#/components/parameters/RequestId" }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/CreateExecutionRequest" }
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Execution created",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ExecutionResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                },
                "get": {
                    "operationId": "listExecutions",
                    "summary": "List all executions",
                    "responses": {
                        "200": {
                            "description": "Execution list",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ExecutionListResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/executions/{execution_id}": {
                "get": {
                    "operationId": "getExecution",
                    "summary": "Get an execution by ID",
                    "parameters": [
                        { "$ref": "#/components/parameters/ExecutionId" }
                    ],
                    "responses": {
                        "200": {
                            "description": "Execution details",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ExecutionResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Execution not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                },
                "delete": {
                    "operationId": "cancelExecution",
                    "summary": "Cancel a running execution",
                    "parameters": [
                        { "$ref": "#/components/parameters/ExecutionId" },
                        { "$ref": "#/components/parameters/IdempotencyKey" }
                    ],
                    "responses": {
                        "200": {
                            "description": "Execution canceled",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ExecutionResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Execution not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/executions/{execution_id}/resize": {
                "post": {
                    "operationId": "resizeExec",
                    "summary": "Resize the PTY of a running execution",
                    "parameters": [
                        { "$ref": "#/components/parameters/ExecutionId" }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/ResizeExecRequest" }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Resize acknowledged",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ExecutionResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Execution not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "409": {
                            "description": "Execution is not running",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "422": {
                            "description": "PTY resize unsupported by backend",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/executions/{execution_id}/signal": {
                "post": {
                    "operationId": "signalExec",
                    "summary": "Send a signal to a running execution",
                    "parameters": [
                        { "$ref": "#/components/parameters/ExecutionId" }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/SignalExecRequest" }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Signal acknowledged",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ExecutionResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Execution not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "409": {
                            "description": "Execution is in terminal state",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/checkpoints": {
                "post": {
                    "operationId": "createCheckpoint",
                    "summary": "Create a checkpoint of a sandbox (capability-gated: requires fs_quick_checkpoint or vm_full_checkpoint)",
                    "parameters": [
                        { "$ref": "#/components/parameters/IdempotencyKey" },
                        { "$ref": "#/components/parameters/RequestId" }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/CreateCheckpointRequest" }
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Checkpoint created",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/CheckpointResponse" }
                                }
                            }
                        },
                        "400": {
                            "description": "Invalid checkpoint class",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "422": {
                            "description": "Unsupported checkpoint class — the requested class is not enabled by the current backend capabilities (error code: unsupported_checkpoint_class)",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                },
                "get": {
                    "operationId": "listCheckpoints",
                    "summary": "List all checkpoints",
                    "responses": {
                        "200": {
                            "description": "Checkpoint list",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/CheckpointListResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/checkpoints/{checkpoint_id}": {
                "get": {
                    "operationId": "getCheckpoint",
                    "summary": "Get a checkpoint by ID",
                    "parameters": [
                        { "$ref": "#/components/parameters/CheckpointId" }
                    ],
                    "responses": {
                        "200": {
                            "description": "Checkpoint details",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/CheckpointResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Checkpoint not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/checkpoints/{checkpoint_id}/restore": {
                "post": {
                    "operationId": "restoreCheckpoint",
                    "summary": "Restore a sandbox from a checkpoint",
                    "parameters": [
                        { "$ref": "#/components/parameters/CheckpointId" },
                        { "$ref": "#/components/parameters/IdempotencyKey" }
                    ],
                    "responses": {
                        "200": {
                            "description": "Checkpoint restore initiated",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/CheckpointResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Checkpoint not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "409": {
                            "description": "Checkpoint not in ready state",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/checkpoints/{checkpoint_id}/fork": {
                "post": {
                    "operationId": "forkCheckpoint",
                    "summary": "Fork a checkpoint into a new sandbox",
                    "parameters": [
                        { "$ref": "#/components/parameters/CheckpointId" },
                        { "$ref": "#/components/parameters/IdempotencyKey" }
                    ],
                    "requestBody": {
                        "required": false,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/ForkCheckpointRequest" }
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Forked checkpoint created",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/CheckpointResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Parent checkpoint not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "409": {
                            "description": "Parent checkpoint not in ready state",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/checkpoints/{checkpoint_id}/children": {
                "get": {
                    "operationId": "listCheckpointChildren",
                    "summary": "List child checkpoints forked from a parent checkpoint",
                    "parameters": [
                        { "$ref": "#/components/parameters/CheckpointId" }
                    ],
                    "responses": {
                        "200": {
                            "description": "List of child checkpoints",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/CheckpointListResponse" }
                                }
                            }
                        },
                        "404": {
                            "description": "Parent checkpoint not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/images": {
                "get": {
                    "operationId": "listImages",
                    "summary": "List all cached images",
                    "responses": {
                        "200": {
                            "description": "List of images",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/images/{image_ref}": {
                "get": {
                    "operationId": "getImage",
                    "summary": "Get image details by reference",
                    "parameters": [
                        { "name": "image_ref", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": {
                        "200": {
                            "description": "Image details",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object" }
                                }
                            }
                        },
                        "404": {
                            "description": "Image not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/builds": {
                "get": {
                    "operationId": "listBuilds",
                    "summary": "List all builds",
                    "responses": {
                        "200": {
                            "description": "List of builds",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object" }
                                }
                            }
                        }
                    }
                },
                "post": {
                    "operationId": "startBuild",
                    "summary": "Start a new build",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "type": "object" }
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Build started",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/builds/{build_id}": {
                "get": {
                    "operationId": "getBuild",
                    "summary": "Get build details",
                    "parameters": [
                        { "name": "build_id", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": {
                        "200": {
                            "description": "Build details",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object" }
                                }
                            }
                        },
                        "404": {
                            "description": "Build not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                },
                "delete": {
                    "operationId": "cancelBuild",
                    "summary": "Cancel a running build",
                    "parameters": [
                        { "name": "build_id", "in": "path", "required": true, "schema": { "type": "string" } },
                        { "$ref": "#/components/parameters/IdempotencyKey" }
                    ],
                    "responses": {
                        "200": {
                            "description": "Build canceled",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object" }
                                }
                            }
                        },
                        "404": {
                            "description": "Build not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal error",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/containers": {
                "get": {
                    "operationId": "listContainers",
                    "summary": "List all containers",
                    "responses": {
                        "200": {
                            "description": "List of containers",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object" }
                                }
                            }
                        }
                    }
                },
                "post": {
                    "operationId": "createContainer",
                    "summary": "Create a new container",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "type": "object" }
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Container created",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/containers/{container_id}": {
                "get": {
                    "operationId": "getContainer",
                    "summary": "Get container details",
                    "parameters": [
                        { "name": "container_id", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": {
                        "200": {
                            "description": "Container details",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object" }
                                }
                            }
                        },
                        "404": {
                            "description": "Container not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                },
                "delete": {
                    "operationId": "removeContainer",
                    "summary": "Remove a container",
                    "parameters": [
                        { "name": "container_id", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": {
                        "204": {
                            "description": "Container removed"
                        },
                        "404": {
                            "description": "Container not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/receipts/{receipt_id}": {
                "get": {
                    "operationId": "getReceipt",
                    "summary": "Get receipt details",
                    "parameters": [
                        { "name": "receipt_id", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": {
                        "200": {
                            "description": "Receipt details",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object" }
                                }
                            }
                        },
                        "404": {
                            "description": "Receipt not found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                                }
                            }
                        }
                    }
                }
            }
        },
        "components": {
            "parameters": {
                "StackName": {
                    "name": "stack_name",
                    "in": "path",
                    "required": true,
                    "description": "Stack identifier for event filtering",
                    "schema": { "type": "string" }
                },
                "SandboxId": {
                    "name": "sandbox_id",
                    "in": "path",
                    "required": true,
                    "description": "Unique sandbox identifier (sbx-...)",
                    "schema": { "type": "string" }
                },
                "LeaseId": {
                    "name": "lease_id",
                    "in": "path",
                    "required": true,
                    "description": "Unique lease identifier (ls-...)",
                    "schema": { "type": "string" }
                },
                "ExecutionId": {
                    "name": "execution_id",
                    "in": "path",
                    "required": true,
                    "description": "Unique execution identifier (exec-...)",
                    "schema": { "type": "string" }
                },
                "CheckpointId": {
                    "name": "checkpoint_id",
                    "in": "path",
                    "required": true,
                    "description": "Unique checkpoint identifier (ckpt-...)",
                    "schema": { "type": "string" }
                },
                "IdempotencyKey": {
                    "name": "Idempotency-Key",
                    "in": "header",
                    "required": false,
                    "description": "Client-supplied idempotency key. Repeated requests with the same key and body return the cached response. Same key with a different body returns 409 Conflict.",
                    "schema": { "type": "string" }
                },
                "RequestId": {
                    "name": "X-Request-Id",
                    "in": "header",
                    "required": false,
                    "description": "Client-supplied request identifier echoed back in every response. Auto-generated when absent.",
                    "schema": { "type": "string" }
                }
            },
            "schemas": {
                "ErrorResponse": {
                    "type": "object",
                    "required": ["error"],
                    "properties": {
                        "error": {
                            "type": "object",
                            "required": ["code", "message"],
                            "properties": {
                                "code": { "type": "string", "description": "Machine-readable error code" },
                                "message": { "type": "string", "description": "Human-readable error description" },
                                "request_id": { "type": "string", "description": "Request identifier for tracing" }
                            }
                        }
                    }
                },
                "CapabilitiesResponse": {
                    "type": "object",
                    "required": ["request_id", "capabilities"],
                    "properties": {
                        "request_id": { "type": "string" },
                        "capabilities": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "List of capability identifiers supported by this runtime"
                        }
                    }
                },
                "EventRecord": {
                    "type": "object",
                    "required": ["id", "stack_name", "created_at", "event"],
                    "properties": {
                        "id": { "type": "integer", "format": "int64", "description": "Monotonic cursor value" },
                        "stack_name": { "type": "string" },
                        "created_at": { "type": "string", "description": "SQLite event timestamp" },
                        "event": { "type": "object", "description": "Serialized stack event payload" }
                    }
                },
                "EventsResponse": {
                    "type": "object",
                    "required": ["request_id", "events", "next_cursor"],
                    "properties": {
                        "request_id": { "type": "string" },
                        "events": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/EventRecord" }
                        },
                        "next_cursor": { "type": "integer", "format": "int64", "description": "Cursor for the next page; use as ?after= value" }
                    }
                },
                "SandboxPayload": {
                    "type": "object",
                    "required": ["sandbox_id", "backend", "state", "created_at", "updated_at", "labels"],
                    "properties": {
                        "sandbox_id": { "type": "string", "description": "Unique sandbox identifier (sbx-...)" },
                        "backend": { "type": "string", "enum": ["macos_vz", "linux_native"], "description": "Runtime backend" },
                        "state": { "type": "string", "enum": ["creating", "ready", "draining", "terminated", "failed"], "description": "Current lifecycle state" },
                        "cpus": { "type": "integer", "nullable": true, "description": "Allocated vCPU count" },
                        "memory_mb": { "type": "integer", "nullable": true, "description": "Allocated memory in MiB" },
                        "created_at": { "type": "integer", "format": "uint64", "description": "Unix epoch seconds" },
                        "updated_at": { "type": "integer", "format": "uint64", "description": "Unix epoch seconds" },
                        "labels": {
                            "type": "object",
                            "additionalProperties": { "type": "string" },
                            "description": "Arbitrary key-value labels"
                        }
                    }
                },
                "CreateSandboxRequest": {
                    "type": "object",
                    "properties": {
                        "stack_name": { "type": "string", "nullable": true, "description": "Optional stack to associate" },
                        "cpus": { "type": "integer", "nullable": true, "description": "vCPU count" },
                        "memory_mb": { "type": "integer", "nullable": true, "description": "Memory in MiB" },
                        "labels": {
                            "type": "object",
                            "additionalProperties": { "type": "string" },
                            "default": {}
                        }
                    }
                },
                "SandboxResponse": {
                    "type": "object",
                    "required": ["request_id", "sandbox"],
                    "properties": {
                        "request_id": { "type": "string" },
                        "sandbox": { "$ref": "#/components/schemas/SandboxPayload" }
                    }
                },
                "SandboxListResponse": {
                    "type": "object",
                    "required": ["request_id", "sandboxes"],
                    "properties": {
                        "request_id": { "type": "string" },
                        "sandboxes": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/SandboxPayload" }
                        }
                    }
                },
                "LeasePayload": {
                    "type": "object",
                    "required": ["lease_id", "sandbox_id", "ttl_secs", "last_heartbeat_at", "state"],
                    "properties": {
                        "lease_id": { "type": "string", "description": "Unique lease identifier (ls-...)" },
                        "sandbox_id": { "type": "string", "description": "Sandbox this lease belongs to" },
                        "ttl_secs": { "type": "integer", "format": "uint64", "description": "Time-to-live in seconds" },
                        "last_heartbeat_at": { "type": "integer", "format": "uint64", "description": "Unix epoch seconds of last heartbeat" },
                        "state": { "type": "string", "enum": ["opening", "active", "closed", "expired"], "description": "Current lease state" }
                    }
                },
                "OpenLeaseRequest": {
                    "type": "object",
                    "required": ["sandbox_id"],
                    "properties": {
                        "sandbox_id": { "type": "string", "description": "Sandbox to lease" },
                        "ttl_secs": { "type": "integer", "nullable": true, "description": "Lease TTL in seconds (default 300)" }
                    }
                },
                "LeaseResponse": {
                    "type": "object",
                    "required": ["request_id", "lease"],
                    "properties": {
                        "request_id": { "type": "string" },
                        "lease": { "$ref": "#/components/schemas/LeasePayload" }
                    }
                },
                "LeaseListResponse": {
                    "type": "object",
                    "required": ["request_id", "leases"],
                    "properties": {
                        "request_id": { "type": "string" },
                        "leases": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/LeasePayload" }
                        }
                    }
                },
                "ExecutionPayload": {
                    "type": "object",
                    "required": ["execution_id", "container_id", "state"],
                    "properties": {
                        "execution_id": { "type": "string", "description": "Unique execution identifier (exec-...)" },
                        "container_id": { "type": "string", "description": "Container that owns this execution" },
                        "state": { "type": "string", "enum": ["queued", "running", "completed", "failed", "canceled"], "description": "Current execution state" },
                        "exit_code": { "type": "integer", "nullable": true, "description": "Process exit code (set on completion)" },
                        "started_at": { "type": "integer", "format": "uint64", "nullable": true, "description": "Unix epoch seconds" },
                        "ended_at": { "type": "integer", "format": "uint64", "nullable": true, "description": "Unix epoch seconds" }
                    }
                },
                "CreateExecutionRequest": {
                    "type": "object",
                    "required": ["container_id", "cmd"],
                    "properties": {
                        "container_id": { "type": "string", "description": "Target container" },
                        "cmd": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Command to execute"
                        },
                        "args": {
                            "type": "array",
                            "items": { "type": "string" },
                            "nullable": true,
                            "description": "Additional arguments"
                        },
                        "env_override": {
                            "type": "object",
                            "additionalProperties": { "type": "string" },
                            "nullable": true,
                            "description": "Environment variable overrides"
                        },
                        "pty": { "type": "boolean", "nullable": true, "description": "Allocate a pseudo-TTY" },
                        "timeout_secs": { "type": "integer", "nullable": true, "description": "Execution timeout in seconds" }
                    }
                },
                "ResizeExecRequest": {
                    "type": "object",
                    "required": ["cols", "rows"],
                    "properties": {
                        "cols": { "type": "integer", "format": "uint16", "description": "Number of columns" },
                        "rows": { "type": "integer", "format": "uint16", "description": "Number of rows" }
                    }
                },
                "SignalExecRequest": {
                    "type": "object",
                    "required": ["signal"],
                    "properties": {
                        "signal": { "type": "string", "description": "Signal name (e.g. SIGTERM) or number (e.g. 9)" }
                    }
                },
                "ExecutionResponse": {
                    "type": "object",
                    "required": ["request_id", "execution"],
                    "properties": {
                        "request_id": { "type": "string" },
                        "execution": { "$ref": "#/components/schemas/ExecutionPayload" }
                    }
                },
                "ExecutionListResponse": {
                    "type": "object",
                    "required": ["request_id", "executions"],
                    "properties": {
                        "request_id": { "type": "string" },
                        "executions": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/ExecutionPayload" }
                        }
                    }
                },
                "CheckpointPayload": {
                    "type": "object",
                    "required": ["checkpoint_id", "sandbox_id", "class", "state", "compatibility_fingerprint", "created_at"],
                    "properties": {
                        "checkpoint_id": { "type": "string", "description": "Unique checkpoint identifier (ckpt-...)" },
                        "sandbox_id": { "type": "string", "description": "Sandbox this checkpoint belongs to" },
                        "parent_checkpoint_id": { "type": "string", "nullable": true, "description": "Parent checkpoint if forked" },
                        "class": { "type": "string", "enum": ["fs_quick", "vm_full"], "description": "Checkpoint class" },
                        "state": { "type": "string", "enum": ["creating", "ready", "restoring", "deleted"], "description": "Current checkpoint state" },
                        "compatibility_fingerprint": { "type": "string", "description": "Opaque fingerprint for restore compatibility checks" },
                        "created_at": { "type": "integer", "format": "uint64", "description": "Unix epoch seconds" }
                    }
                },
                "CreateCheckpointRequest": {
                    "type": "object",
                    "required": ["sandbox_id"],
                    "properties": {
                        "sandbox_id": { "type": "string", "description": "Sandbox to checkpoint" },
                        "class": { "type": "string", "enum": ["fs_quick", "vm_full"], "nullable": true, "description": "Checkpoint class (default fs_quick)" },
                        "compatibility_fingerprint": { "type": "string", "nullable": true, "description": "Opaque fingerprint for restore compatibility" }
                    }
                },
                "ForkCheckpointRequest": {
                    "type": "object",
                    "properties": {
                        "new_sandbox_id": { "type": "string", "nullable": true, "description": "Sandbox ID for the fork target (auto-generated if omitted)" }
                    }
                },
                "CheckpointResponse": {
                    "type": "object",
                    "required": ["request_id", "checkpoint"],
                    "properties": {
                        "request_id": { "type": "string" },
                        "checkpoint": { "$ref": "#/components/schemas/CheckpointPayload" }
                    }
                },
                "CheckpointListResponse": {
                    "type": "object",
                    "required": ["request_id", "checkpoints"],
                    "properties": {
                        "request_id": { "type": "string" },
                        "checkpoints": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/CheckpointPayload" }
                        }
                    }
                }
            }
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
            "/v1/containers/{container_id}" => "/v1/containers/ctr-nonexistent".to_string(),
            "/v1/images/{image_ref}" => "/v1/images/nginx:latest".to_string(),
            "/v1/receipts/{receipt_id}" => "/v1/receipts/rcp-nonexistent".to_string(),
            "/v1/builds/{build_id}" => "/v1/builds/bld-nonexistent".to_string(),
            _ => path.to_string(),
        }
    }

    #[test]
    fn openapi_document_contains_required_paths() {
        let document = openapi_document();
        let paths = document["paths"].as_object().unwrap();
        assert!(paths.contains_key("/v1/sandboxes"));
        assert!(paths.contains_key("/v1/sandboxes/{sandbox_id}"));
        assert!(paths.contains_key("/v1/leases"));
        assert!(paths.contains_key("/v1/leases/{lease_id}"));
        assert!(paths.contains_key("/v1/images"));
        assert!(paths.contains_key("/v1/images/{image_ref}"));
        assert!(paths.contains_key("/v1/builds"));
        assert!(paths.contains_key("/v1/builds/{build_id}"));
        assert!(paths.contains_key("/v1/containers"));
        assert!(paths.contains_key("/v1/containers/{container_id}"));
        assert!(paths.contains_key("/v1/executions"));
        assert!(paths.contains_key("/v1/executions/{execution_id}"));
        assert!(paths.contains_key("/v1/executions/{execution_id}/resize"));
        assert!(paths.contains_key("/v1/executions/{execution_id}/signal"));
        assert!(paths.contains_key("/v1/checkpoints"));
        assert!(paths.contains_key("/v1/checkpoints/{checkpoint_id}"));
        assert!(paths.contains_key("/v1/checkpoints/{checkpoint_id}/children"));
        assert!(paths.contains_key("/v1/events/{stack_name}"));
        assert!(paths.contains_key("/v1/events/{stack_name}/stream"));
        assert!(paths.contains_key("/v1/events/{stack_name}/ws"));
        assert!(paths.contains_key("/v1/receipts/{receipt_id}"));
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

        // Containers are now implemented, so GET /v1/containers returns 200.
        let app = router(test_config(state_path.clone()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/containers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // GET for a non-existent container returns 404 with proper error envelope.
        let app = router(test_config(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/containers/nonexistent-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
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
                        | "/v1/containers"
                        | "/v1/images"
                        | "/v1/builds"
                )
            {
                continue;
            }

            // 404 is valid for parameterized GET endpoints where no entity exists.
            if status == StatusCode::NOT_FOUND
                && matches!(
                    surface.path,
                    "/v1/receipts/{receipt_id}"
                        | "/v1/containers/{container_id}"
                        | "/v1/images/{image_ref}"
                        | "/v1/builds/{build_id}"
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

    fn test_config_with_resize(state_store_path: PathBuf) -> ApiConfig {
        ApiConfig {
            state_store_path,
            capabilities: RuntimeCapabilities {
                fs_quick_checkpoint: true,
                checkpoint_fork: true,
                live_resize: true,
                ..RuntimeCapabilities::default()
            },
            event_poll_interval: Duration::from_millis(10),
            default_event_page_size: 2,
        }
    }

    #[tokio::test]
    async fn resize_on_running_execution_succeeds() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        // Create a Running execution directly.
        let execution = Execution {
            execution_id: "exec-resize-1".to_string(),
            container_id: "ctr-1".to_string(),
            exec_spec: ExecutionSpec {
                cmd: vec!["bash".to_string()],
                args: vec![],
                env_override: BTreeMap::new(),
                pty: true,
                timeout_secs: None,
            },
            state: ExecutionState::Running,
            exit_code: None,
            started_at: Some(now_epoch_secs()),
            ended_at: None,
        };
        store.save_execution(&execution).unwrap();

        let app = router(test_config_with_resize(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/executions/exec-resize-1/resize")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"cols":120,"rows":40}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["execution"]["execution_id"].as_str().unwrap(),
            "exec-resize-1"
        );
        assert_eq!(payload["execution"]["state"].as_str().unwrap(), "running");
    }

    #[tokio::test]
    async fn resize_on_non_running_execution_returns_409() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        // Create a Queued execution.
        let execution = Execution {
            execution_id: "exec-resize-q".to_string(),
            container_id: "ctr-1".to_string(),
            exec_spec: ExecutionSpec {
                cmd: vec!["bash".to_string()],
                args: vec![],
                env_override: BTreeMap::new(),
                pty: true,
                timeout_secs: None,
            },
            state: ExecutionState::Queued,
            exit_code: None,
            started_at: None,
            ended_at: None,
        };
        store.save_execution(&execution).unwrap();

        let app = router(test_config_with_resize(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/executions/exec-resize-q/resize")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"cols":80,"rows":24}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn signal_on_running_execution_succeeds() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        // Create a Running execution.
        let execution = Execution {
            execution_id: "exec-sig-1".to_string(),
            container_id: "ctr-1".to_string(),
            exec_spec: ExecutionSpec {
                cmd: vec!["bash".to_string()],
                args: vec![],
                env_override: BTreeMap::new(),
                pty: false,
                timeout_secs: None,
            },
            state: ExecutionState::Running,
            exit_code: None,
            started_at: Some(now_epoch_secs()),
            ended_at: None,
        };
        store.save_execution(&execution).unwrap();

        let app = router(test_config(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/executions/exec-sig-1/signal")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"signal":"SIGTERM"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["execution"]["execution_id"].as_str().unwrap(),
            "exec-sig-1"
        );
    }

    #[tokio::test]
    async fn cancel_queued_execution_transitions_to_canceled() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        // Create a Queued execution.
        let execution = Execution {
            execution_id: "exec-cancel-q".to_string(),
            container_id: "ctr-1".to_string(),
            exec_spec: ExecutionSpec {
                cmd: vec!["echo".to_string()],
                args: vec![],
                env_override: BTreeMap::new(),
                pty: false,
                timeout_secs: None,
            },
            state: ExecutionState::Queued,
            exit_code: None,
            started_at: None,
            ended_at: None,
        };
        store.save_execution(&execution).unwrap();

        let app = router(test_config(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/executions/exec-cancel-q")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["execution"]["state"].as_str().unwrap(), "canceled");
        assert!(payload["execution"]["ended_at"].as_u64().is_some());
    }

    #[tokio::test]
    async fn cancel_terminal_execution_is_idempotent() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        // Create a Completed (terminal) execution.
        let mut execution = Execution {
            execution_id: "exec-done".to_string(),
            container_id: "ctr-1".to_string(),
            exec_spec: ExecutionSpec {
                cmd: vec!["echo".to_string()],
                args: vec![],
                env_override: BTreeMap::new(),
                pty: false,
                timeout_secs: None,
            },
            state: ExecutionState::Running,
            exit_code: None,
            started_at: Some(now_epoch_secs()),
            ended_at: None,
        };
        let _ = execution.transition_to(ExecutionState::Exited);
        execution.exit_code = Some(0);
        execution.ended_at = Some(now_epoch_secs());
        store.save_execution(&execution).unwrap();

        let app = router(test_config(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/executions/exec-done")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // Should remain in its terminal state, not transition again.
        assert_eq!(payload["execution"]["state"].as_str().unwrap(), "exited");
    }

    // ── Checkpoint capability gating tests ──

    /// Helper that builds a router with specific capability flags.
    fn test_config_with_capabilities(
        state_store_path: PathBuf,
        capabilities: RuntimeCapabilities,
    ) -> ApiConfig {
        ApiConfig {
            state_store_path,
            capabilities,
            event_poll_interval: Duration::from_millis(10),
            default_event_page_size: 2,
        }
    }

    #[tokio::test]
    async fn checkpoint_create_vm_full_rejected_when_capability_disabled() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        StateStore::open(&state_path).unwrap();

        // vm_full_checkpoint is false by default.
        let config = test_config_with_capabilities(
            state_path,
            RuntimeCapabilities {
                fs_quick_checkpoint: true,
                ..RuntimeCapabilities::default()
            },
        );
        let app = router(config);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/checkpoints")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sandbox_id": "sbx-test", "class": "vm_full"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"]["code"].as_str().unwrap(),
            "unsupported_checkpoint_class"
        );
    }

    #[tokio::test]
    async fn checkpoint_create_fs_quick_succeeds_when_capability_enabled() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        StateStore::open(&state_path).unwrap();

        let config = test_config_with_capabilities(
            state_path,
            RuntimeCapabilities {
                fs_quick_checkpoint: true,
                ..RuntimeCapabilities::default()
            },
        );
        let app = router(config);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/checkpoints")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sandbox_id": "sbx-test", "class": "fs_quick"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            payload["checkpoint"]["checkpoint_id"]
                .as_str()
                .unwrap()
                .starts_with("ckpt-")
        );
        assert_eq!(payload["checkpoint"]["class"].as_str().unwrap(), "fs_quick");
    }

    #[tokio::test]
    async fn checkpoint_create_fs_quick_rejected_when_capability_disabled() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        StateStore::open(&state_path).unwrap();

        // Both checkpoint capabilities disabled.
        let config = test_config_with_capabilities(state_path, RuntimeCapabilities::default());
        let app = router(config);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/checkpoints")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sandbox_id": "sbx-test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Default class is fs_quick; it should be rejected.
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"]["code"].as_str().unwrap(),
            "unsupported_checkpoint_class"
        );
    }

    #[tokio::test]
    async fn checkpoint_fork_from_non_ready_returns_409() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        // Create a checkpoint directly in Creating state (no transition to Ready).
        let checkpoint = Checkpoint {
            checkpoint_id: "ckpt-creating".to_string(),
            sandbox_id: "sbx-test".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Creating,
            created_at: 1000,
            compatibility_fingerprint: "fp-1".to_string(),
        };
        store.save_checkpoint(&checkpoint).unwrap();

        let config = test_config(state_path);
        let app = router(config);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/checkpoints/ckpt-creating/fork")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"]["code"].as_str().unwrap(),
            "checkpoint_not_ready"
        );
    }

    #[tokio::test]
    async fn checkpoint_children_returns_forked_children() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        // Create a parent checkpoint in Ready state.
        let mut parent = Checkpoint {
            checkpoint_id: "ckpt-parent".to_string(),
            sandbox_id: "sbx-parent".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Creating,
            created_at: 1000,
            compatibility_fingerprint: "fp-parent".to_string(),
        };
        parent.transition_to(CheckpointState::Ready).unwrap();
        store.save_checkpoint(&parent).unwrap();

        // Create a child checkpoint.
        let mut child = Checkpoint {
            checkpoint_id: "ckpt-child".to_string(),
            sandbox_id: "sbx-child".to_string(),
            parent_checkpoint_id: Some("ckpt-parent".to_string()),
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Creating,
            created_at: 2000,
            compatibility_fingerprint: "fp-parent".to_string(),
        };
        child.transition_to(CheckpointState::Ready).unwrap();
        store.save_checkpoint(&child).unwrap();

        let config = test_config(state_path);
        let app = router(config);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/checkpoints/ckpt-parent/children")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let checkpoints = payload["checkpoints"].as_array().unwrap();
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(
            checkpoints[0]["checkpoint_id"].as_str().unwrap(),
            "ckpt-child"
        );
        assert_eq!(
            checkpoints[0]["parent_checkpoint_id"].as_str().unwrap(),
            "ckpt-parent"
        );
    }

    #[tokio::test]
    async fn checkpoint_children_404_for_unknown_parent() {
        let (app, _dir) = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/checkpoints/ckpt-nonexistent/children")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn checkpoint_restore_includes_fingerprint_metadata() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        let mut checkpoint = Checkpoint {
            checkpoint_id: "ckpt-fp".to_string(),
            sandbox_id: "sbx-fp".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Creating,
            created_at: 1000,
            compatibility_fingerprint: "kernel-6.1-arm64".to_string(),
        };
        checkpoint.transition_to(CheckpointState::Ready).unwrap();
        store.save_checkpoint(&checkpoint).unwrap();

        let config = test_config(state_path);
        let app = router(config);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/checkpoints/ckpt-fp/restore")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["compatibility_fingerprint"].as_str().unwrap(),
            "kernel-6.1-arm64"
        );
        assert!(payload["restore_note"].as_str().is_some());
    }

    // ── Cross-transport behavior parity tests ──────────────────────

    #[test]
    fn transport_parity_openapi_operations_match_grpc_rpcs() {
        let doc = openapi_document();
        let paths = doc["paths"].as_object().unwrap();

        // Extract all operationIds from OpenAPI.
        let mut openapi_operations: Vec<String> = Vec::new();
        for (_path, methods) in paths {
            let methods_obj = match methods.as_object() {
                Some(obj) => obj,
                None => continue,
            };
            for (_method, op) in methods_obj {
                if let Some(op_id) = op.get("operationId").and_then(|v| v.as_str()) {
                    openapi_operations.push(op_id.to_string());
                }
            }
        }
        openapi_operations.sort();

        // Define expected gRPC RPC names (from runtime_v2.proto).
        let grpc_rpcs = [
            // SandboxService
            "CreateSandbox",
            "GetSandbox",
            "ListSandboxes",
            "TerminateSandbox",
            // LeaseService
            "OpenLease",
            "GetLease",
            "ListLeases",
            "HeartbeatLease",
            "CloseLease",
            // ContainerService
            "CreateContainer",
            "GetContainer",
            "ListContainers",
            "RemoveContainer",
            // ExecutionService
            "CreateExecution",
            "GetExecution",
            "ListExecutions",
            "CancelExecution",
            "StreamExecOutput",
            "ResizeExecPty",
            "SignalExec",
            // CheckpointService
            "CreateCheckpoint",
            "GetCheckpoint",
            "ListCheckpoints",
            "RestoreCheckpoint",
            "ForkCheckpoint",
            // BuildService
            "StartBuild",
            "GetBuild",
            "ListBuilds",
            "CancelBuild",
            "StreamBuildEvents",
            // EventService
            "ListEvents",
            "StreamEvents",
            // CapabilityService
            "GetCapabilities",
        ];

        // Streaming RPCs use SSE/WS, not REST — they have separate
        // OpenAPI operations (streamEventsSse, streamEventsWs) rather
        // than a direct camelCase mapping.
        let streaming_rpcs = ["StreamExecOutput", "StreamBuildEvents", "StreamEvents"];

        for rpc in &grpc_rpcs {
            if streaming_rpcs.contains(rpc) {
                continue;
            }
            // Map PascalCase to camelCase: "CreateSandbox" -> "createSandbox"
            let camel = rpc[..1].to_lowercase() + &rpc[1..];
            // ResizeExecPty maps to "resizeExec" in OpenAPI (shorter form)
            let aliases: Vec<String> = if *rpc == "ResizeExecPty" {
                vec![camel.clone(), "resizeExec".to_string()]
            } else {
                vec![camel.clone()]
            };
            assert!(
                aliases
                    .iter()
                    .any(|alias| openapi_operations.iter().any(|op| op == alias)),
                "gRPC RPC '{}' has no matching OpenAPI operationId (tried {:?}). Available: {:?}",
                rpc,
                aliases,
                openapi_operations
            );
        }
    }

    #[test]
    fn transport_parity_shared_error_codes() {
        let doc = openapi_document();
        let error_schema = &doc["components"]["schemas"]["ErrorResponse"];
        assert!(
            error_schema.is_object(),
            "ErrorResponse schema must exist in components/schemas"
        );

        // Verify error response has the required 'error' field with code and message.
        let properties = &error_schema["properties"];
        assert!(
            properties["error"].is_object(),
            "ErrorResponse must have an 'error' property"
        );
        let error_properties = &properties["error"]["properties"];
        assert!(
            error_properties["code"].is_object(),
            "error.code must be defined"
        );
        assert!(
            error_properties["message"].is_object(),
            "error.message must be defined"
        );
        assert!(
            error_properties["request_id"].is_object(),
            "error.request_id must be defined"
        );
    }

    #[test]
    fn transport_parity_request_metadata_fields_present() {
        let doc = openapi_document();
        let params = doc["components"]["parameters"].as_object().unwrap();

        assert!(
            params.contains_key("IdempotencyKey"),
            "IdempotencyKey parameter must exist in components/parameters"
        );
        assert!(
            params.contains_key("RequestId"),
            "RequestId parameter must exist in components/parameters"
        );

        // Verify IdempotencyKey is a header parameter.
        let idem = &params["IdempotencyKey"];
        assert_eq!(idem["in"].as_str().unwrap(), "header");
        assert_eq!(idem["name"].as_str().unwrap(), "Idempotency-Key");

        // Verify RequestId is a header parameter.
        let req_id = &params["RequestId"];
        assert_eq!(req_id["in"].as_str().unwrap(), "header");
        assert_eq!(req_id["name"].as_str().unwrap(), "X-Request-Id");
    }

    #[test]
    fn transport_parity_entity_payload_field_consistency() {
        let doc = openapi_document();
        let schemas = doc["components"]["schemas"].as_object().unwrap();

        // SandboxPayload fields: sandbox_id, backend, state, cpus, memory_mb,
        // created_at, updated_at, labels.
        let sandbox = &schemas["SandboxPayload"]["properties"];
        assert!(
            sandbox["sandbox_id"].is_object(),
            "SandboxPayload.sandbox_id missing"
        );
        assert!(
            sandbox["backend"].is_object(),
            "SandboxPayload.backend missing"
        );
        assert!(sandbox["state"].is_object(), "SandboxPayload.state missing");
        assert!(sandbox["cpus"].is_object(), "SandboxPayload.cpus missing");
        assert!(
            sandbox["memory_mb"].is_object(),
            "SandboxPayload.memory_mb missing"
        );
        assert!(
            sandbox["created_at"].is_object(),
            "SandboxPayload.created_at missing"
        );
        assert!(
            sandbox["updated_at"].is_object(),
            "SandboxPayload.updated_at missing"
        );
        assert!(
            sandbox["labels"].is_object(),
            "SandboxPayload.labels missing"
        );

        // LeasePayload fields: lease_id, sandbox_id, ttl_secs, last_heartbeat_at, state.
        let lease = &schemas["LeasePayload"]["properties"];
        assert!(
            lease["lease_id"].is_object(),
            "LeasePayload.lease_id missing"
        );
        assert!(
            lease["sandbox_id"].is_object(),
            "LeasePayload.sandbox_id missing"
        );
        assert!(
            lease["ttl_secs"].is_object(),
            "LeasePayload.ttl_secs missing"
        );
        assert!(
            lease["last_heartbeat_at"].is_object(),
            "LeasePayload.last_heartbeat_at missing"
        );
        assert!(lease["state"].is_object(), "LeasePayload.state missing");

        // ExecutionPayload fields: execution_id, container_id, state, exit_code,
        // started_at, ended_at.
        let exec = &schemas["ExecutionPayload"]["properties"];
        assert!(
            exec["execution_id"].is_object(),
            "ExecutionPayload.execution_id missing"
        );
        assert!(
            exec["container_id"].is_object(),
            "ExecutionPayload.container_id missing"
        );
        assert!(exec["state"].is_object(), "ExecutionPayload.state missing");
        assert!(
            exec["exit_code"].is_object(),
            "ExecutionPayload.exit_code missing"
        );
        assert!(
            exec["started_at"].is_object(),
            "ExecutionPayload.started_at missing"
        );
        assert!(
            exec["ended_at"].is_object(),
            "ExecutionPayload.ended_at missing"
        );

        // CheckpointPayload fields: checkpoint_id, sandbox_id, parent_checkpoint_id,
        // class, state, compatibility_fingerprint, created_at.
        let ckpt = &schemas["CheckpointPayload"]["properties"];
        assert!(
            ckpt["checkpoint_id"].is_object(),
            "CheckpointPayload.checkpoint_id missing"
        );
        assert!(
            ckpt["sandbox_id"].is_object(),
            "CheckpointPayload.sandbox_id missing"
        );
        assert!(
            ckpt["parent_checkpoint_id"].is_object(),
            "CheckpointPayload.parent_checkpoint_id missing"
        );
        assert!(ckpt["class"].is_object(), "CheckpointPayload.class missing");
        assert!(ckpt["state"].is_object(), "CheckpointPayload.state missing");
        assert!(
            ckpt["compatibility_fingerprint"].is_object(),
            "CheckpointPayload.compatibility_fingerprint missing"
        );
        assert!(
            ckpt["created_at"].is_object(),
            "CheckpointPayload.created_at missing"
        );
    }

    #[test]
    fn transport_parity_idempotency_on_mutating_operations() {
        let doc = openapi_document();
        let paths = doc["paths"].as_object().unwrap();

        let mut mutating_without_idempotency = Vec::new();

        for (path, methods) in paths {
            let methods_obj = match methods.as_object() {
                Some(obj) => obj,
                None => continue,
            };
            // Check POST operations.
            if let Some(post_op) = methods_obj.get("post") {
                let op_id = post_op
                    .get("operationId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");

                // Check if parameters reference IdempotencyKey.
                let has_idempotency = post_op
                    .get("parameters")
                    .and_then(|p| p.as_array())
                    .map(|params| {
                        params.iter().any(|p| {
                            p.get("$ref")
                                .and_then(|r| r.as_str())
                                .map(|r| r.contains("IdempotencyKey"))
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false);

                // Heartbeat, restore, resize, signal are POST but may not
                // need idempotency.
                let exempt = [
                    "heartbeatLease",
                    "restoreCheckpoint",
                    "resizeExec",
                    "signalExec",
                ];
                if !has_idempotency && !exempt.contains(&op_id) {
                    mutating_without_idempotency.push(format!("{} ({})", path, op_id));
                }
            }

            // Check DELETE operations (which are also mutating).
            if let Some(delete_op) = methods_obj.get("delete") {
                let op_id = delete_op
                    .get("operationId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");

                let has_idempotency = delete_op
                    .get("parameters")
                    .and_then(|p| p.as_array())
                    .map(|params| {
                        params.iter().any(|p| {
                            p.get("$ref")
                                .and_then(|r| r.as_str())
                                .map(|r| r.contains("IdempotencyKey"))
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false);

                let exempt_delete = ["removeContainer", "cancelExecution"];
                if !has_idempotency && !exempt_delete.contains(&op_id) {
                    mutating_without_idempotency.push(format!("{} DELETE ({})", path, op_id));
                }
            }
        }

        // All major create/terminate/close operations should have idempotency.
        // We assert that none of the critical mutating POST operations lack it.
        let critical_missing: Vec<&str> = mutating_without_idempotency
            .iter()
            .filter(|s| {
                s.contains("createSandbox")
                    || s.contains("openLease")
                    || s.contains("createExecution")
                    || s.contains("createCheckpoint")
                    || s.contains("terminateSandbox")
                    || s.contains("closeLease")
                    || s.contains("forkCheckpoint")
            })
            .map(|s| s.as_str())
            .collect();
        assert!(
            critical_missing.is_empty(),
            "Critical mutating operations missing IdempotencyKey: {:?}",
            critical_missing
        );
    }

    // ── Authorization and policy-enforcement verification tests (vz-9gz) ──

    // -- Scenario 1: Mutating endpoints require valid request bodies --

    #[tokio::test]
    async fn authz_sandbox_create_rejects_invalid_json() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sandboxes")
                    .header("content-type", "application/json")
                    .body(Body::from("not valid json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"]["code"].as_str().unwrap(),
            "invalid_request"
        );
        // Must include request_id in error envelope
        assert!(payload["error"]["request_id"].as_str().is_some());
    }

    #[tokio::test]
    async fn authz_lease_create_rejects_invalid_json() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/leases")
                    .header("content-type", "application/json")
                    .body(Body::from("{malformed"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"]["code"].as_str().unwrap(),
            "invalid_request"
        );
    }

    #[tokio::test]
    async fn authz_execution_create_rejects_invalid_json() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/executions")
                    .header("content-type", "application/json")
                    .body(Body::from("<<invalid>>"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"]["code"].as_str().unwrap(),
            "invalid_request"
        );
    }

    #[tokio::test]
    async fn authz_checkpoint_create_rejects_invalid_json() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/checkpoints")
                    .header("content-type", "application/json")
                    .body(Body::from("not-json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"]["code"].as_str().unwrap(),
            "invalid_request"
        );
    }

    #[tokio::test]
    async fn authz_container_create_rejects_invalid_json() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/containers")
                    .header("content-type", "application/json")
                    .body(Body::from("{{bad"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"]["code"].as_str().unwrap(),
            "invalid_request"
        );
    }

    #[tokio::test]
    async fn authz_fork_checkpoint_rejects_invalid_json() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        // Create a ready checkpoint so we get past the 404 check.
        let mut ckpt = Checkpoint {
            checkpoint_id: "ckpt-fork-json".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Creating,
            created_at: 1000,
            compatibility_fingerprint: "fp-1".to_string(),
        };
        ckpt.transition_to(CheckpointState::Ready).unwrap();
        store.save_checkpoint(&ckpt).unwrap();

        let app = router(test_config(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/checkpoints/ckpt-fork-json/fork")
                    .header("content-type", "application/json")
                    .body(Body::from("not json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"]["code"].as_str().unwrap(),
            "invalid_request"
        );
    }

    // -- Scenario 2: Sandbox ownership validation --

    #[tokio::test]
    async fn authz_sandbox_get_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/sandboxes/sbx-does-not-exist")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["error"]["code"].as_str().unwrap(), "not_found");
        // Error message should reference the sandbox ID but not leak internals.
        let msg = payload["error"]["message"].as_str().unwrap();
        assert!(msg.contains("sbx-does-not-exist"));
        assert!(
            !msg.contains("sqlite"),
            "error must not leak storage internals"
        );
        assert!(!msg.contains("SQL"), "error must not leak SQL details");
    }

    #[tokio::test]
    async fn authz_sandbox_terminate_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/sandboxes/sbx-phantom")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["error"]["code"].as_str().unwrap(), "not_found");
    }

    #[tokio::test]
    async fn authz_sandbox_operations_scoped_to_id() {
        let (app, _dir) = test_router();

        // Create a sandbox
        let create_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sandboxes")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"cpus": 1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create_resp.status(), StatusCode::CREATED);

        let create_body = to_bytes(create_resp.into_body(), usize::MAX).await.unwrap();
        let created: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
        let real_id = created["sandbox"]["sandbox_id"].as_str().unwrap();

        // GET with the real ID succeeds
        let get_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sandboxes/{real_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);

        // GET with a tampered ID returns 404
        let tampered_id = format!("{real_id}-tampered");
        let tampered_resp = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sandboxes/{tampered_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(tampered_resp.status(), StatusCode::NOT_FOUND);
    }

    // -- Scenario 3: Execution operations validate parent entity access --

    #[tokio::test]
    async fn authz_execution_get_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/executions/exec-nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["error"]["code"].as_str().unwrap(), "not_found");
    }

    #[tokio::test]
    async fn authz_execution_cancel_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/executions/exec-phantom")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn authz_execution_resize_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/executions/exec-phantom/resize")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"cols":80,"rows":24}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn authz_execution_signal_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/executions/exec-phantom/signal")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"signal":"SIGTERM"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn authz_execution_signal_on_terminal_returns_conflict() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        let mut execution = Execution {
            execution_id: "exec-done-sig".to_string(),
            container_id: "ctr-1".to_string(),
            exec_spec: ExecutionSpec {
                cmd: vec!["echo".to_string()],
                args: vec![],
                env_override: BTreeMap::new(),
                pty: false,
                timeout_secs: None,
            },
            state: ExecutionState::Running,
            exit_code: None,
            started_at: Some(now_epoch_secs()),
            ended_at: None,
        };
        let _ = execution.transition_to(ExecutionState::Exited);
        execution.exit_code = Some(0);
        execution.ended_at = Some(now_epoch_secs());
        store.save_execution(&execution).unwrap();

        let app = router(test_config(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/executions/exec-done-sig/signal")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"signal":"SIGKILL"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["error"]["code"].as_str().unwrap(), "invalid_state");
    }

    // -- Scenario 4: Lease entity validation --

    #[tokio::test]
    async fn authz_lease_get_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/leases/ls-phantom")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn authz_lease_close_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/leases/ls-phantom")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn authz_lease_heartbeat_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/leases/ls-phantom/heartbeat")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn authz_lease_heartbeat_on_closed_lease_returns_422() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        let mut lease = Lease {
            lease_id: "ls-closed-hb".to_string(),
            sandbox_id: "sbx-1".to_string(),
            ttl_secs: 300,
            last_heartbeat_at: now_epoch_secs(),
            state: LeaseState::Opening,
        };
        lease.transition_to(LeaseState::Active).unwrap();
        lease.transition_to(LeaseState::Closed).unwrap();
        store.save_lease(&lease).unwrap();

        let app = router(test_config(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/leases/ls-closed-hb/heartbeat")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["error"]["code"].as_str().unwrap(), "invalid_state");
    }

    // -- Scenario 5: Checkpoint entity validation --

    #[tokio::test]
    async fn authz_checkpoint_get_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/checkpoints/ckpt-phantom")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn authz_checkpoint_restore_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/checkpoints/ckpt-phantom/restore")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn authz_checkpoint_restore_not_ready_returns_409() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        let ckpt = Checkpoint {
            checkpoint_id: "ckpt-not-ready".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Creating,
            created_at: 1000,
            compatibility_fingerprint: "fp-1".to_string(),
        };
        store.save_checkpoint(&ckpt).unwrap();

        let app = router(test_config(state_path));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/checkpoints/ckpt-not-ready/restore")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"]["code"].as_str().unwrap(),
            "checkpoint_not_ready"
        );
    }

    // -- Scenario 6: Container entity validation --

    #[tokio::test]
    async fn authz_container_get_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/containers/ctr-phantom")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn authz_container_remove_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/containers/ctr-phantom")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // -- Scenario 7: Idempotency conflict detection (rate-limiting behavior) --

    #[tokio::test]
    async fn authz_idempotency_key_replay_returns_cached_response() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        StateStore::open(&state_path).unwrap();

        let app = router(test_config(state_path));
        let body_bytes = r#"{"cpus": 2, "memory_mb": 512}"#;

        // First request with idempotency key
        let first_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sandboxes")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "test-key-alpha")
                    .body(Body::from(body_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_resp.status(), StatusCode::CREATED);

        let first_body = to_bytes(first_resp.into_body(), usize::MAX).await.unwrap();
        let first_payload: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        let first_sandbox_id = first_payload["sandbox"]["sandbox_id"]
            .as_str()
            .unwrap()
            .to_string();

        // Second request with the same key and same body returns cached response
        let second_resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sandboxes")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "test-key-alpha")
                    .body(Body::from(body_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second_resp.status(), StatusCode::CREATED);

        let second_body = to_bytes(second_resp.into_body(), usize::MAX).await.unwrap();
        let second_payload: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        let second_sandbox_id = second_payload["sandbox"]["sandbox_id"]
            .as_str()
            .unwrap()
            .to_string();

        // Same sandbox_id proves it is a replay, not a new creation.
        assert_eq!(first_sandbox_id, second_sandbox_id);
    }

    #[tokio::test]
    async fn authz_idempotency_key_conflict_returns_409() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        StateStore::open(&state_path).unwrap();

        let app = router(test_config(state_path));

        // First request with idempotency key
        let first_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sandboxes")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "test-key-beta")
                    .body(Body::from(r#"{"cpus": 1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_resp.status(), StatusCode::CREATED);

        // Second request with same key but DIFFERENT body triggers conflict
        let conflict_resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sandboxes")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "test-key-beta")
                    .body(Body::from(r#"{"cpus": 4}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(conflict_resp.status(), StatusCode::CONFLICT);

        let conflict_body = to_bytes(conflict_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let conflict_payload: serde_json::Value = serde_json::from_slice(&conflict_body).unwrap();
        assert_eq!(
            conflict_payload["error"]["code"].as_str().unwrap(),
            "idempotency_conflict"
        );
    }

    #[tokio::test]
    async fn authz_idempotency_key_on_lease_creation() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        StateStore::open(&state_path).unwrap();

        let app = router(test_config(state_path));
        let body_bytes = r#"{"sandbox_id": "sbx-1"}"#;

        // First request
        let first_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/leases")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "lease-key-1")
                    .body(Body::from(body_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_resp.status(), StatusCode::CREATED);

        let first_body = to_bytes(first_resp.into_body(), usize::MAX).await.unwrap();
        let first_payload: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        let first_lease_id = first_payload["lease"]["lease_id"]
            .as_str()
            .unwrap()
            .to_string();

        // Same key + same body replays
        let second_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/leases")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "lease-key-1")
                    .body(Body::from(body_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second_resp.status(), StatusCode::CREATED);

        let second_body = to_bytes(second_resp.into_body(), usize::MAX).await.unwrap();
        let second_payload: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(
            first_lease_id,
            second_payload["lease"]["lease_id"].as_str().unwrap()
        );

        // Different body with same key returns 409
        let conflict_resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/leases")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "lease-key-1")
                    .body(Body::from(r#"{"sandbox_id": "sbx-2"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(conflict_resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn authz_idempotency_key_on_execution_creation() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        StateStore::open(&state_path).unwrap();

        let app = router(test_config(state_path));
        let body_bytes = r#"{"container_id": "ctr-1", "cmd": ["echo", "hi"]}"#;

        // First request
        let first_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/executions")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "exec-key-1")
                    .body(Body::from(body_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_resp.status(), StatusCode::CREATED);

        // Conflict with different body
        let conflict_resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/executions")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "exec-key-1")
                    .body(Body::from(
                        r#"{"container_id": "ctr-2", "cmd": ["echo", "bye"]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(conflict_resp.status(), StatusCode::CONFLICT);
    }

    // -- Scenario 8: Error responses don't leak internal details --

    #[tokio::test]
    async fn authz_error_responses_use_consistent_envelope() {
        let (app, _dir) = test_router();

        // Collect error responses from various not-found endpoints.
        let endpoints = vec![
            "/v1/sandboxes/sbx-leak-test",
            "/v1/leases/ls-leak-test",
            "/v1/executions/exec-leak-test",
            "/v1/checkpoints/ckpt-leak-test",
            "/v1/containers/ctr-leak-test",
            "/v1/receipts/rcp-leak-test",
        ];

        for uri in &endpoints {
            let resp = app
                .clone()
                .oneshot(Request::builder().uri(*uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::NOT_FOUND,
                "expected 404 for {uri}"
            );

            let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

            // All error envelopes must have exactly the same structure.
            assert!(
                payload["error"].is_object(),
                "missing error envelope for {uri}"
            );
            assert!(
                payload["error"]["code"].is_string(),
                "missing error.code for {uri}"
            );
            assert!(
                payload["error"]["message"].is_string(),
                "missing error.message for {uri}"
            );
            assert!(
                payload["error"]["request_id"].is_string(),
                "missing error.request_id for {uri}"
            );

            // Error messages must not leak implementation details.
            let msg = payload["error"]["message"].as_str().unwrap();
            let code = payload["error"]["code"].as_str().unwrap();
            assert_eq!(
                code, "not_found",
                "error code should be 'not_found' for {uri}"
            );
            assert!(
                !msg.contains("sqlite"),
                "error message leaks sqlite for {uri}: {msg}"
            );
            assert!(
                !msg.contains("SQL"),
                "error message leaks SQL for {uri}: {msg}"
            );
            assert!(
                !msg.contains("rusqlite"),
                "error message leaks rusqlite for {uri}: {msg}"
            );
            assert!(
                !msg.contains("table"),
                "error message leaks table name for {uri}: {msg}"
            );
            assert!(
                !msg.to_lowercase().contains("stack trace"),
                "error message leaks stack trace for {uri}: {msg}"
            );
            assert!(
                !msg.contains("panicked"),
                "error message leaks panic for {uri}: {msg}"
            );
        }
    }

    #[tokio::test]
    async fn authz_bad_request_errors_do_not_leak_internals() {
        let (app, _dir) = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sandboxes")
                    .header("content-type", "application/json")
                    .body(Body::from("{invalid json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let msg = payload["error"]["message"].as_str().unwrap();

        // The error should describe the problem but not expose internal types.
        assert!(
            msg.contains("invalid JSON body"),
            "expected user-friendly JSON error prefix"
        );
        assert!(
            !msg.contains("serde_json::"),
            "error leaks serde_json module path"
        );
        // Note: serde line/column info (e.g. "at line 1 column 2") is acceptable
        // because it helps API consumers debug malformed request bodies.
        // What must NOT appear is internal stack traces or file paths.
        assert!(!msg.contains("src/"), "error leaks source file path");
    }

    // -- Scenario 9: X-Request-Id propagation --

    #[tokio::test]
    async fn authz_request_id_propagated_in_response() {
        let (app, _dir) = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/capabilities")
                    .header("x-request-id", "custom-req-42")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["request_id"].as_str().unwrap(), "custom-req-42");
    }

    #[tokio::test]
    async fn authz_request_id_propagated_in_error_responses() {
        let (app, _dir) = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/sandboxes/sbx-nonexistent")
                    .header("x-request-id", "err-req-99")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"]["request_id"].as_str().unwrap(),
            "err-req-99"
        );
    }

    #[tokio::test]
    async fn authz_request_id_generated_when_not_provided() {
        let (app, _dir) = test_router();

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
        let request_id = payload["request_id"].as_str().unwrap();
        assert!(
            request_id.starts_with("req_"),
            "auto-generated request_id should start with 'req_', got: {request_id}"
        );
    }

    // -- Scenario 10: Receipt generation for mutating operations --

    #[tokio::test]
    async fn authz_mutating_operations_generate_receipt_header() {
        let (app, _dir) = test_router();

        // Create a sandbox and verify receipt header is present.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sandboxes")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"cpus": 1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let receipt_id = response
            .headers()
            .get("x-receipt-id")
            .expect("mutating operation should return x-receipt-id header")
            .to_str()
            .unwrap()
            .to_string();
        assert!(
            receipt_id.starts_with("rcp-"),
            "receipt_id should start with rcp-, got: {receipt_id}"
        );

        // The receipt should be retrievable via GET /v1/receipts/{receipt_id}.
        let receipt_resp = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/receipts/{receipt_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(receipt_resp.status(), StatusCode::OK);

        let receipt_body = to_bytes(receipt_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let receipt_payload: serde_json::Value = serde_json::from_slice(&receipt_body).unwrap();
        assert_eq!(
            receipt_payload["receipt"]["receipt_id"].as_str().unwrap(),
            receipt_id
        );
        assert_eq!(
            receipt_payload["receipt"]["operation"].as_str().unwrap(),
            "create_sandbox"
        );
        assert_eq!(
            receipt_payload["receipt"]["status"].as_str().unwrap(),
            "completed"
        );
    }

    // -- Scenario 11: Build entity validation --

    #[tokio::test]
    async fn authz_build_get_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/builds/bld-phantom")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn authz_build_cancel_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/builds/bld-phantom")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // -- Scenario 12: Image entity validation --

    #[tokio::test]
    async fn authz_image_get_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/images/nonexistent:latest")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // -- Scenario 13: Receipt entity validation --

    #[tokio::test]
    async fn authz_receipt_get_nonexistent_returns_404() {
        let (app, _dir) = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/receipts/rcp-phantom")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ── Throughput and capacity tests (vz-lbg) ─────────────────────

    /// Simulate multiple sequential create_sandbox calls and verify
    /// that throughput is acceptable.
    #[tokio::test]
    async fn throughput_sequential_create_sandbox() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        StateStore::open(&state_path).unwrap();

        let app = router(test_config(state_path));

        let start = std::time::Instant::now();
        let request_count = 20;

        for i in 0..request_count {
            let body = serde_json::to_vec(&serde_json::json!({
                "stack_name": format!("stack-{i}"),
                "cpus": 2,
                "memory_mb": 512
            }))
            .unwrap();

            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/sandboxes")
                        .header("Content-Type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let elapsed = start.elapsed();
        // 20 sequential creates should complete within 5 seconds on CI.
        assert!(
            elapsed.as_secs() < 5,
            "{request_count} sequential create_sandbox calls took {elapsed:?} (>5s budget)"
        );
    }

    /// List operations with a large result set should remain performant.
    #[tokio::test]
    async fn throughput_list_sandboxes_large_result_set() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        // Pre-populate 50 sandboxes directly via StateStore.
        for i in 0..50 {
            let now = i as u64 + 1_700_000_000;
            let mut labels = std::collections::BTreeMap::new();
            labels.insert("stack_name".to_string(), format!("stack-{i}"));
            let sandbox = Sandbox {
                sandbox_id: format!("sbx-{i:04}"),
                backend: SandboxBackend::MacosVz,
                spec: SandboxSpec {
                    cpus: Some(2),
                    memory_mb: Some(512),
                    ..SandboxSpec::default()
                },
                state: SandboxState::Ready,
                created_at: now,
                updated_at: now,
                labels,
            };
            store.save_sandbox(&sandbox).unwrap();
        }
        drop(store);

        let app = router(test_config(state_path));

        let start = std::time::Instant::now();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/sandboxes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let sandboxes = payload["sandboxes"].as_array().unwrap();
        assert_eq!(sandboxes.len(), 50);

        // Listing 50 sandboxes should complete well under 2 seconds.
        assert!(
            elapsed.as_secs() < 2,
            "list_sandboxes with 50 entries took {elapsed:?} (>2s budget)"
        );
    }

    /// Events endpoint with a large event history should paginate
    /// without performance degradation.
    #[tokio::test]
    async fn throughput_events_large_history() {
        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        // Pre-populate 1,000 events.
        for i in 0..1_000 {
            store
                .emit_event(
                    "perf-stack",
                    &StackEvent::ServiceCreating {
                        stack_name: "perf-stack".to_string(),
                        service_name: format!("svc-{i}"),
                    },
                )
                .unwrap();
        }
        drop(store);

        let app = router(test_config(state_path));

        // Query page from midpoint.
        let start = std::time::Instant::now();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/perf-stack?after=500&limit=100")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let events = payload["events"].as_array().unwrap();
        assert_eq!(events.len(), 100);

        // Paginated query against 1,000 events should complete in under 1 second.
        assert!(
            elapsed.as_secs() < 1,
            "events pagination (100 of 1000) took {elapsed:?} (>1s budget)"
        );
    }

    /// List leases endpoint should handle a large number of leases.
    #[tokio::test]
    async fn throughput_list_leases_large_set() {
        use vz_runtime_contract::Lease;

        let temp_dir = tempdir().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let store = StateStore::open(&state_path).unwrap();

        // Pre-populate a sandbox and 30 leases.
        let mut labels = std::collections::BTreeMap::new();
        labels.insert("stack_name".to_string(), "lease-perf".to_string());
        let sandbox = Sandbox {
            sandbox_id: "sbx-lease-perf".to_string(),
            backend: SandboxBackend::MacosVz,
            spec: SandboxSpec::default(),
            state: SandboxState::Ready,
            created_at: 1_700_000_000,
            updated_at: 1_700_000_000,
            labels,
        };
        store.save_sandbox(&sandbox).unwrap();

        for i in 0..30 {
            let lease = Lease {
                lease_id: format!("lse-{i:04}"),
                sandbox_id: "sbx-lease-perf".to_string(),
                ttl_secs: 300,
                last_heartbeat_at: 1_700_000_000 + i,
                state: vz_runtime_contract::LeaseState::Active,
            };
            store.save_lease(&lease).unwrap();
        }
        drop(store);

        let app = router(test_config(state_path));

        let start = std::time::Instant::now();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/leases")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let leases = payload["leases"].as_array().unwrap();
        assert_eq!(leases.len(), 30);

        assert!(
            elapsed.as_secs() < 2,
            "list_leases with 30 entries took {elapsed:?} (>2s budget)"
        );
    }
}
