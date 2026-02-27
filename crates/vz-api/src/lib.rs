//! OpenAPI/SSE/WebSocket transport adapter for Runtime V2.

#![forbid(unsafe_code)]
#![recursion_limit = "256"]

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_stream::stream;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;
use vz_runtime_contract::{
    Capability, RuntimeCapabilities, SANDBOX_LABEL_BASE_IMAGE_REF, SANDBOX_LABEL_MAIN_CONTAINER,
};
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::{DaemonClient, DaemonClientConfig, DaemonClientError};
use vz_stack::EventRecord;

#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(test)]
use vz_runtime_contract::{
    Checkpoint, CheckpointClass, CheckpointState, Execution, ExecutionSpec, ExecutionState, Lease,
    LeaseState, MachineErrorEnvelope, Sandbox, SandboxBackend, SandboxSpec, SandboxState,
};
#[cfg(test)]
use vz_stack::StateStore;

const DEFAULT_EVENT_PAGE_SIZE: usize = 100;
const MAX_EVENT_PAGE_SIZE: usize = 1000;
const DEFAULT_EVENT_POLL_INTERVAL: Duration = Duration::from_millis(250);

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

mod daemon_bridge;
mod handlers;
mod openapi_doc;

use daemon_bridge::*;
use handlers::*;
use openapi_doc::openapi_document;

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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
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
                .unwrap_or_else(|_| serialization_error_value()),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
struct EventsResponse {
    request_id: String,
    events: Vec<ApiEventRecord>,
    next_cursor: i64,
}

#[derive(Debug, Serialize, ToSchema)]
struct CapabilitiesResponse {
    request_id: String,
    #[schema(value_type = Vec<String>)]
    capabilities: Vec<Capability>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct CreateSandboxRequest {
    #[serde(default)]
    stack_name: Option<String>,
    #[serde(default)]
    cpus: Option<u8>,
    #[serde(default)]
    memory_mb: Option<u64>,
    #[serde(default)]
    base_image_ref: Option<String>,
    #[serde(default)]
    main_container: Option<String>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct SandboxPayload {
    sandbox_id: String,
    backend: String,
    state: String,
    cpus: Option<u8>,
    memory_mb: Option<u64>,
    base_image_ref: Option<String>,
    main_container: Option<String>,
    created_at: u64,
    updated_at: u64,
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct SandboxResponse {
    request_id: String,
    sandbox: SandboxPayload,
}

#[derive(Debug, Serialize, ToSchema)]
struct SandboxListResponse {
    request_id: String,
    sandboxes: Vec<SandboxPayload>,
}

// ── Lease types ──

#[derive(Debug, Deserialize, ToSchema)]
struct OpenLeaseRequest {
    sandbox_id: String,
    #[serde(default)]
    ttl_secs: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
struct LeasePayload {
    lease_id: String,
    sandbox_id: String,
    ttl_secs: u64,
    last_heartbeat_at: u64,
    state: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct LeaseResponse {
    request_id: String,
    lease: LeasePayload,
}

#[derive(Debug, Serialize, ToSchema)]
struct LeaseListResponse {
    request_id: String,
    leases: Vec<LeasePayload>,
}

// ── Execution types ──

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum ExecutionPtyMode {
    Inherit,
    Enabled,
    Disabled,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct CreateExecutionRequest {
    container_id: String,
    cmd: Vec<String>,
    #[serde(default)]
    args: Option<Vec<String>>,
    #[serde(default)]
    env_override: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pty_mode: Option<ExecutionPtyMode>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ExecutionPayload {
    execution_id: String,
    container_id: String,
    state: String,
    exit_code: Option<i32>,
    started_at: Option<u64>,
    ended_at: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ExecutionResponse {
    request_id: String,
    execution: ExecutionPayload,
}

#[derive(Debug, Serialize, ToSchema)]
struct ExecutionListResponse {
    request_id: String,
    executions: Vec<ExecutionPayload>,
}

/// Request body for `POST /v1/executions/{execution_id}/resize`.
#[derive(Debug, Deserialize, ToSchema)]
struct ResizeExecRequest {
    cols: u16,
    rows: u16,
}

/// Request body for `POST /v1/executions/{execution_id}/signal`.
#[derive(Debug, Deserialize, ToSchema)]
struct SignalExecRequest {
    signal: String,
}

/// Request body for `POST /v1/executions/{execution_id}/stdin`.
#[derive(Debug, Deserialize, ToSchema)]
struct WriteExecStdinRequest {
    data: String,
}

// ── Checkpoint types ──

#[derive(Debug, Deserialize, ToSchema)]
struct CreateCheckpointRequest {
    sandbox_id: String,
    #[serde(default)]
    class: Option<String>,
    #[serde(default)]
    compatibility_fingerprint: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct ForkCheckpointRequest {
    #[serde(default)]
    new_sandbox_id: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct CheckpointPayload {
    checkpoint_id: String,
    sandbox_id: String,
    parent_checkpoint_id: Option<String>,
    class: String,
    state: String,
    compatibility_fingerprint: String,
    created_at: u64,
}

#[derive(Debug, Serialize, ToSchema)]
struct CheckpointResponse {
    request_id: String,
    checkpoint: CheckpointPayload,
}

#[derive(Debug, Serialize, ToSchema)]
struct RestoreCheckpointResponse {
    request_id: String,
    checkpoint: CheckpointPayload,
    #[serde(skip_serializing_if = "Option::is_none")]
    compatibility_fingerprint: Option<String>,
    restore_note: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct CheckpointListResponse {
    request_id: String,
    checkpoints: Vec<CheckpointPayload>,
}

// ── Container types ──

#[derive(Debug, Deserialize, ToSchema)]
struct CreateContainerRequest {
    sandbox_id: String,
    #[serde(default)]
    image_digest: Option<String>,
    #[serde(default)]
    cmd: Option<Vec<String>>,
    #[serde(default)]
    env: Option<BTreeMap<String, String>>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    user: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ContainerPayload {
    container_id: String,
    sandbox_id: String,
    image_digest: String,
    state: String,
    created_at: u64,
    started_at: Option<u64>,
    ended_at: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ContainerResponse {
    request_id: String,
    container: ContainerPayload,
}

#[derive(Debug, Serialize, ToSchema)]
struct ContainerListResponse {
    request_id: String,
    containers: Vec<ContainerPayload>,
}

// ── Image types ──

#[derive(Debug, Serialize, ToSchema)]
struct ImagePayload {
    image_ref: String,
    resolved_digest: String,
    platform: String,
    source_registry: String,
    pulled_at: u64,
}

#[derive(Debug, Serialize, ToSchema)]
struct ImageResponse {
    request_id: String,
    image: ImagePayload,
}

#[derive(Debug, Serialize, ToSchema)]
struct ImageListResponse {
    request_id: String,
    images: Vec<ImagePayload>,
}

// ── Receipt types ──

#[derive(Debug, Serialize, ToSchema)]
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

#[derive(Debug, Serialize, ToSchema)]
struct ReceiptResponse {
    request_id: String,
    receipt: ReceiptPayload,
}

// ── Build types ──

#[derive(Debug, Deserialize, ToSchema)]
struct StartBuildRequest {
    sandbox_id: String,
    context: String,
    #[serde(default)]
    dockerfile: Option<String>,
    #[serde(default)]
    args: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Serialize, ToSchema)]
struct BuildPayload {
    build_id: String,
    sandbox_id: String,
    state: String,
    result_digest: Option<String>,
    started_at: u64,
    ended_at: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
struct BuildResponse {
    request_id: String,
    build: BuildPayload,
}

#[derive(Debug, Serialize, ToSchema)]
struct BuildListResponse {
    request_id: String,
    builds: Vec<BuildPayload>,
}

// ── File types ──

#[derive(Debug, Deserialize, ToSchema)]
struct ReadFileRequest {
    sandbox_id: String,
    path: String,
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    limit: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ReadFileResponse {
    request_id: String,
    /// Base64-encoded file content bytes.
    data_base64: String,
    truncated: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
struct WriteFileRequest {
    sandbox_id: String,
    path: String,
    /// Base64-encoded bytes to write.
    data_base64: String,
    #[serde(default)]
    append: Option<bool>,
    #[serde(default)]
    create_parents: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
struct WriteFileResponse {
    request_id: String,
    bytes_written: u64,
}

#[derive(Debug, Deserialize, ToSchema)]
struct ListFilesRequest {
    sandbox_id: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    recursive: Option<bool>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
struct FileEntryPayload {
    path: String,
    is_dir: bool,
    size: u64,
    modified_at: u64,
}

#[derive(Debug, Serialize, ToSchema)]
struct ListFilesResponse {
    request_id: String,
    entries: Vec<FileEntryPayload>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct MakeDirRequest {
    sandbox_id: String,
    path: String,
    #[serde(default)]
    parents: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct RemovePathRequest {
    sandbox_id: String,
    path: String,
    #[serde(default)]
    recursive: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct MovePathRequest {
    sandbox_id: String,
    src_path: String,
    dst_path: String,
    #[serde(default)]
    overwrite: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct CopyPathRequest {
    sandbox_id: String,
    src_path: String,
    dst_path: String,
    #[serde(default)]
    overwrite: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct ChmodPathRequest {
    sandbox_id: String,
    path: String,
    mode: u32,
}

#[derive(Debug, Deserialize, ToSchema)]
struct ChownPathRequest {
    sandbox_id: String,
    path: String,
    uid: u32,
    gid: u32,
}

#[derive(Debug, Serialize, ToSchema)]
struct FileMutationResponse {
    request_id: String,
    path: String,
    status: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct ErrorPayload {
    code: String,
    message: String,
    request_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct ErrorResponse {
    error: ErrorPayload,
}

#[derive(Debug, Serialize)]
struct SerializationErrorPayload {
    #[serde(rename = "type")]
    event_type: &'static str,
}

fn serialization_error_value() -> serde_json::Value {
    match serde_json::to_value(SerializationErrorPayload {
        event_type: "serialization_error",
    }) {
        Ok(value) => value,
        Err(_) => serde_json::Value::Object(serde_json::Map::new()),
    }
}

fn empty_json_object_value() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

fn json_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: ErrorPayload {
                code: code.to_string(),
                message: message.to_string(),
                request_id: request_id.to_string(),
            },
        }),
    )
        .into_response()
}

#[cfg(test)]
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

/// Extract idempotency key from request headers.
fn extract_idempotency_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
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
        .route(
            "/v1/executions/{execution_id}/stdin",
            post(write_exec_stdin),
        )
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
        .route("/v1/files/read", post(read_file))
        .route("/v1/files/write", post(write_file))
        .route("/v1/files/list", post(list_files))
        .route("/v1/files/mkdir", post(make_dir))
        .route("/v1/files/remove", post(remove_path))
        .route("/v1/files/move", post(move_path))
        .route("/v1/files/copy", post(copy_path))
        .route("/v1/files/chmod", post(chmod_path))
        .route("/v1/files/chown", post(chown_path))
        .with_state(state)
}

#[cfg(test)]
mod tests;
