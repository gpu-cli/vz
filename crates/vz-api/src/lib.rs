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
use uuid::Uuid;
use vz_runtime_contract::{
    RuntimeCapabilities, SANDBOX_LABEL_BASE_IMAGE_REF, SANDBOX_LABEL_MAIN_CONTAINER,
};
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::{DaemonClient, DaemonClientConfig, DaemonClientError};

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
mod models;
mod openapi_doc;

use daemon_bridge::*;
use handlers::*;
use models::*;
use openapi_doc::openapi_document;

/// API adapter configuration.
#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// SQLite state-store path used for event reads.
    pub state_store_path: PathBuf,
    /// Optional runtime daemon socket override.
    pub daemon_socket_path: Option<PathBuf>,
    /// Optional runtime daemon data directory override.
    pub daemon_runtime_data_dir: Option<PathBuf>,
    /// Whether API requests may auto-spawn `vz-runtimed` when unreachable.
    pub daemon_auto_spawn: bool,
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
            daemon_socket_path: None,
            daemon_runtime_data_dir: None,
            daemon_auto_spawn: true,
            capabilities: RuntimeCapabilities::default(),
            event_poll_interval: DEFAULT_EVENT_POLL_INTERVAL,
            default_event_page_size: DEFAULT_EVENT_PAGE_SIZE,
        }
    }
}

#[derive(Debug, Clone)]
struct ApiState {
    state_store_path: PathBuf,
    daemon_socket_path: Option<PathBuf>,
    daemon_runtime_data_dir: Option<PathBuf>,
    daemon_auto_spawn: bool,
    capabilities: RuntimeCapabilities,
    event_poll_interval: Duration,
    default_event_page_size: usize,
}

impl From<ApiConfig> for ApiState {
    fn from(config: ApiConfig) -> Self {
        Self {
            state_store_path: config.state_store_path,
            daemon_socket_path: config.daemon_socket_path,
            daemon_runtime_data_dir: config.daemon_runtime_data_dir,
            daemon_auto_spawn: config.daemon_auto_spawn,
            capabilities: config.capabilities,
            event_poll_interval: config.event_poll_interval,
            default_event_page_size: config.default_event_page_size.clamp(1, MAX_EVENT_PAGE_SIZE),
        }
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
        .route(
            "/v1/sandboxes/{sandbox_id}/shell/open",
            post(open_sandbox_shell),
        )
        .route(
            "/v1/sandboxes/{sandbox_id}/shell/close",
            post(close_sandbox_shell),
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
            "/v1/executions/{execution_id}/stream",
            get(stream_execution_output_sse),
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
