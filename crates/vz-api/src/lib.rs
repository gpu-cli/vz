//! OpenAPI/SSE/WebSocket transport adapter for Runtime V2.

#![forbid(unsafe_code)]
#![recursion_limit = "256"]

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_stream::stream;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use tracing::Instrument;
use uuid::Uuid;
use vz_runtime_contract::{
    RuntimeCapabilities, SANDBOX_LABEL_BASE_IMAGE_REF, SANDBOX_LABEL_MAIN_CONTAINER,
    SANDBOX_LABEL_PROJECT_DIR, SANDBOX_LABEL_SPACE_MODE, SANDBOX_SPACE_MODE_REQUIRED,
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
mod observability;
mod openapi_doc;

use daemon_bridge::*;
use handlers::*;
use models::*;
use observability::{ApiObservability, normalize_http_path_for_metrics};
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
    observability: Arc<ApiObservability>,
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
            observability: Arc::new(ApiObservability::default()),
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

async fn observability_middleware(
    State(observability): State<Arc<ApiObservability>>,
    mut request: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    let started_at = Instant::now();
    let method = request.method().as_str().to_string();
    let path = request.uri().path().to_string();
    let request_id = request_id_from_headers(request.headers());
    let request_id_header_present = request.headers().contains_key("x-request-id");

    if !request_id_header_present && let Ok(value) = HeaderValue::try_from(request_id.as_str()) {
        request.headers_mut().insert("x-request-id", value);
    }

    let span = tracing::info_span!(
        "api_http_request",
        request_id = %request_id,
        method = %method,
        path = %path
    );
    let mut response = next.run(request).instrument(span).await;
    let route = normalize_http_path_for_metrics(&path);
    observability.record_http_request(&method, &route, response.status(), started_at.elapsed());

    if !response.headers().contains_key("x-request-id")
        && let Ok(value) = HeaderValue::try_from(request_id.as_str())
    {
        response.headers_mut().insert("x-request-id", value);
    }

    response
}

/// Build the Runtime V2 API router.
pub fn router(config: ApiConfig) -> Router {
    let state: ApiState = config.into();
    let observability = state.observability.clone();
    Router::new()
        .route("/openapi.json", get(openapi_json))
        .route("/metrics", get(metrics_prometheus))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/stacks/apply", post(apply_stack))
        .route("/v1/stacks/teardown", post(teardown_stack))
        .route("/v1/stacks/{stack_name}/status", get(get_stack_status))
        .route("/v1/stacks/{stack_name}/events", get(list_stack_events))
        .route("/v1/stacks/{stack_name}/logs", get(get_stack_logs))
        .route(
            "/v1/stacks/{stack_name}/services/{service_name}/stop",
            post(stop_stack_service),
        )
        .route(
            "/v1/stacks/{stack_name}/services/{service_name}/start",
            post(start_stack_service),
        )
        .route(
            "/v1/stacks/{stack_name}/services/{service_name}/restart",
            post(restart_stack_service),
        )
        .route(
            "/v1/stacks/run-container/create",
            post(create_stack_run_container),
        )
        .route(
            "/v1/stacks/run-container/remove",
            post(remove_stack_run_container),
        )
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
        .route("/v1/checkpoints/import", post(import_checkpoint))
        .route("/v1/checkpoints/diff", get(diff_checkpoints))
        .route("/v1/checkpoints/{checkpoint_id}", get(get_checkpoint))
        .route(
            "/v1/checkpoints/{checkpoint_id}/export",
            post(export_checkpoint),
        )
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
        .route("/v1/images/pull", post(pull_image))
        .route("/v1/images/prune", post(prune_images))
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
        .layer(middleware::from_fn_with_state(
            observability,
            observability_middleware,
        ))
        .with_state(state)
}

#[cfg(test)]
mod tests;
