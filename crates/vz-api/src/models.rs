use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use vz_runtime_contract::Capability;
use vz_stack::EventRecord;

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct EventsQuery {
    pub(crate) after: Option<i64>,
    pub(crate) limit: Option<usize>,
    pub(crate) scope: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
pub(crate) struct ApiEventRecord {
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
pub(crate) struct EventsResponse {
    pub(crate) request_id: String,
    pub(crate) events: Vec<ApiEventRecord>,
    pub(crate) next_cursor: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct CapabilitiesResponse {
    pub(crate) request_id: String,
    #[schema(value_type = Vec<String>)]
    pub(crate) capabilities: Vec<Capability>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct StackEventsQuery {
    pub(crate) after: Option<i64>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct StackLogsQuery {
    pub(crate) service: Option<String>,
    pub(crate) tail: Option<u32>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct ApplyStackRequest {
    pub(crate) stack_name: String,
    pub(crate) compose_yaml: String,
    pub(crate) compose_dir: String,
    #[serde(default)]
    pub(crate) dry_run: Option<bool>,
    #[serde(default)]
    pub(crate) detach: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct StackServiceStatusPayload {
    pub(crate) service_name: String,
    pub(crate) phase: String,
    pub(crate) ready: bool,
    pub(crate) container_id: String,
    pub(crate) last_error: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ApplyStackPayload {
    pub(crate) stack_name: String,
    pub(crate) changed_actions: u32,
    pub(crate) converged: bool,
    pub(crate) services_ready: u32,
    pub(crate) services_failed: u32,
    pub(crate) services: Vec<StackServiceStatusPayload>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ApplyStackResponse {
    pub(crate) request_id: String,
    pub(crate) stack: ApplyStackPayload,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct TeardownStackRequest {
    pub(crate) stack_name: String,
    #[serde(default)]
    pub(crate) dry_run: Option<bool>,
    #[serde(default)]
    pub(crate) remove_volumes: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct TeardownStackPayload {
    pub(crate) stack_name: String,
    pub(crate) changed_actions: u32,
    pub(crate) removed_volumes: u32,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct TeardownStackResponse {
    pub(crate) request_id: String,
    pub(crate) stack: TeardownStackPayload,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct StackStatusResponse {
    pub(crate) request_id: String,
    pub(crate) stack_name: String,
    pub(crate) services: Vec<StackServiceStatusPayload>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct StackEventsResponse {
    pub(crate) request_id: String,
    pub(crate) events: Vec<ApiEventRecord>,
    pub(crate) next_cursor: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct StackServiceLogPayload {
    pub(crate) service_name: String,
    pub(crate) output: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct StackLogsResponse {
    pub(crate) request_id: String,
    pub(crate) stack_name: String,
    pub(crate) logs: Vec<StackServiceLogPayload>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct StackServiceActionPayload {
    pub(crate) stack_name: String,
    pub(crate) service: StackServiceStatusPayload,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct StackServiceActionResponse {
    pub(crate) request_id: String,
    pub(crate) action: StackServiceActionPayload,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct StackRunContainerRequest {
    pub(crate) stack_name: String,
    pub(crate) service_name: String,
    #[serde(default)]
    pub(crate) run_service_name: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct StackRunContainerPayload {
    pub(crate) stack_name: String,
    pub(crate) service_name: String,
    pub(crate) run_service_name: String,
    pub(crate) container_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct StackRunContainerResponse {
    pub(crate) request_id: String,
    pub(crate) run_container: StackRunContainerPayload,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateSandboxRequest {
    #[serde(default)]
    pub(crate) stack_name: Option<String>,
    #[serde(default)]
    pub(crate) cpus: Option<u8>,
    #[serde(default)]
    pub(crate) memory_mb: Option<u64>,
    #[serde(default)]
    pub(crate) base_image_ref: Option<String>,
    #[serde(default)]
    pub(crate) main_container: Option<String>,
    #[serde(default)]
    pub(crate) labels: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct SandboxPayload {
    pub(crate) sandbox_id: String,
    pub(crate) backend: String,
    pub(crate) state: String,
    pub(crate) cpus: Option<u8>,
    pub(crate) memory_mb: Option<u64>,
    pub(crate) base_image_ref: Option<String>,
    pub(crate) main_container: Option<String>,
    pub(crate) created_at: u64,
    pub(crate) updated_at: u64,
    pub(crate) labels: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct SandboxResponse {
    pub(crate) request_id: String,
    pub(crate) sandbox: SandboxPayload,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct SandboxListResponse {
    pub(crate) request_id: String,
    pub(crate) sandboxes: Vec<SandboxPayload>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct OpenSandboxShellPayload {
    pub(crate) sandbox_id: String,
    pub(crate) container_id: String,
    pub(crate) cmd: Vec<String>,
    pub(crate) args: Vec<String>,
    pub(crate) execution_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct OpenSandboxShellResponse {
    pub(crate) request_id: String,
    pub(crate) shell: OpenSandboxShellPayload,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CloseSandboxShellRequest {
    #[serde(default)]
    pub(crate) execution_id: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct CloseSandboxShellPayload {
    pub(crate) sandbox_id: String,
    pub(crate) execution_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct CloseSandboxShellResponse {
    pub(crate) request_id: String,
    pub(crate) shell: CloseSandboxShellPayload,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ExecutionOutputStreamEventPayload {
    pub(crate) sequence: u64,
    pub(crate) event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

// ── Lease types ──

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct OpenLeaseRequest {
    pub(crate) sandbox_id: String,
    #[serde(default)]
    pub(crate) ttl_secs: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct LeasePayload {
    pub(crate) lease_id: String,
    pub(crate) sandbox_id: String,
    pub(crate) ttl_secs: u64,
    pub(crate) last_heartbeat_at: u64,
    pub(crate) state: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct LeaseResponse {
    pub(crate) request_id: String,
    pub(crate) lease: LeasePayload,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct LeaseListResponse {
    pub(crate) request_id: String,
    pub(crate) leases: Vec<LeasePayload>,
}

// ── Execution types ──

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionPtyMode {
    Inherit,
    Enabled,
    Disabled,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateExecutionRequest {
    pub(crate) container_id: String,
    pub(crate) cmd: Vec<String>,
    #[serde(default)]
    pub(crate) args: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) env_override: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub(crate) pty_mode: Option<ExecutionPtyMode>,
    #[serde(default)]
    pub(crate) timeout_secs: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ExecutionPayload {
    pub(crate) execution_id: String,
    pub(crate) container_id: String,
    pub(crate) state: String,
    pub(crate) exit_code: Option<i32>,
    pub(crate) started_at: Option<u64>,
    pub(crate) ended_at: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ExecutionResponse {
    pub(crate) request_id: String,
    pub(crate) execution: ExecutionPayload,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ExecutionListResponse {
    pub(crate) request_id: String,
    pub(crate) executions: Vec<ExecutionPayload>,
}

/// Request body for `POST /v1/executions/{execution_id}/resize`.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct ResizeExecRequest {
    pub(crate) cols: u16,
    pub(crate) rows: u16,
}

/// Request body for `POST /v1/executions/{execution_id}/signal`.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct SignalExecRequest {
    pub(crate) signal: String,
}

/// Request body for `POST /v1/executions/{execution_id}/stdin`.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct WriteExecStdinRequest {
    pub(crate) data: String,
}

// ── Checkpoint types ──

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateCheckpointRequest {
    pub(crate) sandbox_id: String,
    #[serde(default)]
    pub(crate) class: Option<String>,
    #[serde(default)]
    pub(crate) compatibility_fingerprint: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct ForkCheckpointRequest {
    #[serde(default)]
    pub(crate) new_sandbox_id: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct CheckpointPayload {
    pub(crate) checkpoint_id: String,
    pub(crate) sandbox_id: String,
    pub(crate) parent_checkpoint_id: Option<String>,
    pub(crate) class: String,
    pub(crate) state: String,
    pub(crate) compatibility_fingerprint: String,
    pub(crate) created_at: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct CheckpointResponse {
    pub(crate) request_id: String,
    pub(crate) checkpoint: CheckpointPayload,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct RestoreCheckpointResponse {
    pub(crate) request_id: String,
    pub(crate) checkpoint: CheckpointPayload,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) compatibility_fingerprint: Option<String>,
    pub(crate) restore_note: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct CheckpointListResponse {
    pub(crate) request_id: String,
    pub(crate) checkpoints: Vec<CheckpointPayload>,
}

// ── Container types ──

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateContainerRequest {
    pub(crate) sandbox_id: String,
    #[serde(default)]
    pub(crate) image_digest: Option<String>,
    #[serde(default)]
    pub(crate) cmd: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) env: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub(crate) cwd: Option<String>,
    #[serde(default)]
    pub(crate) user: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ContainerPayload {
    pub(crate) container_id: String,
    pub(crate) sandbox_id: String,
    pub(crate) image_digest: String,
    pub(crate) state: String,
    pub(crate) created_at: u64,
    pub(crate) started_at: Option<u64>,
    pub(crate) ended_at: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ContainerResponse {
    pub(crate) request_id: String,
    pub(crate) container: ContainerPayload,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ContainerListResponse {
    pub(crate) request_id: String,
    pub(crate) containers: Vec<ContainerPayload>,
}

// ── Image types ──

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ImagePayload {
    pub(crate) image_ref: String,
    pub(crate) resolved_digest: String,
    pub(crate) platform: String,
    pub(crate) source_registry: String,
    pub(crate) pulled_at: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ImageResponse {
    pub(crate) request_id: String,
    pub(crate) image: ImagePayload,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ImageListResponse {
    pub(crate) request_id: String,
    pub(crate) images: Vec<ImagePayload>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct PullImageRequest {
    pub(crate) image_ref: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct PullImageResponse {
    pub(crate) request_id: String,
    pub(crate) image: ImagePayload,
    pub(crate) receipt_id: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct PruneImagesResponse {
    pub(crate) request_id: String,
    pub(crate) removed_refs: u64,
    pub(crate) removed_manifests: u64,
    pub(crate) removed_configs: u64,
    pub(crate) removed_layer_dirs: u64,
    pub(crate) remaining_images: u64,
    pub(crate) receipt_id: Option<String>,
}

// ── Receipt types ──

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ReceiptPayload {
    pub(crate) receipt_id: String,
    pub(crate) operation: String,
    pub(crate) entity_id: String,
    pub(crate) entity_type: String,
    pub(crate) request_id: String,
    pub(crate) status: String,
    pub(crate) created_at: u64,
    pub(crate) metadata: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ReceiptResponse {
    pub(crate) request_id: String,
    pub(crate) receipt: ReceiptPayload,
}

// ── Build types ──

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct StartBuildRequest {
    pub(crate) sandbox_id: String,
    pub(crate) context: String,
    #[serde(default)]
    pub(crate) dockerfile: Option<String>,
    #[serde(default)]
    pub(crate) args: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct BuildPayload {
    pub(crate) build_id: String,
    pub(crate) sandbox_id: String,
    pub(crate) state: String,
    pub(crate) result_digest: Option<String>,
    pub(crate) started_at: u64,
    pub(crate) ended_at: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct BuildResponse {
    pub(crate) request_id: String,
    pub(crate) build: BuildPayload,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct BuildListResponse {
    pub(crate) request_id: String,
    pub(crate) builds: Vec<BuildPayload>,
}

// ── File types ──

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct ReadFileRequest {
    pub(crate) sandbox_id: String,
    pub(crate) path: String,
    #[serde(default)]
    pub(crate) offset: Option<u64>,
    #[serde(default)]
    pub(crate) limit: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ReadFileResponse {
    pub(crate) request_id: String,
    /// Base64-encoded file content bytes.
    pub(crate) data_base64: String,
    pub(crate) truncated: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct WriteFileRequest {
    pub(crate) sandbox_id: String,
    pub(crate) path: String,
    /// Base64-encoded bytes to write.
    pub(crate) data_base64: String,
    #[serde(default)]
    pub(crate) append: Option<bool>,
    #[serde(default)]
    pub(crate) create_parents: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct WriteFileResponse {
    pub(crate) request_id: String,
    pub(crate) bytes_written: u64,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct ListFilesRequest {
    pub(crate) sandbox_id: String,
    #[serde(default)]
    pub(crate) path: Option<String>,
    #[serde(default)]
    pub(crate) recursive: Option<bool>,
    #[serde(default)]
    pub(crate) limit: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct FileEntryPayload {
    pub(crate) path: String,
    pub(crate) is_dir: bool,
    pub(crate) size: u64,
    pub(crate) modified_at: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ListFilesResponse {
    pub(crate) request_id: String,
    pub(crate) entries: Vec<FileEntryPayload>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct MakeDirRequest {
    pub(crate) sandbox_id: String,
    pub(crate) path: String,
    #[serde(default)]
    pub(crate) parents: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct RemovePathRequest {
    pub(crate) sandbox_id: String,
    pub(crate) path: String,
    #[serde(default)]
    pub(crate) recursive: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct MovePathRequest {
    pub(crate) sandbox_id: String,
    pub(crate) src_path: String,
    pub(crate) dst_path: String,
    #[serde(default)]
    pub(crate) overwrite: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CopyPathRequest {
    pub(crate) sandbox_id: String,
    pub(crate) src_path: String,
    pub(crate) dst_path: String,
    #[serde(default)]
    pub(crate) overwrite: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct ChmodPathRequest {
    pub(crate) sandbox_id: String,
    pub(crate) path: String,
    pub(crate) mode: u32,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct ChownPathRequest {
    pub(crate) sandbox_id: String,
    pub(crate) path: String,
    pub(crate) uid: u32,
    pub(crate) gid: u32,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct FileMutationResponse {
    pub(crate) request_id: String,
    pub(crate) path: String,
    pub(crate) status: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ErrorPayload {
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) request_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ErrorResponse {
    pub(crate) error: ErrorPayload,
}

#[derive(Debug, Serialize)]
pub(crate) struct SerializationErrorPayload {
    #[serde(rename = "type")]
    pub(crate) event_type: &'static str,
}

pub(crate) fn serialization_error_value() -> serde_json::Value {
    match serde_json::to_value(SerializationErrorPayload {
        event_type: "serialization_error",
    }) {
        Ok(value) => value,
        Err(_) => serde_json::Value::Object(serde_json::Map::new()),
    }
}

pub(crate) fn empty_json_object_value() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}
