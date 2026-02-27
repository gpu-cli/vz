#![allow(dead_code)]

use super::{
    BuildListResponse, BuildResponse, CapabilitiesResponse, CheckpointListResponse,
    CheckpointResponse, ChmodPathRequest, ChownPathRequest, ContainerListResponse,
    ContainerResponse, CopyPathRequest, CreateCheckpointRequest, CreateContainerRequest,
    CreateExecutionRequest, CreateSandboxRequest, ErrorResponse, EventsResponse,
    ExecutionListResponse, ExecutionResponse, FileMutationResponse, ForkCheckpointRequest,
    ImageListResponse, ImageResponse, LeaseListResponse, LeaseResponse, ListFilesRequest,
    ListFilesResponse, MakeDirRequest, MovePathRequest, OpenLeaseRequest, ReadFileRequest,
    ReadFileResponse, ReceiptResponse, RemovePathRequest, ResizeExecRequest,
    RestoreCheckpointResponse, SandboxListResponse, SandboxResponse, SignalExecRequest,
    StartBuildRequest, WriteExecStdinRequest, WriteFileRequest, WriteFileResponse,
};
use utoipa::OpenApi;

const API_DESCRIPTION: &str = "Container runtime API with sandbox lifecycle, lease management, execution dispatch, checkpoint/restore, and real-time event streaming via SSE and WebSocket.";
const IDEMPOTENCY_KEY_DESCRIPTION: &str = "Client-supplied idempotency key. Repeated requests with the same key and body return the cached response. Same key with a different body returns 409 Conflict.";
const REQUEST_ID_DESCRIPTION: &str =
    "Client-supplied request identifier echoed back in every response. Auto-generated when absent.";

#[utoipa::path(
    get,
    path = "/openapi.json",
    operation_id = "getOpenApiDocument",
    summary = "Return this OpenAPI 3.1 schema document",
    responses((status = 200, description = "OpenAPI 3.1 JSON document"))
)]
fn get_openapi_document() {}

#[utoipa::path(
    get,
    path = "/v1/capabilities",
    operation_id = "getCapabilities",
    summary = "List runtime capabilities advertised by this API surface",
    responses((status = 200, description = "Capabilities list", body = CapabilitiesResponse))
)]
fn get_capabilities() {}

#[utoipa::path(
    get,
    path = "/v1/events/{stack_name}",
    operation_id = "listEvents",
    summary = "Paginated event log for a stack",
    params(
        ("stack_name" = String, Path, description = "Stack identifier for event filtering"),
        ("after" = Option<i64>, Query, description = "Return events with id strictly greater than this cursor"),
        ("limit" = Option<usize>, Query, description = "Maximum number of events to return (1..1000)"),
        ("scope" = Option<String>, Query, description = "Optional event scope filter"),
    ),
    responses(
        (status = 200, description = "Paginated event list", body = EventsResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn list_events() {}

#[utoipa::path(
    get,
    path = "/v1/events/{stack_name}/stream",
    operation_id = "streamEventsSse",
    summary = "Server-Sent Events stream of stack events",
    params(
        ("stack_name" = String, Path, description = "Stack identifier for event filtering"),
        ("after" = Option<i64>, Query, description = "Return events with id strictly greater than this cursor"),
        ("limit" = Option<usize>, Query, description = "Maximum number of events to return (1..1000)"),
        ("scope" = Option<String>, Query, description = "Optional event scope filter"),
    ),
    responses((
        status = 200,
        description = "SSE event stream",
        content_type = "text/event-stream",
        body = String
    ))
)]
fn stream_events_sse() {}

#[utoipa::path(
    get,
    path = "/v1/events/{stack_name}/ws",
    operation_id = "streamEventsWs",
    summary = "WebSocket stream of stack events",
    params(
        ("stack_name" = String, Path, description = "Stack identifier for event filtering"),
        ("after" = Option<i64>, Query, description = "Return events with id strictly greater than this cursor"),
        ("limit" = Option<usize>, Query, description = "Maximum number of events to return (1..1000)"),
        ("scope" = Option<String>, Query, description = "Optional event scope filter"),
    ),
    responses((status = 101, description = "WebSocket upgrade"))
)]
fn stream_events_ws() {}

#[utoipa::path(
    post,
    path = "/v1/sandboxes",
    operation_id = "createSandbox",
    summary = "Create a new sandbox",
    params(
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
        ("X-Request-Id" = Option<String>, Header, description = REQUEST_ID_DESCRIPTION),
    ),
    request_body = CreateSandboxRequest,
    responses(
        (status = 201, description = "Sandbox created", body = SandboxResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 409, description = "Idempotency conflict", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn create_sandbox() {}

#[utoipa::path(
    get,
    path = "/v1/sandboxes",
    operation_id = "listSandboxes",
    summary = "List all sandboxes",
    responses(
        (status = 200, description = "Sandbox list", body = SandboxListResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn list_sandboxes() {}

#[utoipa::path(
    get,
    path = "/v1/sandboxes/{sandbox_id}",
    operation_id = "getSandbox",
    summary = "Get a sandbox by ID",
    params(("sandbox_id" = String, Path, description = "Unique sandbox identifier (sbx-...)")),
    responses(
        (status = 200, description = "Sandbox details", body = SandboxResponse),
        (status = 404, description = "Sandbox not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn get_sandbox() {}

#[utoipa::path(
    delete,
    path = "/v1/sandboxes/{sandbox_id}",
    operation_id = "terminateSandbox",
    summary = "Terminate a sandbox",
    params(
        ("sandbox_id" = String, Path, description = "Unique sandbox identifier (sbx-...)"),
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
    ),
    responses(
        (status = 200, description = "Sandbox terminated", body = SandboxResponse),
        (status = 404, description = "Sandbox not found", body = ErrorResponse),
        (status = 409, description = "Sandbox in invalid state", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn terminate_sandbox() {}

#[utoipa::path(
    post,
    path = "/v1/leases",
    operation_id = "openLease",
    summary = "Open a lease for a sandbox",
    params(
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
        ("X-Request-Id" = Option<String>, Header, description = REQUEST_ID_DESCRIPTION),
    ),
    request_body = OpenLeaseRequest,
    responses(
        (status = 201, description = "Lease opened", body = LeaseResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 409, description = "Lease conflict", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn open_lease() {}

#[utoipa::path(
    get,
    path = "/v1/leases",
    operation_id = "listLeases",
    summary = "List all leases",
    responses(
        (status = 200, description = "Lease list", body = LeaseListResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn list_leases() {}

#[utoipa::path(
    get,
    path = "/v1/leases/{lease_id}",
    operation_id = "getLease",
    summary = "Get lease details",
    params(("lease_id" = String, Path, description = "Unique lease identifier (ls-...)")),
    responses(
        (status = 200, description = "Lease details", body = LeaseResponse),
        (status = 404, description = "Lease not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn get_lease() {}

#[utoipa::path(
    delete,
    path = "/v1/leases/{lease_id}",
    operation_id = "closeLease",
    summary = "Close a lease",
    params(
        ("lease_id" = String, Path, description = "Unique lease identifier (ls-...)"),
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
    ),
    responses(
        (status = 200, description = "Lease closed", body = LeaseResponse),
        (status = 404, description = "Lease not found", body = ErrorResponse),
        (status = 409, description = "Lease in invalid state", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn close_lease() {}

#[utoipa::path(
    post,
    path = "/v1/leases/{lease_id}/heartbeat",
    operation_id = "heartbeatLease",
    summary = "Heartbeat an active lease",
    params(
        ("lease_id" = String, Path, description = "Unique lease identifier (ls-...)"),
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
    ),
    responses(
        (status = 200, description = "Lease heartbeat accepted", body = LeaseResponse),
        (status = 404, description = "Lease not found", body = ErrorResponse),
        (status = 409, description = "Lease in invalid state", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn heartbeat_lease() {}

#[utoipa::path(
    post,
    path = "/v1/executions",
    operation_id = "createExecution",
    summary = "Create an execution",
    params(
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
        ("X-Request-Id" = Option<String>, Header, description = REQUEST_ID_DESCRIPTION),
    ),
    request_body = CreateExecutionRequest,
    responses(
        (status = 201, description = "Execution created", body = ExecutionResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn create_execution() {}

#[utoipa::path(
    get,
    path = "/v1/executions",
    operation_id = "listExecutions",
    summary = "List all executions",
    responses(
        (status = 200, description = "Execution list", body = ExecutionListResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn list_executions() {}

#[utoipa::path(
    get,
    path = "/v1/executions/{execution_id}",
    operation_id = "getExecution",
    summary = "Get execution details",
    params((
        "execution_id" = String,
        Path,
        description = "Unique execution identifier (exec-...)"
    )),
    responses(
        (status = 200, description = "Execution details", body = ExecutionResponse),
        (status = 404, description = "Execution not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn get_execution() {}

#[utoipa::path(
    delete,
    path = "/v1/executions/{execution_id}",
    operation_id = "cancelExecution",
    summary = "Cancel execution",
    params(
        ("execution_id" = String, Path, description = "Unique execution identifier (exec-...)"),
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
    ),
    responses(
        (status = 200, description = "Execution canceled", body = ExecutionResponse),
        (status = 404, description = "Execution not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn cancel_execution() {}

#[utoipa::path(
    post,
    path = "/v1/executions/{execution_id}/resize",
    operation_id = "resizeExec",
    summary = "Resize an execution PTY",
    params((
        "execution_id" = String,
        Path,
        description = "Unique execution identifier (exec-...)"
    )),
    request_body = ResizeExecRequest,
    responses(
        (status = 200, description = "Execution resized", body = ExecutionResponse),
        (status = 400, description = "Invalid resize parameters", body = ErrorResponse),
        (status = 404, description = "Execution not found", body = ErrorResponse),
        (status = 409, description = "Execution in invalid state", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn resize_exec() {}

#[utoipa::path(
    post,
    path = "/v1/executions/{execution_id}/signal",
    operation_id = "signalExec",
    summary = "Send signal to an execution",
    params((
        "execution_id" = String,
        Path,
        description = "Unique execution identifier (exec-...)"
    )),
    request_body = SignalExecRequest,
    responses(
        (status = 200, description = "Signal delivered", body = ExecutionResponse),
        (status = 400, description = "Invalid signal request", body = ErrorResponse),
        (status = 404, description = "Execution not found", body = ErrorResponse),
        (status = 409, description = "Execution in invalid state", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn signal_exec() {}

#[utoipa::path(
    post,
    path = "/v1/executions/{execution_id}/stdin",
    operation_id = "writeExecStdin",
    summary = "Write stdin data to an execution session",
    params((
        "execution_id" = String,
        Path,
        description = "Unique execution identifier (exec-...)"
    )),
    request_body = WriteExecStdinRequest,
    responses(
        (status = 200, description = "Stdin write accepted", body = ExecutionResponse),
        (status = 400, description = "Invalid stdin request", body = ErrorResponse),
        (status = 404, description = "Execution/session not found", body = ErrorResponse),
        (status = 409, description = "Execution in invalid state", body = ErrorResponse),
        (status = 501, description = "Backend does not support execution stdin", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn write_exec_stdin() {}

#[utoipa::path(
    post,
    path = "/v1/checkpoints",
    operation_id = "createCheckpoint",
    summary = "Create a checkpoint",
    params(
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
        ("X-Request-Id" = Option<String>, Header, description = REQUEST_ID_DESCRIPTION),
    ),
    request_body = CreateCheckpointRequest,
    responses(
        (status = 201, description = "Checkpoint created", body = CheckpointResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 409, description = "Checkpoint conflict", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn create_checkpoint() {}

#[utoipa::path(
    get,
    path = "/v1/checkpoints",
    operation_id = "listCheckpoints",
    summary = "List all checkpoints",
    responses(
        (status = 200, description = "Checkpoint list", body = CheckpointListResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn list_checkpoints() {}

#[utoipa::path(
    get,
    path = "/v1/checkpoints/{checkpoint_id}",
    operation_id = "getCheckpoint",
    summary = "Get checkpoint details",
    params((
        "checkpoint_id" = String,
        Path,
        description = "Unique checkpoint identifier (ckpt-...)"
    )),
    responses(
        (status = 200, description = "Checkpoint details", body = CheckpointResponse),
        (status = 404, description = "Checkpoint not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn get_checkpoint() {}

#[utoipa::path(
    post,
    path = "/v1/checkpoints/{checkpoint_id}/restore",
    operation_id = "restoreCheckpoint",
    summary = "Restore a checkpoint",
    params(
        ("checkpoint_id" = String, Path, description = "Unique checkpoint identifier (ckpt-...)"),
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
    ),
    responses(
        (status = 200, description = "Checkpoint restored", body = RestoreCheckpointResponse),
        (status = 404, description = "Checkpoint not found", body = ErrorResponse),
        (status = 409, description = "Checkpoint not ready", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn restore_checkpoint() {}

#[utoipa::path(
    post,
    path = "/v1/checkpoints/{checkpoint_id}/fork",
    operation_id = "forkCheckpoint",
    summary = "Fork a checkpoint",
    params(
        ("checkpoint_id" = String, Path, description = "Unique checkpoint identifier (ckpt-...)"),
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
    ),
    request_body = ForkCheckpointRequest,
    responses(
        (status = 201, description = "Checkpoint forked", body = CheckpointResponse),
        (status = 404, description = "Parent checkpoint not found", body = ErrorResponse),
        (status = 409, description = "Parent checkpoint not in ready state", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn fork_checkpoint() {}

#[utoipa::path(
    get,
    path = "/v1/checkpoints/{checkpoint_id}/children",
    operation_id = "listCheckpointChildren",
    summary = "List child checkpoints forked from a parent checkpoint",
    params((
        "checkpoint_id" = String,
        Path,
        description = "Unique checkpoint identifier (ckpt-...)"
    )),
    responses(
        (status = 200, description = "List of child checkpoints", body = CheckpointListResponse),
        (status = 404, description = "Parent checkpoint not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn list_checkpoint_children() {}

#[utoipa::path(
    get,
    path = "/v1/containers",
    operation_id = "listContainers",
    summary = "List all containers",
    responses((status = 200, description = "List of containers", body = ContainerListResponse))
)]
fn list_containers() {}

#[utoipa::path(
    post,
    path = "/v1/containers",
    operation_id = "createContainer",
    summary = "Create a new container",
    request_body = CreateContainerRequest,
    responses((status = 201, description = "Container created", body = ContainerResponse))
)]
fn create_container() {}

#[utoipa::path(
    get,
    path = "/v1/containers/{container_id}",
    operation_id = "getContainer",
    summary = "Get container details",
    params((
        "container_id" = String,
        Path,
        description = "Unique container identifier (ctr-...)"
    )),
    responses(
        (status = 200, description = "Container details", body = ContainerResponse),
        (status = 404, description = "Container not found", body = ErrorResponse),
    )
)]
fn get_container() {}

#[utoipa::path(
    delete,
    path = "/v1/containers/{container_id}",
    operation_id = "removeContainer",
    summary = "Remove a container",
    params((
        "container_id" = String,
        Path,
        description = "Unique container identifier (ctr-...)"
    )),
    responses(
        (status = 200, description = "Container removed", body = ContainerResponse),
        (status = 404, description = "Container not found", body = ErrorResponse),
    )
)]
fn remove_container() {}

#[utoipa::path(
    get,
    path = "/v1/images",
    operation_id = "listImages",
    summary = "List all cached images",
    responses((status = 200, description = "List of images", body = ImageListResponse))
)]
fn list_images() {}

#[utoipa::path(
    get,
    path = "/v1/images/{image_ref}",
    operation_id = "getImage",
    summary = "Get image details by reference",
    params((
        "image_ref" = String,
        Path,
        description = "OCI image reference or digest"
    )),
    responses(
        (status = 200, description = "Image details", body = ImageResponse),
        (status = 404, description = "Image not found", body = ErrorResponse),
    )
)]
fn get_image() {}

#[utoipa::path(
    get,
    path = "/v1/builds",
    operation_id = "listBuilds",
    summary = "List all builds",
    responses((status = 200, description = "List of builds", body = BuildListResponse))
)]
fn list_builds() {}

#[utoipa::path(
    post,
    path = "/v1/builds",
    operation_id = "startBuild",
    summary = "Start a new build",
    request_body = StartBuildRequest,
    responses((status = 201, description = "Build started", body = BuildResponse))
)]
fn start_build() {}

#[utoipa::path(
    get,
    path = "/v1/builds/{build_id}",
    operation_id = "getBuild",
    summary = "Get build details",
    params(("build_id" = String, Path, description = "Unique build identifier (bld-...)")),
    responses(
        (status = 200, description = "Build details", body = BuildResponse),
        (status = 404, description = "Build not found", body = ErrorResponse),
    )
)]
fn get_build() {}

#[utoipa::path(
    delete,
    path = "/v1/builds/{build_id}",
    operation_id = "cancelBuild",
    summary = "Cancel a running build",
    params(
        ("build_id" = String, Path, description = "Unique build identifier (bld-...)"),
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
    ),
    responses(
        (status = 200, description = "Build canceled", body = BuildResponse),
        (status = 404, description = "Build not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn cancel_build() {}

#[utoipa::path(
    get,
    path = "/v1/receipts/{receipt_id}",
    operation_id = "getReceipt",
    summary = "Get receipt details",
    params((
        "receipt_id" = String,
        Path,
        description = "Unique receipt identifier (rcp-...)"
    )),
    responses(
        (status = 200, description = "Receipt details", body = ReceiptResponse),
        (status = 404, description = "Receipt not found", body = ErrorResponse),
    )
)]
fn get_receipt() {}

#[utoipa::path(
    post,
    path = "/v1/files/read",
    operation_id = "readFile",
    summary = "Read file content from a sandbox filesystem",
    request_body = ReadFileRequest,
    responses(
        (status = 200, description = "File content", body = ReadFileResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 404, description = "Sandbox or path not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn read_file() {}

#[utoipa::path(
    post,
    path = "/v1/files/write",
    operation_id = "writeFile",
    summary = "Write file content into a sandbox filesystem",
    params(
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
        ("X-Request-Id" = Option<String>, Header, description = REQUEST_ID_DESCRIPTION),
    ),
    request_body = WriteFileRequest,
    responses(
        (status = 200, description = "File written", body = WriteFileResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 404, description = "Sandbox or path not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn write_file() {}

#[utoipa::path(
    post,
    path = "/v1/files/list",
    operation_id = "listFiles",
    summary = "List files under a sandbox path",
    request_body = ListFilesRequest,
    responses(
        (status = 200, description = "File list", body = ListFilesResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 404, description = "Sandbox or path not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn list_files() {}

#[utoipa::path(
    post,
    path = "/v1/files/mkdir",
    operation_id = "makeDir",
    summary = "Create a directory in a sandbox filesystem",
    params(
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
        ("X-Request-Id" = Option<String>, Header, description = REQUEST_ID_DESCRIPTION),
    ),
    request_body = MakeDirRequest,
    responses(
        (status = 200, description = "Directory created", body = FileMutationResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 404, description = "Sandbox or path not found", body = ErrorResponse),
        (status = 409, description = "State conflict", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn make_dir() {}

#[utoipa::path(
    post,
    path = "/v1/files/remove",
    operation_id = "removePath",
    summary = "Remove a file or directory in a sandbox filesystem",
    params(
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
        ("X-Request-Id" = Option<String>, Header, description = REQUEST_ID_DESCRIPTION),
    ),
    request_body = RemovePathRequest,
    responses(
        (status = 200, description = "Path removed", body = FileMutationResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 404, description = "Sandbox or path not found", body = ErrorResponse),
        (status = 409, description = "State conflict", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn remove_path() {}

#[utoipa::path(
    post,
    path = "/v1/files/move",
    operation_id = "movePath",
    summary = "Move or rename a path in a sandbox filesystem",
    params(
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
        ("X-Request-Id" = Option<String>, Header, description = REQUEST_ID_DESCRIPTION),
    ),
    request_body = MovePathRequest,
    responses(
        (status = 200, description = "Path moved", body = FileMutationResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 404, description = "Sandbox or path not found", body = ErrorResponse),
        (status = 409, description = "State conflict", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn move_path() {}

#[utoipa::path(
    post,
    path = "/v1/files/copy",
    operation_id = "copyPath",
    summary = "Copy a path in a sandbox filesystem",
    params(
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
        ("X-Request-Id" = Option<String>, Header, description = REQUEST_ID_DESCRIPTION),
    ),
    request_body = CopyPathRequest,
    responses(
        (status = 200, description = "Path copied", body = FileMutationResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 404, description = "Sandbox or path not found", body = ErrorResponse),
        (status = 409, description = "State conflict", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn copy_path() {}

#[utoipa::path(
    post,
    path = "/v1/files/chmod",
    operation_id = "chmodPath",
    summary = "Change mode bits for a sandbox path",
    params(
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
        ("X-Request-Id" = Option<String>, Header, description = REQUEST_ID_DESCRIPTION),
    ),
    request_body = ChmodPathRequest,
    responses(
        (status = 200, description = "Mode updated", body = FileMutationResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 404, description = "Sandbox or path not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn chmod_path() {}

#[utoipa::path(
    post,
    path = "/v1/files/chown",
    operation_id = "chownPath",
    summary = "Change owner/group for a sandbox path",
    params(
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
        ("X-Request-Id" = Option<String>, Header, description = REQUEST_ID_DESCRIPTION),
    ),
    request_body = ChownPathRequest,
    responses(
        (status = 200, description = "Owner updated", body = FileMutationResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 404, description = "Sandbox or path not found", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn chown_path() {}

include!(concat!(env!("OUT_DIR"), "/openapi_doc_generated.rs"));

pub fn openapi_document() -> serde_json::Value {
    match serde_json::to_value(ApiDoc::openapi()) {
        Ok(value) => value,
        Err(_) => serde_json::Value::Object(serde_json::Map::new()),
    }
}
