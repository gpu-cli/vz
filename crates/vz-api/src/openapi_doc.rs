#![allow(dead_code)]

use super::{
    BuildListResponse, BuildResponse, CapabilitiesResponse, CheckpointListResponse,
    CheckpointResponse, ChmodPathRequest, ChownPathRequest, CloseSandboxShellRequest,
    CloseSandboxShellResponse, ContainerListResponse, ContainerResponse, CopyPathRequest,
    CreateCheckpointRequest, CreateContainerRequest, CreateExecutionRequest, CreateSandboxRequest,
    ErrorResponse, EventsResponse, ExecutionListResponse, ExecutionOutputStreamEventPayload,
    ExecutionResponse, FileMutationResponse, ForkCheckpointRequest, ImageListResponse,
    ImageResponse, LeaseListResponse, LeaseResponse, ListFilesRequest, ListFilesResponse,
    MakeDirRequest, MovePathRequest, OpenLeaseRequest, OpenSandboxShellResponse,
    PruneImagesResponse, PullImageRequest, PullImageResponse, ReadFileRequest, ReadFileResponse,
    ReceiptResponse, RemovePathRequest, ResizeExecRequest, RestoreCheckpointResponse,
    SandboxListResponse, SandboxResponse, SignalExecRequest, StartBuildRequest,
    WriteExecStdinRequest, WriteFileRequest, WriteFileResponse,
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
    path = "/v1/sandboxes/{sandbox_id}/shell/open",
    operation_id = "openSandboxShell",
    summary = "Open an interactive shell session for a sandbox",
    params(("sandbox_id" = String, Path, description = "Unique sandbox identifier (sbx-...)")),
    responses(
        (status = 200, description = "Sandbox shell opened", body = OpenSandboxShellResponse),
        (status = 404, description = "Sandbox not found", body = ErrorResponse),
        (status = 409, description = "Sandbox not ready for shell", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn open_sandbox_shell() {}

#[utoipa::path(
    post,
    path = "/v1/sandboxes/{sandbox_id}/shell/close",
    operation_id = "closeSandboxShell",
    summary = "Close an interactive shell session for a sandbox",
    params(
        ("sandbox_id" = String, Path, description = "Unique sandbox identifier (sbx-...)"),
        ("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION),
    ),
    request_body = CloseSandboxShellRequest,
    responses(
        (status = 200, description = "Sandbox shell closed", body = CloseSandboxShellResponse),
        (status = 404, description = "Sandbox or execution not found", body = ErrorResponse),
        (status = 409, description = "Sandbox shell not active", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn close_sandbox_shell() {}

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
    get,
    path = "/v1/executions/{execution_id}/stream",
    operation_id = "streamExecutionOutputSse",
    summary = "Server-Sent Events stream for execution stdout/stderr/exit",
    params((
        "execution_id" = String,
        Path,
        description = "Unique execution identifier (exec-...)"
    )),
    responses((
        status = 200,
        description = "SSE execution output stream",
        content_type = "text/event-stream",
        body = ExecutionOutputStreamEventPayload
    ))
)]
fn stream_execution_output_sse() {}

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
    post,
    path = "/v1/images/pull",
    operation_id = "pullImage",
    summary = "Pull and cache an OCI image",
    params(("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION)),
    request_body = PullImageRequest,
    responses(
        (status = 200, description = "Image pulled", body = PullImageResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn pull_image() {}

#[utoipa::path(
    post,
    path = "/v1/images/prune",
    operation_id = "pruneImages",
    summary = "Prune unreferenced image artifacts",
    params(("Idempotency-Key" = Option<String>, Header, description = IDEMPOTENCY_KEY_DESCRIPTION)),
    responses(
        (status = 200, description = "Image artifacts pruned", body = PruneImagesResponse),
        (status = 500, description = "Internal error", body = ErrorResponse),
    )
)]
fn prune_images() {}

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

#[derive(OpenApi)]
#[openapi(
    info(
        title = "vz Runtime V2 API",
        version = "2.0.0-alpha",
        description = API_DESCRIPTION
    ),
    paths(
        get_openapi_document,
        get_capabilities,
        list_events,
        stream_events_sse,
        stream_events_ws,
        create_sandbox,
        list_sandboxes,
        get_sandbox,
        terminate_sandbox,
        open_sandbox_shell,
        close_sandbox_shell,
        open_lease,
        list_leases,
        get_lease,
        close_lease,
        heartbeat_lease,
        create_execution,
        list_executions,
        get_execution,
        cancel_execution,
        stream_execution_output_sse,
        resize_exec,
        signal_exec,
        write_exec_stdin,
        create_checkpoint,
        list_checkpoints,
        get_checkpoint,
        restore_checkpoint,
        fork_checkpoint,
        list_checkpoint_children,
        list_containers,
        create_container,
        get_container,
        remove_container,
        list_images,
        get_image,
        pull_image,
        prune_images,
        list_builds,
        start_build,
        get_build,
        cancel_build,
        get_receipt,
        read_file,
        write_file,
        list_files,
        make_dir,
        remove_path,
        move_path,
        copy_path,
        chmod_path,
        chown_path,
    ),
    components(schemas(
        crate::ApiEventRecord,
        crate::EventsResponse,
        crate::CapabilitiesResponse,
        crate::CreateSandboxRequest,
        crate::SandboxPayload,
        crate::SandboxResponse,
        crate::SandboxListResponse,
        crate::OpenSandboxShellPayload,
        crate::OpenSandboxShellResponse,
        crate::CloseSandboxShellRequest,
        crate::CloseSandboxShellPayload,
        crate::CloseSandboxShellResponse,
        crate::OpenLeaseRequest,
        crate::LeasePayload,
        crate::LeaseResponse,
        crate::LeaseListResponse,
        crate::ExecutionPtyMode,
        crate::CreateExecutionRequest,
        crate::ExecutionPayload,
        crate::ExecutionResponse,
        crate::ExecutionListResponse,
        crate::ExecutionOutputStreamEventPayload,
        crate::ResizeExecRequest,
        crate::SignalExecRequest,
        crate::WriteExecStdinRequest,
        crate::CreateCheckpointRequest,
        crate::ForkCheckpointRequest,
        crate::CheckpointPayload,
        crate::CheckpointResponse,
        crate::RestoreCheckpointResponse,
        crate::CheckpointListResponse,
        crate::CreateContainerRequest,
        crate::ContainerPayload,
        crate::ContainerResponse,
        crate::ContainerListResponse,
        crate::ImagePayload,
        crate::ImageResponse,
        crate::ImageListResponse,
        crate::PullImageRequest,
        crate::PullImageResponse,
        crate::PruneImagesResponse,
        crate::ReceiptPayload,
        crate::ReceiptResponse,
        crate::StartBuildRequest,
        crate::BuildPayload,
        crate::BuildResponse,
        crate::BuildListResponse,
        crate::ReadFileRequest,
        crate::ReadFileResponse,
        crate::WriteFileRequest,
        crate::WriteFileResponse,
        crate::ListFilesRequest,
        crate::FileEntryPayload,
        crate::ListFilesResponse,
        crate::MakeDirRequest,
        crate::RemovePathRequest,
        crate::MovePathRequest,
        crate::CopyPathRequest,
        crate::ChmodPathRequest,
        crate::ChownPathRequest,
        crate::FileMutationResponse,
        crate::ErrorPayload,
        crate::ErrorResponse,
    ))
)]
struct ApiDoc;

pub fn openapi_document() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}
