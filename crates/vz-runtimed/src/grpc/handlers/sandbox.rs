use super::super::*;
use std::path::PathBuf;
use vz_runtime_contract::{RuntimeBackend, StackResourceHint, StackVolumeMount};
use vz_runtime_proto::runtime_v2::container_service_server::ContainerService as _;
use vz_runtime_proto::runtime_v2::execution_service_server::ExecutionService as _;

#[derive(Clone)]
pub(in crate::grpc) struct SandboxServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl SandboxServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

type OpenSandboxShellEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::OpenSandboxShellEvent, Status>>;
type CloseSandboxShellEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::CloseSandboxShellEvent, Status>>;
type CreateSandboxEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::CreateSandboxEvent, Status>>;
type TerminateSandboxEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::TerminateSandboxEvent, Status>>;

fn sandbox_shell_stream_from_events<T>(
    events: Vec<Result<T, Status>>,
) -> tokio_stream::wrappers::ReceiverStream<Result<T, Status>>
where
    T: Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(events.len().max(1));
    for event in events {
        if tx.try_send(event).is_err() {
            break;
        }
    }
    drop(tx);
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

fn sandbox_stream_response<T>(
    events: Vec<Result<T, Status>>,
    receipt_id: Option<&str>,
) -> Response<tokio_stream::wrappers::ReceiverStream<Result<T, Status>>>
where
    T: Send + 'static,
{
    let mut response = Response::new(sandbox_shell_stream_from_events(events));
    if let Some(receipt_id) = receipt_id
        && !receipt_id.trim().is_empty()
        && let Ok(value) = MetadataValue::try_from(receipt_id)
    {
        response.metadata_mut().insert("x-receipt-id", value);
    }
    response
}

fn create_sandbox_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::CreateSandboxEvent {
    runtime_v2::CreateSandboxEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::create_sandbox_event::Payload::Progress(
            runtime_v2::SandboxLifecycleProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn create_sandbox_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::SandboxResponse,
    receipt_id: &str,
) -> runtime_v2::CreateSandboxEvent {
    runtime_v2::CreateSandboxEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::create_sandbox_event::Payload::Completion(
            runtime_v2::CreateSandboxCompletion {
                response: Some(response),
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

fn terminate_sandbox_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::TerminateSandboxEvent {
    runtime_v2::TerminateSandboxEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::terminate_sandbox_event::Payload::Progress(
            runtime_v2::SandboxLifecycleProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn terminate_sandbox_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::SandboxResponse,
    receipt_id: &str,
) -> runtime_v2::TerminateSandboxEvent {
    runtime_v2::TerminateSandboxEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::terminate_sandbox_event::Payload::Completion(
            runtime_v2::TerminateSandboxCompletion {
                response: Some(response),
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

fn open_sandbox_shell_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::OpenSandboxShellEvent {
    runtime_v2::OpenSandboxShellEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::open_sandbox_shell_event::Payload::Progress(
            runtime_v2::SandboxShellProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn open_sandbox_shell_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::OpenSandboxShellResponse,
) -> runtime_v2::OpenSandboxShellEvent {
    runtime_v2::OpenSandboxShellEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::open_sandbox_shell_event::Payload::Completion(
            response,
        )),
    }
}

fn close_sandbox_shell_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::CloseSandboxShellEvent {
    runtime_v2::CloseSandboxShellEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::close_sandbox_shell_event::Payload::Progress(
            runtime_v2::SandboxShellProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn close_sandbox_shell_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::CloseSandboxShellResponse,
) -> runtime_v2::CloseSandboxShellEvent {
    runtime_v2::CloseSandboxShellEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::close_sandbox_shell_event::Payload::Completion(
            response,
        )),
    }
}

async fn terminate_runtime_sandbox_resources(
    daemon: Arc<RuntimeDaemon>,
    sandbox_id: &str,
    request_id: &str,
) -> Result<(), Status> {
    let sandbox_id_owned = sandbox_id.to_string();
    let bridge_result = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("failed to initialize runtime bridge: {error}"))?;
        Ok::<_, String>(runtime.block_on(daemon.manager().terminate_sandbox(&sandbox_id_owned)))
    })
    .await
    .map_err(|join_error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "runtime bridge join failure while terminating sandbox {sandbox_id}: {join_error}"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    let runtime_result = bridge_result.map_err(|bridge_error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "runtime bridge initialization failed while terminating sandbox {sandbox_id}: {bridge_error}"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    match runtime_result {
        Ok(()) => Ok(()),
        Err(error) if runtime_shutdown_error_is_not_active(&error, sandbox_id) => Ok(()),
        Err(error) => Err(status_from_machine_error(MachineError::new(
            error.machine_code(),
            format!("failed to terminate runtime resources for sandbox {sandbox_id}: {error}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))),
    }
}

fn sandbox_workspace_volume_mount(
    labels: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<Option<StackVolumeMount>, Status> {
    let Some(project_dir) = labels
        .get("project_dir")
        .and_then(|value| normalize_optional_wire_field(value))
    else {
        return Ok(None);
    };

    let host_path = PathBuf::from(project_dir.trim());
    if !host_path.is_absolute() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "sandbox label `project_dir` must be an absolute path: {}",
                host_path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    if !host_path.exists() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "sandbox label `project_dir` does not exist: {}",
                host_path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    if !host_path.is_dir() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "sandbox label `project_dir` must reference a directory: {}",
                host_path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    Ok(Some(StackVolumeMount {
        tag: "vz-mount-0".to_string(),
        host_path,
        read_only: false,
    }))
}

async fn boot_runtime_sandbox_resources(
    daemon: Arc<RuntimeDaemon>,
    sandbox_id: &str,
    cpus: Option<u8>,
    memory_mb: Option<u64>,
    labels: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<(), Status> {
    let mut volume_mounts = Vec::new();
    if let Some(workspace_mount) = sandbox_workspace_volume_mount(labels, request_id)? {
        volume_mounts.push(workspace_mount);
    }

    let resources = StackResourceHint {
        cpus,
        memory_mb,
        volume_mounts,
        disk_image_path: None,
    };

    match daemon
        .manager()
        .backend()
        .boot_shared_vm(sandbox_id, Vec::new(), resources)
        .await
    {
        Ok(()) => Ok(()),
        Err(error) => Err(status_from_machine_error(MachineError::new(
            error.machine_code(),
            format!("failed to boot runtime resources for sandbox {sandbox_id}: {error}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))),
    }
}

fn runtime_shutdown_error_is_not_active(
    error: &vz_runtime_contract::RuntimeError,
    sandbox_id: &str,
) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    let sandbox_id_lc = sandbox_id.to_ascii_lowercase();

    matches!(
        error,
        vz_runtime_contract::RuntimeError::UnsupportedOperation { .. }
    ) || message.contains("no shared vm running")
        && message.contains("stack")
        && message.contains(&sandbox_id_lc)
        || message.contains("stack")
            && message.contains("not found")
            && message.contains(&sandbox_id_lc)
        || message.contains("not booted")
}

fn default_keepalive_container_cmd() -> Vec<String> {
    vec![
        "/bin/sh".to_string(),
        "-lc".to_string(),
        "while :; do sleep 3600; done".to_string(),
    ]
}

fn default_shell_for_base_image(base_image_ref: Option<&str>) -> &'static str {
    let Some(base_image_ref) = base_image_ref else {
        return "/bin/sh";
    };
    let normalized = base_image_ref.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return "/bin/sh";
    }

    if [
        "ubuntu", "debian", "fedora", "centos", "rocky", "alma", "arch",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
    {
        "/bin/bash"
    } else {
        "/bin/sh"
    }
}

fn parse_main_container_startup_command(
    request_id: &str,
    main_container: &str,
) -> Result<Option<(String, Vec<String>)>, Status> {
    let command_hint = main_container.trim();
    if command_hint.is_empty() {
        return Ok(None);
    }

    let looks_like_command = command_hint.contains(char::is_whitespace)
        || command_hint.starts_with('/')
        || command_hint.contains('/')
        || matches!(command_hint, "sh" | "bash" | "zsh" | "fish" | "nu");
    if !looks_like_command {
        return Ok(None);
    }

    let words = shell_words::split(command_hint).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!("invalid sandbox main_container command: {error}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    if words.is_empty() {
        return Ok(None);
    }

    let mut words = words.into_iter();
    let command = match words.next() {
        Some(command) => command,
        None => return Ok(None),
    };
    let args = words.collect();
    Ok(Some((command, args)))
}

fn resolve_sandbox_shell_command(
    request_id: &str,
    sandbox: &Sandbox,
) -> Result<(String, Vec<String>), Status> {
    let main_container_hint = sandbox
        .spec
        .main_container
        .as_deref()
        .and_then(normalize_optional_wire_field)
        .or_else(|| {
            sandbox
                .labels
                .get(SANDBOX_LABEL_MAIN_CONTAINER)
                .and_then(|value| normalize_optional_wire_field(value))
        });

    if let Some(main_container) = main_container_hint
        && let Some((command, args)) =
            parse_main_container_startup_command(request_id, &main_container)?
    {
        return Ok((command, args));
    }

    let base_image_ref = sandbox
        .spec
        .base_image_ref
        .as_deref()
        .and_then(normalize_optional_wire_field)
        .or_else(|| {
            sandbox
                .labels
                .get(SANDBOX_LABEL_BASE_IMAGE_REF)
                .and_then(|value| normalize_optional_wire_field(value))
        });
    Ok((
        default_shell_for_base_image(base_image_ref.as_deref()).to_string(),
        Vec::new(),
    ))
}

fn find_attachable_sandbox_container(
    daemon: &RuntimeDaemon,
    sandbox_id: &str,
    request_id: &str,
) -> Result<Option<Container>, Status> {
    let mut containers = daemon
        .with_state_store(|store| store.list_containers())
        .map_err(|error| status_from_stack_error(error, request_id))?
        .into_iter()
        .filter(|container| container.sandbox_id == sandbox_id && !container.state.is_terminal())
        .collect::<Vec<_>>();
    containers.sort_by_key(|container| container.created_at);
    Ok(containers.pop())
}

fn sandbox_shell_image_ref(request_id: &str, sandbox: &Sandbox) -> Result<String, Status> {
    sandbox
        .spec
        .base_image_ref
        .as_deref()
        .and_then(normalize_optional_wire_field)
        .or_else(|| {
            sandbox
                .labels
                .get(SANDBOX_LABEL_BASE_IMAGE_REF)
                .and_then(|value| normalize_optional_wire_field(value))
        })
        .ok_or_else(|| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!(
                    "sandbox {} has no base image configured; recreate with --base-image",
                    sandbox.sandbox_id
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        })
}

async fn ensure_sandbox_shell_container(
    daemon: Arc<RuntimeDaemon>,
    sandbox: &Sandbox,
    request_id: &str,
    trace_id: Option<&str>,
) -> Result<String, Status> {
    if let Some(existing) =
        find_attachable_sandbox_container(daemon.as_ref(), &sandbox.sandbox_id, request_id)?
    {
        return Ok(existing.container_id);
    }

    let image_ref = sandbox_shell_image_ref(request_id, sandbox)?;
    let container_service = super::container::ContainerServiceImpl::new(daemon);
    let response = container_service
        .create_container(Request::new(runtime_v2::CreateContainerRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: request_id.to_string(),
                idempotency_key: String::new(),
                trace_id: trace_id.unwrap_or_default().to_string(),
            }),
            sandbox_id: sandbox.sandbox_id.clone(),
            image_digest: image_ref,
            cmd: default_keepalive_container_cmd(),
            env: std::collections::HashMap::new(),
            cwd: "/workspace".to_string(),
            user: String::new(),
        }))
        .await?;
    let container = response.into_inner().container.ok_or_else(|| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            "daemon create_container returned missing payload".to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    Ok(container.container_id)
}

fn session_registry_status(
    error: crate::ExecutionSessionRegistryError,
    request_id: &str,
) -> Status {
    match error {
        crate::ExecutionSessionRegistryError::LockPoisoned
        | crate::ExecutionSessionRegistryError::NotFound { .. } => {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::InternalError,
                "execution session registry lock poisoned".to_string(),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        }
    }
}

fn find_attachable_sandbox_shell_execution(
    daemon: &RuntimeDaemon,
    container_id: &str,
    shell_command: &str,
    shell_args: &[String],
    request_id: &str,
) -> Result<Option<Execution>, Status> {
    let mut executions = daemon
        .with_state_store(|store| store.list_executions())
        .map_err(|error| status_from_stack_error(error, request_id))?
        .into_iter()
        .filter(|execution| {
            execution.container_id == container_id
                && !execution.state.is_terminal()
                && execution_is_sandbox_shell_session(execution, shell_command, shell_args)
        })
        .collect::<Vec<_>>();
    executions.sort_by_key(|execution| execution.started_at.unwrap_or_default());

    for execution in executions.into_iter().rev() {
        let has_session = daemon
            .execution_sessions()
            .contains(&execution.execution_id)
            .map_err(|error| session_registry_status(error, request_id))?;
        if has_session {
            return Ok(Some(execution));
        }
    }

    Ok(None)
}

fn sandbox_container_ids(
    daemon: &RuntimeDaemon,
    sandbox_id: &str,
    request_id: &str,
) -> Result<std::collections::HashSet<String>, Status> {
    let ids = daemon
        .with_state_store(|store| {
            Ok(store
                .list_containers()?
                .into_iter()
                .filter(|container| container.sandbox_id == sandbox_id)
                .map(|container| container.container_id)
                .collect::<std::collections::HashSet<_>>())
        })
        .map_err(|error| status_from_stack_error(error, request_id))?;
    Ok(ids)
}

fn find_latest_active_sandbox_shell_execution(
    daemon: &RuntimeDaemon,
    sandbox: &Sandbox,
    shell_command: &str,
    shell_args: &[String],
    request_id: &str,
) -> Result<Option<Execution>, Status> {
    let container_ids = sandbox_container_ids(daemon, &sandbox.sandbox_id, request_id)?;
    if container_ids.is_empty() {
        return Ok(None);
    }

    let mut executions = daemon
        .with_state_store(|store| store.list_executions())
        .map_err(|error| status_from_stack_error(error, request_id))?
        .into_iter()
        .filter(|execution| {
            container_ids.contains(&execution.container_id)
                && !execution.state.is_terminal()
                && execution_is_sandbox_shell_session(execution, shell_command, shell_args)
        })
        .collect::<Vec<_>>();
    executions.sort_by_key(|execution| execution.started_at.unwrap_or_default());
    Ok(executions.pop())
}

fn resolve_close_sandbox_shell_execution_id(
    daemon: &RuntimeDaemon,
    sandbox: &Sandbox,
    requested_execution_id: Option<&str>,
    request_id: &str,
) -> Result<String, Status> {
    if let Some(execution_id) = requested_execution_id {
        let execution_id = execution_id.trim();
        if execution_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "execution_id cannot be empty when provided".to_string(),
                Some(request_id.to_string()),
                BTreeMap::new(),
            )));
        }

        let execution = daemon
            .with_state_store(|store| store.load_execution(execution_id))
            .map_err(|error| status_from_stack_error(error, request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("execution not found: {execution_id}"),
                    Some(request_id.to_string()),
                    BTreeMap::new(),
                ))
            })?;
        let container = daemon
            .with_state_store(|store| store.load_container(&execution.container_id))
            .map_err(|error| status_from_stack_error(error, request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!(
                        "container not found for execution {}: {}",
                        execution.execution_id, execution.container_id
                    ),
                    Some(request_id.to_string()),
                    BTreeMap::new(),
                ))
            })?;
        if container.sandbox_id != sandbox.sandbox_id {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!(
                    "execution {execution_id} does not belong to sandbox {}",
                    sandbox.sandbox_id
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            )));
        }

        return Ok(execution.execution_id);
    }

    let (shell_command, shell_args) = resolve_sandbox_shell_command(request_id, sandbox)?;
    let execution = find_latest_active_sandbox_shell_execution(
        daemon,
        sandbox,
        &shell_command,
        &shell_args,
        request_id,
    )?
    .ok_or_else(|| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::NotFound,
            format!(
                "no active shell execution found for sandbox {}",
                sandbox.sandbox_id
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    Ok(execution.execution_id)
}

async fn ensure_sandbox_shell_execution(
    daemon: Arc<RuntimeDaemon>,
    container_id: &str,
    shell_command: &str,
    shell_args: &[String],
    request_id: &str,
    trace_id: Option<&str>,
) -> Result<String, Status> {
    if let Some(existing) = find_attachable_sandbox_shell_execution(
        daemon.as_ref(),
        container_id,
        shell_command,
        shell_args,
        request_id,
    )? {
        return Ok(existing.execution_id);
    }

    let execution_service = super::execution::ExecutionServiceImpl::new(daemon);
    let mut env_override = std::collections::HashMap::new();
    env_override.insert(SANDBOX_SHELL_SESSION_ENV_KEY.to_string(), "1".to_string());
    let response = execution_service
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: request_id.to_string(),
                idempotency_key: String::new(),
                trace_id: trace_id.unwrap_or_default().to_string(),
            }),
            container_id: container_id.to_string(),
            cmd: vec![shell_command.to_string()],
            args: shell_args.to_vec(),
            env_override,
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Enabled as i32,
        }))
        .await?;
    let execution = response.into_inner().execution.ok_or_else(|| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            "daemon create_execution returned missing payload".to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    Ok(execution.execution_id)
}

fn execution_is_sandbox_shell_session(
    execution: &Execution,
    shell_command: &str,
    shell_args: &[String],
) -> bool {
    execution.exec_spec.pty
        && execution.exec_spec.cmd == vec![shell_command.to_string()]
        && execution.exec_spec.args == shell_args
        && execution
            .exec_spec
            .env_override
            .get(SANDBOX_SHELL_SESSION_ENV_KEY)
            .is_some_and(|value| value == "1")
}

#[tonic::async_trait]
impl runtime_v2::sandbox_service_server::SandboxService for SandboxServiceImpl {
    type CreateSandboxStream = CreateSandboxEventStream;
    type TerminateSandboxStream = TerminateSandboxEventStream;
    type OpenSandboxShellStream = OpenSandboxShellEventStream;
    type CloseSandboxShellStream = CloseSandboxShellEventStream;

    async fn create_sandbox(
        &self,
        request: Request<runtime_v2::CreateSandboxRequest>,
    ) -> Result<Response<Self::CreateSandboxStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let mut sequence = 1u64;
        let mut events = vec![Ok(create_sandbox_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating create sandbox request",
        ))];
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateSandbox,
            &metadata,
            &request_id,
        )?;
        let idempotency_key = metadata.idempotency_key.clone();

        let sandbox_id = request.stack_name.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let cpus = if request.cpus == 0 {
            None
        } else {
            Some(u8::try_from(request.cpus).map_err(|_| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::ValidationError,
                    format!("cpus out of range for u8: {}", request.cpus),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?)
        };
        let request_hash = create_sandbox_request_hash(&request, cpus);
        let normalized_idempotency_key = normalize_idempotency_key(idempotency_key.as_deref());
        let labels: BTreeMap<String, String> = request.labels.into_iter().collect();
        let base_image_ref = labels
            .get(SANDBOX_LABEL_BASE_IMAGE_REF)
            .and_then(|value| normalize_optional_wire_field(value));
        let main_container = labels
            .get(SANDBOX_LABEL_MAIN_CONTAINER)
            .and_then(|value| normalize_optional_wire_field(value));

        if let Some(key) = normalized_idempotency_key {
            if let Some(cached_sandbox) = load_idempotent_sandbox_replay(
                &self.daemon,
                key,
                "create_sandbox",
                &request_hash,
                &request_id,
            )? {
                sequence += 1;
                events.push(Ok(create_sandbox_progress_event(
                    &request_id,
                    sequence,
                    "idempotency_replay",
                    "replaying cached create sandbox result",
                )));
                sequence += 1;
                events.push(Ok(create_sandbox_completion_event(
                    &request_id,
                    sequence,
                    runtime_v2::SandboxResponse {
                        request_id: request_id.clone(),
                        sandbox: Some(sandbox_to_proto_payload(&cached_sandbox)),
                    },
                    "",
                )));
                return Ok(sandbox_stream_response(events, None));
            }
        }

        let exists = self
            .daemon
            .with_state_store(|store| store.load_sandbox(&sandbox_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .is_some();
        if exists {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("sandbox already exists: {sandbox_id}"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        self.daemon
            .enforce_create_sandbox_placement(&request_id)
            .map_err(status_from_machine_error)?;
        let memory_mb = if request.memory_mb == 0 {
            None
        } else {
            Some(request.memory_mb)
        };
        sequence += 1;
        events.push(Ok(create_sandbox_progress_event(
            &request_id,
            sequence,
            "booting_runtime",
            "booting sandbox runtime resources",
        )));
        if let Err(status) = boot_runtime_sandbox_resources(
            self.daemon.clone(),
            &sandbox_id,
            cpus,
            memory_mb,
            &labels,
            &request_id,
        )
        .await
        {
            events.push(Err(status));
            return Ok(sandbox_stream_response(events, None));
        }

        let spec = SandboxSpec {
            cpus,
            memory_mb,
            base_image_ref,
            main_container,
            network_profile: None,
            volume_mounts: Vec::new(),
        };

        let now = current_unix_secs();
        let sandbox = Sandbox {
            sandbox_id: sandbox_id.clone(),
            backend: daemon_backend(self.daemon.backend_name()),
            spec,
            state: SandboxState::Ready,
            created_at: now,
            updated_at: now,
            labels,
        };

        sequence += 1;
        events.push(Ok(create_sandbox_progress_event(
            &request_id,
            sequence,
            "persisting",
            "persisting sandbox state and receipt",
        )));
        let receipt_id = generate_receipt_id();
        let persist_result = self.daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                if tx.load_sandbox(&sandbox.sandbox_id)?.is_some() {
                    return Err(StackError::Machine {
                        code: MachineErrorCode::StateConflict,
                        message: format!("sandbox already exists: {}", sandbox.sandbox_id),
                    });
                }
                tx.save_sandbox(&sandbox)?;
                tx.emit_event(
                    &sandbox.sandbox_id,
                    &StackEvent::SandboxReady {
                        stack_name: sandbox_stack_name(&sandbox),
                        sandbox_id: sandbox.sandbox_id.clone(),
                    },
                )?;
                tx.save_receipt(&Receipt {
                    receipt_id: receipt_id.clone(),
                    operation: "create_sandbox".to_string(),
                    entity_id: sandbox.sandbox_id.clone(),
                    entity_type: "sandbox".to_string(),
                    request_id: request_id.clone(),
                    status: "success".to_string(),
                    created_at: now,
                    metadata: receipt_idempotent_mutation_metadata(
                        "sandbox_ready",
                        request_hash.as_str(),
                        normalized_idempotency_key,
                    )?,
                })?;
                if let Some(key) = normalized_idempotency_key {
                    tx.save_idempotency_result(&IdempotencyRecord {
                        key: key.to_string(),
                        operation: "create_sandbox".to_string(),
                        request_hash: request_hash.clone(),
                        response_json: sandbox.sandbox_id.clone(),
                        status_code: 201,
                        created_at: now,
                        expires_at: now.saturating_add(IDEMPOTENCY_TTL_SECS),
                    })?;
                }
                Ok(())
            })
        });
        if let Err(error) = persist_result {
            if let Some(key) = normalized_idempotency_key {
                if let Some(cached_sandbox) = load_idempotent_sandbox_replay(
                    &self.daemon,
                    key,
                    "create_sandbox",
                    &request_hash,
                    &request_id,
                )? {
                    sequence += 1;
                    events.push(Ok(create_sandbox_progress_event(
                        &request_id,
                        sequence,
                        "idempotency_replay",
                        "replaying cached create sandbox result after persistence race",
                    )));
                    sequence += 1;
                    events.push(Ok(create_sandbox_completion_event(
                        &request_id,
                        sequence,
                        runtime_v2::SandboxResponse {
                            request_id: request_id.clone(),
                            sandbox: Some(sandbox_to_proto_payload(&cached_sandbox)),
                        },
                        "",
                    )));
                    return Ok(sandbox_stream_response(events, None));
                }
            }

            let exists_after_error = self
                .daemon
                .with_state_store(|store| store.load_sandbox(&sandbox_id))
                .map_err(|store_error| status_from_stack_error(store_error, &request_id))?
                .is_some();
            if exists_after_error {
                events.push(Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::StateConflict,
                    format!("sandbox already exists: {sandbox_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))));
                return Ok(sandbox_stream_response(events, None));
            }

            if let Err(cleanup_error) =
                terminate_runtime_sandbox_resources(self.daemon.clone(), &sandbox_id, &request_id)
                    .await
            {
                warn!(
                    sandbox_id = %sandbox_id,
                    request_id = %request_id,
                    error = %cleanup_error,
                    "failed to clean up runtime resources after create_sandbox persistence failure"
                );
            }

            events.push(Err(status_from_stack_error(error, &request_id)));
            return Ok(sandbox_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(create_sandbox_completion_event(
            &request_id,
            sequence,
            runtime_v2::SandboxResponse {
                request_id: request_id.clone(),
                sandbox: Some(sandbox_to_proto_payload(&sandbox)),
            },
            receipt_id.as_str(),
        )));
        Ok(sandbox_stream_response(events, Some(receipt_id.as_str())))
    }

    async fn get_sandbox(
        &self,
        request: Request<runtime_v2::GetSandboxRequest>,
    ) -> Result<Response<runtime_v2::SandboxResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let sandbox = self
            .daemon
            .with_state_store(|store| store.load_sandbox(&request.sandbox_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("sandbox not found: {}", request.sandbox_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        Ok(Response::new(runtime_v2::SandboxResponse {
            request_id,
            sandbox: Some(sandbox_to_proto_payload(&sandbox)),
        }))
    }

    async fn list_sandboxes(
        &self,
        request: Request<runtime_v2::ListSandboxesRequest>,
    ) -> Result<Response<runtime_v2::ListSandboxesResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let sandboxes = self
            .daemon
            .with_state_store(|store| store.list_sandboxes())
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .iter()
            .map(sandbox_to_proto_payload)
            .collect();

        Ok(Response::new(runtime_v2::ListSandboxesResponse {
            request_id,
            sandboxes,
        }))
    }

    async fn open_sandbox_shell(
        &self,
        request: Request<runtime_v2::OpenSandboxShellRequest>,
    ) -> Result<Response<Self::OpenSandboxShellStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let mut sequence = 1u64;
        let mut events = vec![Ok(open_sandbox_shell_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating sandbox shell request",
        ))];

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "sandbox_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let sandbox = match self
            .daemon
            .with_state_store(|store| store.load_sandbox(&sandbox_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
        {
            Some(sandbox) => sandbox,
            None => {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("sandbox not found: {sandbox_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                )));
            }
        };
        if sandbox.state.is_terminal() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("sandbox {sandbox_id} is in terminal state"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        sequence += 1;
        events.push(Ok(open_sandbox_shell_progress_event(
            &request_id,
            sequence,
            "ensuring_container",
            "ensuring shell container exists",
        )));
        let container_id = match ensure_sandbox_shell_container(
            self.daemon.clone(),
            &sandbox,
            &request_id,
            metadata.trace_id.as_deref(),
        )
        .await
        {
            Ok(container_id) => container_id,
            Err(status) => {
                events.push(Err(status));
                return Ok(Response::new(sandbox_shell_stream_from_events(events)));
            }
        };

        sequence += 1;
        events.push(Ok(open_sandbox_shell_progress_event(
            &request_id,
            sequence,
            "resolving_command",
            "resolving sandbox shell command",
        )));
        let (shell_command, shell_args) = resolve_sandbox_shell_command(&request_id, &sandbox)?;
        sequence += 1;
        events.push(Ok(open_sandbox_shell_progress_event(
            &request_id,
            sequence,
            "ensuring_execution",
            "ensuring interactive shell execution session",
        )));
        let execution_id = match ensure_sandbox_shell_execution(
            self.daemon.clone(),
            &container_id,
            &shell_command,
            &shell_args,
            &request_id,
            metadata.trace_id.as_deref(),
        )
        .await
        {
            Ok(execution_id) => execution_id,
            Err(status) => {
                events.push(Err(status));
                return Ok(Response::new(sandbox_shell_stream_from_events(events)));
            }
        };
        sequence += 1;
        events.push(Ok(open_sandbox_shell_completion_event(
            &request_id,
            sequence,
            runtime_v2::OpenSandboxShellResponse {
                request_id: request_id.clone(),
                sandbox_id: sandbox.sandbox_id,
                container_id,
                cmd: vec![shell_command],
                args: shell_args,
                execution_id,
            },
        )));
        Ok(Response::new(sandbox_shell_stream_from_events(events)))
    }

    async fn close_sandbox_shell(
        &self,
        request: Request<runtime_v2::CloseSandboxShellRequest>,
    ) -> Result<Response<Self::CloseSandboxShellStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let mut sequence = 1u64;
        let mut events = vec![Ok(close_sandbox_shell_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating close shell request",
        ))];

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "sandbox_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let sandbox = self
            .daemon
            .with_state_store(|store| store.load_sandbox(&sandbox_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("sandbox not found: {sandbox_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        let execution_id = resolve_close_sandbox_shell_execution_id(
            self.daemon.as_ref(),
            &sandbox,
            normalize_optional_wire_field(&request.execution_id).as_deref(),
            &request_id,
        )?;

        sequence += 1;
        events.push(Ok(close_sandbox_shell_progress_event(
            &request_id,
            sequence,
            "canceling_execution",
            "canceling active shell execution",
        )));
        let execution_service = super::execution::ExecutionServiceImpl::new(self.daemon.clone());
        match execution_service
            .cancel_execution(Request::new(runtime_v2::CancelExecutionRequest {
                execution_id: execution_id.clone(),
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: request_id.clone(),
                    idempotency_key: String::new(),
                    trace_id: metadata.trace_id.unwrap_or_default(),
                }),
            }))
            .await
        {
            Ok(_) => {}
            Err(status) => {
                events.push(Err(status));
                return Ok(Response::new(sandbox_shell_stream_from_events(events)));
            }
        };
        sequence += 1;
        events.push(Ok(close_sandbox_shell_completion_event(
            &request_id,
            sequence,
            runtime_v2::CloseSandboxShellResponse {
                request_id: request_id.clone(),
                sandbox_id: sandbox.sandbox_id,
                execution_id,
            },
        )));
        Ok(Response::new(sandbox_shell_stream_from_events(events)))
    }

    async fn terminate_sandbox(
        &self,
        request: Request<runtime_v2::TerminateSandboxRequest>,
    ) -> Result<Response<Self::TerminateSandboxStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let mut sequence = 1u64;
        let mut events = vec![Ok(terminate_sandbox_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating terminate sandbox request",
        ))];
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::TerminateSandbox,
            &metadata,
            &request_id,
        )?;
        let idempotency_key = metadata.idempotency_key.clone();
        let normalized_idempotency_key = normalize_idempotency_key(idempotency_key.as_deref());
        let request_hash = format!("sandbox_id={}", request.sandbox_id.trim());

        if let Some(key) = normalized_idempotency_key {
            if let Some(cached_sandbox) = load_idempotent_sandbox_replay(
                &self.daemon,
                key,
                "terminate_sandbox",
                &request_hash,
                &request_id,
            )? {
                sequence += 1;
                events.push(Ok(terminate_sandbox_progress_event(
                    &request_id,
                    sequence,
                    "idempotency_replay",
                    "replaying cached terminate sandbox result",
                )));
                sequence += 1;
                events.push(Ok(terminate_sandbox_completion_event(
                    &request_id,
                    sequence,
                    runtime_v2::SandboxResponse {
                        request_id: request_id.clone(),
                        sandbox: Some(sandbox_to_proto_payload(&cached_sandbox)),
                    },
                    "",
                )));
                return Ok(sandbox_stream_response(events, None));
            }
        }

        let now = current_unix_secs();
        let mut sandbox = self
            .daemon
            .with_state_store(|store| store.load_sandbox(&request.sandbox_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("sandbox not found: {}", request.sandbox_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if sandbox.state != SandboxState::Terminated {
            sequence += 1;
            events.push(Ok(terminate_sandbox_progress_event(
                &request_id,
                sequence,
                "tearing_down_runtime",
                "terminating sandbox runtime resources",
            )));
            if let Err(status) = terminate_runtime_sandbox_resources(
                self.daemon.clone(),
                &sandbox.sandbox_id,
                &request_id,
            )
            .await
            {
                events.push(Err(status));
                return Ok(sandbox_stream_response(events, None));
            }

            sandbox.state = SandboxState::Terminated;
            sandbox.updated_at = now;
            sequence += 1;
            events.push(Ok(terminate_sandbox_progress_event(
                &request_id,
                sequence,
                "persisting",
                "persisting sandbox termination state and receipt",
            )));
            let receipt_id = generate_receipt_id();
            let persist_result = self.daemon.with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_sandbox(&sandbox)?;
                    tx.emit_event(
                        &sandbox.sandbox_id,
                        &StackEvent::SandboxTerminated {
                            stack_name: sandbox_stack_name(&sandbox),
                            sandbox_id: sandbox.sandbox_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "terminate_sandbox".to_string(),
                        entity_id: sandbox.sandbox_id.clone(),
                        entity_type: "sandbox".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_idempotent_mutation_metadata(
                            "sandbox_terminated",
                            request_hash.as_str(),
                            normalized_idempotency_key,
                        )?,
                    })?;
                    if let Some(key) = normalized_idempotency_key {
                        tx.save_idempotency_result(&IdempotencyRecord {
                            key: key.to_string(),
                            operation: "terminate_sandbox".to_string(),
                            request_hash: request_hash.clone(),
                            response_json: sandbox.sandbox_id.clone(),
                            status_code: 200,
                            created_at: now,
                            expires_at: now.saturating_add(IDEMPOTENCY_TTL_SECS),
                        })?;
                    }
                    Ok(())
                })
            });
            if let Err(error) = persist_result {
                if let Some(key) = normalized_idempotency_key {
                    if let Some(cached_sandbox) = load_idempotent_sandbox_replay(
                        &self.daemon,
                        key,
                        "terminate_sandbox",
                        &request_hash,
                        &request_id,
                    )? {
                        sequence += 1;
                        events.push(Ok(terminate_sandbox_progress_event(
                            &request_id,
                            sequence,
                            "idempotency_replay",
                            "replaying cached terminate sandbox result after persistence race",
                        )));
                        sequence += 1;
                        events.push(Ok(terminate_sandbox_completion_event(
                            &request_id,
                            sequence,
                            runtime_v2::SandboxResponse {
                                request_id: request_id.clone(),
                                sandbox: Some(sandbox_to_proto_payload(&cached_sandbox)),
                            },
                            "",
                        )));
                        return Ok(sandbox_stream_response(events, None));
                    }
                }
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(sandbox_stream_response(events, None));
            }

            sequence += 1;
            events.push(Ok(terminate_sandbox_completion_event(
                &request_id,
                sequence,
                runtime_v2::SandboxResponse {
                    request_id: request_id.clone(),
                    sandbox: Some(sandbox_to_proto_payload(&sandbox)),
                },
                receipt_id.as_str(),
            )));
            return Ok(sandbox_stream_response(events, Some(receipt_id.as_str())));
        }

        sequence += 1;
        events.push(Ok(terminate_sandbox_completion_event(
            &request_id,
            sequence,
            runtime_v2::SandboxResponse {
                request_id: request_id.clone(),
                sandbox: Some(sandbox_to_proto_payload(&sandbox)),
            },
            "",
        )));
        Ok(sandbox_stream_response(events, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vz_runtime_contract::RuntimeError;

    #[test]
    fn runtime_shutdown_not_active_detects_missing_shared_vm_message() {
        let error = RuntimeError::InvalidConfig(
            "no shared VM running for stack 'stack-a'; call boot_shared_vm first".to_string(),
        );
        assert!(runtime_shutdown_error_is_not_active(&error, "stack-a"));
    }

    #[test]
    fn runtime_shutdown_not_active_detects_stack_not_found_message() {
        let error = RuntimeError::Backend {
            message: "stack 'stack-b' not found".to_string(),
            source: Box::new(std::io::Error::other("stack missing")),
        };
        assert!(runtime_shutdown_error_is_not_active(&error, "stack-b"));
    }

    #[test]
    fn runtime_shutdown_not_active_ignores_unrelated_errors() {
        let error = RuntimeError::Backend {
            message: "permission denied while stopping vm process".to_string(),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "permission denied",
            )),
        };
        assert!(!runtime_shutdown_error_is_not_active(&error, "stack-c"));
    }
}
