//! Sandbox gRPC handler support code shared by sandbox endpoint RPC methods.
//!
//! Consolidates shell session helpers, sandbox lifecycle validation, and response
//! mapping used by `sandbox::rpc` endpoint implementations.

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

mod rpc;
