//! Stack gRPC handler support code shared by stack endpoint RPC methods.
//!
//! Consolidates request parsing/mapping helpers and runtime bridge adapters used by
//! `stack::rpc` endpoint implementations.

use super::super::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use vz_stack::{
    Action, ComposeBuildSpec, ContainerLogs, ContainerRuntime, OrchestrationConfig,
    ServiceObservedState, ServicePhase, StackExecutor, StackOrchestrator, StackSpec, VolumeManager,
    apply, collect_compose_build_specs_with_dir, parse_compose_with_dir,
};

const STACK_BUILD_POLL_INTERVAL: Duration = Duration::from_millis(250);
const STACK_BUILD_TIMEOUT: Duration = Duration::from_secs(60 * 60);

#[tonic::async_trait]
trait ComposeBuildRunner {
    async fn start_build(
        &self,
        sandbox_id: &str,
        build_spec: BuildSpec,
    ) -> Result<vz_runtime_contract::Build, vz_runtime_contract::RuntimeError>;
    async fn get_build(
        &self,
        build_id: &str,
    ) -> Result<vz_runtime_contract::Build, vz_runtime_contract::RuntimeError>;
    async fn cancel_build(
        &self,
        build_id: &str,
    ) -> Result<vz_runtime_contract::Build, vz_runtime_contract::RuntimeError>;
}

struct DaemonBuildRunner {
    daemon: Arc<RuntimeDaemon>,
}

impl DaemonBuildRunner {
    fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

#[tonic::async_trait]
impl ComposeBuildRunner for DaemonBuildRunner {
    async fn start_build(
        &self,
        sandbox_id: &str,
        build_spec: BuildSpec,
    ) -> Result<vz_runtime_contract::Build, vz_runtime_contract::RuntimeError> {
        self.daemon
            .manager()
            .start_build(sandbox_id, build_spec, None)
            .await
    }

    async fn get_build(
        &self,
        build_id: &str,
    ) -> Result<vz_runtime_contract::Build, vz_runtime_contract::RuntimeError> {
        self.daemon.manager().get_build(build_id).await
    }

    async fn cancel_build(
        &self,
        build_id: &str,
    ) -> Result<vz_runtime_contract::Build, vz_runtime_contract::RuntimeError> {
        self.daemon.manager().cancel_build(build_id).await
    }
}

pub(in crate::grpc) struct StackServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl StackServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

struct DaemonContainerRuntime {
    daemon: Arc<RuntimeDaemon>,
    handle: tokio::runtime::Handle,
}

impl DaemonContainerRuntime {
    fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self {
            daemon,
            handle: tokio::runtime::Handle::current(),
        }
    }

    fn capabilities(&self) -> vz_runtime_contract::RuntimeCapabilities {
        self.daemon.manager().capabilities()
    }

    fn ensure_capability(
        &self,
        operation: &str,
        capability_name: &str,
        enabled: bool,
    ) -> Result<(), StackError> {
        if enabled {
            return Ok(());
        }
        Err(unsupported_operation_error(
            operation,
            format!(
                "backend={} missing capability {}",
                self.daemon.manager().name(),
                capability_name
            ),
        ))
    }
}

fn unsupported_operation_error(operation: &str, reason: impl Into<String>) -> StackError {
    StackError::Network(format!(
        "unsupported_operation: surface=stack; operation={operation}; reason={}",
        reason.into()
    ))
}

fn map_runtime_error(operation: &str, error: vz_runtime_contract::RuntimeError) -> StackError {
    match error {
        vz_runtime_contract::RuntimeError::UnsupportedOperation {
            operation: backend_operation,
            reason,
        } => unsupported_operation_error(
            operation,
            format!("backend_operation={backend_operation}; {reason}"),
        ),
        other => StackError::Network(format!("{operation} failed: {other}")),
    }
}

impl ContainerRuntime for DaemonContainerRuntime {
    fn pull(&self, image: &str) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.daemon.manager().pull_image(image))
                .map_err(|error| map_runtime_error("pull", error))
        })
    }

    fn create(
        &self,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.daemon.manager().create_container(image, config))
                .map_err(|error| map_runtime_error("create", error))
        })
    }

    fn stop(
        &self,
        container_id: &str,
        signal: Option<&str>,
        grace_period: Option<std::time::Duration>,
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.daemon.manager().stop_container(
                    container_id,
                    false,
                    signal,
                    grace_period,
                ))
                .map(|_| ())
                .map_err(|error| map_runtime_error("stop", error))
        })
    }

    fn remove(&self, container_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.daemon.manager().remove_container(container_id))
                .map_err(|error| map_runtime_error("remove", error))
        })
    }

    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError> {
        let (exit_code, _, _) = self.exec_with_output(container_id, command)?;
        Ok(exit_code)
    }

    fn exec_with_output(
        &self,
        container_id: &str,
        command: &[String],
    ) -> Result<(i32, String, String), StackError> {
        tokio::task::block_in_place(|| {
            let exec_config = vz_runtime_contract::ExecConfig {
                cmd: command.to_vec(),
                ..Default::default()
            };
            self.handle
                .block_on(
                    self.daemon
                        .manager()
                        .exec_container(container_id, exec_config),
                )
                .map(|output| (output.exit_code, output.stdout, output.stderr))
                .map_err(|error| map_runtime_error("exec", error))
        })
    }

    fn create_sandbox(
        &self,
        sandbox_id: &str,
        ports: Vec<vz_runtime_contract::PortMapping>,
        resources: vz_runtime_contract::StackResourceHint,
    ) -> Result<(), StackError> {
        let capabilities = self.capabilities();
        self.ensure_capability("create_sandbox", "shared_vm", capabilities.shared_vm)?;
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.daemon
                        .manager()
                        .ensure_stack_runtime(sandbox_id, ports, resources),
                )
                .map_err(|error| map_runtime_error("create_sandbox", error))
        })
    }

    fn create_in_sandbox(
        &self,
        sandbox_id: &str,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<String, StackError> {
        let capabilities = self.capabilities();
        self.ensure_capability("create_in_sandbox", "shared_vm", capabilities.shared_vm)?;
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.daemon
                        .manager()
                        .create_stack_container(sandbox_id, image, config),
                )
                .map_err(|error| map_runtime_error("create_in_sandbox", error))
        })
    }

    fn setup_sandbox_network(
        &self,
        sandbox_id: &str,
        services: Vec<vz_runtime_contract::NetworkServiceConfig>,
    ) -> Result<(), StackError> {
        let capabilities = self.capabilities();
        self.ensure_capability("setup_sandbox_network", "shared_vm", capabilities.shared_vm)?;
        self.ensure_capability(
            "setup_sandbox_network",
            "stack_networking",
            capabilities.stack_networking,
        )?;
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.daemon
                        .manager()
                        .setup_stack_network(sandbox_id, services),
                )
                .map_err(|error| map_runtime_error("setup_sandbox_network", error))
        })
    }

    fn teardown_sandbox_network(
        &self,
        sandbox_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), StackError> {
        let capabilities = self.capabilities();
        self.ensure_capability(
            "teardown_sandbox_network",
            "shared_vm",
            capabilities.shared_vm,
        )?;
        self.ensure_capability(
            "teardown_sandbox_network",
            "stack_networking",
            capabilities.stack_networking,
        )?;
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.daemon
                        .manager()
                        .teardown_stack_network(sandbox_id, service_names),
                )
                .map_err(|error| map_runtime_error("teardown_sandbox_network", error))
        })
    }

    fn shutdown_sandbox(&self, sandbox_id: &str) -> Result<(), StackError> {
        let capabilities = self.capabilities();
        self.ensure_capability("shutdown_sandbox", "shared_vm", capabilities.shared_vm)?;
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.daemon.manager().shutdown_stack_runtime(sandbox_id))
                .map_err(|error| map_runtime_error("shutdown_sandbox", error))
        })
    }

    fn has_sandbox(&self, sandbox_id: &str) -> bool {
        if !self.capabilities().shared_vm {
            return false;
        }
        self.daemon.manager().has_stack_runtime(sandbox_id)
    }

    fn logs(&self, container_id: &str) -> Result<ContainerLogs, StackError> {
        let capabilities = self.capabilities();
        self.ensure_capability("logs", "container_logs", capabilities.container_logs)?;
        let logs = self
            .daemon
            .manager()
            .container_logs(container_id)
            .map_err(|error| map_runtime_error("logs", error))?;
        Ok(ContainerLogs {
            output: logs.output,
        })
    }
}

fn stack_runtime_dir(daemon: &RuntimeDaemon, stack_name: &str) -> PathBuf {
    daemon.runtime_data_dir().join("stacks").join(stack_name)
}

fn stack_status_from_observed(status: &ServiceObservedState) -> runtime_v2::StackServiceStatus {
    runtime_v2::StackServiceStatus {
        service_name: status.service_name.clone(),
        phase: match status.phase {
            ServicePhase::Pending => "pending".to_string(),
            ServicePhase::Creating => "creating".to_string(),
            ServicePhase::Running => "running".to_string(),
            ServicePhase::Stopping => "stopping".to_string(),
            ServicePhase::Stopped => "stopped".to_string(),
            ServicePhase::Failed => "failed".to_string(),
        },
        ready: status.ready,
        container_id: status.container_id.clone().unwrap_or_default(),
        last_error: status.last_error.clone().unwrap_or_default(),
    }
}

fn default_stopped_service(service_name: &str) -> ServiceObservedState {
    ServiceObservedState {
        service_name: service_name.to_string(),
        phase: ServicePhase::Stopped,
        container_id: None,
        last_error: None,
        ready: false,
    }
}

fn load_stack_service_action_context(
    daemon: &RuntimeDaemon,
    stack_name: &str,
    service_name: &str,
    request_id: &str,
) -> Result<(StackSpec, ServiceObservedState), Status> {
    let (desired, observed) = daemon
        .with_state_store(|store| {
            Ok((
                store.load_desired_state(stack_name)?,
                store.load_observed_state(stack_name)?,
            ))
        })
        .map_err(|error| status_from_stack_error(error, request_id))?;

    if desired.is_none() && observed.is_empty() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::NotFound,
            format!("stack not found: {stack_name}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let spec = desired.ok_or_else(|| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::StateConflict,
            format!("desired stack state missing for: {stack_name}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    if !spec
        .services
        .iter()
        .any(|service| service.name == service_name)
    {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::NotFound,
            format!("service not found in stack {stack_name}: {service_name}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let observed_state = observed
        .iter()
        .find(|service| service.service_name == service_name)
        .cloned()
        .unwrap_or_else(|| default_stopped_service(service_name));

    Ok((spec, observed_state))
}

fn stack_service_action_response(
    request_id: String,
    stack_name: String,
    service_state: ServiceObservedState,
) -> runtime_v2::StackServiceActionResponse {
    runtime_v2::StackServiceActionResponse {
        request_id,
        stack_name,
        service: Some(stack_status_from_observed(&service_state)),
    }
}

fn stack_run_container_response(
    request_id: String,
    stack_name: String,
    service_name: String,
    run_service_name: String,
    container_id: String,
) -> runtime_v2::StackRunContainerResponse {
    runtime_v2::StackRunContainerResponse {
        request_id,
        stack_name,
        service_name,
        run_service_name,
        container_id,
    }
}

fn generated_stack_run_service_name(service_name: &str) -> String {
    let suffix = generate_request_id().replace("req_", "");
    format!("{service_name}-run-{suffix}")
}

fn clone_stack_spec_with_run_service(
    spec: &StackSpec,
    service_name: &str,
    run_service_name: &str,
    request_id: &str,
) -> Result<StackSpec, Status> {
    let source_service = spec
        .services
        .iter()
        .find(|service| service.name == service_name)
        .cloned()
        .ok_or_else(|| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("service not found in stack {}: {service_name}", spec.name),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        })?;

    if spec
        .services
        .iter()
        .any(|service| service.name == run_service_name && service.name != service_name)
    {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "run service name already exists in stack {}: {run_service_name}",
                spec.name
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let mut run_service = source_service;
    run_service.name = run_service_name.to_string();
    run_service.container_name = None;

    let mut run_spec = spec.clone();
    run_spec.services.push(run_service);
    Ok(run_spec)
}

fn load_observed_stack_service(
    daemon: &RuntimeDaemon,
    stack_name: &str,
    service_name: &str,
    request_id: &str,
) -> Result<ServiceObservedState, Status> {
    daemon
        .with_state_store(|store| {
            Ok(store
                .load_observed_state(stack_name)?
                .into_iter()
                .find(|service| service.service_name == service_name)
                .unwrap_or_else(|| default_stopped_service(service_name)))
        })
        .map_err(|error| status_from_stack_error(error, request_id))
}

fn execute_stack_service_action(
    daemon: Arc<RuntimeDaemon>,
    spec: &StackSpec,
    action: Action,
    request_id: &str,
    failure_code: MachineErrorCode,
) -> Result<(), Status> {
    let stack_dir = stack_runtime_dir(daemon.as_ref(), &spec.name);
    std::fs::create_dir_all(&stack_dir).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "failed to create stack runtime directory {}: {error}",
                stack_dir.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    let exec_store = daemon
        .open_dedicated_state_store()
        .map_err(|error| status_from_stack_error(error, request_id))?;
    let runtime = DaemonContainerRuntime::new(daemon);
    let mut executor = StackExecutor::new(runtime, exec_store, &stack_dir);
    let result = executor
        .execute(spec, &[action])
        .map_err(|error| status_from_stack_error(error, request_id))?;
    if result.failed > 0 {
        let first_error = result
            .errors
            .first()
            .map(|(_, message)| message.as_str())
            .unwrap_or("unknown stack service action failure");
        return Err(status_from_machine_error(MachineError::new(
            failure_code,
            first_error.to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    Ok(())
}

fn parse_stack_spec(
    stack_name: &str,
    compose_yaml: &str,
    compose_dir: &str,
) -> Result<StackSpec, StackError> {
    let base_dir = if compose_dir.trim().is_empty() {
        PathBuf::from(".")
    } else {
        PathBuf::from(compose_dir)
    };
    parse_compose_with_dir(compose_yaml, stack_name, &base_dir)
        .map_err(|error| StackError::ComposeValidation(error.to_string()))
}

fn parse_stack_build_specs(
    compose_yaml: &str,
    compose_dir: &str,
) -> Result<Vec<ComposeBuildSpec>, StackError> {
    let base_dir = if compose_dir.trim().is_empty() {
        PathBuf::from(".")
    } else {
        PathBuf::from(compose_dir)
    };
    collect_compose_build_specs_with_dir(compose_yaml, &base_dir)
        .map(|builds| builds.into_values().collect())
        .map_err(|error| StackError::ComposeValidation(error.to_string()))
}

fn resolve_build_context_path(compose_dir: &Path, context: &str) -> PathBuf {
    let context_path = PathBuf::from(context);
    if context_path.is_absolute() {
        context_path
    } else {
        compose_dir.join(context_path)
    }
}

async fn run_compose_builds(
    daemon: Arc<RuntimeDaemon>,
    stack_spec: &StackSpec,
    compose_yaml: &str,
    compose_dir: &str,
) -> Result<(), StackError> {
    run_compose_builds_with_runner(
        &DaemonBuildRunner::new(daemon),
        stack_spec,
        compose_yaml,
        compose_dir,
        STACK_BUILD_POLL_INTERVAL,
        STACK_BUILD_TIMEOUT,
    )
    .await
}

async fn run_compose_builds_with_runner(
    runner: &(impl ComposeBuildRunner + ?Sized),
    stack_spec: &StackSpec,
    compose_yaml: &str,
    compose_dir: &str,
    poll_interval: Duration,
    timeout: Duration,
) -> Result<(), StackError> {
    let compose_dir_path = if compose_dir.trim().is_empty() {
        PathBuf::from(".")
    } else {
        PathBuf::from(compose_dir)
    };
    let build_specs = parse_stack_build_specs(compose_yaml, compose_dir)?;
    if build_specs.is_empty() {
        return Ok(());
    }

    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return Err(StackError::Network(
            "stack build timeout overflowed instant range".to_string(),
        ));
    };

    for build_spec in build_specs {
        let service = stack_spec
            .services
            .iter()
            .find(|service| service.name == build_spec.service_name)
            .ok_or_else(|| {
                StackError::ComposeValidation(format!(
                    "service `{}` not found while preparing build directives",
                    build_spec.service_name
                ))
            })?;

        let context_path = resolve_build_context_path(&compose_dir_path, &build_spec.context);
        let mut build = runner
            .start_build(
                &stack_spec.name,
                BuildSpec {
                    context: context_path.to_string_lossy().to_string(),
                    dockerfile: build_spec.dockerfile.clone(),
                    target: build_spec.target.clone(),
                    args: build_spec.args.clone(),
                    cache_from: build_spec.cache_from.clone(),
                    image_tag: Some(service.image.clone()),
                    secrets: Vec::new(),
                    no_cache: false,
                    push: false,
                    output_oci_tar_dest: None,
                },
            )
            .await
            .map_err(|error| map_runtime_error("start_build", error))?;

        while !build.state.is_terminal() {
            if Instant::now() >= deadline {
                let _ = runner.cancel_build(&build.build_id).await;
                return Err(StackError::Network(format!(
                    "timed out waiting for build {} for service {}",
                    build.build_id, build_spec.service_name
                )));
            }

            tokio::time::sleep(poll_interval).await;
            build = runner
                .get_build(&build.build_id)
                .await
                .map_err(|error| map_runtime_error("get_build", error))?;
        }

        if build.state != BuildState::Succeeded {
            return Err(StackError::Network(format!(
                "build {} for service {} finished in state {}",
                build.build_id,
                build_spec.service_name,
                build_state_label(build.state)
            )));
        }
    }

    Ok(())
}

fn build_state_label(state: BuildState) -> &'static str {
    match state {
        BuildState::Queued => "queued",
        BuildState::Running => "running",
        BuildState::Succeeded => "succeeded",
        BuildState::Failed => "failed",
        BuildState::Canceled => "canceled",
    }
}

fn tail_output(raw: &str, tail: usize) -> String {
    if tail == 0 {
        return raw.to_string();
    }
    let mut lines: Vec<&str> = raw.lines().collect();
    if lines.len() > tail {
        let start = lines.len() - tail;
        lines = lines.split_off(start);
    }
    let mut output = lines.join("\n");
    if !output.is_empty() {
        output.push('\n');
    }
    output
}

type ApplyStackEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::ApplyStackEvent, Status>>;
type TeardownStackEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::TeardownStackEvent, Status>>;
type StackServiceActionEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::StackServiceActionEvent, Status>>;

fn stack_stream_from_events<T>(
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

fn stack_stream_response<T>(
    events: Vec<Result<T, Status>>,
    receipt_id: Option<&str>,
) -> Response<tokio_stream::wrappers::ReceiverStream<Result<T, Status>>>
where
    T: Send + 'static,
{
    let mut response = Response::new(stack_stream_from_events(events));
    if let Some(receipt_id) = receipt_id
        && !receipt_id.trim().is_empty()
        && let Ok(value) = MetadataValue::try_from(receipt_id)
    {
        response.metadata_mut().insert("x-receipt-id", value);
    }
    response
}

fn apply_stack_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::ApplyStackEvent {
    runtime_v2::ApplyStackEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::apply_stack_event::Payload::Progress(
            runtime_v2::StackMutationProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn apply_stack_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::ApplyStackResponse,
    receipt_id: &str,
) -> runtime_v2::ApplyStackEvent {
    runtime_v2::ApplyStackEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::apply_stack_event::Payload::Completion(
            runtime_v2::ApplyStackCompletion {
                response: Some(response),
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

fn teardown_stack_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::TeardownStackEvent {
    runtime_v2::TeardownStackEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::teardown_stack_event::Payload::Progress(
            runtime_v2::StackMutationProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn teardown_stack_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::TeardownStackResponse,
    receipt_id: &str,
) -> runtime_v2::TeardownStackEvent {
    runtime_v2::TeardownStackEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::teardown_stack_event::Payload::Completion(
            runtime_v2::TeardownStackCompletion {
                response: Some(response),
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

fn stack_service_action_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::StackServiceActionEvent {
    runtime_v2::StackServiceActionEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::stack_service_action_event::Payload::Progress(
            runtime_v2::StackMutationProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn stack_service_action_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::StackServiceActionResponse,
    receipt_id: &str,
) -> runtime_v2::StackServiceActionEvent {
    runtime_v2::StackServiceActionEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::stack_service_action_event::Payload::Completion(
            runtime_v2::StackServiceActionCompletion {
                response: Some(response),
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

mod rpc;
