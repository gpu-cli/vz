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

#[tonic::async_trait]
impl runtime_v2::stack_service_server::StackService for StackServiceImpl {
    type ApplyStackStream = ApplyStackEventStream;
    type TeardownStackStream = TeardownStackEventStream;

    async fn apply_stack(
        &self,
        request: Request<runtime_v2::ApplyStackRequest>,
    ) -> Result<Response<Self::ApplyStackStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateContainer,
            &metadata,
            &request_id,
        )?;

        let stack_name = request.stack_name.trim().to_string();
        if stack_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        if request.compose_yaml.trim().is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "compose_yaml cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let spec = parse_stack_spec(&stack_name, &request.compose_yaml, &request.compose_dir)
            .map_err(|error| status_from_stack_error(error, &request_id))?;
        let mut sequence = 1u64;
        let mut events = vec![Ok(apply_stack_progress_event(
            &request_id,
            sequence,
            "planning",
            "planning stack apply actions",
        ))];

        let stack_dir = stack_runtime_dir(self.daemon.as_ref(), &stack_name);
        if let Err(error) = std::fs::create_dir_all(&stack_dir) {
            events.push(Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::InternalError,
                format!(
                    "failed to create stack runtime directory {}: {error}",
                    stack_dir.display()
                ),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))));
            return Ok(stack_stream_response(events, None));
        }

        let preview_store = match self
            .daemon
            .open_dedicated_state_store()
        {
            Ok(store) => store,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        let health_statuses = HashMap::new();
        let apply_result = match apply(&spec, &preview_store, &health_statuses) {
            Ok(result) => result,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        if request.dry_run {
            let observed = match preview_store.load_observed_state(&stack_name) {
                Ok(value) => value,
                Err(error) => {
                    events.push(Err(status_from_stack_error(error, &request_id)));
                    return Ok(stack_stream_response(events, None));
                }
            };
            let services: Vec<runtime_v2::StackServiceStatus> =
                observed.iter().map(stack_status_from_observed).collect();
            let services_ready = observed.iter().filter(|item| item.ready).count();
            let services_failed = observed
                .iter()
                .filter(|item| item.phase == ServicePhase::Failed)
                .count();
            sequence += 1;
            events.push(Ok(apply_stack_completion_event(
                &request_id,
                sequence,
                runtime_v2::ApplyStackResponse {
                    request_id: request_id.clone(),
                    stack_name,
                    changed_actions: apply_result.actions.len() as u32,
                    converged: false,
                    services_ready: services_ready as u32,
                    services_failed: services_failed as u32,
                    services,
                },
                "",
            )));
            return Ok(stack_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(apply_stack_progress_event(
            &request_id,
            sequence,
            "building_images",
            "running compose build directives",
        )));
        if let Err(error) = run_compose_builds(
            self.daemon.clone(),
            &spec,
            &request.compose_yaml,
            &request.compose_dir,
        )
        .await
        {
            events.push(Err(status_from_stack_error(error, &request_id)));
            return Ok(stack_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(apply_stack_progress_event(
            &request_id,
            sequence,
            "reconciling",
            "reconciling stack runtime state",
        )));
        let exec_store = match self
            .daemon
            .open_dedicated_state_store()
        {
            Ok(store) => store,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        let reconcile_store = match self
            .daemon
            .open_dedicated_state_store()
        {
            Ok(store) => store,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        let runtime = DaemonContainerRuntime::new(self.daemon.clone());
        let executor = StackExecutor::new(runtime, exec_store, &stack_dir);
        let config = if request.detach {
            OrchestrationConfig {
                max_rounds: 1,
                ..Default::default()
            }
        } else {
            OrchestrationConfig::default()
        };
        let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, config);
        let orchestration_result = match orchestrator.run(&spec, None) {
            Ok(result) => result,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        let observed = match orchestrator.executor().store().load_observed_state(&stack_name) {
            Ok(value) => value,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        let services: Vec<runtime_v2::StackServiceStatus> =
            observed.iter().map(stack_status_from_observed).collect();
        let changed_actions = apply_result.actions.len() as u32;
        let converged = orchestration_result.converged;
        let services_ready = orchestration_result.services_ready as u32;
        let services_failed = orchestration_result.services_failed as u32;

        sequence += 1;
        events.push(Ok(apply_stack_progress_event(
            &request_id,
            sequence,
            "persisting",
            "persisting stack apply receipt",
        )));
        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        let persist_result = self
            .daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.emit_event(
                        &stack_name,
                        &StackEvent::StackApplyCompleted {
                            stack_name: stack_name.clone(),
                            succeeded: orchestration_result.services_ready,
                            failed: orchestration_result.services_failed,
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "apply_stack".to_string(),
                        entity_id: stack_name.clone(),
                        entity_type: "stack".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_stack_apply_metadata(
                            changed_actions,
                            converged,
                            services_ready,
                            services_failed,
                        )?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id));
        if let Err(status) = persist_result {
            events.push(Err(status));
            return Ok(stack_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(apply_stack_completion_event(
            &request_id,
            sequence,
            runtime_v2::ApplyStackResponse {
                request_id: request_id.clone(),
                stack_name,
                changed_actions,
                converged,
                services_ready,
                services_failed,
                services,
            },
            receipt_id.as_str(),
        )));
        Ok(stack_stream_response(events, Some(receipt_id.as_str())))
    }

    async fn teardown_stack(
        &self,
        request: Request<runtime_v2::TeardownStackRequest>,
    ) -> Result<Response<Self::TeardownStackStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::RemoveContainer,
            &metadata,
            &request_id,
        )?;
        let stack_name = request.stack_name.trim().to_string();
        if stack_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let (desired, observed) = self
            .daemon
            .with_state_store(|store| {
                Ok((
                    store.load_desired_state(&stack_name)?,
                    store.load_observed_state(&stack_name)?,
                ))
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;
        if desired.is_none() && observed.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("stack not found: {stack_name}"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let mut sequence = 1u64;
        let mut events = vec![Ok(teardown_stack_progress_event(
            &request_id,
            sequence,
            "planning",
            "planning stack teardown actions",
        ))];

        let empty_spec = StackSpec {
            name: stack_name.clone(),
            services: Vec::new(),
            networks: Vec::new(),
            volumes: Vec::new(),
            secrets: Vec::new(),
            disk_size_mb: None,
        };
        let health_statuses = HashMap::new();
        let apply_result = match self
            .daemon
            .with_state_store(|store| apply(&empty_spec, store, &health_statuses))
        {
            Ok(result) => result,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };

        if request.dry_run {
            sequence += 1;
            events.push(Ok(teardown_stack_completion_event(
                &request_id,
                sequence,
                runtime_v2::TeardownStackResponse {
                    request_id: request_id.clone(),
                    stack_name,
                    changed_actions: apply_result.actions.len() as u32,
                    removed_volumes: 0,
                },
                "",
            )));
            return Ok(stack_stream_response(events, None));
        }

        if !apply_result.actions.is_empty() {
            sequence += 1;
            events.push(Ok(teardown_stack_progress_event(
                &request_id,
                sequence,
                "executing",
                "executing stack teardown actions",
            )));
            let stack_dir = stack_runtime_dir(self.daemon.as_ref(), &stack_name);
            if let Err(error) = std::fs::create_dir_all(&stack_dir) {
                events.push(Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::InternalError,
                    format!(
                        "failed to create stack runtime directory {}: {error}",
                        stack_dir.display()
                    ),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))));
                return Ok(stack_stream_response(events, None));
            }
            let exec_store = match self
                .daemon
                .open_dedicated_state_store()
            {
                Ok(store) => store,
                Err(error) => {
                    events.push(Err(status_from_stack_error(error, &request_id)));
                    return Ok(stack_stream_response(events, None));
                }
            };
            let runtime = DaemonContainerRuntime::new(self.daemon.clone());
            let mut executor = StackExecutor::new(runtime, exec_store, &stack_dir);
            if let Err(error) = executor.execute(&empty_spec, &apply_result.actions) {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        }

        if request.remove_volumes {
            sequence += 1;
            events.push(Ok(teardown_stack_progress_event(
                &request_id,
                sequence,
                "removing_volumes",
                "removing stack volumes",
            )));
        }
        let removed_volumes = if request.remove_volumes {
            let stack_dir = stack_runtime_dir(self.daemon.as_ref(), &stack_name);
            let volume_manager = VolumeManager::new(&stack_dir);
            match volume_manager.remove_all() {
                Ok(count) => count,
                Err(error) => {
                    events.push(Err(status_from_stack_error(error, &request_id)));
                    return Ok(stack_stream_response(events, None));
                }
            }
        } else {
            0
        };

        let changed_actions = apply_result.actions.len() as u32;
        let removed_volumes = removed_volumes as u32;
        sequence += 1;
        events.push(Ok(teardown_stack_progress_event(
            &request_id,
            sequence,
            "persisting",
            "persisting stack teardown receipt",
        )));
        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        let persist_result = self
            .daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.emit_event(
                        &stack_name,
                        &StackEvent::StackDestroyed {
                            stack_name: stack_name.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "teardown_stack".to_string(),
                        entity_id: stack_name.clone(),
                        entity_type: "stack".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_stack_teardown_metadata(
                            changed_actions,
                            removed_volumes,
                        )?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id));
        if let Err(status) = persist_result {
            events.push(Err(status));
            return Ok(stack_stream_response(events, None));
        }
        sequence += 1;
        events.push(Ok(teardown_stack_completion_event(
            &request_id,
            sequence,
            runtime_v2::TeardownStackResponse {
                request_id: request_id.clone(),
                stack_name,
                changed_actions,
                removed_volumes,
            },
            receipt_id.as_str(),
        )));
        Ok(stack_stream_response(events, Some(receipt_id.as_str())))
    }

    async fn get_stack_status(
        &self,
        request: Request<runtime_v2::GetStackStatusRequest>,
    ) -> Result<Response<runtime_v2::GetStackStatusResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let stack_name = request.stack_name.trim().to_string();
        if stack_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let (desired, observed) = self
            .daemon
            .with_state_store(|store| {
                Ok((
                    store.load_desired_state(&stack_name)?,
                    store.load_observed_state(&stack_name)?,
                ))
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;
        if desired.is_none() && observed.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("stack not found: {stack_name}"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        Ok(Response::new(runtime_v2::GetStackStatusResponse {
            request_id,
            stack_name,
            services: observed.iter().map(stack_status_from_observed).collect(),
        }))
    }

    async fn list_stack_events(
        &self,
        request: Request<runtime_v2::ListStackEventsRequest>,
    ) -> Result<Response<runtime_v2::ListStackEventsResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let stack_name = request.stack_name.trim().to_string();
        if stack_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let limit = if request.limit == 0 {
            100
        } else {
            request.limit as usize
        }
        .clamp(1, 1000);
        let after = request.after.max(0);

        let records = self
            .daemon
            .with_state_store(|store| store.load_events_since_limited(&stack_name, after, limit))
            .map_err(|error| status_from_stack_error(error, &request_id))?;
        let events: Vec<runtime_v2::RuntimeEvent> = records
            .iter()
            .map(event_record_to_runtime_event)
            .collect::<Result<_, _>>()?;
        let next_cursor = records.last().map(|record| record.id).unwrap_or(after);

        Ok(Response::new(runtime_v2::ListStackEventsResponse {
            request_id,
            events,
            next_cursor,
        }))
    }

    async fn get_stack_logs(
        &self,
        request: Request<runtime_v2::GetStackLogsRequest>,
    ) -> Result<Response<runtime_v2::GetStackLogsResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let stack_name = request.stack_name.trim().to_string();
        if stack_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let service_filter = request.service.trim().to_string();
        let tail = request.tail as usize;

        let observed = self
            .daemon
            .with_state_store(|store| store.load_observed_state(&stack_name))
            .map_err(|error| status_from_stack_error(error, &request_id))?;
        let targets: Vec<&ServiceObservedState> = if service_filter.is_empty() {
            observed
                .iter()
                .filter(|entry| entry.phase == ServicePhase::Running)
                .collect()
        } else {
            observed
                .iter()
                .filter(|entry| entry.service_name == service_filter)
                .collect()
        };
        if targets.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                if service_filter.is_empty() {
                    format!("no running services for stack: {stack_name}")
                } else {
                    format!("service not found in stack {stack_name}: {service_filter}")
                },
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let runtime = DaemonContainerRuntime::new(self.daemon.clone());
        let mut logs = Vec::with_capacity(targets.len());
        for entry in targets {
            let Some(container_id) = entry.container_id.as_deref() else {
                continue;
            };
            let output = runtime
                .logs(container_id)
                .map(|logs| logs.output)
                .map_err(|error| {
                    status_from_stack_error(
                        StackError::Network(format!(
                            "failed to load logs for service {}: {error}",
                            entry.service_name
                        )),
                        &request_id,
                    )
                })?;
            logs.push(runtime_v2::StackServiceLog {
                service_name: entry.service_name.clone(),
                output: tail_output(&output, tail),
            });
        }

        Ok(Response::new(runtime_v2::GetStackLogsResponse {
            request_id,
            stack_name,
            logs,
        }))
    }

    async fn stop_stack_service(
        &self,
        request: Request<runtime_v2::StackServiceActionRequest>,
    ) -> Result<Response<runtime_v2::StackServiceActionResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::StopContainer,
            &metadata,
            &request_id,
        )?;

        let stack_name = request.stack_name.trim().to_string();
        if stack_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let service_name = request.service_name.trim().to_string();
        if service_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "service_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let (spec, observed_state) = load_stack_service_action_context(
            self.daemon.as_ref(),
            &stack_name,
            &service_name,
            &request_id,
        )?;
        if observed_state.phase != ServicePhase::Stopped || observed_state.container_id.is_some() {
            execute_stack_service_action(
                self.daemon.clone(),
                &spec,
                Action::ServiceRemove {
                    service_name: service_name.clone(),
                },
                &request_id,
                MachineErrorCode::StateConflict,
            )?;
        }

        let service_state = self
            .daemon
            .with_state_store(|store| {
                Ok(store
                    .load_observed_state(&stack_name)?
                    .into_iter()
                    .find(|service| service.service_name == service_name)
                    .unwrap_or_else(|| default_stopped_service(&service_name)))
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "stop_stack_service".to_string(),
                        entity_id: format!("{stack_name}:{service_name}"),
                        entity_type: "stack_service".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("stack_service_stopped")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(stack_service_action_response(
            request_id,
            stack_name,
            service_state,
        ));
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn start_stack_service(
        &self,
        request: Request<runtime_v2::StackServiceActionRequest>,
    ) -> Result<Response<runtime_v2::StackServiceActionResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateContainer,
            &metadata,
            &request_id,
        )?;

        let stack_name = request.stack_name.trim().to_string();
        if stack_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let service_name = request.service_name.trim().to_string();
        if service_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "service_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let (spec, observed_state) = load_stack_service_action_context(
            self.daemon.as_ref(),
            &stack_name,
            &service_name,
            &request_id,
        )?;
        if !(observed_state.phase == ServicePhase::Running && observed_state.container_id.is_some())
        {
            execute_stack_service_action(
                self.daemon.clone(),
                &spec,
                Action::ServiceCreate {
                    service_name: service_name.clone(),
                },
                &request_id,
                MachineErrorCode::InternalError,
            )?;
        }

        let service_state = self
            .daemon
            .with_state_store(|store| {
                Ok(store
                    .load_observed_state(&stack_name)?
                    .into_iter()
                    .find(|service| service.service_name == service_name)
                    .unwrap_or_else(|| default_stopped_service(&service_name)))
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "start_stack_service".to_string(),
                        entity_id: format!("{stack_name}:{service_name}"),
                        entity_type: "stack_service".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("stack_service_started")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(stack_service_action_response(
            request_id,
            stack_name,
            service_state,
        ));
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn restart_stack_service(
        &self,
        request: Request<runtime_v2::StackServiceActionRequest>,
    ) -> Result<Response<runtime_v2::StackServiceActionResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateContainer,
            &metadata,
            &request_id,
        )?;

        let stack_name = request.stack_name.trim().to_string();
        if stack_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let service_name = request.service_name.trim().to_string();
        if service_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "service_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let (spec, _observed_state) = load_stack_service_action_context(
            self.daemon.as_ref(),
            &stack_name,
            &service_name,
            &request_id,
        )?;
        execute_stack_service_action(
            self.daemon.clone(),
            &spec,
            Action::ServiceRecreate {
                service_name: service_name.clone(),
            },
            &request_id,
            MachineErrorCode::InternalError,
        )?;

        let service_state = self
            .daemon
            .with_state_store(|store| {
                Ok(store
                    .load_observed_state(&stack_name)?
                    .into_iter()
                    .find(|service| service.service_name == service_name)
                    .unwrap_or_else(|| default_stopped_service(&service_name)))
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "restart_stack_service".to_string(),
                        entity_id: format!("{stack_name}:{service_name}"),
                        entity_type: "stack_service".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("stack_service_restarted")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(stack_service_action_response(
            request_id,
            stack_name,
            service_state,
        ));
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn create_stack_run_container(
        &self,
        request: Request<runtime_v2::StackRunContainerRequest>,
    ) -> Result<Response<runtime_v2::StackRunContainerResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateContainer,
            &metadata,
            &request_id,
        )?;

        let stack_name = request.stack_name.trim().to_string();
        if stack_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let service_name = request.service_name.trim().to_string();
        if service_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "service_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let run_service_name = if request.run_service_name.trim().is_empty() {
            generated_stack_run_service_name(&service_name)
        } else {
            request.run_service_name.trim().to_string()
        };
        let (spec, _) = load_stack_service_action_context(
            self.daemon.as_ref(),
            &stack_name,
            &service_name,
            &request_id,
        )?;

        let run_service_state = load_observed_stack_service(
            self.daemon.as_ref(),
            &stack_name,
            &run_service_name,
            &request_id,
        )?;
        if !(run_service_state.phase == ServicePhase::Running
            && run_service_state.container_id.is_some())
        {
            let run_spec = clone_stack_spec_with_run_service(
                &spec,
                &service_name,
                &run_service_name,
                &request_id,
            )?;
            execute_stack_service_action(
                self.daemon.clone(),
                &run_spec,
                Action::ServiceCreate {
                    service_name: run_service_name.clone(),
                },
                &request_id,
                MachineErrorCode::InternalError,
            )?;
        }

        let run_service_state = load_observed_stack_service(
            self.daemon.as_ref(),
            &stack_name,
            &run_service_name,
            &request_id,
        )?;
        let container_id = run_service_state.container_id.clone().ok_or_else(|| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!(
                    "run service `{run_service_name}` in stack `{stack_name}` has no running container"
                ),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "create_stack_run_container".to_string(),
                        entity_id: format!("{stack_name}:{run_service_name}"),
                        entity_type: "stack_run_container".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("stack_run_container_created")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(stack_run_container_response(
            request_id,
            stack_name,
            service_name,
            run_service_name,
            container_id,
        ));
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn remove_stack_run_container(
        &self,
        request: Request<runtime_v2::StackRunContainerRequest>,
    ) -> Result<Response<runtime_v2::StackRunContainerResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::StopContainer,
            &metadata,
            &request_id,
        )?;

        let stack_name = request.stack_name.trim().to_string();
        if stack_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let service_name = request.service_name.trim().to_string();
        if service_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "service_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let run_service_name = request.run_service_name.trim().to_string();
        if run_service_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "run_service_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let (spec, _) = load_stack_service_action_context(
            self.daemon.as_ref(),
            &stack_name,
            &service_name,
            &request_id,
        )?;
        let run_service_state_before = load_observed_stack_service(
            self.daemon.as_ref(),
            &stack_name,
            &run_service_name,
            &request_id,
        )?;
        let container_id = run_service_state_before
            .container_id
            .clone()
            .unwrap_or_default();

        if run_service_state_before.phase != ServicePhase::Stopped
            || run_service_state_before.container_id.is_some()
        {
            let run_spec = clone_stack_spec_with_run_service(
                &spec,
                &service_name,
                &run_service_name,
                &request_id,
            )?;
            execute_stack_service_action(
                self.daemon.clone(),
                &run_spec,
                Action::ServiceRemove {
                    service_name: run_service_name.clone(),
                },
                &request_id,
                MachineErrorCode::StateConflict,
            )?;
        }

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "remove_stack_run_container".to_string(),
                        entity_id: format!("{stack_name}:{run_service_name}"),
                        entity_type: "stack_run_container".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("stack_run_container_removed")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(stack_run_container_response(
            request_id,
            stack_name,
            service_name,
            run_service_name,
            container_id,
        ));
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimedConfig;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use vz_runtime_contract::{Build, RuntimeError};

    #[test]
    fn parse_stack_build_specs_collects_build_entries() {
        let yaml = r#"
services:
  web:
    image: web:latest
    build:
      context: ./web
      dockerfile: Dockerfile.dev
      target: runtime
      args:
        APP_ENV: dev
      cache_from:
        - ghcr.io/acme/web:cache
  worker:
    build: .
"#;

        let builds = parse_stack_build_specs(yaml, ".").expect("build specs");
        assert_eq!(builds.len(), 2);

        let web = builds
            .iter()
            .find(|spec| spec.service_name == "web")
            .expect("web build spec");
        assert_eq!(web.context, "./web");
        assert_eq!(web.dockerfile.as_deref(), Some("Dockerfile.dev"));
        assert_eq!(web.target.as_deref(), Some("runtime"));
        assert_eq!(web.args.get("APP_ENV").map(String::as_str), Some("dev"));
        assert_eq!(web.cache_from, vec!["ghcr.io/acme/web:cache".to_string()]);
    }

    #[test]
    fn parse_stack_spec_rejects_service_healthy_without_healthcheck() {
        let yaml = r#"
services:
  web:
    image: ghcr.io/acme/web:dev
    depends_on:
      db:
        condition: service_healthy
  db:
    image: postgres:16
"#;

        let error = parse_stack_spec("demo", yaml, ".").expect_err("spec should be rejected");
        let message = error.to_string();
        assert!(message.contains("service_healthy"));
        assert!(message.contains("has no healthcheck"));
    }

    #[test]
    fn resolve_build_context_path_handles_relative_and_absolute_paths() {
        let base = PathBuf::from("/tmp/compose");
        let relative = resolve_build_context_path(&base, "./web");
        assert_eq!(relative, PathBuf::from("/tmp/compose").join("./web"));

        let absolute = resolve_build_context_path(&base, "/opt/build");
        assert_eq!(absolute, PathBuf::from("/opt/build"));
    }

    #[test]
    fn build_state_label_is_stable() {
        assert_eq!(build_state_label(BuildState::Queued), "queued");
        assert_eq!(build_state_label(BuildState::Running), "running");
        assert_eq!(build_state_label(BuildState::Succeeded), "succeeded");
        assert_eq!(build_state_label(BuildState::Failed), "failed");
        assert_eq!(build_state_label(BuildState::Canceled), "canceled");
    }

    #[test]
    fn default_stopped_service_uses_stopped_phase() {
        let service = default_stopped_service("api");
        assert_eq!(service.service_name, "api");
        assert_eq!(service.phase, ServicePhase::Stopped);
        assert_eq!(service.container_id, None);
        assert_eq!(service.last_error, None);
        assert!(!service.ready);
    }

    #[test]
    fn stack_service_action_response_wraps_service_status() {
        let response = stack_service_action_response(
            "req-1".to_string(),
            "demo".to_string(),
            ServiceObservedState {
                service_name: "web".to_string(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-web-1".to_string()),
                last_error: None,
                ready: true,
            },
        );

        assert_eq!(response.request_id, "req-1");
        assert_eq!(response.stack_name, "demo");
        let service = response.service.expect("service payload");
        assert_eq!(service.service_name, "web");
        assert_eq!(service.phase, "running");
        assert_eq!(service.container_id, "ctr-web-1");
        assert!(service.ready);
    }

    #[test]
    fn stack_run_container_response_wraps_all_fields() {
        let response = stack_run_container_response(
            "req-1".to_string(),
            "demo".to_string(),
            "web".to_string(),
            "web-run-abc".to_string(),
            "ctr-run-1".to_string(),
        );
        assert_eq!(response.request_id, "req-1");
        assert_eq!(response.stack_name, "demo");
        assert_eq!(response.service_name, "web");
        assert_eq!(response.run_service_name, "web-run-abc");
        assert_eq!(response.container_id, "ctr-run-1");
    }

    #[test]
    fn clone_stack_spec_with_run_service_clones_requested_service() {
        let spec = parse_stack_spec(
            "demo",
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .expect("stack spec");
        let run_spec =
            clone_stack_spec_with_run_service(&spec, "web", "web-run-1", "req-clone-run-service")
                .expect("clone run spec");

        assert_eq!(run_spec.services.len(), spec.services.len() + 1);
        let run_service = run_spec
            .services
            .iter()
            .find(|service| service.name == "web-run-1")
            .expect("run service");
        assert_eq!(run_service.image, "ghcr.io/acme/web:dev");
        assert!(
            run_service.container_name.is_none(),
            "run service should not retain explicit container_name"
        );
    }

    #[test]
    fn clone_stack_spec_with_run_service_preserves_env_mounts_and_resources() {
        let mut spec = parse_stack_spec(
            "demo",
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .expect("stack spec");
        let service = spec
            .services
            .iter_mut()
            .find(|service| service.name == "web")
            .expect("web service");
        service
            .environment
            .insert("APP_ENV".to_string(), "dev".to_string());
        service.mounts.push(vz_stack::MountSpec::Named {
            source: "web-data".to_string(),
            target: "/var/lib/web".to_string(),
            read_only: false,
        });
        service.resources.cpus = Some(2.0);
        service.resources.memory_bytes = Some(512 * 1024 * 1024);

        let run_spec =
            clone_stack_spec_with_run_service(&spec, "web", "web-run-2", "req-clone-run-service")
                .expect("clone run spec");
        let run_service = run_spec
            .services
            .iter()
            .find(|service| service.name == "web-run-2")
            .expect("run service");

        assert_eq!(
            run_service.environment.get("APP_ENV").map(String::as_str),
            Some("dev")
        );
        assert_eq!(run_service.mounts.len(), 1);
        assert_eq!(run_service.resources.cpus, Some(2.0));
        assert_eq!(run_service.resources.memory_bytes, Some(512 * 1024 * 1024));
    }

    struct TestBuildRunner {
        next_build_id: Mutex<u64>,
        start_states: Mutex<VecDeque<BuildState>>,
        poll_states: Mutex<VecDeque<BuildState>>,
        started: Mutex<Vec<(String, BuildSpec)>>,
        build_specs_by_id: Mutex<HashMap<String, (String, BuildSpec)>>,
    }

    impl TestBuildRunner {
        fn new(start_states: Vec<BuildState>, poll_states: Vec<BuildState>) -> Self {
            Self {
                next_build_id: Mutex::new(1),
                start_states: Mutex::new(start_states.into()),
                poll_states: Mutex::new(poll_states.into()),
                started: Mutex::new(Vec::new()),
                build_specs_by_id: Mutex::new(HashMap::new()),
            }
        }

        fn started_specs(&self) -> Vec<(String, BuildSpec)> {
            self.started
                .lock()
                .map(|items| items.clone())
                .unwrap_or_default()
        }

        fn next_state_or_default(
            states: &Mutex<VecDeque<BuildState>>,
            default: BuildState,
        ) -> BuildState {
            match states.lock() {
                Ok(mut guard) => guard.pop_front().unwrap_or(default),
                Err(_) => default,
            }
        }

        fn mk_build(
            build_id: &str,
            sandbox_id: &str,
            spec: &BuildSpec,
            state: BuildState,
        ) -> Build {
            let (result_digest, ended_at) = if state == BuildState::Succeeded {
                (Some("sha256:test-digest".to_string()), Some(2))
            } else if state.is_terminal() {
                (None, Some(2))
            } else {
                (None, None)
            };
            Build {
                build_id: build_id.to_string(),
                sandbox_id: sandbox_id.to_string(),
                build_spec: spec.clone(),
                state,
                result_digest,
                started_at: 1,
                ended_at,
            }
        }
    }

    #[tonic::async_trait]
    impl ComposeBuildRunner for TestBuildRunner {
        async fn start_build(
            &self,
            sandbox_id: &str,
            build_spec: BuildSpec,
        ) -> Result<Build, RuntimeError> {
            let build_id = match self.next_build_id.lock() {
                Ok(mut next) => {
                    let id = format!("build-test-{}", *next);
                    *next += 1;
                    id
                }
                Err(_) => {
                    return Err(RuntimeError::Backend {
                        message: "build id mutex poisoned".to_string(),
                        source: Box::new(std::io::Error::other("build id mutex poisoned")),
                    });
                }
            };

            if let Ok(mut started) = self.started.lock() {
                started.push((sandbox_id.to_string(), build_spec.clone()));
            }
            if let Ok(mut specs) = self.build_specs_by_id.lock() {
                specs.insert(
                    build_id.clone(),
                    (sandbox_id.to_string(), build_spec.clone()),
                );
            }

            let state = Self::next_state_or_default(&self.start_states, BuildState::Succeeded);
            Ok(Self::mk_build(&build_id, sandbox_id, &build_spec, state))
        }

        async fn get_build(&self, build_id: &str) -> Result<Build, RuntimeError> {
            let (sandbox_id, spec) = match self.build_specs_by_id.lock() {
                Ok(specs) => specs
                    .get(build_id)
                    .cloned()
                    .ok_or_else(|| RuntimeError::InvalidConfig("unknown build id".to_string()))?,
                Err(_) => {
                    return Err(RuntimeError::Backend {
                        message: "build spec map mutex poisoned".to_string(),
                        source: Box::new(std::io::Error::other("build spec map mutex poisoned")),
                    });
                }
            };

            let state = Self::next_state_or_default(&self.poll_states, BuildState::Succeeded);
            Ok(Self::mk_build(build_id, &sandbox_id, &spec, state))
        }

        async fn cancel_build(&self, build_id: &str) -> Result<Build, RuntimeError> {
            let (sandbox_id, spec) = match self.build_specs_by_id.lock() {
                Ok(specs) => specs
                    .get(build_id)
                    .cloned()
                    .ok_or_else(|| RuntimeError::InvalidConfig("unknown build id".to_string()))?,
                Err(_) => {
                    return Err(RuntimeError::Backend {
                        message: "build spec map mutex poisoned".to_string(),
                        source: Box::new(std::io::Error::other("build spec map mutex poisoned")),
                    });
                }
            };
            Ok(Self::mk_build(
                build_id,
                &sandbox_id,
                &spec,
                BuildState::Canceled,
            ))
        }
    }

    #[tokio::test]
    async fn run_compose_builds_with_runner_translates_build_spec_and_invokes_build() {
        let compose_yaml = r#"
services:
  web:
    image: ghcr.io/acme/web:dev
    build:
      context: ./web
      dockerfile: Dockerfile.dev
      target: runtime
      args:
        APP_ENV: dev
      cache_from:
        - ghcr.io/acme/web:cache
"#;
        let compose_dir = "/tmp/compose-app";
        let stack_spec = parse_stack_spec("demo", compose_yaml, compose_dir).expect("stack spec");
        let runner = TestBuildRunner::new(vec![BuildState::Succeeded], Vec::new());

        run_compose_builds_with_runner(
            &runner,
            &stack_spec,
            compose_yaml,
            compose_dir,
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .await
        .expect("compose build should succeed");

        let started = runner.started_specs();
        assert_eq!(started.len(), 1);
        let (sandbox_id, spec) = &started[0];
        assert_eq!(sandbox_id, "demo");
        assert_eq!(
            PathBuf::from(&spec.context),
            PathBuf::from("/tmp/compose-app").join("./web")
        );
        assert_eq!(spec.dockerfile.as_deref(), Some("Dockerfile.dev"));
        assert_eq!(spec.target.as_deref(), Some("runtime"));
        assert_eq!(spec.args.get("APP_ENV").map(String::as_str), Some("dev"));
        assert_eq!(spec.cache_from, vec!["ghcr.io/acme/web:cache".to_string()]);
        assert_eq!(spec.image_tag.as_deref(), Some("ghcr.io/acme/web:dev"));
    }

    #[tokio::test]
    async fn run_compose_builds_with_runner_propagates_failed_build_state() {
        let compose_yaml = r#"
services:
  web:
    image: ghcr.io/acme/web:dev
    build:
      context: .
"#;
        let compose_dir = "/tmp/compose-app";
        let stack_spec = parse_stack_spec("demo", compose_yaml, compose_dir).expect("stack spec");
        let runner = TestBuildRunner::new(vec![BuildState::Queued], vec![BuildState::Failed]);

        let error = run_compose_builds_with_runner(
            &runner,
            &stack_spec,
            compose_yaml,
            compose_dir,
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .await
        .expect_err("compose build should fail");

        assert!(
            error.to_string().contains("finished in state failed"),
            "expected failed build state to propagate, got: {error}"
        );
    }

    fn stack_test_daemon() -> (tempfile::TempDir, Arc<RuntimeDaemon>) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let config = RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };
        let daemon = Arc::new(RuntimeDaemon::start(config).expect("daemon start"));
        (tmp, daemon)
    }

    #[tokio::test]
    async fn stop_stack_service_noop_returns_stopped_status() {
        let (_tmp, daemon) = stack_test_daemon();
        let spec = parse_stack_spec(
            "demo",
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .expect("stack spec");
        daemon
            .with_state_store(|store| {
                store.save_desired_state("demo", &spec)?;
                store.save_observed_state(
                    "demo",
                    &ServiceObservedState {
                        service_name: "web".to_string(),
                        phase: ServicePhase::Stopped,
                        container_id: None,
                        last_error: None,
                        ready: false,
                    },
                )?;
                Ok(())
            })
            .expect("persist state");

        let service = StackServiceImpl::new(daemon);
        let response = runtime_v2::stack_service_server::StackService::stop_stack_service(
            &service,
            tonic::Request::new(runtime_v2::StackServiceActionRequest {
                metadata: None,
                stack_name: "demo".to_string(),
                service_name: "web".to_string(),
            }),
        )
        .await
        .expect("stop stack service");

        let payload = response.into_inner();
        let service_status = payload.service.expect("service payload");
        assert_eq!(service_status.service_name, "web");
        assert_eq!(service_status.phase, "stopped");
        assert!(service_status.container_id.is_empty());
    }

    #[tokio::test]
    async fn start_stack_service_noop_for_running_service_returns_running_status() {
        let (_tmp, daemon) = stack_test_daemon();
        let spec = parse_stack_spec(
            "demo",
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .expect("stack spec");
        daemon
            .with_state_store(|store| {
                store.save_desired_state("demo", &spec)?;
                store.save_observed_state(
                    "demo",
                    &ServiceObservedState {
                        service_name: "web".to_string(),
                        phase: ServicePhase::Running,
                        container_id: Some("ctr-web-1".to_string()),
                        last_error: None,
                        ready: true,
                    },
                )?;
                Ok(())
            })
            .expect("persist state");

        let service = StackServiceImpl::new(daemon);
        let response = runtime_v2::stack_service_server::StackService::start_stack_service(
            &service,
            tonic::Request::new(runtime_v2::StackServiceActionRequest {
                metadata: None,
                stack_name: "demo".to_string(),
                service_name: "web".to_string(),
            }),
        )
        .await
        .expect("start stack service");

        let payload = response.into_inner();
        let service_status = payload.service.expect("service payload");
        assert_eq!(service_status.service_name, "web");
        assert_eq!(service_status.phase, "running");
        assert_eq!(service_status.container_id, "ctr-web-1");
        assert!(service_status.ready);
    }

    #[tokio::test]
    async fn stop_stack_service_returns_not_found_for_unknown_service() {
        let (_tmp, daemon) = stack_test_daemon();
        let spec = parse_stack_spec(
            "demo",
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .expect("stack spec");
        daemon
            .with_state_store(|store| {
                store.save_desired_state("demo", &spec)?;
                Ok(())
            })
            .expect("persist desired state");

        let service = StackServiceImpl::new(daemon);
        let error = runtime_v2::stack_service_server::StackService::stop_stack_service(
            &service,
            tonic::Request::new(runtime_v2::StackServiceActionRequest {
                metadata: None,
                stack_name: "demo".to_string(),
                service_name: "api".to_string(),
            }),
        )
        .await
        .expect_err("unknown service should fail");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn remove_stack_run_container_requires_run_service_name() {
        let (_tmp, daemon) = stack_test_daemon();
        let service = StackServiceImpl::new(daemon);
        let error = runtime_v2::stack_service_server::StackService::remove_stack_run_container(
            &service,
            tonic::Request::new(runtime_v2::StackRunContainerRequest {
                metadata: None,
                stack_name: "demo".to_string(),
                service_name: "web".to_string(),
                run_service_name: String::new(),
            }),
        )
        .await
        .expect_err("empty run_service_name should fail");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn create_stack_run_container_requires_service_name() {
        let (_tmp, daemon) = stack_test_daemon();
        let service = StackServiceImpl::new(daemon);
        let error = runtime_v2::stack_service_server::StackService::create_stack_run_container(
            &service,
            tonic::Request::new(runtime_v2::StackRunContainerRequest {
                metadata: None,
                stack_name: "demo".to_string(),
                service_name: String::new(),
                run_service_name: String::new(),
            }),
        )
        .await
        .expect_err("empty service_name should fail");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}
