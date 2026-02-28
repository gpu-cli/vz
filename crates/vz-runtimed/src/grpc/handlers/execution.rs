use super::super::*;

fn resolve_inherited_exec_pty(
    daemon: &RuntimeDaemon,
    container_id: &str,
) -> Result<bool, StackError> {
    daemon.with_state_store(|store| {
        Ok(store
            .resolve_service_exec_pty_default_for_container(container_id)?
            .unwrap_or(false))
    })
}

fn resolve_exec_pty_mode(
    daemon: &RuntimeDaemon,
    request: &runtime_v2::CreateExecutionRequest,
    container_id: &str,
    request_id: &str,
) -> Result<bool, Status> {
    let mode = runtime_v2::create_execution_request::PtyMode::try_from(request.pty_mode).map_err(
        |_| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!("invalid pty_mode value: {}", request.pty_mode),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        },
    )?;

    match mode {
        runtime_v2::create_execution_request::PtyMode::Inherit => {
            resolve_inherited_exec_pty(daemon, container_id)
                .map_err(|error| status_from_stack_error(error, request_id))
        }
        runtime_v2::create_execution_request::PtyMode::Enabled => Ok(true),
        runtime_v2::create_execution_request::PtyMode::Disabled => Ok(false),
    }
}

fn execution_not_found_status(execution_id: &str, request_id: &str) -> Status {
    status_from_machine_error(MachineError::new(
        MachineErrorCode::NotFound,
        format!("execution not found: {execution_id}"),
        Some(request_id.to_string()),
        BTreeMap::new(),
    ))
}

fn terminal_stream_event(execution: &Execution) -> Option<runtime_v2::ExecOutputEvent> {
    let payload = match execution.state {
        ExecutionState::Exited => {
            runtime_v2::exec_output_event::Payload::ExitCode(execution.exit_code.unwrap_or(0))
        }
        ExecutionState::Canceled => {
            runtime_v2::exec_output_event::Payload::ExitCode(execution.exit_code.unwrap_or(130))
        }
        ExecutionState::Failed => runtime_v2::exec_output_event::Payload::Error(format!(
            "execution {} is in failed state",
            execution.execution_id
        )),
        ExecutionState::Queued | ExecutionState::Running => return None,
    };
    Some(runtime_v2::ExecOutputEvent {
        payload: Some(payload),
        sequence: 0,
    })
}

fn session_registry_status(
    error: crate::ExecutionSessionRegistryError,
    request_id: &str,
) -> Status {
    match error {
        crate::ExecutionSessionRegistryError::LockPoisoned => {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::InternalError,
                "execution session registry lock poisoned".to_string(),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        }
        crate::ExecutionSessionRegistryError::NotFound { execution_id } => {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("execution session not found: {execution_id}"),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        }
    }
}

fn runtime_operation_status(
    error: vz_runtime_contract::RuntimeError,
    operation: &str,
    request_id: &str,
) -> Status {
    status_from_machine_error(MachineError::new(
        error.machine_code(),
        format!("runtime operation `{operation}` failed: {error}"),
        Some(request_id.to_string()),
        BTreeMap::new(),
    ))
}

const EXEC_CONTROL_STARTUP_RETRY_MAX_ATTEMPTS: usize = 240;
const EXEC_CONTROL_STARTUP_RETRY_DELAY_MS: u64 = 25;

fn is_retryable_exec_control_error(
    error: &vz_runtime_contract::RuntimeError,
    execution_id: &str,
) -> bool {
    matches!(
        error,
        vz_runtime_contract::RuntimeError::ContainerNotFound { id } if id == execution_id
    )
}

async fn run_exec_control_with_startup_retry<F, Fut>(
    execution_id: &str,
    operation: &str,
    should_continue_retry: impl Fn() -> bool,
    mut operation_fn: F,
) -> Result<(), vz_runtime_contract::RuntimeError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), vz_runtime_contract::RuntimeError>>,
{
    let mut retry_count = 0usize;
    loop {
        match operation_fn().await {
            Ok(()) => return Ok(()),
            Err(error)
                if retry_count < EXEC_CONTROL_STARTUP_RETRY_MAX_ATTEMPTS
                    && is_retryable_exec_control_error(&error, execution_id) =>
            {
                if !should_continue_retry() {
                    return Err(error);
                }
                retry_count += 1;
                warn!(
                    execution_id = %execution_id,
                    operation = %operation,
                    retry_count,
                    max_retries = EXEC_CONTROL_STARTUP_RETRY_MAX_ATTEMPTS,
                    "execution control operation hit startup race; retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(
                    EXEC_CONTROL_STARTUP_RETRY_DELAY_MS,
                ))
                .await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn execution_transition_stack_error(message: impl Into<String>) -> StackError {
    StackError::Machine {
        code: MachineErrorCode::StateConflict,
        message: message.into(),
    }
}

fn build_exec_command(spec: &ExecutionSpec) -> Vec<String> {
    let mut command = spec.cmd.clone();
    command.extend(spec.args.clone());
    command
}

fn resolve_runtime_env_override(
    env_key: &str,
    value: &str,
) -> Result<String, vz_runtime_contract::RuntimeError> {
    let Some(source_env_var) = value.strip_prefix(SANDBOX_RUNTIME_ENV_REF_PREFIX) else {
        return Ok(value.to_string());
    };

    let source_env_var = source_env_var.trim();
    if source_env_var.is_empty() {
        return Err(vz_runtime_contract::RuntimeError::InvalidConfig(format!(
            "execution env override `{env_key}` uses an empty runtime env reference source"
        )));
    }

    match std::env::var(source_env_var) {
        Ok(source_value) if !source_value.is_empty() => Ok(source_value),
        Ok(_) => Err(vz_runtime_contract::RuntimeError::InvalidConfig(format!(
            "runtime env source `{source_env_var}` for execution env `{env_key}` is empty"
        ))),
        Err(std::env::VarError::NotPresent) => {
            Err(vz_runtime_contract::RuntimeError::InvalidConfig(format!(
                "runtime env source `{source_env_var}` for execution env `{env_key}` is not set"
            )))
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(vz_runtime_contract::RuntimeError::InvalidConfig(format!(
                "runtime env source `{source_env_var}` for execution env `{env_key}` is not valid UTF-8"
            )))
        }
    }
}

fn exec_config_from_execution(
    execution: &Execution,
) -> Result<vz_runtime_contract::ExecConfig, vz_runtime_contract::RuntimeError> {
    let mut env = Vec::with_capacity(execution.exec_spec.env_override.len());
    for (key, value) in &execution.exec_spec.env_override {
        env.push((key.clone(), resolve_runtime_env_override(key, value)?));
    }

    Ok(vz_runtime_contract::ExecConfig {
        execution_id: Some(execution.execution_id.clone()),
        cmd: build_exec_command(&execution.exec_spec),
        env,
        working_dir: None,
        user: None,
        pty: execution.exec_spec.pty,
        term_rows: if execution.exec_spec.pty {
            Some(24)
        } else {
            None
        },
        term_cols: if execution.exec_spec.pty {
            Some(80)
        } else {
            None
        },
        timeout: execution
            .exec_spec
            .timeout_secs
            .map(std::time::Duration::from_secs),
    })
}

fn should_start_execution_task(
    daemon: &RuntimeDaemon,
    execution: &Execution,
) -> Result<bool, StackError> {
    if execution.state != ExecutionState::Queued {
        return Ok(false);
    }
    daemon.with_state_store(|store| Ok(store.load_container(&execution.container_id)?.is_some()))
}

fn update_execution_running(
    daemon: &RuntimeDaemon,
    execution_id: &str,
) -> Result<Option<Execution>, StackError> {
    daemon.with_state_store(|store| {
        let Some(mut execution) = store.load_execution(execution_id)? else {
            return Ok(None);
        };

        match execution.state {
            ExecutionState::Queued => {
                let now = current_unix_secs();
                execution.started_at = Some(now);
                execution
                    .transition_to(ExecutionState::Running)
                    .map_err(|error| execution_transition_stack_error(error.to_string()))?;
                store.with_immediate_transaction(|tx| {
                    tx.save_execution(&execution)?;
                    tx.emit_event(
                        "api",
                        &StackEvent::ExecutionRunning {
                            execution_id: execution.execution_id.clone(),
                        },
                    )?;
                    Ok(())
                })?;
                Ok(Some(execution))
            }
            ExecutionState::Running => Ok(Some(execution)),
            _ => Ok(None),
        }
    })
}

fn update_execution_terminal(
    daemon: &RuntimeDaemon,
    execution_id: &str,
    target_state: ExecutionState,
    exit_code: Option<i32>,
    failure_error: Option<String>,
) -> Result<Option<Execution>, StackError> {
    daemon.with_state_store(|store| {
        let Some(mut execution) = store.load_execution(execution_id)? else {
            return Ok(None);
        };

        if execution.state.is_terminal() {
            return Ok(Some(execution));
        }

        let now = current_unix_secs();
        if execution.state == ExecutionState::Queued {
            execution.started_at = Some(now);
            execution
                .transition_to(ExecutionState::Running)
                .map_err(|error| execution_transition_stack_error(error.to_string()))?;
        }

        execution.ended_at = Some(now);
        execution.exit_code = exit_code;
        execution
            .transition_to(target_state)
            .map_err(|error| execution_transition_stack_error(error.to_string()))?;

        store.with_immediate_transaction(|tx| {
            tx.save_execution(&execution)?;
            match target_state {
                ExecutionState::Exited => {
                    tx.emit_event(
                        "api",
                        &StackEvent::ExecutionExited {
                            execution_id: execution.execution_id.clone(),
                            exit_code: exit_code.unwrap_or_default(),
                        },
                    )?;
                }
                ExecutionState::Failed => {
                    tx.emit_event(
                        "api",
                        &StackEvent::ExecutionFailed {
                            execution_id: execution.execution_id.clone(),
                            error: failure_error
                                .clone()
                                .unwrap_or_else(|| "execution failed".to_string()),
                        },
                    )?;
                }
                ExecutionState::Canceled => {
                    tx.emit_event(
                        "api",
                        &StackEvent::ExecutionCanceled {
                            execution_id: execution.execution_id.clone(),
                        },
                    )?;
                }
                ExecutionState::Queued | ExecutionState::Running => {}
            }
            Ok(())
        })?;

        Ok(Some(execution))
    })
}

#[cfg(target_os = "macos")]
async fn execute_backend_execution(
    daemon: &RuntimeDaemon,
    execution: &Execution,
) -> Result<(vz_runtime_contract::ExecOutput, bool), vz_runtime_contract::RuntimeError> {
    let execution_id = execution.execution_id.clone();
    let output = daemon
        .manager()
        .backend()
        .exec_container_streaming(
            &execution.container_id,
            exec_config_from_execution(execution)?,
            |event| match event {
                vz_oci_macos::InteractiveExecEvent::Stdout(stdout) => {
                    let _ = daemon
                        .execution_sessions()
                        .publish_stdout(&execution_id, stdout);
                }
                vz_oci_macos::InteractiveExecEvent::Stderr(stderr) => {
                    let _ = daemon
                        .execution_sessions()
                        .publish_stderr(&execution_id, stderr);
                }
                vz_oci_macos::InteractiveExecEvent::Exit(exit_code) => {
                    let _ = daemon
                        .execution_sessions()
                        .publish_exit_code(&execution_id, exit_code);
                }
            },
        )
        .await?;
    Ok((output, true))
}

#[cfg(not(target_os = "macos"))]
async fn execute_backend_execution(
    daemon: &RuntimeDaemon,
    execution: &Execution,
) -> Result<(vz_runtime_contract::ExecOutput, bool), vz_runtime_contract::RuntimeError> {
    let output = daemon
        .manager()
        .exec_container(
            &execution.container_id,
            exec_config_from_execution(execution)?,
        )
        .await?;
    Ok((output, false))
}

async fn run_execution_task(daemon: Arc<RuntimeDaemon>, execution_id: String) {
    let running_execution = match update_execution_running(daemon.as_ref(), &execution_id) {
        Ok(execution) => execution,
        Err(error) => {
            warn!(
                execution_id = %execution_id,
                error = %error,
                "failed to transition execution to running"
            );
            let _ = daemon
                .execution_sessions()
                .publish_error(&execution_id, format!("execution could not start: {error}"));
            let _ = daemon.execution_sessions().remove(&execution_id);
            return;
        }
    };

    let Some(execution) = running_execution else {
        let _ = daemon.execution_sessions().remove(&execution_id);
        return;
    };

    let result = execute_backend_execution(daemon.as_ref(), &execution).await;

    match result {
        Ok((output, emitted_live_events)) => {
            if !emitted_live_events && !output.stdout.is_empty() {
                let _ = daemon
                    .execution_sessions()
                    .publish_stdout(&execution_id, output.stdout.as_bytes().to_vec());
            }
            if !emitted_live_events && !output.stderr.is_empty() {
                let _ = daemon
                    .execution_sessions()
                    .publish_stderr(&execution_id, output.stderr.as_bytes().to_vec());
            }
            if !emitted_live_events {
                let _ = daemon
                    .execution_sessions()
                    .publish_exit_code(&execution_id, output.exit_code);
            }

            if let Err(error) = update_execution_terminal(
                daemon.as_ref(),
                &execution_id,
                ExecutionState::Exited,
                Some(output.exit_code),
                None,
            ) {
                warn!(
                    execution_id = %execution_id,
                    error = %error,
                    "failed to persist exited execution state"
                );
            }
        }
        Err(error) => {
            let error_message = format!("runtime execution failed: {error}");
            let _ = daemon
                .execution_sessions()
                .publish_error(&execution_id, error_message.clone());
            if let Err(persist_error) = update_execution_terminal(
                daemon.as_ref(),
                &execution_id,
                ExecutionState::Failed,
                None,
                Some(error_message),
            ) {
                warn!(
                    execution_id = %execution_id,
                    error = %persist_error,
                    "failed to persist failed execution state"
                );
            }
        }
    }

    let _ = daemon.execution_sessions().remove(&execution_id);
}

fn maybe_spawn_execution_task(
    daemon: Arc<RuntimeDaemon>,
    execution: &Execution,
    request_id: &str,
) -> Result<(), Status> {
    if !should_start_execution_task(daemon.as_ref(), execution)
        .map_err(|error| status_from_stack_error(error, request_id))?
    {
        match daemon.execution_sessions().remove(&execution.execution_id) {
            Ok(()) | Err(crate::ExecutionSessionRegistryError::NotFound { .. }) => {}
            Err(error) => return Err(session_registry_status(error, request_id)),
        }
        return Ok(());
    }

    let execution_id = execution.execution_id.clone();
    let daemon_task = daemon.clone();
    let task = tokio::spawn(async move {
        run_execution_task(daemon_task, execution_id).await;
    });
    let abort_handle = task.abort_handle();

    match daemon
        .execution_sessions()
        .attach_task_abort(&execution.execution_id, abort_handle)
    {
        Ok(()) => Ok(()),
        Err(crate::ExecutionSessionRegistryError::NotFound { .. }) => Ok(()),
        Err(other) => {
            task.abort();
            Err(session_registry_status(other, request_id))
        }
    }
}

pub(in crate::grpc) struct ExecutionServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl ExecutionServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

#[tonic::async_trait]
impl runtime_v2::execution_service_server::ExecutionService for ExecutionServiceImpl {
    type StreamExecOutputStream =
        tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::ExecOutputEvent, Status>>;

    async fn create_execution(
        &self,
        request: Request<runtime_v2::CreateExecutionRequest>,
    ) -> Result<Response<runtime_v2::ExecutionResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::ExecContainer,
            &metadata,
            &request_id,
        )?;
        let idempotency_key = metadata.idempotency_key.clone();
        let normalized_idempotency_key = normalize_idempotency_key(idempotency_key.as_deref());

        let container_id = request.container_id.trim().to_string();
        if container_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "container_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let resolved_pty =
            resolve_exec_pty_mode(self.daemon.as_ref(), &request, &container_id, &request_id)?;
        let request_hash = create_execution_request_hash(&request);

        if let Some(key) = normalized_idempotency_key {
            if let Some(cached_execution) = load_idempotent_execution_replay(
                &self.daemon,
                key,
                "create_execution",
                &request_hash,
                &request_id,
            )? {
                if !cached_execution.state.is_terminal() {
                    self.daemon
                        .execution_sessions()
                        .register(&cached_execution.execution_id)
                        .map_err(|error| session_registry_status(error, &request_id))?;
                    maybe_spawn_execution_task(
                        self.daemon.clone(),
                        &cached_execution,
                        &request_id,
                    )?;
                }
                return Ok(Response::new(runtime_v2::ExecutionResponse {
                    request_id: request_id.clone(),
                    execution: Some(execution_to_proto_payload(&cached_execution)),
                }));
            }
        }

        let execution = Execution {
            execution_id: generate_execution_id(),
            container_id: container_id.clone(),
            exec_spec: ExecutionSpec {
                cmd: request.cmd,
                args: request.args,
                env_override: request.env_override.into_iter().collect(),
                pty: resolved_pty,
                timeout_secs: if request.timeout_secs == 0 {
                    None
                } else {
                    Some(request.timeout_secs)
                },
            },
            state: ExecutionState::Queued,
            exit_code: None,
            started_at: None,
            ended_at: None,
        };

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        let persist_result = self.daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_execution(&execution)?;
                tx.emit_event(
                    "api",
                    &StackEvent::ExecutionQueued {
                        container_id: execution.container_id.clone(),
                        execution_id: execution.execution_id.clone(),
                    },
                )?;
                tx.save_receipt(&Receipt {
                    receipt_id: receipt_id.clone(),
                    operation: "create_execution".to_string(),
                    entity_id: execution.execution_id.clone(),
                    entity_type: "execution".to_string(),
                    request_id: request_id.clone(),
                    status: "success".to_string(),
                    created_at: now,
                    metadata: receipt_idempotent_mutation_metadata(
                        "execution_queued",
                        request_hash.as_str(),
                        normalized_idempotency_key,
                    )?,
                })?;
                if let Some(key) = normalized_idempotency_key {
                    tx.save_idempotency_result(&IdempotencyRecord {
                        key: key.to_string(),
                        operation: "create_execution".to_string(),
                        request_hash: request_hash.clone(),
                        response_json: execution.execution_id.clone(),
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
                if let Some(cached_execution) = load_idempotent_execution_replay(
                    &self.daemon,
                    key,
                    "create_execution",
                    &request_hash,
                    &request_id,
                )? {
                    if !cached_execution.state.is_terminal() {
                        self.daemon
                            .execution_sessions()
                            .register(&cached_execution.execution_id)
                            .map_err(|error| session_registry_status(error, &request_id))?;
                        maybe_spawn_execution_task(
                            self.daemon.clone(),
                            &cached_execution,
                            &request_id,
                        )?;
                    }
                    return Ok(Response::new(runtime_v2::ExecutionResponse {
                        request_id,
                        execution: Some(execution_to_proto_payload(&cached_execution)),
                    }));
                }
            }
            return Err(status_from_stack_error(error, &request_id));
        }

        self.daemon
            .execution_sessions()
            .register(&execution.execution_id)
            .map_err(|error| session_registry_status(error, &request_id))?;
        maybe_spawn_execution_task(self.daemon.clone(), &execution, &request_id)?;

        let mut response = Response::new(runtime_v2::ExecutionResponse {
            request_id,
            execution: Some(execution_to_proto_payload(&execution)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn get_execution(
        &self,
        request: Request<runtime_v2::GetExecutionRequest>,
    ) -> Result<Response<runtime_v2::ExecutionResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let execution = self
            .daemon
            .with_state_store(|store| store.load_execution(&request.execution_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("execution not found: {}", request.execution_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        Ok(Response::new(runtime_v2::ExecutionResponse {
            request_id,
            execution: Some(execution_to_proto_payload(&execution)),
        }))
    }

    async fn list_executions(
        &self,
        request: Request<runtime_v2::ListExecutionsRequest>,
    ) -> Result<Response<runtime_v2::ListExecutionsResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let executions = self
            .daemon
            .with_state_store(|store| store.list_executions())
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .iter()
            .map(execution_to_proto_payload)
            .collect();

        Ok(Response::new(runtime_v2::ListExecutionsResponse {
            request_id,
            executions,
        }))
    }

    async fn cancel_execution(
        &self,
        request: Request<runtime_v2::CancelExecutionRequest>,
    ) -> Result<Response<runtime_v2::ExecutionResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CancelExec,
            &metadata,
            &request_id,
        )?;

        let mut execution = self
            .daemon
            .with_state_store(|store| store.load_execution(&request.execution_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("execution not found: {}", request.execution_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if execution.state.is_terminal() {
            return Ok(Response::new(runtime_v2::ExecutionResponse {
                request_id,
                execution: Some(execution_to_proto_payload(&execution)),
            }));
        }

        let task_abort_result = match self
            .daemon
            .execution_sessions()
            .abort_task(&execution.execution_id)
        {
            Ok(result) => result,
            Err(crate::ExecutionSessionRegistryError::NotFound { .. }) => false,
            Err(error) => return Err(session_registry_status(error, &request_id)),
        };

        if execution.state == ExecutionState::Running {
            let cancel_result = self
                .daemon
                .manager()
                .cancel_exec(&execution.execution_id)
                .await;
            if let Err(error) = cancel_result
                && (!matches!(
                    error,
                    vz_runtime_contract::RuntimeError::UnsupportedOperation { .. }
                ) || !task_abort_result)
            {
                return Err(runtime_operation_status(error, "cancel_exec", &request_id));
            }
        }

        let now = current_unix_secs();
        if execution.started_at.is_none() {
            execution.started_at = Some(now);
        }
        execution.exit_code = Some(130);
        execution.ended_at = Some(now);
        execution
            .transition_to(ExecutionState::Canceled)
            .map_err(|error| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::StateConflict,
                    error.to_string(),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_execution(&execution)?;
                    tx.emit_event(
                        "api",
                        &StackEvent::ExecutionCanceled {
                            execution_id: execution.execution_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "cancel_execution".to_string(),
                        entity_id: execution.execution_id.clone(),
                        entity_type: "execution".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("execution_canceled")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let exit_code = execution.exit_code.unwrap_or(130);
        let _ = self
            .daemon
            .execution_sessions()
            .publish_exit_code(&execution.execution_id, exit_code);
        let _ = self
            .daemon
            .execution_sessions()
            .remove(&execution.execution_id);

        let mut response = Response::new(runtime_v2::ExecutionResponse {
            request_id,
            execution: Some(execution_to_proto_payload(&execution)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn stream_exec_output(
        &self,
        request: Request<runtime_v2::StreamExecOutputRequest>,
    ) -> Result<Response<Self::StreamExecOutputStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let execution_id = request.execution_id.trim().to_string();
        if execution_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "execution_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let execution = self
            .daemon
            .with_state_store(|store| store.load_execution(&execution_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| execution_not_found_status(&execution_id, &request_id))?;

        let mut session_rx = match self.daemon.execution_sessions().subscribe(&execution_id) {
            Ok(receiver) => receiver,
            Err(crate::ExecutionSessionRegistryError::NotFound { .. }) => {
                if let Some(event) = terminal_stream_event(&execution) {
                    let (tx, rx) = tokio::sync::mpsc::channel::<
                        Result<runtime_v2::ExecOutputEvent, Status>,
                    >(1);
                    tokio::spawn(async move {
                        let _ = tx.send(Ok(event)).await;
                    });
                    return Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
                        rx,
                    )));
                }
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::UnsupportedOperation,
                    format!("execution session is not active for {execution_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                )));
            }
            Err(other) => return Err(session_registry_status(other, &request_id)),
        };

        let (tx, rx) =
            tokio::sync::mpsc::channel::<Result<runtime_v2::ExecOutputEvent, Status>>(32);
        let stream_request_id = request_id.clone();
        tokio::spawn(async move {
            loop {
                match session_rx.recv().await {
                    Ok(event) => {
                        if tx.send(Ok(event)).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        let lagged = runtime_v2::ExecOutputEvent {
                            payload: Some(runtime_v2::exec_output_event::Payload::Error(format!(
                                "execution output lagged; dropped {skipped} events"
                            ))),
                            sequence: 0,
                        };
                        if tx.send(Ok(lagged)).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        let _ = tx
                            .send(Ok(runtime_v2::ExecOutputEvent {
                                payload: Some(runtime_v2::exec_output_event::Payload::Error(
                                    format!(
                                        "execution output stream closed (request_id={stream_request_id})"
                                    ),
                                )),
                                sequence: 0,
                            }))
                            .await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn write_exec_stdin(
        &self,
        request: Request<runtime_v2::WriteExecStdinRequest>,
    ) -> Result<Response<runtime_v2::ExecutionResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::WriteExecStdin,
            &metadata,
            &request_id,
        )?;
        let execution_id = request.execution_id.trim().to_string();
        if execution_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "execution_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let execution = self
            .daemon
            .with_state_store(|store| store.load_execution(&execution_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| execution_not_found_status(&execution_id, &request_id))?;

        if execution.state.is_terminal() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("execution {execution_id} is in terminal state"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let has_session = self
            .daemon
            .execution_sessions()
            .contains(&execution_id)
            .map_err(|error| session_registry_status(error, &request_id))?;
        if !has_session {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::UnsupportedOperation,
                format!("execution session is not active for {execution_id}"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let manager = self.daemon.manager();
        let daemon = self.daemon.clone();
        let data = request.data.clone();
        run_exec_control_with_startup_retry(
            &execution_id,
            "write_exec_stdin",
            || match daemon.execution_sessions().contains(&execution_id) {
                Ok(active) => active,
                Err(error) => {
                    warn!(
                        execution_id = %execution_id,
                        error = %error,
                        "failed to inspect execution session registry during stdin retry"
                    );
                    false
                }
            },
            || manager.write_exec_stdin(&execution_id, &data),
        )
        .await
        .map_err(|error| runtime_operation_status(error, "write_exec_stdin", &request_id))?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.emit_event(
                        "api",
                        &StackEvent::ExecutionRunning {
                            execution_id: execution_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "write_exec_stdin".to_string(),
                        entity_id: execution.execution_id.clone(),
                        entity_type: "execution".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_execution_stdin_metadata(request.data.len())?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::ExecutionResponse {
            request_id,
            execution: Some(execution_to_proto_payload(&execution)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn resize_exec_pty(
        &self,
        request: Request<runtime_v2::ResizeExecPtyRequest>,
    ) -> Result<Response<runtime_v2::ExecutionResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::ResizeExecPty,
            &metadata,
            &request_id,
        )?;

        let execution_id = request.execution_id.trim().to_string();
        if execution_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "execution_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let execution = self
            .daemon
            .with_state_store(|store| store.load_execution(&execution_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("execution not found: {execution_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if execution.state != ExecutionState::Running {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("execution {execution_id} is not running"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        if !execution.exec_spec.pty {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::UnsupportedOperation,
                format!("execution PTY is disabled for {execution_id}"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let has_session = self
            .daemon
            .execution_sessions()
            .contains(&execution_id)
            .map_err(|error| session_registry_status(error, &request_id))?;
        if !has_session {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::UnsupportedOperation,
                format!("execution session is not active for {execution_id}"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let cols = u16::try_from(request.cols).map_err(|_| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!("cols out of range for u16: {}", request.cols),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;
        let rows = u16::try_from(request.rows).map_err(|_| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!("rows out of range for u16: {}", request.rows),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;

        let manager = self.daemon.manager();
        let daemon = self.daemon.clone();
        run_exec_control_with_startup_retry(
            &execution_id,
            "resize_exec_pty",
            || match daemon.execution_sessions().contains(&execution_id) {
                Ok(active) => active,
                Err(error) => {
                    warn!(
                        execution_id = %execution_id,
                        error = %error,
                        "failed to inspect execution session registry during resize retry"
                    );
                    false
                }
            },
            || manager.resize_exec_pty(&execution_id, cols, rows),
        )
        .await
        .map_err(|error| runtime_operation_status(error, "resize_exec_pty", &request_id))?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.emit_event(
                        "api",
                        &StackEvent::ExecutionResized {
                            execution_id: execution_id.clone(),
                            cols,
                            rows,
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "resize_exec_pty".to_string(),
                        entity_id: execution.execution_id.clone(),
                        entity_type: "execution".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_execution_resized_metadata(cols, rows)?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::ExecutionResponse {
            request_id,
            execution: Some(execution_to_proto_payload(&execution)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn signal_exec(
        &self,
        request: Request<runtime_v2::SignalExecRequest>,
    ) -> Result<Response<runtime_v2::ExecutionResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::SignalExec,
            &metadata,
            &request_id,
        )?;

        let execution_id = request.execution_id.trim().to_string();
        if execution_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "execution_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let signal = request.signal.trim().to_string();
        if signal.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "signal cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let execution = self
            .daemon
            .with_state_store(|store| store.load_execution(&execution_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("execution not found: {execution_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if execution.state != ExecutionState::Running {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("execution {execution_id} is not running"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let has_session = self
            .daemon
            .execution_sessions()
            .contains(&execution_id)
            .map_err(|error| session_registry_status(error, &request_id))?;
        if !has_session {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::UnsupportedOperation,
                format!("execution session is not active for {execution_id}"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let manager = self.daemon.manager();
        let daemon = self.daemon.clone();
        run_exec_control_with_startup_retry(
            &execution_id,
            "signal_exec",
            || match daemon.execution_sessions().contains(&execution_id) {
                Ok(active) => active,
                Err(error) => {
                    warn!(
                        execution_id = %execution_id,
                        error = %error,
                        "failed to inspect execution session registry during signal retry"
                    );
                    false
                }
            },
            || manager.signal_exec(&execution_id, &signal),
        )
        .await
        .map_err(|error| runtime_operation_status(error, "signal_exec", &request_id))?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.emit_event(
                        "api",
                        &StackEvent::ExecutionSignaled {
                            execution_id: execution_id.clone(),
                            signal: signal.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "signal_exec".to_string(),
                        entity_id: execution.execution_id.clone(),
                        entity_type: "execution".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_execution_signaled_metadata(signal.as_str())?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::ExecutionResponse {
            request_id,
            execution: Some(execution_to_proto_payload(&execution)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn exec_control_startup_retry_retries_container_not_found_for_execution_id() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_operation = Arc::clone(&attempts);
        let result = run_exec_control_with_startup_retry(
            "exec-race",
            "write_exec_stdin",
            || true,
            move || {
                let attempts_for_operation = Arc::clone(&attempts_for_operation);
                async move {
                    let attempt = attempts_for_operation.fetch_add(1, Ordering::Relaxed);
                    if attempt < 2 {
                        Err(vz_runtime_contract::RuntimeError::ContainerNotFound {
                            id: "exec-race".to_string(),
                        })
                    } else {
                        Ok(())
                    }
                }
            },
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn exec_control_startup_retry_does_not_retry_non_retryable_error() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_operation = Arc::clone(&attempts);
        let result = run_exec_control_with_startup_retry(
            "exec-race",
            "write_exec_stdin",
            || true,
            move || {
                let attempts_for_operation = Arc::clone(&attempts_for_operation);
                async move {
                    attempts_for_operation.fetch_add(1, Ordering::Relaxed);
                    Err(vz_runtime_contract::RuntimeError::UnsupportedOperation {
                        operation: "write_exec_stdin".to_string(),
                        reason: "not interactive".to_string(),
                    })
                }
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(vz_runtime_contract::RuntimeError::UnsupportedOperation { .. })
        ));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn exec_control_startup_retry_stops_when_session_is_no_longer_active() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_operation = Arc::clone(&attempts);
        let session_checks = Arc::new(AtomicUsize::new(0));
        let session_checks_for_retry = Arc::clone(&session_checks);
        let result = run_exec_control_with_startup_retry(
            "exec-race",
            "write_exec_stdin",
            move || session_checks_for_retry.fetch_add(1, Ordering::Relaxed) == 0,
            move || {
                let attempts_for_operation = Arc::clone(&attempts_for_operation);
                async move {
                    attempts_for_operation.fetch_add(1, Ordering::Relaxed);
                    Err(vz_runtime_contract::RuntimeError::ContainerNotFound {
                        id: "exec-race".to_string(),
                    })
                }
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(vz_runtime_contract::RuntimeError::ContainerNotFound { .. })
        ));
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert_eq!(session_checks.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn exec_config_from_execution_resolves_runtime_env_references() {
        let home = std::env::var("HOME").expect("HOME should be set for test");
        let execution = Execution {
            execution_id: "exec-runtime-ref".to_string(),
            container_id: "ctr-runtime-ref".to_string(),
            exec_spec: ExecutionSpec {
                cmd: vec!["echo".to_string()],
                args: vec!["hello".to_string()],
                env_override: BTreeMap::from([
                    (
                        "workspace_home".to_string(),
                        format!("{SANDBOX_RUNTIME_ENV_REF_PREFIX}HOME"),
                    ),
                    ("plain".to_string(), "value".to_string()),
                ]),
                pty: false,
                timeout_secs: None,
            },
            state: ExecutionState::Queued,
            exit_code: None,
            started_at: None,
            ended_at: None,
        };

        let resolved =
            exec_config_from_execution(&execution).expect("env references should resolve");
        let resolved_env: BTreeMap<_, _> = resolved.env.into_iter().collect();
        assert_eq!(
            resolved_env.get("workspace_home").map(String::as_str),
            Some(home.as_str())
        );
        assert_eq!(resolved_env.get("plain").map(String::as_str), Some("value"));
    }

    #[test]
    fn exec_config_from_execution_rejects_missing_runtime_env_reference() {
        let missing_source = (0..16u8)
            .map(|attempt| format!("VZ_RUNTIME_ENV_REF_MISSING_{attempt}"))
            .find(|candidate| std::env::var(candidate).is_err())
            .expect("expected to find an unset env var");
        let execution = Execution {
            execution_id: "exec-runtime-ref-missing".to_string(),
            container_id: "ctr-runtime-ref-missing".to_string(),
            exec_spec: ExecutionSpec {
                cmd: vec!["echo".to_string()],
                args: Vec::new(),
                env_override: BTreeMap::from([(
                    "db_password".to_string(),
                    format!("{SANDBOX_RUNTIME_ENV_REF_PREFIX}{missing_source}"),
                )]),
                pty: false,
                timeout_secs: None,
            },
            state: ExecutionState::Queued,
            exit_code: None,
            started_at: None,
            ended_at: None,
        };

        let error = exec_config_from_execution(&execution)
            .expect_err("missing runtime env source should fail execution config resolution");
        assert!(
            matches!(error, vz_runtime_contract::RuntimeError::InvalidConfig(message) if message.contains(missing_source.as_str())),
            "error should mention missing runtime env source"
        );
    }
}
