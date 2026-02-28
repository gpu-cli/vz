use super::*;

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
        let mut labels: BTreeMap<String, String> = request.labels.into_iter().collect();
        // Requesters cannot predeclare default-source audit labels.
        labels.remove(SANDBOX_LABEL_BASE_IMAGE_DEFAULT_SOURCE);
        labels.remove(SANDBOX_LABEL_MAIN_CONTAINER_DEFAULT_SOURCE);

        let requested_base_image_ref = labels
            .get(SANDBOX_LABEL_BASE_IMAGE_REF)
            .and_then(|value| normalize_optional_wire_field(value));
        let requested_main_container = labels
            .get(SANDBOX_LABEL_MAIN_CONTAINER)
            .and_then(|value| normalize_optional_wire_field(value));
        let startup_defaults = self
            .daemon
            .resolve_sandbox_startup_defaults(requested_base_image_ref, requested_main_container);

        if let Some(base_image_ref) = startup_defaults.base_image_ref.as_deref() {
            labels.insert(
                SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
                base_image_ref.to_string(),
            );
        }
        if let Some(main_container) = startup_defaults.main_container.as_deref() {
            labels.insert(
                SANDBOX_LABEL_MAIN_CONTAINER.to_string(),
                main_container.to_string(),
            );
        }
        if let Some(default_source) = startup_defaults.base_image_default_source {
            labels.insert(
                SANDBOX_LABEL_BASE_IMAGE_DEFAULT_SOURCE.to_string(),
                default_source.as_label_value().to_string(),
            );
        }
        if let Some(default_source) = startup_defaults.main_container_default_source {
            labels.insert(
                SANDBOX_LABEL_MAIN_CONTAINER_DEFAULT_SOURCE.to_string(),
                default_source.as_label_value().to_string(),
            );
        }

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
        if startup_defaults.base_image_default_source.is_some()
            || startup_defaults.main_container_default_source.is_some()
        {
            sequence += 1;
            events.push(Ok(create_sandbox_progress_event(
                &request_id,
                sequence,
                "applying_defaults",
                "applying daemon sandbox startup policy defaults",
            )));
        }
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
            base_image_ref: startup_defaults.base_image_ref,
            main_container: startup_defaults.main_container,
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
        let execution_service =
            super::super::execution::ExecutionServiceImpl::new(self.daemon.clone());
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
