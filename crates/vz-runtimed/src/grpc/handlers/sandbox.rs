use super::super::*;

#[derive(Clone)]
pub(in crate::grpc) struct SandboxServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl SandboxServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
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

#[tonic::async_trait]
impl runtime_v2::sandbox_service_server::SandboxService for SandboxServiceImpl {
    async fn create_sandbox(
        &self,
        request: Request<runtime_v2::CreateSandboxRequest>,
    ) -> Result<Response<runtime_v2::SandboxResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
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
                return Ok(Response::new(runtime_v2::SandboxResponse {
                    request_id: request_id.clone(),
                    sandbox: Some(sandbox_to_proto_payload(&cached_sandbox)),
                }));
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

        let spec = SandboxSpec {
            cpus,
            memory_mb: if request.memory_mb == 0 {
                None
            } else {
                Some(request.memory_mb)
            },
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
                    return Ok(Response::new(runtime_v2::SandboxResponse {
                        request_id,
                        sandbox: Some(sandbox_to_proto_payload(&cached_sandbox)),
                    }));
                }
            }

            let exists_after_error = self
                .daemon
                .with_state_store(|store| store.load_sandbox(&sandbox_id))
                .map_err(|store_error| status_from_stack_error(store_error, &request_id))?
                .is_some();
            if exists_after_error {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::StateConflict,
                    format!("sandbox already exists: {sandbox_id}"),
                    Some(request_id),
                    BTreeMap::new(),
                )));
            }

            return Err(status_from_stack_error(error, &request_id));
        }

        let mut response = Response::new(runtime_v2::SandboxResponse {
            request_id,
            sandbox: Some(sandbox_to_proto_payload(&sandbox)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
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

    async fn terminate_sandbox(
        &self,
        request: Request<runtime_v2::TerminateSandboxRequest>,
    ) -> Result<Response<runtime_v2::SandboxResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
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
                return Ok(Response::new(runtime_v2::SandboxResponse {
                    request_id: request_id.clone(),
                    sandbox: Some(sandbox_to_proto_payload(&cached_sandbox)),
                }));
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
            terminate_runtime_sandbox_resources(
                self.daemon.clone(),
                &sandbox.sandbox_id,
                &request_id,
            )
            .await?;

            sandbox.state = SandboxState::Terminated;
            sandbox.updated_at = now;
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
                        return Ok(Response::new(runtime_v2::SandboxResponse {
                            request_id,
                            sandbox: Some(sandbox_to_proto_payload(&cached_sandbox)),
                        }));
                    }
                }
                return Err(status_from_stack_error(error, &request_id));
            }

            let mut response = Response::new(runtime_v2::SandboxResponse {
                request_id,
                sandbox: Some(sandbox_to_proto_payload(&sandbox)),
            });
            if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
                response.metadata_mut().insert("x-receipt-id", value);
            }
            return Ok(response);
        }

        Ok(Response::new(runtime_v2::SandboxResponse {
            request_id,
            sandbox: Some(sandbox_to_proto_payload(&sandbox)),
        }))
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
