use super::super::*;
pub(in crate::grpc) struct CheckpointServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl CheckpointServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

fn checkpoint_class_from_wire(value: &str) -> Result<CheckpointClass, MachineError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "fs_quick" | "fs-quick" => Ok(CheckpointClass::FsQuick),
        "vm_full" | "vm-full" => Ok(CheckpointClass::VmFull),
        other => Err(MachineError::new(
            MachineErrorCode::ValidationError,
            format!("unsupported checkpoint class: {other}"),
            None,
            BTreeMap::new(),
        )),
    }
}

#[tonic::async_trait]
impl runtime_v2::checkpoint_service_server::CheckpointService for CheckpointServiceImpl {
    async fn create_checkpoint(
        &self,
        request: Request<runtime_v2::CreateCheckpointRequest>,
    ) -> Result<Response<runtime_v2::CheckpointResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateCheckpoint,
            &metadata,
            &request_id,
        )?;
        let idempotency_key = metadata.idempotency_key.clone();
        let normalized_idempotency_key = normalize_idempotency_key(idempotency_key.as_deref());

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "sandbox_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let request_hash = create_checkpoint_request_hash(
            &sandbox_id,
            &request.checkpoint_class,
            &request.compatibility_fingerprint,
        );

        let class = checkpoint_class_from_wire(&request.checkpoint_class).map_err(|error| {
            status_from_machine_error(MachineError::new(
                error.code,
                error.message,
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;

        let capabilities = self.daemon.capabilities();
        match class {
            CheckpointClass::VmFull if !capabilities.vm_full_checkpoint => {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::UnsupportedOperation,
                    "VM full checkpoints are not supported by the current backend".to_string(),
                    Some(request_id),
                    BTreeMap::new(),
                )));
            }
            CheckpointClass::FsQuick if !capabilities.fs_quick_checkpoint => {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::UnsupportedOperation,
                    "Filesystem quick checkpoints are not supported by the current backend"
                        .to_string(),
                    Some(request_id),
                    BTreeMap::new(),
                )));
            }
            _ => {}
        }

        if let Some(key) = normalized_idempotency_key {
            if let Some(cached_checkpoint) = load_idempotent_checkpoint_replay(
                &self.daemon,
                key,
                "create_checkpoint",
                &request_hash,
                &request_id,
            )? {
                return Ok(Response::new(runtime_v2::CheckpointResponse {
                    request_id: request_id.clone(),
                    checkpoint: Some(checkpoint_to_proto_payload(&cached_checkpoint)),
                }));
            }
        }

        let now = current_unix_secs();
        let mut checkpoint = Checkpoint {
            checkpoint_id: generate_checkpoint_id(),
            sandbox_id,
            parent_checkpoint_id: None,
            class,
            state: CheckpointState::Creating,
            created_at: now,
            compatibility_fingerprint: request.compatibility_fingerprint,
        };
        checkpoint
            .transition_to(CheckpointState::Ready)
            .map_err(|error| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::StateConflict,
                    error.to_string(),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        let receipt_id = generate_receipt_id();
        let persist_result = self.daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_checkpoint(&checkpoint)?;
                tx.emit_event(
                    &checkpoint.sandbox_id,
                    &StackEvent::CheckpointReady {
                        checkpoint_id: checkpoint.checkpoint_id.clone(),
                    },
                )?;
                tx.save_receipt(&Receipt {
                    receipt_id: receipt_id.clone(),
                    operation: "create_checkpoint".to_string(),
                    entity_id: checkpoint.checkpoint_id.clone(),
                    entity_type: "checkpoint".to_string(),
                    request_id: request_id.clone(),
                    status: "success".to_string(),
                    created_at: now,
                    metadata: receipt_idempotent_mutation_metadata(
                        "checkpoint_ready",
                        request_hash.as_str(),
                        normalized_idempotency_key,
                    )?,
                })?;
                if let Some(key) = normalized_idempotency_key {
                    tx.save_idempotency_result(&IdempotencyRecord {
                        key: key.to_string(),
                        operation: "create_checkpoint".to_string(),
                        request_hash: request_hash.clone(),
                        response_json: checkpoint.checkpoint_id.clone(),
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
                if let Some(cached_checkpoint) = load_idempotent_checkpoint_replay(
                    &self.daemon,
                    key,
                    "create_checkpoint",
                    &request_hash,
                    &request_id,
                )? {
                    return Ok(Response::new(runtime_v2::CheckpointResponse {
                        request_id,
                        checkpoint: Some(checkpoint_to_proto_payload(&cached_checkpoint)),
                    }));
                }
            }
            return Err(status_from_stack_error(error, &request_id));
        }

        let mut response = Response::new(runtime_v2::CheckpointResponse {
            request_id,
            checkpoint: Some(checkpoint_to_proto_payload(&checkpoint)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn get_checkpoint(
        &self,
        request: Request<runtime_v2::GetCheckpointRequest>,
    ) -> Result<Response<runtime_v2::CheckpointResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let checkpoint = self
            .daemon
            .with_state_store(|store| store.load_checkpoint(&request.checkpoint_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("checkpoint not found: {}", request.checkpoint_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        Ok(Response::new(runtime_v2::CheckpointResponse {
            request_id,
            checkpoint: Some(checkpoint_to_proto_payload(&checkpoint)),
        }))
    }

    async fn list_checkpoints(
        &self,
        request: Request<runtime_v2::ListCheckpointsRequest>,
    ) -> Result<Response<runtime_v2::ListCheckpointsResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let checkpoints = self
            .daemon
            .with_state_store(|store| store.list_checkpoints())
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .iter()
            .map(checkpoint_to_proto_payload)
            .collect();

        Ok(Response::new(runtime_v2::ListCheckpointsResponse {
            request_id,
            checkpoints,
        }))
    }

    async fn restore_checkpoint(
        &self,
        request: Request<runtime_v2::RestoreCheckpointRequest>,
    ) -> Result<Response<runtime_v2::CheckpointResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::RestoreCheckpoint,
            &metadata,
            &request_id,
        )?;

        let checkpoint = self
            .daemon
            .with_state_store(|store| store.load_checkpoint(&request.checkpoint_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("checkpoint not found: {}", request.checkpoint_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if checkpoint.state != CheckpointState::Ready {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("checkpoint {} is not in ready state", request.checkpoint_id),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.emit_event(
                        &checkpoint.sandbox_id,
                        &StackEvent::CheckpointRestored {
                            checkpoint_id: checkpoint.checkpoint_id.clone(),
                            sandbox_id: checkpoint.sandbox_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "restore_checkpoint".to_string(),
                        entity_id: checkpoint.checkpoint_id.clone(),
                        entity_type: "checkpoint".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("checkpoint_restored")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::CheckpointResponse {
            request_id,
            checkpoint: Some(checkpoint_to_proto_payload(&checkpoint)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn fork_checkpoint(
        &self,
        request: Request<runtime_v2::ForkCheckpointRequest>,
    ) -> Result<Response<runtime_v2::CheckpointResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::ForkCheckpoint,
            &metadata,
            &request_id,
        )?;
        let idempotency_key = metadata.idempotency_key.clone();
        let normalized_idempotency_key = normalize_idempotency_key(idempotency_key.as_deref());

        let parent_checkpoint_id = request.checkpoint_id.trim().to_string();
        if parent_checkpoint_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "checkpoint_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let requested_new_sandbox_id = request.new_sandbox_id.trim().to_string();
        let request_hash =
            create_fork_checkpoint_request_hash(&parent_checkpoint_id, &requested_new_sandbox_id);

        if let Some(key) = normalized_idempotency_key {
            if let Some(cached_checkpoint) = load_idempotent_checkpoint_replay(
                &self.daemon,
                key,
                "fork_checkpoint",
                &request_hash,
                &request_id,
            )? {
                return Ok(Response::new(runtime_v2::CheckpointResponse {
                    request_id: request_id.clone(),
                    checkpoint: Some(checkpoint_to_proto_payload(&cached_checkpoint)),
                }));
            }
        }

        let parent = self
            .daemon
            .with_state_store(|store| store.load_checkpoint(&parent_checkpoint_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("checkpoint not found: {parent_checkpoint_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if parent.state != CheckpointState::Ready {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("checkpoint {parent_checkpoint_id} is not in ready state"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let now = current_unix_secs();
        let new_sandbox_id = if requested_new_sandbox_id.is_empty() {
            generate_fork_sandbox_id()
        } else {
            requested_new_sandbox_id
        };
        let mut forked = Checkpoint {
            checkpoint_id: generate_checkpoint_id(),
            sandbox_id: new_sandbox_id.clone(),
            parent_checkpoint_id: Some(parent.checkpoint_id.clone()),
            class: parent.class,
            state: CheckpointState::Creating,
            created_at: now,
            compatibility_fingerprint: parent.compatibility_fingerprint.clone(),
        };
        forked
            .transition_to(CheckpointState::Ready)
            .map_err(|error| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::StateConflict,
                    error.to_string(),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        let receipt_id = generate_receipt_id();
        let persist_result = self.daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_checkpoint(&forked)?;
                tx.emit_event(
                    "default",
                    &StackEvent::CheckpointForked {
                        parent_checkpoint_id: parent.checkpoint_id.clone(),
                        new_checkpoint_id: forked.checkpoint_id.clone(),
                        new_sandbox_id: forked.sandbox_id.clone(),
                    },
                )?;
                tx.save_receipt(&Receipt {
                    receipt_id: receipt_id.clone(),
                    operation: "fork_checkpoint".to_string(),
                    entity_id: forked.checkpoint_id.clone(),
                    entity_type: "checkpoint".to_string(),
                    request_id: request_id.clone(),
                    status: "success".to_string(),
                    created_at: now,
                    metadata: receipt_idempotent_mutation_metadata(
                        "checkpoint_forked",
                        request_hash.as_str(),
                        normalized_idempotency_key,
                    )?,
                })?;
                if let Some(key) = normalized_idempotency_key {
                    tx.save_idempotency_result(&IdempotencyRecord {
                        key: key.to_string(),
                        operation: "fork_checkpoint".to_string(),
                        request_hash: request_hash.clone(),
                        response_json: forked.checkpoint_id.clone(),
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
                if let Some(cached_checkpoint) = load_idempotent_checkpoint_replay(
                    &self.daemon,
                    key,
                    "fork_checkpoint",
                    &request_hash,
                    &request_id,
                )? {
                    return Ok(Response::new(runtime_v2::CheckpointResponse {
                        request_id,
                        checkpoint: Some(checkpoint_to_proto_payload(&cached_checkpoint)),
                    }));
                }
            }
            return Err(status_from_stack_error(error, &request_id));
        }

        let mut response = Response::new(runtime_v2::CheckpointResponse {
            request_id,
            checkpoint: Some(checkpoint_to_proto_payload(&forked)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }
}
