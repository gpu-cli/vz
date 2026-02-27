use super::super::*;

#[derive(Clone)]
pub(in crate::grpc) struct ContainerServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl ContainerServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

#[tonic::async_trait]
impl runtime_v2::container_service_server::ContainerService for ContainerServiceImpl {
    async fn create_container(
        &self,
        request: Request<runtime_v2::CreateContainerRequest>,
    ) -> Result<Response<runtime_v2::ContainerResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let mut request = request.into_inner();
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

        let image_digest = if request.image_digest.trim().is_empty() {
            sandbox
                .spec
                .base_image_ref
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    status_from_machine_error(MachineError::new(
                        MachineErrorCode::ValidationError,
                        "image_digest is required when sandbox base_image_ref is unset".to_string(),
                        Some(request_id.clone()),
                        BTreeMap::new(),
                    ))
                })?
        } else {
            request.image_digest.trim().to_string()
        };

        let mut resolved_cmd = request.cmd;
        if resolved_cmd.is_empty()
            && let Some(main_container) = sandbox
                .spec
                .main_container
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
        {
            resolved_cmd.push(main_container.to_string());
        }
        request.cmd = resolved_cmd.clone();

        let request_hash = create_container_request_hash(&request, &sandbox_id, &image_digest);
        if let Some(key) = normalized_idempotency_key {
            if let Some(cached_container) = load_idempotent_container_replay(
                &self.daemon,
                key,
                "create_container",
                &request_hash,
                &request_id,
            )? {
                return Ok(Response::new(runtime_v2::ContainerResponse {
                    request_id: request_id.clone(),
                    container: Some(container_to_proto_payload(&cached_container)),
                }));
            }
        }

        self.daemon
            .enforce_create_container_placement(&request_id)
            .map_err(status_from_machine_error)?;

        let now = current_unix_secs();
        let container = Container {
            container_id: generate_container_id(),
            sandbox_id,
            image_digest,
            container_spec: ContainerSpec {
                cmd: resolved_cmd,
                env: request.env.into_iter().collect(),
                cwd: normalize_optional_wire_field(&request.cwd),
                user: normalize_optional_wire_field(&request.user),
                mounts: Vec::new(),
                resources: Default::default(),
                network_attachments: Vec::new(),
            },
            state: ContainerState::Created,
            created_at: now,
            started_at: None,
            ended_at: None,
        };

        let receipt_id = generate_receipt_id();
        let persist_result = self.daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_container(&container)?;
                tx.emit_event(
                    &container.sandbox_id,
                    &StackEvent::ContainerCreated {
                        sandbox_id: container.sandbox_id.clone(),
                        container_id: container.container_id.clone(),
                    },
                )?;
                tx.save_receipt(&Receipt {
                    receipt_id: receipt_id.clone(),
                    operation: "create_container".to_string(),
                    entity_id: container.container_id.clone(),
                    entity_type: "container".to_string(),
                    request_id: request_id.clone(),
                    status: "success".to_string(),
                    created_at: now,
                    metadata: receipt_idempotent_mutation_metadata(
                        "container_created",
                        request_hash.as_str(),
                        normalized_idempotency_key,
                    )?,
                })?;
                if let Some(key) = normalized_idempotency_key {
                    tx.save_idempotency_result(&IdempotencyRecord {
                        key: key.to_string(),
                        operation: "create_container".to_string(),
                        request_hash: request_hash.clone(),
                        response_json: container.container_id.clone(),
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
                if let Some(cached_container) = load_idempotent_container_replay(
                    &self.daemon,
                    key,
                    "create_container",
                    &request_hash,
                    &request_id,
                )? {
                    return Ok(Response::new(runtime_v2::ContainerResponse {
                        request_id,
                        container: Some(container_to_proto_payload(&cached_container)),
                    }));
                }
            }
            return Err(status_from_stack_error(error, &request_id));
        }

        let mut response = Response::new(runtime_v2::ContainerResponse {
            request_id,
            container: Some(container_to_proto_payload(&container)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn get_container(
        &self,
        request: Request<runtime_v2::GetContainerRequest>,
    ) -> Result<Response<runtime_v2::ContainerResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let container = self
            .daemon
            .with_state_store(|store| store.load_container(&request.container_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("container not found: {}", request.container_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        Ok(Response::new(runtime_v2::ContainerResponse {
            request_id,
            container: Some(container_to_proto_payload(&container)),
        }))
    }

    async fn list_containers(
        &self,
        request: Request<runtime_v2::ListContainersRequest>,
    ) -> Result<Response<runtime_v2::ListContainersResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let containers = self
            .daemon
            .with_state_store(|store| store.list_containers())
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .iter()
            .map(container_to_proto_payload)
            .collect();

        Ok(Response::new(runtime_v2::ListContainersResponse {
            request_id,
            containers,
        }))
    }

    async fn remove_container(
        &self,
        request: Request<runtime_v2::RemoveContainerRequest>,
    ) -> Result<Response<runtime_v2::ContainerResponse>, Status> {
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

        let container = self
            .daemon
            .with_state_store(|store| store.load_container(&request.container_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("container not found: {}", request.container_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        let now = current_unix_secs();
        let mut removed_container = container.clone();
        removed_container.state = ContainerState::Removed;
        if removed_container.ended_at.is_none() {
            removed_container.ended_at = Some(now);
        }

        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.delete_container(&request.container_id)?;
                    tx.emit_event(
                        &container.sandbox_id,
                        &StackEvent::ContainerRemoved {
                            container_id: request.container_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "remove_container".to_string(),
                        entity_id: request.container_id.clone(),
                        entity_type: "container".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("container_removed")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::ContainerResponse {
            request_id,
            container: Some(container_to_proto_payload(&removed_container)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }
}
