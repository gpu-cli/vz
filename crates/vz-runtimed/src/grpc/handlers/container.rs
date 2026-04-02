use super::super::*;
use std::path::PathBuf;
use std::time::Duration;
use vz_runtime_contract::{MountAccess, MountSpec, MountType, RunConfig};

#[derive(Clone)]
pub(in crate::grpc) struct ContainerServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl ContainerServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

fn runtime_container_not_active(
    error: &vz_runtime_contract::RuntimeError,
    container_id: &str,
) -> bool {
    if error.machine_code() == MachineErrorCode::NotFound {
        return true;
    }

    let message = error.to_string().to_ascii_lowercase();
    let container_id_lc = container_id.to_ascii_lowercase();
    message.contains("container not found")
        || message.contains("no active vm handle")
        || message.contains("not running")
        || message.contains("not found") && message.contains(&container_id_lc)
}

fn container_runtime_status(
    error: vz_runtime_contract::RuntimeError,
    operation: &str,
    container_id: &str,
    request_id: &str,
) -> Status {
    status_from_machine_error(MachineError::new(
        error.machine_code(),
        format!("failed to {operation} runtime container {container_id}: {error}"),
        Some(request_id.to_string()),
        BTreeMap::new(),
    ))
}

async fn create_runtime_container(
    daemon: Arc<RuntimeDaemon>,
    sandbox_id: String,
    image_digest: String,
    run_config: RunConfig,
    container_id: &str,
    request_id: &str,
) -> Result<String, Status> {
    let container_id_owned = container_id.to_string();
    let bridge_result = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("failed to initialize runtime bridge: {error}"))?;
        Ok::<_, String>(
            runtime.block_on(daemon.manager().create_container_in_sandbox(
                &sandbox_id,
                &image_digest,
                run_config,
            )),
        )
    })
    .await
    .map_err(|join_error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "runtime bridge join failure while creating container {container_id}: {join_error}"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    let runtime_result = bridge_result.map_err(|bridge_error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "runtime bridge initialization failed while creating container {container_id}: {bridge_error}"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    match runtime_result {
        Ok(runtime_container_id) => Ok(runtime_container_id),
        Err(error) => Err(container_runtime_status(
            error,
            "create",
            &container_id_owned,
            request_id,
        )),
    }
}

async fn stop_runtime_container(
    daemon: Arc<RuntimeDaemon>,
    container_id: &str,
    request_id: &str,
) -> Result<(), Status> {
    let container_id_owned = container_id.to_string();
    let bridge_result = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("failed to initialize runtime bridge: {error}"))?;
        Ok::<_, String>(runtime.block_on(daemon.manager().stop_container(
            &container_id_owned,
            false,
            Some("SIGTERM"),
            Some(Duration::from_secs(5)),
        )))
    })
    .await
    .map_err(|join_error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "runtime bridge join failure while stopping container {container_id}: {join_error}"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    let runtime_result = bridge_result.map_err(|bridge_error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "runtime bridge initialization failed while stopping container {container_id}: {bridge_error}"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    match runtime_result {
        Ok(_) => Ok(()),
        Err(error) if runtime_container_not_active(&error, container_id) => Ok(()),
        Err(error) => Err(container_runtime_status(
            error,
            "stop",
            container_id,
            request_id,
        )),
    }
}

async fn remove_runtime_container(
    daemon: Arc<RuntimeDaemon>,
    container_id: &str,
    request_id: &str,
) -> Result<(), Status> {
    let container_id_owned = container_id.to_string();
    let bridge_result = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("failed to initialize runtime bridge: {error}"))?;
        Ok::<_, String>(runtime.block_on(daemon.manager().remove_container(&container_id_owned)))
    })
    .await
    .map_err(|join_error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "runtime bridge join failure while removing container {container_id}: {join_error}"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    let runtime_result = bridge_result.map_err(|bridge_error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "runtime bridge initialization failed while removing container {container_id}: {bridge_error}"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    match runtime_result {
        Ok(()) => Ok(()),
        Err(error) if runtime_container_not_active(&error, container_id) => Ok(()),
        Err(error) => Err(container_runtime_status(
            error,
            "remove",
            container_id,
            request_id,
        )),
    }
}

fn sandbox_workspace_dir_from_labels(
    sandbox: &Sandbox,
    request_id: &str,
) -> Result<Option<PathBuf>, Status> {
    let Some(project_dir) = sandbox
        .labels
        .get("project_dir")
        .and_then(|value| normalize_optional_wire_field(value))
    else {
        return Ok(None);
    };

    let path = PathBuf::from(project_dir.trim());
    if !path.is_absolute() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "sandbox label `project_dir` must be an absolute path: {}",
                path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    if !path.exists() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "sandbox label `project_dir` does not exist: {}",
                path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    if !path.is_dir() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "sandbox label `project_dir` must reference a directory: {}",
                path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    Ok(Some(path))
}

fn default_keepalive_container_cmd() -> Vec<String> {
    vec![
        "/bin/sh".to_string(),
        "-lc".to_string(),
        "while :; do sleep 3600; done".to_string(),
    ]
}

fn build_runtime_run_config(
    sandbox: &Sandbox,
    container: &Container,
    request_id: &str,
) -> Result<RunConfig, Status> {
    let mut run_config = RunConfig {
        cmd: container.container_spec.cmd.clone(),
        working_dir: container.container_spec.cwd.clone(),
        env: container
            .container_spec
            .env
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        user: container.container_spec.user.clone(),
        container_id: Some(container.container_id.clone()),
        capture_logs: true,
        ..RunConfig::default()
    };

    if run_config.cmd.is_empty() {
        run_config.cmd = default_keepalive_container_cmd();
    }

    if let Some(project_dir) = sandbox_workspace_dir_from_labels(sandbox, request_id)? {
        run_config.mounts.push(MountSpec {
            source: Some(project_dir),
            target: PathBuf::from("/workspace"),
            mount_type: MountType::Bind,
            access: MountAccess::ReadWrite,
            subpath: None,
        });
        if run_config.working_dir.is_none() {
            run_config.working_dir = Some("/workspace".to_string());
        }
    }

    // `vz run` stores VirtioFS mount targets as labels (vz.run.mount.vz-mount-N=/path).
    // Add them as bind mounts so the container sees the VirtioFS shares.
    for (key, guest_path) in &sandbox.labels {
        if let Some(tag) = key.strip_prefix("vz.run.mount.") {
            run_config.mounts.push(MountSpec {
                source: Some(PathBuf::from(format!("/mnt/{tag}"))),
                target: PathBuf::from(guest_path),
                mount_type: MountType::Bind,
                access: MountAccess::ReadWrite,
                subpath: None,
            });
        }
    }

    // Use vz.run.workspace label as default working directory.
    if run_config.working_dir.is_none() {
        if let Some(workspace) = sandbox.labels.get("vz.run.workspace") {
            if !workspace.trim().is_empty() {
                run_config.working_dir = Some(workspace.clone());
            }
        }
    }

    Ok(run_config)
}

async fn cleanup_runtime_container_after_persist_failure(
    daemon: Arc<RuntimeDaemon>,
    container_id: &str,
    request_id: &str,
) {
    if let Err(error) = stop_runtime_container(daemon.clone(), container_id, request_id).await {
        warn!(
            container_id = %container_id,
            request_id = %request_id,
            error = %error,
            "failed to stop runtime container during persistence-failure cleanup"
        );
    }

    if let Err(error) = remove_runtime_container(daemon, container_id, request_id).await {
        warn!(
            container_id = %container_id,
            request_id = %request_id,
            error = %error,
            "failed to remove runtime container during persistence-failure cleanup"
        );
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
        let mut container = Container {
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

        let runtime_run_config = build_runtime_run_config(&sandbox, &container, &request_id)?;
        let runtime_container_id = create_runtime_container(
            self.daemon.clone(),
            container.sandbox_id.clone(),
            container.image_digest.clone(),
            runtime_run_config,
            &container.container_id,
            &request_id,
        )
        .await?;
        if runtime_container_id != container.container_id {
            container.container_id = runtime_container_id;
        }

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
            cleanup_runtime_container_after_persist_failure(
                self.daemon.clone(),
                &container.container_id,
                &request_id,
            )
            .await;
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

        stop_runtime_container(self.daemon.clone(), &container.container_id, &request_id).await?;
        remove_runtime_container(self.daemon.clone(), &container.container_id, &request_id).await?;

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
