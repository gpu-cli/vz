use super::super::*;

const BUILD_EVENT_POLL_INTERVAL: Duration = Duration::from_millis(250);

pub(in crate::grpc) struct BuildServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl BuildServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

#[tonic::async_trait]
impl runtime_v2::build_service_server::BuildService for BuildServiceImpl {
    type StreamBuildEventsStream =
        tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::BuildEvent, Status>>;

    async fn start_build(
        &self,
        request: Request<runtime_v2::StartBuildRequest>,
    ) -> Result<Response<runtime_v2::BuildResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::StartBuild,
            &metadata,
            &request_id,
        )?;

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "sandbox_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let context = request.context.trim().to_string();
        if context.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "context cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let build_spec = BuildSpec {
            context,
            dockerfile: normalize_optional_wire_field(&request.dockerfile),
            target: normalize_optional_wire_field(&request.target),
            args: request.args.into_iter().collect(),
            cache_from: Vec::new(),
            image_tag: normalize_optional_wire_field(&request.image_tag),
            secrets: request
                .secrets
                .into_iter()
                .map(|entry| entry.trim().to_string())
                .filter(|entry| !entry.is_empty())
                .collect(),
            no_cache: request.no_cache,
            push: request.push,
            output_oci_tar_dest: normalize_optional_wire_field(&request.output_oci_tar_dest),
        };
        if build_spec.push && build_spec.output_oci_tar_dest.is_some() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "push and output_oci_tar_dest cannot be set together".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let build = self
            .daemon
            .manager()
            .start_build(&sandbox_id, build_spec, metadata.idempotency_key.clone())
            .await
            .map_err(|error| status_from_runtime_error("start_build", error, &request_id))?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        let persisted_receipt = self
            .daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    let already_persisted = tx.load_build(&build.build_id)?.is_some();
                    tx.save_build(&build)?;
                    if !already_persisted {
                        if let Some(event) = stack_event_for_build_state(&build) {
                            tx.emit_event(&build.sandbox_id, &event)?;
                        }
                        tx.save_receipt(&Receipt {
                            receipt_id: receipt_id.clone(),
                            operation: "start_build".to_string(),
                            entity_id: build.build_id.clone(),
                            entity_type: "build".to_string(),
                            request_id: request_id.clone(),
                            status: "success".to_string(),
                            created_at: now,
                            metadata: receipt_event_metadata(build_state_event_type(build.state))?,
                        })?;
                    }
                    Ok(!already_persisted)
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::BuildResponse {
            request_id,
            build: Some(build_to_proto_payload(&build)),
        });
        if persisted_receipt && let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn get_build(
        &self,
        request: Request<runtime_v2::GetBuildRequest>,
    ) -> Result<Response<runtime_v2::BuildResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let build_id = request.build_id.trim().to_string();
        if build_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "build_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let build = match self.daemon.manager().get_build(&build_id).await {
            Ok(build) => {
                persist_build_snapshot(self.daemon.as_ref(), &build, &request_id)?;
                build
            }
            Err(error) if error.machine_code() == MachineErrorCode::NotFound => self
                .daemon
                .with_state_store(|store| store.load_build(&build_id))
                .map_err(|store_error| status_from_stack_error(store_error, &request_id))?
                .ok_or_else(|| build_not_found_status(&build_id, &request_id))?,
            Err(error) => return Err(status_from_runtime_error("get_build", error, &request_id)),
        };

        Ok(Response::new(runtime_v2::BuildResponse {
            request_id,
            build: Some(build_to_proto_payload(&build)),
        }))
    }

    async fn list_builds(
        &self,
        request: Request<runtime_v2::ListBuildsRequest>,
    ) -> Result<Response<runtime_v2::ListBuildsResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let builds = self
            .daemon
            .with_state_store(|store| store.list_builds())
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .iter()
            .map(build_to_proto_payload)
            .collect();

        Ok(Response::new(runtime_v2::ListBuildsResponse {
            request_id,
            builds,
        }))
    }

    async fn cancel_build(
        &self,
        request: Request<runtime_v2::CancelBuildRequest>,
    ) -> Result<Response<runtime_v2::BuildResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CancelBuild,
            &metadata,
            &request_id,
        )?;

        let build_id = request.build_id.trim().to_string();
        if build_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "build_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let build = self
            .daemon
            .manager()
            .cancel_build(&build_id)
            .await
            .map_err(|error| status_from_runtime_error("cancel_build", error, &request_id))?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        let persisted_receipt = self
            .daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    let previous_state = tx
                        .load_build(&build.build_id)?
                        .map(|existing| existing.state);
                    tx.save_build(&build)?;
                    let state_changed = previous_state != Some(build.state);
                    if state_changed && let Some(event) = stack_event_for_build_state(&build) {
                        tx.emit_event(&build.sandbox_id, &event)?;
                    }
                    if state_changed && build.state == BuildState::Canceled {
                        tx.save_receipt(&Receipt {
                            receipt_id: receipt_id.clone(),
                            operation: "cancel_build".to_string(),
                            entity_id: build.build_id.clone(),
                            entity_type: "build".to_string(),
                            request_id: request_id.clone(),
                            status: "success".to_string(),
                            created_at: now,
                            metadata: receipt_event_metadata("build_canceled")?,
                        })?;
                    }
                    Ok(state_changed && build.state == BuildState::Canceled)
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::BuildResponse {
            request_id,
            build: Some(build_to_proto_payload(&build)),
        });
        if persisted_receipt && let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn stream_build_events(
        &self,
        request: Request<runtime_v2::StreamBuildEventsRequest>,
    ) -> Result<Response<Self::StreamBuildEventsStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let build_id = request.build_id.trim().to_string();
        if build_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "build_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let initial_build = self
            .daemon
            .manager()
            .get_build(&build_id)
            .await
            .map_err(|error| status_from_runtime_error("get_build", error, &request_id))?;
        persist_build_snapshot(self.daemon.as_ref(), &initial_build, &request_id)?;

        let daemon = self.daemon.clone();
        let request_id_for_stream = request_id.clone();
        let build_id_for_stream = build_id.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            let mut cursor: Option<u64> = None;
            loop {
                let events = match daemon
                    .manager()
                    .stream_build_events(&build_id_for_stream, cursor)
                    .await
                {
                    Ok(events) => events,
                    Err(error) => {
                        let _ = tx
                            .send(Err(status_from_runtime_error(
                                "stream_build_events",
                                error,
                                &request_id_for_stream,
                            )))
                            .await;
                        return;
                    }
                };

                for event in events {
                    cursor = Some(event.event_id);
                    let wire_event = build_contract_event_to_proto(&event);
                    if tx.send(Ok(wire_event)).await.is_err() {
                        return;
                    }
                }

                let build = match daemon.manager().get_build(&build_id_for_stream).await {
                    Ok(build) => build,
                    Err(error) => {
                        let _ = tx
                            .send(Err(status_from_runtime_error(
                                "get_build",
                                error,
                                &request_id_for_stream,
                            )))
                            .await;
                        return;
                    }
                };

                if let Err(status) =
                    persist_build_snapshot(daemon.as_ref(), &build, &request_id_for_stream)
                {
                    let _ = tx.send(Err(status)).await;
                    return;
                }

                if build.state.is_terminal() {
                    return;
                }
                tokio::time::sleep(BUILD_EVENT_POLL_INTERVAL).await;
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

fn persist_build_snapshot(
    daemon: &RuntimeDaemon,
    build: &Build,
    request_id: &str,
) -> Result<(), Status> {
    daemon
        .with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                let previous_state = tx
                    .load_build(&build.build_id)?
                    .map(|existing| existing.state);
                tx.save_build(build)?;
                if previous_state != Some(build.state)
                    && let Some(event) = stack_event_for_build_state(build)
                {
                    tx.emit_event(&build.sandbox_id, &event)?;
                }
                Ok(())
            })
        })
        .map_err(|error| status_from_stack_error(error, request_id))
}

fn stack_event_for_build_state(build: &Build) -> Option<StackEvent> {
    match build.state {
        BuildState::Queued => Some(StackEvent::BuildQueued {
            sandbox_id: build.sandbox_id.clone(),
            build_id: build.build_id.clone(),
        }),
        BuildState::Running => Some(StackEvent::BuildRunning {
            build_id: build.build_id.clone(),
        }),
        BuildState::Succeeded => {
            let result_digest = build.result_digest.clone()?;
            Some(StackEvent::BuildSucceeded {
                build_id: build.build_id.clone(),
                result_digest,
            })
        }
        BuildState::Failed => Some(StackEvent::BuildFailed {
            build_id: build.build_id.clone(),
            error: "build failed".to_string(),
        }),
        BuildState::Canceled => Some(StackEvent::BuildCanceled {
            build_id: build.build_id.clone(),
        }),
    }
}

fn build_state_event_type(state: BuildState) -> &'static str {
    match state {
        BuildState::Queued => "build_queued",
        BuildState::Running => "build_running",
        BuildState::Succeeded => "build_succeeded",
        BuildState::Failed => "build_failed",
        BuildState::Canceled => "build_canceled",
    }
}

fn build_not_found_status(build_id: &str, request_id: &str) -> Status {
    status_from_machine_error(MachineError::new(
        MachineErrorCode::NotFound,
        format!("build not found: {build_id}"),
        Some(request_id.to_string()),
        BTreeMap::new(),
    ))
}

fn status_from_runtime_error(
    operation: &str,
    error: vz_runtime_contract::RuntimeError,
    request_id: &str,
) -> Status {
    status_from_machine_error(MachineError::new(
        error.machine_code(),
        format!("{operation} failed: {error}"),
        Some(request_id.to_string()),
        BTreeMap::new(),
    ))
}

fn build_contract_event_to_proto(event: &vz_runtime_contract::Event) -> runtime_v2::BuildEvent {
    runtime_v2::BuildEvent {
        event_type: event.event_type.clone(),
        message: build_event_message(event),
        timestamp: event.ts,
    }
}

fn build_event_message(event: &vz_runtime_contract::Event) -> String {
    if let Some(message) = event.payload.get("message") {
        return message.clone();
    }
    if let Some(reason) = event.payload.get("reason") {
        return reason.clone();
    }
    if let Some(chunk) = event.payload.get("chunk") {
        let stream = event
            .payload
            .get("stream")
            .map(String::as_str)
            .unwrap_or("stream");
        return format!("[{stream}] {chunk}");
    }
    if let Some(status) = event.payload.get("status") {
        return status.clone();
    }

    match serde_json::to_string(&event.payload) {
        Ok(payload) => payload,
        Err(_) => String::new(),
    }
}
