use super::super::*;
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

        let now = current_unix_secs();
        let build = Build {
            build_id: generate_build_id(),
            sandbox_id,
            build_spec: BuildSpec {
                context: request.context,
                dockerfile: normalize_optional_wire_field(&request.dockerfile),
                target: None,
                args: request.args.into_iter().collect(),
                cache_from: Vec::new(),
                image_tag: None,
            },
            state: BuildState::Queued,
            result_digest: None,
            started_at: now,
            ended_at: None,
        };

        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_build(&build)?;
                    tx.emit_event(
                        &build.sandbox_id,
                        &StackEvent::BuildQueued {
                            sandbox_id: build.sandbox_id.clone(),
                            build_id: build.build_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "start_build".to_string(),
                        entity_id: build.build_id.clone(),
                        entity_type: "build".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("build_queued")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::BuildResponse {
            request_id,
            build: Some(build_to_proto_payload(&build)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
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

        let build = self
            .daemon
            .with_state_store(|store| store.load_build(&request.build_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("build not found: {}", request.build_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

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

        let mut build = self
            .daemon
            .with_state_store(|store| store.load_build(&request.build_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("build not found: {}", request.build_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if build.state.is_terminal() {
            return Ok(Response::new(runtime_v2::BuildResponse {
                request_id,
                build: Some(build_to_proto_payload(&build)),
            }));
        }

        let now = current_unix_secs();
        build.ended_at = Some(now);
        build.transition_to(BuildState::Canceled).map_err(|error| {
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
                    tx.save_build(&build)?;
                    tx.emit_event(
                        &build.sandbox_id,
                        &StackEvent::BuildCanceled {
                            build_id: build.build_id.clone(),
                        },
                    )?;
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
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::BuildResponse {
            request_id,
            build: Some(build_to_proto_payload(&build)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn stream_build_events(
        &self,
        _request: Request<runtime_v2::StreamBuildEventsRequest>,
    ) -> Result<Response<Self::StreamBuildEventsStream>, Status> {
        Err(Status::unimplemented(
            "stream_build_events is not implemented yet",
        ))
    }
}
