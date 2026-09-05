use super::super::*;

mod machine_exec;

pub(in crate::grpc) struct TopologyServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl TopologyServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

#[tonic::async_trait]
impl runtime_v2::topology_service_server::TopologyService for TopologyServiceImpl {
    type ExecMachineStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<runtime_v2::MachineExecEvent, Status>> + Send>,
    >;

    async fn exec_machine(
        &self,
        request: Request<tonic::Streaming<runtime_v2::MachineExecFrame>>,
    ) -> Result<Response<Self::ExecMachineStream>, Status> {
        machine_exec::handle(Arc::clone(&self.daemon), request).await
    }
    type StopEnvironmentStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<runtime_v2::StopEnvironmentEvent, Status>>
                + Send,
        >,
    >;

    async fn stop_environment(
        &self,
        request: Request<runtime_v2::StopEnvironmentRequest>,
    ) -> Result<Response<Self::StopEnvironmentStream>, Status> {
        let intercepted = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted);
        #[cfg(not(target_os = "macos"))]
        {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::UnsupportedOperation,
                "Environment Stop physical adapter is currently Linux-on-macOS only".into(),
                metadata.request_id,
                BTreeMap::new(),
            )));
        }
        #[cfg(target_os = "macos")]
        {
            use tokio_stream::StreamExt;
            use vz_runtime_contract::{
                EnvironmentId, EnvironmentSelectionContext, EnvironmentSelector, ProjectId,
            };
            let invalid = |message: String| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::ValidationError,
                    message,
                    metadata.request_id.clone(),
                    BTreeMap::new(),
                ))
            };
            let project_id =
                ProjectId::new(request.project_id).map_err(|error| invalid(error.to_string()))?;
            // A valid higher-tier explicit selector excludes lower-tier process
            // parsing entirely; malformed lower tiers cannot override it.
            let process_environment_id = if request.environment.is_some() {
                None
            } else {
                request
                    .process_environment_id
                    .map(EnvironmentId::new)
                    .transpose()
                    .map_err(|error| invalid(error.to_string()))?
            };
            let input = crate::environment_stop::StopEnvironmentInput {
                project_id,
                selection: EnvironmentSelectionContext {
                    explicit: request.environment.map(EnvironmentSelector::NameOrId),
                    process_environment_id,
                    workspace_key: request.workspace_key,
                },
                metadata,
                machine_timeout: Duration::from_millis(request.machine_timeout_millis),
            };
            let receiver = self
                .daemon
                .stop_environment(input)
                .await
                .map_err(status_from_machine_error)?;
            #[allow(clippy::result_large_err)] // tonic streams require Status by value.
            let stream = tokio_stream::wrappers::ReceiverStream::new(receiver).map(|result| {
                result
                    .map(|event| runtime_v2::StopEnvironmentEvent {
                        schema_version: event.schema_version,
                        request_id: event.request_id,
                        sequence: event.sequence,
                        operation: Some(
                            vz_runtime_translate::environment_lifecycle_operation_to_proto(
                                &event.operation,
                            ),
                        ),
                        terminal: event.terminal,
                        error: event
                            .error
                            .as_ref()
                            .map(vz_runtime_translate::machine_error_to_proto_detail),
                    })
                    .map_err(status_from_machine_error)
            });
            Ok(Response::new(
                Box::pin(stream) as Self::StopEnvironmentStream
            ))
        }
    }

    async fn get_project_state(
        &self,
        request: Request<runtime_v2::GetProjectStateRequest>,
    ) -> Result<Response<runtime_v2::GetProjectStateResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let project_id =
            vz_runtime_contract::ProjectId::new(request.project_id).map_err(|error| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::ValidationError,
                    error.to_string(),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        let project = self
            .daemon
            .with_state_store(|store| store.load_project_state_snapshot(project_id.as_str()))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("project not found: {project_id}"),
                    Some(request_id.clone()),
                    BTreeMap::from([("project_id".to_string(), project_id.to_string())]),
                ))
            })?;

        Ok(Response::new(runtime_v2::GetProjectStateResponse {
            request_id,
            project: Some(vz_runtime_translate::project_state_to_proto(&project)),
        }))
    }
}
