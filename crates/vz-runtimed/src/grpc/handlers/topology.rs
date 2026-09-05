use super::super::*;

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
