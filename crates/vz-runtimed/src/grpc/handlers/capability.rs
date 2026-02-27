use super::super::*;
pub(in crate::grpc) struct CapabilityServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl CapabilityServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

#[tonic::async_trait]
impl runtime_v2::capability_service_server::CapabilityService for CapabilityServiceImpl {
    async fn get_capabilities(
        &self,
        request: Request<runtime_v2::GetCapabilitiesRequest>,
    ) -> Result<Response<runtime_v2::GetCapabilitiesResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let health = self.daemon.health();
        let capabilities = runtime_capabilities_to_proto(health.capabilities);

        let mut response = Response::new(runtime_v2::GetCapabilitiesResponse {
            request_id,
            capabilities,
        });
        insert_health_headers(response.metadata_mut(), &health)?;
        Ok(response)
    }
}
