use super::super::*;

pub(in crate::grpc) struct ImageServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl ImageServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

fn image_to_proto_payload(image: &vz_stack::ImageRecord) -> runtime_v2::ImagePayload {
    runtime_v2::ImagePayload {
        image_ref: image.image_ref.clone(),
        resolved_digest: image.resolved_digest.clone(),
        platform: image.platform.clone(),
        source_registry: image.source_registry.clone(),
        pulled_at: image.pulled_at,
    }
}

#[tonic::async_trait]
impl runtime_v2::image_service_server::ImageService for ImageServiceImpl {
    async fn get_image(
        &self,
        request: Request<runtime_v2::GetImageRequest>,
    ) -> Result<Response<runtime_v2::ImageResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let image_ref = request.image_ref.trim().to_string();
        if image_ref.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "image_ref cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let image = self
            .daemon
            .with_state_store(|store| store.load_image(&image_ref))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("image not found: {image_ref}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        Ok(Response::new(runtime_v2::ImageResponse {
            request_id,
            image: Some(image_to_proto_payload(&image)),
        }))
    }

    async fn list_images(
        &self,
        request: Request<runtime_v2::ListImagesRequest>,
    ) -> Result<Response<runtime_v2::ListImagesResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let images = self
            .daemon
            .with_state_store(|store| store.list_images())
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .iter()
            .map(image_to_proto_payload)
            .collect();

        Ok(Response::new(runtime_v2::ListImagesResponse {
            request_id,
            images,
        }))
    }
}
