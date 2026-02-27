use super::super::*;

pub(in crate::grpc) struct ImageServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl ImageServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

type PullImageStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::PullImageEvent, Status>>;
type PruneImagesStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::PruneImagesEvent, Status>>;

fn image_to_proto_payload(image: &vz_stack::ImageRecord) -> runtime_v2::ImagePayload {
    runtime_v2::ImagePayload {
        image_ref: image.image_ref.clone(),
        resolved_digest: image.resolved_digest.clone(),
        platform: image.platform.clone(),
        source_registry: image.source_registry.clone(),
        pulled_at: image.pulled_at,
    }
}

fn source_registry_for_image_ref(image_ref: &str) -> String {
    let first = image_ref
        .split('/')
        .next()
        .unwrap_or_default()
        .trim()
        .to_string();
    if first == "localhost" || first.contains('.') || first.contains(':') {
        first
    } else {
        "docker.io".to_string()
    }
}

fn runtime_image_record(
    image_ref: &str,
    resolved_digest: &str,
    pulled_at: u64,
) -> vz_stack::ImageRecord {
    vz_stack::ImageRecord {
        image_ref: image_ref.to_string(),
        resolved_digest: resolved_digest.to_string(),
        platform: "unknown".to_string(),
        source_registry: source_registry_for_image_ref(image_ref),
        pulled_at,
    }
}

fn image_runtime_status(
    error: vz_runtime_contract::RuntimeError,
    operation: &str,
    request_id: &str,
) -> Status {
    status_from_machine_error(MachineError::new(
        error.machine_code(),
        format!("failed to {operation}: {error}"),
        Some(request_id.to_string()),
        BTreeMap::new(),
    ))
}

fn stream_from_events<T>(
    events: Vec<Result<T, Status>>,
) -> tokio_stream::wrappers::ReceiverStream<Result<T, Status>>
where
    T: Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(events.len().max(1));
    for event in events {
        if tx.try_send(event).is_err() {
            break;
        }
    }
    drop(tx);
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

fn pull_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::PullImageEvent {
    runtime_v2::PullImageEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::pull_image_event::Payload::Progress(
            runtime_v2::ImageProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn pull_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::PullImageResponse,
) -> runtime_v2::PullImageEvent {
    runtime_v2::PullImageEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::pull_image_event::Payload::Completion(response)),
    }
}

fn prune_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::PruneImagesEvent {
    runtime_v2::PruneImagesEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::prune_images_event::Payload::Progress(
            runtime_v2::ImageProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn prune_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::PruneImagesResponse,
) -> runtime_v2::PruneImagesEvent {
    runtime_v2::PruneImagesEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::prune_images_event::Payload::Completion(
            response,
        )),
    }
}

#[tonic::async_trait]
impl runtime_v2::image_service_server::ImageService for ImageServiceImpl {
    type PullImageStream = PullImageStream;
    type PruneImagesStream = PruneImagesStream;

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

    async fn pull_image(
        &self,
        request: Request<runtime_v2::PullImageRequest>,
    ) -> Result<Response<Self::PullImageStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::PullImage,
            &metadata,
            &request_id,
        )?;

        let image_ref = request.image_ref.trim().to_string();
        if image_ref.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "image_ref cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let mut sequence = 1u64;
        let mut events = vec![Ok(pull_progress_event(
            &request_id,
            sequence,
            "pull_started",
            &format!("pulling {image_ref}"),
        ))];

        let resolved_digest = match self.daemon.manager().pull_image(&image_ref).await {
            Ok(value) => value,
            Err(error) => {
                events.push(Err(image_runtime_status(error, "pull image", &request_id)));
                return Ok(Response::new(stream_from_events(events)));
            }
        };

        sequence += 1;
        events.push(Ok(pull_progress_event(
            &request_id,
            sequence,
            "persisting",
            "persisting pulled image metadata",
        )));

        let pulled_at = current_unix_secs();
        let image = runtime_image_record(&image_ref, &resolved_digest, pulled_at);
        let receipt_id = generate_receipt_id();
        match self.daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_image(&image)?;
                tx.save_receipt(&Receipt {
                    receipt_id: receipt_id.clone(),
                    operation: "pull_image".to_string(),
                    entity_id: image_ref.clone(),
                    entity_type: "image".to_string(),
                    request_id: request_id.clone(),
                    status: "success".to_string(),
                    created_at: pulled_at,
                    metadata: receipt_event_metadata("image_pulled")?,
                })?;
                Ok(())
            })
        }) {
            Ok(()) => {
                sequence += 1;
                events.push(Ok(pull_completion_event(
                    &request_id,
                    sequence,
                    runtime_v2::PullImageResponse {
                        request_id: request_id.clone(),
                        image: Some(image_to_proto_payload(&image)),
                        receipt_id,
                    },
                )));
            }
            Err(error) => events.push(Err(status_from_stack_error(error, &request_id))),
        }

        Ok(Response::new(stream_from_events(events)))
    }

    async fn prune_images(
        &self,
        request: Request<runtime_v2::PruneImagesRequest>,
    ) -> Result<Response<Self::PruneImagesStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let mut sequence = 1u64;
        let mut events = vec![Ok(prune_progress_event(
            &request_id,
            sequence,
            "prune_started",
            "pruning unreferenced image artifacts",
        ))];

        let prune_result = match self.daemon.manager().prune_images() {
            Ok(value) => value,
            Err(error) => {
                events.push(Err(image_runtime_status(
                    error,
                    "prune images",
                    &request_id,
                )));
                return Ok(Response::new(stream_from_events(events)));
            }
        };

        sequence += 1;
        events.push(Ok(prune_progress_event(
            &request_id,
            sequence,
            "refreshing_index",
            "refreshing daemon image index",
        )));

        let remaining_runtime_images = match self.daemon.manager().list_images() {
            Ok(images) => images,
            Err(error) => {
                events.push(Err(image_runtime_status(
                    error,
                    "list images after prune",
                    &request_id,
                )));
                return Ok(Response::new(stream_from_events(events)));
            }
        };
        let now = current_unix_secs();
        let image_snapshot: Vec<vz_stack::ImageRecord> = remaining_runtime_images
            .iter()
            .map(|image| runtime_image_record(&image.reference, &image.image_id, now))
            .collect();
        let receipt_id = generate_receipt_id();
        match self.daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.replace_images(&image_snapshot)?;
                tx.save_receipt(&Receipt {
                    receipt_id: receipt_id.clone(),
                    operation: "prune_images".to_string(),
                    entity_id: "image_store".to_string(),
                    entity_type: "image_store".to_string(),
                    request_id: request_id.clone(),
                    status: "success".to_string(),
                    created_at: now,
                    metadata: receipt_event_metadata("images_pruned")?,
                })?;
                Ok(())
            })
        }) {
            Ok(()) => {
                sequence += 1;
                events.push(Ok(prune_completion_event(
                    &request_id,
                    sequence,
                    runtime_v2::PruneImagesResponse {
                        request_id: request_id.clone(),
                        removed_refs: prune_result.removed_refs as u64,
                        removed_manifests: prune_result.removed_manifests as u64,
                        removed_configs: prune_result.removed_configs as u64,
                        removed_layer_dirs: prune_result.removed_layer_dirs as u64,
                        remaining_images: image_snapshot.len() as u64,
                        receipt_id,
                    },
                )));
            }
            Err(error) => events.push(Err(status_from_stack_error(error, &request_id))),
        }

        Ok(Response::new(stream_from_events(events)))
    }
}
