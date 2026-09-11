use super::super::super::*;
pub(super) async fn handle(
    daemon: Arc<RuntimeDaemon>,
    request: Request<runtime_v2::UpEnvironmentRequest>,
) -> Result<Response<super::UpEnvironmentStream>, Status> {
    let intercepted = request_id_from_extensions(&request);
    let request = request.into_inner();
    let metadata = normalize_metadata(request.metadata.as_ref(), intercepted);
    #[cfg(not(target_os = "macos"))]
    {
        let _ = daemon;
        Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::UnsupportedOperation,
            "Up physical adapter is currently Linux-on-macOS only".into(),
            metadata.request_id,
            BTreeMap::new(),
        )))
    }
    #[cfg(target_os = "macos")]
    {
        let input =
            vz_runtime_translate::environment_up_request_from_proto(&request).map_err(|error| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::ValidationError,
                    error,
                    metadata.request_id.clone(),
                    BTreeMap::new(),
                ))
            })?;
        // The values are taken off the request and handed across separately:
        // they are not part of the Up's persisted identity, and nothing below
        // this point may put them in durable state.
        let secret_values = request
            .secret_values
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        let mut receiver = daemon
            .up_environment(input, secret_values, metadata)
            .await
            .map_err(status_from_machine_error)?;
        let (sender, stream) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                let event = receiver.borrow_and_update().clone();
                let terminal = event.completion.is_some();
                if sender
                    .send(Ok(vz_runtime_translate::environment_up_progress_to_proto(
                        &event,
                    )))
                    .await
                    .is_err()
                    || terminal
                {
                    break;
                }
                if receiver.changed().await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(stream),
        )))
    }
}
