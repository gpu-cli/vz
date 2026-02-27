use super::super::*;

#[derive(Clone)]
pub(in crate::grpc) struct EventServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl EventServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

#[tonic::async_trait]
impl runtime_v2::event_service_server::EventService for EventServiceImpl {
    type StreamEventsStream =
        tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::RuntimeEvent, Status>>;

    async fn list_events(
        &self,
        request: Request<runtime_v2::ListEventsRequest>,
    ) -> Result<Response<runtime_v2::ListEventsResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let stack_name = request.stack_name.trim().to_string();
        if stack_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let limit = if request.limit == 0 {
            100
        } else {
            request.limit as usize
        }
        .clamp(1, 1000);
        let scope = normalize_optional_wire_field(&request.scope);
        let cursor = if request.after <= 0 {
            None
        } else {
            Some(request.after)
        };

        let records = self
            .daemon
            .with_state_store(|store| {
                if let Some(scope_prefix) = scope.as_deref() {
                    store.load_events_by_scope(&stack_name, scope_prefix, cursor, limit)
                } else {
                    store.load_events_since_limited(&stack_name, request.after.max(0), limit)
                }
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let events: Vec<runtime_v2::RuntimeEvent> = records
            .iter()
            .map(event_record_to_runtime_event)
            .collect::<Result<_, _>>()?;
        let next_cursor = records
            .last()
            .map(|record| record.id)
            .unwrap_or_else(|| request.after.max(0));

        Ok(Response::new(runtime_v2::ListEventsResponse {
            request_id,
            events,
            next_cursor,
        }))
    }

    async fn stream_events(
        &self,
        request: Request<runtime_v2::StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let stack_name = request.stack_name.trim().to_string();
        if stack_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let scope = normalize_optional_wire_field(&request.scope);
        let daemon = self.daemon.clone();
        let request_id_for_stream = request_id.clone();
        let mut cursor = if request.after <= 0 { 0 } else { request.after };
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(250));
            loop {
                ticker.tick().await;

                let records_result = daemon.with_state_store(|store| {
                    if let Some(scope_prefix) = scope.as_deref() {
                        store.load_events_by_scope(&stack_name, scope_prefix, Some(cursor), 128)
                    } else {
                        store.load_events_since_limited(&stack_name, cursor, 128)
                    }
                });

                let records = match records_result {
                    Ok(records) => records,
                    Err(error) => {
                        let _ = tx
                            .send(Err(status_from_stack_error(error, &request_id_for_stream)))
                            .await;
                        return;
                    }
                };

                for record in records {
                    cursor = record.id;
                    let event = match event_record_to_runtime_event(&record) {
                        Ok(event) => event,
                        Err(status) => {
                            let _ = tx.send(Err(status)).await;
                            return;
                        }
                    };
                    if tx.send(Ok(event)).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}
