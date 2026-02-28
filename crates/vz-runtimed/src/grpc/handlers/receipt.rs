use super::super::*;

pub(in crate::grpc) struct ReceiptServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl ReceiptServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

fn receipt_to_proto_payload(
    receipt: &Receipt,
    retention: Option<&vz_stack::ReceiptRetentionState>,
) -> Result<runtime_v2::ReceiptPayload, Status> {
    let metadata_json = serde_json::to_string(&receipt.metadata).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "failed to serialize receipt metadata for {}: {error}",
                receipt.receipt_id
            ),
            None,
            BTreeMap::new(),
        ))
    })?;
    let default_policy = vz_stack::ReceiptRetentionPolicy::default();
    let retention_expires_at = retention.map(|state| state.expires_at).unwrap_or_else(|| {
        receipt
            .created_at
            .saturating_add(default_policy.max_age_secs)
    });
    let retention_gc_reason = retention
        .and_then(|state| state.gc_reason)
        .map(vz_stack::RetentionGcReason::as_str)
        .unwrap_or_default()
        .to_string();
    Ok(runtime_v2::ReceiptPayload {
        receipt_id: receipt.receipt_id.clone(),
        operation: receipt.operation.clone(),
        entity_id: receipt.entity_id.clone(),
        entity_type: receipt.entity_type.clone(),
        request_id: receipt.request_id.clone(),
        status: receipt.status.clone(),
        created_at: receipt.created_at,
        metadata_json,
        retention_expires_at,
        retention_gc_reason,
        retention_policy: "bounded_age_count".to_string(),
    })
}

#[tonic::async_trait]
impl runtime_v2::receipt_service_server::ReceiptService for ReceiptServiceImpl {
    async fn get_receipt(
        &self,
        request: Request<runtime_v2::GetReceiptRequest>,
    ) -> Result<Response<runtime_v2::ReceiptResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let receipt_id = request.receipt_id.trim().to_string();
        if receipt_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "receipt_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let receipt = self
            .daemon
            .with_state_store(|store| store.load_receipt(&receipt_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("receipt not found: {receipt_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;
        let retention_states = self
            .daemon
            .with_state_store(|store| {
                store.receipt_retention_state_map(
                    vz_stack::ReceiptRetentionPolicy::default(),
                    current_unix_secs(),
                )
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        Ok(Response::new(runtime_v2::ReceiptResponse {
            request_id,
            receipt: Some(receipt_to_proto_payload(
                &receipt,
                retention_states.get(&receipt.receipt_id),
            )?),
        }))
    }
}
