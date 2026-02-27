use super::super::*;
pub(in crate::grpc) struct LeaseServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl LeaseServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

#[tonic::async_trait]
impl runtime_v2::lease_service_server::LeaseService for LeaseServiceImpl {
    async fn open_lease(
        &self,
        request: Request<runtime_v2::OpenLeaseRequest>,
    ) -> Result<Response<runtime_v2::LeaseResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::OpenLease,
            &metadata,
            &request_id,
        )?;
        let idempotency_key = metadata.idempotency_key.clone();
        let normalized_idempotency_key = normalize_idempotency_key(idempotency_key.as_deref());

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "sandbox_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let ttl_secs = if request.ttl_secs == 0 {
            300
        } else {
            request.ttl_secs
        };
        let request_hash = create_open_lease_request_hash(&sandbox_id, ttl_secs);

        if let Some(key) = normalized_idempotency_key {
            if let Some(cached_lease) = load_idempotent_lease_replay(
                &self.daemon,
                key,
                "open_lease",
                &request_hash,
                &request_id,
            )? {
                return Ok(Response::new(runtime_v2::LeaseResponse {
                    request_id: request_id.clone(),
                    lease: Some(lease_to_proto_payload(&cached_lease)),
                }));
            }
        }

        let now = current_unix_secs();
        let mut lease = Lease {
            lease_id: generate_lease_id(),
            sandbox_id,
            ttl_secs,
            last_heartbeat_at: now,
            state: LeaseState::Opening,
        };
        lease.transition_to(LeaseState::Active).map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                error.to_string(),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;

        let receipt_id = generate_receipt_id();
        let persist_result = self.daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_lease(&lease)?;
                tx.emit_event(
                    &lease.sandbox_id,
                    &StackEvent::LeaseOpened {
                        sandbox_id: lease.sandbox_id.clone(),
                        lease_id: lease.lease_id.clone(),
                    },
                )?;
                tx.save_receipt(&Receipt {
                    receipt_id: receipt_id.clone(),
                    operation: "open_lease".to_string(),
                    entity_id: lease.lease_id.clone(),
                    entity_type: "lease".to_string(),
                    request_id: request_id.clone(),
                    status: "success".to_string(),
                    created_at: now,
                    metadata: receipt_idempotent_mutation_metadata(
                        "lease_opened",
                        request_hash.as_str(),
                        normalized_idempotency_key,
                    )?,
                })?;
                if let Some(key) = normalized_idempotency_key {
                    tx.save_idempotency_result(&IdempotencyRecord {
                        key: key.to_string(),
                        operation: "open_lease".to_string(),
                        request_hash: request_hash.clone(),
                        response_json: lease.lease_id.clone(),
                        status_code: 201,
                        created_at: now,
                        expires_at: now.saturating_add(IDEMPOTENCY_TTL_SECS),
                    })?;
                }
                Ok(())
            })
        });
        if let Err(error) = persist_result {
            if let Some(key) = normalized_idempotency_key {
                if let Some(cached_lease) = load_idempotent_lease_replay(
                    &self.daemon,
                    key,
                    "open_lease",
                    &request_hash,
                    &request_id,
                )? {
                    return Ok(Response::new(runtime_v2::LeaseResponse {
                        request_id,
                        lease: Some(lease_to_proto_payload(&cached_lease)),
                    }));
                }
            }
            return Err(status_from_stack_error(error, &request_id));
        }

        let mut response = Response::new(runtime_v2::LeaseResponse {
            request_id,
            lease: Some(lease_to_proto_payload(&lease)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn get_lease(
        &self,
        request: Request<runtime_v2::GetLeaseRequest>,
    ) -> Result<Response<runtime_v2::LeaseResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let lease = self
            .daemon
            .with_state_store(|store| store.load_lease(&request.lease_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("lease not found: {}", request.lease_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        Ok(Response::new(runtime_v2::LeaseResponse {
            request_id,
            lease: Some(lease_to_proto_payload(&lease)),
        }))
    }

    async fn list_leases(
        &self,
        request: Request<runtime_v2::ListLeasesRequest>,
    ) -> Result<Response<runtime_v2::ListLeasesResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let leases = self
            .daemon
            .with_state_store(|store| store.list_leases())
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .iter()
            .map(lease_to_proto_payload)
            .collect();

        Ok(Response::new(runtime_v2::ListLeasesResponse {
            request_id,
            leases,
        }))
    }

    async fn heartbeat_lease(
        &self,
        request: Request<runtime_v2::HeartbeatLeaseRequest>,
    ) -> Result<Response<runtime_v2::LeaseResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::HeartbeatLease,
            &metadata,
            &request_id,
        )?;

        let mut lease = self
            .daemon
            .with_state_store(|store| store.load_lease(&request.lease_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("lease not found: {}", request.lease_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if lease.state != LeaseState::Active {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("lease {} is not active", request.lease_id),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let now = current_unix_secs();
        lease.last_heartbeat_at = now;
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_lease(&lease)?;
                    tx.emit_event(
                        &lease.sandbox_id,
                        &StackEvent::LeaseHeartbeat {
                            lease_id: lease.lease_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "heartbeat_lease".to_string(),
                        entity_id: lease.lease_id.clone(),
                        entity_type: "lease".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("lease_heartbeat")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::LeaseResponse {
            request_id,
            lease: Some(lease_to_proto_payload(&lease)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn close_lease(
        &self,
        request: Request<runtime_v2::CloseLeaseRequest>,
    ) -> Result<Response<runtime_v2::LeaseResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CloseLease,
            &metadata,
            &request_id,
        )?;

        let mut lease = self
            .daemon
            .with_state_store(|store| store.load_lease(&request.lease_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("lease not found: {}", request.lease_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if lease.state == LeaseState::Closed {
            return Ok(Response::new(runtime_v2::LeaseResponse {
                request_id,
                lease: Some(lease_to_proto_payload(&lease)),
            }));
        }

        lease.transition_to(LeaseState::Closed).map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                error.to_string(),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_lease(&lease)?;
                    tx.emit_event(
                        &lease.sandbox_id,
                        &StackEvent::LeaseClosed {
                            lease_id: lease.lease_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "close_lease".to_string(),
                        entity_id: lease.lease_id.clone(),
                        entity_type: "lease".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("lease_closed")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(runtime_v2::LeaseResponse {
            request_id,
            lease: Some(lease_to_proto_payload(&lease)),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }
}
