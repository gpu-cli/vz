use super::*;

#[tonic::async_trait]
impl runtime_v2::sandbox_service_server::SandboxService for SandboxServiceImpl {
    type CreateSandboxStream = CreateSandboxEventStream;
    type PrepareSpaceCacheStream = PrepareSpaceCacheEventStream;
    type ExportSpaceCacheStream = ExportSpaceCacheEventStream;
    type ImportSpaceCacheStream = ImportSpaceCacheEventStream;
    type TerminateSandboxStream = TerminateSandboxEventStream;
    type OpenSandboxShellStream = OpenSandboxShellEventStream;
    type CloseSandboxShellStream = CloseSandboxShellEventStream;

    async fn create_sandbox(
        &self,
        request: Request<runtime_v2::CreateSandboxRequest>,
    ) -> Result<Response<Self::CreateSandboxStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let mut sequence = 1u64;
        let mut events = vec![Ok(create_sandbox_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating create sandbox request",
        ))];
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateSandbox,
            &metadata,
            &request_id,
        )?;
        let idempotency_key = metadata.idempotency_key.clone();

        let sandbox_id = request.stack_name.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "stack_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let cpus = if request.cpus == 0 {
            None
        } else {
            Some(u8::try_from(request.cpus).map_err(|_| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::ValidationError,
                    format!("cpus out of range for u8: {}", request.cpus),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?)
        };
        let request_hash = create_sandbox_request_hash(&request, cpus);
        let normalized_idempotency_key = normalize_idempotency_key(idempotency_key.as_deref());
        let mut labels: BTreeMap<String, String> = request.labels.into_iter().collect();
        // Requesters cannot predeclare default-source audit labels.
        labels.remove(SANDBOX_LABEL_BASE_IMAGE_DEFAULT_SOURCE);
        labels.remove(SANDBOX_LABEL_MAIN_CONTAINER_DEFAULT_SOURCE);

        let requested_base_image_ref = labels
            .get(SANDBOX_LABEL_BASE_IMAGE_REF)
            .and_then(|value| normalize_optional_wire_field(value));
        let requested_main_container = labels
            .get(SANDBOX_LABEL_MAIN_CONTAINER)
            .and_then(|value| normalize_optional_wire_field(value));
        let startup_defaults = self
            .daemon
            .resolve_sandbox_startup_defaults(requested_base_image_ref, requested_main_container);

        if let Some(base_image_ref) = startup_defaults.base_image_ref.as_deref() {
            labels.insert(
                SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
                base_image_ref.to_string(),
            );
        }
        if let Some(main_container) = startup_defaults.main_container.as_deref() {
            labels.insert(
                SANDBOX_LABEL_MAIN_CONTAINER.to_string(),
                main_container.to_string(),
            );
        }
        if let Some(default_source) = startup_defaults.base_image_default_source {
            labels.insert(
                SANDBOX_LABEL_BASE_IMAGE_DEFAULT_SOURCE.to_string(),
                default_source.as_label_value().to_string(),
            );
        }
        if let Some(default_source) = startup_defaults.main_container_default_source {
            labels.insert(
                SANDBOX_LABEL_MAIN_CONTAINER_DEFAULT_SOURCE.to_string(),
                default_source.as_label_value().to_string(),
            );
        }

        if let Some(key) = normalized_idempotency_key {
            if let Some(cached_sandbox) = load_idempotent_sandbox_replay(
                &self.daemon,
                key,
                "create_sandbox",
                &request_hash,
                &request_id,
            )? {
                sequence += 1;
                events.push(Ok(create_sandbox_progress_event(
                    &request_id,
                    sequence,
                    "idempotency_replay",
                    "replaying cached create sandbox result",
                )));
                sequence += 1;
                events.push(Ok(create_sandbox_completion_event(
                    &request_id,
                    sequence,
                    runtime_v2::SandboxResponse {
                        request_id: request_id.clone(),
                        sandbox: Some(sandbox_to_proto_payload(&cached_sandbox)),
                    },
                    "",
                )));
                return Ok(sandbox_stream_response(events, None));
            }
        }

        let exists = self
            .daemon
            .with_state_store(|store| store.load_sandbox(&sandbox_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .is_some();
        if exists {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("sandbox already exists: {sandbox_id}"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        self.daemon
            .enforce_create_sandbox_placement(&request_id)
            .map_err(status_from_machine_error)?;
        let memory_mb = if request.memory_mb == 0 {
            None
        } else {
            Some(request.memory_mb)
        };
        if startup_defaults.base_image_default_source.is_some()
            || startup_defaults.main_container_default_source.is_some()
        {
            sequence += 1;
            events.push(Ok(create_sandbox_progress_event(
                &request_id,
                sequence,
                "applying_defaults",
                "applying daemon sandbox startup policy defaults",
            )));
        }
        sequence += 1;
        events.push(Ok(create_sandbox_progress_event(
            &request_id,
            sequence,
            "booting_runtime",
            "booting sandbox runtime resources",
        )));
        if let Err(status) = boot_runtime_sandbox_resources(
            self.daemon.clone(),
            &sandbox_id,
            cpus,
            memory_mb,
            &labels,
            &request_id,
        )
        .await
        {
            events.push(Err(status));
            return Ok(sandbox_stream_response(events, None));
        }

        let spec = SandboxSpec {
            cpus,
            memory_mb,
            base_image_ref: startup_defaults.base_image_ref,
            main_container: startup_defaults.main_container,
            network_profile: None,
            volume_mounts: Vec::new(),
        };

        let now = current_unix_secs();
        let sandbox = Sandbox {
            sandbox_id: sandbox_id.clone(),
            backend: daemon_backend(self.daemon.backend_name()),
            spec,
            state: SandboxState::Ready,
            created_at: now,
            updated_at: now,
            labels,
        };

        sequence += 1;
        events.push(Ok(create_sandbox_progress_event(
            &request_id,
            sequence,
            "persisting",
            "persisting sandbox state and receipt",
        )));
        let receipt_id = generate_receipt_id();
        let persist_result = self.daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                if tx.load_sandbox(&sandbox.sandbox_id)?.is_some() {
                    return Err(StackError::Machine {
                        code: MachineErrorCode::StateConflict,
                        message: format!("sandbox already exists: {}", sandbox.sandbox_id),
                    });
                }
                tx.save_sandbox(&sandbox)?;
                tx.emit_event(
                    &sandbox.sandbox_id,
                    &StackEvent::SandboxReady {
                        stack_name: sandbox_stack_name(&sandbox),
                        sandbox_id: sandbox.sandbox_id.clone(),
                    },
                )?;
                tx.save_receipt(&Receipt {
                    receipt_id: receipt_id.clone(),
                    operation: "create_sandbox".to_string(),
                    entity_id: sandbox.sandbox_id.clone(),
                    entity_type: "sandbox".to_string(),
                    request_id: request_id.clone(),
                    status: "success".to_string(),
                    created_at: now,
                    metadata: receipt_idempotent_mutation_metadata(
                        "sandbox_ready",
                        request_hash.as_str(),
                        normalized_idempotency_key,
                    )?,
                })?;
                if let Some(key) = normalized_idempotency_key {
                    tx.save_idempotency_result(&IdempotencyRecord {
                        key: key.to_string(),
                        operation: "create_sandbox".to_string(),
                        request_hash: request_hash.clone(),
                        response_json: sandbox.sandbox_id.clone(),
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
                if let Some(cached_sandbox) = load_idempotent_sandbox_replay(
                    &self.daemon,
                    key,
                    "create_sandbox",
                    &request_hash,
                    &request_id,
                )? {
                    sequence += 1;
                    events.push(Ok(create_sandbox_progress_event(
                        &request_id,
                        sequence,
                        "idempotency_replay",
                        "replaying cached create sandbox result after persistence race",
                    )));
                    sequence += 1;
                    events.push(Ok(create_sandbox_completion_event(
                        &request_id,
                        sequence,
                        runtime_v2::SandboxResponse {
                            request_id: request_id.clone(),
                            sandbox: Some(sandbox_to_proto_payload(&cached_sandbox)),
                        },
                        "",
                    )));
                    return Ok(sandbox_stream_response(events, None));
                }
            }

            let exists_after_error = self
                .daemon
                .with_state_store(|store| store.load_sandbox(&sandbox_id))
                .map_err(|store_error| status_from_stack_error(store_error, &request_id))?
                .is_some();
            if exists_after_error {
                events.push(Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::StateConflict,
                    format!("sandbox already exists: {sandbox_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))));
                return Ok(sandbox_stream_response(events, None));
            }

            if let Err(cleanup_error) =
                terminate_runtime_sandbox_resources(self.daemon.clone(), &sandbox_id, &request_id)
                    .await
            {
                warn!(
                    sandbox_id = %sandbox_id,
                    request_id = %request_id,
                    error = %cleanup_error,
                    "failed to clean up runtime resources after create_sandbox persistence failure"
                );
            }

            events.push(Err(status_from_stack_error(error, &request_id)));
            return Ok(sandbox_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(create_sandbox_completion_event(
            &request_id,
            sequence,
            runtime_v2::SandboxResponse {
                request_id: request_id.clone(),
                sandbox: Some(sandbox_to_proto_payload(&sandbox)),
            },
            receipt_id.as_str(),
        )));
        Ok(sandbox_stream_response(events, Some(receipt_id.as_str())))
    }

    async fn prepare_space_cache(
        &self,
        request: Request<runtime_v2::PrepareSpaceCacheRequest>,
    ) -> Result<Response<Self::PrepareSpaceCacheStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateSandbox,
            &metadata,
            &request_id,
        )?;
        let mut sequence = 1u64;
        let mut events = vec![Ok(prepare_space_cache_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating cache key payloads",
        ))];

        if request.keys.is_empty() {
            sequence += 1;
            events.push(Ok(prepare_space_cache_completion_event(
                &request_id,
                sequence,
                Vec::new(),
                "",
            )));
            return Ok(sandbox_stream_response(events, None));
        }

        let mut keys = Vec::with_capacity(request.keys.len());
        for key in request.keys {
            if key.cache_name.trim().is_empty() {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::ValidationError,
                    "space cache key cache_name cannot be empty".to_string(),
                    Some(request_id),
                    BTreeMap::new(),
                )));
            }
            if key.digest_hex.trim().is_empty() {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::ValidationError,
                    format!(
                        "space cache key digest_hex cannot be empty for cache `{}`",
                        key.cache_name
                    ),
                    Some(request_id),
                    BTreeMap::new(),
                )));
            }
            let schema_version = u16::try_from(key.schema_version).map_err(|_| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::ValidationError,
                    format!(
                        "space cache key schema_version out of range for cache `{}`: {}",
                        key.cache_name, key.schema_version
                    ),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;
            keys.push(SpaceCacheKey {
                schema_version,
                cache_name: key.cache_name,
                digest_hex: key.digest_hex,
                canonical_json: key.canonical_json,
            });
        }

        sequence += 1;
        events.push(Ok(prepare_space_cache_progress_event(
            &request_id,
            sequence,
            "resolving",
            "resolving cache key hits, misses, and remote materialization",
        )));

        let index_path = daemon_space_cache_index_path(self.daemon.as_ref());
        let remote_cache_trust = SpaceRemoteCacheTrustConfig::from_env().map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!("invalid daemon remote cache trust env configuration: {error}"),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;
        let mut index = SpaceCacheIndex::load(&index_path).map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::InternalError,
                format!(
                    "failed to load space cache index {}: {error}",
                    index_path.display()
                ),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;
        let invalidated = index.invalidate_for_schema(SPACE_CACHE_KEY_SCHEMA_VERSION);
        if invalidated > 0 {
            sequence += 1;
            events.push(Ok(prepare_space_cache_progress_event(
                &request_id,
                sequence,
                "invalidating",
                format!(
                    "invalidated {invalidated} cache entries for schema v{SPACE_CACHE_KEY_SCHEMA_VERSION}"
                )
                .as_str(),
            )));
        }

        let mut outcomes = Vec::with_capacity(keys.len());
        let mut remote_verified_materialized = 0usize;
        let mut remote_miss_untrusted = 0usize;
        for key in &keys {
            let lookup = index.lookup(key);
            let (mut outcome, mut detail) = match lookup {
                SpaceCacheLookup::Hit => (
                    runtime_v2::SpaceCacheTrustOutcome::LocalHit,
                    "local cache hit".to_string(),
                ),
                SpaceCacheLookup::MissNotFound => (
                    runtime_v2::SpaceCacheTrustOutcome::LocalMissCold,
                    "local cache miss (cold)".to_string(),
                ),
                SpaceCacheLookup::MissKeyMismatch => (
                    runtime_v2::SpaceCacheTrustOutcome::LocalMissDimensionChange,
                    "local cache miss (dimension change)".to_string(),
                ),
                SpaceCacheLookup::MissVersionMismatch { requested, stored } => (
                    runtime_v2::SpaceCacheTrustOutcome::LocalMissSchemaMismatch,
                    format!(
                        "local cache miss (schema mismatch: stored=v{stored}, requested=v{requested})"
                    ),
                ),
            };
            if !matches!(lookup, SpaceCacheLookup::Hit)
                && let Some(remote_cache_trust) = remote_cache_trust.as_ref()
            {
                match remote_cache_trust.verify_key(key) {
                    SpaceRemoteCacheVerificationOutcome::Verified { artifact } => {
                        match daemon_materialize_verified_remote_cache_artifact(
                            self.daemon.as_ref(),
                            key,
                            &artifact,
                        ) {
                            Ok(path) => {
                                outcome =
                                    runtime_v2::SpaceCacheTrustOutcome::RemoteVerifiedMaterialized;
                                detail = format!(
                                    "remote cache verified and materialized at {}",
                                    path.display()
                                );
                                remote_verified_materialized =
                                    remote_verified_materialized.saturating_add(1);
                            }
                            Err(_) => {
                                outcome = runtime_v2::SpaceCacheTrustOutcome::RemoteMissUntrusted;
                                detail =
                                    "remote cache verification passed but materialization failed"
                                        .to_string();
                                remote_miss_untrusted = remote_miss_untrusted.saturating_add(1);
                            }
                        }
                    }
                    SpaceRemoteCacheVerificationOutcome::Miss(reason) => {
                        outcome = runtime_v2::SpaceCacheTrustOutcome::RemoteMissUntrusted;
                        detail = format!("remote cache miss ({})", reason.diagnostic());
                        remote_miss_untrusted = remote_miss_untrusted.saturating_add(1);
                    }
                }
            }
            outcomes.push(runtime_v2::SpaceCacheOutcomePayload {
                cache_name: key.cache_name.clone(),
                digest_hex: key.digest_hex.clone(),
                outcome: outcome as i32,
                detail,
            });
            index.upsert(key.clone());
        }
        index.save(&index_path).map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::InternalError,
                format!(
                    "failed to save space cache index {}: {error}",
                    index_path.display()
                ),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;

        sequence += 1;
        events.push(Ok(prepare_space_cache_progress_event(
            &request_id,
            sequence,
            "persisting",
            "persisting cache preparation receipt and event",
        )));

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        let metadata = serde_json::to_value(PrepareSpaceCacheReceiptMetadata {
            event_type: "space_cache_prepared",
            prepared: outcomes.len(),
            remote_verified_materialized,
            remote_miss_untrusted,
        })
        .map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::InternalError,
                format!("failed to serialize prepare_space_cache receipt metadata: {error}"),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;
        let status = if remote_miss_untrusted > 0 {
            "warning"
        } else {
            "success"
        };
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.emit_event(
                        "daemon",
                        &StackEvent::DriftDetected {
                            stack_name: "daemon".to_string(),
                            category: "space_cache_prepare".to_string(),
                            description: format!(
                                "space cache prepare completed: prepared={} remote_verified={} remote_untrusted={}",
                                outcomes.len(),
                                remote_verified_materialized,
                                remote_miss_untrusted
                            ),
                            severity: if remote_miss_untrusted > 0 {
                                "warning".to_string()
                            } else {
                                "info".to_string()
                            },
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "prepare_space_cache".to_string(),
                        entity_id: "space_cache".to_string(),
                        entity_type: "cache".to_string(),
                        request_id: request_id.clone(),
                        status: status.to_string(),
                        created_at: now,
                        metadata,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        sequence += 1;
        events.push(Ok(prepare_space_cache_completion_event(
            &request_id,
            sequence,
            outcomes,
            receipt_id.as_str(),
        )));
        Ok(sandbox_stream_response(events, Some(receipt_id.as_str())))
    }

    async fn export_space_cache(
        &self,
        request: Request<runtime_v2::ExportSpaceCacheRequest>,
    ) -> Result<Response<Self::ExportSpaceCacheStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateSandbox,
            &metadata,
            &request_id,
        )?;

        let cache_name = request.cache_name.trim().to_string();
        let digest_hex = request.digest_hex.trim().to_string();
        let stream_path = request.stream_path.trim().to_string();
        if cache_name.is_empty() || digest_hex.is_empty() || stream_path.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "cache_name, digest_hex, and stream_path are required".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let mut sequence = 1u64;
        let mut events = vec![Ok(export_space_cache_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating cache export request",
        ))];

        let source_path = daemon_space_cache_artifact_dir_for_identity(
            self.daemon.as_ref(),
            cache_name.as_str(),
            digest_hex.as_str(),
        );
        if !source_path.is_dir() {
            events.push(Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("cache artifact not found at {}", source_path.display()),
                Some(request_id),
                BTreeMap::new(),
            ))));
            return Ok(sandbox_stream_response(events, None));
        }
        let stream_path_buf = PathBuf::from(stream_path.clone());
        if let Some(parent) = stream_path_buf.parent()
            && let Err(error) = std::fs::create_dir_all(parent)
        {
            events.push(Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!(
                    "failed to create export stream parent directory {}: {error}",
                    parent.display()
                ),
                Some(request_id),
                BTreeMap::new(),
            ))));
            return Ok(sandbox_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(export_space_cache_progress_event(
            &request_id,
            sequence,
            "exporting",
            "streaming btrfs send payload",
        )));
        if let Err(error) = export_subvolume_send_stream(&source_path, &stream_path_buf) {
            events.push(Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::BackendUnavailable,
                format!("failed to export cache artifact via btrfs send: {error}"),
                Some(request_id),
                BTreeMap::new(),
            ))));
            return Ok(sandbox_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(export_space_cache_completion_event(
            &request_id,
            sequence,
            cache_name.as_str(),
            digest_hex.as_str(),
            stream_path.as_str(),
        )));
        Ok(sandbox_stream_response(events, None))
    }

    async fn import_space_cache(
        &self,
        request: Request<runtime_v2::ImportSpaceCacheRequest>,
    ) -> Result<Response<Self::ImportSpaceCacheStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateSandbox,
            &metadata,
            &request_id,
        )?;

        let cache_name = request.cache_name.trim().to_string();
        let digest_hex = request.digest_hex.trim().to_string();
        let stream_path = request.stream_path.trim().to_string();
        if cache_name.is_empty() || digest_hex.is_empty() || stream_path.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "cache_name, digest_hex, and stream_path are required".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let mut sequence = 1u64;
        let mut events = vec![Ok(import_space_cache_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating cache import request",
        ))];
        let stream_path_buf = PathBuf::from(stream_path.clone());
        if !stream_path_buf.is_file() {
            events.push(Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("cache stream path not found: {}", stream_path_buf.display()),
                Some(request_id),
                BTreeMap::new(),
            ))));
            return Ok(sandbox_stream_response(events, None));
        }

        let destination_root = self
            .daemon
            .state_store_path()
            .parent()
            .map(|parent| {
                parent
                    .join(SPACE_CACHE_ARTIFACTS_DIR)
                    .join(cache_name.as_str())
            })
            .unwrap_or_else(|| PathBuf::from(SPACE_CACHE_ARTIFACTS_DIR).join(cache_name.as_str()));
        if let Err(error) = std::fs::create_dir_all(&destination_root) {
            events.push(Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!(
                    "failed to create cache destination root {}: {error}",
                    destination_root.display()
                ),
                Some(request_id),
                BTreeMap::new(),
            ))));
            return Ok(sandbox_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(import_space_cache_progress_event(
            &request_id,
            sequence,
            "importing",
            "receiving btrfs send payload",
        )));
        let received_subvolume =
            match import_subvolume_receive_stream(&stream_path_buf, &destination_root) {
                Ok(path) => path,
                Err(error) => {
                    events.push(Err(status_from_machine_error(MachineError::new(
                        MachineErrorCode::BackendUnavailable,
                        format!("failed to import cache artifact via btrfs receive: {error}"),
                        Some(request_id),
                        BTreeMap::new(),
                    ))));
                    return Ok(sandbox_stream_response(events, None));
                }
            };

        let expected_path = daemon_space_cache_artifact_dir_for_identity(
            self.daemon.as_ref(),
            &cache_name,
            &digest_hex,
        );
        if received_subvolume != expected_path {
            if expected_path.exists()
                && let Err(error) = std::fs::remove_dir_all(&expected_path)
            {
                events.push(Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::InternalError,
                    format!(
                        "failed to remove pre-existing cache artifact {} before rename: {error}",
                        expected_path.display()
                    ),
                    Some(request_id),
                    BTreeMap::new(),
                ))));
                return Ok(sandbox_stream_response(events, None));
            }
            if let Err(error) = std::fs::rename(&received_subvolume, &expected_path) {
                events.push(Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::InternalError,
                    format!(
                        "failed to normalize imported cache path {} -> {}: {error}",
                        received_subvolume.display(),
                        expected_path.display()
                    ),
                    Some(request_id),
                    BTreeMap::new(),
                ))));
                return Ok(sandbox_stream_response(events, None));
            }
        }

        let receipt_id = generate_receipt_id();
        let now = current_unix_secs();
        let metadata = serde_json::json!({
            "event_type": "space_cache_imported",
            "cache_name": cache_name,
            "digest_hex": digest_hex,
            "stream_path": stream_path,
            "received_subvolume_path": expected_path.display().to_string(),
        });
        if let Err(error) = self.daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.emit_event(
                    "daemon",
                    &StackEvent::DriftDetected {
                        stack_name: "daemon".to_string(),
                        category: "space_cache_import".to_string(),
                        description: format!(
                            "imported space cache artifact {}:{} from {}",
                            cache_name, digest_hex, stream_path
                        ),
                        severity: "info".to_string(),
                    },
                )?;
                tx.save_receipt(&Receipt {
                    receipt_id: receipt_id.clone(),
                    operation: "import_space_cache".to_string(),
                    entity_id: format!("{cache_name}:{digest_hex}"),
                    entity_type: "cache".to_string(),
                    request_id: request_id.clone(),
                    status: "success".to_string(),
                    created_at: now,
                    metadata,
                })?;
                Ok(())
            })
        }) {
            events.push(Err(status_from_stack_error(error, &request_id)));
            return Ok(sandbox_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(import_space_cache_completion_event(
            &request_id,
            sequence,
            cache_name.as_str(),
            digest_hex.as_str(),
            expected_path.display().to_string().as_str(),
            receipt_id.as_str(),
        )));
        Ok(sandbox_stream_response(events, Some(receipt_id.as_str())))
    }

    async fn get_sandbox(
        &self,
        request: Request<runtime_v2::GetSandboxRequest>,
    ) -> Result<Response<runtime_v2::SandboxResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let sandbox = self
            .daemon
            .with_state_store(|store| store.load_sandbox(&request.sandbox_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("sandbox not found: {}", request.sandbox_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        Ok(Response::new(runtime_v2::SandboxResponse {
            request_id,
            sandbox: Some(sandbox_to_proto_payload(&sandbox)),
        }))
    }

    async fn list_sandboxes(
        &self,
        request: Request<runtime_v2::ListSandboxesRequest>,
    ) -> Result<Response<runtime_v2::ListSandboxesResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let sandboxes = self
            .daemon
            .with_state_store(|store| store.list_sandboxes())
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .iter()
            .map(sandbox_to_proto_payload)
            .collect();

        Ok(Response::new(runtime_v2::ListSandboxesResponse {
            request_id,
            sandboxes,
        }))
    }

    async fn open_sandbox_shell(
        &self,
        request: Request<runtime_v2::OpenSandboxShellRequest>,
    ) -> Result<Response<Self::OpenSandboxShellStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let mut sequence = 1u64;
        let mut events = vec![Ok(open_sandbox_shell_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating sandbox shell request",
        ))];

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "sandbox_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let sandbox = match self
            .daemon
            .with_state_store(|store| store.load_sandbox(&sandbox_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
        {
            Some(sandbox) => sandbox,
            None => {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("sandbox not found: {sandbox_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                )));
            }
        };
        if sandbox.state.is_terminal() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("sandbox {sandbox_id} is in terminal state"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        sequence += 1;
        events.push(Ok(open_sandbox_shell_progress_event(
            &request_id,
            sequence,
            "ensuring_container",
            "ensuring shell container exists",
        )));
        let container_id = match ensure_sandbox_shell_container(
            self.daemon.clone(),
            &sandbox,
            &request_id,
            metadata.trace_id.as_deref(),
        )
        .await
        {
            Ok(container_id) => container_id,
            Err(status) => {
                events.push(Err(status));
                return Ok(Response::new(sandbox_shell_stream_from_events(events)));
            }
        };

        sequence += 1;
        events.push(Ok(open_sandbox_shell_progress_event(
            &request_id,
            sequence,
            "resolving_command",
            "resolving sandbox shell command",
        )));
        let (shell_command, shell_args) = resolve_sandbox_shell_command(&request_id, &sandbox)?;
        sequence += 1;
        events.push(Ok(open_sandbox_shell_progress_event(
            &request_id,
            sequence,
            "ensuring_execution",
            "ensuring interactive shell execution session",
        )));
        let execution_id = match ensure_sandbox_shell_execution(
            self.daemon.clone(),
            &sandbox,
            &container_id,
            &shell_command,
            &shell_args,
            &request_id,
            metadata.trace_id.as_deref(),
        )
        .await
        {
            Ok(execution_id) => execution_id,
            Err(status) => {
                events.push(Err(status));
                return Ok(Response::new(sandbox_shell_stream_from_events(events)));
            }
        };
        sequence += 1;
        events.push(Ok(open_sandbox_shell_completion_event(
            &request_id,
            sequence,
            runtime_v2::OpenSandboxShellResponse {
                request_id: request_id.clone(),
                sandbox_id: sandbox.sandbox_id,
                container_id,
                cmd: vec![shell_command],
                args: shell_args,
                execution_id,
            },
        )));
        Ok(Response::new(sandbox_shell_stream_from_events(events)))
    }

    async fn close_sandbox_shell(
        &self,
        request: Request<runtime_v2::CloseSandboxShellRequest>,
    ) -> Result<Response<Self::CloseSandboxShellStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let mut sequence = 1u64;
        let mut events = vec![Ok(close_sandbox_shell_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating close shell request",
        ))];

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "sandbox_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let sandbox = self
            .daemon
            .with_state_store(|store| store.load_sandbox(&sandbox_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("sandbox not found: {sandbox_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        let execution_id = resolve_close_sandbox_shell_execution_id(
            self.daemon.as_ref(),
            &sandbox,
            normalize_optional_wire_field(&request.execution_id).as_deref(),
            &request_id,
        )?;

        sequence += 1;
        events.push(Ok(close_sandbox_shell_progress_event(
            &request_id,
            sequence,
            "canceling_execution",
            "canceling active shell execution",
        )));
        let execution_service =
            super::super::execution::ExecutionServiceImpl::new(self.daemon.clone());
        match execution_service
            .cancel_execution(Request::new(runtime_v2::CancelExecutionRequest {
                execution_id: execution_id.clone(),
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: request_id.clone(),
                    idempotency_key: String::new(),
                    trace_id: metadata.trace_id.unwrap_or_default(),
                }),
            }))
            .await
        {
            Ok(_) => {}
            Err(status) => {
                events.push(Err(status));
                return Ok(Response::new(sandbox_shell_stream_from_events(events)));
            }
        };
        sequence += 1;
        events.push(Ok(close_sandbox_shell_completion_event(
            &request_id,
            sequence,
            runtime_v2::CloseSandboxShellResponse {
                request_id: request_id.clone(),
                sandbox_id: sandbox.sandbox_id,
                execution_id,
            },
        )));
        Ok(Response::new(sandbox_shell_stream_from_events(events)))
    }

    async fn terminate_sandbox(
        &self,
        request: Request<runtime_v2::TerminateSandboxRequest>,
    ) -> Result<Response<Self::TerminateSandboxStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let mut sequence = 1u64;
        let mut events = vec![Ok(terminate_sandbox_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating terminate sandbox request",
        ))];
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::TerminateSandbox,
            &metadata,
            &request_id,
        )?;
        let idempotency_key = metadata.idempotency_key.clone();
        let normalized_idempotency_key = normalize_idempotency_key(idempotency_key.as_deref());
        let request_hash = format!("sandbox_id={}", request.sandbox_id.trim());

        if let Some(key) = normalized_idempotency_key {
            if let Some(cached_sandbox) = load_idempotent_sandbox_replay(
                &self.daemon,
                key,
                "terminate_sandbox",
                &request_hash,
                &request_id,
            )? {
                sequence += 1;
                events.push(Ok(terminate_sandbox_progress_event(
                    &request_id,
                    sequence,
                    "idempotency_replay",
                    "replaying cached terminate sandbox result",
                )));
                sequence += 1;
                events.push(Ok(terminate_sandbox_completion_event(
                    &request_id,
                    sequence,
                    runtime_v2::SandboxResponse {
                        request_id: request_id.clone(),
                        sandbox: Some(sandbox_to_proto_payload(&cached_sandbox)),
                    },
                    "",
                )));
                return Ok(sandbox_stream_response(events, None));
            }
        }

        let now = current_unix_secs();
        let mut sandbox = self
            .daemon
            .with_state_store(|store| store.load_sandbox(&request.sandbox_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("sandbox not found: {}", request.sandbox_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if sandbox.state != SandboxState::Terminated {
            sequence += 1;
            events.push(Ok(terminate_sandbox_progress_event(
                &request_id,
                sequence,
                "tearing_down_runtime",
                "terminating sandbox runtime resources",
            )));
            if let Err(status) = terminate_runtime_sandbox_resources(
                self.daemon.clone(),
                &sandbox.sandbox_id,
                &request_id,
            )
            .await
            {
                events.push(Err(status));
                return Ok(sandbox_stream_response(events, None));
            }

            sandbox.state = SandboxState::Terminated;
            sandbox.updated_at = now;
            sequence += 1;
            events.push(Ok(terminate_sandbox_progress_event(
                &request_id,
                sequence,
                "persisting",
                "persisting sandbox termination state and receipt",
            )));
            let receipt_id = generate_receipt_id();
            let persist_result = self.daemon.with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_sandbox(&sandbox)?;
                    tx.emit_event(
                        &sandbox.sandbox_id,
                        &StackEvent::SandboxTerminated {
                            stack_name: sandbox_stack_name(&sandbox),
                            sandbox_id: sandbox.sandbox_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "terminate_sandbox".to_string(),
                        entity_id: sandbox.sandbox_id.clone(),
                        entity_type: "sandbox".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_idempotent_mutation_metadata(
                            "sandbox_terminated",
                            request_hash.as_str(),
                            normalized_idempotency_key,
                        )?,
                    })?;
                    if let Some(key) = normalized_idempotency_key {
                        tx.save_idempotency_result(&IdempotencyRecord {
                            key: key.to_string(),
                            operation: "terminate_sandbox".to_string(),
                            request_hash: request_hash.clone(),
                            response_json: sandbox.sandbox_id.clone(),
                            status_code: 200,
                            created_at: now,
                            expires_at: now.saturating_add(IDEMPOTENCY_TTL_SECS),
                        })?;
                    }
                    Ok(())
                })
            });
            if let Err(error) = persist_result {
                if let Some(key) = normalized_idempotency_key {
                    if let Some(cached_sandbox) = load_idempotent_sandbox_replay(
                        &self.daemon,
                        key,
                        "terminate_sandbox",
                        &request_hash,
                        &request_id,
                    )? {
                        sequence += 1;
                        events.push(Ok(terminate_sandbox_progress_event(
                            &request_id,
                            sequence,
                            "idempotency_replay",
                            "replaying cached terminate sandbox result after persistence race",
                        )));
                        sequence += 1;
                        events.push(Ok(terminate_sandbox_completion_event(
                            &request_id,
                            sequence,
                            runtime_v2::SandboxResponse {
                                request_id: request_id.clone(),
                                sandbox: Some(sandbox_to_proto_payload(&cached_sandbox)),
                            },
                            "",
                        )));
                        return Ok(sandbox_stream_response(events, None));
                    }
                }
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(sandbox_stream_response(events, None));
            }

            sequence += 1;
            events.push(Ok(terminate_sandbox_completion_event(
                &request_id,
                sequence,
                runtime_v2::SandboxResponse {
                    request_id: request_id.clone(),
                    sandbox: Some(sandbox_to_proto_payload(&sandbox)),
                },
                receipt_id.as_str(),
            )));
            return Ok(sandbox_stream_response(events, Some(receipt_id.as_str())));
        }

        sequence += 1;
        events.push(Ok(terminate_sandbox_completion_event(
            &request_id,
            sequence,
            runtime_v2::SandboxResponse {
                request_id: request_id.clone(),
                sandbox: Some(sandbox_to_proto_payload(&sandbox)),
            },
            "",
        )));
        Ok(sandbox_stream_response(events, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vz_runtime_contract::RuntimeError;

    #[test]
    fn runtime_shutdown_not_active_detects_missing_shared_vm_message() {
        let error = RuntimeError::InvalidConfig(
            "no shared VM running for stack 'stack-a'; call boot_shared_vm first".to_string(),
        );
        assert!(runtime_shutdown_error_is_not_active(&error, "stack-a"));
    }

    #[test]
    fn runtime_shutdown_not_active_detects_stack_not_found_message() {
        let error = RuntimeError::Backend {
            message: "stack 'stack-b' not found".to_string(),
            source: Box::new(std::io::Error::other("stack missing")),
        };
        assert!(runtime_shutdown_error_is_not_active(&error, "stack-b"));
    }

    #[test]
    fn runtime_shutdown_not_active_ignores_unrelated_errors() {
        let error = RuntimeError::Backend {
            message: "permission denied while stopping vm process".to_string(),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "permission denied",
            )),
        };
        assert!(!runtime_shutdown_error_is_not_active(&error, "stack-c"));
    }
}
