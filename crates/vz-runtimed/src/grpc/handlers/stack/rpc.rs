use super::*;

#[tonic::async_trait]
impl runtime_v2::stack_service_server::StackService for StackServiceImpl {
    type ApplyStackStream = ApplyStackEventStream;
    type TeardownStackStream = TeardownStackEventStream;
    type StopStackServiceStream = StackServiceActionEventStream;
    type StartStackServiceStream = StackServiceActionEventStream;
    type RestartStackServiceStream = StackServiceActionEventStream;

    async fn apply_stack(
        &self,
        request: Request<runtime_v2::ApplyStackRequest>,
    ) -> Result<Response<Self::ApplyStackStream>, Status> {
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
        if request.compose_yaml.trim().is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "compose_yaml cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let workload_scope = validate_stack_apply_request_scope(
            self.daemon.as_ref(),
            request.scope.as_ref(),
            &stack_name,
            &request_id,
        )?;

        let spec = parse_stack_spec(&stack_name, &request.compose_yaml, &request.compose_dir)
            .map_err(|error| status_from_stack_error(error, &request_id))?;
        if spec.name != stack_name || spec.name != workload_scope.stack_id {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!(
                    "compose stack `{}` does not match requested scoped stack `{stack_name}`",
                    spec.name
                ),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        enforce_stack_policy_preflight_read_only(
            self.daemon.as_ref(),
            RuntimeOperation::CreateContainer,
            &metadata,
            &request_id,
        )?;
        let mut sequence = 1u64;
        let mut events = vec![Ok(apply_stack_progress_event(
            &request_id,
            sequence,
            "planning",
            "planning stack apply actions",
        ))];

        let health_statuses = HashMap::new();
        if request.dry_run {
            let (apply_result, observed) = match self.daemon.with_state_store(|store| {
                Ok((
                    plan_apply(&spec, store, &health_statuses)?,
                    store.load_observed_state(&stack_name)?,
                ))
            }) {
                Ok(value) => value,
                Err(error) => {
                    events.push(Err(status_from_stack_error(error, &request_id)));
                    return Ok(stack_stream_response(events, None));
                }
            };
            let mut projected = observed;
            for action in &apply_result.actions {
                match action {
                    Action::ServiceCreate { target, .. }
                    | Action::ServiceRecreate { target, .. } => {
                        if let Some(status) = projected
                            .iter_mut()
                            .find(|status| status.replica == *target)
                        {
                            status.phase = ServicePhase::Pending;
                            status.last_error = None;
                            status.ready = false;
                        } else {
                            projected.push(ServiceObservedState {
                                replica: target.clone(),
                                applied_config_digest: None,
                                phase: ServicePhase::Pending,
                                container_id: None,
                                failed_create_ownership: None,
                                last_error: None,
                                ready: false,
                            });
                        }
                    }
                    Action::ServiceRemove { target, .. } => {
                        if let Some(status) = projected
                            .iter_mut()
                            .find(|status| status.replica == *target)
                        {
                            status.phase = ServicePhase::Stopped;
                            status.last_error = None;
                            status.ready = false;
                        }
                    }
                }
            }
            let services: Vec<runtime_v2::StackServiceStatus> =
                projected.iter().map(stack_status_from_observed).collect();
            let services_ready = projected.iter().filter(|item| item.ready).count();
            let services_failed = projected
                .iter()
                .filter(|item| item.phase == ServicePhase::Failed)
                .count();
            sequence += 1;
            events.push(Ok(apply_stack_completion_event(
                &request_id,
                sequence,
                runtime_v2::ApplyStackResponse {
                    request_id: request_id.clone(),
                    stack_name,
                    changed_actions: apply_result.actions.len() as u32,
                    converged: false,
                    services_ready: services_ready as u32,
                    services_failed: services_failed as u32,
                    services,
                },
                "",
            )));
            return Ok(stack_stream_response(events, None));
        }

        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateContainer,
            &metadata,
            &request_id,
        )?;
        if let Err(error) = self.daemon.with_state_store(|store| {
            store.reserve_stack_workload_owner(&workload_scope, current_unix_secs())?;
            Ok(())
        }) {
            events.push(Err(status_from_stack_error(error, &request_id)));
            return Ok(stack_stream_response(events, None));
        }

        let preview_store = match self.daemon.open_dedicated_state_store() {
            Ok(store) => store,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        let apply_result = match plan_apply(&spec, &preview_store, &health_statuses) {
            Ok(result) => result,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };

        sequence += 1;
        events.push(Ok(apply_stack_progress_event(
            &request_id,
            sequence,
            "building_images",
            "running compose build directives",
        )));
        if let Err(error) = run_compose_builds(
            self.daemon.clone(),
            &spec,
            &request.compose_yaml,
            &request.compose_dir,
        )
        .await
        {
            events.push(Err(status_from_stack_error(error, &request_id)));
            return Ok(stack_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(apply_stack_progress_event(
            &request_id,
            sequence,
            "reconciling",
            "reconciling stack runtime state",
        )));
        let exec_store = match self.daemon.open_dedicated_state_store() {
            Ok(store) => store,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        let reconcile_store = match self.daemon.open_dedicated_state_store() {
            Ok(store) => store,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        let runtime = DaemonContainerRuntime::new(self.daemon.clone());
        let stack_dir = stack_runtime_dir(self.daemon.as_ref(), &stack_name);
        let executor =
            match StackExecutor::new_scoped(runtime, exec_store, &stack_dir, workload_scope) {
                Ok(executor) => executor,
                Err(error) => {
                    events.push(Err(status_from_stack_error(error, &request_id)));
                    return Ok(stack_stream_response(events, None));
                }
            };
        let config = if request.detach {
            OrchestrationConfig {
                max_rounds: 1,
                ..Default::default()
            }
        } else {
            OrchestrationConfig::default()
        };
        let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, config);
        let orchestration_result = match orchestrator.run(&spec, None) {
            Ok(result) => result,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        let observed = match orchestrator
            .executor()
            .store()
            .load_observed_state(&stack_name)
        {
            Ok(value) => value,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        let services: Vec<runtime_v2::StackServiceStatus> =
            observed.iter().map(stack_status_from_observed).collect();
        let changed_actions = apply_result.actions.len() as u32;
        let converged = orchestration_result.converged;
        let services_ready = orchestration_result.services_ready as u32;
        let services_failed = orchestration_result.services_failed as u32;

        sequence += 1;
        events.push(Ok(apply_stack_progress_event(
            &request_id,
            sequence,
            "persisting",
            "persisting stack apply receipt",
        )));
        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        let persist_result = self
            .daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.emit_event(
                        &stack_name,
                        &StackEvent::StackApplyCompleted {
                            stack_name: stack_name.clone(),
                            succeeded: orchestration_result.services_ready,
                            failed: orchestration_result.services_failed,
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "apply_stack".to_string(),
                        entity_id: stack_name.clone(),
                        entity_type: "stack".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_stack_apply_metadata(
                            changed_actions,
                            converged,
                            services_ready,
                            services_failed,
                        )?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id));
        if let Err(status) = persist_result {
            events.push(Err(status));
            return Ok(stack_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(apply_stack_completion_event(
            &request_id,
            sequence,
            runtime_v2::ApplyStackResponse {
                request_id: request_id.clone(),
                stack_name,
                changed_actions,
                converged,
                services_ready,
                services_failed,
                services,
            },
            receipt_id.as_str(),
        )));
        Ok(stack_stream_response(events, Some(receipt_id.as_str())))
    }

    async fn teardown_stack(
        &self,
        request: Request<runtime_v2::TeardownStackRequest>,
    ) -> Result<Response<Self::TeardownStackStream>, Status> {
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
        // Hold one daemon-wide per-stack lease across admission, stack-wide
        // finalizer effects, claimed commit, and receipt persistence. Exact
        // duplicate requests wait here and observe the first request's durable
        // receipt instead of concurrently consuming the same active claim.
        let teardown_finalizer_lock = self
            .daemon
            .teardown_finalizer_lock(&stack_name)
            .map_err(|error| status_from_stack_error(error, &request_id))?;
        let _teardown_finalizer_guard = teardown_finalizer_lock.lock_owned().await;
        let workload_scope = validate_stack_cleanup_request_scope(
            self.daemon.as_ref(),
            request.scope.as_ref(),
            &stack_name,
            &request_id,
        )?;
        if request.dry_run {
            enforce_stack_policy_preflight_read_only(
                self.daemon.as_ref(),
                RuntimeOperation::RemoveContainer,
                &metadata,
                &request_id,
            )?;
        } else {
            enforce_mutation_policy_preflight(
                self.daemon.as_ref(),
                RuntimeOperation::RemoveContainer,
                &metadata,
                &request_id,
            )?;
        }

        let teardown_request_digest = teardown_request_digest(
            &stack_name,
            &workload_scope,
            &request_id,
            request.dry_run,
            request.remove_volumes,
        );
        let teardown_session_id = teardown_reconcile_session_id(&teardown_request_digest);
        let (existing_teardown_session, existing_receipt) = self
            .daemon
            .with_state_store(|store| {
                let exact = if request.dry_run {
                    None
                } else {
                    store.load_reconcile_session(&teardown_session_id)?
                };
                let receipt = store
                    .list_receipts_for_entity("stack", &stack_name)?
                    .into_iter()
                    .find(|receipt| {
                        receipt.operation == "teardown_stack" && receipt.request_id == request_id
                    });
                if receipt.is_some() {
                    return Ok((exact, receipt));
                }
                let active = store.load_active_reconcile_session(&stack_name)?;
                if let Some(active) = active
                    && exact
                        .as_ref()
                        .is_none_or(|session| session.session_id != active.session_id)
                {
                    return Err(StackError::Machine {
                        code: MachineErrorCode::StateConflict,
                        message: format!(
                            "stack `{stack_name}` has active reconcile operation `{}`; resume that exact operation before teardown",
                            active.operation_id
                        ),
                    });
                }
                if let Some(session) = &exact
                    && (session.stack_name != stack_name
                        || !vz_stack::matches_claimed_teardown_operation(
                            &session.operation_id,
                            &request_id,
                        ))
                {
                    return Err(StackError::Machine {
                        code: MachineErrorCode::StateConflict,
                        message: format!(
                            "teardown session `{teardown_session_id}` belongs to another stack or operation"
                        ),
                    });
                }
                Ok((exact, receipt))
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;
        if let Some(receipt) = existing_receipt {
            if receipt.entity_type != "stack"
                || receipt.entity_id != stack_name
                || receipt.status != "success"
                || receipt
                    .metadata
                    .get("request_digest")
                    .and_then(serde_json::Value::as_str)
                    != Some(teardown_request_digest.as_str())
            {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::StateConflict,
                    format!("request `{request_id}` is already bound to a different receipt"),
                    Some(request_id),
                    BTreeMap::new(),
                )));
            }
            let changed_actions = receipt
                .metadata
                .get("changed_actions")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| {
                    status_from_machine_error(MachineError::new(
                        MachineErrorCode::StateConflict,
                        "persisted teardown receipt has invalid changed_actions".to_string(),
                        Some(request_id.clone()),
                        BTreeMap::new(),
                    ))
                })?;
            let removed_volumes = receipt
                .metadata
                .get("removed_volumes")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| {
                    status_from_machine_error(MachineError::new(
                        MachineErrorCode::StateConflict,
                        "persisted teardown receipt has invalid removed_volumes".to_string(),
                        Some(request_id.clone()),
                        BTreeMap::new(),
                    ))
                })?;
            let event = teardown_stack_completion_event(
                &request_id,
                1,
                runtime_v2::TeardownStackResponse {
                    request_id: request_id.clone(),
                    stack_name,
                    changed_actions,
                    removed_volumes,
                },
                &receipt.receipt_id,
            );
            return Ok(stack_stream_response(
                vec![Ok(event)],
                Some(&receipt.receipt_id),
            ));
        }
        if let Some(session) = &existing_teardown_session
            && session.status != vz_stack::ReconcileSessionStatus::Active
        {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!(
                    "teardown reconcile session `{}` is terminal without a final receipt; refusing unfenced stack-wide effect replay",
                    session.session_id
                ),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let (desired, observed) = self
            .daemon
            .with_state_store(|store| {
                Ok((
                    store.load_desired_state(&stack_name)?,
                    store.load_observed_state(&stack_name)?,
                ))
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;
        if desired.is_none() && observed.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("stack not found: {stack_name}"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let mut sequence = 1u64;
        let mut events = vec![Ok(teardown_stack_progress_event(
            &request_id,
            sequence,
            "planning",
            "planning stack teardown actions",
        ))];

        let empty_spec = StackSpec {
            name: stack_name.clone(),
            services: Vec::new(),
            networks: Vec::new(),
            volumes: Vec::new(),
            secrets: Vec::new(),
            disk_size_mb: None,
        };
        let health_statuses = HashMap::new();
        let teardown_actions = match self.daemon.with_state_store(|store| {
            if let Some(session) = &existing_teardown_session {
                let actions = store.load_reconcile_session_actions(&session.session_id)?;
                if actions.is_empty()
                    || actions.iter().any(|action| {
                        !matches!(action, Action::ServiceRemove { .. })
                            || action.precondition().workload().stack_id != stack_name
                    })
                {
                    return Err(StackError::Machine {
                        code: MachineErrorCode::StateConflict,
                        message: format!(
                            "teardown session `{}` does not contain an exact remove-only action plan",
                            session.session_id
                        ),
                    });
                }
                Ok(actions)
            } else {
                Ok(plan_apply(&empty_spec, store, &health_statuses)?.actions)
            }
        }) {
            Ok(actions) => actions,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };

        if request.dry_run {
            sequence += 1;
            events.push(Ok(teardown_stack_completion_event(
                &request_id,
                sequence,
                runtime_v2::TeardownStackResponse {
                    request_id: request_id.clone(),
                    stack_name,
                    changed_actions: teardown_actions.len() as u32,
                    removed_volumes: 0,
                },
                "",
            )));
            return Ok(stack_stream_response(events, None));
        }

        if teardown_actions.is_empty() {
            events.push(Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                "non-dry teardown with no service actions requires a durable stack-level finalizer claim"
                    .to_string(),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))));
            return Ok(stack_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(teardown_stack_progress_event(
            &request_id,
            sequence,
            "executing",
            "executing stack teardown actions",
        )));
        let pending_teardown = {
            let exec_store = match self.daemon.open_dedicated_state_store() {
                Ok(store) => store,
                Err(error) => {
                    events.push(Err(status_from_stack_error(error, &request_id)));
                    return Ok(stack_stream_response(events, None));
                }
            };
            let runtime = DaemonContainerRuntime::new(self.daemon.clone());
            let stack_dir = stack_runtime_dir(self.daemon.as_ref(), &stack_name);
            let executor = match StackExecutor::new_scoped_for_cleanup(
                runtime,
                exec_store,
                &stack_dir,
                workload_scope.clone(),
            ) {
                Ok(executor) => executor,
                Err(error) => {
                    events.push(Err(status_from_stack_error(error, &request_id)));
                    return Ok(stack_stream_response(events, None));
                }
            };
            let mut executor = executor;
            let admission = match executor.begin_claimed_teardown_batch(
                &empty_spec,
                &teardown_actions,
                &teardown_session_id,
                &request_id,
                0,
            ) {
                Ok(result) => result,
                Err(error) => {
                    events.push(Err(status_from_stack_error(error, &request_id)));
                    return Ok(stack_stream_response(events, None));
                }
            };
            match admission {
                vz_stack::ClaimedTeardownAdmission::Ready(pending) => pending,
                vz_stack::ClaimedTeardownAdmission::Failed(execution_result) => {
                    events.push(Err(status_from_machine_error(MachineError::new(
                        MachineErrorCode::BackendUnavailable,
                        execution_result
                            .errors
                            .first()
                            .map(|(_, error)| error.clone())
                            .unwrap_or_else(|| {
                                "claimed stack teardown did not commit every exact remove outcome"
                                    .to_string()
                            }),
                        Some(request_id.clone()),
                        BTreeMap::new(),
                    ))));
                    return Ok(stack_stream_response(events, None));
                }
            }
        };

        if let Err(error) = self
            .daemon
            .with_state_store(|store| store.save_desired_state(&stack_name, &empty_spec))
        {
            events.push(Err(status_from_stack_error(error, &request_id)));
            return Ok(stack_stream_response(events, None));
        }

        sequence += 1;
        events.push(Ok(teardown_stack_progress_event(
            &request_id,
            sequence,
            "shutting_down_runtime",
            "shutting down stack runtime",
        )));
        if let Err(error) =
            shutdown_stack_runtime_for_teardown(self.daemon.clone(), stack_name.clone()).await
        {
            events.push(Err(status_from_stack_error(error, &request_id)));
            return Ok(stack_stream_response(events, None));
        }

        if request.remove_volumes {
            sequence += 1;
            events.push(Ok(teardown_stack_progress_event(
                &request_id,
                sequence,
                "removing_volumes",
                "removing stack volumes",
            )));
        }
        let removed_volumes = if request.remove_volumes {
            let stack_dir = stack_runtime_dir(self.daemon.as_ref(), &stack_name);
            let volume_manager = VolumeManager::new(&stack_dir);
            match volume_manager.remove_all() {
                Ok(count) => count,
                Err(error) => {
                    events.push(Err(status_from_stack_error(error, &request_id)));
                    return Ok(stack_stream_response(events, None));
                }
            }
        } else {
            0
        };

        let commit_store = match self.daemon.open_dedicated_state_store() {
            Ok(store) => store,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        let commit_runtime = DaemonContainerRuntime::new(self.daemon.clone());
        let stack_dir = stack_runtime_dir(self.daemon.as_ref(), &stack_name);
        let mut commit_executor = match StackExecutor::new_scoped_for_cleanup(
            commit_runtime,
            commit_store,
            &stack_dir,
            workload_scope,
        ) {
            Ok(executor) => executor,
            Err(error) => {
                events.push(Err(status_from_stack_error(error, &request_id)));
                return Ok(stack_stream_response(events, None));
            }
        };
        let execution_result =
            match commit_executor.commit_claimed_teardown_batch(*pending_teardown) {
                Ok(result) => result,
                Err(error) => {
                    events.push(Err(status_from_stack_error(error, &request_id)));
                    return Ok(stack_stream_response(events, None));
                }
            };
        if !execution_result.all_succeeded()
            || execution_result.outcomes.len() != teardown_actions.len()
        {
            events.push(Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                "claimed teardown commit did not preserve every successful remove outcome"
                    .to_string(),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))));
            return Ok(stack_stream_response(events, None));
        }

        let changed_actions = teardown_actions.len() as u32;
        let removed_volumes = removed_volumes as u32;
        sequence += 1;
        events.push(Ok(teardown_stack_progress_event(
            &request_id,
            sequence,
            "persisting",
            "persisting stack teardown receipt",
        )));
        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        let persist_result = self
            .daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.emit_event(
                        &stack_name,
                        &StackEvent::StackDestroyed {
                            stack_name: stack_name.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "teardown_stack".to_string(),
                        entity_id: stack_name.clone(),
                        entity_type: "stack".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_stack_teardown_metadata(
                            &teardown_request_digest,
                            changed_actions,
                            removed_volumes,
                        )?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id));
        if let Err(status) = persist_result {
            events.push(Err(status));
            return Ok(stack_stream_response(events, None));
        }
        sequence += 1;
        events.push(Ok(teardown_stack_completion_event(
            &request_id,
            sequence,
            runtime_v2::TeardownStackResponse {
                request_id: request_id.clone(),
                stack_name,
                changed_actions,
                removed_volumes,
            },
            receipt_id.as_str(),
        )));
        Ok(stack_stream_response(events, Some(receipt_id.as_str())))
    }

    async fn get_stack_status(
        &self,
        request: Request<runtime_v2::GetStackStatusRequest>,
    ) -> Result<Response<runtime_v2::GetStackStatusResponse>, Status> {
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
        validate_stack_request_scope(
            self.daemon.as_ref(),
            request.scope.as_ref(),
            &stack_name,
            false,
            &request_id,
        )?;

        let (desired, observed) = self
            .daemon
            .with_state_store(|store| {
                Ok((
                    store.load_desired_state(&stack_name)?,
                    store.load_observed_state(&stack_name)?,
                ))
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;
        if desired.is_none() && observed.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("stack not found: {stack_name}"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        Ok(Response::new(runtime_v2::GetStackStatusResponse {
            request_id,
            stack_name,
            services: observed.iter().map(stack_status_from_observed).collect(),
        }))
    }

    async fn list_stack_events(
        &self,
        request: Request<runtime_v2::ListStackEventsRequest>,
    ) -> Result<Response<runtime_v2::ListStackEventsResponse>, Status> {
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
        validate_stack_request_scope(
            self.daemon.as_ref(),
            request.scope.as_ref(),
            &stack_name,
            false,
            &request_id,
        )?;
        let limit = if request.limit == 0 {
            100
        } else {
            request.limit as usize
        }
        .clamp(1, 1000);
        let after = request.after.max(0);

        let records = self
            .daemon
            .with_state_store(|store| store.load_events_since_limited(&stack_name, after, limit))
            .map_err(|error| status_from_stack_error(error, &request_id))?;
        let events: Vec<runtime_v2::RuntimeEvent> = records
            .iter()
            .map(event_record_to_runtime_event)
            .collect::<Result<_, _>>()?;
        let next_cursor = records.last().map(|record| record.id).unwrap_or(after);

        Ok(Response::new(runtime_v2::ListStackEventsResponse {
            request_id,
            events,
            next_cursor,
        }))
    }

    async fn get_stack_logs(
        &self,
        request: Request<runtime_v2::GetStackLogsRequest>,
    ) -> Result<Response<runtime_v2::GetStackLogsResponse>, Status> {
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
        validate_stack_request_scope(
            self.daemon.as_ref(),
            request.scope.as_ref(),
            &stack_name,
            false,
            &request_id,
        )?;
        let service_filter = request.service.trim().to_string();
        let tail = request.tail as usize;

        let observed = self
            .daemon
            .with_state_store(|store| store.load_observed_state(&stack_name))
            .map_err(|error| status_from_stack_error(error, &request_id))?;
        let targets: Vec<&ServiceObservedState> = if service_filter.is_empty() {
            observed
                .iter()
                .filter(|entry| entry.phase == ServicePhase::Running)
                .collect()
        } else {
            observed
                .iter()
                .filter(|entry| entry.replica.service_name == service_filter)
                .collect()
        };
        if targets.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                if service_filter.is_empty() {
                    format!("no running services for stack: {stack_name}")
                } else {
                    format!("service not found in stack {stack_name}: {service_filter}")
                },
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let runtime = DaemonContainerRuntime::new(self.daemon.clone());
        let mut logs = Vec::with_capacity(targets.len());
        for entry in targets {
            let Some(container_id) = entry.container_id.as_deref() else {
                continue;
            };
            let output = runtime
                .logs(container_id)
                .map(|logs| logs.output)
                .map_err(|error| {
                    status_from_stack_error(
                        StackError::Network(format!(
                            "failed to load logs for service {}: {error}",
                            entry.replica.display_name()
                        )),
                        &request_id,
                    )
                })?;
            logs.push(runtime_v2::StackServiceLog {
                service_name: entry.replica.display_name(),
                output: tail_output(&output, tail),
            });
        }

        Ok(Response::new(runtime_v2::GetStackLogsResponse {
            request_id,
            stack_name,
            logs,
        }))
    }

    async fn stop_stack_service(
        &self,
        request: Request<runtime_v2::StackServiceActionRequest>,
    ) -> Result<Response<Self::StopStackServiceStream>, Status> {
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
        let mut sequence = 1u64;
        let mut events = vec![Ok(stack_service_action_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating stack service stop request",
        ))];
        let service_name = request.service_name.trim().to_string();
        if service_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "service_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let workload_scope = validate_stack_cleanup_request_scope(
            self.daemon.as_ref(),
            request.scope.as_ref(),
            &stack_name,
            &request_id,
        )?;
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::StopContainer,
            &metadata,
            &request_id,
        )?;

        let (spec, observed_state) = match load_stack_service_action_context(
            self.daemon.as_ref(),
            &stack_name,
            &service_name,
            &request_id,
        ) {
            Ok(value) => value,
            Err(status) => {
                events.push(Err(status));
                return Ok(stack_stream_response(events, None));
            }
        };
        if observed_state.phase != ServicePhase::Stopped || observed_state.container_id.is_some() {
            sequence += 1;
            events.push(Ok(stack_service_action_progress_event(
                &request_id,
                sequence,
                "executing",
                "stopping stack service runtime",
            )));
            if let Err(status) = execute_stack_service_action(
                self.daemon.clone(),
                &spec,
                TargetedActionKind::Remove,
                ServiceReplicaKey::first(service_name.clone())
                    .map_err(|error| status_from_stack_error(error, &request_id))?,
                workload_scope.clone(),
                &request_id,
                MachineErrorCode::StateConflict,
            ) {
                events.push(Err(status));
                return Ok(stack_stream_response(events, None));
            }
        }

        let service_state = match self
            .daemon
            .with_state_store(|store| {
                Ok(store
                    .load_observed_state(&stack_name)?
                    .into_iter()
                    .find(|service| {
                        service.replica.service_name == service_name && service.replica.index() == 1
                    })
                    .unwrap_or_else(|| default_stopped_service(&service_name)))
            })
            .map_err(|error| status_from_stack_error(error, &request_id))
        {
            Ok(value) => value,
            Err(status) => {
                events.push(Err(status));
                return Ok(stack_stream_response(events, None));
            }
        };

        sequence += 1;
        events.push(Ok(stack_service_action_progress_event(
            &request_id,
            sequence,
            "persisting",
            "persisting stop service receipt",
        )));
        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        let persist_result = self
            .daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "stop_stack_service".to_string(),
                        entity_id: format!("{stack_name}:{service_name}"),
                        entity_type: "stack_service".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("stack_service_stopped")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id));
        if let Err(status) = persist_result {
            events.push(Err(status));
            return Ok(stack_stream_response(events, None));
        }
        sequence += 1;
        events.push(Ok(stack_service_action_completion_event(
            &request_id,
            sequence,
            stack_service_action_response(request_id.clone(), stack_name, service_state),
            receipt_id.as_str(),
        )));
        Ok(stack_stream_response(events, Some(receipt_id.as_str())))
    }

    async fn start_stack_service(
        &self,
        request: Request<runtime_v2::StackServiceActionRequest>,
    ) -> Result<Response<Self::StartStackServiceStream>, Status> {
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
        let mut sequence = 1u64;
        let mut events = vec![Ok(stack_service_action_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating stack service start request",
        ))];
        let service_name = request.service_name.trim().to_string();
        if service_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "service_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let workload_scope = validate_stack_request_scope(
            self.daemon.as_ref(),
            request.scope.as_ref(),
            &stack_name,
            true,
            &request_id,
        )?;
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateContainer,
            &metadata,
            &request_id,
        )?;

        let (spec, observed_state) = match load_stack_service_action_context(
            self.daemon.as_ref(),
            &stack_name,
            &service_name,
            &request_id,
        ) {
            Ok(value) => value,
            Err(status) => {
                events.push(Err(status));
                return Ok(stack_stream_response(events, None));
            }
        };
        if !(observed_state.phase == ServicePhase::Running && observed_state.container_id.is_some())
        {
            sequence += 1;
            events.push(Ok(stack_service_action_progress_event(
                &request_id,
                sequence,
                "executing",
                "starting stack service runtime",
            )));
            if let Err(status) = execute_stack_service_action(
                self.daemon.clone(),
                &spec,
                TargetedActionKind::Create,
                ServiceReplicaKey::first(service_name.clone())
                    .map_err(|error| status_from_stack_error(error, &request_id))?,
                workload_scope.clone(),
                &request_id,
                MachineErrorCode::InternalError,
            ) {
                events.push(Err(status));
                return Ok(stack_stream_response(events, None));
            }
        }

        let service_state = match self
            .daemon
            .with_state_store(|store| {
                Ok(store
                    .load_observed_state(&stack_name)?
                    .into_iter()
                    .find(|service| {
                        service.replica.service_name == service_name && service.replica.index() == 1
                    })
                    .unwrap_or_else(|| default_stopped_service(&service_name)))
            })
            .map_err(|error| status_from_stack_error(error, &request_id))
        {
            Ok(value) => value,
            Err(status) => {
                events.push(Err(status));
                return Ok(stack_stream_response(events, None));
            }
        };

        sequence += 1;
        events.push(Ok(stack_service_action_progress_event(
            &request_id,
            sequence,
            "persisting",
            "persisting start service receipt",
        )));
        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        let persist_result = self
            .daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "start_stack_service".to_string(),
                        entity_id: format!("{stack_name}:{service_name}"),
                        entity_type: "stack_service".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("stack_service_started")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id));
        if let Err(status) = persist_result {
            events.push(Err(status));
            return Ok(stack_stream_response(events, None));
        }
        sequence += 1;
        events.push(Ok(stack_service_action_completion_event(
            &request_id,
            sequence,
            stack_service_action_response(request_id.clone(), stack_name, service_state),
            receipt_id.as_str(),
        )));
        Ok(stack_stream_response(events, Some(receipt_id.as_str())))
    }

    async fn restart_stack_service(
        &self,
        request: Request<runtime_v2::StackServiceActionRequest>,
    ) -> Result<Response<Self::RestartStackServiceStream>, Status> {
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
        let mut sequence = 1u64;
        let mut events = vec![Ok(stack_service_action_progress_event(
            &request_id,
            sequence,
            "validating",
            "validating stack service restart request",
        ))];
        let service_name = request.service_name.trim().to_string();
        if service_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "service_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let workload_scope = validate_stack_request_scope(
            self.daemon.as_ref(),
            request.scope.as_ref(),
            &stack_name,
            true,
            &request_id,
        )?;
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateContainer,
            &metadata,
            &request_id,
        )?;

        let (spec, _observed_state) = match load_stack_service_action_context(
            self.daemon.as_ref(),
            &stack_name,
            &service_name,
            &request_id,
        ) {
            Ok(value) => value,
            Err(status) => {
                events.push(Err(status));
                return Ok(stack_stream_response(events, None));
            }
        };
        sequence += 1;
        events.push(Ok(stack_service_action_progress_event(
            &request_id,
            sequence,
            "executing",
            "restarting stack service runtime",
        )));
        if let Err(status) = execute_stack_service_action(
            self.daemon.clone(),
            &spec,
            TargetedActionKind::Recreate,
            ServiceReplicaKey::first(service_name.clone())
                .map_err(|error| status_from_stack_error(error, &request_id))?,
            workload_scope,
            &request_id,
            MachineErrorCode::InternalError,
        ) {
            events.push(Err(status));
            return Ok(stack_stream_response(events, None));
        }

        let service_state = match self
            .daemon
            .with_state_store(|store| {
                Ok(store
                    .load_observed_state(&stack_name)?
                    .into_iter()
                    .find(|service| {
                        service.replica.service_name == service_name && service.replica.index() == 1
                    })
                    .unwrap_or_else(|| default_stopped_service(&service_name)))
            })
            .map_err(|error| status_from_stack_error(error, &request_id))
        {
            Ok(value) => value,
            Err(status) => {
                events.push(Err(status));
                return Ok(stack_stream_response(events, None));
            }
        };

        sequence += 1;
        events.push(Ok(stack_service_action_progress_event(
            &request_id,
            sequence,
            "persisting",
            "persisting restart service receipt",
        )));
        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        let persist_result = self
            .daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "restart_stack_service".to_string(),
                        entity_id: format!("{stack_name}:{service_name}"),
                        entity_type: "stack_service".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("stack_service_restarted")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id));
        if let Err(status) = persist_result {
            events.push(Err(status));
            return Ok(stack_stream_response(events, None));
        }
        sequence += 1;
        events.push(Ok(stack_service_action_completion_event(
            &request_id,
            sequence,
            stack_service_action_response(request_id.clone(), stack_name, service_state),
            receipt_id.as_str(),
        )));
        Ok(stack_stream_response(events, Some(receipt_id.as_str())))
    }

    async fn create_stack_run_container(
        &self,
        request: Request<runtime_v2::StackRunContainerRequest>,
    ) -> Result<Response<runtime_v2::StackRunContainerResponse>, Status> {
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
        let service_name = request.service_name.trim().to_string();
        if service_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "service_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let run_service_name = if request.run_service_name.trim().is_empty() {
            generated_stack_run_service_name(&service_name)
        } else {
            request.run_service_name.trim().to_string()
        };
        reject_primary_service_run_alias(&service_name, &run_service_name, &request_id)?;
        let workload_scope = validate_stack_request_scope(
            self.daemon.as_ref(),
            request.scope.as_ref(),
            &stack_name,
            true,
            &request_id,
        )?;
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateContainer,
            &metadata,
            &request_id,
        )?;
        let (spec, _) = load_stack_service_action_context(
            self.daemon.as_ref(),
            &stack_name,
            &service_name,
            &request_id,
        )?;

        let run_service_state = load_observed_stack_service(
            self.daemon.as_ref(),
            &stack_name,
            &run_service_name,
            &request_id,
        )?;
        if !(run_service_state.phase == ServicePhase::Running
            && run_service_state.container_id.is_some())
        {
            let run_spec = clone_stack_spec_with_run_service(
                &spec,
                &service_name,
                &run_service_name,
                &request_id,
            )?;
            execute_stack_service_action(
                self.daemon.clone(),
                &run_spec,
                TargetedActionKind::Create,
                ServiceReplicaKey::first(run_service_name.clone())
                    .map_err(|error| status_from_stack_error(error, &request_id))?,
                workload_scope.clone(),
                &request_id,
                MachineErrorCode::InternalError,
            )?;
        }

        let run_service_state = load_observed_stack_service(
            self.daemon.as_ref(),
            &stack_name,
            &run_service_name,
            &request_id,
        )?;
        let container_id = run_service_state.container_id.clone().ok_or_else(|| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!(
                    "run service `{run_service_name}` in stack `{stack_name}` has no running container"
                ),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "create_stack_run_container".to_string(),
                        entity_id: format!("{stack_name}:{run_service_name}"),
                        entity_type: "stack_run_container".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("stack_run_container_created")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(stack_run_container_response(
            request_id,
            stack_name,
            service_name,
            run_service_name,
            container_id,
        ));
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn remove_stack_run_container(
        &self,
        request: Request<runtime_v2::StackRunContainerRequest>,
    ) -> Result<Response<runtime_v2::StackRunContainerResponse>, Status> {
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
        let service_name = request.service_name.trim().to_string();
        if service_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "service_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let run_service_name = request.run_service_name.trim().to_string();
        if run_service_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "run_service_name cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        reject_primary_service_run_alias(&service_name, &run_service_name, &request_id)?;
        let workload_scope = validate_stack_cleanup_request_scope(
            self.daemon.as_ref(),
            request.scope.as_ref(),
            &stack_name,
            &request_id,
        )?;
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::StopContainer,
            &metadata,
            &request_id,
        )?;

        let (spec, _) = load_stack_service_action_context(
            self.daemon.as_ref(),
            &stack_name,
            &service_name,
            &request_id,
        )?;
        let run_service_state_before = load_observed_stack_service(
            self.daemon.as_ref(),
            &stack_name,
            &run_service_name,
            &request_id,
        )?;
        let container_id = run_service_state_before
            .container_id
            .clone()
            .unwrap_or_default();

        if run_service_state_before.phase != ServicePhase::Stopped
            || run_service_state_before.container_id.is_some()
        {
            let run_spec = clone_stack_spec_with_run_service(
                &spec,
                &service_name,
                &run_service_name,
                &request_id,
            )?;
            execute_stack_service_action(
                self.daemon.clone(),
                &run_spec,
                TargetedActionKind::Remove,
                ServiceReplicaKey::first(run_service_name.clone())
                    .map_err(|error| status_from_stack_error(error, &request_id))?,
                workload_scope.clone(),
                &request_id,
                MachineErrorCode::StateConflict,
            )?;
        }

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "remove_stack_run_container".to_string(),
                        entity_id: format!("{stack_name}:{run_service_name}"),
                        entity_type: "stack_run_container".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("stack_run_container_removed")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let mut response = Response::new(stack_run_container_response(
            request_id,
            stack_name,
            service_name,
            run_service_name,
            container_id,
        ));
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimedConfig;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use tokio_stream::StreamExt;
    use vz_runtime_contract::{
        Build, PolicyDecision, RequestMetadata, RuntimeError, RuntimeOperation, RuntimePolicyHook,
    };

    struct AllowThenDenyCreatePolicyHook {
        create_evaluations: std::sync::atomic::AtomicUsize,
    }

    impl RuntimePolicyHook for AllowThenDenyCreatePolicyHook {
        fn evaluate(
            &self,
            operation: RuntimeOperation,
            _metadata: &RequestMetadata,
        ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
            if operation != RuntimeOperation::CreateContainer {
                return Ok(PolicyDecision::Allow);
            }
            let evaluation = self
                .create_evaluations
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if evaluation == 0 {
                Ok(PolicyDecision::Allow)
            } else {
                Ok(PolicyDecision::Deny {
                    reason: "blocked after read-only apply preflight".to_string(),
                })
            }
        }
    }

    #[tokio::test]
    async fn apply_policy_denial_cannot_claim_immutable_stack_owner() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let config = RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };
        let daemon = Arc::new(
            RuntimeDaemon::start_with_policy_hook(
                config.clone(),
                Arc::new(AllowThenDenyCreatePolicyHook {
                    create_evaluations: std::sync::atomic::AtomicUsize::new(0),
                }),
                Some("deny-second-create-v1".to_string()),
            )
            .expect("daemon start"),
        );
        let stack_name = "policy-denied-owner";
        let wire_scope = crate::grpc::tests::seed_stack_topology(daemon.as_ref(), stack_name);
        let service = StackServiceImpl::new(daemon.clone());

        let error = runtime_v2::stack_service_server::StackService::apply_stack(
            &service,
            tonic::Request::new(runtime_v2::ApplyStackRequest {
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: "req-policy-denied-owner".to_string(),
                    idempotency_key: String::new(),
                    trace_id: String::new(),
                }),
                stack_name: stack_name.to_string(),
                compose_yaml: "services:\n  web:\n    image: alpine:latest\n".to_string(),
                compose_dir: ".".to_string(),
                dry_run: false,
                detach: true,
                scope: Some(wire_scope),
            }),
        )
        .await
        .expect_err("the audited mutation policy must deny apply");

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        let (owner, desired, observed, stack_events, stack_receipts, policy_receipts) = daemon
            .with_state_store(|store| {
                Ok((
                    store.load_stack_workload_owner(stack_name)?,
                    store.load_desired_state(stack_name)?,
                    store.load_observed_state(stack_name)?,
                    store.load_events(stack_name)?,
                    store.list_receipts_for_entity("stack", stack_name)?,
                    store.list_receipts_for_entity("policy", "req-policy-denied-owner")?,
                ))
            })
            .expect("inspect denied apply state");
        assert!(
            owner.is_none(),
            "policy denial must not claim the owner tombstone"
        );
        assert!(
            desired.is_none(),
            "policy denial must not persist desired state"
        );
        assert!(
            observed.is_empty(),
            "policy denial must not persist observed state"
        );
        assert!(
            stack_events.is_empty(),
            "policy denial must not emit stack events"
        );
        assert!(
            stack_receipts.is_empty(),
            "policy denial must not persist stack receipts"
        );
        assert_eq!(
            policy_receipts.len(),
            1,
            "the denial itself remains audited"
        );
        assert_eq!(policy_receipts[0].status, "deny");
        assert!(
            !config
                .runtime_data_dir
                .join("stacks")
                .join(stack_name)
                .exists(),
            "policy denial must happen before stack filesystem mutation"
        );
    }

    #[tokio::test]
    async fn teardown_execution_failure_returns_only_a_terminal_error() {
        let result = vz_stack::ExecutionResult {
            succeeded: 1,
            failed: 2,
            errors: vec![
                ("api".to_string(), "stop failed".to_string()),
                ("worker".to_string(), "remove failed".to_string()),
            ],
            skipped_mounts: Vec::new(),
            outcomes: Vec::new(),
        };
        let mut events = vec![Ok(teardown_stack_progress_event(
            "req-teardown-failure",
            1,
            "executing",
            "executing stack teardown actions",
        ))];

        let Some(response) =
            teardown_execution_failure_response(&result, "req-teardown-failure", &mut events)
        else {
            panic!("failed teardown actions must terminate the stream");
        };
        assert!(response.metadata().get("x-receipt-id").is_none());

        let mut stream = response.into_inner();
        let first = stream
            .next()
            .await
            .unwrap_or_else(|| panic!("expected executing progress event"))
            .unwrap_or_else(|error| panic!("expected progress before failure: {error}"));
        assert!(matches!(
            first.payload,
            Some(runtime_v2::teardown_stack_event::Payload::Progress(_))
        ));
        let error = match stream.next().await {
            Some(Err(error)) => error,
            Some(Ok(event)) => panic!("expected terminal error, got event: {event:?}"),
            None => panic!("expected terminal teardown stream error"),
        };

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(error.message().contains("backend_unavailable"));
        assert!(error.message().contains("failed for 2 action(s)"));
        assert!(error.message().contains("api: stop failed"));
        assert!(error.message().contains("worker: remove failed"));
        assert!(error.message().contains("request_id=req-teardown-failure"));
        assert!(
            stream.next().await.is_none(),
            "error must terminate the stream"
        );
    }

    #[test]
    fn teardown_execution_success_allows_completion_phases() {
        let mut events = Vec::new();
        assert!(
            teardown_execution_failure_response(
                &vz_stack::ExecutionResult::default(),
                "req-teardown-success",
                &mut events,
            )
            .is_none(),
            "successful execution should continue teardown"
        );
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn zero_action_teardown_refuses_unfenced_runtime_and_volume_mutation() {
        let (tmp, daemon) = stack_test_daemon();
        let stack_name = "shutdown-failure";
        let wire_scope = seed_owned_stack_topology(daemon.as_ref(), stack_name);
        let empty_spec = StackSpec {
            name: stack_name.to_string(),
            services: Vec::new(),
            networks: Vec::new(),
            volumes: Vec::new(),
            secrets: Vec::new(),
            disk_size_mb: None,
        };
        daemon
            .with_state_store(|store| store.save_desired_state(stack_name, &empty_spec))
            .unwrap_or_else(|error| panic!("persist desired stack: {error}"));
        daemon
            .manager()
            .ensure_stack_runtime(stack_name, Vec::new(), Default::default())
            .await
            .unwrap_or_else(|error| panic!("boot test stack runtime: {error}"));
        let volume_path = tmp
            .path()
            .join("runtime")
            .join("stacks")
            .join(stack_name)
            .join("volumes")
            .join("data");
        std::fs::create_dir_all(&volume_path)
            .unwrap_or_else(|error| panic!("create test volume: {error}"));

        let service = StackServiceImpl::new(daemon.clone());
        let response = runtime_v2::stack_service_server::StackService::teardown_stack(
            &service,
            tonic::Request::new(runtime_v2::TeardownStackRequest {
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: "req-shutdown-failure".to_string(),
                    idempotency_key: String::new(),
                    trace_id: String::new(),
                }),
                stack_name: stack_name.to_string(),
                remove_volumes: true,
                dry_run: false,
                scope: Some(wire_scope),
            }),
        )
        .await
        .unwrap_or_else(|error| panic!("teardown stream should start: {error}"));
        assert!(response.metadata().get("x-receipt-id").is_none());

        let mut stream = response.into_inner();
        let mut phases = Vec::new();
        let mut completion_seen = false;
        let mut terminal_error = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(event) => match event.payload {
                    Some(runtime_v2::teardown_stack_event::Payload::Progress(progress)) => {
                        phases.push(progress.phase);
                    }
                    Some(runtime_v2::teardown_stack_event::Payload::Completion(_)) => {
                        completion_seen = true;
                    }
                    None => {}
                },
                Err(error) => terminal_error = Some(error),
            }
        }

        let error = terminal_error
            .unwrap_or_else(|| panic!("unfenced zero-action teardown must terminate the stream"));
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("stack-level finalizer claim"));
        assert!(error.message().contains("req-shutdown-failure"));
        assert!(!phases.iter().any(|phase| phase == "shutting_down_runtime"));
        assert!(!phases.iter().any(|phase| phase == "removing_volumes"));
        assert!(!phases.iter().any(|phase| phase == "persisting"));
        assert!(!completion_seen);
        assert!(volume_path.exists(), "volume deletion must not run");
        assert!(
            daemon.manager().has_stack_runtime(stack_name),
            "unfenced teardown must leave the runtime active"
        );

        let (receipts, stack_events) = daemon
            .with_state_store(|store| {
                Ok((
                    store.list_receipts_for_entity("stack", stack_name)?,
                    store.load_events(stack_name)?,
                ))
            })
            .unwrap_or_else(|error| panic!("inspect teardown persistence: {error}"));
        assert!(
            receipts
                .iter()
                .all(|receipt| receipt.operation != "teardown_stack")
        );
        assert!(
            stack_events
                .iter()
                .all(|event| !matches!(event, StackEvent::StackDestroyed { .. }))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn teardown_finalizer_failure_leaves_active_claim_for_exact_retry() {
        let (tmp, daemon) = stack_test_daemon();
        let stack_name = "shutdown-retry";
        let wire_scope = seed_owned_stack_topology(daemon.as_ref(), stack_name);
        let service = StackServiceImpl::new(daemon.clone());
        seed_running_stack_service(daemon.clone(), stack_name, &wire_scope).await;

        let volume_path = tmp
            .path()
            .join("runtime")
            .join("stacks")
            .join(stack_name)
            .join("volumes")
            .join("data");
        std::fs::create_dir_all(&volume_path).unwrap();
        daemon.manager().backend().fail_next_shared_vm_shutdown();
        let request = || runtime_v2::TeardownStackRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-shutdown-retry".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            stack_name: stack_name.to_string(),
            remove_volumes: true,
            dry_run: false,
            scope: Some(wire_scope.clone()),
        };

        let first = runtime_v2::stack_service_server::StackService::teardown_stack(
            &service,
            tonic::Request::new(request()),
        )
        .await
        .expect("first teardown stream");
        let mut first_stream = first.into_inner();
        let mut first_error = None;
        while let Some(item) = first_stream.next().await {
            if let Err(error) = item {
                first_error = Some(error);
            }
        }
        let first_error = first_error.expect("injected shutdown failure");
        assert_eq!(
            first_error.code(),
            tonic::Code::Unavailable,
            "unexpected teardown failure: {first_error}"
        );
        assert!(volume_path.exists());
        let active = daemon
            .with_state_store(|store| store.load_active_reconcile_session(stack_name))
            .unwrap()
            .expect("teardown claims remain active");
        assert!(
            vz_stack::matches_claimed_teardown_operation(
                &active.operation_id,
                "req-shutdown-retry"
            ),
            "durable teardown operation must remain correlated to its request"
        );
        let changed_option_error = runtime_v2::stack_service_server::StackService::teardown_stack(
            &service,
            tonic::Request::new(runtime_v2::TeardownStackRequest {
                remove_volumes: false,
                ..request()
            }),
        )
        .await
        .expect_err("changed finalizer option must not reuse active teardown claims");
        assert_eq!(changed_option_error.code(), tonic::Code::FailedPrecondition);
        assert!(volume_path.exists());

        let retry = runtime_v2::stack_service_server::StackService::teardown_stack(
            &service,
            tonic::Request::new(request()),
        )
        .await
        .expect("exact teardown retry stream");
        let mut retry_stream = retry.into_inner();
        let mut completion = false;
        while let Some(item) = retry_stream.next().await {
            let event = item.expect("exact retry must succeed");
            completion |= matches!(
                event.payload,
                Some(runtime_v2::teardown_stack_event::Payload::Completion(_))
            );
        }
        assert!(completion);
        assert!(!volume_path.exists());
        assert!(!daemon.manager().has_stack_runtime(stack_name));
        assert!(
            daemon
                .with_state_store(|store| store.load_active_reconcile_session(stack_name))
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_identical_teardowns_serialize_the_finalizer_and_replay_receipt() {
        let (_tmp, daemon) = stack_test_daemon();
        let stack_name = "concurrent-shutdown";
        let wire_scope = seed_owned_stack_topology(daemon.as_ref(), stack_name);
        seed_running_stack_service(daemon.clone(), stack_name, &wire_scope).await;

        let held_finalizer = daemon
            .teardown_finalizer_lock(stack_name)
            .expect("stack finalizer lock")
            .lock_owned()
            .await;
        let baseline_shutdowns = daemon.manager().backend().shared_vm_shutdown_count();
        let spawn_request = |daemon: Arc<RuntimeDaemon>, scope| {
            tokio::spawn(async move {
                let service = StackServiceImpl::new(daemon);
                let response = runtime_v2::stack_service_server::StackService::teardown_stack(
                    &service,
                    tonic::Request::new(runtime_v2::TeardownStackRequest {
                        metadata: Some(runtime_v2::RequestMetadata {
                            request_id: "req-concurrent-shutdown".to_string(),
                            idempotency_key: String::new(),
                            trace_id: String::new(),
                        }),
                        stack_name: "concurrent-shutdown".to_string(),
                        remove_volumes: false,
                        dry_run: false,
                        scope: Some(scope),
                    }),
                )
                .await
                .expect("teardown stream");
                let receipt_id = response
                    .metadata()
                    .get("x-receipt-id")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                let mut stream = response.into_inner();
                let mut completed = false;
                while let Some(item) = stream.next().await {
                    let event = item.expect("teardown event");
                    completed |= matches!(
                        event.payload,
                        Some(runtime_v2::teardown_stack_event::Payload::Completion(_))
                    );
                }
                (receipt_id, completed)
            })
        };
        let first = spawn_request(daemon.clone(), wire_scope.clone());
        let second = spawn_request(daemon.clone(), wire_scope);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !first.is_finished() && !second.is_finished(),
            "both requests must wait behind the shared finalizer lock"
        );
        drop(held_finalizer);

        let (first_receipt, first_completed) = first.await.expect("first teardown task");
        let (second_receipt, second_completed) = second.await.expect("second teardown task");
        assert!(first_completed && second_completed);
        assert_eq!(first_receipt, second_receipt);
        assert!(first_receipt.is_some());
        assert_eq!(
            daemon.manager().backend().shared_vm_shutdown_count(),
            baseline_shutdowns + 1,
            "the duplicate request must replay the receipt without rerunning broad shutdown"
        );
        let receipts = daemon
            .with_state_store(|store| store.list_receipts_for_entity("stack", stack_name))
            .expect("list teardown receipts");
        assert_eq!(
            receipts
                .iter()
                .filter(|receipt| receipt.operation == "teardown_stack")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn teardown_rejects_interrupted_apply_without_touching_its_claim() {
        let (tmp, daemon) = stack_test_daemon();
        let stack_name = "interrupted-apply";
        let wire_scope = seed_owned_stack_topology(daemon.as_ref(), stack_name);
        let spec = parse_stack_spec(
            stack_name,
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .expect("stack spec");
        let session_id = "rs-interrupted-apply";
        let operation_id = "req-original-apply";
        let actions = daemon
            .with_state_store(|store| {
                store.save_desired_state(stack_name, &spec)?;
                let actions = plan_apply(&spec, store, &HashMap::new())?.actions;
                let now = current_unix_secs();
                let session = vz_stack::ReconcileSession {
                    session_id: session_id.to_string(),
                    stack_name: stack_name.to_string(),
                    operation_id: operation_id.to_string(),
                    status: vz_stack::ReconcileSessionStatus::Active,
                    actions_hash: vz_stack::compute_actions_hash(&actions),
                    next_action_index: 0,
                    total_actions: actions.len(),
                    started_at: now,
                    updated_at: now,
                    completed_at: None,
                };
                store.create_reconcile_batch(&session, &actions)?;
                let claims = store.start_reconcile_batch(
                    session_id,
                    stack_name,
                    operation_id,
                    0,
                    &actions,
                )?;
                assert_eq!(claims.len(), 1);
                Ok(actions)
            })
            .expect("seed interrupted apply claim");
        let stack_dir = tmp.path().join("runtime").join("stacks").join(stack_name);
        assert!(!stack_dir.exists());

        let service = StackServiceImpl::new(daemon.clone());
        let error = runtime_v2::stack_service_server::StackService::teardown_stack(
            &service,
            tonic::Request::new(runtime_v2::TeardownStackRequest {
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: "req-teardown".to_string(),
                    idempotency_key: String::new(),
                    trace_id: String::new(),
                }),
                stack_name: stack_name.to_string(),
                remove_volumes: true,
                dry_run: false,
                scope: Some(wire_scope),
            }),
        )
        .await
        .expect_err("teardown must not reinterpret an interrupted apply claim");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains(operation_id));
        let replayed = daemon
            .with_state_store(|store| {
                let active = store
                    .load_active_reconcile_session(stack_name)?
                    .expect("original active session");
                assert_eq!(active.session_id, session_id);
                assert_eq!(active.operation_id, operation_id);
                assert_eq!(active.next_action_index, 0);
                assert_eq!(store.load_reconcile_session_actions(session_id)?, actions);
                assert_eq!(store.load_desired_state(stack_name)?, Some(spec.clone()));
                assert!(store.load_observed_state(stack_name)?.is_empty());
                store.start_reconcile_batch(session_id, stack_name, operation_id, 0, &actions)
            })
            .expect("original claim remains exactly replayable");
        assert_eq!(replayed.len(), 1);
        assert!(!stack_dir.exists());
    }

    #[tokio::test]
    async fn teardown_receipt_replay_is_read_only_even_with_newer_active_apply() {
        let (_tmp, daemon) = stack_test_daemon();
        let stack_name = "shutdown-success";
        let wire_scope = seed_owned_stack_topology(daemon.as_ref(), stack_name);
        let workload_scope =
            vz_runtime_translate::machine_workload_scope_from_proto(&wire_scope).unwrap();
        let spec = parse_stack_spec(
            stack_name,
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .unwrap();
        let request_id = "req-shutdown-success";
        let request_digest =
            teardown_request_digest(stack_name, &workload_scope, request_id, false, false);
        daemon
            .with_state_store(|store| {
                store.save_desired_state(stack_name, &spec)?;
                let actions = plan_apply(&spec, store, &HashMap::new())?.actions;
                let session = vz_stack::ReconcileSession {
                    session_id: "rs-newer-apply".to_string(),
                    stack_name: stack_name.to_string(),
                    operation_id: "req-newer-apply".to_string(),
                    status: vz_stack::ReconcileSessionStatus::Active,
                    actions_hash: vz_stack::compute_actions_hash(&actions),
                    next_action_index: 0,
                    total_actions: actions.len(),
                    started_at: 10,
                    updated_at: 10,
                    completed_at: None,
                };
                store.create_reconcile_batch(&session, &actions)?;
                store.save_receipt(&Receipt {
                    receipt_id: "receipt-shutdown-success".to_string(),
                    operation: "teardown_stack".to_string(),
                    entity_id: stack_name.to_string(),
                    entity_type: "stack".to_string(),
                    request_id: request_id.to_string(),
                    status: "success".to_string(),
                    created_at: 9,
                    metadata: receipt_stack_teardown_metadata(&request_digest, 2, 0)?,
                })
            })
            .unwrap_or_else(|error| panic!("persist desired stack: {error}"));
        daemon
            .manager()
            .ensure_stack_runtime(stack_name, Vec::new(), Default::default())
            .await
            .unwrap_or_else(|error| panic!("boot test stack runtime: {error}"));

        let service = StackServiceImpl::new(daemon.clone());
        for (dry_run, remove_volumes) in [(false, true), (true, false)] {
            let error = runtime_v2::stack_service_server::StackService::teardown_stack(
                &service,
                tonic::Request::new(runtime_v2::TeardownStackRequest {
                    metadata: Some(runtime_v2::RequestMetadata {
                        request_id: request_id.to_string(),
                        idempotency_key: String::new(),
                        trace_id: String::new(),
                    }),
                    stack_name: stack_name.to_string(),
                    remove_volumes,
                    dry_run,
                    scope: Some(wire_scope.clone()),
                }),
            )
            .await
            .expect_err("receipt replay requires the exact teardown option tuple");
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        }
        let response = runtime_v2::stack_service_server::StackService::teardown_stack(
            &service,
            tonic::Request::new(runtime_v2::TeardownStackRequest {
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: request_id.to_string(),
                    idempotency_key: String::new(),
                    trace_id: String::new(),
                }),
                stack_name: stack_name.to_string(),
                remove_volumes: false,
                dry_run: false,
                scope: Some(wire_scope),
            }),
        )
        .await
        .unwrap_or_else(|error| panic!("teardown stream should start: {error}"));
        assert_eq!(
            response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok()),
            Some("receipt-shutdown-success")
        );

        let mut stream = response.into_inner();
        let mut completion = None;
        while let Some(item) = stream.next().await {
            let event = item.unwrap_or_else(|error| panic!("teardown should succeed: {error}"));
            if let Some(runtime_v2::teardown_stack_event::Payload::Completion(done)) = event.payload
            {
                completion = done.response;
            }
        }
        let completion = completion.expect("stored completion response");
        assert_eq!(completion.changed_actions, 2);
        assert_eq!(completion.removed_volumes, 0);
        assert!(daemon.manager().has_stack_runtime(stack_name));

        let (receipts, stack_events) = daemon
            .with_state_store(|store| {
                Ok((
                    store.list_receipts_for_entity("stack", stack_name)?,
                    store.load_events(stack_name)?,
                ))
            })
            .unwrap_or_else(|error| panic!("inspect teardown persistence: {error}"));
        assert_eq!(
            receipts
                .iter()
                .filter(|receipt| receipt.operation == "teardown_stack")
                .count(),
            1
        );
        assert!(stack_events.is_empty());
        let active = daemon
            .with_state_store(|store| store.load_active_reconcile_session(stack_name))
            .unwrap()
            .unwrap();
        assert_eq!(active.operation_id, "req-newer-apply");
    }

    #[test]
    fn parse_stack_build_specs_collects_build_entries() {
        let yaml = r#"
services:
  web:
    image: web:latest
    build:
      context: ./web
      dockerfile: Dockerfile.dev
      target: runtime
      args:
        APP_ENV: dev
      cache_from:
        - ghcr.io/acme/web:cache
  worker:
    build: .
"#;

        let builds = parse_stack_build_specs(yaml, ".").expect("build specs");
        assert_eq!(builds.len(), 2);

        let web = builds
            .iter()
            .find(|spec| spec.service_name == "web")
            .expect("web build spec");
        assert_eq!(web.context, "./web");
        assert_eq!(web.dockerfile.as_deref(), Some("Dockerfile.dev"));
        assert_eq!(web.target.as_deref(), Some("runtime"));
        assert_eq!(web.args.get("APP_ENV").map(String::as_str), Some("dev"));
        assert_eq!(web.cache_from, vec!["ghcr.io/acme/web:cache".to_string()]);
    }

    #[test]
    fn parse_stack_spec_rejects_service_healthy_without_healthcheck() {
        let yaml = r#"
services:
  web:
    image: ghcr.io/acme/web:dev
    depends_on:
      db:
        condition: service_healthy
  db:
    image: postgres:16
"#;

        let error = parse_stack_spec("demo", yaml, ".").expect_err("spec should be rejected");
        let message = error.to_string();
        assert!(message.contains("service_healthy"));
        assert!(message.contains("has no healthcheck"));
    }

    #[test]
    fn resolve_build_context_path_handles_relative_and_absolute_paths() {
        let base = PathBuf::from("/tmp/compose");
        let relative = resolve_build_context_path(&base, "./web");
        assert_eq!(relative, PathBuf::from("/tmp/compose").join("./web"));

        let absolute = resolve_build_context_path(&base, "/opt/build");
        assert_eq!(absolute, PathBuf::from("/opt/build"));
    }

    #[test]
    fn build_state_label_is_stable() {
        assert_eq!(build_state_label(BuildState::Queued), "queued");
        assert_eq!(build_state_label(BuildState::Running), "running");
        assert_eq!(build_state_label(BuildState::Succeeded), "succeeded");
        assert_eq!(build_state_label(BuildState::Failed), "failed");
        assert_eq!(build_state_label(BuildState::Canceled), "canceled");
    }

    #[test]
    fn default_stopped_service_uses_stopped_phase() {
        let service = default_stopped_service("api");
        assert_eq!(service.replica.service_name, "api");
        assert_eq!(service.phase, ServicePhase::Stopped);
        assert_eq!(service.container_id, None);
        assert_eq!(service.last_error, None);
        assert!(!service.ready);
    }

    #[test]
    fn stack_status_projection_preserves_base_service_and_exact_replica_index() {
        let observed = [1, 2].map(|replica_index| ServiceObservedState {
            replica: ServiceReplicaKey::new("web", replica_index).unwrap(),
            applied_config_digest: None,
            phase: ServicePhase::Running,
            container_id: Some(format!("ctr-web-{replica_index}")),
            failed_create_ownership: None,
            last_error: None,
            ready: true,
        });

        let statuses = observed
            .iter()
            .map(stack_status_from_observed)
            .collect::<Vec<_>>();
        assert_eq!(statuses[0].service_name, "web");
        assert_eq!(statuses[0].replica_index, 1);
        assert_eq!(statuses[1].service_name, "web");
        assert_eq!(statuses[1].replica_index, 2);
    }

    #[test]
    fn targeted_session_identity_binds_request_action_and_exact_replica() {
        let api_one = ServiceReplicaKey::new("api", 1).unwrap();
        let api_two = ServiceReplicaKey::new("api", 2).unwrap();
        let expected =
            targeted_reconcile_session_id("demo", &api_one, TargetedActionKind::Create, "req-1");

        assert_eq!(
            targeted_reconcile_session_id("demo", &api_one, TargetedActionKind::Create, "req-1"),
            expected
        );
        assert_ne!(
            targeted_reconcile_session_id("demo", &api_two, TargetedActionKind::Create, "req-1"),
            expected
        );
        assert_ne!(
            targeted_reconcile_session_id("demo", &api_one, TargetedActionKind::Remove, "req-1"),
            expected
        );
        assert_ne!(
            targeted_reconcile_session_id("other", &api_one, TargetedActionKind::Create, "req-1"),
            expected
        );
        assert_ne!(
            targeted_reconcile_session_id("demo", &api_one, TargetedActionKind::Create, "req-2"),
            expected
        );
    }

    #[test]
    fn teardown_session_identity_binds_stack_and_request() {
        let (_, daemon) = stack_test_daemon();
        let scope = seed_owned_stack_topology(daemon.as_ref(), "demo");
        let scope = vz_runtime_translate::machine_workload_scope_from_proto(&scope).unwrap();
        let expected = teardown_request_digest("demo", &scope, "req-1", false, false);
        assert_eq!(
            teardown_request_digest("demo", &scope, "req-1", false, false),
            expected
        );
        assert_ne!(
            teardown_request_digest("demo", &scope, "req-2", false, false),
            expected
        );
        assert_ne!(
            teardown_request_digest("demo", &scope, "req-1", false, true),
            expected
        );
        assert_ne!(
            teardown_request_digest("demo", &scope, "req-1", true, false),
            expected
        );
        assert_eq!(
            teardown_reconcile_session_id(&expected),
            teardown_reconcile_session_id(&expected)
        );
    }

    #[test]
    fn stack_service_action_response_wraps_service_status() {
        let response = stack_service_action_response(
            "req-1".to_string(),
            "demo".to_string(),
            ServiceObservedState {
                replica: ServiceReplicaKey::first("web").unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-web-1".to_string()),
                failed_create_ownership: None,
                last_error: None,
                ready: true,
            },
        );

        assert_eq!(response.request_id, "req-1");
        assert_eq!(response.stack_name, "demo");
        let service = response.service.expect("service payload");
        assert_eq!(service.service_name, "web");
        assert_eq!(service.phase, "running");
        assert_eq!(service.container_id, "ctr-web-1");
        assert!(service.ready);
    }

    #[test]
    fn stack_run_container_response_wraps_all_fields() {
        let response = stack_run_container_response(
            "req-1".to_string(),
            "demo".to_string(),
            "web".to_string(),
            "web-run-abc".to_string(),
            "ctr-run-1".to_string(),
        );
        assert_eq!(response.request_id, "req-1");
        assert_eq!(response.stack_name, "demo");
        assert_eq!(response.service_name, "web");
        assert_eq!(response.run_service_name, "web-run-abc");
        assert_eq!(response.container_id, "ctr-run-1");
    }

    #[test]
    fn clone_stack_spec_with_run_service_clones_requested_service() {
        let spec = parse_stack_spec(
            "demo",
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .expect("stack spec");
        let run_spec =
            clone_stack_spec_with_run_service(&spec, "web", "web-run-1", "req-clone-run-service")
                .expect("clone run spec");

        assert_eq!(run_spec.services.len(), spec.services.len() + 1);
        let run_service = run_spec
            .services
            .iter()
            .find(|service| service.name == "web-run-1")
            .expect("run service");
        assert_eq!(run_service.image, "ghcr.io/acme/web:dev");
        assert!(
            run_service.container_name.is_none(),
            "run service should not retain explicit container_name"
        );
    }

    #[test]
    fn clone_stack_spec_with_run_service_preserves_env_mounts_and_resources() {
        let mut spec = parse_stack_spec(
            "demo",
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .expect("stack spec");
        let service = spec
            .services
            .iter_mut()
            .find(|service| service.name == "web")
            .expect("web service");
        service
            .environment
            .insert("APP_ENV".to_string(), "dev".to_string());
        service.mounts.push(vz_stack::MountSpec::Named {
            source: "web-data".to_string(),
            target: "/var/lib/web".to_string(),
            read_only: false,
        });
        service.resources.cpus = Some(2.0);
        service.resources.memory_bytes = Some(512 * 1024 * 1024);

        let run_spec =
            clone_stack_spec_with_run_service(&spec, "web", "web-run-2", "req-clone-run-service")
                .expect("clone run spec");
        let run_service = run_spec
            .services
            .iter()
            .find(|service| service.name == "web-run-2")
            .expect("run service");

        assert_eq!(
            run_service.environment.get("APP_ENV").map(String::as_str),
            Some("dev")
        );
        assert_eq!(run_service.mounts.len(), 1);
        assert_eq!(run_service.resources.cpus, Some(2.0));
        assert_eq!(run_service.resources.memory_bytes, Some(512 * 1024 * 1024));
    }

    struct TestBuildRunner {
        next_build_id: Mutex<u64>,
        start_states: Mutex<VecDeque<BuildState>>,
        poll_states: Mutex<VecDeque<BuildState>>,
        started: Mutex<Vec<(String, BuildSpec)>>,
        build_specs_by_id: Mutex<HashMap<String, (String, BuildSpec)>>,
    }

    impl TestBuildRunner {
        fn new(start_states: Vec<BuildState>, poll_states: Vec<BuildState>) -> Self {
            Self {
                next_build_id: Mutex::new(1),
                start_states: Mutex::new(start_states.into()),
                poll_states: Mutex::new(poll_states.into()),
                started: Mutex::new(Vec::new()),
                build_specs_by_id: Mutex::new(HashMap::new()),
            }
        }

        fn started_specs(&self) -> Vec<(String, BuildSpec)> {
            self.started
                .lock()
                .map(|items| items.clone())
                .unwrap_or_default()
        }

        fn next_state_or_default(
            states: &Mutex<VecDeque<BuildState>>,
            default: BuildState,
        ) -> BuildState {
            match states.lock() {
                Ok(mut guard) => guard.pop_front().unwrap_or(default),
                Err(_) => default,
            }
        }

        fn mk_build(
            build_id: &str,
            sandbox_id: &str,
            spec: &BuildSpec,
            state: BuildState,
        ) -> Build {
            let (result_digest, ended_at) = if state == BuildState::Succeeded {
                (Some("sha256:test-digest".to_string()), Some(2))
            } else if state.is_terminal() {
                (None, Some(2))
            } else {
                (None, None)
            };
            Build {
                build_id: build_id.to_string(),
                sandbox_id: sandbox_id.to_string(),
                build_spec: spec.clone(),
                state,
                result_digest,
                started_at: 1,
                ended_at,
            }
        }
    }

    #[tonic::async_trait]
    impl ComposeBuildRunner for TestBuildRunner {
        async fn start_build(
            &self,
            sandbox_id: &str,
            build_spec: BuildSpec,
        ) -> Result<Build, RuntimeError> {
            let build_id = match self.next_build_id.lock() {
                Ok(mut next) => {
                    let id = format!("build-test-{}", *next);
                    *next += 1;
                    id
                }
                Err(_) => {
                    return Err(RuntimeError::Backend {
                        message: "build id mutex poisoned".to_string(),
                        source: Box::new(std::io::Error::other("build id mutex poisoned")),
                    });
                }
            };

            if let Ok(mut started) = self.started.lock() {
                started.push((sandbox_id.to_string(), build_spec.clone()));
            }
            if let Ok(mut specs) = self.build_specs_by_id.lock() {
                specs.insert(
                    build_id.clone(),
                    (sandbox_id.to_string(), build_spec.clone()),
                );
            }

            let state = Self::next_state_or_default(&self.start_states, BuildState::Succeeded);
            Ok(Self::mk_build(&build_id, sandbox_id, &build_spec, state))
        }

        async fn get_build(&self, build_id: &str) -> Result<Build, RuntimeError> {
            let (sandbox_id, spec) = match self.build_specs_by_id.lock() {
                Ok(specs) => specs
                    .get(build_id)
                    .cloned()
                    .ok_or_else(|| RuntimeError::InvalidConfig("unknown build id".to_string()))?,
                Err(_) => {
                    return Err(RuntimeError::Backend {
                        message: "build spec map mutex poisoned".to_string(),
                        source: Box::new(std::io::Error::other("build spec map mutex poisoned")),
                    });
                }
            };

            let state = Self::next_state_or_default(&self.poll_states, BuildState::Succeeded);
            Ok(Self::mk_build(build_id, &sandbox_id, &spec, state))
        }

        async fn cancel_build(&self, build_id: &str) -> Result<Build, RuntimeError> {
            let (sandbox_id, spec) = match self.build_specs_by_id.lock() {
                Ok(specs) => specs
                    .get(build_id)
                    .cloned()
                    .ok_or_else(|| RuntimeError::InvalidConfig("unknown build id".to_string()))?,
                Err(_) => {
                    return Err(RuntimeError::Backend {
                        message: "build spec map mutex poisoned".to_string(),
                        source: Box::new(std::io::Error::other("build spec map mutex poisoned")),
                    });
                }
            };
            Ok(Self::mk_build(
                build_id,
                &sandbox_id,
                &spec,
                BuildState::Canceled,
            ))
        }
    }

    #[tokio::test]
    async fn run_compose_builds_with_runner_translates_build_spec_and_invokes_build() {
        let compose_yaml = r#"
services:
  web:
    image: ghcr.io/acme/web:dev
    build:
      context: ./web
      dockerfile: Dockerfile.dev
      target: runtime
      args:
        APP_ENV: dev
      cache_from:
        - ghcr.io/acme/web:cache
"#;
        let compose_dir = "/tmp/compose-app";
        let stack_spec = parse_stack_spec("demo", compose_yaml, compose_dir).expect("stack spec");
        let runner = TestBuildRunner::new(vec![BuildState::Succeeded], Vec::new());

        run_compose_builds_with_runner(
            &runner,
            &stack_spec,
            compose_yaml,
            compose_dir,
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .await
        .expect("compose build should succeed");

        let started = runner.started_specs();
        assert_eq!(started.len(), 1);
        let (sandbox_id, spec) = &started[0];
        assert_eq!(sandbox_id, "demo");
        assert_eq!(
            PathBuf::from(&spec.context),
            PathBuf::from("/tmp/compose-app").join("./web")
        );
        assert_eq!(spec.dockerfile.as_deref(), Some("Dockerfile.dev"));
        assert_eq!(spec.target.as_deref(), Some("runtime"));
        assert_eq!(spec.args.get("APP_ENV").map(String::as_str), Some("dev"));
        assert_eq!(spec.cache_from, vec!["ghcr.io/acme/web:cache".to_string()]);
        assert_eq!(spec.image_tag.as_deref(), Some("ghcr.io/acme/web:dev"));
    }

    #[tokio::test]
    async fn run_compose_builds_with_runner_propagates_failed_build_state() {
        let compose_yaml = r#"
services:
  web:
    image: ghcr.io/acme/web:dev
    build:
      context: .
"#;
        let compose_dir = "/tmp/compose-app";
        let stack_spec = parse_stack_spec("demo", compose_yaml, compose_dir).expect("stack spec");
        let runner = TestBuildRunner::new(vec![BuildState::Queued], vec![BuildState::Failed]);

        let error = run_compose_builds_with_runner(
            &runner,
            &stack_spec,
            compose_yaml,
            compose_dir,
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .await
        .expect_err("compose build should fail");

        assert!(
            error.to_string().contains("finished in state failed"),
            "expected failed build state to propagate, got: {error}"
        );
    }

    fn stack_test_daemon() -> (tempfile::TempDir, Arc<RuntimeDaemon>) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let config = RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };
        let daemon = Arc::new(RuntimeDaemon::start(config).expect("daemon start"));
        (tmp, daemon)
    }

    fn seed_owned_stack_topology(
        daemon: &RuntimeDaemon,
        stack_name: &str,
    ) -> runtime_v2::MachineWorkloadScope {
        let wire_scope = crate::grpc::tests::seed_stack_topology(daemon, stack_name);
        let workload_scope = vz_runtime_translate::machine_workload_scope_from_proto(&wire_scope)
            .expect("valid test workload scope");
        daemon
            .with_state_store(|store| {
                store.reserve_stack_workload_owner(&workload_scope, current_unix_secs())
            })
            .expect("reserve test stack workload owner");
        wire_scope
    }

    async fn seed_running_stack_service(
        daemon: Arc<RuntimeDaemon>,
        stack_name: &str,
        wire_scope: &runtime_v2::MachineWorkloadScope,
    ) {
        daemon
            .manager()
            .backend()
            .enable_exact_generation_lifecycle();
        let spec = parse_stack_spec(
            stack_name,
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .expect("stack spec");
        daemon
            .with_state_store(|store| store.save_desired_state(stack_name, &spec))
            .expect("save desired stack state");
        let service = StackServiceImpl::new(daemon.clone());
        let response = runtime_v2::stack_service_server::StackService::start_stack_service(
            &service,
            tonic::Request::new(runtime_v2::StackServiceActionRequest {
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: format!("req-{stack_name}-start"),
                    idempotency_key: String::new(),
                    trace_id: String::new(),
                }),
                stack_name: stack_name.to_string(),
                service_name: "web".to_string(),
                scope: Some(wire_scope.clone()),
            }),
        )
        .await
        .expect("start service stream");
        let completion = read_stack_service_action_completion(response).await;
        let observed = completion.service.expect("started service status");
        assert_eq!(observed.phase, "running");
        assert!(!observed.container_id.is_empty());
    }

    async fn read_stack_service_action_completion(
        response: Response<StackServiceActionEventStream>,
    ) -> runtime_v2::StackServiceActionResponse {
        let mut stream = response.into_inner();
        let mut completion = None;
        while let Some(item) = stream.next().await {
            let event = item.expect("stack service stream event");
            if let Some(runtime_v2::stack_service_action_event::Payload::Completion(done)) =
                event.payload
            {
                completion = Some(done);
            }
        }
        completion
            .expect("expected terminal stack service completion event")
            .response
            .expect("stack service completion should include response")
    }

    #[tokio::test]
    async fn stop_stack_service_noop_returns_stopped_status() {
        let (_tmp, daemon) = stack_test_daemon();
        let wire_scope = seed_owned_stack_topology(daemon.as_ref(), "demo");
        let spec = parse_stack_spec(
            "demo",
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .expect("stack spec");
        daemon
            .with_state_store(|store| {
                store.save_desired_state("demo", &spec)?;
                store.save_observed_state(
                    "demo",
                    &ServiceObservedState {
                        replica: ServiceReplicaKey::first("web").unwrap(),
                        applied_config_digest: None,
                        phase: ServicePhase::Stopped,
                        container_id: None,
                        failed_create_ownership: None,
                        last_error: None,
                        ready: false,
                    },
                )?;
                Ok(())
            })
            .expect("persist state");

        let service = StackServiceImpl::new(daemon);
        let response = runtime_v2::stack_service_server::StackService::stop_stack_service(
            &service,
            tonic::Request::new(runtime_v2::StackServiceActionRequest {
                metadata: None,
                stack_name: "demo".to_string(),
                service_name: "web".to_string(),
                scope: Some(wire_scope),
            }),
        )
        .await
        .expect("stop stack service");

        let payload = read_stack_service_action_completion(response).await;
        let service_status = payload.service.expect("service payload");
        assert_eq!(service_status.service_name, "web");
        assert_eq!(service_status.phase, "stopped");
        assert!(service_status.container_id.is_empty());
    }

    #[tokio::test]
    async fn start_stack_service_noop_for_running_service_returns_running_status() {
        let (_tmp, daemon) = stack_test_daemon();
        let wire_scope = seed_owned_stack_topology(daemon.as_ref(), "demo");
        let spec = parse_stack_spec(
            "demo",
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .expect("stack spec");
        daemon
            .with_state_store(|store| {
                store.save_desired_state("demo", &spec)?;
                store.save_observed_state(
                    "demo",
                    &ServiceObservedState {
                        replica: ServiceReplicaKey::first("web").unwrap(),
                        applied_config_digest: None,
                        phase: ServicePhase::Running,
                        container_id: Some("ctr-web-1".to_string()),
                        failed_create_ownership: None,
                        last_error: None,
                        ready: true,
                    },
                )?;
                Ok(())
            })
            .expect("persist state");

        let service = StackServiceImpl::new(daemon);
        let response = runtime_v2::stack_service_server::StackService::start_stack_service(
            &service,
            tonic::Request::new(runtime_v2::StackServiceActionRequest {
                metadata: None,
                stack_name: "demo".to_string(),
                service_name: "web".to_string(),
                scope: Some(wire_scope),
            }),
        )
        .await
        .expect("start stack service");

        let payload = read_stack_service_action_completion(response).await;
        let service_status = payload.service.expect("service payload");
        assert_eq!(service_status.service_name, "web");
        assert_eq!(service_status.phase, "running");
        assert_eq!(service_status.container_id, "ctr-web-1");
        assert!(service_status.ready);
    }

    #[tokio::test]
    async fn stop_stack_service_returns_not_found_for_unknown_service() {
        let (_tmp, daemon) = stack_test_daemon();
        let wire_scope = seed_owned_stack_topology(daemon.as_ref(), "demo");
        let spec = parse_stack_spec(
            "demo",
            "services:\n  web:\n    image: ghcr.io/acme/web:dev\n",
            ".",
        )
        .expect("stack spec");
        daemon
            .with_state_store(|store| {
                store.save_desired_state("demo", &spec)?;
                Ok(())
            })
            .expect("persist desired state");

        let service = StackServiceImpl::new(daemon);
        let response = runtime_v2::stack_service_server::StackService::stop_stack_service(
            &service,
            tonic::Request::new(runtime_v2::StackServiceActionRequest {
                metadata: None,
                stack_name: "demo".to_string(),
                service_name: "api".to_string(),
                scope: Some(wire_scope),
            }),
        )
        .await
        .expect("unknown service stream should start");

        let mut stream = response.into_inner();
        let error = loop {
            match stream.next().await {
                Some(Ok(_event)) => continue,
                Some(Err(status)) => break status,
                None => panic!("expected terminal stream error for unknown service"),
            }
        };
        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn remove_stack_run_container_requires_run_service_name() {
        let (_tmp, daemon) = stack_test_daemon();
        let service = StackServiceImpl::new(daemon);
        let error = runtime_v2::stack_service_server::StackService::remove_stack_run_container(
            &service,
            tonic::Request::new(runtime_v2::StackRunContainerRequest {
                metadata: None,
                stack_name: "demo".to_string(),
                service_name: "web".to_string(),
                run_service_name: String::new(),
                scope: None,
            }),
        )
        .await
        .expect_err("empty run_service_name should fail");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn create_stack_run_container_requires_service_name() {
        let (_tmp, daemon) = stack_test_daemon();
        let service = StackServiceImpl::new(daemon);
        let error = runtime_v2::stack_service_server::StackService::create_stack_run_container(
            &service,
            tonic::Request::new(runtime_v2::StackRunContainerRequest {
                metadata: None,
                stack_name: "demo".to_string(),
                service_name: String::new(),
                run_service_name: String::new(),
                scope: None,
            }),
        )
        .await
        .expect_err("empty service_name should fail");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}
