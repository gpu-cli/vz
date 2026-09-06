use super::readiness::{MeasuredLinuxReadiness, ReadinessEvidenceProvider};
use super::*;
use crate::machine_backend::MachineBackendRuntime as MacosRuntimeBackend;
use crate::machine_docker_endpoint::MachineDockerEndpoint;
use crate::machine_runtime_registry::MachineRuntimeEntry;

impl RuntimeDaemon {
    pub(super) async fn supervise_up(
        self: Arc<Self>,
        request: EnvironmentUpRequest,
        metadata: RequestMetadata,
        run: Arc<UpRun>,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(request.timeout_millis);
        let daemon = Arc::clone(&self);
        let worker_request = request.clone();
        let worker_metadata = metadata.clone();
        let worker_run = Arc::clone(&run);
        let mut task = tokio::spawn(async move {
            daemon
                .drive_up(worker_request, worker_metadata, worker_run, deadline)
                .await
        });
        let result = tokio::select! {
            result=&mut task => result,
            ()=tokio::time::sleep_until(deadline) => {
                let error = failure(&metadata, MachineErrorCode::Timeout,
                    "Up deadline elapsed; original supervisor retains in-flight effects and ownership; this receipt does not prove quiescence");
                self.complete_up(&run, Some(error));
                // Never abort or drop an in-flight boot on observer timeout.
                task.await
            }
        };
        match result {
            Ok(Ok(())) => self.complete_up(&run, None),
            Ok(Err(error)) => self.complete_up(&run, Some(error)),
            Err(error) => self.complete_up(
                &run,
                Some(failure(
                    &metadata,
                    MachineErrorCode::InternalError,
                    format!("Up supervisor failed; retained ownership requires recovery: {error}"),
                )),
            ),
        }
    }

    fn complete_up(&self, run: &UpRun, error: Option<MachineError>) {
        if run.progress.borrow().completion.is_some() {
            return;
        }
        let completion = self.with_state_store(|store| {
            if let Some(existing) =
                store.load_environment_up_completion(&run.admission.idempotency_key)?
            {
                return Ok(existing);
            }
            let operation = store
                .load_environment_lifecycle_by_idempotency_key(&run.admission.idempotency_key)?;
            let workspace_binding = if error.is_none() {
                load_environment(store, &run.admission)?.and_then(|environment| {
                    environment.bindings.into_iter().find(|binding| {
                        Some(binding.workspace_key.as_str())
                            == run.admission.workspace_key.as_deref()
                    })
                })
            } else {
                None
            };
            let completion = EnvironmentUpCompletion {
                admission: run.admission.clone(),
                operation,
                workspace_binding,
                error: error.clone(),
                completed_at: current_unix_secs(),
            };
            store.finish_environment_up_admission(&completion)?;
            Ok(completion)
        });
        match completion {
            Ok(completion) => {
                run.publish("terminal", completion.operation.clone(), Some(completion))
            }
            Err(error) => {
                // A storage failure is never presented as a durable success.
                let completion=EnvironmentUpCompletion {admission:run.admission.clone(),operation:None,workspace_binding:None,
                    error:Some(MachineError::new(MachineErrorCode::BackendUnavailable,
                        format!("Up terminal receipt persistence failed; replay/recovery required: {error}").chars().take(2048).collect(),
                        Some(run.admission.request_id.clone()),BTreeMap::from([("receipt_persisted".into(),"false".into())]))),completed_at:current_unix_secs()};
                run.publish("terminal", None, Some(completion));
            }
        }
    }

    async fn drive_up(
        &self,
        request: EnvironmentUpRequest,
        metadata: RequestMetadata,
        run: Arc<UpRun>,
        deadline: tokio::time::Instant,
    ) -> Result<(), MachineError> {
        let state_error = |error: StackError| error.to_machine_error(&metadata);
        let backend_error =
            |error: String| failure(&metadata, MachineErrorCode::StateConflict, error);
        let lease = tokio::time::timeout_at(
            deadline,
            self.acquire_environment_controller(
                &run.admission.project_id,
                &run.admission.environment_id,
            ),
        )
        .await
        .map_err(|_| {
            failure(
                &metadata,
                MachineErrorCode::Timeout,
                "Up admission deadline elapsed; no VM effects admitted",
            )
        })?
        .map_err(|error| backend_error(error.to_string()))?;
        let environment = self
            .with_state_store(|store| {
                let environment = load_environment(store, &run.admission)?.ok_or_else(|| {
                    StackError::Machine {
                        code: MachineErrorCode::StateConflict,
                        message: "Up Environment disappeared".into(),
                    }
                })?;
                self.authorize_up(&metadata, &environment)?;
                Ok(environment)
            })
            .map_err(state_error)?;
        let non_dispatched = self
            .with_state_store(|store| {
                let mut ids = std::collections::BTreeSet::new();
                for machine in &environment.machines {
                    if store
                        .require_machine_boot_non_dispatch(&environment, &machine.machine_id)?
                        .is_some()
                    {
                        ids.insert(machine.machine_id.clone());
                    }
                }
                Ok(ids)
            })
            .map_err(state_error)?;
        let existing = self
            .machine_live_sessions
            .activations_for_up(&lease, &environment, &non_dispatched)
            .map_err(|error| backend_error(error.to_string()))?;
        // Validate every eventual socket pathname before pinning or booting.
        for machine in &environment.machines {
            if machine.target.os == OperatingSystem::Linux
                && machine.profile == MachineProfile::Developer
            {
                MachineDockerEndpoint::socket_path_for(
                    &self.config.runtime_data_dir,
                    &ResourceOwner {
                        project_id: environment.project_id.clone(),
                        environment_id: environment.environment_id.clone(),
                        machine_id: Some(machine.machine_id.clone()),
                    },
                )
                .map_err(|error| backend_error(error.to_string()))?;
            }
        }
        run.publish("preparing", None, None);
        let run_progress = Arc::clone(&run);
        let prepared = match self
            .prepare_environment_machine_runtimes_with_progress(lease, &environment, {
                let mut last = tokio::time::Instant::now() - Duration::from_secs(1);
                move |progress| {
                    if last.elapsed() >= Duration::from_millis(100) {
                        run_progress.preparing(native_progress(&progress));
                        last = tokio::time::Instant::now();
                    }
                    Ok(())
                }
            })
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                // No VM construction occurs during all-sibling prepare. Any
                // retained pin worker owns its separate controller fence.
                *run.fence
                    .lock()
                    .map_err(|error| backend_error(error.to_string()))? = None;
                return Err(backend_error(error.to_string()));
            }
        };
        self.with_state_store(|store| {
            let current =
                load_environment(store, &run.admission)?.ok_or_else(|| StackError::Machine {
                    code: MachineErrorCode::StateConflict,
                    message: "Up owner disappeared".into(),
                })?;
            self.authorize_up(&metadata, &current)
        })
        .map_err(state_error)?;
        if tokio::time::Instant::now() >= deadline {
            *run.fence
                .lock()
                .map_err(|error| backend_error(error.to_string()))? = None;
            return Err(failure(
                &metadata,
                MachineErrorCode::Timeout,
                "Up deadline elapsed before lifecycle effects",
            ));
        }
        let mut operation = self
            .with_state_store(|store| {
                store.begin_environment_lifecycle(
                    environment.environment_id.as_str(),
                    EnvironmentLifecycleKind::Up,
                    &run.admission.request_id,
                    &run.admission.idempotency_key,
                    &run.admission.request_hash,
                    current_unix_secs(),
                )
            })
            .map_err(state_error)?;
        *run.fence
            .lock()
            .map_err(|error| backend_error(error.to_string()))? =
            Some(prepared.lease().retained_guard());
        run.publish("starting", Some(operation.clone()), None);
        // Arm every absent sibling before the first boot. These proofs cover
        // VM non-dispatch only, never absence of pinned stores/disks.
        self.with_state_store(|store| {
            for step in &operation.machine_steps {
                if !existing.contains_key(&step.machine_id)
                    && matches!(
                        step.status,
                        LifecycleStepStatus::Pending | LifecycleStepStatus::Running
                    )
                {
                    store.record_machine_boot_non_dispatch(&operation, &step.machine_id)?;
                }
            }
            Ok(())
        })
        .map_err(state_error)?;
        let mut first_error = None;
        let mut uncertain = false;
        for step in operation.machine_steps.clone() {
            if step.status == LifecycleStepStatus::Succeeded {
                continue;
            }
            let machine = environment
                .machines
                .iter()
                .find(|machine| machine.machine_id == step.machine_id)
                .ok_or_else(|| backend_error("Up sibling vanished".into()))?;
            let result:Result<MachineActivationEvidence,MachineError>=async {
                if tokio::time::Instant::now()>=deadline { return Err(failure(&metadata,MachineErrorCode::Timeout,"Up deadline elapsed; no further Machine effects admitted")); }
                self.with_state_store(|_|self.authorize_up(&metadata,&environment)).map_err(state_error)?;
                let entry=prepared.attach_machine(&self.state_store,&self.machine_runtime_registry,&operation,&step.machine_id)
                    .map_err(|error|backend_error(error.to_string()))?;
                let pin=prepared.pins().iter().find(|pin|pin.store().owner().machine_id.as_ref()==Some(&step.machine_id));
                let native_pin=prepared.native_pins().iter().find(|pin|pin.store().owner().machine_id.as_ref()==Some(&step.machine_id));
                if pin.is_none() && native_pin.is_none() {return Err(backend_error("prepared Machine pin missing".into()));}
                let activation=if let Some(activation)=existing.get(&step.machine_id) {
                    if !Arc::ptr_eq(activation.entry(),&entry) { return Err(backend_error("Up attachment changed original Runtime object".into())); }
                    Arc::clone(activation)
                } else {
                    let (cpus,memory_mb)=if let Some(pin)=native_pin {(pin.configuration().cpus,pin.configuration().memory_mb)} else {let pin=pin.ok_or_else(||backend_error("missing Linux pin".into()))?;(pin.configuration().resources.cpus,pin.configuration().resources.memory_mb)};
                    let reservation=MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(entry.owner()).map_err(|error|backend_error(error.to_string()))?;
                    if let Some(observer)=&self.environment_up_observer {
                        observer.before_dispatch(&EnvironmentUpBootBoundary {admission:run.admission.clone(),operation:operation.clone(),machine_id:step.machine_id.clone(),owner:entry.owner().clone()}).await;
                    }
                    if tokio::time::Instant::now()>=deadline {
                        return Err(failure(&metadata,MachineErrorCode::Timeout,"Up deadline elapsed at exact pre-boot boundary; non-dispatch proof remains armed"));
                    }
                    self.with_state_store(|_|self.authorize_up(&metadata,&environment)).map_err(state_error)?;
                    self.with_state_store(|store|store.consume_machine_boot_non_dispatch(&operation,&step.machine_id)).map_err(state_error)?;
                    let activation=match entry.boot_or_inspect_machine(&reservation,vec![],StackResourceHint {
                        cpus:Some(cpus),memory_mb:Some(memory_mb),..Default::default()
                    }).await { Ok(activation)=>Arc::new(activation),Err(error)=>{uncertain=true;return Err(backend_error(format!("Machine boot failed; original Runtime and fence retained, absence unproven: {error}")));} };
                    run.uncertain.lock().map_err(|error|backend_error(error.to_string()))?.push(Arc::clone(&activation));
                    if let Err(error)=self.machine_live_sessions.register(prepared.lease(),Arc::clone(&activation),&mut None) {
                        uncertain=true; return Err(backend_error(error.to_string()));
                    }
                    // Registry now owns the original boot; no extra reader may
                    // survive and obstruct a later positive Stop shutdown.
                    run.uncertain.lock().map_err(|error|backend_error(error.to_string()))?.clear();
                    activation
                };
                if machine.target.os==OperatingSystem::Linux && machine.profile==MachineProfile::Developer && self.machine_live_sessions.docker_endpoint_path(prepared.lease(),&activation)
                    .map_err(|error|backend_error(error.to_string()))?.is_none() {
                    let path=MachineDockerEndpoint::socket_path_for(&self.config.runtime_data_dir,activation.owner()).map_err(|error|backend_error(error.to_string()))?;
                    let mut endpoint=Some(MachineDockerEndpoint::start(Arc::clone(&activation),&path).await.map_err(|error|backend_error(error.to_string()))?);
                    self.machine_live_sessions.attach_docker_endpoint(prepared.lease(),&activation,&mut endpoint).map_err(|error|backend_error(error.to_string()))?;
                }
                if tokio::time::Instant::now()>=deadline { return Err(failure(&metadata,MachineErrorCode::Timeout,"Machine boot retained, but Up readiness deadline elapsed")); }
                let reused_incarnation=existing.contains_key(&step.machine_id).then(||machine.incarnation.clone()).flatten();
                let incarnation=if let Some(incarnation)=reused_incarnation { incarnation } else {MachineIncarnation {
                    schema_version:1, incarnation_id:MachineIncarnationId::new(format!("inc_runtime_{}",activation.runtime_identity().incarnation_id)).map_err(|error|backend_error(error.to_string()))?,
                    machine_id:machine.machine_id.clone(),generation:machine.incarnation.as_ref().map_or(Some(1),|value|value.generation.checked_add(1))
                        .ok_or_else(||backend_error("Machine incarnation generation overflow".into()))?,created_at:current_unix_secs()
                }};
                let docker_endpoint=self.machine_live_sessions.docker_endpoint_path(prepared.lease(),&activation)
                    .map_err(|error|backend_error(error.to_string()))?;
                if let Some(pin)=native_pin { return super::readiness::await_readiness(super::native_readiness::verify(&activation,pin,machine,incarnation,deadline,&metadata),deadline,&metadata).await; }
                let pin=pin.ok_or_else(||backend_error("missing Linux pin".into()))?;
                let readiness=MeasuredLinuxReadiness {pin,docker_endpoint:docker_endpoint.as_deref(),deadline};
                super::readiness::await_readiness(readiness.verify(&activation,machine,incarnation,&metadata),deadline,&metadata).await
            }.await;
            let (activation, result) = match result {
                Ok(activation) => (Some(activation), LifecycleStepResult::Succeeded),
                Err(error) => {
                    let reason = error.message.clone();
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    (None, LifecycleStepResult::Failed { reason })
                }
            };
            let readiness_failed = matches!(&result, LifecycleStepResult::Failed { .. });
            operation = self
                .with_state_store(|store| {
                    store.acknowledge_environment_machine_step(
                        &MachineLifecycleStepAcknowledgement {
                            operation_id: operation.operation_id.clone(),
                            generation: operation.generation,
                            machine_id: step.machine_id,
                            initial_state: step.initial_state,
                            target_state: step.target_state,
                            expected_incarnation: step.expected_incarnation.clone(),
                            // A failed readiness check has no resulting activation.
                            // Retain the old incarnation only as the expected fence.
                            resulting_incarnation: activation
                                .as_ref()
                                .map(|activation| activation.incarnation.clone()),
                            resulting_activation: activation,
                            result,
                        },
                        current_unix_secs(),
                    )
                })
                .map_err(state_error)?;
            if readiness_failed {
                self.machine_live_sessions
                    .record_failed_up(prepared.lease(), &self.state_store, &operation, machine)
                    .map_err(|error| backend_error(error.to_string()))?;
            }
            run.publish("machine_acknowledged", Some(operation.clone()), None);
            if uncertain {
                break;
            }
        }
        if uncertain {
            return Err(first_error
                .unwrap_or_else(|| backend_error("Up effect uncertainty retained".into())));
        }
        operation = self
            .with_state_store(|store| {
                store.finish_environment_lifecycle(
                    operation.operation_id.as_str(),
                    operation.generation,
                    current_unix_secs(),
                )
            })
            .map_err(state_error)?;
        if first_error.is_none() && tokio::time::Instant::now() >= deadline {
            first_error = Some(failure(
                &metadata,
                MachineErrorCode::Timeout,
                "Up deadline elapsed before success binding; no late binding published",
            ));
        }
        if first_error.is_none() && operation.status == EnvironmentLifecycleStatus::Succeeded {
            if let Some(workspace_key) = &request.selection.workspace_key {
                let binding = WorkspaceBinding {
                    schema_version: 1,
                    binding_id: WorkspaceBindingId::generate(),
                    project_id: environment.project_id.clone(),
                    environment_id: environment.environment_id.clone(),
                    name: {
                        use sha2::{Digest, Sha256};
                        format!("worktree-{:x}", Sha256::digest(workspace_key.as_bytes()))[..41]
                            .into()
                    },
                    workspace_key: workspace_key.clone(),
                    path_hint: request.path_hint,
                };
                if let Err(error) = self.with_state_store(|store| {
                    if tokio::time::Instant::now() >= deadline
                        || run.progress.borrow().completion.is_some()
                    {
                        return Err(StackError::Machine {
                            code: MachineErrorCode::Timeout,
                            message: "Up deadline/terminal failure forbids late success binding"
                                .into(),
                        });
                    }
                    store.refresh_workspace_binding(&binding, current_unix_secs())
                }) {
                    first_error = Some(state_error(error));
                }
            }
        } else if first_error.is_none() {
            first_error = Some(backend_error("durable Up lifecycle did not succeed".into()));
        }
        // Known original sessions remain in registry, available to Stop. Drop
        // this operation fence only after every acknowledgement is durable.
        *run.fence
            .lock()
            .map_err(|error| backend_error(error.to_string()))? = None;
        first_error.map_or(Ok(()), Err)
    }
}

fn load_environment(
    store: &vz_stack::StateStore,
    admission: &EnvironmentUpAdmission,
) -> Result<Option<EnvironmentInstance>, StackError> {
    Ok(store
        .load_project_state_snapshot(admission.project_id.as_str())?
        .and_then(|project| {
            project
                .environments
                .into_iter()
                .find(|environment| environment.environment_id == admission.environment_id)
        }))
}

fn native_progress(
    progress: &vz_macos_provision::bootstrap::Progress,
) -> EnvironmentPreparationProgress {
    use vz_macos_provision::bootstrap::Progress;
    let (label, completed, total) = match progress {
        Progress::Artifact { progress, .. } => {
            use vz_macos_provision::artifact_cache::Phase;
            let label = match progress.phase {
                Phase::Importing => "Copying macOS image files",
                Phase::Downloading => "Downloading macOS image files",
                Phase::VerifyingCache => "Verifying macOS image files",
                Phase::Waiting => "Waiting for macOS image files",
                Phase::Available => "macOS image files ready",
            };
            (label, progress.completed, progress.total)
        }
        Progress::PreparingImage { progress } => {
            ("Preparing macOS image", progress.completed, progress.total)
        }
        Progress::TemplateReady { reused: true } => ("Using prepared macOS image", 1, 1),
        Progress::TemplateReady { reused: false } => ("macOS image prepared", 1, 1),
        _ => ("Preparing macOS image", 0, 1),
    };
    EnvironmentPreparationProgress {
        label: label.into(),
        completed,
        total: total.max(1),
    }
}
