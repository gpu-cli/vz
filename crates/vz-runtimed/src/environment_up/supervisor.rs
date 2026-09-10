use std::collections::BTreeSet;

use super::host_exports;
use super::host_imports;
use super::readiness::{MeasuredLinuxReadiness, ReadinessEvidenceProvider};
use super::volumes;
use super::workspace_projection;
use super::*;
use crate::machine_backend::MachineBackendRuntime as MacosRuntimeBackend;
use crate::machine_docker_endpoint::MachineDockerEndpoint;
use crate::machine_runtime_activation::MachineRuntimeActivationError;
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
        // Seed every unseeded fork here: after the sibling stores are pinned, so
        // a fork's own store exists and is fenced, and before the first boot, so
        // no fork ever starts against an empty Docker disk and is handed its
        // parent's underneath itself. The parent is quiesced immediately before
        // each clone and never stopped -- see `crate::machine_fork` for why.
        self.seed_environment_forks(&prepared, &environment, &existing)
            .await
            .map_err(|error| backend_error(error.to_string()))?;
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
        // Every switch is started and every port minted here, before the first
        // boot. `NetworkSwitch::start` fixes a network's membership when it is
        // constructed and a guest descriptor must exist before `LinuxVm::create`,
        // so a port cannot be minted inside the loop that boots the Machine
        // holding it.
        let mut fabric = self
            .install_environment_fabric(
                prepared.lease(),
                &environment,
                &existing.keys().cloned().collect(),
            )
            .await
            .map_err(|error| backend_error(error.to_string()))?;
        // Host exports are resolved here for a different reason than the fabric.
        // A port relay is not fixed at `LinuxVm::create` — `start_port_forwarding`
        // binds it inside the boot — so the mapping need not be minted early.
        // What must happen before the first boot is the collision proof: a
        // Machine that started before a sibling Environment's listener was found
        // holding the port is an effect admitted for an Up that then failed.
        let resolved_host_exports = host_exports::resolve_environment_host_exports(
            &request.definition.environment,
            &environment.machines,
            &environment.host_exports,
        )
        .map_err(|error| {
            let details = error.details();
            failure_with_details(
                &metadata,
                MachineErrorCode::ValidationError,
                error.to_string(),
                details,
            )
        })?;
        host_exports::probe_exportable_host_ports(
            &resolved_host_exports,
            &existing.keys().cloned().collect(),
        )
        .await
        .map_err(|error| {
            // A collision is the one Up refusal a caller is expected to act on
            // by freeing or re-declaring a port, and it is decided here rather
            // than in `validate_supported` because the holder is usually a
            // sibling Environment this definition cannot see. It therefore
            // reaches the CLI in the terminal receipt, and it carries the
            // contended port as a field so an agent reading the error envelope
            // does not have to parse the sentence to find it.
            let details = error.details();
            failure_with_details(
                &metadata,
                MachineErrorCode::StateConflict,
                error.to_string(),
                details,
            )
        })?;
        let mut host_export_ports = host_exports::boot_port_mappings(&resolved_host_exports);
        // Host imports are resolved and their credentials minted here, before
        // the first boot, for the reason exports are: a declaration Up cannot
        // serve must fail before any Machine has started. The installation
        // itself happens after each boot, because its guest half is an agent
        // RPC and the agent is not running until then.
        //
        // The credentials are minted once per Up and held only in this map and
        // in the relay they are handed to. Nothing writes them to the state
        // store, so a stopped Machine's secrets do not survive it.
        let resolved_host_imports = host_imports::resolve_environment_host_imports(
            &request.definition.environment,
            &environment.machines,
            &environment.host_imports,
        )
        .map_err(|error| {
            failure(
                &metadata,
                MachineErrorCode::ValidationError,
                error.to_string(),
            )
        })?;
        let mut host_import_grants = host_imports::boot_import_grants(&resolved_host_imports)
            .map_err(|error| {
                failure(
                    &metadata,
                    MachineErrorCode::BackendUnavailable,
                    error.to_string(),
                )
            })?;
        // Workspace projections are resolved here for the same reason the
        // fabric is: a VirtioFS share is fixed when `LinuxVm::create` runs, so
        // a share cannot be minted inside the loop that boots the Machine
        // holding it. Decision 8's slot resolution therefore has to be durable
        // BEFORE the first boot, not published with the success binding after
        // the last one.
        let declared_slots =
            workspace_projection::declared_workspace_slots(&request.definition.environment);
        let resolved_workspace = if declared_slots.is_empty() {
            workspace_projection::ResolvedWorkspaceMounts::default()
        } else {
            let workspace_key = request.selection.workspace_key.as_deref().ok_or_else(|| {
                failure(
                    &metadata,
                    MachineErrorCode::ValidationError,
                    "declared workspace projection requires a worktree binding token",
                )
            })?;
            let mut resolved: BTreeSet<String> = environment
                .bindings
                .iter()
                .flat_map(|binding| binding.slots.iter().cloned())
                .collect();
            if !declared_slots.is_subset(&resolved) {
                let binding = WorkspaceBinding {
                    schema_version: 1,
                    binding_id: WorkspaceBindingId::generate(),
                    project_id: environment.project_id.clone(),
                    environment_id: environment.environment_id.clone(),
                    name: workspace_projection::minted_binding_name(workspace_key),
                    workspace_key: workspace_key.to_string(),
                    path_hint: request.path_hint.clone(),
                    slots: declared_slots.clone(),
                };
                let reserved = self
                    .with_state_store(|store| {
                        store.reserve_workspace_binding_for_environment(
                            &binding,
                            current_unix_secs(),
                        )
                    })
                    .map_err(state_error)?;
                resolved.extend(reserved.slots);
            }
            workspace_projection::resolve_environment_workspace_mounts(
                &request.definition.environment,
                &environment.machines,
                &resolved,
                request.workspace_root.as_deref(),
            )
            .map_err(|error| {
                failure(
                    &metadata,
                    MachineErrorCode::ValidationError,
                    error.to_string(),
                )
            })?
        };
        let mut workspace_mounts = resolved_workspace.mounts;
        // A `snapshot` Machine's share still points at its SOURCE here. The
        // private clone is made inside the boot loop, where the Machine's own
        // runtime store finally exists to hold it.
        let snapshot_sources = resolved_workspace.snapshot_sources;
        // Declared Environment-owned storage is materialised here for the same
        // reason the fabric and the workspace shares are: a VirtioFS share and a
        // virtio-block device are both fixed when `LinuxVm::create` runs and
        // cannot be added to a running VM. A shared cache joins the Machine's
        // existing mount sequence, so its VirtioFS index starts after whatever
        // the workspace projection already claimed — reusing `vz-mount-0` would
        // silently replace the Machine's workspace share.
        let mut volume_attachments = if volumes::declares_volumes(&request.definition.environment) {
            let next_index = workspace_mounts
                .iter()
                .map(|(machine_id, mounts)| (machine_id.clone(), mounts.len()))
                .collect();
            volumes::resolve_environment_volumes(
                &request.definition.environment,
                &environment.volumes,
                &environment.machines,
                &self.config.runtime_data_dir,
                &environment.environment_id,
                &next_index,
            )
            .map_err(|error| {
                failure(
                    &metadata,
                    MachineErrorCode::StateConflict,
                    error.to_string(),
                )
            })?
        } else {
            BTreeMap::new()
        };
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
            // Taken, not borrowed: a guest end handed to a boot that does not
            // happen must not be usable again.
            let attachments = fabric.remove(&step.machine_id).unwrap_or_default();
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
                    // Taken, not borrowed, for the same reason as the guest
                    // descriptors above: a mapping handed to a boot that does
                    // not happen must not be handed to a second boot as well.
                    // A Machine reused from `existing` never reaches here, so
                    // its listener is the one its original boot bound.
                    let export_ports=host_export_ports.remove(&step.machine_id).unwrap_or_default();
                    // Taken, not borrowed, for the same reason as the guest
                    // descriptors: storage handed to a boot that does not happen
                    // must not be handed to a second boot as well.
                    let machine_volumes=volume_attachments.remove(&step.machine_id).unwrap_or_default();
                    let mut volume_mounts=workspace_mounts.remove(&step.machine_id).unwrap_or_default();
                    // A snapshot projection becomes a private copy INSIDE this
                    // Machine's own runtime store, which only exists now that
                    // `attach_machine` has leased it. Cloning here rather than
                    // before the loop is what lets the copy be reclaimed by the
                    // store's own ownership record instead of needing a new
                    // resource kind, and re-cloning on every Up is the mode's
                    // semantics: the tree is the source as it was at THIS boot.
                    if let Some(source)=snapshot_sources.get(&step.machine_id) {
                        let clone=workspace_projection::materialise_snapshot(
                            &machine.name,source,entry.data_path(),
                        ).map_err(|error|failure(&metadata,MachineErrorCode::StateConflict,error.to_string()))?;
                        for mount in &mut volume_mounts {
                            if mount.host_path==*source { mount.host_path=clone.clone(); }
                        }
                    }
                    volume_mounts.extend(machine_volumes.shares);
                    let block_volumes=machine_volumes.blocks.into_iter().map(|volume|StackBlockVolume{
                        id:volume.id,host_path:volume.host_path,guest_path:volume.guest_path,read_only:volume.read_only,
                    }).collect();
                    // Only a fork's FIRST boot consumes the cloned image. Once
                    // it has run and stopped, its disk is its own and cleanly
                    // unmounted like any other, so the permission is confined to
                    // the one boot that can actually need it rather than granted
                    // to every Machine that was ever forked.
                    let seeded_by_fork = machine.fork.is_some()
                        && machine.incarnation.is_none()
                        && machine.runtime_identity.is_none();
                    let (activation,start_error)=match entry.boot_or_inspect_machine(&reservation,export_ports,attachments,StackResourceHint {
                        docker_data_seeded_by_fork: seeded_by_fork,
                        cpus:Some(cpus),memory_mb:Some(memory_mb),
                        volume_mounts,
                        block_volumes,
                        ..Default::default()
                    }).await {
                        Ok(activation)=>(Arc::new(activation),None),
                        Err(MachineRuntimeActivationError::NativeStart {error,activation})=>(Arc::from(activation),Some(error)),
                        Err(error)=>{uncertain=true;return Err(backend_error(format!("Machine boot failed; original Runtime and fence retained, absence unproven: {error}")));}
                    };
                    run.uncertain.lock().map_err(|error|backend_error(error.to_string()))?.push(Arc::clone(&activation));
                    if let Err(error)=self.machine_live_sessions.register(prepared.lease(),Arc::clone(&activation),&mut None) {
                        uncertain=true; return Err(backend_error(error.to_string()));
                    }
                    // Registry now owns the original boot; no extra reader may
                    // survive and obstruct a later positive Stop shutdown.
                    run.uncertain.lock().map_err(|error|backend_error(error.to_string()))?.clear();
                    // Failed dispatch is not absence or readiness evidence. The
                    // registry owns the exact VM, so release the Up fence and
                    // let public Stop obtain positive shutdown evidence.
                    if let Some(error)=start_error {
                        return Err(backend_error(format!("native Machine start failed; original VM retained for Stop: {error}")));
                    }
                    // Taken, not borrowed, for the same reason as the export
                    // mappings and the guest descriptors: grants handed to a
                    // boot that did not happen must not be handed to a second
                    // one. A Machine reused from `existing` never reaches here,
                    // so its relay is the one its original boot installed.
                    //
                    // A Machine with no declared import gets no call at all,
                    // and therefore no vsock listener and no guest listener.
                    // That is what "host imports are absent by default" means
                    // at runtime rather than in the definition.
                    let import_grants=host_import_grants.remove(&step.machine_id).unwrap_or_default();
                    if !import_grants.is_empty() {
                        let declared:Vec<String>=import_grants.iter().map(|grant|grant.name.clone()).collect();
                        let installation=activation.install_host_imports(import_grants).await
                            .map_err(|error|backend_error(format!("declared host imports could not be installed on Machine `{}`: {error}",machine.name)))?;
                        // The guest reports where it bound. A non-loopback
                        // answer is a boundary wider than the one declared, so
                        // it fails the Up rather than being logged.
                        if installation.guest_bind_address!="127.0.0.1" {
                            return Err(backend_error(format!("Machine `{}` bound its host imports on {} rather than guest loopback",machine.name,installation.guest_bind_address)));
                        }
                        if installation.bound!=declared {
                            return Err(backend_error(format!("Machine `{}` bound host imports {:?} rather than the declared {declared:?}",machine.name,installation.bound)));
                        }
                    }
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
                    slots: declared_slots.clone(),
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
