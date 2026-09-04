use std::collections::{HashMap, HashSet};
use std::future::{Future, ready};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use vz_runtime_contract::{
    Build, BuildSpec, BuildState, ContainerCreateReceipt, ContainerGenerationInspection,
    ContainerGenerationOwnership, ContainerGenerationReleaseOutcome, ContainerGenerationScope,
    ContainerInfo, ContainerLogs, ContainerStatus, Event, EventScope, ExecConfig, ExecOutput,
    GenerationCleanupOutcome, ImageInfo, OwnedCreateError, PortMapping, PruneResult, RunConfig,
    RuntimeBackend, RuntimeCapabilities, RuntimeError, StackResourceHint,
};

#[cfg(target_os = "macos")]
use vz_oci_macos::InteractiveExecEvent;

#[derive(Debug, Clone)]
struct MockContainerRecord {
    image: String,
    status: ContainerStatus,
    created_unix_secs: u64,
    started_unix_secs: Option<u64>,
    stopped_unix_secs: Option<u64>,
}

#[derive(Debug, Clone)]
struct MockBuildRecord {
    build: Build,
    events: Vec<Event>,
    next_event_id: u64,
}

#[derive(Debug, Clone)]
struct MockGenerationRecord {
    ownership: ContainerGenerationOwnership,
    published: bool,
}

#[derive(Debug, Default)]
pub struct TestRuntimeBackend {
    next_container_seq: AtomicU64,
    next_build_seq: AtomicU64,
    next_generation_seq: AtomicU64,
    exact_generation_supported: AtomicBool,
    fail_next_shared_vm_shutdown: AtomicBool,
    shared_vm_shutdown_count: AtomicU64,
    shared_vms: Mutex<HashSet<String>>,
    containers: Mutex<HashMap<String, MockContainerRecord>>,
    generations: Mutex<HashMap<String, MockGenerationRecord>>,
    live_exec_sessions: Mutex<HashSet<String>>,
    builds: Mutex<HashMap<String, MockBuildRecord>>,
}

impl TestRuntimeBackend {
    #[cfg(test)]
    pub(crate) fn fail_next_shared_vm_shutdown(&self) {
        self.fail_next_shared_vm_shutdown
            .store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn shared_vm_shutdown_count(&self) -> u64 {
        self.shared_vm_shutdown_count.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn enable_exact_generation_lifecycle(&self) {
        self.exact_generation_supported
            .store(true, Ordering::SeqCst);
    }

    /// Test double for the daemon's exact-generation stack exec bridge.
    ///
    /// Keep this separate from container-ID exec so adapter tests fail if the
    /// production bridge ever drops generation or topology scope authority.
    pub(crate) async fn exec_owned_container_generation(
        &self,
        ownership: &ContainerGenerationOwnership,
        config: ExecConfig,
    ) -> Result<ExecOutput, RuntimeError> {
        if !self.exact_generation_supported.load(Ordering::SeqCst) {
            return Err(Self::unsupported_operation(
                "exec_container_generation",
                "test backend exact generation lifecycle is disabled",
            ));
        }
        ownership.validate().map_err(RuntimeError::InvalidConfig)?;
        let inspection = self.inspect_container_generation(ownership).await?;
        match inspection {
            ContainerGenerationInspection::Published(found) if found == *ownership => {}
            other => {
                return Err(RuntimeError::ExecFailed {
                    id: ownership.container_id.clone(),
                    reason: format!(
                        "exact generation is not the current published owner: {other:?}"
                    ),
                });
            }
        }
        self.exec_container(&ownership.container_id, config).await
    }

    fn lock_poisoned_error(resource: &str) -> RuntimeError {
        RuntimeError::Backend {
            message: format!("test runtime backend lock poisoned: {resource}"),
            source: Box::new(std::io::Error::other("lock poisoned")),
        }
    }

    fn current_unix_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn unsupported_operation(operation: &str, reason: impl Into<String>) -> RuntimeError {
        RuntimeError::UnsupportedOperation {
            operation: operation.to_string(),
            reason: reason.into(),
        }
    }

    fn missing_shared_vm_error(stack_id: &str) -> RuntimeError {
        RuntimeError::InvalidConfig(format!(
            "no shared VM running for stack '{stack_id}'; call boot_shared_vm first"
        ))
    }

    fn next_container_id(&self) -> String {
        let sequence = self.next_container_seq.fetch_add(1, Ordering::Relaxed) + 1;
        format!("ctr-test-{sequence}")
    }

    fn next_build_id(&self) -> String {
        let sequence = self.next_build_seq.fetch_add(1, Ordering::Relaxed) + 1;
        format!("build-test-{sequence}")
    }

    fn resolve_container_id(&self, config: &RunConfig) -> String {
        config
            .container_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| self.next_container_id())
    }

    fn create_container_internal(
        &self,
        stack_id: Option<&str>,
        image: &str,
        config: RunConfig,
    ) -> Result<String, RuntimeError> {
        if let Some(stack_id) = stack_id {
            let shared_vms = self
                .shared_vms
                .lock()
                .map_err(|_| Self::lock_poisoned_error("shared_vms"))?;
            if !shared_vms.contains(stack_id) {
                return Err(Self::missing_shared_vm_error(stack_id));
            }
        }

        let container_id = self.resolve_container_id(&config);
        let now = Self::current_unix_secs();
        let record = MockContainerRecord {
            image: image.to_string(),
            status: ContainerStatus::Running,
            created_unix_secs: now,
            started_unix_secs: Some(now),
            stopped_unix_secs: None,
        };

        let mut containers = self
            .containers
            .lock()
            .map_err(|_| Self::lock_poisoned_error("containers"))?;
        containers.insert(container_id.clone(), record);
        Ok(container_id)
    }

    fn cleanup_generation_internal(
        &self,
        ownership: &ContainerGenerationOwnership,
    ) -> Result<GenerationCleanupOutcome, RuntimeError> {
        ownership.validate().map_err(RuntimeError::InvalidConfig)?;
        let mut generations = self
            .generations
            .lock()
            .map_err(|_| Self::lock_poisoned_error("generations"))?;
        let Some(record) = generations.get(&ownership.container_id) else {
            return Ok(GenerationCleanupOutcome::AlreadyAbsent);
        };
        if record.ownership != *ownership {
            return Err(RuntimeError::InvalidConfig(
                "exact container generation ownership does not match test backend state"
                    .to_string(),
            ));
        }
        generations.remove(&ownership.container_id);
        drop(generations);
        self.containers
            .lock()
            .map_err(|_| Self::lock_poisoned_error("containers"))?
            .remove(&ownership.container_id);
        Ok(GenerationCleanupOutcome::Removed)
    }

    fn ensure_container_exists(&self, container_id: &str) -> Result<(), RuntimeError> {
        let containers = self
            .containers
            .lock()
            .map_err(|_| Self::lock_poisoned_error("containers"))?;
        if containers.contains_key(container_id) {
            Ok(())
        } else {
            Err(RuntimeError::ContainerNotFound {
                id: container_id.to_string(),
            })
        }
    }

    fn record_exec_session(&self, execution_id: Option<&str>) -> Result<(), RuntimeError> {
        let Some(execution_id) = execution_id.map(str::trim).filter(|id| !id.is_empty()) else {
            return Ok(());
        };
        let mut sessions = self
            .live_exec_sessions
            .lock()
            .map_err(|_| Self::lock_poisoned_error("live_exec_sessions"))?;
        sessions.insert(execution_id.to_string());
        Ok(())
    }

    fn require_live_exec_session(&self, execution_id: &str) -> Result<(), RuntimeError> {
        let sessions = self
            .live_exec_sessions
            .lock()
            .map_err(|_| Self::lock_poisoned_error("live_exec_sessions"))?;
        if sessions.contains(execution_id) {
            Ok(())
        } else {
            Err(Self::unsupported_operation(
                "execution_control",
                format!("no live execution session found for {execution_id}"),
            ))
        }
    }

    fn remove_exec_session(&self, execution_id: &str) -> Result<(), RuntimeError> {
        let mut sessions = self
            .live_exec_sessions
            .lock()
            .map_err(|_| Self::lock_poisoned_error("live_exec_sessions"))?;
        sessions.remove(execution_id);
        Ok(())
    }

    fn container_info(container_id: &str, record: &MockContainerRecord) -> ContainerInfo {
        ContainerInfo {
            id: container_id.to_string(),
            image: record.image.clone(),
            image_id: record.image.clone(),
            status: record.status.clone(),
            created_unix_secs: record.created_unix_secs,
            started_unix_secs: record.started_unix_secs,
            stopped_unix_secs: record.stopped_unix_secs,
            rootfs_path: None,
            host_pid: None,
        }
    }

    fn exec_output_for_command(command: &[String]) -> ExecOutput {
        let mut exit_code = 0;
        let mut stdout = String::new();
        let stderr = String::new();

        if let Some(program) = command.first() {
            match program.as_str() {
                "echo" => {
                    if command.len() > 1 {
                        stdout = format!("{}\n", command[1..].join(" "));
                    } else {
                        stdout = "\n".to_string();
                    }
                }
                "false" => exit_code = 1,
                _ => {}
            }
        }

        ExecOutput {
            exit_code,
            stdout,
            stderr,
        }
    }

    fn build_state_label(state: BuildState) -> &'static str {
        match state {
            BuildState::Queued => "queued",
            BuildState::Running => "running",
            BuildState::Succeeded => "succeeded",
            BuildState::Failed => "failed",
            BuildState::Canceled => "canceled",
        }
    }

    fn append_build_state_event(
        record: &mut MockBuildRecord,
        state: BuildState,
        reason: Option<&str>,
    ) {
        let mut payload = HashMap::new();
        payload.insert(
            "state".to_string(),
            Self::build_state_label(state).to_string(),
        );
        if let Some(reason) = reason {
            payload.insert("reason".to_string(), reason.to_string());
        }
        let event = Event {
            event_id: record.next_event_id,
            ts: Self::current_unix_secs(),
            scope: EventScope::Build,
            scope_id: record.build.build_id.clone(),
            event_type: format!("build.state.{}", Self::build_state_label(state)),
            payload: payload.into_iter().collect(),
            trace_id: None,
        };
        record.next_event_id = record.next_event_id.saturating_add(1);
        record.events.push(event);
    }
}

impl RuntimeBackend for TestRuntimeBackend {
    fn name(&self) -> &'static str {
        "test-mock"
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            fs_quick_checkpoint: true,
            checkpoint_fork: true,
            ..RuntimeCapabilities::stack_baseline()
        }
    }

    fn pull(&self, image: &str) -> impl Future<Output = Result<String, RuntimeError>> {
        let image = image.trim().to_string();
        if image.is_empty() {
            return ready(Err(RuntimeError::ImageNotFound { reference: image }));
        }

        ready(Ok("sha256:test-mock".to_string()))
    }

    fn images(&self) -> Result<Vec<ImageInfo>, RuntimeError> {
        Ok(Vec::new())
    }

    fn prune_images(&self) -> Result<PruneResult, RuntimeError> {
        Ok(PruneResult {
            removed_refs: 0,
            removed_manifests: 0,
            removed_configs: 0,
            removed_layer_dirs: 0,
        })
    }

    fn run(
        &self,
        _image: &str,
        config: RunConfig,
    ) -> impl Future<Output = Result<ExecOutput, RuntimeError>> {
        ready(Ok(Self::exec_output_for_command(&config.cmd)))
    }

    fn create_container(
        &self,
        image: &str,
        config: RunConfig,
    ) -> impl Future<Output = Result<String, RuntimeError>> {
        ready(self.create_container_internal(None, image, config))
    }

    fn exec_container(
        &self,
        container_id: &str,
        config: ExecConfig,
    ) -> impl Future<Output = Result<ExecOutput, RuntimeError>> {
        let result = self.ensure_container_exists(container_id).and_then(|_| {
            self.record_exec_session(config.execution_id.as_deref())?;
            Ok(Self::exec_output_for_command(&config.cmd))
        });
        ready(result)
    }

    fn stop_container(
        &self,
        container_id: &str,
        force: bool,
        _signal: Option<&str>,
        _grace_period: Option<std::time::Duration>,
    ) -> impl Future<Output = Result<ContainerInfo, RuntimeError>> {
        let result = (|| {
            let mut containers = self
                .containers
                .lock()
                .map_err(|_| Self::lock_poisoned_error("containers"))?;
            let record = containers.get_mut(container_id).ok_or_else(|| {
                RuntimeError::ContainerNotFound {
                    id: container_id.to_string(),
                }
            })?;

            let now = Self::current_unix_secs();
            let exit_code = if force { 137 } else { 0 };
            record.status = ContainerStatus::Stopped { exit_code };
            record.stopped_unix_secs = Some(now);
            Ok(Self::container_info(container_id, record))
        })();
        ready(result)
    }

    fn remove_container(
        &self,
        container_id: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        let result = (|| {
            let mut containers = self
                .containers
                .lock()
                .map_err(|_| Self::lock_poisoned_error("containers"))?;
            if containers.remove(container_id).is_some() {
                Ok(())
            } else {
                Err(RuntimeError::ContainerNotFound {
                    id: container_id.to_string(),
                })
            }
        })();
        ready(result)
    }

    fn list_containers(&self) -> Result<Vec<ContainerInfo>, RuntimeError> {
        let containers = self
            .containers
            .lock()
            .map_err(|_| Self::lock_poisoned_error("containers"))?;
        Ok(containers
            .iter()
            .map(|(container_id, record)| Self::container_info(container_id, record))
            .collect())
    }

    fn boot_shared_vm(
        &self,
        stack_id: &str,
        _ports: Vec<PortMapping>,
        _resources: StackResourceHint,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        let result = (|| {
            let mut shared_vms = self
                .shared_vms
                .lock()
                .map_err(|_| Self::lock_poisoned_error("shared_vms"))?;
            shared_vms.insert(stack_id.to_string());
            Ok(())
        })();
        ready(result)
    }

    fn create_container_in_stack(
        &self,
        stack_id: &str,
        image: &str,
        config: RunConfig,
    ) -> impl Future<Output = Result<String, RuntimeError>> {
        ready(self.create_container_internal(Some(stack_id), image, config))
    }

    fn reserve_container_generation(
        &self,
        scope: &ContainerGenerationScope,
        container_id: &str,
    ) -> impl Future<Output = Result<ContainerGenerationOwnership, RuntimeError>> {
        let result = (|| {
            if !self.exact_generation_supported.load(Ordering::SeqCst) {
                return Err(Self::unsupported_operation(
                    "reserve_container_generation",
                    "test backend exact generation lifecycle is disabled",
                ));
            }
            scope.validate().map_err(RuntimeError::InvalidConfig)?;
            if container_id.trim().is_empty() {
                return Err(RuntimeError::InvalidConfig(
                    "container_id cannot be empty".to_string(),
                ));
            }
            let mut generations = self
                .generations
                .lock()
                .map_err(|_| Self::lock_poisoned_error("generations"))?;
            if let Some(record) = generations.get(container_id) {
                if record.ownership.scope.as_deref() == Some(scope) {
                    return Ok(record.ownership.clone());
                }
                return Err(RuntimeError::InvalidConfig(
                    "container_id is owned by another exact reservation".to_string(),
                ));
            }
            let ownership = ContainerGenerationOwnership {
                container_id: container_id.to_string(),
                generation: self.next_generation_seq.fetch_add(1, Ordering::SeqCst) + 1,
                stack_id: scope.stack_id.clone(),
                scope: Some(Box::new(scope.clone())),
            };
            generations.insert(
                container_id.to_string(),
                MockGenerationRecord {
                    ownership: ownership.clone(),
                    published: false,
                },
            );
            Ok(ownership)
        })();
        ready(result)
    }

    fn inspect_container_reservation(
        &self,
        scope: &ContainerGenerationScope,
        container_id: &str,
    ) -> impl Future<Output = Result<ContainerGenerationInspection, RuntimeError>> {
        if !self.exact_generation_supported.load(Ordering::SeqCst) {
            return ready(Err(Self::unsupported_operation(
                "inspect_container_reservation",
                "test backend exact generation lifecycle is disabled",
            )));
        }
        let result = self
            .generations
            .lock()
            .map_err(|_| Self::lock_poisoned_error("generations"))
            .map(|generations| match generations.get(container_id) {
                None => ContainerGenerationInspection::Absent,
                Some(record) if record.ownership.scope.as_deref() != Some(scope) => {
                    ContainerGenerationInspection::Foreign
                }
                Some(record) if record.published => {
                    ContainerGenerationInspection::Published(record.ownership.clone())
                }
                Some(record) => {
                    ContainerGenerationInspection::ReservedUnpublished(record.ownership.clone())
                }
            });
        ready(result)
    }

    fn inspect_container_generation(
        &self,
        ownership: &ContainerGenerationOwnership,
    ) -> impl Future<Output = Result<ContainerGenerationInspection, RuntimeError>> {
        if !self.exact_generation_supported.load(Ordering::SeqCst) {
            return ready(Err(Self::unsupported_operation(
                "inspect_container_generation",
                "test backend exact generation lifecycle is disabled",
            )));
        }
        let result = self
            .generations
            .lock()
            .map_err(|_| Self::lock_poisoned_error("generations"))
            .map(
                |generations| match generations.get(&ownership.container_id) {
                    None => ContainerGenerationInspection::Absent,
                    Some(record) if record.ownership == *ownership && record.published => {
                        ContainerGenerationInspection::Published(record.ownership.clone())
                    }
                    Some(record) if record.ownership == *ownership => {
                        ContainerGenerationInspection::ReservedUnpublished(record.ownership.clone())
                    }
                    Some(record) if record.ownership.scope == ownership.scope => {
                        ContainerGenerationInspection::Replacement
                    }
                    Some(_) => ContainerGenerationInspection::Foreign,
                },
            );
        ready(result)
    }

    fn activate_container_generation(
        &self,
        ownership: ContainerGenerationOwnership,
        image: &str,
        config: RunConfig,
    ) -> impl Future<Output = Result<ContainerCreateReceipt, OwnedCreateError<RuntimeError>>> {
        let result = (|| {
            if !self.exact_generation_supported.load(Ordering::SeqCst) {
                return Err(OwnedCreateError::unowned(Self::unsupported_operation(
                    "activate_container_generation",
                    "test backend exact generation lifecycle is disabled",
                )));
            }
            ownership
                .validate()
                .map_err(RuntimeError::InvalidConfig)
                .map_err(OwnedCreateError::unowned)?;
            {
                let generations = self.generations.lock().map_err(|_| {
                    OwnedCreateError::unowned(Self::lock_poisoned_error("generations"))
                })?;
                let Some(record) = generations.get(&ownership.container_id) else {
                    return Err(OwnedCreateError::unowned(RuntimeError::InvalidConfig(
                        "exact container generation was not reserved".to_string(),
                    )));
                };
                if record.ownership != ownership || record.published {
                    return Err(OwnedCreateError::unowned(RuntimeError::InvalidConfig(
                        "exact container generation is not an unpublished reservation".to_string(),
                    )));
                }
            }
            let container_id = self
                .create_container_internal(Some(&ownership.stack_id), image, config)
                .map_err(|error| OwnedCreateError {
                    error,
                    cleanup: Some(ownership.clone()),
                })?;
            if container_id != ownership.container_id {
                return Err(OwnedCreateError {
                    error: RuntimeError::InvalidConfig(
                        "activated container_id differs from exact reservation".to_string(),
                    ),
                    cleanup: Some(ownership),
                });
            }
            self.generations
                .lock()
                .map_err(|_| OwnedCreateError::unowned(Self::lock_poisoned_error("generations")))?
                .get_mut(&container_id)
                .ok_or_else(|| {
                    OwnedCreateError::unowned(RuntimeError::InvalidConfig(
                        "exact reservation disappeared during activation".to_string(),
                    ))
                })?
                .published = true;
            Ok(ContainerCreateReceipt {
                container_id,
                ownership: Some(ownership),
            })
        })();
        ready(result)
    }

    fn release_container_reservation(
        &self,
        ownership: ContainerGenerationOwnership,
    ) -> impl Future<Output = Result<ContainerGenerationReleaseOutcome, RuntimeError>> {
        let result = (|| {
            if !self.exact_generation_supported.load(Ordering::SeqCst) {
                return Err(Self::unsupported_operation(
                    "release_container_reservation",
                    "test backend exact generation lifecycle is disabled",
                ));
            }
            let mut generations = self
                .generations
                .lock()
                .map_err(|_| Self::lock_poisoned_error("generations"))?;
            match generations.get(&ownership.container_id) {
                None => Ok(ContainerGenerationReleaseOutcome::AlreadyAbsent),
                Some(record) if record.ownership == ownership && !record.published => {
                    generations.remove(&ownership.container_id);
                    Ok(ContainerGenerationReleaseOutcome::Released)
                }
                Some(_) => Err(RuntimeError::InvalidConfig(
                    "cannot release a foreign or published exact generation".to_string(),
                )),
            }
        })();
        ready(result)
    }

    fn cleanup_container_generation(
        &self,
        ownership: ContainerGenerationOwnership,
    ) -> impl Future<Output = Result<GenerationCleanupOutcome, RuntimeError>> {
        if !self.exact_generation_supported.load(Ordering::SeqCst) {
            return ready(Err(Self::unsupported_operation(
                "cleanup_container_generation",
                "test backend exact generation lifecycle is disabled",
            )));
        }
        ready(self.cleanup_generation_internal(&ownership))
    }

    fn stop_and_remove_container_generation(
        &self,
        ownership: ContainerGenerationOwnership,
        _signal: Option<String>,
        _grace_period: Option<std::time::Duration>,
    ) -> impl Future<Output = Result<GenerationCleanupOutcome, RuntimeError>> {
        if !self.exact_generation_supported.load(Ordering::SeqCst) {
            return ready(Err(Self::unsupported_operation(
                "stop_and_remove_container_generation",
                "test backend exact generation lifecycle is disabled",
            )));
        }
        ready(self.cleanup_generation_internal(&ownership))
    }

    fn network_setup(
        &self,
        _stack_id: &str,
        _services: Vec<vz_runtime_contract::NetworkServiceConfig>,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        ready(Ok(()))
    }

    fn network_teardown(
        &self,
        _stack_id: &str,
        _service_names: Vec<String>,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        ready(Ok(()))
    }

    fn shutdown_shared_vm(&self, stack_id: &str) -> impl Future<Output = Result<(), RuntimeError>> {
        self.shared_vm_shutdown_count.fetch_add(1, Ordering::SeqCst);
        let result = (|| {
            if self
                .fail_next_shared_vm_shutdown
                .swap(false, Ordering::SeqCst)
            {
                return Err(RuntimeError::Io(std::io::Error::other(format!(
                    "injected shared VM shutdown failure for stack '{stack_id}'"
                ))));
            }
            let mut shared_vms = self
                .shared_vms
                .lock()
                .map_err(|_| Self::lock_poisoned_error("shared_vms"))?;
            shared_vms.remove(stack_id);
            Ok(())
        })();
        ready(result)
    }

    fn has_shared_vm(&self, stack_id: &str) -> bool {
        self.shared_vms
            .lock()
            .map(|shared_vms| shared_vms.contains(stack_id))
            .unwrap_or(false)
    }

    fn logs(&self, container_id: &str) -> Result<ContainerLogs, RuntimeError> {
        self.ensure_container_exists(container_id)?;
        Ok(ContainerLogs::default())
    }

    fn write_exec_stdin(
        &self,
        execution_id: &str,
        _data: &[u8],
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        ready(self.require_live_exec_session(execution_id))
    }

    fn signal_exec(
        &self,
        execution_id: &str,
        _signal: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        ready(self.require_live_exec_session(execution_id))
    }

    fn resize_exec_pty(
        &self,
        execution_id: &str,
        _cols: u16,
        _rows: u16,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        ready(self.require_live_exec_session(execution_id))
    }

    fn cancel_exec(&self, execution_id: &str) -> impl Future<Output = Result<(), RuntimeError>> {
        let result = self
            .require_live_exec_session(execution_id)
            .and_then(|_| self.remove_exec_session(execution_id));
        ready(result)
    }

    fn start_build(
        &self,
        sandbox_id: &str,
        build_spec: BuildSpec,
        _idempotency_key: Option<String>,
    ) -> impl Future<Output = Result<Build, RuntimeError>> {
        let result = (|| {
            if sandbox_id.trim().is_empty() {
                return Err(RuntimeError::InvalidConfig(
                    "sandbox_id cannot be empty".to_string(),
                ));
            }

            let build_id = self.next_build_id();
            let now = Self::current_unix_secs();
            let build = Build {
                build_id: build_id.clone(),
                sandbox_id: sandbox_id.to_string(),
                build_spec,
                state: BuildState::Queued,
                result_digest: None,
                started_at: now,
                ended_at: None,
            };
            let mut record = MockBuildRecord {
                build: build.clone(),
                events: Vec::new(),
                next_event_id: 1,
            };
            Self::append_build_state_event(&mut record, BuildState::Queued, None);

            let mut builds = self
                .builds
                .lock()
                .map_err(|_| Self::lock_poisoned_error("builds"))?;
            builds.insert(build_id, record);
            Ok(build)
        })();
        ready(result)
    }

    fn get_build(&self, build_id: &str) -> impl Future<Output = Result<Build, RuntimeError>> {
        let result = (|| {
            let builds = self
                .builds
                .lock()
                .map_err(|_| Self::lock_poisoned_error("builds"))?;
            builds
                .get(build_id)
                .map(|record| record.build.clone())
                .ok_or_else(|| RuntimeError::ContainerNotFound {
                    id: build_id.to_string(),
                })
        })();
        ready(result)
    }

    fn stream_build_events(
        &self,
        build_id: &str,
        after_event_id: Option<u64>,
    ) -> impl Future<Output = Result<Vec<Event>, RuntimeError>> {
        let result = (|| {
            let builds = self
                .builds
                .lock()
                .map_err(|_| Self::lock_poisoned_error("builds"))?;
            let record = builds
                .get(build_id)
                .ok_or_else(|| RuntimeError::ContainerNotFound {
                    id: build_id.to_string(),
                })?;
            let events = record
                .events
                .iter()
                .filter(|event| after_event_id.is_none_or(|after| event.event_id > after))
                .cloned()
                .collect();
            Ok(events)
        })();
        ready(result)
    }

    fn cancel_build(&self, build_id: &str) -> impl Future<Output = Result<Build, RuntimeError>> {
        let result = (|| {
            let mut builds = self
                .builds
                .lock()
                .map_err(|_| Self::lock_poisoned_error("builds"))?;
            let record =
                builds
                    .get_mut(build_id)
                    .ok_or_else(|| RuntimeError::ContainerNotFound {
                        id: build_id.to_string(),
                    })?;
            if record.build.state.is_terminal() {
                return Ok(record.build.clone());
            }

            record
                .build
                .transition_to(BuildState::Canceled)
                .map_err(|error| RuntimeError::InvalidConfig(error.to_string()))?;
            record.build.ended_at = Some(Self::current_unix_secs());
            Self::append_build_state_event(
                record,
                BuildState::Canceled,
                Some("canceled by caller"),
            );
            Ok(record.build.clone())
        })();
        ready(result)
    }
}

#[cfg(target_os = "macos")]
impl TestRuntimeBackend {
    pub async fn exec_container_streaming<F>(
        &self,
        container_id: &str,
        config: ExecConfig,
        mut on_event: F,
    ) -> Result<ExecOutput, RuntimeError>
    where
        F: FnMut(InteractiveExecEvent),
    {
        let output = self.exec_container(container_id, config).await?;
        if !output.stdout.is_empty() {
            on_event(InteractiveExecEvent::Stdout(
                output.stdout.as_bytes().to_vec(),
            ));
        }
        if !output.stderr.is_empty() {
            on_event(InteractiveExecEvent::Stderr(
                output.stderr.as_bytes().to_vec(),
            ));
        }
        on_event(InteractiveExecEvent::Exit(output.exit_code));
        Ok(output)
    }
}
