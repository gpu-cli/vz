//! Stack executor: bridge between reconciler [`Action`]s and the OCI runtime.
//!
//! The [`StackExecutor`] takes a list of actions from [`apply`](crate::apply)
//! and executes them through a [`ContainerRuntime`] implementation:
//! - `ServiceCreate` → pull image + create container + update state to Running
//! - `ServiceRemove` → stop + remove container + update state to Stopped
//! - `ServiceRecreate` → stop + remove + create (full cycle)
//!
//! State transitions and lifecycle events are persisted to the [`StateStore`].

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use tracing::{error, info};

use crate::convert::{secrets_to_mounts, service_to_run_config};
use crate::error::StackError;
use crate::events::StackEvent;
use crate::network::{PublishedPort, resolve_ports};
use crate::reconcile::Action;
use crate::spec::{SecretDef, SecretSource, ServiceSpec, StackSpec};
use crate::state_store::{ServiceObservedState, ServicePhase, StateStore};
use crate::volume::VolumeManager;

fn scope_state_conflict(message: impl Into<String>) -> StackError {
    StackError::Machine {
        code: vz_runtime_contract::MachineErrorCode::StateConflict,
        message: message.into(),
    }
}

fn load_secret_source_bytes(secret_def: &SecretDef) -> Result<Vec<u8>, StackError> {
    match &secret_def.source {
        SecretSource::File(path) => std::fs::read(path).map_err(|error| {
            StackError::InvalidSpec(format!(
                "failed to read secret file for '{}': {}: {error}",
                secret_def.name, path
            ))
        }),
        SecretSource::Environment(env_var) => match std::env::var(env_var) {
            Ok(value) if !value.is_empty() => Ok(value.into_bytes()),
            Ok(_) => Err(StackError::InvalidSpec(format!(
                "secret '{}' environment source '{}' resolved to an empty value",
                secret_def.name, env_var
            ))),
            Err(std::env::VarError::NotPresent) => Err(StackError::InvalidSpec(format!(
                "secret '{}' environment source '{}' is not set",
                secret_def.name, env_var
            ))),
            Err(std::env::VarError::NotUnicode(_)) => Err(StackError::InvalidSpec(format!(
                "secret '{}' environment source '{}' is not valid UTF-8",
                secret_def.name, env_var
            ))),
        },
    }
}

/// Trait abstracting container lifecycle operations.
///
/// The real implementation wraps `vz_runtime_contract::Runtime` (which is async);
/// tests use a synchronous mock. The CLI layer bridges async by
/// calling `block_on` around the real runtime methods.
pub trait ContainerRuntime: Send + Sync {
    /// Pull an image if not already present. Returns the image ID.
    fn pull(&self, image: &str) -> Result<String, StackError>;

    /// Create and start a container from the given image with the given config.
    /// Returns the container ID.
    fn create(
        &self,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<String, StackError>;

    /// Stop a running container. No-op if already stopped.
    ///
    /// `signal` overrides the default stop signal (SIGTERM).
    /// `grace_period` overrides the default grace period before SIGKILL escalation.
    fn stop(
        &self,
        container_id: &str,
        signal: Option<&str>,
        grace_period: Option<std::time::Duration>,
    ) -> Result<(), StackError>;

    /// Remove a stopped container and its resources.
    fn remove(&self, container_id: &str) -> Result<(), StackError>;

    /// Execute a command inside a running container.
    /// Returns the exit code (0 = success).
    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError>;

    /// Execute a command and capture stdout/stderr.
    ///
    /// Default implementation delegates to [`exec`] and returns empty
    /// strings. Runtimes that support output capture should override.
    fn exec_with_output(
        &self,
        container_id: &str,
        command: &[String],
    ) -> Result<(i32, String, String), StackError> {
        let code = self.exec(container_id, command)?;
        Ok((code, String::new(), String::new()))
    }

    /// Retrieve logs (stdout/stderr) from a container.
    ///
    /// Returns a [`ContainerLogs`] with captured stdout and stderr.
    /// The default implementation returns empty logs; real runtimes
    /// should override this to read from the container log driver.
    fn logs(&self, _container_id: &str) -> Result<ContainerLogs, StackError> {
        Ok(ContainerLogs::default())
    }

    /// Stream log output from a container.
    ///
    /// Returns a [`LogStream`] that yields [`LogLine`]s as they become
    /// available. When `follow` is `true`, the stream stays open and
    /// delivers new lines as they are written; when `false`, only existing
    /// log content is replayed and the channel is then closed.
    ///
    /// The default implementation returns an immediately-closed stream.
    fn stream_logs(
        &self,
        _container_id: &str,
        _service_name: &str,
        _follow: bool,
    ) -> Result<LogStream, StackError> {
        let (_tx, rx) = std::sync::mpsc::channel();
        Ok(rx)
    }

    /// Create a sandbox for multi-container isolation.
    ///
    /// After calling this, containers for the stack should be created via
    /// [`create_in_sandbox`](Self::create_in_sandbox) instead of [`create`](Self::create).
    fn create_sandbox(
        &self,
        _sandbox_id: &str,
        _ports: Vec<vz_runtime_contract::PortMapping>,
        _resources: vz_runtime_contract::StackResourceHint,
    ) -> Result<(), StackError> {
        Ok(())
    }

    /// Create a container within a sandbox scope.
    ///
    /// The sandbox must have been created via [`create_sandbox`](Self::create_sandbox).
    fn create_in_sandbox(
        &self,
        sandbox_id: &str,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<String, StackError> {
        let _ = sandbox_id;
        // Default: fall back to individual container create.
        self.create(image, config)
    }

    /// Create a container while retaining runtime-issued generation ownership
    /// when the create fails after admission.
    // Keep the structured StackError and ownership proof together across this
    // public compatibility boundary; boxing here would infect every runtime
    // adapter while the topology-native streaming API is being introduced.
    #[allow(clippy::result_large_err)]
    fn create_in_sandbox_owned(
        &self,
        _sandbox_id: &str,
        _image: &str,
        _config: vz_runtime_contract::RunConfig,
    ) -> Result<
        vz_runtime_contract::ContainerCreateReceipt,
        vz_runtime_contract::OwnedCreateError<StackError>,
    > {
        Err(vz_runtime_contract::OwnedCreateError::unowned(
            StackError::Network(
                "unsupported_operation: surface=stack; operation=create_in_sandbox_owned; reason=runtime cannot issue generation ownership"
                    .to_string(),
            ),
        ))
    }

    /// Durably reserve an exact scoped generation without OCI or guest mutation.
    fn reserve_container_generation(
        &self,
        _scope: &vz_runtime_contract::ContainerGenerationScope,
        _container_id: &str,
    ) -> Result<vz_runtime_contract::ContainerGenerationOwnership, StackError> {
        Err(StackError::Network(
            "unsupported_operation: surface=stack; operation=reserve_container_generation; reason=runtime lacks two-phase create"
                .to_string(),
        ))
    }

    /// Inspect the generation, if any, owned by one exact reservation.
    fn inspect_container_reservation(
        &self,
        _scope: &vz_runtime_contract::ContainerGenerationScope,
        _container_id: &str,
    ) -> Result<vz_runtime_contract::ContainerGenerationInspection, StackError> {
        Err(StackError::Network(
            "unsupported_operation: surface=stack; operation=inspect_container_reservation; reason=runtime lacks two-phase create"
                .to_string(),
        ))
    }

    /// Inspect an exact generation without adopting replacements or legacy state.
    fn inspect_container_generation(
        &self,
        _ownership: &vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::ContainerGenerationInspection, StackError> {
        Err(StackError::Network(
            "unsupported_operation: surface=stack; operation=inspect_container_generation; reason=runtime lacks two-phase create"
                .to_string(),
        ))
    }

    /// Activate only a previously reserved exact ownership proof.
    #[allow(clippy::result_large_err)]
    fn activate_container_generation(
        &self,
        _ownership: vz_runtime_contract::ContainerGenerationOwnership,
        _image: &str,
        _config: vz_runtime_contract::RunConfig,
    ) -> Result<
        vz_runtime_contract::ContainerCreateReceipt,
        vz_runtime_contract::OwnedCreateError<StackError>,
    > {
        Err(vz_runtime_contract::OwnedCreateError::unowned(
            StackError::Network(
                "unsupported_operation: surface=stack; operation=activate_container_generation; reason=runtime lacks two-phase create"
                    .to_string(),
            ),
        ))
    }

    /// Release only an exact unpublished reservation.
    fn release_container_reservation(
        &self,
        _ownership: vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::ContainerGenerationReleaseOutcome, StackError> {
        Err(StackError::Network(
            "unsupported_operation: surface=stack; operation=release_container_reservation; reason=runtime lacks two-phase create"
                .to_string(),
        ))
    }

    /// Remove only the exact failed-create generation named by `ownership`.
    fn cleanup_container_generation(
        &self,
        _ownership: vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::GenerationCleanupOutcome, StackError> {
        Err(StackError::Network(
            "unsupported_operation: surface=stack; operation=cleanup_container_generation; reason=runtime did not issue generation ownership"
                .to_string(),
        ))
    }

    /// Gracefully stop and remove exactly the successful generation named by `ownership`.
    fn stop_and_remove_container_generation(
        &self,
        _ownership: vz_runtime_contract::ContainerGenerationOwnership,
        _signal: Option<&str>,
        _grace_period: Option<std::time::Duration>,
    ) -> Result<vz_runtime_contract::GenerationCleanupOutcome, StackError> {
        Err(StackError::Network(
            "unsupported_operation: surface=stack; operation=stop_and_remove_container_generation; reason=runtime did not issue generation ownership"
                .to_string(),
        ))
    }

    /// Set up networking for services within a sandbox.
    ///
    /// Creates a bridge and per-service netns with veth pairs so that
    /// containers can communicate using real IP addresses (Docker Compose
    /// style networking).
    fn setup_sandbox_network(
        &self,
        _sandbox_id: &str,
        _services: Vec<vz_runtime_contract::NetworkServiceConfig>,
    ) -> Result<(), StackError> {
        Ok(())
    }

    /// Tear down networking within a sandbox.
    fn teardown_sandbox_network(
        &self,
        _sandbox_id: &str,
        _service_names: Vec<String>,
    ) -> Result<(), StackError> {
        Ok(())
    }

    /// Shut down a sandbox.
    fn shutdown_sandbox(&self, _sandbox_id: &str) -> Result<(), StackError> {
        Ok(())
    }

    /// Check if a sandbox is active.
    fn has_sandbox(&self, _sandbox_id: &str) -> bool {
        false
    }

    /// List container IDs currently running within a sandbox scope.
    ///
    /// Returns the IDs of all containers the runtime considers active
    /// (running or paused) for the given sandbox. Used during startup
    /// recovery to detect orphaned containers left by a prior crash.
    fn list_containers(&self, _sandbox_id: &str) -> Result<Vec<String>, StackError> {
        Ok(Vec::new())
    }
}

/// Container log output (stdout + stderr interleaved).
#[derive(Debug, Clone, Default)]
pub struct ContainerLogs {
    /// Combined stdout/stderr output.
    pub output: String,
}

/// A single line of container log output.
#[derive(Debug, Clone)]
pub struct LogLine {
    /// Optional RFC 3339 timestamp from the container log driver.
    pub timestamp: Option<String>,
    /// Service that produced this line.
    pub service: String,
    /// The log line content (without trailing newline).
    pub line: String,
}

/// A receiver for streaming log lines from a container.
///
/// Consumers should call `recv()` in a loop until `None` is returned
/// (indicating the stream has ended).
pub type LogStream = std::sync::mpsc::Receiver<LogLine>;

/// Tracks host port allocations across services within a stack.
///
/// Ensures no two services bind to the same host port and supports
/// explicit host-port publishing only.
pub struct PortTracker {
    /// Allocated ports keyed by service name.
    allocated: HashMap<String, Vec<PublishedPort>>,
}

impl PortTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self {
            allocated: HashMap::new(),
        }
    }

    /// All host ports currently allocated across all services.
    pub fn in_use(&self) -> HashSet<u16> {
        self.allocated
            .values()
            .flat_map(|ports| ports.iter().map(|p| p.host_port))
            .collect()
    }

    /// Allocate ports for a service. Returns the resolved port mappings.
    ///
    /// Explicit host ports are verified against currently allocated ports.
    /// `None` host ports are treated as internal-only and are not published.
    ///
    /// If the service already has ports allocated (e.g. from a failed create
    /// being retried), the old allocation is released first so it doesn't
    /// conflict with itself.
    pub fn allocate(
        &mut self,
        service_name: &str,
        ports: &[crate::spec::PortSpec],
    ) -> Result<Vec<PublishedPort>, StackError> {
        // Release any previous allocation for this service so retries don't
        // conflict with their own prior allocation.
        self.allocated.remove(service_name);
        let explicit_publish_ports: Vec<_> = ports
            .iter()
            .filter(|port| port.host_port.is_some())
            .cloned()
            .collect();
        let in_use = self.in_use();
        let resolved = resolve_ports(&explicit_publish_ports, &in_use)?;
        self.allocated
            .insert(service_name.to_string(), resolved.clone());
        Ok(resolved)
    }

    /// Release all ports for a service.
    pub fn release(&mut self, service_name: &str) {
        self.allocated.remove(service_name);
    }

    /// Snapshot of all allocated ports (for persistence).
    pub fn allocated_snapshot(&self) -> &HashMap<String, Vec<PublishedPort>> {
        &self.allocated
    }

    /// Restore a previous port allocation from a crash-recovery snapshot.
    pub fn restore(&mut self, service_name: String, ports: Vec<PublishedPort>) {
        self.allocated.insert(service_name, ports);
    }

    /// Get the published ports for a service (if any).
    pub fn ports_for(&self, service_name: &str) -> Option<&[PublishedPort]> {
        self.allocated.get(service_name).map(|v| v.as_slice())
    }
}

impl Default for PortTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Stack executor for orchestrating multi-container stacks.
///
/// # Runtime Integration
///
/// Uses sandbox-scoped operations on [`ContainerRuntime`]:
/// `create_sandbox`, `create_in_sandbox`, `setup_sandbox_network`,
/// `teardown_sandbox_network`, `shutdown_sandbox`, and `has_sandbox`.
/// The CLI layer bridges these to [`WorkspaceRuntimeManager`] sandbox methods.
pub struct StackExecutor<R: ContainerRuntime> {
    runtime: R,
    store: StateStore,
    data_dir: PathBuf,
    volumes: VolumeManager,
    ports: PortTracker,
    /// Per-service primary IP (first network IP, used for port forwarding and /etc/hosts).
    /// Populated during shared VM boot / network setup.
    service_ips: HashMap<String, String>,
    /// Per-service IP addresses keyed by network name.
    ///
    /// Used to resolve peer hostnames on a shared network when a service is
    /// attached to multiple networks with different IPs.
    service_network_ips: HashMap<String, HashMap<String, String>>,
    /// Per-service VirtioFS mount tag offset for shared VM mode.
    ///
    /// In a shared VM, all services' bind mounts are configured as VirtioFS
    /// shares with globally-unique sequential tags. Each service's mounts
    /// start at an offset so tags don't collide between services.
    mount_tag_offsets: HashMap<String, usize>,
    /// Exact production topology authority. `None` is legacy compatibility only.
    scoped_authority: Option<ScopedExecutionAuthority>,
    /// Durable input bytes loaded from the scoped batch manifest.
    scoped_secret_inputs: BTreeMap<String, Vec<u8>>,
    /// Digest metadata for the exact staged secret bytes.
    scoped_secret_digests: BTreeMap<String, String>,
    /// Owner-scoped directory containing the staged secret files.
    scoped_secret_dir: Option<PathBuf>,
    /// Recovery-only authority may clean exact journal ownership but never create.
    scoped_cleanup_only: bool,
}

#[derive(Debug, Clone)]
struct ScopedExecutionAuthority {
    scope: vz_runtime_contract::MachineWorkloadScope,
    environment_generation: u64,
    definition_digest: String,
}

/// Result of executing a batch of actions.
#[derive(Debug, Clone, Default)]
pub struct ExecutionResult {
    /// Number of actions that succeeded.
    pub succeeded: usize,
    /// Number of actions that failed.
    pub failed: usize,
    /// Per-action error messages (service_name → error).
    pub errors: Vec<(String, String)>,
    /// Bind mounts that were skipped during validation.
    pub skipped_mounts: Vec<crate::volume::SkippedMount>,
}

impl ExecutionResult {
    /// Whether all actions succeeded.
    pub fn all_succeeded(&self) -> bool {
        self.failed == 0
    }
}

/// Pre-computed data for a service create, ready for parallel execution.
///
/// Port allocation and mount resolution happen serially (they need
/// `&mut self`), then image pull + container create run in parallel.
mod create;
mod dispatch;
mod remove;
mod scoped;

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) mod tests_support;

impl<R: ContainerRuntime> StackExecutor<R> {
    fn load_secret_input(&self, secret_def: &SecretDef) -> Result<Vec<u8>, StackError> {
        if self.scoped_authority.is_some() {
            return self
                .scoped_secret_inputs
                .get(&secret_def.name)
                .cloned()
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "scoped activation manifest is missing secret '{}'",
                        secret_def.name
                    ))
                });
        }
        load_secret_source_bytes(secret_def)
    }

    /// Create a new executor with the given runtime, state store, and data directory.
    ///
    /// The data directory is used for named volume storage under `<data_dir>/volumes/`
    /// and secret staging under `<data_dir>/secrets/`.
    pub fn new(runtime: R, store: StateStore, data_dir: &Path) -> Self {
        Self {
            runtime,
            store,
            data_dir: data_dir.to_path_buf(),
            volumes: VolumeManager::new(data_dir),
            ports: PortTracker::new(),
            service_ips: HashMap::new(),
            service_network_ips: HashMap::new(),
            mount_tag_offsets: HashMap::new(),
            scoped_authority: None,
            scoped_secret_inputs: BTreeMap::new(),
            scoped_secret_digests: BTreeMap::new(),
            scoped_secret_dir: None,
            scoped_cleanup_only: false,
        }
    }

    /// Create a production executor fenced to one current, runnable Machine workload.
    pub fn new_scoped(
        runtime: R,
        store: StateStore,
        data_dir: &Path,
        scope: vz_runtime_contract::MachineWorkloadScope,
    ) -> Result<Self, StackError> {
        scope.validate().map_err(StackError::InvalidSpec)?;
        store.validate_stack_workload_owner(&scope)?;
        let project = store
            .load_project_state(scope.project_id.as_str())?
            .ok_or_else(|| {
                StackError::InvalidSpec(format!("Project `{}` was not found", scope.project_id))
            })?;
        let environment = project
            .environments
            .iter()
            .find(|environment| environment.environment_id == scope.environment_id)
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "Environment `{}` was not found in Project `{}`",
                    scope.environment_id, scope.project_id
                ))
            })?;
        if environment.project_id != scope.project_id {
            return Err(scope_state_conflict(
                "Project does not own scoped Environment",
            ));
        }
        if environment.state != vz_runtime_contract::EnvironmentState::Ready {
            return Err(scope_state_conflict(format!(
                "Environment `{}` is not runnable for stack reconciliation ({:?})",
                environment.environment_id, environment.state
            )));
        }
        let machine = environment
            .machines
            .iter()
            .find(|machine| machine.machine_id == scope.machine_id)
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "Machine `{}` was not found in Environment `{}`",
                    scope.machine_id, scope.environment_id
                ))
            })?;
        if machine.environment_id != scope.environment_id {
            return Err(scope_state_conflict(
                "Environment does not own scoped Machine",
            ));
        }
        if machine.state != vz_runtime_contract::MachineState::Ready {
            return Err(scope_state_conflict(format!(
                "Machine `{}` is not runnable ({:?})",
                machine.machine_id, machine.state
            )));
        }
        let incarnation = machine.incarnation.as_ref().ok_or_else(|| {
            scope_state_conflict(format!(
                "Machine `{}` has no current incarnation",
                machine.machine_id
            ))
        })?;
        if incarnation.incarnation_id != scope.machine_incarnation_id {
            return Err(scope_state_conflict(
                "Machine workload scope names a stale incarnation",
            ));
        }
        let authority = ScopedExecutionAuthority {
            scope,
            environment_generation: environment.lifecycle_generation,
            definition_digest: environment.definition_digest.clone(),
        };
        let mut executor = Self::new(runtime, store, data_dir);
        executor.scoped_authority = Some(authority);
        Ok(executor)
    }

    /// Create a recovery-only executor for exact journal-owned cleanup.
    ///
    /// Unlike activation admission, cleanup remains available while lifecycle
    /// state is non-runnable and for an incarnation retained only by an exact
    /// scoped journal record. The resulting executor rejects every create or
    /// recreate action before filesystem or runtime mutation.
    pub fn new_scoped_for_cleanup(
        runtime: R,
        store: StateStore,
        data_dir: &Path,
        scope: vz_runtime_contract::MachineWorkloadScope,
    ) -> Result<Self, StackError> {
        scope.validate().map_err(StackError::InvalidSpec)?;
        store.validate_stack_workload_owner(&scope)?;
        let project = store
            .load_project_state(scope.project_id.as_str())?
            .ok_or_else(|| scope_state_conflict("scoped cleanup Project was not found"))?;
        let environment = project
            .environments
            .iter()
            .find(|environment| environment.environment_id == scope.environment_id)
            .ok_or_else(|| scope_state_conflict("scoped cleanup Environment was not found"))?;
        if environment.project_id != scope.project_id {
            return Err(scope_state_conflict(
                "scoped cleanup Project does not own Environment",
            ));
        }
        let machine = environment
            .machines
            .iter()
            .find(|machine| machine.machine_id == scope.machine_id)
            .ok_or_else(|| scope_state_conflict("scoped cleanup Machine was not found"))?;
        if machine.environment_id != scope.environment_id {
            return Err(scope_state_conflict(
                "scoped cleanup Environment does not own Machine",
            ));
        }
        let current_incarnation = machine
            .incarnation
            .as_ref()
            .map(|incarnation| &incarnation.incarnation_id);
        if current_incarnation != Some(&scope.machine_incarnation_id) {
            let journal_proves_historical_scope = store
                .list_stack_container_recovery_records_for_machine_workload(&scope)?
                .iter()
                .any(|record| {
                    record.intent.scope.machine_incarnation_id.as_ref()
                        == Some(&scope.machine_incarnation_id)
                });
            if !journal_proves_historical_scope {
                return Err(scope_state_conflict(
                    "scoped cleanup incarnation is neither current nor journal-owned",
                ));
            }
        }
        let authority = ScopedExecutionAuthority {
            scope,
            environment_generation: environment.lifecycle_generation,
            definition_digest: environment.definition_digest.clone(),
        };
        let mut executor = Self::new(runtime, store, data_dir);
        executor.scoped_authority = Some(authority);
        executor.scoped_cleanup_only = true;
        Ok(executor)
    }

    /// Access the underlying state store.
    pub fn store(&self) -> &StateStore {
        &self.store
    }

    /// Mutably access the underlying state store.
    pub fn store_mut(&mut self) -> &mut StateStore {
        &mut self.store
    }

    /// Access the volume manager.
    pub fn volumes(&self) -> &VolumeManager {
        &self.volumes
    }

    /// Access the port tracker.
    pub fn ports(&self) -> &PortTracker {
        &self.ports
    }

    /// Mutable access to the port tracker (for test reallocation checks).
    #[cfg(test)]
    pub fn ports_mut(&mut self) -> &mut PortTracker {
        &mut self.ports
    }

    /// Access the underlying container runtime.
    pub fn runtime(&self) -> &R {
        &self.runtime
    }

    /// Mutable access to the underlying container runtime (for test failure injection).
    #[cfg(test)]
    pub fn runtime_mut(&mut self) -> &mut R {
        &mut self.runtime
    }

    /// Persist current allocator state for crash recovery.
    pub fn persist_allocator_state(&self, stack_name: &str) -> Result<(), StackError> {
        use crate::state_store::AllocatorSnapshot;
        let snapshot = AllocatorSnapshot {
            ports: self.ports.allocated_snapshot().clone(),
            service_ips: self.service_ips.clone(),
            mount_tag_offsets: self.mount_tag_offsets.clone(),
        };
        self.store.save_allocator_state(stack_name, &snapshot)
    }

    /// Restore allocator state from a previous crash-recovery snapshot.
    pub fn restore_allocator_state(&mut self, stack_name: &str) -> Result<(), StackError> {
        if let Some(snapshot) = self.store.load_allocator_state(stack_name)? {
            self.service_ips = snapshot.service_ips;
            self.mount_tag_offsets = snapshot.mount_tag_offsets;
            self.service_network_ips.clear();
            for (name, ports) in snapshot.ports {
                self.ports.restore(name, ports);
            }
        }
        Ok(())
    }
}
