//! Stack executor: bridge between reconciler [`Action`]s and the OCI runtime.
//!
//! The [`StackExecutor`] takes a list of actions from [`apply`](crate::apply)
//! and executes them through a [`ContainerRuntime`] implementation:
//! - `ServiceCreate` → pull image + create container + update state to Running
//! - `ServiceRemove` → stop + remove container + update state to Stopped
//! - `ServiceRecreate` → stop + remove + create (full cycle)
//!
//! State transitions and lifecycle events are persisted to the [`StateStore`].

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use tracing::{error, info};

use crate::convert::{secrets_to_mounts, service_to_run_config};
use crate::error::StackError;
use crate::events::StackEvent;
use crate::network::{PublishedPort, resolve_ports};
use crate::reconcile::Action;
use crate::spec::{ServiceSpec, StackSpec};
use crate::state_store::{ServiceObservedState, ServicePhase, StateStore};
use crate::volume::VolumeManager;

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
/// ephemeral port allocation for ports without an explicit host binding.
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
    /// Explicit host_ports are verified against currently allocated ports.
    /// `None` host_ports get an ephemeral port assigned.
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
        let in_use = self.in_use();
        let resolved = resolve_ports(ports, &in_use)?;
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
    /// Per-service VirtioFS mount tag offset for shared VM mode.
    ///
    /// In a shared VM, all services' bind mounts are configured as VirtioFS
    /// shares with globally-unique sequential tags. Each service's mounts
    /// start at an offset so tags don't collide between services.
    mount_tag_offsets: HashMap<String, usize>,
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

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) mod tests_support;

impl<R: ContainerRuntime> StackExecutor<R> {
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
            mount_tag_offsets: HashMap::new(),
        }
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
            for (name, ports) in snapshot.ports {
                self.ports.restore(name, ports);
            }
        }
        Ok(())
    }
}
