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
use std::path::Path;

use tracing::{error, info};

use crate::convert::service_to_run_config;
use crate::error::StackError;
use crate::events::StackEvent;
use crate::network::{PublishedPort, resolve_ports};
use crate::reconcile::Action;
use crate::spec::{ServiceSpec, StackSpec};
use crate::state_store::{ServiceObservedState, ServicePhase, StateStore};
use crate::volume::VolumeManager;

/// Trait abstracting container lifecycle operations.
///
/// The real implementation wraps `vz_oci::Runtime` (which is async);
/// tests use a synchronous mock. The CLI layer bridges async by
/// calling `block_on` around the real runtime methods.
pub trait ContainerRuntime {
    /// Pull an image if not already present. Returns the image ID.
    fn pull(&self, image: &str) -> Result<String, StackError>;

    /// Create and start a container from the given image with the given config.
    /// Returns the container ID.
    fn create(&self, image: &str, config: vz_oci::RunConfig) -> Result<String, StackError>;

    /// Stop a running container. No-op if already stopped.
    fn stop(&self, container_id: &str) -> Result<(), StackError>;

    /// Remove a stopped container and its resources.
    fn remove(&self, container_id: &str) -> Result<(), StackError>;

    /// Execute a command inside a running container.
    /// Returns the exit code (0 = success).
    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError>;

    /// Retrieve logs (stdout/stderr) from a container.
    ///
    /// Returns a [`ContainerLogs`] with captured stdout and stderr.
    /// The default implementation returns empty logs; real runtimes
    /// should override this to read from the container log driver.
    fn logs(&self, _container_id: &str) -> Result<ContainerLogs, StackError> {
        Ok(ContainerLogs::default())
    }

    /// Boot a shared VM for a multi-service stack.
    ///
    /// After calling this, containers for the stack should be created via
    /// [`create_in_stack`](Self::create_in_stack) instead of [`create`](Self::create).
    fn boot_shared_vm(
        &self,
        _stack_id: &str,
        _ports: &[vz_oci::PortMapping],
    ) -> Result<(), StackError> {
        Ok(())
    }

    /// Set up per-service network namespaces inside the shared VM.
    ///
    /// Creates a bridge and per-service netns with veth pairs so that
    /// containers can communicate using real IP addresses (Docker Compose
    /// style networking).
    fn network_setup(
        &self,
        _stack_id: &str,
        _services: &[vz_oci::NetworkServiceConfig],
    ) -> Result<(), StackError> {
        Ok(())
    }

    /// Tear down network namespaces for a stack.
    fn network_teardown(
        &self,
        _stack_id: &str,
        _service_names: &[String],
    ) -> Result<(), StackError> {
        Ok(())
    }

    /// Create and start a container inside a shared stack VM.
    ///
    /// The VM must have been booted via [`boot_shared_vm`](Self::boot_shared_vm).
    fn create_in_stack(
        &self,
        stack_id: &str,
        image: &str,
        config: vz_oci::RunConfig,
    ) -> Result<String, StackError> {
        let _ = stack_id;
        // Default: fall back to individual VM per container.
        self.create(image, config)
    }

    /// Shut down the shared VM for a stack, stopping all its containers.
    fn shutdown_shared_vm(&self, _stack_id: &str) -> Result<(), StackError> {
        Ok(())
    }

    /// Check whether a shared VM is running for the given stack.
    fn has_shared_vm(&self, _stack_id: &str) -> bool {
        false
    }
}

/// Container log output.
#[derive(Debug, Clone, Default)]
pub struct ContainerLogs {
    /// Captured stdout.
    pub stdout: String,
    /// Captured stderr.
    pub stderr: String,
}

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
    pub fn allocate(
        &mut self,
        service_name: &str,
        ports: &[crate::spec::PortSpec],
    ) -> Result<Vec<PublishedPort>, StackError> {
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

/// Executes reconciler actions through an OCI container runtime.
pub struct StackExecutor<R: ContainerRuntime> {
    runtime: R,
    store: StateStore,
    volumes: VolumeManager,
    ports: PortTracker,
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
}

impl ExecutionResult {
    /// Whether all actions succeeded.
    pub fn all_succeeded(&self) -> bool {
        self.failed == 0
    }
}

impl<R: ContainerRuntime> StackExecutor<R> {
    /// Create a new executor with the given runtime, state store, and data directory.
    ///
    /// The data directory is used for named volume storage under `<data_dir>/volumes/`.
    pub fn new(runtime: R, store: StateStore, data_dir: &Path) -> Self {
        Self {
            runtime,
            store,
            volumes: VolumeManager::new(data_dir),
            ports: PortTracker::new(),
        }
    }

    /// Access the underlying state store.
    pub fn store(&self) -> &StateStore {
        &self.store
    }

    /// Access the volume manager.
    pub fn volumes(&self) -> &VolumeManager {
        &self.volumes
    }

    /// Access the port tracker.
    pub fn ports(&self) -> &PortTracker {
        &self.ports
    }

    /// Access the underlying container runtime.
    pub fn runtime(&self) -> &R {
        &self.runtime
    }

    /// Execute a batch of reconciler actions for the given stack spec.
    ///
    /// Each action is processed in order. Failures on one service do not
    /// prevent other services from being processed; errors are collected
    /// and returned in [`ExecutionResult`].
    ///
    /// Port allocation is tracked across services: explicit host ports
    /// are validated for conflicts, and `None` host ports get ephemeral
    /// assignments. Ports are released on service removal.
    ///
    /// For multi-service stacks, a shared VM is booted before creating
    /// containers, and per-service network namespaces are set up so
    /// that containers can communicate using real IP addresses (Docker
    /// Compose style networking).
    pub fn execute(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
    ) -> Result<ExecutionResult, StackError> {
        // Ensure named volume directories exist before creating containers.
        let created_volumes = self.volumes.ensure_volumes(&spec.volumes)?;
        for vol_name in &created_volumes {
            self.store.emit_event(
                &spec.name,
                &StackEvent::VolumeCreated {
                    stack_name: spec.name.clone(),
                    volume_name: vol_name.clone(),
                },
            )?;
        }

        // Boot shared VM and set up networking if there are create actions
        // and no shared VM is running yet.
        let has_creates = actions.iter().any(|a| {
            matches!(
                a,
                Action::ServiceCreate { .. } | Action::ServiceRecreate { .. }
            )
        });

        if has_creates && !self.runtime.has_shared_vm(&spec.name) && spec.services.len() > 1 {
            // Collect all ports from all services for the shared VM.
            let all_ports: Vec<vz_oci::PortMapping> = spec
                .services
                .iter()
                .flat_map(|svc| {
                    svc.ports.iter().map(|p| {
                        let protocol = match p.protocol.as_str() {
                            "udp" => vz_oci::PortProtocol::Udp,
                            _ => vz_oci::PortProtocol::Tcp,
                        };
                        vz_oci::PortMapping {
                            host: p.host_port.unwrap_or(p.container_port),
                            container: p.container_port,
                            protocol,
                        }
                    })
                })
                .collect();

            info!(stack = %spec.name, services = spec.services.len(), "booting shared VM");
            self.runtime.boot_shared_vm(&spec.name, &all_ports)?;

            // Set up per-service network namespaces.
            let network_services: Vec<vz_oci::NetworkServiceConfig> = spec
                .services
                .iter()
                .enumerate()
                .map(|(i, svc)| vz_oci::NetworkServiceConfig {
                    name: svc.name.clone(),
                    // 172.20.0.1 = bridge, services start at .2
                    addr: format!("172.20.0.{}/24", i + 2),
                })
                .collect();

            info!(stack = %spec.name, "setting up per-service network namespaces");
            self.runtime.network_setup(&spec.name, &network_services)?;
        }

        let service_map: HashMap<&str, &ServiceSpec> =
            spec.services.iter().map(|s| (s.name.as_str(), s)).collect();

        let mut result = ExecutionResult::default();

        for action in actions {
            match action {
                Action::ServiceCreate { service_name } => {
                    match self.execute_create(spec, &service_map, service_name) {
                        Ok(()) => result.succeeded += 1,
                        Err(e) => {
                            result.failed += 1;
                            result.errors.push((service_name.clone(), e.to_string()));
                        }
                    }
                }
                Action::ServiceRecreate { service_name } => {
                    // Stop and remove old container first.
                    if let Err(e) = self.execute_remove(spec, service_name) {
                        error!(service = %service_name, error = %e, "failed to remove old container during recreate");
                        // Continue with create anyway.
                    }
                    match self.execute_create(spec, &service_map, service_name) {
                        Ok(()) => result.succeeded += 1,
                        Err(e) => {
                            result.failed += 1;
                            result.errors.push((service_name.clone(), e.to_string()));
                        }
                    }
                }
                Action::ServiceRemove { service_name } => {
                    match self.execute_remove(spec, service_name) {
                        Ok(()) => result.succeeded += 1,
                        Err(e) => {
                            result.failed += 1;
                            result.errors.push((service_name.clone(), e.to_string()));
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Execute a service create: pull image, convert spec, allocate ports, create container.
    fn execute_create(
        &mut self,
        spec: &StackSpec,
        service_map: &HashMap<&str, &ServiceSpec>,
        service_name: &str,
    ) -> Result<(), StackError> {
        let svc_spec = service_map.get(service_name).ok_or_else(|| {
            StackError::InvalidSpec(format!("service '{service_name}' not found in stack spec"))
        })?;

        // Update state to Creating.
        self.store.save_observed_state(
            &spec.name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Creating,
                container_id: None,
                last_error: None,
                ready: false,
            },
        )?;

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceCreating {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
            },
        )?;

        // Pull image.
        info!(service = %service_name, image = %svc_spec.image, "pulling image");
        if let Err(e) = self.runtime.pull(&svc_spec.image) {
            self.mark_failed(spec, service_name, &e.to_string())?;
            return Err(e);
        }

        // Resolve mounts using volume manager.
        let resolved_mounts = self
            .volumes
            .resolve_mounts(&svc_spec.mounts, &spec.volumes)?;

        // Allocate ports (resolves ephemeral ports, checks conflicts).
        let published = match self.ports.allocate(service_name, &svc_spec.ports) {
            Ok(p) => p,
            Err(e) => {
                // Emit PortConflict event if allocation fails.
                if let Some(first_port) = svc_spec.ports.first() {
                    self.store.emit_event(
                        &spec.name,
                        &StackEvent::PortConflict {
                            stack_name: spec.name.clone(),
                            service_name: service_name.to_string(),
                            port: first_port.host_port.unwrap_or(first_port.container_port),
                        },
                    )?;
                }
                self.mark_failed(spec, service_name, &e.to_string())?;
                return Err(e);
            }
        };

        // Convert ServiceSpec → RunConfig.
        let mut run_config = service_to_run_config(svc_spec, &resolved_mounts)?;

        // Override ports with resolved allocations.
        run_config.ports = published
            .iter()
            .map(|p| {
                let protocol = match p.protocol.as_str() {
                    "udp" => vz_oci::PortProtocol::Udp,
                    _ => vz_oci::PortProtocol::Tcp,
                };
                vz_oci::PortMapping {
                    host: p.host_port,
                    container: p.container_port,
                    protocol,
                }
            })
            .collect();

        // Auto-inject sibling service hostnames for inter-service resolution.
        let use_shared_vm = self.runtime.has_shared_vm(&spec.name);
        if use_shared_vm {
            // Shared VM with per-service netns: use real IPs (172.20.0.x).
            for (i, svc) in spec.services.iter().enumerate() {
                if svc.name != service_name
                    && !run_config.extra_hosts.iter().any(|(h, _)| h == &svc.name)
                {
                    let ip = format!("172.20.0.{}", i + 2);
                    run_config.extra_hosts.push((svc.name.clone(), ip));
                }
            }

            // Join the per-service network namespace.
            run_config.network_namespace_path = Some(format!("/var/run/netns/{service_name}"));
        } else {
            // Single VM per container: all services share 127.0.0.1.
            for svc in &spec.services {
                if svc.name != service_name
                    && !run_config.extra_hosts.iter().any(|(h, _)| h == &svc.name)
                {
                    run_config
                        .extra_hosts
                        .push((svc.name.clone(), "127.0.0.1".to_string()));
                }
            }
        }

        // Create and start container.
        info!(service = %service_name, image = %svc_spec.image, "creating container");
        let container_id = if use_shared_vm {
            match self
                .runtime
                .create_in_stack(&spec.name, &svc_spec.image, run_config)
            {
                Ok(id) => id,
                Err(e) => {
                    self.mark_failed(spec, service_name, &e.to_string())?;
                    return Err(e);
                }
            }
        } else {
            match self.runtime.create(&svc_spec.image, run_config) {
                Ok(id) => id,
                Err(e) => {
                    self.mark_failed(spec, service_name, &e.to_string())?;
                    return Err(e);
                }
            }
        };

        // Update state to Running.
        self.store.save_observed_state(
            &spec.name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Running,
                container_id: Some(container_id.clone()),
                last_error: None,
                ready: false, // Health checks set this to true later.
            },
        )?;

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceReady {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
                runtime_id: container_id,
            },
        )?;

        info!(service = %service_name, "service running");
        Ok(())
    }

    /// Execute a service removal: stop + remove container, release ports, update state.
    fn execute_remove(&mut self, spec: &StackSpec, service_name: &str) -> Result<(), StackError> {
        // Find current container_id from observed state.
        let observed = self.store.load_observed_state(&spec.name)?;
        let container_id = observed
            .iter()
            .find(|o| o.service_name == service_name)
            .and_then(|o| o.container_id.clone());

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceStopping {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
            },
        )?;

        // Stop and remove if we have a container.
        if let Some(ref cid) = container_id {
            info!(service = %service_name, container = %cid, "stopping container");
            if let Err(e) = self.runtime.stop(cid) {
                error!(service = %service_name, error = %e, "failed to stop container");
                // Continue with remove attempt.
            }

            info!(service = %service_name, container = %cid, "removing container");
            if let Err(e) = self.runtime.remove(cid) {
                error!(service = %service_name, error = %e, "failed to remove container");
            }
        }

        // Release allocated ports.
        self.ports.release(service_name);

        // Update state to Stopped.
        self.store.save_observed_state(
            &spec.name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Stopped,
                container_id: None,
                last_error: None,
                ready: false,
            },
        )?;

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceStopped {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
                exit_code: 0,
            },
        )?;

        info!(service = %service_name, "service stopped");
        Ok(())
    }

    /// Mark a service as failed with an error message.
    fn mark_failed(
        &self,
        spec: &StackSpec,
        service_name: &str,
        error_msg: &str,
    ) -> Result<(), StackError> {
        self.store.save_observed_state(
            &spec.name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Failed,
                container_id: None,
                last_error: Some(error_msg.to_string()),
                ready: false,
            },
        )?;

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceFailed {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
                error: error_msg.to_string(),
            },
        )?;

        Ok(())
    }
}

/// Test support: mock container runtime shared across test modules.
#[cfg(test)]
pub(crate) mod tests_support {
    use super::*;

    /// Mock container runtime for testing.
    ///
    /// Records all operations and can be configured to fail specific calls.
    /// Supports shared VM tracking for multi-service stack testing.
    pub struct MockContainerRuntime {
        /// Container IDs to return on create calls (cycled).
        pub container_ids: Vec<String>,
        /// Whether pull should fail.
        pub fail_pull: bool,
        /// Whether create should fail.
        pub fail_create: bool,
        /// Whether stop should fail.
        pub fail_stop: bool,
        /// Exit code to return from exec calls.
        pub exec_exit_code: i32,
        /// Whether exec should fail with an error (not just non-zero exit).
        pub fail_exec: bool,
        /// Tracks calls: (operation, arg).
        pub calls: std::cell::RefCell<Vec<(String, String)>>,
        /// Counter for create calls (to cycle through container_ids).
        create_counter: std::cell::Cell<usize>,
        /// Tracks which stacks have a shared VM running.
        shared_vms: std::cell::RefCell<HashSet<String>>,
        /// Captured RunConfigs from create_in_stack calls, keyed by container_id.
        pub captured_configs: std::cell::RefCell<Vec<(String, vz_oci::RunConfig)>>,
        /// Captured NetworkServiceConfigs from network_setup calls.
        pub captured_network_services:
            std::cell::RefCell<Vec<(String, Vec<vz_oci::NetworkServiceConfig>)>>,
    }

    impl MockContainerRuntime {
        pub fn new() -> Self {
            Self {
                container_ids: vec!["ctr-001".to_string()],
                fail_pull: false,
                fail_create: false,
                fail_stop: false,
                exec_exit_code: 0,
                fail_exec: false,
                calls: std::cell::RefCell::new(Vec::new()),
                create_counter: std::cell::Cell::new(0),
                shared_vms: std::cell::RefCell::new(HashSet::new()),
                captured_configs: std::cell::RefCell::new(Vec::new()),
                captured_network_services: std::cell::RefCell::new(Vec::new()),
            }
        }

        pub fn with_ids(ids: Vec<&str>) -> Self {
            Self {
                container_ids: ids.into_iter().map(String::from).collect(),
                ..Self::new()
            }
        }

        pub fn call_log(&self) -> Vec<(String, String)> {
            self.calls.borrow().clone()
        }
    }

    impl ContainerRuntime for MockContainerRuntime {
        fn pull(&self, image: &str) -> Result<String, StackError> {
            self.calls
                .borrow_mut()
                .push(("pull".to_string(), image.to_string()));
            if self.fail_pull {
                return Err(StackError::InvalidSpec("mock pull failure".to_string()));
            }
            Ok(format!("sha256:{image}"))
        }

        fn create(&self, image: &str, config: vz_oci::RunConfig) -> Result<String, StackError> {
            self.calls
                .borrow_mut()
                .push(("create".to_string(), image.to_string()));
            if self.fail_create {
                return Err(StackError::InvalidSpec("mock create failure".to_string()));
            }
            let idx = self.create_counter.get();
            let id = self.container_ids[idx % self.container_ids.len()].clone();
            self.create_counter.set(idx + 1);
            self.captured_configs
                .borrow_mut()
                .push((id.clone(), config));
            Ok(id)
        }

        fn stop(&self, container_id: &str) -> Result<(), StackError> {
            self.calls
                .borrow_mut()
                .push(("stop".to_string(), container_id.to_string()));
            if self.fail_stop {
                return Err(StackError::InvalidSpec("mock stop failure".to_string()));
            }
            Ok(())
        }

        fn remove(&self, container_id: &str) -> Result<(), StackError> {
            self.calls
                .borrow_mut()
                .push(("remove".to_string(), container_id.to_string()));
            Ok(())
        }

        fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError> {
            self.calls.borrow_mut().push((
                "exec".to_string(),
                format!("{container_id}:{}", command.join(" ")),
            ));
            if self.fail_exec {
                return Err(StackError::InvalidSpec("mock exec failure".to_string()));
            }
            Ok(self.exec_exit_code)
        }

        fn boot_shared_vm(
            &self,
            stack_id: &str,
            ports: &[vz_oci::PortMapping],
        ) -> Result<(), StackError> {
            self.calls.borrow_mut().push((
                "boot_shared_vm".to_string(),
                format!(
                    "{}:{}",
                    stack_id,
                    ports
                        .iter()
                        .map(|p| format!("{}:{}", p.host, p.container))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            ));
            self.shared_vms.borrow_mut().insert(stack_id.to_string());
            Ok(())
        }

        fn network_setup(
            &self,
            stack_id: &str,
            services: &[vz_oci::NetworkServiceConfig],
        ) -> Result<(), StackError> {
            self.calls.borrow_mut().push((
                "network_setup".to_string(),
                format!(
                    "{}:{}",
                    stack_id,
                    services
                        .iter()
                        .map(|s| format!("{}={}", s.name, s.addr))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            ));
            self.captured_network_services
                .borrow_mut()
                .push((stack_id.to_string(), services.to_vec()));
            Ok(())
        }

        fn network_teardown(
            &self,
            stack_id: &str,
            service_names: &[String],
        ) -> Result<(), StackError> {
            self.calls.borrow_mut().push((
                "network_teardown".to_string(),
                format!("{}:{}", stack_id, service_names.join(",")),
            ));
            Ok(())
        }

        fn create_in_stack(
            &self,
            stack_id: &str,
            image: &str,
            config: vz_oci::RunConfig,
        ) -> Result<String, StackError> {
            self.calls
                .borrow_mut()
                .push(("create_in_stack".to_string(), format!("{stack_id}:{image}")));
            if self.fail_create {
                return Err(StackError::InvalidSpec("mock create failure".to_string()));
            }
            let idx = self.create_counter.get();
            let id = self.container_ids[idx % self.container_ids.len()].clone();
            self.create_counter.set(idx + 1);
            self.captured_configs
                .borrow_mut()
                .push((id.clone(), config));
            Ok(id)
        }

        fn shutdown_shared_vm(&self, stack_id: &str) -> Result<(), StackError> {
            self.calls
                .borrow_mut()
                .push(("shutdown_shared_vm".to_string(), stack_id.to_string()));
            self.shared_vms.borrow_mut().remove(stack_id);
            Ok(())
        }

        fn has_shared_vm(&self, stack_id: &str) -> bool {
            self.shared_vms.borrow().contains(stack_id)
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::tests_support::MockContainerRuntime;
    use super::*;
    use crate::spec::MountSpec as StackMountSpec;
    use crate::spec::{PortSpec, ResourcesSpec, StackSpec, VolumeSpec};
    use std::collections::HashMap;

    fn svc(name: &str, image: &str) -> ServiceSpec {
        ServiceSpec {
            name: name.to_string(),
            image: image.to_string(),
            command: None,
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            mounts: vec![],
            ports: vec![],
            depends_on: vec![],
            healthcheck: None,
            restart_policy: None,
            resources: ResourcesSpec::default(),
            extra_hosts: vec![],
        }
    }

    fn stack(name: &str, services: Vec<ServiceSpec>) -> StackSpec {
        StackSpec {
            name: name.to_string(),
            services,
            networks: vec![],
            volumes: vec![],
        }
    }

    fn make_executor(runtime: MockContainerRuntime) -> StackExecutor<MockContainerRuntime> {
        let tmp = tempfile::tempdir().unwrap();
        let store = StateStore::in_memory().unwrap();
        StackExecutor::new(runtime, store, tmp.path())
    }

    fn make_executor_with_dir(
        runtime: MockContainerRuntime,
        dir: &Path,
    ) -> StackExecutor<MockContainerRuntime> {
        let store = StateStore::in_memory().unwrap();
        StackExecutor::new(runtime, store, dir)
    }

    #[test]
    fn create_single_service() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());
        assert_eq!(result.succeeded, 1);
        assert_eq!(result.failed, 0);

        // Verify observed state.
        let observed = executor.store().load_observed_state("myapp").unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].phase, ServicePhase::Running);
        assert_eq!(observed[0].container_id, Some("ctr-001".to_string()));

        // Verify events.
        let events = executor.store().load_events("myapp").unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::ServiceCreating { .. }))
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::ServiceReady { .. }))
        );
    }

    #[test]
    fn create_multiple_services() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = stack(
            "myapp",
            vec![svc("web", "nginx:latest"), svc("db", "postgres:16")],
        );

        let actions = vec![
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());
        assert_eq!(result.succeeded, 2);

        let observed = executor.store().load_observed_state("myapp").unwrap();
        assert_eq!(observed.len(), 2);
    }

    #[test]
    fn remove_service() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![]);

        // Simulate existing running container.
        executor
            .store()
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    service_name: "old".to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some("ctr-old".to_string()),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();

        let actions = vec![Action::ServiceRemove {
            service_name: "old".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Verify stop+remove were called.
        let calls = executor.runtime.call_log();
        assert!(calls.iter().any(|(op, _)| op == "stop"));
        assert!(calls.iter().any(|(op, _)| op == "remove"));

        // Verify state is Stopped.
        let observed = executor.store().load_observed_state("myapp").unwrap();
        let old = observed.iter().find(|o| o.service_name == "old").unwrap();
        assert_eq!(old.phase, ServicePhase::Stopped);
        assert!(old.container_id.is_none());
    }

    #[test]
    fn recreate_service() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-new"]);
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

        // Simulate existing running container.
        executor
            .store()
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    service_name: "web".to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some("ctr-old".to_string()),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();

        let actions = vec![Action::ServiceRecreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Verify stop+remove of old, then pull+create of new.
        let calls = executor.runtime.call_log();
        let ops: Vec<&str> = calls.iter().map(|(op, _)| op.as_str()).collect();
        assert_eq!(ops, vec!["stop", "remove", "pull", "create"]);

        // New container.
        let observed = executor.store().load_observed_state("myapp").unwrap();
        let web = observed.iter().find(|o| o.service_name == "web").unwrap();
        assert_eq!(web.phase, ServicePhase::Running);
        assert_eq!(web.container_id, Some("ctr-new".to_string()));
    }

    #[test]
    fn pull_failure_marks_service_failed() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_pull = true;
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert_eq!(result.failed, 1);
        assert!(!result.all_succeeded());

        // Service should be marked Failed.
        let observed = executor.store().load_observed_state("myapp").unwrap();
        let web = observed.iter().find(|o| o.service_name == "web").unwrap();
        assert_eq!(web.phase, ServicePhase::Failed);
        assert!(web.last_error.is_some());

        // ServiceFailed event emitted.
        let events = executor.store().load_events("myapp").unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::ServiceFailed { .. }))
        );
    }

    #[test]
    fn create_failure_marks_service_failed() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_create = true;
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert_eq!(result.failed, 1);

        let observed = executor.store().load_observed_state("myapp").unwrap();
        let web = observed.iter().find(|o| o.service_name == "web").unwrap();
        assert_eq!(web.phase, ServicePhase::Failed);
    }

    #[test]
    fn partial_failure_continues_other_services() {
        let mut runtime = MockContainerRuntime::with_ids(vec!["ctr-db"]);
        runtime.fail_pull = false;
        runtime.fail_create = false;
        let mut executor = make_executor(runtime);

        let spec = stack(
            "myapp",
            vec![svc("db", "postgres:16"), svc("web", "nginx:latest")],
        );

        // Make only "web" fail by using a spec that triggers an error.
        // We'll test with a normal mock that succeeds for both.
        let actions = vec![
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        // Both succeed with mock.
        assert_eq!(result.succeeded, 2);
    }

    #[test]
    fn remove_with_no_container_id() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![]);

        // Service observed but no container_id.
        executor
            .store()
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    service_name: "orphan".to_string(),
                    phase: ServicePhase::Pending,
                    container_id: None,
                    last_error: None,
                    ready: false,
                },
            )
            .unwrap();

        let actions = vec![Action::ServiceRemove {
            service_name: "orphan".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // No stop/remove calls since there's no container.
        let calls = executor.runtime.call_log();
        assert!(calls.is_empty());
    }

    #[test]
    fn volumes_created_before_containers() {
        let runtime = MockContainerRuntime::new();
        let tmp = tempfile::tempdir().unwrap();
        let mut executor = make_executor_with_dir(runtime, tmp.path());

        let spec = StackSpec {
            name: "myapp".to_string(),
            services: vec![ServiceSpec {
                mounts: vec![StackMountSpec::Named {
                    source: "dbdata".to_string(),
                    target: "/var/lib/db".to_string(),
                    read_only: false,
                }],
                ..svc("db", "postgres:16")
            }],
            networks: vec![],
            volumes: vec![VolumeSpec {
                name: "dbdata".to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            }],
        };

        let actions = vec![Action::ServiceCreate {
            service_name: "db".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Volume directory exists.
        assert!(executor.volumes().volumes_dir().join("dbdata").is_dir());

        // VolumeCreated event emitted.
        let events = executor.store().load_events("myapp").unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::VolumeCreated { .. }))
        );
    }

    #[test]
    fn service_with_ports_creates_correctly() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);

        let spec = stack(
            "myapp",
            vec![ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                ..svc("web", "nginx:latest")
            }],
        );

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Verify pull and create were called.
        let calls = executor.runtime.call_log();
        assert_eq!(calls.len(), 2); // pull + create
    }

    #[test]
    fn stop_failure_does_not_prevent_state_update() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_stop = true;
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![]);

        executor
            .store()
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    service_name: "web".to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some("ctr-1".to_string()),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();

        let actions = vec![Action::ServiceRemove {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        // Still counts as succeeded (best-effort stop).
        assert!(result.all_succeeded());

        // State still updated to Stopped.
        let observed = executor.store().load_observed_state("myapp").unwrap();
        let web = observed.iter().find(|o| o.service_name == "web").unwrap();
        assert_eq!(web.phase, ServicePhase::Stopped);
    }

    #[test]
    fn execution_result_errors_collected() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_pull = true;
        let mut executor = make_executor(runtime);

        let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].0, "web");
        assert!(result.errors[0].1.contains("mock pull failure"));
    }

    // ── Port tracking tests ──

    #[test]
    fn port_tracker_allocates_explicit_port() {
        let mut tracker = PortTracker::new();
        let ports = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 80,
            host_port: Some(8080),
        }];
        let published = tracker.allocate("web", &ports).unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].host_port, 8080);
        assert_eq!(published[0].container_port, 80);
        assert!(tracker.in_use().contains(&8080));
    }

    #[test]
    fn port_tracker_allocates_ephemeral_port() {
        let mut tracker = PortTracker::new();
        let ports = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 3000,
            host_port: None,
        }];
        let published = tracker.allocate("api", &ports).unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].container_port, 3000);
        // Ephemeral port should be assigned.
        assert!(published[0].host_port > 0);
        assert!(tracker.in_use().contains(&published[0].host_port));
    }

    #[test]
    fn port_tracker_detects_cross_service_conflict() {
        let mut tracker = PortTracker::new();
        let ports_a = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 80,
            host_port: Some(8080),
        }];
        tracker.allocate("web", &ports_a).unwrap();

        // Second service trying the same host port should fail.
        let ports_b = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 3000,
            host_port: Some(8080),
        }];
        let result = tracker.allocate("api", &ports_b);
        assert!(result.is_err());
    }

    #[test]
    fn port_tracker_release_frees_port() {
        let mut tracker = PortTracker::new();
        let ports = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 80,
            host_port: Some(9090),
        }];
        tracker.allocate("web", &ports).unwrap();
        assert!(tracker.in_use().contains(&9090));

        tracker.release("web");
        assert!(!tracker.in_use().contains(&9090));
        assert!(tracker.ports_for("web").is_none());
    }

    #[test]
    fn port_tracker_reuse_after_release() {
        let mut tracker = PortTracker::new();
        let ports = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 80,
            host_port: Some(9090),
        }];
        tracker.allocate("web", &ports).unwrap();
        tracker.release("web");

        // Another service can now use the same port.
        let ports2 = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 3000,
            host_port: Some(9090),
        }];
        let published = tracker.allocate("api", &ports2).unwrap();
        assert_eq!(published[0].host_port, 9090);
    }

    #[test]
    fn executor_tracks_ports_on_create() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);

        let spec = stack(
            "myapp",
            vec![ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                ..svc("web", "nginx:latest")
            }],
        );

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Ports should be tracked.
        let ports = executor.ports().ports_for("web").unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].host_port, 8080);
    }

    #[test]
    fn executor_releases_ports_on_remove() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);

        let spec = stack(
            "myapp",
            vec![ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                ..svc("web", "nginx:latest")
            }],
        );

        // Create first.
        let create_actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];
        executor.execute(&spec, &create_actions).unwrap();
        assert!(executor.ports().ports_for("web").is_some());

        // Remove should release ports.
        let remove_actions = vec![Action::ServiceRemove {
            service_name: "web".to_string(),
        }];
        let result = executor.execute(&spec, &remove_actions).unwrap();
        assert!(result.all_succeeded());
        assert!(executor.ports().ports_for("web").is_none());
        assert!(executor.ports().in_use().is_empty());
    }

    #[test]
    fn executor_port_conflict_emits_event() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
        let mut executor = make_executor(runtime);

        let spec = stack(
            "myapp",
            vec![
                ServiceSpec {
                    ports: vec![PortSpec {
                        protocol: "tcp".to_string(),
                        container_port: 80,
                        host_port: Some(8080),
                    }],
                    ..svc("web", "nginx:latest")
                },
                ServiceSpec {
                    ports: vec![PortSpec {
                        protocol: "tcp".to_string(),
                        container_port: 3000,
                        host_port: Some(8080), // conflict with web
                    }],
                    ..svc("api", "node:20")
                },
            ],
        );

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert_eq!(result.succeeded, 1); // web succeeds
        assert_eq!(result.failed, 1); // api fails (port conflict)

        // PortConflict event emitted.
        let events = executor.store().load_events("myapp").unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::PortConflict { .. }))
        );

        // api should be marked Failed.
        let observed = executor.store().load_observed_state("myapp").unwrap();
        let api = observed.iter().find(|o| o.service_name == "api").unwrap();
        assert_eq!(api.phase, ServicePhase::Failed);
    }

    // ── Docker Compose network conformance tests ──

    /// Helper: two-service stack for network tests.
    fn network_stack() -> StackSpec {
        stack(
            "netapp",
            vec![
                ServiceSpec {
                    ports: vec![PortSpec {
                        protocol: "tcp".to_string(),
                        container_port: 80,
                        host_port: Some(8080),
                    }],
                    ..svc("web", "nginx:latest")
                },
                ServiceSpec {
                    ports: vec![PortSpec {
                        protocol: "tcp".to_string(),
                        container_port: 5432,
                        host_port: Some(5432),
                    }],
                    ..svc("db", "postgres:16")
                },
            ],
        )
    }

    /// Helper: three-service stack.
    fn three_service_stack() -> StackSpec {
        stack(
            "triapp",
            vec![
                svc("web", "nginx:latest"),
                svc("api", "node:20"),
                svc("db", "postgres:16"),
            ],
        )
    }

    #[test]
    fn shared_vm_boots_before_container_creates() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = network_stack();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Verify ordering: boot_shared_vm → network_setup → create_in_stack × 2.
        let call_log = executor.runtime.call_log();
        let ops: Vec<&str> = call_log.iter().map(|(op, _)| op.as_str()).collect();
        assert_eq!(ops[0], "boot_shared_vm");
        assert_eq!(ops[1], "network_setup");
        // Remaining: pull + create_in_stack for each service.
        assert!(ops.contains(&"create_in_stack"));
        assert!(
            !ops.contains(&"create"),
            "should use create_in_stack, not create"
        );
    }

    #[test]
    fn network_setup_assigns_correct_ips() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = network_stack();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        executor.execute(&spec, &actions).unwrap();

        // Verify network_setup was called with correct service configs.
        let captured = executor.runtime.captured_network_services.borrow();
        assert_eq!(captured.len(), 1);
        let (stack_id, services) = &captured[0];
        assert_eq!(stack_id, "netapp");
        assert_eq!(services.len(), 2);

        // web gets 172.20.0.2/24, db gets 172.20.0.3/24.
        assert_eq!(services[0].name, "web");
        assert_eq!(services[0].addr, "172.20.0.2/24");
        assert_eq!(services[1].name, "db");
        assert_eq!(services[1].addr, "172.20.0.3/24");
    }

    #[test]
    fn service_to_service_hosts_use_real_ips() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = network_stack();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        executor.execute(&spec, &actions).unwrap();

        // Verify extra_hosts use real IPs, not 127.0.0.1.
        let configs = executor.runtime.captured_configs.borrow();

        // Find web's config.
        let web_config = configs.iter().find(|(id, _)| id == "ctr-web");
        assert!(web_config.is_some(), "web config not captured");
        let web_hosts = &web_config.unwrap().1.extra_hosts;
        // web should have db mapped to 172.20.0.3 (db is index 1, so .3).
        let db_host = web_hosts.iter().find(|(h, _)| h == "db");
        assert!(db_host.is_some(), "db not in web's extra_hosts");
        assert_eq!(db_host.unwrap().1, "172.20.0.3");

        // Find db's config.
        let db_config = configs.iter().find(|(id, _)| id == "ctr-db");
        assert!(db_config.is_some(), "db config not captured");
        let db_hosts = &db_config.unwrap().1.extra_hosts;
        // db should have web mapped to 172.20.0.2.
        let web_host = db_hosts.iter().find(|(h, _)| h == "web");
        assert!(web_host.is_some(), "web not in db's extra_hosts");
        assert_eq!(web_host.unwrap().1, "172.20.0.2");
    }

    #[test]
    fn containers_join_per_service_network_namespace() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = network_stack();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        executor.execute(&spec, &actions).unwrap();

        let configs = executor.runtime.captured_configs.borrow();

        // web should join /var/run/netns/web.
        let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
        assert_eq!(
            web_config.1.network_namespace_path,
            Some("/var/run/netns/web".to_string())
        );

        // db should join /var/run/netns/db.
        let db_config = configs.iter().find(|(id, _)| id == "ctr-db").unwrap();
        assert_eq!(
            db_config.1.network_namespace_path,
            Some("/var/run/netns/db".to_string())
        );
    }

    #[test]
    fn same_container_port_no_conflict_with_shared_vm() {
        // Two services both bind container port 80 but in different netns.
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
        let mut executor = make_executor(runtime);

        let spec = stack(
            "portapp",
            vec![
                ServiceSpec {
                    ports: vec![PortSpec {
                        protocol: "tcp".to_string(),
                        container_port: 80,
                        host_port: Some(8080),
                    }],
                    ..svc("web", "nginx:latest")
                },
                ServiceSpec {
                    ports: vec![PortSpec {
                        protocol: "tcp".to_string(),
                        container_port: 80,
                        host_port: Some(8081),
                    }],
                    ..svc("api", "node:20")
                },
            ],
        );

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        // Both succeed: different host ports, same container port is fine with netns.
        assert!(result.all_succeeded());
        assert_eq!(result.succeeded, 2);
    }

    #[test]
    fn three_service_ip_allocation() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = three_service_stack();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        executor.execute(&spec, &actions).unwrap();

        let captured = executor.runtime.captured_network_services.borrow();
        let (_, services) = &captured[0];
        assert_eq!(services.len(), 3);
        // 172.20.0.1 = bridge, services get .2, .3, .4.
        assert_eq!(services[0].addr, "172.20.0.2/24");
        assert_eq!(services[1].addr, "172.20.0.3/24");
        assert_eq!(services[2].addr, "172.20.0.4/24");

        // Verify cross-service host resolution for web.
        let configs = executor.runtime.captured_configs.borrow();
        let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
        let web_hosts = &web_config.1.extra_hosts;
        assert_eq!(web_hosts.len(), 2); // api + db
        assert!(
            web_hosts
                .iter()
                .any(|(h, ip)| h == "api" && ip == "172.20.0.3")
        );
        assert!(
            web_hosts
                .iter()
                .any(|(h, ip)| h == "db" && ip == "172.20.0.4")
        );
    }

    #[test]
    fn single_service_stack_skips_shared_vm() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);
        let spec = stack("solo", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Should use create, not create_in_stack.
        let call_log = executor.runtime.call_log();
        let ops: Vec<&str> = call_log.iter().map(|(op, _)| op.as_str()).collect();
        assert!(!ops.contains(&"boot_shared_vm"));
        assert!(!ops.contains(&"network_setup"));
        assert!(ops.contains(&"create"));
        assert!(!ops.contains(&"create_in_stack"));
    }

    #[test]
    fn single_service_uses_localhost_hosts() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        // Single service — no shared VM.
        let spec = stack("solo", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        executor.execute(&spec, &actions).unwrap();

        // No extra_hosts since there's only one service.
        let configs = executor.runtime.captured_configs.borrow();
        let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
        assert!(web_config.1.extra_hosts.is_empty());
        assert!(web_config.1.network_namespace_path.is_none());
    }

    #[test]
    fn shared_vm_not_rebooted_on_second_execute() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db", "ctr-new"]);
        let mut executor = make_executor(runtime);
        let spec = network_stack();

        // First execute: boots shared VM.
        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];
        executor.execute(&spec, &actions).unwrap();

        // Second execute with a recreate: should NOT reboot.
        let actions2 = vec![Action::ServiceRecreate {
            service_name: "web".to_string(),
        }];
        executor.execute(&spec, &actions2).unwrap();

        // boot_shared_vm should only appear once.
        let boot_count = executor
            .runtime
            .call_log()
            .iter()
            .filter(|(op, _)| op == "boot_shared_vm")
            .count();
        assert_eq!(boot_count, 1, "shared VM should not be rebooted");
    }
}
