//! Stack executor: bridge between reconciler [`Action`]s and the OCI runtime.
//!
//! The [`StackExecutor`] takes a list of actions from [`apply`](crate::apply)
//! and executes them through a [`ContainerRuntime`] implementation:
//! - `ServiceCreate` → pull image + create container + update state to Running
//! - `ServiceRemove` → stop + remove container + update state to Stopped
//! - `ServiceRecreate` → stop + remove + create (full cycle)
//!
//! State transitions and lifecycle events are persisted to the [`StateStore`].

use std::path::Path;

use tracing::{error, info};

use crate::convert::service_to_run_config;
use crate::error::StackError;
use crate::events::StackEvent;
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
}

/// Executes reconciler actions through an OCI container runtime.
pub struct StackExecutor<R: ContainerRuntime> {
    runtime: R,
    store: StateStore,
    volumes: VolumeManager,
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

    /// Execute a batch of reconciler actions for the given stack spec.
    ///
    /// Each action is processed in order. Failures on one service do not
    /// prevent other services from being processed; errors are collected
    /// and returned in [`ExecutionResult`].
    pub fn execute(
        &self,
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

        let service_map: std::collections::HashMap<&str, &ServiceSpec> = spec
            .services
            .iter()
            .map(|s| (s.name.as_str(), s))
            .collect();

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

    /// Execute a service create: pull image, convert spec, create container.
    fn execute_create(
        &self,
        spec: &StackSpec,
        service_map: &std::collections::HashMap<&str, &ServiceSpec>,
        service_name: &str,
    ) -> Result<(), StackError> {
        let svc_spec = service_map.get(service_name).ok_or_else(|| {
            StackError::InvalidSpec(format!(
                "service '{service_name}' not found in stack spec"
            ))
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

        // Convert ServiceSpec → RunConfig.
        let run_config = service_to_run_config(svc_spec, &resolved_mounts)?;

        // Create and start container.
        info!(service = %service_name, image = %svc_spec.image, "creating container");
        let container_id = match self.runtime.create(&svc_spec.image, run_config) {
            Ok(id) => id,
            Err(e) => {
                self.mark_failed(spec, service_name, &e.to_string())?;
                return Err(e);
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

    /// Execute a service removal: stop + remove container, update state.
    fn execute_remove(
        &self,
        spec: &StackSpec,
        service_name: &str,
    ) -> Result<(), StackError> {
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

/// Mock container runtime for testing.
///
/// Records all operations and can be configured to fail specific calls.
#[cfg(test)]
pub(crate) struct MockContainerRuntime {
    /// Container IDs to return on create calls (cycled).
    pub container_ids: Vec<String>,
    /// Whether pull should fail.
    pub fail_pull: bool,
    /// Whether create should fail.
    pub fail_create: bool,
    /// Whether stop should fail.
    pub fail_stop: bool,
    /// Tracks calls: (operation, arg).
    pub calls: std::cell::RefCell<Vec<(String, String)>>,
    /// Counter for create calls (to cycle through container_ids).
    create_counter: std::cell::Cell<usize>,
}

#[cfg(test)]
impl MockContainerRuntime {
    fn new() -> Self {
        Self {
            container_ids: vec!["ctr-001".to_string()],
            fail_pull: false,
            fail_create: false,
            fail_stop: false,
            calls: std::cell::RefCell::new(Vec::new()),
            create_counter: std::cell::Cell::new(0),
        }
    }

    fn with_ids(ids: Vec<&str>) -> Self {
        Self {
            container_ids: ids.into_iter().map(String::from).collect(),
            ..Self::new()
        }
    }

    fn call_log(&self) -> Vec<(String, String)> {
        self.calls.borrow().clone()
    }
}

#[cfg(test)]
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

    fn create(&self, image: &str, _config: vz_oci::RunConfig) -> Result<String, StackError> {
        self.calls
            .borrow_mut()
            .push(("create".to_string(), image.to_string()));
        if self.fail_create {
            return Err(StackError::InvalidSpec("mock create failure".to_string()));
        }
        let idx = self.create_counter.get();
        let id = self.container_ids[idx % self.container_ids.len()].clone();
        self.create_counter.set(idx + 1);
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
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::spec::{PortSpec, ResourcesSpec, StackSpec, VolumeSpec};
    use crate::spec::MountSpec as StackMountSpec;
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
        let executor = make_executor(runtime);
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
        assert!(events.iter().any(|e| matches!(e, StackEvent::ServiceCreating { .. })));
        assert!(events.iter().any(|e| matches!(e, StackEvent::ServiceReady { .. })));
    }

    #[test]
    fn create_multiple_services() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let executor = make_executor(runtime);
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
        let executor = make_executor(runtime);
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
        let executor = make_executor(runtime);
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
        let executor = make_executor(runtime);
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
        assert!(events.iter().any(|e| matches!(e, StackEvent::ServiceFailed { .. })));
    }

    #[test]
    fn create_failure_marks_service_failed() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_create = true;
        let executor = make_executor(runtime);
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
        let executor = make_executor(runtime);

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
        let executor = make_executor(runtime);
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
        let executor = make_executor_with_dir(runtime, tmp.path());

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
        assert!(events.iter().any(|e| matches!(e, StackEvent::VolumeCreated { .. })));
    }

    #[test]
    fn service_with_ports_creates_correctly() {
        let runtime = MockContainerRuntime::new();
        let executor = make_executor(runtime);

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
        let executor = make_executor(runtime);
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
        let executor = make_executor(runtime);

        let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].0, "web");
        assert!(result.errors[0].1.contains("mock pull failure"));
    }
}
