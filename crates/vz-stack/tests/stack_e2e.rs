//! End-to-end stack tests exercising real Linux VMs via the full pipeline:
//! compose YAML → parse → reconcile → execute → verify.
//!
//! These tests boot real Linux VMs, pull real OCI images, and execute
//! the complete stack control plane through the OCI container runtime.
//!
//! Requirements:
//! - Apple Silicon Mac (arm64)
//! - Linux kernel artifacts installed (`~/.vz/linux/`)
//! - Network access for image pulls (first run only; cached after)
//!
//! Run with: `cargo nextest run -p vz-stack --test stack_e2e -- --ignored`

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use vz_oci::{ExecConfig, RuntimeConfig};
use vz_stack::{
    Action, ContainerRuntime, OrchestrationConfig, StackError, StackEvent, StackExecutor,
    StackOrchestrator, StateStore, apply, parse_compose,
};

/// Bridge the async `vz_oci::Runtime` to the sync `ContainerRuntime` trait.
struct OciContainerRuntime {
    runtime: vz_oci::Runtime,
    handle: tokio::runtime::Handle,
}

impl OciContainerRuntime {
    fn new(data_dir: &Path) -> Self {
        let config = RuntimeConfig {
            data_dir: data_dir.to_path_buf(),
            require_exact_agent_version: false,
            agent_ready_timeout: Duration::from_secs(15),
            exec_timeout: Duration::from_secs(30),
            ..RuntimeConfig::default()
        };
        Self {
            runtime: vz_oci::Runtime::new(config),
            handle: tokio::runtime::Handle::current(),
        }
    }
}

impl ContainerRuntime for OciContainerRuntime {
    fn pull(&self, image: &str) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.pull(image))
                .map(|id| id.0)
                .map_err(|e| StackError::Network(format!("pull failed: {e}")))
        })
    }

    fn create(&self, image: &str, config: vz_oci::RunConfig) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.create_container(image, config))
                .map_err(|e| StackError::Network(format!("create failed: {e}")))
        })
    }

    fn stop(&self, container_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.stop_container(container_id, false))
                .map(|_| ())
                .map_err(|e| StackError::Network(format!("stop failed: {e}")))
        })
    }

    fn remove(&self, container_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.remove_container(container_id))
                .map_err(|e| StackError::Network(format!("remove failed: {e}")))
        })
    }

    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError> {
        tokio::task::block_in_place(|| {
            let exec_config = ExecConfig {
                cmd: command.to_vec(),
                ..ExecConfig::default()
            };
            self.handle
                .block_on(self.runtime.exec_container(container_id, exec_config))
                .map(|output| output.exit_code)
                .map_err(|e| StackError::Network(format!("exec failed: {e}")))
        })
    }
}

/// Parse a 2-service compose YAML, reconcile, execute through real OCI runtime,
/// and verify containers reach Running state.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn full_pipeline_two_services() {
    let yaml = r#"
services:
  worker:
    image: alpine:latest
    command: ["sleep", "300"]

  web:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      - worker
"#;

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");
    let oci_data = tmp.path().join("oci-data");
    std::fs::create_dir_all(&oci_data).unwrap();

    // Parse compose.
    let spec = parse_compose(yaml, "e2e-test").unwrap();
    assert_eq!(spec.services.len(), 2);

    // Reconcile to get actions.
    let store = StateStore::open(&db_path).unwrap();
    let health = HashMap::new();
    let result = apply(&spec, &store, &health).unwrap();

    // Should have 2 create actions, worker first (web depends on worker).
    assert_eq!(result.actions.len(), 2);
    assert!(
        matches!(&result.actions[0], Action::ServiceCreate { service_name } if service_name == "worker"),
        "first action should create worker, got: {:?}",
        result.actions[0]
    );
    assert!(
        matches!(&result.actions[1], Action::ServiceCreate { service_name } if service_name == "web"),
        "second action should create web, got: {:?}",
        result.actions[1]
    );

    // Execute through the real OCI runtime.
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let mut executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let exec_result = executor.execute(&spec, &result.actions).unwrap();
    assert_eq!(
        exec_result.failed, 0,
        "no actions should fail: {:?}",
        exec_result.errors
    );
    assert_eq!(exec_result.succeeded, 2, "both services should succeed");

    // Verify observed state: both services running.
    let observed = executor.store().load_observed_state("e2e-test").unwrap();
    for name in &["worker", "web"] {
        let svc = observed
            .iter()
            .find(|o| o.service_name == *name)
            .unwrap_or_else(|| panic!("service '{name}' should be in observed state"));
        assert!(
            svc.container_id.is_some(),
            "service '{name}' should have a container ID"
        );
    }

    // Verify events were emitted.
    let events = executor.store().load_events("e2e-test").unwrap();
    let creating_count = events
        .iter()
        .filter(|e| matches!(e, StackEvent::ServiceCreating { .. }))
        .count();
    let ready_count = events
        .iter()
        .filter(|e| matches!(e, StackEvent::ServiceReady { .. }))
        .count();
    assert!(
        creating_count >= 2,
        "should have at least 2 creating events"
    );
    assert!(ready_count >= 2, "should have at least 2 ready events");

    // Exec a command inside the worker container to prove it's alive.
    let worker_id = observed
        .iter()
        .find(|o| o.service_name == "worker")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();
    let exit_code = executor
        .runtime()
        .exec(worker_id, &["echo".into(), "stack-e2e".into()])
        .unwrap();
    assert_eq!(exit_code, 0, "exec inside worker should succeed");

    // Teardown: stop and remove both containers.
    let down_actions: Vec<Action> = spec
        .services
        .iter()
        .map(|s| Action::ServiceRemove {
            service_name: s.name.clone(),
        })
        .collect();
    let down_result = executor.execute(&spec, &down_actions).unwrap();
    assert_eq!(
        down_result.failed, 0,
        "teardown should succeed: {:?}",
        down_result.errors
    );
}

/// Parse and reconcile, then execute a single service and verify exec works.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn single_service_exec() {
    let yaml = r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
    environment:
      MY_VAR: "hello-from-stack"
"#;

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");
    let oci_data = tmp.path().join("oci-data");
    std::fs::create_dir_all(&oci_data).unwrap();

    let spec = parse_compose(yaml, "exec-test").unwrap();
    let store = StateStore::open(&db_path).unwrap();
    let result = apply(&spec, &store, &HashMap::new()).unwrap();

    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let mut executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let exec_result = executor.execute(&spec, &result.actions).unwrap();
    assert_eq!(exec_result.failed, 0);

    // Exec into the container.
    let observed = executor.store().load_observed_state("exec-test").unwrap();
    let app = observed.iter().find(|o| o.service_name == "app").unwrap();
    let container_id = app.container_id.as_ref().unwrap();

    let exit_code = executor
        .runtime()
        .exec(container_id, &["echo".into(), "alive".into()])
        .unwrap();
    assert_eq!(exit_code, 0);

    // Cleanup.
    let down = vec![Action::ServiceRemove {
        service_name: "app".into(),
    }];
    executor.execute(&spec, &down).unwrap();
}

/// Exercise the orchestration loop: deploy 2 services through the
/// StackOrchestrator and verify convergence with real OCI containers.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn orchestrator_converges_two_services() {
    let yaml = r#"
services:
  db:
    image: alpine:latest
    command: ["sleep", "300"]

  api:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      - db
"#;

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");
    let oci_data = tmp.path().join("oci-data");
    std::fs::create_dir_all(&oci_data).unwrap();

    let spec = parse_compose(yaml, "orch-test").unwrap();

    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let mut orchestrator =
        StackOrchestrator::new(executor, reconcile_store, OrchestrationConfig::default());

    let result = orchestrator.run(&spec, None).unwrap();

    assert!(result.converged, "stack should converge");
    assert_eq!(result.services_ready, 2, "both services should be ready");
    assert_eq!(result.services_failed, 0, "no services should fail");
    assert_eq!(
        result.rounds, 1,
        "should converge in 1 round without health checks"
    );

    // Verify observed state through the executor's store.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("orch-test")
        .unwrap();
    assert_eq!(observed.len(), 2);
    for name in &["db", "api"] {
        let svc = observed
            .iter()
            .find(|o| o.service_name == *name)
            .unwrap_or_else(|| panic!("service '{name}' should be in observed state"));
        assert!(
            svc.container_id.is_some(),
            "service '{name}' should have a container ID"
        );
    }

    // Teardown through the orchestrator's executor.
    let down_spec = vz_stack::StackSpec {
        name: "orch-test".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
    };
    let down_result = orchestrator.run(&down_spec, None).unwrap();
    assert!(down_result.converged);
}
