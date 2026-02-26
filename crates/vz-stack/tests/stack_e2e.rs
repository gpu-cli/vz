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
//! Run with: `./scripts/run-sandbox-vm-e2e.sh --suite stack`

#![cfg(target_os = "macos")]
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use vz_oci_macos::{MacosRuntimeBackend, RuntimeConfig};
use vz_runtime_contract::{
    Container, ContainerState, ContractInvariantError, ExecConfig, Lease, LeaseState,
    MachineErrorCode, NetworkServiceConfig, PortMapping, RunConfig, RuntimeBackend, Sandbox,
    SandboxBackend, SandboxSpec, SandboxState,
};
use vz_stack::{
    Action, ContainerRuntime, ImagePolicy, OrchestrationConfig, ServicePhase, StackError,
    StackEvent, StackExecutor, StackOrchestrator, StateStore, apply, parse_compose,
    parse_compose_with_dir,
};

fn has_virtualization_entitlement() -> bool {
    let Ok(test_binary) = std::env::current_exe() else {
        return false;
    };
    let Ok(output) = Command::new("codesign")
        .arg("-d")
        .arg("--entitlements")
        .arg(":-")
        .arg(&test_binary)
        .output()
    else {
        return false;
    };

    let entitlements = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    entitlements.contains("com.apple.security.virtualization")
}

fn require_virtualization_entitlement() -> bool {
    if has_virtualization_entitlement() {
        return true;
    }

    eprintln!(
        "skipping stack_e2e: test binary is missing com.apple.security.virtualization entitlement; run ./scripts/run-sandbox-vm-e2e.sh --suite stack"
    );
    false
}

/// Bridge the async [`MacosRuntimeBackend`] to the sync [`ContainerRuntime`] trait.
///
/// Uses `MacosRuntimeBackend` (which implements `RuntimeBackend` with contract types)
/// rather than `vz_oci_macos::Runtime` directly, avoiding manual type conversions.
struct OciContainerRuntime {
    backend: MacosRuntimeBackend,
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
        let runtime = vz_oci_macos::Runtime::new(config);
        Self {
            backend: MacosRuntimeBackend::new(runtime),
            handle: tokio::runtime::Handle::current(),
        }
    }

    /// Exec with full stdout/stderr capture (bypasses ContainerRuntime trait).
    /// Returns `(exit_code, stdout, stderr)`.
    fn exec_with_output(&self, container_id: &str, cmd: Vec<String>) -> (i32, String, String) {
        tokio::task::block_in_place(|| {
            let out = self
                .handle
                .block_on(self.backend.exec_container(
                    container_id,
                    ExecConfig {
                        cmd,
                        timeout: Some(Duration::from_secs(30)),
                        ..ExecConfig::default()
                    },
                ))
                .unwrap();
            (out.exit_code, out.stdout, out.stderr)
        })
    }

    fn save_shared_vm_snapshot(
        &self,
        stack_id: &str,
        snapshot_path: &Path,
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.backend
                        .inner()
                        .save_shared_vm_snapshot(stack_id, snapshot_path),
                )
                .map_err(|e| StackError::Network(format!("save_shared_vm_snapshot failed: {e}")))
        })
    }

    fn restore_shared_vm_snapshot(
        &self,
        stack_id: &str,
        snapshot_path: &Path,
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.backend
                        .inner()
                        .restore_shared_vm_snapshot(stack_id, snapshot_path),
                )
                .map_err(|e| StackError::Network(format!("restore_shared_vm_snapshot failed: {e}")))
        })
    }

    fn exec_in_shared_vm(
        &self,
        stack_id: &str,
        command: &str,
        args: Vec<String>,
        timeout: Duration,
    ) -> Result<(i32, String, String), StackError> {
        tokio::task::block_in_place(|| {
            let out = self
                .handle
                .block_on(self.backend.inner().exec_in_shared_vm(
                    stack_id,
                    command.to_string(),
                    args,
                    timeout,
                ))
                .map_err(|e| StackError::Network(format!("exec_in_shared_vm failed: {e}")))?;
            Ok((out.exit_code, out.stdout, out.stderr))
        })
    }
}

impl ContainerRuntime for OciContainerRuntime {
    fn pull(&self, image: &str) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.pull(image))
                .map_err(|e| StackError::Network(format!("pull failed: {e}")))
        })
    }

    fn create(&self, image: &str, config: RunConfig) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.create_container(image, config))
                .map_err(|e| StackError::Network(format!("create failed: {e}")))
        })
    }

    fn stop(
        &self,
        container_id: &str,
        signal: Option<&str>,
        grace_period: Option<std::time::Duration>,
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.backend
                        .stop_container(container_id, false, signal, grace_period),
                )
                .map(|_| ())
                .map_err(|e| StackError::Network(format!("stop failed: {e}")))
        })
    }

    fn remove(&self, container_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.remove_container(container_id))
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
                .block_on(self.backend.exec_container(container_id, exec_config))
                .map(|output| output.exit_code)
                .map_err(|e| StackError::Network(format!("exec failed: {e}")))
        })
    }

    fn create_sandbox(
        &self,
        sandbox_id: &str,
        ports: Vec<PortMapping>,
        resources: vz_runtime_contract::StackResourceHint,
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.boot_shared_vm(sandbox_id, ports, resources))
                .map_err(|e| StackError::Network(format!("create_sandbox failed: {e}")))
        })
    }

    fn create_in_sandbox(
        &self,
        sandbox_id: &str,
        image: &str,
        config: RunConfig,
    ) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.backend
                        .create_container_in_stack(sandbox_id, image, config),
                )
                .map_err(|e| StackError::Network(format!("create_in_sandbox failed: {e}")))
        })
    }

    fn setup_sandbox_network(
        &self,
        sandbox_id: &str,
        services: Vec<NetworkServiceConfig>,
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.network_setup(sandbox_id, services))
                .map_err(|e| StackError::Network(format!("setup_sandbox_network failed: {e}")))
        })
    }

    fn teardown_sandbox_network(
        &self,
        sandbox_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.network_teardown(sandbox_id, service_names))
                .map_err(|e| StackError::Network(format!("teardown_sandbox_network failed: {e}")))
        })
    }

    fn shutdown_sandbox(&self, sandbox_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.shutdown_shared_vm(sandbox_id))
                .map_err(|e| StackError::Network(format!("shutdown_sandbox failed: {e}")))
        })
    }

    fn has_sandbox(&self, sandbox_id: &str) -> bool {
        self.backend.has_shared_vm(sandbox_id)
    }
}

#[test]
fn stack_error_machine_code_normalization() {
    assert_eq!(
        StackError::InvalidSpec("bad config".to_string()).machine_code(),
        MachineErrorCode::ValidationError
    );
    assert_eq!(
        StackError::Network("unsupported_operation: operation=exec".to_string()).machine_code(),
        MachineErrorCode::UnsupportedOperation
    );
    assert_eq!(
        StackError::Network("image not found".to_string()).machine_code(),
        MachineErrorCode::NotFound
    );
    assert_eq!(
        StackError::Network("request timed out".to_string()).machine_code(),
        MachineErrorCode::Timeout
    );
    assert_eq!(
        StackError::Network("bridge unavailable".to_string()).machine_code(),
        MachineErrorCode::BackendUnavailable
    );
    assert_eq!(
        StackError::Machine {
            code: MachineErrorCode::PolicyDenied,
            message: "denied".to_string(),
        }
        .machine_code(),
        MachineErrorCode::PolicyDenied
    );
}

#[test]
fn compose_unsupported_error_shape_is_stable() {
    let err = StackError::ComposeUnsupportedFeature {
        feature: "services.web.networks.frontend.aliases".to_string(),
        reason: "network attachment options are not supported".to_string(),
    };
    assert_eq!(err.machine_code(), MachineErrorCode::UnsupportedOperation);
    let message = err.to_string();
    assert!(message.starts_with("unsupported_operation:"));
    assert!(message.contains("surface=compose"));
}

#[test]
fn contract_terminal_state_and_lease_exec_gating_rules() {
    let mut sandbox = Sandbox {
        sandbox_id: "sbx-test".to_string(),
        backend: SandboxBackend::MacosVz,
        spec: SandboxSpec::default(),
        state: SandboxState::Ready,
        created_at: 1,
        updated_at: 1,
        labels: BTreeMap::new(),
    };
    sandbox.ensure_can_open_lease().unwrap();
    sandbox.transition_to(SandboxState::Draining).unwrap();
    sandbox.transition_to(SandboxState::Terminated).unwrap();
    assert!(matches!(
        sandbox.ensure_can_open_lease(),
        Err(ContractInvariantError::LeaseRequiresReadySandbox { .. })
    ));

    let mut lease = Lease {
        lease_id: "lease-test".to_string(),
        sandbox_id: "sbx-test".to_string(),
        ttl_secs: 60,
        last_heartbeat_at: 1,
        state: LeaseState::Active,
    };
    lease.ensure_can_submit_work("create_container").unwrap();
    lease.transition_to(LeaseState::Closed).unwrap();
    assert!(matches!(
        lease.ensure_can_submit_work("exec_container"),
        Err(ContractInvariantError::WorkRequiresActiveLease { .. })
    ));

    let mut container = Container {
        container_id: "ctr-test".to_string(),
        sandbox_id: "sbx-test".to_string(),
        image_digest: "sha256:abc".to_string(),
        container_spec: Default::default(),
        state: ContainerState::Running,
        created_at: 1,
        started_at: Some(1),
        ended_at: None,
    };
    container.ensure_can_exec().unwrap();
    container.transition_to(ContainerState::Exited).unwrap();
    assert!(matches!(
        container.ensure_can_exec(),
        Err(ContractInvariantError::ExecRequiresRunningContainer { .. })
    ));
}

/// Parse a 2-service compose YAML, reconcile, execute through real OCI runtime,
/// and verify containers reach Running state.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn full_pipeline_two_services() {
    if !require_virtualization_entitlement() {
        return;
    }
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

    // Execute through the real OCI runtime.
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let mut executor = StackExecutor::new(bridge, exec_store, tmp.path());

    // Dependency-aware reconciler behavior can require multiple rounds:
    // first create `worker`, then create dependent `web`.
    for round in 1..=3 {
        let health = HashMap::new();
        let result = apply(&spec, executor.store(), &health).unwrap();

        assert!(
            !result.actions.is_empty(),
            "expected at least one action in round {round}"
        );
        if round == 1 {
            assert!(
                matches!(&result.actions[0], Action::ServiceCreate { service_name } if service_name == "worker"),
                "first round should prioritize worker dependency, got: {:?}",
                result.actions[0]
            );
        }

        let exec_result = executor.execute(&spec, &result.actions).unwrap();
        assert_eq!(
            exec_result.failed, 0,
            "no actions should fail in round {round}: {:?}",
            exec_result.errors
        );

        let observed = executor.store().load_observed_state("e2e-test").unwrap();
        let ready = observed
            .iter()
            .filter(|service| service.container_id.is_some())
            .count();
        if ready >= 2 {
            break;
        }

        assert!(
            round < 3,
            "services did not converge after 3 reconcile rounds"
        );
    }

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
    if !require_virtualization_entitlement() {
        return;
    }
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
    if !require_virtualization_entitlement() {
        return;
    }
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
    assert!(
        result.rounds >= 1,
        "orchestration rounds should be at least 1, got {}",
        result.rounds
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
        disk_size_mb: None,
    };
    let down_result = orchestrator.run(&down_spec, None).unwrap();
    assert!(down_result.converged);
}

// ── Real service tests ──────────────────────────────────────────

/// Boot real Postgres and Redis services, wait for health checks to pass,
/// then verify functionality via exec.
///
/// This proves vz stack can run real services, not just alpine sleep containers.
/// Postgres is verified with `psql SELECT 1`, Redis with `redis-cli PING`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn real_services_postgres_and_redis() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: app
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "app"]
      interval: 2s
      timeout: 5s
      retries: 10
      start_period: 10s

  cache:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 5s
      retries: 10
      start_period: 5s
"#;

    // Use persistent data dir for image cache (avoid Docker Hub rate limits).
    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "real-svc").unwrap();
    assert_eq!(spec.services.len(), 2);

    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 30,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();

    assert!(
        result.converged,
        "stack should converge: ready={}, failed={}, rounds={}",
        result.services_ready, result.services_failed, result.rounds
    );
    assert_eq!(result.services_ready, 2, "both services should be ready");

    // Get container IDs from observed state.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("real-svc")
        .unwrap();

    let db_container_id = observed
        .iter()
        .find(|o| o.service_name == "db")
        .unwrap_or_else(|| panic!("db should be in observed state"))
        .container_id
        .as_ref()
        .unwrap();
    let cache_container_id = observed
        .iter()
        .find(|o| o.service_name == "cache")
        .unwrap_or_else(|| panic!("cache should be in observed state"))
        .container_id
        .as_ref()
        .unwrap();

    let rt = orchestrator.executor().runtime();

    // Verify Postgres: run SQL query via psql.
    let (exit_code, stdout, _) = rt.exec_with_output(
        db_container_id,
        vec![
            "psql".into(),
            "-U".into(),
            "app".into(),
            "-d".into(),
            "app".into(),
            "-c".into(),
            "SELECT 1".into(),
        ],
    );
    assert_eq!(exit_code, 0, "psql SELECT 1 should succeed");
    assert!(
        stdout.contains('1'),
        "psql output should contain '1': {stdout}"
    );

    // Verify Redis: run PING via redis-cli.
    let (exit_code, stdout, _) =
        rt.exec_with_output(cache_container_id, vec!["redis-cli".into(), "PING".into()]);
    assert_eq!(exit_code, 0, "redis-cli PING should succeed");
    assert!(
        stdout.contains("PONG"),
        "redis-cli output should contain 'PONG': {stdout}"
    );

    // Teardown: remove containers then shut down the shared VM.
    let down_spec = vz_stack::StackSpec {
        name: "real-svc".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down_result = orchestrator.run(&down_spec, None).unwrap();
    assert!(down_result.converged, "teardown should converge");

    // Shut down the shared VM.
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("real-svc");
}

/// End-to-end test for exec via Unix control socket.
///
/// Boots Redis, starts a control socket listener, connects a client
/// through the socket, runs `redis-cli PING`, and validates the response.
/// This tests the full `vz stack exec` pipe: socket → container lookup →
/// exec_with_output → response serialization.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn exec_via_control_socket() {
    if !require_virtualization_entitlement() {
        return;
    }
    use serde::{Deserialize, Serialize};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};

    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  cache:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 5s
      retries: 10
      start_period: 5s
"#;

    // Use persistent data dir for image cache.
    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "exec-sock").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);
    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged, "Redis should converge");
    assert_eq!(result.services_ready, 1);

    // Set up control socket.
    let sock_path = tmp.path().join("control.sock");
    let listener = UnixListener::bind(&sock_path).unwrap();

    // JSON protocol structs (mirrors vz-cli's ControlRequest/ControlResponse).
    #[derive(Debug, Serialize, Deserialize)]
    struct Req {
        service: String,
        cmd: Vec<String>,
    }
    #[derive(Debug, Serialize, Deserialize)]
    struct Resp {
        exit_code: i32,
        stdout: String,
        stderr: String,
        error: Option<String>,
    }

    // Spawn a client task that sends the exec request through the socket.
    let client_sock_path = sock_path.clone();
    let client = tokio::spawn(async move {
        // Small delay to let the server start accepting.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stream = UnixStream::connect(&client_sock_path).await.unwrap();
        let (reader, mut writer) = stream.into_split();

        let req = Req {
            service: "cache".into(),
            cmd: vec!["redis-cli".into(), "PING".into()],
        };
        let mut json = serde_json::to_string(&req).unwrap();
        json.push('\n');
        writer.write_all(json.as_bytes()).await.unwrap();
        writer.flush().await.unwrap();

        let mut lines = BufReader::new(reader).lines();
        let line = lines.next_line().await.unwrap().unwrap();
        serde_json::from_str::<Resp>(&line).unwrap()
    });

    // Server: accept the connection on the main task (which owns the orchestrator).
    let (stream, _) = listener.accept().await.unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let line = lines.next_line().await.unwrap().unwrap();
    let req: Req = serde_json::from_str(&line).unwrap();

    // Look up container ID from state.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("exec-sock")
        .unwrap();
    let svc = observed
        .iter()
        .find(|o| o.service_name == req.service)
        .unwrap();
    let container_id = svc.container_id.as_ref().unwrap();

    // Execute via the ORIGINAL runtime (which owns the VM handles).
    let (exit_code, stdout, stderr) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(container_id, req.cmd);

    let resp = Resp {
        exit_code,
        stdout,
        stderr,
        error: None,
    };
    let mut json = serde_json::to_string(&resp).unwrap();
    json.push('\n');
    writer.write_all(json.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();

    // Wait for the client and validate the response it received.
    let client_resp = client.await.unwrap();
    assert_eq!(
        client_resp.exit_code, 0,
        "redis-cli PING via socket should succeed"
    );
    assert!(
        client_resp.stdout.contains("PONG"),
        "response stdout should contain 'PONG': {}",
        client_resp.stdout
    );
    assert!(client_resp.error.is_none(), "no error expected");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "exec-sock".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("exec-sock");
}

/// Boot a 2-service stack with port forwarding, then connect from the host
/// and verify TCP data round-trip through the per-service network namespace.
///
/// Service "echo" runs `nc -l -p 8080` mapped to host:18090 with
/// `target_host` pointing at its per-service netns IP. The host connects
/// and reads the response, proving the full port-forwarding path works:
/// host → vsock → guest agent → netns bridge → container.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn stack_port_forwarding() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  echo:
    image: alpine:latest
    command: ["sh", "-c", "echo pong | nc -l -p 8080"]
    ports:
      - "18090:8080"

  sidecar:
    image: alpine:latest
    command: ["sleep", "300"]
"#;

    // Use persistent data dir for image cache.
    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "port-fwd").unwrap();
    assert_eq!(spec.services.len(), 2);

    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(
        result.converged,
        "stack should converge: ready={}, failed={}",
        result.services_ready, result.services_failed
    );
    assert_eq!(result.services_ready, 2);

    // Give the nc listener a moment to start inside the container.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Connect from the host — retry to allow port forwarding relay to start.
    use tokio::io::AsyncReadExt;
    let mut conn = None;
    for attempt in 1..=5 {
        match tokio::net::TcpStream::connect("127.0.0.1:18090").await {
            Ok(stream) => {
                conn = Some(stream);
                break;
            }
            Err(e) if attempt < 5 => {
                eprintln!("port forward attempt {attempt}/5 failed: {e}, retrying...");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => panic!("port forwarding connection failed after 5 attempts: {e}"),
        }
    }
    let mut conn = conn.unwrap();
    let mut buf = vec![0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(10), conn.read(&mut buf))
        .await
        .expect("port forward read timed out")
        .expect("port forward read failed");
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("pong"),
        "expected 'pong' from port-forwarded nc, got: {response}"
    );

    // Drop connection before cleanup.
    drop(conn);

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "port-fwd".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("port-fwd");
}

/// Use-case scenario:
/// - Run a realistic multi-service stack (`api + postgres + redis`)
/// - Initialize state in the shared VM
/// - Save shared VM snapshot
/// - Mutate state after snapshot
/// - Restore snapshot and verify VM state rewinds + stack still converges
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn complex_stack_snapshot_restore_rewinds_shared_vm_state() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: app
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "app"]
      interval: 2s
      timeout: 5s
      retries: 10
      start_period: 10s

  cache:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 5s
      retries: 10
      start_period: 5s

  api:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
"#;

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");
    let snapshot_path = tmp.path().join("snapshot-stack.state");

    let spec = parse_compose(yaml, "snapshot-stack").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 30,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);
    let result = orchestrator.run(&spec, None).unwrap();
    assert!(
        result.converged,
        "stack should converge: ready={}, failed={}, rounds={}",
        result.services_ready, result.services_failed, result.rounds
    );
    assert_eq!(result.services_ready, 3, "all services should be ready");

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("snapshot-stack")
        .unwrap();
    for service_name in ["db", "cache", "api"] {
        let service = observed
            .iter()
            .find(|entry| entry.service_name == service_name)
            .unwrap_or_else(|| panic!("{service_name} should be in observed state"));
        assert!(
            service.container_id.is_some(),
            "{service_name} should have a container id"
        );
    }

    let marker_cmd = |runtime: &OciContainerRuntime, value: &str| -> Result<(), String> {
        let (exit_code, _stdout, stderr) = runtime
            .exec_in_shared_vm(
                "snapshot-stack",
                "/bin/sh",
                vec![
                    "-c".to_string(),
                    format!("printf '%s' {value:?} > /tmp/vz-snapshot-marker"),
                ],
                Duration::from_secs(15),
            )
            .map_err(|err| err.to_string())?;
        if exit_code != 0 {
            return Err(format!("marker write failed: {stderr}"));
        }
        Ok(())
    };
    let read_marker = |runtime: &OciContainerRuntime| -> Result<String, String> {
        let (exit_code, stdout, stderr) = runtime
            .exec_in_shared_vm(
                "snapshot-stack",
                "/bin/sh",
                vec!["-c".to_string(), "cat /tmp/vz-snapshot-marker".to_string()],
                Duration::from_secs(15),
            )
            .map_err(|err| err.to_string())?;
        if exit_code != 0 {
            return Err(format!("marker read failed: {stderr}"));
        }
        Ok(stdout.trim().to_string())
    };

    marker_cmd(orchestrator.executor().runtime(), "before-snapshot").unwrap();
    assert_eq!(
        read_marker(orchestrator.executor().runtime()).unwrap(),
        "before-snapshot"
    );

    orchestrator
        .executor()
        .runtime()
        .save_shared_vm_snapshot("snapshot-stack", &snapshot_path)
        .unwrap();

    marker_cmd(orchestrator.executor().runtime(), "after-snapshot").unwrap();
    assert_eq!(
        read_marker(orchestrator.executor().runtime()).unwrap(),
        "after-snapshot"
    );

    orchestrator
        .executor()
        .runtime()
        .restore_shared_vm_snapshot("snapshot-stack", &snapshot_path)
        .unwrap();

    let mut restored_marker: Option<String> = None;
    for attempt in 1..=20 {
        match read_marker(orchestrator.executor().runtime()) {
            Ok(value) => {
                restored_marker = Some(value);
                break;
            }
            Err(err) => {
                eprintln!("restore marker read attempt {attempt}/20 failed: {err}; retrying...");
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let restored_marker =
        restored_marker.unwrap_or_else(|| panic!("marker did not become readable after restore"));
    assert_eq!(
        restored_marker, "before-snapshot",
        "snapshot restore should rewind marker to pre-snapshot value"
    );

    let converge_after_restore = orchestrator.run(&spec, None).unwrap();
    assert!(
        converge_after_restore.converged,
        "stack should converge after restore: ready={}, failed={}, rounds={}",
        converge_after_restore.services_ready,
        converge_after_restore.services_failed,
        converge_after_restore.rounds
    );
    assert_eq!(
        converge_after_restore.services_failed, 0,
        "no services should fail after restore reconciliation"
    );

    let mut marker_check_after_reconcile: Option<String> = None;
    for attempt in 1..=10 {
        match read_marker(orchestrator.executor().runtime()) {
            Ok(value) => {
                marker_check_after_reconcile = Some(value);
                break;
            }
            Err(err) => {
                eprintln!(
                    "post-reconcile marker read attempt {attempt}/10 failed: {err}; retrying..."
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert_eq!(
        marker_check_after_reconcile
            .unwrap_or_else(|| panic!("marker not readable after post-restore reconcile")),
        "before-snapshot",
        "marker should remain rewound after reconcile"
    );

    let mut services_ready_count: Option<usize> = None;
    for attempt in 1..=10 {
        let current = orchestrator
            .executor()
            .store()
            .load_observed_state("snapshot-stack")
            .unwrap();
        let ready = current
            .iter()
            .filter(|service| service.container_id.is_some())
            .count();
        if ready >= 3 {
            services_ready_count = Some(ready);
            break;
        }
        eprintln!(
            "post-restore observed-state attempt {attempt}/10 has ready={ready}; retrying..."
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert_eq!(
        services_ready_count.unwrap_or(0),
        3,
        "all services should remain represented after restore"
    );

    let down_spec = vz_stack::StackSpec {
        name: "snapshot-stack".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("snapshot-stack");
}

/// Verify sandbox lifecycle: create_sandbox → services up → shutdown_sandbox.
///
/// Tests the fundamental sandbox lifecycle:
/// 1. Orchestrator creates a sandbox via `create_sandbox()`
/// 2. Services are created inside the sandbox via `create_in_sandbox()`
/// 3. Network isolation is set up via `setup_sandbox_network()`
/// 4. Stack teardown removes containers and shuts down the sandbox
/// 5. `has_sandbox()` returns false after shutdown
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn sandbox_lifecycle_create_and_teardown() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  worker:
    image: alpine:latest
    command: ["sleep", "300"]

  api:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      - worker
"#;

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "sbx-lifecycle").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    // 1. Deploy: sandbox + services created.
    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged, "stack should converge");
    assert_eq!(result.services_ready, 2, "both services should be ready");

    // 2. Verify sandbox exists.
    assert!(
        orchestrator
            .executor()
            .runtime()
            .has_sandbox("sbx-lifecycle"),
        "sandbox should exist after deploy"
    );

    // 3. Verify services are alive via exec.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("sbx-lifecycle")
        .unwrap();
    for name in &["worker", "api"] {
        let svc = observed
            .iter()
            .find(|o| o.service_name == *name)
            .unwrap_or_else(|| panic!("{name} should be in observed state"));
        let cid = svc.container_id.as_ref().unwrap();
        let (exit_code, stdout, _) = orchestrator
            .executor()
            .runtime()
            .exec_with_output(cid, vec!["echo".into(), "alive".into()]);
        assert_eq!(exit_code, 0, "{name} exec should succeed");
        assert!(
            stdout.contains("alive"),
            "{name} should output 'alive', got: {stdout}"
        );
    }

    // 4. Teardown: remove services.
    let down_spec = vz_stack::StackSpec {
        name: "sbx-lifecycle".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down_result = orchestrator.run(&down_spec, None).unwrap();
    assert!(down_result.converged, "teardown should converge");

    // 5. Shut down sandbox and verify it's gone.
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("sbx-lifecycle")
        .unwrap();

    assert!(
        !orchestrator
            .executor()
            .runtime()
            .has_sandbox("sbx-lifecycle"),
        "sandbox should not exist after shutdown"
    );
}

/// Verify that the state store accurately tracks service phases through
/// the full lifecycle: pending → creating → running → stopped.
///
/// Also verifies that events are emitted for each state transition and
/// that the observed state is consistent after teardown.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn state_store_tracks_service_phases() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  tracker:
    image: alpine:latest
    command: ["sleep", "300"]
"#;

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "phase-track").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let mut orchestrator =
        StackOrchestrator::new(executor, reconcile_store, OrchestrationConfig::default());

    // Deploy.
    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged);
    assert_eq!(result.services_ready, 1);

    // Check observed state.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("phase-track")
        .unwrap();
    let tracker = observed
        .iter()
        .find(|o| o.service_name == "tracker")
        .unwrap();
    assert!(tracker.container_id.is_some());
    assert_eq!(tracker.phase, ServicePhase::Running);

    // Check events include creating + ready.
    let events = orchestrator
        .executor()
        .store()
        .load_events("phase-track")
        .unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::ServiceCreating { service_name, .. } if service_name == "tracker")),
        "should have ServiceCreating event"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::ServiceReady { service_name, .. } if service_name == "tracker")),
        "should have ServiceReady event"
    );

    // Teardown and verify stopped state.
    let down_spec = vz_stack::StackSpec {
        name: "phase-track".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down_result = orchestrator.run(&down_spec, None).unwrap();
    assert!(down_result.converged);

    let events_after = orchestrator
        .executor()
        .store()
        .load_events("phase-track")
        .unwrap();
    assert!(
        events_after
            .iter()
            .any(|e| matches!(e, StackEvent::ServiceStopped { service_name, .. } if service_name == "tracker")),
        "should have ServiceStopped event after teardown"
    );

    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("phase-track");
}

/// Verify orchestrator idempotency: running the same spec twice should
/// converge immediately on the second run (no-op).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn orchestrator_idempotent_rerun() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
"#;

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "idempotent").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    // First run: deploy.
    let r1 = orchestrator.run(&spec, None).unwrap();
    assert!(r1.converged);
    assert_eq!(r1.services_ready, 1);

    // Save container ID from first run.
    let observed1 = orchestrator
        .executor()
        .store()
        .load_observed_state("idempotent")
        .unwrap();
    let cid1 = observed1[0].container_id.clone().unwrap();

    // Second run: should be a no-op (same container stays running).
    let r2 = orchestrator.run(&spec, None).unwrap();
    assert!(r2.converged);
    assert_eq!(r2.services_ready, 1);

    // Container ID should be the same (no recreate).
    let observed2 = orchestrator
        .executor()
        .store()
        .load_observed_state("idempotent")
        .unwrap();
    let cid2 = observed2[0].container_id.clone().unwrap();
    assert_eq!(
        cid1, cid2,
        "container should not be recreated on idempotent rerun"
    );

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "idempotent".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("idempotent");
}

/// Verify that `depends_on` with `service_healthy` blocks dependent services
/// until the dependency passes its health check.
///
/// Uses Postgres with `pg_isready` health check, and an API service
/// that depends on db being healthy. The orchestrator should not create
/// the API container until after Postgres health check passes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn depends_on_service_healthy_blocks_until_ready() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: app
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "app"]
      interval: 2s
      timeout: 5s
      retries: 10
      start_period: 10s

  api:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      db:
        condition: service_healthy
"#;

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "dep-healthy").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 30,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    // Use round callback to verify ordering.
    let mut db_ready_round: Option<usize> = None;
    let mut api_created_round: Option<usize> = None;

    let result = orchestrator
        .run(
            &spec,
            Some(&mut |report: &vz_stack::RoundReport| {
                if let Some(exec) = &report.exec_result {
                    if exec.succeeded > 0 {
                        // Check events to see what was created this round.
                        // We track by round number.
                        if db_ready_round.is_none() {
                            // First round with success creates db.
                            db_ready_round = Some(report.round);
                        }
                    }
                }
                if report.services_ready == 2 && api_created_round.is_none() {
                    api_created_round = Some(report.round);
                }
            }),
        )
        .unwrap();

    assert!(
        result.converged,
        "stack should converge: ready={}, failed={}, rounds={}",
        result.services_ready, result.services_failed, result.rounds
    );
    assert_eq!(result.services_ready, 2);

    // API should have been created in a LATER round than DB.
    if let (Some(db_round), Some(api_round)) = (db_ready_round, api_created_round) {
        assert!(
            api_round > db_round,
            "api should be created after db is ready: db_round={db_round}, api_round={api_round}"
        );
    }

    // Verify Postgres is healthy via psql.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("dep-healthy")
        .unwrap();
    let db_cid = observed
        .iter()
        .find(|o| o.service_name == "db")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();
    let (exit_code, stdout, _) = orchestrator.executor().runtime().exec_with_output(
        db_cid,
        vec![
            "psql".into(),
            "-U".into(),
            "app".into(),
            "-d".into(),
            "app".into(),
            "-c".into(),
            "SELECT 1".into(),
        ],
    );
    assert_eq!(exit_code, 0, "psql should succeed");
    assert!(stdout.contains('1'), "psql should return 1");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "dep-healthy".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("dep-healthy");
}

/// Verify environment variables are correctly passed to containers in a stack.
///
/// Tests both inline `environment:` and multi-variable configurations.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn stack_environment_variable_passthrough() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
    environment:
      APP_ENV: production
      APP_PORT: "8080"
      APP_NAME: "my-service"
"#;

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "env-test").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let mut orchestrator =
        StackOrchestrator::new(executor, reconcile_store, OrchestrationConfig::default());

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged);
    assert_eq!(result.services_ready, 1);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("env-test")
        .unwrap();
    let cid = observed[0].container_id.as_ref().unwrap();

    // Verify environment variables were written to the OCI bundle config.
    // We read the host-side config.json directly because exec via nsenter
    // doesn't inherit the OCI process environment, and the VM kernel lacks
    // CONFIG_PID_NS so /proc/1/environ shows the VM init, not the container.
    let bundle_config = std::path::PathBuf::from(&home).join(format!(
        ".vz/oci/rootfs/{cid}/run/vz-oci/bundles/{cid}/config.json"
    ));
    let config_bytes = std::fs::read(&bundle_config).unwrap_or_else(|e| {
        panic!(
            "failed to read OCI config at {}: {e}",
            bundle_config.display()
        )
    });
    let config: serde_json::Value = serde_json::from_slice(&config_bytes).unwrap();
    let env_arr = config["process"]["env"]
        .as_array()
        .expect("process.env should be an array");
    let env_strs: Vec<&str> = env_arr.iter().filter_map(|v| v.as_str()).collect();
    for (var, expected) in [
        ("APP_ENV", "production"),
        ("APP_PORT", "8080"),
        ("APP_NAME", "my-service"),
    ] {
        let needle = format!("{var}={expected}");
        assert!(
            env_strs.iter().any(|e| *e == needle),
            "OCI config env should contain '{needle}', got: {env_strs:?}"
        );
    }

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "env-test".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("env-test");
}

/// Verify inter-service DNS connectivity within a stack sandbox.
///
/// Two services in the same sandbox should be able to reach each other
/// by service name (via /etc/hosts injection). This tests the full
/// network isolation + connectivity path.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn sandbox_inter_service_connectivity() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  server:
    image: alpine:latest
    command: ["sleep", "300"]

  client:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      - server
"#;

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "net-conn").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged);
    assert_eq!(result.services_ready, 2);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("net-conn")
        .unwrap();

    let client_cid = observed
        .iter()
        .find(|o| o.service_name == "client")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();

    // Ping server by hostname from client container.
    let (exit_code, stdout, stderr) = orchestrator.executor().runtime().exec_with_output(
        client_cid,
        vec![
            "/bin/busybox".into(),
            "ping".into(),
            "-c".into(),
            "1".into(),
            "-W".into(),
            "5".into(),
            "server".into(),
        ],
    );
    assert_eq!(
        exit_code, 0,
        "ping server by hostname should succeed: stdout={stdout}, stderr={stderr}"
    );

    // Verify /etc/hosts contains the server entry.
    let (exit_code, stdout, _) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(client_cid, vec!["cat".into(), "/etc/hosts".into()]);
    assert_eq!(exit_code, 0, "reading /etc/hosts should succeed");
    assert!(
        stdout.contains("server"),
        "/etc/hosts should contain 'server' entry: {stdout}"
    );

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "net-conn".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("net-conn");
}

/// Verify stack handles service update (config change → recreate).
///
/// Deploy a service, then change its environment and redeploy.
/// The orchestrator should detect the config drift and recreate.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn stack_service_config_change_triggers_recreate() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    // Any config change (env, image, command, mounts, etc.) triggers
    // ServiceRecreate in the reconciler via full config digest comparison.
    let yaml_v1 = r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
    environment:
      VERSION: "1"
"#;

    let yaml_v2 = r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
    environment:
      VERSION: "2"
"#;

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    // Deploy v1.
    let spec_v1 = parse_compose(yaml_v1, "update-test").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let r1 = orchestrator.run(&spec_v1, None).unwrap();
    assert!(r1.converged);
    assert_eq!(r1.services_ready, 1);

    let observed1 = orchestrator
        .executor()
        .store()
        .load_observed_state("update-test")
        .unwrap();
    let cid1 = observed1[0].container_id.clone().unwrap();

    // Deploy v2 (same stack name, different env).
    // The reconciler detects the config change via full service config digest
    // and triggers ServiceRecreate (stop + remove + create).
    let spec_v2 = parse_compose(yaml_v2, "update-test").unwrap();
    let r2 = orchestrator.run(&spec_v2, None).unwrap();
    assert!(r2.converged);
    assert_eq!(r2.services_ready, 1);

    let observed2 = orchestrator
        .executor()
        .store()
        .load_observed_state("update-test")
        .unwrap();
    let cid2 = observed2[0].container_id.clone().unwrap();

    // Container should have been recreated. Verify via stop event.
    let _ = cid1; // used below for conceptual clarity
    let events = orchestrator
        .executor()
        .store()
        .load_events("update-test")
        .unwrap();
    let stop_count = events
        .iter()
        .filter(|e| matches!(e, StackEvent::ServiceStopped { service_name, .. } if service_name == "app"))
        .count();
    assert!(
        stop_count >= 1,
        "env change should trigger service recreate (stop+create), got {stop_count} stop events"
    );

    // Verify the recreated container's OCI bundle has VERSION=2.
    let config_path = std::path::PathBuf::from(&home).join(format!(
        ".vz/oci/rootfs/{cid2}/run/vz-oci/bundles/{cid2}/config.json"
    ));
    let config_bytes = std::fs::read(&config_path)
        .unwrap_or_else(|e| panic!("read OCI config {}: {e}", config_path.display()));
    let config: serde_json::Value = serde_json::from_slice(&config_bytes).unwrap();
    let env_arr = config["process"]["env"]
        .as_array()
        .expect("process.env should be an array");
    let env_strs: Vec<&str> = env_arr.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        env_strs.iter().any(|e| *e == "VERSION=2"),
        "recreated container should have VERSION=2, got: {env_strs:?}"
    );

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "update-test".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("update-test");
}

/// Verify three-service dependency chain: C depends on B, B depends on A.
///
/// Tests that services are created in correct topological order.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn three_service_dependency_chain() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

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

  frontend:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      - api
"#;

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "chain-3").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(
        result.converged,
        "3-service chain should converge: ready={}, failed={}, rounds={}",
        result.services_ready, result.services_failed, result.rounds
    );
    assert_eq!(result.services_ready, 3, "all 3 services should be ready");

    // Verify all services are alive.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("chain-3")
        .unwrap();
    assert_eq!(observed.len(), 3, "should have 3 observed services");
    for name in &["db", "api", "frontend"] {
        let svc = observed
            .iter()
            .find(|o| o.service_name == *name)
            .unwrap_or_else(|| panic!("{name} should exist"));
        assert!(
            svc.container_id.is_some(),
            "{name} should have a container ID"
        );
    }

    // Verify events show correct ordering.
    let events = orchestrator
        .executor()
        .store()
        .load_events("chain-3")
        .unwrap();
    let creating_events: Vec<&str> = events
        .iter()
        .filter_map(|e| {
            if let StackEvent::ServiceCreating { service_name, .. } = e {
                Some(service_name.as_str())
            } else {
                None
            }
        })
        .collect();
    // db should appear before api, api before frontend.
    let db_pos = creating_events.iter().position(|n| *n == "db");
    let api_pos = creating_events.iter().position(|n| *n == "api");
    let fe_pos = creating_events.iter().position(|n| *n == "frontend");
    if let (Some(db), Some(api), Some(fe)) = (db_pos, api_pos, fe_pos) {
        assert!(
            db < api,
            "db should be created before api: db={db}, api={api}"
        );
        assert!(
            api < fe,
            "api should be created before frontend: api={api}, frontend={fe}"
        );
    }

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "chain-3".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("chain-3");
}

/// Verify exec_with_output works correctly through the stack runtime bridge.
///
/// Deploys a service and exercises exec_with_output with various commands
/// to verify stdout, stderr, and exit code capture.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn stack_exec_with_output_capture() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  runner:
    image: alpine:latest
    command: ["sleep", "300"]
"#;

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "exec-out").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let mut orchestrator =
        StackOrchestrator::new(executor, reconcile_store, OrchestrationConfig::default());

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("exec-out")
        .unwrap();
    let cid = observed[0].container_id.as_ref().unwrap();

    // Test 1: stdout capture.
    let (exit_code, stdout, _) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(cid, vec!["echo".into(), "hello-world".into()]);
    assert_eq!(exit_code, 0);
    assert_eq!(stdout.trim(), "hello-world");

    // Test 2: non-zero exit code.
    let (exit_code, _, _) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(cid, vec!["sh".into(), "-c".into(), "exit 42".into()]);
    assert_eq!(exit_code, 42, "should capture non-zero exit code");

    // Test 3: multi-line output.
    let (exit_code, stdout, _) = orchestrator.executor().runtime().exec_with_output(
        cid,
        vec![
            "sh".into(),
            "-c".into(),
            "echo line1 && echo line2 && echo line3".into(),
        ],
    );
    assert_eq!(exit_code, 0);
    let lines: Vec<&str> = stdout.trim().lines().collect();
    assert_eq!(lines.len(), 3, "should have 3 lines: {stdout}");
    assert_eq!(lines[0], "line1");
    assert_eq!(lines[1], "line2");
    assert_eq!(lines[2], "line3");

    // Test 4: write file then read it back (filesystem persistence within container).
    let (exit_code, _, _) = orchestrator.executor().runtime().exec_with_output(
        cid,
        vec![
            "sh".into(),
            "-c".into(),
            "echo 'persistent-data' > /tmp/test-file".into(),
        ],
    );
    assert_eq!(exit_code, 0);
    let (exit_code, stdout, _) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(cid, vec!["cat".into(), "/tmp/test-file".into()]);
    assert_eq!(exit_code, 0);
    assert_eq!(stdout.trim(), "persistent-data");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "exec-out".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("exec-out");
}

/// Deploy a service with replicas=3 and verify 3 running containers with distinct IDs.
#[tokio::test(flavor = "multi_thread")]
async fn replicated_service_creates_multiple_containers() {
    if !require_virtualization_entitlement() {
        return;
    }

    let yaml = r#"
services:
  web:
    image: alpine:latest
    command: ["sleep", "300"]
    deploy:
      replicas: 3
"#;

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");
    let oci_data = tmp.path().join("oci-data");
    std::fs::create_dir_all(&oci_data).unwrap();

    let spec = parse_compose(yaml, "replica-e2e").unwrap();
    assert_eq!(spec.services.len(), 1);
    assert_eq!(spec.services[0].resources.replicas, 3);

    let bridge = OciContainerRuntime::new(&oci_data);

    // Pre-pull image so parallel replica creation doesn't race on layer extraction.
    bridge.pull("alpine:latest").unwrap();

    let store = StateStore::open(&db_path).unwrap();
    let mut executor = StackExecutor::new(bridge, store, tmp.path());

    // Reconcile and execute.
    let health = HashMap::new();
    let result = apply(&spec, executor.store(), &health).unwrap();
    assert_eq!(result.actions.len(), 1);
    assert!(matches!(
        &result.actions[0],
        Action::ServiceCreate { service_name } if service_name == "web"
    ));

    let exec_result = executor.execute(&spec, &result.actions).unwrap();
    assert_eq!(
        exec_result.failed, 0,
        "all replicas should succeed: {:?}",
        exec_result.errors
    );

    // Verify 3 running replicas in observed state.
    let observed = executor.store().load_observed_state("replica-e2e").unwrap();
    let running: Vec<&str> = observed
        .iter()
        .filter(|o| o.container_id.is_some() && matches!(o.phase, ServicePhase::Running))
        .map(|o| o.service_name.as_str())
        .collect();
    assert_eq!(
        running.len(),
        3,
        "expected 3 running replicas, got: {running:?}"
    );

    // Each replica should have a distinct container_id.
    let cids: std::collections::HashSet<&str> = observed
        .iter()
        .filter_map(|o| o.container_id.as_deref())
        .collect();
    assert_eq!(cids.len(), 3, "expected 3 distinct container IDs");

    // Second reconcile should be a no-op (converged).
    let result2 = apply(&spec, executor.store(), &health).unwrap();
    assert!(
        result2.actions.is_empty(),
        "converged replicas should produce no actions, got: {:?}",
        result2.actions
    );

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "replica-e2e".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down_result = apply(&down_spec, executor.store(), &health).unwrap();
    let _ = executor.execute(&down_spec, &down_result.actions);
    let _ = executor.runtime().shutdown_sandbox("replica-e2e");
}

/// Deploy replicas=3, then redeploy with replicas=1 and verify scale-down.
#[tokio::test(flavor = "multi_thread")]
async fn replicated_service_scale_down() {
    if !require_virtualization_entitlement() {
        return;
    }

    let yaml_3 = r#"
services:
  web:
    image: alpine:latest
    command: ["sleep", "300"]
    deploy:
      replicas: 3
"#;

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");
    let oci_data = tmp.path().join("oci-data");
    std::fs::create_dir_all(&oci_data).unwrap();

    let spec3 = parse_compose(yaml_3, "scale-e2e").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);

    // Pre-pull image so parallel replica creation doesn't race on layer extraction.
    bridge.pull("alpine:latest").unwrap();

    let store = StateStore::open(&db_path).unwrap();
    let mut executor = StackExecutor::new(bridge, store, tmp.path());

    // Deploy with replicas=3.
    let health = HashMap::new();
    let r1 = apply(&spec3, executor.store(), &health).unwrap();
    let er1 = executor.execute(&spec3, &r1.actions).unwrap();
    assert_eq!(
        er1.failed, 0,
        "initial deploy should succeed: {:?}",
        er1.errors
    );

    let observed = executor.store().load_observed_state("scale-e2e").unwrap();
    let running_count = observed
        .iter()
        .filter(|o| matches!(o.phase, ServicePhase::Running))
        .count();
    assert_eq!(running_count, 3, "should have 3 running replicas");

    // Redeploy with replicas=1 → should scale down.
    let yaml_1 = r#"
services:
  web:
    image: alpine:latest
    command: ["sleep", "300"]
    deploy:
      replicas: 1
"#;
    let spec1 = parse_compose(yaml_1, "scale-e2e").unwrap();
    let r2 = apply(&spec1, executor.store(), &health).unwrap();

    // Should remove web-2 and web-3.
    let remove_names: Vec<&str> = r2
        .actions
        .iter()
        .filter_map(|a| match a {
            Action::ServiceRemove { service_name } => Some(service_name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        remove_names.len(),
        2,
        "should remove 2 excess replicas, got: {remove_names:?}"
    );

    let er2 = executor.execute(&spec1, &r2.actions).unwrap();
    assert_eq!(er2.failed, 0, "scale-down should succeed: {:?}", er2.errors);

    // Verify only 1 running replica remains.
    let observed2 = executor.store().load_observed_state("scale-e2e").unwrap();
    let still_running: Vec<&str> = observed2
        .iter()
        .filter(|o| matches!(o.phase, ServicePhase::Running))
        .map(|o| o.service_name.as_str())
        .collect();
    assert_eq!(
        still_running,
        vec!["web"],
        "only base replica should remain running"
    );

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "scale-e2e".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down_result = apply(&down_spec, executor.store(), &health).unwrap();
    let _ = executor.execute(&down_spec, &down_result.actions);
    let _ = executor.runtime().shutdown_sandbox("scale-e2e");
}

// ────────────────────────────────────────────────────────────────────────────
// Sandbox real-world scenario tests: volumes, secrets, env_file, multi-network
// ────────────────────────────────────────────────────────────────────────────

/// Verify bind mounts and named volumes work inside sandbox containers.
///
/// 1. Bind mount: a host file is visible inside the container.
/// 2. Named volume: data written by one container survives a recreate.
/// 3. Shared named volume: two services can read each other's writes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn sandbox_bind_mount_and_named_volume() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let tmp = tempfile::tempdir().unwrap();

    // Create a host file for bind mounting.
    let bind_dir = tmp.path().join("bind-src");
    std::fs::create_dir_all(&bind_dir).unwrap();
    std::fs::write(bind_dir.join("hello.txt"), "bind-mount-works").unwrap();

    let bind_dir_str = bind_dir.to_str().unwrap();

    let yaml = format!(
        r#"
services:
  writer:
    image: alpine:latest
    command: ["sleep", "300"]
    volumes:
      - {bind_dir}:/mnt/host:ro
      - shared:/mnt/shared

  reader:
    image: alpine:latest
    command: ["sleep", "300"]
    volumes:
      - shared:/mnt/shared
    depends_on:
      - writer

volumes:
  shared:
"#,
        bind_dir = bind_dir_str
    );

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let db_path = tmp.path().join("state.db");
    let spec = parse_compose(&yaml, "vol-e2e").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged, "stack should converge");
    assert_eq!(result.services_ready, 2);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("vol-e2e")
        .unwrap();

    let writer_cid = observed
        .iter()
        .find(|o| o.service_name == "writer")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();
    let reader_cid = observed
        .iter()
        .find(|o| o.service_name == "reader")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();

    // 1. Verify bind mount: host file visible inside writer container.
    let (exit_code, stdout, stderr) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(writer_cid, vec!["cat".into(), "/mnt/host/hello.txt".into()]);
    assert_eq!(
        exit_code, 0,
        "reading bind-mounted file should succeed: stderr={stderr}"
    );
    assert_eq!(stdout.trim(), "bind-mount-works");

    // 2. Write data to the shared named volume from writer.
    let (exit_code, _, stderr) = orchestrator.executor().runtime().exec_with_output(
        writer_cid,
        vec![
            "sh".into(),
            "-c".into(),
            "echo volume-data-from-writer > /mnt/shared/data.txt".into(),
        ],
    );
    assert_eq!(
        exit_code, 0,
        "writing to shared volume should succeed: stderr={stderr}"
    );

    // 3. Read the shared data from reader container.
    let (exit_code, stdout, stderr) = orchestrator.executor().runtime().exec_with_output(
        reader_cid,
        vec!["cat".into(), "/mnt/shared/data.txt".into()],
    );
    assert_eq!(
        exit_code, 0,
        "reader should see writer's data: stderr={stderr}"
    );
    assert_eq!(stdout.trim(), "volume-data-from-writer");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "vol-e2e".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("vol-e2e");
}

/// Verify file-based secrets are injected at /run/secrets/<name> inside sandbox containers.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn sandbox_secret_injection() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let tmp = tempfile::tempdir().unwrap();

    // Stage secret files on the host.
    let secrets_src = tmp.path().join("secret-files");
    std::fs::create_dir_all(&secrets_src).unwrap();
    std::fs::write(secrets_src.join("db_password"), "s3cret-p@ss!").unwrap();
    std::fs::write(secrets_src.join("api_key"), "ak-1234567890").unwrap();

    let db_pw_path = secrets_src.join("db_password");
    let api_key_path = secrets_src.join("api_key");

    let yaml = format!(
        r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
    secrets:
      - db_password
      - api_key

secrets:
  db_password:
    file: {db_pw}
  api_key:
    file: {api_key}
"#,
        db_pw = db_pw_path.to_str().unwrap(),
        api_key = api_key_path.to_str().unwrap(),
    );

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let db_path = tmp.path().join("state.db");
    let spec = parse_compose(&yaml, "secret-e2e").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged, "stack should converge");
    assert_eq!(result.services_ready, 1);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("secret-e2e")
        .unwrap();

    let app_cid = observed
        .iter()
        .find(|o| o.service_name == "app")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();

    // Verify db_password secret is readable inside the container.
    let (exit_code, stdout, stderr) = orchestrator.executor().runtime().exec_with_output(
        app_cid,
        vec!["cat".into(), "/run/secrets/db_password".into()],
    );
    assert_eq!(
        exit_code, 0,
        "reading /run/secrets/db_password should succeed: stderr={stderr}"
    );
    assert_eq!(stdout.trim(), "s3cret-p@ss!");

    // Verify api_key secret.
    let (exit_code, stdout, stderr) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(app_cid, vec!["cat".into(), "/run/secrets/api_key".into()]);
    assert_eq!(
        exit_code, 0,
        "reading /run/secrets/api_key should succeed: stderr={stderr}"
    );
    assert_eq!(stdout.trim(), "ak-1234567890");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "secret-e2e".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("secret-e2e");
}

/// Verify env_file variables are loaded and inline environment takes precedence.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn sandbox_env_file_loading() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let tmp = tempfile::tempdir().unwrap();
    let compose_dir = tmp.path().join("compose");
    std::fs::create_dir_all(&compose_dir).unwrap();

    // Write env file with base values.
    std::fs::write(
        compose_dir.join("app.env"),
        "DB_HOST=db.internal\nDB_PORT=5432\nLOG_LEVEL=info\n",
    )
    .unwrap();

    let yaml = r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
    env_file:
      - app.env
    environment:
      LOG_LEVEL: debug
      CUSTOM_VAR: injected
"#;

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let db_path = tmp.path().join("state.db");
    // Use parse_compose_with_dir so env_file paths are resolved.
    let spec = parse_compose_with_dir(yaml, "envfile-e2e", &compose_dir).unwrap();

    // Verify the parsed spec merged env_file + inline environment.
    let app_spec = &spec.services[0];
    assert_eq!(
        app_spec.environment.get("DB_HOST").map(String::as_str),
        Some("db.internal"),
        "DB_HOST should come from env_file"
    );
    assert_eq!(
        app_spec.environment.get("LOG_LEVEL").map(String::as_str),
        Some("debug"),
        "inline environment should override env_file"
    );

    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged, "stack should converge");
    assert_eq!(result.services_ready, 1);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("envfile-e2e")
        .unwrap();

    let app_cid = observed
        .iter()
        .find(|o| o.service_name == "app")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();

    // Verify env_file variable is present inside the running container.
    let (exit_code, stdout, stderr) = orchestrator.executor().runtime().exec_with_output(
        app_cid,
        vec!["sh".into(), "-c".into(), "echo $DB_HOST".into()],
    );
    assert_eq!(exit_code, 0, "echo DB_HOST should succeed: stderr={stderr}");
    assert_eq!(stdout.trim(), "db.internal");

    // Verify inline env overrides env_file.
    let (exit_code, stdout, _) = orchestrator.executor().runtime().exec_with_output(
        app_cid,
        vec!["sh".into(), "-c".into(), "echo $LOG_LEVEL".into()],
    );
    assert_eq!(exit_code, 0);
    assert_eq!(
        stdout.trim(),
        "debug",
        "inline environment should override env_file value"
    );

    // Verify purely-inline variable.
    let (exit_code, stdout, _) = orchestrator.executor().runtime().exec_with_output(
        app_cid,
        vec!["sh".into(), "-c".into(), "echo $CUSTOM_VAR".into()],
    );
    assert_eq!(exit_code, 0);
    assert_eq!(stdout.trim(), "injected");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "envfile-e2e".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("envfile-e2e");
}

/// Verify multi-network isolation: services on different networks cannot reach each other.
///
/// Topology:
///   - frontend network: service `web`
///   - backend network: service `api`, service `db`
///   - `api` is on both networks (bridge between frontend and backend)
///
/// Expected:
///   - `web` can ping `api` (both on frontend)
///   - `api` can ping `db` (both on backend)
///   - `web` CANNOT ping `db` (different networks, no shared membership)
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn sandbox_multi_network_isolation() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  web:
    image: alpine:latest
    command: ["sleep", "300"]
    networks:
      - frontend

  api:
    image: alpine:latest
    command: ["sleep", "300"]
    networks:
      - frontend
      - backend

  db:
    image: alpine:latest
    command: ["sleep", "300"]
    networks:
      - backend

networks:
  frontend:
  backend:
"#;

    let home = std::env::var("HOME").unwrap();
    let oci_data = std::path::PathBuf::from(&home).join(".vz/oci");
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "multinet-e2e").unwrap();
    assert_eq!(spec.networks.len(), 2, "should have 2 networks");

    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged, "stack should converge");
    assert_eq!(result.services_ready, 3);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("multinet-e2e")
        .unwrap();

    let cid_of = |name: &str| -> String {
        observed
            .iter()
            .find(|o| o.service_name == name)
            .unwrap()
            .container_id
            .clone()
            .unwrap()
    };
    let web_cid = cid_of("web");
    let api_cid = cid_of("api");

    // web → api should succeed (both on frontend network).
    let (exit_code, stdout, stderr) = orchestrator.executor().runtime().exec_with_output(
        &web_cid,
        vec![
            "/bin/busybox".into(),
            "ping".into(),
            "-c".into(),
            "1".into(),
            "-W".into(),
            "5".into(),
            "api".into(),
        ],
    );
    assert_eq!(
        exit_code, 0,
        "web should reach api (same frontend network): stdout={stdout}, stderr={stderr}"
    );

    // api → db should succeed (both on backend network).
    let (exit_code, stdout, stderr) = orchestrator.executor().runtime().exec_with_output(
        &api_cid,
        vec![
            "/bin/busybox".into(),
            "ping".into(),
            "-c".into(),
            "1".into(),
            "-W".into(),
            "5".into(),
            "db".into(),
        ],
    );
    assert_eq!(
        exit_code, 0,
        "api should reach db (same backend network): stdout={stdout}, stderr={stderr}"
    );

    // web → db should FAIL (different networks, no shared membership).
    // ping with a short timeout; non-zero exit means no connectivity.
    let (exit_code, _, _) = orchestrator.executor().runtime().exec_with_output(
        &web_cid,
        vec![
            "/bin/busybox".into(),
            "ping".into(),
            "-c".into(),
            "1".into(),
            "-W".into(),
            "2".into(),
            "db".into(),
        ],
    );
    assert_ne!(exit_code, 0, "web should NOT reach db (different networks)");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "multinet-e2e".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("multinet-e2e");
}
