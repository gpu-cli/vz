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

#![cfg(target_os = "macos")]
#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use vz_oci::{MacosRuntimeBackend, RuntimeConfig};
use vz_runtime_contract::{
    ExecConfig, NetworkServiceConfig, PortMapping, RunConfig, RuntimeBackend,
};
use vz_stack::{
    Action, ContainerRuntime, OrchestrationConfig, StackError, StackEvent, StackExecutor,
    StackOrchestrator, StateStore, apply, parse_compose,
};

/// Bridge the async [`MacosRuntimeBackend`] to the sync [`ContainerRuntime`] trait.
///
/// Uses `MacosRuntimeBackend` (which implements `RuntimeBackend` with contract types)
/// rather than `vz_oci::Runtime` directly, avoiding manual type conversions.
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
        let runtime = vz_oci::Runtime::new(config);
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

    fn stop(&self, container_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.stop_container(container_id, false))
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

    fn boot_shared_vm(&self, stack_id: &str, ports: &[PortMapping]) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.boot_shared_vm(stack_id, ports.to_vec()))
                .map_err(|e| StackError::Network(format!("boot_shared_vm failed: {e}")))
        })
    }

    fn network_setup(
        &self,
        stack_id: &str,
        services: &[NetworkServiceConfig],
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.network_setup(stack_id, services.to_vec()))
                .map_err(|e| StackError::Network(format!("network_setup failed: {e}")))
        })
    }

    fn network_teardown(&self, stack_id: &str, service_names: &[String]) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.backend
                        .network_teardown(stack_id, service_names.to_vec()),
                )
                .map_err(|e| StackError::Network(format!("network_teardown failed: {e}")))
        })
    }

    fn create_in_stack(
        &self,
        stack_id: &str,
        image: &str,
        config: RunConfig,
    ) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.backend
                        .create_container_in_stack(stack_id, image, config),
                )
                .map_err(|e| StackError::Network(format!("create_in_stack failed: {e}")))
        })
    }

    fn shutdown_shared_vm(&self, stack_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.shutdown_shared_vm(stack_id))
                .map_err(|e| StackError::Network(format!("shutdown_shared_vm failed: {e}")))
        })
    }

    fn has_shared_vm(&self, stack_id: &str) -> bool {
        self.backend.has_shared_vm(stack_id)
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

// ── Real service tests ──────────────────────────────────────────

/// Boot real Postgres and Redis services, wait for health checks to pass,
/// then verify functionality via exec.
///
/// This proves vz stack can run real services, not just alpine sleep containers.
/// Postgres is verified with `psql SELECT 1`, Redis with `redis-cli PING`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn real_services_postgres_and_redis() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci=debug,vz_linux=debug,vz_stack=debug")
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
    };
    let down_result = orchestrator.run(&down_spec, None).unwrap();
    assert!(down_result.converged, "teardown should converge");

    // Shut down the shared VM.
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_shared_vm("real-svc");
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
    use serde::{Deserialize, Serialize};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};

    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci=debug,vz_linux=debug,vz_stack=debug")
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
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_shared_vm("exec-sock");
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
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci=debug,vz_linux=debug,vz_stack=debug")
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
    };
    let _ = orchestrator.run(&down_spec, None);
    let _ = orchestrator
        .executor()
        .runtime()
        .shutdown_shared_vm("port-fwd");
}
