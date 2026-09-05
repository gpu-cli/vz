#![cfg(target_os = "macos")]
#![cfg(feature = "e2e-test-hooks")]
#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hyper_util::rt::TokioIo;
use prost::Message;
use serde_json::json;
use sha2::{Digest, Sha256};
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use vz_runtime_contract::{
    Architecture, CapabilitySet, EnvironmentId, EnvironmentInstance, EnvironmentSpec,
    EnvironmentState, MachineCapability, MachineId, MachineIncarnation, MachineIncarnationId,
    MachineInstance, MachineProfile, MachineResources, MachineSpec, MachineState,
    MachineWorkloadScope, OperatingSystem, OwnedResourceKind, OwnershipRecord, ProjectDefinition,
    ProjectId, ProjectState, StackRuntimeIdentity, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};
use vz_runtime_proto::runtime_v2;
use vz_runtimed::{RuntimeDaemon, RuntimedConfig, serve_runtime_uds_with_shutdown};
use vz_stack::{StackEvent, StackSpec, StateStore, TeardownFinalizerStatus};

const HELPER_ENV: &str = "VZ_RUNTIMED_TEARDOWN_E2E_HELPER";
const ROOT_ENV: &str = "VZ_RUNTIMED_TEARDOWN_E2E_ROOT";
const BOOT_STACK_ENV: &str = "VZ_RUNTIMED_TEARDOWN_E2E_BOOT_STACK";
const IDENTITY_ENV: &str = "VZ_RUNTIMED_TEARDOWN_E2E_IDENTITY";
const STOP_ENV: &str = "VZ_RUNTIMED_TEARDOWN_E2E_STOP";
const SURVIVOR_ENV: &str = "VZ_RUNTIMED_TEARDOWN_E2E_SURVIVOR";
const STACK_ENV: &str = "VZ_RUNTIMED_TEARDOWN_E2E_STACK";
const RUNTIME_DATA_ENV: &str = "VZ_RUNTIMED_TEARDOWN_E2E_RUNTIME_DATA";
const EVIDENCE_ENV: &str = "VZ_RUNTIMED_TEARDOWN_FINALIZER_EVIDENCE";
const BUILD_PROFILE_ENV: &str = "VZ_RUNTIMED_TEARDOWN_BUILD_PROFILE";
const TEST_BINARY_SHA256_ENV: &str = "VZ_RUNTIMED_TEARDOWN_TEST_BINARY_SHA256";
const INSTALLED_DAEMON_ENV: &str = "VZ_RUNTIMED_TEARDOWN_INSTALLED_DAEMON";
const INSTALLED_DAEMON_SHA256_ENV: &str = "VZ_RUNTIMED_TEARDOWN_INSTALLED_DAEMON_SHA256";
const INSTALLED_DAEMON_FEATURES_ENV: &str = "VZ_RUNTIMED_TEARDOWN_INSTALLED_DAEMON_FEATURES";
const STACK_ID: &str = "runtimed-finalizer-e2e";
const REQUEST_ID: &str = "req-runtimed-finalizer-e2e";
const OPERATION_KEY: &str = "req:req-runtimed-finalizer-e2e";
const INSTALLED_STACK_ID: &str = "runtimed-installed-product-e2e";
const INSTALLED_APPLY_REQUEST_ID: &str = "req-runtimed-installed-product-apply";
const INSTALLED_TEARDOWN_REQUEST_ID: &str = "req-runtimed-installed-product-teardown";

#[derive(Clone, Copy)]
struct TeardownBoundaryCase {
    selector: &'static str,
    boundary: &'static str,
    resource: Option<&'static str>,
}

const TEARDOWN_BOUNDARY_CASES: &[TeardownBoundaryCase] = &[
    TeardownBoundaryCase {
        selector: "finalizer_reserved",
        boundary: "finalizer_reserved",
        resource: None,
    },
    TeardownBoundaryCase {
        selector: "service_runtime_cleanup:api#1",
        boundary: "service_runtime_cleanup",
        resource: Some("api#1"),
    },
    TeardownBoundaryCase {
        selector: "service_cleanup_committed:api#1",
        boundary: "service_cleanup_committed",
        resource: Some("api#1"),
    },
    TeardownBoundaryCase {
        selector: "service_runtime_cleanup:worker#1",
        boundary: "service_runtime_cleanup",
        resource: Some("worker#1"),
    },
    TeardownBoundaryCase {
        selector: "service_cleanup_committed:worker#1",
        boundary: "service_cleanup_committed",
        resource: Some("worker#1"),
    },
    TeardownBoundaryCase {
        selector: "allocator_released:api#1",
        boundary: "allocator_released",
        resource: Some("api#1"),
    },
    TeardownBoundaryCase {
        selector: "allocator_released:worker#1",
        boundary: "allocator_released",
        resource: Some("worker#1"),
    },
    TeardownBoundaryCase {
        selector: "empty_desired_state_persisted",
        boundary: "empty_desired_state_persisted",
        resource: None,
    },
    TeardownBoundaryCase {
        selector: "runtime_shutdown_before_progress",
        boundary: "runtime_shutdown_before_progress",
        resource: None,
    },
    TeardownBoundaryCase {
        selector: "volume_staged:cache",
        boundary: "volume_staged",
        resource: Some("cache"),
    },
    TeardownBoundaryCase {
        selector: "volume_purged:cache",
        boundary: "volume_purged",
        resource: Some("cache"),
    },
    TeardownBoundaryCase {
        selector: "volume_staged:data",
        boundary: "volume_staged",
        resource: Some("data"),
    },
    TeardownBoundaryCase {
        selector: "volume_purged:data",
        boundary: "volume_purged",
        resource: Some("data"),
    },
    TeardownBoundaryCase {
        selector: "disk_staged",
        boundary: "disk_staged",
        resource: None,
    },
    TeardownBoundaryCase {
        selector: "disk_purged",
        boundary: "disk_purged",
        resource: None,
    },
    TeardownBoundaryCase {
        selector: "terminal_transaction_before_commit",
        boundary: "terminal_transaction_before_commit",
        resource: None,
    },
    TeardownBoundaryCase {
        selector: "terminal_transaction_committed",
        boundary: "terminal_transaction_committed",
        resource: None,
    },
];

fn config(root: &Path) -> RuntimedConfig {
    config_with_runtime_data(root, root.join("runtime"))
}

fn config_with_runtime_data(root: &Path, runtime_data_dir: PathBuf) -> RuntimedConfig {
    RuntimedConfig {
        state_store_path: root.join("state").join("stack-state.db"),
        runtime_data_dir,
        socket_path: root.join("control").join("runtimed.sock"),
    }
}

fn install_stack_authority(store: &StateStore, stack_id: &str) -> MachineWorkloadScope {
    let project_id = ProjectId::new("prj-runtimed-finalizer-e2e").expect("project id");
    let environment_id = EnvironmentId::new("env-runtimed-finalizer-e2e").expect("environment id");
    let machine_id = MachineId::new("mch-runtimed-finalizer-e2e").expect("machine id");
    let incarnation_id =
        MachineIncarnationId::new("inc-runtimed-finalizer-e2e").expect("incarnation id");
    let scope = MachineWorkloadScope {
        schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
        project_id: project_id.clone(),
        environment_id: environment_id.clone(),
        machine_id: machine_id.clone(),
        machine_incarnation_id: incarnation_id.clone(),
        stack_id: stack_id.to_string(),
    };
    let target = TargetSpec {
        os: OperatingSystem::Linux,
        arch: Architecture::Aarch64,
        image: "local-vz-developer-bundle".to_string(),
        version: None,
        channel: None,
        digest: Some("sha256:runtimed-finalizer-e2e".to_string()),
    };
    let capabilities = CapabilitySet::new([
        MachineCapability::DockerEngine,
        MachineCapability::Compose,
        MachineCapability::Buildx,
    ]);
    let definition = ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: project_id.clone(),
        name: "runtimed-finalizer-e2e".to_string(),
        environment: EnvironmentSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            machines: vec![MachineSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                name: "linux".to_string(),
                profile: MachineProfile::Developer,
                target: target.clone(),
                resources: MachineResources::default(),
                requested_capabilities: capabilities.clone(),
                workspace: None,
            }],
            networks: Vec::new(),
            endpoints: Vec::new(),
        },
    };
    let environment = EnvironmentInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        environment_id: environment_id.clone(),
        project_id: project_id.clone(),
        name: "developer".to_string(),
        definition_digest: definition.digest().expect("definition digest"),
        state: EnvironmentState::Ready,
        lifecycle_generation: 1,
        active_operation_id: None,
        bindings: Vec::new(),
        machines: vec![MachineInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            machine_id: machine_id.clone(),
            environment_id: environment_id.clone(),
            name: "linux".to_string(),
            profile: MachineProfile::Developer,
            target,
            resources: MachineResources::default(),
            requested_capabilities: capabilities.clone(),
            negotiated_capabilities: capabilities,
            backend: None,
            incarnation: Some(MachineIncarnation {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                incarnation_id: incarnation_id.clone(),
                machine_id: machine_id.clone(),
                generation: 1,
                created_at: 1,
            }),
            state: MachineState::Ready,
            runtime_identity: None,
            legacy_sandbox_id: None,
        }],
        networks: Vec::new(),
        endpoints: Vec::new(),
        ownership: vec![
            OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::Machine,
                resource_id: machine_id.to_string(),
                environment_id: environment_id.clone(),
                machine_id: Some(machine_id.clone()),
            },
            OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::Incarnation,
                resource_id: incarnation_id.to_string(),
                environment_id,
                machine_id: Some(machine_id),
            },
        ],
        legacy_migration: None,
        created_at: 1,
        updated_at: 1,
    };
    store
        .save_project_state(&ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments: vec![environment],
        })
        .expect("save topology authority");
    store
        .reserve_stack_workload_owner(&scope, 1)
        .expect("reserve stack owner");
    store
        .save_desired_state(
            stack_id,
            &StackSpec {
                name: stack_id.to_string(),
                services: Vec::new(),
                networks: Vec::new(),
                volumes: Vec::new(),
                secrets: Vec::new(),
                disk_size_mb: None,
            },
        )
        .expect("save empty desired stack");
    scope
}

async fn connect_channel(socket_path: &Path) -> Channel {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let connector_path = socket_path.to_path_buf();
        let result = Endpoint::try_from("http://[::]:50051")
            .expect("endpoint")
            .connect_with_connector(service_fn(move |_: Uri| {
                let connector_path = connector_path.clone();
                async move {
                    tokio::net::UnixStream::connect(connector_path)
                        .await
                        .map(TokioIo::new)
                }
            }))
            .await;
        if let Ok(channel) = result {
            return channel;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "could not connect stack client to {}",
            socket_path.display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn connect_stack_client(
    socket_path: &Path,
) -> runtime_v2::stack_service_client::StackServiceClient<Channel> {
    runtime_v2::stack_service_client::StackServiceClient::new(connect_channel(socket_path).await)
}

fn teardown_request_with_options(
    scope: &MachineWorkloadScope,
    request_id: &str,
    remove_volumes: bool,
) -> runtime_v2::TeardownStackRequest {
    runtime_v2::TeardownStackRequest {
        metadata: Some(runtime_v2::RequestMetadata {
            request_id: request_id.to_string(),
            idempotency_key: String::new(),
            trace_id: "trace-runtimed-finalizer-e2e".to_string(),
        }),
        stack_name: scope.stack_id.clone(),
        remove_volumes,
        dry_run: false,
        scope: Some(vz_runtime_translate::machine_workload_scope_to_proto(scope)),
    }
}

async fn successful_apply(
    client: &mut runtime_v2::stack_service_client::StackServiceClient<Channel>,
    scope: &MachineWorkloadScope,
    compose_dir: &Path,
) -> runtime_v2::ApplyStackResponse {
    successful_apply_with(
        client,
        scope,
        compose_dir,
        INSTALLED_APPLY_REQUEST_ID,
        r#"services:
  sleeper:
    image: alpine:latest
    command: ["sleep", "300"]
"#,
    )
    .await
}

async fn successful_apply_with(
    client: &mut runtime_v2::stack_service_client::StackServiceClient<Channel>,
    scope: &MachineWorkloadScope,
    compose_dir: &Path,
    request_id: &str,
    compose_yaml: &str,
) -> runtime_v2::ApplyStackResponse {
    let response = client
        .apply_stack(tonic::Request::new(runtime_v2::ApplyStackRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: request_id.to_string(),
                idempotency_key: String::new(),
                trace_id: "trace-runtimed-installed-product-apply".to_string(),
            }),
            stack_name: scope.stack_id.clone(),
            compose_yaml: compose_yaml.to_string(),
            compose_dir: compose_dir.to_string_lossy().into_owned(),
            dry_run: false,
            detach: false,
            scope: Some(vz_runtime_translate::machine_workload_scope_to_proto(scope)),
        }))
        .await
        .expect("installed daemon apply stream starts");
    let mut stream = response.into_inner();
    loop {
        let event = stream
            .message()
            .await
            .expect("read installed daemon apply stream")
            .expect("installed daemon apply completion event");
        if let Some(runtime_v2::apply_stack_event::Payload::Completion(completion)) = event.payload
        {
            assert!(
                stream
                    .message()
                    .await
                    .expect("apply terminal status")
                    .is_none(),
                "apply emitted an event after its terminal completion"
            );
            return completion
                .response
                .expect("installed daemon apply response");
        }
    }
}

async fn wait_for_file(path: &Path, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if path.is_file() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("file was not created in time: {}", path.display());
}

async fn wait_for_json_file(path: &Path, timeout: Duration) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if let Ok(bytes) = std::fs::read(path)
            && let Ok(value) = serde_json::from_slice(&bytes)
        {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "complete JSON file was not created in time: {}",
        path.display()
    );
}

async fn wait_for_socket(path: &Path) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while tokio::time::Instant::now() < deadline {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("socket was not created in time: {}", path.display());
}

fn write_json(path: &Path, value: &serde_json::Value) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create JSON parent");
    }
    std::fs::write(
        path,
        serde_json::to_vec_pretty(value).expect("serialize JSON evidence"),
    )
    .expect("write JSON evidence");
}

async fn run_helper() {
    let root = PathBuf::from(std::env::var(ROOT_ENV).expect("helper root"));
    let stack_id = std::env::var(STACK_ENV).unwrap_or_else(|_| STACK_ID.to_string());
    let cfg = std::env::var_os(RUNTIME_DATA_ENV).map_or_else(
        || config(&root),
        |runtime_data_dir| config_with_runtime_data(&root, PathBuf::from(runtime_data_dir)),
    );
    let daemon = Arc::new(RuntimeDaemon::start(cfg).expect("start production daemon"));
    assert_eq!(daemon.backend_name(), "macos-vz");

    if std::env::var(BOOT_STACK_ENV).as_deref() == Ok("1") {
        let identity = daemon
            .e2e_boot_stack_runtime(&stack_id)
            .await
            .expect("boot real shared Linux VM");
        let identity_path = PathBuf::from(std::env::var(IDENTITY_ENV).expect("identity path"));
        write_json(
            &identity_path,
            &serde_json::to_value(identity).expect("serialize runtime identity"),
        );
    }

    let stop_path = PathBuf::from(std::env::var(STOP_ENV).expect("helper stop path"));
    let socket_path = daemon.socket_path().to_path_buf();
    let server_daemon = daemon.clone();
    serve_runtime_uds_with_shutdown(server_daemon, socket_path, async move {
        while !stop_path.is_file() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("serve production runtime daemon");

    if let Ok(path) = std::env::var(SURVIVOR_ENV) {
        let identity = daemon
            .e2e_inspect_stack_runtime(&stack_id)
            .await
            .expect("inspect replacement after stale refusal");
        write_json(
            Path::new(&path),
            &serde_json::to_value(&identity).expect("serialize survivor identity"),
        );
    }
    if daemon
        .e2e_inspect_stack_runtime(&stack_id)
        .await
        .expect("inspect helper runtime for cleanup")
        .is_some()
    {
        daemon
            .e2e_shutdown_stack_runtime(&stack_id)
            .await
            .expect("clean up helper shared Linux VM");
    }
}

struct HelperChild(Option<Child>);

impl HelperChild {
    fn command(root: &Path, stop_path: &Path) -> Command {
        let mut command = Command::new(std::env::current_exe().expect("current test executable"));
        command
            .arg("--ignored")
            .arg("--exact")
            .arg("teardown_finalizer_sigkill_restart_replacement_refusal")
            .arg("--nocapture")
            .env(HELPER_ENV, "1")
            .env(ROOT_ENV, root)
            .env(STOP_ENV, stop_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        command
    }

    fn spawn(
        root: &Path,
        stop_path: &Path,
        boot_stack: bool,
        identity_path: Option<&Path>,
        survivor_path: Option<&Path>,
        crash_marker: Option<&Path>,
    ) -> Self {
        let mut command = Self::command(root, stop_path);
        if boot_stack {
            command.env(BOOT_STACK_ENV, "1");
        }
        if let Some(path) = identity_path {
            command.env(IDENTITY_ENV, path);
        }
        if let Some(path) = survivor_path {
            command.env(SURVIVOR_ENV, path);
        }
        if let Some(path) = crash_marker {
            command
                .env("VZ_ENABLE_UNSAFE_E2E_FAULT_INJECTION", "1")
                .env(
                    "VZ_TEST_TEARDOWN_FINALIZER_BOUNDARY",
                    "runtime_shutdown_before_progress",
                )
                .env("VZ_TEST_TEARDOWN_FINALIZER_MARKER", path)
                .env("VZ_TEST_TEARDOWN_FINALIZER_STACK", STACK_ID);
        }
        Self(Some(command.spawn().expect("spawn runtimed E2E helper")))
    }

    fn spawn_for_boundary(
        root: &Path,
        runtime_data_dir: &Path,
        stop_path: &Path,
        stack_id: &str,
        selector: &str,
        marker_path: &Path,
        audit_path: &Path,
    ) -> Self {
        let mut command = Self::command(root, stop_path);
        command
            .env(STACK_ENV, stack_id)
            .env(RUNTIME_DATA_ENV, runtime_data_dir)
            .env("VZ_ENABLE_UNSAFE_E2E_FAULT_INJECTION", "1")
            .env("VZ_TEST_TEARDOWN_FINALIZER_BOUNDARY", selector)
            .env("VZ_TEST_TEARDOWN_FINALIZER_MARKER", marker_path)
            .env("VZ_TEST_TEARDOWN_FINALIZER_AUDIT_LOG", audit_path)
            .env("VZ_TEST_TEARDOWN_FINALIZER_STACK", stack_id);
        Self(Some(
            command.spawn().expect("spawn runtimed boundary helper"),
        ))
    }

    fn sigkill_and_wait(&mut self) {
        let mut child = self.0.take().expect("live helper child");
        child.kill().expect("SIGKILL helper process");
        let status = child.wait().expect("wait for SIGKILLed helper");
        assert!(!status.success(), "SIGKILLed helper unexpectedly succeeded");
    }

    async fn wait_success(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            let child = self.0.as_mut().expect("live helper child");
            if let Some(status) = child.try_wait().expect("poll helper process") {
                self.0 = None;
                assert!(status.success(), "helper exited unsuccessfully: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "helper did not exit in time");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for HelperChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct InstalledDaemonChild(Option<Child>);

impl InstalledDaemonChild {
    fn spawn(binary: &Path, cfg: &RuntimedConfig) -> Self {
        let child = Command::new(binary)
            .arg("--state-store-path")
            .arg(&cfg.state_store_path)
            .arg("--runtime-data-dir")
            .arg(&cfg.runtime_data_dir)
            .arg("--socket-path")
            .arg(&cfg.socket_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn staged installed vz-runtimed");
        Self(Some(child))
    }

    async fn terminate_and_wait(&mut self, timeout: Duration) {
        let pid = self.0.as_ref().expect("live installed daemon").id();
        let signal = Command::new("/bin/kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .expect("signal installed daemon");
        assert!(signal.success(), "could not SIGTERM installed daemon");

        let deadline = Instant::now() + timeout;
        loop {
            let child = self.0.as_mut().expect("live installed daemon");
            if let Some(status) = child.try_wait().expect("poll installed daemon") {
                self.0 = None;
                assert!(
                    status.success(),
                    "installed daemon exited unsuccessfully: {status}"
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "installed daemon did not exit in time"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for InstalledDaemonChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn terminal_teardown_error(
    client: &mut runtime_v2::stack_service_client::StackServiceClient<Channel>,
    scope: &MachineWorkloadScope,
) -> tonic::Status {
    terminal_teardown_error_with_options(client, scope, REQUEST_ID, false).await
}

async fn terminal_teardown_error_with_options(
    client: &mut runtime_v2::stack_service_client::StackServiceClient<Channel>,
    scope: &MachineWorkloadScope,
    request_id: &str,
    remove_volumes: bool,
) -> tonic::Status {
    let response = match client
        .teardown_stack(tonic::Request::new(teardown_request_with_options(
            scope,
            request_id,
            remove_volumes,
        )))
        .await
    {
        Ok(response) => response,
        Err(status) => return status,
    };
    let mut stream = response.into_inner();
    loop {
        match stream.message().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("teardown stream completed without expected error"),
            Err(status) => return status,
        }
    }
}

struct TeardownCompletion {
    response: runtime_v2::TeardownStackResponse,
    response_bytes: Vec<u8>,
    receipt_id: String,
}

async fn successful_teardown(
    client: &mut runtime_v2::stack_service_client::StackServiceClient<Channel>,
    scope: &MachineWorkloadScope,
) -> runtime_v2::TeardownStackResponse {
    successful_teardown_with_id(client, scope, REQUEST_ID).await
}

async fn successful_teardown_with_id(
    client: &mut runtime_v2::stack_service_client::StackServiceClient<Channel>,
    scope: &MachineWorkloadScope,
    request_id: &str,
) -> runtime_v2::TeardownStackResponse {
    successful_teardown_with_options(client, scope, request_id, false)
        .await
        .response
}

async fn successful_teardown_with_options(
    client: &mut runtime_v2::stack_service_client::StackServiceClient<Channel>,
    scope: &MachineWorkloadScope,
    request_id: &str,
    remove_volumes: bool,
) -> TeardownCompletion {
    let response = client
        .teardown_stack(tonic::Request::new(teardown_request_with_options(
            scope,
            request_id,
            remove_volumes,
        )))
        .await
        .expect("teardown stream starts");
    let receipt_id = response
        .metadata()
        .get("x-receipt-id")
        .expect("teardown response receipt metadata")
        .to_str()
        .expect("teardown response receipt metadata text")
        .to_string();
    let mut stream = response.into_inner();
    loop {
        let event = stream
            .message()
            .await
            .expect("read teardown stream")
            .expect("teardown completion event");
        if let Some(runtime_v2::teardown_stack_event::Payload::Completion(completion)) =
            event.payload
        {
            let response = completion.response.expect("teardown response");
            assert!(
                stream
                    .message()
                    .await
                    .expect("teardown terminal status")
                    .is_none(),
                "teardown emitted an event after its terminal completion"
            );
            return TeardownCompletion {
                response_bytes: response.encode_to_vec(),
                response,
                receipt_id,
            };
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn read_boundary_audit(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .expect("read teardown boundary audit")
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).expect("decode boundary audit line")
        })
        .collect()
}

fn serialized_teardown_receipt(store: &StateStore, stack_id: &str, request_id: &str) -> Vec<u8> {
    let receipts = store
        .list_receipts_for_entity("stack", stack_id)
        .expect("list teardown receipts");
    let receipt = receipts
        .iter()
        .find(|receipt| receipt.request_id == request_id)
        .expect("durable teardown receipt");
    serde_json::to_vec(receipt).expect("serialize durable teardown receipt")
}

fn assert_initial_boundary_effect(case: TeardownBoundaryCase, event: &serde_json::Value) {
    match case.boundary {
        "service_runtime_cleanup" => {
            assert_eq!(event["details"]["outcome"], "stopped_and_removed")
        }
        "allocator_released" => assert_eq!(event["details"]["already_released"], false),
        "volume_staged" | "volume_purged" | "disk_staged" | "disk_purged" => {
            assert_eq!(event["details"]["mutated"], true)
        }
        "runtime_shutdown_before_progress" => {
            assert_eq!(event["details"]["outcome"], "stopped")
        }
        _ => {}
    }
}

fn assert_recovery_boundary_audit(case: TeardownBoundaryCase, audit: &[serde_json::Value]) {
    let retried = matches!(
        case.boundary,
        "finalizer_reserved"
            | "service_runtime_cleanup"
            | "allocator_released"
            | "empty_desired_state_persisted"
            | "volume_staged"
            | "volume_purged"
            | "disk_staged"
            | "disk_purged"
            | "terminal_transaction_before_commit"
    );
    assert_eq!(
        audit.len(),
        if retried { 2 } else { 1 },
        "{} boundary retry count",
        case.selector
    );
    if !retried {
        return;
    }
    let replay = &audit[1];
    assert_eq!(replay["boundary_id"], case.selector);
    match case.boundary {
        "service_runtime_cleanup" => {
            assert_eq!(replay["details"]["outcome"], "already_absent")
        }
        "allocator_released" => assert_eq!(replay["details"]["already_released"], true),
        "volume_staged" | "volume_purged" | "disk_staged" | "disk_purged" => {
            assert_eq!(replay["details"]["mutated"], false)
        }
        _ => {}
    }
}

fn durable_stack_snapshot(
    store: &StateStore,
    stack_id: &str,
    operation_key: &str,
) -> serde_json::Value {
    // Each daemon boot emits its own host-maintenance health receipts. Those
    // are independent of the workload request under test. Preserve every
    // workload and policy receipt so replay cannot hide a repeated admission.
    let receipts = store
        .list_receipts()
        .expect("load receipt snapshot")
        .into_iter()
        .filter(|receipt| receipt.entity_type != "maintenance")
        .collect::<Vec<_>>();
    let finalizer = store
        .load_teardown_finalizer(operation_key)
        .expect("load teardown finalizer snapshot");
    let session = finalizer
        .as_ref()
        .map(|record| {
            store
                .load_reconcile_session(&record.session_id)
                .expect("load teardown reconcile session snapshot")
        })
        .unwrap_or(None);
    let events = store
        .load_events_since(stack_id, 0)
        .expect("load event snapshot")
        .into_iter()
        .map(|record| {
            json!({
                "id": record.id,
                "stack_name": record.stack_name,
                "created_at": record.created_at,
                "event": record.event,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "finalizer": finalizer,
        "session": session,
        "desired": store.load_desired_state(stack_id).expect("load desired snapshot"),
        "observed": store.load_observed_state(stack_id).expect("load observed snapshot"),
        "allocator": store.load_allocator_state(stack_id).expect("load allocator snapshot"),
        "events": events,
        "receipts": receipts,
    })
}

fn assert_crash_boundary_progress(
    case: TeardownBoundaryCase,
    finalizer: &vz_stack::TeardownFinalizer,
) {
    assert_eq!(finalizer.changed_actions, 2, "{}", case.selector);
    assert_eq!(
        finalizer.initial_volumes,
        ["cache", "data"],
        "{}",
        case.selector
    );
    assert!(finalizer.initial_disk_image, "{}", case.selector);
    assert!(finalizer.initial_runtime_present, "{}", case.selector);

    let completed = case.boundary == "terminal_transaction_committed";
    assert_eq!(
        finalizer.status,
        if completed {
            TeardownFinalizerStatus::Completed
        } else {
            TeardownFinalizerStatus::Prepared
        },
        "{}",
        case.selector
    );

    let runtime_progress = matches!(
        case.selector,
        "volume_staged:cache"
            | "volume_purged:cache"
            | "volume_staged:data"
            | "volume_purged:data"
            | "disk_staged"
            | "disk_purged"
            | "terminal_transaction_before_commit"
            | "terminal_transaction_committed"
    );
    assert_eq!(
        finalizer.runtime_shutdown, runtime_progress,
        "{}",
        case.selector
    );

    let (staged, purged, disk_staged, disk_purged): (&[&str], &[&str], bool, bool) = match case
        .selector
    {
        "volume_purged:cache" => (&["cache"], &[], false, false),
        "volume_staged:data" => (&["cache"], &["cache"], false, false),
        "volume_purged:data" => (&["cache", "data"], &["cache"], false, false),
        "disk_staged" => (&["cache", "data"], &["cache", "data"], false, false),
        "disk_purged" => (&["cache", "data"], &["cache", "data"], true, false),
        "terminal_transaction_before_commit" => {
            (&["cache", "data"], &["cache", "data"], true, true)
        }
        "terminal_transaction_committed" => (&["cache", "data"], &["cache", "data"], true, true),
        _ => (&[], &[], false, false),
    };
    assert_eq!(
        finalizer.staged_volumes,
        staged
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>(),
        "{}",
        case.selector
    );
    assert_eq!(
        finalizer.purged_volumes,
        purged
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>(),
        "{}",
        case.selector
    );
    assert_eq!(finalizer.disk_staged, disk_staged, "{}", case.selector);
    assert_eq!(finalizer.disk_purged, disk_purged, "{}", case.selector);
}

const MATRIX_COMPOSE: &str = r#"services:
  api:
    image: alpine:latest
    command: ["sleep", "300"]
    volumes:
      - cache:/var/cache/vz
  worker:
    image: alpine:latest
    command: ["sleep", "300"]
    volumes:
      - data:/var/lib/vz
volumes:
  cache:
  data:
x-vz:
  disk_size: "512m"
"#;

async fn run_teardown_boundary_matrix(
    root: &Path,
    shared_runtime_data: &Path,
) -> serde_json::Value {
    let mut cases = Vec::with_capacity(TEARDOWN_BOUNDARY_CASES.len());

    for (index, case) in TEARDOWN_BOUNDARY_CASES.iter().copied().enumerate() {
        // macOS Unix sockets have a short sockaddr_un path limit. Keep the
        // case directory compact; the evidence carries the full boundary ID.
        let case_root = root.join(format!("b{index:02}"));
        std::fs::create_dir_all(&case_root).expect("create teardown matrix case root");
        let stack_id = format!("runtimed-finalizer-matrix-{index:02}");
        let request_id = format!("req-runtimed-finalizer-matrix-{index:02}");
        let operation_key = format!("req:{request_id}");
        let cfg = config_with_runtime_data(&case_root, shared_runtime_data.to_path_buf());
        std::fs::create_dir_all(
            cfg.state_store_path
                .parent()
                .expect("matrix state store has parent"),
        )
        .expect("create matrix state store directory");
        let scope = {
            let store = StateStore::open(&cfg.state_store_path).expect("open matrix state store");
            install_stack_authority(&store, &stack_id)
        };

        let crash_stop = case_root.join("crash.stop");
        let marker_path = case_root.join("boundary.json");
        let audit_path = case_root.join("boundary-audit.jsonl");
        let mut crashing = HelperChild::spawn_for_boundary(
            &case_root,
            shared_runtime_data,
            &crash_stop,
            &stack_id,
            case.selector,
            &marker_path,
            &audit_path,
        );
        wait_for_socket(&cfg.socket_path).await;
        let mut crash_client = connect_stack_client(&cfg.socket_path).await;
        let apply = successful_apply_with(
            &mut crash_client,
            &scope,
            &case_root,
            &format!("req-runtimed-finalizer-matrix-apply-{index:02}"),
            MATRIX_COMPOSE,
        )
        .await;
        assert_eq!(apply.changed_actions, 2, "{}", case.selector);
        assert_eq!(apply.services_ready, 2, "{}", case.selector);

        let interrupted_scope = scope.clone();
        let interrupted_request_id = request_id.clone();
        let interrupted = tokio::spawn(async move {
            terminal_teardown_error_with_options(
                &mut crash_client,
                &interrupted_scope,
                &interrupted_request_id,
                true,
            )
            .await
        });
        let marker = wait_for_json_file(&marker_path, Duration::from_secs(120)).await;
        crashing.sigkill_and_wait();
        let transport_error = tokio::time::timeout(Duration::from_secs(30), interrupted)
            .await
            .expect("interrupted teardown stream did not terminate")
            .expect("join interrupted matrix teardown");
        assert!(
            matches!(
                transport_error.code(),
                tonic::Code::Unavailable | tonic::Code::Cancelled | tonic::Code::Unknown
            ),
            "{} lost acknowledgement returned {transport_error:?}",
            case.selector
        );

        assert_eq!(marker["boundary_id"], case.selector);
        assert_eq!(marker["stack_id"], stack_id);
        assert_eq!(marker["resource"], json!(case.resource));
        let audit_lines = read_boundary_audit(&audit_path);
        assert_eq!(audit_lines.len(), 1, "{}", case.selector);
        assert_eq!(audit_lines[0]["boundary_id"], case.selector);
        assert_initial_boundary_effect(case, &audit_lines[0]);

        let (crash_snapshot, crash_receipt_bytes) = {
            let store =
                StateStore::open(&cfg.state_store_path).expect("reopen crashed matrix state");
            let finalizer = store
                .load_teardown_finalizer(&operation_key)
                .expect("load crashed matrix finalizer")
                .expect("crashed matrix finalizer exists");
            assert_crash_boundary_progress(case, &finalizer);
            let crash_receipt_bytes = (case.boundary == "terminal_transaction_committed")
                .then(|| serialized_teardown_receipt(&store, &stack_id, &request_id));
            (
                durable_stack_snapshot(&store, &stack_id, &operation_key),
                crash_receipt_bytes,
            )
        };

        let conflict_stop = case_root.join("conflict.stop");
        let mut conflict_helper = HelperChild::spawn_for_boundary(
            &case_root,
            shared_runtime_data,
            &conflict_stop,
            &stack_id,
            case.selector,
            &marker_path,
            &audit_path,
        );
        wait_for_socket(&cfg.socket_path).await;
        let mut conflict_client = connect_stack_client(&cfg.socket_path).await;
        let conflict =
            terminal_teardown_error_with_options(&mut conflict_client, &scope, &request_id, false)
                .await;
        assert_eq!(conflict.code(), tonic::Code::FailedPrecondition);
        let conflict_detail =
            runtime_v2::ErrorDetail::decode(conflict.details()).expect("decode matrix conflict");
        assert_eq!(conflict_detail.code, "state_conflict");
        drop(conflict_client);
        std::fs::write(&conflict_stop, b"stop").expect("stop matrix conflict helper");
        conflict_helper.wait_success(Duration::from_secs(30)).await;
        assert_eq!(
            read_boundary_audit(&audit_path).len(),
            1,
            "{} conflicting payload reached the mutation boundary",
            case.selector
        );
        let post_conflict_snapshot = {
            let store = StateStore::open(&cfg.state_store_path).expect("open post-conflict state");
            durable_stack_snapshot(&store, &stack_id, &operation_key)
        };
        assert_eq!(
            post_conflict_snapshot, crash_snapshot,
            "{} conflicting replay mutated durable stack state",
            case.selector
        );

        let recovery_stop = case_root.join("recovery.stop");
        let mut recovery_helper = HelperChild::spawn_for_boundary(
            &case_root,
            shared_runtime_data,
            &recovery_stop,
            &stack_id,
            case.selector,
            &marker_path,
            &audit_path,
        );
        wait_for_socket(&cfg.socket_path).await;
        let mut recovery_client = connect_stack_client(&cfg.socket_path).await;
        let recovered =
            successful_teardown_with_options(&mut recovery_client, &scope, &request_id, true).await;
        let recovery_audit = read_boundary_audit(&audit_path);
        assert_recovery_boundary_audit(case, &recovery_audit);
        let recovered_receipt_bytes = {
            let store = StateStore::open(&cfg.state_store_path)
                .expect("open recovered matrix state for receipt");
            serialized_teardown_receipt(&store, &stack_id, &request_id)
        };
        if let Some(crash_receipt_bytes) = &crash_receipt_bytes {
            assert_eq!(crash_receipt_bytes, &recovered_receipt_bytes);
        }
        let pre_replay_snapshot = {
            let store = StateStore::open(&cfg.state_store_path)
                .expect("open recovered matrix state before exact replay");
            durable_stack_snapshot(&store, &stack_id, &operation_key)
        };
        let replayed =
            successful_teardown_with_options(&mut recovery_client, &scope, &request_id, true).await;
        assert_eq!(
            read_boundary_audit(&audit_path),
            recovery_audit,
            "{} completed replay re-entered teardown",
            case.selector
        );
        assert_eq!(recovered.response_bytes, replayed.response_bytes);
        assert_eq!(recovered.receipt_id, replayed.receipt_id);
        let replayed_receipt_bytes = {
            let store = StateStore::open(&cfg.state_store_path)
                .expect("open replayed matrix state for receipt");
            serialized_teardown_receipt(&store, &stack_id, &request_id)
        };
        assert_eq!(recovered_receipt_bytes, replayed_receipt_bytes);
        let post_replay_snapshot = {
            let store = StateStore::open(&cfg.state_store_path)
                .expect("open recovered matrix state after exact replay");
            durable_stack_snapshot(&store, &stack_id, &operation_key)
        };
        assert_eq!(
            post_replay_snapshot, pre_replay_snapshot,
            "{} completed exact replay mutated durable stack state",
            case.selector
        );
        assert_eq!(recovered.response.changed_actions, 2);
        assert_eq!(recovered.response.removed_volumes, 2);
        drop(recovery_client);
        std::fs::write(&recovery_stop, b"stop").expect("stop matrix recovery helper");
        recovery_helper.wait_success(Duration::from_secs(30)).await;

        let (receipt_count, destroyed_event_count, finalizer, final_snapshot) = {
            let store =
                StateStore::open(&cfg.state_store_path).expect("open completed matrix state");
            let finalizer = store
                .load_teardown_finalizer(&operation_key)
                .expect("load completed matrix finalizer")
                .expect("completed matrix finalizer exists");
            let receipts = store
                .list_receipts_for_entity("stack", &stack_id)
                .expect("list matrix stack receipts");
            let receipt_count = receipts
                .iter()
                .filter(|receipt| receipt.request_id == request_id)
                .count();
            let destroyed_event_count = store
                .load_events_since(&stack_id, 0)
                .expect("list matrix stack events")
                .iter()
                .filter(|record| matches!(&record.event, StackEvent::StackDestroyed { .. }))
                .count();
            let snapshot = durable_stack_snapshot(&store, &stack_id, &operation_key);
            (receipt_count, destroyed_event_count, finalizer, snapshot)
        };
        assert_eq!(finalizer.status, TeardownFinalizerStatus::Completed);
        assert!(finalizer.runtime_shutdown);
        assert_eq!(finalizer.staged_volumes, ["cache", "data"]);
        assert_eq!(finalizer.purged_volumes, ["cache", "data"]);
        assert!(finalizer.disk_staged && finalizer.disk_purged);
        assert_eq!(receipt_count, 1);
        assert_eq!(destroyed_event_count, 1);
        assert_eq!(
            finalizer
                .receipt
                .as_ref()
                .map(|receipt| receipt.receipt_id.as_str()),
            Some(recovered.receipt_id.as_str())
        );
        let stack_dir = shared_runtime_data.join("stacks").join(&stack_id);
        assert!(!stack_dir.join("volumes/cache").exists());
        assert!(!stack_dir.join("volumes/data").exists());
        assert!(!stack_dir.join("data.img").exists());

        cases.push(json!({
            "boundary_id": case.selector,
            "boundary": case.boundary,
            "resource": case.resource,
            "stack_id": stack_id,
            "request_id": request_id,
            "marker": marker,
            "boundary_audit": recovery_audit,
            "transport_code": format!("{:?}", transport_error.code()).to_lowercase(),
            "conflicting_request_code": conflict_detail.code,
            "conflicting_request_zero_write": post_conflict_snapshot == crash_snapshot,
            "completed_replay_zero_write": post_replay_snapshot == pre_replay_snapshot,
            "crash_snapshot": crash_snapshot,
            "terminal_snapshot": final_snapshot,
            "changed_actions": recovered.response.changed_actions,
            "removed_volumes": recovered.response.removed_volumes,
            "response_sha256": sha256_hex(&recovered.response_bytes),
            "replay_response_sha256": sha256_hex(&replayed.response_bytes),
            "durable_receipt_sha256": sha256_hex(&recovered_receipt_bytes),
            "replay_durable_receipt_sha256": sha256_hex(&replayed_receipt_bytes),
            "crash_durable_receipt_sha256": crash_receipt_bytes
                .as_ref()
                .map(|bytes| sha256_hex(bytes)),
            "receipt_id": recovered.receipt_id,
            "receipt_count": receipt_count,
            "destroyed_event_count": destroyed_event_count,
        }));
    }

    json!({
        "schema_version": 1,
        "required_boundaries": TEARDOWN_BOUNDARY_CASES
            .iter()
            .map(|case| case.selector)
            .collect::<Vec<_>>(),
        "executed": cases.len(),
        "shared_runtime_data": shared_runtime_data,
        "shared_image_reference": "alpine:latest",
        "cases": cases,
    })
}

async fn installed_product_daemon_roundtrip(root: &Path) -> serde_json::Value {
    let daemon_path = PathBuf::from(
        std::env::var(INSTALLED_DAEMON_ENV)
            .expect("VZ_RUNTIMED_TEARDOWN_INSTALLED_DAEMON is required"),
    );
    let daemon_sha256 = std::env::var(INSTALLED_DAEMON_SHA256_ENV)
        .expect("VZ_RUNTIMED_TEARDOWN_INSTALLED_DAEMON_SHA256 is required");
    let daemon_features: Vec<String> = serde_json::from_str(
        &std::env::var(INSTALLED_DAEMON_FEATURES_ENV)
            .expect("VZ_RUNTIMED_TEARDOWN_INSTALLED_DAEMON_FEATURES is required"),
    )
    .expect("decode installed daemon feature inventory");
    assert!(
        daemon_path.is_absolute(),
        "installed daemon path must be absolute"
    );
    assert!(
        daemon_path
            .components()
            .any(|component| component.as_os_str() == "staged-install"),
        "installed daemon must run from the staged-install tree"
    );
    assert!(
        daemon_features.is_empty(),
        "installed product daemon must have no test features"
    );
    assert_eq!(daemon_sha256.len(), 64);

    let installed_root = root.join("installed-product");
    let cfg = config(&installed_root);
    std::fs::create_dir_all(
        cfg.state_store_path
            .parent()
            .expect("installed state store has parent"),
    )
    .expect("create installed state store directory");
    let scope = {
        let store = StateStore::open(&cfg.state_store_path).expect("open installed state store");
        install_stack_authority(&store, INSTALLED_STACK_ID)
    };

    let mut daemon = InstalledDaemonChild::spawn(&daemon_path, &cfg);
    wait_for_socket(&cfg.socket_path).await;
    let channel = connect_channel(&cfg.socket_path).await;
    let mut capability_client =
        runtime_v2::capability_service_client::CapabilityServiceClient::new(channel.clone());
    let capability_response = capability_client
        .get_capabilities(tonic::Request::new(runtime_v2::GetCapabilitiesRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-runtimed-installed-product-capabilities".to_string(),
                idempotency_key: String::new(),
                trace_id: "trace-runtimed-installed-product-capabilities".to_string(),
            }),
        }))
        .await
        .expect("installed daemon capabilities");
    let backend = capability_response
        .metadata()
        .get("x-vz-runtimed-backend")
        .expect("installed daemon backend metadata")
        .to_str()
        .expect("installed daemon backend metadata text")
        .to_string();
    let daemon_id = capability_response
        .metadata()
        .get("x-vz-runtimed-id")
        .expect("installed daemon identity metadata")
        .to_str()
        .expect("installed daemon identity metadata text")
        .to_string();
    assert_eq!(backend, "macos-vz");
    assert!(!daemon_id.is_empty());

    let mut stack_client = runtime_v2::stack_service_client::StackServiceClient::new(channel);
    let applied = successful_apply(&mut stack_client, &scope, &installed_root).await;
    assert_eq!(applied.request_id, INSTALLED_APPLY_REQUEST_ID);
    assert_eq!(applied.stack_name, INSTALLED_STACK_ID);
    assert!(applied.changed_actions >= 1);
    assert!(applied.converged);
    assert_eq!(applied.services_ready, 1);
    assert_eq!(applied.services_failed, 0);
    assert_eq!(applied.services.len(), 1);
    assert!(!applied.services[0].container_id.is_empty());

    let running = stack_client
        .get_stack_status(tonic::Request::new(runtime_v2::GetStackStatusRequest {
            metadata: None,
            stack_name: INSTALLED_STACK_ID.to_string(),
            scope: Some(vz_runtime_translate::machine_workload_scope_to_proto(
                &scope,
            )),
        }))
        .await
        .expect("installed daemon stack status after apply")
        .into_inner();
    assert_eq!(running.services.len(), 1);
    assert!(running.services[0].ready);

    let torn_down =
        successful_teardown_with_id(&mut stack_client, &scope, INSTALLED_TEARDOWN_REQUEST_ID).await;
    assert_eq!(torn_down.request_id, INSTALLED_TEARDOWN_REQUEST_ID);
    assert_eq!(torn_down.stack_name, INSTALLED_STACK_ID);
    assert!(torn_down.changed_actions >= 1);

    let stopped = stack_client
        .get_stack_status(tonic::Request::new(runtime_v2::GetStackStatusRequest {
            metadata: None,
            stack_name: INSTALLED_STACK_ID.to_string(),
            scope: Some(vz_runtime_translate::machine_workload_scope_to_proto(
                &scope,
            )),
        }))
        .await
        .expect("installed daemon stack status after teardown")
        .into_inner();
    assert_eq!(
        stopped.services.len(),
        1,
        "status retains one terminal service record for auditability"
    );
    assert!(
        stopped
            .services
            .iter()
            .all(|service| service.phase == "stopped" && !service.ready)
    );
    assert!(
        stopped
            .services
            .iter()
            .all(|service| service.container_id.is_empty())
    );
    drop(stack_client);
    drop(capability_client);
    daemon.terminate_and_wait(Duration::from_secs(30)).await;

    let store = StateStore::open(&cfg.state_store_path).expect("reopen installed product state");
    let operation_key = format!("req:{INSTALLED_TEARDOWN_REQUEST_ID}");
    let finalizer = store
        .load_teardown_finalizer(&operation_key)
        .expect("load installed product finalizer")
        .expect("installed product finalizer exists");
    assert_eq!(finalizer.status, TeardownFinalizerStatus::Completed);
    assert!(finalizer.runtime_shutdown);
    let captured_identity = finalizer
        .initial_runtime_identity
        .expect("installed teardown captured exact runtime identity");
    let receipt_count = store
        .list_receipts_for_entity("stack", INSTALLED_STACK_ID)
        .expect("list installed teardown receipts")
        .iter()
        .filter(|receipt| receipt.request_id == INSTALLED_TEARDOWN_REQUEST_ID)
        .count();
    assert_eq!(receipt_count, 1);
    let destroyed_event_count = store
        .load_events_since(INSTALLED_STACK_ID, 0)
        .expect("list installed teardown events")
        .iter()
        .filter(|record| matches!(&record.event, StackEvent::StackDestroyed { .. }))
        .count();
    assert_eq!(destroyed_event_count, 1);

    json!({
        "executable_path": daemon_path,
        "sha256": daemon_sha256,
        "cargo_features": daemon_features,
        "codesign_verify_strict": true,
        "backend": backend,
        "daemon_id": daemon_id,
        "apply_request_id": applied.request_id,
        "apply_changed_actions": applied.changed_actions,
        "ready_services_before_teardown": running.services.len(),
        "teardown_request_id": torn_down.request_id,
        "teardown_changed_actions": torn_down.changed_actions,
        "retained_stopped_services_after_teardown": stopped.services.len(),
        "ready_services_after_teardown": stopped.services.iter().filter(|service| service.ready).count(),
        "live_container_ids_after_teardown": stopped.services.iter().filter(|service| !service.container_id.is_empty()).count(),
        "captured_runtime_identity": captured_identity,
        "finalizer_runtime_shutdown": finalizer.runtime_shutdown,
        "receipt_count": receipt_count,
        "destroyed_event_count": destroyed_event_count
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "boots signed production macOS VZ runtimed subprocesses and SIGKILLs one at an exact crash boundary"]
async fn teardown_finalizer_sigkill_restart_replacement_refusal() {
    if std::env::var(HELPER_ENV).as_deref() == Ok("1") {
        run_helper().await;
        return;
    }

    let evidence_path = PathBuf::from(
        std::env::var(EVIDENCE_ENV).expect("VZ_RUNTIMED_TEARDOWN_FINALIZER_EVIDENCE is required"),
    );
    let build_profile = std::env::var(BUILD_PROFILE_ENV).expect("release build profile evidence");
    let test_binary_sha256 =
        std::env::var(TEST_BINARY_SHA256_ENV).expect("test binary digest evidence");
    assert_eq!(build_profile, "release");
    assert_eq!(test_binary_sha256.len(), 64);
    let tmp = tempfile::tempdir().expect("E2E tempdir");
    let root = tmp.path();
    let cfg = config(root);
    std::fs::create_dir_all(
        cfg.state_store_path
            .parent()
            .expect("state store has parent"),
    )
    .expect("create state store directory");
    let scope = {
        let store = StateStore::open(&cfg.state_store_path).expect("open seed state store");
        install_stack_authority(&store, STACK_ID)
    };

    let first_stop = root.join("first.stop");
    let original_identity_path = root.join("original-identity.json");
    let exact_stop_marker = root.join("runtime-shutdown-before-progress.json");
    let mut first = HelperChild::spawn(
        root,
        &first_stop,
        true,
        Some(&original_identity_path),
        None,
        Some(&exact_stop_marker),
    );
    wait_for_socket(&cfg.socket_path).await;
    wait_for_file(&original_identity_path, Duration::from_secs(90)).await;
    let original: StackRuntimeIdentity = serde_json::from_slice(
        &std::fs::read(&original_identity_path).expect("read original identity"),
    )
    .expect("decode original identity");

    let mut first_client = connect_stack_client(&cfg.socket_path).await;
    let first_scope = scope.clone();
    let first_request =
        tokio::spawn(async move { terminal_teardown_error(&mut first_client, &first_scope).await });
    let stop_marker = wait_for_json_file(&exact_stop_marker, Duration::from_secs(90)).await;
    assert_eq!(
        stop_marker["boundary_id"],
        "runtime_shutdown_before_progress"
    );
    first.sigkill_and_wait();
    let transport_error = first_request.await.expect("join interrupted teardown");
    assert!(
        matches!(
            transport_error.code(),
            tonic::Code::Unavailable | tonic::Code::Cancelled | tonic::Code::Unknown
        ),
        "lost acknowledgement must surface as transport loss, got {transport_error:?}"
    );

    let prepared = StateStore::open(&cfg.state_store_path)
        .expect("reopen state after SIGKILL")
        .load_teardown_finalizer(OPERATION_KEY)
        .expect("load prepared finalizer")
        .expect("prepared finalizer exists");
    assert_eq!(prepared.status, TeardownFinalizerStatus::Prepared);
    assert!(!prepared.runtime_shutdown);
    assert_eq!(prepared.initial_runtime_identity.as_ref(), Some(&original));

    // A new daemon owns a newly booted runtime with the same logical stack ID.
    // The prepared teardown must compare identities and refuse to name-stop it.
    let replacement_stop = root.join("replacement.stop");
    let replacement_identity_path = root.join("replacement-identity.json");
    let survivor_path = root.join("replacement-survivor.json");
    let mut replacement = HelperChild::spawn(
        root,
        &replacement_stop,
        true,
        Some(&replacement_identity_path),
        Some(&survivor_path),
        None,
    );
    wait_for_socket(&cfg.socket_path).await;
    wait_for_file(&replacement_identity_path, Duration::from_secs(90)).await;
    let replacement_identity: StackRuntimeIdentity = serde_json::from_slice(
        &std::fs::read(&replacement_identity_path).expect("read replacement identity"),
    )
    .expect("decode replacement identity");
    assert_ne!(replacement_identity, original);

    let mut replacement_client = connect_stack_client(&cfg.socket_path).await;
    let refusal = terminal_teardown_error(&mut replacement_client, &scope).await;
    assert_eq!(refusal.code(), tonic::Code::FailedPrecondition);
    let detail = runtime_v2::ErrorDetail::decode(refusal.details()).expect("decode ErrorDetail");
    let details: BTreeMap<_, _> = detail.details.into_iter().collect();
    assert_eq!(detail.code, "state_conflict");
    assert_eq!(
        details.get("expected_runtime_incarnation_id"),
        Some(&original.incarnation_id)
    );
    assert_eq!(
        details.get("current_runtime_incarnation_id"),
        Some(&replacement_identity.incarnation_id)
    );

    drop(replacement_client);
    std::fs::write(&replacement_stop, b"stop").expect("request graceful helper stop");
    wait_for_file(&survivor_path, Duration::from_secs(30)).await;
    replacement.wait_success(Duration::from_secs(30)).await;
    let survivor: Option<StackRuntimeIdentity> = serde_json::from_slice(
        &std::fs::read(&survivor_path).expect("read replacement survivor evidence"),
    )
    .expect("decode replacement survivor evidence");
    assert_eq!(survivor.as_ref(), Some(&replacement_identity));

    // With the replacement intentionally removed, another production-daemon
    // restart classifies the original exact target as absent and completes the
    // same durable finalizer exactly once.
    let final_stop = root.join("final.stop");
    let mut final_helper = HelperChild::spawn(root, &final_stop, false, None, None, None);
    wait_for_socket(&cfg.socket_path).await;
    let mut final_client = connect_stack_client(&cfg.socket_path).await;
    let completion = successful_teardown(&mut final_client, &scope).await;
    assert_eq!(completion.request_id, REQUEST_ID);
    assert_eq!(completion.stack_name, STACK_ID);
    drop(final_client);
    std::fs::write(&final_stop, b"stop").expect("request final helper stop");
    final_helper.wait_success(Duration::from_secs(15)).await;

    let store = StateStore::open(&cfg.state_store_path).expect("open completed state");
    let completed = store
        .load_teardown_finalizer(OPERATION_KEY)
        .expect("load completed finalizer")
        .expect("completed finalizer exists");
    assert_eq!(completed.status, TeardownFinalizerStatus::Completed);
    assert!(completed.runtime_shutdown);
    let receipts = store
        .list_receipts_for_entity("stack", STACK_ID)
        .expect("list stack receipts");
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt.request_id == REQUEST_ID)
            .count(),
        1
    );

    let installed_product_daemon = installed_product_daemon_roundtrip(root).await;
    let teardown_boundary_matrix =
        run_teardown_boundary_matrix(root, &root.join("installed-product/runtime")).await;

    write_json(
        &evidence_path,
        &json!({
            "schema_version": 2,
            "scenario": "runtimed-teardown-finalizer-crash-reopen",
            "host_os": "macos",
            "machine_target_os": "linux",
            "backend": "macos-vz",
            "build_identity": {
                "profile": build_profile,
                "test_binary_sha256": test_binary_sha256
            },
            "original_runtime_identity": original,
            "replacement_runtime_identity": replacement_identity,
            "stop_boundary": stop_marker,
            "sigkill_after_stop_before_progress": true,
            "prepared_finalizer_reopened": true,
            "replacement_refusal_code": detail.code,
            "replacement_survived_refusal": survivor,
            "same_operation_completed_after_replacement_removal": true,
            "receipt_count": 1,
            "installed_product_daemon": installed_product_daemon,
            "teardown_boundary_matrix": teardown_boundary_matrix
        }),
    );
}
