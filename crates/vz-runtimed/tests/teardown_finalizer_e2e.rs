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

fn config(root: &Path) -> RuntimedConfig {
    RuntimedConfig {
        state_store_path: root.join("state").join("stack-state.db"),
        runtime_data_dir: root.join("runtime"),
        socket_path: root.join("runtime").join("runtimed.sock"),
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

fn teardown_request(scope: &MachineWorkloadScope) -> runtime_v2::TeardownStackRequest {
    teardown_request_with_id(scope, REQUEST_ID)
}

fn teardown_request_with_id(
    scope: &MachineWorkloadScope,
    request_id: &str,
) -> runtime_v2::TeardownStackRequest {
    runtime_v2::TeardownStackRequest {
        metadata: Some(runtime_v2::RequestMetadata {
            request_id: request_id.to_string(),
            idempotency_key: String::new(),
            trace_id: "trace-runtimed-finalizer-e2e".to_string(),
        }),
        stack_name: scope.stack_id.clone(),
        remove_volumes: false,
        dry_run: false,
        scope: Some(vz_runtime_translate::machine_workload_scope_to_proto(scope)),
    }
}

async fn successful_apply(
    client: &mut runtime_v2::stack_service_client::StackServiceClient<Channel>,
    scope: &MachineWorkloadScope,
    compose_dir: &Path,
) -> runtime_v2::ApplyStackResponse {
    let response = client
        .apply_stack(tonic::Request::new(runtime_v2::ApplyStackRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: INSTALLED_APPLY_REQUEST_ID.to_string(),
                idempotency_key: String::new(),
                trace_id: "trace-runtimed-installed-product-apply".to_string(),
            }),
            stack_name: scope.stack_id.clone(),
            compose_yaml: r#"services:
  sleeper:
    image: alpine:latest
    command: ["sleep", "300"]
"#
            .to_string(),
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
    let daemon = Arc::new(RuntimeDaemon::start(config(&root)).expect("start production daemon"));
    assert_eq!(daemon.backend_name(), "macos-vz");

    if std::env::var(BOOT_STACK_ENV).as_deref() == Ok("1") {
        let identity = daemon
            .e2e_boot_stack_runtime(STACK_ID)
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
            .e2e_inspect_stack_runtime(STACK_ID)
            .await
            .expect("inspect replacement after stale refusal");
        write_json(
            Path::new(&path),
            &serde_json::to_value(&identity).expect("serialize survivor identity"),
        );
    }
    if daemon
        .e2e_inspect_stack_runtime(STACK_ID)
        .await
        .expect("inspect helper runtime for cleanup")
        .is_some()
    {
        daemon
            .e2e_shutdown_stack_runtime(STACK_ID)
            .await
            .expect("clean up helper shared Linux VM");
    }
}

struct HelperChild(Option<Child>);

impl HelperChild {
    fn spawn(
        root: &Path,
        stop_path: &Path,
        boot_stack: bool,
        identity_path: Option<&Path>,
        survivor_path: Option<&Path>,
        crash_marker: Option<&Path>,
    ) -> Self {
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
                .env("VZ_TEST_TEARDOWN_AFTER_EXACT_STOP_MARKER", path)
                .env("VZ_TEST_TEARDOWN_AFTER_EXACT_STOP_STACK", STACK_ID);
        }
        Self(Some(command.spawn().expect("spawn runtimed E2E helper")))
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
    let response = match client
        .teardown_stack(tonic::Request::new(teardown_request(scope)))
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
    let response = client
        .teardown_stack(tonic::Request::new(teardown_request_with_id(
            scope, request_id,
        )))
        .await
        .expect("teardown stream starts");
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
            return completion.response.expect("teardown response");
        }
    }
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
    let exact_stop_marker = root.join("after-exact-stop.json");
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
    wait_for_file(&exact_stop_marker, Duration::from_secs(90)).await;
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

    let stop_marker: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&exact_stop_marker).expect("read exact-stop marker"))
            .expect("decode exact-stop marker");
    write_json(
        &evidence_path,
        &json!({
            "schema_version": 1,
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
            "installed_product_daemon": installed_product_daemon
        }),
    );
}
