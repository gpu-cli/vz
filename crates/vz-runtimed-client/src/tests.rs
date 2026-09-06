use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tonic::Code;
use vz_runtimed::{RuntimeDaemon, RuntimedConfig, serve_runtime_uds_with_shutdown};

use super::*;
struct RunningDaemon {
    shutdown: Arc<Notify>,
    task: tokio::task::JoinHandle<std::result::Result<(), vz_runtimed::RuntimedServerError>>,
}

impl RunningDaemon {
    async fn stop(self) {
        self.shutdown.notify_waiters();
        let join = tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("server join timeout")
            .expect("server task join failed");
        assert!(join.is_ok());
    }
}

async fn wait_for_socket(path: &Path) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("socket was not created in time: {}", path.display());
}

async fn start_daemon(config: RuntimedConfig) -> RunningDaemon {
    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(Notify::new());
    let shutdown_task = shutdown.clone();
    let socket_path = config.socket_path.clone();
    let task = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;
    RunningDaemon { shutdown, task }
}

fn runtimed_config(tmp: &tempfile::TempDir) -> RuntimedConfig {
    RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    }
}

fn client_config(tmp: &tempfile::TempDir, auto_spawn: bool) -> DaemonClientConfig {
    let daemon = runtimed_config(tmp);
    DaemonClientConfig {
        socket_path: daemon.socket_path,
        auto_spawn,
        startup_timeout: Duration::from_secs(3),
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_millis(500),
        retry_backoff: Duration::from_millis(30),
        max_retry_backoff: Duration::from_millis(120),
        ..DaemonClientConfig::default()
    }
}

fn seed_stack_topology(
    config: &RuntimedConfig,
    stack_id: &str,
) -> runtime_v2::MachineWorkloadScope {
    use vz_runtime_contract::{
        Architecture, CapabilitySet, EnvironmentId, EnvironmentInstance, EnvironmentSpec,
        EnvironmentState, MachineCapability, MachineId, MachineIncarnation, MachineIncarnationId,
        MachineInstance, MachineProfile, MachineResources, MachineSpec, MachineState,
        MachineWorkloadScope, OperatingSystem, OwnedResourceKind, OwnershipRecord,
        ProjectDefinition, ProjectId, ProjectState, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
    };

    let project_id = ProjectId::new("prj-runtimed-client").expect("valid project id");
    let environment_id = EnvironmentId::new("env-runtimed-client").expect("valid environment id");
    let machine_id = MachineId::new("mch-runtimed-client").expect("valid machine id");
    let incarnation_id =
        MachineIncarnationId::new("inc-runtimed-client").expect("valid incarnation id");
    let target = TargetSpec {
        os: OperatingSystem::Linux,
        arch: Architecture::Aarch64,
        image: "ubuntu:24.04".to_string(),
        version: None,
        channel: None,
        digest: Some("sha256:runtimed-client".to_string()),
    };
    let capabilities = CapabilitySet::new([
        MachineCapability::DockerEngine,
        MachineCapability::Compose,
        MachineCapability::Buildx,
    ]);
    let definition = ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: project_id.clone(),
        name: "runtimed-client".to_string(),
        environment: EnvironmentSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            default_machine: None,
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
            docker_context: None,
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
                environment_id: environment_id.clone(),
                machine_id: Some(machine_id.clone()),
            },
        ],
        legacy_migration: None,
        created_at: 1,
        updated_at: 1,
    };

    std::fs::create_dir_all(
        config
            .state_store_path
            .parent()
            .expect("state store path has a parent"),
    )
    .expect("create state store directory");
    vz_stack::StateStore::open(&config.state_store_path)
        .expect("open state store")
        .save_project_state(&ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments: vec![environment],
        })
        .expect("seed stack topology");

    vz_runtime_translate::machine_workload_scope_to_proto(&MachineWorkloadScope {
        schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
        project_id,
        environment_id,
        machine_id,
        machine_incarnation_id: incarnation_id,
        stack_id: stack_id.to_string(),
    })
}

fn seed_multi_environment_topology(config: &RuntimedConfig) -> vz_runtime_contract::ProjectState {
    use vz_runtime_contract::{
        Architecture, CapabilitySet, EnvironmentId, EnvironmentInstance, EnvironmentSpec,
        EnvironmentState, MachineCapability, MachineId, MachineIncarnation, MachineIncarnationId,
        MachineInstance, MachineProfile, MachineResources, MachineSpec, MachineState,
        OperatingSystem, OwnedResourceKind, OwnershipRecord, ProjectDefinition, ProjectId,
        TOPOLOGY_SCHEMA_VERSION, TargetSpec,
    };

    let project_id = ProjectId::new("prj_status_roundtrip").expect("valid project id");
    let target = TargetSpec {
        os: OperatingSystem::Linux,
        arch: Architecture::Aarch64,
        image: "ubuntu:24.04".to_string(),
        version: Some("24.04".to_string()),
        channel: Some("lts".to_string()),
        digest: Some("sha256:status-roundtrip".to_string()),
    };
    let capabilities = CapabilitySet::new([
        MachineCapability::PosixExec,
        MachineCapability::DockerEngine,
        MachineCapability::Compose,
        MachineCapability::Buildx,
    ]);
    let definition = ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: project_id.clone(),
        name: "status-roundtrip".to_string(),
        environment: EnvironmentSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            default_machine: None,
            machines: vec![MachineSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                name: "linux".to_string(),
                profile: MachineProfile::Developer,
                target: target.clone(),
                resources: MachineResources {
                    cpus: Some(4),
                    memory_mb: Some(8192),
                    disk_bytes: Some(64 * 1024 * 1024 * 1024),
                },
                requested_capabilities: capabilities.clone(),
                workspace: None,
            }],
            networks: Vec::new(),
            endpoints: Vec::new(),
        },
    };
    let definition_digest = definition.digest().expect("definition digest");
    let environment = |environment_id: &str,
                       environment_name: &str,
                       machine_id: &str,
                       incarnation_id: &str,
                       timestamp: u64| {
        let environment_id = EnvironmentId::new(environment_id).expect("valid environment id");
        let machine_id = MachineId::new(machine_id).expect("valid machine id");
        let incarnation_id =
            MachineIncarnationId::new(incarnation_id).expect("valid incarnation id");
        EnvironmentInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            environment_id: environment_id.clone(),
            project_id: project_id.clone(),
            name: environment_name.to_string(),
            definition_digest: definition_digest.clone(),
            state: EnvironmentState::Ready,
            lifecycle_generation: 3,
            active_operation_id: None,
            bindings: Vec::new(),
            machines: vec![MachineInstance {
                docker_context: None,
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                machine_id: machine_id.clone(),
                environment_id: environment_id.clone(),
                name: "linux".to_string(),
                profile: MachineProfile::Developer,
                target: target.clone(),
                resources: MachineResources {
                    cpus: Some(4),
                    memory_mb: Some(8192),
                    disk_bytes: Some(64 * 1024 * 1024 * 1024),
                },
                requested_capabilities: capabilities.clone(),
                negotiated_capabilities: capabilities.clone(),
                backend: None,
                incarnation: Some(MachineIncarnation {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    incarnation_id: incarnation_id.clone(),
                    machine_id: machine_id.clone(),
                    generation: 3,
                    created_at: timestamp,
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
                    environment_id: environment_id.clone(),
                    machine_id: Some(machine_id.clone()),
                },
                OwnershipRecord {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    resource_kind: OwnedResourceKind::DockerContext,
                    resource_id: format!("docker-{environment_name}"),
                    environment_id,
                    machine_id: Some(machine_id),
                },
            ],
            legacy_migration: None,
            created_at: timestamp,
            updated_at: timestamp,
        }
    };
    let state = vz_runtime_contract::ProjectState {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        definition,
        environments: vec![
            environment(
                "env_status_alpha",
                "alpha",
                "mch_status_alpha",
                "inc_status_alpha",
                101,
            ),
            environment(
                "env_status_beta",
                "beta",
                "mch_status_beta",
                "inc_status_beta",
                202,
            ),
        ],
    };

    std::fs::create_dir_all(
        config
            .state_store_path
            .parent()
            .expect("state store path has a parent"),
    )
    .expect("create state store directory");
    let store = vz_stack::StateStore::open(&config.state_store_path).expect("open state store");
    store
        .save_project_state(&state)
        .expect("seed multi-Environment topology");
    store
        .load_project_state_snapshot(state.definition.project_id.as_str())
        .expect("load canonical seeded topology")
        .expect("seeded topology must exist")
}

fn required_sandbox_labels(tmp: &tempfile::TempDir) -> HashMap<String, String> {
    HashMap::from([("project_dir".to_string(), tmp.path().display().to_string())])
}

fn assert_grpc_status_in(error: DaemonClientError, expected: &[Code]) {
    match error {
        DaemonClientError::Grpc(status) => {
            assert!(
                expected.iter().any(|code| *code == status.code()),
                "unexpected grpc status code: {:?}, expected one of {:?}",
                status.code(),
                expected
            );
        }
        other => panic!("expected grpc status error, got {other:?}"),
    }
}

async fn assert_local_scope_preflight<T>(
    operation: &str,
    future: impl std::future::Future<Output = Result<T>>,
) {
    let result = tokio::time::timeout(Duration::from_millis(250), future)
        .await
        .unwrap_or_else(|_| panic!("{operation} scope validation reached the unavailable network"));
    let error = match result {
        Ok(_) => panic!("{operation} accepted a request without exact Machine scope"),
        Err(error) => error,
    };
    match error {
        DaemonClientError::Grpc(status) => {
            assert_eq!(status.code(), Code::InvalidArgument, "{operation}");
            assert!(
                status.message().contains(operation)
                    && status.message().contains("MachineWorkloadScope"),
                "unexpected {operation} scope error: {status}"
            );
        }
        other => panic!("{operation} reached transport before scope validation: {other:?}"),
    }
}

#[test]
fn structured_unavailable_status_preserves_application_error_details() {
    let socket = Path::new("/tmp/vz-structured-status.sock");
    let status = tonic::Status::with_details(
        Code::Unavailable,
        "backend_unavailable: runtime operation failed",
        vec![1, 2, 3].into(),
    );
    match crate::transport::status_to_client_error(socket, status) {
        DaemonClientError::Grpc(status) => assert_eq!(status.details(), &[1, 2, 3]),
        other => panic!("structured application status was flattened: {other:?}"),
    }
}

#[test]
fn detail_free_unavailable_status_remains_a_connection_failure() {
    let socket = Path::new("/tmp/vz-unavailable.sock");
    match crate::transport::status_to_client_error(
        socket,
        tonic::Status::unavailable("connection closed"),
    ) {
        DaemonClientError::Unavailable { socket_path, .. } => assert_eq!(socket_path, socket),
        other => panic!("detail-free transport status changed category: {other:?}"),
    }
}

#[tokio::test]
async fn connect_retries_until_daemon_cold_start_is_ready() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = runtimed_config(&tmp);
    let socket_path = config.socket_path.clone();

    let shutdown = Arc::new(Notify::new());
    let shutdown_task = shutdown.clone();
    let server = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(220)).await;
        let daemon = Arc::new(RuntimeDaemon::start(config).expect("daemon start"));
        serve_runtime_uds_with_shutdown(daemon, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client should connect after delayed startup");
    assert!(!client.handshake().daemon_id.is_empty());

    let error = client
        .create_sandbox(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "   ".to_string(),
            cpus: 0,
            memory_mb: 0,
            labels: required_sandbox_labels(&tmp),
            ..Default::default()
        })
        .await
        .expect_err("empty stack name should fail validation");
    assert!(matches!(
        error,
        DaemonClientError::Grpc(status) if status.code() == Code::InvalidArgument
    ));

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server task join failed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn connect_with_missing_socket_returns_unavailable() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut config = client_config(&tmp, false);
    let missing_socket = tmp.path().join("missing").join("runtimed.sock");
    config.socket_path = missing_socket.clone();

    let error = DaemonClient::connect_with_config(config)
        .await
        .expect_err("missing socket should fail");
    match error {
        DaemonClientError::Unavailable { socket_path, .. }
        | DaemonClientError::StartupTimeout { socket_path, .. } => {
            assert_eq!(socket_path, missing_socket);
        }
        other => panic!("expected unavailable/startup-timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn explicit_socket_path_override_connects_successfully() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon_config = runtimed_config(&tmp);
    let daemon = start_daemon(daemon_config.clone()).await;

    let config = DaemonClientConfig {
        socket_path: daemon_config.socket_path,
        auto_spawn: false,
        state_store_path: Some(tmp.path().join("alternate").join("stack-state.db")),
        runtime_data_dir: Some(tmp.path().join("alternate-runtime")),
        startup_timeout: Duration::from_secs(3),
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_millis(500),
        retry_backoff: Duration::from_millis(30),
        max_retry_backoff: Duration::from_millis(120),
        ..DaemonClientConfig::default()
    };

    let client = DaemonClient::connect_with_config(config)
        .await
        .expect("socket override should connect");
    assert!(!client.handshake().daemon_id.is_empty());

    daemon.stop().await;
}

#[tokio::test]
async fn get_project_state_round_trips_exact_multi_environment_snapshot_without_mutation() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon_config = runtimed_config(&tmp);
    let expected = seed_multi_environment_topology(&daemon_config);
    let daemon = start_daemon(daemon_config.clone()).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let snapshot = client
        .get_project_state(runtime_v2::GetProjectStateRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-project-status".to_string(),
                idempotency_key: String::new(),
                trace_id: "trace-project-status".to_string(),
            }),
            project_id: expected.definition.project_id.to_string(),
        })
        .await
        .expect("get exact Project topology");

    assert_eq!(snapshot.request_id, "req-project-status");
    assert_eq!(snapshot.project, expected);
    assert_eq!(snapshot.project.environments.len(), 2);
    assert_eq!(snapshot.project.environments[0].name, "alpha");
    assert_eq!(snapshot.project.environments[1].name, "beta");

    daemon.stop().await;
    let store = vz_stack::StateStore::open(&daemon_config.state_store_path)
        .expect("reopen state store after read-only RPC");
    assert_eq!(
        store
            .load_project_state_snapshot(expected.definition.project_id.as_str())
            .expect("reload Project snapshot"),
        Some(expected)
    );
    assert!(
        store
            .list_receipts()
            .expect("list receipts")
            .iter()
            .all(|receipt| receipt.entity_type == "maintenance"),
        "read-only topology lookup must not create non-maintenance receipts"
    );
    assert!(store.list_sandboxes().expect("list sandboxes").is_empty());
    assert!(store.list_containers().expect("list containers").is_empty());
    assert!(store.list_executions().expect("list executions").is_empty());
}

#[tokio::test]
async fn get_project_state_rejects_invalid_id_and_reports_missing_project_over_uds() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon_config = runtimed_config(&tmp);
    let expected = seed_multi_environment_topology(&daemon_config);
    let daemon = start_daemon(daemon_config.clone()).await;
    let mut typed_client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("typed client connect");

    let channel = connect_channel(&daemon_config.socket_path, Duration::from_millis(300))
        .await
        .expect("connect raw topology channel");
    let mut raw_client = runtime_v2::topology_service_client::TopologyServiceClient::new(channel);
    let invalid = raw_client
        .get_project_state(runtime_v2::GetProjectStateRequest {
            metadata: None,
            project_id: "not valid/id".to_string(),
        })
        .await
        .expect_err("daemon must reject malformed Project identity");
    assert_eq!(invalid.code(), Code::InvalidArgument);

    let missing = raw_client
        .get_project_state(runtime_v2::GetProjectStateRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-project-missing".to_string(),
                ..Default::default()
            }),
            project_id: "prj_missing".to_string(),
        })
        .await
        .expect_err("daemon must report missing Project identity");
    assert_eq!(missing.code(), Code::NotFound);
    assert!(missing.message().contains("req-project-missing"));

    daemon.stop().await;
    let locally_rejected = typed_client
        .get_project_state(runtime_v2::GetProjectStateRequest {
            metadata: None,
            project_id: "not valid/id".to_string(),
        })
        .await
        .expect_err("client must reject malformed identity before using the stopped transport");
    assert_grpc_status_in(locally_rejected, &[Code::InvalidArgument]);

    let store = vz_stack::StateStore::open(&daemon_config.state_store_path)
        .expect("reopen state store after rejected reads");
    assert_eq!(
        store
            .load_project_state_snapshot(expected.definition.project_id.as_str())
            .expect("reload Project snapshot"),
        Some(expected)
    );
    assert!(
        store
            .list_receipts()
            .expect("list receipts")
            .iter()
            .all(|receipt| receipt.entity_type == "maintenance"),
        "rejected topology lookups must not create non-maintenance receipts"
    );
    assert!(store.list_sandboxes().expect("list sandboxes").is_empty());
    assert!(store.list_containers().expect("list containers").is_empty());
    assert!(store.list_executions().expect("list executions").is_empty());
}

#[tokio::test]
async fn reconnect_after_daemon_restart_yields_new_handshake() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon_config = runtimed_config(&tmp);
    let first = start_daemon(daemon_config.clone()).await;

    let client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");
    let first_request_id = client.handshake().request_id.clone();

    first.stop().await;

    let second = start_daemon(daemon_config).await;
    let mut reconnected = client.reconnect().await.expect("client reconnect");
    let second_request_id = reconnected.handshake().request_id.clone();
    assert_ne!(first_request_id, second_request_id);

    let error = reconnected
        .create_sandbox(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "".to_string(),
            cpus: 0,
            memory_mb: 0,
            labels: required_sandbox_labels(&tmp),
            ..Default::default()
        })
        .await
        .expect_err("empty stack name should fail validation");
    assert!(matches!(
        error,
        DaemonClientError::Grpc(status) if status.code() == Code::InvalidArgument
    ));

    second.stop().await;
}

#[tokio::test]
async fn create_sandbox_stream_emits_progress_and_completion() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let mut stream = client
        .create_sandbox_stream(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stream-sandbox-client".to_string(),
            cpus: 1,
            memory_mb: 256,
            labels: required_sandbox_labels(&tmp),
            ..Default::default()
        })
        .await
        .expect("create sandbox stream");

    let mut saw_progress = false;
    let mut completion = None;
    let mut terminal_error = None;
    loop {
        match stream.message().await {
            Ok(Some(event)) => match event.payload {
                Some(runtime_v2::create_sandbox_event::Payload::Progress(progress)) => {
                    saw_progress = true;
                    assert!(!progress.phase.trim().is_empty());
                }
                Some(runtime_v2::create_sandbox_event::Payload::Completion(done)) => {
                    completion = Some(done);
                }
                None => {}
            },
            Ok(None) => break,
            Err(error) => {
                terminal_error = Some(error);
                break;
            }
        }
    }

    if !cfg!(target_os = "linux") {
        let error = terminal_error.expect("non-linux should report unsupported spaces mode");
        assert_eq!(error.code(), Code::Unimplemented);
        daemon.stop().await;
        return;
    }

    assert!(
        saw_progress,
        "stream should emit at least one progress event"
    );
    let completion = completion.expect("stream should emit completion");
    let response = completion
        .response
        .expect("completion should include sandbox response");
    assert_eq!(
        response
            .sandbox
            .expect("sandbox payload should exist")
            .sandbox_id,
        "stream-sandbox-client"
    );
    assert!(!completion.receipt_id.trim().is_empty());

    daemon.stop().await;
}

#[tokio::test]
async fn create_sandbox_with_metadata_preserves_receipt_header_from_stream_completion() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let response_result = client
        .create_sandbox_with_metadata(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "sandbox-receipt-header".to_string(),
            cpus: 1,
            memory_mb: 256,
            labels: required_sandbox_labels(&tmp),
            ..Default::default()
        })
        .await;
    if !cfg!(target_os = "linux") {
        let error = response_result.expect_err("non-linux should reject spaces-mode sandbox");
        assert!(matches!(
            error,
            DaemonClientError::Grpc(status) if status.code() == Code::Unimplemented
        ));
        daemon.stop().await;
        return;
    }
    let response = response_result.expect("create sandbox with metadata");

    let receipt_id = response
        .metadata()
        .get("x-receipt-id")
        .expect("receipt header should be present")
        .to_str()
        .expect("receipt header should be valid utf8");
    assert!(receipt_id.starts_with("rcp-"));

    daemon.stop().await;
}

#[tokio::test]
async fn create_sandbox_stream_terminal_error_is_mapped_to_invalid_argument() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let mut stream = client
        .create_sandbox_stream(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "sandbox-invalid-project-dir".to_string(),
            cpus: 1,
            memory_mb: 256,
            labels: HashMap::from([(
                "project_dir".to_string(),
                "relative/not-absolute".to_string(),
            )]),
            ..Default::default()
        })
        .await
        .expect("create sandbox stream should start");

    let mut saw_progress = false;
    let error = loop {
        match stream.message().await {
            Ok(Some(event)) => match event.payload {
                Some(runtime_v2::create_sandbox_event::Payload::Progress(_)) => {
                    saw_progress = true;
                }
                Some(runtime_v2::create_sandbox_event::Payload::Completion(_)) => {
                    panic!("stream should not emit completion for invalid project_dir request");
                }
                None => {}
            },
            Ok(None) => {
                panic!("stream ended without terminal validation error");
            }
            Err(error) => break error,
        }
    };
    assert!(
        saw_progress,
        "stream should emit progress before terminal error"
    );
    assert_eq!(error.code(), Code::InvalidArgument);

    let wrapped = client
        .create_sandbox_with_metadata(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "sandbox-invalid-project-dir-2".to_string(),
            cpus: 1,
            memory_mb: 256,
            labels: HashMap::from([(
                "project_dir".to_string(),
                "relative/not-absolute".to_string(),
            )]),
            ..Default::default()
        })
        .await
        .expect_err("unary compatibility wrapper should map stream terminal error");
    assert!(matches!(
        wrapped,
        DaemonClientError::Grpc(status) if status.code() == Code::InvalidArgument
    ));

    daemon.stop().await;
}

#[tokio::test]
async fn stack_apply_and_teardown_round_trip_via_daemon_client() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon_config = runtimed_config(&tmp);
    let stack_name = "stack-client-e2e".to_string();
    let scope = seed_stack_topology(&daemon_config, &stack_name);
    let daemon = start_daemon(daemon_config).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let applied = client
        .apply_stack_with_metadata(runtime_v2::ApplyStackRequest {
            metadata: None,
            stack_name: stack_name.clone(),
            compose_yaml: "services: {}\n".to_string(),
            compose_dir: ".".to_string(),
            detach: false,
            dry_run: false,
            scope: Some(scope.clone()),
        })
        .await
        .expect("apply stack");
    assert!(applied.metadata().get("x-receipt-id").is_some());
    let applied = applied.into_inner();
    assert_eq!(applied.stack_name, stack_name);

    let status = client
        .get_stack_status(runtime_v2::GetStackStatusRequest {
            metadata: None,
            stack_name: stack_name.clone(),
            scope: Some(scope.clone()),
        })
        .await
        .expect("get stack status");
    assert!(status.services.is_empty());

    let events = client
        .list_stack_events(runtime_v2::ListStackEventsRequest {
            metadata: None,
            stack_name: stack_name.clone(),
            after: 0,
            limit: 100,
            scope: Some(scope.clone()),
        })
        .await
        .expect("list stack events");
    assert!(
        !events.events.is_empty(),
        "stack apply should emit observable events"
    );

    let torn_down = client
        .teardown_stack_with_metadata(runtime_v2::TeardownStackRequest {
            metadata: None,
            stack_name,
            remove_volumes: false,
            dry_run: false,
            scope: Some(scope),
        })
        .await
        .expect("teardown stack");
    assert!(torn_down.metadata().get("x-receipt-id").is_some());

    daemon.stop().await;
}

#[tokio::test]
async fn pull_and_prune_images_round_trip_via_daemon_client() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let mut pull_stream = client
        .pull_image(runtime_v2::PullImageRequest {
            metadata: None,
            image_ref: "alpine:3.20".to_string(),
        })
        .await
        .expect("pull image");
    let mut pulled = None;
    while let Some(event) = pull_stream.message().await.expect("read pull image stream") {
        if let Some(runtime_v2::pull_image_event::Payload::Completion(done)) = event.payload {
            pulled = Some(done);
        }
    }
    let pulled = pulled.expect("pull stream completion");
    assert_eq!(
        pulled
            .image
            .as_ref()
            .map(|image| image.image_ref.as_str())
            .unwrap_or_default(),
        "alpine:3.20"
    );
    assert!(!pulled.receipt_id.trim().is_empty());

    let listed = client
        .list_images(runtime_v2::ListImagesRequest { metadata: None })
        .await
        .expect("list images");
    assert!(
        listed
            .images
            .iter()
            .any(|image| image.image_ref == "alpine:3.20"),
        "pulled image should be present in daemon image index"
    );

    let mut prune_stream = client
        .prune_images(runtime_v2::PruneImagesRequest { metadata: None })
        .await
        .expect("prune images");
    let mut pruned = None;
    while let Some(event) = prune_stream
        .message()
        .await
        .expect("read prune image stream")
    {
        if let Some(runtime_v2::prune_images_event::Payload::Completion(done)) = event.payload {
            pruned = Some(done);
        }
    }
    let pruned = pruned.expect("prune stream completion");
    assert!(
        pruned.remaining_images <= listed.images.len() as u64,
        "prune completion remaining count should not increase image index size"
    );
    assert!(!pruned.receipt_id.trim().is_empty());

    daemon.stop().await;
}

#[tokio::test]
async fn version_mismatch_returns_incompatible_version() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;

    let mut config = client_config(&tmp, false);
    config.expected_daemon_version = Some("999.999.999".to_string());
    let error = match DaemonClient::connect_with_config(config).await {
        Ok(_) => panic!("mismatch should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        DaemonClientError::IncompatibleVersion { .. }
    ));

    daemon.stop().await;
}

#[tokio::test]
async fn autostart_version_mismatch_preserves_original_daemon_and_decoys() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let original = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("original");
    let identity = original.handshake().daemon_id.clone();
    let mut config = client_config(&tmp, true);
    config.expected_daemon_version = Some("999.999.999".into());
    config.daemon_binary = Some(tmp.path().join("must-not-launch"));
    let pid = config.socket_path.with_extension("pid");
    let log = config.socket_path.with_extension("log");
    std::fs::write(&pid, "untrusted-pid-decoy").expect("pid");
    std::fs::write(&log, "retained-log-decoy").expect("log");
    assert!(matches!(
        DaemonClient::connect_with_config(config).await,
        Err(DaemonClientError::IncompatibleVersion { .. })
    ));
    let after = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("still live");
    assert_eq!(after.handshake().daemon_id, identity);
    assert_eq!(
        std::fs::read_to_string(pid).expect("pid retained"),
        "untrusted-pid-decoy"
    );
    assert_eq!(
        std::fs::read_to_string(log).expect("log retained"),
        "retained-log-decoy"
    );
    daemon.stop().await;
}

#[test]
fn autostart_never_removes_existing_socket_or_store_lock() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = DaemonClientConfig {
        socket_path: tmp.path().join("occupied.sock"),
        state_store_path: Some(tmp.path().join("state.db")),
        daemon_binary: Some(tmp.path().join("must-not-launch")),
        ..Default::default()
    };
    std::fs::write(&config.socket_path, "foreign socket-path decoy").expect("socket decoy");
    let lock = tmp.path().join("state.db.lock");
    std::fs::write(&lock, "owned lock").expect("lock");
    assert!(matches!(
        DaemonClient::spawn_daemon(&config),
        Err(DaemonClientError::Unavailable { .. })
    ));
    assert_eq!(
        std::fs::read_to_string(&config.socket_path).expect("retained"),
        "foreign socket-path decoy"
    );
    assert_eq!(
        std::fs::read_to_string(lock).expect("retained"),
        "owned lock"
    );
}

#[test]
fn managed_delete_requires_both_prior_owner_discovery_and_existing_state() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = DaemonClientConfig {
        socket_path: tmp.path().join("recovery.sock"),
        state_store_path: Some(tmp.path().join("state.db")),
        recover_existing_owner_only: true,
        ..Default::default()
    };
    let owner = tmp.path().join("recovery.sock.owner.json");
    assert!(validate_spawn_candidate(&config).is_err());
    std::fs::write(&owner, "not-authority-only-a-discovery-candidate").expect("fixture owner");
    assert!(validate_spawn_candidate(&config).is_err());
    assert!(!config.state_store_path.as_ref().expect("path").exists());
    std::fs::write(
        config.state_store_path.as_ref().expect("path"),
        "existing database decoy",
    )
    .expect("fixture database");
    // This client-side check never parses or authorizes recovery. The actual
    // daemon must independently reject these deliberately invalid contents.
    assert!(validate_spawn_candidate(&config).is_ok());
    assert_eq!(
        std::fs::read_to_string(&owner).expect("retained"),
        "not-authority-only-a-discovery-candidate"
    );
    assert_eq!(
        std::fs::read_to_string(config.state_store_path.as_ref().expect("path")).expect("retained"),
        "existing database decoy"
    );
}

#[cfg(unix)]
#[test]
fn managed_delete_discovery_rejects_symlink_owner_and_database() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = DaemonClientConfig {
        socket_path: tmp.path().join("recovery.sock"),
        state_store_path: Some(tmp.path().join("state.db")),
        recover_existing_owner_only: true,
        ..Default::default()
    };
    let source = tmp.path().join("foreign");
    std::fs::write(&source, "foreign bytes").expect("decoy");
    std::os::unix::fs::symlink(&source, tmp.path().join("recovery.sock.owner.json"))
        .expect("owner link");
    std::os::unix::fs::symlink(&source, config.state_store_path.as_ref().expect("path"))
        .expect("database link");
    assert!(validate_spawn_candidate(&config).is_err());
    assert_eq!(
        std::fs::read_to_string(source).expect("retained"),
        "foreign bytes"
    );
}

#[cfg(target_os = "macos")]
#[test]
fn stale_socket_candidate_is_never_removed_by_client_discovery() {
    use std::os::unix::fs::MetadataExt;
    let tmp = tempfile::Builder::new()
        .prefix("vz-recover-")
        .tempdir_in("/private/tmp")
        .expect("tempdir");
    let config = DaemonClientConfig {
        socket_path: tmp.path().join("recovery.sock"),
        ..Default::default()
    };
    let listener =
        std::os::unix::net::UnixListener::bind(&config.socket_path).expect("owned fixture socket");
    let inode = std::fs::symlink_metadata(&config.socket_path)
        .expect("socket")
        .ino();
    drop(listener);
    assert!(validate_spawn_candidate(&config).is_err());
    std::fs::write(
        tmp.path().join("recovery.sock.owner.json"),
        "candidate only",
    )
    .expect("fixture owner");
    assert!(validate_spawn_candidate(&config).is_ok());
    assert_eq!(
        std::fs::symlink_metadata(&config.socket_path)
            .expect("retained socket")
            .ino(),
        inode
    );
}

#[tokio::test]
async fn heartbeat_lease_round_trip_and_signal_exec_missing_returns_not_found() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let sandbox_result = client
        .create_sandbox(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "client-heartbeat-sandbox".to_string(),
            cpus: 1,
            memory_mb: 128,
            labels: required_sandbox_labels(&tmp),
            ..Default::default()
        })
        .await;
    if !cfg!(target_os = "linux") {
        let error = sandbox_result.expect_err("non-linux should reject spaces-mode sandbox");
        assert!(matches!(
            error,
            DaemonClientError::Grpc(status) if status.code() == Code::Unimplemented
        ));
        daemon.stop().await;
        return;
    }
    let sandbox = sandbox_result.expect("create sandbox");
    let sandbox_id = sandbox
        .sandbox
        .expect("sandbox payload")
        .sandbox_id
        .to_string();

    let lease = client
        .open_lease(runtime_v2::OpenLeaseRequest {
            metadata: None,
            sandbox_id,
            ttl_secs: 30,
        })
        .await
        .expect("open lease");
    let lease_id = lease.lease.expect("lease payload").lease_id;

    let heartbeat = client
        .heartbeat_lease(runtime_v2::HeartbeatLeaseRequest {
            metadata: None,
            lease_id: lease_id.clone(),
        })
        .await
        .expect("heartbeat lease");
    assert_eq!(
        heartbeat.lease.expect("heartbeat lease payload").lease_id,
        lease_id
    );

    let signal_error = client
        .signal_exec(runtime_v2::SignalExecRequest {
            metadata: None,
            execution_id: "exec-missing-client".to_string(),
            signal: "SIGTERM".to_string(),
        })
        .await
        .expect_err("missing execution should fail");
    assert_grpc_status_in(signal_error, &[Code::NotFound]);

    daemon.stop().await;
}

#[tokio::test]
async fn image_get_receipt_and_stream_build_events_are_covered() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let mut pull_stream = client
        .pull_image(runtime_v2::PullImageRequest {
            metadata: None,
            image_ref: "alpine:3.20".to_string(),
        })
        .await
        .expect("pull image");
    let mut pulled = None;
    while let Some(event) = pull_stream.message().await.expect("read pull image stream") {
        if let Some(runtime_v2::pull_image_event::Payload::Completion(done)) = event.payload {
            pulled = Some(done);
        }
    }
    let pulled = pulled.expect("pull completion");

    let image = client
        .get_image(runtime_v2::GetImageRequest {
            metadata: None,
            image_ref: "alpine:3.20".to_string(),
        })
        .await
        .expect("get image");
    assert_eq!(
        image.image.expect("image payload").image_ref,
        "alpine:3.20".to_string()
    );

    let receipt = client
        .get_receipt(runtime_v2::GetReceiptRequest {
            metadata: None,
            receipt_id: pulled.receipt_id.clone(),
        })
        .await
        .expect("get receipt");
    assert_eq!(
        receipt.receipt.expect("receipt payload").receipt_id,
        pulled.receipt_id
    );

    let stream_error = client
        .stream_build_events(runtime_v2::StreamBuildEventsRequest {
            build_id: "bld-missing-client".to_string(),
            metadata: None,
        })
        .await
        .expect_err("missing build should fail");
    assert_grpc_status_in(stream_error, &[Code::NotFound, Code::Unimplemented]);

    daemon.stop().await;
}

#[tokio::test]
async fn checkpoint_restore_and_fork_missing_return_not_found() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let get_error = client
        .get_checkpoint(runtime_v2::GetCheckpointRequest {
            checkpoint_id: "ckpt-missing-client".to_string(),
            metadata: None,
        })
        .await
        .expect_err("missing checkpoint get should fail");
    assert_grpc_status_in(get_error, &[Code::NotFound]);

    let restore_error = client
        .restore_checkpoint(runtime_v2::RestoreCheckpointRequest {
            checkpoint_id: "ckpt-missing-client".to_string(),
            metadata: None,
        })
        .await
        .expect_err("missing checkpoint restore should fail");
    assert_grpc_status_in(restore_error, &[Code::NotFound]);

    let fork_error = client
        .fork_checkpoint(runtime_v2::ForkCheckpointRequest {
            checkpoint_id: "ckpt-missing-client".to_string(),
            new_sandbox_id: "sbx-fork-target".to_string(),
            metadata: None,
        })
        .await
        .expect_err("missing checkpoint fork should fail");
    assert_grpc_status_in(fork_error, &[Code::NotFound]);

    daemon.stop().await;
}

#[tokio::test]
async fn checkpoint_export_and_import_missing_paths_return_not_found() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let export_error = client
        .export_checkpoint(runtime_v2::ExportCheckpointRequest {
            checkpoint_id: "ckpt-missing-client".to_string(),
            stream_path: "/tmp/vz-missing-export.stream".to_string(),
            metadata: None,
        })
        .await
        .expect_err("missing checkpoint export should fail");
    assert_grpc_status_in(export_error, &[Code::NotFound]);

    let import_error = client
        .import_checkpoint(runtime_v2::ImportCheckpointRequest {
            sandbox_id: "sbx-missing-client".to_string(),
            stream_path: "/tmp/vz-missing-import.stream".to_string(),
            checkpoint_class: "fs_quick".to_string(),
            compatibility_fingerprint: String::new(),
            retention_tag: String::new(),
            metadata: None,
        })
        .await
        .expect_err("missing sandbox import should fail");
    assert_grpc_status_in(import_error, &[Code::NotFound]);

    daemon.stop().await;
}

#[tokio::test]
async fn stack_service_scope_validation_fails_locally_before_every_rpc_send() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    // Leave the established client pointed at a dead transport. Every call
    // below must still return InvalidArgument immediately; Unavailable or a
    // timeout proves that its lowest-level send skipped local scope preflight.
    let RunningDaemon { task, .. } = daemon;
    task.abort();
    assert!(
        task.await
            .expect_err("aborted daemon task must not complete")
            .is_cancelled(),
        "daemon task must be cancelled before scope preflight probes"
    );

    let stack_name = "stack-client-scope-preflight";
    assert_local_scope_preflight(
        "ApplyStack",
        client.apply_stack_stream_with_metadata(runtime_v2::ApplyStackRequest {
            stack_name: stack_name.to_string(),
            ..Default::default()
        }),
    )
    .await;
    assert_local_scope_preflight(
        "TeardownStack",
        client.teardown_stack_stream_with_metadata(runtime_v2::TeardownStackRequest {
            stack_name: stack_name.to_string(),
            ..Default::default()
        }),
    )
    .await;
    assert_local_scope_preflight(
        "GetStackStatus",
        client.get_stack_status_with_metadata(runtime_v2::GetStackStatusRequest {
            stack_name: stack_name.to_string(),
            ..Default::default()
        }),
    )
    .await;
    assert_local_scope_preflight(
        "ListStackEvents",
        client.list_stack_events_with_metadata(runtime_v2::ListStackEventsRequest {
            stack_name: stack_name.to_string(),
            ..Default::default()
        }),
    )
    .await;
    assert_local_scope_preflight(
        "GetStackLogs",
        client.get_stack_logs_with_metadata(runtime_v2::GetStackLogsRequest {
            stack_name: stack_name.to_string(),
            ..Default::default()
        }),
    )
    .await;
    assert_local_scope_preflight(
        "StopStackService",
        client.stop_stack_service_stream_with_metadata(runtime_v2::StackServiceActionRequest {
            stack_name: stack_name.to_string(),
            ..Default::default()
        }),
    )
    .await;
    assert_local_scope_preflight(
        "StartStackService",
        client.start_stack_service_stream_with_metadata(runtime_v2::StackServiceActionRequest {
            stack_name: stack_name.to_string(),
            ..Default::default()
        }),
    )
    .await;
    assert_local_scope_preflight(
        "RestartStackService",
        client.restart_stack_service_stream_with_metadata(runtime_v2::StackServiceActionRequest {
            stack_name: stack_name.to_string(),
            ..Default::default()
        }),
    )
    .await;
    assert_local_scope_preflight(
        "CreateStackRunContainer",
        client.create_stack_run_container_with_metadata(runtime_v2::StackRunContainerRequest {
            stack_name: stack_name.to_string(),
            ..Default::default()
        }),
    )
    .await;
    assert_local_scope_preflight(
        "RemoveStackRunContainer",
        client.remove_stack_run_container_with_metadata(runtime_v2::StackRunContainerRequest {
            stack_name: stack_name.to_string(),
            ..Default::default()
        }),
    )
    .await;

    let malformed_scope = runtime_v2::MachineWorkloadScope {
        schema_version: 0,
        project_id: "prj-client-scope".to_string(),
        environment_id: "env-client-scope".to_string(),
        machine_id: "mch-client-scope".to_string(),
        machine_incarnation_id: "inc-client-scope".to_string(),
        stack_id: stack_name.to_string(),
    };
    assert_local_scope_preflight(
        "GetStackStatus",
        client.get_stack_status_with_metadata(runtime_v2::GetStackStatusRequest {
            stack_name: stack_name.to_string(),
            scope: Some(malformed_scope),
            ..Default::default()
        }),
    )
    .await;

    let mismatched_scope = runtime_v2::MachineWorkloadScope {
        schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
        project_id: "prj-client-scope".to_string(),
        environment_id: "env-client-scope".to_string(),
        machine_id: "mch-client-scope".to_string(),
        machine_incarnation_id: "inc-client-scope".to_string(),
        stack_id: "stack-other-machine-workload".to_string(),
    };
    assert_local_scope_preflight(
        "GetStackStatus",
        client.get_stack_status_with_metadata(runtime_v2::GetStackStatusRequest {
            stack_name: stack_name.to_string(),
            scope: Some(mismatched_scope),
            ..Default::default()
        }),
    )
    .await;
}

#[tokio::test]
async fn stack_auxiliary_methods_and_event_stream_paths_are_covered() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon_config = runtimed_config(&tmp);
    let stack_name = "stack-client-aux".to_string();
    let scope = seed_stack_topology(&daemon_config, &stack_name);
    let daemon = start_daemon(daemon_config).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    client
        .apply_stack(runtime_v2::ApplyStackRequest {
            metadata: None,
            stack_name: stack_name.clone(),
            compose_yaml: "services: {}\n".to_string(),
            compose_dir: ".".to_string(),
            dry_run: false,
            detach: false,
            scope: Some(scope.clone()),
        })
        .await
        .expect("apply stack");

    let mut stream = client
        .stream_events(runtime_v2::StreamEventsRequest {
            stack_name: stack_name.clone(),
            after: 0,
            scope: String::new(),
            metadata: None,
        })
        .await
        .expect("stream events");
    let first_event = tokio::time::timeout(Duration::from_secs(2), stream.message())
        .await
        .expect("stream event timeout")
        .expect("stream events read")
        .expect("at least one event");
    assert_eq!(first_event.stack_name, stack_name);

    let logs_result = client
        .get_stack_logs(runtime_v2::GetStackLogsRequest {
            metadata: None,
            stack_name: "stack-missing-client".to_string(),
            service: "svc".to_string(),
            tail: 50,
            scope: Some(runtime_v2::MachineWorkloadScope {
                stack_id: "stack-missing-client".to_string(),
                ..scope.clone()
            }),
        })
        .await;
    if let Err(error) = logs_result {
        assert_grpc_status_in(
            error,
            &[
                Code::NotFound,
                Code::FailedPrecondition,
                Code::Unimplemented,
            ],
        );
    }

    let stop_error = client
        .stop_stack_service(runtime_v2::StackServiceActionRequest {
            metadata: None,
            stack_name: "stack-missing-client".to_string(),
            service_name: "svc".to_string(),
            scope: Some(runtime_v2::MachineWorkloadScope {
                stack_id: "stack-missing-client".to_string(),
                ..scope.clone()
            }),
        })
        .await
        .expect_err("stop stack service should fail for missing stack/service");
    assert_grpc_status_in(stop_error, &[Code::NotFound, Code::FailedPrecondition]);

    let start_error = client
        .start_stack_service(runtime_v2::StackServiceActionRequest {
            metadata: None,
            stack_name: "stack-missing-client".to_string(),
            service_name: "svc".to_string(),
            scope: Some(runtime_v2::MachineWorkloadScope {
                stack_id: "stack-missing-client".to_string(),
                ..scope.clone()
            }),
        })
        .await
        .expect_err("start stack service should fail for missing stack/service");
    assert_grpc_status_in(start_error, &[Code::NotFound, Code::FailedPrecondition]);

    let restart_error = client
        .restart_stack_service(runtime_v2::StackServiceActionRequest {
            metadata: None,
            stack_name: "stack-missing-client".to_string(),
            service_name: "svc".to_string(),
            scope: Some(runtime_v2::MachineWorkloadScope {
                stack_id: "stack-missing-client".to_string(),
                ..scope.clone()
            }),
        })
        .await
        .expect_err("restart stack service should fail for missing stack/service");
    assert_grpc_status_in(restart_error, &[Code::NotFound, Code::FailedPrecondition]);

    let run_create_error = client
        .create_stack_run_container(runtime_v2::StackRunContainerRequest {
            metadata: None,
            stack_name: "stack-missing-client".to_string(),
            service_name: "svc".to_string(),
            run_service_name: "svc-run".to_string(),
            scope: Some(runtime_v2::MachineWorkloadScope {
                stack_id: "stack-missing-client".to_string(),
                ..scope.clone()
            }),
        })
        .await
        .expect_err("create stack run container should fail for missing stack/service");
    assert_grpc_status_in(
        run_create_error,
        &[Code::NotFound, Code::FailedPrecondition],
    );

    let run_remove_error = client
        .remove_stack_run_container(runtime_v2::StackRunContainerRequest {
            metadata: None,
            stack_name: "stack-missing-client".to_string(),
            service_name: "svc".to_string(),
            run_service_name: "svc-run".to_string(),
            scope: Some(runtime_v2::MachineWorkloadScope {
                stack_id: "stack-missing-client".to_string(),
                ..scope
            }),
        })
        .await
        .expect_err("remove stack run container should fail for missing stack/service");
    assert_grpc_status_in(
        run_remove_error,
        &[Code::NotFound, Code::FailedPrecondition],
    );

    drop(stream);
    daemon.stop().await;
}

#[tokio::test]
async fn file_mutation_rpc_methods_are_covered() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let sandbox_result = client
        .create_sandbox(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "client-file-rpc-sandbox".to_string(),
            cpus: 1,
            memory_mb: 128,
            labels: required_sandbox_labels(&tmp),
            ..Default::default()
        })
        .await;
    if !cfg!(target_os = "linux") {
        let error = sandbox_result.expect_err("non-linux should reject spaces-mode sandbox");
        assert!(matches!(
            error,
            DaemonClientError::Grpc(status) if status.code() == Code::Unimplemented
        ));
        daemon.stop().await;
        return;
    }
    let sandbox = sandbox_result.expect("create sandbox");
    let sandbox_id = sandbox
        .sandbox
        .expect("sandbox payload")
        .sandbox_id
        .to_string();

    client
        .write_file(runtime_v2::WriteFileRequest {
            metadata: None,
            sandbox_id: sandbox_id.clone(),
            path: "source.txt".to_string(),
            data: b"hello".to_vec(),
            append: false,
            create_parents: true,
        })
        .await
        .expect("write source file");

    client
        .copy_path(runtime_v2::CopyPathRequest {
            metadata: None,
            sandbox_id: sandbox_id.clone(),
            src_path: "source.txt".to_string(),
            dst_path: "copied.txt".to_string(),
            overwrite: true,
        })
        .await
        .expect("copy file");

    client
        .move_path(runtime_v2::MovePathRequest {
            metadata: None,
            sandbox_id: sandbox_id.clone(),
            src_path: "copied.txt".to_string(),
            dst_path: "moved.txt".to_string(),
            overwrite: true,
        })
        .await
        .expect("move file");

    client
        .remove_path(runtime_v2::RemovePathRequest {
            metadata: None,
            sandbox_id: sandbox_id.clone(),
            path: "moved.txt".to_string(),
            recursive: false,
        })
        .await
        .expect("remove file");

    let chmod_result = client
        .chmod_path(runtime_v2::ChmodPathRequest {
            metadata: None,
            sandbox_id: sandbox_id.clone(),
            path: "source.txt".to_string(),
            mode: 0o644,
        })
        .await;
    if let Err(error) = chmod_result {
        assert_grpc_status_in(error, &[Code::Unimplemented, Code::NotFound]);
    }

    let chown_result = client
        .chown_path(runtime_v2::ChownPathRequest {
            metadata: None,
            sandbox_id,
            path: "source.txt".to_string(),
            uid: 0,
            gid: 0,
        })
        .await;
    if let Err(error) = chown_result {
        assert_grpc_status_in(error, &[Code::Unimplemented, Code::NotFound]);
    }

    daemon.stop().await;
}

#[tokio::test]
async fn validate_linux_vm_stream_reports_descriptor_checksum_failure() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let artifact_dir = tmp.path().join("artifacts");
    std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");
    let kernel_path = artifact_dir.join("vmlinux");
    let initramfs_path = artifact_dir.join("initramfs.img");
    let version_path = artifact_dir.join("version.json");
    let descriptor_path = artifact_dir.join("validate-linux.json");

    std::fs::write(&kernel_path, b"kernel-good").expect("write kernel");
    std::fs::write(&initramfs_path, b"initramfs-good").expect("write initramfs");

    let expected_kernel_sha = "00".repeat(32);
    let expected_initramfs_sha = "11".repeat(32);

    let version_json = format!(
        "{{\"kernel\":\"6.12.11\",\"sha256_vmlinux\":\"{expected_kernel_sha}\",\"sha256_initramfs\":\"{expected_initramfs_sha}\"}}"
    );
    std::fs::write(&version_path, version_json).expect("write version json");

    let descriptor_json = format!(
        "{{\"schema_version\":1,\"image_name\":\"validate-linux\",\"kernel_path\":\"{}\",\"initramfs_path\":\"{}\",\"version_json_path\":\"{}\",\"disk_path\":\"{}\",\"disk_size_gb\":8,\"linux_artifact_version\":\"6.12.11\",\"sha256_vmlinux\":\"\",\"sha256_initramfs\":\"\",\"created_at_unix_secs\":1700000000}}",
        kernel_path.display(),
        initramfs_path.display(),
        version_path.display(),
        artifact_dir.join("disk.img").display()
    );
    std::fs::write(&descriptor_path, descriptor_json).expect("write descriptor");

    let mut stream = client
        .validate_linux_vm_stream(runtime_v2::ValidateLinuxVmRequest {
            metadata: None,
            descriptor_path: descriptor_path.display().to_string(),
            sandbox_id: String::new(),
        })
        .await
        .expect("validate stream");

    let mut completion = None;
    while let Some(event) = stream.message().await.expect("read stream event") {
        if let Some(runtime_v2::validate_linux_vm_event::Payload::Completion(done)) = event.payload
        {
            completion = Some(done);
            break;
        }
    }
    let completion = completion.expect("completion event");
    assert!(!completion.ok, "checksum mismatch must fail validation");
    assert!(
        completion
            .checks
            .iter()
            .any(|check| check.name == "descriptor_consistency" && check.status == "fail")
    );

    daemon.stop().await;
}

#[tokio::test]
async fn linux_vm_base_lifecycle_rpc_methods_are_covered() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let kernel = tmp.path().join("vmlinux");
    let initramfs = tmp.path().join("initramfs.img");
    let version = tmp.path().join("version.json");
    std::fs::write(&kernel, b"kernel").expect("write kernel");
    std::fs::write(&initramfs, b"initramfs").expect("write initramfs");
    std::fs::write(&version, b"{\"kernel\":\"6.12.11\"}").expect("write version");

    let mut upsert_stream = client
        .upsert_linux_vm_base_stream(runtime_v2::UpsertLinuxVmBaseRequest {
            metadata: None,
            base: Some(runtime_v2::LinuxVmBaseDefinition {
                base_id: "base-e2e".to_string(),
                kernel_path: kernel.display().to_string(),
                initramfs_path: initramfs.display().to_string(),
                version_json_path: version.display().to_string(),
                description: "test base".to_string(),
                updated_at_unix_secs: 0,
            }),
        })
        .await
        .expect("upsert linux vm base stream");
    let mut saw_upsert_completion = false;
    while let Some(event) = upsert_stream
        .message()
        .await
        .expect("read upsert stream event")
    {
        if matches!(
            event.payload,
            Some(runtime_v2::upsert_linux_vm_base_event::Payload::Completion(
                _
            ))
        ) {
            saw_upsert_completion = true;
            break;
        }
    }
    assert!(saw_upsert_completion, "upsert stream must emit completion");

    let listed = client
        .list_linux_vm_bases(runtime_v2::ListLinuxVmBasesRequest { metadata: None })
        .await
        .expect("list linux vm bases");
    assert!(listed.bases.iter().any(|base| base.base_id == "base-e2e"));

    let inspected = client
        .get_linux_vm_base(runtime_v2::GetLinuxVmBaseRequest {
            metadata: None,
            base_id: "base-e2e".to_string(),
        })
        .await
        .expect("inspect linux vm base");
    let inspected_base = inspected.base.expect("base payload");
    assert_eq!(inspected_base.base_id, "base-e2e");

    let mut delete_stream = client
        .delete_linux_vm_base_stream(runtime_v2::DeleteLinuxVmBaseRequest {
            metadata: None,
            base_id: "base-e2e".to_string(),
        })
        .await
        .expect("delete linux vm base stream");
    let mut saw_delete_completion = false;
    while let Some(event) = delete_stream
        .message()
        .await
        .expect("read delete stream event")
    {
        if matches!(
            event.payload,
            Some(runtime_v2::delete_linux_vm_base_event::Payload::Completion(
                _
            ))
        ) {
            saw_delete_completion = true;
            break;
        }
    }
    assert!(saw_delete_completion, "delete stream must emit completion");

    let missing = client
        .get_linux_vm_base(runtime_v2::GetLinuxVmBaseRequest {
            metadata: None,
            base_id: "base-e2e".to_string(),
        })
        .await
        .expect_err("deleted base should be missing");
    assert_grpc_status_in(missing, &[Code::NotFound]);

    daemon.stop().await;
}

#[tokio::test]
async fn linux_vm_patch_apply_and_rollback_rpc_methods_are_covered() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let kernel = tmp.path().join("vmlinux");
    let initramfs = tmp.path().join("initramfs.img");
    let version = tmp.path().join("version.json");
    std::fs::write(&kernel, b"kernel").expect("write kernel");
    std::fs::write(&initramfs, b"initramfs").expect("write initramfs");
    std::fs::write(&version, b"{\"kernel\":\"6.12.11\"}").expect("write version");

    let mut upsert_stream = client
        .upsert_linux_vm_base_stream(runtime_v2::UpsertLinuxVmBaseRequest {
            metadata: None,
            base: Some(runtime_v2::LinuxVmBaseDefinition {
                base_id: "patch-base".to_string(),
                kernel_path: kernel.display().to_string(),
                initramfs_path: initramfs.display().to_string(),
                version_json_path: version.display().to_string(),
                description: "before".to_string(),
                updated_at_unix_secs: 0,
            }),
        })
        .await
        .expect("upsert base");
    while let Some(event) = upsert_stream
        .message()
        .await
        .expect("read upsert stream event")
    {
        if matches!(
            event.payload,
            Some(runtime_v2::upsert_linux_vm_base_event::Payload::Completion(
                _
            ))
        ) {
            break;
        }
    }

    let bundle_path = tmp.path().join("patch-1.json");
    std::fs::write(
        &bundle_path,
        format!(
            "{{\"schema_version\":1,\"patch_id\":\"patch-1\",\"base_id\":\"patch-base\",\"set\":{{\"description\":\"after\"}}}}"
        ),
    )
    .expect("write bundle");

    let mut apply_stream = client
        .apply_linux_vm_patch_stream(runtime_v2::ApplyLinuxVmPatchRequest {
            metadata: None,
            bundle_path: bundle_path.display().to_string(),
        })
        .await
        .expect("apply patch stream");
    let mut apply_completion = None;
    while let Some(event) = apply_stream
        .message()
        .await
        .expect("read apply stream event")
    {
        if let Some(runtime_v2::apply_linux_vm_patch_event::Payload::Completion(done)) =
            event.payload
        {
            apply_completion = Some(done);
            break;
        }
    }
    let apply_completion = apply_completion.expect("apply completion");
    assert!(!apply_completion.receipt_id.trim().is_empty());
    let rollback_id = apply_completion.rollback_id.clone();
    let patched_base = apply_completion.base.expect("patched base");
    assert_eq!(patched_base.description, "after");

    let receipt = client
        .get_receipt(runtime_v2::GetReceiptRequest {
            receipt_id: apply_completion.receipt_id.clone(),
            metadata: None,
        })
        .await
        .expect("get apply receipt");
    assert!(
        receipt.receipt.is_some(),
        "apply receipt payload should exist"
    );

    let mut rollback_stream = client
        .rollback_linux_vm_patch_stream(runtime_v2::RollbackLinuxVmPatchRequest {
            metadata: None,
            rollback_id: rollback_id.clone(),
        })
        .await
        .expect("rollback stream");
    let mut rollback_completion = None;
    while let Some(event) = rollback_stream
        .message()
        .await
        .expect("read rollback stream event")
    {
        if let Some(runtime_v2::rollback_linux_vm_patch_event::Payload::Completion(done)) =
            event.payload
        {
            rollback_completion = Some(done);
            break;
        }
    }
    let rollback_completion = rollback_completion.expect("rollback completion");
    let rolled_base = rollback_completion.base.expect("rolled base");
    assert_eq!(rolled_base.description, "before");

    let incompatible_bundle = tmp.path().join("patch-missing.json");
    std::fs::write(
        &incompatible_bundle,
        "{\"schema_version\":1,\"patch_id\":\"patch-missing\",\"base_id\":\"missing-base\",\"set\":{\"description\":\"x\"}}",
    )
    .expect("write incompatible bundle");
    let incompatible_error = client
        .apply_linux_vm_patch_stream(runtime_v2::ApplyLinuxVmPatchRequest {
            metadata: None,
            bundle_path: incompatible_bundle.display().to_string(),
        })
        .await
        .expect_err("apply patch should fail for missing base");
    assert_grpc_status_in(incompatible_error, &[Code::NotFound]);

    daemon.stop().await;
}
