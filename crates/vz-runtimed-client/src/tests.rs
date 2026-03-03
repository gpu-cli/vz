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
            labels: HashMap::new(),
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
            labels: HashMap::new(),
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
            labels: HashMap::new(),
        })
        .await
        .expect("create sandbox stream");

    let mut saw_progress = false;
    let mut completion = None;
    while let Some(event) = stream.message().await.expect("read create sandbox stream") {
        match event.payload {
            Some(runtime_v2::create_sandbox_event::Payload::Progress(progress)) => {
                saw_progress = true;
                assert!(!progress.phase.trim().is_empty());
            }
            Some(runtime_v2::create_sandbox_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            None => {}
        }
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

    let response = client
        .create_sandbox_with_metadata(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "sandbox-receipt-header".to_string(),
            cpus: 1,
            memory_mb: 256,
            labels: HashMap::new(),
        })
        .await
        .expect("create sandbox with metadata");

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
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let stack_name = "stack-client-e2e".to_string();
    let applied = client
        .apply_stack_with_metadata(runtime_v2::ApplyStackRequest {
            metadata: None,
            stack_name: stack_name.clone(),
            compose_yaml: "services: {}\n".to_string(),
            compose_dir: ".".to_string(),
            detach: false,
            dry_run: false,
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
async fn heartbeat_lease_round_trip_and_signal_exec_missing_returns_not_found() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let sandbox = client
        .create_sandbox(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "client-heartbeat-sandbox".to_string(),
            cpus: 1,
            memory_mb: 128,
            labels: HashMap::new(),
        })
        .await
        .expect("create sandbox");
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
async fn stack_auxiliary_methods_and_event_stream_paths_are_covered() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let daemon = start_daemon(runtimed_config(&tmp)).await;
    let mut client = DaemonClient::connect_with_config(client_config(&tmp, false))
        .await
        .expect("client connect");

    let stack_name = "stack-client-aux".to_string();
    client
        .apply_stack(runtime_v2::ApplyStackRequest {
            metadata: None,
            stack_name: stack_name.clone(),
            compose_yaml: "services: {}\n".to_string(),
            compose_dir: ".".to_string(),
            dry_run: false,
            detach: false,
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
        })
        .await;
    if let Err(error) = logs_result {
        assert_grpc_status_in(error, &[Code::NotFound, Code::Unimplemented]);
    }

    let stop_error = client
        .stop_stack_service(runtime_v2::StackServiceActionRequest {
            metadata: None,
            stack_name: "stack-missing-client".to_string(),
            service_name: "svc".to_string(),
        })
        .await
        .expect_err("stop stack service should fail for missing stack/service");
    assert_grpc_status_in(stop_error, &[Code::NotFound, Code::FailedPrecondition]);

    let start_error = client
        .start_stack_service(runtime_v2::StackServiceActionRequest {
            metadata: None,
            stack_name: "stack-missing-client".to_string(),
            service_name: "svc".to_string(),
        })
        .await
        .expect_err("start stack service should fail for missing stack/service");
    assert_grpc_status_in(start_error, &[Code::NotFound, Code::FailedPrecondition]);

    let restart_error = client
        .restart_stack_service(runtime_v2::StackServiceActionRequest {
            metadata: None,
            stack_name: "stack-missing-client".to_string(),
            service_name: "svc".to_string(),
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

    let sandbox = client
        .create_sandbox(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "client-file-rpc-sandbox".to_string(),
            cpus: 1,
            memory_mb: 128,
            labels: HashMap::new(),
        })
        .await
        .expect("create sandbox");
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
