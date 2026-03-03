use std::collections::{BTreeMap, HashSet};
#[cfg(target_os = "linux")]
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use tonic::Code;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use vz_runtime_contract::{
    PolicyDecision, RequestMetadata, RuntimeOperation, RuntimePolicyHook,
    SANDBOX_LABEL_PROJECT_DIR, SANDBOX_LABEL_SPACE_MODE, SANDBOX_LABEL_SPACE_SECRET_ENV_PREFIX,
    SANDBOX_SPACE_MODE_REQUIRED, SandboxBackend,
};

use super::*;
use crate::{RuntimeDaemon, RuntimedConfig};

#[cfg(target_os = "macos")]
fn require_virtualization_entitlement() -> bool {
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
    if entitlements.contains("com.apple.security.virtualization") {
        return true;
    }
    eprintln!(
        "skipping runtimed interactive e2e: test binary is missing com.apple.security.virtualization entitlement"
    );
    false
}

#[cfg(not(target_os = "macos"))]
fn require_virtualization_entitlement() -> bool {
    true
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

async fn connect_capability_client(
    socket_path: &Path,
) -> runtime_v2::capability_service_client::CapabilityServiceClient<Channel> {
    let socket_path = socket_path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .expect("connect channel");
    runtime_v2::capability_service_client::CapabilityServiceClient::new(channel)
}

async fn connect_sandbox_client(
    socket_path: &Path,
) -> runtime_v2::sandbox_service_client::SandboxServiceClient<Channel> {
    let socket_path = socket_path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .expect("connect channel");
    runtime_v2::sandbox_service_client::SandboxServiceClient::new(channel)
}

async fn connect_lease_client(
    socket_path: &Path,
) -> runtime_v2::lease_service_client::LeaseServiceClient<Channel> {
    let socket_path = socket_path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .expect("connect channel");
    runtime_v2::lease_service_client::LeaseServiceClient::new(channel)
}

async fn connect_container_client(
    socket_path: &Path,
) -> runtime_v2::container_service_client::ContainerServiceClient<Channel> {
    let socket_path = socket_path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .expect("connect channel");
    runtime_v2::container_service_client::ContainerServiceClient::new(channel)
}

async fn connect_image_client(
    socket_path: &Path,
) -> runtime_v2::image_service_client::ImageServiceClient<Channel> {
    let socket_path = socket_path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .expect("connect channel");
    runtime_v2::image_service_client::ImageServiceClient::new(channel)
}

async fn connect_build_client(
    socket_path: &Path,
) -> runtime_v2::build_service_client::BuildServiceClient<Channel> {
    let socket_path = socket_path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .expect("connect channel");
    runtime_v2::build_service_client::BuildServiceClient::new(channel)
}

async fn connect_execution_client(
    socket_path: &Path,
) -> runtime_v2::execution_service_client::ExecutionServiceClient<Channel> {
    let socket_path = socket_path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .expect("connect channel");
    runtime_v2::execution_service_client::ExecutionServiceClient::new(channel)
}

async fn connect_checkpoint_client(
    socket_path: &Path,
) -> runtime_v2::checkpoint_service_client::CheckpointServiceClient<Channel> {
    let socket_path = socket_path.to_path_buf();
    let mut connect_error = None;
    let mut connected_channel = None;
    for _ in 0..40 {
        let connect = Endpoint::try_from("http://[::]:50051")
            .expect("endpoint")
            .connect_with_connector(service_fn({
                let socket_path = socket_path.clone();
                move |_: Uri| {
                    let socket_path = socket_path.clone();
                    async move {
                        tokio::net::UnixStream::connect(socket_path)
                            .await
                            .map(TokioIo::new)
                    }
                }
            }))
            .await;
        match connect {
            Ok(channel) => {
                connected_channel = Some(channel);
                break;
            }
            Err(error) => {
                connect_error = Some(error);
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
    let channel = connected_channel.unwrap_or_else(|| {
        panic!(
            "connect channel: {}",
            connect_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "unknown connection error".to_string())
        )
    });
    runtime_v2::checkpoint_service_client::CheckpointServiceClient::new(channel)
}

async fn connect_event_client(
    socket_path: &Path,
) -> runtime_v2::event_service_client::EventServiceClient<Channel> {
    let socket_path = socket_path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .expect("connect channel");
    runtime_v2::event_service_client::EventServiceClient::new(channel)
}

async fn connect_stack_client(
    socket_path: &Path,
) -> runtime_v2::stack_service_client::StackServiceClient<Channel> {
    let socket_path = socket_path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .expect("connect channel");
    runtime_v2::stack_service_client::StackServiceClient::new(channel)
}

async fn connect_file_client(
    socket_path: &Path,
) -> runtime_v2::file_service_client::FileServiceClient<Channel> {
    let socket_path = socket_path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(socket_path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .expect("connect channel");
    runtime_v2::file_service_client::FileServiceClient::new(channel)
}

async fn read_open_sandbox_shell_completion(
    stream: &mut tonic::Streaming<runtime_v2::OpenSandboxShellEvent>,
) -> runtime_v2::OpenSandboxShellResponse {
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .expect("read open sandbox shell stream event")
    {
        if let Some(runtime_v2::open_sandbox_shell_event::Payload::Completion(done)) = event.payload
        {
            completion = Some(done);
        }
    }
    completion.expect("expected terminal open sandbox shell completion event")
}

async fn read_create_sandbox_completion(
    stream: &mut tonic::Streaming<runtime_v2::CreateSandboxEvent>,
) -> runtime_v2::SandboxResponse {
    try_read_create_sandbox_completion(stream)
        .await
        .expect("expected terminal create sandbox completion event")
}

async fn try_read_create_sandbox_completion(
    stream: &mut tonic::Streaming<runtime_v2::CreateSandboxEvent>,
) -> Result<runtime_v2::SandboxResponse, tonic::Status> {
    let mut completion = None;
    while let Some(event) = stream.message().await? {
        if let Some(runtime_v2::create_sandbox_event::Payload::Completion(done)) = event.payload {
            completion = Some(done);
        }
    }
    completion
        .ok_or_else(|| tonic::Status::internal("missing create sandbox completion event"))?
        .response
        .ok_or_else(|| tonic::Status::internal("create sandbox completion missing response"))
}

async fn read_create_sandbox_completion_response(
    response: tonic::Response<tonic::Streaming<runtime_v2::CreateSandboxEvent>>,
) -> runtime_v2::SandboxResponse {
    let mut stream = response.into_inner();
    read_create_sandbox_completion(&mut stream).await
}

async fn read_close_sandbox_shell_completion(
    stream: &mut tonic::Streaming<runtime_v2::CloseSandboxShellEvent>,
) -> runtime_v2::CloseSandboxShellResponse {
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .expect("read close sandbox shell stream event")
    {
        if let Some(runtime_v2::close_sandbox_shell_event::Payload::Completion(done)) =
            event.payload
        {
            completion = Some(done);
        }
    }
    completion.expect("expected terminal close sandbox shell completion event")
}

async fn read_terminate_sandbox_completion(
    stream: &mut tonic::Streaming<runtime_v2::TerminateSandboxEvent>,
) -> runtime_v2::SandboxResponse {
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .expect("read terminate sandbox stream event")
    {
        if let Some(runtime_v2::terminate_sandbox_event::Payload::Completion(done)) = event.payload
        {
            completion = Some(done);
        }
    }
    completion
        .expect("expected terminal terminate sandbox completion event")
        .response
        .expect("terminate sandbox completion should include response")
}

async fn read_terminate_sandbox_completion_response(
    response: tonic::Response<tonic::Streaming<runtime_v2::TerminateSandboxEvent>>,
) -> runtime_v2::SandboxResponse {
    let mut stream = response.into_inner();
    read_terminate_sandbox_completion(&mut stream).await
}

async fn read_apply_stack_completion(
    stream: &mut tonic::Streaming<runtime_v2::ApplyStackEvent>,
) -> runtime_v2::ApplyStackResponse {
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .expect("read apply stack stream event")
    {
        if let Some(runtime_v2::apply_stack_event::Payload::Completion(done)) = event.payload {
            completion = Some(done);
        }
    }
    completion
        .expect("expected terminal apply stack completion event")
        .response
        .expect("apply stack completion should include response")
}

async fn read_apply_stack_completion_response(
    response: tonic::Response<tonic::Streaming<runtime_v2::ApplyStackEvent>>,
) -> runtime_v2::ApplyStackResponse {
    let mut stream = response.into_inner();
    read_apply_stack_completion(&mut stream).await
}

async fn read_teardown_stack_completion(
    stream: &mut tonic::Streaming<runtime_v2::TeardownStackEvent>,
) -> runtime_v2::TeardownStackResponse {
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .expect("read teardown stack stream event")
    {
        if let Some(runtime_v2::teardown_stack_event::Payload::Completion(done)) = event.payload {
            completion = Some(done);
        }
    }
    completion
        .expect("expected terminal teardown stack completion event")
        .response
        .expect("teardown stack completion should include response")
}

async fn read_teardown_stack_completion_response(
    response: tonic::Response<tonic::Streaming<runtime_v2::TeardownStackEvent>>,
) -> runtime_v2::TeardownStackResponse {
    let mut stream = response.into_inner();
    read_teardown_stack_completion(&mut stream).await
}

#[cfg(target_os = "linux")]
async fn read_export_checkpoint_completion(
    stream: &mut tonic::Streaming<runtime_v2::ExportCheckpointEvent>,
) -> runtime_v2::ExportCheckpointCompletion {
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .expect("read export checkpoint stream event")
    {
        if let Some(runtime_v2::export_checkpoint_event::Payload::Completion(done)) = event.payload
        {
            completion = Some(done);
        }
    }
    completion.expect("expected terminal export checkpoint completion event")
}

#[cfg(target_os = "linux")]
async fn read_export_checkpoint_completion_response(
    response: tonic::Response<tonic::Streaming<runtime_v2::ExportCheckpointEvent>>,
) -> runtime_v2::ExportCheckpointCompletion {
    let mut stream = response.into_inner();
    read_export_checkpoint_completion(&mut stream).await
}

#[cfg(target_os = "linux")]
async fn read_import_checkpoint_completion(
    stream: &mut tonic::Streaming<runtime_v2::ImportCheckpointEvent>,
) -> runtime_v2::ImportCheckpointCompletion {
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .expect("read import checkpoint stream event")
    {
        if let Some(runtime_v2::import_checkpoint_event::Payload::Completion(done)) = event.payload
        {
            completion = Some(done);
        }
    }
    completion.expect("expected terminal import checkpoint completion event")
}

#[cfg(target_os = "linux")]
async fn read_import_checkpoint_completion_response(
    response: tonic::Response<tonic::Streaming<runtime_v2::ImportCheckpointEvent>>,
) -> runtime_v2::ImportCheckpointCompletion {
    let mut stream = response.into_inner();
    read_import_checkpoint_completion(&mut stream).await
}

struct DenyCreateSandboxPolicyHook;

impl RuntimePolicyHook for DenyCreateSandboxPolicyHook {
    fn evaluate(
        &self,
        operation: RuntimeOperation,
        _metadata: &RequestMetadata,
    ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
        if operation == RuntimeOperation::CreateSandbox {
            Ok(PolicyDecision::Deny {
                reason: "blocked by daemon policy".to_string(),
            })
        } else {
            Ok(PolicyDecision::Allow)
        }
    }
}

struct FailingCreateSandboxPolicyHook;

impl RuntimePolicyHook for FailingCreateSandboxPolicyHook {
    fn evaluate(
        &self,
        operation: RuntimeOperation,
        _metadata: &RequestMetadata,
    ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
        if operation == RuntimeOperation::CreateSandbox {
            Err(Box::new(std::io::Error::other("policy backend offline")))
        } else {
            Ok(PolicyDecision::Allow)
        }
    }
}

#[tokio::test]
async fn uds_server_exposes_capabilities_with_health_headers() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_capability_client(&config.socket_path).await;
    let response = client
        .get_capabilities(Request::new(runtime_v2::GetCapabilitiesRequest {
            metadata: None,
        }))
        .await
        .expect("capabilities call")
        .into_inner();

    assert!(!response.request_id.is_empty());

    let mut client = connect_capability_client(&config.socket_path).await;
    let response = client
        .get_capabilities(Request::new(runtime_v2::GetCapabilitiesRequest {
            metadata: None,
        }))
        .await
        .expect("capabilities call");

    let headers = response.metadata();
    assert!(headers.get("x-vz-runtimed-id").is_some());
    assert!(headers.get("x-vz-runtimed-version").is_some());
    assert!(headers.get("x-vz-runtimed-backend").is_some());
    assert!(headers.get("x-vz-runtimed-started-at").is_some());

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_rejects_empty_stack_name() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let status = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "   ".to_string(),
            cpus: 0,
            memory_mb: 0,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect_err("create_sandbox should reject empty stack_name")
        .code();
    assert_eq!(status, tonic::Code::InvalidArgument);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_requires_project_dir_label() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let response = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-spaces-missing-project-dir".to_string(),
            cpus: 1,
            memory_mb: 512,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect("create_sandbox call");
    let mut stream = response.into_inner();
    let status = try_read_create_sandbox_completion(&mut stream)
        .await
        .expect_err("spaces mode should fail without project_dir");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains(SANDBOX_LABEL_PROJECT_DIR),
        "error should mention missing project_dir label"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_spaces_mode_rejects_non_btrfs_workspace_storage() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    #[cfg(target_os = "linux")]
    let project_dir = "/proc".to_string();
    #[cfg(not(target_os = "linux"))]
    let project_dir = {
        let workspace = tmp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("create workspace dir");
        workspace.to_string_lossy().to_string()
    };

    let mut labels = std::collections::HashMap::new();
    labels.insert(
        SANDBOX_LABEL_SPACE_MODE.to_string(),
        SANDBOX_SPACE_MODE_REQUIRED.to_string(),
    );
    labels.insert(SANDBOX_LABEL_PROJECT_DIR.to_string(), project_dir);

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let response = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-spaces-preflight".to_string(),
            cpus: 1,
            memory_mb: 512,
            labels,
        }))
        .await
        .expect("create_sandbox call");
    let mut stream = response.into_inner();
    let status = try_read_create_sandbox_completion(&mut stream)
        .await
        .expect_err("spaces mode should fail when btrfs preflight fails");
    assert_eq!(status.code(), tonic::Code::Unimplemented);
    assert!(
        status.message().contains("btrfs workspace storage"),
        "error should mention btrfs storage requirement"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_rejects_non_spaces_mode_label_value() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let workspace_dir = tmp.path().join("workspace");
    std::fs::create_dir_all(&workspace_dir).expect("create workspace dir");

    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut labels = std::collections::HashMap::new();
    labels.insert(SANDBOX_LABEL_SPACE_MODE.to_string(), "legacy".to_string());
    labels.insert(
        SANDBOX_LABEL_PROJECT_DIR.to_string(),
        workspace_dir.to_string_lossy().to_string(),
    );

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let response = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-invalid-spaces-mode".to_string(),
            cpus: 1,
            memory_mb: 512,
            labels,
        }))
        .await
        .expect("create_sandbox call");
    let mut stream = response.into_inner();
    let status = try_read_create_sandbox_completion(&mut stream)
        .await
        .expect_err("invalid spaces mode should be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains(SANDBOX_LABEL_SPACE_MODE));

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_writes_policy_allow_audit_receipt() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let created = read_create_sandbox_completion_response(
        client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: "req-policy-allow".to_string(),
                    idempotency_key: "".to_string(),
                    trace_id: "trace-policy-allow".to_string(),
                }),
                stack_name: "stack-policy-allow".to_string(),
                cpus: 1,
                memory_mb: 512,
                labels: std::collections::HashMap::new(),
            }))
            .await
            .expect("create sandbox"),
    )
    .await;
    assert_eq!(created.request_id, "req-policy-allow");

    let policy_receipts = daemon
        .with_state_store(|store| store.list_receipts_for_entity("policy", "req-policy-allow"))
        .expect("load policy receipts");
    assert_eq!(policy_receipts.len(), 1);
    let receipt = &policy_receipts[0];
    assert_eq!(receipt.operation, "policy_preflight:create_sandbox");
    assert_eq!(receipt.status, "allow");
    assert_eq!(
        receipt
            .metadata
            .get("operation")
            .and_then(serde_json::Value::as_str),
        Some("create_sandbox")
    );
    assert_eq!(
        receipt
            .metadata
            .get("decision")
            .and_then(serde_json::Value::as_str),
        Some("allow")
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_policy_deny_returns_permission_denied_and_audit_receipt() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(
        RuntimeDaemon::start_with_policy_hook(
            config.clone(),
            Arc::new(DenyCreateSandboxPolicyHook),
            Some("policy-v1".to_string()),
        )
        .expect("daemon start"),
    );
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let error = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-policy-deny".to_string(),
                idempotency_key: "".to_string(),
                trace_id: "".to_string(),
            }),
            stack_name: "stack-policy-deny".to_string(),
            cpus: 1,
            memory_mb: 512,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect_err("policy deny should reject mutation");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);

    let policy_receipts = daemon
        .with_state_store(|store| store.list_receipts_for_entity("policy", "req-policy-deny"))
        .expect("load policy receipts");
    assert_eq!(policy_receipts.len(), 1);
    let receipt = &policy_receipts[0];
    assert_eq!(receipt.operation, "policy_preflight:create_sandbox");
    assert_eq!(receipt.status, "deny");
    assert_eq!(
        receipt
            .metadata
            .get("machine_code")
            .and_then(serde_json::Value::as_str),
        Some("policy_denied")
    );
    assert_eq!(
        receipt
            .metadata
            .get("policy_hash")
            .and_then(serde_json::Value::as_str),
        Some("policy-v1")
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_policy_transport_failure_is_unavailable_and_audited() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(
        RuntimeDaemon::start_with_policy_hook(
            config.clone(),
            Arc::new(FailingCreateSandboxPolicyHook),
            Some("policy-v2".to_string()),
        )
        .expect("daemon start"),
    );
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let error = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-policy-transport".to_string(),
                idempotency_key: "".to_string(),
                trace_id: "".to_string(),
            }),
            stack_name: "stack-policy-transport".to_string(),
            cpus: 1,
            memory_mb: 512,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect_err("policy transport failure should reject mutation");
    assert_eq!(error.code(), tonic::Code::Unavailable);

    let policy_receipts = daemon
        .with_state_store(|store| store.list_receipts_for_entity("policy", "req-policy-transport"))
        .expect("load policy receipts");
    assert_eq!(policy_receipts.len(), 1);
    let receipt = &policy_receipts[0];
    assert_eq!(receipt.operation, "policy_preflight:create_sandbox");
    assert_eq!(receipt.status, "error");
    assert_eq!(
        receipt
            .metadata
            .get("machine_code")
            .and_then(serde_json::Value::as_str),
        Some("backend_unavailable")
    );
    assert_eq!(
        receipt
            .metadata
            .get("policy_hash")
            .and_then(serde_json::Value::as_str),
        Some("policy-v2")
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_is_persisted_in_state_store() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let created = read_create_sandbox_completion_response(
        client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: "req-persist".to_string(),
                    idempotency_key: "".to_string(),
                    trace_id: "".to_string(),
                }),
                stack_name: "stack-a".to_string(),
                cpus: 2,
                memory_mb: 1024,
                labels: std::collections::HashMap::from([("env".to_string(), "test".to_string())]),
            }))
            .await
            .expect("create sandbox"),
    )
    .await;
    let sandbox_id = created
        .sandbox
        .as_ref()
        .expect("sandbox payload")
        .sandbox_id
        .clone();

    let fetched = client
        .get_sandbox(Request::new(runtime_v2::GetSandboxRequest {
            sandbox_id: sandbox_id.clone(),
            metadata: None,
        }))
        .await
        .expect("get sandbox")
        .into_inner();
    assert_eq!(
        fetched.sandbox.expect("sandbox payload").sandbox_id,
        sandbox_id
    );

    let listed = client
        .list_sandboxes(Request::new(runtime_v2::ListSandboxesRequest {
            metadata: None,
        }))
        .await
        .expect("list sandboxes")
        .into_inner();
    assert_eq!(listed.sandboxes.len(), 1);

    let sandbox_receipts = daemon
        .with_state_store(|store| store.list_receipts_for_entity("sandbox", &sandbox_id))
        .expect("list sandbox receipts");
    let create_receipt = sandbox_receipts
        .iter()
        .find(|receipt| receipt.operation == "create_sandbox")
        .expect("create_sandbox receipt");
    assert_eq!(
        create_receipt
            .metadata
            .get("event_type")
            .and_then(serde_json::Value::as_str),
        Some("sandbox_ready")
    );
    assert!(
        create_receipt
            .metadata
            .get("request_hash")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|hash| !hash.is_empty())
    );
    assert!(
        create_receipt
            .metadata
            .get("idempotency_key")
            .is_some_and(serde_json::Value::is_null)
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_stream_slow_consumer_preserves_order_and_completion() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let mut stream = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-slow-consumer".to_string(),
            cpus: 1,
            memory_mb: 512,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect("create sandbox stream")
        .into_inner();

    let mut last_sequence = 0_u64;
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .expect("read create sandbox stream event")
    {
        assert!(
            event.sequence > last_sequence,
            "expected strictly increasing stream sequence numbers"
        );
        last_sequence = event.sequence;
        tokio::time::sleep(Duration::from_millis(15)).await;
        if let Some(runtime_v2::create_sandbox_event::Payload::Completion(done)) = event.payload {
            completion = Some(done);
        }
    }

    let completion = completion.expect("expected terminal create sandbox completion event");
    let response = completion
        .response
        .expect("completion should include sandbox response");
    assert_eq!(
        response
            .sandbox
            .expect("sandbox payload should exist")
            .sandbox_id,
        "stack-slow-consumer"
    );
    assert!(
        !completion.receipt_id.trim().is_empty(),
        "completion should include receipt id"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_applies_legacy_base_image_default_and_audit_label() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let mut labels = std::collections::HashMap::new();
    labels.insert(
        SANDBOX_LABEL_BASE_IMAGE_DEFAULT_SOURCE.to_string(),
        "policy_config".to_string(),
    );
    let mut stream = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-defaults-legacy-fallback".to_string(),
            cpus: 1,
            memory_mb: 512,
            labels,
        }))
        .await
        .expect("create sandbox stream")
        .into_inner();

    let mut saw_defaults_phase = false;
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .expect("read create sandbox stream event")
    {
        match event.payload {
            Some(runtime_v2::create_sandbox_event::Payload::Progress(progress))
                if progress.phase == "applying_defaults" =>
            {
                saw_defaults_phase = true;
            }
            Some(runtime_v2::create_sandbox_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            _ => {}
        }
    }

    let created = completion.expect("completion payload");
    let sandbox = created
        .response
        .and_then(|value| value.sandbox)
        .expect("sandbox payload");
    assert_eq!(
        sandbox
            .labels
            .get(SANDBOX_LABEL_BASE_IMAGE_REF)
            .map(String::as_str),
        Some("debian:bookworm")
    );
    assert_eq!(
        sandbox
            .labels
            .get(SANDBOX_LABEL_BASE_IMAGE_DEFAULT_SOURCE)
            .map(String::as_str),
        Some("compat_legacy")
    );
    assert!(
        !sandbox
            .labels
            .contains_key(SANDBOX_LABEL_MAIN_CONTAINER_DEFAULT_SOURCE),
        "main container default source should be absent when no default applied"
    );
    assert!(
        saw_defaults_phase,
        "create sandbox stream should include applying_defaults phase when defaults are injected"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_strips_spoofed_default_source_labels_when_defaults_not_applied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let mut labels = std::collections::HashMap::new();
    labels.insert(
        SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
        "alpine:3.20".to_string(),
    );
    labels.insert(
        SANDBOX_LABEL_MAIN_CONTAINER.to_string(),
        "workspace-main".to_string(),
    );
    labels.insert(
        SANDBOX_LABEL_BASE_IMAGE_DEFAULT_SOURCE.to_string(),
        "compat_legacy".to_string(),
    );
    labels.insert(
        SANDBOX_LABEL_MAIN_CONTAINER_DEFAULT_SOURCE.to_string(),
        "policy_config".to_string(),
    );
    let mut stream = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-defaults-explicit-values".to_string(),
            cpus: 1,
            memory_mb: 512,
            labels,
        }))
        .await
        .expect("create sandbox stream")
        .into_inner();

    let mut saw_defaults_phase = false;
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .expect("read create sandbox stream event")
    {
        match event.payload {
            Some(runtime_v2::create_sandbox_event::Payload::Progress(progress))
                if progress.phase == "applying_defaults" =>
            {
                saw_defaults_phase = true;
            }
            Some(runtime_v2::create_sandbox_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            _ => {}
        }
    }

    let created = completion.expect("completion payload");
    let sandbox = created
        .response
        .and_then(|value| value.sandbox)
        .expect("sandbox payload");
    assert_eq!(
        sandbox
            .labels
            .get(SANDBOX_LABEL_BASE_IMAGE_REF)
            .map(String::as_str),
        Some("alpine:3.20")
    );
    assert_eq!(
        sandbox
            .labels
            .get(SANDBOX_LABEL_MAIN_CONTAINER)
            .map(String::as_str),
        Some("workspace-main")
    );
    assert!(
        !sandbox
            .labels
            .contains_key(SANDBOX_LABEL_BASE_IMAGE_DEFAULT_SOURCE),
        "default source labels should not be trusted from request input"
    );
    assert!(
        !sandbox
            .labels
            .contains_key(SANDBOX_LABEL_MAIN_CONTAINER_DEFAULT_SOURCE),
        "default source labels should not be trusted from request input"
    );
    assert!(
        !saw_defaults_phase,
        "create sandbox stream should not emit applying_defaults when no defaults are injected"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn open_sandbox_shell_creates_container_and_resolves_default_shell() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut labels = std::collections::HashMap::new();
    labels.insert(
        SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
        "debian:bookworm".to_string(),
    );
    labels.insert(
        SANDBOX_LABEL_MAIN_CONTAINER.to_string(),
        "workspace-main".to_string(),
    );

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-open-shell-default".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels,
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut opened_stream = sandbox_client
        .open_sandbox_shell(Request::new(runtime_v2::OpenSandboxShellRequest {
            sandbox_id: sandbox.sandbox_id.clone(),
            metadata: None,
        }))
        .await
        .expect("open sandbox shell")
        .into_inner();
    let opened = read_open_sandbox_shell_completion(&mut opened_stream).await;

    assert_eq!(opened.sandbox_id, sandbox.sandbox_id);
    assert!(!opened.container_id.is_empty());
    assert!(!opened.execution_id.is_empty());
    assert_eq!(opened.cmd, vec!["/bin/bash".to_string()]);
    assert!(opened.args.is_empty());

    let persisted = daemon
        .with_state_store(|store| store.load_container(&opened.container_id))
        .expect("load container from state store")
        .expect("container should be persisted");
    assert_eq!(persisted.sandbox_id, sandbox.sandbox_id);
    assert!(
        persisted.container_spec.cwd.is_none(),
        "sandbox shell keepalive container should not force `/workspace` cwd when no workspace mount is configured"
    );
    let persisted_execution = daemon
        .with_state_store(|store| store.load_execution(&opened.execution_id))
        .expect("load execution from state store")
        .expect("execution should be persisted");
    assert_eq!(persisted_execution.container_id, opened.container_id);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn open_sandbox_shell_prefers_main_container_command_override() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut labels = std::collections::HashMap::new();
    labels.insert(
        SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
        "alpine:3.20".to_string(),
    );
    labels.insert(
        SANDBOX_LABEL_MAIN_CONTAINER.to_string(),
        "bash -lc \"echo ready\"".to_string(),
    );

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-open-shell-main-container".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels,
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut opened_stream = sandbox_client
        .open_sandbox_shell(Request::new(runtime_v2::OpenSandboxShellRequest {
            sandbox_id: sandbox.sandbox_id,
            metadata: None,
        }))
        .await
        .expect("open sandbox shell")
        .into_inner();
    let opened = read_open_sandbox_shell_completion(&mut opened_stream).await;

    assert!(!opened.execution_id.is_empty());
    assert_eq!(opened.cmd, vec!["bash".to_string()]);
    assert_eq!(
        opened.args,
        vec!["-lc".to_string(), "echo ready".to_string()]
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn open_sandbox_shell_reuses_existing_active_execution_session() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut labels = std::collections::HashMap::new();
    labels.insert(
        SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
        "debian:bookworm".to_string(),
    );

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-open-shell-reuse".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels,
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut container_client = connect_container_client(&config.socket_path).await;
    let container = container_client
        .create_container(Request::new(runtime_v2::CreateContainerRequest {
            metadata: None,
            sandbox_id: sandbox.sandbox_id.clone(),
            image_digest: "debian:bookworm".to_string(),
            cmd: vec![
                "/bin/sh".to_string(),
                "-lc".to_string(),
                "while :; do sleep 3600; done".to_string(),
            ],
            env: std::collections::HashMap::new(),
            cwd: "/workspace".to_string(),
            user: String::new(),
        }))
        .await
        .expect("create container")
        .into_inner()
        .container
        .expect("container payload");

    let existing_execution = Execution {
        execution_id: "exec-open-shell-reuse".to_string(),
        container_id: container.container_id.clone(),
        exec_spec: ExecutionSpec {
            cmd: vec!["/bin/bash".to_string()],
            args: Vec::new(),
            env_override: BTreeMap::from([(
                SANDBOX_SHELL_SESSION_ENV_KEY.to_string(),
                "1".to_string(),
            )]),
            pty: true,
            timeout_secs: None,
        },
        state: ExecutionState::Running,
        exit_code: None,
        started_at: Some(current_unix_secs()),
        ended_at: None,
    };
    daemon
        .with_state_store(|store| store.save_execution(&existing_execution))
        .expect("persist existing execution");
    daemon
        .execution_sessions()
        .register(&existing_execution.execution_id)
        .expect("register existing execution session");

    let mut opened_stream = sandbox_client
        .open_sandbox_shell(Request::new(runtime_v2::OpenSandboxShellRequest {
            sandbox_id: sandbox.sandbox_id,
            metadata: None,
        }))
        .await
        .expect("open sandbox shell")
        .into_inner();
    let opened = read_open_sandbox_shell_completion(&mut opened_stream).await;

    assert_eq!(opened.execution_id, existing_execution.execution_id);
    let executions = daemon
        .with_state_store(|store| store.list_executions())
        .expect("list executions");
    assert_eq!(executions.len(), 1);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn open_sandbox_shell_persists_external_secret_env_references_only() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut labels = std::collections::HashMap::new();
    labels.insert(
        SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
        "debian:bookworm".to_string(),
    );
    labels.insert(
        format!("{SANDBOX_LABEL_SPACE_SECRET_ENV_PREFIX}workspace_home"),
        "HOME".to_string(),
    );

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-open-shell-external-secret".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels,
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut opened_stream = sandbox_client
        .open_sandbox_shell(Request::new(runtime_v2::OpenSandboxShellRequest {
            sandbox_id: sandbox.sandbox_id.clone(),
            metadata: None,
        }))
        .await
        .expect("open sandbox shell")
        .into_inner();
    let opened = read_open_sandbox_shell_completion(&mut opened_stream).await;

    let persisted_execution = daemon
        .with_state_store(|store| store.load_execution(&opened.execution_id))
        .expect("load execution from state store")
        .expect("execution should be persisted");
    let home = std::env::var("HOME").expect("HOME should be set for test");
    let expected_reference = format!("{SANDBOX_RUNTIME_ENV_REF_PREFIX}HOME");
    assert_eq!(
        persisted_execution
            .exec_spec
            .env_override
            .get("workspace_home")
            .map(String::as_str),
        Some(expected_reference.as_str())
    );
    assert!(
        persisted_execution
            .exec_spec
            .env_override
            .values()
            .all(|value| value != home.as_str()),
        "execution spec should not persist raw secret env values"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn open_sandbox_shell_rejects_missing_external_secret_env_source() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let missing_env_var = "VZ_RUNTIME_TEST_MISSING_SPACE_SECRET_8319";
    let mut labels = std::collections::HashMap::new();
    labels.insert(
        SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
        "debian:bookworm".to_string(),
    );
    labels.insert(
        format!("{SANDBOX_LABEL_SPACE_SECRET_ENV_PREFIX}db_password"),
        missing_env_var.to_string(),
    );

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-open-shell-missing-external-secret".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels,
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut opened_stream = sandbox_client
        .open_sandbox_shell(Request::new(runtime_v2::OpenSandboxShellRequest {
            sandbox_id: sandbox.sandbox_id,
            metadata: None,
        }))
        .await
        .expect("open sandbox shell request should be accepted")
        .into_inner();
    let status = loop {
        match opened_stream.message().await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("expected open_sandbox_shell stream to return an error"),
            Err(status) => break status,
        }
    };
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains(missing_env_var),
        "error should identify missing env var source"
    );
    assert!(
        !status.message().contains("super-secret"),
        "error should not leak secret values"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn close_sandbox_shell_closes_active_execution_and_clears_session() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut labels = std::collections::HashMap::new();
    labels.insert(
        SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
        "debian:bookworm".to_string(),
    );

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-close-shell".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels,
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut opened_stream = sandbox_client
        .open_sandbox_shell(Request::new(runtime_v2::OpenSandboxShellRequest {
            sandbox_id: sandbox.sandbox_id.clone(),
            metadata: None,
        }))
        .await
        .expect("open sandbox shell")
        .into_inner();
    let opened = read_open_sandbox_shell_completion(&mut opened_stream).await;
    assert!(!opened.execution_id.is_empty());

    let mut close_stream = sandbox_client
        .close_sandbox_shell(Request::new(runtime_v2::CloseSandboxShellRequest {
            sandbox_id: sandbox.sandbox_id.clone(),
            execution_id: opened.execution_id.clone(),
            metadata: None,
        }))
        .await
        .expect("close sandbox shell")
        .into_inner();
    let closed = read_close_sandbox_shell_completion(&mut close_stream).await;
    assert_eq!(closed.sandbox_id, sandbox.sandbox_id);
    assert_eq!(closed.execution_id, opened.execution_id);

    let execution = daemon
        .with_state_store(|store| store.load_execution(&opened.execution_id))
        .expect("load execution")
        .expect("execution should exist");
    assert!(
        execution.state.is_terminal(),
        "close_sandbox_shell should leave execution in terminal state"
    );

    let has_session = daemon
        .execution_sessions()
        .contains(&opened.execution_id)
        .expect("check execution session");
    assert!(
        !has_session,
        "execution session should be removed after close_sandbox_shell"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn maintenance_reaps_orphaned_shell_execution_sessions() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let now = current_unix_secs();
    daemon
        .with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_sandbox(&Sandbox {
                    sandbox_id: "sandbox-orphan-shell".to_string(),
                    backend: SandboxBackend::MacosVz,
                    spec: SandboxSpec::default(),
                    state: SandboxState::Ready,
                    created_at: now.saturating_sub(10),
                    updated_at: now.saturating_sub(10),
                    labels: BTreeMap::new(),
                })?;
                tx.save_container(&Container {
                    container_id: "container-orphan-shell".to_string(),
                    sandbox_id: "sandbox-orphan-shell".to_string(),
                    image_digest: "debian:bookworm".to_string(),
                    container_spec: ContainerSpec::default(),
                    state: ContainerState::Running,
                    created_at: now.saturating_sub(10),
                    started_at: Some(now.saturating_sub(10)),
                    ended_at: None,
                })?;
                tx.save_execution(&Execution {
                    execution_id: "exec-orphan-shell".to_string(),
                    container_id: "container-orphan-shell".to_string(),
                    exec_spec: ExecutionSpec {
                        cmd: vec!["/bin/bash".to_string()],
                        args: Vec::new(),
                        env_override: BTreeMap::from([(
                            SANDBOX_SHELL_SESSION_ENV_KEY.to_string(),
                            "1".to_string(),
                        )]),
                        pty: true,
                        timeout_secs: None,
                    },
                    state: ExecutionState::Running,
                    exit_code: None,
                    started_at: Some(now.saturating_sub(10)),
                    ended_at: None,
                })?;
                Ok(())
            })
        })
        .expect("seed orphan shell execution");

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let execution = daemon
            .with_state_store(|store| store.load_execution("exec-orphan-shell"))
            .expect("load execution")
            .expect("execution exists");
        if execution.state == ExecutionState::Canceled {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "expected orphan shell execution to be canceled by maintenance, got {:?}",
                execution.state
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_denied_when_scheduler_capacity_is_exhausted() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    daemon.set_placement_limits_for_test(0, 1024);
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let denied = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-placement-sandbox-deny".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            stack_name: "stack-pressure".to_string(),
            cpus: 1,
            memory_mb: 256,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect_err("placement preflight should deny create_sandbox");
    assert_eq!(denied.code(), tonic::Code::Unavailable);

    let sandboxes = daemon
        .with_state_store(|store| store.list_sandboxes())
        .expect("list sandboxes");
    assert!(
        sandboxes.is_empty(),
        "denied create should not persist state"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_sandbox_honors_idempotency_key_and_conflict() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let first = read_create_sandbox_completion_response(
        client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: "req-idem-1".to_string(),
                    idempotency_key: "idem-key-a".to_string(),
                    trace_id: "".to_string(),
                }),
                stack_name: "stack-idem".to_string(),
                cpus: 1,
                memory_mb: 256,
                labels: std::collections::HashMap::new(),
            }))
            .await
            .expect("first create"),
    )
    .await;
    let first_id = first
        .sandbox
        .as_ref()
        .expect("sandbox payload")
        .sandbox_id
        .clone();

    let replay = read_create_sandbox_completion_response(
        client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: "req-idem-2".to_string(),
                    idempotency_key: "idem-key-a".to_string(),
                    trace_id: "".to_string(),
                }),
                stack_name: "stack-idem".to_string(),
                cpus: 1,
                memory_mb: 256,
                labels: std::collections::HashMap::new(),
            }))
            .await
            .expect("idempotent replay"),
    )
    .await;
    let replay_id = replay
        .sandbox
        .as_ref()
        .expect("sandbox payload")
        .sandbox_id
        .clone();
    assert_eq!(first_id, replay_id);

    let conflict = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-idem-3".to_string(),
                idempotency_key: "idem-key-a".to_string(),
                trace_id: "".to_string(),
            }),
            stack_name: "stack-idem-different".to_string(),
            cpus: 1,
            memory_mb: 256,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect_err("same idempotency key with different request should fail");
    assert_eq!(conflict.code(), tonic::Code::FailedPrecondition);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn open_lease_then_get_and_close_round_trip() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut lease_client = connect_lease_client(&config.socket_path).await;

    let opened = lease_client
        .open_lease(Request::new(runtime_v2::OpenLeaseRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-lease-open".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            sandbox_id: "sbx-lease-test".to_string(),
            ttl_secs: 60,
        }))
        .await
        .expect("open lease");
    assert!(opened.metadata().get("x-receipt-id").is_some());
    let opened = opened.into_inner();
    let lease_id = opened
        .lease
        .as_ref()
        .expect("lease payload")
        .lease_id
        .clone();
    assert_eq!(opened.lease.expect("lease payload").state, "active");

    let fetched = lease_client
        .get_lease(Request::new(runtime_v2::GetLeaseRequest {
            lease_id: lease_id.clone(),
            metadata: None,
        }))
        .await
        .expect("get lease")
        .into_inner();
    assert_eq!(
        fetched.lease.expect("lease payload").lease_id,
        lease_id,
        "lease id should round-trip"
    );

    let closed = lease_client
        .close_lease(Request::new(runtime_v2::CloseLeaseRequest {
            lease_id: lease_id.clone(),
            metadata: None,
        }))
        .await
        .expect("close lease");
    assert!(closed.metadata().get("x-receipt-id").is_some());
    assert_eq!(
        closed.into_inner().lease.expect("lease payload").state,
        "closed"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn start_build_then_get_and_cancel_round_trip() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut build_client = connect_build_client(&config.socket_path).await;
    let mut build_args = std::collections::HashMap::new();
    build_args.insert("FOO".to_string(), "bar".to_string());
    let secret_spec = "id=token,src=/tmp/token".to_string();

    let started = build_client
        .start_build(Request::new(runtime_v2::StartBuildRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-build-start".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            sandbox_id: "sbx-build-test".to_string(),
            context: ".".to_string(),
            dockerfile: "Dockerfile".to_string(),
            args: build_args,
            target: "release".to_string(),
            image_tag: "example:latest".to_string(),
            secrets: vec![secret_spec.clone()],
            no_cache: true,
            push: false,
            output_oci_tar_dest: "/tmp/build.oci.tar".to_string(),
        }))
        .await
        .expect("start build");
    assert!(started.metadata().get("x-receipt-id").is_some());
    let started = started.into_inner();
    let build_id = started
        .build
        .as_ref()
        .expect("build payload")
        .build_id
        .clone();
    assert_eq!(started.build.expect("build payload").state, "queued");

    let fetched = build_client
        .get_build(Request::new(runtime_v2::GetBuildRequest {
            build_id: build_id.clone(),
            metadata: None,
        }))
        .await
        .expect("get build")
        .into_inner();
    assert_eq!(
        fetched.build.expect("build payload").build_id,
        build_id,
        "build id should round-trip"
    );
    let persisted_build = daemon
        .with_state_store(|store| store.load_build(&build_id))
        .expect("load persisted build")
        .expect("persisted build");
    assert_eq!(
        persisted_build.build_spec.target.as_deref(),
        Some("release"),
        "target should persist from gRPC request"
    );
    assert_eq!(
        persisted_build.build_spec.image_tag.as_deref(),
        Some("example:latest"),
        "image tag should persist from gRPC request"
    );
    assert_eq!(
        persisted_build
            .build_spec
            .args
            .get("FOO")
            .map(String::as_str),
        Some("bar"),
        "build args should persist from gRPC request"
    );
    assert_eq!(
        persisted_build.build_spec.secrets,
        vec![secret_spec],
        "secrets should persist from gRPC request"
    );
    assert!(
        persisted_build.build_spec.no_cache,
        "no_cache should persist from gRPC request"
    );
    assert_eq!(
        persisted_build.build_spec.output_oci_tar_dest.as_deref(),
        Some("/tmp/build.oci.tar"),
        "output destination should persist from gRPC request"
    );
    assert!(
        !persisted_build.build_spec.push,
        "push should remain disabled when explicit OCI tar output is selected"
    );

    let canceled = build_client
        .cancel_build(Request::new(runtime_v2::CancelBuildRequest {
            build_id,
            metadata: None,
        }))
        .await
        .expect("cancel build");
    assert!(canceled.metadata().get("x-receipt-id").is_some());
    assert_eq!(
        canceled.into_inner().build.expect("build payload").state,
        "canceled"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn stream_build_events_emits_queued_and_canceled_events() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut build_client = connect_build_client(&config.socket_path).await;
    let mut cancel_client = connect_build_client(&config.socket_path).await;

    let started = build_client
        .start_build(Request::new(runtime_v2::StartBuildRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-build-stream-start".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            sandbox_id: "sbx-build-stream".to_string(),
            context: ".".to_string(),
            dockerfile: "Dockerfile".to_string(),
            args: std::collections::HashMap::new(),
            target: String::new(),
            image_tag: String::new(),
            secrets: Vec::new(),
            no_cache: false,
            push: false,
            output_oci_tar_dest: String::new(),
        }))
        .await
        .expect("start build")
        .into_inner();
    let build_id = started
        .build
        .as_ref()
        .expect("build payload")
        .build_id
        .clone();

    let mut stream = build_client
        .stream_build_events(Request::new(runtime_v2::StreamBuildEventsRequest {
            build_id: build_id.clone(),
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-build-stream".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
        }))
        .await
        .expect("stream build events")
        .into_inner();

    let first = tokio::time::timeout(Duration::from_secs(2), stream.message())
        .await
        .expect("queued stream timeout")
        .expect("queued stream read")
        .expect("queued event");
    assert!(
        first.event_type.contains("queued"),
        "expected queued event type, got {}",
        first.event_type
    );

    cancel_client
        .cancel_build(Request::new(runtime_v2::CancelBuildRequest {
            build_id,
            metadata: None,
        }))
        .await
        .expect("cancel build");

    let mut saw_canceled = false;
    for _ in 0..16 {
        let next = tokio::time::timeout(Duration::from_secs(2), stream.message())
            .await
            .expect("canceled stream timeout")
            .expect("canceled stream read");
        let Some(event) = next else {
            break;
        };
        if event.event_type.contains("canceled") {
            saw_canceled = true;
            break;
        }
    }
    assert!(saw_canceled, "expected canceled build event");

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn build_endpoints_recover_persisted_state_after_daemon_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    std::fs::create_dir_all(config.state_store_path.parent().expect("state parent"))
        .expect("create state directory");
    let store = vz_stack::StateStore::open(&config.state_store_path).expect("state store");
    let spec = vz_runtime_contract::BuildSpec {
        context: ".".to_string(),
        dockerfile: Some("Dockerfile".to_string()),
        target: None,
        args: BTreeMap::new(),
        cache_from: Vec::new(),
        image_tag: Some("example:restart".to_string()),
        secrets: Vec::new(),
        no_cache: false,
        push: false,
        output_oci_tar_dest: None,
    };
    store
        .save_build(&vz_runtime_contract::Build {
            build_id: "build-restart-running".to_string(),
            sandbox_id: "sbx-build-restart".to_string(),
            build_spec: spec.clone(),
            state: vz_runtime_contract::BuildState::Running,
            result_digest: None,
            started_at: 1,
            ended_at: None,
        })
        .expect("save running build");
    store
        .save_build(&vz_runtime_contract::Build {
            build_id: "build-restart-succeeded".to_string(),
            sandbox_id: "sbx-build-restart".to_string(),
            build_spec: spec,
            state: vz_runtime_contract::BuildState::Succeeded,
            result_digest: Some("sha256:feedface".to_string()),
            started_at: 2,
            ended_at: Some(3),
        })
        .expect("save succeeded build");
    drop(store);

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let reconciled = daemon
        .with_state_store(|store| store.load_build("build-restart-running"))
        .expect("load reconciled build")
        .expect("reconciled build should exist");
    assert_eq!(reconciled.state, vz_runtime_contract::BuildState::Failed);
    let persisted_succeeded = daemon
        .with_state_store(|store| store.load_build("build-restart-succeeded"))
        .expect("load succeeded build")
        .expect("succeeded build should exist");
    assert_eq!(
        persisted_succeeded.state,
        vz_runtime_contract::BuildState::Succeeded
    );

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut build_client = connect_build_client(&config.socket_path).await;

    let reconciled_build = build_client
        .get_build(Request::new(runtime_v2::GetBuildRequest {
            build_id: "build-restart-running".to_string(),
            metadata: None,
        }))
        .await
        .expect("get reconciled build")
        .into_inner()
        .build
        .expect("build payload");
    assert_eq!(reconciled_build.state, "failed");

    let canceled_terminal = build_client
        .cancel_build(Request::new(runtime_v2::CancelBuildRequest {
            build_id: "build-restart-succeeded".to_string(),
            metadata: None,
        }))
        .await
        .expect("cancel terminal build")
        .into_inner()
        .build
        .expect("build payload");
    assert_eq!(canceled_terminal.state, "succeeded");
    assert_eq!(canceled_terminal.result_digest, "sha256:feedface");

    let mut stream = build_client
        .stream_build_events(Request::new(runtime_v2::StreamBuildEventsRequest {
            build_id: "build-restart-running".to_string(),
            metadata: None,
        }))
        .await
        .expect("stream reconciled build events")
        .into_inner();
    let first = stream
        .message()
        .await
        .expect("stream read should succeed")
        .expect("stream should emit at least one event");
    assert!(
        first.event_type.contains("build_failed"),
        "expected build_failed event type, got {}",
        first.event_type
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn apply_stack_dry_run_multiservice_round_trip() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut stack_client = connect_stack_client(&config.socket_path).await;
    let compose_yaml = r#"
services:
  api:
    image: nginx:latest
  db:
    image: postgres:16
"#;

    let applied = read_apply_stack_completion_response(
        stack_client
            .apply_stack(Request::new(runtime_v2::ApplyStackRequest {
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: "req-stack-apply".to_string(),
                    idempotency_key: String::new(),
                    trace_id: String::new(),
                }),
                stack_name: "stack-multi".to_string(),
                compose_yaml: compose_yaml.to_string(),
                compose_dir: ".".to_string(),
                detach: false,
                dry_run: true,
            }))
            .await
            .expect("apply stack dry-run"),
    )
    .await;

    assert_eq!(applied.stack_name, "stack-multi");
    assert_eq!(applied.changed_actions, 2);
    assert_eq!(applied.services_failed, 0);
    assert_eq!(applied.services.len(), 2);
    let service_names: HashSet<&str> = applied
        .services
        .iter()
        .map(|service| service.service_name.as_str())
        .collect();
    assert!(service_names.contains("api"));
    assert!(service_names.contains("db"));

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn apply_and_teardown_stack_persist_receipts_with_metadata() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut stack_client = connect_stack_client(&config.socket_path).await;
    let stack_name = "stack-empty".to_string();
    let compose_yaml = "services: {}\n".to_string();

    let applied = stack_client
        .apply_stack(Request::new(runtime_v2::ApplyStackRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-stack-apply-live".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            stack_name: stack_name.clone(),
            compose_yaml,
            compose_dir: ".".to_string(),
            detach: false,
            dry_run: false,
        }))
        .await
        .expect("apply stack");
    assert!(applied.metadata().get("x-receipt-id").is_some());
    let applied = read_apply_stack_completion_response(applied).await;

    let torn_down = stack_client
        .teardown_stack(Request::new(runtime_v2::TeardownStackRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-stack-teardown-live".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            stack_name: stack_name.clone(),
            remove_volumes: false,
            dry_run: false,
        }))
        .await
        .expect("teardown stack");
    assert!(torn_down.metadata().get("x-receipt-id").is_some());
    let torn_down = read_teardown_stack_completion_response(torn_down).await;

    let stack_receipts = daemon
        .with_state_store(|store| store.list_receipts_for_entity("stack", &stack_name))
        .expect("list stack receipts");
    let apply_receipt = stack_receipts
        .iter()
        .find(|receipt| receipt.operation == "apply_stack")
        .expect("apply_stack receipt");
    assert_eq!(
        apply_receipt
            .metadata
            .get("event_type")
            .and_then(serde_json::Value::as_str),
        Some("stack_apply_completed")
    );
    assert_eq!(
        apply_receipt
            .metadata
            .get("changed_actions")
            .and_then(serde_json::Value::as_u64),
        Some(u64::from(applied.changed_actions))
    );
    assert_eq!(
        apply_receipt
            .metadata
            .get("converged")
            .and_then(serde_json::Value::as_bool),
        Some(applied.converged)
    );

    let teardown_receipt = stack_receipts
        .iter()
        .find(|receipt| receipt.operation == "teardown_stack")
        .expect("teardown_stack receipt");
    assert_eq!(
        teardown_receipt
            .metadata
            .get("event_type")
            .and_then(serde_json::Value::as_str),
        Some("stack_destroyed")
    );
    assert_eq!(
        teardown_receipt
            .metadata
            .get("changed_actions")
            .and_then(serde_json::Value::as_u64),
        Some(u64::from(torn_down.changed_actions))
    );
    assert_eq!(
        teardown_receipt
            .metadata
            .get("removed_volumes")
            .and_then(serde_json::Value::as_u64),
        Some(u64::from(torn_down.removed_volumes))
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_execution_then_get_and_cancel_round_trip() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut execution_client = connect_execution_client(&config.socket_path).await;

    let created = execution_client
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-exec-create".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            container_id: "ctr-exec-test".to_string(),
            cmd: vec!["echo".to_string()],
            args: vec!["hello".to_string()],
            env_override: std::collections::HashMap::new(),
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Inherit as i32,
        }))
        .await
        .expect("create execution");
    assert!(created.metadata().get("x-receipt-id").is_some());
    let created = created.into_inner();
    let execution_id = created
        .execution
        .as_ref()
        .expect("execution payload")
        .execution_id
        .clone();

    let fetched = execution_client
        .get_execution(Request::new(runtime_v2::GetExecutionRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        }))
        .await
        .expect("get execution")
        .into_inner();
    assert_eq!(
        fetched.execution.expect("execution payload").execution_id,
        execution_id,
        "execution id should round-trip"
    );

    let canceled = execution_client
        .cancel_execution(Request::new(runtime_v2::CancelExecutionRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        }))
        .await
        .expect("cancel execution");
    assert!(canceled.metadata().get("x-receipt-id").is_some());
    assert_eq!(
        canceled
            .into_inner()
            .execution
            .expect("execution payload")
            .state,
        "canceled"
    );

    let execution_receipts = daemon
        .with_state_store(|store| store.list_receipts_for_entity("execution", &execution_id))
        .expect("list execution receipts");
    let create_receipt = execution_receipts
        .iter()
        .find(|receipt| receipt.operation == "create_execution")
        .expect("create_execution receipt");
    assert_eq!(
        create_receipt
            .metadata
            .get("event_type")
            .and_then(serde_json::Value::as_str),
        Some("execution_queued")
    );
    assert!(
        create_receipt
            .metadata
            .get("request_hash")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|hash| !hash.is_empty())
    );
    assert!(
        create_receipt
            .metadata
            .get("idempotency_key")
            .is_some_and(serde_json::Value::is_null)
    );
    let cancel_receipt = execution_receipts
        .iter()
        .find(|receipt| receipt.operation == "cancel_execution")
        .expect("cancel_execution receipt");
    assert_eq!(
        cancel_receipt
            .metadata
            .get("event_type")
            .and_then(serde_json::Value::as_str),
        Some("execution_canceled")
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn stream_exec_output_returns_unimplemented_without_live_backend_session() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut execution_client_create = connect_execution_client(&config.socket_path).await;
    let mut execution_client_stream = connect_execution_client(&config.socket_path).await;

    let created = execution_client_create
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id: "ctr-stream".to_string(),
            cmd: vec!["echo".to_string()],
            args: vec!["hello".to_string()],
            env_override: std::collections::HashMap::new(),
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Inherit as i32,
        }))
        .await
        .expect("create execution")
        .into_inner();
    let execution_id = created
        .execution
        .expect("execution payload")
        .execution_id
        .clone();

    assert!(
        !daemon
            .execution_sessions()
            .contains(&execution_id)
            .expect("session lookup"),
        "non-started execution should not retain an active session"
    );

    let error = execution_client_stream
        .stream_exec_output(Request::new(runtime_v2::StreamExecOutputRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        }))
        .await
        .expect_err("stream should fail without backend session");
    assert_eq!(error.code(), tonic::Code::Unimplemented);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn stream_exec_output_repeat_attach_without_live_backend_session_returns_unimplemented() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut execution_client_create = connect_execution_client(&config.socket_path).await;
    let mut execution_client_stream_1 = connect_execution_client(&config.socket_path).await;
    let mut execution_client_stream_2 = connect_execution_client(&config.socket_path).await;

    let created = execution_client_create
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id: "ctr-reattach".to_string(),
            cmd: vec!["echo".to_string()],
            args: vec!["hello".to_string()],
            env_override: std::collections::HashMap::new(),
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Inherit as i32,
        }))
        .await
        .expect("create execution")
        .into_inner();
    let execution_id = created
        .execution
        .expect("execution payload")
        .execution_id
        .clone();

    assert!(
        !daemon
            .execution_sessions()
            .contains(&execution_id)
            .expect("session lookup"),
        "non-started execution should not retain an active session"
    );

    let error_1 = execution_client_stream_1
        .stream_exec_output(Request::new(runtime_v2::StreamExecOutputRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        }))
        .await
        .expect_err("first stream attach should fail without backend session");
    assert_eq!(error_1.code(), tonic::Code::Unimplemented);

    let error_2 = execution_client_stream_2
        .stream_exec_output(Request::new(runtime_v2::StreamExecOutputRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        }))
        .await
        .expect_err("reattach stream should fail without backend session");
    assert_eq!(error_2.code(), tonic::Code::Unimplemented);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn stream_exec_output_after_restart_reconcile_returns_terminal_failure_event() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };
    std::fs::create_dir_all(config.state_store_path.parent().expect("state parent"))
        .expect("create state directory");
    let store = vz_stack::StateStore::open(&config.state_store_path).expect("state store");
    store
        .save_execution(&Execution {
            execution_id: "exec-restart-reattach".to_string(),
            container_id: "ctr-restart".to_string(),
            exec_spec: ExecutionSpec {
                cmd: vec!["sleep".to_string()],
                args: vec!["10".to_string()],
                env_override: BTreeMap::new(),
                pty: false,
                timeout_secs: None,
            },
            state: ExecutionState::Running,
            exit_code: None,
            started_at: Some(1),
            ended_at: None,
        })
        .expect("save running execution");
    drop(store);

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let reconciled = daemon
        .with_state_store(|store| store.load_execution("exec-restart-reattach"))
        .expect("load reconciled execution")
        .expect("execution should exist");
    assert_eq!(reconciled.state, ExecutionState::Failed);
    assert!(
        !daemon
            .execution_sessions()
            .contains("exec-restart-reattach")
            .expect("session lookup"),
        "reconciled execution should not retain an active session"
    );

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut execution_client_stream = connect_execution_client(&config.socket_path).await;
    let mut stream = execution_client_stream
        .stream_exec_output(Request::new(runtime_v2::StreamExecOutputRequest {
            execution_id: "exec-restart-reattach".to_string(),
            metadata: None,
        }))
        .await
        .expect("stream execution output")
        .into_inner();
    let event = stream
        .message()
        .await
        .expect("stream read should succeed")
        .expect("stream should emit terminal failure event");
    match event.payload {
        Some(runtime_v2::exec_output_event::Payload::Error(message)) => {
            assert!(message.contains("failed state"));
        }
        other => panic!("expected error payload, got {other:?}"),
    }

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn write_exec_stdin_returns_unimplemented_for_execution_without_live_backend_session() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut execution_client = connect_execution_client(&config.socket_path).await;
    let created = execution_client
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id: "ctr-stdin".to_string(),
            cmd: vec!["echo".to_string()],
            args: vec!["hello".to_string()],
            env_override: std::collections::HashMap::new(),
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Inherit as i32,
        }))
        .await
        .expect("create execution")
        .into_inner();
    let execution_id = created
        .execution
        .expect("execution payload")
        .execution_id
        .clone();
    assert!(
        !daemon
            .execution_sessions()
            .contains(&execution_id)
            .expect("session lookup"),
        "non-started execution should not retain an active session"
    );

    let error = execution_client
        .write_exec_stdin(Request::new(runtime_v2::WriteExecStdinRequest {
            execution_id,
            data: b"hello\n".to_vec(),
            metadata: None,
        }))
        .await
        .expect_err("stdin write should fail without backend session");
    assert_eq!(error.code(), tonic::Code::Unimplemented);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn resize_exec_pty_returns_unimplemented_when_execution_pty_is_disabled() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    daemon
        .with_state_store(|store| {
            store.save_execution(&Execution {
                execution_id: "exec-no-pty".to_string(),
                container_id: "ctr-no-pty".to_string(),
                exec_spec: ExecutionSpec {
                    cmd: vec!["sleep".to_string()],
                    args: vec!["300".to_string()],
                    env_override: BTreeMap::new(),
                    pty: false,
                    timeout_secs: None,
                },
                state: ExecutionState::Running,
                exit_code: None,
                started_at: Some(1),
                ended_at: None,
            })
        })
        .expect("seed running execution");
    daemon
        .execution_sessions()
        .register("exec-no-pty")
        .expect("register test session");

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut execution_client = connect_execution_client(&config.socket_path).await;
    let error = execution_client
        .resize_exec_pty(Request::new(runtime_v2::ResizeExecPtyRequest {
            execution_id: "exec-no-pty".to_string(),
            cols: 120,
            rows: 40,
            metadata: None,
        }))
        .await
        .expect_err("resize should fail for non-pty execution");
    assert_eq!(error.code(), tonic::Code::Unimplemented);
    assert!(
        error.message().contains("PTY is disabled"),
        "expected PTY-disabled message, got: {}",
        error.message()
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_execution_with_known_container_records_backend_failure() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    daemon
        .with_state_store(|store| {
            store.save_container(&Container {
                container_id: "ctr-known".to_string(),
                sandbox_id: "sbx-known".to_string(),
                image_digest: "alpine:latest".to_string(),
                container_spec: ContainerSpec {
                    cmd: vec!["sleep".to_string(), "infinity".to_string()],
                    env: BTreeMap::new(),
                    cwd: None,
                    user: None,
                    mounts: Vec::new(),
                    resources: Default::default(),
                    network_attachments: Vec::new(),
                },
                state: ContainerState::Running,
                created_at: 1,
                started_at: Some(1),
                ended_at: None,
            })
        })
        .expect("seed running container");

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut execution_client_create = connect_execution_client(&config.socket_path).await;
    let mut execution_client_get = connect_execution_client(&config.socket_path).await;
    let mut execution_client_stream = connect_execution_client(&config.socket_path).await;

    let created = execution_client_create
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id: "ctr-known".to_string(),
            cmd: vec!["echo".to_string()],
            args: vec!["hello".to_string()],
            env_override: std::collections::HashMap::new(),
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Inherit as i32,
        }))
        .await
        .expect("create execution")
        .into_inner();
    let execution_id = created
        .execution
        .expect("execution payload")
        .execution_id
        .clone();

    let terminal_execution = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let fetched = execution_client_get
                .get_execution(Request::new(runtime_v2::GetExecutionRequest {
                    execution_id: execution_id.clone(),
                    metadata: None,
                }))
                .await
                .expect("get execution")
                .into_inner()
                .execution
                .expect("execution payload");
            if fetched.state == "failed" {
                return fetched;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("execution should reach terminal state");

    assert_eq!(terminal_execution.state, "failed");
    assert!(terminal_execution.ended_at > 0);

    let mut stream = execution_client_stream
        .stream_exec_output(Request::new(runtime_v2::StreamExecOutputRequest {
            execution_id,
            metadata: None,
        }))
        .await
        .expect("stream execution output for terminal replay")
        .into_inner();
    let event = stream
        .message()
        .await
        .expect("stream read should succeed")
        .expect("terminal replay should emit at least one event");
    match event.payload {
        Some(runtime_v2::exec_output_event::Payload::Error(message)) => {
            assert!(
                message.contains("failed state"),
                "expected failed-state replay error message, got {message}"
            );
        }
        other => panic!("expected error payload, got {other:?}"),
    }

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_execution_inherit_pty_uses_compose_tty_default() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let compose = r#"
services:
  web:
    image: nginx:latest
    tty: true
"#;
    let spec = vz_stack::parse_compose(compose, "tty-stack").expect("compose parse");
    daemon
        .with_state_store(|store| {
            store.save_desired_state("tty-stack", &spec)?;
            store.save_observed_state(
                "tty-stack",
                &vz_stack::ServiceObservedState {
                    service_name: "web".to_string(),
                    phase: vz_stack::ServicePhase::Running,
                    container_id: Some("ctr-tty-web".to_string()),
                    last_error: None,
                    ready: true,
                },
            )
        })
        .expect("seed stack state");

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;

    let mut execution_client = connect_execution_client(&config.socket_path).await;
    let created = execution_client
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id: "ctr-tty-web".to_string(),
            cmd: vec!["echo".to_string()],
            args: vec!["hello".to_string()],
            env_override: std::collections::HashMap::new(),
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Inherit as i32,
        }))
        .await
        .expect("create execution")
        .into_inner();

    let execution_id = created
        .execution
        .expect("execution payload")
        .execution_id
        .clone();
    let persisted = daemon
        .with_state_store(|store| store.load_execution(&execution_id))
        .expect("load execution")
        .expect("execution should persist");
    assert!(persisted.exec_spec.pty);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_execution_inherit_pty_uses_compose_tty_false_default() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let compose = r#"
services:
  web:
    image: nginx:latest
    tty: false
"#;
    let spec = vz_stack::parse_compose(compose, "tty-false-stack").expect("compose parse");
    daemon
        .with_state_store(|store| {
            store.save_desired_state("tty-false-stack", &spec)?;
            store.save_observed_state(
                "tty-false-stack",
                &vz_stack::ServiceObservedState {
                    service_name: "web".to_string(),
                    phase: vz_stack::ServicePhase::Running,
                    container_id: Some("ctr-tty-false-web".to_string()),
                    last_error: None,
                    ready: true,
                },
            )
        })
        .expect("seed stack state");

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;

    let mut execution_client = connect_execution_client(&config.socket_path).await;
    let created = execution_client
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id: "ctr-tty-false-web".to_string(),
            cmd: vec!["echo".to_string()],
            args: vec!["hello".to_string()],
            env_override: std::collections::HashMap::new(),
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Inherit as i32,
        }))
        .await
        .expect("create execution")
        .into_inner();

    let execution_id = created
        .execution
        .expect("execution payload")
        .execution_id
        .clone();
    let persisted = daemon
        .with_state_store(|store| store.load_execution(&execution_id))
        .expect("load execution")
        .expect("execution should persist");
    assert!(!persisted.exec_spec.pty);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_execution_inherit_pty_uses_compose_stdin_open_default() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let compose = r#"
services:
  web:
    image: nginx:latest
    tty: false
    stdin_open: true
"#;
    let spec = vz_stack::parse_compose(compose, "stdin-stack").expect("compose parse");
    daemon
        .with_state_store(|store| {
            store.save_desired_state("stdin-stack", &spec)?;
            store.save_observed_state(
                "stdin-stack",
                &vz_stack::ServiceObservedState {
                    service_name: "web".to_string(),
                    phase: vz_stack::ServicePhase::Running,
                    container_id: Some("ctr-stdin-web".to_string()),
                    last_error: None,
                    ready: true,
                },
            )
        })
        .expect("seed stack state");

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;

    let mut execution_client = connect_execution_client(&config.socket_path).await;
    let created = execution_client
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id: "ctr-stdin-web".to_string(),
            cmd: vec!["echo".to_string()],
            args: vec!["hello".to_string()],
            env_override: std::collections::HashMap::new(),
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Inherit as i32,
        }))
        .await
        .expect("create execution")
        .into_inner();

    let execution_id = created
        .execution
        .expect("execution payload")
        .execution_id
        .clone();
    let persisted = daemon
        .with_state_store(|store| store.load_execution(&execution_id))
        .expect("load execution")
        .expect("execution should persist");
    assert!(persisted.exec_spec.pty);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_execution_explicit_pty_override_beats_inherited_default() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let compose = r#"
services:
  web:
    image: nginx:latest
    tty: true
"#;
    let spec = vz_stack::parse_compose(compose, "tty-stack").expect("compose parse");
    daemon
        .with_state_store(|store| {
            store.save_desired_state("tty-stack", &spec)?;
            store.save_observed_state(
                "tty-stack",
                &vz_stack::ServiceObservedState {
                    service_name: "web".to_string(),
                    phase: vz_stack::ServicePhase::Running,
                    container_id: Some("ctr-tty-web".to_string()),
                    last_error: None,
                    ready: true,
                },
            )
        })
        .expect("seed stack state");

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;

    let mut execution_client = connect_execution_client(&config.socket_path).await;
    let created = execution_client
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id: "ctr-tty-web".to_string(),
            cmd: vec!["echo".to_string()],
            args: vec!["hello".to_string()],
            env_override: std::collections::HashMap::new(),
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Disabled as i32,
        }))
        .await
        .expect("create execution")
        .into_inner();

    let execution_id = created
        .execution
        .expect("execution payload")
        .execution_id
        .clone();
    let persisted = daemon
        .with_state_store(|store| store.load_execution(&execution_id))
        .expect("load execution")
        .expect("execution should persist");
    assert!(!persisted.exec_spec.pty);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
#[ignore = "requires backend environment with interactive exec + container runtime"]
async fn write_exec_stdin_round_trip_for_compose_stdin_open_service() {
    if !require_virtualization_entitlement() {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };
    let stack_name = "stdin-open-e2e-stack";

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;

    let mut stack_client = connect_stack_client(&config.socket_path).await;
    let mut execution_client = connect_execution_client(&config.socket_path).await;
    let mut execution_client_stream = connect_execution_client(&config.socket_path).await;

    let compose_yaml = r#"
services:
  app:
    image: alpine:latest
    command: ["sh", "-lc", "sleep 300"]
    tty: false
    stdin_open: true
"#;

    let applied = match stack_client
        .apply_stack(Request::new(runtime_v2::ApplyStackRequest {
            metadata: None,
            stack_name: stack_name.to_string(),
            compose_yaml: compose_yaml.to_string(),
            compose_dir: ".".to_string(),
            dry_run: false,
            detach: false,
        }))
        .await
    {
        Ok(response) => read_apply_stack_completion_response(response).await,
        Err(error) => {
            eprintln!(
                "skipping interactive stdin round trip: failed to apply stack in this environment ({error})"
            );
            shutdown.notify_waiters();
            let _ = tokio::time::timeout(Duration::from_secs(5), server).await;
            return;
        }
    };

    let maybe_container_id = applied
        .services
        .iter()
        .find(|service| service.service_name == "app")
        .map(|service| service.container_id.trim().to_string())
        .filter(|id| !id.is_empty());
    let Some(container_id) = maybe_container_id else {
        eprintln!("skipping interactive stdin round trip: compose service has no container_id");
        let _ = stack_client
            .teardown_stack(Request::new(runtime_v2::TeardownStackRequest {
                metadata: None,
                stack_name: stack_name.to_string(),
                dry_run: false,
                remove_volumes: false,
            }))
            .await;
        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(5), server).await;
        return;
    };

    let created = execution_client
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id: container_id.clone(),
            cmd: vec!["sh".to_string()],
            args: vec!["-lc".to_string(), "read line; echo got:$line".to_string()],
            env_override: std::collections::HashMap::new(),
            timeout_secs: 30,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Inherit as i32,
        }))
        .await
        .expect("create execution should succeed")
        .into_inner();
    let execution_id = created
        .execution
        .expect("execution payload")
        .execution_id
        .clone();

    let mut stream = execution_client_stream
        .stream_exec_output(Request::new(runtime_v2::StreamExecOutputRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        }))
        .await
        .expect("stream exec output")
        .into_inner();

    let mut wrote = false;
    for _ in 0..120 {
        match execution_client
            .write_exec_stdin(Request::new(runtime_v2::WriteExecStdinRequest {
                execution_id: execution_id.clone(),
                data: b"hello-daemon\n".to_vec(),
                metadata: None,
            }))
            .await
        {
            Ok(_) => {
                wrote = true;
                break;
            }
            Err(status) if status.code() == tonic::Code::Unimplemented => {
                eprintln!(
                    "skipping interactive stdin round trip: backend reports unsupported stdin write ({status})"
                );
                let _ = stack_client
                    .teardown_stack(Request::new(runtime_v2::TeardownStackRequest {
                        metadata: None,
                        stack_name: stack_name.to_string(),
                        dry_run: false,
                        remove_volumes: false,
                    }))
                    .await;
                shutdown.notify_waiters();
                let _ = tokio::time::timeout(Duration::from_secs(5), server).await;
                return;
            }
            Err(status) if status.code() == tonic::Code::NotFound => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(status) => panic!("unexpected stdin write failure: {status}"),
        }
    }
    assert!(wrote, "execution session should accept stdin writes");

    let mut stdout = Vec::new();
    let mut saw_exit = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let maybe_event = tokio::time::timeout(Duration::from_millis(500), stream.message())
            .await
            .expect("stream read timeout")
            .expect("stream read should succeed");
        let Some(event) = maybe_event else {
            break;
        };
        match event.payload {
            Some(runtime_v2::exec_output_event::Payload::Stdout(chunk)) => {
                stdout.extend(chunk);
            }
            Some(runtime_v2::exec_output_event::Payload::ExitCode(code)) => {
                saw_exit = Some(code);
                break;
            }
            Some(runtime_v2::exec_output_event::Payload::Error(error)) => {
                panic!("unexpected exec error event: {error}");
            }
            _ => {}
        }
    }

    let stdout_text = String::from_utf8_lossy(&stdout);
    assert_eq!(saw_exit, Some(0), "expected zero exit code");
    assert!(
        stdout_text.contains("got:hello-daemon"),
        "expected stdin-fed output, got: {stdout_text}"
    );

    let _ = stack_client
        .teardown_stack(Request::new(runtime_v2::TeardownStackRequest {
            metadata: None,
            stack_name: stack_name.to_string(),
            dry_run: false,
            remove_volumes: false,
        }))
        .await;
    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_execution_rejects_invalid_pty_mode() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;

    let mut execution_client = connect_execution_client(&config.socket_path).await;
    let error = execution_client
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id: "ctr-exec-test".to_string(),
            cmd: vec!["echo".to_string()],
            args: vec!["hello".to_string()],
            env_override: std::collections::HashMap::new(),
            timeout_secs: 0,
            pty_mode: 999,
        }))
        .await
        .expect_err("invalid pty_mode should fail");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_container_then_get_list_and_remove_round_trip() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-container-test".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels: std::collections::HashMap::new(),
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut container_client = connect_container_client(&config.socket_path).await;
    let created = container_client
        .create_container(Request::new(runtime_v2::CreateContainerRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-container-create".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            sandbox_id: sandbox.sandbox_id.clone(),
            image_digest: "sha256:test".to_string(),
            cmd: vec!["sleep".to_string(), "1".to_string()],
            env: std::collections::HashMap::new(),
            cwd: "/".to_string(),
            user: "root".to_string(),
        }))
        .await
        .expect("create container");
    assert!(created.metadata().get("x-receipt-id").is_some());
    let created = created.into_inner();
    let container_id = created
        .container
        .as_ref()
        .expect("container payload")
        .container_id
        .clone();
    assert_eq!(
        created.container.expect("container payload").state,
        "created",
        "new containers should start in created state"
    );

    let fetched = container_client
        .get_container(Request::new(runtime_v2::GetContainerRequest {
            container_id: container_id.clone(),
            metadata: None,
        }))
        .await
        .expect("get container")
        .into_inner();
    assert_eq!(
        fetched.container.expect("container payload").container_id,
        container_id
    );

    let listed = container_client
        .list_containers(Request::new(runtime_v2::ListContainersRequest {
            metadata: None,
        }))
        .await
        .expect("list containers")
        .into_inner();
    assert!(
        listed
            .containers
            .iter()
            .any(|container| container.container_id == container_id),
        "container list should include created container"
    );

    let removed = container_client
        .remove_container(Request::new(runtime_v2::RemoveContainerRequest {
            container_id: container_id.clone(),
            metadata: None,
        }))
        .await
        .expect("remove container");
    assert!(removed.metadata().get("x-receipt-id").is_some());
    assert_eq!(
        removed
            .into_inner()
            .container
            .expect("container payload")
            .state,
        "removed"
    );

    let get_after_remove = container_client
        .get_container(Request::new(runtime_v2::GetContainerRequest {
            container_id,
            metadata: None,
        }))
        .await
        .expect_err("removed container should not be returned by get");
    assert_eq!(get_after_remove.code(), tonic::Code::NotFound);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_container_denied_when_scheduler_capacity_is_exhausted() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    daemon.set_placement_limits_for_test(64, 0);
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-container-pressure".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels: std::collections::HashMap::new(),
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut container_client = connect_container_client(&config.socket_path).await;
    let denied = container_client
        .create_container(Request::new(runtime_v2::CreateContainerRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-placement-container-deny".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            sandbox_id: sandbox.sandbox_id,
            image_digest: "sha256:test".to_string(),
            cmd: vec!["sleep".to_string(), "1".to_string()],
            env: std::collections::HashMap::new(),
            cwd: "/".to_string(),
            user: "root".to_string(),
        }))
        .await
        .expect_err("placement preflight should deny create_container");
    assert_eq!(denied.code(), tonic::Code::Unavailable);

    let containers = daemon
        .with_state_store(|store| store.list_containers())
        .expect("list containers");
    assert!(
        containers.is_empty(),
        "denied create_container should not persist state"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_container_uses_sandbox_startup_defaults_when_request_omits_image_and_cmd() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut labels = std::collections::HashMap::new();
    labels.insert(
        SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
        "alpine:3.20".to_string(),
    );
    labels.insert(
        SANDBOX_LABEL_MAIN_CONTAINER.to_string(),
        "workspace-main".to_string(),
    );

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-container-defaults".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels,
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut container_client = connect_container_client(&config.socket_path).await;
    let created = container_client
        .create_container(Request::new(runtime_v2::CreateContainerRequest {
            metadata: None,
            sandbox_id: sandbox.sandbox_id.clone(),
            image_digest: String::new(),
            cmd: Vec::new(),
            env: std::collections::HashMap::new(),
            cwd: String::new(),
            user: String::new(),
        }))
        .await
        .expect("create container with sandbox defaults")
        .into_inner()
        .container
        .expect("container payload");

    assert_eq!(created.image_digest, "alpine:3.20");

    let persisted = daemon
        .with_state_store(|store| store.load_container(&created.container_id))
        .expect("load container from state store")
        .expect("persisted container should exist");
    assert_eq!(persisted.image_digest, "alpine:3.20");
    assert_eq!(
        persisted.container_spec.cmd,
        vec!["workspace-main".to_string()],
        "main_container default should be promoted to container cmd"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_container_preserves_explicit_image_and_cmd_over_sandbox_defaults() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut labels = std::collections::HashMap::new();
    labels.insert(
        SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
        "alpine:3.20".to_string(),
    );
    labels.insert(
        SANDBOX_LABEL_MAIN_CONTAINER.to_string(),
        "workspace-main".to_string(),
    );

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-container-overrides".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels,
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut container_client = connect_container_client(&config.socket_path).await;
    let created = container_client
        .create_container(Request::new(runtime_v2::CreateContainerRequest {
            metadata: None,
            sandbox_id: sandbox.sandbox_id.clone(),
            image_digest: "ubuntu:24.04".to_string(),
            cmd: vec![
                "bash".to_string(),
                "-lc".to_string(),
                "echo ready".to_string(),
            ],
            env: std::collections::HashMap::new(),
            cwd: String::new(),
            user: String::new(),
        }))
        .await
        .expect("create container with explicit overrides")
        .into_inner()
        .container
        .expect("container payload");

    assert_eq!(created.image_digest, "ubuntu:24.04");

    let persisted = daemon
        .with_state_store(|store| store.load_container(&created.container_id))
        .expect("load container from state store")
        .expect("persisted container should exist");
    assert_eq!(persisted.image_digest, "ubuntu:24.04");
    assert_eq!(
        persisted.container_spec.cmd,
        vec![
            "bash".to_string(),
            "-lc".to_string(),
            "echo ready".to_string()
        ],
        "explicit cmd should not be replaced by sandbox main_container default"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn list_events_returns_persisted_stack_events() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let _ = sandbox_client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-events-test".to_string(),
            cpus: 0,
            memory_mb: 0,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect("create sandbox to emit event");

    let mut event_client = connect_event_client(&config.socket_path).await;
    let response = event_client
        .list_events(Request::new(runtime_v2::ListEventsRequest {
            stack_name: "stack-events-test".to_string(),
            after: 0,
            limit: 20,
            scope: "sandbox_".to_string(),
            metadata: None,
        }))
        .await
        .expect("list events")
        .into_inner();

    assert!(
        !response.events.is_empty(),
        "event listing should include at least one sandbox event"
    );
    assert!(response.next_cursor >= response.events[0].id);
    let parsed_event: serde_json::Value =
        serde_json::from_str(&response.events[0].event_json).expect("event JSON must parse");
    assert!(parsed_event.is_object());

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_checkpoint_requires_existing_sandbox_workspace() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;

    let status = checkpoint_client
        .create_checkpoint(Request::new(runtime_v2::CreateCheckpointRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-ckpt-create".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            sandbox_id: "sbx-ckpt-test".to_string(),
            checkpoint_class: "fs_quick".to_string(),
            compatibility_fingerprint: "fp-test".to_string(),
            retention_tag: String::new(),
        }))
        .await
        .expect_err("missing sandbox should fail checkpoint creation");
    assert_eq!(status.code(), Code::NotFound);
    assert!(status.message().contains("sandbox not found"));

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn create_checkpoint_spaces_mode_fails_closed_without_linux_btrfs() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let workspace_root = tmp.path().join("workspace");
    std::fs::create_dir_all(&workspace_root).expect("create workspace root");

    let now = current_unix_secs();
    daemon
        .with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_sandbox(&Sandbox {
                    sandbox_id: "sbx-space-btrfs-checkpoint".to_string(),
                    backend: SandboxBackend::MacosVz,
                    spec: SandboxSpec::default(),
                    state: SandboxState::Ready,
                    created_at: now,
                    updated_at: now,
                    labels: BTreeMap::from([
                        (
                            SANDBOX_LABEL_SPACE_MODE.to_string(),
                            SANDBOX_SPACE_MODE_REQUIRED.to_string(),
                        ),
                        (
                            SANDBOX_LABEL_PROJECT_DIR.to_string(),
                            workspace_root.to_string_lossy().to_string(),
                        ),
                    ]),
                })?;
                Ok(())
            })
        })
        .expect("seed spaces-mode sandbox");

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;
    let status = checkpoint_client
        .create_checkpoint(Request::new(runtime_v2::CreateCheckpointRequest {
            metadata: None,
            sandbox_id: "sbx-space-btrfs-checkpoint".to_string(),
            checkpoint_class: "fs_quick".to_string(),
            compatibility_fingerprint: "fp-space".to_string(),
            retention_tag: String::new(),
        }))
        .await
        .expect_err("non-linux runtimed should fail-closed for spaces btrfs checkpointing");
    assert_eq!(status.code(), Code::Unimplemented);
    assert!(
        status.message().contains("Linux btrfs"),
        "error should explain linux btrfs requirement: {}",
        status.message()
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_checkpoint_with_retention_tag_is_protected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;
    let created = checkpoint_client
        .create_checkpoint(Request::new(runtime_v2::CreateCheckpointRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-ckpt-tagged".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            sandbox_id: "sbx-ckpt-tagged".to_string(),
            checkpoint_class: "fs_quick".to_string(),
            compatibility_fingerprint: "fp-tagged".to_string(),
            retention_tag: "pre-session".to_string(),
        }))
        .await
        .expect("create checkpoint")
        .into_inner();

    let checkpoint = created.checkpoint.expect("checkpoint payload");
    assert_eq!(checkpoint.retention_tag, "pre-session");
    assert!(checkpoint.retention_protected);
    assert_eq!(checkpoint.retention_expires_at, 0);
    assert!(checkpoint.retention_gc_reason.is_empty());

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_checkpoint_defaults_runtime_compatibility_fingerprint_when_unset() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;
    let created = checkpoint_client
        .create_checkpoint(Request::new(runtime_v2::CreateCheckpointRequest {
            metadata: None,
            sandbox_id: "sbx-ckpt-default-fp".to_string(),
            checkpoint_class: "fs_quick".to_string(),
            compatibility_fingerprint: String::new(),
            retention_tag: String::new(),
        }))
        .await
        .expect("create checkpoint")
        .into_inner();
    let checkpoint = created.checkpoint.expect("checkpoint payload");
    assert!(
        checkpoint
            .compatibility_fingerprint
            .starts_with("runtime:backend="),
        "default fingerprint should be daemon runtime scoped"
    );
    assert_ne!(checkpoint.compatibility_fingerprint, "unset");

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn restore_checkpoint_rejects_runtime_fingerprint_mismatch() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let now = current_unix_secs();
    daemon
        .with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                let checkpoint = Checkpoint {
                    checkpoint_id: "ckpt-runtime-mismatch".to_string(),
                    sandbox_id: "sbx-runtime-mismatch".to_string(),
                    parent_checkpoint_id: None,
                    class: CheckpointClass::FsQuick,
                    state: CheckpointState::Ready,
                    created_at: now,
                    compatibility_fingerprint: "runtime:backend=other;daemon=0.0.0".to_string(),
                };
                tx.save_checkpoint(&checkpoint)?;
                Ok(())
            })
        })
        .expect("seed mismatched checkpoint");

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;
    let status = checkpoint_client
        .restore_checkpoint(Request::new(runtime_v2::RestoreCheckpointRequest {
            checkpoint_id: "ckpt-runtime-mismatch".to_string(),
            metadata: None,
        }))
        .await
        .expect_err("restore should reject runtime fingerprint mismatch");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("compatibility fingerprint mismatch")
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn checkpoint_state_survives_daemon_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;
    let created = checkpoint_client
        .create_checkpoint(Request::new(runtime_v2::CreateCheckpointRequest {
            metadata: None,
            sandbox_id: "sbx-restart-checkpoint".to_string(),
            checkpoint_class: "fs_quick".to_string(),
            compatibility_fingerprint: String::new(),
            retention_tag: String::new(),
        }))
        .await
        .expect("create checkpoint before restart")
        .into_inner()
        .checkpoint
        .expect("checkpoint payload");
    let checkpoint_id = created.checkpoint_id.clone();
    let fingerprint = created.compatibility_fingerprint.clone();
    assert!(
        fingerprint.starts_with("runtime:backend="),
        "daemon should persist runtime-scoped compatibility fingerprint"
    );
    drop(checkpoint_client);

    shutdown.notify_waiters();
    let first_result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(first_result.is_ok());
    drop(daemon);

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon restart"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;
    let listed = checkpoint_client
        .list_checkpoints(Request::new(runtime_v2::ListCheckpointsRequest {
            metadata: None,
        }))
        .await
        .expect("list checkpoints after restart")
        .into_inner();
    let restored_checkpoint = listed
        .checkpoints
        .into_iter()
        .find(|checkpoint| checkpoint.checkpoint_id == checkpoint_id)
        .expect("checkpoint should persist across daemon restart");
    assert_eq!(restored_checkpoint.compatibility_fingerprint, fingerprint);

    let restored = checkpoint_client
        .restore_checkpoint(Request::new(runtime_v2::RestoreCheckpointRequest {
            checkpoint_id: checkpoint_id.clone(),
            metadata: None,
        }))
        .await
        .expect("restore checkpoint after restart")
        .into_inner();
    assert_eq!(
        restored
            .checkpoint
            .expect("restored checkpoint payload")
            .checkpoint_id,
        checkpoint_id
    );

    shutdown.notify_waiters();
    let second_result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(second_result.is_ok());
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires VZ_TEST_BTRFS_WORKSPACE on a host btrfs filesystem"]
async fn spaces_btrfs_checkpoint_restore_and_fork_use_real_subvolumes() {
    use std::process::Command;

    fn run_btrfs(args: &[&str]) {
        let output = Command::new("btrfs")
            .args(args)
            .output()
            .expect("run btrfs");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            panic!("btrfs command failed ({args:?}): {stderr}");
        }
    }

    let workspace_root_base = std::env::var("VZ_TEST_BTRFS_WORKSPACE")
        .expect("VZ_TEST_BTRFS_WORKSPACE must point at a btrfs path");
    let workspace_root = PathBuf::from(workspace_root_base)
        .join(format!("vz-space-{}", current_unix_secs()))
        .join("workspace");
    let parent = workspace_root
        .parent()
        .expect("workspace root parent should exist");
    std::fs::create_dir_all(parent).expect("create test workspace parent");
    run_btrfs(&[
        "subvolume",
        "create",
        workspace_root.to_string_lossy().as_ref(),
    ]);

    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;

    let now = current_unix_secs();
    daemon
        .with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_sandbox(&Sandbox {
                    sandbox_id: "sbx-space-btrfs-real".to_string(),
                    backend: SandboxBackend::LinuxFirecracker,
                    spec: SandboxSpec::default(),
                    state: SandboxState::Ready,
                    created_at: now,
                    updated_at: now,
                    labels: BTreeMap::from([
                        (
                            SANDBOX_LABEL_SPACE_MODE.to_string(),
                            SANDBOX_SPACE_MODE_REQUIRED.to_string(),
                        ),
                        (
                            SANDBOX_LABEL_PROJECT_DIR.to_string(),
                            workspace_root.to_string_lossy().to_string(),
                        ),
                    ]),
                })?;
                Ok(())
            })
        })
        .expect("seed spaces-mode sandbox");

    std::fs::write(workspace_root.join("marker.txt"), b"before").expect("seed workspace marker");

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;
    let parent_checkpoint_id = checkpoint_client
        .create_checkpoint(Request::new(runtime_v2::CreateCheckpointRequest {
            metadata: None,
            sandbox_id: "sbx-space-btrfs-real".to_string(),
            checkpoint_class: "fs_quick".to_string(),
            compatibility_fingerprint: String::new(),
            retention_tag: String::new(),
        }))
        .await
        .expect("create checkpoint")
        .into_inner()
        .checkpoint
        .expect("checkpoint payload")
        .checkpoint_id;

    std::fs::write(workspace_root.join("marker.txt"), b"after").expect("mutate workspace marker");

    checkpoint_client
        .restore_checkpoint(Request::new(runtime_v2::RestoreCheckpointRequest {
            checkpoint_id: parent_checkpoint_id.clone(),
            metadata: None,
        }))
        .await
        .expect("restore checkpoint");
    let restored_marker =
        std::fs::read_to_string(workspace_root.join("marker.txt")).expect("read restored marker");
    assert_eq!(restored_marker, "before");

    let fork_checkpoint_id = checkpoint_client
        .fork_checkpoint(Request::new(runtime_v2::ForkCheckpointRequest {
            checkpoint_id: parent_checkpoint_id,
            new_sandbox_id: "sbx-space-btrfs-fork".to_string(),
            metadata: None,
        }))
        .await
        .expect("fork checkpoint")
        .into_inner()
        .checkpoint
        .expect("fork payload")
        .checkpoint_id;

    let fork_snapshot_path = config
        .runtime_data_dir
        .join("checkpoints")
        .join("workspace-subvolumes")
        .join(&fork_checkpoint_id);
    assert!(
        fork_snapshot_path.exists(),
        "fork snapshot should exist at {}",
        fork_snapshot_path.display()
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());

    if workspace_root.exists() {
        let _ = Command::new("btrfs")
            .args([
                "subvolume",
                "delete",
                workspace_root.to_string_lossy().as_ref(),
            ])
            .output();
    }
}

#[tokio::test]
async fn diff_checkpoints_returns_real_file_level_deltas() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let workspace_root = tmp.path().join("workspace");
    std::fs::create_dir_all(&workspace_root).expect("create workspace root");
    std::fs::write(workspace_root.join("deleted.txt"), b"deleted").expect("seed deleted file");
    std::fs::write(workspace_root.join("modified.txt"), b"before").expect("seed modified file");
    std::fs::write(workspace_root.join("rename-old.txt"), b"rename").expect("seed rename file");

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-checkpoint-diff".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels: std::collections::HashMap::from([(
                    SANDBOX_LABEL_PROJECT_DIR.to_string(),
                    workspace_root.to_string_lossy().to_string(),
                )]),
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;
    let from_checkpoint_id = checkpoint_client
        .create_checkpoint(Request::new(runtime_v2::CreateCheckpointRequest {
            metadata: None,
            sandbox_id: sandbox.sandbox_id.clone(),
            checkpoint_class: "fs_quick".to_string(),
            compatibility_fingerprint: "fp-a".to_string(),
            retention_tag: String::new(),
        }))
        .await
        .expect("create from checkpoint")
        .into_inner()
        .checkpoint
        .expect("from checkpoint payload")
        .checkpoint_id;

    std::fs::remove_file(workspace_root.join("deleted.txt")).expect("remove deleted file");
    std::fs::write(workspace_root.join("modified.txt"), b"after").expect("rewrite modified file");
    std::fs::rename(
        workspace_root.join("rename-old.txt"),
        workspace_root.join("rename-new.txt"),
    )
    .expect("rename file");
    std::fs::write(workspace_root.join("added.txt"), b"added").expect("write added file");

    let to_checkpoint_id = checkpoint_client
        .create_checkpoint(Request::new(runtime_v2::CreateCheckpointRequest {
            metadata: None,
            sandbox_id: sandbox.sandbox_id.clone(),
            checkpoint_class: "fs_quick".to_string(),
            compatibility_fingerprint: "fp-b".to_string(),
            retention_tag: String::new(),
        }))
        .await
        .expect("create to checkpoint")
        .into_inner()
        .checkpoint
        .expect("to checkpoint payload")
        .checkpoint_id;

    let diff = checkpoint_client
        .diff_checkpoints(Request::new(runtime_v2::DiffCheckpointsRequest {
            from_checkpoint_id: from_checkpoint_id.clone(),
            to_checkpoint_id: to_checkpoint_id.clone(),
            metadata: None,
        }))
        .await
        .expect("diff checkpoints")
        .into_inner();
    let rendered: Vec<(String, String)> = diff
        .files
        .iter()
        .map(|item| (item.path.clone(), item.change.clone()))
        .collect();
    assert_eq!(
        rendered,
        vec![
            ("added.txt".to_string(), "A".to_string()),
            ("deleted.txt".to_string(), "D".to_string()),
            ("modified.txt".to_string(), "M".to_string()),
            ("rename-new.txt".to_string(), "A".to_string()),
            ("rename-old.txt".to_string(), "D".to_string()),
        ]
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn diff_checkpoints_fails_closed_when_workspace_snapshot_missing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };
    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;

    let now = current_unix_secs();
    daemon
        .with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_checkpoint(&Checkpoint {
                    checkpoint_id: "ckpt-diff-a".to_string(),
                    sandbox_id: "sbx-diff".to_string(),
                    parent_checkpoint_id: None,
                    class: CheckpointClass::FsQuick,
                    state: CheckpointState::Ready,
                    created_at: now,
                    compatibility_fingerprint: "fp-a".to_string(),
                })?;
                tx.save_checkpoint(&Checkpoint {
                    checkpoint_id: "ckpt-diff-b".to_string(),
                    sandbox_id: "sbx-diff".to_string(),
                    parent_checkpoint_id: None,
                    class: CheckpointClass::FsQuick,
                    state: CheckpointState::Ready,
                    created_at: now,
                    compatibility_fingerprint: "fp-b".to_string(),
                })?;
                Ok(())
            })
        })
        .expect("seed checkpoints");

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;
    let status = checkpoint_client
        .diff_checkpoints(Request::new(runtime_v2::DiffCheckpointsRequest {
            from_checkpoint_id: "ckpt-diff-a".to_string(),
            to_checkpoint_id: "ckpt-diff-b".to_string(),
            metadata: None,
        }))
        .await
        .expect_err("missing snapshot subvolumes should fail diff");
    assert_eq!(status.code(), Code::NotFound);
    assert!(
        status.message().contains("snapshot is missing"),
        "error should identify missing workspace snapshot: {}",
        status.message()
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn fork_checkpoint_fails_closed_when_parent_snapshot_missing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };
    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;

    let now = current_unix_secs();
    daemon
        .with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_checkpoint(&Checkpoint {
                    checkpoint_id: "ckpt-parent-missing-subvol".to_string(),
                    sandbox_id: "sbx-parent".to_string(),
                    parent_checkpoint_id: None,
                    class: CheckpointClass::FsQuick,
                    state: CheckpointState::Ready,
                    created_at: now,
                    compatibility_fingerprint: "fp-parent".to_string(),
                })?;
                Ok(())
            })
        })
        .expect("seed parent checkpoint");

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;
    let status = checkpoint_client
        .fork_checkpoint(Request::new(runtime_v2::ForkCheckpointRequest {
            checkpoint_id: "ckpt-parent-missing-subvol".to_string(),
            new_sandbox_id: "sbx-fork".to_string(),
            metadata: None,
        }))
        .await
        .expect_err("missing parent snapshot subvolume should fail fork");
    assert!(
        status.code() == Code::NotFound || status.code() == Code::Unimplemented,
        "unexpected status code for missing parent snapshot: {} ({})",
        status.code(),
        status.message()
    );
    if status.code() == Code::NotFound {
        assert!(
            status.message().contains("snapshot is missing"),
            "error should identify missing workspace parent snapshot: {}",
            status.message()
        );
    }

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn checkpoint_export_import_missing_entities_return_not_found() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };
    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;
    let export_error = checkpoint_client
        .export_checkpoint(Request::new(runtime_v2::ExportCheckpointRequest {
            checkpoint_id: "ckpt-missing-export".to_string(),
            stream_path: "/tmp/vz-missing-export.stream".to_string(),
            metadata: None,
        }))
        .await
        .expect_err("missing checkpoint export should fail");
    assert_eq!(export_error.code(), tonic::Code::NotFound);

    let import_error = checkpoint_client
        .import_checkpoint(Request::new(runtime_v2::ImportCheckpointRequest {
            sandbox_id: "sbx-missing-import".to_string(),
            stream_path: "/tmp/vz-missing-import.stream".to_string(),
            checkpoint_class: "fs_quick".to_string(),
            compatibility_fingerprint: String::new(),
            retention_tag: String::new(),
            metadata: None,
        }))
        .await
        .expect_err("missing sandbox import should fail");
    assert_eq!(import_error.code(), tonic::Code::NotFound);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires VZ_TEST_BTRFS_WORKSPACE on a host btrfs filesystem"]
async fn checkpoint_export_import_round_trip_preserves_workspace_snapshot() {
    fn run_btrfs(args: &[&str]) {
        let output = Command::new("btrfs")
            .args(args)
            .output()
            .expect("run btrfs");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            panic!("btrfs command failed ({args:?}): {stderr}");
        }
    }

    let workspace_root_base = std::env::var("VZ_TEST_BTRFS_WORKSPACE")
        .expect("VZ_TEST_BTRFS_WORKSPACE must point at a btrfs path");
    let workspace_root = PathBuf::from(workspace_root_base)
        .join(format!("vz-portability-{}", current_unix_secs()))
        .join("workspace");
    let parent = workspace_root
        .parent()
        .expect("workspace root parent should exist");
    std::fs::create_dir_all(parent).expect("create test workspace parent");
    run_btrfs(&[
        "subvolume",
        "create",
        workspace_root.to_string_lossy().as_ref(),
    ]);

    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };
    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&config.socket_path).await;

    let now = current_unix_secs();
    daemon
        .with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_sandbox(&Sandbox {
                    sandbox_id: "sbx-portability".to_string(),
                    backend: SandboxBackend::LinuxFirecracker,
                    spec: SandboxSpec::default(),
                    state: SandboxState::Ready,
                    created_at: now,
                    updated_at: now,
                    labels: BTreeMap::from([
                        (
                            SANDBOX_LABEL_SPACE_MODE.to_string(),
                            SANDBOX_SPACE_MODE_REQUIRED.to_string(),
                        ),
                        (
                            SANDBOX_LABEL_PROJECT_DIR.to_string(),
                            workspace_root.to_string_lossy().to_string(),
                        ),
                    ]),
                })?;
                Ok(())
            })
        })
        .expect("seed spaces-mode sandbox");

    std::fs::write(workspace_root.join("marker.txt"), b"portable").expect("seed workspace marker");

    let mut checkpoint_client = connect_checkpoint_client(&config.socket_path).await;
    let source_checkpoint = checkpoint_client
        .create_checkpoint(Request::new(runtime_v2::CreateCheckpointRequest {
            metadata: None,
            sandbox_id: "sbx-portability".to_string(),
            checkpoint_class: "fs_quick".to_string(),
            compatibility_fingerprint: String::new(),
            retention_tag: String::new(),
        }))
        .await
        .expect("create checkpoint")
        .into_inner()
        .checkpoint
        .expect("checkpoint payload");

    let stream_path = tmp.path().join("portable").join("checkpoint.send");
    let export_completion = read_export_checkpoint_completion_response(
        checkpoint_client
            .export_checkpoint(Request::new(runtime_v2::ExportCheckpointRequest {
                checkpoint_id: source_checkpoint.checkpoint_id.clone(),
                stream_path: stream_path.to_string_lossy().to_string(),
                metadata: None,
            }))
            .await
            .expect("export checkpoint"),
    )
    .await;
    assert_eq!(
        export_completion.checkpoint_id,
        source_checkpoint.checkpoint_id
    );
    assert_eq!(
        export_completion.stream_path,
        stream_path.to_string_lossy().to_string()
    );
    assert!(stream_path.exists(), "exported send stream should exist");

    let import_completion = read_import_checkpoint_completion_response(
        checkpoint_client
            .import_checkpoint(Request::new(runtime_v2::ImportCheckpointRequest {
                sandbox_id: "sbx-portability".to_string(),
                stream_path: stream_path.to_string_lossy().to_string(),
                checkpoint_class: "fs_quick".to_string(),
                compatibility_fingerprint: "fp-portability".to_string(),
                retention_tag: String::new(),
                metadata: None,
            }))
            .await
            .expect("import checkpoint"),
    )
    .await;
    let imported = import_completion
        .response
        .expect("import completion response")
        .checkpoint
        .expect("imported checkpoint payload");
    assert_ne!(
        imported.checkpoint_id, source_checkpoint.checkpoint_id,
        "import should create a distinct checkpoint id"
    );
    let imported_snapshot = config
        .runtime_data_dir
        .join("checkpoints")
        .join("workspace-subvolumes")
        .join(imported.checkpoint_id.as_str());
    assert!(
        imported_snapshot.exists(),
        "imported workspace snapshot should exist at {}",
        imported_snapshot.display()
    );

    let diff = checkpoint_client
        .diff_checkpoints(Request::new(runtime_v2::DiffCheckpointsRequest {
            from_checkpoint_id: source_checkpoint.checkpoint_id.clone(),
            to_checkpoint_id: imported.checkpoint_id.clone(),
            metadata: None,
        }))
        .await
        .expect("diff checkpoints")
        .into_inner();
    assert!(
        diff.files.is_empty(),
        "portable round-trip should preserve checkpoint file entries"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());

    if workspace_root.exists() {
        let _ = Command::new("btrfs")
            .args([
                "subvolume",
                "delete",
                workspace_root.to_string_lossy().as_ref(),
            ])
            .output();
    }
}

#[tokio::test]
async fn file_service_write_read_list_round_trip_with_receipts() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-file-ops".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels: std::collections::HashMap::new(),
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut file_client = connect_file_client(&config.socket_path).await;
    let mkdir = file_client
        .make_dir(Request::new(runtime_v2::MakeDirRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-file-mkdir".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            sandbox_id: sandbox.sandbox_id.clone(),
            path: "workspace".to_string(),
            parents: true,
        }))
        .await
        .expect("make dir");
    assert!(mkdir.metadata().get("x-receipt-id").is_some());

    let write = file_client
        .write_file(Request::new(runtime_v2::WriteFileRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-file-write".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            sandbox_id: sandbox.sandbox_id.clone(),
            path: "workspace/hello.txt".to_string(),
            data: b"hello daemon file api".to_vec(),
            append: false,
            create_parents: false,
        }))
        .await
        .expect("write file");
    assert!(write.metadata().get("x-receipt-id").is_some());
    assert_eq!(write.into_inner().bytes_written, 21);

    let read = file_client
        .read_file(Request::new(runtime_v2::ReadFileRequest {
            metadata: None,
            sandbox_id: sandbox.sandbox_id.clone(),
            path: "workspace/hello.txt".to_string(),
            offset: 0,
            limit: 0,
        }))
        .await
        .expect("read file")
        .into_inner();
    assert_eq!(read.data, b"hello daemon file api".to_vec());
    assert!(!read.truncated);

    let listed = file_client
        .list_files(Request::new(runtime_v2::ListFilesRequest {
            metadata: None,
            sandbox_id: sandbox.sandbox_id.clone(),
            path: "workspace".to_string(),
            recursive: true,
            limit: 100,
        }))
        .await
        .expect("list files")
        .into_inner();
    assert!(
        listed
            .entries
            .iter()
            .any(|entry| entry.path == "workspace/hello.txt"),
        "listed entries should include written file"
    );

    let file_entity_id = format!("{}:workspace/hello.txt", sandbox.sandbox_id);
    let file_receipts = daemon
        .with_state_store(|store| store.list_receipts_for_entity("file", &file_entity_id))
        .expect("list file receipts");
    assert!(
        file_receipts
            .iter()
            .any(|receipt| receipt.operation == "write_file"),
        "write_file receipt should be persisted"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn file_service_rejects_path_traversal() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut sandbox_client = connect_sandbox_client(&config.socket_path).await;
    let sandbox = read_create_sandbox_completion_response(
        sandbox_client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-file-validate".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels: std::collections::HashMap::new(),
            }))
            .await
            .expect("create sandbox"),
    )
    .await
    .sandbox
    .expect("sandbox payload");

    let mut file_client = connect_file_client(&config.socket_path).await;
    let error = file_client
        .read_file(Request::new(runtime_v2::ReadFileRequest {
            metadata: None,
            sandbox_id: sandbox.sandbox_id,
            path: "../escape.txt".to_string(),
            offset: 0,
            limit: 0,
        }))
        .await
        .expect_err("path traversal should fail validation");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn terminate_sandbox_honors_idempotency_and_emits_receipt_header() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut client = connect_sandbox_client(&config.socket_path).await;
    let first = read_create_sandbox_completion_response(
        client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-term-a".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels: std::collections::HashMap::new(),
            }))
            .await
            .expect("create first sandbox"),
    )
    .await;
    let first_id = first.sandbox.expect("payload").sandbox_id;

    let second = read_create_sandbox_completion_response(
        client
            .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: "stack-term-b".to_string(),
                cpus: 0,
                memory_mb: 0,
                labels: std::collections::HashMap::new(),
            }))
            .await
            .expect("create second sandbox"),
    )
    .await;
    let second_id = second.sandbox.expect("payload").sandbox_id;

    let terminated = client
        .terminate_sandbox(Request::new(runtime_v2::TerminateSandboxRequest {
            sandbox_id: first_id.clone(),
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-term-1".to_string(),
                idempotency_key: "term-key-a".to_string(),
                trace_id: "".to_string(),
            }),
        }))
        .await
        .expect("terminate sandbox");
    assert!(terminated.metadata().get("x-receipt-id").is_some());
    let terminated = read_terminate_sandbox_completion_response(terminated).await;
    assert_eq!(terminated.sandbox.expect("payload").state, "terminated");

    let replay = read_terminate_sandbox_completion_response(
        client
            .terminate_sandbox(Request::new(runtime_v2::TerminateSandboxRequest {
                sandbox_id: first_id,
                metadata: Some(runtime_v2::RequestMetadata {
                    request_id: "req-term-2".to_string(),
                    idempotency_key: "term-key-a".to_string(),
                    trace_id: "".to_string(),
                }),
            }))
            .await
            .expect("idempotent replay should succeed"),
    )
    .await;
    assert_eq!(replay.sandbox.expect("payload").state, "terminated");

    let conflict = client
        .terminate_sandbox(Request::new(runtime_v2::TerminateSandboxRequest {
            sandbox_id: second_id,
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-term-3".to_string(),
                idempotency_key: "term-key-a".to_string(),
                trace_id: "".to_string(),
            }),
        }))
        .await
        .expect_err("idempotency conflict expected");
    assert_eq!(conflict.code(), tonic::Code::FailedPrecondition);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_create_sandbox_replays_idempotent_result_with_single_mutation() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    const CONCURRENCY: usize = 8;
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for index in 0..CONCURRENCY {
        let socket = config.socket_path.clone();
        handles.push(tokio::spawn(async move {
            let mut client = connect_sandbox_client(&socket).await;
            let response = client
                .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                    metadata: Some(runtime_v2::RequestMetadata {
                        request_id: format!("req-concurrent-{index}"),
                        idempotency_key: "idem-key-concurrent".to_string(),
                        trace_id: "".to_string(),
                    }),
                    stack_name: "stack-concurrent-idem".to_string(),
                    cpus: 2,
                    memory_mb: 512,
                    labels: std::collections::HashMap::new(),
                }))
                .await?;

            let receipt_id = response
                .metadata()
                .get("x-receipt-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let sandbox_response = read_create_sandbox_completion_response(response).await;
            let sandbox_id = sandbox_response
                .sandbox
                .expect("sandbox payload")
                .sandbox_id;
            Ok::<(String, Option<String>), tonic::Status>((sandbox_id, receipt_id))
        }));
    }

    let mut sandbox_ids = Vec::with_capacity(CONCURRENCY);
    let mut receipt_headers = Vec::with_capacity(CONCURRENCY);
    for handle in handles {
        let result = handle.await.expect("create task should join");
        let (sandbox_id, receipt_id) = result.expect("concurrent idempotent create should succeed");
        sandbox_ids.push(sandbox_id);
        receipt_headers.push(receipt_id);
    }

    assert!(
        sandbox_ids
            .iter()
            .all(|sandbox_id| sandbox_id == "stack-concurrent-idem")
    );
    assert!(
        receipt_headers.iter().any(Option::is_some),
        "at least one call should include a receipt header"
    );

    let (ready_event_count, create_receipt_count, has_idempotency_record) = daemon
        .with_state_store(|store| {
            let ready_event_count = store
                .load_events_since("stack-concurrent-idem", 0)?
                .into_iter()
                .filter(|record| matches!(record.event, StackEvent::SandboxReady { .. }))
                .count();
            let create_receipt_count = store
                .list_receipts_for_entity("sandbox", "stack-concurrent-idem")?
                .into_iter()
                .filter(|receipt| receipt.operation == "create_sandbox")
                .count();
            let has_idempotency_record = store
                .find_idempotency_result("idem-key-concurrent")?
                .is_some();
            Ok((
                ready_event_count,
                create_receipt_count,
                has_idempotency_record,
            ))
        })
        .expect("state store query");

    assert_eq!(
        ready_event_count, 1,
        "only one ready event should persist for idempotent concurrent creates"
    );
    assert_eq!(
        create_receipt_count, 1,
        "only one receipt should persist for idempotent concurrent creates"
    );
    assert!(has_idempotency_record);

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn maintenance_loop_cleans_expired_idempotency_keys() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    daemon
        .with_state_store(|store| {
            store.save_idempotency_result(&IdempotencyRecord {
                key: "expired-key".to_string(),
                operation: "create_sandbox".to_string(),
                request_hash: "hash".to_string(),
                response_json: "sandbox-id".to_string(),
                status_code: 201,
                created_at: 0,
                expires_at: 0,
            })
        })
        .expect("seed idempotency record");

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let cleanup_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let exists = daemon
            .with_state_store(|store| Ok(store.find_idempotency_result("expired-key")?.is_some()))
            .expect("query idempotency record");
        if !exists {
            break;
        }
        if tokio::time::Instant::now() >= cleanup_deadline {
            panic!("expired idempotency key was not cleaned by maintenance loop");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn maintenance_loop_compacts_checkpoints_and_receipts() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    daemon
        .with_state_store(|store| {
            store.save_checkpoint(&vz_runtime_contract::Checkpoint {
                checkpoint_id: "ckpt-old".to_string(),
                sandbox_id: "sbx-retention".to_string(),
                parent_checkpoint_id: None,
                class: vz_runtime_contract::CheckpointClass::FsQuick,
                state: vz_runtime_contract::CheckpointState::Ready,
                created_at: 0,
                compatibility_fingerprint: "fp-old".to_string(),
            })?;
            store.save_checkpoint(&vz_runtime_contract::Checkpoint {
                checkpoint_id: "ckpt-tagged".to_string(),
                sandbox_id: "sbx-retention".to_string(),
                parent_checkpoint_id: None,
                class: vz_runtime_contract::CheckpointClass::FsQuick,
                state: vz_runtime_contract::CheckpointState::Ready,
                created_at: 0,
                compatibility_fingerprint: "fp-tagged".to_string(),
            })?;
            store.save_checkpoint_retention_tag("ckpt-tagged", "golden")?;
            store.save_receipt(&Receipt {
                receipt_id: "rcp-old".to_string(),
                operation: "create_sandbox".to_string(),
                entity_id: "sbx-retention".to_string(),
                entity_type: "sandbox".to_string(),
                request_id: "req-old".to_string(),
                status: "success".to_string(),
                created_at: 0,
                metadata: serde_json::json!({}),
            })?;
            store.save_receipt(&Receipt {
                receipt_id: "rcp-fresh".to_string(),
                operation: "create_sandbox".to_string(),
                entity_id: "sbx-retention".to_string(),
                entity_type: "sandbox".to_string(),
                request_id: "req-fresh".to_string(),
                status: "success".to_string(),
                created_at: current_unix_secs(),
                metadata: serde_json::json!({}),
            })?;
            Ok(())
        })
        .expect("seed retention records");

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let cleanup_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let (old_checkpoint_exists, tagged_exists, old_receipt_exists, fresh_receipt_exists) =
            daemon
                .with_state_store(|store| {
                    Ok((
                        store.load_checkpoint("ckpt-old")?.is_some(),
                        store.load_checkpoint("ckpt-tagged")?.is_some(),
                        store.load_receipt("rcp-old")?.is_some(),
                        store.load_receipt("rcp-fresh")?.is_some(),
                    ))
                })
                .expect("query retention cleanup state");
        if !old_checkpoint_exists && !old_receipt_exists {
            assert!(tagged_exists, "tagged checkpoint must be preserved");
            assert!(fresh_receipt_exists, "fresh receipt must be preserved");
            break;
        }
        if tokio::time::Instant::now() >= cleanup_deadline {
            panic!("checkpoint/receipt retention cleanup did not complete in maintenance loop");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_create_without_idempotency_returns_conflict_not_internal() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    const CONCURRENCY: usize = 8;
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for index in 0..CONCURRENCY {
        let socket = config.socket_path.clone();
        handles.push(tokio::spawn(async move {
            let mut client = connect_sandbox_client(&socket).await;
            let response = client
                .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
                    metadata: Some(runtime_v2::RequestMetadata {
                        request_id: format!("req-race-{index}"),
                        idempotency_key: String::new(),
                        trace_id: String::new(),
                    }),
                    stack_name: "stack-race-no-idem".to_string(),
                    cpus: 1,
                    memory_mb: 256,
                    labels: std::collections::HashMap::new(),
                }))
                .await?;
            let mut stream = response.into_inner();
            try_read_create_sandbox_completion(&mut stream)
                .await
                .map(|_| ())
        }));
    }

    let mut successes = 0usize;
    let mut conflicts = 0usize;
    for handle in handles {
        match handle.await.expect("create task should join") {
            Ok(_) => successes += 1,
            Err(status) => match status.code() {
                tonic::Code::FailedPrecondition => conflicts += 1,
                other => panic!("unexpected grpc status: {other} ({status})"),
            },
        }
    }

    assert_eq!(successes, 1, "exactly one create should succeed");
    assert_eq!(
        conflicts,
        CONCURRENCY - 1,
        "all other creates should fail with conflict"
    );

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn pull_image_stream_emits_progress_and_terminal_completion() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut image_client = connect_image_client(&config.socket_path).await;
    let mut stream = image_client
        .pull_image(Request::new(runtime_v2::PullImageRequest {
            image_ref: "alpine:3.20".to_string(),
            metadata: None,
        }))
        .await
        .expect("pull image stream")
        .into_inner();

    let mut saw_progress = false;
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .expect("read pull image stream event")
    {
        match event.payload {
            Some(runtime_v2::pull_image_event::Payload::Progress(progress)) => {
                saw_progress = true;
                assert!(!progress.phase.is_empty());
            }
            Some(runtime_v2::pull_image_event::Payload::Completion(done)) => {
                completion = Some(done)
            }
            None => {}
        }
    }
    assert!(saw_progress, "expected at least one progress event");
    let completion = completion.expect("expected terminal completion event");
    let image = completion.image.expect("completion image payload");
    assert_eq!(image.image_ref, "alpine:3.20");
    assert_eq!(image.resolved_digest, "sha256:test-mock");
    assert!(!completion.receipt_id.is_empty());

    let list_response = image_client
        .list_images(Request::new(runtime_v2::ListImagesRequest {
            metadata: None,
        }))
        .await
        .expect("list images")
        .into_inner();
    assert_eq!(list_response.images.len(), 1);
    assert_eq!(list_response.images[0].image_ref, "alpine:3.20");

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}

#[tokio::test]
async fn prune_images_stream_emits_terminal_completion_and_clears_image_index() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = RuntimedConfig {
        state_store_path: tmp.path().join("state").join("stack-state.db"),
        runtime_data_dir: tmp.path().join("runtime"),
        socket_path: tmp.path().join("runtime").join("runtimed.sock"),
    };

    let daemon = Arc::new(RuntimeDaemon::start(config.clone()).expect("daemon start"));
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = config.socket_path.clone();

    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(&config.socket_path).await;

    let mut image_client = connect_image_client(&config.socket_path).await;
    let mut pull_stream = image_client
        .pull_image(Request::new(runtime_v2::PullImageRequest {
            image_ref: "nginx:latest".to_string(),
            metadata: None,
        }))
        .await
        .expect("pull image stream")
        .into_inner();
    while pull_stream
        .message()
        .await
        .expect("read pull event before prune")
        .is_some()
    {}

    let mut prune_stream = image_client
        .prune_images(Request::new(runtime_v2::PruneImagesRequest {
            metadata: None,
        }))
        .await
        .expect("prune images stream")
        .into_inner();
    let mut completion = None;
    while let Some(event) = prune_stream
        .message()
        .await
        .expect("read prune images stream event")
    {
        if let Some(runtime_v2::prune_images_event::Payload::Completion(done)) = event.payload {
            completion = Some(done);
        }
    }
    let completion = completion.expect("expected terminal prune completion event");
    assert_eq!(completion.remaining_images, 0);
    assert!(!completion.receipt_id.is_empty());

    let list_response = image_client
        .list_images(Request::new(runtime_v2::ListImagesRequest {
            metadata: None,
        }))
        .await
        .expect("list images")
        .into_inner();
    assert!(list_response.images.is_empty());

    shutdown.notify_waiters();
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server join timeout")
        .expect("server join should succeed");
    assert!(result.is_ok());
}
