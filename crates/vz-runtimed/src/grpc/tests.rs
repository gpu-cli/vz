use std::collections::{BTreeMap, HashSet};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use vz_runtime_contract::{PolicyDecision, RequestMetadata, RuntimeOperation, RuntimePolicyHook};

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
    let created = client
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
        .expect("create sandbox")
        .into_inner();
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
    let created = client
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
        .expect("create sandbox")
        .into_inner();
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
    let first = client
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
        .expect("first create")
        .into_inner();
    let first_id = first
        .sandbox
        .as_ref()
        .expect("sandbox payload")
        .sandbox_id
        .clone();

    let replay = client
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
        .expect("idempotent replay")
        .into_inner();
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
            args: std::collections::HashMap::new(),
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

    let applied = stack_client
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
        .expect("apply stack dry-run")
        .into_inner();

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
    let applied = applied.into_inner();

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
    let torn_down = torn_down.into_inner();

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
        Ok(response) => response.into_inner(),
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
    let sandbox = sandbox_client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-container-test".to_string(),
            cpus: 0,
            memory_mb: 0,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect("create sandbox")
        .into_inner()
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
    let sandbox = sandbox_client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-container-pressure".to_string(),
            cpus: 0,
            memory_mb: 0,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect("create sandbox")
        .into_inner()
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
    let sandbox = sandbox_client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-container-defaults".to_string(),
            cpus: 0,
            memory_mb: 0,
            labels,
        }))
        .await
        .expect("create sandbox")
        .into_inner()
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
    let sandbox = sandbox_client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-container-overrides".to_string(),
            cpus: 0,
            memory_mb: 0,
            labels,
        }))
        .await
        .expect("create sandbox")
        .into_inner()
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
async fn create_checkpoint_then_get_and_list_round_trip() {
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
                request_id: "req-ckpt-create".to_string(),
                idempotency_key: String::new(),
                trace_id: String::new(),
            }),
            sandbox_id: "sbx-ckpt-test".to_string(),
            checkpoint_class: "fs_quick".to_string(),
            compatibility_fingerprint: "fp-test".to_string(),
        }))
        .await
        .expect("create checkpoint");
    assert!(created.metadata().get("x-receipt-id").is_some());
    let created = created.into_inner();
    let checkpoint_id = created
        .checkpoint
        .as_ref()
        .expect("checkpoint payload")
        .checkpoint_id
        .clone();
    assert_eq!(
        created.checkpoint.expect("checkpoint payload").state,
        "ready",
        "checkpoint should advance to ready"
    );

    let fetched = checkpoint_client
        .get_checkpoint(Request::new(runtime_v2::GetCheckpointRequest {
            checkpoint_id: checkpoint_id.clone(),
            metadata: None,
        }))
        .await
        .expect("get checkpoint")
        .into_inner();
    assert_eq!(
        fetched
            .checkpoint
            .expect("checkpoint payload")
            .checkpoint_id,
        checkpoint_id,
        "checkpoint id should round-trip"
    );

    let listed = checkpoint_client
        .list_checkpoints(Request::new(runtime_v2::ListCheckpointsRequest {
            metadata: None,
        }))
        .await
        .expect("list checkpoints")
        .into_inner();
    assert_eq!(listed.checkpoints.len(), 1);

    let checkpoint_receipts = daemon
        .with_state_store(|store| store.list_receipts_for_entity("checkpoint", &checkpoint_id))
        .expect("list checkpoint receipts");
    let create_receipt = checkpoint_receipts
        .iter()
        .find(|receipt| receipt.operation == "create_checkpoint")
        .expect("create_checkpoint receipt");
    assert_eq!(
        create_receipt
            .metadata
            .get("event_type")
            .and_then(serde_json::Value::as_str),
        Some("checkpoint_ready")
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
    let sandbox = sandbox_client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-file-ops".to_string(),
            cpus: 0,
            memory_mb: 0,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect("create sandbox")
        .into_inner()
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
    let sandbox = sandbox_client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-file-validate".to_string(),
            cpus: 0,
            memory_mb: 0,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect("create sandbox")
        .into_inner()
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
    let first = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-term-a".to_string(),
            cpus: 0,
            memory_mb: 0,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect("create first sandbox")
        .into_inner();
    let first_id = first.sandbox.expect("payload").sandbox_id;

    let second = client
        .create_sandbox(Request::new(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: "stack-term-b".to_string(),
            cpus: 0,
            memory_mb: 0,
            labels: std::collections::HashMap::new(),
        }))
        .await
        .expect("create second sandbox")
        .into_inner();
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
    assert_eq!(
        terminated.into_inner().sandbox.expect("payload").state,
        "terminated"
    );

    let replay = client
        .terminate_sandbox(Request::new(runtime_v2::TerminateSandboxRequest {
            sandbox_id: first_id,
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: "req-term-2".to_string(),
                idempotency_key: "term-key-a".to_string(),
                trace_id: "".to_string(),
            }),
        }))
        .await
        .expect("idempotent replay should succeed")
        .into_inner();
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
            let sandbox_id = response
                .into_inner()
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
            client
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
                .await
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
