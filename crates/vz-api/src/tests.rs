#![allow(clippy::unwrap_used)]

use super::*;
use axum::body::{Body, to_bytes};
use axum::http::Request;
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use tempfile::tempdir;
use tower::ServiceExt;
use vz_runtimed::{RuntimeDaemon, RuntimedConfig, serve_runtime_uds_with_shutdown};
use vz_stack::StackEvent;

struct TestDaemonHandle {
    #[allow(dead_code)]
    shutdown: Arc<tokio::sync::Notify>,
    #[allow(dead_code)]
    thread: JoinHandle<()>,
}

static TEST_DAEMONS: OnceLock<Mutex<HashMap<PathBuf, TestDaemonHandle>>> = OnceLock::new();

fn ensure_test_daemon_for_state_store(state_store_path: &Path) {
    let runtime_data_dir = state_store_path
        .parent()
        .expect("state_store_path should have parent")
        .join(".vz-runtime");
    std::fs::create_dir_all(&runtime_data_dir).expect("create runtime data dir");
    let socket_path = runtime_data_dir.join("runtimed.sock");

    let daemons = TEST_DAEMONS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = daemons.lock().expect("lock daemon map");
    if guard.contains_key(&socket_path) {
        return;
    }

    let daemon = Arc::new(
        RuntimeDaemon::start(RuntimedConfig {
            state_store_path: state_store_path.to_path_buf(),
            runtime_data_dir: runtime_data_dir.clone(),
            socket_path: socket_path.clone(),
        })
        .expect("start test runtimed daemon"),
    );
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_task = socket_path.clone();

    let thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build daemon runtime");
        let _ = runtime.block_on(async move {
            serve_runtime_uds_with_shutdown(daemon_task, socket_task, async move {
                shutdown_task.notified().await;
            })
            .await
        });
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if socket_path.exists() {
            guard.insert(socket_path, TestDaemonHandle { shutdown, thread });
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!(
        "daemon socket not ready for test: {}",
        state_store_path.display()
    );
}

fn base_test_config(state_store_path: PathBuf) -> ApiConfig {
    ApiConfig {
        state_store_path,
        daemon_socket_path: None,
        daemon_runtime_data_dir: None,
        daemon_auto_spawn: true,
        capabilities: RuntimeCapabilities {
            fs_quick_checkpoint: true,
            checkpoint_fork: true,
            ..RuntimeCapabilities::default()
        },
        event_poll_interval: Duration::from_millis(10),
        default_event_page_size: 2,
    }
}

fn test_config(state_store_path: PathBuf) -> ApiConfig {
    ensure_test_daemon_for_state_store(&state_store_path);
    base_test_config(state_store_path)
}

fn test_config_daemon_only(state_store_path: PathBuf) -> ApiConfig {
    let mut config = base_test_config(state_store_path);
    config.daemon_auto_spawn = false;
    config
}

fn sample_openapi_path(path: &str) -> String {
    match path {
        "/v1/events/{stack_name}" => "/v1/events/runtime-conformance-stack".to_string(),
        "/v1/containers/{container_id}" => "/v1/containers/ctr-nonexistent".to_string(),
        "/v1/images/{image_ref}" => "/v1/images/nginx:latest".to_string(),
        "/v1/receipts/{receipt_id}" => "/v1/receipts/rcp-nonexistent".to_string(),
        "/v1/builds/{build_id}" => "/v1/builds/bld-nonexistent".to_string(),
        _ => path.to_string(),
    }
}

fn openapi_document_json() -> serde_json::Value {
    serde_json::to_value(openapi_document()).expect("serialize OpenAPI document")
}

#[test]
fn openapi_document_contains_required_paths() {
    let document = openapi_document_json();
    let paths = document["paths"].as_object().unwrap();
    assert!(paths.contains_key("/v1/sandboxes"));
    assert!(paths.contains_key("/v1/sandboxes/{sandbox_id}"));
    assert!(paths.contains_key("/v1/leases"));
    assert!(paths.contains_key("/v1/leases/{lease_id}"));
    assert!(paths.contains_key("/v1/images"));
    assert!(paths.contains_key("/v1/images/{image_ref}"));
    assert!(paths.contains_key("/v1/builds"));
    assert!(paths.contains_key("/v1/builds/{build_id}"));
    assert!(paths.contains_key("/v1/containers"));
    assert!(paths.contains_key("/v1/containers/{container_id}"));
    assert!(paths.contains_key("/v1/executions"));
    assert!(paths.contains_key("/v1/executions/{execution_id}"));
    assert!(paths.contains_key("/v1/executions/{execution_id}/resize"));
    assert!(paths.contains_key("/v1/executions/{execution_id}/stdin"));
    assert!(paths.contains_key("/v1/executions/{execution_id}/signal"));
    assert!(paths.contains_key("/v1/checkpoints"));
    assert!(paths.contains_key("/v1/checkpoints/{checkpoint_id}"));
    assert!(paths.contains_key("/v1/checkpoints/{checkpoint_id}/children"));
    assert!(paths.contains_key("/v1/events/{stack_name}"));
    assert!(paths.contains_key("/v1/events/{stack_name}/stream"));
    assert!(paths.contains_key("/v1/events/{stack_name}/ws"));
    assert!(paths.contains_key("/v1/receipts/{receipt_id}"));
    assert!(paths.contains_key("/v1/capabilities"));
    assert!(paths.contains_key("/v1/files/read"));
    assert!(paths.contains_key("/v1/files/write"));
    assert!(paths.contains_key("/v1/files/list"));
    assert!(paths.contains_key("/v1/files/mkdir"));
    assert!(paths.contains_key("/v1/files/remove"));
    assert!(paths.contains_key("/v1/files/move"));
    assert!(paths.contains_key("/v1/files/copy"));
    assert!(paths.contains_key("/v1/files/chmod"));
    assert!(paths.contains_key("/v1/files/chown"));
}

#[test]
fn openapi_document_source_avoids_codegen_and_manual_schema_maps() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let openapi_doc_source = std::fs::read_to_string(manifest_dir.join("src/openapi_doc.rs"))
        .expect("read openapi_doc.rs");
    assert!(
        !openapi_doc_source
            .contains("include!(concat!(env!(\"OUT_DIR\"), \"/openapi_doc_generated.rs\"));"),
        "OpenAPI registration should be source-defined; OUT_DIR codegen include is disallowed"
    );
    assert!(
        !openapi_doc_source.contains("serde_json::Map::new"),
        "manual schema fallback maps are disallowed"
    );
    assert!(
        !openapi_doc_source.contains("serde_json::json!"),
        "manual schema json! fragments are disallowed"
    );
    assert!(
        !manifest_dir.join("build.rs").exists(),
        "vz-api should not use build.rs for OpenAPI/schema registration"
    );
}

#[test]
fn transport_modules_do_not_directly_import_state_store() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    for relative in ["src/handlers.rs", "src/daemon_bridge.rs"] {
        let source = std::fs::read_to_string(manifest_dir.join(relative))
            .expect("read transport source module");
        assert!(
            !source.contains("StateStore"),
            "{relative} must not directly import or mutate StateStore"
        );
    }
}

#[test]
fn daemon_client_config_defaults_to_autospawn_and_state_store_runtime_dir() {
    let state_store_path = PathBuf::from("/tmp/vz-api/state/stack-state.db");
    let state = ApiState::from(base_test_config(state_store_path.clone()));
    let config = daemon_client_config(&state);

    assert!(config.auto_spawn);
    assert_eq!(config.state_store_path, Some(state_store_path));
    assert_eq!(
        config.socket_path,
        PathBuf::from("/tmp/vz-api/state/.vz-runtime/runtimed.sock")
    );
    assert_eq!(
        config.runtime_data_dir,
        Some(PathBuf::from("/tmp/vz-api/state/.vz-runtime"))
    );
}

#[test]
fn daemon_client_config_can_disable_autospawn() {
    let state_store_path = PathBuf::from("/tmp/vz-api/state/stack-state.db");
    let mut api_config = base_test_config(state_store_path);
    api_config.daemon_auto_spawn = false;
    let state = ApiState::from(api_config);
    let config = daemon_client_config(&state);

    assert!(!config.auto_spawn);
}

#[test]
fn daemon_client_config_uses_explicit_socket_and_runtime_dir_overrides() {
    let state_store_path = PathBuf::from("/tmp/vz-api/state/stack-state.db");
    let mut api_config = base_test_config(state_store_path);
    api_config.daemon_socket_path = Some(PathBuf::from("/tmp/custom-runtime/runtimed.sock"));
    api_config.daemon_runtime_data_dir = Some(PathBuf::from("/tmp/custom-runtime"));
    let state = ApiState::from(api_config);
    let config = daemon_client_config(&state);

    assert_eq!(
        config.socket_path,
        PathBuf::from("/tmp/custom-runtime/runtimed.sock")
    );
    assert_eq!(
        config.runtime_data_dir,
        Some(PathBuf::from("/tmp/custom-runtime"))
    );
}

#[tokio::test]
async fn capabilities_endpoint_returns_runtime_capabilities() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config(state_path));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/capabilities")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(payload["request_id"].as_str().is_some());
    let capabilities = payload["capabilities"].as_array().unwrap();
    assert!(capabilities.contains(&serde_json::Value::String(
        "fs_quick_checkpoint".to_string()
    )));
    assert!(capabilities.contains(&serde_json::Value::String("checkpoint_fork".to_string())));
}

#[tokio::test]
async fn sandbox_create_requires_daemon_when_legacy_fallback_disabled() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config_daemon_only(state_path.clone()));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"stack_name":"stack-daemon-only"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "daemon_unavailable"
    );

    let store = StateStore::open(&state_path).unwrap();
    assert!(store.load_sandbox("stack-daemon-only").unwrap().is_none());
}

#[tokio::test]
async fn lease_open_requires_daemon_when_legacy_fallback_disabled() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config_daemon_only(state_path.clone()));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/leases")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"sandbox_id":"sbx-daemon-only"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "daemon_unavailable"
    );

    let store = StateStore::open(&state_path).unwrap();
    assert!(store.list_leases().unwrap().is_empty());
}

#[tokio::test]
async fn build_start_requires_daemon_when_legacy_fallback_disabled() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config_daemon_only(state_path.clone()));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/builds")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"sandbox_id":"sbx-daemon-only","context":"."}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "daemon_unavailable"
    );

    let store = StateStore::open(&state_path).unwrap();
    assert!(store.list_builds().unwrap().is_empty());
}

#[tokio::test]
async fn execution_create_requires_daemon_when_legacy_fallback_disabled() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config_daemon_only(state_path.clone()));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"container_id":"ctr-daemon","cmd":["echo"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "daemon_unavailable"
    );

    let store = StateStore::open(&state_path).unwrap();
    assert!(store.list_executions().unwrap().is_empty());
}

#[tokio::test]
async fn checkpoint_create_requires_daemon_when_legacy_fallback_disabled() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config_daemon_only(state_path.clone()));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/checkpoints")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"sandbox_id":"sbx-daemon","class":"fs_quick"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "daemon_unavailable"
    );

    let store = StateStore::open(&state_path).unwrap();
    assert!(store.list_checkpoints().unwrap().is_empty());
}

#[tokio::test]
async fn events_endpoint_respects_cursor_and_limit() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();
    for index in 0..3 {
        store
            .emit_event(
                "my-stack",
                &StackEvent::ServiceCreating {
                    stack_name: "my-stack".to_string(),
                    service_name: format!("svc-{index}"),
                },
            )
            .unwrap();
    }

    let app = router(test_config(state_path.clone()));
    let first_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/events/my-stack?after=0&limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_response.status(), StatusCode::OK);

    let first_payload: serde_json::Value = serde_json::from_slice(
        &to_bytes(first_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first_payload["events"].as_array().unwrap().len(), 2);
    let next_cursor = first_payload["next_cursor"].as_i64().unwrap();

    let second_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/events/my-stack?after={next_cursor}&limit=2"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_response.status(), StatusCode::OK);
    let second_payload: serde_json::Value = serde_json::from_slice(
        &to_bytes(second_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(second_payload["events"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn transport_parity_error_codes_match_runtime_contract() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    // Containers are now implemented, so GET /v1/containers returns 200.
    let app = router(test_config(state_path.clone()));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/containers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // GET for a non-existent container returns 404 with proper error envelope.
    let app = router(test_config(state_path));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/containers/nonexistent-id")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn transport_parity_event_cursor_matches_state_store_slice() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();
    for index in 0..4 {
        store
            .emit_event(
                "my-stack",
                &StackEvent::ServiceCreating {
                    stack_name: "my-stack".to_string(),
                    service_name: format!("svc-{index}"),
                },
            )
            .unwrap();
    }
    let expected = store.load_events_since_limited("my-stack", 0, 3).unwrap();
    let expected_ids: Vec<i64> = expected.iter().map(|record| record.id).collect();

    let app = router(test_config(state_path));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/my-stack?after=0&limit=3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let ids: Vec<i64> = payload["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, expected_ids);
}

#[test]
fn transport_parity_openapi_and_grpc_surface_require_metadata_and_ordering() {
    let exec_request = vz_agent_proto::ExecRequest::default();
    assert!(exec_request.metadata.is_none());
    let oci_create_request = vz_agent_proto::OciCreateRequest::default();
    assert!(oci_create_request.metadata.is_none());

    let exec_event = vz_agent_proto::ExecEvent::default();
    assert_eq!(exec_event.sequence, 0);
    assert!(exec_event.request_id.is_empty());

    let document = openapi_document_json();
    let paths = document["paths"].as_object().unwrap();
    assert!(paths.contains_key("/v1/executions"));
    assert!(paths.contains_key("/v1/events/{stack_name}/stream"));
    assert!(paths.contains_key("/v1/events/{stack_name}/ws"));
}

#[tokio::test]
async fn transport_parity_openapi_matrix_paths_match_contract() {
    let document = openapi_document_json();
    let paths = document["paths"].as_object().unwrap();
    let mut matrix_paths = BTreeSet::new();

    for entry in vz_runtime_contract::PRIMITIVE_CONFORMANCE_MATRIX {
        if let Some(surface) = entry.openapi {
            assert!(!surface.path.is_empty());
            assert!(surface.path.starts_with('/'));
            assert!(!surface.surface.is_empty());
            assert!(
                paths.contains_key(surface.path),
                "missing OpenAPI path `{}` for `{}`",
                surface.path,
                entry.operation.as_str()
            );
            matrix_paths.insert(surface.path);
        }
    }

    assert!(!matrix_paths.is_empty());
}

#[tokio::test]
async fn transport_parity_openapi_surface_errors_match_runtime_operation_labels() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config(state_path));

    for entry in vz_runtime_contract::PRIMITIVE_CONFORMANCE_MATRIX {
        let Some(surface) = entry.openapi else {
            continue;
        };

        let request = Request::builder()
            .uri(sample_openapi_path(surface.path))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();

        if status == StatusCode::NOT_IMPLEMENTED {
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let envelope: MachineErrorEnvelope = serde_json::from_slice(&body).unwrap();

            assert_eq!(
                envelope.error.code,
                vz_runtime_contract::MachineErrorCode::UnsupportedOperation
            );
            assert_eq!(
                envelope.error.details.get("operation").map(String::as_str),
                Some(surface.surface),
                "matrix operation mismatch for `{}` at `{}`",
                entry.operation.as_str(),
                surface.path
            );
            continue;
        }

        if status == StatusCode::OK
            && matches!(
                surface.path,
                "/v1/capabilities"
                    | "/v1/events/{stack_name}"
                    | "/v1/sandboxes"
                    | "/v1/leases"
                    | "/v1/executions"
                    | "/v1/checkpoints"
                    | "/v1/containers"
                    | "/v1/images"
                    | "/v1/builds"
            )
        {
            continue;
        }

        // 404 is valid for parameterized GET endpoints where no entity exists.
        if status == StatusCode::NOT_FOUND
            && matches!(
                surface.path,
                "/v1/receipts/{receipt_id}"
                    | "/v1/containers/{container_id}"
                    | "/v1/images/{image_ref}"
                    | "/v1/builds/{build_id}"
            )
        {
            continue;
        }

        panic!(
            "unexpected matrix API status for `{}` at `{}`: {status}",
            entry.operation.as_str(),
            surface.path
        );
    }
}

fn test_router() -> (Router, tempfile::TempDir) {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();
    let app = router(test_config(state_path));
    (app, temp_dir)
}

#[tokio::test]
async fn sandbox_create_returns_201() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"cpus": 2, "memory_mb": 1024}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let sandbox_id = payload["sandbox"]["sandbox_id"].as_str().unwrap();
    assert!(sandbox_id.starts_with("sbx-"), "id should start with sbx-");
    assert_eq!(payload["sandbox"]["state"].as_str().unwrap(), "ready");
    assert_eq!(payload["sandbox"]["cpus"].as_u64().unwrap(), 2);
    assert_eq!(payload["sandbox"]["memory_mb"].as_u64().unwrap(), 1024);
}

#[tokio::test]
async fn sandbox_create_stream_terminal_validation_error_maps_to_http_bad_request() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"stack_name":"sbx-invalid-project-dir","labels":{"project_dir":"relative/not-absolute"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"]["code"].as_str(), Some("invalid_request"));
    assert!(
        payload["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("project_dir")),
        "error message should include project_dir validation details"
    );
}

#[tokio::test]
async fn sandbox_list_empty() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/sandboxes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let sandboxes = payload["sandboxes"].as_array().unwrap();
    assert!(sandboxes.is_empty());
}

#[tokio::test]
async fn sandbox_get_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/sandboxes/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn sandbox_create_then_get() {
    let (app, _dir) = test_router();

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"cpus": 4}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);

    let create_body = to_bytes(create_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let create_payload: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    let sandbox_id = create_payload["sandbox"]["sandbox_id"]
        .as_str()
        .unwrap()
        .to_string();

    let get_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/sandboxes/{sandbox_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    let get_body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let get_payload: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(
        get_payload["sandbox"]["sandbox_id"].as_str().unwrap(),
        sandbox_id
    );
    assert_eq!(get_payload["sandbox"]["cpus"].as_u64().unwrap(), 4);
}

#[tokio::test]
async fn sandbox_create_with_startup_selection_round_trips() {
    let (app, _dir) = test_router();

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"base_image_ref":"alpine:3.20","main_container":"workspace-main"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);

    let create_body = to_bytes(create_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let create_payload: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    let sandbox = &create_payload["sandbox"];
    let sandbox_id = sandbox["sandbox_id"].as_str().unwrap().to_string();

    assert_eq!(sandbox["base_image_ref"].as_str(), Some("alpine:3.20"));
    assert_eq!(sandbox["main_container"].as_str(), Some("workspace-main"));

    let get_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/sandboxes/{sandbox_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    let get_body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let get_payload: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(
        get_payload["sandbox"]["base_image_ref"].as_str(),
        Some("alpine:3.20")
    );
    assert_eq!(
        get_payload["sandbox"]["main_container"].as_str(),
        Some("workspace-main")
    );

    let list_response = app
        .oneshot(
            Request::builder()
                .uri("/v1/sandboxes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);

    let list_body = to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let list_payload: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    let listed = list_payload["sandboxes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["sandbox_id"].as_str() == Some(sandbox_id.as_str()))
        .expect("created sandbox should be present in list");
    assert_eq!(listed["base_image_ref"].as_str(), Some("alpine:3.20"));
    assert_eq!(listed["main_container"].as_str(), Some("workspace-main"));
}

#[tokio::test]
async fn container_create_uses_sandbox_startup_defaults_when_image_omitted() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();
    let app = router(test_config(state_path.clone()));

    let create_sandbox = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"base_image_ref":"alpine:3.20","main_container":"workspace-main"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_sandbox.status(), StatusCode::CREATED);
    let create_sandbox_body = to_bytes(create_sandbox.into_body(), usize::MAX)
        .await
        .unwrap();
    let sandbox_payload: serde_json::Value = serde_json::from_slice(&create_sandbox_body).unwrap();
    let sandbox_id = sandbox_payload["sandbox"]["sandbox_id"]
        .as_str()
        .unwrap()
        .to_string();

    let create_container = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/containers")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"sandbox_id":"{sandbox_id}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_container.status(), StatusCode::CREATED);

    let create_container_body = to_bytes(create_container.into_body(), usize::MAX)
        .await
        .unwrap();
    let container_payload: serde_json::Value =
        serde_json::from_slice(&create_container_body).unwrap();
    assert_eq!(
        container_payload["container"]["image_digest"].as_str(),
        Some("alpine:3.20")
    );
    let container_id = container_payload["container"]["container_id"]
        .as_str()
        .unwrap()
        .to_string();

    let store = StateStore::open(&state_path).unwrap();
    let persisted = store
        .load_container(&container_id)
        .unwrap()
        .expect("container should be persisted");
    assert_eq!(
        persisted.container_spec.cmd,
        vec!["workspace-main".to_string()]
    );
}

#[tokio::test]
async fn container_create_preserves_explicit_overrides_when_sandbox_defaults_exist() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();
    let app = router(test_config(state_path.clone()));

    let create_sandbox = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"base_image_ref":"alpine:3.20","main_container":"workspace-main"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_sandbox.status(), StatusCode::CREATED);
    let create_sandbox_body = to_bytes(create_sandbox.into_body(), usize::MAX)
        .await
        .unwrap();
    let sandbox_payload: serde_json::Value = serde_json::from_slice(&create_sandbox_body).unwrap();
    let sandbox_id = sandbox_payload["sandbox"]["sandbox_id"]
        .as_str()
        .unwrap()
        .to_string();

    let create_container = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/containers")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"sandbox_id":"{sandbox_id}","image_digest":"ubuntu:24.04","cmd":["bash","-lc","echo ready"]}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_container.status(), StatusCode::CREATED);

    let create_container_body = to_bytes(create_container.into_body(), usize::MAX)
        .await
        .unwrap();
    let container_payload: serde_json::Value =
        serde_json::from_slice(&create_container_body).unwrap();
    assert_eq!(
        container_payload["container"]["image_digest"].as_str(),
        Some("ubuntu:24.04")
    );
    let container_id = container_payload["container"]["container_id"]
        .as_str()
        .unwrap()
        .to_string();

    let store = StateStore::open(&state_path).unwrap();
    let persisted = store
        .load_container(&container_id)
        .unwrap()
        .expect("container should be persisted");
    assert_eq!(
        persisted.container_spec.cmd,
        vec![
            "bash".to_string(),
            "-lc".to_string(),
            "echo ready".to_string()
        ]
    );
}

#[tokio::test]
async fn sandbox_terminate() {
    let (app, _dir) = test_router();

    // Create a sandbox
    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .body(Body::from(r#"{}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);

    let create_body = to_bytes(create_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let create_payload: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    let sandbox_id = create_payload["sandbox"]["sandbox_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Terminate sandbox (Creating -> Failed since can't go directly to Terminated)
    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v1/sandboxes/{sandbox_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete_response.status(), StatusCode::OK);

    let delete_body = to_bytes(delete_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let delete_payload: serde_json::Value = serde_json::from_slice(&delete_body).unwrap();
    let state = delete_payload["sandbox"]["state"].as_str().unwrap();
    assert!(
        state == "failed" || state == "terminated",
        "expected terminal state, got {state}"
    );

    // GET should still return it in terminal state
    let get_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/sandboxes/{sandbox_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    let get_body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let get_payload: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    let final_state = get_payload["sandbox"]["state"].as_str().unwrap();
    assert!(
        final_state == "failed" || final_state == "terminated",
        "expected terminal state after GET, got {final_state}"
    );
}

fn test_config_with_resize(state_store_path: PathBuf) -> ApiConfig {
    ensure_test_daemon_for_state_store(&state_store_path);
    ApiConfig {
        state_store_path,
        daemon_socket_path: None,
        daemon_runtime_data_dir: None,
        daemon_auto_spawn: true,
        capabilities: RuntimeCapabilities {
            fs_quick_checkpoint: true,
            checkpoint_fork: true,
            live_resize: true,
            ..RuntimeCapabilities::default()
        },
        event_poll_interval: Duration::from_millis(10),
        default_event_page_size: 2,
    }
}

#[tokio::test]
async fn resize_on_running_execution_without_active_session_returns_501() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();
    let app = router(test_config_with_resize(state_path.clone()));
    let store = StateStore::open(&state_path).unwrap();

    // Create a Running execution directly.
    let execution = Execution {
        execution_id: "exec-resize-1".to_string(),
        container_id: "ctr-1".to_string(),
        exec_spec: ExecutionSpec {
            cmd: vec!["bash".to_string()],
            args: vec![],
            env_override: BTreeMap::new(),
            pty: true,
            timeout_secs: None,
        },
        state: ExecutionState::Running,
        exit_code: None,
        started_at: Some(now_epoch_secs()),
        ended_at: None,
    };
    store.save_execution(&execution).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions/exec-resize-1/resize")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"cols":120,"rows":40}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn resize_on_non_running_execution_returns_409() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();

    // Create a Queued execution.
    let execution = Execution {
        execution_id: "exec-resize-q".to_string(),
        container_id: "ctr-1".to_string(),
        exec_spec: ExecutionSpec {
            cmd: vec!["bash".to_string()],
            args: vec![],
            env_override: BTreeMap::new(),
            pty: true,
            timeout_secs: None,
        },
        state: ExecutionState::Queued,
        exit_code: None,
        started_at: None,
        ended_at: None,
    };
    store.save_execution(&execution).unwrap();

    let app = router(test_config_with_resize(state_path));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions/exec-resize-q/resize")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"cols":80,"rows":24}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn signal_on_running_execution_without_active_session_returns_501() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();
    let app = router(test_config(state_path.clone()));
    let store = StateStore::open(&state_path).unwrap();

    // Create a Running execution.
    let execution = Execution {
        execution_id: "exec-sig-1".to_string(),
        container_id: "ctr-1".to_string(),
        exec_spec: ExecutionSpec {
            cmd: vec!["bash".to_string()],
            args: vec![],
            env_override: BTreeMap::new(),
            pty: false,
            timeout_secs: None,
        },
        state: ExecutionState::Running,
        exit_code: None,
        started_at: Some(now_epoch_secs()),
        ended_at: None,
    };
    store.save_execution(&execution).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions/exec-sig-1/signal")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"signal":"SIGTERM"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn cancel_queued_execution_transitions_to_canceled() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();
    let app = router(test_config(state_path.clone()));
    let store = StateStore::open(&state_path).unwrap();

    // Create a Queued execution.
    let execution = Execution {
        execution_id: "exec-cancel-q".to_string(),
        container_id: "ctr-1".to_string(),
        exec_spec: ExecutionSpec {
            cmd: vec!["echo".to_string()],
            args: vec![],
            env_override: BTreeMap::new(),
            pty: false,
            timeout_secs: None,
        },
        state: ExecutionState::Queued,
        exit_code: None,
        started_at: None,
        ended_at: None,
    };
    store.save_execution(&execution).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/v1/executions/exec-cancel-q")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["execution"]["state"].as_str().unwrap(), "canceled");
    assert!(payload["execution"]["ended_at"].as_u64().is_some());
}

#[tokio::test]
async fn cancel_terminal_execution_is_idempotent() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();

    // Create a Completed (terminal) execution.
    let mut execution = Execution {
        execution_id: "exec-done".to_string(),
        container_id: "ctr-1".to_string(),
        exec_spec: ExecutionSpec {
            cmd: vec!["echo".to_string()],
            args: vec![],
            env_override: BTreeMap::new(),
            pty: false,
            timeout_secs: None,
        },
        state: ExecutionState::Running,
        exit_code: None,
        started_at: Some(now_epoch_secs()),
        ended_at: None,
    };
    let _ = execution.transition_to(ExecutionState::Exited);
    execution.exit_code = Some(0);
    execution.ended_at = Some(now_epoch_secs());
    store.save_execution(&execution).unwrap();

    let app = router(test_config(state_path));
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/v1/executions/exec-done")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // Should remain in its terminal state, not transition again.
    assert_eq!(payload["execution"]["state"].as_str().unwrap(), "exited");
}

// ── Checkpoint capability gating tests ──

/// Helper that builds a router with specific capability flags.
fn test_config_with_capabilities(
    state_store_path: PathBuf,
    capabilities: RuntimeCapabilities,
) -> ApiConfig {
    ensure_test_daemon_for_state_store(&state_store_path);
    ApiConfig {
        state_store_path,
        daemon_socket_path: None,
        daemon_runtime_data_dir: None,
        daemon_auto_spawn: true,
        capabilities,
        event_poll_interval: Duration::from_millis(10),
        default_event_page_size: 2,
    }
}

#[tokio::test]
async fn checkpoint_create_vm_full_rejected_when_capability_disabled() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    // vm_full_checkpoint is false by default.
    let config = test_config_with_capabilities(
        state_path,
        RuntimeCapabilities {
            fs_quick_checkpoint: true,
            ..RuntimeCapabilities::default()
        },
    );
    let app = router(config);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/checkpoints")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"sandbox_id": "sbx-test", "class": "vm_full"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "unsupported_checkpoint_class"
    );
}

#[tokio::test]
async fn checkpoint_create_fs_quick_succeeds_when_capability_enabled() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let config = test_config_with_capabilities(
        state_path,
        RuntimeCapabilities {
            fs_quick_checkpoint: true,
            ..RuntimeCapabilities::default()
        },
    );
    let app = router(config);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/checkpoints")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"sandbox_id": "sbx-test", "class": "fs_quick"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        payload["checkpoint"]["checkpoint_id"]
            .as_str()
            .unwrap()
            .starts_with("ckpt-")
    );
    assert_eq!(payload["checkpoint"]["class"].as_str().unwrap(), "fs_quick");
}

#[tokio::test]
async fn checkpoint_create_fs_quick_rejected_when_capability_disabled() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    // Both checkpoint capabilities disabled.
    let config = test_config_with_capabilities(state_path, RuntimeCapabilities::default());
    let app = router(config);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/checkpoints")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"sandbox_id": "sbx-test"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Default class is fs_quick; it should be rejected.
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "unsupported_checkpoint_class"
    );
}

#[tokio::test]
async fn checkpoint_fork_from_non_ready_returns_409() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();

    // Create a checkpoint directly in Creating state (no transition to Ready).
    let checkpoint = Checkpoint {
        checkpoint_id: "ckpt-creating".to_string(),
        sandbox_id: "sbx-test".to_string(),
        parent_checkpoint_id: None,
        class: CheckpointClass::FsQuick,
        state: CheckpointState::Creating,
        created_at: 1000,
        compatibility_fingerprint: "fp-1".to_string(),
    };
    store.save_checkpoint(&checkpoint).unwrap();

    let config = test_config(state_path);
    let app = router(config);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/checkpoints/ckpt-creating/fork")
                .header("content-type", "application/json")
                .body(Body::from(r#"{}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "checkpoint_not_ready"
    );
}

#[tokio::test]
async fn checkpoint_children_returns_forked_children() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();

    // Create a parent checkpoint in Ready state.
    let mut parent = Checkpoint {
        checkpoint_id: "ckpt-parent".to_string(),
        sandbox_id: "sbx-parent".to_string(),
        parent_checkpoint_id: None,
        class: CheckpointClass::FsQuick,
        state: CheckpointState::Creating,
        created_at: 1000,
        compatibility_fingerprint: "fp-parent".to_string(),
    };
    parent.transition_to(CheckpointState::Ready).unwrap();
    store.save_checkpoint(&parent).unwrap();

    // Create a child checkpoint.
    let mut child = Checkpoint {
        checkpoint_id: "ckpt-child".to_string(),
        sandbox_id: "sbx-child".to_string(),
        parent_checkpoint_id: Some("ckpt-parent".to_string()),
        class: CheckpointClass::FsQuick,
        state: CheckpointState::Creating,
        created_at: 2000,
        compatibility_fingerprint: "fp-parent".to_string(),
    };
    child.transition_to(CheckpointState::Ready).unwrap();
    store.save_checkpoint(&child).unwrap();

    let config = test_config(state_path);
    let app = router(config);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/checkpoints/ckpt-parent/children")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let checkpoints = payload["checkpoints"].as_array().unwrap();
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(
        checkpoints[0]["checkpoint_id"].as_str().unwrap(),
        "ckpt-child"
    );
    assert_eq!(
        checkpoints[0]["parent_checkpoint_id"].as_str().unwrap(),
        "ckpt-parent"
    );
}

#[tokio::test]
async fn checkpoint_children_404_for_unknown_parent() {
    let (app, _dir) = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/checkpoints/ckpt-nonexistent/children")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn checkpoint_restore_includes_fingerprint_metadata() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();

    let mut checkpoint = Checkpoint {
        checkpoint_id: "ckpt-fp".to_string(),
        sandbox_id: "sbx-fp".to_string(),
        parent_checkpoint_id: None,
        class: CheckpointClass::FsQuick,
        state: CheckpointState::Creating,
        created_at: 1000,
        compatibility_fingerprint: "kernel-6.1-arm64".to_string(),
    };
    checkpoint.transition_to(CheckpointState::Ready).unwrap();
    store.save_checkpoint(&checkpoint).unwrap();

    let config = test_config(state_path);
    let app = router(config);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/checkpoints/ckpt-fp/restore")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["compatibility_fingerprint"].as_str().unwrap(),
        "kernel-6.1-arm64"
    );
    assert!(payload["restore_note"].as_str().is_some());
}

// ── Cross-transport behavior parity tests ──────────────────────

#[test]
fn transport_parity_openapi_operations_match_grpc_rpcs() {
    let doc = openapi_document_json();
    let paths = doc["paths"].as_object().unwrap();

    // Extract all operationIds from OpenAPI.
    let mut openapi_operations: Vec<String> = Vec::new();
    for (_path, methods) in paths {
        let methods_obj = match methods.as_object() {
            Some(obj) => obj,
            None => continue,
        };
        for (_method, op) in methods_obj {
            if let Some(op_id) = op.get("operationId").and_then(|v| v.as_str()) {
                openapi_operations.push(op_id.to_string());
            }
        }
    }
    openapi_operations.sort();

    // Define expected gRPC RPC names (from runtime_v2.proto).
    let grpc_rpcs = [
        // SandboxService
        "CreateSandbox",
        "GetSandbox",
        "ListSandboxes",
        "TerminateSandbox",
        // LeaseService
        "OpenLease",
        "GetLease",
        "ListLeases",
        "HeartbeatLease",
        "CloseLease",
        // ContainerService
        "CreateContainer",
        "GetContainer",
        "ListContainers",
        "RemoveContainer",
        // ExecutionService
        "CreateExecution",
        "GetExecution",
        "ListExecutions",
        "CancelExecution",
        "StreamExecOutput",
        "WriteExecStdin",
        "ResizeExecPty",
        "SignalExec",
        // CheckpointService
        "CreateCheckpoint",
        "GetCheckpoint",
        "ListCheckpoints",
        "RestoreCheckpoint",
        "ForkCheckpoint",
        // BuildService
        "StartBuild",
        "GetBuild",
        "ListBuilds",
        "CancelBuild",
        "StreamBuildEvents",
        // EventService
        "ListEvents",
        "StreamEvents",
        // FileService
        "ReadFile",
        "WriteFile",
        "ListFiles",
        "MakeDir",
        "RemovePath",
        "MovePath",
        "CopyPath",
        "ChmodPath",
        "ChownPath",
        // CapabilityService
        "GetCapabilities",
    ];

    // Streaming RPCs use SSE/WS, not REST — they have separate
    // OpenAPI operations (streamEventsSse, streamEventsWs) rather
    // than a direct camelCase mapping.
    let streaming_rpcs = ["StreamExecOutput", "StreamBuildEvents", "StreamEvents"];

    for rpc in &grpc_rpcs {
        if streaming_rpcs.contains(rpc) {
            continue;
        }
        // Map PascalCase to camelCase: "CreateSandbox" -> "createSandbox"
        let camel = rpc[..1].to_lowercase() + &rpc[1..];
        // ResizeExecPty maps to "resizeExec" in OpenAPI (shorter form)
        let aliases: Vec<String> = if *rpc == "ResizeExecPty" {
            vec![camel.clone(), "resizeExec".to_string()]
        } else {
            vec![camel.clone()]
        };
        assert!(
            aliases
                .iter()
                .any(|alias| openapi_operations.iter().any(|op| op == alias)),
            "gRPC RPC '{}' has no matching OpenAPI operationId (tried {:?}). Available: {:?}",
            rpc,
            aliases,
            openapi_operations
        );
    }
}

#[test]
fn transport_parity_shared_error_codes() {
    let doc = openapi_document_json();
    let error_schema = &doc["components"]["schemas"]["ErrorResponse"];
    assert!(
        error_schema.is_object(),
        "ErrorResponse schema must exist in components/schemas"
    );

    // Verify error response has the required 'error' field with code and message.
    let properties = &error_schema["properties"];
    assert!(
        properties["error"].is_object(),
        "ErrorResponse must have an 'error' property"
    );

    let inline_error_properties = properties["error"]["properties"].as_object();
    let referenced_error_properties = properties["error"]
        .get("$ref")
        .and_then(|reference| reference.as_str())
        .and_then(|reference| reference.rsplit('/').next())
        .and_then(|schema_name| {
            doc["components"]["schemas"][schema_name]["properties"].as_object()
        });
    let error_properties = inline_error_properties
        .or(referenced_error_properties)
        .unwrap_or_else(|| {
            panic!("error schema must define properties inline or by component reference")
        });

    assert!(
        error_properties
            .get("code")
            .is_some_and(|value| value.is_object()),
        "error.code must be defined"
    );
    assert!(
        error_properties
            .get("message")
            .is_some_and(|value| value.is_object()),
        "error.message must be defined"
    );
    assert!(
        error_properties
            .get("request_id")
            .is_some_and(|value| value.is_object()),
        "error.request_id must be defined"
    );
}

#[test]
fn transport_parity_request_metadata_fields_present() {
    let doc = openapi_document_json();
    let paths = doc["paths"].as_object().unwrap();
    let component_params = doc["components"]["parameters"].as_object();

    let component_header = |component_name: &str, expected_header_name: &str| -> bool {
        component_params
            .and_then(|params| params.get(component_name))
            .map(|param| {
                param["in"].as_str() == Some("header")
                    && param["name"].as_str() == Some(expected_header_name)
            })
            .unwrap_or(false)
    };

    let inline_header = |expected_header_name: &str| -> bool {
        paths.values().any(|methods| {
            methods
                .as_object()
                .into_iter()
                .flat_map(|method_map| method_map.values())
                .any(|operation| {
                    operation["parameters"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .any(|param| {
                            param["in"].as_str() == Some("header")
                                && param["name"].as_str() == Some(expected_header_name)
                        })
                })
        })
    };

    assert!(
        component_header("IdempotencyKey", "Idempotency-Key") || inline_header("Idempotency-Key"),
        "Idempotency-Key header parameter must be documented"
    );
    assert!(
        component_header("RequestId", "X-Request-Id") || inline_header("X-Request-Id"),
        "X-Request-Id header parameter must be documented"
    );
}

#[test]
fn transport_parity_entity_payload_field_consistency() {
    let doc = openapi_document_json();
    let schemas = doc["components"]["schemas"].as_object().unwrap();

    // SandboxPayload fields: sandbox_id, backend, state, cpus, memory_mb,
    // created_at, updated_at, labels.
    let sandbox = &schemas["SandboxPayload"]["properties"];
    assert!(
        sandbox["sandbox_id"].is_object(),
        "SandboxPayload.sandbox_id missing"
    );
    assert!(
        sandbox["backend"].is_object(),
        "SandboxPayload.backend missing"
    );
    assert!(sandbox["state"].is_object(), "SandboxPayload.state missing");
    assert!(sandbox["cpus"].is_object(), "SandboxPayload.cpus missing");
    assert!(
        sandbox["memory_mb"].is_object(),
        "SandboxPayload.memory_mb missing"
    );
    assert!(
        sandbox["created_at"].is_object(),
        "SandboxPayload.created_at missing"
    );
    assert!(
        sandbox["updated_at"].is_object(),
        "SandboxPayload.updated_at missing"
    );
    assert!(
        sandbox["labels"].is_object(),
        "SandboxPayload.labels missing"
    );

    // LeasePayload fields: lease_id, sandbox_id, ttl_secs, last_heartbeat_at, state.
    let lease = &schemas["LeasePayload"]["properties"];
    assert!(
        lease["lease_id"].is_object(),
        "LeasePayload.lease_id missing"
    );
    assert!(
        lease["sandbox_id"].is_object(),
        "LeasePayload.sandbox_id missing"
    );
    assert!(
        lease["ttl_secs"].is_object(),
        "LeasePayload.ttl_secs missing"
    );
    assert!(
        lease["last_heartbeat_at"].is_object(),
        "LeasePayload.last_heartbeat_at missing"
    );
    assert!(lease["state"].is_object(), "LeasePayload.state missing");

    // ExecutionPayload fields: execution_id, container_id, state, exit_code,
    // started_at, ended_at.
    let exec = &schemas["ExecutionPayload"]["properties"];
    assert!(
        exec["execution_id"].is_object(),
        "ExecutionPayload.execution_id missing"
    );
    assert!(
        exec["container_id"].is_object(),
        "ExecutionPayload.container_id missing"
    );
    assert!(exec["state"].is_object(), "ExecutionPayload.state missing");
    assert!(
        exec["exit_code"].is_object(),
        "ExecutionPayload.exit_code missing"
    );
    assert!(
        exec["started_at"].is_object(),
        "ExecutionPayload.started_at missing"
    );
    assert!(
        exec["ended_at"].is_object(),
        "ExecutionPayload.ended_at missing"
    );

    // CheckpointPayload fields: checkpoint_id, sandbox_id, parent_checkpoint_id,
    // class, state, compatibility_fingerprint, created_at.
    let ckpt = &schemas["CheckpointPayload"]["properties"];
    assert!(
        ckpt["checkpoint_id"].is_object(),
        "CheckpointPayload.checkpoint_id missing"
    );
    assert!(
        ckpt["sandbox_id"].is_object(),
        "CheckpointPayload.sandbox_id missing"
    );
    assert!(
        ckpt["parent_checkpoint_id"].is_object(),
        "CheckpointPayload.parent_checkpoint_id missing"
    );
    assert!(ckpt["class"].is_object(), "CheckpointPayload.class missing");
    assert!(ckpt["state"].is_object(), "CheckpointPayload.state missing");
    assert!(
        ckpt["compatibility_fingerprint"].is_object(),
        "CheckpointPayload.compatibility_fingerprint missing"
    );
    assert!(
        ckpt["created_at"].is_object(),
        "CheckpointPayload.created_at missing"
    );
}

#[test]
fn transport_parity_idempotency_on_mutating_operations() {
    let doc = openapi_document_json();
    let paths = doc["paths"].as_object().unwrap();
    let component_params = doc["components"]["parameters"].as_object();

    let mut mutating_without_idempotency = Vec::new();

    let has_idempotency_parameter = |operation: &serde_json::Value| {
        operation
            .get("parameters")
            .and_then(|p| p.as_array())
            .map(|params| {
                params.iter().any(|param| {
                    if param
                        .get("name")
                        .and_then(|name| name.as_str())
                        .zip(param.get("in").and_then(|location| location.as_str()))
                        .is_some_and(|(name, location)| {
                            location == "header" && name == "Idempotency-Key"
                        })
                    {
                        return true;
                    }

                    let Some(reference) = param.get("$ref").and_then(|value| value.as_str()) else {
                        return false;
                    };
                    if reference.contains("IdempotencyKey") {
                        return true;
                    }

                    let Some(component_name) = reference.rsplit('/').next() else {
                        return false;
                    };

                    component_params
                        .and_then(|params| params.get(component_name))
                        .map(|component| {
                            component["in"].as_str() == Some("header")
                                && component["name"].as_str() == Some("Idempotency-Key")
                        })
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false)
    };

    for (path, methods) in paths {
        let methods_obj = match methods.as_object() {
            Some(obj) => obj,
            None => continue,
        };
        // Check POST operations.
        if let Some(post_op) = methods_obj.get("post") {
            let op_id = post_op
                .get("operationId")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            let has_idempotency = has_idempotency_parameter(post_op);

            // Heartbeat, restore, resize, signal are POST but may not
            // need idempotency.
            let exempt = [
                "heartbeatLease",
                "restoreCheckpoint",
                "resizeExec",
                "signalExec",
            ];
            if !has_idempotency && !exempt.contains(&op_id) {
                mutating_without_idempotency.push(format!("{} ({})", path, op_id));
            }
        }

        // Check DELETE operations (which are also mutating).
        if let Some(delete_op) = methods_obj.get("delete") {
            let op_id = delete_op
                .get("operationId")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            let has_idempotency = has_idempotency_parameter(delete_op);

            let exempt_delete = ["removeContainer", "cancelExecution"];
            if !has_idempotency && !exempt_delete.contains(&op_id) {
                mutating_without_idempotency.push(format!("{} DELETE ({})", path, op_id));
            }
        }
    }

    // All major create/terminate/close operations should have idempotency.
    // We assert that none of the critical mutating POST operations lack it.
    let critical_missing: Vec<&str> = mutating_without_idempotency
        .iter()
        .filter(|s| {
            s.contains("createSandbox")
                || s.contains("openLease")
                || s.contains("createExecution")
                || s.contains("createCheckpoint")
                || s.contains("terminateSandbox")
                || s.contains("closeLease")
                || s.contains("forkCheckpoint")
        })
        .map(|s| s.as_str())
        .collect();
    assert!(
        critical_missing.is_empty(),
        "Critical mutating operations missing IdempotencyKey: {:?}",
        critical_missing
    );
}

// ── Authorization and policy-enforcement verification tests (vz-9gz) ──

// -- Scenario 1: Mutating endpoints require valid request bodies --

#[tokio::test]
async fn authz_sandbox_create_rejects_invalid_json() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .body(Body::from("not valid json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "invalid_request"
    );
    // Must include request_id in error envelope
    assert!(payload["error"]["request_id"].as_str().is_some());
}

#[tokio::test]
async fn authz_lease_create_rejects_invalid_json() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/leases")
                .header("content-type", "application/json")
                .body(Body::from("{malformed"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "invalid_request"
    );
}

#[tokio::test]
async fn authz_execution_create_rejects_invalid_json() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions")
                .header("content-type", "application/json")
                .body(Body::from("<<invalid>>"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "invalid_request"
    );
}

#[tokio::test]
async fn authz_execution_create_accepts_explicit_pty_mode() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config(state_path));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"container_id":"ctr-1","cmd":["echo","hi"],"pty_mode":"enabled"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn authz_execution_create_rejects_legacy_pty_field() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config(state_path));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"container_id":"ctr-1","cmd":["echo","hi"],"pty":true}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn authz_checkpoint_create_rejects_invalid_json() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/checkpoints")
                .header("content-type", "application/json")
                .body(Body::from("not-json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "invalid_request"
    );
}

#[tokio::test]
async fn authz_container_create_rejects_invalid_json() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/containers")
                .header("content-type", "application/json")
                .body(Body::from("{{bad"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "invalid_request"
    );
}

#[tokio::test]
async fn authz_build_start_rejects_invalid_json() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/builds")
                .header("content-type", "application/json")
                .body(Body::from("{bad-build-json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "invalid_request"
    );
}

#[tokio::test]
async fn authz_fork_checkpoint_rejects_invalid_json() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();

    // Create a ready checkpoint so we get past the 404 check.
    let mut ckpt = Checkpoint {
        checkpoint_id: "ckpt-fork-json".to_string(),
        sandbox_id: "sbx-1".to_string(),
        parent_checkpoint_id: None,
        class: CheckpointClass::FsQuick,
        state: CheckpointState::Creating,
        created_at: 1000,
        compatibility_fingerprint: "fp-1".to_string(),
    };
    ckpt.transition_to(CheckpointState::Ready).unwrap();
    store.save_checkpoint(&ckpt).unwrap();

    let app = router(test_config(state_path));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/checkpoints/ckpt-fork-json/fork")
                .header("content-type", "application/json")
                .body(Body::from("not json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "invalid_request"
    );
}

// -- Scenario 2: Sandbox ownership validation --

#[tokio::test]
async fn authz_sandbox_get_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/sandboxes/sbx-does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"]["code"].as_str().unwrap(), "not_found");
    // Error message should reference the sandbox ID but not leak internals.
    let msg = payload["error"]["message"].as_str().unwrap();
    assert!(msg.contains("sbx-does-not-exist"));
    assert!(
        !msg.contains("sqlite"),
        "error must not leak storage internals"
    );
    assert!(!msg.contains("SQL"), "error must not leak SQL details");
}

#[tokio::test]
async fn authz_sandbox_terminate_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/v1/sandboxes/sbx-phantom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"]["code"].as_str().unwrap(), "not_found");
}

#[tokio::test]
async fn authz_sandbox_operations_scoped_to_id() {
    let (app, _dir) = test_router();

    // Create a sandbox
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"cpus": 1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let create_body = to_bytes(create_resp.into_body(), usize::MAX).await.unwrap();
    let created: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    let real_id = created["sandbox"]["sandbox_id"].as_str().unwrap();

    // GET with the real ID succeeds
    let get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/sandboxes/{real_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);

    // GET with a tampered ID returns 404
    let tampered_id = format!("{real_id}-tampered");
    let tampered_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/sandboxes/{tampered_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tampered_resp.status(), StatusCode::NOT_FOUND);
}

// -- Scenario 3: Execution operations validate parent entity access --

#[tokio::test]
async fn authz_execution_get_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/executions/exec-nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"]["code"].as_str().unwrap(), "not_found");
}

#[tokio::test]
async fn authz_execution_cancel_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/v1/executions/exec-phantom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn authz_execution_resize_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions/exec-phantom/resize")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"cols":80,"rows":24}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn authz_execution_signal_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions/exec-phantom/signal")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"signal":"SIGTERM"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn authz_execution_stdin_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions/exec-phantom/stdin")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"data":"hello"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn authz_execution_stdin_without_live_backend_session_returns_501() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();
    let app = router(test_config(state_path));

    let created = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"container_id":"ctr-stdin","cmd":["echo","hi"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let created_body = to_bytes(created.into_body(), usize::MAX).await.unwrap();
    let created_payload: serde_json::Value = serde_json::from_slice(&created_body).unwrap();
    let execution_id = created_payload["execution"]["execution_id"]
        .as_str()
        .unwrap()
        .to_string();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/executions/{execution_id}/stdin"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"data":"hello"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "unsupported_operation"
    );
}

#[tokio::test]
async fn authz_execution_signal_on_terminal_returns_conflict() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();

    let mut execution = Execution {
        execution_id: "exec-done-sig".to_string(),
        container_id: "ctr-1".to_string(),
        exec_spec: ExecutionSpec {
            cmd: vec!["echo".to_string()],
            args: vec![],
            env_override: BTreeMap::new(),
            pty: false,
            timeout_secs: None,
        },
        state: ExecutionState::Running,
        exit_code: None,
        started_at: Some(now_epoch_secs()),
        ended_at: None,
    };
    let _ = execution.transition_to(ExecutionState::Exited);
    execution.exit_code = Some(0);
    execution.ended_at = Some(now_epoch_secs());
    store.save_execution(&execution).unwrap();

    let app = router(test_config(state_path));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions/exec-done-sig/signal")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"signal":"SIGKILL"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"]["code"].as_str().unwrap(), "invalid_state");
}

// -- Scenario 4: Lease entity validation --

#[tokio::test]
async fn authz_lease_get_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/leases/ls-phantom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn authz_lease_close_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/v1/leases/ls-phantom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn authz_lease_heartbeat_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/leases/ls-phantom/heartbeat")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn authz_lease_heartbeat_on_closed_lease_returns_422() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();

    let mut lease = Lease {
        lease_id: "ls-closed-hb".to_string(),
        sandbox_id: "sbx-1".to_string(),
        ttl_secs: 300,
        last_heartbeat_at: now_epoch_secs(),
        state: LeaseState::Opening,
    };
    lease.transition_to(LeaseState::Active).unwrap();
    lease.transition_to(LeaseState::Closed).unwrap();
    store.save_lease(&lease).unwrap();

    let app = router(test_config(state_path));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/leases/ls-closed-hb/heartbeat")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"]["code"].as_str().unwrap(), "invalid_state");
}

// -- Scenario 5: Checkpoint entity validation --

#[tokio::test]
async fn authz_checkpoint_get_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/checkpoints/ckpt-phantom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn authz_checkpoint_restore_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/checkpoints/ckpt-phantom/restore")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn authz_checkpoint_restore_not_ready_returns_409() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();

    let ckpt = Checkpoint {
        checkpoint_id: "ckpt-not-ready".to_string(),
        sandbox_id: "sbx-1".to_string(),
        parent_checkpoint_id: None,
        class: CheckpointClass::FsQuick,
        state: CheckpointState::Creating,
        created_at: 1000,
        compatibility_fingerprint: "fp-1".to_string(),
    };
    store.save_checkpoint(&ckpt).unwrap();

    let app = router(test_config(state_path));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/checkpoints/ckpt-not-ready/restore")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["code"].as_str().unwrap(),
        "checkpoint_not_ready"
    );
}

// -- Scenario 6: Container entity validation --

#[tokio::test]
async fn authz_container_get_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/containers/ctr-phantom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn authz_container_remove_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/v1/containers/ctr-phantom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// -- Scenario 7: Idempotency conflict detection (rate-limiting behavior) --

#[tokio::test]
async fn authz_idempotency_key_replay_returns_cached_response() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config(state_path));
    let body_bytes = r#"{"cpus": 2, "memory_mb": 512}"#;

    // First request with idempotency key
    let first_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .header("idempotency-key", "test-key-alpha")
                .body(Body::from(body_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_resp.status(), StatusCode::CREATED);

    let first_body = to_bytes(first_resp.into_body(), usize::MAX).await.unwrap();
    let first_payload: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
    let first_sandbox_id = first_payload["sandbox"]["sandbox_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Second request with the same key and same body returns cached response
    let second_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .header("idempotency-key", "test-key-alpha")
                .body(Body::from(body_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_resp.status(), StatusCode::CREATED);

    let second_body = to_bytes(second_resp.into_body(), usize::MAX).await.unwrap();
    let second_payload: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
    let second_sandbox_id = second_payload["sandbox"]["sandbox_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Same sandbox_id proves it is a replay, not a new creation.
    assert_eq!(first_sandbox_id, second_sandbox_id);
}

#[tokio::test]
async fn authz_idempotency_key_conflict_returns_409() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config(state_path));

    // First request with idempotency key
    let first_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .header("idempotency-key", "test-key-beta")
                .body(Body::from(r#"{"cpus": 1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_resp.status(), StatusCode::CREATED);

    // Second request with same key but DIFFERENT body triggers conflict
    let conflict_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .header("idempotency-key", "test-key-beta")
                .body(Body::from(r#"{"cpus": 4}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(conflict_resp.status(), StatusCode::CONFLICT);

    let conflict_body = to_bytes(conflict_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let conflict_payload: serde_json::Value = serde_json::from_slice(&conflict_body).unwrap();
    assert_eq!(
        conflict_payload["error"]["code"].as_str().unwrap(),
        "idempotency_conflict"
    );
}

#[tokio::test]
async fn authz_idempotency_key_on_lease_creation() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config(state_path));
    let body_bytes = r#"{"sandbox_id": "sbx-1"}"#;

    // First request
    let first_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/leases")
                .header("content-type", "application/json")
                .header("idempotency-key", "lease-key-1")
                .body(Body::from(body_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_resp.status(), StatusCode::CREATED);

    let first_body = to_bytes(first_resp.into_body(), usize::MAX).await.unwrap();
    let first_payload: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
    let first_lease_id = first_payload["lease"]["lease_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Same key + same body replays
    let second_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/leases")
                .header("content-type", "application/json")
                .header("idempotency-key", "lease-key-1")
                .body(Body::from(body_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_resp.status(), StatusCode::CREATED);

    let second_body = to_bytes(second_resp.into_body(), usize::MAX).await.unwrap();
    let second_payload: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
    assert_eq!(
        first_lease_id,
        second_payload["lease"]["lease_id"].as_str().unwrap()
    );

    // Different body with same key returns 409
    let conflict_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/leases")
                .header("content-type", "application/json")
                .header("idempotency-key", "lease-key-1")
                .body(Body::from(r#"{"sandbox_id": "sbx-2"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(conflict_resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn authz_idempotency_key_on_execution_creation() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config(state_path));
    let body_bytes = r#"{"container_id": "ctr-1", "cmd": ["echo", "hi"]}"#;

    // First request
    let first_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions")
                .header("content-type", "application/json")
                .header("idempotency-key", "exec-key-1")
                .body(Body::from(body_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_resp.status(), StatusCode::CREATED);

    // Conflict with different body
    let conflict_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/executions")
                .header("content-type", "application/json")
                .header("idempotency-key", "exec-key-1")
                .body(Body::from(
                    r#"{"container_id": "ctr-2", "cmd": ["echo", "bye"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(conflict_resp.status(), StatusCode::CONFLICT);
}

// -- Scenario 8: Error responses don't leak internal details --

#[tokio::test]
async fn authz_error_responses_use_consistent_envelope() {
    let (app, _dir) = test_router();

    // Collect error responses across implemented and pending surfaces.
    let endpoints = vec![
        (
            "/v1/sandboxes/sbx-leak-test",
            StatusCode::NOT_FOUND,
            "not_found",
        ),
        (
            "/v1/leases/ls-leak-test",
            StatusCode::NOT_FOUND,
            "not_found",
        ),
        (
            "/v1/executions/exec-leak-test",
            StatusCode::NOT_FOUND,
            "not_found",
        ),
        (
            "/v1/checkpoints/ckpt-leak-test",
            StatusCode::NOT_FOUND,
            "not_found",
        ),
        (
            "/v1/containers/ctr-leak-test",
            StatusCode::NOT_FOUND,
            "not_found",
        ),
        (
            "/v1/receipts/rcp-leak-test",
            StatusCode::NOT_FOUND,
            "not_found",
        ),
    ];

    for (uri, expected_status, expected_code) in &endpoints {
        let resp = app
            .clone()
            .oneshot(Request::builder().uri(*uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            *expected_status,
            "unexpected status for {uri}"
        );

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // All error envelopes must have exactly the same structure.
        assert!(
            payload["error"].is_object(),
            "missing error envelope for {uri}"
        );
        assert!(
            payload["error"]["code"].is_string(),
            "missing error.code for {uri}"
        );
        assert!(
            payload["error"]["message"].is_string(),
            "missing error.message for {uri}"
        );
        assert!(
            payload["error"]["request_id"].is_string(),
            "missing error.request_id for {uri}"
        );

        // Error messages must not leak implementation details.
        let msg = payload["error"]["message"].as_str().unwrap();
        let code = payload["error"]["code"].as_str().unwrap();
        assert_eq!(code, *expected_code, "unexpected error code for {uri}");
        assert!(
            !msg.contains("sqlite"),
            "error message leaks sqlite for {uri}: {msg}"
        );
        assert!(
            !msg.contains("SQL"),
            "error message leaks SQL for {uri}: {msg}"
        );
        assert!(
            !msg.contains("rusqlite"),
            "error message leaks rusqlite for {uri}: {msg}"
        );
        assert!(
            !msg.contains("table"),
            "error message leaks table name for {uri}: {msg}"
        );
        assert!(
            !msg.to_lowercase().contains("stack trace"),
            "error message leaks stack trace for {uri}: {msg}"
        );
        assert!(
            !msg.contains("panicked"),
            "error message leaks panic for {uri}: {msg}"
        );
    }
}

#[tokio::test]
async fn authz_bad_request_errors_do_not_leak_internals() {
    let (app, _dir) = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .body(Body::from("{invalid json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let msg = payload["error"]["message"].as_str().unwrap();

    // The error should describe the problem but not expose internal types.
    assert!(
        msg.contains("invalid JSON body"),
        "expected user-friendly JSON error prefix"
    );
    assert!(
        !msg.contains("serde_json::"),
        "error leaks serde_json module path"
    );
    // Note: serde line/column info (e.g. "at line 1 column 2") is acceptable
    // because it helps API consumers debug malformed request bodies.
    // What must NOT appear is internal stack traces or file paths.
    assert!(!msg.contains("src/"), "error leaks source file path");
}

// -- Scenario 9: X-Request-Id propagation --

#[tokio::test]
async fn authz_request_id_propagated_in_response() {
    let (app, _dir) = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/capabilities")
                .header("x-request-id", "custom-req-42")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["request_id"].as_str().unwrap(), "custom-req-42");
}

#[tokio::test]
async fn authz_request_id_propagated_in_error_responses() {
    let (app, _dir) = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/sandboxes/sbx-nonexistent")
                .header("x-request-id", "err-req-99")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["error"]["request_id"].as_str().unwrap(),
        "err-req-99"
    );
}

#[tokio::test]
async fn authz_request_id_generated_when_not_provided() {
    let (app, _dir) = test_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/capabilities")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let request_id = payload["request_id"].as_str().unwrap();
    assert!(
        request_id.starts_with("req_"),
        "auto-generated request_id should start with 'req_', got: {request_id}"
    );
}

// -- Scenario 10: Receipt generation for mutating operations --

#[tokio::test]
async fn authz_mutating_operations_generate_receipt_header() {
    let (app, _dir) = test_router();

    // Create a sandbox and verify receipt header is present.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sandboxes")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"cpus": 1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let receipt_id = response
        .headers()
        .get("x-receipt-id")
        .expect("mutating operation should return x-receipt-id header")
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        receipt_id.starts_with("rcp-"),
        "receipt_id should start with rcp-, got: {receipt_id}"
    );

    // Receipt query should return the persisted receipt by id.
    let receipt_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/receipts/{receipt_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(receipt_resp.status(), StatusCode::OK);
    let receipt_payload: serde_json::Value = serde_json::from_slice(
        &to_bytes(receipt_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        receipt_payload["receipt"]["receipt_id"].as_str().unwrap(),
        receipt_id
    );
}

// -- Scenario 11: Build entity validation --

#[tokio::test]
async fn authz_build_get_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/builds/bld-phantom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn authz_build_cancel_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/v1/builds/bld-phantom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// -- Scenario 12: Image entity validation --

#[tokio::test]
async fn authz_image_get_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/images/nonexistent:latest")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// -- Scenario 13: Receipt entity validation --

#[tokio::test]
async fn authz_receipt_get_nonexistent_returns_404() {
    let (app, _dir) = test_router();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/receipts/rcp-phantom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// ── Throughput and capacity tests (vz-lbg) ─────────────────────

/// Simulate multiple sequential create_sandbox calls and verify
/// that throughput is acceptable.
#[tokio::test]
async fn throughput_sequential_create_sandbox() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    StateStore::open(&state_path).unwrap();

    let app = router(test_config(state_path));

    let start = std::time::Instant::now();
    let request_count = 20;

    for i in 0..request_count {
        let body = serde_json::to_vec(&serde_json::json!({
            "stack_name": format!("stack-{i}"),
            "cpus": 2,
            "memory_mb": 512
        }))
        .unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sandboxes")
                    .header("Content-Type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let elapsed = start.elapsed();
    // 20 sequential creates should complete within 5 seconds on CI.
    assert!(
        elapsed.as_secs() < 5,
        "{request_count} sequential create_sandbox calls took {elapsed:?} (>5s budget)"
    );
}

/// List operations with a large result set should remain performant.
#[tokio::test]
async fn throughput_list_sandboxes_large_result_set() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();

    // Pre-populate 50 sandboxes directly via StateStore.
    for i in 0..50 {
        let now = i as u64 + 1_700_000_000;
        let mut labels = std::collections::BTreeMap::new();
        labels.insert("stack_name".to_string(), format!("stack-{i}"));
        let sandbox = Sandbox {
            sandbox_id: format!("sbx-{i:04}"),
            backend: SandboxBackend::MacosVz,
            spec: SandboxSpec {
                cpus: Some(2),
                memory_mb: Some(512),
                ..SandboxSpec::default()
            },
            state: SandboxState::Ready,
            created_at: now,
            updated_at: now,
            labels,
        };
        store.save_sandbox(&sandbox).unwrap();
    }
    drop(store);

    let app = router(test_config(state_path));

    let start = std::time::Instant::now();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/sandboxes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let sandboxes = payload["sandboxes"].as_array().unwrap();
    assert_eq!(sandboxes.len(), 50);

    // Listing 50 sandboxes should complete well under 2 seconds.
    assert!(
        elapsed.as_secs() < 2,
        "list_sandboxes with 50 entries took {elapsed:?} (>2s budget)"
    );
}

/// Events endpoint with a large event history should paginate
/// without performance degradation.
#[tokio::test]
async fn throughput_events_large_history() {
    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();

    // Pre-populate 1,000 events.
    for i in 0..1_000 {
        store
            .emit_event(
                "perf-stack",
                &StackEvent::ServiceCreating {
                    stack_name: "perf-stack".to_string(),
                    service_name: format!("svc-{i}"),
                },
            )
            .unwrap();
    }
    drop(store);

    let app = router(test_config(state_path));

    // Query page from midpoint.
    let start = std::time::Instant::now();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/perf-stack?after=500&limit=100")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let events = payload["events"].as_array().unwrap();
    assert_eq!(events.len(), 100);

    // Paginated query against 1,000 events should complete in under 1 second.
    assert!(
        elapsed.as_secs() < 1,
        "events pagination (100 of 1000) took {elapsed:?} (>1s budget)"
    );
}

/// List leases endpoint should handle a large number of leases.
#[tokio::test]
async fn throughput_list_leases_large_set() {
    use vz_runtime_contract::Lease;

    let temp_dir = tempdir().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let store = StateStore::open(&state_path).unwrap();

    // Pre-populate a sandbox and 30 leases.
    let mut labels = std::collections::BTreeMap::new();
    labels.insert("stack_name".to_string(), "lease-perf".to_string());
    let sandbox = Sandbox {
        sandbox_id: "sbx-lease-perf".to_string(),
        backend: SandboxBackend::MacosVz,
        spec: SandboxSpec::default(),
        state: SandboxState::Ready,
        created_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        labels,
    };
    store.save_sandbox(&sandbox).unwrap();

    for i in 0..30 {
        let lease = Lease {
            lease_id: format!("lse-{i:04}"),
            sandbox_id: "sbx-lease-perf".to_string(),
            ttl_secs: 300,
            last_heartbeat_at: 1_700_000_000 + i,
            state: vz_runtime_contract::LeaseState::Active,
        };
        store.save_lease(&lease).unwrap();
    }
    drop(store);

    let app = router(test_config(state_path));

    let start = std::time::Instant::now();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/leases")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let leases = payload["leases"].as_array().unwrap();
    assert_eq!(leases.len(), 30);

    assert!(
        elapsed.as_secs() < 2,
        "list_leases with 30 entries took {elapsed:?} (>2s budget)"
    );
}
