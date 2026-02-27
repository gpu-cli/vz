#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use vz_api::{ApiConfig, router};
use vz_runtime_contract::RuntimeCapabilities;
use vz_runtimed::{RuntimeDaemon, RuntimedConfig, serve_runtime_uds_with_shutdown};

async fn wait_for_socket(path: &Path) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if path.exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("daemon socket not created in time: {}", path.display());
}

async fn start_daemon_for_state_store(
    state_store_path: &Path,
) -> Result<(
    Arc<tokio::sync::Notify>,
    tokio::task::JoinHandle<Result<(), vz_runtimed::RuntimedServerError>>,
)> {
    let runtime_data_dir = state_store_path
        .parent()
        .context("state store path must have a parent directory")?
        .join(".vz-runtime");
    tokio::fs::create_dir_all(&runtime_data_dir)
        .await
        .context("create runtime data dir")?;
    let socket_path = runtime_data_dir.join("runtimed.sock");

    let daemon = Arc::new(
        RuntimeDaemon::start(RuntimedConfig {
            state_store_path: state_store_path.to_path_buf(),
            runtime_data_dir: runtime_data_dir.clone(),
            socket_path: socket_path.clone(),
        })
        .context("start vz-runtimed daemon")?,
    );

    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });

    wait_for_socket(daemon.socket_path()).await?;
    Ok((shutdown, server))
}

#[tokio::test]
async fn runtime_api_server_smoke_serves_capabilities() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("create temp dir")?;
    let state_store_path = temp_dir.path().join("state.db");
    let app = router(ApiConfig {
        state_store_path,
        daemon_socket_path: None,
        daemon_runtime_data_dir: None,
        daemon_auto_spawn: true,
        capabilities: RuntimeCapabilities {
            fs_quick_checkpoint: true,
            ..RuntimeCapabilities::default()
        },
        event_poll_interval: Duration::from_millis(10),
        default_event_page_size: 10,
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind ephemeral API listener")?;
    let address = listener.local_addr().context("resolve listener address")?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let response = reqwest::get(format!("http://{address}/v1/capabilities"))
        .await
        .context("request capabilities endpoint")?;
    anyhow::ensure!(
        response.status() == reqwest::StatusCode::OK,
        "expected 200 OK, received {}",
        response.status()
    );

    let response_body = response
        .bytes()
        .await
        .context("read capabilities response body")?;
    let payload: serde_json::Value =
        serde_json::from_slice(&response_body).context("decode capabilities response body")?;
    let capabilities = payload
        .get("capabilities")
        .and_then(serde_json::Value::as_array)
        .context("capabilities field should be an array")?;
    anyhow::ensure!(
        capabilities
            .iter()
            .any(|entry| entry == &serde_json::Value::String("fs_quick_checkpoint".to_string())),
        "fs_quick_checkpoint capability was not returned"
    );

    let _ = shutdown_tx.send(());
    server
        .await
        .context("join API server task")?
        .context("run API server")?;
    Ok(())
}

#[tokio::test]
async fn sandbox_crud_over_http() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("create temp dir")?;
    let state_store_path = temp_dir.path().join("state.db");
    let (daemon_shutdown, daemon_server) = start_daemon_for_state_store(&state_store_path).await?;
    let app = router(ApiConfig {
        state_store_path,
        daemon_socket_path: None,
        daemon_runtime_data_dir: None,
        daemon_auto_spawn: true,
        capabilities: RuntimeCapabilities::default(),
        event_poll_interval: Duration::from_millis(10),
        default_event_page_size: 10,
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind ephemeral API listener")?;
    let address = listener.local_addr().context("resolve listener address")?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client = reqwest::Client::new();
    let base = format!("http://{address}");

    // POST /v1/sandboxes -> 201
    let create_response = client
        .post(format!("{base}/v1/sandboxes"))
        .header("content-type", "application/json")
        .body(r#"{"cpus":2,"memory_mb":512}"#)
        .send()
        .await
        .context("POST /v1/sandboxes")?;
    anyhow::ensure!(
        create_response.status() == reqwest::StatusCode::CREATED,
        "expected 201, got {}",
        create_response.status()
    );

    let create_body_bytes = create_response
        .bytes()
        .await
        .context("read create response body")?;
    let create_body: serde_json::Value =
        serde_json::from_slice(&create_body_bytes).context("decode create")?;
    let sandbox_id = create_body["sandbox"]["sandbox_id"]
        .as_str()
        .context("sandbox_id")?
        .to_string();
    anyhow::ensure!(sandbox_id.starts_with("sbx-"), "id must start with sbx-");

    // GET /v1/sandboxes -> list includes the created sandbox
    let list_response = client
        .get(format!("{base}/v1/sandboxes"))
        .send()
        .await
        .context("GET /v1/sandboxes")?;
    anyhow::ensure!(list_response.status() == reqwest::StatusCode::OK);
    let list_body_bytes = list_response
        .bytes()
        .await
        .context("read list response body")?;
    let list_body: serde_json::Value =
        serde_json::from_slice(&list_body_bytes).context("decode list")?;
    let sandboxes = list_body["sandboxes"]
        .as_array()
        .context("sandboxes array")?;
    anyhow::ensure!(sandboxes.len() == 1, "expected 1 sandbox in list");

    // GET /v1/sandboxes/{id} -> 200
    let get_response = client
        .get(format!("{base}/v1/sandboxes/{sandbox_id}"))
        .send()
        .await
        .context("GET by id")?;
    anyhow::ensure!(get_response.status() == reqwest::StatusCode::OK);
    let get_body_bytes = get_response
        .bytes()
        .await
        .context("read get response body")?;
    let get_body: serde_json::Value =
        serde_json::from_slice(&get_body_bytes).context("decode get")?;
    anyhow::ensure!(
        get_body["sandbox"]["sandbox_id"].as_str() == Some(sandbox_id.as_str()),
        "sandbox id mismatch"
    );

    // DELETE /v1/sandboxes/{id} -> terminated
    let delete_response = client
        .delete(format!("{base}/v1/sandboxes/{sandbox_id}"))
        .send()
        .await
        .context("DELETE sandbox")?;
    anyhow::ensure!(delete_response.status() == reqwest::StatusCode::OK);
    let delete_body_bytes = delete_response
        .bytes()
        .await
        .context("read delete response body")?;
    let delete_body: serde_json::Value =
        serde_json::from_slice(&delete_body_bytes).context("decode delete")?;
    let state = delete_body["sandbox"]["state"].as_str().context("state")?;
    anyhow::ensure!(
        state == "failed" || state == "terminated",
        "expected terminal state, got {state}"
    );

    let _ = shutdown_tx.send(());
    server
        .await
        .context("join API server task")?
        .context("run API server")?;
    daemon_shutdown.notify_waiters();
    daemon_server
        .await
        .context("join daemon server task")?
        .context("run daemon server")?;
    Ok(())
}

#[tokio::test]
async fn file_service_round_trip_over_http() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("create temp dir")?;
    let state_store_path = temp_dir.path().join("state.db");
    let (daemon_shutdown, daemon_server) = start_daemon_for_state_store(&state_store_path).await?;
    let app = router(ApiConfig {
        state_store_path,
        daemon_socket_path: None,
        daemon_runtime_data_dir: None,
        daemon_auto_spawn: true,
        capabilities: RuntimeCapabilities::default(),
        event_poll_interval: Duration::from_millis(10),
        default_event_page_size: 10,
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind ephemeral API listener")?;
    let address = listener.local_addr().context("resolve listener address")?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client = reqwest::Client::new();
    let base = format!("http://{address}");

    let create_response = client
        .post(format!("{base}/v1/sandboxes"))
        .header("content-type", "application/json")
        .body(r#"{"cpus":2,"memory_mb":512}"#)
        .send()
        .await
        .context("POST /v1/sandboxes")?;
    anyhow::ensure!(
        create_response.status() == reqwest::StatusCode::CREATED,
        "expected 201, got {}",
        create_response.status()
    );
    let create_body: serde_json::Value = create_response
        .json()
        .await
        .context("decode sandbox create response")?;
    let sandbox_id = create_body["sandbox"]["sandbox_id"]
        .as_str()
        .context("missing sandbox_id")?
        .to_string();

    let file_contents_base64 = "aGVsbG8gdnoK";
    let write_response = client
        .post(format!("{base}/v1/files/write"))
        .header("content-type", "application/json")
        .body(format!(
            r#"{{"sandbox_id":"{sandbox_id}","path":"hello.txt","data_base64":"{file_contents_base64}","create_parents":true}}"#
        ))
        .send()
        .await
        .context("POST /v1/files/write")?;
    anyhow::ensure!(
        write_response.status() == reqwest::StatusCode::OK,
        "expected 200, got {}",
        write_response.status()
    );
    anyhow::ensure!(
        write_response.headers().contains_key("x-receipt-id"),
        "file write response missing x-receipt-id"
    );

    let read_response = client
        .post(format!("{base}/v1/files/read"))
        .header("content-type", "application/json")
        .body(format!(
            r#"{{"sandbox_id":"{sandbox_id}","path":"hello.txt"}}"#
        ))
        .send()
        .await
        .context("POST /v1/files/read")?;
    anyhow::ensure!(
        read_response.status() == reqwest::StatusCode::OK,
        "expected 200, got {}",
        read_response.status()
    );
    let read_body: serde_json::Value =
        read_response.json().await.context("decode read response")?;
    anyhow::ensure!(
        read_body["data_base64"].as_str() == Some(file_contents_base64),
        "unexpected file read payload"
    );
    anyhow::ensure!(
        read_body["truncated"].as_bool() == Some(false),
        "expected non-truncated read"
    );

    let list_response = client
        .post(format!("{base}/v1/files/list"))
        .header("content-type", "application/json")
        .body(format!(
            r#"{{"sandbox_id":"{sandbox_id}","path":"","recursive":true}}"#
        ))
        .send()
        .await
        .context("POST /v1/files/list")?;
    anyhow::ensure!(
        list_response.status() == reqwest::StatusCode::OK,
        "expected 200, got {}",
        list_response.status()
    );
    let list_body: serde_json::Value =
        list_response.json().await.context("decode list response")?;
    let entries = list_body["entries"]
        .as_array()
        .context("entries array missing")?;
    anyhow::ensure!(
        entries
            .iter()
            .any(|entry| entry["path"].as_str() == Some("hello.txt")),
        "expected hello.txt in list response"
    );

    let _ = shutdown_tx.send(());
    server
        .await
        .context("join API server task")?
        .context("run API server")?;
    daemon_shutdown.notify_waiters();
    daemon_server
        .await
        .context("join daemon server task")?
        .context("run daemon server")?;
    Ok(())
}
