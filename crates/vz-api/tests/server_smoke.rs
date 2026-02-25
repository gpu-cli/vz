#![allow(clippy::unwrap_used)]

use std::time::Duration;

use anyhow::{Context, Result};
use vz_api::{ApiConfig, router};
use vz_runtime_contract::RuntimeCapabilities;

#[tokio::test]
async fn runtime_api_server_smoke_serves_capabilities() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("create temp dir")?;
    let state_store_path = temp_dir.path().join("state.db");
    let app = router(ApiConfig {
        state_store_path,
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
    let app = router(ApiConfig {
        state_store_path,
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
        .json(&serde_json::json!({"cpus": 2, "memory_mb": 512}))
        .send()
        .await
        .context("POST /v1/sandboxes")?;
    anyhow::ensure!(
        create_response.status() == reqwest::StatusCode::CREATED,
        "expected 201, got {}",
        create_response.status()
    );

    let create_body: serde_json::Value = create_response.json().await.context("decode create")?;
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
    let list_body: serde_json::Value = list_response.json().await.context("decode list")?;
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
    let get_body: serde_json::Value = get_response.json().await.context("decode get")?;
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
    let delete_body: serde_json::Value = delete_response.json().await.context("decode delete")?;
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
    Ok(())
}
