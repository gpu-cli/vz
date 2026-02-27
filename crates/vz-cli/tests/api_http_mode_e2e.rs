#![allow(clippy::unwrap_used)]

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router as AxumRouter,
    extract::{Json as ExtractJson, Path as AxumPath},
    http::StatusCode,
    routing::{get, post},
};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use serde::{Deserialize, Serialize};
use vz_api::{ApiConfig, router};
use vz_runtime_contract::RuntimeCapabilities;
use vz_runtimed::{RuntimeDaemon, RuntimedConfig, serve_runtime_uds_with_shutdown};

#[derive(Debug, Serialize)]
struct CreateSandboxRequest {
    cpus: u8,
    memory_mb: u64,
}

#[derive(Debug, Deserialize)]
struct ApiCreateSandboxResponse {
    sandbox: ApiSandboxPayload,
}

#[derive(Debug, Deserialize)]
struct ApiSandboxPayload {
    sandbox_id: String,
}

#[cfg(target_os = "macos")]
fn has_virtualization_entitlement() -> bool {
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
    entitlements.contains("com.apple.security.virtualization")
}

#[cfg(not(target_os = "macos"))]
fn has_virtualization_entitlement() -> bool {
    true
}

fn require_virtualization_entitlement() -> bool {
    if has_virtualization_entitlement() {
        return true;
    }

    eprintln!(
        "skipping api_http_mode_e2e: test binary is missing com.apple.security.virtualization entitlement"
    );
    false
}

async fn wait_for_socket(path: &Path) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if path.exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    bail!("daemon socket not created in time: {}", path.display());
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

async fn start_api_server(
    state_store_path: PathBuf,
) -> Result<(
    String,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<std::io::Result<()>>,
)> {
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
        .context("bind API listener")?;
    let address = listener
        .local_addr()
        .context("resolve API listener address")?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    Ok((format!("http://{address}"), shutdown_tx, server))
}

async fn create_sandbox_via_api(api_base_url: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{api_base_url}/v1/sandboxes"))
        .json(&CreateSandboxRequest {
            cpus: 2,
            memory_mb: 512,
        })
        .send()
        .await
        .context("POST /v1/sandboxes")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("sandbox create failed with status {status}: {body}");
    }
    let payload: ApiCreateSandboxResponse = response
        .json()
        .await
        .context("decode sandbox create response")?;
    if payload.sandbox.sandbox_id.trim().is_empty() {
        bail!("sandbox create response returned empty sandbox_id");
    }
    Ok(payload.sandbox.sandbox_id)
}

fn resolve_vz_binary() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_vz") {
        return Ok(PathBuf::from(path));
    }

    let current_exe = std::env::current_exe().context("resolve current test binary path")?;
    if let Some(target_debug_dir) = current_exe.parent().and_then(|path| path.parent()) {
        let candidate = target_debug_dir.join("vz");
        if candidate.exists() {
            return Ok(candidate);
        }
        #[cfg(windows)]
        {
            let candidate_exe = target_debug_dir.join("vz.exe");
            if candidate_exe.exists() {
                return Ok(candidate_exe);
            }
        }
    }

    bail!("resolve vz binary")
}

fn run_vz_command_blocking(
    vz_bin: PathBuf,
    api_base_url: String,
    home_dir: PathBuf,
    args: Vec<String>,
) -> Result<Output> {
    let mut child = Command::new(&vz_bin)
        .args(&args)
        .env("VZ_CONTROL_PLANE_TRANSPORT", "api-http")
        .env("VZ_RUNTIME_API_BASE_URL", &api_base_url)
        .env("HOME", &home_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn vz command: {:?}", args))?;

    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().context("poll vz command status")? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("timed out running vz command: {:?}", args);
        }
        std::thread::sleep(Duration::from_millis(25));
    };

    let mut stdout = Vec::new();
    if let Some(mut stream) = child.stdout.take() {
        stream
            .read_to_end(&mut stdout)
            .context("read vz command stdout")?;
    }

    let mut stderr = Vec::new();
    if let Some(mut stream) = child.stderr.take() {
        stream
            .read_to_end(&mut stderr)
            .context("read vz command stderr")?;
    }

    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

async fn run_vz_command(
    vz_bin: &Path,
    api_base_url: &str,
    home_dir: &Path,
    args: &[&str],
) -> Result<Output> {
    let vz_bin = vz_bin.to_path_buf();
    let api_base_url = api_base_url.to_string();
    let home_dir = home_dir.to_path_buf();
    let args: Vec<String> = args.iter().map(|value| (*value).to_string()).collect();

    tokio::task::spawn_blocking(move || {
        run_vz_command_blocking(vz_bin, api_base_url, home_dir, args)
    })
    .await
    .context("join vz command task")?
}

fn transcript_contains(transcript: &Arc<Mutex<Vec<u8>>>, needle: &str) -> bool {
    match transcript.lock() {
        Ok(buffer) => String::from_utf8_lossy(&buffer).contains(needle),
        Err(_) => false,
    }
}

fn transcript_string(transcript: &Arc<Mutex<Vec<u8>>>) -> String {
    match transcript.lock() {
        Ok(buffer) => String::from_utf8_lossy(&buffer).to_string(),
        Err(_) => "<poisoned transcript>".to_string(),
    }
}

fn wait_for_transcript_contains(
    transcript: &Arc<Mutex<Vec<u8>>>,
    needle: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if transcript_contains(transcript, needle) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    bail!("timed out waiting for transcript marker: {needle}");
}

fn wait_for_pty_exit(
    child: &mut Box<dyn portable_pty::Child + Send + Sync>,
    timeout: Duration,
) -> Result<portable_pty::ExitStatus> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().context("poll pty child status")? {
            return Ok(status);
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    let _ = child.kill();
    bail!("timed out waiting for attach process to exit");
}

fn run_attach_detach_flow(
    vz_bin: &Path,
    api_base_url: &str,
    home_dir: &Path,
    sandbox_id: &str,
) -> Result<String> {
    let pty_system = native_pty_system();
    let pair = pty_system
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .context("open pty for detach flow")?;

    let mut command = CommandBuilder::new(vz_bin.to_string_lossy().into_owned());
    command.arg("attach");
    command.arg(sandbox_id);
    command.env("VZ_CONTROL_PLANE_TRANSPORT", "api-http");
    command.env("VZ_RUNTIME_API_BASE_URL", api_base_url);
    command.env("HOME", home_dir);

    let mut child = pair
        .slave
        .spawn_command(command)
        .context("spawn attach command (detach flow)")?;
    drop(pair.slave);

    let transcript = Arc::new(Mutex::new(Vec::new()));
    let transcript_clone = Arc::clone(&transcript);
    let mut reader = pair.master.try_clone_reader().context("clone pty reader")?;
    let reader_handle = std::thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    if let Ok(mut data) = transcript_clone.lock() {
                        data.extend_from_slice(&buffer[..count]);
                    } else {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let mut writer = pair.master.take_writer().context("open pty writer")?;
    let token = "__VZ_ATTACH_STREAM_OK__";
    writer
        .write_all(format!("echo {token}\n").as_bytes())
        .context("write attach stream command")?;
    writer.flush().context("flush attach stream command")?;
    wait_for_transcript_contains(&transcript, token, Duration::from_secs(10))?;

    writer
        .write_all(&[0x10])
        .context("send detach prefix Ctrl-P")?;
    writer.flush().context("flush detach prefix")?;
    std::thread::sleep(Duration::from_millis(150));
    writer
        .write_all(&[0x11])
        .context("send detach confirm Ctrl-Q")?;
    writer.flush().context("flush detach confirm")?;
    drop(writer);

    let status = wait_for_pty_exit(&mut child, Duration::from_secs(10))?;
    if !status.success() {
        bail!(
            "detach attach flow should exit successfully, got status {}",
            status.exit_code()
        );
    }

    drop(pair.master);
    reader_handle
        .join()
        .map_err(|_| anyhow::anyhow!("failed to join detach flow reader thread"))?;

    Ok(transcript_string(&transcript))
}

fn run_attach_non_zero_exit_flow(
    vz_bin: &Path,
    api_base_url: &str,
    home_dir: &Path,
    sandbox_id: &str,
) -> Result<(portable_pty::ExitStatus, String)> {
    let pty_system = native_pty_system();
    let pair = pty_system
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .context("open pty for non-zero flow")?;

    let mut command = CommandBuilder::new(vz_bin.to_string_lossy().into_owned());
    command.arg("attach");
    command.arg(sandbox_id);
    command.env("VZ_CONTROL_PLANE_TRANSPORT", "api-http");
    command.env("VZ_RUNTIME_API_BASE_URL", api_base_url);
    command.env("HOME", home_dir);

    let mut child = pair
        .slave
        .spawn_command(command)
        .context("spawn attach command (non-zero flow)")?;
    drop(pair.slave);

    let transcript = Arc::new(Mutex::new(Vec::new()));
    let transcript_clone = Arc::clone(&transcript);
    let mut reader = pair.master.try_clone_reader().context("clone pty reader")?;
    let reader_handle = std::thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    if let Ok(mut data) = transcript_clone.lock() {
                        data.extend_from_slice(&buffer[..count]);
                    } else {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let mut writer = pair.master.take_writer().context("open pty writer")?;
    writer
        .write_all(b"exit 7\n")
        .context("write non-zero exit command")?;
    writer.flush().context("flush non-zero exit command")?;
    drop(writer);

    let status = wait_for_pty_exit(&mut child, Duration::from_secs(10))?;
    drop(pair.master);
    reader_handle
        .join()
        .map_err(|_| anyhow::anyhow!("failed to join non-zero flow reader thread"))?;

    Ok((status, transcript_string(&transcript)))
}

#[tokio::test]
async fn cli_api_http_mode_end_to_end_sandbox_and_attach_flow() -> Result<()> {
    if !require_virtualization_entitlement() {
        return Ok(());
    }

    let temp_dir = tempfile::tempdir().context("create temp dir")?;
    let home_dir = temp_dir.path().join("home");
    std::fs::create_dir_all(&home_dir).context("create isolated HOME directory")?;
    let state_store_path = temp_dir.path().join("state.db");

    let (daemon_shutdown, daemon_server) = start_daemon_for_state_store(&state_store_path).await?;
    let (api_base_url, api_shutdown_tx, api_server) =
        start_api_server(state_store_path.clone()).await?;

    let sandbox_id = create_sandbox_via_api(&api_base_url).await?;
    let vz_bin = resolve_vz_binary()?;

    let image_ls_output =
        run_vz_command(&vz_bin, &api_base_url, &home_dir, &["image", "ls"]).await?;
    if !image_ls_output.status.success() {
        bail!(
            "vz image ls failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&image_ls_output.stdout),
            String::from_utf8_lossy(&image_ls_output.stderr)
        );
    }
    let image_ls_stdout = String::from_utf8_lossy(&image_ls_output.stdout);
    if !image_ls_stdout.contains("No images found.") && !image_ls_stdout.contains("DIGEST") {
        bail!(
            "vz image ls output missing expected marker:\n{}",
            image_ls_stdout
        );
    }

    let image_prune_output =
        run_vz_command(&vz_bin, &api_base_url, &home_dir, &["image", "prune"]).await?;
    if !image_prune_output.status.success() {
        bail!(
            "vz image prune failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&image_prune_output.stdout),
            String::from_utf8_lossy(&image_prune_output.stderr)
        );
    }
    let image_prune_stdout = String::from_utf8_lossy(&image_prune_output.stdout);
    if !image_prune_stdout.contains("Pruned images:") {
        bail!(
            "vz image prune output missing expected marker:\n{}",
            image_prune_stdout
        );
    }

    let ls_output = run_vz_command(&vz_bin, &api_base_url, &home_dir, &["ls", "--json"]).await?;
    if !ls_output.status.success() {
        bail!(
            "vz ls failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&ls_output.stdout),
            String::from_utf8_lossy(&ls_output.stderr)
        );
    }
    let listed: serde_json::Value =
        serde_json::from_slice(&ls_output.stdout).context("decode ls output")?;
    let listed_items = listed.as_array().context("ls output should be an array")?;
    if !listed_items.iter().any(|item| {
        item.get("sandbox_id").and_then(serde_json::Value::as_str) == Some(sandbox_id.as_str())
    }) {
        bail!("vz ls output did not contain created sandbox {sandbox_id}");
    }

    let inspect_output =
        run_vz_command(&vz_bin, &api_base_url, &home_dir, &["inspect", &sandbox_id]).await?;
    if !inspect_output.status.success() {
        bail!(
            "vz inspect failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&inspect_output.stdout),
            String::from_utf8_lossy(&inspect_output.stderr)
        );
    }
    let inspected: serde_json::Value =
        serde_json::from_slice(&inspect_output.stdout).context("decode inspect output")?;
    if inspected
        .get("sandbox_id")
        .and_then(serde_json::Value::as_str)
        != Some(sandbox_id.as_str())
    {
        bail!("inspect output sandbox_id mismatch");
    }

    let detach_transcript = run_attach_detach_flow(&vz_bin, &api_base_url, &home_dir, &sandbox_id)?;
    if !detach_transcript.contains("Detached (Ctrl-P Ctrl-Q). Session remains active.") {
        bail!(
            "detach transcript missing detach confirmation:\n{}",
            detach_transcript
        );
    }

    let close_output = run_vz_command(
        &vz_bin,
        &api_base_url,
        &home_dir,
        &["close-shell", &sandbox_id],
    )
    .await?;
    if !close_output.status.success() {
        bail!(
            "vz close-shell failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&close_output.stdout),
            String::from_utf8_lossy(&close_output.stderr)
        );
    }

    let (non_zero_status, non_zero_transcript) =
        run_attach_non_zero_exit_flow(&vz_bin, &api_base_url, &home_dir, &sandbox_id)?;
    if non_zero_status.success() {
        bail!("non-zero attach flow unexpectedly succeeded");
    }
    if !non_zero_transcript.contains("sandbox shell exited with status 7") {
        bail!(
            "non-zero attach transcript missing propagated exit status:\n{}",
            non_zero_transcript
        );
    }

    let rm_output = run_vz_command(&vz_bin, &api_base_url, &home_dir, &["rm", &sandbox_id]).await?;
    if !rm_output.status.success() {
        bail!(
            "vz rm failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&rm_output.stdout),
            String::from_utf8_lossy(&rm_output.stderr)
        );
    }

    let _ = api_shutdown_tx.send(());
    api_server
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
async fn cli_api_http_mode_image_commands_work_against_stub_api_without_daemon() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("create temp dir")?;
    let home_dir = temp_dir.path().join("home");
    std::fs::create_dir_all(&home_dir).context("create isolated HOME directory")?;

    let list_calls = Arc::new(AtomicUsize::new(0));
    let pull_calls = Arc::new(AtomicUsize::new(0));
    let prune_calls = Arc::new(AtomicUsize::new(0));

    let list_calls_clone = Arc::clone(&list_calls);
    let pull_calls_clone = Arc::clone(&pull_calls);
    let prune_calls_clone = Arc::clone(&prune_calls);

    let app = AxumRouter::new()
        .route(
            "/v1/images",
            get(move || {
                let list_calls = Arc::clone(&list_calls_clone);
                async move {
                    list_calls.fetch_add(1, Ordering::SeqCst);
                    Json(serde_json::json!({
                        "request_id": "req-list",
                        "images": [{
                            "image_ref": "alpine:3.20",
                            "resolved_digest": "sha256:abc123",
                            "platform": "linux/arm64",
                            "source_registry": "docker.io",
                            "pulled_at": 1730000000u64
                        }]
                    }))
                }
            }),
        )
        .route(
            "/v1/images/pull",
            post(
                move |ExtractJson(payload): ExtractJson<serde_json::Value>| {
                    let pull_calls = Arc::clone(&pull_calls_clone);
                    async move {
                        pull_calls.fetch_add(1, Ordering::SeqCst);
                        if payload["image_ref"].as_str() != Some("alpine:3.20") {
                            return Json(serde_json::json!({
                                "error": {
                                    "code": "invalid_request",
                                    "message": "unexpected image_ref",
                                    "request_id": "req-pull-err"
                                }
                            }));
                        }
                        Json(serde_json::json!({
                            "request_id": "req-pull",
                            "image": {
                                "image_ref": "alpine:3.20",
                                "resolved_digest": "sha256:abc123",
                                "platform": "linux/arm64",
                                "source_registry": "docker.io",
                                "pulled_at": 1730000000u64
                            },
                            "receipt_id": "rcp-pull-1"
                        }))
                    }
                },
            ),
        )
        .route(
            "/v1/images/prune",
            post(move || {
                let prune_calls = Arc::clone(&prune_calls_clone);
                async move {
                    prune_calls.fetch_add(1, Ordering::SeqCst);
                    Json(serde_json::json!({
                        "request_id": "req-prune",
                        "removed_refs": 1u64,
                        "removed_manifests": 1u64,
                        "removed_configs": 1u64,
                        "removed_layer_dirs": 1u64,
                        "remaining_images": 0u64,
                        "receipt_id": "rcp-prune-1"
                    }))
                }
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind stub API listener")?;
    let address = listener
        .local_addr()
        .context("resolve stub API listener address")?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let api_base_url = format!("http://{address}");
    let vz_bin = resolve_vz_binary()?;

    let pull_output = run_vz_command(
        &vz_bin,
        &api_base_url,
        &home_dir,
        &["image", "pull", "alpine:3.20"],
    )
    .await?;
    if !pull_output.status.success() {
        bail!(
            "vz image pull failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&pull_output.stdout),
            String::from_utf8_lossy(&pull_output.stderr)
        );
    }
    let pull_stdout = String::from_utf8_lossy(&pull_output.stdout);
    if !pull_stdout.contains("Pulled alpine:3.20 as sha256:abc123") {
        bail!(
            "vz image pull output missing expected marker:\n{}",
            pull_stdout
        );
    }

    let ls_output = run_vz_command(&vz_bin, &api_base_url, &home_dir, &["image", "ls"]).await?;
    if !ls_output.status.success() {
        bail!(
            "vz image ls failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&ls_output.stdout),
            String::from_utf8_lossy(&ls_output.stderr)
        );
    }
    let ls_stdout = String::from_utf8_lossy(&ls_output.stdout);
    if !ls_stdout.contains("alpine:3.20") {
        bail!(
            "vz image ls output missing expected image reference:\n{}",
            ls_stdout
        );
    }

    let prune_output =
        run_vz_command(&vz_bin, &api_base_url, &home_dir, &["image", "prune"]).await?;
    if !prune_output.status.success() {
        bail!(
            "vz image prune failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&prune_output.stdout),
            String::from_utf8_lossy(&prune_output.stderr)
        );
    }
    let prune_stdout = String::from_utf8_lossy(&prune_output.stdout);
    if !prune_stdout.contains("Pruned images: refs=1") {
        bail!(
            "vz image prune output missing expected marker:\n{}",
            prune_stdout
        );
    }

    assert_eq!(
        pull_calls.load(Ordering::SeqCst),
        1,
        "pull endpoint should be called exactly once"
    );
    assert_eq!(
        list_calls.load(Ordering::SeqCst),
        1,
        "list endpoint should be called exactly once"
    );
    assert_eq!(
        prune_calls.load(Ordering::SeqCst),
        1,
        "prune endpoint should be called exactly once"
    );

    let _ = shutdown_tx.send(());
    server
        .await
        .context("join stub API server task")?
        .context("run stub API server")?;

    Ok(())
}

#[tokio::test]
async fn cli_api_http_mode_checkpoint_commands_work_against_stub_api_without_daemon() -> Result<()>
{
    let temp_dir = tempfile::tempdir().context("create temp dir")?;
    let home_dir = temp_dir.path().join("home");
    std::fs::create_dir_all(&home_dir).context("create isolated HOME directory")?;

    let list_calls = Arc::new(AtomicUsize::new(0));
    let create_calls = Arc::new(AtomicUsize::new(0));
    let inspect_calls = Arc::new(AtomicUsize::new(0));

    let list_calls_clone = Arc::clone(&list_calls);
    let create_calls_clone = Arc::clone(&create_calls);
    let inspect_calls_clone = Arc::clone(&inspect_calls);

    let app = AxumRouter::new()
        .route(
            "/v1/checkpoints",
            get(move || {
                let list_calls = Arc::clone(&list_calls_clone);
                async move {
                    list_calls.fetch_add(1, Ordering::SeqCst);
                    Json(serde_json::json!({
                        "request_id": "req-checkpoint-list",
                        "checkpoints": [{
                            "checkpoint_id": "ckpt-stub-1",
                            "sandbox_id": "sbx-stub",
                            "parent_checkpoint_id": serde_json::Value::Null,
                            "class": "fs_quick",
                            "state": "ready",
                            "compatibility_fingerprint": "fp-stub",
                            "created_at": 1730000000u64
                        }]
                    }))
                }
            })
            .post(
                move |ExtractJson(payload): ExtractJson<serde_json::Value>| {
                    let create_calls = Arc::clone(&create_calls_clone);
                    async move {
                        create_calls.fetch_add(1, Ordering::SeqCst);
                        if payload["sandbox_id"].as_str() != Some("sbx-stub")
                            || payload["class"].as_str() != Some("fs_quick")
                            || payload["compatibility_fingerprint"].as_str() != Some("fp-stub")
                        {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(serde_json::json!({
                                    "error": {
                                        "code": "invalid_request",
                                        "message": "unexpected checkpoint create payload",
                                        "request_id": "req-checkpoint-create-err"
                                    }
                                })),
                            );
                        }
                        (
                            StatusCode::CREATED,
                            Json(serde_json::json!({
                                "request_id": "req-checkpoint-create",
                                "checkpoint": {
                                    "checkpoint_id": "ckpt-stub-1",
                                    "sandbox_id": "sbx-stub",
                                    "parent_checkpoint_id": serde_json::Value::Null,
                                    "class": "fs_quick",
                                    "state": "ready",
                                    "compatibility_fingerprint": "fp-stub",
                                    "created_at": 1730000000u64
                                }
                            })),
                        )
                    }
                },
            ),
        )
        .route(
            "/v1/checkpoints/{checkpoint_id}",
            get(move |AxumPath(checkpoint_id): AxumPath<String>| {
                let inspect_calls = Arc::clone(&inspect_calls_clone);
                async move {
                    inspect_calls.fetch_add(1, Ordering::SeqCst);
                    if checkpoint_id != "ckpt-stub-1" {
                        return (
                            StatusCode::NOT_FOUND,
                            Json(serde_json::json!({
                                "error": {
                                    "code": "not_found",
                                    "message": format!("checkpoint {checkpoint_id} not found"),
                                    "request_id": "req-checkpoint-inspect-err"
                                }
                            })),
                        );
                    }
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "request_id": "req-checkpoint-inspect",
                            "checkpoint": {
                                "checkpoint_id": "ckpt-stub-1",
                                "sandbox_id": "sbx-stub",
                                "parent_checkpoint_id": serde_json::Value::Null,
                                "class": "fs_quick",
                                "state": "ready",
                                "compatibility_fingerprint": "fp-stub",
                                "created_at": 1730000000u64
                            }
                        })),
                    )
                }
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind stub API listener")?;
    let address = listener
        .local_addr()
        .context("resolve stub API listener address")?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let api_base_url = format!("http://{address}");
    let vz_bin = resolve_vz_binary()?;

    let create_output = run_vz_command(
        &vz_bin,
        &api_base_url,
        &home_dir,
        &[
            "debug",
            "checkpoint",
            "create",
            "sbx-stub",
            "--class",
            "fs_quick",
            "--fingerprint",
            "fp-stub",
        ],
    )
    .await?;
    if !create_output.status.success() {
        bail!(
            "vz checkpoint create failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&create_output.stdout),
            String::from_utf8_lossy(&create_output.stderr)
        );
    }
    let create_stdout = String::from_utf8_lossy(&create_output.stdout);
    if !create_stdout.contains("Checkpoint ckpt-stub-1 created (state: ready).") {
        bail!(
            "vz checkpoint create output missing expected marker:\n{}",
            create_stdout
        );
    }

    let list_output = run_vz_command(
        &vz_bin,
        &api_base_url,
        &home_dir,
        &["debug", "checkpoint", "list", "--json"],
    )
    .await?;
    if !list_output.status.success() {
        bail!(
            "vz checkpoint list failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&list_output.stdout),
            String::from_utf8_lossy(&list_output.stderr)
        );
    }
    let listed: serde_json::Value =
        serde_json::from_slice(&list_output.stdout).context("decode checkpoint list output")?;
    let listed_items = listed
        .as_array()
        .context("checkpoint list output should be JSON array")?;
    if !listed_items.iter().any(|item| {
        item.get("checkpoint_id")
            .and_then(serde_json::Value::as_str)
            == Some("ckpt-stub-1")
    }) {
        bail!("checkpoint list output missing ckpt-stub-1");
    }

    let inspect_output = run_vz_command(
        &vz_bin,
        &api_base_url,
        &home_dir,
        &["debug", "checkpoint", "inspect", "ckpt-stub-1"],
    )
    .await?;
    if !inspect_output.status.success() {
        bail!(
            "vz checkpoint inspect failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&inspect_output.stdout),
            String::from_utf8_lossy(&inspect_output.stderr)
        );
    }
    let inspected: serde_json::Value = serde_json::from_slice(&inspect_output.stdout)
        .context("decode checkpoint inspect output")?;
    if inspected
        .get("checkpoint_id")
        .and_then(serde_json::Value::as_str)
        != Some("ckpt-stub-1")
    {
        bail!("checkpoint inspect output missing ckpt-stub-1");
    }

    assert_eq!(
        create_calls.load(Ordering::SeqCst),
        1,
        "checkpoint create endpoint should be called exactly once"
    );
    assert_eq!(
        list_calls.load(Ordering::SeqCst),
        1,
        "checkpoint list endpoint should be called exactly once"
    );
    assert_eq!(
        inspect_calls.load(Ordering::SeqCst),
        1,
        "checkpoint inspect endpoint should be called exactly once"
    );

    let _ = shutdown_tx.send(());
    server
        .await
        .context("join stub API server task")?
        .context("run stub API server")?;

    Ok(())
}
