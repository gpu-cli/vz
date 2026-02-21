//! End-to-end runtime tests exercising real Linux VMs.
//!
//! These tests pull real OCI images, boot real Linux VMs via
//! Virtualization.framework, and execute commands through the
//! guest agent + youki pipeline.
//!
//! Requirements:
//! - Apple Silicon Mac (arm64)
//! - Linux kernel artifacts installed (`~/.vz/linux/`)
//! - Network access for image pulls (first run only; cached after)
//!
//! Run with: `cargo nextest run -p vz-oci --test runtime_e2e -- --ignored`

#![allow(clippy::unwrap_used)]

use std::time::Duration;

use vz_oci::{ExecConfig, ExecutionMode, RunConfig, Runtime, RuntimeConfig};

/// Set up tracing for test diagnostics.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci=debug,vz_linux=debug")
        .with_test_writer()
        .try_init();
}

/// Build a runtime with a unique temp data dir for test isolation.
fn test_runtime(data_dir: &std::path::Path) -> Runtime {
    let config = RuntimeConfig {
        data_dir: data_dir.to_path_buf(),
        require_exact_agent_version: false,
        agent_ready_timeout: Duration::from_secs(15),
        exec_timeout: Duration::from_secs(30),
        default_memory_mb: 4096,
        ..RuntimeConfig::default()
    };
    Runtime::new(config)
}

// ── Smoke test: pull + run ──────────────────────────────────────

/// Pull alpine:latest and run `echo hello` via one-shot `Runtime::run()`.
///
/// This is the most fundamental E2E test: proves the full pipeline
/// from image pull → rootfs assembly → VM boot → guest agent → exec → output.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn smoke_pull_and_run_alpine() {
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    // Pull alpine (arm64 only, ~7 MB).
    let image_id = rt.pull("alpine:latest").await.unwrap();
    assert!(
        !image_id.0.is_empty(),
        "image ID should be non-empty after pull"
    );

    // Run `echo hello` via GuestExec mode with serial log for diagnostics.
    let serial_log = tmp.path().join("serial.log");
    let output = rt
        .run(
            "alpine:latest",
            RunConfig {
                cmd: vec!["echo".into(), "hello".into()],
                serial_log_file: Some(serial_log.clone()),
                ..RunConfig::default()
            },
        )
        .await;

    // Print serial log on failure for diagnostics.
    if output.is_err() {
        if let Ok(log) = std::fs::read_to_string(&serial_log) {
            eprintln!("=== Serial log ===\n{log}\n=== End serial log ===");
        }
    }

    let output = output.unwrap();
    assert_eq!(output.exit_code, 0, "exit code should be 0");
    assert_eq!(output.stdout.trim(), "hello", "stdout should be 'hello'");
}

/// Run with OCI runtime mode (youki create → start → exec → delete).
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn smoke_run_oci_runtime_mode() {
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    let output = rt
        .run(
            "alpine:latest",
            RunConfig {
                cmd: vec!["echo".into(), "oci-hello".into()],
                execution_mode: ExecutionMode::OciRuntime,
                ..RunConfig::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(output.exit_code, 0);
    assert_eq!(output.stdout.trim(), "oci-hello");
}

/// Run a command that exits non-zero and verify we capture the exit code.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn smoke_nonzero_exit_code() {
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    let output = rt
        .run(
            "alpine:latest",
            RunConfig {
                cmd: vec!["sh".into(), "-c".into(), "exit 42".into()],
                ..RunConfig::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(output.exit_code, 42, "should capture non-zero exit code");
}

/// Verify environment variables are passed to the container.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn smoke_environment_variables() {
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    let output = rt
        .run(
            "alpine:latest",
            RunConfig {
                cmd: vec!["sh".into(), "-c".into(), "echo $MY_VAR".into()],
                env: vec![("MY_VAR".into(), "test_value".into())],
                ..RunConfig::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(output.exit_code, 0);
    assert_eq!(output.stdout.trim(), "test_value");
}

// ── Container lifecycle: create → exec → stop → remove ─────────

/// Exercise the long-lived container lifecycle:
/// create_container → exec → exec again → stop → remove.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn lifecycle_create_exec_stop_remove() {
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    // Create a long-lived container with a sleep init process.
    let container_id = rt
        .create_container(
            "alpine:latest",
            RunConfig {
                cmd: vec!["sleep".into(), "300".into()],
                execution_mode: ExecutionMode::OciRuntime,
                ..RunConfig::default()
            },
        )
        .await
        .unwrap();

    assert!(!container_id.is_empty(), "container ID should be non-empty");

    // Container should be listed as running.
    let containers = rt.list_containers().unwrap();
    let found = containers.iter().find(|c| c.id == container_id);
    assert!(found.is_some(), "container should appear in list");

    // Exec a command inside the running container.
    let exec_out = rt
        .exec_container(
            &container_id,
            ExecConfig {
                cmd: vec!["echo".into(), "from-exec".into()],
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(exec_out.exit_code, 0);
    assert_eq!(exec_out.stdout.trim(), "from-exec");

    // Exec another command to prove the container stays alive across execs.
    let exec_out2 = rt
        .exec_container(
            &container_id,
            ExecConfig {
                cmd: vec!["echo".into(), "still-alive".into()],
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(exec_out2.exit_code, 0);
    assert_eq!(exec_out2.stdout.trim(), "still-alive");

    // Stop the container.
    let stopped = rt.stop_container(&container_id, false).await.unwrap();
    assert!(
        !matches!(stopped.status, vz_oci::ContainerStatus::Running),
        "container should not be running after stop"
    );

    // Remove the container.
    rt.remove_container(&container_id).await.unwrap();

    // Verify it's gone.
    let containers_after = rt.list_containers().unwrap();
    assert!(
        !containers_after.iter().any(|c| c.id == container_id),
        "container should be removed from list"
    );
}

// ── Port forwarding ─────────────────────────────────────────────

/// Start a container with port forwarding and verify TCP connectivity.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn port_forwarding_tcp() {
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    // Run a simple TCP listener on port 8080 inside the container,
    // mapped to host port 18080. Use nc to echo a response.
    let container_id = rt
        .create_container(
            "alpine:latest",
            RunConfig {
                cmd: vec![
                    "sh".into(),
                    "-c".into(),
                    "echo 'pong' | nc -l -p 8080".into(),
                ],
                execution_mode: ExecutionMode::OciRuntime,
                ports: vec![vz_oci::PortMapping {
                    host: 18080,
                    container: 8080,
                    protocol: vz_oci::PortProtocol::Tcp,
                    target_host: None,
                }],
                ..RunConfig::default()
            },
        )
        .await
        .unwrap();

    // Give the listener a moment to start.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Connect from the host — retry a few times to allow the listener to start.
    use tokio::io::AsyncReadExt;
    let mut conn = None;
    for attempt in 1..=5 {
        match tokio::net::TcpStream::connect("127.0.0.1:18080").await {
            Ok(stream) => {
                conn = Some(stream);
                break;
            }
            Err(e) if attempt < 5 => {
                eprintln!("port forward connect attempt {attempt}/5 failed: {e}, retrying...");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => panic!("port forwarding connection failed after 5 attempts: {e}"),
        }
    }
    let mut conn = conn.unwrap();
    let mut buf = vec![0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(10), conn.read(&mut buf))
        .await
        .expect("port forward read timed out")
        .expect("port forward read failed");
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("pong"),
        "expected 'pong', got: {response}"
    );

    // Drop the connection before cleanup to unblock the relay.
    drop(conn);

    // Cleanup.
    let _ = rt.stop_container(&container_id, true).await;
    let _ = rt.remove_container(&container_id).await;
}

// ── Image pull caching ──────────────────────────────────────────

/// Verify that pulling the same image twice is idempotent (uses cache).
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn pull_is_idempotent() {
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    let id1 = rt.pull("alpine:latest").await.unwrap();
    let id2 = rt.pull("alpine:latest").await.unwrap();
    assert_eq!(id1.0, id2.0, "same image should produce same ID");

    let images = rt.images().unwrap();
    assert!(
        !images.is_empty(),
        "images list should contain pulled image"
    );
}

/// Pulling a nonexistent image should fail gracefully.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn pull_nonexistent_image_fails() {
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    let result = rt.pull("library/this-image-does-not-exist:v999").await;
    assert!(result.is_err(), "pulling nonexistent image should fail");
}

// ── Shared VM inter-service connectivity ────────────────────────

/// Boot a shared VM with two containers in isolated network namespaces,
/// then verify cross-service connectivity by IP and hostname.
///
/// This exercises the full stack VM pipeline:
/// boot_shared_vm → network_setup → create_container_in_stack × 2 → exec ping → shutdown.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn shared_vm_inter_service_connectivity() {
    init_tracing();
    // Use persistent data dir for image cache to avoid Docker Hub rate limits.
    let home = std::env::var("HOME").unwrap();
    let data_dir = std::path::PathBuf::from(home).join(".vz/oci");
    std::fs::create_dir_all(&data_dir).unwrap();
    let rt = test_runtime(&data_dir);

    // Pull alpine (skip if already cached to avoid Docker Hub rate limits).
    if rt.pull("alpine:latest").await.is_err() {
        eprintln!("WARN: pull failed (rate limit?), assuming image is cached");
    }

    let stack_id = "e2e-net";

    // 1. Boot shared VM.
    rt.boot_shared_vm(stack_id, vec![]).await.unwrap();

    // 2. Set up per-service networking.
    let services = vec![
        vz_oci::NetworkServiceConfig {
            name: "web".to_string(),
            addr: "172.20.0.2/24".to_string(),
        },
        vz_oci::NetworkServiceConfig {
            name: "db".to_string(),
            addr: "172.20.0.3/24".to_string(),
        },
    ];
    rt.network_setup(stack_id, services).await.unwrap();

    // 3. Create containers with cross-service /etc/hosts.
    let hosts = vec![
        ("web".to_string(), "172.20.0.2".to_string()),
        ("db".to_string(), "172.20.0.3".to_string()),
    ];

    let web_id = rt
        .create_container_in_stack(
            stack_id,
            "alpine:latest",
            RunConfig {
                cmd: vec!["sleep".into(), "300".into()],
                execution_mode: ExecutionMode::OciRuntime,
                extra_hosts: hosts.clone(),
                network_namespace_path: Some("/var/run/netns/web".to_string()),
                ..RunConfig::default()
            },
        )
        .await
        .unwrap_or_else(|e| panic!("create web container failed: {e:?}"));

    let db_id = rt
        .create_container_in_stack(
            stack_id,
            "alpine:latest",
            RunConfig {
                cmd: vec!["sleep".into(), "300".into()],
                execution_mode: ExecutionMode::OciRuntime,
                extra_hosts: hosts.clone(),
                network_namespace_path: Some("/var/run/netns/db".to_string()),
                ..RunConfig::default()
            },
        )
        .await
        .unwrap();

    // 4. Exec ping by IP: web → db.
    // Use /bin/busybox directly since busybox is the real binary, not a
    // symlink. VirtioFS-backed overlays may not properly expose busybox
    // applet symlinks to the guest.
    // Timeout set to 30s to account for vsock handshake retries.
    let ping_by_ip = rt
        .exec_container(
            &web_id,
            ExecConfig {
                cmd: vec![
                    "/bin/busybox".into(),
                    "ping".into(),
                    "-c".into(),
                    "1".into(),
                    "-W".into(),
                    "3".into(),
                    "172.20.0.3".into(),
                ],
                timeout: Some(Duration::from_secs(30)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(
        ping_by_ip.exit_code, 0,
        "ping by IP should succeed (web→db): stderr={}",
        ping_by_ip.stderr
    );

    // 5. Exec ping by hostname: db → web.
    let ping_by_name = rt
        .exec_container(
            &db_id,
            ExecConfig {
                cmd: vec![
                    "/bin/busybox".into(),
                    "ping".into(),
                    "-c".into(),
                    "1".into(),
                    "-W".into(),
                    "3".into(),
                    "web".into(),
                ],
                timeout: Some(Duration::from_secs(30)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(
        ping_by_name.exit_code, 0,
        "ping by hostname should succeed (db→web): stderr={}",
        ping_by_name.stderr
    );

    // 6. Tear down.
    let _ = rt
        .network_teardown(stack_id, vec!["web".to_string(), "db".to_string()])
        .await;
    rt.shutdown_shared_vm(stack_id).await.unwrap();
}
