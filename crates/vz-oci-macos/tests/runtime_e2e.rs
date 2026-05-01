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
//! Run with: `./scripts/run-sandbox-vm-e2e.sh --suite runtime`

#![allow(clippy::unwrap_used)]

use std::process::Command;
use std::time::Duration;

use vz_oci_macos::{ExecConfig, ExecutionMode, RunConfig, Runtime, RuntimeConfig};

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

fn require_virtualization_entitlement() -> bool {
    if has_virtualization_entitlement() {
        return true;
    }

    eprintln!(
        "skipping runtime_e2e: test binary is missing com.apple.security.virtualization entitlement; run ./scripts/run-sandbox-vm-e2e.sh --suite runtime"
    );
    false
}

// ── Smoke test: pull + run ──────────────────────────────────────

/// Pull alpine:latest and run `echo hello` via one-shot `Runtime::run()`.
///
/// This is the most fundamental E2E test: proves the full pipeline
/// from image pull → rootfs assembly → VM boot → guest agent → exec → output.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn smoke_pull_and_run_alpine() {
    if !require_virtualization_entitlement() {
        return;
    }
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
    if !require_virtualization_entitlement() {
        return;
    }
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
    if !require_virtualization_entitlement() {
        return;
    }
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
    if !require_virtualization_entitlement() {
        return;
    }
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
    if !require_virtualization_entitlement() {
        return;
    }
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
    let stopped = rt
        .stop_container(&container_id, false, None, None)
        .await
        .unwrap();
    assert!(
        !matches!(stopped.status, vz_oci_macos::ContainerStatus::Running),
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

/// Validate live interactive exec control (stdin/resize/signal) and stale
/// session diagnostics after completion.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn interactive_exec_control_session_round_trip() {
    if !require_virtualization_entitlement() {
        return;
    }
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

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

    let execution_id = "exec-interactive-e2e".to_string();
    let rt_exec = rt.clone();
    let container_for_exec = container_id.clone();
    let execution_for_task = execution_id.clone();
    let exec_task = tokio::spawn(async move {
        rt_exec
            .exec_container(
                &container_for_exec,
                ExecConfig {
                    execution_id: Some(execution_for_task),
                    cmd: vec![
                        "sh".into(),
                        "-lc".into(),
                        "read line; sleep 1; echo got:$line".into(),
                    ],
                    pty: true,
                    term_rows: Some(24),
                    term_cols: Some(80),
                    timeout: Some(Duration::from_secs(30)),
                    ..ExecConfig::default()
                },
            )
            .await
    });

    let mut wrote = false;
    for _ in 0..40 {
        match rt
            .write_exec_stdin(&execution_id, b"hello-interactive\n")
            .await
        {
            Ok(()) => {
                wrote = true;
                break;
            }
            Err(vz_oci_macos::MacosOciError::ExecutionSessionNotFound { .. }) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => panic!("unexpected stdin error: {err:?}"),
        }
    }
    assert!(wrote, "interactive session should accept stdin writes");

    rt.resize_exec_pty(&execution_id, 120, 40).await.unwrap();
    rt.signal_exec(&execution_id, "SIGWINCH").await.unwrap();

    let output = exec_task.await.unwrap().unwrap();
    assert_eq!(output.exit_code, 0, "interactive exec should complete");
    assert!(
        output.stdout.contains("got:hello-interactive"),
        "interactive stdout should contain echoed line, got: {}",
        output.stdout
    );

    let stale = rt
        .write_exec_stdin(&execution_id, b"after-complete\n")
        .await
        .unwrap_err();
    assert!(
        matches!(
            stale,
            vz_oci_macos::MacosOciError::ExecutionSessionNotFound { .. }
        ),
        "stale session should return ExecutionSessionNotFound, got: {stale:?}"
    );

    let _ = rt.stop_container(&container_id, true, None, None).await;
    let _ = rt.remove_container(&container_id).await;
}

// ── Container logs ──────────────────────────────────────────────

/// Create a container with capture_logs, run a command that writes output,
/// then verify we can read the logs via exec.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn container_logs_capture_and_retrieve() {
    if !require_virtualization_entitlement() {
        return;
    }
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    // Create a container with capture_logs enabled.
    // The init process writes output that gets captured to /var/log/vz-oci/output.log.
    let container_id = rt
        .create_container(
            "alpine:latest",
            RunConfig {
                cmd: vec![
                    "sh".into(),
                    "-c".into(),
                    "echo log-line-one && echo log-line-two && sleep 300".into(),
                ],
                execution_mode: ExecutionMode::OciRuntime,
                capture_logs: true,
                ..RunConfig::default()
            },
        )
        .await
        .unwrap();

    // Give the init process a moment to produce output.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Read the log file via exec.
    let log_output = rt
        .exec_container(
            &container_id,
            ExecConfig {
                cmd: vec![
                    "tail".into(),
                    "-n".into(),
                    "100".into(),
                    "/var/log/vz-oci/output.log".into(),
                ],
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(log_output.exit_code, 0, "tail should succeed");
    assert!(
        log_output.stdout.contains("log-line-one"),
        "logs should contain 'log-line-one', got: {}",
        log_output.stdout
    );
    assert!(
        log_output.stdout.contains("log-line-two"),
        "logs should contain 'log-line-two', got: {}",
        log_output.stdout
    );

    // Also test via the RuntimeBackend::logs() trait (through MacosRuntimeBackend).
    use vz_runtime_contract::RuntimeBackend;
    let backend = vz_oci_macos::MacosRuntimeBackend::new(rt);
    let logs = backend.logs(&container_id).unwrap();
    assert!(
        logs.output.contains("log-line-one"),
        "RuntimeBackend::logs() should contain 'log-line-one', got: {}",
        logs.output
    );

    // Cleanup.
    backend
        .inner()
        .stop_container(&container_id, true, None, None)
        .await
        .unwrap();
    backend
        .inner()
        .remove_container(&container_id)
        .await
        .unwrap();
}

// ── Port forwarding ─────────────────────────────────────────────

/// Start a container with port forwarding and verify TCP connectivity.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn port_forwarding_tcp() {
    if !require_virtualization_entitlement() {
        return;
    }
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
                ports: vec![vz_oci_macos::PortMapping {
                    host: 18080,
                    container: 8080,
                    protocol: vz_oci_macos::PortProtocol::Tcp,
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
    let _ = rt.stop_container(&container_id, true, None, None).await;
    let _ = rt.remove_container(&container_id).await;
}

/// VRT-gsk0 Bug B: when the daemon respawns after a kill, the in-memory
/// `stack_vms` map is empty. A subsequent `vz stop` enters
/// `shutdown_shared_vm` for a stack_id that exists only in SQLite, not
/// in the runtime. The fix is to treat this as idempotent ("already
/// stopped") instead of erroring with "no shared VM running" and
/// relying on a string-match mask in the gRPC handler.
///
/// This test does not require the virtualization entitlement — it
/// constructs a Runtime and calls shutdown_shared_vm on an unknown
/// stack_id, which exercises only the in-memory branch.
#[tokio::test]
async fn shutdown_shared_vm_is_idempotent_when_in_memory_state_empty() {
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    // No prior boot — stack_vms is empty.
    let result = rt.shutdown_shared_vm("does-not-exist").await;
    assert!(
        result.is_ok(),
        "shutdown_shared_vm should be idempotent when no in-memory VM, got: {result:?}"
    );

    // Still idempotent on a second call.
    let result2 = rt.shutdown_shared_vm("does-not-exist").await;
    assert!(result2.is_ok(), "shutdown_shared_vm should remain idempotent: {result2:?}");
}

// ── Image pull caching ──────────────────────────────────────────

/// Verify that pulling the same image twice is idempotent (uses cache).
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn pull_is_idempotent() {
    if !require_virtualization_entitlement() {
        return;
    }
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
    if !require_virtualization_entitlement() {
        return;
    }
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    let result = rt.pull("library/this-image-does-not-exist:v999").await;
    assert!(result.is_err(), "pulling nonexistent image should fail");
}

// ── Cgroup resource limits ───────────────────────────────────────

/// Verify that cgroup cpu.max is correctly enforced inside the container.
///
/// Creates a container with `cpu_quota=50000` and `cpu_period=100000`
/// (equivalent to `cpus=0.5`), then reads `/sys/fs/cgroup/cpu.max` inside
/// the running container and asserts the kernel exposes the expected
/// `"50000 100000"` throttle values.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn cgroup_cpu_max_enforcement() {
    if !require_virtualization_entitlement() {
        return;
    }
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    // Create a long-lived container with cpu_quota=50000 / cpu_period=100000 (0.5 CPU).
    let container_id = rt
        .create_container(
            "alpine:latest",
            RunConfig {
                cmd: vec!["sleep".into(), "300".into()],
                execution_mode: ExecutionMode::OciRuntime,
                cpu_quota: Some(50_000),
                cpu_period: Some(100_000),
                ..RunConfig::default()
            },
        )
        .await
        .unwrap();

    // Read CPU throttling values inside the container.
    //
    // Some guests expose cgroup v2 (`cpu.max`), while others still expose
    // cgroup v1 (`cpu.cfs_quota_us` + `cpu.cfs_period_us`).
    let exec_out = rt
        .exec_container(
            &container_id,
            ExecConfig {
                cmd: vec![
                    "sh".into(),
                    "-c".into(),
                    "if [ -f /sys/fs/cgroup/cpu.max ]; then \
                        cat /sys/fs/cgroup/cpu.max; \
                    elif [ -f /sys/fs/cgroup/cpu/cpu.cfs_quota_us ] && [ -f /sys/fs/cgroup/cpu/cpu.cfs_period_us ]; then \
                        cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us /sys/fs/cgroup/cpu/cpu.cfs_period_us; \
                    else \
                        echo 'missing cpu cgroup controls' >&2; \
                        exit 1; \
                    fi"
                        .into(),
                ],
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();

    if exec_out.exit_code != 0 {
        if exec_out.stderr.contains("missing cpu cgroup controls") {
            eprintln!(
                "skipping cgroup_cpu_max_enforcement: guest does not expose cpu cgroup controls"
            );
            let _ = rt.stop_container(&container_id, true, None, None).await;
            let _ = rt.remove_container(&container_id).await;
            return;
        }

        panic!(
            "reading cpu cgroup throttling controls should succeed: stderr={}",
            exec_out.stderr
        );
    }
    let normalized = exec_out.stdout.trim();
    if normalized.contains(' ') {
        assert_eq!(
            normalized, "50000 100000",
            "cpu.max should reflect quota=50000 period=100000 (0.5 CPU), got: {normalized}"
        );
    } else {
        let lines: Vec<&str> = normalized.lines().map(str::trim).collect();
        assert_eq!(
            lines.len(),
            2,
            "expected cgroup v1 output with quota and period lines, got: {normalized}"
        );
        assert_eq!(
            lines[0], "50000",
            "cpu.cfs_quota_us should be 50000, got: {}",
            lines[0]
        );
        assert_eq!(
            lines[1], "100000",
            "cpu.cfs_period_us should be 100000, got: {}",
            lines[1]
        );
    }

    // Cleanup.
    let _ = rt.stop_container(&container_id, true, None, None).await;
    let _ = rt.remove_container(&container_id).await;
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
    if !require_virtualization_entitlement() {
        return;
    }
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
    rt.boot_shared_vm(stack_id, vec![], Default::default())
        .await
        .unwrap();

    // 2. Set up per-service networking.
    let services = vec![
        vz_oci_macos::NetworkServiceConfig {
            name: "web".to_string(),
            addr: "172.20.0.2/24".to_string(),
            network_name: "default".to_string(),
        },
        vz_oci_macos::NetworkServiceConfig {
            name: "db".to_string(),
            addr: "172.20.0.3/24".to_string(),
            network_name: "default".to_string(),
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
            None,
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
            None,
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
