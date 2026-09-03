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

use vz_oci_macos::{
    ExecConfig, ExecutionMode, InteractiveExecEvent, KernelProfile, MountAccess, MountSpec,
    MountType, RunConfig, Runtime, RuntimeConfig,
};

/// Set up tracing for test diagnostics.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci=debug,vz_linux=debug")
        .with_test_writer()
        .try_init();
}

/// Build a runtime with a unique temp data dir for test isolation.
fn test_runtime(data_dir: &std::path::Path) -> Runtime {
    test_runtime_for_profile(data_dir, None)
}

fn test_runtime_for_profile(
    data_dir: &std::path::Path,
    linux_profile: Option<KernelProfile>,
) -> Runtime {
    let config = RuntimeConfig {
        data_dir: data_dir.to_path_buf(),
        linux_profile,
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
        "VZ_E2E_REQUIRED_SKIP: runtime_e2e test binary is missing com.apple.security.virtualization entitlement; run ./scripts/run-sandbox-vm-e2e.sh --suite runtime"
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

/// Verify the container kernel exposes the cgroup memory controller and
/// bridge-netfilter sysctls required by dockerd.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn container_kernel_exposes_docker_prerequisites() {
    if !require_virtualization_entitlement() {
        return;
    }
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime_for_profile(tmp.path(), Some(KernelProfile::Container));

    let output = rt
        .run(
            "alpine:latest",
            RunConfig {
                cmd: vec![
                    "sh".into(),
                    "-c".into(),
                    "grep -qw memory /sys/fs/cgroup/cgroup.controllers && \
                     test -e /proc/sys/net/bridge/bridge-nf-call-iptables && \
                     echo docker-kernel-prerequisites-ok"
                        .into(),
                ],
                ..RunConfig::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(output.exit_code, 0, "kernel prerequisites probe failed");
    assert_eq!(output.stdout.trim(), "docker-kernel-prerequisites-ok");
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
    assert!(
        result2.is_ok(),
        "shutdown_shared_vm should remain idempotent: {result2:?}"
    );
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

const CGROUP_EXEC_PROBE: &str = r#"set -eu
self_cgroup=$(/bin/busybox cat /proc/self/cgroup)
self_pid=$$
cgroup_procs=$(/bin/busybox cat /sys/fs/cgroup/cgroup.procs)
printf '%s\n' "$cgroup_procs" | /bin/busybox grep -qx "$self_pid"
init_pid=$(printf '%s\n' "$cgroup_procs" | /bin/busybox sort -n | /bin/busybox head -n 1)
test -n "$init_pid"
init_cgroup=$(/bin/busybox cat "/proc/$init_pid/cgroup")
cwd_identity=$(/bin/busybox stat -c '%d:%i' .)
root_identity=$(/bin/busybox stat -c '%d:%i' /)
leaked_namespace_fds=0
for fd in /proc/self/fd/*; do
    case "$fd" in */0|*/1|*/2) continue ;; esac
    target=$(/bin/busybox readlink "$fd" 2>/dev/null || true)
    case "$target" in mnt:\[*|net:\[*|pid:\[*|ipc:\[*|uts:\[*|cgroup:\[* )
        leaked_namespace_fds=$((leaked_namespace_fds + 1))
        ;;
    esac
done
cgroup_filesystem=$(/bin/busybox awk '$2 == "/sys/fs/cgroup" { print $3 }' /proc/mounts)
controllers=$(/bin/busybox cat /sys/fs/cgroup/cgroup.controllers)
cpu_max=$(/bin/busybox cat /sys/fs/cgroup/cpu.max)
pids_max=$(/bin/busybox cat /sys/fs/cgroup/pids.max)
pids_current=$(/bin/busybox cat /sys/fs/cgroup/pids.current)
if [ -f /sys/fs/cgroup/memory.max ]; then
    memory_max=$(/bin/busybox cat /sys/fs/cgroup/memory.max)
    memory_current=$(/bin/busybox cat /sys/fs/cgroup/memory.current)
else
    memory_max=absent
    memory_current=absent
fi
before=$(/bin/busybox awk '$1 == "nr_throttled" { print $2 }' /sys/fs/cgroup/cpu.stat)
/bin/busybox timeout 2 /bin/busybox yes >/dev/null || true
after=$(/bin/busybox awk '$1 == "nr_throttled" { print $2 }' /sys/fs/cgroup/cpu.stat)
printf 'mode=%s\nself_pid=%s\nself_cgroup=%s\ninit_pid=%s\ninit_cgroup=%s\ncwd_identity=%s\nroot_identity=%s\nleaked_namespace_fds=%s\ncgroup_filesystem=%s\ncontrollers=%s\ncpu_max=%s\npids_max=%s\npids_current=%s\nmemory_max=%s\nmemory_current=%s\nnr_throttled_before=%s\nnr_throttled_after=%s\n' \
    "$0" "$self_pid" "$self_cgroup" "$init_pid" "$init_cgroup" "$cwd_identity" "$root_identity" "$leaked_namespace_fds" "$cgroup_filesystem" "$controllers" \
    "$cpu_max" "$pids_max" "$pids_current" \
    "$memory_max" "$memory_current" "$before" "$after"
test "$self_cgroup" = "$init_cgroup"
test "$cwd_identity" = "$root_identity"
test "$leaked_namespace_fds" -eq 0
test "$cgroup_filesystem" = cgroup2
echo "$controllers" | /bin/busybox grep -qw cpu
echo "$controllers" | /bin/busybox grep -qw pids
test "$cpu_max" = '50000 100000'
test "$pids_max" = '64'
test "$pids_current" -gt 0
test "$pids_current" -le "$pids_max"
if [ "$memory_current" != absent ]; then
    test "$memory_max" = max || test "$memory_max" -gt 0
    test "$memory_current" -ge 0
fi
test "$after" -gt "$before""#;

fn cgroup_probe_config(mode: &str, pty: bool) -> ExecConfig {
    ExecConfig {
        execution_id: pty.then(|| format!("cgroup-probe-{mode}")),
        cmd: vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            CGROUP_EXEC_PROBE.to_string(),
            mode.to_string(),
        ],
        pty,
        timeout: Some(Duration::from_secs(30)),
        ..ExecConfig::default()
    }
}

fn normalized_probe_output(output: &str) -> String {
    output.replace('\r', "")
}

#[allow(clippy::print_stderr)]
fn assert_cgroup_probe(mode: &str, output: &vz::protocol::ExecOutput) {
    let stdout = normalized_probe_output(&output.stdout);
    eprintln!("cgroup exec evidence ({mode}):\n{stdout}");
    assert_eq!(
        output.exit_code, 0,
        "{mode} cgroup probe failed: stdout={stdout} stderr={}",
        output.stderr
    );
    let evidence: std::collections::HashMap<&str, &str> = stdout
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect();
    assert_eq!(evidence.get("mode"), Some(&mode));
    assert!(
        evidence
            .get("self_pid")
            .is_some_and(|pid| pid.parse::<u32>().is_ok())
    );
    assert!(
        evidence
            .get("init_pid")
            .is_some_and(|pid| pid.parse::<u32>().is_ok())
    );
    assert_eq!(evidence.get("self_cgroup"), evidence.get("init_cgroup"));
    assert_eq!(evidence.get("cwd_identity"), evidence.get("root_identity"));
    assert_eq!(evidence.get("leaked_namespace_fds"), Some(&"0"));
    assert_eq!(evidence.get("cgroup_filesystem"), Some(&"cgroup2"));
    let controllers = evidence["controllers"];
    assert!(controllers.split_whitespace().any(|item| item == "cpu"));
    assert!(controllers.split_whitespace().any(|item| item == "pids"));
    assert_eq!(evidence.get("cpu_max"), Some(&"50000 100000"));
    assert_eq!(evidence.get("pids_max"), Some(&"64"));
    let pids_current: u64 = evidence["pids_current"].parse().unwrap();
    assert!((1..=64).contains(&pids_current));
    match (evidence.get("memory_max"), evidence.get("memory_current")) {
        (Some(&"absent"), Some(&"absent")) => {}
        (Some(memory_max), Some(memory_current)) => {
            assert!(*memory_max == "max" || memory_max.parse::<u64>().is_ok());
            let _: u64 = memory_current.parse().unwrap();
        }
        values => panic!("incomplete memory controller evidence for {mode}: {values:?}"),
    }
    let before: u64 = evidence["nr_throttled_before"].parse().unwrap();
    let after: u64 = evidence["nr_throttled_after"].parse().unwrap();
    assert!(
        after > before,
        "{mode} CPU work was not throttled: before={before} after={after}"
    );
}

fn assert_callback_stdout_precedes_exit(mode: &str, events: &[InteractiveExecEvent]) {
    assert!(
        matches!(events.last(), Some(InteractiveExecEvent::Exit(0))),
        "{mode} callback must end with Exit(0): {events:?}"
    );
    let exit_index = events.len() - 1;
    assert!(
        events[..exit_index].iter().any(|event| matches!(
            event,
            InteractiveExecEvent::Stdout(bytes)
                if !normalized_probe_output(&String::from_utf8_lossy(bytes)).is_empty()
        )),
        "{mode} callback emitted no stdout before Exit(0): {events:?}"
    );
}

/// Verify every Linux container exec adapter joins the target cgroup before
/// launching bounded CPU work and inherits its CPU, pids, and memory controls.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn cgroup_cpu_max_enforcement() {
    if !require_virtualization_entitlement() {
        return;
    }
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());

    let create_result = rt
        .create_container(
            "alpine:latest",
            RunConfig {
                cmd: vec!["/bin/busybox".into(), "sleep".into(), "300".into()],
                execution_mode: ExecutionMode::OciRuntime,
                cpu_quota: Some(50_000),
                cpu_period: Some(100_000),
                pids_limit: Some(64),
                ..RunConfig::default()
            },
        )
        .await;
    let mut streaming_events = Vec::new();
    let mut pty_events = Vec::new();
    let (unary_result, streaming_result, pty_result, stop_result, remove_result) =
        match &create_result {
            Ok(container_id) => {
                let unary_result = rt
                    .exec_container_oci_unary(container_id, cgroup_probe_config("oci-unary", false))
                    .await;
                let streaming_result = rt
                    .exec_container_streaming(
                        container_id,
                        cgroup_probe_config("streaming", false),
                        |event| streaming_events.push(event),
                    )
                    .await;
                let pty_result = rt
                    .exec_container_streaming(
                        container_id,
                        cgroup_probe_config("pty", true),
                        |event| pty_events.push(event),
                    )
                    .await;
                let stop_result = rt.stop_container(container_id, true, None, None).await;
                let remove_result = rt.remove_container(container_id).await;
                (
                    Some(unary_result),
                    Some(streaming_result),
                    Some(pty_result),
                    Some(stop_result),
                    Some(remove_result),
                )
            }
            Err(_) => (None, None, None, None, None),
        };

    let container_id = create_result.unwrap();
    let unary = unary_result.unwrap().unwrap();
    let streaming = streaming_result.unwrap().unwrap();
    let pty = pty_result.unwrap().unwrap();
    assert!(
        stop_result.unwrap().is_ok(),
        "container cleanup stop failed for {container_id}"
    );
    assert!(
        remove_result.unwrap().is_ok(),
        "container cleanup remove failed for {container_id}"
    );

    assert_cgroup_probe("oci-unary", &unary);
    assert_cgroup_probe("streaming", &streaming);
    assert_cgroup_probe("pty", &pty);
    assert_callback_stdout_precedes_exit("streaming", &streaming_events);
    assert_callback_stdout_precedes_exit("pty", &pty_events);
}

// ── Container exec process semantics ───────────────────────────

const EXEC_SEMANTICS_IMAGE: &str = "alpine:3.20";
const EXEC_SEMANTICS_PROBE: &str = r#"/bin/busybox printf 'VZ_UID='
/bin/busybox id -u
/bin/busybox printf 'VZ_GID='
/bin/busybox id -g
/bin/busybox printf 'VZ_GROUPS='
/bin/busybox awk '$1 == "Groups:" { $1 = ""; sub(/^ /, ""); print }' /proc/self/status
/bin/busybox printf 'VZ_CWD='
/bin/busybox pwd
/bin/busybox printf 'VZ_ENV_BEGIN\n'
/bin/busybox tr '\000' '\n' < /proc/$$/environ
/bin/busybox printf 'VZ_ENV_END\n'"#;

#[derive(Debug, Clone, Copy)]
enum ExecSemanticsAdapter {
    OciUnary,
    StreamingPipe,
    Pty,
}

impl ExecSemanticsAdapter {
    const fn name(self) -> &'static str {
        match self {
            Self::OciUnary => "oci-unary",
            Self::StreamingPipe => "streaming-pipe",
            Self::Pty => "pty",
        }
    }
}

async fn exec_via_semantics_adapter(
    rt: &Runtime,
    container_id: &str,
    adapter: ExecSemanticsAdapter,
    mut config: ExecConfig,
) -> Result<vz::protocol::ExecOutput, vz_oci_macos::MacosOciError> {
    match adapter {
        ExecSemanticsAdapter::OciUnary => rt.exec_container_oci_unary(container_id, config).await,
        ExecSemanticsAdapter::StreamingPipe => {
            rt.exec_container_streaming(container_id, config, |_| {})
                .await
        }
        ExecSemanticsAdapter::Pty => {
            config.pty = true;
            config.execution_id = Some(format!("exec-semantics-{}", adapter.name()));
            rt.exec_container_streaming(container_id, config, |_| {})
                .await
        }
    }
}

fn exec_semantics_probe_config() -> ExecConfig {
    ExecConfig {
        cmd: vec![
            "/bin/busybox".to_string(),
            "sh".to_string(),
            "-c".to_string(),
            EXEC_SEMANTICS_PROBE.to_string(),
        ],
        working_dir: Some("/tmp".to_string()),
        env: vec![
            ("PATH".to_string(), "/vz/exec-semantics/bin".to_string()),
            ("TERM".to_string(), "vz-exec-semantics".to_string()),
            ("VZ_EXEC".to_string(), "from-exec".to_string()),
            ("VZ_OVERRIDE".to_string(), "from-exec".to_string()),
        ],
        user: Some("developer".to_string()),
        timeout: Some(Duration::from_secs(30)),
        ..ExecConfig::default()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ExecSemanticsEvidence {
    uid: u32,
    gid: u32,
    supplementary_groups: Vec<u32>,
    cwd: String,
    canonical_environment: Vec<u8>,
}

fn parse_exec_semantics_evidence(
    adapter: ExecSemanticsAdapter,
    output: &vz::protocol::ExecOutput,
) -> ExecSemanticsEvidence {
    assert_eq!(
        output.exit_code,
        0,
        "{} semantics probe failed: stdout={} stderr={}",
        adapter.name(),
        output.stdout,
        output.stderr
    );
    let normalized = output.stdout.replace('\r', "");
    let mut uid = None;
    let mut gid = None;
    let mut supplementary_groups = None;
    let mut cwd = None;
    let mut environment = Vec::new();
    let mut reading_environment = false;

    for line in normalized.lines() {
        match line {
            "VZ_ENV_BEGIN" => reading_environment = true,
            "VZ_ENV_END" => reading_environment = false,
            _ if reading_environment => environment.push(line.to_string()),
            _ => {
                if let Some(value) = line.strip_prefix("VZ_UID=") {
                    uid = Some(value.parse().unwrap());
                } else if let Some(value) = line.strip_prefix("VZ_GID=") {
                    gid = Some(value.parse().unwrap());
                } else if let Some(value) = line.strip_prefix("VZ_GROUPS=") {
                    supplementary_groups = Some(
                        value
                            .split_whitespace()
                            .map(|group| group.parse().unwrap())
                            .collect(),
                    );
                } else if let Some(value) = line.strip_prefix("VZ_CWD=") {
                    cwd = Some(value.to_string());
                }
            }
        }
    }

    environment.sort_unstable();
    let mut canonical_environment = environment.join("\n").into_bytes();
    canonical_environment.push(b'\n');
    ExecSemanticsEvidence {
        uid: uid.unwrap(),
        gid: gid.unwrap(),
        supplementary_groups: supplementary_groups.unwrap(),
        cwd: cwd.unwrap(),
        canonical_environment,
    }
}

/// Verify the three container exec adapters share one Docker-compatible
/// process contract for identity, environment, and working directory.
#[allow(clippy::print_stderr)]
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn container_exec_user_environment_semantics() {
    if !require_virtualization_entitlement() {
        return;
    }
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let sentinel_dir = tmp.path().join("sentinels");
    let identity_dir = tmp.path().join("identity");
    std::fs::create_dir(&sentinel_dir).unwrap();
    std::fs::create_dir(&identity_dir).unwrap();
    let passwd_file = identity_dir.join("passwd");
    let group_file = identity_dir.join("group");
    std::fs::write(
        &passwd_file,
        "root:x:0:0:root:/root:/bin/sh\ndeveloper:x:1234:2345:Developer:/tmp:/bin/sh\n",
    )
    .unwrap();
    std::fs::write(
        &group_file,
        "root:x:0:root\ndevprimary:x:2345:\ndevextra:x:3456:developer\ndevextra2:x:4567:other,developer\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&sentinel_dir, std::fs::Permissions::from_mode(0o777)).unwrap();
    }
    let rt = test_runtime(tmp.path());

    let create_result = rt
        .create_container(
            EXEC_SEMANTICS_IMAGE,
            RunConfig {
                cmd: vec!["/bin/busybox".into(), "sleep".into(), "300".into()],
                execution_mode: ExecutionMode::OciRuntime,
                env: vec![
                    ("VZ_BASE".into(), "from-create".into()),
                    ("VZ_OVERRIDE".into(), "from-create".into()),
                ],
                mounts: vec![
                    MountSpec {
                        source: Some(sentinel_dir.clone()),
                        target: "/vz-e2e".into(),
                        mount_type: MountType::Bind,
                        access: MountAccess::ReadWrite,
                        subpath: None,
                    },
                    MountSpec {
                        source: Some(passwd_file),
                        target: "/etc/passwd".into(),
                        mount_type: MountType::Bind,
                        access: MountAccess::ReadOnly,
                        subpath: Some("passwd".to_string()),
                    },
                    MountSpec {
                        source: Some(group_file),
                        target: "/etc/group".into(),
                        mount_type: MountType::Bind,
                        access: MountAccess::ReadOnly,
                        subpath: Some("group".to_string()),
                    },
                ],
                ..RunConfig::default()
            },
        )
        .await;

    let mut valid_results = Vec::new();
    let mut missing_identity_results = Vec::new();
    let mut stop_result = None;
    let mut remove_result = None;
    if let Ok(container_id) = &create_result {
        for adapter in [
            ExecSemanticsAdapter::OciUnary,
            ExecSemanticsAdapter::StreamingPipe,
            ExecSemanticsAdapter::Pty,
        ] {
            valid_results.push((
                adapter,
                exec_via_semantics_adapter(
                    &rt,
                    container_id,
                    adapter,
                    exec_semantics_probe_config(),
                )
                .await,
            ));

            let sentinel_name = format!("missing-user-{}-ran", adapter.name());
            missing_identity_results.push((
                adapter,
                sentinel_name.clone(),
                exec_via_semantics_adapter(
                    &rt,
                    container_id,
                    adapter,
                    ExecConfig {
                        cmd: vec![
                            "/bin/busybox".into(),
                            "touch".into(),
                            format!("/vz-e2e/{sentinel_name}"),
                        ],
                        user: Some("vz-user-does-not-exist".into()),
                        timeout: Some(Duration::from_secs(30)),
                        ..ExecConfig::default()
                    },
                )
                .await,
            ));
        }
        stop_result = Some(rt.stop_container(container_id, true, None, None).await);
        remove_result = Some(rt.remove_container(container_id).await);
    }

    let container_id = create_result.unwrap();
    assert!(
        stop_result.unwrap().is_ok(),
        "container cleanup stop failed for {container_id}"
    );
    assert!(
        remove_result.unwrap().is_ok(),
        "container cleanup remove failed for {container_id}"
    );

    let mut evidence = Vec::new();
    for (adapter, result) in valid_results {
        evidence.push((
            adapter,
            parse_exec_semantics_evidence(adapter, &result.unwrap()),
        ));
    }
    assert_eq!(evidence.len(), 3);
    for (adapter, actual) in &evidence {
        eprintln!(
            "container exec semantics evidence ({}): uid={} gid={} groups={:?} cwd={} environment=\n{}",
            adapter.name(),
            actual.uid,
            actual.gid,
            actual.supplementary_groups,
            actual.cwd,
            String::from_utf8_lossy(&actual.canonical_environment)
        );
        assert_eq!(actual.uid, 1234, "{} uid", adapter.name());
        assert_eq!(actual.gid, 2345, "{} gid", adapter.name());
        assert_eq!(
            actual.supplementary_groups,
            vec![2345, 3456, 4567],
            "{} supplementary groups",
            adapter.name()
        );
        assert_eq!(actual.cwd, "/tmp", "{} cwd", adapter.name());
        let expected_environment = format!(
            "PATH=/vz/exec-semantics/bin\nTERM=vz-exec-semantics\nVZ_BASE=from-create\nVZ_CONTAINER_ID={container_id}\nVZ_EXEC=from-exec\nVZ_OVERRIDE=from-exec\n"
        );
        assert_eq!(
            actual.canonical_environment,
            expected_environment.into_bytes(),
            "{} received an unexpected or leaked environment",
            adapter.name()
        );
    }
    assert_eq!(
        evidence[0].1.canonical_environment, evidence[1].1.canonical_environment,
        "unary and streaming-pipe canonical environments differ"
    );
    assert_eq!(
        evidence[0].1.canonical_environment, evidence[2].1.canonical_environment,
        "unary and PTY canonical environments differ"
    );

    for (adapter, sentinel_name, result) in missing_identity_results {
        let output = match result {
            Ok(output) => output,
            Err(error) => panic!(
                "{} identity rejection was not returned as an observable exec result: {error}",
                adapter.name()
            ),
        };
        assert_ne!(
            output.exit_code,
            0,
            "{} accepted a missing named identity",
            adapter.name()
        );
        let message = format!("{}{}", output.stdout, output.stderr);
        eprintln!(
            "container exec missing-identity evidence ({}): exit_code={} diagnostic={message:?}",
            adapter.name(),
            output.exit_code
        );
        assert!(
            message.contains("vz-user-does-not-exist") && message.contains("does not exist"),
            "{} returned an unactionable missing-user error: {message}",
            adapter.name()
        );
        assert!(
            !sentinel_dir.join(&sentinel_name).exists(),
            "{} ran the sentinel command for a missing named identity",
            adapter.name()
        );
    }
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

    // 1b. Accessor sanity: shared_vm_for must return Some for a booted
    // stack and None for an unknown one. Embedding consumers (e.g. the
    // AGENTS.jsonc broker) use this to reach `vm.vsock_listen` for
    // capability-shim installation.
    assert!(rt.shared_vm_for(stack_id).await.is_some());
    assert!(rt.shared_vm_for("not-booted").await.is_none());

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

    // 5. Retain the actual post-start hosts file before relying on it for
    // hostname connectivity. This proves the typed container exec write
    // reached the target mount namespace rather than merely exiting zero.
    let hosts_evidence = rt
        .exec_container(
            &db_id,
            ExecConfig {
                cmd: vec!["/bin/busybox".into(), "cat".into(), "/etc/hosts".into()],
                timeout: Some(Duration::from_secs(30)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();
    eprintln!("db /etc/hosts evidence:\n{}", hosts_evidence.stdout);
    assert_eq!(hosts_evidence.exit_code, 0);
    assert!(
        hosts_evidence.stdout.lines().any(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            fields == ["172.20.0.2", "web"]
        }),
        "db /etc/hosts omitted web mapping: {}",
        hosts_evidence.stdout
    );

    // 6. Exec ping by hostname: db → web.
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

    // 7. Tear down.
    let _ = rt
        .network_teardown(stack_id, vec!["web".to_string(), "db".to_string()])
        .await;
    rt.shutdown_shared_vm(stack_id).await.unwrap();
}

// ── undeclared host import isolation ────────────────────────────

/// Verify that a stack-managed container without a declared host import does
/// not receive the retired `host.vz.internal` alias.
///
/// This exercises the default-deny side of the 0.4 host-import contract through
/// a real shared VM and per-service network namespace. It intentionally does
/// not test host reachability: authenticated host-import relay behavior has not
/// landed yet, and an arbitrary NAT gateway must not be treated as authorization.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn undeclared_host_import_does_not_inject_host_vz_internal() {
    if !require_virtualization_entitlement() {
        return;
    }
    init_tracing();

    let home = std::env::var("HOME").unwrap();
    let data_dir = std::path::PathBuf::from(home).join(".vz/oci");
    std::fs::create_dir_all(&data_dir).unwrap();
    let rt = test_runtime(&data_dir);

    if rt.pull("alpine:latest").await.is_err() {
        eprintln!("WARN: pull failed (rate limit?), assuming image is cached");
    }

    let stack_id = "e2e-undeclared-host-import";

    rt.boot_shared_vm(stack_id, vec![], Default::default())
        .await
        .unwrap();

    let services = vec![vz_oci_macos::NetworkServiceConfig {
        name: "client".to_string(),
        addr: "172.20.0.2/24".to_string(),
        network_name: "default".to_string(),
    }];
    rt.network_setup(stack_id, services).await.unwrap();

    let client_id = rt
        .create_container_in_stack(
            stack_id,
            "alpine:latest",
            RunConfig {
                cmd: vec!["sleep".into(), "300".into()],
                execution_mode: ExecutionMode::OciRuntime,
                network_namespace_path: Some("/var/run/netns/client".to_string()),
                ..RunConfig::default()
            },
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("create client container failed: {e:?}"));

    let grep = rt
        .exec_container(
            &client_id,
            ExecConfig {
                cmd: vec![
                    "/bin/busybox".into(),
                    "grep".into(),
                    "host.vz.internal".into(),
                    "/etc/hosts".into(),
                ],
                timeout: Some(Duration::from_secs(10)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();

    let _ = rt
        .network_teardown(stack_id, vec!["client".to_string()])
        .await;
    rt.shutdown_shared_vm(stack_id).await.unwrap();

    assert_eq!(
        grep.exit_code, 1,
        "undeclared host import must not add host.vz.internal to /etc/hosts: stdout={} stderr={}",
        grep.stdout, grep.stderr
    );
}
