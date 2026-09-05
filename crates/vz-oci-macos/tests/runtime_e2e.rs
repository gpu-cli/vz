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

use std::io::Cursor;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::time::Duration;

use oci_distribution::Reference;
use serde_json::json;
use sha2::{Digest, Sha256};
use tar::{Builder as TarBuilder, EntryType, Header};
use vz_image::{ImageStore, parse_image_config_summary_from_store};
use vz_oci_macos::{
    ContainerReadyGeneration, ExecConfig, ExecutionMode, InteractiveExecEvent, KernelProfile,
    MacosRuntimeBackend, MountAccess, MountSpec, MountType, RunConfig, Runtime, RuntimeConfig,
    RuntimeLifecycleAdmissionEvent, RuntimeLifecycleAdmissionKind, RuntimeLifecycleObserver,
};
use vz_runtime_contract::RuntimeBackend as _;

/// Preserve the strict harness's raw stderr markers without depending on tracing.
fn write_test_stderr(arguments: std::fmt::Arguments<'_>) {
    use std::io::Write as _;

    writeln!(std::io::stderr().lock(), "{arguments}")
        .unwrap_or_else(|error| panic!("write test diagnostic to stderr: {error}"));
}

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

    write_test_stderr(format_args!(
        "VZ_E2E_REQUIRED_SKIP: runtime_e2e test binary is missing com.apple.security.virtualization entitlement; run ./scripts/run-sandbox-vm-e2e.sh --suite runtime"
    ));
    false
}

async fn container_generation_evidence(rt: &Runtime, container_id: &str) -> serde_json::Value {
    let output = rt
        .exec_container(
            container_id,
            ExecConfig {
                cmd: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "start=$(awk '{print $22}' /proc/1/stat); \
                     cgroup=$(tr '\\n' ',' </proc/1/cgroup); \
                     root=$(stat -Lc '%d:%i' /proc/1/root); \
                     printf '{\"owner\":\"%s\",\"boot_id\":\"%s\",\"init_pid\":1,\"start_time\":\"%s\",\"cgroup\":\"%s\",\"mnt_ns\":\"%s\",\"net_ns\":\"%s\",\"pid_ns\":\"%s\",\"ipc_ns\":\"%s\",\"uts_ns\":\"%s\",\"root_identity\":\"%s\"}\\n' \
                       \"$VZ_E2E_OWNER\" \"$(cat /proc/sys/kernel/random/boot_id)\" \"$start\" \"$cgroup\" \
                       \"$(readlink /proc/1/ns/mnt)\" \"$(readlink /proc/1/ns/net)\" \
                       \"$(readlink /proc/1/ns/pid)\" \"$(readlink /proc/1/ns/ipc)\" \
                       \"$(readlink /proc/1/ns/uts)\" \"$root\""
                        .into(),
                ],
                timeout: Some(Duration::from_secs(15)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap_or_else(|error| {
            panic!("capture generation evidence for '{container_id}' failed: {error}")
        });
    assert_eq!(
        output.exit_code, 0,
        "generation evidence command failed: {}",
        output.stderr
    );
    serde_json::from_str(output.stdout.trim()).unwrap_or_else(|error| {
        panic!(
            "generation evidence was not valid JSON: {error}; stdout={}",
            output.stdout
        )
    })
}

async fn guest_container_generation_evidence(
    rt: &Runtime,
    stack_id: &str,
    container_id: &str,
) -> serde_json::Value {
    let script = format!(
        r#"state=$(/run/vz-oci/bin/youki --root /run/vz-oci/state state {container_id}) || exit 1
pid=$(printf '%s\n' "$state" | sed -n 's/.*"pid"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n1)
test -n "$pid" || exit 2
start=$(awk '{{print $22}}' /proc/$pid/stat)
cgroup_path=$(sed -n 's/^[^:]*:[^:]*://p' /proc/$pid/cgroup | head -n1)
owner=$(tr '\000' '\n' < /proc/$pid/environ | sed -n 's/^VZ_E2E_OWNER=//p' | head -n1)
printf '{{"owner":"%s","guest_init_pid":%s,"start_time":"%s","cgroup_path":"%s","cgroup_identity":"%s","mnt_identity":"%s","net_identity":"%s","pid_identity":"%s","ipc_identity":"%s","uts_identity":"%s","root_identity":"%s"}}\n' \
  "$owner" "$pid" "$start" "$cgroup_path" "$(stat -Lc '%d:%i' /sys/fs/cgroup$cgroup_path)" \
  "$(stat -Lc '%d:%i' /proc/$pid/ns/mnt)" "$(stat -Lc '%d:%i' /proc/$pid/ns/net)" \
  "$(stat -Lc '%d:%i' /proc/$pid/ns/pid)" "$(stat -Lc '%d:%i' /proc/$pid/ns/ipc)" \
  "$(stat -Lc '%d:%i' /proc/$pid/ns/uts)" "$(stat -Lc '%d:%i' /proc/$pid/root)""#
    );
    let output = rt
        .exec_in_shared_vm(
            stack_id,
            "/bin/sh".into(),
            vec!["-c".into(), script],
            Duration::from_secs(15),
        )
        .await
        .unwrap_or_else(|error| panic!("guest generation probe failed: {error}"));
    assert_eq!(
        output.exit_code, 0,
        "guest generation probe failed: {}",
        output.stderr
    );
    serde_json::from_str(output.stdout.trim()).unwrap_or_else(|error| {
        panic!(
            "guest generation probe was not JSON: {error}; stdout={}",
            output.stdout
        )
    })
}

fn generation_fingerprint(evidence: &serde_json::Value) -> String {
    [
        "guest_init_pid",
        "start_time",
        "boot_id",
        "cgroup",
        "cgroup_path",
        "cgroup_identity",
        "mnt_ns",
        "mnt_identity",
        "net_ns",
        "net_identity",
        "pid_ns",
        "pid_identity",
        "ipc_ns",
        "ipc_identity",
        "uts_ns",
        "uts_identity",
        "root_identity",
    ]
    .into_iter()
    .map(|key| evidence[key].to_string())
    .collect::<Vec<_>>()
    .join("|")
}

fn ready_generation_evidence(ready: &ContainerReadyGeneration) -> serde_json::Value {
    let object = |identity: vz_oci_macos::KernelObjectIdentity| json!({"device": identity.device, "inode": identity.inode});
    json!({
        "lifecycle_generation": ready.lifecycle_generation,
        "container_id": ready.container_id,
        "init_pid": ready.init_pid,
        "init_start_time": ready.init_start_time,
        "cgroup_path": ready.cgroup_path,
        "cgroup": object(ready.cgroup),
        "namespaces": {
            "mount": object(ready.namespaces.mount),
            "network": object(ready.namespaces.network),
            "pid": object(ready.namespaces.pid),
            "ipc": object(ready.namespaces.ipc),
            "uts": object(ready.namespaces.uts),
        },
        "root": object(ready.root),
    })
}

/// Prove lazy Docker activation against one exact, vz-managed Developer Linux
/// shared VM. This intentionally stops at the guest socket boundary: a host
/// Docker proxy/context is a separate product layer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Developer Linux artifacts + Docker facade artifacts"]
async fn developer_shared_vm_docker_readiness_is_generation_fenced_and_engine_backed() {
    if !require_virtualization_entitlement() {
        return;
    }
    init_tracing();
    let temp = tempfile::tempdir().unwrap();
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: temp.path().join("runtime"),
        linux_profile: Some(KernelProfile::Developer),
        require_exact_agent_version: true,
        agent_ready_timeout: Duration::from_secs(15),
        exec_timeout: Duration::from_secs(30),
        default_memory_mb: 4096,
        ..RuntimeConfig::default()
    });
    let stack_id = "e2e-developer-docker-ready";
    runtime
        .boot_shared_vm(stack_id, vec![], Default::default())
        .await
        .unwrap();
    let identity = runtime
        .inspect_shared_vm_identity(stack_id)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("shared VM identity"));

    let stale = vz_runtime_contract::StackRuntimeIdentity::new(stack_id).unwrap();
    let stale_error = runtime
        .ensure_shared_vm_docker_ready_exact(&stale)
        .await
        .map_or_else(|error| error, |value| panic!("replacement identity must fail before Docker activation; unexpected success: {value:?}"));
    let before = runtime
        .exec_in_shared_vm(
            stack_id,
            "/bin/sh".to_string(),
            vec![
                "-c".to_string(),
                "test ! -S /run/vz-docker/docker.sock".to_string(),
            ],
            Duration::from_secs(5),
        )
        .await
        .unwrap();

    let readiness_result = tokio::time::timeout(
        Duration::from_secs(90),
        runtime.ensure_shared_vm_docker_ready_exact(&identity),
    )
    .await;
    let readiness = match readiness_result {
        Ok(Ok(readiness)) => readiness,
        failure => {
            let daemon_logs = runtime
                .exec_in_shared_vm(
                    stack_id,
                    "/bin/sh".to_string(),
                    vec![
                        "-c".to_string(),
                        "for log in /var/lib/docker/log/containerd.log /var/lib/docker/log/dockerd.log; do echo ===$log===; /bin/busybox tail -n 200 $log 2>&1 || true; done".to_string(),
                    ],
                    Duration::from_secs(10),
                )
                .await;
            let shutdown = runtime.shutdown_shared_vm(stack_id).await;
            panic!(
                "Docker readiness failed: {failure:?}; guest daemon logs: {daemon_logs:?}; shutdown: {shutdown:?}"
            );
        }
    };
    let replay = runtime
        .ensure_shared_vm_docker_ready_exact(&identity)
        .await
        .unwrap_or_else(|error| panic!("Docker readiness replay: {error:?}"));
    let current = runtime.inspect_shared_vm_identity(stack_id).await.unwrap();

    let persisted = runtime
        .exec_in_shared_vm(
            stack_id,
            "/bin/sh".to_string(),
            vec![
                "-c".to_string(),
                "printf 'machine-state-survives-reboot\\n' > /var/lib/docker/vz-e2e-marker && printf '{\"log-level\":\"warn\"}\\n' > /var/lib/docker/config/daemon.json && /bin/busybox sync".to_string(),
            ],
            Duration::from_secs(10),
        )
        .await
        .unwrap_or_else(|error| panic!("write persistent Docker state: {error:?}"));
    assert_eq!(persisted.exit_code, 0, "persist Docker state failed");

    runtime.shutdown_shared_vm(stack_id).await.unwrap();
    runtime
        .boot_shared_vm(stack_id, vec![], Default::default())
        .await
        .unwrap_or_else(|error| panic!("reboot Developer shared VM: {error:?}"));
    let reboot_identity = runtime
        .inspect_shared_vm_identity(stack_id)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("reboot shared VM identity"));
    let old_identity_error = runtime
        .ensure_shared_vm_docker_ready_exact(&identity)
        .await
        .map_or_else(|error| error, |value| panic!("old identity must not activate Docker after reboot; unexpected success: {value:?}"));
    let reboot_readiness = runtime
        .ensure_shared_vm_docker_ready_exact(&reboot_identity)
        .await
        .unwrap_or_else(|error| panic!("Docker readiness after reboot: {error:?}"));
    let persisted_probe = runtime
        .exec_in_shared_vm(
            stack_id,
            "/bin/sh".to_string(),
            vec![
                "-c".to_string(),
                "cat /var/lib/docker/vz-e2e-marker; cat /var/lib/docker/config/daemon.json"
                    .to_string(),
            ],
            Duration::from_secs(10),
        )
        .await
        .unwrap_or_else(|error| panic!("read persistent Docker state after reboot: {error:?}"));
    runtime.shutdown_shared_vm(stack_id).await.unwrap();

    assert!(matches!(
        stale_error,
        vz_oci_macos::MacosOciError::SharedRuntimeIdentityMismatch { .. }
    ));
    assert_eq!(before.exit_code, 0, "stale access started Docker");
    assert_eq!(readiness.runtime_identity, identity);
    assert_eq!(readiness.verified_profile, KernelProfile::Developer);
    assert_eq!(readiness.guest_socket_path, "/run/vz-docker/docker.sock");
    assert_eq!(replay, readiness);
    assert_eq!(current.as_ref(), Some(&identity));
    assert_ne!(reboot_identity, identity);
    assert!(matches!(
        old_identity_error,
        vz_oci_macos::MacosOciError::SharedRuntimeIdentityMismatch { .. }
    ));
    assert_eq!(reboot_readiness.runtime_identity, reboot_identity);
    assert_eq!(persisted_probe.exit_code, 0);
    assert_eq!(
        persisted_probe.stdout,
        "machine-state-survives-reboot\n{\"log-level\":\"warn\"}\n"
    );

    if let Some(path) = std::env::var_os("VZ_DOCKER_READINESS_EVIDENCE") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&json!({
                "schema_version": 1,
                "scope": "guest_only",
                "runtime_identity": identity,
                "verified_profile": "developer",
                "guest_socket_path": readiness.guest_socket_path,
                "engine_ping_proven": true,
                "stale_identity_refused_before_socket": true,
                "replay_same_identity": true,
                "reboot_runtime_identity": reboot_identity,
                "old_identity_refused_after_reboot": true,
                "persistent_machine_state_survived_reboot": true,
                "bootstrap_preserved_daemon_config": true,
                "host_socket_or_context": null,
            }))
            .unwrap(),
        )
        .unwrap();
    }
}

fn ready_matches_process_probe(
    ready: &ContainerReadyGeneration,
    probe: &serde_json::Value,
) -> bool {
    let start_time = ready.init_start_time.to_string();
    let identity =
        |value: vz_oci_macos::KernelObjectIdentity| format!("{}:{}", value.device, value.inode);
    ready.container_id == "id-serialization-e2e"
        && probe["guest_init_pid"].as_u64() == Some(u64::from(ready.init_pid))
        && probe["start_time"].as_str() == Some(start_time.as_str())
        && probe["cgroup_path"].as_str() == Some(ready.cgroup_path.as_str())
        && probe["cgroup_identity"].as_str() == Some(identity(ready.cgroup).as_str())
        && probe["mnt_identity"].as_str() == Some(identity(ready.namespaces.mount).as_str())
        && probe["net_identity"].as_str() == Some(identity(ready.namespaces.network).as_str())
        && probe["pid_identity"].as_str() == Some(identity(ready.namespaces.pid).as_str())
        && probe["ipc_identity"].as_str() == Some(identity(ready.namespaces.ipc).as_str())
        && probe["uts_identity"].as_str() == Some(identity(ready.namespaces.uts).as_str())
        && probe["root_identity"].as_str() == Some(identity(ready.root).as_str())
}

async fn capture_ready_generation(rt: &Runtime, container_id: &str) -> ContainerReadyGeneration {
    let ready = std::sync::Arc::new(std::sync::Mutex::new(None));
    let observed = std::sync::Arc::clone(&ready);
    let output = rt
        .exec_container_streaming(
            container_id,
            ExecConfig {
                cmd: vec!["/bin/true".into()],
                timeout: Some(Duration::from_secs(15)),
                ..ExecConfig::default()
            },
            move |event| {
                if let InteractiveExecEvent::ContainerReady(generation) = event {
                    *observed.lock().unwrap() = Some(generation);
                }
            },
        )
        .await
        .unwrap_or_else(|error| panic!("ready-generation exec failed: {error}"));
    assert_eq!(output.exit_code, 0, "ready-generation probe failed");
    ready
        .lock()
        .unwrap()
        .take()
        .unwrap_or_else(|| panic!("container exec omitted guest readiness generation"))
}

fn write_container_id_ownership_evidence(evidence: &serde_json::Value) {
    let rendered = serde_json::to_string_pretty(evidence).unwrap();
    write_test_stderr(format_args!(
        "VZ_CONTAINER_ID_OWNERSHIP_EVIDENCE={rendered}"
    ));
    let Ok(path) = std::env::var("VZ_CONTAINER_ID_OWNERSHIP_EVIDENCE") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, format!("{rendered}\n")).unwrap();
}

fn stable_guest_network_inventory_command() -> String {
    r#"for interface in /sys/class/net/*; do printf '%s\n' "${interface##*/}"; done | /bin/busybox sort
echo __routes__
/bin/busybox ip route show
echo __netns__
/bin/busybox ls -1 /var/run/netns 2>/dev/null | /bin/busybox sort || true"#
        .to_string()
}

async fn wait_for_guest_base_network(
    runtime: &Runtime,
    stack_id: &str,
) -> vz::protocol::ExecOutput {
    const BASE_NETWORK_READY: &str = r#"set -eu
address="$('/bin/busybox' ip -4 addr show dev eth0)"
routes="$('/bin/busybox' ip -4 route show)"
printf 'address:\n%s\nroutes:\n%s\n' "$address" "$routes"
printf '%s\n' "$address" | /bin/busybox grep -q ' inet '
printf '%s\n' "$routes" | /bin/busybox grep -Eq '^default .* dev eth0([[:space:]]|$)'"#;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let output = runtime
            .exec_in_shared_vm(
                stack_id,
                "/bin/sh".into(),
                vec!["-c".into(), BASE_NETWORK_READY.into()],
                Duration::from_secs(5),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("base-network readiness probe failed before completion: {error}")
            });
        if output.exit_code == 0 {
            return output;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "base eth0 did not acquire an IPv4 address and default route: exit={} stdout={:?} stderr={:?}",
            output.exit_code,
            output.stdout,
            output.stderr
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn standalone_duplicate_id_was_rejected(
    result: &Result<String, vz_oci_macos::MacosOciError>,
) -> bool {
    matches!(
        result,
        Err(vz_oci_macos::MacosOciError::ContainerAlreadyExists { id })
            if id == "id-serialization-e2e"
    )
}

fn backend_duplicate_id_was_rejected(
    result: &Result<String, vz_runtime_contract::RuntimeError>,
) -> bool {
    matches!(
        result,
        Err(vz_runtime_contract::RuntimeError::ContainerFailed { id, reason })
            if id == "id-serialization-e2e" && reason.contains("already owned")
    )
}

async fn expect_lifecycle_admission(
    observer: &mut RuntimeLifecycleObserver,
    kind: RuntimeLifecycleAdmissionKind,
    container_id: &str,
) -> RuntimeLifecycleAdmissionEvent {
    let event = tokio::time::timeout(Duration::from_secs(30), observer.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for lifecycle event {kind:?}"))
        .unwrap_or_else(|| panic!("lifecycle observer closed before {kind:?}"));
    assert_eq!(event.kind(), kind, "unexpected lifecycle admission event");
    assert_eq!(event.container_id(), container_id);
    event
}

// ── Smoke test: pull + run ──────────────────────────────────────

/// Pull pinned Alpine and run `echo hello` via one-shot `Runtime::run()`.
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
    let image_id = rt.pull("alpine:3.20").await.unwrap();
    assert!(
        !image_id.0.is_empty(),
        "image ID should be non-empty after pull"
    );

    // Run `echo hello` via GuestExec mode with serial log for diagnostics.
    let serial_log = tmp.path().join("serial.log");
    let output = rt
        .run(
            "alpine:3.20",
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
            write_test_stderr(format_args!(
                "=== Serial log ===\n{log}\n=== End serial log ==="
            ));
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
            "alpine:3.20",
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
            "alpine:3.20",
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
            "alpine:3.20",
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
            "alpine:3.20",
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
            "alpine:3.20",
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

/// Prove caller-selected IDs have one lifecycle owner across standalone and
/// shared-VM stack paths, including setup failure and stop/remove/recreate.
///
/// The exec/recreate phase waits for the guest-originated post-pin readiness
/// acknowledgement, then uses an in-container gate to fix the remaining
/// schedule. The retained raw `/proc/1` identity proves that the admitted exec
/// cannot cross to the replacement generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn container_id_lifecycle_serialization_and_generation_ownership() {
    if !require_virtualization_entitlement() {
        return;
    }
    init_tracing();

    const IMAGE: &str = "alpine:3.20";
    const CONTAINER_ID: &str = "id-serialization-e2e";
    const STACK_ID: &str = "id-owner-stack";
    const SERVICE_NAME: &str = "owner";

    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());
    rt.pull(IMAGE).await.unwrap();

    // Standalone: a live caller-selected ID rejects a duplicate without
    // changing its process defaults, metadata, or guest generation.
    let standalone_config = |owner: &str| RunConfig {
        cmd: vec!["sleep".into(), "300".into()],
        env: vec![("VZ_E2E_OWNER".into(), owner.into())],
        execution_mode: ExecutionMode::OciRuntime,
        container_id: Some(CONTAINER_ID.into()),
        ..RunConfig::default()
    };
    let mut standalone_admissions = rt.install_lifecycle_observer();
    let first_runtime = rt.clone();
    let first_standalone = tokio::spawn(async move {
        first_runtime
            .create_container(IMAGE, standalone_config("standalone-a"))
            .await
    });
    let first_create_admission =
        tokio::time::timeout(Duration::from_secs(10), standalone_admissions.recv())
            .await
            .unwrap_or_else(|error| {
                panic!("first standalone create never reached ID admission: {error:?}")
            })
            .unwrap_or_else(|| panic!("standalone lifecycle observer closed unexpectedly"));
    assert_eq!(
        first_create_admission.kind(),
        RuntimeLifecycleAdmissionKind::CreateBeforeReservation
    );
    assert_eq!(first_create_admission.container_id(), CONTAINER_ID);
    let in_flight_standalone_duplicate = tokio::time::timeout(
        Duration::from_secs(10),
        rt.create_container(IMAGE, standalone_config("standalone-duplicate")),
    )
    .await
    .unwrap_or_else(|error| {
        panic!("concurrent standalone duplicate queued instead of failing closed: {error:?}")
    });
    let in_flight_standalone_duplicate_rejected =
        standalone_duplicate_id_was_rejected(&in_flight_standalone_duplicate);
    first_create_admission.resume();
    let first_create_reserved = expect_lifecycle_admission(
        &mut standalone_admissions,
        RuntimeLifecycleAdmissionKind::CreateAfterReservation,
        CONTAINER_ID,
    )
    .await;
    first_create_reserved.resume();
    let first_standalone_id = tokio::time::timeout(Duration::from_secs(120), first_standalone)
        .await
        .unwrap_or_else(|error| {
            panic!("first standalone create timed out after admission release: {error:?}")
        })
        .unwrap_or_else(|error| panic!("first standalone create task panicked: {error:?}"))
        .unwrap_or_else(|error| panic!("first standalone create failed: {error:?}"));
    assert_eq!(first_standalone_id, CONTAINER_ID);
    drop(standalone_admissions);

    let standalone_a = container_generation_evidence(&rt, CONTAINER_ID).await;
    let active_standalone_duplicate = tokio::time::timeout(
        Duration::from_secs(10),
        rt.create_container(IMAGE, standalone_config("standalone-active-duplicate")),
    )
    .await
    .unwrap_or_else(|error| {
        panic!("active standalone duplicate queued instead of failing closed: {error:?}")
    });
    let active_standalone_duplicate_rejected =
        standalone_duplicate_id_was_rejected(&active_standalone_duplicate);
    let standalone_after_duplicate = container_generation_evidence(&rt, CONTAINER_ID).await;
    assert_eq!(
        standalone_after_duplicate["owner"], "standalone-a",
        "duplicate create overwrote the original standalone owner"
    );
    assert_eq!(
        generation_fingerprint(&standalone_after_duplicate),
        generation_fingerprint(&standalone_a),
        "duplicate create changed the original standalone generation"
    );
    assert_eq!(
        rt.list_containers()
            .unwrap()
            .iter()
            .filter(|container| container.id == CONTAINER_ID)
            .count(),
        1,
        "standalone ID must have exactly one metadata record"
    );

    rt.stop_container(CONTAINER_ID, true, None, None)
        .await
        .unwrap();
    rt.remove_container(CONTAINER_ID).await.unwrap();
    rt.create_container(IMAGE, standalone_config("standalone-b"))
        .await
        .unwrap();
    let standalone_b = container_generation_evidence(&rt, CONTAINER_ID).await;
    assert_eq!(standalone_b["owner"], "standalone-b");
    assert_ne!(
        generation_fingerprint(&standalone_a),
        generation_fingerprint(&standalone_b),
        "standalone recreate must produce a distinct raw guest generation"
    );
    rt.stop_container(CONTAINER_ID, true, None, None)
        .await
        .unwrap();
    rt.remove_container(CONTAINER_ID).await.unwrap();

    // Stack: enter setup deterministically, then issue a duplicate while the
    // complete contract-backend create transaction is still in progress.
    rt.boot_shared_vm(STACK_ID, vec![], Default::default())
        .await
        .unwrap();
    // Guest init starts DHCP asynchronously. Wait for the host-provided base
    // network to converge before snapshotting it, so the cleanup oracle only
    // compares topology owned by this test rather than DHCP timing.
    let base_network_precondition = wait_for_guest_base_network(&rt, STACK_ID).await;
    let baseline_network_inventory = rt
        .exec_in_shared_vm(
            STACK_ID,
            "/bin/sh".into(),
            vec!["-c".into(), stable_guest_network_inventory_command()],
            Duration::from_secs(15),
        )
        .await
        .unwrap();
    assert_eq!(baseline_network_inventory.exit_code, 0);
    rt.network_setup(
        STACK_ID,
        vec![vz_oci_macos::NetworkServiceConfig {
            name: SERVICE_NAME.into(),
            addr: "172.31.73.2/24".into(),
            network_name: "default".into(),
        }],
    )
    .await
    .unwrap();

    let backend = std::sync::Arc::new(MacosRuntimeBackend::new(rt.clone()));
    let failed_setup_commands = vec![
        "rm -f /setup-release; mkfifo /setup-release; printf 'entered\\n' > /setup-entered"
            .to_string(),
        "read _ < /setup-release; exit 37".to_string(),
    ];
    let failed_setup_ref = Runtime::setup_commit_reference(IMAGE, &failed_setup_commands);
    let first_backend = std::sync::Arc::clone(&backend);
    let first_commands = failed_setup_commands.clone();
    let first_create = tokio::spawn(async move {
        first_backend
            .create_container_in_stack(
                STACK_ID,
                IMAGE,
                vz_runtime_contract::RunConfig {
                    cmd: vec!["sleep".into(), "300".into()],
                    env: vec![("VZ_E2E_OWNER".into(), "stack-setup-failing".into())],
                    container_id: Some(CONTAINER_ID.into()),
                    network_namespace_path: Some(format!("/var/run/netns/{SERVICE_NAME}")),
                    setup_commands: first_commands,
                    ..vz_runtime_contract::RunConfig::default()
                },
            )
            .await
    });

    let setup_overlay = format!("/run/vz-oci/containers/{CONTAINER_ID}/merged");
    let entered = rt
        .exec_in_shared_vm(
            STACK_ID,
            "/bin/sh".into(),
            vec![
                "-c".into(),
                format!(
                    "while [ ! -e {setup_overlay}/setup-entered ]; do :; done; test -e {setup_overlay}/setup-entered"
                ),
            ],
            Duration::from_secs(90),
        )
        .await
        .unwrap_or_else(|error| panic!("first stack create never entered its gated setup transaction: {error:?}"));
    assert_eq!(entered.exit_code, 0, "setup entry observer failed");
    let failed_stack_generation =
        guest_container_generation_evidence(&rt, STACK_ID, CONTAINER_ID).await;

    let duplicate_backend = std::sync::Arc::clone(&backend);
    let duplicate_create = async move {
        duplicate_backend
            .create_container_in_stack(
                STACK_ID,
                IMAGE,
                vz_runtime_contract::RunConfig {
                    cmd: vec!["sleep".into(), "300".into()],
                    env: vec![("VZ_E2E_OWNER".into(), "stack-duplicate".into())],
                    container_id: Some(CONTAINER_ID.into()),
                    network_namespace_path: Some(format!("/var/run/netns/{SERVICE_NAME}")),
                    setup_commands: vec!["printf duplicate > /duplicate-setup-ran".into()],
                    ..vz_runtime_contract::RunConfig::default()
                },
            )
            .await
    };
    tokio::pin!(duplicate_create);

    // Admission must fail closed while the first transaction owns the ID; it
    // must not wait and become a surprise create after the first rolls back.
    let duplicate_result = tokio::time::timeout(Duration::from_secs(10), &mut duplicate_create)
        .await
        .unwrap_or_else(|error| {
            panic!("duplicate stack create queued instead of failing closed: {error:?}")
        });
    let duplicate_completed_before_release = true;
    let loser_probe = rt
        .exec_in_shared_vm(
            STACK_ID,
            "/bin/sh".into(),
            vec![
                "-c".into(),
                format!("test ! -e {setup_overlay}/duplicate-setup-ran"),
            ],
            Duration::from_secs(10),
        )
        .await
        .unwrap_or_else(|error| panic!("failed to inspect duplicate setup marker: {error:?}"));
    let loser_setup_absent = loser_probe.exit_code == 0;

    let release = rt
        .exec_in_shared_vm(
            STACK_ID,
            "/bin/sh".into(),
            vec![
                "-c".into(),
                format!("printf 'release\\n' > {setup_overlay}/setup-release"),
            ],
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert_eq!(release.exit_code, 0, "failed to release setup gate");
    let first_result = tokio::time::timeout(Duration::from_secs(90), first_create)
        .await
        .unwrap_or_else(|error| {
            panic!("first stack create did not finish after setup gate release: {error:?}")
        })
        .unwrap_or_else(|error| panic!("first stack create task panicked: {error:?}"));
    if duplicate_result.is_ok() {
        let _ = rt.stop_container(CONTAINER_ID, true, None, None).await;
        let _ = rt.remove_container(CONTAINER_ID).await;
    }

    let failed_cgroup_path = failed_stack_generation["cgroup_path"]
        .as_str()
        .unwrap_or_default();
    let failed_guest_inventory = rt
        .exec_in_shared_vm(
            STACK_ID,
            "/bin/sh".into(),
            vec![
                "-c".into(),
                format!(
                    "printf 'overlay='; test -e /run/vz-oci/containers/{CONTAINER_ID} && echo present || echo absent; \
                     printf 'youki_state='; test -e /run/vz-oci/state/{CONTAINER_ID} && echo present || echo absent; \
                     printf 'cgroup='; test -e /sys/fs/cgroup{failed_cgroup_path} && echo present || echo absent"
                ),
            ],
            Duration::from_secs(15),
        )
        .await
        .unwrap_or_else(|error| panic!("failed to inspect failed-setup guest cleanup: {error:?}"));
    let failed_setup_diagnostics = rt.lifecycle_diagnostics().await.unwrap();
    let failed_generation_released = failed_setup_diagnostics
        .generations
        .iter()
        .any(|entry| entry.container_id == CONTAINER_ID && !entry.reserved);
    let failed_host_maps_clean = failed_setup_diagnostics.vm_handles == 0
        && failed_setup_diagnostics.container_routes == 0
        && failed_setup_diagnostics.exec_bindings == 0
        && failed_setup_diagnostics.active_lifecycles == 0
        && failed_setup_diagnostics.exec_sessions == 0
        && failed_setup_diagnostics.setup_restore_entries == 0
        && failed_setup_diagnostics.rootfs_directories == 0
        && failed_setup_diagnostics.overlay_cleanup_pending == 0;
    let failed_guest_resources_clean = failed_guest_inventory.stdout.contains("overlay=absent")
        && failed_guest_inventory.stdout.contains("youki_state=absent")
        && failed_guest_inventory.stdout.contains("cgroup=absent");
    let failed_setup_clean = rt
        .list_containers()
        .unwrap()
        .iter()
        .all(|container| container.id != CONTAINER_ID)
        && !tmp.path().join("rootfs").join(CONTAINER_ID).exists()
        && failed_generation_released
        && failed_host_maps_clean
        && failed_guest_resources_clean;
    let failed_setup_commit_absent = !rt
        .setup_commits_host_dir()
        .join(format!("{failed_setup_ref}.tar"))
        .exists();

    // A later explicit recreate is valid only after setup failure cleanup has
    // completed. Its setup commit must be fully published, never left as .tmp.
    let stack_a_setup = vec!["printf 'stack-a\\n' > /setup-owner".to_string()];
    let stack_a_ref = Runtime::setup_commit_reference(IMAGE, &stack_a_setup);
    backend
        .create_container_in_stack(
            STACK_ID,
            IMAGE,
            vz_runtime_contract::RunConfig {
                cmd: vec!["sleep".into(), "300".into()],
                env: vec![("VZ_E2E_OWNER".into(), "stack-a".into())],
                container_id: Some(CONTAINER_ID.into()),
                network_namespace_path: Some(format!("/var/run/netns/{SERVICE_NAME}")),
                setup_commands: stack_a_setup,
                ..vz_runtime_contract::RunConfig::default()
            },
        )
        .await
        .unwrap();
    let stack_a = guest_container_generation_evidence(&rt, STACK_ID, CONTAINER_ID).await;
    let successful_setup_commit_present = rt
        .setup_commits_host_dir()
        .join(format!("{stack_a_ref}.tar"))
        .is_file();

    // Fixed schedule: pause exec after it owns read admission but before its
    // guest RPC, request the complete lifecycle replacement, then prove the
    // writer cannot acquire until the guest-originated post-execve proof.
    let mut lifecycle_observer = rt.install_lifecycle_observer();
    let exec_runtime = rt.clone();
    let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
    let (stdout_sender, stdout_receiver) = tokio::sync::oneshot::channel();
    let streamed_stdout = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed_stdout = std::sync::Arc::clone(&streamed_stdout);
    let exec_during_recreate = tokio::spawn(async move {
        let mut ready_sender = Some(ready_sender);
        let mut stdout_sender = Some(stdout_sender);
        exec_runtime
            .exec_container_streaming(
                CONTAINER_ID,
                ExecConfig {
                    execution_id: Some("id-serialization-old-exec".into()),
                    pty: true,
                    cmd: vec![
                        "/bin/sh".into(),
                        "-c".into(),
                        "start=$(awk '{print $22}' /proc/1/stat); \
                         touch /exec-entered; \
                         printf 'pinned-owner=%s start=%s mnt=%s net=%s\\n' \
                           \"$VZ_E2E_OWNER\" \"$start\" \
                           \"$(readlink /proc/1/ns/mnt)\" \"$(readlink /proc/1/ns/net)\"; \
                         read _; \
                         printf 'finished-owner=%s\\n' \"$VZ_E2E_OWNER\""
                            .into(),
                    ],
                    timeout: Some(Duration::from_secs(90)),
                    ..ExecConfig::default()
                },
                move |event| match event {
                    InteractiveExecEvent::ContainerReady(generation) => {
                        if let Some(sender) = ready_sender.take() {
                            let _ = sender.send(generation);
                        }
                    }
                    InteractiveExecEvent::Stdout(bytes) => {
                        let mut stdout = observed_stdout.lock().unwrap();
                        stdout.extend(bytes);
                        if stdout
                            .windows(b"pinned-owner=stack-a".len())
                            .any(|window| window == b"pinned-owner=stack-a")
                        {
                            if let Some(sender) = stdout_sender.take() {
                                let _ = sender.send(());
                            }
                        }
                    }
                    _ => {}
                },
            )
            .await
    });

    let exec_before_guest = expect_lifecycle_admission(
        &mut lifecycle_observer,
        RuntimeLifecycleAdmissionKind::ExecBeforeGuestRpc,
        CONTAINER_ID,
    )
    .await;
    let lifecycle_runtime = rt.clone();
    let replacement = async move {
        lifecycle_runtime
            .stop_container(CONTAINER_ID, true, None, None)
            .await?;
        lifecycle_runtime.remove_container(CONTAINER_ID).await?;
        lifecycle_runtime
            .create_container_in_stack(
                STACK_ID,
                IMAGE,
                RunConfig {
                    cmd: vec!["sleep".into(), "300".into()],
                    env: vec![("VZ_E2E_OWNER".into(), "stack-b".into())],
                    execution_mode: ExecutionMode::OciRuntime,
                    container_id: Some(CONTAINER_ID.into()),
                    network_namespace_path: Some(format!("/var/run/netns/{SERVICE_NAME}")),
                    ..RunConfig::default()
                },
                None,
            )
            .await
    };
    tokio::pin!(replacement);
    let stop_requested = tokio::select! {
        event = expect_lifecycle_admission(
            &mut lifecycle_observer,
            RuntimeLifecycleAdmissionKind::StopWriterRequested,
            CONTAINER_ID,
        ) => event,
        result = &mut replacement => panic!("replacement completed before stop request was observed: {result:?}"),
    };
    stop_requested.resume();
    exec_before_guest.resume();

    let guest_rpc_ready = tokio::select! {
        event = expect_lifecycle_admission(
            &mut lifecycle_observer,
            RuntimeLifecycleAdmissionKind::ExecGuestRpcReadyBeforeOwner,
            CONTAINER_ID,
        ) => event,
        result = &mut replacement => panic!("replacement crossed exec admission before guest RPC readiness: {result:?}"),
    };
    guest_rpc_ready.resume();

    // If stop bypasses exec's read admission, StopWriterAcquired arrives here
    // and this exact event-order assertion fails.
    let guest_ready_boundary = tokio::select! {
        event = expect_lifecycle_admission(
            &mut lifecycle_observer,
            RuntimeLifecycleAdmissionKind::ExecGuestReady,
            CONTAINER_ID,
        ) => event,
        result = &mut replacement => panic!("replacement crossed exec admission before guest readiness: {result:?}"),
    };
    let exec_marker = rt
        .exec_in_shared_vm(
            STACK_ID,
            "/bin/sh".into(),
            vec![
                "-c".into(),
                format!(
                    "while [ ! -e /run/vz-oci/containers/{CONTAINER_ID}/merged/exec-entered ]; do :; done"
                ),
            ],
            Duration::from_secs(30),
        )
        .await
        .unwrap_or_else(|error| panic!("generation-A exec did not reach its command gate: {error:?}"));
    assert_eq!(exec_marker.exit_code, 0);
    guest_ready_boundary.resume();

    let stack_a_ready = tokio::time::timeout(Duration::from_secs(30), ready_receiver)
        .await
        .unwrap_or_else(|error| {
            panic!("exec never published guest target-ready acknowledgement: {error:?}")
        })
        .unwrap_or_else(|error| {
            panic!("exec ended before reporting its pinned generation: {error:?}")
        });
    let stop_acquired = tokio::select! {
        event = expect_lifecycle_admission(
            &mut lifecycle_observer,
            RuntimeLifecycleAdmissionKind::StopWriterAcquired,
            CONTAINER_ID,
        ) => event,
        result = &mut replacement => panic!("replacement completed before stop writer admission: {result:?}"),
    };
    tokio::time::timeout(Duration::from_secs(30), stdout_receiver)
        .await
        .unwrap_or_else(|error| {
            panic!("generation-A exec did not emit its owner sentinel: {error:?}")
        })
        .unwrap_or_else(|error| panic!("generation-A stdout observer closed: {error:?}"));
    let ready_a_matches_probe = ready_matches_process_probe(&stack_a_ready, &stack_a);
    stop_acquired.resume();

    let remove_acquired = tokio::select! {
        event = expect_lifecycle_admission(
            &mut lifecycle_observer,
            RuntimeLifecycleAdmissionKind::RemoveWriterAcquired,
            CONTAINER_ID,
        ) => event,
        result = &mut replacement => panic!("replacement completed before remove writer admission: {result:?}"),
    };
    remove_acquired.resume();
    let recreate_admitted = tokio::select! {
        event = expect_lifecycle_admission(
            &mut lifecycle_observer,
            RuntimeLifecycleAdmissionKind::CreateBeforeReservation,
            CONTAINER_ID,
        ) => event,
        result = &mut replacement => panic!("replacement completed before recreate admission: {result:?}"),
    };
    recreate_admitted.resume();
    let recreate_reserved = tokio::select! {
        event = expect_lifecycle_admission(
            &mut lifecycle_observer,
            RuntimeLifecycleAdmissionKind::CreateAfterReservation,
            CONTAINER_ID,
        ) => event,
        result = &mut replacement => panic!("replacement completed before recreate reservation: {result:?}"),
    };
    recreate_reserved.resume();
    let recovery_route_published = tokio::select! {
        event = expect_lifecycle_admission(
            &mut lifecycle_observer,
            RuntimeLifecycleAdmissionKind::StackRoutePublishedBeforeOverlay,
            CONTAINER_ID,
        ) => event,
        result = &mut replacement => panic!("replacement completed before recovery route publication: {result:?}"),
    };
    recovery_route_published.resume();
    let overlay_setup_starting = tokio::select! {
        event = expect_lifecycle_admission(
            &mut lifecycle_observer,
            RuntimeLifecycleAdmissionKind::StackOverlaySetupStarting,
            CONTAINER_ID,
        ) => event,
        result = &mut replacement => panic!("replacement completed before overlay setup admission: {result:?}"),
    };
    overlay_setup_starting.resume();
    let replacement_id = tokio::time::timeout(Duration::from_secs(120), &mut replacement)
        .await
        .unwrap_or_else(|error| panic!("stop/remove/recreate transaction timed out: {error:?}"))
        .unwrap_or_else(|error| panic!("stop/remove/recreate transaction failed: {error:?}"));
    assert_eq!(replacement_id, CONTAINER_ID);
    drop(lifecycle_observer);

    // If the old pinned exec survived OCI deletion, release it only after B is
    // active. If stop terminated it, session-not-found is the allowed stale-A
    // outcome; either way it must never be routed into B.
    let _ = rt
        .write_exec_stdin("id-serialization-old-exec", b"release\n")
        .await;
    let raced_exec = tokio::time::timeout(Duration::from_secs(30), exec_during_recreate)
        .await
        .unwrap_or_else(|error| {
            panic!("old generation exec did not terminate after recreate: {error:?}")
        })
        .unwrap_or_else(|error| panic!("old generation exec task panicked: {error:?}"));
    let stack_b = guest_container_generation_evidence(&rt, STACK_ID, CONTAINER_ID).await;
    let stack_b_ready = capture_ready_generation(&rt, CONTAINER_ID).await;
    let ready_b_matches_probe = ready_matches_process_probe(&stack_b_ready, &stack_b);
    let raced_stdout = String::from_utf8_lossy(&streamed_stdout.lock().unwrap()).into_owned();
    let exec_did_not_cross_generation =
        raced_stdout.contains("pinned-owner=stack-a") && !raced_stdout.contains("stack-b");
    let ready_generations_distinct = stack_b_ready.lifecycle_generation
        > stack_a_ready.lifecycle_generation
        && (stack_b_ready.init_start_time != stack_a_ready.init_start_time
            || stack_b_ready.cgroup != stack_a_ready.cgroup
            || stack_b_ready.namespaces != stack_a_ready.namespaces
            || stack_b_ready.root != stack_a_ready.root);

    rt.stop_container(CONTAINER_ID, true, None, None)
        .await
        .unwrap();
    rt.remove_container(CONTAINER_ID).await.unwrap();
    let stale_exec = rt
        .exec_container(
            CONTAINER_ID,
            ExecConfig {
                cmd: vec!["/bin/true".into()],
                ..ExecConfig::default()
            },
        )
        .await;
    let stale_exec_rejected = matches!(
        &stale_exec,
        Err(vz_oci_macos::MacosOciError::ContainerNotFound { id }) if id == CONTAINER_ID
    );
    rt.network_teardown(STACK_ID, vec![SERVICE_NAME.into()])
        .await
        .unwrap();
    let cgroup_a_path = stack_a["cgroup_path"].as_str().unwrap_or_default();
    let cgroup_b_path = stack_b["cgroup_path"].as_str().unwrap_or_default();
    let guest_inventory = rt
        .exec_in_shared_vm(
            STACK_ID,
            "/bin/sh".into(),
            vec![
                "-c".into(),
                format!(
                    "printf 'overlay='; test -e /run/vz-oci/containers/{CONTAINER_ID} && echo present || echo absent; \
                     printf 'service_netns='; test -e /var/run/netns/{SERVICE_NAME} && echo present || echo absent; \
                     printf 'youki_state='; test -e /run/vz-oci/state/{CONTAINER_ID} && echo present || echo absent; \
                     printf 'cgroup_a='; test -e /sys/fs/cgroup{cgroup_a_path} && echo present || echo absent; \
                     printf 'cgroup_b='; test -e /sys/fs/cgroup{cgroup_b_path} && echo present || echo absent"
                ),
            ],
            Duration::from_secs(15),
        )
        .await
        .unwrap();
    let final_network_inventory = rt
        .exec_in_shared_vm(
            STACK_ID,
            "/bin/sh".into(),
            vec!["-c".into(), stable_guest_network_inventory_command()],
            Duration::from_secs(15),
        )
        .await
        .unwrap();
    assert_eq!(final_network_inventory.exit_code, 0);
    let guest_resources_clean = guest_inventory.exit_code == 0
        && guest_inventory.stdout.contains("overlay=absent")
        && guest_inventory.stdout.contains("service_netns=absent")
        && guest_inventory.stdout.contains("youki_state=absent")
        && guest_inventory.stdout.contains("cgroup_a=absent")
        && guest_inventory.stdout.contains("cgroup_b=absent")
        && final_network_inventory.stdout.trim() == baseline_network_inventory.stdout.trim();
    let lifecycle_diagnostics = rt.lifecycle_diagnostics().await.unwrap();
    let generation_released = lifecycle_diagnostics
        .generations
        .iter()
        .any(|entry| entry.container_id == CONTAINER_ID && !entry.reserved);
    let host_maps_clean = lifecycle_diagnostics.vm_handles == 0
        && lifecycle_diagnostics.container_routes == 0
        && lifecycle_diagnostics.exec_bindings == 0
        && lifecycle_diagnostics.active_lifecycles == 0
        && lifecycle_diagnostics.exec_sessions == 0
        && lifecycle_diagnostics.setup_restore_entries == 0
        && lifecycle_diagnostics.rootfs_directories == 0
        && lifecycle_diagnostics.overlay_cleanup_pending == 0;
    rt.shutdown_shared_vm(STACK_ID).await.unwrap();

    let orphan_setup_tmp: Vec<String> = std::fs::read_dir(rt.setup_commits_host_dir())
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            name.ends_with(".tmp").then_some(name)
        })
        .collect();
    let metadata_absent = rt
        .list_containers()
        .unwrap()
        .iter()
        .all(|container| container.id != CONTAINER_ID);
    let rootfs_absent = !tmp.path().join("rootfs").join(CONTAINER_ID).exists();
    let shared_vm_absent = !rt.has_shared_vm(STACK_ID).await;

    let evidence = json!({
        "schema_version": 1,
        "scenario": "runtime-container-id-ownership",
        "container_id": CONTAINER_ID,
        "standalone": {
            "in_flight_duplicate_rejected": in_flight_standalone_duplicate_rejected,
            "active_duplicate_rejected": active_standalone_duplicate_rejected,
            "generation_a": standalone_a.clone(),
            "generation_b": standalone_b.clone(),
        },
        "stack": {
            "duplicate_rejected_before_release": duplicate_completed_before_release
                && backend_duplicate_id_was_rejected(&duplicate_result),
            "loser_setup_absent": loser_setup_absent,
            "failed_setup_returned_error": first_result.is_err(),
            "failed_setup_clean": failed_setup_clean,
            "failed_generation": failed_stack_generation,
            "failed_generation_released": failed_generation_released,
            "failed_guest_resources_clean": failed_guest_resources_clean,
            "failed_guest_inventory": failed_guest_inventory.stdout,
            "failed_host_maps_clean": failed_host_maps_clean,
            "failed_lifecycle_diagnostics": format!("{failed_setup_diagnostics:?}"),
            "failed_setup_commit_absent": failed_setup_commit_absent,
            "successful_setup_commit_present": successful_setup_commit_present,
            "generation_a": stack_a.clone(),
            "generation_b": stack_b.clone(),
            "ready_generation_a": ready_generation_evidence(&stack_a_ready),
            "ready_generation_b": ready_generation_evidence(&stack_b_ready),
            "ready_a_matches_process_probe": ready_a_matches_probe,
            "ready_b_matches_process_probe": ready_b_matches_probe,
            "ready_generations_distinct": ready_generations_distinct,
            "raced_exec_result": raced_exec.as_ref().map(|output| json!({
                "exit_code": output.exit_code,
                "stdout": output.stdout,
                "stderr": output.stderr,
            })).unwrap_or_else(|error| json!({"error": error.to_string()})),
            "exec_did_not_cross_generation": exec_did_not_cross_generation,
        },
        "final": {
            "metadata_absent": metadata_absent,
            "rootfs_absent": rootfs_absent,
            "shared_vm_absent": shared_vm_absent,
            "guest_resources_clean": guest_resources_clean,
            "stale_exec_rejected": stale_exec_rejected,
            "generation_released": generation_released,
            "host_maps_clean": host_maps_clean,
            "lifecycle_diagnostics": format!("{lifecycle_diagnostics:?}"),
            "base_network_precondition": {
                "exit_code": base_network_precondition.exit_code,
                "stdout": base_network_precondition.stdout,
                "stderr": base_network_precondition.stderr,
            },
            "guest_inventory": guest_inventory.stdout,
            "guest_inventory_exit_code": guest_inventory.exit_code,
            "guest_inventory_stderr": guest_inventory.stderr,
            "baseline_network_inventory": baseline_network_inventory.stdout,
            "baseline_network_inventory_exit_code": baseline_network_inventory.exit_code,
            "baseline_network_inventory_stderr": baseline_network_inventory.stderr,
            "final_network_inventory": final_network_inventory.stdout,
            "final_network_inventory_exit_code": final_network_inventory.exit_code,
            "final_network_inventory_stderr": final_network_inventory.stderr,
            "orphan_setup_tmp": orphan_setup_tmp.clone(),
        },
    });
    write_container_id_ownership_evidence(&evidence);

    assert!(
        in_flight_standalone_duplicate_rejected,
        "in-flight standalone duplicate did not fail closed: {in_flight_standalone_duplicate:?}"
    );
    assert!(
        active_standalone_duplicate_rejected,
        "active standalone duplicate did not fail closed: {active_standalone_duplicate:?}"
    );
    assert!(
        evidence["stack"]["duplicate_rejected_before_release"] == true,
        "stack duplicate must fail before the owning setup transaction is released: {duplicate_result:?}"
    );
    assert!(first_result.is_err(), "gated setup was expected to fail");
    assert!(
        loser_setup_absent,
        "duplicate setup command reached the winner"
    );
    assert!(failed_setup_clean, "failed setup leaked metadata or rootfs");
    assert!(
        failed_setup_commit_absent,
        "failed setup published a commit"
    );
    assert!(
        successful_setup_commit_present,
        "successful setup did not atomically publish its commit"
    );
    assert_eq!(stack_a["owner"], "stack-a");
    assert_eq!(stack_b["owner"], "stack-b");
    assert_ne!(
        generation_fingerprint(&stack_a),
        generation_fingerprint(&stack_b),
        "stack recreate must produce a distinct raw guest generation"
    );
    assert!(
        ready_a_matches_probe,
        "guest readiness A did not match its independent /proc probe: {stack_a_ready:?}"
    );
    assert!(
        ready_b_matches_probe,
        "guest readiness B did not match its independent /proc probe: {stack_b_ready:?}"
    );
    assert!(
        ready_generations_distinct,
        "same-ID recreate did not advance lifecycle/raw guest generation: A={stack_a_ready:?} B={stack_b_ready:?}"
    );
    assert!(
        exec_did_not_cross_generation,
        "generation-A exec crossed into replacement generation B: {raced_stdout}"
    );
    assert!(metadata_absent, "final metadata record leaked");
    assert!(rootfs_absent, "final rootfs leaked");
    assert!(shared_vm_absent, "shared VM leaked after shutdown");
    assert!(
        stale_exec_rejected,
        "removed ID did not return its exact not-found result: {stale_exec:?}"
    );
    assert!(generation_released, "durable generation remained reserved");
    assert!(
        host_maps_clean,
        "host lifecycle maps leaked: {lifecycle_diagnostics:?}"
    );
    assert!(
        guest_resources_clean,
        "guest cleanup mismatch: inventory(exit={} stdout={:?} stderr={:?}); baseline(exit={} stdout={:?} stderr={:?}); final(exit={} stdout={:?} stderr={:?})",
        guest_inventory.exit_code,
        guest_inventory.stdout,
        guest_inventory.stderr,
        baseline_network_inventory.exit_code,
        baseline_network_inventory.stdout,
        baseline_network_inventory.stderr,
        final_network_inventory.exit_code,
        final_network_inventory.stdout,
        final_network_inventory.stderr,
    );
    assert!(
        orphan_setup_tmp.is_empty(),
        "orphan setup commit temp files leaked"
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
            "alpine:3.20",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecSupervisionAdapter {
    Unary,
    Streaming,
    Pty,
}

impl ExecSupervisionAdapter {
    const ALL: [Self; 3] = [Self::Unary, Self::Streaming, Self::Pty];

    const fn name(self) -> &'static str {
        match self {
            Self::Unary => "unary",
            Self::Streaming => "streaming",
            Self::Pty => "pty",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecSupervisionTermination {
    Term,
    Int,
    Kill,
    Timeout,
}

impl ExecSupervisionTermination {
    const ALL: [Self; 4] = [Self::Term, Self::Int, Self::Kill, Self::Timeout];

    const fn name(self) -> &'static str {
        match self {
            Self::Term => "term",
            Self::Int => "int",
            Self::Kill => "kill",
            Self::Timeout => "timeout",
        }
    }

    const fn signal(self) -> Option<&'static str> {
        match self {
            Self::Term => Some("SIGTERM"),
            Self::Int => Some("SIGINT"),
            Self::Kill => Some("SIGKILL"),
            Self::Timeout => None,
        }
    }

    const fn expected_exit_code(self) -> Option<i32> {
        match self {
            Self::Term => Some(143),
            Self::Int => Some(130),
            Self::Kill => Some(137),
            Self::Timeout => None,
        }
    }
}

fn parse_exec_supervision_fields(output: &str) -> std::collections::BTreeMap<String, String> {
    output
        .replace('\r', "")
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

async fn exec_supervision_host_command(
    rt: &Runtime,
    container_id: &str,
    script: String,
) -> vz::protocol::ExecOutput {
    let output = rt
        .exec_host(
            container_id,
            ExecConfig {
                cmd: vec!["/bin/sh".into(), "-c".into(), script],
                timeout: Some(Duration::from_secs(15)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap_or_else(|error| panic!("guest supervision probe failed: {error}"));
    assert_eq!(
        output.exit_code, 0,
        "guest supervision probe failed: stdout={} stderr={}",
        output.stdout, output.stderr
    );
    output
}

async fn exec_supervision_cgroup(
    rt: &Runtime,
    container_id: &str,
) -> (String, Vec<(u32, u64, u32)>) {
    let output = exec_supervision_host_command(
        rt,
        container_id,
        format!(
            r#"state=$(/run/vz-oci/bin/youki --root /run/vz-oci/state state {container_id})
init_pid=$(printf '%s\n' "$state" | sed -n 's/.*"pid"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n1)
test -n "$init_pid"
cgroup_path=$(sed -n 's/^[^:]*:[^:]*://p' /proc/$init_pid/cgroup | head -n1)
test -n "$cgroup_path"
printf 'cgroup_path=%s\n' "$cgroup_path"
for pid in $(sort -n /sys/fs/cgroup$cgroup_path/cgroup.procs); do
    test -r /proc/$pid/stat || continue
    printf 'member=%s:%s:%s\n' "$pid" "$(awk '{{print $22}}' /proc/$pid/stat)" "$(awk '{{print $5}}' /proc/$pid/stat)"
done"#
        ),
    )
    .await;
    let fields = parse_exec_supervision_fields(&output.stdout);
    let cgroup_path = fields
        .get("cgroup_path")
        .cloned()
        .unwrap_or_else(|| panic!("cgroup probe omitted cgroup_path"));
    let members = output
        .stdout
        .replace('\r', "")
        .lines()
        .filter_map(|line| line.strip_prefix("member="))
        .map(|member| {
            let mut fields = member.split(':');
            let pid = fields.next().unwrap().parse().unwrap();
            let start_time = fields.next().unwrap().parse().unwrap();
            let pgid = fields.next().unwrap().parse().unwrap();
            assert!(fields.next().is_none(), "malformed cgroup member: {member}");
            (pid, start_time, pgid)
        })
        .collect();
    (cgroup_path, members)
}

async fn exec_supervision_marker(
    rt: &Runtime,
    container_id: &str,
    marker_name: &str,
) -> std::collections::BTreeMap<String, String> {
    let marker =
        format!("/run/vz-oci/containers/{container_id}/merged/vz-exec-supervision/{marker_name}");
    let output = exec_supervision_host_command(
        rt,
        container_id,
        format!(
            r#"i=0
while test ! -s '{marker}'; do
    i=$((i + 1))
    test "$i" -lt 200
    sleep 0.02
done
cat '{marker}'"#
        ),
    )
    .await;
    let fields = parse_exec_supervision_fields(&output.stdout);
    for required in [
        "pid",
        "start_time",
        "pgid",
        "child_pid",
        "child_start_time",
        "child_pgid",
        "cgroup_path",
    ] {
        assert!(
            fields.get(required).is_some_and(|value| !value.is_empty()),
            "exec marker omitted {required}: {}",
            output.stdout
        );
    }
    fields
}

async fn assert_exec_supervision_identity_live(
    rt: &Runtime,
    container_id: &str,
    expected_cgroup: &str,
    marker_name: &str,
    marker: &std::collections::BTreeMap<String, String>,
    active_members: &[(u32, u64, u32)],
) -> serde_json::Value {
    assert_eq!(
        marker["cgroup_path"], expected_cgroup,
        "exec target entered the wrong cgroup"
    );
    let start_time: u64 = marker["start_time"].parse().unwrap();
    let child_start_time: u64 = marker["child_start_time"].parse().unwrap();
    let output = exec_supervision_host_command(
        rt,
        container_id,
        format!(
            r#"matches=0
child_matches=0
host_pid=
child_host_pid=
for candidate in $(cat '/sys/fs/cgroup{expected_cgroup}/cgroup.procs'); do
    test -r /proc/$candidate/cmdline || continue
    if test "$(cat /proc/$candidate/comm)" = sh \
        && test "$(awk '{{print $22}}' /proc/$candidate/stat)" = '{start_time}' \
        && tr '\000' ' ' < /proc/$candidate/cmdline | grep -Fq '/vz-exec-supervision/{marker_name}'; then
        matches=$((matches + 1))
        host_pid=$candidate
    fi
    if test "$(cat /proc/$candidate/comm)" = sh \
        && test "$(awk '{{print $22}}' /proc/$candidate/stat)" = '{child_start_time}' \
        && tr '\000' ' ' < /proc/$candidate/cmdline | grep -Fq 'vz-child-{marker_name}' \
        && ! tr '\000' ' ' < /proc/$candidate/cmdline | grep -Fq '/vz-exec-supervision/{marker_name}'; then
        child_matches=$((child_matches + 1))
        child_host_pid=$candidate
    fi
done
test "$matches" -eq 1
test "$child_matches" -eq 1
test -r /proc/$host_pid/stat
test -r /proc/$child_host_pid/stat
printf 'host_pid=%s\n' "$host_pid"
printf 'start_time=%s\n' "$(awk '{{print $22}}' /proc/$host_pid/stat)"
printf 'host_pgid=%s\n' "$(awk '{{print $5}}' /proc/$host_pid/stat)"
printf 'child_host_pid=%s\n' "$child_host_pid"
printf 'child_start_time=%s\n' "$(awk '{{print $22}}' /proc/$child_host_pid/stat)"
printf 'child_host_pgid=%s\n' "$(awk '{{print $5}}' /proc/$child_host_pid/stat)"
printf 'cgroup_path=%s\n' "$(sed -n 's/^[^:]*:[^:]*://p' /proc/$host_pid/cgroup | head -n1)"
grep -qx "$host_pid" '/sys/fs/cgroup{expected_cgroup}/cgroup.procs'
grep -qx "$child_host_pid" '/sys/fs/cgroup{expected_cgroup}/cgroup.procs'"#
        ),
    )
    .await;
    let observed = parse_exec_supervision_fields(&output.stdout);
    let host_pid = observed["host_pid"].parse::<u32>().unwrap();
    assert_eq!(observed["start_time"].parse::<u64>().unwrap(), start_time);
    let host_pgid = observed["host_pgid"].parse::<u32>().unwrap();
    let child_host_pid = observed["child_host_pid"].parse::<u32>().unwrap();
    let child_start_time = observed["child_start_time"].parse::<u64>().unwrap();
    let child_host_pgid = observed["child_host_pgid"].parse::<u32>().unwrap();
    assert_eq!(
        child_start_time,
        marker["child_start_time"].parse::<u64>().unwrap()
    );
    assert_eq!(
        marker["child_pgid"].parse::<u32>().unwrap(),
        marker["pgid"].parse::<u32>().unwrap(),
        "container leader and retained child were not in one process group"
    );
    assert_eq!(
        child_host_pgid, host_pgid,
        "guest leader and retained child were not in one host process group"
    );
    assert_eq!(observed["cgroup_path"], expected_cgroup);
    assert!(
        active_members.contains(&(host_pid, start_time, host_pgid)),
        "resolved target identity was absent from raw cgroup snapshot: {active_members:?}"
    );
    assert!(
        active_members.contains(&(child_host_pid, child_start_time, child_host_pgid)),
        "resolved child identity was absent from raw cgroup snapshot: {active_members:?}"
    );
    json!({
        "container_pid": marker["pid"].parse::<u32>().unwrap(),
        "host_pid": host_pid,
        "start_time": start_time,
        "container_pgid": marker["pgid"].parse::<u32>().unwrap(),
        "host_pgid": host_pgid,
        "child_container_pid": marker["child_pid"].parse::<u32>().unwrap(),
        "child_host_pid": child_host_pid,
        "child_start_time": child_start_time,
        "child_container_pgid": marker["child_pgid"].parse::<u32>().unwrap(),
        "child_host_pgid": child_host_pgid,
        "cgroup_path": expected_cgroup,
    })
}

async fn exec_supervision_identity_absent(
    rt: &Runtime,
    container_id: &str,
    pid: u32,
    start_time: u64,
) -> bool {
    let output = exec_supervision_host_command(
        rt,
        container_id,
        format!(
            "if test ! -r /proc/{pid}/stat; then echo absent=true; \
             elif test \"$(awk '{{print $22}}' /proc/{pid}/stat)\" != '{start_time}'; then echo absent=true; \
             else echo absent=false; fi"
        ),
    )
    .await;
    parse_exec_supervision_fields(&output.stdout).get("absent") == Some(&"true".to_string())
}

async fn exec_supervision_outer_identity(
    rt: &Runtime,
    container_id: &str,
    expected_cgroup: &str,
    marker_name: &str,
    target_host_pid: u32,
) -> serde_json::Value {
    let output = exec_supervision_host_command(
        rt,
        container_id,
        format!(
            r#"set -eu
test -r /proc/{target_host_pid}/status
outer_pid=$(awk '$1 == "PPid:" {{print $2}}' /proc/{target_host_pid}/status)
test -n "$outer_pid"
tr '\000' ' ' < /proc/$outer_pid/cmdline | grep -Fq '__vz_container_exec_v4'
tr '\000' ' ' < /proc/$outer_pid/cmdline | grep -Fq '/vz-exec-supervision/{marker_name}'
if grep -qx "$outer_pid" '/sys/fs/cgroup{expected_cgroup}/cgroup.procs'; then
    target_cgroup_member=true
else
    target_cgroup_member=false
fi
printf 'pid=%s\n' "$outer_pid"
printf 'start_time=%s\n' "$(awk '{{print $22}}' /proc/$outer_pid/stat)"
printf 'pgid=%s\n' "$(awk '{{print $5}}' /proc/$outer_pid/stat)"
printf 'cgroup_path=%s\n' "$(sed -n 's/^[^:]*:[^:]*://p' /proc/$outer_pid/cgroup | head -n1)"
printf 'target_cgroup_member=%s\n' "$target_cgroup_member""#
        ),
    )
    .await;
    let fields = parse_exec_supervision_fields(&output.stdout);
    assert_eq!(fields["cgroup_path"], "/");
    assert_eq!(fields["target_cgroup_member"], "false");
    json!({
        "pid": fields["pid"].parse::<u32>().unwrap(),
        "start_time": fields["start_time"].parse::<u64>().unwrap(),
        "pgid": fields["pgid"].parse::<u32>().unwrap(),
        "cgroup_path": fields["cgroup_path"],
        "target_cgroup_member": false,
    })
}

async fn await_exec_supervision_cleanup(
    rt: &Runtime,
    container_id: &str,
    baseline_members: &[(u32, u64, u32)],
    identities: &[(u32, u64)],
    context: &str,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let diagnostics = rt.lifecycle_diagnostics().await.unwrap();
            let identities_absent = {
                let mut absent = true;
                for (pid, start_time) in identities {
                    absent &=
                        exec_supervision_identity_absent(rt, container_id, *pid, *start_time).await;
                }
                absent
            };
            let restored = exec_supervision_cgroup(rt, container_id).await.1 == baseline_members;
            if diagnostics.exec_sessions == 0 && identities_absent && restored {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{context} cleanup did not converge within 10s"));
}

fn write_exec_supervision_evidence(evidence: &serde_json::Value) {
    let rendered = serde_json::to_string_pretty(evidence).unwrap();
    write_test_stderr(format_args!("VZ_EXEC_SUPERVISION_EVIDENCE={rendered}"));
    let path = std::env::var("VZ_EXEC_SUPERVISION_EVIDENCE").unwrap_or_else(|error| {
        panic!("VZ_EXEC_SUPERVISION_EVIDENCE must be set by the strict VM harness: {error:?}")
    });
    assert!(
        Path::new(&path).is_absolute(),
        "exec supervision evidence path must be absolute"
    );
    std::fs::write(path, format!("{rendered}\n")).unwrap();
}

fn exec_supervision_build_identity() -> serde_json::Value {
    let profile = std::env::var("VZ_EXEC_SUPERVISION_BUILD_PROFILE").unwrap_or_else(|error| {
        panic!("VZ_EXEC_SUPERVISION_BUILD_PROFILE must be set by the strict VM harness: {error:?}")
    });
    assert_eq!(
        profile, "release",
        "exec supervision release evidence cannot be emitted by a non-release build"
    );
    let required_digest = |name: &str| {
        let digest = std::env::var(name)
            .unwrap_or_else(|_| panic!("{name} must be set by the strict VM harness"));
        assert_eq!(
            digest.len(),
            64,
            "{name} must contain a lowercase SHA-256 digest"
        );
        assert!(
            digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "{name} must contain a lowercase SHA-256 digest"
        );
        digest
    };
    json!({
        "profile": profile,
        "test_binary_sha256": required_digest("VZ_EXEC_SUPERVISION_TEST_BINARY_SHA256"),
        "developer_initramfs_sha256": required_digest(
            "VZ_EXEC_SUPERVISION_DEVELOPER_INITRAMFS_SHA256"
        ),
    })
}

async fn assert_exec_supervision_lifecycle_writer_available(
    rt: &Runtime,
    container_id: &str,
    context: &str,
) -> bool {
    let error = tokio::time::timeout(Duration::from_secs(10), rt.remove_container(container_id))
        .await
        .unwrap_or_else(|_| panic!("{context} left the lifecycle writer blocked"))
        .map_or_else(|error| error, |value| panic!("lifecycle writer unexpectedly removed a running container; unexpected success: {value:?}"));
    let expected = format!("cannot remove running container '{container_id}'; stop it first");
    assert!(
        matches!(
            error,
            vz_oci_macos::MacosOciError::InvalidConfig(ref message) if message == &expected
        ),
        "{context} lifecycle writer returned the wrong result: {error}"
    );
    true
}

/// Prove all three container-exec adapters supervise the actual target process
/// group and synchronously reap it on signals and deadline cancellation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires release-built Apple Silicon host + Linux VM artifacts"]
async fn runtime_exec_supervision() {
    if !require_virtualization_entitlement() {
        return;
    }
    init_tracing();
    let build_identity = exec_supervision_build_identity();

    const IMAGE: &str = "alpine:3.20";
    const CONTAINER_ID: &str = "exec-supervision-e2e";
    const TIMEOUT_MS: u64 = 2_000;

    let tmp = tempfile::tempdir().unwrap();
    let rt = test_runtime(tmp.path());
    let container_id = rt
        .create_container(
            IMAGE,
            RunConfig {
                container_id: Some(CONTAINER_ID.into()),
                cmd: vec!["/bin/busybox".into(), "sleep".into(), "300".into()],
                execution_mode: ExecutionMode::OciRuntime,
                ..RunConfig::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(container_id, CONTAINER_ID);

    let setup = rt
        .exec_container(
            CONTAINER_ID,
            ExecConfig {
                cmd: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "mkdir -p /vz-exec-supervision; \
                     rm -f /vz-exec-supervision/*; \
                     mkdir -p /vz-exec-response-loss-command; \
                     ln -sf /bin/busybox /vz-exec-response-loss-command/sh"
                        .into(),
                ],
                timeout: Some(Duration::from_secs(10)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(setup.exit_code, 0, "marker setup failed: {}", setup.stderr);

    let (cgroup_path, baseline_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
    assert_eq!(
        baseline_members.len(),
        1,
        "idle container cgroup must contain only init: {baseline_members:?}"
    );

    // An authenticated container-targeted request rejected by guest-side
    // validation is a definite pre-spawn failure. It must not be confused
    // with transport ambiguity or retain either exec or lifecycle authority.
    const PRE_SPAWN_CASE: &str = "pre-spawn-rejection";
    const PRE_SPAWN_EXECUTION_ID: &str = "exec-supervision-pre-spawn-rejection";
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_START={PRE_SPAWN_CASE}"
    ));
    let pre_spawn_sessions_before = rt.lifecycle_diagnostics().await.unwrap().exec_sessions;
    let (_, pre_spawn_members_before) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
    let pre_spawn_events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let pre_spawn_callback_events = std::sync::Arc::clone(&pre_spawn_events);
    let pre_spawn_error = tokio::time::timeout(
        Duration::from_secs(10),
        rt.exec_container_streaming(
            CONTAINER_ID,
            ExecConfig {
                execution_id: Some(PRE_SPAWN_EXECUTION_ID.into()),
                cmd: vec!["/bin/busybox".into(), "true".into()],
                env: vec![("INVALID=ENVIRONMENT_KEY".into(), "rejected".into())],
                timeout: Some(Duration::from_secs(10)),
                ..ExecConfig::default()
            },
            move |event| pre_spawn_callback_events.lock().unwrap().push(event),
        ),
    )
    .await
    .unwrap_or_else(|error| panic!("authenticated pre-spawn rejection did not become definite: {error:?}"))
    .map_or_else(|error| error, |value| panic!("invalid environment key unexpectedly spawned an exec; unexpected success: {value:?}"))
    .to_string();
    assert!(
        pre_spawn_error.contains("invalid key"),
        "pre-spawn rejection lost the authenticated guest error: {pre_spawn_error}"
    );
    assert!(
        !pre_spawn_error.contains("lifecycle authority was retained"),
        "definite pre-spawn rejection was misclassified as transport ambiguity: {pre_spawn_error}"
    );
    assert!(
        pre_spawn_events.lock().unwrap().is_empty(),
        "pre-spawn rejection published an interactive event"
    );
    let pre_spawn_sessions_after = rt.lifecycle_diagnostics().await.unwrap().exec_sessions;
    let (_, pre_spawn_members_after) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
    assert_eq!(pre_spawn_sessions_before, 0);
    assert_eq!(pre_spawn_sessions_after, pre_spawn_sessions_before);
    assert_eq!(pre_spawn_members_before, baseline_members);
    assert_eq!(pre_spawn_members_after, pre_spawn_members_before);
    let pre_spawn_stale_control_rejected = matches!(
        rt.cancel_exec(PRE_SPAWN_EXECUTION_ID).await,
        Err(vz_oci_macos::MacosOciError::ExecutionSessionNotFound { .. })
    );
    assert!(pre_spawn_stale_control_rejected);
    let pre_spawn_lifecycle_writer =
        assert_exec_supervision_lifecycle_writer_available(&rt, CONTAINER_ID, PRE_SPAWN_CASE).await;
    let pre_spawn_post = rt
        .exec_container(
            CONTAINER_ID,
            ExecConfig {
                cmd: vec!["/bin/busybox".into(), "true".into()],
                timeout: Some(Duration::from_secs(10)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();
    let pre_spawn_post_case_probe = pre_spawn_post.exit_code == 0
        && exec_supervision_cgroup(&rt, CONTAINER_ID).await.1 == baseline_members;
    assert!(pre_spawn_post_case_probe);
    let pre_spawn_rejection = json!({
        "attempts": 1,
        "adapter": "streaming",
        "target": "container",
        "execution_id": PRE_SPAWN_EXECUTION_ID,
        "invalid_environment_key": "INVALID=ENVIRONMENT_KEY",
        "authenticated_definite_error": true,
        "terminal_error": pre_spawn_error,
        "interactive_events": pre_spawn_events.lock().unwrap().len(),
        "session_count_before": pre_spawn_sessions_before,
        "session_count_after": pre_spawn_sessions_after,
        "cgroup_members_before": pre_spawn_members_before.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
        "cgroup_members_after": pre_spawn_members_after.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
        "stale_control_rejected": pre_spawn_stale_control_rejected,
        "lifecycle_writer_available": pre_spawn_lifecycle_writer,
        "post_case_probe": pre_spawn_post_case_probe,
    });

    // Hold a live callback beyond the retired five-second drain heuristic
    // while the guest emits more than the complete bounded channel capacity.
    // No liveness shortcut may truncate or reorder the terminal stream.
    const SLOW_CONSUMER_CASE: &str = "slow-live-consumer";
    const SLOW_CONSUMER_EXECUTION_ID: &str = "exec-supervision-slow-live-consumer";
    const SLOW_CONSUMER_PAUSE_MS: u64 = 6_000;
    const EXEC_EVENT_CHANNEL_CAPACITY: usize = 64;
    const EXEC_EVENT_MAX_CHUNK_BYTES: usize = 65_536;
    const SLOW_CONSUMER_STDOUT_BYTES: usize = 5 * 1024 * 1024;
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_START={SLOW_CONSUMER_CASE}"
    ));
    const {
        assert!(
            SLOW_CONSUMER_STDOUT_BYTES > EXEC_EVENT_CHANNEL_CAPACITY * EXEC_EVENT_MAX_CHUNK_BYTES
        );
    }
    let slow_events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let slow_callback_events = std::sync::Arc::clone(&slow_events);
    let mut callback_paused = false;
    let slow_output = tokio::time::timeout(
        Duration::from_secs(40),
        rt.exec_container_streaming(
            CONTAINER_ID,
            ExecConfig {
                execution_id: Some(SLOW_CONSUMER_EXECUTION_ID.into()),
                cmd: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    format!(
                        "/bin/busybox yes 0123456789abcdef | /bin/busybox head -c {SLOW_CONSUMER_STDOUT_BYTES}"
                    ),
                ],
                timeout: Some(Duration::from_secs(35)),
                ..ExecConfig::default()
            },
            move |event| {
                if !callback_paused && matches!(event, InteractiveExecEvent::Stdout(_)) {
                    callback_paused = true;
                    std::thread::sleep(Duration::from_millis(SLOW_CONSUMER_PAUSE_MS));
                }
                slow_callback_events.lock().unwrap().push(event);
            },
        ),
    )
    .await
    .unwrap_or_else(|error| panic!("slow live consumer did not terminate within its outer bound: {error:?}"))
    .unwrap_or_else(|error| panic!("slow live consumer was treated as abandoned: {error:?}"));
    assert_eq!(slow_output.exit_code, 0);
    assert!(slow_output.stderr.is_empty());
    let (slow_ready_events, slow_exit_events, slow_stderr_bytes, slow_stdout, slow_exit_last) = {
        let slow_events = slow_events.lock().unwrap();
        let ready_events = slow_events
            .iter()
            .filter(|event| matches!(event, InteractiveExecEvent::ContainerReady(_)))
            .count();
        let exit_events = slow_events
            .iter()
            .filter(|event| matches!(event, InteractiveExecEvent::Exit(_)))
            .count();
        let stderr_bytes = slow_events
            .iter()
            .filter_map(|event| match event {
                InteractiveExecEvent::Stderr(bytes) => Some(bytes.len()),
                _ => None,
            })
            .sum::<usize>();
        let stdout = slow_events
            .iter()
            .filter_map(|event| match event {
                InteractiveExecEvent::Stdout(bytes) => Some(bytes.as_slice()),
                _ => None,
            })
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let exit_last = matches!(slow_events.last(), Some(InteractiveExecEvent::Exit(0)));
        (ready_events, exit_events, stderr_bytes, stdout, exit_last)
    };
    let expected_slow_stdout = b"0123456789abcdef\n"
        .iter()
        .copied()
        .cycle()
        .take(SLOW_CONSUMER_STDOUT_BYTES)
        .collect::<Vec<_>>();
    assert_eq!(slow_ready_events, 1);
    assert_eq!(slow_exit_events, 1);
    assert_eq!(slow_stderr_bytes, 0);
    assert_eq!(slow_stdout, expected_slow_stdout);
    assert_eq!(slow_output.stdout.as_bytes(), expected_slow_stdout);
    assert!(slow_exit_last);
    let slow_stdout_sha256 = format!("{:x}", Sha256::digest(&slow_stdout));
    let slow_diagnostics = rt.lifecycle_diagnostics().await.unwrap();
    let (_, slow_members_after) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
    assert_eq!(slow_diagnostics.exec_sessions, 0);
    assert_eq!(slow_members_after, baseline_members);
    let slow_stale_control_rejected = matches!(
        rt.signal_exec(SLOW_CONSUMER_EXECUTION_ID, "SIGTERM").await,
        Err(vz_oci_macos::MacosOciError::ExecutionSessionNotFound { .. })
    );
    assert!(slow_stale_control_rejected);
    let slow_lifecycle_writer =
        assert_exec_supervision_lifecycle_writer_available(&rt, CONTAINER_ID, SLOW_CONSUMER_CASE)
            .await;
    let slow_post = rt
        .exec_container(
            CONTAINER_ID,
            ExecConfig {
                cmd: vec!["/bin/busybox".into(), "true".into()],
                timeout: Some(Duration::from_secs(10)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();
    let slow_post_case_probe = slow_post.exit_code == 0
        && exec_supervision_cgroup(&rt, CONTAINER_ID).await.1 == baseline_members;
    assert!(slow_post_case_probe);
    let slow_live_consumer = json!({
        "attempts": 1,
        "adapter": "streaming",
        "execution_id": SLOW_CONSUMER_EXECUTION_ID,
        "pause_ms": SLOW_CONSUMER_PAUSE_MS,
        "retired_drain_threshold_ms": 5_000,
        "channel_capacity_events": EXEC_EVENT_CHANNEL_CAPACITY,
        "max_chunk_bytes": EXEC_EVENT_MAX_CHUNK_BYTES,
        "expected_stdout_bytes": SLOW_CONSUMER_STDOUT_BYTES,
        "observed_stdout_bytes": slow_stdout.len(),
        "stdout_sha256": slow_stdout_sha256,
        "content_exact": slow_stdout == expected_slow_stdout,
        "ready_events": slow_ready_events,
        "exit_events": slow_exit_events,
        "stderr_bytes": slow_stderr_bytes,
        "exit_code": slow_output.exit_code,
        "exit_last": slow_exit_last,
        "cgroup_restored": slow_members_after == baseline_members,
        "session_reaped": slow_diagnostics.exec_sessions == 0 && slow_stale_control_rejected,
        "lifecycle_writer_available": slow_lifecycle_writer,
        "post_case_probe": slow_post_case_probe,
    });

    let mut cells = Vec::new();

    for adapter in ExecSupervisionAdapter::ALL {
        for termination in ExecSupervisionTermination::ALL {
            let case_name = format!("{}-{}", adapter.name(), termination.name());
            write_test_stderr(format_args!("VZ_EXEC_SUPERVISION_CASE_START={case_name}"));
            let execution_id = format!("exec-supervision-{case_name}");
            let timeout = if termination == ExecSupervisionTermination::Timeout {
                Duration::from_millis(TIMEOUT_MS)
            } else {
                Duration::from_secs(20)
            };
            let command = format!(
                r#"marker=/vz-exec-supervision/{case_name}
pid=$$
start_time=$(awk '{{print $22}}' /proc/$pid/stat)
pgid=$(awk '{{print $5}}' /proc/$pid/stat)
cgroup_path=$(sed -n 's/^[^:]*:[^:]*://p' /proc/$pid/cgroup | head -n1)
/bin/sh -c 'while :; do /bin/busybox sleep 300; done' 'vz-child-{case_name}' &
child_pid=$!
child_start_time=$(awk '{{print $22}}' /proc/$child_pid/stat)
child_pgid=$(awk '{{print $5}}' /proc/$child_pid/stat)
printf 'pid=%s\nstart_time=%s\npgid=%s\nchild_pid=%s\nchild_start_time=%s\nchild_pgid=%s\ncgroup_path=%s\n' \
  "$pid" "$start_time" "$pgid" "$child_pid" "$child_start_time" "$child_pgid" "$cgroup_path" > "$marker"
trap 'exit 143' TERM
trap 'exit 130' INT
while :; do wait "$child_pid"; done"#
            );
            let config = ExecConfig {
                execution_id: Some(execution_id.clone()),
                cmd: vec!["/bin/sh".into(), "-c".into(), command],
                pty: adapter == ExecSupervisionAdapter::Pty,
                term_rows: (adapter == ExecSupervisionAdapter::Pty).then_some(24),
                term_cols: (adapter == ExecSupervisionAdapter::Pty).then_some(80),
                timeout: Some(timeout),
                ..ExecConfig::default()
            };
            let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let callback_events = std::sync::Arc::clone(&events);
            let task_rt = rt.clone();
            let task_container_id = container_id.clone();
            let started = std::time::Instant::now();
            let task = match adapter {
                ExecSupervisionAdapter::Unary => tokio::spawn(async move {
                    task_rt
                        .exec_container_oci_unary(&task_container_id, config)
                        .await
                }),
                ExecSupervisionAdapter::Streaming | ExecSupervisionAdapter::Pty => {
                    tokio::spawn(async move {
                        task_rt
                            .exec_container_streaming(&task_container_id, config, move |event| {
                                callback_events.lock().unwrap().push(event)
                            })
                            .await
                    })
                }
            };

            let marker = exec_supervision_marker(&rt, CONTAINER_ID, &case_name).await;
            write_test_stderr(format_args!("VZ_EXEC_SUPERVISION_CASE_MARKER={case_name}"));
            let (_, active_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
            assert!(
                active_members.len() > baseline_members.len(),
                "{case_name} did not add a target process to the exact container cgroup"
            );
            let identity = assert_exec_supervision_identity_live(
                &rt,
                CONTAINER_ID,
                &cgroup_path,
                &case_name,
                &marker,
                &active_members,
            )
            .await;
            let host_pid = identity["host_pid"].as_u64().unwrap() as u32;
            assert!(
                active_members.iter().any(|member| member.0 == host_pid),
                "{case_name} target PID was absent from the exact cgroup: {active_members:?}"
            );

            if adapter != ExecSupervisionAdapter::Unary {
                let observed_events = events.lock().unwrap();
                assert!(
                    observed_events
                        .iter()
                        .any(|event| matches!(event, InteractiveExecEvent::ContainerReady(_))),
                    "{case_name} exposed its process marker before ContainerReady"
                );
            }
            let pty_resized = if adapter == ExecSupervisionAdapter::Pty {
                tokio::time::timeout(
                    Duration::from_secs(10),
                    rt.resize_exec_pty(&execution_id, 111, 37),
                )
                .await
                .unwrap_or_else(|_| panic!("{case_name} PTY resize did not complete within 10s"))
                .unwrap();
                true
            } else {
                false
            };
            if let Some(signal) = termination.signal() {
                tokio::time::timeout(
                    Duration::from_secs(10),
                    rt.signal_exec(&execution_id, signal),
                )
                .await
                .unwrap_or_else(|_| {
                    panic!("{case_name} signal control did not complete within 10s")
                })
                .unwrap_or_else(|error| {
                    panic!("{case_name} was not control-registered at process readiness: {error}")
                });
                write_test_stderr(format_args!(
                    "VZ_EXEC_SUPERVISION_CASE_SIGNALLED={case_name}"
                ));
            }

            let result = tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .unwrap_or_else(|_| panic!("{case_name} did not terminate and reap within 10s"))
                .unwrap_or_else(|error| panic!("{case_name} exec task panicked: {error}"));
            let elapsed_ms = started.elapsed().as_millis() as u64;
            write_test_stderr(format_args!(
                "VZ_EXEC_SUPERVISION_CASE_TERMINAL={case_name}:{elapsed_ms}"
            ));
            let (observed_exit_code, timed_out) = match termination.expected_exit_code() {
                Some(expected) => {
                    let output = result.unwrap_or_else(|error| {
                        panic!("{case_name} returned an error instead of exit {expected}: {error}")
                    });
                    assert_eq!(
                        output.exit_code, expected,
                        "{case_name} did not preserve signal exit semantics: stdout={} stderr={}",
                        output.stdout, output.stderr
                    );
                    (Some(output.exit_code), false)
                }
                None => {
                    let error =
                        result.map_or_else(|error| error, |value| panic!("deadline cancellation unexpectedly returned output; unexpected success: {value:?}"));
                    let rendered = error.to_string();
                    assert!(
                        rendered.contains("timed out"),
                        "deadline cancellation returned the wrong error: {rendered}"
                    );
                    assert!(
                        (TIMEOUT_MS..=TIMEOUT_MS + 4_000).contains(&elapsed_ms),
                        "deadline was not bounded and stable: requested={TIMEOUT_MS}ms observed={elapsed_ms}ms"
                    );
                    (None, true)
                }
            };

            let stale_control_rejected = matches!(
                rt.signal_exec(&execution_id, "SIGTERM").await,
                Err(vz_oci_macos::MacosOciError::ExecutionSessionNotFound { .. })
            );
            assert!(
                stale_control_rejected,
                "{case_name} retained a control session after terminal reap"
            );
            let diagnostics = rt.lifecycle_diagnostics().await.unwrap();
            assert_eq!(
                diagnostics.exec_sessions, 0,
                "{case_name} leaked a host execution session: {diagnostics:?}"
            );
            assert!(
                exec_supervision_identity_absent(
                    &rt,
                    CONTAINER_ID,
                    host_pid,
                    identity["start_time"].as_u64().unwrap(),
                )
                .await,
                "{case_name} retained the exact /proc process identity"
            );
            assert!(
                exec_supervision_identity_absent(
                    &rt,
                    CONTAINER_ID,
                    identity["child_host_pid"].as_u64().unwrap() as u32,
                    identity["child_start_time"].as_u64().unwrap(),
                )
                .await,
                "{case_name} retained the exact child /proc process identity"
            );
            let (_, restored_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
            assert_eq!(
                restored_members, baseline_members,
                "{case_name} did not restore exact cgroup membership"
            );
            let marker_removed = exec_supervision_host_command(
                &rt,
                CONTAINER_ID,
                format!(
                    "rm -f '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{case_name}'; \
                     test ! -e '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{case_name}'"
                ),
            )
            .await
            .exit_code
                == 0;
            assert!(marker_removed, "{case_name} marker did not clean up");
            let post_case = rt
                .exec_container(
                    CONTAINER_ID,
                    ExecConfig {
                        cmd: vec![
                            "/bin/busybox".into(),
                            "printf".into(),
                            format!("post-case:{case_name}"),
                        ],
                        timeout: Some(Duration::from_secs(10)),
                        ..ExecConfig::default()
                    },
                )
                .await
                .unwrap();
            let post_case_probe = post_case.exit_code == 0
                && post_case.stdout.replace('\r', "") == format!("post-case:{case_name}");
            assert!(post_case_probe, "{case_name} left the container unhealthy");
            let (_, post_probe_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
            assert_eq!(post_probe_members, baseline_members);

            cells.push(json!({
                "adapter": adapter.name(),
                "termination": termination.name(),
                "execution_id": execution_id,
                "signal": termination.signal(),
                "expected_exit_code": termination.expected_exit_code(),
                "observed_exit_code": observed_exit_code,
                "timed_out": timed_out,
                "timeout_requested_ms": (termination == ExecSupervisionTermination::Timeout).then_some(TIMEOUT_MS),
                "elapsed_ms": elapsed_ms,
                "identity": identity,
                "baseline_cgroup_members": baseline_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
                "active_cgroup_members": active_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
                "cgroup_restored": restored_members == baseline_members && post_probe_members == baseline_members,
                "process_identity_absent": true,
                "session_reaped": diagnostics.exec_sessions == 0 && stale_control_rejected,
                "marker_removed": marker_removed,
                "pty_resized": pty_resized,
                "post_case_probe": post_case_probe,
            }));
        }
    }

    // A normal leader exit is also terminal ownership: a retained background
    // child must be killed and reaped before the successful result is exposed.
    const NORMAL_CASE: &str = "normal-exit";
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_START={NORMAL_CASE}"
    ));
    let normal_execution_id = "exec-supervision-normal-exit".to_string();
    let normal_events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let normal_callback_events = std::sync::Arc::clone(&normal_events);
    let normal_rt = rt.clone();
    let normal_container_id = container_id.clone();
    let normal_task = tokio::spawn(async move {
        normal_rt
            .exec_container_streaming(
                &normal_container_id,
                ExecConfig {
                    execution_id: Some(normal_execution_id.clone()),
                    cmd: vec![
                        "/bin/sh".into(),
                        "-c".into(),
                        format!(
                            r#"marker=/vz-exec-supervision/{NORMAL_CASE}
pid=$$
start_time=$(awk '{{print $22}}' /proc/$pid/stat)
pgid=$(awk '{{print $5}}' /proc/$pid/stat)
cgroup_path=$(sed -n 's/^[^:]*:[^:]*://p' /proc/$pid/cgroup | head -n1)
/bin/sh -c 'while :; do /bin/busybox sleep 300; done' 'vz-child-{NORMAL_CASE}' &
child_pid=$!
child_start_time=$(awk '{{print $22}}' /proc/$child_pid/stat)
child_pgid=$(awk '{{print $5}}' /proc/$child_pid/stat)
printf 'pid=%s\nstart_time=%s\npgid=%s\nchild_pid=%s\nchild_start_time=%s\nchild_pgid=%s\ncgroup_path=%s\n' \
  "$pid" "$start_time" "$pgid" "$child_pid" "$child_start_time" "$child_pgid" "$cgroup_path" > "$marker"
while test ! -e /vz-exec-supervision/normal-release; do /bin/busybox sleep 0.02; done
exit 0"#
                        ),
                    ],
                    timeout: Some(Duration::from_secs(20)),
                    ..ExecConfig::default()
                },
                move |event| normal_callback_events.lock().unwrap().push(event),
            )
            .await
    });
    let normal_marker = exec_supervision_marker(&rt, CONTAINER_ID, NORMAL_CASE).await;
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_MARKER={NORMAL_CASE}"
    ));
    let (_, normal_active_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
    let normal_identity = assert_exec_supervision_identity_live(
        &rt,
        CONTAINER_ID,
        &cgroup_path,
        NORMAL_CASE,
        &normal_marker,
        &normal_active_members,
    )
    .await;
    exec_supervision_host_command(
        &rt,
        CONTAINER_ID,
        format!(
            "touch '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/normal-release'"
        ),
    )
    .await;
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_RELEASED={NORMAL_CASE}"
    ));
    let normal_output = tokio::time::timeout(Duration::from_secs(10), normal_task)
        .await
        .unwrap_or_else(|error| {
            panic!("normal leader exit did not synchronously reap its child: {error:?}")
        })
        .unwrap_or_else(|error| panic!("normal leader-exit task panicked: {error:?}"))
        .unwrap_or_else(|error| panic!("normal leader-exit exec failed: {error:?}"));
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_TERMINAL={NORMAL_CASE}"
    ));
    assert_eq!(normal_output.exit_code, 0);
    assert!(matches!(
        normal_events.lock().unwrap().last(),
        Some(InteractiveExecEvent::Exit(0))
    ));
    let normal_leader_absent = exec_supervision_identity_absent(
        &rt,
        CONTAINER_ID,
        normal_identity["host_pid"].as_u64().unwrap() as u32,
        normal_identity["start_time"].as_u64().unwrap(),
    )
    .await;
    let normal_child_absent = exec_supervision_identity_absent(
        &rt,
        CONTAINER_ID,
        normal_identity["child_host_pid"].as_u64().unwrap() as u32,
        normal_identity["child_start_time"].as_u64().unwrap(),
    )
    .await;
    assert!(normal_leader_absent && normal_child_absent);
    let normal_diagnostics = rt.lifecycle_diagnostics().await.unwrap();
    let normal_session_reaped = normal_diagnostics.exec_sessions == 0
        && matches!(
            rt.signal_exec("exec-supervision-normal-exit", "SIGTERM")
                .await,
            Err(vz_oci_macos::MacosOciError::ExecutionSessionNotFound { .. })
        );
    assert!(normal_session_reaped);
    let (_, normal_restored_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
    let normal_cgroup_restored = normal_restored_members == baseline_members;
    assert!(normal_cgroup_restored);
    let normal_markers_removed = exec_supervision_host_command(
        &rt,
        CONTAINER_ID,
        format!(
            "rm -f '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{NORMAL_CASE}' \
                '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/normal-release'; \
             test -z \"$(find '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision' -mindepth 1 -maxdepth 1 -print -quit)\""
        ),
    )
    .await
    .exit_code
        == 0;
    assert!(normal_markers_removed);
    let normal_post_case = rt
        .exec_container(
            CONTAINER_ID,
            ExecConfig {
                cmd: vec!["/bin/busybox".into(), "true".into()],
                timeout: Some(Duration::from_secs(10)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();
    let normal_post_case_probe = normal_post_case.exit_code == 0;
    assert!(normal_post_case_probe);
    let normal_exit = json!({
        "adapter": "streaming",
        "execution_id": "exec-supervision-normal-exit",
        "exit_code": normal_output.exit_code,
        "identity": normal_identity,
        "baseline_cgroup_members": baseline_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
        "active_cgroup_members": normal_active_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
        "cgroup_restored": normal_cgroup_restored,
        "leader_identity_absent": normal_leader_absent,
        "child_identity_absent": normal_child_absent,
        "session_reaped": normal_session_reaped,
        "markers_removed": normal_markers_removed,
        "post_case_probe": normal_post_case_probe,
    });

    // Exercise the explicit CancelExec receipt path independently of host
    // deadline cancellation. It must carry the same descendant-reap proof.
    const CANCEL_CASE: &str = "cancel";
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_START={CANCEL_CASE}"
    ));
    let cancel_events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let cancel_callback_events = std::sync::Arc::clone(&cancel_events);
    let cancel_rt = rt.clone();
    let cancel_container_id = container_id.clone();
    let cancel_task = tokio::spawn(async move {
        cancel_rt
            .exec_container_streaming(
                &cancel_container_id,
                ExecConfig {
                    execution_id: Some("exec-supervision-cancel".into()),
                    cmd: vec![
                        "/bin/sh".into(),
                        "-c".into(),
                        format!(
                            r#"marker=/vz-exec-supervision/{CANCEL_CASE}
pid=$$
start_time=$(awk '{{print $22}}' /proc/$pid/stat)
pgid=$(awk '{{print $5}}' /proc/$pid/stat)
cgroup_path=$(sed -n 's/^[^:]*:[^:]*://p' /proc/$pid/cgroup | head -n1)
/bin/sh -c 'while :; do /bin/busybox sleep 300; done' 'vz-child-{CANCEL_CASE}' &
child_pid=$!
child_start_time=$(awk '{{print $22}}' /proc/$child_pid/stat)
child_pgid=$(awk '{{print $5}}' /proc/$child_pid/stat)
printf 'pid=%s\nstart_time=%s\npgid=%s\nchild_pid=%s\nchild_start_time=%s\nchild_pgid=%s\ncgroup_path=%s\n' \
  "$pid" "$start_time" "$pgid" "$child_pid" "$child_start_time" "$child_pgid" "$cgroup_path" > "$marker"
trap 'exit 143' TERM
while :; do wait "$child_pid"; done"#
                        ),
                    ],
                    timeout: Some(Duration::from_secs(20)),
                    ..ExecConfig::default()
                },
                move |event| cancel_callback_events.lock().unwrap().push(event),
            )
            .await
    });
    let cancel_marker = exec_supervision_marker(&rt, CONTAINER_ID, CANCEL_CASE).await;
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_MARKER={CANCEL_CASE}"
    ));
    let (_, cancel_active_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
    let cancel_identity = assert_exec_supervision_identity_live(
        &rt,
        CONTAINER_ID,
        &cgroup_path,
        CANCEL_CASE,
        &cancel_marker,
        &cancel_active_members,
    )
    .await;
    tokio::time::timeout(
        Duration::from_secs(10),
        rt.cancel_exec("exec-supervision-cancel"),
    )
    .await
    .unwrap_or_else(|error| panic!("explicit cancellation receipt timed out: {error:?}"))
    .unwrap();
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_CANCELLED={CANCEL_CASE}"
    ));
    let cancel_output = tokio::time::timeout(Duration::from_secs(10), cancel_task)
        .await
        .unwrap_or_else(|error| {
            panic!("explicit cancellation did not synchronously reap its child: {error:?}")
        })
        .unwrap_or_else(|error| panic!("explicit cancellation task panicked: {error:?}"))
        .unwrap_or_else(|error| panic!("explicit cancellation exec failed: {error:?}"));
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_TERMINAL={CANCEL_CASE}"
    ));
    assert_eq!(cancel_output.exit_code, 143);
    assert!(matches!(
        cancel_events.lock().unwrap().last(),
        Some(InteractiveExecEvent::Exit(143))
    ));
    let cancel_leader_absent = exec_supervision_identity_absent(
        &rt,
        CONTAINER_ID,
        cancel_identity["host_pid"].as_u64().unwrap() as u32,
        cancel_identity["start_time"].as_u64().unwrap(),
    )
    .await;
    let cancel_child_absent = exec_supervision_identity_absent(
        &rt,
        CONTAINER_ID,
        cancel_identity["child_host_pid"].as_u64().unwrap() as u32,
        cancel_identity["child_start_time"].as_u64().unwrap(),
    )
    .await;
    assert!(cancel_leader_absent && cancel_child_absent);
    let cancel_diagnostics = rt.lifecycle_diagnostics().await.unwrap();
    let cancel_session_reaped = cancel_diagnostics.exec_sessions == 0
        && matches!(
            rt.cancel_exec("exec-supervision-cancel").await,
            Err(vz_oci_macos::MacosOciError::ExecutionSessionNotFound { .. })
        );
    assert!(cancel_session_reaped);
    let (_, cancel_restored_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
    let cancel_cgroup_restored = cancel_restored_members == baseline_members;
    assert!(cancel_cgroup_restored);
    let cancel_marker_removed = exec_supervision_host_command(
        &rt,
        CONTAINER_ID,
        format!(
            "rm -f '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{CANCEL_CASE}'; \
             test ! -e '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{CANCEL_CASE}'"
        ),
    )
    .await
    .exit_code
        == 0;
    assert!(cancel_marker_removed);
    let cancel_post_case = rt
        .exec_container(
            CONTAINER_ID,
            ExecConfig {
                cmd: vec!["/bin/busybox".into(), "true".into()],
                timeout: Some(Duration::from_secs(10)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();
    let cancel_post_case_probe = cancel_post_case.exit_code == 0;
    assert!(cancel_post_case_probe);
    assert_eq!(
        exec_supervision_cgroup(&rt, CONTAINER_ID).await.1,
        baseline_members
    );
    let cancellation = json!({
        "adapter": "streaming",
        "execution_id": "exec-supervision-cancel",
        "exit_code": cancel_output.exit_code,
        "identity": cancel_identity,
        "baseline_cgroup_members": baseline_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
        "active_cgroup_members": cancel_active_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
        "cgroup_restored": cancel_cgroup_restored,
        "leader_identity_absent": cancel_leader_absent,
        "child_identity_absent": cancel_child_absent,
        "session_reaped": cancel_session_reaped,
        "marker_removed": cancel_marker_removed,
        "post_case_probe": cancel_post_case_probe,
    });

    // Pause after host registration but before any guest RPC, queue cancel,
    // and prove that readiness and the target process never become observable.
    const CANCEL_BEFORE_READY_ID: &str = "exec-supervision-cancel-before-ready";
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_START=cancel-before-ready"
    ));
    let mut pre_ready_observer = rt.install_lifecycle_observer();
    let pre_ready_events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let pre_ready_callback_events = std::sync::Arc::clone(&pre_ready_events);
    let pre_ready_rt = rt.clone();
    let pre_ready_container_id = container_id.clone();
    let pre_ready_task = tokio::spawn(async move {
        pre_ready_rt
            .exec_container_streaming(
                &pre_ready_container_id,
                ExecConfig {
                    execution_id: Some(CANCEL_BEFORE_READY_ID.into()),
                    cmd: vec![
                        "/bin/sh".into(),
                        "-c".into(),
                        "touch /vz-exec-supervision/cancel-before-ready; sleep 300".into(),
                    ],
                    timeout: Some(Duration::from_secs(20)),
                    ..ExecConfig::default()
                },
                move |event| pre_ready_callback_events.lock().unwrap().push(event),
            )
            .await
    });
    let pre_ready_admission =
        tokio::time::timeout(Duration::from_secs(10), pre_ready_observer.recv())
            .await
            .unwrap_or_else(|error| {
                panic!("pre-ready exec did not reach deterministic admission: {error:?}")
            })
            .unwrap_or_else(|| panic!("pre-ready lifecycle observer closed"));
    assert_eq!(
        pre_ready_admission.kind(),
        RuntimeLifecycleAdmissionKind::ExecBeforeGuestRpc
    );
    assert_eq!(pre_ready_admission.container_id(), CONTAINER_ID);
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_ADMITTED=cancel-before-ready"
    ));
    assert!(
        pre_ready_events.lock().unwrap().is_empty(),
        "ContainerReady was published before ExecBeforeGuestRpc cancellation"
    );
    let mut pre_ready_cancel = Box::pin(rt.cancel_exec(CANCEL_BEFORE_READY_ID));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut pre_ready_cancel)
            .await
            .is_err(),
        "pre-ready cancel returned before the paused exec consumed its queued cancellation"
    );
    pre_ready_admission.resume();
    tokio::time::timeout(Duration::from_secs(10), &mut pre_ready_cancel)
        .await
        .unwrap_or_else(|error| panic!("pre-ready cancellation receipt timed out: {error:?}"))
        .unwrap_or_else(|error| panic!("pre-ready cancellation failed: {error:?}"));
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_CANCELLED=cancel-before-ready"
    ));
    let pre_ready_result = tokio::time::timeout(Duration::from_secs(10), pre_ready_task)
        .await
        .unwrap_or_else(|error| panic!("pre-ready exec did not become terminal: {error:?}"))
        .unwrap_or_else(|error| panic!("pre-ready exec task panicked: {error:?}"));
    let pre_ready_error = pre_ready_result
        .map_or_else(|error| error, |value| panic!("pre-ready cancellation unexpectedly launched and returned command output; unexpected success: {value:?}"))
        .to_string();
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_TERMINAL=cancel-before-ready"
    ));
    assert!(pre_ready_error.contains("cancelled during startup"));
    assert!(pre_ready_events.lock().unwrap().is_empty());
    // The observer deliberately pauses every exec admission. Remove it before
    // issuing cleanup probes, otherwise those probes wait for a resume handle
    // that this test never consumes.
    drop(pre_ready_observer);
    let pre_ready_marker_absent = exec_supervision_host_command(
        &rt,
        CONTAINER_ID,
        format!(
            "test ! -e '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/cancel-before-ready'"
        ),
    )
    .await
    .exit_code
        == 0;
    let pre_ready_cgroup_restored =
        exec_supervision_cgroup(&rt, CONTAINER_ID).await.1 == baseline_members;
    let pre_ready_session_reaped = rt.lifecycle_diagnostics().await.unwrap().exec_sessions == 0
        && matches!(
            rt.cancel_exec(CANCEL_BEFORE_READY_ID).await,
            Err(vz_oci_macos::MacosOciError::ExecutionSessionNotFound { .. })
        );
    let pre_ready_post_case = rt
        .exec_container(
            CONTAINER_ID,
            ExecConfig {
                cmd: vec!["/bin/busybox".into(), "true".into()],
                timeout: Some(Duration::from_secs(10)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();
    assert!(pre_ready_marker_absent);
    assert!(pre_ready_cgroup_restored);
    assert!(pre_ready_session_reaped);
    assert_eq!(pre_ready_post_case.exit_code, 0);
    let cancel_before_ready = json!({
        "adapter": "streaming",
        "execution_id": CANCEL_BEFORE_READY_ID,
        "admission": "exec-before-guest-rpc",
        "container_ready_events": pre_ready_events.lock().unwrap().iter().filter(|event| matches!(event, InteractiveExecEvent::ContainerReady(_))).count(),
        "terminal_error": pre_ready_error,
        "marker_absent": pre_ready_marker_absent,
        "cgroup_restored": pre_ready_cgroup_restored,
        "session_reaped": pre_ready_session_reaped,
        "post_case_probe": pre_ready_post_case.exit_code == 0,
    });
    // Dropping the host execution future models abrupt transport ownership
    // loss. The registration drop guard must retain cleanup authority.
    const DROP_CASE: &str = "dropped-future";
    write_test_stderr(format_args!("VZ_EXEC_SUPERVISION_STAGE_START={DROP_CASE}"));
    let drop_rt = rt.clone();
    let drop_container_id = container_id.clone();
    let drop_task = tokio::spawn(async move {
        drop_rt
            .exec_container_streaming(
                &drop_container_id,
                ExecConfig {
                    execution_id: Some("exec-supervision-dropped-future".into()),
                    cmd: vec![
                        "/bin/sh".into(),
                        "-c".into(),
                        format!(
                            r#"marker=/vz-exec-supervision/{DROP_CASE}
pid=$$
start_time=$(awk '{{print $22}}' /proc/$pid/stat)
pgid=$(awk '{{print $5}}' /proc/$pid/stat)
cgroup_path=$(sed -n 's/^[^:]*:[^:]*://p' /proc/$pid/cgroup | head -n1)
/bin/sh -c 'while :; do /bin/busybox sleep 300; done' 'vz-child-{DROP_CASE}' &
child_pid=$!
child_start_time=$(awk '{{print $22}}' /proc/$child_pid/stat)
child_pgid=$(awk '{{print $5}}' /proc/$child_pid/stat)
printf 'pid=%s\nstart_time=%s\npgid=%s\nchild_pid=%s\nchild_start_time=%s\nchild_pgid=%s\ncgroup_path=%s\n' \
  "$pid" "$start_time" "$pgid" "$child_pid" "$child_start_time" "$child_pgid" "$cgroup_path" > "$marker"
while :; do wait "$child_pid"; done"#
                        ),
                    ],
                    timeout: Some(Duration::from_secs(20)),
                    ..ExecConfig::default()
                },
                |_| {},
            )
            .await
    });
    let drop_marker = exec_supervision_marker(&rt, CONTAINER_ID, DROP_CASE).await;
    write_test_stderr(format_args!("VZ_EXEC_SUPERVISION_STAGE_MARKER={DROP_CASE}"));
    let (_, drop_active_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
    let drop_identity = assert_exec_supervision_identity_live(
        &rt,
        CONTAINER_ID,
        &cgroup_path,
        DROP_CASE,
        &drop_marker,
        &drop_active_members,
    )
    .await;
    drop_task.abort();
    let drop_join = drop_task.await.map_or_else(
        |error| error,
        |value| {
            panic!("aborted execution future unexpectedly completed; unexpected success: {value:?}")
        },
    );
    assert!(drop_join.is_cancelled());
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_ABORTED={DROP_CASE}"
    ));
    let drop_identities = [
        (
            drop_identity["host_pid"].as_u64().unwrap() as u32,
            drop_identity["start_time"].as_u64().unwrap(),
        ),
        (
            drop_identity["child_host_pid"].as_u64().unwrap() as u32,
            drop_identity["child_start_time"].as_u64().unwrap(),
        ),
    ];
    await_exec_supervision_cleanup(
        &rt,
        CONTAINER_ID,
        &baseline_members,
        &drop_identities,
        DROP_CASE,
    )
    .await;
    write_test_stderr(format_args!("VZ_EXEC_SUPERVISION_STAGE_CLEAN={DROP_CASE}"));
    let drop_marker_removed = exec_supervision_host_command(
        &rt,
        CONTAINER_ID,
        format!(
            "rm -f '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{DROP_CASE}'; \
             test ! -e '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{DROP_CASE}'"
        ),
    )
    .await
    .exit_code
        == 0;
    let drop_post_case = rt
        .exec_container(
            CONTAINER_ID,
            ExecConfig {
                cmd: vec!["/bin/busybox".into(), "true".into()],
                timeout: Some(Duration::from_secs(10)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();
    assert!(drop_marker_removed);
    assert_eq!(drop_post_case.exit_code, 0);
    let dropped_future = json!({
        "adapter": "streaming",
        "execution_id": "exec-supervision-dropped-future",
        "join_cancelled": drop_join.is_cancelled(),
        "identity": drop_identity,
        "baseline_cgroup_members": baseline_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
        "active_cgroup_members": drop_active_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
        "cgroup_restored": exec_supervision_cgroup(&rt, CONTAINER_ID).await.1 == baseline_members,
        "leader_identity_absent": exec_supervision_identity_absent(&rt, CONTAINER_ID, drop_identities[0].0, drop_identities[0].1).await,
        "child_identity_absent": exec_supervision_identity_absent(&rt, CONTAINER_ID, drop_identities[1].0, drop_identities[1].1).await,
        "session_reaped": rt.lifecycle_diagnostics().await.unwrap().exec_sessions == 0,
        "marker_removed": drop_marker_removed,
        "post_case_probe": drop_post_case.exit_code == 0,
    });

    // Lose the authenticated response after the guest has dispatched the
    // uniquely selected command but before the host polls ContainerReady.
    // Request-ID reconciliation must retain lifecycle authority until the
    // exact leader and descendant are terminal and reaped.
    const RESPONSE_LOSS_CASE: &str = "response-loss-before-ready";
    const RESPONSE_LOSS_EXECUTION_ID: &str = "exec-supervision-response-loss-before-ready";
    const RESPONSE_LOSS_COMMAND: &str = "/vz-exec-response-loss-command/sh";
    const RESPONSE_LOSS_ERROR_MARKER: &str =
        "test-injected container exec response loss before readiness";
    let response_loss_selector =
        std::env::var("VZ_TEST_DROP_CONTAINER_EXEC_RESPONSE_BEFORE_READY_COMMAND")
            .unwrap_or_else(|error| panic!("strict exec-supervision harness must provide the exact response-loss command: {error:?}"));
    assert_eq!(response_loss_selector, RESPONSE_LOSS_COMMAND);
    let response_loss_dwell_ms = std::env::var("VZ_TEST_DROP_CONTAINER_EXEC_RESPONSE_DWELL_MS")
        .unwrap_or_else(|error| {
            panic!(
                "strict exec-supervision harness must provide the response-loss dwell: {error:?}"
            )
        })
        .parse::<u64>()
        .unwrap_or_else(|error| panic!("response-loss dwell must be an integer: {error:?}"));
    assert_eq!(response_loss_dwell_ms, 5_000);
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_START={RESPONSE_LOSS_CASE}"
    ));
    let response_loss_events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let response_loss_callback_events = std::sync::Arc::clone(&response_loss_events);
    let response_loss_rt = rt.clone();
    let response_loss_container_id = container_id.clone();
    let response_loss_task = tokio::spawn(async move {
        response_loss_rt
            .exec_container_streaming(
                &response_loss_container_id,
                ExecConfig {
                    execution_id: Some(RESPONSE_LOSS_EXECUTION_ID.into()),
                    cmd: vec![
                        RESPONSE_LOSS_COMMAND.into(),
                        "-c".into(),
                        format!(
                            r#"marker=/vz-exec-supervision/{RESPONSE_LOSS_CASE}
pid=$$
start_time=$(awk '{{print $22}}' /proc/$pid/stat)
pgid=$(awk '{{print $5}}' /proc/$pid/stat)
cgroup_path=$(sed -n 's/^[^:]*:[^:]*://p' /proc/$pid/cgroup | head -n1)
/bin/sh -c 'while :; do /bin/busybox sleep 300; done' 'vz-child-{RESPONSE_LOSS_CASE}' &
child_pid=$!
child_start_time=$(awk '{{print $22}}' /proc/$child_pid/stat)
child_pgid=$(awk '{{print $5}}' /proc/$child_pid/stat)
printf 'pid=%s\nstart_time=%s\npgid=%s\nchild_pid=%s\nchild_start_time=%s\nchild_pgid=%s\ncgroup_path=%s\n' \
  "$pid" "$start_time" "$pgid" "$child_pid" "$child_start_time" "$child_pgid" "$cgroup_path" > "$marker"
while :; do wait "$child_pid"; done"#
                        ),
                    ],
                    timeout: Some(Duration::from_secs(20)),
                    ..ExecConfig::default()
                },
                move |event| response_loss_callback_events.lock().unwrap().push(event),
            )
            .await
    });
    let response_loss_marker = exec_supervision_marker(&rt, CONTAINER_ID, RESPONSE_LOSS_CASE).await;
    let response_loss_marker_observed = response_loss_marker
        .get("start_time")
        .is_some_and(|value| !value.is_empty());
    assert!(response_loss_marker_observed);
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_MARKER={RESPONSE_LOSS_CASE}"
    ));
    let (_, response_loss_active_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
    assert!(response_loss_active_members.len() > baseline_members.len());
    let response_loss_identity = assert_exec_supervision_identity_live(
        &rt,
        CONTAINER_ID,
        &cgroup_path,
        RESPONSE_LOSS_CASE,
        &response_loss_marker,
        &response_loss_active_members,
    )
    .await;
    let response_loss_result = tokio::time::timeout(Duration::from_secs(15), response_loss_task)
        .await
        .unwrap_or_else(|error| {
            panic!("response-loss exec did not return its injected error: {error:?}")
        })
        .unwrap_or_else(|error| panic!("response-loss exec task panicked: {error:?}"));
    let response_loss_error = response_loss_result
        .map_or_else(
            |error| error,
            |value| {
                panic!(
                    "response-loss exec unexpectedly returned output; unexpected success: {value:?}"
                )
            },
        )
        .to_string();
    let response_loss_injected_error_observed =
        response_loss_error.contains(RESPONSE_LOSS_ERROR_MARKER);
    assert!(
        response_loss_injected_error_observed,
        "response-loss hook did not produce its stable injected error: {response_loss_error}"
    );
    let response_loss_reconciled = response_loss_error.contains("; reconciliation=TERMINAL_REAPED");
    assert!(
        response_loss_reconciled,
        "response-loss error did not expose exact request-ID terminal reconciliation: {response_loss_error}"
    );
    let response_loss_reconcile_outcome = if response_loss_reconciled {
        "TERMINAL_REAPED"
    } else {
        unreachable!("asserted exact reconciliation outcome")
    };
    assert!(
        response_loss_events.lock().unwrap().is_empty(),
        "response-loss case published readiness or terminal output before reconciliation"
    );
    let response_loss_identities = [
        (
            response_loss_identity["host_pid"].as_u64().unwrap() as u32,
            response_loss_identity["start_time"].as_u64().unwrap(),
        ),
        (
            response_loss_identity["child_host_pid"].as_u64().unwrap() as u32,
            response_loss_identity["child_start_time"].as_u64().unwrap(),
        ),
    ];
    await_exec_supervision_cleanup(
        &rt,
        CONTAINER_ID,
        &baseline_members,
        &response_loss_identities,
        RESPONSE_LOSS_CASE,
    )
    .await;
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_CLEAN={RESPONSE_LOSS_CASE}"
    ));
    let response_loss_diagnostics = rt.lifecycle_diagnostics().await.unwrap();
    let (_, response_loss_restored_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
    let response_loss_leader_absent = exec_supervision_identity_absent(
        &rt,
        CONTAINER_ID,
        response_loss_identities[0].0,
        response_loss_identities[0].1,
    )
    .await;
    let response_loss_child_absent = exec_supervision_identity_absent(
        &rt,
        CONTAINER_ID,
        response_loss_identities[1].0,
        response_loss_identities[1].1,
    )
    .await;
    let response_loss_stale_control_rejected = matches!(
        rt.cancel_exec(RESPONSE_LOSS_EXECUTION_ID).await,
        Err(vz_oci_macos::MacosOciError::ExecutionSessionNotFound { .. })
    );
    assert!(response_loss_leader_absent);
    assert!(response_loss_child_absent);
    assert!(response_loss_stale_control_rejected);
    assert_eq!(response_loss_diagnostics.exec_sessions, 0);
    assert_eq!(response_loss_restored_members, baseline_members);
    let response_loss_marker_removed = exec_supervision_host_command(
        &rt,
        CONTAINER_ID,
        format!(
            "rm -f '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{RESPONSE_LOSS_CASE}'; \
             test ! -e '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{RESPONSE_LOSS_CASE}'"
        ),
    )
    .await
    .exit_code
        == 0;
    assert!(response_loss_marker_removed);
    let response_loss_lifecycle_writer =
        assert_exec_supervision_lifecycle_writer_available(&rt, CONTAINER_ID, RESPONSE_LOSS_CASE)
            .await;
    let response_loss_post = rt
        .exec_container(
            CONTAINER_ID,
            ExecConfig {
                cmd: vec!["/bin/busybox".into(), "true".into()],
                timeout: Some(Duration::from_secs(10)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();
    let response_loss_post_case_probe = response_loss_post.exit_code == 0
        && exec_supervision_cgroup(&rt, CONTAINER_ID).await.1 == baseline_members;
    assert!(response_loss_post_case_probe);
    let response_loss_before_ready = json!({
        "attempts": 1,
        "adapter": "streaming",
        "target": "container",
        "execution_id": RESPONSE_LOSS_EXECUTION_ID,
        "fault_selector": response_loss_selector,
        "fault_dwell_ms": response_loss_dwell_ms,
        "injection_error_marker": RESPONSE_LOSS_ERROR_MARKER,
        "injected_error_observed": response_loss_injected_error_observed,
        "terminal_error": response_loss_error,
        "marker_observed_during_fault_dwell": response_loss_marker_observed,
        "interactive_events": response_loss_events.lock().unwrap().len(),
        "identity": response_loss_identity,
        "baseline_cgroup_members": baseline_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
        "active_cgroup_members": response_loss_active_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
        "request_id_reconcile_outcome": response_loss_reconcile_outcome,
        "request_id_reconciled_to_terminal_proof": response_loss_reconciled,
        "cgroup_restored": response_loss_restored_members == baseline_members,
        "leader_identity_absent": response_loss_leader_absent,
        "child_identity_absent": response_loss_child_absent,
        "session_reaped": response_loss_diagnostics.exec_sessions == 0,
        "stale_control_rejected": response_loss_stale_control_rejected,
        "lifecycle_writer_available": response_loss_lifecycle_writer,
        "marker_removed": response_loss_marker_removed,
        "post_case_probe": response_loss_post_case_probe,
    });

    // Abort while the guest RPC has returned Ready but its JoinHandle is
    // deliberately held before the outer owner can promote it. The pending
    // startup lease must retain both named and anonymous exec cleanup.
    let mut ready_before_owner_aborts = Vec::new();
    for (case_name, execution_id) in [
        (
            "ready-before-owner-named",
            Some("exec-supervision-ready-before-owner".to_string()),
        ),
        ("ready-before-owner-anonymous", None),
    ] {
        write_test_stderr(format_args!("VZ_EXEC_SUPERVISION_STAGE_START={case_name}"));
        let mut observer = rt.install_lifecycle_observer();
        let abort_rt = rt.clone();
        let abort_container_id = container_id.clone();
        let task_execution_id = execution_id.clone();
        let adapter = if execution_id.is_some() {
            "streaming"
        } else {
            "unary"
        };
        let command = format!(
            r#"marker=/vz-exec-supervision/{case_name}
pid=$$
start_time=$(awk '{{print $22}}' /proc/$pid/stat)
pgid=$(awk '{{print $5}}' /proc/$pid/stat)
cgroup_path=$(sed -n 's/^[^:]*:[^:]*://p' /proc/$pid/cgroup | head -n1)
/bin/sh -c 'while :; do /bin/busybox sleep 300; done' 'vz-child-{case_name}' &
child_pid=$!
child_start_time=$(awk '{{print $22}}' /proc/$child_pid/stat)
child_pgid=$(awk '{{print $5}}' /proc/$child_pid/stat)
printf 'pid=%s\nstart_time=%s\npgid=%s\nchild_pid=%s\nchild_start_time=%s\nchild_pgid=%s\ncgroup_path=%s\n' \
  "$pid" "$start_time" "$pgid" "$child_pid" "$child_start_time" "$child_pgid" "$cgroup_path" > "$marker"
while :; do wait "$child_pid"; done"#
        );
        let abort_task = tokio::spawn(async move {
            let config = ExecConfig {
                execution_id: task_execution_id.clone(),
                cmd: vec!["/bin/sh".into(), "-c".into(), command],
                timeout: Some(Duration::from_secs(20)),
                ..ExecConfig::default()
            };
            if task_execution_id.is_some() {
                abort_rt
                    .exec_container_streaming(&abort_container_id, config, |_| {})
                    .await
            } else {
                abort_rt
                    .exec_container_oci_unary(&abort_container_id, config)
                    .await
            }
        });
        let before_rpc = expect_lifecycle_admission(
            &mut observer,
            RuntimeLifecycleAdmissionKind::ExecBeforeGuestRpc,
            CONTAINER_ID,
        )
        .await;
        before_rpc.resume();
        let ready_before_owner = expect_lifecycle_admission(
            &mut observer,
            RuntimeLifecycleAdmissionKind::ExecGuestRpcReadyBeforeOwner,
            CONTAINER_ID,
        )
        .await;
        let marker = exec_supervision_marker(&rt, CONTAINER_ID, case_name).await;
        write_test_stderr(format_args!("VZ_EXEC_SUPERVISION_STAGE_MARKER={case_name}"));
        let (_, active_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
        assert!(
            active_members.len() > baseline_members.len(),
            "{case_name} did not add a target process to the exact container cgroup"
        );
        let identity = assert_exec_supervision_identity_live(
            &rt,
            CONTAINER_ID,
            &cgroup_path,
            case_name,
            &marker,
            &active_members,
        )
        .await;
        let pre_abort_diagnostics = rt.lifecycle_diagnostics().await.unwrap();
        let session_registered_before_abort = pre_abort_diagnostics.exec_sessions == 1;
        assert_eq!(
            pre_abort_diagnostics.exec_sessions,
            usize::from(execution_id.is_some()),
            "{case_name} had the wrong host session count before abort: {pre_abort_diagnostics:?}"
        );
        abort_task.abort();
        let abort_join = abort_task
            .await
            .map_or_else(|error| error, |value| panic!("ready-before-owner execution unexpectedly completed; unexpected success: {value:?}"));
        assert!(abort_join.is_cancelled());
        ready_before_owner.resume();
        drop(observer);
        let identities = [
            (
                identity["host_pid"].as_u64().unwrap() as u32,
                identity["start_time"].as_u64().unwrap(),
            ),
            (
                identity["child_host_pid"].as_u64().unwrap() as u32,
                identity["child_start_time"].as_u64().unwrap(),
            ),
        ];
        await_exec_supervision_cleanup(
            &rt,
            CONTAINER_ID,
            &baseline_members,
            &identities,
            case_name,
        )
        .await;
        write_test_stderr(format_args!("VZ_EXEC_SUPERVISION_STAGE_CLEAN={case_name}"));
        let stale_control_rejected = if let Some(execution_id) = execution_id.as_deref() {
            Some(matches!(
                rt.signal_exec(execution_id, "SIGTERM").await,
                Err(vz_oci_macos::MacosOciError::ExecutionSessionNotFound { .. })
            ))
        } else {
            None
        };
        assert!(
            stale_control_rejected.unwrap_or(true),
            "{case_name} retained a named control session after cleanup"
        );
        let marker_removed = exec_supervision_host_command(
            &rt,
            CONTAINER_ID,
            format!(
                "rm -f '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{case_name}'; \
                 test ! -e '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{case_name}'"
            ),
        )
        .await
        .exit_code
            == 0;
        assert!(marker_removed, "{case_name} marker did not clean up");
        let post_case = rt
            .exec_container(
                CONTAINER_ID,
                ExecConfig {
                    cmd: vec![
                        "/bin/busybox".into(),
                        "printf".into(),
                        format!("post-case:{case_name}"),
                    ],
                    timeout: Some(Duration::from_secs(10)),
                    ..ExecConfig::default()
                },
            )
            .await
            .unwrap();
        let post_case_probe = post_case.exit_code == 0
            && post_case.stdout.replace('\r', "") == format!("post-case:{case_name}");
        assert!(post_case_probe, "{case_name} left the container unhealthy");
        let (_, restored_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
        assert_eq!(
            restored_members, baseline_members,
            "{case_name} did not retain exact cgroup restoration after its health probe"
        );
        let diagnostics = rt.lifecycle_diagnostics().await.unwrap();
        assert_eq!(
            diagnostics.exec_sessions, 0,
            "{case_name} leaked a host execution session: {diagnostics:?}"
        );
        ready_before_owner_aborts.push(json!({
            "case": case_name,
            "adapter": adapter,
            "execution_id": execution_id,
            "admission": "exec-guest-rpc-ready-before-owner",
            "join_cancelled": abort_join.is_cancelled(),
            "identity": identity,
            "baseline_cgroup_members": baseline_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
            "active_cgroup_members": active_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
            "session_registered_before_abort": session_registered_before_abort,
            "cgroup_restored": restored_members == baseline_members,
            "leader_identity_absent": exec_supervision_identity_absent(&rt, CONTAINER_ID, identities[0].0, identities[0].1).await,
            "child_identity_absent": exec_supervision_identity_absent(&rt, CONTAINER_ID, identities[1].0, identities[1].1).await,
            "session_reaped": diagnostics.exec_sessions == 0,
            "stale_control_rejected": stale_control_rejected,
            "marker_removed": marker_removed,
            "post_case_probe": post_case_probe,
        }));
    }

    // Killing the exact outer trampoline exercises the sentinel PDEATHSIG
    // path rather than any cooperative control-plane cancellation.
    const OUTER_KILL_CASE: &str = "outer-trampoline-kill";
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_START={OUTER_KILL_CASE}"
    ));
    let outer_events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let outer_callback_events = std::sync::Arc::clone(&outer_events);
    let outer_rt = rt.clone();
    let outer_container_id = container_id.clone();
    let outer_task = tokio::spawn(async move {
        outer_rt
            .exec_container_streaming(
                &outer_container_id,
                ExecConfig {
                    execution_id: Some("exec-supervision-outer-trampoline-kill".into()),
                    cmd: vec![
                        "/bin/sh".into(),
                        "-c".into(),
                        format!(
                            r#"marker=/vz-exec-supervision/{OUTER_KILL_CASE}
pid=$$
start_time=$(awk '{{print $22}}' /proc/$pid/stat)
pgid=$(awk '{{print $5}}' /proc/$pid/stat)
cgroup_path=$(sed -n 's/^[^:]*:[^:]*://p' /proc/$pid/cgroup | head -n1)
/bin/sh -c 'while :; do /bin/busybox sleep 300; done' 'vz-child-{OUTER_KILL_CASE}' &
child_pid=$!
child_start_time=$(awk '{{print $22}}' /proc/$child_pid/stat)
child_pgid=$(awk '{{print $5}}' /proc/$child_pid/stat)
printf 'pid=%s\nstart_time=%s\npgid=%s\nchild_pid=%s\nchild_start_time=%s\nchild_pgid=%s\ncgroup_path=%s\n' \
  "$pid" "$start_time" "$pgid" "$child_pid" "$child_start_time" "$child_pgid" "$cgroup_path" > "$marker"
while :; do wait "$child_pid"; done"#
                        ),
                    ],
                    timeout: Some(Duration::from_secs(20)),
                    ..ExecConfig::default()
                },
                move |event| outer_callback_events.lock().unwrap().push(event),
            )
            .await
    });
    let outer_marker = exec_supervision_marker(&rt, CONTAINER_ID, OUTER_KILL_CASE).await;
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_MARKER={OUTER_KILL_CASE}"
    ));
    let (_, outer_active_members) = exec_supervision_cgroup(&rt, CONTAINER_ID).await;
    let outer_target_identity = assert_exec_supervision_identity_live(
        &rt,
        CONTAINER_ID,
        &cgroup_path,
        OUTER_KILL_CASE,
        &outer_marker,
        &outer_active_members,
    )
    .await;
    let outer_identity = exec_supervision_outer_identity(
        &rt,
        CONTAINER_ID,
        &cgroup_path,
        OUTER_KILL_CASE,
        outer_target_identity["host_pid"].as_u64().unwrap() as u32,
    )
    .await;
    assert!(
        outer_active_members
            .iter()
            .all(|member| member.0 != outer_identity["pid"].as_u64().unwrap() as u32),
        "outer supervisor consumed a target cgroup slot: {outer_active_members:?}"
    );
    exec_supervision_host_command(
        &rt,
        CONTAINER_ID,
        format!(
            "test \"$(awk '{{print $22}}' /proc/{}/stat)\" = '{}'; kill -KILL {}",
            outer_identity["pid"].as_u64().unwrap(),
            outer_identity["start_time"].as_u64().unwrap(),
            outer_identity["pid"].as_u64().unwrap(),
        ),
    )
    .await;
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_KILLED={OUTER_KILL_CASE}"
    ));
    let outer_output = tokio::time::timeout(Duration::from_secs(10), outer_task)
        .await
        .unwrap_or_else(|error| {
            panic!("outer trampoline death did not terminate the host task: {error:?}")
        })
        .unwrap_or_else(|error| panic!("outer trampoline task panicked: {error:?}"))
        .unwrap_or_else(|error| {
            panic!("outer trampoline death did not preserve terminal output: {error:?}")
        });
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_TERMINAL={OUTER_KILL_CASE}"
    ));
    assert_eq!(outer_output.exit_code, 137);
    assert!(matches!(
        outer_events.lock().unwrap().last(),
        Some(InteractiveExecEvent::Exit(137))
    ));
    let outer_identities = [
        (
            outer_target_identity["host_pid"].as_u64().unwrap() as u32,
            outer_target_identity["start_time"].as_u64().unwrap(),
        ),
        (
            outer_target_identity["child_host_pid"].as_u64().unwrap() as u32,
            outer_target_identity["child_start_time"].as_u64().unwrap(),
        ),
        (
            outer_identity["pid"].as_u64().unwrap() as u32,
            outer_identity["start_time"].as_u64().unwrap(),
        ),
    ];
    await_exec_supervision_cleanup(
        &rt,
        CONTAINER_ID,
        &baseline_members,
        &outer_identities,
        OUTER_KILL_CASE,
    )
    .await;
    write_test_stderr(format_args!(
        "VZ_EXEC_SUPERVISION_STAGE_CLEAN={OUTER_KILL_CASE}"
    ));
    let outer_marker_removed = exec_supervision_host_command(
        &rt,
        CONTAINER_ID,
        format!(
            "rm -f '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{OUTER_KILL_CASE}'; \
             test ! -e '/run/vz-oci/containers/{CONTAINER_ID}/merged/vz-exec-supervision/{OUTER_KILL_CASE}'"
        ),
    )
    .await
    .exit_code
        == 0;
    let outer_post_case = rt
        .exec_container(
            CONTAINER_ID,
            ExecConfig {
                cmd: vec!["/bin/busybox".into(), "true".into()],
                timeout: Some(Duration::from_secs(10)),
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap();
    assert!(outer_marker_removed);
    assert_eq!(outer_post_case.exit_code, 0);
    let outer_trampoline_kill = json!({
        "adapter": "streaming",
        "execution_id": "exec-supervision-outer-trampoline-kill",
        "exit_code": outer_output.exit_code,
        "identity": outer_target_identity,
        "outer_identity": outer_identity,
        "baseline_cgroup_members": baseline_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
        "active_cgroup_members": outer_active_members.iter().map(|(pid, start, pgid)| json!({"pid": pid, "start_time": start, "pgid": pgid})).collect::<Vec<_>>(),
        "cgroup_restored": exec_supervision_cgroup(&rt, CONTAINER_ID).await.1 == baseline_members,
        "outer_identity_absent": exec_supervision_identity_absent(&rt, CONTAINER_ID, outer_identities[2].0, outer_identities[2].1).await,
        "leader_identity_absent": exec_supervision_identity_absent(&rt, CONTAINER_ID, outer_identities[0].0, outer_identities[0].1).await,
        "child_identity_absent": exec_supervision_identity_absent(&rt, CONTAINER_ID, outer_identities[1].0, outer_identities[1].1).await,
        "session_reaped": rt.lifecycle_diagnostics().await.unwrap().exec_sessions == 0,
        "marker_removed": outer_marker_removed,
        "post_case_probe": outer_post_case.exit_code == 0,
    });

    rt.stop_container(CONTAINER_ID, true, None, None)
        .await
        .unwrap();
    rt.remove_container(CONTAINER_ID).await.unwrap();
    let final_diagnostics = rt.lifecycle_diagnostics().await.unwrap();
    let final_zero_leaks = final_diagnostics.vm_handles == 0
        && final_diagnostics.container_routes == 0
        && final_diagnostics.exec_bindings == 0
        && final_diagnostics.active_lifecycles == 0
        && final_diagnostics.exec_sessions == 0
        && final_diagnostics.setup_restore_entries == 0
        && final_diagnostics.rootfs_directories == 0
        && final_diagnostics.overlay_cleanup_pending == 0
        && rt
            .list_containers()
            .unwrap()
            .iter()
            .all(|container| container.id != CONTAINER_ID)
        && !tmp.path().join("rootfs").join(CONTAINER_ID).exists();
    assert!(
        final_zero_leaks,
        "exec supervision scenario leaked runtime resources: {final_diagnostics:?}"
    );
    assert_eq!(cells.len(), 12);
    write_exec_supervision_evidence(&json!({
        "schema_version": 4,
        "scenario": "runtime-exec-supervision",
        "build": build_identity,
        "container_id": CONTAINER_ID,
        "cgroup_path": cgroup_path,
        "matrix": cells,
        "normal_exit": normal_exit,
        "cancellation": cancellation,
        "cancel_before_ready": cancel_before_ready,
        "pre_spawn_rejection": pre_spawn_rejection,
        "slow_live_consumer": slow_live_consumer,
        "response_loss_before_ready": response_loss_before_ready,
        "dropped_future": dropped_future,
        "ready_before_owner_aborts": ready_before_owner_aborts,
        "outer_trampoline_kill": outer_trampoline_kill,
        "final": {
            "zero_leaks": final_zero_leaks,
            "tracked_container_absent": true,
            "metadata_absent": true,
            "rootfs_absent": true,
            "diagnostics": format!("{final_diagnostics:?}"),
        },
    }));
}

// Container logs.

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
            "alpine:3.20",
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
            "alpine:3.20",
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
                write_test_stderr(format_args!(
                    "port forward connect attempt {attempt}/5 failed: {e}, retrying..."
                ));
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => panic!("port forwarding connection failed after 5 attempts: {e}"),
        }
    }
    let mut conn = conn.unwrap();
    let mut buf = vec![0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(10), conn.read(&mut buf))
        .await
        .unwrap_or_else(|error| panic!("port forward read timed out: {error:?}"))
        .unwrap_or_else(|error| panic!("port forward read failed: {error:?}"));
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

    let id1 = rt.pull("alpine:3.20").await.unwrap();
    let id2 = rt.pull("alpine:3.20").await.unwrap();
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
            "alpine:3.20",
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
    config: ExecConfig,
) -> Result<vz::protocol::ExecOutput, vz_oci_macos::MacosOciError> {
    exec_via_semantics_adapter_with_events(rt, container_id, adapter, config)
        .await
        .0
}

async fn exec_via_semantics_adapter_with_events(
    rt: &Runtime,
    container_id: &str,
    adapter: ExecSemanticsAdapter,
    mut config: ExecConfig,
) -> (
    Result<vz::protocol::ExecOutput, vz_oci_macos::MacosOciError>,
    Vec<InteractiveExecEvent>,
) {
    let mut events = Vec::new();
    let result = match adapter {
        ExecSemanticsAdapter::OciUnary => rt.exec_container_oci_unary(container_id, config).await,
        ExecSemanticsAdapter::StreamingPipe => {
            rt.exec_container_streaming(container_id, config, |event| events.push(event))
                .await
        }
        ExecSemanticsAdapter::Pty => {
            config.pty = true;
            config.execution_id = Some(format!("exec-semantics-{}", adapter.name()));
            rt.exec_container_streaming(container_id, config, |event| events.push(event))
                .await
        }
    };
    (result, events)
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
                exec_via_semantics_adapter_with_events(
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

    for (adapter, sentinel_name, (result, events)) in missing_identity_results {
        let error = result.map_or_else(|error| error, |value| panic!("container setup failure must fail before readiness; unexpected success: {value:?}"));
        let vz_oci_macos::MacosOciError::Linux(vz_linux::LinuxError::Protocol(diagnostic)) = error
        else {
            panic!(
                "{} returned the wrong pre-readiness error: {error}",
                adapter.name()
            );
        };
        let rejection = match adapter {
            ExecSemanticsAdapter::Pty => "PTY exec rejected before readiness",
            ExecSemanticsAdapter::OciUnary | ExecSemanticsAdapter::StreamingPipe => {
                "exec rejected before readiness"
            }
        };
        assert_eq!(
            diagnostic,
            format!(
                "exec stream reported an error: {rejection}; spawned process reaped: container trampoline failed before readiness: container exec user `vz-user-does-not-exist` does not exist"
            )
        );
        assert!(
            events.is_empty(),
            "{} emitted events before rejecting the missing identity: {events:?}",
            adapter.name()
        );
        eprintln!(
            "container exec missing-identity evidence ({}): diagnostic={:?} events=0",
            adapter.name(),
            diagnostic
        );
        assert!(
            !sentinel_dir.join(&sentinel_name).exists(),
            "{} ran the sentinel command for a missing named identity",
            adapter.name()
        );
    }
}

// ── Container exec image-default inheritance ───────────────────

const EXEC_DEFAULTS_IMAGE: &str = "localhost/vz-e2e-exec-defaults:latest";
const EXEC_DEFAULTS_LAYER_MEDIA_TYPE: &str = "application/vnd.oci.image.layer.v1.tar";

#[derive(Debug)]
struct ExecDefaultsImageFixture {
    reference: String,
    manifest_digest: String,
    config_digest: String,
    layer_digest: String,
    busybox_digest: String,
}

fn sha256_digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

fn append_exec_defaults_tar_entry(
    archive: &mut TarBuilder<Vec<u8>>,
    path: &str,
    entry_type: EntryType,
    mode: u32,
    bytes: &[u8],
) {
    let mut header = Header::new_gnu();
    header.set_entry_type(entry_type);
    header.set_mode(mode);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(bytes.len() as u64);
    header.set_cksum();
    archive
        .append_data(&mut header, path, Cursor::new(bytes))
        .unwrap();
}

fn build_exec_defaults_layer(busybox: &[u8]) -> Vec<u8> {
    let mut archive = TarBuilder::new(Vec::new());
    for directory in [
        "bin/",
        "dev/",
        "etc/",
        "proc/",
        "run/",
        "sys/",
        "tmp/",
        "workspace/",
        "workspace/image-default/",
        "workspace/override/",
    ] {
        append_exec_defaults_tar_entry(
            &mut archive,
            directory,
            EntryType::Directory,
            if directory == "tmp/" { 0o1777 } else { 0o755 },
            &[],
        );
    }
    append_exec_defaults_tar_entry(
        &mut archive,
        "bin/busybox",
        EntryType::Regular,
        0o755,
        busybox,
    );
    append_exec_defaults_tar_entry(
        &mut archive,
        "etc/passwd",
        EntryType::Regular,
        0o644,
        b"root:x:0:0:root:/root:/bin/busybox\ndeveloper:x:1234:2345:Developer:/workspace/image-default:/bin/busybox\n",
    );
    append_exec_defaults_tar_entry(
        &mut archive,
        "etc/group",
        EntryType::Regular,
        0o644,
        b"root:x:0:root\ndevprimary:x:2345:\ndevextra:x:3456:developer\ndevextra2:x:4567:other,developer\n",
    );
    archive.into_inner().unwrap()
}

fn install_exec_defaults_image(data_dir: &Path) -> ExecDefaultsImageFixture {
    let bundle_dir = std::env::var_os("VZ_LINUX_DEVELOPER_BUNDLE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap();
    let busybox_path = bundle_dir.join("busybox");
    let busybox = std::fs::read(&busybox_path).unwrap();
    assert!(
        busybox.len() >= 20 && &busybox[..4] == b"\x7fELF",
        "release BusyBox is not an ELF binary: {}",
        busybox_path.display()
    );
    assert_eq!(
        &busybox[18..20],
        &[0xb7, 0x00],
        "release BusyBox is not Linux/arm64 (ELF e_machine): {}",
        busybox_path.display()
    );

    let busybox_digest = sha256_digest(&busybox);
    let layer = build_exec_defaults_layer(&busybox);
    let layer_digest = sha256_digest(&layer);
    let config = serde_json::to_vec(&json!({
        "architecture": "arm64",
        "config": {
            "Cmd": ["/bin/busybox", "sleep", "300"],
            "Env": [
                "PATH=/vz/image-default/bin",
                "TERM=vz-image-default",
                "VZ_IMAGE=from-image",
                "VZ_OVERRIDE=from-image"
            ],
            "User": "developer",
            "WorkingDir": "/workspace/image-default"
        },
        "os": "linux",
        "rootfs": {
            "diff_ids": [&layer_digest],
            "type": "layers"
        }
    }))
    .unwrap();
    let config_digest = sha256_digest(&config);
    let manifest = serde_json::to_vec(&json!({
        "config": {
            "digest": &config_digest,
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": config.len()
        },
        "layers": [{
            "digest": &layer_digest,
            "mediaType": EXEC_DEFAULTS_LAYER_MEDIA_TYPE,
            "size": layer.len()
        }],
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "schemaVersion": 2
    }))
    .unwrap();
    let manifest_digest = sha256_digest(&manifest);

    let store = ImageStore::new(data_dir.to_path_buf());
    store.ensure_layout().unwrap();
    store
        .write_layer_blob(&layer_digest, EXEC_DEFAULTS_LAYER_MEDIA_TYPE, &layer)
        .unwrap();
    store
        .write_manifest_json(&manifest_digest, &manifest)
        .unwrap();
    store.write_config_json(&manifest_digest, &config).unwrap();
    let canonical_reference = Reference::from_str(EXEC_DEFAULTS_IMAGE).unwrap().whole();
    store
        .write_reference(&canonical_reference, &manifest_digest)
        .unwrap();
    if canonical_reference != EXEC_DEFAULTS_IMAGE {
        store
            .write_reference(EXEC_DEFAULTS_IMAGE, &manifest_digest)
            .unwrap();
    }

    ExecDefaultsImageFixture {
        reference: EXEC_DEFAULTS_IMAGE.to_string(),
        manifest_digest,
        config_digest,
        layer_digest,
        busybox_digest,
    }
}

async fn exec_via_defaults_adapter(
    rt: &Runtime,
    container_id: &str,
    case: &str,
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
            config.execution_id = Some(format!("exec-defaults-{case}-{}", adapter.name()));
            rt.exec_container_streaming(container_id, config, |_| {})
                .await
        }
    }
}

fn exec_defaults_probe_config(explicit_override: bool) -> ExecConfig {
    ExecConfig {
        cmd: vec![
            "/bin/busybox".to_string(),
            "sh".to_string(),
            "-c".to_string(),
            EXEC_SEMANTICS_PROBE.to_string(),
        ],
        working_dir: explicit_override.then(|| "/workspace/override".to_string()),
        env: if explicit_override {
            vec![
                ("PATH".to_string(), "/vz/exec-override/bin".to_string()),
                ("TERM".to_string(), "vz-exec-override".to_string()),
                ("VZ_EXEC".to_string(), "from-exec".to_string()),
                ("VZ_OVERRIDE".to_string(), "from-exec".to_string()),
            ]
        } else {
            Vec::new()
        },
        user: explicit_override.then(|| "0:0".to_string()),
        timeout: Some(Duration::from_secs(30)),
        ..ExecConfig::default()
    }
}

/// Build a deterministic offline OCI fixture from the pinned release BusyBox,
/// then prove image `User` and `WorkingDir` inheritance (and explicit exec
/// precedence) through every OCI exec adapter.
#[allow(clippy::print_stderr)]
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn container_exec_inherits_image_process_defaults() {
    if !require_virtualization_entitlement() {
        return;
    }
    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let fixture = install_exec_defaults_image(tmp.path());
    let installed_config = parse_image_config_summary_from_store(
        &ImageStore::new(tmp.path().to_path_buf()),
        &fixture.manifest_digest,
    )
    .unwrap();
    eprintln!(
        "container exec defaults OCI fixture evidence: reference={} manifest={} config={} layer={} busybox={} image_user={:?} image_working_dir={:?}",
        fixture.reference,
        fixture.manifest_digest,
        fixture.config_digest,
        fixture.layer_digest,
        fixture.busybox_digest,
        installed_config.user,
        installed_config.working_dir
    );
    assert_eq!(installed_config.user.as_deref(), Some("developer"));
    assert_eq!(
        installed_config.working_dir.as_deref(),
        Some("/workspace/image-default")
    );
    let rt = test_runtime(tmp.path());

    let create_result = rt
        .create_container(
            &fixture.reference,
            RunConfig {
                cmd: vec!["/bin/busybox".into(), "sleep".into(), "300".into()],
                execution_mode: ExecutionMode::OciRuntime,
                env: vec![
                    ("VZ_CREATE".into(), "from-create".into()),
                    ("VZ_OVERRIDE".into(), "from-create".into()),
                ],
                ..RunConfig::default()
            },
        )
        .await;

    let mut results = Vec::new();
    let mut stop_result = None;
    let mut remove_result = None;
    if let Ok(container_id) = &create_result {
        for adapter in [
            ExecSemanticsAdapter::OciUnary,
            ExecSemanticsAdapter::StreamingPipe,
            ExecSemanticsAdapter::Pty,
        ] {
            results.push((
                "image-default",
                adapter,
                exec_via_defaults_adapter(
                    &rt,
                    container_id,
                    "image-default",
                    adapter,
                    exec_defaults_probe_config(false),
                )
                .await,
            ));
            results.push((
                "explicit-override",
                adapter,
                exec_via_defaults_adapter(
                    &rt,
                    container_id,
                    "explicit-override",
                    adapter,
                    exec_defaults_probe_config(true),
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

    assert_eq!(results.len(), 6);
    let expected_inherited_environment = format!(
        "PATH=/vz/image-default/bin\nTERM=vz-image-default\nVZ_CONTAINER_ID={container_id}\nVZ_CREATE=from-create\nVZ_IMAGE=from-image\nVZ_OVERRIDE=from-create\n"
    )
    .into_bytes();
    let expected_override_environment = format!(
        "PATH=/vz/exec-override/bin\nTERM=vz-exec-override\nVZ_CONTAINER_ID={container_id}\nVZ_CREATE=from-create\nVZ_EXEC=from-exec\nVZ_IMAGE=from-image\nVZ_OVERRIDE=from-exec\n"
    )
    .into_bytes();
    let mut inherited_environments = Vec::new();
    let mut override_environments = Vec::new();
    for (case, adapter, result) in results {
        let actual = parse_exec_semantics_evidence(adapter, &result.unwrap());
        eprintln!(
            "container exec defaults evidence ({case}/{}): uid={} gid={} groups={:?} cwd={} environment=\n{}",
            adapter.name(),
            actual.uid,
            actual.gid,
            actual.supplementary_groups,
            actual.cwd,
            String::from_utf8_lossy(&actual.canonical_environment)
        );
        match case {
            "image-default" => {
                assert_eq!(actual.uid, 1234, "{} inherited uid", adapter.name());
                assert_eq!(actual.gid, 2345, "{} inherited gid", adapter.name());
                assert_eq!(
                    actual.supplementary_groups,
                    vec![2345, 3456, 4567],
                    "{} inherited supplementary groups",
                    adapter.name()
                );
                assert_eq!(
                    actual.cwd,
                    "/workspace/image-default",
                    "{} inherited working directory",
                    adapter.name()
                );
                assert_eq!(
                    actual.canonical_environment,
                    expected_inherited_environment,
                    "{case}/{} received an unexpected or leaked environment",
                    adapter.name()
                );
                inherited_environments.push(actual.canonical_environment);
            }
            "explicit-override" => {
                assert_eq!(actual.uid, 0, "{} override uid", adapter.name());
                assert_eq!(actual.gid, 0, "{} override gid", adapter.name());
                assert_eq!(
                    actual.supplementary_groups,
                    vec![0],
                    "{} override supplementary groups",
                    adapter.name()
                );
                assert_eq!(
                    actual.cwd,
                    "/workspace/override",
                    "{} override working directory",
                    adapter.name()
                );
                assert_eq!(
                    actual.canonical_environment,
                    expected_override_environment,
                    "{case}/{} received an unexpected or leaked environment",
                    adapter.name()
                );
                override_environments.push(actual.canonical_environment);
            }
            unexpected => panic!("unexpected defaults test case: {unexpected}"),
        }
    }
    assert_eq!(inherited_environments.len(), 3);
    assert_eq!(override_environments.len(), 3);
    for environment in &inherited_environments {
        assert_eq!(
            environment, &inherited_environments[0],
            "inherited canonical environment differs across adapters"
        );
    }
    for environment in &override_environments {
        assert_eq!(
            environment, &override_environments[0],
            "override canonical environment differs across adapters"
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
    if rt.pull("alpine:3.20").await.is_err() {
        write_test_stderr(format_args!(
            "WARN: pull failed (rate limit?), assuming image is cached"
        ));
    }

    let stack_id = "e2e-net";

    // 1. Boot shared VM.
    rt.boot_shared_vm(stack_id, vec![], Default::default())
        .await
        .unwrap();

    // 1b. Diagnostic sanity: has_shared_vm reports a booted stack and false
    // for an unknown one without exposing a lifecycle-mutable VM handle.
    assert!(rt.has_shared_vm(stack_id).await);
    assert!(!rt.has_shared_vm("not-booted").await);

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
            "alpine:3.20",
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
            "alpine:3.20",
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
    write_test_stderr(format_args!(
        "db /etc/hosts evidence:\n{}",
        hosts_evidence.stdout
    ));
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

    if rt.pull("alpine:3.20").await.is_err() {
        write_test_stderr(format_args!(
            "WARN: pull failed (rate limit?), assuming image is cached"
        ));
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
            "alpine:3.20",
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
