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
        .expect("container exec omitted guest readiness generation")
}

fn write_container_id_ownership_evidence(evidence: &serde_json::Value) {
    let rendered = serde_json::to_string_pretty(evidence).unwrap();
    eprintln!("VZ_CONTAINER_ID_OWNERSHIP_EVIDENCE={rendered}");
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

    const IMAGE: &str = "alpine:latest";
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
            .expect("first standalone create never reached ID admission")
            .expect("standalone lifecycle observer closed unexpectedly");
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
    .expect("concurrent standalone duplicate queued instead of failing closed");
    let in_flight_standalone_duplicate_rejected =
        standalone_duplicate_id_was_rejected(&in_flight_standalone_duplicate);
    first_create_admission.resume();
    let first_standalone_id = tokio::time::timeout(Duration::from_secs(120), first_standalone)
        .await
        .expect("first standalone create timed out after admission release")
        .expect("first standalone create task panicked")
        .expect("first standalone create failed");
    assert_eq!(first_standalone_id, CONTAINER_ID);
    drop(standalone_admissions);

    let standalone_a = container_generation_evidence(&rt, CONTAINER_ID).await;
    let active_standalone_duplicate = tokio::time::timeout(
        Duration::from_secs(10),
        rt.create_container(IMAGE, standalone_config("standalone-active-duplicate")),
    )
    .await
    .expect("active standalone duplicate queued instead of failing closed");
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
        .expect("first stack create never entered its gated setup transaction");
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
        .expect("duplicate stack create queued instead of failing closed");
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
        .expect("failed to inspect duplicate setup marker");
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
        .expect("first stack create did not finish after setup gate release")
        .expect("first stack create task panicked");
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
        .expect("failed to inspect failed-setup guest cleanup");
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
        .expect("generation-A exec did not reach its command gate");
    assert_eq!(exec_marker.exit_code, 0);
    guest_ready_boundary.resume();

    let stack_a_ready = tokio::time::timeout(Duration::from_secs(30), ready_receiver)
        .await
        .expect("exec never published guest target-ready acknowledgement")
        .expect("exec ended before reporting its pinned generation");
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
        .expect("generation-A exec did not emit its owner sentinel")
        .expect("generation-A stdout observer closed");
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
        .expect("stop/remove/recreate transaction timed out")
        .expect("stop/remove/recreate transaction failed");
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
        .expect("old generation exec did not terminate after recreate")
        .expect("old generation exec task panicked");
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
    let guest_resources_clean = guest_inventory.stdout.contains("overlay=absent")
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
            "guest_inventory": guest_inventory.stdout,
            "baseline_network_inventory": baseline_network_inventory.stdout,
            "final_network_inventory": final_network_inventory.stdout,
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
        "guest resources leaked: {}",
        guest_inventory.stdout
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
        match adapter {
            ExecSemanticsAdapter::OciUnary => {
                let output = result.expect("unary setup failure must remain result-shaped");
                assert_ne!(
                    output.exit_code, 0,
                    "unary accepted a missing named identity"
                );
                let message = format!("{}{}", output.stdout, output.stderr);
                eprintln!(
                    "container exec missing-identity evidence ({}): exit_code={} diagnostic={message:?}",
                    adapter.name(),
                    output.exit_code
                );
                assert!(
                    message.contains("vz-user-does-not-exist")
                        && message.contains("does not exist"),
                    "unary returned an unactionable missing-user error: {message}"
                );
            }
            ExecSemanticsAdapter::StreamingPipe | ExecSemanticsAdapter::Pty => {
                let error = result.expect_err("streaming setup failure must fail before readiness");
                let vz_oci_macos::MacosOciError::Linux(vz_linux::LinuxError::Grpc(status)) = error
                else {
                    panic!(
                        "{} returned the wrong pre-readiness error: {error}",
                        adapter.name()
                    );
                };
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                assert_eq!(
                    status.message(),
                    "container trampoline failed before readiness: container exec user `vz-user-does-not-exist` does not exist"
                );
                assert!(
                    events.is_empty(),
                    "{} emitted events before rejecting the missing identity: {events:?}",
                    adapter.name()
                );
                eprintln!(
                    "container exec missing-identity evidence ({}): status={:?} diagnostic={:?} events=0",
                    adapter.name(),
                    status.code(),
                    status.message()
                );
            }
        }
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
    if rt.pull("alpine:latest").await.is_err() {
        eprintln!("WARN: pull failed (rate limit?), assuming image is cached");
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
