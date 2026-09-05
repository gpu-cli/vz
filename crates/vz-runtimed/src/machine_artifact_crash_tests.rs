#![cfg(unix)]
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::json;
use sha2::{Digest, Sha256};
use vz_linux::{KernelProfile, verify_kernel_bundle_read_only};
use vz_runtime_contract::{
    Architecture, CapabilitySet, EnvironmentId, EnvironmentSpec, HostSpec, MachineCapability,
    MachineId, MachineProfile, MachineResources, MachineSpec, OperatingSystem, ProjectDefinition,
    ProjectId, ResourceOwner, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};

use super::{MachineArtifactStoreError, load_machine_artifacts, pin_machine_artifacts};
use crate::machine_runtime_registry::{MachineRuntimeAdmission, MachineRuntimeRegistry};
use crate::machine_target_resolver::{
    LINUX_APPLIANCE_IMAGE, LinuxTargetCatalogEntry, MachineTargetCatalog, MachineTargetResolver,
    ResolvedLinuxMachineTarget,
};

const CHILD_ENV: &str = "VZ_ARTIFACT_PIN_CRASH_CHILD";
const ROOT_ENV: &str = "VZ_ARTIFACT_PIN_CRASH_ROOT";
const PHASE_ENV: &str = "VZ_ARTIFACT_PIN_CRASH_PHASE";
const TEST_NAME: &str =
    "machine_artifact_store::crash_tests::pin_publication_survives_sigkill_at_every_checkpoint";
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CHILD_OUTPUT: usize = 64 * 1024;
const FILES: [&str; 4] = ["vmlinux", "initramfs.img", "youki", "version.json"];
const PHASES: [(&str, bool); 10] = [
    ("pending_created", false),
    ("vmlinux_synced", false),
    ("initramfs_synced", false),
    ("youki_synced", false),
    ("version_synced", false),
    ("configuration_synced", false),
    ("bundle_synced", false),
    ("directory_synced", false),
    ("published", true),
    ("parent_synced", true),
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct TreeEntry {
    relative_path: PathBuf,
    device: u64,
    inode: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    links: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build crash-test runtime")
}

fn host() -> HostSpec {
    HostSpec {
        os: OperatingSystem::Macos,
        arch: Architecture::Aarch64,
    }
}

fn owner() -> ResourceOwner {
    ResourceOwner {
        project_id: ProjectId::new("prj_artifact_crash").expect("valid Project ID"),
        environment_id: EnvironmentId::new("env_artifact_crash").expect("valid Environment ID"),
        machine_id: Some(MachineId::new("mch_artifact_crash").expect("valid Machine ID")),
    }
}

async fn write_bundle_and_resolve(source: &Path) -> (ResolvedLinuxMachineTarget, MachineSpec) {
    fs::create_dir(source).expect("create source bundle");
    fs::write(source.join("vmlinux"), b"crash-vmlinux").expect("write vmlinux");
    fs::write(source.join("initramfs.img"), b"crash-initramfs").expect("write initramfs");
    fs::write(source.join("youki"), b"crash-youki").expect("write youki");
    let hash = |bytes: &[u8]| format!("{:x}", Sha256::digest(bytes));
    let profile = KernelProfile::Container;
    let version = json!({
        "kernel": "crash-kernel",
        "busybox": "crash-busybox",
        "agent": env!("CARGO_PKG_VERSION"),
        "agent_protocol_revision": vz_agent_proto::AGENT_PROTOCOL_REVISION,
        "youki": "crash-youki",
        "profile": profile.as_str(),
        "security_profile": profile.security_profile(),
        "capabilities": profile.default_capabilities(),
        "sha256_vmlinux": hash(b"crash-vmlinux"),
        "sha256_initramfs": hash(b"crash-initramfs"),
        "sha256_youki": hash(b"crash-youki"),
    });
    fs::write(
        source.join("version.json"),
        serde_json::to_vec(&version).expect("encode version metadata"),
    )
    .expect("write version metadata");
    let verified = verify_kernel_bundle_read_only(source, profile)
        .await
        .expect("verify source fixture");
    let entry = LinuxTargetCatalogEntry {
        image: LINUX_APPLIANCE_IMAGE.into(),
        version: "0.4.0-crash-test".into(),
        profile: MachineProfile::Hardened,
        bundle_dir: source.to_path_buf(),
        digest: verified.artifact_identity.digest,
        channels: BTreeSet::from(["crash-test".into()]),
    };
    let machine = MachineSpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        name: "crash-machine".into(),
        profile: MachineProfile::Hardened,
        target: TargetSpec {
            os: OperatingSystem::Linux,
            arch: Architecture::Aarch64,
            image: entry.image.clone(),
            version: Some(entry.version.clone()),
            channel: Some("crash-test".into()),
            digest: Some(entry.digest.clone()),
        },
        resources: MachineResources::default(),
        requested_capabilities: CapabilitySet::new([MachineCapability::PosixExec]),
        workspace: None,
    };
    let definition = ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: ProjectId::new("prj_artifact_crash").expect("valid Project ID"),
        name: "artifact-crash".into(),
        environment: EnvironmentSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            default_machine: None,
            machines: vec![machine.clone()],
            networks: Vec::new(),
            endpoints: Vec::new(),
        },
    };
    let resolver = MachineTargetResolver::new(
        host(),
        MachineTargetCatalog {
            schema_version: 1,
            linux: vec![entry],
        },
    )
    .expect("construct fixture resolver");
    let mut targets = resolver
        .resolve_project(&definition)
        .await
        .expect("resolve fixture target");
    let target = targets
        .machines
        .remove(&machine.name)
        .expect("resolved crash Machine");
    (target, machine)
}

fn child_main(root: &Path) {
    runtime().block_on(async {
        let source = root.join("source");
        let (target, _) = write_bundle_and_resolve(&source).await;
        let registry = MachineRuntimeRegistry::<usize>::new(root.join("runtime"))
            .expect("construct child registry");
        let reservation = MachineRuntimeRegistry::<usize>::reservation(&owner())
            .expect("derive exact store reservation");
        let store = registry
            .acquire_store(
                &owner(),
                &reservation,
                Some(target.configuration_digest()),
                MachineRuntimeAdmission::CreateOrOpen,
            )
            .expect("acquire child store");
        match pin_machine_artifacts(store, &target).await {
            Ok(_) => panic!("pin completed without reaching configured SIGKILL checkpoint"),
            Err(error) => panic!("pin failed before configured SIGKILL checkpoint: {error}"),
        }
    });
}

fn wait_bounded(mut command: Command, phase: &str) -> Output {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .unwrap_or_else(|error| panic!("spawn {phase} crash child: {error}"));
    let deadline = Instant::now() + CHILD_TIMEOUT;
    loop {
        if child
            .try_wait()
            .unwrap_or_else(|error| panic!("poll {phase} crash child: {error}"))
            .is_some()
        {
            return child
                .wait_with_output()
                .unwrap_or_else(|error| panic!("collect {phase} crash child: {error}"));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child
                .wait_with_output()
                .unwrap_or_else(|error| panic!("collect timed-out {phase} child: {error}"));
            panic!(
                "{phase} child exceeded {CHILD_TIMEOUT:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn run_crash_child(root: &Path, phase: &str) -> Output {
    let mut command = Command::new(std::env::current_exe().expect("locate current test driver"));
    command
        .arg("--ignored")
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .env(ROOT_ENV, root)
        .env(PHASE_ENV, phase)
        .env("RUST_TEST_THREADS", "1")
        .env_remove("RUST_BACKTRACE");
    wait_bounded(command, phase)
}

fn write_child_evidence(phase: &str, output: &Output) {
    assert!(
        output.stdout.len() <= MAX_CHILD_OUTPUT && output.stderr.len() <= MAX_CHILD_OUTPUT,
        "{phase} child output exceeded the evidence bound"
    );
    let mut evidence = std::io::stderr().lock();
    writeln!(
        evidence,
        "=== artifact pin crash phase={phase} status={} stdout_bytes={} stderr_bytes={} ===",
        output.status,
        output.stdout.len(),
        output.stderr.len()
    )
    .expect("write crash evidence header");
    evidence
        .write_all(&output.stdout)
        .expect("write raw crash child stdout");
    if !output.stdout.is_empty() && !output.stdout.ends_with(b"\n") {
        writeln!(evidence).expect("terminate crash child stdout");
    }
    evidence
        .write_all(&output.stderr)
        .expect("write raw crash child stderr");
    if !output.stderr.is_empty() && !output.stderr.ends_with(b"\n") {
        writeln!(evidence).expect("terminate crash child stderr");
    }
}

fn collect_tree(root: &Path, current: &Path, entries: &mut Vec<TreeEntry>) {
    let mut children = fs::read_dir(current)
        .unwrap_or_else(|error| panic!("read {}: {error}", current.display()))
        .map(|entry| entry.expect("read tree entry").path())
        .collect::<Vec<_>>();
    children.sort();
    for path in children {
        let metadata = fs::symlink_metadata(&path)
            .unwrap_or_else(|error| panic!("stat {}: {error}", path.display()));
        entries.push(TreeEntry {
            relative_path: path
                .strip_prefix(root)
                .expect("tree entry below snapshot root")
                .to_path_buf(),
            device: metadata.dev(),
            inode: metadata.ino(),
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            links: metadata.nlink(),
            length: metadata.len(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        });
        if metadata.is_dir() {
            collect_tree(root, &path, entries);
        }
    }
}

fn tree_snapshot(root: &Path) -> Vec<TreeEntry> {
    let mut entries = Vec::new();
    collect_tree(root, root, &mut entries);
    entries
}

fn pending_directories(data: &Path) -> Vec<PathBuf> {
    let mut paths = fs::read_dir(data)
        .expect("read Machine store data")
        .map(|entry| entry.expect("read Machine store data entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".pending-linux-target-"))
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn remove_source_bundle(source: &Path) {
    for name in FILES {
        fs::remove_file(source.join(name))
            .unwrap_or_else(|error| panic!("remove source {name}: {error}"));
    }
    fs::remove_dir(source).expect("remove exact source bundle directory");
}

fn cleanup_pin_directory(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("make {} removable: {error}", path.display()));
    let bundle = path.join("bundle");
    if bundle.exists() {
        fs::set_permissions(&bundle, fs::Permissions::from_mode(0o700))
            .expect("make artifact bundle removable");
        for name in FILES {
            let file = bundle.join(name);
            if file.exists() {
                fs::remove_file(&file)
                    .unwrap_or_else(|error| panic!("remove {}: {error}", file.display()));
            }
        }
        assert!(
            fs::read_dir(&bundle)
                .expect("inspect cleaned artifact bundle")
                .next()
                .is_none(),
            "artifact bundle contains an unexpected entry"
        );
        fs::remove_dir(&bundle).expect("remove exact artifact bundle directory");
    }
    let configuration = path.join("configuration.json");
    if configuration.exists() {
        fs::remove_file(&configuration).expect("remove exact pin configuration");
    }
    assert!(
        fs::read_dir(path)
            .expect("inspect cleaned pin directory")
            .next()
            .is_none(),
        "pin directory contains an unexpected entry"
    );
    fs::remove_dir(path).expect("remove exact pin directory");
}

fn cleanup_store(root: &Path, reservation_id: &str) {
    let namespace = root.join("runtime/topology-machines");
    let store = namespace.join(reservation_id);
    let data = store.join("data");
    let mut entries = fs::read_dir(&data)
        .expect("read data for exact cleanup")
        .map(|entry| entry.expect("read exact cleanup entry").path())
        .collect::<Vec<_>>();
    entries.sort();
    for entry in entries {
        let name = entry
            .file_name()
            .and_then(|name| name.to_str())
            .expect("UTF-8 pin entry");
        assert!(
            name == "linux-target" || name.starts_with(".pending-linux-target-"),
            "refuse to clean unexpected Machine artifact entry {name}"
        );
        cleanup_pin_directory(&entry);
    }
    fs::remove_dir(&data).expect("remove exact Machine data directory");
    fs::remove_file(store.join("owner.json")).expect("remove exact owner manifest");
    fs::remove_dir(&store).expect("remove exact Machine store directory");
    fs::remove_dir(&namespace).expect("remove exact topology Machine namespace");
    fs::remove_dir(root.join("runtime")).expect("remove exact registry root");
}

fn phase_root() -> PathBuf {
    let value = std::env::var_os(ROOT_ENV).unwrap_or_else(|| panic!("{ROOT_ENV} is required"));
    let root = PathBuf::from(value);
    assert!(root.is_absolute(), "{ROOT_ENV} must be absolute");
    root
}

fn run_phase(phase: &str, published: bool) {
    let temp = tempfile::tempdir().expect("create crash phase root");
    let root = fs::canonicalize(temp.path()).expect("canonical crash phase root");
    let runtime_root = root.join("runtime");
    fs::create_dir(&runtime_root).expect("create registry root");
    fs::set_permissions(&runtime_root, fs::Permissions::from_mode(0o700))
        .expect("make registry root private");

    let output = run_crash_child(&root, phase);
    write_child_evidence(phase, &output);
    assert_eq!(
        output.status.signal(),
        Some(9),
        "{phase} child did not terminate by SIGKILL"
    );
    assert!(
        output
            .stderr
            .windows(format!("{PHASE_ENV}={phase}").len())
            .any(|window| window == format!("{PHASE_ENV}={phase}").as_bytes()),
        "{phase} child did not report the exact reached checkpoint"
    );
    let stderr = std::str::from_utf8(&output.stderr).expect("UTF-8 crash child diagnostics");
    assert!(
        !stderr.contains("panicked at"),
        "{phase} child unwound before SIGKILL and may have run cleanup"
    );
    let reached = stderr
        .lines()
        .filter_map(|line| line.strip_prefix("VZ_ARTIFACT_PIN_CHECKPOINT="))
        .collect::<Vec<_>>();
    let phase_index = PHASES
        .iter()
        .position(|(candidate, _)| *candidate == phase)
        .expect("declared crash checkpoint");
    let expected = PHASES[..=phase_index]
        .iter()
        .map(|(checkpoint, _)| *checkpoint)
        .collect::<Vec<_>>();
    assert_eq!(
        reached, expected,
        "{phase} child crossed its crash boundary"
    );

    let source = root.join("source");
    let (target, machine) =
        runtime().block_on(write_bundle_and_resolve(&root.join("recovery-source")));
    remove_source_bundle(&source);
    let registry =
        MachineRuntimeRegistry::<usize>::new(runtime_root).expect("construct recovery registry");
    let reservation = MachineRuntimeRegistry::<usize>::reservation(&owner())
        .expect("derive recovery reservation");
    let store = registry
        .acquire_store(
            &owner(),
            &reservation,
            None,
            MachineRuntimeAdmission::ExistingOnly,
        )
        .expect("reacquire exact persisted store without a runtime");
    assert_eq!(store.configuration_digest(), target.configuration_digest());
    let data = store.data_path().to_path_buf();
    let before_load = tree_snapshot(&data);
    let loaded = runtime().block_on(load_machine_artifacts(Arc::clone(&store), host(), &machine));

    if published {
        let pin = loaded.expect("published pin must reopen without its original source");
        assert_eq!(pin.configuration(), target.configuration());
        pin.validate_current()
            .expect("published pin remains current");
        assert!(pending_directories(&data).is_empty());
        remove_source_bundle(&root.join("recovery-source"));
        let reopened = runtime()
            .block_on(load_machine_artifacts(Arc::clone(&store), host(), &machine))
            .expect("published pin remains independent of every source path");
        assert_eq!(reopened.configuration(), target.configuration());
        drop(reopened);
        drop(pin);
    } else {
        assert!(matches!(loaded, Err(MachineArtifactStoreError::Missing)));
        assert_eq!(
            tree_snapshot(&data),
            before_load,
            "{phase} recovery load mutated an unpublished store"
        );
        let pending = pending_directories(&data);
        assert_eq!(
            pending.len(),
            1,
            "{phase} must retain one crash staging dir"
        );
        let pending_before = tree_snapshot(&pending[0]);
        let replay_store = registry
            .acquire_store(
                &owner(),
                &reservation,
                Some(target.configuration_digest()),
                MachineRuntimeAdmission::CreateOrOpen,
            )
            .expect("exact CreateOrOpen retry reacquires the response-loss store");
        assert!(Arc::ptr_eq(&store, &replay_store));
        let pin = runtime()
            .block_on(pin_machine_artifacts(replay_store, &target))
            .expect("retry publishes beside unknown crash staging");
        assert_eq!(
            tree_snapshot(&pending[0]),
            pending_before,
            "{phase} retry adopted or changed unknown staging"
        );
        pin.validate_current().expect("retried pin remains current");
        remove_source_bundle(&root.join("recovery-source"));
        let reopened = runtime()
            .block_on(load_machine_artifacts(Arc::clone(&store), host(), &machine))
            .expect("retried pin remains independent of its source");
        assert_eq!(reopened.configuration(), target.configuration());
        drop(reopened);
        drop(pin);
    }

    // Every recovery path above uses the runtime-free store lease API; there is
    // deliberately no Runtime factory in this test process to invoke.
    let reservation_id = reservation.resource_id.clone();
    drop(store);
    drop(registry);
    cleanup_store(&root, &reservation_id);
    assert!(
        fs::read_dir(&root)
            .expect("inspect cleaned crash phase root")
            .next()
            .is_none(),
        "{phase} left data outside the exact cleaned store"
    );

    let mut evidence = std::io::stderr().lock();
    writeln!(
        evidence,
        "phase={phase} recovery={} runtime_factory_calls=0 cleanup=exact",
        if published {
            "published-exact"
        } else {
            "missing-then-retry-exact"
        }
    )
    .expect("write phase recovery evidence");
}

#[test]
#[ignore = "spawns exact SIGKILL children for the artifact publication durability gate"]
fn pin_publication_survives_sigkill_at_every_checkpoint() {
    if std::env::var_os(CHILD_ENV) == Some(OsString::from("1")) {
        child_main(&phase_root());
        panic!("crash child returned without SIGKILL");
    }

    for (phase, published) in PHASES {
        run_phase(phase, published);
    }
}
