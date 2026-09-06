#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use vz_linux::{KernelProfile, verify_kernel_bundle_read_only};
use vz_runtime_contract::{
    Architecture, CapabilitySet, EnvironmentId, EnvironmentSpec, HostSpec, MachineCapability,
    MachineId, MachineProfile, MachineResources, MachineSpec, OperatingSystem, OwnershipRecord,
    ProjectDefinition, ProjectId, ResourceOwner, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};

use super::*;
use crate::machine_runtime_registry::{MachineRuntimeAdmission, MachineRuntimeRegistry};
use crate::machine_target_resolver::{
    LINUX_APPLIANCE_IMAGE, LinuxTargetCatalogEntry, MachineTargetCatalog, MachineTargetResolver,
};

struct Fixture {
    _temp: TempDir,
    root: PathBuf,
    registry_root: PathBuf,
    registry: Option<MachineRuntimeRegistry<()>>,
    store: Option<Arc<MachineRuntimeStoreLease>>,
    owner: ResourceOwner,
    reservation: OwnershipRecord,
    target: ResolvedLinuxMachineTarget,
    host: HostSpec,
    machine: MachineSpec,
}

impl Fixture {
    async fn new(profile: MachineProfile) -> Self {
        Self::with_probe(profile, false).await
    }

    async fn with_probe(profile: MachineProfile, probe: bool) -> Self {
        let temp = TempDir::new().expect("temporary artifact store");
        let root = temp
            .path()
            .canonicalize()
            .expect("canonical temporary root");
        let source = root.join("catalog-bundle");
        let entry = write_catalog_bundle(&source, profile, probe).await;
        let host = HostSpec {
            os: OperatingSystem::Macos,
            arch: Architecture::Aarch64,
        };
        let machine = MachineSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            name: "machine-artifacts".into(),
            profile,
            target: TargetSpec {
                os: OperatingSystem::Linux,
                arch: Architecture::Aarch64,
                image: entry.image.clone(),
                version: Some(entry.version.clone()),
                channel: Some("test".into()),
                digest: Some(entry.digest.clone()),
            },
            resources: MachineResources::default(),
            requested_capabilities: CapabilitySet::new([MachineCapability::PosixExec]),
            workspace: None,
        };
        let definition = ProjectDefinition {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: ProjectId::new("prj_artifact_store").expect("Project ID"),
            name: "artifact-store".into(),
            environment: EnvironmentSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                default_machine: None,
                machines: vec![machine.clone()],
                networks: Vec::new(),
                endpoints: Vec::new(),
            },
        };
        let resolver = MachineTargetResolver::new(
            host,
            MachineTargetCatalog {
                macos: Vec::new(),
                schema_version: 1,
                linux: vec![entry],
            },
        )
        .expect("valid target catalog");
        let mut targets = resolver
            .resolve_project(&definition)
            .await
            .expect("resolved target");
        let target = targets
            .machines
            .remove(&machine.name)
            .expect("resolved Machine target");
        let owner = ResourceOwner {
            project_id: definition.project_id,
            environment_id: EnvironmentId::new("env_artifact_store").expect("Environment ID"),
            machine_id: Some(MachineId::new("mch_artifact_store").expect("Machine ID")),
        };
        let reservation = MachineRuntimeRegistry::<()>::reservation(&owner)
            .expect("valid runtime store reservation");
        let registry_root = root.join("runtime-registry");
        fs::create_dir(&registry_root).expect("runtime registry root");
        let registry = MachineRuntimeRegistry::new(registry_root.clone()).expect("registry");
        let store = registry
            .acquire_store(
                &owner,
                &reservation,
                Some(target.configuration_digest()),
                MachineRuntimeAdmission::CreateOrOpen,
            )
            .expect("runtime store");
        Self {
            _temp: temp,
            root,
            registry_root,
            registry: Some(registry),
            store: Some(store),
            owner,
            reservation,
            target,
            host,
            machine,
        }
    }

    fn store(&self) -> Arc<MachineRuntimeStoreLease> {
        Arc::clone(self.store.as_ref().expect("live runtime store"))
    }

    fn pin_path(&self) -> PathBuf {
        self.store
            .as_ref()
            .expect("live runtime store")
            .data_path()
            .join(PIN)
    }

    fn data_path(&self) -> PathBuf {
        self.store
            .as_ref()
            .expect("live runtime store")
            .data_path()
            .to_path_buf()
    }

    fn source(&self) -> PathBuf {
        self.target.bundle_dir().to_path_buf()
    }

    fn reopen_store(&mut self) -> Arc<MachineRuntimeStoreLease> {
        self.store.take();
        self.registry.take();
        let registry = MachineRuntimeRegistry::new(self.registry_root.clone()).expect("registry");
        let store = registry
            .acquire_store(
                &self.owner,
                &self.reservation,
                None,
                MachineRuntimeAdmission::ExistingOnly,
            )
            .expect("reopened runtime store");
        self.registry = Some(registry);
        self.store = Some(Arc::clone(&store));
        store
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        make_tree_owner_writable(&self.root);
    }
}

async fn write_catalog_bundle(
    root: &Path,
    profile: MachineProfile,
    probe: bool,
) -> LinuxTargetCatalogEntry {
    fs::create_dir_all(root).expect("bundle directory");
    let kernel_profile = match profile {
        MachineProfile::Developer => KernelProfile::Developer,
        MachineProfile::Hardened => KernelProfile::Container,
    };
    for name in ["vmlinux", "initramfs.img", "youki"] {
        fs::write(root.join(name), name.as_bytes()).expect("artifact");
    }
    let hash = |value: &str| format!("{:x}", Sha256::digest(value.as_bytes()));
    let mut metadata = json!({
        "kernel": "test-kernel",
        "busybox": "test-busybox",
        "agent": env!("CARGO_PKG_VERSION"),
        "agent_protocol_revision": vz_agent_proto::AGENT_PROTOCOL_REVISION,
        "youki": "test-youki",
        "profile": kernel_profile.as_str(),
        "security_profile": kernel_profile.security_profile(),
        "capabilities": kernel_profile.default_capabilities(),
        "sha256_vmlinux": hash("vmlinux"),
        "sha256_initramfs": hash("initramfs.img"),
        "sha256_youki": hash("youki"),
    });
    if probe {
        fs::write(root.join(DEVELOPER_PROBE_ARCHIVE), b"probe-rootfs").unwrap();
        metadata["busybox"] = "1.37.0".into();
        metadata["developer_probe"] = json!({
            "schema_version": 1, "archive": DEVELOPER_PROBE_ARCHIVE,
            "sha256": hash("probe-rootfs"), "busybox_sha256": "a".repeat(64),
            "busybox_version": "1.37.0", "source_archive_sha256": "b".repeat(64),
            "source_inventory_sha256": "c".repeat(64), "build_provenance_sha256": "d".repeat(64),
            "marker_sha256": format!("{:x}", Sha256::digest(vz_linux::DEVELOPER_PROBE_MARKER))
        });
    }
    fs::write(
        root.join("version.json"),
        serde_json::to_vec(&metadata).expect("version metadata"),
    )
    .expect("version metadata file");
    let verified = verify_kernel_bundle_read_only(root, kernel_profile)
        .await
        .expect("verified fixture bundle");
    LinuxTargetCatalogEntry {
        image: LINUX_APPLIANCE_IMAGE.into(),
        version: "0.4.0-test".into(),
        profile,
        bundle_dir: root.to_path_buf(),
        digest: verified.artifact_identity.digest,
        channels: BTreeSet::from(["test".into()]),
    }
}

#[tokio::test]
async fn developer_probe_pin_copies_verified_archive_and_recovers_without_source() {
    let mut fixture = Fixture::with_probe(MachineProfile::Developer, true).await;
    let pin = pin_machine_artifacts(fixture.store(), &fixture.target)
        .await
        .unwrap();
    let probe = pin.developer_probe().unwrap();
    assert_eq!(
        probe.archive,
        pin.bundle_dir().join(DEVELOPER_PROBE_ARCHIVE)
    );
    assert_eq!(fs::read(&probe.archive).unwrap(), b"probe-rootfs");
    assert_eq!(
        fs::metadata(&probe.archive).unwrap().permissions().mode() & 0o777,
        0o400
    );
    let metadata = probe.metadata.clone();
    fs::remove_file(fixture.source().join(DEVELOPER_PROBE_ARCHIVE)).unwrap();
    drop(pin);
    let store = fixture.reopen_store();
    let recovered = load_machine_artifacts(store, fixture.host, &fixture.machine)
        .await
        .unwrap();
    assert_eq!(recovered.developer_probe().unwrap().metadata, metadata);
}

#[tokio::test]
async fn developer_probe_pin_rejects_changed_source_and_recovery_archive() {
    let fixture = Fixture::with_probe(MachineProfile::Developer, true).await;
    fs::write(
        fixture.source().join(DEVELOPER_PROBE_ARCHIVE),
        b"changed-source",
    )
    .unwrap();
    assert!(
        pin_machine_artifacts(fixture.store(), &fixture.target)
            .await
            .is_err()
    );
    assert!(!fixture.pin_path().exists());

    let fixture = Fixture::with_probe(MachineProfile::Developer, true).await;
    let pin = pin_machine_artifacts(fixture.store(), &fixture.target)
        .await
        .unwrap();
    let archive = pin.developer_probe().unwrap().archive.clone();
    fs::set_permissions(&archive, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&archive, b"changed-pinned-content").unwrap();
    fs::set_permissions(&archive, fs::Permissions::from_mode(0o400)).unwrap();
    drop(pin);
    assert!(
        load_machine_artifacts(fixture.store(), fixture.host, &fixture.machine)
            .await
            .is_err()
    );
}

fn make_tree_owner_writable(path: &Path) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    if metadata.file_type().is_symlink() {
        return;
    }
    if metadata.is_dir() {
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                make_tree_owner_writable(&entry.path());
            }
        }
    } else {
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
}

fn tree_snapshot(root: &Path) -> Vec<(PathBuf, u32, Vec<u8>)> {
    fn visit(base: &Path, path: &Path, snapshot: &mut Vec<(PathBuf, u32, Vec<u8>)>) {
        let metadata = fs::symlink_metadata(path).expect("snapshot metadata");
        let relative = path
            .strip_prefix(base)
            .expect("relative snapshot")
            .to_path_buf();
        let payload = if metadata.file_type().is_symlink() {
            fs::read_link(path)
                .expect("symlink target")
                .as_os_str()
                .as_encoded_bytes()
                .to_vec()
        } else if metadata.is_file() {
            fs::read(path).expect("snapshot file")
        } else {
            Vec::new()
        };
        snapshot.push((relative, metadata.permissions().mode(), payload));
        if metadata.is_dir() {
            for entry in fs::read_dir(path).expect("snapshot directory") {
                visit(base, &entry.expect("snapshot entry").path(), snapshot);
            }
        }
    }

    let mut snapshot = Vec::new();
    visit(root, root, &mut snapshot);
    snapshot.sort_by(|left, right| left.0.cmp(&right.0));
    snapshot
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path)
        .expect("artifact mode metadata")
        .permissions()
        .mode()
        & 0o777
}

fn assert_no_pin_or_pending(fixture: &Fixture) {
    let entries = fs::read_dir(fixture.data_path())
        .expect("runtime data inventory")
        .map(|entry| entry.expect("runtime data entry").file_name())
        .collect::<Vec<_>>();
    assert!(!fixture.pin_path().exists());
    assert!(
        entries
            .iter()
            .all(|name| !name.to_string_lossy().starts_with(".pending-linux-target-")),
        "unpublished staging directory leaked: {entries:?}"
    );
}

#[tokio::test]
async fn pin_reopens_from_private_bytes_after_catalog_source_is_removed() {
    let mut fixture = Fixture::new(MachineProfile::Developer).await;
    let pinned = pin_inner(
        fixture.store(),
        fixture.target.configuration().clone(),
        fixture.source(),
    )
    .await
    .expect("published artifact pin");
    let expected_configuration = pinned.configuration().clone();
    let expected_snapshot = tree_snapshot(&fixture.pin_path());
    drop(pinned);
    fs::remove_dir_all(fixture.source()).expect("remove catalog source");

    let reopened_store = fixture.reopen_store();
    let reopened = load_inner(reopened_store, fixture.host, &fixture.machine)
        .await
        .expect("load private pin without catalog source");

    assert_eq!(reopened.configuration(), &expected_configuration);
    assert_eq!(tree_snapshot(&fixture.pin_path()), expected_snapshot);
    assert!(!fixture.source().exists());
}

#[tokio::test]
async fn persisted_stop_receipt_does_not_mutate_or_broaden_immutable_artifact_pin() {
    let mut fixture = Fixture::new(MachineProfile::Developer).await;
    let pinned = pin_machine_artifacts(fixture.store(), &fixture.target)
        .await
        .expect("published artifact pin");
    let expected_configuration = pinned.configuration().clone();
    let expected_pin = tree_snapshot(&fixture.pin_path());
    let entry = fixture
        .registry
        .as_ref()
        .expect("live registry")
        .attach_runtime(fixture.store(), |_| Ok(()))
        .expect("attach exact fixture runtime");
    // Synthetic closure isolates the filesystem contract in this regression;
    // only the separate installed backend gate establishes physical Stop.
    let receipt = crate::machine_live_sessions::MachineSessionStopReceipt {
        owner: fixture.owner.clone(),
        operation_id: vz_runtime_contract::LifecycleOperationId::generate().to_string(),
        generation: 1,
        runtime_identity: vz_runtime_contract::StackRuntimeIdentity::new("artifact-reopen-stop")
            .expect("fixture runtime identity"),
        endpoint: None,
        docker_shutdown: None,
        outcome: vz_runtime_contract::StackRuntimeShutdownOutcome::Stopped,
    };
    entry
        .persist_stop_receipt(&receipt)
        .expect("publish Stop through production receipt writer");
    let receipt_path = fixture
        .data_path()
        .join("linux-lifecycle/stops")
        .join(format!("{}.json", receipt.operation_id));
    let receipt_bytes = serde_json::to_vec(&receipt).expect("serialized Stop receipt");
    assert_eq!(fs::read(&receipt_path).unwrap(), receipt_bytes);
    assert_eq!(mode(&receipt_path), 0o600);
    assert!(!fixture.pin_path().join("stops").exists());
    assert_eq!(tree_snapshot(&fixture.pin_path()), expected_pin);
    drop(entry);
    drop(pinned);
    fs::remove_dir_all(fixture.source()).expect("remove catalog source");

    let reopened_store = fixture.reopen_store();
    let reopened = load_machine_artifacts(reopened_store, fixture.host, &fixture.machine)
        .await
        .expect("reopen immutable private artifacts after persisted Stop");
    assert_eq!(reopened.configuration(), &expected_configuration);
    assert_eq!(tree_snapshot(&fixture.pin_path()), expected_pin);
    assert_eq!(fs::read(&receipt_path).unwrap(), receipt_bytes);
    drop(reopened);

    // A lifecycle-looking directory inside the pin is still unexpected. The
    // fix must relocate mutable evidence, not weaken exact artifact inventory.
    fs::create_dir(fixture.pin_path().join("stops")).expect("unexpected pin directory");
    let rejected_snapshot = tree_snapshot(&fixture.pin_path());
    assert!(
        load_machine_artifacts(fixture.store(), fixture.host, &fixture.machine)
            .await
            .is_err()
    );
    assert_eq!(tree_snapshot(&fixture.pin_path()), rejected_snapshot);
    assert_eq!(fs::read(&receipt_path).unwrap(), receipt_bytes);
}

#[tokio::test]
async fn published_modes_are_exact_and_recovery_rejects_permission_drift_read_only() {
    for profile in [MachineProfile::Developer, MachineProfile::Hardened] {
        let fixture = Fixture::new(profile).await;
        let pinned = pin_inner(
            fixture.store(),
            fixture.target.configuration().clone(),
            fixture.source(),
        )
        .await
        .expect("published artifact pin");
        drop(pinned);

        let pin = fixture.pin_path();
        let bundle = pin.join(BUNDLE);
        assert_eq!(mode(&pin), 0o700);
        assert_eq!(mode(&bundle), 0o500);
        assert_eq!(mode(&pin.join(CONFIG)), 0o400);
        for artifact in ["vmlinux", "initramfs.img", "version.json"] {
            assert_eq!(mode(&bundle.join(artifact)), 0o400, "{artifact} mode");
        }
        assert_eq!(mode(&bundle.join("youki")), 0o500);

        for (case, artifact, invalid_mode, valid_mode) in [
            ("youki missing execute", "youki", 0o400, 0o500),
            ("youki writable", "youki", 0o700, 0o500),
            ("non-youki executable", "vmlinux", 0o500, 0o400),
        ] {
            let path = bundle.join(artifact);
            fs::set_permissions(&path, fs::Permissions::from_mode(invalid_mode))
                .expect("set invalid artifact mode");
            let before = tree_snapshot(&pin);
            assert!(
                load_inner(fixture.store(), fixture.host, &fixture.machine)
                    .await
                    .is_err(),
                "{case} was accepted for {profile:?}"
            );
            assert_eq!(
                tree_snapshot(&pin),
                before,
                "{case} recovery mutated the pin for {profile:?}"
            );
            fs::set_permissions(&path, fs::Permissions::from_mode(valid_mode))
                .expect("restore exact artifact mode");
        }

        load_inner(fixture.store(), fixture.host, &fixture.machine)
            .await
            .expect("restored exact modes remain readable");
    }
}

#[tokio::test]
async fn wrong_configuration_and_tampered_source_publish_nothing() {
    let fixture = Fixture::new(MachineProfile::Developer).await;
    let mut wrong = fixture.target.configuration().clone();
    wrong.release_version = "0.4.0-wrong".into();
    wrong.machine.target.version = Some(wrong.release_version.clone());
    assert!(
        pin_inner(fixture.store(), wrong, fixture.source())
            .await
            .is_err()
    );
    assert_no_pin_or_pending(&fixture);

    fs::write(fixture.source().join("vmlinux"), b"tampered-source").expect("tamper source");
    assert!(
        pin_inner(
            fixture.store(),
            fixture.target.configuration().clone(),
            fixture.source(),
        )
        .await
        .is_err()
    );
    assert_no_pin_or_pending(&fixture);
}

#[tokio::test]
async fn source_symlink_and_hardlink_are_rejected_without_publication() {
    for kind in ["symlink", "hardlink"] {
        let fixture = Fixture::new(MachineProfile::Developer).await;
        if kind == "symlink" {
            fs::remove_file(fixture.source().join("vmlinux")).expect("remove source artifact");
            symlink("initramfs.img", fixture.source().join("vmlinux"))
                .expect("source artifact symlink");
        } else {
            fs::hard_link(
                fixture.source().join("vmlinux"),
                fixture.source().join("vmlinux-alias"),
            )
            .expect("source artifact hardlink");
        }

        assert!(
            pin_inner(
                fixture.store(),
                fixture.target.configuration().clone(),
                fixture.source(),
            )
            .await
            .is_err(),
            "{kind} source was accepted"
        );
        assert_no_pin_or_pending(&fixture);
    }
}

#[tokio::test]
async fn config_profile_and_resource_drift_are_read_only_failures() {
    let fixture = Fixture::new(MachineProfile::Developer).await;
    let pinned = pin_inner(
        fixture.store(),
        fixture.target.configuration().clone(),
        fixture.source(),
    )
    .await
    .expect("published artifact pin");
    let expected = tree_snapshot(&fixture.pin_path());

    let mut target_drift = fixture.machine.clone();
    target_drift.target.channel = Some("other-channel".into());
    let mut profile_drift = fixture.machine.clone();
    profile_drift.profile = MachineProfile::Hardened;
    let mut resource_drift = fixture.machine.clone();
    resource_drift.resources.cpus = Some(8);
    for machine in [target_drift, profile_drift, resource_drift] {
        assert!(
            load_inner(fixture.store(), fixture.host, &machine)
                .await
                .is_err()
        );
        assert_eq!(tree_snapshot(&fixture.pin_path()), expected);
    }
    let wrong_host = HostSpec {
        os: OperatingSystem::Linux,
        arch: Architecture::Aarch64,
    };
    assert!(
        load_inner(fixture.store(), wrong_host, &fixture.machine)
            .await
            .is_err()
    );
    assert_eq!(tree_snapshot(&fixture.pin_path()), expected);
    drop(pinned);
}

#[tokio::test]
async fn persisted_semantic_and_noncanonical_configuration_drift_fail_read_only() {
    for kind in ["semantic", "noncanonical"] {
        let fixture = Fixture::new(MachineProfile::Developer).await;
        let pinned = pin_inner(
            fixture.store(),
            fixture.target.configuration().clone(),
            fixture.source(),
        )
        .await
        .expect("published artifact pin");
        drop(pinned);
        let configuration_path = fixture.pin_path().join(CONFIG);
        fs::set_permissions(&configuration_path, fs::Permissions::from_mode(0o600))
            .expect("writable configuration");
        let mut bytes = fs::read(&configuration_path).expect("configuration bytes");
        if kind == "semantic" {
            let mut value: serde_json::Value =
                serde_json::from_slice(&bytes).expect("configuration JSON");
            value["release_version"] = serde_json::Value::String("0.4.0-drift".into());
            bytes = serde_json::to_vec(&value).expect("drifted configuration");
        } else {
            bytes.push(b'\n');
        }
        fs::write(&configuration_path, bytes).expect("drift configuration");
        fs::set_permissions(&configuration_path, fs::Permissions::from_mode(0o400))
            .expect("restore configuration mode");
        let before = tree_snapshot(&fixture.pin_path());

        assert!(
            load_inner(fixture.store(), fixture.host, &fixture.machine)
                .await
                .is_err(),
            "{kind} persisted configuration drift was accepted"
        );
        assert_eq!(tree_snapshot(&fixture.pin_path()), before);
    }
}

#[tokio::test]
async fn published_symlink_hardlink_mode_and_extra_entry_are_rejected_read_only() {
    for kind in ["symlink", "hardlink", "mode", "extra"] {
        let fixture = Fixture::new(MachineProfile::Developer).await;
        let pinned = pin_inner(
            fixture.store(),
            fixture.target.configuration().clone(),
            fixture.source(),
        )
        .await
        .expect("published artifact pin");
        drop(pinned);
        let pin = fixture.pin_path();
        let bundle = pin.join(BUNDLE);
        match kind {
            "symlink" => {
                fs::set_permissions(&bundle, fs::Permissions::from_mode(0o700))
                    .expect("writable bundle");
                fs::remove_file(bundle.join("vmlinux")).expect("remove artifact");
                symlink("initramfs.img", bundle.join("vmlinux")).expect("artifact symlink");
                fs::set_permissions(&bundle, fs::Permissions::from_mode(0o500))
                    .expect("restore bundle mode");
            }
            "hardlink" => {
                fs::set_permissions(&bundle, fs::Permissions::from_mode(0o700))
                    .expect("writable bundle");
                fs::remove_file(bundle.join("initramfs.img")).expect("remove artifact");
                fs::hard_link(bundle.join("vmlinux"), bundle.join("initramfs.img"))
                    .expect("artifact hardlink");
                fs::set_permissions(&bundle, fs::Permissions::from_mode(0o500))
                    .expect("restore bundle mode");
            }
            "mode" => fs::set_permissions(pin.join(CONFIG), fs::Permissions::from_mode(0o600))
                .expect("invalid configuration mode"),
            "extra" => {
                fs::set_permissions(&pin, fs::Permissions::from_mode(0o700)).expect("writable pin");
                fs::write(pin.join("unexpected"), b"unexpected").expect("extra pin entry");
                fs::set_permissions(&pin, fs::Permissions::from_mode(0o700))
                    .expect("restore pin mode");
            }
            _ => unreachable!(),
        }
        let before = tree_snapshot(&pin);

        assert!(
            load_inner(fixture.store(), fixture.host, &fixture.machine)
                .await
                .is_err(),
            "{kind} corruption was accepted"
        );
        assert_eq!(tree_snapshot(&pin), before, "{kind} failure mutated pin");
    }
}

#[test]
fn concurrent_exact_publication_completes_with_one_blocking_thread_and_no_pending_leftovers() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("single-blocking-thread runtime");
    runtime.block_on(async {
        let fixture = Fixture::new(MachineProfile::Developer).await;
        let starts = Arc::new(Barrier::new(8));
        let mut tasks = JoinSet::new();
        for _ in 0..8 {
            let store = fixture.store();
            let configuration = fixture.target.configuration().clone();
            let source = fixture.source();
            let starts = Arc::clone(&starts);
            tasks.spawn(async move {
                starts.wait().await;
                pin_inner(store, configuration, source).await
            });
        }

        let mut pins = Vec::new();
        while let Some(result) = tasks.join_next().await {
            pins.push(result.expect("publication task").expect("exact contender"));
        }
        assert_eq!(pins.len(), 8);
        assert!(pins.iter().all(|pin| {
            pin.configuration() == fixture.target.configuration() && pin.bundle_dir().is_dir()
        }));
        let entries = fs::read_dir(fixture.data_path())
            .expect("runtime data inventory")
            .map(|entry| entry.expect("runtime data entry").file_name())
            .collect::<Vec<_>>();
        assert_eq!(entries, vec![std::ffi::OsString::from(PIN)]);
        assert!(
            entries
                .iter()
                .all(|name| !name.to_string_lossy().starts_with(".pending-linux-target-"))
        );
    });
}
