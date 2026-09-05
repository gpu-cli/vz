#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::Duration;

use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, oneshot};
use vz_runtime_contract::{
    Architecture, CapabilitySet, EnvironmentId, EnvironmentSpec, HostSpec, MachineCapability,
    MachineId, MachineProfile, MachineResources, MachineSpec, OperatingSystem, ProjectDefinition,
    ProjectId, ResourceOwner, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};

use super::*;
use crate::machine_runtime_registry::{MachineRuntimeAdmission, MachineRuntimeRegistry};
use crate::machine_target_resolver::{
    LINUX_APPLIANCE_IMAGE, LinuxTargetCatalogEntry, MachineTargetCatalog, MachineTargetResolver,
};

struct WorkerPause {
    started: oneshot::Sender<()>,
    resume: mpsc::Receiver<()>,
}

static WORKER_PAUSES: OnceLock<Mutex<HashMap<PathBuf, WorkerPause>>> = OnceLock::new();

// Path-keyed, one-shot instrumentation cannot pause another parallel test.
pub(super) fn pause_copy_worker(data: &Path) {
    let pause = WORKER_PAUSES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap()
        .remove(data);
    if let Some(pause) = pause {
        pause.started.send(()).expect("announce paused copy worker");
        pause
            .resume
            .recv_timeout(Duration::from_secs(15))
            .expect("release paused copy worker");
    }
}

struct FenceDropProbe {
    data: PathBuf,
    dropped: Option<oneshot::Sender<(bool, bool)>>,
    _guard: OwnedMutexGuard<()>,
}

impl Drop for FenceDropProbe {
    fn drop(&mut self) {
        let clean = fs::read_dir(&self.data).is_ok_and(|mut entries| entries.next().is_none());
        let unpublished = !self.data.join(PIN).exists();
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send((clean, unpublished));
        }
    }
}

async fn target(source: &Path) -> ResolvedLinuxMachineTarget {
    fs::create_dir(source).unwrap();
    let bytes = b"cancellation-fixture";
    for name in ["vmlinux", "initramfs.img", "youki"] {
        fs::write(source.join(name), bytes).unwrap();
    }
    let digest = format!("{:x}", Sha256::digest(bytes));
    let profile = vz_linux::KernelProfile::Container;
    fs::write(
        source.join("version.json"),
        serde_json::to_vec(&json!({
            "kernel": "test-kernel", "busybox": "test-busybox",
            "agent": env!("CARGO_PKG_VERSION"),
            "agent_protocol_revision": vz_agent_proto::AGENT_PROTOCOL_REVISION,
            "youki": "test-youki", "profile": profile.as_str(),
            "security_profile": profile.security_profile(),
            "capabilities": profile.default_capabilities(),
            "sha256_vmlinux": digest, "sha256_initramfs": digest, "sha256_youki": digest,
        }))
        .unwrap(),
    )
    .unwrap();
    let verified = verify_kernel_bundle_read_only(source, profile)
        .await
        .unwrap();
    let entry = LinuxTargetCatalogEntry {
        image: LINUX_APPLIANCE_IMAGE.into(),
        version: "0.4.0-cancellation".into(),
        profile: MachineProfile::Hardened,
        bundle_dir: source.into(),
        digest: verified.artifact_identity.digest,
        channels: BTreeSet::from(["test".into()]),
    };
    let machine = MachineSpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        name: "cancel-machine".into(),
        profile: MachineProfile::Hardened,
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
    let resolver = MachineTargetResolver::new(
        HostSpec {
            os: OperatingSystem::Macos,
            arch: Architecture::Aarch64,
        },
        MachineTargetCatalog {
            schema_version: 1,
            linux: vec![entry],
        },
    )
    .unwrap();
    let definition = ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: ProjectId::new("prj_cancel_pin").unwrap(),
        name: "cancel-pin".into(),
        environment: EnvironmentSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            machines: vec![machine],
            networks: Vec::new(),
            endpoints: Vec::new(),
        },
    };
    resolver
        .resolve_project(&definition)
        .await
        .unwrap()
        .machines
        .remove("cancel-machine")
        .unwrap()
}

#[tokio::test]
async fn cancelled_copy_retains_environment_fence_until_staging_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let target = target(&root.join("source")).await;
    let registry_root = root.join("registry");
    fs::create_dir(&registry_root).unwrap();
    let registry = MachineRuntimeRegistry::<()>::new(registry_root).unwrap();
    let owner = ResourceOwner {
        project_id: ProjectId::new("prj_cancel_pin").unwrap(),
        environment_id: EnvironmentId::new("env_cancel_pin").unwrap(),
        machine_id: Some(MachineId::new("mch_cancel_pin").unwrap()),
    };
    let reservation = MachineRuntimeRegistry::<()>::reservation(&owner).unwrap();
    let store = registry
        .acquire_store(
            &owner,
            &reservation,
            Some(target.configuration_digest()),
            MachineRuntimeAdmission::CreateOrOpen,
        )
        .unwrap();
    let data = store.data_path().to_path_buf();
    let (started_tx, started_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    WORKER_PAUSES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap()
        .insert(
            data.clone(),
            WorkerPause {
                started: started_tx,
                resume: resume_rx,
            },
        );
    let environment_lock = Arc::new(AsyncMutex::new(()));
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let fence: Arc<dyn Send + Sync> = Arc::new(FenceDropProbe {
        data: data.clone(),
        dropped: Some(dropped_tx),
        _guard: Arc::clone(&environment_lock).lock_owned().await,
    });
    let weak_fence = Arc::downgrade(&fence);
    let task =
        tokio::spawn(
            async move { pin_machine_artifacts_retaining_fence(store, &target, fence).await },
        );
    tokio::time::timeout(Duration::from_secs(10), started_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        fs::read_dir(&data).unwrap().count(),
        1,
        "pending directory exists while copy is paused"
    );
    assert!(!data.join(PIN).exists());
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert!(
        weak_fence.upgrade().is_some(),
        "detached copy still owns the fence"
    );
    assert!(
        Arc::clone(&environment_lock).try_lock_owned().is_err(),
        "next controller remains excluded after cancellation"
    );
    resume_tx.send(()).unwrap();
    let (clean_at_release, unpublished_at_release) =
        tokio::time::timeout(Duration::from_secs(10), dropped_rx)
            .await
            .unwrap()
            .unwrap();
    assert!(
        clean_at_release,
        "owned pending cleanup precedes fence release"
    );
    assert!(
        unpublished_at_release,
        "cancelled copy never publishes final pins"
    );
    let _next_controller =
        tokio::time::timeout(Duration::from_secs(10), environment_lock.lock_owned())
            .await
            .unwrap();
    assert!(weak_fence.upgrade().is_none());
    assert_eq!(fs::read_dir(&data).unwrap().count(), 0);
    drop(registry);
    temp.close().unwrap();
}
