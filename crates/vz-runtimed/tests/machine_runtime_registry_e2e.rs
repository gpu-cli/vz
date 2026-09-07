//! Physical proof for private per-Machine runtime admission and leased VM boots.
//! This proves infrastructure and focused host Docker transport, not production
//! `vz up`, managed contexts, or complete Docker compatibility.

#![cfg(all(target_os = "macos", target_arch = "aarch64"))]
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "support/docker_device_policy.rs"]
mod docker_device_policy;
#[path = "support/docker_exec_root.rs"]
mod docker_exec_root;
#[path = "support/docker_exec_root_values.rs"]
mod docker_exec_root_values;
#[path = "support/docker_namespace_values.rs"]
mod docker_namespace_values;
#[path = "support/docker_seccomp_policy.rs"]
mod docker_seccomp_policy;
#[path = "support/docker_time_namespace.rs"]
mod docker_time_namespace;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use vz_oci_macos::{KernelProfile, Runtime, SharedVmDockerReadiness};
use vz_runtime_contract::{
    Architecture, CapabilitySet, EnvironmentLifecycleKind, EnvironmentLifecycleOperation,
    EnvironmentLifecycleStatus, EnvironmentSpec, EnvironmentState, HostSpec, LifecycleStepResult,
    MachineActivationEvidence, MachineBackend, MachineCapability, MachineIncarnation,
    MachineIncarnationId, MachineLifecycleStepAcknowledgement, MachineProfile, MachineResources,
    MachineRuntimeIdentity, MachineSpec, MachineState, OperatingSystem, OwnershipRecord,
    ProjectDefinition, ProjectState, ResourceOwner, STACK_RUNTIME_SHUTDOWN_REQUEST_SCHEMA_VERSION,
    StackResourceHint, StackRuntimeIdentity, StackRuntimeShutdownOutcome,
    StackRuntimeShutdownRequest, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};
use vz_runtimed::environment_runtime_controller::EnvironmentRuntimeController;
use vz_runtimed::machine_artifact_store::{PinnedMachineArtifacts, pin_machine_artifacts};
use vz_runtimed::machine_backend::MachineBackendRuntime as MacosRuntimeBackend;
use vz_runtimed::machine_docker_endpoint::MachineDockerEndpoint;
use vz_runtimed::machine_live_sessions::MachineLiveSessions;
use vz_runtimed::machine_runtime_activation::MachineRuntimeActivation;
use vz_runtimed::machine_runtime_registry::{
    MachineRuntimeAdmission, MachineRuntimeEntry, MachineRuntimeRegistry,
    MachineRuntimeRegistryError, MachineRuntimeStoreLease,
};
use vz_runtimed::machine_target_resolver::{
    LINUX_APPLIANCE_IMAGE, LinuxTargetCatalogEntry, MACHINE_TARGET_CATALOG_SCHEMA_VERSION,
    MachineTargetCatalog, MachineTargetResolver, ResolvedProjectTargets, TargetResolutionError,
};
use vz_stack::StateStore;

const EVIDENCE_ENV: &str = "VZ_MACHINE_RUNTIME_REGISTRY_EVIDENCE";
const BUILD_PROFILE_ENV: &str = "VZ_MACHINE_RUNTIME_REGISTRY_BUILD_PROFILE";
const TEST_SHA_ENV: &str = "VZ_MACHINE_RUNTIME_REGISTRY_TEST_BINARY_SHA256";
const DEV_INITRAMFS_SHA_ENV: &str = "VZ_MACHINE_RUNTIME_REGISTRY_DEVELOPER_INITRAMFS_SHA256";
const HARD_INITRAMFS_SHA_ENV: &str = "VZ_MACHINE_RUNTIME_REGISTRY_CONTAINER_INITRAMFS_SHA256";
const DEV_BUNDLE_ENV: &str = "VZ_LINUX_DEVELOPER_BUNDLE_DIR";
const HARD_BUNDLE_ENV: &str = "VZ_LINUX_CONTAINER_BUNDLE_DIR";
const DOCKER_PROBE_ENV: &str = "VZ_MACHINE_RUNTIME_REGISTRY_DOCKER_PROBE";
const DOCKER_PROBE_SOURCE_SHA_ENV: &str = "VZ_MACHINE_RUNTIME_REGISTRY_DOCKER_PROBE_SOURCE_SHA256";
const DOCKER_PROBE_GO_VERSION_ENV: &str = "VZ_MACHINE_RUNTIME_REGISTRY_DOCKER_PROBE_GO_VERSION";
const SERIAL_LOG_DIR_ENV: &str = "VZ_STACK_SERIAL_LOG_DIR";
const TIMEOUT: Duration = Duration::from_secs(20);
const DEVELOPER_ACTIVATION_FAILURE: &str = "insufficient host Docker conformance: Developer Ready requires DockerEngine, DockerCompose, and Buildx evidence";

#[derive(Clone)]
struct MachineFixture {
    name: &'static str,
    profile: KernelProfile,
    owner: ResourceOwner,
    store_reservation: OwnershipRecord,
    vm_reservation: OwnershipRecord,
    config_digest: String,
    resolved_configuration: Value,
    artifact: Value,
    resources: StackResourceHint,
}

impl MachineFixture {
    fn stack_id(&self) -> &str {
        &self.vm_reservation.resource_id
    }

    fn reservations(&self) -> [&OwnershipRecord; 2] {
        [&self.store_reservation, &self.vm_reservation]
    }
}

struct CleanupTarget {
    runtime: Runtime,
    stack_id: String,
}

fn entitled() -> bool {
    let Ok(exe) = std::env::current_exe() else {
        return false;
    };
    let Ok(output) = Command::new("codesign")
        .args(["-d", "--entitlements", ":-"])
        .arg(exe)
        .output()
    else {
        return false;
    };
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .contains("com.apple.security.virtualization")
}

fn file_sha(path: &Path) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

#[path = "support/registry_fixture_bundle.rs"]
mod registry_fixture_bundle;
use registry_fixture_bundle::{artifact_names, copy_fixture_bundle};

async fn create_fixture_sources(root: &Path) -> Result<(PathBuf, PathBuf, PathBuf)> {
    let original_developer = fs::canonicalize(PathBuf::from(
        std::env::var_os(DEV_BUNDLE_ENV).context(DEV_BUNDLE_ENV)?,
    ))?;
    let original_hardened = fs::canonicalize(PathBuf::from(
        std::env::var_os(HARD_BUNDLE_ENV).context(HARD_BUNDLE_ENV)?,
    ))?;
    let source_root = root.join("source-bundles");
    fs::DirBuilder::new().mode(0o700).create(&source_root)?;
    let developer = source_root.join("developer");
    let hardened = source_root.join("hardened");
    copy_fixture_bundle(&original_developer, &developer, KernelProfile::Developer).await?;
    copy_fixture_bundle(&original_hardened, &hardened, KernelProfile::Container).await?;
    File::open(&source_root)?.sync_all()?;
    Ok((source_root, developer, hardened))
}

fn serial_log_evidence(path: &Path) -> Result<Value> {
    let path = fs::canonicalize(path)?;
    let metadata = fs::symlink_metadata(&path)?;
    ensure!(metadata.is_file() && metadata.nlink() == 1 && metadata.size() > 0);
    Ok(json!({ "path": path, "sha256": file_sha(&path)? }))
}

fn preserve_first_boot_serial_logs(
    directory: &Path,
    fixtures: &[MachineFixture],
) -> Result<Vec<Value>> {
    let directory = fs::canonicalize(directory)?;
    let mut evidence = Vec::new();
    for fixture in fixtures {
        let source = directory.join(format!("{}.log", fixture.stack_id()));
        let target = directory.join(format!("{}.first-boot.log", fixture.stack_id()));
        let metadata = fs::symlink_metadata(&source)?;
        ensure!(metadata.is_file() && metadata.nlink() == 1 && metadata.size() > 0);
        fs::hard_link(&source, &target).with_context(|| {
            format!(
                "preserve first boot serial log without overwriting {}",
                target.display()
            )
        })?;
        File::open(&directory)?.sync_all()?;
        fs::remove_file(&source)?;
        File::open(&directory)?.sync_all()?;
        evidence.push(serial_log_evidence(&target)?);
    }
    Ok(evidence)
}

fn second_boot_serial_logs(directory: &Path, fixtures: &[MachineFixture]) -> Result<Vec<Value>> {
    let directory = fs::canonicalize(directory)?;
    fixtures
        .iter()
        .map(|fixture| serial_log_evidence(&directory.join(format!("{}.log", fixture.stack_id()))))
        .collect()
}

fn artifact(
    bundle: &Path,
    profile: KernelProfile,
    identity: &vz_linux::KernelBundleArtifactIdentity,
) -> Value {
    json!({
        "bundle": bundle,
        "profile": profile.as_str(),
        "kernel_sha256": identity.kernel_sha256,
        "initramfs_sha256": identity.initramfs_sha256,
        "youki_sha256": identity.youki_sha256,
        "version_sha256": identity.version_sha256,
    })
}

fn definition(
    project_id: vz_runtime_contract::ProjectId,
    developer_digest: &str,
    hardened_digest: &str,
) -> Result<ProjectDefinition> {
    let spec = |name: &str, profile: MachineProfile, memory_mb: u64, digest: &str| {
        Ok::<_, anyhow::Error>(MachineSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            name: name.into(),
            profile,
            target: TargetSpec {
                os: OperatingSystem::Linux,
                arch: Architecture::Aarch64,
                image: LINUX_APPLIANCE_IMAGE.to_string(),
                version: Some("0.4.0-registry-e2e".into()),
                channel: Some("local-physical-e2e".into()),
                digest: Some(digest.to_string()),
            },
            resources: MachineResources {
                cpus: Some(2),
                memory_mb: Some(memory_mb),
                disk_bytes: None,
            },
            // Readiness below must not synthesize Docker/Compose/buildx.
            requested_capabilities: CapabilitySet::new([MachineCapability::PosixExec]),
            workspace: None,
        })
    };
    Ok(ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id,
        name: "machine-runtime-registry-e2e".into(),
        environment: EnvironmentSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            default_machine: None,
            machines: vec![
                spec(
                    "developer-a",
                    MachineProfile::Developer,
                    4096,
                    developer_digest,
                )?,
                spec(
                    "developer-b",
                    MachineProfile::Developer,
                    4096,
                    developer_digest,
                )?,
                spec("hardened", MachineProfile::Hardened, 1024, hardened_digest)?,
            ],
            networks: vec![],
            endpoints: vec![],
        },
    })
}

fn target_catalog(
    developer_bundle: PathBuf,
    developer_digest: String,
    hardened_bundle: PathBuf,
    hardened_digest: String,
) -> MachineTargetCatalog {
    let entry = |profile, bundle_dir, digest| LinuxTargetCatalogEntry {
        image: LINUX_APPLIANCE_IMAGE.to_string(),
        version: "0.4.0-registry-e2e".to_string(),
        profile,
        bundle_dir,
        digest,
        channels: BTreeSet::from(["local-physical-e2e".to_string()]),
    };
    MachineTargetCatalog {
        source_path: None,
        macos: Vec::new(),
        schema_version: MACHINE_TARGET_CATALOG_SCHEMA_VERSION,
        linux: vec![
            entry(
                MachineProfile::Developer,
                developer_bundle,
                developer_digest,
            ),
            entry(MachineProfile::Hardened, hardened_bundle, hardened_digest),
        ],
    }
}

async fn invalid_sibling_preflight(
    root: &Path,
    source_root: &Path,
    resolver: &MachineTargetResolver,
    definition: &ProjectDefinition,
) -> Result<bool> {
    let mut invalid_definitions = Vec::new();

    let mut unknown_image = definition.clone();
    unknown_image.environment.machines[1].target.image = "unknown-linux-appliance".to_string();
    invalid_definitions.push(unknown_image);

    let mut unknown_version = definition.clone();
    unknown_version.environment.machines[1].target.version = Some("unknown-release".to_string());
    invalid_definitions.push(unknown_version);

    let mut unknown_channel = definition.clone();
    unknown_channel.environment.machines[1].target.channel = Some("unknown-channel".to_string());
    invalid_definitions.push(unknown_channel);

    let mut wrong_digest = definition.clone();
    wrong_digest.environment.machines[1].target.digest = Some(format!("sha256:{}", "0".repeat(64)));
    invalid_definitions.push(wrong_digest);

    for invalid in invalid_definitions {
        ensure!(matches!(
            resolver.resolve_project(&invalid).await,
            Err(TargetResolutionError::TargetNotFound { machine }) if machine == "developer-b"
        ));
        ensure!(!root.join("topology.db").exists());
        ensure!(!root.join("registry").exists());
        let entries = fs::read_dir(root)?
            .map(|entry| Ok(entry?.path()))
            .collect::<Result<Vec<_>>>()?;
        ensure!(entries.len() == 1 && entries[0] == source_root);
    }
    Ok(true)
}

fn fixtures(
    state: &ProjectState,
    resolved: &ResolvedProjectTargets,
) -> Result<Vec<MachineFixture>> {
    let environment = state.environments.first().context("fixture Environment")?;
    [
        ("developer-a", KernelProfile::Developer, 4096),
        ("developer-b", KernelProfile::Developer, 4096),
        ("hardened", KernelProfile::Container, 1024),
    ]
    .into_iter()
    .map(|(name, profile, memory_mb)| {
        let machine = environment
            .machines
            .iter()
            .find(|machine| machine.name == name)
            .with_context(|| format!("Machine {name}"))?;
        let owner = ResourceOwner {
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(machine.machine_id.clone()),
        };
        let store_reservation = MachineRuntimeRegistry::<MacosRuntimeBackend>::reservation(&owner)?;
        let vm_reservation = MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(&owner)?;
        let target = resolved
            .machines
            .get(name)
            .with_context(|| format!("resolved Machine target {name}"))?;
        ensure!(target.profile() == profile);
        let configuration = target.configuration();
        ensure!(configuration.machine.name == machine.name);
        ensure!(configuration.machine.profile == machine.profile);
        ensure!(configuration.machine.target == machine.target);
        ensure!(configuration.machine.resources == machine.resources);
        ensure!(configuration.machine.requested_capabilities == machine.requested_capabilities);
        ensure!(configuration.machine.workspace.is_none());
        ensure!(configuration.host.os == OperatingSystem::Macos);
        ensure!(configuration.host.arch == Architecture::Aarch64);
        ensure!(configuration.backend == MachineBackend::MacosVirtualizationLinux);
        ensure!(configuration.release_version == "0.4.0-registry-e2e");
        ensure!(target.configuration().resources.cpus == 2);
        ensure!(target.configuration().resources.memory_mb == memory_mb);
        let bundle = target.bundle_dir().to_path_buf();
        let artifact = artifact(&bundle, profile, &target.configuration().artifact);
        Ok(MachineFixture {
            name,
            profile,
            owner,
            store_reservation,
            vm_reservation,
            config_digest: target.configuration_digest().to_string(),
            resolved_configuration: serde_json::to_value(target.configuration())?,
            artifact,
            resources: StackResourceHint {
                cpus: Some(2),
                memory_mb: Some(memory_mb),
                ..StackResourceHint::default()
            },
        })
    })
    .collect()
}

fn prepared_stores(
    pins: &[PinnedMachineArtifacts],
    fixtures: &[MachineFixture],
) -> Result<Vec<Arc<MachineRuntimeStoreLease>>> {
    ensure!(pins.len() == fixtures.len());
    fixtures
        .iter()
        .map(|fixture| {
            let pin = pins
                .iter()
                .find(|pin| pin.configuration().machine.name == fixture.name)
                .with_context(|| format!("prepared Machine {}", fixture.name))?;
            ensure!(pin.store().owner() == &fixture.owner);
            ensure!(pin.store().configuration_digest() == fixture.config_digest);
            Ok(Arc::clone(pin.store()))
        })
        .collect()
}

/// Poll each acquisition exactly once: no sleeps or timing-based lock proof.
async fn controller_serialization_proof(
    controller: &EnvironmentRuntimeController,
    project_id: &vz_runtime_contract::ProjectId,
    environment_id: &vz_runtime_contract::EnvironmentId,
) -> Result<bool> {
    let mut same = Box::pin(controller.acquire(project_id, environment_id));
    let same_pending = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(same.as_mut(), context).is_pending())
    })
    .await;
    ensure!(
        same_pending,
        "prepared Environment did not retain its controller lock"
    );
    drop(same);
    let sibling_id = vz_runtime_contract::EnvironmentId::generate();
    let mut sibling = Box::pin(controller.acquire(project_id, &sibling_id));
    let sibling_result = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(sibling.as_mut(), context))
    })
    .await;
    let sibling_ready = matches!(sibling_result, std::task::Poll::Ready(Ok(_)));
    ensure!(
        sibling_ready,
        "a different Environment was blocked by the controller lock"
    );
    Ok(same_pending && sibling_ready)
}

async fn pin_all(
    stores: &[Arc<MachineRuntimeStoreLease>],
    fixtures: &[MachineFixture],
    resolved: &ResolvedProjectTargets,
    cleanup_directories: &mut Vec<PathBuf>,
) -> Result<Vec<PinnedMachineArtifacts>> {
    let mut pins = Vec::new();
    for (store, fixture) in stores.iter().zip(fixtures) {
        let target = resolved
            .machines
            .get(fixture.name)
            .with_context(|| format!("resolved Machine target {}", fixture.name))?;
        let pin = pin_machine_artifacts(Arc::clone(store), target).await?;
        let bundle = pin.bundle_dir();
        let directory = bundle.parent().context("pin parent")?.to_path_buf();
        cleanup_directories.push(bundle);
        cleanup_directories.push(directory);
        pins.push(pin);
    }
    Ok(pins)
}

fn host() -> HostSpec {
    HostSpec {
        os: OperatingSystem::Macos,
        arch: Architecture::Aarch64,
    }
}

fn pin_snapshot(
    stores: &[Arc<MachineRuntimeStoreLease>],
    fixtures: &[MachineFixture],
) -> Result<Value> {
    let mut snapshots = BTreeMap::new();
    for (store, fixture) in stores.iter().zip(fixtures) {
        snapshots.insert(fixture.name, installed(store.data_path())?);
    }
    Ok(serde_json::to_value(snapshots)?)
}

fn all_reservations(fixtures: &[MachineFixture]) -> Vec<&OwnershipRecord> {
    fixtures
        .iter()
        .flat_map(MachineFixture::reservations)
        .collect()
}

fn require_owned_read_only(
    store: &StateStore,
    project: &str,
    fixtures: &[MachineFixture],
) -> Result<bool> {
    let before = store.load_project_state(project)?;
    for record in all_reservations(fixtures) {
        ensure!(store.require_owned_resource(record)? == *record);
    }
    Ok(store.load_project_state(project)? == before)
}

fn require_machine_fence(
    store: &StateStore,
    operation: &EnvironmentLifecycleOperation,
    fixture: &MachineFixture,
) -> Result<()> {
    let step = operation
        .machine_steps
        .iter()
        .find(|step| fixture.owner.machine_id.as_ref() == Some(&step.machine_id))
        .context("fixture lifecycle step")?;
    store.require_current_machine_lifecycle_fence(
        operation,
        step,
        &[
            fixture.store_reservation.clone(),
            fixture.vm_reservation.clone(),
        ],
    )?;
    Ok(())
}

fn activation(
    operation: &EnvironmentLifecycleOperation,
    machine_id: &vz_runtime_contract::MachineId,
    identity: &StackRuntimeIdentity,
    generation: u64,
    now: u64,
) -> Result<MachineActivationEvidence> {
    let step = operation
        .machine_steps
        .iter()
        .find(|step| step.machine_id == *machine_id)
        .context("lifecycle Machine step")?;
    let incarnation = MachineIncarnation {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        incarnation_id: MachineIncarnationId::new(format!(
            "inc_runtime_{}",
            identity.incarnation_id
        ))?,
        machine_id: machine_id.clone(),
        generation,
        created_at: now,
    };
    ensure!(step.machine_id == incarnation.machine_id);
    Ok(MachineActivationEvidence {
        docker_context: None,
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        backend: MachineBackend::MacosVirtualizationLinux,
        negotiated_capabilities: CapabilitySet::new([MachineCapability::PosixExec]),
        runtime_identity: MachineRuntimeIdentity {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            opaque_id: serde_json::to_string(identity)?,
        },
        incarnation,
    })
}

struct FailedUpProof {
    developer_rejections: usize,
    hardened_activation_published: bool,
}

fn finish_failed_up(
    store: &StateStore,
    mut operation: EnvironmentLifecycleOperation,
    fixtures: &[MachineFixture],
    identities: &[StackRuntimeIdentity],
    generation: u64,
    now: u64,
) -> Result<FailedUpProof> {
    let mut developer_rejections = 0;
    let mut hardened_activation_published = false;
    for step in operation.machine_steps.clone() {
        let index = fixtures
            .iter()
            .position(|fixture| fixture.owner.machine_id.as_ref() == Some(&step.machine_id))
            .context("fixture for Up step")?;
        let evidence = activation(
            &operation,
            &step.machine_id,
            &identities[index],
            generation,
            now,
        )?;
        let success = MachineLifecycleStepAcknowledgement {
            operation_id: operation.operation_id.clone(),
            generation: operation.generation,
            machine_id: step.machine_id.clone(),
            initial_state: step.initial_state,
            target_state: step.target_state,
            expected_incarnation: step.expected_incarnation.clone(),
            resulting_incarnation: Some(evidence.incarnation.clone()),
            resulting_activation: Some(evidence.clone()),
            result: LifecycleStepResult::Succeeded,
        };
        if fixtures[index].profile == KernelProfile::Developer {
            let state_before = store.load_project_state(operation.project_id.as_str())?;
            let operation_before = store
                .load_environment_lifecycle(operation.operation_id.as_str())?
                .context("active Up operation before rejected Developer activation")?;
            let error = store
                .acknowledge_environment_machine_step(&success, now)
                .expect_err("POSIX-only Developer activation must be rejected");
            ensure!(
                error
                    .to_string()
                    .contains("missing required capability `DockerEngine`")
            );
            ensure!(store.load_project_state(operation.project_id.as_str())? == state_before);
            ensure!(
                store.load_environment_lifecycle(operation.operation_id.as_str())?
                    == Some(operation_before)
            );
            operation = store.acknowledge_environment_machine_step(
                &MachineLifecycleStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    machine_id: step.machine_id,
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: step.expected_incarnation,
                    resulting_incarnation: None,
                    resulting_activation: None,
                    result: LifecycleStepResult::Failed {
                        reason: DEVELOPER_ACTIVATION_FAILURE.to_string(),
                    },
                },
                now,
            )?;
            developer_rejections += 1;
        } else {
            operation = store.acknowledge_environment_machine_step(&success, now)?;
            let state = store
                .load_project_state(operation.project_id.as_str())?
                .context("project after Hardened activation")?;
            let machine = state.environments[0]
                .machines
                .iter()
                .find(|machine| machine.machine_id == step.machine_id)
                .context("Hardened Machine after activation")?;
            ensure!(machine.state == MachineState::Ready);
            ensure!(machine.runtime_identity.as_ref() == Some(&evidence.runtime_identity));
            ensure!(machine.negotiated_capabilities == evidence.negotiated_capabilities);
            hardened_activation_published = true;
        }
    }
    let finished = store.finish_environment_lifecycle(
        operation.operation_id.as_str(),
        operation.generation,
        now + 1,
    )?;
    ensure!(finished.status == EnvironmentLifecycleStatus::Failed);
    let state = store
        .load_project_state(finished.project_id.as_str())?
        .context("project after failed aggregate Up")?;
    let environment = state
        .environments
        .iter()
        .find(|environment| environment.environment_id == finished.environment_id)
        .context("Environment after failed aggregate Up")?;
    ensure!(environment.state == EnvironmentState::Degraded);
    ensure!(
        environment
            .machines
            .iter()
            .filter(|machine| {
                fixtures.iter().any(|fixture| {
                    fixture.profile == KernelProfile::Developer
                        && fixture.owner.machine_id.as_ref() == Some(&machine.machine_id)
                })
            })
            .all(|machine| {
                machine.state == MachineState::Failed
                    && machine.incarnation.is_none()
                    && machine.runtime_identity.is_none()
                    && machine.negotiated_capabilities.capabilities.is_empty()
                    && machine.negotiated_capabilities.unsupported.is_empty()
            })
    );
    ensure!(developer_rejections == 2 && hardened_activation_published);
    Ok(FailedUpProof {
        developer_rejections,
        hardened_activation_published,
    })
}

fn finish_stop(
    store: &StateStore,
    mut operation: EnvironmentLifecycleOperation,
    now: u64,
) -> Result<()> {
    for step in operation.machine_steps.clone() {
        operation = store.acknowledge_environment_machine_step(
            &MachineLifecycleStepAcknowledgement {
                operation_id: operation.operation_id.clone(),
                generation: operation.generation,
                machine_id: step.machine_id,
                initial_state: step.initial_state,
                target_state: step.target_state,
                expected_incarnation: step.expected_incarnation,
                resulting_incarnation: None,
                resulting_activation: None,
                result: LifecycleStepResult::Succeeded,
            },
            now,
        )?;
    }
    store.finish_environment_lifecycle(
        operation.operation_id.as_str(),
        operation.generation,
        now + 1,
    )?;
    Ok(())
}

async fn boot(
    entry: &Arc<MachineRuntimeEntry<MacosRuntimeBackend>>,
    fixture: &MachineFixture,
) -> Result<MachineRuntimeActivation> {
    let activation = entry
        .boot_or_inspect_machine(&fixture.vm_reservation, vec![], fixture.resources.clone())
        .await?;
    ensure!(activation.owner() == &fixture.owner);
    ensure!(activation.runtime_identity().stack_id == fixture.stack_id());
    ensure!(activation.verified_profile() == Some(fixture.profile));
    Ok(activation)
}

async fn guest(activation: &MachineRuntimeActivation, script: &str) -> Result<String> {
    let output = activation
        .exec("/bin/sh".into(), vec!["-c".into(), script.into()], TIMEOUT)
        .await?;
    ensure!(
        output.exit_code == 0,
        "guest exit={} stdout={:?} stderr={:?}",
        output.exit_code,
        output.stdout,
        output.stderr
    );
    Ok(output.stdout)
}

/// Every Docker invocation carries an explicit endpoint and an isolated client
/// configuration. No inherited context, TLS option, or daemon can select a peer.
#[allow(clippy::print_stderr)] // Preserve raw failed-command evidence in the harness log.
async fn host_docker(socket: &Path, config: &Path, args: &[&str], input: Vec<u8>) -> Result<Value> {
    use std::process::Stdio;
    use tokio::io::AsyncWriteExt;
    let mut command = tokio::process::Command::new("/usr/local/bin/docker");
    command.args(["--host", &format!("unix://{}", socket.display())]);
    command.args(args).env("DOCKER_CONFIG", config);
    for name in [
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_TLS",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
        "DOCKER_API_VERSION",
    ] {
        command.env_remove(name);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .context("spawn unmodified host Docker CLI")?;
    let mut stdin = child.stdin.take().context("Docker stdin")?;
    let input_sha256 = format!("{:x}", Sha256::digest(&input));
    let input_bytes = input.len();
    let started = std::time::Instant::now();
    let (write, output) = tokio::time::timeout(Duration::from_secs(60), async move {
        tokio::join!(
            async move {
                stdin.write_all(&input).await?;
                stdin.shutdown().await
            },
            child.wait_with_output()
        )
    })
    .await
    .context("host Docker command exceeded 60 seconds")?;
    let output = output?;
    if output.status.success() {
        write?;
    }
    let value = json!({"args": args, "endpoint": format!("unix://{}", socket.display()),
        "config": config, "exit_code": output.status.code().context("Docker killed by signal")?,
        "stdout": String::from_utf8(output.stdout)?, "stderr": String::from_utf8(output.stderr)?,
        "input_bytes": input_bytes, "input_sha256": input_sha256,
        "elapsed_ms": started.elapsed().as_millis()});
    eprintln!("host Docker command: {}", serde_json::to_string(&value)?);
    Ok(value)
}

fn docker_stdout(value: &Value) -> Result<&str> {
    ensure!(value["exit_code"] == 0, "host Docker failed: {value}");
    value["stdout"].as_str().context("host Docker stdout")
}

async fn host_endpoint_proof(
    a: Arc<MachineRuntimeActivation>,
    b: Arc<MachineRuntimeActivation>,
    hardened: Arc<MachineRuntimeActivation>,
) -> Result<Value> {
    // Short private paths avoid Darwin's sockaddr_un length limit.
    let temporary = tempfile::Builder::new()
        .prefix("vz-de-")
        .tempdir_in("/private/tmp")?;
    let root = temporary.path();
    fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
    let config = root.join("client");
    fs::DirBuilder::new().mode(0o700).create(&config)?;
    let decoy = root.join("unrelated.sock");
    fs::write(&decoy, b"unrelated-host-file")?;
    let hardened_path = MachineDockerEndpoint::socket_path_for(root, hardened.owner())?;
    let hardened_refusal =
        match MachineDockerEndpoint::start(Arc::clone(&hardened), &hardened_path).await {
            Err(error) => error.to_string(),
            Ok(endpoint) => {
                endpoint.shutdown().await?;
                return Err(anyhow!("Hardened Machine acquired a Docker endpoint"));
            }
        };
    ensure!(!hardened_path.exists());
    let target_a = MachineDockerEndpoint::socket_path_for(root, a.owner())?;
    let target_b = MachineDockerEndpoint::socket_path_for(root, b.owner())?;
    fs::write(&target_a, b"do-not-adopt-this-endpoint")?;
    let collision_before = fs::symlink_metadata(&target_a)?;
    let preexisting_path_refusal =
        match MachineDockerEndpoint::start(Arc::clone(&a), &target_a).await {
            Err(error) => error.to_string(),
            Ok(endpoint) => {
                endpoint.shutdown().await?;
                return Err(anyhow!("endpoint adopted a preexisting host file"));
            }
        };
    ensure!(
        fs::read(&target_a)? == b"do-not-adopt-this-endpoint"
            && fs::symlink_metadata(&target_a)?.ino() == collision_before.ino()
    );
    // Only remove the fixture file whose identity and bytes were just verified.
    fs::remove_file(&target_a)?;
    let endpoint_a = MachineDockerEndpoint::start(Arc::clone(&a), &target_a).await?;
    let endpoint_b = match MachineDockerEndpoint::start(Arc::clone(&b), &target_b).await {
        Ok(endpoint) => endpoint,
        Err(error) => {
            endpoint_a.shutdown().await?;
            return Err(error.into());
        }
    };
    let path_a = endpoint_a.socket_path().to_path_buf();
    let path_b = endpoint_b.socket_path().to_path_buf();
    let result = async {
        let socket_modes = [
            fs::symlink_metadata(&path_a)?.mode() & 0o7777,
            fs::symlink_metadata(&path_b)?.mode() & 0o7777,
        ];
        ensure!(socket_modes == [0o600, 0o600]);
        let mut commands = BTreeMap::new();
        let version = host_docker(&path_a, &config, &["--version"], vec![]).await?;
        ensure!(docker_stdout(&version)?.starts_with("Docker version "));
        commands.insert("client_version", version);
        let info_args = ["info", "--format", "{{json .}}"];
        let (info_a, info_b) = tokio::join!(
            host_docker(&path_a, &config, &info_args, vec![]),
            host_docker(&path_b, &config, &info_args, vec![])
        );
        let (info_a, info_b) = (info_a?, info_b?);
        let engine_a: Value = serde_json::from_str(docker_stdout(&info_a)?)?;
        let engine_b: Value = serde_json::from_str(docker_stdout(&info_b)?)?;
        ensure!(
            engine_a["ID"].as_str().is_some_and(|id| !id.is_empty())
                && engine_a["ID"] != engine_b["ID"]
        );
        ensure!(engine_a["DefaultRuntime"] == "youki" && engine_b["DefaultRuntime"] == "youki");
        ensure!(engine_a["MemoryLimit"] == true && engine_b["MemoryLimit"] == true);
        for engine in [&engine_a, &engine_b] {
            let features: Value = serde_json::from_str(
                engine["Runtimes"]["youki"]["status"]["org.opencontainers.runtime-spec.features"]
                    .as_str()
                    .context("youki runtime feature report")?,
            )?;
            ensure!(features["linux"]["cgroup"]["v2"] == true);
        }
        commands.insert("info_a", info_a);
        commands.insert("info_b", info_b);

        // Import an offline test image through the host CLI, using the exact
        // busybox bytes executing in this guest rather than pulling an unpinned tag.
        let busybox = PathBuf::from(std::env::var_os(DEV_BUNDLE_ENV).context(DEV_BUNDLE_ENV)?)
            .join("busybox");
        let busybox = fs::canonicalize(busybox)?;
        let busybox_sha256 = file_sha(&busybox)?;
        for activation in [&a, &b] {
            ensure!(
                guest(activation, "/bin/busybox sha256sum /bin/busybox")
                    .await?
                    .split_whitespace()
                    .next()
                    == Some(busybox_sha256.as_str())
            );
        }
        let image_root = root.join("image");
        fs::create_dir(&image_root)?;
        fs::create_dir(image_root.join("bin"))?;
        fs::copy(&busybox, image_root.join("bin/busybox"))?;
        fs::set_permissions(
            image_root.join("bin/busybox"),
            fs::Permissions::from_mode(0o755),
        )?;
        let tar = Command::new("/usr/bin/tar")
            .args(["-cf", "-"])
            .arg("-C")
            .arg(&image_root)
            .arg("bin")
            .output()?;
        ensure!(tar.status.success(), "fixture rootfs tar failed");
        for (label, socket) in [("a", &path_a), ("b", &path_b)] {
            let imported = host_docker(
                socket,
                &config,
                &["image", "import", "-", "vz-endpoint-fixture:local"],
                tar.stdout.clone(),
            )
            .await?;
            ensure!(docker_stdout(&imported)?.trim().starts_with("sha256:"));
            commands.insert(if label == "a" { "import_a" } else { "import_b" }, imported);
            let marker = if label == "a" {
                "developer-a"
            } else {
                "developer-b"
            };
            let created = host_docker(
                socket,
                &config,
                &[
                    "volume",
                    "create",
                    "--label",
                    &format!("dev.vz.endpoint.owner={marker}"),
                    "vz-endpoint-shared",
                ],
                vec![],
            )
            .await?;
            ensure!(docker_stdout(&created)?.trim() == "vz-endpoint-shared");
            commands.insert(if label == "a" { "volume_a" } else { "volume_b" }, created);
            let script = format!("printf {marker} > /data/marker; /bin/busybox cat /data/marker");
            let write_args = [
                "run",
                "--rm",
                "--network",
                "none",
                "-v",
                "vz-endpoint-shared:/data",
                "vz-endpoint-fixture:local",
                "/bin/busybox",
                "sh",
                "-c",
                &script,
            ];
            let write = host_docker(socket, &config, &write_args, vec![]);
            // Diagnostic-only observation of the fixture's runtime logs. It
            // neither substitutes a guest client nor changes OCI execution.
            // The shim removes its bundle after a failed create, so a later
            // daemon-log dump alone cannot retain the actual runtime error.
            let written = if std::env::var_os("VZ_TEST_DOCKER_TRACE_CREATE").is_some() {
                let activation = if label == "a" { &a } else { &b };
                let (written, trace) = tokio::join!(write, trace_docker_create(activation));
                let written = written?;
                ensure!(
                    written["exit_code"] == 0,
                    "host Docker failed: {written}\n{trace}"
                );
                written
            } else {
                write.await?
            };
            ensure!(docker_stdout(&written)? == marker);
            commands.insert(if label == "a" { "write_a" } else { "write_b" }, written);
        }
        let (time_a, time_b) = tokio::join!(
            docker_time_namespace::prove(&a, &path_a, &config),
            docker_time_namespace::prove(&b, &path_b, &config),
        );
        let time_namespaces = [time_a?, time_b?];
        let (devices_a, devices_b) = tokio::join!(
            docker_device_policy::prove(&a, &path_a, &config),
            docker_device_policy::prove(&b, &path_b, &config),
        );
        let device_policies = [devices_a?, devices_b?];
        let (seccomp_a, seccomp_b) = tokio::join!(
            docker_seccomp_policy::prove(&a, &path_a, &config),
            docker_seccomp_policy::prove(&b, &path_b, &config),
        );
        let seccomp_policies = [seccomp_a?, seccomp_b?];
        let read_args = [
            "run",
            "--rm",
            "--network",
            "none",
            "-v",
            "vz-endpoint-shared:/data",
            "vz-endpoint-fixture:local",
            "/bin/busybox",
            "cat",
            "/data/marker",
        ];
        let (read_a, read_b) = tokio::join!(
            host_docker(&path_a, &config, &read_args, vec![]),
            host_docker(&path_b, &config, &read_args, vec![])
        );
        let (read_a, read_b) = (read_a?, read_b?);
        ensure!(
            docker_stdout(&read_a)? == "developer-a" && docker_stdout(&read_b)? == "developer-b"
        );
        commands.insert("read_a", read_a);
        commands.insert("read_b", read_b);
        for (label, socket) in [("memory_a", &path_a), ("memory_b", &path_b)] {
            let limited = host_docker(
                socket,
                &config,
                &[
                    "run",
                    "--rm",
                    "--network",
                    "none",
                    "--memory",
                    "64m",
                    "vz-endpoint-fixture:local",
                    "/bin/busybox",
                    "cat",
                    "/sys/fs/cgroup/memory.max",
                ],
                vec![],
            )
            .await?;
            ensure!(docker_stdout(&limited)?.trim() == "67108864");
            commands.insert(label, limited);
        }
        let input = b"vz-endpoint-half-close\n".repeat(12_000);
        let streamed = host_docker(
            &path_a,
            &config,
            &[
                "run",
                "-i",
                "--rm",
                "--network",
                "none",
                "vz-endpoint-fixture:local",
                "/bin/busybox",
                "sh",
                "-c",
                "/bin/busybox cat; /bin/busybox sleep 1; printf done",
            ],
            input.clone(),
        )
        .await?;
        let mut expected = input;
        expected.extend_from_slice(b"done");
        ensure!(docker_stdout(&streamed)?.as_bytes() == expected);
        commands.insert("stdin_eof", streamed);
        let since = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let since = since.to_string();
        let until = (since.parse::<u64>()? + 3).to_string();
        let events_args = [
            "events",
            "--since",
            &since,
            "--until",
            &until,
            "--filter",
            "type=volume",
            "--filter",
            "event=create",
            "--format",
            "{{json .}}",
        ];
        let create_args = [
            "volume",
            "create",
            "--label",
            "dev.vz.endpoint.owner=developer-a",
            "vz-endpoint-event",
        ];
        let (events, created) = tokio::join!(
            host_docker(&path_a, &config, &events_args, vec![]),
            host_docker(&path_a, &config, &create_args, vec![])
        );
        let (events, created) = (events?, created?);
        ensure!(docker_stdout(&created)?.trim() == "vz-endpoint-event");
        let events_json: Vec<Value> = docker_stdout(&events)?
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()?;
        ensure!(events_json.iter().any(|event| event["Type"] == "volume"
            && event["Action"] == "create"
            && event["Actor"]["ID"] == "vz-endpoint-event"));
        commands.insert("events_a", events);
        commands.insert("event_volume_a", created);
        Ok::<_, anyhow::Error>((
            commands,
            busybox_sha256,
            socket_modes,
            time_namespaces,
            device_policies,
            seccomp_policies,
        ))
    }
    .await;
    let result = match result {
        Ok(value) => Ok(value),
        Err(error) => {
            let (diagnostics_a, diagnostics_b) = tokio::join!(
                docker_failure_diagnostics(&a, "developer-a host endpoint"),
                docker_failure_diagnostics(&b, "developer-b host endpoint")
            );
            Err(anyhow!("{error:#}\n{diagnostics_a}\n{diagnostics_b}"))
        }
    };
    // Always stop both adapters before returning an assertion failure. The VM
    // cleanup path must never wait on an endpoint's retained activation lease.
    let stop_a = endpoint_a.shutdown().await;
    let after_a = if result.is_ok() && stop_a.is_ok() {
        Some(tokio::join!(
            host_docker(&path_a, &config, &["info"], vec![]),
            host_docker(
                &path_b,
                &config,
                &["info", "--format", "{{json .}}"],
                vec![]
            )
        ))
    } else {
        None
    };
    let stop_b = endpoint_b.shutdown().await;
    let shutdown_a = stop_a?;
    let shutdown_b = stop_b?;
    ensure!(shutdown_a.active_connections == 0 && shutdown_b.active_connections == 0);
    ensure!(shutdown_a.socket_removed && shutdown_b.socket_removed);
    let (
        mut commands,
        busybox_sha256,
        socket_modes,
        time_namespaces,
        device_policies,
        seccomp_policies,
    ) = result?;
    let (refused, survivor) = after_a.context("missing endpoint shutdown probe")?;
    let (refused, survivor) = (refused?, survivor?);
    ensure!(refused["exit_code"].as_i64().is_some_and(|code| code != 0));
    let survivor_info: Value = serde_json::from_str(docker_stdout(&survivor)?)?;
    let previous_info: Value = serde_json::from_str(docker_stdout(&commands["info_b"])?)?;
    ensure!(survivor_info["ID"] == previous_info["ID"]);
    commands.insert("stopped_a", refused);
    commands.insert("surviving_b", survivor);
    ensure!(!path_a.exists() && !path_b.exists());
    ensure!(fs::read(&decoy)? == b"unrelated-host-file");
    let value = json!({"scope": "focused_host_endpoint_transport_only", "client": "/usr/local/bin/docker",
        "client_sha256": file_sha(Path::new("/usr/local/bin/docker"))?, "busybox_sha256": busybox_sha256,
        "owners": [a.owner(), b.owner()], "runtime_identities": [a.runtime_identity(), b.runtime_identity()],
        "commands": commands, "socket_modes": socket_modes,
        "time_namespaces": time_namespaces,
        "device_policies": device_policies,
        "seccomp_policies": seccomp_policies,
        "shutdown": [shutdown_a, shutdown_b],
        "sockets_removed": true, "unrelated_file_preserved": true, "managed_contexts": false,
        "compose_buildx": false, "hardened_refusal": hardened_refusal,
        "preexisting_path_refusal": preexisting_path_refusal});
    temporary.close()?;
    Ok(value)
}

async fn trace_docker_create(activation: &MachineRuntimeActivation) -> String {
    let script = r#"
set +e
sample=0
emitted=0
previous=''
config_seen=''
while test "$sample" -lt 600 && test "$emitted" -lt 16; do
  for config in /run/vz-docker/containerd/io.containerd.runtime.v2.task/moby/*/config.json; do
    if test -s "$config" && test "$config" != "$config_seen"; then
      echo "--- OCI create config: $config sample=$sample ---"
      /bin/busybox head -c 32768 "$config"
      echo
      config_seen="$config"
    fi
  done
  for log in /run/vz-docker/containerd/io.containerd.runtime.v2.task/moby/*/log.json; do
    if test -s "$log"; then
      current=$(/bin/busybox sha256sum "$log" 2>/dev/null)
      if test "$current" != "$previous"; then
        echo "--- OCI create log: $log sample=$sample ---"
        /bin/busybox tail -c 8192 "$log"
        previous="$current"
        emitted=$((emitted + 1))
      fi
    fi
  done
  sample=$((sample + 1))
  /bin/busybox sleep 0.01
done
echo "--- OCI create observer finished: samples=$sample emitted=$emitted ---"
"#;
    match activation
        .exec(
            "/bin/sh".into(),
            vec!["-c".into(), script.into()],
            Duration::from_secs(12),
        )
        .await
    {
        Ok(output) => format!(
            "OCI create observer exit={}\nstdout:\n{}\nstderr:\n{}",
            output.exit_code, output.stdout, output.stderr
        ),
        Err(error) => format!("OCI create observer failed: {error}"),
    }
}

async fn docker_failure_diagnostics(activation: &MachineRuntimeActivation, label: &str) -> String {
    let script = r#"
set +e
echo '--- mounts ---'
/bin/busybox grep -E ' (/var/lib/docker|/mnt/vz-docker-bin|/mnt/linux-bin) ' /proc/mounts
echo '--- youki metadata ---'
/bin/busybox stat -c 'path=%n mode=%a uid=%u gid=%g size=%s inode=%i links=%h device=%d' /mnt/linux-bin/youki /usr/local/bin/youki
/bin/busybox sha256sum /mnt/linux-bin/youki
/mnt/linux-bin/youki --version
/usr/local/bin/youki --version
echo '--- daemon processes ---'
/bin/busybox ps
for log in /var/lib/docker/log/containerd.log /var/lib/docker/log/dockerd.log; do
  echo "--- $log (last 65536 bytes) ---"
  if test -f "$log"; then
    /bin/busybox tail -c 65536 "$log"
  else
    echo '<missing>'
  fi
done
echo '--- end Docker diagnostics ---'
exit 0
"#;
    match activation
        .exec(
            "/bin/sh".into(),
            vec!["-c".into(), script.into()],
            Duration::from_secs(10),
        )
        .await
    {
        Ok(output) => format!(
            "Machine {label} diagnostic exit={}\nstdout:\n{}\nstderr:\n{}",
            output.exit_code, output.stdout, output.stderr
        ),
        Err(error) => format!("Machine {label} diagnostic capture failed: {error}"),
    }
}

#[expect(
    clippy::print_stderr,
    reason = "fail-only guest diagnostics must survive in the physical harness log"
)]
async fn ensure_docker_ready_with_diagnostics(
    activation: &MachineRuntimeActivation,
    label: &str,
) -> Result<SharedVmDockerReadiness> {
    match activation.ensure_docker_ready().await {
        Ok(readiness) => Ok(readiness),
        Err(error) => {
            let diagnostics = docker_failure_diagnostics(activation, label).await;
            eprintln!("{diagnostics}");
            Err(anyhow!(
                "Machine {label} Docker readiness failed: {error}; diagnostics were written to the harness log"
            ))
        }
    }
}

fn docker_probe_evidence(output: &str, operation: &str, marker: &str) -> Result<Value> {
    let value: Value = serde_json::from_str(output.trim()).context("Docker API probe JSON")?;
    ensure!(value["operation"] == operation);
    ensure!(value["name"] == "vz-registry-shared");
    ensure!(value["mountpoint"] == "/var/lib/docker/engine/volumes/vz-registry-shared/_data");
    ensure!(value["marker"] == marker);
    ensure!(value["api_owner"] == marker);
    ensure!(value["marker_sha256"] == format!("{:x}", Sha256::digest(marker.as_bytes())));
    Ok(value)
}

async fn shutdown(
    runtime: &Runtime,
    identity: &StackRuntimeIdentity,
    operation: &str,
) -> Result<StackRuntimeShutdownOutcome> {
    Ok(runtime
        .shutdown_shared_vm_exact(&StackRuntimeShutdownRequest {
            schema_version: STACK_RUNTIME_SHUTDOWN_REQUEST_SCHEMA_VERSION,
            operation_id: operation.into(),
            expected: identity.clone(),
        })
        .await?)
}

async fn cleanup(targets: &[CleanupTarget]) -> Result<()> {
    let mut failures = Vec::new();
    for target in targets.iter().rev() {
        match target
            .runtime
            .inspect_shared_vm_identity(&target.stack_id)
            .await
        {
            Ok(Some(identity)) => {
                if let Err(error) =
                    shutdown(&target.runtime, &identity, "machine-registry-e2e-cleanup").await
                {
                    failures.push(error.to_string());
                }
            }
            Ok(None) => {}
            Err(error) => failures.push(error.to_string()),
        }
    }
    ensure!(failures.is_empty(), "VM cleanup failures: {failures:?}");
    Ok(())
}

fn cleanup_pin_permissions(root: &Path, directories: &[PathBuf]) -> Result<()> {
    for directory in directories {
        ensure!(directory.starts_with(root));
        ensure!(matches!(
            directory.file_name().and_then(|name| name.to_str()),
            Some("linux-target" | "bundle")
        ));
        let metadata = fs::symlink_metadata(directory)?;
        ensure!(metadata.is_dir() && !metadata.file_type().is_symlink());
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn locked_store_acquisition_refused(
    registry: &MachineRuntimeRegistry<MacosRuntimeBackend>,
    fixture: &MachineFixture,
) -> Result<bool> {
    let result = registry.acquire_store(
        &fixture.owner,
        &fixture.store_reservation,
        None,
        MachineRuntimeAdmission::ExistingOnly,
    );
    ensure!(matches!(
        result,
        Err(MachineRuntimeRegistryError::Leased(_))
    ));
    Ok(true)
}

fn immutable_identity(path: &Path) -> Result<Value> {
    let metadata = fs::symlink_metadata(path)?;
    Ok(json!({
        "device": metadata.dev(),
        "inode": metadata.ino(),
        "mode": metadata.mode() & 0o7777,
        "uid": metadata.uid(),
        "links": metadata.nlink(),
        "size": metadata.size(),
        "mtime_seconds": metadata.mtime(),
        "mtime_nanoseconds": metadata.mtime_nsec(),
        "ctime_seconds": metadata.ctime(),
        "ctime_nanoseconds": metadata.ctime_nsec(),
    }))
}

fn installed(data_path: &Path) -> Result<Value> {
    let pin_dir = fs::canonicalize(data_path.join("linux-target"))?;
    let dir = fs::canonicalize(pin_dir.join("bundle"))?;
    let configuration_path = fs::canonicalize(pin_dir.join("configuration.json"))?;
    let configuration_bytes = fs::read(&configuration_path)?;
    let configuration: Value = serde_json::from_slice(&configuration_bytes)?;
    let profile = configuration["kernel_profile"]
        .as_str()
        .context("pinned kernel profile")?;
    let version_path = dir.join("version.json");
    ensure!(fs::symlink_metadata(&version_path)?.len() <= 1024 * 1024);
    let version_json = fs::read_to_string(&version_path)?;
    let version: vz_linux::KernelVersion = serde_json::from_str(&version_json)?;
    ensure!(version.profile.as_deref() == Some(profile));
    let artifacts = artifact_names(&version)?;
    let mut observed_names = fs::read_dir(&dir)?
        .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
        .collect::<Result<Vec<_>>>()?;
    observed_names.sort();
    let mut expected_names = artifacts.clone();
    expected_names.sort();
    ensure!(
        observed_names == expected_names,
        "unexpected pinned bundle file inventory"
    );
    let developer_probe_sha256 = version
        .developer_probe
        .as_ref()
        .map(|probe| {
            let actual = file_sha(&dir.join(vz_linux::DEVELOPER_PROBE_ARCHIVE))?;
            ensure!(
                actual == probe.sha256,
                "pinned Developer probe digest mismatch"
            );
            Ok::<_, anyhow::Error>(actual)
        })
        .transpose()?;
    let mut artifact_identities = BTreeMap::new();
    for name in artifacts {
        let expected_mode = if name == "youki" { 0o500 } else { 0o400 };
        let path = dir.join(name);
        let metadata = fs::symlink_metadata(&path)?;
        ensure!(
            metadata.is_file()
                && metadata.nlink() == 1
                && metadata.mode() & 0o7777 == expected_mode
        );
        artifact_identities.insert(name, immutable_identity(&path)?);
    }
    ensure!(fs::symlink_metadata(&pin_dir)?.mode() & 0o7777 == 0o700);
    ensure!(fs::symlink_metadata(&dir)?.mode() & 0o7777 == 0o500);
    ensure!(fs::symlink_metadata(&configuration_path)?.mode() & 0o7777 == 0o400);
    Ok(json!({
        "pin_dir": pin_dir,
        "dir": dir,
        "profile": profile,
        "kernel_sha256": file_sha(&dir.join("vmlinux"))?,
        "initramfs_sha256": file_sha(&dir.join("initramfs.img"))?,
        "youki_sha256": file_sha(&dir.join("youki"))?,
        "version_sha256": file_sha(&dir.join("version.json"))?,
        "version_json": version_json,
        "developer_probe_sha256": developer_probe_sha256,
        "configuration_path": configuration_path,
        "configuration_sha256": format!("{:x}", Sha256::digest(&configuration_bytes)),
        "pin_directory_identity": immutable_identity(&pin_dir)?,
        "bundle_directory_identity": immutable_identity(&dir)?,
        "configuration_identity": immutable_identity(&configuration_path)?,
        "artifact_identities": artifact_identities,
    }))
}

fn docker_disk(entry: &MachineRuntimeEntry<MacosRuntimeBackend>, stack: &str) -> PathBuf {
    entry
        .data_path()
        .join("docker-machines")
        .join(format!("{:x}", Sha256::digest(stack.as_bytes())))
        .join("data.img")
}

fn install_docker_probe(
    entry: &MachineRuntimeEntry<MacosRuntimeBackend>,
    source: &Path,
) -> Result<PathBuf> {
    let rootfs = entry
        .runtime()
        .linux()
        .expect("Linux runtime")
        .rootfs_store_dir();
    fs::create_dir_all(&rootfs)?;
    let target = rootfs.join("vz-machine-registry-docker-probe");
    let mut source_file = File::open(source)?;
    let mut target_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o555)
        .open(&target)?;
    io::copy(&mut source_file, &mut target_file)?;
    target_file.sync_all()?;
    drop(target_file);
    File::open(&rootfs)?.sync_all()?;
    let metadata = fs::symlink_metadata(&target)?;
    ensure!(metadata.is_file() && metadata.nlink() == 1);
    ensure!(metadata.mode() & 0o777 == 0o555);
    Ok(target)
}

fn host_identity(path: &Path) -> Result<Value> {
    let metadata = fs::symlink_metadata(path)?;
    Ok(json!({
        "device": metadata.dev(),
        "inode": metadata.ino(),
        "mode": metadata.mode() & 0o7777,
        "uid": metadata.uid(),
        "links": metadata.nlink(),
        "size": metadata.size(),
    }))
}

fn storage_evidence(
    entries: &[Arc<MachineRuntimeEntry<MacosRuntimeBackend>>],
    fixtures: &[MachineFixture],
) -> Result<Value> {
    let mut roots = Vec::new();
    let mut root_identities = Vec::new();
    let mut identities = Vec::new();
    let mut installs = BTreeMap::new();
    for (entry, fixture) in entries.iter().zip(fixtures) {
        let root = fs::canonicalize(entry.data_path())?;
        let metadata = fs::metadata(&root)?;
        ensure!(metadata.mode() & 0o777 == 0o700);
        ensure!(
            identities
                .iter()
                .all(|identity| *identity != (metadata.dev(), metadata.ino()))
        );
        identities.push((metadata.dev(), metadata.ino()));
        root_identities.push(host_identity(&root)?);
        for child in [
            entry
                .runtime()
                .linux()
                .expect("Linux runtime")
                .rootfs_store_dir(),
            entry
                .runtime()
                .linux()
                .expect("Linux runtime")
                .setup_commits_host_dir(),
            entry.data_path().join("linux-target"),
        ] {
            ensure!(fs::canonicalize(child)?.starts_with(&root));
        }
        roots.push(root);
        installs.insert(fixture.name, installed(entry.data_path())?);
    }
    let disk_a = docker_disk(&entries[0], fixtures[0].stack_id());
    let disk_b = docker_disk(&entries[1], fixtures[1].stack_id());
    let meta_a = fs::metadata(&disk_a)?;
    let meta_b = fs::metadata(&disk_b)?;
    ensure!(meta_a.is_file() && meta_b.is_file());
    ensure!(meta_a.nlink() == 1 && meta_b.nlink() == 1);
    ensure!((meta_a.dev(), meta_a.ino()) != (meta_b.dev(), meta_b.ino()));
    ensure!(!entries[2].data_path().join("docker-machines").exists());
    let disk_identities = [host_identity(&disk_a)?, host_identity(&disk_b)?];
    Ok(json!({
        "data_roots": roots,
        "data_root_identities": root_identities,
        "private_0700_and_distinct_inodes": true,
        "all_writable_roots_below_machine_data": true,
        "installed_artifacts": installs,
        "developer_docker_disks": [disk_a, disk_b],
        "developer_docker_disk_identities": disk_identities,
        "developer_docker_disks_distinct_inodes": true,
        "hardened_docker_state_absent": true,
    }))
}

fn machine_evidence(
    fixture: &MachineFixture,
    first: &StackRuntimeIdentity,
    reopened: &StackRuntimeIdentity,
) -> Value {
    json!({
        "owner": fixture.owner,
        "store_reservation": fixture.store_reservation,
        "vm_reservation": fixture.vm_reservation,
        "configuration_digest": fixture.config_digest,
        "resolved_configuration": fixture.resolved_configuration,
        "verified_profile": fixture.profile.as_str(),
        "artifact": fixture.artifact,
        "first_identity": first,
        "reopened_identity": reopened,
    })
}

async fn run_inner(
    root: &Path,
    cleanup_targets: &mut Vec<CleanupTarget>,
    pin_cleanup_directories: &mut Vec<PathBuf>,
) -> Result<Value> {
    let (source_root, developer_bundle, hardened_bundle) = create_fixture_sources(root).await?;
    let developer_verified =
        vz_linux::verify_kernel_bundle_read_only(&developer_bundle, KernelProfile::Developer)
            .await?;
    let hardened_verified =
        vz_linux::verify_kernel_bundle_read_only(&hardened_bundle, KernelProfile::Container)
            .await?;
    let project_id = vz_runtime_contract::ProjectId::new("prj_machine_registry_e2e")?;
    let definition = definition(
        project_id.clone(),
        &developer_verified.artifact_identity.digest,
        &hardened_verified.artifact_identity.digest,
    )?;
    let resolver = MachineTargetResolver::new(
        host(),
        target_catalog(
            developer_bundle,
            developer_verified.artifact_identity.digest,
            hardened_bundle,
            hardened_verified.artifact_identity.digest,
        ),
    )?;
    let invalid_sibling_rejected_without_state =
        invalid_sibling_preflight(root, &source_root, &resolver, &definition).await?;
    let resolved = resolver.resolve_project(&definition).await?;
    ensure!(resolved.machines.len() == definition.environment.machines.len());
    ensure!(resolved.definition_digest == definition.digest()?);
    let all_machines_resolved_before_state = true;
    let environment = definition.instantiate_environment("registry-e2e", 100)?;
    let environment_id = environment.environment_id.clone();
    let store_path = root.join("topology.db");
    let store = StateStore::open(&store_path)?;
    store.save_project_state(&ProjectState {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        definition,
        environments: vec![environment],
    })?;
    let state = store
        .load_project_state(project_id.as_str())?
        .context("Project state")?;
    let fixtures = fixtures(&state, &resolved)?;
    let registry_root = root.join("registry");
    fs::DirBuilder::new().mode(0o700).create(&registry_root)?;
    let registry = MachineRuntimeRegistry::new(registry_root.clone())?;
    let controller = EnvironmentRuntimeController::default();
    let prepared = controller
        .acquire(&project_id, &environment_id)
        .await?
        .prepare(&store, &registry, &resolver, &state.environments[0], 101)
        .await?;
    ensure!(prepared.environment().environment_id == environment_id);
    let environment_scoped_serialization =
        controller_serialization_proof(&controller, &project_id, &environment_id).await?;
    let stores = prepared_stores(prepared.pins(), &fixtures)?;
    for fixture in &fixtures {
        let pin = prepared
            .pins()
            .iter()
            .find(|pin| pin.configuration().machine.name == fixture.name)
            .context("prepared cleanup pin")?;
        let bundle = pin.bundle_dir();
        pin_cleanup_directories.push(bundle.clone());
        pin_cleanup_directories.push(bundle.parent().context("pin parent")?.to_path_buf());
    }
    let creating_owned_read_only = require_owned_read_only(&store, project_id.as_str(), &fixtures)?;
    let pin_snapshot_before_replay = pin_snapshot(&stores, &fixtures)?;
    let mut replay_cleanup_directories = Vec::new();
    let replay_pins = pin_all(
        &stores,
        &fixtures,
        &resolved,
        &mut replay_cleanup_directories,
    )
    .await?;
    ensure!(replay_cleanup_directories.as_slice() == pin_cleanup_directories.as_slice());
    let pin_snapshot_after_replay = pin_snapshot(&stores, &fixtures)?;
    ensure!(pin_snapshot_before_replay == pin_snapshot_after_replay);
    drop(replay_pins);
    fs::remove_dir_all(&source_root)?;
    ensure!(!source_root.exists());
    File::open(root)?.sync_all()?;
    drop(resolved);
    drop(resolver);
    let first_up = store.begin_environment_lifecycle(
        environment_id.as_str(),
        EnvironmentLifecycleKind::Up,
        "req-registry-up-1",
        "idem-registry-up-1",
        "sha256:registry-up-1",
        110,
    )?;
    for fixture in &fixtures {
        require_machine_fence(&store, &first_up, fixture)?;
    }
    let mut stale_first_up = first_up.clone();
    stale_first_up.generation += 1;
    ensure!(
        require_machine_fence(&store, &stale_first_up, &fixtures[0]).is_err(),
        "stale lifecycle generation authorized a Machine boot"
    );
    let stale_controller_attachment_refused = prepared
        .attach_machine(
            &store,
            &registry,
            &stale_first_up,
            fixtures[0].owner.machine_id.as_ref().unwrap(),
        )
        .is_err();
    ensure!(
        stale_controller_attachment_refused,
        "controller attached a stale lifecycle generation"
    );
    let mut entries = Vec::new();
    for fixture in &fixtures {
        let machine_id = fixture
            .owner
            .machine_id
            .as_ref()
            .context("fixture Machine ID")?;
        let entry = prepared.attach_machine(&store, &registry, &first_up, machine_id)?;
        cleanup_targets.push(CleanupTarget {
            runtime: entry.runtime().linux().expect("Linux runtime").clone(),
            stack_id: fixture.stack_id().into(),
        });
        let replay = prepared.attach_machine(&store, &registry, &first_up, machine_id)?;
        ensure!(Arc::ptr_eq(&entry, &replay));
        entries.push(entry);
    }
    let entries_controller_verified = entries.len() == fixtures.len()
        && entries
            .iter()
            .zip(&fixtures)
            .all(|(entry, fixture)| entry.owner() == &fixture.owner);
    ensure!(entries_controller_verified);
    drop(stores);
    let docker_probe = fs::canonicalize(PathBuf::from(
        std::env::var_os(DOCKER_PROBE_ENV).context(DOCKER_PROBE_ENV)?,
    ))?;
    let docker_probe_sha256 = file_sha(&docker_probe)?;
    let probe_a_path = install_docker_probe(&entries[0], &docker_probe)?;
    let probe_b_path = install_docker_probe(&entries[1], &docker_probe)?;
    ensure!(file_sha(&probe_a_path)? == docker_probe_sha256);
    ensure!(file_sha(&probe_b_path)? == docker_probe_sha256);
    // Both Developer Machines boot concurrently and remain live together.
    let (dev_a, dev_b) = tokio::join!(
        boot(&entries[0], &fixtures[0]),
        boot(&entries[1], &fixtures[1])
    );
    let dev_a = Arc::new(dev_a?);
    let dev_b = Arc::new(dev_b?);
    let hard = Arc::new(boot(&entries[2], &fixtures[2]).await?);
    let first = vec![
        dev_a.runtime_identity().clone(),
        dev_b.runtime_identity().clone(),
        hard.runtime_identity().clone(),
    ];
    let (ready_a, ready_b) = tokio::join!(
        ensure_docker_ready_with_diagnostics(&dev_a, "developer-a first boot"),
        ensure_docker_ready_with_diagnostics(&dev_b, "developer-b first boot")
    );
    ensure!(ready_a?.runtime_identity == first[0]);
    ensure!(ready_b?.runtime_identity == first[1]);
    ensure!(matches!(
        hard.ensure_docker_ready().await,
        Err(vz_oci_macos::MacosOciError::UnsupportedOperation { .. })
    ));

    let write_developer = |label: &'static str| {
        format!(
            "set -eu; probe=$(/vz-rootfs/vz-machine-registry-docker-probe create --socket /run/vz-docker/docker.sock --volume vz-registry-shared --marker {label}); mkdir -p /vz-rootfs/registry-e2e /vz-setup-commits; printf {label} > /vz-rootfs/registry-e2e/shared; printf {label} > /vz-setup-commits/shared; printf {label}-only > /vz-rootfs/registry-e2e/{label}-only; sync; printf '%s' \"$probe\""
        )
    };
    let hard_write = "set -eu; test ! -e /vz-rootfs/vz-machine-registry-docker-probe; mkdir -p /vz-rootfs/registry-e2e /vz-setup-commits; printf hardened > /vz-rootfs/registry-e2e/shared; printf hardened > /vz-setup-commits/shared; printf hardened-only > /vz-rootfs/registry-e2e/hardened-only; sync";
    let write_a_script = write_developer("developer-a");
    let write_b_script = write_developer("developer-b");
    let (write_a, write_b, write_h) = tokio::join!(
        guest(&dev_a, &write_a_script),
        guest(&dev_b, &write_b_script),
        guest(&hard, hard_write),
    );
    let create_probe_a = docker_probe_evidence(&write_a?, "create", "developer-a")?;
    let create_probe_b = docker_probe_evidence(&write_b?, "create", "developer-b")?;
    write_h?;
    let verify_developer = |label: &'static str, sibling: &'static str| {
        format!(
            "set -eu; probe=$(/vz-rootfs/vz-machine-registry-docker-probe verify --socket /run/vz-docker/docker.sock --volume vz-registry-shared --marker {label}); test \"$(cat /vz-rootfs/registry-e2e/shared)\" = {label}; test \"$(cat /vz-setup-commits/shared)\" = {label}; test ! -e /vz-rootfs/registry-e2e/{sibling}-only; test ! -e /vz-rootfs/registry-e2e/hardened-only; printf '%s' \"$probe\""
        )
    };
    let verify_a_script = verify_developer("developer-a", "developer-b");
    let verify_b_script = verify_developer("developer-b", "developer-a");
    let (probe_a, probe_b, probe_h) = tokio::join!(
        guest(&dev_a, &verify_a_script),
        guest(&dev_b, &verify_b_script),
        guest(
            &hard,
            "set -eu; test \"$(cat /vz-rootfs/registry-e2e/shared)\" = hardened; test \"$(cat /vz-setup-commits/shared)\" = hardened; test ! -e /vz-rootfs/registry-e2e/developer-a-only; test ! -e /vz-rootfs/registry-e2e/developer-b-only; printf hardened-isolated"
        ),
    );
    let first_verify_probe_a = docker_probe_evidence(&probe_a?, "verify", "developer-a")?;
    let first_verify_probe_b = docker_probe_evidence(&probe_b?, "verify", "developer-b")?;
    ensure!(probe_h? == "hardened-isolated");
    let first_storage = storage_evidence(&entries, &fixtures)?;
    let host_endpoint =
        host_endpoint_proof(Arc::clone(&dev_a), Arc::clone(&dev_b), Arc::clone(&hard)).await?;

    drop(dev_a);
    drop(dev_b);
    drop(hard);
    require_machine_fence(&store, &first_up, &fixtures[0])?;
    require_machine_fence(&store, &first_up, &fixtures[1])?;
    let (replay_a, replay_b) = tokio::join!(
        boot(&entries[0], &fixtures[0]),
        boot(&entries[1], &fixtures[1])
    );
    let replay_a = replay_a?;
    let replay_b = replay_b?;
    ensure!(replay_a.runtime_identity() == &first[0]);
    ensure!(replay_b.runtime_identity() == &first[1]);
    drop(replay_a);
    drop(replay_b);
    let mut drift = fixtures[0].resources.clone();
    drift.memory_mb = Some(2048);
    ensure!(
        entries[0]
            .boot_or_inspect_machine(&fixtures[0].vm_reservation, vec![], drift)
            .await
            .is_err()
    );
    ensure!(
        entries[0]
            .runtime()
            .linux()
            .expect("Linux runtime")
            .inspect_shared_vm_identity(fixtures[0].stack_id())
            .await?
            == Some(first[0].clone())
    );

    for fixture in &fixtures {
        require_machine_fence(&store, &first_up, fixture)?;
    }
    let (retained_a, retained_b) = tokio::join!(
        boot(&entries[0], &fixtures[0]),
        boot(&entries[1], &fixtures[1])
    );
    let retained_a = Arc::new(retained_a?);
    let retained_b = Arc::new(retained_b?);
    let retained_h = Arc::new(boot(&entries[2], &fixtures[2]).await?);
    let first_up_proof = finish_failed_up(&store, first_up, &fixtures, &first, 1, 120)?;
    let first_failed_up_owned_read_only =
        require_owned_read_only(&store, project_id.as_str(), &fixtures)?;
    drop(prepared);
    drop(entries);
    drop(registry);
    let reopened_registry = MachineRuntimeRegistry::new(registry_root)?;
    let mut locked_reopen_store_acquisition_refused = true;
    for fixture in &fixtures {
        locked_reopen_store_acquisition_refused &=
            locked_store_acquisition_refused(&reopened_registry, fixture)?;
    }
    ensure!(guest(&retained_a, "printf retained-a").await? == "retained-a");
    ensure!(guest(&retained_b, "printf retained-b").await? == "retained-b");
    ensure!(guest(&retained_h, "printf retained-h").await? == "retained-h");
    let first_stop_lease = controller.acquire(&project_id, &environment_id).await?;
    let sessions = MachineLiveSessions::default();
    let session_root = tempfile::Builder::new()
        .prefix("vz-ls-")
        .tempdir_in("/private/tmp")?;
    fs::set_permissions(session_root.path(), fs::Permissions::from_mode(0o700))?;
    let session_config = session_root.path().join("client");
    fs::DirBuilder::new().mode(0o700).create(&session_config)?;
    let session_a_path =
        MachineDockerEndpoint::socket_path_for(session_root.path(), retained_a.owner())?;
    let session_b_path =
        MachineDockerEndpoint::socket_path_for(session_root.path(), retained_b.owner())?;
    for (activation, path) in [
        (&retained_a, Some(&session_a_path)),
        (&retained_b, Some(&session_b_path)),
        (&retained_h, None),
    ] {
        let mut endpoint = match path {
            Some(path) => Some(MachineDockerEndpoint::start(Arc::clone(activation), path).await?),
            None => None,
        };
        sessions.register(&first_stop_lease, Arc::clone(activation), &mut endpoint)?;
        ensure!(endpoint.is_none());
    }
    drop(retained_a);
    drop(retained_b);
    drop(retained_h);
    let session_info_args = ["info", "--format", "{{json .}}"];
    let (session_info_a, session_info_b) = tokio::join!(
        host_docker(&session_a_path, &session_config, &session_info_args, vec![]),
        host_docker(&session_b_path, &session_config, &session_info_args, vec![]),
    );
    let (session_info_a, session_info_b) = (session_info_a?, session_info_b?);
    docker_stdout(&session_info_a)?;
    let session_b_identity: Value = serde_json::from_str(docker_stdout(&session_info_b)?)?;
    let first_stop = store.begin_environment_lifecycle(
        environment_id.as_str(),
        EnvironmentLifecycleKind::Stop,
        "req-registry-stop-1",
        "idem-registry-stop-1",
        "sha256:registry-stop-1",
        130,
    )?;
    let mut session_stop_receipts = Vec::new();
    let mut session_stop_commands =
        BTreeMap::from([("before_a", session_info_a), ("before_b", session_info_b)]);
    for (index, fixture) in fixtures.iter().enumerate() {
        let receipt = sessions
            .stop(
                &first_stop_lease,
                &store,
                &first_stop,
                fixture
                    .owner
                    .machine_id
                    .as_ref()
                    .context("session Machine ID")?,
                TIMEOUT,
            )
            .await?;
        ensure!(receipt.owner == fixture.owner && receipt.runtime_identity == first[index]);
        ensure!(receipt.outcome == StackRuntimeShutdownOutcome::Stopped);
        session_stop_receipts.push(receipt);
        if index == 0 {
            let (refused, survivor) = tokio::join!(
                host_docker(&session_a_path, &session_config, &session_info_args, vec![]),
                host_docker(&session_b_path, &session_config, &session_info_args, vec![]),
            );
            let (refused, survivor) = (refused?, survivor?);
            ensure!(refused["exit_code"].as_i64().is_some_and(|code| code != 0));
            let surviving_info: Value = serde_json::from_str(docker_stdout(&survivor)?)?;
            ensure!(surviving_info["ID"] == session_b_identity["ID"]);
            session_stop_commands.insert("stopped_a", refused);
            session_stop_commands.insert("surviving_b", survivor);
        }
    }
    ensure!(!session_a_path.exists() && !session_b_path.exists());
    let live_sessions = json!({
        "scope": "registered_original_runtime_stop_only",
        "receipts": session_stop_receipts,
        "commands": session_stop_commands,
        "sockets_removed": true,
        "restart_recovery": false,
        "public_stop": false,
    });
    session_root.close()?;
    finish_stop(&store, first_stop, 131)?;
    drop(first_stop_lease);
    let stopped_owned_read_only = require_owned_read_only(&store, project_id.as_str(), &fixtures)?;
    let serial_log_dir =
        PathBuf::from(std::env::var_os(SERIAL_LOG_DIR_ENV).context(SERIAL_LOG_DIR_ENV)?);
    let first_boot_serial_logs = preserve_first_boot_serial_logs(&serial_log_dir, &fixtures)?;
    drop(store);

    let reopened_store = StateStore::open(&store_path)?;
    let reopened_state = reopened_store
        .load_project_state(project_id.as_str())?
        .context("reopened Project state")?;
    let reopened_stopped_owned_read_only =
        require_owned_read_only(&reopened_store, project_id.as_str(), &fixtures)?;
    ensure!(!source_root.exists());
    // This recovery deliberately has no target catalog or source bundle. The
    // fixture still retains its expected owners/resources for assertions; the
    // runtime configuration itself is loaded only from persisted state + pins.
    let empty_resolver = MachineTargetResolver::new(host(), MachineTargetCatalog::default())?;
    ensure!(matches!(
        empty_resolver
            .resolve_project(&reopened_state.definition)
            .await,
        Err(TargetResolutionError::TargetNotFound { .. })
    ));
    let recovery_state_before = reopened_store.load_project_state(project_id.as_str())?;
    let reopened_prepared = controller
        .acquire(&project_id, &environment_id)
        .await?
        .prepare(
            &reopened_store,
            &reopened_registry,
            &empty_resolver,
            &reopened_state.environments[0],
            139,
        )
        .await?;
    let recovery_preparation_read_only =
        reopened_store.load_project_state(project_id.as_str())? == recovery_state_before;
    ensure!(recovery_preparation_read_only);
    let reopened_stores = prepared_stores(reopened_prepared.pins(), &fixtures)?;
    let recovered_pin_snapshot = pin_snapshot(&reopened_stores, &fixtures)?;
    ensure!(recovered_pin_snapshot == pin_snapshot_before_replay);
    let second_up = reopened_store.begin_environment_lifecycle(
        environment_id.as_str(),
        EnvironmentLifecycleKind::Up,
        "req-registry-up-2",
        "idem-registry-up-2",
        "sha256:registry-up-2",
        140,
    )?;
    for fixture in &fixtures {
        require_machine_fence(&reopened_store, &second_up, fixture)?;
    }
    let mut reopened_entries = Vec::new();
    for fixture in &fixtures {
        let entry = reopened_prepared.attach_machine(
            &reopened_store,
            &reopened_registry,
            &second_up,
            fixture
                .owner
                .machine_id
                .as_ref()
                .context("fixture Machine ID")?,
        )?;
        cleanup_targets.push(CleanupTarget {
            runtime: entry.runtime().linux().expect("Linux runtime").clone(),
            stack_id: fixture.stack_id().into(),
        });
        reopened_entries.push(entry);
    }
    let recovery_entries_controller_verified = reopened_entries.len() == fixtures.len()
        && reopened_entries
            .iter()
            .zip(&fixtures)
            .all(|(entry, fixture)| entry.owner() == &fixture.owner);
    ensure!(recovery_entries_controller_verified);
    drop(reopened_stores);
    let (second_a, second_b) = tokio::join!(
        boot(&reopened_entries[0], &fixtures[0]),
        boot(&reopened_entries[1], &fixtures[1])
    );
    let second_a = second_a?;
    let second_b = second_b?;
    let second_h = boot(&reopened_entries[2], &fixtures[2]).await?;
    let second = vec![
        second_a.runtime_identity().clone(),
        second_b.runtime_identity().clone(),
        second_h.runtime_identity().clone(),
    ];
    ensure!(second.iter().zip(&first).all(|(new, old)| new != old));
    let (ready_a, ready_b) = tokio::join!(
        ensure_docker_ready_with_diagnostics(&second_a, "developer-a reopened boot"),
        ensure_docker_ready_with_diagnostics(&second_b, "developer-b reopened boot")
    );
    ready_a?;
    ready_b?;
    let reopen_verify = |label: &'static str| {
        format!(
            "set -eu; probe=$(/vz-rootfs/vz-machine-registry-docker-probe verify --socket /run/vz-docker/docker.sock --volume vz-registry-shared --marker {label}); test \"$(cat /vz-setup-commits/shared)\" = {label}; test ! -e /vz-rootfs/registry-e2e; printf '%s' \"$probe\""
        )
    };
    let reopen_a_script = reopen_verify("developer-a");
    let reopen_b_script = reopen_verify("developer-b");
    let (persist_a, persist_b) = tokio::join!(
        guest(&second_a, &reopen_a_script),
        guest(&second_b, &reopen_b_script),
    );
    let reopen_verify_probe_a = docker_probe_evidence(&persist_a?, "verify", "developer-a")?;
    let reopen_verify_probe_b = docker_probe_evidence(&persist_b?, "verify", "developer-b")?;
    let reopened_storage = storage_evidence(&reopened_entries, &fixtures)?;
    ensure!(reopened_storage["installed_artifacts"] == first_storage["installed_artifacts"]);
    let second_up_proof = finish_failed_up(&reopened_store, second_up, &fixtures, &second, 2, 150)?;
    let second_failed_up_owned_read_only =
        require_owned_read_only(&reopened_store, project_id.as_str(), &fixtures)?;
    drop(second_a);
    drop(second_b);
    drop(second_h);
    drop(reopened_prepared);
    let second_stop_lease = controller.acquire(&project_id, &environment_id).await?;
    let second_stop = reopened_store.begin_environment_lifecycle(
        environment_id.as_str(),
        EnvironmentLifecycleKind::Stop,
        "req-registry-stop-2",
        "idem-registry-stop-2",
        "sha256:registry-stop-2",
        160,
    )?;
    for ((entry, old), new) in reopened_entries.iter().zip(&first).zip(&second) {
        ensure!(
            matches!(shutdown(entry.runtime().linux().expect("Linux runtime"), old, second_stop.operation_id.as_str()).await?, StackRuntimeShutdownOutcome::ReplacementPresent { current } if current == *new)
        );
        ensure!(
            shutdown(
                entry.runtime().linux().expect("Linux runtime"),
                new,
                second_stop.operation_id.as_str()
            )
            .await?
                == StackRuntimeShutdownOutcome::Stopped
        );
    }
    finish_stop(&reopened_store, second_stop, 161)?;
    drop(second_stop_lease);
    let second_boot_serial_logs = second_boot_serial_logs(&serial_log_dir, &fixtures)?;
    let final_stopped_owned_read_only =
        require_owned_read_only(&reopened_store, project_id.as_str(), &fixtures)?;
    ensure!(
        reopened_store
            .load_project_state(project_id.as_str())?
            .unwrap()
            .environments[0]
            .machines
            .iter()
            .all(|machine| machine.state == MachineState::Stopped)
    );

    Ok(json!({
        "schema_version": 1,
        "host_endpoint": host_endpoint,
        "live_sessions": live_sessions,
        "scope": "registry_boot_lease_and_host_endpoint_infrastructure_only",
        "build": {
            "profile": std::env::var(BUILD_PROFILE_ENV).unwrap_or_else(|_| "unknown".into()),
            "test_binary_sha256": std::env::var(TEST_SHA_ENV).unwrap_or_else(|_| "unknown".into()),
            "developer_initramfs_sha256": std::env::var(DEV_INITRAMFS_SHA_ENV).unwrap_or_else(|_| "unknown".into()),
            "container_initramfs_sha256": std::env::var(HARD_INITRAMFS_SHA_ENV).unwrap_or_else(|_| "unknown".into()),
            "docker_probe_source_sha256": std::env::var(DOCKER_PROBE_SOURCE_SHA_ENV).unwrap_or_else(|_| "unknown".into()),
            "docker_probe_go_version": std::env::var(DOCKER_PROBE_GO_VERSION_ENV).unwrap_or_else(|_| "unknown".into()),
            "docker_probe_sha256": docker_probe_sha256.clone(),
        },
        "target_resolution": {
            "all_machines_resolved_before_state": all_machines_resolved_before_state,
            "invalid_sibling_rejected_without_state": invalid_sibling_rejected_without_state,
        },
        "artifact_pinning": {
            "all_pins_before_runtime_construction": true,
            "source_bundles_removed_before_boot": true,
            "recovery_without_catalog_or_source": true,
            "pin_replay_read_only": true,
        },
        "controller": {
            "environment_scoped_serialization": environment_scoped_serialization,
            "fresh_preparation_and_attachment": entries_controller_verified,
            "stale_attachment_refused": stale_controller_attachment_refused,
            "recovery_preparation_read_only": recovery_preparation_read_only,
            "recovery_attachment_without_catalog": recovery_entries_controller_verified,
        },
        "topology": {
            "project_id": project_id, "environment_id": environment_id,
            "creating_owned_read_only": creating_owned_read_only,
            "failed_up_owned_read_only": first_failed_up_owned_read_only && second_failed_up_owned_read_only,
            "stopped_owned_read_only": stopped_owned_read_only,
            "reopened_stopped_owned_read_only": reopened_stopped_owned_read_only,
            "final_stopped_owned_read_only": final_stopped_owned_read_only,
            "attempted_activation_capabilities": ["posix_exec"],
            "docker_capabilities_synthesized": false,
            "developer_ready_without_docker_conformance_rejected": first_up_proof.developer_rejections == 2 && second_up_proof.developer_rejections == 2,
            "hardened_activation_published": first_up_proof.hardened_activation_published && second_up_proof.hardened_activation_published,
        },
        "machines": {
            "developer_a": machine_evidence(&fixtures[0], &first[0], &second[0]),
            "developer_b": machine_evidence(&fixtures[1], &first[1], &second[1]),
            "hardened": machine_evidence(&fixtures[2], &first[2], &second[2]),
        },
        "storage": {
            "first": first_storage, "reopened": reopened_storage,
            "pin_snapshots": {
                "before_replay": pin_snapshot_before_replay,
                "after_replay": pin_snapshot_after_replay,
                "recovered": recovered_pin_snapshot,
            },
            "same_named_docker_volumes_hold_distinct_values": true,
            "sibling_rootfs_and_setup_sentinels_invisible": true,
            "developer_docker_state_survived_reopen": true,
            "docker_api_probe_sha256": docker_probe_sha256,
            "docker_api_probe_outputs": {
                "developer_a_create": create_probe_a,
                "developer_b_create": create_probe_b,
                "developer_a_first_verify": first_verify_probe_a,
                "developer_b_first_verify": first_verify_probe_b,
                "developer_a_reopened_verify": reopen_verify_probe_a,
                "developer_b_reopened_verify": reopen_verify_probe_b,
            },
            "orphan_rootfs_cleanup_observed_on_reopen": true,
        },
        "lease": {
            "same_registry_admission_reused_arc": true,
            "same_boot_request_replayed_identity": true,
            "stale_generation_refused_before_boot": true,
            "resource_drift_refused_without_replacement": true,
            "activation_retained_store_lock_after_registry_drop": true,
            "locked_reopen_store_acquisition_refused": locked_reopen_store_acquisition_refused,
            "activation_exec_after_registry_drop": true,
            "cold_reopen_new_identities": true,
            "old_identities_refused_as_replacements": true,
        },
        "serial_logs": {
            "first_boot": first_boot_serial_logs,
            "second_boot": second_boot_serial_logs,
            "regular_nonempty": true,
        },
        "claims": { "production_up": false, "native_macos_machine": false, "managed_docker_context_or_full_compatibility": false },
    }))
}

fn write_evidence(value: &Value) -> Result<()> {
    let path = PathBuf::from(std::env::var_os(EVIDENCE_ENV).context(EVIDENCE_ENV)?);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

async fn physical() -> Result<Value> {
    let temporary = tempfile::Builder::new()
        .prefix("vz-machine-registry-e2e-")
        .tempdir()?;
    // macOS exposes /var through a symlink; the registry correctly refuses it.
    let root = fs::canonicalize(temporary.path())?;
    let mut cleanup_targets = Vec::new();
    let mut pin_cleanup_directories = Vec::new();
    let result = run_inner(&root, &mut cleanup_targets, &mut pin_cleanup_directories).await;
    let mut cleanup_failures = Vec::new();
    if let Err(error) = cleanup(&cleanup_targets).await {
        cleanup_failures.push(format!("VM cleanup failed: {error:#}"));
    }
    drop(cleanup_targets);
    if let Err(error) = cleanup_pin_permissions(&root, &pin_cleanup_directories) {
        cleanup_failures.push(format!("pin permission cleanup failed: {error:#}"));
    }
    if let Err(error) = temporary.close() {
        cleanup_failures.push(format!("fixture TempDir close failed: {error}"));
    }
    match (result, cleanup_failures.is_empty()) {
        (Ok(value), true) => Ok(value),
        (Err(error), true) => Err(error),
        (Ok(_), false) => Err(anyhow!(cleanup_failures.join("; "))),
        (Err(error), false) => Err(anyhow!(
            "scenario failed: {error:#}; {}",
            cleanup_failures.join("; ")
        )),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "requires signed Apple-Silicon binary and three real local VMs"]
async fn three_machine_registry_boot_lease_reopen_isolation() {
    if !entitled() {
        panic!("VZ_E2E_REQUIRED_SKIP: virtualization entitlement required");
    }
    let evidence = physical()
        .await
        .expect("physical per-Machine registry scenario");
    write_evidence(&evidence).expect("write registry evidence");
}
