//! Physical proof for private per-Machine runtime admission and leased VM boots.
//! This proves infrastructure, not production `vz up` or a host Docker endpoint.

#![cfg(all(target_os = "macos", target_arch = "aarch64"))]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use vz_oci_macos::{KernelProfile, MacosRuntimeBackend, Runtime, RuntimeConfig};
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
use vz_runtimed::machine_runtime_activation::MachineRuntimeActivation;
use vz_runtimed::machine_runtime_registry::{
    MachineRuntimeAdmission, MachineRuntimeEntry, MachineRuntimeRegistry,
    MachineRuntimeRegistryError,
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
    bundle: PathBuf,
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
        ensure!(fs::read_dir(root)?.next().transpose()?.is_none());
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
            bundle,
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

fn admit(
    registry: &MachineRuntimeRegistry<MacosRuntimeBackend>,
    fixture: &MachineFixture,
    mode: MachineRuntimeAdmission,
) -> Result<Arc<MachineRuntimeEntry<MacosRuntimeBackend>>> {
    let profile = fixture.profile;
    let bundle = fixture.bundle.clone();
    Ok(registry.admit(
        &fixture.owner,
        &fixture.store_reservation,
        &fixture.config_digest,
        mode,
        move |data| {
            Ok(MacosRuntimeBackend::new(Runtime::new(RuntimeConfig {
                data_dir: data.into(),
                linux_install_dir: Some(data.join("linux-install")),
                linux_bundle_dir: Some(bundle),
                linux_profile: Some(profile),
                require_exact_agent_version: true,
                agent_ready_timeout: Duration::from_secs(35),
                exec_timeout: Duration::from_secs(30),
                default_memory_mb: if profile == KernelProfile::Developer {
                    4096
                } else {
                    1024
                },
                ..RuntimeConfig::default()
            })))
        },
    )?)
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
    ensure!(activation.verified_profile() == fixture.profile);
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

fn leased_without_factory(
    registry: &MachineRuntimeRegistry<MacosRuntimeBackend>,
    fixture: &MachineFixture,
) -> Result<()> {
    let called = AtomicBool::new(false);
    let result = registry.admit(
        &fixture.owner,
        &fixture.store_reservation,
        &fixture.config_digest,
        MachineRuntimeAdmission::ExistingOnly,
        |_| {
            called.store(true, Ordering::SeqCst);
            Err(MachineRuntimeRegistryError::Invalid("must not run".into()))
        },
    );
    ensure!(matches!(
        result,
        Err(MachineRuntimeRegistryError::Leased(_))
    ));
    ensure!(!called.load(Ordering::SeqCst));
    Ok(())
}

fn installed(entry: &MachineRuntimeEntry<MacosRuntimeBackend>) -> Result<Value> {
    let dir = fs::canonicalize(entry.data_path().join("linux-install"))?;
    let version: Value = serde_json::from_slice(&fs::read(dir.join("version.json"))?)?;
    Ok(json!({
        "dir": dir,
        "profile": version["profile"],
        "kernel_sha256": file_sha(&dir.join("vmlinux"))?,
        "initramfs_sha256": file_sha(&dir.join("initramfs.img"))?,
        "youki_sha256": file_sha(&dir.join("youki"))?,
        "version_sha256": file_sha(&dir.join("version.json"))?,
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
    let rootfs = entry.runtime().inner().rootfs_store_dir();
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
            entry.runtime().inner().rootfs_store_dir(),
            entry.runtime().inner().setup_commits_host_dir(),
            entry.data_path().join("linux-install"),
        ] {
            ensure!(fs::canonicalize(child)?.starts_with(&root));
        }
        roots.push(root);
        installs.insert(fixture.name, installed(entry)?);
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

async fn run_inner(root: &Path, cleanup_targets: &mut Vec<CleanupTarget>) -> Result<Value> {
    let developer_bundle = fs::canonicalize(PathBuf::from(
        std::env::var_os(DEV_BUNDLE_ENV).context(DEV_BUNDLE_ENV)?,
    ))?;
    let hardened_bundle = fs::canonicalize(PathBuf::from(
        std::env::var_os(HARD_BUNDLE_ENV).context(HARD_BUNDLE_ENV)?,
    ))?;
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
        HostSpec {
            os: OperatingSystem::Macos,
            arch: Architecture::Aarch64,
        },
        target_catalog(
            developer_bundle,
            developer_verified.artifact_identity.digest,
            hardened_bundle,
            hardened_verified.artifact_identity.digest,
        ),
    )?;
    let invalid_sibling_rejected_without_state =
        invalid_sibling_preflight(root, &resolver, &definition).await?;
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
    for (offset, record) in all_reservations(&fixtures).into_iter().enumerate() {
        store.reserve_owned_resource(record, 101 + offset as u64)?;
    }
    let creating_owned_read_only = require_owned_read_only(&store, project_id.as_str(), &fixtures)?;
    let registry_root = root.join("registry");
    fs::DirBuilder::new().mode(0o700).create(&registry_root)?;
    let registry = MachineRuntimeRegistry::new(registry_root.clone())?;
    let mut entries = Vec::new();
    for fixture in &fixtures {
        let entry = admit(&registry, fixture, MachineRuntimeAdmission::CreateOrOpen)?;
        cleanup_targets.push(CleanupTarget {
            runtime: entry.runtime().inner().clone(),
            stack_id: fixture.stack_id().into(),
        });
        let replay = admit(&registry, fixture, MachineRuntimeAdmission::CreateOrOpen)?;
        ensure!(Arc::ptr_eq(&entry, &replay));
        entries.push(entry);
    }
    let docker_probe = fs::canonicalize(PathBuf::from(
        std::env::var_os(DOCKER_PROBE_ENV).context(DOCKER_PROBE_ENV)?,
    ))?;
    let docker_probe_sha256 = file_sha(&docker_probe)?;
    let probe_a_path = install_docker_probe(&entries[0], &docker_probe)?;
    let probe_b_path = install_docker_probe(&entries[1], &docker_probe)?;
    ensure!(file_sha(&probe_a_path)? == docker_probe_sha256);
    ensure!(file_sha(&probe_b_path)? == docker_probe_sha256);
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

    // Both Developer Machines boot concurrently and remain live together.
    let (dev_a, dev_b) = tokio::join!(
        boot(&entries[0], &fixtures[0]),
        boot(&entries[1], &fixtures[1])
    );
    let dev_a = dev_a?;
    let dev_b = dev_b?;
    let hard = boot(&entries[2], &fixtures[2]).await?;
    let first = vec![
        dev_a.runtime_identity().clone(),
        dev_b.runtime_identity().clone(),
        hard.runtime_identity().clone(),
    ];
    let (ready_a, ready_b) = tokio::join!(dev_a.ensure_docker_ready(), dev_b.ensure_docker_ready());
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
            .inner()
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
    let retained_a = retained_a?;
    let retained_b = retained_b?;
    let retained_h = boot(&entries[2], &fixtures[2]).await?;
    drop(entries);
    drop(registry);
    let reopened_registry = MachineRuntimeRegistry::new(registry_root)?;
    for fixture in &fixtures {
        leased_without_factory(&reopened_registry, fixture)?;
    }
    ensure!(guest(&retained_a, "printf retained-a").await? == "retained-a");
    ensure!(guest(&retained_b, "printf retained-b").await? == "retained-b");
    ensure!(guest(&retained_h, "printf retained-h").await? == "retained-h");
    let first_up_proof = finish_failed_up(&store, first_up, &fixtures, &first, 1, 120)?;
    let first_failed_up_owned_read_only =
        require_owned_read_only(&store, project_id.as_str(), &fixtures)?;
    drop(retained_a);
    drop(retained_b);
    drop(retained_h);

    let first_stop = store.begin_environment_lifecycle(
        environment_id.as_str(),
        EnvironmentLifecycleKind::Stop,
        "req-registry-stop-1",
        "idem-registry-stop-1",
        "sha256:registry-stop-1",
        130,
    )?;
    for (target, identity) in cleanup_targets.iter().take(3).zip(&first) {
        ensure!(
            shutdown(&target.runtime, identity, first_stop.operation_id.as_str()).await?
                == StackRuntimeShutdownOutcome::Stopped
        );
    }
    finish_stop(&store, first_stop, 131)?;
    let stopped_owned_read_only = require_owned_read_only(&store, project_id.as_str(), &fixtures)?;
    let serial_log_dir =
        PathBuf::from(std::env::var_os(SERIAL_LOG_DIR_ENV).context(SERIAL_LOG_DIR_ENV)?);
    let first_boot_serial_logs = preserve_first_boot_serial_logs(&serial_log_dir, &fixtures)?;
    drop(store);

    let reopened_store = StateStore::open(&store_path)?;
    let reopened_stopped_owned_read_only =
        require_owned_read_only(&reopened_store, project_id.as_str(), &fixtures)?;
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
        let entry = admit(
            &reopened_registry,
            fixture,
            MachineRuntimeAdmission::ExistingOnly,
        )?;
        cleanup_targets.push(CleanupTarget {
            runtime: entry.runtime().inner().clone(),
            stack_id: fixture.stack_id().into(),
        });
        reopened_entries.push(entry);
    }
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
        second_a.ensure_docker_ready(),
        second_b.ensure_docker_ready()
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
            matches!(shutdown(entry.runtime().inner(), old, second_stop.operation_id.as_str()).await?, StackRuntimeShutdownOutcome::ReplacementPresent { current } if current == *new)
        );
        ensure!(
            shutdown(
                entry.runtime().inner(),
                new,
                second_stop.operation_id.as_str()
            )
            .await?
                == StackRuntimeShutdownOutcome::Stopped
        );
    }
    finish_stop(&reopened_store, second_stop, 161)?;
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
        "scope": "registry_and_boot_lease_infrastructure_only",
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
            "locked_reopen_factory_invocations": 0,
            "activation_exec_after_registry_drop": true,
            "cold_reopen_new_identities": true,
            "old_identities_refused_as_replacements": true,
        },
        "serial_logs": {
            "first_boot": first_boot_serial_logs,
            "second_boot": second_boot_serial_logs,
            "regular_nonempty": true,
        },
        "claims": { "production_up": false, "native_macos_machine": false, "host_docker_socket_or_context": false },
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
    let result = run_inner(&root, &mut cleanup_targets).await;
    let cleanup_result = cleanup(&cleanup_targets).await;
    match (result, cleanup_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(error), Err(cleanup)) => Err(anyhow!(
            "scenario failed: {error:#}; cleanup failed: {cleanup:#}"
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
