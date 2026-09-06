//! Build pipeline execution against the guest BuildKit daemon.
//!
//! Ordering invariants:
//! - Streamed solve/output events are forwarded in receive order.
//! - `buildctl` raw-json decode callbacks are emitted before terminal status handling.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsStr;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine as _;
use docker_credential::{CredentialRetrievalError, DockerCredential, get_credential};
use oci_distribution::Reference;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tracing::warn;
use vz::NetworkConfig;
use vz::SharedDirConfig;
use vz::protocol::{ExecEvent, ExecOutput};
use vz_image::ImageStore;
use vz_linux::{KernelCapability, KernelPaths, KernelProfile, LinuxVm, LinuxVmConfig};

use crate::RuntimeConfig;
use crate::buildkit_rawjson::BuildkitRawJsonStreamDecoder;
use crate::config::ensure_kernel_for_config;

use super::artifacts::{ensure_buildkit_artifacts, import_oci_tar_to_store};
use super::common::{
    canonicalize_existing_dir, default_buildkit_dir, expand_home_dir, resolve_dockerfile_path,
    unique_dir,
};
use super::{
    BUILD_OUTPUT_ARCHIVE, BUILDKIT_AUTH_GUEST_CONFIG, BUILDKIT_AUTH_GUEST_DIR, BUILDKIT_AUTH_TAG,
    BUILDKIT_BUILD_TIMEOUT, BUILDKIT_CACHE_KEEP_BYTES, BUILDKIT_CACHE_KEEP_DURATION,
    BUILDKIT_OCI_RUNTIME_SHIM_GUEST_PATH, BUILDKIT_RUNTIME_EXEC_EVIDENCE_GUEST_PATH,
    BUILDKIT_SETUP_TIMEOUT, BUILDKIT_SHUTDOWN_TIMEOUT, BUILDKIT_SNAPSHOTTER, BUILDKIT_VM_MEMORY_MB,
    BUILDKITD_ADDR, BuildEvent, BuildLogStream, BuildOutput, BuildProgress, BuildRequest,
    BuildResult, BuildkitError, BuildkitRuntimeInventory, CachePruneOptions,
};

const BUILDKIT_VM_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const BUILDKIT_VM_RETRY_DELAY: Duration = Duration::from_millis(100);
const BUILDKIT_SHARED_OUTPUT_TAG: &str = "build-output";
const BUILDKIT_SHARED_CONTEXT_TAG: &str = "build-context";

static BUILDKIT_VM_MANAGER: OnceLock<Arc<BuildkitVmManager>> = OnceLock::new();
static VIRTUALIZATION_ENTITLEMENT_PREFLIGHT: OnceLock<Result<(), String>> = OnceLock::new();

#[derive(Debug, Serialize)]
struct DockerConfigFile {
    auths: BTreeMap<String, DockerConfigAuth>,
}

#[derive(Debug, Clone, Serialize)]
struct DockerConfigAuth {
    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    identitytoken: Option<String>,
}

#[derive(Debug, Clone)]
struct BuildkitSharedMounts {
    output_root: PathBuf,
    auth_dir: PathBuf,
}

#[derive(Debug, Clone)]
struct BuildOutputArtifact {
    host_tar_path: PathBuf,
    guest_tar_path: String,
    cleanup_dir: PathBuf,
}

#[derive(Debug)]
struct ManagedBuildkitVm {
    vm: Arc<LinuxVm>,
    _runtime_dir: tempfile::TempDir,
    config: RuntimeConfig,
    context_dir: Option<PathBuf>,
    output_root: PathBuf,
    auth_dir: PathBuf,
}

#[derive(Debug)]
struct BuildkitVmState {
    managed: Option<ManagedBuildkitVm>,
    active_leases: usize,
    activity_generation: u64,
    last_activity: Instant,
    idle_timeout: Duration,
    boot_in_progress: bool,
    transition_failure: Option<String>,
}

impl Default for BuildkitVmState {
    fn default() -> Self {
        Self {
            managed: None,
            active_leases: 0,
            activity_generation: 0,
            last_activity: Instant::now(),
            idle_timeout: buildkit_vm_idle_timeout(),
            boot_in_progress: false,
            transition_failure: None,
        }
    }
}

#[derive(Debug)]
struct BuildkitVmManager {
    state: Mutex<BuildkitVmState>,
}

#[derive(Clone)]
struct BuildkitVmLease {
    manager: Arc<BuildkitVmManager>,
    vm: Arc<LinuxVm>,
}

impl BuildkitVmLease {
    fn vm(&self) -> &LinuxVm {
        self.vm.as_ref()
    }
}

impl Drop for BuildkitVmLease {
    fn drop(&mut self) {
        BuildkitVmManager::release_arc(&self.manager);
    }
}

enum BuildkitVmAcquireAction {
    Reuse(Arc<LinuxVm>),
    Boot,
    Replace(Box<ManagedBuildkitVm>),
    Wait,
}

#[derive(Debug)]
struct StartedBuildkitVm {
    vm: LinuxVm,
    runtime_dir: tempfile::TempDir,
}

struct StagedRuntimeGuard {
    runtime_dir: Option<tempfile::TempDir>,
    preserve_on_drop: bool,
}

impl StagedRuntimeGuard {
    fn new(runtime_dir: tempfile::TempDir) -> Self {
        Self {
            runtime_dir: Some(runtime_dir),
            preserve_on_drop: false,
        }
    }

    fn path(&self) -> Result<&Path, BuildkitError> {
        self.runtime_dir
            .as_ref()
            .map(tempfile::TempDir::path)
            .ok_or_else(|| {
                BuildkitError::InvalidConfig(
                    "staged runtime guard lost ownership of its directory".to_string(),
                )
            })
    }

    fn preserve_on_drop(&mut self) {
        self.preserve_on_drop = true;
    }

    fn cleanup_on_drop(&mut self) {
        self.preserve_on_drop = false;
    }

    fn into_runtime_dir(mut self) -> Result<tempfile::TempDir, BuildkitError> {
        self.runtime_dir.take().ok_or_else(|| {
            BuildkitError::InvalidConfig(
                "staged runtime guard lost ownership of its directory".to_string(),
            )
        })
    }
}

impl Drop for StagedRuntimeGuard {
    fn drop(&mut self) {
        if self.preserve_on_drop
            && let Some(runtime_dir) = self.runtime_dir.take()
        {
            let _ = runtime_dir.keep();
        }
    }
}

struct BuildkitVmTransitionGuard {
    manager: Arc<BuildkitVmManager>,
    managed: Option<ManagedBuildkitVm>,
    armed: bool,
}

impl BuildkitVmTransitionGuard {
    fn new(manager: Arc<BuildkitVmManager>, managed: Option<ManagedBuildkitVm>) -> Self {
        Self {
            manager,
            managed,
            armed: true,
        }
    }

    fn complete(mut self) {
        self.armed = false;
    }
}

impl Drop for BuildkitVmTransitionGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(mut state) = self.manager.state.lock() else {
            return;
        };
        state.boot_in_progress = false;
        if state.managed.is_none() {
            state.managed = self.managed.take();
        }
        state.transition_failure = Some(
            "a BuildKit VM transition was interrupted; restart this process before retrying"
                .to_string(),
        );
    }
}

impl BuildkitVmManager {
    fn new() -> Self {
        Self {
            state: Mutex::new(BuildkitVmState::default()),
        }
    }

    async fn acquire(
        self: &Arc<Self>,
        config: &RuntimeConfig,
        context_dir: Option<&Path>,
        shared_mounts: &BuildkitSharedMounts,
    ) -> Result<BuildkitVmLease, BuildkitError> {
        let normalized_config = normalize_buildkit_config(config)?;
        let config = &normalized_config;
        let requested_context = context_dir.map(Path::to_path_buf);

        loop {
            let action = {
                let mut state = self.lock_state()?;
                if let Some(failure) = &state.transition_failure {
                    return Err(BuildkitError::InvalidConfig(failure.clone()));
                }
                if let Some((existing_vm, compatible)) = state.managed.as_ref().map(|managed| {
                    let compatible = managed.config == *config
                        && managed.output_root == shared_mounts.output_root
                        && managed.auth_dir == shared_mounts.auth_dir
                        && context_mount_compatible(managed.context_dir.as_deref(), context_dir);
                    (Arc::clone(&managed.vm), compatible)
                }) {
                    if compatible {
                        state.active_leases = state.active_leases.saturating_add(1);
                        state.activity_generation = state.activity_generation.saturating_add(1);
                        state.last_activity = Instant::now();
                        BuildkitVmAcquireAction::Reuse(existing_vm)
                    } else if state.active_leases == 0 && !state.boot_in_progress {
                        state.boot_in_progress = true;
                        let existing = state.managed.take().ok_or_else(|| {
                            BuildkitError::InvalidConfig(
                                "BuildKit VM manager lost the VM selected for replacement"
                                    .to_string(),
                            )
                        })?;
                        BuildkitVmAcquireAction::Replace(Box::new(existing))
                    } else {
                        BuildkitVmAcquireAction::Wait
                    }
                } else if state.boot_in_progress {
                    BuildkitVmAcquireAction::Wait
                } else {
                    state.boot_in_progress = true;
                    BuildkitVmAcquireAction::Boot
                }
            };

            match action {
                BuildkitVmAcquireAction::Reuse(vm) => {
                    return Ok(BuildkitVmLease {
                        manager: Arc::clone(self),
                        vm,
                    });
                }
                BuildkitVmAcquireAction::Wait => {
                    tokio::time::sleep(BUILDKIT_VM_RETRY_DELAY).await;
                }
                BuildkitVmAcquireAction::Replace(old) => {
                    let mut transition =
                        BuildkitVmTransitionGuard::new(Arc::clone(self), Some(*old));
                    let old = transition.managed.as_ref().ok_or_else(|| {
                        BuildkitError::InvalidConfig(
                            "BuildKit VM replacement lost ownership of the existing VM".to_string(),
                        )
                    })?;
                    if let Err(error) = shutdown_managed_vm(old.vm.as_ref()).await {
                        let mut state = self.lock_state()?;
                        state.boot_in_progress = false;
                        state.transition_failure = Some(format!(
                            "the previous BuildKit VM could not be stopped safely: {error}"
                        ));
                        state.managed = transition.managed.take();
                        transition.complete();
                        return Err(error);
                    }
                    transition.managed = None;
                    let started = match start_buildkit_vm(
                        config,
                        context_dir,
                        &shared_mounts.output_root,
                        &shared_mounts.auth_dir,
                    )
                    .await
                    {
                        Ok(started) => started,
                        Err(error) => {
                            let mut state = self.lock_state()?;
                            state.boot_in_progress = false;
                            transition.complete();
                            return Err(error);
                        }
                    };
                    let vm = Arc::new(started.vm);
                    let mut state = self.lock_state()?;
                    state.boot_in_progress = false;
                    state.managed = Some(ManagedBuildkitVm {
                        vm: Arc::clone(&vm),
                        _runtime_dir: started.runtime_dir,
                        config: config.clone(),
                        context_dir: requested_context.clone(),
                        output_root: shared_mounts.output_root.clone(),
                        auth_dir: shared_mounts.auth_dir.clone(),
                    });
                    state.active_leases = 1;
                    state.activity_generation = state.activity_generation.saturating_add(1);
                    state.last_activity = Instant::now();
                    transition.complete();
                    return Ok(BuildkitVmLease {
                        manager: Arc::clone(self),
                        vm,
                    });
                }
                BuildkitVmAcquireAction::Boot => {
                    let transition = BuildkitVmTransitionGuard::new(Arc::clone(self), None);
                    let started = match start_buildkit_vm(
                        config,
                        context_dir,
                        &shared_mounts.output_root,
                        &shared_mounts.auth_dir,
                    )
                    .await
                    {
                        Ok(started) => started,
                        Err(error) => {
                            let mut state = self.lock_state()?;
                            state.boot_in_progress = false;
                            transition.complete();
                            return Err(error);
                        }
                    };
                    let vm = Arc::new(started.vm);
                    let mut state = self.lock_state()?;
                    state.boot_in_progress = false;
                    state.managed = Some(ManagedBuildkitVm {
                        vm: Arc::clone(&vm),
                        _runtime_dir: started.runtime_dir,
                        config: config.clone(),
                        context_dir: requested_context.clone(),
                        output_root: shared_mounts.output_root.clone(),
                        auth_dir: shared_mounts.auth_dir.clone(),
                    });
                    state.active_leases = 1;
                    state.activity_generation = state.activity_generation.saturating_add(1);
                    state.last_activity = Instant::now();
                    transition.complete();
                    return Ok(BuildkitVmLease {
                        manager: Arc::clone(self),
                        vm,
                    });
                }
            }
        }
    }

    fn release_arc(manager: &Arc<Self>) {
        let (generation, idle_timeout) = {
            let mut state = match manager.lock_state() {
                Ok(state) => state,
                Err(error) => {
                    warn!(%error, "failed to acquire BuildKit VM manager lock during release");
                    return;
                }
            };
            state.active_leases = state.active_leases.saturating_sub(1);
            state.last_activity = Instant::now();
            state.activity_generation = state.activity_generation.saturating_add(1);
            (state.activity_generation, state.idle_timeout)
        };

        let manager = Arc::downgrade(manager);
        thread::spawn(move || {
            thread::sleep(idle_timeout);
            if let Some(manager) = manager.upgrade() {
                manager.try_idle_shutdown(generation);
            }
        });
    }

    async fn shutdown_now(self: &Arc<Self>) -> Result<(), BuildkitError> {
        let managed = {
            let mut state = self.lock_state()?;
            if let Some(failure) = &state.transition_failure {
                return Err(BuildkitError::InvalidConfig(failure.clone()));
            }
            if state.active_leases != 0 || state.boot_in_progress {
                return Err(BuildkitError::InvalidConfig(
                    "BuildKit VM cannot shut down while an operation is active".to_string(),
                ));
            }
            state.activity_generation = state.activity_generation.saturating_add(1);
            state.boot_in_progress = true;
            match state.managed.take() {
                Some(managed) => managed,
                None => {
                    state.boot_in_progress = false;
                    return Ok(());
                }
            }
        };

        let mut transition = BuildkitVmTransitionGuard::new(Arc::clone(self), Some(managed));
        let managed = transition.managed.as_ref().ok_or_else(|| {
            BuildkitError::InvalidConfig(
                "BuildKit shutdown lost ownership of the managed VM".to_string(),
            )
        })?;
        if let Err(error) = shutdown_managed_vm(managed.vm.as_ref()).await {
            let mut state = self.lock_state()?;
            state.boot_in_progress = false;
            state.transition_failure = Some(format!(
                "the BuildKit VM could not be stopped safely: {error}"
            ));
            state.managed = transition.managed.take();
            transition.complete();
            return Err(error);
        }

        transition.managed = None;
        let mut state = self.lock_state()?;
        state.boot_in_progress = false;
        transition.complete();
        Ok(())
    }

    fn try_idle_shutdown(&self, generation: u64) {
        let managed_to_shutdown = {
            let mut state = match self.lock_state() {
                Ok(state) => state,
                Err(error) => {
                    warn!(%error, "failed to acquire BuildKit VM manager lock during idle check");
                    return;
                }
            };
            if state.active_leases != 0 {
                return;
            }
            if state.activity_generation != generation {
                return;
            }
            if state.last_activity.elapsed() < state.idle_timeout {
                return;
            }
            let managed = state.managed.take();
            if managed.is_some() {
                state.boot_in_progress = true;
            }
            managed
        };

        if let Some(managed) = managed_to_shutdown {
            match block_on_managed_vm_shutdown(&managed) {
                Ok(()) => {
                    if let Ok(mut state) = self.lock_state() {
                        state.boot_in_progress = false;
                    }
                }
                Err(error) => {
                    warn!(%error, "failed to shutdown idle BuildKit VM");
                    if let Ok(mut state) = self.lock_state() {
                        state.boot_in_progress = false;
                        state.transition_failure = Some(format!(
                            "the idle BuildKit VM could not be stopped safely: {error}"
                        ));
                        if state.managed.is_none() {
                            state.managed = Some(managed);
                        }
                    }
                }
            }
        }
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, BuildkitVmState>, BuildkitError> {
        self.state.lock().map_err(|_| {
            BuildkitError::InvalidConfig("BuildKit VM manager lock poisoned".to_string())
        })
    }
}

fn buildkit_vm_manager() -> Arc<BuildkitVmManager> {
    Arc::clone(BUILDKIT_VM_MANAGER.get_or_init(|| Arc::new(BuildkitVmManager::new())))
}

/// Stop the shared BuildKit VM after active operations complete.
pub async fn shutdown_buildkit_vm() -> Result<(), BuildkitError> {
    buildkit_vm_manager().shutdown_now().await
}

fn context_mount_compatible(existing: Option<&Path>, requested: Option<&Path>) -> bool {
    match (existing, requested) {
        (_, None) => true,
        (Some(existing), Some(requested)) => existing == requested,
        (None, Some(_)) => false,
    }
}

fn buildkit_vm_idle_timeout() -> Duration {
    let value = std::env::var("VZ_BUILDKIT_VM_IDLE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());
    match value {
        Some(0) | None => BUILDKIT_VM_IDLE_TIMEOUT,
        Some(seconds) => Duration::from_secs(seconds),
    }
}

fn ensure_virtualization_entitlement_preflight() -> Result<(), BuildkitError> {
    let result = VIRTUALIZATION_ENTITLEMENT_PREFLIGHT.get_or_init(|| {
        let executable = std::env::current_exe().map_err(|error| {
            format!("failed to resolve current executable for preflight: {error}")
        })?;
        let output = Command::new("codesign")
            .arg("-d")
            .arg("--entitlements")
            .arg(":-")
            .arg(&executable)
            .output()
            .map_err(|error| {
                format!(
                    "failed to run `codesign --entitlements` for {}: {error}",
                    executable.display()
                )
            })?;
        let entitlements = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if !output.status.success() {
            return Err(format!(
                "virtualization entitlement preflight failed for {} (codesign exit: {})\n{}",
                executable.display(),
                output.status,
                entitlement_remediation_message()
            ));
        }
        if !entitlements.contains("com.apple.security.virtualization") {
            return Err(format!(
                "missing `com.apple.security.virtualization` entitlement for {}\n{}",
                executable.display(),
                entitlement_remediation_message()
            ));
        }
        Ok(())
    });

    match result {
        Ok(()) => Ok(()),
        Err(message) => Err(BuildkitError::InvalidConfig(message.clone())),
    }
}

fn map_vm_boot_error(error: BuildkitError) -> BuildkitError {
    let message = error.to_string().to_ascii_lowercase();
    if is_virtualization_entitlement_error(&message) {
        BuildkitError::InvalidConfig(format!(
            "BuildKit VM startup failed due to virtualization entitlement state.\n{}",
            entitlement_remediation_message()
        ))
    } else {
        error
    }
}

fn is_virtualization_entitlement_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("vzerrordomain:2")
        || normalized.contains("com.apple.security.virtualization")
        || normalized.contains("virtualization entitlement")
}

fn entitlement_remediation_message() -> String {
    "Remediation: re-sign binaries with `./scripts/sign-dev.sh --profile debug` \
and retry (or use `vz vm self-sign`)."
        .to_string()
}

fn block_on_managed_vm_shutdown(managed: &ManagedBuildkitVm) -> Result<(), BuildkitError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(BuildkitError::Io)?;
    runtime.block_on(async { shutdown_managed_vm(managed.vm.as_ref()).await })
}

async fn shutdown_managed_vm(vm: &LinuxVm) -> Result<(), BuildkitError> {
    if let Err(error) = shutdown_guest_buildkitd(vm).await {
        warn!(%error, "failed to stop buildkitd in guest before VM shutdown");
    }
    vm.stop().await?;
    Ok(())
}

async fn prepare_shared_mounts() -> Result<BuildkitSharedMounts, BuildkitError> {
    let runtime_dir = default_buildkit_dir()?.join("runtime");
    let output_root = runtime_dir.join("output");
    let auth_dir = runtime_dir.join("auth");
    tokio::fs::create_dir_all(&output_root).await?;
    tokio::fs::create_dir_all(&auth_dir).await?;
    Ok(BuildkitSharedMounts {
        output_root,
        auth_dir,
    })
}

async fn prepare_output_artifact(
    output_mode: &BuildOutput,
    shared_mounts: &BuildkitSharedMounts,
) -> Result<Option<BuildOutputArtifact>, BuildkitError> {
    if matches!(output_mode, BuildOutput::RegistryPush) {
        return Ok(None);
    }

    let output_dir = unique_dir(shared_mounts.output_root.clone(), "build-output");
    tokio::fs::create_dir_all(&output_dir).await?;
    let dir_name = output_dir
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .ok_or_else(|| {
            BuildkitError::InvalidConfig(format!(
                "invalid output directory: {}",
                output_dir.display()
            ))
        })?;
    let host_tar_path = output_dir.join(BUILD_OUTPUT_ARCHIVE);
    let guest_tar_path =
        format!("/mnt/{BUILDKIT_SHARED_OUTPUT_TAG}/{dir_name}/{BUILD_OUTPUT_ARCHIVE}");
    Ok(Some(BuildOutputArtifact {
        host_tar_path,
        guest_tar_path,
        cleanup_dir: output_dir,
    }))
}

async fn finish_managed_buildkit_operation<T>(
    result: Result<T, BuildkitError>,
) -> Result<T, BuildkitError> {
    match (result, shutdown_buildkit_vm().await) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(shutdown_error)) => Err(shutdown_error),
        (Err(operation_error), Ok(())) => Err(operation_error),
        (Err(operation_error), Err(shutdown_error)) => {
            warn!(%shutdown_error, "failed to shutdown BuildKit VM after operation error");
            Err(operation_error)
        }
    }
}

/// Build a Dockerfile and handle the requested output mode.
pub async fn build_image(
    config: &RuntimeConfig,
    request: BuildRequest,
) -> Result<BuildResult, BuildkitError> {
    build_image_with_events(config, request, |_event| {}).await
}

/// Build a Dockerfile and stream lifecycle/output events as they happen.
pub async fn build_image_with_events<F>(
    config: &RuntimeConfig,
    request: BuildRequest,
    mut on_event: F,
) -> Result<BuildResult, BuildkitError>
where
    F: FnMut(BuildEvent),
{
    let context_dir = canonicalize_existing_dir(&request.context_dir)?;
    if request.tag.trim().is_empty() {
        return Err(BuildkitError::InvalidConfig(
            "image tag must not be empty".to_string(),
        ));
    }

    let dockerfile_host = resolve_dockerfile_path(&context_dir, &request.dockerfile)?;
    let dockerfile_relative = dockerfile_host.strip_prefix(&context_dir).map_err(|_| {
        BuildkitError::InvalidConfig(format!(
            "Dockerfile must be inside build context: {}",
            dockerfile_host.display()
        ))
    })?;

    let shared_mounts = prepare_shared_mounts().await?;
    let output_mode = request.output.clone();
    let output_artifact = prepare_output_artifact(&output_mode, &shared_mounts).await?;
    let dockerfile_text = tokio::fs::read_to_string(&dockerfile_host).await?;
    let using_auth =
        prepare_buildkit_auth_dir(&shared_mounts.auth_dir, config, &dockerfile_text, &request)
            .await?;
    if using_auth {
        on_event(BuildEvent::Status {
            message: "Using registry credentials for BuildKit".to_string(),
        });
    }

    let result = async {
        on_event(BuildEvent::Status {
            message: "Ensuring BuildKit VM is ready".to_string(),
        });
        let vm = buildkit_vm_manager()
            .acquire(config, Some(&context_dir), &shared_mounts)
            .await?;
        on_event(BuildEvent::Status {
            message: "Running BuildKit solve".to_string(),
        });
        run_guest_build(
            vm.vm(),
            &request,
            dockerfile_relative,
            "/mnt/build-context",
            output_artifact
                .as_ref()
                .map(|artifact| artifact.guest_tar_path.as_str()),
            &mut on_event,
        )
        .await?;

        let final_result = match output_mode {
            BuildOutput::VzStore => {
                on_event(BuildEvent::Status {
                    message: "Importing OCI archive into local store".to_string(),
                });
                let image_tar = output_artifact
                    .as_ref()
                    .map(|artifact| artifact.host_tar_path.clone())
                    .ok_or_else(|| {
                        BuildkitError::InvalidConfig("missing output artifact".to_string())
                    })?;
                if !image_tar.is_file() {
                    return Err(BuildkitError::InvalidOciLayout(format!(
                        "build output archive not found: {}",
                        image_tar.display()
                    )));
                }

                let data_dir = expand_home_dir(&config.data_dir);
                let store = ImageStore::new(data_dir);
                let image_id = import_oci_tar_to_store(&store, &image_tar, &request.tag).await?;

                BuildResult {
                    image_id: Some(image_id),
                    tag: request.tag,
                    output_path: None,
                    pushed: false,
                }
            }
            BuildOutput::OciTar { dest } => {
                on_event(BuildEvent::Status {
                    message: "Writing OCI archive output".to_string(),
                });
                let image_tar = output_artifact
                    .as_ref()
                    .map(|artifact| artifact.host_tar_path.clone())
                    .ok_or_else(|| {
                        BuildkitError::InvalidConfig("missing output artifact".to_string())
                    })?;
                if !image_tar.is_file() {
                    return Err(BuildkitError::InvalidOciLayout(format!(
                        "build output archive not found: {}",
                        image_tar.display()
                    )));
                }

                let destination = expand_home_dir(&dest);
                if let Some(parent) = destination.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::copy(&image_tar, &destination).await?;

                BuildResult {
                    image_id: None,
                    tag: request.tag,
                    output_path: Some(destination),
                    pushed: false,
                }
            }
            BuildOutput::RegistryPush => BuildResult {
                image_id: None,
                tag: request.tag,
                output_path: None,
                pushed: true,
            },
        };

        Ok(final_result)
    }
    .await;

    if let Some(output_artifact) = &output_artifact {
        cleanup_temp_dir(&output_artifact.cleanup_dir, "BuildKit output").await;
    }

    result
}

/// Return a human-readable BuildKit cache usage table (from `buildctl du`).
pub async fn cache_disk_usage(config: &RuntimeConfig) -> Result<String, BuildkitError> {
    let result = cache_disk_usage_inner(config).await;
    finish_managed_buildkit_operation(result).await
}

async fn cache_disk_usage_inner(config: &RuntimeConfig) -> Result<String, BuildkitError> {
    let shared_mounts = prepare_shared_mounts().await?;
    let vm = buildkit_vm_manager()
        .acquire(config, None, &shared_mounts)
        .await?;
    ensure_guest_buildkit_ready(vm.vm()).await?;
    let output = run_buildctl(
        vm.vm(),
        vec!["du".to_string(), "--verbose".to_string()],
        BUILDKIT_BUILD_TIMEOUT,
        None,
        false,
    )
    .await?;

    if output.exit_code != 0 {
        return Err(BuildkitError::BuildFailed {
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        });
    }

    Ok(render_command_output(output))
}

/// Inspect the OCI runtime wiring inside the retained managed BuildKit VM.
///
/// The command fails closed unless the configured worker binary, multicall
/// target, runtime executable, daemon argv, and cgroup v2 mount all resolve to
/// the expected guest paths.
pub async fn buildkit_runtime_inventory(
    config: &RuntimeConfig,
) -> Result<BuildkitRuntimeInventory, BuildkitError> {
    let shared_mounts = prepare_shared_mounts().await?;
    let vm = buildkit_vm_manager()
        .acquire(config, None, &shared_mounts)
        .await?;
    ensure_guest_buildkit_ready(vm.vm()).await?;

    let inventory_script = format!(
        r#"
set -eu

configured='  binary = "{BUILDKIT_OCI_RUNTIME_SHIM_GUEST_PATH}"'
/bin/busybox grep -Fqx "$configured" /etc/buildkit/buildkitd.toml
configured_enabled='  enabled = true'
/bin/busybox grep -Fqx "$configured_enabled" /etc/buildkit/buildkitd.toml

shim_target=$(/bin/busybox readlink {BUILDKIT_OCI_RUNTIME_SHIM_GUEST_PATH})
if [ "$shim_target" != "/usr/bin/vz-guest-agent" ]; then
  echo "unexpected BuildKit OCI runtime shim target: $shim_target" >&2
  exit 1
fi

scratch="/tmp/vz-buildkit-runtime-inventory.$$"
/bin/busybox rm -rf "$scratch"
/bin/busybox mkdir -p "$scratch"
trap '/bin/busybox rm -rf "$scratch"' EXIT
observed_paths="$scratch/observed-paths"
observed_subcommands="$scratch/observed-subcommands"
candidate_paths="$scratch/candidate-paths"
candidate_elf_paths="$scratch/candidate-elf-paths"
forbidden_paths="$scratch/forbidden-paths"
: >"$observed_paths"
: >"$observed_subcommands"
: >"$candidate_paths"
: >"$candidate_elf_paths"
: >"$forbidden_paths"

if [ ! -s {BUILDKIT_RUNTIME_EXEC_EVIDENCE_GUEST_PATH} ]; then
  echo "BuildKit runtime execution evidence is missing" >&2
  exit 1
fi

saw_create_or_run=0
while IFS='|' read -r target subcommand extra; do
  if [ -z "$target" ] || [ -z "$subcommand" ] || [ -n "$extra" ]; then
    echo "malformed BuildKit runtime execution evidence" >&2
    exit 1
  fi
  /bin/busybox printf '%s\n' "$target" >>"$observed_paths"
  /bin/busybox printf '%s\n' "$subcommand" >>"$observed_subcommands"
  case "$subcommand" in
    create|run) saw_create_or_run=1 ;;
  esac
done <{BUILDKIT_RUNTIME_EXEC_EVIDENCE_GUEST_PATH}
/bin/busybox sort -u "$observed_paths" -o "$observed_paths"
/bin/busybox sort -u "$observed_subcommands" -o "$observed_subcommands"

if [ "$saw_create_or_run" -ne 1 ]; then
  echo "BuildKit runtime evidence has no successful-build create/run observation" >&2
  exit 1
fi

for path in $(/bin/busybox cat "$observed_paths"); do
  if [ ! -f "$path" ] || [ ! -x "$path" ]; then
    echo "observed OCI runtime is missing or not executable: $path" >&2
    exit 1
  fi
  runtime_magic=$(/bin/busybox od -An -tx1 -N4 "$path" | /bin/busybox tr -d ' ')
  if [ "$runtime_magic" != "7f454c46" ]; then
    echo "observed OCI runtime is not an ELF executable: $path" >&2
    exit 1
  fi
  /bin/busybox printf '%s\n' "$path" >>"$candidate_paths"
done

for root in /mnt/buildkit-bin /mnt/linux-bin /tmp /run /bin /sbin /usr/bin /usr/local/bin; do
  if [ ! -e "$root" ]; then
    continue
  fi
  /bin/busybox find "$root" -maxdepth 3 \( \
    -name youki -o -name runc -o -name runc-real -o \
    -name buildkit-runc -o -name crun \
  \) -print >>"$candidate_paths"
done

for path in \
  /mnt/buildkit-bin/buildkit-runc /mnt/buildkit-bin/runc /mnt/buildkit-bin/crun \
  /mnt/linux-bin/runc /mnt/linux-bin/crun \
  /tmp/buildkit-runc /tmp/runc /tmp/runc-real /tmp/crun \
  /run/buildkit-runc /run/runc /run/runc-real /run/crun \
  /bin/runc /bin/crun /sbin/runc /sbin/crun \
  /usr/bin/runc /usr/bin/crun /usr/local/bin/runc /usr/local/bin/crun
do
  if [ -e "$path" ] || [ -L "$path" ]; then
    /bin/busybox printf '%s\n' "$path" >>"$candidate_paths"
    /bin/busybox printf '%s\n' "$path" >>"$forbidden_paths"
  fi
done

/bin/busybox sort -u "$candidate_paths" -o "$candidate_paths"
while IFS= read -r path; do
  base=${{path##*/}}
  case "$base" in
    runc|runc-real|buildkit-runc|crun)
      /bin/busybox printf '%s\n' "$path" >>"$forbidden_paths"
      ;;
  esac
  if [ ! -f "$path" ] || [ ! -x "$path" ]; then
    continue
  fi
  runtime_magic=$(/bin/busybox od -An -tx1 -N4 "$path" | /bin/busybox tr -d ' ')
  if [ "$runtime_magic" = "7f454c46" ]; then
    /bin/busybox printf '%s\n' "$path" >>"$candidate_elf_paths"
  fi
done <"$candidate_paths"

for proc_dir in /proc/[0-9]*; do
  [ -d "$proc_dir" ] || continue
  pid=${{proc_dir##*/}}
  [ "$pid" = "$$" ] && continue
  executable=$(/bin/busybox readlink "$proc_dir/exe" 2>/dev/null || true)
  if [ -n "$executable" ]; then
    base=${{executable##*/}}
    case "$base" in
      runc|runc-real|buildkit-runc|crun|runc\ \(deleted\)|runc-real\ \(deleted\)|buildkit-runc\ \(deleted\)|crun\ \(deleted\))
        /bin/busybox printf 'proc:%s:exe:%s\n' "$pid" "$executable" >>"$forbidden_paths"
        ;;
    esac
  fi
  if [ -r "$proc_dir/cmdline" ]; then
    if ! /bin/busybox tr '\000' '\n' <"$proc_dir/cmdline" >"$scratch/argv.$pid" 2>/dev/null; then
      continue
    fi
    while IFS= read -r argument; do
      argument_path=${{argument#*=}}
      base=${{argument_path##*/}}
      case "$base" in
        runc|runc-real|buildkit-runc|crun)
          /bin/busybox printf 'proc:%s:argv:%s\n' "$pid" "$argument" >>"$forbidden_paths"
          ;;
      esac
    done <"$scratch/argv.$pid"
  fi
done

/bin/busybox sort -u "$candidate_elf_paths" -o "$candidate_elf_paths"
/bin/busybox sort -u "$forbidden_paths" -o "$forbidden_paths"
if [ -s "$forbidden_paths" ]; then
  echo "forbidden OCI runtime paths/processes found:" >&2
  /bin/busybox cat "$forbidden_paths" >&2
  exit 1
fi

if [ "$(/bin/busybox wc -l <"$observed_paths" | /bin/busybox tr -d ' ')" != "1" ] || \
   ! /bin/busybox grep -Fqx /mnt/linux-bin/youki "$observed_paths"; then
  echo "observed OCI runtime set is not exactly /mnt/linux-bin/youki" >&2
  /bin/busybox cat "$observed_paths" >&2
  exit 1
fi
if [ "$(/bin/busybox wc -l <"$candidate_elf_paths" | /bin/busybox tr -d ' ')" != "1" ] || \
   ! /bin/busybox grep -Fqx /mnt/linux-bin/youki "$candidate_elf_paths"; then
  echo "candidate OCI runtime ELF set is not exactly /mnt/linux-bin/youki" >&2
  /bin/busybox cat "$candidate_elf_paths" >&2
  exit 1
fi

runtime_binary=$(/bin/busybox cat "$observed_paths")
runtime_version=$("$runtime_binary" --version | /bin/busybox head -n 1)
if [ -z "$runtime_version" ]; then
  echo "observed youki runtime returned an empty version identity" >&2
  exit 1
fi

pid=$(/bin/busybox cat /tmp/buildkitd.pid)
buildkitd_executable=$(/bin/busybox readlink "/proc/$pid/exe")
if [ "$buildkitd_executable" != "/mnt/buildkit-bin/buildkitd" ]; then
  echo "unexpected BuildKit daemon executable: $buildkitd_executable" >&2
  exit 1
fi
/bin/busybox tr '\000' '\n' <"/proc/$pid/cmdline" >/tmp/buildkitd.argv
/bin/busybox grep -Fx -- "--oci-worker-binary" /tmp/buildkitd.argv >/dev/null
/bin/busybox grep -Fx -- "{BUILDKIT_OCI_RUNTIME_SHIM_GUEST_PATH}" /tmp/buildkitd.argv >/dev/null
/bin/busybox grep -q " /sys/fs/cgroup cgroup2 " /proc/mounts

/bin/busybox printf 'oci_worker_binary=%s\n' '{BUILDKIT_OCI_RUNTIME_SHIM_GUEST_PATH}'
/bin/busybox printf 'shim_target=%s\n' "$shim_target"
/bin/busybox printf 'runtime_binary=%s\n' "$runtime_binary"
while IFS= read -r path; do
  /bin/busybox printf 'observed_runtime_path=%s\n' "$path"
done <"$observed_paths"
while IFS= read -r subcommand; do
  /bin/busybox printf 'observed_oci_subcommand=%s\n' "$subcommand"
done <"$observed_subcommands"
while IFS= read -r path; do
  /bin/busybox printf 'oci_runtime_elf_path=%s\n' "$path"
done <"$candidate_elf_paths"
while IFS= read -r path; do
  /bin/busybox printf 'forbidden_runtime_path=%s\n' "$path"
done <"$forbidden_paths"
/bin/busybox printf 'runtime_version=%s\n' "$runtime_version"
/bin/busybox printf 'buildkitd_executable=%s\n' "$buildkitd_executable"
/bin/busybox printf 'buildkitd_oci_worker_binary=%s\n' '{BUILDKIT_OCI_RUNTIME_SHIM_GUEST_PATH}'
/bin/busybox printf 'cgroup_filesystem=%s\n' 'cgroup2'
"#
    );
    let output = vm
        .vm()
        .exec_collect(
            "/bin/busybox".to_string(),
            vec!["sh".to_string(), "-c".to_string(), inventory_script],
            BUILDKIT_SETUP_TIMEOUT,
        )
        .await
        .map_err(BuildkitError::from)?;
    if output.exit_code != 0 {
        return Err(BuildkitError::GuestCommandFailed {
            command: "inspect BuildKit OCI runtime inventory".to_string(),
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        });
    }
    let inventory = parse_buildkit_runtime_inventory(&output.stdout)?;
    validate_buildkit_runtime_inventory(&inventory)?;
    Ok(inventory)
}

fn parse_buildkit_runtime_inventory(
    output: &str,
) -> Result<BuildkitRuntimeInventory, BuildkitError> {
    let values = output
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect::<BTreeMap<_, _>>();
    let required = |key: &str| {
        values
            .get(key)
            .map(|value| (*value).to_string())
            .ok_or_else(|| {
                BuildkitError::InvalidConfig(format!(
                    "BuildKit runtime inventory is missing field '{key}'"
                ))
            })
    };
    let repeated = |key: &str| {
        output
            .lines()
            .filter_map(|line| {
                let (field, value) = line.split_once('=')?;
                (field == key).then(|| value.to_string())
            })
            .collect::<Vec<_>>()
    };

    Ok(BuildkitRuntimeInventory {
        oci_worker_binary: required("oci_worker_binary")?,
        shim_target: required("shim_target")?,
        runtime_binary: required("runtime_binary")?,
        observed_runtime_paths: repeated("observed_runtime_path"),
        observed_oci_subcommands: repeated("observed_oci_subcommand"),
        oci_runtime_elf_paths: repeated("oci_runtime_elf_path"),
        forbidden_runtime_paths: repeated("forbidden_runtime_path"),
        runtime_version: required("runtime_version")?,
        buildkitd_executable: required("buildkitd_executable")?,
        buildkitd_oci_worker_binary: required("buildkitd_oci_worker_binary")?,
        cgroup_filesystem: required("cgroup_filesystem")?,
    })
}

fn validate_buildkit_runtime_inventory(
    inventory: &BuildkitRuntimeInventory,
) -> Result<(), BuildkitError> {
    let expected_runtime = "/mnt/linux-bin/youki";
    let expected_shim = BUILDKIT_OCI_RUNTIME_SHIM_GUEST_PATH;
    let has_build_subcommand = inventory
        .observed_oci_subcommands
        .iter()
        .any(|command| matches!(command.as_str(), "create" | "run"));
    let valid = inventory.oci_worker_binary == expected_shim
        && inventory.shim_target == "/usr/bin/vz-guest-agent"
        && inventory.runtime_binary == expected_runtime
        && inventory.observed_runtime_paths == [expected_runtime]
        && has_build_subcommand
        && inventory.oci_runtime_elf_paths == [expected_runtime]
        && inventory.forbidden_runtime_paths.is_empty()
        && inventory
            .runtime_version
            .to_ascii_lowercase()
            .contains("youki")
        && inventory.buildkitd_executable == "/mnt/buildkit-bin/buildkitd"
        && inventory.buildkitd_oci_worker_binary == expected_shim
        && inventory.cgroup_filesystem == "cgroup2";
    if valid {
        Ok(())
    } else {
        Err(BuildkitError::InvalidConfig(format!(
            "BuildKit runtime inventory violates the youki-only contract: {inventory:?}"
        )))
    }
}

/// Prune BuildKit cache and return command output summary.
pub async fn cache_prune(
    config: &RuntimeConfig,
    options: CachePruneOptions,
) -> Result<String, BuildkitError> {
    let result = cache_prune_inner(config, options).await;
    finish_managed_buildkit_operation(result).await
}

async fn cache_prune_inner(
    config: &RuntimeConfig,
    options: CachePruneOptions,
) -> Result<String, BuildkitError> {
    let shared_mounts = prepare_shared_mounts().await?;
    let vm = buildkit_vm_manager()
        .acquire(config, None, &shared_mounts)
        .await?;
    ensure_guest_buildkit_ready(vm.vm()).await?;

    let mut args = vec!["prune".to_string()];
    if options.all {
        args.push("--all".to_string());
    }
    if let Some(keep_duration) = options.keep_duration {
        args.push("--keep-duration".to_string());
        args.push(keep_duration);
    }
    if let Some(keep_storage) = options.keep_storage {
        args.push("--keep-storage".to_string());
        args.push(keep_storage);
    }
    let output = run_buildctl(vm.vm(), args, BUILDKIT_BUILD_TIMEOUT, None, false).await?;

    if output.exit_code != 0 {
        return Err(BuildkitError::BuildFailed {
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        });
    }

    Ok(render_command_output(output))
}

async fn prepare_buildkit_auth_dir(
    auth_dir: &Path,
    config: &RuntimeConfig,
    dockerfile_text: &str,
    request: &BuildRequest,
) -> Result<bool, BuildkitError> {
    let mut registries = registries_for_build(dockerfile_text, request);
    if registries.is_empty() {
        registries.insert("docker.io".to_string());
    }

    let mut auths = BTreeMap::new();
    match &config.auth {
        vz_image::Auth::Anonymous => {
            clear_buildkit_auth_config(auth_dir).await?;
            return Ok(false);
        }
        vz_image::Auth::Basic { username, password } => {
            let entry = basic_docker_auth(username, password);
            for registry in &registries {
                for key in docker_auth_keys_for_registry(registry) {
                    auths.insert(key, entry.clone());
                }
            }
        }
        vz_image::Auth::DockerConfig => {
            for registry in &registries {
                let server = docker_server_for_registry(registry);
                match get_credential(&server) {
                    Ok(DockerCredential::UsernamePassword(username, password)) => {
                        let entry = basic_docker_auth(&username, &password);
                        for key in docker_auth_keys_for_registry(registry) {
                            auths.insert(key, entry.clone());
                        }
                    }
                    Ok(DockerCredential::IdentityToken(token)) => {
                        let entry = DockerConfigAuth {
                            auth: None,
                            identitytoken: Some(token),
                        };
                        for key in docker_auth_keys_for_registry(registry) {
                            auths.insert(key, entry.clone());
                        }
                    }
                    Err(error) if is_nonfatal_credential_lookup_error(&error) => {}
                    Err(error) => {
                        return Err(BuildkitError::CredentialLookup {
                            registry: registry.clone(),
                            source: error,
                        });
                    }
                }
            }
        }
    }

    if auths.is_empty() {
        clear_buildkit_auth_config(auth_dir).await?;
        return Ok(false);
    }

    tokio::fs::create_dir_all(&auth_dir).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(auth_dir, std::fs::Permissions::from_mode(0o700))?;
    }

    let config_file = DockerConfigFile { auths };
    let config_json = serde_json::to_vec_pretty(&config_file)?;
    let config_path = auth_dir.join("config.json");
    tokio::fs::write(&config_path, config_json).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(config_path, std::fs::Permissions::from_mode(0o600))?;
    }

    Ok(true)
}

async fn clear_buildkit_auth_config(auth_dir: &Path) -> Result<(), BuildkitError> {
    let config_path = auth_dir.join("config.json");
    match tokio::fs::remove_file(config_path).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(BuildkitError::Io(error)),
    }
}

pub(crate) fn registries_for_build(
    dockerfile_text: &str,
    request: &BuildRequest,
) -> BTreeSet<String> {
    let mut registries = parse_dockerfile_registries(dockerfile_text);
    if let Some(registry) = parse_dockerfile_syntax_registry(dockerfile_text) {
        registries.insert(registry);
    }
    // Dockerfile frontend images are frequently hosted on Docker Hub.
    // Keep Hub credentials available even when FROM references only other registries.
    registries.insert("docker.io".to_string());

    if matches!(request.output, BuildOutput::RegistryPush)
        && let Some(registry) = parse_registry_from_reference(&request.tag)
    {
        registries.insert(registry);
    }

    registries
}

pub(crate) fn parse_dockerfile_registries(dockerfile_text: &str) -> BTreeSet<String> {
    let mut registries = BTreeSet::new();

    for line in dockerfile_text.lines() {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let mut tokens = trimmed.split_whitespace();
        let Some(first) = tokens.next() else {
            continue;
        };
        if !first.eq_ignore_ascii_case("from") {
            continue;
        }

        let image = tokens.find(|token| !token.starts_with("--"));
        let Some(image) = image else {
            continue;
        };

        if image.contains("${") {
            continue;
        }

        if let Some(registry) = parse_registry_from_reference(image) {
            registries.insert(registry);
        }
    }

    registries
}

pub(crate) fn parse_dockerfile_syntax_registry(dockerfile_text: &str) -> Option<String> {
    for line in dockerfile_text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !trimmed.starts_with('#') {
            return None;
        }

        let directive = trimmed.trim_start_matches('#').trim();
        let Some(rest) = directive.strip_prefix("syntax=") else {
            continue;
        };
        let image_ref = rest.trim();
        if image_ref.is_empty() || image_ref.contains("${") {
            return None;
        }
        return parse_registry_from_reference(image_ref);
    }

    None
}

fn parse_registry_from_reference(reference: &str) -> Option<String> {
    Reference::from_str(reference)
        .ok()
        .map(|parsed| parsed.registry().to_string())
}

fn docker_server_for_registry(registry: &str) -> String {
    if is_docker_hub_registry(registry) {
        "https://index.docker.io/v1/".to_string()
    } else {
        registry.to_string()
    }
}

pub(crate) fn docker_auth_keys_for_registry(registry: &str) -> Vec<String> {
    if is_docker_hub_registry(registry) {
        vec![
            "https://index.docker.io/v1/".to_string(),
            "docker.io".to_string(),
            "index.docker.io".to_string(),
            "registry-1.docker.io".to_string(),
        ]
    } else {
        vec![registry.to_string()]
    }
}

fn is_docker_hub_registry(registry: &str) -> bool {
    matches!(
        registry,
        "docker.io" | "index.docker.io" | "registry-1.docker.io"
    )
}

fn basic_docker_auth(username: &str, password: &str) -> DockerConfigAuth {
    let encoded =
        base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
    DockerConfigAuth {
        auth: Some(encoded),
        identitytoken: None,
    }
}

fn is_nonfatal_credential_lookup_error(error: &CredentialRetrievalError) -> bool {
    match error {
        CredentialRetrievalError::NoCredentialConfigured
        | CredentialRetrievalError::ConfigNotFound
        | CredentialRetrievalError::ConfigReadError => true,
        CredentialRetrievalError::HelperFailure { stdout, stderr, .. } => {
            let text = format!("{stdout}\n{stderr}").to_ascii_lowercase();
            text.contains("not found")
                || text.contains("credentials not found")
                || text.contains("no credentials")
        }
        _ => false,
    }
}

async fn cleanup_temp_dir(path: &Path, label: &str) {
    if let Err(error) = tokio::fs::remove_dir_all(path).await {
        warn!(
            label,
            path = %path.display(),
            %error,
            "failed to clean temporary directory"
        );
    }
}

fn validate_buildkit_kernel_capabilities(kernel: &KernelPaths) -> Result<(), BuildkitError> {
    validate_declared_buildkit_kernel_capabilities(kernel.version.capabilities.as_ref())
}

fn validate_declared_buildkit_kernel_capabilities(
    capabilities: Option<&BTreeSet<KernelCapability>>,
) -> Result<(), BuildkitError> {
    let required = [KernelCapability::CgroupBpf, KernelCapability::UserNs];
    let missing = required
        .into_iter()
        .filter(|capability| {
            capabilities.is_none_or(|capabilities| !capabilities.contains(capability))
        })
        .map(KernelCapability::as_str)
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }

    Err(BuildkitError::InvalidConfig(format!(
        "BuildKit kernel bundle is missing required capabilities: {}",
        missing.join(", ")
    )))
}

fn normalize_buildkit_config(config: &RuntimeConfig) -> Result<RuntimeConfig, BuildkitError> {
    if config.linux_profile == Some(KernelProfile::Container) {
        return Err(BuildkitError::InvalidConfig(
            "BuildKit requires the developer Linux profile; the container profile intentionally omits user namespaces"
                .to_string(),
        ));
    }
    let mut normalized = config.clone();
    normalized.linux_profile = Some(KernelProfile::Developer);
    Ok(normalized)
}

async fn ensure_buildkit_kernel_for_config(
    config: &RuntimeConfig,
) -> Result<KernelPaths, BuildkitError> {
    let developer_config = normalize_buildkit_config(config)?;
    let kernel = ensure_kernel_for_config(&developer_config).await?;
    validate_buildkit_kernel_capabilities(&kernel)?;
    Ok(kernel)
}

async fn stage_buildkit_runtime(
    kernel: &KernelPaths,
    stage_root: &Path,
) -> Result<tempfile::TempDir, BuildkitError> {
    let expected_sha256 = kernel
        .version
        .sha256_youki
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            BuildkitError::InvalidConfig(
                "BuildKit requires sha256_youki metadata for the selected youki runtime"
                    .to_string(),
            )
        })?
        .trim()
        .to_ascii_lowercase();

    tokio::fs::create_dir_all(stage_root).await?;
    tokio::fs::set_permissions(stage_root, std::fs::Permissions::from_mode(0o700)).await?;
    let runtime_dir = tempfile::Builder::new()
        .prefix("youki-")
        .tempdir_in(stage_root)?;
    std::fs::set_permissions(runtime_dir.path(), std::fs::Permissions::from_mode(0o700))?;

    let mut source = tokio::fs::File::open(&kernel.youki).await?;
    if !source.metadata().await?.is_file() {
        return Err(BuildkitError::InvalidConfig(format!(
            "selected youki runtime is not a regular file: {}",
            kernel.youki.display()
        )));
    }

    let staged_path = runtime_dir.path().join("youki");
    let staged_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o500)
        .open(&staged_path)?;
    let mut staged = tokio::fs::File::from_std(staged_file);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = source.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        staged.write_all(&buffer[..read]).await?;
    }
    staged.flush().await?;
    staged.sync_all().await?;
    drop(staged);
    tokio::fs::set_permissions(&staged_path, std::fs::Permissions::from_mode(0o555)).await?;

    let found_sha256 = format!("{:x}", hasher.finalize());
    if found_sha256 != expected_sha256 {
        return Err(BuildkitError::InvalidConfig(format!(
            "selected youki checksum mismatch while staging: expected {expected_sha256}, found {found_sha256}"
        )));
    }

    let mut entries = tokio::fs::read_dir(runtime_dir.path()).await?;
    let entry = entries.next_entry().await?.ok_or_else(|| {
        BuildkitError::InvalidConfig("staged BuildKit runtime directory is empty".to_string())
    })?;
    let file_type = entry.file_type().await?;
    if entry.file_name() != OsStr::new("youki")
        || !file_type.is_file()
        || file_type.is_symlink()
        || entries.next_entry().await?.is_some()
    {
        return Err(BuildkitError::InvalidConfig(
            "staged BuildKit runtime directory must contain exactly one regular youki file"
                .to_string(),
        ));
    }
    std::fs::File::open(runtime_dir.path())?.sync_all()?;

    Ok(runtime_dir)
}

async fn start_buildkit_vm(
    config: &RuntimeConfig,
    context_dir: Option<&Path>,
    output_root: &Path,
    auth_dir: &Path,
) -> Result<StartedBuildkitVm, BuildkitError> {
    ensure_virtualization_entitlement_preflight()?;

    let artifacts = ensure_buildkit_artifacts().await?;
    let kernel = ensure_buildkit_kernel_for_config(config).await?;
    let runtime_dir = stage_buildkit_runtime(
        &kernel,
        &default_buildkit_dir()?.join("runtime").join("oci-runtime"),
    )
    .await?;
    let mut runtime_guard = StagedRuntimeGuard::new(runtime_dir);

    let mut vm_config = LinuxVmConfig::new(kernel.kernel, kernel.initramfs);
    vm_config.cpus = 4;
    vm_config.memory_mb = BUILDKIT_VM_MEMORY_MB;
    vm_config.disk_image = Some(artifacts.disk_image_path.clone());
    vm_config.shared_dirs = vec![
        SharedDirConfig {
            tag: "buildkit-bin".to_string(),
            source: artifacts.bin_dir,
            read_only: true,
        },
        SharedDirConfig {
            tag: "buildkit-cache".to_string(),
            source: artifacts.cache_dir,
            read_only: false,
        },
    ];

    vm_config.shared_dirs.push(SharedDirConfig {
        tag: "linux-bin".to_string(),
        source: runtime_guard.path()?.to_path_buf(),
        read_only: true,
    });

    if let Some(host_ssl_dir) = host_ssl_dir() {
        vm_config.shared_dirs.push(SharedDirConfig {
            tag: "host-ssl".to_string(),
            source: host_ssl_dir,
            read_only: true,
        });
    }

    if let Some(context_dir) = context_dir {
        vm_config.shared_dirs.push(SharedDirConfig {
            tag: BUILDKIT_SHARED_CONTEXT_TAG.to_string(),
            source: context_dir.to_path_buf(),
            read_only: true,
        });
    }

    vm_config.shared_dirs.push(SharedDirConfig {
        tag: BUILDKIT_SHARED_OUTPUT_TAG.to_string(),
        source: output_root.to_path_buf(),
        read_only: false,
    });
    vm_config.shared_dirs.push(SharedDirConfig {
        tag: BUILDKIT_AUTH_TAG.to_string(),
        source: auth_dir.to_path_buf(),
        read_only: true,
    });

    if !config.default_network_enabled {
        vm_config.network = Some(NetworkConfig::None);
    }

    runtime_guard.preserve_on_drop();
    let vm = match LinuxVm::create(vm_config).await {
        Ok(vm) => vm,
        Err(error) => {
            runtime_guard.cleanup_on_drop();
            return Err(map_vm_boot_error(BuildkitError::from(error)));
        }
    };
    if let Err(error) = vm.start().await {
        if vm.stop().await.is_ok() {
            runtime_guard.cleanup_on_drop();
        }
        return Err(map_vm_boot_error(BuildkitError::from(error)));
    }

    if let Err(err) = vm.wait_for_agent(config.agent_ready_timeout).await {
        if vm.stop().await.is_ok() {
            runtime_guard.cleanup_on_drop();
        }
        return Err(err.into());
    }

    // init starts DHCP in the background: agent readiness does not establish
    // the route needed by a newly created (including context-switched) builder.
    if let Err(error) = wait_for_buildkit_network(
        config.default_network_enabled,
        config.agent_ready_timeout,
        |timeout| {
            vm.exec_collect(
                "/bin/busybox".to_string(),
                vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    BUILDKIT_NETWORK_SAMPLE.to_string(),
                ],
                timeout,
            )
        },
    )
    .await
    {
        if vm.stop().await.is_ok() {
            runtime_guard.cleanup_on_drop();
        }
        return Err(error);
    }

    let runtime_dir = runtime_guard.into_runtime_dir()?;
    Ok(StartedBuildkitVm { vm, runtime_dir })
}

// Local configuration observations only; never resolve DNS, contact an external
// endpoint, restart DHCP, or infer offline policy from a missing interface.
const BUILDKIT_NETWORK_SAMPLE: &str = r#"
set -eu
printf 'addresses\n'
/bin/busybox ip -4 addr show dev eth0
printf 'routes\n'
/bin/busybox ip -4 route show default
printf 'dhcp_pids\n'
/bin/busybox pidof udhcpc || true
"#;

fn buildkit_network_sample_ready(sample: &str) -> bool {
    fn usable_ipv4(value: &str) -> bool {
        value.parse::<std::net::Ipv4Addr>().is_ok_and(|address| {
            let octets = address.octets();
            octets[0] != 0 && !address.is_loopback() && !address.is_link_local() && octets[0] < 224
        })
    }

    let Some(sample) = sample.strip_prefix("addresses\n") else {
        return false;
    };
    let Some((addresses, remainder)) = sample.split_once("routes\n") else {
        return false;
    };
    let Some((routes, _dhcp)) = remainder.split_once("dhcp_pids\n") else {
        return false;
    };
    let link_up = addresses.lines().next().is_some_and(|line| {
        let fields: Vec<_> = line.split_whitespace().collect();
        fields.get(1) == Some(&"eth0:")
            && fields.get(2).is_some_and(|flags| {
                flags
                    .trim_matches(['<', '>'])
                    .split(',')
                    .any(|flag| flag == "UP")
            })
    });
    let address_ready = addresses.lines().any(|line| {
        let fields: Vec<_> = line.split_whitespace().collect();
        fields.first() == Some(&"inet")
            && fields.get(1).is_some_and(|cidr| {
                cidr.split_once('/').is_some_and(|(ip, prefix)| {
                    usable_ipv4(ip) && prefix.parse::<u8>().is_ok_and(|bits| bits <= 32)
                })
            })
            && fields.windows(2).any(|pair| pair == ["scope", "global"])
            && fields.last() == Some(&"eth0")
            && !fields
                .iter()
                .any(|field| matches!(*field, "tentative" | "dadfailed"))
    });
    let route_ready = routes.lines().any(|line| {
        let fields: Vec<_> = line.split_whitespace().collect();
        fields.first() == Some(&"default")
            && fields.windows(2).any(|pair| pair == ["dev", "eth0"])
            && fields
                .windows(2)
                .any(|pair| pair[0] == "via" && usable_ipv4(pair[1]))
            && !fields.contains(&"linkdown")
    });
    link_up && address_ready && route_ready
}

async fn wait_for_buildkit_network<F, Fut, E>(
    enabled: bool,
    timeout: Duration,
    mut sample: F,
) -> Result<(), BuildkitError>
where
    F: FnMut(Duration) -> Fut,
    Fut: std::future::Future<Output = Result<ExecOutput, E>>,
    E: std::fmt::Display,
{
    if !enabled {
        return Ok(());
    }
    let started = tokio::time::Instant::now();
    let deadline = started + timeout;
    let mut observations = VecDeque::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(buildkit_network_readiness_error(
                timeout,
                "deadline expired",
                &observations,
            ));
        }
        let probe_timeout = remaining.min(Duration::from_secs(2));
        let output = match tokio::time::timeout(probe_timeout, sample(probe_timeout)).await {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => {
                return Err(buildkit_network_readiness_error(
                    timeout,
                    &format!("sample execution failed: {error}"),
                    &observations,
                ));
            }
            Err(_) => {
                return Err(buildkit_network_readiness_error(
                    timeout,
                    "sample execution timed out",
                    &observations,
                ));
            }
        };
        let ready = output.exit_code == 0 && buildkit_network_sample_ready(&output.stdout);
        let observation = format!(
            "elapsed_ms={} exit={} stdout={:?} stderr={:?}",
            started.elapsed().as_millis(),
            output.exit_code,
            output.stdout.chars().take(2048).collect::<String>(),
            output.stderr.chars().take(2048).collect::<String>(),
        );
        tracing::info!(sample = %observation, ready, "BuildKit guest local network readiness sample");
        if observations.len() == 4 {
            observations.pop_front();
        }
        observations.push_back(observation);
        if output.exit_code != 0 {
            return Err(buildkit_network_readiness_error(
                timeout,
                "sample command failed",
                &observations,
            ));
        }
        if ready {
            tracing::info!(samples = ?observations, "BuildKit guest eth0 IPv4 address and default route ready");
            return Ok(());
        }
        tokio::time::sleep_until(
            (tokio::time::Instant::now() + BUILDKIT_VM_RETRY_DELAY).min(deadline),
        )
        .await;
    }
}

fn buildkit_network_readiness_error(
    timeout: Duration,
    reason: &str,
    observations: &VecDeque<String>,
) -> BuildkitError {
    std::io::Error::other(format!(
        "BuildKit guest network readiness failed (budget {timeout:?}): {}; expected usable eth0 IPv4 address and default route; last local samples: {observations:?}",
        reason.chars().take(2048).collect::<String>(),
    ))
    .into()
}

async fn run_guest_build(
    vm: &LinuxVm,
    request: &BuildRequest,
    dockerfile_relative: &Path,
    guest_context_dir: &str,
    guest_output_tar: Option<&str>,
    on_event: &mut impl FnMut(BuildEvent),
) -> Result<(), BuildkitError> {
    ensure_guest_buildkit_ready(vm).await?;
    clear_guest_buildkit_runtime_evidence(vm).await?;

    let mut args = vec![
        "build".to_string(),
        "--progress".to_string(),
        request.progress.as_buildctl_value().to_string(),
        "--frontend".to_string(),
        "dockerfile.v0".to_string(),
        "--local".to_string(),
        format!("context={guest_context_dir}"),
        "--local".to_string(),
        format!("dockerfile={guest_context_dir}"),
        "--opt".to_string(),
        format!("filename={}", dockerfile_relative.display()),
    ];

    match &request.output {
        BuildOutput::VzStore | BuildOutput::OciTar { .. } => {
            let guest_output_tar = guest_output_tar.ok_or_else(|| {
                BuildkitError::InvalidConfig("missing guest output archive path".to_string())
            })?;
            args.push("--output".to_string());
            args.push(format!(
                "type=oci,dest={guest_output_tar},name={}",
                request.tag
            ));
        }
        BuildOutput::RegistryPush => {
            args.push("--output".to_string());
            args.push(format!("type=image,name={},push=true", request.tag));
        }
    }

    if let Some(target) = &request.target {
        args.push("--opt".to_string());
        args.push(format!("target={target}"));
    }
    for cache_ref in &request.cache_from {
        args.push("--import-cache".to_string());
        args.push(format!("type=registry,ref={cache_ref}"));
    }
    if request.no_cache {
        args.push("--no-cache".to_string());
    }
    for (key, value) in &request.build_args {
        args.push("--opt".to_string());
        args.push(format!("build-arg:{key}={value}"));
    }
    for secret in &request.secrets {
        args.push("--secret".to_string());
        args.push(secret.clone());
    }

    let output = run_buildctl(
        vm,
        args,
        BUILDKIT_BUILD_TIMEOUT,
        Some(on_event),
        request.progress == BuildProgress::RawJson,
    )
    .await?;
    if output.exit_code != 0 {
        return Err(BuildkitError::BuildFailed {
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        });
    }

    Ok(())
}

async fn clear_guest_buildkit_runtime_evidence(vm: &LinuxVm) -> Result<(), BuildkitError> {
    run_guest_command(
        vm,
        "clear BuildKit OCI runtime execution evidence",
        "/bin/busybox",
        vec![
            "rm".to_string(),
            "-f".to_string(),
            BUILDKIT_RUNTIME_EXEC_EVIDENCE_GUEST_PATH.to_string(),
        ],
        BUILDKIT_SETUP_TIMEOUT,
    )
    .await
}

async fn shutdown_guest_buildkitd(vm: &LinuxVm) -> Result<(), BuildkitError> {
    let shutdown_script = r#"
set -eu

if [ ! -f /tmp/buildkitd.pid ]; then
  exit 0
fi

pid=$(/bin/busybox cat /tmp/buildkitd.pid 2>/dev/null || true)
if [ -z "$pid" ]; then
  exit 0
fi

if /bin/busybox kill -0 "$pid" 2>/dev/null; then
  /bin/busybox kill "$pid" 2>/dev/null || true
  i=0
  while [ "$i" -lt 15 ]; do
    if ! /bin/busybox kill -0 "$pid" 2>/dev/null; then
      break
    fi
    i=$((i + 1))
    /bin/busybox sleep 1
  done
  if /bin/busybox kill -0 "$pid" 2>/dev/null; then
    /bin/busybox kill -9 "$pid" 2>/dev/null || true
  fi
fi

# Virtualization.framework stop is equivalent to pulling the power cord. Flush
# and detach the persistent cache filesystem before the host stops the VM.
/bin/busybox sync
if /bin/busybox grep -q " /var/lib/buildkit " /proc/mounts; then
  /bin/busybox umount /var/lib/buildkit
fi
/bin/busybox sync
/bin/busybox rm -f /tmp/buildkitd.pid
exit 0
"#;

    run_guest_command(
        vm,
        "shutdown buildkitd in guest",
        "/bin/busybox",
        vec![
            "sh".to_string(),
            "-c".to_string(),
            shutdown_script.to_string(),
        ],
        BUILDKIT_SHUTDOWN_TIMEOUT,
    )
    .await
}

async fn ensure_guest_buildkit_ready(vm: &LinuxVm) -> Result<(), BuildkitError> {
    let setup_script = guest_buildkit_setup_script();

    run_guest_command(
        vm,
        "setup buildkit guest environment",
        "/bin/busybox",
        vec!["sh".to_string(), "-c".to_string(), setup_script],
        BUILDKIT_SETUP_TIMEOUT,
    )
    .await
}

fn guest_buildkit_setup_script() -> String {
    format!(
        r#"
set -eu

/bin/busybox mkdir -p /mnt/buildkit-bin /mnt/linux-bin /var/lib/buildkit /mnt/build-context /mnt/build-output /mnt/host-ssl {BUILDKIT_AUTH_GUEST_DIR}
/bin/busybox mkdir -p /etc/buildkit
if ! /bin/busybox grep -q " /mnt/buildkit-bin " /proc/mounts; then
  /bin/busybox mount -t virtiofs buildkit-bin /mnt/buildkit-bin
fi
if ! /bin/busybox grep -q " /mnt/buildkit-bin " /proc/mounts; then
  echo "BuildKit binary share is not mounted" >&2
  exit 1
fi
if [ ! -x /mnt/buildkit-bin/buildkitd ] || [ ! -x /mnt/buildkit-bin/buildctl ]; then
  echo "BuildKit bundle is missing executable buildkitd or buildctl" >&2
  exit 1
fi
if ! /bin/busybox grep -q " /var/lib/buildkit " /proc/mounts; then
  if [ ! -b /dev/vda ]; then
    echo "buildkit cache disk /dev/vda is unavailable" >&2
    exit 1
  fi
  if ! /bin/busybox mount -t ext4 /dev/vda /var/lib/buildkit 2>/tmp/buildkit-disk-mount.log; then
    /bin/busybox mke2fs -F /dev/vda >/tmp/buildkit-disk-format.log 2>&1
    /bin/busybox mount -t ext4 /dev/vda /var/lib/buildkit
  fi
fi
/bin/busybox mkdir -p /var/lib/buildkit/build-output
if ! /bin/busybox grep -q " /mnt/linux-bin " /proc/mounts; then
  /bin/busybox mount -t virtiofs linux-bin /mnt/linux-bin
fi
if ! /bin/busybox grep -q " /mnt/linux-bin " /proc/mounts; then
  echo "Linux runtime binary share is not mounted" >&2
  exit 1
fi
if [ ! -x /mnt/linux-bin/youki ]; then
  echo "youki is missing or not executable at /mnt/linux-bin/youki" >&2
  exit 1
fi
runtime_share_entries=$(/bin/busybox find /mnt/linux-bin -mindepth 1 -maxdepth 1 -print)
if [ "$runtime_share_entries" != "/mnt/linux-bin/youki" ]; then
  echo "BuildKit runtime share must contain exactly /mnt/linux-bin/youki" >&2
  /bin/busybox printf '%s\n' "$runtime_share_entries" >&2
  exit 1
fi
forbidden_runtime_paths=$(/bin/busybox find /mnt/buildkit-bin /mnt/linux-bin /tmp -maxdepth 1 -name '*runc*' -print)
if [ -n "$forbidden_runtime_paths" ]; then
  echo "forbidden legacy OCI runtime path found: $forbidden_runtime_paths" >&2
  exit 1
fi
/bin/busybox mount -t virtiofs build-context /mnt/build-context 2>/dev/null || true
/bin/busybox mount -t virtiofs build-output /mnt/build-output 2>/dev/null || true
/bin/busybox mount -t virtiofs host-ssl /mnt/host-ssl 2>/dev/null || true
/bin/busybox mount -t virtiofs {BUILDKIT_AUTH_TAG} {BUILDKIT_AUTH_GUEST_DIR} 2>/dev/null || true
/bin/busybox mkdir -p /sys/fs/cgroup
if ! /bin/busybox grep -q " /sys/fs/cgroup cgroup2 " /proc/mounts; then
  /bin/busybox mount -t cgroup2 none /sys/fs/cgroup
fi
if ! /bin/busybox grep -q " /sys/fs/cgroup cgroup2 " /proc/mounts; then
  echo "cgroup v2 is not mounted at /sys/fs/cgroup" >&2
  exit 1
fi
if [ ! -x /usr/bin/vz-guest-agent ]; then
  echo "vz-guest-agent is missing or not executable" >&2
  exit 1
fi
/bin/busybox ln -sf /usr/bin/vz-guest-agent {BUILDKIT_OCI_RUNTIME_SHIM_GUEST_PATH}
if [ ! -x {BUILDKIT_OCI_RUNTIME_SHIM_GUEST_PATH} ]; then
  echo "failed to install BuildKit OCI runtime shim" >&2
  exit 1
fi
export PATH="/tmp:/mnt/buildkit-bin:$PATH"
if [ -f /mnt/host-ssl/cert.pem ]; then
  /bin/busybox mkdir -p /etc/ssl/certs
  /bin/busybox cp /mnt/host-ssl/cert.pem /etc/ssl/cert.pem
  /bin/busybox cp /mnt/host-ssl/cert.pem /etc/ssl/certs/ca-certificates.crt
  export SSL_CERT_FILE=/mnt/host-ssl/cert.pem
fi
/bin/busybox mkdir -p /root/.docker
if [ -f {BUILDKIT_AUTH_GUEST_CONFIG} ]; then
  /bin/busybox cp {BUILDKIT_AUTH_GUEST_CONFIG} /root/.docker/config.json
  /bin/busybox chmod 0600 /root/.docker/config.json
else
  /bin/busybox rm -f /root/.docker/config.json
fi
export HOME=/root
export DOCKER_CONFIG=/root/.docker

/bin/busybox cat >/etc/buildkit/buildkitd.toml <<'CFG'
[worker.oci]
  enabled = true
  binary = "{BUILDKIT_OCI_RUNTIME_SHIM_GUEST_PATH}"
  gc = true
  snapshotter = "{BUILDKIT_SNAPSHOTTER}"

[[worker.oci.gcpolicy]]
  keepDuration = "{BUILDKIT_CACHE_KEEP_DURATION}"
  all = true

[[worker.oci.gcpolicy]]
  keepBytes = {BUILDKIT_CACHE_KEEP_BYTES}
  all = true
CFG

start_buildkitd() {{
  /mnt/buildkit-bin/buildkitd \
    --config /etc/buildkit/buildkitd.toml \
    --addr {BUILDKITD_ADDR} \
    --oci-worker-binary {BUILDKIT_OCI_RUNTIME_SHIM_GUEST_PATH} \
    --oci-worker-snapshotter {BUILDKIT_SNAPSHOTTER} \
    --root /var/lib/buildkit >/tmp/buildkitd.log 2>&1 &
  /bin/busybox echo "$!" >/tmp/buildkitd.pid
}}

if ! /mnt/buildkit-bin/buildctl --addr {BUILDKITD_ADDR} debug workers >/dev/null 2>&1; then
  start_buildkitd
fi

recovered_bolt=0
i=0
while [ "$i" -lt 60 ]; do
  if /mnt/buildkit-bin/buildctl --addr {BUILDKITD_ADDR} debug workers >/dev/null 2>&1; then
    exit 0
  fi

  if [ "$recovered_bolt" -eq 0 ] && [ -f /tmp/buildkitd.log ] && \
     ( /bin/busybox grep -q "invalid freelist page" /tmp/buildkitd.log || \
       /bin/busybox grep -q "^panic:" /tmp/buildkitd.log || \
       /bin/busybox grep -q "page type is unknown" /tmp/buildkitd.log ); then
    if [ -f /tmp/buildkitd.pid ]; then
      pid=$(/bin/busybox cat /tmp/buildkitd.pid 2>/dev/null || true)
      if [ -n "$pid" ]; then
        /bin/busybox kill "$pid" 2>/dev/null || true
        /bin/busybox sleep 1
        /bin/busybox kill -9 "$pid" 2>/dev/null || true
      fi
    fi
    # Corrupted BuildKit root state cannot always be recovered by deleting only
    # cache.db; reset the worker root and let BuildKit bootstrap cleanly.
    /bin/busybox rm -rf /var/lib/buildkit/*
    /bin/busybox mkdir -p /var/lib/buildkit/build-output
    /bin/busybox sync
    /bin/busybox rm -f /tmp/buildkitd.log /tmp/buildkitd.pid
    recovered_bolt=1
    start_buildkitd
  fi

  i=$((i + 1))
  /bin/busybox sleep 1
done

echo "buildkitd did not become ready in guest" >&2
if [ -f /tmp/buildkitd.log ]; then
  /bin/busybox tail -n 200 /tmp/buildkitd.log >&2
fi
exit 1
"#
    )
}

async fn run_buildctl(
    vm: &LinuxVm,
    args: Vec<String>,
    timeout: Duration,
    mut on_event: Option<&mut dyn FnMut(BuildEvent)>,
    parse_rawjson: bool,
) -> Result<ExecOutput, BuildkitError> {
    let mut full_args = vec!["--addr".to_string(), BUILDKITD_ADDR.to_string()];
    full_args.extend(args);
    let mut stdout_decoder = parse_rawjson.then(BuildkitRawJsonStreamDecoder::default);
    let mut stderr_decoder = parse_rawjson.then(BuildkitRawJsonStreamDecoder::default);
    let mut stdout_started = false;
    let mut stderr_started = false;

    let output = vm
        .exec_streaming(
            "/bin/busybox".to_string(),
            {
                let mut args = vec![
                    "env".to_string(),
                    "HOME=/root".to_string(),
                    "DOCKER_CONFIG=/root/.docker".to_string(),
                    "/mnt/buildkit-bin/buildctl".to_string(),
                ];
                args.extend(full_args);
                args
            },
            timeout,
            |event| {
                if let Some(callback) = on_event.as_mut() {
                    match event {
                        ExecEvent::Stdout(chunk) => {
                            callback(BuildEvent::Output {
                                stream: BuildLogStream::Stdout,
                                chunk: chunk.clone(),
                            });
                            if let Some(decoder) = stdout_decoder.as_mut() {
                                for decoded in decoder.push_chunk(chunk) {
                                    match decoded {
                                        Ok(status) => {
                                            stdout_started = true;
                                            callback(BuildEvent::SolveStatus { status });
                                        }
                                        Err(error) => {
                                            if stdout_started || looks_like_json(&error.line) {
                                                callback(BuildEvent::RawJsonDecodeError {
                                                    line: rawjson_line_preview(&error.line),
                                                    error: error.error,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        ExecEvent::Stderr(chunk) => {
                            callback(BuildEvent::Output {
                                stream: BuildLogStream::Stderr,
                                chunk: chunk.clone(),
                            });
                            if let Some(decoder) = stderr_decoder.as_mut() {
                                for decoded in decoder.push_chunk(chunk) {
                                    match decoded {
                                        Ok(status) => {
                                            stderr_started = true;
                                            callback(BuildEvent::SolveStatus { status });
                                        }
                                        Err(error) => {
                                            if stderr_started || looks_like_json(&error.line) {
                                                callback(BuildEvent::RawJsonDecodeError {
                                                    line: rawjson_line_preview(&error.line),
                                                    error: error.error,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        ExecEvent::Exit(_) => {
                            if let Some(decoder) = stdout_decoder.as_mut() {
                                for decoded in decoder.finish() {
                                    match decoded {
                                        Ok(status) => {
                                            stdout_started = true;
                                            callback(BuildEvent::SolveStatus { status });
                                        }
                                        Err(error) => {
                                            if stdout_started || looks_like_json(&error.line) {
                                                callback(BuildEvent::RawJsonDecodeError {
                                                    line: rawjson_line_preview(&error.line),
                                                    error: error.error,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                            if let Some(decoder) = stderr_decoder.as_mut() {
                                for decoded in decoder.finish() {
                                    match decoded {
                                        Ok(status) => {
                                            stderr_started = true;
                                            callback(BuildEvent::SolveStatus { status });
                                        }
                                        Err(error) => {
                                            if stderr_started || looks_like_json(&error.line) {
                                                callback(BuildEvent::RawJsonDecodeError {
                                                    line: rawjson_line_preview(&error.line),
                                                    error: error.error,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
        )
        .await
        .map_err(BuildkitError::from)?;

    Ok(output)
}

fn rawjson_line_preview(line: &[u8]) -> String {
    const MAX_CHARS: usize = 240;
    let mut preview = String::from_utf8_lossy(line).into_owned();
    if preview.chars().count() > MAX_CHARS {
        preview = preview.chars().take(MAX_CHARS).collect::<String>();
        preview.push_str("...");
    }
    preview
}

fn looks_like_json(line: &[u8]) -> bool {
    line.iter()
        .find(|byte| !byte.is_ascii_whitespace())
        .is_some_and(|byte| *byte == b'{' || *byte == b'[')
}

async fn run_guest_command(
    vm: &LinuxVm,
    label: &str,
    command: &str,
    args: Vec<String>,
    timeout: Duration,
) -> Result<(), BuildkitError> {
    let output = vm
        .exec_collect(command.to_string(), args, timeout)
        .await
        .map_err(BuildkitError::from)?;

    if output.exit_code != 0 {
        return Err(BuildkitError::GuestCommandFailed {
            command: label.to_string(),
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        });
    }
    Ok(())
}

fn render_command_output(output: ExecOutput) -> String {
    let mut rendered = String::new();
    if !output.stdout.trim().is_empty() {
        rendered.push_str(output.stdout.trim_end());
    }
    if !output.stderr.trim().is_empty() {
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        rendered.push_str(output.stderr.trim_end());
    }
    rendered
}

fn host_ssl_dir() -> Option<PathBuf> {
    let ssl_dir = PathBuf::from("/etc/ssl");
    if ssl_dir.join("cert.pem").is_file() {
        Some(ssl_dir)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::ffi::OsString;
    use std::os::unix::fs::symlink;

    use tempfile::tempdir;

    use super::*;

    const NETWORK_READY: &str = "addresses\n2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500\n    inet 192.168.64.2/24 brd 192.168.64.255 scope global eth0\n       valid_lft forever preferred_lft forever\nroutes\ndefault via 192.168.64.1 dev eth0\ndhcp_pids\n";

    fn network_output(stdout: &str) -> ExecOutput {
        ExecOutput {
            stdout: stdout.to_string(),
            stderr: String::new(),
            exit_code: 0,
        }
    }

    #[test]
    fn buildkit_network_readiness_requires_address_and_route_on_up_eth0() {
        assert!(buildkit_network_sample_ready(NETWORK_READY));
        for bad in [
            NETWORK_READY.replace("UP,LOWER_UP", "LOWER_UP"),
            NETWORK_READY.replace("scope global", "scope host"),
            NETWORK_READY.replace("192.168.64.2/24", "127.0.0.1/8"),
            NETWORK_READY.replace("192.168.64.2/24", "169.254.1.2/16"),
            NETWORK_READY.replace("192.168.64.2/24", "0.0.0.0/0"),
            NETWORK_READY.replace("192.168.64.2/24", "224.0.0.1/24"),
            NETWORK_READY.replace("192.168.64.2/24", "192.168.64.2/33"),
            NETWORK_READY.replace("scope global eth0", "scope global eth1"),
            NETWORK_READY.replace("scope global", "scope global tentative"),
            NETWORK_READY.replace("dev eth0", "dev eth1"),
            NETWORK_READY.replace("via 192.168.64.1 ", ""),
            NETWORK_READY.replace("via 192.168.64.1", "via 0.0.0.0"),
            NETWORK_READY.replace("dev eth0", "dev eth0 linkdown"),
            NETWORK_READY.replace("default via 192.168.64.1 dev eth0\n", ""),
            NETWORK_READY.replace("addresses\n", ""),
        ] {
            assert!(!buildkit_network_sample_ready(&bad), "accepted {bad}");
        }
        // DHCP can successfully exit after obtaining a lease. Its process is
        // diagnostic, neither necessary nor sufficient to declare readiness.
        assert!(buildkit_network_sample_ready(&format!(
            "{NETWORK_READY}42\n"
        )));
        assert!(!buildkit_network_sample_ready(
            "addresses\nroutes\ndhcp_pids\n42\n"
        ));
    }

    #[tokio::test]
    async fn buildkit_network_readiness_offline_never_samples() {
        wait_for_buildkit_network(false, Duration::ZERO, |_| async {
            panic!("offline VM must not execute a network probe");
            #[allow(unreachable_code)]
            Ok::<_, String>(network_output(""))
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn buildkit_network_readiness_waits_through_dhcp_configuration() {
        let mut calls = 0;
        wait_for_buildkit_network(true, Duration::from_secs(3), |_| {
            calls += 1;
            let output = match calls {
                1 => network_output("addresses\n2: eth0: <UP> mtu 1500\nroutes\ndhcp_pids\n42\n"),
                2 => network_output(
                    &NETWORK_READY.replace("default via 192.168.64.1 dev eth0\n", ""),
                ),
                _ => network_output(NETWORK_READY),
            };
            async { Ok::<_, String>(output) }
        })
        .await
        .unwrap();
        assert_eq!(calls, 3);
    }

    #[tokio::test]
    async fn buildkit_network_readiness_deadline_retains_bounded_samples() {
        let output = format!("addresses\nroutes\ndhcp_pids\n42\n{}", "x".repeat(20_000));
        let error = wait_for_buildkit_network(true, Duration::from_millis(10), |_| {
            let output = network_output(&output);
            async { Ok::<_, String>(output) }
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("deadline expired"));
        assert!(error.contains("dhcp_pids"));
        assert!(error.contains("42"));
        assert!(error.contains("elapsed_ms="));
        assert!(error.len() < 5000);
    }

    #[tokio::test]
    async fn buildkit_network_readiness_bounds_hung_probe() {
        let mut calls = 0;
        let error = wait_for_buildkit_network(true, Duration::from_millis(10), |_| {
            calls += 1;
            std::future::pending::<Result<ExecOutput, String>>()
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("sample execution timed out"));
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn buildkit_network_readiness_command_and_transport_errors_fail_closed() {
        let error = wait_for_buildkit_network(true, Duration::from_secs(1), |_| async {
            let mut output = network_output(NETWORK_READY);
            output.exit_code = 1;
            output.stderr = "eth0 missing".to_string();
            Ok::<_, String>(output)
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("sample command failed"));
        assert!(error.contains("eth0 missing"));

        let error = wait_for_buildkit_network(true, Duration::from_secs(1), |_| async {
            Err::<ExecOutput, _>("checked exec stream truncated")
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("checked exec stream truncated"));
    }

    #[test]
    fn context_mount_compatibility_allows_cache_without_context() {
        let existing = PathBuf::from("/tmp/context-a");
        let requested = PathBuf::from("/tmp/context-b");
        assert!(context_mount_compatible(Some(existing.as_path()), None));
        assert!(context_mount_compatible(
            Some(existing.as_path()),
            Some(existing.as_path())
        ));
        assert!(!context_mount_compatible(
            Some(existing.as_path()),
            Some(requested.as_path())
        ));
        assert!(!context_mount_compatible(None, Some(existing.as_path())));
    }

    #[test]
    fn interrupted_vm_transition_fails_closed_without_stuck_boot_flag() {
        let manager = Arc::new(BuildkitVmManager::new());
        manager.state.lock().unwrap().boot_in_progress = true;
        drop(BuildkitVmTransitionGuard::new(Arc::clone(&manager), None));

        let state = manager.state.lock().unwrap();
        assert!(!state.boot_in_progress);
        assert!(state.transition_failure.is_some());
    }

    #[test]
    fn buildkit_config_normalizes_default_to_explicit_developer() {
        let implicit = RuntimeConfig::default();
        let explicit = RuntimeConfig {
            linux_profile: Some(KernelProfile::Developer),
            ..implicit.clone()
        };
        assert_eq!(
            normalize_buildkit_config(&implicit).unwrap(),
            normalize_buildkit_config(&explicit).unwrap()
        );
    }

    #[test]
    fn entitlement_error_detection_matches_known_signatures() {
        assert!(is_virtualization_entitlement_error(
            "Virtualization.framework error: VZErrorDomain:2"
        ));
        assert!(is_virtualization_entitlement_error(
            "missing com.apple.security.virtualization entitlement"
        ));
        assert!(!is_virtualization_entitlement_error(
            "generic guest-agent startup timeout"
        ));
    }

    #[test]
    fn entitlement_remediation_message_mentions_signing_paths() {
        let message = entitlement_remediation_message();
        assert!(message.contains("./scripts/sign-dev.sh"));
        assert!(message.contains("self-sign"));
    }

    #[test]
    fn guest_setup_is_fail_closed_and_uses_multicall_runtime() {
        let script = guest_buildkit_setup_script();

        assert!(script.contains("mount -t virtiofs linux-bin /mnt/linux-bin"));
        assert!(script.contains("[ ! -x /mnt/linux-bin/youki ]"));
        assert!(script.contains("runtime share must contain exactly"));
        assert!(script.contains("mount -t cgroup2 none /sys/fs/cgroup"));
        assert!(script.contains(" /sys/fs/cgroup cgroup2 "));
        assert!(script.contains("ln -sf /usr/bin/vz-guest-agent /tmp/vz-buildkit-oci-runtime"));
        assert!(script.contains("binary = \"/tmp/vz-buildkit-oci-runtime\""));
        assert!(script.contains("enabled = true"));
        assert!(script.contains("--oci-worker-binary /tmp/vz-buildkit-oci-runtime"));
        assert!(!script.contains("eval"));
        assert!(!script.contains("/bin/busybox basename"));
        assert!(!script.contains("mount -t virtiofs linux-bin /mnt/linux-bin 2>/dev/null || true"));
        assert!(!script.contains("mount -t cgroup2 none /sys/fs/cgroup 2>/dev/null || true"));
    }

    #[test]
    fn buildkit_kernel_requires_declared_youki_capabilities_for_every_profile_selection() {
        assert!(validate_declared_buildkit_kernel_capabilities(None).is_err());
        assert!(validate_declared_buildkit_kernel_capabilities(Some(&BTreeSet::new())).is_err());
        for incomplete in [
            [KernelCapability::CgroupBpf].into_iter().collect(),
            [KernelCapability::UserNs].into_iter().collect(),
        ] {
            assert!(validate_declared_buildkit_kernel_capabilities(Some(&incomplete)).is_err());
        }
        let capabilities = [KernelCapability::CgroupBpf, KernelCapability::UserNs]
            .into_iter()
            .collect();
        assert!(validate_declared_buildkit_kernel_capabilities(Some(&capabilities)).is_ok());
    }

    #[test]
    fn developer_profile_satisfies_buildkit_while_container_remains_hardened() {
        let developer = vz_linux::KernelProfile::Developer.default_capabilities();
        validate_declared_buildkit_kernel_capabilities(Some(&developer)).unwrap();

        let container = vz_linux::KernelProfile::Container.default_capabilities();
        let error = validate_declared_buildkit_kernel_capabilities(Some(&container)).unwrap_err();
        assert!(error.to_string().contains("user_ns"));
        assert!(!error.to_string().contains("cgroup_bpf"));
    }

    fn test_kernel_paths(youki: PathBuf, sha256_youki: Option<String>) -> KernelPaths {
        KernelPaths {
            kernel: PathBuf::from("vmlinux"),
            initramfs: PathBuf::from("initramfs.img"),
            youki,
            version: vz_linux::KernelVersion {
                kernel: "test".to_string(),
                profile: Some("developer".to_string()),
                security_profile: Some("developer-nested-virt".to_string()),
                busybox: "test".to_string(),
                agent: "test".to_string(),
                agent_protocol_revision: Some(1),
                youki: "test".to_string(),
                built: None,
                sha256_vmlinux: None,
                sha256_initramfs: None,
                sha256_youki,
                developer_probe: None,
                capabilities: Some(
                    [KernelCapability::CgroupBpf, KernelCapability::UserNs]
                        .into_iter()
                        .collect(),
                ),
            },
        }
    }

    fn sha256_bytes(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }

    #[tokio::test]
    async fn stages_only_selected_checksum_verified_youki() {
        let temp = tempdir().unwrap();
        let bundle = temp.path().join("bundle");
        std::fs::create_dir_all(bundle.join("container/tools")).unwrap();
        std::fs::write(bundle.join("youki"), b"selected-youki").unwrap();
        std::fs::write(bundle.join("container/youki"), b"other-youki").unwrap();
        std::fs::write(bundle.join("container/tools/runc"), b"runc").unwrap();
        std::fs::write(bundle.join("notes.txt"), b"unrelated").unwrap();
        let kernel = test_kernel_paths(bundle.join("youki"), Some(sha256_bytes(b"selected-youki")));

        let staged = stage_buildkit_runtime(&kernel, &temp.path().join("stage"))
            .await
            .unwrap();
        let entries = std::fs::read_dir(staged.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(entries, vec![OsString::from("youki")]);
        assert_eq!(
            std::fs::read(staged.path().join("youki")).unwrap(),
            b"selected-youki"
        );
        assert!(
            !std::fs::symlink_metadata(staged.path().join("youki"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[tokio::test]
    async fn runtime_staging_dereferences_source_symlink() {
        let temp = tempdir().unwrap();
        let target = temp.path().join("youki-real");
        let source = temp.path().join("youki-link");
        std::fs::write(&target, b"selected-youki").unwrap();
        symlink(&target, &source).unwrap();
        let kernel = test_kernel_paths(source, Some(sha256_bytes(b"selected-youki")));

        let staged = stage_buildkit_runtime(&kernel, &temp.path().join("stage"))
            .await
            .unwrap();
        assert!(
            !std::fs::symlink_metadata(staged.path().join("youki"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[tokio::test]
    async fn runtime_staging_requires_matching_checksum_metadata() {
        let temp = tempdir().unwrap();
        let source = temp.path().join("youki");
        std::fs::write(&source, b"selected-youki").unwrap();

        let missing = test_kernel_paths(source.clone(), None);
        assert!(
            stage_buildkit_runtime(&missing, &temp.path().join("missing"))
                .await
                .unwrap_err()
                .to_string()
                .contains("sha256_youki")
        );

        let mismatch = test_kernel_paths(source, Some(sha256_bytes(b"different")));
        assert!(
            stage_buildkit_runtime(&mismatch, &temp.path().join("mismatch"))
                .await
                .unwrap_err()
                .to_string()
                .contains("checksum mismatch")
        );
    }

    #[tokio::test]
    async fn buildkit_rejects_container_profile_before_artifact_resolution() {
        let config = RuntimeConfig {
            linux_profile: Some(KernelProfile::Container),
            ..RuntimeConfig::default()
        };
        let error = ensure_buildkit_kernel_for_config(&config)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("developer Linux profile"));
    }

    #[test]
    fn runtime_inventory_parser_keeps_path_evidence() {
        let output = concat!(
            "oci_worker_binary=/tmp/vz-buildkit-oci-runtime\n",
            "shim_target=/usr/bin/vz-guest-agent\n",
            "runtime_binary=/mnt/linux-bin/youki\n",
            "observed_runtime_path=/mnt/linux-bin/youki\n",
            "observed_oci_subcommand=create\n",
            "observed_oci_subcommand=delete\n",
            "oci_runtime_elf_path=/mnt/linux-bin/youki\n",
            "runtime_version=youki 0.5.5\n",
            "buildkitd_executable=/mnt/buildkit-bin/buildkitd\n",
            "buildkitd_oci_worker_binary=/tmp/vz-buildkit-oci-runtime\n",
            "cgroup_filesystem=cgroup2\n",
        );

        let inventory = parse_buildkit_runtime_inventory(output).unwrap();
        assert_eq!(
            inventory.oci_runtime_elf_paths,
            vec!["/mnt/linux-bin/youki"]
        );
        assert!(inventory.forbidden_runtime_paths.is_empty());
        assert_eq!(inventory.observed_oci_subcommands, vec!["create", "delete"]);
        assert_eq!(inventory.shim_target, "/usr/bin/vz-guest-agent");
        assert_eq!(inventory.cgroup_filesystem, "cgroup2");
        validate_buildkit_runtime_inventory(&inventory).unwrap();
    }

    #[test]
    fn runtime_inventory_parser_reports_forbidden_paths() {
        let output = concat!(
            "oci_worker_binary=/tmp/vz-buildkit-oci-runtime\n",
            "shim_target=/usr/bin/vz-guest-agent\n",
            "runtime_binary=/mnt/linux-bin/youki\n",
            "observed_runtime_path=/mnt/linux-bin/youki\n",
            "observed_oci_subcommand=run\n",
            "oci_runtime_elf_path=/mnt/linux-bin/youki\n",
            "forbidden_runtime_path=/tmp/runc-real\n",
            "forbidden_runtime_path=/mnt/buildkit-bin/buildkit-runc\n",
            "runtime_version=youki 0.5.5\n",
            "buildkitd_executable=/mnt/buildkit-bin/buildkitd\n",
            "buildkitd_oci_worker_binary=/tmp/vz-buildkit-oci-runtime\n",
            "cgroup_filesystem=cgroup2\n",
        );

        let inventory = parse_buildkit_runtime_inventory(output).unwrap();
        assert_eq!(
            inventory.forbidden_runtime_paths,
            vec!["/tmp/runc-real", "/mnt/buildkit-bin/buildkit-runc"]
        );
        assert!(validate_buildkit_runtime_inventory(&inventory).is_err());
    }

    #[test]
    fn runtime_inventory_requires_an_observed_create_or_run() {
        let output = concat!(
            "oci_worker_binary=/tmp/vz-buildkit-oci-runtime\n",
            "shim_target=/usr/bin/vz-guest-agent\n",
            "runtime_binary=/mnt/linux-bin/youki\n",
            "observed_runtime_path=/mnt/linux-bin/youki\n",
            "observed_oci_subcommand=delete\n",
            "oci_runtime_elf_path=/mnt/linux-bin/youki\n",
            "runtime_version=youki 0.5.5\n",
            "buildkitd_executable=/mnt/buildkit-bin/buildkitd\n",
            "buildkitd_oci_worker_binary=/tmp/vz-buildkit-oci-runtime\n",
            "cgroup_filesystem=cgroup2\n",
        );

        let inventory = parse_buildkit_runtime_inventory(output).unwrap();
        assert!(validate_buildkit_runtime_inventory(&inventory).is_err());
    }

    #[tokio::test]
    async fn prepare_output_artifact_uses_shared_output_root() {
        let temp = tempdir().unwrap();
        let shared_mounts = BuildkitSharedMounts {
            output_root: temp.path().join("output"),
            auth_dir: temp.path().join("auth"),
        };
        tokio::fs::create_dir_all(&shared_mounts.output_root)
            .await
            .unwrap();
        tokio::fs::create_dir_all(&shared_mounts.auth_dir)
            .await
            .unwrap();

        let artifact = prepare_output_artifact(&BuildOutput::VzStore, &shared_mounts)
            .await
            .unwrap()
            .unwrap();
        assert!(artifact.cleanup_dir.starts_with(&shared_mounts.output_root));
        assert_eq!(
            artifact.host_tar_path,
            artifact.cleanup_dir.join(BUILD_OUTPUT_ARCHIVE)
        );
        assert!(artifact.guest_tar_path.starts_with("/mnt/build-output/"));
    }
}
