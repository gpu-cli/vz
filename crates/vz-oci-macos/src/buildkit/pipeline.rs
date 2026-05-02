//! Build pipeline execution against the guest BuildKit daemon.
//!
//! Ordering invariants:
//! - Streamed solve/output events are forwarded in receive order.
//! - `buildctl` raw-json decode callbacks are emitted before terminal status handling.

use std::collections::{BTreeMap, BTreeSet};
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
use tracing::warn;
use vz::NetworkConfig;
use vz::SharedDirConfig;
use vz::protocol::{ExecEvent, ExecOutput};
use vz_image::ImageStore;
use vz_linux::{LinuxVm, LinuxVmConfig};

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
    BUILDKIT_RUNC_GUEST_PATH, BUILDKIT_SETUP_TIMEOUT, BUILDKIT_SHUTDOWN_TIMEOUT,
    BUILDKIT_SNAPSHOTTER, BUILDKIT_VM_MEMORY_MB, BUILDKITD_ADDR, BuildEvent, BuildLogStream,
    BuildOutput, BuildProgress, BuildRequest, BuildResult, BuildkitError, CachePruneOptions,
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
    Replace(Arc<LinuxVm>),
    Wait,
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
        let requested_context = context_dir.map(Path::to_path_buf);

        loop {
            let action = {
                let mut state = self.lock_state()?;
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
                        state.managed = None;
                        BuildkitVmAcquireAction::Replace(existing_vm)
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
                BuildkitVmAcquireAction::Replace(old_vm) => {
                    if let Err(error) = shutdown_managed_vm(old_vm.as_ref()).await {
                        let mut state = self.lock_state()?;
                        state.boot_in_progress = false;
                        return Err(error);
                    }
                    let vm = match start_buildkit_vm(
                        config,
                        context_dir,
                        &shared_mounts.output_root,
                        &shared_mounts.auth_dir,
                    )
                    .await
                    {
                        Ok(vm) => Arc::new(vm),
                        Err(error) => {
                            let mut state = self.lock_state()?;
                            state.boot_in_progress = false;
                            return Err(error);
                        }
                    };
                    let mut state = self.lock_state()?;
                    state.boot_in_progress = false;
                    state.managed = Some(ManagedBuildkitVm {
                        vm: Arc::clone(&vm),
                        config: config.clone(),
                        context_dir: requested_context.clone(),
                        output_root: shared_mounts.output_root.clone(),
                        auth_dir: shared_mounts.auth_dir.clone(),
                    });
                    state.active_leases = 1;
                    state.activity_generation = state.activity_generation.saturating_add(1);
                    state.last_activity = Instant::now();
                    return Ok(BuildkitVmLease {
                        manager: Arc::clone(self),
                        vm,
                    });
                }
                BuildkitVmAcquireAction::Boot => {
                    let vm = match start_buildkit_vm(
                        config,
                        context_dir,
                        &shared_mounts.output_root,
                        &shared_mounts.auth_dir,
                    )
                    .await
                    {
                        Ok(vm) => Arc::new(vm),
                        Err(error) => {
                            let mut state = self.lock_state()?;
                            state.boot_in_progress = false;
                            return Err(error);
                        }
                    };
                    let mut state = self.lock_state()?;
                    state.boot_in_progress = false;
                    state.managed = Some(ManagedBuildkitVm {
                        vm: Arc::clone(&vm),
                        config: config.clone(),
                        context_dir: requested_context.clone(),
                        output_root: shared_mounts.output_root.clone(),
                        auth_dir: shared_mounts.auth_dir.clone(),
                    });
                    state.active_leases = 1;
                    state.activity_generation = state.activity_generation.saturating_add(1);
                    state.last_activity = Instant::now();
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

    fn try_idle_shutdown(&self, generation: u64) {
        let vm_to_shutdown = {
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
            state.managed.take().map(|managed| Arc::clone(&managed.vm))
        };

        if let Some(vm) = vm_to_shutdown
            && let Err(error) = block_on_vm_shutdown(vm.as_ref())
        {
            warn!(%error, "failed to shutdown idle BuildKit VM");
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

fn block_on_vm_shutdown(vm: &LinuxVm) -> Result<(), BuildkitError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(BuildkitError::Io)?;
    runtime.block_on(async { shutdown_managed_vm(vm).await })
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

/// Prune BuildKit cache and return command output summary.
pub async fn cache_prune(
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

async fn start_buildkit_vm(
    config: &RuntimeConfig,
    context_dir: Option<&Path>,
    output_root: &Path,
    auth_dir: &Path,
) -> Result<LinuxVm, BuildkitError> {
    ensure_virtualization_entitlement_preflight()?;

    let artifacts = ensure_buildkit_artifacts().await?;
    let kernel = ensure_kernel_for_config(config).await?;
    let linux_bin_dir = kernel.youki.parent().map(Path::to_path_buf);

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

    if let Some(linux_install_dir) = &config.linux_install_dir {
        vm_config.shared_dirs.push(SharedDirConfig {
            tag: "linux-bin".to_string(),
            source: expand_home_dir(linux_install_dir),
            read_only: true,
        });
    } else if let Some(kernel_dir) = linux_bin_dir {
        vm_config.shared_dirs.push(SharedDirConfig {
            tag: "linux-bin".to_string(),
            source: kernel_dir,
            read_only: true,
        });
    }

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

    let vm = LinuxVm::create(vm_config)
        .await
        .map_err(BuildkitError::from)
        .map_err(map_vm_boot_error)?;
    vm.start()
        .await
        .map_err(BuildkitError::from)
        .map_err(map_vm_boot_error)?;

    if let Err(err) = vm.wait_for_agent(config.agent_ready_timeout).await {
        let _ = vm.stop().await;
        return Err(err.into());
    }

    Ok(vm)
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
      exit 0
    fi
    i=$((i + 1))
    /bin/busybox sleep 1
  done
  /bin/busybox kill -9 "$pid" 2>/dev/null || true
fi
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
    let setup_script = format!(
        r#"
set -eu

/bin/busybox mkdir -p /mnt/buildkit-bin /mnt/linux-bin /var/lib/buildkit /mnt/build-context /mnt/build-output /mnt/host-ssl {BUILDKIT_AUTH_GUEST_DIR}
/bin/busybox mkdir -p /etc/buildkit
if ! /bin/busybox grep -q " /mnt/buildkit-bin " /proc/mounts; then
  /bin/busybox mount -t virtiofs buildkit-bin /mnt/buildkit-bin
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
/bin/busybox mount -t virtiofs linux-bin /mnt/linux-bin 2>/dev/null || true
/bin/busybox mount -t virtiofs build-context /mnt/build-context 2>/dev/null || true
/bin/busybox mount -t virtiofs build-output /mnt/build-output 2>/dev/null || true
/bin/busybox mount -t virtiofs host-ssl /mnt/host-ssl 2>/dev/null || true
/bin/busybox mount -t virtiofs {BUILDKIT_AUTH_TAG} {BUILDKIT_AUTH_GUEST_DIR} 2>/dev/null || true
/bin/busybox mkdir -p /sys/fs/cgroup
/bin/busybox mount -t cgroup2 none /sys/fs/cgroup 2>/dev/null || true

/bin/busybox cp /mnt/buildkit-bin/buildkit-runc /tmp/runc-real
/bin/busybox cat >{BUILDKIT_RUNC_GUEST_PATH} <<'RUNC'
#!/bin/sh
set -eu

new_args=""
inserted=0
for arg in "$@"; do
  escaped=$(/bin/busybox sed "s/'/'\\\\''/g" <<EOF
$arg
EOF
)
  new_args="$new_args '$escaped'"
  if [ "$inserted" -eq 0 ] && {{ [ "$arg" = "run" ] || [ "$arg" = "create" ]; }}; then
    new_args="$new_args '--no-pivot'"
    inserted=1
  fi
done

eval "exec /tmp/runc-real $new_args"
RUNC
/bin/busybox chmod 0755 {BUILDKIT_RUNC_GUEST_PATH} /tmp/runc-real
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
  binary = "{BUILDKIT_RUNC_GUEST_PATH}"
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
    --oci-worker-binary {BUILDKIT_RUNC_GUEST_PATH} \
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
    );

    run_guest_command(
        vm,
        "setup buildkit guest environment",
        "/bin/busybox",
        vec!["sh".to_string(), "-c".to_string(), setup_script],
        BUILDKIT_SETUP_TIMEOUT,
    )
    .await
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

    use tempfile::tempdir;

    use super::*;

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
