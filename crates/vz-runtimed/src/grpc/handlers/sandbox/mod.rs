//! Sandbox gRPC handler support code shared by sandbox endpoint RPC methods.
//!
//! Consolidates shell session helpers, sandbox lifecycle validation, and response
//! mapping used by `sandbox::rpc` endpoint implementations.

use super::super::*;
use crate::btrfs_portability::{export_subvolume_send_stream, import_subvolume_receive_stream};
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::process::Command;
use vz_runtime_contract::{
    RuntimeBackend, SPACE_CACHE_KEY_SCHEMA_VERSION, SpaceCacheIndex, SpaceCacheKey,
    SpaceCacheLookup, SpaceRemoteCacheTrustConfig, SpaceRemoteCacheVerificationOutcome,
    SpaceRemoteCacheVerifiedArtifact, StackResourceHint, StackVolumeMount,
};
use vz_runtime_proto::runtime_v2::container_service_server::ContainerService as _;
use vz_runtime_proto::runtime_v2::execution_service_server::ExecutionService as _;

#[derive(Clone)]
pub(in crate::grpc) struct SandboxServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl SandboxServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

type OpenSandboxShellEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::OpenSandboxShellEvent, Status>>;
type CloseSandboxShellEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::CloseSandboxShellEvent, Status>>;
type CreateSandboxEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::CreateSandboxEvent, Status>>;
type PrepareSpaceCacheEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::PrepareSpaceCacheEvent, Status>>;
type ExportSpaceCacheEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::ExportSpaceCacheEvent, Status>>;
type ImportSpaceCacheEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::ImportSpaceCacheEvent, Status>>;
type TerminateSandboxEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::TerminateSandboxEvent, Status>>;

const SPACE_CACHE_INDEX_FILE: &str = "space-cache-index.json";
const SPACE_CACHE_ARTIFACTS_DIR: &str = "space-cache-artifacts";

#[derive(Debug, serde::Serialize)]
struct PrepareSpaceCacheReceiptMetadata {
    event_type: &'static str,
    prepared: usize,
    remote_verified_materialized: usize,
    remote_miss_untrusted: usize,
}

fn sandbox_exec_control_debug_enabled() -> bool {
    std::env::var("VZ_RUNTIMED_EXEC_CONTROL_DEBUG")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn sandbox_shell_stream_from_events<T>(
    events: Vec<Result<T, Status>>,
) -> tokio_stream::wrappers::ReceiverStream<Result<T, Status>>
where
    T: Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(events.len().max(1));
    for event in events {
        if tx.try_send(event).is_err() {
            break;
        }
    }
    drop(tx);
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

fn sandbox_stream_response<T>(
    events: Vec<Result<T, Status>>,
    receipt_id: Option<&str>,
) -> Response<tokio_stream::wrappers::ReceiverStream<Result<T, Status>>>
where
    T: Send + 'static,
{
    let mut response = Response::new(sandbox_shell_stream_from_events(events));
    if let Some(receipt_id) = receipt_id
        && !receipt_id.trim().is_empty()
        && let Ok(value) = MetadataValue::try_from(receipt_id)
    {
        response.metadata_mut().insert("x-receipt-id", value);
    }
    response
}

fn create_sandbox_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::CreateSandboxEvent {
    runtime_v2::CreateSandboxEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::create_sandbox_event::Payload::Progress(
            runtime_v2::SandboxLifecycleProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn create_sandbox_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::SandboxResponse,
    receipt_id: &str,
) -> runtime_v2::CreateSandboxEvent {
    runtime_v2::CreateSandboxEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::create_sandbox_event::Payload::Completion(
            runtime_v2::CreateSandboxCompletion {
                response: Some(response),
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

fn prepare_space_cache_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::PrepareSpaceCacheEvent {
    runtime_v2::PrepareSpaceCacheEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::prepare_space_cache_event::Payload::Progress(
            runtime_v2::PrepareSpaceCacheProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn prepare_space_cache_completion_event(
    request_id: &str,
    sequence: u64,
    outcomes: Vec<runtime_v2::SpaceCacheOutcomePayload>,
    receipt_id: &str,
) -> runtime_v2::PrepareSpaceCacheEvent {
    runtime_v2::PrepareSpaceCacheEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::prepare_space_cache_event::Payload::Completion(
            runtime_v2::PrepareSpaceCacheCompletion {
                request_id: request_id.to_string(),
                outcomes,
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

fn export_space_cache_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::ExportSpaceCacheEvent {
    runtime_v2::ExportSpaceCacheEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::export_space_cache_event::Payload::Progress(
            runtime_v2::SpaceCachePortabilityProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn export_space_cache_completion_event(
    request_id: &str,
    sequence: u64,
    cache_name: &str,
    digest_hex: &str,
    stream_path: &str,
) -> runtime_v2::ExportSpaceCacheEvent {
    runtime_v2::ExportSpaceCacheEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::export_space_cache_event::Payload::Completion(
            runtime_v2::ExportSpaceCacheCompletion {
                cache_name: cache_name.to_string(),
                digest_hex: digest_hex.to_string(),
                stream_path: stream_path.to_string(),
            },
        )),
    }
}

fn import_space_cache_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::ImportSpaceCacheEvent {
    runtime_v2::ImportSpaceCacheEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::import_space_cache_event::Payload::Progress(
            runtime_v2::SpaceCachePortabilityProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn import_space_cache_completion_event(
    request_id: &str,
    sequence: u64,
    cache_name: &str,
    digest_hex: &str,
    received_subvolume_path: &str,
    receipt_id: &str,
) -> runtime_v2::ImportSpaceCacheEvent {
    runtime_v2::ImportSpaceCacheEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::import_space_cache_event::Payload::Completion(
            runtime_v2::ImportSpaceCacheCompletion {
                cache_name: cache_name.to_string(),
                digest_hex: digest_hex.to_string(),
                received_subvolume_path: received_subvolume_path.to_string(),
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

fn terminate_sandbox_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::TerminateSandboxEvent {
    runtime_v2::TerminateSandboxEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::terminate_sandbox_event::Payload::Progress(
            runtime_v2::SandboxLifecycleProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn terminate_sandbox_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::SandboxResponse,
    receipt_id: &str,
) -> runtime_v2::TerminateSandboxEvent {
    runtime_v2::TerminateSandboxEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::terminate_sandbox_event::Payload::Completion(
            runtime_v2::TerminateSandboxCompletion {
                response: Some(response),
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

fn open_sandbox_shell_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::OpenSandboxShellEvent {
    runtime_v2::OpenSandboxShellEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::open_sandbox_shell_event::Payload::Progress(
            runtime_v2::SandboxShellProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn open_sandbox_shell_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::OpenSandboxShellResponse,
) -> runtime_v2::OpenSandboxShellEvent {
    runtime_v2::OpenSandboxShellEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::open_sandbox_shell_event::Payload::Completion(
            response,
        )),
    }
}

fn close_sandbox_shell_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::CloseSandboxShellEvent {
    runtime_v2::CloseSandboxShellEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::close_sandbox_shell_event::Payload::Progress(
            runtime_v2::SandboxShellProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn close_sandbox_shell_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::CloseSandboxShellResponse,
) -> runtime_v2::CloseSandboxShellEvent {
    runtime_v2::CloseSandboxShellEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::close_sandbox_shell_event::Payload::Completion(
            response,
        )),
    }
}

fn daemon_space_cache_index_path(daemon: &RuntimeDaemon) -> PathBuf {
    daemon
        .state_store_path()
        .parent()
        .map(|parent| parent.join(SPACE_CACHE_INDEX_FILE))
        .unwrap_or_else(|| PathBuf::from(SPACE_CACHE_INDEX_FILE))
}

fn daemon_space_cache_artifact_dir(daemon: &RuntimeDaemon, key: &SpaceCacheKey) -> PathBuf {
    daemon
        .state_store_path()
        .parent()
        .map(|parent| {
            parent
                .join(SPACE_CACHE_ARTIFACTS_DIR)
                .join(&key.cache_name)
                .join(&key.digest_hex)
        })
        .unwrap_or_else(|| {
            PathBuf::from(SPACE_CACHE_ARTIFACTS_DIR)
                .join(&key.cache_name)
                .join(&key.digest_hex)
        })
}

fn daemon_space_cache_artifact_dir_for_identity(
    daemon: &RuntimeDaemon,
    cache_name: &str,
    digest_hex: &str,
) -> PathBuf {
    daemon
        .state_store_path()
        .parent()
        .map(|parent| {
            parent
                .join(SPACE_CACHE_ARTIFACTS_DIR)
                .join(cache_name)
                .join(digest_hex)
        })
        .unwrap_or_else(|| {
            PathBuf::from(SPACE_CACHE_ARTIFACTS_DIR)
                .join(cache_name)
                .join(digest_hex)
        })
}

fn daemon_materialize_verified_remote_cache_artifact(
    daemon: &RuntimeDaemon,
    key: &SpaceCacheKey,
    artifact: &SpaceRemoteCacheVerifiedArtifact,
) -> Result<PathBuf, Status> {
    let target_dir = daemon_space_cache_artifact_dir(daemon, key);
    ensure_cache_artifact_directory_layout(&target_dir)?;
    let target_manifest = target_dir.join("manifest.json");
    let target_signature = target_dir.join("signature.sig");
    let target_blob = target_dir.join("payload.tar.zst");
    std::fs::copy(&artifact.manifest_path, &target_manifest).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "failed to materialize manifest {} -> {}: {error}",
                artifact.manifest_path.display(),
                target_manifest.display()
            ),
            None,
            BTreeMap::new(),
        ))
    })?;
    std::fs::copy(&artifact.signature_path, &target_signature).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "failed to materialize signature {} -> {}: {error}",
                artifact.signature_path.display(),
                target_signature.display()
            ),
            None,
            BTreeMap::new(),
        ))
    })?;
    std::fs::copy(&artifact.blob_path, &target_blob).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "failed to materialize payload {} -> {}: {error}",
                artifact.blob_path.display(),
                target_blob.display()
            ),
            None,
            BTreeMap::new(),
        ))
    })?;
    Ok(target_blob)
}

#[cfg(not(target_os = "linux"))]
fn ensure_cache_artifact_directory_layout(target_dir: &Path) -> Result<(), Status> {
    let request_id = "req-space-cache-materialize-layout";
    Err(status_from_machine_error(MachineError::new(
        MachineErrorCode::UnsupportedOperation,
        format!(
            "spaces cache materialization requires Linux btrfs storage; current platform `{}` is unsupported for {}",
            std::env::consts::OS,
            target_dir.display()
        ),
        Some(request_id.to_string()),
        BTreeMap::new(),
    )))
}

#[cfg(target_os = "linux")]
fn ensure_cache_artifact_directory_layout(target_dir: &Path) -> Result<(), Status> {
    let request_id = "req-space-cache-materialize-layout";
    let parent = target_dir.parent().ok_or_else(|| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "cache artifact target path has no parent directory: {}",
                target_dir.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    std::fs::create_dir_all(parent).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "failed to create cache artifact parent directory {}: {error}",
                parent.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    let parent_on_btrfs = path_is_on_btrfs(parent, request_id)?;
    if !parent_on_btrfs {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::UnsupportedOperation,
            format!(
                "spaces cache artifacts require btrfs-backed daemon state storage; `{}` is not on btrfs",
                parent.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    if target_dir.exists() {
        let output = std::process::Command::new("btrfs")
            .args(["subvolume", "show", target_dir.to_string_lossy().as_ref()])
            .output()
            .map_err(|error| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::BackendUnavailable,
                    format!("failed to inspect cache artifact subvolume layout: {error}"),
                    Some(request_id.to_string()),
                    BTreeMap::new(),
                ))
            })?;
        if output.status.success() {
            return Ok(());
        }
        std::fs::remove_dir_all(target_dir).map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::InternalError,
                format!(
                    "failed to clear non-subvolume cache artifact directory {}: {error}",
                    target_dir.display()
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        })?;
    }

    let output = std::process::Command::new("btrfs")
        .args(["subvolume", "create", target_dir.to_string_lossy().as_ref()])
        .output()
        .map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::BackendUnavailable,
                format!("failed to create cache artifact subvolume: {error}"),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::BackendUnavailable,
            format!(
                "btrfs subvolume create failed for {}: {}",
                target_dir.display(),
                stderr.trim()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    Ok(())
}

async fn terminate_runtime_sandbox_resources(
    daemon: Arc<RuntimeDaemon>,
    sandbox_id: &str,
    request_id: &str,
) -> Result<(), Status> {
    let sandbox_id_owned = sandbox_id.to_string();
    let bridge_result = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("failed to initialize runtime bridge: {error}"))?;
        Ok::<_, String>(runtime.block_on(daemon.manager().terminate_sandbox(&sandbox_id_owned)))
    })
    .await
    .map_err(|join_error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "runtime bridge join failure while terminating sandbox {sandbox_id}: {join_error}"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    let runtime_result = bridge_result.map_err(|bridge_error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "runtime bridge initialization failed while terminating sandbox {sandbox_id}: {bridge_error}"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    match runtime_result {
        Ok(()) => Ok(()),
        Err(error) if runtime_shutdown_error_is_not_active(&error, sandbox_id) => Ok(()),
        Err(error) => Err(status_from_machine_error(MachineError::new(
            error.machine_code(),
            format!("failed to terminate runtime resources for sandbox {sandbox_id}: {error}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))),
    }
}

fn sandbox_workspace_volume_mount(
    labels: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<Option<StackVolumeMount>, Status> {
    validate_spaces_mode_label(labels, request_id)?;
    let Some(project_dir) = labels
        .get(SANDBOX_LABEL_PROJECT_DIR)
        .and_then(|value| normalize_optional_wire_field(value))
    else {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "spaces mode requires sandbox label `{SANDBOX_LABEL_PROJECT_DIR}` with an absolute workspace directory path"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    };

    let host_path = PathBuf::from(project_dir.trim());
    if !host_path.is_absolute() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "sandbox label `{SANDBOX_LABEL_PROJECT_DIR}` must be an absolute path: {}",
                host_path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    if !host_path.exists() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "sandbox label `{SANDBOX_LABEL_PROJECT_DIR}` does not exist: {}",
                host_path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    if !host_path.is_dir() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "sandbox label `{SANDBOX_LABEL_PROJECT_DIR}` must reference a directory: {}",
                host_path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    enforce_spaces_workspace_storage_preflight(&host_path, request_id)?;

    Ok(Some(StackVolumeMount {
        tag: "vz-mount-0".to_string(),
        host_path,
        read_only: false,
    }))
}

fn validate_spaces_mode_label(
    labels: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<(), Status> {
    let mode = labels
        .get(SANDBOX_LABEL_SPACE_MODE)
        .and_then(|value| normalize_optional_wire_field(value));
    if let Some(mode) = mode
        && !mode.eq_ignore_ascii_case(SANDBOX_SPACE_MODE_REQUIRED)
    {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "unsupported sandbox label `{SANDBOX_LABEL_SPACE_MODE}` value `{mode}`; only `{SANDBOX_SPACE_MODE_REQUIRED}` is supported"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    Ok(())
}

fn enforce_spaces_workspace_storage_preflight(
    host_path: &Path,
    request_id: &str,
) -> Result<(), Status> {
    #[cfg(target_os = "linux")]
    {
        if path_is_on_btrfs(host_path, request_id)? {
            return Ok(());
        }
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::UnsupportedOperation,
            format!(
                "spaces mode requires btrfs workspace storage; `{}` is not on btrfs",
                host_path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    #[cfg(not(target_os = "linux"))]
    {
        Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::UnsupportedOperation,
            format!(
                "spaces mode requires Linux btrfs workspace storage; current platform `{}` is unsupported for workspace `{}`",
                std::env::consts::OS,
                host_path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )))
    }
}

#[cfg(target_os = "linux")]
fn path_is_on_btrfs(path: &Path, request_id: &str) -> Result<bool, Status> {
    let canonical = std::fs::canonicalize(path).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::BackendUnavailable,
            format!(
                "failed to resolve workspace path {} during btrfs preflight: {error}",
                path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    let fs_type = detect_filesystem_type(&canonical).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::BackendUnavailable,
            format!(
                "failed to inspect workspace filesystem for {}: {}",
                canonical.display(),
                error
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    Ok(fs_type == "btrfs")
}

#[cfg(target_os = "linux")]
fn detect_filesystem_type(path: &Path) -> std::io::Result<String> {
    let findmnt_output = Command::new("findmnt")
        .arg("-n")
        .arg("-T")
        .arg(path)
        .arg("-o")
        .arg("FSTYPE")
        .output();

    if let Ok(output) = findmnt_output
        && output.status.success()
    {
        let fs_type = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !fs_type.is_empty() {
            return Ok(fs_type);
        }
    }

    let stat_output = Command::new("stat")
        .arg("-f")
        .arg("-c")
        .arg("%T")
        .arg(path)
        .output()?;
    if !stat_output.status.success() {
        let stderr = String::from_utf8_lossy(&stat_output.stderr)
            .trim()
            .to_string();
        return Err(std::io::Error::other(format!(
            "stat filesystem probe failed: {stderr}"
        )));
    }
    let fs_type = String::from_utf8_lossy(&stat_output.stdout)
        .trim()
        .to_string();
    if fs_type.is_empty() {
        return Err(std::io::Error::other(
            "filesystem probe returned empty type",
        ));
    }
    Ok(fs_type)
}

async fn boot_runtime_sandbox_resources(
    daemon: Arc<RuntimeDaemon>,
    sandbox_id: &str,
    cpus: Option<u8>,
    memory_mb: Option<u64>,
    labels: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<(), Status> {
    let mut volume_mounts = Vec::new();
    if let Some(workspace_mount) = sandbox_workspace_volume_mount(labels, request_id)? {
        volume_mounts.push(workspace_mount);
    }

    let resources = StackResourceHint {
        cpus,
        memory_mb,
        volume_mounts,
        disk_image_path: None,
    };

    match daemon
        .manager()
        .backend()
        .boot_shared_vm(sandbox_id, Vec::new(), resources)
        .await
    {
        Ok(()) => Ok(()),
        Err(error) => Err(status_from_machine_error(MachineError::new(
            error.machine_code(),
            format!("failed to boot runtime resources for sandbox {sandbox_id}: {error}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))),
    }
}

fn runtime_shutdown_error_is_not_active(
    error: &vz_runtime_contract::RuntimeError,
    sandbox_id: &str,
) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    let sandbox_id_lc = sandbox_id.to_ascii_lowercase();

    matches!(
        error,
        vz_runtime_contract::RuntimeError::UnsupportedOperation { .. }
    ) || message.contains("no shared vm running")
        && message.contains("stack")
        && message.contains(&sandbox_id_lc)
        || message.contains("stack")
            && message.contains("not found")
            && message.contains(&sandbox_id_lc)
        || message.contains("not booted")
}

fn default_keepalive_container_cmd() -> Vec<String> {
    vec![
        "/bin/sh".to_string(),
        "-lc".to_string(),
        "while :; do sleep 3600; done".to_string(),
    ]
}

fn default_shell_for_base_image(base_image_ref: Option<&str>) -> &'static str {
    let Some(base_image_ref) = base_image_ref else {
        return "/bin/sh";
    };
    let normalized = base_image_ref.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return "/bin/sh";
    }

    if [
        "ubuntu", "debian", "fedora", "centos", "rocky", "alma", "arch",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
    {
        "/bin/bash"
    } else {
        "/bin/sh"
    }
}

#[cfg(all(test, not(target_os = "linux")))]
mod tests {
    use super::ensure_cache_artifact_directory_layout;

    #[test]
    fn cache_artifact_layout_rejects_non_linux_platforms() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let target = tmp
            .path()
            .join("space-cache-artifacts")
            .join("deps")
            .join("abc123");
        let status = ensure_cache_artifact_directory_layout(&target)
            .expect_err("non-linux cache layout should fail closed");
        assert!(
            status.message().contains("requires Linux btrfs storage"),
            "status should explain Linux+btrfs requirement: {}",
            status.message()
        );
    }
}

fn parse_main_container_startup_command(
    request_id: &str,
    main_container: &str,
) -> Result<Option<(String, Vec<String>)>, Status> {
    let command_hint = main_container.trim();
    if command_hint.is_empty() {
        return Ok(None);
    }

    let looks_like_command = command_hint.contains(char::is_whitespace)
        || command_hint.starts_with('/')
        || command_hint.contains('/')
        || matches!(command_hint, "sh" | "bash" | "zsh" | "fish" | "nu");
    if !looks_like_command {
        return Ok(None);
    }

    let words = shell_words::split(command_hint).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!("invalid sandbox main_container command: {error}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    if words.is_empty() {
        return Ok(None);
    }

    let mut words = words.into_iter();
    let command = match words.next() {
        Some(command) => command,
        None => return Ok(None),
    };
    let args = words.collect();
    Ok(Some((command, args)))
}

fn resolve_sandbox_shell_command(
    request_id: &str,
    sandbox: &Sandbox,
) -> Result<(String, Vec<String>), Status> {
    let debug = sandbox_exec_control_debug_enabled();
    let main_container_hint = sandbox
        .spec
        .main_container
        .as_deref()
        .and_then(normalize_optional_wire_field)
        .or_else(|| {
            sandbox
                .labels
                .get(SANDBOX_LABEL_MAIN_CONTAINER)
                .and_then(|value| normalize_optional_wire_field(value))
        });

    if let Some(main_container) = main_container_hint
        && let Some((command, args)) =
            parse_main_container_startup_command(request_id, &main_container)?
    {
        if debug {
            eprintln!(
                "[vz-runtimed exec-control] resolved sandbox shell command from main_container sandbox_id={} request_id={} command={:?} args={:?}",
                sandbox.sandbox_id, request_id, command, args
            );
        }
        return Ok((command, args));
    }

    let base_image_ref = sandbox
        .spec
        .base_image_ref
        .as_deref()
        .and_then(normalize_optional_wire_field)
        .or_else(|| {
            sandbox
                .labels
                .get(SANDBOX_LABEL_BASE_IMAGE_REF)
                .and_then(|value| normalize_optional_wire_field(value))
        });
    let command = default_shell_for_base_image(base_image_ref.as_deref()).to_string();
    if debug {
        eprintln!(
            "[vz-runtimed exec-control] resolved sandbox shell command from base_image sandbox_id={} request_id={} base_image_ref={:?} command={:?}",
            sandbox.sandbox_id, request_id, base_image_ref, command
        );
    }
    Ok((command, Vec::new()))
}

fn find_attachable_sandbox_container(
    daemon: &RuntimeDaemon,
    sandbox_id: &str,
    request_id: &str,
) -> Result<Option<Container>, Status> {
    let mut containers = daemon
        .with_state_store(|store| store.list_containers())
        .map_err(|error| status_from_stack_error(error, request_id))?
        .into_iter()
        .filter(|container| container.sandbox_id == sandbox_id && !container.state.is_terminal())
        .collect::<Vec<_>>();
    containers.sort_by_key(|container| container.created_at);
    Ok(containers.pop())
}

fn sandbox_shell_image_ref(request_id: &str, sandbox: &Sandbox) -> Result<String, Status> {
    sandbox
        .spec
        .base_image_ref
        .as_deref()
        .and_then(normalize_optional_wire_field)
        .or_else(|| {
            sandbox
                .labels
                .get(SANDBOX_LABEL_BASE_IMAGE_REF)
                .and_then(|value| normalize_optional_wire_field(value))
        })
        .ok_or_else(|| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!(
                    "sandbox {} has no base image configured; recreate with --base-image",
                    sandbox.sandbox_id
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        })
}

async fn ensure_sandbox_shell_container(
    daemon: Arc<RuntimeDaemon>,
    sandbox: &Sandbox,
    request_id: &str,
    trace_id: Option<&str>,
) -> Result<String, Status> {
    if let Some(existing) =
        find_attachable_sandbox_container(daemon.as_ref(), &sandbox.sandbox_id, request_id)?
    {
        return Ok(existing.container_id);
    }

    let image_ref = sandbox_shell_image_ref(request_id, sandbox)?;
    let container_service = super::container::ContainerServiceImpl::new(daemon);
    let response = container_service
        .create_container(Request::new(runtime_v2::CreateContainerRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: request_id.to_string(),
                idempotency_key: String::new(),
                trace_id: trace_id.unwrap_or_default().to_string(),
            }),
            sandbox_id: sandbox.sandbox_id.clone(),
            image_digest: image_ref,
            cmd: default_keepalive_container_cmd(),
            env: std::collections::HashMap::new(),
            // Keep shell container startup portable across base images.
            // Explicitly use `/` so runtime backends do not inherit image
            // defaults like `/workspace`, which may not exist yet.
            cwd: "/".to_string(),
            user: String::new(),
        }))
        .await?;
    let container = response.into_inner().container.ok_or_else(|| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            "daemon create_container returned missing payload".to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    Ok(container.container_id)
}

fn session_registry_status(
    error: crate::ExecutionSessionRegistryError,
    request_id: &str,
) -> Status {
    match error {
        crate::ExecutionSessionRegistryError::LockPoisoned
        | crate::ExecutionSessionRegistryError::NotFound { .. } => {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::InternalError,
                "execution session registry lock poisoned".to_string(),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        }
    }
}

fn find_attachable_sandbox_shell_execution(
    daemon: &RuntimeDaemon,
    container_id: &str,
    shell_command: &str,
    shell_args: &[String],
    request_id: &str,
) -> Result<Option<Execution>, Status> {
    let debug = sandbox_exec_control_debug_enabled();
    let mut executions = daemon
        .with_state_store(|store| store.list_executions())
        .map_err(|error| status_from_stack_error(error, request_id))?
        .into_iter()
        .filter(|execution| {
            execution.container_id == container_id && !execution.state.is_terminal()
        })
        .collect::<Vec<_>>();
    executions.sort_by_key(|execution| execution.started_at.unwrap_or_default());

    for execution in executions.into_iter().rev() {
        if !execution_is_sandbox_shell_session(&execution, shell_command, shell_args) {
            if debug {
                let shell_env_flag = execution
                    .exec_spec
                    .env_override
                    .get(SANDBOX_SHELL_SESSION_ENV_KEY)
                    .map(|value| value.as_str());
                eprintln!(
                    "[vz-runtimed exec-control] execution not reusable as sandbox shell execution_id={} request_id={} expected_cmd={:?} expected_args={:?} actual_cmd={:?} actual_args={:?} shell_env={:?}",
                    execution.execution_id,
                    request_id,
                    shell_command,
                    shell_args,
                    execution.exec_spec.cmd,
                    execution.exec_spec.args,
                    shell_env_flag
                );
            }
            continue;
        }
        let has_session = daemon
            .execution_sessions()
            .contains(&execution.execution_id)
            .map_err(|error| session_registry_status(error, request_id))?;
        if debug {
            eprintln!(
                "[vz-runtimed exec-control] sandbox shell reuse candidate execution_id={} request_id={} has_session={} state={:?}",
                execution.execution_id, request_id, has_session, execution.state
            );
        }
        if has_session {
            return Ok(Some(execution));
        }
    }

    if debug {
        eprintln!(
            "[vz-runtimed exec-control] no reusable sandbox shell execution found container_id={} request_id={} expected_cmd={:?} expected_args={:?}",
            container_id, request_id, shell_command, shell_args
        );
    }
    Ok(None)
}

fn sandbox_container_ids(
    daemon: &RuntimeDaemon,
    sandbox_id: &str,
    request_id: &str,
) -> Result<std::collections::HashSet<String>, Status> {
    let ids = daemon
        .with_state_store(|store| {
            Ok(store
                .list_containers()?
                .into_iter()
                .filter(|container| container.sandbox_id == sandbox_id)
                .map(|container| container.container_id)
                .collect::<std::collections::HashSet<_>>())
        })
        .map_err(|error| status_from_stack_error(error, request_id))?;
    Ok(ids)
}

fn find_latest_active_sandbox_shell_execution(
    daemon: &RuntimeDaemon,
    sandbox: &Sandbox,
    shell_command: &str,
    shell_args: &[String],
    request_id: &str,
) -> Result<Option<Execution>, Status> {
    let container_ids = sandbox_container_ids(daemon, &sandbox.sandbox_id, request_id)?;
    if container_ids.is_empty() {
        return Ok(None);
    }

    let mut executions = daemon
        .with_state_store(|store| store.list_executions())
        .map_err(|error| status_from_stack_error(error, request_id))?
        .into_iter()
        .filter(|execution| {
            container_ids.contains(&execution.container_id)
                && !execution.state.is_terminal()
                && execution_is_sandbox_shell_session(execution, shell_command, shell_args)
        })
        .collect::<Vec<_>>();
    executions.sort_by_key(|execution| execution.started_at.unwrap_or_default());
    Ok(executions.pop())
}

fn resolve_close_sandbox_shell_execution_id(
    daemon: &RuntimeDaemon,
    sandbox: &Sandbox,
    requested_execution_id: Option<&str>,
    request_id: &str,
) -> Result<String, Status> {
    if let Some(execution_id) = requested_execution_id {
        let execution_id = execution_id.trim();
        if execution_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "execution_id cannot be empty when provided".to_string(),
                Some(request_id.to_string()),
                BTreeMap::new(),
            )));
        }

        let execution = daemon
            .with_state_store(|store| store.load_execution(execution_id))
            .map_err(|error| status_from_stack_error(error, request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("execution not found: {execution_id}"),
                    Some(request_id.to_string()),
                    BTreeMap::new(),
                ))
            })?;
        let container = daemon
            .with_state_store(|store| store.load_container(&execution.container_id))
            .map_err(|error| status_from_stack_error(error, request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!(
                        "container not found for execution {}: {}",
                        execution.execution_id, execution.container_id
                    ),
                    Some(request_id.to_string()),
                    BTreeMap::new(),
                ))
            })?;
        if container.sandbox_id != sandbox.sandbox_id {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!(
                    "execution {execution_id} does not belong to sandbox {}",
                    sandbox.sandbox_id
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            )));
        }

        return Ok(execution.execution_id);
    }

    let (shell_command, shell_args) = resolve_sandbox_shell_command(request_id, sandbox)?;
    let execution = find_latest_active_sandbox_shell_execution(
        daemon,
        sandbox,
        &shell_command,
        &shell_args,
        request_id,
    )?
    .ok_or_else(|| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::NotFound,
            format!(
                "no active shell execution found for sandbox {}",
                sandbox.sandbox_id
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    Ok(execution.execution_id)
}

async fn wait_for_shell_execution_control_ready(
    daemon: &RuntimeDaemon,
    execution_id: &str,
    request_id: &str,
) -> Result<(), Status> {
    let retry_deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match daemon.manager().write_exec_stdin(execution_id, &[]).await {
            Ok(()) => return Ok(()),
            Err(vz_runtime_contract::RuntimeError::UnsupportedOperation { .. }) => {
                // Some test/backends do not implement interactive execution control.
                // In those cases, do not block shell-open on stdin readiness probes.
                return Ok(());
            }
            Err(vz_runtime_contract::RuntimeError::ContainerNotFound { id })
                if id == execution_id =>
            {
                let execution = daemon
                    .with_state_store(|store| store.load_execution(execution_id))
                    .map_err(|error| status_from_stack_error(error, request_id))?;
                let Some(execution) = execution else {
                    return Err(status_from_machine_error(MachineError::new(
                        MachineErrorCode::NotFound,
                        format!("execution not found: {execution_id}"),
                        Some(request_id.to_string()),
                        BTreeMap::new(),
                    )));
                };
                if execution.state.is_terminal() {
                    return Err(status_from_machine_error(MachineError::new(
                        MachineErrorCode::StateConflict,
                        format!("execution {execution_id} is in terminal state"),
                        Some(request_id.to_string()),
                        BTreeMap::new(),
                    )));
                }
                if std::time::Instant::now() >= retry_deadline {
                    return Err(status_from_machine_error(MachineError::new(
                        MachineErrorCode::Timeout,
                        format!(
                            "execution session did not become stdin-ready in time: {execution_id}"
                        ),
                        Some(request_id.to_string()),
                        BTreeMap::new(),
                    )));
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            Err(error) => {
                return Err(status_from_machine_error(MachineError::new(
                    error.machine_code(),
                    format!("runtime operation `write_exec_stdin` failed: {error}"),
                    Some(request_id.to_string()),
                    BTreeMap::new(),
                )));
            }
        }
    }
}

async fn ensure_sandbox_shell_execution(
    daemon: Arc<RuntimeDaemon>,
    sandbox: &Sandbox,
    container_id: &str,
    shell_command: &str,
    shell_args: &[String],
    request_id: &str,
    trace_id: Option<&str>,
) -> Result<String, Status> {
    if let Some(existing) = find_attachable_sandbox_shell_execution(
        daemon.as_ref(),
        container_id,
        shell_command,
        shell_args,
        request_id,
    )? {
        let existing_execution_id = existing.execution_id.clone();
        match wait_for_shell_execution_control_ready(
            daemon.as_ref(),
            &existing_execution_id,
            request_id,
        )
        .await
        {
            Ok(()) => return Ok(existing_execution_id),
            Err(error) => {
                warn!(
                    execution_id = %existing_execution_id,
                    request_id = %request_id,
                    error = %error,
                    "existing sandbox shell execution is not control-ready; creating replacement"
                );
                match daemon.execution_sessions().remove(&existing_execution_id) {
                    Ok(()) | Err(crate::ExecutionSessionRegistryError::NotFound { .. }) => {}
                    Err(other) => return Err(session_registry_status(other, request_id)),
                }
            }
        }
    }
    if sandbox_exec_control_debug_enabled() {
        eprintln!(
            "[vz-runtimed exec-control] creating new sandbox shell execution container_id={} request_id={} command={:?} args={:?}",
            container_id, request_id, shell_command, shell_args
        );
    }

    let execution_service = super::execution::ExecutionServiceImpl::new(daemon.clone());
    let mut env_override = std::collections::HashMap::new();
    env_override.insert(SANDBOX_SHELL_SESSION_ENV_KEY.to_string(), "1".to_string());
    env_override.extend(sandbox_shell_secret_env_reference_overrides(
        &sandbox.labels,
        request_id,
    )?);
    let response = execution_service
        .create_execution(Request::new(runtime_v2::CreateExecutionRequest {
            metadata: Some(runtime_v2::RequestMetadata {
                request_id: request_id.to_string(),
                idempotency_key: String::new(),
                trace_id: trace_id.unwrap_or_default().to_string(),
            }),
            container_id: container_id.to_string(),
            cmd: vec![shell_command.to_string()],
            args: shell_args.to_vec(),
            env_override,
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Enabled as i32,
        }))
        .await?;
    let execution = response.into_inner().execution.ok_or_else(|| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            "daemon create_execution returned missing payload".to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    wait_for_shell_execution_control_ready(daemon.as_ref(), &execution.execution_id, request_id)
        .await?;
    Ok(execution.execution_id)
}

fn sandbox_shell_secret_env_reference_overrides(
    labels: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<std::collections::HashMap<String, String>, Status> {
    let mut overrides = std::collections::HashMap::new();
    for (key, env_var_name) in labels {
        let Some(secret_name) = key.strip_prefix(SANDBOX_LABEL_SPACE_SECRET_ENV_PREFIX) else {
            continue;
        };
        let secret_name = secret_name.trim();
        if secret_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!("sandbox label key `{key}` is invalid: secret name segment is empty"),
                Some(request_id.to_string()),
                BTreeMap::new(),
            )));
        }
        if !secret_name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
        {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!(
                    "sandbox label key `{key}` is invalid: secret name must contain only ASCII letters, digits, `_`, or `-`"
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            )));
        }
        let env_var_name = env_var_name.trim();
        if env_var_name.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!(
                    "sandbox secret `{secret_name}` has an empty external env source label value"
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            )));
        }
        match std::env::var(env_var_name) {
            Ok(value) if !value.is_empty() => {}
            Ok(_) => {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::ValidationError,
                    format!(
                        "required external secret source env var `{env_var_name}` for sandbox secret `{secret_name}` is empty"
                    ),
                    Some(request_id.to_string()),
                    BTreeMap::new(),
                )));
            }
            Err(std::env::VarError::NotPresent) => {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::ValidationError,
                    format!(
                        "required external secret source env var `{env_var_name}` for sandbox secret `{secret_name}` is not set"
                    ),
                    Some(request_id.to_string()),
                    BTreeMap::new(),
                )));
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::ValidationError,
                    format!(
                        "required external secret source env var `{env_var_name}` for sandbox secret `{secret_name}` is not valid UTF-8"
                    ),
                    Some(request_id.to_string()),
                    BTreeMap::new(),
                )));
            }
        }
        overrides.insert(
            secret_name.to_string(),
            format!("{SANDBOX_RUNTIME_ENV_REF_PREFIX}{env_var_name}"),
        );
    }
    Ok(overrides)
}

fn execution_is_sandbox_shell_session(
    execution: &Execution,
    shell_command: &str,
    shell_args: &[String],
) -> bool {
    execution.exec_spec.pty
        && execution.exec_spec.cmd == vec![shell_command.to_string()]
        && execution.exec_spec.args == shell_args
        && execution
            .exec_spec
            .env_override
            .get(SANDBOX_SHELL_SESSION_ENV_KEY)
            .is_some_and(|value| value == "1")
}

mod rpc;
