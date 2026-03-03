use super::super::*;
use crate::btrfs_portability::{export_subvolume_send_stream, import_subvolume_receive_stream};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::process::Command;
use vz_runtime_contract::CheckpointFileEntry;
pub(in crate::grpc) struct CheckpointServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl CheckpointServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

type ExportCheckpointEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::ExportCheckpointEvent, Status>>;
type ImportCheckpointEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::ImportCheckpointEvent, Status>>;

fn checkpoint_event_stream_from_events<T>(
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

fn checkpoint_stream_response<T>(
    events: Vec<Result<T, Status>>,
    receipt_id: Option<&str>,
) -> Response<tokio_stream::wrappers::ReceiverStream<Result<T, Status>>>
where
    T: Send + 'static,
{
    let mut response = Response::new(checkpoint_event_stream_from_events(events));
    if let Some(receipt_id) = receipt_id
        && !receipt_id.trim().is_empty()
        && let Ok(value) = MetadataValue::try_from(receipt_id)
    {
        response.metadata_mut().insert("x-receipt-id", value);
    }
    response
}

fn export_checkpoint_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::ExportCheckpointEvent {
    runtime_v2::ExportCheckpointEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::export_checkpoint_event::Payload::Progress(
            runtime_v2::CheckpointMutationProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn export_checkpoint_completion_event(
    request_id: &str,
    sequence: u64,
    checkpoint_id: &str,
    stream_path: &str,
) -> runtime_v2::ExportCheckpointEvent {
    runtime_v2::ExportCheckpointEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::export_checkpoint_event::Payload::Completion(
            runtime_v2::ExportCheckpointCompletion {
                checkpoint_id: checkpoint_id.to_string(),
                stream_path: stream_path.to_string(),
            },
        )),
    }
}

fn import_checkpoint_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::ImportCheckpointEvent {
    runtime_v2::ImportCheckpointEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::import_checkpoint_event::Payload::Progress(
            runtime_v2::CheckpointMutationProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn import_checkpoint_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::CheckpointResponse,
    receipt_id: &str,
    received_subvolume_path: &Path,
) -> runtime_v2::ImportCheckpointEvent {
    runtime_v2::ImportCheckpointEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::import_checkpoint_event::Payload::Completion(
            runtime_v2::ImportCheckpointCompletion {
                response: Some(response),
                receipt_id: receipt_id.to_string(),
                received_subvolume_path: received_subvolume_path.display().to_string(),
            },
        )),
    }
}

fn checkpoint_class_from_wire(value: &str) -> Result<CheckpointClass, MachineError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "fs_quick" | "fs-quick" => Ok(CheckpointClass::FsQuick),
        "vm_full" | "vm-full" => Ok(CheckpointClass::VmFull),
        other => Err(MachineError::new(
            MachineErrorCode::ValidationError,
            format!("unsupported checkpoint class: {other}"),
            None,
            BTreeMap::new(),
        )),
    }
}

fn runtime_compatibility_fingerprint(daemon: &RuntimeDaemon) -> String {
    format!(
        "runtime:backend={};daemon={}",
        daemon.backend_name(),
        daemon.daemon_version()
    )
}

fn resolve_checkpoint_compatibility_fingerprint(daemon: &RuntimeDaemon, requested: &str) -> String {
    let normalized = requested.trim();
    if normalized.is_empty() || normalized.eq_ignore_ascii_case("unset") {
        runtime_compatibility_fingerprint(daemon)
    } else {
        normalized.to_string()
    }
}

fn enforce_restore_checkpoint_compatibility(
    daemon: &RuntimeDaemon,
    checkpoint: &Checkpoint,
    request_id: &str,
) -> Result<(), Status> {
    let checkpoint_fingerprint = checkpoint.compatibility_fingerprint.trim();
    if checkpoint_fingerprint.is_empty() || checkpoint_fingerprint.eq_ignore_ascii_case("unset") {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "checkpoint {} is missing compatibility fingerprint metadata",
                checkpoint.checkpoint_id
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    if checkpoint_fingerprint.starts_with("runtime:") {
        let expected = runtime_compatibility_fingerprint(daemon);
        if checkpoint_fingerprint != expected {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!(
                    "checkpoint {} compatibility fingerprint mismatch: expected `{expected}`, got `{checkpoint_fingerprint}`",
                    checkpoint.checkpoint_id
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            )));
        }
    }

    Ok(())
}

fn checkpoint_workspace_snapshot_subvolume_path(
    daemon: &RuntimeDaemon,
    checkpoint_id: &str,
) -> PathBuf {
    daemon
        .runtime_data_dir()
        .join("checkpoints")
        .join("workspace-subvolumes")
        .join(checkpoint_id)
}

fn sandbox_workspace_project_dir(
    labels: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<Option<PathBuf>, Status> {
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

    let candidate = PathBuf::from(project_dir);
    if !candidate.is_absolute() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "sandbox label `{SANDBOX_LABEL_PROJECT_DIR}` must be an absolute path: {}",
                candidate.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    if !candidate.exists() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::NotFound,
            format!(
                "sandbox label `{SANDBOX_LABEL_PROJECT_DIR}` does not exist: {}",
                candidate.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    if !candidate.is_dir() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "sandbox label `{SANDBOX_LABEL_PROJECT_DIR}` must reference a directory: {}",
                candidate.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    Ok(Some(candidate))
}

#[cfg(target_os = "linux")]
fn path_is_on_btrfs(path: &Path, request_id: &str) -> Result<bool, Status> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    const BTRFS_SUPER_MAGIC: libc::c_long = 0x9123_683E;

    let canonical = std::fs::canonicalize(path).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::BackendUnavailable,
            format!(
                "failed to resolve checkpoint workspace path {}: {error}",
                path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    let path_cstr = CString::new(canonical.as_os_str().as_bytes()).map_err(|_| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "workspace path contains unsupported null byte: {}",
                canonical.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    #[allow(unsafe_code)]
    let f_type = unsafe {
        let mut stat: libc::statfs = std::mem::zeroed();
        if libc::statfs(path_cstr.as_ptr(), &mut stat) != 0 {
            let io_error = std::io::Error::last_os_error();
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::BackendUnavailable,
                format!(
                    "failed to inspect workspace filesystem for {}: {}",
                    canonical.display(),
                    io_error
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            )));
        }
        stat.f_type as libc::c_long
    };

    Ok(f_type == BTRFS_SUPER_MAGIC)
}

#[cfg(target_os = "linux")]
fn run_btrfs_command(args: &[&str], request_id: &str, operation: &str) -> Result<(), Status> {
    let output = Command::new("btrfs").args(args).output().map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::BackendUnavailable,
            format!("failed to execute btrfs for {operation}: {error}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let detail = if stderr.is_empty() {
        "no stderr output".to_string()
    } else {
        stderr
    };
    Err(status_from_machine_error(MachineError::new(
        MachineErrorCode::BackendUnavailable,
        format!("btrfs {operation} failed: {detail}"),
        Some(request_id.to_string()),
        BTreeMap::new(),
    )))
}

#[cfg(not(target_os = "linux"))]
fn create_workspace_checkpoint_subvolume(
    _daemon: &RuntimeDaemon,
    _checkpoint: &Checkpoint,
    _workspace_root: &Path,
    request_id: &str,
) -> Result<(), Status> {
    Err(status_from_machine_error(MachineError::new(
        MachineErrorCode::UnsupportedOperation,
        format!(
            "spaces checkpoint snapshots require Linux btrfs; current platform `{}` is unsupported",
            std::env::consts::OS
        ),
        Some(request_id.to_string()),
        BTreeMap::new(),
    )))
}

#[cfg(target_os = "linux")]
fn create_workspace_checkpoint_subvolume(
    daemon: &RuntimeDaemon,
    checkpoint: &Checkpoint,
    workspace_root: &Path,
    request_id: &str,
) -> Result<(), Status> {
    if !path_is_on_btrfs(workspace_root, request_id)? {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::UnsupportedOperation,
            format!(
                "spaces checkpoint snapshots require btrfs workspace storage; `{}` is not on btrfs",
                workspace_root.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let snapshot_root =
        checkpoint_workspace_snapshot_subvolume_path(daemon, &checkpoint.checkpoint_id);
    if snapshot_root.exists() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::StateConflict,
            format!(
                "workspace checkpoint snapshot already exists: {}",
                snapshot_root.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    if let Some(parent) = snapshot_root.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::BackendUnavailable,
                format!(
                    "failed to prepare workspace checkpoint directory {}: {error}",
                    parent.display()
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        })?;
    }

    run_btrfs_command(
        &[
            "subvolume",
            "snapshot",
            "-r",
            &workspace_root.display().to_string(),
            &snapshot_root.display().to_string(),
        ],
        request_id,
        "create workspace checkpoint snapshot",
    )
}

#[cfg(not(target_os = "linux"))]
fn restore_workspace_from_checkpoint_subvolume(
    _daemon: &RuntimeDaemon,
    _checkpoint: &Checkpoint,
    _workspace_root: &Path,
    request_id: &str,
) -> Result<(), Status> {
    Err(status_from_machine_error(MachineError::new(
        MachineErrorCode::UnsupportedOperation,
        format!(
            "spaces checkpoint restore requires Linux btrfs; current platform `{}` is unsupported",
            std::env::consts::OS
        ),
        Some(request_id.to_string()),
        BTreeMap::new(),
    )))
}

#[cfg(target_os = "linux")]
fn restore_workspace_from_checkpoint_subvolume(
    daemon: &RuntimeDaemon,
    checkpoint: &Checkpoint,
    workspace_root: &Path,
    request_id: &str,
) -> Result<(), Status> {
    if !path_is_on_btrfs(workspace_root, request_id)? {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::UnsupportedOperation,
            format!(
                "spaces checkpoint restore requires btrfs workspace storage; `{}` is not on btrfs",
                workspace_root.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let snapshot_root =
        checkpoint_workspace_snapshot_subvolume_path(daemon, &checkpoint.checkpoint_id);
    if !snapshot_root.is_dir() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::NotFound,
            format!(
                "workspace checkpoint snapshot is missing for restore: {}",
                snapshot_root.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    run_btrfs_command(
        &["subvolume", "delete", &workspace_root.display().to_string()],
        request_id,
        "delete workspace subvolume before restore",
    )?;

    run_btrfs_command(
        &[
            "subvolume",
            "snapshot",
            &snapshot_root.display().to_string(),
            &workspace_root.display().to_string(),
        ],
        request_id,
        "restore workspace snapshot",
    )
}

#[cfg(not(target_os = "linux"))]
fn fork_workspace_checkpoint_subvolume(
    _daemon: &RuntimeDaemon,
    _parent_checkpoint_id: &str,
    _fork_checkpoint_id: &str,
    request_id: &str,
) -> Result<(), Status> {
    Err(status_from_machine_error(MachineError::new(
        MachineErrorCode::UnsupportedOperation,
        format!(
            "spaces checkpoint fork requires Linux btrfs; current platform `{}` is unsupported",
            std::env::consts::OS
        ),
        Some(request_id.to_string()),
        BTreeMap::new(),
    )))
}

#[cfg(target_os = "linux")]
fn fork_workspace_checkpoint_subvolume(
    daemon: &RuntimeDaemon,
    parent_checkpoint_id: &str,
    fork_checkpoint_id: &str,
    request_id: &str,
) -> Result<(), Status> {
    let parent_snapshot =
        checkpoint_workspace_snapshot_subvolume_path(daemon, parent_checkpoint_id);
    if !parent_snapshot.is_dir() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::NotFound,
            format!(
                "workspace parent checkpoint snapshot is missing for fork: {}",
                parent_snapshot.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    let fork_snapshot = checkpoint_workspace_snapshot_subvolume_path(daemon, fork_checkpoint_id);
    if fork_snapshot.exists() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::StateConflict,
            format!(
                "workspace fork snapshot already exists: {}",
                fork_snapshot.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    if let Some(parent) = fork_snapshot.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::BackendUnavailable,
                format!(
                    "failed to prepare workspace fork checkpoint directory {}: {error}",
                    parent.display()
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        })?;
    }

    run_btrfs_command(
        &[
            "subvolume",
            "snapshot",
            "-r",
            &parent_snapshot.display().to_string(),
            &fork_snapshot.display().to_string(),
        ],
        request_id,
        "fork workspace checkpoint snapshot",
    )
}

fn required_space_workspace_root_for_checkpoint(
    daemon: &RuntimeDaemon,
    sandbox_id: &str,
    request_id: &str,
) -> Result<PathBuf, Status> {
    let sandbox = daemon
        .with_state_store(|store| store.load_sandbox(sandbox_id))
        .map_err(|error| status_from_stack_error(error, request_id))?;
    let sandbox = sandbox.ok_or_else(|| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::NotFound,
            format!("sandbox not found: {sandbox_id}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;
    sandbox_workspace_project_dir(&sandbox.labels, request_id).and_then(|value| {
        value.ok_or_else(|| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                format!(
                    "spaces mode requires sandbox label `{SANDBOX_LABEL_PROJECT_DIR}` with an absolute workspace directory path"
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        })
    })
}

fn hash_file_bytes(path: &Path) -> Result<String, std::io::Error> {
    let bytes = std::fs::read(path)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn hash_symlink_target(path: &Path) -> Result<String, std::io::Error> {
    let target = std::fs::read_link(path)?;
    let mut hasher = Sha256::new();
    hasher.update(target.to_string_lossy().as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

fn checkpoint_relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn collect_checkpoint_file_entries(
    root: &Path,
    request_id: &str,
) -> Result<Vec<CheckpointFileEntry>, Status> {
    let mut pending = vec![root.to_path_buf()];
    let mut entries = Vec::new();

    while let Some(dir) = pending.pop() {
        let read_dir = std::fs::read_dir(&dir).map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::InternalError,
                format!(
                    "failed to read checkpoint snapshot directory {}: {error}",
                    dir.display()
                ),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        })?;

        for item in read_dir {
            let item = item.map_err(|error| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::InternalError,
                    format!(
                        "failed to iterate checkpoint snapshot directory {}: {error}",
                        dir.display()
                    ),
                    Some(request_id.to_string()),
                    BTreeMap::new(),
                ))
            })?;
            let path = item.path();
            let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::InternalError,
                    format!(
                        "failed to stat checkpoint snapshot path {}: {error}",
                        path.display()
                    ),
                    Some(request_id.to_string()),
                    BTreeMap::new(),
                ))
            })?;
            if metadata.is_dir() {
                pending.push(path);
                continue;
            }

            let digest_sha256 = if metadata.file_type().is_symlink() {
                hash_symlink_target(&path).map_err(|error| {
                    status_from_machine_error(MachineError::new(
                        MachineErrorCode::InternalError,
                        format!(
                            "failed to hash checkpoint symlink target {}: {error}",
                            path.display()
                        ),
                        Some(request_id.to_string()),
                        BTreeMap::new(),
                    ))
                })?
            } else if metadata.is_file() {
                hash_file_bytes(&path).map_err(|error| {
                    status_from_machine_error(MachineError::new(
                        MachineErrorCode::InternalError,
                        format!("failed to hash checkpoint file {}: {error}", path.display()),
                        Some(request_id.to_string()),
                        BTreeMap::new(),
                    ))
                })?
            } else {
                // Skip unsupported entry kinds (sockets/devices/fifos) for now.
                continue;
            };

            entries.push(CheckpointFileEntry {
                path: checkpoint_relative_path(root, &path),
                digest_sha256,
                size: metadata.len(),
            });
        }
    }

    entries.sort_by(|lhs, rhs| lhs.path.cmp(&rhs.path));
    Ok(entries)
}

fn diff_checkpoint_file_entries(
    from_entries: &[CheckpointFileEntry],
    to_entries: &[CheckpointFileEntry],
) -> Vec<runtime_v2::CheckpointFileDiffPayload> {
    let from_map: BTreeMap<&str, &CheckpointFileEntry> = from_entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect();
    let to_map: BTreeMap<&str, &CheckpointFileEntry> = to_entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect();

    let mut all_paths: BTreeSet<&str> = BTreeSet::new();
    all_paths.extend(from_map.keys().copied());
    all_paths.extend(to_map.keys().copied());

    let mut diffs = Vec::new();
    for path in all_paths {
        match (from_map.get(path), to_map.get(path)) {
            (Some(before), Some(after))
                if before.digest_sha256 == after.digest_sha256 && before.size == after.size => {}
            (Some(before), Some(after)) => diffs.push(runtime_v2::CheckpointFileDiffPayload {
                path: path.to_string(),
                change: "M".to_string(),
                before_digest_sha256: before.digest_sha256.clone(),
                after_digest_sha256: after.digest_sha256.clone(),
                before_size: before.size,
                after_size: after.size,
            }),
            (Some(before), None) => diffs.push(runtime_v2::CheckpointFileDiffPayload {
                path: path.to_string(),
                change: "D".to_string(),
                before_digest_sha256: before.digest_sha256.clone(),
                after_digest_sha256: String::new(),
                before_size: before.size,
                after_size: 0,
            }),
            (None, Some(after)) => diffs.push(runtime_v2::CheckpointFileDiffPayload {
                path: path.to_string(),
                change: "A".to_string(),
                before_digest_sha256: String::new(),
                after_digest_sha256: after.digest_sha256.clone(),
                before_size: 0,
                after_size: after.size,
            }),
            (None, None) => {}
        }
    }
    diffs
}

fn normalize_checkpoint_retention_tag(tag: &str) -> Result<Option<String>, MachineError> {
    let normalized = tag.trim();
    if normalized.is_empty() {
        return Ok(None);
    }
    if normalized.len() > 128 {
        return Err(MachineError::new(
            MachineErrorCode::ValidationError,
            "checkpoint retention tag must be 128 characters or fewer".to_string(),
            None,
            BTreeMap::new(),
        ));
    }
    Ok(Some(normalized.to_string()))
}

fn normalize_required_absolute_path(
    raw: &str,
    field_name: &str,
    request_id: &str,
) -> Result<PathBuf, Status> {
    let normalized = raw.trim();
    if normalized.is_empty() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!("{field_name} cannot be empty"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    let path = PathBuf::from(normalized);
    if !path.is_absolute() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!("{field_name} must be an absolute path: {}", path.display()),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    Ok(path)
}

fn load_checkpoint_retention_states(
    daemon: &RuntimeDaemon,
    request_id: &str,
) -> Result<HashMap<String, vz_stack::CheckpointRetentionState>, Status> {
    let checkpoint_policy = daemon.checkpoint_retention_policy();
    daemon
        .with_state_store(|store| {
            store.checkpoint_retention_state_map(checkpoint_policy, current_unix_secs())
        })
        .map_err(|error| status_from_stack_error(error, request_id))
}

fn checkpoint_to_proto_with_retention(
    checkpoint: &Checkpoint,
    retention: Option<&vz_stack::CheckpointRetentionState>,
) -> runtime_v2::CheckpointPayload {
    let mut payload = checkpoint_to_proto_payload(checkpoint);
    if let Some(retention) = retention {
        payload.retention_tag = retention.tag.clone().unwrap_or_default();
        payload.retention_protected = retention.protected;
        payload.retention_gc_reason = retention
            .gc_reason
            .map(vz_stack::RetentionGcReason::as_str)
            .unwrap_or_default()
            .to_string();
        payload.retention_expires_at = retention.expires_at.unwrap_or_default();
    }
    payload
}

#[tonic::async_trait]
impl runtime_v2::checkpoint_service_server::CheckpointService for CheckpointServiceImpl {
    type ExportCheckpointStream = ExportCheckpointEventStream;
    type ImportCheckpointStream = ImportCheckpointEventStream;

    async fn create_checkpoint(
        &self,
        request: Request<runtime_v2::CreateCheckpointRequest>,
    ) -> Result<Response<runtime_v2::CheckpointResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateCheckpoint,
            &metadata,
            &request_id,
        )?;
        let idempotency_key = metadata.idempotency_key.clone();
        let normalized_idempotency_key = normalize_idempotency_key(idempotency_key.as_deref());

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "sandbox_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let request_hash = create_checkpoint_request_hash(
            &sandbox_id,
            &request.checkpoint_class,
            &request.compatibility_fingerprint,
            &request.retention_tag,
        );
        let retention_tag =
            normalize_checkpoint_retention_tag(&request.retention_tag).map_err(|error| {
                status_from_machine_error(MachineError::new(
                    error.code,
                    error.message,
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        let class = checkpoint_class_from_wire(&request.checkpoint_class).map_err(|error| {
            status_from_machine_error(MachineError::new(
                error.code,
                error.message,
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;

        let capabilities = self.daemon.capabilities();
        match class {
            CheckpointClass::VmFull if !capabilities.vm_full_checkpoint => {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::UnsupportedOperation,
                    "VM full checkpoints are not supported by the current backend".to_string(),
                    Some(request_id),
                    BTreeMap::new(),
                )));
            }
            CheckpointClass::FsQuick if !capabilities.fs_quick_checkpoint => {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::UnsupportedOperation,
                    "Filesystem quick checkpoints are not supported by the current backend"
                        .to_string(),
                    Some(request_id),
                    BTreeMap::new(),
                )));
            }
            _ => {}
        }

        if let Some(key) = normalized_idempotency_key {
            if let Some(cached_checkpoint) = load_idempotent_checkpoint_replay(
                &self.daemon,
                key,
                "create_checkpoint",
                &request_hash,
                &request_id,
            )? {
                let retention_states =
                    load_checkpoint_retention_states(self.daemon.as_ref(), &request_id)?;
                return Ok(Response::new(runtime_v2::CheckpointResponse {
                    request_id: request_id.clone(),
                    checkpoint: Some(checkpoint_to_proto_with_retention(
                        &cached_checkpoint,
                        retention_states.get(&cached_checkpoint.checkpoint_id),
                    )),
                }));
            }
        }

        let now = current_unix_secs();
        let mut checkpoint = Checkpoint {
            checkpoint_id: generate_checkpoint_id(),
            sandbox_id,
            parent_checkpoint_id: None,
            class,
            state: CheckpointState::Creating,
            created_at: now,
            compatibility_fingerprint: resolve_checkpoint_compatibility_fingerprint(
                self.daemon.as_ref(),
                request.compatibility_fingerprint.as_str(),
            ),
        };
        checkpoint
            .transition_to(CheckpointState::Ready)
            .map_err(|error| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::StateConflict,
                    error.to_string(),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        let workspace_root = required_space_workspace_root_for_checkpoint(
            self.daemon.as_ref(),
            checkpoint.sandbox_id.as_str(),
            &request_id,
        )?;
        create_workspace_checkpoint_subvolume(
            self.daemon.as_ref(),
            &checkpoint,
            &workspace_root,
            &request_id,
        )?;
        let snapshot_root = checkpoint_workspace_snapshot_subvolume_path(
            self.daemon.as_ref(),
            checkpoint.checkpoint_id.as_str(),
        );
        let checkpoint_file_entries = collect_checkpoint_file_entries(&snapshot_root, &request_id)?;
        let receipt_id = generate_receipt_id();
        let persist_result = self.daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_checkpoint(&checkpoint)?;
                if let Some(tag) = retention_tag.as_deref() {
                    tx.save_checkpoint_retention_tag(checkpoint.checkpoint_id.as_str(), tag)?;
                }
                tx.replace_checkpoint_file_entries(
                    checkpoint.checkpoint_id.as_str(),
                    &checkpoint_file_entries,
                )?;
                tx.emit_event(
                    &checkpoint.sandbox_id,
                    &StackEvent::CheckpointReady {
                        checkpoint_id: checkpoint.checkpoint_id.clone(),
                    },
                )?;
                tx.save_receipt(&Receipt {
                    receipt_id: receipt_id.clone(),
                    operation: "create_checkpoint".to_string(),
                    entity_id: checkpoint.checkpoint_id.clone(),
                    entity_type: "checkpoint".to_string(),
                    request_id: request_id.clone(),
                    status: "success".to_string(),
                    created_at: now,
                    metadata: receipt_idempotent_mutation_metadata(
                        "checkpoint_ready",
                        request_hash.as_str(),
                        normalized_idempotency_key,
                    )?,
                })?;
                if let Some(key) = normalized_idempotency_key {
                    tx.save_idempotency_result(&IdempotencyRecord {
                        key: key.to_string(),
                        operation: "create_checkpoint".to_string(),
                        request_hash: request_hash.clone(),
                        response_json: checkpoint.checkpoint_id.clone(),
                        status_code: 201,
                        created_at: now,
                        expires_at: now.saturating_add(IDEMPOTENCY_TTL_SECS),
                    })?;
                }
                Ok(())
            })
        });
        if let Err(error) = persist_result {
            if let Some(key) = normalized_idempotency_key {
                if let Some(cached_checkpoint) = load_idempotent_checkpoint_replay(
                    &self.daemon,
                    key,
                    "create_checkpoint",
                    &request_hash,
                    &request_id,
                )? {
                    let retention_states =
                        load_checkpoint_retention_states(self.daemon.as_ref(), &request_id)?;
                    return Ok(Response::new(runtime_v2::CheckpointResponse {
                        request_id,
                        checkpoint: Some(checkpoint_to_proto_with_retention(
                            &cached_checkpoint,
                            retention_states.get(&cached_checkpoint.checkpoint_id),
                        )),
                    }));
                }
            }
            return Err(status_from_stack_error(error, &request_id));
        }

        let retention_states = load_checkpoint_retention_states(self.daemon.as_ref(), &request_id)?;
        let mut response = Response::new(runtime_v2::CheckpointResponse {
            request_id: request_id.clone(),
            checkpoint: Some(checkpoint_to_proto_with_retention(
                &checkpoint,
                retention_states.get(&checkpoint.checkpoint_id),
            )),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn get_checkpoint(
        &self,
        request: Request<runtime_v2::GetCheckpointRequest>,
    ) -> Result<Response<runtime_v2::CheckpointResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        let checkpoint = self
            .daemon
            .with_state_store(|store| store.load_checkpoint(&request.checkpoint_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("checkpoint not found: {}", request.checkpoint_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;
        let retention_states = load_checkpoint_retention_states(self.daemon.as_ref(), &request_id)?;

        Ok(Response::new(runtime_v2::CheckpointResponse {
            request_id,
            checkpoint: Some(checkpoint_to_proto_with_retention(
                &checkpoint,
                retention_states.get(&checkpoint.checkpoint_id),
            )),
        }))
    }

    async fn list_checkpoints(
        &self,
        request: Request<runtime_v2::ListCheckpointsRequest>,
    ) -> Result<Response<runtime_v2::ListCheckpointsResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let checkpoints = self
            .daemon
            .with_state_store(|store| store.list_checkpoints())
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .into_iter()
            .collect::<Vec<_>>();
        let retention_states = load_checkpoint_retention_states(self.daemon.as_ref(), &request_id)?;
        let checkpoints = checkpoints
            .iter()
            .map(|checkpoint| {
                checkpoint_to_proto_with_retention(
                    checkpoint,
                    retention_states.get(&checkpoint.checkpoint_id),
                )
            })
            .collect();

        Ok(Response::new(runtime_v2::ListCheckpointsResponse {
            request_id,
            checkpoints,
        }))
    }

    async fn diff_checkpoints(
        &self,
        request: Request<runtime_v2::DiffCheckpointsRequest>,
    ) -> Result<Response<runtime_v2::DiffCheckpointsResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let from_checkpoint_id = request.from_checkpoint_id.trim().to_string();
        if from_checkpoint_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "from_checkpoint_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let to_checkpoint_id = request.to_checkpoint_id.trim().to_string();
        if to_checkpoint_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "to_checkpoint_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let (from_checkpoint, to_checkpoint, from_entries, to_entries) = self
            .daemon
            .with_state_store(|store| {
                Ok((
                    store.load_checkpoint(&from_checkpoint_id)?,
                    store.load_checkpoint(&to_checkpoint_id)?,
                    store.load_checkpoint_file_entries(&from_checkpoint_id)?,
                    store.load_checkpoint_file_entries(&to_checkpoint_id)?,
                ))
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        if from_checkpoint.is_none() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("checkpoint not found: {from_checkpoint_id}"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        if to_checkpoint.is_none() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("checkpoint not found: {to_checkpoint_id}"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let from_snapshot =
            checkpoint_workspace_snapshot_subvolume_path(self.daemon.as_ref(), &from_checkpoint_id);
        if !from_snapshot.is_dir() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!(
                    "workspace checkpoint snapshot is missing for diff: {}",
                    from_snapshot.display()
                ),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let to_snapshot =
            checkpoint_workspace_snapshot_subvolume_path(self.daemon.as_ref(), &to_checkpoint_id);
        if !to_snapshot.is_dir() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!(
                    "workspace checkpoint snapshot is missing for diff: {}",
                    to_snapshot.display()
                ),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        Ok(Response::new(runtime_v2::DiffCheckpointsResponse {
            request_id,
            files: diff_checkpoint_file_entries(&from_entries, &to_entries),
        }))
    }

    async fn export_checkpoint(
        &self,
        request: Request<runtime_v2::ExportCheckpointRequest>,
    ) -> Result<Response<Self::ExportCheckpointStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let checkpoint_id = request.checkpoint_id.trim().to_string();
        if checkpoint_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "checkpoint_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let stream_path =
            normalize_required_absolute_path(&request.stream_path, "stream_path", &request_id)?;

        let checkpoint = self
            .daemon
            .with_state_store(|store| store.load_checkpoint(&checkpoint_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("checkpoint not found: {checkpoint_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if checkpoint.state != CheckpointState::Ready {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("checkpoint {checkpoint_id} is not in ready state"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let snapshot_root =
            checkpoint_workspace_snapshot_subvolume_path(self.daemon.as_ref(), &checkpoint_id);
        if !snapshot_root.is_dir() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!(
                    "workspace checkpoint snapshot is missing for export: {}",
                    snapshot_root.display()
                ),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        export_subvolume_send_stream(&snapshot_root, &stream_path)
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        Ok(checkpoint_stream_response(
            vec![
                Ok(export_checkpoint_progress_event(
                    &request_id,
                    1,
                    "validate",
                    "validated checkpoint export request",
                )),
                Ok(export_checkpoint_progress_event(
                    &request_id,
                    2,
                    "export",
                    "streaming btrfs send payload",
                )),
                Ok(export_checkpoint_completion_event(
                    &request_id,
                    3,
                    &checkpoint_id,
                    stream_path.display().to_string().as_str(),
                )),
            ],
            None,
        ))
    }

    async fn import_checkpoint(
        &self,
        request: Request<runtime_v2::ImportCheckpointRequest>,
    ) -> Result<Response<Self::ImportCheckpointStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::CreateCheckpoint,
            &metadata,
            &request_id,
        )?;

        let sandbox_id = request.sandbox_id.trim().to_string();
        if sandbox_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "sandbox_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        let stream_path =
            normalize_required_absolute_path(&request.stream_path, "stream_path", &request_id)?;

        self.daemon
            .with_state_store(|store| store.load_sandbox(&sandbox_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("sandbox not found: {sandbox_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        let class = checkpoint_class_from_wire(&request.checkpoint_class).map_err(|error| {
            status_from_machine_error(MachineError::new(
                error.code,
                error.message,
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;
        let capabilities = self.daemon.capabilities();
        match class {
            CheckpointClass::VmFull if !capabilities.vm_full_checkpoint => {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::UnsupportedOperation,
                    "VM full checkpoints are not supported by the current backend".to_string(),
                    Some(request_id),
                    BTreeMap::new(),
                )));
            }
            CheckpointClass::FsQuick if !capabilities.fs_quick_checkpoint => {
                return Err(status_from_machine_error(MachineError::new(
                    MachineErrorCode::UnsupportedOperation,
                    "Filesystem quick checkpoints are not supported by the current backend"
                        .to_string(),
                    Some(request_id),
                    BTreeMap::new(),
                )));
            }
            _ => {}
        }

        let retention_tag =
            normalize_checkpoint_retention_tag(&request.retention_tag).map_err(|error| {
                status_from_machine_error(MachineError::new(
                    error.code,
                    error.message,
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        let now = current_unix_secs();
        let request_hash = create_checkpoint_request_hash(
            &sandbox_id,
            &request.checkpoint_class,
            &request.compatibility_fingerprint,
            &request.retention_tag,
        );
        let checkpoint = Checkpoint {
            checkpoint_id: generate_checkpoint_id(),
            sandbox_id,
            parent_checkpoint_id: None,
            class,
            state: CheckpointState::Ready,
            created_at: now,
            compatibility_fingerprint: resolve_checkpoint_compatibility_fingerprint(
                self.daemon.as_ref(),
                request.compatibility_fingerprint.as_str(),
            ),
        };

        let receive_parent = self
            .daemon
            .runtime_data_dir()
            .join("checkpoints")
            .join("import-staging")
            .join(request_id.as_str());
        let received_subvolume_path =
            import_subvolume_receive_stream(&stream_path, &receive_parent)
                .map_err(|error| status_from_stack_error(error, &request_id))?;

        let checkpoint_snapshot_path = checkpoint_workspace_snapshot_subvolume_path(
            self.daemon.as_ref(),
            checkpoint.checkpoint_id.as_str(),
        );
        if checkpoint_snapshot_path.exists() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!(
                    "workspace checkpoint snapshot already exists: {}",
                    checkpoint_snapshot_path.display()
                ),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        if let Some(parent) = checkpoint_snapshot_path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::BackendUnavailable,
                    format!(
                        "failed to prepare workspace checkpoint directory {}: {error}",
                        parent.display()
                    ),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;
        }
        std::fs::rename(&received_subvolume_path, &checkpoint_snapshot_path).map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::BackendUnavailable,
                format!(
                    "failed to move received subvolume {} to {}: {error}",
                    received_subvolume_path.display(),
                    checkpoint_snapshot_path.display()
                ),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;

        let checkpoint_file_entries =
            collect_checkpoint_file_entries(&checkpoint_snapshot_path, &request_id)?;
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.save_checkpoint(&checkpoint)?;
                    if let Some(tag) = retention_tag.as_deref() {
                        tx.save_checkpoint_retention_tag(checkpoint.checkpoint_id.as_str(), tag)?;
                    }
                    tx.replace_checkpoint_file_entries(
                        checkpoint.checkpoint_id.as_str(),
                        &checkpoint_file_entries,
                    )?;
                    tx.emit_event(
                        &checkpoint.sandbox_id,
                        &StackEvent::CheckpointReady {
                            checkpoint_id: checkpoint.checkpoint_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "import_checkpoint".to_string(),
                        entity_id: checkpoint.checkpoint_id.clone(),
                        entity_type: "checkpoint".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_idempotent_mutation_metadata(
                            "checkpoint_imported",
                            request_hash.as_str(),
                            None,
                        )?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let retention_states = load_checkpoint_retention_states(self.daemon.as_ref(), &request_id)?;
        let checkpoint_response = runtime_v2::CheckpointResponse {
            request_id: request_id.clone(),
            checkpoint: Some(checkpoint_to_proto_with_retention(
                &checkpoint,
                retention_states.get(&checkpoint.checkpoint_id),
            )),
        };

        Ok(checkpoint_stream_response(
            vec![
                Ok(import_checkpoint_progress_event(
                    &request_id,
                    1,
                    "validate",
                    "validated checkpoint import request",
                )),
                Ok(import_checkpoint_progress_event(
                    &request_id,
                    2,
                    "receive",
                    "receiving btrfs send stream payload",
                )),
                Ok(import_checkpoint_progress_event(
                    &request_id,
                    3,
                    "persist",
                    "persisted imported checkpoint metadata and receipts",
                )),
                Ok(import_checkpoint_completion_event(
                    &request_id,
                    4,
                    checkpoint_response,
                    &receipt_id,
                    &checkpoint_snapshot_path,
                )),
            ],
            Some(receipt_id.as_str()),
        ))
    }

    async fn restore_checkpoint(
        &self,
        request: Request<runtime_v2::RestoreCheckpointRequest>,
    ) -> Result<Response<runtime_v2::CheckpointResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::RestoreCheckpoint,
            &metadata,
            &request_id,
        )?;

        let checkpoint = self
            .daemon
            .with_state_store(|store| store.load_checkpoint(&request.checkpoint_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("checkpoint not found: {}", request.checkpoint_id),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if checkpoint.state != CheckpointState::Ready {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("checkpoint {} is not in ready state", request.checkpoint_id),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        enforce_restore_checkpoint_compatibility(self.daemon.as_ref(), &checkpoint, &request_id)?;

        let workspace_root = required_space_workspace_root_for_checkpoint(
            self.daemon.as_ref(),
            checkpoint.sandbox_id.as_str(),
            &request_id,
        )?;
        restore_workspace_from_checkpoint_subvolume(
            self.daemon.as_ref(),
            &checkpoint,
            &workspace_root,
            &request_id,
        )?;

        let now = current_unix_secs();
        let receipt_id = generate_receipt_id();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.emit_event(
                        &checkpoint.sandbox_id,
                        &StackEvent::CheckpointRestored {
                            checkpoint_id: checkpoint.checkpoint_id.clone(),
                            sandbox_id: checkpoint.sandbox_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: "restore_checkpoint".to_string(),
                        entity_id: checkpoint.checkpoint_id.clone(),
                        entity_type: "checkpoint".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_event_metadata("checkpoint_restored")?,
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        let retention_states = load_checkpoint_retention_states(self.daemon.as_ref(), &request_id)?;
        let mut response = Response::new(runtime_v2::CheckpointResponse {
            request_id: request_id.clone(),
            checkpoint: Some(checkpoint_to_proto_with_retention(
                &checkpoint,
                retention_states.get(&checkpoint.checkpoint_id),
            )),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }

    async fn fork_checkpoint(
        &self,
        request: Request<runtime_v2::ForkCheckpointRequest>,
    ) -> Result<Response<runtime_v2::CheckpointResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);
        enforce_mutation_policy_preflight(
            self.daemon.as_ref(),
            RuntimeOperation::ForkCheckpoint,
            &metadata,
            &request_id,
        )?;
        let idempotency_key = metadata.idempotency_key.clone();
        let normalized_idempotency_key = normalize_idempotency_key(idempotency_key.as_deref());

        let parent_checkpoint_id = request.checkpoint_id.trim().to_string();
        if parent_checkpoint_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "checkpoint_id cannot be empty".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let requested_new_sandbox_id = request.new_sandbox_id.trim().to_string();
        let request_hash =
            create_fork_checkpoint_request_hash(&parent_checkpoint_id, &requested_new_sandbox_id);

        if let Some(key) = normalized_idempotency_key {
            if let Some(cached_checkpoint) = load_idempotent_checkpoint_replay(
                &self.daemon,
                key,
                "fork_checkpoint",
                &request_hash,
                &request_id,
            )? {
                let retention_states =
                    load_checkpoint_retention_states(self.daemon.as_ref(), &request_id)?;
                return Ok(Response::new(runtime_v2::CheckpointResponse {
                    request_id: request_id.clone(),
                    checkpoint: Some(checkpoint_to_proto_with_retention(
                        &cached_checkpoint,
                        retention_states.get(&cached_checkpoint.checkpoint_id),
                    )),
                }));
            }
        }

        let parent = self
            .daemon
            .with_state_store(|store| store.load_checkpoint(&parent_checkpoint_id))
            .map_err(|error| status_from_stack_error(error, &request_id))?
            .ok_or_else(|| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::NotFound,
                    format!("checkpoint not found: {parent_checkpoint_id}"),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        if parent.state != CheckpointState::Ready {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::StateConflict,
                format!("checkpoint {parent_checkpoint_id} is not in ready state"),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let now = current_unix_secs();
        let new_sandbox_id = if requested_new_sandbox_id.is_empty() {
            generate_fork_sandbox_id()
        } else {
            requested_new_sandbox_id
        };
        let mut forked = Checkpoint {
            checkpoint_id: generate_checkpoint_id(),
            sandbox_id: new_sandbox_id.clone(),
            parent_checkpoint_id: Some(parent.checkpoint_id.clone()),
            class: parent.class,
            state: CheckpointState::Creating,
            created_at: now,
            compatibility_fingerprint: parent.compatibility_fingerprint.clone(),
        };
        forked
            .transition_to(CheckpointState::Ready)
            .map_err(|error| {
                status_from_machine_error(MachineError::new(
                    MachineErrorCode::StateConflict,
                    error.to_string(),
                    Some(request_id.clone()),
                    BTreeMap::new(),
                ))
            })?;

        fork_workspace_checkpoint_subvolume(
            self.daemon.as_ref(),
            parent.checkpoint_id.as_str(),
            forked.checkpoint_id.as_str(),
            &request_id,
        )?;

        let receipt_id = generate_receipt_id();
        let persist_result = self.daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_checkpoint(&forked)?;
                tx.emit_event(
                    "default",
                    &StackEvent::CheckpointForked {
                        parent_checkpoint_id: parent.checkpoint_id.clone(),
                        new_checkpoint_id: forked.checkpoint_id.clone(),
                        new_sandbox_id: forked.sandbox_id.clone(),
                    },
                )?;
                tx.save_receipt(&Receipt {
                    receipt_id: receipt_id.clone(),
                    operation: "fork_checkpoint".to_string(),
                    entity_id: forked.checkpoint_id.clone(),
                    entity_type: "checkpoint".to_string(),
                    request_id: request_id.clone(),
                    status: "success".to_string(),
                    created_at: now,
                    metadata: receipt_idempotent_mutation_metadata(
                        "checkpoint_forked",
                        request_hash.as_str(),
                        normalized_idempotency_key,
                    )?,
                })?;
                if let Some(key) = normalized_idempotency_key {
                    tx.save_idempotency_result(&IdempotencyRecord {
                        key: key.to_string(),
                        operation: "fork_checkpoint".to_string(),
                        request_hash: request_hash.clone(),
                        response_json: forked.checkpoint_id.clone(),
                        status_code: 201,
                        created_at: now,
                        expires_at: now.saturating_add(IDEMPOTENCY_TTL_SECS),
                    })?;
                }
                Ok(())
            })
        });
        if let Err(error) = persist_result {
            if let Some(key) = normalized_idempotency_key {
                if let Some(cached_checkpoint) = load_idempotent_checkpoint_replay(
                    &self.daemon,
                    key,
                    "fork_checkpoint",
                    &request_hash,
                    &request_id,
                )? {
                    let retention_states =
                        load_checkpoint_retention_states(self.daemon.as_ref(), &request_id)?;
                    return Ok(Response::new(runtime_v2::CheckpointResponse {
                        request_id,
                        checkpoint: Some(checkpoint_to_proto_with_retention(
                            &cached_checkpoint,
                            retention_states.get(&cached_checkpoint.checkpoint_id),
                        )),
                    }));
                }
            }
            return Err(status_from_stack_error(error, &request_id));
        }

        let retention_states = load_checkpoint_retention_states(self.daemon.as_ref(), &request_id)?;
        let mut response = Response::new(runtime_v2::CheckpointResponse {
            request_id: request_id.clone(),
            checkpoint: Some(checkpoint_to_proto_with_retention(
                &forked,
                retention_states.get(&forked.checkpoint_id),
            )),
        });
        if let Ok(value) = MetadataValue::try_from(receipt_id.as_str()) {
            response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, digest_sha256: &str, size: u64) -> CheckpointFileEntry {
        CheckpointFileEntry {
            path: path.to_string(),
            digest_sha256: digest_sha256.to_string(),
            size,
        }
    }

    #[test]
    fn checkpoint_file_diff_detects_add_delete_modify_and_rename() {
        let from_entries = vec![
            entry("old-name.txt", "digest-rename", 10),
            entry("deleted.txt", "digest-delete", 4),
            entry("modified.txt", "digest-before", 5),
            entry("unchanged.txt", "digest-same", 7),
        ];
        let to_entries = vec![
            entry("new-name.txt", "digest-rename", 10),
            entry("added.txt", "digest-add", 3),
            entry("modified.txt", "digest-after", 8),
            entry("unchanged.txt", "digest-same", 7),
        ];

        let diffs = diff_checkpoint_file_entries(&from_entries, &to_entries);
        let rendered: Vec<(String, String)> = diffs
            .iter()
            .map(|item| (item.path.clone(), item.change.clone()))
            .collect();
        assert_eq!(
            rendered,
            vec![
                ("added.txt".to_string(), "A".to_string()),
                ("deleted.txt".to_string(), "D".to_string()),
                ("modified.txt".to_string(), "M".to_string()),
                ("new-name.txt".to_string(), "A".to_string()),
                ("old-name.txt".to_string(), "D".to_string()),
            ]
        );
    }
}
