use super::super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use vz_runtime_contract::SandboxBackend;

const LINUX_VM_BASE_REGISTRY_VERSION: u32 = 1;
const LINUX_VM_BASE_REGISTRY_FILENAME: &str = "linux-vm-bases.json";
const LINUX_VM_PATCH_ROLLBACK_DIR: &str = "linux-vm-patch-rollbacks";
const LINUX_VM_PATCH_EVENT_STACK: &str = "daemon";
const LINUX_VM_PATCH_APPLY_OPERATION: &str = "linux_vm_patch_apply";
const LINUX_VM_PATCH_ROLLBACK_OPERATION: &str = "linux_vm_patch_rollback";

pub(in crate::grpc) struct LinuxVmServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl LinuxVmServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }

    fn linux_vm_base_registry_path(&self) -> PathBuf {
        self.daemon
            .runtime_data_dir()
            .join(LINUX_VM_BASE_REGISTRY_FILENAME)
    }

    fn load_linux_vm_base_registry(&self) -> Result<LinuxVmBaseRegistry, MachineError> {
        let path = self.linux_vm_base_registry_path();
        if !path.exists() {
            return Ok(LinuxVmBaseRegistry::default());
        }
        let raw = std::fs::read(&path).map_err(|error| {
            MachineError::new(
                MachineErrorCode::InternalError,
                format!(
                    "failed to read linux vm base registry {}: {error}",
                    path.display()
                ),
                None,
                BTreeMap::new(),
            )
        })?;

        let registry = serde_json::from_slice::<LinuxVmBaseRegistry>(&raw).map_err(|error| {
            MachineError::new(
                MachineErrorCode::InternalError,
                format!(
                    "failed to parse linux vm base registry {}: {error}",
                    path.display()
                ),
                None,
                BTreeMap::new(),
            )
        })?;

        if registry.version != LINUX_VM_BASE_REGISTRY_VERSION {
            return Err(MachineError::new(
                MachineErrorCode::ValidationError,
                format!(
                    "unsupported linux vm base registry version {} in {}",
                    registry.version,
                    path.display()
                ),
                None,
                BTreeMap::new(),
            ));
        }

        Ok(registry)
    }

    fn persist_linux_vm_base_registry(
        &self,
        registry: &LinuxVmBaseRegistry,
    ) -> Result<(), MachineError> {
        let path = self.linux_vm_base_registry_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                MachineError::new(
                    MachineErrorCode::InternalError,
                    format!(
                        "failed to create linux vm base registry parent {}: {error}",
                        parent.display()
                    ),
                    None,
                    BTreeMap::new(),
                )
            })?;
        }

        let payload = serde_json::to_vec_pretty(registry).map_err(|error| {
            MachineError::new(
                MachineErrorCode::InternalError,
                format!("failed to serialize linux vm base registry: {error}"),
                None,
                BTreeMap::new(),
            )
        })?;
        std::fs::write(&path, payload).map_err(|error| {
            MachineError::new(
                MachineErrorCode::InternalError,
                format!(
                    "failed to persist linux vm base registry {}: {error}",
                    path.display()
                ),
                None,
                BTreeMap::new(),
            )
        })?;

        Ok(())
    }

    fn linux_vm_patch_rollback_dir(&self) -> PathBuf {
        self.daemon
            .runtime_data_dir()
            .join(LINUX_VM_PATCH_ROLLBACK_DIR)
    }

    fn linux_vm_patch_rollback_path(&self, rollback_id: &str) -> PathBuf {
        self.linux_vm_patch_rollback_dir()
            .join(format!("{rollback_id}.json"))
    }

    fn persist_patch_rollback_snapshot(
        &self,
        snapshot: &LinuxVmPatchRollbackSnapshot,
    ) -> Result<(), MachineError> {
        let path = self.linux_vm_patch_rollback_path(&snapshot.rollback_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                MachineError::new(
                    MachineErrorCode::InternalError,
                    format!(
                        "failed to create linux vm rollback dir {}: {error}",
                        parent.display()
                    ),
                    None,
                    BTreeMap::new(),
                )
            })?;
        }
        let payload = serde_json::to_vec_pretty(snapshot).map_err(|error| {
            MachineError::new(
                MachineErrorCode::InternalError,
                format!("failed to serialize linux vm rollback snapshot: {error}"),
                None,
                BTreeMap::new(),
            )
        })?;
        std::fs::write(&path, payload).map_err(|error| {
            MachineError::new(
                MachineErrorCode::InternalError,
                format!(
                    "failed to persist linux vm rollback snapshot {}: {error}",
                    path.display()
                ),
                None,
                BTreeMap::new(),
            )
        })?;
        Ok(())
    }

    fn load_patch_rollback_snapshot(
        &self,
        rollback_id: &str,
    ) -> Result<LinuxVmPatchRollbackSnapshot, MachineError> {
        let path = self.linux_vm_patch_rollback_path(rollback_id);
        let raw = std::fs::read(&path).map_err(|error| {
            let code = if error.kind() == std::io::ErrorKind::NotFound {
                MachineErrorCode::NotFound
            } else {
                MachineErrorCode::InternalError
            };
            MachineError::new(
                code,
                format!(
                    "failed to read linux vm rollback snapshot {}: {error}",
                    path.display()
                ),
                None,
                BTreeMap::new(),
            )
        })?;
        serde_json::from_slice::<LinuxVmPatchRollbackSnapshot>(&raw).map_err(|error| {
            MachineError::new(
                MachineErrorCode::InternalError,
                format!(
                    "failed to parse linux vm rollback snapshot {}: {error}",
                    path.display()
                ),
                None,
                BTreeMap::new(),
            )
        })
    }

    fn delete_patch_rollback_snapshot(&self, rollback_id: &str) -> Result<(), MachineError> {
        let path = self.linux_vm_patch_rollback_path(rollback_id);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(MachineError::new(
                MachineErrorCode::InternalError,
                format!(
                    "failed to remove linux vm rollback snapshot {}: {error}",
                    path.display()
                ),
                None,
                BTreeMap::new(),
            )),
        }
    }
}

type ValidateLinuxVmEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::ValidateLinuxVmEvent, Status>>;
type UpsertLinuxVmBaseEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::UpsertLinuxVmBaseEvent, Status>>;
type DeleteLinuxVmBaseEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::DeleteLinuxVmBaseEvent, Status>>;
type ApplyLinuxVmPatchEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::ApplyLinuxVmPatchEvent, Status>>;
type RollbackLinuxVmPatchEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::RollbackLinuxVmPatchEvent, Status>>;

fn linux_vm_event_stream_from_events<T>(
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

fn linux_vm_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::ValidateLinuxVmEvent {
    runtime_v2::ValidateLinuxVmEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::validate_linux_vm_event::Payload::Progress(
            runtime_v2::LinuxVmValidationProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn linux_vm_completion_event(
    request_id: &str,
    sequence: u64,
    descriptor_path: &Path,
    daemon_backend: &str,
    ok: bool,
    checks: Vec<runtime_v2::LinuxVmValidationCheck>,
) -> runtime_v2::ValidateLinuxVmEvent {
    runtime_v2::ValidateLinuxVmEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::validate_linux_vm_event::Payload::Completion(
            runtime_v2::ValidateLinuxVmCompletion {
                descriptor_path: descriptor_path.display().to_string(),
                daemon_backend: daemon_backend.to_string(),
                ok,
                checks,
            },
        )),
    }
}

fn linux_vm_base_upsert_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::UpsertLinuxVmBaseEvent {
    runtime_v2::UpsertLinuxVmBaseEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::upsert_linux_vm_base_event::Payload::Progress(
            runtime_v2::LinuxVmBaseMutationProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn linux_vm_base_delete_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::DeleteLinuxVmBaseEvent {
    runtime_v2::DeleteLinuxVmBaseEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::delete_linux_vm_base_event::Payload::Progress(
            runtime_v2::LinuxVmBaseMutationProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn linux_vm_base_upsert_completion_event(
    request_id: &str,
    sequence: u64,
    base: runtime_v2::LinuxVmBaseDefinition,
) -> runtime_v2::UpsertLinuxVmBaseEvent {
    runtime_v2::UpsertLinuxVmBaseEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::upsert_linux_vm_base_event::Payload::Completion(
            runtime_v2::UpsertLinuxVmBaseCompletion { base: Some(base) },
        )),
    }
}

fn linux_vm_base_delete_completion_event(
    request_id: &str,
    sequence: u64,
    base_id: &str,
) -> runtime_v2::DeleteLinuxVmBaseEvent {
    runtime_v2::DeleteLinuxVmBaseEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::delete_linux_vm_base_event::Payload::Completion(
            runtime_v2::DeleteLinuxVmBaseCompletion {
                base_id: base_id.to_string(),
            },
        )),
    }
}

fn linux_vm_patch_apply_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::ApplyLinuxVmPatchEvent {
    runtime_v2::ApplyLinuxVmPatchEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::apply_linux_vm_patch_event::Payload::Progress(
            runtime_v2::LinuxVmPatchMutationProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn linux_vm_patch_rollback_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::RollbackLinuxVmPatchEvent {
    runtime_v2::RollbackLinuxVmPatchEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(
            runtime_v2::rollback_linux_vm_patch_event::Payload::Progress(
                runtime_v2::LinuxVmPatchMutationProgress {
                    phase: phase.to_string(),
                    detail: detail.to_string(),
                },
            ),
        ),
    }
}

fn linux_vm_patch_apply_completion_event(
    request_id: &str,
    sequence: u64,
    base: runtime_v2::LinuxVmBaseDefinition,
    patch_id: &str,
    rollback_id: &str,
    receipt_id: &str,
) -> runtime_v2::ApplyLinuxVmPatchEvent {
    runtime_v2::ApplyLinuxVmPatchEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::apply_linux_vm_patch_event::Payload::Completion(
            runtime_v2::ApplyLinuxVmPatchCompletion {
                base: Some(base),
                patch_id: patch_id.to_string(),
                rollback_id: rollback_id.to_string(),
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

fn linux_vm_patch_rollback_completion_event(
    request_id: &str,
    sequence: u64,
    base: runtime_v2::LinuxVmBaseDefinition,
    patch_id: &str,
    rollback_id: &str,
    receipt_id: &str,
) -> runtime_v2::RollbackLinuxVmPatchEvent {
    runtime_v2::RollbackLinuxVmPatchEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(
            runtime_v2::rollback_linux_vm_patch_event::Payload::Completion(
                runtime_v2::RollbackLinuxVmPatchCompletion {
                    base: Some(base),
                    patch_id: patch_id.to_string(),
                    rollback_id: rollback_id.to_string(),
                    receipt_id: receipt_id.to_string(),
                },
            ),
        ),
    }
}

#[derive(Debug, serde::Deserialize)]
struct LinuxVmImageDescriptor {
    schema_version: u16,
    image_name: String,
    kernel_path: PathBuf,
    initramfs_path: PathBuf,
    version_json_path: PathBuf,
    #[allow(dead_code)]
    disk_path: PathBuf,
    #[allow(dead_code)]
    disk_size_gb: u64,
    linux_artifact_version: String,
    #[allow(dead_code)]
    sha256_vmlinux: String,
    #[allow(dead_code)]
    sha256_initramfs: String,
    #[allow(dead_code)]
    created_at_unix_secs: u64,
}

#[derive(Debug, serde::Deserialize)]
struct LinuxArtifactVersionJson {
    kernel: String,
    sha256_vmlinux: String,
    sha256_initramfs: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LinuxVmBaseRecord {
    base_id: String,
    kernel_path: PathBuf,
    initramfs_path: PathBuf,
    version_json_path: PathBuf,
    description: String,
    updated_at_unix_secs: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LinuxVmBaseRegistry {
    version: u32,
    bases: BTreeMap<String, LinuxVmBaseRecord>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LinuxVmPatchBundle {
    schema_version: u32,
    patch_id: String,
    base_id: String,
    set: LinuxVmPatchSet,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
struct LinuxVmPatchSet {
    kernel_path: Option<String>,
    initramfs_path: Option<String>,
    version_json_path: Option<String>,
    description: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LinuxVmPatchRollbackSnapshot {
    rollback_id: String,
    patch_id: String,
    base_id: String,
    previous: LinuxVmBaseRecord,
    created_at_unix_secs: u64,
}

#[derive(Debug, serde::Serialize)]
struct LinuxVmPatchApplyReceiptMetadata {
    event_type: &'static str,
    patch_id: String,
    rollback_id: String,
    bundle_path: String,
    base_id: String,
}

#[derive(Debug, serde::Serialize)]
struct LinuxVmPatchRollbackReceiptMetadata {
    event_type: &'static str,
    patch_id: String,
    rollback_id: String,
    base_id: String,
}

impl Default for LinuxVmBaseRegistry {
    fn default() -> Self {
        Self {
            version: LINUX_VM_BASE_REGISTRY_VERSION,
            bases: BTreeMap::new(),
        }
    }
}

fn sha256_file(path: &Path) -> Result<String, std::io::Error> {
    let bytes = std::fs::read(path)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn check(name: &str, ok: bool, detail: impl Into<String>) -> runtime_v2::LinuxVmValidationCheck {
    runtime_v2::LinuxVmValidationCheck {
        name: name.to_string(),
        status: if ok { "pass" } else { "fail" }.to_string(),
        detail: detail.into(),
    }
}

fn validate_descriptor(
    descriptor_path: &Path,
) -> Result<Vec<runtime_v2::LinuxVmValidationCheck>, String> {
    let mut checks = Vec::new();
    let raw = std::fs::read(descriptor_path).map_err(|error| {
        format!(
            "failed to read descriptor {}: {error}",
            descriptor_path.display()
        )
    })?;
    let descriptor = serde_json::from_slice::<LinuxVmImageDescriptor>(&raw)
        .map_err(|error| error.to_string())?;
    if descriptor.schema_version != 1 {
        return Err(format!(
            "unsupported descriptor schema {} in {}",
            descriptor.schema_version,
            descriptor_path.display()
        ));
    }
    checks.push(check(
        "descriptor_load",
        true,
        format!(
            "loaded descriptor {} for image {}",
            descriptor_path.display(),
            descriptor.image_name
        ),
    ));

    let version_raw = std::fs::read(&descriptor.version_json_path).map_err(|error| {
        format!(
            "failed to read version metadata {}: {error}",
            descriptor.version_json_path.display()
        )
    })?;
    let version =
        serde_json::from_slice::<LinuxArtifactVersionJson>(&version_raw).map_err(|error| {
            format!(
                "failed to parse version metadata {}: {error}",
                descriptor.version_json_path.display()
            )
        })?;

    let kernel_sha = sha256_file(&descriptor.kernel_path).map_err(|error| {
        format!(
            "failed to hash kernel {}: {error}",
            descriptor.kernel_path.display()
        )
    })?;
    let initramfs_sha = sha256_file(&descriptor.initramfs_path).map_err(|error| {
        format!(
            "failed to hash initramfs {}: {error}",
            descriptor.initramfs_path.display()
        )
    })?;

    if descriptor.linux_artifact_version != version.kernel {
        return Err(format!(
            "descriptor linux artifact version mismatch: expected {}, got {}",
            descriptor.linux_artifact_version, version.kernel
        ));
    }
    if kernel_sha != version.sha256_vmlinux {
        return Err(format!(
            "kernel checksum mismatch: expected {}, got {}",
            version.sha256_vmlinux, kernel_sha
        ));
    }
    if initramfs_sha != version.sha256_initramfs {
        return Err(format!(
            "initramfs checksum mismatch: expected {}, got {}",
            version.sha256_initramfs, initramfs_sha
        ));
    }
    checks.push(check(
        "descriptor_consistency",
        true,
        "descriptor artifacts and checksums validated",
    ));
    Ok(checks)
}

fn sandbox_backend_name(backend: &SandboxBackend) -> String {
    match backend {
        SandboxBackend::MacosVz => "macos-vz".to_string(),
        SandboxBackend::LinuxFirecracker => "linux-firecracker".to_string(),
        SandboxBackend::Other(value) => value.to_string(),
    }
}

fn validate_linux_vm_base_definition(
    base: runtime_v2::LinuxVmBaseDefinition,
    now: u64,
    request_id: &str,
) -> Result<LinuxVmBaseRecord, MachineError> {
    let base_id = base.base_id.trim();
    if base_id.is_empty() {
        return Err(MachineError::new(
            MachineErrorCode::ValidationError,
            "base_id is required".to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ));
    }
    let kernel_path = PathBuf::from(base.kernel_path.trim());
    let initramfs_path = PathBuf::from(base.initramfs_path.trim());
    let version_json_path = PathBuf::from(base.version_json_path.trim());

    for (name, path) in [
        ("kernel_path", &kernel_path),
        ("initramfs_path", &initramfs_path),
        ("version_json_path", &version_json_path),
    ] {
        if path.as_os_str().is_empty() {
            return Err(MachineError::new(
                MachineErrorCode::ValidationError,
                format!("{name} is required"),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ));
        }
        if !path.is_absolute() {
            return Err(MachineError::new(
                MachineErrorCode::ValidationError,
                format!("{name} must be absolute: {}", path.display()),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ));
        }
        if !path.exists() {
            return Err(MachineError::new(
                MachineErrorCode::NotFound,
                format!("{name} does not exist: {}", path.display()),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ));
        }
    }

    Ok(LinuxVmBaseRecord {
        base_id: base_id.to_string(),
        kernel_path,
        initramfs_path,
        version_json_path,
        description: base.description.trim().to_string(),
        updated_at_unix_secs: now,
    })
}

fn linux_vm_base_record_to_proto(record: &LinuxVmBaseRecord) -> runtime_v2::LinuxVmBaseDefinition {
    runtime_v2::LinuxVmBaseDefinition {
        base_id: record.base_id.clone(),
        kernel_path: record.kernel_path.display().to_string(),
        initramfs_path: record.initramfs_path.display().to_string(),
        version_json_path: record.version_json_path.display().to_string(),
        description: record.description.clone(),
        updated_at_unix_secs: record.updated_at_unix_secs,
    }
}

fn parse_linux_vm_patch_bundle(
    bundle_path: &Path,
    request_id: &str,
) -> Result<LinuxVmPatchBundle, MachineError> {
    let raw = std::fs::read(bundle_path).map_err(|error| {
        let code = if error.kind() == std::io::ErrorKind::NotFound {
            MachineErrorCode::NotFound
        } else {
            MachineErrorCode::InternalError
        };
        MachineError::new(
            code,
            format!(
                "failed to read patch bundle {}: {error}",
                bundle_path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )
    })?;
    let bundle = serde_json::from_slice::<LinuxVmPatchBundle>(&raw).map_err(|error| {
        MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "failed to parse patch bundle {}: {error}",
                bundle_path.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )
    })?;
    if bundle.schema_version != 1 {
        return Err(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "unsupported linux patch bundle schema_version {}",
                bundle.schema_version
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ));
    }
    if bundle.patch_id.trim().is_empty() {
        return Err(MachineError::new(
            MachineErrorCode::ValidationError,
            "patch_id is required".to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ));
    }
    if bundle.base_id.trim().is_empty() {
        return Err(MachineError::new(
            MachineErrorCode::ValidationError,
            "base_id is required".to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ));
    }
    Ok(bundle)
}

fn validate_patch_path_field(
    request_id: &str,
    field_name: &str,
    path_value: &str,
) -> Result<PathBuf, MachineError> {
    let path = PathBuf::from(path_value.trim());
    if path.as_os_str().is_empty() {
        return Err(MachineError::new(
            MachineErrorCode::ValidationError,
            format!("{field_name} cannot be empty"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ));
    }
    if !path.is_absolute() {
        return Err(MachineError::new(
            MachineErrorCode::ValidationError,
            format!("{field_name} must be absolute: {}", path.display()),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ));
    }
    if !path.exists() {
        return Err(MachineError::new(
            MachineErrorCode::NotFound,
            format!("{field_name} does not exist: {}", path.display()),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ));
    }
    Ok(path)
}

fn apply_patch_set_to_base(
    request_id: &str,
    current: &LinuxVmBaseRecord,
    patch: &LinuxVmPatchSet,
    now: u64,
) -> Result<LinuxVmBaseRecord, MachineError> {
    let mut next = current.clone();
    if let Some(kernel_path) = patch.kernel_path.as_deref() {
        next.kernel_path = validate_patch_path_field(request_id, "kernel_path", kernel_path)?;
    }
    if let Some(initramfs_path) = patch.initramfs_path.as_deref() {
        next.initramfs_path =
            validate_patch_path_field(request_id, "initramfs_path", initramfs_path)?;
    }
    if let Some(version_json_path) = patch.version_json_path.as_deref() {
        next.version_json_path =
            validate_patch_path_field(request_id, "version_json_path", version_json_path)?;
    }
    if let Some(description) = patch.description.as_ref() {
        next.description = description.trim().to_string();
    }
    next.updated_at_unix_secs = now;
    Ok(next)
}

fn attach_request_id(mut error: MachineError, request_id: &str) -> MachineError {
    if error.request_id.is_none() {
        error.request_id = Some(request_id.to_string());
    }
    error
}

fn sanitize_id_for_persistence(value: &str) -> String {
    let mut sanitized = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    if sanitized.is_empty() {
        "patch".to_string()
    } else {
        sanitized
    }
}

#[tonic::async_trait]
impl runtime_v2::linux_vm_service_server::LinuxVmService for LinuxVmServiceImpl {
    type ValidateLinuxVmStream = ValidateLinuxVmEventStream;
    type UpsertLinuxVmBaseStream = UpsertLinuxVmBaseEventStream;
    type DeleteLinuxVmBaseStream = DeleteLinuxVmBaseEventStream;
    type ApplyLinuxVmPatchStream = ApplyLinuxVmPatchEventStream;
    type RollbackLinuxVmPatchStream = RollbackLinuxVmPatchEventStream;

    async fn validate_linux_vm(
        &self,
        request: Request<runtime_v2::ValidateLinuxVmRequest>,
    ) -> Result<Response<Self::ValidateLinuxVmStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata
            .request_id
            .clone()
            .unwrap_or_else(generate_request_id);

        let descriptor_path = PathBuf::from(request.descriptor_path.trim());
        if descriptor_path.as_os_str().is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "descriptor_path is required".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let mut sequence = 1;
        let mut checks = Vec::new();
        let mut ok = true;
        let mut events = vec![Ok(linux_vm_progress_event(
            &request_id,
            sequence,
            "descriptor",
            "validating linux descriptor metadata",
        ))];

        match validate_descriptor(&descriptor_path) {
            Ok(mut descriptor_checks) => checks.append(&mut descriptor_checks),
            Err(error) => {
                checks.push(check("descriptor_consistency", false, error));
                ok = false;
            }
        }

        sequence += 1;
        events.push(Ok(linux_vm_progress_event(
            &request_id,
            sequence,
            "daemon",
            "validating daemon linux readiness",
        )));

        if !self
            .daemon
            .backend_name()
            .to_ascii_lowercase()
            .contains("linux")
        {
            checks.push(check(
                "daemon_connectivity",
                false,
                format!(
                    "connected daemon backend `{}` is not linux",
                    self.daemon.backend_name()
                ),
            ));
            ok = false;
        } else {
            checks.push(check(
                "daemon_connectivity",
                true,
                "connected to linux daemon backend",
            ));
        }

        if let Some(sandbox_id) = normalize_optional_wire_field(&request.sandbox_id) {
            match self
                .daemon
                .with_state_store(|store| store.load_sandbox(&sandbox_id))
            {
                Ok(Some(sandbox)) => {
                    let backend = sandbox_backend_name(&sandbox.backend);
                    let sandbox_state = format!("{:?}", sandbox.state).to_ascii_lowercase();
                    if backend.contains("linux") {
                        checks.push(check(
                            "sandbox_readiness",
                            true,
                            format!(
                                "sandbox {} resolved in state {} on backend {}",
                                sandbox_id, sandbox_state, backend
                            ),
                        ));
                    } else {
                        checks.push(check(
                            "sandbox_readiness",
                            false,
                            format!("sandbox {} backend {} is not linux", sandbox_id, backend),
                        ));
                        ok = false;
                    }
                }
                Ok(None) => {
                    checks.push(check(
                        "sandbox_readiness",
                        false,
                        format!("sandbox {} not found", sandbox_id),
                    ));
                    ok = false;
                }
                Err(error) => {
                    checks.push(check("sandbox_readiness", false, error.to_string()));
                    ok = false;
                }
            }
        }

        sequence += 1;
        events.push(Ok(linux_vm_completion_event(
            &request_id,
            sequence,
            &descriptor_path,
            self.daemon.backend_name(),
            ok,
            checks,
        )));

        Ok(Response::new(linux_vm_event_stream_from_events(events)))
    }

    async fn list_linux_vm_bases(
        &self,
        request: Request<runtime_v2::ListLinuxVmBasesRequest>,
    ) -> Result<Response<runtime_v2::ListLinuxVmBasesResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata.request_id.unwrap_or_else(generate_request_id);

        let registry = self
            .load_linux_vm_base_registry()
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;

        let bases = registry
            .bases
            .values()
            .map(linux_vm_base_record_to_proto)
            .collect::<Vec<_>>();

        Ok(Response::new(runtime_v2::ListLinuxVmBasesResponse {
            bases,
        }))
    }

    async fn get_linux_vm_base(
        &self,
        request: Request<runtime_v2::GetLinuxVmBaseRequest>,
    ) -> Result<Response<runtime_v2::LinuxVmBaseResponse>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata.request_id.unwrap_or_else(generate_request_id);

        let base_id = request.base_id.trim();
        if base_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "base_id is required".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let registry = self
            .load_linux_vm_base_registry()
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;
        let Some(base) = registry.bases.get(base_id) else {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("linux vm base {} not found", base_id),
                Some(request_id),
                BTreeMap::new(),
            )));
        };

        Ok(Response::new(runtime_v2::LinuxVmBaseResponse {
            base: Some(linux_vm_base_record_to_proto(base)),
        }))
    }

    async fn upsert_linux_vm_base(
        &self,
        request: Request<runtime_v2::UpsertLinuxVmBaseRequest>,
    ) -> Result<Response<Self::UpsertLinuxVmBaseStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata.request_id.unwrap_or_else(generate_request_id);

        let mut sequence = 1;
        let mut events = vec![Ok(linux_vm_base_upsert_progress_event(
            &request_id,
            sequence,
            "validation",
            "validating linux base definition",
        ))];

        let Some(base_definition) = request.base else {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "base definition is required".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        };

        let base_record =
            validate_linux_vm_base_definition(base_definition, current_unix_secs(), &request_id)
                .map_err(status_from_machine_error)?;

        sequence += 1;
        events.push(Ok(linux_vm_base_upsert_progress_event(
            &request_id,
            sequence,
            "persistence",
            "persisting linux base definition",
        )));

        let mut registry = self
            .load_linux_vm_base_registry()
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;
        registry
            .bases
            .insert(base_record.base_id.clone(), base_record.clone());
        self.persist_linux_vm_base_registry(&registry)
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;

        sequence += 1;
        events.push(Ok(linux_vm_base_upsert_completion_event(
            &request_id,
            sequence,
            linux_vm_base_record_to_proto(&base_record),
        )));

        Ok(Response::new(linux_vm_event_stream_from_events(events)))
    }

    async fn delete_linux_vm_base(
        &self,
        request: Request<runtime_v2::DeleteLinuxVmBaseRequest>,
    ) -> Result<Response<Self::DeleteLinuxVmBaseStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata.request_id.unwrap_or_else(generate_request_id);

        let base_id = request.base_id.trim();
        if base_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "base_id is required".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let mut sequence = 1;
        let mut events = vec![Ok(linux_vm_base_delete_progress_event(
            &request_id,
            sequence,
            "persistence",
            "removing linux base definition",
        ))];

        let mut registry = self
            .load_linux_vm_base_registry()
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;
        if registry.bases.remove(base_id).is_none() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("linux vm base {} not found", base_id),
                Some(request_id),
                BTreeMap::new(),
            )));
        }
        self.persist_linux_vm_base_registry(&registry)
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;

        sequence += 1;
        events.push(Ok(linux_vm_base_delete_completion_event(
            &request_id,
            sequence,
            base_id,
        )));

        Ok(Response::new(linux_vm_event_stream_from_events(events)))
    }

    async fn apply_linux_vm_patch(
        &self,
        request: Request<runtime_v2::ApplyLinuxVmPatchRequest>,
    ) -> Result<Response<Self::ApplyLinuxVmPatchStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata.request_id.unwrap_or_else(generate_request_id);

        let bundle_path = PathBuf::from(request.bundle_path.trim());
        if bundle_path.as_os_str().is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "bundle_path is required".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let mut sequence = 1;
        let mut events = vec![Ok(linux_vm_patch_apply_progress_event(
            &request_id,
            sequence,
            "bundle",
            "loading linux patch bundle",
        ))];

        let bundle = parse_linux_vm_patch_bundle(&bundle_path, &request_id)
            .map_err(status_from_machine_error)?;
        let now = current_unix_secs();
        let rollback_id = format!(
            "lrb-{}-{now}",
            sanitize_id_for_persistence(&bundle.patch_id)
        );

        sequence += 1;
        events.push(Ok(linux_vm_patch_apply_progress_event(
            &request_id,
            sequence,
            "apply",
            "applying linux patch to base definition",
        )));

        let mut registry = self
            .load_linux_vm_base_registry()
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;
        let Some(current_base) = registry.bases.get(bundle.base_id.as_str()).cloned() else {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("linux vm base {} not found", bundle.base_id),
                Some(request_id),
                BTreeMap::new(),
            )));
        };
        let next_base = apply_patch_set_to_base(&request_id, &current_base, &bundle.set, now)
            .map_err(status_from_machine_error)?;
        let rollback_snapshot = LinuxVmPatchRollbackSnapshot {
            rollback_id: rollback_id.clone(),
            patch_id: bundle.patch_id.clone(),
            base_id: bundle.base_id.clone(),
            previous: current_base,
            created_at_unix_secs: now,
        };
        self.persist_patch_rollback_snapshot(&rollback_snapshot)
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;
        registry
            .bases
            .insert(next_base.base_id.clone(), next_base.clone());
        self.persist_linux_vm_base_registry(&registry)
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;

        let receipt_id = format!("rcp-linux-patch-apply-{now}");
        let receipt_metadata = serde_json::to_value(LinuxVmPatchApplyReceiptMetadata {
            event_type: "linux_vm_patch_applied",
            patch_id: bundle.patch_id.clone(),
            rollback_id: rollback_id.clone(),
            bundle_path: bundle_path.display().to_string(),
            base_id: next_base.base_id.clone(),
        })
        .map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::InternalError,
                format!("failed to serialize patch apply receipt metadata: {error}"),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;
        let event_patch_id = bundle.patch_id.clone();
        let event_rollback_id = rollback_id.clone();
        let event_base_id = next_base.base_id.clone();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.emit_event(
                        LINUX_VM_PATCH_EVENT_STACK,
                        &StackEvent::LinuxVmPatchApplied {
                            base_id: event_base_id.clone(),
                            patch_id: event_patch_id.clone(),
                            rollback_id: event_rollback_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: LINUX_VM_PATCH_APPLY_OPERATION.to_string(),
                        entity_id: event_base_id.clone(),
                        entity_type: "linux_vm_base".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_metadata.clone(),
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        sequence += 1;
        events.push(Ok(linux_vm_patch_apply_completion_event(
            &request_id,
            sequence,
            linux_vm_base_record_to_proto(&next_base),
            event_patch_id.as_str(),
            event_rollback_id.as_str(),
            receipt_id.as_str(),
        )));

        Ok(Response::new(linux_vm_event_stream_from_events(events)))
    }

    async fn rollback_linux_vm_patch(
        &self,
        request: Request<runtime_v2::RollbackLinuxVmPatchRequest>,
    ) -> Result<Response<Self::RollbackLinuxVmPatchStream>, Status> {
        let intercepted_request_id = request_id_from_extensions(&request);
        let request = request.into_inner();
        let metadata = normalize_metadata(request.metadata.as_ref(), intercepted_request_id);
        let request_id = metadata.request_id.unwrap_or_else(generate_request_id);

        let rollback_id = request.rollback_id.trim().to_string();
        if rollback_id.is_empty() {
            return Err(status_from_machine_error(MachineError::new(
                MachineErrorCode::ValidationError,
                "rollback_id is required".to_string(),
                Some(request_id),
                BTreeMap::new(),
            )));
        }

        let mut sequence = 1;
        let mut events = vec![Ok(linux_vm_patch_rollback_progress_event(
            &request_id,
            sequence,
            "rollback",
            "loading rollback snapshot",
        ))];

        let snapshot = self
            .load_patch_rollback_snapshot(rollback_id.as_str())
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;

        let now = current_unix_secs();
        let mut registry = self
            .load_linux_vm_base_registry()
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;
        registry
            .bases
            .insert(snapshot.base_id.clone(), snapshot.previous.clone());
        self.persist_linux_vm_base_registry(&registry)
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;
        self.delete_patch_rollback_snapshot(rollback_id.as_str())
            .map_err(|error| status_from_machine_error(attach_request_id(error, &request_id)))?;

        let receipt_id = format!("rcp-linux-patch-rollback-{now}");
        let receipt_metadata = serde_json::to_value(LinuxVmPatchRollbackReceiptMetadata {
            event_type: "linux_vm_patch_rolled_back",
            patch_id: snapshot.patch_id.clone(),
            rollback_id: snapshot.rollback_id.clone(),
            base_id: snapshot.base_id.clone(),
        })
        .map_err(|error| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::InternalError,
                format!("failed to serialize patch rollback receipt metadata: {error}"),
                Some(request_id.clone()),
                BTreeMap::new(),
            ))
        })?;
        let event_base_id = snapshot.base_id.clone();
        let event_patch_id = snapshot.patch_id.clone();
        let event_rollback_id = snapshot.rollback_id.clone();
        self.daemon
            .with_state_store(|store| {
                store.with_immediate_transaction(|tx| {
                    tx.emit_event(
                        LINUX_VM_PATCH_EVENT_STACK,
                        &StackEvent::LinuxVmPatchRolledBack {
                            base_id: event_base_id.clone(),
                            patch_id: event_patch_id.clone(),
                            rollback_id: event_rollback_id.clone(),
                        },
                    )?;
                    tx.save_receipt(&Receipt {
                        receipt_id: receipt_id.clone(),
                        operation: LINUX_VM_PATCH_ROLLBACK_OPERATION.to_string(),
                        entity_id: event_base_id.clone(),
                        entity_type: "linux_vm_base".to_string(),
                        request_id: request_id.clone(),
                        status: "success".to_string(),
                        created_at: now,
                        metadata: receipt_metadata.clone(),
                    })?;
                    Ok(())
                })
            })
            .map_err(|error| status_from_stack_error(error, &request_id))?;

        sequence += 1;
        events.push(Ok(linux_vm_patch_rollback_progress_event(
            &request_id,
            sequence,
            "rollback",
            "restored linux base definition from rollback snapshot",
        )));

        sequence += 1;
        events.push(Ok(linux_vm_patch_rollback_completion_event(
            &request_id,
            sequence,
            linux_vm_base_record_to_proto(&snapshot.previous),
            event_patch_id.as_str(),
            event_rollback_id.as_str(),
            receipt_id.as_str(),
        )));

        Ok(Response::new(linux_vm_event_stream_from_events(events)))
    }
}
