use super::super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use vz_runtime_contract::SandboxBackend;

pub(in crate::grpc) struct LinuxVmServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl LinuxVmServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

type ValidateLinuxVmEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::ValidateLinuxVmEvent, Status>>;

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

#[tonic::async_trait]
impl runtime_v2::linux_vm_service_server::LinuxVmService for LinuxVmServiceImpl {
    type ValidateLinuxVmStream = ValidateLinuxVmEventStream;

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
}
