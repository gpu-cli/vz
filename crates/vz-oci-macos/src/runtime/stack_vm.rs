use super::bundle::{
    container_log_dir, make_oci_runtime_share, mount_specs_to_bundle_mounts, oci_bundle_guest_path,
    oci_bundle_guest_root, oci_bundle_host_dir, resolve_oci_runtime_binary_path,
    setup_stack_guest_container_overlay, teardown_guest_container_overlay,
};
use super::networking::{
    shutdown_port_forwarding_registry_entry, start_port_forwarding, stop_or_reuse_exit_code,
};
use super::resolve::{current_unix_secs, resolve_container_lifecycle, resolve_run_config};
use super::*;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};

const DOCKER_BIN_SHARE_TAG: &str = "vz-docker-bin";
const DOCKER_YOUKI_SHARE_TAG: &str = "linux-bin";
const DOCKER_DATA_DEVICE: &str = "/dev/vda";
const DOCKER_GUEST_SOCKET: &str = "/run/vz-docker/docker.sock";
const NAMED_VOLUME_DEVICE_WITH_DOCKER: &str = "/dev/vdb";
const NAMED_VOLUME_DEVICE_WITHOUT_DOCKER: &str = "/dev/vda";
const DOCKER_DATA_DISK_SIZE_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const DOCKER_FORMAT_INTENT_VERSION: &str = "vz-private-docker-disk-format-v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GuestDiskPhase {
    Probe,
    Format,
}

impl GuestDiskPhase {
    pub(super) const fn timeout(self) -> Duration {
        match self {
            Self::Probe => Duration::from_secs(30),
            // The pinned ext4 formatter eagerly initializes metadata on the 64 GiB
            // private disk. That write workload needs a separate bounded
            // budget from read-only blkid, especially under host I/O load.
            // The outer Up supervisor/fence retains unresolved boot uncertainty;
            // this does not establish a recoverable early-boot VM handle. The
            // local bound never authorizes a retry or proves quiescence.
            Self::Format => Duration::from_secs(180),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Probe => "probe",
            Self::Format => "format",
        }
    }
}

pub(super) fn guest_disk_phase_error(
    phase: GuestDiskPhase,
    purpose: &str,
    device: &str,
    elapsed: Duration,
    reason: &str,
) -> OciError {
    let message = format!(
        "guest disk {} failed: purpose={purpose}, device={device}, budget={:.3}s, elapsed={:.3}s: {reason}; disk preparation completion is not proven",
        phase.label(),
        phase.timeout().as_secs_f64(),
        elapsed.as_secs_f64(),
    );
    tracing::warn!(phase = phase.label(), purpose, device, "{message}");
    OciError::InvalidConfig(message)
}

pub(super) const DOCKER_DISK_BOOTSTRAP_SCRIPT: &str = r#"
set -eu
umask 077
device="$1"
root=/run/vz-docker-bootstrap
/bin/busybox mkdir -p "$root"
/bin/busybox mount -t ext4 "$device" "$root"
cleanup() { /bin/busybox umount "$root"; }
trap cleanup EXIT
/bin/busybox mkdir -p "$root/config" "$root/containerd" "$root/engine" "$root/log"
mount_identity=$(/bin/busybox awk -v path="$root" '$2 == path { print $1 " " $3; count++ } END { if (count != 1) exit 1 }' /proc/mounts)
test "$mount_identity" = "$device ext4"
create_config_once() {
  path="$1"
  contents="$2"
  if test -e "$path" || test -L "$path"; then
    test -f "$path"
    test ! -L "$path"
    test "$(/bin/busybox stat -Lc '%h' "$path")" = 1
    return
  fi
  (set -C; : > "$path")
  printf '%s' "$contents" > "$path"
}
create_config_once "$root/config/containerd.toml" ''
create_config_once "$root/config/daemon.json" '{}'
test ! -s "$root/config/containerd.toml"
/bin/busybox chmod 700 "$root" "$root/config" "$root/containerd" "$root/engine" "$root/log"
/bin/busybox chmod 600 "$root/config/containerd.toml" "$root/config/daemon.json"
/bin/busybox sync
cleanup
trap - EXIT
"#;

pub(super) const SHARED_VM_FULL_CHECKPOINT_UNSUPPORTED_REASON: &str = "vm_full_checkpoint=false: shared VM state depends on external VirtioFS/device state that is not captured atomically";

pub(super) fn classify_stack_runtime_shutdown(
    current: Option<&vz_runtime_contract::StackRuntimeIdentity>,
    expected: &vz_runtime_contract::StackRuntimeIdentity,
) -> vz_runtime_contract::StackRuntimeShutdownOutcome {
    match current {
        None => vz_runtime_contract::StackRuntimeShutdownOutcome::AlreadyAbsent,
        Some(current) if current == expected => {
            vz_runtime_contract::StackRuntimeShutdownOutcome::Stopped
        }
        Some(current) => vz_runtime_contract::StackRuntimeShutdownOutcome::ReplacementPresent {
            current: current.clone(),
        },
    }
}

pub(super) fn require_exact_stack_runtime(
    current: Option<&vz_runtime_contract::StackRuntimeIdentity>,
    expected: &vz_runtime_contract::StackRuntimeIdentity,
) -> Result<(), OciError> {
    match current {
        None => Err(OciError::SharedRuntimeAbsent {
            stack_id: expected.stack_id.clone(),
        }),
        Some(current) if current == expected => Ok(()),
        Some(current) => Err(OciError::SharedRuntimeIdentityMismatch {
            stack_id: expected.stack_id.clone(),
            expected_incarnation_id: expected.incarnation_id.clone(),
            current_incarnation_id: current.incarnation_id.clone(),
        }),
    }
}

pub(super) fn kernel_profile_from_metadata(
    version: &vz_linux::KernelVersion,
) -> Option<KernelProfile> {
    match version.profile.as_deref() {
        Some("developer") => Some(KernelProfile::Developer),
        Some("container") => Some(KernelProfile::Container),
        _ => None,
    }
}

pub(super) fn require_explicit_verified_profile(
    configured: Option<KernelProfile>,
    verified: Option<KernelProfile>,
    operation: &str,
) -> Result<KernelProfile, OciError> {
    let Some(configured) = configured else {
        return Err(OciError::UnsupportedOperation {
            operation: operation.to_string(),
            reason: "managed shared-VM acquisition requires an explicit Linux profile".to_string(),
        });
    };
    if verified != Some(configured) {
        return Err(OciError::UnsupportedOperation {
            operation: operation.to_string(),
            reason: format!(
                "configured Linux profile `{}` was not proven by the selected boot artifact",
                configured.as_str()
            ),
        });
    }
    Ok(configured)
}

pub(super) fn require_matching_shared_vm_boot_request(
    stack_id: &str,
    actual_ports: &[PortMapping],
    actual_resources: &vz_runtime_contract::StackResourceHint,
    requested_ports: &[PortMapping],
    requested_resources: &vz_runtime_contract::StackResourceHint,
) -> Result<(), OciError> {
    let resources_match = actual_resources.cpus == requested_resources.cpus
        && actual_resources.memory_mb == requested_resources.memory_mb
        && actual_resources.volume_mounts == requested_resources.volume_mounts
        && actual_resources.disk_image_path == requested_resources.disk_image_path;
    if actual_ports == requested_ports && resources_match {
        return Ok(());
    }
    Err(OciError::InvalidConfig(format!(
        "shared VM boot request drift for stack '{stack_id}': the active boot has different ports or resources"
    )))
}

pub(super) fn require_docker_provisioned_developer_profile(
    verified_profile: Option<KernelProfile>,
    docker_provisioned: bool,
    operation: &str,
) -> Result<KernelProfile, OciError> {
    if verified_profile != Some(KernelProfile::Developer) {
        return Err(OciError::UnsupportedOperation {
            operation: operation.to_string(),
            reason: format!(
                "Docker requires an actually verified Developer Linux boot; active profile is {verified_profile:?}"
            ),
        });
    }
    if !docker_provisioned {
        return Err(OciError::UnsupportedOperation {
            operation: operation.to_string(),
            reason: "active Developer Linux boot has no private Docker disk and binary shares"
                .to_string(),
        });
    }
    Ok(KernelProfile::Developer)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GuestDiskProbe {
    ExtFilesystem,
    Unformatted,
}

pub(super) fn classify_guest_disk_probe(
    exit_code: i32,
    stdout: &str,
    stderr: &str,
    allow_unformatted: bool,
) -> Result<GuestDiskProbe, String> {
    let output = stdout.trim();
    if allow_unformatted && exit_code == 0 && output.is_empty() && stderr.trim().is_empty() {
        return Ok(GuestDiskProbe::Unformatted);
    }
    if exit_code == 0
        && (output.contains("TYPE=\"ext2\"")
            || output.contains("TYPE=\"ext3\"")
            || output.contains("TYPE=\"ext4\""))
    {
        return Ok(GuestDiskProbe::ExtFilesystem);
    }
    Err(format!(
        "refusing to format an existing or unrecognized disk: exit_code={exit_code}, stdout={output:?}, stderr={:?}",
        stderr.trim()
    ))
}

/// Docker disks must be journaled ext4 and positively clean before admission.
/// This reads metadata only: incompatible or damaged disks are never repaired,
/// reformatted, or accepted merely because the ext4 driver can mount ext2.
fn validate_docker_filesystem_header(header: &str) -> Result<(), String> {
    let field = |name: &str| -> Result<&str, String> {
        let mut values = header.lines().filter_map(|line| {
            line.split_once(':')
                .filter(|(key, _)| *key == name)
                .map(|(_, value)| value.trim())
        });
        let value = values.next().ok_or_else(|| format!("missing {name}"))?;
        if value.is_empty() || values.next().is_some() {
            return Err(format!("empty or duplicate {name}"));
        }
        Ok(value)
    };
    let uuid = field("Filesystem UUID")?;
    if uuid.len() != 36
        || !uuid.chars().enumerate().all(|(index, value)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                value == '-'
            } else {
                value.is_ascii_hexdigit()
            }
        })
        || uuid == "00000000-0000-0000-0000-000000000000"
    {
        return Err("missing or malformed filesystem UUID".to_string());
    }
    let features: Vec<_> = field("Filesystem features")?.split_whitespace().collect();
    if !features.contains(&"has_journal") || !features.contains(&"extent") {
        return Err("Docker data requires ext4 with has_journal and extent".to_string());
    }
    if features.contains(&"needs_recovery") || field("Filesystem state")? != "clean" {
        return Err(
            "Docker filesystem is not positively clean; automatic repair is forbidden".to_string(),
        );
    }
    if header
        .lines()
        .any(|line| line.starts_with("FS Error count:"))
        && field("FS Error count")? != "0"
    {
        return Err(
            "Docker filesystem has recorded errors; automatic repair is forbidden".to_string(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod docker_filesystem_tests {
    use super::{
        classify_guest_disk_probe, require_complete_shared_vm_boot,
        validate_docker_filesystem_header,
    };

    #[test]
    fn incomplete_boot_is_owned_but_cannot_be_reused_or_power_stopped() {
        assert!(require_complete_shared_vm_boot("exact-machine", true).is_ok());
        let error = require_complete_shared_vm_boot("exact-machine", false).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("exact guest exec reconciliation")
        );
        assert!(error.to_string().contains("exact-machine"));
    }

    #[test]
    fn bootstrap_publishes_ownership_before_start_and_never_power_stops_on_error() {
        let source = include_str!("stack_vm.rs");
        let boot = source
            .rsplit_once("async fn boot_shared_vm_locked(")
            .unwrap()
            .1
            .split_once("/// Create and start an OCI container")
            .unwrap()
            .0;
        let published = boot.find("self.stack_vms.lock().await.insert(").unwrap();
        let started = boot.find("vm.start().await?").unwrap();
        assert!(published < started);
        assert!(!boot.contains("vm.stop()"));
        assert!(boot.contains("boot_complete: false"));
        assert!(boot.contains("record.boot_complete = true"));
    }

    const CLEAN: &str = "Filesystem UUID: 8b0fa999-c711-4717-af6a-bddfeacdeeee\nFilesystem features: has_journal ext_attr dir_index filetype extent 64bit metadata_csum\nFilesystem state: clean\n";

    #[test]
    fn accepts_clean_journaled_ext4_header() {
        assert!(validate_docker_filesystem_header(CLEAN).is_ok());
        assert!(validate_docker_filesystem_header(&format!("{CLEAN}FS Error count: 0\n")).is_ok());
    }

    #[test]
    fn rejects_legacy_dirty_or_corrupt_filesystems_without_repair() {
        for header in [
            CLEAN.replace("has_journal ", ""),
            CLEAN.replace("extent ", ""),
            CLEAN.replace("clean", "not clean"),
            CLEAN.replace("clean", "clean with errors"),
            CLEAN.replace("has_journal", "has_journal needs_recovery"),
            format!("{CLEAN}FS Error count: 7\n"),
        ] {
            assert!(
                validate_docker_filesystem_header(&header).is_err(),
                "{header}"
            );
        }
    }

    #[test]
    fn rejects_missing_duplicated_or_malformed_filesystem_proof() {
        for header in [
            String::new(),
            CLEAN.replace("Filesystem UUID:", "Other UUID:"),
            CLEAN.replace("8b0fa999-c711-4717-af6a-bddfeacdeeee", "<none>"),
            CLEAN.replace(
                "8b0fa999-c711-4717-af6a-bddfeacdeeee",
                "00000000-0000-0000-0000-000000000000",
            ),
            format!("{CLEAN}Filesystem state: clean\n"),
            format!("{CLEAN}Filesystem features: has_journal extent\n"),
            format!("{CLEAN}FS Error count: 0\nFS Error count: 0\n"),
        ] {
            assert!(
                validate_docker_filesystem_header(&header).is_err(),
                "{header}"
            );
        }
    }

    #[test]
    fn failed_empty_probe_is_not_format_authority() {
        assert!(classify_guest_disk_probe(2, "", "", true).is_err());
        assert!(classify_guest_disk_probe(-1, "", "", true).is_err());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PrivateDiskDisposition {
    FormatAuthorized,
    Existing,
}

fn private_disk_format_intent_path(path: &Path) -> Result<PathBuf, OciError> {
    let file_name = path.file_name().ok_or_else(|| {
        OciError::InvalidConfig(format!(
            "Docker data disk has no filename: {}",
            path.display()
        ))
    })?;
    let mut marker_name = file_name.to_os_string();
    marker_name.push(".format-intent");
    Ok(path.with_file_name(marker_name))
}

fn pending_private_disk_format_intent(size: u64) -> String {
    format!("{DOCKER_FORMAT_INTENT_VERSION}\nsize={size}\nstate=pending\n")
}

fn bound_private_disk_format_intent(size: u64, metadata: &fs::Metadata) -> String {
    format!(
        "{DOCKER_FORMAT_INTENT_VERSION}\nsize={size}\ndev={}\nino={}\n",
        metadata.dev(),
        metadata.ino()
    )
}

fn validate_private_disk_format_intent(
    path: &Path,
    size: u64,
    disk_metadata: Option<&fs::Metadata>,
) -> Result<bool, OciError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.nlink() != 1
        || metadata.len() > 128
    {
        return Err(OciError::InvalidConfig(format!(
            "Docker disk format intent must be a small regular non-symlink file: {}",
            path.display()
        )));
    }
    let expected = disk_metadata.map_or_else(
        || pending_private_disk_format_intent(size),
        |metadata| bound_private_disk_format_intent(size, metadata),
    );
    if fs::read_to_string(path)? != expected {
        return Err(OciError::InvalidConfig(format!(
            "Docker disk format intent is invalid: {}",
            path.display()
        )));
    }
    Ok(true)
}

fn write_private_disk_format_intent(
    path: &Path,
    contents: &str,
    create_new: bool,
) -> Result<(), OciError> {
    let mut options = OpenOptions::new();
    options.write(true);
    if create_new {
        options.create_new(true);
    } else {
        options.truncate(true);
    }
    let mut marker = options.open(path)?;
    use std::io::Write;
    marker.write_all(contents.as_bytes())?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    marker.sync_all()?;
    Ok(())
}

pub(super) fn ensure_private_sparse_disk(
    path: &Path,
    size: u64,
) -> Result<PrivateDiskDisposition, OciError> {
    let parent = path.parent().ok_or_else(|| {
        OciError::InvalidConfig(format!(
            "Docker data disk has no parent directory: {}",
            path.display()
        ))
    })?;
    let mut directory = fs::DirBuilder::new();
    directory.recursive(true).mode(0o700);
    directory.create(parent)?;
    let parent_metadata = fs::symlink_metadata(parent)?;
    if !parent_metadata.is_dir() || parent_metadata.file_type().is_symlink() {
        return Err(OciError::InvalidConfig(format!(
            "Docker data directory must be a non-symlink directory: {}",
            parent.display()
        )));
    }
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;

    let existing_metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.nlink() != 1 {
                return Err(OciError::InvalidConfig(format!(
                    "Docker data disk must be a single-link regular non-symlink file: {}",
                    path.display()
                )));
            }
            if metadata.len() != size {
                return Err(OciError::InvalidConfig(format!(
                    "existing Docker data disk has unexpected size: {}",
                    path.display()
                )));
            }
            Some(metadata)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };

    let intent_path = private_disk_format_intent_path(path)?;
    let mut format_authorized =
        validate_private_disk_format_intent(&intent_path, size, existing_metadata.as_ref())?;
    if existing_metadata.is_none() && !format_authorized {
        write_private_disk_format_intent(
            &intent_path,
            &pending_private_disk_format_intent(size),
            true,
        )?;
        File::open(parent)?.sync_all()?;
        format_authorized = true;
    }

    let file = if existing_metadata.is_some() {
        OpenOptions::new().read(true).write(true).open(path)?
    } else {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        if let Err(error) = file.set_len(size) {
            drop(file);
            let _ = fs::remove_file(path);
            return Err(error.into());
        }
        let metadata = file.metadata()?;
        write_private_disk_format_intent(
            &intent_path,
            &bound_private_disk_format_intent(size, &metadata),
            false,
        )?;
        file
    };
    #[cfg(unix)]
    {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    file.sync_all()?;
    File::open(parent)?.sync_all()?;
    Ok(if format_authorized {
        PrivateDiskDisposition::FormatAuthorized
    } else {
        PrivateDiskDisposition::Existing
    })
}

pub(super) fn complete_private_disk_format(
    path: &Path,
    size: u64,
    disposition: PrivateDiskDisposition,
) -> Result<(), OciError> {
    if disposition != PrivateDiskDisposition::FormatAuthorized {
        return Ok(());
    }
    let intent_path = private_disk_format_intent_path(path)?;
    let disk_metadata = fs::symlink_metadata(path)?;
    validate_private_disk_format_intent(&intent_path, size, Some(&disk_metadata))?;
    fs::remove_file(intent_path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Owned proof that one stack's complete guest activation transaction is
/// serialized. The first overlay mutation requires this value, and its drop
/// scope extends through OCI activation and post-start validation.
pub(super) struct StackActivationGuard {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

pub(super) fn require_running_pid(
    container_id: &str,
    phase: &str,
    state: &OciContainerState,
) -> Result<u32, OciError> {
    if state.status != "running" {
        return Err(OciError::InvalidConfig(format!(
            "container '{container_id}' is not running during {phase}: status='{}', pid={:?}",
            state.status, state.pid
        )));
    }

    state.pid.filter(|pid| *pid > 0).ok_or_else(|| {
        OciError::InvalidConfig(format!(
            "container '{container_id}' has no running pid during {phase}"
        ))
    })
}

pub(super) fn require_successful_hosts_write(
    container_id: &str,
    output: &ExecOutput,
) -> Result<(), OciError> {
    if output.exit_code == 0 {
        return Ok(());
    }
    Err(OciError::InvalidConfig(format!(
        "container '{}' /etc/hosts write failed with exit code {}: {}",
        container_id,
        output.exit_code,
        output.stderr.trim()
    )))
}

const WRITE_HOSTS_SCRIPT: &str = "set -eu; printf '%s' \"$1\" > /etc/hosts";

pub(super) fn hosts_write_command(content: String) -> (String, Vec<String>) {
    (
        "/bin/sh".to_string(),
        vec![
            "-c".to_string(),
            WRITE_HOSTS_SCRIPT.to_string(),
            "vz-write-hosts".to_string(),
            content,
        ],
    )
}

pub(super) fn activation_error_with_rollback(
    activation_error: OciError,
    rollback: Result<(), OciError>,
) -> OciError {
    match rollback {
        Ok(()) => activation_error,
        Err(rollback_error) => OciError::InvalidConfig(format!(
            "stack container activation failed: {activation_error}; rollback also failed: {rollback_error}"
        )),
    }
}

pub(super) async fn publish_recovery_route_first<V: Clone>(
    routes: &Mutex<HashMap<String, String>>,
    handles: &Mutex<HashMap<String, V>>,
    stack_id: &str,
    container_id: &str,
    handle: &V,
    after_route: impl Future<Output = ()>,
) {
    routes
        .lock()
        .await
        .insert(container_id.to_string(), stack_id.to_string());
    after_route.await;
    handles
        .lock()
        .await
        .insert(container_id.to_string(), handle.clone());
}

pub(super) async fn clear_recovery_route_last<V>(
    routes: &Mutex<HashMap<String, String>>,
    handles: &Mutex<HashMap<String, V>>,
    container_id: &str,
    after_handle: impl Future<Output = ()>,
) {
    handles.lock().await.remove(container_id);
    after_handle.await;
    routes.lock().await.remove(container_id);
}

pub(super) async fn shutdown_container_cleanup_transition<
    Delete,
    DeleteFuture,
    Overlay,
    OverlayFuture,
>(
    runtime: &Runtime,
    container_id: &str,
    generation: ContainerGeneration,
    delete: Delete,
    overlay: Overlay,
) -> Result<(), OciError>
where
    Delete: FnOnce() -> DeleteFuture,
    DeleteFuture: Future<Output = Result<(), OciError>>,
    Overlay: FnOnce() -> OverlayFuture,
    OverlayFuture: Future<Output = Result<(), OciError>>,
{
    if runtime.stack_guest_cleanup_is_complete(container_id, generation) {
        return Ok(());
    }
    if !runtime.overlay_cleanup_is_pending(container_id, generation) {
        delete().await.map_err(|error| {
            OciError::InvalidConfig(format!(
                "container '{container_id}' OCI delete failed before overlay teardown; retained running metadata, recovery routing, and VM for retry: {error}"
            ))
        })?;
    }

    // Close the cancellation window after successful deletion before any
    // further await. A retry that sees this marker skips the destructive
    // delete and resumes at stopped-state publication/overlay teardown.
    runtime.mark_overlay_cleanup_pending(container_id, generation);
    let metadata_error = runtime
        .record_shutdown_delete_success(container_id, generation)
        .err();
    let overlay_error = overlay().await.err();

    if metadata_error.is_some() || overlay_error.is_some() {
        let mut failures = Vec::new();
        if let Some(error) = metadata_error {
            failures.push(format!("stopped-state publication failed: {error}"));
        }
        if let Some(error) = overlay_error {
            failures.push(format!("overlay teardown failed: {error}"));
        }
        return Err(OciError::InvalidConfig(format!(
            "container '{container_id}' cleanup retained recovery routing, VM, and pending marker for retry: {}",
            failures.join("; ")
        )));
    }

    runtime.mark_stack_guest_cleanup_complete(container_id, generation);

    // Do not clear this member's recovery state yet. Stack shutdown is a batch:
    // a later member may fail, and retry discovers membership from routes.
    // The synchronous batch commit below clears every registry only after all
    // members have completed both destructive phases.
    Ok(())
}

pub(super) async fn commit_stack_cleanup_batch<V>(
    runtime: &Runtime,
    routes: &Mutex<HashMap<String, String>>,
    handles: &Mutex<HashMap<String, V>>,
    container_ids: &[String],
) {
    // Acquire every async registry before mutating any of them. Cancellation
    // while waiting therefore leaves the complete batch discoverable. Once all
    // guards are held, the commit contains no await and cannot be interrupted.
    let mut handles = handles.lock().await;
    let mut routes = routes.lock().await;
    let mut active_lifecycle = runtime.active_lifecycle.lock().await;
    let mut setup_restored = runtime.setup_restored_containers.lock().await;
    let mut pending = runtime
        .oci_deleted_pending_overlay
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for container_id in container_ids {
        handles.remove(container_id);
        routes.remove(container_id);
        active_lifecycle.remove(container_id);
        setup_restored.remove(container_id);
        pending.remove(container_id);
        runtime
            .stack_guest_cleanup_complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(container_id);
    }
}

impl Runtime {
    pub(super) fn stack_guest_cleanup_is_complete(
        &self,
        container_id: &str,
        generation: ContainerGeneration,
    ) -> bool {
        self.stack_guest_cleanup_complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(container_id)
            .is_some_and(|complete| *complete == generation)
    }

    fn mark_stack_guest_cleanup_complete(
        &self,
        container_id: &str,
        generation: ContainerGeneration,
    ) {
        self.stack_guest_cleanup_complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(container_id.to_string(), generation);
    }

    pub(super) fn stack_vm_stop_is_complete(&self, stack_id: &str) -> bool {
        self.stack_vm_stop_complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(stack_id)
    }

    pub(super) fn mark_stack_vm_stop_complete(&self, stack_id: &str) {
        self.stack_vm_stop_complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(stack_id.to_string());
    }

    pub(super) fn clear_stack_vm_stop_complete(&self, stack_id: &str) {
        self.stack_vm_stop_complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(stack_id);
    }

    async fn ensure_stack_not_tearing_down(
        &self,
        stack_id: &str,
        operation: &str,
    ) -> Result<(), OciError> {
        if let Some(record) = self.stack_vms.lock().await.get(stack_id) {
            require_complete_shared_vm_boot(stack_id, record.boot_complete)?;
        }
        let stack_container_ids = self
            .container_stack
            .lock()
            .await
            .iter()
            .filter_map(|(container_id, member_stack_id)| {
                (member_stack_id == stack_id).then_some(container_id.clone())
            })
            .collect::<HashSet<_>>();
        let guest_cleanup_complete = self
            .stack_guest_cleanup_complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .any(|container_id| stack_container_ids.contains(container_id));
        let guest_cleanup_pending = self
            .oci_deleted_pending_overlay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .any(|container_id| stack_container_ids.contains(container_id));

        if self.stack_vm_stop_is_complete(stack_id)
            || guest_cleanup_complete
            || guest_cleanup_pending
        {
            return Err(OciError::InvalidConfig(format!(
                "cannot {operation} stack '{stack_id}' while teardown cleanup is pending"
            )));
        }
        Ok(())
    }

    async fn publish_stack_overlay_recovery_route(
        &self,
        stack_id: &str,
        container_id: &str,
        vm: &Arc<LinuxVm>,
    ) {
        // Route first: if cancellation occurs while acquiring the VM-handle
        // map, stack shutdown can still discover the overlay through stack_vms.
        publish_recovery_route_first(
            &self.container_stack,
            &self.vm_handles,
            stack_id,
            container_id,
            vm,
            self.observe_lifecycle_admission(
                RuntimeLifecycleAdmissionKind::StackRoutePublishedBeforeOverlay,
                container_id,
            ),
        )
        .await;
    }

    async fn clear_stack_overlay_recovery_route(&self, container_id: &str) {
        // Clear route last: cancellation between these operations leaves the
        // stack fallback discoverable even after the direct handle is gone.
        clear_recovery_route_last(
            &self.container_stack,
            &self.vm_handles,
            container_id,
            std::future::ready(()),
        )
        .await;
    }

    pub(super) fn mark_overlay_cleanup_pending(
        &self,
        container_id: &str,
        generation: ContainerGeneration,
    ) {
        self.oci_deleted_pending_overlay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(container_id.to_string(), generation);
    }

    pub(super) fn overlay_cleanup_is_pending(
        &self,
        container_id: &str,
        generation: ContainerGeneration,
    ) -> bool {
        self.oci_deleted_pending_overlay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(container_id)
            .is_some_and(|pending| *pending == generation)
    }

    pub(super) fn clear_overlay_cleanup_pending(&self, container_id: &str) {
        self.oci_deleted_pending_overlay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(container_id);
    }

    pub(super) fn record_shutdown_delete_success(
        &self,
        container_id: &str,
        generation: ContainerGeneration,
    ) -> Result<(), OciError> {
        self.mark_overlay_cleanup_pending(container_id, generation);
        let mut container = self
            .container_store
            .load_all()
            .map_err(OciError::from)?
            .into_iter()
            .find(|container| container.id == container_id)
            .ok_or_else(|| OciError::ContainerNotFound {
                id: container_id.to_string(),
            })?;
        container.status = ContainerStatus::Stopped { exit_code: 0 };
        container.stopped_unix_secs = Some(current_unix_secs());
        container.host_pid = None;
        self.container_store
            .upsert_if_generation(container, generation)
            .map_err(|error| Self::map_container_store_error(container_id, error))
    }

    pub(super) async fn teardown_owned_stack_container_overlay(
        &self,
        vm: &LinuxVm,
        container_id: &str,
        generation: ContainerGeneration,
    ) -> Result<(), OciError> {
        let current = self
            .container_store
            .current_generation(container_id)
            .map_err(|error| Self::map_container_store_error(container_id, error))?;
        if current != Some(generation) {
            return Err(OciError::InvalidConfig(format!(
                "refusing to tear down shared-VM overlay for stale container generation '{container_id}'"
            )));
        }
        teardown_guest_container_overlay(vm, container_id).await
    }

    async fn stack_prepare_error_with_overlay_cleanup(
        &self,
        vm: &Arc<LinuxVm>,
        _stack_id: &str,
        container_id: &str,
        generation: ContainerGeneration,
        prepare_error: OciError,
    ) -> OciError {
        self.setup_restored_containers
            .lock()
            .await
            .remove(container_id);
        match self
            .teardown_owned_stack_container_overlay(vm.as_ref(), container_id, generation)
            .await
        {
            Ok(()) => {
                self.clear_stack_overlay_recovery_route(container_id).await;
                self.clear_overlay_cleanup_pending(container_id);
                prepare_error
            }
            Err(cleanup_error) => {
                // No OCI create has occurred on this path, so shutdown may
                // safely retry overlay teardown without first deleting state.
                self.mark_overlay_cleanup_pending(container_id, generation);
                // Route and VM publication completed before the first overlay
                // mutation. Do not await or mutate either map on this failure
                // path: cancellation must preserve that recovery state.
                OciError::InvalidConfig(format!(
                    "stack container preparation failed: {prepare_error}; partial-overlay cleanup also failed and stack routing was retained for retry: {cleanup_error}"
                ))
            }
        }
    }
}

impl SharedVmLifecycleLease {
    /// Open the fixed private Docker socket in this exact leased boot. No
    /// target path, TCP address, or replacement VM is selected by the caller.
    /// Retain this lease until the returned stream is closed.
    pub async fn open_docker_stream(&self) -> Result<vz_linux::GrpcDockerStream, OciError> {
        let record = self
            .stack_vms
            .lock()
            .await
            .get(&self.runtime_identity.stack_id)
            .cloned();
        require_exact_stack_runtime(
            record.as_ref().map(|record| &record.identity),
            &self.runtime_identity,
        )?;
        let record = record.ok_or_else(|| OciError::SharedRuntimeAbsent {
            stack_id: self.runtime_identity.stack_id.clone(),
        })?;
        require_docker_provisioned_developer_profile(
            record.verified_linux_profile,
            record.docker_provisioned,
            "shared_vm_lease_open_docker_stream",
        )?;
        Ok(record.vm.open_docker_stream().await?)
    }

    /// Full identity of the exact shared-VM boot protected by this lease.
    pub fn runtime_identity(&self) -> &vz_runtime_contract::StackRuntimeIdentity {
        &self.runtime_identity
    }

    /// Linux profile proven from the selected boot artifact metadata.
    pub const fn verified_profile(&self) -> KernelProfile {
        self.verified_profile
    }

    /// Start and health-check the private Docker Engine without releasing this
    /// lease's generation fence.
    ///
    /// Success is an Engine and mount readiness proof only. It does not
    /// negotiate or publish Developer Machine capabilities.
    pub async fn ensure_docker_ready(&self) -> Result<SharedVmDockerReadiness, OciError> {
        let record = self
            .stack_vms
            .lock()
            .await
            .get(&self.runtime_identity.stack_id)
            .cloned();
        require_exact_stack_runtime(
            record.as_ref().map(|record| &record.identity),
            &self.runtime_identity,
        )?;
        let Some(record) = record else {
            return Err(OciError::SharedRuntimeAbsent {
                stack_id: self.runtime_identity.stack_id.clone(),
            });
        };
        Runtime::docker_readiness_for_record(&record, "shared_vm_lease_ensure_docker_ready").await
    }

    /// Execute directly in the exact shared VM protected by this lease.
    ///
    /// This deliberately does not reacquire the lifecycle reader. Tokio's
    /// writer-preferring lock could otherwise deadlock a lease holder behind a
    /// queued shutdown writer while the writer waits for this lease to drop.
    pub async fn exec(
        &self,
        command: String,
        args: Vec<String>,
        timeout: Duration,
    ) -> Result<ExecOutput, OciError> {
        let record = self
            .stack_vms
            .lock()
            .await
            .get(&self.runtime_identity.stack_id)
            .cloned();
        require_exact_stack_runtime(
            record.as_ref().map(|record| &record.identity),
            &self.runtime_identity,
        )?;
        let Some(record) = record else {
            return Err(OciError::SharedRuntimeAbsent {
                stack_id: self.runtime_identity.stack_id.clone(),
            });
        };
        let result = record.vm.exec_collect(command, args, timeout).await?;
        Ok(ExecOutput {
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
        })
    }
}

fn require_complete_shared_vm_boot(stack_id: &str, complete: bool) -> Result<(), OciError> {
    if !complete {
        return Err(OciError::InvalidConfig(format!(
            "shared VM '{stack_id}' has an incomplete owned bootstrap; exact guest exec reconciliation is required before reuse, filesystem closure, or power stop"
        )));
    }
    Ok(())
}

fn diagnostic_file_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

impl Runtime {
    // ── Shared stack VM API ──────────────────────────────────────────

    pub(super) async fn stack_activation_lock(&self, stack_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.stack_activation_locks.lock().await;
        locks
            .entry(stack_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(super) async fn acquire_stack_activation_guard(
        &self,
        stack_id: &str,
    ) -> StackActivationGuard {
        let lock = self.stack_activation_lock(stack_id).await;
        StackActivationGuard {
            _guard: lock.lock_owned().await,
        }
    }

    /// Return the rootfs store directory where assembled rootfs trees are stored.
    ///
    /// This is the parent directory of all per-container rootfs directories.
    /// For a shared stack VM, it is used as the VirtioFS `rootfs` share so
    /// that each container's assembled rootfs appears at `/<container_id>/`
    /// inside the guest.
    pub fn rootfs_store_dir(&self) -> PathBuf {
        self.config.data_dir.join("rootfs")
    }

    /// Host-side directory where setup-commit tarballs are stored.
    ///
    /// VirtioFS-shared into every shared VM at `/vz-setup-commits` so that
    /// the post-setup filesystem state of a container can be tarred to
    /// host once and replayed on subsequent boots, instead of re-running
    /// `apt-get install` etc. on every cold boot.
    pub fn setup_commits_host_dir(&self) -> PathBuf {
        self.config.data_dir.join("setup-commits")
    }

    /// Compute a stable identifier for a (image, setup_commands) tuple.
    ///
    /// Used as the filename of the cached setup tarball
    /// (`<reference>.tar` under [`setup_commits_host_dir`]). Hashes the
    /// image string verbatim — when the user pins a digest the cache is
    /// content-addressed; when they use a tag they accept that the cache
    /// can be stale across image updates and is cleared by manually
    /// removing the tarball.
    pub fn setup_commit_reference(image: &str, setup_commands: &[String]) -> String {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(image.as_bytes());
        hasher.update(b"\0");
        for cmd in setup_commands {
            hasher.update(cmd.as_bytes());
            hasher.update(b"\0");
        }
        format!("{:x}", hasher.finalize())
    }

    fn docker_data_disk_path(&self, stack_id: &str) -> PathBuf {
        let digest = Sha256::digest(stack_id.as_bytes());
        self.config
            .data_dir
            .join("docker-machines")
            .join(format!("{digest:x}"))
            .join("data.img")
    }

    async fn ensure_guest_ext4_disk(
        vm: &LinuxVm,
        device: &str,
        purpose: &str,
        allow_unformatted: bool,
        docker_data: bool,
    ) -> Result<(), OciError> {
        let phase = GuestDiskPhase::Probe;
        let started = std::time::Instant::now();
        tracing::info!(
            phase = phase.label(),
            purpose,
            device,
            budget_seconds = phase.timeout().as_secs(),
            "guest disk phase started"
        );
        let inspection = vm
            .exec_collect(
                "/bin/busybox".to_string(),
                vec!["blkid".to_string(), device.to_string()],
                phase.timeout(),
            )
            .await
            .map_err(|error| {
                guest_disk_phase_error(
                    phase,
                    purpose,
                    device,
                    started.elapsed(),
                    &error.to_string(),
                )
            })?;
        tracing::info!(
            phase = phase.label(),
            purpose,
            device,
            elapsed_seconds = started.elapsed().as_secs_f64(),
            exit_code = inspection.exit_code,
            "guest disk phase returned"
        );
        match classify_guest_disk_probe(
            inspection.exit_code,
            &inspection.stdout,
            &inspection.stderr,
            allow_unformatted,
        ) {
            Ok(GuestDiskProbe::ExtFilesystem) => {
                if docker_data {
                    if !inspection.stdout.contains("TYPE=\"ext4\"") {
                        return Err(OciError::InvalidConfig(
                            "Docker data requires journaled ext4; existing ext2/ext3 disks are preserved, never reformatted".to_string(),
                        ));
                    }
                    Self::verify_guest_docker_filesystem(vm, device).await?;
                }
                return Ok(());
            }
            Ok(GuestDiskProbe::Unformatted) => {}
            Err(reason) => {
                return Err(guest_disk_phase_error(
                    phase,
                    purpose,
                    device,
                    started.elapsed(),
                    &reason,
                ));
            }
        }

        let phase = GuestDiskPhase::Format;
        let started = std::time::Instant::now();
        tracing::info!(
            phase = phase.label(),
            purpose,
            device,
            budget_seconds = phase.timeout().as_secs(),
            "guest disk phase started"
        );
        let (formatter, arguments) = if docker_data {
            (
                "/sbin/mke2fs",
                vec![
                    "-t",
                    "ext4",
                    "-F",
                    "-O",
                    "has_journal,extent,64bit,metadata_csum",
                    "-E",
                    "lazy_itable_init=0,lazy_journal_init=0",
                    device,
                ],
            )
        } else {
            ("/bin/busybox", vec!["mke2fs", "-F", device])
        };
        let output = vm
            .exec_collect(
                formatter.to_string(),
                arguments.into_iter().map(str::to_string).collect(),
                phase.timeout(),
            )
            .await
            .map_err(|error| {
                guest_disk_phase_error(
                    phase,
                    purpose,
                    device,
                    started.elapsed(),
                    &error.to_string(),
                )
            })?;
        tracing::info!(
            phase = phase.label(),
            purpose,
            device,
            elapsed_seconds = started.elapsed().as_secs_f64(),
            exit_code = output.exit_code,
            "guest disk phase returned"
        );
        if output.exit_code != 0 {
            return Err(guest_disk_phase_error(
                phase,
                purpose,
                device,
                started.elapsed(),
                &format!(
                    "exit {}: {}{}",
                    output.exit_code, output.stdout, output.stderr
                ),
            ));
        }
        if docker_data {
            // Do not consume the host's exact format intent based on formatter
            // exit alone. The actual on-disk journal and clean state are proof.
            Self::verify_guest_docker_filesystem(vm, device).await?;
        }
        Ok(())
    }

    async fn verify_guest_docker_filesystem(vm: &LinuxVm, device: &str) -> Result<(), OciError> {
        let header = vm
            .exec_collect(
                "/sbin/dumpe2fs".to_string(),
                vec!["-h".to_string(), device.to_string()],
                GuestDiskPhase::Probe.timeout(),
            )
            .await?;
        if header.exit_code != 0 {
            return Err(OciError::InvalidConfig(format!(
                "Docker filesystem header probe failed for {device}: exit {}: {}{}",
                header.exit_code, header.stdout, header.stderr
            )));
        }
        validate_docker_filesystem_header(&header.stdout).map_err(|reason| {
            OciError::InvalidConfig(format!(
                "Docker filesystem admission refused for {device}: {reason}"
            ))
        })?;
        tracing::info!(device, filesystem_header = %header.stdout, "journaled Docker filesystem admitted");
        Ok(())
    }

    async fn bootstrap_guest_docker_disk(vm: &LinuxVm) -> Result<(), OciError> {
        let output = vm
            .exec_collect(
                "/bin/busybox".to_string(),
                vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    DOCKER_DISK_BOOTSTRAP_SCRIPT.to_string(),
                    "vz-docker-bootstrap".to_string(),
                    DOCKER_DATA_DEVICE.to_string(),
                ],
                Duration::from_secs(30),
            )
            .await?;
        if output.exit_code != 0 {
            return Err(OciError::InvalidConfig(format!(
                "failed to bootstrap private Docker data disk: {}{}",
                output.stdout, output.stderr
            )));
        }
        Ok(())
    }

    async fn verify_guest_docker_mounts(vm: &LinuxVm) -> Result<(), OciError> {
        const SCRIPT: &str = r#"
set -eu
require_mount() {
  expected_source="$1"
  expected_target="$2"
  expected_type="$3"
  identity=$(/bin/busybox awk -v path="$expected_target" '$2 == path { print $1 " " $3; count++ } END { if (count != 1) exit 1 }' /proc/mounts)
  test "$identity" = "$expected_source $expected_type"
}
require_mount vz-docker-bin /mnt/vz-docker-bin virtiofs
require_mount linux-bin /mnt/linux-bin virtiofs
require_mount /dev/vda /var/lib/docker ext4
"#;
        let output = vm
            .exec_collect(
                "/bin/busybox".to_string(),
                vec!["sh".to_string(), "-c".to_string(), SCRIPT.to_string()],
                Duration::from_secs(10),
            )
            .await?;
        if output.exit_code != 0 {
            return Err(OciError::InvalidConfig(format!(
                "Docker readiness mount proof failed: {}{}",
                output.stdout, output.stderr
            )));
        }
        Ok(())
    }

    async fn verify_guest_docker_prerequisites(
        vm: &LinuxVm,
        operation: &str,
    ) -> Result<(), OciError> {
        const SCRIPT: &str = r#"
set -eu
fail() { echo "$1" >&2; exit 1; }
test -x /sbin/xtables-legacy-multi || fail 'missing executable /sbin/xtables-legacy-multi'
for tool in iptables iptables-save iptables-restore ip6tables ip6tables-save ip6tables-restore; do
  test -L "/sbin/$tool" || fail "missing symlink /sbin/$tool"
  target=$(/bin/busybox readlink "/sbin/$tool") || fail "cannot read /sbin/$tool"
  test "$target" = /sbin/xtables-legacy-multi || fail "unexpected /sbin/$tool target: $target"
done
version=$(/sbin/iptables --version 2>&1) || fail 'iptables version probe failed'
case "$version" in
  'iptables v1.8.13 (legacy)') ;;
  *) fail "unexpected iptables version: $version" ;;
esac
"#;
        let output = vm
            .exec_collect(
                "/bin/busybox".to_string(),
                vec!["sh".to_string(), "-c".to_string(), SCRIPT.to_string()],
                Duration::from_secs(10),
            )
            .await?;
        if output.exit_code != 0 {
            return Err(OciError::UnsupportedOperation {
                operation: operation.to_string(),
                reason: format!(
                    "verified Developer Linux boot lacks the pinned iptables prerequisite: {}{}",
                    output.stdout, output.stderr
                ),
            });
        }
        Ok(())
    }

    async fn docker_readiness_for_record(
        record: &super::StackVmRecord,
        operation: &str,
    ) -> Result<SharedVmDockerReadiness, OciError> {
        require_complete_shared_vm_boot(&record.identity.stack_id, record.boot_complete)?;
        let verified_profile = require_docker_provisioned_developer_profile(
            record.verified_linux_profile,
            record.docker_provisioned,
            operation,
        )?;
        Self::verify_guest_docker_prerequisites(&record.vm, operation).await?;
        let guest_socket_path = record.vm.ensure_docker_ready().await?;
        if guest_socket_path != DOCKER_GUEST_SOCKET {
            return Err(OciError::InvalidConfig(format!(
                "Docker readiness returned unexpected guest socket path {guest_socket_path:?}"
            )));
        }
        Self::verify_guest_docker_mounts(&record.vm).await?;
        Ok(SharedVmDockerReadiness {
            runtime_identity: record.identity.clone(),
            verified_profile,
            guest_socket_path,
        })
    }

    /// Boot a shared VM for a multi-service stack.
    ///
    /// The VM runs a single kernel with the guest agent, and multiple OCI
    /// containers can be created inside it via
    /// [`create_container_in_stack`](Self::create_container_in_stack).
    ///
    /// The rootfs store directory is shared via VirtioFS so that each
    /// container's assembled rootfs appears at `/<container_id>/` inside
    /// the guest after overlay+chroot.
    ///
    /// # Errors
    ///
    /// Returns an error if a shared VM is already running for `stack_id`, or
    /// if the VM fails to boot.
    pub async fn boot_shared_vm(
        &self,
        stack_id: &str,
        ports: Vec<PortMapping>,
        resources: vz_runtime_contract::StackResourceHint,
    ) -> Result<(), OciError> {
        let _stack_lifecycle_guard = self
            .stack_lifecycle_lock(stack_id)
            .await
            .write_owned()
            .await;
        self.boot_shared_vm_locked(stack_id, ports, resources, None)
            .await
    }

    async fn boot_shared_vm_locked(
        &self,
        stack_id: &str,
        ports: Vec<PortMapping>,
        resources: vz_runtime_contract::StackResourceHint,
        required_profile: Option<KernelProfile>,
    ) -> Result<(), OciError> {
        let _activation_guard = self.acquire_stack_activation_guard(stack_id).await;

        // Snapshot lock-protected counters into locals so the lock guards
        // don't span the tracing::info! call (which was captured across
        // the next .await and broke Send).
        let (stack_vms_count, stack_port_forwards_count, has_leftover_pf, already_booted) = {
            let vms = self.stack_vms.lock().await;
            let pfs = self.stack_port_forwards.lock().await;
            (
                vms.len(),
                pfs.len(),
                pfs.contains_key(stack_id),
                vms.contains_key(stack_id),
            )
        };
        let sample_ports: Vec<(u16, u16)> = ports
            .iter()
            .take(4)
            .map(|p| (p.host, p.container))
            .collect();
        tracing::info!(
            target: "vz_post_stop",
            stack_id = %stack_id,
            in_count = ports.len(),
            ?sample_ports,
            stack_vms_count,
            stack_port_forwards_count,
            "[L4/stack-vm] boot_shared_vm entry"
        );
        // Guard against double-boot.
        if already_booted {
            tracing::info!(
                target: "vz_post_stop",
                stack_id = %stack_id,
                "[L4/stack-vm] returning 'shared VM already running' (BUG SUSPECT — partial-cleanup leftover)"
            );
            return Err(OciError::InvalidConfig(format!(
                "shared VM already running for stack '{stack_id}'"
            )));
        }
        // Inspect partial-cleanup state: stack_vms cleared but stack_port_forwards
        // not. This is suspect (c).
        if has_leftover_pf {
            tracing::warn!(
                target: "vz_post_stop",
                stack_id = %stack_id,
                "[L4/stack-vm] LEFTOVER PortForwarding entry for this stack from prior run (BUG SUSPECT (c))"
            );
        }

        let kernel = ensure_kernel_for_config(&self.config).await?;
        let verified_linux_profile = kernel_profile_from_metadata(&kernel.version);
        if let Some(required_profile) = required_profile {
            require_explicit_verified_profile(
                Some(required_profile),
                verified_linux_profile,
                "boot_or_inspect_shared_vm",
            )?;
        }

        let rootfs_store = self.rootfs_store_dir();
        fs::create_dir_all(&rootfs_store)?;

        // Setup-commit cache: VirtioFS-shared into the guest at
        // /vz-setup-commits. Lets create_container_in_stack tar a
        // post-setup upperdir to host once, then restore it on every
        // subsequent cold boot — turning a 32s `apt-get install ...`
        // into a sub-second `tar -xpf`.
        let setup_commits = self.setup_commits_host_dir();
        fs::create_dir_all(&setup_commits)?;

        let docker_provisioning = if self.config.linux_profile == Some(KernelProfile::Developer) {
            if verified_linux_profile != Some(KernelProfile::Developer) {
                return Err(OciError::UnsupportedOperation {
                    operation: "boot_shared_vm".to_string(),
                    reason:
                        "Docker provisioning requires a verified Developer Linux kernel profile"
                            .to_string(),
                });
            }
            let artifacts = tokio::task::spawn_blocking(vz_oci::ensure_docker_binaries)
                .await
                .map_err(|error| {
                    OciError::InvalidConfig(format!(
                        "Docker artifact validation worker failed: {error}"
                    ))
                })??;
            let data_disk_path = self.docker_data_disk_path(stack_id);
            let disposition =
                ensure_private_sparse_disk(&data_disk_path, DOCKER_DATA_DISK_SIZE_BYTES)?;
            Some((artifacts, data_disk_path, disposition))
        } else {
            None
        };

        let runtime_binary = resolve_oci_runtime_binary_path(
            self.config.guest_oci_runtime,
            self.config.guest_oci_runtime_path.as_deref(),
            &kernel,
        )?;

        let mut vm_config =
            LinuxVmConfig::new(kernel.kernel, kernel.initramfs).with_rootfs_dir(rootfs_store);
        vm_config
            .shared_dirs
            .push(make_oci_runtime_share(&runtime_binary)?);
        vm_config.shared_dirs.push(SharedDirConfig {
            tag: "vz-setup-commits".to_string(),
            source: setup_commits,
            read_only: false,
        });
        if let Some((artifacts, data_disk_path, _)) = &docker_provisioning {
            let youki_dir = runtime_binary.parent().ok_or_else(|| {
                OciError::InvalidConfig(format!(
                    "verified youki path has no parent directory: {}",
                    runtime_binary.display()
                ))
            })?;
            vm_config.shared_dirs.extend([
                SharedDirConfig {
                    tag: DOCKER_BIN_SHARE_TAG.to_string(),
                    source: artifacts.bin_dir.clone(),
                    read_only: true,
                },
                SharedDirConfig {
                    tag: DOCKER_YOUKI_SHARE_TAG.to_string(),
                    source: youki_dir.to_path_buf(),
                    read_only: true,
                },
            ]);
            vm_config.disks.push(DiskConfig {
                id: "docker-data".to_string(),
                path: data_disk_path.clone(),
                read_only: false,
            });
        }

        // Add VirtioFS shares for per-service volume mounts. These must be
        // configured at VM creation time because VirtioFS shares are static.
        for vol in &resources.volume_mounts {
            vm_config.shared_dirs.push(SharedDirConfig {
                tag: vol.tag.clone(),
                source: vol.host_path.clone(),
                read_only: vol.read_only,
            });
            // When a guest_path is specified, append the kernel cmdline
            // parameter that tells the init script where to bind-mount this
            // VirtioFS share inside the chroot.
            if let Some(guest_path) = &vol.guest_path {
                if let Some(idx_str) = vol.tag.strip_prefix("vz-mount-") {
                    vm_config
                        .cmdline
                        .push_str(&format!(" vz.mount.{idx_str}={guest_path}"));
                }
            }
        }

        vm_config.cpus = resources.cpus.unwrap_or(self.config.default_cpus);
        vm_config.memory_mb = resources.memory_mb.unwrap_or(self.config.default_memory_mb);

        // Attach a persistent named-volume disk after the private Docker disk.
        if let Some(ref disk_path) = resources.disk_image_path {
            vm_config.disk_image = Some(disk_path.clone());
        }

        // Capture one serial log per shared VM when the E2E harness provides
        // an artifact directory. Preserve the older exact-path override for
        // focused/manual debugging.
        if let Ok(log_dir) = std::env::var("VZ_STACK_SERIAL_LOG_DIR") {
            let log_dir = PathBuf::from(log_dir);
            fs::create_dir_all(&log_dir)?;
            vm_config.serial_log_file =
                Some(log_dir.join(format!("{}.log", diagnostic_file_component(stack_id))));
        } else if let Ok(log_path) = std::env::var("VZ_STACK_SERIAL_LOG") {
            vm_config.serial_log_file = Some(std::path::PathBuf::from(log_path));
        }

        if !self.config.default_network_enabled {
            vm_config.network = Some(NetworkConfig::None);
        }

        let runtime_identity = vz_runtime_contract::StackRuntimeIdentity::new(stack_id)
            .map_err(OciError::InvalidConfig)?;
        let vm = Arc::new(LinuxVm::create(vm_config).await?);
        // Register ownership before the first start or guest-side effect. A
        // failed/cancelled exec may still be formatting or mounting the disk;
        // neither an RPC error nor a bootstrap shell trap proves it quiescent.
        self.stack_vms.lock().await.insert(
            stack_id.to_string(),
            super::StackVmRecord {
                identity: runtime_identity,
                verified_linux_profile,
                docker_provisioned: docker_provisioning.is_some(),
                boot_complete: false,
                docker_shutdown: Arc::new(Mutex::new(None)),
                boot_ports: ports.clone(),
                boot_resources: resources.clone(),
                vm: Arc::clone(&vm),
            },
        );
        vm.start().await?;

        vm.wait_for_agent(self.config.agent_ready_timeout).await?;

        if let Some((_, data_disk_path, disposition)) = &docker_provisioning {
            Self::ensure_guest_ext4_disk(
                &vm,
                DOCKER_DATA_DEVICE,
                "private Docker data",
                *disposition == PrivateDiskDisposition::FormatAuthorized,
                true,
            )
            .await?;
            complete_private_disk_format(
                data_disk_path,
                DOCKER_DATA_DISK_SIZE_BYTES,
                *disposition,
            )?;
            Self::bootstrap_guest_docker_disk(&vm).await?;
        }

        // Format and mount the persistent named-volume disk if attached.
        if resources.disk_image_path.is_some() {
            let timeout = Duration::from_secs(30);
            let volume_device = if docker_provisioning.is_some() {
                NAMED_VOLUME_DEVICE_WITH_DOCKER
            } else {
                NAMED_VOLUME_DEVICE_WITHOUT_DOCKER
            };

            Self::ensure_guest_ext4_disk(
                &vm,
                volume_device,
                "persistent named-volume",
                true,
                false,
            )
            .await?;

            // Mount the formatted disk.
            let mount_result = vm
                .exec_collect(
                    "/bin/busybox".to_string(),
                    vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        format!(
                            "/bin/busybox mkdir -p /run/vz-oci/volumes && /bin/busybox mount -t ext4 {volume_device} /run/vz-oci/volumes"
                        ),
                    ],
                    timeout,
                )
                .await;
            match &mount_result {
                Ok(output) if output.exit_code != 0 => {
                    return Err(OciError::InvalidConfig(format!(
                        "failed to mount persistent volume disk: {}{}",
                        output.stdout, output.stderr
                    )));
                }
                Err(err) => {
                    return Err(OciError::InvalidConfig(format!(
                        "failed to mount persistent volume disk: {err}"
                    )));
                }
                _ => {
                    tracing::info!("persistent volume disk mounted at /run/vz-oci/volumes");
                }
            }
        }

        // Mount the setup-commits VirtioFS share inside the host VM so
        // create_container_in_stack can tar/untar setup state. Idempotent —
        // mountpoint may already exist from a prior boot of the same VM.
        let mount_cmd = "mkdir -p /vz-setup-commits && \
             ( mountpoint -q /vz-setup-commits || \
               mount -t virtiofs vz-setup-commits /vz-setup-commits )"
            .to_string();
        match vm
            .exec_collect(
                "sh".to_string(),
                vec!["-c".to_string(), mount_cmd],
                Duration::from_secs(5),
            )
            .await
        {
            Ok(out) if out.exit_code == 0 => {
                tracing::info!("setup-commits VirtioFS share mounted at /vz-setup-commits");
            }
            Ok(out) => {
                tracing::warn!(
                    exit_code = out.exit_code,
                    stderr = %out.stderr.trim(),
                    "setup-commits mount returned non-zero (cache will be unavailable)"
                );
            }
            Err(error) => {
                tracing::warn!(error = %error, "setup-commits mount exec failed (cache will be unavailable)");
            }
        }

        // Set up port forwarding for all services' ports.
        let port_forwarding = start_port_forwarding(vm.inner_shared(), &ports).await?;

        if let Some(pf) = port_forwarding {
            self.stack_port_forwards
                .lock()
                .await
                .insert(stack_id.to_string(), pf);
        }

        let mut records = self.stack_vms.lock().await;
        let record = records.get_mut(stack_id).ok_or_else(|| {
            OciError::InvalidConfig(format!(
                "shared VM ownership disappeared during bootstrap for '{stack_id}'"
            ))
        })?;
        record.boot_complete = true;

        Ok(())
    }

    /// Create and start an OCI container inside a shared stack VM.
    ///
    /// The VM must have been booted via [`boot_shared_vm`](Self::boot_shared_vm).
    /// This method pulls the image, assembles its rootfs, writes an OCI bundle,
    /// and runs the OCI create/start lifecycle inside the shared VM.
    ///
    /// Returns the container identifier.
    pub async fn create_container_in_stack(
        &self,
        stack_id: &str,
        image: &str,
        mut run: RunConfig,
        setup_commit_tar_guest: Option<String>,
    ) -> Result<String, OciError> {
        let scope = vz_runtime_contract::ContainerGenerationScope::synthetic_legacy_stack(stack_id)
            .map_err(OciError::InvalidConfig)?;
        let mut transaction = self.begin_scoped_container_create(&mut run, &scope).await?;
        self.create_container_in_stack_transaction(
            stack_id,
            image,
            run,
            setup_commit_tar_guest,
            &mut transaction,
        )
        .await
    }

    pub(crate) async fn create_container_in_stack_transaction(
        &self,
        stack_id: &str,
        image: &str,
        run: RunConfig,
        setup_commit_tar_guest: Option<String>,
        transaction: &mut ContainerLifecycleTransaction,
    ) -> Result<String, OciError> {
        let effective_stack_id = match transaction.scope() {
            Some(scope) if scope.stack_id != stack_id => {
                return Err(OciError::ContainerOwnershipMismatch {
                    id: transaction.container_id().to_string(),
                    reason: format!(
                        "create requested stack '{stack_id}', but the reserved scope belongs to '{}'",
                        scope.stack_id
                    ),
                });
            }
            Some(scope) => scope.stack_id.clone(),
            None => stack_id.to_string(),
        };
        let stack_id = effective_stack_id.as_str();
        self.ensure_stack_not_tearing_down(stack_id, "create a container in")
            .await?;
        let vm = self
            .stack_vms
            .lock()
            .await
            .get(stack_id)
            .map(|record| record.vm.clone())
            .ok_or_else(|| {
                OciError::InvalidConfig(format!(
                    "no shared VM running for stack '{stack_id}'; call boot_shared_vm first"
                ))
            })?;

        let container_id = transaction.container_id().to_string();
        validate_container_id(&container_id)?;
        let image_id = self.pull(image).await?;

        let created_unix_secs = current_unix_secs();
        let mut container = ContainerInfo {
            id: container_id.clone(),
            image: image.to_string(),
            image_id: image_id.0.clone(),
            status: ContainerStatus::Created,
            created_unix_secs,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(process::id()),
        };

        self.persist_owned(transaction, container.clone())?;

        // Step 2 runs concurrently with rootfs assembly (no disk I/O dependency).
        tracing::debug!("step 2: parse_image_config_summary_from_store (concurrent with step 1)");
        let image_config = parse_image_config_summary_from_store(&self.store, &image_id.0)
            .map_err(|e| {
                tracing::error!(error = %e, "step 2 FAILED");
                e
            })?;
        tracing::debug!("step 2 OK");
        let run = resolve_run_config(image_config, run, &container_id)?;
        tracing::debug!(
            container_id = %container_id,
            working_dir = ?run.working_dir,
            "resolved container run configuration"
        );
        let lifecycle = resolve_container_lifecycle(
            &run.oci_annotations,
            ContainerLifecycleClass::Service,
            false,
        )?;

        // Build OCI bundle referencing the assembled rootfs (shared via VirtioFS).
        //
        // In a shared VM, the rootfs store directory is the VirtioFS share.
        // Each container's assembled rootfs appears at `/<container_id>/` inside
        // the guest after overlay+chroot. The bundle is written under the
        // container's rootfs dir so its guest path is `/<container_id>/<bundle>`.
        let oci_container_id = run
            .container_id
            .clone()
            .unwrap_or_else(|| container_id.to_string());
        let bundle_guest_root = oci_bundle_guest_root(self.config.guest_state_dir.as_deref())?;
        let bundle_relative_path = oci_bundle_guest_path(&bundle_guest_root, &oci_container_id);

        let rootfs_dir = match self
            .assemble_rootfs_in_transaction(&image_id.0, transaction)
            .await
        {
            Ok(rootfs_dir) => rootfs_dir,
            Err(err) => {
                tracing::error!(error = %err, "step 1 FAILED: assemble_rootfs");
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.persist_owned(transaction, container)?;
                return Err(err);
            }
        };
        container.rootfs_path = Some(rootfs_dir.clone());
        self.persist_owned(transaction, container.clone())?;

        // Host: <data_dir>/rootfs/<container_id>/<bundle_path>
        let bundle_host_dir = oci_bundle_host_dir(&rootfs_dir, &bundle_relative_path);
        // Guest: /vz-rootfs/<container_id>/<bundle_path>
        let bundle_guest_path = format!("/vz-rootfs/{container_id}{bundle_relative_path}");
        let bundle_cmd = run
            .init_process
            .clone()
            .or_else(|| {
                if run.cmd.is_empty() {
                    None
                } else {
                    Some(run.cmd.clone())
                }
            })
            .ok_or_else(|| {
                OciError::InvalidConfig(
                    "container requires a command (init_process or cmd)".to_string(),
                )
            })?;

        let mut bundle_mounts = mount_specs_to_bundle_mounts(&run.mounts, run.mount_tag_offset)?;

        // Setup commit/restore: caller (the macOS backend) precomputes the
        // (image, setup_commands) hash and resolves it to a guest path
        // under /vz-setup-commits — those fields are stripped during the
        // contract → oci_config conversion so they can't be derived here.

        // Serialize the complete guest-critical activation transaction for
        // this stack. In particular, overlay cleanup performs a VM-global
        // drop_caches operation, so it must not overlap a sibling service's
        // overlay mount or OCI create/start. Image pull, rootfs assembly, and
        // image-config resolution above remain parallel; independent stacks
        // use independent locks.
        let activation_guard = self.acquire_stack_activation_guard(stack_id).await;
        self.publish_stack_overlay_recovery_route(stack_id, &container_id, &vm)
            .await;
        // Until OCI create begins, cancellation is known to leave only an
        // overlay to clean. Shutdown may therefore skip OCI delete and retry
        // this exact generation's teardown directly.
        self.mark_overlay_cleanup_pending(&container_id, transaction.generation());
        self.observe_lifecycle_admission(
            RuntimeLifecycleAdmissionKind::StackOverlaySetupStarting,
            &container_id,
        )
        .await;

        // Per-container overlay: VirtioFS doesn't support mknod, so we create a
        // guest-side overlay with tmpfs as upperdir for device nodes.
        let vz_rootfs_path = format!("/vz-rootfs/{container_id}");
        let (guest_rootfs_path, setup_was_restored) = match setup_stack_guest_container_overlay(
            vm.as_ref(),
            &vz_rootfs_path,
            &container_id,
            setup_commit_tar_guest.as_deref(),
            &activation_guard,
        )
        .await
        {
            Ok(out) => out,
            Err(err) => {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                let mut error = err;
                if let Err(persist_error) = self.persist_owned(transaction, container) {
                    error = OciError::InvalidConfig(format!(
                        "{error}; could not persist overlay setup failure: {persist_error}"
                    ));
                }
                return Err(self
                    .stack_prepare_error_with_overlay_cleanup(
                        &vm,
                        stack_id,
                        &container_id,
                        transaction.generation(),
                        error,
                    )
                    .await);
            }
        };
        if setup_was_restored {
            let commit_ref = match setup_commit_tar_guest
                .as_deref()
                .and_then(|path| path.rsplit('/').next())
                .and_then(|name| name.strip_suffix(".tar"))
            {
                Some(commit_ref) => commit_ref,
                None => {
                    let error = OciError::InvalidConfig(
                        "setup overlay reported a restore without a valid commit reference"
                            .to_string(),
                    );
                    return Err(self
                        .stack_prepare_error_with_overlay_cleanup(
                            &vm,
                            stack_id,
                            &container_id,
                            transaction.generation(),
                            error,
                        )
                        .await);
                }
            };
            self.setup_restored_containers.lock().await.insert(
                container_id.clone(),
                SetupRestoreIdentity {
                    generation: transaction.generation(),
                    commit_ref: commit_ref.to_string(),
                },
            );
            tracing::info!(
                container_id = %container_id,
                "setup commit restored into overlay upperdir before mount"
            );
        }
        // When sharing the VM's host network, ensure the container has a
        // working /etc/resolv.conf. Container images (e.g., Ubuntu) often
        // ship a resolv.conf pointing to systemd-resolved (127.0.0.53)
        // which isn't running in the VM. Write public DNS nameservers into
        // the overlay's upper layer so DNS resolution works immediately.
        if run.share_host_network {
            let dns_cmd = format!(
                "printf 'nameserver 8.8.8.8\\nnameserver 8.8.4.4\\n' > {guest_rootfs_path}/etc/resolv.conf"
            );
            let _ = vm
                .exec_collect(
                    "sh".to_string(),
                    vec!["-c".to_string(), dns_cmd],
                    Duration::from_secs(5),
                )
                .await;
        }

        // Bind-mount the VM-level log directory into the container so captured
        // stdout/stderr survives even if the container's init process exits.
        if run.capture_logs {
            bundle_mounts.push(BundleMount {
                destination: PathBuf::from("/var/log/vz-oci"),
                source: PathBuf::from(container_log_dir(&container_id)),
                typ: "bind".to_string(),
                options: vec!["rbind".to_string(), "rw".to_string()],
            });
        }

        // Create directories on the persistent volume disk for named volumes.
        // These must exist before the OCI runtime bind-mounts them into the container.
        let volume_dirs: Vec<String> = run
            .mounts
            .iter()
            .filter_map(|m| {
                if let MountType::Volume { ref volume_name } = m.mount_type {
                    Some(format!("/run/vz-oci/volumes/{volume_name}"))
                } else {
                    None
                }
            })
            .collect();
        if !volume_dirs.is_empty() {
            let mkdir_cmd = format!("/bin/busybox mkdir -p {}", volume_dirs.join(" "));
            let mkdir_result = vm
                .exec_collect(
                    "/bin/busybox".to_string(),
                    vec!["sh".to_string(), "-c".to_string(), mkdir_cmd],
                    Duration::from_secs(10),
                )
                .await;
            if let Err(err) = &mkdir_result {
                tracing::warn!(error = %err, "failed to create volume directories on persistent disk");
            }
        }

        // extra_hosts are written AFTER the container starts (step 5) via
        // typed container exec. Writing before
        // start (via guest exec or bind mount) fails due to VirtioFS caching
        // and youki's pivot_root creating an isolated mount tree.

        if let Err(error) = write_oci_bundle(
            &bundle_host_dir,
            Path::new(&guest_rootfs_path),
            BundleSpec {
                cmd: bundle_cmd,
                env: run.env.clone(),
                cwd: run.working_dir.clone(),
                user: run.user.clone(),
                mounts: bundle_mounts,
                oci_annotations: run.oci_annotations.clone(),
                network_namespace_path: run.network_namespace_path.clone(),
                share_host_network: run.share_host_network,
                cpu_quota: run.cpu_quota,
                cpu_period: run.cpu_period,
                capture_logs: run.capture_logs,
                cap_add: run.cap_add.clone(),
                cap_drop: run.cap_drop.clone(),
                privileged: run.privileged,
                read_only_rootfs: run.read_only_rootfs,
                sysctls: run.sysctls.clone(),
                ulimits: run.ulimits.clone(),
                pids_limit: run.pids_limit,
                hostname: run.hostname.clone(),
                domainname: run.domainname.clone(),
            },
        ) {
            let error = self
                .stack_prepare_error_with_overlay_cleanup(
                    &vm,
                    stack_id,
                    &container_id,
                    transaction.generation(),
                    OciError::from(error),
                )
                .await;
            self.cleanup_owned_rootfs(transaction, rootfs_dir.as_ref());
            return Err(error);
        }

        let vm_is_current = self
            .stack_vms
            .lock()
            .await
            .get(stack_id)
            .is_some_and(|current| Arc::ptr_eq(&current.vm, &vm));
        if !vm_is_current {
            container.status = ContainerStatus::Stopped { exit_code: -1 };
            container.stopped_unix_secs = Some(current_unix_secs());
            container.host_pid = None;
            let mut error = OciError::InvalidConfig(format!(
                "shared VM for stack '{stack_id}' changed while container '{container_id}' was being prepared"
            ));
            if let Err(persist_error) = self.persist_owned(transaction, container) {
                error = OciError::InvalidConfig(format!(
                    "{error}; could not persist preparation failure: {persist_error}"
                ));
            }
            let error = self
                .stack_prepare_error_with_overlay_cleanup(
                    &vm,
                    stack_id,
                    &container_id,
                    transaction.generation(),
                    error,
                )
                .await;
            self.cleanup_owned_rootfs(transaction, rootfs_dir.as_ref());
            return Err(error);
        }

        // OCI create + start inside the shared VM.
        // From this point OCI state may exist, so cleanup must prove deletion
        // before unmounting the overlay.
        self.clear_overlay_cleanup_pending(&container_id);
        if let Err(err) = vm
            .oci_create(oci_container_id.clone(), bundle_guest_path.clone())
            .await
        {
            tracing::error!(
                container_id = %oci_container_id,
                error = %err,
                "step 4 FAILED: oci_create"
            );
            let error = OciError::from(err);
            let rollback = self
                .rollback_stack_container_activation(
                    &vm,
                    stack_id,
                    &oci_container_id,
                    &mut container,
                    rootfs_dir.as_ref(),
                    transaction,
                )
                .await;
            return Err(activation_error_with_rollback(error, rollback));
        }

        if let Err(err) = vm.oci_start(oci_container_id.clone()).await {
            let error = OciError::from(err);
            let rollback = self
                .rollback_stack_container_activation(
                    &vm,
                    stack_id,
                    &oci_container_id,
                    &mut container,
                    rootfs_dir.as_ref(),
                    transaction,
                )
                .await;
            return Err(activation_error_with_rollback(error, rollback));
        }

        // Step 5: Write /etc/hosts inside the running container via oci_exec.
        // This writes directly into the container's mount namespace after
        // pivot_root, avoiding VirtioFS caching and overlay visibility issues.
        if let Err(error) = self
            .validate_stack_container_running(&vm, &oci_container_id, "post-start")
            .await
        {
            let rollback = self
                .rollback_stack_container_activation(
                    &vm,
                    stack_id,
                    &oci_container_id,
                    &mut container,
                    rootfs_dir.as_ref(),
                    transaction,
                )
                .await;
            return Err(activation_error_with_rollback(error, rollback));
        }

        if !run.extra_hosts.is_empty() {
            tracing::debug!(
                container_id = %oci_container_id,
                "step 5: write /etc/hosts via typed container exec"
            );
            let mut printf_content = String::from("127.0.0.1\tlocalhost\n::1\tlocalhost\n");
            for (hostname, ip) in &run.extra_hosts {
                printf_content.push_str(&format!("{ip}\t{hostname}\n"));
            }
            let (hosts_command, hosts_args) = hosts_write_command(printf_content);
            let hosts_result = vm
                .exec_container_collect_with_options(
                    oci_container_id.clone(),
                    hosts_command,
                    hosts_args,
                    Duration::from_secs(30),
                    ExecOptions {
                        working_dir: Some("/".to_string()),
                        ..ExecOptions::default()
                    },
                )
                .await
                .map_err(OciError::from)
                .and_then(|output| require_successful_hosts_write(&oci_container_id, &output));
            if let Err(error) = hosts_result {
                tracing::error!(
                    container_id = %oci_container_id,
                    error = %error,
                    "step 5 FAILED: /etc/hosts write"
                );
                let rollback = self
                    .rollback_stack_container_activation(
                        &vm,
                        stack_id,
                        &oci_container_id,
                        &mut container,
                        rootfs_dir.as_ref(),
                        transaction,
                    )
                    .await;
                return Err(activation_error_with_rollback(error, rollback));
            }
            tracing::debug!(
                container_id = %oci_container_id,
                "step 5 OK: /etc/hosts written"
            );
        }

        if let Err(error) = self
            .validate_stack_container_running(&vm, &oci_container_id, "activation-finalize")
            .await
        {
            let rollback = self
                .rollback_stack_container_activation(
                    &vm,
                    stack_id,
                    &oci_container_id,
                    &mut container,
                    rootfs_dir.as_ref(),
                    transaction,
                )
                .await;
            return Err(activation_error_with_rollback(error, rollback));
        }

        if let Err(error) = self
            .start_log_rotation_task_if_needed(container_id.as_str(), Arc::clone(&vm), &run)
            .await
        {
            let rollback = self
                .rollback_stack_container_activation(
                    &vm,
                    stack_id,
                    &oci_container_id,
                    &mut container,
                    rootfs_dir.as_ref(),
                    transaction,
                )
                .await;
            return Err(activation_error_with_rollback(error, rollback));
        }

        container.status = ContainerStatus::Running;
        container.started_unix_secs = Some(current_unix_secs());
        container.host_pid = Some(process::id());
        if let Err(error) = self.persist_owned(transaction, container.clone()) {
            let rollback = self
                .rollback_stack_container_activation(
                    &vm,
                    stack_id,
                    &oci_container_id,
                    &mut container,
                    rootfs_dir.as_ref(),
                    transaction,
                )
                .await;
            return Err(activation_error_with_rollback(error, rollback));
        }

        // Reaffirm recovery routes after every required post-start action,
        // final liveness validation, and durable Running metadata have
        // succeeded. Only then publish active lifecycle state and the atomic
        // public exec binding for this exact VM generation/default snapshot.
        self.vm_handles
            .lock()
            .await
            .insert(container_id.to_string(), Arc::clone(&vm));
        self.container_stack
            .lock()
            .await
            .insert(container_id.to_string(), stack_id.to_string());
        self.track_active_lifecycle(container_id.clone(), lifecycle)
            .await;
        self.container_exec_bindings.lock().await.insert(
            container_id.clone(),
            ContainerExecBinding {
                vm: Arc::clone(&vm),
                defaults: ContainerExecDefaults::from(&run),
                generation: transaction.generation(),
            },
        );
        Ok(container_id)
    }

    async fn validate_stack_container_running(
        &self,
        vm: &LinuxVm,
        container_id: &str,
        phase: &str,
    ) -> Result<u32, OciError> {
        let state = vm.oci_state(container_id.to_string()).await?;
        let pid = require_running_pid(container_id, phase, &state)?;
        let proc_root = format!("/proc/{pid}/root");
        let liveness = vm
            .exec_collect(
                "/bin/busybox".to_string(),
                vec!["test".to_string(), "-d".to_string(), proc_root.clone()],
                Duration::from_secs(5),
            )
            .await?;
        if liveness.exit_code != 0 {
            return Err(OciError::InvalidConfig(format!(
                "container '{container_id}' reported status='{}' pid={pid} during {phase}, but {proc_root} is not live: {}",
                state.status,
                liveness.stderr.trim()
            )));
        }
        tracing::debug!(
            container_id,
            phase,
            status = %state.status,
            pid,
            "validated running OCI container"
        );
        Ok(pid)
    }

    async fn rollback_stack_container_activation(
        &self,
        vm: &Arc<LinuxVm>,
        stack_id: &str,
        oci_container_id: &str,
        container: &mut ContainerInfo,
        rootfs_dir: &Path,
        transaction: &ContainerLifecycleTransaction,
    ) -> Result<(), OciError> {
        let container_id = transaction.container_id();
        // Publish recovery routing before any await. This keeps the container
        // discoverable even if rollback is cancelled while stopping log
        // rotation, collecting diagnostics, or deleting guest OCI state.
        self.vm_handles
            .lock()
            .await
            .insert(container_id.to_string(), Arc::clone(vm));
        self.container_stack
            .lock()
            .await
            .insert(container_id.to_string(), stack_id.to_string());

        self.stop_log_rotation_task(container_id).await;
        self.log_stack_activation_diagnostics(vm, oci_container_id)
            .await;

        if let Err(error) = vm.oci_delete(oci_container_id.to_string(), true).await {
            // The guest may still have a live process or OCI state. Keep every
            // resource needed for a later stack shutdown retry instead of
            // publishing Stopped and orphaning the guest workload.
            container.status = ContainerStatus::Created;
            container.started_unix_secs = None;
            container.stopped_unix_secs = None;
            container.host_pid = Some(process::id());
            let persist_error = self.persist_owned(transaction, container.clone()).err();

            tracing::error!(
                container_id = %oci_container_id,
                stack_id,
                error = %error,
                "activation rollback could not delete OCI state; retained VM tracking and rootfs"
            );
            let mut message = format!(
                "activation rollback could not delete OCI state for container '{oci_container_id}'; retained stack '{stack_id}' tracking and rootfs for shutdown retry: {error}"
            );
            if let Some(persist_error) = persist_error {
                message.push_str(&format!(
                    "; could not persist activation-incomplete state: {persist_error}"
                ));
            }
            return Err(OciError::InvalidConfig(message));
        }
        self.mark_overlay_cleanup_pending(container_id, transaction.generation());
        if let Err(error) = self
            .teardown_owned_stack_container_overlay(
                vm.as_ref(),
                container_id,
                transaction.generation(),
            )
            .await
        {
            container.status = ContainerStatus::Created;
            container.started_unix_secs = None;
            container.stopped_unix_secs = None;
            container.host_pid = Some(process::id());
            let persist_error = self.persist_owned(transaction, container.clone()).err();
            let mut message = format!(
                "activation rollback deleted OCI state but could not tear down the guest overlay for container '{oci_container_id}'; retained stack '{stack_id}' tracking and rootfs for cleanup retry: {error}"
            );
            if let Some(persist_error) = persist_error {
                message.push_str(&format!(
                    "; could not persist activation-incomplete state: {persist_error}"
                ));
            }
            return Err(OciError::InvalidConfig(message));
        }
        self.mark_stack_guest_cleanup_complete(container_id, transaction.generation());

        container.status = ContainerStatus::Stopped { exit_code: -1 };
        container.stopped_unix_secs = Some(current_unix_secs());
        container.host_pid = None;
        let persist_result = self.persist_owned(transaction, container.clone());
        if persist_result.is_ok() {
            self.commit_container_cleanup_ownership(container_id).await;
        }
        self.cleanup_owned_rootfs(transaction, rootfs_dir);
        persist_result
    }

    async fn log_stack_activation_diagnostics(&self, vm: &LinuxVm, container_id: &str) {
        let commands = [
            (
                "process-table",
                "/bin/busybox",
                vec!["ps".to_string(), "-ef".to_string()],
            ),
            (
                "youki-create-log",
                "/bin/busybox",
                vec![
                    "cat".to_string(),
                    format!("/run/vz-oci/logs/{container_id}-create.log"),
                ],
            ),
            (
                "youki-start-log",
                "/bin/busybox",
                vec![
                    "cat".to_string(),
                    format!("/run/vz-oci/logs/{container_id}-start.log"),
                ],
            ),
            (
                "container-output",
                "/bin/busybox",
                vec![
                    "cat".to_string(),
                    format!("/run/vz-oci/logs/{container_id}/output.log"),
                ],
            ),
            ("kernel-log", "/bin/busybox", vec!["dmesg".to_string()]),
        ];

        for (diagnostic, command, args) in commands {
            match vm
                .exec_collect(command.to_string(), args, Duration::from_secs(5))
                .await
            {
                Ok(output) => tracing::error!(
                    container_id,
                    diagnostic,
                    exit_code = output.exit_code,
                    stdout = %output.stdout.trim(),
                    stderr = %output.stderr.trim(),
                    "stack activation diagnostic"
                ),
                Err(error) => tracing::error!(
                    container_id,
                    diagnostic,
                    error = %error,
                    "stack activation diagnostic unavailable"
                ),
            }
        }
    }

    /// Tar the container's overlay upperdir to host as the cached commit
    /// for `commit_ref`. Atomic via `<ref>.tar.tmp` + rename. Best-effort:
    /// failures here only mean the next cold boot will run setup again.
    pub async fn save_setup_commit(
        &self,
        stack_id: &str,
        container_id: &str,
        commit_ref: &str,
    ) -> Result<(), OciError> {
        let transaction = self.begin_existing_container(container_id).await?;
        self.save_setup_commit_in_transaction(stack_id, container_id, commit_ref, &transaction)
            .await
    }

    pub(crate) async fn save_setup_commit_in_transaction(
        &self,
        stack_id: &str,
        container_id: &str,
        commit_ref: &str,
        transaction: &ContainerLifecycleTransaction,
    ) -> Result<(), OciError> {
        debug_assert_eq!(container_id, transaction.container_id());
        self.ensure_stack_not_tearing_down(stack_id, "save setup state in")
            .await?;
        let vm = self
            .stack_vms
            .lock()
            .await
            .get(stack_id)
            .map(|record| record.vm.clone())
            .ok_or_else(|| {
                OciError::InvalidConfig(format!(
                    "no shared VM running for stack '{stack_id}'; call boot_shared_vm first"
                ))
            })?;
        let tar_guest_tmp = format!(
            "/vz-setup-commits/{commit_ref}.{}.{}.tar.tmp",
            transaction.container_id(),
            transaction.generation().0
        );
        let tar_guest = format!("/vz-setup-commits/{commit_ref}.tar");
        let upper_dir = format!("/run/vz-oci/containers/{container_id}/upper");
        // -C cd into upper, -p preserve perms, -f write to tmp file. Use
        // busybox tar for portability inside the minimal guest rootfs.
        let save_cmd = format!(
            "/bin/busybox tar -C {upper_dir} -cpf {tar_guest_tmp} . && \
             mv {tar_guest_tmp} {tar_guest}"
        );
        let started = std::time::Instant::now();
        let result = vm
            .exec_collect(
                "sh".to_string(),
                vec!["-c".to_string(), save_cmd],
                Duration::from_secs(120),
            )
            .await;
        match result {
            Ok(out) if out.exit_code == 0 => {
                let bytes = fs::metadata(
                    self.setup_commits_host_dir()
                        .join(format!("{commit_ref}.tar")),
                )
                .map(|m| m.len())
                .unwrap_or(0);
                tracing::info!(
                    commit_ref,
                    bytes,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "saved setup commit to cache"
                );
                Ok(())
            }
            Ok(out) => {
                tracing::warn!(
                    commit_ref,
                    exit_code = out.exit_code,
                    stderr = %out.stderr.trim(),
                    "setup commit save returned non-zero (next boot will re-run setup)"
                );
                Ok(())
            }
            Err(error) => {
                tracing::warn!(
                    commit_ref,
                    %error,
                    "setup commit save exec failed (next boot will re-run setup)"
                );
                Ok(())
            }
        }
    }

    /// Stop all containers and shut down the shared VM for a stack.
    ///
    /// Each container is stopped via `oci_kill` + `oci_delete`, then the
    /// shared VM is torn down. Container metadata is updated to `Stopped`.
    pub async fn shutdown_shared_vm(&self, stack_id: &str) -> Result<(), OciError> {
        let _stack_lifecycle_guard = self
            .stack_lifecycle_lock(stack_id)
            .await
            .write_owned()
            .await;
        self.shutdown_shared_vm_locked(stack_id, stack_id)
            .await
            .map(|_| ())
    }

    /// Shut down a shared VM while the caller holds the stack lifecycle writer.
    async fn shutdown_shared_vm_locked(
        &self,
        stack_id: &str,
        request_id: &str,
    ) -> Result<Option<vz_linux::DockerShutdownComplete>, OciError> {
        let (stack_vms_count, stack_port_forwards_count) = {
            let vms = self.stack_vms.lock().await;
            let pfs = self.stack_port_forwards.lock().await;
            (vms.len(), pfs.len())
        };
        tracing::info!(
            target: "vz_post_stop",
            stack_id = %stack_id,
            stack_vms_count,
            stack_port_forwards_count,
            "[L4/stack-vm] shutdown_shared_vm entry"
        );
        let stack_containers: Vec<String> = {
            let routes = self.container_stack.lock().await;
            routes
                .iter()
                .filter(|(_, member_stack_id)| *member_stack_id == stack_id)
                .map(|(container_id, _)| container_id.clone())
                .collect()
        };
        // Stack lifecycle is already exclusively held. Acquire every member ID
        // in canonical order and retain those local+OS writers through guest
        // teardown, metadata publication, map cleanup, and VM stop.
        let container_admissions = self
            .acquire_sorted_container_write_admissions(&stack_containers)
            .await?;
        let activation_lock = self.stack_activation_lock(stack_id).await;
        let _activation_guard = activation_lock.lock().await;
        {
            let mut exec_bindings = self.container_exec_bindings.lock().await;
            for container_id in &stack_containers {
                exec_bindings.remove(container_id);
            }
        }

        let Some(record) = self.stack_vms.lock().await.get(stack_id).cloned() else {
            // Bug B fix: in-memory state can be empty after a daemon
            // respawn (kill -9 / OS reboot mid-operation). In that case
            // the SQLite state-store may still claim the sandbox is
            // running, but we have no VM handle to shut down. Treat
            // this as idempotent "already stopped" rather than the
            // previous error path that relied on a string-match mask
            // (`runtime_shutdown_error_is_not_active`) in the gRPC
            // handler. Still drop any leftover port-forward map entry
            // for this stack so subsequent boots start from a clean slate.
            tracing::warn!(
                stack_id,
                "shutdown_shared_vm: no in-memory VM (likely after daemon respawn); treating as already-stopped"
            );
            shutdown_port_forwarding_registry_entry(&self.stack_port_forwards, stack_id).await?;
            commit_stack_cleanup_batch(
                self,
                &self.container_stack,
                &self.vm_handles,
                &stack_containers,
            )
            .await;
            self.clear_stack_vm_stop_complete(stack_id);
            return Ok(None);
        };
        require_complete_shared_vm_boot(stack_id, record.boot_complete)?;
        let vm = Arc::clone(&record.vm);

        // Stop each container via OCI lifecycle, then tear down and verify its
        // generation-owned guest overlay before publishing host cleanup.
        let mut cleanup_failures = Vec::new();
        for cid in &stack_containers {
            self.stop_log_rotation_task(cid).await;
            let status = match self.container_store.find(cid).map_err(OciError::from)? {
                Some(container) => container.status,
                None => {
                    cleanup_failures.push(format!(
                        "container '{cid}' metadata is missing during shared VM shutdown"
                    ));
                    continue;
                }
            };
            let generation = container_admissions
                .iter()
                .find(|admission| admission.container_id == *cid)
                .and_then(|admission| admission.generation);
            let Some(generation) = generation else {
                cleanup_failures.push(format!(
                    "container '{cid}' has no durable generation for guest overlay teardown"
                ));
                continue;
            };
            if let Err(error) = stop_or_reuse_exit_code(
                &*vm,
                cid,
                &status,
                self.overlay_cleanup_is_pending(cid, generation),
                false,
                STOP_GRACE_PERIOD,
                None,
            )
            .await
            {
                cleanup_failures.push(format!(
                    "container '{cid}' stop failed before OCI delete: {error}"
                ));
                continue;
            }
            if let Err(error) = shutdown_container_cleanup_transition(
                self,
                cid,
                generation,
                || async {
                    vm.oci_delete(cid.to_string(), true)
                        .await
                        .map_err(OciError::from)
                },
                || async {
                    self.teardown_owned_stack_container_overlay(&vm, cid, generation)
                        .await
                },
            )
            .await
            {
                cleanup_failures.push(error.to_string());
            }
        }

        if !cleanup_failures.is_empty() {
            return Err(OciError::InvalidConfig(format!(
                "shared VM shutdown retained stack '{stack_id}' routing and VM for cleanup retry: {}",
                cleanup_failures.join("; ")
            )));
        }

        // Shut down relays and the VM while every ownership registry remains
        // published. Retry can therefore resume from stopped metadata without
        // re-signalling or losing the shared VM handle.
        let mut infrastructure_failures = Vec::new();
        let pf_present = {
            let mut guard = self.stack_port_forwards.lock().await;
            if let Some(pf) = guard.get_mut(stack_id) {
                tracing::info!(
                    target: "vz_post_stop",
                    stack_id = %stack_id,
                    "[L4/stack-vm] shutdown_shared_vm: awaiting PortForwarding::shutdown"
                );
                let started = std::time::Instant::now();
                if let Err(error) = pf.shutdown().await {
                    infrastructure_failures.push(error.to_string());
                }
                tracing::info!(
                    target: "vz_post_stop",
                    stack_id = %stack_id,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "[L4/stack-vm] shutdown_shared_vm: PortForwarding::shutdown returned"
                );
                true
            } else {
                false
            }
        };
        if !pf_present {
            tracing::info!(
                target: "vz_post_stop",
                stack_id = %stack_id,
                "[L4/stack-vm] shutdown_shared_vm: no PortForwarding registered for stack"
            );
        }

        // A Developer disk must be normally unmounted by its owning guest
        // before VZ's hard power stop. Failure retains the VM and all ownership
        // records; it is not permission to cut power or publish Stopped.
        let docker_shutdown = if record.docker_provisioned {
            let mut cached = record.docker_shutdown.lock().await;
            if cached.is_none() {
                *cached = Some(vm.shutdown_docker(request_id.to_string()).await?);
            }
            if cached
                .as_ref()
                .is_some_and(|receipt| receipt.request_id != request_id)
            {
                return Err(OciError::InvalidConfig(
                    "Docker closure belongs to another shutdown operation; exact recovery required"
                        .into(),
                ));
            }
            cached.clone()
        } else {
            None
        };
        if !infrastructure_failures.is_empty() {
            return Err(OciError::InvalidConfig(format!(
                "shared VM shutdown retained stack '{stack_id}' after relay teardown failure: {}",
                infrastructure_failures.join("; ")
            )));
        }
        if !self.stack_vm_stop_is_complete(stack_id) {
            match vm.stop().await {
                Ok(()) => self.mark_stack_vm_stop_complete(stack_id),
                Err(error) => infrastructure_failures.push(format!("VM stop failed: {error}")),
            }
        }
        if !infrastructure_failures.is_empty() {
            return Err(OciError::InvalidConfig(format!(
                "VZ_STACK_TEARDOWN_VIOLATION:SHARED_VM_STOP_FAILED shared VM shutdown retained stack '{stack_id}' ownership for retry: {}",
                infrastructure_failures.join("; ")
            )));
        }

        // No fallible teardown remains: publish the complete registry commit.
        if pf_present {
            self.stack_port_forwards.lock().await.remove(stack_id);
        }
        self.stack_vms.lock().await.remove(stack_id);
        self.clear_stack_vm_stop_complete(stack_id);
        commit_stack_cleanup_batch(
            self,
            &self.container_stack,
            &self.vm_handles,
            &stack_containers,
        )
        .await;
        let (stack_vms_count_after, stack_port_forwards_count_after) = {
            let vms = self.stack_vms.lock().await;
            let pfs = self.stack_port_forwards.lock().await;
            (vms.len(), pfs.len())
        };
        tracing::info!(
            target: "vz_post_stop",
            stack_id = %stack_id,
            stack_vms_count_after,
            stack_port_forwards_count_after,
            "[L4/stack-vm] shutdown_shared_vm complete"
        );
        Ok(docker_shutdown)
    }

    /// Return the identity of the currently active shared-runtime boot.
    pub async fn inspect_shared_vm_identity(
        &self,
        stack_id: &str,
    ) -> Result<Option<vz_runtime_contract::StackRuntimeIdentity>, OciError> {
        let _stack_lifecycle_guard = self.stack_lifecycle_lock(stack_id).await.read_owned().await;
        Ok(self
            .stack_vms
            .lock()
            .await
            .get(stack_id)
            .map(|record| record.identity.clone()))
    }

    /// Atomically boot or reuse one managed shared VM and retain exact
    /// lifecycle ownership in the returned lease.
    ///
    /// This managed entrypoint requires an explicit Linux profile. An active
    /// boot is reusable only when its verified artifact profile and complete
    /// boot request match exactly. The write fence used for boot/inspection is
    /// downgraded directly into the returned read lease, leaving no replacement
    /// window between observation and subsequent readiness work.
    pub async fn boot_or_inspect_shared_vm(
        &self,
        stack_id: &str,
        ports: Vec<PortMapping>,
        resources: vz_runtime_contract::StackResourceHint,
    ) -> Result<SharedVmLifecycleLease, OciError> {
        vz_runtime_contract::StackRuntimeIdentity::new(stack_id)
            .map_err(OciError::InvalidConfig)?;
        let required_profile = require_explicit_verified_profile(
            self.config.linux_profile,
            self.config.linux_profile,
            "boot_or_inspect_shared_vm",
        )?;
        let stack_lifecycle_guard = self
            .stack_lifecycle_lock(stack_id)
            .await
            .write_owned()
            .await;

        let current = { self.stack_vms.lock().await.get(stack_id).cloned() };
        let record = match current {
            Some(record) => {
                require_complete_shared_vm_boot(stack_id, record.boot_complete)?;
                require_explicit_verified_profile(
                    Some(required_profile),
                    record.verified_linux_profile,
                    "boot_or_inspect_shared_vm",
                )?;
                require_matching_shared_vm_boot_request(
                    stack_id,
                    &record.boot_ports,
                    &record.boot_resources,
                    &ports,
                    &resources,
                )?;
                record
            }
            None => {
                self.boot_shared_vm_locked(stack_id, ports, resources, Some(required_profile))
                    .await?;
                self.stack_vms
                    .lock()
                    .await
                    .get(stack_id)
                    .cloned()
                    .ok_or_else(|| OciError::SharedRuntimeAbsent {
                        stack_id: stack_id.to_string(),
                    })?
            }
        };
        let verified_profile = require_explicit_verified_profile(
            Some(required_profile),
            record.verified_linux_profile,
            "boot_or_inspect_shared_vm",
        )?;
        let stack_lifecycle_guard = stack_lifecycle_guard.downgrade();
        Ok(SharedVmLifecycleLease {
            runtime_identity: record.identity,
            verified_profile,
            stack_vms: Arc::clone(&self.stack_vms),
            _stack_lifecycle_guard: stack_lifecycle_guard,
        })
    }

    /// Start and health-check the private Docker Engine in one exact shared VM.
    ///
    /// The returned socket path is guest-local. This method does not create a
    /// host proxy or Docker context. The stack lifecycle reader is retained
    /// through the guest Engine API health check, so a shutdown or replacement
    /// cannot cross the generation check before the lazy-start effect.
    pub async fn ensure_shared_vm_docker_ready_exact(
        &self,
        expected: &vz_runtime_contract::StackRuntimeIdentity,
    ) -> Result<SharedVmDockerReadiness, OciError> {
        expected.validate().map_err(OciError::InvalidConfig)?;
        let stack_id = expected.stack_id.as_str();
        let _stack_lifecycle_guard = self.stack_lifecycle_lock(stack_id).await.read_owned().await;
        let current = self.stack_vms.lock().await.get(stack_id).cloned();
        require_exact_stack_runtime(current.as_ref().map(|record| &record.identity), expected)?;
        let Some(current) = current else {
            return Err(OciError::SharedRuntimeAbsent {
                stack_id: stack_id.to_string(),
            });
        };
        Self::docker_readiness_for_record(&current, "ensure_shared_vm_docker_ready_exact").await
    }

    /// Atomically compare and stop exactly one shared-runtime boot.
    pub async fn shutdown_shared_vm_exact(
        &self,
        request: &vz_runtime_contract::StackRuntimeShutdownRequest,
    ) -> Result<vz_runtime_contract::StackRuntimeShutdownOutcome, OciError> {
        self.shutdown_shared_vm_with_receipt_exact(request)
            .await
            .map(|(outcome, _)| outcome)
    }

    /// Stop an exact boot and return its positive guest Docker disk closure.
    /// An absent or replacement boot never manufactures a filesystem receipt.
    pub async fn shutdown_shared_vm_with_receipt_exact(
        &self,
        request: &vz_runtime_contract::StackRuntimeShutdownRequest,
    ) -> Result<
        (
            vz_runtime_contract::StackRuntimeShutdownOutcome,
            Option<vz_linux::DockerShutdownComplete>,
        ),
        OciError,
    > {
        request.validate().map_err(OciError::InvalidConfig)?;
        let expected = &request.expected;
        let stack_id = expected.stack_id.as_str();
        let _stack_lifecycle_guard = self
            .stack_lifecycle_lock(stack_id)
            .await
            .write_owned()
            .await;
        let current = self.stack_vms.lock().await.get(stack_id).cloned();
        match classify_stack_runtime_shutdown(
            current.as_ref().map(|record| &record.identity),
            expected,
        ) {
            vz_runtime_contract::StackRuntimeShutdownOutcome::Stopped => {
                let receipt = self
                    .shutdown_shared_vm_locked(stack_id, &request.operation_id)
                    .await?;
                Ok((
                    vz_runtime_contract::StackRuntimeShutdownOutcome::Stopped,
                    receipt,
                ))
            }
            outcome => Ok((outcome, None)),
        }
    }

    /// Check whether a shared VM is owned for the given stack.
    ///
    /// This includes incomplete/failed bootstrap ownership and is not readiness
    /// proof. Reuse and execution separately require a completed bootstrap.
    pub async fn has_shared_vm(&self, stack_id: &str) -> bool {
        self.stack_vms.lock().await.contains_key(stack_id)
    }

    /// Reject raw shared-VM state snapshots.
    ///
    /// A shared VM depends on external VirtioFS and device state that a VZ
    /// machine-state file does not capture atomically. Treating that file as a
    /// full checkpoint would therefore violate the runtime contract.
    pub async fn save_shared_vm_snapshot(
        &self,
        _stack_id: &str,
        _state_path: impl AsRef<Path>,
    ) -> Result<(), OciError> {
        Err(OciError::UnsupportedOperation {
            operation: vz_runtime_contract::RuntimeOperation::CreateCheckpoint
                .as_str()
                .to_string(),
            reason: SHARED_VM_FULL_CHECKPOINT_UNSUPPORTED_REASON.to_string(),
        })
    }

    /// Reject restore from a raw shared-VM state snapshot.
    ///
    /// This remains fail-closed even if the canonical capability matrix changes:
    /// this primitive has no coordinated snapshot of external VirtioFS/device
    /// state and cannot implement a full checkpoint by itself.
    pub async fn restore_shared_vm_snapshot(
        &self,
        _stack_id: &str,
        _state_path: impl AsRef<Path>,
    ) -> Result<(), OciError> {
        Err(OciError::UnsupportedOperation {
            operation: vz_runtime_contract::RuntimeOperation::RestoreCheckpoint
                .as_str()
                .to_string(),
            reason: SHARED_VM_FULL_CHECKPOINT_UNSUPPORTED_REASON.to_string(),
        })
    }

    /// Execute a raw command in the shared VM (not through the OCI runtime).
    ///
    /// Useful for diagnostics, inspecting the guest filesystem, or running
    /// non-containerized commands inside the VM.
    pub async fn exec_in_shared_vm(
        &self,
        stack_id: &str,
        command: String,
        args: Vec<String>,
        timeout: Duration,
    ) -> Result<ExecOutput, OciError> {
        let _stack_lifecycle_guard = self.stack_lifecycle_lock(stack_id).await.read_owned().await;
        self.ensure_stack_not_tearing_down(stack_id, "execute in")
            .await?;
        let vm = self
            .stack_vms
            .lock()
            .await
            .get(stack_id)
            .map(|record| record.vm.clone())
            .ok_or_else(|| {
                OciError::InvalidConfig(format!("no shared VM running for stack '{stack_id}'"))
            })?;

        let result = vm.exec_collect(command, args, timeout).await?;

        Ok(ExecOutput {
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
        })
    }

    /// Set up per-service network isolation inside the shared VM.
    ///
    /// Creates a bridge and per-service network namespaces so that
    /// containers can communicate using real IP addresses.
    pub async fn network_setup(
        &self,
        stack_id: &str,
        services: Vec<vz::protocol::NetworkServiceConfig>,
    ) -> Result<(), OciError> {
        let _stack_lifecycle_guard = self.stack_lifecycle_lock(stack_id).await.read_owned().await;
        self.ensure_stack_not_tearing_down(stack_id, "configure networking in")
            .await?;
        let vm = self
            .stack_vms
            .lock()
            .await
            .get(stack_id)
            .map(|record| record.vm.clone())
            .ok_or_else(|| {
                OciError::InvalidConfig(format!("no shared VM running for stack '{stack_id}'"))
            })?;

        vm.network_setup(stack_id.to_string(), services)
            .await
            .map_err(OciError::from)
    }

    /// Tear down per-service network resources inside the shared VM.
    pub async fn network_teardown(
        &self,
        stack_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), OciError> {
        let _stack_lifecycle_guard = self.stack_lifecycle_lock(stack_id).await.read_owned().await;
        self.ensure_stack_not_tearing_down(stack_id, "tear down networking in")
            .await?;
        let vm = self
            .stack_vms
            .lock()
            .await
            .get(stack_id)
            .map(|record| record.vm.clone())
            .ok_or_else(|| {
                OciError::InvalidConfig(format!("no shared VM running for stack '{stack_id}'"))
            })?;

        vm.network_teardown(stack_id.to_string(), service_names)
            .await
            .map_err(OciError::from)
    }
}
