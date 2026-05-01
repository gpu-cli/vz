use super::*;

pub(super) fn resolve_oci_runtime_binary_path(
    runtime_kind: OciRuntimeKind,
    configured_path: Option<&Path>,
    kernel: &KernelPaths,
) -> Result<PathBuf, OciError> {
    let binary = configured_path
        .map(PathBuf::from)
        .unwrap_or_else(|| kernel.youki.clone());
    validate_oci_runtime_binary_path(runtime_kind, &binary)?;
    Ok(binary)
}

pub(super) fn validate_oci_runtime_binary_path(
    runtime_kind: OciRuntimeKind,
    path: &Path,
) -> Result<(), OciError> {
    let expected_binary = runtime_kind.binary_name();
    let Some(file_name) = path.file_name() else {
        return Err(OciError::InvalidConfig(format!(
            "guest oci runtime path must end with '{expected_binary}': {}",
            path.display()
        )));
    };

    if file_name != expected_binary {
        return Err(OciError::InvalidConfig(format!(
            "guest oci runtime path must point to '{expected_binary}': {}",
            path.display()
        )));
    }

    if !path.is_file() {
        return Err(OciError::InvalidConfig(format!(
            "guest oci runtime binary not found: {}",
            path.display()
        )));
    }

    Ok(())
}

/// Convert public `MountSpec` entries to internal `BundleMount` entries for
/// OCI runtime-spec generation.
///
/// `tag_offset` shifts the VirtioFS mount tag indices (e.g., `vz-mount-{N}`)
/// so that multiple containers in a shared VM can have non-overlapping tags.
/// Pass 0 for single-VM mode.
pub(super) fn mount_specs_to_bundle_mounts(
    mounts: &[MountSpec],
    tag_offset: usize,
) -> Result<Vec<BundleMount>, OciError> {
    let mut bundle_mounts = Vec::with_capacity(mounts.len());
    for (idx, spec) in mounts.iter().enumerate() {
        if !spec.target.is_absolute() {
            return Err(OciError::InvalidConfig(format!(
                "mount target must be an absolute path: {}",
                spec.target.display()
            )));
        }

        let (typ, source, options) = match &spec.mount_type {
            MountType::Bind => {
                let source = spec.source.clone().ok_or_else(|| {
                    OciError::InvalidConfig(format!(
                        "bind mount at {} requires a source path",
                        spec.target.display()
                    ))
                })?;
                let mut opts = vec!["rbind".to_string()];
                match spec.access {
                    MountAccess::ReadWrite => opts.push("rw".to_string()),
                    MountAccess::ReadOnly => opts.push("ro".to_string()),
                }
                ("bind".to_string(), source, opts)
            }
            MountType::Tmpfs => {
                let opts = vec!["nosuid".to_string(), "nodev".to_string()];
                ("tmpfs".to_string(), PathBuf::from("tmpfs"), opts)
            }
            MountType::Volume { volume_name } => {
                // Named volumes are backed by the persistent ext4 disk image
                // mounted at /run/vz-oci/volumes inside the guest.
                let source = PathBuf::from(format!("/run/vz-oci/volumes/{volume_name}"));
                let mut opts = vec!["rbind".to_string()];
                match spec.access {
                    MountAccess::ReadWrite => opts.push("rw".to_string()),
                    MountAccess::ReadOnly => opts.push("ro".to_string()),
                }
                ("bind".to_string(), source, opts)
            }
        };

        // Use the virtio mount tag as the in-guest source path for bind mounts.
        // Volume mounts already have their guest path set (from /run/vz-oci/volumes).
        let guest_source = match &spec.mount_type {
            MountType::Bind => {
                let global_idx = tag_offset + idx;
                let base = PathBuf::from(format!("/mnt/vz-mount-{global_idx}"));
                match &spec.subpath {
                    Some(sub) => base.join(sub),
                    None => base,
                }
            }
            MountType::Tmpfs | MountType::Volume { .. } => source,
        };

        bundle_mounts.push(BundleMount {
            destination: spec.target.clone(),
            source: guest_source,
            typ,
            options,
        });
    }
    Ok(bundle_mounts)
}

/// Generate VirtioFS shared directory entries for bind mount sources.
///
/// `tag_offset` shifts the mount tag indices to avoid collisions in shared VM mode.
///
/// Note: VirtioFS requires shared directories, not files. For file bind mounts,
/// we share the parent directory and use the subpath (handled separately in
/// mount_specs_to_bundle_mounts) to access the specific file.
pub(super) fn mount_specs_to_shared_dirs(
    mounts: &[MountSpec],
    tag_offset: usize,
) -> Vec<SharedDirConfig> {
    mounts
        .iter()
        .enumerate()
        .filter_map(|(idx, spec)| {
            if !matches!(spec.mount_type, MountType::Bind) {
                return None;
            }
            let source = spec.source.as_ref()?;
            let global_idx = tag_offset + idx;

            // VirtioFS requires a directory, not a file. For file mounts,
            // share the parent directory and rely on subpath in the container.
            let share_source = if source.is_file() {
                source.parent().map(|p| p.to_path_buf())
            } else {
                Some(source.clone())
            };

            share_source.map(|source| SharedDirConfig {
                tag: format!("vz-mount-{global_idx}"),
                source,
                read_only: matches!(spec.access, MountAccess::ReadOnly),
            })
        })
        .collect()
}

pub(super) fn make_oci_runtime_share(runtime_binary: &Path) -> Result<SharedDirConfig, OciError> {
    let Some(parent) = runtime_binary.parent() else {
        return Err(OciError::InvalidConfig(format!(
            "guest oci runtime path has no parent directory: {}",
            runtime_binary.display()
        )));
    };

    Ok(SharedDirConfig {
        tag: OCI_RUNTIME_BIN_SHARE_TAG.to_string(),
        source: parent.to_path_buf(),
        read_only: true,
    })
}

/// Write an `/etc/hosts` file into the OCI bundle directory.
///
/// The generated file contains standard localhost entries plus one line
/// per extra host mapping (hostname → IP).
pub(super) fn write_hosts_file(
    rootfs_dir: &Path,
    extra_hosts: &[(String, String)],
) -> Result<(), OciError> {
    use std::io::Write;
    let etc_dir = rootfs_dir.join("etc");
    fs::create_dir_all(&etc_dir)?;
    let hosts_path = etc_dir.join("hosts");
    let mut f = fs::File::create(&hosts_path)?;
    writeln!(f, "127.0.0.1\tlocalhost")?;
    writeln!(f, "::1\tlocalhost")?;
    for (hostname, ip) in extra_hosts {
        writeln!(f, "{ip}\t{hostname}")?;
    }
    Ok(())
}

pub(super) fn oci_bundle_host_dir(rootfs_dir: &Path, bundle_guest_path: &str) -> PathBuf {
    rootfs_dir.join(bundle_guest_path.trim_start_matches('/'))
}

pub(super) fn oci_bundle_guest_path(bundle_guest_root: &str, container_id: &str) -> String {
    format!(
        "{}/{}",
        bundle_guest_root.trim_end_matches('/'),
        container_id
    )
}

pub(super) fn oci_bundle_guest_root(guest_state_dir: Option<&Path>) -> Result<String, OciError> {
    let state_dir = guest_state_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(OCI_DEFAULT_GUEST_STATE_DIR));

    if !state_dir.is_absolute() {
        return Err(OciError::InvalidConfig(format!(
            "guest state dir must be an absolute path: {}",
            state_dir.display()
        )));
    }

    let state_lossy = state_dir.to_string_lossy();
    let state_root = state_lossy.trim_end_matches('/');
    if state_root.is_empty() {
        return Ok(format!("/{OCI_BUNDLE_DIRNAME}"));
    }

    Ok(format!("{state_root}/{OCI_BUNDLE_DIRNAME}"))
}

/// Set up a per-container overlay in the guest VM.
///
/// VirtioFS doesn't support mknod, which the OCI runtime needs for default
/// devices (/dev/null etc). This creates a local overlay in the guest with
/// VirtioFS as lowerdir and tmpfs as upperdir so that mknod writes go to the
/// tmpfs layer.
///
/// Returns the guest-side merged rootfs path for use in the OCI bundle spec.
/// VM-level log directory for a container's captured stdout/stderr.
///
/// The init process writes to `/var/log/vz-oci/output.log` inside the container,
/// which is bind-mounted to this directory so logs survive container death.
pub fn container_log_dir(container_id: &str) -> String {
    format!("/run/vz-oci/logs/{container_id}")
}

/// Set up the per-container overlay rootfs in the guest VM and optionally
/// pre-populate the upper layer from a setup-commit tarball.
///
/// `setup_commit_tar_path`: if `Some(guest_path)`, the overlay's upper dir
/// is populated by extracting that tarball BEFORE the overlay is mounted.
/// This is the only correct point to inject content — the kernel does not
/// support reliable concurrent modification of an overlay's upperdir
/// while the overlay is mounted (lookups silently miss the new files).
///
/// Returns `(merged_path, setup_was_restored)`. `setup_was_restored` is
/// `true` if a commit tar was extracted; the caller should then skip
/// `run_setup_commands`.
pub(super) async fn setup_guest_container_overlay(
    vm: &LinuxVm,
    vz_rootfs_path: &str,
    container_id: &str,
    setup_commit_tar_path: Option<&str>,
) -> Result<(String, bool), OciError> {
    let container_overlay = format!("/run/vz-oci/containers/{container_id}");
    let guest_rootfs_path = format!("{container_overlay}/merged");
    let log_dir = container_log_dir(container_id);

    // Clean up any stale overlay from a previous container with the same ID
    // (e.g. during recreate). Best-effort: unmount merged overlay, then the
    // tmpfs backing, then remove the directory tree.  Invalidate the VirtioFS
    // dcache so the kernel re-reads host-side changes (the rootfs may have
    // been deleted + reassembled on the host during recreate).
    let cleanup_cmd = format!(
        "umount {container_overlay}/merged 2>/dev/null; \
         umount {container_overlay} 2>/dev/null; \
         rm -rf {container_overlay}; \
         echo 2 > /proc/sys/vm/drop_caches 2>/dev/null || true"
    );
    let _ = vm
        .exec_collect(
            "sh".to_string(),
            vec!["-c".to_string(), cleanup_cmd],
            Duration::from_secs(5),
        )
        .await;

    // Build the overlay-setup script. If a setup-commit tar is provided,
    // extract it into the upper dir AFTER creating it but BEFORE mounting
    // the overlay — the kernel takes a snapshot of the upperdir state at
    // mount time and won't reliably reflect later additions.
    let extract_step = match setup_commit_tar_path {
        Some(tar) => {
            format!("/bin/busybox tar -C {container_overlay}/upper -xpf {tar} && ")
        }
        None => String::new(),
    };
    let overlay_cmd = format!(
        "mkdir -p {container_overlay} && \
         mount -t tmpfs tmpfs {container_overlay} && \
         mkdir -p {container_overlay}/upper {container_overlay}/work {container_overlay}/merged && \
         {extract_step}\
         mount -t overlay overlay \
         -o lowerdir={vz_rootfs_path},upperdir={container_overlay}/upper,workdir={container_overlay}/work \
         {container_overlay}/merged && \
         mkdir -p {log_dir}"
    );

    let result = vm
        .exec_collect(
            "sh".to_string(),
            vec!["-c".to_string(), overlay_cmd],
            // Restore can take seconds if the tar is large (~400MB ext4-style),
            // so allow generous time when a setup commit is being applied.
            if setup_commit_tar_path.is_some() {
                Duration::from_secs(120)
            } else {
                Duration::from_secs(10)
            },
        )
        .await
        .map_err(OciError::from)?;

    if result.exit_code != 0 {
        return Err(OciError::Linux(LinuxError::Protocol(format!(
            "per-container overlay setup failed (exit {}): {}",
            result.exit_code,
            result.stderr.trim()
        ))));
    }

    Ok((guest_rootfs_path, setup_commit_tar_path.is_some()))
}

pub(super) fn expand_home_dir(path: &Path) -> PathBuf {
    let raw = path.to_string_lossy();
    if raw == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home);
        }
        return path.to_path_buf();
    }

    if let Some(stripped) = raw.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(stripped);
        }
    }

    path.to_path_buf()
}
