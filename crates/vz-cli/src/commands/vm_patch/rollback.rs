use super::*;

pub(super) fn apply_operations_transactional(
    root: &Path,
    manifest: &PatchBundleManifest,
    payload_by_digest: &BTreeMap<String, Vec<u8>>,
) -> anyhow::Result<()> {
    let mut applied = Vec::new();
    for (index, operation) in manifest.operations.iter().enumerate() {
        let path = operation.path();
        let relative =
            guest_path_to_relative(path).with_context(|| operation_error_context(index, path))?;
        let host_path = root.join(&relative);
        let snapshot =
            capture_snapshot(&host_path).with_context(|| operation_error_context(index, path))?;
        applied.push(RollbackEntry {
            operation_index: index,
            path: path.to_string(),
            host_path: host_path.clone(),
            snapshot,
        });

        if let Err(err) = apply_operation(root, &relative, &host_path, operation, payload_by_digest)
        {
            let message = format!("{}: {err:#}", operation_error_context(index, path));
            let rollback_errors = rollback_operations(&applied);
            return Err(error_with_rollback(message, rollback_errors));
        }
    }

    if let Err(err) = verify_post_state_hashes(root, manifest) {
        let rollback_errors = rollback_operations(&applied);
        return Err(error_with_rollback(
            format!("post-state hash verification failed: {err:#}"),
            rollback_errors,
        ));
    }

    Ok(())
}

fn operation_error_context(index: usize, path: &str) -> String {
    format!("operation[{index}] path '{path}'")
}

fn error_with_rollback(message: String, rollback_errors: Vec<String>) -> anyhow::Error {
    if rollback_errors.is_empty() {
        anyhow!(message)
    } else {
        anyhow!(
            "{message}; rollback encountered {} error(s): {}",
            rollback_errors.len(),
            rollback_errors.join(" | ")
        )
    }
}

fn rollback_operations(entries: &[RollbackEntry]) -> Vec<String> {
    let mut errors = Vec::new();
    for entry in entries.iter().rev() {
        if let Err(err) = restore_snapshot(&entry.host_path, &entry.snapshot) {
            errors.push(format!(
                "operation[{}] path '{}': {:#}",
                entry.operation_index, entry.path, err
            ));
        }
    }
    errors
}

fn apply_operation(
    root: &Path,
    relative: &Path,
    host_path: &Path,
    operation: &PatchOperation,
    payload_by_digest: &BTreeMap<String, Vec<u8>>,
) -> anyhow::Result<()> {
    match operation {
        PatchOperation::Mkdir { mode, .. } => apply_mkdir(root, relative, host_path, *mode),
        PatchOperation::WriteFile {
            content_digest,
            mode,
            ..
        } => apply_write_file(
            root,
            relative,
            host_path,
            content_digest,
            *mode,
            payload_by_digest,
        ),
        PatchOperation::DeleteFile { .. } => apply_delete_file(root, relative, host_path),
        PatchOperation::Symlink { target, .. } => apply_symlink(root, relative, host_path, target),
        PatchOperation::SetOwner { uid, gid, .. } => {
            apply_set_owner(root, relative, host_path, *uid, *gid)
        }
        PatchOperation::SetMode { mode, .. } => apply_set_mode(root, relative, host_path, *mode),
    }
}

fn apply_mkdir(
    root: &Path,
    relative: &Path,
    host_path: &Path,
    mode: Option<u32>,
) -> anyhow::Result<()> {
    ensure_ancestors_within_root(root, relative.parent().unwrap_or(Path::new("")))?;

    match fs::symlink_metadata(host_path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!("refusing to follow symlink at {}", host_path.display());
            }
            if !metadata.is_dir() {
                bail!("mkdir target already exists and is not a directory");
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {
            fs::create_dir_all(host_path)
                .with_context(|| format!("failed to create directory {}", host_path.display()))?;
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to inspect directory {}", host_path.display()));
        }
    }

    if let Some(mode) = mode {
        set_path_mode(host_path, mode)?;
    }

    Ok(())
}

fn apply_write_file(
    root: &Path,
    relative: &Path,
    host_path: &Path,
    content_digest: &str,
    mode: Option<u32>,
    payload_by_digest: &BTreeMap<String, Vec<u8>>,
) -> anyhow::Result<()> {
    let parent = ensure_parent_dir_within_root(root, relative)?;
    let digest = normalize_sha256_field("write_file.content_digest", content_digest)?;
    let contents = payload_by_digest
        .get(&digest)
        .ok_or_else(|| anyhow!("missing payload content for digest '{}'", digest))?;

    let mut existing_mode = None;
    match fs::symlink_metadata(host_path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                bail!("write_file target {} is a directory", host_path.display());
            }
            if metadata.file_type().is_symlink() {
                fs::remove_file(host_path).with_context(|| {
                    format!("failed to remove existing symlink {}", host_path.display())
                })?;
            } else if metadata.is_file() {
                existing_mode = Some(metadata.mode() & 0o7777);
            } else {
                bail!(
                    "write_file target {} has unsupported file type",
                    host_path.display()
                );
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("failed to inspect {}", host_path.display()));
        }
    }

    let effective_mode = mode.unwrap_or(existing_mode.unwrap_or(DEFAULT_FILE_MODE));
    write_file_atomic(&parent, host_path, contents, effective_mode)
}

fn apply_delete_file(root: &Path, relative: &Path, host_path: &Path) -> anyhow::Result<()> {
    ensure_parent_dir_within_root(root, relative)?;

    match fs::symlink_metadata(host_path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                bail!("delete_file target {} is a directory", host_path.display());
            }
            fs::remove_file(host_path)
                .with_context(|| format!("failed to delete {}", host_path.display()))?;
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("failed to inspect {}", host_path.display()));
        }
    }

    Ok(())
}

fn apply_symlink(
    root: &Path,
    relative: &Path,
    host_path: &Path,
    target: &str,
) -> anyhow::Result<()> {
    ensure_parent_dir_within_root(root, relative)?;

    match fs::symlink_metadata(host_path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                bail!("symlink target path {} is a directory", host_path.display());
            }
            if metadata.file_type().is_symlink() {
                let existing_target = fs::read_link(host_path).with_context(|| {
                    format!("failed to inspect symlink {}", host_path.display())
                })?;
                if existing_target == Path::new(target) {
                    return Ok(());
                }
            }
            fs::remove_file(host_path)
                .with_context(|| format!("failed to remove {}", host_path.display()))?;
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("failed to inspect {}", host_path.display()));
        }
    }

    symlink(target, host_path).with_context(|| {
        format!(
            "failed to create symlink {} -> {}",
            host_path.display(),
            target
        )
    })?;
    Ok(())
}

fn apply_set_owner(
    root: &Path,
    relative: &Path,
    host_path: &Path,
    uid: u32,
    gid: u32,
) -> anyhow::Result<()> {
    ensure_parent_dir_within_root(root, relative)?;
    let _ = fs::symlink_metadata(host_path)
        .with_context(|| format!("set_owner path {} does not exist", host_path.display()))?;
    set_path_owner(host_path, uid, gid)
}

fn apply_set_mode(root: &Path, relative: &Path, host_path: &Path, mode: u32) -> anyhow::Result<()> {
    ensure_parent_dir_within_root(root, relative)?;
    let metadata = fs::symlink_metadata(host_path)
        .with_context(|| format!("set_mode path {} does not exist", host_path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!(
            "set_mode refuses to operate on symlink {}",
            host_path.display()
        );
    }
    set_path_mode(host_path, mode)
}

pub(super) fn verify_post_state_hashes(
    root: &Path,
    manifest: &PatchBundleManifest,
) -> anyhow::Result<()> {
    for (guest_path, digest) in &manifest.post_state_hashes {
        let relative = guest_path_to_relative(guest_path)
            .with_context(|| format!("invalid post_state path '{guest_path}'"))?;
        ensure_parent_dir_within_root(root, &relative)?;
        let host_path = root.join(&relative);
        let expected = normalize_sha256_field(
            &format!("manifest.post_state_hashes['{guest_path}']"),
            digest,
        )?;

        let metadata = fs::symlink_metadata(&host_path)
            .with_context(|| format!("post_state path '{}' does not exist", host_path.display()))?;
        let actual = if metadata.is_file() {
            ipsw::sha256_file(&host_path)
                .with_context(|| format!("failed to hash {}", host_path.display()))?
        } else if metadata.file_type().is_symlink() {
            let target = fs::read_link(&host_path).with_context(|| {
                format!("failed to read symlink target for {}", host_path.display())
            })?;
            sha256_bytes_hex(target.as_os_str().as_bytes())
        } else {
            bail!(
                "post_state path '{}' is not a regular file or symlink",
                guest_path
            );
        };

        if actual != expected {
            bail!(
                "post-state hash mismatch for '{}': expected {}, actual {}",
                guest_path,
                expected,
                actual
            );
        }
    }

    Ok(())
}

pub(super) fn write_file_atomic(
    parent: &Path,
    destination: &Path,
    contents: &[u8],
    mode: u32,
) -> anyhow::Result<()> {
    let (temp_path, mut temp_file) = create_temp_file(parent)?;
    if let Err(err) = (|| -> anyhow::Result<()> {
        temp_file
            .write_all(contents)
            .with_context(|| format!("failed to write temp file {}", temp_path.display()))?;
        temp_file
            .sync_all()
            .with_context(|| format!("failed to sync temp file {}", temp_path.display()))?;
        drop(temp_file);
        fs::set_permissions(&temp_path, fs::Permissions::from_mode(mode))
            .with_context(|| format!("failed to set mode on {}", temp_path.display()))?;
        fs::rename(&temp_path, destination).with_context(|| {
            format!(
                "failed to move temp file {} to {}",
                temp_path.display(),
                destination.display()
            )
        })?;
        Ok(())
    })() {
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }

    Ok(())
}

fn create_temp_file(parent: &Path) -> anyhow::Result<(PathBuf, File)> {
    for _ in 0..64 {
        let next = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(".vzpatch.tmp.{next}"));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to create temp file {}", candidate.display())
                });
            }
        }
    }

    bail!(
        "failed to create unique temp file under {} after multiple attempts",
        parent.display()
    )
}

pub(super) fn guest_path_to_relative(guest_path: &str) -> anyhow::Result<PathBuf> {
    validate_operation_path("operation.path", guest_path)?;
    let mut relative = PathBuf::new();
    for component in Path::new(guest_path).components() {
        match component {
            Component::RootDir => {}
            Component::Normal(part) => relative.push(part),
            Component::CurDir | Component::ParentDir => {
                bail!("path must not contain traversal components");
            }
            Component::Prefix(_) => {
                bail!("path contains unsupported platform prefix");
            }
        }
    }

    if relative.as_os_str().is_empty() {
        bail!("path '/' is not allowed");
    }

    Ok(relative)
}

fn ensure_ancestors_within_root(root: &Path, relative_ancestor: &Path) -> anyhow::Result<()> {
    let mut current = root.to_path_buf();
    for component in relative_ancestor.components() {
        let Component::Normal(part) = component else {
            bail!(
                "invalid path component '{}' while enforcing path safety",
                component.as_os_str().to_string_lossy()
            );
        };
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    bail!(
                        "path escapes apply root via symlink component {}",
                        current.display()
                    );
                }
                if !metadata.is_dir() {
                    bail!(
                        "path escapes apply root because component {} is not a directory",
                        current.display()
                    );
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => break,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to inspect ancestor component {}", current.display())
                });
            }
        }
    }

    Ok(())
}

fn ensure_parent_dir_within_root(root: &Path, relative: &Path) -> anyhow::Result<PathBuf> {
    let parent_relative = relative.parent().unwrap_or(Path::new(""));
    ensure_ancestors_within_root(root, parent_relative)?;
    let parent = root.join(parent_relative);
    let metadata = fs::symlink_metadata(&parent)
        .with_context(|| format!("parent directory {} does not exist", parent.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("parent directory {} is a symlink", parent.display());
    }
    if !metadata.is_dir() {
        bail!("parent path {} is not a directory", parent.display());
    }
    Ok(parent)
}

fn capture_snapshot(path: &Path) -> anyhow::Result<PathSnapshot> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                let target = fs::read_link(path)
                    .with_context(|| format!("failed to read symlink {}", path.display()))?;
                Ok(PathSnapshot::Symlink {
                    target,
                    owner: PosixOwner {
                        uid: metadata.uid(),
                        gid: metadata.gid(),
                    },
                })
            } else if metadata.is_file() {
                let contents = fs::read(path)
                    .with_context(|| format!("failed to snapshot file {}", path.display()))?;
                Ok(PathSnapshot::File {
                    contents,
                    metadata: PosixMetadata {
                        mode: metadata.mode() & 0o7777,
                        owner: PosixOwner {
                            uid: metadata.uid(),
                            gid: metadata.gid(),
                        },
                    },
                })
            } else if metadata.is_dir() {
                Ok(PathSnapshot::Directory {
                    metadata: PosixMetadata {
                        mode: metadata.mode() & 0o7777,
                        owner: PosixOwner {
                            uid: metadata.uid(),
                            gid: metadata.gid(),
                        },
                    },
                })
            } else {
                bail!(
                    "cannot snapshot unsupported file type at {}",
                    path.display()
                )
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(PathSnapshot::Missing),
        Err(err) => Err(err).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

fn restore_snapshot(path: &Path, snapshot: &PathSnapshot) -> anyhow::Result<()> {
    match snapshot {
        PathSnapshot::Missing => remove_path_if_exists(path),
        PathSnapshot::File { contents, metadata } => {
            ensure_parent_exists_for_restore(path)?;
            remove_path_if_exists(path)?;
            fs::write(path, contents)
                .with_context(|| format!("failed to restore file {}", path.display()))?;
            set_path_mode(path, metadata.mode)?;
            set_path_owner(path, metadata.owner.uid, metadata.owner.gid)?;
            Ok(())
        }
        PathSnapshot::Directory { metadata } => {
            match fs::symlink_metadata(path) {
                Ok(current) if current.is_dir() => {}
                Ok(_) => {
                    remove_path_if_exists(path)?;
                    fs::create_dir_all(path).with_context(|| {
                        format!("failed to recreate directory {}", path.display())
                    })?;
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    fs::create_dir_all(path).with_context(|| {
                        format!("failed to recreate directory {}", path.display())
                    })?;
                }
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("failed to inspect {}", path.display()));
                }
            }
            set_path_mode(path, metadata.mode)?;
            set_path_owner(path, metadata.owner.uid, metadata.owner.gid)?;
            Ok(())
        }
        PathSnapshot::Symlink { target, owner } => {
            ensure_parent_exists_for_restore(path)?;
            remove_path_if_exists(path)?;
            symlink(target, path).with_context(|| {
                format!(
                    "failed to restore symlink {} -> {}",
                    path.display(),
                    target.display()
                )
            })?;
            set_path_owner(path, owner.uid, owner.gid)?;
            Ok(())
        }
    }
}

fn ensure_parent_exists_for_restore(path: &Path) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("path {} has no parent", path.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create parent directory {}", parent.display()))
}

fn remove_path_if_exists(path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                fs::remove_dir_all(path)
                    .with_context(|| format!("failed to remove directory {}", path.display()))?;
            } else {
                fs::remove_file(path)
                    .with_context(|| format!("failed to remove file {}", path.display()))?;
            }
            Ok(())
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

fn set_path_mode(path: &Path, mode: u32) -> anyhow::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("failed to set mode {mode:o} on {}", path.display()))
}

fn set_path_owner(path: &Path, uid: u32, gid: u32) -> anyhow::Result<()> {
    lchown(path, Some(uid), Some(gid))
        .with_context(|| format!("failed to set owner {uid}:{gid} on {}", path.display()))
}

pub(super) fn load_payload_archive(
    payload_path: &Path,
) -> anyhow::Result<BTreeMap<String, Vec<u8>>> {
    let payload_file = File::open(payload_path)
        .with_context(|| format!("failed to open payload {}", payload_path.display()))?;
    let decoder = zstd::Decoder::new(payload_file)
        .with_context(|| format!("failed to decode zstd payload {}", payload_path.display()))?;
    let mut archive = tar::Archive::new(decoder);
    let mut payload_by_digest = BTreeMap::new();

    let entries = archive
        .entries()
        .with_context(|| format!("failed to read tar payload {}", payload_path.display()))?;

    for entry in entries {
        let mut entry = entry.with_context(|| {
            format!("failed to inspect tar entry in {}", payload_path.display())
        })?;
        if !entry.header().entry_type().is_file() {
            continue;
        }

        let entry_path = entry
            .path()
            .context("failed to read tar entry path")?
            .into_owned();
        let digest = payload_entry_digest(&entry_path)?;

        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .with_context(|| format!("failed to read payload entry {}", entry_path.display()))?;
        let actual = sha256_bytes_hex(&bytes);
        if actual != digest {
            bail!(
                "payload entry '{}' digest mismatch: expected {}, actual {}",
                entry_path.display(),
                digest,
                actual
            );
        }

        if payload_by_digest.insert(digest.clone(), bytes).is_some() {
            bail!(
                "payload contains duplicate content digest entry '{}'",
                digest
            );
        }
    }

    Ok(payload_by_digest)
}
