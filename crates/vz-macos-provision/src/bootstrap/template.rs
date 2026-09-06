use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{Component, Progress, ReleaseManifest};
use crate::artifact_cache::{Artifact, private_directory};
use crate::image_delta;

const FILES: [&str; 3] = ["disk.img", "hardware-model", "auxiliary-storage-seed"];

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stamp {
    device: u64,
    inode: u64,
    size: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

impl Stamp {
    fn read(path: &Path) -> Result<Self> {
        let m = open_regular(path)?.metadata()?;
        // SAFETY: geteuid only returns the caller's effective user identity.
        #[allow(unsafe_code)]
        let uid = unsafe { libc::geteuid() };
        ensure!(
            m.uid() == uid && m.mode() & 0o777 == 0o400 && m.nlink() == 1,
            "template file must be private, read-only, and not hard-linked"
        );
        Ok(Self {
            device: m.dev(),
            inode: m.ino(),
            size: m.len(),
            modified: (m.mtime(), m.mtime_nsec()),
            changed: (m.ctime(), m.ctime_nsec()),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    schema_version: u32,
    manifest_sha256: String,
    files: [Stamp; 3],
}

/// Verified immutable files, not Machine readiness. Paths are borrowed read-only
/// inputs: never attach a cached disk or auxiliary-storage seed directly to a VM.
/// The native adapter owns fresh Machine identity, auxiliary state and credentials.
pub struct PreparedTemplate {
    directory: PathBuf,
    manifest_sha256: String,
    manifest: ReleaseManifest,
}

impl PreparedTemplate {
    /// Authenticated release metadata for host checks and guest verification.
    pub fn manifest(&self) -> &ReleaseManifest {
        &self.manifest
    }

    /// Persisted identity of the exact manifest JSON, unaffected by channel moves.
    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    /// Immutable hardware model bytes. Check support through the native framework.
    pub fn hardware_model_path(&self) -> PathBuf {
        self.directory.join(FILES[1])
    }

    /// Immutable auxiliary-storage seed. The adapter must allocate private state
    /// and establish its compatibility with the Machine's identity before boot.
    pub fn auxiliary_storage_seed_path(&self) -> PathBuf {
        self.directory.join(FILES[2])
    }

    /// Recheck the receipt and filesystem stamps without rehashing disk bytes.
    /// A modified template fails closed; no automatic replacement of it occurs.
    pub fn validate_cached(&self) -> Result<()> {
        ensure!(
            fs::symlink_metadata(&self.directory)?.is_dir(),
            "template directory is missing or invalid"
        );
        private_directory(&self.directory)?;
        let bytes = read_small(
            &self.directory.join("manifest.json"),
            super::MAX_MANIFEST_BYTES,
        )?;
        verify_bytes(&bytes, &self.manifest_sha256)?;
        let receipt: Receipt = serde_json::from_slice(&read_small(
            &self.directory.join("receipt.json"),
            16 * 1024,
        )?)?;
        ensure!(
            receipt.schema_version == 1 && receipt.manifest_sha256 == self.manifest_sha256,
            "template receipt does not match release"
        );
        let expected_sizes = [
            self.manifest.prepared_image.size_bytes,
            self.manifest.platform.hardware_model.size_bytes,
            self.manifest.platform.auxiliary_storage_seed.size_bytes,
        ];
        for ((name, expected), size) in FILES.iter().zip(&receipt.files).zip(expected_sizes) {
            let actual = Stamp::read(&self.directory.join(name))?;
            ensure!(
                &actual == expected && actual.size == size,
                "cached template changed: {name}"
            );
        }
        Ok(())
    }

    pub(super) fn load(directory: PathBuf, key: &str, manifest: ReleaseManifest) -> Result<Self> {
        let ready = Self {
            directory,
            manifest_sha256: key.into(),
            manifest,
        };
        ready.validate_cached()?;
        Ok(ready)
    }

    /// Create one private writable APFS clone at a new absolute path. The parent
    /// must be caller-owned and private, on the same clone-capable filesystem.
    /// Fails on existing destinations or unsupported filesystems/platforms;
    /// never silently falls back to a full disk copy. No identity sidecars are
    /// copied. This bounded filesystem operation does not boot a Machine.
    pub fn clone_disk(&self, destination: &Path) -> Result<()> {
        self.validate_cached()?;
        ensure!(
            destination.is_absolute() && destination.file_name().is_some(),
            "clone destination must be an absolute file path"
        );
        let cache_root = self
            .directory
            .parent()
            .and_then(Path::parent)
            .context("template is missing cache ancestry")?;
        // Reject traversal before checking containment; private_directory rejects
        // symlink ancestry but lexical parent components can disguise a cache path.
        ensure!(
            !destination
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir)),
            "clone destination must not contain parent traversal"
        );
        ensure!(
            !destination.starts_with(cache_root),
            "Machine disks must be outside the bootstrap cache"
        );
        let parent = destination
            .parent()
            .context("clone destination needs a parent")?;
        ensure!(
            parent.is_dir(),
            "create the private Machine directory first"
        );
        private_directory(parent)?;
        let staging = tempfile::tempdir_in(parent)?;
        let cloned = staging.path().join("disk.img");
        clone_file(&self.directory.join(FILES[0]), &cloned)?;
        fs::set_permissions(&cloned, fs::Permissions::from_mode(0o600))?;
        File::open(&cloned)?.sync_all()?;
        // Hard-link only the newly cloned inode to publish without replacement.
        fs::hard_link(&cloned, destination)
            .context("publish private disk without replacing destination")?;
        fs::remove_file(&cloned)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

pub(super) fn open_regular(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    ensure!(
        file.metadata()?.is_file(),
        "expected regular bootstrap file"
    );
    Ok(file)
}

pub(super) fn read_small(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    open_regular(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= limit,
        "bootstrap metadata exceeds size limit"
    );
    Ok(bytes)
}

pub(super) fn verify_bytes(bytes: &[u8], expected: &str) -> Result<()> {
    ensure!(
        format!("{:x}", Sha256::digest(bytes)) == expected,
        "bootstrap metadata digest mismatch"
    );
    Ok(())
}

pub(super) fn remove_stale_stage(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            private_directory(path)?;
            fs::remove_dir_all(path).context("remove interrupted template staging")?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).context("inspect template staging"),
    }
    Ok(())
}

struct Stage(PathBuf);
impl Drop for Stage {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(%error, "unable to discard bootstrap staging");
            }
        }
    }
}

pub(super) fn build(
    staging: &Path,
    destination: PathBuf,
    key: &str,
    manifest: ReleaseManifest,
    bytes: &[u8],
    inputs: &[PathBuf],
    mut progress: impl FnMut(Progress) -> Result<()>,
) -> Result<PreparedTemplate> {
    private_directory(staging)?;
    let _stage = Stage(staging.to_owned());
    let (hardware, auxiliary) = if let Some(local) = &manifest.local_image {
        let [image, hardware, auxiliary] = inputs else {
            anyhow::bail!("missing local image inputs")
        };
        let metadata = open_regular(image)?.metadata()?;
        ensure!(
            metadata.len() == local.size_bytes && metadata.nlink() == 1,
            "invalid local image input"
        );
        let disk = staging.join(FILES[0]);
        clone_file(image, &disk)?;
        let mut input = open_regular(&disk)?;
        let mut hash = Sha256::new();
        let mut buffer = vec![0; 4 * 1024 * 1024];
        let mut completed = 0;
        loop {
            progress(Progress::Artifact {
                component: Component::LocalImage,
                progress: crate::artifact_cache::Progress {
                    phase: crate::artifact_cache::Phase::VerifyingCache,
                    completed,
                    total: local.size_bytes,
                },
            })?;
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            completed += count as u64;
            ensure!(
                completed <= local.size_bytes,
                "local image exceeded size pin"
            );
            hash.update(&buffer[..count]);
        }
        ensure!(
            completed == local.size_bytes && format!("{:x}", hash.finalize()) == local.sha256,
            "local image checksum mismatch"
        );
        (hardware, auxiliary)
    } else {
        let [base, patch, hardware, auxiliary] = inputs else {
            anyhow::bail!("missing delta inputs")
        };
        let expected = manifest.base.as_ref().context("missing base pin")?;
        let info = image_delta::inspect(patch)?;
        let hex = |digest: &[u8]| {
            digest
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        };
        ensure!(
            info.base_size == expected.size_bytes
                && hex(&info.base_sha256) == expected.sha256
                && info.target_size == manifest.prepared_image.size_bytes
                && hex(&info.target_sha256) == manifest.prepared_image.sha256,
            "delta header does not match pinned base/output"
        );
        image_delta::apply(base, patch, &staging.join(FILES[0]), |p| {
            progress(Progress::PreparingImage { progress: p })
        })?;
        (hardware, auxiliary)
    };
    for (source, name, artifact, component) in [
        (
            hardware,
            FILES[1],
            &manifest.platform.hardware_model,
            Component::HardwareModel,
        ),
        (
            auxiliary,
            FILES[2],
            &manifest.platform.auxiliary_storage_seed,
            Component::AuxiliaryStorageSeed,
        ),
    ] {
        copy_verified(
            source,
            &staging.join(name),
            artifact,
            component,
            &mut progress,
        )?;
    }
    for name in FILES {
        fs::set_permissions(staging.join(name), fs::Permissions::from_mode(0o400))?;
        File::open(staging.join(name))?.sync_all()?;
    }
    let receipt = Receipt {
        schema_version: 1,
        manifest_sha256: key.into(),
        files: [
            Stamp::read(&staging.join(FILES[0]))?,
            Stamp::read(&staging.join(FILES[1]))?,
            Stamp::read(&staging.join(FILES[2]))?,
        ],
    };
    write_new(&staging.join("manifest.json"), bytes)?;
    write_new(
        &staging.join("receipt.json"),
        &serde_json::to_vec(&receipt)?,
    )?;
    File::open(staging)?.sync_all()?;
    progress(Progress::PublishingTemplate)?;
    publish_directory(staging, &destination)?;
    File::open(
        destination
            .parent()
            .context("template directory needs parent")?,
    )?
    .sync_all()?;
    PreparedTemplate::load(destination, key, manifest)
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn copy_verified(
    source: &Path,
    destination: &Path,
    artifact: &Artifact,
    component: Component,
    progress: &mut impl FnMut(Progress) -> Result<()>,
) -> Result<()> {
    let mut input = open_regular(source)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    let mut done = 0;
    loop {
        progress(Progress::PreparingPlatform {
            component,
            completed: done,
            total: artifact.size_bytes,
        })?;
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        done += n as u64;
        ensure!(
            done <= artifact.size_bytes,
            "platform artifact exceeded size pin"
        );
        hash.update(&buffer[..n]);
        output.write_all(&buffer[..n])?;
    }
    ensure!(
        done == artifact.size_bytes && format!("{:x}", hash.finalize()) == artifact.sha256,
        "platform artifact changed during preparation"
    );
    output.sync_all()?;
    Ok(())
}

fn publish_directory(source: &Path, destination: &Path) -> Result<()> {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let source = CString::new(source.as_os_str().as_bytes())?;
        let destination = CString::new(destination.as_os_str().as_bytes())?;
        // SAFETY: paths are live NUL-terminated strings. Exclusive rename
        // atomically publishes our owned directory and never replaces a target.
        #[allow(unsafe_code)]
        let result = unsafe {
            #[cfg(target_os = "macos")]
            {
                libc::renameatx_np(
                    libc::AT_FDCWD,
                    source.as_ptr(),
                    libc::AT_FDCWD,
                    destination.as_ptr(),
                    libc::RENAME_EXCL,
                )
            }
            #[cfg(target_os = "linux")]
            {
                libc::renameat2(
                    libc::AT_FDCWD,
                    source.as_ptr(),
                    libc::AT_FDCWD,
                    destination.as_ptr(),
                    libc::RENAME_NOREPLACE,
                )
            }
        };
        ensure!(
            result == 0,
            "publish template: {}",
            std::io::Error::last_os_error()
        );
        Ok(())
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (source, destination);
        anyhow::bail!("atomic template publication unsupported on this host")
    }
}

fn clone_file(source: &Path, destination: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let source = CString::new(source.as_os_str().as_bytes())?;
        let destination = CString::new(destination.as_os_str().as_bytes())?;
        // SAFETY: paths are live NUL-terminated strings. The source is a
        // validated immutable file and destination is new private staging.
        // clonefile creates a separate COW inode, never a hard link to source.
        #[allow(unsafe_code)]
        let result = unsafe { libc::clonefile(source.as_ptr(), destination.as_ptr(), 0) };
        ensure!(
            result == 0,
            "APFS clone failed (no full-copy fallback): {}",
            std::io::Error::last_os_error()
        );
        Ok(())
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (source, destination);
        anyhow::bail!("native macOS disk cloning requires a macOS host")
    }
}
