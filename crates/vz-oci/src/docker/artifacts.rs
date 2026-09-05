use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flate2::read::GzDecoder;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const VERSION_FILE: &str = "version.json";
const DOCKER_ARTIFACT_SUBDIR: &str = ".vz/docker";
const DOCKER_DATA_DISK: &str = "data.img";
const DOCKER_DATA_DISK_SIZE_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const DOCKER_PLATFORM: &str = "linux/arm64";
const MAX_ARCHIVE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_BINARY_BYTES: u64 = 128 * 1024 * 1024;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(120);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const INSTALL_LOCK_TIMEOUT: Duration = Duration::from_secs(60);
const DOCKER_ARCHIVE_SHA256_LINUX_ARM64: &str =
    "43d143448adf2c2787704e7d7704fd6d62d367a54c5edaef0a3f75509cb0938d";

/// Pinned Docker Engine static release version.
pub const DOCKER_ENGINE_VERSION: &str = "29.7.2";

/// Daemon-side binaries installed from Docker's static archive.
///
/// Keep this list intentionally narrow. In particular, the archive's `runc`,
/// `docker`, and `ctr` entries must never be installed: the host Docker CLI is
/// the facade client and youki is the guest's sole OCI runtime.
const REQUIRED_BINARIES: [&str; 5] = [
    "containerd",
    "containerd-shim-runc-v2",
    "docker-init",
    "docker-proxy",
    "dockerd",
];

const PINNED_BINARY_DIGESTS: [(&str, &str); 5] = [
    (
        "containerd",
        "9afba5d84c3de5cba841aab3645efec2f131ddb0b167682727c2af560d59dc33",
    ),
    (
        "containerd-shim-runc-v2",
        "5068a6b0f28fb306204184109b0235c6c49f898cfc9f5fd5a1453935ceb9d6b7",
    ),
    (
        "docker-init",
        "ab2244312e069c6c97bece2437f61fb01db474eae5b0f1ce006162e212eae5b0",
    ),
    (
        "docker-proxy",
        "ad32d5e0b2e9807f71b67f711d7c5ea97114fa1c68c86892c08d0459f3348fde",
    ),
    (
        "dockerd",
        "27898f395958b5dfcc44a7ce7982e73ca9bf10229bea41c6d404991b2f74b069",
    ),
];

/// Verified immutable daemon binaries, without any Docker data disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DockerBinaries {
    /// Directory containing only the verified allowlisted daemon executables.
    pub bin_dir: PathBuf,
    /// Pinned Engine release associated with the verified metadata.
    pub version: String,
}

impl DockerBinaries {
    /// Path to the verified Docker daemon.
    pub fn dockerd_path(&self) -> PathBuf {
        self.bin_dir.join("dockerd")
    }

    /// Path to the verified containerd daemon.
    pub fn containerd_path(&self) -> PathBuf {
        self.bin_dir.join("containerd")
    }
}

/// Legacy Docker facade artifact locations and metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DockerArtifacts {
    /// Directory containing the allowlisted daemon-side binaries.
    pub bin_dir: PathBuf,
    /// Sparse persistent disk attached to the Docker facade VM.
    pub data_disk_path: PathBuf,
    /// Pinned Docker Engine version.
    pub version: String,
}

impl DockerArtifacts {
    /// Path to the installed Docker daemon.
    pub fn dockerd_path(&self) -> PathBuf {
        self.bin_dir.join("dockerd")
    }

    /// Path to the installed containerd daemon.
    pub fn containerd_path(&self) -> PathBuf {
        self.bin_dir.join("containerd")
    }
}

/// Serialized metadata for the installed Docker facade artifact set.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DockerVersionMetadata {
    /// Pinned Docker Engine release.
    pub docker_engine: String,
    /// Artifact target; currently always `linux/arm64`.
    pub platform: String,
    /// Unix timestamp recording when this artifact set was installed.
    pub downloaded_at: u64,
    /// SHA-256 of the official static release archive.
    pub archive_sha256: String,
    /// SHA-256 of each installed binary, keyed by basename.
    pub binaries: BTreeMap<String, String>,
}

/// Docker artifact provisioning failures.
#[derive(Debug, Error)]
pub enum DockerArtifactError {
    /// HOME is unavailable when resolving the persistent artifact directory.
    #[error("HOME environment variable is not set")]
    HomeDirectoryUnavailable,

    /// The pinned archive could not be downloaded.
    #[error("failed to download Docker archive from {url}: {source}")]
    Download {
        /// Download URL.
        url: String,
        /// HTTP client error.
        source: reqwest::Error,
    },

    /// The pinned archive URL returned a non-success status.
    #[error("Docker archive download from {url} returned HTTP {status}")]
    DownloadStatus {
        /// Download URL.
        url: String,
        /// HTTP response status.
        status: u16,
    },

    /// The response exceeded the maximum accepted static archive size.
    #[error("Docker archive exceeds the {limit_bytes}-byte download limit")]
    ArchiveTooLarge {
        /// Configured maximum response size.
        limit_bytes: u64,
    },

    /// Another process held the artifact installation lock for too long.
    #[error("timed out waiting for Docker artifact install lock at {path}")]
    InstallLockTimeout {
        /// Lock path.
        path: String,
    },

    /// An artifact path was a symlink or unexpected file type.
    #[error("unsafe Docker artifact path {path}: {reason}")]
    UnsafePath {
        /// Rejected path.
        path: String,
        /// Rejection detail.
        reason: String,
    },

    /// An extracted binary did not match its independently pinned digest.
    #[error("Docker artifact checksum mismatch for {binary}: expected {expected}, found {found}")]
    BinaryChecksumMismatch {
        /// Binary basename.
        binary: String,
        /// Independently pinned checksum.
        expected: String,
        /// Calculated checksum.
        found: String,
    },

    /// The downloaded archive did not match the pinned digest.
    #[error("Docker archive checksum mismatch: expected {expected}, found {found}")]
    ArchiveChecksumMismatch {
        /// Pinned checksum.
        expected: String,
        /// Calculated checksum.
        found: String,
    },

    /// A required daemon binary was absent from the archive.
    #[error("Docker archive missing required entry: {entry}")]
    MissingArchiveEntry {
        /// Expected archive entry.
        entry: String,
    },

    /// An extracted artifact is not a static Linux arm64 ELF binary.
    #[error("Docker artifact {binary} is not a static Linux arm64 ELF binary: {reason}")]
    InvalidBinary {
        /// Binary basename.
        binary: String,
        /// Validation detail.
        reason: String,
    },

    /// Installed metadata could not be encoded or decoded.
    #[error("Docker artifact metadata error: {0}")]
    Metadata(#[from] serde_json::Error),

    /// Local artifact filesystem access failed.
    #[error("Docker artifact io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Ensure the pinned Docker daemon artifact set exists under `~/.vz/docker`.
///
/// Cached binaries are hash-checked on every reuse. Any missing, altered, or
/// outdated file causes the complete allowlisted set to be reinstalled through
/// a staging directory. The official archive checksum is checked before the
/// first archive entry is inspected.
pub fn ensure_docker_artifacts() -> Result<DockerArtifacts, DockerArtifactError> {
    let home = std::env::var_os("HOME").ok_or(DockerArtifactError::HomeDirectoryUnavailable)?;
    let docker_dir = PathBuf::from(home).join(DOCKER_ARTIFACT_SUBDIR);
    ensure_docker_artifacts_in_dir_with(&docker_dir, download_archive_bytes)
}

/// Ensure the pinned daemon binaries without creating or inspecting a global
/// data disk. Developer Machines must provision their own private state.
pub fn ensure_docker_binaries() -> Result<DockerBinaries, DockerArtifactError> {
    let user_home_path =
        std::env::var_os("HOME").ok_or(DockerArtifactError::HomeDirectoryUnavailable)?;
    let docker_dir = PathBuf::from(user_home_path).join(DOCKER_ARTIFACT_SUBDIR);
    ensure_docker_binaries_in_dir_with_pins(
        &docker_dir,
        download_archive_bytes,
        DOCKER_ARCHIVE_SHA256_LINUX_ARM64,
        &pinned_binary_digests(),
    )
}

fn ensure_docker_artifacts_in_dir_with<F>(
    docker_dir: &Path,
    download: F,
) -> Result<DockerArtifacts, DockerArtifactError>
where
    F: FnOnce(&str) -> Result<Vec<u8>, DockerArtifactError>,
{
    ensure_docker_artifacts_in_dir_with_pins(
        docker_dir,
        download,
        DOCKER_ARCHIVE_SHA256_LINUX_ARM64,
        &pinned_binary_digests(),
    )
}

fn ensure_docker_artifacts_in_dir_with_pins<F>(
    docker_dir: &Path,
    download: F,
    expected_archive_sha256: &str,
    expected_binary_digests: &BTreeMap<String, String>,
) -> Result<DockerArtifacts, DockerArtifactError>
where
    F: FnOnce(&str) -> Result<Vec<u8>, DockerArtifactError>,
{
    create_private_dir(docker_dir)?;
    let data_disk_path = docker_dir.join(DOCKER_DATA_DISK);
    ensure_sparse_disk_image(&data_disk_path, DOCKER_DATA_DISK_SIZE_BYTES)?;
    let binaries = ensure_docker_binaries_in_dir_with_pins(
        docker_dir,
        download,
        expected_archive_sha256,
        expected_binary_digests,
    )?;
    Ok(DockerArtifacts {
        bin_dir: binaries.bin_dir,
        data_disk_path,
        version: binaries.version,
    })
}

fn ensure_docker_binaries_in_dir_with_pins<F>(
    docker_dir: &Path,
    download: F,
    expected_archive_sha256: &str,
    expected_binary_digests: &BTreeMap<String, String>,
) -> Result<DockerBinaries, DockerArtifactError>
where
    F: FnOnce(&str) -> Result<Vec<u8>, DockerArtifactError>,
{
    create_private_dir(docker_dir)?;

    if let Some(existing) = load_existing_artifacts_with_pins(
        docker_dir,
        expected_archive_sha256,
        expected_binary_digests,
    )? {
        return Ok(existing);
    }

    let _install_lock = InstallLock::acquire(docker_dir, INSTALL_LOCK_TIMEOUT)?;
    if let Some(existing) = load_existing_artifacts_with_pins(
        docker_dir,
        expected_archive_sha256,
        expected_binary_digests,
    )? {
        return Ok(existing);
    }

    let archive_url = docker_archive_url(DOCKER_ENGINE_VERSION);
    let archive_bytes = download(&archive_url)?;
    install_archive(
        docker_dir,
        &archive_bytes,
        expected_archive_sha256,
        expected_binary_digests,
    )
}

fn install_archive(
    docker_dir: &Path,
    archive_bytes: &[u8],
    expected_archive_sha256: &str,
    expected_binary_digests: &BTreeMap<String, String>,
) -> Result<DockerBinaries, DockerArtifactError> {
    verify_archive_checksum(archive_bytes, expected_archive_sha256)?;

    let nonce = format!("{}-{}", std::process::id(), current_unix_nanos());
    let staging_dir = docker_dir.join(format!(".staging-{nonce}"));
    let staging_bin_dir = staging_dir.join("bin");
    create_private_dir(&staging_bin_dir)?;

    let install_result = (|| {
        let binary_digests =
            extract_required_binaries(archive_bytes, &staging_bin_dir, expected_binary_digests)?;

        let metadata = DockerVersionMetadata {
            docker_engine: DOCKER_ENGINE_VERSION.to_string(),
            platform: DOCKER_PLATFORM.to_string(),
            downloaded_at: current_unix_secs(),
            archive_sha256: expected_archive_sha256.to_ascii_lowercase(),
            binaries: binary_digests,
        };
        let final_bin_dir = docker_dir.join("bin");
        commit_install(
            &staging_bin_dir,
            &final_bin_dir,
            docker_dir,
            &metadata,
            &nonce,
        )?;

        Ok(DockerBinaries {
            bin_dir: final_bin_dir,
            version: DOCKER_ENGINE_VERSION.to_string(),
        })
    })();

    if staging_dir.exists() {
        let _ = std::fs::remove_dir_all(&staging_dir);
    }
    install_result
}

fn load_existing_artifacts_with_pins(
    docker_dir: &Path,
    expected_archive_sha256: &str,
    expected_binary_digests: &BTreeMap<String, String>,
) -> Result<Option<DockerBinaries>, DockerArtifactError> {
    let metadata_path = docker_dir.join(VERSION_FILE);
    if !regular_non_symlink(&metadata_path)? {
        return Ok(None);
    }
    let metadata = match read_metadata(&metadata_path) {
        Ok(metadata) => metadata,
        Err(DockerArtifactError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(DockerArtifactError::Metadata(_)) => return Ok(None),
        Err(error) => return Err(error),
    };

    if metadata.docker_engine != DOCKER_ENGINE_VERSION
        || metadata.platform != DOCKER_PLATFORM
        || metadata.archive_sha256 != expected_archive_sha256
        || metadata.binaries != *expected_binary_digests
    {
        return Ok(None);
    }

    let bin_dir = docker_dir.join("bin");
    if !private_directory(&bin_dir)? || !inventory_is_exact(&bin_dir)? {
        return Ok(None);
    }
    for binary in REQUIRED_BINARIES {
        let expected_digest = &expected_binary_digests[binary];
        let path = bin_dir.join(binary);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if sha256_hex(&bytes) != *expected_digest {
            return Ok(None);
        }
        if validate_static_linux_arm64_elf(&bytes).is_err() {
            return Ok(None);
        }
    }

    Ok(Some(DockerBinaries {
        bin_dir,
        version: metadata.docker_engine,
    }))
}

fn docker_archive_url(version: &str) -> String {
    format!("https://download.docker.com/linux/static/stable/aarch64/docker-{version}.tgz")
}

fn download_archive_bytes(url: &str) -> Result<Vec<u8>, DockerArtifactError> {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(DOWNLOAD_TIMEOUT)
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            let url = attempt.url();
            if attempt.previous().len() >= 3
                || url.scheme() != "https"
                || url.host_str() != Some("download.docker.com")
            {
                attempt.stop()
            } else {
                attempt.follow()
            }
        }))
        .build()
        .map_err(|source| DockerArtifactError::Download {
            url: url.to_string(),
            source,
        })?;
    let mut response = client
        .get(url)
        .send()
        .map_err(|source| DockerArtifactError::Download {
            url: url.to_string(),
            source,
        })?;
    if !response.status().is_success() {
        return Err(DockerArtifactError::DownloadStatus {
            url: url.to_string(),
            status: response.status().as_u16(),
        });
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_ARCHIVE_BYTES)
    {
        return Err(DockerArtifactError::ArchiveTooLarge {
            limit_bytes: MAX_ARCHIVE_BYTES,
        });
    }
    let mut bytes = Vec::new();
    response
        .by_ref()
        .take(MAX_ARCHIVE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_ARCHIVE_BYTES {
        return Err(DockerArtifactError::ArchiveTooLarge {
            limit_bytes: MAX_ARCHIVE_BYTES,
        });
    }
    Ok(bytes)
}

fn verify_archive_checksum(
    archive_bytes: &[u8],
    expected: &str,
) -> Result<(), DockerArtifactError> {
    let found = sha256_hex(archive_bytes);
    let expected = expected.trim().to_ascii_lowercase();
    if found != expected {
        return Err(DockerArtifactError::ArchiveChecksumMismatch { expected, found });
    }
    Ok(())
}

fn extract_required_binaries(
    archive_bytes: &[u8],
    out_dir: &Path,
    expected_binary_digests: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, DockerArtifactError> {
    let decoder = GzDecoder::new(Cursor::new(archive_bytes));
    let mut archive = tar::Archive::new(decoder);
    let mut digests = BTreeMap::new();

    for entry_result in archive.entries()? {
        let mut entry = entry_result?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let entry_path = entry.path()?;
        let Some(file_name) = entry_path
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_owned)
        else {
            continue;
        };
        if entry_path.parent() != Some(Path::new("docker"))
            || !REQUIRED_BINARIES.contains(&file_name.as_str())
        {
            continue;
        }
        if digests.contains_key(&file_name) {
            return Err(DockerArtifactError::InvalidBinary {
                binary: file_name,
                reason: "duplicate archive entry".to_string(),
            });
        }

        if entry.size() > MAX_BINARY_BYTES {
            return Err(DockerArtifactError::InvalidBinary {
                binary: file_name,
                reason: format!("entry exceeds {MAX_BINARY_BYTES}-byte limit"),
            });
        }

        let mut bytes = Vec::new();
        entry
            .by_ref()
            .take(MAX_BINARY_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_BINARY_BYTES {
            return Err(DockerArtifactError::InvalidBinary {
                binary: file_name,
                reason: format!("entry exceeds {MAX_BINARY_BYTES}-byte limit"),
            });
        }
        validate_static_linux_arm64_elf(&bytes).map_err(|reason| {
            DockerArtifactError::InvalidBinary {
                binary: file_name.clone(),
                reason,
            }
        })?;

        let found_digest = sha256_hex(&bytes);
        let expected_digest = expected_binary_digests.get(&file_name).ok_or_else(|| {
            DockerArtifactError::MissingArchiveEntry {
                entry: format!("pinned digest for docker/{file_name}"),
            }
        })?;
        if &found_digest != expected_digest {
            return Err(DockerArtifactError::BinaryChecksumMismatch {
                binary: file_name,
                expected: expected_digest.clone(),
                found: found_digest,
            });
        }

        let output_path = out_dir.join(&file_name);
        let mut output = private_new_file(&output_path)?;
        output.write_all(&bytes)?;
        output.flush()?;
        mark_executable(&output_path)?;
        digests.insert(file_name, expected_digest.clone());
    }

    for binary in REQUIRED_BINARIES {
        if !digests.contains_key(binary) {
            return Err(DockerArtifactError::MissingArchiveEntry {
                entry: format!("docker/{binary}"),
            });
        }
    }
    Ok(digests)
}

fn validate_static_linux_arm64_elf(bytes: &[u8]) -> Result<(), String> {
    const ELF_HEADER_SIZE: usize = 64;
    const PROGRAM_HEADER_SIZE: usize = 56;
    const EM_AARCH64: u16 = 183;
    const ET_EXEC: u16 = 2;
    const ET_DYN: u16 = 3;
    const PT_DYNAMIC: u32 = 2;
    const PT_INTERP: u32 = 3;

    if bytes.len() < ELF_HEADER_SIZE || &bytes[..4] != b"\x7fELF" {
        return Err("missing ELF64 header".to_string());
    }
    if bytes[4] != 2 || bytes[5] != 1 {
        return Err("expected 64-bit little-endian ELF".to_string());
    }
    if bytes[6] != 1 || u32::from_le_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]) != 1 {
        return Err("unsupported ELF header version".to_string());
    }
    let elf_type = u16::from_le_bytes([bytes[16], bytes[17]]);
    if !matches!(elf_type, ET_EXEC | ET_DYN) {
        return Err("expected executable or static PIE ELF type".to_string());
    }
    if u16::from_le_bytes([bytes[18], bytes[19]]) != EM_AARCH64 {
        return Err("expected AArch64 machine type".to_string());
    }

    let ph_offset = u64::from_le_bytes(
        bytes[32..40]
            .try_into()
            .map_err(|_| "invalid program header offset")?,
    );
    let ph_entry_size = u16::from_le_bytes([bytes[54], bytes[55]]) as usize;
    let ph_count = u16::from_le_bytes([bytes[56], bytes[57]]) as usize;
    if ph_count == 0 {
        return Err("ELF has no program headers".to_string());
    }
    if ph_entry_size != PROGRAM_HEADER_SIZE {
        return Err("invalid program header entry size".to_string());
    }
    let ph_offset = usize::try_from(ph_offset).map_err(|_| "program header offset overflow")?;
    let ph_bytes = ph_entry_size
        .checked_mul(ph_count)
        .and_then(|size| ph_offset.checked_add(size))
        .ok_or("program header table overflow")?;
    if ph_bytes > bytes.len() {
        return Err("truncated program header table".to_string());
    }
    for index in 0..ph_count {
        let offset = ph_offset + index * ph_entry_size;
        let header_type = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .map_err(|_| "truncated program header")?,
        );
        if matches!(header_type, PT_INTERP | PT_DYNAMIC) {
            return Err("ELF contains dynamic linking metadata".to_string());
        }
    }
    Ok(())
}

fn commit_install(
    staged: &Path,
    final_path: &Path,
    parent: &Path,
    metadata: &DockerVersionMetadata,
    nonce: &str,
) -> Result<(), DockerArtifactError> {
    commit_install_with(staged, final_path, parent, nonce, || {
        write_metadata_atomically(parent, metadata, nonce)
    })
}

fn commit_install_with<F>(
    staged: &Path,
    final_path: &Path,
    parent: &Path,
    nonce: &str,
    metadata_commit: F,
) -> Result<(), DockerArtifactError>
where
    F: FnOnce() -> Result<(), DockerArtifactError>,
{
    let backup = parent.join(format!(".bin-backup-{nonce}"));
    let had_existing = final_path.exists();
    if had_existing {
        std::fs::rename(final_path, &backup)?;
    }
    if let Err(error) = std::fs::rename(staged, final_path) {
        if had_existing {
            let _ = std::fs::rename(&backup, final_path);
        }
        return Err(error.into());
    }
    if let Err(error) = metadata_commit() {
        let rejected = parent.join(format!(".bin-rejected-{nonce}"));
        let moved_new = std::fs::rename(final_path, &rejected).is_ok();
        if had_existing {
            let _ = std::fs::rename(&backup, final_path);
        }
        if moved_new {
            let _ = std::fs::remove_dir_all(rejected);
        }
        return Err(error);
    }
    if had_existing {
        std::fs::remove_dir_all(backup)?;
    }
    Ok(())
}

fn write_metadata_atomically(
    docker_dir: &Path,
    metadata: &DockerVersionMetadata,
    nonce: &str,
) -> Result<(), DockerArtifactError> {
    let temp_path = docker_dir.join(format!(".{VERSION_FILE}-{nonce}"));
    let final_path = docker_dir.join(VERSION_FILE);
    let json = serde_json::to_vec_pretty(metadata)?;
    if final_path.exists() && !regular_non_symlink(&final_path)? {
        return Err(DockerArtifactError::UnsafePath {
            path: final_path.display().to_string(),
            reason: "version metadata must be a regular non-symlink file".to_string(),
        });
    }
    let mut file = private_new_file(&temp_path)?;
    file.write_all(&json)?;
    file.flush()?;
    file.sync_all()?;
    std::fs::rename(temp_path, final_path)?;
    Ok(())
}

fn read_metadata(path: &Path) -> Result<DockerVersionMetadata, DockerArtifactError> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn ensure_sparse_disk_image(path: &Path, desired_size: u64) -> Result<(), DockerArtifactError> {
    if path.exists() {
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Err(DockerArtifactError::UnsafePath {
                path: path.display().to_string(),
                reason: "persistent disk must be a regular non-symlink file".to_string(),
            });
        }
        set_private_file_permissions(path)?;
        if metadata.len() >= desired_size {
            return Ok(());
        }
    }
    let file = private_open_file(path, true)?;
    file.set_len(desired_size)?;
    Ok(())
}

fn pinned_binary_digests() -> BTreeMap<String, String> {
    PINNED_BINARY_DIGESTS
        .into_iter()
        .map(|(name, digest)| (name.to_string(), digest.to_string()))
        .collect()
}

fn inventory_is_exact(bin_dir: &Path) -> Result<bool, DockerArtifactError> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(bin_dir)? {
        let entry = entry?;
        let metadata = std::fs::symlink_metadata(entry.path())?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Ok(false);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o111 == 0 {
                return Ok(false);
            }
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return Ok(false);
        };
        names.push(name);
    }
    names.sort();
    Ok(names == REQUIRED_BINARIES.map(str::to_owned))
}

fn regular_non_symlink(path: &Path) -> Result<bool, DockerArtifactError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.file_type().is_file() && !metadata.file_type().is_symlink()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn private_directory(path: &Path) -> Result<bool, DockerArtifactError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn create_private_dir(path: &Path) -> Result<(), DockerArtifactError> {
    std::fs::create_dir_all(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(DockerArtifactError::UnsafePath {
            path: path.display().to_string(),
            reason: "expected a directory, not a symlink or special file".to_string(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn private_new_file(path: &Path) -> Result<File, DockerArtifactError> {
    private_file_options(true).open(path).map_err(Into::into)
}

fn private_open_file(path: &Path, create: bool) -> Result<File, DockerArtifactError> {
    private_file_options(false)
        .create(create)
        .open(path)
        .map_err(Into::into)
}

fn private_file_options(create_new: bool) -> OpenOptions {
    let mut options = OpenOptions::new();
    options.write(true).create_new(create_new);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    options
}

fn set_private_file_permissions(path: &Path) -> Result<(), DockerArtifactError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[derive(Debug)]
struct InstallLock {
    file: File,
}

impl InstallLock {
    fn acquire(parent: &Path, timeout: Duration) -> Result<Self, DockerArtifactError> {
        let path = parent.join(".install.lock");
        let file = private_open_file(&path, true)?;
        if !file.metadata()?.is_file() {
            return Err(DockerArtifactError::UnsafePath {
                path: path.display().to_string(),
                reason: "install lock must be a regular file".to_string(),
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        let started = std::time::Instant::now();
        loop {
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => return Ok(Self { file }),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if started.elapsed() >= timeout {
                        return Err(DockerArtifactError::InstallLockTimeout {
                            path: path.display().to_string(),
                        });
                    }
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

impl Drop for InstallLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

#[cfg(unix)]
fn mark_executable(path: &Path) -> Result<(), DockerArtifactError> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path)?.permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn mark_executable(_path: &Path) -> Result<(), DockerArtifactError> {
    Ok(())
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn current_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tempfile::tempdir;

    use super::*;

    fn fake_static_arm64_elf(marker: u8) -> Vec<u8> {
        let mut bytes = vec![0u8; 64 + 56 + 1];
        bytes[..4].copy_from_slice(b"\x7fELF");
        bytes[4] = 2;
        bytes[5] = 1;
        bytes[6] = 1;
        bytes[16..18].copy_from_slice(&2u16.to_le_bytes());
        bytes[18..20].copy_from_slice(&183u16.to_le_bytes());
        bytes[20..24].copy_from_slice(&1u32.to_le_bytes());
        bytes[32..40].copy_from_slice(&64u64.to_le_bytes());
        bytes[52..54].copy_from_slice(&64u16.to_le_bytes());
        bytes[54..56].copy_from_slice(&56u16.to_le_bytes());
        bytes[56..58].copy_from_slice(&1u16.to_le_bytes());
        bytes[64..68].copy_from_slice(&1u32.to_le_bytes());
        bytes[120] = marker;
        bytes
    }

    fn test_binary_digests() -> BTreeMap<String, String> {
        REQUIRED_BINARIES
            .into_iter()
            .enumerate()
            .map(|(index, binary)| {
                (
                    binary.to_string(),
                    sha256_hex(&fake_static_arm64_elf(index as u8)),
                )
            })
            .collect()
    }

    fn append_archive_file(
        archive: &mut tar::Builder<GzEncoder<Vec<u8>>>,
        path: &str,
        bytes: &[u8],
    ) {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o755);
        header.set_cksum();
        archive.append_data(&mut header, path, bytes).unwrap();
    }

    fn build_test_archive(include_runc: bool) -> Vec<u8> {
        let encoder = GzEncoder::new(Vec::new(), Compression::default());
        let mut archive = tar::Builder::new(encoder);
        for (index, binary) in REQUIRED_BINARIES.into_iter().enumerate() {
            append_archive_file(
                &mut archive,
                &format!("docker/{binary}"),
                &fake_static_arm64_elf(index as u8),
            );
        }
        append_archive_file(&mut archive, "docker/docker", &fake_static_arm64_elf(100));
        append_archive_file(&mut archive, "docker/ctr", &fake_static_arm64_elf(101));
        if include_runc {
            append_archive_file(&mut archive, "docker/runc", &fake_static_arm64_elf(102));
        }
        let encoder = archive.into_inner().unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn extracts_only_daemon_allowlist_and_never_runc() {
        let temp = tempdir().unwrap();
        let archive = build_test_archive(true);
        let digest = sha256_hex(&archive);
        let data_disk = temp.path().join(DOCKER_DATA_DISK);
        ensure_sparse_disk_image(&data_disk, 1024).unwrap();

        let artifacts =
            install_archive(temp.path(), &archive, &digest, &test_binary_digests()).unwrap();

        let installed: Vec<String> = std::fs::read_dir(&artifacts.bin_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(installed.len(), REQUIRED_BINARIES.len());
        for binary in REQUIRED_BINARIES {
            assert!(artifacts.bin_dir.join(binary).is_file());
        }
        assert!(!artifacts.bin_dir.join("runc").exists());
        assert!(!artifacts.bin_dir.join("docker").exists());
        assert!(!artifacts.bin_dir.join("ctr").exists());
    }

    #[test]
    fn archive_checksum_is_verified_before_staging_or_extraction() {
        let temp = tempdir().unwrap();
        let archive = build_test_archive(false);

        let error =
            install_archive(temp.path(), &archive, "00", &test_binary_digests()).unwrap_err();

        assert!(matches!(
            error,
            DockerArtifactError::ArchiveChecksumMismatch { .. }
        ));
        assert!(!temp.path().join("bin").exists());
        assert!(std::fs::read_dir(temp.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".staging")
        }));
    }

    #[test]
    fn cached_binary_digests_are_verified_before_reuse() {
        let temp = tempdir().unwrap();
        let archive = build_test_archive(false);
        let digest = sha256_hex(&archive);
        let data_disk = temp.path().join(DOCKER_DATA_DISK);
        ensure_sparse_disk_image(&data_disk, 1024).unwrap();
        let test_digests = test_binary_digests();
        let artifacts = install_archive(temp.path(), &archive, &digest, &test_digests).unwrap();

        assert!(
            load_existing_artifacts_with_pins(temp.path(), &digest, &test_digests,)
                .unwrap()
                .is_some()
        );
        std::fs::write(artifacts.dockerd_path(), b"tampered").unwrap();
        assert!(
            load_existing_artifacts_with_pins(temp.path(), &digest, &test_digests,)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn binary_only_install_and_cache_never_create_global_data_disk() {
        let temp = tempdir().unwrap();
        let archive = build_test_archive(false);
        let archive_digest = sha256_hex(&archive);
        let binary_digests = test_binary_digests();
        let installed = ensure_docker_binaries_in_dir_with_pins(
            temp.path(),
            |_| Ok(archive),
            &archive_digest,
            &binary_digests,
        )
        .unwrap();
        assert!(!temp.path().join(DOCKER_DATA_DISK).exists());
        assert!(installed.dockerd_path().is_file());
        assert!(installed.containerd_path().is_file());
        let reused = ensure_docker_binaries_in_dir_with_pins(
            temp.path(),
            |_| panic!("verified binaries must not redownload"),
            &archive_digest,
            &binary_digests,
        )
        .unwrap();
        assert_eq!(reused, installed);
        assert!(!temp.path().join(DOCKER_DATA_DISK).exists());

        // A legacy disk can exist, but is neither resized nor validated nor
        // written as a side effect of requesting immutable binaries.
        let disk = temp.path().join(DOCKER_DATA_DISK);
        std::fs::write(&disk, b"legacy private data sentinel").unwrap();
        ensure_docker_binaries_in_dir_with_pins(
            temp.path(),
            |_| panic!("verified binaries must not redownload"),
            &archive_digest,
            &binary_digests,
        )
        .unwrap();
        assert_eq!(
            std::fs::read(&disk).unwrap(),
            b"legacy private data sentinel"
        );
    }

    #[test]
    fn valid_cache_does_not_download_again() {
        let temp = tempdir().unwrap();
        let archive = build_test_archive(false);
        let archive_digest = sha256_hex(&archive);
        let test_digests = test_binary_digests();
        let data_disk = temp.path().join(DOCKER_DATA_DISK);
        ensure_sparse_disk_image(&data_disk, 1024).unwrap();

        // Use metadata shaped like the pinned artifact after installing the
        // synthetic archive with its own digest.
        let artifacts =
            install_archive(temp.path(), &archive, &archive_digest, &test_digests).unwrap();

        let reused = ensure_docker_artifacts_in_dir_with_pins(
            temp.path(),
            |_| panic!("valid artifact cache must not download"),
            &archive_digest,
            &test_digests,
        )
        .unwrap();
        assert_eq!(reused.bin_dir, artifacts.bin_dir);
    }

    #[test]
    fn static_arm64_validator_rejects_dynamic_interpreter() {
        let mut bytes = fake_static_arm64_elf(0);
        bytes[64..68].copy_from_slice(&3u32.to_le_bytes());

        let error = validate_static_linux_arm64_elf(&bytes).unwrap_err();
        assert!(error.contains("dynamic linking metadata"));
    }

    #[test]
    fn persistent_data_disk_is_sparse_and_never_part_of_bin_directory() {
        let temp = tempdir().unwrap();
        let disk = temp.path().join(DOCKER_DATA_DISK);
        ensure_sparse_disk_image(&disk, 8 * 1024 * 1024).unwrap();
        assert_eq!(std::fs::metadata(&disk).unwrap().len(), 8 * 1024 * 1024);
        assert_eq!(disk.parent(), Some(temp.path()));
        assert_ne!(disk.parent(), Some(temp.path().join("bin").as_path()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&disk).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn cache_rejects_extra_or_non_executable_inventory() {
        let temp = tempdir().unwrap();
        let archive = build_test_archive(false);
        let archive_digest = sha256_hex(&archive);
        let binary_digests = test_binary_digests();
        let disk = temp.path().join(DOCKER_DATA_DISK);
        ensure_sparse_disk_image(&disk, 1024).unwrap();
        let artifacts =
            install_archive(temp.path(), &archive, &archive_digest, &binary_digests).unwrap();

        std::fs::write(artifacts.bin_dir.join("crun"), b"extra runtime").unwrap();
        assert!(
            load_existing_artifacts_with_pins(temp.path(), &archive_digest, &binary_digests,)
                .unwrap()
                .is_none()
        );
        std::fs::remove_file(artifacts.bin_dir.join("crun")).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let dockerd = artifacts.dockerd_path();
            std::fs::set_permissions(&dockerd, std::fs::Permissions::from_mode(0o600)).unwrap();
            assert!(
                load_existing_artifacts_with_pins(temp.path(), &archive_digest, &binary_digests,)
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn metadata_cannot_repin_a_tampered_binary() {
        let temp = tempdir().unwrap();
        let archive = build_test_archive(false);
        let archive_digest = sha256_hex(&archive);
        let binary_digests = test_binary_digests();
        let disk = temp.path().join(DOCKER_DATA_DISK);
        ensure_sparse_disk_image(&disk, 1024).unwrap();
        let artifacts =
            install_archive(temp.path(), &archive, &archive_digest, &binary_digests).unwrap();
        let tampered = fake_static_arm64_elf(200);
        std::fs::write(artifacts.dockerd_path(), &tampered).unwrap();
        mark_executable(&artifacts.dockerd_path()).unwrap();
        let mut metadata = read_metadata(&temp.path().join(VERSION_FILE)).unwrap();
        metadata
            .binaries
            .insert("dockerd".to_string(), sha256_hex(&tampered));
        write_metadata_atomically(temp.path(), &metadata, "repin").unwrap();

        assert!(
            load_existing_artifacts_with_pins(temp.path(), &archive_digest, &binary_digests,)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn failed_metadata_commit_rolls_back_previous_binary_generation() {
        let temp = tempdir().unwrap();
        let old = temp.path().join("bin");
        let staged = temp.path().join("staged");
        create_private_dir(&old).unwrap();
        create_private_dir(&staged).unwrap();
        std::fs::write(old.join("marker"), b"old").unwrap();
        std::fs::write(staged.join("marker"), b"new").unwrap();

        let error = commit_install_with(&staged, &old, temp.path(), "rollback", || {
            Err(DockerArtifactError::Io(std::io::Error::other(
                "metadata commit failed",
            )))
        })
        .unwrap_err();

        assert!(error.to_string().contains("metadata commit failed"));
        assert_eq!(std::fs::read(old.join("marker")).unwrap(), b"old");
    }

    #[test]
    fn installer_rechecks_cache_after_acquiring_lock() {
        let temp = tempdir().unwrap();
        let archive = build_test_archive(false);
        let archive_digest = sha256_hex(&archive);
        let binary_digests = test_binary_digests();
        create_private_dir(temp.path()).unwrap();
        let disk = temp.path().join(DOCKER_DATA_DISK);
        ensure_sparse_disk_image(&disk, 1024).unwrap();
        let lock = InstallLock::acquire(temp.path(), Duration::from_secs(1)).unwrap();

        let root = temp.path().to_path_buf();
        let thread_digest = archive_digest.clone();
        let thread_binary_digests = binary_digests.clone();
        let waiter = std::thread::spawn(move || {
            ensure_docker_artifacts_in_dir_with_pins(
                &root,
                |_| panic!("post-lock cache recheck must skip download"),
                &thread_digest,
                &thread_binary_digests,
            )
        });

        std::thread::sleep(Duration::from_millis(75));
        install_archive(temp.path(), &archive, &archive_digest, &binary_digests).unwrap();
        drop(lock);
        assert!(waiter.join().unwrap().is_ok());
    }

    #[test]
    fn advisory_install_lock_times_out_and_kernel_releases_on_drop() {
        let temp = tempdir().unwrap();
        create_private_dir(temp.path()).unwrap();
        let lock_path = temp.path().join(".install.lock");
        let first = InstallLock::acquire(temp.path(), Duration::from_secs(1)).unwrap();

        let blocked = InstallLock::acquire(temp.path(), Duration::from_millis(30)).unwrap_err();
        assert!(matches!(
            blocked,
            DockerArtifactError::InstallLockTimeout { .. }
        ));
        drop(first);

        let second = InstallLock::acquire(temp.path(), Duration::from_secs(1)).unwrap();
        assert!(lock_path.is_file());
        drop(second);
        assert!(
            lock_path.is_file(),
            "advisory lock inode must remain stable while the kernel releases ownership"
        );
    }

    #[cfg(unix)]
    #[test]
    fn persistent_data_disk_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let temp = tempdir().unwrap();
        let target = temp.path().join("target");
        std::fs::write(&target, b"not a disk").unwrap();
        let disk = temp.path().join(DOCKER_DATA_DISK);
        symlink(&target, &disk).unwrap();
        let error = ensure_sparse_disk_image(&disk, 1024).unwrap_err();
        assert!(matches!(error, DockerArtifactError::UnsafePath { .. }));
    }

    #[test]
    #[ignore = "requires VZ_DOCKER_TEST_ARCHIVE pointing at the official pinned arm64 archive"]
    fn official_pinned_archive_matches_all_independent_digests() {
        let archive_path = std::env::var_os("VZ_DOCKER_TEST_ARCHIVE")
            .map(PathBuf::from)
            .unwrap();
        let archive = std::fs::read(archive_path).unwrap();
        let temp = tempdir().unwrap();
        let disk = temp.path().join(DOCKER_DATA_DISK);
        ensure_sparse_disk_image(&disk, 1024).unwrap();

        let artifacts = install_archive(
            temp.path(),
            &archive,
            DOCKER_ARCHIVE_SHA256_LINUX_ARM64,
            &pinned_binary_digests(),
        )
        .unwrap();

        assert!(inventory_is_exact(&artifacts.bin_dir).unwrap());
        assert!(!artifacts.bin_dir.join("runc").exists());
    }
}
