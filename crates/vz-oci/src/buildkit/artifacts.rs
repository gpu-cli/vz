use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const VERSION_FILE: &str = "version.json";
const MANIFEST_FILE: &str = "manifest.json";
const BUILDKITD_BINARY: &str = "buildkitd";
const BUILDCTL_BINARY: &str = "buildctl";
const BUILDKIT_ARTIFACT_SUBDIR: &str = ".vz/buildkit";
const BUILDKIT_PLATFORM: &str = "linux/arm64";
const BUILDKIT_SOURCE_COMMIT: &str = "3637d1b15a13fc3cdd0c16fcf3be0845ae68f53d";
const BUILDKIT_ARTIFACT_RELEASE_TAG: &str = "v0.3.21";
const BUILDKIT_ARCHIVE_SHA256_LINUX_ARM64: &str =
    "a611138d4675290f96b83b440156b16224626bd6b3fea55cba9c7f3ea2e06c09";
const MAX_ARCHIVE_BYTES: u64 = 160 * 1024 * 1024;
const MAX_BINARY_BYTES: u64 = 96 * 1024 * 1024;
const MAX_MANIFEST_BYTES: u64 = 16 * 1024;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(120);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const INSTALL_LOCK_TIMEOUT: Duration = Duration::from_secs(60);
const LOCAL_ARCHIVE_ENV: &str = "VZ_BUILDKIT_ARTIFACT_ARCHIVE";
const LOCAL_ARCHIVE_SHA256_ENV: &str = "VZ_BUILDKIT_ARTIFACT_SHA256";

/// Pinned BuildKit release version.
pub const BUILDKIT_VERSION: &str = "0.19.0";

/// Runtime-free BuildKit artifact layout understood by this installer.
pub const BUILDKIT_ARTIFACT_LAYOUT: u32 = 2;

const REQUIRED_BINARIES: [&str; 2] = [BUILDCTL_BINARY, BUILDKITD_BINARY];
const REQUIRED_ARCHIVE_ENTRIES: [&str; 3] = ["manifest.json", "bin/buildctl", "bin/buildkitd"];
const PINNED_BINARY_DIGESTS: [(&str, &str); 2] = [
    (
        BUILDCTL_BINARY,
        "725c7416fc7212805d301194df723939fc9e51f84157d7bcfba6fd2f1ee319c9",
    ),
    (
        BUILDKITD_BINARY,
        "38f1e204552fb19f661eb1da18f537fd2d4d5d790d09619d379e2dd0a034ffaf",
    ),
];

/// Installed BuildKit artifact locations and metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildkitArtifacts {
    pub bin_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub version: String,
}

impl BuildkitArtifacts {
    pub fn buildkitd_path(&self) -> PathBuf {
        self.bin_dir.join(BUILDKITD_BINARY)
    }

    pub fn buildctl_path(&self) -> PathBuf {
        self.bin_dir.join(BUILDCTL_BINARY)
    }
}

/// Serialized metadata for installed BuildKit artifacts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildkitVersionMetadata {
    pub buildkit: String,
    pub layout: u32,
    pub platform: String,
    pub source_commit: String,
    pub downloaded_at: u64,
    pub archive_sha256: String,
    pub binaries: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BuildkitArchiveManifest {
    buildkit: String,
    layout: u32,
    platform: String,
    source_commit: String,
    binaries: BTreeMap<String, String>,
}

impl BuildkitArchiveManifest {
    fn pinned() -> Self {
        Self {
            buildkit: BUILDKIT_VERSION.to_string(),
            layout: BUILDKIT_ARTIFACT_LAYOUT,
            platform: BUILDKIT_PLATFORM.to_string(),
            source_commit: BUILDKIT_SOURCE_COMMIT.to_string(),
            binaries: pinned_binary_digests(),
        }
    }
}

#[derive(Debug, Error)]
pub enum BuildkitError {
    #[error("HOME environment variable is not set")]
    HomeDirectoryUnavailable,
    #[error("failed to download BuildKit archive from {url}: {source}")]
    Download { url: String, source: reqwest::Error },
    #[error("BuildKit archive download from {url} returned HTTP {status}")]
    DownloadStatus { url: String, status: u16 },
    #[error("BuildKit archive exceeds the {limit_bytes}-byte size limit")]
    ArchiveTooLarge { limit_bytes: u64 },
    #[error("{LOCAL_ARCHIVE_SHA256_ENV} must be set when {LOCAL_ARCHIVE_ENV} is used")]
    LocalArchiveChecksumRequired,
    #[error("BuildKit archive checksum mismatch: expected {expected}, found {found}")]
    ArchiveChecksumMismatch { expected: String, found: String },
    #[error("BuildKit archive missing required entry: {entry}")]
    MissingArchiveEntry { entry: String },
    #[error("BuildKit archive contains unexpected entry: {entry}")]
    UnexpectedArchiveEntry { entry: String },
    #[error("BuildKit archive contains duplicate entry: {entry}")]
    DuplicateArchiveEntry { entry: String },
    #[error("BuildKit archive manifest does not match the pinned artifact contract")]
    ManifestMismatch,
    #[error("BuildKit binary checksum mismatch for {binary}: expected {expected}, found {found}")]
    BinaryChecksumMismatch {
        binary: String,
        expected: String,
        found: String,
    },
    #[error("BuildKit artifact {binary} is not a static Linux arm64 ELF binary: {reason}")]
    InvalidBinary { binary: String, reason: String },
    #[error("timed out waiting for BuildKit artifact install lock at {path}")]
    InstallLockTimeout { path: String },
    #[error("unsafe BuildKit artifact path {path}: {reason}")]
    UnsafePath { path: String, reason: String },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("metadata serialization error: {0}")]
    Metadata(#[from] serde_json::Error),
}

/// Ensure pinned, runtime-free BuildKit artifacts are present in `~/.vz/buildkit`.
///
/// Set `VZ_BUILDKIT_ARTIFACT_ARCHIVE` and `VZ_BUILDKIT_ARTIFACT_SHA256` together
/// to consume a locally built package before its release asset is published.
pub fn ensure_buildkit_artifacts() -> Result<BuildkitArtifacts, BuildkitError> {
    let home = std::env::var_os("HOME").ok_or(BuildkitError::HomeDirectoryUnavailable)?;
    let buildkit_dir = PathBuf::from(home).join(BUILDKIT_ARTIFACT_SUBDIR);
    ensure_buildkit_artifacts_in_dir(&buildkit_dir)
}

fn ensure_buildkit_artifacts_in_dir(
    buildkit_dir: &Path,
) -> Result<BuildkitArtifacts, BuildkitError> {
    let expected_archive_sha256 = expected_archive_sha256()?;
    create_private_dir(buildkit_dir)?;
    let _install_lock = InstallLock::acquire(buildkit_dir, INSTALL_LOCK_TIMEOUT)?;
    recover_stale_install_generations(buildkit_dir)?;
    if let Some(existing) = load_existing_artifacts(
        buildkit_dir,
        &expected_archive_sha256,
        &pinned_binary_digests(),
    )? {
        discard_stale_rollback_generations(buildkit_dir)?;
        return Ok(existing);
    }
    let archive_bytes = load_archive_bytes()?;
    let installed = install_archive(
        buildkit_dir,
        &archive_bytes,
        &expected_archive_sha256,
        &BuildkitArchiveManifest::pinned(),
    )?;
    discard_stale_rollback_generations(buildkit_dir)?;
    Ok(installed)
}

fn expected_archive_sha256() -> Result<String, BuildkitError> {
    if std::env::var_os(LOCAL_ARCHIVE_ENV).is_some() {
        return std::env::var(LOCAL_ARCHIVE_SHA256_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.trim().to_ascii_lowercase())
            .ok_or(BuildkitError::LocalArchiveChecksumRequired);
    }
    Ok(BUILDKIT_ARCHIVE_SHA256_LINUX_ARM64.to_string())
}

fn load_archive_bytes() -> Result<Vec<u8>, BuildkitError> {
    if let Some(path) = std::env::var_os(LOCAL_ARCHIVE_ENV) {
        return read_local_archive(Path::new(&path));
    }
    download_archive_bytes(&buildkit_archive_url())
}

fn install_archive(
    buildkit_dir: &Path,
    archive_bytes: &[u8],
    expected_archive_sha256: &str,
    expected_manifest: &BuildkitArchiveManifest,
) -> Result<BuildkitArtifacts, BuildkitError> {
    verify_archive_checksum(archive_bytes, expected_archive_sha256)?;
    create_private_dir(buildkit_dir)?;
    let cache_dir = buildkit_dir.join("cache");
    create_private_dir(&cache_dir)?;

    let nonce = format!("{}-{}", std::process::id(), current_unix_nanos());
    let staging_dir = buildkit_dir.join(format!(".staging-{nonce}"));
    let staging_bin_dir = staging_dir.join("bin");
    create_private_dir(&staging_bin_dir)?;
    let install_result = (|| {
        let binary_digests = extract_archive(archive_bytes, &staging_bin_dir, expected_manifest)?;
        let metadata = BuildkitVersionMetadata {
            buildkit: expected_manifest.buildkit.clone(),
            layout: expected_manifest.layout,
            platform: expected_manifest.platform.clone(),
            source_commit: expected_manifest.source_commit.clone(),
            downloaded_at: current_unix_secs(),
            archive_sha256: expected_archive_sha256.trim().to_ascii_lowercase(),
            binaries: binary_digests,
        };
        let final_bin_dir = buildkit_dir.join("bin");
        commit_install(
            &staging_bin_dir,
            &final_bin_dir,
            buildkit_dir,
            &metadata,
            &nonce,
        )?;
        Ok(BuildkitArtifacts {
            bin_dir: final_bin_dir,
            cache_dir,
            version: BUILDKIT_VERSION.to_string(),
        })
    })();
    if staging_dir.exists() {
        let _ = std::fs::remove_dir_all(&staging_dir);
    }
    install_result
}

fn load_existing_artifacts(
    buildkit_dir: &Path,
    expected_archive_sha256: &str,
    expected_binary_digests: &BTreeMap<String, String>,
) -> Result<Option<BuildkitArtifacts>, BuildkitError> {
    let version_path = buildkit_dir.join(VERSION_FILE);
    if !regular_non_symlink(&version_path)? {
        return Ok(None);
    }
    let metadata = match read_metadata(&version_path) {
        Ok(metadata) => metadata,
        Err(BuildkitError::Metadata(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let pinned = BuildkitArchiveManifest::pinned();
    if metadata.buildkit != pinned.buildkit
        || metadata.layout != pinned.layout
        || metadata.platform != pinned.platform
        || metadata.source_commit != pinned.source_commit
        || metadata.archive_sha256 != expected_archive_sha256
        || metadata.binaries != *expected_binary_digests
    {
        return Ok(None);
    }

    let bin_dir = buildkit_dir.join("bin");
    if !inventory_is_exact(&bin_dir)? {
        return Ok(None);
    }
    for binary in REQUIRED_BINARIES {
        if !cached_binary_matches(&bin_dir.join(binary), &expected_binary_digests[binary])? {
            return Ok(None);
        }
    }
    let cache_dir = buildkit_dir.join("cache");
    create_private_dir(&cache_dir)?;
    Ok(Some(BuildkitArtifacts {
        bin_dir,
        cache_dir,
        version: metadata.buildkit,
    }))
}

fn buildkit_archive_url() -> String {
    format!(
        "https://github.com/gpu-cli/vz/releases/download/{BUILDKIT_ARTIFACT_RELEASE_TAG}/vz-buildkit-v{BUILDKIT_VERSION}-linux-arm64.tar"
    )
}

fn download_archive_bytes(url: &str) -> Result<Vec<u8>, BuildkitError> {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(DOWNLOAD_TIMEOUT)
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            let url = attempt.url();
            if attempt.previous().len() >= 3
                || url.scheme() != "https"
                || !matches!(
                    url.host_str(),
                    Some(
                        "github.com"
                            | "objects.githubusercontent.com"
                            | "release-assets.githubusercontent.com"
                    )
                )
            {
                attempt.stop()
            } else {
                attempt.follow()
            }
        }))
        .build()
        .map_err(|source| BuildkitError::Download {
            url: url.to_string(),
            source,
        })?;
    let mut response = client
        .get(url)
        .send()
        .map_err(|source| BuildkitError::Download {
            url: url.to_string(),
            source,
        })?;
    if !response.status().is_success() {
        return Err(BuildkitError::DownloadStatus {
            url: url.to_string(),
            status: response.status().as_u16(),
        });
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_ARCHIVE_BYTES)
    {
        return Err(BuildkitError::ArchiveTooLarge {
            limit_bytes: MAX_ARCHIVE_BYTES,
        });
    }
    let mut bytes = Vec::new();
    response
        .by_ref()
        .take(MAX_ARCHIVE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_ARCHIVE_BYTES {
        return Err(BuildkitError::ArchiveTooLarge {
            limit_bytes: MAX_ARCHIVE_BYTES,
        });
    }
    Ok(bytes)
}

fn read_local_archive(path: &Path) -> Result<Vec<u8>, BuildkitError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(BuildkitError::UnsafePath {
            path: path.display().to_string(),
            reason: "local archive must be a regular non-symlink file".to_string(),
        });
    }
    if metadata.len() > MAX_ARCHIVE_BYTES {
        return Err(BuildkitError::ArchiveTooLarge {
            limit_bytes: MAX_ARCHIVE_BYTES,
        });
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_ARCHIVE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_ARCHIVE_BYTES {
        return Err(BuildkitError::ArchiveTooLarge {
            limit_bytes: MAX_ARCHIVE_BYTES,
        });
    }
    Ok(bytes)
}

fn verify_archive_checksum(archive_bytes: &[u8], expected: &str) -> Result<(), BuildkitError> {
    let found = sha256_hex(archive_bytes);
    let expected = expected.trim().to_ascii_lowercase();
    if found != expected {
        return Err(BuildkitError::ArchiveChecksumMismatch { expected, found });
    }
    Ok(())
}

fn extract_archive(
    archive_bytes: &[u8],
    out_dir: &Path,
    expected_manifest: &BuildkitArchiveManifest,
) -> Result<BTreeMap<String, String>, BuildkitError> {
    let mut archive = tar::Archive::new(Cursor::new(archive_bytes));
    let mut found_entries = BTreeSet::new();
    let mut manifest = None;
    let mut binary_digests = BTreeMap::new();
    for entry_result in archive.entries()? {
        let mut entry = entry_result?;
        let entry_path = entry.path()?.into_owned();
        let entry_name = entry_path.to_string_lossy().into_owned();
        if !entry.header().entry_type().is_file()
            || !REQUIRED_ARCHIVE_ENTRIES.contains(&entry_name.as_str())
        {
            return Err(BuildkitError::UnexpectedArchiveEntry { entry: entry_name });
        }
        if !found_entries.insert(entry_name.clone()) {
            return Err(BuildkitError::DuplicateArchiveEntry { entry: entry_name });
        }
        if entry_path == Path::new(MANIFEST_FILE) {
            if entry.size() > MAX_MANIFEST_BYTES {
                return Err(BuildkitError::ManifestMismatch);
            }
            let mut bytes = Vec::new();
            entry
                .by_ref()
                .take(MAX_MANIFEST_BYTES + 1)
                .read_to_end(&mut bytes)?;
            manifest = Some(serde_json::from_slice::<BuildkitArchiveManifest>(&bytes)?);
            continue;
        }

        let Some(binary) = entry_path.file_name().and_then(|name| name.to_str()) else {
            return Err(BuildkitError::UnexpectedArchiveEntry { entry: entry_name });
        };
        if entry.size() > MAX_BINARY_BYTES {
            return Err(BuildkitError::ArchiveTooLarge {
                limit_bytes: MAX_BINARY_BYTES,
            });
        }
        let mut bytes = Vec::new();
        entry
            .by_ref()
            .take(MAX_BINARY_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_BINARY_BYTES {
            return Err(BuildkitError::ArchiveTooLarge {
                limit_bytes: MAX_BINARY_BYTES,
            });
        }
        validate_static_linux_arm64_elf(&bytes).map_err(|reason| BuildkitError::InvalidBinary {
            binary: binary.to_string(),
            reason,
        })?;
        let found = sha256_hex(&bytes);
        let expected = expected_manifest.binaries.get(binary).ok_or_else(|| {
            BuildkitError::UnexpectedArchiveEntry {
                entry: entry_name.clone(),
            }
        })?;
        if &found != expected {
            return Err(BuildkitError::BinaryChecksumMismatch {
                binary: binary.to_string(),
                expected: expected.clone(),
                found,
            });
        }
        let output_path = out_dir.join(binary);
        let mut output = private_new_file(&output_path)?;
        output.write_all(&bytes)?;
        output.flush()?;
        mark_executable(&output_path)?;
        binary_digests.insert(binary.to_string(), expected.clone());
    }
    for entry in REQUIRED_ARCHIVE_ENTRIES {
        if !found_entries.contains(entry) {
            return Err(BuildkitError::MissingArchiveEntry {
                entry: entry.to_string(),
            });
        }
    }
    if manifest.as_ref() != Some(expected_manifest) || binary_digests != expected_manifest.binaries
    {
        return Err(BuildkitError::ManifestMismatch);
    }
    Ok(binary_digests)
}

fn commit_install(
    staged: &Path,
    final_path: &Path,
    parent: &Path,
    metadata: &BuildkitVersionMetadata,
    nonce: &str,
) -> Result<(), BuildkitError> {
    let metadata_temp = parent.join(format!(".{VERSION_FILE}-{nonce}"));
    let metadata_final = parent.join(VERSION_FILE);
    if metadata_final.exists() && !regular_non_symlink(&metadata_final)? {
        return Err(BuildkitError::UnsafePath {
            path: metadata_final.display().to_string(),
            reason: "version metadata must be a regular non-symlink file".to_string(),
        });
    }
    let mut metadata_file = private_new_file(&metadata_temp)?;
    metadata_file.write_all(&serde_json::to_vec_pretty(metadata)?)?;
    metadata_file.flush()?;
    metadata_file.sync_all()?;

    let backup = parent.join(format!(".bin-backup-{nonce}"));
    let had_existing = final_path.exists();
    if had_existing {
        std::fs::rename(final_path, &backup)?;
    }
    if let Err(error) = std::fs::rename(staged, final_path) {
        if had_existing {
            let _ = std::fs::rename(&backup, final_path);
        }
        let _ = std::fs::remove_file(metadata_temp);
        return Err(error.into());
    }
    if let Err(error) = std::fs::rename(&metadata_temp, &metadata_final) {
        let rejected = parent.join(format!(".bin-rejected-{nonce}"));
        let moved_new = std::fs::rename(final_path, &rejected).is_ok();
        if had_existing {
            let _ = std::fs::rename(&backup, final_path);
        }
        if moved_new {
            let _ = std::fs::remove_dir_all(rejected);
        }
        let _ = std::fs::remove_file(metadata_temp);
        return Err(error.into());
    }
    if had_existing {
        std::fs::remove_dir_all(backup)?;
    }
    Ok(())
}

fn recover_stale_install_generations(buildkit_dir: &Path) -> Result<(), BuildkitError> {
    let mut rollback_generations = Vec::new();
    for entry in std::fs::read_dir(buildkit_dir)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if name.starts_with(".bin-backup-") {
            rollback_generations.push(entry.path());
            continue;
        }
        if !(name.starts_with(".staging-")
            || name.starts_with(".bin-rejected-")
            || name.starts_with(".version.json-"))
        {
            continue;
        }
        remove_stale_path(&entry.path())?;
    }

    rollback_generations.sort();
    let bin_dir = buildkit_dir.join("bin");
    let bin_exists = match std::fs::symlink_metadata(&bin_dir) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    if !bin_exists && rollback_generations.len() == 1 {
        let rollback = &rollback_generations[0];
        let metadata = std::fs::symlink_metadata(rollback)?;
        if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
            std::fs::rename(rollback, bin_dir)?;
            return Ok(());
        }
    }

    Ok(())
}

fn discard_stale_rollback_generations(buildkit_dir: &Path) -> Result<(), BuildkitError> {
    for entry in std::fs::read_dir(buildkit_dir)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(".bin-backup-"))
        {
            remove_stale_path(&entry.path())?;
        }
    }
    Ok(())
}

fn remove_stale_path(path: &Path) -> Result<(), BuildkitError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        std::fs::remove_dir_all(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

fn cached_binary_matches(path: &Path, expected_sha256: &str) -> Result<bool, BuildkitError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > MAX_BINARY_BYTES {
        return Ok(false);
    }
    if validate_static_linux_arm64_elf_reader(&mut file, metadata.len()).is_err() {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(0))?;

    let mut bounded = file.take(MAX_BINARY_BYTES + 1);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = bounded.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as u64);
        if total > MAX_BINARY_BYTES {
            return Ok(false);
        }
        hasher.update(&buffer[..read]);
    }
    if total != metadata.len() {
        return Ok(false);
    }
    Ok(format!("{:x}", hasher.finalize()) == expected_sha256)
}

fn validate_static_linux_arm64_elf(bytes: &[u8]) -> Result<(), String> {
    validate_static_linux_arm64_elf_reader(&mut Cursor::new(bytes), bytes.len() as u64)
}

fn validate_static_linux_arm64_elf_reader<R: Read + Seek>(
    reader: &mut R,
    binary_len: u64,
) -> Result<(), String> {
    const ELF_HEADER_SIZE: usize = 64;
    const PROGRAM_HEADER_SIZE: usize = 56;
    const EM_AARCH64: u16 = 183;
    const ET_EXEC: u16 = 2;
    const ET_DYN: u16 = 3;
    const PT_DYNAMIC: u32 = 2;
    const PT_INTERP: u32 = 3;

    if binary_len < ELF_HEADER_SIZE as u64 {
        return Err("missing ELF64 header".to_string());
    }
    reader
        .seek(SeekFrom::Start(0))
        .map_err(|error| error.to_string())?;
    let mut bytes = [0_u8; ELF_HEADER_SIZE];
    reader
        .read_exact(&mut bytes)
        .map_err(|_| "missing ELF64 header".to_string())?;
    if &bytes[..4] != b"\x7fELF" {
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
    let ph_bytes = (ph_entry_size as u64)
        .checked_mul(ph_count as u64)
        .and_then(|size| ph_offset.checked_add(size))
        .ok_or("program header table overflow")?;
    if ph_bytes > binary_len {
        return Err("truncated program header table".to_string());
    }
    for index in 0..ph_count {
        let offset = ph_offset + index as u64 * ph_entry_size as u64;
        reader
            .seek(SeekFrom::Start(offset))
            .map_err(|error| error.to_string())?;
        let mut header_type = [0_u8; 4];
        reader
            .read_exact(&mut header_type)
            .map_err(|_| "truncated program header".to_string())?;
        let header_type = u32::from_le_bytes(header_type);
        if matches!(header_type, PT_INTERP | PT_DYNAMIC) {
            return Err("ELF contains dynamic linking metadata".to_string());
        }
    }
    Ok(())
}

fn read_metadata(path: &Path) -> Result<BuildkitVersionMetadata, BuildkitError> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn inventory_is_exact(bin_dir: &Path) -> Result<bool, BuildkitError> {
    let metadata = match std::fs::symlink_metadata(bin_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
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

fn regular_non_symlink(path: &Path) -> Result<bool, BuildkitError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.file_type().is_file() && !metadata.file_type().is_symlink()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn create_private_dir(path: &Path) -> Result<(), BuildkitError> {
    std::fs::create_dir_all(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(BuildkitError::UnsafePath {
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

fn private_new_file(path: &Path) -> Result<File, BuildkitError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    Ok(options.open(path)?)
}

#[derive(Debug)]
struct InstallLock {
    file: File,
}

impl InstallLock {
    fn acquire(parent: &Path, timeout: Duration) -> Result<Self, BuildkitError> {
        let path = parent.join(".install.lock");
        let mut options = OpenOptions::new();
        options.write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let file = options.open(&path)?;
        if !file.metadata()?.is_file() {
            return Err(BuildkitError::UnsafePath {
                path: path.display().to_string(),
                reason: "install lock must be a regular file".to_string(),
            });
        }
        let started = Instant::now();
        loop {
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => return Ok(Self { file }),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if started.elapsed() >= timeout {
                        return Err(BuildkitError::InstallLockTimeout {
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

fn pinned_binary_digests() -> BTreeMap<String, String> {
    PINNED_BINARY_DIGESTS
        .into_iter()
        .map(|(name, digest)| (name.to_string(), digest.to_string()))
        .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

#[cfg(unix)]
fn mark_executable(path: &Path) -> Result<(), BuildkitError> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn mark_executable(_path: &Path) -> Result<(), BuildkitError> {
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

    use tempfile::tempdir;

    use super::*;

    fn test_binary(marker: u8) -> Vec<u8> {
        let mut bytes = vec![0_u8; 64 + 56 + 1];
        bytes[..4].copy_from_slice(b"\x7fELF");
        bytes[4] = 2;
        bytes[5] = 1;
        bytes[6] = 1;
        bytes[16..18].copy_from_slice(&2_u16.to_le_bytes());
        bytes[18..20].copy_from_slice(&183_u16.to_le_bytes());
        bytes[20..24].copy_from_slice(&1_u32.to_le_bytes());
        bytes[32..40].copy_from_slice(&64_u64.to_le_bytes());
        bytes[54..56].copy_from_slice(&56_u16.to_le_bytes());
        bytes[56..58].copy_from_slice(&1_u16.to_le_bytes());
        bytes[64..68].copy_from_slice(&1_u32.to_le_bytes());
        bytes[120] = marker;
        bytes
    }

    fn test_manifest(buildctl: &[u8], buildkitd: &[u8]) -> BuildkitArchiveManifest {
        BuildkitArchiveManifest {
            buildkit: BUILDKIT_VERSION.to_string(),
            layout: BUILDKIT_ARTIFACT_LAYOUT,
            platform: BUILDKIT_PLATFORM.to_string(),
            source_commit: BUILDKIT_SOURCE_COMMIT.to_string(),
            binaries: BTreeMap::from([
                (BUILDCTL_BINARY.to_string(), sha256_hex(buildctl)),
                (BUILDKITD_BINARY.to_string(), sha256_hex(buildkitd)),
            ]),
        }
    }

    fn build_test_archive(
        manifest: &BuildkitArchiveManifest,
        buildctl: &[u8],
        buildkitd: &[u8],
        extra: Option<(&str, &[u8])>,
    ) -> Vec<u8> {
        let mut archive = tar::Builder::new(Vec::new());
        append_archive_file(
            &mut archive,
            MANIFEST_FILE,
            &serde_json::to_vec(manifest).unwrap(),
        );
        append_archive_file(&mut archive, "bin/buildctl", buildctl);
        append_archive_file(&mut archive, "bin/buildkitd", buildkitd);
        if let Some((path, bytes)) = extra {
            append_archive_file(&mut archive, path, bytes);
        }
        archive.into_inner().unwrap()
    }

    fn append_archive_file<W: Write>(archive: &mut tar::Builder<W>, path: &str, bytes: &[u8]) {
        let mut header = tar::Header::new_ustar();
        header.set_path(path).unwrap();
        header.set_size(bytes.len() as u64);
        header.set_mode(if path == MANIFEST_FILE { 0o644 } else { 0o755 });
        header.set_cksum();
        archive.append(&header, bytes).unwrap();
    }

    fn install_test_archive(
        buildkit_dir: &Path,
        archive: &[u8],
        manifest: &BuildkitArchiveManifest,
    ) -> BuildkitArtifacts {
        install_archive(buildkit_dir, archive, &sha256_hex(archive), manifest).unwrap()
    }

    fn installed_names(bin_dir: &Path) -> Vec<String> {
        let mut names = std::fs::read_dir(bin_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[test]
    fn runtime_free_archive_installs_exact_binary_allowlist() {
        let temp = tempdir().unwrap();
        let buildctl = test_binary(1);
        let buildkitd = test_binary(2);
        let manifest = test_manifest(&buildctl, &buildkitd);
        let archive = build_test_archive(&manifest, &buildctl, &buildkitd, None);

        let artifacts = install_test_archive(temp.path(), &archive, &manifest);

        assert_eq!(installed_names(&artifacts.bin_dir), REQUIRED_BINARIES);
        let metadata = read_metadata(&temp.path().join(VERSION_FILE)).unwrap();
        assert_eq!(metadata.layout, BUILDKIT_ARTIFACT_LAYOUT);
        assert_eq!(metadata.binaries, manifest.binaries);
        assert_eq!(metadata.archive_sha256, sha256_hex(&archive));
    }

    #[test]
    fn archive_with_unexpected_runtime_binary_is_rejected() {
        let temp = tempdir().unwrap();
        let buildctl = test_binary(1);
        let buildkitd = test_binary(2);
        let manifest = test_manifest(&buildctl, &buildkitd);
        let archive = build_test_archive(
            &manifest,
            &buildctl,
            &buildkitd,
            Some(("bin/buildkit-runc", b"forbidden")),
        );

        let error =
            install_archive(temp.path(), &archive, &sha256_hex(&archive), &manifest).unwrap_err();

        assert!(matches!(
            error,
            BuildkitError::UnexpectedArchiveEntry { .. }
        ));
        assert!(!temp.path().join("bin").exists());
    }

    #[test]
    fn legacy_cache_is_rejected_and_replaced_without_runtime_binary() {
        let temp = tempdir().unwrap();
        let bin_dir = temp.path().join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        std::fs::write(bin_dir.join(BUILDKITD_BINARY), b"legacy").unwrap();
        std::fs::write(bin_dir.join("buildkit-runc"), b"legacy").unwrap();
        std::fs::write(
            temp.path().join(VERSION_FILE),
            br#"{"buildkit":"0.19.0","downloaded_at":1,"archive_sha256":"legacy"}"#,
        )
        .unwrap();
        assert!(
            load_existing_artifacts(
                temp.path(),
                BUILDKIT_ARCHIVE_SHA256_LINUX_ARM64,
                &pinned_binary_digests(),
            )
            .unwrap()
            .is_none()
        );

        let buildctl = test_binary(1);
        let buildkitd = test_binary(2);
        let manifest = test_manifest(&buildctl, &buildkitd);
        let archive = build_test_archive(&manifest, &buildctl, &buildkitd, None);
        let artifacts = install_test_archive(temp.path(), &archive, &manifest);

        assert_eq!(installed_names(&artifacts.bin_dir), REQUIRED_BINARIES);
    }

    #[test]
    fn lone_valid_rollback_generation_is_recovered_before_cache_reuse() {
        let temp = tempdir().unwrap();
        let buildctl = test_binary(1);
        let buildkitd = test_binary(2);
        let manifest = test_manifest(&buildctl, &buildkitd);
        let archive = build_test_archive(&manifest, &buildctl, &buildkitd, None);
        let archive_sha = sha256_hex(&archive);
        install_test_archive(temp.path(), &archive, &manifest);
        let rollback = temp.path().join(".bin-backup-crash");
        std::fs::rename(temp.path().join("bin"), &rollback).unwrap();

        recover_stale_install_generations(temp.path()).unwrap();

        assert!(!rollback.exists());
        assert!(
            load_existing_artifacts(temp.path(), &archive_sha, &manifest.binaries)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn rollback_is_preserved_until_current_generation_is_accepted() {
        let temp = tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join("bin")).unwrap();
        for name in [".staging-crash", ".bin-backup-crash", ".bin-rejected-crash"] {
            let dir = temp.path().join(name);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("buildkit-runc"), b"legacy").unwrap();
        }
        std::fs::write(temp.path().join(".version.json-crash"), b"legacy").unwrap();

        recover_stale_install_generations(temp.path()).unwrap();

        assert!(temp.path().join("bin").exists());
        assert!(temp.path().join(".bin-backup-crash").exists());
        for name in [
            ".staging-crash",
            ".bin-rejected-crash",
            ".version.json-crash",
        ] {
            assert!(!temp.path().join(name).exists());
        }

        discard_stale_rollback_generations(temp.path()).unwrap();
        assert!(!temp.path().join(".bin-backup-crash").exists());
    }

    #[test]
    fn exact_cache_inventory_and_binary_digests_are_enforced() {
        let temp = tempdir().unwrap();
        let buildctl = test_binary(1);
        let buildkitd = test_binary(2);
        let manifest = test_manifest(&buildctl, &buildkitd);
        let archive = build_test_archive(&manifest, &buildctl, &buildkitd, None);
        let archive_sha = sha256_hex(&archive);
        let artifacts = install_test_archive(temp.path(), &archive, &manifest);

        std::fs::write(artifacts.bin_dir.join("extra"), b"extra").unwrap();
        assert!(
            load_existing_artifacts(temp.path(), &archive_sha, &manifest.binaries)
                .unwrap()
                .is_none()
        );
        std::fs::remove_file(artifacts.bin_dir.join("extra")).unwrap();

        std::fs::write(artifacts.buildctl_path(), b"tampered").unwrap();
        assert!(
            load_existing_artifacts(temp.path(), &archive_sha, &manifest.binaries)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn oversized_cached_binary_is_rejected_before_reading_contents() {
        let temp = tempdir().unwrap();
        let buildctl = test_binary(1);
        let buildkitd = test_binary(2);
        let manifest = test_manifest(&buildctl, &buildkitd);
        let archive = build_test_archive(&manifest, &buildctl, &buildkitd, None);
        let archive_sha = sha256_hex(&archive);
        let artifacts = install_test_archive(temp.path(), &archive, &manifest);
        OpenOptions::new()
            .write(true)
            .open(artifacts.buildctl_path())
            .unwrap()
            .set_len(MAX_BINARY_BYTES + 1)
            .unwrap();

        assert!(
            load_existing_artifacts(temp.path(), &archive_sha, &manifest.binaries)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn cached_binary_hash_covers_bytes_after_elf_headers() {
        let temp = tempdir().unwrap();
        let mut buildctl = test_binary(1);
        let buildkitd = test_binary(2);
        let manifest = test_manifest(&buildctl, &buildkitd);
        let archive = build_test_archive(&manifest, &buildctl, &buildkitd, None);
        let archive_sha = sha256_hex(&archive);
        let artifacts = install_test_archive(temp.path(), &archive, &manifest);
        *buildctl.last_mut().unwrap() ^= 0xff;
        std::fs::write(artifacts.buildctl_path(), buildctl).unwrap();

        assert!(
            load_existing_artifacts(temp.path(), &archive_sha, &manifest.binaries)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn manifest_must_match_pinned_contract() {
        let temp = tempdir().unwrap();
        let buildctl = test_binary(1);
        let buildkitd = test_binary(2);
        let mut archive_manifest = test_manifest(&buildctl, &buildkitd);
        let expected_manifest = archive_manifest.clone();
        archive_manifest.layout += 1;
        let archive = build_test_archive(&archive_manifest, &buildctl, &buildkitd, None);

        let error = install_archive(
            temp.path(),
            &archive,
            &sha256_hex(&archive),
            &expected_manifest,
        )
        .unwrap_err();

        assert!(matches!(error, BuildkitError::ManifestMismatch));
    }

    #[test]
    fn archive_and_binary_checksums_are_both_enforced() {
        let temp = tempdir().unwrap();
        let buildctl = test_binary(1);
        let buildkitd = test_binary(2);
        let manifest = test_manifest(&buildctl, &buildkitd);
        let archive = build_test_archive(&manifest, &buildctl, &buildkitd, None);
        let archive_error = install_archive(temp.path(), &archive, "00", &manifest).unwrap_err();
        assert!(matches!(
            archive_error,
            BuildkitError::ArchiveChecksumMismatch { .. }
        ));

        let wrong_buildctl = test_binary(3);
        let wrong_manifest = test_manifest(&wrong_buildctl, &buildkitd);
        let binary_error = install_archive(
            temp.path(),
            &archive,
            &sha256_hex(&archive),
            &wrong_manifest,
        )
        .unwrap_err();
        assert!(matches!(
            binary_error,
            BuildkitError::BinaryChecksumMismatch { .. }
        ));
    }

    #[test]
    fn binary_validator_rejects_dynamic_or_wrong_architecture_elf() {
        let mut dynamic = test_binary(1);
        dynamic[64..68].copy_from_slice(&2_u32.to_le_bytes());
        assert!(validate_static_linux_arm64_elf(&dynamic).is_err());

        let mut wrong_arch = test_binary(1);
        wrong_arch[18..20].copy_from_slice(&62_u16.to_le_bytes());
        assert!(validate_static_linux_arm64_elf(&wrong_arch).is_err());
    }

    #[test]
    fn buildkit_artifact_paths_are_correct() {
        let artifacts = BuildkitArtifacts {
            bin_dir: PathBuf::from("/tmp/vz/buildkit/bin"),
            cache_dir: PathBuf::from("/tmp/vz/buildkit/cache"),
            version: BUILDKIT_VERSION.to_string(),
        };
        assert_eq!(
            artifacts.buildkitd_path(),
            PathBuf::from("/tmp/vz/buildkit/bin/buildkitd")
        );
        assert_eq!(
            artifacts.buildctl_path(),
            PathBuf::from("/tmp/vz/buildkit/bin/buildctl")
        );
    }

    #[test]
    fn default_source_is_vz_runtime_free_release_asset() {
        let url = buildkit_archive_url();
        assert!(url.starts_with("https://github.com/gpu-cli/vz/releases/download/"));
        assert!(url.ends_with("vz-buildkit-v0.19.0-linux-arm64.tar"));
        assert!(!url.contains("moby/buildkit"));
    }
}
