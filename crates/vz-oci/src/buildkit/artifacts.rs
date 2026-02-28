use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const VERSION_FILE: &str = "version.json";
const BUILDKITD_BINARY: &str = "buildkitd";
const BUILDKIT_RUNC_BINARY: &str = "buildkit-runc";
const BUILDKIT_ARTIFACT_SUBDIR: &str = ".vz/buildkit";
const BUILDKIT_ARCHIVE_SHA256_LINUX_ARM64: &str =
    "be7f7922d8f5eea02704cd707fb62b5a18e272452243804601b523ae6bef0ef5";

/// Pinned BuildKit release version.
pub const BUILDKIT_VERSION: &str = "0.19.0";

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

    pub fn buildkit_runc_path(&self) -> PathBuf {
        self.bin_dir.join(BUILDKIT_RUNC_BINARY)
    }
}

/// Serialized metadata for installed BuildKit artifacts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildkitVersionMetadata {
    pub buildkit: String,
    pub downloaded_at: u64,
    pub archive_sha256: String,
}

#[derive(Debug, Error)]
pub enum BuildkitError {
    #[error("HOME environment variable is not set")]
    HomeDirectoryUnavailable,

    #[error("failed to download BuildKit archive from {url}: {source}")]
    Download { url: String, source: reqwest::Error },

    #[error("BuildKit archive download from {url} returned HTTP {status}")]
    DownloadStatus { url: String, status: u16 },

    #[error("BuildKit archive checksum mismatch: expected {expected}, found {found}")]
    ChecksumMismatch { expected: String, found: String },

    #[error("BuildKit archive missing required entry: {entry}")]
    MissingArchiveEntry { entry: String },

    #[error("failed to read BuildKit archive entry path: {0}")]
    ArchiveEntryPath(#[from] std::path::StripPrefixError),

    #[error("invalid BuildKit archive entry path")]
    InvalidArchiveEntryPath,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("metadata serialization error: {0}")]
    Metadata(#[from] serde_json::Error),
}

/// Ensure pinned BuildKit artifacts are present in `~/.vz/buildkit`.
pub fn ensure_buildkit_artifacts() -> Result<BuildkitArtifacts, BuildkitError> {
    let home = std::env::var_os("HOME").ok_or(BuildkitError::HomeDirectoryUnavailable)?;
    let buildkit_dir = PathBuf::from(home).join(BUILDKIT_ARTIFACT_SUBDIR);
    ensure_buildkit_artifacts_in_dir(&buildkit_dir)
}

fn ensure_buildkit_artifacts_in_dir(
    buildkit_dir: &Path,
) -> Result<BuildkitArtifacts, BuildkitError> {
    if let Some(existing) = load_existing_artifacts(buildkit_dir)? {
        return Ok(existing);
    }

    std::fs::create_dir_all(buildkit_dir)?;
    let cache_dir = buildkit_dir.join("cache");
    std::fs::create_dir_all(&cache_dir)?;

    let archive_url = buildkit_archive_url(BUILDKIT_VERSION);
    let archive_bytes = download_archive_bytes(&archive_url)?;
    verify_archive_checksum(&archive_bytes, BUILDKIT_ARCHIVE_SHA256_LINUX_ARM64)?;

    let staging_dir = buildkit_dir.join(".staging");
    if staging_dir.exists() {
        std::fs::remove_dir_all(&staging_dir)?;
    }
    std::fs::create_dir_all(&staging_dir)?;
    let staging_bin_dir = staging_dir.join("bin");
    std::fs::create_dir_all(&staging_bin_dir)?;

    extract_required_binaries(&archive_bytes, &staging_bin_dir)?;

    let final_bin_dir = buildkit_dir.join("bin");
    if final_bin_dir.exists() {
        std::fs::remove_dir_all(&final_bin_dir)?;
    }
    std::fs::rename(&staging_bin_dir, &final_bin_dir)?;
    if staging_dir.exists() {
        std::fs::remove_dir_all(&staging_dir)?;
    }

    let metadata = BuildkitVersionMetadata {
        buildkit: BUILDKIT_VERSION.to_string(),
        downloaded_at: current_unix_secs(),
        archive_sha256: BUILDKIT_ARCHIVE_SHA256_LINUX_ARM64.to_string(),
    };
    let version_path = buildkit_dir.join(VERSION_FILE);
    write_metadata(&version_path, &metadata)?;

    Ok(BuildkitArtifacts {
        bin_dir: final_bin_dir,
        cache_dir,
        version: BUILDKIT_VERSION.to_string(),
    })
}

fn load_existing_artifacts(
    buildkit_dir: &Path,
) -> Result<Option<BuildkitArtifacts>, BuildkitError> {
    let version_path = buildkit_dir.join(VERSION_FILE);
    if !version_path.exists() {
        return Ok(None);
    }

    let metadata = match read_metadata(&version_path) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(None),
    };
    if metadata.buildkit != BUILDKIT_VERSION {
        return Ok(None);
    }

    let bin_dir = buildkit_dir.join("bin");
    let cache_dir = buildkit_dir.join("cache");
    let buildkitd_path = bin_dir.join(BUILDKITD_BINARY);
    let buildkit_runc_path = bin_dir.join(BUILDKIT_RUNC_BINARY);
    if !buildkitd_path.exists() || !buildkit_runc_path.exists() {
        return Ok(None);
    }

    std::fs::create_dir_all(&cache_dir)?;
    Ok(Some(BuildkitArtifacts {
        bin_dir,
        cache_dir,
        version: metadata.buildkit,
    }))
}

fn write_metadata(path: &Path, metadata: &BuildkitVersionMetadata) -> Result<(), BuildkitError> {
    let json = serde_json::to_vec_pretty(metadata)?;
    std::fs::write(path, json)?;
    Ok(())
}

fn read_metadata(path: &Path) -> Result<BuildkitVersionMetadata, BuildkitError> {
    let content = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&content)?)
}

fn buildkit_archive_url(version: &str) -> String {
    format!(
        "https://github.com/moby/buildkit/releases/download/v{version}/buildkit-v{version}.linux-arm64.tar.gz"
    )
}

fn download_archive_bytes(url: &str) -> Result<Vec<u8>, BuildkitError> {
    let response = reqwest::blocking::get(url).map_err(|source| BuildkitError::Download {
        url: url.to_string(),
        source,
    })?;
    if !response.status().is_success() {
        return Err(BuildkitError::DownloadStatus {
            url: url.to_string(),
            status: response.status().as_u16(),
        });
    }
    let bytes = response.bytes().map_err(|source| BuildkitError::Download {
        url: url.to_string(),
        source,
    })?;
    Ok(bytes.to_vec())
}

fn verify_archive_checksum(archive_bytes: &[u8], expected: &str) -> Result<(), BuildkitError> {
    let mut hasher = Sha256::new();
    hasher.update(archive_bytes);
    let found = format!("{:x}", hasher.finalize());
    let expected_normalized = expected.trim().to_ascii_lowercase();
    if found != expected_normalized {
        return Err(BuildkitError::ChecksumMismatch {
            expected: expected_normalized,
            found,
        });
    }
    Ok(())
}

fn extract_required_binaries(archive_bytes: &[u8], out_dir: &Path) -> Result<(), BuildkitError> {
    let decoder = GzDecoder::new(Cursor::new(archive_bytes));
    let mut archive = tar::Archive::new(decoder);
    let mut found_buildkitd = false;
    let mut found_buildkit_runc = false;

    for entry_result in archive.entries()? {
        let mut entry = entry_result?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let entry_path = entry.path()?;
        let entry_path = entry_path.as_ref();
        let file_name = match entry_path {
            path if path == Path::new("bin/buildkitd") => BUILDKITD_BINARY,
            path if path == Path::new("bin/buildkit-runc") => BUILDKIT_RUNC_BINARY,
            _ => continue,
        };

        let output_path = out_dir.join(file_name);
        let mut output_file = std::fs::File::create(&output_path)?;
        std::io::copy(&mut entry, &mut output_file)?;
        output_file.flush()?;
        mark_executable(&output_path)?;

        if file_name == BUILDKITD_BINARY {
            found_buildkitd = true;
        } else if file_name == BUILDKIT_RUNC_BINARY {
            found_buildkit_runc = true;
        }
    }

    if !found_buildkitd {
        return Err(BuildkitError::MissingArchiveEntry {
            entry: "bin/buildkitd".to_string(),
        });
    }
    if !found_buildkit_runc {
        return Err(BuildkitError::MissingArchiveEntry {
            entry: "bin/buildkit-runc".to_string(),
        });
    }
    Ok(())
}

#[cfg(unix)]
fn mark_executable(path: &Path) -> Result<(), BuildkitError> {
    use std::os::unix::fs::PermissionsExt;

    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(not(unix))]
fn mark_executable(path: &Path) -> Result<(), BuildkitError> {
    let _ = path;
    Ok(())
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tempfile::tempdir;

    use super::*;

    fn build_test_archive() -> Vec<u8> {
        let encoder = GzEncoder::new(Vec::new(), Compression::default());
        let mut archive = tar::Builder::new(encoder);

        append_archive_file(&mut archive, "bin/buildkitd", b"buildkitd");
        append_archive_file(&mut archive, "bin/buildkit-runc", b"buildkit-runc");
        append_archive_file(&mut archive, "bin/buildctl", b"buildctl");

        let encoder = archive.into_inner().unwrap();
        encoder.finish().unwrap()
    }

    fn append_archive_file<W: Write>(archive: &mut tar::Builder<W>, path: &str, bytes: &[u8]) {
        let mut header = tar::Header::new_gnu();
        header.set_path(path).unwrap();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive.append(&header, bytes).unwrap();
    }

    #[test]
    fn extract_required_binaries_only_writes_expected_files() {
        let temp = tempdir().unwrap();
        let out_dir = temp.path().join("bin");
        std::fs::create_dir_all(&out_dir).unwrap();
        let archive = build_test_archive();

        extract_required_binaries(&archive, &out_dir).unwrap();

        assert!(out_dir.join(BUILDKITD_BINARY).exists());
        assert!(out_dir.join(BUILDKIT_RUNC_BINARY).exists());
        assert!(!out_dir.join("buildctl").exists());
    }

    #[test]
    fn existing_install_is_reused_when_version_and_binaries_match() {
        let temp = tempdir().unwrap();
        let buildkit_dir = temp.path().join("buildkit");
        let bin_dir = buildkit_dir.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        std::fs::write(bin_dir.join(BUILDKITD_BINARY), b"bin").unwrap();
        std::fs::write(bin_dir.join(BUILDKIT_RUNC_BINARY), b"bin").unwrap();
        write_metadata(
            &buildkit_dir.join(VERSION_FILE),
            &BuildkitVersionMetadata {
                buildkit: BUILDKIT_VERSION.to_string(),
                downloaded_at: 1,
                archive_sha256: BUILDKIT_ARCHIVE_SHA256_LINUX_ARM64.to_string(),
            },
        )
        .unwrap();

        let existing = load_existing_artifacts(&buildkit_dir).unwrap();
        assert!(existing.is_some());
        let existing = existing.unwrap();
        assert_eq!(existing.version, BUILDKIT_VERSION);
    }

    #[test]
    fn existing_install_is_ignored_when_version_mismatches() {
        let temp = tempdir().unwrap();
        let buildkit_dir = temp.path().join("buildkit");
        let bin_dir = buildkit_dir.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        std::fs::write(bin_dir.join(BUILDKITD_BINARY), b"bin").unwrap();
        std::fs::write(bin_dir.join(BUILDKIT_RUNC_BINARY), b"bin").unwrap();
        write_metadata(
            &buildkit_dir.join(VERSION_FILE),
            &BuildkitVersionMetadata {
                buildkit: "0.18.0".to_string(),
                downloaded_at: 1,
                archive_sha256: BUILDKIT_ARCHIVE_SHA256_LINUX_ARM64.to_string(),
            },
        )
        .unwrap();

        let existing = load_existing_artifacts(&buildkit_dir).unwrap();
        assert!(existing.is_none());
    }
}
