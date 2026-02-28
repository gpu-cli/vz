use std::path::{Path, PathBuf};
use std::str::FromStr;

use oci_distribution::Reference;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tracing::warn;
use vz_image::{ImageId, ImageStore};

use super::common::{default_buildkit_dir, unique_dir, unix_timestamp_secs};
use super::{
    BUILDCTL_BINARY, BUILDKIT_CACHE_DISK_IMAGE, BUILDKIT_CACHE_DISK_SIZE_BYTES,
    BUILDKIT_RUNC_BINARY, BUILDKIT_VERSION, BUILDKITD_BINARY, BuildkitError, VERSION_FILE,
};

pub(super) struct BuildkitArtifacts {
    pub(super) bin_dir: PathBuf,
    pub(super) cache_dir: PathBuf,
    pub(super) disk_image_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BuildkitVersionFile {
    buildkit: String,
    downloaded_at: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OciDescriptor {
    media_type: String,
    digest: String,
}

#[derive(Debug, Deserialize)]
struct OciIndex {
    manifests: Vec<OciDescriptor>,
}

#[derive(Debug, Deserialize)]
struct OciManifest {
    config: OciDescriptor,
    layers: Vec<OciDescriptor>,
}
pub(super) async fn ensure_buildkit_artifacts() -> Result<BuildkitArtifacts, BuildkitError> {
    let base_dir = default_buildkit_dir()?;
    let bin_dir = base_dir.join("bin");
    let cache_dir = base_dir.join("cache");
    let disk_image_path = base_dir.join(BUILDKIT_CACHE_DISK_IMAGE);
    tokio::fs::create_dir_all(&cache_dir).await?;
    ensure_sparse_disk_image(&disk_image_path, BUILDKIT_CACHE_DISK_SIZE_BYTES)?;

    if artifacts_are_current(&base_dir, &bin_dir).await? {
        return Ok(BuildkitArtifacts {
            bin_dir,
            cache_dir,
            disk_image_path,
        });
    }

    tokio::fs::create_dir_all(&base_dir).await?;
    let staging_dir = unique_dir(base_dir.clone(), "download");
    tokio::fs::create_dir_all(&staging_dir).await?;
    let tarball_path = staging_dir.join("buildkit.tar.gz");

    let url = format!(
        "https://github.com/moby/buildkit/releases/download/v{version}/buildkit-v{version}.linux-arm64.tar.gz",
        version = BUILDKIT_VERSION
    );
    download_file(&url, &tarball_path).await?;
    extract_buildkit_archive(&tarball_path, &staging_dir).await?;

    let extracted_bin_dir = staging_dir.join("bin");
    let buildkitd_path = extracted_bin_dir.join(BUILDKITD_BINARY);
    let buildctl_path = extracted_bin_dir.join(BUILDCTL_BINARY);
    let runc_path = extracted_bin_dir.join(BUILDKIT_RUNC_BINARY);
    for path in [&buildkitd_path, &buildctl_path, &runc_path] {
        if !path.is_file() {
            return Err(BuildkitError::InvalidConfig(format!(
                "missing expected BuildKit binary: {}",
                path.display()
            )));
        }
        make_executable(path).await?;
    }

    if tokio::fs::metadata(&bin_dir).await.is_ok() {
        tokio::fs::remove_dir_all(&bin_dir).await?;
    }
    tokio::fs::rename(&extracted_bin_dir, &bin_dir).await?;

    let version = BuildkitVersionFile {
        buildkit: BUILDKIT_VERSION.to_string(),
        downloaded_at: unix_timestamp_secs(),
    };
    let version_json = serde_json::to_vec_pretty(&version)?;
    tokio::fs::write(base_dir.join(VERSION_FILE), version_json).await?;

    if let Err(error) = tokio::fs::remove_dir_all(&staging_dir).await {
        warn!(
            path = %staging_dir.display(),
            %error,
            "failed to clean BuildKit staging directory"
        );
    }

    Ok(BuildkitArtifacts {
        bin_dir,
        cache_dir,
        disk_image_path,
    })
}

async fn artifacts_are_current(base_dir: &Path, bin_dir: &Path) -> Result<bool, BuildkitError> {
    let version_path = base_dir.join(VERSION_FILE);
    let version_text = match tokio::fs::read_to_string(version_path).await {
        Ok(value) => value,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(BuildkitError::Io(err)),
    };
    let metadata: BuildkitVersionFile = serde_json::from_str(&version_text)?;
    if metadata.buildkit != BUILDKIT_VERSION {
        return Ok(false);
    }

    for name in [BUILDKITD_BINARY, BUILDCTL_BINARY, BUILDKIT_RUNC_BINARY] {
        if !bin_dir.join(name).is_file() {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn download_file(url: &str, destination: &Path) -> Result<(), BuildkitError> {
    let client = reqwest::Client::new();
    let mut response = client.get(url).send().await?.error_for_status()?;
    let mut file = tokio::fs::File::create(destination).await?;

    while let Some(chunk) = response.chunk().await? {
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    Ok(())
}

async fn extract_buildkit_archive(
    tarball_path: &Path,
    destination: &Path,
) -> Result<(), BuildkitError> {
    let output = Command::new("tar")
        .arg("-xzf")
        .arg(tarball_path)
        .arg("-C")
        .arg(destination)
        .arg("bin/buildkitd")
        .arg("bin/buildctl")
        .arg("bin/buildkit-runc")
        .output()
        .await?;
    if !output.status.success() {
        return Err(BuildkitError::InvalidConfig(format!(
            "failed to extract BuildKit archive with tar: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

fn ensure_sparse_disk_image(path: &Path, desired_size: u64) -> Result<(), BuildkitError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    if let Ok(metadata) = std::fs::metadata(path) {
        if metadata.len() >= desired_size {
            return Ok(());
        }
    }

    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    file.set_len(desired_size)?;
    Ok(())
}

async fn make_executable(path: &Path) -> Result<(), BuildkitError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut perms = tokio::fs::metadata(path).await?.permissions();
        perms.set_mode(0o755);
        tokio::fs::set_permissions(path, perms).await?;
    }
    Ok(())
}

pub(crate) async fn import_oci_tar_to_store(
    store: &ImageStore,
    image_tar: &Path,
    reference: &str,
) -> Result<ImageId, BuildkitError> {
    let parent = image_tar.parent().ok_or_else(|| {
        BuildkitError::InvalidOciLayout("output tar has no parent directory".to_string())
    })?;
    let extract_dir = unique_dir(parent.to_path_buf(), "oci-import");
    tokio::fs::create_dir_all(&extract_dir).await?;

    let extract_output = Command::new("tar")
        .arg("-xf")
        .arg(image_tar)
        .arg("-C")
        .arg(&extract_dir)
        .output()
        .await?;
    if !extract_output.status.success() {
        return Err(BuildkitError::InvalidOciLayout(format!(
            "unable to unpack OCI tarball: {}",
            String::from_utf8_lossy(&extract_output.stderr)
        )));
    }

    let index_json = tokio::fs::read(extract_dir.join("index.json")).await?;
    let index: OciIndex = serde_json::from_slice(&index_json)?;
    let descriptor = index
        .manifests
        .iter()
        .find(|descriptor| descriptor.media_type.contains("image.manifest"))
        .or_else(|| index.manifests.first())
        .ok_or_else(|| {
            BuildkitError::InvalidOciLayout("index.json contains no manifests".to_string())
        })?;

    let manifest_digest = descriptor.digest.clone();
    let manifest_blob = read_blob(&extract_dir, &manifest_digest).await?;
    verify_blob_digest(&manifest_digest, &manifest_blob)?;
    let manifest: OciManifest = serde_json::from_slice(&manifest_blob)?;

    let config_blob = read_blob(&extract_dir, &manifest.config.digest).await?;
    verify_blob_digest(&manifest.config.digest, &config_blob)?;

    store.ensure_layout()?;
    store.write_manifest_json(&manifest_digest, &manifest_blob)?;
    store.write_config_json(&manifest_digest, &config_blob)?;

    for layer in &manifest.layers {
        let layer_blob = read_blob(&extract_dir, &layer.digest).await?;
        verify_blob_digest(&layer.digest, &layer_blob)?;
        store.write_layer_blob(&layer.digest, &layer.media_type, &layer_blob)?;
    }
    let canonical_reference = canonicalize_reference(reference);
    store.write_reference(&canonical_reference, &manifest_digest)?;
    if canonical_reference != reference {
        store.write_reference(reference, &manifest_digest)?;
    }

    if let Err(error) = tokio::fs::remove_dir_all(&extract_dir).await {
        warn!(
            path = %extract_dir.display(),
            %error,
            "failed to clean OCI import extraction directory"
        );
    }

    Ok(ImageId(manifest_digest))
}

fn canonicalize_reference(reference: &str) -> String {
    Reference::from_str(reference)
        .map(|parsed| parsed.whole())
        .unwrap_or_else(|_| reference.to_string())
}

async fn read_blob(root: &Path, digest: &str) -> Result<Vec<u8>, BuildkitError> {
    let path = blob_path(root, digest)?;
    tokio::fs::read(path).await.map_err(BuildkitError::from)
}

fn blob_path(root: &Path, digest: &str) -> Result<PathBuf, BuildkitError> {
    let (algorithm, encoded) = digest.split_once(':').ok_or_else(|| {
        BuildkitError::InvalidOciLayout(format!("invalid digest format: {digest}"))
    })?;
    Ok(root.join("blobs").join(algorithm).join(encoded))
}

fn verify_blob_digest(digest: &str, data: &[u8]) -> Result<(), BuildkitError> {
    let (algorithm, expected) = digest.split_once(':').ok_or_else(|| {
        BuildkitError::InvalidOciLayout(format!("invalid digest format: {digest}"))
    })?;
    if algorithm != "sha256" {
        return Err(BuildkitError::UnsupportedDigestAlgorithm {
            digest: digest.to_string(),
            algorithm: algorithm.to_string(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(data);
    let found = format!("{:x}", hasher.finalize());
    let expected = expected.to_ascii_lowercase();
    if found != expected {
        return Err(BuildkitError::DigestMismatch {
            digest: digest.to_string(),
            expected,
            found,
        });
    }
    Ok(())
}
