use std::path::{Path, PathBuf};
use std::str::FromStr;

use oci_distribution::Reference;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::process::Command;
use tracing::warn;
use vz_image::{ImageId, ImageStore};

use super::common::{default_buildkit_dir, unique_dir};
use super::{BUILDKIT_CACHE_DISK_IMAGE, BUILDKIT_CACHE_DISK_SIZE_BYTES, BuildkitError};

#[derive(Debug)]
pub(super) struct BuildkitArtifacts {
    pub(super) bin_dir: PathBuf,
    pub(super) cache_dir: PathBuf,
    pub(super) disk_image_path: PathBuf,
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
    ensure_buildkit_artifacts_in(base_dir, |buildkit_dir| {
        vz_oci::buildkit::ensure_buildkit_artifacts_in_dir(buildkit_dir)
    })
    .await
}

async fn ensure_buildkit_artifacts_in<F>(
    base_dir: PathBuf,
    provider: F,
) -> Result<BuildkitArtifacts, BuildkitError>
where
    F: FnOnce(
            &Path,
        )
            -> Result<vz_oci::buildkit::BuildkitArtifacts, vz_oci::buildkit::BuildkitError>
        + Send
        + 'static,
{
    let provider_dir = base_dir.clone();
    let shared = tokio::task::spawn_blocking(move || provider(&provider_dir))
        .await
        .map_err(BuildkitError::ArtifactProvisionTask)?
        .map_err(BuildkitError::ArtifactProvision)?;

    let disk_image_path = base_dir.join(BUILDKIT_CACHE_DISK_IMAGE);
    ensure_sparse_disk_image(&disk_image_path, BUILDKIT_CACHE_DISK_SIZE_BYTES)?;

    Ok(BuildkitArtifacts {
        bin_dir: shared.bin_dir,
        cache_dir: shared.cache_dir,
        disk_image_path,
    })
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn live_provider_delegates_with_managed_directory_and_preserves_layout() {
        let temp = tempdir().unwrap();
        let base_dir = temp.path().join("managed-buildkit");
        let expected_dir = base_dir.clone();
        let artifacts = ensure_buildkit_artifacts_in(base_dir.clone(), move |requested_dir| {
            assert_eq!(requested_dir, expected_dir);
            Ok(vz_oci::buildkit::BuildkitArtifacts {
                bin_dir: requested_dir.join("bin"),
                cache_dir: requested_dir.join("cache"),
                version: "test".to_string(),
            })
        })
        .await
        .unwrap();

        assert_eq!(artifacts.bin_dir, base_dir.join("bin"));
        assert_eq!(artifacts.cache_dir, base_dir.join("cache"));
        assert_eq!(
            artifacts.disk_image_path,
            base_dir.join(BUILDKIT_CACHE_DISK_IMAGE)
        );
        assert_eq!(
            std::fs::metadata(&artifacts.disk_image_path).unwrap().len(),
            BUILDKIT_CACHE_DISK_SIZE_BYTES
        );
    }

    #[tokio::test]
    async fn shared_provider_failure_keeps_runtime_free_context() {
        let temp = tempdir().unwrap();
        let error = ensure_buildkit_artifacts_in(temp.path().to_path_buf(), |_| {
            Err(vz_oci::buildkit::BuildkitError::LocalArchiveOverrideIncomplete)
        })
        .await
        .unwrap_err();

        assert!(matches!(error, BuildkitError::ArtifactProvision(_)));
        let message = error.to_string();
        assert!(message.contains("runtime-free BuildKit artifacts"));
        assert!(message.contains("VZ_BUILDKIT_ARTIFACT_SHA256"));
    }

    #[test]
    fn live_provider_source_contains_no_private_archive_downloader() {
        let production_source = include_str!("artifacts.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();

        assert!(!production_source.contains("github.com/"));
        assert!(!production_source.contains(".tar.gz"));
        assert!(!production_source.contains("reqwest"));
    }
}
