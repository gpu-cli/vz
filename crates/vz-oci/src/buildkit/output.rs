use std::path::{Path, PathBuf};

use oci_distribution::Reference;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::process::Command;
use vz_image::{ImageId, ImageStore};

use super::BuildOutput;

/// Finalized output details for a completed build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildOutputResult {
    pub image_id: Option<ImageId>,
    pub output_path: Option<PathBuf>,
    pub pushed: bool,
}

/// Errors returned while materializing build output.
#[derive(Debug, thiserror::Error)]
pub enum BuildOutputError {
    #[error("invalid OCI image layout: {0}")]
    InvalidOciLayout(String),

    #[error("blob digest mismatch for {digest}: expected {expected}, found {found}")]
    DigestMismatch {
        digest: String,
        expected: String,
        found: String,
    },

    #[error("unsupported digest algorithm '{algorithm}' in {digest}")]
    UnsupportedDigestAlgorithm { digest: String, algorithm: String },

    #[error("output mode requires OCI archive path")]
    MissingOciArchive,

    #[error("output mode requires image store handle")]
    MissingImageStore,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Image(#[from] vz_image::ImageError),
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

/// Materialize caller-requested output mode using optional OCI archive/store handles.
pub async fn materialize_build_output(
    output: &BuildOutput,
    store: Option<&ImageStore>,
    oci_archive: Option<&Path>,
) -> Result<BuildOutputResult, BuildOutputError> {
    match output {
        BuildOutput::Registry { .. } => Ok(BuildOutputResult {
            image_id: None,
            output_path: None,
            pushed: true,
        }),
        BuildOutput::OciTarball { dest } => {
            let archive = oci_archive.ok_or(BuildOutputError::MissingOciArchive)?;
            if let Some(parent) = dest.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::copy(archive, dest).await?;
            Ok(BuildOutputResult {
                image_id: None,
                output_path: Some(dest.clone()),
                pushed: false,
            })
        }
        BuildOutput::Local { dest } => {
            tokio::fs::create_dir_all(dest).await?;
            Ok(BuildOutputResult {
                image_id: None,
                output_path: Some(dest.clone()),
                pushed: false,
            })
        }
        BuildOutput::VzStore { tag } => {
            let archive = oci_archive.ok_or(BuildOutputError::MissingOciArchive)?;
            let image_store = store.ok_or(BuildOutputError::MissingImageStore)?;
            let image_id = import_oci_tar_to_store(image_store, archive, tag).await?;
            Ok(BuildOutputResult {
                image_id: Some(image_id),
                output_path: None,
                pushed: false,
            })
        }
    }
}

/// Import OCI image tarball into local `ImageStore` and attach a reference tag.
pub async fn import_oci_tar_to_store(
    store: &ImageStore,
    image_tar: &Path,
    reference: &str,
) -> Result<ImageId, BuildOutputError> {
    let parent = image_tar.parent().ok_or_else(|| {
        BuildOutputError::InvalidOciLayout("output tar has no parent directory".to_string())
    })?;
    let extract_dir = unique_dir(parent, "oci-import");
    tokio::fs::create_dir_all(&extract_dir).await?;

    let extract_output = Command::new("tar")
        .arg("-xf")
        .arg(image_tar)
        .arg("-C")
        .arg(&extract_dir)
        .output()
        .await?;
    if !extract_output.status.success() {
        return Err(BuildOutputError::InvalidOciLayout(format!(
            "unable to unpack OCI tarball: {}",
            String::from_utf8_lossy(&extract_output.stderr)
        )));
    }

    let result = import_extracted_oci_layout(store, &extract_dir, reference).await;
    if let Err(error) = tokio::fs::remove_dir_all(&extract_dir).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        // Cleanup failures should not mask successful import results.
    }
    result
}

async fn import_extracted_oci_layout(
    store: &ImageStore,
    root: &Path,
    reference: &str,
) -> Result<ImageId, BuildOutputError> {
    let index_json = tokio::fs::read(root.join("index.json")).await?;
    let index: OciIndex = serde_json::from_slice(&index_json)?;
    let descriptor = index
        .manifests
        .iter()
        .find(|entry| entry.media_type.contains("image.manifest"))
        .or_else(|| index.manifests.first())
        .ok_or_else(|| {
            BuildOutputError::InvalidOciLayout("index.json contains no manifests".to_string())
        })?;

    let manifest_digest = descriptor.digest.clone();
    let manifest_blob = read_blob(root, &manifest_digest).await?;
    verify_blob_digest(&manifest_digest, &manifest_blob)?;
    let manifest: OciManifest = serde_json::from_slice(&manifest_blob)?;

    let config_blob = read_blob(root, &manifest.config.digest).await?;
    verify_blob_digest(&manifest.config.digest, &config_blob)?;

    store.ensure_layout()?;
    store.write_manifest_json(&manifest_digest, &manifest_blob)?;
    store.write_config_json(&manifest_digest, &config_blob)?;

    for layer in &manifest.layers {
        let layer_blob = read_blob(root, &layer.digest).await?;
        verify_blob_digest(&layer.digest, &layer_blob)?;
        store.write_layer_blob(&layer.digest, &layer.media_type, &layer_blob)?;
    }

    let canonical_reference = canonicalize_reference(reference);
    store.write_reference(&canonical_reference, &manifest_digest)?;
    if canonical_reference != reference {
        store.write_reference(reference, &manifest_digest)?;
    }

    Ok(ImageId(manifest_digest))
}

fn unique_dir(base: &Path, prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    base.join(format!("{prefix}-{nanos}"))
}

fn canonicalize_reference(reference: &str) -> String {
    reference
        .parse::<Reference>()
        .map(|parsed| parsed.whole())
        .unwrap_or_else(|_| reference.to_string())
}

async fn read_blob(root: &Path, digest: &str) -> Result<Vec<u8>, BuildOutputError> {
    let path = blob_path(root, digest)?;
    Ok(tokio::fs::read(path).await?)
}

fn blob_path(root: &Path, digest: &str) -> Result<PathBuf, BuildOutputError> {
    let mut parts = digest.splitn(2, ':');
    let Some(algorithm) = parts.next() else {
        return Err(BuildOutputError::InvalidOciLayout(format!(
            "invalid digest format: {digest}"
        )));
    };
    let Some(hash) = parts.next() else {
        return Err(BuildOutputError::InvalidOciLayout(format!(
            "invalid digest format: {digest}"
        )));
    };

    if algorithm != "sha256" {
        return Err(BuildOutputError::UnsupportedDigestAlgorithm {
            digest: digest.to_string(),
            algorithm: algorithm.to_string(),
        });
    }

    Ok(root.join("blobs").join(algorithm).join(hash))
}

fn verify_blob_digest(digest: &str, blob: &[u8]) -> Result<(), BuildOutputError> {
    let mut parts = digest.splitn(2, ':');
    let algorithm = parts.next().unwrap_or_default();
    let expected = parts.next().unwrap_or_default();
    if algorithm != "sha256" {
        return Err(BuildOutputError::UnsupportedDigestAlgorithm {
            digest: digest.to_string(),
            algorithm: algorithm.to_string(),
        });
    }

    let found = format!("{:x}", Sha256::digest(blob));
    if found != expected {
        return Err(BuildOutputError::DigestMismatch {
            digest: digest.to_string(),
            expected: expected.to_string(),
            found,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::fs;
    use std::path::Path;

    use serde::Serialize;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;
    use tokio::process::Command;
    use vz_image::ImageStore;

    use super::{BuildOutputError, import_oci_tar_to_store};

    #[tokio::test]
    async fn import_oci_tar_writes_store_reference_and_blobs() {
        let tmp = tempdir().unwrap();
        let layout = tmp.path().join("layout");
        fs::create_dir_all(layout.join("blobs/sha256")).unwrap();
        fs::write(
            layout.join("oci-layout"),
            r#"{"imageLayoutVersion":"1.0.0"}"#,
        )
        .unwrap();

        let config_json =
            br#"{"architecture":"arm64","os":"linux","config":{"Cmd":["echo","ok"]}}"#;
        let config_digest = sha256_digest(config_json);
        write_blob(&layout, &config_digest, config_json);

        let layer_source = tmp.path().join("layer-src");
        fs::create_dir_all(&layer_source).unwrap();
        fs::write(layer_source.join("message.txt"), "hello from layer\n").unwrap();
        let layer_tar = tmp.path().join("layer.tar");
        let tar_status = Command::new("tar")
            .arg("-cf")
            .arg(&layer_tar)
            .arg("-C")
            .arg(&layer_source)
            .arg(".")
            .status()
            .await
            .unwrap();
        assert!(tar_status.success());
        let layer_bytes = fs::read(&layer_tar).unwrap();
        let layer_digest = sha256_digest(&layer_bytes);
        write_blob(&layout, &layer_digest, &layer_bytes);

        let manifest = ManifestJson {
            schema_version: 2,
            media_type: "application/vnd.oci.image.manifest.v1+json",
            config: DescriptorJson {
                media_type: "application/vnd.oci.image.config.v1+json",
                digest: config_digest.clone(),
                size: config_json.len(),
            },
            layers: vec![DescriptorJson {
                media_type: "application/vnd.oci.image.layer.v1.tar",
                digest: layer_digest.clone(),
                size: layer_bytes.len(),
            }],
        };
        let manifest_json = serde_json::to_vec(&manifest).unwrap();
        let manifest_digest = sha256_digest(&manifest_json);
        write_blob(&layout, &manifest_digest, &manifest_json);

        let index = IndexJson {
            schema_version: 2,
            media_type: "application/vnd.oci.image.index.v1+json",
            manifests: vec![DescriptorJson {
                media_type: "application/vnd.oci.image.manifest.v1+json",
                digest: manifest_digest.clone(),
                size: manifest_json.len(),
            }],
        };
        fs::write(
            layout.join("index.json"),
            serde_json::to_vec(&index).unwrap(),
        )
        .unwrap();

        let image_tar = tmp.path().join("image.tar");
        let tar_status = Command::new("tar")
            .arg("-cf")
            .arg(&image_tar)
            .arg("-C")
            .arg(&layout)
            .arg(".")
            .status()
            .await
            .unwrap();
        assert!(tar_status.success());

        let store = ImageStore::new(tmp.path().join("oci"));
        let imported = import_oci_tar_to_store(&store, &image_tar, "demo:latest")
            .await
            .unwrap();

        assert_eq!(imported.0, manifest_digest);
        assert_eq!(
            store.read_reference("demo:latest").unwrap(),
            manifest_digest
        );
        assert!(store.read_manifest_json(&manifest_digest).is_ok());
        assert!(store.read_config_json(&manifest_digest).is_ok());
        assert!(store.has_layer_blob(&layer_digest));
    }

    #[tokio::test]
    async fn import_oci_tar_fails_for_invalid_digest_algorithm() {
        let temp = tempdir().unwrap();
        let store = ImageStore::new(temp.path().join("oci"));
        let bogus = temp.path().join("bogus.tar");
        fs::write(&bogus, b"not-a-tar").unwrap();

        let result = import_oci_tar_to_store(&store, &bogus, "demo:latest").await;
        assert!(matches!(
            result,
            Err(BuildOutputError::InvalidOciLayout(_)) | Err(BuildOutputError::Io(_))
        ));
    }

    fn write_blob(root: &Path, digest: &str, bytes: &[u8]) {
        let (algorithm, hash) = digest.split_once(':').unwrap();
        let path = root.join("blobs").join(algorithm).join(hash);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    fn sha256_digest(bytes: &[u8]) -> String {
        format!("sha256:{:x}", Sha256::digest(bytes))
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct DescriptorJson {
        media_type: &'static str,
        digest: String,
        size: usize,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ManifestJson {
        schema_version: i32,
        media_type: &'static str,
        config: DescriptorJson,
        layers: Vec<DescriptorJson>,
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct IndexJson {
        schema_version: i32,
        media_type: &'static str,
        manifests: Vec<DescriptorJson>,
    }
}
