//! Filesystem-backed OCI image store.
//!
//! The store uses a content-addressed layout:
//! - `manifests/<digest>.json`
//! - `configs/<digest>.json`
//! - `layers/<digest>.<ext>` for compressed blob content
//! - `layers/<digest>/` unpacked layer tree
//! - `refs/<ref>` mapping from image reference to image digest
//! - `rootfs/<container-id>/` assembled rootfs for a container

use std::collections::{HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;
#[cfg(unix)]
use std::os::unix::fs as unix_fs;

/// Layout-aware OCI image cache and layer assembly helpers.
#[derive(Debug, Clone)]
pub struct ImageStore {
    base_dir: PathBuf,
}

/// Parsed OCI layer metadata used when reconstructing a rootfs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerDescriptor {
    /// Layer digest, e.g. `sha256:...`.
    pub digest: String,
    /// OCI media type of the layer blob.
    pub media_type: String,
}

/// Cached image reference and manifest identifier pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageInfo {
    /// Human-readable image reference, for example `ubuntu:latest`.
    pub reference: String,
    /// Image identifier used by stored manifests/configs (digest form).
    pub image_id: String,
}

/// Summary of a local image prune pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneResult {
    /// Number of stale reference mappings that were removed.
    pub removed_refs: usize,
    /// Number of manifest JSON files removed.
    pub removed_manifests: usize,
    /// Number of config JSON files removed.
    pub removed_configs: usize,
    /// Number of unpacked layer directories removed.
    pub removed_layer_dirs: usize,
}

/// Supported OCI layer media types this store recognizes for unpacking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LayerMediaType {
    Gzip,
    Zstd,
    Tar,
}

impl LayerMediaType {
    fn extension(self) -> &'static str {
        match self {
            Self::Gzip => "tar.gz",
            Self::Zstd => "tar.zst",
            Self::Tar => "tar",
        }
    }

    fn from_media_type(media_type: &str) -> Self {
        let lower = media_type.to_lowercase();
        if lower.contains("zstd") {
            Self::Zstd
        } else if lower.contains("gzip") || lower.contains("x-gzip") {
            Self::Gzip
        } else {
            Self::Tar
        }
    }
}

impl ImageStore {
    /// Create a store rooted at `base_dir`.
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Ensure all OCI directories exist.
    pub fn ensure_layout(&self) -> io::Result<()> {
        fs::create_dir_all(self.manifests_dir())?;
        fs::create_dir_all(self.configs_dir())?;
        fs::create_dir_all(self.layers_dir())?;
        fs::create_dir_all(self.refs_dir())?;
        fs::create_dir_all(self.rootfs_dir_root())?;
        Ok(())
    }

    /// Persist a manifest JSON blob for an image digest.
    pub fn write_manifest_json(&self, image_id: &str, manifest_json: &[u8]) -> io::Result<()> {
        let path = self.manifest_path(image_id);
        self.write_atomic(&path, manifest_json)
    }

    /// Read a manifest JSON blob by image digest.
    pub fn read_manifest_json(&self, image_id: &str) -> io::Result<Vec<u8>> {
        fs::read(self.manifest_path(image_id))
    }

    /// Persist a config JSON blob for an image digest.
    pub fn write_config_json(&self, image_id: &str, config_json: &[u8]) -> io::Result<()> {
        let path = self.config_path(image_id);
        self.write_atomic(&path, config_json)
    }

    /// Read a config JSON blob by image digest.
    pub fn read_config_json(&self, image_id: &str) -> io::Result<Vec<u8>> {
        fs::read(self.config_path(image_id))
    }

    /// Write a reference -> digest mapping.
    pub fn write_reference(&self, reference: &str, image_id: &str) -> io::Result<()> {
        self.ensure_layout()?;
        self.write_atomic(&self.ref_path(reference), image_id.as_bytes())
    }

    /// Read an image digest for a saved reference.
    pub fn read_reference(&self, reference: &str) -> io::Result<String> {
        let data = fs::read_to_string(self.ref_path(reference))?;
        Ok(data.trim().to_string())
    }

    /// List cached image references and their resolved image identifiers.
    pub fn list_images(&self) -> io::Result<Vec<ImageInfo>> {
        let mut refs = self.reference_entries()?;
        refs.sort_by(|a, b| a.reference.cmp(&b.reference));

        Ok(refs
            .into_iter()
            .map(|entry| ImageInfo {
                reference: entry.reference,
                image_id: entry.image_id,
            })
            .collect())
    }

    /// Prune image cache metadata and unpacked layers not referenced by any manifest.
    pub fn prune_images(&self) -> io::Result<PruneResult> {
        let mut result = PruneResult {
            removed_refs: 0,
            removed_manifests: 0,
            removed_configs: 0,
            removed_layer_dirs: 0,
        };

        let mut references = self.reference_entries()?;
        let mut referenced_image_ids = HashSet::new();

        for reference in references.drain(..) {
            let manifest_exists = self.manifest_path(&reference.image_id).is_file();

            if !manifest_exists {
                fs::remove_file(&reference.path)?;
                result.removed_refs += 1;
                continue;
            }

            let _ = referenced_image_ids.insert(reference.image_id);
        }

        let manifests_dir = self.manifests_dir();
        let mut referenced_layer_digests = HashSet::new();

        for image_id in referenced_image_ids.iter() {
            let manifest_json = match fs::read(self.manifest_path(image_id)) {
                Ok(json) => json,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };

            if let Ok(layers) = parse_manifest_layers(&manifest_json) {
                referenced_layer_digests.extend(layers.into_iter().map(|layer| layer.digest));
            }
        }

        if manifests_dir.exists() {
            if !manifests_dir.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manifests path is not a directory",
                ));
            }

            for entry in fs::read_dir(manifests_dir)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }

                let path = entry.path();
                let Some(stem_os) = path.file_stem() else {
                    continue;
                };
                let Some(image_id) = stem_os.to_str() else {
                    continue;
                };

                if referenced_image_ids.contains(image_id) {
                    continue;
                }

                fs::remove_file(&path)?;
                result.removed_manifests += 1;

                let config_path = self.config_path(image_id);
                if config_path.is_file() {
                    fs::remove_file(config_path)?;
                    result.removed_configs += 1;
                }
            }
        }

        let layers_dir = self.layers_dir();
        if layers_dir.exists() {
            if !layers_dir.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "layers path is not a directory",
                ));
            }

            for entry in fs::read_dir(layers_dir)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }

                let name_os = entry.file_name();
                let Some(name) = name_os.to_str() else {
                    continue;
                };

                if !is_image_id(name) {
                    continue;
                }

                if referenced_layer_digests.contains(name) {
                    continue;
                }

                fs::remove_dir_all(entry.path())?;
                result.removed_layer_dirs += 1;
            }
        }

        Ok(result)
    }

    /// Write a compressed layer blob indexed by digest.
    pub fn write_layer_blob(&self, digest: &str, media_type: &str, data: &[u8]) -> io::Result<()> {
        self.ensure_layout()?;
        let path = self.layer_blob_path(digest, LayerMediaType::from_media_type(media_type));
        self.write_atomic(&path, data)
    }

    /// Return whether any layer blob exists for `digest`.
    pub fn has_layer_blob(&self, digest: &str) -> bool {
        self.layer_file_candidates(digest)
            .into_iter()
            .any(|candidate| candidate.exists())
    }

    /// Unpack a layer blob into `layers/<digest>/`.
    ///
    /// Supports gzip, zstd, and plain tar media types.
    pub fn unpack_layer(&self, digest: &str, media_type: &str) -> io::Result<PathBuf> {
        self.unpack_layer_inner(digest, media_type)
    }

    /// Unpack a layer blob in a blocking task.
    ///
    /// This async helper mirrors the runtime behavior expected by the planner:
    /// heavy I/O and traversal are moved to a dedicated blocking worker.
    pub async fn unpack_layer_async(&self, digest: &str, media_type: &str) -> io::Result<PathBuf> {
        let store = self.clone();
        let digest = digest.to_string();
        let media_type = media_type.to_string();

        tokio::task::spawn_blocking(move || store.unpack_layer_inner(&digest, &media_type))
            .await
            .map_err(|err| io::Error::other(err.to_string()))?
    }

    /// Internal helper for unpacking a layer.
    fn unpack_layer_inner(&self, digest: &str, media_type: &str) -> io::Result<PathBuf> {
        let src = self.resolve_layer_blob_path(digest)?;
        let destination = self.unpacked_layer_dir(digest);

        if destination.exists() {
            return Ok(destination);
        }

        let media = LayerMediaType::from_media_type(media_type);

        fs::create_dir_all(&destination)?;

        let status = match media {
            LayerMediaType::Gzip => Command::new("tar")
                .arg("-xpf")
                .arg(&src)
                .arg("-C")
                .arg(&destination)
                .arg("-z")
                .status()?,
            LayerMediaType::Zstd => Command::new("tar")
                .arg("-xpf")
                .arg(&src)
                .arg("-C")
                .arg(&destination)
                .arg("--zstd")
                .status()?,
            LayerMediaType::Tar => Command::new("tar")
                .arg("-xpf")
                .arg(&src)
                .arg("-C")
                .arg(&destination)
                .status()?,
        };

        if !status.success() {
            fs::remove_dir_all(&destination)?;
            return Err(io::Error::other(format!(
                "unable to unpack layer {digest} using media type {media_type}",
            )));
        }

        Ok(destination)
    }

    /// Assemble and apply all image layers into `rootfs/<container_id>/`.
    pub fn assemble_rootfs(&self, image_id: &str, container_id: &str) -> io::Result<PathBuf> {
        let manifest_json = self.read_manifest_json(image_id)?;
        let layers = parse_manifest_layers(&manifest_json)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
        let rootfs = self.rootfs_path(container_id);

        if rootfs.exists() {
            fs::remove_dir_all(&rootfs)?;
        }

        fs::create_dir_all(&rootfs)?;

        for layer in layers {
            let layer_root = self.unpack_layer(&layer.digest, &layer.media_type)?;
            overlay_copy_layer(&layer_root, &rootfs)?;
        }

        Ok(rootfs)
    }

    /// Assemble a rootfs for `container_id` in a blocking task.
    ///
    /// This keeps heavy filesystem traversal off the async runtime.
    pub async fn assemble_rootfs_async(
        &self,
        image_id: &str,
        container_id: &str,
    ) -> io::Result<PathBuf> {
        let store = self.clone();
        let image_id = image_id.to_string();
        let container_id = container_id.to_string();

        tokio::task::spawn_blocking(move || store.assemble_rootfs(&image_id, &container_id))
            .await
            .map_err(|err| io::Error::other(err.to_string()))?
    }

    fn manifest_path(&self, image_id: &str) -> PathBuf {
        self.manifests_dir().join(format!("{image_id}.json"))
    }

    fn config_path(&self, image_id: &str) -> PathBuf {
        self.configs_dir().join(format!("{image_id}.json"))
    }

    fn ref_path(&self, reference: &str) -> PathBuf {
        self.refs_dir().join(encode_reference(reference))
    }

    fn layer_blob_path(&self, digest: &str, media: LayerMediaType) -> PathBuf {
        self.layers_dir()
            .join(format!("{digest}.{}", media.extension()))
    }

    fn layer_file_candidates(&self, digest: &str) -> Vec<PathBuf> {
        [
            LayerMediaType::Tar,
            LayerMediaType::Gzip,
            LayerMediaType::Zstd,
        ]
        .into_iter()
        .map(|media| self.layer_blob_path(digest, media))
        .collect()
    }

    fn resolve_layer_blob_path(&self, digest: &str) -> io::Result<PathBuf> {
        let mut found: Option<PathBuf> = None;
        for path in self.layer_file_candidates(digest) {
            if path.exists() && found.is_none() {
                found = Some(path);
            }
        }

        found.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("missing layer blob for digest {digest}"),
            )
        })
    }

    fn unpacked_layer_dir(&self, digest: &str) -> PathBuf {
        self.layers_dir().join(digest)
    }

    fn rootfs_path(&self, container_id: &str) -> PathBuf {
        self.rootfs_dir_root().join(container_id)
    }

    fn manifests_dir(&self) -> PathBuf {
        self.base_dir.join("manifests")
    }

    fn configs_dir(&self) -> PathBuf {
        self.base_dir.join("configs")
    }

    fn layers_dir(&self) -> PathBuf {
        self.base_dir.join("layers")
    }

    fn refs_dir(&self) -> PathBuf {
        self.base_dir.join("refs")
    }

    fn rootfs_dir_root(&self) -> PathBuf {
        self.base_dir.join("rootfs")
    }

    fn reference_entries(&self) -> io::Result<Vec<ReferenceEntry>> {
        let refs_dir = self.refs_dir();
        if !refs_dir.exists() {
            return Ok(Vec::new());
        }

        if !refs_dir.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "refs path is not a directory",
            ));
        }

        let mut references = Vec::new();

        for entry in fs::read_dir(&refs_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }

            let encoded_reference = entry.file_name();
            let Some(encoded_reference) = encoded_reference.to_str() else {
                continue;
            };

            let image_id = match fs::read_to_string(entry.path()) {
                Ok(data) => data.trim().to_string(),
                Err(err) => return Err(err),
            };

            if image_id.is_empty() {
                continue;
            }

            references.push(ReferenceEntry {
                reference: decode_reference(encoded_reference),
                image_id,
                path: entry.path(),
            });
        }

        Ok(references)
    }

    fn write_atomic(&self, destination: &Path, bytes: &[u8]) -> io::Result<()> {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }

        let tmp = unique_temp_path(destination);
        {
            let mut file = File::create(&tmp)?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }

        fs::rename(&tmp, destination)
    }
}

#[derive(Debug)]
struct ReferenceEntry {
    reference: String,
    image_id: String,
    path: PathBuf,
}

fn encode_reference(reference: &str) -> String {
    let mut encoded = String::with_capacity(reference.len());
    for &byte in reference.as_bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' || byte == b'.' {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02x}"));
        }
    }

    encoded
}

fn decode_reference(reference: &str) -> String {
    let bytes = reference.as_bytes();
    let mut decoded = String::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let encoded = &reference[index + 1..index + 3];
            if let Ok(byte) = u8::from_str_radix(encoded, 16) {
                decoded.push(byte as char);
                index += 3;
                continue;
            }
        }

        decoded.push(bytes[index] as char);
        index += 1;
    }

    decoded
}

fn is_image_id(value: &str) -> bool {
    value.contains(':')
}

fn unique_temp_path(path: &Path) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id();

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("store");
    let temp_name = format!("{file_name}.tmp.{pid}.{timestamp}");
    let mut out = path.to_path_buf();
    out.set_file_name(temp_name);
    out
}

fn overlay_copy_layer(source_layer_dir: &Path, rootfs_dir: &Path) -> io::Result<()> {
    let mut queue = VecDeque::from([(source_layer_dir.to_path_buf(), rootfs_dir.to_path_buf())]);

    while let Some((src_dir, dst_dir)) = queue.pop_front() {
        let mut entries: Vec<_> = fs::read_dir(&src_dir)?.collect::<io::Result<Vec<_>>>()?;

        // Apply whiteouts first so they are not negated by same-layer entries.
        for entry in entries.iter() {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if !file_name.starts_with(".wh.") {
                continue;
            }

            handle_whiteout(&dst_dir, &file_name)?;
        }

        // Process regular layer entries after whiteouts.
        while let Some(entry) = entries.pop() {
            let src = entry.path();
            let file_name_os = entry.file_name();
            let file_name = file_name_os.to_string_lossy();

            if file_name.starts_with(".wh.") {
                continue;
            }

            let metadata = fs::symlink_metadata(&src)?;
            let destination = dst_dir.join(file_name_os);

            if metadata.is_dir() {
                queue.push_back((src.clone(), destination.clone()));
                fs::create_dir_all(&destination)?;
                fs::set_permissions(&destination, metadata.permissions())?;
            } else if metadata.file_type().is_symlink() {
                copy_symlink(&src, &destination)?;
            } else if metadata.is_file() {
                let parent = destination.parent().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "layer entry has no parent")
                })?;
                fs::create_dir_all(parent)?;
                hard_link_or_copy_file(&src, &destination)?;
                fs::set_permissions(&destination, metadata.permissions())?;
            }
        }
    }

    Ok(())
}

fn handle_whiteout(parent: &Path, file_name: &str) -> io::Result<()> {
    if file_name == ".wh..wh..opq" {
        clear_directory(parent)
    } else {
        let target_name = file_name.strip_prefix(".wh.").ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid whiteout filename")
        })?;
        let target = parent.join(target_name);
        remove_path_if_exists(&target)
    }
}

fn clear_directory(directory: &Path) -> io::Result<()> {
    if !directory.exists() {
        return Ok(());
    }

    for child in fs::read_dir(directory)? {
        let child = child?;
        remove_path_if_exists(&child.path())?;
    }

    Ok(())
}

fn remove_path_if_exists(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                fs::remove_dir_all(path)
            } else {
                fs::remove_file(path)
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

#[cfg(unix)]
fn copy_symlink(source: &Path, destination: &Path) -> io::Result<()> {
    remove_path_if_exists(destination)?;
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }

    let target = fs::read_link(source)?;
    unix_fs::symlink(target, destination)
}

fn hard_link_or_copy_file(source: &Path, destination: &Path) -> io::Result<()> {
    remove_path_if_exists(destination)?;
    match fs::hard_link(source, destination) {
        Ok(()) => Ok(()),
        Err(_) => {
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)?;
            }

            let mut src = File::open(source)?;
            let mut dst = File::create(destination)?;
            io::copy(&mut src, &mut dst)?;
            Ok(())
        }
    }
}

#[derive(Debug, Deserialize)]
struct ManifestLayers {
    #[serde(default)]
    layers: Vec<ManifestLayerEntry>,
}

#[derive(Debug, Deserialize)]
struct ManifestLayerEntry {
    digest: String,
    #[serde(default, rename = "mediaType")]
    media_type: String,
}

fn parse_manifest_layers(raw_manifest: &[u8]) -> Result<Vec<LayerDescriptor>, &'static str> {
    let manifest: ManifestLayers =
        serde_json::from_slice(raw_manifest).map_err(|_| "manifest is not valid json")?;

    let layers = manifest
        .layers
        .into_iter()
        .map(|layer| LayerDescriptor {
            digest: layer.digest,
            media_type: if layer.media_type.is_empty() {
                "application/vnd.oci.image.layer.v1.tar".to_string()
            } else {
                layer.media_type
            },
        })
        .collect();

    Ok(layers)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use std::env;

    fn unique_temp_dir(name: &str) -> PathBuf {
        let mut base = env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        base.push(format!(
            "vz-image-store-test-{}-{}-{}",
            name,
            std::process::id(),
            nanos.as_nanos(),
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    #[test]
    fn whiteout_file_and_opaque_entries_apply_in_order() {
        let root = unique_temp_dir("whiteout");
        let mut layer1 = root.clone();
        layer1.push("layer1");
        let mut layer2 = root.clone();
        layer2.push("layer2");
        let rootfs = root.join("rootfs");
        fs::create_dir_all(&rootfs).unwrap();

        fs::create_dir_all(layer1.join("app")).unwrap();
        fs::write(layer1.join("app").join("keep"), b"keep").unwrap();
        fs::write(layer1.join("app").join("present"), b"present").unwrap();

        fs::create_dir_all(layer2.join("app")).unwrap();
        fs::write(layer2.join("app").join(".wh.keep"), b"").unwrap();
        fs::write(layer2.join("app").join(".wh..wh..opq"), b"").unwrap();
        fs::write(layer2.join("app").join("new"), b"new").unwrap();

        overlay_copy_layer(&layer1, &rootfs).unwrap();
        overlay_copy_layer(&layer2, &rootfs).unwrap();

        assert!(!rootfs.join("app").join("present").exists());
        assert!(!rootfs.join("app").join("keep").exists());
        assert_eq!(
            fs::read_to_string(rootfs.join("app").join("new")).unwrap(),
            "new"
        );
        assert!(!rootfs.join("app").join(".wh.keep").exists());
        assert!(!rootfs.join("app").join(".wh..wh..opq").exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn hard_link_or_copy_falls_back_when_linking_fails() {
        let root = unique_temp_dir("fallback");
        let source = root.join("src_file");
        let destination = root.join("nested").join("dest");

        fs::write(&source, b"payload").unwrap();
        hard_link_or_copy_file(&source, &destination).unwrap();

        assert_eq!(fs::read_to_string(&destination).unwrap(), "payload");
        assert!(destination.metadata().unwrap().is_file());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn list_images_reads_reference_mappings() {
        let root = unique_temp_dir("list-images");
        let store = ImageStore::new(root.clone());
        store.ensure_layout().unwrap();

        store
            .write_reference("library/ubuntu:24.04", "sha256:ubuntu")
            .unwrap();
        store
            .write_reference("alpine:3.22", "sha256:alpine")
            .unwrap();

        let images = store.list_images().unwrap();

        assert_eq!(images.len(), 2);
        assert_eq!(images[0].reference, "alpine:3.22");
        assert_eq!(images[0].image_id, "sha256:alpine");
        assert_eq!(images[1].reference, "library/ubuntu:24.04");
        assert_eq!(images[1].image_id, "sha256:ubuntu");

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prune_images_removes_unused_cached_objects() {
        let root = unique_temp_dir("prune-images");
        let store = ImageStore::new(root.clone());
        store.ensure_layout().unwrap();

        fn manifest_json(layers: &[&str]) -> Vec<u8> {
            let layers = layers
                .iter()
                .map(|layer| {
                    format!(
                        "{{\"digest\":\"{layer}\",\"mediaType\":\"application/vnd.oci.image.layer.v1.tar\"}}",
                    )
                })
                .collect::<Vec<_>>()
                .join(",");

            format!("{{\"layers\":[{layers}]}}").into_bytes()
        }

        store
            .write_manifest_json(
                "sha256:img-a",
                &manifest_json(&["sha256:layer-a", "sha256:layer-shared"]),
            )
            .unwrap();
        store
            .write_manifest_json("sha256:img-b", &manifest_json(&["sha256:layer-shared"]))
            .unwrap();
        store
            .write_manifest_json("sha256:img-c", &manifest_json(&["sha256:layer-orphan"]))
            .unwrap();

        store.write_config_json("sha256:img-a", br#"{}"#).unwrap();
        store.write_config_json("sha256:img-b", br#"{}"#).unwrap();
        store.write_config_json("sha256:img-c", br#"{}"#).unwrap();

        store
            .write_reference("ubuntu:24.04", "sha256:img-a")
            .unwrap();
        store
            .write_reference("alpine:3.22", "sha256:img-b")
            .unwrap();
        store
            .write_reference("stale:latest", "sha256:missing-manifest")
            .unwrap();

        fs::create_dir_all(store.unpacked_layer_dir("sha256:layer-a")).unwrap();
        fs::create_dir_all(store.unpacked_layer_dir("sha256:layer-shared")).unwrap();
        fs::create_dir_all(store.unpacked_layer_dir("sha256:layer-orphan")).unwrap();

        let result = store.prune_images().unwrap();

        assert_eq!(result.removed_refs, 1);
        assert_eq!(result.removed_manifests, 1);
        assert_eq!(result.removed_configs, 1);
        assert_eq!(result.removed_layer_dirs, 1);

        assert!(!store.manifest_path("sha256:img-c").exists());
        assert!(!store.config_path("sha256:img-c").exists());
        assert!(!store.unpacked_layer_dir("sha256:layer-orphan").exists());
        assert!(store.unpacked_layer_dir("sha256:layer-a").exists());
        assert!(store.unpacked_layer_dir("sha256:layer-shared").exists());

        fs::remove_dir_all(root).unwrap();
    }
}
