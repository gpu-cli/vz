# 02 — OCI Image Pulling, Unpacking & Storage

## What's an OCI Image

An OCI (Open Container Initiative) image is a standardized package format for container filesystems. It consists of:

1. **Manifest** — JSON describing the image (layers, config, platform)
2. **Config** — JSON with entrypoint, env vars, working dir, exposed ports
3. **Layers** — Compressed tarballs (gzip or zstd) of filesystem diffs, stacked bottom-to-top

When you `docker pull ubuntu:24.04`, you're downloading a manifest, a config, and N layer tarballs from a registry. The registry speaks the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec) — a REST API over HTTPS.

## Image Pulling

### Registry Protocol

Image references follow the standard format:

```
[registry/][namespace/]name[:tag|@digest]

Examples:
  ubuntu:24.04                    → docker.io/library/ubuntu:24.04
  python:3.12-slim                → docker.io/library/python:3.12-slim
  ghcr.io/owner/repo:latest       → ghcr.io/owner/repo:latest
  registry.example.com/app:v1.2   → registry.example.com/app:v1.2
```

Default registry is `docker.io` (Docker Hub). Default tag is `latest`.

### Rust Implementation

Use the `oci-distribution` crate for registry interaction:

```rust
use oci_distribution::{Client, Reference, secrets::RegistryAuth};
use oci_distribution::manifest::OciImageManifest;

pub struct ImagePuller {
    client: Client,
    store: ImageStore,
}

impl ImagePuller {
    /// Pull an image from a registry. Returns the local image ID.
    /// If the image is already cached (all layers present), this is a no-op.
    pub async fn pull(&self, reference: &str, auth: &RegistryAuth) -> Result<ImageId> {
        let reference: Reference = reference.parse()?;

        // 1. Fetch manifest
        let (manifest, digest) = self.client
            .pull_manifest(&reference, auth)
            .await?;

        // 2. Check if all layers are already cached
        if self.store.has_all_layers(&manifest) {
            tracing::info!("Image already cached, skipping download");
            return Ok(ImageId(digest));
        }

        // 3. Pull missing layers (parallel)
        let layers = manifest.layers();
        let mut tasks = Vec::new();
        for layer in layers {
            if !self.store.has_layer(&layer.digest) {
                let client = self.client.clone();
                let reference = reference.clone();
                let store = self.store.clone();
                tasks.push(tokio::spawn(async move {
                    let data = client.pull_blob(&reference, &layer).await?;
                    store.write_layer(&layer.digest, &data).await?;
                    Ok::<_, anyhow::Error>(())
                }));
            }
        }
        futures::future::try_join_all(tasks).await?;

        // 4. Pull config
        let config = self.client.pull_blob(&reference, &manifest.config()).await?;
        self.store.write_config(&digest, &config).await?;

        // 5. Write manifest
        self.store.write_manifest(&digest, &manifest).await?;

        Ok(ImageId(digest))
    }
}
```

### Authentication

```rust
pub enum Auth {
    /// No authentication (public images)
    Anonymous,

    /// Username + password (or token)
    Basic { username: String, password: String },

    /// Docker config.json (~/.docker/config.json)
    /// Reads stored credentials for the target registry
    DockerConfig,
}
```

The `DockerConfig` variant reads `~/.docker/config.json`, which most developers already have configured from using Docker. This means `vz pull ghcr.io/private/repo` works if the user has previously `docker login ghcr.io`.

### Platform Filtering

OCI images can be multi-platform (manifest list / image index). We filter for `linux/arm64`:

```rust
let platform = Platform {
    os: "linux".to_string(),
    architecture: "arm64".to_string(),
    variant: Some("v8".to_string()),
    ..Default::default()
};
```

If a `linux/arm64` variant is not available, the pull fails with a clear error:

```
Error: Image "some-image:latest" does not have a linux/arm64 variant.
Available platforms: linux/amd64, linux/arm/v7

vz only supports arm64 images on Apple Silicon.
```

### Pull UX

```
vz pull python:3.12-slim

  Pulling python:3.12-slim from docker.io...

  ✓ Manifest     1.2 KB
  ✓ Config       5.4 KB
  ● Layer 1/4    ████████████████████░░░░░░░░░░  67%   18 MB / 27 MB  •  12 MB/s
  ○ Layer 2/4    (queued)
  ○ Layer 3/4    (queued)
  ○ Layer 4/4    (queued)

  Destination: ~/.vz/oci/
```

Layers are pulled in parallel (up to 4 concurrent downloads). Progress is shown per-layer.

## Layer Storage

### Layout

```
~/.vz/oci/
├── manifests/
│   └── <digest>.json                    # OCI manifest
├── configs/
│   └── <digest>.json                    # OCI image config
├── layers/
│   ├── <digest>.tar.gz                  # Compressed layer tarballs
│   └── <digest>/                        # Unpacked layer directory
│       ├── usr/
│       ├── etc/
│       └── ...
├── refs/
│   └── docker.io/library/python/3.12-slim → <digest>  # Tag-to-digest mapping
└── rootfs/
    └── <container-id>/                  # Assembled rootfs for running containers
        ├── usr/
        ├── etc/
        └── ...
```

### Content-Addressable Storage

Layers are stored by their content digest (SHA256). This means:
- Identical layers across different images are stored once
- `python:3.12-slim` and `python:3.12` share most layers
- Deduplication is automatic

### Layer Unpacking

Compressed layers (`.tar.gz`) are unpacked into directories for VirtioFS mounting:

```rust
impl ImageStore {
    /// Unpack a layer tarball into a directory
    pub async fn unpack_layer(&self, digest: &str) -> Result<PathBuf> {
        let tarball = self.layer_path(digest);
        let output_dir = self.unpacked_layer_path(digest);

        if output_dir.exists() {
            return Ok(output_dir); // already unpacked
        }

        let file = tokio::fs::File::open(&tarball).await?;
        let decoder = async_compression::tokio::bufread::GzipDecoder::new(
            tokio::io::BufReader::new(file)
        );
        let mut archive = tokio_tar::Archive::new(decoder);
        archive.unpack(&output_dir).await?;

        Ok(output_dir)
    }
}
```

### Whiteout Files

OCI layers use "whiteout" files to represent deletions:
- `.wh.filename` — Delete `filename` from lower layers
- `.wh..wh..opq` — Delete all contents of the directory from lower layers

These are handled during rootfs assembly (see below), not during layer unpacking.

## Rootfs Assembly

### The Problem

An OCI image has N layers that stack bottom-to-top. Each layer adds, modifies, or deletes files from the layers below it. The final filesystem is the union of all layers with whiteouts applied.

### Strategy: Host-Side Assembly + VirtioFS

Assemble the final rootfs on the host and mount it into the VM via VirtioFS:

```rust
impl ImageStore {
    /// Assemble a rootfs from image layers.
    /// Returns the path to the assembled rootfs directory.
    pub async fn assemble_rootfs(
        &self,
        image_id: &ImageId,
        container_id: &str,
    ) -> Result<PathBuf> {
        let manifest = self.read_manifest(image_id).await?;
        let rootfs_dir = self.rootfs_path(container_id);

        // Ensure all layers are unpacked
        for layer in manifest.layers() {
            self.unpack_layer(&layer.digest).await?;
        }

        // Stack layers bottom-to-top using hard links (fast, space-efficient)
        for layer in manifest.layers() {
            let layer_dir = self.unpacked_layer_path(&layer.digest);
            overlay_copy(layer_dir, rootfs_dir.clone()).await?;
        }

        Ok(rootfs_dir)
    }
}

/// Copy layer contents into rootfs, handling whiteouts, symlinks, and permissions.
///
/// This function uses synchronous `walkdir` for filesystem traversal, so it is
/// wrapped in `tokio::task::spawn_blocking` by the caller to avoid blocking the
/// tokio runtime. For large images with thousands of files, this is I/O-bound
/// and benefits from running on a blocking thread.
fn overlay_copy_blocking(layer: &Path, rootfs: &Path) -> Result<()> {
    // follow_links(false) is critical: OCI layers contain symlinks that must
    // be preserved as symlinks, not followed (which could cause infinite loops).
    for entry in walkdir::WalkDir::new(layer).follow_links(false) {
        let entry = entry?;
        let relative = entry.path().strip_prefix(layer)?;
        let filename = entry.file_name().to_string_lossy();

        if filename.starts_with(".wh.") {
            // Whiteout: delete the target file/dir from rootfs
            if filename == ".wh..wh..opq" {
                // Opaque whiteout: delete all contents of parent dir, preserving dir metadata
                let parent = rootfs.join(relative.parent().unwrap_or(Path::new("")));
                if parent.exists() {
                    let metadata = std::fs::metadata(&parent)?;
                    // Delete contents, not the directory itself
                    for child in std::fs::read_dir(&parent)? {
                        let child = child?;
                        if child.file_type()?.is_dir() {
                            std::fs::remove_dir_all(child.path())?;
                        } else {
                            std::fs::remove_file(child.path())?;
                        }
                    }
                    // Restore original permissions
                    std::fs::set_permissions(&parent, metadata.permissions())?;
                }
            } else {
                // File whiteout: delete the specific file
                let target_name = filename.strip_prefix(".wh.").unwrap();
                let target = rootfs.join(relative.parent().unwrap_or(Path::new(""))).join(target_name);
                if target.exists() {
                    if target.is_dir() {
                        std::fs::remove_dir_all(&target)?;
                    } else {
                        std::fs::remove_file(&target)?;
                    }
                }
            }
        } else if entry.file_type().is_symlink() {
            // Preserve symlinks as-is (do not follow them)
            let link_target = std::fs::read_link(entry.path())?;
            let dest = rootfs.join(relative);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            // Remove existing file/symlink at destination if present
            let _ = std::fs::remove_file(&dest);
            std::os::unix::fs::symlink(&link_target, &dest)?;
        } else if entry.file_type().is_dir() {
            let dest = rootfs.join(relative);
            std::fs::create_dir_all(&dest)?;
            // Preserve directory permissions from the layer
            let metadata = std::fs::metadata(entry.path())?;
            std::fs::set_permissions(&dest, metadata.permissions())?;
        } else if entry.file_type().is_file() {
            let dest = rootfs.join(relative);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            hard_link_or_copy(entry.path(), &dest)?;
            // Preserve file permissions (including setuid/setgid bits)
            let metadata = std::fs::metadata(entry.path())?;
            std::fs::set_permissions(&dest, metadata.permissions())?;
        }
        // Skip device nodes, FIFOs, sockets — these are uncommon in OCI layers
        // and cannot be created without root. The VM root handles /dev via devtmpfs.
    }
    Ok(())
}

/// Try hard link first (fast, space-efficient when on same filesystem),
/// fall back to copy if hard link fails (e.g., cross-device).
fn hard_link_or_copy(src: &Path, dest: &Path) -> Result<()> {
    // Remove existing file at destination to allow hard link
    let _ = std::fs::remove_file(dest);
    match std::fs::hard_link(src, dest) {
        Ok(()) => Ok(()),
        Err(_) => {
            std::fs::copy(src, dest)?;
            Ok(())
        }
    }
}

/// Async wrapper that runs overlay_copy on a blocking thread.
async fn overlay_copy(layer: PathBuf, rootfs: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || overlay_copy_blocking(&layer, &rootfs)).await??;
    Ok(())
}
```

### Why Not Overlayfs on Host

macOS does not have overlayfs. The host-side assembly uses a flat copy (with hard links for speed). Overlayfs is used *inside* the Linux VM for the writable layer on top of the read-only rootfs.

### Writable Layer

The assembled rootfs is mounted read-only into the VM via VirtioFS. The VM's init script sets up an overlayfs with a tmpfs upper layer for writes:

```
Lower (read-only):   /mnt/rootfs  ← VirtioFS from host
Upper (read-write):  /mnt/overlay-work/upper  ← tmpfs inside VM
Merged:              /mnt/merged  ← what the container sees
```

Container writes go to tmpfs and are lost when the VM stops. This is the standard ephemeral container behavior.

### Persistent Volumes

For containers that need persistent storage, bind mounts are passed through as additional VirtioFS shares:

```rust
RunConfig {
    mounts: vec![
        Mount::bind("./data", "/data"),       // host:./data → guest:/data
        Mount::bind("./output", "/output"),
    ],
    ..Default::default()
}
```

Each bind mount becomes a separate VirtioFS `SharedDirConfig` configured at VM creation time.

## Image Config

The OCI image config contains the container's default behavior:

```json
{
  "config": {
    "Entrypoint": ["/usr/bin/python3"],
    "Cmd": ["--version"],
    "Env": ["PATH=/usr/local/bin:/usr/bin:/bin", "PYTHON_VERSION=3.12"],
    "WorkingDir": "/app",
    "User": "nobody",
    "ExposedPorts": { "8080/tcp": {} }
  }
}
```

We extract and apply:

| Field | How it's used |
|-------|--------------|
| `Entrypoint` + `Cmd` | Sent as the initial `Exec` request to the guest agent after handshake |
| `Env` | Passed in `Exec.env` field |
| `WorkingDir` | Passed in `Exec.working_dir` field |
| `User` | Passed in `Exec.user` field |
| `ExposedPorts` | Used for default port forwarding if configured |

## Image Management

### List Images

```
vz images

  REPOSITORY            TAG           SIZE      PULLED
  python                3.12-slim     52 MB     2 hours ago
  ubuntu                24.04         78 MB     1 day ago
  node                  22-alpine     45 MB     3 days ago

  Total: 175 MB (89 MB deduplicated across shared layers)
```

### Prune Unused Images

```
vz images prune

  Removing unused layers...
  Freed 124 MB (3 images, 8 layers)
```

Prune removes:
- Images with no running containers referencing them
- Unpacked layer directories (compressed tarballs are kept for faster re-assembly)
- Assembled rootfs directories for stopped containers

### Orphaned Rootfs Cleanup

If the host process crashes, assembled rootfs directories at `~/.vz/oci/rootfs/<container-id>/` are orphaned. On `Runtime::new()`, the runtime scans this directory and deletes any rootfs not associated with an active entry in `containers.json`. This prevents disk space leaks from crashed containers.

### Cache Location

All OCI data lives under `~/.vz/oci/`. This is separate from macOS VM data (`~/.vz/images/`, `~/.vz/states/`).

```
~/.vz/
├── images/          # macOS golden images
├── states/          # macOS saved states
├── linux/           # Linux kernel + initramfs
├── oci/             # OCI image data (layers, manifests, rootfs)
└── cache/           # IPSW cache
```
