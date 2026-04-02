use std::path::PathBuf;

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

// ── Network types ─────────────────────────────────────────────────

/// Per-service network configuration for stack networking.
///
/// Each entry represents one service on one network. A service that belongs
/// to multiple custom networks will have multiple `NetworkServiceConfig`
/// entries (one per network), each with a different `network_name` and
/// subnet-specific `addr`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkServiceConfig {
    /// Service name.
    pub name: String,
    /// IP address assigned to this service (CIDR, e.g., `"172.20.0.2/24"`).
    pub addr: String,
    /// Logical network this entry belongs to (e.g., `"default"`, `"frontend"`).
    pub network_name: String,
}

/// Aggregate resource hints for sizing a shared stack VM.
///
/// When multiple services define CPU/memory limits, the stack executor
/// computes an aggregate and passes it to the runtime backend so the
/// shared VM gets enough CPU cores and memory.
#[derive(Debug, Clone, Default)]
pub struct StackResourceHint {
    /// Suggested CPU cores for the VM (max of all service limits, ceiling).
    pub cpus: Option<u8>,
    /// Suggested memory in MB for the VM (sum of all service limits).
    pub memory_mb: Option<u64>,
    /// Host directories to share as VirtioFS mounts inside the VM.
    ///
    /// Each entry is `(tag, host_path, read_only)`. The tag is used as the
    /// VirtioFS mount tag and the init script mounts it at `/mnt/{tag}`.
    /// Named volumes and bind mounts from all services are collected here
    /// so the shared VM can set them up at boot time (VirtioFS shares are
    /// static and must be configured before the VM starts).
    pub volume_mounts: Vec<StackVolumeMount>,
    /// Optional path to a disk image to attach as a VirtioBlock device.
    ///
    /// Used for persistent named volumes: the image contains an ext4
    /// filesystem mounted at `/run/vz-oci/volumes` inside the guest VM.
    pub disk_image_path: Option<PathBuf>,
}

/// A host directory to expose inside the shared VM via VirtioFS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackVolumeMount {
    /// VirtioFS mount tag (e.g., `"vz-mount-0"`).
    pub tag: String,
    /// Absolute path on the host.
    pub host_path: std::path::PathBuf,
    /// Target path inside the guest where this mount should appear.
    ///
    /// When set, the init script bind-mounts the VirtioFS share from
    /// `/mnt/{tag}` to this path inside the chroot. Communicated to the
    /// guest via kernel cmdline parameter `vz.mount.{N}={guest_path}`.
    pub guest_path: Option<String>,
    /// Whether the mount is read-only.
    pub read_only: bool,
}

/// Container log output.
#[derive(Debug, Clone, Default)]
pub struct ContainerLogs {
    /// Combined stdout/stderr output.
    pub output: String,
}
