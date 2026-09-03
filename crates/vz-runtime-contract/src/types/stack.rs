use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Runtime-issued proof that one stack reserved a specific container-ID generation.
///
/// The tuple is intentionally generation-qualified: callers must never use the
/// container ID alone to clean up a failed create because a later lifecycle may
/// have reused the same ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerGenerationOwnership {
    /// Caller-selected runtime container identifier.
    pub container_id: String,
    /// Monotonic durable generation reserved for this create transaction.
    pub generation: u64,
    /// Stack/sandbox scope that reserved the generation.
    pub stack_id: String,
}

/// Successful container creation result with optional generation ownership proof.
///
/// Backends that implement generation-owned cleanup return `Some`; compatibility
/// backends may return `None` and therefore cannot authorize failed-create cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerCreateReceipt {
    /// Runtime container identifier returned by the backend.
    pub container_id: String,
    /// Runtime-issued generation ownership, when supported by the backend.
    pub ownership: Option<ContainerGenerationOwnership>,
}

/// Container creation failure that may retain cleanup ownership.
///
/// `cleanup` is present only when the backend actually admitted the create and
/// reserved the reported generation. Admission failures such as a foreign
/// duplicate must return `None`.
#[derive(Debug)]
pub struct OwnedCreateError<E> {
    /// Underlying backend or adapter error.
    pub error: E,
    /// Exact failed generation the caller may attempt to clean up.
    pub cleanup: Option<ContainerGenerationOwnership>,
}

impl<E> OwnedCreateError<E> {
    /// Construct a failure that carries no cleanup authority.
    pub fn unowned(error: E) -> Self {
        Self {
            error,
            cleanup: None,
        }
    }

    /// Transform the underlying error while preserving cleanup ownership.
    pub fn map_error<T>(self, map: impl FnOnce(E) -> T) -> OwnedCreateError<T> {
        OwnedCreateError {
            error: map(self.error),
            cleanup: self.cleanup,
        }
    }
}

impl<E: std::fmt::Display> std::fmt::Display for OwnedCreateError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl<E: std::error::Error + 'static> std::error::Error for OwnedCreateError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Result of generation-qualified failed-create cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationCleanupOutcome {
    /// The exact owned generation and its artifacts were removed.
    Removed,
    /// The generation was already fully absent and no replacement was touched.
    AlreadyAbsent,
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
