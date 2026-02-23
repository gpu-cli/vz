//! Volume planner for bind, named, and ephemeral mount specifications.
//!
//! Resolves [`MountSpec`](crate::MountSpec) entries from the stack spec
//! into concrete host paths, manages named volume lifecycle, and detects
//! mount configuration changes that require service recreation.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::StackError;
use crate::spec::{MountSpec, VolumeSpec};

/// Resolved mount ready for runtime consumption.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedMount {
    /// Host source path (absolute).
    pub host_path: Option<PathBuf>,
    /// Container destination path.
    pub target: String,
    /// Whether the mount is read-only.
    pub read_only: bool,
    /// Mount kind.
    pub kind: ResolvedMountKind,
    /// For file bind mounts: the filename within the VirtioFS-shared parent directory.
    ///
    /// When set, `host_path` points to the parent directory (not the file itself).
    pub subpath: Option<String>,
}

/// Kind of a resolved mount.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResolvedMountKind {
    /// Bind mount from host filesystem.
    Bind,
    /// Named volume with managed host directory.
    Named {
        /// Volume name.
        volume_name: String,
    },
    /// Ephemeral tmpfs mount (no host path).
    Ephemeral,
}

/// Resolve mount specifications to concrete host paths.
///
/// Named volumes are placed under `volumes_dir/<volume_name>`.
/// Bind mounts use the source path as-is.
/// Ephemeral mounts have no host path.
pub fn resolve_mounts(
    mounts: &[MountSpec],
    volumes: &[VolumeSpec],
    volumes_dir: &Path,
) -> Result<Vec<ResolvedMount>, StackError> {
    let volume_names: HashSet<&str> = volumes.iter().map(|v| v.name.as_str()).collect();
    let mut resolved = Vec::new();

    for mount in mounts {
        match mount {
            MountSpec::Bind {
                source,
                target,
                read_only,
            } => {
                resolved.push(ResolvedMount {
                    host_path: Some(PathBuf::from(source)),
                    target: target.clone(),
                    read_only: *read_only,
                    kind: ResolvedMountKind::Bind,
                    subpath: None,
                });
            }
            MountSpec::Named {
                source,
                target,
                read_only,
            } => {
                if !volume_names.contains(source.as_str()) {
                    return Err(StackError::InvalidSpec(format!(
                        "mount references undefined volume '{source}'"
                    )));
                }
                let host_path = volumes_dir.join(source);
                resolved.push(ResolvedMount {
                    host_path: Some(host_path),
                    target: target.clone(),
                    read_only: *read_only,
                    kind: ResolvedMountKind::Named {
                        volume_name: source.clone(),
                    },
                    subpath: None,
                });
            }
            MountSpec::Ephemeral { target } => {
                resolved.push(ResolvedMount {
                    host_path: None,
                    target: target.clone(),
                    read_only: false,
                    kind: ResolvedMountKind::Ephemeral,
                    subpath: None,
                });
            }
        }
    }

    Ok(resolved)
}

/// Validate bind mount source paths and handle file/socket sources.
///
/// - **Non-existent source** → error with clear message
/// - **Regular file** → set `host_path` to parent dir, `subpath` to filename
/// - **Socket/pipe/device** → remove from list with warning
/// - **Directory** → resolve to absolute path, pass through
///
/// Must be called after [`resolve_mounts`] and before the executor builds
/// VirtioFS tags, so that filtered entries don't consume tag indices.
pub fn validate_bind_mounts(resolved: &mut Vec<ResolvedMount>) -> Result<(), StackError> {
    use std::os::unix::fs::FileTypeExt;

    let mut i = 0;
    while i < resolved.len() {
        if resolved[i].kind != ResolvedMountKind::Bind {
            i += 1;
            continue;
        }
        let host_path = match &resolved[i].host_path {
            Some(p) => p.clone(),
            None => {
                i += 1;
                continue;
            }
        };

        let absolute = if host_path.is_relative() {
            std::env::current_dir()
                .map_err(|e| StackError::InvalidSpec(format!("cannot resolve CWD: {e}")))?
                .join(&host_path)
        } else {
            host_path.clone()
        };

        if !absolute.exists() {
            return Err(StackError::InvalidSpec(format!(
                "bind mount source does not exist: {}",
                host_path.display()
            )));
        }

        let metadata = std::fs::metadata(&absolute).map_err(|e| {
            StackError::InvalidSpec(format!(
                "cannot stat bind mount source {}: {e}",
                absolute.display()
            ))
        })?;

        if metadata.is_file() {
            let parent = absolute.parent().ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "file bind mount has no parent directory: {}",
                    absolute.display()
                ))
            })?;
            let filename = absolute.file_name().ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "file bind mount has no filename: {}",
                    absolute.display()
                ))
            })?;
            resolved[i].host_path = Some(parent.to_path_buf());
            resolved[i].subpath = Some(filename.to_string_lossy().into_owned());
            i += 1;
        } else if metadata.is_dir() {
            resolved[i].host_path = Some(absolute);
            i += 1;
        } else {
            // Socket, pipe, device, etc. — VirtioFS cannot share these.
            let ft = metadata.file_type();
            let kind = if ft.is_socket() {
                "socket"
            } else if ft.is_fifo() {
                "named pipe"
            } else if ft.is_block_device() {
                "block device"
            } else if ft.is_char_device() {
                "character device"
            } else {
                "unsupported file type"
            };
            tracing::warn!(
                source = %host_path.display(),
                target = %resolved[i].target,
                kind,
                "skipping bind mount: source is a {kind} (VirtioFS only supports files and directories)"
            );
            resolved.remove(i);
        }
    }
    Ok(())
}

/// Detect whether mount configurations have changed between two service specs.
///
/// Returns `true` if mounts differ, which should trigger a service recreate.
pub fn mounts_changed(old: &[MountSpec], new: &[MountSpec]) -> bool {
    old != new
}

/// Collect named volume names referenced by a set of services.
pub fn referenced_volume_names<'a>(mounts: impl Iterator<Item = &'a MountSpec>) -> HashSet<String> {
    mounts
        .filter_map(|m| match m {
            MountSpec::Named { source, .. } => Some(source.clone()),
            _ => None,
        })
        .collect()
}

/// Determine which named volumes are orphaned (defined but not referenced).
pub fn orphaned_volumes(defined: &[VolumeSpec], referenced: &HashSet<String>) -> Vec<String> {
    defined
        .iter()
        .filter(|v| !referenced.contains(&v.name))
        .map(|v| v.name.clone())
        .collect()
}

/// Default size for the sparse disk image (10 GiB).
const DEFAULT_DISK_IMAGE_SIZE: u64 = 10 * 1024 * 1024 * 1024;

/// Manages on-disk volume directories for a stack.
///
/// Named volumes are stored as directories under a stack-scoped data dir:
/// `<base_dir>/<stack_name>/volumes/<volume_name>/`.
///
/// Persistent volume data is stored in a sparse ext4 disk image at
/// `<base_dir>/<stack_name>/data.img`, attached as a VirtioBlock device.
pub struct VolumeManager {
    /// Root directory for this stack's volumes.
    volumes_dir: PathBuf,
    /// Root directory for this stack's data (parent of volumes_dir).
    stack_data_dir: PathBuf,
}

impl VolumeManager {
    /// Create a new volume manager for the given stack data directory.
    ///
    /// The volumes subdirectory is `<stack_data_dir>/volumes/`.
    pub fn new(stack_data_dir: &Path) -> Self {
        Self {
            volumes_dir: stack_data_dir.join("volumes"),
            stack_data_dir: stack_data_dir.to_path_buf(),
        }
    }

    /// Root directory where volume subdirectories are stored.
    pub fn volumes_dir(&self) -> &Path {
        &self.volumes_dir
    }

    /// Ensure all named volumes have their directories created.
    ///
    /// Returns the list of volume names that were newly created.
    pub fn ensure_volumes(&self, volumes: &[VolumeSpec]) -> Result<Vec<String>, StackError> {
        let mut created = Vec::new();
        for vol in volumes {
            let dir = self.volumes_dir.join(&vol.name);
            if !dir.exists() {
                std::fs::create_dir_all(&dir)?;
                info!(volume = %vol.name, path = %dir.display(), "created volume directory");
                created.push(vol.name.clone());
            }
        }
        Ok(created)
    }

    /// Ensure a single named volume directory exists.
    ///
    /// Returns `true` if the directory was newly created.
    pub fn ensure_volume(&self, name: &str) -> Result<bool, StackError> {
        let dir = self.volumes_dir.join(name);
        if dir.exists() {
            return Ok(false);
        }
        std::fs::create_dir_all(&dir)?;
        info!(volume = %name, path = %dir.display(), "created volume directory");
        Ok(true)
    }

    /// Remove a named volume directory and its contents.
    ///
    /// Returns `true` if the directory existed and was removed.
    pub fn remove_volume(&self, name: &str) -> Result<bool, StackError> {
        let dir = self.volumes_dir.join(name);
        if !dir.exists() {
            return Ok(false);
        }
        std::fs::remove_dir_all(&dir)?;
        info!(volume = %name, path = %dir.display(), "removed volume directory");
        Ok(true)
    }

    /// Remove all volumes that are orphaned (defined but not referenced).
    ///
    /// Returns the list of volume names that were removed.
    pub fn remove_orphaned(
        &self,
        defined: &[VolumeSpec],
        referenced: &HashSet<String>,
    ) -> Result<Vec<String>, StackError> {
        let orphans = orphaned_volumes(defined, referenced);
        let mut removed = Vec::new();
        for name in orphans {
            if self.remove_volume(&name)? {
                removed.push(name);
            }
        }
        Ok(removed)
    }

    /// Remove all volume directories and the disk image for this stack.
    ///
    /// Returns the number of volume directories removed.
    pub fn remove_all(&self) -> Result<usize, StackError> {
        // Remove the persistent disk image.
        self.remove_disk_image()?;

        if !self.volumes_dir.exists() {
            return Ok(0);
        }
        let mut count = 0;
        for entry in std::fs::read_dir(&self.volumes_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                std::fs::remove_dir_all(entry.path())?;
                count += 1;
            }
        }
        Ok(count)
    }

    /// List all volume directories that currently exist on disk.
    pub fn list_volumes(&self) -> Result<Vec<String>, StackError> {
        if !self.volumes_dir.exists() {
            return Ok(Vec::new());
        }
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&self.volumes_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    names.push(name.to_string());
                }
            }
        }
        names.sort();
        Ok(names)
    }

    /// Path to the persistent disk image for named volumes.
    pub fn disk_image_path(&self) -> PathBuf {
        self.stack_data_dir.join("data.img")
    }

    /// Ensure the sparse disk image exists.
    ///
    /// Creates a sparse file of the given `size_bytes` if it does not
    /// already exist. Pass `None` for the default (10 GiB).
    /// Returns `true` if a new image was created.
    pub fn ensure_disk_image(&self, size_bytes: Option<u64>) -> Result<bool, StackError> {
        let path = self.disk_image_path();
        if path.exists() {
            return Ok(false);
        }
        std::fs::create_dir_all(&self.stack_data_dir)?;
        let file = std::fs::File::create(&path)?;
        let actual_size = size_bytes.unwrap_or(DEFAULT_DISK_IMAGE_SIZE);
        file.set_len(actual_size)?;
        info!(path = %path.display(), size_bytes = actual_size, "created sparse disk image");
        Ok(true)
    }

    /// Check whether the disk image exists.
    pub fn has_disk_image(&self) -> bool {
        self.disk_image_path().exists()
    }

    /// Remove the disk image.
    ///
    /// Returns `true` if the image existed and was removed.
    pub fn remove_disk_image(&self) -> Result<bool, StackError> {
        let path = self.disk_image_path();
        if !path.exists() {
            return Ok(false);
        }
        std::fs::remove_file(&path)?;
        info!(path = %path.display(), "removed disk image");
        Ok(true)
    }

    /// Resolve mount specs using this manager's volumes directory.
    pub fn resolve_mounts(
        &self,
        mounts: &[MountSpec],
        volumes: &[VolumeSpec],
    ) -> Result<Vec<ResolvedMount>, StackError> {
        resolve_mounts(mounts, volumes, &self.volumes_dir)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use std::path::PathBuf;

    #[test]
    fn resolve_bind_mount() {
        let mounts = vec![MountSpec::Bind {
            source: "/host/data".to_string(),
            target: "/container/data".to_string(),
            read_only: true,
        }];
        let volumes = vec![];
        let dir = PathBuf::from("/volumes");

        let resolved = resolve_mounts(&mounts, &volumes, &dir).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].host_path, Some(PathBuf::from("/host/data")));
        assert_eq!(resolved[0].target, "/container/data");
        assert!(resolved[0].read_only);
        assert_eq!(resolved[0].kind, ResolvedMountKind::Bind);
    }

    #[test]
    fn resolve_named_volume() {
        let mounts = vec![MountSpec::Named {
            source: "dbdata".to_string(),
            target: "/var/lib/postgres".to_string(),
            read_only: false,
        }];
        let volumes = vec![VolumeSpec {
            name: "dbdata".to_string(),
            driver: "local".to_string(),
            driver_opts: None,
        }];
        let dir = PathBuf::from("/volumes");

        let resolved = resolve_mounts(&mounts, &volumes, &dir).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(
            resolved[0].host_path,
            Some(PathBuf::from("/volumes/dbdata"))
        );
        assert_eq!(resolved[0].target, "/var/lib/postgres");
        assert!(!resolved[0].read_only);
        assert_eq!(
            resolved[0].kind,
            ResolvedMountKind::Named {
                volume_name: "dbdata".to_string()
            }
        );
    }

    #[test]
    fn resolve_ephemeral_mount() {
        let mounts = vec![MountSpec::Ephemeral {
            target: "/tmp".to_string(),
        }];
        let volumes = vec![];
        let dir = PathBuf::from("/volumes");

        let resolved = resolve_mounts(&mounts, &volumes, &dir).unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(resolved[0].host_path.is_none());
        assert_eq!(resolved[0].target, "/tmp");
        assert_eq!(resolved[0].kind, ResolvedMountKind::Ephemeral);
    }

    #[test]
    fn resolve_rejects_undefined_volume() {
        let mounts = vec![MountSpec::Named {
            source: "missing".to_string(),
            target: "/data".to_string(),
            read_only: false,
        }];
        let volumes = vec![];
        let dir = PathBuf::from("/volumes");

        let err = resolve_mounts(&mounts, &volumes, &dir).unwrap_err();
        assert!(matches!(err, StackError::InvalidSpec(_)));
    }

    #[test]
    fn mounts_changed_detects_addition() {
        let old = vec![];
        let new = vec![MountSpec::Bind {
            source: "/a".to_string(),
            target: "/b".to_string(),
            read_only: false,
        }];
        assert!(mounts_changed(&old, &new));
    }

    #[test]
    fn mounts_changed_detects_removal() {
        let old = vec![MountSpec::Bind {
            source: "/a".to_string(),
            target: "/b".to_string(),
            read_only: false,
        }];
        let new = vec![];
        assert!(mounts_changed(&old, &new));
    }

    #[test]
    fn mounts_changed_detects_access_mode_change() {
        let old = vec![MountSpec::Bind {
            source: "/a".to_string(),
            target: "/b".to_string(),
            read_only: false,
        }];
        let new = vec![MountSpec::Bind {
            source: "/a".to_string(),
            target: "/b".to_string(),
            read_only: true,
        }];
        assert!(mounts_changed(&old, &new));
    }

    #[test]
    fn mounts_unchanged_returns_false() {
        let mounts = vec![MountSpec::Bind {
            source: "/a".to_string(),
            target: "/b".to_string(),
            read_only: false,
        }];
        assert!(!mounts_changed(&mounts, &mounts));
    }

    #[test]
    fn referenced_volume_names_collects_named_only() {
        let mounts = [
            MountSpec::Bind {
                source: "/host".to_string(),
                target: "/a".to_string(),
                read_only: false,
            },
            MountSpec::Named {
                source: "vol1".to_string(),
                target: "/b".to_string(),
                read_only: false,
            },
            MountSpec::Ephemeral {
                target: "/tmp".to_string(),
            },
            MountSpec::Named {
                source: "vol2".to_string(),
                target: "/c".to_string(),
                read_only: true,
            },
        ];

        let names = referenced_volume_names(mounts.iter());
        assert_eq!(names.len(), 2);
        assert!(names.contains("vol1"));
        assert!(names.contains("vol2"));
    }

    #[test]
    fn orphaned_volumes_finds_unreferenced() {
        let defined = vec![
            VolumeSpec {
                name: "used".to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            },
            VolumeSpec {
                name: "orphan".to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            },
        ];
        let referenced: HashSet<String> = ["used".to_string()].into();

        let orphans = orphaned_volumes(&defined, &referenced);
        assert_eq!(orphans, vec!["orphan".to_string()]);
    }

    #[test]
    fn orphaned_volumes_empty_when_all_used() {
        let defined = vec![VolumeSpec {
            name: "v1".to_string(),
            driver: "local".to_string(),
            driver_opts: None,
        }];
        let referenced: HashSet<String> = ["v1".to_string()].into();

        let orphans = orphaned_volumes(&defined, &referenced);
        assert!(orphans.is_empty());
    }

    #[test]
    fn resolve_mixed_mounts() {
        let mounts = vec![
            MountSpec::Bind {
                source: "/host/src".to_string(),
                target: "/workspace".to_string(),
                read_only: false,
            },
            MountSpec::Named {
                source: "cache".to_string(),
                target: "/cache".to_string(),
                read_only: false,
            },
            MountSpec::Ephemeral {
                target: "/tmp".to_string(),
            },
        ];
        let volumes = vec![VolumeSpec {
            name: "cache".to_string(),
            driver: "local".to_string(),
            driver_opts: None,
        }];
        let dir = PathBuf::from("/data/volumes");

        let resolved = resolve_mounts(&mounts, &volumes, &dir).unwrap();
        assert_eq!(resolved.len(), 3);
        assert_eq!(resolved[0].kind, ResolvedMountKind::Bind);
        assert_eq!(
            resolved[1].kind,
            ResolvedMountKind::Named {
                volume_name: "cache".to_string()
            }
        );
        assert_eq!(
            resolved[1].host_path,
            Some(PathBuf::from("/data/volumes/cache"))
        );
        assert_eq!(resolved[2].kind, ResolvedMountKind::Ephemeral);
    }

    // --- VolumeManager tests ---

    fn test_volumes() -> Vec<VolumeSpec> {
        vec![
            VolumeSpec {
                name: "dbdata".to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            },
            VolumeSpec {
                name: "cache".to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            },
        ]
    }

    #[test]
    fn manager_ensure_creates_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        let created = mgr.ensure_volumes(&test_volumes()).unwrap();
        assert_eq!(created.len(), 2);
        assert!(created.contains(&"dbdata".to_string()));
        assert!(created.contains(&"cache".to_string()));

        // Directories exist on disk.
        assert!(mgr.volumes_dir().join("dbdata").is_dir());
        assert!(mgr.volumes_dir().join("cache").is_dir());
    }

    #[test]
    fn manager_ensure_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        mgr.ensure_volumes(&test_volumes()).unwrap();
        // Second call creates nothing.
        let created = mgr.ensure_volumes(&test_volumes()).unwrap();
        assert!(created.is_empty());
    }

    #[test]
    fn manager_ensure_single_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        assert!(mgr.ensure_volume("mydata").unwrap());
        assert!(mgr.volumes_dir().join("mydata").is_dir());
        // Second call returns false.
        assert!(!mgr.ensure_volume("mydata").unwrap());
    }

    #[test]
    fn manager_remove_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        mgr.ensure_volume("mydata").unwrap();
        assert!(mgr.remove_volume("mydata").unwrap());
        assert!(!mgr.volumes_dir().join("mydata").exists());
        // Removing non-existent returns false.
        assert!(!mgr.remove_volume("mydata").unwrap());
    }

    #[test]
    fn manager_remove_volume_with_contents() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        mgr.ensure_volume("mydata").unwrap();
        // Write a file inside the volume.
        let file = mgr.volumes_dir().join("mydata").join("test.txt");
        std::fs::write(&file, "hello").unwrap();
        assert!(file.exists());

        assert!(mgr.remove_volume("mydata").unwrap());
        assert!(!mgr.volumes_dir().join("mydata").exists());
    }

    #[test]
    fn manager_remove_orphaned() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        mgr.ensure_volumes(&test_volumes()).unwrap();

        // Only "dbdata" is referenced.
        let referenced: HashSet<String> = ["dbdata".to_string()].into();
        let removed = mgr.remove_orphaned(&test_volumes(), &referenced).unwrap();
        assert_eq!(removed, vec!["cache".to_string()]);
        assert!(mgr.volumes_dir().join("dbdata").is_dir());
        assert!(!mgr.volumes_dir().join("cache").exists());
    }

    #[test]
    fn manager_remove_all() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        mgr.ensure_volumes(&test_volumes()).unwrap();
        let count = mgr.remove_all().unwrap();
        assert_eq!(count, 2);
        assert!(mgr.list_volumes().unwrap().is_empty());
    }

    #[test]
    fn manager_remove_all_when_none_exist() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());
        assert_eq!(mgr.remove_all().unwrap(), 0);
    }

    #[test]
    fn manager_list_volumes() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        mgr.ensure_volumes(&test_volumes()).unwrap();
        let names = mgr.list_volumes().unwrap();
        assert_eq!(names, vec!["cache", "dbdata"]); // sorted
    }

    #[test]
    fn manager_list_empty_when_no_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());
        assert!(mgr.list_volumes().unwrap().is_empty());
    }

    // --- Disk image tests ---

    #[test]
    fn manager_disk_image_path() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());
        assert_eq!(mgr.disk_image_path(), tmp.path().join("data.img"));
    }

    #[test]
    fn manager_ensure_disk_image_creates_sparse_file() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        assert!(!mgr.has_disk_image());
        assert!(mgr.ensure_disk_image(None).unwrap());
        assert!(mgr.has_disk_image());

        let metadata = std::fs::metadata(mgr.disk_image_path()).unwrap();
        assert_eq!(metadata.len(), 10 * 1024 * 1024 * 1024); // 10 GiB
    }

    #[test]
    fn manager_ensure_disk_image_custom_size() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        assert!(mgr.ensure_disk_image(Some(512 * 1024 * 1024)).unwrap());
        let metadata = std::fs::metadata(mgr.disk_image_path()).unwrap();
        assert_eq!(metadata.len(), 512 * 1024 * 1024);
    }

    #[test]
    fn manager_ensure_disk_image_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        assert!(mgr.ensure_disk_image(None).unwrap());
        // Second call returns false (already exists).
        assert!(!mgr.ensure_disk_image(None).unwrap());
    }

    #[test]
    fn manager_remove_disk_image() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        mgr.ensure_disk_image(None).unwrap();
        assert!(mgr.remove_disk_image().unwrap());
        assert!(!mgr.has_disk_image());
        // Removing non-existent returns false.
        assert!(!mgr.remove_disk_image().unwrap());
    }

    #[test]
    fn manager_remove_all_includes_disk_image() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        mgr.ensure_volumes(&test_volumes()).unwrap();
        mgr.ensure_disk_image(None).unwrap();

        let count = mgr.remove_all().unwrap();
        assert_eq!(count, 2); // 2 volume directories
        assert!(!mgr.has_disk_image()); // disk image also removed
    }

    #[test]
    fn manager_resolve_mounts_uses_volumes_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumeManager::new(tmp.path());

        let mounts = vec![MountSpec::Named {
            source: "dbdata".to_string(),
            target: "/var/lib/db".to_string(),
            read_only: false,
        }];

        let resolved = mgr.resolve_mounts(&mounts, &test_volumes()).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(
            resolved[0].host_path,
            Some(mgr.volumes_dir().join("dbdata"))
        );
    }

    // --- validate_bind_mounts tests ---

    #[test]
    fn validate_bind_mounts_directory_resolves_to_absolute() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("mydir");
        std::fs::create_dir(&dir).unwrap();

        let mut resolved = vec![ResolvedMount {
            host_path: Some(dir.clone()),
            target: "/data".to_string(),
            read_only: false,
            kind: ResolvedMountKind::Bind,
            subpath: None,
        }];

        validate_bind_mounts(&mut resolved).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].host_path, Some(dir));
        assert!(resolved[0].subpath.is_none());
    }

    #[test]
    fn validate_bind_mounts_file_sets_subpath() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("config.toml");
        std::fs::write(&file, "key = 'value'").unwrap();

        let mut resolved = vec![ResolvedMount {
            host_path: Some(file),
            target: "/etc/config.toml".to_string(),
            read_only: true,
            kind: ResolvedMountKind::Bind,
            subpath: None,
        }];

        validate_bind_mounts(&mut resolved).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].host_path, Some(tmp.path().to_path_buf()));
        assert_eq!(resolved[0].subpath, Some("config.toml".to_string()));
    }

    #[test]
    fn validate_bind_mounts_nonexistent_returns_error() {
        let mut resolved = vec![ResolvedMount {
            host_path: Some(PathBuf::from("/nonexistent/path/that/does/not/exist")),
            target: "/data".to_string(),
            read_only: false,
            kind: ResolvedMountKind::Bind,
            subpath: None,
        }];

        let err = validate_bind_mounts(&mut resolved).unwrap_err();
        assert!(matches!(err, StackError::InvalidSpec(_)));
    }

    #[cfg(unix)]
    #[test]
    fn validate_bind_mounts_socket_is_skipped() {
        use std::os::unix::net::UnixListener;

        let tmp = tempfile::tempdir().unwrap();
        let sock_path = tmp.path().join("test.sock");
        let _listener = UnixListener::bind(&sock_path).unwrap();

        let mut resolved = vec![
            ResolvedMount {
                host_path: Some(sock_path),
                target: "/var/run/test.sock".to_string(),
                read_only: false,
                kind: ResolvedMountKind::Bind,
                subpath: None,
            },
            ResolvedMount {
                host_path: Some(tmp.path().to_path_buf()),
                target: "/data".to_string(),
                read_only: false,
                kind: ResolvedMountKind::Bind,
                subpath: None,
            },
        ];

        validate_bind_mounts(&mut resolved).unwrap();
        // Socket should be filtered out, directory should remain.
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].target, "/data");
    }

    #[test]
    fn validate_bind_mounts_skips_non_bind_mounts() {
        let mut resolved = vec![
            ResolvedMount {
                host_path: None,
                target: "/tmp".to_string(),
                read_only: false,
                kind: ResolvedMountKind::Ephemeral,
                subpath: None,
            },
            ResolvedMount {
                host_path: Some(PathBuf::from("/volumes/dbdata")),
                target: "/var/lib/db".to_string(),
                read_only: false,
                kind: ResolvedMountKind::Named {
                    volume_name: "dbdata".to_string(),
                },
                subpath: None,
            },
        ];

        // Should not error — non-bind mounts are passed through without validation.
        validate_bind_mounts(&mut resolved).unwrap();
        assert_eq!(resolved.len(), 2);
    }
}
