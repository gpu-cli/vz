//! Volume planner for bind, named, and ephemeral mount specifications.
//!
//! Resolves [`MountSpec`](crate::MountSpec) entries from the stack spec
//! into concrete host paths, manages named volume lifecycle, and detects
//! mount configuration changes that require service recreation.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

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
                });
            }
            MountSpec::Ephemeral { target } => {
                resolved.push(ResolvedMount {
                    host_path: None,
                    target: target.clone(),
                    read_only: false,
                    kind: ResolvedMountKind::Ephemeral,
                });
            }
        }
    }

    Ok(resolved)
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
        let mounts = vec![
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
}
