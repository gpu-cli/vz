//! Spawn-only installed catalog selection. No workspace or legacy bundle discovery.

use std::path::{Component, Path, PathBuf};

use crate::{DaemonClientConfig, DaemonClientError, Result};

pub(super) fn resolve(config: &DaemonClientConfig, binary: &Path) -> Result<Option<PathBuf>> {
    let path = if let Some(explicit) = &config.machine_target_catalog {
        explicit.clone()
    } else if config.discover_installed_machine_target_catalog {
        let binary = std::fs::canonicalize(binary)?;
        let bin = binary.parent().filter(|parent| parent.file_name().is_some_and(|name| name == "bin"))
            .ok_or_else(|| invalid("daemon is not in an installed bin directory; provide VZ_MACHINE_TARGET_CATALOG explicitly"))?;
        bin.parent()
            .ok_or_else(|| invalid("installed daemon has no prefix"))?
            .join("machine-target-catalog.json")
    } else {
        return Ok(None);
    };
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
    {
        return Err(invalid(
            "Machine catalog path must be absolute without traversal",
        ));
    }
    // Do not follow a final symlink or accept an unbounded/nonregular catalog.
    // The daemon performs authoritative ownership, schema and bundle validation.
    let metadata = std::fs::symlink_metadata(&path).map_err(|error| invalid(&format!("cannot read installed Machine catalog {}: {error}; reinstall artifacts or configure an explicit catalog", path.display())))?;
    if !metadata.is_file() || metadata.len() > 1024 * 1024 {
        return Err(invalid(
            "Machine catalog must be a bounded regular file, not a symlink",
        ));
    }
    Ok(Some(path))
}

fn invalid(reason: &str) -> DaemonClientError {
    DaemonClientError::IncompatibleProtocol {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    #[test]
    fn explicit_invalid_does_not_fall_back_and_no_ambient_default() {
        let config = DaemonClientConfig::default();
        assert_eq!(
            resolve(&config, Path::new("/missing/daemon")).expect("no discovery"),
            None
        );
        let config = DaemonClientConfig {
            machine_target_catalog: Some("relative.json".into()),
            discover_installed_machine_target_catalog: true,
            ..config
        };
        assert!(resolve(&config, Path::new("/missing/daemon")).is_err());
    }
    #[test]
    fn installed_discovery_and_explicit_precedence_are_exact() {
        let temp = tempfile::tempdir().expect("directory");
        let root = temp.path().canonicalize().expect("canonical");
        std::fs::create_dir(root.join("bin")).expect("bin");
        let binary = root.join("bin/vz-runtimed");
        std::fs::write(&binary, b"binary").expect("binary");
        let config = DaemonClientConfig {
            discover_installed_machine_target_catalog: true,
            ..Default::default()
        };
        assert!(resolve(&config, &binary).is_err());
        let catalog = root.join("machine-target-catalog.json");
        std::fs::write(&catalog, b"{}").expect("catalog");
        assert_eq!(
            resolve(&config, &binary).expect("installed"),
            Some(catalog.clone())
        );
        let explicit = root.join("explicit.json");
        std::fs::write(&explicit, b"{}").expect("explicit");
        let config = DaemonClientConfig {
            machine_target_catalog: Some(explicit.clone()),
            ..config
        };
        assert_eq!(
            resolve(&config, Path::new("/missing")).expect("explicit"),
            Some(explicit)
        );
        std::fs::remove_file(&catalog).expect("remove fixture");
        std::os::unix::fs::symlink(root.join("explicit.json"), &catalog).expect("symlink");
        assert!(
            resolve(
                &DaemonClientConfig {
                    machine_target_catalog: Some(catalog),
                    ..Default::default()
                },
                &binary
            )
            .is_err()
        );
    }
}
