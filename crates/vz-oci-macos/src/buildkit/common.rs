use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::BuildkitError;

pub(super) fn resolve_dockerfile_path(
    context_dir: &Path,
    dockerfile: &Path,
) -> Result<PathBuf, BuildkitError> {
    let path = if dockerfile.is_absolute() {
        dockerfile.to_path_buf()
    } else {
        context_dir.join(dockerfile)
    };
    if !path.is_file() {
        return Err(BuildkitError::InvalidConfig(format!(
            "Dockerfile not found: {}",
            path.display()
        )));
    }
    Ok(path)
}

pub(super) fn canonicalize_existing_dir(path: &Path) -> Result<PathBuf, BuildkitError> {
    let expanded = expand_home_dir(path);
    let canonical = expanded.canonicalize()?;
    if !canonical.is_dir() {
        return Err(BuildkitError::InvalidConfig(format!(
            "build context is not a directory: {}",
            canonical.display()
        )));
    }
    Ok(canonical)
}

pub(super) fn expand_home_dir(path: &Path) -> PathBuf {
    if let Some(path_str) = path.to_str() {
        if let Some(rest) = path_str.strip_prefix("~/")
            && let Some(home) = std::env::var_os("HOME")
        {
            return PathBuf::from(home).join(rest);
        }
    }
    path.to_path_buf()
}

pub(super) fn default_buildkit_dir() -> Result<PathBuf, BuildkitError> {
    let home = std::env::var_os("HOME").ok_or(BuildkitError::HomeDirectoryUnavailable)?;
    Ok(PathBuf::from(home).join(".vz").join("buildkit"))
}

pub(super) fn unique_dir(parent: PathBuf, prefix: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    parent.join(format!("{prefix}-{stamp}"))
}

pub(super) fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
