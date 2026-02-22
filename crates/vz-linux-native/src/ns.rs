//! Network namespace helpers for Linux-native container networking.
//!
//! Creates and manages named network namespaces used for per-service
//! isolation in multi-container stacks.

use std::path::{Path, PathBuf};

use tokio::process::Command;
use tracing::{debug, warn};

use crate::error::LinuxNativeError;

/// Base directory for named network namespaces.
const NETNS_DIR: &str = "/var/run/netns";

/// Create a named network namespace.
///
/// Runs: `ip netns add <name>`
pub async fn create_netns(name: &str) -> Result<PathBuf, LinuxNativeError> {
    debug!(name, "creating network namespace");

    let output = Command::new("ip")
        .args(["netns", "add", name])
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // If namespace already exists, that's fine.
        if stderr.contains("File exists") {
            debug!(name, "network namespace already exists");
        } else {
            return Err(LinuxNativeError::InvalidConfig(format!(
                "failed to create netns '{name}': {stderr}"
            )));
        }
    }

    Ok(netns_path(name))
}

/// Delete a named network namespace.
///
/// Runs: `ip netns del <name>`
pub async fn delete_netns(name: &str) -> Result<(), LinuxNativeError> {
    debug!(name, "deleting network namespace");

    let output = Command::new("ip")
        .args(["netns", "del", name])
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // If namespace doesn't exist, that's fine during cleanup.
        if stderr.contains("No such file") {
            debug!(name, "network namespace already removed");
        } else {
            warn!(name, stderr = %stderr.trim(), "failed to delete netns");
        }
    }

    Ok(())
}

/// Check if a named network namespace exists.
pub fn netns_exists(name: &str) -> bool {
    Path::new(NETNS_DIR).join(name).exists()
}

/// Get the filesystem path for a named network namespace.
pub fn netns_path(name: &str) -> PathBuf {
    PathBuf::from(NETNS_DIR).join(name)
}

/// Execute a command inside a network namespace.
///
/// Runs: `ip netns exec <name> <cmd> [args...]`
pub async fn exec_in_netns(
    name: &str,
    cmd: &str,
    args: &[&str],
) -> Result<std::process::Output, LinuxNativeError> {
    let mut command = Command::new("ip");
    command.args(["netns", "exec", name, cmd]);
    command.args(args);

    let output = command.output().await?;
    Ok(output)
}
