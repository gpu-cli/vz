//! Cgroup v2 detection and limit helpers.
//!
//! Provides runtime checks for cgroup v2 availability and helpers
//! to verify CPU/memory limits applied by the OCI runtime.

use std::path::Path;

use tracing::debug;

/// Standard cgroup v2 mount point.
const CGROUP_V2_MOUNT: &str = "/sys/fs/cgroup";

/// Check if cgroup v2 (unified hierarchy) is available.
///
/// Returns `true` if `/sys/fs/cgroup/cgroup.controllers` exists,
/// indicating a cgroup v2 unified hierarchy.
pub fn is_cgroup_v2_available() -> bool {
    let available = Path::new(CGROUP_V2_MOUNT)
        .join("cgroup.controllers")
        .exists();
    debug!(available, "cgroup v2 check");
    available
}

/// Check if cgroup delegation is set up for rootless containers.
///
/// Returns `true` if the current user has a writable cgroup subtree.
/// This is required for rootless containers to apply resource limits.
pub fn is_delegation_available() -> bool {
    let uid = std::env::var("UID").ok().or_else(|| {
        // Fallback: read UID from /proc/self/status
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        status
            .lines()
            .find(|l| l.starts_with("Uid:"))
            .and_then(|l| l.split_whitespace().nth(1))
            .map(String::from)
    });

    if let Some(uid) = uid {
        let user_slice = format!("{CGROUP_V2_MOUNT}/user.slice/user-{uid}.slice");
        Path::new(&user_slice).exists()
    } else {
        false
    }
}

/// Read the CPU quota from a cgroup (if any).
///
/// Parses `/sys/fs/cgroup/<path>/cpu.max` which has format:
/// `<quota> <period>` or `max <period>`.
pub fn read_cpu_max(cgroup_path: &Path) -> Option<(Option<i64>, u64)> {
    let cpu_max_path = cgroup_path.join("cpu.max");
    let content = std::fs::read_to_string(cpu_max_path).ok()?;
    let parts: Vec<&str> = content.split_whitespace().collect();
    if parts.len() != 2 {
        return None;
    }

    let quota = if parts[0] == "max" {
        None
    } else {
        parts[0].parse::<i64>().ok()
    };
    let period = parts[1].parse::<u64>().ok()?;

    Some((quota, period))
}

/// Read memory limit from a cgroup.
///
/// Parses `/sys/fs/cgroup/<path>/memory.max` which contains
/// either a byte count or `max` (unlimited).
pub fn read_memory_max(cgroup_path: &Path) -> Option<Option<u64>> {
    let mem_max_path = cgroup_path.join("memory.max");
    let content = std::fs::read_to_string(mem_max_path).ok()?;
    let trimmed = content.trim();
    if trimmed == "max" {
        Some(None) // unlimited
    } else {
        Some(Some(trimmed.parse::<u64>().ok()?))
    }
}
