//! Host capability probes for the Linux-native backend.
//!
//! Checks whether the current Linux host has the prerequisites for running
//! OCI containers: cgroup v2, user namespaces, and a runtime binary.

use std::path::Path;

/// Result of a single capability probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeResult {
    /// Name of the capability being probed.
    pub name: &'static str,
    /// Whether the capability is available.
    pub available: bool,
    /// Human-readable status message.
    pub message: String,
    /// Remediation hint if not available.
    pub remediation: Option<String>,
}

/// Aggregated probe results for the Linux host.
#[derive(Debug, Clone)]
pub struct HostProbeReport {
    /// Individual probe results.
    pub probes: Vec<ProbeResult>,
}

impl HostProbeReport {
    /// Whether all required capabilities are present.
    pub fn all_satisfied(&self) -> bool {
        self.probes.iter().all(|p| p.available)
    }

    /// Return a formatted summary string.
    pub fn summary(&self) -> String {
        let mut lines = Vec::new();
        for p in &self.probes {
            let status = if p.available { "ok" } else { "MISSING" };
            lines.push(format!("  [{status}] {}: {}", p.name, p.message));
            if let Some(ref rem) = p.remediation {
                lines.push(format!("         -> {rem}"));
            }
        }
        lines.join("\n")
    }
}

/// Run all host capability probes and return a report.
pub fn probe_host() -> HostProbeReport {
    let probes = vec![
        probe_cgroup_v2(),
        probe_user_namespaces(),
        probe_oci_runtime(),
    ];
    HostProbeReport { probes }
}

/// Check that cgroup v2 (unified hierarchy) is mounted.
fn probe_cgroup_v2() -> ProbeResult {
    let cgroup2_path = Path::new("/sys/fs/cgroup/cgroup.controllers");
    if cgroup2_path.exists() {
        ProbeResult {
            name: "cgroup-v2",
            available: true,
            message: "cgroup v2 unified hierarchy is mounted".to_string(),
            remediation: None,
        }
    } else {
        ProbeResult {
            name: "cgroup-v2",
            available: false,
            message: "cgroup v2 unified hierarchy not found".to_string(),
            remediation: Some(
                "Ensure your kernel has cgroup v2 enabled. \
                 Add 'systemd.unified_cgroup_hierarchy=1' to kernel boot params, \
                 or use a distro with cgroup v2 by default (Ubuntu 22.04+, Fedora 31+)."
                    .to_string(),
            ),
        }
    }
}

/// Check that unprivileged user namespaces are enabled.
fn probe_user_namespaces() -> ProbeResult {
    let userns_path = Path::new("/proc/sys/kernel/unprivileged_userns_clone");

    // If the sysctl doesn't exist, user namespaces are likely unrestricted.
    if !userns_path.exists() {
        return ProbeResult {
            name: "user-namespaces",
            available: true,
            message: "user namespaces available (no kernel restriction)".to_string(),
            remediation: None,
        };
    }

    match std::fs::read_to_string(userns_path) {
        Ok(val) if val.trim() == "1" => ProbeResult {
            name: "user-namespaces",
            available: true,
            message: "unprivileged user namespaces enabled".to_string(),
            remediation: None,
        },
        Ok(_) => ProbeResult {
            name: "user-namespaces",
            available: false,
            message: "unprivileged user namespaces disabled".to_string(),
            remediation: Some(
                "Enable with: sudo sysctl -w kernel.unprivileged_userns_clone=1\n\
                 To persist: echo 'kernel.unprivileged_userns_clone=1' | \
                 sudo tee /etc/sysctl.d/99-userns.conf && sudo sysctl --system"
                    .to_string(),
            ),
        },
        Err(e) => ProbeResult {
            name: "user-namespaces",
            available: false,
            message: format!("could not read userns sysctl: {e}"),
            remediation: Some("Check kernel support for user namespaces.".to_string()),
        },
    }
}

/// Check that an OCI runtime binary (youki or runc) is on $PATH.
fn probe_oci_runtime() -> ProbeResult {
    // Try youki first, then runc.
    for runtime in &["youki", "runc"] {
        if which_exists(runtime) {
            return ProbeResult {
                name: "oci-runtime",
                available: true,
                message: format!("found '{runtime}' on PATH"),
                remediation: None,
            };
        }
    }

    ProbeResult {
        name: "oci-runtime",
        available: false,
        message: "no OCI runtime found (checked: youki, runc)".to_string(),
        remediation: Some(
            "Install an OCI runtime:\n\
             - youki: cargo install youki (or download from https://github.com/youki-dev/youki)\n\
             - runc: apt install runc (Debian/Ubuntu) or dnf install runc (Fedora)"
                .to_string(),
        ),
    }
}

/// Check if a binary exists on $PATH.
fn which_exists(name: &str) -> bool {
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(name).is_file()))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_report_summary_includes_all_probes() {
        let report = HostProbeReport {
            probes: vec![
                ProbeResult {
                    name: "test-ok",
                    available: true,
                    message: "all good".to_string(),
                    remediation: None,
                },
                ProbeResult {
                    name: "test-fail",
                    available: false,
                    message: "not found".to_string(),
                    remediation: Some("install it".to_string()),
                },
            ],
        };
        let summary = report.summary();
        assert!(summary.contains("[ok] test-ok"));
        assert!(summary.contains("[MISSING] test-fail"));
        assert!(summary.contains("install it"));
    }

    #[test]
    fn all_satisfied_true_when_all_available() {
        let report = HostProbeReport {
            probes: vec![ProbeResult {
                name: "cap",
                available: true,
                message: "ok".to_string(),
                remediation: None,
            }],
        };
        assert!(report.all_satisfied());
    }

    #[test]
    fn all_satisfied_false_when_any_missing() {
        let report = HostProbeReport {
            probes: vec![
                ProbeResult {
                    name: "a",
                    available: true,
                    message: "ok".to_string(),
                    remediation: None,
                },
                ProbeResult {
                    name: "b",
                    available: false,
                    message: "missing".to_string(),
                    remediation: None,
                },
            ],
        };
        assert!(!report.all_satisfied());
    }

    #[test]
    fn which_exists_finds_common_binary() {
        // `sh` should exist on any Unix system.
        assert!(which_exists("sh"));
    }

    #[test]
    fn which_exists_returns_false_for_nonexistent() {
        assert!(!which_exists("definitely_not_a_real_binary_xyz_12345"));
    }
}
