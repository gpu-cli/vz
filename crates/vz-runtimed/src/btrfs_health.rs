use std::path::Path;

#[cfg(target_os = "linux")]
use std::process::Command;

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BtrfsHealthSeverity {
    Healthy,
    Warning,
    Error,
    Unsupported,
}

impl BtrfsHealthSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Unsupported => "unsupported",
        }
    }

    pub fn metric_value(&self) -> i64 {
        match self {
            Self::Healthy => 1,
            Self::Warning => 0,
            Self::Error => -1,
            Self::Unsupported => -2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtrfsHealthProbe {
    pub component: &'static str,
    pub severity: BtrfsHealthSeverity,
    pub detail: String,
}

#[cfg(target_os = "linux")]
impl BtrfsHealthProbe {
    fn healthy(component: &'static str, detail: impl Into<String>) -> Self {
        Self {
            component,
            severity: BtrfsHealthSeverity::Healthy,
            detail: detail.into(),
        }
    }

    fn warning(component: &'static str, detail: impl Into<String>) -> Self {
        Self {
            component,
            severity: BtrfsHealthSeverity::Warning,
            detail: detail.into(),
        }
    }

    fn error(component: &'static str, detail: impl Into<String>) -> Self {
        Self {
            component,
            severity: BtrfsHealthSeverity::Error,
            detail: detail.into(),
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub fn collect_btrfs_health_probes(target_path: &Path) -> Vec<BtrfsHealthProbe> {
    let detail = format!(
        "btrfs maintenance probes are unsupported on platform `{}` for `{}`",
        std::env::consts::OS,
        target_path.display()
    );
    vec![
        BtrfsHealthProbe {
            component: "scrub",
            severity: BtrfsHealthSeverity::Unsupported,
            detail: detail.clone(),
        },
        BtrfsHealthProbe {
            component: "balance",
            severity: BtrfsHealthSeverity::Unsupported,
            detail,
        },
    ]
}

#[cfg(target_os = "linux")]
pub fn collect_btrfs_health_probes(target_path: &Path) -> Vec<BtrfsHealthProbe> {
    vec![
        probe_scrub_status(target_path),
        probe_balance_status(target_path),
    ]
}

#[cfg(target_os = "linux")]
fn probe_scrub_status(target_path: &Path) -> BtrfsHealthProbe {
    let output = match Command::new("btrfs")
        .args(["scrub", "status", &target_path.to_string_lossy()])
        .output()
    {
        Ok(output) => output,
        Err(error) => {
            return BtrfsHealthProbe::error(
                "scrub",
                format!("failed to execute `btrfs scrub status`: {error}"),
            );
        }
    };
    if !output.status.success() {
        return BtrfsHealthProbe::error(
            "scrub",
            format!(
                "`btrfs scrub status` exited {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout).to_lowercase();
    if stdout.contains("running") {
        return BtrfsHealthProbe::healthy("scrub", "scrub currently running");
    }
    if stdout.contains("errors found: 0") || stdout.contains("status: finished") {
        return BtrfsHealthProbe::healthy("scrub", "last scrub completed without reported errors");
    }
    if stdout.contains("no stats available") {
        return BtrfsHealthProbe::warning("scrub", "no prior scrub stats available");
    }
    BtrfsHealthProbe::warning(
        "scrub",
        format!(
            "scrub status not clearly healthy; output={}",
            first_line(&stdout)
        ),
    )
}

#[cfg(target_os = "linux")]
fn probe_balance_status(target_path: &Path) -> BtrfsHealthProbe {
    let output = match Command::new("btrfs")
        .args(["balance", "status", &target_path.to_string_lossy()])
        .output()
    {
        Ok(output) => output,
        Err(error) => {
            return BtrfsHealthProbe::error(
                "balance",
                format!("failed to execute `btrfs balance status`: {error}"),
            );
        }
    };
    if !output.status.success() {
        return BtrfsHealthProbe::error(
            "balance",
            format!(
                "`btrfs balance status` exited {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout).to_lowercase();
    if stdout.contains("is running") {
        return BtrfsHealthProbe::healthy("balance", "balance currently running");
    }
    if stdout.contains("no balance found") {
        return BtrfsHealthProbe::healthy("balance", "no active balance operation");
    }
    if stdout.contains("paused") {
        return BtrfsHealthProbe::warning("balance", "balance operation paused");
    }
    BtrfsHealthProbe::warning(
        "balance",
        format!(
            "balance status not clearly healthy; output={}",
            first_line(&stdout)
        ),
    )
}

#[cfg(target_os = "linux")]
fn first_line(value: &str) -> String {
    value.lines().next().unwrap_or_default().trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn probes_are_marked_unsupported_on_non_linux() {
        let probes = collect_btrfs_health_probes(Path::new("/tmp"));
        assert_eq!(probes.len(), 2);
        assert_eq!(probes[0].severity, BtrfsHealthSeverity::Unsupported);
        assert_eq!(probes[1].severity, BtrfsHealthSeverity::Unsupported);
    }

    #[test]
    fn severity_metric_values_are_stable() {
        assert_eq!(BtrfsHealthSeverity::Healthy.metric_value(), 1);
        assert_eq!(BtrfsHealthSeverity::Warning.metric_value(), 0);
        assert_eq!(BtrfsHealthSeverity::Error.metric_value(), -1);
        assert_eq!(BtrfsHealthSeverity::Unsupported.metric_value(), -2);
    }
}
