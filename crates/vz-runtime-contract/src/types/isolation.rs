use std::fmt;

use serde::{Deserialize, Serialize};

/// Isolation level supported by a runtime backend.
///
/// Backends expose the strongest isolation they provide. Callers can
/// query a backend's isolation level to make scheduling, security, and
/// resource decisions.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    /// Full hardware-virtualised isolation (e.g., Virtualization.framework VM).
    #[default]
    Full,
    /// OCI-runtime container isolation (namespaces + cgroups + seccomp).
    Container,
    /// Lightweight namespace-only isolation (no cgroup/seccomp enforcement).
    ///
    /// Provides filesystem, PID, network, and user separation without the
    /// overhead of a full OCI runtime or VM. Suitable for trusted workloads
    /// that need process separation but not a full security boundary.
    Namespace,
    /// No isolation — direct host execution.
    None,
}

impl IsolationLevel {
    /// Human-readable label for diagnostics and reporting.
    pub fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Container => "container",
            Self::Namespace => "namespace",
            Self::None => "none",
        }
    }

    /// Whether this level provides at least namespace-level separation.
    pub fn has_namespace_isolation(self) -> bool {
        matches!(self, Self::Full | Self::Container | Self::Namespace)
    }

    /// Whether this level provides cgroup and seccomp enforcement.
    pub fn has_container_isolation(self) -> bool {
        matches!(self, Self::Full | Self::Container)
    }

    /// Whether this level provides full VM-based isolation.
    pub fn has_vm_isolation(self) -> bool {
        matches!(self, Self::Full)
    }
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Configuration for Linux namespace isolation.
///
/// Controls which namespaces are created for a lightweight
/// namespace-only isolation mode. Each field enables or disables the
/// corresponding `clone(2)` / `unshare(2)` namespace flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceConfig {
    /// Create a new user namespace (`CLONE_NEWUSER`).
    pub user: bool,
    /// Create a new network namespace (`CLONE_NEWNET`).
    pub net: bool,
    /// Create a new PID namespace (`CLONE_NEWPID`).
    pub pid: bool,
    /// Create a new mount namespace (`CLONE_NEWNS`).
    pub mnt: bool,
    /// Create a new IPC namespace (`CLONE_NEWIPC`).
    pub ipc: bool,
    /// Create a new UTS namespace (`CLONE_NEWUTS`).
    pub uts: bool,
}

impl NamespaceConfig {
    /// All namespaces enabled.
    pub const ALL: Self = Self {
        user: true,
        net: true,
        pid: true,
        mnt: true,
        ipc: true,
        uts: true,
    };

    /// No namespaces enabled (host execution).
    pub const NONE: Self = Self {
        user: false,
        net: false,
        pid: false,
        mnt: false,
        ipc: false,
        uts: false,
    };

    /// Count of enabled namespaces.
    pub fn enabled_count(self) -> usize {
        [self.user, self.net, self.pid, self.mnt, self.ipc, self.uts]
            .iter()
            .filter(|&&v| v)
            .count()
    }
}

/// Sensible default namespace configuration.
///
/// Enables PID, mount, IPC, and UTS namespaces for basic process
/// separation. Network and user namespaces are disabled by default
/// because they require additional setup (veth pairs, UID mapping).
pub fn default_namespace_config() -> NamespaceConfig {
    NamespaceConfig {
        user: false,
        net: false,
        pid: true,
        mnt: true,
        ipc: true,
        uts: true,
    }
}

impl Default for NamespaceConfig {
    fn default() -> Self {
        default_namespace_config()
    }
}
