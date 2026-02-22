use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Which OCI runtime binary to use.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OciRuntime {
    /// Youki (default, Rust-native OCI runtime).
    #[default]
    Youki,
    /// runc (fallback).
    Runc,
    /// Custom runtime binary at a specific path.
    Custom(PathBuf),
}

impl OciRuntime {
    /// Resolve the binary path for this runtime.
    ///
    /// For well-known runtimes, returns the standard binary name
    /// (caller is expected to find it on `$PATH`). For custom
    /// runtimes, returns the user-supplied path.
    pub fn binary_name(&self) -> &str {
        match self {
            Self::Youki => "youki",
            Self::Runc => "runc",
            Self::Custom(path) => path.to_str().unwrap_or("youki"),
        }
    }
}

/// Container isolation mode.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationMode {
    /// Rootless containers (preferred default).
    ///
    /// Requires delegated cgroup v2 support on the host.
    #[default]
    Rootless,
    /// Rootful containers (explicit opt-in).
    Rootful,
}

/// Linux-native backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinuxNativeConfig {
    /// OCI runtime to use (youki, runc, or custom).
    pub runtime: OciRuntime,

    /// Isolation mode.
    pub isolation: IsolationMode,

    /// Root directory for container bundles and state.
    ///
    /// Defaults to `~/.vz/linux-native`.
    pub data_dir: PathBuf,

    /// Directory for OCI runtime state files.
    ///
    /// Defaults to `<data_dir>/state`.
    pub state_dir: Option<PathBuf>,

    /// Directory for container bundle directories.
    ///
    /// Defaults to `<data_dir>/bundles`.
    pub bundle_dir: Option<PathBuf>,
}

impl Default for LinuxNativeConfig {
    fn default() -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        Self {
            runtime: OciRuntime::default(),
            isolation: IsolationMode::default(),
            data_dir: PathBuf::from(home).join(".vz/linux-native"),
            state_dir: None,
            bundle_dir: None,
        }
    }
}

impl LinuxNativeConfig {
    /// Resolved state directory.
    pub fn state_dir(&self) -> PathBuf {
        self.state_dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("state"))
    }

    /// Resolved bundle directory.
    pub fn bundle_dir(&self) -> PathBuf {
        self.bundle_dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("bundles"))
    }
}
