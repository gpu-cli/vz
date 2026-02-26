#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tracing::info;
use vz_runtime_contract::{RuntimeCapabilities, WorkspaceRuntimeManager};

#[cfg(target_os = "linux")]
type PlatformBackend = vz_linux_native::LinuxNativeBackend;
#[cfg(target_os = "macos")]
type PlatformBackend = vz_oci_macos::MacosRuntimeBackend;

/// Configuration for booting `vz-runtimed`.
#[derive(Debug, Clone)]
pub struct RuntimedConfig {
    /// State store path used for receipts/events/runtime entities.
    pub state_store_path: PathBuf,
    /// Runtime backend data directory.
    pub runtime_data_dir: PathBuf,
}

impl Default for RuntimedConfig {
    fn default() -> Self {
        Self {
            state_store_path: PathBuf::from("stack-state.db"),
            runtime_data_dir: PathBuf::from(".vz-runtime"),
        }
    }
}

/// Runtime daemon health snapshot.
#[derive(Debug, Clone)]
pub struct DaemonHealth {
    /// Runtime backend name (e.g., `macos-vz` / `linux-native`).
    pub backend_name: String,
    /// Backend capability set exposed via Runtime V2 contract.
    pub capabilities: RuntimeCapabilities,
    /// Daemon start timestamp in unix seconds.
    pub started_at_unix_secs: u64,
}

/// Authoritative runtime owner for Runtime V2 control-plane mutations.
///
/// This process boundary is the foundation for moving side-effecting API
/// mutations out of transport handlers and into a long-lived daemon.
pub struct RuntimeDaemon {
    config: RuntimedConfig,
    manager: WorkspaceRuntimeManager<PlatformBackend>,
    started_at_unix_secs: u64,
}

impl RuntimeDaemon {
    /// Start the daemon runtime owner with the host platform backend.
    pub fn start(config: RuntimedConfig) -> Result<Self, RuntimedError> {
        ensure_parent_dir(&config.state_store_path).map_err(|source| {
            RuntimedError::CreateStateStoreDir {
                path: config.state_store_path.clone(),
                source,
            }
        })?;
        std::fs::create_dir_all(&config.runtime_data_dir).map_err(|source| {
            RuntimedError::CreateRuntimeDataDir {
                path: config.runtime_data_dir.clone(),
                source,
            }
        })?;

        let manager = build_runtime_manager(&config.runtime_data_dir);
        let started_at_unix_secs = current_unix_secs();

        info!(
            backend = %manager.name(),
            state_store = %config.state_store_path.display(),
            runtime_data_dir = %config.runtime_data_dir.display(),
            "vz-runtimed started"
        );

        Ok(Self {
            config,
            manager,
            started_at_unix_secs,
        })
    }

    /// Runtime backend name exposed by the daemon.
    pub fn backend_name(&self) -> &str {
        self.manager.name()
    }

    /// State store path bound to this daemon process.
    pub fn state_store_path(&self) -> &Path {
        &self.config.state_store_path
    }

    /// Runtime data directory used by this daemon process.
    pub fn runtime_data_dir(&self) -> &Path {
        &self.config.runtime_data_dir
    }

    /// Return the backend capability matrix.
    pub fn capabilities(&self) -> RuntimeCapabilities {
        self.manager.capabilities()
    }

    /// Snapshot daemon health information for monitoring/probes.
    pub fn health(&self) -> DaemonHealth {
        DaemonHealth {
            backend_name: self.backend_name().to_string(),
            capabilities: self.capabilities(),
            started_at_unix_secs: self.started_at_unix_secs,
        }
    }

    /// Borrow the canonical runtime manager.
    ///
    /// API adapters should call runtime operations through this manager
    /// instead of mutating state-store entities directly.
    pub fn manager(&self) -> &WorkspaceRuntimeManager<PlatformBackend> {
        &self.manager
    }
}

#[cfg(target_os = "macos")]
fn build_runtime_manager(data_dir: &Path) -> WorkspaceRuntimeManager<PlatformBackend> {
    let runtime = vz_oci_macos::Runtime::new(vz_oci_macos::RuntimeConfig {
        data_dir: data_dir.to_path_buf(),
        ..Default::default()
    });
    let backend = vz_oci_macos::MacosRuntimeBackend::new(runtime);
    WorkspaceRuntimeManager::new(backend)
}

#[cfg(target_os = "linux")]
fn build_runtime_manager(data_dir: &Path) -> WorkspaceRuntimeManager<PlatformBackend> {
    let backend = vz_linux_native::LinuxNativeBackend::new(vz_linux_native::LinuxNativeConfig {
        data_dir: data_dir.to_path_buf(),
        ..Default::default()
    });
    WorkspaceRuntimeManager::new(backend)
}

fn ensure_parent_dir(path: &Path) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Daemon startup failures.
#[derive(Debug, Error)]
pub enum RuntimedError {
    #[error("failed to create state-store directory for {path}: {source}")]
    CreateStateStoreDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create runtime data directory {path}: {source}")]
    CreateRuntimeDataDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_expected_paths() {
        let cfg = RuntimedConfig::default();
        assert_eq!(cfg.state_store_path, PathBuf::from("stack-state.db"));
        assert_eq!(cfg.runtime_data_dir, PathBuf::from(".vz-runtime"));
    }

    #[test]
    fn start_creates_required_directories() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
        };

        let daemon = RuntimeDaemon::start(cfg.clone()).expect("daemon should start");

        assert!(cfg.runtime_data_dir.is_dir());
        assert!(cfg.state_store_path.parent().expect("parent").is_dir());
        assert_eq!(daemon.state_store_path(), cfg.state_store_path.as_path());
        assert_eq!(daemon.runtime_data_dir(), cfg.runtime_data_dir.as_path());
    }
}
