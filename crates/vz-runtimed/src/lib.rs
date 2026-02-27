#![forbid(unsafe_code)]

mod execution_sessions;
mod grpc;
mod placement_scheduler;
#[cfg(any(test, feature = "test-backend"))]
mod test_backend;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tracing::info;
use vz_runtime_contract::{
    ExecutionState, MachineError, MachineErrorCode, PolicyDecision, RequestMetadata,
    RuntimeCapabilities, RuntimeError, RuntimeOperation, RuntimePolicyHook,
    WorkspaceRuntimeManager, enforce_runtime_policy_hook,
};
use vz_stack::{StackError, StackEvent, StateStore, StateStorePragmas};

pub(crate) use execution_sessions::{ExecutionSessionRegistry, ExecutionSessionRegistryError};
pub use grpc::{RuntimedServerError, serve_runtime_uds_with_shutdown};
use placement_scheduler::{PlacementScheduler, PlacementSnapshot};

const MAX_SUPPORTED_SCHEMA_VERSION: u32 = 1;

#[derive(Default)]
struct AllowAllPolicyHook;

impl RuntimePolicyHook for AllowAllPolicyHook {
    fn evaluate(
        &self,
        _operation: RuntimeOperation,
        _metadata: &RequestMetadata,
    ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
        Ok(PolicyDecision::Allow)
    }
}

#[cfg(any(test, feature = "test-backend"))]
type PlatformBackend = test_backend::TestRuntimeBackend;
#[cfg(all(not(any(test, feature = "test-backend")), target_os = "linux"))]
type PlatformBackend = vz_linux_native::LinuxNativeBackend;
#[cfg(all(not(any(test, feature = "test-backend")), target_os = "macos"))]
type PlatformBackend = vz_oci_macos::MacosRuntimeBackend;

/// Configuration for booting `vz-runtimed`.
#[derive(Debug, Clone)]
pub struct RuntimedConfig {
    /// State store path used for receipts/events/runtime entities.
    pub state_store_path: PathBuf,
    /// Runtime backend data directory.
    pub runtime_data_dir: PathBuf,
    /// Unix domain socket path for daemon gRPC transport.
    pub socket_path: PathBuf,
}

impl Default for RuntimedConfig {
    fn default() -> Self {
        Self {
            state_store_path: PathBuf::from("stack-state.db"),
            runtime_data_dir: PathBuf::from(".vz-runtime"),
            socket_path: PathBuf::from(".vz-runtime/runtimed.sock"),
        }
    }
}

/// Runtime daemon health snapshot.
#[derive(Debug, Clone)]
pub struct DaemonHealth {
    /// Stable daemon process identifier.
    pub daemon_id: String,
    /// Daemon version string.
    pub daemon_version: String,
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
    state_store: Mutex<StateStore>,
    execution_sessions: ExecutionSessionRegistry,
    placement_scheduler: PlacementScheduler,
    policy_hook: Arc<dyn RuntimePolicyHook>,
    policy_hash: Option<String>,
    daemon_id: String,
    daemon_version: String,
    started_at_unix_secs: u64,
    startup_lock: StartupLock,
}

impl RuntimeDaemon {
    /// Start the daemon runtime owner with the host platform backend.
    pub fn start(config: RuntimedConfig) -> Result<Self, RuntimedError> {
        Self::start_with_policy_hook(config, Arc::new(AllowAllPolicyHook), None)
    }

    pub(crate) fn start_with_policy_hook(
        config: RuntimedConfig,
        policy_hook: Arc<dyn RuntimePolicyHook>,
        policy_hash: Option<String>,
    ) -> Result<Self, RuntimedError> {
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
        ensure_parent_dir(&config.socket_path).map_err(|source| {
            RuntimedError::CreateSocketDir {
                path: config.socket_path.clone(),
                source,
            }
        })?;

        let startup_lock = StartupLock::acquire(startup_lock_path(&config.state_store_path))?;
        let state_store = StateStore::open_with_pragmas(
            &config.state_store_path,
            StateStorePragmas::daemon_defaults(),
        )
        .map_err(|source| RuntimedError::OpenStateStore {
            path: config.state_store_path.clone(),
            source,
        })?;
        let schema_version =
            state_store
                .schema_version()
                .map_err(|source| RuntimedError::ReadSchemaVersion {
                    path: config.state_store_path.clone(),
                    source,
                })?;
        if schema_version > MAX_SUPPORTED_SCHEMA_VERSION {
            return Err(RuntimedError::UnsupportedSchemaVersion {
                path: config.state_store_path.clone(),
                found: schema_version,
                max_supported: MAX_SUPPORTED_SCHEMA_VERSION,
            });
        }
        let reconciled_execution_count =
            reconcile_orphaned_executions(&state_store).map_err(|source| {
                RuntimedError::ReconcileExecutionState {
                    path: config.state_store_path.clone(),
                    source,
                }
            })?;

        let manager = build_runtime_manager(&config.runtime_data_dir);
        let placement_scheduler = PlacementScheduler::default();
        placement_scheduler
            .refresh(&state_store, current_unix_secs())
            .map_err(|source| RuntimedError::RefreshPlacementSnapshot {
                path: config.state_store_path.clone(),
                source,
            })?;
        let started_at_unix_secs = current_unix_secs();
        let daemon_id = format!("runtimed-{}-{started_at_unix_secs}", std::process::id());
        let daemon_version = env!("CARGO_PKG_VERSION").to_string();
        let journal_mode = state_store
            .journal_mode()
            .unwrap_or_else(|_| "unknown".to_string());
        let busy_timeout_ms = state_store.busy_timeout_ms().unwrap_or(0);
        let foreign_keys_enabled = state_store.foreign_keys_enabled().unwrap_or(false);

        info!(
            daemon_id = %daemon_id,
            daemon_version = %daemon_version,
            backend = %manager.name(),
            state_store = %config.state_store_path.display(),
            sqlite_journal_mode = %journal_mode,
            sqlite_busy_timeout_ms = busy_timeout_ms,
            sqlite_foreign_keys = foreign_keys_enabled,
            sqlite_schema_version = schema_version,
            reconciled_executions = reconciled_execution_count,
            runtime_data_dir = %config.runtime_data_dir.display(),
            socket_path = %config.socket_path.display(),
            "vz-runtimed started"
        );

        Ok(Self {
            config,
            manager,
            state_store: Mutex::new(state_store),
            execution_sessions: ExecutionSessionRegistry::default(),
            placement_scheduler,
            policy_hook,
            policy_hash,
            daemon_id,
            daemon_version,
            started_at_unix_secs,
            startup_lock,
        })
    }

    /// Stable daemon process identifier.
    pub fn daemon_id(&self) -> &str {
        &self.daemon_id
    }

    /// Daemon version string.
    pub fn daemon_version(&self) -> &str {
        &self.daemon_version
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

    /// UDS socket path used by daemon gRPC transport.
    pub fn socket_path(&self) -> &Path {
        &self.config.socket_path
    }

    /// Startup lock path for single-writer guard.
    pub fn startup_lock_path(&self) -> &Path {
        self.startup_lock.path()
    }

    /// Return the backend capability matrix.
    pub fn capabilities(&self) -> RuntimeCapabilities {
        self.manager.capabilities()
    }

    /// Snapshot daemon health information for monitoring/probes.
    pub fn health(&self) -> DaemonHealth {
        DaemonHealth {
            daemon_id: self.daemon_id().to_string(),
            daemon_version: self.daemon_version().to_string(),
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

    pub(crate) fn enforce_policy_preflight(
        &self,
        operation: RuntimeOperation,
        metadata: &RequestMetadata,
    ) -> Result<(), RuntimeError> {
        enforce_runtime_policy_hook(self.policy_hook.as_ref(), operation, metadata)
    }

    pub(crate) fn policy_hash(&self) -> Option<&str> {
        self.policy_hash.as_deref()
    }

    pub(crate) fn execution_sessions(&self) -> &ExecutionSessionRegistry {
        &self.execution_sessions
    }

    pub(crate) fn with_state_store<T>(
        &self,
        f: impl FnOnce(&StateStore) -> Result<T, StackError>,
    ) -> Result<T, StackError> {
        let store = self.state_store.lock().map_err(|_| StackError::Machine {
            code: MachineErrorCode::InternalError,
            message: "state store mutex poisoned".to_string(),
        })?;
        f(&store)
    }

    pub(crate) fn open_dedicated_state_store(&self) -> Result<StateStore, StackError> {
        StateStore::open_with_pragmas(
            self.state_store_path(),
            StateStorePragmas::daemon_defaults(),
        )
    }

    pub(crate) fn refresh_placement_snapshot(&self) -> Result<PlacementSnapshot, StackError> {
        let now = current_unix_secs();
        self.with_state_store(|store| self.placement_scheduler.refresh(store, now))
    }

    pub(crate) fn enforce_create_sandbox_placement(
        &self,
        request_id: &str,
    ) -> Result<(), MachineError> {
        self.refresh_placement_snapshot()
            .map_err(|error| placement_internal_machine_error(error, request_id))?;
        self.placement_scheduler
            .evaluate_create_sandbox(self.capabilities(), request_id)
    }

    pub(crate) fn enforce_create_container_placement(
        &self,
        request_id: &str,
    ) -> Result<(), MachineError> {
        self.refresh_placement_snapshot()
            .map_err(|error| placement_internal_machine_error(error, request_id))?;
        self.placement_scheduler
            .evaluate_create_container(self.capabilities(), request_id)
    }

    #[cfg(test)]
    pub(crate) fn set_placement_limits_for_test(
        &self,
        max_sandboxes: usize,
        max_containers: usize,
    ) {
        self.placement_scheduler
            .set_limits_for_test(max_sandboxes, max_containers);
    }
}

#[cfg(any(test, feature = "test-backend"))]
fn build_runtime_manager(_data_dir: &Path) -> WorkspaceRuntimeManager<PlatformBackend> {
    WorkspaceRuntimeManager::new(test_backend::TestRuntimeBackend::default())
}

#[cfg(all(not(any(test, feature = "test-backend")), target_os = "macos"))]
fn build_runtime_manager(data_dir: &Path) -> WorkspaceRuntimeManager<PlatformBackend> {
    let runtime = vz_oci_macos::Runtime::new(vz_oci_macos::RuntimeConfig {
        data_dir: data_dir.to_path_buf(),
        ..Default::default()
    });
    let backend = vz_oci_macos::MacosRuntimeBackend::new(runtime);
    WorkspaceRuntimeManager::new(backend)
}

#[cfg(all(not(any(test, feature = "test-backend")), target_os = "linux"))]
fn build_runtime_manager(data_dir: &Path) -> WorkspaceRuntimeManager<PlatformBackend> {
    let backend = vz_linux_native::LinuxNativeBackend::new(vz_linux_native::LinuxNativeConfig {
        data_dir: data_dir.to_path_buf(),
        ..Default::default()
    });
    WorkspaceRuntimeManager::new(backend)
}

fn reconcile_orphaned_executions(state_store: &StateStore) -> Result<u64, StackError> {
    let mut reconciled = 0;
    for mut execution in state_store.list_executions()? {
        if !matches!(
            execution.state,
            ExecutionState::Queued | ExecutionState::Running
        ) {
            continue;
        }

        let now = current_unix_secs();
        if execution.started_at.is_none() {
            execution.started_at = Some(now);
        }
        execution.ended_at = Some(now);
        execution.exit_code = None;
        execution
            .transition_to(ExecutionState::Failed)
            .map_err(|error| StackError::Machine {
                code: MachineErrorCode::StateConflict,
                message: error.to_string(),
            })?;

        state_store.with_immediate_transaction(|tx| {
            tx.save_execution(&execution)?;
            tx.emit_event(
                "daemon",
                &StackEvent::ExecutionFailed {
                    execution_id: execution.execution_id.clone(),
                    error: "execution reconciled after daemon restart".to_string(),
                },
            )?;
            Ok(())
        })?;
        reconciled += 1;
    }

    Ok(reconciled)
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

fn startup_lock_path(state_store_path: &Path) -> PathBuf {
    let mut path = state_store_path.to_path_buf();
    let new_ext = match path.extension() {
        Some(ext) => format!("{}.lock", ext.to_string_lossy()),
        None => "lock".to_string(),
    };
    path.set_extension(new_ext);
    path
}

fn placement_internal_machine_error(error: StackError, request_id: &str) -> MachineError {
    let mut details = std::collections::BTreeMap::new();
    details.insert("reason".to_string(), error.to_string());
    MachineError::new(
        MachineErrorCode::InternalError,
        format!("failed to refresh placement snapshot: {error}"),
        Some(request_id.to_string()),
        details,
    )
}

#[derive(Debug)]
struct StartupLock {
    path: PathBuf,
    _file: std::fs::File,
}

impl StartupLock {
    fn acquire(path: PathBuf) -> Result<Self, RuntimedError> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| RuntimedError::AcquireStartupLock {
                path: path.clone(),
                source,
            })?;
        if let Err(source) = file.try_lock() {
            return Err(match source {
                std::fs::TryLockError::WouldBlock => {
                    RuntimedError::StartupLockAlreadyHeld { path: path.clone() }
                }
                std::fs::TryLockError::Error(source) => RuntimedError::AcquireStartupLock {
                    path: path.clone(),
                    source,
                },
            });
        }

        let mut owner_file = &file;
        let _ = owner_file.set_len(0);
        let _ = writeln!(&mut owner_file, "pid={}", std::process::id());
        let _ = owner_file.flush();

        Ok(Self { path, _file: file })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for StartupLock {
    fn drop(&mut self) {
        let _ = self._file.unlock();
        let _ = std::fs::remove_file(&self.path);
    }
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
    #[error("failed to create socket directory for {path}: {source}")]
    CreateSocketDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("startup lock already held for state store via {path}")]
    StartupLockAlreadyHeld { path: PathBuf },
    #[error("failed to acquire startup lock {path}: {source}")]
    AcquireStartupLock {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to open daemon state store at {path}: {source}")]
    OpenStateStore {
        path: PathBuf,
        #[source]
        source: StackError,
    },
    #[error("failed to read schema version from {path}: {source}")]
    ReadSchemaVersion {
        path: PathBuf,
        #[source]
        source: StackError,
    },
    #[error("failed to reconcile execution state from {path}: {source}")]
    ReconcileExecutionState {
        path: PathBuf,
        #[source]
        source: StackError,
    },
    #[error("failed to refresh placement snapshot from {path}: {source}")]
    RefreshPlacementSnapshot {
        path: PathBuf,
        #[source]
        source: StackError,
    },
    #[error("unsupported schema version for {path}: found={found}, max_supported={max_supported}")]
    UnsupportedSchemaVersion {
        path: PathBuf,
        found: u32,
        max_supported: u32,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use vz_runtime_contract::{Execution, ExecutionSpec, ExecutionState};

    #[test]
    fn default_config_has_expected_paths() {
        let cfg = RuntimedConfig::default();
        assert_eq!(cfg.state_store_path, PathBuf::from("stack-state.db"));
        assert_eq!(cfg.runtime_data_dir, PathBuf::from(".vz-runtime"));
        assert_eq!(cfg.socket_path, PathBuf::from(".vz-runtime/runtimed.sock"));
    }

    #[test]
    fn start_creates_required_directories() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };

        let daemon = RuntimeDaemon::start(cfg.clone()).expect("daemon should start");

        assert!(cfg.runtime_data_dir.is_dir());
        assert!(cfg.state_store_path.parent().expect("parent").is_dir());
        assert!(cfg.socket_path.parent().expect("parent").is_dir());
        assert_eq!(daemon.state_store_path(), cfg.state_store_path.as_path());
        assert_eq!(daemon.runtime_data_dir(), cfg.runtime_data_dir.as_path());
        assert_eq!(daemon.socket_path(), cfg.socket_path.as_path());
        assert_eq!(
            daemon.startup_lock_path(),
            startup_lock_path(&cfg.state_store_path).as_path()
        );
    }

    #[test]
    fn startup_lock_path_uses_lock_extension() {
        assert_eq!(
            startup_lock_path(Path::new("stack-state.db")),
            PathBuf::from("stack-state.db.lock")
        );
        assert_eq!(
            startup_lock_path(Path::new("state")),
            PathBuf::from("state.lock")
        );
    }

    #[test]
    fn second_daemon_start_fails_when_lock_is_held() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RuntimedConfig {
            state_store_path: tmp.path().join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };

        let _first = RuntimeDaemon::start(cfg.clone()).expect("first daemon start should work");
        let second = RuntimeDaemon::start(cfg);
        assert!(matches!(
            second,
            Err(RuntimedError::StartupLockAlreadyHeld { .. })
        ));
    }

    #[test]
    fn startup_lock_is_released_when_daemon_drops() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RuntimedConfig {
            state_store_path: tmp.path().join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };

        {
            let _first = RuntimeDaemon::start(cfg.clone()).expect("first daemon start should work");
        }

        let second = RuntimeDaemon::start(cfg);
        assert!(second.is_ok(), "lock should be released after daemon drop");
    }

    #[test]
    fn daemon_start_applies_sqlite_pragmas() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };

        let daemon = RuntimeDaemon::start(cfg).expect("daemon should start");
        let (journal_mode, busy_timeout_ms, foreign_keys) = daemon
            .with_state_store(|store| {
                Ok((
                    store.journal_mode()?,
                    store.busy_timeout_ms()?,
                    store.foreign_keys_enabled()?,
                ))
            })
            .expect("state-store pragmas readable");

        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        assert_eq!(busy_timeout_ms, 5_000);
        assert!(foreign_keys);
    }

    #[test]
    fn daemon_start_rejects_unsupported_schema_version() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };

        std::fs::create_dir_all(cfg.state_store_path.parent().expect("state parent"))
            .expect("create state directory");
        let store = StateStore::open(&cfg.state_store_path).expect("state store");
        store
            .set_schema_version(MAX_SUPPORTED_SCHEMA_VERSION + 1)
            .expect("set schema version");
        drop(store);

        let result = RuntimeDaemon::start(cfg.clone());
        assert!(matches!(
            result,
            Err(RuntimedError::UnsupportedSchemaVersion {
                found,
                max_supported,
                ..
            }) if found == MAX_SUPPORTED_SCHEMA_VERSION + 1
                && max_supported == MAX_SUPPORTED_SCHEMA_VERSION
        ));
    }

    #[test]
    fn daemon_start_reconciles_non_terminal_executions_to_failed() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };

        std::fs::create_dir_all(cfg.state_store_path.parent().expect("state parent"))
            .expect("create state directory");
        let store = StateStore::open(&cfg.state_store_path).expect("state store");
        store
            .save_execution(&Execution {
                execution_id: "exec-queued".to_string(),
                container_id: "ctr-1".to_string(),
                exec_spec: ExecutionSpec {
                    cmd: vec!["echo".to_string()],
                    args: vec![],
                    env_override: BTreeMap::new(),
                    pty: false,
                    timeout_secs: None,
                },
                state: ExecutionState::Queued,
                exit_code: None,
                started_at: None,
                ended_at: None,
            })
            .expect("save queued execution");
        store
            .save_execution(&Execution {
                execution_id: "exec-running".to_string(),
                container_id: "ctr-2".to_string(),
                exec_spec: ExecutionSpec {
                    cmd: vec!["sleep".to_string(), "1".to_string()],
                    args: vec![],
                    env_override: BTreeMap::new(),
                    pty: false,
                    timeout_secs: None,
                },
                state: ExecutionState::Running,
                exit_code: None,
                started_at: Some(1),
                ended_at: None,
            })
            .expect("save running execution");
        store
            .save_execution(&Execution {
                execution_id: "exec-exited".to_string(),
                container_id: "ctr-3".to_string(),
                exec_spec: ExecutionSpec {
                    cmd: vec!["true".to_string()],
                    args: vec![],
                    env_override: BTreeMap::new(),
                    pty: false,
                    timeout_secs: None,
                },
                state: ExecutionState::Exited,
                exit_code: Some(0),
                started_at: Some(1),
                ended_at: Some(2),
            })
            .expect("save exited execution");
        drop(store);

        let daemon = RuntimeDaemon::start(cfg).expect("daemon should start");

        let queued = daemon
            .with_state_store(|store| store.load_execution("exec-queued"))
            .expect("load queued execution")
            .expect("queued execution should exist");
        assert_eq!(queued.state, ExecutionState::Failed);
        assert!(queued.started_at.is_some());
        assert!(queued.ended_at.is_some());

        let running = daemon
            .with_state_store(|store| store.load_execution("exec-running"))
            .expect("load running execution")
            .expect("running execution should exist");
        assert_eq!(running.state, ExecutionState::Failed);
        assert!(running.started_at.is_some());
        assert!(running.ended_at.is_some());

        let exited = daemon
            .with_state_store(|store| store.load_execution("exec-exited"))
            .expect("load exited execution")
            .expect("exited execution should exist");
        assert_eq!(exited.state, ExecutionState::Exited);
        assert_eq!(exited.exit_code, Some(0));
    }
}
