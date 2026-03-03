#![forbid(unsafe_code)]

mod btrfs_health;
pub mod btrfs_portability;
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
    BuildState, ExecutionState, MachineError, MachineErrorCode, PolicyDecision, RequestMetadata,
    RuntimeCapabilities, RuntimeError, RuntimeOperation, RuntimePolicyHook,
    WorkspaceRuntimeManager, enforce_runtime_policy_hook,
};
use vz_stack::{Receipt, StackError, StackEvent, StateStore, StateStorePragmas};

pub(crate) use execution_sessions::{ExecutionSessionRegistry, ExecutionSessionRegistryError};
pub use grpc::{RuntimedServerError, serve_runtime_uds_with_shutdown};
use placement_scheduler::{BackendPlacementCandidate, PlacementScheduler, PlacementSnapshot};

const MAX_SUPPORTED_SCHEMA_VERSION: u32 = 1;
const LEGACY_SANDBOX_BASE_IMAGE_REF: &str = "debian:bookworm";
const SANDBOX_DEFAULT_BASE_IMAGE_ENV: &str = "VZ_SANDBOX_DEFAULT_BASE_IMAGE";
const SANDBOX_DEFAULT_MAIN_CONTAINER_ENV: &str = "VZ_SANDBOX_DEFAULT_MAIN_CONTAINER";
const SANDBOX_DISABLE_LEGACY_DEFAULT_ENV: &str = "VZ_SANDBOX_DISABLE_LEGACY_DEFAULT_BASE_IMAGE";
const LEGACY_CHECKPOINT_MIGRATION_ENV: &str = "VZ_RUNTIMED_MIGRATE_LEGACY_CHECKPOINT_ARTIFACTS";
const BUILD_RESTART_RECONCILE_ERROR: &str = "build reconciled after daemon restart";
const BUILD_RESTART_RECONCILE_OPERATION: &str = "reconcile_build_after_restart";
const BUILD_RESTART_RECONCILE_REQUEST_PREFIX: &str = "req-build-reconcile";
const LEGACY_CHECKPOINT_MIGRATION_OPERATION: &str = "migrate_legacy_checkpoint_artifacts";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SandboxDefaultSource {
    PolicyConfig,
    CompatLegacy,
}

impl SandboxDefaultSource {
    pub(crate) const fn as_label_value(self) -> &'static str {
        match self {
            Self::PolicyConfig => "policy_config",
            Self::CompatLegacy => "compat_legacy",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SandboxStartupPolicy {
    default_base_image_ref: Option<String>,
    default_main_container: Option<String>,
    legacy_default_base_image_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SandboxStartupResolution {
    pub(crate) base_image_ref: Option<String>,
    pub(crate) base_image_default_source: Option<SandboxDefaultSource>,
    pub(crate) main_container: Option<String>,
    pub(crate) main_container_default_source: Option<SandboxDefaultSource>,
}

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
    sandbox_startup_policy: SandboxStartupPolicy,
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
        let legacy_checkpoint_artifacts =
            detect_legacy_runtime_checkpoint_roots(&config.runtime_data_dir).map_err(|source| {
                RuntimedError::InspectLegacyCheckpointArtifacts {
                    path: config.runtime_data_dir.clone(),
                    source,
                }
            })?;
        let migrate_legacy_checkpoint_artifacts = load_legacy_checkpoint_migration_mode_enabled()?;

        let startup_lock = StartupLock::acquire(startup_lock_path(&config.state_store_path))?;
        let state_store = StateStore::open_with_pragmas(
            &config.state_store_path,
            StateStorePragmas::daemon_defaults(),
        )
        .map_err(|source| RuntimedError::OpenStateStore {
            path: config.state_store_path.clone(),
            source,
        })?;
        if !legacy_checkpoint_artifacts.is_empty() {
            if migrate_legacy_checkpoint_artifacts {
                let report = migrate_legacy_checkpoint_artifacts_to_archive(
                    &config.runtime_data_dir,
                    &legacy_checkpoint_artifacts,
                )
                .map_err(|source| {
                    RuntimedError::MigrateLegacyCheckpointArtifacts {
                        runtime_data_dir: config.runtime_data_dir.clone(),
                        source,
                    }
                })?;
                persist_legacy_checkpoint_migration_audit(&state_store, &report).map_err(
                    |source| RuntimedError::RecordLegacyCheckpointMigration {
                        path: config.state_store_path.clone(),
                        source,
                    },
                )?;
            } else {
                return Err(RuntimedError::LegacyCheckpointArtifactsIncompatible {
                    runtime_data_dir: config.runtime_data_dir.clone(),
                    migration_env: LEGACY_CHECKPOINT_MIGRATION_ENV,
                    paths: legacy_checkpoint_artifacts,
                });
            }
        }
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
        let reconciled_build_count = reconcile_orphaned_builds(&state_store).map_err(|source| {
            RuntimedError::ReconcileBuildState {
                path: config.state_store_path.clone(),
                source,
            }
        })?;

        let manager = build_runtime_manager(&config.runtime_data_dir);
        let placement_scheduler = PlacementScheduler::default();
        let sandbox_startup_policy = load_sandbox_startup_policy()?;
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
            reconciled_builds = reconciled_build_count,
            sandbox_default_base_image_ref = sandbox_startup_policy.default_base_image_ref.as_deref().unwrap_or(""),
            sandbox_default_main_container = sandbox_startup_policy.default_main_container.as_deref().unwrap_or(""),
            sandbox_legacy_base_default_enabled = sandbox_startup_policy.legacy_default_base_image_enabled,
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
            sandbox_startup_policy,
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

    pub(crate) fn resolve_sandbox_startup_defaults(
        &self,
        base_image_ref: Option<String>,
        main_container: Option<String>,
    ) -> SandboxStartupResolution {
        resolve_sandbox_startup_defaults_with_policy(
            &self.sandbox_startup_policy,
            base_image_ref,
            main_container,
        )
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
        let candidates = [BackendPlacementCandidate::available(
            self.backend_name().to_string(),
            self.capabilities(),
        )];
        self.placement_scheduler
            .evaluate_create_sandbox(&candidates, request_id)
            .map(|_| ())
    }

    pub(crate) fn enforce_create_container_placement(
        &self,
        request_id: &str,
    ) -> Result<(), MachineError> {
        self.refresh_placement_snapshot()
            .map_err(|error| placement_internal_machine_error(error, request_id))?;
        let candidates = [BackendPlacementCandidate::available(
            self.backend_name().to_string(),
            self.capabilities(),
        )];
        self.placement_scheduler
            .evaluate_create_container(&candidates, request_id)
            .map(|_| ())
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

fn detect_legacy_runtime_checkpoint_roots(
    runtime_data_dir: &Path,
) -> Result<Vec<PathBuf>, std::io::Error> {
    let sandboxes_dir = runtime_data_dir.join("sandboxes");
    if !sandboxes_dir.is_dir() {
        return Ok(Vec::new());
    }

    let mut legacy_roots = Vec::new();
    for sandbox_entry in std::fs::read_dir(&sandboxes_dir)? {
        let sandbox_entry = sandbox_entry?;
        if !sandbox_entry.file_type()?.is_dir() {
            continue;
        }
        let legacy_fs_root = sandbox_entry.path().join("fs");
        if legacy_fs_root.is_dir() {
            legacy_roots.push(legacy_fs_root);
        }
    }
    legacy_roots.sort();
    Ok(legacy_roots)
}

#[derive(Debug, Clone)]
struct LegacyCheckpointMigrationReport {
    archive_root: PathBuf,
    migrated_paths: Vec<PathBuf>,
}

#[derive(serde::Serialize)]
struct LegacyCheckpointMigrationReceiptMetadata {
    event_type: &'static str,
    migration_mode: &'static str,
    archive_root: String,
    migrated_path_count: usize,
    migrated_paths: Vec<String>,
}

fn migrate_legacy_checkpoint_artifacts_to_archive(
    runtime_data_dir: &Path,
    legacy_paths: &[PathBuf],
) -> Result<LegacyCheckpointMigrationReport, std::io::Error> {
    let now = current_unix_secs();
    let archive_root = runtime_data_dir
        .join("checkpoints")
        .join("legacy-artifacts")
        .join(format!("{now}"));
    std::fs::create_dir_all(&archive_root)?;

    let mut migrated_paths = Vec::new();
    for path in legacy_paths {
        if !path.is_dir() {
            continue;
        }
        let sandbox_id = path
            .parent()
            .and_then(Path::file_name)
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown-sandbox".to_string());
        let mut destination = archive_root.join(format!("{sandbox_id}-fs"));
        if destination.exists() {
            let mut suffix = 1_u32;
            loop {
                let candidate = archive_root.join(format!("{sandbox_id}-fs-{suffix}"));
                if !candidate.exists() {
                    destination = candidate;
                    break;
                }
                suffix = suffix.saturating_add(1);
            }
        }
        std::fs::rename(path, &destination)?;
        migrated_paths.push(destination);
    }

    migrated_paths.sort();
    Ok(LegacyCheckpointMigrationReport {
        archive_root,
        migrated_paths,
    })
}

fn persist_legacy_checkpoint_migration_audit(
    state_store: &StateStore,
    report: &LegacyCheckpointMigrationReport,
) -> Result<(), StackError> {
    let now = current_unix_secs();
    let request_id = format!("req-legacy-checkpoint-migration-{now}");
    let receipt_id = format!("rcp-legacy-checkpoint-migration-{now}");
    let metadata = serde_json::to_value(LegacyCheckpointMigrationReceiptMetadata {
        event_type: "checkpoint_failed",
        migration_mode: "archive_legacy_paths",
        archive_root: report.archive_root.display().to_string(),
        migrated_path_count: report.migrated_paths.len(),
        migrated_paths: report
            .migrated_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
    })
    .map_err(StackError::from)?;
    let migration_note = format!(
        "legacy checkpoint artifacts migrated to archive root {} ({} paths)",
        report.archive_root.display(),
        report.migrated_paths.len()
    );

    state_store.with_immediate_transaction(|tx| {
        tx.emit_event(
            "daemon",
            &StackEvent::CheckpointFailed {
                checkpoint_id: "legacy-checkpoint-layout".to_string(),
                error: migration_note.clone(),
            },
        )?;
        tx.save_receipt(&Receipt {
            receipt_id,
            operation: LEGACY_CHECKPOINT_MIGRATION_OPERATION.to_string(),
            entity_id: "daemon".to_string(),
            entity_type: "migration".to_string(),
            request_id,
            status: "success".to_string(),
            created_at: now,
            metadata,
        })?;
        Ok(())
    })
}

fn load_legacy_checkpoint_migration_mode_enabled() -> Result<bool, RuntimedError> {
    match std::env::var(LEGACY_CHECKPOINT_MIGRATION_ENV) {
        Ok(value) => parse_env_bool(&value, LEGACY_CHECKPOINT_MIGRATION_ENV),
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(std::env::VarError::NotUnicode(value)) => Err(RuntimedError::InvalidPolicyEnvValue {
            env_name: LEGACY_CHECKPOINT_MIGRATION_ENV,
            value: value.to_string_lossy().to_string(),
        }),
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

fn normalize_optional_startup_value(value: Option<&str>) -> Option<String> {
    let raw = value?.trim();
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

fn resolve_sandbox_startup_defaults_with_policy(
    policy: &SandboxStartupPolicy,
    base_image_ref: Option<String>,
    main_container: Option<String>,
) -> SandboxStartupResolution {
    let requested_base_image = normalize_optional_startup_value(base_image_ref.as_deref());
    let mut base_image_default_source = None;
    let resolved_base_image = match requested_base_image {
        Some(value) => Some(value),
        None => {
            if let Some(configured_default) = policy.default_base_image_ref.as_deref() {
                base_image_default_source = Some(SandboxDefaultSource::PolicyConfig);
                Some(configured_default.to_string())
            } else if policy.legacy_default_base_image_enabled {
                base_image_default_source = Some(SandboxDefaultSource::CompatLegacy);
                Some(LEGACY_SANDBOX_BASE_IMAGE_REF.to_string())
            } else {
                None
            }
        }
    };

    let requested_main_container = normalize_optional_startup_value(main_container.as_deref());
    let mut main_container_default_source = None;
    let resolved_main_container = match requested_main_container {
        Some(value) => Some(value),
        None => policy
            .default_main_container
            .as_deref()
            .map(|configured_default| {
                main_container_default_source = Some(SandboxDefaultSource::PolicyConfig);
                configured_default.to_string()
            }),
    };

    SandboxStartupResolution {
        base_image_ref: resolved_base_image,
        base_image_default_source,
        main_container: resolved_main_container,
        main_container_default_source,
    }
}

fn parse_env_bool(raw: &str, env_name: &'static str) -> Result<bool, RuntimedError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(RuntimedError::InvalidPolicyEnvValue {
            env_name,
            value: raw.to_string(),
        }),
    }
}

fn load_sandbox_startup_policy() -> Result<SandboxStartupPolicy, RuntimedError> {
    let default_base_image_ref = std::env::var(SANDBOX_DEFAULT_BASE_IMAGE_ENV)
        .ok()
        .and_then(|value| normalize_optional_startup_value(Some(value.as_str())));
    let default_main_container = std::env::var(SANDBOX_DEFAULT_MAIN_CONTAINER_ENV)
        .ok()
        .and_then(|value| normalize_optional_startup_value(Some(value.as_str())));

    let disable_legacy_default = match std::env::var(SANDBOX_DISABLE_LEGACY_DEFAULT_ENV) {
        Ok(value) => parse_env_bool(&value, SANDBOX_DISABLE_LEGACY_DEFAULT_ENV)?,
        Err(std::env::VarError::NotPresent) => false,
        Err(std::env::VarError::NotUnicode(value)) => {
            return Err(RuntimedError::InvalidPolicyEnvValue {
                env_name: SANDBOX_DISABLE_LEGACY_DEFAULT_ENV,
                value: value.to_string_lossy().to_string(),
            });
        }
    };

    Ok(SandboxStartupPolicy {
        default_base_image_ref,
        default_main_container,
        legacy_default_base_image_enabled: !disable_legacy_default,
    })
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

#[derive(serde::Serialize)]
struct BuildRestartReconcileReceiptMetadata<'a> {
    event_type: &'static str,
    reason: &'a str,
}

fn reconcile_orphaned_builds(state_store: &StateStore) -> Result<u64, StackError> {
    let mut reconciled: u64 = 0;
    for mut build in state_store.list_builds()? {
        if !matches!(build.state, BuildState::Queued | BuildState::Running) {
            continue;
        }

        let now = current_unix_secs();
        build.ended_at = Some(now);
        build.result_digest = None;
        build
            .transition_to(BuildState::Failed)
            .map_err(|error| StackError::Machine {
                code: MachineErrorCode::StateConflict,
                message: error.to_string(),
            })?;

        let receipt_id = format!("rcp-build-reconcile-{}-{now}", build.build_id);
        let request_id = format!(
            "{BUILD_RESTART_RECONCILE_REQUEST_PREFIX}-{}",
            build.build_id
        );
        let receipt_metadata = serde_json::to_value(BuildRestartReconcileReceiptMetadata {
            event_type: "build_failed",
            reason: BUILD_RESTART_RECONCILE_ERROR,
        })
        .map_err(StackError::from)?;

        state_store.with_immediate_transaction(|tx| {
            tx.save_build(&build)?;
            tx.emit_event(
                &build.sandbox_id,
                &StackEvent::BuildFailed {
                    build_id: build.build_id.clone(),
                    error: BUILD_RESTART_RECONCILE_ERROR.to_string(),
                },
            )?;
            tx.save_receipt(&Receipt {
                receipt_id: receipt_id.clone(),
                operation: BUILD_RESTART_RECONCILE_OPERATION.to_string(),
                entity_id: build.build_id.clone(),
                entity_type: "build".to_string(),
                request_id: request_id.clone(),
                status: "success".to_string(),
                created_at: now,
                metadata: receipt_metadata.clone(),
            })?;
            Ok(())
        })?;
        reconciled = reconciled.saturating_add(1);
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
    #[error("failed to inspect legacy checkpoint artifacts under {path}: {source}")]
    InspectLegacyCheckpointArtifacts {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to migrate legacy checkpoint artifacts under {runtime_data_dir}: {source}")]
    MigrateLegacyCheckpointArtifacts {
        runtime_data_dir: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to record legacy checkpoint migration in state store {path}: {source}")]
    RecordLegacyCheckpointMigration {
        path: PathBuf,
        #[source]
        source: StackError,
    },
    #[error(
        "legacy checkpoint artifact layout is incompatible with btrfs-only mode under {runtime_data_dir}; set {migration_env}=1 to auto-archive or migrate/remove these paths manually: {paths:?}"
    )]
    LegacyCheckpointArtifactsIncompatible {
        runtime_data_dir: PathBuf,
        migration_env: &'static str,
        paths: Vec<PathBuf>,
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
    #[error("failed to reconcile build state from {path}: {source}")]
    ReconcileBuildState {
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
    #[error("invalid sandbox startup policy env {env_name}={value}")]
    InvalidPolicyEnvValue {
        env_name: &'static str,
        value: String,
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
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use vz_runtime_contract::{
        Build, BuildSpec, BuildState, Execution, ExecutionSpec, ExecutionState,
    };

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
    fn start_rejects_legacy_runtime_checkpoint_layout() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };
        let legacy_root = cfg
            .runtime_data_dir
            .join("sandboxes")
            .join("sbx-legacy")
            .join("fs");
        std::fs::create_dir_all(&legacy_root).expect("create legacy checkpoint root");

        let error = match RuntimeDaemon::start(cfg.clone()) {
            Ok(_) => panic!("daemon should reject legacy layout"),
            Err(error) => error,
        };
        match error {
            RuntimedError::LegacyCheckpointArtifactsIncompatible {
                runtime_data_dir,
                migration_env: _,
                paths,
            } => {
                assert_eq!(runtime_data_dir, cfg.runtime_data_dir);
                assert_eq!(paths, vec![legacy_root]);
            }
            other => panic!("unexpected start error: {other}"),
        }
    }

    #[test]
    fn migrate_legacy_checkpoint_artifacts_archives_paths() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let runtime_data_dir = tmp.path().join("runtime");
        let legacy_one = runtime_data_dir
            .join("sandboxes")
            .join("sbx-one")
            .join("fs");
        let legacy_two = runtime_data_dir
            .join("sandboxes")
            .join("sbx-two")
            .join("fs");
        std::fs::create_dir_all(&legacy_one).expect("create legacy one");
        std::fs::create_dir_all(&legacy_two).expect("create legacy two");

        let report = migrate_legacy_checkpoint_artifacts_to_archive(
            &runtime_data_dir,
            &[legacy_one.clone(), legacy_two.clone()],
        )
        .expect("migration should succeed");

        assert!(
            report.archive_root.is_dir(),
            "archive root should be created: {}",
            report.archive_root.display()
        );
        assert_eq!(report.migrated_paths.len(), 2);
        assert!(
            !legacy_one.exists() && !legacy_two.exists(),
            "legacy roots should be moved to archive"
        );
        for archived in &report.migrated_paths {
            assert!(
                archived.is_dir(),
                "archived path should exist: {}",
                archived.display()
            );
            assert!(
                archived.starts_with(&report.archive_root),
                "archived path should remain under archive root"
            );
        }
    }

    #[test]
    fn persist_legacy_checkpoint_migration_audit_writes_receipt_and_event() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let state_store_path = tmp.path().join("state").join("stack-state.db");
        std::fs::create_dir_all(state_store_path.parent().expect("parent")).expect("state parent");
        let store = StateStore::open(&state_store_path).expect("state store");
        let report = LegacyCheckpointMigrationReport {
            archive_root: tmp
                .path()
                .join("runtime")
                .join("checkpoints")
                .join("legacy-artifacts"),
            migrated_paths: vec![tmp.path().join("runtime").join("legacy-a")],
        };

        persist_legacy_checkpoint_migration_audit(&store, &report)
            .expect("audit receipt/event should be written");

        let receipts = store.list_receipts().expect("list receipts");
        assert_eq!(receipts.len(), 1);
        let receipt = &receipts[0];
        assert_eq!(receipt.operation, LEGACY_CHECKPOINT_MIGRATION_OPERATION);
        assert_eq!(receipt.entity_type, "migration");
        assert_eq!(receipt.status, "success");
        assert_eq!(
            receipt
                .metadata
                .get("migrated_path_count")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );

        let events = store.load_events("daemon").expect("daemon events");
        assert_eq!(events.len(), 1);
        match &events[0] {
            StackEvent::CheckpointFailed {
                checkpoint_id,
                error,
            } => {
                assert_eq!(checkpoint_id, "legacy-checkpoint-layout");
                assert!(error.contains("legacy checkpoint artifacts migrated"));
            }
            other => panic!("unexpected migration event: {other:?}"),
        }
    }

    #[test]
    fn upgrade_restart_succeeds_after_legacy_checkpoint_migration() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };
        let legacy_root = cfg
            .runtime_data_dir
            .join("sandboxes")
            .join("sbx-legacy")
            .join("fs");
        std::fs::create_dir_all(&legacy_root).expect("create legacy checkpoint root");

        let first_start_error = match RuntimeDaemon::start(cfg.clone()) {
            Ok(_) => panic!("initial start should fail with incompatible legacy layout"),
            Err(error) => error,
        };
        match first_start_error {
            RuntimedError::LegacyCheckpointArtifactsIncompatible { paths, .. } => {
                assert_eq!(paths, vec![legacy_root.clone()]);
            }
            other => panic!("unexpected start error before migration: {other}"),
        }

        let report = migrate_legacy_checkpoint_artifacts_to_archive(
            &cfg.runtime_data_dir,
            std::slice::from_ref(&legacy_root),
        )
        .expect("legacy checkpoint migration should succeed");
        assert_eq!(report.migrated_paths.len(), 1);

        let daemon =
            RuntimeDaemon::start(cfg).expect("restart should succeed after migration cleanup");
        drop(daemon);
    }

    #[tokio::test]
    async fn server_writes_initial_metrics_snapshot_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };

        let daemon = Arc::new(RuntimeDaemon::start(cfg.clone()).expect("daemon should start"));
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let shutdown_task = shutdown.clone();
        let daemon_task = daemon.clone();
        let socket_path = cfg.socket_path.clone();

        let server = tokio::spawn(async move {
            serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
                shutdown_task.notified().await;
            })
            .await
        });

        let socket_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < socket_deadline {
            if cfg.socket_path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(cfg.socket_path.exists(), "daemon socket should be created");

        let metrics_path = cfg.runtime_data_dir.join("runtimed-grpc-metrics.prom");
        let metrics_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < metrics_deadline {
            if metrics_path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            metrics_path.exists(),
            "metrics snapshot file should be created"
        );
        let metrics_text = std::fs::read_to_string(&metrics_path).expect("read metrics snapshot");
        assert!(metrics_text.contains("vz_runtimed_grpc_requests_total"));

        shutdown.notify_waiters();
        let result = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("server join timeout")
            .expect("server join should succeed");
        assert!(result.is_ok(), "server should stop cleanly");
    }

    #[test]
    fn normalize_optional_startup_value_trims_and_drops_empty() {
        assert_eq!(
            normalize_optional_startup_value(Some("  debian:bookworm  ")),
            Some("debian:bookworm".to_string())
        );
        assert_eq!(normalize_optional_startup_value(Some("   ")), None);
        assert_eq!(normalize_optional_startup_value(None), None);
    }

    #[test]
    fn parse_env_bool_accepts_common_true_false_values() {
        assert_eq!(parse_env_bool("true", "TEST_ENV").ok(), Some(true));
        assert_eq!(parse_env_bool("1", "TEST_ENV").ok(), Some(true));
        assert_eq!(parse_env_bool("yes", "TEST_ENV").ok(), Some(true));
        assert_eq!(parse_env_bool("false", "TEST_ENV").ok(), Some(false));
        assert_eq!(parse_env_bool("0", "TEST_ENV").ok(), Some(false));
        assert_eq!(parse_env_bool("off", "TEST_ENV").ok(), Some(false));
    }

    #[test]
    fn parse_env_bool_rejects_invalid_values() {
        let err = parse_env_bool("maybe", "TEST_ENV").expect_err("invalid bool env should fail");
        assert!(matches!(
            err,
            RuntimedError::InvalidPolicyEnvValue { env_name, .. } if env_name == "TEST_ENV"
        ));
    }

    #[test]
    fn resolve_sandbox_defaults_applies_policy_and_compat_fallback() {
        let policy = SandboxStartupPolicy {
            default_base_image_ref: Some("ubuntu:24.04".to_string()),
            default_main_container: Some("workspace-main".to_string()),
            legacy_default_base_image_enabled: true,
        };
        let resolved = resolve_sandbox_startup_defaults_with_policy(&policy, None, None);
        assert_eq!(resolved.base_image_ref.as_deref(), Some("ubuntu:24.04"));
        assert_eq!(
            resolved.base_image_default_source,
            Some(SandboxDefaultSource::PolicyConfig)
        );
        assert_eq!(resolved.main_container.as_deref(), Some("workspace-main"));
        assert_eq!(
            resolved.main_container_default_source,
            Some(SandboxDefaultSource::PolicyConfig)
        );

        let compat_only_policy = SandboxStartupPolicy {
            default_base_image_ref: None,
            default_main_container: None,
            legacy_default_base_image_enabled: true,
        };
        let compat_resolved =
            resolve_sandbox_startup_defaults_with_policy(&compat_only_policy, None, None);
        assert_eq!(
            compat_resolved.base_image_ref.as_deref(),
            Some(LEGACY_SANDBOX_BASE_IMAGE_REF)
        );
        assert_eq!(
            compat_resolved.base_image_default_source,
            Some(SandboxDefaultSource::CompatLegacy)
        );
        assert!(compat_resolved.main_container.is_none());
    }

    #[test]
    fn resolve_sandbox_defaults_respects_explicit_request_values() {
        let policy = SandboxStartupPolicy {
            default_base_image_ref: Some("ubuntu:24.04".to_string()),
            default_main_container: Some("workspace-main".to_string()),
            legacy_default_base_image_enabled: true,
        };
        let resolved = resolve_sandbox_startup_defaults_with_policy(
            &policy,
            Some("debian:bookworm".to_string()),
            Some("bash -lc 'echo hi'".to_string()),
        );
        assert_eq!(resolved.base_image_ref.as_deref(), Some("debian:bookworm"));
        assert!(resolved.base_image_default_source.is_none());
        assert_eq!(
            resolved.main_container.as_deref(),
            Some("bash -lc 'echo hi'")
        );
        assert!(resolved.main_container_default_source.is_none());
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

    #[test]
    fn daemon_start_reconciles_non_terminal_builds_to_failed() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = RuntimedConfig {
            state_store_path: tmp.path().join("state").join("stack-state.db"),
            runtime_data_dir: tmp.path().join("runtime"),
            socket_path: tmp.path().join("runtime").join("runtimed.sock"),
        };

        std::fs::create_dir_all(cfg.state_store_path.parent().expect("state parent"))
            .expect("create state directory");
        let store = StateStore::open(&cfg.state_store_path).expect("state store");
        let sample_spec = BuildSpec {
            context: ".".to_string(),
            dockerfile: Some("Dockerfile".to_string()),
            target: None,
            args: BTreeMap::new(),
            cache_from: Vec::new(),
            image_tag: None,
            secrets: Vec::new(),
            no_cache: false,
            push: false,
            output_oci_tar_dest: None,
        };
        store
            .save_build(&Build {
                build_id: "build-queued".to_string(),
                sandbox_id: "sbx-build-queued".to_string(),
                build_spec: sample_spec.clone(),
                state: BuildState::Queued,
                result_digest: None,
                started_at: 1,
                ended_at: None,
            })
            .expect("save queued build");
        store
            .save_build(&Build {
                build_id: "build-running".to_string(),
                sandbox_id: "sbx-build-running".to_string(),
                build_spec: sample_spec.clone(),
                state: BuildState::Running,
                result_digest: None,
                started_at: 2,
                ended_at: None,
            })
            .expect("save running build");
        store
            .save_build(&Build {
                build_id: "build-succeeded".to_string(),
                sandbox_id: "sbx-build-succeeded".to_string(),
                build_spec: sample_spec,
                state: BuildState::Succeeded,
                result_digest: Some("sha256:deadbeef".to_string()),
                started_at: 3,
                ended_at: Some(4),
            })
            .expect("save succeeded build");
        drop(store);

        let daemon = RuntimeDaemon::start(cfg).expect("daemon should start");

        let queued = daemon
            .with_state_store(|store| store.load_build("build-queued"))
            .expect("load queued build")
            .expect("queued build should exist");
        assert_eq!(queued.state, BuildState::Failed);
        assert!(queued.ended_at.is_some());
        assert!(queued.result_digest.is_none());

        let running = daemon
            .with_state_store(|store| store.load_build("build-running"))
            .expect("load running build")
            .expect("running build should exist");
        assert_eq!(running.state, BuildState::Failed);
        assert!(running.ended_at.is_some());
        assert!(running.result_digest.is_none());

        let succeeded = daemon
            .with_state_store(|store| store.load_build("build-succeeded"))
            .expect("load succeeded build")
            .expect("succeeded build should exist");
        assert_eq!(succeeded.state, BuildState::Succeeded);
        assert_eq!(succeeded.result_digest.as_deref(), Some("sha256:deadbeef"));

        let queued_receipts = daemon
            .with_state_store(|store| store.list_receipts_for_entity("build", "build-queued"))
            .expect("load queued build receipts");
        assert_eq!(queued_receipts.len(), 1);
        assert_eq!(
            queued_receipts[0].operation,
            BUILD_RESTART_RECONCILE_OPERATION
        );

        let running_receipts = daemon
            .with_state_store(|store| store.list_receipts_for_entity("build", "build-running"))
            .expect("load running build receipts");
        assert_eq!(running_receipts.len(), 1);
        assert_eq!(
            running_receipts[0].operation,
            BUILD_RESTART_RECONCILE_OPERATION
        );

        let queued_events = daemon
            .with_state_store(|store| {
                store.load_events_by_scope("sbx-build-queued", "build_", None, 20)
            })
            .expect("load queued build events");
        assert!(queued_events.iter().any(|record| {
            matches!(
                &record.event,
                StackEvent::BuildFailed { build_id, error }
                if build_id == "build-queued" && error == BUILD_RESTART_RECONCILE_ERROR
            )
        }));

        let running_events = daemon
            .with_state_store(|store| {
                store.load_events_by_scope("sbx-build-running", "build_", None, 20)
            })
            .expect("load running build events");
        assert!(running_events.iter().any(|record| {
            matches!(
                &record.event,
                StackEvent::BuildFailed { build_id, error }
                if build_id == "build-running" && error == BUILD_RESTART_RECONCILE_ERROR
            )
        }));
    }
}
