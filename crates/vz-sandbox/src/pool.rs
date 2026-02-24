//! Pre-warmed VM pool for fast sandbox acquisition.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use vz::Vm;
use vz::protocol::AGENT_PORT;
use vz_linux::grpc_client::GrpcAgentClient;
use vz_runtime_contract::{CheckpointLineageStore, CheckpointMetadata};

use crate::error::SandboxError;
use crate::session::SandboxSession;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the sandbox pool.
#[derive(Debug, Clone)]
pub struct SandboxConfig {
    /// Path to the golden disk image.
    pub image_path: PathBuf,

    /// Number of CPU cores per VM (default: 4).
    pub cpus: u32,

    /// Memory in GB per VM (default: 8).
    pub memory_gb: u32,

    /// Path to saved VM state for fast restore (skip cold boot).
    /// If None, VMs will cold boot (~30-60s). If Some, restore takes ~5-10s.
    pub state_path: Option<PathBuf>,

    /// Path to checkpoint lineage metadata catalog.
    ///
    /// When unset, defaults to `<state_path>.checkpoint-lineage.json` when
    /// state_path is configured, otherwise `<image_path>.checkpoint-lineage.json`.
    pub checkpoint_catalog_path: Option<PathBuf>,

    /// Host workspace root to mount via VirtioFS.
    /// Individual sessions work within subdirectories of this mount.
    pub workspace_mount: PathBuf,

    /// Vsock port where the guest agent listens (default: 7424).
    pub agent_port: u32,

    /// How to isolate sessions from each other (default: RestoreOnAcquire).
    pub isolation: IsolationMode,

    /// Network policy for sandbox VMs (default: None).
    pub network: NetworkPolicy,

    /// Default timeout for exec calls. None = no timeout.
    /// Can be overridden per-exec.
    pub default_exec_timeout: Option<Duration>,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            image_path: PathBuf::new(),
            cpus: 4,
            memory_gb: 8,
            state_path: None,
            checkpoint_catalog_path: None,
            workspace_mount: PathBuf::new(),
            agent_port: AGENT_PORT,
            isolation: IsolationMode::RestoreOnAcquire,
            network: NetworkPolicy::None,
            default_exec_timeout: None,
        }
    }
}

/// How to isolate sessions from each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationMode {
    /// Fast: VM stays running between sessions. No filesystem reset.
    /// ~0ms acquire time (reconnect to guest agent).
    Reuse,

    /// Secure: VM is restored from saved state between sessions.
    /// Every session starts from a clean, known-good snapshot.
    /// ~5-10s acquire time (restore from saved state).
    RestoreOnAcquire,
}

/// Network policy for sandbox VMs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkPolicy {
    /// No network device — maximum isolation.
    None,
    /// NAT networking — guest can reach the internet via host.
    Nat,
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

/// Maximum pool size enforced by macOS kernel (2 concurrent macOS VMs).
const MAX_POOL_SIZE: u8 = 2;

/// Maximum reconnect attempts when agent is unreachable.
const MAX_RECONNECT_ATTEMPTS: u32 = 3;
/// Default on-disk checkpoint lineage file extension.
const CHECKPOINT_LINEAGE_EXTENSION: &str = "checkpoint-lineage.json";

/// A single entry in the VM pool.
struct PoolEntry {
    /// Whether this VM is currently assigned to a session.
    in_use: bool,
    /// Whether this VM is poisoned (needs replacement).
    poisoned: bool,
    /// Unique index for this pool slot.
    index: usize,
    /// The VM backing this pool slot (None when no disk image is available).
    vm: Option<Arc<Vm>>,
}

/// A pool of pre-warmed macOS VMs ready for use.
///
/// The pool manages VM lifecycle and provides fast session acquisition.
/// For macOS guests, the pool size is limited to 2 by Apple's kernel.
pub struct SandboxPool {
    config: SandboxConfig,
    entries: Mutex<Vec<PoolEntry>>,
    lease_counter: AtomicU64,
    checkpoint_catalog_path: PathBuf,
    checkpoint_lineage: Mutex<CheckpointLineageStore>,
}

impl SandboxPool {
    /// Create a new pool and pre-warm the specified number of VMs.
    ///
    /// `pool_size` is clamped to 2 for macOS guests (kernel limit).
    /// If `state_path` is set in config, VMs restore from saved state (~5-10s).
    /// Otherwise, VMs cold boot (~30-60s).
    pub async fn new(config: SandboxConfig, pool_size: u8) -> Result<Self, SandboxError> {
        let actual_size = pool_size.min(MAX_POOL_SIZE) as usize;
        let checkpoint_catalog_path = Self::checkpoint_catalog_path(&config);
        let checkpoint_lineage = Self::load_checkpoint_lineage(&checkpoint_catalog_path)?;

        info!(
            pool_size = actual_size,
            image = %config.image_path.display(),
            checkpoint_catalog = %checkpoint_catalog_path.display(),
            isolation = ?config.isolation,
            "creating sandbox pool"
        );

        let mut entries = Vec::with_capacity(actual_size);

        for i in 0..actual_size {
            let vm = if config.image_path.exists() {
                match Self::create_vm(&config).await {
                    Ok(vm) => {
                        debug!(slot = i, "VM created and started for pool slot");
                        Some(Arc::new(vm))
                    }
                    Err(e) => {
                        warn!(slot = i, error = %e, "VM creation failed, pool slot has no VM");
                        None
                    }
                }
            } else {
                debug!(
                    slot = i,
                    path = %config.image_path.display(),
                    "no disk image found, pool slot has no VM"
                );
                None
            };

            entries.push(PoolEntry {
                in_use: false,
                poisoned: false,
                index: i,
                vm,
            });
        }

        info!(pool_size = actual_size, "sandbox pool ready");

        Ok(Self {
            config,
            entries: Mutex::new(entries),
            lease_counter: AtomicU64::new(0),
            checkpoint_catalog_path,
            checkpoint_lineage: Mutex::new(checkpoint_lineage),
        })
    }

    /// Acquire a sandbox session from the pool.
    ///
    /// The `project_dir` must be a subdirectory of the workspace mount.
    /// Returns a session with the working directory set to the
    /// corresponding path under the VirtioFS mount inside the VM.
    pub async fn acquire(&self, project_dir: &Path) -> Result<SandboxSession, SandboxError> {
        // Validate project_dir is under workspace_mount
        let relative = project_dir
            .strip_prefix(&self.config.workspace_mount)
            .map_err(|_| {
                SandboxError::ProjectOutsideWorkspace(
                    project_dir.to_path_buf(),
                    self.config.workspace_mount.clone(),
                )
            })?;

        let guest_project_path = format!("/mnt/workspace/{}", relative.display());

        // Find a free pool entry
        let slot_index = {
            let mut entries = self.entries.lock().await;
            let slot = entries
                .iter_mut()
                .find(|e| !e.in_use && !e.poisoned)
                .ok_or(SandboxError::PoolExhausted)?;
            slot.in_use = true;
            slot.index
        };

        debug!(
            slot = slot_index,
            project = %project_dir.display(),
            guest_path = %guest_project_path,
            "acquired pool slot"
        );
        let lease_id = self.next_lease_id(slot_index);

        // If RestoreOnAcquire, restore VM from saved state
        if self.config.isolation == IsolationMode::RestoreOnAcquire {
            if let Some(ref state_path) = self.config.state_path {
                let entries = self.entries.lock().await;
                if let Some(entry) = entries.iter().find(|e| e.index == slot_index) {
                    if let Some(ref vm) = entry.vm {
                        if state_path.exists() {
                            debug!(slot = slot_index, "restoring VM from saved state");
                            if let Err(e) = vm.stop().await {
                                warn!(slot = slot_index, error = %e, "failed to stop VM for restore");
                            }
                            if let Err(e) = vm.restore_state(state_path).await {
                                warn!(slot = slot_index, error = %e, "failed to restore VM state");
                            }
                            if let Err(e) = vm.resume().await {
                                warn!(slot = slot_index, error = %e, "failed to resume VM after restore");
                            }
                        }
                    }
                }
                drop(entries);
            }
        }

        // Connect to guest agent via gRPC (if VM is available)
        let grpc = {
            let entries = self.entries.lock().await;
            let vm = entries
                .iter()
                .find(|e| e.index == slot_index)
                .and_then(|e| e.vm.clone());
            drop(entries);

            if let Some(vm) = vm {
                match self.connect_agent_to_vm(vm).await {
                    Ok(client) => Arc::new(Mutex::new(Some(client))),
                    Err(e) => {
                        warn!(slot = slot_index, error = %e, "failed to connect to guest agent");
                        Arc::new(Mutex::new(None))
                    }
                }
            } else {
                Arc::new(Mutex::new(None))
            }
        };

        Ok(SandboxSession::new(
            lease_id,
            slot_index,
            guest_project_path,
            self.config.default_exec_timeout,
            grpc,
        ))
    }

    /// Validate that a session can be safely released.
    pub fn validate_release(&self, session: &SandboxSession) -> Result<(), SandboxError> {
        let pinned = session.pinned_workloads();
        if pinned.is_empty() {
            return Ok(());
        }

        let active_workloads = pinned
            .into_iter()
            .map(|(workload_id, class)| format!("{workload_id}:{class}"))
            .collect();

        Err(SandboxError::LeaseReleaseDenied {
            lease_id: session.lease_id().to_string(),
            active_workloads,
        })
    }

    /// Register checkpoint metadata in the persisted lineage catalog.
    pub async fn register_checkpoint_metadata(
        &self,
        metadata: CheckpointMetadata,
    ) -> Result<(), SandboxError> {
        let mut lineage = self.checkpoint_lineage.lock().await;
        let mut updated = lineage.clone();
        updated
            .register(metadata)
            .map_err(|err| SandboxError::CheckpointLineageViolation(err.to_string()))?;
        self.persist_checkpoint_lineage(&updated)?;
        *lineage = updated;
        Ok(())
    }

    /// Look up checkpoint metadata by id from the lineage catalog.
    pub async fn checkpoint_metadata(&self, checkpoint_id: &str) -> Option<CheckpointMetadata> {
        let lineage = self.checkpoint_lineage.lock().await;
        lineage.get(checkpoint_id).cloned()
    }

    /// List all checkpoint metadata records for a sandbox lineage.
    pub async fn checkpoint_lineage_for_sandbox(
        &self,
        sandbox_id: &str,
    ) -> Vec<CheckpointMetadata> {
        let lineage = self.checkpoint_lineage.lock().await;
        lineage.list_for_sandbox(sandbox_id)
    }

    /// List direct children for a parent checkpoint id.
    pub async fn checkpoint_children(&self, parent_checkpoint_id: &str) -> Vec<CheckpointMetadata> {
        let lineage = self.checkpoint_lineage.lock().await;
        lineage.children_of(parent_checkpoint_id)
    }

    /// Release a sandbox session back to the pool.
    ///
    /// Kills any remaining child processes and marks the VM as available.
    /// Call [`validate_release`](Self::validate_release) first when lifecycle
    /// pinning is enabled.
    pub async fn release(&self, session: SandboxSession) -> Result<(), SandboxError> {
        let slot_index = session.slot_index();

        // The session's gRPC client (if any) is dropped when the session is dropped,
        // which closes the connection to the guest agent automatically.
        drop(session);

        debug!(slot = slot_index, "releasing pool slot");

        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.iter_mut().find(|e| e.index == slot_index) {
            entry.in_use = false;
        }

        info!(slot = slot_index, "pool slot released");
        Ok(())
    }

    /// Create and start a VM from the pool configuration.
    async fn create_vm(config: &SandboxConfig) -> Result<Vm, SandboxError> {
        let mut builder = vz::VmConfigBuilder::new()
            .cpus(config.cpus)
            .memory_gb(config.memory_gb)
            .boot_loader(vz::BootLoader::MacOS)
            .disk(config.image_path.clone())
            .enable_vsock();

        // Add workspace mount if configured
        if config.workspace_mount.exists() {
            builder = builder.shared_dir(vz::SharedDirConfig {
                tag: "workspace".to_string(),
                source: config.workspace_mount.clone(),
                read_only: false,
            });
        }

        // Network configuration
        match config.network {
            NetworkPolicy::Nat => {
                builder = builder.network(vz::config::NetworkConfig::Nat);
            }
            NetworkPolicy::None => {
                builder = builder.network(vz::config::NetworkConfig::None);
            }
        }

        // Look for platform identity files alongside the disk image
        let hw_model_path = config.image_path.with_extension("hwmodel");
        let machine_id_path = config.image_path.with_extension("machineid");
        let aux_path = config.image_path.with_extension("aux");

        if hw_model_path.exists() && machine_id_path.exists() && aux_path.exists() {
            builder = builder.mac_platform(vz::MacPlatformConfig {
                hardware_model_path: hw_model_path,
                machine_identifier_path: machine_id_path,
                auxiliary_storage_path: aux_path,
            });
        }

        let vm_config = builder.build().map_err(SandboxError::Vm)?;
        let vm = Vm::create(vm_config).await.map_err(SandboxError::Vm)?;

        // Start or restore
        if let Some(ref state_path) = config.state_path {
            if state_path.exists() {
                vm.restore_state(state_path)
                    .await
                    .map_err(SandboxError::Vm)?;
                vm.resume().await.map_err(SandboxError::Vm)?;
            } else {
                vm.start().await.map_err(SandboxError::Vm)?;
            }
        } else {
            vm.start().await.map_err(SandboxError::Vm)?;
        }

        Ok(vm)
    }

    /// Connect to the guest agent over gRPC with retry and exponential backoff.
    async fn connect_agent_to_vm(&self, vm: Arc<Vm>) -> Result<GrpcAgentClient, SandboxError> {
        let mut attempts = 0u32;
        let mut delay = Duration::from_secs(1);

        loop {
            attempts += 1;

            match GrpcAgentClient::connect(Arc::clone(&vm), self.config.agent_port).await {
                Ok(mut client) => {
                    // Verify connectivity with ping
                    match client.ping().await {
                        Ok(()) => {
                            info!(attempts, "connected to guest agent via gRPC");
                            return Ok(client);
                        }
                        Err(e) => {
                            if attempts >= MAX_RECONNECT_ATTEMPTS {
                                return Err(SandboxError::HandshakeFailed(format!(
                                    "ping failed after connect: {e}"
                                )));
                            }
                            warn!(
                                attempt = attempts,
                                error = %e,
                                "ping failed after connect, retrying"
                            );
                        }
                    }
                }
                Err(e) => {
                    if attempts >= MAX_RECONNECT_ATTEMPTS {
                        return Err(SandboxError::AgentUnreachable { attempts });
                    }
                    warn!(
                        attempt = attempts,
                        delay_ms = delay.as_millis(),
                        error = %e,
                        "agent unreachable, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
            }
        }
    }

    fn checkpoint_catalog_path(config: &SandboxConfig) -> PathBuf {
        if let Some(path) = &config.checkpoint_catalog_path {
            return path.clone();
        }

        if let Some(path) = &config.state_path {
            return path.with_extension(CHECKPOINT_LINEAGE_EXTENSION);
        }

        config
            .image_path
            .with_extension(CHECKPOINT_LINEAGE_EXTENSION)
    }

    fn load_checkpoint_lineage(path: &Path) -> Result<CheckpointLineageStore, SandboxError> {
        if !path.exists() {
            return Ok(CheckpointLineageStore::default());
        }

        let bytes = fs::read(path).map_err(|err| SandboxError::CheckpointCatalogCorrupt {
            path: path.to_path_buf(),
            reason: err.to_string(),
        })?;
        serde_json::from_slice(&bytes).map_err(|err| SandboxError::CheckpointCatalogCorrupt {
            path: path.to_path_buf(),
            reason: err.to_string(),
        })
    }

    fn persist_checkpoint_lineage(
        &self,
        lineage: &CheckpointLineageStore,
    ) -> Result<(), SandboxError> {
        let path = &self.checkpoint_catalog_path;
        let bytes = serde_json::to_vec_pretty(lineage).map_err(|err| {
            SandboxError::CheckpointCatalogPersistence {
                path: path.clone(),
                reason: err.to_string(),
            }
        })?;

        Self::write_atomic(path, &bytes)
    }

    fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), SandboxError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                SandboxError::CheckpointCatalogPersistence {
                    path: path.to_path_buf(),
                    reason: err.to_string(),
                }
            })?;
        }

        let tmp_path = Self::unique_temp_path(path);
        {
            let mut file = fs::File::create(&tmp_path).map_err(|err| {
                SandboxError::CheckpointCatalogPersistence {
                    path: path.to_path_buf(),
                    reason: err.to_string(),
                }
            })?;
            file.write_all(bytes)
                .map_err(|err| SandboxError::CheckpointCatalogPersistence {
                    path: path.to_path_buf(),
                    reason: err.to_string(),
                })?;
            file.sync_all()
                .map_err(|err| SandboxError::CheckpointCatalogPersistence {
                    path: path.to_path_buf(),
                    reason: err.to_string(),
                })?;
        }

        fs::rename(&tmp_path, path).map_err(|err| SandboxError::CheckpointCatalogPersistence {
            path: path.to_path_buf(),
            reason: err.to_string(),
        })
    }

    fn unique_temp_path(path: &Path) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let pid = std::process::id();

        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("checkpoint-lineage.json");
        let temp_name = format!("{file_name}.tmp.{pid}.{timestamp}");
        let mut out = path.to_path_buf();
        out.set_file_name(temp_name);
        out
    }

    /// Mark a pool entry as poisoned (needs replacement).
    #[allow(dead_code)]
    async fn poison_entry(&self, slot_index: usize) {
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.iter_mut().find(|e| e.index == slot_index) {
            entry.poisoned = true;
            entry.in_use = false;
            warn!(slot = slot_index, "pool entry poisoned");
        }
    }

    /// Get the number of available (not in-use, not poisoned) entries.
    pub async fn available(&self) -> usize {
        let entries = self.entries.lock().await;
        entries.iter().filter(|e| !e.in_use && !e.poisoned).count()
    }

    /// Get the total pool size.
    pub async fn size(&self) -> usize {
        let entries = self.entries.lock().await;
        entries.len()
    }

    fn next_lease_id(&self, slot_index: usize) -> String {
        let seq = self.lease_counter.fetch_add(1, Ordering::Relaxed) + 1;
        format!("lease-{slot_index}-{seq}")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    use vz_runtime_contract::{
        Checkpoint, CheckpointClass, CheckpointCompatibilityMetadata, CheckpointMetadata,
        CheckpointState,
    };

    fn unique_temp_dir(name: &str) -> PathBuf {
        let mut base = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        base.push(format!(
            "vz-sandbox-checkpoint-lineage-test-{name}-{}-{nanos}",
            std::process::id(),
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    fn compatibility(version: &str) -> CheckpointCompatibilityMetadata {
        CheckpointCompatibilityMetadata {
            backend_id: "macos-vz".to_string(),
            backend_version: version.to_string(),
            runtime_version: "2".to_string(),
            guest_artifact_versions: std::collections::BTreeMap::new(),
            config_hash: "sha256:config".to_string(),
            host_compatibility_markers: std::collections::BTreeMap::new(),
        }
    }

    fn test_config() -> SandboxConfig {
        SandboxConfig {
            image_path: PathBuf::from("/test/base.img"),
            cpus: 4,
            memory_gb: 8,
            state_path: None,
            checkpoint_catalog_path: None,
            workspace_mount: PathBuf::from("/Users/dev/workspace"),
            agent_port: 7424,
            isolation: IsolationMode::Reuse,
            network: NetworkPolicy::None,
            default_exec_timeout: None,
        }
    }

    #[tokio::test]
    async fn pool_creation_clamps_size() {
        let pool = SandboxPool::new(test_config(), 5).await.unwrap();
        assert_eq!(pool.size().await, 2); // Clamped to MAX_POOL_SIZE
    }

    #[tokio::test]
    async fn pool_creation_respects_small_size() {
        let pool = SandboxPool::new(test_config(), 1).await.unwrap();
        assert_eq!(pool.size().await, 1);
    }

    #[tokio::test]
    async fn pool_available_starts_full() {
        let pool = SandboxPool::new(test_config(), 2).await.unwrap();
        assert_eq!(pool.available().await, 2);
    }

    #[tokio::test]
    async fn acquire_validates_project_dir() {
        let pool = SandboxPool::new(test_config(), 1).await.unwrap();

        // Valid project dir
        let result = pool
            .acquire(Path::new("/Users/dev/workspace/my-project"))
            .await;
        assert!(result.is_ok());

        // Release it
        pool.release(result.unwrap()).await.unwrap();
    }

    #[tokio::test]
    async fn acquire_rejects_outside_workspace() {
        let pool = SandboxPool::new(test_config(), 1).await.unwrap();

        let result = pool.acquire(Path::new("/tmp/evil-project")).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SandboxError::ProjectOutsideWorkspace(_, _)
        ));
    }

    #[tokio::test]
    async fn acquire_exhausts_pool() {
        let pool = SandboxPool::new(test_config(), 1).await.unwrap();

        // First acquire succeeds
        let session = pool
            .acquire(Path::new("/Users/dev/workspace/project1"))
            .await
            .unwrap();
        assert_eq!(pool.available().await, 0);

        // Second acquire fails (pool exhausted)
        let result = pool
            .acquire(Path::new("/Users/dev/workspace/project2"))
            .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SandboxError::PoolExhausted));

        // Release and acquire again
        pool.release(session).await.unwrap();
        assert_eq!(pool.available().await, 1);

        let _session2 = pool
            .acquire(Path::new("/Users/dev/workspace/project2"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn release_returns_slot_to_pool() {
        let pool = SandboxPool::new(test_config(), 1).await.unwrap();

        let session = pool
            .acquire(Path::new("/Users/dev/workspace/proj"))
            .await
            .unwrap();
        assert_eq!(pool.available().await, 0);

        pool.release(session).await.unwrap();
        assert_eq!(pool.available().await, 1);
    }

    #[tokio::test]
    async fn validate_release_allows_unpinned_session() {
        let pool = SandboxPool::new(test_config(), 1).await.unwrap();
        let session = pool
            .acquire(Path::new("/Users/dev/workspace/proj"))
            .await
            .unwrap();

        assert!(pool.validate_release(&session).is_ok());
        pool.release(session).await.unwrap();
    }

    #[tokio::test]
    async fn validate_release_denies_active_pinned_workloads() {
        let pool = SandboxPool::new(test_config(), 1).await.unwrap();
        let session = pool
            .acquire(Path::new("/Users/dev/workspace/proj"))
            .await
            .unwrap();

        session.pin_workload(
            "workspace-main",
            crate::session::ContainerLifecycleClass::Workspace,
        );
        session.pin_workload("svc-db", crate::session::ContainerLifecycleClass::Service);

        let err = pool.validate_release(&session).unwrap_err();
        assert!(matches!(err, SandboxError::LeaseReleaseDenied { .. }));
        if let SandboxError::LeaseReleaseDenied {
            lease_id,
            active_workloads,
        } = err
        {
            assert_eq!(lease_id, session.lease_id());
            assert_eq!(
                active_workloads,
                vec![
                    "svc-db:service".to_string(),
                    "workspace-main:workspace".to_string(),
                ],
            );
        }

        session.unpin_workload("workspace-main");
        session.unpin_workload("svc-db");
        assert!(pool.validate_release(&session).is_ok());
        pool.release(session).await.unwrap();
    }

    #[tokio::test]
    async fn default_config_values() {
        let config = SandboxConfig::default();
        assert_eq!(config.cpus, 4);
        assert_eq!(config.memory_gb, 8);
        assert_eq!(config.agent_port, AGENT_PORT);
        assert_eq!(config.isolation, IsolationMode::RestoreOnAcquire);
        assert_eq!(config.network, NetworkPolicy::None);
        assert!(config.checkpoint_catalog_path.is_none());
        assert!(config.default_exec_timeout.is_none());
    }

    #[tokio::test]
    async fn checkpoint_lineage_round_trip_persists_to_catalog() {
        let temp = unique_temp_dir("roundtrip");
        let catalog = temp.join("lineage.json");
        let mut config = test_config();
        config.checkpoint_catalog_path = Some(catalog.clone());

        let pool = SandboxPool::new(config.clone(), 1).await.unwrap();
        let root = CheckpointMetadata::new(
            Checkpoint {
                checkpoint_id: "ckpt-root".to_string(),
                sandbox_id: "sbx-1".to_string(),
                parent_checkpoint_id: None,
                class: CheckpointClass::FsQuick,
                state: CheckpointState::Ready,
                created_at: 1,
                compatibility_fingerprint: "fp-root".to_string(),
            },
            compatibility("0.1.0"),
        );
        pool.register_checkpoint_metadata(root).await.unwrap();

        let child = CheckpointMetadata::new(
            Checkpoint {
                checkpoint_id: "ckpt-child".to_string(),
                sandbox_id: "sbx-2".to_string(),
                parent_checkpoint_id: Some("ckpt-root".to_string()),
                class: CheckpointClass::VmFull,
                state: CheckpointState::Ready,
                created_at: 2,
                compatibility_fingerprint: "fp-child".to_string(),
            },
            compatibility("0.1.1"),
        );
        pool.register_checkpoint_metadata(child).await.unwrap();
        assert_eq!(pool.checkpoint_children("ckpt-root").await.len(), 1);
        drop(pool);

        let reloaded = SandboxPool::new(config, 1).await.unwrap();
        let restored = reloaded
            .checkpoint_metadata("ckpt-child")
            .await
            .expect("child checkpoint should be persisted");
        assert_eq!(
            restored.checkpoint.parent_checkpoint_id.as_deref(),
            Some("ckpt-root")
        );
        assert_eq!(
            reloaded.checkpoint_lineage_for_sandbox("sbx-2").await.len(),
            1
        );

        fs::remove_dir_all(&temp).unwrap();
    }

    #[tokio::test]
    async fn checkpoint_lineage_rejects_missing_parent() {
        let temp = unique_temp_dir("missing-parent");
        let mut config = test_config();
        config.checkpoint_catalog_path = Some(temp.join("lineage.json"));

        let pool = SandboxPool::new(config, 1).await.unwrap();
        let orphan = CheckpointMetadata::new(
            Checkpoint {
                checkpoint_id: "ckpt-orphan".to_string(),
                sandbox_id: "sbx-2".to_string(),
                parent_checkpoint_id: Some("unknown-parent".to_string()),
                class: CheckpointClass::FsQuick,
                state: CheckpointState::Creating,
                created_at: 2,
                compatibility_fingerprint: "fp-orphan".to_string(),
            },
            compatibility("0.2.0"),
        );

        let err = pool.register_checkpoint_metadata(orphan).await.unwrap_err();
        assert!(matches!(
            err,
            SandboxError::CheckpointLineageViolation(message)
                if message.contains("missing parent")
        ));

        fs::remove_dir_all(&temp).unwrap();
    }

    #[tokio::test]
    async fn poison_entry_makes_it_unavailable() {
        let pool = SandboxPool::new(test_config(), 2).await.unwrap();
        assert_eq!(pool.available().await, 2);

        pool.poison_entry(0).await;
        assert_eq!(pool.available().await, 1);
    }
}
