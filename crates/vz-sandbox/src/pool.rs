//! Pre-warmed VM pool for fast sandbox acquisition.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use vz::Vm;

use crate::channel::Channel;
use crate::error::SandboxError;
use crate::protocol::{self, Request, Response};
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
            workspace_mount: PathBuf::new(),
            agent_port: protocol::AGENT_PORT,
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
    next_exec_id: Arc<AtomicU64>,
}

impl SandboxPool {
    /// Create a new pool and pre-warm the specified number of VMs.
    ///
    /// `pool_size` is clamped to 2 for macOS guests (kernel limit).
    /// If `state_path` is set in config, VMs restore from saved state (~5-10s).
    /// Otherwise, VMs cold boot (~30-60s).
    pub async fn new(config: SandboxConfig, pool_size: u8) -> Result<Self, SandboxError> {
        let actual_size = pool_size.min(MAX_POOL_SIZE) as usize;

        info!(
            pool_size = actual_size,
            image = %config.image_path.display(),
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
            next_exec_id: Arc::new(AtomicU64::new(1)),
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

        // Connect to guest agent with retry (if VM is available)
        let channel = {
            let entries = self.entries.lock().await;
            let vm = entries
                .iter()
                .find(|e| e.index == slot_index)
                .and_then(|e| e.vm.clone());
            drop(entries);

            if let Some(vm) = vm {
                match self.connect_agent_to_vm(&vm).await {
                    Ok(ch) => Some(Arc::new(ch)),
                    Err(e) => {
                        warn!(slot = slot_index, error = %e, "failed to connect to guest agent");
                        None
                    }
                }
            } else {
                None
            }
        };

        Ok(SandboxSession::new(
            slot_index,
            guest_project_path,
            self.config.default_exec_timeout,
            Arc::clone(&self.next_exec_id),
            channel,
        ))
    }

    /// Release a sandbox session back to the pool.
    ///
    /// Kills any remaining child processes and marks the VM as available.
    pub async fn release(&self, session: SandboxSession) -> Result<(), SandboxError> {
        let slot_index = session.slot_index();

        // The session's channel (if any) is dropped when the session is dropped,
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

    /// Connect to the guest agent over vsock with retry and exponential backoff.
    async fn connect_agent_to_vm(
        &self,
        vm: &Vm,
    ) -> Result<Channel<Request, Response>, SandboxError> {
        let mut attempts = 0u32;
        let mut delay = Duration::from_secs(1);

        loop {
            attempts += 1;

            match vm.vsock_connect(self.config.agent_port).await {
                Ok(stream) => {
                    let channel: Channel<Request, Response> = Channel::new(stream);

                    // Verify connectivity with ping/pong handshake
                    channel
                        .send(&Request::Ping { id: 0 })
                        .await
                        .map_err(|e| SandboxError::HandshakeFailed(e.to_string()))?;

                    let resp = channel
                        .recv()
                        .await
                        .map_err(|e| SandboxError::HandshakeFailed(e.to_string()))?;

                    match resp {
                        Response::Pong { .. } => {
                            info!(attempts, "connected to guest agent");
                            return Ok(channel);
                        }
                        _ => {
                            return Err(SandboxError::HandshakeFailed(format!(
                                "expected Pong, got: {resp:?}"
                            )));
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
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> SandboxConfig {
        SandboxConfig {
            image_path: PathBuf::from("/test/base.img"),
            cpus: 4,
            memory_gb: 8,
            state_path: None,
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
    async fn default_config_values() {
        let config = SandboxConfig::default();
        assert_eq!(config.cpus, 4);
        assert_eq!(config.memory_gb, 8);
        assert_eq!(config.agent_port, protocol::AGENT_PORT);
        assert_eq!(config.isolation, IsolationMode::RestoreOnAcquire);
        assert_eq!(config.network, NetworkPolicy::None);
        assert!(config.default_exec_timeout.is_none());
    }

    #[tokio::test]
    async fn poison_entry_makes_it_unavailable() {
        let pool = SandboxPool::new(test_config(), 2).await.unwrap();
        assert_eq!(pool.available().await, 2);

        pool.poison_entry(0).await;
        assert_eq!(pool.available().await, 1);
    }
}
