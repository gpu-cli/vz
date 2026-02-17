//! Pre-warmed VM pool for fast sandbox acquisition.

use std::path::{Path, PathBuf};

use crate::session::SandboxSession;

/// Configuration for the sandbox pool.
#[derive(Debug, Clone)]
pub struct SandboxConfig {
    /// Path to the golden disk image.
    pub image_path: PathBuf,
    /// Number of CPU cores per VM.
    pub cpus: u32,
    /// Memory in GB per VM.
    pub memory_gb: u32,
    /// Path to saved VM state for fast restore (skip cold boot).
    /// If None, VMs will cold boot (~30-60s). If Some, restore takes ~5-10s.
    pub state_path: Option<PathBuf>,
    /// Host workspace root to mount via VirtioFS.
    /// Individual sessions work within subdirectories of this mount.
    pub workspace_mount: PathBuf,
}

/// A pool of pre-warmed macOS VMs ready for use.
///
/// The pool manages VM lifecycle and provides fast session acquisition.
/// For macOS guests, the pool size is limited to 2 by Apple's kernel.
pub struct SandboxPool {
    _config: SandboxConfig,
    // Will hold: Vec<Vm> of available VMs, managed with tokio::sync::Mutex
}

impl SandboxPool {
    /// Create a new pool and pre-warm the specified number of VMs.
    ///
    /// `pool_size` is clamped to 2 for macOS guests (kernel limit).
    /// If `state_path` is set in config, VMs restore from saved state (~5-10s).
    /// Otherwise, VMs cold boot (~30-60s).
    pub async fn new(config: SandboxConfig, pool_size: u8) -> anyhow::Result<Self> {
        let _ = pool_size;
        // TODO: Phase 2
        // 1. Clamp pool_size to min(pool_size, 2)
        // 2. For each slot: create VM, restore state or cold boot
        // 3. Return pool with available VMs
        Ok(Self { _config: config })
    }

    /// Acquire a sandbox session from the pool.
    ///
    /// The `project_dir` should be a subdirectory of the workspace mount.
    /// The session's working directory inside the VM will be set to the
    /// corresponding path under the VirtioFS mount.
    pub async fn acquire(&self, project_dir: &Path) -> anyhow::Result<SandboxSession> {
        let _ = project_dir;
        // TODO: Phase 2
        // 1. Take a VM from the available pool
        // 2. Set up project directory context
        // 3. Return SandboxSession
        todo!("Phase 2: implement acquire")
    }

    /// Release a sandbox session back to the pool.
    ///
    /// Cleans up the VM's temporary state so it's ready for the next session.
    pub async fn release(&self, session: SandboxSession) -> anyhow::Result<()> {
        let _ = session;
        // TODO: Phase 2
        // 1. Clean up temp files in the VM
        // 2. Return VM to available pool
        todo!("Phase 2: implement release")
    }
}
