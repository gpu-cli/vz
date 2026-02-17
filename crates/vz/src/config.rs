//! VM configuration builder.

use std::path::PathBuf;

use crate::VzError;

/// How to boot the VM.
#[derive(Debug, Clone)]
pub enum BootLoader {
    /// Boot macOS from a disk image.
    MacOS,
    /// Boot Linux with a kernel, optional initrd, and command line.
    Linux {
        kernel: PathBuf,
        initrd: Option<PathBuf>,
        cmdline: String,
    },
}

/// A directory shared between host and guest via VirtioFS.
#[derive(Debug, Clone)]
pub struct SharedDirConfig {
    /// Tag the guest uses to mount this share (e.g., `mount -t virtiofs <tag> /mnt/project`).
    pub tag: String,
    /// Host directory to share.
    pub source: PathBuf,
    /// If true, guest cannot write to this share.
    pub read_only: bool,
}

/// Network configuration for the VM.
#[derive(Debug, Clone)]
pub enum NetworkConfig {
    /// NAT networking — guest gets internet through host.
    Nat,
    /// No network — fully isolated.
    None,
}

/// Builder for VM configuration.
#[derive(Debug)]
pub struct VmConfigBuilder {
    cpus: u32,
    memory_bytes: u64,
    boot_loader: Option<BootLoader>,
    disk_path: Option<PathBuf>,
    disk_size_bytes: Option<u64>,
    shared_dirs: Vec<SharedDirConfig>,
    network: NetworkConfig,
    vsock: bool,
    headless: bool,
}

impl VmConfigBuilder {
    /// Create a new builder with sensible defaults.
    pub fn new() -> Self {
        Self {
            cpus: 2,
            memory_bytes: 4 * 1024 * 1024 * 1024, // 4 GB
            boot_loader: None,
            disk_path: None,
            disk_size_bytes: None,
            shared_dirs: Vec::new(),
            network: NetworkConfig::Nat,
            vsock: false,
            headless: true,
        }
    }

    /// Set number of CPU cores.
    pub fn cpus(mut self, cpus: u32) -> Self {
        self.cpus = cpus;
        self
    }

    /// Set memory in gigabytes.
    pub fn memory_gb(mut self, gb: u32) -> Self {
        self.memory_bytes = u64::from(gb) * 1024 * 1024 * 1024;
        self
    }

    /// Set the boot loader.
    pub fn boot_loader(mut self, loader: BootLoader) -> Self {
        self.boot_loader = Some(loader);
        self
    }

    /// Set the disk image path.
    pub fn disk(mut self, path: impl Into<PathBuf>) -> Self {
        self.disk_path = Some(path.into());
        self
    }

    /// Set the disk size in bytes (for creating new images).
    pub fn disk_size(mut self, bytes: u64) -> Self {
        self.disk_size_bytes = Some(bytes);
        self
    }

    /// Add a shared directory (VirtioFS).
    pub fn shared_dir(mut self, config: SharedDirConfig) -> Self {
        self.shared_dirs.push(config);
        self
    }

    /// Set network configuration.
    pub fn network(mut self, config: NetworkConfig) -> Self {
        self.network = config;
        self
    }

    /// Enable vsock for host↔guest communication.
    pub fn enable_vsock(mut self) -> Self {
        self.vsock = true;
        self
    }

    /// Run with a display (for debugging). Default is headless.
    pub fn with_display(mut self) -> Self {
        self.headless = false;
        self
    }

    /// Validate and build the configuration.
    pub fn build(self) -> Result<VmConfig, VzError> {
        let boot_loader = self
            .boot_loader
            .ok_or_else(|| VzError::InvalidConfig("boot loader is required".into()))?;

        let disk_path = self
            .disk_path
            .ok_or_else(|| VzError::InvalidConfig("disk path is required".into()))?;

        Ok(VmConfig {
            cpus: self.cpus,
            memory_bytes: self.memory_bytes,
            boot_loader,
            disk_path,
            disk_size_bytes: self.disk_size_bytes,
            shared_dirs: self.shared_dirs,
            network: self.network,
            vsock: self.vsock,
            headless: self.headless,
        })
    }
}

impl Default for VmConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Validated VM configuration, ready to create a VM.
#[derive(Debug)]
pub struct VmConfig {
    pub(crate) cpus: u32,
    pub(crate) memory_bytes: u64,
    pub(crate) boot_loader: BootLoader,
    pub(crate) disk_path: PathBuf,
    pub(crate) disk_size_bytes: Option<u64>,
    pub(crate) shared_dirs: Vec<SharedDirConfig>,
    pub(crate) network: NetworkConfig,
    pub(crate) vsock: bool,
    pub(crate) headless: bool,
}
