//! VM configuration builder.

use std::path::PathBuf;

use crate::VzError;

/// How to boot the VM.
#[derive(Debug, Clone)]
pub enum BootLoader {
    /// Boot macOS from a disk image.
    ///
    /// Requires `MacPlatformConfig` to be set via `VmConfigBuilder::mac_platform`.
    MacOS,
    /// Boot Linux with a kernel, optional initrd, and command line.
    Linux {
        kernel: PathBuf,
        initrd: Option<PathBuf>,
        cmdline: String,
    },
}

/// macOS platform configuration for Apple Silicon VMs.
///
/// These files are generated during `install_macos` and must be preserved
/// across VM restarts. They identify the virtual hardware to the guest OS.
#[derive(Debug, Clone)]
pub struct MacPlatformConfig {
    /// Path to the hardware model data file.
    /// Created during macOS installation from the IPSW restore image.
    pub hardware_model_path: PathBuf,
    /// Path to the machine identifier data file.
    /// A unique identifier for this VM instance.
    pub machine_identifier_path: PathBuf,
    /// Path to the auxiliary storage file (NVRAM equivalent).
    /// Contains boot configuration and OS settings.
    pub auxiliary_storage_path: PathBuf,
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
    mac_platform: Option<MacPlatformConfig>,
    disk_path: Option<PathBuf>,
    disk_size_bytes: Option<u64>,
    shared_dirs: Vec<SharedDirConfig>,
    serial_log_file: Option<PathBuf>,
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
            mac_platform: None,
            disk_path: None,
            disk_size_bytes: None,
            shared_dirs: Vec::new(),
            serial_log_file: None,
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

    /// Set memory in megabytes.
    pub fn memory_mb(mut self, mb: u64) -> Self {
        self.memory_bytes = mb * 1024 * 1024;
        self
    }

    /// Set memory in bytes.
    pub fn memory_bytes(mut self, bytes: u64) -> Self {
        self.memory_bytes = bytes;
        self
    }

    /// Set the boot loader.
    pub fn boot_loader(mut self, loader: BootLoader) -> Self {
        self.boot_loader = Some(loader);
        self
    }

    /// Convenience: configure macOS boot.
    pub fn boot_macos(mut self) -> Self {
        self.boot_loader = Some(BootLoader::MacOS);
        self
    }

    /// Convenience: configure Linux boot.
    pub fn boot_linux<K, I, C>(mut self, kernel: K, initrd: Option<I>, cmdline: C) -> Self
    where
        K: Into<PathBuf>,
        I: Into<PathBuf>,
        C: Into<String>,
    {
        self.boot_loader = Some(BootLoader::Linux {
            kernel: kernel.into(),
            initrd: initrd.map(Into::into),
            cmdline: cmdline.into(),
        });
        self
    }

    /// Set the macOS platform configuration.
    ///
    /// Required when using `BootLoader::MacOS`. Provides the hardware model,
    /// machine identifier, and auxiliary storage paths created during installation.
    pub fn mac_platform(mut self, config: MacPlatformConfig) -> Self {
        self.mac_platform = Some(config);
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

    /// Add multiple shared directories (VirtioFS).
    pub fn shared_dirs(mut self, configs: Vec<SharedDirConfig>) -> Self {
        self.shared_dirs.extend(configs);
        self
    }

    /// Write guest serial console output to a host file.
    pub fn serial_log_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.serial_log_file = Some(path.into());
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

        // macOS boot requires platform configuration
        if matches!(boot_loader, BootLoader::MacOS) && self.mac_platform.is_none() {
            return Err(VzError::InvalidConfig(
                "macOS boot loader requires mac_platform configuration".into(),
            ));
        }

        let disk_path = match &boot_loader {
            BootLoader::MacOS => Some(self.disk_path.ok_or_else(|| {
                VzError::InvalidConfig("macOS boot requires a disk image".into())
            })?),
            BootLoader::Linux { .. } => self.disk_path,
        };

        Ok(VmConfig {
            cpus: self.cpus,
            memory_bytes: self.memory_bytes,
            boot_loader,
            mac_platform: self.mac_platform,
            disk_path,
            disk_size_bytes: self.disk_size_bytes,
            shared_dirs: self.shared_dirs,
            serial_log_file: self.serial_log_file,
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
#[derive(Debug, Clone)]
pub struct VmConfig {
    pub(crate) cpus: u32,
    pub(crate) memory_bytes: u64,
    pub(crate) boot_loader: BootLoader,
    pub(crate) mac_platform: Option<MacPlatformConfig>,
    pub(crate) disk_path: Option<PathBuf>,
    /// Used when creating new disk images (e.g., during install).
    #[allow(dead_code)]
    pub(crate) disk_size_bytes: Option<u64>,
    pub(crate) shared_dirs: Vec<SharedDirConfig>,
    pub(crate) serial_log_file: Option<PathBuf>,
    pub(crate) network: NetworkConfig,
    pub(crate) vsock: bool,
    /// Controls whether to attach a virtual display. Used by CLI layer.
    #[allow(dead_code)]
    pub(crate) headless: bool,
}
