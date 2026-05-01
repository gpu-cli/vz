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

/// One block device attached to the VM.
///
/// Disks are presented to the guest as virtio-block devices in the order
/// they were appended to the builder — the first disk is `vda`, the second
/// `vdb`, and so on. Apple's `setStorageDevices_` accepts an array of
/// configurations, and consumers running structured microVM workloads
/// typically need an ordered set (rootfs, data, metadata, override) rather
/// than a single image with a partition table.
#[derive(Debug, Clone)]
pub struct DiskConfig {
    /// Stable identifier used for logging and (future) hot-replace flows.
    /// Not visible to the guest — the guest sees `vda`/`vdb`/... by order.
    pub id: String,
    /// Host-side path to the disk image.
    pub path: PathBuf,
    /// If true, the guest cannot write to this disk.
    pub read_only: bool,
}

/// Builder for VM configuration.
#[derive(Debug)]
pub struct VmConfigBuilder {
    cpus: u32,
    memory_bytes: u64,
    boot_loader: Option<BootLoader>,
    mac_platform: Option<MacPlatformConfig>,
    disks: Vec<DiskConfig>,
    shared_dirs: Vec<SharedDirConfig>,
    serial_log_file: Option<PathBuf>,
    generic_machine_identifier: Option<Vec<u8>>,
    network: NetworkConfig,
    network_mac: Option<String>,
    vsock: bool,
    headless: bool,
    nested_virtualization: bool,
    memory_balloon: bool,
}

impl VmConfigBuilder {
    /// Create a new builder with sensible defaults.
    pub fn new() -> Self {
        Self {
            cpus: 2,
            memory_bytes: 4 * 1024 * 1024 * 1024, // 4 GB
            boot_loader: None,
            mac_platform: None,
            disks: Vec::new(),
            shared_dirs: Vec::new(),
            serial_log_file: None,
            generic_machine_identifier: None,
            network: NetworkConfig::Nat,
            network_mac: None,
            vsock: false,
            headless: true,
            nested_virtualization: true,
            memory_balloon: true,
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

    /// Append a disk to the VM's storage devices.
    ///
    /// Disks appear in the guest as virtio-block devices in declaration order:
    /// the first appended disk is `vda`, the second `vdb`, and so on. For
    /// macOS guests the first disk must be the rootfs.
    pub fn disk(mut self, disk: DiskConfig) -> Self {
        self.disks.push(disk);
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

    /// Set a persisted generic machine identifier for Linux VM save/restore.
    pub fn generic_machine_identifier(mut self, machine_identifier: Vec<u8>) -> Self {
        self.generic_machine_identifier = Some(machine_identifier);
        self
    }

    /// Set network configuration.
    pub fn network(mut self, config: NetworkConfig) -> Self {
        self.network = config;
        self
    }

    /// Pin the VM's MAC address to a specific value.
    ///
    /// Format is `"XX:XX:XX:XX:XX:XX"` (six hex bytes, colon-separated).
    /// When unset, `build()` generates a fresh random locally-administered MAC.
    /// Setting an explicit MAC is useful when the consumer derives a deterministic
    /// MAC from a stable VM identity (e.g., for traffic correlation across restarts).
    pub fn mac(mut self, mac: impl Into<String>) -> Self {
        self.network_mac = Some(mac.into());
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

    /// Enable nested virtualization for Linux guests.
    ///
    /// When enabled, the guest VM exposes `/dev/kvm`, allowing it to run
    /// hypervisors like Firecracker or Cloud Hypervisor inside the guest.
    /// Only supported on `VZGenericPlatformConfiguration` (Linux guests).
    /// Requires Apple Silicon with Virtualization.framework support.
    pub fn nested_virtualization(mut self, enabled: bool) -> Self {
        self.nested_virtualization = enabled;
        self
    }

    /// Enable or disable the virtio memory balloon device. Default: enabled.
    ///
    /// When enabled, the host can call [`Vm::set_target_memory_size`] at runtime
    /// to ask the guest to release pages back to the host (or to give them back).
    /// Apple's framework allows at most one balloon device per VM, so this is
    /// a simple on/off knob — there is nothing else to configure.
    pub fn memory_balloon(mut self, enabled: bool) -> Self {
        self.memory_balloon = enabled;
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

        // macOS must boot from disk; Linux can boot from initramfs alone.
        if matches!(boot_loader, BootLoader::MacOS) && self.disks.is_empty() {
            return Err(VzError::InvalidConfig(
                "macOS boot requires at least one disk".into(),
            ));
        }

        let network_mac = match self.network_mac {
            Some(mac) => mac,
            None => crate::bridge::random_locally_administered_mac_string(),
        };

        Ok(VmConfig {
            cpus: self.cpus,
            memory_bytes: self.memory_bytes,
            boot_loader,
            mac_platform: self.mac_platform,
            disks: self.disks,
            shared_dirs: self.shared_dirs,
            serial_log_file: self.serial_log_file,
            generic_machine_identifier: self.generic_machine_identifier,
            network: self.network,
            network_mac,
            vsock: self.vsock,
            headless: self.headless,
            nested_virtualization: self.nested_virtualization,
            memory_balloon: self.memory_balloon,
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
    /// Block devices in declaration order — guest sees them as `vda`, `vdb`, ...
    pub(crate) disks: Vec<DiskConfig>,
    pub(crate) shared_dirs: Vec<SharedDirConfig>,
    pub(crate) serial_log_file: Option<PathBuf>,
    pub(crate) generic_machine_identifier: Option<Vec<u8>>,
    pub(crate) network: NetworkConfig,
    /// MAC address in `"XX:XX:XX:XX:XX:XX"` form. Always populated by `build()`.
    /// Persisting it here keeps save/restore correct (the restored VM's NIC must
    /// match the MAC the saved guest expects) and gives every VM a unique address
    /// when more than one runs in the same process.
    pub(crate) network_mac: String,
    pub(crate) vsock: bool,
    /// Controls whether to attach a virtual display. Used by CLI layer.
    #[allow(dead_code)]
    pub(crate) headless: bool,
    /// Enable nested virtualization (exposes /dev/kvm in Linux guests).
    pub(crate) nested_virtualization: bool,
    /// Attach a virtio memory balloon device. Required for runtime memory
    /// reclaim via [`Vm::set_target_memory_size`].
    pub(crate) memory_balloon: bool,
}

impl VmConfig {
    /// Read the MAC address assigned to this VM's primary NIC.
    ///
    /// Always returns a value: either what the caller passed via
    /// `VmConfigBuilder::mac()`, or a fresh random locally-administered
    /// address generated at `build()` time.
    pub fn mac_address(&self) -> &str {
        &self.network_mac
    }

    /// Read the ordered list of disks attached to this VM.
    ///
    /// The first entry is `vda` in the guest, the second `vdb`, and so on.
    pub fn disks(&self) -> &[DiskConfig] {
        &self.disks
    }

    /// Whether this VM was built with a virtio memory balloon device.
    ///
    /// When `true`, the host can call [`Vm::set_target_memory_size`] to ask
    /// the guest to release pages back to the host.
    pub fn memory_balloon_enabled(&self) -> bool {
        self.memory_balloon
    }
}
