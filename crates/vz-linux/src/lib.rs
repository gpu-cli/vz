//! Linux VM backend for OCI containers.

#![cfg(target_os = "macos")]
#![forbid(unsafe_code)]

use std::path::PathBuf;

use vz::config::VmConfig;
use vz::{NetworkConfig, SharedDirConfig, VmConfigBuilder, VzError};

/// Linux guest VM configuration.
#[derive(Debug, Clone)]
pub struct LinuxVmConfig {
    /// Path to the Linux kernel image.
    pub kernel: PathBuf,
    /// Path to the initramfs image.
    pub initramfs: PathBuf,
    /// Kernel command line.
    pub cmdline: String,
    /// Number of vCPUs.
    pub cpus: u8,
    /// Memory in megabytes.
    pub memory_mb: u64,
    /// VirtioFS shared directories.
    pub shared_dirs: Vec<SharedDirConfig>,
    /// Enable vsock.
    pub vsock: bool,
    /// Optional network config.
    pub network: Option<NetworkConfig>,
}

impl LinuxVmConfig {
    /// Create a config from kernel + initramfs paths.
    pub fn new(kernel: impl Into<PathBuf>, initramfs: impl Into<PathBuf>) -> Self {
        Self {
            kernel: kernel.into(),
            initramfs: initramfs.into(),
            ..Self::default()
        }
    }

    /// Convert to a base `vz::VmConfig`.
    pub fn to_vm_config(&self) -> Result<VmConfig, VzError> {
        let mut builder = VmConfigBuilder::new()
            .cpus(u32::from(self.cpus))
            .memory_mb(self.memory_mb)
            .boot_linux(
                self.kernel.clone(),
                Some(self.initramfs.clone()),
                self.cmdline.clone(),
            )
            .shared_dirs(self.shared_dirs.clone());

        if self.vsock {
            builder = builder.enable_vsock();
        }

        if let Some(network) = &self.network {
            builder = builder.network(network.clone());
        }

        builder.build()
    }
}

impl Default for LinuxVmConfig {
    fn default() -> Self {
        Self {
            kernel: PathBuf::new(),
            initramfs: PathBuf::new(),
            cmdline: "console=hvc0 quiet".to_string(),
            cpus: 2,
            memory_mb: 512,
            shared_dirs: Vec::new(),
            vsock: true,
            network: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values_match_plan() {
        let cfg = LinuxVmConfig::default();
        assert_eq!(cfg.cmdline, "console=hvc0 quiet");
        assert_eq!(cfg.cpus, 2);
        assert_eq!(cfg.memory_mb, 512);
        assert!(cfg.vsock);
        assert!(cfg.network.is_none());
    }

    #[test]
    fn to_vm_config_linux_boot_without_disk() {
        let cfg = LinuxVmConfig::new("/boot/vmlinux", "/boot/initramfs.img");
        let vm_cfg = cfg.to_vm_config();
        assert!(vm_cfg.is_ok());
    }
}
