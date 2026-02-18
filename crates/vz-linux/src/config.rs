use std::path::PathBuf;

use vz::config::VmConfig;
use vz::{NetworkConfig, SharedDirConfig, VmConfigBuilder};

use crate::LinuxError;

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
    /// Optional container rootfs directory exposed as VirtioFS `rootfs` tag.
    ///
    /// When set, initramfs mounts this share and switches into an overlay-backed
    /// root filesystem before starting the guest agent.
    pub rootfs_dir: Option<PathBuf>,
    /// Optional file path for guest serial console output.
    pub serial_log_file: Option<PathBuf>,
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

    /// Set an optional rootfs directory for container-style boot.
    pub fn with_rootfs_dir(mut self, rootfs_dir: impl Into<PathBuf>) -> Self {
        self.rootfs_dir = Some(rootfs_dir.into());
        self
    }

    /// Validate config values and required file paths.
    pub fn validate(&self) -> Result<(), LinuxError> {
        if self.kernel.as_os_str().is_empty() {
            return Err(LinuxError::InvalidConfig(
                "kernel path must not be empty".to_string(),
            ));
        }
        if self.initramfs.as_os_str().is_empty() {
            return Err(LinuxError::InvalidConfig(
                "initramfs path must not be empty".to_string(),
            ));
        }
        if self.cpus == 0 {
            return Err(LinuxError::InvalidConfig(
                "cpus must be greater than 0".to_string(),
            ));
        }
        if self.memory_mb == 0 {
            return Err(LinuxError::InvalidConfig(
                "memory_mb must be greater than 0".to_string(),
            ));
        }
        if !self.kernel.exists() {
            return Err(LinuxError::InvalidConfig(format!(
                "kernel file does not exist: {}",
                self.kernel.display()
            )));
        }
        if !self.initramfs.exists() {
            return Err(LinuxError::InvalidConfig(format!(
                "initramfs file does not exist: {}",
                self.initramfs.display()
            )));
        }

        if let Some(rootfs_dir) = &self.rootfs_dir {
            if !rootfs_dir.exists() {
                return Err(LinuxError::InvalidConfig(format!(
                    "rootfs directory does not exist: {}",
                    rootfs_dir.display()
                )));
            }

            if !rootfs_dir.is_dir() {
                return Err(LinuxError::InvalidConfig(format!(
                    "rootfs path is not a directory: {}",
                    rootfs_dir.display()
                )));
            }

            if self.shared_dirs.iter().any(|d| d.tag == "rootfs") {
                return Err(LinuxError::InvalidConfig(
                    "shared_dirs must not contain tag 'rootfs' when rootfs_dir is set".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Convert to a base `vz::VmConfig`.
    pub fn to_vm_config(&self) -> Result<VmConfig, LinuxError> {
        let mut shared_dirs =
            Vec::with_capacity(self.shared_dirs.len() + usize::from(self.rootfs_dir.is_some()));

        if let Some(rootfs_dir) = &self.rootfs_dir {
            shared_dirs.push(SharedDirConfig {
                tag: "rootfs".to_string(),
                source: rootfs_dir.clone(),
                read_only: true,
            });
        }
        shared_dirs.extend(self.shared_dirs.clone());

        let mut builder = VmConfigBuilder::new()
            .cpus(u32::from(self.cpus))
            .memory_mb(self.memory_mb)
            .boot_linux(
                self.kernel.clone(),
                Some(self.initramfs.clone()),
                self.cmdline.clone(),
            )
            .shared_dirs(shared_dirs);

        if let Some(serial_log_file) = &self.serial_log_file {
            builder = builder.serial_log_file(serial_log_file.clone());
        }

        if self.vsock {
            builder = builder.enable_vsock();
        }

        if let Some(network) = &self.network {
            builder = builder.network(network.clone());
        }

        Ok(builder.build()?)
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
            rootfs_dir: None,
            serial_log_file: None,
            vsock: true,
            network: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn default_values_match_plan() {
        let cfg = LinuxVmConfig::default();
        assert_eq!(cfg.cmdline, "console=hvc0 quiet");
        assert_eq!(cfg.cpus, 2);
        assert_eq!(cfg.memory_mb, 512);
        assert!(cfg.vsock);
        assert!(cfg.network.is_none());
        assert!(cfg.rootfs_dir.is_none());
        assert!(cfg.serial_log_file.is_none());
    }

    #[test]
    fn validate_fails_without_paths() {
        let cfg = LinuxVmConfig::default();
        let err = cfg.validate();
        assert!(err.is_err());
    }

    #[test]
    fn to_vm_config_linux_boot_without_disk() {
        let tmp = tempdir().expect("tempdir");
        let kernel = tmp.path().join("vmlinux");
        let initramfs = tmp.path().join("initramfs.img");

        fs::write(&kernel, b"kernel").expect("write kernel");
        fs::write(&initramfs, b"initramfs").expect("write initramfs");

        let cfg = LinuxVmConfig::new(&kernel, &initramfs);
        let vm_cfg = cfg.to_vm_config();
        assert!(vm_cfg.is_ok());
    }

    #[test]
    fn validate_fails_when_rootfs_dir_missing() {
        let tmp = tempdir().expect("tempdir");
        let kernel = tmp.path().join("vmlinux");
        let initramfs = tmp.path().join("initramfs.img");
        fs::write(&kernel, b"kernel").expect("write kernel");
        fs::write(&initramfs, b"initramfs").expect("write initramfs");

        let cfg = LinuxVmConfig::new(&kernel, &initramfs)
            .with_rootfs_dir(tmp.path().join("missing-rootfs"));

        let err = cfg.validate().expect_err("missing rootfs must fail");
        assert!(err.to_string().contains("rootfs directory does not exist"));
    }

    #[test]
    fn validate_rejects_duplicate_rootfs_tag() {
        let tmp = tempdir().expect("tempdir");
        let kernel = tmp.path().join("vmlinux");
        let initramfs = tmp.path().join("initramfs.img");
        let rootfs = tmp.path().join("rootfs");
        fs::write(&kernel, b"kernel").expect("write kernel");
        fs::write(&initramfs, b"initramfs").expect("write initramfs");
        fs::create_dir_all(&rootfs).expect("create rootfs");

        let cfg = LinuxVmConfig::new(&kernel, &initramfs).with_rootfs_dir(&rootfs);
        let mut cfg = cfg;
        cfg.shared_dirs.push(SharedDirConfig {
            tag: "rootfs".to_string(),
            source: rootfs,
            read_only: true,
        });

        let err = cfg.validate().expect_err("duplicate rootfs tag must fail");
        assert!(
            err.to_string()
                .contains("shared_dirs must not contain tag 'rootfs'")
        );
    }
}
