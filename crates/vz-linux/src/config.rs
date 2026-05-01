use std::path::PathBuf;

use vz::config::VmConfig;
use vz::{DiskConfig, NetworkConfig, SharedDirConfig, VmConfigBuilder};

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
    /// Opaque machine identifier payload for generic Linux platform config.
    ///
    /// Persist this across boots when using VM save/restore snapshots.
    pub machine_identifier: Option<Vec<u8>>,
    /// Enable vsock.
    pub vsock: bool,
    /// Optional network config.
    pub network: Option<NetworkConfig>,
    /// Optional disk image to attach as a VirtioBlock device.
    ///
    /// Used for persistent named volumes — an ext4 filesystem image
    /// that is mounted inside the guest at `/run/vz-oci/volumes`.
    pub disk_image: Option<PathBuf>,
    /// Enable nested virtualization (exposes `/dev/kvm` in the guest).
    ///
    /// When enabled, the guest can run hypervisors like Firecracker or
    /// Cloud Hypervisor. Requires Apple Silicon with Virtualization.framework
    /// nested virtualization support and a guest kernel with `CONFIG_KVM=y`.
    pub nested_virtualization: bool,
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
        if let Some(machine_identifier) = &self.machine_identifier
            && machine_identifier.is_empty()
        {
            return Err(LinuxError::InvalidConfig(
                "machine_identifier must not be empty".to_string(),
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

    fn ordered_shared_dirs(&self) -> Vec<SharedDirConfig> {
        let mut shared_dirs = self.shared_dirs.clone();
        shared_dirs.sort_by(|left, right| {
            left.tag
                .cmp(&right.tag)
                .then_with(|| left.source.cmp(&right.source))
                .then_with(|| left.read_only.cmp(&right.read_only))
        });

        if let Some(rootfs_dir) = &self.rootfs_dir {
            let mut ordered = Vec::with_capacity(shared_dirs.len() + 1);
            ordered.push(SharedDirConfig {
                tag: "rootfs".to_string(),
                source: rootfs_dir.clone(),
                read_only: false,
            });
            ordered.extend(shared_dirs);
            ordered
        } else {
            shared_dirs
        }
    }

    /// Convert to a base `vz::VmConfig`.
    pub fn to_vm_config(&self) -> Result<VmConfig, LinuxError> {
        let shared_dirs = self.ordered_shared_dirs();

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
        if let Some(machine_identifier) = &self.machine_identifier {
            builder = builder.generic_machine_identifier(machine_identifier.clone());
        }

        if self.vsock {
            builder = builder.enable_vsock();
        }

        if let Some(network) = &self.network {
            builder = builder.network(network.clone());
        }

        if let Some(disk_image) = &self.disk_image {
            builder = builder.disk(DiskConfig {
                id: "rootfs".into(),
                path: disk_image.clone(),
                read_only: false,
            });
        }

        if self.nested_virtualization {
            builder = builder.nested_virtualization(true);
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
            machine_identifier: None,
            vsock: true,
            network: None,
            disk_image: None,
            nested_virtualization: true,
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

    #[test]
    fn ordered_shared_dirs_places_rootfs_first_and_sorts_remaining() {
        let mut cfg = LinuxVmConfig::default();
        cfg.rootfs_dir = Some(PathBuf::from("/tmp/rootfs"));
        cfg.shared_dirs = vec![
            SharedDirConfig {
                tag: "mount-z".to_string(),
                source: PathBuf::from("/tmp/z"),
                read_only: false,
            },
            SharedDirConfig {
                tag: "mount-a".to_string(),
                source: PathBuf::from("/tmp/b"),
                read_only: false,
            },
            SharedDirConfig {
                tag: "mount-a".to_string(),
                source: PathBuf::from("/tmp/a"),
                read_only: true,
            },
        ];

        let ordered = cfg.ordered_shared_dirs();
        assert_eq!(ordered.len(), 4);
        assert_eq!(ordered[0].tag, "rootfs");
        assert_eq!(ordered[0].source, PathBuf::from("/tmp/rootfs"));
        assert!(!ordered[0].read_only);
        assert_eq!(ordered[1].tag, "mount-a");
        assert_eq!(ordered[1].source, PathBuf::from("/tmp/a"));
        assert_eq!(ordered[2].tag, "mount-a");
        assert_eq!(ordered[2].source, PathBuf::from("/tmp/b"));
        assert_eq!(ordered[3].tag, "mount-z");
    }

    #[test]
    fn ordered_shared_dirs_sorts_by_tag_source_and_access_mode() {
        let mut cfg = LinuxVmConfig::default();
        cfg.shared_dirs = vec![
            SharedDirConfig {
                tag: "mount-b".to_string(),
                source: PathBuf::from("/tmp/share"),
                read_only: false,
            },
            SharedDirConfig {
                tag: "mount-a".to_string(),
                source: PathBuf::from("/tmp/share"),
                read_only: false,
            },
            SharedDirConfig {
                tag: "mount-a".to_string(),
                source: PathBuf::from("/tmp/share"),
                read_only: true,
            },
        ];

        let ordered = cfg.ordered_shared_dirs();
        assert_eq!(ordered.len(), 3);
        assert_eq!(ordered[0].tag, "mount-a");
        assert_eq!(ordered[0].source, PathBuf::from("/tmp/share"));
        assert!(!ordered[0].read_only);
        assert_eq!(ordered[1].tag, "mount-a");
        assert_eq!(ordered[1].source, PathBuf::from("/tmp/share"));
        assert!(ordered[1].read_only);
        assert_eq!(ordered[2].tag, "mount-b");
    }

    #[test]
    fn initramfs_overlay_path_uses_writable_lower_and_upper() {
        let init_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("linux/initramfs/init");
        let script = fs::read_to_string(&init_script).expect("read initramfs init script");

        assert!(script.contains("lowerdir=/mnt/rootfs"));
        assert!(script.contains("upperdir=/run/vz-oci/overlay/upper"));
        assert!(script.contains("workdir=/run/vz-oci/overlay/work"));
        // VirtioFS rootfs share is kept rw so the bind mount at /vz-rootfs
        // can be rw for the OCI runtime.
        assert!(!script.contains("remount,ro /mnt/rootfs"));
    }
}
