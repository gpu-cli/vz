use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

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
    /// Ordered block devices attached before the legacy named-volume disk.
    ///
    /// Developer Machines use this for their private Docker data disk so it
    /// remains `/dev/vda`; an optional named-volume disk then follows as
    /// `/dev/vdb`. Callers must use stable IDs and private writable images.
    pub disks: Vec<DiskConfig>,
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

        self.validate_disks()?;

        Ok(())
    }

    fn validate_disks(&self) -> Result<(), LinuxError> {
        let mut ids = BTreeSet::new();
        let mut canonical_paths = BTreeMap::new();
        let mut file_identities = BTreeMap::new();
        let disks = self
            .disks
            .iter()
            .map(|disk| (disk.id.as_str(), disk.path.as_path()))
            .chain(self.disk_image.as_deref().map(|path| ("rootfs", path)));

        for (id, path) in disks {
            if id.trim().is_empty() {
                return Err(LinuxError::InvalidConfig(
                    "disk id must not be empty".to_string(),
                ));
            }
            if !ids.insert(id.to_string()) {
                return Err(LinuxError::InvalidConfig(format!(
                    "duplicate disk id `{id}`"
                )));
            }
            validate_disk_file(id, path, &mut canonical_paths, &mut file_identities)?;
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
        self.validate()?;
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

        for disk in &self.disks {
            builder = builder.disk(disk.clone());
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

fn validate_disk_file(
    id: &str,
    path: &Path,
    canonical_paths: &mut BTreeMap<PathBuf, String>,
    file_identities: &mut BTreeMap<(u64, u64), String>,
) -> Result<(), LinuxError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        LinuxError::InvalidConfig(format!(
            "disk `{id}` image is unavailable at {}: {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(LinuxError::InvalidConfig(format!(
            "disk `{id}` image must be a regular non-symlink file: {}",
            path.display()
        )));
    }

    let canonical = fs::canonicalize(path).map_err(|error| {
        LinuxError::InvalidConfig(format!(
            "disk `{id}` image cannot be resolved at {}: {error}",
            path.display()
        ))
    })?;
    if let Some(existing) = canonical_paths.insert(canonical, id.to_string()) {
        return Err(LinuxError::InvalidConfig(format!(
            "disk `{id}` and disk `{existing}` reference the same physical image"
        )));
    }

    let file_identity = (metadata.dev(), metadata.ino());
    if let Some(existing) = file_identities.insert(file_identity, id.to_string()) {
        return Err(LinuxError::InvalidConfig(format!(
            "disk `{id}` and disk `{existing}` reference the same physical image"
        )));
    }
    Ok(())
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
            disks: Vec::new(),
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
    fn explicit_disks_precede_legacy_named_volume_disk() {
        let tmp = tempdir().expect("tempdir");
        let kernel = tmp.path().join("vmlinux");
        let initramfs = tmp.path().join("initramfs.img");
        let docker = tmp.path().join("docker.img");
        let volumes = tmp.path().join("volumes.img");
        for path in [&kernel, &initramfs, &docker, &volumes] {
            fs::write(path, b"fixture").expect("write fixture");
        }

        let mut cfg = LinuxVmConfig::new(&kernel, &initramfs);
        cfg.disks.push(DiskConfig {
            id: "docker".to_string(),
            path: docker.clone(),
            read_only: false,
        });
        cfg.disk_image = Some(volumes.clone());

        let vm = cfg.to_vm_config().expect("valid VM config");
        assert_eq!(vm.disks().len(), 2);
        assert_eq!(vm.disks()[0].id, "docker");
        assert_eq!(vm.disks()[0].path, docker);
        assert_eq!(vm.disks()[1].id, "rootfs");
        assert_eq!(vm.disks()[1].path, volumes);
    }

    #[test]
    fn validate_rejects_empty_duplicate_and_legacy_colliding_disk_ids() {
        let tmp = tempdir().expect("tempdir");
        let kernel = tmp.path().join("vmlinux");
        let initramfs = tmp.path().join("initramfs.img");
        let first = tmp.path().join("first.img");
        let second = tmp.path().join("second.img");
        let legacy = tmp.path().join("legacy.img");
        for path in [&kernel, &initramfs, &first, &second, &legacy] {
            fs::write(path, b"fixture").expect("write fixture");
        }

        let mut empty = LinuxVmConfig::new(&kernel, &initramfs);
        empty.disks.push(DiskConfig {
            id: "  ".to_string(),
            path: first.clone(),
            read_only: false,
        });
        let error = empty.validate().expect_err("blank disk id must fail");
        assert!(error.to_string().contains("disk id must not be empty"));

        let mut duplicate = LinuxVmConfig::new(&kernel, &initramfs);
        duplicate.disks.extend([
            DiskConfig {
                id: "docker".to_string(),
                path: first.clone(),
                read_only: false,
            },
            DiskConfig {
                id: "docker".to_string(),
                path: second,
                read_only: false,
            },
        ]);
        let error = duplicate
            .validate()
            .expect_err("duplicate explicit disk id must fail");
        assert!(error.to_string().contains("duplicate disk id `docker`"));

        let mut legacy_collision = LinuxVmConfig::new(&kernel, &initramfs);
        legacy_collision.disks.push(DiskConfig {
            id: "rootfs".to_string(),
            path: first,
            read_only: false,
        });
        legacy_collision.disk_image = Some(legacy);
        let error = legacy_collision
            .to_vm_config()
            .expect_err("legacy rootfs disk id collision must fail before VM creation");
        assert!(error.to_string().contains("duplicate disk id `rootfs`"));
    }

    #[test]
    fn validate_rejects_lexical_alias_and_same_inode_across_legacy_disk() {
        let tmp = tempdir().expect("tempdir");
        let kernel = tmp.path().join("vmlinux");
        let initramfs = tmp.path().join("initramfs.img");
        let disk = tmp.path().join("docker.img");
        let alias_parent = tmp.path().join("alias-parent");
        for path in [&kernel, &initramfs, &disk] {
            fs::write(path, b"fixture").expect("write fixture");
        }
        fs::create_dir(&alias_parent).expect("create alias parent");

        let mut lexical_alias = LinuxVmConfig::new(&kernel, &initramfs);
        lexical_alias.disks.push(DiskConfig {
            id: "docker".to_string(),
            path: disk.clone(),
            read_only: false,
        });
        lexical_alias.disk_image = Some(alias_parent.join("..").join("docker.img"));
        let error = lexical_alias
            .validate()
            .expect_err("lexical alias must not attach one image twice");
        assert!(error.to_string().contains("same physical image"));

        let hard_link = tmp.path().join("docker-hard-link.img");
        fs::hard_link(&disk, &hard_link).expect("create hard link");
        let mut same_inode = LinuxVmConfig::new(&kernel, &initramfs);
        same_inode.disks.push(DiskConfig {
            id: "docker".to_string(),
            path: disk,
            read_only: false,
        });
        same_inode.disk_image = Some(hard_link);
        let error = same_inode
            .validate()
            .expect_err("same inode must not be attached twice");
        assert!(error.to_string().contains("same physical image"));
    }

    #[test]
    fn validate_rejects_symlink_and_nonregular_disk_images() {
        use std::os::unix::fs::symlink;

        let tmp = tempdir().expect("tempdir");
        let kernel = tmp.path().join("vmlinux");
        let initramfs = tmp.path().join("initramfs.img");
        let disk = tmp.path().join("docker.img");
        let disk_symlink = tmp.path().join("docker-link.img");
        let directory = tmp.path().join("not-a-disk");
        for path in [&kernel, &initramfs, &disk] {
            fs::write(path, b"fixture").expect("write fixture");
        }
        symlink(&disk, &disk_symlink).expect("create disk symlink");
        fs::create_dir(&directory).expect("create nonregular disk path");

        let mut symlink_config = LinuxVmConfig::new(&kernel, &initramfs);
        symlink_config.disks.push(DiskConfig {
            id: "docker".to_string(),
            path: disk_symlink,
            read_only: false,
        });
        let error = symlink_config
            .validate()
            .expect_err("symlink disk image must fail");
        assert!(error.to_string().contains("regular non-symlink file"));

        let mut nonregular = LinuxVmConfig::new(&kernel, &initramfs);
        nonregular.disk_image = Some(directory);
        let error = nonregular
            .validate()
            .expect_err("directory disk image must fail");
        assert!(error.to_string().contains("regular non-symlink file"));
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
