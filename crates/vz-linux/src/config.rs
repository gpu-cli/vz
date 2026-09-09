use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use vz::config::VmConfig;
use vz::{DiskConfig, Nic, SharedDirConfig, VmConfigBuilder};

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
    /// Optional NIC list, replacing the builder's default single NAT NIC.
    ///
    /// An empty list leaves the Machine with no network at all.
    pub nics: Option<Vec<Nic>>,
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

        if let Some(nics) = &self.nics {
            builder = builder.nics(nics.clone());
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
            nics: None,
            disk_image: None,
            disks: Vec::new(),
            nested_virtualization: true,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
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
        assert!(cfg.nics.is_none());
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
        let cfg = LinuxVmConfig {
            rootfs_dir: Some(PathBuf::from("/tmp/rootfs")),
            shared_dirs: vec![
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
            ],
            ..LinuxVmConfig::default()
        };

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
        let cfg = LinuxVmConfig {
            shared_dirs: vec![
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
            ],
            ..LinuxVmConfig::default()
        };

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

    fn initramfs_init_source() -> String {
        let init_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("linux/initramfs/init");
        fs::read_to_string(&init_script).expect("read initramfs init script")
    }

    /// The `vz.net.N` block of the guest init script, relocated so it can run
    /// on this host against a fixture instead of a booted guest.
    ///
    /// The block is lifted verbatim between its markers and only its absolute
    /// paths are rewritten, so what runs below is the shell the guest runs and
    /// not a restatement of it.
    fn relocated_fabric_block(root: &std::path::Path, busybox: &std::path::Path) -> String {
        relocated_init_block(
            root,
            busybox,
            "# --- BEGIN vz.net fabric ports (extracted verbatim by vz-linux tests) ---",
            "# --- END vz.net fabric ports ---",
        )
    }

    /// One marked block of the guest init script, relocated so it can run on
    /// this host against a fixture instead of a booted guest.
    fn relocated_init_block(
        root: &std::path::Path,
        busybox: &std::path::Path,
        begin: &str,
        end: &str,
    ) -> String {
        let source = initramfs_init_source();
        let (_, rest) = source
            .split_once(begin)
            .expect("the initramfs init script carries the block's begin marker");
        let (block, _) = rest
            .split_once(end)
            .expect("the block is closed by its end marker");
        block
            .replace("/bin/busybox", &busybox.display().to_string())
            .replace("/sys/class/net", &root.join("net").display().to_string())
            .replace("/proc/cmdline", &root.join("cmdline").display().to_string())
            .replace("/dev/console", &root.join("console").display().to_string())
    }

    /// The `vz.host.N` block, which resolves declared endpoint names.
    fn relocated_hosts_block(root: &std::path::Path, busybox: &std::path::Path) -> String {
        relocated_init_block(
            root,
            busybox,
            "# --- BEGIN vz.host endpoint names (extracted verbatim by vz-linux tests) ---",
            "# --- END vz.host endpoint names ---",
        )
    }

    /// A BusyBox stub that fails the run on any applet beyond `cat`.
    ///
    /// The guest BusyBox is not guaranteed to carry more than the Makefile's
    /// applet list, and a missing applet in a booted guest is a silent
    /// unresolvable name rather than a loud failure, so reaching for one here
    /// has to be an error at this level instead.
    fn cat_only_busybox(root: &std::path::Path) -> std::path::PathBuf {
        let busybox = root.join("busybox");
        fs::write(
            &busybox,
            "#!/bin/sh\napplet=\"$1\"; shift\ncase \"$applet\" in\n\
             cat) exec /bin/cat \"$@\" ;;\n\
             *) echo \"unexpected applet: $applet\" >&2; exit 127 ;;\nesac\n",
        )
        .expect("write busybox stub");
        fs::set_permissions(
            &busybox,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .expect("make busybox stub executable");
        busybox
    }

    /// The `vz.dns.N` block, which points the Machine at its Environment's own
    /// resolver.
    fn relocated_resolver_block(root: &std::path::Path, busybox: &std::path::Path) -> String {
        relocated_init_block(
            root,
            busybox,
            "# --- BEGIN vz.dns resolver (extracted verbatim by vz-linux tests) ---",
            "# --- END vz.dns resolver ---",
        )
    }

    /// Run the resolver block against `cmdline`, in both roots the Machine may
    /// end up running in, with an image resolv.conf already in place.
    ///
    /// The image ships a resolv.conf naming public resolvers, so "the block did
    /// nothing" and "the block wrote the Environment's resolver" are visibly
    /// different outcomes here rather than both being an empty file.
    fn run_resolver_block(cmdline: &str, overlay: &str) -> (tempfile::TempDir, String) {
        let fixture = tempfile::Builder::new()
            .prefix("vz-environment-resolver-")
            .tempdir()
            .expect("temp root");
        let root = fixture.path().to_path_buf();
        for prefix in ["", overlay] {
            fs::create_dir_all(root.join(prefix).join("etc")).expect("etc");
            fs::write(
                root.join(prefix).join("etc/resolv.conf"),
                "nameserver 1.1.1.1\nnameserver 8.8.8.8\n",
            )
            .expect("image resolv.conf");
        }
        fs::write(root.join("cmdline"), cmdline).expect("fake cmdline");
        let busybox = cat_only_busybox(&root);

        let mut script = relocated_resolver_block(&root, &busybox);
        script = script.replace(
            "write_fabric_resolver \"\"",
            &format!("write_fabric_resolver \"{}\"", root.display()),
        );
        script.push_str(&format!(
            "\nwrite_fabric_resolver \"{}\"\n",
            root.join(overlay).display()
        ));
        let script_path = root.join("resolver.sh");
        fs::write(&script_path, script).expect("write harness script");

        let output = std::process::Command::new("/bin/sh")
            .arg(&script_path)
            .output()
            .expect("run the guest resolver block");
        assert!(
            output.status.success(),
            "guest resolver block failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let console = fs::read_to_string(root.join("console")).unwrap_or_default();
        (fixture, console)
    }

    #[test]
    fn a_machine_on_a_public_like_network_resolves_through_its_environment_alone() {
        // Replaced, not appended to. The Environment's resolver answers the
        // Environment's names and nothing else; a public resolver left beside
        // it would be asked for an Environment name the moment the first query
        // came back unanswered, which is exactly the split the declaration
        // asked not to have.
        let (fixture, console) = run_resolver_block(
            "console=hvc0 vz.net.0=02:aa:bb:cc:dd:03,10.9.0.5/24,10.9.0.1 \
             vz.host.0=10.9.0.7,db.internal vz.dns.0=10.9.0.1\n",
            "merged",
        );
        let root = fixture.path();
        for prefix in ["", "merged"] {
            assert_eq!(
                fs::read_to_string(root.join(prefix).join("etc/resolv.conf")).expect("resolv.conf"),
                "nameserver 10.9.0.1\n",
                "prefix {prefix:?}, console: {console}"
            );
        }
    }

    #[test]
    fn a_machine_with_no_declared_resolver_keeps_the_one_its_image_shipped() {
        // Most Machines declare no public-like network at all. Truncating their
        // resolv.conf to an empty file would be a regression for every one of
        // them, so the absence of the argument is the absence of the write.
        let (fixture, console) = run_resolver_block(
            "console=hvc0 vz.net.0=02:aa:bb:cc:dd:03,10.9.0.5/24\n",
            "merged",
        );
        let root = fixture.path();
        for prefix in ["", "merged"] {
            assert_eq!(
                fs::read_to_string(root.join(prefix).join("etc/resolv.conf")).expect("resolv.conf"),
                "nameserver 1.1.1.1\nnameserver 8.8.8.8\n",
                "prefix {prefix:?}, console: {console}"
            );
        }
    }

    /// Run the endpoint-name block against `cmdline`, with the roots it should
    /// write into already carrying an `etc` directory.
    ///
    /// The block's own trailing invocation writes the initramfs root, so the
    /// harness adds only the second call the real script makes from inside
    /// `switch_root_into_overlay_rootfs`. That the initramfs write happens at
    /// all is therefore part of what this exercises rather than something the
    /// harness supplies.
    fn run_hosts_block(cmdline: &str, overlay: &str) -> (tempfile::TempDir, String) {
        let fixture = tempfile::Builder::new()
            .prefix("vz-endpoint-hosts-")
            .tempdir()
            .expect("temp root");
        let root = fixture.path().to_path_buf();
        fs::create_dir_all(root.join("etc")).expect("initramfs etc");
        fs::create_dir_all(root.join(overlay).join("etc")).expect("overlay etc");
        fs::write(root.join("cmdline"), cmdline).expect("fake cmdline");
        let busybox = cat_only_busybox(&root);

        let mut script = relocated_hosts_block(&root, &busybox);
        // `""` is the initramfs root prefix in the real script; here the
        // fixture root stands in for `/`.
        script = script.replace(
            "write_fabric_hosts \"\"",
            &format!("write_fabric_hosts \"{}\"", root.display()),
        );
        script.push_str(&format!(
            "\nwrite_fabric_hosts \"{}\"\n",
            root.join(overlay).display()
        ));
        let script_path = root.join("hosts.sh");
        fs::write(&script_path, script).expect("write harness script");

        let output = std::process::Command::new("/bin/sh")
            .arg(&script_path)
            .output()
            .expect("run the guest endpoint-name block");
        assert!(
            output.status.success(),
            "guest endpoint-name block failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let console = fs::read_to_string(root.join("console")).unwrap_or_default();
        (fixture, console)
    }

    #[test]
    fn the_guest_resolves_every_declared_endpoint_name_in_both_roots_it_may_run_in() {
        // Which root the Machine ends up running in is decided after this block
        // runs: the overlay root when the VirtioFS rootfs mounted, the
        // initramfs itself when it did not. A name that resolved in only one of
        // them would resolve or not depending on a mount the declaration says
        // nothing about, so both are written.
        let (fixture, console) = run_hosts_block(
            "console=hvc0 vz.mount.0=/workspace \
             vz.net.0=02:aa:bb:cc:dd:03,10.9.0.5/24 \
             vz.host.0=10.9.0.7,api vz.host.1=10.9.0.5,db.internal\n",
            "merged",
        );

        let root = fixture.path();
        let expected = "127.0.0.1 localhost\n::1 localhost\n10.9.0.7 api\n10.9.0.5 db.internal\n";
        assert_eq!(
            fs::read_to_string(root.join("etc/hosts")).expect("initramfs /etc/hosts"),
            expected,
            "console: {console}"
        );
        assert_eq!(
            fs::read_to_string(root.join("merged/etc/hosts")).expect("overlay /etc/hosts"),
            expected,
            "console: {console}"
        );
        // Localhost survives. The file is generated rather than appended to, so
        // if the standard entries were not emitted here nothing else would put
        // them back and the Machine would lose `localhost`.
        assert!(expected.contains("127.0.0.1 localhost"));
    }

    #[test]
    fn a_machine_that_declares_no_endpoint_keeps_the_hosts_file_its_image_shipped() {
        // Most Machines declare no endpoint. Truncating their `/etc/hosts` to a
        // generated file would be a regression for every one of them, and the
        // image's own entries are not this block's to discard.
        let (fixture, console) = run_hosts_block(
            "console=hvc0 vz.net.0=02:aa:bb:cc:dd:03,10.9.0.5/24\n",
            "merged",
        );
        let root = fixture.path();
        assert!(
            !root.join("etc/hosts").exists(),
            "no endpoint was declared, so nothing should have been written: {console}"
        );
        assert!(
            !root.join("merged/etc/hosts").exists(),
            "console: {console}"
        );
    }

    #[test]
    fn a_malformed_endpoint_argument_is_reported_and_does_not_cost_the_others() {
        // A value with no comma would otherwise write its own half as both the
        // address and the name, which resolves — wrongly — instead of failing.
        let (fixture, console) = run_hosts_block(
            "vz.host.0=10.9.0.7,api vz.host.1=nonsense vz.host.2=10.9.0.9,cache\n",
            "merged",
        );
        let root = fixture.path();
        assert!(
            console.contains("ignoring malformed vz.host.1=nonsense"),
            "the malformed argument must say so: {console}"
        );
        assert_eq!(
            fs::read_to_string(root.join("etc/hosts")).expect("initramfs /etc/hosts"),
            "127.0.0.1 localhost\n::1 localhost\n10.9.0.7 api\n10.9.0.9 cache\n",
            "console: {console}"
        );
    }

    #[test]
    fn the_overlay_root_is_written_by_the_init_script_and_not_only_by_the_harness() {
        // The second call happens outside the extracted markers, so the block
        // test above cannot prove the real script makes it. This does: without
        // this line the overlay root — the one the guest agent is chroot'd into,
        // and therefore the one every `vz exec` resolves against — would keep
        // whatever `/etc/hosts` the image shipped.
        let script = initramfs_init_source();
        let (_, after) = script
            .split_once("switch_root_into_overlay_rootfs() {")
            .expect("the overlay switch_root function exists");
        assert!(
            after.contains("write_fabric_hosts \"$ROOTFS\""),
            "switch_root_into_overlay_rootfs must write the Machine's endpoint names into the root it chroots into"
        );
    }

    #[test]
    fn the_guest_configures_a_fabric_port_by_matching_its_mac_and_leaves_eth0_alone() {
        // Interface enumeration order is not guaranteed, so the fixture below
        // deliberately inverts it: `vz.net.0` names the MAC that sysfs
        // enumerates last and `vz.net.1` the one before it. If the script ever
        // selected by index, by position or by name, the assertions on which
        // device each address landed on would swap.
        let root = tempfile::Builder::new()
            .prefix("vz-fabric-init-")
            .tempdir()
            .expect("temp root");
        let root = root.path();
        for (name, address) in [
            ("eth0", "5a:11:22:33:44:00"),
            ("eth1", "02:aa:bb:cc:dd:02"),
            ("eth2", "02:aa:bb:cc:dd:03"),
        ] {
            fs::create_dir_all(root.join("net").join(name)).expect("fake sysfs device");
            fs::write(root.join("net").join(name).join("address"), address)
                .expect("fake sysfs address");
        }
        fs::write(
            root.join("cmdline"),
            "console=hvc0 vz.mount.0=/workspace \
             vz.net.0=02:aa:bb:cc:dd:03,10.9.0.5/24 \
             vz.net.1=02:aa:bb:cc:dd:02,10.9.1.7/16,10.9.0.1\n",
        )
        .expect("fake cmdline");

        // Any applet beyond `cat` and `ip` fails the run: the guest BusyBox is
        // not guaranteed to carry more than the Makefile's applet list, and a
        // missing applet in a booted guest is a silent unconfigured NIC.
        let busybox = root.join("busybox");
        let log = root.join("ip.log");
        fs::write(
            &busybox,
            format!(
                "#!/bin/sh\napplet=\"$1\"; shift\ncase \"$applet\" in\n\
                 cat) exec /bin/cat \"$@\" ;;\n\
                 ip) echo \"ip $*\" >> {log} ;;\n\
                 *) echo \"unexpected applet: $applet\" >&2; exit 127 ;;\nesac\n",
                log = log.display()
            ),
        )
        .expect("write busybox stub");
        fs::set_permissions(
            &busybox,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .expect("make busybox stub executable");

        // The block's own trailing invocation is what runs it, so the harness
        // adds no call of its own: that the definitions are actually applied
        // at boot is part of what this pins.
        let script = root.join("fabric.sh");
        fs::write(&script, relocated_fabric_block(root, &busybox)).expect("write harness script");

        let output = std::process::Command::new("/bin/sh")
            .arg(&script)
            .output()
            .expect("run the guest fabric block");
        assert!(
            output.status.success(),
            "guest fabric block failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let console = fs::read_to_string(root.join("console")).unwrap_or_default();

        let applied = fs::read_to_string(&log).unwrap_or_default();
        let applied: Vec<&str> = applied.lines().collect();
        assert_eq!(
            applied,
            vec![
                // Matched on MAC, so the first argument lands on eth2.
                "ip address add 10.9.0.5/24 dev eth2",
                "ip link set dev eth2 up",
                "ip address add 10.9.1.7/16 dev eth1",
                "ip link set dev eth1 up",
                // Only the port that named a gateway gets a route.
                "ip route add default via 10.9.0.1 dev eth1",
            ],
            "console: {console}"
        );
        // eth0 carries Apple's NAT address and is configured by udhcpc. A
        // fabric port must never touch it.
        assert!(!applied.iter().any(|line| line.contains("eth0")));
    }

    #[test]
    fn a_fabric_port_whose_mac_no_interface_has_is_reported_and_does_not_stop_the_others() {
        // A NIC that failed to attach must not cost the Machine every other
        // port, and must not pass silently either: an unconfigured port looks
        // exactly like an application that cannot reach its peer.
        let root = tempfile::Builder::new()
            .prefix("vz-fabric-init-missing-")
            .tempdir()
            .expect("temp root");
        let root = root.path();
        fs::create_dir_all(root.join("net").join("eth1")).expect("fake sysfs device");
        fs::write(
            root.join("net").join("eth1").join("address"),
            "02:aa:bb:cc:dd:02",
        )
        .expect("fake sysfs address");
        fs::write(
            root.join("cmdline"),
            "vz.net.0=02:aa:bb:cc:dd:99,10.9.0.5/24 \
             vz.net.1=02:aa:bb:cc:dd:02,10.9.1.7/16\n",
        )
        .expect("fake cmdline");

        let busybox = root.join("busybox");
        let log = root.join("ip.log");
        fs::write(
            &busybox,
            format!(
                "#!/bin/sh\napplet=\"$1\"; shift\ncase \"$applet\" in\n\
                 cat) exec /bin/cat \"$@\" ;;\n\
                 ip) echo \"ip $*\" >> {log} ;;\n\
                 *) exit 127 ;;\nesac\n",
                log = log.display()
            ),
        )
        .expect("write busybox stub");
        fs::set_permissions(
            &busybox,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .expect("make busybox stub executable");

        let script = root.join("fabric.sh");
        fs::write(&script, relocated_fabric_block(root, &busybox)).expect("write harness script");

        let output = std::process::Command::new("/bin/sh")
            .arg(&script)
            .output()
            .expect("run the guest fabric block");
        // An unmatched port must not take the boot down with it: the block is
        // reached before the guest agent starts, and a non-zero exit here
        // would cost the Machine everything, not just one NIC.
        assert!(
            output.status.success(),
            "an unmatched port must not fail the boot: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let console = fs::read_to_string(root.join("console")).unwrap_or_default();
        assert!(
            console.contains("no interface has address 02:aa:bb:cc:dd:99"),
            "the unmatched port must say so: {console}"
        );
        assert!(
            console.contains("could not apply vz.net.0="),
            "the unmatched port must name the argument it could not apply: {console}"
        );
        assert_eq!(
            fs::read_to_string(&log)
                .unwrap_or_default()
                .lines()
                .collect::<Vec<_>>(),
            vec![
                "ip address add 10.9.1.7/16 dev eth1",
                "ip link set dev eth1 up",
            ],
            "the port that did match must still be configured"
        );
    }
}
