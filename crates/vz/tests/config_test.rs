//! Layer 1: VmConfigBuilder validation tests.
//!
//! Tests config validation logic without needing macOS or a VM.

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use vz::{BootLoader, DiskConfig, MacPlatformConfig, SharedDirConfig, VmConfigBuilder, VzError};

fn rootfs_disk(path: &str) -> DiskConfig {
    DiskConfig {
        id: "rootfs".into(),
        path: PathBuf::from(path),
        read_only: false,
    }
}

// ---------------------------------------------------------------------------
// Builder defaults
// ---------------------------------------------------------------------------

#[test]
fn builder_default_matches_new() {
    let from_new = format!("{:?}", VmConfigBuilder::new());
    let from_default = format!("{:?}", VmConfigBuilder::default());
    assert_eq!(from_new, from_default);
}

#[test]
fn builder_defaults_reasonable() {
    let builder = VmConfigBuilder::new();
    let debug = format!("{:?}", builder);
    // Default: 2 CPUs, 4 GB memory, no boot loader, headless
    assert!(debug.contains("cpus: 2"));
    assert!(debug.contains("headless: true"));
}

// ---------------------------------------------------------------------------
// Builder method chaining
// ---------------------------------------------------------------------------

#[test]
fn builder_chaining_fluent() {
    // All builder methods should return Self for chaining
    let _builder = VmConfigBuilder::new()
        .cpus(4)
        .memory_mb(8192)
        .boot_macos()
        .disk(rootfs_disk("/tmp/test.img"))
        .shared_dir(SharedDirConfig {
            tag: "project".into(),
            source: "/tmp/project".into(),
            read_only: false,
        })
        .shared_dirs(vec![SharedDirConfig {
            tag: "tools".into(),
            source: "/tmp/tools".into(),
            read_only: true,
        }])
        .enable_vsock()
        .with_display();
}

// ---------------------------------------------------------------------------
// Successful builds
// ---------------------------------------------------------------------------

#[test]
fn build_macos_minimal() {
    let config = VmConfigBuilder::new()
        .boot_macos()
        .mac_platform(MacPlatformConfig {
            hardware_model_path: PathBuf::from("/tmp/hw.model"),
            machine_identifier_path: PathBuf::from("/tmp/machine.id"),
            auxiliary_storage_path: PathBuf::from("/tmp/aux.storage"),
        })
        .disk(rootfs_disk("/tmp/test.img"))
        .build();

    assert!(config.is_ok());
}

#[test]
fn build_with_shared_dirs() {
    let config = VmConfigBuilder::new()
        .boot_macos()
        .mac_platform(MacPlatformConfig {
            hardware_model_path: PathBuf::from("/tmp/hw.model"),
            machine_identifier_path: PathBuf::from("/tmp/machine.id"),
            auxiliary_storage_path: PathBuf::from("/tmp/aux.storage"),
        })
        .disk(rootfs_disk("/tmp/test.img"))
        .shared_dir(SharedDirConfig {
            tag: "project".into(),
            source: "/tmp/project".into(),
            read_only: false,
        })
        .shared_dir(SharedDirConfig {
            tag: "tools".into(),
            source: "/tmp/tools".into(),
            read_only: true,
        })
        .build();

    assert!(config.is_ok());
}

// ---------------------------------------------------------------------------
// Validation failures
// ---------------------------------------------------------------------------

#[test]
fn build_fails_without_boot_loader() {
    let result = VmConfigBuilder::new()
        .disk(rootfs_disk("/tmp/test.img"))
        .build();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, VzError::InvalidConfig(ref msg) if msg.contains("boot loader")));
}

#[test]
fn build_fails_without_disk() {
    let result = VmConfigBuilder::new()
        .boot_macos()
        .mac_platform(MacPlatformConfig {
            hardware_model_path: PathBuf::from("/tmp/hw.model"),
            machine_identifier_path: PathBuf::from("/tmp/machine.id"),
            auxiliary_storage_path: PathBuf::from("/tmp/aux.storage"),
        })
        .build();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, VzError::InvalidConfig(ref msg) if msg.contains("disk")));
}

#[test]
fn build_macos_fails_without_mac_platform() {
    let result = VmConfigBuilder::new()
        .boot_macos()
        .disk(rootfs_disk("/tmp/test.img"))
        .build();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, VzError::InvalidConfig(ref msg) if msg.contains("mac_platform")));
}

// ---------------------------------------------------------------------------
// CPU and memory
// ---------------------------------------------------------------------------

#[test]
fn builder_custom_cpus() {
    let builder = VmConfigBuilder::new().cpus(8);
    let debug = format!("{:?}", builder);
    assert!(debug.contains("cpus: 8"));
}

#[test]
fn builder_memory_gb_conversion() {
    let builder = VmConfigBuilder::new().memory_gb(16);
    let debug = format!("{:?}", builder);
    let expected_bytes = 16u64 * 1024 * 1024 * 1024;
    assert!(debug.contains(&format!("memory_bytes: {expected_bytes}")));
}

#[test]
fn builder_memory_mb_conversion() {
    let builder = VmConfigBuilder::new().memory_mb(512);
    let debug = format!("{:?}", builder);
    let expected_bytes = 512u64 * 1024 * 1024;
    assert!(debug.contains(&format!("memory_bytes: {expected_bytes}")));
}

#[test]
fn builder_memory_bytes_direct() {
    let builder = VmConfigBuilder::new().memory_bytes(123_456_789);
    let debug = format!("{:?}", builder);
    assert!(debug.contains("memory_bytes: 123456789"));
}

#[test]
fn build_linux_without_disk_succeeds() {
    let config = VmConfigBuilder::new()
        .boot_linux("/boot/vmlinuz", None::<PathBuf>, "console=ttyS0")
        .build();

    assert!(config.is_ok());
}

#[test]
fn boot_macos_helper_sets_boot_loader() {
    let builder = VmConfigBuilder::new().boot_macos();
    let debug = format!("{:?}", builder);
    assert!(debug.contains("boot_loader: Some(MacOS)"));
}

// ---------------------------------------------------------------------------
// Boot loader variants
// ---------------------------------------------------------------------------

#[test]
fn boot_loader_macos_debug() {
    let loader = BootLoader::MacOS;
    let debug = format!("{:?}", loader);
    assert_eq!(debug, "MacOS");
}

#[test]
fn boot_loader_linux_debug() {
    let loader = BootLoader::Linux {
        kernel: PathBuf::from("/boot/vmlinuz"),
        initrd: Some(PathBuf::from("/boot/initrd")),
        cmdline: "console=ttyS0".into(),
    };
    let debug = format!("{:?}", loader);
    assert!(debug.contains("Linux"));
    assert!(debug.contains("vmlinuz"));
    assert!(debug.contains("initrd"));
}

#[test]
fn boot_loader_linux_clone() {
    let loader = BootLoader::Linux {
        kernel: PathBuf::from("/boot/vmlinuz"),
        initrd: None,
        cmdline: "root=/dev/vda1".into(),
    };
    let cloned = loader.clone();
    assert_eq!(format!("{:?}", loader), format!("{:?}", cloned));
}

// ---------------------------------------------------------------------------
// SharedDirConfig
// ---------------------------------------------------------------------------

#[test]
fn shared_dir_config_debug_and_clone() {
    let shared = SharedDirConfig {
        tag: "project".into(),
        source: "/Users/dev/code".into(),
        read_only: true,
    };
    let cloned = shared.clone();
    let debug = format!("{:?}", shared);
    assert!(debug.contains("project"));
    assert!(debug.contains("read_only: true"));
    assert_eq!(format!("{:?}", cloned), debug);
}

// ---------------------------------------------------------------------------
// MacPlatformConfig
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Per-VM MAC address
// ---------------------------------------------------------------------------

fn build_minimal_linux_config() -> vz::config::VmConfig {
    VmConfigBuilder::new()
        .boot_linux("/boot/vmlinuz", None::<PathBuf>, "console=ttyS0")
        .build()
        .unwrap()
}

fn parse_mac_octets(mac: &str) -> [u8; 6] {
    let parts: Vec<&str> = mac.split(':').collect();
    assert_eq!(parts.len(), 6, "mac {mac:?} should have 6 octets");
    let mut bytes = [0u8; 6];
    for (i, part) in parts.iter().enumerate() {
        bytes[i] = u8::from_str_radix(part, 16).unwrap();
    }
    bytes
}

#[test]
fn builder_generates_locally_administered_mac_by_default() {
    let cfg = build_minimal_linux_config();
    let mac = cfg.mac_address();
    assert_eq!(mac.split(':').count(), 6, "mac should be 6 octets: {mac}");
    let bytes = parse_mac_octets(mac);
    // Locally administered: second-LSB of first octet set.
    assert_eq!(
        bytes[0] & 0x02,
        0x02,
        "mac {mac} should be locally administered"
    );
    // Unicast: LSB of first octet clear.
    assert_eq!(bytes[0] & 0x01, 0x00, "mac {mac} should be unicast");
}

#[test]
fn two_vm_configs_get_different_macs() {
    // Build twice from the same builder shape — they must NOT share a MAC,
    // otherwise running both VMs in one process collides on the host bridge.
    let a = build_minimal_linux_config();
    let b = build_minimal_linux_config();
    assert_ne!(
        a.mac_address(),
        b.mac_address(),
        "fresh VmConfigs must get distinct MACs (got {} for both)",
        a.mac_address()
    );
}

#[test]
fn explicit_mac_is_preserved() {
    let cfg = VmConfigBuilder::new()
        .boot_linux("/boot/vmlinuz", None::<PathBuf>, "console=ttyS0")
        .mac("aa:bb:cc:dd:ee:ff")
        .build()
        .unwrap();
    assert_eq!(cfg.mac_address(), "aa:bb:cc:dd:ee:ff");
}

#[test]
fn cloned_vmconfig_shares_mac() {
    // VmConfig is Clone — save/restore takes the same VmConfig and reuses it,
    // so the clone path must produce a config whose MAC matches the original.
    let cfg = build_minimal_linux_config();
    let original = cfg.mac_address().to_string();
    let cloned = cfg.clone();
    assert_eq!(cloned.mac_address(), original);
}

#[test]
fn mac_platform_config_debug_and_clone() {
    let config = MacPlatformConfig {
        hardware_model_path: PathBuf::from("/vm/hw.model"),
        machine_identifier_path: PathBuf::from("/vm/machine.id"),
        auxiliary_storage_path: PathBuf::from("/vm/aux.storage"),
    };
    let cloned = config.clone();
    let debug = format!("{:?}", config);
    assert!(debug.contains("hw.model"));
    assert!(debug.contains("machine.id"));
    assert!(debug.contains("aux.storage"));
    assert_eq!(format!("{:?}", cloned), debug);
}

// ---------------------------------------------------------------------------
// Multi-disk
// ---------------------------------------------------------------------------

#[test]
fn build_with_multiple_disks_preserves_order() {
    let cfg = VmConfigBuilder::new()
        .boot_linux("/boot/vmlinuz", None::<PathBuf>, "console=ttyS0")
        .disk(DiskConfig {
            id: "rootfs".into(),
            path: PathBuf::from("/vm/root.img"),
            read_only: false,
        })
        .disk(DiskConfig {
            id: "data".into(),
            path: PathBuf::from("/vm/data.img"),
            read_only: false,
        })
        .disk(DiskConfig {
            id: "metadata".into(),
            path: PathBuf::from("/vm/metadata.img"),
            read_only: true,
        })
        .build()
        .unwrap();

    assert_eq!(cfg.disks().len(), 3, "three disks should round-trip");
    assert_eq!(cfg.disks()[0].id, "rootfs");
    assert_eq!(cfg.disks()[1].id, "data");
    assert_eq!(cfg.disks()[2].id, "metadata");
    assert!(!cfg.disks()[0].read_only);
    assert!(!cfg.disks()[1].read_only);
    assert!(
        cfg.disks()[2].read_only,
        "metadata disk should be read-only"
    );
}

#[test]
fn linux_boot_with_no_disks_succeeds() {
    // Linux can boot from initramfs alone — no disk required.
    let cfg = VmConfigBuilder::new()
        .boot_linux("/boot/vmlinuz", None::<PathBuf>, "console=ttyS0")
        .build()
        .unwrap();
    assert!(cfg.disks().is_empty());
}

// ---------------------------------------------------------------------------
// Memory balloon
// ---------------------------------------------------------------------------

#[test]
fn memory_balloon_enabled_by_default() {
    // Builder defaults must enable the balloon — without it the host has no
    // path to reclaim guest memory at runtime, which defeats the multi-VM
    // story this whole change set was about.
    let cfg = VmConfigBuilder::new()
        .boot_linux("/boot/vmlinuz", None::<PathBuf>, "console=ttyS0")
        .build()
        .unwrap();
    assert!(
        cfg.memory_balloon_enabled(),
        "memory balloon must be on by default"
    );
}

#[test]
fn memory_balloon_can_be_opted_out() {
    let cfg = VmConfigBuilder::new()
        .boot_linux("/boot/vmlinuz", None::<PathBuf>, "console=ttyS0")
        .memory_balloon(false)
        .build()
        .unwrap();
    assert!(!cfg.memory_balloon_enabled());
}

#[test]
fn macos_boot_without_disks_fails() {
    let result = VmConfigBuilder::new()
        .boot_macos()
        .mac_platform(MacPlatformConfig {
            hardware_model_path: PathBuf::from("/tmp/hw.model"),
            machine_identifier_path: PathBuf::from("/tmp/machine.id"),
            auxiliary_storage_path: PathBuf::from("/tmp/aux.storage"),
        })
        .build();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, VzError::InvalidConfig(ref msg) if msg.contains("disk")));
}
