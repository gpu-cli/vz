//! Layer 2: Integration tests for ObjC bridging.
//!
//! These tests exercise the macOS-specific ObjC interop that is already
//! partially tested in bridge.rs inline tests. Integration tests here
//! focus on end-to-end bridging behavior.

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use vz::{BootLoader, MacPlatformConfig, VmConfigBuilder, VzError};

// ---------------------------------------------------------------------------
// VmConfig → ObjC validation (requires macOS for Vz.framework)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_vm_fails_with_invalid_disk_path() {
    // A valid-looking config but with a non-existent disk image.
    // Vm::create should fail during ObjC config validation because
    // VZDiskImageStorageDeviceAttachment requires the file to exist.
    let config = VmConfigBuilder::new()
        .boot_loader(BootLoader::MacOS)
        .mac_platform(MacPlatformConfig {
            hardware_model_path: PathBuf::from("/nonexistent/hw.model"),
            machine_identifier_path: PathBuf::from("/nonexistent/machine.id"),
            auxiliary_storage_path: PathBuf::from("/nonexistent/aux.storage"),
        })
        .disk("/nonexistent/disk.img")
        .build();

    assert!(config.is_ok(), "builder validation should pass");

    let result = vz::Vm::create(config.unwrap()).await;
    assert!(result.is_err(), "VM creation should fail with bad paths");
}

#[tokio::test]
async fn create_vm_fails_with_invalid_hardware_model() {
    // Create a temporary disk image to pass the disk validation,
    // but use invalid platform files.
    let tmp = tempfile::tempdir().unwrap();
    let disk_path = tmp.path().join("test.img");
    let hw_path = tmp.path().join("hw.model");
    let id_path = tmp.path().join("machine.id");
    let aux_path = tmp.path().join("aux.storage");

    // Create a real sparse disk image
    let f = std::fs::File::create(&disk_path).unwrap();
    f.set_len(1024 * 1024 * 1024).unwrap(); // 1 GB sparse

    // Write garbage data as hardware model (should fail validation)
    std::fs::write(&hw_path, b"invalid-hardware-model-data").unwrap();
    std::fs::write(&id_path, b"invalid-machine-id-data").unwrap();
    std::fs::write(&aux_path, b"invalid-aux-data").unwrap();

    let config = VmConfigBuilder::new()
        .boot_loader(BootLoader::MacOS)
        .mac_platform(MacPlatformConfig {
            hardware_model_path: hw_path,
            machine_identifier_path: id_path,
            auxiliary_storage_path: aux_path,
        })
        .disk(&disk_path)
        .build()
        .unwrap();

    let result = vz::Vm::create(config).await;
    assert!(
        result.is_err(),
        "VM creation should fail with invalid platform data"
    );

    // Error should be InvalidConfig (from hardware model parsing)
    let err = result.unwrap_err();
    assert!(
        matches!(err, VzError::InvalidConfig(_)),
        "expected InvalidConfig, got: {err}"
    );
}
