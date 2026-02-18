//! Layer 1: Install module pure Rust tests.
//!
//! Tests install types (IpswSource, InstallResult) and disk image creation.
//! Does not require macOS-specific APIs (no actual IPSW or VM).

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use vz::{InstallResult, IpswSource};

// ---------------------------------------------------------------------------
// IpswSource
// ---------------------------------------------------------------------------

#[test]
fn ipsw_source_latest_variant() {
    let source = IpswSource::Latest;
    assert!(matches!(source, IpswSource::Latest));
}

#[test]
fn ipsw_source_path_variant() {
    let source = IpswSource::Path(PathBuf::from("/tmp/restore.ipsw"));
    match source {
        IpswSource::Path(ref p) => {
            assert_eq!(p, &PathBuf::from("/tmp/restore.ipsw"));
        }
        IpswSource::Latest => panic!("expected Path variant"),
    }
}

// ---------------------------------------------------------------------------
// InstallResult
// ---------------------------------------------------------------------------

#[test]
fn install_result_debug() {
    let result = InstallResult {
        disk_path: PathBuf::from("/vm/base.img"),
        hardware_model_path: PathBuf::from("/vm/base.hwmodel"),
        machine_identifier_path: PathBuf::from("/vm/base.machineid"),
        auxiliary_storage_path: PathBuf::from("/vm/base.aux"),
    };
    let debug = format!("{:?}", result);
    assert!(debug.contains("base.img"));
    assert!(debug.contains("base.hwmodel"));
    assert!(debug.contains("base.machineid"));
    assert!(debug.contains("base.aux"));
}

#[test]
fn install_result_clone() {
    let result = InstallResult {
        disk_path: PathBuf::from("/vm/base.img"),
        hardware_model_path: PathBuf::from("/vm/base.hwmodel"),
        machine_identifier_path: PathBuf::from("/vm/base.machineid"),
        auxiliary_storage_path: PathBuf::from("/vm/base.aux"),
    };
    let cloned = result.clone();
    assert_eq!(result.disk_path, cloned.disk_path);
    assert_eq!(result.hardware_model_path, cloned.hardware_model_path);
    assert_eq!(
        result.machine_identifier_path,
        cloned.machine_identifier_path
    );
    assert_eq!(result.auxiliary_storage_path, cloned.auxiliary_storage_path);
}

// ---------------------------------------------------------------------------
// install_macos with invalid input
// ---------------------------------------------------------------------------

#[tokio::test]
async fn install_macos_rejects_nonexistent_ipsw() {
    let result = vz::install_macos(
        IpswSource::Path(PathBuf::from("/nonexistent/restore.ipsw")),
        &PathBuf::from("/tmp/vz-test-install.img"),
        64 * 1024 * 1024 * 1024,
    )
    .await;

    // Should fail because the IPSW file doesn't exist.
    // The exact error depends on whether the load callback fires before
    // the path check, but it should definitely be an error.
    assert!(result.is_err());
}
