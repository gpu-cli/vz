//! Layer 1: VzError type tests.
//!
//! Tests error Display formatting and variant matching.

#![allow(clippy::unwrap_used)]

use vz::{VmState, VzError};

// ---------------------------------------------------------------------------
// Display formatting
// ---------------------------------------------------------------------------

#[test]
fn invalid_config_display() {
    let err = VzError::InvalidConfig("missing boot loader".into());
    assert_eq!(
        err.to_string(),
        "invalid VM configuration: missing boot loader"
    );
}

#[test]
fn invalid_state_display() {
    let err = VzError::InvalidState {
        current: VmState::Stopped,
        expected: VmState::Running,
    };
    let msg = err.to_string();
    assert!(msg.contains("Stopped"));
    assert!(msg.contains("Running"));
}

#[test]
fn start_failed_display() {
    let err = VzError::StartFailed("permission denied".into());
    assert_eq!(err.to_string(), "failed to start VM: permission denied");
}

#[test]
fn stop_failed_display() {
    let err = VzError::StopFailed("already stopped".into());
    assert_eq!(err.to_string(), "failed to stop VM: already stopped");
}

#[test]
fn save_failed_display() {
    let err = VzError::SaveFailed("disk full".into());
    assert_eq!(err.to_string(), "failed to save VM state: disk full");
}

#[test]
fn restore_failed_display() {
    let err = VzError::RestoreFailed("corrupted state file".into());
    assert_eq!(
        err.to_string(),
        "failed to restore VM state: corrupted state file"
    );
}

#[test]
fn vsock_failed_display() {
    let err = VzError::VsockFailed {
        port: 7424,
        reason: "connection refused".into(),
    };
    let msg = err.to_string();
    assert!(msg.contains("7424"));
    assert!(msg.contains("connection refused"));
}

#[test]
fn install_failed_display() {
    let err = VzError::InstallFailed("IPSW not found".into());
    assert_eq!(err.to_string(), "macOS install failed: IPSW not found");
}

#[test]
fn disk_error_display() {
    let err = VzError::DiskError("no space left on device".into());
    assert_eq!(err.to_string(), "disk image error: no space left on device");
}

#[test]
fn framework_error_display() {
    let err = VzError::FrameworkError("VZErrorDomain:1".into());
    assert_eq!(
        err.to_string(),
        "Virtualization.framework error: VZErrorDomain:1"
    );
}

// ---------------------------------------------------------------------------
// Debug formatting
// ---------------------------------------------------------------------------

#[test]
fn all_variants_debug() {
    // Ensure all variants implement Debug without panicking
    let variants: Vec<VzError> = vec![
        VzError::InvalidConfig("test".into()),
        VzError::InvalidState {
            current: VmState::Stopped,
            expected: VmState::Running,
        },
        VzError::StartFailed("test".into()),
        VzError::StopFailed("test".into()),
        VzError::SaveFailed("test".into()),
        VzError::RestoreFailed("test".into()),
        VzError::VsockFailed {
            port: 0,
            reason: "test".into(),
        },
        VzError::InstallFailed("test".into()),
        VzError::DiskError("test".into()),
        VzError::FrameworkError("test".into()),
    ];
    for err in &variants {
        let debug = format!("{:?}", err);
        assert!(!debug.is_empty());
    }
}

// ---------------------------------------------------------------------------
// Error trait
// ---------------------------------------------------------------------------

#[test]
fn vz_error_is_std_error() {
    let err: Box<dyn std::error::Error> = Box::new(VzError::InvalidConfig("test".into()));
    assert!(!err.to_string().is_empty());
}
