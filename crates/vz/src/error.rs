//! Error types for the vz crate.

/// Errors that can occur when working with virtual machines.
#[derive(Debug, thiserror::Error)]
pub enum VzError {
    /// VM configuration is invalid
    #[error("invalid VM configuration: {0}")]
    InvalidConfig(String),

    /// VM is in wrong state for the requested operation
    #[error("VM is in state {current:?}, expected {expected:?}")]
    InvalidState {
        current: super::VmState,
        expected: super::VmState,
    },

    /// Failed to start the VM
    #[error("failed to start VM: {0}")]
    StartFailed(String),

    /// Failed to stop the VM
    #[error("failed to stop VM: {0}")]
    StopFailed(String),

    /// Failed to save VM state
    #[error("failed to save VM state: {0}")]
    SaveFailed(String),

    /// Failed to restore VM state
    #[error("failed to restore VM state: {0}")]
    RestoreFailed(String),

    /// vsock connection failed
    #[error("vsock connection to port {port} failed: {reason}")]
    VsockFailed { port: u32, reason: String },

    /// IPSW download or install failed
    #[error("macOS install failed: {0}")]
    InstallFailed(String),

    /// Disk image error
    #[error("disk image error: {0}")]
    DiskError(String),

    /// Objective-C / framework error
    #[error("Virtualization.framework error: {0}")]
    FrameworkError(String),
}
