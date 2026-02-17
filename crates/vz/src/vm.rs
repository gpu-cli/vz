//! Virtual machine lifecycle management.

use std::path::Path;

use crate::config::VmConfig;
use crate::error::VzError;
use crate::vsock::{VsockListener, VsockStream};

/// The state of a virtual machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmState {
    Stopped,
    Starting,
    Running,
    Pausing,
    Paused,
    Resuming,
    Stopping,
    Saving,
    Restoring,
    Error,
}

/// A macOS or Linux virtual machine.
///
/// Wraps `VZVirtualMachine` from Apple's Virtualization.framework.
pub struct Vm {
    _config: VmConfig,
    // Will hold: Retained<VZVirtualMachine> from vz-sys
}

impl Vm {
    /// Create a new VM from a validated configuration.
    ///
    /// The VM is created but not started — call [`start`](Self::start) to boot it.
    pub fn create(config: VmConfig) -> Result<Self, VzError> {
        // TODO: Phase 1 — bridge to VZVirtualMachineConfiguration via vz-sys
        Ok(Self { _config: config })
    }

    /// Start (cold boot) the VM.
    pub async fn start(&self) -> Result<(), VzError> {
        // TODO: Phase 1 — call VZVirtualMachine.start(completionHandler:)
        todo!("Phase 1: implement VM start")
    }

    /// Pause the VM (freeze execution, keep state in memory).
    pub async fn pause(&self) -> Result<(), VzError> {
        // TODO: Phase 1
        todo!("Phase 1: implement VM pause")
    }

    /// Resume a paused VM.
    pub async fn resume(&self) -> Result<(), VzError> {
        // TODO: Phase 1
        todo!("Phase 1: implement VM resume")
    }

    /// Stop the VM (equivalent to pulling the power cord).
    pub async fn stop(&self) -> Result<(), VzError> {
        // TODO: Phase 1 — call VZVirtualMachine.stop(completionHandler:)
        todo!("Phase 1: implement VM stop")
    }

    /// Request a graceful guest shutdown.
    ///
    /// Sends a power button event. The guest OS decides how to handle it.
    pub async fn request_stop(&self) -> Result<(), VzError> {
        // TODO: Phase 1 — call VZVirtualMachine.requestStop
        todo!("Phase 1: implement request_stop")
    }

    /// Save full VM state to disk. VM must be paused first.
    ///
    /// Requires macOS 14 (Sonoma) or later. The saved state file is
    /// hardware-encrypted and tied to this Mac + user account.
    pub async fn save_state(&self, path: &Path) -> Result<(), VzError> {
        // TODO: Phase 1 — call VZVirtualMachine.saveMachineStateTo(url:completionHandler:)
        let _ = path;
        todo!("Phase 1: implement save_state")
    }

    /// Restore VM from a previously saved state file.
    ///
    /// Must use the same VmConfig that was used when the state was saved.
    /// This is much faster than a cold boot (~5-10s vs 30-60s).
    pub async fn restore_state(&self, path: &Path) -> Result<(), VzError> {
        // TODO: Phase 1 — call VZVirtualMachine.restoreMachineStateFrom(url:completionHandler:)
        let _ = path;
        todo!("Phase 1: implement restore_state")
    }

    /// Connect to the guest over vsock on the given port.
    ///
    /// Returns a bidirectional async byte stream.
    /// Requires vsock to be enabled in the VM configuration.
    pub async fn vsock_connect(&self, port: u32) -> Result<VsockStream, VzError> {
        // TODO: Phase 1 — use VZVirtioSocketDevice.connect(toPort:completionHandler:)
        let _ = port;
        todo!("Phase 1: implement vsock_connect")
    }

    /// Listen for incoming vsock connections from the guest.
    pub async fn vsock_listen(&self, port: u32) -> Result<VsockListener, VzError> {
        let _ = port;
        todo!("Phase 1: implement vsock_listen")
    }

    /// Get the current VM state.
    pub fn state(&self) -> VmState {
        // TODO: Phase 1 — read VZVirtualMachine.state
        VmState::Stopped
    }
}

/// Source for macOS IPSW restore images.
pub enum IpswSource {
    /// Download the latest supported IPSW from Apple.
    Latest,
    /// Use a local IPSW file.
    Path(std::path::PathBuf),
}

/// Install macOS from an IPSW into a disk image.
///
/// This is a one-time operation to create a golden image.
/// After installation, the VM needs to go through the macOS setup assistant.
pub async fn install_macos(
    _ipsw: IpswSource,
    _disk_path: &Path,
    _config: &VmConfig,
) -> Result<(), VzError> {
    // TODO: Phase 1 — VZMacOSRestoreImage + VZMacOSInstaller
    todo!("Phase 1: implement install_macos")
}
