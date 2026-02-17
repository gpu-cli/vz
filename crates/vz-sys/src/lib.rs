//! Raw FFI bindings to Apple's Virtualization.framework.
//!
//! This crate provides unsafe Objective-C bindings via `objc2`.
//! End users should use the safe `vz` crate instead.

#![cfg(target_os = "macos")]
#![allow(unsafe_code)]

// Phase 1: Bind these Virtualization.framework classes
//
// VM lifecycle:
//   - VZVirtualMachine
//   - VZVirtualMachineConfiguration
//
// macOS boot:
//   - VZMacOSBootLoader
//   - VZMacPlatformConfiguration
//   - VZMacOSRestoreImage
//   - VZMacOSInstaller
//   - VZMacHardwareModel
//   - VZMacMachineIdentifier
//   - VZMacAuxiliaryStorage
//
// Storage:
//   - VZDiskImageStorageDeviceAttachment
//   - VZVirtioBlockDeviceConfiguration
//
// Shared directories (VirtioFS):
//   - VZVirtioFileSystemDeviceConfiguration
//   - VZSharedDirectory
//   - VZSingleDirectoryShare
//   - VZMultipleDirectoryShare
//
// Networking:
//   - VZVirtioNetworkDeviceConfiguration
//   - VZNATNetworkDeviceAttachment
//
// vsock:
//   - VZVirtioSocketDeviceConfiguration
//   - VZVirtioSocketDevice
//   - VZVirtioSocketConnection
//   - VZVirtioSocketListener
//
// Display (optional):
//   - VZMacGraphicsDeviceConfiguration
//   - VZMacGraphicsDisplayConfiguration
//
// Save/restore (macOS 14+):
//   - VZVirtualMachine.saveMachineStateTo(url:completionHandler:)
//   - VZVirtualMachine.restoreMachineStateFrom(url:completionHandler:)
