//! VirtioFS shared directory management.
//!
//! VirtioFS allows sharing host directories with the guest VM.
//! Shares are configured at VM creation time and cannot be
//! added or removed while the VM is running.
//!
//! Inside the guest, mount a share with:
//! ```text
//! mount -t virtiofs <tag> /mnt/project
//! ```

// VirtioFS configuration is handled via SharedDirConfig in config.rs.
// This module will contain any runtime VirtioFS utilities if needed.

// Future: helpers for guest-side mount automation via the guest agent.
