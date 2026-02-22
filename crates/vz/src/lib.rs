//! Safe Rust API for Apple's Virtualization.framework.
//!
//! Create and manage macOS virtual machines with VirtioFS file sharing
//! and vsock host↔guest communication.
//!
//! # Example
//!
//! ```rust,no_run
//! # #[cfg(target_os = "macos")]
//! # {
//! use vz::{BootLoader, SharedDirConfig, VmConfigBuilder};
//!
//! # async fn example() -> anyhow::Result<()> {
//! let config = VmConfigBuilder::new()
//!     .cpus(4)
//!     .memory_gb(8)
//!     .boot_loader(BootLoader::MacOS)
//!     .disk("./base-macos.img")
//!     .shared_dir(SharedDirConfig {
//!         tag: "project".into(),
//!         source: "./my-project".into(),
//!         read_only: false,
//!     })
//!     .enable_vsock()
//!     .build()?;
//!
//! let vm = vz::Vm::create(config).await?;
//! vm.start().await?;
//!
//! // Connect to guest agent over vsock
//! let stream = vm.vsock_connect(5000).await?;
//!
//! // When done
//! vm.stop().await?;
//! # Ok(())
//! # }
//! # }
//! ```

// unsafe is required for objc2 FFI calls — kept minimal and contained
#![allow(unsafe_code)]

pub mod protocol;

#[cfg(target_os = "macos")]
pub(crate) mod bridge;
#[cfg(target_os = "macos")]
pub mod config;
#[cfg(target_os = "macos")]
pub mod error;
#[cfg(target_os = "macos")]
pub mod install;
#[cfg(target_os = "macos")]
pub mod virtio_fs;
#[cfg(target_os = "macos")]
pub mod vm;
#[cfg(target_os = "macos")]
pub mod vsock;

#[cfg(target_os = "macos")]
pub use config::{BootLoader, MacPlatformConfig, NetworkConfig, SharedDirConfig, VmConfigBuilder};
#[cfg(target_os = "macos")]
pub use error::VzError;
#[cfg(target_os = "macos")]
pub use install::{InstallResult, IpswSource, fetch_latest_ipsw_url, install_macos};
#[cfg(target_os = "macos")]
pub use vm::{Vm, VmState};
#[cfg(target_os = "macos")]
pub use vsock::{VsockListener, VsockStream};
