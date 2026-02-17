//! Safe Rust API for Apple's Virtualization.framework.
//!
//! Create and manage macOS virtual machines with VirtioFS file sharing
//! and vsock host↔guest communication.
//!
//! # Example
//!
//! ```rust,no_run
//! use vz::{VmConfigBuilder, BootLoader, SharedDirConfig};
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
//! let vm = vz::Vm::create(config)?;
//! vm.start().await?;
//!
//! // Connect to guest agent over vsock
//! let stream = vm.vsock_connect(5000).await?;
//!
//! // When done
//! vm.stop().await?;
//! # Ok(())
//! # }
//! ```

#![cfg(target_os = "macos")]
#![forbid(unsafe_code)]

pub mod config;
pub mod error;
pub mod vm;
pub mod vsock;
pub mod virtio_fs;

pub use config::{BootLoader, SharedDirConfig, VmConfigBuilder};
pub use error::VzError;
pub use vm::{Vm, VmState};
pub use vsock::{VsockListener, VsockStream};
