//! High-level macOS VM sandbox for coding agents.
//!
//! This crate provides a "just give me a sandbox" abstraction on top of `vz`.
//! It manages a pool of pre-warmed macOS VMs and provides session-based
//! access with project directory mounting and typed communication channels.
//!
//! # Example
//!
//! ```rust,no_run
//! use vz_sandbox::{SandboxPool, SandboxConfig};
//! use std::path::PathBuf;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let config = SandboxConfig {
//!     image_path: PathBuf::from("./base-macos.img"),
//!     cpus: 4,
//!     memory_gb: 8,
//!     state_path: Some(PathBuf::from("./base-macos.state")),
//!     workspace_mount: PathBuf::from("/Users/me/workspace"),
//! };
//!
//! let pool = SandboxPool::new(config, 1).await?;
//!
//! // Acquire a sandbox session — VM is already warm
//! let session = pool.acquire("/Users/me/workspace/my-project").await?;
//!
//! // Run commands inside the sandbox
//! let output = session.exec("cargo build").await?;
//! println!("exit: {}", output.exit_code);
//!
//! // Release back to pool
//! pool.release(session).await?;
//! # Ok(())
//! # }
//! ```

#![cfg(target_os = "macos")]
#![forbid(unsafe_code)]

pub mod channel;
pub mod pool;
pub mod session;

pub use pool::{SandboxConfig, SandboxPool};
pub use session::{ExecOutput, SandboxSession};
pub use channel::Channel;
