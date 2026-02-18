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
//! use std::path::Path;
//!
//! # async fn example() -> Result<(), vz_sandbox::SandboxError> {
//! let config = SandboxConfig {
//!     image_path: "base-macos.img".into(),
//!     cpus: 4,
//!     memory_gb: 8,
//!     state_path: Some("base-macos.state".into()),
//!     workspace_mount: "/Users/me/workspace".into(),
//!     ..Default::default()
//! };
//!
//! let pool = SandboxPool::new(config, 1).await?;
//!
//! // Acquire a sandbox session — VM is already warm
//! let session = pool.acquire(Path::new("/Users/me/workspace/my-project")).await?;
//!
//! // Run commands inside the sandbox
//! let output = session.exec("cargo build").await?;
//! assert_eq!(output.exit_code, 0);
//!
//! // Stream output from long-running commands
//! let mut stream = session.exec_streaming("cargo test").await?;
//! while let Some(event) = stream.next().await {
//!     match event {
//!         vz_sandbox::ExecEvent::Stdout(data) => {
//!             let _ = String::from_utf8_lossy(&data);
//!         }
//!         vz_sandbox::ExecEvent::Exit(code) => {
//!             assert_eq!(code, 0);
//!         }
//!         _ => {}
//!     }
//! }
//!
//! // Release back to pool
//! pool.release(session).await?;
//! # Ok(())
//! # }
//! ```

#![cfg(target_os = "macos")]
#![forbid(unsafe_code)]

pub mod channel;
pub mod error;
pub mod pool;
pub mod protocol;
pub mod session;

pub use channel::Channel;
pub use error::SandboxError;
pub use pool::{IsolationMode, NetworkPolicy, SandboxConfig, SandboxPool};
pub use protocol::{Handshake, HandshakeAck, Request, Response};
pub use session::{ExecEvent, ExecOutput, ExecStream, SandboxSession};
