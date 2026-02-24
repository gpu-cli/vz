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
//! // Release back to pool
//! pool.release(session).await?;
//! # Ok(())
//! # }
//! ```

#![cfg(target_os = "macos")]
#![forbid(unsafe_code)]

pub mod error;
pub mod pool;
pub mod session;

pub use error::SandboxError;
pub use pool::{CreateCheckpointSpec, IsolationMode, NetworkPolicy, SandboxConfig, SandboxPool};
pub use session::{ContainerLifecycleClass, SandboxSession};
pub use vz::protocol::{ExecEvent, ExecOutput};
pub use vz_linux::grpc_client::GrpcExecStream;
