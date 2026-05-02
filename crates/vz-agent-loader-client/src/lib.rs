//! Client library for vz-agent-loader.
//!
//! Provides:
//! - Protocol types shared between loader binary and host-side clients
//! - A generic async client that works over any `AsyncRead + AsyncWrite` stream
//! - Convenience helpers for building service entries
//!
//! This crate has no dependency on `vz` or any VM framework — it only needs
//! `tokio`, `serde`, and `serde_json`. Consumers bring their own transport
//! (vsock via vz crate, unix socket for testing, etc.).
//!
//! # Example
//!
//! ```rust,no_run
//! use vz_agent_loader_client::{LoaderClient, service_entry};
//!
//! # async fn example(stream: tokio::net::UnixStream) -> std::io::Result<()> {
//! let mut client = LoaderClient::new(stream);
//!
//! // Check loader is alive
//! let pong = client.ping().await?;
//! println!("loader v{}, up {}s", pong.version, pong.uptime_secs);
//!
//! // Register a service that persists across reboots
//! let entry = service_entry("my-agent", "/usr/local/bin/my-agent", true);
//! let result = client.register(entry, true).await?;
//! println!("started with exec_id={:?}", result.exec_id);
//! # Ok(())
//! # }
//! ```

mod client;
pub mod protocol;

pub use client::{LoaderClient, service_entry};
pub use protocol::*;
