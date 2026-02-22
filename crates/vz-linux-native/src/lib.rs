//! Linux-native container backend for `vz`.
//!
//! Provides OCI bundle generation and container lifecycle management
//! for running containers directly on a Linux host (no VM). This crate
//! implements the [`RuntimeBackend`](vz_runtime_contract::RuntimeBackend)
//! trait from `vz-runtime-contract`.

#![forbid(unsafe_code)]

pub mod backend;
pub mod bundle;
pub mod cgroups;
pub mod config;
pub mod error;
pub mod network;
pub mod ns;
pub mod probe;
pub mod process;
pub mod runtime;

pub use backend::LinuxNativeBackend;
pub use bundle::{BundleMount, BundleSpec};
pub use config::{IsolationMode, LinuxNativeConfig, OciRuntime};
pub use error::LinuxNativeError;
pub use probe::{HostProbeReport, ProbeResult, probe_host};
pub use process::{OciState, ProcessOutput};
pub use runtime::ContainerRuntime;
