//! macOS Virtualization.framework backend for the vz OCI runtime.
//!
//! This crate contains the macOS-specific runtime implementation that
//! boots Linux VMs via Virtualization.framework and manages OCI
//! container lifecycles within those VMs.

#![cfg(target_os = "macos")]
#![forbid(unsafe_code)]

pub mod config;
pub mod error;
pub mod macos_backend;
pub mod runtime;

pub use config::{
    ExecConfig, ExecutionMode, MountAccess, MountSpec, MountType, OciRuntimeKind, PortMapping,
    PortProtocol, RunConfig, RuntimeBackend, RuntimeConfig,
};
pub use error::MacosOciError;
pub use macos_backend::MacosRuntimeBackend;
pub use runtime::Runtime;

// Re-export shared types for convenience.
pub use vz_oci::bundle::CONTAINER_LOG_FILE;
pub use vz_oci::container_store::{ContainerInfo, ContainerStatus, ContainerStore};

/// Re-export the runtime contract crate for downstream access.
pub use vz_runtime_contract as contract;

// Re-export image types from the shared vz-image crate.
pub use vz_image::{
    Auth, ImageConfigSummary, ImageError, ImageId, ImageInfo, ImagePuller, ImageStore,
    LayerDescriptor, PruneResult, parse_image_config_summary_from_store,
};

pub use vz_linux::NetworkServiceConfig;
