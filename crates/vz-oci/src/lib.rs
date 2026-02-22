//! Platform-agnostic OCI bundle generation and container metadata.
//!
//! This crate provides the shared OCI infrastructure (bundle spec
//! generation, container metadata store) used by both the macOS VM
//! backend (`vz-oci-macos`) and the Linux-native backend
//! (`vz-linux-native`).

#![forbid(unsafe_code)]

pub mod bundle;
pub mod container_store;
pub mod error;

pub use bundle::CONTAINER_LOG_FILE;
pub use container_store::{ContainerInfo, ContainerStatus, ContainerStore};
pub use error::OciError;

// Re-export image types from the shared vz-image crate.
pub use vz_image::{
    Auth, ImageConfigSummary, ImageError, ImageId, ImageInfo, ImagePuller, ImageStore,
    LayerDescriptor, PruneResult, parse_image_config_summary_from_store,
};

/// Re-export the runtime contract crate for downstream access.
pub use vz_runtime_contract as contract;
