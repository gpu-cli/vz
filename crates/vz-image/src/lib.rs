//! Platform-independent OCI image store and puller.
//!
//! Provides content-addressed image storage, registry pull, and image
//! config resolution. Used by both the macOS (`vz-oci`) and Linux-native
//! (`vz-linux-native`) backends.

#![forbid(unsafe_code)]

pub mod error;
pub mod puller;
pub mod store;

pub use error::ImageError;
pub use puller::{Auth, ImageConfigSummary, ImageId, ImagePuller, parse_image_config_summary_from_store};
pub use store::{ImageInfo, ImageStore, LayerDescriptor, PruneResult};
