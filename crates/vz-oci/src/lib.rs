//! OCI runtime layer for `vz`.

#![cfg(target_os = "macos")]
#![forbid(unsafe_code)]

mod bundle;
mod config;
mod container_store;
mod error;
mod image;
mod runtime;
mod store;

pub use config::{
    Auth, ExecutionMode, MountAccess, MountSpec, MountType, OciRuntimeKind, PortMapping,
    PortProtocol, RunConfig, RuntimeBackend, RuntimeConfig,
};
pub use container_store::{ContainerInfo, ContainerStatus, ContainerStore};
pub use error::OciError;
pub use image::{ImageConfigSummary, ImageId, ImagePuller};
pub use runtime::Runtime;
pub use store::{ImageInfo, ImageStore, LayerDescriptor, PruneResult};

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn backend_selection_defaults_to_linux() {
        let backend = Runtime::select_backend("ubuntu:24.04", false);
        assert_eq!(backend, RuntimeBackend::Linux);
    }

    #[test]
    fn backend_selection_supports_macos_override() {
        let backend = Runtime::select_backend("ubuntu:24.04", true);
        assert_eq!(backend, RuntimeBackend::MacOS);
    }

    #[test]
    fn backend_selection_supports_macos_prefix() {
        let backend = Runtime::select_backend("macos:sonoma", false);
        assert_eq!(backend, RuntimeBackend::MacOS);
    }

    #[test]
    fn run_config_default_command_is_empty() {
        let cfg = RunConfig::default();
        assert!(cfg.cmd.is_empty());
        assert!(cfg.ports.is_empty());
        assert_eq!(cfg.execution_mode, ExecutionMode::GuestExec);
        assert!(cfg.container_id.is_none());
        assert!(cfg.init_process.is_none());
        assert!(cfg.oci_annotations.is_empty());
    }

    #[test]
    fn runtime_config_default_data_dir_points_to_oci_cache() {
        let cfg = RuntimeConfig::default();

        assert_eq!(cfg.data_dir, PathBuf::from("~/.vz/oci"));
        assert_eq!(cfg.guest_oci_runtime, OciRuntimeKind::Youki);
        assert!(cfg.guest_oci_runtime_path.is_none());
        assert!(cfg.guest_state_dir.is_none());
    }

    #[test]
    fn runtime_config_default_auth_is_anonymous() {
        let cfg = RuntimeConfig::default();

        assert_eq!(cfg.auth, Auth::Anonymous);
    }
}
