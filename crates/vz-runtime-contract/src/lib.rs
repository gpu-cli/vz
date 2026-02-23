//! Backend-neutral runtime contract for vz container backends.
//!
//! This crate defines the [`RuntimeBackend`] trait and shared types that
//! both the macOS (Virtualization.framework) and Linux-native backends
//! implement. Callers depend only on this contract, making the backend
//! selection transparent.

pub mod error;
pub mod selection;
pub mod types;

pub use error::RuntimeError;
pub use selection::{HostBackend, ResolvedBackend};
pub use types::{
    ContainerInfo, ContainerLogs, ContainerStatus, ExecConfig, ExecOutput, ImageInfo, MountAccess,
    MountSpec, MountType, NetworkServiceConfig, PortMapping, PortProtocol, PruneResult, RunConfig,
    StackResourceHint, StackVolumeMount,
};

/// Backend-neutral container runtime trait.
///
/// Each host platform provides an implementation of this trait. The
/// [`vz-oci`] facade holds a backend and delegates lifecycle operations
/// to it, keeping callers (`vz-stack`, `vz-cli`) unaware of the
/// underlying platform.
///
/// # Async Methods
///
/// Lifecycle methods are `async` because they may involve network I/O
/// (image pulls), IPC (guest agent communication on macOS), or
/// process management (OCI runtime invocation on Linux).
pub trait RuntimeBackend: Send + Sync {
    /// Human-readable backend name for diagnostics.
    fn name(&self) -> &'static str;

    // ── Image operations ──────────────────────────────────────────

    /// Pull an image from a registry and return its image ID (digest).
    fn pull(&self, image: &str) -> impl Future<Output = Result<String, RuntimeError>>;

    /// List locally cached images.
    fn images(&self) -> Result<Vec<ImageInfo>, RuntimeError>;

    /// Remove unreferenced images and layers.
    fn prune_images(&self) -> Result<PruneResult, RuntimeError>;

    // ── Container lifecycle ───────────────────────────────────────

    /// Pull image (if needed), run a command, and return output.
    ///
    /// This is the "one-shot" convenience path. Implementations may
    /// create a container, start it, wait for the command to finish,
    /// and clean up.
    fn run(
        &self,
        image: &str,
        config: RunConfig,
    ) -> impl Future<Output = Result<ExecOutput, RuntimeError>>;

    /// Create a container from an image and return its container ID.
    fn create_container(
        &self,
        image: &str,
        config: RunConfig,
    ) -> impl Future<Output = Result<String, RuntimeError>>;

    /// Execute a command in an already-running container.
    fn exec_container(
        &self,
        id: &str,
        config: ExecConfig,
    ) -> impl Future<Output = Result<ExecOutput, RuntimeError>>;

    /// Stop a running container.
    ///
    /// `signal` overrides the default stop signal (SIGTERM).
    /// `grace_period` overrides the default grace period before SIGKILL escalation.
    fn stop_container(
        &self,
        id: &str,
        force: bool,
        signal: Option<&str>,
        grace_period: Option<std::time::Duration>,
    ) -> impl Future<Output = Result<ContainerInfo, RuntimeError>>;

    /// Remove a stopped container and clean up its resources.
    fn remove_container(&self, id: &str) -> impl Future<Output = Result<(), RuntimeError>>;

    /// List all tracked containers.
    fn list_containers(&self) -> Result<Vec<ContainerInfo>, RuntimeError>;

    // ── Stack / multi-container support ───────────────────────────

    /// Boot a shared runtime environment for multi-container stacks.
    ///
    /// On macOS this boots a shared Linux VM. On Linux-native this may
    /// set up a shared network bridge. Returns `Ok(())` if already booted.
    fn boot_shared_vm(
        &self,
        _stack_id: &str,
        _ports: Vec<PortMapping>,
        _resources: StackResourceHint,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async { Ok(()) }
    }

    /// Create a container within a shared stack environment.
    ///
    /// Default implementation delegates to [`create_container`](Self::create_container).
    fn create_container_in_stack(
        &self,
        _stack_id: &str,
        image: &str,
        config: RunConfig,
    ) -> impl Future<Output = Result<String, RuntimeError>> {
        self.create_container(image, config)
    }

    /// Set up per-service networking within a stack.
    fn network_setup(
        &self,
        _stack_id: &str,
        _services: Vec<NetworkServiceConfig>,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async { Ok(()) }
    }

    /// Tear down per-service networking within a stack.
    fn network_teardown(
        &self,
        _stack_id: &str,
        _service_names: Vec<String>,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async { Ok(()) }
    }

    /// Shut down a shared stack runtime environment.
    fn shutdown_shared_vm(
        &self,
        _stack_id: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async { Ok(()) }
    }

    /// Check if a shared stack environment is currently booted.
    fn has_shared_vm(&self, _stack_id: &str) -> bool {
        false
    }

    /// Retrieve captured logs from a container.
    fn logs(&self, _container_id: &str) -> Result<ContainerLogs, RuntimeError> {
        Ok(ContainerLogs::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the trait is object-safe enough for our usage pattern.
    /// We use `impl RuntimeBackend` (static dispatch) not `dyn RuntimeBackend`,
    /// but this test documents that the types compile correctly.
    #[test]
    fn contract_types_are_constructible() {
        let _run = RunConfig::default();
        let _exec = ExecConfig::default();
        let _output = ExecOutput {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        };
        let _info = ContainerInfo {
            id: "test".to_string(),
            image: "img".to_string(),
            image_id: "sha256:abc".to_string(),
            status: ContainerStatus::Created,
            created_unix_secs: 0,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: None,
        };
        let _img = ImageInfo {
            reference: "ubuntu:latest".to_string(),
            image_id: "sha256:abc".to_string(),
        };
        let _prune = PruneResult {
            removed_refs: 0,
            removed_manifests: 0,
            removed_configs: 0,
            removed_layer_dirs: 0,
        };
        let _port = PortMapping {
            host: 8080,
            container: 80,
            protocol: PortProtocol::Tcp,
            target_host: None,
        };
        let _mount = MountSpec {
            source: None,
            target: std::path::PathBuf::from("/data"),
            mount_type: MountType::Tmpfs,
            access: MountAccess::ReadWrite,
            subpath: None,
        };
        let _net = NetworkServiceConfig {
            name: "web".to_string(),
            addr: "172.20.0.2".to_string(),
            network_name: "default".to_string(),
        };
        let _logs = ContainerLogs::default();
    }

    #[test]
    fn port_protocol_as_str() {
        assert_eq!(PortProtocol::Tcp.as_str(), "tcp");
        assert_eq!(PortProtocol::Udp.as_str(), "udp");
    }

    #[test]
    fn runtime_error_display() {
        let err = RuntimeError::ContainerNotFound {
            id: "abc".to_string(),
        };
        assert_eq!(err.to_string(), "container not found: abc");

        let err = RuntimeError::PullFailed {
            reference: "ubuntu:latest".to_string(),
            reason: "network timeout".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "pull failed for ubuntu:latest: network timeout"
        );
    }
}
