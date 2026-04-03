use std::future::Future;

use crate::{
    Build, BuildSpec, ContainerInfo, ContainerLogs, Event, ExecConfig, ExecOutput, ImageInfo,
    IsolationLevel, NetworkServiceConfig, PortMapping, PruneResult, RunConfig, RuntimeCapabilities,
    RuntimeError, RuntimeOperation, SandboxSpec, StackResourceHint,
};

/// Workspace-oriented runtime manager that routes stack operations
/// through backend capabilities with deterministic fallback behavior.
pub struct WorkspaceRuntimeManager<B: RuntimeBackend> {
    backend: B,
}

impl<B: RuntimeBackend> WorkspaceRuntimeManager<B> {
    /// Create a new runtime manager over a concrete backend.
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    /// Access the wrapped backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Consume the manager and return the wrapped backend.
    pub fn into_inner(self) -> B {
        self.backend
    }

    /// Backend name for diagnostics.
    pub fn name(&self) -> &'static str {
        self.backend.name()
    }

    /// Capability snapshot.
    pub fn capabilities(&self) -> RuntimeCapabilities {
        self.backend.capabilities()
    }

    /// Pull an image reference and return resolved image id.
    pub async fn pull_image(&self, image: &str) -> Result<String, RuntimeError> {
        self.backend.pull(image).await
    }

    /// Create a standalone container.
    pub async fn create_container(
        &self,
        image: &str,
        config: RunConfig,
    ) -> Result<String, RuntimeError> {
        self.backend.create_container(image, config).await
    }

    /// Execute command inside a running container.
    pub async fn exec_container(
        &self,
        id: &str,
        config: ExecConfig,
    ) -> Result<ExecOutput, RuntimeError> {
        self.backend.exec_container(id, config).await
    }

    /// Stop a running container.
    pub async fn stop_container(
        &self,
        id: &str,
        force: bool,
        signal: Option<&str>,
        grace_period: Option<std::time::Duration>,
    ) -> Result<ContainerInfo, RuntimeError> {
        self.backend
            .stop_container(id, force, signal, grace_period)
            .await
    }

    /// Remove a container.
    pub async fn remove_container(&self, id: &str) -> Result<(), RuntimeError> {
        self.backend.remove_container(id).await
    }

    /// Fetch persisted container logs if supported by backend.
    pub fn container_logs(&self, container_id: &str) -> Result<ContainerLogs, RuntimeError> {
        self.backend.logs(container_id)
    }

    /// Ensure stack runtime environment is prepared.
    ///
    /// Transitional behavior: when `shared_vm` is unsupported this is a no-op
    /// and stack services fall back to plain container primitives.
    pub async fn ensure_stack_runtime(
        &self,
        stack_id: &str,
        ports: Vec<PortMapping>,
        resources: StackResourceHint,
    ) -> Result<(), RuntimeError> {
        if self.capabilities().shared_vm {
            self.backend
                .boot_shared_vm(stack_id, ports, resources)
                .await?;
        }
        Ok(())
    }

    /// Create a stack service container.
    ///
    /// If shared runtime capability is present, route through backend stack
    /// create path; otherwise fall back to plain `create_container`.
    pub async fn create_stack_container(
        &self,
        stack_id: &str,
        image: &str,
        config: RunConfig,
    ) -> Result<String, RuntimeError> {
        if self.capabilities().shared_vm {
            self.backend
                .create_container_in_stack(stack_id, image, config)
                .await
        } else {
            self.backend.create_container(image, config).await
        }
    }

    /// Configure stack service networking when capability is available.
    pub async fn setup_stack_network(
        &self,
        stack_id: &str,
        services: Vec<NetworkServiceConfig>,
    ) -> Result<(), RuntimeError> {
        let caps = self.capabilities();
        if caps.shared_vm && caps.stack_networking {
            self.backend.network_setup(stack_id, services).await?;
        }
        Ok(())
    }

    /// Tear down stack service networking when capability is available.
    pub async fn teardown_stack_network(
        &self,
        stack_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), RuntimeError> {
        let caps = self.capabilities();
        if caps.shared_vm && caps.stack_networking {
            self.backend
                .network_teardown(stack_id, service_names)
                .await?;
        }
        Ok(())
    }

    /// Shut down stack runtime environment when capability is available.
    pub async fn shutdown_stack_runtime(&self, stack_id: &str) -> Result<(), RuntimeError> {
        if self.capabilities().shared_vm {
            self.backend.shutdown_shared_vm(stack_id).await?;
        }
        Ok(())
    }

    /// Whether stack runtime is currently active.
    pub fn has_stack_runtime(&self, stack_id: &str) -> bool {
        if !self.capabilities().shared_vm {
            return false;
        }
        self.backend.has_shared_vm(stack_id)
    }

    /// List all tracked containers.
    pub fn list_containers(&self) -> Result<Vec<ContainerInfo>, RuntimeError> {
        self.backend.list_containers()
    }

    /// List locally cached images.
    pub fn list_images(&self) -> Result<Vec<ImageInfo>, RuntimeError> {
        self.backend.images()
    }

    /// Remove unreferenced images and layers.
    pub fn prune_images(&self) -> Result<PruneResult, RuntimeError> {
        self.backend.prune_images()
    }

    // ── Build operations ──────────────────────────────────────────

    /// Start an asynchronous image build.
    pub async fn start_build(
        &self,
        sandbox_id: &str,
        build_spec: BuildSpec,
        idempotency_key: Option<String>,
    ) -> Result<Build, RuntimeError> {
        self.backend
            .start_build(sandbox_id, build_spec, idempotency_key)
            .await
    }

    /// Load build status/details.
    pub async fn get_build(&self, build_id: &str) -> Result<Build, RuntimeError> {
        self.backend.get_build(build_id).await
    }

    /// Stream historical build events.
    pub async fn stream_build_events(
        &self,
        build_id: &str,
        after_event_id: Option<u64>,
    ) -> Result<Vec<Event>, RuntimeError> {
        self.backend
            .stream_build_events(build_id, after_event_id)
            .await
    }

    /// Cancel an in-flight build.
    pub async fn cancel_build(&self, build_id: &str) -> Result<Build, RuntimeError> {
        self.backend.cancel_build(build_id).await
    }

    // ── Sandbox-scoped operations ──────────────────────────────────
    //
    // These methods provide sandbox-oriented terminology for operations
    // that delegate to the underlying shared-VM / stack primitives on
    // `RuntimeBackend`.  They are intentionally thin wrappers that
    // align the manager surface with the Runtime V2 sandbox entity
    // model.

    /// Create a sandbox, delegating to the backend's `boot_shared_vm`.
    ///
    /// This is the sandbox-scoped entry point for provisioning an
    /// isolated runtime environment. The sandbox owns all containers,
    /// networking, and volumes created within its scope.
    pub async fn create_sandbox(
        &self,
        sandbox_id: &str,
        spec: &SandboxSpec,
        ports: Vec<PortMapping>,
    ) -> Result<(), RuntimeError> {
        let resources = StackResourceHint {
            cpus: spec.cpus,
            memory_mb: spec.memory_mb,
            volume_mounts: Vec::new(),
            disk_image_path: None,
        };
        self.backend
            .boot_shared_vm(sandbox_id, ports, resources)
            .await
    }

    /// Terminate a sandbox, delegating to `shutdown_shared_vm`.
    pub async fn terminate_sandbox(&self, sandbox_id: &str) -> Result<(), RuntimeError> {
        self.backend.shutdown_shared_vm(sandbox_id).await
    }

    /// Check if a sandbox is active.
    pub fn has_sandbox(&self, sandbox_id: &str) -> bool {
        self.backend.has_shared_vm(sandbox_id)
    }

    /// Create a container within a sandbox scope.
    ///
    /// Routes through the backend's `create_container_in_stack` path
    /// so that the container is created inside the sandbox's shared
    /// runtime environment.
    pub async fn create_container_in_sandbox(
        &self,
        sandbox_id: &str,
        image: &str,
        config: RunConfig,
    ) -> Result<String, RuntimeError> {
        self.backend
            .create_container_in_stack(sandbox_id, image, config)
            .await
    }

    /// Set up networking for services within a sandbox.
    pub async fn setup_sandbox_network(
        &self,
        sandbox_id: &str,
        services: Vec<NetworkServiceConfig>,
    ) -> Result<(), RuntimeError> {
        self.backend.network_setup(sandbox_id, services).await
    }

    /// Tear down networking for services within a sandbox.
    pub async fn teardown_sandbox_network(
        &self,
        sandbox_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), RuntimeError> {
        self.backend
            .network_teardown(sandbox_id, service_names)
            .await
    }

    // ── Execution control ───────────────────────────────────────────

    /// Write data to a running execution's stdin stream.
    pub async fn write_exec_stdin(
        &self,
        execution_id: &str,
        data: &[u8],
    ) -> Result<(), RuntimeError> {
        self.backend.write_exec_stdin(execution_id, data).await
    }

    /// Send a signal to a running execution.
    pub async fn signal_exec(&self, execution_id: &str, signal: &str) -> Result<(), RuntimeError> {
        self.backend.signal_exec(execution_id, signal).await
    }

    /// Resize the PTY dimensions for a running execution.
    pub async fn resize_exec_pty(
        &self,
        execution_id: &str,
        cols: u16,
        rows: u16,
    ) -> Result<(), RuntimeError> {
        self.backend.resize_exec_pty(execution_id, cols, rows).await
    }

    /// Cancel a running execution.
    pub async fn cancel_exec(&self, execution_id: &str) -> Result<(), RuntimeError> {
        self.backend.cancel_exec(execution_id).await
    }

    // ── Checkpoint operations ───────────────────────────────────────

    /// Create a checkpoint for a sandbox with the given class and fingerprint.
    pub async fn create_checkpoint(
        &self,
        sandbox_id: &str,
        class: &str,
        fingerprint: &str,
    ) -> Result<String, RuntimeError> {
        self.backend
            .create_checkpoint(sandbox_id, class, fingerprint)
            .await
    }

    /// Restore a sandbox from a previously created checkpoint.
    pub async fn restore_checkpoint(&self, checkpoint_id: &str) -> Result<(), RuntimeError> {
        self.backend.restore_checkpoint(checkpoint_id).await
    }

    /// Fork a checkpoint into a new sandbox lineage.
    pub async fn fork_checkpoint(
        &self,
        checkpoint_id: &str,
        new_sandbox_id: &str,
    ) -> Result<String, RuntimeError> {
        self.backend
            .fork_checkpoint(checkpoint_id, new_sandbox_id)
            .await
    }

    // ── Volume operations ───────────────────────────────────────────

    /// Create a named volume.
    pub async fn create_volume(&self, name: &str) -> Result<(), RuntimeError> {
        self.backend.create_volume(name).await
    }

    /// Attach a volume to a container at the given mount path.
    pub async fn attach_volume(
        &self,
        container_id: &str,
        volume_name: &str,
        mount_path: &str,
    ) -> Result<(), RuntimeError> {
        self.backend
            .attach_volume(container_id, volume_name, mount_path)
            .await
    }

    /// Detach a volume from a container.
    pub async fn detach_volume(
        &self,
        container_id: &str,
        volume_name: &str,
    ) -> Result<(), RuntimeError> {
        self.backend.detach_volume(container_id, volume_name).await
    }

    // ── Network domain operations ───────────────────────────────────

    /// Create an isolated network domain for a sandbox.
    pub async fn create_network_domain(
        &self,
        network_id: &str,
        sandbox_id: &str,
    ) -> Result<(), RuntimeError> {
        self.backend
            .create_network_domain(network_id, sandbox_id)
            .await
    }

    /// Destroy a network domain and release its resources.
    pub async fn destroy_network_domain(&self, network_id: &str) -> Result<(), RuntimeError> {
        self.backend.destroy_network_domain(network_id).await
    }

    /// Publish an ingress port on a network domain.
    pub async fn publish_port(
        &self,
        network_id: &str,
        host_port: u16,
        container_port: u16,
        protocol: &str,
    ) -> Result<(), RuntimeError> {
        self.backend
            .publish_port(network_id, host_port, container_port, protocol)
            .await
    }

    /// Connect a container to a network domain.
    pub async fn connect_container_to_network(
        &self,
        container_id: &str,
        network_id: &str,
    ) -> Result<(), RuntimeError> {
        self.backend
            .connect_container_to_network(container_id, network_id)
            .await
    }
}

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

    /// Capability flags for this backend/runtime implementation.
    ///
    /// Callers must check these flags before invoking capability-gated flows
    /// and return deterministic `unsupported_operation` diagnostics when false.
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities::default()
    }

    /// Isolation level provided by this backend.
    ///
    /// Defaults to [`IsolationLevel::Full`] (VM-based isolation). Override
    /// in backends that offer lighter-weight isolation modes.
    fn isolation_level(&self) -> IsolationLevel {
        IsolationLevel::Full
    }

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

    // ── Container commit ─────────────────────────────────────────

    /// Commit a running container's filesystem as a reusable snapshot.
    ///
    /// After setup commands modify a container's rootfs (installing packages,
    /// creating files, etc.), calling `commit_container` saves that state so
    /// future containers can start from the post-setup filesystem instead of
    /// re-assembling from the base image layers.
    ///
    /// Returns a reference string that can be passed to
    /// [`create_container_from_commit`](Self::create_container_from_commit)
    /// to start new containers from the committed state.
    fn commit_container(
        &self,
        _id: &str,
        _reference: &str,
    ) -> impl Future<Output = Result<String, RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: "commit_container".to_string(),
                reason: "backend does not support container commits".to_string(),
            })
        }
    }

    /// Check whether a committed rootfs snapshot exists for the given reference.
    fn has_committed_rootfs(&self, _reference: &str) -> bool {
        false
    }

    /// Create a container from a previously committed snapshot.
    ///
    /// Instead of pulling an image and assembling layers, this starts from
    /// the committed rootfs. The container gets its own writable copy.
    fn create_container_from_commit(
        &self,
        _reference: &str,
        _config: RunConfig,
    ) -> impl Future<Output = Result<String, RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: "create_container_from_commit".to_string(),
                reason: "backend does not support container commits".to_string(),
            })
        }
    }

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

    // ── Build operations ──────────────────────────────────────────

    /// Start an asynchronous build.
    fn start_build(
        &self,
        _sandbox_id: &str,
        _build_spec: BuildSpec,
        _idempotency_key: Option<String>,
    ) -> impl Future<Output = Result<Build, RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::StartBuild.as_str().to_string(),
                reason: "build operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Get build status/details.
    fn get_build(&self, _build_id: &str) -> impl Future<Output = Result<Build, RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::GetBuild.as_str().to_string(),
                reason: "build operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Stream historical build events for a build ID.
    fn stream_build_events(
        &self,
        _build_id: &str,
        _after_event_id: Option<u64>,
    ) -> impl Future<Output = Result<Vec<Event>, RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::StreamBuildEvents.as_str().to_string(),
                reason: "build operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Cancel an in-flight build.
    fn cancel_build(&self, _build_id: &str) -> impl Future<Output = Result<Build, RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::CancelBuild.as_str().to_string(),
                reason: "build operations are not supported by this backend".to_string(),
            })
        }
    }

    // ── Execution control ───────────────────────────────────────────

    /// Write data to a running execution's stdin stream.
    fn write_exec_stdin(
        &self,
        _execution_id: &str,
        _data: &[u8],
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::WriteExecStdin.as_str().to_string(),
                reason: "execution stdin control is not supported by this backend".to_string(),
            })
        }
    }

    /// Send a signal to a running execution.
    fn signal_exec(
        &self,
        _execution_id: &str,
        _signal: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::SignalExec.as_str().to_string(),
                reason: "execution signal control is not supported by this backend".to_string(),
            })
        }
    }

    /// Resize the PTY dimensions for a running execution.
    fn resize_exec_pty(
        &self,
        _execution_id: &str,
        _cols: u16,
        _rows: u16,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::ResizeExecPty.as_str().to_string(),
                reason: "execution PTY resize is not supported by this backend".to_string(),
            })
        }
    }

    /// Cancel a running execution.
    fn cancel_exec(&self, _execution_id: &str) -> impl Future<Output = Result<(), RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::CancelExec.as_str().to_string(),
                reason: "execution cancellation is not supported by this backend".to_string(),
            })
        }
    }

    // ── Checkpoint operations ───────────────────────────────────────

    /// Create a checkpoint for a sandbox with the given class and fingerprint.
    ///
    /// Returns the checkpoint identifier on success.
    fn create_checkpoint(
        &self,
        _sandbox_id: &str,
        _class: &str,
        _fingerprint: &str,
    ) -> impl Future<Output = Result<String, RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::CreateCheckpoint.as_str().to_string(),
                reason: "checkpoint operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Restore a sandbox from a previously created checkpoint.
    fn restore_checkpoint(
        &self,
        _checkpoint_id: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::RestoreCheckpoint.as_str().to_string(),
                reason: "checkpoint operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Fork a checkpoint into a new sandbox lineage.
    ///
    /// Returns the new checkpoint identifier on success.
    fn fork_checkpoint(
        &self,
        _checkpoint_id: &str,
        _new_sandbox_id: &str,
    ) -> impl Future<Output = Result<String, RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::ForkCheckpoint.as_str().to_string(),
                reason: "checkpoint operations are not supported by this backend".to_string(),
            })
        }
    }

    // ── Volume operations ───────────────────────────────────────────

    /// Create a named volume.
    fn create_volume(&self, _name: &str) -> impl Future<Output = Result<(), RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::CreateVolume.as_str().to_string(),
                reason: "volume operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Attach a volume to a container at the given mount path.
    fn attach_volume(
        &self,
        _container_id: &str,
        _volume_name: &str,
        _mount_path: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::AttachVolume.as_str().to_string(),
                reason: "volume operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Detach a volume from a container.
    fn detach_volume(
        &self,
        _container_id: &str,
        _volume_name: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::DetachVolume.as_str().to_string(),
                reason: "volume operations are not supported by this backend".to_string(),
            })
        }
    }

    // ── Network domain operations ───────────────────────────────────

    /// Create an isolated network domain for a sandbox.
    fn create_network_domain(
        &self,
        _network_id: &str,
        _sandbox_id: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::CreateNetworkDomain.as_str().to_string(),
                reason: "network domain operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Destroy a network domain and release its resources.
    fn destroy_network_domain(
        &self,
        _network_id: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: "destroy_network_domain".to_string(),
                reason: "network domain operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Publish an ingress port on a network domain.
    fn publish_port(
        &self,
        _network_id: &str,
        _host_port: u16,
        _container_port: u16,
        _protocol: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::PublishPort.as_str().to_string(),
                reason: "network domain operations are not supported by this backend".to_string(),
            })
        }
    }

    /// Connect a container to a network domain.
    fn connect_container_to_network(
        &self,
        _container_id: &str,
        _network_id: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        async {
            Err(RuntimeError::UnsupportedOperation {
                operation: RuntimeOperation::ConnectContainer.as_str().to_string(),
                reason: "network domain operations are not supported by this backend".to_string(),
            })
        }
    }
}
