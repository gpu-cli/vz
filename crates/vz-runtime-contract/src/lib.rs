//! Backend-neutral runtime contract for vz container backends.
//!
//! This crate defines the [`RuntimeBackend`] trait and shared types that
//! both the macOS (Virtualization.framework) and Linux-native backends
//! implement. Callers depend only on this contract, making the backend
//! selection transparent.

pub mod error;
pub mod selection;
pub mod types;

pub use error::{MachineErrorCode, RuntimeError};
pub use selection::{HostBackend, ResolvedBackend};
pub use types::{
    Build, BuildSpec, BuildState, Capability, Checkpoint, CheckpointClass, CheckpointState,
    Container, ContainerInfo, ContainerLogs, ContainerMount, ContainerResources, ContainerSpec,
    ContainerState, ContainerStatus, ContractInvariantError, Event, EventRange, EventScope,
    ExecConfig, ExecOutput, Execution, ExecutionSpec, ExecutionState, Image, ImageInfo, Lease,
    LeaseState, MountAccess, MountSpec, MountType, NetworkDomain, NetworkDomainState,
    NetworkServiceConfig, PortMapping, PortProtocol, PruneResult, PublishedPort, Receipt,
    ReceiptResultClassification, RunConfig, RuntimeCapabilities, RuntimeOperation, Sandbox,
    SandboxBackend, SandboxSpec, SandboxState, SandboxVolumeMount, SharedVmPhase,
    SharedVmPhaseTracker, StackResourceHint, StackVolumeMount, Volume, VolumeType,
};

/// Canonical Runtime V2 operation surface expected from implementations.
pub const REQUIRED_RUNTIME_OPERATIONS: &[RuntimeOperation] = &RuntimeOperation::ALL;

/// Required idempotent mutation paths and their canonical operation names.
pub const REQUIRED_IDEMPOTENT_MUTATIONS: &[RuntimeOperation] = &[
    RuntimeOperation::CreateSandbox,
    RuntimeOperation::OpenLease,
    RuntimeOperation::PullImage,
    RuntimeOperation::StartBuild,
    RuntimeOperation::CreateContainer,
    RuntimeOperation::ExecContainer,
    RuntimeOperation::CreateCheckpoint,
    RuntimeOperation::ForkCheckpoint,
];

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::future::{Future, ready};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Wake, Waker};

    fn unsupported(operation: &str) -> RuntimeError {
        RuntimeError::UnsupportedOperation {
            operation: operation.to_string(),
            reason: "test stub".to_string(),
        }
    }

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn poll_immediate<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let waker = Waker::from(Arc::new(NoopWaker));
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);

        match Future::poll(future.as_mut(), &mut cx) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("future unexpectedly pending"),
        }
    }

    #[derive(Debug, Default)]
    struct StubBackend;

    impl RuntimeBackend for StubBackend {
        fn name(&self) -> &'static str {
            "stub"
        }

        fn pull(&self, _image: &str) -> impl Future<Output = Result<String, RuntimeError>> {
            ready(Err(unsupported("pull")))
        }

        fn images(&self) -> Result<Vec<ImageInfo>, RuntimeError> {
            Err(unsupported("images"))
        }

        fn prune_images(&self) -> Result<PruneResult, RuntimeError> {
            Err(unsupported("prune_images"))
        }

        fn run(
            &self,
            _image: &str,
            _config: RunConfig,
        ) -> impl Future<Output = Result<ExecOutput, RuntimeError>> {
            ready(Err(unsupported("run")))
        }

        fn create_container(
            &self,
            _image: &str,
            _config: RunConfig,
        ) -> impl Future<Output = Result<String, RuntimeError>> {
            ready(Err(unsupported("create_container")))
        }

        fn exec_container(
            &self,
            _id: &str,
            _config: ExecConfig,
        ) -> impl Future<Output = Result<ExecOutput, RuntimeError>> {
            ready(Err(unsupported("exec_container")))
        }

        fn stop_container(
            &self,
            _id: &str,
            _force: bool,
            _signal: Option<&str>,
            _grace_period: Option<std::time::Duration>,
        ) -> impl Future<Output = Result<ContainerInfo, RuntimeError>> {
            ready(Err(unsupported("stop_container")))
        }

        fn remove_container(&self, _id: &str) -> impl Future<Output = Result<(), RuntimeError>> {
            ready(Err(unsupported("remove_container")))
        }

        fn list_containers(&self) -> Result<Vec<ContainerInfo>, RuntimeError> {
            Err(unsupported("list_containers"))
        }
    }

    #[derive(Debug)]
    struct ManagerRoutingBackend {
        capabilities: RuntimeCapabilities,
        calls: Mutex<Vec<String>>,
    }

    impl ManagerRoutingBackend {
        fn new(capabilities: RuntimeCapabilities) -> Self {
            Self {
                capabilities,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn record(&self, call: &str) {
            self.calls.lock().unwrap().push(call.to_string());
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl RuntimeBackend for ManagerRoutingBackend {
        fn name(&self) -> &'static str {
            "manager-routing"
        }

        fn capabilities(&self) -> RuntimeCapabilities {
            self.capabilities
        }

        fn pull(&self, _image: &str) -> impl Future<Output = Result<String, RuntimeError>> {
            self.record("pull");
            ready(Ok("sha256:test".to_string()))
        }

        fn images(&self) -> Result<Vec<ImageInfo>, RuntimeError> {
            Ok(Vec::new())
        }

        fn prune_images(&self) -> Result<PruneResult, RuntimeError> {
            Ok(PruneResult {
                removed_refs: 0,
                removed_manifests: 0,
                removed_configs: 0,
                removed_layer_dirs: 0,
            })
        }

        fn run(
            &self,
            _image: &str,
            _config: RunConfig,
        ) -> impl Future<Output = Result<ExecOutput, RuntimeError>> {
            self.record("run");
            ready(Ok(ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            }))
        }

        fn create_container(
            &self,
            _image: &str,
            _config: RunConfig,
        ) -> impl Future<Output = Result<String, RuntimeError>> {
            self.record("create_container");
            ready(Ok("ctr-plain".to_string()))
        }

        fn exec_container(
            &self,
            _id: &str,
            _config: ExecConfig,
        ) -> impl Future<Output = Result<ExecOutput, RuntimeError>> {
            self.record("exec_container");
            ready(Ok(ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            }))
        }

        fn stop_container(
            &self,
            _id: &str,
            _force: bool,
            _signal: Option<&str>,
            _grace_period: Option<std::time::Duration>,
        ) -> impl Future<Output = Result<ContainerInfo, RuntimeError>> {
            self.record("stop_container");
            ready(Err(unsupported("stop_container")))
        }

        fn remove_container(&self, _id: &str) -> impl Future<Output = Result<(), RuntimeError>> {
            self.record("remove_container");
            ready(Ok(()))
        }

        fn list_containers(&self) -> Result<Vec<ContainerInfo>, RuntimeError> {
            Ok(Vec::new())
        }

        fn boot_shared_vm(
            &self,
            _stack_id: &str,
            _ports: Vec<PortMapping>,
            _resources: StackResourceHint,
        ) -> impl Future<Output = Result<(), RuntimeError>> {
            self.record("boot_shared_vm");
            ready(Ok(()))
        }

        fn create_container_in_stack(
            &self,
            _stack_id: &str,
            _image: &str,
            _config: RunConfig,
        ) -> impl Future<Output = Result<String, RuntimeError>> {
            self.record("create_container_in_stack");
            ready(Ok("ctr-stack".to_string()))
        }

        fn network_setup(
            &self,
            _stack_id: &str,
            _services: Vec<NetworkServiceConfig>,
        ) -> impl Future<Output = Result<(), RuntimeError>> {
            self.record("network_setup");
            ready(Ok(()))
        }
    }

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
        let _capabilities = RuntimeCapabilities::default();
        let _stack_capabilities = RuntimeCapabilities::stack_baseline();
        let _sandbox_spec = SandboxSpec {
            cpus: Some(2),
            memory_mb: Some(4096),
            network_profile: Some("default".to_string()),
            volume_mounts: vec![SandboxVolumeMount {
                volume_id: "vol-1".to_string(),
                target: "/data".to_string(),
                read_only: false,
            }],
        };
        let _sandbox = Sandbox {
            sandbox_id: "sbx-1".to_string(),
            backend: SandboxBackend::MacosVz,
            spec: _sandbox_spec.clone(),
            state: SandboxState::Ready,
            created_at: 10,
            updated_at: 11,
            labels: BTreeMap::new(),
        };
        let _lease = Lease {
            lease_id: "lease-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            ttl_secs: 60,
            last_heartbeat_at: 20,
            state: LeaseState::Active,
        };
        let _image = Image {
            image_ref: "alpine:latest".to_string(),
            resolved_digest: "sha256:abc".to_string(),
            platform: "linux/amd64".to_string(),
            source_registry: "docker.io".to_string(),
            pulled_at: 30,
        };
        let _build = Build {
            build_id: "b-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            build_spec: BuildSpec {
                context: ".".to_string(),
                dockerfile: Some("Dockerfile".to_string()),
                args: BTreeMap::new(),
            },
            state: BuildState::Queued,
            result_digest: None,
            started_at: 40,
            ended_at: None,
        };
        let _container = Container {
            container_id: "ctr-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            image_digest: "sha256:abc".to_string(),
            container_spec: ContainerSpec {
                cmd: vec!["sleep".to_string(), "1".to_string()],
                env: BTreeMap::new(),
                cwd: None,
                user: None,
                mounts: vec![ContainerMount {
                    volume_id: "vol-1".to_string(),
                    target: "/work".to_string(),
                    access_mode: MountAccess::ReadWrite,
                }],
                resources: ContainerResources::default(),
                network_attachments: vec!["net-1".to_string()],
            },
            state: ContainerState::Created,
            created_at: 50,
            started_at: None,
            ended_at: None,
        };
        let _execution = Execution {
            execution_id: "exec-1".to_string(),
            container_id: "ctr-1".to_string(),
            exec_spec: ExecutionSpec {
                cmd: vec!["echo".to_string()],
                args: vec!["hello".to_string()],
                env_override: BTreeMap::new(),
                pty: false,
                timeout_secs: Some(10),
            },
            state: ExecutionState::Queued,
            exit_code: None,
            started_at: None,
            ended_at: None,
        };
        let _volume = Volume {
            volume_id: "vol-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            volume_type: VolumeType::Named,
            source: "named://vol-1".to_string(),
            target: "/data".to_string(),
            access_mode: MountAccess::ReadWrite,
        };
        let _network = NetworkDomain {
            network_id: "net-1".to_string(),
            sandbox_id: Some("sbx-1".to_string()),
            stack_id: None,
            state: NetworkDomainState::Ready,
            dns_zone: "sandbox.local".to_string(),
            published_ports: vec![PublishedPort {
                host_port: 8080,
                container_port: 80,
                protocol: PortProtocol::Tcp,
            }],
        };
        let _checkpoint = Checkpoint {
            checkpoint_id: "ckpt-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Creating,
            created_at: 60,
            compatibility_fingerprint: "linux-amd64".to_string(),
        };
        let _event = Event {
            event_id: 1,
            ts: 70,
            scope: EventScope::Sandbox,
            scope_id: "sbx-1".to_string(),
            event_type: "sandbox.ready".to_string(),
            payload: BTreeMap::new(),
            trace_id: Some("trace-1".to_string()),
        };
        let _receipt = Receipt {
            receipt_id: "r-1".to_string(),
            scope: EventScope::Sandbox,
            scope_id: "sbx-1".to_string(),
            request_hash: "req".to_string(),
            policy_hash: None,
            result_classification: ReceiptResultClassification::Success,
            artifacts: vec![],
            resource_summary: BTreeMap::new(),
            event_range: EventRange {
                start_event_id: 1,
                end_event_id: 1,
            },
        };
        let _capability = Capability::ComposeAdapter;
    }

    #[test]
    fn default_build_operations_return_unsupported_operation() {
        let backend = StubBackend;

        let start_error = poll_immediate(backend.start_build(
            "sandbox-1",
            BuildSpec::default(),
            Some("idem-1".to_string()),
        ))
        .unwrap_err();
        let get_error = poll_immediate(backend.get_build("build-1")).unwrap_err();
        let stream_error =
            poll_immediate(backend.stream_build_events("build-1", Some(10))).unwrap_err();
        let cancel_error = poll_immediate(backend.cancel_build("build-1")).unwrap_err();

        for (error, operation) in [
            (start_error, RuntimeOperation::StartBuild.as_str()),
            (get_error, RuntimeOperation::GetBuild.as_str()),
            (stream_error, RuntimeOperation::StreamBuildEvents.as_str()),
            (cancel_error, RuntimeOperation::CancelBuild.as_str()),
        ] {
            match error {
                RuntimeError::UnsupportedOperation { operation: got, .. } => {
                    assert_eq!(got, operation);
                }
                other => panic!("expected unsupported operation error, got: {other:?}"),
            }
        }
    }

    #[test]
    fn workspace_runtime_manager_routes_stack_create_with_shared_runtime() {
        let backend = ManagerRoutingBackend::new(RuntimeCapabilities::stack_baseline());
        let manager = WorkspaceRuntimeManager::new(backend);

        let created = poll_immediate(manager.create_stack_container(
            "stack-1",
            "nginx:latest",
            RunConfig::default(),
        ))
        .unwrap();

        assert_eq!(created, "ctr-stack");
        assert_eq!(manager.backend().calls(), vec!["create_container_in_stack"]);
    }

    #[test]
    fn workspace_runtime_manager_falls_back_to_plain_create_when_shared_disabled() {
        let mut caps = RuntimeCapabilities::stack_baseline();
        caps.shared_vm = false;
        let backend = ManagerRoutingBackend::new(caps);
        let manager = WorkspaceRuntimeManager::new(backend);

        let created = poll_immediate(manager.create_stack_container(
            "stack-1",
            "nginx:latest",
            RunConfig::default(),
        ))
        .unwrap();

        assert_eq!(created, "ctr-plain");
        assert_eq!(manager.backend().calls(), vec!["create_container"]);
    }

    #[test]
    fn workspace_runtime_manager_skips_network_setup_without_capability() {
        let mut caps = RuntimeCapabilities::stack_baseline();
        caps.stack_networking = false;
        let backend = ManagerRoutingBackend::new(caps);
        let manager = WorkspaceRuntimeManager::new(backend);

        poll_immediate(manager.setup_stack_network("stack-1", Vec::new())).unwrap();

        assert!(manager.backend().calls().is_empty());
    }

    #[test]
    fn lifecycle_consistency_checks() {
        let mut info = ContainerInfo {
            id: "c1".to_string(),
            image: "img".to_string(),
            image_id: "sha256:abc".to_string(),
            status: ContainerStatus::Running,
            created_unix_secs: 0,
            started_unix_secs: Some(1),
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: None,
        };

        assert!(info.ensure_lifecycle_consistency().is_ok());

        info.started_unix_secs = None;
        assert!(matches!(
            info.ensure_lifecycle_consistency(),
            Err(ContractInvariantError::LifecycleInconsistency { .. })
        ));

        info.status = ContainerStatus::Stopped { exit_code: 0 };
        info.created_unix_secs = 2;
        info.started_unix_secs = Some(1);
        info.stopped_unix_secs = Some(3);
        assert!(matches!(
            info.ensure_lifecycle_consistency(),
            Err(ContractInvariantError::LifecycleInconsistency { .. })
        ));
    }

    #[test]
    fn shared_vm_phase_transitions() {
        let mut tracker = SharedVmPhaseTracker::new();
        assert_eq!(tracker.phase(), SharedVmPhase::Shutdown);

        tracker.transition_to(SharedVmPhase::Booting).unwrap();
        tracker.transition_to(SharedVmPhase::Ready).unwrap();
        tracker.transition_to(SharedVmPhase::ShuttingDown).unwrap();
        tracker.transition_to(SharedVmPhase::Shutdown).unwrap();

        assert!(matches!(
            tracker.transition_to(SharedVmPhase::Ready),
            Err(ContractInvariantError::SharedVmPhaseTransition { .. })
        ));
    }

    #[test]
    fn sandbox_and_lease_state_invariants() {
        let mut sandbox = Sandbox {
            sandbox_id: "s-1".to_string(),
            backend: SandboxBackend::LinuxFirecracker,
            spec: SandboxSpec::default(),
            state: SandboxState::Creating,
            created_at: 0,
            updated_at: 0,
            labels: BTreeMap::new(),
        };

        assert!(matches!(
            sandbox.ensure_can_open_lease(),
            Err(ContractInvariantError::LeaseRequiresReadySandbox { .. })
        ));

        sandbox.transition_to(SandboxState::Ready).unwrap();
        sandbox.ensure_can_open_lease().unwrap();
        sandbox.transition_to(SandboxState::Draining).unwrap();
        sandbox.transition_to(SandboxState::Terminated).unwrap();
        assert!(matches!(
            sandbox.transition_to(SandboxState::Ready),
            Err(ContractInvariantError::SandboxStateTransition { .. })
        ));

        let mut lease = Lease {
            lease_id: "l-1".to_string(),
            sandbox_id: "s-1".to_string(),
            ttl_secs: 30,
            last_heartbeat_at: 1,
            state: LeaseState::Opening,
        };
        assert!(matches!(
            lease.ensure_can_submit_work("create_container"),
            Err(ContractInvariantError::WorkRequiresActiveLease { .. })
        ));
        lease.transition_to(LeaseState::Active).unwrap();
        lease.ensure_can_submit_work("create_container").unwrap();
        lease.transition_to(LeaseState::Closed).unwrap();
        assert!(matches!(
            lease.ensure_can_submit_work("create_container"),
            Err(ContractInvariantError::WorkRequiresActiveLease { .. })
        ));
        assert!(matches!(
            lease.transition_to(LeaseState::Active),
            Err(ContractInvariantError::LeaseStateTransition { .. })
        ));
    }

    #[test]
    fn container_and_execution_state_invariants() {
        let mut container = Container {
            container_id: "c-1".to_string(),
            sandbox_id: "s-1".to_string(),
            image_digest: "sha256:abc".to_string(),
            container_spec: ContainerSpec::default(),
            state: ContainerState::Created,
            created_at: 1,
            started_at: None,
            ended_at: None,
        };

        assert!(matches!(
            container.ensure_can_exec(),
            Err(ContractInvariantError::ExecRequiresRunningContainer { .. })
        ));
        container.transition_to(ContainerState::Starting).unwrap();
        container.transition_to(ContainerState::Running).unwrap();
        container.ensure_can_exec().unwrap();
        container.transition_to(ContainerState::Stopping).unwrap();
        container.transition_to(ContainerState::Exited).unwrap();
        assert!(matches!(
            container.ensure_can_exec(),
            Err(ContractInvariantError::ExecRequiresRunningContainer { .. })
        ));
        container.transition_to(ContainerState::Removed).unwrap();
        assert!(matches!(
            container.transition_to(ContainerState::Running),
            Err(ContractInvariantError::ContainerStateTransition { .. })
        ));

        let mut execution = Execution {
            execution_id: "e-1".to_string(),
            container_id: "c-1".to_string(),
            exec_spec: ExecutionSpec::default(),
            state: ExecutionState::Queued,
            exit_code: None,
            started_at: None,
            ended_at: None,
        };
        execution.ensure_lifecycle_consistency().unwrap();
        execution.transition_to(ExecutionState::Running).unwrap();
        execution.started_at = Some(2);
        execution.ensure_lifecycle_consistency().unwrap();
        execution.transition_to(ExecutionState::Exited).unwrap();
        execution.ended_at = Some(3);
        execution.exit_code = Some(0);
        execution.ensure_lifecycle_consistency().unwrap();
        assert!(matches!(
            execution.transition_to(ExecutionState::Running),
            Err(ContractInvariantError::ExecutionStateTransition { .. })
        ));
    }

    #[test]
    fn build_receipt_and_capability_invariants() {
        let mut build = Build {
            build_id: "b-1".to_string(),
            sandbox_id: "s-1".to_string(),
            build_spec: BuildSpec::default(),
            state: BuildState::Queued,
            result_digest: None,
            started_at: 1,
            ended_at: None,
        };
        build.ensure_lifecycle_consistency().unwrap();
        build.transition_to(BuildState::Running).unwrap();
        build.transition_to(BuildState::Succeeded).unwrap();
        build.result_digest = Some("sha256:abcd".to_string());
        build.ended_at = Some(2);
        build.ensure_lifecycle_consistency().unwrap();
        assert!(matches!(
            build.transition_to(BuildState::Running),
            Err(ContractInvariantError::BuildStateTransition { .. })
        ));

        let image = Image {
            image_ref: "alpine:latest".to_string(),
            resolved_digest: "sha256:abcd".to_string(),
            platform: "linux/amd64".to_string(),
            source_registry: "docker.io".to_string(),
            pulled_at: 1,
        };
        image.ensure_digest_immutable().unwrap();

        let bad_image = Image {
            image_ref: "alpine:latest".to_string(),
            resolved_digest: "latest".to_string(),
            platform: "linux/amd64".to_string(),
            source_registry: "docker.io".to_string(),
            pulled_at: 1,
        };
        assert!(matches!(
            bad_image.ensure_digest_immutable(),
            Err(ContractInvariantError::ImageDigestInvariant { .. })
        ));

        let receipt = Receipt {
            receipt_id: "r-1".to_string(),
            scope: EventScope::Sandbox,
            scope_id: "s-1".to_string(),
            request_hash: "req".to_string(),
            policy_hash: None,
            result_classification: ReceiptResultClassification::Success,
            artifacts: vec![],
            resource_summary: BTreeMap::new(),
            event_range: EventRange {
                start_event_id: 10,
                end_event_id: 11,
            },
        };
        receipt.ensure_event_range_ordered().unwrap();

        let bad_receipt = Receipt {
            event_range: EventRange {
                start_event_id: 12,
                end_event_id: 11,
            },
            ..receipt
        };
        assert!(matches!(
            bad_receipt.ensure_event_range_ordered(),
            Err(ContractInvariantError::ReceiptEventRangeInvalid { .. })
        ));

        let list = RuntimeCapabilities::stack_baseline().to_capability_list();
        assert!(list.contains(&Capability::ComposeAdapter));
        assert!(list.contains(&Capability::SharedVm));
        assert!(list.contains(&Capability::StackNetworking));
    }

    #[test]
    fn required_operations_and_idempotency_surface_match_contract() {
        assert_eq!(REQUIRED_RUNTIME_OPERATIONS.len(), 34);
        assert_eq!(
            RuntimeOperation::ALL.len(),
            REQUIRED_RUNTIME_OPERATIONS.len()
        );
        assert_eq!(REQUIRED_IDEMPOTENT_MUTATIONS.len(), 8);

        for operation in REQUIRED_RUNTIME_OPERATIONS {
            assert_eq!(
                operation.requires_idempotency_key(),
                operation.idempotency_key_prefix().is_some()
            );
        }

        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::CreateSandbox));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::OpenLease));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::PullImage));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::StartBuild));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::CreateContainer));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::ExecContainer));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::CreateCheckpoint));
        assert!(REQUIRED_IDEMPOTENT_MUTATIONS.contains(&RuntimeOperation::ForkCheckpoint));

        assert!(!RuntimeOperation::GetReceipt.requires_idempotency_key());
        assert!(!RuntimeOperation::ListEvents.requires_idempotency_key());
        assert_eq!(
            RuntimeOperation::CreateSandbox.idempotency_key_prefix(),
            Some("create_sandbox")
        );
        assert_eq!(
            RuntimeOperation::GetCapabilities.idempotency_key_prefix(),
            None
        );
    }

    #[test]
    fn runtime_error_machine_codes_are_stable() {
        assert_eq!(
            MachineErrorCode::ALL.map(MachineErrorCode::as_str),
            [
                "validation_error",
                "not_found",
                "state_conflict",
                "policy_denied",
                "timeout",
                "backend_unavailable",
                "unsupported_operation",
                "internal_error",
            ]
        );

        assert_eq!(
            RuntimeError::InvalidConfig("bad".to_string()).machine_code(),
            MachineErrorCode::ValidationError
        );
        assert_eq!(
            RuntimeError::ContainerNotFound {
                id: "c1".to_string()
            }
            .machine_code(),
            MachineErrorCode::NotFound
        );
        assert_eq!(
            RuntimeError::ContainerFailed {
                id: "c1".to_string(),
                reason: "already stopped".to_string(),
            }
            .machine_code(),
            MachineErrorCode::StateConflict
        );
        assert_eq!(
            RuntimeError::PullFailed {
                reference: "img:latest".to_string(),
                reason: "network timeout".to_string(),
            }
            .machine_code(),
            MachineErrorCode::Timeout
        );
        assert_eq!(
            RuntimeError::UnsupportedOperation {
                operation: "fork_checkpoint".to_string(),
                reason: "missing checkpoint_fork capability".to_string(),
            }
            .machine_code(),
            MachineErrorCode::UnsupportedOperation
        );
        assert_eq!(
            RuntimeError::Backend {
                message: "agent unavailable".to_string(),
                source: Box::new(std::io::Error::other("dial failed")),
            }
            .machine_code(),
            MachineErrorCode::InternalError
        );
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

        let err = RuntimeError::UnsupportedOperation {
            operation: "network_setup".to_string(),
            reason: "missing stack_networking capability".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "unsupported operation `network_setup`: missing stack_networking capability"
        );
    }
}
