use super::*;
use std::collections::{BTreeMap, BTreeSet, HashSet};
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

#[derive(Debug, Clone, Copy)]
enum StubPolicyMode {
    Allow,
    Deny,
    Fail,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct StubPolicyError(&'static str);

#[derive(Debug, Clone, Copy)]
struct StubPolicyHook {
    mode: StubPolicyMode,
}

impl RuntimePolicyHook for StubPolicyHook {
    fn evaluate(
        &self,
        _operation: RuntimeOperation,
        _metadata: &RequestMetadata,
    ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
        match self.mode {
            StubPolicyMode::Allow => Ok(PolicyDecision::Allow),
            StubPolicyMode::Deny => Ok(PolicyDecision::Deny {
                reason: "blocked by test policy".to_string(),
            }),
            StubPolicyMode::Fail => Err(Box::new(StubPolicyError("policy backend offline"))),
        }
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

    fn logs(&self, _container_id: &str) -> Result<ContainerLogs, RuntimeError> {
        self.record("logs");
        Ok(ContainerLogs::default())
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

    fn network_teardown(
        &self,
        _stack_id: &str,
        _service_names: Vec<String>,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        self.record("network_teardown");
        ready(Ok(()))
    }

    fn shutdown_shared_vm(
        &self,
        _stack_id: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> {
        self.record("shutdown_shared_vm");
        ready(Ok(()))
    }

    fn has_shared_vm(&self, _stack_id: &str) -> bool {
        self.record("has_shared_vm");
        true
    }
}

/// Verify the trait is object-safe enough for our usage pattern.
/// We use `impl RuntimeBackend` (static dispatch) not `dyn RuntimeBackend`,
/// but this test documents that the types compile correctly.
#[test]
fn contract_types_are_constructible() {
    let _run = RunConfig::default();
    let exec = ExecConfig::default();
    assert!(exec.execution_id.is_none());
    assert!(!exec.pty);
    assert!(exec.term_rows.is_none());
    assert!(exec.term_cols.is_none());
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
        base_image_ref: Some("alpine:3.20".to_string()),
        main_container: Some("workspace-main".to_string()),
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
            target: None,
            args: BTreeMap::new(),
            cache_from: Vec::new(),
            image_tag: None,
            secrets: Vec::new(),
            no_cache: false,
            push: false,
            output_oci_tar_dest: None,
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
    let _checkpoint_class_metadata = CheckpointClass::FsQuick.metadata();
    let _checkpoint_compatibility = CheckpointCompatibilityMetadata {
        backend_id: "macos-vz".to_string(),
        backend_version: "0.1.0".to_string(),
        runtime_version: "2".to_string(),
        guest_artifact_versions: BTreeMap::from([("agent".to_string(), "1.2.3".to_string())]),
        config_hash: "sha256:abc".to_string(),
        host_compatibility_markers: BTreeMap::from([(
            "host.os".to_string(),
            "macos-15".to_string(),
        )]),
    };
    let _checkpoint_metadata =
        CheckpointMetadata::new(_checkpoint.clone(), _checkpoint_compatibility);
    let _checkpoint_store = CheckpointLineageStore::default();
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
fn manager_create_sandbox_delegates_to_boot_shared_vm() {
    let backend = ManagerRoutingBackend::new(RuntimeCapabilities::stack_baseline());
    let manager = WorkspaceRuntimeManager::new(backend);
    let spec = SandboxSpec {
        cpus: Some(4),
        memory_mb: Some(8192),
        ..SandboxSpec::default()
    };

    poll_immediate(manager.create_sandbox("sbx-1", &spec, vec![])).unwrap();

    assert_eq!(manager.backend().calls(), vec!["boot_shared_vm"]);
}

#[test]
fn manager_terminate_sandbox_delegates_to_shutdown_shared_vm() {
    let backend = ManagerRoutingBackend::new(RuntimeCapabilities::stack_baseline());
    let manager = WorkspaceRuntimeManager::new(backend);

    poll_immediate(manager.terminate_sandbox("sbx-1")).unwrap();

    assert_eq!(manager.backend().calls(), vec!["shutdown_shared_vm"]);
}

#[test]
fn manager_has_sandbox_delegates_to_has_shared_vm() {
    let backend = ManagerRoutingBackend::new(RuntimeCapabilities::stack_baseline());
    let manager = WorkspaceRuntimeManager::new(backend);

    assert!(manager.has_sandbox("sbx-1"));
    assert_eq!(manager.backend().calls(), vec!["has_shared_vm"]);
}

#[test]
fn manager_create_container_in_sandbox_delegates_to_create_container_in_stack() {
    let backend = ManagerRoutingBackend::new(RuntimeCapabilities::stack_baseline());
    let manager = WorkspaceRuntimeManager::new(backend);

    let id = poll_immediate(manager.create_container_in_sandbox(
        "sbx-1",
        "alpine:latest",
        RunConfig::default(),
    ))
    .unwrap();

    assert_eq!(id, "ctr-stack");
    assert_eq!(manager.backend().calls(), vec!["create_container_in_stack"]);
}

#[test]
fn manager_setup_sandbox_network_delegates_to_network_setup() {
    let backend = ManagerRoutingBackend::new(RuntimeCapabilities::stack_baseline());
    let manager = WorkspaceRuntimeManager::new(backend);

    poll_immediate(manager.setup_sandbox_network(
        "sbx-1",
        vec![NetworkServiceConfig {
            name: "web".to_string(),
            addr: "172.20.0.2".to_string(),
            network_name: "default".to_string(),
        }],
    ))
    .unwrap();

    assert_eq!(manager.backend().calls(), vec!["network_setup"]);
}

#[test]
fn manager_teardown_sandbox_network_delegates_to_network_teardown() {
    let backend = ManagerRoutingBackend::new(RuntimeCapabilities::stack_baseline());
    let manager = WorkspaceRuntimeManager::new(backend);

    poll_immediate(manager.teardown_sandbox_network("sbx-1", vec!["web".to_string()])).unwrap();

    assert_eq!(manager.backend().calls(), vec!["network_teardown"]);
}

#[test]
fn manager_list_containers_delegates_to_backend() {
    let backend = ManagerRoutingBackend::new(RuntimeCapabilities::stack_baseline());
    let manager = WorkspaceRuntimeManager::new(backend);

    let containers = manager.list_containers().unwrap();
    assert!(containers.is_empty());
}

#[test]
fn manager_list_images_delegates_to_backend() {
    let backend = ManagerRoutingBackend::new(RuntimeCapabilities::stack_baseline());
    let manager = WorkspaceRuntimeManager::new(backend);

    let images = manager.list_images().unwrap();
    assert!(images.is_empty());
}

#[test]
fn manager_prune_images_delegates_to_backend() {
    let backend = ManagerRoutingBackend::new(RuntimeCapabilities::stack_baseline());
    let manager = WorkspaceRuntimeManager::new(backend);

    let result = manager.prune_images().unwrap();
    assert_eq!(result.removed_refs, 0);
}

#[test]
fn checkpoint_lineage_store_enforces_parent_and_duplicates() {
    let mut store = CheckpointLineageStore::default();
    let compatibility = CheckpointCompatibilityMetadata {
        backend_id: "linux-native".to_string(),
        backend_version: "0.1.0".to_string(),
        runtime_version: "2".to_string(),
        guest_artifact_versions: BTreeMap::new(),
        config_hash: "sha256:config".to_string(),
        host_compatibility_markers: BTreeMap::new(),
    };
    assert!(compatibility.is_complete());

    let root = CheckpointMetadata::new(
        Checkpoint {
            checkpoint_id: "ckpt-root".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Ready,
            created_at: 1,
            compatibility_fingerprint: "fingerprint-1".to_string(),
        },
        compatibility.clone(),
    );
    assert_eq!(
        root.class_metadata,
        CheckpointClassMetadata {
            includes_filesystem_state: true,
            includes_memory_state: false,
            includes_cpu_and_device_state: false,
        }
    );
    store.register(root).unwrap();

    let child = CheckpointMetadata::new(
        Checkpoint {
            checkpoint_id: "ckpt-child".to_string(),
            sandbox_id: "sbx-2".to_string(),
            parent_checkpoint_id: Some("ckpt-root".to_string()),
            class: CheckpointClass::VmFull,
            state: CheckpointState::Ready,
            created_at: 2,
            compatibility_fingerprint: "fingerprint-2".to_string(),
        },
        compatibility.clone(),
    );
    store.register(child).unwrap();

    assert_eq!(store.children_of("ckpt-root").len(), 1);
    assert_eq!(store.list_for_sandbox("sbx-2").len(), 1);

    let missing_parent = CheckpointMetadata::new(
        Checkpoint {
            checkpoint_id: "ckpt-missing-parent".to_string(),
            sandbox_id: "sbx-3".to_string(),
            parent_checkpoint_id: Some("does-not-exist".to_string()),
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Creating,
            created_at: 3,
            compatibility_fingerprint: "fingerprint-3".to_string(),
        },
        compatibility.clone(),
    );
    assert!(matches!(
        store.register(missing_parent),
        Err(ContractInvariantError::CheckpointParentNotFound { .. })
    ));

    let duplicate = CheckpointMetadata::new(
        Checkpoint {
            checkpoint_id: "ckpt-root".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Ready,
            created_at: 4,
            compatibility_fingerprint: "fingerprint-4".to_string(),
        },
        compatibility,
    );
    assert!(matches!(
        store.register(duplicate),
        Err(ContractInvariantError::CheckpointAlreadyExists { .. })
    ));
}

#[test]
fn validate_checkpoint_restore_compatibility_accepts_matching_metadata() {
    let compatibility = CheckpointCompatibilityMetadata {
        backend_id: "macos-vz".to_string(),
        backend_version: "0.1.0".to_string(),
        runtime_version: "2".to_string(),
        guest_artifact_versions: BTreeMap::from([("guest-agent".to_string(), "1.0.0".to_string())]),
        config_hash: "sha256:cfg".to_string(),
        host_compatibility_markers: BTreeMap::from([(
            "host.cpu".to_string(),
            "apple-silicon".to_string(),
        )]),
    };
    let metadata = CheckpointMetadata::new(
        Checkpoint {
            checkpoint_id: "ckpt-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Ready,
            created_at: 10,
            compatibility_fingerprint: "fp-1".to_string(),
        },
        compatibility.clone(),
    );

    validate_checkpoint_restore_compatibility(
        &metadata,
        "fp-1",
        Some(&compatibility),
        CheckpointClass::FsQuick,
        false,
    )
    .unwrap();
}

#[test]
fn validate_checkpoint_restore_compatibility_rejects_mismatch() {
    let metadata = CheckpointMetadata::new(
        Checkpoint {
            checkpoint_id: "ckpt-2".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::VmFull,
            state: CheckpointState::Ready,
            created_at: 11,
            compatibility_fingerprint: "fp-actual".to_string(),
        },
        CheckpointCompatibilityMetadata {
            backend_id: "linux-native".to_string(),
            backend_version: "0.1.0".to_string(),
            runtime_version: "2".to_string(),
            guest_artifact_versions: BTreeMap::new(),
            config_hash: "sha256:cfg-a".to_string(),
            host_compatibility_markers: BTreeMap::new(),
        },
    );

    let err = validate_checkpoint_restore_compatibility(
        &metadata,
        "fp-expected",
        Some(&CheckpointCompatibilityMetadata {
            backend_id: "macos-vz".to_string(),
            backend_version: "0.1.0".to_string(),
            runtime_version: "2".to_string(),
            guest_artifact_versions: BTreeMap::new(),
            config_hash: "sha256:cfg-b".to_string(),
            host_compatibility_markers: BTreeMap::new(),
        }),
        CheckpointClass::VmFull,
        false,
    )
    .unwrap_err();

    match err {
        RuntimeError::InvalidConfig(message) => {
            assert!(message.contains("compatibility fingerprint mismatch"));
        }
        other => panic!("expected invalid config error, got: {other:?}"),
    }

    let err = validate_checkpoint_restore_compatibility(
        &metadata,
        "fp-actual",
        Some(&CheckpointCompatibilityMetadata {
            backend_id: "macos-vz".to_string(),
            backend_version: "0.1.0".to_string(),
            runtime_version: "2".to_string(),
            guest_artifact_versions: BTreeMap::new(),
            config_hash: "sha256:cfg-b".to_string(),
            host_compatibility_markers: BTreeMap::new(),
        }),
        CheckpointClass::VmFull,
        false,
    )
    .unwrap_err();

    match err {
        RuntimeError::InvalidConfig(message) => {
            assert!(message.contains("incompatible for restore"));
            assert!(message.contains("backend_id"));
            assert!(message.contains("config_hash"));
        }
        other => panic!("expected invalid config error, got: {other:?}"),
    }
}

#[test]
fn validate_checkpoint_restore_compatibility_rejects_class_degradation_without_ack() {
    let metadata = CheckpointMetadata::new(
        Checkpoint {
            checkpoint_id: "ckpt-3".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Ready,
            created_at: 12,
            compatibility_fingerprint: "fp-3".to_string(),
        },
        CheckpointCompatibilityMetadata {
            backend_id: "macos-vz".to_string(),
            backend_version: "0.1.0".to_string(),
            runtime_version: "2".to_string(),
            guest_artifact_versions: BTreeMap::new(),
            config_hash: "sha256:cfg".to_string(),
            host_compatibility_markers: BTreeMap::new(),
        },
    );

    let err = validate_checkpoint_restore_compatibility(
        &metadata,
        "fp-3",
        Some(&metadata.compatibility),
        CheckpointClass::VmFull,
        false,
    )
    .unwrap_err();
    match err {
        RuntimeError::UnsupportedOperation { operation, reason } => {
            assert_eq!(operation, RuntimeOperation::RestoreCheckpoint.as_str());
            assert!(reason.contains("degradation"));
            assert!(reason.contains("allow_class_degradation=true"));
        }
        other => panic!("expected unsupported operation error, got: {other:?}"),
    }
}

#[test]
fn validate_checkpoint_restore_compatibility_allows_class_degradation_with_ack() {
    let metadata = CheckpointMetadata::new(
        Checkpoint {
            checkpoint_id: "ckpt-4".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Ready,
            created_at: 13,
            compatibility_fingerprint: "fp-4".to_string(),
        },
        CheckpointCompatibilityMetadata {
            backend_id: "macos-vz".to_string(),
            backend_version: "0.1.0".to_string(),
            runtime_version: "2".to_string(),
            guest_artifact_versions: BTreeMap::new(),
            config_hash: "sha256:cfg".to_string(),
            host_compatibility_markers: BTreeMap::new(),
        },
    );

    validate_checkpoint_restore_compatibility(
        &metadata,
        "fp-4",
        Some(&metadata.compatibility),
        CheckpointClass::VmFull,
        true,
    )
    .unwrap();
}

#[test]
fn ensure_checkpoint_class_supported_rejects_missing_vm_full_capability() {
    let mut capabilities = RuntimeCapabilities::stack_baseline();
    capabilities.fs_quick_checkpoint = true;
    capabilities.vm_full_checkpoint = false;

    let err = ensure_checkpoint_class_supported(
        capabilities,
        CheckpointClass::VmFull,
        RuntimeOperation::CreateCheckpoint,
    )
    .unwrap_err();

    match err {
        RuntimeError::UnsupportedOperation { operation, reason } => {
            assert_eq!(operation, RuntimeOperation::CreateCheckpoint.as_str());
            assert!(reason.contains("vm_full_checkpoint"));
        }
        other => panic!("expected unsupported operation error, got: {other:?}"),
    }
}

#[test]
fn ensure_checkpoint_class_supported_allows_enabled_class_capability() {
    let mut capabilities = RuntimeCapabilities::stack_baseline();
    capabilities.fs_quick_checkpoint = true;

    ensure_checkpoint_class_supported(
        capabilities,
        CheckpointClass::FsQuick,
        RuntimeOperation::RestoreCheckpoint,
    )
    .unwrap();
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
fn primitive_conformance_matrix_is_complete_and_stable() {
    assert_eq!(
        REQUIRED_RUNTIME_OPERATIONS.len(),
        PRIMITIVE_CONFORMANCE_MATRIX.len()
    );

    let mut by_operation = HashSet::new();
    for entry in PRIMITIVE_CONFORMANCE_MATRIX {
        assert!(
            by_operation.insert(entry.operation),
            "duplicate conformance entry for operation `{}`",
            entry.operation.as_str()
        );
        assert!(
            REQUIRED_RUNTIME_OPERATIONS.contains(&entry.operation),
            "coverage entry uses non-required operation `{}`",
            entry.operation.as_str()
        );
        if let Some(surface) = entry.openapi {
            assert!(surface.path.starts_with('/'));
            assert!(!surface.surface.is_empty());
        }
    }

    for operation in REQUIRED_RUNTIME_OPERATIONS {
        assert!(
            by_operation.contains(operation),
            "missing conformance entry for operation `{}`",
            operation.as_str()
        );
    }

    let mut required_paths = BTreeSet::new();
    let mut covered_by_matrix = 0usize;
    for entry in PRIMITIVE_CONFORMANCE_MATRIX {
        if let Some(surface) = entry.openapi {
            covered_by_matrix += 1;
            required_paths.insert(surface.path);
        }
        // Rows without an explicit external claim represent transport-op gaps tracked
        // in the Runtime V2 readiness plan.
    }

    assert!(covered_by_matrix > 0);
    assert!(required_paths.len() >= 3);
}

#[test]
fn primitive_conformance_matrix_idempotency_and_metadata_are_consistent() {
    for entry in PRIMITIVE_CONFORMANCE_MATRIX {
        assert_eq!(
            entry.operation.requires_idempotency_key(),
            entry.operation.idempotency_key_prefix().is_some()
        );

        let (request_id, expected_key) = transport_metadata_for_sequence(0, Some(entry.operation));
        assert_eq!(request_id, "req_0000000000000001");

        let expected_prefix = entry
            .operation
            .idempotency_key_prefix()
            .map(|prefix| format!("{prefix}:{request_id}"));
        assert_eq!(expected_key, expected_prefix);
    }
}

fn expected_manager_surface_operations() -> HashSet<RuntimeOperation> {
    [
        RuntimeOperation::CreateSandbox,
        RuntimeOperation::GetSandbox,
        RuntimeOperation::TerminateSandbox,
        RuntimeOperation::PullImage,
        RuntimeOperation::CreateContainer,
        RuntimeOperation::ExecContainer,
        RuntimeOperation::StopContainer,
        RuntimeOperation::RemoveContainer,
        RuntimeOperation::GetContainerLogs,
        RuntimeOperation::WriteExecStdin,
        RuntimeOperation::SignalExec,
        RuntimeOperation::ResizeExecPty,
        RuntimeOperation::CancelExec,
        RuntimeOperation::CreateCheckpoint,
        RuntimeOperation::RestoreCheckpoint,
        RuntimeOperation::ForkCheckpoint,
        RuntimeOperation::CreateVolume,
        RuntimeOperation::AttachVolume,
        RuntimeOperation::DetachVolume,
        RuntimeOperation::CreateNetworkDomain,
        RuntimeOperation::PublishPort,
        RuntimeOperation::ConnectContainer,
    ]
    .into_iter()
    .collect()
}

#[test]
fn primitive_conformance_matrix_manager_rows_match_surface_contract() {
    let expected = expected_manager_surface_operations();
    let actual = PRIMITIVE_CONFORMANCE_MATRIX
        .iter()
        .filter(|entry| entry.manager)
        .map(|entry| entry.operation)
        .collect::<HashSet<_>>();

    assert_eq!(expected, actual);
}

#[test]
fn workspace_runtime_manager_routes_claimed_parity_operations() {
    let backend = ManagerRoutingBackend::new(RuntimeCapabilities::stack_baseline());
    let manager = WorkspaceRuntimeManager::new(backend);

    let _ = poll_immediate(manager.pull_image("alpine:latest")).unwrap();
    let _ =
        poll_immediate(manager.create_container("alpine:latest", RunConfig::default())).unwrap();
    let _ = poll_immediate(manager.exec_container("container-1", ExecConfig::default())).unwrap();
    let _ = poll_immediate(manager.stop_container("container-1", false, None, None));
    let _ = poll_immediate(manager.remove_container("container-1")).unwrap();
    manager.container_logs("container-1").unwrap();

    assert_eq!(
        manager.backend().calls(),
        [
            "pull",
            "create_container",
            "exec_container",
            "stop_container",
            "remove_container",
            "logs"
        ]
    );
}

#[test]
fn docker_shim_v1_command_mapping_is_stable() {
    assert_eq!(DockerShimCommand::V1_ALL.len(), 8);
    assert_eq!(DockerShimCommand::Run.as_str(), "run");
    assert_eq!(
        DockerShimCommand::Run.runtime_operation(),
        Some(RuntimeOperation::CreateContainer)
    );
    assert_eq!(
        DockerShimCommand::Exec.runtime_operation(),
        Some(RuntimeOperation::ExecContainer)
    );
    assert_eq!(DockerShimCommand::Ps.runtime_operation(), None);
    assert_eq!(
        DockerShimCommand::Logs.runtime_operation(),
        Some(RuntimeOperation::GetContainerLogs)
    );
    assert_eq!(
        DockerShimCommand::Pull.runtime_operation(),
        Some(RuntimeOperation::PullImage)
    );
    assert_eq!(
        DockerShimCommand::Build.runtime_operation(),
        Some(RuntimeOperation::StartBuild)
    );
    assert_eq!(
        DockerShimCommand::Stop.runtime_operation(),
        Some(RuntimeOperation::StopContainer)
    );
    assert_eq!(
        DockerShimCommand::Rm.runtime_operation(),
        Some(RuntimeOperation::RemoveContainer)
    );
}

#[test]
fn required_backend_adapter_operations_are_subset_of_runtime_surface() {
    assert!(!REQUIRED_BACKEND_ADAPTER_OPERATIONS.is_empty());
    for operation in REQUIRED_BACKEND_ADAPTER_OPERATIONS {
        assert!(REQUIRED_RUNTIME_OPERATIONS.contains(operation));
    }
    assert!(REQUIRED_BACKEND_ADAPTER_OPERATIONS.contains(&RuntimeOperation::CreateSandbox));
    assert!(REQUIRED_BACKEND_ADAPTER_OPERATIONS.contains(&RuntimeOperation::ExecContainer));
    assert!(REQUIRED_BACKEND_ADAPTER_OPERATIONS.contains(&RuntimeOperation::GetCapabilities));
}

#[test]
fn canonical_backend_capabilities_share_same_matrix_shape() {
    let macos = canonical_backend_capabilities(&SandboxBackend::MacosVz);
    let linux = canonical_backend_capabilities(&SandboxBackend::LinuxFirecracker);
    assert_eq!(
        backend_capability_matrix(macos),
        backend_capability_matrix(linux)
    );

    let matrix = backend_capability_matrix(macos);
    assert!(matrix.fs_quick_checkpoint);
    assert!(!matrix.vm_full_checkpoint);
    assert!(matrix.checkpoint_fork);
    assert!(!matrix.docker_compat);
    assert!(matrix.compose_adapter);
    assert!(!matrix.gpu_passthrough);
    assert!(!matrix.live_resize);
    assert_eq!(
        BackendCapabilityMatrix::FIELD_NAMES,
        [
            "fs_quick_checkpoint",
            "vm_full_checkpoint",
            "checkpoint_fork",
            "docker_compat",
            "compose_adapter",
            "gpu_passthrough",
            "live_resize",
        ]
    );
}

#[test]
fn backend_adapter_contract_surface_has_valid_idempotency_mapping() {
    validate_backend_adapter_contract_surface().unwrap();
}

#[test]
fn backend_adapter_parity_validates_required_capability_baseline() {
    let capabilities = canonical_backend_capabilities(&SandboxBackend::MacosVz);
    validate_backend_adapter_parity(capabilities).unwrap();

    let mut missing_checkpoint = capabilities;
    missing_checkpoint.fs_quick_checkpoint = false;
    let err = validate_backend_adapter_parity(missing_checkpoint).unwrap_err();
    match err {
        RuntimeError::UnsupportedOperation { operation, reason } => {
            assert_eq!(operation, RuntimeOperation::CreateCheckpoint.as_str());
            assert!(reason.contains("fs_quick_checkpoint"));
        }
        other => panic!("expected unsupported operation error, got: {other:?}"),
    }

    let mut missing_network = capabilities;
    missing_network.stack_networking = false;
    let err = validate_backend_adapter_parity(missing_network).unwrap_err();
    match err {
        RuntimeError::UnsupportedOperation { operation, reason } => {
            assert_eq!(operation, RuntimeOperation::CreateNetworkDomain.as_str());
            assert!(reason.contains("stack_networking"));
        }
        other => panic!("expected unsupported operation error, got: {other:?}"),
    }
}

#[test]
fn request_metadata_validation_enforces_required_idempotency_keys() {
    let metadata =
        RequestMetadata::from_optional_refs(Some(" req-1 "), Some(" create_container:abc "))
            .with_trace_id(Some(" trace-7 ".to_string()))
            .with_passthrough(
                RuntimeOperation::CreateContainer,
                BTreeMap::from([(" customer ".to_string(), " west ".to_string())]),
            )
            .unwrap();
    assert_eq!(metadata.request_id.as_deref(), Some("req-1"));
    assert_eq!(
        metadata.idempotency_key.as_deref(),
        Some("create_container:abc")
    );
    assert_eq!(metadata.trace_id.as_deref(), Some("trace-7"));
    assert_eq!(
        metadata.passthrough.get("customer").map(String::as_str),
        Some("west")
    );

    validate_request_metadata_for_operation(RuntimeOperation::CreateContainer, &metadata).unwrap();
    validate_request_metadata_for_operation(RuntimeOperation::GetReceipt, &metadata).unwrap();

    let missing = RequestMetadata::default();
    let err = validate_request_metadata_for_operation(RuntimeOperation::CreateContainer, &missing)
        .unwrap_err();
    assert!(matches!(err, RuntimeError::InvalidConfig(_)));
    assert!(err.to_string().contains("create_container"));
}

#[test]
fn metadata_passthrough_rejects_reserved_keys() {
    let err = RequestMetadata::default()
        .with_passthrough(
            RuntimeOperation::CreateContainer,
            BTreeMap::from([("vz.internal".to_string(), "1".to_string())]),
        )
        .unwrap_err();

    assert!(matches!(err, RuntimeError::InvalidConfig(_)));
    assert!(err.to_string().contains("metadata_passthrough"));
    assert!(err.to_string().contains("reserved `vz.` prefix"));
}

#[test]
fn runtime_extension_failure_mapping_is_stable() {
    let denied = map_runtime_extension_failure(
        RuntimeExtensionPoint::PolicyHook,
        RuntimeOperation::CreateContainer.as_str(),
        RuntimeExtensionFailureKind::PolicyDenied,
        "no quota",
    );
    assert_eq!(denied.machine_code(), MachineErrorCode::PolicyDenied);
    assert!(denied.to_string().contains("extension=policy_hook"));

    let transport = map_runtime_extension_failure(
        RuntimeExtensionPoint::EventSink,
        "stack.emit_event",
        RuntimeExtensionFailureKind::Transport,
        "sink closed",
    );
    assert_eq!(
        transport.machine_code(),
        MachineErrorCode::BackendUnavailable
    );
    assert!(transport.to_string().contains("extension_failure:"));
    assert!(transport.to_string().contains("extension=event_sink"));
    assert!(transport.to_string().contains("operation=stack.emit_event"));

    let invalid = map_runtime_extension_failure(
        RuntimeExtensionPoint::MetadataPassthrough,
        RuntimeOperation::CreateContainer.as_str(),
        RuntimeExtensionFailureKind::InvalidMetadata,
        "key cannot be empty",
    );
    assert_eq!(invalid.machine_code(), MachineErrorCode::ValidationError);
    assert!(invalid.to_string().contains("kind=invalid_metadata"));
}

#[test]
fn runtime_policy_hook_maps_allow_deny_and_transport_errors() {
    let metadata = RequestMetadata::from_optional_refs(Some("req-7"), None);

    let allow_hook = StubPolicyHook {
        mode: StubPolicyMode::Allow,
    };
    enforce_runtime_policy_hook(&allow_hook, RuntimeOperation::CreateContainer, &metadata).unwrap();

    let deny_hook = StubPolicyHook {
        mode: StubPolicyMode::Deny,
    };
    let deny =
        enforce_runtime_policy_hook(&deny_hook, RuntimeOperation::CreateContainer, &metadata)
            .unwrap_err();
    assert_eq!(deny.machine_code(), MachineErrorCode::PolicyDenied);
    assert!(deny.to_string().contains("blocked by test policy"));

    let fail_hook = StubPolicyHook {
        mode: StubPolicyMode::Fail,
    };
    let transport =
        enforce_runtime_policy_hook(&fail_hook, RuntimeOperation::CreateContainer, &metadata)
            .unwrap_err();
    assert_eq!(
        transport.machine_code(),
        MachineErrorCode::BackendUnavailable
    );
    assert!(transport.to_string().contains("operation=create_container"));
}

#[test]
fn runtime_error_machine_envelope_carries_request_id_and_details() {
    let metadata = RequestMetadata::from_optional_refs(Some("req_123"), None);
    let error = RuntimeError::UnsupportedOperation {
        operation: "restore_checkpoint".to_string(),
        reason: "missing vm_full_checkpoint capability".to_string(),
    };

    let envelope = runtime_error_machine_envelope(&error, &metadata);
    assert_eq!(envelope.error.code, MachineErrorCode::UnsupportedOperation);
    assert_eq!(envelope.error.request_id.as_deref(), Some("req_123"));
    assert_eq!(
        envelope.error.details.get("operation").map(String::as_str),
        Some("restore_checkpoint")
    );
    assert_eq!(
        envelope.error.details.get("reason").map(String::as_str),
        Some("missing vm_full_checkpoint capability")
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
        RuntimeError::PolicyDenied {
            operation: "create_container".to_string(),
            reason: "extension=policy_hook; reason=test".to_string(),
        }
        .machine_code(),
        MachineErrorCode::PolicyDenied
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
fn runtime_surface_forbids_product_domain_primitives() {
    const FORBIDDEN: [&str; 5] = [
        "identity_provider",
        "memory_provider",
        "tool_gateway",
        "mission",
        "workflow",
    ];

    let mut labels = Vec::new();
    labels.extend(RuntimeOperation::ALL.map(RuntimeOperation::as_str));
    labels.extend(MachineErrorCode::ALL.map(MachineErrorCode::as_str));
    labels.extend(RuntimeExtensionPoint::ALL.map(RuntimeExtensionPoint::as_str));
    labels.extend(DockerShimCommand::V1_ALL.map(DockerShimCommand::as_str));

    for label in labels {
        let normalized = label.to_ascii_lowercase();
        for forbidden in FORBIDDEN {
            assert!(
                !normalized.contains(forbidden),
                "runtime surface label `{label}` must not contain forbidden primitive `{forbidden}`"
            );
        }
    }
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

// ── Backend conformance test suite ──────────────────────────────

#[test]
fn default_execution_control_operations_return_unsupported() {
    let backend = StubBackend;

    let write_err = poll_immediate(backend.write_exec_stdin("exec-1", b"hello")).unwrap_err();
    let signal_err = poll_immediate(backend.signal_exec("exec-1", "SIGTERM")).unwrap_err();
    let resize_err = poll_immediate(backend.resize_exec_pty("exec-1", 80, 24)).unwrap_err();
    let cancel_err = poll_immediate(backend.cancel_exec("exec-1")).unwrap_err();

    for (error, expected_op) in [
        (write_err, RuntimeOperation::WriteExecStdin.as_str()),
        (signal_err, RuntimeOperation::SignalExec.as_str()),
        (resize_err, RuntimeOperation::ResizeExecPty.as_str()),
        (cancel_err, RuntimeOperation::CancelExec.as_str()),
    ] {
        match error {
            RuntimeError::UnsupportedOperation { operation, .. } => {
                assert_eq!(operation, expected_op);
            }
            other => panic!("expected unsupported operation error, got: {other:?}"),
        }
    }
}

#[test]
fn default_checkpoint_operations_return_unsupported() {
    let backend = StubBackend;

    let create_err =
        poll_immediate(backend.create_checkpoint("sbx-1", "fs_quick", "fp-1")).unwrap_err();
    let restore_err = poll_immediate(backend.restore_checkpoint("ckpt-1")).unwrap_err();
    let fork_err = poll_immediate(backend.fork_checkpoint("ckpt-1", "sbx-2")).unwrap_err();

    for (error, expected_op) in [
        (create_err, RuntimeOperation::CreateCheckpoint.as_str()),
        (restore_err, RuntimeOperation::RestoreCheckpoint.as_str()),
        (fork_err, RuntimeOperation::ForkCheckpoint.as_str()),
    ] {
        match error {
            RuntimeError::UnsupportedOperation { operation, .. } => {
                assert_eq!(operation, expected_op);
            }
            other => panic!("expected unsupported operation error, got: {other:?}"),
        }
    }
}

#[test]
fn default_volume_operations_return_unsupported() {
    let backend = StubBackend;

    let create_err = poll_immediate(backend.create_volume("vol-1")).unwrap_err();
    let attach_err = poll_immediate(backend.attach_volume("ctr-1", "vol-1", "/data")).unwrap_err();
    let detach_err = poll_immediate(backend.detach_volume("ctr-1", "vol-1")).unwrap_err();

    for (error, expected_op) in [
        (create_err, RuntimeOperation::CreateVolume.as_str()),
        (attach_err, RuntimeOperation::AttachVolume.as_str()),
        (detach_err, RuntimeOperation::DetachVolume.as_str()),
    ] {
        match error {
            RuntimeError::UnsupportedOperation { operation, .. } => {
                assert_eq!(operation, expected_op);
            }
            other => panic!("expected unsupported operation error, got: {other:?}"),
        }
    }
}

#[test]
fn default_network_domain_operations_return_unsupported() {
    let backend = StubBackend;

    let create_err = poll_immediate(backend.create_network_domain("net-1", "sbx-1")).unwrap_err();
    let destroy_err = poll_immediate(backend.destroy_network_domain("net-1")).unwrap_err();
    let publish_err = poll_immediate(backend.publish_port("net-1", 8080, 80, "tcp")).unwrap_err();
    let connect_err =
        poll_immediate(backend.connect_container_to_network("ctr-1", "net-1")).unwrap_err();

    for (error, expected_op) in [
        (create_err, RuntimeOperation::CreateNetworkDomain.as_str()),
        (destroy_err, "destroy_network_domain"),
        (publish_err, RuntimeOperation::PublishPort.as_str()),
        (connect_err, RuntimeOperation::ConnectContainer.as_str()),
    ] {
        match error {
            RuntimeError::UnsupportedOperation { operation, .. } => {
                assert_eq!(operation, expected_op);
            }
            other => panic!("expected unsupported operation error, got: {other:?}"),
        }
    }
}

#[test]
fn manager_passthrough_for_execution_control_operations() {
    let backend = StubBackend;
    let manager = WorkspaceRuntimeManager::new(backend);

    let write_err = poll_immediate(manager.write_exec_stdin("exec-1", b"data")).unwrap_err();
    assert!(matches!(
        write_err,
        RuntimeError::UnsupportedOperation { .. }
    ));

    let signal_err = poll_immediate(manager.signal_exec("exec-1", "SIGINT")).unwrap_err();
    assert!(matches!(
        signal_err,
        RuntimeError::UnsupportedOperation { .. }
    ));

    let resize_err = poll_immediate(manager.resize_exec_pty("exec-1", 120, 40)).unwrap_err();
    assert!(matches!(
        resize_err,
        RuntimeError::UnsupportedOperation { .. }
    ));

    let cancel_err = poll_immediate(manager.cancel_exec("exec-1")).unwrap_err();
    assert!(matches!(
        cancel_err,
        RuntimeError::UnsupportedOperation { .. }
    ));
}

#[test]
fn manager_passthrough_for_checkpoint_operations() {
    let backend = StubBackend;
    let manager = WorkspaceRuntimeManager::new(backend);

    let create_err =
        poll_immediate(manager.create_checkpoint("sbx-1", "fs_quick", "fp-1")).unwrap_err();
    assert!(matches!(
        create_err,
        RuntimeError::UnsupportedOperation { .. }
    ));

    let restore_err = poll_immediate(manager.restore_checkpoint("ckpt-1")).unwrap_err();
    assert!(matches!(
        restore_err,
        RuntimeError::UnsupportedOperation { .. }
    ));

    let fork_err = poll_immediate(manager.fork_checkpoint("ckpt-1", "sbx-2")).unwrap_err();
    assert!(matches!(
        fork_err,
        RuntimeError::UnsupportedOperation { .. }
    ));
}

#[test]
fn manager_passthrough_for_volume_operations() {
    let backend = StubBackend;
    let manager = WorkspaceRuntimeManager::new(backend);

    let create_err = poll_immediate(manager.create_volume("vol-1")).unwrap_err();
    assert!(matches!(
        create_err,
        RuntimeError::UnsupportedOperation { .. }
    ));

    let attach_err = poll_immediate(manager.attach_volume("ctr-1", "vol-1", "/data")).unwrap_err();
    assert!(matches!(
        attach_err,
        RuntimeError::UnsupportedOperation { .. }
    ));

    let detach_err = poll_immediate(manager.detach_volume("ctr-1", "vol-1")).unwrap_err();
    assert!(matches!(
        detach_err,
        RuntimeError::UnsupportedOperation { .. }
    ));
}

#[test]
fn manager_passthrough_for_network_domain_operations() {
    let backend = StubBackend;
    let manager = WorkspaceRuntimeManager::new(backend);

    let create_err = poll_immediate(manager.create_network_domain("net-1", "sbx-1")).unwrap_err();
    assert!(matches!(
        create_err,
        RuntimeError::UnsupportedOperation { .. }
    ));

    let destroy_err = poll_immediate(manager.destroy_network_domain("net-1")).unwrap_err();
    assert!(matches!(
        destroy_err,
        RuntimeError::UnsupportedOperation { .. }
    ));

    let publish_err = poll_immediate(manager.publish_port("net-1", 8080, 80, "tcp")).unwrap_err();
    assert!(matches!(
        publish_err,
        RuntimeError::UnsupportedOperation { .. }
    ));

    let connect_err =
        poll_immediate(manager.connect_container_to_network("ctr-1", "net-1")).unwrap_err();
    assert!(matches!(
        connect_err,
        RuntimeError::UnsupportedOperation { .. }
    ));
}

#[test]
fn conformance_all_new_backend_trait_default_stubs_return_unsupported_machine_code() {
    let backend = StubBackend;

    let errors: Vec<RuntimeError> = vec![
        poll_immediate(backend.write_exec_stdin("e", b"")).unwrap_err(),
        poll_immediate(backend.signal_exec("e", "SIGTERM")).unwrap_err(),
        poll_immediate(backend.resize_exec_pty("e", 80, 24)).unwrap_err(),
        poll_immediate(backend.cancel_exec("e")).unwrap_err(),
        poll_immediate(backend.create_checkpoint("s", "c", "f")).unwrap_err(),
        poll_immediate(backend.restore_checkpoint("c")).unwrap_err(),
        poll_immediate(backend.fork_checkpoint("c", "s2")).unwrap_err(),
        poll_immediate(backend.create_volume("v")).unwrap_err(),
        poll_immediate(backend.attach_volume("c", "v", "/m")).unwrap_err(),
        poll_immediate(backend.detach_volume("c", "v")).unwrap_err(),
        poll_immediate(backend.create_network_domain("n", "s")).unwrap_err(),
        poll_immediate(backend.destroy_network_domain("n")).unwrap_err(),
        poll_immediate(backend.publish_port("n", 80, 80, "tcp")).unwrap_err(),
        poll_immediate(backend.connect_container_to_network("c", "n")).unwrap_err(),
    ];

    for error in &errors {
        assert_eq!(
            error.machine_code(),
            MachineErrorCode::UnsupportedOperation,
            "expected UnsupportedOperation machine code for error: {error}"
        );
    }
}

// ── IsolationLevel tests ────────────────────────────────────────

#[test]
fn isolation_level_default_is_full() {
    assert_eq!(IsolationLevel::default(), IsolationLevel::Full);
}

#[test]
fn isolation_level_labels() {
    assert_eq!(IsolationLevel::Full.label(), "full");
    assert_eq!(IsolationLevel::Container.label(), "container");
    assert_eq!(IsolationLevel::Namespace.label(), "namespace");
    assert_eq!(IsolationLevel::None.label(), "none");
}

#[test]
fn isolation_level_display() {
    assert_eq!(format!("{}", IsolationLevel::Full), "full");
    assert_eq!(format!("{}", IsolationLevel::Container), "container");
    assert_eq!(format!("{}", IsolationLevel::Namespace), "namespace");
    assert_eq!(format!("{}", IsolationLevel::None), "none");
}

#[test]
fn isolation_level_hierarchy_full() {
    let level = IsolationLevel::Full;
    assert!(level.has_vm_isolation());
    assert!(level.has_container_isolation());
    assert!(level.has_namespace_isolation());
}

#[test]
fn isolation_level_hierarchy_container() {
    let level = IsolationLevel::Container;
    assert!(!level.has_vm_isolation());
    assert!(level.has_container_isolation());
    assert!(level.has_namespace_isolation());
}

#[test]
fn isolation_level_hierarchy_namespace() {
    let level = IsolationLevel::Namespace;
    assert!(!level.has_vm_isolation());
    assert!(!level.has_container_isolation());
    assert!(level.has_namespace_isolation());
}

#[test]
fn isolation_level_hierarchy_none() {
    let level = IsolationLevel::None;
    assert!(!level.has_vm_isolation());
    assert!(!level.has_container_isolation());
    assert!(!level.has_namespace_isolation());
}

#[test]
fn isolation_level_round_trip() {
    for level in [
        IsolationLevel::Full,
        IsolationLevel::Container,
        IsolationLevel::Namespace,
        IsolationLevel::None,
    ] {
        let json = serde_json::to_string(&level).unwrap();
        let deserialized: IsolationLevel = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, level);
    }
}

// ── NamespaceConfig tests ───────────────────────────────────────

#[test]
fn namespace_config_default_sensible() {
    let config = NamespaceConfig::default();
    // Default: PID, mount, IPC, UTS enabled; user and net disabled.
    assert!(!config.user);
    assert!(!config.net);
    assert!(config.pid);
    assert!(config.mnt);
    assert!(config.ipc);
    assert!(config.uts);
}

#[test]
fn namespace_config_default_matches_free_fn() {
    assert_eq!(NamespaceConfig::default(), default_namespace_config());
}

#[test]
fn namespace_config_all() {
    let config = NamespaceConfig::ALL;
    assert!(config.user);
    assert!(config.net);
    assert!(config.pid);
    assert!(config.mnt);
    assert!(config.ipc);
    assert!(config.uts);
    assert_eq!(config.enabled_count(), 6);
}

#[test]
fn namespace_config_none() {
    let config = NamespaceConfig::NONE;
    assert!(!config.user);
    assert!(!config.net);
    assert!(!config.pid);
    assert!(!config.mnt);
    assert!(!config.ipc);
    assert!(!config.uts);
    assert_eq!(config.enabled_count(), 0);
}

#[test]
fn namespace_config_enabled_count() {
    let config = default_namespace_config();
    // pid + mnt + ipc + uts = 4
    assert_eq!(config.enabled_count(), 4);
}

#[test]
fn namespace_config_round_trip() {
    let config = NamespaceConfig {
        user: true,
        net: true,
        pid: true,
        mnt: true,
        ipc: false,
        uts: false,
    };
    let json = serde_json::to_string(&config).unwrap();
    let deserialized: NamespaceConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, config);
}

#[test]
fn stub_backend_default_isolation_level_is_full() {
    let backend = StubBackend;
    assert_eq!(backend.isolation_level(), IsolationLevel::Full);
}
