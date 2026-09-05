use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, process};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;
use tracing::{debug, warn};
use vz::Vm;
use vz::protocol::{ExecEvent, ExecOutput};
use vz::{DiskConfig, NetworkConfig, SharedDirConfig};
use vz_image::{
    ImageConfigSummary, ImageId, ImagePuller, ImageStore, parse_image_config_summary_from_store,
};
use vz_linux::{
    ContainerExecDispatchGate, ExecOptions, KernelPaths, LinuxError, LinuxVm, LinuxVmConfig,
    OciExecOptions,
};
use vz_oci::bundle::{BundleMount, BundleSpec, write_oci_bundle};
use vz_oci::container_store::{
    ContainerGeneration, ContainerGenerationDiagnostic, ContainerIdLease, ContainerInfo,
    ContainerStatus, ContainerStore, ScopedGenerationInspection,
};

use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};
use vz::protocol::OciContainerState;

use crate::config::{
    ExecConfig, ExecutionMode, KernelProfile, MountAccess, MountSpec, MountType, OciRuntimeKind,
    PortMapping, PortProtocol, RunConfig, RuntimeBackend, RuntimeConfig, ensure_kernel_for_config,
};
use crate::error::MacosOciError as OciError;
use vz_image::{ImageInfo, PruneResult};

mod bundle;
mod exec;
mod networking;
mod oci_lifecycle;
mod resolve;
mod run_rootfs;
mod stack_vm;
#[cfg(test)]
mod tests;

pub use self::bundle::container_log_dir;
use self::bundle::expand_home_dir;
use self::networking::PortForwarding;
use self::networking::{shutdown_port_forwarding_registry_entry, stop_or_reuse_exit_code};
#[cfg(test)]
use self::networking::{stop_via_oci_runtime, test_port_forwarding};
use self::oci_lifecycle::LogRotationTask;
use self::resolve::{
    current_unix_secs, new_container_id, resolve_container_lifecycle, resolve_run_config,
};

#[cfg(test)]
use self::bundle::{
    make_oci_runtime_share, mount_specs_to_bundle_mounts, mount_specs_to_shared_dirs,
    oci_bundle_guest_path, oci_bundle_guest_root, oci_bundle_host_dir,
    resolve_oci_runtime_binary_path, write_hosts_file,
};
#[cfg(test)]
use self::exec::{
    ExecStartInterruption, await_exec_start, container_ready_generation,
    resolve_container_exec_binding, resolve_container_exec_options,
};
#[cfg(test)]
use self::oci_lifecycle::{
    OciLifecycleFuture, OciLifecycleOps, build_log_rotation_script, lifecycle_exec_options,
    parse_signal_number, run_oci_lifecycle,
};
#[cfg(test)]
use self::resolve::parse_compose_log_rotation;

const STOP_GRACE_PERIOD: Duration = Duration::from_secs(10);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(100);
const LOG_ROTATION_POLL_INTERVAL: Duration = Duration::from_secs(1);
const LOG_ROTATION_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_INTERACTIVE_EXEC_ROWS: u16 = 24;
const DEFAULT_INTERACTIVE_EXEC_COLS: u16 = 80;
const INTERACTIVE_EXEC_PTY_PREP_TIMEOUT: Duration = Duration::from_secs(2);
const OCI_RUNTIME_BIN_SHARE_TAG: &str = "oci-runtime-bin";
const OCI_DEFAULT_GUEST_STATE_DIR: &str = "/run/vz-oci";
const OCI_BUNDLE_DIRNAME: &str = "bundles";
const OCI_ANNOTATION_CONTAINER_CLASS: &str = "io.vz.container.class";
const OCI_ANNOTATION_AUTO_REMOVE: &str = "io.vz.container.auto_remove";
const OCI_ANNOTATION_COMPOSE_LOGGING_DRIVER: &str = "io.vz.compose.logging.driver";
const OCI_ANNOTATION_COMPOSE_LOGGING_OPTIONS: &str = "io.vz.compose.logging.options";
const MAX_CONTAINER_ID_BYTES: usize = 128;

fn validate_container_id(container_id: &str) -> Result<(), OciError> {
    if container_id.is_empty() || container_id.len() > MAX_CONTAINER_ID_BYTES {
        return Err(OciError::InvalidConfig(format!(
            "container ID must contain 1..={MAX_CONTAINER_ID_BYTES} bytes"
        )));
    }
    if !container_id
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        || !container_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(OciError::InvalidConfig(
            "container ID must start with an ASCII alphanumeric byte and contain only ASCII alphanumeric, '_', '-', or '.' bytes"
                .to_string(),
        ));
    }
    Ok(())
}

fn contract_inspection(
    container_id: &str,
    scope: &vz_runtime_contract::ContainerGenerationScope,
    inspection: ScopedGenerationInspection,
) -> vz_runtime_contract::ContainerGenerationInspection {
    let ownership =
        |generation: ContainerGeneration| vz_runtime_contract::ContainerGenerationOwnership {
            container_id: container_id.to_string(),
            generation: generation.0,
            stack_id: scope.stack_id.clone(),
            scope: Some(Box::new(scope.clone())),
        };
    match inspection {
        ScopedGenerationInspection::Absent => {
            vz_runtime_contract::ContainerGenerationInspection::Absent
        }
        ScopedGenerationInspection::ReservedUnpublished(generation) => {
            vz_runtime_contract::ContainerGenerationInspection::ReservedUnpublished(ownership(
                generation,
            ))
        }
        ScopedGenerationInspection::Published(generation) => {
            vz_runtime_contract::ContainerGenerationInspection::Published(ownership(generation))
        }
        ScopedGenerationInspection::Foreign => {
            vz_runtime_contract::ContainerGenerationInspection::Foreign
        }
        ScopedGenerationInspection::Replacement => {
            vz_runtime_contract::ContainerGenerationInspection::Replacement
        }
        ScopedGenerationInspection::LegacyUnscoped => {
            vz_runtime_contract::ContainerGenerationInspection::LegacyUnscoped
        }
        ScopedGenerationInspection::Malformed(reason) => {
            vz_runtime_contract::ContainerGenerationInspection::Malformed(reason)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContainerLifecycleClass {
    Workspace,
    Service,
    Ephemeral,
}

impl ContainerLifecycleClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Workspace => "workspace",
            Self::Service => "service",
            Self::Ephemeral => "ephemeral",
        }
    }
}

impl fmt::Display for ContainerLifecycleClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveContainerLifecycle {
    class: ContainerLifecycleClass,
    auto_remove: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SetupRestoreIdentity {
    generation: ContainerGeneration,
    commit_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ComposeLogRotation {
    max_size_bytes: u64,
    max_files: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InteractiveExecEvent {
    /// The guest proved the exact OCI target was pinned and crossed execve.
    ContainerReady(ContainerReadyGeneration),
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(i32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelObjectIdentity {
    pub device: u64,
    pub inode: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerNamespaceIdentity {
    pub mount: KernelObjectIdentity,
    pub network: KernelObjectIdentity,
    pub pid: KernelObjectIdentity,
    pub ipc: KernelObjectIdentity,
    pub uts: KernelObjectIdentity,
}

/// Host lifecycle generation paired with the full guest-observed identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerReadyGeneration {
    pub lifecycle_generation: u64,
    pub container_id: String,
    pub init_pid: u32,
    pub init_start_time: u64,
    pub cgroup_path: String,
    pub cgroup: KernelObjectIdentity,
    pub namespaces: ContainerNamespaceIdentity,
    pub root: KernelObjectIdentity,
}

/// Read-only lifecycle state used by deterministic leak/recovery diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLifecycleDiagnostics {
    /// Durable generation history and current reservation state.
    pub generations: Vec<ContainerGenerationDiagnostic>,
    /// Stable per-ID process-local admission slots.
    pub container_lock_slots: usize,
    /// Stable per-stack process-local admission slots.
    pub stack_lock_slots: usize,
    /// Published VM handles.
    pub vm_handles: usize,
    /// Sorted container IDs with published VM handles.
    pub vm_handle_ids: Vec<String>,
    /// Shared stack VMs retained by the runtime.
    pub stack_vms: usize,
    /// Sorted shared-stack IDs retained by the runtime.
    pub stack_vm_ids: Vec<String>,
    /// Container-to-stack recovery routes.
    pub container_routes: usize,
    /// Sorted `(container_id, stack_id)` recovery routes.
    pub container_route_pairs: Vec<(String, String)>,
    /// Shared-stack host port-forwarding registries.
    pub stack_port_forwards: usize,
    /// Sorted stack IDs with active host port-forwarding registries.
    pub stack_port_forward_ids: Vec<String>,
    /// Public exec bindings.
    pub exec_bindings: usize,
    /// Generation-scoped runtime cleanup records.
    pub active_lifecycles: usize,
    /// Active interactive PTY sessions.
    pub exec_sessions: usize,
    /// Generation-and-commit-scoped setup restore entries.
    pub setup_restore_entries: usize,
    /// Generations whose OCI state was deleted but guest overlay cleanup remains.
    pub overlay_cleanup_pending: usize,
    /// Rootfs directories currently present on disk.
    pub rootfs_directories: usize,
}

/// Generation-fenced proof that Docker Engine answered inside one shared VM.
///
/// `guest_socket_path` is meaningful only inside this exact Linux VM. It is
/// not a host Docker endpoint or context and must not be published as one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedVmDockerReadiness {
    pub runtime_identity: vz_runtime_contract::StackRuntimeIdentity,
    pub verified_profile: KernelProfile,
    pub guest_socket_path: String,
}

/// Integration-test lifecycle admission points.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeLifecycleAdmissionKind {
    CreateBeforeReservation,
    CreateAfterReservation,
    ExecBeforeGuestRpc,
    ExecGuestRpcReadyBeforeOwner,
    ExecGuestReady,
    StackRoutePublishedBeforeOverlay,
    StackOverlaySetupStarting,
    StopWriterRequested,
    StopWriterAcquired,
    RemoveWriterAcquired,
}

/// A paused lifecycle operation. Dropping or resuming the event releases it.
#[doc(hidden)]
#[derive(Debug)]
pub struct RuntimeLifecycleAdmissionEvent {
    kind: RuntimeLifecycleAdmissionKind,
    container_id: String,
    resume: tokio::sync::oneshot::Sender<()>,
}

impl RuntimeLifecycleAdmissionEvent {
    pub fn kind(&self) -> RuntimeLifecycleAdmissionKind {
        self.kind
    }

    pub fn container_id(&self) -> &str {
        &self.container_id
    }

    pub fn resume(self) {
        let _ = self.resume.send(());
    }
}

/// Receiver installed by integration tests to pause lifecycle admission points.
#[doc(hidden)]
pub type RuntimeLifecycleObserver =
    tokio::sync::mpsc::UnboundedReceiver<RuntimeLifecycleAdmissionEvent>;

#[derive(Clone)]
struct ContainerExecSession {
    vm: Arc<LinuxVm>,
    pty_enabled: bool,
    control: Arc<Mutex<()>>,
    state: Arc<Mutex<ContainerExecSessionState>>,
    start_cancel: Arc<tokio::sync::Notify>,
    terminal: Arc<tokio::sync::Notify>,
}

#[derive(Debug)]
enum ContainerExecSessionState {
    Starting {
        pending: PendingExecControls,
        dispatch_gate: Option<ContainerExecDispatchGate>,
    },
    Running {
        guest_exec_id: u64,
        cancel_requested: bool,
    },
    Finished,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct PendingExecControls {
    operations: Vec<PendingExecControl>,
    stdin_bytes: usize,
    cancel_requested: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum PendingExecControl {
    Signal(i32),
    Stdin(Vec<u8>),
    Resize { rows: u32, cols: u32 },
    Cancel,
}

/// Immutable process defaults captured from the fully resolved container
/// configuration. Ad-hoc exec requests resolve against this snapshot instead
/// of consulting mutable guest process state.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ContainerExecDefaults {
    env: Vec<(String, String)>,
    working_dir: Option<String>,
    user: Option<String>,
}

impl From<&RunConfig> for ContainerExecDefaults {
    fn from(run: &RunConfig) -> Self {
        Self {
            env: run.env.clone(),
            working_dir: run.working_dir.clone(),
            user: run.user.clone(),
        }
    }
}

/// Atomically couples one live VM generation to the immutable defaults that
/// were resolved for that same activation. Public container exec clones this
/// whole record under one map lock, so it cannot mix generations.
struct ContainerExecBinding<V = LinuxVm> {
    vm: Arc<V>,
    defaults: ContainerExecDefaults,
    generation: ContainerGeneration,
}

impl<V> Clone for ContainerExecBinding<V> {
    fn clone(&self) -> Self {
        Self {
            vm: Arc::clone(&self.vm),
            defaults: self.defaults.clone(),
            generation: self.generation,
        }
    }
}

type ContainerExecBindingMap = HashMap<String, ContainerExecBinding>;
type StackActivationLockMap = HashMap<String, Arc<Mutex<()>>>;
type ContainerLifecycleLockMap = HashMap<String, Arc<RwLock<()>>>;
type StackLifecycleLockMap = HashMap<String, Arc<RwLock<()>>>;

/// Exclusive ownership of one caller-selected container ID generation.
pub(crate) struct ContainerLifecycleTransaction {
    lease: Option<ContainerLifecycleLease>,
}

struct ContainerLifecycleLease {
    container_id: String,
    generation: ContainerGeneration,
    scope: Option<vz_runtime_contract::ContainerGenerationScope>,
    container_store: ContainerStore,
    container_stack: Arc<Mutex<HashMap<String, String>>>,
    _os_guard: ContainerIdLease,
    _stack_guard: Option<OwnedRwLockReadGuard<()>>,
    _container_guard: OwnedRwLockWriteGuard<()>,
}

struct ContainerReadAdmission {
    _os_guard: ContainerIdLease,
    _container_guard: OwnedRwLockReadGuard<()>,
}

struct ContainerWriteAdmission {
    container_id: String,
    generation: Option<ContainerGeneration>,
    _os_guard: ContainerIdLease,
    _container_guard: OwnedRwLockWriteGuard<()>,
}

struct RootfsAssemblyReturn {
    lease: Option<ContainerLifecycleLease>,
    result: Option<std::io::Result<PathBuf>>,
    container_store: ContainerStore,
    container_id: String,
    generation: ContainerGeneration,
}

impl RootfsAssemblyReturn {
    fn into_parts(mut self) -> (ContainerLifecycleLease, std::io::Result<PathBuf>) {
        let lease = match self.lease.take() {
            Some(lease) => lease,
            None => unreachable!("rootfs assembly return owns its lifecycle lease"),
        };
        let result = match self.result.take() {
            Some(result) => result,
            None => unreachable!("rootfs assembly return owns its result"),
        };
        (lease, result)
    }
}

impl Drop for RootfsAssemblyReturn {
    fn drop(&mut self) {
        if let Some(Ok(rootfs)) = self.result.as_ref()
            && self
                .container_store
                .current_generation(&self.container_id)
                .is_ok_and(|current| current == Some(self.generation))
        {
            let _ = fs::remove_dir_all(rootfs);
        }
    }
}

impl Drop for ContainerLifecycleLease {
    fn drop(&mut self) {
        let released = self
            .container_store
            .release_generation_if_absent(&self.container_id, self.generation)
            .unwrap_or(false);
        if released
            && let Some(scope) = self.scope.as_ref()
            && let Ok(mut routes) = self.container_stack.try_lock()
            && routes.get(&self.container_id) == Some(&scope.stack_id)
        {
            routes.remove(&self.container_id);
        }
    }
}

impl ContainerLifecycleTransaction {
    pub(crate) fn container_id(&self) -> &str {
        &self.lease().container_id
    }

    pub(crate) fn generation(&self) -> ContainerGeneration {
        self.lease().generation
    }

    pub(crate) fn scope(&self) -> Option<&vz_runtime_contract::ContainerGenerationScope> {
        self.lease().scope.as_ref()
    }

    fn lease(&self) -> &ContainerLifecycleLease {
        match self.lease.as_ref() {
            Some(lease) => lease,
            None => unreachable!("container lifecycle transaction lease is in its worker"),
        }
    }

    fn take_lease(&mut self) -> ContainerLifecycleLease {
        match self.lease.take() {
            Some(lease) => lease,
            None => unreachable!("container lifecycle transaction lease is in its worker"),
        }
    }

    fn restore_lease(&mut self, lease: ContainerLifecycleLease) {
        debug_assert!(self.lease.is_none());
        self.lease = Some(lease);
    }
}

#[derive(Clone)]
struct StackVmRecord {
    identity: vz_runtime_contract::StackRuntimeIdentity,
    verified_linux_profile: Option<KernelProfile>,
    docker_provisioned: bool,
    boot_ports: Vec<PortMapping>,
    boot_resources: vz_runtime_contract::StackResourceHint,
    vm: Arc<LinuxVm>,
}

/// Atomic ownership of one exact shared-VM boot.
///
/// The lease is intentionally non-cloneable. While it is alive, replacement
/// and shutdown of the same shared VM wait behind its lifecycle fence.
#[must_use = "dropping the lease releases exact shared-VM lifecycle ownership"]
pub struct SharedVmLifecycleLease {
    runtime_identity: vz_runtime_contract::StackRuntimeIdentity,
    verified_profile: KernelProfile,
    stack_vms: Arc<Mutex<HashMap<String, StackVmRecord>>>,
    _stack_lifecycle_guard: OwnedRwLockReadGuard<()>,
}

/// Unified runtime entrypoint.
#[derive(Clone)]
pub struct Runtime {
    config: RuntimeConfig,
    store: ImageStore,
    container_store: ContainerStore,
    puller: ImagePuller,
    /// Active VM handles keyed by container ID, for OCI lifecycle operations.
    vm_handles: Arc<Mutex<HashMap<String, Arc<LinuxVm>>>>,
    /// Shared VMs keyed by stack ID, for multi-container stacks.
    ///
    /// When a container belongs to a stack, its VM handle in [`vm_handles`]
    /// points to the same [`LinuxVm`] instance stored here. Individual
    /// container stop/remove should not tear down the shared VM.
    stack_vms: Arc<Mutex<HashMap<String, StackVmRecord>>>,
    /// Serializes the guest-critical OCI activation transaction per stack.
    ///
    /// Image/rootfs preparation remains parallel, and distinct stacks use
    /// distinct locks. The lock only covers create, start, post-start setup,
    /// and final liveness validation because the bundled youki runtime has
    /// exhibited stale init state when those transactions interleave.
    stack_activation_locks: Arc<Mutex<StackActivationLockMap>>,
    container_lifecycle_locks: Arc<Mutex<ContainerLifecycleLockMap>>,
    stack_lifecycle_locks: Arc<Mutex<StackLifecycleLockMap>>,
    /// Startup ownership-recovery failure that disables every mutating lifecycle admission.
    ///
    /// Continuing with an unreadable generation index could reinterpret a shared-stack
    /// container as standalone and mutate it without durable ownership proof.
    ownership_mutation_quarantine: Arc<Option<String>>,
    /// Maps container IDs to the stack they belong to (if any).
    ///
    /// Used to determine whether a container's VM is shared and should
    /// not be torn down when the container is stopped individually.
    container_stack: Arc<Mutex<HashMap<String, String>>>,
    /// Active port-forwarding handles keyed by container ID.
    ///
    /// Kept alive so the TCP listeners and relay tasks continue running.
    /// Dropped when the container is stopped or removed.
    port_forwards: Arc<Mutex<HashMap<String, PortForwarding>>>,
    /// Active port-forwarding handles keyed by stack ID.
    ///
    /// Kept alive so TCP listeners for shared VM stacks continue running.
    /// Cleaned up when the shared VM is shut down.
    stack_port_forwards: Arc<Mutex<HashMap<String, PortForwarding>>>,
    /// Active container lifecycle metadata keyed by container ID.
    ///
    /// Entries exist only while container lifecycle is active (running/leased).
    active_lifecycle: Arc<Mutex<HashMap<String, ActiveContainerLifecycle>>>,
    /// Active compose log-rotation background tasks keyed by container ID.
    ///
    /// Tasks enforce `logging.options.max-size`/`max-file` for compose
    /// services by rotating `/run/vz-oci/logs/<container>/output.log` in
    /// the guest VM with copy-truncate semantics.
    log_rotation_tasks: Arc<Mutex<HashMap<String, LogRotationTask>>>,
    /// Active interactive execution sessions keyed by daemon execution_id.
    exec_sessions: Arc<Mutex<HashMap<String, ContainerExecSession>>>,
    /// Public exec bindings for durably running containers.
    ///
    /// Each record atomically couples a VM generation to its immutable
    /// activation-time environment, working directory, and user defaults.
    container_exec_bindings: Arc<Mutex<ContainerExecBindingMap>>,
    /// VM instances that already ran interactive PTY prerequisite setup.
    ///
    /// Keyed by `Arc<LinuxVm>` pointer identity (`Arc::as_ptr` cast to usize)
    /// so prep runs once per live VM instance.
    interactive_pty_prep_vms: Arc<Mutex<HashSet<usize>>>,
    /// Container IDs whose overlay upperdir was prepopulated from a
    /// setup-commit tarball at creation time. The backend reads the exact
    /// generation and commit identity to skip `run_setup_commands` on a cache
    /// hit. Entries are cleared on every terminal lifecycle transition.
    setup_restored_containers: Arc<Mutex<HashMap<String, SetupRestoreIdentity>>>,
    oci_deleted_pending_overlay: Arc<std::sync::Mutex<HashMap<String, ContainerGeneration>>>,
    stack_guest_cleanup_complete: Arc<std::sync::Mutex<HashMap<String, ContainerGeneration>>>,
    container_vm_stop_complete: Arc<std::sync::Mutex<HashMap<String, ContainerGeneration>>>,
    stack_vm_stop_complete: Arc<std::sync::Mutex<HashSet<String>>>,
    lifecycle_observer: Arc<
        std::sync::Mutex<
            Option<tokio::sync::mpsc::UnboundedSender<RuntimeLifecycleAdmissionEvent>>,
        >,
    >,
}

impl Runtime {
    /// Create a runtime instance.
    pub fn new(config: RuntimeConfig) -> Self {
        let mut config = config;
        config.data_dir = expand_home_dir(&config.data_dir);

        let store = ImageStore::new(config.data_dir.clone());
        let container_store = ContainerStore::new(config.data_dir.clone());
        let puller = ImagePuller::new(store.clone());
        let (recovered_container_routes, ownership_mutation_quarantine) = match container_store
            .generation_diagnostics()
        {
            Ok(records) => (
                records
                    .into_iter()
                    .filter(|record| record.reserved)
                    .filter_map(|record| {
                        record
                            .scope
                            .map(|scope| (record.container_id, scope.stack_id))
                    })
                    .collect::<HashMap<_, _>>(),
                None,
            ),
            Err(error) => {
                let reason =
                    format!("durable container generation ownership is quarantined: {error}");
                warn!(%error, "could not validate durable container generation ownership; mutating lifecycle operations are disabled");
                (HashMap::new(), Some(reason))
            }
        };

        let runtime = Self {
            config,
            store,
            container_store,
            puller,
            vm_handles: Arc::new(Mutex::new(HashMap::new())),
            stack_vms: Arc::new(Mutex::new(HashMap::new())),
            stack_activation_locks: Arc::new(Mutex::new(HashMap::new())),
            container_lifecycle_locks: Arc::new(Mutex::new(HashMap::new())),
            stack_lifecycle_locks: Arc::new(Mutex::new(HashMap::new())),
            ownership_mutation_quarantine: Arc::new(ownership_mutation_quarantine),
            container_stack: Arc::new(Mutex::new(recovered_container_routes)),
            port_forwards: Arc::new(Mutex::new(HashMap::new())),
            stack_port_forwards: Arc::new(Mutex::new(HashMap::new())),
            active_lifecycle: Arc::new(Mutex::new(HashMap::new())),
            log_rotation_tasks: Arc::new(Mutex::new(HashMap::new())),
            exec_sessions: Arc::new(Mutex::new(HashMap::new())),
            container_exec_bindings: Arc::new(Mutex::new(HashMap::new())),
            interactive_pty_prep_vms: Arc::new(Mutex::new(HashSet::new())),
            setup_restored_containers: Arc::new(Mutex::new(HashMap::new())),
            oci_deleted_pending_overlay: Arc::new(std::sync::Mutex::new(HashMap::new())),
            stack_guest_cleanup_complete: Arc::new(std::sync::Mutex::new(HashMap::new())),
            container_vm_stop_complete: Arc::new(std::sync::Mutex::new(HashMap::new())),
            stack_vm_stop_complete: Arc::new(std::sync::Mutex::new(HashSet::new())),
            lifecycle_observer: Arc::new(std::sync::Mutex::new(None)),
        };

        if runtime.ownership_mutation_quarantine.is_none() {
            runtime.reconcile_stale_containers();
            runtime.cleanup_orphaned_rootfs();
        }

        runtime
    }

    fn ensure_ownership_mutation_allowed(&self) -> Result<(), OciError> {
        match self.ownership_mutation_quarantine.as_ref() {
            Some(reason) => Err(OciError::InvalidConfig(reason.clone())),
            None => Ok(()),
        }
    }

    async fn container_lifecycle_lock(&self, id: &str) -> Arc<RwLock<()>> {
        let mut locks = self.container_lifecycle_locks.lock().await;
        locks
            .entry(id.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    /// Install an instance-scoped lifecycle observer for deterministic integration tests.
    #[doc(hidden)]
    pub fn install_lifecycle_observer(&self) -> RuntimeLifecycleObserver {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        *self
            .lifecycle_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sender);
        receiver
    }

    async fn observe_lifecycle_admission(
        &self,
        kind: RuntimeLifecycleAdmissionKind,
        container_id: &str,
    ) {
        let sender = self
            .lifecycle_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(sender) = sender else {
            return;
        };
        let (resume, paused) = tokio::sync::oneshot::channel();
        if sender
            .send(RuntimeLifecycleAdmissionEvent {
                kind,
                container_id: container_id.to_string(),
                resume,
            })
            .is_ok()
        {
            let _ = paused.await;
        }
    }

    fn map_container_store_error(id: &str, error: std::io::Error) -> OciError {
        match error.kind() {
            std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::WouldBlock => {
                OciError::ContainerAlreadyExists { id: id.to_string() }
            }
            std::io::ErrorKind::NotFound => OciError::ContainerNotFound { id: id.to_string() },
            _ => OciError::Storage(error),
        }
    }

    async fn acquire_container_os_write(&self, id: &str) -> Result<ContainerIdLease, OciError> {
        let store = self.container_store.clone();
        let owned_id = id.to_string();
        tokio::task::spawn_blocking(move || store.acquire_container_write_lease(&owned_id))
            .await
            .map_err(|error| OciError::Storage(std::io::Error::other(error.to_string())))?
            .map_err(|error| Self::map_container_store_error(id, error))
    }

    async fn acquire_container_read_admission(
        &self,
        id: &str,
    ) -> Result<ContainerReadAdmission, OciError> {
        let container_guard = self.container_lifecycle_lock(id).await.read_owned().await;
        let store = self.container_store.clone();
        let owned_id = id.to_string();
        let os_guard =
            tokio::task::spawn_blocking(move || store.acquire_container_read_lease(&owned_id))
                .await
                .map_err(|error| OciError::Storage(std::io::Error::other(error.to_string())))?
                .map_err(|error| Self::map_container_store_error(id, error))?;
        Ok(ContainerReadAdmission {
            _os_guard: os_guard,
            _container_guard: container_guard,
        })
    }

    async fn acquire_sorted_container_write_admissions(
        &self,
        container_ids: &[String],
    ) -> Result<Vec<ContainerWriteAdmission>, OciError> {
        self.ensure_ownership_mutation_allowed()?;
        let mut sorted = container_ids.to_vec();
        sorted.sort();
        sorted.dedup();
        let mut admissions = Vec::with_capacity(sorted.len());
        for container_id in sorted {
            let container_guard = self
                .container_lifecycle_lock(&container_id)
                .await
                .write_owned()
                .await;
            let os_guard = self.acquire_container_os_write(&container_id).await?;
            let generation = self
                .container_store
                .current_generation(&container_id)
                .map_err(|error| Self::map_container_store_error(&container_id, error))?;
            admissions.push(ContainerWriteAdmission {
                container_id,
                generation,
                _os_guard: os_guard,
                _container_guard: container_guard,
            });
        }
        Ok(admissions)
    }

    pub(super) async fn stack_lifecycle_lock(&self, stack_id: &str) -> Arc<RwLock<()>> {
        let mut locks = self.stack_lifecycle_locks.lock().await;
        locks
            .entry(stack_id.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    /// Reserve a new generation. Stack membership is locked before the ID,
    /// establishing the global stack -> container -> activation ordering.
    pub(crate) async fn begin_container_create(
        &self,
        run: &mut RunConfig,
        stack_id: Option<&str>,
    ) -> Result<ContainerLifecycleTransaction, OciError> {
        self.begin_container_create_inner(run, stack_id, None).await
    }

    /// Reserve a generation with the exact durable topology scope.
    pub(crate) async fn begin_scoped_container_create(
        &self,
        run: &mut RunConfig,
        scope: &vz_runtime_contract::ContainerGenerationScope,
    ) -> Result<ContainerLifecycleTransaction, OciError> {
        scope.validate().map_err(OciError::InvalidConfig)?;
        self.begin_container_create_inner(run, Some(&scope.stack_id), Some(scope))
            .await
    }

    /// Reserve a detached generation without beginning OCI/rootfs/guest activation.
    ///
    /// Unlike `ContainerLifecycleTransaction`, the returned ownership has no
    /// drop-release behavior and therefore survives caller or process loss.
    pub async fn reserve_scoped_container_generation(
        &self,
        container_id: &str,
        scope: &vz_runtime_contract::ContainerGenerationScope,
    ) -> Result<vz_runtime_contract::ContainerGenerationOwnership, OciError> {
        self.ensure_ownership_mutation_allowed()?;
        validate_container_id(container_id)?;
        scope.validate().map_err(OciError::InvalidConfig)?;
        let _stack_guard = self
            .stack_lifecycle_lock(&scope.stack_id)
            .await
            .read_owned()
            .await;
        let _container_guard = self
            .container_lifecycle_lock(container_id)
            .await
            .try_write_owned()
            .map_err(|_| OciError::ContainerAlreadyExists {
                id: container_id.to_string(),
            })?;
        let os_guard = self
            .container_store
            .try_acquire_container_write_lease(container_id)
            .map_err(|error| Self::map_container_store_error(container_id, error))?;
        self.observe_lifecycle_admission(
            RuntimeLifecycleAdmissionKind::CreateBeforeReservation,
            container_id,
        )
        .await;
        let generation = self
            .container_store
            .reserve_scoped_generation_with_write_lease(container_id, scope, &os_guard)
            .map_err(|error| Self::map_container_store_error(container_id, error))?;
        let ownership = vz_runtime_contract::ContainerGenerationOwnership {
            container_id: container_id.to_string(),
            generation: generation.0,
            stack_id: scope.stack_id.clone(),
            scope: Some(Box::new(scope.clone())),
        };
        // A detached reservation intentionally survives caller cancellation
        // after publication. Expose that crash boundary to integration tests
        // before returning the ownership proof.
        self.observe_lifecycle_admission(
            RuntimeLifecycleAdmissionKind::CreateAfterReservation,
            container_id,
        )
        .await;
        Ok(ownership)
    }

    /// Assign a runtime ID when needed, then reserve it without activation.
    pub async fn reserve_scoped_container_run(
        &self,
        run: &mut RunConfig,
        scope: &vz_runtime_contract::ContainerGenerationScope,
    ) -> Result<vz_runtime_contract::ContainerGenerationOwnership, OciError> {
        let container_id = run.container_id.clone().unwrap_or_else(new_container_id);
        run.container_id = Some(container_id.clone());
        self.reserve_scoped_container_generation(&container_id, scope)
            .await
    }

    /// Inspect a detached reservation without updating route or lifecycle caches.
    pub fn inspect_scoped_container_reservation(
        &self,
        container_id: &str,
        scope: &vz_runtime_contract::ContainerGenerationScope,
    ) -> Result<vz_runtime_contract::ContainerGenerationInspection, OciError> {
        validate_container_id(container_id)?;
        scope.validate().map_err(OciError::InvalidConfig)?;
        let inspection = match self
            .container_store
            .inspect_scoped_reservation(container_id, scope)
        {
            Ok(inspection) => inspection,
            Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                return Ok(
                    vz_runtime_contract::ContainerGenerationInspection::Malformed(
                        error.to_string(),
                    ),
                );
            }
            Err(error) => return Err(Self::map_container_store_error(container_id, error)),
        };
        Ok(contract_inspection(container_id, scope, inspection))
    }

    /// Inspect an exact generation without adopting or repairing it.
    pub fn inspect_scoped_container_generation(
        &self,
        ownership: &vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::ContainerGenerationInspection, OciError> {
        ownership.validate().map_err(OciError::InvalidConfig)?;
        let scope = ownership.scope.as_deref().ok_or_else(|| {
            OciError::InvalidConfig("generation ownership requires exact scope".to_string())
        })?;
        let inspection = match self.container_store.inspect_scoped_generation(
            &ownership.container_id,
            ContainerGeneration(ownership.generation),
            scope,
        ) {
            Ok(inspection) => inspection,
            Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                return Ok(
                    vz_runtime_contract::ContainerGenerationInspection::Malformed(
                        error.to_string(),
                    ),
                );
            }
            Err(error) => {
                return Err(Self::map_container_store_error(
                    &ownership.container_id,
                    error,
                ));
            }
        };
        Ok(contract_inspection(
            &ownership.container_id,
            scope,
            inspection,
        ))
    }

    /// Explicitly abandon only an exact unpublished detached reservation.
    pub async fn release_scoped_container_reservation(
        &self,
        ownership: &vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::ContainerGenerationReleaseOutcome, OciError> {
        self.ensure_ownership_mutation_allowed()?;
        ownership.validate().map_err(OciError::InvalidConfig)?;
        let scope = ownership.scope.as_deref().ok_or_else(|| {
            OciError::InvalidConfig("generation ownership requires exact scope".to_string())
        })?;
        let _stack_guard = self
            .stack_lifecycle_lock(&scope.stack_id)
            .await
            .read_owned()
            .await;
        let _container_guard = self
            .container_lifecycle_lock(&ownership.container_id)
            .await
            .write_owned()
            .await;
        let os_guard = self
            .acquire_container_os_write(&ownership.container_id)
            .await?;
        let released = self
            .container_store
            .release_scoped_generation_with_write_lease(
                &ownership.container_id,
                ContainerGeneration(ownership.generation),
                scope,
                &os_guard,
            )
            .map_err(|error| Self::map_container_store_error(&ownership.container_id, error))?;
        Ok(if released {
            vz_runtime_contract::ContainerGenerationReleaseOutcome::Released
        } else {
            vz_runtime_contract::ContainerGenerationReleaseOutcome::AlreadyAbsent
        })
    }

    async fn begin_container_create_inner(
        &self,
        run: &mut RunConfig,
        stack_id: Option<&str>,
        scope: Option<&vz_runtime_contract::ContainerGenerationScope>,
    ) -> Result<ContainerLifecycleTransaction, OciError> {
        self.ensure_ownership_mutation_allowed()?;
        let id = run.container_id.clone().unwrap_or_else(new_container_id);
        validate_container_id(&id)?;
        run.container_id = Some(id.clone());
        let stack_guard = if let Some(stack_id) = stack_id {
            Some(self.stack_lifecycle_lock(stack_id).await.read_owned().await)
        } else {
            None
        };
        // New-generation admission is deliberately fail-fast. Waiting here would let
        // a duplicate create inherit the name after the current setup transaction
        // rolls back, violating the caller-selected ID's duplicate semantics.
        let container_guard = self
            .container_lifecycle_lock(&id)
            .await
            .try_write_owned()
            .map_err(|_| OciError::ContainerAlreadyExists { id: id.clone() })?;
        let os_guard = self
            .container_store
            .try_acquire_container_write_lease(&id)
            .map_err(|error| Self::map_container_store_error(&id, error))?;
        self.observe_lifecycle_admission(
            RuntimeLifecycleAdmissionKind::CreateBeforeReservation,
            &id,
        )
        .await;
        let generation = match scope {
            Some(scope) => self
                .container_store
                .reserve_scoped_generation_with_write_lease(&id, scope, &os_guard),
            None => self
                .container_store
                .reserve_generation_with_write_lease(&id, &os_guard),
        }
        .map_err(|error| Self::map_container_store_error(&id, error))?;
        // Take cleanup ownership before the next await. If the caller drops
        // this future after the durable reservation, the lease releases that
        // exact unpublished generation instead of stranding the ID.
        let transaction = ContainerLifecycleTransaction {
            lease: Some(ContainerLifecycleLease {
                container_id: id.clone(),
                generation,
                scope: scope.cloned(),
                container_store: self.container_store.clone(),
                container_stack: Arc::clone(&self.container_stack),
                _os_guard: os_guard,
                _stack_guard: stack_guard,
                _container_guard: container_guard,
            }),
        };
        self.observe_lifecycle_admission(
            RuntimeLifecycleAdmissionKind::CreateAfterReservation,
            &id,
        )
        .await;
        self.setup_restored_containers.lock().await.remove(&id);
        self.oci_deleted_pending_overlay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
        Ok(transaction)
    }

    async fn begin_existing_container(
        &self,
        id: &str,
    ) -> Result<ContainerLifecycleTransaction, OciError> {
        self.ensure_ownership_mutation_allowed()?;
        loop {
            let routed_stack = self.container_stack.lock().await.get(id).cloned();
            let stack_guard = if let Some(stack_id) = routed_stack.as_deref() {
                Some(self.stack_lifecycle_lock(stack_id).await.read_owned().await)
            } else {
                None
            };
            let container_guard = self.container_lifecycle_lock(id).await.write_owned().await;
            let current_stack = self.container_stack.lock().await.get(id).cloned();
            if current_stack != routed_stack {
                drop(container_guard);
                drop(stack_guard);
                continue;
            }
            // Cross-process admission precedes durable generation lookup.
            let os_guard = self.acquire_container_os_write(id).await?;
            let diagnostic = self
                .container_store
                .generation_diagnostic(id)
                .map_err(|error| Self::map_container_store_error(id, error))?;
            let (generation, scope) = match diagnostic {
                Some(current) if !current.reserved => {
                    return Err(OciError::ContainerNotFound { id: id.to_string() });
                }
                Some(current) => match current.scope {
                    Some(scope) => {
                        if current_stack.as_deref() != Some(scope.stack_id.as_str()) {
                            // The durable scope is authoritative, but this attempt currently
                            // holds the stack lock selected by the stale cache. Repair only the
                            // cache while ID+OS ownership is exclusive, then release and retry
                            // so the returned transaction holds the durable stack's lock.
                            self.container_stack
                                .lock()
                                .await
                                .insert(id.to_string(), scope.stack_id);
                            drop(os_guard);
                            drop(container_guard);
                            drop(stack_guard);
                            continue;
                        }
                        (current.generation, Some(scope))
                    }
                    None => {
                        if let Some(cached_stack) = current_stack {
                            return Err(OciError::ContainerOwnershipMismatch {
                                id: id.to_string(),
                                reason: format!(
                                    "cached route belongs to stack '{cached_stack}', but the durable generation is legacy-unscoped and quarantined"
                                ),
                            });
                        }
                        (current.generation, None)
                    }
                },
                None => {
                    if let Some(cached_stack) = current_stack {
                        return Err(OciError::ContainerOwnershipMismatch {
                            id: id.to_string(),
                            reason: format!(
                                "cached route belongs to stack '{cached_stack}', but no durable generation ownership exists"
                            ),
                        });
                    }
                    // Preserve compatibility for truly standalone metadata written by an older
                    // runtime. Adoption remains forbidden whenever any stack route claims it.
                    let generation = self
                        .container_store
                        .current_generation(id)
                        .map_err(|error| Self::map_container_store_error(id, error))?
                        .ok_or_else(|| OciError::ContainerNotFound { id: id.to_string() })?;
                    (generation, None)
                }
            };
            return Ok(ContainerLifecycleTransaction {
                lease: Some(ContainerLifecycleLease {
                    container_id: id.to_string(),
                    generation,
                    scope,
                    container_store: self.container_store.clone(),
                    container_stack: Arc::clone(&self.container_stack),
                    _os_guard: os_guard,
                    _stack_guard: stack_guard,
                    _container_guard: container_guard,
                }),
            });
        }
    }

    /// Acquire lifecycle ownership only if an exact runtime-issued generation
    /// still owns the requested ID and stack scope.
    ///
    /// `Ok(None)` means that no generation is currently reserved. A different
    /// current generation or a foreign published stack route fails closed.
    pub(crate) async fn begin_owned_container_generation(
        &self,
        id: &str,
        generation: ContainerGeneration,
        scope: &vz_runtime_contract::ContainerGenerationScope,
    ) -> Result<Option<ContainerLifecycleTransaction>, OciError> {
        self.begin_owned_container_generation_inner(id, generation, scope, false)
            .await
    }

    /// Acquire exact lifecycle ownership for cleanup of either an unpublished
    /// reservation or its published container metadata.
    async fn begin_owned_container_generation_for_cleanup(
        &self,
        id: &str,
        generation: ContainerGeneration,
        scope: &vz_runtime_contract::ContainerGenerationScope,
    ) -> Result<Option<ContainerLifecycleTransaction>, OciError> {
        self.begin_owned_container_generation_inner(id, generation, scope, true)
            .await
    }

    async fn begin_owned_container_generation_inner(
        &self,
        id: &str,
        generation: ContainerGeneration,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        allow_published: bool,
    ) -> Result<Option<ContainerLifecycleTransaction>, OciError> {
        self.ensure_ownership_mutation_allowed()?;
        validate_container_id(id)?;
        scope.validate().map_err(OciError::InvalidConfig)?;
        let stack_guard = self
            .stack_lifecycle_lock(&scope.stack_id)
            .await
            .read_owned()
            .await;
        let container_guard = self.container_lifecycle_lock(id).await.write_owned().await;
        let os_guard = self.acquire_container_os_write(id).await?;
        let current = self
            .container_store
            .inspect_scoped_generation(id, generation, scope)
            .map_err(|error| Self::map_container_store_error(id, error))?;
        match current {
            ScopedGenerationInspection::ReservedUnpublished(current) if current == generation => {}
            ScopedGenerationInspection::Absent => return Ok(None),
            ScopedGenerationInspection::Published(current)
                if allow_published && current == generation => {}
            ScopedGenerationInspection::Published(_) => {
                return Err(OciError::ContainerOwnershipMismatch {
                    id: id.to_string(),
                    reason: "exact generation is already published and cannot be activated again"
                        .to_string(),
                });
            }
            ScopedGenerationInspection::Foreign => {
                return Err(OciError::ContainerOwnershipMismatch {
                    id: id.to_string(),
                    reason: "proof scope does not match the durable generation scope".to_string(),
                });
            }
            ScopedGenerationInspection::Replacement => {
                return Err(OciError::ContainerOwnershipMismatch {
                    id: id.to_string(),
                    reason: "proof generation was replaced by another generation".to_string(),
                });
            }
            ScopedGenerationInspection::LegacyUnscoped => {
                return Err(OciError::ContainerOwnershipMismatch {
                    id: id.to_string(),
                    reason: "current generation is legacy-unscoped and quarantined".to_string(),
                });
            }
            ScopedGenerationInspection::Malformed(reason) => {
                return Err(OciError::ContainerOwnershipMismatch {
                    id: id.to_string(),
                    reason: format!("durable generation metadata is malformed: {reason}"),
                });
            }
            ScopedGenerationInspection::ReservedUnpublished(current) => {
                return Err(OciError::ContainerOwnershipMismatch {
                    id: id.to_string(),
                    reason: format!(
                        "proof names generation {}, but current generation is {}",
                        generation.0, current.0
                    ),
                });
            }
        }

        // A process-local route is only a cache. The exact durable scope was
        // validated while holding stack -> container -> OS -> store ownership,
        // so stale cache state must be repaired rather than overriding it.
        let mut routes = self.container_stack.lock().await;
        routes.insert(id.to_string(), scope.stack_id.clone());
        drop(routes);

        Ok(Some(ContainerLifecycleTransaction {
            lease: Some(ContainerLifecycleLease {
                container_id: id.to_string(),
                generation,
                scope: Some(scope.clone()),
                container_store: self.container_store.clone(),
                container_stack: Arc::clone(&self.container_stack),
                _os_guard: os_guard,
                _stack_guard: Some(stack_guard),
                _container_guard: container_guard,
            }),
        }))
    }

    /// Stop and remove exactly one generation owned by a stack create receipt.
    pub(crate) async fn cleanup_owned_container_generation(
        &self,
        id: &str,
        generation: ContainerGeneration,
        scope: &vz_runtime_contract::ContainerGenerationScope,
    ) -> Result<vz_runtime_contract::GenerationCleanupOutcome, OciError> {
        self.teardown_owned_container_generation(id, generation, scope, true, None, None)
            .await
    }

    /// Gracefully stop and remove exactly one successful stack generation.
    pub(crate) async fn stop_and_remove_owned_container_generation(
        &self,
        id: &str,
        generation: ContainerGeneration,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        signal: Option<&str>,
        grace_period: Option<Duration>,
    ) -> Result<vz_runtime_contract::GenerationCleanupOutcome, OciError> {
        self.teardown_owned_container_generation(id, generation, scope, false, signal, grace_period)
            .await
    }

    async fn teardown_owned_container_generation(
        &self,
        id: &str,
        generation: ContainerGeneration,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        force: bool,
        signal: Option<&str>,
        grace_period: Option<Duration>,
    ) -> Result<vz_runtime_contract::GenerationCleanupOutcome, OciError> {
        let Some(transaction) = self
            .begin_owned_container_generation_for_cleanup(id, generation, scope)
            .await?
        else {
            return Ok(vz_runtime_contract::GenerationCleanupOutcome::AlreadyAbsent);
        };

        if self
            .container_store
            .find(id)
            .map_err(OciError::from)?
            .is_none()
        {
            drop(transaction);
            return Ok(vz_runtime_contract::GenerationCleanupOutcome::AlreadyAbsent);
        }

        let stop_error = self
            .stop_container_in_transaction(id, force, signal, grace_period, &transaction)
            .await
            .err();
        let remove_error = match self.remove_container_in_transaction(id, &transaction).await {
            Ok(()) => None,
            Err(OciError::ContainerNotFound { .. }) => None,
            Err(error) => Some(error),
        };

        match (stop_error, remove_error) {
            (None, None) => Ok(vz_runtime_contract::GenerationCleanupOutcome::Removed),
            (Some(stop), None) => Err(OciError::InvalidConfig(format!(
                "generation-owned cleanup removed container '{id}' after stop failed: {stop}"
            ))),
            (None, Some(remove)) => Err(remove),
            (Some(stop), Some(remove)) => Err(OciError::InvalidConfig(format!(
                "generation-owned cleanup failed for container '{id}': stop: {stop}; remove: {remove}"
            ))),
        }
    }

    /// Run blocking layer assembly while the worker itself owns the lifecycle
    /// lease. If this async caller is cancelled, the worker conditionally removes
    /// its completed rootfs before releasing the ID; a replacement generation can
    /// therefore never overlap the old writer.
    async fn assemble_rootfs_in_transaction(
        &self,
        image_id: &str,
        transaction: &mut ContainerLifecycleTransaction,
    ) -> Result<PathBuf, OciError> {
        let store = self.store.clone();
        let image_id = image_id.to_string();
        let container_id = transaction.container_id().to_string();
        let generation = transaction.generation();
        let lease = transaction.take_lease();
        let (sender, receiver) = oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                store.assemble_rootfs_structured(&image_id, &container_id)
            }))
            .unwrap_or_else(|_| {
                Err(std::io::Error::other(
                    "rootfs assembly worker panicked while holding lifecycle ownership",
                ))
            });
            let returned = RootfsAssemblyReturn {
                container_store: lease.container_store.clone(),
                container_id,
                generation,
                lease: Some(lease),
                result: Some(result),
            };
            // When the receiver disappeared either before or after send, the
            // returned payload's Drop owns conditional cleanup and lease release.
            let _ = sender.send(returned);
        });

        let returned = receiver.await.map_err(|error| {
            OciError::Storage(std::io::Error::other(format!(
                "rootfs assembly worker terminated without returning ownership: {error}"
            )))
        })?;
        let (lease, result) = returned.into_parts();
        transaction.restore_lease(lease);
        result.map_err(OciError::from)
    }

    fn persist_owned(
        &self,
        transaction: &ContainerLifecycleTransaction,
        container: ContainerInfo,
    ) -> Result<(), OciError> {
        let container_id = container.id.clone();
        self.container_store
            .upsert_if_generation(container, transaction.generation())
            .map_err(|error| Self::map_container_store_error(&container_id, error))
    }

    fn container_vm_stop_is_complete(
        &self,
        container_id: &str,
        generation: ContainerGeneration,
    ) -> bool {
        self.container_vm_stop_complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(container_id)
            .is_some_and(|complete| *complete == generation)
    }

    fn mark_container_vm_stop_complete(&self, container_id: &str, generation: ContainerGeneration) {
        self.container_vm_stop_complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(container_id.to_string(), generation);
    }

    async fn commit_container_cleanup_ownership(&self, container_id: &str) {
        // Acquire every async registry before the first mutation. Cancellation
        // while waiting leaves the complete recovery record intact; after the
        // final guard is acquired, the commit contains no await.
        let mut handles = self.vm_handles.lock().await;
        let mut routes = self.container_stack.lock().await;
        let mut active_lifecycle = self.active_lifecycle.lock().await;
        let mut setup_restored = self.setup_restored_containers.lock().await;
        let mut exec_bindings = self.container_exec_bindings.lock().await;
        let mut pending = self
            .oci_deleted_pending_overlay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut guest_complete = self
            .stack_guest_cleanup_complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut vm_stop_complete = self
            .container_vm_stop_complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        handles.remove(container_id);
        routes.remove(container_id);
        active_lifecycle.remove(container_id);
        setup_restored.remove(container_id);
        exec_bindings.remove(container_id);
        pending.remove(container_id);
        guest_complete.remove(container_id);
        vm_stop_complete.remove(container_id);
    }

    async fn persist_stopped_and_commit_cleanup(
        &self,
        transaction: &ContainerLifecycleTransaction,
        container: ContainerInfo,
    ) -> Result<(), OciError> {
        self.persist_generation_and_commit_cleanup(container, transaction.generation())
            .await?;
        Ok(())
    }

    async fn persist_generation_and_commit_cleanup(
        &self,
        container: ContainerInfo,
        generation: ContainerGeneration,
    ) -> Result<(), OciError> {
        let container_id = container.id.clone();
        self.container_store
            .upsert_if_generation(container, generation)
            .map_err(|error| Self::map_container_store_error(&container_id, error))?;
        self.commit_container_cleanup_ownership(&container_id).await;
        Ok(())
    }

    fn cleanup_owned_rootfs(&self, transaction: &ContainerLifecycleTransaction, rootfs: &Path) {
        if self
            .container_store
            .current_generation(transaction.container_id())
            .is_ok_and(|current| current == Some(transaction.generation()))
        {
            self.cleanup_rootfs_dir(rootfs);
        }
    }

    /// Return configured data directory.
    /// Whether the named container's overlay upperdir was pre-populated
    /// from a cached setup-commit tarball at creation time. Backends use
    /// this to decide whether to skip `run_setup_commands`.
    pub async fn was_setup_restored(&self, container_id: &str, commit_ref: &str) -> bool {
        let current = self
            .container_store
            .current_generation(container_id)
            .ok()
            .flatten();
        self.setup_restored_containers
            .lock()
            .await
            .get(container_id)
            .is_some_and(|identity| {
                Some(identity.generation) == current && identity.commit_ref == commit_ref
            })
    }

    pub fn data_dir(&self) -> &PathBuf {
        &self.config.data_dir
    }

    /// Snapshot durable reservations and process-local lifecycle maps.
    pub async fn lifecycle_diagnostics(&self) -> Result<RuntimeLifecycleDiagnostics, OciError> {
        let generations = self
            .container_store
            .generation_diagnostics()
            .map_err(OciError::from)?;
        let mut vm_handle_ids = self
            .vm_handles
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        vm_handle_ids.sort();
        let mut stack_vm_ids = self
            .stack_vms
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        stack_vm_ids.sort();
        let mut container_route_pairs = self
            .container_stack
            .lock()
            .await
            .iter()
            .map(|(container_id, stack_id)| (container_id.clone(), stack_id.clone()))
            .collect::<Vec<_>>();
        container_route_pairs.sort();
        let mut stack_port_forward_ids = self
            .stack_port_forwards
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        stack_port_forward_ids.sort();
        let rootfs_directories = fs::read_dir(self.config.data_dir.join("rootfs"))
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|entry| entry.path().is_dir())
                    .count()
            })
            .unwrap_or(0);
        Ok(RuntimeLifecycleDiagnostics {
            generations,
            container_lock_slots: self.container_lifecycle_locks.lock().await.len(),
            stack_lock_slots: self.stack_lifecycle_locks.lock().await.len(),
            vm_handles: vm_handle_ids.len(),
            vm_handle_ids,
            stack_vms: stack_vm_ids.len(),
            stack_vm_ids,
            container_routes: container_route_pairs.len(),
            container_route_pairs,
            stack_port_forwards: stack_port_forward_ids.len(),
            stack_port_forward_ids,
            exec_bindings: self.container_exec_bindings.lock().await.len(),
            active_lifecycles: self.active_lifecycle.lock().await.len(),
            exec_sessions: self.exec_sessions.lock().await.len(),
            setup_restore_entries: self.setup_restored_containers.lock().await.len(),
            overlay_cleanup_pending: self
                .oci_deleted_pending_overlay
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            rootfs_directories,
        })
    }

    /// Clone the runtime configuration used by this runtime instance.
    pub fn clone_config(&self) -> RuntimeConfig {
        self.config.clone()
    }

    /// Advertised checkpoint capabilities for this backend runtime.
    pub fn checkpoint_capabilities(&self) -> vz_runtime_contract::RuntimeCapabilities {
        vz_runtime_contract::canonical_backend_capabilities(
            &vz_runtime_contract::SandboxBackend::MacosVz,
        )
    }

    /// Validate that checkpoint class semantics are supported before execution.
    pub fn ensure_checkpoint_class_supported(
        &self,
        class: vz_runtime_contract::CheckpointClass,
        operation: vz_runtime_contract::RuntimeOperation,
    ) -> Result<(), OciError> {
        vz_runtime_contract::ensure_checkpoint_class_supported(
            self.checkpoint_capabilities(),
            class,
            operation,
        )
        .map_err(|err| match err {
            vz_runtime_contract::RuntimeError::UnsupportedOperation { operation, reason } => {
                OciError::UnsupportedOperation { operation, reason }
            }
            other => OciError::InvalidConfig(other.to_string()),
        })
    }

    /// Create a [`MacosRuntimeBackend`] adapter for this runtime.
    ///
    /// The returned adapter implements [`vz_runtime_contract::RuntimeBackend`]
    /// and delegates all operations back to this runtime instance.
    pub fn into_backend(self) -> crate::macos_backend::MacosRuntimeBackend {
        crate::macos_backend::MacosRuntimeBackend::new(self)
    }

    /// List cached images currently tracked by refs.
    pub fn images(&self) -> Result<Vec<ImageInfo>, OciError> {
        self.store.list_images().map_err(Into::into)
    }

    /// List all containers tracked in local metadata.
    pub fn list_containers(&self) -> Result<Vec<ContainerInfo>, OciError> {
        self.container_store.load_all().map_err(OciError::from)
    }

    /// Remove container metadata and rootfs artifacts.
    ///
    /// If a VM handle is still active for this container, sends an OCI delete
    /// to the guest runtime before cleaning up host metadata.
    pub async fn remove_container(&self, id: &str) -> Result<(), OciError> {
        let transaction = self.begin_existing_container(id).await?;
        self.observe_lifecycle_admission(RuntimeLifecycleAdmissionKind::RemoveWriterAcquired, id)
            .await;
        self.remove_container_in_transaction(id, &transaction).await
    }

    pub(crate) async fn remove_container_in_transaction(
        &self,
        id: &str,
        transaction: &ContainerLifecycleTransaction,
    ) -> Result<(), OciError> {
        debug_assert_eq!(id, transaction.container_id());
        let containers = self.container_store.load_all().map_err(OciError::from)?;
        let container = containers
            .into_iter()
            .find(|container| container.id == id)
            .ok_or_else(|| OciError::ContainerNotFound { id: id.to_string() })?;

        if matches!(container.status, ContainerStatus::Running) {
            return Err(OciError::InvalidConfig(format!(
                "cannot remove running container '{id}'; stop it first"
            )));
        }

        // Removal is a terminal transition: stop admitting new public execs
        // before any guest cleanup or recovery routing changes.
        self.container_exec_bindings.lock().await.remove(id);

        // Keep the registry published until its fallible shutdown completes so
        // an error cannot erase the only ownership record.
        shutdown_port_forwarding_registry_entry(&self.port_forwards, id).await?;
        self.stop_log_rotation_task(id).await;
        // Delete OCI state via the guest runtime if its VM is still up. Keep
        // recovery routing, metadata, and rootfs intact if deletion fails so a
        // later explicit remove or stack shutdown can safely retry.
        // Try the per-container handle first; fall back to the shared stack VM
        // (the per-container handle may have been removed by stop_container).
        let vm = self.vm_handles.lock().await.get(id).cloned();
        let stack_id = self.container_stack.lock().await.get(id).cloned();
        let activation_guard = if let Some(stack_id) = stack_id.as_deref() {
            Some(self.acquire_stack_activation_guard(stack_id).await)
        } else {
            None
        };
        let guest_vm = if vm.is_some() {
            vm
        } else if let Some(stack_id) = stack_id.as_deref() {
            self.stack_vms
                .lock()
                .await
                .get(stack_id)
                .map(|record| record.vm.clone())
        } else {
            None
        };
        if let Some(vm) = guest_vm {
            if stack_id.is_some() {
                stack_vm::shutdown_container_cleanup_transition(
                    self,
                    id,
                    transaction.generation(),
                    || async {
                        vm.oci_delete(id.to_string(), true)
                            .await
                            .map_err(OciError::from)
                    },
                    || async {
                        self.teardown_owned_stack_container_overlay(
                            &vm,
                            id,
                            transaction.generation(),
                        )
                        .await
                    },
                )
                .await
                .map_err(|error| {
                    OciError::InvalidConfig(format!(
                        "cannot remove container '{id}': shared-VM guest cleanup failed; retained metadata, recovery routing, and rootfs for retry: {error}"
                    ))
                })?;
            } else {
                if !self.overlay_cleanup_is_pending(id, transaction.generation()) {
                    vm.oci_delete(id.to_string(), true).await.map_err(|error| {
                        OciError::InvalidConfig(format!(
                            "cannot remove container '{id}': OCI delete failed; retained metadata, recovery routing, and rootfs for retry: {error}"
                        ))
                    })?;
                    self.mark_overlay_cleanup_pending(id, transaction.generation());
                }
                if !self.container_vm_stop_is_complete(id, transaction.generation()) {
                    vm.stop().await.map_err(|error| {
                        OciError::InvalidConfig(format!(
                            "cannot remove container '{id}': VM stop failed; retained metadata, VM, and rootfs for retry: {error}"
                        ))
                    })?;
                    self.mark_container_vm_stop_complete(id, transaction.generation());
                }
            }
            tracing::debug!(container_id = %id, "remove_container: guest cleanup succeeded");
        } else if let Some(stack_id) = stack_id.as_deref() {
            tracing::warn!(container_id = %id, %stack_id, "remove_container: stack VM not found; guest overlay disappeared with the VM");
        } else {
            tracing::debug!(container_id = %id, "remove_container: no vm_handle or stack_id, skipping oci_delete");
        }
        drop(activation_guard);
        self.commit_container_cleanup_ownership(id).await;

        if let Some(path) = container.rootfs_path {
            if self
                .container_store
                .current_generation(transaction.container_id())
                .is_ok_and(|current| current == Some(transaction.generation()))
                && path.exists()
            {
                fs::remove_dir_all(path).map_err(OciError::from)?;
            }
        }

        // Release the durable name only after generation-owned artifacts are
        // gone. Another process cannot reserve the next generation while the
        // sidecar remains reserved above.
        self.container_store
            .remove_if_generation(id, transaction.generation())
            .map_err(|error| Self::map_container_store_error(id, error))?;

        Ok(())
    }

    /// Stop a running container using the OCI runtime lifecycle.
    ///
    /// Sends `oci_kill` (SIGTERM for graceful, SIGKILL for forced) and polls
    /// `oci_state` until the container exits or the grace period expires.
    ///
    /// `signal` overrides the default stop signal (SIGTERM).
    /// `grace_period` overrides the default grace period before SIGKILL escalation.
    pub async fn stop_container(
        &self,
        id: &str,
        force: bool,
        signal: Option<&str>,
        grace_period: Option<Duration>,
    ) -> Result<ContainerInfo, OciError> {
        self.observe_lifecycle_admission(RuntimeLifecycleAdmissionKind::StopWriterRequested, id)
            .await;
        let transaction = self.begin_existing_container(id).await?;
        self.observe_lifecycle_admission(RuntimeLifecycleAdmissionKind::StopWriterAcquired, id)
            .await;
        self.stop_container_in_transaction(id, force, signal, grace_period, &transaction)
            .await
    }

    pub(crate) async fn stop_container_in_transaction(
        &self,
        id: &str,
        force: bool,
        signal: Option<&str>,
        grace_period: Option<Duration>,
        transaction: &ContainerLifecycleTransaction,
    ) -> Result<ContainerInfo, OciError> {
        debug_assert_eq!(id, transaction.container_id());
        let mut container = self
            .container_store
            .load_all()
            .map_err(OciError::from)?
            .into_iter()
            .find(|item| item.id == id)
            .ok_or_else(|| OciError::ContainerNotFound { id: id.to_string() })?;

        let retained_vm = self.vm_handles.lock().await.get(id).cloned();
        if !matches!(
            container.status,
            ContainerStatus::Running | ContainerStatus::Stopped { .. }
        ) || (!matches!(container.status, ContainerStatus::Running) && retained_vm.is_none())
        {
            self.container_exec_bindings.lock().await.remove(id);
            self.active_lifecycle.lock().await.remove(id);
            self.stop_log_rotation_task(id).await;
            self.vm_handles.lock().await.remove(id);
            self.setup_restored_containers.lock().await.remove(id);
            return Ok(container);
        }

        // Atomically stop admitting new exec calls before shutdown begins.
        // Recovery/lifecycle routing remains available through vm_handles.
        self.container_exec_bindings.lock().await.remove(id);

        let vm = retained_vm.ok_or_else(|| {
            OciError::InvalidConfig(format!(
                "no active VM handle for container '{id}'; container may have already exited"
            ))
        })?;

        let stack_id = self.container_stack.lock().await.get(id).cloned();
        let is_stack_container = stack_id.is_some();
        let activation_guard = if let Some(stack_id) = stack_id.as_deref() {
            Some(self.acquire_stack_activation_guard(stack_id).await)
        } else {
            None
        };
        let effective_grace = grace_period.unwrap_or(STOP_GRACE_PERIOD);
        let cleanup_pending = self.overlay_cleanup_is_pending(id, transaction.generation());
        let exit_code = stop_or_reuse_exit_code(
            &*vm,
            id,
            &container.status,
            cleanup_pending,
            force,
            effective_grace,
            signal,
        )
        .await?;
        let lifecycle = self.active_lifecycle.lock().await.get(id).copied();
        self.stop_log_rotation_task(id).await;

        if is_stack_container {
            if let Err(error) = stack_vm::shutdown_container_cleanup_transition(
                self,
                id,
                transaction.generation(),
                || async {
                    vm.oci_delete(id.to_string(), true)
                        .await
                        .map_err(OciError::from)
                },
                || async {
                    self.teardown_owned_stack_container_overlay(&vm, id, transaction.generation())
                        .await
                },
            )
            .await
            {
                container.host_pid = None;
                container.status = ContainerStatus::Stopped { exit_code };
                container.stopped_unix_secs = Some(current_unix_secs());
                let persist_error = self.persist_owned(transaction, container.clone()).err();
                let mut message = format!(
                    "container '{id}' stop cleanup failed; retained VM, stack routing, metadata, and rootfs for retry: {error}"
                );
                if let Some(persist_error) = persist_error {
                    message.push_str(&format!(
                        "; could not persist stopped state: {persist_error}"
                    ));
                }
                return Err(OciError::InvalidConfig(message));
            }
        } else if !self.overlay_cleanup_is_pending(id, transaction.generation()) {
            if let Err(error) = vm.oci_delete(id.to_string(), true).await {
                container.host_pid = None;
                container.status = ContainerStatus::Stopped { exit_code };
                container.stopped_unix_secs = Some(current_unix_secs());
                let persist_error = self.persist_owned(transaction, container.clone()).err();
                let mut message = format!(
                    "cannot stop container '{id}': OCI delete failed; retained VM and stack routing for cleanup retry: {error}"
                );
                if let Some(persist_error) = persist_error {
                    message.push_str(&format!(
                        "; could not persist stopped state: {persist_error}"
                    ));
                }
                return Err(OciError::InvalidConfig(message));
            }
            self.mark_overlay_cleanup_pending(id, transaction.generation());
        }
        tracing::debug!(container_id = %id, "stop_container: oci_delete succeeded");
        drop(activation_guard);

        // Keep the generation-scoped completion marker and recovery ownership
        // published through all remaining fallible cleanup. A retry can then
        // resume without repeating OCI delete or overlay teardown even if the
        // final stopped-state publication fails.
        container.host_pid = None;
        container.status = ContainerStatus::Stopped { exit_code };
        container.stopped_unix_secs = Some(current_unix_secs());

        // Only tear down the VM if the container does NOT belong to a shared
        // stack VM. Retain both the VM and port-forward registry until every
        // fallible cleanup step succeeds, while still attempting VM stop after
        // a relay shutdown failure.
        if !is_stack_container {
            let mut cleanup_failures = Vec::new();
            {
                let mut port_forwards = self.port_forwards.lock().await;
                if let Some(pf) = port_forwards.get_mut(id) {
                    if let Err(error) = pf.shutdown().await {
                        cleanup_failures.push(error.to_string());
                    }
                }
            }
            if !self.container_vm_stop_is_complete(id, transaction.generation()) {
                match vm.stop().await {
                    Ok(()) => {
                        self.mark_container_vm_stop_complete(id, transaction.generation());
                    }
                    Err(error) => cleanup_failures.push(format!("VM stop failed: {error}")),
                }
            }
            if !cleanup_failures.is_empty() {
                return Err(OciError::InvalidConfig(format!(
                    "container '{id}' stopped but retained VM and port forwarding for cleanup retry: {}",
                    cleanup_failures.join("; ")
                )));
            }
            self.port_forwards.lock().await.remove(id);
        }

        // Only remove rootfs for non-stack containers. For stack containers the
        // shared VM's VirtioFS cache holds stale metadata after host-side deletion,
        // causing recreates to fail (overlay sees empty lowerdir). The rootfs will
        // be cleaned up by remove_container or overwritten by a subsequent create.
        if !is_stack_container {
            if let Some(rootfs_path) = container.rootfs_path.take() {
                let _ = fs::remove_dir_all(rootfs_path);
            }
        }

        self.persist_stopped_and_commit_cleanup(transaction, container.clone())
            .await?;

        if lifecycle.is_some_and(|state| state.auto_remove) {
            // Keep one-off semantics best-effort: cleanup failure should not
            // mask a successful stop result.
            if let Err(err) = self.remove_container_in_transaction(id, transaction).await {
                warn!(container_id = %id, error = %err, "auto-remove cleanup failed after stop");
            }
        }

        Ok(container)
    }

    /// Remove unused manifest/config metadata and stale unpacked layer directories.
    pub fn prune_images(&self) -> Result<PruneResult, OciError> {
        self.store.prune_images().map_err(Into::into)
    }

    /// Pull an image reference into local storage.
    pub async fn pull(&self, image: &str) -> Result<ImageId, OciError> {
        Ok(self.puller.pull(image, &self.config.auth).await?)
    }

    /// Pick backend from image reference and optional override.
    pub fn select_backend(image_ref: &str, force_macos: bool) -> RuntimeBackend {
        if force_macos || image_ref.starts_with("macos:") {
            RuntimeBackend::MacOS
        } else {
            RuntimeBackend::Linux
        }
    }

    /// Pull an image, assemble its rootfs and execute a command.
    pub async fn run(&self, image: &str, mut run: RunConfig) -> Result<ExecOutput, OciError> {
        let mut transaction = self.begin_container_create(&mut run, None).await?;
        let runtime = self.clone();
        let image = image.to_string();
        // The worker owns the lifecycle transaction. Dropping the public
        // future therefore detaches a task that still completes VM shutdown,
        // rootfs cleanup, metadata finalization, and generation release.
        tokio::spawn(async move {
            runtime
                .run_in_transaction(&image, run, &mut transaction)
                .await
        })
        .await
        .map_err(|error| {
            OciError::InvalidConfig(format!(
                "one-shot container lifecycle task failed while retaining its owned generation: {error}"
            ))
        })?
    }

    async fn run_in_transaction(
        &self,
        image: &str,
        run: RunConfig,
        transaction: &mut ContainerLifecycleTransaction,
    ) -> Result<ExecOutput, OciError> {
        if matches!(Self::select_backend(image, false), RuntimeBackend::MacOS) {
            return Err(OciError::InvalidConfig(
                "macos backend is not supported by Runtime::run".to_string(),
            ));
        }

        let container_id = transaction.container_id().to_string();
        validate_container_id(&container_id)?;
        let image_id = self.pull(image).await?;

        let created_unix_secs = current_unix_secs();
        let mut container = ContainerInfo {
            id: container_id.clone(),
            image: image.to_string(),
            image_id: image_id.0.clone(),
            status: ContainerStatus::Created,
            created_unix_secs,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(process::id()),
        };

        // Resolve every fallible image/run/lifecycle option before publishing
        // Created metadata. If any of these steps rejects the request, dropping
        // the still-unpublished transaction releases its exact generation.
        let image_config = parse_image_config_summary_from_store(&self.store, &image_id.0)?;
        let run = resolve_run_config(image_config, run, &container_id)?;
        let lifecycle = resolve_container_lifecycle(
            &run.oci_annotations,
            ContainerLifecycleClass::Ephemeral,
            true,
        )?;
        self.persist_owned(transaction, container.clone())?;

        let rootfs_dir = match self
            .assemble_rootfs_in_transaction(&image_id.0, transaction)
            .await
        {
            Ok(rootfs_dir) => rootfs_dir,
            Err(err) => {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.persist_owned(transaction, container)?;
                self.finalize_one_off_cleanup(&container_id, lifecycle.auto_remove, transaction)
                    .await;
                return Err(err);
            }
        };

        container.rootfs_path = Some(rootfs_dir.clone());
        container.status = ContainerStatus::Running;
        container.started_unix_secs = Some(current_unix_secs());
        container.host_pid = Some(process::id());
        self.persist_owned(transaction, container.clone())?;
        self.track_active_lifecycle(container_id.clone(), lifecycle)
            .await;

        let output = match run.execution_mode {
            ExecutionMode::GuestExec => self.run_rootfs(&rootfs_dir, run).await,
            ExecutionMode::OciRuntime => {
                self.run_rootfs_with_oci_runtime(&rootfs_dir, run, &container_id)
                    .await
            }
        };

        // Each transient runner removes only its pointer-identical recovery
        // route after terminal VM proof. An unconditional removal here could
        // race a later public rootfs run that legitimately reused the ID.
        self.cleanup_owned_rootfs(transaction, rootfs_dir.as_ref());

        container.status = match &output {
            Ok(exec_output) => ContainerStatus::Stopped {
                exit_code: exec_output.exit_code,
            },
            Err(_) => ContainerStatus::Stopped { exit_code: -1 },
        };
        container.stopped_unix_secs = Some(current_unix_secs());
        container.host_pid = None;

        self.persist_owned(transaction, container)?;
        self.finalize_one_off_cleanup(&container_id, lifecycle.auto_remove, transaction)
            .await;

        output
    }

    /// Create and start a long-lived container from an OCI image.
    ///
    /// Pulls the image, assembles its rootfs, boots a Linux VM, and runs the
    /// OCI create/start lifecycle. The container remains running after this
    /// call returns and can be accessed via [`exec_container`](Self::exec_container),
    /// [`stop_container`](Self::stop_container), and
    /// [`remove_container`](Self::remove_container).
    ///
    /// Returns the container identifier.
    pub async fn create_container(
        &self,
        image: &str,
        mut run: RunConfig,
    ) -> Result<String, OciError> {
        let mut transaction = self.begin_container_create(&mut run, None).await?;
        self.create_container_in_transaction(image, run, &mut transaction)
            .await
    }

    pub(crate) async fn create_container_in_transaction(
        &self,
        image: &str,
        run: RunConfig,
        transaction: &mut ContainerLifecycleTransaction,
    ) -> Result<String, OciError> {
        if matches!(Self::select_backend(image, false), RuntimeBackend::MacOS) {
            return Err(OciError::InvalidConfig(
                "macos backend is not supported by Runtime::create_container".to_string(),
            ));
        }

        let container_id = transaction.container_id().to_string();
        validate_container_id(&container_id)?;
        let image_id = self.pull(image).await?;

        let created_unix_secs = current_unix_secs();
        let mut container = ContainerInfo {
            id: container_id.clone(),
            image: image.to_string(),
            image_id: image_id.0.clone(),
            status: ContainerStatus::Created,
            created_unix_secs,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(process::id()),
        };

        self.persist_owned(transaction, container.clone())?;

        let image_config = parse_image_config_summary_from_store(&self.store, &image_id.0)?;
        let run = resolve_run_config(image_config, run, &container_id)?;
        let lifecycle = resolve_container_lifecycle(
            &run.oci_annotations,
            ContainerLifecycleClass::Workspace,
            false,
        )?;

        let rootfs_dir = match self
            .assemble_rootfs_in_transaction(&image_id.0, transaction)
            .await
        {
            Ok(rootfs_dir) => rootfs_dir,
            Err(err) => {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.persist_owned(transaction, container)?;
                return Err(err);
            }
        };

        container.rootfs_path = Some(rootfs_dir.clone());
        self.persist_owned(transaction, container.clone())?;

        match self
            .boot_and_start_container(&rootfs_dir, &run, &container_id)
            .await
        {
            Ok(vm) => {
                container.status = ContainerStatus::Running;
                container.started_unix_secs = Some(current_unix_secs());
                container.host_pid = Some(process::id());
                self.persist_owned(transaction, container)?;
                self.track_active_lifecycle(container_id.clone(), lifecycle)
                    .await;
                self.container_exec_bindings.lock().await.insert(
                    container_id.clone(),
                    ContainerExecBinding {
                        vm,
                        defaults: ContainerExecDefaults::from(&run),
                        generation: transaction.generation(),
                    },
                );
                Ok(container_id)
            }
            Err(err) => {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.persist_owned(transaction, container)?;
                self.cleanup_owned_rootfs(transaction, rootfs_dir.as_ref());
                Err(err)
            }
        }
    }
}
