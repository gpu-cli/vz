use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, process};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{debug, warn};
use vz::Vm;
use vz::protocol::{ExecEvent, ExecOutput};
use vz::{NetworkConfig, SharedDirConfig};
use vz_image::{
    ImageConfigSummary, ImageId, ImagePuller, ImageStore, parse_image_config_summary_from_store,
};
use vz_linux::{
    EnsureKernelOptions, ExecOptions, KernelPaths, LinuxError, LinuxVm, LinuxVmConfig,
    OciExecOptions, ensure_kernel_with_options,
};
use vz_oci::bundle::{BundleMount, BundleSpec, write_oci_bundle};
use vz_oci::container_store::{ContainerInfo, ContainerStatus, ContainerStore};

use tokio::sync::Mutex;
use vz::protocol::OciContainerState;

use crate::config::{
    ExecConfig, ExecutionMode, MountAccess, MountSpec, MountType, OciRuntimeKind, PortMapping,
    PortProtocol, RunConfig, RuntimeBackend, RuntimeConfig,
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
use self::networking::stop_via_oci_runtime;
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
use self::oci_lifecycle::{
    OciLifecycleFuture, OciLifecycleOps, build_log_rotation_script, parse_signal_number,
    run_oci_lifecycle,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ComposeLogRotation {
    max_size_bytes: u64,
    max_files: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InteractiveExecEvent {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(i32),
}

#[derive(Clone)]
struct InteractiveExecSession {
    vm: Arc<LinuxVm>,
    guest_exec_id: u64,
    pty_enabled: bool,
}

type ContainerExecEnvMap = HashMap<String, Vec<(String, String)>>;

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
    stack_vms: Arc<Mutex<HashMap<String, Arc<LinuxVm>>>>,
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
    exec_sessions: Arc<Mutex<HashMap<String, InteractiveExecSession>>>,
    /// Resolved container environment captured at create/start time.
    ///
    /// Used to provide docker-compatible exec behavior where ad-hoc exec
    /// commands inherit the container's configured environment by default.
    container_exec_env: Arc<Mutex<ContainerExecEnvMap>>,
    /// VM instances that already ran interactive PTY prerequisite setup.
    ///
    /// Keyed by `Arc<LinuxVm>` pointer identity (`Arc::as_ptr` cast to usize)
    /// so prep runs once per live VM instance.
    interactive_pty_prep_vms: Arc<Mutex<HashSet<usize>>>,
}

impl Runtime {
    /// Create a runtime instance.
    pub fn new(config: RuntimeConfig) -> Self {
        let mut config = config;
        config.data_dir = expand_home_dir(&config.data_dir);

        let store = ImageStore::new(config.data_dir.clone());
        let container_store = ContainerStore::new(config.data_dir.clone());
        let puller = ImagePuller::new(store.clone());

        let runtime = Self {
            config,
            store,
            container_store,
            puller,
            vm_handles: Arc::new(Mutex::new(HashMap::new())),
            stack_vms: Arc::new(Mutex::new(HashMap::new())),
            container_stack: Arc::new(Mutex::new(HashMap::new())),
            port_forwards: Arc::new(Mutex::new(HashMap::new())),
            stack_port_forwards: Arc::new(Mutex::new(HashMap::new())),
            active_lifecycle: Arc::new(Mutex::new(HashMap::new())),
            log_rotation_tasks: Arc::new(Mutex::new(HashMap::new())),
            exec_sessions: Arc::new(Mutex::new(HashMap::new())),
            container_exec_env: Arc::new(Mutex::new(HashMap::new())),
            interactive_pty_prep_vms: Arc::new(Mutex::new(HashSet::new())),
        };

        runtime.reconcile_stale_containers();
        runtime.cleanup_orphaned_rootfs();

        runtime
    }

    /// Return configured data directory.
    pub fn data_dir(&self) -> &PathBuf {
        &self.config.data_dir
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
        .map_err(|err| OciError::InvalidConfig(err.to_string()))
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

    /// Remove container metadata and best-effort rootfs artifacts.
    ///
    /// If a VM handle is still active for this container, sends an OCI delete
    /// to the guest runtime before cleaning up host metadata.
    pub async fn remove_container(&self, id: &str) -> Result<(), OciError> {
        let containers = self.container_store.load_all().map_err(OciError::from)?;
        let container = containers
            .into_iter()
            .find(|container| container.id == id)
            .ok_or_else(|| {
                OciError::Storage(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("container '{id}' not found"),
                ))
            })?;

        if matches!(container.status, ContainerStatus::Running) {
            return Err(OciError::InvalidConfig(format!(
                "cannot remove running container '{id}'; stop it first"
            )));
        }

        // Shut down port forwarding for this container.
        if let Some(pf) = self.port_forwards.lock().await.remove(id) {
            pf.shutdown().await;
        }
        self.stop_log_rotation_task(id).await;
        self.container_exec_env.lock().await.remove(id);

        // Best-effort OCI delete via guest runtime if VM is still up.
        // Try the per-container handle first; fall back to the shared stack VM
        // (the per-container handle may have been removed by stop_container).
        let vm = self.vm_handles.lock().await.remove(id);
        let stack_id = self.container_stack.lock().await.remove(id);
        if let Some(vm) = vm {
            match vm.oci_delete(id.to_string(), true).await {
                Ok(_) => {
                    tracing::debug!(container_id = %id, "remove_container: oci_delete via vm_handle succeeded")
                }
                Err(e) => {
                    tracing::warn!(container_id = %id, error = %e, "remove_container: oci_delete via vm_handle failed")
                }
            }
        } else if let Some(sid) = &stack_id {
            if let Some(vm) = self.stack_vms.lock().await.get(sid) {
                match vm.oci_delete(id.to_string(), true).await {
                    Ok(_) => {
                        tracing::debug!(container_id = %id, stack_id = %sid, "remove_container: oci_delete via stack_vm succeeded")
                    }
                    Err(e) => {
                        tracing::warn!(container_id = %id, stack_id = %sid, error = %e, "remove_container: oci_delete via stack_vm failed")
                    }
                }
            } else {
                tracing::warn!(container_id = %id, stack_id = %sid, "remove_container: stack_vm not found");
            }
        } else {
            tracing::debug!(container_id = %id, "remove_container: no vm_handle or stack_id, skipping oci_delete");
        }
        self.active_lifecycle.lock().await.remove(id);

        self.container_store.remove(id).map_err(OciError::from)?;

        if let Some(path) = container.rootfs_path {
            let _ = fs::remove_dir_all(path);
        }

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
        let mut container = self
            .container_store
            .load_all()
            .map_err(OciError::from)?
            .into_iter()
            .find(|item| item.id == id)
            .ok_or_else(|| {
                OciError::Storage(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("container '{id}' not found"),
                ))
            })?;

        if !matches!(container.status, ContainerStatus::Running) {
            self.active_lifecycle.lock().await.remove(id);
            self.stop_log_rotation_task(id).await;
            self.container_exec_env.lock().await.remove(id);
            return Ok(container);
        }

        let vm = self
            .vm_handles
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| {
                OciError::InvalidConfig(format!(
                    "no active VM handle for container '{id}'; container may have already exited"
                ))
            })?;

        let effective_grace = grace_period.unwrap_or(STOP_GRACE_PERIOD);
        let exit_code = stop_via_oci_runtime(&*vm, id, force, effective_grace, signal).await?;
        let lifecycle = self.active_lifecycle.lock().await.remove(id);
        self.stop_log_rotation_task(id).await;
        self.container_exec_env.lock().await.remove(id);

        // Best-effort OCI delete.
        match vm.oci_delete(id.to_string(), true).await {
            Ok(_) => tracing::debug!(container_id = %id, "stop_container: oci_delete succeeded"),
            Err(e) => {
                tracing::warn!(container_id = %id, error = %e, "stop_container: oci_delete failed (best-effort)")
            }
        }

        // Only tear down the VM if the container does NOT belong to a shared stack VM.
        let is_stack_container = self.container_stack.lock().await.contains_key(id);
        if !is_stack_container {
            let _ = vm.stop().await;
        }
        self.vm_handles.lock().await.remove(id);
        // Keep container_stack entry so remove_container can find the stack VM
        // for a retry oci_delete if the best-effort delete above failed.

        // Shut down port forwarding for this container.
        if let Some(pf) = self.port_forwards.lock().await.remove(id) {
            pf.shutdown().await;
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

        container.host_pid = None;
        container.status = ContainerStatus::Stopped { exit_code };
        container.stopped_unix_secs = Some(current_unix_secs());
        self.container_store
            .upsert(container.clone())
            .map_err(OciError::from)?;

        if lifecycle.is_some_and(|state| state.auto_remove) {
            // Keep one-off semantics best-effort: cleanup failure should not
            // mask a successful stop result.
            if let Err(err) = self.remove_container(id).await {
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
    pub async fn run(&self, image: &str, run: RunConfig) -> Result<ExecOutput, OciError> {
        if matches!(Self::select_backend(image, false), RuntimeBackend::MacOS) {
            return Err(OciError::InvalidConfig(
                "macos backend is not supported by Runtime::run".to_string(),
            ));
        }

        let image_id = self.pull(image).await?;
        let container_id = run.container_id.clone().unwrap_or_else(new_container_id);

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

        self.container_store
            .upsert(container.clone())
            .map_err(OciError::from)?;

        // Spawn rootfs assembly in background so image config parsing runs
        // concurrently with the heavy layer extraction I/O.
        let rootfs_handle = self.store.spawn_assemble_rootfs(&image_id.0, &container_id);

        // Parse image config concurrently with rootfs assembly (reads from
        // local store, no dependency on assembled rootfs).
        let image_config = parse_image_config_summary_from_store(&self.store, &image_id.0)?;
        let run = resolve_run_config(image_config, run, &container_id)?;
        let lifecycle = resolve_container_lifecycle(
            &run.oci_annotations,
            ContainerLifecycleClass::Ephemeral,
            true,
        )?;

        // Await rootfs assembly before proceeding to VM boot.
        let rootfs_dir = match rootfs_handle.await {
            Ok(Ok(rootfs_dir)) => rootfs_dir,
            Ok(Err(err)) => {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                self.finalize_one_off_cleanup(&container_id, lifecycle.auto_remove)
                    .await;
                return Err(err.into());
            }
            Err(join_err) => {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                self.finalize_one_off_cleanup(&container_id, lifecycle.auto_remove)
                    .await;
                return Err(OciError::Storage(std::io::Error::other(
                    join_err.to_string(),
                )));
            }
        };

        container.rootfs_path = Some(rootfs_dir.clone());
        container.status = ContainerStatus::Running;
        container.started_unix_secs = Some(current_unix_secs());
        container.host_pid = Some(process::id());
        self.container_store
            .upsert(container.clone())
            .map_err(OciError::from)?;
        self.track_active_lifecycle(container_id.clone(), lifecycle)
            .await;

        let output = match run.execution_mode {
            ExecutionMode::GuestExec => self.run_rootfs(&rootfs_dir, run).await,
            ExecutionMode::OciRuntime => {
                self.run_rootfs_with_oci_runtime(&rootfs_dir, run, &container_id)
                    .await
            }
        };

        // Deregister VM handle after run completes.
        self.vm_handles.lock().await.remove(&container_id);
        self.cleanup_rootfs_dir(rootfs_dir.as_ref());

        container.status = match &output {
            Ok(exec_output) => ContainerStatus::Stopped {
                exit_code: exec_output.exit_code,
            },
            Err(_) => ContainerStatus::Stopped { exit_code: -1 },
        };
        container.stopped_unix_secs = Some(current_unix_secs());
        container.host_pid = None;

        self.container_store
            .upsert(container)
            .map_err(OciError::from)?;
        self.finalize_one_off_cleanup(&container_id, lifecycle.auto_remove)
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
    pub async fn create_container(&self, image: &str, run: RunConfig) -> Result<String, OciError> {
        if matches!(Self::select_backend(image, false), RuntimeBackend::MacOS) {
            return Err(OciError::InvalidConfig(
                "macos backend is not supported by Runtime::create_container".to_string(),
            ));
        }

        let image_id = self.pull(image).await?;
        let container_id = run.container_id.clone().unwrap_or_else(new_container_id);

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

        self.container_store
            .upsert(container.clone())
            .map_err(OciError::from)?;

        // Spawn rootfs assembly in background so image config parsing runs
        // concurrently with the heavy layer extraction I/O.
        let rootfs_handle = self.store.spawn_assemble_rootfs(&image_id.0, &container_id);

        // Parse image config concurrently with rootfs assembly.
        let image_config = parse_image_config_summary_from_store(&self.store, &image_id.0)?;
        let run = resolve_run_config(image_config, run, &container_id)?;
        let lifecycle = resolve_container_lifecycle(
            &run.oci_annotations,
            ContainerLifecycleClass::Workspace,
            false,
        )?;

        // Await rootfs assembly before booting the VM.
        let rootfs_dir = match rootfs_handle.await {
            Ok(Ok(rootfs_dir)) => rootfs_dir,
            Ok(Err(err)) => {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                return Err(err.into());
            }
            Err(join_err) => {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                return Err(OciError::Storage(std::io::Error::other(
                    join_err.to_string(),
                )));
            }
        };

        container.rootfs_path = Some(rootfs_dir.clone());
        self.container_store
            .upsert(container.clone())
            .map_err(OciError::from)?;

        match self
            .boot_and_start_container(&rootfs_dir, &run, &container_id)
            .await
        {
            Ok(()) => {
                container.status = ContainerStatus::Running;
                container.started_unix_secs = Some(current_unix_secs());
                container.host_pid = Some(process::id());
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                self.track_active_lifecycle(container_id.clone(), lifecycle)
                    .await;
                self.container_exec_env
                    .lock()
                    .await
                    .insert(container_id.clone(), run.env.clone());
                Ok(container_id)
            }
            Err(err) => {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                self.cleanup_rootfs_dir(rootfs_dir.as_ref());
                Err(err)
            }
        }
    }
}
