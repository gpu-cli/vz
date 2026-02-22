use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, process};

use crate::bundle::{BundleMount, BundleSpec, write_oci_bundle};
use crate::container_store::{ContainerInfo, ContainerStatus, ContainerStore};
use crate::image::{
    ImageConfigSummary, ImageId, ImagePuller, parse_image_config_summary_from_store,
};
use crate::store::ImageStore;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::warn;
use vz::Vm;
use vz::protocol::ExecOutput;
use vz::{NetworkConfig, SharedDirConfig};
use vz_linux::{
    EnsureKernelOptions, ExecOptions, KernelPaths, LinuxError, LinuxVm, LinuxVmConfig,
    OciExecOptions, ensure_kernel_with_options,
};

use tokio::sync::Mutex;
use vz::protocol::OciContainerState;

use crate::config::{
    ExecConfig, ExecutionMode, MountAccess, MountSpec, MountType, OciRuntimeKind, PortMapping,
    PortProtocol, RunConfig, RuntimeBackend, RuntimeConfig,
};
use crate::error::OciError;
use crate::store::{ImageInfo, PruneResult};

const STOP_GRACE_PERIOD: Duration = Duration::from_secs(10);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(100);
const OCI_RUNTIME_BIN_SHARE_TAG: &str = "oci-runtime-bin";
const OCI_DEFAULT_GUEST_STATE_DIR: &str = "/run/vz-oci";
const OCI_BUNDLE_DIRNAME: &str = "bundles";

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
        };

        runtime.reconcile_stale_containers();
        runtime.cleanup_orphaned_rootfs();

        runtime
    }

    /// Return configured data directory.
    pub fn data_dir(&self) -> &PathBuf {
        &self.config.data_dir
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

        // Best-effort OCI delete via guest runtime if VM is still up.
        if let Some(vm) = self.vm_handles.lock().await.remove(id) {
            let _ = vm.oci_delete(id.to_string(), true).await;
        }

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
    pub async fn stop_container(&self, id: &str, force: bool) -> Result<ContainerInfo, OciError> {
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

        let exit_code = stop_via_oci_runtime(&*vm, id, force, STOP_GRACE_PERIOD).await?;

        // Best-effort OCI delete.
        let _ = vm.oci_delete(id.to_string(), true).await;

        // Only tear down the VM if the container does NOT belong to a shared stack VM.
        let is_stack_container = self.container_stack.lock().await.contains_key(id);
        if !is_stack_container {
            let _ = vm.stop().await;
        }
        self.vm_handles.lock().await.remove(id);
        self.container_stack.lock().await.remove(id);

        // Shut down port forwarding for this container.
        if let Some(pf) = self.port_forwards.lock().await.remove(id) {
            pf.shutdown().await;
        }

        if let Some(rootfs_path) = container.rootfs_path.take() {
            let _ = fs::remove_dir_all(rootfs_path);
        }

        container.host_pid = None;
        container.status = ContainerStatus::Stopped { exit_code };
        container.stopped_unix_secs = Some(current_unix_secs());
        self.container_store
            .upsert(container.clone())
            .map_err(OciError::from)?;

        Ok(container)
    }

    /// Remove unused manifest/config metadata and stale unpacked layer directories.
    pub fn prune_images(&self) -> Result<PruneResult, OciError> {
        self.store.prune_images().map_err(Into::into)
    }

    /// Pull an image reference into local storage.
    pub async fn pull(&self, image: &str) -> Result<ImageId, OciError> {
        self.puller.pull(image, &self.config.auth).await
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

        let rootfs_dir = match self
            .store
            .assemble_rootfs_async(&image_id.0, &container_id)
            .await
        {
            Ok(rootfs_dir) => rootfs_dir,
            Err(err) => {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                return Err(err.into());
            }
        };

        container.rootfs_path = Some(rootfs_dir.clone());
        container.status = ContainerStatus::Running;
        container.started_unix_secs = Some(current_unix_secs());
        container.host_pid = Some(process::id());
        self.container_store
            .upsert(container.clone())
            .map_err(OciError::from)?;

        let image_config = parse_image_config_summary_from_store(&self.store, &image_id.0)?;
        let run = resolve_run_config(image_config, run, &container_id)?;

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

        let rootfs_dir = match self
            .store
            .assemble_rootfs_async(&image_id.0, &container_id)
            .await
        {
            Ok(rootfs_dir) => rootfs_dir,
            Err(err) => {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                return Err(err.into());
            }
        };

        container.rootfs_path = Some(rootfs_dir.clone());
        self.container_store
            .upsert(container.clone())
            .map_err(OciError::from)?;

        let image_config = parse_image_config_summary_from_store(&self.store, &image_id.0)?;
        let run = resolve_run_config(image_config, run, &container_id)?;

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

    // ── Shared stack VM API ──────────────────────────────────────────

    /// Return the rootfs store directory where assembled rootfs trees are stored.
    ///
    /// This is the parent directory of all per-container rootfs directories.
    /// For a shared stack VM, it is used as the VirtioFS `rootfs` share so
    /// that each container's assembled rootfs appears at `/<container_id>/`
    /// inside the guest.
    pub fn rootfs_store_dir(&self) -> PathBuf {
        self.config.data_dir.join("rootfs")
    }

    /// Boot a shared VM for a multi-service stack.
    ///
    /// The VM runs a single kernel with the guest agent, and multiple OCI
    /// containers can be created inside it via
    /// [`create_container_in_stack`](Self::create_container_in_stack).
    ///
    /// The rootfs store directory is shared via VirtioFS so that each
    /// container's assembled rootfs appears at `/<container_id>/` inside
    /// the guest after overlay+chroot.
    ///
    /// # Errors
    ///
    /// Returns an error if a shared VM is already running for `stack_id`, or
    /// if the VM fails to boot.
    pub async fn boot_shared_vm(
        &self,
        stack_id: &str,
        ports: Vec<PortMapping>,
    ) -> Result<(), OciError> {
        // Guard against double-boot.
        if self.stack_vms.lock().await.contains_key(stack_id) {
            return Err(OciError::InvalidConfig(format!(
                "shared VM already running for stack '{stack_id}'"
            )));
        }

        let rootfs_store = self.rootfs_store_dir();
        fs::create_dir_all(&rootfs_store)?;

        let kernel = ensure_kernel_with_options(EnsureKernelOptions {
            install_dir: self.config.linux_install_dir.clone(),
            bundle_dir: self.config.linux_bundle_dir.clone(),
            require_exact_agent_version: self.config.require_exact_agent_version,
        })
        .await?;

        let runtime_binary = resolve_oci_runtime_binary_path(
            self.config.guest_oci_runtime,
            self.config.guest_oci_runtime_path.as_deref(),
            &kernel,
        )?;

        let mut vm_config =
            LinuxVmConfig::new(kernel.kernel, kernel.initramfs).with_rootfs_dir(rootfs_store);
        vm_config
            .shared_dirs
            .push(make_oci_runtime_share(&runtime_binary)?);
        vm_config.cpus = self.config.default_cpus;
        vm_config.memory_mb = self.config.default_memory_mb;

        // Debug: capture serial log for shared VM diagnostics.
        if let Ok(log_path) = std::env::var("VZ_STACK_SERIAL_LOG") {
            vm_config.serial_log_file = Some(std::path::PathBuf::from(log_path));
        }

        if !self.config.default_network_enabled {
            vm_config.network = Some(NetworkConfig::None);
        }

        let vm = LinuxVm::create(vm_config).await?;
        vm.start().await?;

        if let Err(err) = vm.wait_for_agent(self.config.agent_ready_timeout).await {
            let _ = vm.stop().await;
            return Err(err.into());
        }

        let vm = Arc::new(vm);

        // Set up port forwarding for all services' ports.
        let port_forwarding = match start_port_forwarding(vm.inner_shared(), &ports).await {
            Ok(pf) => pf,
            Err(err) => {
                let _ = vm.stop().await;
                return Err(err);
            }
        };

        if let Some(pf) = port_forwarding {
            self.stack_port_forwards
                .lock()
                .await
                .insert(stack_id.to_string(), pf);
        }

        self.stack_vms.lock().await.insert(stack_id.to_string(), vm);

        Ok(())
    }

    /// Create and start an OCI container inside a shared stack VM.
    ///
    /// The VM must have been booted via [`boot_shared_vm`](Self::boot_shared_vm).
    /// This method pulls the image, assembles its rootfs, writes an OCI bundle,
    /// and runs the OCI create/start lifecycle inside the shared VM.
    ///
    /// Returns the container identifier.
    pub async fn create_container_in_stack(
        &self,
        stack_id: &str,
        image: &str,
        run: RunConfig,
    ) -> Result<String, OciError> {
        let vm = self
            .stack_vms
            .lock()
            .await
            .get(stack_id)
            .cloned()
            .ok_or_else(|| {
                OciError::InvalidConfig(format!(
                    "no shared VM running for stack '{stack_id}'; call boot_shared_vm first"
                ))
            })?;

        let image_id = self.pull(image).await?;
        let container_id = run.container_id.clone().unwrap_or_else(new_container_id);
        tracing::debug!(stack_id = %stack_id, container_id = %container_id, image_id = %image_id.0, "create_container_in_stack: starting");

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

        tracing::debug!("step 1: assemble_rootfs_async");
        let rootfs_dir = match self
            .store
            .assemble_rootfs_async(&image_id.0, &container_id)
            .await
        {
            Ok(rootfs_dir) => rootfs_dir,
            Err(err) => {
                tracing::error!(error = %err, "step 1 FAILED: assemble_rootfs_async");
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                return Err(err.into());
            }
        };
        tracing::debug!(rootfs_dir = %rootfs_dir.display(), "step 1 OK");

        container.rootfs_path = Some(rootfs_dir.clone());
        self.container_store
            .upsert(container.clone())
            .map_err(OciError::from)?;

        tracing::debug!("step 2: parse_image_config_summary_from_store");
        let image_config = parse_image_config_summary_from_store(&self.store, &image_id.0)
            .map_err(|e| { tracing::error!(error = %e, "step 2 FAILED"); e })?;
        tracing::debug!("step 2 OK");
        let run = resolve_run_config(image_config, run, &container_id)?;

        // Build OCI bundle referencing the assembled rootfs (shared via VirtioFS).
        //
        // In a shared VM, the rootfs store directory is the VirtioFS share.
        // Each container's assembled rootfs appears at `/<container_id>/` inside
        // the guest after overlay+chroot. The bundle is written under the
        // container's rootfs dir so its guest path is `/<container_id>/<bundle>`.
        let oci_container_id = run
            .container_id
            .clone()
            .unwrap_or_else(|| container_id.to_string());
        let bundle_guest_root = oci_bundle_guest_root(self.config.guest_state_dir.as_deref())?;
        let bundle_relative_path = oci_bundle_guest_path(&bundle_guest_root, &oci_container_id);
        // Host: <data_dir>/rootfs/<container_id>/<bundle_path>
        let bundle_host_dir = oci_bundle_host_dir(&rootfs_dir, &bundle_relative_path);
        // Guest: /vz-rootfs/<container_id>/<bundle_path>
        let bundle_guest_path = format!("/vz-rootfs/{container_id}{bundle_relative_path}");
        tracing::debug!(bundle_host_dir = %bundle_host_dir.display(), bundle_guest_path = %bundle_guest_path, "step 3: write bundle");

        let bundle_cmd = run
            .init_process
            .clone()
            .or_else(|| {
                if run.cmd.is_empty() {
                    None
                } else {
                    Some(run.cmd.clone())
                }
            })
            .ok_or_else(|| {
                OciError::InvalidConfig(
                    "container requires a command (init_process or cmd)".to_string(),
                )
            })?;

        let bundle_mounts = mount_specs_to_bundle_mounts(&run.mounts)?;

        // Per-container overlay: VirtioFS doesn't support mknod, so we create a
        // guest-side overlay with tmpfs as upperdir for device nodes.
        let vz_rootfs_path = format!("/vz-rootfs/{container_id}");
        tracing::debug!("step 3a: setup per-container overlay in guest");
        let guest_rootfs_path = match setup_guest_container_overlay(
            vm.as_ref(),
            &vz_rootfs_path,
            &container_id,
        )
        .await
        {
            Ok(path) => path,
            Err(err) => {
                tracing::error!(error = %err, "step 3a FAILED: per-container overlay setup");
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                return Err(err);
            }
        };
        tracing::debug!("step 3a OK");

        // extra_hosts are written AFTER the container starts (step 5) via
        // oci_exec inside the container's mount namespace. Writing before
        // start (via guest exec or bind mount) fails due to VirtioFS caching
        // and youki's pivot_root creating an isolated mount tree.

        tracing::debug!("step 3c: write_oci_bundle");
        write_oci_bundle(
            &bundle_host_dir,
            Path::new(&guest_rootfs_path),
            BundleSpec {
                cmd: bundle_cmd,
                env: run.env.clone(),
                cwd: run.working_dir.clone(),
                user: run.user.clone(),
                mounts: bundle_mounts,
                oci_annotations: run.oci_annotations.clone(),
                network_namespace_path: run.network_namespace_path.clone(),
                share_host_network: false,
                cpu_quota: run.cpu_quota,
                cpu_period: run.cpu_period,
                capture_logs: run.capture_logs,
            },
        )
        .map_err(|e| { tracing::error!(error = %e, "step 3c FAILED: write_oci_bundle"); e })?;
        tracing::debug!("step 3c OK");

        // OCI create + start inside the shared VM.
        tracing::debug!("step 4: oci_create + oci_start");
        if let Err(err) = vm
            .oci_create(oci_container_id.clone(), bundle_guest_path.clone())
            .await
        {
            container.status = ContainerStatus::Stopped { exit_code: -1 };
            container.stopped_unix_secs = Some(current_unix_secs());
            container.host_pid = None;
            self.container_store
                .upsert(container)
                .map_err(OciError::from)?;
            self.cleanup_rootfs_dir(rootfs_dir.as_ref());
            return Err(OciError::from(err));
        }

        if let Err(err) = vm.oci_start(oci_container_id.clone()).await {
            let _ = vm.oci_delete(oci_container_id, true).await;
            container.status = ContainerStatus::Stopped { exit_code: -1 };
            container.stopped_unix_secs = Some(current_unix_secs());
            container.host_pid = None;
            self.container_store
                .upsert(container)
                .map_err(OciError::from)?;
            self.cleanup_rootfs_dir(rootfs_dir.as_ref());
            return Err(OciError::from(err));
        }

        // Register VM handle for exec/stop and track stack membership.
        self.vm_handles
            .lock()
            .await
            .insert(container_id.to_string(), Arc::clone(&vm));
        self.container_stack
            .lock()
            .await
            .insert(container_id.to_string(), stack_id.to_string());

        container.status = ContainerStatus::Running;
        container.started_unix_secs = Some(current_unix_secs());
        container.host_pid = Some(process::id());
        self.container_store
            .upsert(container)
            .map_err(OciError::from)?;

        // Step 5: Write /etc/hosts inside the running container via oci_exec.
        // This writes directly into the container's mount namespace after
        // pivot_root, avoiding VirtioFS caching and overlay visibility issues.
        if !run.extra_hosts.is_empty() {
            tracing::debug!("step 5: write /etc/hosts via oci_exec");
            let mut printf_content = String::from("127.0.0.1\\tlocalhost\\n::1\\tlocalhost\\n");
            for (hostname, ip) in &run.extra_hosts {
                printf_content.push_str(&format!("{ip}\\t{hostname}\\n"));
            }
            let hosts_result = tokio::time::timeout(
                Duration::from_secs(30),
                vm.oci_exec(
                    oci_container_id.clone(),
                    "/bin/sh".to_string(),
                    vec![
                        "-c".to_string(),
                        format!("printf '{printf_content}' > /etc/hosts"),
                    ],
                    OciExecOptions::default(),
                ),
            )
            .await;
            match hosts_result {
                Ok(Ok(r)) if r.exit_code == 0 => {
                    tracing::debug!("step 5 OK: /etc/hosts written");
                }
                Ok(Ok(r)) => {
                    tracing::warn!(
                        exit_code = r.exit_code,
                        stderr = %r.stderr.trim(),
                        "step 5: /etc/hosts write returned non-zero"
                    );
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "step 5: /etc/hosts write failed");
                }
                Err(_) => {
                    tracing::warn!("step 5: /etc/hosts write timed out");
                }
            }
        }

        Ok(container_id)
    }

    /// Stop all containers and shut down the shared VM for a stack.
    ///
    /// Each container is stopped via `oci_kill` + `oci_delete`, then the
    /// shared VM is torn down. Container metadata is updated to `Stopped`.
    pub async fn shutdown_shared_vm(&self, stack_id: &str) -> Result<(), OciError> {
        let vm = self
            .stack_vms
            .lock()
            .await
            .remove(stack_id)
            .ok_or_else(|| {
                OciError::InvalidConfig(format!("no shared VM running for stack '{stack_id}'"))
            })?;

        // Find all containers belonging to this stack.
        let stack_containers: Vec<String> = {
            let cs = self.container_stack.lock().await;
            cs.iter()
                .filter(|(_, sid)| *sid == stack_id)
                .map(|(cid, _)| cid.clone())
                .collect()
        };

        // Stop each container via OCI lifecycle.
        for cid in &stack_containers {
            let _ = stop_via_oci_runtime(&*vm, cid, false, STOP_GRACE_PERIOD).await;
            let _ = vm.oci_delete(cid.to_string(), true).await;

            // Update container metadata.
            if let Ok(mut containers) = self.container_store.load_all() {
                if let Some(container) = containers.iter_mut().find(|c| c.id == *cid) {
                    container.status = ContainerStatus::Stopped { exit_code: 0 };
                    container.stopped_unix_secs = Some(current_unix_secs());
                    container.host_pid = None;
                    let _ = self.container_store.upsert(container.clone());
                }
            }
        }

        // Clean up tracking maps.
        {
            let mut vm_handles = self.vm_handles.lock().await;
            let mut cs = self.container_stack.lock().await;
            for cid in &stack_containers {
                vm_handles.remove(cid);
                cs.remove(cid);
            }
        }

        // Shut down port forwarding relays for this stack.
        if let Some(pf) = self.stack_port_forwards.lock().await.remove(stack_id) {
            pf.shutdown().await;
        }

        // Tear down the shared VM.
        let _ = vm.stop().await;

        Ok(())
    }

    /// Check whether a shared VM is running for the given stack.
    pub async fn has_shared_vm(&self, stack_id: &str) -> bool {
        self.stack_vms.lock().await.contains_key(stack_id)
    }

    /// Execute a raw command in the shared VM (not through the OCI runtime).
    ///
    /// Useful for diagnostics, inspecting the guest filesystem, or running
    /// non-containerized commands inside the VM.
    pub async fn exec_in_shared_vm(
        &self,
        stack_id: &str,
        command: String,
        args: Vec<String>,
        timeout: Duration,
    ) -> Result<ExecOutput, OciError> {
        let vm = self
            .stack_vms
            .lock()
            .await
            .get(stack_id)
            .cloned()
            .ok_or_else(|| {
                OciError::InvalidConfig(format!("no shared VM running for stack '{stack_id}'"))
            })?;

        let result = vm.exec_capture(command, args, timeout).await?;

        Ok(ExecOutput {
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
        })
    }

    /// Set up per-service network isolation inside the shared VM.
    ///
    /// Creates a bridge and per-service network namespaces so that
    /// containers can communicate using real IP addresses.
    pub async fn network_setup(
        &self,
        stack_id: &str,
        services: Vec<vz::protocol::NetworkServiceConfig>,
    ) -> Result<(), OciError> {
        let vm = self
            .stack_vms
            .lock()
            .await
            .get(stack_id)
            .cloned()
            .ok_or_else(|| {
                OciError::InvalidConfig(format!("no shared VM running for stack '{stack_id}'"))
            })?;

        vm.network_setup(stack_id.to_string(), services)
            .await
            .map_err(OciError::from)
    }

    /// Tear down per-service network resources inside the shared VM.
    pub async fn network_teardown(
        &self,
        stack_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), OciError> {
        let vm = self
            .stack_vms
            .lock()
            .await
            .get(stack_id)
            .cloned()
            .ok_or_else(|| {
                OciError::InvalidConfig(format!("no shared VM running for stack '{stack_id}'"))
            })?;

        vm.network_teardown(stack_id.to_string(), service_names)
            .await
            .map_err(OciError::from)
    }

    // ── Single-container exec ──────────────────────────────────────

    /// Execute a command inside an already-running container.
    ///
    /// The container must have been created with
    /// [`create_container`](Self::create_container) or be running from a
    /// detached [`run`](Self::run) call.
    pub async fn exec_container(&self, id: &str, exec: ExecConfig) -> Result<ExecOutput, OciError> {
        let vm = self
            .vm_handles
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| {
                OciError::InvalidConfig(format!(
                    "no active VM handle for container '{id}'; container may not be running"
                ))
            })?;

        let (command, args) = exec
            .cmd
            .split_first()
            .ok_or_else(|| OciError::InvalidConfig("exec command must not be empty".to_string()))?;

        let timeout = exec.timeout.unwrap_or(self.config.exec_timeout);

        let result = tokio::time::timeout(
            timeout,
            vm.oci_exec(
                id.to_string(),
                command.clone(),
                args.to_vec(),
                OciExecOptions {
                    env: exec.env,
                    cwd: exec.working_dir,
                    user: exec.user,
                },
            ),
        )
        .await
        .map_err(|_| {
            OciError::InvalidConfig(format!(
                "exec timed out after {:.3}s",
                timeout.as_secs_f64()
            ))
        })??;

        Ok(ExecOutput {
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
        })
    }

    /// Boot a VM, wait for agent, register VM handle, set up port forwarding,
    /// and run OCI create+start (but NOT exec).
    async fn boot_and_start_container(
        &self,
        rootfs_dir: &Path,
        run: &RunConfig,
        container_id: &str,
    ) -> Result<(), OciError> {
        if !rootfs_dir.is_dir() {
            return Err(OciError::InvalidRootfs {
                path: rootfs_dir.to_path_buf(),
            });
        }

        let oci_container_id = run
            .container_id
            .clone()
            .unwrap_or_else(|| container_id.to_string());
        let bundle_guest_root = oci_bundle_guest_root(self.config.guest_state_dir.as_deref())?;
        let bundle_guest_path = oci_bundle_guest_path(&bundle_guest_root, &oci_container_id);
        let bundle_host_dir = oci_bundle_host_dir(rootfs_dir, &bundle_guest_path);

        let bundle_cmd = run
            .init_process
            .clone()
            .or_else(|| {
                if run.cmd.is_empty() {
                    None
                } else {
                    Some(run.cmd.clone())
                }
            })
            .ok_or_else(|| {
                OciError::InvalidConfig(
                    "container requires a command (init_process or cmd)".to_string(),
                )
            })?;

        // Per-container overlay path: VirtioFS doesn't support mknod, so we
        // create a guest-side overlay with tmpfs as upperdir. The path is
        // deterministic so we can write the bundle config before booting.
        let container_overlay = format!("/run/vz-oci/containers/{oci_container_id}");
        let guest_rootfs_path = format!("{container_overlay}/merged");

        let mut bundle_mounts = mount_specs_to_bundle_mounts(&run.mounts)?;

        // Generate /etc/hosts file for inter-service hostname resolution.
        if !run.extra_hosts.is_empty() {
            write_hosts_file(&bundle_host_dir, &run.extra_hosts)?;
            bundle_mounts.push(BundleMount {
                destination: PathBuf::from("/etc/hosts"),
                source: PathBuf::from(format!("{bundle_guest_path}/etc/hosts")),
                typ: "bind".to_string(),
                options: vec!["rbind".to_string(), "ro".to_string()],
            });
        }

        write_oci_bundle(
            &bundle_host_dir,
            Path::new(&guest_rootfs_path),
            BundleSpec {
                cmd: bundle_cmd,
                env: run.env.clone(),
                cwd: run.working_dir.clone(),
                user: run.user.clone(),
                mounts: bundle_mounts,
                oci_annotations: run.oci_annotations.clone(),
                network_namespace_path: None,
                share_host_network: true,
                cpu_quota: run.cpu_quota,
                cpu_period: run.cpu_period,
                capture_logs: run.capture_logs,
            },
        )?;

        let kernel = ensure_kernel_with_options(EnsureKernelOptions {
            install_dir: self.config.linux_install_dir.clone(),
            bundle_dir: self.config.linux_bundle_dir.clone(),
            require_exact_agent_version: self.config.require_exact_agent_version,
        })
        .await?;
        let runtime_binary = resolve_oci_runtime_binary_path(
            self.config.guest_oci_runtime,
            self.config.guest_oci_runtime_path.as_deref(),
            &kernel,
        )?;

        let mount_shares = mount_specs_to_shared_dirs(&run.mounts);
        let mut vm_config = LinuxVmConfig::new(kernel.kernel, kernel.initramfs)
            .with_rootfs_dir(rootfs_dir.to_path_buf());
        vm_config
            .shared_dirs
            .push(make_oci_runtime_share(&runtime_binary)?);
        vm_config.shared_dirs.extend(mount_shares);
        vm_config.cpus = run.cpus.unwrap_or(self.config.default_cpus);
        vm_config.memory_mb = run.memory_mb.unwrap_or(self.config.default_memory_mb);
        vm_config.serial_log_file = run.serial_log_file.clone();

        let network_enabled = run
            .network_enabled
            .unwrap_or(self.config.default_network_enabled);
        if !network_enabled {
            vm_config.network = Some(NetworkConfig::None);
        }

        let vm = LinuxVm::create(vm_config).await?;
        vm.start().await?;

        if let Err(err) = vm.wait_for_agent(self.config.agent_ready_timeout).await {
            let _ = vm.stop().await;
            return Err(err.into());
        }

        // Set up per-container overlay so youki can mknod on tmpfs.
        if let Err(err) =
            setup_guest_container_overlay(&vm, "/vz-rootfs", &oci_container_id).await
        {
            let _ = vm.stop().await;
            return Err(err);
        }

        let vm = Arc::new(vm);

        // Set up port forwarding; failures tear down the VM.
        let port_forwarding = match start_port_forwarding(vm.inner_shared(), &run.ports).await {
            Ok(pf) => pf,
            Err(err) => {
                let _ = vm.stop().await;
                return Err(err);
            }
        };

        // OCI create + start.
        if let Err(err) = vm
            .oci_create(oci_container_id.clone(), bundle_guest_path)
            .await
        {
            let _ = vm.stop().await;
            return Err(OciError::from(err));
        }

        if let Err(err) = vm.oci_start(oci_container_id.clone()).await {
            let _ = vm.oci_delete(oci_container_id, true).await;
            let _ = vm.stop().await;
            return Err(OciError::from(err));
        }

        // Register VM handle for exec/stop/remove.
        self.vm_handles
            .lock()
            .await
            .insert(container_id.to_string(), vm);

        // Keep port forwarding alive for the container's lifetime.
        if let Some(pf) = port_forwarding {
            self.port_forwards
                .lock()
                .await
                .insert(container_id.to_string(), pf);
        }

        Ok(())
    }

    async fn run_rootfs_with_oci_runtime(
        &self,
        rootfs_dir: impl AsRef<Path>,
        run: RunConfig,
        registered_container_id: &str,
    ) -> Result<ExecOutput, OciError> {
        let RunConfig {
            cmd,
            init_process,
            working_dir,
            env,
            user,
            ports,
            mounts,
            cpus,
            memory_mb,
            network_enabled,
            serial_log_file,
            execution_mode: _,
            timeout,
            container_id,
            oci_annotations,
            extra_hosts,
            network_namespace_path: _,
            cpu_quota: _,
            cpu_period: _,
            capture_logs: _,
        } = run;

        let rootfs_dir = rootfs_dir.as_ref().to_path_buf();

        if !rootfs_dir.is_dir() {
            return Err(OciError::InvalidRootfs { path: rootfs_dir });
        }

        let (command, args) = cmd
            .split_first()
            .ok_or_else(|| OciError::InvalidConfig("run command must not be empty".to_string()))?;

        let container_id = container_id.unwrap_or_else(new_container_id);
        let bundle_guest_root = oci_bundle_guest_root(self.config.guest_state_dir.as_deref())?;
        let bundle_guest_path = oci_bundle_guest_path(&bundle_guest_root, &container_id);
        let bundle_host_dir = oci_bundle_host_dir(&rootfs_dir, &bundle_guest_path);
        // OCI lifecycle: create → start → exec → delete.
        // The init process must be long-lived so the container stays running for exec.
        // If no explicit init process is set, use `sleep infinity` as the default.
        let bundle_cmd = init_process.unwrap_or_else(|| vec!["sleep".into(), "infinity".into()]);

        // Per-container overlay path: VirtioFS doesn't support mknod, so we
        // create a guest-side overlay with tmpfs as upperdir. The path is
        // deterministic so we can write the bundle config before booting.
        let container_overlay = format!("/run/vz-oci/containers/{container_id}");
        let guest_rootfs_path = format!("{container_overlay}/merged");

        let mut bundle_mounts = mount_specs_to_bundle_mounts(&mounts)?;

        if !extra_hosts.is_empty() {
            write_hosts_file(&bundle_host_dir, &extra_hosts)?;
            bundle_mounts.push(BundleMount {
                destination: PathBuf::from("/etc/hosts"),
                source: PathBuf::from(format!("{bundle_guest_path}/etc/hosts")),
                typ: "bind".to_string(),
                options: vec!["rbind".to_string(), "ro".to_string()],
            });
        }

        write_oci_bundle(
            &bundle_host_dir,
            Path::new(&guest_rootfs_path),
            BundleSpec {
                cmd: bundle_cmd,
                env: env.clone(),
                cwd: working_dir.clone(),
                user: user.clone(),
                mounts: bundle_mounts,
                oci_annotations,
                network_namespace_path: None,
                share_host_network: true,
                cpu_quota: None,
                cpu_period: None,
                capture_logs: false,
            },
        )?;

        let kernel = ensure_kernel_with_options(EnsureKernelOptions {
            install_dir: self.config.linux_install_dir.clone(),
            bundle_dir: self.config.linux_bundle_dir.clone(),
            require_exact_agent_version: self.config.require_exact_agent_version,
        })
        .await?;
        let runtime_binary = resolve_oci_runtime_binary_path(
            self.config.guest_oci_runtime,
            self.config.guest_oci_runtime_path.as_deref(),
            &kernel,
        )?;

        let mount_shares = mount_specs_to_shared_dirs(&mounts);
        let mut vm_config =
            LinuxVmConfig::new(kernel.kernel, kernel.initramfs).with_rootfs_dir(rootfs_dir);
        vm_config
            .shared_dirs
            .push(make_oci_runtime_share(&runtime_binary)?);
        vm_config.shared_dirs.extend(mount_shares);
        vm_config.cpus = cpus.unwrap_or(self.config.default_cpus);
        vm_config.memory_mb = memory_mb.unwrap_or(self.config.default_memory_mb);
        vm_config.serial_log_file = serial_log_file;

        let network_enabled = network_enabled.unwrap_or(self.config.default_network_enabled);
        if !network_enabled {
            vm_config.network = Some(NetworkConfig::None);
        }

        let vm = LinuxVm::create(vm_config).await?;
        vm.start().await?;

        if let Err(err) = vm.wait_for_agent(self.config.agent_ready_timeout).await {
            let _ = vm.stop().await;
            return Err(err.into());
        }

        // Set up per-container overlay so youki can mknod on tmpfs.
        if let Err(err) =
            setup_guest_container_overlay(&vm, "/vz-rootfs", &container_id).await
        {
            let _ = vm.stop().await;
            return Err(err);
        }

        // Register VM handle so external stop/remove can reach the guest.
        let vm = Arc::new(vm);
        self.vm_handles
            .lock()
            .await
            .insert(registered_container_id.to_string(), Arc::clone(&vm));

        let port_forwards = match start_port_forwarding(vm.inner_shared(), &ports).await {
            Ok(port_forwards) => port_forwards,
            Err(err) => {
                let _ = vm.stop().await;
                return Err(err);
            }
        };

        let lifecycle_timeout = timeout.unwrap_or(self.config.exec_timeout);
        let lifecycle = tokio::time::timeout(
            lifecycle_timeout,
            run_oci_lifecycle(
                vm.as_ref(),
                container_id,
                bundle_guest_path,
                command.clone(),
                args.to_vec(),
                OciExecOptions {
                    env,
                    cwd: working_dir,
                    user,
                },
            ),
        )
        .await
        .map_err(|_| {
            OciError::InvalidConfig(format!(
                "oci runtime exec timed out after {:.3}s",
                lifecycle_timeout.as_secs_f64()
            ))
        })?;

        if let Some(port_forwards) = port_forwards {
            port_forwards.shutdown().await;
        }

        let stop = vm.stop().await;

        match (lifecycle, stop) {
            (Ok(output), Ok(())) => Ok(output),
            (Err(exec_err), Ok(())) => Err(exec_err),
            (Ok(_), Err(stop_err)) => Err(stop_err.into()),
            (Err(exec_err), Err(_stop_err)) => Err(exec_err),
        }
    }

    /// Run a command against a local rootfs mounted as VirtioFS `rootfs`.
    ///
    /// This is a stepping stone toward full OCI image lifecycle support.
    pub async fn run_rootfs(
        &self,
        rootfs_dir: impl AsRef<Path>,
        run: RunConfig,
    ) -> Result<ExecOutput, OciError> {
        let RunConfig {
            cmd,
            init_process: _,
            working_dir,
            env,
            user,
            ports,
            mounts,
            cpus,
            memory_mb,
            network_enabled,
            serial_log_file,
            execution_mode: _,
            timeout,
            container_id: _,
            oci_annotations: _,
            extra_hosts: _,
            network_namespace_path: _,
            cpu_quota: _,
            cpu_period: _,
            capture_logs: _,
        } = run;

        let rootfs_dir = rootfs_dir.as_ref().to_path_buf();

        if !rootfs_dir.is_dir() {
            return Err(OciError::InvalidRootfs { path: rootfs_dir });
        }

        let (command, args) = cmd
            .split_first()
            .ok_or_else(|| OciError::InvalidConfig("run command must not be empty".to_string()))?;

        let kernel = ensure_kernel_with_options(EnsureKernelOptions {
            install_dir: self.config.linux_install_dir.clone(),
            bundle_dir: self.config.linux_bundle_dir.clone(),
            require_exact_agent_version: self.config.require_exact_agent_version,
        })
        .await?;
        let runtime_binary = resolve_oci_runtime_binary_path(
            self.config.guest_oci_runtime,
            self.config.guest_oci_runtime_path.as_deref(),
            &kernel,
        )?;

        let mut vm_config =
            LinuxVmConfig::new(kernel.kernel, kernel.initramfs).with_rootfs_dir(rootfs_dir);
        vm_config
            .shared_dirs
            .push(make_oci_runtime_share(&runtime_binary)?);

        // Add VirtioFS shares for bind mounts and encode target paths in
        // the kernel command line so the initramfs can mount them.
        let mount_shares = mount_specs_to_shared_dirs(&mounts);
        if !mount_shares.is_empty() {
            vm_config.shared_dirs.extend(mount_shares);
            for (idx, spec) in mounts.iter().enumerate() {
                if matches!(spec.mount_type, MountType::Bind) {
                    vm_config
                        .cmdline
                        .push_str(&format!(" vz.mount.{}={}", idx, spec.target.display()));
                }
            }
        }

        vm_config.cpus = cpus.unwrap_or(self.config.default_cpus);
        vm_config.memory_mb = memory_mb.unwrap_or(self.config.default_memory_mb);
        vm_config.serial_log_file = serial_log_file;

        let network_enabled = network_enabled.unwrap_or(self.config.default_network_enabled);
        if !network_enabled {
            vm_config.network = Some(NetworkConfig::None);
        }

        let vm = LinuxVm::create(vm_config).await?;
        vm.start().await?;

        if let Err(err) = vm.wait_for_agent(self.config.agent_ready_timeout).await {
            let _ = vm.stop().await;
            return Err(err.into());
        }

        let port_forwards = match start_port_forwarding(vm.inner_shared(), &ports).await {
            Ok(port_forwards) => port_forwards,
            Err(err) => {
                let _ = vm.stop().await;
                return Err(err);
            }
        };

        let exec_timeout = timeout.unwrap_or(self.config.exec_timeout);
        let exec = vm
            .exec_capture_with_options(
                command.clone(),
                args.to_vec(),
                exec_timeout,
                ExecOptions {
                    working_dir,
                    env,
                    user,
                },
            )
            .await;

        if let Some(port_forwards) = port_forwards {
            port_forwards.shutdown().await;
        }

        let stop = vm.stop().await;

        match (exec, stop) {
            (Ok(output), Ok(())) => Ok(output),
            (Err(exec_err), Ok(())) => Err(exec_err.into()),
            (Ok(_), Err(stop_err)) => Err(stop_err.into()),
            (Err(exec_err), Err(_stop_err)) => Err(exec_err.into()),
        }
    }

    /// Reconcile containers whose managing host PID is no longer alive.
    ///
    /// Transitions stale `Running`/`Created` containers to `Stopped` and
    /// cleans up their rootfs. Called automatically during `Runtime::new()`.
    fn reconcile_stale_containers(&self) {
        if let Ok(reconciled) = self.container_store.reconcile_stale() {
            for id in &reconciled {
                tracing::info!(container_id = %id, "reconciled stale container");
            }
        }
    }

    fn cleanup_rootfs_dir(&self, rootfs_dir: &Path) {
        let _ = fs::remove_dir_all(rootfs_dir);
    }

    fn cleanup_orphaned_rootfs(&self) {
        let rootfs_root = self.config.data_dir.join("rootfs");
        if !rootfs_root.is_dir() {
            return;
        }

        let referenced_rootfs: HashSet<PathBuf> = self
            .container_store
            .load_all()
            .map(|containers| {
                let mut roots = HashSet::new();
                for container in containers {
                    let Some(rootfs_path) = container.rootfs_path else {
                        continue;
                    };

                    if let Ok(canonical_rootfs) = rootfs_path.canonicalize() {
                        let _ = roots.insert(canonical_rootfs);
                    } else {
                        let _ = roots.insert(rootfs_path);
                    }
                }

                roots
            })
            .unwrap_or_default();

        let entries = match fs::read_dir(rootfs_root) {
            Ok(entries) => entries,
            Err(_) => return,
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };

            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let canonical_path = path.canonicalize().unwrap_or(path.clone());
            if !referenced_rootfs.contains(&canonical_path) {
                let _ = fs::remove_dir_all(path);
            }
        }
    }
}

type OciLifecycleFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, OciError>> + 'a>>;

trait OciLifecycleOps {
    fn oci_create<'a>(&'a self, id: String, bundle_path: String) -> OciLifecycleFuture<'a, ()>;
    fn oci_start<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, ()>;
    fn oci_exec<'a>(
        &'a self,
        id: String,
        command: String,
        args: Vec<String>,
        options: OciExecOptions,
    ) -> OciLifecycleFuture<'a, ExecOutput>;
    fn oci_kill<'a>(&'a self, id: String, signal: String) -> OciLifecycleFuture<'a, ()>;
    fn oci_state<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, OciContainerState>;
    fn oci_delete<'a>(&'a self, id: String, force: bool) -> OciLifecycleFuture<'a, ()>;
}

impl OciLifecycleOps for LinuxVm {
    fn oci_create<'a>(&'a self, id: String, bundle_path: String) -> OciLifecycleFuture<'a, ()> {
        Box::pin(async move {
            self.oci_create(id, bundle_path)
                .await
                .map_err(OciError::from)
        })
    }

    fn oci_start<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, ()> {
        Box::pin(async move { self.oci_start(id).await.map_err(OciError::from) })
    }

    fn oci_exec<'a>(
        &'a self,
        id: String,
        command: String,
        args: Vec<String>,
        options: OciExecOptions,
    ) -> OciLifecycleFuture<'a, ExecOutput> {
        Box::pin(async move {
            let result = self
                .oci_exec(id, command, args, options)
                .await
                .map_err(OciError::from)?;
            Ok(ExecOutput {
                exit_code: result.exit_code,
                stdout: result.stdout,
                stderr: result.stderr,
            })
        })
    }

    fn oci_kill<'a>(&'a self, id: String, signal: String) -> OciLifecycleFuture<'a, ()> {
        Box::pin(async move { self.oci_kill(id, signal).await.map_err(OciError::from) })
    }

    fn oci_state<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, OciContainerState> {
        Box::pin(async move { self.oci_state(id).await.map_err(OciError::from) })
    }

    fn oci_delete<'a>(&'a self, id: String, force: bool) -> OciLifecycleFuture<'a, ()> {
        Box::pin(async move { self.oci_delete(id, force).await.map_err(OciError::from) })
    }
}

async fn run_oci_lifecycle(
    vm: &impl OciLifecycleOps,
    container_id: String,
    bundle_guest_path: String,
    command: String,
    args: Vec<String>,
    options: OciExecOptions,
) -> Result<ExecOutput, OciError> {
    vm.oci_create(container_id.clone(), bundle_guest_path)
        .await?;

    if let Err(start_error) = vm.oci_start(container_id.clone()).await {
        let _ = vm.oci_delete(container_id, true).await;
        return Err(start_error);
    }

    let exec = vm
        .oci_exec(container_id.clone(), command, args, options)
        .await;
    let delete = vm.oci_delete(container_id, true).await;

    match (exec, delete) {
        (Ok(output), Ok(())) => Ok(output),
        (Err(exec_err), Ok(())) => Err(exec_err),
        (Ok(_), Err(delete_err)) => Err(delete_err),
        (Err(exec_err), Err(_delete_err)) => Err(exec_err),
    }
}

struct PortForwarding {
    shutdown_tx: watch::Sender<bool>,
    listener_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl PortForwarding {
    async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        for task in self.listener_tasks {
            let _ = task.await;
        }
    }
}

async fn start_port_forwarding(
    vm: Arc<Vm>,
    ports: &[PortMapping],
) -> Result<Option<PortForwarding>, OciError> {
    if ports.is_empty() {
        return Ok(None);
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut listener_tasks = Vec::with_capacity(ports.len());

    for mapping in ports {
        if mapping.protocol != PortProtocol::Tcp {
            let _ = shutdown_tx.send(true);
            for task in listener_tasks.drain(..) {
                let _ = task.await;
            }
            return Err(OciError::InvalidConfig(format!(
                "unsupported port forward protocol: {}",
                mapping.protocol.as_str()
            )));
        }

        let listener = match TcpListener::bind(("127.0.0.1", mapping.host)).await {
            Ok(listener) => listener,
            Err(error) => {
                let _ = shutdown_tx.send(true);
                for task in listener_tasks.drain(..) {
                    let _ = task.await;
                }

                return Err(OciError::InvalidConfig(format!(
                    "failed to bind host port {} for forwarding to {}: {error}",
                    mapping.host, mapping.container
                )));
            }
        };

        let mut listener_shutdown_rx = shutdown_rx.clone();
        let listener_vm = Arc::clone(&vm);
        let listener_mapping = mapping.clone();

        listener_tasks.push(tokio::spawn(async move {
            let mut connection_tasks = JoinSet::new();

            loop {
                tokio::select! {
                    changed = listener_shutdown_rx.changed() => {
                        if changed.is_err() || *listener_shutdown_rx.borrow() {
                            break;
                        }
                    }
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((host_stream, _peer)) => {
                                let connection_vm = Arc::clone(&listener_vm);
                                let connection_mapping = listener_mapping.clone();
                                connection_tasks.spawn(async move {
                                    let host_port = connection_mapping.host;
                                    let container_port = connection_mapping.container;
                                    if let Err(error) = relay_port_forward_connection(
                                        connection_vm,
                                        host_stream,
                                        connection_mapping,
                                    )
                                    .await
                                    {
                                        warn!(
                                            host_port,
                                            container_port,
                                            error = %error,
                                            "port forward connection failed"
                                        );
                                    }
                                });
                            }
                            Err(error) => {
                                warn!(
                                    host_port = listener_mapping.host,
                                    container_port = listener_mapping.container,
                                    error = %error,
                                    "port forward listener accept failed"
                                );
                                break;
                            }
                        }
                    }
                    join_result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                        if let Some(Err(error)) = join_result {
                            warn!(
                                host_port = listener_mapping.host,
                                container_port = listener_mapping.container,
                                error = %error,
                                "port forward relay task join failed"
                            );
                        }
                    }
                }
            }

            while let Some(join_result) = connection_tasks.join_next().await {
                if let Err(error) = join_result {
                    warn!(
                        host_port = listener_mapping.host,
                        container_port = listener_mapping.container,
                        error = %error,
                        "port forward relay task join failed"
                    );
                }
            }
        }));
    }

    Ok(Some(PortForwarding {
        shutdown_tx,
        listener_tasks,
    }))
}

async fn relay_port_forward_connection(
    vm: Arc<Vm>,
    mut host_stream: TcpStream,
    mapping: PortMapping,
) -> Result<(), LinuxError> {
    let mut guest_stream = vz_linux::open_port_forward_stream(
        vm.as_ref(),
        mapping.container,
        mapping.protocol.as_str(),
        mapping.target_host.as_deref(),
    )
    .await?;

    tokio::io::copy_bidirectional(&mut host_stream, &mut guest_stream)
        .await
        .map_err(|error| LinuxError::Protocol(format!("port forward relay failed: {error}")))?;

    Ok(())
}

/// Stop a container through OCI runtime lifecycle: kill → poll state → escalate.
///
/// Graceful (force=false): sends SIGTERM, polls state until stopped or grace
/// period expires, then escalates to SIGKILL.
/// Forced (force=true): sends SIGKILL immediately.
///
/// Returns the conventional exit code: 128+signal (143 for SIGTERM, 137 for SIGKILL).
async fn stop_via_oci_runtime(
    vm: &impl OciLifecycleOps,
    container_id: &str,
    force: bool,
    grace_period: Duration,
) -> Result<i32, OciError> {
    let id = container_id.to_string();

    if force {
        let _ = vm.oci_kill(id.clone(), "SIGKILL".to_string()).await;
        return Ok(137); // 128 + 9
    }

    // Graceful: SIGTERM first.
    vm.oci_kill(id.clone(), "SIGTERM".to_string()).await?;

    // Poll state until stopped or grace period expires.
    let deadline = tokio::time::Instant::now() + grace_period;
    loop {
        if is_container_stopped(vm, &id).await {
            return Ok(143); // 128 + 15 (SIGTERM)
        }

        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(STOP_POLL_INTERVAL).await;
    }

    // Escalate to SIGKILL after grace period.
    let _ = vm.oci_kill(id.clone(), "SIGKILL".to_string()).await;
    Ok(137) // 128 + 9
}

/// Check if the OCI runtime reports the container as stopped.
async fn is_container_stopped(vm: &impl OciLifecycleOps, container_id: &str) -> bool {
    match vm.oci_state(container_id.to_string()).await {
        Ok(state) => state.status == "stopped",
        Err(_) => true, // If state query fails, assume stopped.
    }
}

fn resolve_oci_runtime_binary_path(
    runtime_kind: OciRuntimeKind,
    configured_path: Option<&Path>,
    kernel: &KernelPaths,
) -> Result<PathBuf, OciError> {
    let binary = configured_path
        .map(PathBuf::from)
        .unwrap_or_else(|| kernel.youki.clone());
    validate_oci_runtime_binary_path(runtime_kind, &binary)?;
    Ok(binary)
}

fn validate_oci_runtime_binary_path(
    runtime_kind: OciRuntimeKind,
    path: &Path,
) -> Result<(), OciError> {
    let expected_binary = runtime_kind.binary_name();
    let Some(file_name) = path.file_name() else {
        return Err(OciError::InvalidConfig(format!(
            "guest oci runtime path must end with '{expected_binary}': {}",
            path.display()
        )));
    };

    if file_name != expected_binary {
        return Err(OciError::InvalidConfig(format!(
            "guest oci runtime path must point to '{expected_binary}': {}",
            path.display()
        )));
    }

    if !path.is_file() {
        return Err(OciError::InvalidConfig(format!(
            "guest oci runtime binary not found: {}",
            path.display()
        )));
    }

    Ok(())
}

/// Convert public `MountSpec` entries to internal `BundleMount` entries for
/// OCI runtime-spec generation.
fn mount_specs_to_bundle_mounts(mounts: &[MountSpec]) -> Result<Vec<BundleMount>, OciError> {
    let mut bundle_mounts = Vec::with_capacity(mounts.len());
    for (idx, spec) in mounts.iter().enumerate() {
        if !spec.target.is_absolute() {
            return Err(OciError::InvalidConfig(format!(
                "mount target must be an absolute path: {}",
                spec.target.display()
            )));
        }

        let (typ, source, options) = match &spec.mount_type {
            MountType::Bind => {
                let source = spec.source.clone().ok_or_else(|| {
                    OciError::InvalidConfig(format!(
                        "bind mount at {} requires a source path",
                        spec.target.display()
                    ))
                })?;
                let mut opts = vec!["rbind".to_string()];
                match spec.access {
                    MountAccess::ReadWrite => opts.push("rw".to_string()),
                    MountAccess::ReadOnly => opts.push("ro".to_string()),
                }
                ("bind".to_string(), source, opts)
            }
            MountType::Tmpfs => {
                let opts = vec!["nosuid".to_string(), "nodev".to_string()];
                ("tmpfs".to_string(), PathBuf::from("tmpfs"), opts)
            }
        };

        // Use the virtio mount tag as the in-guest source path for bind mounts.
        let guest_source = match &spec.mount_type {
            MountType::Bind => PathBuf::from(format!("/mnt/vz-mount-{idx}")),
            MountType::Tmpfs => source,
        };

        bundle_mounts.push(BundleMount {
            destination: spec.target.clone(),
            source: guest_source,
            typ,
            options,
        });
    }
    Ok(bundle_mounts)
}

/// Generate VirtioFS shared directory entries for bind mount sources.
fn mount_specs_to_shared_dirs(mounts: &[MountSpec]) -> Vec<SharedDirConfig> {
    mounts
        .iter()
        .enumerate()
        .filter_map(|(idx, spec)| {
            if !matches!(spec.mount_type, MountType::Bind) {
                return None;
            }
            let source = spec.source.as_ref()?;
            Some(SharedDirConfig {
                tag: format!("vz-mount-{idx}"),
                source: source.clone(),
                read_only: matches!(spec.access, MountAccess::ReadOnly),
            })
        })
        .collect()
}

fn make_oci_runtime_share(runtime_binary: &Path) -> Result<SharedDirConfig, OciError> {
    let Some(parent) = runtime_binary.parent() else {
        return Err(OciError::InvalidConfig(format!(
            "guest oci runtime path has no parent directory: {}",
            runtime_binary.display()
        )));
    };

    Ok(SharedDirConfig {
        tag: OCI_RUNTIME_BIN_SHARE_TAG.to_string(),
        source: parent.to_path_buf(),
        read_only: true,
    })
}

/// Write an `/etc/hosts` file into the OCI bundle directory.
///
/// The generated file contains standard localhost entries plus one line
/// per extra host mapping (hostname → IP).
fn write_hosts_file(rootfs_dir: &Path, extra_hosts: &[(String, String)]) -> Result<(), OciError> {
    use std::io::Write;
    let etc_dir = rootfs_dir.join("etc");
    fs::create_dir_all(&etc_dir)?;
    let hosts_path = etc_dir.join("hosts");
    let mut f = fs::File::create(&hosts_path)?;
    writeln!(f, "127.0.0.1\tlocalhost")?;
    writeln!(f, "::1\tlocalhost")?;
    for (hostname, ip) in extra_hosts {
        writeln!(f, "{ip}\t{hostname}")?;
    }
    Ok(())
}

fn oci_bundle_host_dir(rootfs_dir: &Path, bundle_guest_path: &str) -> PathBuf {
    rootfs_dir.join(bundle_guest_path.trim_start_matches('/'))
}

fn oci_bundle_guest_path(bundle_guest_root: &str, container_id: &str) -> String {
    format!(
        "{}/{}",
        bundle_guest_root.trim_end_matches('/'),
        container_id
    )
}

fn oci_bundle_guest_root(guest_state_dir: Option<&Path>) -> Result<String, OciError> {
    let state_dir = guest_state_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(OCI_DEFAULT_GUEST_STATE_DIR));

    if !state_dir.is_absolute() {
        return Err(OciError::InvalidConfig(format!(
            "guest state dir must be an absolute path: {}",
            state_dir.display()
        )));
    }

    let state_lossy = state_dir.to_string_lossy();
    let state_root = state_lossy.trim_end_matches('/');
    if state_root.is_empty() {
        return Ok(format!("/{OCI_BUNDLE_DIRNAME}"));
    }

    Ok(format!("{state_root}/{OCI_BUNDLE_DIRNAME}"))
}

/// Set up a per-container overlay in the guest VM.
///
/// VirtioFS doesn't support mknod, which the OCI runtime needs for default
/// devices (/dev/null etc). This creates a local overlay in the guest with
/// VirtioFS as lowerdir and tmpfs as upperdir so that mknod writes go to the
/// tmpfs layer.
///
/// Returns the guest-side merged rootfs path for use in the OCI bundle spec.
async fn setup_guest_container_overlay(
    vm: &LinuxVm,
    vz_rootfs_path: &str,
    container_id: &str,
) -> Result<String, OciError> {
    let container_overlay = format!("/run/vz-oci/containers/{container_id}");
    let guest_rootfs_path = format!("{container_overlay}/merged");

    let overlay_cmd = format!(
        "mkdir -p {container_overlay} && \
         mount -t tmpfs tmpfs {container_overlay} && \
         mkdir -p {container_overlay}/upper {container_overlay}/work {container_overlay}/merged && \
         mount -t overlay overlay \
         -o lowerdir={vz_rootfs_path},upperdir={container_overlay}/upper,workdir={container_overlay}/work \
         {container_overlay}/merged"
    );

    let result = vm
        .exec_capture(
            "sh".to_string(),
            vec!["-c".to_string(), overlay_cmd],
            Duration::from_secs(10),
        )
        .await
        .map_err(OciError::from)?;

    if result.exit_code != 0 {
        return Err(OciError::Linux(LinuxError::Protocol(format!(
            "per-container overlay setup failed (exit {}): {}",
            result.exit_code,
            result.stderr.trim()
        ))));
    }

    Ok(guest_rootfs_path)
}

fn expand_home_dir(path: &Path) -> PathBuf {
    let raw = path.to_string_lossy();
    if raw == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home);
        }
        return path.to_path_buf();
    }

    if let Some(stripped) = raw.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(stripped);
        }
    }

    path.to_path_buf()
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("config", &self.config)
            .field("data_dir", &self.config.data_dir)
            .finish()
    }
}

fn resolve_run_config(
    image_config: ImageConfigSummary,
    run: RunConfig,
    container_id: &str,
) -> Result<RunConfig, OciError> {
    let RunConfig {
        cmd: run_cmd,
        init_process,
        working_dir: run_working_dir,
        env: run_env,
        user: run_user,
        ports,
        mounts,
        cpus,
        memory_mb,
        network_enabled,
        serial_log_file,
        timeout,
        execution_mode,
        container_id: _,
        oci_annotations,
        extra_hosts,
        network_namespace_path,
        cpu_quota: _,
        cpu_period: _,
        capture_logs,
    } = run;

    let resolved_cmd = if !run_cmd.is_empty() {
        run_cmd
    } else {
        let mut image_cmd = Vec::new();
        if let Some(entrypoint) = image_config.entrypoint {
            image_cmd.extend(entrypoint);
        }

        if let Some(cmd) = image_config.cmd {
            image_cmd.extend(cmd);
        }

        if image_cmd.is_empty() {
            return Err(OciError::InvalidConfig(
                "run command must not be empty".to_string(),
            ));
        }

        image_cmd
    };

    let resolved_env = merge_run_env(image_config.env, run_env, container_id);
    let working_dir = run_working_dir.or(image_config.working_dir);
    let user = run_user.or(image_config.user);
    if init_process.as_ref().is_some_and(Vec::is_empty) {
        return Err(OciError::InvalidConfig(
            "init process must not be empty".to_string(),
        ));
    }

    Ok(RunConfig {
        cmd: resolved_cmd,
        working_dir,
        env: resolved_env,
        user,
        ports,
        mounts,
        cpus,
        memory_mb,
        network_enabled,
        serial_log_file,
        timeout,
        execution_mode,
        container_id: Some(container_id.to_string()),
        init_process,
        oci_annotations,
        extra_hosts,
        network_namespace_path,
        cpu_quota: None,
        cpu_period: None,
        capture_logs,
    })
}

fn merge_run_env(
    image_env: Option<Vec<String>>,
    run_env: Vec<(String, String)>,
    container_id: &str,
) -> Vec<(String, String)> {
    let mut merged: Vec<(String, String)> = image_env
        .unwrap_or_default()
        .into_iter()
        .map(|entry| {
            entry
                .split_once('=')
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .unwrap_or_else(|| (entry, String::new()))
        })
        .collect();

    for (run_key, run_value) in run_env {
        let mut was_updated = false;
        for (existing_key, existing_value) in merged.iter_mut() {
            if *existing_key == run_key {
                *existing_value = run_value.clone();
                was_updated = true;
            }
        }

        if !was_updated && !merged.iter().any(|(key, _)| *key == run_key) {
            merged.push((run_key, run_value));
        }
    }

    merged.retain(|(key, _)| key != "VZ_CONTAINER_ID");
    merged.push(("VZ_CONTAINER_ID".to_string(), container_id.to_string()));

    merged
}

fn new_container_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = process::id();

    format!("vz-oci-{pid}-{nanos}")
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::env;
    use std::io;

    use super::*;
    use vz_linux::KernelVersion;

    fn unique_temp_dir(name: &str) -> PathBuf {
        let mut base = env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        base.push(format!(
            "vz-oci-runtime-test-{name}-{}-{}",
            process::id(),
            nanos.as_nanos(),
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    #[test]
    fn runtime_list_containers_reads_from_store() {
        let data_dir = unique_temp_dir("list");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "container-2".to_string(),
                image: "alpine:3.22".to_string(),
                image_id: "sha256:img2".to_string(),
                status: ContainerStatus::Created,
                created_unix_secs: 100,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "container-1".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:img1".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 200,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        let containers = runtime.list_containers().unwrap();

        assert_eq!(containers.len(), 2);
        assert_eq!(containers[0].id, "container-1");
        assert_eq!(containers[1].id, "container-2");
    }

    #[tokio::test]
    async fn runtime_remove_container_removes_metadata_and_rootfs() {
        let data_dir = unique_temp_dir("remove");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });
        let rootfs_path = data_dir.join("rootfs");
        fs::create_dir_all(&rootfs_path).unwrap();

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "container-1".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:img1".to_string(),
                status: ContainerStatus::Created,
                created_unix_secs: 100,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: Some(rootfs_path.clone()),
                host_pid: None,
            })
            .unwrap();

        runtime.remove_container("container-1").await.unwrap();

        assert!(!rootfs_path.exists());
        assert!(runtime.list_containers().unwrap().is_empty());

        let missing = runtime.remove_container("container-1").await;
        let err = missing.err().unwrap();
        assert!(matches!(err, OciError::Storage(_)));
        if let OciError::Storage(io_err) = err {
            assert_eq!(io_err.kind(), io::ErrorKind::NotFound);
        }
    }

    #[tokio::test]
    async fn runtime_remove_container_rejects_running_container() {
        let data_dir = unique_temp_dir("remove-running");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "container-run".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:img1".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 100,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(process::id()),
            })
            .unwrap();

        let error = runtime.remove_container("container-run").await.unwrap_err();
        assert!(matches!(error, OciError::InvalidConfig(_)));
    }

    #[tokio::test]
    async fn stop_via_oci_runtime_sends_sigterm_and_polls_state() {
        let mock = MockOciLifecycleOps::new(ExecOutput {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        });

        let exit_code = stop_via_oci_runtime(&mock, "svc-web", false, Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(exit_code, 143); // 128 + SIGTERM(15)
        let calls = mock.calls.lock().unwrap();
        assert!(calls.contains(&"kill:SIGTERM"));
        assert!(calls.contains(&"state"));
    }

    #[tokio::test]
    async fn stop_via_oci_runtime_forced_sends_sigkill() {
        let mock = MockOciLifecycleOps::new(ExecOutput {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        });

        let exit_code = stop_via_oci_runtime(&mock, "svc-web", true, Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(exit_code, 137); // 128 + SIGKILL(9)
        let calls = mock.calls.lock().unwrap();
        assert!(calls.contains(&"kill:SIGKILL"));
        assert!(!calls.contains(&"kill:SIGTERM"));
    }

    #[tokio::test]
    async fn stop_via_oci_runtime_escalates_after_grace_period() {
        let mock = MockOciLifecycleOps::new(ExecOutput {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        });
        // Keep the container "running" so SIGTERM doesn't stop it.
        *mock.state_status.lock().unwrap() = "running".to_string();

        // Override kill to NOT change state (simulate unresponsive container).
        struct StubbornMock;
        impl OciLifecycleOps for StubbornMock {
            fn oci_create<'a>(
                &'a self,
                _id: String,
                _bundle_path: String,
            ) -> OciLifecycleFuture<'a, ()> {
                Box::pin(async { Ok(()) })
            }
            fn oci_start<'a>(&'a self, _id: String) -> OciLifecycleFuture<'a, ()> {
                Box::pin(async { Ok(()) })
            }
            fn oci_exec<'a>(
                &'a self,
                _id: String,
                _command: String,
                _args: Vec<String>,
                _options: OciExecOptions,
            ) -> OciLifecycleFuture<'a, ExecOutput> {
                Box::pin(async {
                    Ok(ExecOutput {
                        exit_code: 0,
                        stdout: String::new(),
                        stderr: String::new(),
                    })
                })
            }
            fn oci_kill<'a>(&'a self, _id: String, _signal: String) -> OciLifecycleFuture<'a, ()> {
                Box::pin(async { Ok(()) })
            }
            fn oci_state<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, OciContainerState> {
                // Always report running — container never stops from SIGTERM.
                Box::pin(async move {
                    Ok(OciContainerState {
                        id,
                        status: "running".to_string(),
                        pid: Some(42),
                        bundle_path: None,
                    })
                })
            }
            fn oci_delete<'a>(&'a self, _id: String, _force: bool) -> OciLifecycleFuture<'a, ()> {
                Box::pin(async { Ok(()) })
            }
        }

        let exit_code = stop_via_oci_runtime(
            &StubbornMock,
            "svc-stuck",
            false,
            Duration::from_millis(200),
        )
        .await
        .unwrap();

        // Should escalate to SIGKILL after grace period.
        assert_eq!(exit_code, 137);
    }

    #[test]
    fn runtime_new_preserves_referenced_rootfs() {
        let data_dir = unique_temp_dir("cleanup-preserve");
        let rootfs_root = data_dir.join("rootfs");
        fs::create_dir_all(&rootfs_root).unwrap();

        let referenced_rootfs = rootfs_root.join("container-keep");
        let orphan_rootfs = rootfs_root.join("container-remove");
        let non_rootfs_path = rootfs_root.join("keep.txt");

        fs::create_dir_all(&referenced_rootfs).unwrap();
        fs::create_dir_all(&orphan_rootfs).unwrap();
        fs::write(&non_rootfs_path, b"preserve").unwrap();

        let container_store = ContainerStore::new(data_dir.clone());
        container_store
            .upsert(ContainerInfo {
                id: "container-1".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:img1".to_string(),
                status: ContainerStatus::Created,
                created_unix_secs: 100,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: Some(referenced_rootfs.clone()),
                host_pid: Some(std::process::id()),
            })
            .unwrap();

        let _runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        assert!(referenced_rootfs.is_dir());
        assert!(!orphan_rootfs.exists());
        assert!(non_rootfs_path.is_file());
    }

    #[test]
    fn runtime_new_removes_unreferenced_rootfs_directories() {
        let data_dir = unique_temp_dir("cleanup-orphan");
        let rootfs_root = data_dir.join("rootfs");
        fs::create_dir_all(&rootfs_root).unwrap();

        let orphan_one = rootfs_root.join("orphan-one");
        let orphan_two = rootfs_root.join("orphan-two");
        fs::create_dir_all(&orphan_one).unwrap();
        fs::create_dir_all(&orphan_two).unwrap();

        let _runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        assert!(!orphan_one.exists());
        assert!(!orphan_two.exists());
    }

    #[test]
    fn resolve_run_config_prefers_run_command_when_present() {
        let image_config = ImageConfigSummary {
            entrypoint: Some(vec!["/default-entrypoint".to_string()]),
            cmd: Some(vec!["default-arg".to_string()]),
            ..ImageConfigSummary::default()
        };

        let run = RunConfig {
            cmd: vec!["container".to_string(), "command".to_string()],
            ..RunConfig::default()
        };

        let resolved = resolve_run_config(image_config, run, "container-123").unwrap();
        assert_eq!(
            resolved.cmd,
            vec!["container".to_string(), "command".to_string()],
        );
    }

    #[test]
    fn resolve_run_config_uses_image_entrypoint_and_cmd_when_run_command_empty() {
        let image_config = ImageConfigSummary {
            entrypoint: Some(vec!["/entrypoint".to_string()]),
            cmd: Some(vec!["arg".to_string()]),
            ..ImageConfigSummary::default()
        };

        let resolved =
            resolve_run_config(image_config, RunConfig::default(), "container-123").unwrap();
        assert_eq!(
            resolved.cmd,
            vec!["/entrypoint".to_string(), "arg".to_string()],
        );
    }

    #[test]
    fn resolve_run_config_preserves_execution_mode() {
        let image_config = ImageConfigSummary {
            cmd: Some(vec!["default".to_string()]),
            ..ImageConfigSummary::default()
        };

        let run = RunConfig {
            execution_mode: ExecutionMode::OciRuntime,
            ..RunConfig::default()
        };

        let resolved = resolve_run_config(image_config, run, "container-123").unwrap();
        assert_eq!(resolved.execution_mode, ExecutionMode::OciRuntime);
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedOciExec {
        id: String,
        command: String,
        args: Vec<String>,
        options: OciExecOptions,
    }

    struct MockOciLifecycleOps {
        calls: std::sync::Mutex<Vec<&'static str>>,
        exec_call: std::sync::Mutex<Option<RecordedOciExec>>,
        exec_output: ExecOutput,
        fail_start: bool,
        state_status: std::sync::Mutex<String>,
    }

    impl MockOciLifecycleOps {
        fn new(exec_output: ExecOutput) -> Self {
            Self {
                calls: std::sync::Mutex::new(Vec::new()),
                exec_call: std::sync::Mutex::new(None),
                exec_output,
                fail_start: false,
                state_status: std::sync::Mutex::new("running".to_string()),
            }
        }
    }

    impl OciLifecycleOps for MockOciLifecycleOps {
        fn oci_create<'a>(
            &'a self,
            _id: String,
            _bundle_path: String,
        ) -> OciLifecycleFuture<'a, ()> {
            self.calls.lock().unwrap().push("create");
            Box::pin(async { Ok(()) })
        }

        fn oci_start<'a>(&'a self, _id: String) -> OciLifecycleFuture<'a, ()> {
            self.calls.lock().unwrap().push("start");
            let fail_start = self.fail_start;
            Box::pin(async move {
                if fail_start {
                    Err(OciError::InvalidConfig("mock start failure".to_string()))
                } else {
                    Ok(())
                }
            })
        }

        fn oci_exec<'a>(
            &'a self,
            id: String,
            command: String,
            args: Vec<String>,
            options: OciExecOptions,
        ) -> OciLifecycleFuture<'a, ExecOutput> {
            self.calls.lock().unwrap().push("exec");
            *self.exec_call.lock().unwrap() = Some(RecordedOciExec {
                id,
                command,
                args,
                options,
            });
            let output = self.exec_output.clone();
            Box::pin(async move { Ok(output) })
        }

        fn oci_kill<'a>(&'a self, _id: String, signal: String) -> OciLifecycleFuture<'a, ()> {
            self.calls.lock().unwrap().push(if signal == "SIGKILL" {
                "kill:SIGKILL"
            } else {
                "kill:SIGTERM"
            });
            // Simulate: after kill, container becomes stopped.
            *self.state_status.lock().unwrap() = "stopped".to_string();
            Box::pin(async { Ok(()) })
        }

        fn oci_state<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, OciContainerState> {
            self.calls.lock().unwrap().push("state");
            let status = self.state_status.lock().unwrap().clone();
            Box::pin(async move {
                Ok(OciContainerState {
                    id,
                    status,
                    pid: None,
                    bundle_path: None,
                })
            })
        }

        fn oci_delete<'a>(&'a self, _id: String, _force: bool) -> OciLifecycleFuture<'a, ()> {
            self.calls.lock().unwrap().push("delete");
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn oci_runtime_lifecycle_uses_create_start_exec_delete_sequence() {
        let mock = MockOciLifecycleOps::new(ExecOutput {
            exit_code: 7,
            stdout: "ok".to_string(),
            stderr: String::new(),
        });

        let output = run_oci_lifecycle(
            &mock,
            "svc-web".to_string(),
            "/run/vz-oci/bundles/svc-web".to_string(),
            "/bin/echo".to_string(),
            vec!["hello".to_string()],
            OciExecOptions {
                env: vec![("GREETING".to_string(), "hello".to_string())],
                cwd: Some("/workspace".to_string()),
                user: Some("1000:1001".to_string()),
            },
        )
        .await
        .expect("OCI lifecycle should succeed");

        assert_eq!(
            output,
            ExecOutput {
                exit_code: 7,
                stdout: "ok".to_string(),
                stderr: String::new(),
            }
        );
        assert_eq!(
            *mock.calls.lock().unwrap(),
            vec!["create", "start", "exec", "delete"]
        );
        assert_eq!(
            *mock.exec_call.lock().unwrap(),
            Some(RecordedOciExec {
                id: "svc-web".to_string(),
                command: "/bin/echo".to_string(),
                args: vec!["hello".to_string()],
                options: OciExecOptions {
                    env: vec![("GREETING".to_string(), "hello".to_string())],
                    cwd: Some("/workspace".to_string()),
                    user: Some("1000:1001".to_string()),
                },
            }),
        );
    }

    #[tokio::test]
    async fn oci_runtime_lifecycle_attempts_delete_on_start_failure() {
        let mut mock = MockOciLifecycleOps::new(ExecOutput {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        });
        mock.fail_start = true;

        let error = run_oci_lifecycle(
            &mock,
            "svc-start-fail".to_string(),
            "/run/vz-oci/bundles/svc-start-fail".to_string(),
            "/bin/echo".to_string(),
            vec!["hello".to_string()],
            OciExecOptions::default(),
        )
        .await
        .expect_err("start failure should surface");
        assert!(matches!(error, OciError::InvalidConfig(ref msg) if msg == "mock start failure"));
        assert_eq!(
            *mock.calls.lock().unwrap(),
            vec!["create", "start", "delete"]
        );
    }

    #[test]
    fn oci_bundle_host_dir_is_rootfs_scoped() {
        let rootfs_dir = PathBuf::from("/tmp/vz-oci-rootfs");
        let guest_root = oci_bundle_guest_root(None).unwrap();
        let guest_path = oci_bundle_guest_path(&guest_root, "svc-bundle");
        let host_bundle = oci_bundle_host_dir(&rootfs_dir, &guest_path);
        assert_eq!(
            host_bundle,
            PathBuf::from("/tmp/vz-oci-rootfs/run/vz-oci/bundles/svc-bundle")
        );
        assert_eq!(guest_path, "/run/vz-oci/bundles/svc-bundle".to_string());
    }

    #[test]
    fn oci_bundle_guest_root_uses_custom_state_dir() {
        let guest_root = oci_bundle_guest_root(Some(Path::new("/var/lib/vz-oci"))).unwrap();
        assert_eq!(guest_root, "/var/lib/vz-oci/bundles".to_string());
    }

    #[test]
    fn oci_bundle_guest_root_rejects_relative_state_dir() {
        let error = oci_bundle_guest_root(Some(Path::new("var/lib/vz-oci"))).unwrap_err();
        assert!(matches!(error, OciError::InvalidConfig(_)));
    }

    #[test]
    fn write_hosts_file_generates_correct_content() {
        let tmp = unique_temp_dir("hosts-gen");
        let hosts = vec![
            ("db".to_string(), "127.0.0.1".to_string()),
            ("cache".to_string(), "10.0.0.5".to_string()),
        ];
        write_hosts_file(&tmp, &hosts).unwrap();
        let content = fs::read_to_string(tmp.join("etc/hosts")).unwrap();
        assert!(content.contains("127.0.0.1\tlocalhost"));
        assert!(content.contains("::1\tlocalhost"));
        assert!(content.contains("127.0.0.1\tdb"));
        assert!(content.contains("10.0.0.5\tcache"));
    }

    #[tokio::test]
    async fn run_rootfs_with_oci_runtime_rejects_nonexistent_rootfs() {
        let runtime = Runtime::new(RuntimeConfig {
            data_dir: unique_temp_dir("oci-missing-rootfs"),
            ..RuntimeConfig::default()
        });

        let err = runtime
            .run_rootfs_with_oci_runtime(
                "/tmp/vz-oci-missing-rootfs",
                RunConfig {
                    cmd: vec!["/bin/true".to_string()],
                    execution_mode: ExecutionMode::OciRuntime,
                    ..RunConfig::default()
                },
                "test-container",
            )
            .await
            .expect_err("missing rootfs should fail before VM wiring");

        assert!(matches!(err, OciError::InvalidRootfs { .. }));
    }

    #[test]
    fn resolve_run_config_merges_env_with_run_precedence() {
        let image_config = ImageConfigSummary {
            env: Some(vec![
                "BASE=1".to_string(),
                "OVERRIDE=old".to_string(),
                "VZ_CONTAINER_ID=stale".to_string(),
            ]),
            cmd: Some(vec!["default".to_string()]),
            ..ImageConfigSummary::default()
        };

        let run = RunConfig {
            env: vec![
                ("OVERRIDE".to_string(), "new".to_string()),
                ("NEW".to_string(), "value".to_string()),
                ("OVERRIDE".to_string(), "newer".to_string()),
            ],
            ..RunConfig::default()
        };

        let resolved = resolve_run_config(image_config, run, "container-123").unwrap();
        assert_eq!(
            resolved.env,
            vec![
                ("BASE".to_string(), "1".to_string()),
                ("OVERRIDE".to_string(), "newer".to_string()),
                ("NEW".to_string(), "value".to_string()),
                ("VZ_CONTAINER_ID".to_string(), "container-123".to_string()),
            ],
        );
    }

    #[test]
    fn resolve_run_config_preserves_ports() {
        let image_config = ImageConfigSummary {
            cmd: Some(vec!["default".to_string()]),
            ..ImageConfigSummary::default()
        };

        let run = RunConfig {
            ports: vec![PortMapping {
                host: 8080,
                container: 80,
                protocol: PortProtocol::Tcp,
                target_host: None,
            }],
            ..RunConfig::default()
        };

        let resolved = resolve_run_config(image_config, run, "container-123").unwrap();
        assert_eq!(
            resolved.ports,
            vec![PortMapping {
                host: 8080,
                container: 80,
                protocol: PortProtocol::Tcp,
                target_host: None,
            }],
        );
    }

    #[test]
    fn resolve_run_config_sets_container_id() {
        let image_config = ImageConfigSummary {
            cmd: Some(vec!["default".to_string()]),
            ..ImageConfigSummary::default()
        };

        let resolved =
            resolve_run_config(image_config, RunConfig::default(), "container-abc").unwrap();

        assert_eq!(resolved.container_id, Some("container-abc".to_string()));
    }

    fn make_kernel_paths_with_youki(path: PathBuf) -> KernelPaths {
        KernelPaths {
            kernel: PathBuf::from("/tmp/vmlinux"),
            initramfs: PathBuf::from("/tmp/initramfs.img"),
            youki: path,
            version: KernelVersion {
                kernel: "6.12.11".to_string(),
                busybox: "1.37.0".to_string(),
                agent: "0.1.0".to_string(),
                youki: "0.5.7".to_string(),
                built: Some("2026-02-18T00:00:00Z".to_string()),
                sha256_vmlinux: None,
                sha256_initramfs: None,
                sha256_youki: None,
            },
        }
    }

    #[test]
    fn resolve_oci_runtime_binary_path_uses_kernel_artifact_by_default() {
        let temp = unique_temp_dir("runtime-bin-default");
        let youki = temp.join("youki");
        fs::write(&youki, b"youki").unwrap();
        let kernel = make_kernel_paths_with_youki(youki.clone());

        let resolved =
            resolve_oci_runtime_binary_path(OciRuntimeKind::Youki, None, &kernel).unwrap();

        assert_eq!(resolved, youki);
    }

    #[test]
    fn resolve_oci_runtime_binary_path_prefers_configured_override() {
        let temp = unique_temp_dir("runtime-bin-override");
        let bundled_dir = temp.join("bundled");
        let override_dir = temp.join("override");
        fs::create_dir_all(&bundled_dir).unwrap();
        fs::create_dir_all(&override_dir).unwrap();
        let bundled_youki = bundled_dir.join("youki");
        let override_youki = override_dir.join("youki");
        fs::write(&bundled_youki, b"bundled").unwrap();
        fs::write(&override_youki, b"override").unwrap();
        let kernel = make_kernel_paths_with_youki(bundled_youki);

        let resolved =
            resolve_oci_runtime_binary_path(OciRuntimeKind::Youki, Some(&override_youki), &kernel)
                .unwrap();

        assert_eq!(resolved, override_youki);
    }

    #[test]
    fn resolve_oci_runtime_binary_path_rejects_non_youki_name() {
        let temp = unique_temp_dir("runtime-bin-name");
        let bad_path = temp.join("runtime");
        fs::write(&bad_path, b"binary").unwrap();
        let kernel = make_kernel_paths_with_youki(temp.join("youki"));

        let err = resolve_oci_runtime_binary_path(OciRuntimeKind::Youki, Some(&bad_path), &kernel)
            .unwrap_err();
        assert!(matches!(err, OciError::InvalidConfig(_)));
    }

    #[test]
    fn make_oci_runtime_share_uses_parent_dir_with_expected_tag() {
        let temp = unique_temp_dir("runtime-share");
        let youki = temp.join("youki");
        fs::write(&youki, b"runtime").unwrap();

        let share = make_oci_runtime_share(&youki).unwrap();

        assert_eq!(share.tag, OCI_RUNTIME_BIN_SHARE_TAG);
        assert_eq!(share.source, temp);
        assert!(share.read_only);
    }

    #[test]
    fn expand_home_dir_resolves_tilde_prefix() {
        let Some(home) = std::env::var_os("HOME") else {
            return;
        };

        let resolved = expand_home_dir(Path::new("~/.vz/oci"));
        assert_eq!(resolved, PathBuf::from(home).join(".vz/oci"));
    }

    // B09 - RuntimeConfig and RunConfig OCI extension tests

    #[test]
    fn runtime_config_guest_oci_runtime_defaults_to_youki() {
        let cfg = RuntimeConfig::default();
        assert_eq!(cfg.guest_oci_runtime, OciRuntimeKind::Youki);
        assert_eq!(cfg.guest_oci_runtime.binary_name(), "youki");
    }

    #[test]
    fn runtime_config_guest_state_dir_defaults_to_none() {
        let cfg = RuntimeConfig::default();
        assert!(cfg.guest_state_dir.is_none());
        // When None, bundle root uses the default /run/vz-oci.
        let root = oci_bundle_guest_root(cfg.guest_state_dir.as_deref()).unwrap();
        assert_eq!(root, "/run/vz-oci/bundles");
    }

    #[test]
    fn runtime_config_custom_guest_state_dir_flows_to_bundle_root() {
        let cfg = RuntimeConfig {
            guest_state_dir: Some(PathBuf::from("/var/lib/custom")),
            ..RuntimeConfig::default()
        };
        let root = oci_bundle_guest_root(cfg.guest_state_dir.as_deref()).unwrap();
        assert_eq!(root, "/var/lib/custom/bundles");
    }

    #[test]
    fn resolve_run_config_preserves_init_process() {
        let image_config = ImageConfigSummary {
            cmd: Some(vec!["default".to_string()]),
            ..ImageConfigSummary::default()
        };
        let run = RunConfig {
            init_process: Some(vec!["/sbin/init".to_string(), "--flag".to_string()]),
            ..RunConfig::default()
        };

        let resolved = resolve_run_config(image_config, run, "container-abc").unwrap();
        assert_eq!(
            resolved.init_process,
            Some(vec!["/sbin/init".to_string(), "--flag".to_string()])
        );
    }

    #[test]
    fn resolve_run_config_rejects_empty_init_process() {
        let image_config = ImageConfigSummary {
            cmd: Some(vec!["default".to_string()]),
            ..ImageConfigSummary::default()
        };
        let run = RunConfig {
            init_process: Some(Vec::new()),
            ..RunConfig::default()
        };

        let err = resolve_run_config(image_config, run, "container-abc").unwrap_err();
        assert!(matches!(err, OciError::InvalidConfig(_)));
    }

    #[test]
    fn mount_specs_to_bundle_mounts_converts_bind_mount() {
        let mounts = vec![MountSpec {
            source: Some(PathBuf::from("/host/data")),
            target: PathBuf::from("/container/data"),
            mount_type: MountType::Bind,
            access: MountAccess::ReadWrite,
        }];

        let bundle_mounts = mount_specs_to_bundle_mounts(&mounts).unwrap();
        assert_eq!(bundle_mounts.len(), 1);
        assert_eq!(
            bundle_mounts[0].destination,
            PathBuf::from("/container/data")
        );
        // Guest source should use the VirtioFS mount tag path.
        assert_eq!(bundle_mounts[0].source, PathBuf::from("/mnt/vz-mount-0"));
        assert_eq!(bundle_mounts[0].typ, "bind");
        assert!(bundle_mounts[0].options.contains(&"rbind".to_string()));
        assert!(bundle_mounts[0].options.contains(&"rw".to_string()));
    }

    #[test]
    fn mount_specs_to_bundle_mounts_converts_ro_bind_mount() {
        let mounts = vec![MountSpec {
            source: Some(PathBuf::from("/host/config")),
            target: PathBuf::from("/etc/app"),
            mount_type: MountType::Bind,
            access: MountAccess::ReadOnly,
        }];

        let bundle_mounts = mount_specs_to_bundle_mounts(&mounts).unwrap();
        assert_eq!(bundle_mounts.len(), 1);
        assert!(bundle_mounts[0].options.contains(&"ro".to_string()));
    }

    #[test]
    fn mount_specs_to_bundle_mounts_converts_tmpfs_mount() {
        let mounts = vec![MountSpec {
            source: None,
            target: PathBuf::from("/tmp"),
            mount_type: MountType::Tmpfs,
            access: MountAccess::ReadWrite,
        }];

        let bundle_mounts = mount_specs_to_bundle_mounts(&mounts).unwrap();
        assert_eq!(bundle_mounts.len(), 1);
        assert_eq!(bundle_mounts[0].destination, PathBuf::from("/tmp"));
        assert_eq!(bundle_mounts[0].source, PathBuf::from("tmpfs"));
        assert_eq!(bundle_mounts[0].typ, "tmpfs");
    }

    #[test]
    fn mount_specs_to_bundle_mounts_rejects_relative_target() {
        let mounts = vec![MountSpec {
            source: Some(PathBuf::from("/host")),
            target: PathBuf::from("relative/path"),
            mount_type: MountType::Bind,
            access: MountAccess::ReadWrite,
        }];

        let err = mount_specs_to_bundle_mounts(&mounts).unwrap_err();
        assert!(matches!(err, OciError::InvalidConfig(_)));
    }

    #[test]
    fn mount_specs_to_bundle_mounts_rejects_bind_without_source() {
        let mounts = vec![MountSpec {
            source: None,
            target: PathBuf::from("/container/path"),
            mount_type: MountType::Bind,
            access: MountAccess::ReadWrite,
        }];

        let err = mount_specs_to_bundle_mounts(&mounts).unwrap_err();
        assert!(matches!(err, OciError::InvalidConfig(_)));
    }

    #[test]
    fn mount_specs_to_shared_dirs_generates_virtio_shares_for_binds() {
        let mounts = vec![
            MountSpec {
                source: Some(PathBuf::from("/host/a")),
                target: PathBuf::from("/container/a"),
                mount_type: MountType::Bind,
                access: MountAccess::ReadWrite,
            },
            MountSpec {
                source: None,
                target: PathBuf::from("/tmp"),
                mount_type: MountType::Tmpfs,
                access: MountAccess::ReadWrite,
            },
            MountSpec {
                source: Some(PathBuf::from("/host/b")),
                target: PathBuf::from("/container/b"),
                mount_type: MountType::Bind,
                access: MountAccess::ReadOnly,
            },
        ];

        let shares = mount_specs_to_shared_dirs(&mounts);
        // Tmpfs is skipped, so only 2 entries.
        assert_eq!(shares.len(), 2);
        assert_eq!(shares[0].tag, "vz-mount-0");
        assert_eq!(shares[0].source, PathBuf::from("/host/a"));
        assert!(!shares[0].read_only);
        assert_eq!(shares[1].tag, "vz-mount-2");
        assert_eq!(shares[1].source, PathBuf::from("/host/b"));
        assert!(shares[1].read_only);
    }

    #[test]
    fn resolve_run_config_preserves_mounts() {
        let image_config = ImageConfigSummary {
            cmd: Some(vec!["default".to_string()]),
            ..ImageConfigSummary::default()
        };

        let run = RunConfig {
            mounts: vec![MountSpec {
                source: Some(PathBuf::from("/host/data")),
                target: PathBuf::from("/data"),
                mount_type: MountType::Bind,
                access: MountAccess::ReadWrite,
            }],
            ..RunConfig::default()
        };

        let resolved = resolve_run_config(image_config, run, "container-abc").unwrap();
        assert_eq!(resolved.mounts.len(), 1);
        assert_eq!(resolved.mounts[0].target, PathBuf::from("/data"));
    }

    #[test]
    fn resolve_run_config_preserves_oci_annotations() {
        let image_config = ImageConfigSummary {
            cmd: Some(vec!["default".to_string()]),
            ..ImageConfigSummary::default()
        };
        let annotations = vec![
            (
                "org.opencontainers.image.title".to_string(),
                "test".to_string(),
            ),
            ("custom.key".to_string(), "value".to_string()),
        ];
        let run = RunConfig {
            oci_annotations: annotations.clone(),
            ..RunConfig::default()
        };

        let resolved = resolve_run_config(image_config, run, "container-abc").unwrap();
        assert_eq!(resolved.oci_annotations, annotations);
    }

    #[test]
    fn exec_config_default_is_empty() {
        let cfg = ExecConfig::default();
        assert!(cfg.cmd.is_empty());
        assert!(cfg.working_dir.is_none());
        assert!(cfg.env.is_empty());
        assert!(cfg.user.is_none());
        assert!(cfg.timeout.is_none());
    }

    #[tokio::test]
    async fn exec_container_rejects_missing_vm_handle() {
        let data_dir = unique_temp_dir("exec-missing");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        let err = runtime
            .exec_container(
                "nonexistent",
                ExecConfig {
                    cmd: vec!["/bin/echo".to_string(), "hello".to_string()],
                    ..ExecConfig::default()
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(err, OciError::InvalidConfig(_)));
    }

    #[tokio::test]
    async fn exec_container_rejects_empty_command() {
        let data_dir = unique_temp_dir("exec-empty-cmd");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        // Manually register a mock VM handle to bypass the "no handle" error.
        // We can't actually create a LinuxVm in unit tests, but we can verify
        // the error path before it reaches the VM by testing with no handle.
        let err = runtime
            .exec_container(
                "no-such-container",
                ExecConfig {
                    cmd: vec![],
                    ..ExecConfig::default()
                },
            )
            .await
            .unwrap_err();

        // Should fail with "no active VM handle" since there's no container.
        assert!(matches!(err, OciError::InvalidConfig(_)));
    }

    #[tokio::test]
    async fn create_container_rejects_macos_backend() {
        let data_dir = unique_temp_dir("create-macos");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        let err = runtime
            .create_container("macos:sonoma", RunConfig::default())
            .await
            .unwrap_err();

        assert!(matches!(err, OciError::InvalidConfig(ref msg) if msg.contains("macos")));
    }

    // ── B14: Crash recovery conformance ──

    /// Simulates host crash by seeding container store with stale state, then
    /// creating a new Runtime (which triggers reconciliation in `::new()`).
    #[test]
    fn crash_recovery_transitions_stale_running_to_stopped() {
        let data_dir = unique_temp_dir("crash-stale-running");
        let store = ContainerStore::new(data_dir.clone());

        // Seed: a "Running" container whose host_pid is long dead.
        store
            .upsert(ContainerInfo {
                id: "running-stale".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:aaa".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 100,
                started_unix_secs: Some(101),
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(999_999_999),
            })
            .unwrap();

        // "Restart" — construct a fresh Runtime from the same data_dir.
        let _runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        let containers = ContainerStore::new(data_dir).load_all().unwrap();
        let c = containers.iter().find(|c| c.id == "running-stale").unwrap();
        assert!(matches!(
            c.status,
            ContainerStatus::Stopped { exit_code: -1 }
        ));
        assert!(c.stopped_unix_secs.is_some());
        assert!(c.host_pid.is_none());
    }

    #[test]
    fn crash_recovery_transitions_stale_created_to_stopped() {
        let data_dir = unique_temp_dir("crash-stale-created");
        let store = ContainerStore::new(data_dir.clone());

        store
            .upsert(ContainerInfo {
                id: "created-stale".to_string(),
                image: "alpine:3.22".to_string(),
                image_id: "sha256:bbb".to_string(),
                status: ContainerStatus::Created,
                created_unix_secs: 200,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(999_999_999),
            })
            .unwrap();

        let _runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        let containers = ContainerStore::new(data_dir).load_all().unwrap();
        let c = containers.iter().find(|c| c.id == "created-stale").unwrap();
        assert!(matches!(
            c.status,
            ContainerStatus::Stopped { exit_code: -1 }
        ));
        assert!(c.host_pid.is_none());
    }

    #[test]
    fn crash_recovery_preserves_alive_running_container() {
        let data_dir = unique_temp_dir("crash-alive");
        let store = ContainerStore::new(data_dir.clone());

        store
            .upsert(ContainerInfo {
                id: "alive".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:ccc".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 300,
                started_unix_secs: Some(301),
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(process::id()),
            })
            .unwrap();

        let _runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        let containers = ContainerStore::new(data_dir).load_all().unwrap();
        let c = containers.iter().find(|c| c.id == "alive").unwrap();
        assert!(matches!(c.status, ContainerStatus::Running));
        assert_eq!(c.host_pid, Some(process::id()));
    }

    #[test]
    fn crash_recovery_does_not_alter_stopped_containers() {
        let data_dir = unique_temp_dir("crash-stopped");
        let store = ContainerStore::new(data_dir.clone());

        store
            .upsert(ContainerInfo {
                id: "already-done".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:ddd".to_string(),
                status: ContainerStatus::Stopped { exit_code: 42 },
                created_unix_secs: 50,
                started_unix_secs: Some(51),
                stopped_unix_secs: Some(60),
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        let _runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        let containers = ContainerStore::new(data_dir).load_all().unwrap();
        let c = containers.iter().find(|c| c.id == "already-done").unwrap();
        assert!(matches!(
            c.status,
            ContainerStatus::Stopped { exit_code: 42 }
        ));
        assert_eq!(c.stopped_unix_secs, Some(60));
    }

    #[test]
    fn crash_recovery_mixed_state_reconciles_correctly() {
        let data_dir = unique_temp_dir("crash-mixed");
        let rootfs_root = data_dir.join("rootfs");
        let store = ContainerStore::new(data_dir.clone());

        // Stale running container with rootfs.
        let stale_rootfs = rootfs_root.join("stale-ctr");
        fs::create_dir_all(&stale_rootfs).unwrap();
        store
            .upsert(ContainerInfo {
                id: "stale-ctr".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:s1".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 100,
                started_unix_secs: Some(101),
                stopped_unix_secs: None,
                rootfs_path: Some(stale_rootfs.clone()),
                host_pid: Some(999_999_999),
            })
            .unwrap();

        // Alive running container with rootfs.
        let alive_rootfs = rootfs_root.join("alive-ctr");
        fs::create_dir_all(&alive_rootfs).unwrap();
        store
            .upsert(ContainerInfo {
                id: "alive-ctr".to_string(),
                image: "alpine:3.22".to_string(),
                image_id: "sha256:a1".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 200,
                started_unix_secs: Some(201),
                stopped_unix_secs: None,
                rootfs_path: Some(alive_rootfs.clone()),
                host_pid: Some(process::id()),
            })
            .unwrap();

        // Already stopped container.
        store
            .upsert(ContainerInfo {
                id: "stopped-ctr".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:p1".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 50,
                started_unix_secs: Some(51),
                stopped_unix_secs: Some(60),
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        // Orphaned rootfs with no container record.
        let orphan_rootfs = rootfs_root.join("orphan-dir");
        fs::create_dir_all(&orphan_rootfs).unwrap();

        // Simulate restart.
        let _runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        let containers = ContainerStore::new(data_dir).load_all().unwrap();
        assert_eq!(containers.len(), 3);

        // Stale container: reconciled to stopped, rootfs cleaned.
        let stale = containers.iter().find(|c| c.id == "stale-ctr").unwrap();
        assert!(matches!(
            stale.status,
            ContainerStatus::Stopped { exit_code: -1 }
        ));
        assert!(stale.rootfs_path.is_none());
        assert!(!stale_rootfs.exists());

        // Alive container: untouched, rootfs preserved.
        let alive = containers.iter().find(|c| c.id == "alive-ctr").unwrap();
        assert!(matches!(alive.status, ContainerStatus::Running));
        assert!(alive_rootfs.is_dir());

        // Stopped container: unchanged.
        let stopped = containers.iter().find(|c| c.id == "stopped-ctr").unwrap();
        assert!(matches!(
            stopped.status,
            ContainerStatus::Stopped { exit_code: 0 }
        ));

        // Orphaned rootfs: cleaned up.
        assert!(!orphan_rootfs.exists());
    }

    #[test]
    fn crash_recovery_is_idempotent() {
        let data_dir = unique_temp_dir("crash-idempotent");
        let store = ContainerStore::new(data_dir.clone());

        store
            .upsert(ContainerInfo {
                id: "stale-idem".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:idem".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 100,
                started_unix_secs: Some(101),
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(999_999_999),
            })
            .unwrap();

        // First restart — reconciles the stale container.
        let _rt1 = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        let after_first = ContainerStore::new(data_dir.clone()).load_all().unwrap();
        let c1 = after_first.iter().find(|c| c.id == "stale-idem").unwrap();
        assert!(matches!(
            c1.status,
            ContainerStatus::Stopped { exit_code: -1 }
        ));
        let stopped_ts = c1.stopped_unix_secs;

        // Second restart — should produce identical state.
        let _rt2 = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        let after_second = ContainerStore::new(data_dir).load_all().unwrap();
        let c2 = after_second.iter().find(|c| c.id == "stale-idem").unwrap();
        assert!(matches!(
            c2.status,
            ContainerStatus::Stopped { exit_code: -1 }
        ));
        // Timestamp should not be overwritten on second restart since it's already Stopped.
        assert_eq!(c2.stopped_unix_secs, stopped_ts);
    }

    #[test]
    fn crash_recovery_stale_container_with_no_pid_is_reconciled() {
        let data_dir = unique_temp_dir("crash-no-pid");
        let store = ContainerStore::new(data_dir.clone());

        // A Created container with no host_pid — the creating process crashed
        // before recording its PID.
        store
            .upsert(ContainerInfo {
                id: "no-pid".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:nopid".to_string(),
                status: ContainerStatus::Created,
                created_unix_secs: 100,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        let _runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        let containers = ContainerStore::new(data_dir).load_all().unwrap();
        let c = containers.iter().find(|c| c.id == "no-pid").unwrap();
        // host_pid is None → is_some_and returns false → treated as stale.
        assert!(matches!(
            c.status,
            ContainerStatus::Stopped { exit_code: -1 }
        ));
    }

    #[test]
    fn crash_recovery_metadata_persists_across_restarts() {
        let data_dir = unique_temp_dir("crash-persist");
        let store = ContainerStore::new(data_dir.clone());

        store
            .upsert(ContainerInfo {
                id: "persist-1".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:p1".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 100,
                started_unix_secs: Some(101),
                stopped_unix_secs: Some(110),
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        store
            .upsert(ContainerInfo {
                id: "persist-2".to_string(),
                image: "alpine:3.22".to_string(),
                image_id: "sha256:p2".to_string(),
                status: ContainerStatus::Stopped { exit_code: 1 },
                created_unix_secs: 200,
                started_unix_secs: Some(201),
                stopped_unix_secs: Some(210),
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        // Restart #1
        let rt1 = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });
        let list1 = rt1.list_containers().unwrap();
        assert_eq!(list1.len(), 2);

        // Restart #2
        let rt2 = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });
        let list2 = rt2.list_containers().unwrap();
        assert_eq!(list2.len(), 2);

        // Original metadata is unchanged.
        let c1 = list2.iter().find(|c| c.id == "persist-1").unwrap();
        assert_eq!(c1.image, "ubuntu:24.04");
        assert_eq!(c1.started_unix_secs, Some(101));
        assert_eq!(c1.stopped_unix_secs, Some(110));

        let c2 = list2.iter().find(|c| c.id == "persist-2").unwrap();
        assert_eq!(c2.image, "alpine:3.22");
        assert!(matches!(
            c2.status,
            ContainerStatus::Stopped { exit_code: 1 }
        ));
    }

    #[tokio::test]
    async fn crash_recovery_reconciled_container_can_be_removed() {
        let data_dir = unique_temp_dir("crash-remove");
        let store = ContainerStore::new(data_dir.clone());

        store
            .upsert(ContainerInfo {
                id: "remove-me".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:rm".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 100,
                started_unix_secs: Some(101),
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(999_999_999),
            })
            .unwrap();

        // Restart reconciles it to Stopped.
        let runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });

        // Removing the reconciled (now Stopped) container should succeed.
        runtime.remove_container("remove-me").await.unwrap();

        let remaining = runtime.list_containers().unwrap();
        assert!(remaining.is_empty());
    }

    // ── B15: Lifecycle conformance harness ──

    #[tokio::test]
    async fn lifecycle_stop_nonrunning_container_is_noop() {
        let data_dir = unique_temp_dir("lc-stop-noop");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        // Seed a Stopped container.
        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "stopped-ctr".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:s1".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 100,
                started_unix_secs: Some(101),
                stopped_unix_secs: Some(110),
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        // Stopping a non-running container returns it unchanged.
        let result = runtime.stop_container("stopped-ctr", false).await.unwrap();
        assert!(matches!(
            result.status,
            ContainerStatus::Stopped { exit_code: 0 }
        ));
        assert_eq!(result.stopped_unix_secs, Some(110));
    }

    #[tokio::test]
    async fn lifecycle_stop_created_container_is_noop() {
        let data_dir = unique_temp_dir("lc-stop-created");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "created-ctr".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:c1".to_string(),
                status: ContainerStatus::Created,
                created_unix_secs: 100,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(process::id()),
            })
            .unwrap();

        let result = runtime.stop_container("created-ctr", false).await.unwrap();
        assert!(matches!(result.status, ContainerStatus::Created));
    }

    #[tokio::test]
    async fn lifecycle_stop_missing_container_returns_error() {
        let data_dir = unique_temp_dir("lc-stop-missing");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        let err = runtime
            .stop_container("nonexistent", false)
            .await
            .unwrap_err();
        assert!(matches!(err, OciError::Storage(_)));
        if let OciError::Storage(io_err) = err {
            assert_eq!(io_err.kind(), io::ErrorKind::NotFound);
        }
    }

    #[tokio::test]
    async fn lifecycle_remove_missing_container_returns_error() {
        let data_dir = unique_temp_dir("lc-remove-missing");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        let err = runtime.remove_container("nonexistent").await.unwrap_err();
        assert!(matches!(err, OciError::Storage(_)));
        if let OciError::Storage(io_err) = err {
            assert_eq!(io_err.kind(), io::ErrorKind::NotFound);
        }
    }

    #[tokio::test]
    async fn lifecycle_remove_created_container_succeeds() {
        let data_dir = unique_temp_dir("lc-remove-created");
        let rootfs = data_dir.join("rootfs").join("ctr-created");
        fs::create_dir_all(&rootfs).unwrap();

        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "ctr-created".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:c1".to_string(),
                status: ContainerStatus::Created,
                created_unix_secs: 100,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: Some(rootfs.clone()),
                host_pid: Some(process::id()),
            })
            .unwrap();

        runtime.remove_container("ctr-created").await.unwrap();
        assert!(runtime.list_containers().unwrap().is_empty());
        assert!(!rootfs.exists());
    }

    #[tokio::test]
    async fn lifecycle_remove_stopped_container_cleans_rootfs() {
        let data_dir = unique_temp_dir("lc-remove-stopped-rootfs");
        let rootfs = data_dir.join("rootfs").join("ctr-stopped");
        fs::create_dir_all(&rootfs).unwrap();

        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "ctr-stopped".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:s1".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 100,
                started_unix_secs: Some(101),
                stopped_unix_secs: Some(110),
                rootfs_path: Some(rootfs.clone()),
                host_pid: None,
            })
            .unwrap();

        runtime.remove_container("ctr-stopped").await.unwrap();
        assert!(runtime.list_containers().unwrap().is_empty());
        assert!(!rootfs.exists());
    }

    #[tokio::test]
    async fn lifecycle_exec_on_stopped_container_returns_error() {
        let data_dir = unique_temp_dir("lc-exec-stopped");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "stopped-exec".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:se".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 100,
                started_unix_secs: Some(101),
                stopped_unix_secs: Some(110),
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        let err = runtime
            .exec_container(
                "stopped-exec",
                ExecConfig {
                    cmd: vec!["echo".to_string(), "hello".to_string()],
                    ..ExecConfig::default()
                },
            )
            .await
            .unwrap_err();

        // No VM handle exists for a stopped container.
        assert!(matches!(err, OciError::InvalidConfig(ref msg) if msg.contains("not be running")));
    }

    #[tokio::test]
    async fn lifecycle_exec_on_created_container_returns_error() {
        let data_dir = unique_temp_dir("lc-exec-created");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "created-exec".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:ce".to_string(),
                status: ContainerStatus::Created,
                created_unix_secs: 100,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(process::id()),
            })
            .unwrap();

        let err = runtime
            .exec_container(
                "created-exec",
                ExecConfig {
                    cmd: vec!["echo".to_string()],
                    ..ExecConfig::default()
                },
            )
            .await
            .unwrap_err();

        // No VM handle for a Created container that hasn't started.
        assert!(matches!(err, OciError::InvalidConfig(ref msg) if msg.contains("not be running")));
    }

    #[tokio::test]
    async fn lifecycle_exec_on_missing_container_returns_error() {
        let data_dir = unique_temp_dir("lc-exec-missing");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        let err = runtime
            .exec_container(
                "ghost",
                ExecConfig {
                    cmd: vec!["echo".to_string()],
                    ..ExecConfig::default()
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(err, OciError::InvalidConfig(ref msg) if msg.contains("not be running")));
    }

    #[test]
    fn lifecycle_list_containers_returns_all_states() {
        let data_dir = unique_temp_dir("lc-list-all");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "created-1".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:a".to_string(),
                status: ContainerStatus::Created,
                created_unix_secs: 100,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(process::id()),
            })
            .unwrap();

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "running-1".to_string(),
                image: "alpine:3.22".to_string(),
                image_id: "sha256:b".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 200,
                started_unix_secs: Some(201),
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(process::id()),
            })
            .unwrap();

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "stopped-1".to_string(),
                image: "debian:bookworm".to_string(),
                image_id: "sha256:c".to_string(),
                status: ContainerStatus::Stopped { exit_code: 42 },
                created_unix_secs: 50,
                started_unix_secs: Some(51),
                stopped_unix_secs: Some(60),
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        let list = runtime.list_containers().unwrap();
        assert_eq!(list.len(), 3);

        // Sorted by ID.
        assert_eq!(list[0].id, "created-1");
        assert!(matches!(list[0].status, ContainerStatus::Created));
        assert_eq!(list[1].id, "running-1");
        assert!(matches!(list[1].status, ContainerStatus::Running));
        assert_eq!(list[2].id, "stopped-1");
        assert!(matches!(
            list[2].status,
            ContainerStatus::Stopped { exit_code: 42 }
        ));
    }

    #[tokio::test]
    async fn lifecycle_double_remove_returns_not_found() {
        let data_dir = unique_temp_dir("lc-double-remove");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir,
            ..RuntimeConfig::default()
        });

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "once".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:once".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 100,
                started_unix_secs: Some(101),
                stopped_unix_secs: Some(110),
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        runtime.remove_container("once").await.unwrap();

        // Second remove should fail with NotFound.
        let err = runtime.remove_container("once").await.unwrap_err();
        assert!(matches!(err, OciError::Storage(_)));
    }

    #[tokio::test]
    async fn lifecycle_oci_sequence_create_start_exec_delete() {
        // Validates the mock OCI lifecycle sequence end-to-end.
        let mock = MockOciLifecycleOps::new(ExecOutput {
            exit_code: 0,
            stdout: "world".to_string(),
            stderr: String::new(),
        });

        let output = run_oci_lifecycle(
            &mock,
            "conformance-ctr".to_string(),
            "/run/vz-oci/bundles/conformance-ctr".to_string(),
            "/bin/echo".to_string(),
            vec!["hello".to_string()],
            OciExecOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(output.exit_code, 0);
        assert_eq!(output.stdout, "world");
        let calls = mock.calls.lock().unwrap();
        assert_eq!(calls.as_slice(), &["create", "start", "exec", "delete"]);
    }

    #[tokio::test]
    async fn lifecycle_oci_kill_graceful_then_state() {
        let mock = MockOciLifecycleOps::new(ExecOutput {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        });

        let exit_code = stop_via_oci_runtime(&mock, "kill-test", false, Duration::from_secs(5))
            .await
            .unwrap();

        // SIGTERM exit convention: 128 + 15 = 143.
        assert_eq!(exit_code, 143);
        let calls = mock.calls.lock().unwrap();
        assert!(calls.contains(&"kill:SIGTERM"));
        assert!(calls.contains(&"state"));
        assert!(!calls.contains(&"kill:SIGKILL"));
    }

    #[tokio::test]
    async fn lifecycle_oci_kill_forced_sends_sigkill() {
        let mock = MockOciLifecycleOps::new(ExecOutput {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        });

        let exit_code = stop_via_oci_runtime(&mock, "force-kill", true, Duration::from_secs(5))
            .await
            .unwrap();

        // SIGKILL exit convention: 128 + 9 = 137.
        assert_eq!(exit_code, 137);
        let calls = mock.calls.lock().unwrap();
        assert!(calls.contains(&"kill:SIGKILL"));
        // Forced kill should not attempt SIGTERM first.
        assert!(!calls.contains(&"kill:SIGTERM"));
    }

    #[tokio::test]
    async fn lifecycle_oci_delete_after_start_failure() {
        let mut mock = MockOciLifecycleOps::new(ExecOutput {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        });
        mock.fail_start = true;

        let err = run_oci_lifecycle(
            &mock,
            "fail-start".to_string(),
            "/run/vz-oci/bundles/fail-start".to_string(),
            "/bin/echo".to_string(),
            vec![],
            OciExecOptions::default(),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, OciError::InvalidConfig(_)));
        let calls = mock.calls.lock().unwrap();
        // create → start (fails) → delete (cleanup).
        assert_eq!(calls.as_slice(), &["create", "start", "delete"]);
    }

    #[tokio::test]
    async fn lifecycle_oci_exec_with_env_and_cwd() {
        let mock = MockOciLifecycleOps::new(ExecOutput {
            exit_code: 0,
            stdout: "ok".to_string(),
            stderr: String::new(),
        });

        let _ = run_oci_lifecycle(
            &mock,
            "env-cwd-ctr".to_string(),
            "/run/vz-oci/bundles/env-cwd-ctr".to_string(),
            "/usr/bin/env".to_string(),
            vec![],
            OciExecOptions {
                env: vec![("FOO".to_string(), "bar".to_string())],
                cwd: Some("/workspace".to_string()),
                user: Some("1000:1000".to_string()),
            },
        )
        .await
        .unwrap();

        let recorded = mock.exec_call.lock().unwrap();
        let exec = recorded.as_ref().unwrap();
        assert_eq!(exec.command, "/usr/bin/env");
        assert_eq!(
            exec.options.env,
            vec![("FOO".to_string(), "bar".to_string())]
        );
        assert_eq!(exec.options.cwd, Some("/workspace".to_string()));
        assert_eq!(exec.options.user, Some("1000:1000".to_string()));
    }
}
