//! `RuntimeBackend` trait implementation for the Linux-native backend.
//!
//! Composes bundle generation, OCI runtime invocation, namespace/cgroup
//! setup, and networking into a complete container lifecycle backend.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use tracing::{debug, info};
use vz_image::{ImagePuller, ImageStore, parse_image_config_summary_from_store};
use vz_runtime_contract::{self as contract, RuntimeBackend, RuntimeError};

use crate::config::LinuxNativeConfig;
use crate::network;
use crate::ns;
use crate::runtime::ContainerRuntime;
use vz_oci::bundle::{BundleMount, BundleSpec};

/// Tracked state for a container.
struct TrackedContainer {
    info: contract::ContainerInfo,
}

/// Linux-native container backend.
///
/// Runs OCI containers directly on the Linux host using an OCI runtime
/// binary (youki, runc) without a VM layer.
pub struct LinuxNativeBackend {
    config: LinuxNativeConfig,
    runtime: ContainerRuntime,
    image_store: ImageStore,
    image_puller: ImagePuller,
    containers: Arc<Mutex<HashMap<String, TrackedContainer>>>,
    /// Stack-level bridge state: stack_id → bridge_name.
    stacks: Arc<Mutex<HashMap<String, StackState>>>,
}

struct StackState {
    bridge_name: String,
    services: HashMap<String, ServiceNetState>,
    port_forwards: Vec<PortForwardRule>,
}

struct ServiceNetState {
    netns_name: String,
    veth_host: String,
    _addr: String,
}

struct PortForwardRule {
    host_port: u16,
    dest_ip: String,
    container_port: u16,
    protocol: String,
}

impl LinuxNativeBackend {
    /// Create a new Linux-native backend.
    pub fn new(config: LinuxNativeConfig) -> Self {
        let runtime = ContainerRuntime::new(config.clone());
        let image_store = ImageStore::new(config.data_dir.join("oci"));
        let image_puller = ImagePuller::new(image_store.clone());
        Self {
            config,
            runtime,
            image_store,
            image_puller,
            containers: Arc::new(Mutex::new(HashMap::new())),
            stacks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn native_err(e: crate::error::LinuxNativeError) -> RuntimeError {
    match e {
        crate::error::LinuxNativeError::RuntimeBinaryNotFound { path } => {
            RuntimeError::UnsupportedOperation {
                operation: "oci_runtime_binary".to_string(),
                reason: format!("runtime binary not found at '{path}'"),
            }
        }
        other => RuntimeError::Backend {
            message: other.to_string(),
            source: Box::new(other),
        },
    }
}

/// Convert contract MountSpec to bundle BundleMount.
fn mount_to_bundle(m: &contract::MountSpec) -> BundleMount {
    let options = match m.mount_type {
        contract::MountType::Bind | contract::MountType::Volume { .. } => {
            let mut opts = vec!["rbind".to_string()];
            match m.access {
                contract::MountAccess::ReadWrite => opts.push("rw".to_string()),
                contract::MountAccess::ReadOnly => opts.push("ro".to_string()),
            }
            opts
        }
        contract::MountType::Tmpfs => {
            vec![
                "nosuid".to_string(),
                "nodev".to_string(),
                "mode=1777".to_string(),
            ]
        }
    };

    BundleMount {
        destination: m.target.clone(),
        source: m.source.clone().unwrap_or_default(),
        typ: match m.mount_type {
            contract::MountType::Bind | contract::MountType::Volume { .. } => "bind".to_string(),
            contract::MountType::Tmpfs => "tmpfs".to_string(),
        },
        options,
    }
}

/// Build a BundleSpec from RunConfig.
fn run_config_to_bundle_spec(config: &contract::RunConfig) -> BundleSpec {
    BundleSpec {
        cmd: config.cmd.clone(),
        env: config.env.clone(),
        cwd: config.working_dir.clone(),
        user: config.user.clone(),
        mounts: config.mounts.iter().map(mount_to_bundle).collect(),
        oci_annotations: config.oci_annotations.clone(),
        network_namespace_path: config.network_namespace_path.clone(),
        share_host_network: config.network_enabled == Some(false),
        cpu_quota: config.cpu_quota,
        cpu_period: config.cpu_period,
        capture_logs: config.capture_logs,
        cap_add: config.cap_add.clone(),
        cap_drop: config.cap_drop.clone(),
        privileged: config.privileged,
        read_only_rootfs: config.read_only_rootfs,
        sysctls: config.sysctls.iter().cloned().collect(),
        ulimits: config.ulimits.clone(),
        pids_limit: config.pids_limit,
        hostname: config.hostname.clone(),
        domainname: config.domainname.clone(),
    }
}

/// Pull image and assemble rootfs, returning the rootfs path and image id.
async fn pull_and_assemble(
    puller: &ImagePuller,
    store: &ImageStore,
    image: &str,
    auth: &vz_image::Auth,
    container_id: &str,
) -> Result<(std::path::PathBuf, String), crate::error::LinuxNativeError> {
    let image_id = puller.pull(image, auth).await?;
    let rootfs_dir = store
        .assemble_rootfs_async(&image_id.0, container_id)
        .await?;
    Ok((rootfs_dir, image_id.0))
}

/// Resolve image config into a RunConfig, applying image defaults.
fn apply_image_config(
    store: &ImageStore,
    image_id: &str,
    config: &mut contract::RunConfig,
    container_id: &str,
) -> Result<(), crate::error::LinuxNativeError> {
    let image_config = parse_image_config_summary_from_store(store, image_id)?;

    if config.cmd.is_empty() {
        config.cmd = image_config.resolve_cmd(&config.cmd).unwrap_or_default();
    }
    config.env = image_config.resolve_env(&config.env, container_id);
    if config.working_dir.is_none() {
        config.working_dir = image_config.resolve_working_dir(None);
    }
    if config.user.is_none() {
        config.user = image_config.resolve_user(None);
    }
    Ok(())
}

impl RuntimeBackend for LinuxNativeBackend {
    fn name(&self) -> &'static str {
        "linux-native"
    }

    fn capabilities(&self) -> contract::RuntimeCapabilities {
        contract::RuntimeCapabilities::stack_baseline()
    }

    // ── Image operations ──────────────────────────────────────────

    async fn pull(&self, image: &str) -> Result<String, RuntimeError> {
        let image_id = self
            .image_puller
            .pull(image, &self.config.auth)
            .await
            .map_err(|e| RuntimeError::PullFailed {
                reference: image.to_string(),
                reason: e.to_string(),
            })?;
        Ok(image_id.0)
    }

    fn images(&self) -> Result<Vec<contract::ImageInfo>, RuntimeError> {
        self.image_store
            .list_images()
            .map(|v| {
                v.into_iter()
                    .map(|i| contract::ImageInfo {
                        reference: i.reference,
                        image_id: i.image_id,
                    })
                    .collect()
            })
            .map_err(|e| RuntimeError::Backend {
                message: e.to_string(),
                source: Box::new(e),
            })
    }

    fn prune_images(&self) -> Result<contract::PruneResult, RuntimeError> {
        self.image_store
            .prune_images()
            .map(|p| contract::PruneResult {
                removed_refs: p.removed_refs,
                removed_manifests: p.removed_manifests,
                removed_configs: p.removed_configs,
                removed_layer_dirs: p.removed_layer_dirs,
            })
            .map_err(|e| RuntimeError::Backend {
                message: e.to_string(),
                source: Box::new(e),
            })
    }

    // ── Container lifecycle ───────────────────────────────────────

    async fn run(
        &self,
        image: &str,
        mut config: contract::RunConfig,
    ) -> Result<contract::ExecOutput, RuntimeError> {
        let container_id = config
            .container_id
            .clone()
            .unwrap_or_else(|| format!("vz-{}", &uuid_short()));

        let has_setup = !config.setup_commands.is_empty();

        // Deterministic commit reference: hash(image + sorted setup commands).
        let commit_ref = if has_setup {
            Some(setup_commit_reference(image, &config.setup_commands))
        } else {
            None
        };

        // If a committed rootfs exists for this image+setup combo, use it
        // directly — skip pull, layer assembly, and setup execution.
        let used_commit = if let Some(ref cref) = commit_ref {
            if self.image_store.has_committed_rootfs(cref) {
                let rootfs_dir = self
                    .image_store
                    .assemble_rootfs_from_commit_async(cref, &container_id)
                    .await
                    .map_err(|e| RuntimeError::Backend {
                        message: format!("failed to restore committed rootfs: {e}"),
                        source: Box::new(e),
                    })?;

                let init = config
                    .init_process
                    .clone()
                    .unwrap_or_else(|| vec!["sleep".to_string(), "infinity".to_string()]);

                let mut spec = run_config_to_bundle_spec(&config);
                spec.cmd = init;

                self.runtime
                    .create_and_start(&container_id, &rootfs_dir, spec)
                    .await
                    .map_err(native_err)?;

                true
            } else {
                false
            }
        } else {
            false
        };

        if !used_commit {
            // Fresh path: pull image, assemble rootfs from layers.
            let (rootfs_dir, image_id) = pull_and_assemble(
                &self.image_puller,
                &self.image_store,
                image,
                &self.config.auth,
                &container_id,
            )
            .await
            .map_err(native_err)?;

            apply_image_config(&self.image_store, &image_id, &mut config, &container_id)
                .map_err(native_err)?;

            let init = config
                .init_process
                .clone()
                .unwrap_or_else(|| vec!["sleep".to_string(), "infinity".to_string()]);

            let mut spec = run_config_to_bundle_spec(&config);
            spec.cmd = init;

            self.runtime
                .create_and_start(&container_id, &rootfs_dir, spec)
                .await
                .map_err(native_err)?;

            // Run setup commands inside the container, then commit.
            if has_setup {
                for setup_cmd in &config.setup_commands {
                    let setup_result = self
                        .runtime
                        .exec(
                            &container_id,
                            &["sh".to_string(), "-c".to_string(), setup_cmd.clone()],
                            &config.env,
                            config.working_dir.as_deref(),
                            config.user.as_deref(),
                            config.timeout,
                        )
                        .await
                        .map_err(native_err)?;

                    if setup_result.exit_code != 0 {
                        let _ = self.runtime.stop(&container_id, true).await;
                        let _ = self.runtime.delete(&container_id, true).await;
                        return Err(RuntimeError::Backend {
                            message: format!(
                                "setup command failed (exit {}): {}\nstderr: {}",
                                setup_result.exit_code, setup_cmd, setup_result.stderr,
                            ),
                            source: Box::new(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "setup command failed",
                            )),
                        });
                    }
                }

                // Commit the post-setup rootfs for future runs.
                if let Some(ref cref) = commit_ref {
                    // Stop the container so the rootfs is quiesced, then commit.
                    let _ = self.runtime.stop(&container_id, true).await;
                    let _ = self.runtime.delete(&container_id, true).await;

                    let commit_result = self
                        .image_store
                        .commit_rootfs_async(&container_id, cref)
                        .await;

                    if let Err(e) = &commit_result {
                        info!(error = %e, "failed to commit setup rootfs (non-fatal, setup will re-run next time)");
                    }

                    // Re-create a fresh container from the committed rootfs
                    // for the actual command execution.
                    let new_container_id = format!("vz-{}", &uuid_short());
                    let rootfs_dir = self
                        .image_store
                        .assemble_rootfs_from_commit_async(cref, &new_container_id)
                        .await
                        .map_err(|e| RuntimeError::Backend {
                            message: format!("failed to restore committed rootfs: {e}"),
                            source: Box::new(e),
                        })?;

                    let init = config
                        .init_process
                        .clone()
                        .unwrap_or_else(|| vec!["sleep".to_string(), "infinity".to_string()]);

                    let mut spec = run_config_to_bundle_spec(&config);
                    spec.cmd = init;

                    self.runtime
                        .create_and_start(&new_container_id, &rootfs_dir, spec)
                        .await
                        .map_err(native_err)?;

                    // Exec actual command in the committed container.
                    let result = self
                        .runtime
                        .exec(
                            &new_container_id,
                            &config.cmd,
                            &config.env,
                            config.working_dir.as_deref(),
                            config.user.as_deref(),
                            config.timeout,
                        )
                        .await
                        .map_err(native_err)?;

                    let _ = self.runtime.stop(&new_container_id, true).await;
                    let _ = self.runtime.delete(&new_container_id, true).await;

                    return Ok(contract::ExecOutput {
                        exit_code: result.exit_code,
                        stdout: result.stdout,
                        stderr: result.stderr,
                    });
                }
            }
        }

        // Exec the actual command.
        let result = self
            .runtime
            .exec(
                &container_id,
                &config.cmd,
                &config.env,
                config.working_dir.as_deref(),
                config.user.as_deref(),
                config.timeout,
            )
            .await
            .map_err(native_err)?;

        // Cleanup.
        let _ = self.runtime.stop(&container_id, true).await;
        let _ = self.runtime.delete(&container_id, true).await;

        Ok(contract::ExecOutput {
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
        })
    }

    async fn create_container(
        &self,
        image: &str,
        mut config: contract::RunConfig,
    ) -> Result<String, RuntimeError> {
        let container_id = config
            .container_id
            .clone()
            .unwrap_or_else(|| format!("vz-{}", &uuid_short()));

        let has_setup = !config.setup_commands.is_empty();
        let commit_ref = if has_setup {
            Some(setup_commit_reference(image, &config.setup_commands))
        } else {
            None
        };

        // Fast path: committed rootfs exists for this image+setup combo.
        if let Some(ref cref) = commit_ref {
            if self.image_store.has_committed_rootfs(cref) {
                info!(commit_ref = %cref, "using committed rootfs (setup already cached)");
                let rootfs_dir = self
                    .image_store
                    .assemble_rootfs_from_commit_async(cref, &container_id)
                    .await
                    .map_err(|e| RuntimeError::Backend {
                        message: format!("failed to restore committed rootfs: {e}"),
                        source: Box::new(e),
                    })?;

                let init = config
                    .init_process
                    .clone()
                    .unwrap_or_else(|| vec!["sleep".to_string(), "infinity".to_string()]);

                let mut spec = run_config_to_bundle_spec(&config);
                spec.cmd = init;

                self.runtime
                    .create_and_start(&container_id, &rootfs_dir, spec)
                    .await
                    .map_err(native_err)?;

                let info = contract::ContainerInfo {
                    id: container_id.clone(),
                    image: image.to_string(),
                    image_id: String::new(),
                    status: contract::ContainerStatus::Running,
                    created_unix_secs: now_unix_secs(),
                    started_unix_secs: Some(now_unix_secs()),
                    stopped_unix_secs: None,
                    rootfs_path: Some(rootfs_dir),
                    host_pid: None,
                };

                self.containers
                    .lock()
                    .await
                    .insert(container_id.clone(), TrackedContainer { info });

                return Ok(container_id);
            }
        }

        // Cold path: pull image and assemble rootfs from layers.
        let (rootfs_dir, image_id) = pull_and_assemble(
            &self.image_puller,
            &self.image_store,
            image,
            &self.config.auth,
            &container_id,
        )
        .await
        .map_err(native_err)?;

        apply_image_config(&self.image_store, &image_id, &mut config, &container_id)
            .map_err(native_err)?;

        let init = config
            .init_process
            .clone()
            .unwrap_or_else(|| vec!["sleep".to_string(), "infinity".to_string()]);

        let mut spec = run_config_to_bundle_spec(&config);
        spec.cmd = init;

        self.runtime
            .create_and_start(&container_id, &rootfs_dir, spec)
            .await
            .map_err(native_err)?;

        // Run setup commands, commit, and re-create from the committed rootfs.
        if has_setup {
            info!(
                num_commands = config.setup_commands.len(),
                "running setup commands"
            );
            for (i, setup_cmd) in config.setup_commands.iter().enumerate() {
                info!(step = i + 1, total = config.setup_commands.len(), cmd = %setup_cmd, "setup");
                let setup_result = self
                    .runtime
                    .exec(
                        &container_id,
                        &["sh".to_string(), "-c".to_string(), setup_cmd.clone()],
                        &config.env,
                        config.working_dir.as_deref(),
                        config.user.as_deref(),
                        config.timeout,
                    )
                    .await
                    .map_err(native_err)?;

                if setup_result.exit_code != 0 {
                    let _ = self.runtime.stop(&container_id, true).await;
                    let _ = self.runtime.delete(&container_id, true).await;
                    return Err(RuntimeError::Backend {
                        message: format!(
                            "setup command failed (exit {}): {}\nstderr: {}",
                            setup_result.exit_code, setup_cmd, setup_result.stderr,
                        ),
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "setup command failed",
                        )),
                    });
                }
            }

            if let Some(ref cref) = commit_ref {
                // Stop container to quiesce rootfs, then commit.
                let _ = self.runtime.stop(&container_id, true).await;
                let _ = self.runtime.delete(&container_id, true).await;

                match self
                    .image_store
                    .commit_rootfs_async(&container_id, cref)
                    .await
                {
                    Ok(_) => info!(commit_ref = %cref, "committed setup rootfs"),
                    Err(e) => info!(error = %e, "failed to commit setup rootfs (non-fatal)"),
                }

                // Re-create container from committed rootfs so the caller
                // gets a clean running container with setup baked in.
                let new_id = config
                    .container_id
                    .clone()
                    .unwrap_or_else(|| format!("vz-{}", &uuid_short()));

                let rootfs_dir = self
                    .image_store
                    .assemble_rootfs_from_commit_async(cref, &new_id)
                    .await
                    .map_err(|e| RuntimeError::Backend {
                        message: format!("failed to restore committed rootfs: {e}"),
                        source: Box::new(e),
                    })?;

                let init = config
                    .init_process
                    .clone()
                    .unwrap_or_else(|| vec!["sleep".to_string(), "infinity".to_string()]);

                let mut spec = run_config_to_bundle_spec(&config);
                spec.cmd = init;

                self.runtime
                    .create_and_start(&new_id, &rootfs_dir, spec)
                    .await
                    .map_err(native_err)?;

                let info = contract::ContainerInfo {
                    id: new_id.clone(),
                    image: image.to_string(),
                    image_id: image_id.clone(),
                    status: contract::ContainerStatus::Running,
                    created_unix_secs: now_unix_secs(),
                    started_unix_secs: Some(now_unix_secs()),
                    stopped_unix_secs: None,
                    rootfs_path: Some(rootfs_dir),
                    host_pid: None,
                };

                self.containers
                    .lock()
                    .await
                    .insert(new_id.clone(), TrackedContainer { info });

                return Ok(new_id);
            }
        }

        // No setup — track and return.
        let info = contract::ContainerInfo {
            id: container_id.clone(),
            image: image.to_string(),
            image_id,
            status: contract::ContainerStatus::Running,
            created_unix_secs: now_unix_secs(),
            started_unix_secs: Some(now_unix_secs()),
            stopped_unix_secs: None,
            rootfs_path: Some(rootfs_dir),
            host_pid: None,
        };

        self.containers
            .lock()
            .await
            .insert(container_id.clone(), TrackedContainer { info });

        Ok(container_id)
    }

    async fn exec_container(
        &self,
        id: &str,
        config: contract::ExecConfig,
    ) -> Result<contract::ExecOutput, RuntimeError> {
        let result = self
            .runtime
            .exec(
                id,
                &config.cmd,
                &config.env,
                config.working_dir.as_deref(),
                config.user.as_deref(),
                config.timeout,
            )
            .await
            .map_err(native_err)?;

        Ok(contract::ExecOutput {
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
        })
    }

    async fn stop_container(
        &self,
        id: &str,
        force: bool,
        _signal: Option<&str>,
        _grace_period: Option<std::time::Duration>,
    ) -> Result<contract::ContainerInfo, RuntimeError> {
        self.runtime.stop(id, force).await.map_err(native_err)?;

        let mut containers = self.containers.lock().await;
        if let Some(tracked) = containers.get_mut(id) {
            tracked.info.status = contract::ContainerStatus::Stopped { exit_code: 0 };
            tracked.info.stopped_unix_secs = Some(now_unix_secs());
            Ok(tracked.info.clone())
        } else {
            Ok(contract::ContainerInfo {
                id: id.to_string(),
                image: String::new(),
                image_id: String::new(),
                status: contract::ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 0,
                started_unix_secs: None,
                stopped_unix_secs: Some(now_unix_secs()),
                rootfs_path: None,
                host_pid: None,
            })
        }
    }

    async fn remove_container(&self, id: &str) -> Result<(), RuntimeError> {
        self.runtime.delete(id, true).await.map_err(native_err)?;
        self.containers.lock().await.remove(id);
        Ok(())
    }

    // ── Container commit ─────────────────────────────────────────

    async fn commit_container(&self, id: &str, reference: &str) -> Result<String, RuntimeError> {
        // Look up the container's rootfs path and extract the container_id
        // (last path component) while the lock is held, then drop the lock
        // before the async commit call.
        let container_id = {
            let containers = self.containers.lock().await;
            let tracked = containers
                .get(id)
                .ok_or_else(|| RuntimeError::ContainerNotFound { id: id.to_string() })?;
            let rootfs_path =
                tracked
                    .info
                    .rootfs_path
                    .as_ref()
                    .ok_or_else(|| RuntimeError::Backend {
                        message: format!("container '{id}' has no rootfs path"),
                        source: Box::new(crate::error::LinuxNativeError::InvalidConfig(
                            "no rootfs path".to_string(),
                        )),
                    })?;

            if !rootfs_path.exists() {
                return Err(RuntimeError::Backend {
                    message: format!("container rootfs not found: {}", rootfs_path.display()),
                    source: Box::new(crate::error::LinuxNativeError::InvalidConfig(
                        "rootfs does not exist".to_string(),
                    )),
                });
            }

            rootfs_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(id)
                .to_string()
        };

        self.image_store
            .commit_rootfs_async(&container_id, reference)
            .await
            .map_err(|e| RuntimeError::Backend {
                message: format!("failed to commit container rootfs: {e}"),
                source: Box::new(e),
            })
    }

    fn has_committed_rootfs(&self, reference: &str) -> bool {
        self.image_store.has_committed_rootfs(reference)
    }

    async fn create_container_from_commit(
        &self,
        reference: &str,
        config: contract::RunConfig,
    ) -> Result<String, RuntimeError> {
        let container_id = config
            .container_id
            .clone()
            .unwrap_or_else(|| format!("vz-{}", &uuid_short()));

        // Assemble rootfs from the committed snapshot.
        let rootfs_dir = self
            .image_store
            .assemble_rootfs_from_commit_async(reference, &container_id)
            .await
            .map_err(|e| RuntimeError::Backend {
                message: format!("failed to assemble rootfs from commit: {e}"),
                source: Box::new(e),
            })?;

        // We don't have the original image config, so only apply defaults
        // if cmd is empty (use sleep infinity as init).
        let init = config
            .init_process
            .clone()
            .unwrap_or_else(|| vec!["sleep".to_string(), "infinity".to_string()]);

        let mut spec = run_config_to_bundle_spec(&config);
        spec.cmd = init;

        self.runtime
            .create_and_start(&container_id, &rootfs_dir, spec)
            .await
            .map_err(native_err)?;

        // Track container.
        let info = contract::ContainerInfo {
            id: container_id.clone(),
            image: format!("commit:{reference}"),
            image_id: String::new(),
            status: contract::ContainerStatus::Running,
            created_unix_secs: now_unix_secs(),
            started_unix_secs: Some(now_unix_secs()),
            stopped_unix_secs: None,
            rootfs_path: Some(rootfs_dir),
            host_pid: None,
        };

        self.containers
            .lock()
            .await
            .insert(container_id.clone(), TrackedContainer { info });

        Ok(container_id)
    }

    fn list_containers(&self) -> Result<Vec<contract::ContainerInfo>, RuntimeError> {
        let containers = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async { self.containers.lock().await })
        });
        Ok(containers.values().map(|t| t.info.clone()).collect())
    }

    // ── Stack / multi-container support ───────────────────────────

    async fn boot_shared_vm(
        &self,
        stack_id: &str,
        ports: Vec<contract::PortMapping>,
        _resources: contract::StackResourceHint,
    ) -> Result<(), RuntimeError> {
        let mut stacks = self.stacks.lock().await;
        if stacks.contains_key(stack_id) {
            debug!(stack_id, "stack already booted");
            return Ok(());
        }

        let bridge_name = format!("vz-{}", &stack_id[..std::cmp::min(8, stack_id.len())]);

        network::create_bridge(&bridge_name, network::DEFAULT_BRIDGE_SUBNET)
            .await
            .map_err(native_err)?;

        // Enable NAT masquerade so containers can reach the internet.
        network::setup_nat_masquerade(&bridge_name, network::DEFAULT_BRIDGE_SUBNET)
            .await
            .map_err(native_err)?;

        // Set up port forwarding (DNAT) for each published port.
        let mut port_forwards = Vec::new();
        for pm in &ports {
            if let Some(ref dest_ip) = pm.target_host {
                let proto = match pm.protocol {
                    contract::PortProtocol::Udp => "udp",
                    contract::PortProtocol::Tcp => "tcp",
                };
                network::setup_port_forward(pm.host, dest_ip, pm.container, proto)
                    .await
                    .map_err(native_err)?;
                port_forwards.push(PortForwardRule {
                    host_port: pm.host,
                    dest_ip: dest_ip.clone(),
                    container_port: pm.container,
                    protocol: proto.to_string(),
                });
            }
        }

        stacks.insert(
            stack_id.to_string(),
            StackState {
                bridge_name,
                services: HashMap::new(),
                port_forwards,
            },
        );

        info!(stack_id, ports = ports.len(), "stack network initialized");
        Ok(())
    }

    async fn create_container_in_stack(
        &self,
        stack_id: &str,
        image: &str,
        mut config: contract::RunConfig,
    ) -> Result<String, RuntimeError> {
        // The executor sets network_namespace_path to /var/run/netns/{service_name},
        // but our netns names are vz-{stack_id}-{service_name}. Look up the correct
        // name from the stacks map and override the path.
        if let Some(ref ns_path) = config.network_namespace_path {
            if let Some(service_name) = ns_path.rsplit('/').next() {
                let stacks = self.stacks.lock().await;
                if let Some(stack) = stacks.get(stack_id) {
                    if let Some(svc) = stack.services.get(service_name) {
                        config.network_namespace_path =
                            Some(format!("/var/run/netns/{}", svc.netns_name));
                    }
                }
            }
        }
        self.create_container(image, config).await
    }

    async fn network_setup(
        &self,
        stack_id: &str,
        services: Vec<contract::NetworkServiceConfig>,
    ) -> Result<(), RuntimeError> {
        let mut stacks = self.stacks.lock().await;
        let stack = stacks
            .get_mut(stack_id)
            .ok_or_else(|| RuntimeError::Backend {
                message: format!("stack '{stack_id}' not found"),
                source: Box::new(crate::error::LinuxNativeError::InvalidConfig(format!(
                    "stack '{stack_id}' not booted"
                ))),
            })?;

        let gateway = "172.20.0.1";

        for svc in &services {
            let netns_name = format!("vz-{stack_id}-{}", &svc.name);
            let veth_host = format!("vh-{}", &svc.name[..std::cmp::min(11, svc.name.len())]);
            let veth_container = format!("vc-{}", &svc.name[..std::cmp::min(11, svc.name.len())]);

            ns::create_netns(&netns_name).await.map_err(native_err)?;

            network::wire_veth_to_netns(
                &stack.bridge_name,
                &netns_name,
                &veth_host,
                &veth_container,
                &svc.addr,
                gateway,
            )
            .await
            .map_err(native_err)?;

            stack.services.insert(
                svc.name.clone(),
                ServiceNetState {
                    netns_name,
                    veth_host,
                    _addr: svc.addr.clone(),
                },
            );
        }

        Ok(())
    }

    async fn network_teardown(
        &self,
        stack_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), RuntimeError> {
        let mut stacks = self.stacks.lock().await;
        let Some(stack) = stacks.get_mut(stack_id) else {
            return Ok(());
        };

        for name in &service_names {
            if let Some(svc) = stack.services.remove(name) {
                let _ = network::delete_veth(&svc.veth_host).await;
                let _ = ns::delete_netns(&svc.netns_name).await;
            }
        }

        Ok(())
    }

    async fn shutdown_shared_vm(&self, stack_id: &str) -> Result<(), RuntimeError> {
        let mut stacks = self.stacks.lock().await;
        let Some(stack) = stacks.remove(stack_id) else {
            return Ok(());
        };

        // Tear down port forwards.
        for pf in &stack.port_forwards {
            let _ = network::teardown_port_forward(
                pf.host_port,
                &pf.dest_ip,
                pf.container_port,
                &pf.protocol,
            )
            .await;
        }

        // Tear down NAT masquerade.
        let _ =
            network::teardown_nat_masquerade(&stack.bridge_name, network::DEFAULT_BRIDGE_SUBNET)
                .await;

        // Tear down all services.
        for (name, svc) in &stack.services {
            debug!(name, "tearing down service network");
            let _ = network::delete_veth(&svc.veth_host).await;
            let _ = ns::delete_netns(&svc.netns_name).await;
        }

        // Delete bridge.
        let _ = network::delete_bridge(&stack.bridge_name).await;

        info!(stack_id, "stack network shut down");
        Ok(())
    }

    fn has_shared_vm(&self, stack_id: &str) -> bool {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { self.stacks.lock().await.contains_key(stack_id) })
        })
    }
}

/// Build a deterministic commit reference from image + setup commands.
///
/// The reference is stable across runs so the runtime can detect when
/// a matching committed rootfs already exists and skip re-running setup.
fn setup_commit_reference(image: &str, setup_commands: &[String]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(image.as_bytes());
    for cmd in setup_commands {
        hasher.update(b"\x00"); // null separator to prevent collisions
        hasher.update(cmd.as_bytes());
    }
    let hash = format!("{:x}", hasher.finalize());
    format!("vz-setup:{image}:{hash}")
}

/// Generate a short pseudo-random ID.
fn uuid_short() -> String {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", nanos % 0xFFFF_FFFF)
}
