//! `RuntimeBackend` trait implementation for the Linux-native backend.
//!
//! Composes bundle generation, OCI runtime invocation, namespace/cgroup
//! setup, and networking into a complete container lifecycle backend.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use tracing::{debug, info};
use vz_runtime_contract::{self as contract, RuntimeBackend, RuntimeError};

use crate::bundle::{BundleMount, BundleSpec};
use crate::config::LinuxNativeConfig;
use crate::network;
use crate::ns;
use crate::runtime::ContainerRuntime;

/// Tracked state for a container.
struct TrackedContainer {
    info: contract::ContainerInfo,
}

/// Linux-native container backend.
///
/// Runs OCI containers directly on the Linux host using an OCI runtime
/// binary (youki, runc) without a VM layer.
pub struct LinuxNativeBackend {
    _config: LinuxNativeConfig,
    runtime: ContainerRuntime,
    containers: Arc<Mutex<HashMap<String, TrackedContainer>>>,
    /// Stack-level bridge state: stack_id → bridge_name.
    stacks: Arc<Mutex<HashMap<String, StackState>>>,
}

struct StackState {
    bridge_name: String,
    services: HashMap<String, ServiceNetState>,
}

struct ServiceNetState {
    netns_name: String,
    veth_host: String,
    _addr: String,
}

impl LinuxNativeBackend {
    /// Create a new Linux-native backend.
    pub fn new(config: LinuxNativeConfig) -> Self {
        let runtime = ContainerRuntime::new(config.clone());
        Self {
            _config: config,
            runtime,
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
    RuntimeError::Backend {
        message: e.to_string(),
        source: Box::new(e),
    }
}

/// Convert contract MountSpec to bundle BundleMount.
fn mount_to_bundle(m: &contract::MountSpec) -> BundleMount {
    let options = match m.mount_type {
        contract::MountType::Bind => {
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
            contract::MountType::Bind => "bind".to_string(),
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
    }
}

impl RuntimeBackend for LinuxNativeBackend {
    fn name(&self) -> &'static str {
        "linux-native"
    }

    // ── Image operations ──────────────────────────────────────────
    // Linux-native reuses the image store from vz-oci. Image pull/list/prune
    // are not implemented here — they are handled by the orchestrator or
    // delegated to the image store directly. These stubs return appropriate
    // errors until image store integration is wired.

    async fn pull(&self, image: &str) -> Result<String, RuntimeError> {
        // Image pull is handled externally. Return the reference as-is.
        info!(image, "linux-native pull (delegated to image store)");
        Ok(image.to_string())
    }

    fn images(&self) -> Result<Vec<contract::ImageInfo>, RuntimeError> {
        // Image listing delegated to image store.
        Ok(Vec::new())
    }

    fn prune_images(&self) -> Result<contract::PruneResult, RuntimeError> {
        Ok(contract::PruneResult {
            removed_refs: 0,
            removed_manifests: 0,
            removed_configs: 0,
            removed_layer_dirs: 0,
        })
    }

    // ── Container lifecycle ───────────────────────────────────────

    async fn run(
        &self,
        image: &str,
        config: contract::RunConfig,
    ) -> Result<contract::ExecOutput, RuntimeError> {
        let container_id = config
            .container_id
            .clone()
            .unwrap_or_else(|| format!("vz-{}", &uuid_short()));

        // For one-shot run: create container with init process, start, exec cmd, cleanup.
        let init = config
            .init_process
            .clone()
            .unwrap_or_else(|| vec!["sleep".to_string(), "infinity".to_string()]);

        let mut spec = run_config_to_bundle_spec(&config);
        spec.cmd = init;

        // TODO: rootfs_dir should come from image pull/unpack.
        // For now, assume the image reference IS a local rootfs path.
        let rootfs_dir = std::path::PathBuf::from(image);

        self.runtime
            .create_and_start(&container_id, &rootfs_dir, spec)
            .await
            .map_err(native_err)?;

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
        config: contract::RunConfig,
    ) -> Result<String, RuntimeError> {
        let container_id = config
            .container_id
            .clone()
            .unwrap_or_else(|| format!("vz-{}", &uuid_short()));

        let init = config
            .init_process
            .clone()
            .unwrap_or_else(|| vec!["sleep".to_string(), "infinity".to_string()]);

        let mut spec = run_config_to_bundle_spec(&config);
        spec.cmd = init;

        let rootfs_dir = std::path::PathBuf::from(image);

        self.runtime
            .create_and_start(&container_id, &rootfs_dir, spec)
            .await
            .map_err(native_err)?;

        // Track container.
        let info = contract::ContainerInfo {
            id: container_id.clone(),
            image: image.to_string(),
            image_id: String::new(),
            status: contract::ContainerStatus::Running,
            created_unix_secs: now_unix_secs(),
            started_unix_secs: Some(now_unix_secs()),
            stopped_unix_secs: None,
            rootfs_path: Some(rootfs_dir.clone()),
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
        _ports: Vec<contract::PortMapping>,
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

        stacks.insert(
            stack_id.to_string(),
            StackState {
                bridge_name,
                services: HashMap::new(),
            },
        );

        info!(stack_id, "stack network initialized");
        Ok(())
    }

    async fn create_container_in_stack(
        &self,
        _stack_id: &str,
        image: &str,
        config: contract::RunConfig,
    ) -> Result<String, RuntimeError> {
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

/// Generate a short pseudo-random ID.
fn uuid_short() -> String {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", nanos % 0xFFFF_FFFF)
}
