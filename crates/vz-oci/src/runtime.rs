use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{fs, process};

use crate::container_store::{ContainerInfo, ContainerStatus, ContainerStore};
use crate::image::{
    ImageConfigSummary, ImageId, ImagePuller, parse_image_config_summary_from_store,
};
use crate::store::ImageStore;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::warn;
use vz::NetworkConfig;
use vz::Vm;
use vz::protocol::ExecOutput;
use vz_linux::{
    EnsureKernelOptions, ExecOptions, LinuxError, LinuxVm, LinuxVmConfig,
    ensure_kernel_with_options,
};

use crate::config::{PortMapping, PortProtocol, RunConfig, RuntimeBackend, RuntimeConfig};
use crate::error::OciError;
use crate::store::{ImageInfo, PruneResult};

const STOP_GRACE_PERIOD: Duration = Duration::from_secs(10);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Unified runtime entrypoint.
#[derive(Clone)]
pub struct Runtime {
    config: RuntimeConfig,
    store: ImageStore,
    container_store: ContainerStore,
    puller: ImagePuller,
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
        };

        runtime.cleanup_orphaned_rootfs();

        runtime
    }

    /// Return configured data directory.
    pub fn data_dir(&self) -> &PathBuf {
        &self.config.data_dir
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
    pub fn remove_container(&self, id: &str) -> Result<(), OciError> {
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

        self.container_store.remove(id).map_err(OciError::from)?;

        if let Some(path) = container.rootfs_path {
            let _ = fs::remove_dir_all(path);
        }

        Ok(())
    }

    /// Stop a running container by signaling its managing host process.
    pub fn stop_container(&self, id: &str, force: bool) -> Result<ContainerInfo, OciError> {
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

        if let Some(pid) = container.host_pid {
            if is_pid_alive(pid) {
                send_signal(pid, force)?;

                if !force && !wait_for_pid_exit(pid, STOP_GRACE_PERIOD) {
                    return Err(OciError::InvalidConfig(format!(
                        "container process {pid} did not stop within {}s",
                        STOP_GRACE_PERIOD.as_secs()
                    )));
                }
            }
        }

        if let Some(rootfs_path) = container.rootfs_path.take() {
            let _ = fs::remove_dir_all(rootfs_path);
        }

        container.host_pid = None;
        container.status = ContainerStatus::Stopped {
            exit_code: if force { -9 } else { 143 },
        };
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
                container.host_pid = None;
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                return Err(err.into());
            }
        };

        container.rootfs_path = Some(rootfs_dir.clone());
        container.status = ContainerStatus::Running;
        container.host_pid = Some(process::id());
        self.container_store
            .upsert(container.clone())
            .map_err(OciError::from)?;

        let image_config = parse_image_config_summary_from_store(&self.store, &image_id.0)?;
        let run = resolve_run_config(image_config, run, &container_id)?;

        let output = self.run_rootfs(&rootfs_dir, run).await;
        self.cleanup_rootfs_dir(rootfs_dir.as_ref());

        container.status = match &output {
            Ok(exec_output) => ContainerStatus::Stopped {
                exit_code: exec_output.exit_code,
            },
            Err(_) => ContainerStatus::Stopped { exit_code: -1 },
        };
        container.host_pid = None;

        self.container_store
            .upsert(container)
            .map_err(OciError::from)?;

        output
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
            working_dir,
            env,
            user,
            ports,
            cpus,
            memory_mb,
            network_enabled,
            serial_log_file,
            timeout,
            container_id: _,
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

        let mut vm_config =
            LinuxVmConfig::new(kernel.kernel, kernel.initramfs).with_rootfs_dir(rootfs_dir);
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
        let listener_mapping = *mapping;

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
                                connection_tasks.spawn(async move {
                                    if let Err(error) = relay_port_forward_connection(
                                        connection_vm,
                                        host_stream,
                                        listener_mapping,
                                    )
                                    .await
                                    {
                                        warn!(
                                            host_port = listener_mapping.host,
                                            container_port = listener_mapping.container,
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
    )
    .await?;

    tokio::io::copy_bidirectional(&mut host_stream, &mut guest_stream)
        .await
        .map_err(|error| LinuxError::Protocol(format!("port forward relay failed: {error}")))?;

    Ok(())
}

fn send_signal(pid: u32, force: bool) -> Result<(), OciError> {
    let signal = if force { "-KILL" } else { "-TERM" };
    let status = process::Command::new("kill")
        .arg(signal)
        .arg(pid.to_string())
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::null())
        .status()
        .map_err(OciError::Storage)?;

    if status.success() || !is_pid_alive(pid) {
        return Ok(());
    }

    Err(OciError::InvalidConfig(format!(
        "failed to send {signal} to process {pid}"
    )))
}

fn is_pid_alive(pid: u32) -> bool {
    process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn wait_for_pid_exit(pid: u32, timeout: Duration) -> bool {
    let started = Instant::now();

    loop {
        if !is_pid_alive(pid) {
            return true;
        }

        let elapsed = started.elapsed();
        if elapsed >= timeout {
            return false;
        }

        let remaining = timeout.saturating_sub(elapsed);
        std::thread::sleep(std::cmp::min(STOP_POLL_INTERVAL, remaining));
    }
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
        working_dir: run_working_dir,
        env: run_env,
        user: run_user,
        ports,
        cpus,
        memory_mb,
        network_enabled,
        serial_log_file,
        timeout,
        container_id: _,
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

    Ok(RunConfig {
        cmd: resolved_cmd,
        working_dir,
        env: resolved_env,
        user,
        ports,
        cpus,
        memory_mb,
        network_enabled,
        serial_log_file,
        timeout,
        container_id: Some(container_id.to_string()),
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
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        let containers = runtime.list_containers().unwrap();

        assert_eq!(containers.len(), 2);
        assert_eq!(containers[0].id, "container-1");
        assert_eq!(containers[1].id, "container-2");
    }

    #[test]
    fn runtime_remove_container_removes_metadata_and_rootfs() {
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
                rootfs_path: Some(rootfs_path.clone()),
                host_pid: None,
            })
            .unwrap();

        runtime.remove_container("container-1").unwrap();

        assert!(!rootfs_path.exists());
        assert!(runtime.list_containers().unwrap().is_empty());

        let missing = runtime.remove_container("container-1");
        let err = missing.err().unwrap();
        assert!(matches!(err, OciError::Storage(_)));
        if let OciError::Storage(io_err) = err {
            assert_eq!(io_err.kind(), io::ErrorKind::NotFound);
        }
    }

    #[test]
    fn runtime_remove_container_rejects_running_container() {
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
                rootfs_path: None,
                host_pid: Some(process::id()),
            })
            .unwrap();

        let error = runtime.remove_container("container-run").unwrap_err();
        assert!(matches!(error, OciError::InvalidConfig(_)));
    }

    #[test]
    fn runtime_stop_container_marks_container_stopped_and_cleans_rootfs() {
        let data_dir = unique_temp_dir("stop");
        let runtime = Runtime::new(RuntimeConfig {
            data_dir: data_dir.clone(),
            ..RuntimeConfig::default()
        });
        let rootfs_path = data_dir.join("rootfs-stop");
        fs::create_dir_all(&rootfs_path).unwrap();

        runtime
            .container_store
            .upsert(ContainerInfo {
                id: "container-1".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:img1".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 100,
                rootfs_path: Some(rootfs_path.clone()),
                host_pid: Some(4_000_000),
            })
            .unwrap();

        let stopped = runtime.stop_container("container-1", false).unwrap();

        assert!(matches!(
            stopped.status,
            ContainerStatus::Stopped { exit_code: 143 }
        ));
        assert!(stopped.rootfs_path.is_none());
        assert!(stopped.host_pid.is_none());
        assert!(!rootfs_path.exists());
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
                rootfs_path: Some(referenced_rootfs.clone()),
                host_pid: None,
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

    #[test]
    fn expand_home_dir_resolves_tilde_prefix() {
        let Some(home) = std::env::var_os("HOME") else {
            return;
        };

        let resolved = expand_home_dir(Path::new("~/.vz/oci"));
        assert_eq!(resolved, PathBuf::from(home).join(".vz/oci"));
    }
}
