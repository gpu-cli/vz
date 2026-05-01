//! macOS Virtualization.framework backend adapter.
//!
//! Wraps the existing [`Runtime`] to implement the
//! [`RuntimeBackend`](vz_runtime_contract::RuntimeBackend) trait,
//! bridging between vz-oci internal types and the backend-neutral contract.

use vz_runtime_contract::{self as contract, RuntimeBackend, RuntimeError};

use crate::buildkit::{BuildManager, BuildManagerError};
use crate::config as oci_config;
use crate::runtime::Runtime;
use vz_oci::container_store as oci_container;

/// macOS backend wrapping the existing [`Runtime`].
pub struct MacosRuntimeBackend {
    runtime: Runtime,
    build_manager: BuildManager,
}

impl MacosRuntimeBackend {
    /// Create a new macOS backend from an existing runtime.
    pub fn new(runtime: Runtime) -> Self {
        let build_manager = BuildManager::new(runtime.clone_config());
        Self {
            runtime,
            build_manager,
        }
    }

    /// Access the underlying runtime.
    pub fn inner(&self) -> &Runtime {
        &self.runtime
    }

    /// Execute setup commands inside a running container.
    ///
    /// Runs each command as `sh -c <cmd>` in order. If any command
    /// fails, the container is stopped and removed.
    async fn run_setup_commands(
        &self,
        container_id: &str,
        commands: &[String],
        env: &[(String, String)],
        _working_dir: Option<&str>,
        user: Option<&str>,
        _timeout: Option<std::time::Duration>,
    ) -> Result<(), RuntimeError> {
        // Setup commands (apt install, rustup, etc.) can take minutes.
        let setup_timeout = Some(std::time::Duration::from_secs(1800));

        tracing::info!(num_commands = commands.len(), "running setup commands");
        for (i, cmd) in commands.iter().enumerate() {
            tracing::info!(step = i + 1, total = commands.len(), cmd = %cmd, "setup");
            // Ensure HOME is set for setup commands (e.g. rustup uses HOME
            // to decide where to install). Merge caller env with HOME fallback.
            let mut setup_env = env.to_vec();
            if !setup_env.iter().any(|(k, _)| k == "HOME") {
                setup_env.push(("HOME".to_string(), "/root".to_string()));
            }
            let exec_config = contract::ExecConfig {
                // Prefix with `cd /tmp` so getcwd() resolves — the kernel
                // can't resolve CWD through stacked overlay+VirtioFS mounts.
                cmd: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    format!("cd /tmp && {cmd}"),
                ],
                env: setup_env,
                working_dir: Some("/tmp".to_string()),
                user: user.map(str::to_string),
                timeout: setup_timeout,
                ..contract::ExecConfig::default()
            };
            let oci_config = exec_config_from_contract(exec_config);
            let result = self
                .runtime
                .exec_container(container_id, oci_config)
                .await
                .map(exec_output_to_contract)
                .map_err(oci_err)?;
            if result.exit_code != 0 {
                let _ = self
                    .stop_container(container_id, true, None, None)
                    .await;
                let _ = self.remove_container(container_id).await;
                return Err(RuntimeError::Backend {
                    message: format!(
                        "setup command failed (exit {}): {}\nstderr: {}",
                        result.exit_code, cmd, result.stderr,
                    ),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "setup command failed",
                    )),
                });
            }
        }
        tracing::info!("setup commands completed");
        Ok(())
    }

    pub async fn exec_container_streaming<F>(
        &self,
        id: &str,
        config: contract::ExecConfig,
        on_event: F,
    ) -> Result<contract::ExecOutput, RuntimeError>
    where
        F: FnMut(crate::runtime::InteractiveExecEvent),
    {
        let oci_config = exec_config_from_contract(config);
        self.runtime
            .exec_container_streaming(id, oci_config, on_event)
            .await
            .map(exec_output_to_contract)
            .map_err(oci_err)
    }
}

impl RuntimeBackend for MacosRuntimeBackend {
    fn name(&self) -> &'static str {
        "macos-vz"
    }

    fn capabilities(&self) -> contract::RuntimeCapabilities {
        self.runtime.checkpoint_capabilities()
    }

    async fn pull(&self, image: &str) -> Result<String, RuntimeError> {
        let id = self.runtime.pull(image).await.map_err(oci_err)?;
        Ok(id.0)
    }

    fn images(&self) -> Result<Vec<contract::ImageInfo>, RuntimeError> {
        self.runtime
            .images()
            .map(|v| v.into_iter().map(image_info_to_contract).collect())
            .map_err(oci_err)
    }

    fn prune_images(&self) -> Result<contract::PruneResult, RuntimeError> {
        self.runtime
            .prune_images()
            .map(prune_result_to_contract)
            .map_err(oci_err)
    }

    async fn run(
        &self,
        image: &str,
        config: contract::RunConfig,
    ) -> Result<contract::ExecOutput, RuntimeError> {
        let oci_config = run_config_from_contract(config);
        self.runtime
            .run(image, oci_config)
            .await
            .map(exec_output_to_contract)
            .map_err(oci_err)
    }

    async fn create_container(
        &self,
        image: &str,
        config: contract::RunConfig,
    ) -> Result<String, RuntimeError> {
        let setup_commands = config.setup_commands.clone();
        let setup_env = config.env.clone();
        let setup_cwd = config.working_dir.clone();
        let setup_user = config.user.clone();
        let setup_timeout = config.timeout;

        let oci_config = run_config_from_contract(config);
        let container_id = self
            .runtime
            .create_container(image, oci_config)
            .await
            .map_err(oci_err)?;

        if !setup_commands.is_empty() {
            self.run_setup_commands(
                &container_id,
                &setup_commands,
                &setup_env,
                setup_cwd.as_deref(),
                setup_user.as_deref(),
                setup_timeout,
            )
            .await?;
        }

        Ok(container_id)
    }

    async fn exec_container(
        &self,
        id: &str,
        config: contract::ExecConfig,
    ) -> Result<contract::ExecOutput, RuntimeError> {
        let oci_config = exec_config_from_contract(config);
        self.runtime
            .exec_container(id, oci_config)
            .await
            .map(exec_output_to_contract)
            .map_err(oci_err)
    }

    async fn write_exec_stdin(&self, execution_id: &str, data: &[u8]) -> Result<(), RuntimeError> {
        self.runtime
            .write_exec_stdin(execution_id, data)
            .await
            .map_err(oci_err)
    }

    async fn signal_exec(&self, execution_id: &str, signal: &str) -> Result<(), RuntimeError> {
        self.runtime
            .signal_exec(execution_id, signal)
            .await
            .map_err(oci_err)
    }

    async fn resize_exec_pty(
        &self,
        execution_id: &str,
        cols: u16,
        rows: u16,
    ) -> Result<(), RuntimeError> {
        self.runtime
            .resize_exec_pty(execution_id, cols, rows)
            .await
            .map_err(oci_err)
    }

    async fn cancel_exec(&self, execution_id: &str) -> Result<(), RuntimeError> {
        self.runtime
            .cancel_exec(execution_id)
            .await
            .map_err(oci_err)
    }

    async fn stop_container(
        &self,
        id: &str,
        force: bool,
        signal: Option<&str>,
        grace_period: Option<std::time::Duration>,
    ) -> Result<contract::ContainerInfo, RuntimeError> {
        self.runtime
            .stop_container(id, force, signal, grace_period)
            .await
            .map(container_info_to_contract)
            .map_err(oci_err)
    }

    async fn remove_container(&self, id: &str) -> Result<(), RuntimeError> {
        self.runtime.remove_container(id).await.map_err(oci_err)
    }

    fn list_containers(&self) -> Result<Vec<contract::ContainerInfo>, RuntimeError> {
        self.runtime
            .list_containers()
            .map(|v| v.into_iter().map(container_info_to_contract).collect())
            .map_err(oci_err)
    }

    async fn boot_shared_vm(
        &self,
        stack_id: &str,
        ports: Vec<contract::PortMapping>,
        resources: contract::StackResourceHint,
    ) -> Result<(), RuntimeError> {
        tracing::info!(
            target: "vz_post_stop",
            stack_id = %stack_id,
            in_count = ports.len(),
            sample_ports = ?ports.iter().take(4).map(|p| (p.host, p.container)).collect::<Vec<_>>(),
            "[L3/macos-backend] boot_shared_vm received contract ports"
        );
        let oci_ports: Vec<oci_config::PortMapping> =
            ports.into_iter().map(port_mapping_from_contract).collect();
        tracing::info!(
            target: "vz_post_stop",
            stack_id = %stack_id,
            out_count = oci_ports.len(),
            "[L3/macos-backend] mapped to oci_config::PortMapping; calling runtime.boot_shared_vm"
        );
        self.runtime
            .boot_shared_vm(stack_id, oci_ports, resources)
            .await
            .map_err(oci_err)
    }

    async fn create_container_in_stack(
        &self,
        stack_id: &str,
        image: &str,
        config: contract::RunConfig,
    ) -> Result<String, RuntimeError> {
        let setup_commands = config.setup_commands.clone();
        let setup_env = config.env.clone();
        let setup_cwd = config.working_dir.clone();
        let setup_user = config.user.clone();
        let setup_timeout = config.timeout;

        let oci_config = run_config_from_contract(config);
        let container_id = self
            .runtime
            .create_container_in_stack(stack_id, image, oci_config)
            .await
            .map_err(oci_err)?;

        // Run setup commands inside the container after creation.
        if !setup_commands.is_empty() {
            self.run_setup_commands(
                &container_id,
                &setup_commands,
                &setup_env,
                setup_cwd.as_deref(),
                setup_user.as_deref(),
                setup_timeout,
            )
            .await?;
        }

        Ok(container_id)
    }

    async fn network_setup(
        &self,
        stack_id: &str,
        services: Vec<contract::NetworkServiceConfig>,
    ) -> Result<(), RuntimeError> {
        let oci_services: Vec<vz::protocol::NetworkServiceConfig> = services
            .into_iter()
            .map(|s| vz::protocol::NetworkServiceConfig {
                name: s.name,
                addr: s.addr,
                network_name: s.network_name,
            })
            .collect();
        self.runtime
            .network_setup(stack_id, oci_services)
            .await
            .map_err(oci_err)
    }

    async fn network_teardown(
        &self,
        stack_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), RuntimeError> {
        self.runtime
            .network_teardown(stack_id, service_names)
            .await
            .map_err(oci_err)
    }

    async fn shutdown_shared_vm(&self, stack_id: &str) -> Result<(), RuntimeError> {
        self.runtime
            .shutdown_shared_vm(stack_id)
            .await
            .map_err(oci_err)
    }

    fn has_shared_vm(&self, stack_id: &str) -> bool {
        // Runtime::has_shared_vm is async (uses Mutex::lock().await).
        // Use block_in_place since this is called from sync context.
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.runtime.has_shared_vm(stack_id))
        })
    }

    fn logs(&self, container_id: &str) -> Result<contract::ContainerLogs, RuntimeError> {
        use crate::runtime::container_log_dir;

        // Read from the VM-level log directory (not inside the container).
        // This works even when the container's init process has exited.
        let log_path = format!("{}/output.log", container_log_dir(container_id));
        let exec_config = oci_config::ExecConfig {
            execution_id: None,
            cmd: vec!["tail".into(), "-n".into(), "100".into(), log_path],
            working_dir: None,
            env: vec![],
            user: None,
            pty: false,
            term_rows: None,
            term_cols: None,
            timeout: Some(std::time::Duration::from_secs(5)),
        };

        let output = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(self.runtime.exec_host(container_id, exec_config))
        })
        .map_err(oci_err)?;

        Ok(contract::ContainerLogs {
            output: if output.exit_code == 0 {
                output.stdout
            } else {
                String::new()
            },
        })
    }

    async fn start_build(
        &self,
        sandbox_id: &str,
        build_spec: contract::BuildSpec,
        idempotency_key: Option<String>,
    ) -> Result<contract::Build, RuntimeError> {
        self.build_manager
            .start_build(sandbox_id, build_spec, idempotency_key)
            .await
            .map_err(build_manager_err)
    }

    async fn get_build(&self, build_id: &str) -> Result<contract::Build, RuntimeError> {
        self.build_manager
            .get_build(build_id)
            .await
            .map_err(build_manager_err)
    }

    async fn stream_build_events(
        &self,
        build_id: &str,
        after_event_id: Option<u64>,
    ) -> Result<Vec<contract::Event>, RuntimeError> {
        self.build_manager
            .stream_build_events(build_id, after_event_id)
            .await
            .map_err(build_manager_err)
    }

    async fn cancel_build(&self, build_id: &str) -> Result<contract::Build, RuntimeError> {
        self.build_manager
            .cancel_build(build_id)
            .await
            .map_err(build_manager_err)
    }
}

// ── Error mapping ─────────────────────────────────────────────────

fn oci_err(e: crate::error::MacosOciError) -> RuntimeError {
    match e {
        crate::error::MacosOciError::InvalidConfig(message) => RuntimeError::InvalidConfig(message),
        crate::error::MacosOciError::InvalidRootfs { path } => RuntimeError::InvalidRootfs { path },
        crate::error::MacosOciError::Storage(source) => RuntimeError::Io(source),
        crate::error::MacosOciError::UnsupportedExecutionMode { mode } => {
            RuntimeError::UnsupportedOperation {
                operation: "execution_mode".to_string(),
                reason: format!("execution mode `{mode}` is not yet supported"),
            }
        }
        crate::error::MacosOciError::ExecutionSessionNotFound { execution_id } => {
            RuntimeError::ContainerNotFound { id: execution_id }
        }
        crate::error::MacosOciError::ExecutionControlUnsupported { operation, reason } => {
            RuntimeError::UnsupportedOperation { operation, reason }
        }
        other => RuntimeError::Backend {
            message: other.to_string(),
            source: Box::new(other),
        },
    }
}

fn build_manager_err(error: BuildManagerError) -> RuntimeError {
    match error {
        BuildManagerError::BuildNotFound { build_id } => RuntimeError::Backend {
            message: format!("build not found: {build_id}"),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("build not found: {build_id}"),
            )),
        },
        BuildManagerError::IdempotencyConflict {
            key,
            existing_build_id,
        } => RuntimeError::ContainerFailed {
            id: existing_build_id,
            reason: format!("idempotency key conflict: {key}"),
        },
        BuildManagerError::RequestNormalization { details } => {
            RuntimeError::InvalidConfig(format!("failed to normalize build request: {details}"))
        }
        BuildManagerError::Buildkit(source) => RuntimeError::Backend {
            message: source.to_string(),
            source: Box::new(source),
        },
    }
}

// ── Type conversions: contract → vz-oci ───────────────────────────

fn run_config_from_contract(c: contract::RunConfig) -> oci_config::RunConfig {
    oci_config::RunConfig {
        cmd: c.cmd,
        working_dir: c.working_dir,
        env: c.env,
        user: c.user,
        ports: c
            .ports
            .into_iter()
            .map(port_mapping_from_contract)
            .collect(),
        mounts: c.mounts.into_iter().map(mount_spec_from_contract).collect(),
        cpus: c.cpus,
        memory_mb: c.memory_mb,
        network_enabled: c.network_enabled,
        serial_log_file: None,
        timeout: c.timeout,
        execution_mode: oci_config::ExecutionMode::OciRuntime,
        container_id: c.container_id,
        init_process: c.init_process,
        oci_annotations: c.oci_annotations,
        extra_hosts: c.extra_hosts,
        network_namespace_path: c.network_namespace_path,
        cpu_quota: c.cpu_quota,
        cpu_period: c.cpu_period,
        capture_logs: c.capture_logs,
        cap_add: c.cap_add,
        cap_drop: c.cap_drop,
        privileged: c.privileged,
        read_only_rootfs: c.read_only_rootfs,
        sysctls: c.sysctls.into_iter().collect(),
        ulimits: c.ulimits,
        pids_limit: c.pids_limit,
        hostname: c.hostname,
        domainname: c.domainname,
        stop_signal: c.stop_signal,
        stop_grace_period_secs: c.stop_grace_period_secs,
        share_host_network: c.share_host_network,
        mount_tag_offset: c.mount_tag_offset,
    }
}

fn exec_config_from_contract(c: contract::ExecConfig) -> oci_config::ExecConfig {
    oci_config::ExecConfig {
        execution_id: c.execution_id,
        cmd: c.cmd,
        working_dir: c.working_dir,
        env: c.env,
        user: c.user,
        pty: c.pty,
        term_rows: c.term_rows,
        term_cols: c.term_cols,
        timeout: c.timeout,
    }
}

fn port_mapping_from_contract(p: contract::PortMapping) -> oci_config::PortMapping {
    oci_config::PortMapping {
        host: p.host,
        container: p.container,
        protocol: match p.protocol {
            contract::PortProtocol::Tcp => oci_config::PortProtocol::Tcp,
            contract::PortProtocol::Udp => oci_config::PortProtocol::Udp,
        },
        target_host: p.target_host,
    }
}

fn mount_spec_from_contract(m: contract::MountSpec) -> oci_config::MountSpec {
    oci_config::MountSpec {
        source: m.source,
        target: m.target,
        mount_type: match m.mount_type {
            contract::MountType::Bind => oci_config::MountType::Bind,
            contract::MountType::Tmpfs => oci_config::MountType::Tmpfs,
            contract::MountType::Volume { volume_name } => {
                oci_config::MountType::Volume { volume_name }
            }
        },
        access: match m.access {
            contract::MountAccess::ReadWrite => oci_config::MountAccess::ReadWrite,
            contract::MountAccess::ReadOnly => oci_config::MountAccess::ReadOnly,
        },
        subpath: m.subpath,
    }
}

// ── Type conversions: vz-oci → contract ───────────────────────────

fn exec_output_to_contract(o: vz::protocol::ExecOutput) -> contract::ExecOutput {
    contract::ExecOutput {
        exit_code: o.exit_code,
        stdout: o.stdout,
        stderr: o.stderr,
    }
}

fn container_info_to_contract(c: oci_container::ContainerInfo) -> contract::ContainerInfo {
    contract::ContainerInfo {
        id: c.id,
        image: c.image,
        image_id: c.image_id,
        status: match c.status {
            oci_container::ContainerStatus::Created => contract::ContainerStatus::Created,
            oci_container::ContainerStatus::Running => contract::ContainerStatus::Running,
            oci_container::ContainerStatus::Stopped { exit_code } => {
                contract::ContainerStatus::Stopped { exit_code }
            }
        },
        created_unix_secs: c.created_unix_secs,
        started_unix_secs: c.started_unix_secs,
        stopped_unix_secs: c.stopped_unix_secs,
        rootfs_path: c.rootfs_path,
        host_pid: c.host_pid,
    }
}

fn image_info_to_contract(i: vz_image::ImageInfo) -> contract::ImageInfo {
    contract::ImageInfo {
        reference: i.reference,
        image_id: i.image_id,
    }
}

fn prune_result_to_contract(p: vz_image::PruneResult) -> contract::PruneResult {
    contract::PruneResult {
        removed_refs: p.removed_refs,
        removed_manifests: p.removed_manifests,
        removed_configs: p.removed_configs,
        removed_layer_dirs: p.removed_layer_dirs,
    }
}
