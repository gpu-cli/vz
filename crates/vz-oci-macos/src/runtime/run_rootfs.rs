use super::bundle::{
    container_log_dir, make_oci_runtime_share, mount_specs_to_bundle_mounts,
    mount_specs_to_shared_dirs, oci_bundle_guest_path, oci_bundle_guest_root, oci_bundle_host_dir,
    resolve_oci_runtime_binary_path, setup_unshared_guest_container_overlay, write_hosts_file,
};
use super::networking::start_port_forwarding;
use super::oci_lifecycle::{run_oci_lifecycle, spawn_log_rotation_task};
use super::resolve::parse_compose_log_rotation;
use super::*;

const TRANSIENT_VM_STOP_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const TRANSIENT_VM_STOP_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Retains a one-shot VM until its force-stop is proven.
///
/// The normal path calls [`TransientVmCleanupGuard::stop`] before returning.
/// If its owner is cancelled at any await point, `Drop` transfers the VM and
/// its recovery-route cleanup to a background task that keeps retrying bounded
/// stop attempts. This prevents an aborted one-shot or rootfs lifecycle from
/// dropping the last cleanup authority while guest work may still be live.
struct TransientVmCleanupGuard {
    vm: Option<Arc<LinuxVm>>,
    registered_container_id: String,
    vm_handles: Arc<Mutex<HashMap<String, Arc<LinuxVm>>>>,
}

impl TransientVmCleanupGuard {
    fn new(
        vm: Arc<LinuxVm>,
        registered_container_id: &str,
        vm_handles: Arc<Mutex<HashMap<String, Arc<LinuxVm>>>>,
    ) -> Self {
        Self {
            vm: Some(vm),
            registered_container_id: registered_container_id.to_string(),
            vm_handles,
        }
    }

    async fn stop(&mut self) -> Result<(), LinuxError> {
        let Some(vm) = self.vm.as_ref().cloned() else {
            return Ok(());
        };
        if matches!(
            vm.inner().state(),
            vz::VmState::Stopped | vz::VmState::Error(_)
        ) {
            self.finish_stopped(&vm).await;
            return Ok(());
        }

        let result = tokio::time::timeout(TRANSIENT_VM_STOP_ATTEMPT_TIMEOUT, vm.stop())
            .await
            .map_err(|_| {
                LinuxError::Protocol(format!(
                    "transient VM stop timed out after {:.3}s; cleanup continues under retained authority",
                    TRANSIENT_VM_STOP_ATTEMPT_TIMEOUT.as_secs_f64()
                ))
            });
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error))
                if matches!(
                    vm.inner().state(),
                    vz::VmState::Stopped | vz::VmState::Error(_)
                ) =>
            {
                warn!(
                    container_id = %self.registered_container_id,
                    %error,
                    "transient VM stop reported an error after reaching a terminal state"
                );
            }
            Ok(Err(error)) => return Err(error),
            Err(error)
                if matches!(
                    vm.inner().state(),
                    vz::VmState::Stopped | vz::VmState::Error(_)
                ) =>
            {
                warn!(
                    container_id = %self.registered_container_id,
                    %error,
                    "transient VM stop waiter timed out after the VM reached a terminal state"
                );
            }
            Err(error) => return Err(error),
        }

        self.finish_stopped(&vm).await;
        Ok(())
    }

    async fn finish_stopped(&mut self, vm: &Arc<LinuxVm>) {
        let mut vm_handles = self.vm_handles.lock().await;
        if vm_handles
            .get(&self.registered_container_id)
            .is_some_and(|registered| Arc::ptr_eq(registered, vm))
        {
            vm_handles.remove(&self.registered_container_id);
        }
        self.vm.take();
    }
}

impl Drop for TransientVmCleanupGuard {
    fn drop(&mut self) {
        let Some(vm) = self.vm.take() else {
            return;
        };
        let registered_container_id = self.registered_container_id.clone();
        let vm_handles = Arc::clone(&self.vm_handles);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            warn!(
                container_id = %registered_container_id,
                "transient VM cleanup lost its Tokio runtime; retaining VM authority"
            );
            std::mem::forget(vm);
            return;
        };

        runtime.spawn(async move {
            loop {
                if matches!(
                    vm.inner().state(),
                    vz::VmState::Stopped | vz::VmState::Error(_)
                ) {
                    break;
                }
                match tokio::time::timeout(TRANSIENT_VM_STOP_ATTEMPT_TIMEOUT, vm.stop()).await {
                    Ok(Ok(())) => break,
                    Ok(Err(error))
                        if matches!(
                            vm.inner().state(),
                            vz::VmState::Stopped | vz::VmState::Error(_)
                        ) =>
                    {
                        warn!(
                            container_id = %registered_container_id,
                            %error,
                            "transient VM background stop reached a terminal state"
                        );
                        break;
                    }
                    Ok(Err(error)) => warn!(
                        container_id = %registered_container_id,
                        %error,
                        "transient VM background stop failed; retaining authority and retrying"
                    ),
                    Err(_)
                        if matches!(
                            vm.inner().state(),
                            vz::VmState::Stopped | vz::VmState::Error(_)
                        ) =>
                    {
                        break;
                    }
                    Err(_) => warn!(
                        container_id = %registered_container_id,
                        timeout_secs = TRANSIENT_VM_STOP_ATTEMPT_TIMEOUT.as_secs_f64(),
                        "transient VM background stop timed out; retaining authority and retrying"
                    ),
                }
                tokio::time::sleep(TRANSIENT_VM_STOP_RETRY_DELAY).await;
            }

            let mut vm_handles = vm_handles.lock().await;
            if vm_handles
                .get(&registered_container_id)
                .is_some_and(|registered| Arc::ptr_eq(registered, &vm))
            {
                vm_handles.remove(&registered_container_id);
            }
        });
    }
}

impl Runtime {
    pub(super) async fn boot_and_start_container(
        &self,
        rootfs_dir: &Path,
        run: &RunConfig,
        container_id: &str,
    ) -> Result<Arc<LinuxVm>, OciError> {
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

        let mut bundle_mounts = mount_specs_to_bundle_mounts(&run.mounts, 0)?;

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

        // Bind-mount the VM-level log directory into the container so captured
        // stdout/stderr survives even if the container's init process exits.
        if run.capture_logs {
            bundle_mounts.push(BundleMount {
                destination: PathBuf::from("/var/log/vz-oci"),
                source: PathBuf::from(container_log_dir(&oci_container_id)),
                typ: "bind".to_string(),
                options: vec!["rbind".to_string(), "rw".to_string()],
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
                cap_add: run.cap_add.clone(),
                cap_drop: run.cap_drop.clone(),
                privileged: run.privileged,
                read_only_rootfs: run.read_only_rootfs,
                sysctls: run.sysctls.clone(),
                ulimits: run.ulimits.clone(),
                pids_limit: run.pids_limit,
                hostname: run.hostname.clone(),
                domainname: run.domainname.clone(),
            },
        )?;

        let kernel = ensure_kernel_for_config(&self.config).await?;
        let runtime_binary = resolve_oci_runtime_binary_path(
            self.config.guest_oci_runtime,
            self.config.guest_oci_runtime_path.as_deref(),
            &kernel,
        )?;

        let mount_shares = mount_specs_to_shared_dirs(&run.mounts, 0);
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
        // Non-stack path: no setup-commit cache, always start fresh.
        if let Err(err) =
            setup_unshared_guest_container_overlay(&vm, "/vz-rootfs", &oci_container_id, None).await
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

        // Keep port forwarding alive for the container's lifetime.
        if let Some(pf) = port_forwarding {
            self.port_forwards
                .lock()
                .await
                .insert(container_id.to_string(), pf);
        }
        self.start_log_rotation_task_if_needed(container_id, Arc::clone(&vm), run)
            .await?;

        // This is recovery/lifecycle routing only. Runtime::create_container
        // publishes the public exec binding after durable Running metadata and
        // active lifecycle state have both succeeded.
        self.vm_handles
            .lock()
            .await
            .insert(container_id.to_string(), Arc::clone(&vm));

        Ok(vm)
    }

    pub(super) async fn run_rootfs_with_oci_runtime(
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
            cap_add,
            cap_drop,
            privileged,
            read_only_rootfs,
            sysctls,
            ulimits,
            pids_limit,
            hostname,
            domainname,
            stop_signal: _,
            stop_grace_period_secs: _,
            share_host_network: _,
            mount_tag_offset: _,
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

        let mut bundle_mounts = mount_specs_to_bundle_mounts(&mounts, 0)?;

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
                cap_add,
                cap_drop,
                privileged,
                read_only_rootfs,
                sysctls: sysctls.into_iter().collect(),
                ulimits,
                pids_limit,
                hostname,
                domainname,
            },
        )?;

        let kernel = ensure_kernel_for_config(&self.config).await?;
        let runtime_binary = resolve_oci_runtime_binary_path(
            self.config.guest_oci_runtime,
            self.config.guest_oci_runtime_path.as_deref(),
            &kernel,
        )?;

        let mount_shares = mount_specs_to_shared_dirs(&mounts, 0);
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

        let vm = Arc::new(LinuxVm::create(vm_config).await?);
        // Arm cancellation cleanup before the first VM operation that can be
        // interrupted after Virtualization.framework has accepted work.
        let mut vm_cleanup = TransientVmCleanupGuard::new(
            Arc::clone(&vm),
            registered_container_id,
            Arc::clone(&self.vm_handles),
        );
        if let Err(error) = vm.start().await {
            let stop = vm_cleanup.stop().await;
            return finish_transient_execution(Err::<ExecOutput, _>(error), Ok(()), stop);
        }

        if let Err(err) = vm.wait_for_agent(self.config.agent_ready_timeout).await {
            let stop = vm_cleanup.stop().await;
            return finish_transient_execution(Err::<ExecOutput, _>(err), Ok(()), stop);
        }

        // Set up per-container overlay so youki can mknod on tmpfs.
        // Non-stack path: no setup-commit cache, always start fresh.
        if let Err(err) =
            setup_unshared_guest_container_overlay(&vm, "/vz-rootfs", &container_id, None).await
        {
            let stop = vm_cleanup.stop().await;
            return finish_transient_execution(Err(err), Ok(()), stop);
        }

        // One-off execution is transient and carries its resolved options
        // directly through run_oci_lifecycle. It intentionally has no public
        // container-exec binding; this handle is recovery/lifecycle routing.
        self.vm_handles
            .lock()
            .await
            .insert(registered_container_id.to_string(), Arc::clone(&vm));

        let port_forwards = match start_port_forwarding(vm.inner_shared(), &ports).await {
            Ok(port_forwards) => port_forwards,
            Err(err) => {
                let stop = vm_cleanup.stop().await;
                return finish_transient_execution(Err(err), Ok(()), stop);
            }
        };

        let lifecycle_timeout = timeout.unwrap_or(self.config.exec_timeout);
        let lifecycle = match tokio::time::timeout(
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
        {
            Ok(result) => result,
            Err(_) => Err(OciError::InvalidConfig(format!(
                "oci runtime exec timed out after {:.3}s",
                lifecycle_timeout.as_secs_f64()
            ))),
        };

        let forwarding_shutdown = match port_forwards {
            Some(mut port_forwards) => port_forwards.shutdown().await,
            None => Ok(()),
        };
        let stop = vm_cleanup.stop().await;
        finish_transient_execution(lifecycle, forwarding_shutdown, stop)
    }

    /// Run a command against a local rootfs mounted as VirtioFS `rootfs`.
    ///
    /// The spawned worker owns the complete transient lifecycle. Dropping this
    /// public future therefore cannot interrupt VM/forwarding cleanup.
    pub async fn run_rootfs(
        &self,
        rootfs_dir: impl AsRef<Path>,
        run: RunConfig,
    ) -> Result<ExecOutput, OciError> {
        let rootfs_dir = rootfs_dir.as_ref().to_path_buf();
        let runtime = self.clone();
        tokio::spawn(async move { runtime.run_rootfs_owned(rootfs_dir, run).await })
            .await
            .map_err(|error| {
                OciError::InvalidConfig(format!(
                    "rootfs lifecycle worker failed while retaining VM cleanup authority: {error}"
                ))
            })?
    }

    async fn run_rootfs_owned(
        &self,
        rootfs_dir: PathBuf,
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
            container_id,
            oci_annotations: _,
            extra_hosts: _,
            network_namespace_path: _,
            cpu_quota: _,
            cpu_period: _,
            capture_logs: _,
            cap_add: _,
            cap_drop: _,
            privileged: _,
            read_only_rootfs: _,
            sysctls: _,
            ulimits: _,
            pids_limit: _,
            hostname: _,
            domainname: _,
            stop_signal: _,
            stop_grace_period_secs: _,
            share_host_network: _,
            mount_tag_offset: _,
        } = run;

        if !rootfs_dir.is_dir() {
            return Err(OciError::InvalidRootfs { path: rootfs_dir });
        }

        let (command, args) = cmd
            .split_first()
            .ok_or_else(|| OciError::InvalidConfig("run command must not be empty".to_string()))?;

        let kernel = ensure_kernel_for_config(&self.config).await?;
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
        let mount_shares = mount_specs_to_shared_dirs(&mounts, 0);
        if !mount_shares.is_empty() {
            vm_config.shared_dirs.extend(mount_shares);
            for (idx, spec) in mounts.iter().enumerate() {
                if matches!(spec.mount_type, MountType::Bind) {
                    vm_config.cmdline.push_str(&format!(
                        " vz.mount.{}={}",
                        idx,
                        spec.target.display()
                    ));
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

        let registered_container_id = container_id.unwrap_or_else(new_container_id);
        validate_container_id(&registered_container_id)?;
        let vm = Arc::new(LinuxVm::create(vm_config).await?);
        let mut vm_cleanup = TransientVmCleanupGuard::new(
            Arc::clone(&vm),
            &registered_container_id,
            Arc::clone(&self.vm_handles),
        );
        {
            let mut vm_handles = self.vm_handles.lock().await;
            if vm_handles.contains_key(&registered_container_id) {
                drop(vm_handles);
                let stop = vm_cleanup.stop().await;
                return finish_transient_execution(
                    Err::<ExecOutput, _>(OciError::ContainerAlreadyExists {
                        id: registered_container_id,
                    }),
                    Ok(()),
                    stop,
                );
            }
            vm_handles.insert(registered_container_id.clone(), Arc::clone(&vm));
        }
        if let Err(error) = vm.start().await {
            let stop = vm_cleanup.stop().await;
            return finish_transient_execution(Err::<ExecOutput, _>(error), Ok(()), stop);
        }

        if let Err(err) = vm.wait_for_agent(self.config.agent_ready_timeout).await {
            let stop = vm_cleanup.stop().await;
            return finish_transient_execution(Err::<ExecOutput, _>(err), Ok(()), stop);
        }

        let port_forwards = match start_port_forwarding(vm.inner_shared(), &ports).await {
            Ok(port_forwards) => port_forwards,
            Err(err) => {
                let stop = vm_cleanup.stop().await;
                return finish_transient_execution(Err(err), Ok(()), stop);
            }
        };

        let exec_timeout = timeout.unwrap_or(self.config.exec_timeout);
        let exec = vm
            .exec_collect_with_options(
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

        let forwarding_shutdown = match port_forwards {
            Some(mut port_forwards) => port_forwards.shutdown().await,
            None => Ok(()),
        };
        let stop = vm_cleanup.stop().await;
        finish_transient_execution(exec, forwarding_shutdown, stop)
    }

    /// Reconcile containers whose managing host PID is no longer alive.
    ///
    /// Transitions stale `Running`/`Created` containers to `Stopped` and
    /// cleans up their rootfs. Called automatically during `Runtime::new()`.
    pub(super) fn reconcile_stale_containers(&self) {
        if let Ok(reconciled) = self.container_store.reconcile_stale() {
            for id in &reconciled {
                tracing::info!(container_id = %id, "reconciled stale container");
            }
        }
    }

    pub(super) fn cleanup_rootfs_dir(&self, rootfs_dir: &Path) {
        let _ = fs::remove_dir_all(rootfs_dir);
    }

    pub(super) async fn track_active_lifecycle(
        &self,
        container_id: String,
        lifecycle: ActiveContainerLifecycle,
    ) {
        self.active_lifecycle
            .lock()
            .await
            .insert(container_id, lifecycle);
    }

    pub(super) async fn start_log_rotation_task_if_needed(
        &self,
        container_id: &str,
        vm: Arc<LinuxVm>,
        run: &RunConfig,
    ) -> Result<(), OciError> {
        if !run.capture_logs {
            self.stop_log_rotation_task(container_id).await;
            return Ok(());
        }

        let Some(rotation) = parse_compose_log_rotation(&run.oci_annotations)? else {
            self.stop_log_rotation_task(container_id).await;
            return Ok(());
        };

        self.stop_log_rotation_task(container_id).await;
        let task = spawn_log_rotation_task(container_id.to_string(), vm, rotation);
        self.log_rotation_tasks
            .lock()
            .await
            .insert(container_id.to_string(), task);
        Ok(())
    }

    pub(super) async fn stop_log_rotation_task(&self, container_id: &str) {
        let task = { self.log_rotation_tasks.lock().await.remove(container_id) };
        if let Some(task) = task {
            task.shutdown().await;
        }
    }

    pub(super) async fn finalize_one_off_cleanup(
        &self,
        container_id: &str,
        auto_remove: bool,
        transaction: &ContainerLifecycleTransaction,
    ) {
        self.active_lifecycle.lock().await.remove(container_id);
        self.stop_log_rotation_task(container_id).await;
        self.container_exec_bindings
            .lock()
            .await
            .remove(container_id);

        if auto_remove {
            if let Err(err) = self
                .remove_container_in_transaction(container_id, transaction)
                .await
            {
                warn!(
                    container_id = %container_id,
                    error = %err,
                    "one-off auto-remove cleanup failed"
                );
            }
        }
    }

    pub(super) fn cleanup_orphaned_rootfs(&self) {
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

            // A create transaction publishes its durable generation before
            // assembly and may not yet have persisted rootfs_path. Never let
            // startup orphan cleanup race that generation's active writer.
            let container_id = entry.file_name();
            if let Some(container_id) = container_id.to_str()
                && self
                    .container_store
                    .current_generation(container_id)
                    .is_ok_and(|generation| generation.is_some())
            {
                continue;
            }

            let canonical_path = path.canonicalize().unwrap_or(path.clone());
            if !referenced_rootfs.contains(&canonical_path) {
                let _ = fs::remove_dir_all(path);
            }
        }
    }
}

fn finish_transient_execution<E, S>(
    execution: Result<ExecOutput, E>,
    forwarding_shutdown: Result<(), OciError>,
    vm_stop: Result<(), S>,
) -> Result<ExecOutput, OciError>
where
    E: std::fmt::Display + Into<OciError>,
    S: std::fmt::Display,
{
    if forwarding_shutdown.is_ok() && vm_stop.is_ok() {
        return execution.map_err(Into::into);
    }

    let mut failures = Vec::new();
    let output = match execution {
        Ok(output) => Some(output),
        Err(error) => {
            failures.push(format!("execution failed: {error}"));
            None
        }
    };
    if let Err(error) = forwarding_shutdown {
        failures.push(error.to_string());
    }
    if let Err(error) = vm_stop {
        failures.push(format!("VM stop failed: {error}"));
    }

    match (output, failures.is_empty()) {
        (Some(output), true) => Ok(output),
        (_, false) => Err(OciError::InvalidConfig(format!(
            "transient VM cleanup failed: {}",
            failures.join("; ")
        ))),
        (None, true) => Err(OciError::InvalidConfig(
            "transient execution produced neither output nor an error".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_execution_aggregates_forwarding_and_vm_stop_failures() {
        let error = finish_transient_execution::<OciError, OciError>(
            Ok(ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            }),
            Err(OciError::InvalidConfig(
                "injected forwarding failure".to_string(),
            )),
            Err(OciError::InvalidConfig(
                "injected VM stop failure".to_string(),
            )),
        )
        .map_or_else(
            |error| error,
            |value| panic!("expected cleanup failure, got success: {value:?}"),
        );

        assert!(error.to_string().contains("injected forwarding failure"));
        assert!(error.to_string().contains("injected VM stop failure"));
    }

    #[test]
    fn transient_execution_preserves_execution_error_when_cleanup_succeeds() {
        let error = finish_transient_execution::<OciError, OciError>(
            Err(OciError::ContainerNotFound {
                id: "original-error".to_string(),
            }),
            Ok(()),
            Ok(()),
        )
        .map_or_else(
            |error| error,
            |value| panic!("expected cleanup failure, got success: {value:?}"),
        );

        assert!(matches!(
            error,
            OciError::ContainerNotFound { ref id } if id == "original-error"
        ));
    }
}
