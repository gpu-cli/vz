use super::bundle::{
    container_log_dir, make_oci_runtime_share, mount_specs_to_bundle_mounts,
    mount_specs_to_shared_dirs, oci_bundle_guest_path, oci_bundle_guest_root, oci_bundle_host_dir,
    resolve_oci_runtime_binary_path, setup_guest_container_overlay, write_hosts_file,
};
use super::networking::start_port_forwarding;
use super::oci_lifecycle::{run_oci_lifecycle, spawn_log_rotation_task};
use super::resolve::parse_compose_log_rotation;
use super::*;

impl Runtime {
    pub(super) async fn boot_and_start_container(
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
        if let Err(err) = setup_guest_container_overlay(&vm, "/vz-rootfs", &oci_container_id).await
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
            .insert(container_id.to_string(), Arc::clone(&vm));

        // Keep port forwarding alive for the container's lifetime.
        if let Some(pf) = port_forwarding {
            self.port_forwards
                .lock()
                .await
                .insert(container_id.to_string(), pf);
        }
        self.start_log_rotation_task_if_needed(container_id, Arc::clone(&vm), run)
            .await?;

        Ok(())
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

        let vm = LinuxVm::create(vm_config).await?;
        vm.start().await?;

        if let Err(err) = vm.wait_for_agent(self.config.agent_ready_timeout).await {
            let _ = vm.stop().await;
            return Err(err.into());
        }

        // Set up per-container overlay so youki can mknod on tmpfs.
        if let Err(err) = setup_guest_container_overlay(&vm, "/vz-rootfs", &container_id).await {
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

    pub(super) async fn finalize_one_off_cleanup(&self, container_id: &str, auto_remove: bool) {
        self.active_lifecycle.lock().await.remove(container_id);
        self.stop_log_rotation_task(container_id).await;
        self.container_exec_env.lock().await.remove(container_id);

        if auto_remove {
            if let Err(err) = self.remove_container(container_id).await {
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

            let canonical_path = path.canonicalize().unwrap_or(path.clone());
            if !referenced_rootfs.contains(&canonical_path) {
                let _ = fs::remove_dir_all(path);
            }
        }
    }
}
