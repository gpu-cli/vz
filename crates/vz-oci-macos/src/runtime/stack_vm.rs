use super::bundle::{
    container_log_dir, make_oci_runtime_share, mount_specs_to_bundle_mounts, oci_bundle_guest_path,
    oci_bundle_guest_root, oci_bundle_host_dir, resolve_oci_runtime_binary_path,
    setup_guest_container_overlay,
};
use super::networking::{start_port_forwarding, stop_via_oci_runtime};
use super::resolve::{
    current_unix_secs, new_container_id, resolve_container_lifecycle, resolve_run_config,
};
use super::*;

impl Runtime {
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
        resources: vz_runtime_contract::StackResourceHint,
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

        // Add VirtioFS shares for per-service volume mounts. These must be
        // configured at VM creation time because VirtioFS shares are static.
        for vol in &resources.volume_mounts {
            vm_config.shared_dirs.push(SharedDirConfig {
                tag: vol.tag.clone(),
                source: vol.host_path.clone(),
                read_only: vol.read_only,
            });
            // When a guest_path is specified, append the kernel cmdline
            // parameter that tells the init script where to bind-mount this
            // VirtioFS share inside the chroot.
            if let Some(guest_path) = &vol.guest_path {
                if let Some(idx_str) = vol.tag.strip_prefix("vz-mount-") {
                    vm_config
                        .cmdline
                        .push_str(&format!(" vz.mount.{idx_str}={guest_path}"));
                }
            }
        }

        vm_config.cpus = resources.cpus.unwrap_or(self.config.default_cpus);
        vm_config.memory_mb = resources.memory_mb.unwrap_or(self.config.default_memory_mb);

        // Attach persistent disk image for named volumes.
        if let Some(ref disk_path) = resources.disk_image_path {
            vm_config.disk_image = Some(disk_path.clone());
        }

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

        // Format and mount the persistent volume disk if attached.
        if resources.disk_image_path.is_some() {
            let timeout = Duration::from_secs(30);

            // Check if disk already has a filesystem. If not, format it as ext4.
            let blkid_result = vm
                .exec_collect(
                    "/bin/busybox".to_string(),
                    vec!["blkid".to_string(), "/dev/vda".to_string()],
                    timeout,
                )
                .await;

            // Busybox blkid may return exit 0 even on empty disks (with
            // no output). A disk with a filesystem produces output like
            // "/dev/vda: TYPE="ext4"". Format only if there's no TYPE output.
            let needs_format = match &blkid_result {
                Ok(output) => {
                    let has_fs = output.exit_code == 0 && output.stdout.contains("TYPE=");
                    tracing::debug!(
                        exit_code = output.exit_code,
                        has_filesystem = has_fs,
                        "blkid check result"
                    );
                    !has_fs
                }
                Err(err) => {
                    tracing::warn!(error = %err, "blkid exec failed");
                    true
                }
            };

            if needs_format {
                tracing::info!("formatting persistent volume disk as ext4");
                // Busybox mke2fs creates ext2 (no -t flag). The ext4 driver
                // can mount ext2/ext3/ext4, so this is fine.
                let format_result = vm
                    .exec_collect(
                        "/bin/busybox".to_string(),
                        vec![
                            "mke2fs".to_string(),
                            "-F".to_string(),
                            "/dev/vda".to_string(),
                        ],
                        timeout,
                    )
                    .await;
                match &format_result {
                    Ok(output) if output.exit_code != 0 => {
                        let _ = vm.stop().await;
                        return Err(OciError::InvalidConfig(format!(
                            "failed to format persistent volume disk: {}{}",
                            output.stdout, output.stderr
                        )));
                    }
                    Err(err) => {
                        let _ = vm.stop().await;
                        return Err(OciError::InvalidConfig(format!(
                            "failed to format persistent volume disk: {err}"
                        )));
                    }
                    Ok(output) => {
                        tracing::debug!(
                            stdout = %output.stdout, stderr = %output.stderr,
                            "mke2fs completed"
                        );
                    }
                }
            }

            // Mount the formatted disk.
            let mount_result = vm
                .exec_collect(
                    "/bin/busybox".to_string(),
                    vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        "/bin/busybox mkdir -p /run/vz-oci/volumes && /bin/busybox mount -t ext4 /dev/vda /run/vz-oci/volumes".to_string(),
                    ],
                    timeout,
                )
                .await;
            match &mount_result {
                Ok(output) if output.exit_code != 0 => {
                    let _ = vm.stop().await;
                    return Err(OciError::InvalidConfig(format!(
                        "failed to mount persistent volume disk: {}{}",
                        output.stdout, output.stderr
                    )));
                }
                Err(err) => {
                    let _ = vm.stop().await;
                    return Err(OciError::InvalidConfig(format!(
                        "failed to mount persistent volume disk: {err}"
                    )));
                }
                _ => {
                    tracing::info!("persistent volume disk mounted at /run/vz-oci/volumes");
                }
            }
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

        let rootfs_handle = self.store.spawn_assemble_rootfs(&image_id.0, &container_id);

        // Step 2 runs concurrently with rootfs assembly (no disk I/O dependency).
        tracing::debug!("step 2: parse_image_config_summary_from_store (concurrent with step 1)");
        let image_config = parse_image_config_summary_from_store(&self.store, &image_id.0)
            .map_err(|e| {
                tracing::error!(error = %e, "step 2 FAILED");
                e
            })?;
        tracing::debug!("step 2 OK");
        let run = resolve_run_config(image_config, run, &container_id)?;
        tracing::debug!(
            container_id = %container_id,
            working_dir = ?run.working_dir,
            "resolved container run configuration"
        );
        let lifecycle = resolve_container_lifecycle(
            &run.oci_annotations,
            ContainerLifecycleClass::Service,
            false,
        )?;

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

        let rootfs_dir = match rootfs_handle.await {
            Ok(Ok(rootfs_dir)) => rootfs_dir,
            Ok(Err(err)) => {
                tracing::error!(error = %err, "step 1 FAILED: assemble_rootfs");
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                return Err(err.into());
            }
            Err(join_err) => {
                tracing::error!(error = %join_err, "step 1 FAILED: rootfs task panicked");
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

        // Host: <data_dir>/rootfs/<container_id>/<bundle_path>
        let bundle_host_dir = oci_bundle_host_dir(&rootfs_dir, &bundle_relative_path);
        // Guest: /vz-rootfs/<container_id>/<bundle_path>
        let bundle_guest_path = format!("/vz-rootfs/{container_id}{bundle_relative_path}");
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

        let mut bundle_mounts = mount_specs_to_bundle_mounts(&run.mounts, run.mount_tag_offset)?;

        // Per-container overlay: VirtioFS doesn't support mknod, so we create a
        // guest-side overlay with tmpfs as upperdir for device nodes.
        let vz_rootfs_path = format!("/vz-rootfs/{container_id}");
        let guest_rootfs_path = match setup_guest_container_overlay(
            vm.as_ref(),
            &vz_rootfs_path,
            &container_id,
        )
        .await
        {
            Ok(path) => path,
            Err(err) => {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(current_unix_secs());
                container.host_pid = None;
                self.container_store
                    .upsert(container)
                    .map_err(OciError::from)?;
                return Err(err);
            }
        };
        // When sharing the VM's host network, ensure the container has a
        // working /etc/resolv.conf. Container images (e.g., Ubuntu) often
        // ship a resolv.conf pointing to systemd-resolved (127.0.0.53)
        // which isn't running in the VM. Write public DNS nameservers into
        // the overlay's upper layer so DNS resolution works immediately.
        if run.share_host_network {
            let dns_cmd = format!(
                "printf 'nameserver 8.8.8.8\\nnameserver 8.8.4.4\\n' > {guest_rootfs_path}/etc/resolv.conf"
            );
            let _ = vm
                .exec_collect(
                    "sh".to_string(),
                    vec!["-c".to_string(), dns_cmd],
                    Duration::from_secs(5),
                )
                .await;
        }

        // Bind-mount the VM-level log directory into the container so captured
        // stdout/stderr survives even if the container's init process exits.
        if run.capture_logs {
            bundle_mounts.push(BundleMount {
                destination: PathBuf::from("/var/log/vz-oci"),
                source: PathBuf::from(container_log_dir(&container_id)),
                typ: "bind".to_string(),
                options: vec!["rbind".to_string(), "rw".to_string()],
            });
        }

        // Create directories on the persistent volume disk for named volumes.
        // These must exist before the OCI runtime bind-mounts them into the container.
        let volume_dirs: Vec<String> = run
            .mounts
            .iter()
            .filter_map(|m| {
                if let MountType::Volume { ref volume_name } = m.mount_type {
                    Some(format!("/run/vz-oci/volumes/{volume_name}"))
                } else {
                    None
                }
            })
            .collect();
        if !volume_dirs.is_empty() {
            let mkdir_cmd = format!("/bin/busybox mkdir -p {}", volume_dirs.join(" "));
            let mkdir_result = vm
                .exec_collect(
                    "/bin/busybox".to_string(),
                    vec!["sh".to_string(), "-c".to_string(), mkdir_cmd],
                    Duration::from_secs(10),
                )
                .await;
            if let Err(err) = &mkdir_result {
                tracing::warn!(error = %err, "failed to create volume directories on persistent disk");
            }
        }

        // extra_hosts are written AFTER the container starts (step 5) via
        // oci_exec inside the container's mount namespace. Writing before
        // start (via guest exec or bind mount) fails due to VirtioFS caching
        // and youki's pivot_root creating an isolated mount tree.

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
                share_host_network: run.share_host_network,
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

        // OCI create + start inside the shared VM.
        if let Err(err) = vm
            .oci_create(oci_container_id.clone(), bundle_guest_path.clone())
            .await
        {
            tracing::error!(
                container_id = %oci_container_id,
                error = %err,
                "step 4 FAILED: oci_create"
            );
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
        self.start_log_rotation_task_if_needed(container_id.as_str(), Arc::clone(&vm), &run)
            .await?;

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

        // Step 5: Write /etc/hosts inside the running container via oci_exec.
        // This writes directly into the container's mount namespace after
        // pivot_root, avoiding VirtioFS caching and overlay visibility issues.
        if !run.extra_hosts.is_empty() {
            tracing::debug!("step 5: write /etc/hosts via nsenter streaming exec");
            let mut printf_content = String::from("127.0.0.1\\tlocalhost\\n::1\\tlocalhost\\n");
            for (hostname, ip) in &run.extra_hosts {
                printf_content.push_str(&format!("{ip}\\t{hostname}\\n"));
            }
            // Get the container's init PID for nsenter.
            let hosts_result = match vm.oci_state(oci_container_id.clone()).await {
                Ok(state) if state.pid.is_some() => {
                    let pid = state.pid.unwrap();
                    vm.exec_collect(
                        "/bin/busybox".to_string(),
                        vec![
                            "nsenter".to_string(),
                            format!("--mount=/proc/{pid}/ns/mnt"),
                            format!("--root=/proc/{pid}/root"),
                            "--wd=/".to_string(),
                            "--".to_string(),
                            "/bin/sh".to_string(),
                            "-c".to_string(),
                            format!("printf '{printf_content}' > /etc/hosts"),
                        ],
                        Duration::from_secs(30),
                    )
                    .await
                    .map_err(OciError::from)
                }
                Ok(_) => Err(OciError::InvalidConfig(format!(
                    "container '{}' has no running pid for /etc/hosts write",
                    oci_container_id
                ))),
                Err(e) => Err(OciError::from(e)),
            };
            match hosts_result {
                Ok(r) if r.exit_code == 0 => {
                    tracing::debug!("step 5 OK: /etc/hosts written");
                }
                Ok(r) => {
                    tracing::warn!(
                        exit_code = r.exit_code,
                        stderr = %r.stderr.trim(),
                        "step 5: /etc/hosts write returned non-zero"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "step 5: /etc/hosts write failed");
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
            self.stop_log_rotation_task(cid).await;
            let _ = stop_via_oci_runtime(&*vm, cid, false, STOP_GRACE_PERIOD, None).await;
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
            let mut active_lifecycle = self.active_lifecycle.lock().await;
            let mut container_exec_env = self.container_exec_env.lock().await;
            for cid in &stack_containers {
                vm_handles.remove(cid);
                cs.remove(cid);
                active_lifecycle.remove(cid);
                container_exec_env.remove(cid);
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

    /// Save a shared stack VM snapshot to disk.
    ///
    /// The VM is paused, state is saved, then the VM is resumed and the guest
    /// agent is revalidated before returning.
    pub async fn save_shared_vm_snapshot(
        &self,
        stack_id: &str,
        state_path: impl AsRef<Path>,
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

        let state_path = state_path.as_ref();
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent)?;
        }

        vm.save_state_snapshot(state_path).await?;
        vm.wait_for_agent(self.config.agent_ready_timeout).await?;
        Ok(())
    }

    /// Restore a shared stack VM from a saved snapshot file.
    ///
    /// Existing shared VM instance is stopped, restored from `state_path`, then
    /// resumed and reconnected to the guest agent.
    pub async fn restore_shared_vm_snapshot(
        &self,
        stack_id: &str,
        state_path: impl AsRef<Path>,
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

        let state_path = state_path.as_ref();
        if !state_path.exists() {
            return Err(OciError::InvalidConfig(format!(
                "shared VM snapshot path does not exist: {}",
                state_path.display()
            )));
        }

        vm.restore_state_snapshot(state_path, self.config.agent_ready_timeout)
            .await?;
        Ok(())
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

        let result = vm.exec_collect(command, args, timeout).await?;

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
}
