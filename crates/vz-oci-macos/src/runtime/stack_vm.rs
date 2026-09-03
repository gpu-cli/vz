use super::bundle::{
    container_log_dir, make_oci_runtime_share, mount_specs_to_bundle_mounts, oci_bundle_guest_path,
    oci_bundle_guest_root, oci_bundle_host_dir, resolve_oci_runtime_binary_path,
    setup_stack_guest_container_overlay,
};
use super::networking::{start_port_forwarding, stop_via_oci_runtime};
use super::resolve::{
    current_unix_secs, new_container_id, resolve_container_lifecycle, resolve_run_config,
};
use super::*;

/// Owned proof that one stack's complete guest activation transaction is
/// serialized. The first overlay mutation requires this value, and its drop
/// scope extends through OCI activation and post-start validation.
pub(super) struct StackActivationGuard {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

pub(super) fn require_running_pid(
    container_id: &str,
    phase: &str,
    state: &OciContainerState,
) -> Result<u32, OciError> {
    if state.status != "running" {
        return Err(OciError::InvalidConfig(format!(
            "container '{container_id}' is not running during {phase}: status='{}', pid={:?}",
            state.status, state.pid
        )));
    }

    state.pid.filter(|pid| *pid > 0).ok_or_else(|| {
        OciError::InvalidConfig(format!(
            "container '{container_id}' has no running pid during {phase}"
        ))
    })
}

pub(super) fn require_successful_hosts_write(
    container_id: &str,
    output: &ExecOutput,
) -> Result<(), OciError> {
    if output.exit_code == 0 {
        return Ok(());
    }
    Err(OciError::InvalidConfig(format!(
        "container '{}' /etc/hosts write failed with exit code {}: {}",
        container_id,
        output.exit_code,
        output.stderr.trim()
    )))
}

pub(super) fn activation_error_with_rollback(
    activation_error: OciError,
    rollback: Result<(), OciError>,
) -> OciError {
    match rollback {
        Ok(()) => activation_error,
        Err(rollback_error) => OciError::InvalidConfig(format!(
            "stack container activation failed: {activation_error}; rollback also failed: {rollback_error}"
        )),
    }
}

fn diagnostic_file_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

impl Runtime {
    // ── Shared stack VM API ──────────────────────────────────────────

    pub(super) async fn stack_activation_lock(&self, stack_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.stack_activation_locks.lock().await;
        locks
            .entry(stack_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(super) async fn acquire_stack_activation_guard(
        &self,
        stack_id: &str,
    ) -> StackActivationGuard {
        let lock = self.stack_activation_lock(stack_id).await;
        StackActivationGuard {
            _guard: lock.lock_owned().await,
        }
    }

    /// Return the rootfs store directory where assembled rootfs trees are stored.
    ///
    /// This is the parent directory of all per-container rootfs directories.
    /// For a shared stack VM, it is used as the VirtioFS `rootfs` share so
    /// that each container's assembled rootfs appears at `/<container_id>/`
    /// inside the guest.
    pub fn rootfs_store_dir(&self) -> PathBuf {
        self.config.data_dir.join("rootfs")
    }

    /// Host-side directory where setup-commit tarballs are stored.
    ///
    /// VirtioFS-shared into every shared VM at `/vz-setup-commits` so that
    /// the post-setup filesystem state of a container can be tarred to
    /// host once and replayed on subsequent boots, instead of re-running
    /// `apt-get install` etc. on every cold boot.
    pub fn setup_commits_host_dir(&self) -> PathBuf {
        self.config.data_dir.join("setup-commits")
    }

    /// Compute a stable identifier for a (image, setup_commands) tuple.
    ///
    /// Used as the filename of the cached setup tarball
    /// (`<reference>.tar` under [`setup_commits_host_dir`]). Hashes the
    /// image string verbatim — when the user pins a digest the cache is
    /// content-addressed; when they use a tag they accept that the cache
    /// can be stale across image updates and is cleared by manually
    /// removing the tarball.
    pub fn setup_commit_reference(image: &str, setup_commands: &[String]) -> String {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(image.as_bytes());
        hasher.update(b"\0");
        for cmd in setup_commands {
            hasher.update(cmd.as_bytes());
            hasher.update(b"\0");
        }
        format!("{:x}", hasher.finalize())
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
        let _activation_guard = self.acquire_stack_activation_guard(stack_id).await;

        // Snapshot lock-protected counters into locals so the lock guards
        // don't span the tracing::info! call (which was captured across
        // the next .await and broke Send).
        let (stack_vms_count, stack_port_forwards_count, has_leftover_pf, already_booted) = {
            let vms = self.stack_vms.lock().await;
            let pfs = self.stack_port_forwards.lock().await;
            (
                vms.len(),
                pfs.len(),
                pfs.contains_key(stack_id),
                vms.contains_key(stack_id),
            )
        };
        let sample_ports: Vec<(u16, u16)> = ports
            .iter()
            .take(4)
            .map(|p| (p.host, p.container))
            .collect();
        tracing::info!(
            target: "vz_post_stop",
            stack_id = %stack_id,
            in_count = ports.len(),
            ?sample_ports,
            stack_vms_count,
            stack_port_forwards_count,
            "[L4/stack-vm] boot_shared_vm entry"
        );
        // Guard against double-boot.
        if already_booted {
            tracing::info!(
                target: "vz_post_stop",
                stack_id = %stack_id,
                "[L4/stack-vm] returning 'shared VM already running' (BUG SUSPECT — partial-cleanup leftover)"
            );
            return Err(OciError::InvalidConfig(format!(
                "shared VM already running for stack '{stack_id}'"
            )));
        }
        // Inspect partial-cleanup state: stack_vms cleared but stack_port_forwards
        // not. This is suspect (c).
        if has_leftover_pf {
            tracing::warn!(
                target: "vz_post_stop",
                stack_id = %stack_id,
                "[L4/stack-vm] LEFTOVER PortForwarding entry for this stack from prior run (BUG SUSPECT (c))"
            );
        }

        let rootfs_store = self.rootfs_store_dir();
        fs::create_dir_all(&rootfs_store)?;

        // Setup-commit cache: VirtioFS-shared into the guest at
        // /vz-setup-commits. Lets create_container_in_stack tar a
        // post-setup upperdir to host once, then restore it on every
        // subsequent cold boot — turning a 32s `apt-get install ...`
        // into a sub-second `tar -xpf`.
        let setup_commits = self.setup_commits_host_dir();
        fs::create_dir_all(&setup_commits)?;

        let kernel = ensure_kernel_for_config(&self.config).await?;

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
        vm_config.shared_dirs.push(SharedDirConfig {
            tag: "vz-setup-commits".to_string(),
            source: setup_commits,
            read_only: false,
        });

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

        // Capture one serial log per shared VM when the E2E harness provides
        // an artifact directory. Preserve the older exact-path override for
        // focused/manual debugging.
        if let Ok(log_dir) = std::env::var("VZ_STACK_SERIAL_LOG_DIR") {
            let log_dir = PathBuf::from(log_dir);
            fs::create_dir_all(&log_dir)?;
            vm_config.serial_log_file =
                Some(log_dir.join(format!("{}.log", diagnostic_file_component(stack_id))));
        } else if let Ok(log_path) = std::env::var("VZ_STACK_SERIAL_LOG") {
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

        // Mount the setup-commits VirtioFS share inside the host VM so
        // create_container_in_stack can tar/untar setup state. Idempotent —
        // mountpoint may already exist from a prior boot of the same VM.
        let mount_cmd = "mkdir -p /vz-setup-commits && \
             ( mountpoint -q /vz-setup-commits || \
               mount -t virtiofs vz-setup-commits /vz-setup-commits )"
            .to_string();
        match vm
            .exec_collect(
                "sh".to_string(),
                vec!["-c".to_string(), mount_cmd],
                Duration::from_secs(5),
            )
            .await
        {
            Ok(out) if out.exit_code == 0 => {
                tracing::info!("setup-commits VirtioFS share mounted at /vz-setup-commits");
            }
            Ok(out) => {
                tracing::warn!(
                    exit_code = out.exit_code,
                    stderr = %out.stderr.trim(),
                    "setup-commits mount returned non-zero (cache will be unavailable)"
                );
            }
            Err(error) => {
                tracing::warn!(error = %error, "setup-commits mount exec failed (cache will be unavailable)");
            }
        }

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
        setup_commit_tar_guest: Option<String>,
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

        // Setup commit/restore: caller (the macOS backend) precomputes the
        // (image, setup_commands) hash and resolves it to a guest path
        // under /vz-setup-commits — those fields are stripped during the
        // contract → oci_config conversion so they can't be derived here.

        // Serialize the complete guest-critical activation transaction for
        // this stack. In particular, overlay cleanup performs a VM-global
        // drop_caches operation, so it must not overlap a sibling service's
        // overlay mount or OCI create/start. Image pull, rootfs assembly, and
        // image-config resolution above remain parallel; independent stacks
        // use independent locks.
        let activation_guard = self.acquire_stack_activation_guard(stack_id).await;

        // Per-container overlay: VirtioFS doesn't support mknod, so we create a
        // guest-side overlay with tmpfs as upperdir for device nodes.
        let vz_rootfs_path = format!("/vz-rootfs/{container_id}");
        let (guest_rootfs_path, setup_was_restored) = match setup_stack_guest_container_overlay(
            vm.as_ref(),
            &vz_rootfs_path,
            &container_id,
            setup_commit_tar_guest.as_deref(),
            &activation_guard,
        )
        .await
        {
            Ok(out) => out,
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
        if setup_was_restored {
            self.setup_restored_containers
                .lock()
                .await
                .insert(container_id.clone());
            tracing::info!(
                container_id = %container_id,
                "setup commit restored into overlay upperdir before mount"
            );
        }
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

        let vm_is_current = self
            .stack_vms
            .lock()
            .await
            .get(stack_id)
            .is_some_and(|current| Arc::ptr_eq(current, &vm));
        if !vm_is_current {
            container.status = ContainerStatus::Stopped { exit_code: -1 };
            container.stopped_unix_secs = Some(current_unix_secs());
            container.host_pid = None;
            self.container_store
                .upsert(container)
                .map_err(OciError::from)?;
            self.cleanup_rootfs_dir(rootfs_dir.as_ref());
            return Err(OciError::InvalidConfig(format!(
                "shared VM for stack '{stack_id}' changed while container '{container_id}' was being prepared"
            )));
        }

        // Publish the stack route before the first guest OCI mutation. A task
        // cancelled during create/start/rollback is therefore still visible to
        // stack shutdown. The per-container handle is published second because
        // container_stack alone is sufficient to reach the shared VM.
        self.container_stack
            .lock()
            .await
            .insert(container_id.to_string(), stack_id.to_string());
        self.vm_handles
            .lock()
            .await
            .insert(container_id.to_string(), Arc::clone(&vm));

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
            let error = OciError::from(err);
            let rollback = self
                .rollback_stack_container_activation(
                    &vm,
                    stack_id,
                    &oci_container_id,
                    &container_id,
                    &mut container,
                    rootfs_dir.as_ref(),
                )
                .await;
            return Err(activation_error_with_rollback(error, rollback));
        }

        if let Err(err) = vm.oci_start(oci_container_id.clone()).await {
            let error = OciError::from(err);
            let rollback = self
                .rollback_stack_container_activation(
                    &vm,
                    stack_id,
                    &oci_container_id,
                    &container_id,
                    &mut container,
                    rootfs_dir.as_ref(),
                )
                .await;
            return Err(activation_error_with_rollback(error, rollback));
        }

        // Step 5: Write /etc/hosts inside the running container via oci_exec.
        // This writes directly into the container's mount namespace after
        // pivot_root, avoiding VirtioFS caching and overlay visibility issues.
        let initial_pid = match self
            .validate_stack_container_running(&vm, &oci_container_id, "post-start")
            .await
        {
            Ok(pid) => pid,
            Err(error) => {
                let rollback = self
                    .rollback_stack_container_activation(
                        &vm,
                        stack_id,
                        &oci_container_id,
                        &container_id,
                        &mut container,
                        rootfs_dir.as_ref(),
                    )
                    .await;
                return Err(activation_error_with_rollback(error, rollback));
            }
        };

        if !run.extra_hosts.is_empty() {
            tracing::debug!(
                container_id = %oci_container_id,
                pid = initial_pid,
                "step 5: write /etc/hosts via nsenter streaming exec"
            );
            let mut printf_content = String::from("127.0.0.1\\tlocalhost\\n::1\\tlocalhost\\n");
            for (hostname, ip) in &run.extra_hosts {
                printf_content.push_str(&format!("{ip}\\t{hostname}\\n"));
            }
            let hosts_result = vm
                .exec_collect(
                    "/bin/busybox".to_string(),
                    vec![
                        "nsenter".to_string(),
                        format!("--mount=/proc/{initial_pid}/ns/mnt"),
                        format!("--root=/proc/{initial_pid}/root"),
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
                .and_then(|output| require_successful_hosts_write(&oci_container_id, &output));
            if let Err(error) = hosts_result {
                tracing::error!(
                    container_id = %oci_container_id,
                    pid = initial_pid,
                    error = %error,
                    "step 5 FAILED: /etc/hosts write"
                );
                let rollback = self
                    .rollback_stack_container_activation(
                        &vm,
                        stack_id,
                        &oci_container_id,
                        &container_id,
                        &mut container,
                        rootfs_dir.as_ref(),
                    )
                    .await;
                return Err(activation_error_with_rollback(error, rollback));
            }
            tracing::debug!(
                container_id = %oci_container_id,
                pid = initial_pid,
                "step 5 OK: /etc/hosts written"
            );
        }

        if let Err(error) = self
            .validate_stack_container_running(&vm, &oci_container_id, "activation-finalize")
            .await
        {
            let rollback = self
                .rollback_stack_container_activation(
                    &vm,
                    stack_id,
                    &oci_container_id,
                    &container_id,
                    &mut container,
                    rootfs_dir.as_ref(),
                )
                .await;
            return Err(activation_error_with_rollback(error, rollback));
        }

        if let Err(error) = self
            .start_log_rotation_task_if_needed(container_id.as_str(), Arc::clone(&vm), &run)
            .await
        {
            let rollback = self
                .rollback_stack_container_activation(
                    &vm,
                    stack_id,
                    &oci_container_id,
                    &container_id,
                    &mut container,
                    rootfs_dir.as_ref(),
                )
                .await;
            return Err(activation_error_with_rollback(error, rollback));
        }

        container.status = ContainerStatus::Running;
        container.started_unix_secs = Some(current_unix_secs());
        container.host_pid = Some(process::id());
        if let Err(error) = self.container_store.upsert(container.clone()) {
            let rollback = self
                .rollback_stack_container_activation(
                    &vm,
                    stack_id,
                    &oci_container_id,
                    &container_id,
                    &mut container,
                    rootfs_dir.as_ref(),
                )
                .await;
            return Err(activation_error_with_rollback(
                OciError::from(error),
                rollback,
            ));
        }

        // Publish in-memory handles only after every required post-start
        // action, final liveness validation, and durable Running metadata have
        // succeeded. The map updates themselves are infallible.
        self.vm_handles
            .lock()
            .await
            .insert(container_id.to_string(), Arc::clone(&vm));
        self.container_stack
            .lock()
            .await
            .insert(container_id.to_string(), stack_id.to_string());
        self.track_active_lifecycle(container_id.clone(), lifecycle)
            .await;
        self.container_exec_env
            .lock()
            .await
            .insert(container_id.clone(), run.env.clone());

        Ok(container_id)
    }

    async fn validate_stack_container_running(
        &self,
        vm: &LinuxVm,
        container_id: &str,
        phase: &str,
    ) -> Result<u32, OciError> {
        let state = vm.oci_state(container_id.to_string()).await?;
        let pid = require_running_pid(container_id, phase, &state)?;
        let proc_root = format!("/proc/{pid}/root");
        let liveness = vm
            .exec_collect(
                "/bin/busybox".to_string(),
                vec!["test".to_string(), "-d".to_string(), proc_root.clone()],
                Duration::from_secs(5),
            )
            .await?;
        if liveness.exit_code != 0 {
            return Err(OciError::InvalidConfig(format!(
                "container '{container_id}' reported status='{}' pid={pid} during {phase}, but {proc_root} is not live: {}",
                state.status,
                liveness.stderr.trim()
            )));
        }
        tracing::debug!(
            container_id,
            phase,
            status = %state.status,
            pid,
            "validated running OCI container"
        );
        Ok(pid)
    }

    async fn rollback_stack_container_activation(
        &self,
        vm: &Arc<LinuxVm>,
        stack_id: &str,
        oci_container_id: &str,
        container_id: &str,
        container: &mut ContainerInfo,
        rootfs_dir: &Path,
    ) -> Result<(), OciError> {
        // Publish recovery routing before any await. This keeps the container
        // discoverable even if rollback is cancelled while stopping log
        // rotation, collecting diagnostics, or deleting guest OCI state.
        self.vm_handles
            .lock()
            .await
            .insert(container_id.to_string(), Arc::clone(vm));
        self.container_stack
            .lock()
            .await
            .insert(container_id.to_string(), stack_id.to_string());

        self.stop_log_rotation_task(container_id).await;
        self.log_stack_activation_diagnostics(vm, oci_container_id)
            .await;

        if let Err(error) = vm.oci_delete(oci_container_id.to_string(), true).await {
            // The guest may still have a live process or OCI state. Keep every
            // resource needed for a later stack shutdown retry instead of
            // publishing Stopped and orphaning the guest workload.
            container.status = ContainerStatus::Created;
            container.started_unix_secs = None;
            container.stopped_unix_secs = None;
            container.host_pid = Some(process::id());
            let persist_error = self.container_store.upsert(container.clone()).err();

            tracing::error!(
                container_id = %oci_container_id,
                stack_id,
                error = %error,
                "activation rollback could not delete OCI state; retained VM tracking and rootfs"
            );
            let mut message = format!(
                "activation rollback could not delete OCI state for container '{oci_container_id}'; retained stack '{stack_id}' tracking and rootfs for shutdown retry: {error}"
            );
            if let Some(persist_error) = persist_error {
                message.push_str(&format!(
                    "; could not persist activation-incomplete state: {persist_error}"
                ));
            }
            return Err(OciError::InvalidConfig(message));
        }
        self.vm_handles.lock().await.remove(container_id);
        self.container_stack.lock().await.remove(container_id);
        self.active_lifecycle.lock().await.remove(container_id);
        self.container_exec_env.lock().await.remove(container_id);
        self.setup_restored_containers
            .lock()
            .await
            .remove(container_id);

        container.status = ContainerStatus::Stopped { exit_code: -1 };
        container.stopped_unix_secs = Some(current_unix_secs());
        container.host_pid = None;
        let persist_result = self
            .container_store
            .upsert(container.clone())
            .map_err(OciError::from);
        self.cleanup_rootfs_dir(rootfs_dir);
        persist_result
    }

    async fn log_stack_activation_diagnostics(&self, vm: &LinuxVm, container_id: &str) {
        let commands = [
            (
                "process-table",
                "/bin/busybox",
                vec!["ps".to_string(), "-ef".to_string()],
            ),
            (
                "youki-create-log",
                "/bin/busybox",
                vec![
                    "cat".to_string(),
                    format!("/run/vz-oci/logs/{container_id}-create.log"),
                ],
            ),
            (
                "youki-start-log",
                "/bin/busybox",
                vec![
                    "cat".to_string(),
                    format!("/run/vz-oci/logs/{container_id}-start.log"),
                ],
            ),
            (
                "container-output",
                "/bin/busybox",
                vec![
                    "cat".to_string(),
                    format!("/run/vz-oci/logs/{container_id}/output.log"),
                ],
            ),
            ("kernel-log", "/bin/busybox", vec!["dmesg".to_string()]),
        ];

        for (diagnostic, command, args) in commands {
            match vm
                .exec_collect(command.to_string(), args, Duration::from_secs(5))
                .await
            {
                Ok(output) => tracing::error!(
                    container_id,
                    diagnostic,
                    exit_code = output.exit_code,
                    stdout = %output.stdout.trim(),
                    stderr = %output.stderr.trim(),
                    "stack activation diagnostic"
                ),
                Err(error) => tracing::error!(
                    container_id,
                    diagnostic,
                    error = %error,
                    "stack activation diagnostic unavailable"
                ),
            }
        }
    }

    /// Tar the container's overlay upperdir to host as the cached commit
    /// for `commit_ref`. Atomic via `<ref>.tar.tmp` + rename. Best-effort:
    /// failures here only mean the next cold boot will run setup again.
    pub async fn save_setup_commit(
        &self,
        stack_id: &str,
        container_id: &str,
        commit_ref: &str,
    ) -> Result<(), OciError> {
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
        let tar_guest_tmp = format!("/vz-setup-commits/{commit_ref}.tar.tmp");
        let tar_guest = format!("/vz-setup-commits/{commit_ref}.tar");
        let upper_dir = format!("/run/vz-oci/containers/{container_id}/upper");
        // -C cd into upper, -p preserve perms, -f write to tmp file. Use
        // busybox tar for portability inside the minimal guest rootfs.
        let save_cmd = format!(
            "/bin/busybox tar -C {upper_dir} -cpf {tar_guest_tmp} . && \
             mv {tar_guest_tmp} {tar_guest}"
        );
        let started = std::time::Instant::now();
        let result = vm
            .exec_collect(
                "sh".to_string(),
                vec!["-c".to_string(), save_cmd],
                Duration::from_secs(120),
            )
            .await;
        match result {
            Ok(out) if out.exit_code == 0 => {
                let bytes = fs::metadata(
                    self.setup_commits_host_dir()
                        .join(format!("{commit_ref}.tar")),
                )
                .map(|m| m.len())
                .unwrap_or(0);
                tracing::info!(
                    commit_ref,
                    bytes,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "saved setup commit to cache"
                );
                Ok(())
            }
            Ok(out) => {
                tracing::warn!(
                    commit_ref,
                    exit_code = out.exit_code,
                    stderr = %out.stderr.trim(),
                    "setup commit save returned non-zero (next boot will re-run setup)"
                );
                Ok(())
            }
            Err(error) => {
                tracing::warn!(
                    commit_ref,
                    %error,
                    "setup commit save exec failed (next boot will re-run setup)"
                );
                Ok(())
            }
        }
    }

    /// Stop all containers and shut down the shared VM for a stack.
    ///
    /// Each container is stopped via `oci_kill` + `oci_delete`, then the
    /// shared VM is torn down. Container metadata is updated to `Stopped`.
    pub async fn shutdown_shared_vm(&self, stack_id: &str) -> Result<(), OciError> {
        let activation_lock = self.stack_activation_lock(stack_id).await;
        let _activation_guard = activation_lock.lock().await;
        let (stack_vms_count, stack_port_forwards_count) = {
            let vms = self.stack_vms.lock().await;
            let pfs = self.stack_port_forwards.lock().await;
            (vms.len(), pfs.len())
        };
        tracing::info!(
            target: "vz_post_stop",
            stack_id = %stack_id,
            stack_vms_count,
            stack_port_forwards_count,
            "[L4/stack-vm] shutdown_shared_vm entry"
        );
        let Some(vm) = self.stack_vms.lock().await.remove(stack_id) else {
            // Bug B fix: in-memory state can be empty after a daemon
            // respawn (kill -9 / OS reboot mid-operation). In that case
            // the SQLite state-store may still claim the sandbox is
            // running, but we have no VM handle to shut down. Treat
            // this as idempotent "already stopped" rather than the
            // previous error path that relied on a string-match mask
            // (`runtime_shutdown_error_is_not_active`) in the gRPC
            // handler. Still drop any leftover port-forward map entry
            // for this stack so subsequent boots start from a clean slate.
            tracing::warn!(
                stack_id,
                "shutdown_shared_vm: no in-memory VM (likely after daemon respawn); treating as already-stopped"
            );
            if let Some(pf) = self.stack_port_forwards.lock().await.remove(stack_id) {
                pf.shutdown().await;
            }
            return Ok(());
        };

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
        let pf_present = {
            let mut guard = self.stack_port_forwards.lock().await;
            guard.remove(stack_id)
        };
        match pf_present {
            Some(pf) => {
                tracing::info!(
                    target: "vz_post_stop",
                    stack_id = %stack_id,
                    "[L4/stack-vm] shutdown_shared_vm: awaiting PortForwarding::shutdown"
                );
                let started = std::time::Instant::now();
                pf.shutdown().await;
                tracing::info!(
                    target: "vz_post_stop",
                    stack_id = %stack_id,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "[L4/stack-vm] shutdown_shared_vm: PortForwarding::shutdown returned"
                );
            }
            None => {
                tracing::info!(
                    target: "vz_post_stop",
                    stack_id = %stack_id,
                    "[L4/stack-vm] shutdown_shared_vm: no PortForwarding registered for stack"
                );
            }
        }

        // Tear down the shared VM.
        let _ = vm.stop().await;
        let (stack_vms_count_after, stack_port_forwards_count_after) = {
            let vms = self.stack_vms.lock().await;
            let pfs = self.stack_port_forwards.lock().await;
            (vms.len(), pfs.len())
        };
        tracing::info!(
            target: "vz_post_stop",
            stack_id = %stack_id,
            stack_vms_count_after,
            stack_port_forwards_count_after,
            "[L4/stack-vm] shutdown_shared_vm complete"
        );
        Ok(())
    }

    /// Check whether a shared VM is running for the given stack.
    pub async fn has_shared_vm(&self, stack_id: &str) -> bool {
        self.stack_vms.lock().await.contains_key(stack_id)
    }

    /// Return the shared Linux VM hosting the given stack, if any.
    ///
    /// Returns `None` if the stack is not currently up. Intended for
    /// consumers that embed [`Runtime`] as a library and need direct
    /// access to the underlying VM handle for vsock operations
    /// (e.g., installing capability shims that dial back to a host
    /// broker via [`vz::Vm::vsock_listen`]).
    ///
    /// The returned [`Arc`] keeps the VM alive for as long as the
    /// caller holds it; normal lifecycle (shutdown via
    /// [`Self::shutdown_shared_vm`]) remains the caller's contract.
    pub async fn shared_vm_for(&self, stack_id: &str) -> Option<Arc<LinuxVm>> {
        self.stack_vms.lock().await.get(stack_id).cloned()
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
