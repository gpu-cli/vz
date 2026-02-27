use super::create::PreparedCreate;
use super::*;

/// Group create/recreate actions into topological levels for parallel execution.
///
/// Services at the same level have no dependency edges between them
/// (within the current action set) and can safely run in parallel.
/// Level 0 contains services with no in-batch deps, level 1 depends
/// only on level 0, etc.
pub(super) fn compute_topo_levels<'a>(
    creates: &[&'a Action],
    spec: &StackSpec,
) -> Vec<Vec<&'a Action>> {
    if creates.is_empty() {
        return vec![];
    }

    // Build dependency map from the spec.
    let dep_map: HashMap<&str, Vec<&str>> = spec
        .services
        .iter()
        .map(|s| {
            let deps: Vec<&str> = s.depends_on.iter().map(|d| d.service.as_str()).collect();
            (s.name.as_str(), deps)
        })
        .collect();

    // Only consider deps that are also in our action set.
    let action_names: HashSet<&str> = creates.iter().map(|a| a.service_name()).collect();

    // Assign each action a level. Since creates are already topo-sorted,
    // we can process in order and look up deps that have already been assigned.
    let mut levels: HashMap<&str, usize> = HashMap::new();
    for action in creates {
        let name = action.service_name();
        let deps = dep_map.get(name).map(|d| d.as_slice()).unwrap_or(&[]);
        let max_dep_level = deps
            .iter()
            .filter(|d| action_names.contains(**d))
            .filter_map(|d| levels.get(d))
            .copied()
            .max();

        let my_level = match max_dep_level {
            Some(l) => l + 1,
            None => 0,
        };
        levels.insert(name, my_level);
    }

    // Group by level.
    let max_level = levels.values().copied().max().unwrap_or(0);
    let mut result: Vec<Vec<&Action>> = (0..=max_level).map(|_| Vec::new()).collect();
    for action in creates {
        let level = levels[action.service_name()];
        result[level].push(action);
    }

    result
}

/// Parse the base octets from a CIDR subnet string (e.g., `"172.20.1.0/24"` -> `[172, 20, 1, 0]`).
pub(super) fn parse_subnet_base(subnet: &str) -> [u8; 4] {
    let ip_part = subnet.split('/').next().unwrap_or("172.20.0.0");
    let octets: Vec<u8> = ip_part.split('.').filter_map(|o| o.parse().ok()).collect();
    [
        octets.first().copied().unwrap_or(172),
        octets.get(1).copied().unwrap_or(20),
        octets.get(2).copied().unwrap_or(0),
        octets.get(3).copied().unwrap_or(0),
    ]
}

/// Parse the prefix length from a CIDR subnet string (e.g., `"172.20.1.0/24"` -> `24`).
pub(super) fn parse_subnet_prefix(subnet: &str) -> u8 {
    subnet
        .split('/')
        .nth(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(24)
}

impl<R: ContainerRuntime> StackExecutor<R> {
    /// Execute a batch of reconciler actions for the given stack spec.
    ///
    /// Services at the same topological level (no dependency edges
    /// between them) are created in parallel using [`std::thread::scope`],
    /// while services at different levels execute sequentially to respect
    /// `depends_on` ordering. This gives up to N x speedup for stacks
    /// with N independent services.
    ///
    /// Port allocation is tracked across services: explicit host ports
    /// are validated for conflicts, and `None` host ports get ephemeral
    /// assignments. Ports are released on service removal.
    ///
    /// For multi-service stacks, a sandbox is created before spawning
    /// containers, and per-service network namespaces are set up so that
    /// containers can communicate using real IP addresses (Docker Compose
    /// style networking). The sandbox owns the lifecycle of all containers
    /// and networking within the stack.
    pub fn execute(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
    ) -> Result<ExecutionResult, StackError> {
        // Ensure named volume directories exist before creating containers.
        let created_volumes = self.volumes.ensure_volumes(&spec.volumes)?;
        for vol_name in &created_volumes {
            self.store.emit_event(
                &spec.name,
                &StackEvent::VolumeCreated {
                    stack_name: spec.name.clone(),
                    volume_name: vol_name.clone(),
                },
            )?;
        }

        // Boot shared VM and set up networking if there are create actions
        // and no shared VM is running yet.
        let mut all_skipped_mounts: Vec<crate::volume::SkippedMount> = Vec::new();
        let has_creates = actions.iter().any(|a| {
            matches!(
                a,
                Action::ServiceCreate { .. } | Action::ServiceRecreate { .. }
            )
        });

        if has_creates && !self.runtime.has_sandbox(&spec.name) {
            // ── Compute per-network subnets ─────────────────────────────
            //
            // Each distinct network gets its own subnet. Explicit subnets
            // from `NetworkSpec` are honoured; others are auto-assigned
            // from the 172.20.N.0/24 pool.
            let network_subnets: HashMap<String, String> = {
                let mut subnets = HashMap::new();
                let mut next_subnet_idx: u8 = 0;
                for net in &spec.networks {
                    let subnet = if let Some(ref explicit) = net.subnet {
                        explicit.clone()
                    } else {
                        let s = format!("172.20.{}.0/24", next_subnet_idx);
                        next_subnet_idx = next_subnet_idx.saturating_add(1);
                        s
                    };
                    subnets.insert(net.name.clone(), subnet);
                }
                subnets
            };

            // ── Per-service IP allocation ───────────────────────────────
            //
            // For each (network, service) pair, assign an IP within that
            // network's subnet. Gateway is .1, services start at .2.
            // `service_primary_ip` maps service_name -> first assigned IP
            // (used for port forwarding target_host).
            let mut service_primary_ip: HashMap<String, String> = HashMap::new();
            let mut network_services: Vec<vz_runtime_contract::NetworkServiceConfig> = Vec::new();

            for net in &spec.networks {
                let subnet = &network_subnets[&net.name];
                let base_octets = parse_subnet_base(subnet);
                let prefix = parse_subnet_prefix(subnet);
                let mut host_offset: u8 = 2; // .1 = bridge gateway

                for svc in &spec.services {
                    // A service belongs to this network if its `networks` list
                    // contains this network name (Issue 1 ensures default membership).
                    if !svc.networks.contains(&net.name) {
                        continue;
                    }

                    let replicas = svc.resources.replicas.max(1);
                    for r in 1..=replicas {
                        let replica_name = if r == 1 {
                            svc.name.clone()
                        } else {
                            format!("{}-{r}", svc.name)
                        };

                        let ip = format!(
                            "{}.{}.{}.{}/{}",
                            base_octets[0], base_octets[1], base_octets[2], host_offset, prefix
                        );
                        let ip_no_prefix = format!(
                            "{}.{}.{}.{}",
                            base_octets[0], base_octets[1], base_octets[2], host_offset
                        );

                        // First IP assigned becomes the primary (for port forwarding).
                        service_primary_ip
                            .entry(replica_name.clone())
                            .or_insert(ip_no_prefix);

                        network_services.push(vz_runtime_contract::NetworkServiceConfig {
                            name: replica_name,
                            addr: ip,
                            network_name: net.name.clone(),
                        });

                        host_offset = host_offset.saturating_add(1);
                    }
                }
            }

            // ── Collect all ports using primary IPs for target_host ──────
            let all_ports: Vec<vz_runtime_contract::PortMapping> = spec
                .services
                .iter()
                .flat_map(|svc| {
                    let service_ip = service_primary_ip
                        .get(&svc.name)
                        .cloned()
                        .unwrap_or_else(|| "127.0.0.1".to_string());
                    svc.ports.iter().map(move |p| {
                        let protocol = match p.protocol.as_str() {
                            "udp" => vz_runtime_contract::PortProtocol::Udp,
                            _ => vz_runtime_contract::PortProtocol::Tcp,
                        };
                        vz_runtime_contract::PortMapping {
                            host: p.host_port.unwrap_or(p.container_port),
                            container: p.container_port,
                            protocol,
                            target_host: Some(service_ip.clone()),
                        }
                    })
                })
                .collect();

            // Collect all bind mounts across services so VirtioFS shares can
            // be configured at VM creation time. Named volumes use a persistent
            // disk image (not VirtioFS), so they're skipped here.
            let mut all_volume_mounts: Vec<vz_runtime_contract::StackVolumeMount> = Vec::new();
            let mut mount_tag_offsets: HashMap<String, usize> = HashMap::new();
            let mut has_named_volumes = false;
            for svc in &spec.services {
                let mut resolved = self.volumes.resolve_mounts(&svc.mounts, &spec.volumes)?;
                all_skipped_mounts.extend(crate::volume::validate_bind_mounts(&mut resolved)?);
                // This service's bind mounts start at the current global index.
                mount_tag_offsets.insert(svc.name.clone(), all_volume_mounts.len());
                for rm in &resolved {
                    match &rm.kind {
                        crate::volume::ResolvedMountKind::Bind => {
                            if let Some(host_path) = &rm.host_path {
                                let idx = all_volume_mounts.len();
                                all_volume_mounts.push(vz_runtime_contract::StackVolumeMount {
                                    tag: format!("vz-mount-{idx}"),
                                    host_path: host_path.clone(),
                                    read_only: rm.read_only,
                                });
                            }
                        }
                        crate::volume::ResolvedMountKind::Named { .. } => {
                            has_named_volumes = true;
                        }
                        crate::volume::ResolvedMountKind::Ephemeral => {}
                    }
                }
            }
            self.mount_tag_offsets = mount_tag_offsets;

            // Stage all secrets before boot so they can be included in VirtioFS shares.
            // This must happen BEFORE creating resources so secrets are in all_volume_mounts.
            let secrets_dir = self.data_dir.join("secrets").join(&spec.name);
            for svc in &spec.services {
                for secret_ref in &svc.secrets {
                    let secret_def = spec.secrets.iter().find(|d| d.name == secret_ref.source);
                    if let Some(def) = secret_def {
                        let secret_path = secrets_dir.join(&secret_ref.source);
                        if !secret_path.exists() {
                            if let Some(file_path) = def.file() {
                                if let Ok(content) = std::fs::read(file_path) {
                                    let _ = std::fs::create_dir_all(&secrets_dir);
                                    let _ = std::fs::write(&secret_path, content);

                                    // Add secret to volume mounts for VirtioFS sharing.
                                    // Use "vz-mount-" prefix so OCI runtime translates to /mnt/vz-mount-X.
                                    let idx = all_volume_mounts.len();
                                    all_volume_mounts.push(vz_runtime_contract::StackVolumeMount {
                                        tag: format!("vz-mount-{idx}"),
                                        host_path: secret_path,
                                        read_only: true,
                                    });
                                }
                            }
                        }
                    }
                }
            }

            // Adjust mount_tag_offsets to account for secrets added to all_volume_mounts.
            // The offset needs to account for:
            // 1. All regular mounts from all services (they come before secrets)
            // 2. All secrets from services that come before this one
            //
            // When OCI runtime calculates global_idx = tag_offset + idx:
            // - idx is position in the combined [regular + secrets] mount list
            // - Secrets in all_volume_mounts are after ALL regular mounts
            // So we need to shift by: total regular mounts + secrets from previous services
            let total_regular_mounts: usize = spec
                .services
                .iter()
                .map(|s| {
                    self.volumes
                        .resolve_mounts(&s.mounts, &spec.volumes)
                        .map(|m| {
                            m.iter()
                                .filter(|m| {
                                    matches!(m.kind, crate::volume::ResolvedMountKind::Bind)
                                })
                                .count()
                        })
                        .unwrap_or(0)
                })
                .sum();

            let adjustment_for_each_service: Vec<(String, usize)> = spec
                .services
                .iter()
                .map(|svc| {
                    // Secrets from services that come before this one
                    let prev_secrets: usize = spec
                        .services
                        .iter()
                        .take_while(|s| s.name != svc.name)
                        .map(|s| s.secrets.len())
                        .sum();
                    // Total regular mounts + previous secrets
                    let adjustment = total_regular_mounts + prev_secrets;
                    (svc.name.clone(), adjustment)
                })
                .collect();

            for (svc_name, adjustment) in adjustment_for_each_service {
                if let Some(offset) = self.mount_tag_offsets.get_mut(&svc_name) {
                    *offset += adjustment;
                }
            }

            // Create persistent disk image for named volumes if needed.
            let disk_image_path = if has_named_volumes {
                let disk_size_bytes = spec.disk_size_mb.map(|mb| mb * 1024 * 1024);
                let is_new = self.volumes.ensure_disk_image(disk_size_bytes)?;
                if is_new {
                    info!(stack = %spec.name, "created persistent disk image for named volumes");
                }
                Some(self.volumes.disk_image_path())
            } else {
                None
            };

            // Compute aggregate resource hints for VM sizing.
            let resources = {
                let max_cpus = spec
                    .services
                    .iter()
                    .filter_map(|s| s.resources.cpus)
                    .map(|c| c.ceil() as u8)
                    .max();
                let total_memory_mb = {
                    let sum: u64 = spec
                        .services
                        .iter()
                        .filter_map(|s| s.resources.memory_bytes)
                        .map(|b| b / (1024 * 1024))
                        .sum();
                    if sum > 0 { Some(sum) } else { None }
                };
                vz_runtime_contract::StackResourceHint {
                    cpus: max_cpus,
                    memory_mb: total_memory_mb,
                    volume_mounts: all_volume_mounts,
                    disk_image_path,
                }
            };

            info!(stack = %spec.name, services = spec.services.len(), "creating sandbox");
            self.runtime
                .create_sandbox(&spec.name, all_ports, resources)?;

            info!(stack = %spec.name, "setting up per-service network namespaces");
            self.runtime
                .setup_sandbox_network(&spec.name, network_services)?;

            // Store primary IPs for use in prepare_create.
            self.service_ips = service_primary_ip;

            // Persist allocator state after VM boot + network setup.
            self.persist_allocator_state(&spec.name)?;
        }

        let service_map: HashMap<&str, &ServiceSpec> =
            spec.services.iter().map(|s| (s.name.as_str(), s)).collect();

        let mut result = ExecutionResult::default();

        // Partition into creates/recreates and removes.
        let creates: Vec<&Action> = actions
            .iter()
            .filter(|a| {
                matches!(
                    a,
                    Action::ServiceCreate { .. } | Action::ServiceRecreate { .. }
                )
            })
            .collect();
        let removes: Vec<&Action> = actions
            .iter()
            .filter(|a| matches!(a, Action::ServiceRemove { .. }))
            .collect();

        // Group creates by topo level for parallel execution.
        let levels = compute_topo_levels(&creates, spec);

        for level in &levels {
            // Clean up old containers before creating new ones.
            // Recreates always remove the old container. For creates from a
            // Failed state, the old container may still exist in the runtime
            // — clean it up to avoid "container already exists" errors.
            for action in level {
                let should_remove = match action {
                    Action::ServiceRecreate { .. } => true,
                    Action::ServiceCreate { service_name } => {
                        let observed = self
                            .store
                            .load_observed_state(&spec.name)
                            .unwrap_or_default();
                        observed
                            .iter()
                            .any(|o| o.service_name == *service_name && o.container_id.is_some())
                    }
                    _ => false,
                };
                if should_remove {
                    if let Err(e) = self.execute_remove(spec, action.service_name()) {
                        error!(service = %action.service_name(), error = %e, "failed to remove old container");
                    }
                }
            }

            // Serial prep: allocate ports, resolve mounts, build configs.
            // Expand each service into multiple creates based on replica count.
            let mut prepared: Vec<PreparedCreate> = Vec::new();
            for action in level {
                let service_name = action.service_name();

                // Get replica count for this service
                let replicas = if let Some(svc_spec) = service_map.get(service_name) {
                    svc_spec.resources.replicas.max(1)
                } else {
                    1
                };

                // Create one PreparedCreate per replica
                for replica_index in 1..=replicas {
                    match self.prepare_create(spec, &service_map, service_name, replica_index) {
                        Ok(prep) => prepared.push(prep),
                        Err(e) => {
                            result.failed += 1;
                            let name = if replicas > 1 {
                                format!("{}-{}", service_name, replica_index)
                            } else {
                                service_name.to_string()
                            };
                            result.errors.push((name, e.to_string()));
                        }
                    }
                }
            }

            // Deduplicate image pulls: pull each unique image once serially
            // before entering the parallel container creation phase. This avoids
            // concurrent layer extraction races when multiple replicas share an image.
            let mut pulled_images: HashSet<String> = HashSet::new();
            let mut pull_failed: HashSet<String> = HashSet::new();
            for prep in &prepared {
                if pulled_images.contains(&prep.image) || pull_failed.contains(&prep.image) {
                    continue;
                }
                info!(image = %prep.image, "pulling image (deduplicated)");
                if let Err(e) = self.runtime.pull(&prep.image) {
                    error!(image = %prep.image, error = %e, "image pull failed");
                    pull_failed.insert(prep.image.clone());
                } else {
                    pulled_images.insert(prep.image.clone());
                }
            }

            // Partition prepared creates: those whose image pull failed go straight
            // to the error path; the rest proceed to parallel container creation.
            let (ok_prepared, failed_prepared): (Vec<_>, Vec<_>) = prepared
                .into_iter()
                .partition(|p| pulled_images.contains(&p.image));

            for prep in failed_prepared {
                let full_name = prep.full_name();
                let msg = format!("image pull failed for {}", prep.image);
                self.mark_failed(spec, &full_name, &msg)?;
                result.failed += 1;
                result.errors.push((full_name, msg));
            }

            if ok_prepared.len() <= 1 {
                // Single container — execute inline, no thread overhead.
                for prep in ok_prepared {
                    let full_name = prep.full_name();
                    info!(service = %full_name, image = %prep.image, "creating container");
                    let create_result =
                        self.runtime
                            .create_in_sandbox(&spec.name, &prep.image, prep.run_config);
                    match create_result {
                        Ok(container_id) => {
                            self.finalize_create(spec, &full_name, &container_id)?;
                            result.succeeded += 1;
                        }
                        Err(e) => {
                            self.mark_failed(spec, &full_name, &e.to_string())?;
                            result.failed += 1;
                            result.errors.push((full_name, e.to_string()));
                        }
                    }
                }
            } else {
                // Parallel create for multiple containers at the same level.
                // Images are already pulled; only create_in_sandbox runs in threads.
                let full_names: Vec<String> = ok_prepared.iter().map(|p| p.full_name()).collect();
                info!(
                    services = ?full_names,
                    "creating {} containers in parallel",
                    full_names.len()
                );

                let runtime = &self.runtime;
                let stack_name = &spec.name;
                let outcomes: Vec<Result<String, StackError>> = std::thread::scope(|s| {
                    let handles: Vec<_> = ok_prepared
                        .into_iter()
                        .map(|prep| {
                            let full_name = prep.full_name();
                            s.spawn(move || -> Result<String, StackError> {
                                info!(service = %full_name, image = %prep.image, "creating container");
                                runtime.create_in_sandbox(
                                    stack_name,
                                    &prep.image,
                                    prep.run_config,
                                )
                            })
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|h| match h.join() {
                            Ok(result) => result,
                            Err(_) => Err(StackError::Network(
                                "container create thread panicked".to_string(),
                            )),
                        })
                        .collect()
                });

                // Serial post: update state for each outcome.
                for (service_name, outcome) in full_names.iter().zip(outcomes) {
                    match outcome {
                        Ok(container_id) => {
                            self.finalize_create(spec, service_name, &container_id)?;
                            result.succeeded += 1;
                        }
                        Err(e) => {
                            self.mark_failed(spec, service_name, &e.to_string())?;
                            result.failed += 1;
                            result.errors.push((service_name.clone(), e.to_string()));
                        }
                    }
                }
            }
        }

        // Execute removes sequentially.
        for action in &removes {
            match self.execute_remove(spec, action.service_name()) {
                Ok(()) => result.succeeded += 1,
                Err(e) => {
                    result.failed += 1;
                    result
                        .errors
                        .push((action.service_name().to_string(), e.to_string()));
                }
            }
        }

        result.skipped_mounts = all_skipped_mounts;
        Ok(result)
    }
}
