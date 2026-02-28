use super::*;

pub(super) struct PreparedCreate {
    pub(super) service_name: String,
    pub(super) replica_index: u32,
    pub(super) image: String,
    pub(super) run_config: vz_runtime_contract::RunConfig,
}

impl PreparedCreate {
    /// Full name including replica index if replicas > 1.
    pub(super) fn full_name(&self) -> String {
        if self.replica_index > 1 {
            format!("{}-{}", self.service_name, self.replica_index)
        } else {
            self.service_name.clone()
        }
    }
}

impl<R: ContainerRuntime> StackExecutor<R> {
    /// Prepare a service create: resolve mounts, allocate ports, build config.
    ///
    /// This runs serially (needs `&mut self` for port allocation) and produces
    /// a [`PreparedCreate`] that can be executed in parallel.
    pub(super) fn prepare_create(
        &mut self,
        spec: &StackSpec,
        service_map: &HashMap<&str, &ServiceSpec>,
        service_name: &str,
        replica_index: u32,
    ) -> Result<PreparedCreate, StackError> {
        let svc_spec = service_map.get(service_name).ok_or_else(|| {
            StackError::InvalidSpec(format!("service '{service_name}' not found in stack spec"))
        })?;

        // Update state to Creating.
        self.store.save_observed_state(
            &spec.name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Creating,
                container_id: None,
                last_error: None,
                ready: false,
            },
        )?;

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceCreating {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
            },
        )?;

        // Resolve mounts using volume manager.
        let mut resolved_mounts = self
            .volumes
            .resolve_mounts(&svc_spec.mounts, &spec.volumes)?;
        // Skipped mounts from prepare_create are surfaced via the shared
        // VM boot path; single-service creates don't need separate tracking
        // because the shared boot already validated all service mounts.
        let _skipped = crate::volume::validate_bind_mounts(&mut resolved_mounts)?;

        // Allocate ports (resolves ephemeral ports, checks conflicts).
        let published = match self.ports.allocate(service_name, &svc_spec.ports) {
            Ok(p) => p,
            Err(e) => {
                if let Some(first_port) = svc_spec.ports.first() {
                    self.store.emit_event(
                        &spec.name,
                        &StackEvent::PortConflict {
                            stack_name: spec.name.clone(),
                            service_name: service_name.to_string(),
                            port: first_port.host_port.unwrap_or(first_port.container_port),
                        },
                    )?;
                }
                self.mark_failed(spec, service_name, &e.to_string())?;
                return Err(e);
            }
        };

        // Stage secret files and generate bind mounts.
        let secret_mounts = if svc_spec.secrets.is_empty() {
            vec![]
        } else {
            let secrets_dir = self.data_dir.join("secrets").join(&spec.name);
            std::fs::create_dir_all(&secrets_dir)?;
            for secret_ref in &svc_spec.secrets {
                let secret_def = spec
                    .secrets
                    .iter()
                    .find(|d| d.name == secret_ref.source)
                    .ok_or_else(|| {
                        StackError::InvalidSpec(format!(
                            "secret '{}' referenced by service '{}' not defined at top level",
                            secret_ref.source, service_name,
                        ))
                    })?;
                let content = load_secret_source_bytes(secret_def)?;
                std::fs::write(secrets_dir.join(&secret_ref.source), content)?;
            }
            secrets_to_mounts(&svc_spec.secrets, &secrets_dir)
        };

        // Convert ServiceSpec → RunConfig.
        let mut run_config = service_to_run_config(svc_spec, &resolved_mounts, &secret_mounts)?;

        // Generate container_id: replica 1 = base name, replica N = "{base}-{N}".
        let replicas = svc_spec.resources.replicas.max(1);
        let base_name = svc_spec.container_name.as_deref().unwrap_or(service_name);
        let container_id = if replicas > 1 && replica_index > 1 {
            format!("{}-{}", base_name, replica_index)
        } else {
            base_name.to_string()
        };
        run_config.container_id = Some(container_id);

        // Set the VirtioFS mount tag offset for this service in sandbox mode.
        if let Some(&offset) = self.mount_tag_offsets.get(service_name) {
            run_config.mount_tag_offset = offset;
        }

        // Compute the replica-qualified name for IP/netns lookup.
        let replica_qualified_name = if replicas > 1 && replica_index > 1 {
            format!("{service_name}-{replica_index}")
        } else {
            service_name.to_string()
        };

        // Override ports with resolved allocations.
        let service_target_host = self.service_ips.get(&replica_qualified_name).cloned();
        run_config.ports = published
            .iter()
            .map(|p| {
                let protocol = match p.protocol.as_str() {
                    "udp" => vz_runtime_contract::PortProtocol::Udp,
                    _ => vz_runtime_contract::PortProtocol::Tcp,
                };
                vz_runtime_contract::PortMapping {
                    host: p.host_port,
                    container: p.container_port,
                    protocol,
                    target_host: service_target_host.clone(),
                }
            })
            .collect();

        // Auto-inject sibling service hostnames for inter-service resolution.
        // Only inject hosts for services that share at least one network.
        let my_networks: HashSet<&str> = svc_spec.networks.iter().map(|n| n.as_str()).collect();

        for svc in &spec.services {
            if svc.name == service_name {
                continue;
            }
            if run_config.extra_hosts.iter().any(|(h, _)| h == &svc.name) {
                continue;
            }
            // Only add if the sibling shares at least one network.
            let shares_network = svc
                .networks
                .iter()
                .any(|n| my_networks.contains(n.as_str()));
            if shares_network {
                if let Some(ip) = self.service_ips.get(&svc.name) {
                    run_config.extra_hosts.push((svc.name.clone(), ip.clone()));
                }
            }
        }
        run_config.network_namespace_path =
            Some(format!("/var/run/netns/{replica_qualified_name}"));

        Ok(PreparedCreate {
            service_name: service_name.to_string(),
            replica_index,
            image: svc_spec.image.clone(),
            run_config,
        })
    }

    /// Finalize a successful container create: update state to Running.
    pub(super) fn finalize_create(
        &self,
        spec: &StackSpec,
        service_name: &str,
        container_id: &str,
    ) -> Result<(), StackError> {
        self.store.save_observed_state(
            &spec.name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Running,
                container_id: Some(container_id.to_string()),
                last_error: None,
                ready: false, // Health checks set this to true later.
            },
        )?;

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceReady {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
                runtime_id: container_id.to_string(),
            },
        )?;

        info!(service = %service_name, "service running");
        Ok(())
    }
}
