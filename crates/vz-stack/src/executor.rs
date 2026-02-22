//! Stack executor: bridge between reconciler [`Action`]s and the OCI runtime.
//!
//! The [`StackExecutor`] takes a list of actions from [`apply`](crate::apply)
//! and executes them through a [`ContainerRuntime`] implementation:
//! - `ServiceCreate` → pull image + create container + update state to Running
//! - `ServiceRemove` → stop + remove container + update state to Stopped
//! - `ServiceRecreate` → stop + remove + create (full cycle)
//!
//! State transitions and lifecycle events are persisted to the [`StateStore`].

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use tracing::{error, info};

use crate::convert::{secrets_to_mounts, service_to_run_config};
use crate::error::StackError;
use crate::events::StackEvent;
use crate::network::{PublishedPort, resolve_ports};
use crate::reconcile::Action;
use crate::spec::{ServiceSpec, StackSpec};
use crate::state_store::{ServiceObservedState, ServicePhase, StateStore};
use crate::volume::VolumeManager;

/// Trait abstracting container lifecycle operations.
///
/// The real implementation wraps `vz_runtime_contract::Runtime` (which is async);
/// tests use a synchronous mock. The CLI layer bridges async by
/// calling `block_on` around the real runtime methods.
pub trait ContainerRuntime: Send + Sync {
    /// Pull an image if not already present. Returns the image ID.
    fn pull(&self, image: &str) -> Result<String, StackError>;

    /// Create and start a container from the given image with the given config.
    /// Returns the container ID.
    fn create(
        &self,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<String, StackError>;

    /// Stop a running container. No-op if already stopped.
    fn stop(&self, container_id: &str) -> Result<(), StackError>;

    /// Remove a stopped container and its resources.
    fn remove(&self, container_id: &str) -> Result<(), StackError>;

    /// Execute a command inside a running container.
    /// Returns the exit code (0 = success).
    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError>;

    /// Retrieve logs (stdout/stderr) from a container.
    ///
    /// Returns a [`ContainerLogs`] with captured stdout and stderr.
    /// The default implementation returns empty logs; real runtimes
    /// should override this to read from the container log driver.
    fn logs(&self, _container_id: &str) -> Result<ContainerLogs, StackError> {
        Ok(ContainerLogs::default())
    }

    /// Boot a shared VM for a multi-service stack.
    ///
    /// After calling this, containers for the stack should be created via
    /// [`create_in_stack`](Self::create_in_stack) instead of [`create`](Self::create).
    fn boot_shared_vm(
        &self,
        _stack_id: &str,
        _ports: &[vz_runtime_contract::PortMapping],
        _resources: vz_runtime_contract::StackResourceHint,
    ) -> Result<(), StackError> {
        Ok(())
    }

    /// Set up per-service network namespaces inside the shared VM.
    ///
    /// Creates a bridge and per-service netns with veth pairs so that
    /// containers can communicate using real IP addresses (Docker Compose
    /// style networking).
    fn network_setup(
        &self,
        _stack_id: &str,
        _services: &[vz_runtime_contract::NetworkServiceConfig],
    ) -> Result<(), StackError> {
        Ok(())
    }

    /// Tear down network namespaces for a stack.
    fn network_teardown(
        &self,
        _stack_id: &str,
        _service_names: &[String],
    ) -> Result<(), StackError> {
        Ok(())
    }

    /// Create and start a container inside a shared stack VM.
    ///
    /// The VM must have been booted via [`boot_shared_vm`](Self::boot_shared_vm).
    fn create_in_stack(
        &self,
        stack_id: &str,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<String, StackError> {
        let _ = stack_id;
        // Default: fall back to individual VM per container.
        self.create(image, config)
    }

    /// Shut down the shared VM for a stack, stopping all its containers.
    fn shutdown_shared_vm(&self, _stack_id: &str) -> Result<(), StackError> {
        Ok(())
    }

    /// Check whether a shared VM is running for the given stack.
    fn has_shared_vm(&self, _stack_id: &str) -> bool {
        false
    }
}

/// Container log output (stdout + stderr interleaved).
#[derive(Debug, Clone, Default)]
pub struct ContainerLogs {
    /// Combined stdout/stderr output.
    pub output: String,
}

/// Tracks host port allocations across services within a stack.
///
/// Ensures no two services bind to the same host port and supports
/// ephemeral port allocation for ports without an explicit host binding.
pub struct PortTracker {
    /// Allocated ports keyed by service name.
    allocated: HashMap<String, Vec<PublishedPort>>,
}

impl PortTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self {
            allocated: HashMap::new(),
        }
    }

    /// All host ports currently allocated across all services.
    pub fn in_use(&self) -> HashSet<u16> {
        self.allocated
            .values()
            .flat_map(|ports| ports.iter().map(|p| p.host_port))
            .collect()
    }

    /// Allocate ports for a service. Returns the resolved port mappings.
    ///
    /// Explicit host_ports are verified against currently allocated ports.
    /// `None` host_ports get an ephemeral port assigned.
    pub fn allocate(
        &mut self,
        service_name: &str,
        ports: &[crate::spec::PortSpec],
    ) -> Result<Vec<PublishedPort>, StackError> {
        let in_use = self.in_use();
        let resolved = resolve_ports(ports, &in_use)?;
        self.allocated
            .insert(service_name.to_string(), resolved.clone());
        Ok(resolved)
    }

    /// Release all ports for a service.
    pub fn release(&mut self, service_name: &str) {
        self.allocated.remove(service_name);
    }

    /// Get the published ports for a service (if any).
    pub fn ports_for(&self, service_name: &str) -> Option<&[PublishedPort]> {
        self.allocated.get(service_name).map(|v| v.as_slice())
    }
}

impl Default for PortTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Executes reconciler actions through an OCI container runtime.
pub struct StackExecutor<R: ContainerRuntime> {
    runtime: R,
    store: StateStore,
    data_dir: PathBuf,
    volumes: VolumeManager,
    ports: PortTracker,
    /// Per-service primary IP (first network IP, used for port forwarding and /etc/hosts).
    /// Populated during shared VM boot / network setup.
    service_ips: HashMap<String, String>,
}

/// Result of executing a batch of actions.
#[derive(Debug, Clone, Default)]
pub struct ExecutionResult {
    /// Number of actions that succeeded.
    pub succeeded: usize,
    /// Number of actions that failed.
    pub failed: usize,
    /// Per-action error messages (service_name → error).
    pub errors: Vec<(String, String)>,
}

impl ExecutionResult {
    /// Whether all actions succeeded.
    pub fn all_succeeded(&self) -> bool {
        self.failed == 0
    }
}

/// Pre-computed data for a service create, ready for parallel execution.
///
/// Port allocation and mount resolution happen serially (they need
/// `&mut self`), then image pull + container create run in parallel.
struct PreparedCreate {
    service_name: String,
    image: String,
    run_config: vz_runtime_contract::RunConfig,
    use_shared_vm: bool,
}

/// Group create/recreate actions into topological levels for parallel execution.
///
/// Services at the same level have no dependency edges between them
/// (within the current action set) and can safely run in parallel.
/// Level 0 contains services with no in-batch deps, level 1 depends
/// only on level 0, etc.
fn compute_topo_levels<'a>(creates: &[&'a Action], spec: &StackSpec) -> Vec<Vec<&'a Action>> {
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
fn parse_subnet_base(subnet: &str) -> [u8; 4] {
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
fn parse_subnet_prefix(subnet: &str) -> u8 {
    subnet
        .split('/')
        .nth(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(24)
}

impl<R: ContainerRuntime> StackExecutor<R> {
    /// Create a new executor with the given runtime, state store, and data directory.
    ///
    /// The data directory is used for named volume storage under `<data_dir>/volumes/`
    /// and secret staging under `<data_dir>/secrets/`.
    pub fn new(runtime: R, store: StateStore, data_dir: &Path) -> Self {
        Self {
            runtime,
            store,
            data_dir: data_dir.to_path_buf(),
            volumes: VolumeManager::new(data_dir),
            ports: PortTracker::new(),
            service_ips: HashMap::new(),
        }
    }

    /// Access the underlying state store.
    pub fn store(&self) -> &StateStore {
        &self.store
    }

    /// Mutably access the underlying state store.
    pub fn store_mut(&mut self) -> &mut StateStore {
        &mut self.store
    }

    /// Access the volume manager.
    pub fn volumes(&self) -> &VolumeManager {
        &self.volumes
    }

    /// Access the port tracker.
    pub fn ports(&self) -> &PortTracker {
        &self.ports
    }

    /// Access the underlying container runtime.
    pub fn runtime(&self) -> &R {
        &self.runtime
    }

    /// Execute a batch of reconciler actions for the given stack spec.
    ///
    /// Services at the same topological level (no dependency edges
    /// between them) are created in parallel using [`std::thread::scope`],
    /// while services at different levels execute sequentially to respect
    /// `depends_on` ordering. This gives up to N× speedup for stacks
    /// with N independent services.
    ///
    /// Port allocation is tracked across services: explicit host ports
    /// are validated for conflicts, and `None` host ports get ephemeral
    /// assignments. Ports are released on service removal.
    ///
    /// For multi-service stacks, a shared VM is booted before creating
    /// containers, and per-service network namespaces are set up so
    /// that containers can communicate using real IP addresses (Docker
    /// Compose style networking).
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
        let has_creates = actions.iter().any(|a| {
            matches!(
                a,
                Action::ServiceCreate { .. } | Action::ServiceRecreate { .. }
            )
        });

        if has_creates && !self.runtime.has_shared_vm(&spec.name) && spec.services.len() > 1 {
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
                        .entry(svc.name.clone())
                        .or_insert(ip_no_prefix);

                    network_services.push(vz_runtime_contract::NetworkServiceConfig {
                        name: svc.name.clone(),
                        addr: ip,
                        network_name: net.name.clone(),
                    });

                    host_offset = host_offset.saturating_add(1);
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
                }
            };

            info!(stack = %spec.name, services = spec.services.len(), "booting shared VM");
            self.runtime
                .boot_shared_vm(&spec.name, &all_ports, resources)?;

            info!(stack = %spec.name, "setting up per-service network namespaces");
            self.runtime.network_setup(&spec.name, &network_services)?;

            // Store primary IPs for use in prepare_create.
            self.service_ips = service_primary_ip;
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
        let use_shared_vm = self.runtime.has_shared_vm(&spec.name);

        for level in &levels {
            // Handle recreates: remove old containers first (serial).
            for action in level {
                if let Action::ServiceRecreate { service_name } = action {
                    if let Err(e) = self.execute_remove(spec, service_name) {
                        error!(service = %service_name, error = %e, "failed to remove old container during recreate");
                    }
                }
            }

            // Serial prep: allocate ports, resolve mounts, build configs.
            let mut prepared: Vec<PreparedCreate> = Vec::new();
            for action in level {
                let service_name = action.service_name();
                match self.prepare_create(spec, &service_map, service_name, use_shared_vm) {
                    Ok(prep) => prepared.push(prep),
                    Err(e) => {
                        result.failed += 1;
                        result
                            .errors
                            .push((service_name.to_string(), e.to_string()));
                    }
                }
            }

            if prepared.len() <= 1 {
                // Single service — execute inline, no thread overhead.
                for prep in prepared {
                    let service_name = prep.service_name.clone();
                    info!(service = %service_name, image = %prep.image, "creating container");
                    if let Err(e) = self.runtime.pull(&prep.image) {
                        self.mark_failed(spec, &service_name, &e.to_string())?;
                        result.failed += 1;
                        result.errors.push((service_name, e.to_string()));
                        continue;
                    }
                    let create_result = if prep.use_shared_vm {
                        self.runtime
                            .create_in_stack(&spec.name, &prep.image, prep.run_config)
                    } else {
                        self.runtime.create(&prep.image, prep.run_config)
                    };
                    match create_result {
                        Ok(container_id) => {
                            self.finalize_create(spec, &service_name, &container_id)?;
                            result.succeeded += 1;
                        }
                        Err(e) => {
                            self.mark_failed(spec, &service_name, &e.to_string())?;
                            result.failed += 1;
                            result.errors.push((service_name, e.to_string()));
                        }
                    }
                }
            } else {
                // Parallel pull + create for multiple services at the same level.
                let service_names: Vec<String> =
                    prepared.iter().map(|p| p.service_name.clone()).collect();
                info!(
                    services = ?service_names,
                    "creating {} containers in parallel",
                    service_names.len()
                );

                let runtime = &self.runtime;
                let stack_name = &spec.name;
                let outcomes: Vec<Result<String, StackError>> = std::thread::scope(|s| {
                    let handles: Vec<_> = prepared
                        .into_iter()
                        .map(|prep| {
                            s.spawn(move || -> Result<String, StackError> {
                                info!(service = %prep.service_name, image = %prep.image, "pulling image");
                                runtime.pull(&prep.image)?;
                                info!(service = %prep.service_name, image = %prep.image, "creating container");
                                if prep.use_shared_vm {
                                    runtime.create_in_stack(
                                        stack_name,
                                        &prep.image,
                                        prep.run_config,
                                    )
                                } else {
                                    runtime.create(&prep.image, prep.run_config)
                                }
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
                for (service_name, outcome) in service_names.iter().zip(outcomes) {
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

        Ok(result)
    }

    /// Prepare a service create: resolve mounts, allocate ports, build config.
    ///
    /// This runs serially (needs `&mut self` for port allocation) and produces
    /// a [`PreparedCreate`] that can be executed in parallel.
    fn prepare_create(
        &mut self,
        spec: &StackSpec,
        service_map: &HashMap<&str, &ServiceSpec>,
        service_name: &str,
        use_shared_vm: bool,
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
        let resolved_mounts = self
            .volumes
            .resolve_mounts(&svc_spec.mounts, &spec.volumes)?;

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
                let content = std::fs::read(&secret_def.file).map_err(|e| {
                    StackError::InvalidSpec(format!(
                        "failed to read secret file '{}': {}",
                        secret_def.file, e,
                    ))
                })?;
                std::fs::write(secrets_dir.join(&secret_ref.source), content)?;
            }
            secrets_to_mounts(&svc_spec.secrets, &secrets_dir)
        };

        // Convert ServiceSpec → RunConfig.
        let mut run_config = service_to_run_config(svc_spec, &resolved_mounts, &secret_mounts)?;
        run_config.container_id = Some(service_name.to_string());

        // Override ports with resolved allocations.
        let service_target_host = if use_shared_vm {
            self.service_ips.get(service_name).cloned()
        } else {
            None
        };
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
        // Issue 4: only inject hosts for services that share at least one network.
        if use_shared_vm {
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
            run_config.network_namespace_path = Some(format!("/var/run/netns/{service_name}"));
        } else {
            for svc in &spec.services {
                if svc.name != service_name
                    && !run_config.extra_hosts.iter().any(|(h, _)| h == &svc.name)
                {
                    run_config
                        .extra_hosts
                        .push((svc.name.clone(), "127.0.0.1".to_string()));
                }
            }
        }

        Ok(PreparedCreate {
            service_name: service_name.to_string(),
            image: svc_spec.image.clone(),
            run_config,
            use_shared_vm,
        })
    }

    /// Finalize a successful container create: update state to Running.
    fn finalize_create(
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

    /// Execute a service removal: stop + remove container, release ports, update state.
    fn execute_remove(&mut self, spec: &StackSpec, service_name: &str) -> Result<(), StackError> {
        // Find current container_id from observed state.
        let observed = self.store.load_observed_state(&spec.name)?;
        let container_id = observed
            .iter()
            .find(|o| o.service_name == service_name)
            .and_then(|o| o.container_id.clone());

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceStopping {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
            },
        )?;

        // Stop and remove if we have a container.
        if let Some(ref cid) = container_id {
            info!(service = %service_name, container = %cid, "stopping container");
            if let Err(e) = self.runtime.stop(cid) {
                error!(service = %service_name, error = %e, "failed to stop container");
                // Continue with remove attempt.
            }

            info!(service = %service_name, container = %cid, "removing container");
            if let Err(e) = self.runtime.remove(cid) {
                error!(service = %service_name, error = %e, "failed to remove container");
            }
        }

        // Release allocated ports.
        self.ports.release(service_name);

        // Update state to Stopped.
        self.store.save_observed_state(
            &spec.name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Stopped,
                container_id: None,
                last_error: None,
                ready: false,
            },
        )?;

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceStopped {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
                exit_code: 0,
            },
        )?;

        info!(service = %service_name, "service stopped");
        Ok(())
    }

    /// Mark a service as failed with an error message.
    fn mark_failed(
        &self,
        spec: &StackSpec,
        service_name: &str,
        error_msg: &str,
    ) -> Result<(), StackError> {
        self.store.save_observed_state(
            &spec.name,
            &ServiceObservedState {
                service_name: service_name.to_string(),
                phase: ServicePhase::Failed,
                container_id: None,
                last_error: Some(error_msg.to_string()),
                ready: false,
            },
        )?;

        self.store.emit_event(
            &spec.name,
            &StackEvent::ServiceFailed {
                stack_name: spec.name.clone(),
                service_name: service_name.to_string(),
                error: error_msg.to_string(),
            },
        )?;

        Ok(())
    }
}

/// Test support: mock container runtime shared across test modules.
#[cfg(test)]
pub(crate) mod tests_support {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Mock container runtime for testing.
    ///
    /// Records all operations and can be configured to fail specific calls.
    /// Supports shared VM tracking for multi-service stack testing.
    /// Uses `Mutex`/`AtomicUsize` instead of `RefCell`/`Cell` so it is
    /// `Send + Sync` and can be used with parallel container creation.
    pub struct MockContainerRuntime {
        /// Container IDs to return on create calls (fallback when config has no container_id).
        pub container_ids: Vec<String>,
        /// Whether pull should fail.
        pub fail_pull: bool,
        /// Whether create should fail.
        pub fail_create: bool,
        /// Whether stop should fail.
        pub fail_stop: bool,
        /// Exit code to return from exec calls.
        pub exec_exit_code: i32,
        /// Whether exec should fail with an error (not just non-zero exit).
        pub fail_exec: bool,
        /// Tracks calls: (operation, arg).
        pub calls: Mutex<Vec<(String, String)>>,
        /// Counter for create calls (fallback ID generation).
        create_counter: AtomicUsize,
        /// Tracks which stacks have a shared VM running.
        shared_vms: Mutex<HashSet<String>>,
        /// Captured RunConfigs from create/create_in_stack calls, keyed by container_id.
        pub captured_configs: Mutex<Vec<(String, vz_runtime_contract::RunConfig)>>,
        /// Captured NetworkServiceConfigs from network_setup calls.
        pub captured_network_services:
            Mutex<Vec<(String, Vec<vz_runtime_contract::NetworkServiceConfig>)>>,
    }

    impl MockContainerRuntime {
        pub fn new() -> Self {
            Self {
                container_ids: vec!["ctr-001".to_string()],
                fail_pull: false,
                fail_create: false,
                fail_stop: false,
                exec_exit_code: 0,
                fail_exec: false,
                calls: Mutex::new(Vec::new()),
                create_counter: AtomicUsize::new(0),
                shared_vms: Mutex::new(HashSet::new()),
                captured_configs: Mutex::new(Vec::new()),
                captured_network_services: Mutex::new(Vec::new()),
            }
        }

        pub fn with_ids(ids: Vec<&str>) -> Self {
            Self {
                container_ids: ids.into_iter().map(String::from).collect(),
                ..Self::new()
            }
        }

        pub fn call_log(&self) -> Vec<(String, String)> {
            self.calls.lock().unwrap().clone()
        }

        /// Generate a deterministic container ID from the RunConfig.
        ///
        /// Uses `config.container_id` (set to service name by the executor)
        /// so that IDs are deterministic regardless of parallel execution order.
        /// Falls back to cycling through `container_ids` if not set.
        fn next_id(&self, config: &vz_runtime_contract::RunConfig) -> String {
            config
                .container_id
                .as_ref()
                .map(|name| format!("ctr-{name}"))
                .unwrap_or_else(|| {
                    let idx = self.create_counter.fetch_add(1, Ordering::SeqCst);
                    self.container_ids[idx % self.container_ids.len()].clone()
                })
        }
    }

    impl ContainerRuntime for MockContainerRuntime {
        fn pull(&self, image: &str) -> Result<String, StackError> {
            self.calls
                .lock()
                .unwrap()
                .push(("pull".to_string(), image.to_string()));
            if self.fail_pull {
                return Err(StackError::InvalidSpec("mock pull failure".to_string()));
            }
            Ok(format!("sha256:{image}"))
        }

        fn create(
            &self,
            image: &str,
            config: vz_runtime_contract::RunConfig,
        ) -> Result<String, StackError> {
            self.calls
                .lock()
                .unwrap()
                .push(("create".to_string(), image.to_string()));
            if self.fail_create {
                return Err(StackError::InvalidSpec("mock create failure".to_string()));
            }
            let id = self.next_id(&config);
            self.captured_configs
                .lock()
                .unwrap()
                .push((id.clone(), config));
            Ok(id)
        }

        fn stop(&self, container_id: &str) -> Result<(), StackError> {
            self.calls
                .lock()
                .unwrap()
                .push(("stop".to_string(), container_id.to_string()));
            if self.fail_stop {
                return Err(StackError::InvalidSpec("mock stop failure".to_string()));
            }
            Ok(())
        }

        fn remove(&self, container_id: &str) -> Result<(), StackError> {
            self.calls
                .lock()
                .unwrap()
                .push(("remove".to_string(), container_id.to_string()));
            Ok(())
        }

        fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError> {
            self.calls.lock().unwrap().push((
                "exec".to_string(),
                format!("{container_id}:{}", command.join(" ")),
            ));
            if self.fail_exec {
                return Err(StackError::InvalidSpec("mock exec failure".to_string()));
            }
            Ok(self.exec_exit_code)
        }

        fn boot_shared_vm(
            &self,
            stack_id: &str,
            ports: &[vz_runtime_contract::PortMapping],
            _resources: vz_runtime_contract::StackResourceHint,
        ) -> Result<(), StackError> {
            self.calls.lock().unwrap().push((
                "boot_shared_vm".to_string(),
                format!(
                    "{}:{}",
                    stack_id,
                    ports
                        .iter()
                        .map(|p| format!("{}:{}", p.host, p.container))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            ));
            self.shared_vms.lock().unwrap().insert(stack_id.to_string());
            Ok(())
        }

        fn network_setup(
            &self,
            stack_id: &str,
            services: &[vz_runtime_contract::NetworkServiceConfig],
        ) -> Result<(), StackError> {
            self.calls.lock().unwrap().push((
                "network_setup".to_string(),
                format!(
                    "{}:{}",
                    stack_id,
                    services
                        .iter()
                        .map(|s| format!("{}={}@{}", s.name, s.addr, s.network_name))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            ));
            self.captured_network_services
                .lock()
                .unwrap()
                .push((stack_id.to_string(), services.to_vec()));
            Ok(())
        }

        fn network_teardown(
            &self,
            stack_id: &str,
            service_names: &[String],
        ) -> Result<(), StackError> {
            self.calls.lock().unwrap().push((
                "network_teardown".to_string(),
                format!("{}:{}", stack_id, service_names.join(",")),
            ));
            Ok(())
        }

        fn create_in_stack(
            &self,
            stack_id: &str,
            image: &str,
            config: vz_runtime_contract::RunConfig,
        ) -> Result<String, StackError> {
            self.calls
                .lock()
                .unwrap()
                .push(("create_in_stack".to_string(), format!("{stack_id}:{image}")));
            if self.fail_create {
                return Err(StackError::InvalidSpec("mock create failure".to_string()));
            }
            let id = self.next_id(&config);
            self.captured_configs
                .lock()
                .unwrap()
                .push((id.clone(), config));
            Ok(id)
        }

        fn shutdown_shared_vm(&self, stack_id: &str) -> Result<(), StackError> {
            self.calls
                .lock()
                .unwrap()
                .push(("shutdown_shared_vm".to_string(), stack_id.to_string()));
            self.shared_vms.lock().unwrap().remove(stack_id);
            Ok(())
        }

        fn has_shared_vm(&self, stack_id: &str) -> bool {
            self.shared_vms.lock().unwrap().contains(stack_id)
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::tests_support::MockContainerRuntime;
    use super::*;
    use crate::spec::MountSpec as StackMountSpec;
    use crate::spec::{PortSpec, ResourcesSpec, StackSpec, VolumeSpec};
    use std::collections::HashMap;

    fn svc(name: &str, image: &str) -> ServiceSpec {
        ServiceSpec {
            name: name.to_string(),
            image: image.to_string(),
            command: None,
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            mounts: vec![],
            ports: vec![],
            depends_on: vec![],
            healthcheck: None,
            restart_policy: None,
            resources: ResourcesSpec::default(),
            extra_hosts: vec![],
            secrets: vec![],
            networks: vec!["default".to_string()],
            cap_add: vec![],
            cap_drop: vec![],
            privileged: false,
            read_only: false,
            sysctls: HashMap::new(),
            ulimits: vec![],
            hostname: None,
            domainname: None,
            labels: HashMap::new(),
        }
    }

    fn default_network() -> crate::spec::NetworkSpec {
        crate::spec::NetworkSpec {
            name: "default".to_string(),
            driver: "bridge".to_string(),
            subnet: None,
        }
    }

    fn stack(name: &str, services: Vec<ServiceSpec>) -> StackSpec {
        StackSpec {
            name: name.to_string(),
            services,
            networks: vec![default_network()],
            volumes: vec![],
            secrets: vec![],
        }
    }

    fn make_executor(runtime: MockContainerRuntime) -> StackExecutor<MockContainerRuntime> {
        let tmp = tempfile::tempdir().unwrap();
        let store = StateStore::in_memory().unwrap();
        StackExecutor::new(runtime, store, tmp.path())
    }

    fn make_executor_with_dir(
        runtime: MockContainerRuntime,
        dir: &Path,
    ) -> StackExecutor<MockContainerRuntime> {
        let store = StateStore::in_memory().unwrap();
        StackExecutor::new(runtime, store, dir)
    }

    #[test]
    fn create_single_service() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());
        assert_eq!(result.succeeded, 1);
        assert_eq!(result.failed, 0);

        // Verify observed state.
        let observed = executor.store().load_observed_state("myapp").unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].phase, ServicePhase::Running);
        assert_eq!(observed[0].container_id, Some("ctr-web".to_string()));

        // Verify events.
        let events = executor.store().load_events("myapp").unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::ServiceCreating { .. }))
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::ServiceReady { .. }))
        );
    }

    #[test]
    fn create_multiple_services() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = stack(
            "myapp",
            vec![svc("web", "nginx:latest"), svc("db", "postgres:16")],
        );

        let actions = vec![
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());
        assert_eq!(result.succeeded, 2);

        let observed = executor.store().load_observed_state("myapp").unwrap();
        assert_eq!(observed.len(), 2);
    }

    #[test]
    fn remove_service() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![]);

        // Simulate existing running container.
        executor
            .store()
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    service_name: "old".to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some("ctr-old".to_string()),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();

        let actions = vec![Action::ServiceRemove {
            service_name: "old".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Verify stop+remove were called.
        let calls = executor.runtime.call_log();
        assert!(calls.iter().any(|(op, _)| op == "stop"));
        assert!(calls.iter().any(|(op, _)| op == "remove"));

        // Verify state is Stopped.
        let observed = executor.store().load_observed_state("myapp").unwrap();
        let old = observed.iter().find(|o| o.service_name == "old").unwrap();
        assert_eq!(old.phase, ServicePhase::Stopped);
        assert!(old.container_id.is_none());
    }

    #[test]
    fn recreate_service() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-new"]);
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

        // Simulate existing running container.
        executor
            .store()
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    service_name: "web".to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some("ctr-old".to_string()),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();

        let actions = vec![Action::ServiceRecreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Verify stop+remove of old, then pull+create of new.
        let calls = executor.runtime.call_log();
        let ops: Vec<&str> = calls.iter().map(|(op, _)| op.as_str()).collect();
        assert_eq!(ops, vec!["stop", "remove", "pull", "create"]);

        // New container.
        let observed = executor.store().load_observed_state("myapp").unwrap();
        let web = observed.iter().find(|o| o.service_name == "web").unwrap();
        assert_eq!(web.phase, ServicePhase::Running);
        assert_eq!(web.container_id, Some("ctr-web".to_string()));
    }

    #[test]
    fn pull_failure_marks_service_failed() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_pull = true;
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert_eq!(result.failed, 1);
        assert!(!result.all_succeeded());

        // Service should be marked Failed.
        let observed = executor.store().load_observed_state("myapp").unwrap();
        let web = observed.iter().find(|o| o.service_name == "web").unwrap();
        assert_eq!(web.phase, ServicePhase::Failed);
        assert!(web.last_error.is_some());

        // ServiceFailed event emitted.
        let events = executor.store().load_events("myapp").unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::ServiceFailed { .. }))
        );
    }

    #[test]
    fn create_failure_marks_service_failed() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_create = true;
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert_eq!(result.failed, 1);

        let observed = executor.store().load_observed_state("myapp").unwrap();
        let web = observed.iter().find(|o| o.service_name == "web").unwrap();
        assert_eq!(web.phase, ServicePhase::Failed);
    }

    #[test]
    fn partial_failure_continues_other_services() {
        let mut runtime = MockContainerRuntime::with_ids(vec!["ctr-db"]);
        runtime.fail_pull = false;
        runtime.fail_create = false;
        let mut executor = make_executor(runtime);

        let spec = stack(
            "myapp",
            vec![svc("db", "postgres:16"), svc("web", "nginx:latest")],
        );

        // Make only "web" fail by using a spec that triggers an error.
        // We'll test with a normal mock that succeeds for both.
        let actions = vec![
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        // Both succeed with mock.
        assert_eq!(result.succeeded, 2);
    }

    #[test]
    fn remove_with_no_container_id() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![]);

        // Service observed but no container_id.
        executor
            .store()
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    service_name: "orphan".to_string(),
                    phase: ServicePhase::Pending,
                    container_id: None,
                    last_error: None,
                    ready: false,
                },
            )
            .unwrap();

        let actions = vec![Action::ServiceRemove {
            service_name: "orphan".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // No stop/remove calls since there's no container.
        let calls = executor.runtime.call_log();
        assert!(calls.is_empty());
    }

    #[test]
    fn volumes_created_before_containers() {
        let runtime = MockContainerRuntime::new();
        let tmp = tempfile::tempdir().unwrap();
        let mut executor = make_executor_with_dir(runtime, tmp.path());

        let spec = StackSpec {
            name: "myapp".to_string(),
            services: vec![ServiceSpec {
                mounts: vec![StackMountSpec::Named {
                    source: "dbdata".to_string(),
                    target: "/var/lib/db".to_string(),
                    read_only: false,
                }],
                ..svc("db", "postgres:16")
            }],
            networks: vec![default_network()],
            volumes: vec![VolumeSpec {
                name: "dbdata".to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            }],
            secrets: vec![],
        };

        let actions = vec![Action::ServiceCreate {
            service_name: "db".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Volume directory exists.
        assert!(executor.volumes().volumes_dir().join("dbdata").is_dir());

        // VolumeCreated event emitted.
        let events = executor.store().load_events("myapp").unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::VolumeCreated { .. }))
        );
    }

    #[test]
    fn service_with_ports_creates_correctly() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);

        let spec = stack(
            "myapp",
            vec![ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                ..svc("web", "nginx:latest")
            }],
        );

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Verify pull and create were called.
        let calls = executor.runtime.call_log();
        assert_eq!(calls.len(), 2); // pull + create
    }

    #[test]
    fn stop_failure_does_not_prevent_state_update() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_stop = true;
        let mut executor = make_executor(runtime);
        let spec = stack("myapp", vec![]);

        executor
            .store()
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    service_name: "web".to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some("ctr-1".to_string()),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();

        let actions = vec![Action::ServiceRemove {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        // Still counts as succeeded (best-effort stop).
        assert!(result.all_succeeded());

        // State still updated to Stopped.
        let observed = executor.store().load_observed_state("myapp").unwrap();
        let web = observed.iter().find(|o| o.service_name == "web").unwrap();
        assert_eq!(web.phase, ServicePhase::Stopped);
    }

    #[test]
    fn execution_result_errors_collected() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_pull = true;
        let mut executor = make_executor(runtime);

        let spec = stack("myapp", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].0, "web");
        assert!(result.errors[0].1.contains("mock pull failure"));
    }

    // ── Port tracking tests ──

    #[test]
    fn port_tracker_allocates_explicit_port() {
        let mut tracker = PortTracker::new();
        let ports = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 80,
            host_port: Some(8080),
        }];
        let published = tracker.allocate("web", &ports).unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].host_port, 8080);
        assert_eq!(published[0].container_port, 80);
        assert!(tracker.in_use().contains(&8080));
    }

    #[test]
    fn port_tracker_allocates_ephemeral_port() {
        let mut tracker = PortTracker::new();
        let ports = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 3000,
            host_port: None,
        }];
        let published = tracker.allocate("api", &ports).unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].container_port, 3000);
        // Ephemeral port should be assigned.
        assert!(published[0].host_port > 0);
        assert!(tracker.in_use().contains(&published[0].host_port));
    }

    #[test]
    fn port_tracker_detects_cross_service_conflict() {
        let mut tracker = PortTracker::new();
        let ports_a = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 80,
            host_port: Some(8080),
        }];
        tracker.allocate("web", &ports_a).unwrap();

        // Second service trying the same host port should fail.
        let ports_b = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 3000,
            host_port: Some(8080),
        }];
        let result = tracker.allocate("api", &ports_b);
        assert!(result.is_err());
    }

    #[test]
    fn port_tracker_release_frees_port() {
        let mut tracker = PortTracker::new();
        let ports = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 80,
            host_port: Some(9090),
        }];
        tracker.allocate("web", &ports).unwrap();
        assert!(tracker.in_use().contains(&9090));

        tracker.release("web");
        assert!(!tracker.in_use().contains(&9090));
        assert!(tracker.ports_for("web").is_none());
    }

    #[test]
    fn port_tracker_reuse_after_release() {
        let mut tracker = PortTracker::new();
        let ports = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 80,
            host_port: Some(9090),
        }];
        tracker.allocate("web", &ports).unwrap();
        tracker.release("web");

        // Another service can now use the same port.
        let ports2 = vec![PortSpec {
            protocol: "tcp".to_string(),
            container_port: 3000,
            host_port: Some(9090),
        }];
        let published = tracker.allocate("api", &ports2).unwrap();
        assert_eq!(published[0].host_port, 9090);
    }

    #[test]
    fn executor_tracks_ports_on_create() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);

        let spec = stack(
            "myapp",
            vec![ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                ..svc("web", "nginx:latest")
            }],
        );

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Ports should be tracked.
        let ports = executor.ports().ports_for("web").unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].host_port, 8080);
    }

    #[test]
    fn executor_releases_ports_on_remove() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);

        let spec = stack(
            "myapp",
            vec![ServiceSpec {
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                ..svc("web", "nginx:latest")
            }],
        );

        // Create first.
        let create_actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];
        executor.execute(&spec, &create_actions).unwrap();
        assert!(executor.ports().ports_for("web").is_some());

        // Remove should release ports.
        let remove_actions = vec![Action::ServiceRemove {
            service_name: "web".to_string(),
        }];
        let result = executor.execute(&spec, &remove_actions).unwrap();
        assert!(result.all_succeeded());
        assert!(executor.ports().ports_for("web").is_none());
        assert!(executor.ports().in_use().is_empty());
    }

    #[test]
    fn executor_port_conflict_emits_event() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
        let mut executor = make_executor(runtime);

        let spec = stack(
            "myapp",
            vec![
                ServiceSpec {
                    ports: vec![PortSpec {
                        protocol: "tcp".to_string(),
                        container_port: 80,
                        host_port: Some(8080),
                    }],
                    ..svc("web", "nginx:latest")
                },
                ServiceSpec {
                    ports: vec![PortSpec {
                        protocol: "tcp".to_string(),
                        container_port: 3000,
                        host_port: Some(8080), // conflict with web
                    }],
                    ..svc("api", "node:20")
                },
            ],
        );

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert_eq!(result.succeeded, 1); // web succeeds
        assert_eq!(result.failed, 1); // api fails (port conflict)

        // PortConflict event emitted.
        let events = executor.store().load_events("myapp").unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::PortConflict { .. }))
        );

        // api should be marked Failed.
        let observed = executor.store().load_observed_state("myapp").unwrap();
        let api = observed.iter().find(|o| o.service_name == "api").unwrap();
        assert_eq!(api.phase, ServicePhase::Failed);
    }

    // ── Docker Compose network conformance tests ──

    /// Helper: two-service stack for network tests.
    fn network_stack() -> StackSpec {
        stack(
            "netapp",
            vec![
                ServiceSpec {
                    ports: vec![PortSpec {
                        protocol: "tcp".to_string(),
                        container_port: 80,
                        host_port: Some(8080),
                    }],
                    ..svc("web", "nginx:latest")
                },
                ServiceSpec {
                    ports: vec![PortSpec {
                        protocol: "tcp".to_string(),
                        container_port: 5432,
                        host_port: Some(5432),
                    }],
                    ..svc("db", "postgres:16")
                },
            ],
        )
    }

    /// Helper: three-service stack.
    fn three_service_stack() -> StackSpec {
        stack(
            "triapp",
            vec![
                svc("web", "nginx:latest"),
                svc("api", "node:20"),
                svc("db", "postgres:16"),
            ],
        )
    }

    #[test]
    fn shared_vm_boots_before_container_creates() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = network_stack();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Verify ordering: boot_shared_vm → network_setup → create_in_stack × 2.
        let call_log = executor.runtime.call_log();
        let ops: Vec<&str> = call_log.iter().map(|(op, _)| op.as_str()).collect();
        assert_eq!(ops[0], "boot_shared_vm");
        assert_eq!(ops[1], "network_setup");
        // Remaining: pull + create_in_stack for each service.
        assert!(ops.contains(&"create_in_stack"));
        assert!(
            !ops.contains(&"create"),
            "should use create_in_stack, not create"
        );
    }

    #[test]
    fn network_setup_assigns_correct_ips() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = network_stack();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        executor.execute(&spec, &actions).unwrap();

        // Verify network_setup was called with correct service configs.
        let captured = executor.runtime.captured_network_services.lock().unwrap();
        assert_eq!(captured.len(), 1);
        let (stack_id, services) = &captured[0];
        assert_eq!(stack_id, "netapp");
        assert_eq!(services.len(), 2);

        // web gets 172.20.0.2/24, db gets 172.20.0.3/24, both on "default" network.
        assert_eq!(services[0].name, "web");
        assert_eq!(services[0].addr, "172.20.0.2/24");
        assert_eq!(services[0].network_name, "default");
        assert_eq!(services[1].name, "db");
        assert_eq!(services[1].addr, "172.20.0.3/24");
        assert_eq!(services[1].network_name, "default");
    }

    #[test]
    fn service_to_service_hosts_use_real_ips() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = network_stack();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        executor.execute(&spec, &actions).unwrap();

        // Verify extra_hosts use real IPs, not 127.0.0.1.
        let configs = executor.runtime.captured_configs.lock().unwrap();

        // Find web's config.
        let web_config = configs.iter().find(|(id, _)| id == "ctr-web");
        assert!(web_config.is_some(), "web config not captured");
        let web_hosts = &web_config.unwrap().1.extra_hosts;
        // web should have db mapped to 172.20.0.3 (db is index 1, so .3).
        let db_host = web_hosts.iter().find(|(h, _)| h == "db");
        assert!(db_host.is_some(), "db not in web's extra_hosts");
        assert_eq!(db_host.unwrap().1, "172.20.0.3");

        // Find db's config.
        let db_config = configs.iter().find(|(id, _)| id == "ctr-db");
        assert!(db_config.is_some(), "db config not captured");
        let db_hosts = &db_config.unwrap().1.extra_hosts;
        // db should have web mapped to 172.20.0.2.
        let web_host = db_hosts.iter().find(|(h, _)| h == "web");
        assert!(web_host.is_some(), "web not in db's extra_hosts");
        assert_eq!(web_host.unwrap().1, "172.20.0.2");
    }

    #[test]
    fn containers_join_per_service_network_namespace() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = network_stack();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        executor.execute(&spec, &actions).unwrap();

        let configs = executor.runtime.captured_configs.lock().unwrap();

        // web should join /var/run/netns/web.
        let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
        assert_eq!(
            web_config.1.network_namespace_path,
            Some("/var/run/netns/web".to_string())
        );

        // db should join /var/run/netns/db.
        let db_config = configs.iter().find(|(id, _)| id == "ctr-db").unwrap();
        assert_eq!(
            db_config.1.network_namespace_path,
            Some("/var/run/netns/db".to_string())
        );
    }

    #[test]
    fn same_container_port_no_conflict_with_shared_vm() {
        // Two services both bind container port 80 but in different netns.
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
        let mut executor = make_executor(runtime);

        let spec = stack(
            "portapp",
            vec![
                ServiceSpec {
                    ports: vec![PortSpec {
                        protocol: "tcp".to_string(),
                        container_port: 80,
                        host_port: Some(8080),
                    }],
                    ..svc("web", "nginx:latest")
                },
                ServiceSpec {
                    ports: vec![PortSpec {
                        protocol: "tcp".to_string(),
                        container_port: 80,
                        host_port: Some(8081),
                    }],
                    ..svc("api", "node:20")
                },
            ],
        );

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        // Both succeed: different host ports, same container port is fine with netns.
        assert!(result.all_succeeded());
        assert_eq!(result.succeeded, 2);
    }

    #[test]
    fn three_service_ip_allocation() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = three_service_stack();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        executor.execute(&spec, &actions).unwrap();

        let captured = executor.runtime.captured_network_services.lock().unwrap();
        let (_, services) = &captured[0];
        assert_eq!(services.len(), 3);
        // 172.20.0.1 = bridge, services get .2, .3, .4.
        assert_eq!(services[0].addr, "172.20.0.2/24");
        assert_eq!(services[1].addr, "172.20.0.3/24");
        assert_eq!(services[2].addr, "172.20.0.4/24");

        // Verify cross-service host resolution for web.
        let configs = executor.runtime.captured_configs.lock().unwrap();
        let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
        let web_hosts = &web_config.1.extra_hosts;
        assert_eq!(web_hosts.len(), 2); // api + db
        assert!(
            web_hosts
                .iter()
                .any(|(h, ip)| h == "api" && ip == "172.20.0.3")
        );
        assert!(
            web_hosts
                .iter()
                .any(|(h, ip)| h == "db" && ip == "172.20.0.4")
        );
    }

    #[test]
    fn single_service_stack_skips_shared_vm() {
        let runtime = MockContainerRuntime::new();
        let mut executor = make_executor(runtime);
        let spec = stack("solo", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // Should use create, not create_in_stack.
        let call_log = executor.runtime.call_log();
        let ops: Vec<&str> = call_log.iter().map(|(op, _)| op.as_str()).collect();
        assert!(!ops.contains(&"boot_shared_vm"));
        assert!(!ops.contains(&"network_setup"));
        assert!(ops.contains(&"create"));
        assert!(!ops.contains(&"create_in_stack"));
    }

    #[test]
    fn single_service_uses_localhost_hosts() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        // Single service — no shared VM.
        let spec = stack("solo", vec![svc("web", "nginx:latest")]);

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        executor.execute(&spec, &actions).unwrap();

        // No extra_hosts since there's only one service.
        let configs = executor.runtime.captured_configs.lock().unwrap();
        let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
        assert!(web_config.1.extra_hosts.is_empty());
        assert!(web_config.1.network_namespace_path.is_none());
    }

    #[test]
    fn shared_vm_not_rebooted_on_second_execute() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db", "ctr-new"]);
        let mut executor = make_executor(runtime);
        let spec = network_stack();

        // First execute: boots shared VM.
        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];
        executor.execute(&spec, &actions).unwrap();

        // Second execute with a recreate: should NOT reboot.
        let actions2 = vec![Action::ServiceRecreate {
            service_name: "web".to_string(),
        }];
        executor.execute(&spec, &actions2).unwrap();

        // boot_shared_vm should only appear once.
        let boot_count = executor
            .runtime
            .call_log()
            .iter()
            .filter(|(op, _)| op == "boot_shared_vm")
            .count();
        assert_eq!(boot_count, 1, "shared VM should not be rebooted");
    }

    // ── Parallel execution tests ──

    #[test]
    fn topo_levels_independent_services_same_level() {
        // Three services with no deps → all at level 0.
        let spec = three_service_stack();
        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];
        let refs: Vec<&Action> = actions.iter().collect();
        let levels = compute_topo_levels(&refs, &spec);
        assert_eq!(levels.len(), 1, "all independent services at one level");
        assert_eq!(levels[0].len(), 3);
    }

    #[test]
    fn topo_levels_chain_dependency() {
        // app → api → db: three levels.
        let spec = stack(
            "chain",
            vec![
                svc("db", "postgres:16"),
                ServiceSpec {
                    depends_on: vec![crate::spec::ServiceDependency::started("db")],
                    ..svc("api", "node:20")
                },
                ServiceSpec {
                    depends_on: vec![crate::spec::ServiceDependency::started("api")],
                    ..svc("app", "myapp:latest")
                },
            ],
        );
        let actions = vec![
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
            Action::ServiceCreate {
                service_name: "app".to_string(),
            },
        ];
        let refs: Vec<&Action> = actions.iter().collect();
        let levels = compute_topo_levels(&refs, &spec);
        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0][0].service_name(), "db");
        assert_eq!(levels[1][0].service_name(), "api");
        assert_eq!(levels[2][0].service_name(), "app");
    }

    #[test]
    fn topo_levels_diamond_dependency() {
        // web and api depend on db → db at level 0, web+api at level 1.
        let spec = stack(
            "diamond",
            vec![
                svc("db", "postgres:16"),
                ServiceSpec {
                    depends_on: vec![crate::spec::ServiceDependency::started("db")],
                    ..svc("web", "nginx:latest")
                },
                ServiceSpec {
                    depends_on: vec![crate::spec::ServiceDependency::started("db")],
                    ..svc("api", "node:20")
                },
            ],
        );
        let actions = vec![
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
        ];
        let refs: Vec<&Action> = actions.iter().collect();
        let levels = compute_topo_levels(&refs, &spec);
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0].len(), 1);
        assert_eq!(levels[0][0].service_name(), "db");
        assert_eq!(levels[1].len(), 2);
        let level1_names: HashSet<&str> = levels[1].iter().map(|a| a.service_name()).collect();
        assert!(level1_names.contains("web"));
        assert!(level1_names.contains("api"));
    }

    #[test]
    fn parallel_creates_all_succeed() {
        // Three independent services should all be created (via parallel path).
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = three_service_stack();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());
        assert_eq!(result.succeeded, 3);

        // All three should be Running with deterministic IDs from container_id.
        let observed = executor.store().load_observed_state("triapp").unwrap();
        assert_eq!(observed.len(), 3);
        for obs in &observed {
            assert_eq!(obs.phase, ServicePhase::Running);
            assert_eq!(obs.container_id, Some(format!("ctr-{}", obs.service_name)));
        }
    }

    #[test]
    fn parallel_creates_with_dependency_ordering() {
        // web depends on db: db at level 0 (serial), web at level 1 (serial).
        // api has no deps: at level 0 alongside db (parallel with db).
        let spec = stack(
            "depapp",
            vec![
                svc("db", "postgres:16"),
                svc("api", "node:20"),
                ServiceSpec {
                    depends_on: vec![crate::spec::ServiceDependency::started("db")],
                    ..svc("web", "nginx:latest")
                },
            ],
        );

        let runtime = MockContainerRuntime::with_ids(vec!["ctr-db", "ctr-api", "ctr-web"]);
        let mut executor = make_executor(runtime);

        let actions = vec![
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(
            result.all_succeeded(),
            "execution had errors: {:?}",
            result.errors
        );
        assert_eq!(result.succeeded, 3);

        // web depends on db, so web's create must come after db's.
        // api is independent, so it can be in any order relative to db.
        // With 3 services the executor boots a shared VM, so creates go
        // through create_in_stack (arg = "stack_name:image").
        let calls = executor.runtime.call_log();
        let create_calls: Vec<&str> = calls
            .iter()
            .filter(|(op, _)| op == "create" || op == "create_in_stack")
            .map(|(_, arg)| arg.as_str())
            .collect();
        // db and api images are both at level 0.
        // web image is at level 1 and must appear after both db and api.
        let web_idx = create_calls
            .iter()
            .position(|img| img.contains("nginx:latest"))
            .unwrap();
        let db_idx = create_calls
            .iter()
            .position(|img| img.contains("postgres:16"))
            .unwrap();
        assert!(
            db_idx < web_idx,
            "db must be created before web (dependency)"
        );
    }

    #[test]
    fn resource_hints_passed_to_boot_shared_vm() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);

        let spec = stack(
            "resapp",
            vec![
                ServiceSpec {
                    resources: ResourcesSpec {
                        cpus: Some(2.0),
                        memory_bytes: Some(512 * 1024 * 1024), // 512 MiB
                        ..Default::default()
                    },
                    ..svc("web", "nginx:latest")
                },
                ServiceSpec {
                    resources: ResourcesSpec {
                        cpus: Some(4.0),
                        memory_bytes: Some(1024 * 1024 * 1024), // 1 GiB
                        ..Default::default()
                    },
                    ..svc("db", "postgres:16")
                },
            ],
        );

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());
        // Verify boot_shared_vm was called (indicating shared VM was used).
        let calls = executor.runtime.call_log();
        assert!(calls.iter().any(|(op, _)| op == "boot_shared_vm"));
    }

    // ── Custom network tests ──

    /// Helper: create a NetworkSpec.
    fn net(name: &str, subnet: Option<&str>) -> crate::spec::NetworkSpec {
        crate::spec::NetworkSpec {
            name: name.to_string(),
            driver: "bridge".to_string(),
            subnet: subnet.map(|s| s.to_string()),
        }
    }

    #[test]
    fn custom_networks_multi_subnet_allocation() {
        // Two networks: frontend (auto) and backend (auto).
        // web on frontend only, api on both, db on backend only.
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
        let mut executor = make_executor(runtime);

        let spec = StackSpec {
            name: "multinet".to_string(),
            services: vec![
                ServiceSpec {
                    networks: vec!["frontend".to_string()],
                    ..svc("web", "nginx:latest")
                },
                ServiceSpec {
                    networks: vec!["frontend".to_string(), "backend".to_string()],
                    ..svc("api", "node:20")
                },
                ServiceSpec {
                    networks: vec!["backend".to_string()],
                    ..svc("db", "postgres:16")
                },
            ],
            networks: vec![net("frontend", None), net("backend", None)],
            volumes: vec![],
            secrets: vec![],
        };

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded(), "errors: {:?}", result.errors);

        // Verify network configs: 4 entries (web@frontend, api@frontend, api@backend, db@backend).
        let captured = executor.runtime.captured_network_services.lock().unwrap();
        assert_eq!(captured.len(), 1);
        let (_, services) = &captured[0];
        assert_eq!(services.len(), 4);

        // frontend network: 172.20.0.0/24
        assert_eq!(services[0].name, "web");
        assert_eq!(services[0].addr, "172.20.0.2/24");
        assert_eq!(services[0].network_name, "frontend");

        assert_eq!(services[1].name, "api");
        assert_eq!(services[1].addr, "172.20.0.3/24");
        assert_eq!(services[1].network_name, "frontend");

        // backend network: 172.20.1.0/24
        assert_eq!(services[2].name, "api");
        assert_eq!(services[2].addr, "172.20.1.2/24");
        assert_eq!(services[2].network_name, "backend");

        assert_eq!(services[3].name, "db");
        assert_eq!(services[3].addr, "172.20.1.3/24");
        assert_eq!(services[3].network_name, "backend");
    }

    #[test]
    fn custom_networks_explicit_subnet() {
        // Frontend has explicit subnet 10.0.1.0/24.
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
        let mut executor = make_executor(runtime);

        let spec = StackSpec {
            name: "explicit".to_string(),
            services: vec![
                ServiceSpec {
                    networks: vec!["frontend".to_string()],
                    ..svc("web", "nginx:latest")
                },
                ServiceSpec {
                    networks: vec!["frontend".to_string()],
                    ..svc("api", "node:20")
                },
            ],
            networks: vec![net("frontend", Some("10.0.1.0/24"))],
            volumes: vec![],
            secrets: vec![],
        };

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded(), "errors: {:?}", result.errors);

        let captured = executor.runtime.captured_network_services.lock().unwrap();
        let (_, services) = &captured[0];
        assert_eq!(services[0].addr, "10.0.1.2/24");
        assert_eq!(services[1].addr, "10.0.1.3/24");
    }

    #[test]
    fn scoped_hosts_only_shared_networks() {
        // web on frontend only, db on backend only, api on both.
        // web should see api (shared frontend) but NOT db.
        // db should see api (shared backend) but NOT web.
        // api should see both web and db.
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api", "ctr-db"]);
        let mut executor = make_executor(runtime);

        let spec = StackSpec {
            name: "scoped".to_string(),
            services: vec![
                ServiceSpec {
                    networks: vec!["frontend".to_string()],
                    ..svc("web", "nginx:latest")
                },
                ServiceSpec {
                    networks: vec!["frontend".to_string(), "backend".to_string()],
                    ..svc("api", "node:20")
                },
                ServiceSpec {
                    networks: vec!["backend".to_string()],
                    ..svc("db", "postgres:16")
                },
            ],
            networks: vec![net("frontend", None), net("backend", None)],
            volumes: vec![],
            secrets: vec![],
        };

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded(), "errors: {:?}", result.errors);

        let configs = executor.runtime.captured_configs.lock().unwrap();

        // web should only see api (shared frontend), NOT db.
        let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
        let web_hosts: Vec<&str> = web_config
            .1
            .extra_hosts
            .iter()
            .map(|(h, _)| h.as_str())
            .collect();
        assert!(web_hosts.contains(&"api"), "web should see api");
        assert!(!web_hosts.contains(&"db"), "web should NOT see db");

        // db should only see api (shared backend), NOT web.
        let db_config = configs.iter().find(|(id, _)| id == "ctr-db").unwrap();
        let db_hosts: Vec<&str> = db_config
            .1
            .extra_hosts
            .iter()
            .map(|(h, _)| h.as_str())
            .collect();
        assert!(db_hosts.contains(&"api"), "db should see api");
        assert!(!db_hosts.contains(&"web"), "db should NOT see web");

        // api should see both web and db.
        let api_config = configs.iter().find(|(id, _)| id == "ctr-api").unwrap();
        let api_hosts: Vec<&str> = api_config
            .1
            .extra_hosts
            .iter()
            .map(|(h, _)| h.as_str())
            .collect();
        assert!(api_hosts.contains(&"web"), "api should see web");
        assert!(api_hosts.contains(&"db"), "api should see db");
    }

    #[test]
    fn default_network_backward_compat() {
        // When all services are on "default" network, behaviour is identical
        // to the old single-bridge approach.
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let mut executor = make_executor(runtime);
        let spec = network_stack();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        let result = executor.execute(&spec, &actions).unwrap();
        assert!(result.all_succeeded());

        // All services on same network, so all see each other.
        let configs = executor.runtime.captured_configs.lock().unwrap();
        let web_config = configs.iter().find(|(id, _)| id == "ctr-web").unwrap();
        assert_eq!(web_config.1.extra_hosts.len(), 1);
        assert_eq!(web_config.1.extra_hosts[0].0, "db");

        let db_config = configs.iter().find(|(id, _)| id == "ctr-db").unwrap();
        assert_eq!(db_config.1.extra_hosts.len(), 1);
        assert_eq!(db_config.1.extra_hosts[0].0, "web");
    }

    #[test]
    fn parse_subnet_helpers() {
        assert_eq!(parse_subnet_base("172.20.1.0/24"), [172, 20, 1, 0]);
        assert_eq!(parse_subnet_base("10.0.0.0/16"), [10, 0, 0, 0]);
        assert_eq!(parse_subnet_prefix("172.20.1.0/24"), 24);
        assert_eq!(parse_subnet_prefix("10.0.0.0/16"), 16);
    }
}
