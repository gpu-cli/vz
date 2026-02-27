use super::topo::topo_sort;
use super::*;

pub(super) fn service_config_digest(svc: &ServiceSpec) -> String {
    let mut hasher = DefaultHasher::new();

    svc.image.hash(&mut hasher);
    svc.command.hash(&mut hasher);
    svc.entrypoint.hash(&mut hasher);
    svc.working_dir.hash(&mut hasher);
    svc.user.hash(&mut hasher);
    svc.hostname.hash(&mut hasher);
    svc.privileged.hash(&mut hasher);
    svc.read_only.hash(&mut hasher);
    svc.container_name.hash(&mut hasher);

    // Sort env for determinism (HashMap iteration order is random).
    let mut env: Vec<(&String, &String)> = svc.environment.iter().collect();
    env.sort();
    for (k, v) in &env {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }

    // Sort sysctls for determinism.
    let mut sysctls: Vec<(&String, &String)> = svc.sysctls.iter().collect();
    sysctls.sort();
    for (k, v) in &sysctls {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }

    // Ports (order-sensitive).
    for p in &svc.ports {
        p.container_port.hash(&mut hasher);
        p.host_port.hash(&mut hasher);
        p.protocol.hash(&mut hasher);
    }

    // Capabilities.
    let mut cap_add = svc.cap_add.clone();
    cap_add.sort();
    cap_add.hash(&mut hasher);
    let mut cap_drop = svc.cap_drop.clone();
    cap_drop.sort();
    cap_drop.hash(&mut hasher);

    // Mount topology (reuse existing digest for consistency).
    volume::mount_plan_digest(&svc.mounts).hash(&mut hasher);

    format!("{:016x}", hasher.finish())
}
/// Compute all expected replica container names for a service.
///
/// Replica 1 uses the base name (container_name or service name).
/// Replicas 2+ use `{base}-{N}`. Returns exactly `replicas` entries.
fn replica_names(svc: &ServiceSpec) -> Vec<String> {
    let base = svc.container_name.as_deref().unwrap_or(&svc.name);
    let count = svc.resources.replicas.max(1);
    (1..=count)
        .map(|i| {
            if i == 1 {
                base.to_string()
            } else {
                format!("{base}-{i}")
            }
        })
        .collect()
}

/// Compute a deterministic, dependency-ordered action plan.
///
/// Compares desired services against observed state and generates actions:
/// - `ServiceCreate` for services in desired but not observed
/// - `ServiceRecreate` for services whose image changed
/// - `ServiceRemove` for services in observed but not desired
///
/// Services whose dependencies are not ready are deferred.
/// Actions are topologically sorted by `depends_on` with name-based
/// tie-breaking for deterministic ordering.
///
/// `previous_services` provides dependency info from the previous desired
/// spec, used to order removals correctly during teardown. When the
/// current `desired_services` is empty (full teardown), the dep graph
/// would otherwise be empty and removals would happen in alphabetical
/// order instead of reverse-dependency order.
#[cfg(test)]
pub(super) fn compute_actions(
    desired_services: &[ServiceSpec],
    observed: &[ServiceObservedState],
    health_statuses: &HashMap<String, HealthStatus>,
    previous_services: Option<&[ServiceSpec]>,
) -> (Vec<Action>, Vec<DeferredService>) {
    let observed_mount_digests = HashMap::new();
    compute_actions_with_mount_digests(
        desired_services,
        observed,
        health_statuses,
        previous_services,
        &observed_mount_digests,
    )
}

pub(super) fn compute_actions_with_mount_digests(
    desired_services: &[ServiceSpec],
    observed: &[ServiceObservedState],
    health_statuses: &HashMap<String, HealthStatus>,
    previous_services: Option<&[ServiceSpec]>,
    observed_mount_digests: &HashMap<String, String>,
) -> (Vec<Action>, Vec<DeferredService>) {
    let observed_map: HashMap<&str, &ServiceObservedState> = observed
        .iter()
        .map(|o| (o.service_name.as_str(), o))
        .collect();
    let previous_service_map: HashMap<&str, &ServiceSpec> = previous_services
        .unwrap_or(&[])
        .iter()
        .map(|svc| (svc.name.as_str(), svc))
        .collect();

    // Build the full set of expected replica names across all desired services.
    // This is used for removal filtering so that replica-qualified names
    // (e.g., "web-2", "web-3") are not mistakenly removed.
    let all_desired_replica_names: HashSet<String> =
        desired_services.iter().flat_map(replica_names).collect();

    let mut actions = Vec::new();
    let mut deferred = Vec::new();

    // Services to create or recreate.
    for svc in desired_services {
        let expected_replicas = replica_names(svc);

        // Check full config digest (image, env, command, ports, mounts, etc.).
        let desired_digest = service_config_digest(svc);
        let previous_digest = observed_mount_digests
            .get(svc.name.as_str())
            .cloned()
            .or_else(|| {
                previous_service_map
                    .get(svc.name.as_str())
                    .map(|previous| service_config_digest(previous))
            });
        let config_changed = previous_digest
            .as_ref()
            .is_some_and(|previous| previous != &desired_digest);

        // Recreate if config changed and the base replica is Running.
        let needs_recreate = config_changed
            && observed_map
                .get(svc.name.as_str())
                .is_some_and(|obs| matches!(obs.phase, ServicePhase::Running));

        // Create if any expected replica is missing, pending, failed, or stopped.
        let needs_create =
            expected_replicas
                .iter()
                .any(|name| match observed_map.get(name.as_str()) {
                    None => true,
                    Some(obs) => matches!(
                        obs.phase,
                        ServicePhase::Pending | ServicePhase::Failed | ServicePhase::Stopped
                    ),
                });

        if needs_recreate || needs_create {
            // Check dependency readiness before allowing creation.
            match check_dependencies(svc, observed, desired_services, health_statuses) {
                DependencyCheck::Ready => {
                    if needs_recreate {
                        actions.push(Action::ServiceRecreate {
                            service_name: svc.name.clone(),
                        });
                    } else {
                        actions.push(Action::ServiceCreate {
                            service_name: svc.name.clone(),
                        });
                    }
                }
                DependencyCheck::Blocked { waiting_on } => {
                    deferred.push(DeferredService {
                        service_name: svc.name.clone(),
                        waiting_on,
                    });
                }
            }
        }

        // Scale-down: remove excess replicas beyond the desired count.
        // Check observed for replica names that exceed current replica count.
        let desired_set: HashSet<&str> = expected_replicas.iter().map(|s| s.as_str()).collect();
        let base = svc.container_name.as_deref().unwrap_or(&svc.name);
        for o in observed {
            // Match observed entries that belong to this service but are excess.
            // A replica belongs to this service if it equals the base name or
            // matches the pattern "{base}-{N}" where N > 1.
            let belongs = o.service_name == base
                || o.service_name.strip_prefix(base).is_some_and(|suffix| {
                    suffix
                        .strip_prefix('-')
                        .and_then(|n| n.parse::<u32>().ok())
                        .is_some_and(|n| n > 1)
                });
            if belongs && !desired_set.contains(o.service_name.as_str()) {
                actions.push(Action::ServiceRemove {
                    service_name: o.service_name.clone(),
                });
            }
        }
    }

    // Remove observed entries that aren't in any desired service's replica set.
    let mut removals: Vec<String> = observed
        .iter()
        .filter(|o| !all_desired_replica_names.contains(&o.service_name))
        .filter(|o| {
            // Don't double-add scale-down removals already handled above.
            !actions.iter().any(|a| {
                matches!(a, Action::ServiceRemove { service_name } if service_name == &o.service_name)
            })
        })
        .map(|o| o.service_name.clone())
        .collect();
    removals.sort();

    for name in removals {
        actions.push(Action::ServiceRemove { service_name: name });
    }

    // Build dependency graph for ordering.
    // Include deps from both current desired services and previous desired
    // services (if available). This ensures removals during teardown are
    // ordered correctly even when desired_services is empty.
    let mut dep_names: HashMap<&str, Vec<String>> = desired_services
        .iter()
        .map(|s| {
            let names: Vec<String> = s.depends_on.iter().map(|d| d.service.clone()).collect();
            (s.name.as_str(), names)
        })
        .collect();
    if let Some(prev_services) = previous_services {
        for svc in prev_services {
            dep_names
                .entry(svc.name.as_str())
                .or_insert_with(|| svc.depends_on.iter().map(|d| d.service.clone()).collect());
        }
    }
    let dep_map: HashMap<&str, &[String]> =
        dep_names.iter().map(|(k, v)| (*k, v.as_slice())).collect();

    (topo_sort(&actions, &dep_map), deferred)
}
