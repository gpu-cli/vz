use super::topo::topo_sort;
use super::*;

/// Compute the versioned digest used to compare a desired service definition
/// with the configuration applied to one running replica.
pub fn service_config_digest(svc: &ServiceSpec) -> String {
    fn write_canonical(value: &serde_json::Value, output: &mut Vec<u8>) {
        match value {
            serde_json::Value::Array(values) => {
                output.push(b'[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(b',');
                    }
                    write_canonical(value, output);
                }
                output.push(b']');
            }
            serde_json::Value::Object(values) => {
                output.push(b'{');
                let mut keys = values.keys().collect::<Vec<_>>();
                keys.sort_unstable();
                for (index, key) in keys.into_iter().enumerate() {
                    if index > 0 {
                        output.push(b',');
                    }
                    serde_json::to_writer(&mut *output, key)
                        .unwrap_or_else(|error| unreachable!("JSON key serialization: {error}"));
                    output.push(b':');
                    write_canonical(&values[key], output);
                }
                output.push(b'}');
            }
            primitive => serde_json::to_writer(output, primitive)
                .unwrap_or_else(|error| unreachable!("JSON value serialization: {error}")),
        }
    }

    let mut projection = svc.clone();
    projection.resources.replicas = 1;
    let value = serde_json::to_value(projection)
        .unwrap_or_else(|error| unreachable!("normalized service serialization: {error}"));
    let mut canonical = Vec::new();
    write_canonical(&value, &mut canonical);
    let mut hasher = Sha256::new();
    hasher.update(b"vz-service-config-v1\0");
    hasher.update((canonical.len() as u64).to_be_bytes());
    hasher.update(canonical);
    format!("vzsc1-sha256:{:x}", hasher.finalize())
}
/// Compute all expected observed-state names for a service's replicas.
///
/// These are logical service identities, not runtime container IDs. An explicit
/// `container_name` must therefore never change reconciliation keys.
fn replica_keys(svc: &ServiceSpec) -> Vec<ServiceReplicaKey> {
    let count = svc.resources.replicas.max(1);
    (1..=count)
        .map(|index| {
            ServiceReplicaKey::new(svc.name.clone(), index).unwrap_or_else(|error| {
                unreachable!("validated service spec yielded an invalid replica key: {error}")
            })
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
#[expect(clippy::expect_used, reason = "test-only planning entry point")]
pub(super) fn compute_actions(
    desired_services: &[ServiceSpec],
    observed: &[ServiceObservedState],
    health_statuses: &HashMap<String, HealthStatus>,
    previous_services: Option<&[ServiceSpec]>,
) -> (Vec<Action>, Vec<DeferredService>) {
    let observed_mount_digests = HashMap::new();
    let (drafts, deferred) = compute_actions_with_mount_digests(
        desired_services,
        observed,
        health_statuses,
        previous_services,
        &observed_mount_digests,
    )
    .expect("test fixture dependency graph must be acyclic");
    let actions = drafts
        .into_iter()
        .map(|draft| draft.into_action(test_replica_precondition()))
        .collect();
    (actions, deferred)
}

pub(super) fn compute_actions_with_mount_digests(
    desired_services: &[ServiceSpec],
    observed: &[ServiceObservedState],
    health_statuses: &HashMap<String, HealthStatus>,
    previous_services: Option<&[ServiceSpec]>,
    _observed_mount_digests: &HashMap<String, String>,
) -> Result<(Vec<ActionDraft>, Vec<DeferredService>), StackError> {
    validate_desired_dependency_graph(desired_services)?;
    let observed_map: HashMap<&ServiceReplicaKey, &ServiceObservedState> =
        observed.iter().map(|o| (&o.replica, o)).collect();
    // Build the full set of expected replica names across all desired services.
    // This is used for removal filtering so that replica-qualified names
    // (e.g., "web-2", "web-3") are not mistakenly removed.
    let all_desired_replica_keys: HashSet<ServiceReplicaKey> =
        desired_services.iter().flat_map(replica_keys).collect();

    let mut actions = Vec::new();
    let mut deferred = Vec::new();

    // Services to create or recreate.
    for svc in desired_services {
        let expected_replicas = replica_keys(svc);
        let desired_digest = service_config_digest(svc);
        let replica_actions = expected_replicas
            .iter()
            .filter_map(|target| match observed_map.get(target) {
                None => Some(ActionDraft::Create {
                    target: target.clone(),
                    observed: None,
                }),
                Some(observed)
                    if matches!(
                        observed.phase,
                        ServicePhase::Pending | ServicePhase::Failed | ServicePhase::Stopped
                    ) =>
                {
                    Some(ActionDraft::Create {
                        target: target.clone(),
                        observed: Some((*observed).clone()),
                    })
                }
                Some(observed)
                    if observed.phase == ServicePhase::Running
                        && observed.applied_config_digest.as_deref()
                            != Some(desired_digest.as_str()) =>
                {
                    Some(ActionDraft::Recreate {
                        target: target.clone(),
                        observed: (*observed).clone(),
                    })
                }
                Some(_) => None,
            })
            .collect::<Vec<_>>();

        if !replica_actions.is_empty() {
            // Check dependency readiness before allowing creation.
            match check_dependencies(svc, observed, desired_services, health_statuses) {
                DependencyCheck::Ready => actions.extend(replica_actions),
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
        let desired_set: HashSet<&ServiceReplicaKey> = expected_replicas.iter().collect();
        for o in observed {
            let belongs = o.replica.service_name == svc.name;
            if belongs && !desired_set.contains(&o.replica) && o.phase != ServicePhase::Stopped {
                actions.push(ActionDraft::Remove {
                    target: o.replica.clone(),
                    observed: o.clone(),
                });
            }
        }
    }

    // Remove observed entries that aren't in any desired service's replica set.
    let mut removals: Vec<ServiceReplicaKey> = observed
        .iter()
        .filter(|o| !all_desired_replica_keys.contains(&o.replica))
        .filter(|o| o.phase != ServicePhase::Stopped)
        .filter(|o| {
            // Don't double-add scale-down removals already handled above.
            !actions
                .iter()
                .any(|action| matches!(action, ActionDraft::Remove { target, .. } if target == &o.replica))
        })
        .map(|o| o.replica.clone())
        .collect();
    removals.sort();

    for target in removals {
        let observed = observed_map.get(&target).ok_or_else(|| {
            StackError::InvalidSpec(format!(
                "planned removal `{}` has no observed-state snapshot",
                target.display_name()
            ))
        })?;
        actions.push(ActionDraft::Remove {
            target,
            observed: (**observed).clone(),
        });
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

    Ok((topo_sort(&actions, &dep_map)?, deferred))
}

fn validate_desired_dependency_graph(services: &[ServiceSpec]) -> Result<(), StackError> {
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for service in services {
        if in_degree.insert(service.name.as_str(), 0).is_some() {
            return Err(StackError::InvalidSpec(format!(
                "duplicate service `{}`",
                service.name
            )));
        }
        dependents.entry(service.name.as_str()).or_default();
    }
    for service in services {
        for dependency in &service.depends_on {
            if dependency.service == service.name {
                return Err(StackError::InvalidSpec(format!(
                    "service `{}` depends on itself",
                    service.name
                )));
            }
            if !in_degree.contains_key(dependency.service.as_str()) {
                return Err(StackError::InvalidSpec(format!(
                    "service `{}` depends on unknown service `{}`",
                    service.name, dependency.service
                )));
            }
            let degree = in_degree.get_mut(service.name.as_str()).ok_or_else(|| {
                StackError::InvalidSpec("dependency graph lost a service".to_string())
            })?;
            *degree += 1;
            dependents
                .entry(dependency.service.as_str())
                .or_default()
                .push(service.name.as_str());
        }
    }
    let mut ready = in_degree
        .iter()
        .filter_map(|(name, degree)| (*degree == 0).then_some(*name))
        .collect::<Vec<_>>();
    ready.sort_unstable();
    let mut visited = 0usize;
    while let Some(name) = ready.pop() {
        visited += 1;
        for dependent in dependents.get(name).into_iter().flatten() {
            let degree = in_degree.get_mut(dependent).ok_or_else(|| {
                StackError::InvalidSpec("dependency graph lost a dependent".to_string())
            })?;
            *degree -= 1;
            if *degree == 0 {
                ready.push(dependent);
                ready.sort_unstable_by(|left, right| right.cmp(left));
            }
        }
    }
    if visited != services.len() {
        let mut cyclic = in_degree
            .into_iter()
            .filter_map(|(name, degree)| (degree > 0).then_some(name))
            .collect::<Vec<_>>();
        cyclic.sort_unstable();
        return Err(StackError::InvalidSpec(format!(
            "dependency cycle among desired services: {}",
            cyclic.join(", ")
        )));
    }
    Ok(())
}
