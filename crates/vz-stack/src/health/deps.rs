use super::*;

pub fn is_service_ready(
    observed: &ServiceObservedState,
    healthcheck: Option<&HealthCheckSpec>,
    health_status: Option<&HealthStatus>,
) -> bool {
    // Must be Running to be ready.
    if observed.phase != ServicePhase::Running {
        return false;
    }

    // If the observed state already has `ready: true` and there's no
    // health check, that's sufficient.
    let Some(_spec) = healthcheck else {
        // No health check — running means ready.
        return true;
    };

    // Has a health check — need at least one consecutive pass.
    match health_status {
        Some(status) => status.consecutive_passes >= 1,
        None => false,
    }
}

/// Check if any dependency blocks creation of this service.
///
/// Dependency gating is strict: downstream services only start after
/// dependencies satisfy the declared readiness predicates.
///
/// A dependency blocks when:
/// - It has no observed state yet.
/// - The condition is `service_started` and the dependency is not `Running`.
/// - The condition is `service_healthy` and the dependency is not `Running`
///   with a passing health check.
/// - The condition is `service_completed_successfully` and the dependency
///   is not `Stopped` with no recorded error.
///
/// With the default `service_started` condition, a running service
/// is considered ready regardless of health check status — matching
/// Docker Compose semantics.
pub fn check_dependencies(
    service: &ServiceSpec,
    observed: &[ServiceObservedState],
    all_services: &[ServiceSpec],
    health_statuses: &HashMap<String, HealthStatus>,
) -> DependencyCheck {
    if service.depends_on.is_empty() {
        return DependencyCheck::Ready;
    }

    let observed_map: HashMap<&str, &ServiceObservedState> = observed
        .iter()
        .map(|o| (o.service_name.as_str(), o))
        .collect();

    let spec_map: HashMap<&str, &ServiceSpec> =
        all_services.iter().map(|s| (s.name.as_str(), s)).collect();

    let mut waiting_on = Vec::new();

    for dep in &service.depends_on {
        let dep_obs = observed_map.get(dep.service.as_str());
        let dep_spec = spec_map.get(dep.service.as_str());
        let dep_health = health_statuses.get(&dep.service);

        let blocked = match dep_obs {
            None => true,
            Some(obs) => match dep.condition {
                DependencyCondition::ServiceStarted => obs.phase != ServicePhase::Running,
                DependencyCondition::ServiceHealthy => {
                    if obs.phase != ServicePhase::Running {
                        true
                    } else {
                        let healthcheck = dep_spec.and_then(|s| s.healthcheck.as_ref());
                        match healthcheck {
                            None => false, // No health check defined = ready.
                            Some(hc) => !is_service_ready(obs, Some(hc), dep_health),
                        }
                    }
                }
                DependencyCondition::ServiceCompletedSuccessfully => {
                    !(obs.phase == ServicePhase::Stopped && obs.last_error.is_none())
                }
            },
        };

        if blocked {
            waiting_on.push(dep.service.clone());
        }
    }

    if waiting_on.is_empty() {
        DependencyCheck::Ready
    } else {
        waiting_on.sort();
        DependencyCheck::Blocked { waiting_on }
    }
}
