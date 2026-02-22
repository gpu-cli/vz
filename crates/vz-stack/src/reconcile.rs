//! Stack reconciliation: diff planner and ordered executor.
//!
//! The [`apply`] function compares desired [`StackSpec`] against
//! observed state, computes a deterministic action plan, and
//! persists all state transitions. Actions are ordered by service
//! dependency graph (topological sort with name-based tie-break).

use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::StackError;
use crate::events::StackEvent;
use crate::health::{DependencyCheck, HealthStatus, check_dependencies};
use crate::spec::{ServiceSpec, StackSpec};
use crate::state_store::{ServiceObservedState, ServicePhase, StateStore};

/// A reconciliation action to converge observed state toward desired state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Create and start a new service.
    ServiceCreate {
        /// Service name.
        service_name: String,
    },
    /// Recreate a service whose configuration changed.
    ServiceRecreate {
        /// Service name.
        service_name: String,
    },
    /// Remove a service that is no longer in the desired spec.
    ServiceRemove {
        /// Service name.
        service_name: String,
    },
}

impl Action {
    /// Service name this action targets.
    pub fn service_name(&self) -> &str {
        match self {
            Self::ServiceCreate { service_name }
            | Self::ServiceRecreate { service_name }
            | Self::ServiceRemove { service_name } => service_name,
        }
    }
}

/// Result of an [`apply`] call.
#[derive(Debug, Clone, Default)]
pub struct ApplyResult {
    /// Actions that were planned (and would be executed by a real runtime).
    pub actions: Vec<Action>,
    /// Services deferred because their dependencies are not ready.
    pub deferred: Vec<DeferredService>,
}

/// A service whose creation was deferred due to unready dependencies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredService {
    /// Service name that was deferred.
    pub service_name: String,
    /// Dependencies that are not yet ready.
    pub waiting_on: Vec<String>,
}

/// Persist desired state, compute action plan, and update observed state.
///
/// The reconciler:
/// 1. Persists the desired spec in the state store.
/// 2. Loads current observed state.
/// 3. Computes a deterministic, dependency-ordered action plan.
/// 4. Gates service creation on dependency readiness.
/// 5. Updates observed state for each action (create/remove).
/// 6. Emits lifecycle events for observability.
///
/// Services whose dependencies are not ready are deferred and
/// reported in [`ApplyResult::deferred`]. Re-applying the same
/// spec after dependencies become ready will create them.
pub fn apply(
    spec: &StackSpec,
    store: &StateStore,
    health_statuses: &HashMap<String, HealthStatus>,
) -> Result<ApplyResult, StackError> {
    // 1. Persist desired state.
    store.save_desired_state(&spec.name, spec)?;

    // 2. Emit start event.
    store.emit_event(
        &spec.name,
        &StackEvent::StackApplyStarted {
            stack_name: spec.name.clone(),
            services_count: spec.services.len(),
        },
    )?;

    // 3. Load current observed state.
    let observed = store.load_observed_state(&spec.name)?;

    // 4. Compute action plan with dependency gating.
    let (actions, deferred) = compute_actions(&spec.services, &observed, health_statuses);

    // 5. Emit events for deferred services.
    for d in &deferred {
        store.emit_event(
            &spec.name,
            &StackEvent::DependencyBlocked {
                stack_name: spec.name.clone(),
                service_name: d.service_name.clone(),
                waiting_on: d.waiting_on.clone(),
            },
        )?;
    }

    // 6. Execute action plan (update observed state).
    let mut succeeded = 0;
    let failed = 0;
    for action in &actions {
        match action {
            Action::ServiceCreate { service_name } => {
                store.save_observed_state(
                    &spec.name,
                    &ServiceObservedState {
                        service_name: service_name.clone(),
                        phase: ServicePhase::Pending,
                        container_id: None,
                        last_error: None,
                        ready: false,
                    },
                )?;
                store.emit_event(
                    &spec.name,
                    &StackEvent::ServiceCreating {
                        stack_name: spec.name.clone(),
                        service_name: service_name.clone(),
                    },
                )?;
                succeeded += 1;
            }
            Action::ServiceRecreate { service_name } => {
                store.save_observed_state(
                    &spec.name,
                    &ServiceObservedState {
                        service_name: service_name.clone(),
                        phase: ServicePhase::Pending,
                        container_id: None,
                        last_error: None,
                        ready: false,
                    },
                )?;
                store.emit_event(
                    &spec.name,
                    &StackEvent::ServiceCreating {
                        stack_name: spec.name.clone(),
                        service_name: service_name.clone(),
                    },
                )?;
                succeeded += 1;
            }
            Action::ServiceRemove { service_name } => {
                store.save_observed_state(
                    &spec.name,
                    &ServiceObservedState {
                        service_name: service_name.clone(),
                        phase: ServicePhase::Stopped,
                        container_id: None,
                        last_error: None,
                        ready: false,
                    },
                )?;
                store.emit_event(
                    &spec.name,
                    &StackEvent::ServiceStopped {
                        stack_name: spec.name.clone(),
                        service_name: service_name.clone(),
                        exit_code: 0,
                    },
                )?;
                succeeded += 1;
            }
        }
    }

    // 7. Emit completion event.
    store.emit_event(
        &spec.name,
        &StackEvent::StackApplyCompleted {
            stack_name: spec.name.clone(),
            succeeded,
            failed,
        },
    )?;

    Ok(ApplyResult { actions, deferred })
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
fn compute_actions(
    desired_services: &[ServiceSpec],
    observed: &[ServiceObservedState],
    health_statuses: &HashMap<String, HealthStatus>,
) -> (Vec<Action>, Vec<DeferredService>) {
    let observed_map: HashMap<&str, &ServiceObservedState> = observed
        .iter()
        .map(|o| (o.service_name.as_str(), o))
        .collect();

    let desired_names: HashSet<&str> = desired_services.iter().map(|s| s.name.as_str()).collect();

    let mut actions = Vec::new();
    let mut deferred = Vec::new();

    // Services to create or recreate.
    for svc in desired_services {
        let needs_create = match observed_map.get(svc.name.as_str()) {
            None => true,
            Some(obs) => {
                // If observed state is Pending/Failed/Stopped, treat as needing creation.
                matches!(
                    obs.phase,
                    ServicePhase::Pending | ServicePhase::Failed | ServicePhase::Stopped
                )
            }
        };

        if needs_create {
            // Check dependency readiness before allowing creation.
            match check_dependencies(svc, observed, desired_services, health_statuses) {
                DependencyCheck::Ready => {
                    actions.push(Action::ServiceCreate {
                        service_name: svc.name.clone(),
                    });
                }
                DependencyCheck::Blocked { waiting_on } => {
                    deferred.push(DeferredService {
                        service_name: svc.name.clone(),
                        waiting_on,
                    });
                }
            }
        }
    }

    // Services to remove (in observed but not in desired).
    let mut removals: Vec<String> = observed
        .iter()
        .filter(|o| !desired_names.contains(o.service_name.as_str()))
        .map(|o| o.service_name.clone())
        .collect();
    removals.sort();

    for name in removals {
        actions.push(Action::ServiceRemove { service_name: name });
    }

    // Build dependency graph for ordering.
    let dep_names: HashMap<&str, Vec<String>> = desired_services
        .iter()
        .map(|s| {
            let names: Vec<String> = s.depends_on.iter().map(|d| d.service.clone()).collect();
            (s.name.as_str(), names)
        })
        .collect();
    let dep_map: HashMap<&str, &[String]> =
        dep_names.iter().map(|(k, v)| (*k, v.as_slice())).collect();

    (topo_sort(&actions, &dep_map), deferred)
}

/// Topologically sort actions respecting depends_on relationships.
///
/// Create/Recreate actions for dependencies come before dependents.
/// Remove actions for dependents come before dependencies.
/// Ties within the same topological level are broken by service name.
fn topo_sort(actions: &[Action], deps: &HashMap<&str, &[String]>) -> Vec<Action> {
    // Partition into creates and removes.
    let mut creates: Vec<&Action> = Vec::new();
    let mut removes: Vec<&Action> = Vec::new();

    for action in actions {
        match action {
            Action::ServiceCreate { .. } | Action::ServiceRecreate { .. } => {
                creates.push(action);
            }
            Action::ServiceRemove { .. } => {
                removes.push(action);
            }
        }
    }

    // Topological sort for creates: dependencies first.
    let create_names: HashSet<&str> = creates.iter().map(|a| a.service_name()).collect();
    let sorted_creates = topo_sort_names(&creates, deps, &create_names, false);

    // Topological sort for removes: dependents first (reverse dependency order).
    let remove_names: HashSet<&str> = removes.iter().map(|a| a.service_name()).collect();
    let sorted_removes = topo_sort_names(&removes, deps, &remove_names, true);

    // Creates first, then removes.
    let action_map: HashMap<&str, &Action> =
        actions.iter().map(|a| (a.service_name(), a)).collect();

    let mut result = Vec::new();
    for name in sorted_creates {
        if let Some(action) = action_map.get(name.as_str()) {
            result.push((*action).clone());
        }
    }
    for name in sorted_removes {
        if let Some(action) = action_map.get(name.as_str()) {
            result.push((*action).clone());
        }
    }

    result
}

/// Kahn's algorithm for topological sort with name-based tie-breaking.
///
/// When `reverse` is true, returns dependents before dependencies
/// (useful for teardown ordering).
fn topo_sort_names(
    actions: &[&Action],
    deps: &HashMap<&str, &[String]>,
    action_set: &HashSet<&str>,
    reverse: bool,
) -> Vec<String> {
    let names: Vec<&str> = actions.iter().map(|a| a.service_name()).collect();

    // Build in-degree map considering only actions in our set.
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();

    for &name in &names {
        in_degree.entry(name).or_insert(0);
        adj.entry(name).or_default();
    }

    for &name in &names {
        let dependencies = deps.get(name).copied().unwrap_or_default();
        for dep in dependencies {
            if action_set.contains(dep.as_str()) {
                if reverse {
                    // For teardown: dependent → dependency (dependent goes first).
                    *in_degree.entry(dep.as_str()).or_insert(0) += 1;
                    adj.entry(name).or_default().push(dep.as_str());
                } else {
                    // For startup: dependency → dependent (dependency goes first).
                    *in_degree.entry(name).or_insert(0) += 1;
                    adj.entry(dep.as_str()).or_default().push(name);
                }
            }
        }
    }

    // Kahn's algorithm with sorted queue for deterministic tie-breaking.
    let mut queue: VecDeque<&str> = VecDeque::new();
    let mut ready: Vec<&str> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(name, _)| *name)
        .collect();
    ready.sort();
    queue.extend(ready);

    let mut result = Vec::new();
    while let Some(name) = queue.pop_front() {
        result.push(name.to_string());

        let neighbors: Vec<&str> = adj.get(name).cloned().unwrap_or_default();
        let mut newly_ready: Vec<&str> = Vec::new();

        for neighbor in neighbors {
            if let Some(deg) = in_degree.get_mut(neighbor) {
                *deg -= 1;
                if *deg == 0 {
                    newly_ready.push(neighbor);
                }
            }
        }

        newly_ready.sort();
        queue.extend(newly_ready);
    }

    result
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::spec::{ServiceDependency, StackSpec};

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
            resources: Default::default(),
            extra_hosts: vec![],
            secrets: vec![],
            networks: vec![],
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

    fn svc_with_deps(name: &str, image: &str, deps: Vec<&str>) -> ServiceSpec {
        ServiceSpec {
            depends_on: deps.into_iter().map(ServiceDependency::started).collect(),
            ..svc(name, image)
        }
    }

    fn svc_with_healthy_deps(name: &str, image: &str, deps: Vec<&str>) -> ServiceSpec {
        ServiceSpec {
            depends_on: deps.into_iter().map(ServiceDependency::healthy).collect(),
            ..svc(name, image)
        }
    }

    fn svc_with_healthcheck(name: &str, image: &str) -> ServiceSpec {
        use crate::spec::HealthCheckSpec;
        ServiceSpec {
            healthcheck: Some(HealthCheckSpec {
                test: vec!["CMD".to_string(), "true".to_string()],
                interval_secs: Some(5),
                timeout_secs: Some(3),
                retries: Some(3),
                start_period_secs: None,
            }),
            ..svc(name, image)
        }
    }

    fn spec(name: &str, services: Vec<ServiceSpec>) -> StackSpec {
        StackSpec {
            name: name.to_string(),
            services,
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
        }
    }

    fn no_health() -> HashMap<String, HealthStatus> {
        HashMap::new()
    }

    fn obs(name: &str, phase: ServicePhase) -> ServiceObservedState {
        ServiceObservedState {
            service_name: name.to_string(),
            phase,
            container_id: None,
            last_error: None,
            ready: false,
        }
    }

    fn obs_running(name: &str) -> ServiceObservedState {
        ServiceObservedState {
            service_name: name.to_string(),
            phase: ServicePhase::Running,
            container_id: Some(format!("ctr-{name}")),
            last_error: None,
            ready: true,
        }
    }

    // ── Diff planner tests ──

    #[test]
    fn compute_actions_creates_new_services() {
        let desired = vec![svc("web", "nginx:latest"), svc("db", "postgres:16")];
        let observed = vec![];

        let (actions, deferred) = compute_actions(&desired, &observed, &no_health());
        assert_eq!(actions.len(), 2);
        assert!(deferred.is_empty());
        assert!(
            actions
                .iter()
                .all(|a| matches!(a, Action::ServiceCreate { .. }))
        );
    }

    #[test]
    fn compute_actions_noop_when_converged() {
        let desired = vec![svc("web", "nginx:latest")];
        let observed = vec![obs_running("web")];

        let (actions, _) = compute_actions(&desired, &observed, &no_health());
        assert!(actions.is_empty());
    }

    #[test]
    fn compute_actions_removes_extra_services() {
        let desired = vec![svc("web", "nginx:latest")];
        let observed = vec![obs_running("web"), obs_running("old-svc")];

        let (actions, _) = compute_actions(&desired, &observed, &no_health());
        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            Action::ServiceRemove {
                service_name: "old-svc".to_string()
            }
        );
    }

    #[test]
    fn compute_actions_recreates_pending_services() {
        let desired = vec![svc("web", "nginx:latest")];
        let observed = vec![obs("web", ServicePhase::Pending)];

        let (actions, _) = compute_actions(&desired, &observed, &no_health());
        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            Action::ServiceCreate {
                service_name: "web".to_string()
            }
        );
    }

    #[test]
    fn compute_actions_recreates_failed_services() {
        let desired = vec![svc("web", "nginx:latest")];
        let observed = vec![ServiceObservedState {
            last_error: Some("oom".to_string()),
            ..obs("web", ServicePhase::Failed)
        }];

        let (actions, _) = compute_actions(&desired, &observed, &no_health());
        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            Action::ServiceCreate {
                service_name: "web".to_string()
            }
        );
    }

    #[test]
    fn compute_actions_recreates_stopped_services() {
        let desired = vec![svc("web", "nginx:latest")];
        let observed = vec![obs("web", ServicePhase::Stopped)];

        let (actions, _) = compute_actions(&desired, &observed, &no_health());
        assert_eq!(actions.len(), 1);
    }

    // ── Dependency ordering tests ──

    #[test]
    fn topo_sort_respects_depends_on() {
        let desired = vec![
            svc_with_deps("web", "nginx:latest", vec!["db"]),
            svc("db", "postgres:16"),
        ];
        let observed = vec![];

        let (actions, _) = compute_actions(&desired, &observed, &no_health());
        let names: Vec<&str> = actions.iter().map(|a| a.service_name()).collect();

        // db must come before web.
        let db_idx = names.iter().position(|&n| n == "db").unwrap();
        let web_idx = names.iter().position(|&n| n == "web").unwrap();
        assert!(db_idx < web_idx);
    }

    #[test]
    fn topo_sort_chain_dependency() {
        let desired = vec![
            svc_with_deps("app", "myapp:latest", vec!["api"]),
            svc_with_deps("api", "api:latest", vec!["db"]),
            svc("db", "postgres:16"),
        ];
        let observed = vec![];

        let (actions, _) = compute_actions(&desired, &observed, &no_health());
        let names: Vec<&str> = actions.iter().map(|a| a.service_name()).collect();

        // db → api → app
        let db_idx = names.iter().position(|&n| n == "db").unwrap();
        let api_idx = names.iter().position(|&n| n == "api").unwrap();
        let app_idx = names.iter().position(|&n| n == "app").unwrap();
        assert!(db_idx < api_idx);
        assert!(api_idx < app_idx);
    }

    #[test]
    fn topo_sort_name_tiebreak_is_deterministic() {
        // Three services with no dependencies — should sort by name.
        let desired = vec![
            svc("charlie", "img:1"),
            svc("alpha", "img:1"),
            svc("bravo", "img:1"),
        ];
        let observed = vec![];

        let (actions, _) = compute_actions(&desired, &observed, &no_health());
        let names: Vec<&str> = actions.iter().map(|a| a.service_name()).collect();
        assert_eq!(names, vec!["alpha", "bravo", "charlie"]);
    }

    #[test]
    fn topo_sort_is_deterministic_across_calls() {
        let desired = vec![
            svc_with_deps("web", "nginx:latest", vec!["api"]),
            svc_with_deps("api", "api:latest", vec!["db", "cache"]),
            svc("db", "postgres:16"),
            svc("cache", "redis:7"),
        ];
        let observed = vec![];

        let (run1, _) = compute_actions(&desired, &observed, &no_health());
        let (run2, _) = compute_actions(&desired, &observed, &no_health());
        assert_eq!(run1, run2);
    }

    #[test]
    fn topo_sort_removes_dependents_before_dependencies() {
        // When removing, dependents should be removed before dependencies.
        let desired = vec![]; // remove everything
        let observed = vec![obs_running("web"), obs_running("db")];

        let (actions, _) = compute_actions(&desired, &observed, &no_health());
        assert_eq!(actions.len(), 2);
        // Both are removes, sorted by name since no deps in this case.
        assert!(
            actions
                .iter()
                .all(|a| matches!(a, Action::ServiceRemove { .. }))
        );
    }

    // ── Dependency gating tests ──

    #[test]
    fn dep_gating_no_healthcheck_creates_all_in_batch() {
        // Without health checks, all services are created in one pass
        // (topo sorted). No gating needed.
        let desired = vec![
            svc("db", "postgres:16"),
            svc_with_deps("web", "nginx:latest", vec!["db"]),
        ];
        let observed = vec![];

        let (actions, deferred) = compute_actions(&desired, &observed, &no_health());

        assert_eq!(actions.len(), 2);
        assert!(deferred.is_empty());
        // db comes before web (topo order).
        assert_eq!(actions[0].service_name(), "db");
        assert_eq!(actions[1].service_name(), "web");
    }

    #[test]
    fn dep_gating_failed_dep_blocks() {
        // db is Failed → web is deferred.
        let desired = vec![
            svc("db", "postgres:16"),
            svc_with_deps("web", "nginx:latest", vec!["db"]),
        ];
        let observed = vec![obs("db", ServicePhase::Failed)];

        let (actions, deferred) = compute_actions(&desired, &observed, &no_health());

        // db gets ServiceCreate (recreate from Failed), web is deferred.
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].service_name(), "db");
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].service_name, "web");
    }

    #[test]
    fn dep_gating_service_started_ignores_healthcheck() {
        // db has a health check and is Running but not yet healthy.
        // With service_started condition (default), web should NOT be blocked.
        let desired = vec![
            svc_with_healthcheck("db", "postgres:16"),
            svc_with_deps("web", "nginx:latest", vec!["db"]),
        ];
        let observed = vec![obs_running("db")];

        // No health status — but condition is service_started, so web is unblocked.
        // db is Running (no action), web is not yet created → ServiceCreate.
        let (actions, deferred) = compute_actions(&desired, &observed, &no_health());

        assert!(deferred.is_empty());
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].service_name(), "web");
    }

    #[test]
    fn dep_gating_service_healthy_blocks_until_healthy() {
        // db has a health check and is Running but not yet healthy.
        // With service_healthy condition, web should be blocked.
        let desired = vec![
            svc_with_healthcheck("db", "postgres:16"),
            svc_with_healthy_deps("web", "nginx:latest", vec!["db"]),
        ];
        let observed = vec![obs_running("db")];

        // No health status → health check hasn't passed → web is deferred.
        let (actions, deferred) = compute_actions(&desired, &observed, &no_health());

        assert!(actions.is_empty()); // db is Running, no action needed.
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].service_name, "web");
    }

    #[test]
    fn dep_gating_service_healthy_unblocks_when_healthy() {
        // db has a health check and is Running + healthy.
        // With service_healthy condition, web should be unblocked.
        let desired = vec![
            svc_with_healthcheck("db", "postgres:16"),
            svc_with_healthy_deps("web", "nginx:latest", vec!["db"]),
        ];
        let observed = vec![obs_running("db")];

        let mut health = HashMap::new();
        let mut db_health = HealthStatus::new("db");
        db_health.record_pass();
        health.insert("db".to_string(), db_health);

        let (actions, deferred) = compute_actions(&desired, &observed, &health);

        // web should now be created.
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].service_name(), "web");
        assert!(deferred.is_empty());
    }

    #[test]
    fn dep_gating_chain_all_in_one_pass_without_healthcheck() {
        // app → api → db. No health checks. All created in one pass.
        let desired = vec![
            svc("db", "postgres:16"),
            svc_with_deps("api", "api:latest", vec!["db"]),
            svc_with_deps("app", "myapp:latest", vec!["api"]),
        ];
        let observed = vec![];

        let (actions, deferred) = compute_actions(&desired, &observed, &no_health());

        assert_eq!(actions.len(), 3);
        assert!(deferred.is_empty());
        // Topo order: db → api → app.
        let names: Vec<&str> = actions.iter().map(|a| a.service_name()).collect();
        assert_eq!(names, vec!["db", "api", "app"]);
    }

    #[test]
    fn dep_gating_no_deps_always_proceeds() {
        let desired = vec![svc("web", "nginx:latest")];
        let observed = vec![];

        let (actions, deferred) = compute_actions(&desired, &observed, &no_health());

        assert_eq!(actions.len(), 1);
        assert!(deferred.is_empty());
    }

    // ── Apply integration tests ──

    #[test]
    fn apply_creates_new_services() {
        let store = StateStore::in_memory().unwrap();
        let s = spec("myapp", vec![svc("web", "nginx:latest")]);

        let result = apply(&s, &store, &no_health()).unwrap();
        assert_eq!(result.actions.len(), 1);
        assert_eq!(
            result.actions[0],
            Action::ServiceCreate {
                service_name: "web".to_string()
            }
        );

        let observed = store.load_observed_state("myapp").unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].phase, ServicePhase::Pending);
    }

    #[test]
    fn apply_is_idempotent_when_running() {
        let store = StateStore::in_memory().unwrap();
        let s = spec("myapp", vec![svc("web", "nginx:latest")]);

        // First apply creates the service.
        apply(&s, &store, &no_health()).unwrap();

        // Simulate the service becoming Running.
        store
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

        // Second apply should produce no actions.
        let result = apply(&s, &store, &no_health()).unwrap();
        assert!(result.actions.is_empty());
    }

    #[test]
    fn apply_removes_deleted_services() {
        let store = StateStore::in_memory().unwrap();

        // Start with two services.
        let s1 = spec(
            "myapp",
            vec![svc("web", "nginx:latest"), svc("db", "postgres:16")],
        );
        apply(&s1, &store, &no_health()).unwrap();

        // Simulate both Running.
        for name in &["web", "db"] {
            store
                .save_observed_state("myapp", &obs_running(name))
                .unwrap();
        }

        // Remove db from the spec.
        let s2 = spec("myapp", vec![svc("web", "nginx:latest")]);
        let result = apply(&s2, &store, &no_health()).unwrap();

        assert_eq!(result.actions.len(), 1);
        assert_eq!(
            result.actions[0],
            Action::ServiceRemove {
                service_name: "db".to_string()
            }
        );
    }

    #[test]
    fn apply_emits_events_for_actions() {
        let store = StateStore::in_memory().unwrap();
        let s = spec("myapp", vec![svc("web", "nginx:latest")]);

        apply(&s, &store, &no_health()).unwrap();

        let events = store.load_events("myapp").unwrap();
        // Started + ServiceCreating + Completed.
        assert_eq!(events.len(), 3);
        assert!(matches!(events[0], StackEvent::StackApplyStarted { .. }));
        assert!(matches!(events[1], StackEvent::ServiceCreating { .. }));
        assert!(matches!(events[2], StackEvent::StackApplyCompleted { .. }));
    }

    #[test]
    fn apply_with_healthcheck_gating_service_healthy() {
        let store = StateStore::in_memory().unwrap();
        // db has a health check; web depends on db with service_healthy condition.
        let s = spec(
            "myapp",
            vec![
                svc_with_healthcheck("db", "postgres:16"),
                svc_with_healthy_deps("web", "nginx:latest", vec!["db"]),
            ],
        );

        // First apply: db is created, web is created too (both in batch, topo sorted).
        let r1 = apply(&s, &store, &no_health()).unwrap();
        assert_eq!(r1.actions.len(), 2);
        assert_eq!(r1.actions[0].service_name(), "db");
        assert_eq!(r1.actions[1].service_name(), "web");

        // Simulate db Running but health check NOT yet passing.
        store
            .save_observed_state("myapp", &obs_running("db"))
            .unwrap();
        store
            .save_observed_state("myapp", &obs_running("web"))
            .unwrap();

        // Both are running, second apply is a no-op.
        let r2 = apply(&s, &store, &no_health()).unwrap();
        assert!(r2.actions.is_empty());

        // Now test: if web was stopped, it should be deferred until db is healthy.
        store
            .save_observed_state("myapp", &obs("web", ServicePhase::Stopped))
            .unwrap();

        // Apply with no health status: web is deferred (service_healthy condition, not passed).
        let r3 = apply(&s, &store, &no_health()).unwrap();
        assert!(r3.actions.is_empty()); // No create for web.
        assert_eq!(r3.deferred.len(), 1);
        assert_eq!(r3.deferred[0].service_name, "web");

        // Apply with db healthy: web should be created.
        let mut health = HashMap::new();
        let mut db_health = HealthStatus::new("db");
        db_health.record_pass();
        health.insert("db".to_string(), db_health);

        let r4 = apply(&s, &store, &health).unwrap();
        assert_eq!(r4.actions.len(), 1);
        assert_eq!(r4.actions[0].service_name(), "web");
        assert!(r4.deferred.is_empty());
    }

    #[test]
    fn apply_with_healthcheck_service_started_ignores_health() {
        let store = StateStore::in_memory().unwrap();
        // db has a health check; web depends on db with default (service_started) condition.
        let s = spec(
            "myapp",
            vec![
                svc_with_healthcheck("db", "postgres:16"),
                svc_with_deps("web", "nginx:latest", vec!["db"]),
            ],
        );

        // Simulate db Running but health check NOT yet passing.
        store
            .save_observed_state("myapp", &obs_running("db"))
            .unwrap();
        store
            .save_observed_state("myapp", &obs("web", ServicePhase::Stopped))
            .unwrap();

        // Apply with no health status: web should NOT be deferred because
        // service_started doesn't care about health checks.
        let r = apply(&s, &store, &no_health()).unwrap();
        assert_eq!(r.actions.len(), 1);
        assert_eq!(r.actions[0].service_name(), "web");
        assert!(r.deferred.is_empty());
    }

    #[test]
    fn apply_emits_dependency_blocked_events() {
        let store = StateStore::in_memory().unwrap();
        // db is Failed, web depends on db → web is deferred.
        store
            .save_observed_state("myapp", &obs("db", ServicePhase::Failed))
            .unwrap();

        let s = spec(
            "myapp",
            vec![
                svc("db", "postgres:16"),
                svc_with_deps("web", "nginx:latest", vec!["db"]),
            ],
        );

        let result = apply(&s, &store, &no_health()).unwrap();
        assert_eq!(result.deferred.len(), 1);

        let events = store.load_events("myapp").unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::DependencyBlocked { .. }))
        );
    }

    #[test]
    fn apply_determinism_proof() {
        let store1 = StateStore::in_memory().unwrap();
        let store2 = StateStore::in_memory().unwrap();
        let s = spec(
            "myapp",
            vec![svc("db", "postgres:16"), svc("cache", "redis:7")],
        );

        let r1 = apply(&s, &store1, &no_health()).unwrap();
        let r2 = apply(&s, &store2, &no_health()).unwrap();
        assert_eq!(r1.actions, r2.actions);
    }

    #[test]
    fn apply_empty_spec_produces_no_actions() {
        let store = StateStore::in_memory().unwrap();
        let s = spec("myapp", vec![]);

        let result = apply(&s, &store, &no_health()).unwrap();
        assert!(result.actions.is_empty());
    }
}
