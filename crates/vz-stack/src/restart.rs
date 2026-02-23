//! Restart policy evaluation for stopped and failed services.
//!
//! The [`RestartTracker`] monitors service lifecycle phases and
//! decides whether a service should be restarted based on its
//! [`RestartPolicy`]. Call [`compute_restarts`] in a monitoring
//! loop to generate reconciler [`Action`]s for services that need
//! restarting.

use std::collections::HashMap;

use tracing::{debug, info};

use crate::reconcile::Action;
use crate::spec::{RestartPolicy, StackSpec};
use crate::state_store::{ServiceObservedState, ServicePhase};

/// Tracks restart counts and decides whether services should restart.
pub struct RestartTracker {
    /// Number of restarts performed per service.
    counts: HashMap<String, u32>,
    /// Services explicitly stopped by the user (exempt from restart).
    explicitly_stopped: std::collections::HashSet<String>,
}

impl RestartTracker {
    /// Create a new tracker with no history.
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
            explicitly_stopped: std::collections::HashSet::new(),
        }
    }

    /// Record that a restart was performed for a service.
    pub fn record_restart(&mut self, service_name: &str) {
        *self.counts.entry(service_name.to_string()).or_insert(0) += 1;
    }

    /// Get the number of restarts performed for a service.
    pub fn restart_count(&self, service_name: &str) -> u32 {
        self.counts.get(service_name).copied().unwrap_or(0)
    }

    /// Mark a service as explicitly stopped (won't restart under `UnlessStopped`).
    pub fn mark_explicitly_stopped(&mut self, service_name: &str) {
        self.explicitly_stopped.insert(service_name.to_string());
    }

    /// Check if a service was explicitly stopped.
    pub fn is_explicitly_stopped(&self, service_name: &str) -> bool {
        self.explicitly_stopped.contains(service_name)
    }

    /// Clear the explicitly-stopped flag (e.g., when user starts the service again).
    pub fn clear_explicitly_stopped(&mut self, service_name: &str) {
        self.explicitly_stopped.remove(service_name);
    }

    /// Reset all state for a service.
    pub fn clear(&mut self, service_name: &str) {
        self.counts.remove(service_name);
        self.explicitly_stopped.remove(service_name);
    }

    /// Reset all tracked state.
    pub fn clear_all(&mut self) {
        self.counts.clear();
        self.explicitly_stopped.clear();
    }

    /// Evaluate whether a single service should be restarted.
    ///
    /// Returns `true` if the service's current phase and restart
    /// policy indicate it should be recreated.
    pub fn should_restart(
        &self,
        service_name: &str,
        phase: ServicePhase,
        policy: Option<&RestartPolicy>,
    ) -> bool {
        // Only restart services that are Stopped or Failed.
        match phase {
            ServicePhase::Stopped | ServicePhase::Failed => {}
            _ => return false,
        }

        let Some(policy) = policy else {
            // No policy means no automatic restart.
            return false;
        };

        match policy {
            RestartPolicy::No => false,
            RestartPolicy::Always => true,
            RestartPolicy::OnFailure { max_retries } => {
                // Only restart on failure, not clean exit.
                if phase != ServicePhase::Failed {
                    return false;
                }
                match max_retries {
                    Some(max) => self.restart_count(service_name) < *max,
                    None => true,
                }
            }
            RestartPolicy::UnlessStopped => {
                // Restart unless the service was explicitly stopped by the user.
                !self.is_explicitly_stopped(service_name)
            }
        }
    }
}

impl Default for RestartTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Evaluate all services and produce restart actions.
///
/// Scans observed state for services that are `Stopped` or `Failed`
/// and returns [`Action::ServiceCreate`] for those whose
/// [`RestartPolicy`] requires a restart.
pub fn compute_restarts(
    spec: &StackSpec,
    observed: &[ServiceObservedState],
    tracker: &RestartTracker,
) -> Vec<Action> {
    let observed_map: HashMap<&str, &ServiceObservedState> = observed
        .iter()
        .map(|o| (o.service_name.as_str(), o))
        .collect();

    let mut actions = Vec::new();

    for svc in &spec.services {
        let Some(obs) = observed_map.get(svc.name.as_str()) else {
            continue;
        };

        if tracker.should_restart(&svc.name, obs.phase.clone(), svc.restart_policy.as_ref()) {
            info!(
                service = %svc.name,
                restarts = tracker.restart_count(&svc.name),
                policy = ?svc.restart_policy,
                "scheduling restart"
            );
            actions.push(Action::ServiceCreate {
                service_name: svc.name.clone(),
            });
        } else {
            debug!(
                service = %svc.name,
                phase = ?obs.phase,
                "not restarting"
            );
        }
    }

    actions
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::spec::ServiceSpec;

    fn svc(name: &str, policy: Option<RestartPolicy>) -> ServiceSpec {
        ServiceSpec {
            name: name.to_string(),
            image: "img:latest".to_string(),
            command: None,
            entrypoint: None,
            environment: std::collections::HashMap::new(),
            working_dir: None,
            user: None,
            mounts: vec![],
            ports: vec![],
            depends_on: vec![],
            healthcheck: None,
            restart_policy: policy,
            resources: Default::default(),
            extra_hosts: vec![],
            secrets: vec![],
            networks: vec![],
            cap_add: vec![],
            cap_drop: vec![],
            privileged: false,
            read_only: false,
            sysctls: std::collections::HashMap::new(),
            ulimits: vec![],
            container_name: None,
            hostname: None,
            domainname: None,
            labels: std::collections::HashMap::new(),
            stop_signal: None,
            stop_grace_period_secs: None,
        }
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

    fn stack(services: Vec<ServiceSpec>) -> StackSpec {
        StackSpec {
            name: "app".to_string(),
            services,
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
            disk_size_mb: None,
        }
    }

    // ── should_restart ──

    #[test]
    fn no_policy_never_restarts() {
        let tracker = RestartTracker::new();
        assert!(!tracker.should_restart("web", ServicePhase::Failed, None));
        assert!(!tracker.should_restart("web", ServicePhase::Stopped, None));
    }

    #[test]
    fn policy_no_never_restarts() {
        let tracker = RestartTracker::new();
        assert!(!tracker.should_restart("web", ServicePhase::Failed, Some(&RestartPolicy::No)));
    }

    #[test]
    fn policy_always_restarts_on_any_exit() {
        let tracker = RestartTracker::new();
        assert!(tracker.should_restart("web", ServicePhase::Failed, Some(&RestartPolicy::Always)));
        assert!(tracker.should_restart("web", ServicePhase::Stopped, Some(&RestartPolicy::Always)));
    }

    #[test]
    fn policy_always_does_not_restart_running() {
        let tracker = RestartTracker::new();
        assert!(!tracker.should_restart(
            "web",
            ServicePhase::Running,
            Some(&RestartPolicy::Always)
        ));
    }

    #[test]
    fn on_failure_restarts_only_on_failure() {
        let tracker = RestartTracker::new();
        let policy = RestartPolicy::OnFailure { max_retries: None };
        assert!(tracker.should_restart("web", ServicePhase::Failed, Some(&policy)));
        assert!(!tracker.should_restart("web", ServicePhase::Stopped, Some(&policy)));
    }

    #[test]
    fn on_failure_respects_max_retries() {
        let mut tracker = RestartTracker::new();
        let policy = RestartPolicy::OnFailure {
            max_retries: Some(2),
        };

        // First restart ok.
        assert!(tracker.should_restart("web", ServicePhase::Failed, Some(&policy)));
        tracker.record_restart("web");

        // Second restart ok.
        assert!(tracker.should_restart("web", ServicePhase::Failed, Some(&policy)));
        tracker.record_restart("web");

        // Third restart blocked (count = 2 = max).
        assert!(!tracker.should_restart("web", ServicePhase::Failed, Some(&policy)));
    }

    #[test]
    fn unless_stopped_restarts_when_not_explicitly_stopped() {
        let tracker = RestartTracker::new();
        let policy = RestartPolicy::UnlessStopped;
        assert!(tracker.should_restart("web", ServicePhase::Failed, Some(&policy)));
        assert!(tracker.should_restart("web", ServicePhase::Stopped, Some(&policy)));
    }

    #[test]
    fn unless_stopped_does_not_restart_when_explicitly_stopped() {
        let mut tracker = RestartTracker::new();
        tracker.mark_explicitly_stopped("web");
        let policy = RestartPolicy::UnlessStopped;
        assert!(!tracker.should_restart("web", ServicePhase::Stopped, Some(&policy)));
    }

    #[test]
    fn clear_explicitly_stopped_allows_restart() {
        let mut tracker = RestartTracker::new();
        tracker.mark_explicitly_stopped("web");
        tracker.clear_explicitly_stopped("web");
        let policy = RestartPolicy::UnlessStopped;
        assert!(tracker.should_restart("web", ServicePhase::Stopped, Some(&policy)));
    }

    // ── compute_restarts ──

    #[test]
    fn compute_restarts_empty_when_all_running() {
        let spec = stack(vec![svc("web", Some(RestartPolicy::Always))]);
        let observed = vec![obs("web", ServicePhase::Running)];
        let tracker = RestartTracker::new();

        let actions = compute_restarts(&spec, &observed, &tracker);
        assert!(actions.is_empty());
    }

    #[test]
    fn compute_restarts_generates_create_for_failed_always() {
        let spec = stack(vec![svc("web", Some(RestartPolicy::Always))]);
        let observed = vec![obs("web", ServicePhase::Failed)];
        let tracker = RestartTracker::new();

        let actions = compute_restarts(&spec, &observed, &tracker);
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0],
            Action::ServiceCreate { service_name } if service_name == "web"
        ));
    }

    #[test]
    fn compute_restarts_skips_no_policy() {
        let spec = stack(vec![svc("web", Some(RestartPolicy::No))]);
        let observed = vec![obs("web", ServicePhase::Failed)];
        let tracker = RestartTracker::new();

        let actions = compute_restarts(&spec, &observed, &tracker);
        assert!(actions.is_empty());
    }

    #[test]
    fn compute_restarts_respects_max_retries() {
        let spec = stack(vec![svc(
            "web",
            Some(RestartPolicy::OnFailure {
                max_retries: Some(1),
            }),
        )]);
        let observed = vec![obs("web", ServicePhase::Failed)];

        let mut tracker = RestartTracker::new();
        tracker.record_restart("web"); // Already restarted once.

        let actions = compute_restarts(&spec, &observed, &tracker);
        assert!(actions.is_empty());
    }

    #[test]
    fn compute_restarts_multiple_services() {
        let spec = stack(vec![
            svc("web", Some(RestartPolicy::Always)),
            svc("db", Some(RestartPolicy::OnFailure { max_retries: None })),
            svc("cache", Some(RestartPolicy::No)),
        ]);
        let observed = vec![
            obs("web", ServicePhase::Failed),
            obs("db", ServicePhase::Failed),
            obs("cache", ServicePhase::Failed),
        ];
        let tracker = RestartTracker::new();

        let actions = compute_restarts(&spec, &observed, &tracker);
        let names: Vec<&str> = actions
            .iter()
            .map(|a| match a {
                Action::ServiceCreate { service_name } => service_name.as_str(),
                _ => "",
            })
            .collect();
        assert_eq!(names, vec!["web", "db"]);
    }

    // ── RestartTracker state management ──

    #[test]
    fn clear_resets_service_state() {
        let mut tracker = RestartTracker::new();
        tracker.record_restart("web");
        tracker.record_restart("web");
        tracker.mark_explicitly_stopped("web");

        tracker.clear("web");
        assert_eq!(tracker.restart_count("web"), 0);
        assert!(!tracker.is_explicitly_stopped("web"));
    }

    #[test]
    fn clear_all_resets_everything() {
        let mut tracker = RestartTracker::new();
        tracker.record_restart("web");
        tracker.record_restart("db");
        tracker.mark_explicitly_stopped("cache");

        tracker.clear_all();
        assert_eq!(tracker.restart_count("web"), 0);
        assert_eq!(tracker.restart_count("db"), 0);
        assert!(!tracker.is_explicitly_stopped("cache"));
    }
}
