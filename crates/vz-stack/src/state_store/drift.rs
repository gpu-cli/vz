use super::*;

impl StateStore {
    // ── Startup drift verification ──

    /// Verify persisted state consistency on startup.
    ///
    /// Returns a list of drift findings describing any inconsistencies
    /// between desired state, observed state, health poller state, and
    /// reconcile sessions. Callers should emit events for each finding
    /// and log appropriately.
    pub fn verify_startup_drift(&self, stack_name: &str) -> Result<Vec<DriftFinding>, StackError> {
        let mut findings = Vec::new();

        let desired = self.load_desired_state(stack_name)?;
        let observed = self.load_observed_state(stack_name)?;
        let health_state = self.load_health_poller_state(stack_name)?;
        let active_session = self.load_active_reconcile_session(stack_name)?;

        // 1. Desired state exists but no observed state.
        if desired.is_some() && observed.is_empty() {
            findings.push(DriftFinding {
                category: "desired_state".to_string(),
                description: "desired state without observations".to_string(),
                severity: DriftSeverity::Warning,
            });
        }

        // 2. Observed state has services not in desired state.
        if let Some(ref spec) = desired {
            let desired_names: std::collections::HashSet<&str> =
                spec.services.iter().map(|s| s.name.as_str()).collect();
            for obs in &observed {
                if !desired_names.contains(obs.service_name.as_str()) {
                    findings.push(DriftFinding {
                        category: "observed_state".to_string(),
                        description: format!(
                            "orphaned observed state for service '{}'",
                            obs.service_name
                        ),
                        severity: DriftSeverity::Warning,
                    });
                }
            }
        }

        // 3. Active reconcile session older than 5 minutes.
        if let Some(ref session) = active_session {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let age_secs = now.saturating_sub(session.updated_at);
            if age_secs > 300 {
                findings.push(DriftFinding {
                    category: "reconcile".to_string(),
                    description: format!(
                        "stale reconcile session '{}' ({}s since last update)",
                        session.session_id, age_secs
                    ),
                    severity: DriftSeverity::Warning,
                });
            }
        }

        // 4. Health poller state exists but desired state is missing.
        if !health_state.is_empty() && desired.is_none() {
            findings.push(DriftFinding {
                category: "health".to_string(),
                description: "orphaned health state".to_string(),
                severity: DriftSeverity::Info,
            });
        }

        Ok(findings)
    }
}
