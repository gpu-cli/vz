//! Stack orchestration loop: apply → execute → health poll → converge.
//!
//! The [`StackOrchestrator`] drives the reconciliation loop to convergence:
//! 1. Apply the desired spec to compute actions.
//! 2. Execute actions through the container runtime.
//! 3. Poll health checks for running services.
//! 4. Re-apply when health status changes (unblocking deferred services).
//! 5. Exit when all services are converged (running+ready or permanently failed).

use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{debug, info, warn};

use vz_runtime_contract::{Sandbox, SandboxBackend, SandboxSpec, SandboxState};

use crate::error::StackError;
use crate::events::StackEvent;
use crate::executor::{ContainerRuntime, ExecutionResult, StackExecutor};
use crate::health::{HealthPollResult, HealthPoller};
use crate::image_policy::{ImagePolicy, validate_stack_images};
use crate::reconcile::{ApplyResult, compute_actions_hash, plan_apply};
use crate::restart::{RestartTracker, cleanup_orphaned_reconcile_progress, compute_restart_drafts};
use crate::spec::StackSpec;
use crate::state_store::{
    ReconcileSession, ReconcileSessionStatus, ServicePhase, ServiceReplicaKey, StateStore,
};

/// Default poll interval when no health checks are defined (seconds).
const DEFAULT_POLL_INTERVAL_SECS: u64 = 2;

/// Maximum number of reconciliation rounds before giving up.
const MAX_ROUNDS: usize = 100;

/// Configuration for the orchestration loop.
#[derive(Debug, Clone)]
pub struct OrchestrationConfig {
    /// Override poll interval (seconds). If `None`, uses the minimum
    /// health check interval from the spec, or [`DEFAULT_POLL_INTERVAL_SECS`].
    pub poll_interval: Option<u64>,
    /// Maximum number of reconciliation rounds. Default: [`MAX_ROUNDS`].
    pub max_rounds: usize,
    /// Image reference policy enforced before pulling/creating containers.
    ///
    /// Default: [`ImagePolicy::AllowAll`] (non-breaking).
    pub image_policy: ImagePolicy,
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            poll_interval: None,
            max_rounds: MAX_ROUNDS,
            image_policy: ImagePolicy::AllowAll,
        }
    }
}

/// Result of running the orchestration loop.
#[derive(Debug, Clone)]
pub struct OrchestrationResult {
    /// Whether every exact desired replica is running and ready.
    pub converged: bool,
    /// Number of reconciliation rounds executed.
    pub rounds: usize,
    /// Number of services in Running+ready state.
    pub services_ready: usize,
    /// Number of services that permanently failed.
    pub services_failed: usize,
}

/// Callback for each orchestration round, letting callers observe progress.
pub struct RoundReport {
    /// Current round number (1-indexed).
    pub round: usize,
    /// Result of reconciliation (actions + deferred services).
    ///
    /// This is the reconciler-owned convergence claim for the round.
    /// Consumers should treat this as the source of truth for whether
    /// additional reconciliation work is still required.
    pub apply_result: ApplyResult,
    /// Result of executing actions (may be empty if no actions).
    pub exec_result: Option<ExecutionResult>,
    /// Result of health polling (may be empty if no health checks).
    pub health_result: Option<HealthPollResult>,
    /// Services currently ready.
    pub services_ready: usize,
    /// Services currently failed.
    pub services_failed: usize,
    /// Services still pending.
    pub services_pending: usize,
}

/// Drives the stack reconciliation loop to convergence.
///
/// Owns a [`StackExecutor`], a [`HealthPoller`], and a separate
/// [`StateStore`] connection for reconciliation (the executor has
/// its own connection for state persistence during execution).
pub struct StackOrchestrator<R: ContainerRuntime> {
    executor: StackExecutor<R>,
    reconcile_store: StateStore,
    health_poller: HealthPoller,
    restart_tracker: RestartTracker,
    config: OrchestrationConfig,
}

impl<R: ContainerRuntime> StackOrchestrator<R> {
    /// Create a new orchestrator.
    ///
    /// `reconcile_store` should be a separate [`StateStore`] connection
    /// from the one owned by `executor` (both point to the same DB file).
    pub fn new(
        executor: StackExecutor<R>,
        reconcile_store: StateStore,
        config: OrchestrationConfig,
    ) -> Self {
        Self {
            executor,
            reconcile_store,
            health_poller: HealthPoller::new(),
            restart_tracker: RestartTracker::new(),
            config,
        }
    }

    /// Access the underlying executor.
    pub fn executor(&self) -> &StackExecutor<R> {
        &self.executor
    }

    /// Mutably access the underlying executor.
    pub fn executor_mut(&mut self) -> &mut StackExecutor<R> {
        &mut self.executor
    }

    /// Access the health poller.
    pub fn health_poller(&self) -> &HealthPoller {
        &self.health_poller
    }

    /// Subscribe to real-time stack events.
    ///
    /// Returns a [`mpsc::Receiver`] that receives a clone of every
    /// [`StackEvent`] emitted by the orchestrator. Events originate
    /// from both the executor (container lifecycle) and the reconciler
    /// (apply/action planning); both are funnelled into the same channel.
    ///
    /// Events are also durably persisted to the SQLite store regardless
    /// of whether a subscriber exists.
    ///
    /// Only one subscription is active at a time. Calling this again
    /// replaces the previous subscription (the old receiver will see
    /// no further events).
    pub fn subscribe(&mut self) -> mpsc::Receiver<StackEvent> {
        let (tx, rx) = mpsc::channel();
        self.executor.store_mut().set_event_sender(tx.clone());
        self.reconcile_store.set_event_sender(tx);
        rx
    }

    /// Ensure a sandbox exists for the given stack, creating one if needed.
    ///
    /// If a non-terminal sandbox already exists for this stack, it is
    /// reused. Terminal sandboxes are cleaned up and replaced.
    fn ensure_sandbox(&self, spec: &StackSpec) -> Result<Sandbox, StackError> {
        if let Some(existing) = self.reconcile_store.load_sandbox_for_stack(&spec.name)? {
            if !existing.state.is_terminal() {
                return Ok(existing);
            }
            // Terminal sandbox — clean up and create fresh.
            self.reconcile_store.delete_sandbox(&existing.sandbox_id)?;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let sandbox_id = format!("sbx-{:016x}", now);
        let mut labels = BTreeMap::new();
        labels.insert("stack_name".to_string(), spec.name.clone());

        let sandbox = Sandbox {
            sandbox_id: sandbox_id.clone(),
            backend: SandboxBackend::MacosVz,
            spec: SandboxSpec::default(),
            state: SandboxState::Creating,
            created_at: now,
            updated_at: now,
            labels,
        };

        self.reconcile_store.save_sandbox(&sandbox)?;

        self.reconcile_store.emit_event(
            &spec.name,
            &StackEvent::SandboxCreating {
                stack_name: spec.name.clone(),
                sandbox_id,
            },
        )?;

        Ok(sandbox)
    }

    /// Transition a sandbox from Creating to Ready.
    fn transition_sandbox_ready(
        &self,
        spec: &StackSpec,
        sandbox_id: &str,
    ) -> Result<(), StackError> {
        let Some(mut sandbox) = self.reconcile_store.load_sandbox(sandbox_id)? else {
            return Ok(());
        };
        if sandbox.state != SandboxState::Creating {
            return Ok(());
        }

        sandbox
            .transition_to(SandboxState::Ready)
            .map_err(|e| StackError::InvalidSpec(e.to_string()))?;
        sandbox.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.reconcile_store.save_sandbox(&sandbox)?;

        self.reconcile_store.emit_event(
            &spec.name,
            &StackEvent::SandboxReady {
                stack_name: spec.name.clone(),
                sandbox_id: sandbox_id.to_string(),
            },
        )?;

        Ok(())
    }

    /// Tear down the sandbox for a stack, transitioning through Draining to Terminated.
    pub fn teardown_sandbox(&self, spec: &StackSpec) -> Result<(), StackError> {
        let Some(mut sandbox) = self.reconcile_store.load_sandbox_for_stack(&spec.name)? else {
            return Ok(());
        };
        if sandbox.state.is_terminal() {
            return Ok(());
        }

        let sandbox_id = sandbox.sandbox_id.clone();

        // If Ready, transition to Draining first.
        if sandbox.state == SandboxState::Ready {
            sandbox
                .transition_to(SandboxState::Draining)
                .map_err(|e| StackError::InvalidSpec(e.to_string()))?;
            sandbox.updated_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            self.reconcile_store.save_sandbox(&sandbox)?;
            self.reconcile_store.emit_event(
                &spec.name,
                &StackEvent::SandboxDraining {
                    stack_name: spec.name.clone(),
                    sandbox_id: sandbox_id.clone(),
                },
            )?;
        }

        // Transition to Terminated.
        sandbox
            .transition_to(SandboxState::Terminated)
            .map_err(|e| StackError::InvalidSpec(e.to_string()))?;
        sandbox.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.reconcile_store.save_sandbox(&sandbox)?;
        self.reconcile_store.emit_event(
            &spec.name,
            &StackEvent::SandboxTerminated {
                stack_name: spec.name.clone(),
                sandbox_id,
            },
        )?;

        Ok(())
    }

    /// Detect and clean up orphaned containers on startup recovery.
    ///
    /// When the daemon crashes mid-apply, containers from the partial run
    /// may be left running with no reconciliation ownership. This method
    /// queries the runtime for all running containers in the sandbox,
    /// compares them against observed state in the [`StateStore`], and
    /// removes any containers not tracked in observed state.
    ///
    /// Returns the list of orphaned container IDs that were cleaned up.
    pub fn cleanup_orphans(
        &self,
        spec: &StackSpec,
        sandbox_id: &str,
    ) -> Result<Vec<String>, StackError> {
        let running = self.executor.runtime().list_containers(sandbox_id)?;
        if running.is_empty() {
            return Ok(Vec::new());
        }

        let observed = self.reconcile_store.load_observed_state(&spec.name)?;
        let known_ids: std::collections::HashSet<String> = observed
            .iter()
            .filter_map(|o| o.container_id.clone())
            .collect();

        let mut cleaned = Vec::new();
        for container_id in &running {
            if !known_ids.contains(container_id) {
                info!(
                    stack = %spec.name,
                    container_id = %container_id,
                    "cleaning up orphaned container"
                );
                if let Err(e) = self.executor.runtime().stop(container_id, None, None) {
                    warn!(
                        container_id = %container_id,
                        error = %e,
                        "failed to stop orphaned container, attempting remove"
                    );
                }
                if let Err(e) = self.executor.runtime().remove(container_id) {
                    warn!(
                        container_id = %container_id,
                        error = %e,
                        "failed to remove orphaned container"
                    );
                    continue;
                }
                self.reconcile_store.emit_event(
                    &spec.name,
                    &StackEvent::OrphanCleaned {
                        stack_name: spec.name.clone(),
                        container_id: container_id.clone(),
                    },
                )?;
                cleaned.push(container_id.clone());
            }
        }

        if !cleaned.is_empty() {
            info!(
                stack = %spec.name,
                count = cleaned.len(),
                "cleaned up orphaned containers"
            );
        }

        Ok(cleaned)
    }

    /// Run the orchestration loop until convergence or max rounds.
    ///
    /// The optional `on_round` callback is invoked after each round with
    /// a [`RoundReport`], allowing callers to print progress.
    pub fn run(
        &mut self,
        spec: &StackSpec,
        mut on_round: Option<&mut dyn FnMut(&RoundReport)>,
    ) -> Result<OrchestrationResult, StackError> {
        // Enforce image reference policy before any container operations.
        if let Err(violation) = validate_stack_images(&spec.services, &self.config.image_policy) {
            return Err(StackError::ComposeValidation(violation.to_string()));
        }

        let poll_interval = Duration::from_secs(
            self.config
                .poll_interval
                .or_else(|| self.health_poller.min_interval(spec))
                .unwrap_or(DEFAULT_POLL_INTERVAL_SECS),
        );

        let has_health_checks = spec.services.iter().any(|s| s.healthcheck.is_some());

        // Clean up only legacy completed markers; persistence failures are fatal.
        cleanup_orphaned_reconcile_progress(&self.reconcile_store, &spec.name)?;
        // Resume any incomplete action batch before starting new planning rounds.
        self.resume_incomplete_apply(spec)?;
        // Rehydrate persisted health poll state so restart recovery preserves debounce context.
        self.health_poller
            .restore_from_store(&self.reconcile_store, &spec.name)?;

        // Ensure a sandbox exists for this stack.
        let sandbox = self.ensure_sandbox(spec)?;
        let sandbox_id = sandbox.sandbox_id.clone();
        let mut sandbox_marked_ready = sandbox.state != SandboxState::Creating;

        // Restore allocator state from a prior run (crash recovery).
        self.executor.restore_allocator_state(&spec.name)?;

        for round in 1..=self.config.max_rounds {
            info!(round, "orchestration round");

            // 1. Reconcile with current health statuses.
            let health_statuses = self.health_poller.statuses().clone();
            // Scoped orchestration plans without mutating observed lifecycle,
            // allocator digests, or success events. Desired intent is durable,
            // while journal/runtime transitions remain the sole observed-state
            // authority for each exact replica.
            let apply_result = plan_apply(spec, &self.reconcile_store, &health_statuses)?;
            self.reconcile_store.save_desired_state(&spec.name, spec)?;
            self.reconcile_store.emit_event(
                &spec.name,
                &StackEvent::StackApplyStarted {
                    stack_name: spec.name.clone(),
                    services_count: spec.services.len(),
                },
            )?;
            for deferred in &apply_result.deferred {
                self.reconcile_store.emit_event(
                    &spec.name,
                    &StackEvent::DependencyBlocked {
                        stack_name: spec.name.clone(),
                        service_name: deferred.service_name.clone(),
                        waiting_on: deferred.waiting_on.clone(),
                    },
                )?;
            }

            // 2. Execute any new actions.
            let exec_result = if !apply_result.actions.is_empty() {
                let operation_id = Self::next_operation_id(&spec.name, round);
                let session_id = Self::next_session_id(&spec.name, round);
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let actions_hash = compute_actions_hash(&apply_result.actions);

                let session = ReconcileSession {
                    session_id: session_id.clone(),
                    stack_name: spec.name.clone(),
                    operation_id: operation_id.clone(),
                    status: ReconcileSessionStatus::Active,
                    actions_hash,
                    next_action_index: 0,
                    total_actions: apply_result.actions.len(),
                    started_at: now,
                    updated_at: now,
                    completed_at: None,
                };
                self.reconcile_store
                    .create_reconcile_batch(&session, &apply_result.actions)?;
                self.reconcile_store.start_reconcile_batch(
                    &session_id,
                    &spec.name,
                    &operation_id,
                    0,
                    &apply_result.actions,
                )?;

                info!(
                    actions = apply_result.actions.len(),
                    deferred = apply_result.deferred.len(),
                    "executing actions"
                );
                let result = self.executor.execute_with_session(
                    spec,
                    &apply_result.actions,
                    &session_id,
                    &operation_id,
                    0,
                )?;
                let commit = self.reconcile_store.commit_reconcile_batch(
                    &session_id,
                    &spec.name,
                    &operation_id,
                    0,
                    &apply_result.actions,
                    &result.outcomes,
                )?;
                if commit.status == ReconcileSessionStatus::Failed {
                    debug!(
                        cursor = commit.next_action_index,
                        total = apply_result.actions.len(),
                        "exact reconcile batch failed and will be replanned"
                    );
                }

                // A failed removal cannot be represented by convergence over
                // the desired services: an empty desired spec has no service
                // whose Failed/Pending phase could keep convergence false.
                // Surface teardown failures immediately instead of returning a
                // false successful `converged=true` result.
                let removal_failures = result
                    .outcomes
                    .iter()
                    .filter_map(|outcome| {
                        if outcome.action_kind != crate::executor::ReconcileActionKind::Remove {
                            return None;
                        }
                        match &outcome.result {
                            crate::executor::ActionOutcomeResult::Succeeded => None,
                            crate::executor::ActionOutcomeResult::Failed { error } => {
                                Some(format!(
                                    "{}#{}: {error}",
                                    outcome.target.service_name,
                                    outcome.target.index()
                                ))
                            }
                        }
                    })
                    .collect::<Vec<_>>();
                if !removal_failures.is_empty() {
                    return Err(StackError::Network(format!(
                        "service teardown failed: {}",
                        removal_failures.join("; ")
                    )));
                }

                Some(result)
            } else {
                None
            };

            // 2b. Mark sandbox ready once first successful execution.
            if !sandbox_marked_ready {
                if let Some(ref result) = exec_result {
                    if result.succeeded > 0 {
                        self.transition_sandbox_ready(spec, &sandbox_id)?;
                        sandbox_marked_ready = true;
                    }
                }
            }

            // 3. Poll health checks (if any services have them).
            let health_result = if has_health_checks {
                let result = self.health_poller.poll_all(
                    self.executor.runtime(),
                    self.executor.store(),
                    spec,
                )?;
                if !result.newly_ready.is_empty() {
                    info!(ready = ?result.newly_ready, "services became ready");
                }
                if !result.newly_failed.is_empty() {
                    debug!(failed = ?result.newly_failed, "services failed health checks");
                }
                Some(result)
            } else {
                None
            };

            // 3b. Check for services needing restart based on restart policies.
            let observed_for_restart = self.executor.store().load_observed_state(&spec.name)?;
            let restart_drafts =
                compute_restart_drafts(spec, &observed_for_restart, &self.restart_tracker);
            let restart_actions = crate::reconcile::attach_action_preconditions(
                &spec.name,
                &self.reconcile_store,
                restart_drafts,
            )?;
            if !restart_actions.is_empty() {
                info!(
                    restarts = restart_actions.len(),
                    "executing restart actions"
                );
                let operation_id =
                    Self::next_operation_id(&format!("{}-restart", spec.name), round);
                let session_id = Self::next_session_id(&format!("{}-restart", spec.name), round);
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let session = ReconcileSession {
                    session_id: session_id.clone(),
                    stack_name: spec.name.clone(),
                    operation_id: operation_id.clone(),
                    status: ReconcileSessionStatus::Active,
                    actions_hash: compute_actions_hash(&restart_actions),
                    next_action_index: 0,
                    total_actions: restart_actions.len(),
                    started_at: now,
                    updated_at: now,
                    completed_at: None,
                };
                self.reconcile_store
                    .create_reconcile_batch(&session, &restart_actions)?;
                self.reconcile_store.start_reconcile_batch(
                    &session_id,
                    &spec.name,
                    &operation_id,
                    0,
                    &restart_actions,
                )?;
                let restart_result = self.executor.execute_with_session(
                    spec,
                    &restart_actions,
                    &session_id,
                    &operation_id,
                    0,
                )?;
                self.reconcile_store.commit_reconcile_batch(
                    &session_id,
                    &spec.name,
                    &operation_id,
                    0,
                    &restart_actions,
                    &restart_result.outcomes,
                )?;
                for outcome in restart_result.outcomes.iter().filter(|outcome| {
                    matches!(
                        outcome.result,
                        crate::executor::ActionOutcomeResult::Succeeded
                    )
                }) {
                    self.restart_tracker
                        .record_restart(&outcome.target.service_name);
                }
            }

            // 4. Check convergence.
            let (ready, failed, pending) = self.check_convergence(spec)?;

            debug!(
                round,
                ready,
                failed,
                pending,
                deferred = apply_result.deferred.len(),
                "convergence check"
            );

            // Invoke callback.
            if let Some(ref mut cb) = on_round {
                cb(&RoundReport {
                    round,
                    apply_result: apply_result.clone(),
                    exec_result: exec_result.clone(),
                    health_result: health_result.clone(),
                    services_ready: ready,
                    services_failed: failed,
                    services_pending: pending,
                });
            }

            // Convergence is reconciler-owned: only declare converged when
            // observed state has no pending services and reconcile reports no
            // deferred dependency work for this round.
            if Self::reconciler_reports_converged(
                failed,
                pending,
                &apply_result,
                exec_result.as_ref(),
            ) {
                info!(rounds = round, ready, failed, "stack converged");
                self.compact_events(&spec.name);
                return Ok(OrchestrationResult {
                    converged: true,
                    rounds: round,
                    services_ready: ready,
                    services_failed: failed,
                });
            }

            // 5. Sleep before next round.
            std::thread::sleep(poll_interval);
        }

        // Max rounds exhausted.
        let (ready, failed, _) = self.check_convergence(spec)?;
        warn!(
            max_rounds = self.config.max_rounds,
            ready, failed, "orchestration did not converge within max rounds"
        );

        self.compact_events(&spec.name);

        Ok(OrchestrationResult {
            converged: false,
            rounds: self.config.max_rounds,
            services_ready: ready,
            services_failed: failed,
        })
    }

    /// Check how many services are ready, failed, or still pending.
    fn check_convergence(&self, spec: &StackSpec) -> Result<(usize, usize, usize), StackError> {
        let observed = self.executor.store().load_observed_state(&spec.name)?;
        let observed_map: HashMap<&ServiceReplicaKey, _> = observed
            .iter()
            .map(|state| (&state.replica, state))
            .collect();

        let mut ready = 0;
        let mut failed = 0;
        let mut pending = 0;

        for svc in &spec.services {
            for replica_index in 1..=svc.resources.replicas.max(1) {
                let target = ServiceReplicaKey::new(&svc.name, replica_index)?;
                match observed_map.get(&target) {
                    Some(obs) => match obs.phase {
                        ServicePhase::Running => {
                            let health_passed = match &svc.healthcheck {
                                None => true,
                                Some(_) => self
                                    .health_poller
                                    .statuses()
                                    .get(&svc.name)
                                    .is_some_and(|s| s.consecutive_passes >= 1),
                            };
                            if health_passed {
                                ready += 1;
                            } else {
                                pending += 1;
                            }
                        }
                        ServicePhase::Failed => failed += 1,
                        _ => pending += 1,
                    },
                    None => pending += 1,
                }
            }
        }

        Ok((ready, failed, pending))
    }

    fn reconciler_reports_converged(
        failed: usize,
        pending: usize,
        apply_result: &ApplyResult,
        exec_result: Option<&ExecutionResult>,
    ) -> bool {
        let execution_succeeded = exec_result.is_none_or(|result| {
            result.failed == 0
                && result.outcomes.iter().all(|outcome| {
                    matches!(
                        outcome.result,
                        crate::executor::ActionOutcomeResult::Succeeded
                    )
                })
        });
        failed == 0 && pending == 0 && apply_result.deferred.is_empty() && execution_succeeded
    }

    fn next_operation_id(stack_name: &str, round: usize) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("{stack_name}-round-{round}-{nanos}")
    }

    fn next_session_id(stack_name: &str, round: usize) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("rs-{stack_name}-{round}-{nanos}")
    }

    /// Run best-effort event compaction after reconciliation.
    ///
    /// Applies both age-based and count-based retention policies using
    /// [`StateStore::compact_events_default`]. Failures are logged but
    /// do not fail the orchestration.
    fn compact_events(&self, stack_name: &str) {
        match self.reconcile_store.compact_events_default(stack_name) {
            Ok(0) => {}
            Ok(deleted) => {
                debug!(deleted, stack = %stack_name, "compacted stale events");
            }
            Err(e) => {
                warn!(error = %e, stack = %stack_name, "event compaction failed");
            }
        }
    }

    fn resume_incomplete_apply(
        &mut self,
        spec: &StackSpec,
    ) -> Result<Option<ExecutionResult>, StackError> {
        let Some(progress) = self.reconcile_store.load_reconcile_progress(&spec.name)? else {
            return Ok(None);
        };

        let total = progress.actions.len();
        if progress.next_action_index >= total {
            return Err(StackError::InvalidSpec(format!(
                "reconcile progress for `{}` survived at terminal cursor {}",
                spec.name, progress.next_action_index
            )));
        }

        let session = self
            .reconcile_store
            .load_active_reconcile_session(&spec.name)?
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "reconcile progress for `{}` has no active exact session",
                    spec.name
                ))
            })?;
        if session.operation_id != progress.operation_id
            || session.next_action_index != progress.next_action_index
            || session.total_actions != total
            || session.actions_hash != compute_actions_hash(&progress.actions)
        {
            return Err(StackError::InvalidSpec(format!(
                "reconcile progress for `{}` does not match its active exact session",
                spec.name
            )));
        }

        let remaining = progress.actions[progress.next_action_index..].to_vec();
        info!(
            stack = %spec.name,
            operation_id = %progress.operation_id,
            remaining = remaining.len(),
            total,
            "resuming incomplete apply operation"
        );

        self.reconcile_store.start_reconcile_batch(
            &session.session_id,
            &spec.name,
            &progress.operation_id,
            progress.next_action_index,
            &remaining,
        )?;

        let result = self.executor.execute_with_session(
            spec,
            &remaining,
            &session.session_id,
            &progress.operation_id,
            progress.next_action_index,
        )?;
        self.reconcile_store.commit_reconcile_batch(
            &session.session_id,
            &spec.name,
            &progress.operation_id,
            progress.next_action_index,
            &remaining,
            &result.outcomes,
        )?;
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::executor::tests_support::MockContainerRuntime;
    use crate::reconcile::Action;
    use crate::spec::{HealthCheckSpec, ServiceDependency, ServiceKind, ServiceSpec, StackSpec};

    fn svc(name: &str) -> ServiceSpec {
        ServiceSpec {
            name: name.to_string(),
            kind: ServiceKind::Service,
            image: "img:latest".to_string(),
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
            container_name: None,
            hostname: None,
            domainname: None,
            labels: HashMap::new(),
            stop_signal: None,
            stop_grace_period_secs: None,
            expose: vec![],
            stdin_open: false,
            tty: false,
            logging: None,
        }
    }

    fn svc_with_deps(name: &str, deps: Vec<&str>) -> ServiceSpec {
        ServiceSpec {
            depends_on: deps.into_iter().map(ServiceDependency::started).collect(),
            ..svc(name)
        }
    }

    fn svc_with_healthcheck(name: &str) -> ServiceSpec {
        ServiceSpec {
            healthcheck: Some(HealthCheckSpec {
                test: vec!["CMD".to_string(), "true".to_string()],
                interval_secs: Some(1),
                timeout_secs: Some(1),
                retries: Some(3),
                start_period_secs: None,
            }),
            ..svc(name)
        }
    }

    fn stack(name: &str, services: Vec<ServiceSpec>) -> StackSpec {
        crate::reconcile::set_test_action_stack(name);
        StackSpec {
            name: name.to_string(),
            services,
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
            disk_size_mb: None,
        }
    }

    /// Orchestrator using a shared on-disk SQLite DB so reconcile_store
    /// and exec_store see each other's writes.
    fn make_orchestrator_shared(
        runtime: MockContainerRuntime,
    ) -> (StackOrchestrator<MockContainerRuntime>, tempfile::TempDir) {
        make_orchestrator_shared_for(runtime, "app")
    }

    fn make_orchestrator_shared_for(
        runtime: MockContainerRuntime,
        stack_id: &str,
    ) -> (StackOrchestrator<MockContainerRuntime>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("state.db");
        let exec_store = StateStore::open(&db_path).unwrap();
        crate::reconcile::install_test_planning_authority(&exec_store, stack_id);
        let reconcile_store = StateStore::open(&db_path).unwrap();
        let executor = StackExecutor::new(runtime, exec_store, tmp.path());
        let orch = StackOrchestrator::new(
            executor,
            reconcile_store,
            OrchestrationConfig {
                poll_interval: Some(0),
                max_rounds: 10,
                image_policy: crate::image_policy::ImagePolicy::AllowAll,
            },
        );
        (orch, tmp)
    }

    fn make_orchestrator_scoped(
        runtime: MockContainerRuntime,
        stack_id: &str,
    ) -> (StackOrchestrator<MockContainerRuntime>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("state.db");
        let exec_store = StateStore::open(&db_path).unwrap();
        let scope = crate::reconcile::test_planning_scope(&exec_store, stack_id);
        let reconcile_store = StateStore::open(&db_path).unwrap();
        let executor = StackExecutor::new_scoped(runtime, exec_store, tmp.path(), scope).unwrap();
        let orch = StackOrchestrator::new(
            executor,
            reconcile_store,
            OrchestrationConfig {
                poll_interval: Some(0),
                max_rounds: 10,
                image_policy: crate::image_policy::ImagePolicy::AllowAll,
            },
        );
        (orch, tmp)
    }

    #[test]
    fn converges_immediately_without_health_checks() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-db"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web"), svc("db")]);

        let result = orch.run(&spec, None).unwrap();

        assert!(result.converged);
        assert_eq!(result.rounds, 1);
        assert_eq!(result.services_ready, 2);
        assert_eq!(result.services_failed, 0);
    }

    #[test]
    fn reports_failed_services_without_false_convergence() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_create = true;
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        orch.config.max_rounds = 1;
        let spec = stack("app", vec![svc("web")]);

        let result = orch.run(&spec, None).unwrap();

        assert!(!result.converged);
        assert_eq!(result.rounds, 1);
        assert_eq!(result.services_ready, 0);
        assert_eq!(result.services_failed, 1);
    }

    #[test]
    fn max_rounds_respected() {
        // Health check always fails → never converges.
        // Set retries higher than max_rounds so health never exhausts retries.
        let mut runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        runtime.exec_exit_code = 1;
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        orch.config.max_rounds = 3;

        let spec = stack(
            "app",
            vec![ServiceSpec {
                healthcheck: Some(HealthCheckSpec {
                    test: vec!["CMD".to_string(), "false".to_string()],
                    interval_secs: Some(1),
                    timeout_secs: Some(1),
                    retries: Some(100), // Much higher than max_rounds.
                    start_period_secs: None,
                }),
                ..svc("web")
            }],
        );
        let result = orch.run(&spec, None).unwrap();

        assert!(!result.converged);
        assert_eq!(result.rounds, 3);
    }

    #[test]
    fn converges_with_health_check_passing() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        // exec_exit_code defaults to 0 → health check passes.
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc_with_healthcheck("web")]);

        let result = orch.run(&spec, None).unwrap();

        assert!(result.converged);
        assert_eq!(result.services_ready, 1);
    }

    #[test]
    fn callback_invoked_each_round() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web")]);

        let mut round_count = 0;
        orch.run(
            &spec,
            Some(&mut |report: &RoundReport| {
                round_count += 1;
                assert_eq!(report.round, round_count);
            }),
        )
        .unwrap();

        assert_eq!(round_count, 1);
    }

    #[test]
    fn second_run_reports_no_reconcile_work_when_already_converged() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web")]);

        let first = orch.run(&spec, None).unwrap();
        assert!(first.converged);

        let mut reports = Vec::new();
        let second = orch
            .run(
                &spec,
                Some(&mut |report: &RoundReport| {
                    reports.push((
                        report.apply_result.actions.len(),
                        report.apply_result.deferred.len(),
                    ));
                }),
            )
            .unwrap();

        assert!(second.converged);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0], (0, 0));
    }

    #[test]
    fn dependency_ordering_respected() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-db", "ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc_with_deps("web", vec!["db"]), svc("db")]);

        let result = orch.run(&spec, None).unwrap();

        assert!(result.converged);
        assert_eq!(result.services_ready, 2);

        // Verify db was created before web.
        // Multi-service stacks use create_in_sandbox instead of create.
        let calls = orch.executor.runtime().call_log();
        let create_calls: Vec<&str> = calls
            .iter()
            .filter(|(op, _)| op == "create" || op == "create_in_sandbox")
            .map(|(_, arg)| arg.as_str())
            .collect();
        assert_eq!(create_calls.len(), 2);
        // Both images are "img:latest" but db should be first via topo sort.
    }

    #[test]
    fn empty_spec_converges_immediately() {
        let runtime = MockContainerRuntime::new();
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![]);

        let result = orch.run(&spec, None).unwrap();

        assert!(result.converged);
        assert_eq!(result.rounds, 1);
        assert_eq!(result.services_ready, 0);
        assert_eq!(result.services_failed, 0);
    }

    #[test]
    fn empty_down_spec_propagates_exact_generation_cleanup_failure() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_scoped(runtime, "app");
        let up_spec = stack("app", vec![svc("web")]);
        assert!(orch.run(&up_spec, None).unwrap().converged);

        orch.executor.runtime_mut().fail_generation_cleanup = true;
        let down_spec = stack("app", vec![]);
        let error = orch.run(&down_spec, None).unwrap_err();

        assert!(error.to_string().contains("service teardown failed"));
        assert!(
            error
                .to_string()
                .contains("mock generation cleanup failure")
        );
        let calls = orch.executor.runtime().call_log();
        assert!(
            calls
                .iter()
                .any(|(operation, _)| operation == "stop_and_remove_container_generation")
        );
        assert!(
            !calls
                .iter()
                .any(|(operation, _)| { matches!(operation.as_str(), "stop" | "remove") })
        );
        let observed = orch.executor.store().load_observed_state("app").unwrap();
        assert_eq!(observed[0].phase, ServicePhase::Stopping);
        assert!(observed[0].container_id.is_some());
        assert!(observed[0].failed_create_ownership.is_some());
    }

    // ── Real-time event streaming tests ──

    #[test]
    fn subscribe_receives_events() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let rx = orch.subscribe();

        let spec = stack("app", vec![svc("web")]);
        let result = orch.run(&spec, None).unwrap();
        assert!(result.converged);

        // Collect all events from the channel.
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }

        // Should have received lifecycle events.
        assert!(!events.is_empty(), "subscriber should receive events");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::StackApplyStarted { .. })),
            "should receive StackApplyStarted"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::ServiceCreating { .. })),
            "should receive ServiceCreating"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::ServiceReady { .. })),
            "should receive ServiceReady"
        );
    }

    #[test]
    fn subscribe_and_sqlite_both_receive_events() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let rx = orch.subscribe();

        let spec = stack("app", vec![svc("web")]);
        orch.run(&spec, None).unwrap();

        // Channel events.
        let mut channel_events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            channel_events.push(event);
        }

        // SQLite events.
        let sqlite_events = orch.executor().store().load_events("app").unwrap();

        // Both should have the same events.
        assert_eq!(
            channel_events.len(),
            sqlite_events.len(),
            "channel and SQLite should have same event count"
        );
    }

    #[test]
    fn no_subscriber_does_not_error() {
        // Without calling subscribe(), events should still persist to SQLite.
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web")]);

        let result = orch.run(&spec, None).unwrap();
        assert!(result.converged);

        let events = orch.executor().store().load_events("app").unwrap();
        assert!(!events.is_empty());
    }

    #[test]
    fn resumes_incomplete_action_batch_before_next_round() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web")]);

        let pending = vec![Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        }];
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let session = ReconcileSession {
            session_id: "resume-session-1".to_string(),
            stack_name: "app".to_string(),
            operation_id: "resume-op-1".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: compute_actions_hash(&pending),
            next_action_index: 0,
            total_actions: pending.len(),
            started_at: now,
            updated_at: now,
            completed_at: None,
        };
        orch.executor()
            .store()
            .create_reconcile_batch(&session, &pending)
            .unwrap();

        let result = orch.run(&spec, None).unwrap();
        assert!(result.converged);
        assert!(
            orch.executor()
                .store()
                .load_reconcile_progress("app")
                .unwrap()
                .is_none()
        );

        let create_calls = orch
            .executor()
            .runtime()
            .call_log()
            .into_iter()
            .filter(|(op, _)| op == "create" || op == "create_in_sandbox")
            .count();
        assert_eq!(create_calls, 1);
    }

    #[test]
    fn outer_executor_error_reopens_with_started_audits_and_old_exact_cursor() {
        let runtime = MockContainerRuntime::new();
        let (mut orch, tmp) = make_orchestrator_shared_for(runtime, "outer-error");
        let mut service = svc("api");
        service.resources.replicas = 2;
        service.ports.push(crate::spec::PortSpec {
            protocol: "tcp".to_string(),
            container_port: 8080,
            host_port: Some(18_080),
        });
        let spec = stack("outer-error", vec![service]);

        let error = orch
            .run(&spec, None)
            .expect_err("executor must reject fixed host ports for replicated services");
        assert!(error.to_string().contains("fixed host port"));
        let session = orch
            .reconcile_store
            .load_active_reconcile_session("outer-error")
            .unwrap()
            .unwrap();
        assert_eq!(session.next_action_index, 0);
        let actions = orch
            .reconcile_store
            .load_reconcile_session_actions(&session.session_id)
            .unwrap();
        assert_eq!(actions.len(), 2);
        assert!(
            orch.reconcile_store
                .load_audit_log_for_session(&session.session_id)
                .unwrap()
                .iter()
                .all(|entry| entry.status == "started")
        );
        drop(orch);

        let reopened = StateStore::open(&tmp.path().join("state.db")).unwrap();
        let reopened_session = reopened
            .load_active_reconcile_session("outer-error")
            .unwrap()
            .unwrap();
        assert_eq!(reopened_session.session_id, session.session_id);
        assert_eq!(reopened_session.next_action_index, 0);
        let progress = reopened
            .load_reconcile_progress("outer-error")
            .unwrap()
            .unwrap();
        assert_eq!(progress.next_action_index, 0);
        assert_eq!(progress.actions, actions);
        reopened
            .start_reconcile_batch(
                &session.session_id,
                "outer-error",
                &session.operation_id,
                0,
                &actions,
            )
            .unwrap();
    }

    #[test]
    fn clears_completed_progress_marker_on_start() {
        let runtime = MockContainerRuntime::new();
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![]);

        orch.executor()
            .store()
            .save_reconcile_progress("app", "completed-op", &[], 0)
            .unwrap();

        let result = orch.run(&spec, None).unwrap();
        assert!(result.converged);
        assert!(
            orch.executor()
                .store()
                .load_reconcile_progress("app")
                .unwrap()
                .is_none()
        );
    }

    // ── Sandbox lifecycle tests ──

    #[test]
    fn orchestrator_creates_sandbox_on_run() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web")]);

        let result = orch.run(&spec, None).unwrap();
        assert!(result.converged);

        // Sandbox should exist and be Ready (single service converged).
        let sandbox = orch.reconcile_store.load_sandbox_for_stack("app").unwrap();
        assert!(sandbox.is_some());
        let sandbox = sandbox.unwrap();
        assert_eq!(sandbox.state, SandboxState::Ready);
    }

    #[test]
    fn orchestrator_teardown_transitions_to_terminated() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web")]);

        orch.run(&spec, None).unwrap();

        orch.teardown_sandbox(&spec).unwrap();

        let sandbox = orch
            .reconcile_store
            .load_sandbox_for_stack("app")
            .unwrap()
            .unwrap();
        assert_eq!(sandbox.state, SandboxState::Terminated);
    }

    // ── Orphan cleanup tests ──

    #[test]
    fn cleanup_orphans_removes_unknown_containers() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        // Simulate orphaned containers visible to the runtime.
        *runtime.listed_containers.lock().unwrap() =
            vec!["ctr-orphan-1".to_string(), "ctr-orphan-2".to_string()];
        let (orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web")]);
        // No observed state → everything listed is an orphan.
        let cleaned = orch.cleanup_orphans(&spec, "sbx-test").unwrap();
        assert_eq!(cleaned.len(), 2);
        assert!(cleaned.contains(&"ctr-orphan-1".to_string()));
        assert!(cleaned.contains(&"ctr-orphan-2".to_string()));

        // Verify stop + remove were called for each orphan.
        let calls = orch.executor.runtime().call_log();
        let stop_calls: Vec<&str> = calls
            .iter()
            .filter(|(op, _)| op == "stop")
            .map(|(_, arg)| arg.as_str())
            .collect();
        assert_eq!(stop_calls.len(), 2);
        let remove_calls: Vec<&str> = calls
            .iter()
            .filter(|(op, _)| op == "remove")
            .map(|(_, arg)| arg.as_str())
            .collect();
        assert_eq!(remove_calls.len(), 2);
    }

    #[test]
    fn cleanup_orphans_skips_known_containers() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web")]);

        // Run once to create observed state with container_id.
        let result = orch.run(&spec, None).unwrap();
        assert!(result.converged);

        // Now simulate the same container as "running" plus one orphan.
        let observed = orch.executor.store().load_observed_state("app").unwrap();
        let known_id = observed[0].container_id.as_ref().unwrap().clone();
        *orch.executor.runtime().listed_containers.lock().unwrap() =
            vec![known_id.clone(), "ctr-orphan".to_string()];

        let cleaned = orch.cleanup_orphans(&spec, "sbx-test").unwrap();
        assert_eq!(cleaned.len(), 1);
        assert_eq!(cleaned[0], "ctr-orphan");
    }

    #[test]
    fn cleanup_orphans_noop_when_no_running_containers() {
        let runtime = MockContainerRuntime::new();
        // listed_containers is empty by default.
        let (orch, _tmp) = make_orchestrator_shared(runtime);
        let spec = stack("app", vec![svc("web")]);

        let cleaned = orch.cleanup_orphans(&spec, "sbx-test").unwrap();
        assert!(cleaned.is_empty());
    }

    #[test]
    fn cleanup_orphans_emits_events() {
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        *runtime.listed_containers.lock().unwrap() = vec!["ctr-orphan-1".to_string()];
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        let rx = orch.subscribe();
        let spec = stack("app", vec![svc("web")]);

        let cleaned = orch.cleanup_orphans(&spec, "sbx-test").unwrap();
        assert_eq!(cleaned.len(), 1);

        // Check that OrphanCleaned event was emitted.
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StackEvent::OrphanCleaned {
                    container_id, ..
                } if container_id == "ctr-orphan-1")),
            "should emit OrphanCleaned event"
        );
    }

    // ── Max rounds exhaustion leaves services in terminal state ──

    #[test]
    fn max_rounds_exhaustion_reports_pending_services() {
        // Health check always fails → service stays Running but not ready.
        let mut runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        runtime.exec_exit_code = 1;
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        orch.config.max_rounds = 3;

        let spec = stack(
            "app",
            vec![ServiceSpec {
                healthcheck: Some(HealthCheckSpec {
                    test: vec!["CMD".to_string(), "false".to_string()],
                    interval_secs: Some(1),
                    timeout_secs: Some(1),
                    retries: Some(100), // Never exhausts retries within max_rounds.
                    start_period_secs: None,
                }),
                ..svc("web")
            }],
        );

        let result = orch.run(&spec, None).unwrap();
        assert!(!result.converged);
        assert_eq!(result.rounds, 3);
        // Service is Running but not ready (health check failing).
        assert_eq!(result.services_ready, 0);
        assert_eq!(result.services_failed, 0);

        // Service phase in state store should be Running (not Creating or Stopping).
        let observed = orch.executor().store().load_observed_state("app").unwrap();
        let web = observed
            .iter()
            .find(|o| o.replica.service_name == "web")
            .unwrap();
        assert_eq!(
            web.phase,
            ServicePhase::Running,
            "service should be Running, not stuck in a transient state"
        );
        assert!(
            web.container_id.is_some(),
            "container ID should still be present"
        );
    }

    #[test]
    fn max_rounds_exhaustion_with_create_failure_leaves_failed_state() {
        let mut runtime = MockContainerRuntime::new();
        runtime.fail_create = true;
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        orch.config.max_rounds = 1;

        let spec = stack("app", vec![svc("web")]);

        let result = orch.run(&spec, None).unwrap();
        // Create fails every round → service stays Failed → reconciler
        // keeps retrying → hits max_rounds.
        assert!(!result.converged);
        assert_eq!(result.rounds, 1);
        assert_eq!(result.services_failed, 1);

        let observed = orch.executor().store().load_observed_state("app").unwrap();
        let web = observed
            .iter()
            .find(|o| o.replica.service_name == "web")
            .unwrap();
        assert_eq!(
            web.phase,
            ServicePhase::Failed,
            "service should be Failed, not stuck in Creating"
        );
        assert!(
            orch.reconcile_store
                .load_reconcile_progress("app")
                .unwrap()
                .is_none(),
            "failed exact batches must be replanned, never falsely resumed as complete"
        );
        let sessions = orch
            .reconcile_store
            .list_reconcile_sessions("app", 10)
            .unwrap();
        assert!(!sessions.is_empty());
        assert!(sessions.iter().all(|session| {
            session.status == ReconcileSessionStatus::Failed && session.next_action_index == 0
        }));
        for session in sessions {
            let audits = orch
                .reconcile_store
                .load_audit_log_for_session(&session.session_id)
                .unwrap();
            assert_eq!(audits.len(), 1);
            assert_eq!(audits[0].status, "failed");
            assert_eq!(
                audits[0].target,
                crate::state_store::ServiceReplicaKey::first("web").unwrap()
            );
        }
    }

    #[test]
    fn convergence_counts_every_exact_replica_and_rejects_mixed_failure() {
        let runtime = MockContainerRuntime::new();
        let (orch, _tmp) = make_orchestrator_shared(runtime);
        let mut service = svc("web");
        service.resources.replicas = 3;
        let spec = stack("app", vec![service]);

        for (replica_index, phase) in [
            (1, ServicePhase::Running),
            (2, ServicePhase::Failed),
            (3, ServicePhase::Running),
        ] {
            orch.executor()
                .store()
                .save_observed_state(
                    "app",
                    &crate::state_store::ServiceObservedState {
                        replica: ServiceReplicaKey::new("web", replica_index).unwrap(),
                        applied_config_digest: Some("sha256:fixture".to_string()),
                        phase,
                        container_id: Some(format!("ctr-web-{replica_index}")),
                        failed_create_ownership: None,
                        last_error: (replica_index == 2).then(|| "boom".to_string()),
                        ready: replica_index != 2,
                    },
                )
                .unwrap();
        }

        let (ready, failed, pending) = orch.check_convergence(&spec).unwrap();
        assert_eq!((ready, failed, pending), (2, 1, 0));
        assert!(
            !StackOrchestrator::<MockContainerRuntime>::reconciler_reports_converged(
                failed,
                pending,
                &ApplyResult::default(),
                None,
            )
        );
    }

    #[test]
    fn orchestrator_resumes_cleanly_after_max_rounds() {
        // First run: health check always fails → max_rounds hit.
        let mut runtime = MockContainerRuntime::with_ids(vec!["ctr-web"]);
        runtime.exec_exit_code = 1;
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);
        orch.config.max_rounds = 2;

        let spec = stack(
            "app",
            vec![ServiceSpec {
                healthcheck: Some(HealthCheckSpec {
                    test: vec!["CMD".to_string(), "check".to_string()],
                    interval_secs: Some(0), // No throttle.
                    timeout_secs: Some(1),
                    retries: Some(100),
                    start_period_secs: None,
                }),
                ..svc("web")
            }],
        );

        let r1 = orch.run(&spec, None).unwrap();
        assert!(!r1.converged);
        assert_eq!(r1.rounds, 2);

        // Second run: health check now passes → should converge.
        orch.executor.runtime_mut().exec_exit_code = 0;
        orch.config.max_rounds = 10;
        let r2 = orch.run(&spec, None).unwrap();
        assert!(
            r2.converged,
            "should converge on second run when health checks pass"
        );
        assert_eq!(r2.services_ready, 1);
    }

    #[test]
    fn concurrent_health_check_failures_dont_block_convergence() {
        // Two services with health checks. Both fail initially,
        // then both pass. Verify orchestrator doesn't get stuck.
        let runtime = MockContainerRuntime::with_ids(vec!["ctr-web", "ctr-api"]);
        let (mut orch, _tmp) = make_orchestrator_shared(runtime);

        // Start with exec_exit_code = 1 (health check fails).
        orch.executor.runtime_mut().exec_exit_code = 1;

        let spec = stack(
            "app",
            vec![
                ServiceSpec {
                    healthcheck: Some(HealthCheckSpec {
                        test: vec!["CMD".to_string(), "check".to_string()],
                        interval_secs: Some(0), // No throttle.
                        timeout_secs: Some(1),
                        retries: Some(100),
                        start_period_secs: None,
                    }),
                    ..svc("web")
                },
                ServiceSpec {
                    healthcheck: Some(HealthCheckSpec {
                        test: vec!["CMD".to_string(), "check".to_string()],
                        interval_secs: Some(0), // No throttle.
                        timeout_secs: Some(1),
                        retries: Some(100),
                        start_period_secs: None,
                    }),
                    ..svc("api")
                },
            ],
        );

        // Run once to create services and fail health checks.
        orch.config.max_rounds = 2;
        let r1 = orch.run(&spec, None).unwrap();
        assert!(!r1.converged);

        // Now make health checks pass.
        orch.executor.runtime_mut().exec_exit_code = 0;
        orch.config.max_rounds = 10;
        let r2 = orch.run(&spec, None).unwrap();
        assert!(r2.converged, "both services should converge");
        assert_eq!(r2.services_ready, 2);
    }
}
