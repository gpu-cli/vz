use super::create::PreparedCreate;
use super::*;

fn validate_failed_create_ownership(
    stack_id: &str,
    requested_container_id: &str,
    ownership: Option<vz_runtime_contract::ContainerGenerationOwnership>,
) -> Option<vz_runtime_contract::ContainerGenerationOwnership> {
    match ownership {
        Some(ownership)
            if ownership.stack_id == stack_id
                && ownership.container_id == requested_container_id
                && ownership.validate().is_ok() =>
        {
            Some(ownership)
        }
        Some(ownership) => {
            error!(
                stack = %stack_id,
                requested_container = %requested_container_id,
                ownership_stack = %ownership.stack_id,
                ownership_container = %ownership.container_id,
                ownership_generation = ownership.generation,
                "runtime returned invalid failed-create ownership; discarding cleanup authority"
            );
            None
        }
        None => None,
    }
}

fn claimed_preflight_failure_result(
    actions: &[Action],
    first_action_index: usize,
    failures: &HashMap<ServiceReplicaKey, String>,
) -> Result<ExecutionResult, StackError> {
    let mut result = ExecutionResult::default();
    for (relative_index, action) in actions.iter().enumerate() {
        let error = failures.get(action.target()).cloned().unwrap_or_else(|| {
            "scoped batch stopped before effects because another claimed predecessor failed preflight"
                .to_string()
        });
        result.failed += 1;
        result
            .errors
            .push((action.target().display_name(), error.clone()));
        result.outcomes.push(IndexedActionOutcome {
            absolute_index: first_action_index
                .checked_add(relative_index)
                .ok_or_else(|| {
                    StackError::InvalidSpec("absolute action index overflow".to_string())
                })?,
            action_hash: crate::reconcile::compute_actions_hash(std::slice::from_ref(action)),
            action_kind: ReconcileActionKind::from_action(action),
            target: action.target().clone(),
            result: ActionOutcomeResult::Failed { error },
        });
    }
    Ok(result)
}

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
    pub(crate) fn validate_scoped_batch_inputs(
        &self,
        spec: &StackSpec,
        actions: &[Action],
    ) -> Result<(), StackError> {
        for action in actions {
            action.validate()?;
            if action.precondition().workload().stack_id != spec.name {
                return Err(super::scope_state_conflict(format!(
                    "action `{}` is scoped to stack `{}` instead of `{}`",
                    action.target().display_name(),
                    action.precondition().workload().stack_id,
                    spec.name
                )));
            }
        }
        if let Some(authority) = &self.scoped_authority
            && authority.scope.stack_id != spec.name
        {
            return Err(super::scope_state_conflict(format!(
                "stack spec `{}` does not match scoped stack `{}`",
                spec.name, authority.scope.stack_id
            )));
        }
        if self.scoped_cleanup_only
            && actions.iter().any(|action| {
                matches!(
                    action,
                    Action::ServiceCreate { .. } | Action::ServiceRecreate { .. }
                )
            })
        {
            return Err(super::scope_state_conflict(
                "cleanup-only scoped executor cannot reserve or activate containers",
            ));
        }
        let has_creates = actions.iter().any(|action| {
            matches!(
                action,
                Action::ServiceCreate { .. } | Action::ServiceRecreate { .. }
            )
        });
        if !has_creates && self.scoped_authority.is_some() {
            return Ok(());
        }
        for service in &spec.services {
            let replicas = service.resources.replicas.max(1);
            if replicas > 1 && service.container_name.is_some() {
                return Err(StackError::InvalidSpec(format!(
                    "service `{}` cannot combine container_name with replicas > 1",
                    service.name
                )));
            }
            if replicas > 1 && service.ports.iter().any(|port| port.host_port.is_some()) {
                return Err(StackError::InvalidSpec(format!(
                    "service `{}` cannot publish a fixed host port with replicas > 1",
                    service.name
                )));
            }
            for replica_index in 1..=replicas {
                ServiceReplicaKey::new(&service.name, replica_index)?;
            }
            let mut resolved_mounts = self
                .volumes
                .resolve_mounts(&service.mounts, &spec.volumes)?;
            crate::volume::validate_bind_mounts(&mut resolved_mounts)?;
            for secret_ref in &service.secrets {
                if !spec
                    .secrets
                    .iter()
                    .any(|secret| secret.name == secret_ref.source)
                {
                    return Err(StackError::InvalidSpec(format!(
                        "secret '{}' referenced by service '{}' not defined at top level",
                        secret_ref.source, service.name
                    )));
                }
                if self.scoped_secret_dir.is_some()
                    && !self.scoped_secret_inputs.contains_key(&secret_ref.source)
                {
                    return Err(super::scope_state_conflict(format!(
                        "scoped staged secret '{}' is missing",
                        secret_ref.source
                    )));
                }
            }
            let secret_mounts = self
                .scoped_secret_dir
                .as_ref()
                .map(|directory| secrets_to_mounts(&service.secrets, directory))
                .unwrap_or_default();
            service_to_run_config(service, &resolved_mounts, &secret_mounts)?;
        }
        let mut port_preview = self.ports.clone();
        for action in actions.iter().filter(|action| {
            matches!(
                action,
                Action::ServiceCreate { .. } | Action::ServiceRecreate { .. }
            )
        }) {
            let service = spec
                .services
                .iter()
                .find(|service| service.name == action.target().service_name)
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "action target references unknown service `{}`",
                        action.target().service_name
                    ))
                })?;
            port_preview.allocate_replica(action.target(), &service.ports)?;
        }
        if let Some(disk_size_mb) = spec.disk_size_mb {
            disk_size_mb.checked_mul(1024 * 1024).ok_or_else(|| {
                StackError::InvalidSpec("stack disk_size_mb overflows bytes".to_string())
            })?;
        }
        spec.services
            .iter()
            .filter_map(|service| service.resources.memory_bytes)
            .try_fold(0_u64, |total, bytes| {
                total.checked_add(bytes).ok_or_else(|| {
                    StackError::InvalidSpec("aggregate service memory overflows u64".to_string())
                })
            })?;
        let total_mounts = spec.services.iter().try_fold(0_usize, |total, service| {
            total
                .checked_add(service.mounts.len())
                .and_then(|value| value.checked_add(service.secrets.len()))
                .ok_or_else(|| {
                    StackError::InvalidSpec(
                        "aggregate service mount count overflows usize".to_string(),
                    )
                })
        })?;
        for service in &spec.services {
            total_mounts
                .checked_add(service.secrets.len())
                .ok_or_else(|| {
                    StackError::InvalidSpec("service mount tag offset overflows usize".to_string())
                })?;
        }
        Ok(())
    }

    /// Execute one exact, durable claimed action batch.
    ///
    /// Fresh batches stage their immutable activation manifest before the
    /// reconcile session is persisted. Retrying the same active session
    /// requires an exact operation, cursor, and action-slice match. Per-action
    /// failures are committed as terminal outcomes; an outer error leaves the
    /// session and started claims at their original cursor for exact replay.
    pub fn execute_claimed_batch(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
        session_id: &str,
        operation_id: &str,
        first_action_index: usize,
    ) -> Result<ExecutionResult, StackError> {
        let authority = self.scoped_authority.as_ref().ok_or_else(|| {
            super::scope_state_conflict("claimed batch execution requires scoped authority")
        })?;
        if authority.scope.stack_id != spec.name {
            return Err(super::scope_state_conflict(format!(
                "stack spec `{}` does not match scoped stack `{}`",
                spec.name, authority.scope.stack_id
            )));
        }
        if actions.is_empty() || session_id.trim().is_empty() || operation_id.trim().is_empty() {
            return Err(StackError::InvalidSpec(
                "claimed batch requires non-empty actions, session_id, and operation_id"
                    .to_string(),
            ));
        }
        if super::is_claimed_teardown_operation(operation_id) {
            return Err(super::scope_state_conflict(
                "reserved teardown-finalizing operation requires the typed teardown API",
            ));
        }
        if let Some(persisted) = self.store.load_reconcile_session(session_id)? {
            if persisted.stack_name != spec.name
                || persisted.operation_id != operation_id
                || persisted.actions_hash != crate::reconcile::compute_actions_hash(actions)
                || persisted.total_actions != actions.len()
                || self.store.load_reconcile_session_actions(session_id)? != actions
            {
                return Err(super::scope_state_conflict(
                    "persisted reconcile session does not match claimed batch identity",
                ));
            }
            match persisted.status {
                ReconcileSessionStatus::Completed | ReconcileSessionStatus::Failed => {
                    if first_action_index != 0 {
                        return Err(super::scope_state_conflict(
                            "terminal claimed batch replay must provide the full action plan",
                        ));
                    }
                    self.require_scoped_batch_manifest(
                        spec,
                        actions,
                        session_id,
                        operation_id,
                        first_action_index,
                    )?;
                    return self.reconstruct_terminal_claimed_result(&persisted, actions);
                }
                ReconcileSessionStatus::Superseded => {
                    return Err(super::scope_state_conflict(
                        "superseded reconcile session cannot be replayed",
                    ));
                }
                ReconcileSessionStatus::Active => {}
            }
        }
        self.validate_scoped_batch_inputs(spec, actions)?;

        let active = self.store.load_active_reconcile_session(&spec.name)?;
        if let Some(active) = active {
            let persisted_actions = self
                .store
                .load_reconcile_session_actions(&active.session_id)?;
            let end = first_action_index
                .checked_add(actions.len())
                .ok_or_else(|| StackError::InvalidSpec("action slice overflow".to_string()))?;
            if active.session_id != session_id
                || active.operation_id != operation_id
                || active.next_action_index != first_action_index
                || active.total_actions != persisted_actions.len()
                || active.actions_hash != crate::reconcile::compute_actions_hash(&persisted_actions)
                || persisted_actions.get(first_action_index..end) != Some(actions)
                || end != persisted_actions.len()
            {
                return Err(super::scope_state_conflict(
                    "active reconcile session does not match the exact claimed batch replay",
                ));
            }
            self.require_scoped_batch_manifest(
                spec,
                actions,
                session_id,
                operation_id,
                first_action_index,
            )?;
        } else {
            if first_action_index != 0 {
                return Err(super::scope_state_conflict(
                    "fresh claimed batch must start at action index zero",
                ));
            }
            self.stage_scoped_batch_manifest(
                spec,
                actions,
                session_id,
                operation_id,
                first_action_index,
            )?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let session = ReconcileSession {
                session_id: session_id.to_string(),
                stack_name: spec.name.clone(),
                operation_id: operation_id.to_string(),
                status: ReconcileSessionStatus::Active,
                actions_hash: crate::reconcile::compute_actions_hash(actions),
                next_action_index: 0,
                total_actions: actions.len(),
                started_at: now,
                updated_at: now,
                completed_at: None,
            };
            self.store.create_reconcile_batch(&session, actions)?;
        }

        let claims = self.store.start_reconcile_batch(
            session_id,
            &spec.name,
            operation_id,
            first_action_index,
            actions,
        )?;
        if claims.len() != actions.len() {
            return Err(super::scope_state_conflict(
                "admitted claim count differs from exact action count",
            ));
        }
        let result = match self.preflight_scoped_claims(
            spec,
            actions,
            session_id,
            operation_id,
            first_action_index,
            &claims,
        )? {
            Some(failed) => failed,
            None => self.execute_with_session(
                spec,
                actions,
                session_id,
                operation_id,
                first_action_index,
                &claims,
            )?,
        };
        let commit = self.store.commit_reconcile_batch(
            session_id,
            &spec.name,
            operation_id,
            first_action_index,
            actions,
            &result.outcomes,
        )?;
        let successful_prefix = result
            .outcomes
            .iter()
            .take_while(|outcome| matches!(outcome.result, ActionOutcomeResult::Succeeded))
            .count();
        let expected_cursor = first_action_index
            .checked_add(successful_prefix)
            .ok_or_else(|| StackError::InvalidSpec("reconcile cursor overflow".to_string()))?;
        let any_failure = result
            .outcomes
            .iter()
            .any(|outcome| matches!(outcome.result, ActionOutcomeResult::Failed { .. }));
        let commit_is_exact = if any_failure {
            commit.status == ReconcileSessionStatus::Failed
                && commit.next_action_index == expected_cursor
        } else {
            commit.status == ReconcileSessionStatus::Completed
                && commit.next_action_index == first_action_index + actions.len()
        };
        if !commit_is_exact {
            return Err(super::scope_state_conflict(
                "claimed batch commit did not prove its exact terminal result",
            ));
        }
        Ok(result)
    }

    /// Execute an exact remove-only batch while deliberately retaining its
    /// active claims for a broader teardown finalizer.
    pub fn begin_claimed_teardown_batch(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
        session_id: &str,
        operation_id: &str,
        first_action_index: usize,
    ) -> Result<ClaimedTeardownAdmission, StackError> {
        let durable_operation_id = super::claimed_teardown_operation_id(operation_id)?;
        if !self.scoped_cleanup_only {
            return Err(super::scope_state_conflict(
                "claimed teardown batching requires cleanup-only authority",
            ));
        }
        if actions.is_empty()
            || actions
                .iter()
                .any(|action| !matches!(action, Action::ServiceRemove { .. }))
        {
            return Err(StackError::InvalidSpec(
                "claimed teardown batch requires one or more exact Remove actions".to_string(),
            ));
        }
        self.validate_scoped_batch_inputs(spec, actions)?;
        if let Some(persisted) = self.store.load_reconcile_session(session_id)? {
            if persisted.status != ReconcileSessionStatus::Active {
                return Err(super::scope_state_conflict(
                    "terminal teardown session cannot rerun its broad finalizer",
                ));
            }
        }
        let active = self.store.load_active_reconcile_session(&spec.name)?;
        if let Some(active) = active {
            let persisted_actions = self
                .store
                .load_reconcile_session_actions(&active.session_id)?;
            let end = first_action_index
                .checked_add(actions.len())
                .ok_or_else(|| StackError::InvalidSpec("teardown action overflow".to_string()))?;
            if active.session_id != session_id
                || active.operation_id != durable_operation_id
                || active.next_action_index != first_action_index
                || active.total_actions != persisted_actions.len()
                || active.actions_hash != crate::reconcile::compute_actions_hash(&persisted_actions)
                || persisted_actions.get(first_action_index..end) != Some(actions)
                || end != persisted_actions.len()
            {
                return Err(super::scope_state_conflict(
                    "active reconcile session does not match exact teardown replay",
                ));
            }
            self.require_scoped_batch_manifest(
                spec,
                actions,
                session_id,
                &durable_operation_id,
                first_action_index,
            )?;
        } else {
            if first_action_index != 0 {
                return Err(super::scope_state_conflict(
                    "fresh claimed teardown must start at action index zero",
                ));
            }
            self.stage_scoped_batch_manifest(
                spec,
                actions,
                session_id,
                &durable_operation_id,
                first_action_index,
            )?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            self.store.create_reconcile_batch(
                &ReconcileSession {
                    session_id: session_id.to_string(),
                    stack_name: spec.name.clone(),
                    operation_id: durable_operation_id.clone(),
                    status: ReconcileSessionStatus::Active,
                    actions_hash: crate::reconcile::compute_actions_hash(actions),
                    next_action_index: 0,
                    total_actions: actions.len(),
                    started_at: now,
                    updated_at: now,
                    completed_at: None,
                },
                actions,
            )?;
        }
        let claims = self.store.start_reconcile_batch(
            session_id,
            &spec.name,
            &durable_operation_id,
            first_action_index,
            actions,
        )?;
        if claims.len() != actions.len() {
            return Err(super::scope_state_conflict(
                "admitted teardown claim count differs from exact action count",
            ));
        }
        let result = match self.preflight_scoped_claims(
            spec,
            actions,
            session_id,
            &durable_operation_id,
            first_action_index,
            &claims,
        )? {
            Some(failed) => failed,
            None => self.execute_with_session(
                spec,
                actions,
                session_id,
                &durable_operation_id,
                first_action_index,
                &claims,
            )?,
        };
        let pending = PendingClaimedTeardown {
            stack_name: spec.name.clone(),
            spec: spec.clone(),
            session_id: session_id.to_string(),
            operation_id: durable_operation_id,
            first_action_index,
            actions: actions.to_vec(),
            claims,
            result,
        };
        let all_removes_succeeded = pending.result.all_succeeded()
            && pending.result.outcomes.len() == pending.actions.len()
            && pending.result.outcomes.iter().all(|outcome| {
                outcome.action_kind == ReconcileActionKind::Remove
                    && matches!(outcome.result, ActionOutcomeResult::Succeeded)
            });
        if all_removes_succeeded {
            Ok(ClaimedTeardownAdmission::Ready(Box::new(pending)))
        } else {
            Ok(ClaimedTeardownAdmission::Failed(
                self.commit_claimed_teardown_batch(pending)?,
            ))
        }
    }

    /// Commit a previously executed teardown after its broad finalizer succeeds.
    pub fn commit_claimed_teardown_batch(
        &mut self,
        pending: PendingClaimedTeardown,
    ) -> Result<ExecutionResult, StackError> {
        if !self.scoped_cleanup_only {
            return Err(super::scope_state_conflict(
                "claimed teardown commit requires cleanup-only authority",
            ));
        }
        let authority = self.scoped_authority.as_ref().ok_or_else(|| {
            super::scope_state_conflict("claimed teardown commit requires scoped authority")
        })?;
        if authority.scope.stack_id != pending.stack_name {
            return Err(super::scope_state_conflict(
                "claimed teardown commit authority does not match its exact stack",
            ));
        }
        self.require_scoped_batch_manifest(
            &pending.spec,
            &pending.actions,
            &pending.session_id,
            &pending.operation_id,
            pending.first_action_index,
        )?;
        let commit = self.store.commit_claimed_teardown_batch(
            crate::state_store::ClaimedTeardownCommit {
                claims: &pending.claims,
                session_id: &pending.session_id,
                stack_name: &pending.stack_name,
                operation_id: &pending.operation_id,
                expected_cursor: pending.first_action_index,
                actions: &pending.actions,
                outcomes: &pending.result.outcomes,
            },
        )?;
        let successful_prefix = pending
            .result
            .outcomes
            .iter()
            .take_while(|outcome| matches!(outcome.result, ActionOutcomeResult::Succeeded))
            .count();
        let expected_cursor = pending
            .first_action_index
            .checked_add(successful_prefix)
            .ok_or_else(|| StackError::InvalidSpec("teardown cursor overflow".to_string()))?;
        let any_failure = pending
            .result
            .outcomes
            .iter()
            .any(|outcome| matches!(outcome.result, ActionOutcomeResult::Failed { .. }));
        let exact = if any_failure {
            commit.status == ReconcileSessionStatus::Failed
                && commit.next_action_index == expected_cursor
        } else {
            commit.status == ReconcileSessionStatus::Completed
                && commit.next_action_index
                    == pending
                        .first_action_index
                        .checked_add(pending.actions.len())
                        .ok_or_else(|| {
                            StackError::InvalidSpec("teardown end cursor overflow".to_string())
                        })?
        };
        if !exact {
            return Err(super::scope_state_conflict(
                "claimed teardown commit did not prove its exact terminal result",
            ));
        }
        Ok(pending.result)
    }

    fn reconstruct_terminal_claimed_result(
        &self,
        session: &ReconcileSession,
        actions: &[Action],
    ) -> Result<ExecutionResult, StackError> {
        let audits = self.store.load_audit_log_for_session(&session.session_id)?;
        if audits.len() != actions.len() {
            return Err(super::scope_state_conflict(
                "terminal reconcile session audit is not bijective with its actions",
            ));
        }
        let mut result = ExecutionResult::default();
        for (index, (action, audit)) in actions.iter().zip(&audits).enumerate() {
            if audit.session_id != session.session_id
                || audit.stack_name != session.stack_name
                || audit.action_index != index
                || audit.action_kind != ReconcileActionKind::from_action(action).as_audit_str()
                || audit.target != *action.target()
                || audit.action_hash
                    != crate::reconcile::compute_actions_hash(std::slice::from_ref(action))
                || audit.completed_at.is_none()
            {
                return Err(super::scope_state_conflict(
                    "terminal reconcile audit does not match exact action identity",
                ));
            }
            let outcome = match audit.status.as_str() {
                "completed" if audit.error_message.is_none() => {
                    result.succeeded += 1;
                    ActionOutcomeResult::Succeeded
                }
                "failed" => {
                    let error = audit.error_message.clone().ok_or_else(|| {
                        super::scope_state_conflict(
                            "failed terminal reconcile audit is missing its error",
                        )
                    })?;
                    result.failed += 1;
                    result
                        .errors
                        .push((action.target().display_name(), error.clone()));
                    ActionOutcomeResult::Failed { error }
                }
                _ => {
                    return Err(super::scope_state_conflict(
                        "terminal reconcile audit has a nonterminal or malformed outcome",
                    ));
                }
            };
            result.outcomes.push(IndexedActionOutcome {
                absolute_index: index,
                action_hash: audit.action_hash.clone(),
                action_kind: ReconcileActionKind::from_action(action),
                target: action.target().clone(),
                result: outcome,
            });
        }
        let successful_prefix = result
            .outcomes
            .iter()
            .take_while(|outcome| matches!(outcome.result, ActionOutcomeResult::Succeeded))
            .count();
        let terminal_is_exact = match session.status {
            ReconcileSessionStatus::Completed => {
                result.failed == 0 && session.next_action_index == actions.len()
            }
            ReconcileSessionStatus::Failed => {
                result.failed > 0 && session.next_action_index == successful_prefix
            }
            ReconcileSessionStatus::Active | ReconcileSessionStatus::Superseded => false,
        };
        if !terminal_is_exact {
            return Err(super::scope_state_conflict(
                "terminal reconcile session status/cursor disagrees with exact audits",
            ));
        }
        Ok(result)
    }

    /// Execute a batch of reconciler actions for the given stack spec.
    ///
    /// Services at the same topological level (no dependency edges
    /// between them) are created in parallel using [`std::thread::scope`],
    /// while services at different levels execute sequentially to respect
    /// `depends_on` ordering. This gives up to N x speedup for stacks
    /// with N independent services.
    ///
    /// Port allocation is tracked across services: explicit host ports
    /// are validated for conflicts. `None` host ports are treated as
    /// internal-only and are not published to the host.
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
        self.execute_internal(spec, actions, None, None)
    }

    /// Execute a persisted action batch with stable operation identity.
    pub fn execute_with_operation(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
        operation_id: &str,
        first_action_index: usize,
    ) -> Result<ExecutionResult, StackError> {
        if self.scoped_authority.is_some() {
            return Err(StackError::InvalidSpec(
                "scoped stack execution requires an exact reconcile session_id".to_string(),
            ));
        }
        if operation_id.trim().is_empty() {
            return Err(StackError::InvalidSpec(
                "scoped stack execution requires a non-empty operation_id".to_string(),
            ));
        }
        self.execute_internal(
            spec,
            actions,
            Some((operation_id, operation_id, first_action_index)),
            None,
        )
    }

    /// Revalidate and clean exact predecessors before any sandbox or allocator effect.
    pub(crate) fn preflight_scoped_claims(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
        session_id: &str,
        operation_id: &str,
        first_action_index: usize,
        claims: &[ReconcileActionClaim],
    ) -> Result<Option<ExecutionResult>, StackError> {
        if self.scoped_authority.is_none() {
            return Ok(None);
        }
        self.validate_scoped_batch_inputs(spec, actions)?;
        if claims.len() != actions.len() {
            return Err(super::scope_state_conflict(
                "scoped action and claim counts are not bijective",
            ));
        }
        for (relative_index, (action, claim)) in actions.iter().zip(claims).enumerate() {
            let absolute_index =
                first_action_index
                    .checked_add(relative_index)
                    .ok_or_else(|| {
                        StackError::InvalidSpec("absolute action index overflow".to_string())
                    })?;
            self.store.validate_reconcile_action_claim(
                claim,
                session_id,
                operation_id,
                absolute_index,
                action,
            )?;
        }
        self.require_scoped_batch_manifest(
            spec,
            actions,
            session_id,
            operation_id,
            first_action_index,
        )?;
        let failures = self.preflight_claimed_predecessors(spec, actions, claims);
        if failures.is_empty() {
            Ok(None)
        } else {
            Ok(Some(claimed_preflight_failure_result(
                actions,
                first_action_index,
                &failures,
            )?))
        }
    }

    /// Execute a persisted scoped action batch with exact session and operation identity.
    pub(crate) fn execute_with_session(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
        session_id: &str,
        operation_id: &str,
        first_action_index: usize,
        claims: &[ReconcileActionClaim],
    ) -> Result<ExecutionResult, StackError> {
        if session_id.trim().is_empty() || operation_id.trim().is_empty() {
            return Err(StackError::InvalidSpec(
                "scoped stack execution requires non-empty session_id and operation_id".to_string(),
            ));
        }
        self.execute_internal(
            spec,
            actions,
            Some((session_id, operation_id, first_action_index)),
            Some(claims),
        )
    }

    #[cfg(test)]
    pub(crate) fn execute_with_test_session(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
        operation_id: &str,
        first_action_index: usize,
    ) -> Result<ExecutionResult, StackError> {
        if first_action_index != 0 {
            return Err(StackError::InvalidSpec(
                "test claimed execution requires an explicit persisted resume fixture".to_string(),
            ));
        }
        let observed = self.store.load_observed_state(&spec.name)?;
        let drafts = actions
            .iter()
            .map(|action| {
                let current = observed
                    .iter()
                    .find(|state| state.replica == *action.target())
                    .cloned();
                match action {
                    Action::ServiceCreate { target, .. } => {
                        Ok(crate::reconcile::ActionDraft::Create {
                            target: target.clone(),
                            observed: current,
                        })
                    }
                    Action::ServiceRecreate { target, .. } => {
                        Ok(crate::reconcile::ActionDraft::Recreate {
                            target: target.clone(),
                            observed: current.ok_or_else(|| {
                                StackError::InvalidSpec(
                                    "test recreate target has no observed predecessor".to_string(),
                                )
                            })?,
                        })
                    }
                    Action::ServiceRemove { target, .. } => {
                        Ok(crate::reconcile::ActionDraft::Remove {
                            target: target.clone(),
                            observed: current.ok_or_else(|| {
                                StackError::InvalidSpec(
                                    "test remove target has no observed predecessor".to_string(),
                                )
                            })?,
                        })
                    }
                }
            })
            .collect::<Result<Vec<_>, StackError>>()?;
        let actions =
            crate::reconcile::attach_action_preconditions(&spec.name, &self.store, drafts)?;
        self.validate_scoped_batch_inputs(spec, &actions)?;
        let session_id = format!("test-session-{operation_id}");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let session = crate::state_store::ReconcileSession {
            session_id: session_id.clone(),
            stack_name: spec.name.clone(),
            operation_id: operation_id.to_string(),
            status: crate::state_store::ReconcileSessionStatus::Active,
            actions_hash: crate::reconcile::compute_actions_hash(&actions),
            next_action_index: 0,
            total_actions: actions.len(),
            started_at: now,
            updated_at: now,
            completed_at: None,
        };
        if self.scoped_authority.is_some() {
            self.stage_scoped_batch_manifest(spec, &actions, &session_id, operation_id, 0)?;
        }
        self.store.create_reconcile_batch(&session, &actions)?;
        let claims =
            self.store
                .start_reconcile_batch(&session_id, &spec.name, operation_id, 0, &actions)?;
        let result = self.execute_internal(
            spec,
            &actions,
            Some((&session_id, operation_id, first_action_index)),
            Some(&claims),
        )?;
        self.store.commit_reconcile_batch(
            &session_id,
            &spec.name,
            operation_id,
            0,
            &actions,
            &result.outcomes,
        )?;
        Ok(result)
    }

    fn execute_internal(
        &mut self,
        spec: &StackSpec,
        actions: &[Action],
        batch: Option<(&str, &str, usize)>,
        claims: Option<&[ReconcileActionClaim]>,
    ) -> Result<ExecutionResult, StackError> {
        self.validate_scoped_batch_inputs(spec, actions)?;
        if self.scoped_authority.is_some() {
            let (session_id, operation_id, first_action_index) = batch.ok_or_else(|| {
                StackError::InvalidSpec(
                    "scoped executor requires persisted operation identity".to_string(),
                )
            })?;
            let claims = claims.ok_or_else(|| {
                StackError::InvalidSpec(
                    "scoped executor requires exact admitted action claims".to_string(),
                )
            })?;
            if claims.len() != actions.len() {
                return Err(super::scope_state_conflict(
                    "scoped action and claim counts are not bijective",
                ));
            }
            for (relative_index, (action, claim)) in actions.iter().zip(claims).enumerate() {
                let absolute_index =
                    first_action_index
                        .checked_add(relative_index)
                        .ok_or_else(|| {
                            StackError::InvalidSpec("absolute action index overflow".to_string())
                        })?;
                self.store.validate_reconcile_action_claim(
                    claim,
                    session_id,
                    operation_id,
                    absolute_index,
                    action,
                )?;
            }
            self.require_scoped_batch_manifest(
                spec,
                actions,
                session_id,
                operation_id,
                first_action_index,
            )?;
            let failures = self.preflight_claimed_predecessors(spec, actions, claims);
            if !failures.is_empty() {
                return claimed_preflight_failure_result(actions, first_action_index, &failures);
            }
        }
        let has_creates = actions.iter().any(|action| {
            matches!(
                action,
                Action::ServiceCreate { .. } | Action::ServiceRecreate { .. }
            )
        });
        // Ensure named volume directories exist only for a batch that can
        // actually reserve a successor. Remove-only batches never create.
        let created_volumes = if self.scoped_cleanup_only || !has_creates {
            Vec::new()
        } else {
            self.volumes.ensure_volumes(&spec.volumes)?
        };
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
            let mut service_primary_ip: HashMap<ServiceReplicaKey, String> = HashMap::new();
            let mut service_network_ips: HashMap<ServiceReplicaKey, HashMap<String, String>> =
                HashMap::new();
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
                        let target = ServiceReplicaKey::new(&svc.name, r)?;
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
                            .entry(target.clone())
                            .or_insert(ip_no_prefix.clone());
                        service_network_ips
                            .entry(target)
                            .or_default()
                            .insert(net.name.clone(), ip_no_prefix.clone());

                        network_services.push(vz_runtime_contract::NetworkServiceConfig {
                            name: replica_name,
                            addr: ip,
                            network_name: net.name.clone(),
                        });

                        host_offset = host_offset.saturating_add(1);
                    }
                }
            }

            // ── Collect explicit host-published ports using service identity ──
            let mut all_ports = Vec::new();
            for svc in &spec.services {
                let primary = ServiceReplicaKey::first(&svc.name)?;
                let Some(service_ip) = service_primary_ip.get(&primary) else {
                    continue;
                };
                for port in &svc.ports {
                    let Some(host_port) = port.host_port else {
                        // host publish requires explicit opt-in.
                        continue;
                    };
                    let protocol = match port.protocol.as_str() {
                        "udp" => vz_runtime_contract::PortProtocol::Udp,
                        _ => vz_runtime_contract::PortProtocol::Tcp,
                    };
                    all_ports.push(vz_runtime_contract::PortMapping {
                        host: host_port,
                        container: port.container_port,
                        protocol,
                        target_host: Some(service_ip.clone()),
                    });
                }
            }

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
                                    guest_path: None,
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
            let secrets_dir = self
                .scoped_secret_dir
                .clone()
                .unwrap_or_else(|| self.data_dir.join("secrets").join(&spec.name));
            for svc in &spec.services {
                for secret_ref in &svc.secrets {
                    let def = spec
                        .secrets
                        .iter()
                        .find(|d| d.name == secret_ref.source)
                        .ok_or_else(|| {
                            StackError::InvalidSpec(format!(
                                "secret '{}' referenced by service '{}' not defined at top level",
                                secret_ref.source, svc.name
                            ))
                        })?;
                    let secret_path = secrets_dir.join(&secret_ref.source);
                    if !secret_path.exists() {
                        if self.scoped_authority.is_some() {
                            return Err(super::scope_state_conflict(format!(
                                "scoped staged secret '{}' is missing",
                                secret_ref.source
                            )));
                        }
                        let content = self.load_secret_input(def)?;
                        std::fs::create_dir_all(&secrets_dir).map_err(|error| {
                            StackError::InvalidSpec(format!(
                                "failed to create staged secrets directory '{}': {error}",
                                secrets_dir.display()
                            ))
                        })?;
                        std::fs::write(&secret_path, content).map_err(|error| {
                            StackError::InvalidSpec(format!(
                                "failed to write staged secret '{}': {error}",
                                secret_path.display()
                            ))
                        })?;
                    }

                    // Add secret to volume mounts for VirtioFS sharing.
                    // Use "vz-mount-" prefix so OCI runtime translates to /mnt/vz-mount-X.
                    let idx = all_volume_mounts.len();
                    all_volume_mounts.push(vz_runtime_contract::StackVolumeMount {
                        tag: format!("vz-mount-{idx}"),
                        host_path: secrets_dir.clone(),
                        guest_path: None,
                        read_only: true,
                    });
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
            self.service_network_ips = service_network_ips;

            // Legacy execution persists the whole snapshot. Scoped execution
            // persists each exact target atomically with its claimed successor.
            if self.scoped_authority.is_none() {
                self.persist_allocator_state(&spec.name)?;
            }
        }

        let service_map: HashMap<&str, &ServiceSpec> =
            spec.services.iter().map(|s| (s.name.as_str(), s)).collect();

        if self.scoped_authority.is_some() {
            let (session_id, operation_id, first_action_index) = batch.ok_or_else(|| {
                StackError::InvalidSpec(
                    "scoped executor lost its validated operation identity".to_string(),
                )
            })?;
            let claims = claims.ok_or_else(|| {
                StackError::InvalidSpec(
                    "scoped executor lost its admitted action claims".to_string(),
                )
            })?;
            return self.execute_scoped_actions(
                spec,
                actions,
                claims,
                &service_map,
                (session_id, operation_id, first_action_index),
                all_skipped_mounts,
            );
        }

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
            let mut cleanup_failed: HashSet<ServiceReplicaKey> = HashSet::new();
            for action in level {
                let should_remove = match action {
                    Action::ServiceRecreate { .. } => true,
                    Action::ServiceCreate { target, .. } => {
                        let observed = self
                            .store
                            .load_observed_state(&spec.name)
                            .unwrap_or_default();
                        observed
                            .iter()
                            .any(|o| o.replica == *target && o.container_id.is_some())
                    }
                    _ => false,
                };
                if should_remove {
                    if let Err(e) = self.execute_remove(spec, action.target()) {
                        error!(service = %action.service_name(), error = %e, "failed to remove old container");
                        cleanup_failed.insert(action.target().clone());
                        result.failed += 1;
                        result
                            .errors
                            .push((action.service_name().to_string(), e.to_string()));
                    }
                }
            }

            // Serial prep: allocate ports, resolve mounts, and build the exact
            // replica named by each action. Reconciliation already expands a
            // desired service into one action per replica; expanding again here
            // would let one exact action mutate unrelated replicas.
            let mut prepared: Vec<PreparedCreate> = Vec::new();
            for action in level {
                let target = action.target();
                if cleanup_failed.contains(target) {
                    continue;
                }

                match self.prepare_create(spec, &service_map, &target.service_name, target.index())
                {
                    Ok(prep) => prepared.push(prep),
                    Err(error) => {
                        result.failed += 1;
                        result
                            .errors
                            .push((target.display_name(), error.to_string()));
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
                self.mark_failed(spec, &prep.target, &msg)?;
                result.failed += 1;
                result.errors.push((full_name, msg));
            }

            if ok_prepared.len() <= 1 {
                // Single container — execute inline, no thread overhead.
                for prep in ok_prepared {
                    let full_name = prep.full_name();
                    let target = prep.target.clone();
                    let requested_container_id = prep.requested_container_id.clone();
                    info!(service = %full_name, image = %prep.image, "creating container");
                    let create_result = self.runtime.create_in_sandbox_owned(
                        &spec.name,
                        &prep.image,
                        prep.run_config,
                    );
                    match create_result {
                        Ok(receipt) => {
                            self.finalize_create(spec, &target, &receipt)?;
                            result.succeeded += 1;
                        }
                        Err(failure) => {
                            let cleanup = validate_failed_create_ownership(
                                &spec.name,
                                &requested_container_id,
                                failure.cleanup,
                            );
                            self.mark_failed_with_ownership(
                                spec,
                                &target,
                                &failure.error.to_string(),
                                cleanup,
                            )?;
                            result.failed += 1;
                            result.errors.push((full_name, failure.error.to_string()));
                        }
                    }
                }
            } else {
                // Parallel create for multiple containers at the same level.
                // Images are already pulled; only create_in_sandbox runs in threads.
                let full_names: Vec<String> = ok_prepared.iter().map(|p| p.full_name()).collect();
                let full_targets: Vec<ServiceReplicaKey> =
                    ok_prepared.iter().map(|p| p.target.clone()).collect();
                info!(
                    services = ?full_names,
                    "creating {} containers in parallel",
                    full_names.len()
                );

                let runtime = &self.runtime;
                let stack_name = &spec.name;
                let outcomes: Vec<(
                    String,
                    Result<
                        vz_runtime_contract::ContainerCreateReceipt,
                        vz_runtime_contract::OwnedCreateError<StackError>,
                    >,
                )> = std::thread::scope(|s| {
                    let handles: Vec<_> = ok_prepared
                        .into_iter()
                        .map(|prep| {
                            let full_name = prep.full_name();
                            let requested_container_id = prep.requested_container_id.clone();
                            s.spawn(move || {
                                info!(service = %full_name, image = %prep.image, "creating container");
                                (
                                    requested_container_id,
                                    runtime.create_in_sandbox_owned(
                                        stack_name,
                                        &prep.image,
                                        prep.run_config,
                                    ),
                                )
                            })
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|h| match h.join() {
                            Ok(result) => result,
                            Err(_) => (
                                String::new(),
                                Err(vz_runtime_contract::OwnedCreateError {
                                    error: StackError::Network(
                                        "container create thread panicked".to_string(),
                                    ),
                                    cleanup: None,
                                }),
                            ),
                        })
                        .collect()
                });

                // Serial post: update state for each outcome.
                for ((service_name, target), (requested_container_id, outcome)) in
                    full_names.iter().zip(full_targets).zip(outcomes)
                {
                    match outcome {
                        Ok(receipt) => {
                            self.finalize_create(spec, &target, &receipt)?;
                            result.succeeded += 1;
                        }
                        Err(failure) => {
                            let cleanup = validate_failed_create_ownership(
                                &spec.name,
                                &requested_container_id,
                                failure.cleanup,
                            );
                            self.mark_failed_with_ownership(
                                spec,
                                &target,
                                &failure.error.to_string(),
                                cleanup,
                            )?;
                            result.failed += 1;
                            result
                                .errors
                                .push((service_name.clone(), failure.error.to_string()));
                        }
                    }
                }
            }
        }

        // Execute removes sequentially.
        for action in &removes {
            match self.execute_remove(spec, action.target()) {
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
        let first_action_index = batch.map(|(_, _, index)| index).unwrap_or(0);
        result.outcomes = actions
            .iter()
            .enumerate()
            .map(|(relative_index, action)| {
                let exact_label = format!(
                    "{}#{}",
                    action.target().service_name,
                    action.target().replica_index
                );
                let failure = result.errors.iter().find(|(label, _)| {
                    label == &exact_label
                        || label == action.service_name()
                        || label == &action.target().display_name()
                });
                Ok(IndexedActionOutcome {
                    absolute_index: first_action_index.checked_add(relative_index).ok_or_else(
                        || StackError::InvalidSpec("absolute action index overflow".to_string()),
                    )?,
                    action_hash: crate::reconcile::compute_actions_hash(std::slice::from_ref(
                        action,
                    )),
                    action_kind: ReconcileActionKind::from_action(action),
                    target: action.target().clone(),
                    result: failure
                        .map(|(_, error)| ActionOutcomeResult::Failed {
                            error: error.clone(),
                        })
                        .unwrap_or(ActionOutcomeResult::Succeeded),
                })
            })
            .collect::<Result<Vec<_>, StackError>>()?;
        Ok(result)
    }
}
