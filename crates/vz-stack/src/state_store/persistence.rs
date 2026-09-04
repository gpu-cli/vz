use super::*;

#[derive(serde::Serialize, serde::Deserialize)]
struct AllocatorIpSnapshotWire {
    schema_version: u32,
    primary: Vec<AllocatorIpLease>,
    networks: Vec<AllocatorNetworkIpLease>,
}

#[derive(PartialEq, Eq)]
struct PersistedAuditIdentity {
    stack_name: String,
    action_kind: String,
    service_name: String,
    replica_index: i64,
    action_hash: String,
    status: String,
    error_message: Option<String>,
}

fn persisted_u64(entity: &str, id: &str, field: &str, value: i64) -> Result<u64, StackError> {
    u64::try_from(value).map_err(|_| {
        StackError::InvalidSpec(format!(
            "persisted {entity} `{id}` has negative `{field}` timestamp {value}"
        ))
    })
}

fn persisted_optional_u64(
    entity: &str,
    id: &str,
    field: &str,
    value: Option<i64>,
) -> Result<Option<u64>, StackError> {
    value
        .map(|value| persisted_u64(entity, id, field, value))
        .transpose()
}

fn persisted_usize(entity: &str, id: &str, field: &str, value: i64) -> Result<usize, StackError> {
    usize::try_from(value).map_err(|_| {
        StackError::InvalidSpec(format!(
            "persisted {entity} `{id}` has invalid `{field}` value {value}"
        ))
    })
}

fn sqlite_usize(entity: &str, id: &str, field: &str, value: usize) -> Result<i64, StackError> {
    i64::try_from(value).map_err(|_| {
        StackError::InvalidSpec(format!(
            "{entity} `{id}` has `{field}` too large for durable storage"
        ))
    })
}

fn sqlite_timestamp(entity: &str, id: &str, field: &str, value: u64) -> Result<i64, StackError> {
    i64::try_from(value).map_err(|_| {
        StackError::InvalidSpec(format!(
            "{entity} `{id}` has `{field}` too large for durable storage"
        ))
    })
}

impl StateStore {
    // ── Sandbox persistence ──

    /// Persist a sandbox, upserting on `sandbox_id`.
    pub fn save_sandbox(&self, sandbox: &Sandbox) -> Result<(), StackError> {
        // Standalone sandboxes have no stack_name label — use sandbox_id to
        // satisfy the UNIQUE(stack_name) constraint (each sandbox is its own
        // "stack" when running standalone).
        let stack_name = sandbox
            .labels
            .get("stack_name")
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_else(|| sandbox.sandbox_id.clone());
        let state_json = serde_json::to_string(&sandbox.state)?;
        let backend_json = serde_json::to_string(&sandbox.backend)?;
        let spec_json = serde_json::to_string(&sandbox.spec)?;
        let labels_json = serde_json::to_string(&sandbox.labels)?;

        self.conn.execute(
            "INSERT INTO sandbox_state (sandbox_id, stack_name, state, backend, spec_json, labels_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(sandbox_id) DO UPDATE SET
                stack_name = excluded.stack_name,
                state = excluded.state,
                backend = excluded.backend,
                spec_json = excluded.spec_json,
                labels_json = excluded.labels_json,
                updated_at = excluded.updated_at",
            params![
                sandbox.sandbox_id,
                stack_name,
                state_json,
                backend_json,
                spec_json,
                labels_json,
                sandbox.created_at as i64,
                sandbox.updated_at as i64,
            ],
        )?;
        Ok(())
    }

    /// Load a sandbox by its identifier.
    pub fn load_sandbox(&self, sandbox_id: &str) -> Result<Option<Sandbox>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT sandbox_id, stack_name, state, backend, spec_json, labels_json, created_at, updated_at
             FROM sandbox_state WHERE sandbox_id = ?1",
        )?;
        let mut rows = stmt.query(params![sandbox_id])?;

        match rows.next()? {
            Some(row) => Ok(Some(Self::sandbox_from_row(row)?)),
            None => Ok(None),
        }
    }

    /// Load the sandbox associated with a given stack name.
    pub fn load_sandbox_for_stack(&self, stack_name: &str) -> Result<Option<Sandbox>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT sandbox_id, stack_name, state, backend, spec_json, labels_json, created_at, updated_at
             FROM sandbox_state WHERE stack_name = ?1",
        )?;
        let mut rows = stmt.query(params![stack_name])?;

        match rows.next()? {
            Some(row) => Ok(Some(Self::sandbox_from_row(row)?)),
            None => Ok(None),
        }
    }

    /// List all sandboxes ordered by creation time.
    pub fn list_sandboxes(&self) -> Result<Vec<Sandbox>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT sandbox_id, stack_name, state, backend, spec_json, labels_json, created_at, updated_at
             FROM sandbox_state ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })?;

        let mut sandboxes = Vec::new();
        for row_result in rows {
            let (
                sandbox_id,
                _stack_name,
                state_str,
                backend_str,
                spec_str,
                labels_str,
                created_at,
                updated_at,
            ) = row_result?;
            let state: SandboxState = serde_json::from_str(&state_str)?;
            let backend: SandboxBackend = serde_json::from_str(&backend_str)?;
            let spec: SandboxSpec = serde_json::from_str(&spec_str)?;
            let labels: std::collections::BTreeMap<String, String> =
                serde_json::from_str(&labels_str)?;
            let created_at = persisted_u64("sandbox", &sandbox_id, "created_at", created_at)?;
            let updated_at = persisted_u64("sandbox", &sandbox_id, "updated_at", updated_at)?;

            sandboxes.push(Sandbox {
                sandbox_id,
                backend,
                spec,
                state,
                created_at,
                updated_at,
                labels,
            });
        }
        Ok(sandboxes)
    }

    /// Delete a sandbox by its identifier.
    pub fn delete_sandbox(&self, sandbox_id: &str) -> Result<(), StackError> {
        self.conn.execute(
            "DELETE FROM sandbox_state WHERE sandbox_id = ?1",
            params![sandbox_id],
        )?;
        Ok(())
    }

    /// Deserialize a sandbox from a rusqlite row.
    fn sandbox_from_row(row: &rusqlite::Row<'_>) -> Result<Sandbox, StackError> {
        let sandbox_id: String = row.get(0)?;
        let _stack_name: String = row.get(1)?;
        let state_str: String = row.get(2)?;
        let backend_str: String = row.get(3)?;
        let spec_str: String = row.get(4)?;
        let labels_str: String = row.get(5)?;
        let created_at: i64 = row.get(6)?;
        let updated_at: i64 = row.get(7)?;

        let state: SandboxState = serde_json::from_str(&state_str)?;
        let backend: SandboxBackend = serde_json::from_str(&backend_str)?;
        let spec: SandboxSpec = serde_json::from_str(&spec_str)?;
        let labels: std::collections::BTreeMap<String, String> = serde_json::from_str(&labels_str)?;
        let created_at = persisted_u64("sandbox", &sandbox_id, "created_at", created_at)?;
        let updated_at = persisted_u64("sandbox", &sandbox_id, "updated_at", updated_at)?;

        Ok(Sandbox {
            sandbox_id,
            backend,
            spec,
            state,
            created_at,
            updated_at,
            labels,
        })
    }

    // ── Allocator state persistence ──

    /// Persist allocator snapshot for a stack.
    pub fn save_allocator_state(
        &self,
        stack_name: &str,
        snapshot: &AllocatorSnapshot,
    ) -> Result<(), StackError> {
        snapshot.validate()?;
        let ports_json = serde_json::to_string(&snapshot.ports)?;
        let service_ips_json = serde_json::to_string(&AllocatorIpSnapshotWire {
            schema_version: snapshot.schema_version,
            primary: snapshot.service_ips.clone(),
            networks: snapshot.service_network_ips.clone(),
        })?;
        let mount_tag_offsets_json = serde_json::to_string(&snapshot.mount_tag_offsets)?;

        self.conn.execute(
            "INSERT INTO allocator_state (stack_name, ports_json, service_ips_json, mount_tag_offsets_json)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(stack_name) DO UPDATE SET
                ports_json = excluded.ports_json,
                service_ips_json = excluded.service_ips_json,
                mount_tag_offsets_json = excluded.mount_tag_offsets_json,
                updated_at = datetime('now')",
            params![stack_name, ports_json, service_ips_json, mount_tag_offsets_json],
        )?;
        Ok(())
    }

    /// Load allocator snapshot for a stack.
    pub fn load_allocator_state(
        &self,
        stack_name: &str,
    ) -> Result<Option<AllocatorSnapshot>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT ports_json, service_ips_json, mount_tag_offsets_json
             FROM allocator_state WHERE stack_name = ?1",
        )?;
        let mut rows = stmt.query(params![stack_name])?;

        let Some(row) = rows.next()? else {
            return Ok(None);
        };

        let ports_json: String = row.get(0)?;
        let service_ips_json: String = row.get(1)?;
        let mount_tag_offsets_json: String = row.get(2)?;

        let ports: Vec<AllocatorPortLease> = serde_json::from_str(&ports_json)?;
        let ip_wire: AllocatorIpSnapshotWire = serde_json::from_str(&service_ips_json)?;
        let mount_tag_offsets: HashMap<String, usize> =
            serde_json::from_str(&mount_tag_offsets_json)?;
        let snapshot = AllocatorSnapshot {
            schema_version: ip_wire.schema_version,
            ports,
            service_ips: ip_wire.primary,
            service_network_ips: ip_wire.networks,
            mount_tag_offsets,
        };
        snapshot.validate()?;
        Ok(Some(snapshot))
    }

    // ── Reconcile session tracking ──

    /// Create a new reconcile session.
    ///
    /// The `actions` slice is serialized into the `actions_json` column
    /// for auditability. The session struct carries the hash and cursor.
    pub(crate) fn create_reconcile_session(
        &self,
        session: &ReconcileSession,
        actions: &[Action],
    ) -> Result<(), StackError> {
        let computed_hash = crate::reconcile::compute_actions_hash(actions);
        if session.actions_hash != computed_hash
            || session.total_actions != actions.len()
            || session.next_action_index > actions.len()
        {
            return Err(StackError::InvalidSpec(format!(
                "reconcile session `{}` metadata does not match its exact action plan",
                session.session_id
            )));
        }
        let stored_actions: Vec<StoredAction> =
            actions.iter().map(StoredAction::from_action).collect();
        let actions_json = serde_json::to_string(&stored_actions)?;

        self.conn.execute(
            "INSERT INTO reconcile_sessions (
                session_id, stack_name, operation_id, status,
                action_schema_version, actions_json, actions_hash, next_action_index,
                total_actions, started_at, updated_at, completed_at
            ) VALUES (?1, ?2, ?3, ?4, 2, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                session.session_id,
                session.stack_name,
                session.operation_id,
                session.status.as_str(),
                actions_json,
                session.actions_hash,
                sqlite_usize(
                    "reconcile session",
                    &session.session_id,
                    "next_action_index",
                    session.next_action_index
                )?,
                sqlite_usize(
                    "reconcile session",
                    &session.session_id,
                    "total_actions",
                    session.total_actions
                )?,
                sqlite_timestamp(
                    "reconcile session",
                    &session.session_id,
                    "started_at",
                    session.started_at
                )?,
                sqlite_timestamp(
                    "reconcile session",
                    &session.session_id,
                    "updated_at",
                    session.updated_at
                )?,
                session
                    .completed_at
                    .map(|t| sqlite_timestamp(
                        "reconcile session",
                        &session.session_id,
                        "completed_at",
                        t
                    ))
                    .transpose()?,
            ],
        )?;
        Ok(())
    }

    /// Atomically install an exact action plan as the active reconcile operation.
    pub fn create_reconcile_batch(
        &self,
        session: &ReconcileSession,
        actions: &[Action],
    ) -> Result<(), StackError> {
        if session.operation_id.trim().is_empty()
            || actions.is_empty()
            || session.status != ReconcileSessionStatus::Active
            || session.next_action_index != 0
            || session.completed_at.is_some()
        {
            return Err(StackError::InvalidSpec(
                "new reconcile batch requires an active zero-cursor session, non-empty operation, and non-empty action plan"
                    .to_string(),
            ));
        }
        let mut exact_targets = std::collections::BTreeSet::new();
        if actions
            .iter()
            .any(|action| !exact_targets.insert(action.target().clone()))
        {
            return Err(StackError::InvalidSpec(
                "reconcile action plan contains a duplicate exact replica target".to_string(),
            ));
        }
        self.with_immediate_transaction(|store| {
            let active_session = store
                .conn
                .query_row(
                    "SELECT session_id FROM reconcile_sessions
                     WHERE stack_name = ?1 AND status = 'active' LIMIT 1",
                    params![session.stack_name],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(active_session) = active_session {
                return Err(StackError::InvalidSpec(format!(
                    "stack `{}` already has active reconcile session `{active_session}`",
                    session.stack_name
                )));
            }
            let existing_progress = store
                .conn
                .query_row(
                    "SELECT operation_id FROM reconcile_progress WHERE stack_name = ?1",
                    params![session.stack_name],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(operation_id) = existing_progress {
                return Err(StackError::InvalidSpec(format!(
                    "stack `{}` already has reconcile progress for operation `{operation_id}`",
                    session.stack_name
                )));
            }
            store.save_reconcile_progress(
                &session.stack_name,
                &session.operation_id,
                actions,
                session.next_action_index,
            )?;
            store.create_reconcile_session(session, actions)
        })
    }

    /// Atomically validate and mark a contiguous exact action slice as started.
    ///
    /// Replaying the same start is idempotent while the session cursor remains
    /// unchanged. Any mismatch is rejected before the transaction writes.
    pub fn start_reconcile_batch(
        &self,
        session_id: &str,
        stack_name: &str,
        operation_id: &str,
        expected_cursor: usize,
        actions: &[Action],
    ) -> Result<(), StackError> {
        if actions.is_empty() {
            return Err(StackError::InvalidSpec(
                "cannot start an empty reconcile action batch".to_string(),
            ));
        }
        self.with_immediate_transaction(|store| {
            store.validate_active_reconcile_batch(
                session_id,
                stack_name,
                operation_id,
                expected_cursor,
                actions,
            )?;

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let now = sqlite_timestamp("reconcile session", session_id, "started_at", now)?;
            for (relative_index, action) in actions.iter().enumerate() {
                let absolute_index = expected_cursor.checked_add(relative_index).ok_or_else(|| {
                    StackError::InvalidSpec("reconcile action index overflow".to_string())
                })?;
                let action_index = sqlite_usize(
                    "reconcile audit",
                    session_id,
                    "action_index",
                    absolute_index,
                )?;
                let action_hash =
                    crate::reconcile::compute_actions_hash(std::slice::from_ref(action));
                let action_kind = crate::executor::ReconcileActionKind::from_action(action)
                    .as_audit_str();
                store.conn.execute(
                    "INSERT INTO reconcile_audit_log (
                        session_id, stack_name, action_index, action_kind,
                        service_name, replica_index, action_hash, status,
                        started_at, completed_at, error_message
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'started', ?8, NULL, NULL)
                     ON CONFLICT(session_id, action_index) DO NOTHING",
                    params![
                        session_id,
                        stack_name,
                        action_index,
                        action_kind,
                        action.target().service_name,
                        i64::from(action.target().index()),
                        action_hash,
                        now,
                    ],
                )?;

                let persisted = store.conn.query_row(
                    "SELECT stack_name, action_kind, service_name, replica_index,
                            action_hash, status, completed_at, error_message
                     FROM reconcile_audit_log
                     WHERE session_id = ?1 AND action_index = ?2",
                    params![session_id, action_index],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, Option<i64>>(6)?,
                            row.get::<_, Option<String>>(7)?,
                        ))
                    },
                )?;
                if persisted
                    != (
                        stack_name.to_string(),
                        action_kind.to_string(),
                        action.target().service_name.clone(),
                        i64::from(action.target().index()),
                        crate::reconcile::compute_actions_hash(std::slice::from_ref(action)),
                        "started".to_string(),
                        None,
                        None,
                    )
                {
                    return Err(StackError::InvalidSpec(format!(
                        "reconcile audit identity conflict for session `{session_id}` action {absolute_index}"
                    )));
                }
            }
            Ok(())
        })
    }

    fn validate_active_reconcile_batch(
        &self,
        session_id: &str,
        stack_name: &str,
        operation_id: &str,
        expected_cursor: usize,
        actions: &[Action],
    ) -> Result<Vec<Action>, StackError> {
        let (stored_stack, stored_operation, status, schema_version, actions_hash, cursor_raw) =
            self.conn
                .query_row(
                    "SELECT stack_name, operation_id, status, action_schema_version,
                            actions_hash, next_action_index
                     FROM reconcile_sessions WHERE session_id = ?1",
                    params![session_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, i64>(5)?,
                        ))
                    },
                )
                .optional()?
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "reconcile session `{session_id}` was not found"
                    ))
                })?;
        let cursor = persisted_usize(
            "reconcile session",
            session_id,
            "next_action_index",
            cursor_raw,
        )?;
        let plan = self.load_reconcile_session_actions(session_id)?;
        let end = expected_cursor.checked_add(actions.len()).ok_or_else(|| {
            StackError::InvalidSpec("reconcile action slice overflow".to_string())
        })?;
        if stored_stack != stack_name
            || stored_operation != operation_id
            || status != "active"
            || schema_version != 2
            || actions_hash != crate::reconcile::compute_actions_hash(&plan)
            || cursor != expected_cursor
            || end > plan.len()
            || plan[expected_cursor..end] != *actions
        {
            return Err(StackError::InvalidSpec(format!(
                "reconcile batch does not match active session `{session_id}`"
            )));
        }
        let progress = self.load_reconcile_progress(stack_name)?.ok_or_else(|| {
            StackError::InvalidSpec(format!(
                "active reconcile session `{session_id}` has no progress record"
            ))
        })?;
        if progress.operation_id != operation_id
            || progress.next_action_index != expected_cursor
            || progress.actions != plan
        {
            return Err(StackError::InvalidSpec(format!(
                "reconcile progress does not match active session `{session_id}`"
            )));
        }
        Ok(plan)
    }

    /// Atomically terminalize exact action audits and compare-and-swap progress.
    pub fn commit_reconcile_batch(
        &self,
        session_id: &str,
        stack_name: &str,
        operation_id: &str,
        expected_cursor: usize,
        actions: &[Action],
        outcomes: &[crate::executor::IndexedActionOutcome],
    ) -> Result<ReconcileBatchCommit, StackError> {
        self.commit_reconcile_batch_inner(
            session_id,
            stack_name,
            operation_id,
            expected_cursor,
            actions,
            outcomes,
            #[cfg(test)]
            None,
        )
    }

    fn commit_reconcile_batch_inner(
        &self,
        session_id: &str,
        stack_name: &str,
        operation_id: &str,
        expected_cursor: usize,
        actions: &[Action],
        outcomes: &[crate::executor::IndexedActionOutcome],
        #[cfg(test)] failpoint: Option<ReconcileBatchCommitFailpoint>,
    ) -> Result<ReconcileBatchCommit, StackError> {
        if actions.is_empty() || outcomes.len() != actions.len() {
            return Err(StackError::InvalidSpec(
                "reconcile outcomes must form a bijection over the dispatched actions".to_string(),
            ));
        }
        self.with_immediate_transaction(|store| {
            let plan = store.load_reconcile_session_actions(session_id)?;
            let end = expected_cursor.checked_add(actions.len()).ok_or_else(|| {
                StackError::InvalidSpec("reconcile action slice overflow".to_string())
            })?;
            if end > plan.len() || plan[expected_cursor..end] != *actions {
                return Err(StackError::InvalidSpec(format!(
                    "reconcile commit does not match session `{session_id}` plan"
                )));
            }
            for (relative_index, (action, outcome)) in
                actions.iter().zip(outcomes.iter()).enumerate()
            {
                let absolute_index =
                    expected_cursor.checked_add(relative_index).ok_or_else(|| {
                        StackError::InvalidSpec("reconcile action index overflow".to_string())
                    })?;
                if outcome.absolute_index != absolute_index
                    || outcome.action_kind
                        != crate::executor::ReconcileActionKind::from_action(action)
                    || outcome.target != *action.target()
                    || outcome.action_hash
                        != crate::reconcile::compute_actions_hash(std::slice::from_ref(action))
                {
                    return Err(StackError::InvalidSpec(format!(
                        "reconcile outcome does not match exact action {absolute_index}"
                    )));
                }
            }

            let successful_prefix = outcomes
                .iter()
                .take_while(|outcome| {
                    matches!(
                        outcome.result,
                        crate::executor::ActionOutcomeResult::Succeeded
                    )
                })
                .count();
            let next_action_index = expected_cursor
                .checked_add(successful_prefix)
                .ok_or_else(|| StackError::InvalidSpec("reconcile cursor overflow".to_string()))?;
            let any_failure = outcomes.iter().any(|outcome| {
                matches!(
                    outcome.result,
                    crate::executor::ActionOutcomeResult::Failed { .. }
                )
            });
            let status = if !any_failure && next_action_index == plan.len() {
                ReconcileSessionStatus::Completed
            } else if any_failure {
                ReconcileSessionStatus::Failed
            } else {
                ReconcileSessionStatus::Active
            };

            let session_state = store.conn.query_row(
                "SELECT stack_name, operation_id, status, action_schema_version,
                        actions_hash, next_action_index
                 FROM reconcile_sessions WHERE session_id = ?1",
                params![session_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )?;
            if session_state.0 != stack_name
                || session_state.1 != operation_id
                || session_state.3 != 2
                || session_state.4 != crate::reconcile::compute_actions_hash(&plan)
            {
                return Err(StackError::InvalidSpec(format!(
                    "reconcile commit identity mismatch for session `{session_id}`"
                )));
            }
            let persisted_cursor = persisted_usize(
                "reconcile session",
                session_id,
                "next_action_index",
                session_state.5,
            )?;

            if session_state.2 != "active" || persisted_cursor != expected_cursor {
                if persisted_cursor == next_action_index
                    && session_state.2 == status.as_str()
                    && store.reconcile_commit_is_identical(
                        session_id,
                        stack_name,
                        next_action_index,
                        &status,
                        actions,
                        outcomes,
                    )?
                {
                    return Ok(ReconcileBatchCommit {
                        next_action_index,
                        status,
                    });
                }
                return Err(StackError::InvalidSpec(format!(
                    "reconcile session `{session_id}` cursor compare-and-swap lost"
                )));
            }

            let progress = store.load_reconcile_progress(stack_name)?.ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "active reconcile session `{session_id}` has no progress record"
                ))
            })?;
            if progress.operation_id != operation_id
                || progress.next_action_index != expected_cursor
                || progress.actions != plan
            {
                return Err(StackError::InvalidSpec(format!(
                    "reconcile progress compare-and-swap lost for session `{session_id}`"
                )));
            }

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let now = sqlite_timestamp("reconcile session", session_id, "completed_at", now)?;
            for outcome in outcomes {
                let (audit_status, error_message) = match &outcome.result {
                    crate::executor::ActionOutcomeResult::Succeeded => ("completed", None),
                    crate::executor::ActionOutcomeResult::Failed { error } => {
                        ("failed", Some(error.as_str()))
                    }
                };
                let changed = store.conn.execute(
                    "UPDATE reconcile_audit_log
                     SET status = ?1, completed_at = ?2, error_message = ?3
                     WHERE session_id = ?4 AND action_index = ?5
                       AND action_kind = ?6 AND service_name = ?7
                       AND replica_index = ?8 AND action_hash = ?9
                       AND status = 'started' AND completed_at IS NULL
                       AND error_message IS NULL",
                    params![
                        audit_status,
                        now,
                        error_message,
                        session_id,
                        sqlite_usize(
                            "reconcile audit",
                            session_id,
                            "action_index",
                            outcome.absolute_index
                        )?,
                        outcome.action_kind.as_audit_str(),
                        outcome.target.service_name,
                        i64::from(outcome.target.index()),
                        outcome.action_hash,
                    ],
                )?;
                if changed != 1 {
                    return Err(StackError::InvalidSpec(format!(
                        "reconcile action {} has no matching started audit",
                        outcome.absolute_index
                    )));
                }
            }

            #[cfg(test)]
            if failpoint == Some(ReconcileBatchCommitFailpoint::AfterAuditTerminalization) {
                return Err(StackError::InvalidSpec(
                    "injected reconcile commit failure after audit terminalization".to_string(),
                ));
            }

            let completed_at = if status == ReconcileSessionStatus::Active {
                None
            } else {
                Some(now)
            };
            let changed = store.conn.execute(
                "UPDATE reconcile_sessions
                 SET next_action_index = ?1, status = ?2, updated_at = ?3,
                     completed_at = ?4
                 WHERE session_id = ?5 AND stack_name = ?6 AND operation_id = ?7
                   AND action_schema_version = 2 AND actions_hash = ?8
                   AND status = 'active' AND next_action_index = ?9",
                params![
                    sqlite_usize(
                        "reconcile session",
                        session_id,
                        "next_action_index",
                        next_action_index
                    )?,
                    status.as_str(),
                    now,
                    completed_at,
                    session_id,
                    stack_name,
                    operation_id,
                    crate::reconcile::compute_actions_hash(&plan),
                    sqlite_usize(
                        "reconcile session",
                        session_id,
                        "expected_cursor",
                        expected_cursor
                    )?,
                ],
            )?;
            if changed != 1 {
                return Err(StackError::InvalidSpec(format!(
                    "reconcile session `{session_id}` cursor compare-and-swap lost"
                )));
            }

            #[cfg(test)]
            if failpoint == Some(ReconcileBatchCommitFailpoint::AfterSessionCas) {
                return Err(StackError::InvalidSpec(
                    "injected reconcile commit failure after session CAS".to_string(),
                ));
            }

            let progress_changed = if status == ReconcileSessionStatus::Active {
                store.conn.execute(
                    "UPDATE reconcile_progress SET next_action_index = ?1,
                            updated_at = datetime('now')
                     WHERE stack_name = ?2 AND operation_id = ?3
                       AND action_schema_version = 2 AND actions_hash = ?4
                       AND next_action_index = ?5",
                    params![
                        sqlite_usize(
                            "reconcile progress",
                            stack_name,
                            "next_action_index",
                            next_action_index
                        )?,
                        stack_name,
                        operation_id,
                        crate::reconcile::compute_actions_hash(&plan),
                        sqlite_usize(
                            "reconcile progress",
                            stack_name,
                            "expected_cursor",
                            expected_cursor
                        )?,
                    ],
                )?
            } else {
                store.conn.execute(
                    "DELETE FROM reconcile_progress
                     WHERE stack_name = ?1 AND operation_id = ?2
                       AND action_schema_version = 2 AND actions_hash = ?3
                       AND next_action_index = ?4",
                    params![
                        stack_name,
                        operation_id,
                        crate::reconcile::compute_actions_hash(&plan),
                        sqlite_usize(
                            "reconcile progress",
                            stack_name,
                            "expected_cursor",
                            expected_cursor
                        )?,
                    ],
                )?
            };
            if progress_changed != 1 {
                return Err(StackError::InvalidSpec(format!(
                    "reconcile progress compare-and-swap lost for session `{session_id}`"
                )));
            }

            Ok(ReconcileBatchCommit {
                next_action_index,
                status,
            })
        })
    }

    fn reconcile_commit_is_identical(
        &self,
        session_id: &str,
        stack_name: &str,
        next_action_index: usize,
        status: &ReconcileSessionStatus,
        actions: &[Action],
        outcomes: &[crate::executor::IndexedActionOutcome],
    ) -> Result<bool, StackError> {
        for (action, outcome) in actions.iter().zip(outcomes) {
            let expected_status = match outcome.result {
                crate::executor::ActionOutcomeResult::Succeeded => "completed",
                crate::executor::ActionOutcomeResult::Failed { .. } => "failed",
            };
            let expected_error = match &outcome.result {
                crate::executor::ActionOutcomeResult::Succeeded => None,
                crate::executor::ActionOutcomeResult::Failed { error } => Some(error.as_str()),
            };
            let audit = self
                .conn
                .query_row(
                    "SELECT stack_name, action_kind, service_name, replica_index, action_hash,
                            status, error_message
                     FROM reconcile_audit_log
                     WHERE session_id = ?1 AND action_index = ?2",
                    params![
                        session_id,
                        sqlite_usize(
                            "reconcile audit",
                            session_id,
                            "action_index",
                            outcome.absolute_index
                        )?,
                    ],
                    |row| {
                        Ok(PersistedAuditIdentity {
                            stack_name: row.get(0)?,
                            action_kind: row.get(1)?,
                            service_name: row.get(2)?,
                            replica_index: row.get(3)?,
                            action_hash: row.get(4)?,
                            status: row.get(5)?,
                            error_message: row.get(6)?,
                        })
                    },
                )
                .optional()?;
            if audit
                != Some(PersistedAuditIdentity {
                    stack_name: stack_name.to_string(),
                    action_kind: crate::executor::ReconcileActionKind::from_action(action)
                        .as_audit_str()
                        .to_string(),
                    service_name: action.target().service_name.clone(),
                    replica_index: i64::from(action.target().index()),
                    action_hash: crate::reconcile::compute_actions_hash(std::slice::from_ref(
                        action,
                    )),
                    status: expected_status.to_string(),
                    error_message: expected_error.map(ToString::to_string),
                })
            {
                return Ok(false);
            }
        }
        let progress = self.load_reconcile_progress(stack_name)?;
        if *status == ReconcileSessionStatus::Active {
            let session_operation: String = self.conn.query_row(
                "SELECT operation_id FROM reconcile_sessions WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )?;
            let plan = self.load_reconcile_session_actions(session_id)?;
            Ok(progress.is_some_and(|progress| {
                progress.operation_id == session_operation
                    && progress.next_action_index == next_action_index
                    && progress.actions == plan
            }))
        } else {
            Ok(progress.is_none())
        }
    }

    #[cfg(test)]
    pub(crate) fn commit_reconcile_batch_with_failpoint(
        &self,
        session_id: &str,
        stack_name: &str,
        operation_id: &str,
        expected_cursor: usize,
        actions: &[Action],
        outcomes: &[crate::executor::IndexedActionOutcome],
        failpoint: ReconcileBatchCommitFailpoint,
    ) -> Result<ReconcileBatchCommit, StackError> {
        self.commit_reconcile_batch_inner(
            session_id,
            stack_name,
            operation_id,
            expected_cursor,
            actions,
            outcomes,
            Some(failpoint),
        )
    }

    /// Load the active reconcile session for a stack, if any.
    pub fn load_active_reconcile_session(
        &self,
        stack_name: &str,
    ) -> Result<Option<ReconcileSession>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT session_id, stack_name, operation_id, status,
                    action_schema_version, actions_json, actions_hash,
                    next_action_index, total_actions, started_at, updated_at, completed_at
             FROM reconcile_sessions
             WHERE stack_name = ?1 AND status = 'active'
             ORDER BY started_at DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![stack_name])?;

        let Some(row) = rows.next()? else {
            return Ok(None);
        };

        let status_str: String = row.get(3)?;
        let session_id: String = row.get(0)?;
        let action_schema_version: i64 = row.get(4)?;
        if action_schema_version != 2 {
            return Err(StackError::InvalidSpec(format!(
                "active reconcile session `{session_id}` uses legacy action identity"
            )));
        }
        let actions_json: String = row.get(5)?;
        let stored: Vec<StoredAction> = serde_json::from_str(&actions_json).map_err(|error| {
            StackError::InvalidSpec(format!(
                "active reconcile session `{session_id}` has malformed exact actions: {error}"
            ))
        })?;
        let actions = stored
            .into_iter()
            .map(StoredAction::into_action)
            .collect::<Vec<_>>();
        let actions_hash: String = row.get(6)?;
        let next_action_index = persisted_usize(
            "reconcile session",
            &session_id,
            "next_action_index",
            row.get(7)?,
        )?;
        let total_actions = persisted_usize(
            "reconcile session",
            &session_id,
            "total_actions",
            row.get(8)?,
        )?;
        if total_actions != actions.len()
            || next_action_index > total_actions
            || actions_hash != crate::reconcile::compute_actions_hash(&actions)
        {
            return Err(StackError::InvalidSpec(format!(
                "active reconcile session `{session_id}` action metadata is inconsistent"
            )));
        }
        let completed_at: Option<i64> = row.get(11)?;

        Ok(Some(ReconcileSession {
            session_id: session_id.clone(),
            stack_name: row.get(1)?,
            operation_id: row.get(2)?,
            status: ReconcileSessionStatus::from_str(&status_str)?,
            actions_hash,
            next_action_index,
            total_actions,
            started_at: persisted_u64("reconcile session", &session_id, "started_at", row.get(9)?)?,
            updated_at: persisted_u64(
                "reconcile session",
                &session_id,
                "updated_at",
                row.get(10)?,
            )?,
            completed_at: persisted_optional_u64(
                "reconcile session",
                &session_id,
                "completed_at",
                completed_at,
            )?,
        }))
    }

    /// Load the exact replica-qualified action plan stored for a session.
    pub fn load_reconcile_session_actions(
        &self,
        session_id: &str,
    ) -> Result<Vec<Action>, StackError> {
        let row = self
            .conn
            .query_row(
                "SELECT action_schema_version, actions_json, actions_hash,
                        next_action_index, total_actions
                 FROM reconcile_sessions WHERE session_id = ?1",
                params![session_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| {
                StackError::InvalidSpec(format!("reconcile session `{session_id}` was not found"))
            })?;
        let (schema_version, actions_json, actions_hash, cursor_raw, count_raw) = row;
        if schema_version != 2 {
            return Err(StackError::InvalidSpec(format!(
                "reconcile session `{session_id}` uses legacy aggregate action identity"
            )));
        }
        let stored: Vec<StoredAction> = serde_json::from_str(&actions_json).map_err(|error| {
            StackError::InvalidSpec(format!(
                "reconcile session `{session_id}` contains legacy or malformed replica-unqualified actions: {error}"
            ))
        })?;
        let actions = stored
            .into_iter()
            .map(StoredAction::into_action)
            .collect::<Vec<_>>();
        let cursor = persisted_usize(
            "reconcile session",
            session_id,
            "next_action_index",
            cursor_raw,
        )?;
        let count = persisted_usize("reconcile session", session_id, "total_actions", count_raw)?;
        if count != actions.len()
            || cursor > count
            || actions_hash != crate::reconcile::compute_actions_hash(&actions)
        {
            return Err(StackError::InvalidSpec(format!(
                "reconcile session `{session_id}` action metadata is inconsistent"
            )));
        }
        Ok(actions)
    }

    /// Update session progress (next_action_index, status).
    #[cfg(test)]
    pub(crate) fn update_reconcile_session_progress(
        &self,
        session_id: &str,
        next_action_index: usize,
        status: &ReconcileSessionStatus,
    ) -> Result<(), StackError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let cursor = sqlite_usize(
            "reconcile session",
            session_id,
            "next_action_index",
            next_action_index,
        )?;
        let total: i64 = self.conn.query_row(
            "SELECT total_actions FROM reconcile_sessions
             WHERE session_id = ?1 AND action_schema_version = 2",
            params![session_id],
            |row| row.get(0),
        )?;
        if cursor > total {
            return Err(StackError::InvalidSpec(format!(
                "reconcile session `{session_id}` cursor {cursor} exceeds action count {total}"
            )));
        }
        if matches!(status, ReconcileSessionStatus::Completed) && cursor != total {
            return Err(StackError::InvalidSpec(format!(
                "reconcile session `{session_id}` cannot complete at cursor {cursor} of {total}"
            )));
        }
        let completed_at = if matches!(status, ReconcileSessionStatus::Active) {
            None
        } else {
            Some(sqlite_timestamp(
                "reconcile session",
                session_id,
                "completed_at",
                now,
            )?)
        };
        self.conn.execute(
            "UPDATE reconcile_sessions
             SET next_action_index = ?1, status = ?2, updated_at = ?3, completed_at = ?4
             WHERE session_id = ?5",
            params![
                cursor,
                status.as_str(),
                sqlite_timestamp("reconcile session", session_id, "updated_at", now)?,
                completed_at,
                session_id
            ],
        )?;
        Ok(())
    }

    /// Mark a session as completed.
    #[cfg(test)]
    pub(crate) fn complete_reconcile_session(&self, session_id: &str) -> Result<(), StackError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let actions = self.load_reconcile_session_actions(session_id)?;
        let cursor: i64 = self.conn.query_row(
            "SELECT next_action_index FROM reconcile_sessions WHERE session_id = ?1",
            params![session_id],
            |row| row.get(0),
        )?;
        if usize::try_from(cursor).ok() != Some(actions.len()) {
            return Err(StackError::InvalidSpec(format!(
                "reconcile session `{session_id}` cannot complete before all exact actions finish"
            )));
        }
        self.conn.execute(
            "UPDATE reconcile_sessions
             SET status = 'completed', updated_at = ?1, completed_at = ?1
             WHERE session_id = ?2",
            params![now as i64, session_id],
        )?;
        Ok(())
    }

    /// Mark a session as failed.
    #[cfg(test)]
    pub(crate) fn fail_reconcile_session(&self, session_id: &str) -> Result<(), StackError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.conn.execute(
            "UPDATE reconcile_sessions
             SET status = 'failed', updated_at = ?1, completed_at = ?1
             WHERE session_id = ?2",
            params![now as i64, session_id],
        )?;
        Ok(())
    }

    /// Supersede all active sessions for a stack in legacy persistence tests.
    #[cfg(test)]
    pub(crate) fn supersede_active_sessions(&self, stack_name: &str) -> Result<usize, StackError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let count = self.conn.execute(
            "UPDATE reconcile_sessions
             SET status = 'superseded', updated_at = ?1, completed_at = ?1
             WHERE stack_name = ?2 AND status = 'active'",
            params![now as i64, stack_name],
        )?;
        Ok(count)
    }

    /// Load recent sessions for a stack.
    pub fn list_reconcile_sessions(
        &self,
        stack_name: &str,
        limit: usize,
    ) -> Result<Vec<ReconcileSession>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT session_id, stack_name, operation_id, status,
                    actions_hash, next_action_index, total_actions,
                    started_at, updated_at, completed_at
             FROM reconcile_sessions
             WHERE stack_name = ?1
             ORDER BY started_at DESC
             LIMIT ?2",
        )?;
        let mut rows = stmt.query(params![stack_name, limit as i64])?;

        let mut sessions = Vec::new();
        while let Some(row) = rows.next()? {
            let status_str: String = row.get(3)?;
            let session_id: String = row.get(0)?;
            let completed_at: Option<i64> = row.get(9)?;
            let next_action_index = persisted_usize(
                "reconcile session",
                &session_id,
                "next_action_index",
                row.get(5)?,
            )?;
            let total_actions = persisted_usize(
                "reconcile session",
                &session_id,
                "total_actions",
                row.get(6)?,
            )?;
            let actions = self.load_reconcile_session_actions(&session_id)?;
            if total_actions != actions.len() || next_action_index > total_actions {
                return Err(StackError::InvalidSpec(format!(
                    "reconcile session `{session_id}` action metadata is inconsistent"
                )));
            }
            sessions.push(ReconcileSession {
                session_id: session_id.clone(),
                stack_name: row.get(1)?,
                operation_id: row.get(2)?,
                status: ReconcileSessionStatus::from_str(&status_str)?,
                actions_hash: row.get(4)?,
                next_action_index,
                total_actions,
                started_at: persisted_u64(
                    "reconcile session",
                    &session_id,
                    "started_at",
                    row.get(7)?,
                )?,
                updated_at: persisted_u64(
                    "reconcile session",
                    &session_id,
                    "updated_at",
                    row.get(8)?,
                )?,
                completed_at: persisted_optional_u64(
                    "reconcile session",
                    &session_id,
                    "completed_at",
                    completed_at,
                )?,
            });
        }
        Ok(sessions)
    }

    // ── Reconcile audit log ──

    /// Record the start of a reconcile action in the audit log.
    ///
    /// Returns the auto-generated row ID which should be passed to
    /// [`log_reconcile_action_complete`](Self::log_reconcile_action_complete)
    /// when the action finishes.
    #[cfg(test)]
    pub(crate) fn log_reconcile_action_start(
        &self,
        entry: &ReconcileAuditEntry,
    ) -> Result<i64, StackError> {
        let actions = self.load_reconcile_session_actions(&entry.session_id)?;
        let expected = actions.get(entry.action_index).ok_or_else(|| {
            StackError::InvalidSpec(format!(
                "reconcile audit index {} is outside session `{}` action plan",
                entry.action_index, entry.session_id
            ))
        })?;
        let expected_kind = match expected {
            Action::ServiceCreate { .. } => "service_create",
            Action::ServiceRecreate { .. } => "service_recreate",
            Action::ServiceRemove { .. } => "service_remove",
        };
        let expected_hash = crate::reconcile::compute_actions_hash(std::slice::from_ref(expected));
        let session_stack: String = self.conn.query_row(
            "SELECT stack_name FROM reconcile_sessions WHERE session_id = ?1",
            params![entry.session_id],
            |row| row.get(0),
        )?;
        if entry.stack_name != session_stack
            || entry.action_kind != expected_kind
            || &entry.target != expected.target()
            || entry.action_hash != expected_hash
        {
            return Err(StackError::InvalidSpec(format!(
                "reconcile audit identity does not match session `{}` action {}",
                entry.session_id, entry.action_index
            )));
        }
        let action_index = sqlite_usize(
            "reconcile audit",
            &entry.session_id,
            "action_index",
            entry.action_index,
        )?;
        let started_at = sqlite_timestamp(
            "reconcile audit",
            &entry.session_id,
            "started_at",
            entry.started_at,
        )?;
        let completed_at = entry
            .completed_at
            .map(|value| {
                sqlite_timestamp("reconcile audit", &entry.session_id, "completed_at", value)
            })
            .transpose()?;
        self.conn.execute(
            "INSERT INTO reconcile_audit_log (
                session_id, stack_name, action_index, action_kind,
                service_name, replica_index, action_hash, status, started_at,
                completed_at, error_message
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                entry.session_id,
                entry.stack_name,
                action_index,
                entry.action_kind,
                entry.target.service_name,
                i64::from(entry.target.index()),
                entry.action_hash,
                entry.status,
                started_at,
                completed_at,
                entry.error_message,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Mark a previously-started audit entry as completed or failed.
    ///
    /// Sets `status` to `"completed"` on success or `"failed"` when an
    /// error message is provided, and records `completed_at` as the
    /// current Unix epoch second.
    #[cfg(test)]
    pub(crate) fn log_reconcile_action_complete(
        &self,
        id: i64,
        error: Option<&str>,
    ) -> Result<(), StackError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let status = if error.is_some() {
            "failed"
        } else {
            "completed"
        };

        self.conn.execute(
            "UPDATE reconcile_audit_log
             SET status = ?1, completed_at = ?2, error_message = ?3
             WHERE id = ?4",
            params![status, now as i64, error, id],
        )?;
        Ok(())
    }

    /// Load all audit log entries for a given reconcile session, ordered by action index.
    pub fn load_audit_log_for_session(
        &self,
        session_id: &str,
    ) -> Result<Vec<ReconcileAuditEntry>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, stack_name, action_index, action_kind,
                    service_name, replica_index, action_hash, status, started_at,
                    completed_at, error_message
             FROM reconcile_audit_log
             WHERE session_id = ?1
             ORDER BY action_index ASC",
        )?;
        self.collect_audit_entries(&mut stmt, params![session_id])
    }

    /// Load the most recent audit log entries for a stack, ordered newest-first.
    ///
    /// `limit` is clamped to `[1, 1000]` to keep queries bounded.
    pub fn load_recent_audit_log(
        &self,
        stack_name: &str,
        limit: usize,
    ) -> Result<Vec<ReconcileAuditEntry>, StackError> {
        let clamped = limit.clamp(1, 1000) as i64;
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, stack_name, action_index, action_kind,
                    service_name, replica_index, action_hash, status, started_at,
                    completed_at, error_message
             FROM reconcile_audit_log
             WHERE stack_name = ?1
             ORDER BY id DESC
             LIMIT ?2",
        )?;
        self.collect_audit_entries(&mut stmt, params![stack_name, clamped])
    }

    fn collect_audit_entries(
        &self,
        stmt: &mut rusqlite::Statement<'_>,
        params: impl rusqlite::Params,
    ) -> Result<Vec<ReconcileAuditEntry>, StackError> {
        let rows = stmt.query_map(params, |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, i64>(9)?,
                row.get::<_, Option<i64>>(10)?,
                row.get::<_, Option<String>>(11)?,
            ))
        })?;

        let mut entries = Vec::new();
        for row_result in rows {
            let (
                id,
                session_id,
                stack_name,
                action_index,
                action_kind,
                service_name,
                replica_index,
                action_hash,
                status,
                started_at,
                completed_at,
                error_message,
            ) = row_result?;
            let action_index = persisted_usize(
                "reconcile audit",
                &id.to_string(),
                "action_index",
                action_index,
            )?;
            let target = ServiceReplicaKey::new(
                service_name,
                u32::try_from(replica_index).map_err(|_| {
                    StackError::InvalidSpec(format!(
                        "reconcile audit row {id} has invalid replica index {replica_index}"
                    ))
                })?,
            )?;
            let entry = ReconcileAuditEntry {
                id,
                session_id: session_id.clone(),
                stack_name,
                action_index,
                action_kind,
                target,
                action_hash,
                status,
                started_at: persisted_u64(
                    "reconcile audit",
                    &id.to_string(),
                    "started_at",
                    started_at,
                )?,
                completed_at: persisted_optional_u64(
                    "reconcile audit",
                    &id.to_string(),
                    "completed_at",
                    completed_at,
                )?,
                error_message,
            };
            let actions = self.load_reconcile_session_actions(&session_id)?;
            let session_stack: String = self.conn.query_row(
                "SELECT stack_name FROM reconcile_sessions WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )?;
            let expected = actions.get(action_index).ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "reconcile audit row {id} is outside its exact session action plan"
                ))
            })?;
            let expected_kind = match expected {
                Action::ServiceCreate { .. } => "service_create",
                Action::ServiceRecreate { .. } => "service_recreate",
                Action::ServiceRemove { .. } => "service_remove",
            };
            if entry.stack_name != session_stack
                || entry.action_kind != expected_kind
                || &entry.target != expected.target()
                || entry.action_hash
                    != crate::reconcile::compute_actions_hash(std::slice::from_ref(expected))
            {
                return Err(StackError::InvalidSpec(format!(
                    "reconcile audit row {id} identity does not match its exact session action"
                )));
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    // ── Idempotency key persistence ──

    /// Check for an existing idempotency key result.
    ///
    /// Returns `Ok(Some(record))` when the key has been used before, or
    /// `Ok(None)` when the key is fresh.
    pub fn find_idempotency_result(
        &self,
        key: &str,
    ) -> Result<Option<IdempotencyRecord>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT key, operation, request_hash, response_json, status_code, created_at, expires_at
             FROM idempotency_keys WHERE key = ?1",
        )?;
        let mut rows = stmt.query(params![key])?;

        match rows.next()? {
            Some(row) => {
                let status_raw: i64 = row.get(4)?;
                Ok(Some(IdempotencyRecord {
                    key: row.get(0)?,
                    operation: row.get(1)?,
                    request_hash: row.get(2)?,
                    response_json: row.get(3)?,
                    status_code: status_raw as u16,
                    created_at: row.get::<_, i64>(5)? as u64,
                    expires_at: row.get::<_, i64>(6)? as u64,
                }))
            }
            None => Ok(None),
        }
    }

    /// Save an idempotency key with its result.
    ///
    /// Uses upsert semantics so that concurrent callers racing on the
    /// same key converge to the first-written response.
    pub fn save_idempotency_result(&self, record: &IdempotencyRecord) -> Result<(), StackError> {
        self.conn.execute(
            "INSERT INTO idempotency_keys (key, operation, request_hash, response_json, status_code, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(key) DO UPDATE SET
                response_json = excluded.response_json,
                status_code = excluded.status_code",
            params![
                record.key,
                record.operation,
                record.request_hash,
                record.response_json,
                record.status_code as i64,
                record.created_at as i64,
                record.expires_at as i64,
            ],
        )?;
        Ok(())
    }

    /// Clean up expired idempotency keys (24h TTL).
    ///
    /// Returns the number of rows removed.
    pub fn cleanup_expired_idempotency_keys(&self) -> Result<usize, StackError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let deleted = self.conn.execute(
            "DELETE FROM idempotency_keys WHERE expires_at <= ?1",
            params![now as i64],
        )?;
        Ok(deleted)
    }

    // ── Lease persistence ──

    /// Persist or update a lease.
    pub fn save_lease(&self, lease: &Lease) -> Result<(), StackError> {
        let state_json = serde_json::to_string(&lease.state)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        self.conn.execute(
            "INSERT INTO lease_state (lease_id, sandbox_id, ttl_secs, last_heartbeat_at, state, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(lease_id) DO UPDATE SET
                sandbox_id = excluded.sandbox_id,
                ttl_secs = excluded.ttl_secs,
                last_heartbeat_at = excluded.last_heartbeat_at,
                state = excluded.state,
                updated_at = excluded.updated_at",
            params![
                lease.lease_id,
                lease.sandbox_id,
                lease.ttl_secs as i64,
                lease.last_heartbeat_at as i64,
                state_json,
                now,
                now,
            ],
        )?;
        Ok(())
    }

    /// Load a lease by ID.
    pub fn load_lease(&self, lease_id: &str) -> Result<Option<Lease>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT lease_id, sandbox_id, ttl_secs, last_heartbeat_at, state
             FROM lease_state WHERE lease_id = ?1",
        )?;
        let mut rows = stmt.query(params![lease_id])?;

        match rows.next()? {
            Some(row) => Ok(Some(Self::lease_from_row(row)?)),
            None => Ok(None),
        }
    }

    /// List all leases for a sandbox.
    pub fn list_leases_for_sandbox(&self, sandbox_id: &str) -> Result<Vec<Lease>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT lease_id, sandbox_id, ttl_secs, last_heartbeat_at, state
             FROM lease_state WHERE sandbox_id = ?1 ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map(params![sandbox_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;

        let mut leases = Vec::new();
        for row_result in rows {
            let (lease_id, sandbox_id, ttl_secs, last_heartbeat_at, state_str) = row_result?;
            let state: LeaseState = serde_json::from_str(&state_str)?;
            leases.push(Lease {
                lease_id,
                sandbox_id,
                ttl_secs: ttl_secs as u64,
                last_heartbeat_at: last_heartbeat_at as u64,
                state,
            });
        }
        Ok(leases)
    }

    /// List all leases.
    pub fn list_leases(&self) -> Result<Vec<Lease>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT lease_id, sandbox_id, ttl_secs, last_heartbeat_at, state
             FROM lease_state ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;

        let mut leases = Vec::new();
        for row_result in rows {
            let (lease_id, sandbox_id, ttl_secs, last_heartbeat_at, state_str) = row_result?;
            let state: LeaseState = serde_json::from_str(&state_str)?;
            leases.push(Lease {
                lease_id,
                sandbox_id,
                ttl_secs: ttl_secs as u64,
                last_heartbeat_at: last_heartbeat_at as u64,
                state,
            });
        }
        Ok(leases)
    }

    /// Delete a lease.
    pub fn delete_lease(&self, lease_id: &str) -> Result<(), StackError> {
        self.conn.execute(
            "DELETE FROM lease_state WHERE lease_id = ?1",
            params![lease_id],
        )?;
        Ok(())
    }

    /// Deserialize a lease from a rusqlite row.
    fn lease_from_row(row: &rusqlite::Row<'_>) -> Result<Lease, StackError> {
        let lease_id: String = row.get(0)?;
        let sandbox_id: String = row.get(1)?;
        let ttl_secs: i64 = row.get(2)?;
        let last_heartbeat_at: i64 = row.get(3)?;
        let state_str: String = row.get(4)?;

        let state: LeaseState = serde_json::from_str(&state_str)?;

        Ok(Lease {
            lease_id,
            sandbox_id,
            ttl_secs: ttl_secs as u64,
            last_heartbeat_at: last_heartbeat_at as u64,
            state,
        })
    }

    // ── Execution persistence ──

    /// Persist an execution, upserting on `execution_id`.
    pub fn save_execution(&self, execution: &Execution) -> Result<(), StackError> {
        let spec_json = serde_json::to_string(&execution.exec_spec)?;
        let state_json = serde_json::to_string(&execution.state)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        self.conn.execute(
            "INSERT INTO execution_state (execution_id, container_id, spec_json, state, exit_code, started_at, ended_at, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
             ON CONFLICT(execution_id) DO UPDATE SET
                container_id = excluded.container_id,
                spec_json = excluded.spec_json,
                state = excluded.state,
                exit_code = excluded.exit_code,
                started_at = excluded.started_at,
                ended_at = excluded.ended_at,
                updated_at = excluded.updated_at",
            params![
                execution.execution_id,
                execution.container_id,
                spec_json,
                state_json,
                execution.exit_code,
                execution.started_at.map(|v| v as i64),
                execution.ended_at.map(|v| v as i64),
                now,
            ],
        )?;
        Ok(())
    }

    /// Load an execution by its identifier.
    pub fn load_execution(&self, execution_id: &str) -> Result<Option<Execution>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT execution_id, container_id, spec_json, state, exit_code, started_at, ended_at
             FROM execution_state WHERE execution_id = ?1",
        )?;
        let mut rows = stmt.query(params![execution_id])?;

        match rows.next()? {
            Some(row) => Ok(Some(Self::execution_from_row(row)?)),
            None => Ok(None),
        }
    }

    /// List all executions ordered by creation time.
    pub fn list_executions(&self) -> Result<Vec<Execution>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT execution_id, container_id, spec_json, state, exit_code, started_at, ended_at
             FROM execution_state ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<i32>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })?;

        let mut executions = Vec::new();
        for row_result in rows {
            let (execution_id, container_id, spec_str, state_str, exit_code, started_at, ended_at) =
                row_result?;
            let exec_spec: ExecutionSpec = serde_json::from_str(&spec_str)?;
            let state: ExecutionState = serde_json::from_str(&state_str)?;
            let started_at =
                persisted_optional_u64("execution", &execution_id, "started_at", started_at)?;
            let ended_at =
                persisted_optional_u64("execution", &execution_id, "ended_at", ended_at)?;

            executions.push(Execution {
                execution_id,
                container_id,
                exec_spec,
                state,
                exit_code,
                started_at,
                ended_at,
            });
        }
        Ok(executions)
    }

    /// List all executions for a specific container.
    pub fn list_executions_for_container(
        &self,
        container_id: &str,
    ) -> Result<Vec<Execution>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT execution_id, container_id, spec_json, state, exit_code, started_at, ended_at
             FROM execution_state WHERE container_id = ?1 ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map(params![container_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<i32>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })?;

        let mut executions = Vec::new();
        for row_result in rows {
            let (execution_id, container_id, spec_str, state_str, exit_code, started_at, ended_at) =
                row_result?;
            let exec_spec: ExecutionSpec = serde_json::from_str(&spec_str)?;
            let state: ExecutionState = serde_json::from_str(&state_str)?;
            let started_at =
                persisted_optional_u64("execution", &execution_id, "started_at", started_at)?;
            let ended_at =
                persisted_optional_u64("execution", &execution_id, "ended_at", ended_at)?;

            executions.push(Execution {
                execution_id,
                container_id,
                exec_spec,
                state,
                exit_code,
                started_at,
                ended_at,
            });
        }
        Ok(executions)
    }

    /// Delete an execution by its identifier.
    pub fn delete_execution(&self, execution_id: &str) -> Result<(), StackError> {
        self.conn.execute(
            "DELETE FROM execution_state WHERE execution_id = ?1",
            params![execution_id],
        )?;
        Ok(())
    }

    /// Deserialize an execution from a rusqlite row.
    fn execution_from_row(row: &rusqlite::Row<'_>) -> Result<Execution, StackError> {
        let execution_id: String = row.get(0)?;
        let container_id: String = row.get(1)?;
        let spec_str: String = row.get(2)?;
        let state_str: String = row.get(3)?;
        let exit_code: Option<i32> = row.get(4)?;
        let started_at: Option<i64> = row.get(5)?;
        let ended_at: Option<i64> = row.get(6)?;

        let exec_spec: ExecutionSpec = serde_json::from_str(&spec_str)?;
        let state: ExecutionState = serde_json::from_str(&state_str)?;
        let started_at =
            persisted_optional_u64("execution", &execution_id, "started_at", started_at)?;
        let ended_at = persisted_optional_u64("execution", &execution_id, "ended_at", ended_at)?;

        Ok(Execution {
            execution_id,
            container_id,
            exec_spec,
            state,
            exit_code,
            started_at,
            ended_at,
        })
    }

    // ── Checkpoint persistence ──

    /// Persist a checkpoint, upserting on `checkpoint_id`.
    pub fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), StackError> {
        let class_json = serde_json::to_string(&checkpoint.class)?;
        let state_json = serde_json::to_string(&checkpoint.state)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        self.conn.execute(
            "INSERT INTO checkpoint_state (checkpoint_id, sandbox_id, parent_checkpoint_id, class, state, compatibility_fingerprint, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(checkpoint_id) DO UPDATE SET
                sandbox_id = excluded.sandbox_id,
                parent_checkpoint_id = excluded.parent_checkpoint_id,
                class = excluded.class,
                state = excluded.state,
                compatibility_fingerprint = excluded.compatibility_fingerprint,
                updated_at = excluded.updated_at",
            params![
                checkpoint.checkpoint_id,
                checkpoint.sandbox_id,
                checkpoint.parent_checkpoint_id,
                class_json,
                state_json,
                checkpoint.compatibility_fingerprint,
                checkpoint.created_at as i64,
                now,
            ],
        )?;
        Ok(())
    }

    /// Load a checkpoint by its identifier.
    pub fn load_checkpoint(&self, checkpoint_id: &str) -> Result<Option<Checkpoint>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT checkpoint_id, sandbox_id, parent_checkpoint_id, class, state, compatibility_fingerprint, created_at, updated_at
             FROM checkpoint_state WHERE checkpoint_id = ?1",
        )?;
        let mut rows = stmt.query(params![checkpoint_id])?;

        match rows.next()? {
            Some(row) => Ok(Some(Self::checkpoint_from_row(row)?)),
            None => Ok(None),
        }
    }

    /// List all checkpoints ordered by creation time.
    pub fn list_checkpoints(&self) -> Result<Vec<Checkpoint>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT checkpoint_id, sandbox_id, parent_checkpoint_id, class, state, compatibility_fingerprint, created_at, updated_at
             FROM checkpoint_state ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })?;

        let mut checkpoints = Vec::new();
        for row_result in rows {
            let (
                checkpoint_id,
                sandbox_id,
                parent_checkpoint_id,
                class_str,
                state_str,
                compatibility_fingerprint,
                created_at,
                _updated_at,
            ) = row_result?;
            let class: CheckpointClass = serde_json::from_str(&class_str)?;
            let state: CheckpointState = serde_json::from_str(&state_str)?;

            checkpoints.push(Checkpoint {
                checkpoint_id,
                sandbox_id,
                parent_checkpoint_id,
                class,
                state,
                created_at: created_at as u64,
                compatibility_fingerprint,
            });
        }
        Ok(checkpoints)
    }

    /// List checkpoints belonging to a specific sandbox.
    pub fn list_checkpoints_for_sandbox(
        &self,
        sandbox_id: &str,
    ) -> Result<Vec<Checkpoint>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT checkpoint_id, sandbox_id, parent_checkpoint_id, class, state, compatibility_fingerprint, created_at, updated_at
             FROM checkpoint_state WHERE sandbox_id = ?1 ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map(params![sandbox_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })?;

        let mut checkpoints = Vec::new();
        for row_result in rows {
            let (
                checkpoint_id,
                sandbox_id,
                parent_checkpoint_id,
                class_str,
                state_str,
                compatibility_fingerprint,
                created_at,
                _updated_at,
            ) = row_result?;
            let class: CheckpointClass = serde_json::from_str(&class_str)?;
            let state: CheckpointState = serde_json::from_str(&state_str)?;

            checkpoints.push(Checkpoint {
                checkpoint_id,
                sandbox_id,
                parent_checkpoint_id,
                class,
                state,
                created_at: created_at as u64,
                compatibility_fingerprint,
            });
        }
        Ok(checkpoints)
    }

    /// List direct children of a parent checkpoint.
    pub fn list_checkpoint_children(
        &self,
        parent_checkpoint_id: &str,
    ) -> Result<Vec<Checkpoint>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT checkpoint_id, sandbox_id, parent_checkpoint_id, class, state, compatibility_fingerprint, created_at, updated_at
             FROM checkpoint_state WHERE parent_checkpoint_id = ?1 ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map(params![parent_checkpoint_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })?;

        let mut checkpoints = Vec::new();
        for row_result in rows {
            let (
                checkpoint_id,
                sandbox_id,
                parent_checkpoint_id,
                class_str,
                state_str,
                compatibility_fingerprint,
                created_at,
                _updated_at,
            ) = row_result?;
            let class: CheckpointClass = serde_json::from_str(&class_str)?;
            let state: CheckpointState = serde_json::from_str(&state_str)?;

            checkpoints.push(Checkpoint {
                checkpoint_id,
                sandbox_id,
                parent_checkpoint_id,
                class,
                state,
                created_at: created_at as u64,
                compatibility_fingerprint,
            });
        }
        Ok(checkpoints)
    }

    /// Persist a retention tag for a checkpoint.
    pub fn save_checkpoint_retention_tag(
        &self,
        checkpoint_id: &str,
        tag: &str,
    ) -> Result<(), StackError> {
        let checkpoint_id = checkpoint_id.trim();
        if checkpoint_id.is_empty() {
            return Err(StackError::Machine {
                code: MachineErrorCode::ValidationError,
                message: "checkpoint_id cannot be empty when tagging checkpoint".to_string(),
            });
        }
        let tag = tag.trim();
        if tag.is_empty() {
            return Err(StackError::Machine {
                code: MachineErrorCode::ValidationError,
                message: "checkpoint retention tag cannot be empty".to_string(),
            });
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        self.conn.execute(
            "INSERT INTO checkpoint_retention_tags (checkpoint_id, tag, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(checkpoint_id) DO UPDATE SET
                tag = excluded.tag,
                updated_at = excluded.updated_at",
            params![checkpoint_id, tag, now, now],
        )?;
        Ok(())
    }

    /// Remove a retention tag from a checkpoint.
    pub fn delete_checkpoint_retention_tag(&self, checkpoint_id: &str) -> Result<(), StackError> {
        self.conn.execute(
            "DELETE FROM checkpoint_retention_tags WHERE checkpoint_id = ?1",
            params![checkpoint_id],
        )?;
        Ok(())
    }

    /// Load the retention tag for a checkpoint, when present.
    pub fn load_checkpoint_retention_tag(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<String>, StackError> {
        let mut stmt = self
            .conn
            .prepare("SELECT tag FROM checkpoint_retention_tags WHERE checkpoint_id = ?1")?;
        let mut rows = stmt.query(params![checkpoint_id])?;
        match rows.next()? {
            Some(row) => Ok(Some(row.get::<_, String>(0)?)),
            None => Ok(None),
        }
    }

    /// Load all checkpoint retention tags keyed by checkpoint id.
    pub fn list_checkpoint_retention_tags(&self) -> Result<HashMap<String, String>, StackError> {
        let mut stmt = self
            .conn
            .prepare("SELECT checkpoint_id, tag FROM checkpoint_retention_tags")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut tags = HashMap::new();
        for row in rows {
            let (checkpoint_id, tag) = row?;
            tags.insert(checkpoint_id, tag);
        }
        Ok(tags)
    }

    /// Evaluate effective retention state for every checkpoint.
    pub fn checkpoint_retention_state_map(
        &self,
        policy: CheckpointRetentionPolicy,
        now: u64,
    ) -> Result<HashMap<String, CheckpointRetentionState>, StackError> {
        let checkpoints = self.list_checkpoints()?;
        let tags = self.list_checkpoint_retention_tags()?;
        let plan = Self::compute_checkpoint_gc_plan(&checkpoints, &tags, policy, now);
        let age_deleted = plan.deleted_by_age;
        let count_deleted = plan.deleted_by_count;
        let lineage_deleted = plan.deleted_by_lineage;
        let age_set: std::collections::HashSet<_> = age_deleted.into_iter().collect();
        let count_set: std::collections::HashSet<_> = count_deleted.into_iter().collect();
        let lineage_set: std::collections::HashSet<_> = lineage_deleted.into_iter().collect();

        let mut states = HashMap::new();
        for checkpoint in checkpoints {
            let tag = tags.get(&checkpoint.checkpoint_id).cloned();
            let protected = tag.is_some();
            let gc_reason = if age_set.contains(&checkpoint.checkpoint_id) {
                Some(RetentionGcReason::AgeLimit)
            } else if count_set.contains(&checkpoint.checkpoint_id) {
                Some(RetentionGcReason::CountLimit)
            } else if lineage_set.contains(&checkpoint.checkpoint_id) {
                Some(RetentionGcReason::LineageCascade)
            } else {
                None
            };
            states.insert(
                checkpoint.checkpoint_id,
                CheckpointRetentionState {
                    tag,
                    protected,
                    expires_at: if protected {
                        None
                    } else {
                        Some(checkpoint.created_at.saturating_add(policy.max_age_secs))
                    },
                    gc_reason,
                },
            );
        }
        Ok(states)
    }

    /// Run checkpoint GC with an explicit policy.
    pub fn compact_checkpoints_with_policy(
        &self,
        policy: CheckpointRetentionPolicy,
    ) -> Result<CheckpointGcReport, StackError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.compact_checkpoints_with_policy_at(policy, now)
    }

    /// Run checkpoint GC with default policy.
    pub fn compact_checkpoints_default(&self) -> Result<CheckpointGcReport, StackError> {
        self.compact_checkpoints_with_policy(CheckpointRetentionPolicy::default())
    }

    pub(crate) fn compact_checkpoints_with_policy_at(
        &self,
        policy: CheckpointRetentionPolicy,
        now: u64,
    ) -> Result<CheckpointGcReport, StackError> {
        self.compact_checkpoints_with_policy_at_and_then(policy, now, |_tx, _report| Ok(()))
    }

    /// Run checkpoint GC at a fixed timestamp and execute an in-transaction callback
    /// after deletes are applied but before commit.
    pub fn compact_checkpoints_with_policy_at_and_then<F>(
        &self,
        policy: CheckpointRetentionPolicy,
        now: u64,
        on_compacted: F,
    ) -> Result<CheckpointGcReport, StackError>
    where
        F: FnOnce(&StateStore, &CheckpointGcReport) -> Result<(), StackError>,
    {
        let checkpoints = self.list_checkpoints()?;
        let tags = self.list_checkpoint_retention_tags()?;
        let plan = Self::compute_checkpoint_gc_plan(&checkpoints, &tags, policy, now);
        let deleted_by_age = plan.deleted_by_age;
        let deleted_by_count = plan.deleted_by_count;
        let deleted_by_lineage = plan.deleted_by_lineage;
        let report = CheckpointGcReport {
            deleted_by_age,
            deleted_by_count,
            deleted_by_lineage,
        };
        let to_delete: Vec<String> = report
            .deleted_by_age
            .iter()
            .chain(report.deleted_by_count.iter())
            .chain(report.deleted_by_lineage.iter())
            .cloned()
            .collect();
        if to_delete.is_empty() {
            return Ok(report);
        }
        let mut callback = Some(on_compacted);
        let callback_report = report.clone();
        self.with_immediate_transaction(move |tx| {
            for checkpoint_id in &to_delete {
                tx.delete_checkpoint(checkpoint_id)?;
            }
            if let Some(callback) = callback.take() {
                callback(tx, &callback_report)?;
            }
            Ok(())
        })?;
        Ok(report)
    }

    fn compute_checkpoint_gc_plan(
        checkpoints: &[Checkpoint],
        tags: &HashMap<String, String>,
        policy: CheckpointRetentionPolicy,
        now: u64,
    ) -> CheckpointGcReport {
        use std::collections::{HashMap, HashSet};

        let by_id: HashMap<&str, &Checkpoint> = checkpoints
            .iter()
            .map(|checkpoint| (checkpoint.checkpoint_id.as_str(), checkpoint))
            .collect();
        let mut children_by_parent: HashMap<&str, Vec<&str>> = HashMap::new();
        for checkpoint in checkpoints {
            if let Some(parent) = checkpoint.parent_checkpoint_id.as_deref() {
                children_by_parent
                    .entry(parent)
                    .or_default()
                    .push(checkpoint.checkpoint_id.as_str());
            }
        }

        // Protect tagged checkpoints and every ancestor in their lineage.
        let mut protected_ids: HashSet<&str> = HashSet::new();
        for tagged_checkpoint_id in tags.keys() {
            let mut cursor = Some(tagged_checkpoint_id.as_str());
            while let Some(current) = cursor {
                if !protected_ids.insert(current) {
                    break;
                }
                cursor = by_id
                    .get(current)
                    .and_then(|checkpoint| checkpoint.parent_checkpoint_id.as_deref());
            }
        }

        let cutoff = now.saturating_sub(policy.max_age_secs);
        let mut untagged: Vec<&Checkpoint> = checkpoints
            .iter()
            .filter(|checkpoint| !protected_ids.contains(checkpoint.checkpoint_id.as_str()))
            .collect();
        untagged.sort_by(|lhs, rhs| {
            lhs.created_at
                .cmp(&rhs.created_at)
                .then_with(|| lhs.checkpoint_id.cmp(&rhs.checkpoint_id))
        });

        let mut deleted_by_age = Vec::new();
        let mut retained_after_age = Vec::new();
        for checkpoint in untagged {
            if checkpoint.created_at <= cutoff {
                deleted_by_age.push(checkpoint.checkpoint_id.clone());
            } else {
                retained_after_age.push(checkpoint);
            }
        }

        let overflow = retained_after_age
            .len()
            .saturating_sub(policy.max_untagged_count);
        let deleted_by_count = retained_after_age
            .into_iter()
            .take(overflow)
            .map(|checkpoint| checkpoint.checkpoint_id.clone())
            .collect::<Vec<_>>();

        let mut selected_set: HashSet<String> = deleted_by_age
            .iter()
            .chain(deleted_by_count.iter())
            .cloned()
            .collect();
        let mut deleted_by_lineage = Vec::new();
        let mut stack: Vec<String> = deleted_by_age
            .iter()
            .chain(deleted_by_count.iter())
            .cloned()
            .collect();
        while let Some(current) = stack.pop() {
            if let Some(children) = children_by_parent.get(current.as_str()) {
                let mut sorted_children = children.clone();
                sorted_children.sort_unstable();
                for child in sorted_children {
                    if selected_set.insert(child.to_string()) {
                        deleted_by_lineage.push(child.to_string());
                        stack.push(child.to_string());
                    }
                }
            }
        }

        CheckpointGcReport {
            deleted_by_age,
            deleted_by_count,
            deleted_by_lineage,
        }
    }

    /// Replace file snapshot entries for a checkpoint.
    pub fn replace_checkpoint_file_entries(
        &self,
        checkpoint_id: &str,
        entries: &[CheckpointFileEntry],
    ) -> Result<(), StackError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        self.conn.execute(
            "DELETE FROM checkpoint_file_entries WHERE checkpoint_id = ?1",
            params![checkpoint_id],
        )?;

        let mut stmt = self.conn.prepare(
            "INSERT INTO checkpoint_file_entries (checkpoint_id, path, digest_sha256, size, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(checkpoint_id, path) DO UPDATE SET
                digest_sha256 = excluded.digest_sha256,
                size = excluded.size,
                updated_at = excluded.updated_at",
        )?;
        for entry in entries {
            let size_i64 = i64::try_from(entry.size).map_err(|_| StackError::Machine {
                code: MachineErrorCode::ValidationError,
                message: format!(
                    "checkpoint file entry `{}` size exceeds sqlite integer range",
                    entry.path
                ),
            })?;
            stmt.execute(params![
                checkpoint_id,
                entry.path,
                entry.digest_sha256,
                size_i64,
                now,
                now,
            ])?;
        }
        Ok(())
    }

    /// Load file snapshot entries for a checkpoint ordered by path.
    pub fn load_checkpoint_file_entries(
        &self,
        checkpoint_id: &str,
    ) -> Result<Vec<CheckpointFileEntry>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT path, digest_sha256, size
             FROM checkpoint_file_entries
             WHERE checkpoint_id = ?1
             ORDER BY path ASC",
        )?;
        let rows = stmt.query_map(params![checkpoint_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;

        let mut entries = Vec::new();
        for row in rows {
            let (path, digest_sha256, size_i64) = row?;
            let size = u64::try_from(size_i64).map_err(|_| StackError::Machine {
                code: MachineErrorCode::InternalError,
                message: format!(
                    "checkpoint file entry `{path}` has negative size {size_i64} in state store"
                ),
            })?;
            entries.push(CheckpointFileEntry {
                path,
                digest_sha256,
                size,
            });
        }
        Ok(entries)
    }

    /// Delete a checkpoint by its identifier.
    pub fn delete_checkpoint(&self, checkpoint_id: &str) -> Result<(), StackError> {
        self.conn.execute(
            "DELETE FROM checkpoint_file_entries WHERE checkpoint_id = ?1",
            params![checkpoint_id],
        )?;
        self.conn.execute(
            "DELETE FROM checkpoint_retention_tags WHERE checkpoint_id = ?1",
            params![checkpoint_id],
        )?;
        self.conn.execute(
            "DELETE FROM checkpoint_state WHERE checkpoint_id = ?1",
            params![checkpoint_id],
        )?;
        Ok(())
    }

    /// Deserialize a checkpoint from a rusqlite row.
    fn checkpoint_from_row(row: &rusqlite::Row<'_>) -> Result<Checkpoint, StackError> {
        let checkpoint_id: String = row.get(0)?;
        let sandbox_id: String = row.get(1)?;
        let parent_checkpoint_id: Option<String> = row.get(2)?;
        let class_str: String = row.get(3)?;
        let state_str: String = row.get(4)?;
        let compatibility_fingerprint: String = row.get(5)?;
        let created_at: i64 = row.get(6)?;
        let _updated_at: i64 = row.get(7)?;

        let class: CheckpointClass = serde_json::from_str(&class_str)?;
        let state: CheckpointState = serde_json::from_str(&state_str)?;

        Ok(Checkpoint {
            checkpoint_id,
            sandbox_id,
            parent_checkpoint_id,
            class,
            state,
            created_at: created_at as u64,
            compatibility_fingerprint,
        })
    }

    // ── Container persistence ──

    /// Persist a container, upserting on `container_id`.
    pub fn save_container(&self, container: &Container) -> Result<(), StackError> {
        let spec_json = serde_json::to_string(&container.container_spec)?;
        let state_json = serde_json::to_string(&container.state)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        self.conn.execute(
            "INSERT INTO container_state (container_id, sandbox_id, image_digest, spec_json, state, created_at, started_at, ended_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(container_id) DO UPDATE SET
                sandbox_id = excluded.sandbox_id,
                image_digest = excluded.image_digest,
                spec_json = excluded.spec_json,
                state = excluded.state,
                started_at = excluded.started_at,
                ended_at = excluded.ended_at,
                updated_at = excluded.updated_at",
            params![
                container.container_id,
                container.sandbox_id,
                container.image_digest,
                spec_json,
                state_json,
                container.created_at as i64,
                container.started_at.map(|v| v as i64),
                container.ended_at.map(|v| v as i64),
                now,
            ],
        )?;
        Ok(())
    }

    /// Load a container by its identifier.
    pub fn load_container(&self, container_id: &str) -> Result<Option<Container>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT container_id, sandbox_id, image_digest, spec_json, state, created_at, started_at, ended_at
             FROM container_state WHERE container_id = ?1",
        )?;
        let mut rows = stmt.query(params![container_id])?;

        match rows.next()? {
            Some(row) => Ok(Some(Self::container_from_row(row)?)),
            None => Ok(None),
        }
    }

    /// List all containers ordered by creation time.
    pub fn list_containers(&self) -> Result<Vec<Container>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT container_id, sandbox_id, image_digest, spec_json, state, created_at, started_at, ended_at
             FROM container_state ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<i64>>(7)?,
            ))
        })?;

        let mut containers = Vec::new();
        for row_result in rows {
            let (
                container_id,
                sandbox_id,
                image_digest,
                spec_str,
                state_str,
                created_at,
                started_at,
                ended_at,
            ) = row_result?;
            let container_spec: ContainerSpec = serde_json::from_str(&spec_str)?;
            let state: ContainerState = serde_json::from_str(&state_str)?;
            let created_at = persisted_u64("container", &container_id, "created_at", created_at)?;
            let started_at =
                persisted_optional_u64("container", &container_id, "started_at", started_at)?;
            let ended_at =
                persisted_optional_u64("container", &container_id, "ended_at", ended_at)?;

            containers.push(Container {
                container_id,
                sandbox_id,
                image_digest,
                container_spec,
                state,
                created_at,
                started_at,
                ended_at,
            });
        }
        Ok(containers)
    }

    /// Delete a container by its identifier.
    pub fn delete_container(&self, container_id: &str) -> Result<(), StackError> {
        self.conn.execute(
            "DELETE FROM container_state WHERE container_id = ?1",
            params![container_id],
        )?;
        Ok(())
    }

    /// Deserialize a container from a rusqlite row.
    fn container_from_row(row: &rusqlite::Row<'_>) -> Result<Container, StackError> {
        let container_id: String = row.get(0)?;
        let sandbox_id: String = row.get(1)?;
        let image_digest: String = row.get(2)?;
        let spec_str: String = row.get(3)?;
        let state_str: String = row.get(4)?;
        let created_at: i64 = row.get(5)?;
        let started_at: Option<i64> = row.get(6)?;
        let ended_at: Option<i64> = row.get(7)?;

        let container_spec: ContainerSpec = serde_json::from_str(&spec_str)?;
        let state: ContainerState = serde_json::from_str(&state_str)?;
        let created_at = persisted_u64("container", &container_id, "created_at", created_at)?;
        let started_at =
            persisted_optional_u64("container", &container_id, "started_at", started_at)?;
        let ended_at = persisted_optional_u64("container", &container_id, "ended_at", ended_at)?;

        Ok(Container {
            container_id,
            sandbox_id,
            image_digest,
            container_spec,
            state,
            created_at,
            started_at,
            ended_at,
        })
    }

    // ── Image persistence ──

    /// Persist an image record, upserting on `image_ref`.
    pub fn save_image(&self, image: &ImageRecord) -> Result<(), StackError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        self.conn.execute(
            "INSERT INTO image_state (image_ref, resolved_digest, platform, source_registry, pulled_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(image_ref) DO UPDATE SET
                resolved_digest = excluded.resolved_digest,
                platform = excluded.platform,
                source_registry = excluded.source_registry,
                pulled_at = excluded.pulled_at,
                updated_at = excluded.updated_at",
            params![
                image.image_ref,
                image.resolved_digest,
                image.platform,
                image.source_registry,
                image.pulled_at as i64,
                now,
            ],
        )?;
        Ok(())
    }

    /// Load an image by its reference string.
    pub fn load_image(&self, image_ref: &str) -> Result<Option<ImageRecord>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT image_ref, resolved_digest, platform, source_registry, pulled_at
             FROM image_state WHERE image_ref = ?1",
        )?;
        let mut rows = stmt.query(params![image_ref])?;

        match rows.next()? {
            Some(row) => Ok(Some(ImageRecord {
                image_ref: row.get(0)?,
                resolved_digest: row.get(1)?,
                platform: row.get(2)?,
                source_registry: row.get(3)?,
                pulled_at: row.get::<_, i64>(4)? as u64,
            })),
            None => Ok(None),
        }
    }

    /// List all images ordered by pull time.
    pub fn list_images(&self) -> Result<Vec<ImageRecord>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT image_ref, resolved_digest, platform, source_registry, pulled_at
             FROM image_state ORDER BY pulled_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;

        let mut images = Vec::new();
        for row_result in rows {
            let (image_ref, resolved_digest, platform, source_registry, pulled_at) = row_result?;
            images.push(ImageRecord {
                image_ref,
                resolved_digest,
                platform,
                source_registry,
                pulled_at: pulled_at as u64,
            });
        }
        Ok(images)
    }

    /// Replace all persisted image records with a new snapshot.
    pub fn replace_images(&self, images: &[ImageRecord]) -> Result<(), StackError> {
        self.conn.execute("DELETE FROM image_state", [])?;
        for image in images {
            self.save_image(image)?;
        }
        Ok(())
    }

    // ── Receipt persistence (agent-a03881b1's complete version) ──

    /// Persist a receipt for a completed mutating operation.
    pub fn save_receipt(&self, receipt: &Receipt) -> Result<(), StackError> {
        let metadata_json = serde_json::to_string(&receipt.metadata)?;
        self.conn.execute(
            "INSERT INTO receipt_state (receipt_id, operation, entity_id, entity_type, request_id, status, created_at, metadata_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(receipt_id) DO UPDATE SET
                operation = excluded.operation,
                entity_id = excluded.entity_id,
                entity_type = excluded.entity_type,
                request_id = excluded.request_id,
                status = excluded.status,
                metadata_json = excluded.metadata_json",
            params![
                receipt.receipt_id,
                receipt.operation,
                receipt.entity_id,
                receipt.entity_type,
                receipt.request_id,
                receipt.status,
                receipt.created_at as i64,
                metadata_json,
            ],
        )?;
        Ok(())
    }

    /// Load a receipt by its identifier.
    pub fn load_receipt(&self, receipt_id: &str) -> Result<Option<Receipt>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT receipt_id, operation, entity_id, entity_type, request_id, status, created_at, metadata_json
             FROM receipt_state WHERE receipt_id = ?1",
        )?;
        let mut rows = stmt.query(params![receipt_id])?;

        match rows.next()? {
            Some(row) => Ok(Some(Self::receipt_from_row(row)?)),
            None => Ok(None),
        }
    }

    /// Load a receipt by its correlating request identifier.
    pub fn load_receipt_by_request_id(
        &self,
        request_id: &str,
    ) -> Result<Option<Receipt>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT receipt_id, operation, entity_id, entity_type, request_id, status, created_at, metadata_json
             FROM receipt_state WHERE request_id = ?1
             ORDER BY created_at DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(params![request_id])?;

        match rows.next()? {
            Some(row) => Ok(Some(Self::receipt_from_row(row)?)),
            None => Ok(None),
        }
    }

    /// List all receipts for a given entity type and entity identifier.
    pub fn list_receipts_for_entity(
        &self,
        entity_type: &str,
        entity_id: &str,
    ) -> Result<Vec<Receipt>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT receipt_id, operation, entity_id, entity_type, request_id, status, created_at, metadata_json
             FROM receipt_state WHERE entity_type = ?1 AND entity_id = ?2
             ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map(params![entity_type, entity_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?;

        let mut receipts = Vec::new();
        for row_result in rows {
            let (
                receipt_id,
                operation,
                entity_id,
                entity_type,
                request_id,
                status,
                created_at,
                metadata_str,
            ) = row_result?;
            let metadata: serde_json::Value = serde_json::from_str(&metadata_str)?;
            receipts.push(Receipt {
                receipt_id,
                operation,
                entity_id,
                entity_type,
                request_id,
                status,
                created_at: created_at as u64,
                metadata,
            });
        }
        Ok(receipts)
    }

    /// List all receipts ordered by creation time.
    pub fn list_receipts(&self) -> Result<Vec<Receipt>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT receipt_id, operation, entity_id, entity_type, request_id, status, created_at, metadata_json
             FROM receipt_state
             ORDER BY created_at ASC, receipt_id ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?;

        let mut receipts = Vec::new();
        for row in rows {
            let (
                receipt_id,
                operation,
                entity_id,
                entity_type,
                request_id,
                status,
                created_at,
                metadata_str,
            ) = row?;
            let metadata: serde_json::Value = serde_json::from_str(&metadata_str)?;
            receipts.push(Receipt {
                receipt_id,
                operation,
                entity_id,
                entity_type,
                request_id,
                status,
                created_at: created_at as u64,
                metadata,
            });
        }
        Ok(receipts)
    }

    /// Delete a receipt by identifier.
    pub fn delete_receipt(&self, receipt_id: &str) -> Result<(), StackError> {
        self.conn.execute(
            "DELETE FROM receipt_state WHERE receipt_id = ?1",
            params![receipt_id],
        )?;
        Ok(())
    }

    /// Evaluate effective retention state for every receipt.
    pub fn receipt_retention_state_map(
        &self,
        policy: ReceiptRetentionPolicy,
        now: u64,
    ) -> Result<HashMap<String, ReceiptRetentionState>, StackError> {
        let receipts = self.list_receipts()?;
        let (age_deleted, count_deleted) = Self::compute_receipt_gc_plan(&receipts, policy, now);
        let age_set: std::collections::HashSet<_> = age_deleted.into_iter().collect();
        let count_set: std::collections::HashSet<_> = count_deleted.into_iter().collect();

        let mut states = HashMap::new();
        for receipt in receipts {
            let gc_reason = if age_set.contains(&receipt.receipt_id) {
                Some(RetentionGcReason::AgeLimit)
            } else if count_set.contains(&receipt.receipt_id) {
                Some(RetentionGcReason::CountLimit)
            } else {
                None
            };
            states.insert(
                receipt.receipt_id,
                ReceiptRetentionState {
                    expires_at: receipt.created_at.saturating_add(policy.max_age_secs),
                    gc_reason,
                },
            );
        }
        Ok(states)
    }

    /// Run receipt GC with an explicit policy.
    pub fn compact_receipts_with_policy(
        &self,
        policy: ReceiptRetentionPolicy,
    ) -> Result<ReceiptGcReport, StackError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.compact_receipts_with_policy_at(policy, now)
    }

    /// Run receipt GC with default policy.
    pub fn compact_receipts_default(&self) -> Result<ReceiptGcReport, StackError> {
        self.compact_receipts_with_policy(ReceiptRetentionPolicy::default())
    }

    pub(crate) fn compact_receipts_with_policy_at(
        &self,
        policy: ReceiptRetentionPolicy,
        now: u64,
    ) -> Result<ReceiptGcReport, StackError> {
        let receipts = self.list_receipts()?;
        let (deleted_by_age, deleted_by_count) =
            Self::compute_receipt_gc_plan(&receipts, policy, now);
        let to_delete: Vec<String> = deleted_by_age
            .iter()
            .chain(deleted_by_count.iter())
            .cloned()
            .collect();
        if to_delete.is_empty() {
            return Ok(ReceiptGcReport {
                deleted_by_age,
                deleted_by_count,
            });
        }

        self.with_immediate_transaction(|tx| {
            for receipt_id in &to_delete {
                tx.delete_receipt(receipt_id)?;
            }
            Ok(())
        })?;

        Ok(ReceiptGcReport {
            deleted_by_age,
            deleted_by_count,
        })
    }

    fn compute_receipt_gc_plan(
        receipts: &[Receipt],
        policy: ReceiptRetentionPolicy,
        now: u64,
    ) -> (Vec<String>, Vec<String>) {
        let cutoff = now.saturating_sub(policy.max_age_secs);
        let mut ordered: Vec<&Receipt> = receipts.iter().collect();
        ordered.sort_by(|lhs, rhs| {
            lhs.created_at
                .cmp(&rhs.created_at)
                .then_with(|| lhs.receipt_id.cmp(&rhs.receipt_id))
        });

        let mut deleted_by_age = Vec::new();
        let mut retained_after_age = Vec::new();
        for receipt in ordered {
            if receipt.created_at <= cutoff {
                deleted_by_age.push(receipt.receipt_id.clone());
            } else {
                retained_after_age.push(receipt);
            }
        }

        let overflow = retained_after_age.len().saturating_sub(policy.max_count);
        let deleted_by_count = retained_after_age
            .into_iter()
            .take(overflow)
            .map(|receipt| receipt.receipt_id.clone())
            .collect();

        (deleted_by_age, deleted_by_count)
    }

    /// Deserialize a receipt from a rusqlite row.
    fn receipt_from_row(row: &rusqlite::Row<'_>) -> Result<Receipt, StackError> {
        let receipt_id: String = row.get(0)?;
        let operation: String = row.get(1)?;
        let entity_id: String = row.get(2)?;
        let entity_type: String = row.get(3)?;
        let request_id: String = row.get(4)?;
        let status: String = row.get(5)?;
        let created_at: i64 = row.get(6)?;
        let metadata_str: String = row.get(7)?;
        let metadata: serde_json::Value = serde_json::from_str(&metadata_str)?;

        Ok(Receipt {
            receipt_id,
            operation,
            entity_id,
            entity_type,
            request_id,
            status,
            created_at: created_at as u64,
            metadata,
        })
    }

    // ── Scoped event listing ──

    /// Load events filtered by scope (entity type prefix in event type).
    ///
    /// The `scope` parameter filters events whose JSON `type` field starts
    /// with the given prefix (e.g. `"sandbox_"`, `"lease_"`, `"execution_"`,
    /// `"checkpoint_"`). Uses SQL `LIKE` on the serialized event JSON.
    pub fn load_events_by_scope(
        &self,
        stack_name: &str,
        scope: &str,
        after_id: Option<i64>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StackError> {
        let clamped_limit = limit.clamp(1, 1000) as i64;
        let cursor = after_id.unwrap_or(0);
        let like_pattern = format!("%\"type\":\"{scope}%");
        let mut stmt = self.conn.prepare(
            "SELECT id, stack_name, event_json, created_at
             FROM events
             WHERE stack_name = ?1 AND id > ?2 AND event_json LIKE ?3
             ORDER BY id ASC
             LIMIT ?4",
        )?;
        Self::collect_event_records(
            &mut stmt,
            params![stack_name, cursor, like_pattern, clamped_limit],
        )
    }

    // ── Build persistence ──

    /// Persist a build, upserting on `build_id`.
    pub fn save_build(&self, build: &Build) -> Result<(), StackError> {
        let spec_json = serde_json::to_string(&build.build_spec)?;
        let state_json = serde_json::to_string(&build.state)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        self.conn.execute(
            "INSERT INTO build_state (build_id, sandbox_id, spec_json, state, result_digest, started_at, ended_at, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
             ON CONFLICT(build_id) DO UPDATE SET
                sandbox_id = excluded.sandbox_id,
                spec_json = excluded.spec_json,
                state = excluded.state,
                result_digest = excluded.result_digest,
                started_at = excluded.started_at,
                ended_at = excluded.ended_at,
                updated_at = excluded.updated_at",
            params![
                build.build_id,
                build.sandbox_id,
                spec_json,
                state_json,
                build.result_digest,
                build.started_at as i64,
                build.ended_at.map(|v| v as i64),
                now,
            ],
        )?;
        Ok(())
    }

    /// Load a build by its identifier.
    pub fn load_build(&self, build_id: &str) -> Result<Option<Build>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT build_id, sandbox_id, spec_json, state, result_digest, started_at, ended_at
             FROM build_state WHERE build_id = ?1",
        )?;
        let mut rows = stmt.query(params![build_id])?;

        match rows.next()? {
            Some(row) => Ok(Some(Self::build_from_row(row)?)),
            None => Ok(None),
        }
    }

    /// List all builds ordered by creation time.
    pub fn list_builds(&self) -> Result<Vec<Build>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT build_id, sandbox_id, spec_json, state, result_digest, started_at, ended_at
             FROM build_state ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })?;

        let mut builds = Vec::new();
        for row_result in rows {
            let (build_id, sandbox_id, spec_str, state_str, result_digest, started_at, ended_at) =
                row_result?;
            let build_spec: BuildSpec = serde_json::from_str(&spec_str)?;
            let state: BuildState = serde_json::from_str(&state_str)?;
            let started_at = persisted_u64("build", &build_id, "started_at", started_at)?;
            let ended_at = persisted_optional_u64("build", &build_id, "ended_at", ended_at)?;

            builds.push(Build {
                build_id,
                sandbox_id,
                build_spec,
                state,
                result_digest,
                started_at,
                ended_at,
            });
        }
        Ok(builds)
    }

    /// List all builds for a specific sandbox.
    pub fn list_builds_for_sandbox(&self, sandbox_id: &str) -> Result<Vec<Build>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT build_id, sandbox_id, spec_json, state, result_digest, started_at, ended_at
             FROM build_state WHERE sandbox_id = ?1 ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map(params![sandbox_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })?;

        let mut builds = Vec::new();
        for row_result in rows {
            let (build_id, sandbox_id, spec_str, state_str, result_digest, started_at, ended_at) =
                row_result?;
            let build_spec: BuildSpec = serde_json::from_str(&spec_str)?;
            let state: BuildState = serde_json::from_str(&state_str)?;
            let started_at = persisted_u64("build", &build_id, "started_at", started_at)?;
            let ended_at = persisted_optional_u64("build", &build_id, "ended_at", ended_at)?;

            builds.push(Build {
                build_id,
                sandbox_id,
                build_spec,
                state,
                result_digest,
                started_at,
                ended_at,
            });
        }
        Ok(builds)
    }

    /// Delete a build by its identifier.
    pub fn delete_build(&self, build_id: &str) -> Result<(), StackError> {
        self.conn.execute(
            "DELETE FROM build_state WHERE build_id = ?1",
            params![build_id],
        )?;
        Ok(())
    }

    /// Deserialize a build from a rusqlite row.
    fn build_from_row(row: &rusqlite::Row<'_>) -> Result<Build, StackError> {
        let build_id: String = row.get(0)?;
        let sandbox_id: String = row.get(1)?;
        let spec_str: String = row.get(2)?;
        let state_str: String = row.get(3)?;
        let result_digest: Option<String> = row.get(4)?;
        let started_at: i64 = row.get(5)?;
        let ended_at: Option<i64> = row.get(6)?;

        let build_spec: BuildSpec = serde_json::from_str(&spec_str)?;
        let state: BuildState = serde_json::from_str(&state_str)?;
        let started_at = persisted_u64("build", &build_id, "started_at", started_at)?;
        let ended_at = persisted_optional_u64("build", &build_id, "ended_at", ended_at)?;

        Ok(Build {
            build_id,
            sandbox_id,
            build_spec,
            state,
            result_digest,
            started_at,
            ended_at,
        })
    }

    // ── Control metadata ──

    /// Get a control metadata value by key.
    pub fn get_control_metadata(&self, key: &str) -> Result<Option<String>, StackError> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM control_metadata WHERE key = ?1")?;
        let mut rows = stmt.query(params![key])?;

        match rows.next()? {
            Some(row) => {
                let value: String = row.get(0)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Set a control metadata value, upserting on key.
    pub fn set_control_metadata(&self, key: &str, value: &str) -> Result<(), StackError> {
        self.conn.execute(
            "INSERT INTO control_metadata (key, value)
             VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = datetime('now')",
            params![key, value],
        )?;
        Ok(())
    }

    /// Get the current schema version from control metadata.
    ///
    /// Missing or malformed schema metadata is rejected rather than guessed.
    pub fn schema_version(&self) -> Result<u32, StackError> {
        let value = self
            .get_control_metadata("schema_version")?
            .ok_or_else(|| StackError::InvalidSpec("missing state schema version".to_string()))?;
        value.parse().map_err(|_| {
            StackError::InvalidSpec(format!("malformed state schema version `{value}`"))
        })
    }

    /// Set the schema version in control metadata.
    pub fn set_schema_version(&self, version: u32) -> Result<(), StackError> {
        self.set_control_metadata("schema_version", &version.to_string())
    }
}
