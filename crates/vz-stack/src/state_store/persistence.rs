use super::*;

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

            sandboxes.push(Sandbox {
                sandbox_id,
                backend,
                spec,
                state,
                created_at: created_at as u64,
                updated_at: updated_at as u64,
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

        Ok(Sandbox {
            sandbox_id,
            backend,
            spec,
            state,
            created_at: created_at as u64,
            updated_at: updated_at as u64,
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
        let ports_json = serde_json::to_string(&snapshot.ports)?;
        let service_ips_json = serde_json::to_string(&snapshot.service_ips)?;
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

        let ports: HashMap<String, Vec<PublishedPort>> = serde_json::from_str(&ports_json)?;
        let service_ips: HashMap<String, String> = serde_json::from_str(&service_ips_json)?;
        let mount_tag_offsets: HashMap<String, usize> =
            serde_json::from_str(&mount_tag_offsets_json)?;

        Ok(Some(AllocatorSnapshot {
            ports,
            service_ips,
            mount_tag_offsets,
        }))
    }

    // ── Reconcile session tracking ──

    /// Create a new reconcile session.
    ///
    /// The `actions` slice is serialized into the `actions_json` column
    /// for auditability. The session struct carries the hash and cursor.
    pub fn create_reconcile_session(
        &self,
        session: &ReconcileSession,
        actions: &[Action],
    ) -> Result<(), StackError> {
        let stored_actions: Vec<StoredAction> =
            actions.iter().map(StoredAction::from_action).collect();
        let actions_json = serde_json::to_string(&stored_actions)?;

        self.conn.execute(
            "INSERT INTO reconcile_sessions (
                session_id, stack_name, operation_id, status,
                actions_json, actions_hash, next_action_index,
                total_actions, started_at, updated_at, completed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                session.session_id,
                session.stack_name,
                session.operation_id,
                session.status.as_str(),
                actions_json,
                session.actions_hash,
                session.next_action_index as i64,
                session.total_actions as i64,
                session.started_at as i64,
                session.updated_at as i64,
                session.completed_at.map(|t| t as i64),
            ],
        )?;
        Ok(())
    }

    /// Load the active reconcile session for a stack, if any.
    pub fn load_active_reconcile_session(
        &self,
        stack_name: &str,
    ) -> Result<Option<ReconcileSession>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT session_id, stack_name, operation_id, status,
                    actions_hash, next_action_index, total_actions,
                    started_at, updated_at, completed_at
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
        let completed_at: Option<i64> = row.get(9)?;

        Ok(Some(ReconcileSession {
            session_id: row.get(0)?,
            stack_name: row.get(1)?,
            operation_id: row.get(2)?,
            status: ReconcileSessionStatus::from_str(&status_str)?,
            actions_hash: row.get(4)?,
            next_action_index: row.get::<_, i64>(5)?.max(0) as usize,
            total_actions: row.get::<_, i64>(6)?.max(0) as usize,
            started_at: row.get::<_, i64>(7)?.max(0) as u64,
            updated_at: row.get::<_, i64>(8)?.max(0) as u64,
            completed_at: completed_at.map(|t| t.max(0) as u64),
        }))
    }

    /// Update session progress (next_action_index, status).
    pub fn update_reconcile_session_progress(
        &self,
        session_id: &str,
        next_action_index: usize,
        status: &ReconcileSessionStatus,
    ) -> Result<(), StackError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.conn.execute(
            "UPDATE reconcile_sessions
             SET next_action_index = ?1, status = ?2, updated_at = ?3
             WHERE session_id = ?4",
            params![
                next_action_index as i64,
                status.as_str(),
                now as i64,
                session_id
            ],
        )?;
        Ok(())
    }

    /// Mark a session as completed.
    pub fn complete_reconcile_session(&self, session_id: &str) -> Result<(), StackError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.conn.execute(
            "UPDATE reconcile_sessions
             SET status = 'completed', updated_at = ?1, completed_at = ?1
             WHERE session_id = ?2",
            params![now as i64, session_id],
        )?;
        Ok(())
    }

    /// Mark a session as failed.
    pub fn fail_reconcile_session(&self, session_id: &str) -> Result<(), StackError> {
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

    /// Supersede all active sessions for a stack (called when new apply starts).
    ///
    /// Returns the number of sessions superseded.
    pub fn supersede_active_sessions(&self, stack_name: &str) -> Result<usize, StackError> {
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
            let completed_at: Option<i64> = row.get(9)?;
            sessions.push(ReconcileSession {
                session_id: row.get(0)?,
                stack_name: row.get(1)?,
                operation_id: row.get(2)?,
                status: ReconcileSessionStatus::from_str(&status_str)?,
                actions_hash: row.get(4)?,
                next_action_index: row.get::<_, i64>(5)?.max(0) as usize,
                total_actions: row.get::<_, i64>(6)?.max(0) as usize,
                started_at: row.get::<_, i64>(7)?.max(0) as u64,
                updated_at: row.get::<_, i64>(8)?.max(0) as u64,
                completed_at: completed_at.map(|t| t.max(0) as u64),
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
    pub fn log_reconcile_action_start(
        &self,
        entry: &ReconcileAuditEntry,
    ) -> Result<i64, StackError> {
        self.conn.execute(
            "INSERT INTO reconcile_audit_log (
                session_id, stack_name, action_index, action_kind,
                service_name, action_hash, status, started_at,
                completed_at, error_message
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                entry.session_id,
                entry.stack_name,
                entry.action_index as i64,
                entry.action_kind,
                entry.service_name,
                entry.action_hash,
                entry.status,
                entry.started_at as i64,
                entry.completed_at.map(|t| t as i64),
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
    pub fn log_reconcile_action_complete(
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
                    service_name, action_hash, status, started_at,
                    completed_at, error_message
             FROM reconcile_audit_log
             WHERE session_id = ?1
             ORDER BY action_index ASC",
        )?;
        Self::collect_audit_entries(&mut stmt, params![session_id])
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
                    service_name, action_hash, status, started_at,
                    completed_at, error_message
             FROM reconcile_audit_log
             WHERE stack_name = ?1
             ORDER BY id DESC
             LIMIT ?2",
        )?;
        Self::collect_audit_entries(&mut stmt, params![stack_name, clamped])
    }

    fn collect_audit_entries(
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
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, Option<String>>(10)?,
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
                action_hash,
                status,
                started_at,
                completed_at,
                error_message,
            ) = row_result?;
            entries.push(ReconcileAuditEntry {
                id,
                session_id,
                stack_name,
                action_index: action_index.max(0) as usize,
                action_kind,
                service_name,
                action_hash,
                status,
                started_at: started_at.max(0) as u64,
                completed_at: completed_at.map(|t| t.max(0) as u64),
                error_message,
            });
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

            executions.push(Execution {
                execution_id,
                container_id,
                exec_spec,
                state,
                exit_code,
                started_at: started_at.map(|v| v as u64),
                ended_at: ended_at.map(|v| v as u64),
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

            executions.push(Execution {
                execution_id,
                container_id,
                exec_spec,
                state,
                exit_code,
                started_at: started_at.map(|v| v as u64),
                ended_at: ended_at.map(|v| v as u64),
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

        Ok(Execution {
            execution_id,
            container_id,
            exec_spec,
            state,
            exit_code,
            started_at: started_at.map(|v| v as u64),
            ended_at: ended_at.map(|v| v as u64),
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
        let checkpoints = self.list_checkpoints()?;
        let tags = self.list_checkpoint_retention_tags()?;
        let plan = Self::compute_checkpoint_gc_plan(&checkpoints, &tags, policy, now);
        let deleted_by_age = plan.deleted_by_age;
        let deleted_by_count = plan.deleted_by_count;
        let deleted_by_lineage = plan.deleted_by_lineage;
        let to_delete: Vec<String> = deleted_by_age
            .iter()
            .chain(deleted_by_count.iter())
            .chain(deleted_by_lineage.iter())
            .cloned()
            .collect();
        if to_delete.is_empty() {
            return Ok(CheckpointGcReport {
                deleted_by_age,
                deleted_by_count,
                deleted_by_lineage,
            });
        }

        self.with_immediate_transaction(|tx| {
            for checkpoint_id in &to_delete {
                tx.delete_checkpoint(checkpoint_id)?;
            }
            Ok(())
        })?;

        Ok(CheckpointGcReport {
            deleted_by_age,
            deleted_by_count,
            deleted_by_lineage,
        })
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

            containers.push(Container {
                container_id,
                sandbox_id,
                image_digest,
                container_spec,
                state,
                created_at: created_at as u64,
                started_at: started_at.map(|v| v as u64),
                ended_at: ended_at.map(|v| v as u64),
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

        Ok(Container {
            container_id,
            sandbox_id,
            image_digest,
            container_spec,
            state,
            created_at: created_at as u64,
            started_at: started_at.map(|v| v as u64),
            ended_at: ended_at.map(|v| v as u64),
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

            builds.push(Build {
                build_id,
                sandbox_id,
                build_spec,
                state,
                result_digest,
                started_at: started_at as u64,
                ended_at: ended_at.map(|v| v as u64),
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

            builds.push(Build {
                build_id,
                sandbox_id,
                build_spec,
                state,
                result_digest,
                started_at: started_at as u64,
                ended_at: ended_at.map(|v| v as u64),
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

        Ok(Build {
            build_id,
            sandbox_id,
            build_spec,
            state,
            result_digest,
            started_at: started_at as u64,
            ended_at: ended_at.map(|v| v as u64),
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
    /// Returns `1` if no schema version has been recorded.
    pub fn schema_version(&self) -> Result<u32, StackError> {
        self.get_control_metadata("schema_version")
            .map(|v| v.and_then(|s| s.parse().ok()).unwrap_or(1))
    }

    /// Set the schema version in control metadata.
    pub fn set_schema_version(&self, version: u32) -> Result<(), StackError> {
        self.set_control_metadata("schema_version", &version.to_string())
    }
}
