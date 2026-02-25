//! SQLite-backed state store for desired and observed stack state.
//!
//! Provides durable persistence for the reconciliation loop:
//! - **Desired state**: the user-specified [`StackSpec`](crate::StackSpec)
//! - **Observed state**: per-service runtime state from the reconciler
//! - **Events**: structured lifecycle events for observability

use std::collections::HashMap;
use std::path::Path;
use std::sync::mpsc;

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use vz_runtime_contract::{Sandbox, SandboxBackend, SandboxSpec, SandboxState};

use crate::StackSpec;
use crate::error::StackError;
use crate::events::{EventRecord, StackEvent};
use crate::network::PublishedPort;
use crate::reconcile::Action;

/// Observable phase of a service within a stack.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServicePhase {
    /// Service spec received, not yet acted on.
    Pending,
    /// Service container is being created.
    Creating,
    /// Service container is running.
    Running,
    /// Service container is being stopped.
    Stopping,
    /// Service container has stopped.
    Stopped,
    /// Service encountered an unrecoverable error.
    Failed,
}

/// Per-service observed state as recorded by the reconciler.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServiceObservedState {
    /// Service name within the stack.
    pub service_name: String,
    /// Current lifecycle phase.
    pub phase: ServicePhase,
    /// OCI container identifier, if assigned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,
    /// Last error message, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Whether the service is ready (health checks passing or no check defined).
    #[serde(default)]
    pub ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct HealthPollState {
    pub service_name: String,
    pub consecutive_passes: u32,
    pub consecutive_failures: u32,
    pub last_check_millis: Option<i64>,
    pub start_time_millis: Option<i64>,
}

/// Snapshot of ephemeral allocator state for crash recovery.
///
/// The executor tracks port allocations, service IPs, and mount tag
/// offsets in memory. This snapshot captures that state so it can
/// be restored after a daemon restart.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AllocatorSnapshot {
    /// Per-service allocated ports.
    pub ports: HashMap<String, Vec<PublishedPort>>,
    /// Per-service IP addresses within the stack network.
    pub service_ips: HashMap<String, String>,
    /// Per-service VirtioFS mount tag offsets.
    pub mount_tag_offsets: HashMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum StoredActionKind {
    ServiceCreate,
    ServiceRecreate,
    ServiceRemove,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredAction {
    kind: StoredActionKind,
    service_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileProgress {
    /// Stable identifier for the in-flight operation batch.
    pub operation_id: String,
    /// Next action index to execute from `actions`.
    pub next_action_index: usize,
    /// Ordered action plan persisted for restart-safe replay.
    pub actions: Vec<Action>,
}

impl StoredAction {
    fn from_action(action: &Action) -> Self {
        match action {
            Action::ServiceCreate { service_name } => Self {
                kind: StoredActionKind::ServiceCreate,
                service_name: service_name.clone(),
            },
            Action::ServiceRecreate { service_name } => Self {
                kind: StoredActionKind::ServiceRecreate,
                service_name: service_name.clone(),
            },
            Action::ServiceRemove { service_name } => Self {
                kind: StoredActionKind::ServiceRemove,
                service_name: service_name.clone(),
            },
        }
    }

    fn into_action(self) -> Action {
        match self.kind {
            StoredActionKind::ServiceCreate => Action::ServiceCreate {
                service_name: self.service_name,
            },
            StoredActionKind::ServiceRecreate => Action::ServiceRecreate {
                service_name: self.service_name,
            },
            StoredActionKind::ServiceRemove => Action::ServiceRemove {
                service_name: self.service_name,
            },
        }
    }
}

/// Durable state store backed by a single SQLite database file.
pub struct StateStore {
    conn: Connection,
    /// Optional real-time event sender for streaming subscribers.
    event_sender: Option<mpsc::Sender<StackEvent>>,
}

impl StateStore {
    /// Open or create a state store at the given path.
    pub fn open(path: &Path) -> Result<Self, StackError> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn,
            event_sender: None,
        };
        store.init_schema()?;
        Ok(store)
    }

    /// Create an in-memory state store (useful for testing).
    pub fn in_memory() -> Result<Self, StackError> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn,
            event_sender: None,
        };
        store.init_schema()?;
        Ok(store)
    }

    /// Attach an event channel sender for real-time streaming.
    ///
    /// When set, [`emit_event`](Self::emit_event) will send a clone of each
    /// event through this channel in addition to persisting it to SQLite.
    /// Sending failures (receiver dropped) are silently ignored.
    pub fn set_event_sender(&mut self, sender: mpsc::Sender<StackEvent>) {
        self.event_sender = Some(sender);
    }

    fn init_schema(&self) -> Result<(), StackError> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS desired_state (
                id INTEGER PRIMARY KEY,
                stack_name TEXT NOT NULL UNIQUE,
                spec_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS observed_state (
                id INTEGER PRIMARY KEY,
                stack_name TEXT NOT NULL,
                service_name TEXT NOT NULL,
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(stack_name, service_name)
            );

            CREATE TABLE IF NOT EXISTS service_mount_digests (
                id INTEGER PRIMARY KEY,
                stack_name TEXT NOT NULL,
                service_name TEXT NOT NULL,
                mount_digest TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(stack_name, service_name)
            );

            CREATE TABLE IF NOT EXISTS reconcile_progress (
                id INTEGER PRIMARY KEY,
                stack_name TEXT NOT NULL UNIQUE,
                operation_id TEXT NOT NULL,
                actions_json TEXT NOT NULL,
                next_action_index INTEGER NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS health_poller_state (
                stack_name TEXT NOT NULL UNIQUE,
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stack_name TEXT NOT NULL,
                event_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS sandbox_state (
                sandbox_id TEXT PRIMARY KEY,
                stack_name TEXT NOT NULL,
                state TEXT NOT NULL,
                backend TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                labels_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(stack_name)
            );
            CREATE INDEX IF NOT EXISTS idx_sandbox_stack ON sandbox_state(stack_name);

            CREATE TABLE IF NOT EXISTS allocator_state (
                stack_name TEXT PRIMARY KEY,
                ports_json TEXT NOT NULL DEFAULT '{}',
                service_ips_json TEXT NOT NULL DEFAULT '{}',
                mount_tag_offsets_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );",
        )?;
        Ok(())
    }

    /// Persist the desired stack specification.
    pub fn save_desired_state(&self, stack_name: &str, spec: &StackSpec) -> Result<(), StackError> {
        let json = serde_json::to_string(spec)?;
        self.conn.execute(
            "INSERT INTO desired_state (stack_name, spec_json)
             VALUES (?1, ?2)
             ON CONFLICT(stack_name) DO UPDATE SET
                spec_json = excluded.spec_json,
                updated_at = datetime('now')",
            params![stack_name, json],
        )?;
        Ok(())
    }

    /// Load the desired stack specification, if any.
    pub fn load_desired_state(&self, stack_name: &str) -> Result<Option<StackSpec>, StackError> {
        let mut stmt = self
            .conn
            .prepare("SELECT spec_json FROM desired_state WHERE stack_name = ?1")?;
        let mut rows = stmt.query(params![stack_name])?;

        match rows.next()? {
            Some(row) => {
                let json: String = row.get(0)?;
                let spec: StackSpec = serde_json::from_str(&json)?;
                Ok(Some(spec))
            }
            None => Ok(None),
        }
    }

    /// Persist the normalized mount plan digest for a service.
    pub fn save_service_mount_digest(
        &self,
        stack_name: &str,
        service_name: &str,
        mount_digest: &str,
    ) -> Result<(), StackError> {
        self.conn.execute(
            "INSERT INTO service_mount_digests (stack_name, service_name, mount_digest)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(stack_name, service_name) DO UPDATE SET
                mount_digest = excluded.mount_digest,
                updated_at = datetime('now')",
            params![stack_name, service_name, mount_digest],
        )?;
        Ok(())
    }

    /// Remove the persisted mount plan digest for a service.
    pub fn delete_service_mount_digest(
        &self,
        stack_name: &str,
        service_name: &str,
    ) -> Result<(), StackError> {
        self.conn.execute(
            "DELETE FROM service_mount_digests
             WHERE stack_name = ?1 AND service_name = ?2",
            params![stack_name, service_name],
        )?;
        Ok(())
    }

    /// Load all persisted service mount digests for a stack.
    pub fn load_service_mount_digests(
        &self,
        stack_name: &str,
    ) -> Result<HashMap<String, String>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT service_name, mount_digest
             FROM service_mount_digests
             WHERE stack_name = ?1",
        )?;
        let rows = stmt.query_map(params![stack_name], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut digests = HashMap::new();
        for row in rows {
            let (service_name, digest) = row?;
            digests.insert(service_name, digest);
        }
        Ok(digests)
    }

    /// Persist progress for an in-flight reconcile operation.
    pub fn save_reconcile_progress(
        &self,
        stack_name: &str,
        operation_id: &str,
        actions: &[Action],
        next_action_index: usize,
    ) -> Result<(), StackError> {
        let stored_actions: Vec<StoredAction> =
            actions.iter().map(StoredAction::from_action).collect();
        let actions_json = serde_json::to_string(&stored_actions)?;

        self.conn.execute(
            "INSERT INTO reconcile_progress (stack_name, operation_id, actions_json, next_action_index)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(stack_name) DO UPDATE SET
                operation_id = excluded.operation_id,
                actions_json = excluded.actions_json,
                next_action_index = excluded.next_action_index,
                updated_at = datetime('now')",
            params![
                stack_name,
                operation_id,
                actions_json,
                next_action_index as i64
            ],
        )?;
        Ok(())
    }

    /// Load progress for an in-flight reconcile operation.
    pub fn load_reconcile_progress(
        &self,
        stack_name: &str,
    ) -> Result<Option<ReconcileProgress>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT operation_id, actions_json, next_action_index
             FROM reconcile_progress
             WHERE stack_name = ?1",
        )?;
        let mut rows = stmt.query(params![stack_name])?;

        let Some(row) = rows.next()? else {
            return Ok(None);
        };

        let operation_id: String = row.get(0)?;
        let actions_json: String = row.get(1)?;
        let next_action_index: i64 = row.get(2)?;
        let stored_actions: Vec<StoredAction> = serde_json::from_str(&actions_json)?;
        let actions = stored_actions
            .into_iter()
            .map(StoredAction::into_action)
            .collect();

        Ok(Some(ReconcileProgress {
            operation_id,
            next_action_index: next_action_index.max(0) as usize,
            actions,
        }))
    }

    /// Clear any persisted reconcile progress for a stack.
    pub fn clear_reconcile_progress(&self, stack_name: &str) -> Result<(), StackError> {
        self.conn.execute(
            "DELETE FROM reconcile_progress WHERE stack_name = ?1",
            params![stack_name],
        )?;
        Ok(())
    }

    /// Persist observed state for a single service.
    pub fn save_observed_state(
        &self,
        stack_name: &str,
        state: &ServiceObservedState,
    ) -> Result<(), StackError> {
        let json = serde_json::to_string(state)?;
        self.conn.execute(
            "INSERT INTO observed_state (stack_name, service_name, state_json)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(stack_name, service_name) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = datetime('now')",
            params![stack_name, state.service_name, json],
        )?;
        Ok(())
    }

    /// Load all observed service states for a stack.
    pub fn load_observed_state(
        &self,
        stack_name: &str,
    ) -> Result<Vec<ServiceObservedState>, StackError> {
        let mut stmt = self
            .conn
            .prepare("SELECT state_json FROM observed_state WHERE stack_name = ?1")?;
        let rows = stmt.query_map(params![stack_name], |row| row.get::<_, String>(0))?;

        let mut states = Vec::new();
        for json_result in rows {
            let json = json_result?;
            let state: ServiceObservedState = serde_json::from_str(&json)?;
            states.push(state);
        }
        Ok(states)
    }

    /// Persist health poller checkpoint state for a stack.
    ///
    /// This captures counters and timing windows required for deterministic restart
    /// of readiness tracking.
    pub fn save_health_poller_state(
        &self,
        stack_name: &str,
        state: &HashMap<String, HealthPollState>,
    ) -> Result<(), StackError> {
        let json = serde_json::to_string(state)?;
        self.conn.execute(
            "INSERT INTO health_poller_state (stack_name, state_json)
             VALUES (?1, ?2)
             ON CONFLICT(stack_name) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = datetime('now')",
            params![stack_name, json],
        )?;
        Ok(())
    }

    /// Load persisted health poller checkpoint state for a stack.
    ///
    /// Missing rows are treated as an empty checkpoint state.
    pub fn load_health_poller_state(
        &self,
        stack_name: &str,
    ) -> Result<HashMap<String, HealthPollState>, StackError> {
        let mut stmt = self
            .conn
            .prepare("SELECT state_json FROM health_poller_state WHERE stack_name = ?1")?;

        let mut rows = stmt.query(params![stack_name])?;
        let Some(row) = rows.next()? else {
            return Ok(HashMap::new());
        };

        let state_json: String = row.get(0)?;
        let state: HashMap<String, HealthPollState> = serde_json::from_str(&state_json)?;
        Ok(state)
    }

    /// Clear persisted health poller checkpoint state for a stack.
    pub fn clear_health_poller_state(&self, stack_name: &str) -> Result<(), StackError> {
        self.conn.execute(
            "DELETE FROM health_poller_state WHERE stack_name = ?1",
            params![stack_name],
        )?;
        Ok(())
    }

    /// Append a structured event to the event log.
    ///
    /// The event is always persisted to SQLite. If a real-time event
    /// sender has been attached via [`set_event_sender`](Self::set_event_sender),
    /// a clone of the event is also pushed through the channel. Send
    /// failures (receiver dropped) are silently ignored.
    pub fn emit_event(&self, stack_name: &str, event: &StackEvent) -> Result<(), StackError> {
        let json = serde_json::to_string(event)?;
        self.conn.execute(
            "INSERT INTO events (stack_name, event_json) VALUES (?1, ?2)",
            params![stack_name, json],
        )?;
        // Push to real-time subscribers (ignore if receiver dropped).
        if let Some(ref sender) = self.event_sender {
            let _ = sender.send(event.clone());
        }
        Ok(())
    }

    /// Load all events for a stack, ordered by creation time.
    pub fn load_events(&self, stack_name: &str) -> Result<Vec<StackEvent>, StackError> {
        Ok(self
            .load_event_records(stack_name)?
            .into_iter()
            .map(|r| r.event)
            .collect())
    }

    /// Load all event records for a stack, including id and timestamp.
    pub fn load_event_records(&self, stack_name: &str) -> Result<Vec<EventRecord>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, stack_name, event_json, created_at
             FROM events WHERE stack_name = ?1
             ORDER BY id ASC",
        )?;
        Self::collect_event_records(&mut stmt, params![stack_name])
    }

    /// Load events created after a given event ID (exclusive).
    ///
    /// This enables incremental streaming: a consumer stores the last
    /// seen `EventRecord::id` and polls for new events using this method.
    pub fn load_events_since(
        &self,
        stack_name: &str,
        after_id: i64,
    ) -> Result<Vec<EventRecord>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, stack_name, event_json, created_at
             FROM events WHERE stack_name = ?1 AND id > ?2
             ORDER BY id ASC",
        )?;
        Self::collect_event_records(&mut stmt, params![stack_name, after_id])
    }

    /// Load events created after a given event ID (exclusive), capped by limit.
    ///
    /// `limit` is clamped to `[1, 1000]` to keep API pagination bounded.
    pub fn load_events_since_limited(
        &self,
        stack_name: &str,
        after_id: i64,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StackError> {
        let clamped_limit = limit.clamp(1, 1000) as i64;
        let mut stmt = self.conn.prepare(
            "SELECT id, stack_name, event_json, created_at
             FROM events WHERE stack_name = ?1 AND id > ?2
             ORDER BY id ASC
             LIMIT ?3",
        )?;
        Self::collect_event_records(&mut stmt, params![stack_name, after_id, clamped_limit])
    }

    /// Count the total number of events for a stack.
    pub fn event_count(&self, stack_name: &str) -> Result<usize, StackError> {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM events WHERE stack_name = ?1")?;
        let count: i64 = stmt.query_row(params![stack_name], |row| row.get(0))?;
        Ok(count as usize)
    }

    fn collect_event_records(
        stmt: &mut rusqlite::Statement<'_>,
        params: impl rusqlite::Params,
    ) -> Result<Vec<EventRecord>, StackError> {
        let rows = stmt.query_map(params, |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;

        let mut records = Vec::new();
        for row_result in rows {
            let (id, stack_name, json, created_at) = row_result?;
            let event: StackEvent = serde_json::from_str(&json)?;
            records.push(EventRecord {
                id,
                stack_name,
                created_at,
                event,
            });
        }
        Ok(records)
    }

    // ── Sandbox persistence ──

    /// Persist a sandbox, upserting on `sandbox_id`.
    pub fn save_sandbox(&self, sandbox: &Sandbox) -> Result<(), StackError> {
        let stack_name = sandbox
            .labels
            .get("stack_name")
            .cloned()
            .unwrap_or_default();
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
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::spec::{NetworkSpec, ServiceKind, ServiceSpec, VolumeSpec};
    use std::collections::HashMap;

    fn sample_spec() -> StackSpec {
        StackSpec {
            name: "myapp".to_string(),
            services: vec![
                ServiceSpec {
                    name: "web".to_string(),
                    kind: ServiceKind::Service,
                    image: "nginx:latest".to_string(),
                    command: None,
                    entrypoint: None,
                    environment: HashMap::from([("PORT".to_string(), "80".to_string())]),
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
                },
                ServiceSpec {
                    name: "db".to_string(),
                    kind: ServiceKind::Service,
                    image: "postgres:16".to_string(),
                    command: None,
                    entrypoint: None,
                    environment: HashMap::from([(
                        "POSTGRES_PASSWORD".to_string(),
                        "secret".to_string(),
                    )]),
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
                },
            ],
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
            disk_size_mb: None,
        }
    }

    #[test]
    fn desired_state_round_trip() {
        let store = StateStore::in_memory().unwrap();
        let spec = sample_spec();

        store.save_desired_state("myapp", &spec).unwrap();
        let loaded = store.load_desired_state("myapp").unwrap();
        assert_eq!(loaded, Some(spec));
    }

    #[test]
    fn desired_state_missing_returns_none() {
        let store = StateStore::in_memory().unwrap();
        let loaded = store.load_desired_state("nonexistent").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn desired_state_upsert_replaces() {
        let store = StateStore::in_memory().unwrap();
        let spec1 = sample_spec();

        store.save_desired_state("myapp", &spec1).unwrap();

        let spec2 = StackSpec {
            name: "myapp".to_string(),
            services: vec![],
            networks: vec![NetworkSpec {
                name: "net1".to_string(),
                driver: "bridge".to_string(),
                subnet: None,
            }],
            volumes: vec![VolumeSpec {
                name: "vol1".to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            }],
            secrets: vec![],
            disk_size_mb: None,
        };

        store.save_desired_state("myapp", &spec2).unwrap();
        let loaded = store.load_desired_state("myapp").unwrap().unwrap();
        assert_eq!(loaded, spec2);
        assert!(loaded.services.is_empty());
    }

    #[test]
    fn service_mount_digest_round_trip_and_delete() {
        let store = StateStore::in_memory().unwrap();

        store
            .save_service_mount_digest("myapp", "web", "digest-web-v1")
            .unwrap();
        store
            .save_service_mount_digest("myapp", "db", "digest-db-v1")
            .unwrap();

        let digests = store.load_service_mount_digests("myapp").unwrap();
        assert_eq!(digests.len(), 2);
        assert_eq!(digests.get("web"), Some(&"digest-web-v1".to_string()));
        assert_eq!(digests.get("db"), Some(&"digest-db-v1".to_string()));

        store
            .save_service_mount_digest("myapp", "web", "digest-web-v2")
            .unwrap();
        let digests = store.load_service_mount_digests("myapp").unwrap();
        assert_eq!(digests.get("web"), Some(&"digest-web-v2".to_string()));

        store.delete_service_mount_digest("myapp", "db").unwrap();
        let digests = store.load_service_mount_digests("myapp").unwrap();
        assert_eq!(digests.len(), 1);
        assert!(digests.get("db").is_none());
    }

    #[test]
    fn reconcile_progress_round_trip_and_clear() {
        let store = StateStore::in_memory().unwrap();
        let actions = vec![
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
            Action::ServiceCreate {
                service_name: "api".to_string(),
            },
        ];

        store
            .save_reconcile_progress("myapp", "op-1", &actions, 0)
            .unwrap();

        let progress = store.load_reconcile_progress("myapp").unwrap().unwrap();
        assert_eq!(progress.operation_id, "op-1");
        assert_eq!(progress.next_action_index, 0);
        assert_eq!(progress.actions, actions);

        store
            .save_reconcile_progress("myapp", "op-1", &progress.actions, 1)
            .unwrap();
        let updated = store.load_reconcile_progress("myapp").unwrap().unwrap();
        assert_eq!(updated.next_action_index, 1);
        assert_eq!(updated.actions.len(), 2);

        store.clear_reconcile_progress("myapp").unwrap();
        assert!(store.load_reconcile_progress("myapp").unwrap().is_none());
    }

    #[test]
    fn observed_state_round_trip() {
        let store = StateStore::in_memory().unwrap();

        let state1 = ServiceObservedState {
            service_name: "web".to_string(),
            phase: ServicePhase::Running,
            container_id: Some("ctr-abc".to_string()),
            last_error: None,
            ready: true,
        };

        let state2 = ServiceObservedState {
            service_name: "db".to_string(),
            phase: ServicePhase::Pending,
            container_id: None,
            last_error: None,
            ready: false,
        };

        store.save_observed_state("myapp", &state1).unwrap();
        store.save_observed_state("myapp", &state2).unwrap();

        let states = store.load_observed_state("myapp").unwrap();
        assert_eq!(states.len(), 2);
        assert!(states.iter().any(|s| s.service_name == "web"));
        assert!(states.iter().any(|s| s.service_name == "db"));
    }

    #[test]
    fn observed_state_upsert_updates_service() {
        let store = StateStore::in_memory().unwrap();

        let initial = ServiceObservedState {
            service_name: "web".to_string(),
            phase: ServicePhase::Creating,
            container_id: None,
            last_error: None,
            ready: false,
        };

        store.save_observed_state("myapp", &initial).unwrap();

        let updated = ServiceObservedState {
            service_name: "web".to_string(),
            phase: ServicePhase::Running,
            container_id: Some("ctr-xyz".to_string()),
            last_error: None,
            ready: true,
        };

        store.save_observed_state("myapp", &updated).unwrap();

        let states = store.load_observed_state("myapp").unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].phase, ServicePhase::Running);
        assert_eq!(states[0].container_id, Some("ctr-xyz".to_string()));
    }

    #[test]
    fn observed_state_empty_returns_empty_vec() {
        let store = StateStore::in_memory().unwrap();
        let states = store.load_observed_state("empty").unwrap();
        assert!(states.is_empty());
    }

    #[test]
    fn health_poller_state_round_trip_and_clear() {
        let store = StateStore::in_memory().unwrap();
        let mut state = HashMap::new();
        state.insert(
            "web".to_string(),
            HealthPollState {
                service_name: "web".to_string(),
                consecutive_passes: 2,
                consecutive_failures: 1,
                last_check_millis: Some(1_700_000_000_000),
                start_time_millis: Some(1_700_000_000_123),
            },
        );

        store.save_health_poller_state("myapp", &state).unwrap();
        let loaded = store.load_health_poller_state("myapp").unwrap();
        assert_eq!(loaded.get("web").unwrap(), state.get("web").unwrap());

        store.clear_health_poller_state("myapp").unwrap();
        let cleared = store.load_health_poller_state("myapp").unwrap();
        assert!(cleared.is_empty());
    }

    #[test]
    fn events_emit_and_load() {
        let store = StateStore::in_memory().unwrap();

        store
            .emit_event(
                "myapp",
                &StackEvent::StackApplyStarted {
                    stack_name: "myapp".to_string(),
                    services_count: 2,
                },
            )
            .unwrap();

        store
            .emit_event(
                "myapp",
                &StackEvent::StackApplyCompleted {
                    stack_name: "myapp".to_string(),
                    succeeded: 2,
                    failed: 0,
                },
            )
            .unwrap();

        let events = store.load_events("myapp").unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], StackEvent::StackApplyStarted { .. }));
        assert!(matches!(events[1], StackEvent::StackApplyCompleted { .. }));
    }

    #[test]
    fn events_empty_returns_empty_vec() {
        let store = StateStore::in_memory().unwrap();
        let events = store.load_events("empty").unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn events_scoped_by_stack_name() {
        let store = StateStore::in_memory().unwrap();

        store
            .emit_event(
                "app1",
                &StackEvent::StackApplyStarted {
                    stack_name: "app1".to_string(),
                    services_count: 1,
                },
            )
            .unwrap();

        store
            .emit_event(
                "app2",
                &StackEvent::StackApplyStarted {
                    stack_name: "app2".to_string(),
                    services_count: 5,
                },
            )
            .unwrap();

        let app1_events = store.load_events("app1").unwrap();
        assert_eq!(app1_events.len(), 1);
        let app2_events = store.load_events("app2").unwrap();
        assert_eq!(app2_events.len(), 1);
    }

    #[test]
    fn multiple_stacks_isolated() {
        let store = StateStore::in_memory().unwrap();

        let spec1 = StackSpec {
            name: "app1".to_string(),
            services: vec![],
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
            disk_size_mb: None,
        };
        let spec2 = StackSpec {
            name: "app2".to_string(),
            services: vec![],
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
            disk_size_mb: None,
        };

        store.save_desired_state("app1", &spec1).unwrap();
        store.save_desired_state("app2", &spec2).unwrap();

        let loaded1 = store.load_desired_state("app1").unwrap().unwrap();
        let loaded2 = store.load_desired_state("app2").unwrap().unwrap();

        assert_eq!(loaded1.name, "app1");
        assert_eq!(loaded2.name, "app2");
    }

    // ── B17: Event pipeline tests ──

    #[test]
    fn event_records_include_id_and_timestamp() {
        let store = StateStore::in_memory().unwrap();

        store
            .emit_event(
                "myapp",
                &StackEvent::ServiceCreating {
                    stack_name: "myapp".to_string(),
                    service_name: "web".to_string(),
                },
            )
            .unwrap();

        let records = store.load_event_records("myapp").unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].id > 0);
        assert!(!records[0].created_at.is_empty());
        assert_eq!(records[0].stack_name, "myapp");
        assert!(matches!(
            records[0].event,
            StackEvent::ServiceCreating { .. }
        ));
    }

    #[test]
    fn load_events_since_returns_only_newer_events() {
        let store = StateStore::in_memory().unwrap();

        store
            .emit_event(
                "myapp",
                &StackEvent::StackApplyStarted {
                    stack_name: "myapp".to_string(),
                    services_count: 1,
                },
            )
            .unwrap();

        store
            .emit_event(
                "myapp",
                &StackEvent::ServiceCreating {
                    stack_name: "myapp".to_string(),
                    service_name: "web".to_string(),
                },
            )
            .unwrap();

        store
            .emit_event(
                "myapp",
                &StackEvent::ServiceReady {
                    stack_name: "myapp".to_string(),
                    service_name: "web".to_string(),
                    runtime_id: "ctr-1".to_string(),
                },
            )
            .unwrap();

        let all = store.load_event_records("myapp").unwrap();
        assert_eq!(all.len(), 3);

        // Stream from after the first event.
        let cursor = all[0].id;
        let newer = store.load_events_since("myapp", cursor).unwrap();
        assert_eq!(newer.len(), 2);
        assert!(matches!(newer[0].event, StackEvent::ServiceCreating { .. }));
        assert!(matches!(newer[1].event, StackEvent::ServiceReady { .. }));

        // Stream from after the second event.
        let cursor2 = newer[0].id;
        let newest = store.load_events_since("myapp", cursor2).unwrap();
        assert_eq!(newest.len(), 1);
        assert!(matches!(newest[0].event, StackEvent::ServiceReady { .. }));
    }

    #[test]
    fn load_events_since_with_zero_returns_all() {
        let store = StateStore::in_memory().unwrap();

        store
            .emit_event(
                "myapp",
                &StackEvent::StackApplyStarted {
                    stack_name: "myapp".to_string(),
                    services_count: 1,
                },
            )
            .unwrap();

        let all = store.load_events_since("myapp", 0).unwrap();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn load_events_since_with_future_cursor_returns_empty() {
        let store = StateStore::in_memory().unwrap();

        store
            .emit_event(
                "myapp",
                &StackEvent::StackApplyStarted {
                    stack_name: "myapp".to_string(),
                    services_count: 1,
                },
            )
            .unwrap();

        let empty = store.load_events_since("myapp", 999_999).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn load_events_since_limited_applies_limit_and_order() {
        let store = StateStore::in_memory().unwrap();
        for index in 0..3 {
            store
                .emit_event(
                    "myapp",
                    &StackEvent::ServiceCreating {
                        stack_name: "myapp".to_string(),
                        service_name: format!("svc-{index}"),
                    },
                )
                .unwrap();
        }

        let first_page = store.load_events_since_limited("myapp", 0, 2).unwrap();
        assert_eq!(first_page.len(), 2);
        assert!(first_page[0].id < first_page[1].id);

        let second_page = store
            .load_events_since_limited("myapp", first_page[1].id, 2)
            .unwrap();
        assert_eq!(second_page.len(), 1);
        assert!(second_page[0].id > first_page[1].id);
    }

    #[test]
    fn event_count_returns_correct_total() {
        let store = StateStore::in_memory().unwrap();

        assert_eq!(store.event_count("myapp").unwrap(), 0);

        store
            .emit_event(
                "myapp",
                &StackEvent::StackApplyStarted {
                    stack_name: "myapp".to_string(),
                    services_count: 1,
                },
            )
            .unwrap();

        store
            .emit_event(
                "myapp",
                &StackEvent::StackApplyCompleted {
                    stack_name: "myapp".to_string(),
                    succeeded: 1,
                    failed: 0,
                },
            )
            .unwrap();

        assert_eq!(store.event_count("myapp").unwrap(), 2);
        assert_eq!(store.event_count("other").unwrap(), 0);
    }

    #[test]
    fn event_records_ids_are_monotonically_increasing() {
        let store = StateStore::in_memory().unwrap();

        for i in 0..5 {
            store
                .emit_event(
                    "myapp",
                    &StackEvent::ServiceCreating {
                        stack_name: "myapp".to_string(),
                        service_name: format!("svc-{i}"),
                    },
                )
                .unwrap();
        }

        let records = store.load_event_records("myapp").unwrap();
        assert_eq!(records.len(), 5);
        for window in records.windows(2) {
            assert!(window[1].id > window[0].id);
        }
    }

    #[test]
    fn new_event_variants_persist_and_load() {
        let store = StateStore::in_memory().unwrap();

        let events = vec![
            StackEvent::ServiceStopping {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
            StackEvent::ServiceStopped {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
                exit_code: 137,
            },
            StackEvent::PortConflict {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
                port: 8080,
            },
            StackEvent::VolumeCreated {
                stack_name: "myapp".to_string(),
                volume_name: "dbdata".to_string(),
            },
            StackEvent::StackDestroyed {
                stack_name: "myapp".to_string(),
            },
        ];

        for event in &events {
            store.emit_event("myapp", event).unwrap();
        }

        let loaded = store.load_events("myapp").unwrap();
        assert_eq!(loaded, events);
    }

    // ── Real-time event streaming tests ──

    #[test]
    fn emit_event_sends_to_channel() {
        use std::sync::mpsc;

        let mut store = StateStore::in_memory().unwrap();
        let (tx, rx) = mpsc::channel();
        store.set_event_sender(tx);

        store
            .emit_event(
                "test",
                &StackEvent::StackDestroyed {
                    stack_name: "test".to_string(),
                },
            )
            .unwrap();

        let received = rx.try_recv().unwrap();
        assert!(matches!(received, StackEvent::StackDestroyed { .. }));
    }

    #[test]
    fn emit_event_without_sender_works() {
        let store = StateStore::in_memory().unwrap();
        // No sender set — should not error.
        store
            .emit_event(
                "test",
                &StackEvent::StackDestroyed {
                    stack_name: "test".to_string(),
                },
            )
            .unwrap();
    }

    #[test]
    fn emit_event_ignores_dropped_receiver() {
        use std::sync::mpsc;

        let mut store = StateStore::in_memory().unwrap();
        let (tx, rx) = mpsc::channel();
        store.set_event_sender(tx);

        // Drop the receiver so sends fail.
        drop(rx);

        // Should not error even though receiver is gone.
        store
            .emit_event(
                "test",
                &StackEvent::StackDestroyed {
                    stack_name: "test".to_string(),
                },
            )
            .unwrap();

        // Event should still be persisted to SQLite.
        let events = store.load_events("test").unwrap();
        assert_eq!(events.len(), 1);
    }

    // ── Sandbox persistence tests ──

    fn sample_sandbox(id: &str, stack_name: &str) -> Sandbox {
        use std::collections::BTreeMap;
        let mut labels = BTreeMap::new();
        labels.insert("stack_name".to_string(), stack_name.to_string());
        Sandbox {
            sandbox_id: id.to_string(),
            backend: SandboxBackend::MacosVz,
            spec: SandboxSpec::default(),
            state: SandboxState::Creating,
            created_at: 1_700_000_000,
            updated_at: 1_700_000_000,
            labels,
        }
    }

    #[test]
    fn sandbox_round_trip() {
        let store = StateStore::in_memory().unwrap();
        let sandbox = sample_sandbox("sb-1", "myapp");

        store.save_sandbox(&sandbox).unwrap();
        let loaded = store.load_sandbox("sb-1").unwrap().unwrap();
        assert_eq!(loaded, sandbox);
    }

    #[test]
    fn sandbox_for_stack_lookup() {
        let store = StateStore::in_memory().unwrap();
        let sandbox = sample_sandbox("sb-2", "myapp");

        store.save_sandbox(&sandbox).unwrap();
        let loaded = store.load_sandbox_for_stack("myapp").unwrap().unwrap();
        assert_eq!(loaded.sandbox_id, "sb-2");
    }

    #[test]
    fn sandbox_list_returns_all() {
        let store = StateStore::in_memory().unwrap();
        let sb1 = sample_sandbox("sb-a", "app1");
        let mut sb2 = sample_sandbox("sb-b", "app2");
        sb2.created_at = 1_700_000_001;

        store.save_sandbox(&sb1).unwrap();
        store.save_sandbox(&sb2).unwrap();

        let all = store.list_sandboxes().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn sandbox_delete_removes() {
        let store = StateStore::in_memory().unwrap();
        let sandbox = sample_sandbox("sb-del", "myapp");

        store.save_sandbox(&sandbox).unwrap();
        store.delete_sandbox("sb-del").unwrap();
        let loaded = store.load_sandbox("sb-del").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn sandbox_upsert_updates_state() {
        let store = StateStore::in_memory().unwrap();
        let mut sandbox = sample_sandbox("sb-up", "myapp");

        store.save_sandbox(&sandbox).unwrap();

        sandbox.state = SandboxState::Ready;
        sandbox.updated_at = 1_700_000_100;
        store.save_sandbox(&sandbox).unwrap();

        let loaded = store.load_sandbox("sb-up").unwrap().unwrap();
        assert_eq!(loaded.state, SandboxState::Ready);
        assert_eq!(loaded.updated_at, 1_700_000_100);
    }

    #[test]
    fn allocator_state_round_trip() {
        let store = StateStore::in_memory().unwrap();

        let snapshot = AllocatorSnapshot {
            ports: HashMap::from([(
                "web".to_string(),
                vec![PublishedPort {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: 8080,
                }],
            )]),
            service_ips: HashMap::from([("web".to_string(), "10.0.0.2".to_string())]),
            mount_tag_offsets: HashMap::from([("web".to_string(), 3)]),
        };

        store.save_allocator_state("myapp", &snapshot).unwrap();
        let loaded = store.load_allocator_state("myapp").unwrap().unwrap();
        assert_eq!(loaded, snapshot);
    }
}
