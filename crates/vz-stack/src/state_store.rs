//! SQLite-backed state store for desired and observed stack state.
//!
//! Provides durable persistence for the reconciliation loop:
//! - **Desired state**: the user-specified [`StackSpec`](crate::StackSpec)
//! - **Observed state**: per-service runtime state from the reconciler
//! - **Events**: structured lifecycle events for observability

use std::collections::HashMap;
use std::path::Path;
use std::sync::mpsc;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use vz_runtime_contract::{
    Checkpoint, CheckpointClass, CheckpointState, Execution, ExecutionSpec, ExecutionState, Lease,
    LeaseState, Sandbox, SandboxBackend, SandboxSpec, SandboxState,
};

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

/// Metadata for a reconciliation session, enabling deterministic resume.
///
/// Each apply loop creates a session that tracks the full action plan,
/// a hash for identity, and a cursor for crash-safe resume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcileSession {
    /// Unique session identifier (e.g. `rs-{timestamp_nanos}`).
    pub session_id: String,
    /// Stack this session belongs to.
    pub stack_name: String,
    /// Correlates to the orchestrator operation batch.
    pub operation_id: String,
    /// Current session lifecycle status.
    pub status: ReconcileSessionStatus,
    /// Deterministic hash of the action list for identity comparison.
    pub actions_hash: String,
    /// Next action index to execute (resume cursor).
    pub next_action_index: usize,
    /// Total number of actions in the plan.
    pub total_actions: usize,
    /// Unix epoch seconds when the session was created.
    pub started_at: u64,
    /// Unix epoch seconds of the last progress update.
    pub updated_at: u64,
    /// Unix epoch seconds when the session completed (if terminal).
    pub completed_at: Option<u64>,
}

/// Status of a reconcile session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReconcileSessionStatus {
    /// Session is actively applying actions.
    Active,
    /// All actions completed successfully.
    Completed,
    /// Session failed during apply.
    Failed,
    /// Session was superseded by a newer session.
    Superseded,
}

impl ReconcileSessionStatus {
    /// Serialize to the string stored in SQLite.
    fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Superseded => "superseded",
        }
    }

    /// Deserialize from the string stored in SQLite.
    fn from_str(s: &str) -> Result<Self, StackError> {
        match s {
            "active" => Ok(Self::Active),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "superseded" => Ok(Self::Superseded),
            other => Err(StackError::InvalidSpec(format!(
                "unknown reconcile session status: {other}"
            ))),
        }
    }
}

/// Record of a previously-executed idempotent operation.
///
/// Cached responses allow clients to safely retry mutating API calls
/// without duplicating side effects. Each record includes a hash of
/// the original request body so that key reuse with different
/// parameters can be detected and rejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    /// Client-provided idempotency key.
    pub key: String,
    /// Name of the API operation (e.g. `"create_sandbox"`).
    pub operation: String,
    /// Hash of the original request body for conflict detection.
    pub request_hash: String,
    /// JSON-serialized response body returned to the original caller.
    pub response_json: String,
    /// HTTP status code of the original response.
    pub status_code: u16,
    /// Unix epoch seconds when this record was created.
    pub created_at: u64,
    /// Unix epoch seconds after which this record may be garbage-collected.
    pub expires_at: u64,
}

/// Idempotency key time-to-live: 24 hours.
pub const IDEMPOTENCY_TTL_SECS: u64 = 86_400;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)] // serde-serialized names; renaming would break stored data
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
            );

            CREATE TABLE IF NOT EXISTS reconcile_sessions (
                session_id TEXT PRIMARY KEY,
                stack_name TEXT NOT NULL,
                operation_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                actions_json TEXT NOT NULL,
                actions_hash TEXT NOT NULL,
                next_action_index INTEGER NOT NULL DEFAULT 0,
                total_actions INTEGER NOT NULL,
                started_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_reconcile_session_stack ON reconcile_sessions(stack_name);
            CREATE INDEX IF NOT EXISTS idx_reconcile_session_status ON reconcile_sessions(status);

            CREATE TABLE IF NOT EXISTS idempotency_keys (
                key TEXT PRIMARY KEY,
                operation TEXT NOT NULL,
                request_hash TEXT NOT NULL,
                response_json TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON idempotency_keys(expires_at);

            CREATE TABLE IF NOT EXISTS lease_state (
                lease_id TEXT PRIMARY KEY,
                sandbox_id TEXT NOT NULL,
                ttl_secs INTEGER NOT NULL,
                last_heartbeat_at INTEGER NOT NULL,
                state TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_lease_sandbox ON lease_state(sandbox_id);

            CREATE TABLE IF NOT EXISTS execution_state (
                execution_id TEXT PRIMARY KEY,
                container_id TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                state TEXT NOT NULL,
                exit_code INTEGER,
                started_at INTEGER,
                ended_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_execution_container ON execution_state(container_id);

            CREATE TABLE IF NOT EXISTS checkpoint_state (
                checkpoint_id TEXT PRIMARY KEY,
                sandbox_id TEXT NOT NULL,
                parent_checkpoint_id TEXT,
                class TEXT NOT NULL,
                state TEXT NOT NULL,
                compatibility_fingerprint TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_checkpoint_sandbox ON checkpoint_state(sandbox_id);
            CREATE INDEX IF NOT EXISTS idx_checkpoint_parent ON checkpoint_state(parent_checkpoint_id);",
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
    #[cfg(test)]
    pub(crate) fn save_health_poller_state(
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
    pub(crate) fn load_health_poller_state(
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
    #[cfg(test)]
    pub(crate) fn clear_health_poller_state(&self, stack_name: &str) -> Result<(), StackError> {
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

    /// Delete a checkpoint by its identifier.
    pub fn delete_checkpoint(&self, checkpoint_id: &str) -> Result<(), StackError> {
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

    // ── Reconcile session tests ──

    fn sample_session(id: &str, stack: &str) -> ReconcileSession {
        ReconcileSession {
            session_id: id.to_string(),
            stack_name: stack.to_string(),
            operation_id: "op-1".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: "abcdef0123456789".to_string(),
            next_action_index: 0,
            total_actions: 2,
            started_at: 1_700_000_000,
            updated_at: 1_700_000_000,
            completed_at: None,
        }
    }

    fn sample_actions() -> Vec<Action> {
        vec![
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
        ]
    }

    #[test]
    fn reconcile_session_create_and_load_active() {
        let store = StateStore::in_memory().unwrap();
        let session = sample_session("rs-1", "myapp");
        let actions = sample_actions();

        store.create_reconcile_session(&session, &actions).unwrap();

        let loaded = store
            .load_active_reconcile_session("myapp")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.session_id, "rs-1");
        assert_eq!(loaded.stack_name, "myapp");
        assert_eq!(loaded.status, ReconcileSessionStatus::Active);
        assert_eq!(loaded.actions_hash, "abcdef0123456789");
        assert_eq!(loaded.next_action_index, 0);
        assert_eq!(loaded.total_actions, 2);
    }

    #[test]
    fn reconcile_session_update_progress() {
        let store = StateStore::in_memory().unwrap();
        let session = sample_session("rs-2", "myapp");
        store
            .create_reconcile_session(&session, &sample_actions())
            .unwrap();

        store
            .update_reconcile_session_progress("rs-2", 1, &ReconcileSessionStatus::Active)
            .unwrap();

        let loaded = store
            .load_active_reconcile_session("myapp")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.next_action_index, 1);
        assert_eq!(loaded.status, ReconcileSessionStatus::Active);
    }

    #[test]
    fn reconcile_session_complete() {
        let store = StateStore::in_memory().unwrap();
        let session = sample_session("rs-3", "myapp");
        store
            .create_reconcile_session(&session, &sample_actions())
            .unwrap();

        store.complete_reconcile_session("rs-3").unwrap();

        // Active load should return None since it's completed now.
        let active = store.load_active_reconcile_session("myapp").unwrap();
        assert!(active.is_none());

        // List should show it as completed.
        let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].status, ReconcileSessionStatus::Completed);
        assert!(sessions[0].completed_at.is_some());
    }

    #[test]
    fn reconcile_session_fail() {
        let store = StateStore::in_memory().unwrap();
        let session = sample_session("rs-4", "myapp");
        store
            .create_reconcile_session(&session, &sample_actions())
            .unwrap();

        store.fail_reconcile_session("rs-4").unwrap();

        let active = store.load_active_reconcile_session("myapp").unwrap();
        assert!(active.is_none());

        let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].status, ReconcileSessionStatus::Failed);
        assert!(sessions[0].completed_at.is_some());
    }

    #[test]
    fn reconcile_session_supersede_active() {
        let store = StateStore::in_memory().unwrap();

        let session1 = sample_session("rs-5", "myapp");
        store
            .create_reconcile_session(&session1, &sample_actions())
            .unwrap();

        let count = store.supersede_active_sessions("myapp").unwrap();
        assert_eq!(count, 1);

        let active = store.load_active_reconcile_session("myapp").unwrap();
        assert!(active.is_none());

        let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].status, ReconcileSessionStatus::Superseded);
    }

    #[test]
    fn reconcile_session_list_respects_limit_and_ordering() {
        let store = StateStore::in_memory().unwrap();

        for i in 0..5 {
            let mut session = sample_session(&format!("rs-{i}"), "myapp");
            session.started_at = 1_700_000_000 + i as u64;
            session.updated_at = session.started_at;
            store.complete_reconcile_session(&format!("rs-{i}")).ok();
            store
                .create_reconcile_session(&session, &sample_actions())
                .unwrap();
            store
                .complete_reconcile_session(&format!("rs-{i}"))
                .unwrap();
        }

        let all = store.list_reconcile_sessions("myapp", 10).unwrap();
        assert_eq!(all.len(), 5);
        // Ordered by started_at DESC.
        assert!(all[0].started_at >= all[1].started_at);

        let limited = store.list_reconcile_sessions("myapp", 2).unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn reconcile_session_no_active_returns_none() {
        let store = StateStore::in_memory().unwrap();
        let active = store.load_active_reconcile_session("nonexistent").unwrap();
        assert!(active.is_none());
    }

    #[test]
    fn reconcile_session_stacks_are_isolated() {
        let store = StateStore::in_memory().unwrap();

        let s1 = sample_session("rs-a1", "app1");
        let s2 = sample_session("rs-b1", "app2");
        store
            .create_reconcile_session(&s1, &sample_actions())
            .unwrap();
        store
            .create_reconcile_session(&s2, &sample_actions())
            .unwrap();

        let active1 = store
            .load_active_reconcile_session("app1")
            .unwrap()
            .unwrap();
        assert_eq!(active1.session_id, "rs-a1");

        let active2 = store
            .load_active_reconcile_session("app2")
            .unwrap()
            .unwrap();
        assert_eq!(active2.session_id, "rs-b1");

        // Supersede only app1.
        store.supersede_active_sessions("app1").unwrap();
        assert!(
            store
                .load_active_reconcile_session("app1")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .load_active_reconcile_session("app2")
                .unwrap()
                .is_some()
        );
    }

    // ── Idempotency key persistence tests ──

    fn sample_idempotency_record(key: &str) -> IdempotencyRecord {
        IdempotencyRecord {
            key: key.to_string(),
            operation: "create_sandbox".to_string(),
            request_hash: "abc123".to_string(),
            response_json: r#"{"sandbox_id":"sbx-1"}"#.to_string(),
            status_code: 201,
            created_at: 1_700_000_000,
            expires_at: 1_700_000_000 + IDEMPOTENCY_TTL_SECS,
        }
    }

    #[test]
    fn idempotency_save_and_find_round_trip() {
        let store = StateStore::in_memory().unwrap();
        let record = sample_idempotency_record("ik-1");

        store.save_idempotency_result(&record).unwrap();
        let loaded = store.find_idempotency_result("ik-1").unwrap().unwrap();
        assert_eq!(loaded.key, "ik-1");
        assert_eq!(loaded.operation, "create_sandbox");
        assert_eq!(loaded.request_hash, "abc123");
        assert_eq!(loaded.response_json, r#"{"sandbox_id":"sbx-1"}"#);
        assert_eq!(loaded.status_code, 201);
        assert_eq!(loaded.created_at, 1_700_000_000);
        assert_eq!(loaded.expires_at, 1_700_000_000 + IDEMPOTENCY_TTL_SECS);
    }

    #[test]
    fn idempotency_missing_key_returns_none() {
        let store = StateStore::in_memory().unwrap();
        let loaded = store.find_idempotency_result("nonexistent").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn idempotency_cleanup_removes_expired_keys() {
        let store = StateStore::in_memory().unwrap();

        // Record with expires_at in the past (epoch 0 + TTL = ~1 day).
        let expired = IdempotencyRecord {
            key: "ik-expired".to_string(),
            operation: "create_sandbox".to_string(),
            request_hash: "hash1".to_string(),
            response_json: "{}".to_string(),
            status_code: 201,
            created_at: 0,
            expires_at: 1, // Far in the past
        };
        store.save_idempotency_result(&expired).unwrap();

        // Record with expires_at far in the future.
        let fresh = IdempotencyRecord {
            key: "ik-fresh".to_string(),
            operation: "create_sandbox".to_string(),
            request_hash: "hash2".to_string(),
            response_json: "{}".to_string(),
            status_code: 201,
            created_at: 1_700_000_000,
            expires_at: u64::MAX / 2, // Far in the future
        };
        store.save_idempotency_result(&fresh).unwrap();

        let deleted = store.cleanup_expired_idempotency_keys().unwrap();
        assert_eq!(deleted, 1);

        // Expired key is gone.
        assert!(
            store
                .find_idempotency_result("ik-expired")
                .unwrap()
                .is_none()
        );
        // Fresh key is still present.
        assert!(store.find_idempotency_result("ik-fresh").unwrap().is_some());
    }

    // ── Lease persistence tests ──

    fn sample_lease(id: &str, sandbox_id: &str) -> Lease {
        Lease {
            lease_id: id.to_string(),
            sandbox_id: sandbox_id.to_string(),
            ttl_secs: 300,
            last_heartbeat_at: 1_700_000_000,
            state: LeaseState::Opening,
        }
    }

    #[test]
    fn lease_round_trip() {
        let store = StateStore::in_memory().unwrap();
        let lease = sample_lease("ls-1", "sb-1");

        store.save_lease(&lease).unwrap();
        let loaded = store.load_lease("ls-1").unwrap().unwrap();
        assert_eq!(loaded, lease);
    }

    #[test]
    fn lease_list_for_sandbox() {
        let store = StateStore::in_memory().unwrap();
        let lease1 = sample_lease("ls-a", "sb-1");
        let lease2 = sample_lease("ls-b", "sb-1");
        let lease3 = sample_lease("ls-c", "sb-2");

        store.save_lease(&lease1).unwrap();
        store.save_lease(&lease2).unwrap();
        store.save_lease(&lease3).unwrap();

        let sb1_leases = store.list_leases_for_sandbox("sb-1").unwrap();
        assert_eq!(sb1_leases.len(), 2);

        let sb2_leases = store.list_leases_for_sandbox("sb-2").unwrap();
        assert_eq!(sb2_leases.len(), 1);
    }

    #[test]
    fn lease_list_returns_all() {
        let store = StateStore::in_memory().unwrap();
        let lease1 = sample_lease("ls-x", "sb-1");
        let lease2 = sample_lease("ls-y", "sb-2");

        store.save_lease(&lease1).unwrap();
        store.save_lease(&lease2).unwrap();

        let all = store.list_leases().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn lease_delete_removes() {
        let store = StateStore::in_memory().unwrap();
        let lease = sample_lease("ls-del", "sb-1");

        store.save_lease(&lease).unwrap();
        store.delete_lease("ls-del").unwrap();
        let loaded = store.load_lease("ls-del").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn lease_upsert_updates_state() {
        let store = StateStore::in_memory().unwrap();
        let mut lease = sample_lease("ls-up", "sb-1");

        store.save_lease(&lease).unwrap();

        lease.state = LeaseState::Active;
        lease.last_heartbeat_at = 1_700_000_100;
        store.save_lease(&lease).unwrap();

        let loaded = store.load_lease("ls-up").unwrap().unwrap();
        assert_eq!(loaded.state, LeaseState::Active);
        assert_eq!(loaded.last_heartbeat_at, 1_700_000_100);
    }

    // ── Execution persistence tests ──

    fn sample_execution(id: &str, container_id: &str) -> Execution {
        Execution {
            execution_id: id.to_string(),
            container_id: container_id.to_string(),
            exec_spec: ExecutionSpec {
                cmd: vec!["echo".to_string(), "hello".to_string()],
                args: vec![],
                env_override: std::collections::BTreeMap::new(),
                pty: false,
                timeout_secs: None,
            },
            state: ExecutionState::Queued,
            exit_code: None,
            started_at: None,
            ended_at: None,
        }
    }

    #[test]
    fn execution_round_trip() {
        let store = StateStore::in_memory().unwrap();
        let execution = sample_execution("exec-1", "ctr-abc");

        store.save_execution(&execution).unwrap();
        let loaded = store.load_execution("exec-1").unwrap().unwrap();
        assert_eq!(loaded.execution_id, "exec-1");
        assert_eq!(loaded.container_id, "ctr-abc");
        assert_eq!(loaded.state, ExecutionState::Queued);
        assert_eq!(loaded.exec_spec.cmd, vec!["echo", "hello"]);
    }

    #[test]
    fn execution_list_returns_all() {
        let store = StateStore::in_memory().unwrap();
        store
            .save_execution(&sample_execution("exec-a", "ctr-1"))
            .unwrap();
        store
            .save_execution(&sample_execution("exec-b", "ctr-2"))
            .unwrap();

        let all = store.list_executions().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn execution_list_for_container() {
        let store = StateStore::in_memory().unwrap();
        store
            .save_execution(&sample_execution("exec-a", "ctr-1"))
            .unwrap();
        store
            .save_execution(&sample_execution("exec-b", "ctr-1"))
            .unwrap();
        store
            .save_execution(&sample_execution("exec-c", "ctr-2"))
            .unwrap();

        let for_ctr1 = store.list_executions_for_container("ctr-1").unwrap();
        assert_eq!(for_ctr1.len(), 2);
        assert!(for_ctr1.iter().all(|e| e.container_id == "ctr-1"));

        let for_ctr2 = store.list_executions_for_container("ctr-2").unwrap();
        assert_eq!(for_ctr2.len(), 1);
    }

    #[test]
    fn execution_delete_removes() {
        let store = StateStore::in_memory().unwrap();
        store
            .save_execution(&sample_execution("exec-del", "ctr-1"))
            .unwrap();
        store.delete_execution("exec-del").unwrap();
        let loaded = store.load_execution("exec-del").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn execution_upsert_updates_state() {
        let store = StateStore::in_memory().unwrap();
        let mut execution = sample_execution("exec-up", "ctr-1");

        store.save_execution(&execution).unwrap();

        execution.state = ExecutionState::Running;
        execution.started_at = Some(1_700_000_000);
        store.save_execution(&execution).unwrap();

        let loaded = store.load_execution("exec-up").unwrap().unwrap();
        assert_eq!(loaded.state, ExecutionState::Running);
        assert_eq!(loaded.started_at, Some(1_700_000_000));
    }

    #[test]
    fn execution_missing_returns_none() {
        let store = StateStore::in_memory().unwrap();
        let loaded = store.load_execution("nonexistent").unwrap();
        assert!(loaded.is_none());
    }

    // ── Checkpoint persistence tests ──

    fn sample_checkpoint(id: &str, sandbox_id: &str) -> Checkpoint {
        Checkpoint {
            checkpoint_id: id.to_string(),
            sandbox_id: sandbox_id.to_string(),
            parent_checkpoint_id: None,
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Creating,
            created_at: 1_700_000_000,
            compatibility_fingerprint: "fp-abc123".to_string(),
        }
    }

    #[test]
    fn checkpoint_round_trip() {
        let store = StateStore::in_memory().unwrap();
        let checkpoint = sample_checkpoint("ckpt-1", "sb-1");

        store.save_checkpoint(&checkpoint).unwrap();
        let loaded = store.load_checkpoint("ckpt-1").unwrap().unwrap();
        assert_eq!(loaded, checkpoint);
    }

    #[test]
    fn checkpoint_missing_returns_none() {
        let store = StateStore::in_memory().unwrap();
        let loaded = store.load_checkpoint("nonexistent").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn checkpoint_upsert_updates_state() {
        let store = StateStore::in_memory().unwrap();
        let mut checkpoint = sample_checkpoint("ckpt-up", "sb-1");

        store.save_checkpoint(&checkpoint).unwrap();

        checkpoint.state = CheckpointState::Ready;
        store.save_checkpoint(&checkpoint).unwrap();

        let loaded = store.load_checkpoint("ckpt-up").unwrap().unwrap();
        assert_eq!(loaded.state, CheckpointState::Ready);
    }

    #[test]
    fn checkpoint_list_returns_all_ordered() {
        let store = StateStore::in_memory().unwrap();
        let ckpt1 = sample_checkpoint("ckpt-a", "sb-1");
        let mut ckpt2 = sample_checkpoint("ckpt-b", "sb-2");
        ckpt2.created_at = 1_700_000_001;

        store.save_checkpoint(&ckpt1).unwrap();
        store.save_checkpoint(&ckpt2).unwrap();

        let all = store.list_checkpoints().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].checkpoint_id, "ckpt-a");
        assert_eq!(all[1].checkpoint_id, "ckpt-b");
    }

    #[test]
    fn checkpoint_list_for_sandbox_filters() {
        let store = StateStore::in_memory().unwrap();
        let ckpt1 = sample_checkpoint("ckpt-1", "sb-1");
        let ckpt2 = sample_checkpoint("ckpt-2", "sb-2");
        let mut ckpt3 = sample_checkpoint("ckpt-3", "sb-1");
        ckpt3.created_at = 1_700_000_001;

        store.save_checkpoint(&ckpt1).unwrap();
        store.save_checkpoint(&ckpt2).unwrap();
        store.save_checkpoint(&ckpt3).unwrap();

        let sb1 = store.list_checkpoints_for_sandbox("sb-1").unwrap();
        assert_eq!(sb1.len(), 2);
        assert!(sb1.iter().all(|c| c.sandbox_id == "sb-1"));
    }

    #[test]
    fn checkpoint_children_returns_direct_children() {
        let store = StateStore::in_memory().unwrap();
        let parent = sample_checkpoint("ckpt-parent", "sb-1");
        let mut child1 = sample_checkpoint("ckpt-child1", "sb-2");
        child1.parent_checkpoint_id = Some("ckpt-parent".to_string());
        let mut child2 = sample_checkpoint("ckpt-child2", "sb-3");
        child2.parent_checkpoint_id = Some("ckpt-parent".to_string());
        child2.created_at = 1_700_000_001;
        let unrelated = sample_checkpoint("ckpt-other", "sb-4");

        store.save_checkpoint(&parent).unwrap();
        store.save_checkpoint(&child1).unwrap();
        store.save_checkpoint(&child2).unwrap();
        store.save_checkpoint(&unrelated).unwrap();

        let children = store.list_checkpoint_children("ckpt-parent").unwrap();
        assert_eq!(children.len(), 2);
        assert!(
            children
                .iter()
                .all(|c| c.parent_checkpoint_id.as_deref() == Some("ckpt-parent"))
        );
    }

    #[test]
    fn checkpoint_delete_removes() {
        let store = StateStore::in_memory().unwrap();
        let checkpoint = sample_checkpoint("ckpt-del", "sb-1");

        store.save_checkpoint(&checkpoint).unwrap();
        store.delete_checkpoint("ckpt-del").unwrap();
        let loaded = store.load_checkpoint("ckpt-del").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn checkpoint_null_parent_round_trips() {
        let store = StateStore::in_memory().unwrap();
        let checkpoint = sample_checkpoint("ckpt-null-parent", "sb-1");
        assert!(checkpoint.parent_checkpoint_id.is_none());

        store.save_checkpoint(&checkpoint).unwrap();
        let loaded = store.load_checkpoint("ckpt-null-parent").unwrap().unwrap();
        assert!(loaded.parent_checkpoint_id.is_none());
    }

    #[test]
    fn checkpoint_vm_full_class_persists() {
        let store = StateStore::in_memory().unwrap();
        let mut checkpoint = sample_checkpoint("ckpt-vm", "sb-1");
        checkpoint.class = CheckpointClass::VmFull;

        store.save_checkpoint(&checkpoint).unwrap();
        let loaded = store.load_checkpoint("ckpt-vm").unwrap().unwrap();
        assert_eq!(loaded.class, CheckpointClass::VmFull);
    }
}
