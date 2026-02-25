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
    Build, BuildSpec, BuildState, Checkpoint, CheckpointClass, CheckpointState, Container,
    ContainerSpec, ContainerState, Execution, ExecutionSpec, ExecutionState, Lease, LeaseState,
    Sandbox, SandboxBackend, SandboxSpec, SandboxState,
};

use crate::StackSpec;
use crate::error::StackError;
use crate::events::{EventRecord, StackEvent};
use crate::network::PublishedPort;
use crate::reconcile::Action;

/// Severity level for a drift finding detected during startup verification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DriftSeverity {
    /// Informational: no action required.
    Info,
    /// Warning: possible inconsistency that may need attention.
    Warning,
    /// Error: critical inconsistency requiring intervention.
    Error,
}

impl DriftSeverity {
    /// Serialize to the lowercase string used in events.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

/// A single drift finding discovered during startup state verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftFinding {
    /// Category of the drift (e.g. "desired_state", "observed_state", "health", "reconcile").
    pub category: String,
    /// Human-readable description of the inconsistency.
    pub description: String,
    /// Severity level of the finding.
    pub severity: DriftSeverity,
}

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

/// A single audit entry from the reconcile audit log.
///
/// Each action in a reconcile session produces one entry when started
/// and is updated on completion (success or failure). This provides a
/// durable, ordered record of every reconciliation action for
/// crash-recovery analysis and operational audit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcileAuditEntry {
    /// Auto-incremented row identifier.
    pub id: i64,
    /// Session that owns this action.
    pub session_id: String,
    /// Stack this action belongs to.
    pub stack_name: String,
    /// Ordinal position of the action within the session plan.
    pub action_index: usize,
    /// Kind of action (e.g. `"service_create"`, `"service_recreate"`, `"service_remove"`).
    pub action_kind: String,
    /// Target service name.
    pub service_name: String,
    /// Deterministic hash of the action for identity tracking.
    pub action_hash: String,
    /// Lifecycle status: `"started"`, `"completed"`, or `"failed"`.
    pub status: String,
    /// Unix epoch seconds when the action started.
    pub started_at: u64,
    /// Unix epoch seconds when the action completed (if terminal).
    pub completed_at: Option<u64>,
    /// Error message, if the action failed.
    pub error_message: Option<String>,
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

/// Persisted record of a resolved OCI image reference.
///
/// Stores the result of a pull/resolve operation so that subsequent
/// lookups can avoid redundant registry roundtrips.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageRecord {
    /// Original image reference (e.g. `nginx:latest`).
    pub image_ref: String,
    /// Resolved content-addressable digest.
    pub resolved_digest: String,
    /// Target platform string (e.g. `linux/arm64`).
    pub platform: String,
    /// Source registry where the image was pulled from.
    pub source_registry: String,
    /// Unix epoch seconds when the image was pulled.
    pub pulled_at: u64,
}

/// Record of a completed mutating operation, providing a durable receipt
/// that clients can retrieve for auditability and idempotent retry verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    /// Unique receipt identifier (e.g. `rcp-{uuid}`).
    pub receipt_id: String,
    /// Name of the operation that produced this receipt (e.g. `"create_sandbox"`).
    pub operation: String,
    /// Identifier of the entity acted upon.
    pub entity_id: String,
    /// Type of the entity (e.g. `"sandbox"`, `"lease"`, `"execution"`, `"checkpoint"`).
    pub entity_type: String,
    /// Correlating request identifier.
    pub request_id: String,
    /// Completion status (e.g. `"completed"`, `"failed"`).
    pub status: String,
    /// Unix epoch seconds when the receipt was created.
    pub created_at: u64,
    /// Arbitrary JSON metadata associated with the operation.
    pub metadata: serde_json::Value,
}

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
            CREATE INDEX IF NOT EXISTS idx_checkpoint_parent ON checkpoint_state(parent_checkpoint_id);

            CREATE TABLE IF NOT EXISTS container_state (
                container_id TEXT PRIMARY KEY,
                sandbox_id TEXT NOT NULL,
                image_digest TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                started_at INTEGER,
                ended_at INTEGER,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_container_sandbox ON container_state(sandbox_id);

            CREATE TABLE IF NOT EXISTS image_state (
                image_ref TEXT PRIMARY KEY,
                resolved_digest TEXT NOT NULL,
                platform TEXT NOT NULL,
                source_registry TEXT NOT NULL,
                pulled_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS receipt_state (
                receipt_id TEXT PRIMARY KEY,
                operation TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                request_id TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_receipt_entity ON receipt_state(entity_type, entity_id);
            CREATE INDEX IF NOT EXISTS idx_receipt_request ON receipt_state(request_id);

            CREATE TABLE IF NOT EXISTS build_state (
                build_id TEXT PRIMARY KEY,
                sandbox_id TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                state TEXT NOT NULL,
                result_digest TEXT,
                started_at INTEGER NOT NULL,
                ended_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_build_sandbox ON build_state(sandbox_id);

            CREATE TABLE IF NOT EXISTS reconcile_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                stack_name TEXT NOT NULL,
                action_index INTEGER NOT NULL,
                action_kind TEXT NOT NULL,
                service_name TEXT NOT NULL,
                action_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                completed_at INTEGER,
                error_message TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_audit_session ON reconcile_audit_log(session_id);
            CREATE INDEX IF NOT EXISTS idx_audit_stack ON reconcile_audit_log(stack_name);

            CREATE TABLE IF NOT EXISTS control_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );",
        )?;

        // Bootstrap initial metadata if not yet set.
        // Uses INSERT OR IGNORE so that a previously-set schema version
        // (e.g. after a migration) is not overwritten on reopen.
        self.conn.execute(
            "INSERT OR IGNORE INTO control_metadata (key, value) VALUES ('schema_version', '1')",
            [],
        )?;
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Only set created_at on first init (ignore conflict).
        self.conn.execute(
            "INSERT OR IGNORE INTO control_metadata (key, value) VALUES ('created_at', ?1)",
            params![format!("{now_secs}")],
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

    /// Delete events older than `max_age_secs` seconds for a stack.
    ///
    /// Returns the number of events deleted. The `created_at` column stores
    /// an ISO 8601 datetime from SQLite's `datetime('now')`, so the comparison
    /// uses `datetime('now', '-N seconds')`.
    pub fn compact_events(&self, stack_name: &str, max_age_secs: u64) -> Result<usize, StackError> {
        let deleted = self.conn.execute(
            "DELETE FROM events
             WHERE stack_name = ?1
               AND created_at < datetime('now', ?2)",
            params![stack_name, format!("-{max_age_secs} seconds")],
        )?;
        Ok(deleted)
    }

    /// Keep only the most recent `max_count` events for a stack, deleting older ones.
    ///
    /// Returns the number of events deleted.
    pub fn compact_events_by_count(
        &self,
        stack_name: &str,
        max_count: usize,
    ) -> Result<usize, StackError> {
        let deleted = self.conn.execute(
            "DELETE FROM events
             WHERE stack_name = ?1
               AND id NOT IN (
                   SELECT id FROM events
                   WHERE stack_name = ?1
                   ORDER BY id DESC
                   LIMIT ?2
               )",
            params![stack_name, max_count as i64],
        )?;
        Ok(deleted)
    }

    /// Default maximum number of events retained per stack.
    pub const DEFAULT_MAX_EVENTS: usize = 10_000;

    /// Default maximum age of events in seconds (7 days).
    pub const DEFAULT_MAX_AGE_SECS: u64 = 7 * 24 * 3600;

    /// Run both age-based and count-based compaction with default retention policy.
    ///
    /// Deletes events older than [`DEFAULT_MAX_AGE_SECS`](Self::DEFAULT_MAX_AGE_SECS)
    /// and trims to at most [`DEFAULT_MAX_EVENTS`](Self::DEFAULT_MAX_EVENTS) per stack.
    /// Returns the total number of events deleted.
    pub fn compact_events_default(&self, stack_name: &str) -> Result<usize, StackError> {
        let age_deleted = self.compact_events(stack_name, Self::DEFAULT_MAX_AGE_SECS)?;
        let count_deleted = self.compact_events_by_count(stack_name, Self::DEFAULT_MAX_EVENTS)?;
        Ok(age_deleted + count_deleted)
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

    // ── Event compaction tests ──

    fn emit_n_events(store: &StateStore, stack_name: &str, n: usize) {
        for i in 0..n {
            store
                .emit_event(
                    stack_name,
                    &StackEvent::ServiceCreating {
                        stack_name: stack_name.to_string(),
                        service_name: format!("svc-{i}"),
                    },
                )
                .unwrap();
        }
    }

    #[test]
    fn compact_events_by_count_keeps_recent() {
        let store = StateStore::in_memory().unwrap();
        emit_n_events(&store, "myapp", 20);

        assert_eq!(store.event_count("myapp").unwrap(), 20);

        let deleted = store.compact_events_by_count("myapp", 10).unwrap();
        assert_eq!(deleted, 10);
        assert_eq!(store.event_count("myapp").unwrap(), 10);

        // The kept events should be the most recent 10 (IDs 11..=20).
        let records = store.load_event_records("myapp").unwrap();
        assert_eq!(records.len(), 10);
        // Verify ordering is ascending by id and that the oldest kept is > 10.
        assert!(records[0].id > 10);
    }

    #[test]
    fn compact_events_by_count_noop_when_under_limit() {
        let store = StateStore::in_memory().unwrap();
        emit_n_events(&store, "myapp", 5);

        let deleted = store.compact_events_by_count("myapp", 10).unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(store.event_count("myapp").unwrap(), 5);
    }

    #[test]
    fn compact_events_by_count_scoped_to_stack() {
        let store = StateStore::in_memory().unwrap();
        emit_n_events(&store, "app-a", 15);
        emit_n_events(&store, "app-b", 5);

        let deleted = store.compact_events_by_count("app-a", 10).unwrap();
        assert_eq!(deleted, 5);
        assert_eq!(store.event_count("app-a").unwrap(), 10);
        // app-b is untouched.
        assert_eq!(store.event_count("app-b").unwrap(), 5);
    }

    #[test]
    fn compact_events_by_age_deletes_old() {
        let store = StateStore::in_memory().unwrap();
        emit_n_events(&store, "myapp", 5);

        // Back-date all events to 2 hours ago so they are clearly old.
        store
            .conn
            .execute(
                "UPDATE events SET created_at = datetime('now', '-7200 seconds') WHERE stack_name = 'myapp'",
                [],
            )
            .unwrap();

        // Delete events older than 1 hour (3600 seconds). All 5 should be removed.
        let deleted = store.compact_events("myapp", 3600).unwrap();
        assert_eq!(deleted, 5);
        assert_eq!(store.event_count("myapp").unwrap(), 0);
    }

    #[test]
    fn compact_events_by_age_keeps_recent() {
        let store = StateStore::in_memory().unwrap();
        emit_n_events(&store, "myapp", 5);

        // With a generous window (1 hour), nothing should be deleted
        // because the events were just created.
        let deleted = store.compact_events("myapp", 3600).unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(store.event_count("myapp").unwrap(), 5);
    }

    #[test]
    fn compact_events_by_age_partial_delete() {
        let store = StateStore::in_memory().unwrap();
        emit_n_events(&store, "myapp", 5);

        // Back-date 3 events to 2 hours ago, leave 2 at current time.
        store
            .conn
            .execute(
                "UPDATE events SET created_at = datetime('now', '-7200 seconds')
                 WHERE stack_name = 'myapp' AND id IN (
                     SELECT id FROM events WHERE stack_name = 'myapp' ORDER BY id ASC LIMIT 3
                 )",
                [],
            )
            .unwrap();

        let deleted = store.compact_events("myapp", 3600).unwrap();
        assert_eq!(deleted, 3);
        assert_eq!(store.event_count("myapp").unwrap(), 2);
    }

    #[test]
    fn compact_events_default_applies_both_policies() {
        let store = StateStore::in_memory().unwrap();
        // Emit more than the default max (10,000).
        emit_n_events(&store, "myapp", 10_050);
        assert_eq!(store.event_count("myapp").unwrap(), 10_050);

        let deleted = store.compact_events_default("myapp").unwrap();
        // Age-based deletes 0 (all recent), count-based deletes 50.
        assert_eq!(deleted, 50);
        assert_eq!(store.event_count("myapp").unwrap(), 10_000);
    }

    #[test]
    fn event_count_empty_stack() {
        let store = StateStore::in_memory().unwrap();
        assert_eq!(store.event_count("nonexistent").unwrap(), 0);
    }

    #[test]
    fn compact_events_empty_stack() {
        let store = StateStore::in_memory().unwrap();
        let deleted = store.compact_events("nonexistent", 0).unwrap();
        assert_eq!(deleted, 0);
        let deleted = store.compact_events_by_count("nonexistent", 10).unwrap();
        assert_eq!(deleted, 0);
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

    // ── Receipt persistence tests (from agent-a03881b1) ──

    fn sample_receipt(receipt_id: &str, entity_id: &str) -> Receipt {
        Receipt {
            receipt_id: receipt_id.to_string(),
            operation: "create_sandbox".to_string(),
            entity_id: entity_id.to_string(),
            entity_type: "sandbox".to_string(),
            request_id: "req-1".to_string(),
            status: "completed".to_string(),
            created_at: 1_700_000_000,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    #[test]
    fn receipt_save_and_load() {
        let store = StateStore::in_memory().unwrap();
        let receipt = sample_receipt("rcp-1", "sbx-1");

        store.save_receipt(&receipt).unwrap();
        let loaded = store.load_receipt("rcp-1").unwrap().unwrap();
        assert_eq!(loaded.receipt_id, "rcp-1");
        assert_eq!(loaded.operation, "create_sandbox");
        assert_eq!(loaded.entity_id, "sbx-1");
        assert_eq!(loaded.entity_type, "sandbox");
        assert_eq!(loaded.request_id, "req-1");
        assert_eq!(loaded.status, "completed");
        assert_eq!(loaded.created_at, 1_700_000_000);
    }

    #[test]
    fn receipt_load_missing_returns_none() {
        let store = StateStore::in_memory().unwrap();
        let loaded = store.load_receipt("nonexistent").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn receipt_load_by_request_id() {
        let store = StateStore::in_memory().unwrap();
        let receipt = sample_receipt("rcp-2", "sbx-2");

        store.save_receipt(&receipt).unwrap();
        let loaded = store.load_receipt_by_request_id("req-1").unwrap().unwrap();
        assert_eq!(loaded.receipt_id, "rcp-2");
    }

    #[test]
    fn receipt_load_by_request_id_missing_returns_none() {
        let store = StateStore::in_memory().unwrap();
        let loaded = store.load_receipt_by_request_id("nonexistent").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn receipt_list_for_entity() {
        let store = StateStore::in_memory().unwrap();

        let r1 = Receipt {
            receipt_id: "rcp-a".to_string(),
            operation: "create_sandbox".to_string(),
            entity_id: "sbx-1".to_string(),
            entity_type: "sandbox".to_string(),
            request_id: "req-a".to_string(),
            status: "completed".to_string(),
            created_at: 1_700_000_000,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let r2 = Receipt {
            receipt_id: "rcp-b".to_string(),
            operation: "terminate_sandbox".to_string(),
            entity_id: "sbx-1".to_string(),
            entity_type: "sandbox".to_string(),
            request_id: "req-b".to_string(),
            status: "completed".to_string(),
            created_at: 1_700_000_001,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        let r3 = Receipt {
            receipt_id: "rcp-c".to_string(),
            operation: "create_sandbox".to_string(),
            entity_id: "sbx-2".to_string(),
            entity_type: "sandbox".to_string(),
            request_id: "req-c".to_string(),
            status: "completed".to_string(),
            created_at: 1_700_000_002,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };

        store.save_receipt(&r1).unwrap();
        store.save_receipt(&r2).unwrap();
        store.save_receipt(&r3).unwrap();

        let sbx1_receipts = store.list_receipts_for_entity("sandbox", "sbx-1").unwrap();
        assert_eq!(sbx1_receipts.len(), 2);
        assert_eq!(sbx1_receipts[0].receipt_id, "rcp-a");
        assert_eq!(sbx1_receipts[1].receipt_id, "rcp-b");

        let sbx2_receipts = store.list_receipts_for_entity("sandbox", "sbx-2").unwrap();
        assert_eq!(sbx2_receipts.len(), 1);
        assert_eq!(sbx2_receipts[0].receipt_id, "rcp-c");

        let empty = store.list_receipts_for_entity("lease", "ls-1").unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn receipt_upsert_updates() {
        let store = StateStore::in_memory().unwrap();
        let mut receipt = sample_receipt("rcp-upsert", "sbx-1");
        receipt.status = "pending".to_string();
        store.save_receipt(&receipt).unwrap();

        receipt.status = "completed".to_string();
        store.save_receipt(&receipt).unwrap();

        let loaded = store.load_receipt("rcp-upsert").unwrap().unwrap();
        assert_eq!(loaded.status, "completed");
    }

    // ── Scoped event listing tests (from agent-a03881b1) ──

    #[test]
    fn events_by_scope_filters_on_type_prefix() {
        let store = StateStore::in_memory().unwrap();

        store
            .emit_event(
                "myapp",
                &StackEvent::SandboxCreating {
                    stack_name: "myapp".to_string(),
                    sandbox_id: "sb-1".to_string(),
                },
            )
            .unwrap();
        store
            .emit_event(
                "myapp",
                &StackEvent::LeaseOpened {
                    sandbox_id: "sb-1".to_string(),
                    lease_id: "ls-1".to_string(),
                },
            )
            .unwrap();
        store
            .emit_event(
                "myapp",
                &StackEvent::SandboxReady {
                    stack_name: "myapp".to_string(),
                    sandbox_id: "sb-1".to_string(),
                },
            )
            .unwrap();
        store
            .emit_event(
                "myapp",
                &StackEvent::ExecutionQueued {
                    container_id: "ctr-1".to_string(),
                    execution_id: "exec-1".to_string(),
                },
            )
            .unwrap();

        let sandbox_events = store
            .load_events_by_scope("myapp", "sandbox_", None, 100)
            .unwrap();
        assert_eq!(sandbox_events.len(), 2);

        let lease_events = store
            .load_events_by_scope("myapp", "lease_", None, 100)
            .unwrap();
        assert_eq!(lease_events.len(), 1);

        let exec_events = store
            .load_events_by_scope("myapp", "execution_", None, 100)
            .unwrap();
        assert_eq!(exec_events.len(), 1);
    }

    #[test]
    fn events_by_scope_respects_cursor_and_limit() {
        let store = StateStore::in_memory().unwrap();

        for i in 0..5 {
            store
                .emit_event(
                    "myapp",
                    &StackEvent::SandboxCreating {
                        stack_name: "myapp".to_string(),
                        sandbox_id: format!("sb-{i}"),
                    },
                )
                .unwrap();
        }

        let first_page = store
            .load_events_by_scope("myapp", "sandbox_", None, 2)
            .unwrap();
        assert_eq!(first_page.len(), 2);

        let cursor = first_page.last().map(|r| r.id);
        let second_page = store
            .load_events_by_scope("myapp", "sandbox_", cursor, 2)
            .unwrap();
        assert_eq!(second_page.len(), 2);

        // IDs should be strictly greater than the cursor
        assert!(second_page[0].id > first_page[1].id);
    }

    #[test]
    fn events_by_scope_empty_scope_returns_nothing() {
        let store = StateStore::in_memory().unwrap();
        store
            .emit_event(
                "myapp",
                &StackEvent::SandboxCreating {
                    stack_name: "myapp".to_string(),
                    sandbox_id: "sb-1".to_string(),
                },
            )
            .unwrap();

        let events = store
            .load_events_by_scope("myapp", "nonexistent_", None, 100)
            .unwrap();
        assert!(events.is_empty());
    }

    // ── Build persistence tests (from agent-af0c4a41) ──

    fn sample_build(id: &str, sandbox_id: &str) -> Build {
        Build {
            build_id: id.to_string(),
            sandbox_id: sandbox_id.to_string(),
            build_spec: BuildSpec {
                context: "/tmp/ctx".to_string(),
                dockerfile: Some("Dockerfile".to_string()),
                args: std::collections::BTreeMap::new(),
            },
            state: BuildState::Queued,
            result_digest: None,
            started_at: 1_700_000_000,
            ended_at: None,
        }
    }

    #[test]
    fn build_round_trip() {
        let store = StateStore::in_memory().unwrap();
        let build = sample_build("bld-1", "sb-1");

        store.save_build(&build).unwrap();
        let loaded = store.load_build("bld-1").unwrap().unwrap();
        assert_eq!(loaded.build_id, "bld-1");
        assert_eq!(loaded.sandbox_id, "sb-1");
        assert_eq!(loaded.state, BuildState::Queued);
        assert_eq!(loaded.build_spec.context, "/tmp/ctx");
    }

    #[test]
    fn build_list_returns_all() {
        let store = StateStore::in_memory().unwrap();
        store.save_build(&sample_build("bld-a", "sb-1")).unwrap();
        store.save_build(&sample_build("bld-b", "sb-2")).unwrap();

        let all = store.list_builds().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn build_list_for_sandbox() {
        let store = StateStore::in_memory().unwrap();
        store.save_build(&sample_build("bld-a", "sb-1")).unwrap();
        store.save_build(&sample_build("bld-b", "sb-1")).unwrap();
        store.save_build(&sample_build("bld-c", "sb-2")).unwrap();

        let for_sb1 = store.list_builds_for_sandbox("sb-1").unwrap();
        assert_eq!(for_sb1.len(), 2);
        assert!(for_sb1.iter().all(|b| b.sandbox_id == "sb-1"));

        let for_sb2 = store.list_builds_for_sandbox("sb-2").unwrap();
        assert_eq!(for_sb2.len(), 1);
    }

    #[test]
    fn build_delete_removes() {
        let store = StateStore::in_memory().unwrap();
        store.save_build(&sample_build("bld-del", "sb-1")).unwrap();
        store.delete_build("bld-del").unwrap();
        let loaded = store.load_build("bld-del").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn build_upsert_updates_state() {
        let store = StateStore::in_memory().unwrap();
        let mut build = sample_build("bld-up", "sb-1");

        store.save_build(&build).unwrap();

        build.state = BuildState::Running;
        store.save_build(&build).unwrap();

        let loaded = store.load_build("bld-up").unwrap().unwrap();
        assert_eq!(loaded.state, BuildState::Running);
    }

    #[test]
    fn build_missing_returns_none() {
        let store = StateStore::in_memory().unwrap();
        let loaded = store.load_build("nonexistent").unwrap();
        assert!(loaded.is_none());
    }

    // ── Phase 1 validation tests (from agent-a80ffa89) ──

    #[test]
    fn phase1_validation_health_state_persistence_round_trip() {
        let store = StateStore::in_memory().unwrap();

        let mut original_state = HashMap::new();
        original_state.insert(
            "web".to_string(),
            HealthPollState {
                service_name: "web".to_string(),
                consecutive_passes: 5,
                consecutive_failures: 0,
                last_check_millis: Some(1_700_000_000_000),
                start_time_millis: Some(1_700_000_000_123),
            },
        );
        original_state.insert(
            "db".to_string(),
            HealthPollState {
                service_name: "db".to_string(),
                consecutive_passes: 0,
                consecutive_failures: 3,
                last_check_millis: Some(1_700_000_001_000),
                start_time_millis: Some(1_700_000_000_456),
            },
        );

        // Save to store.
        store
            .save_health_poller_state("myapp", &original_state)
            .unwrap();

        // Load from a fresh perspective (same store, simulating reload).
        let loaded = store.load_health_poller_state("myapp").unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded.get("web").unwrap(),
            original_state.get("web").unwrap()
        );
        assert_eq!(loaded.get("db").unwrap(), original_state.get("db").unwrap());

        // Verify counters survived the round-trip.
        let web = loaded.get("web").unwrap();
        assert_eq!(web.consecutive_passes, 5);
        assert_eq!(web.consecutive_failures, 0);

        let db = loaded.get("db").unwrap();
        assert_eq!(db.consecutive_passes, 0);
        assert_eq!(db.consecutive_failures, 3);
    }

    #[test]
    fn phase1_validation_allocator_state_persistence_round_trip() {
        let store = StateStore::in_memory().unwrap();

        let mut ports = HashMap::new();
        ports.insert(
            "web".to_string(),
            vec![PublishedPort {
                host_port: 8080,
                container_port: 80,
                protocol: "tcp".to_string(),
            }],
        );
        ports.insert(
            "db".to_string(),
            vec![PublishedPort {
                host_port: 5432,
                container_port: 5432,
                protocol: "tcp".to_string(),
            }],
        );

        let mut service_ips = HashMap::new();
        service_ips.insert("web".to_string(), "10.0.0.2".to_string());
        service_ips.insert("db".to_string(), "10.0.0.3".to_string());

        let mut mount_tag_offsets = HashMap::new();
        mount_tag_offsets.insert("web".to_string(), 0);
        mount_tag_offsets.insert("db".to_string(), 3);

        let snapshot = AllocatorSnapshot {
            ports: ports.clone(),
            service_ips: service_ips.clone(),
            mount_tag_offsets: mount_tag_offsets.clone(),
        };

        store.save_allocator_state("myapp", &snapshot).unwrap();

        // Reload and verify all fields.
        let loaded = store.load_allocator_state("myapp").unwrap().unwrap();
        assert_eq!(loaded.ports, ports);
        assert_eq!(loaded.service_ips, service_ips);
        assert_eq!(loaded.mount_tag_offsets, mount_tag_offsets);

        // Verify specific port allocations survived.
        let web_ports = loaded.ports.get("web").unwrap();
        assert_eq!(web_ports.len(), 1);
        assert_eq!(web_ports[0].host_port, 8080);
        assert_eq!(web_ports[0].container_port, 80);

        // Verify IPs survived.
        assert_eq!(loaded.service_ips.get("web"), Some(&"10.0.0.2".to_string()));
        assert_eq!(loaded.service_ips.get("db"), Some(&"10.0.0.3".to_string()));

        // Verify mount tag offsets survived.
        assert_eq!(loaded.mount_tag_offsets.get("web"), Some(&0));
        assert_eq!(loaded.mount_tag_offsets.get("db"), Some(&3));
    }

    #[test]
    fn phase1_validation_reconcile_session_lifecycle() {
        let store = StateStore::in_memory().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
        ];

        let session = ReconcileSession {
            session_id: "rs-1000".to_string(),
            stack_name: "myapp".to_string(),
            operation_id: "op-1".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: "hash-abc".to_string(),
            next_action_index: 0,
            total_actions: 2,
            started_at: now,
            updated_at: now,
            completed_at: None,
        };

        // Create session.
        store.create_reconcile_session(&session, &actions).unwrap();

        // Load active session.
        let loaded = store
            .load_active_reconcile_session("myapp")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.session_id, "rs-1000");
        assert_eq!(loaded.status, ReconcileSessionStatus::Active);
        assert_eq!(loaded.next_action_index, 0);

        // Update progress.
        store
            .update_reconcile_session_progress("rs-1000", 1, &ReconcileSessionStatus::Active)
            .unwrap();

        let updated = store
            .load_active_reconcile_session("myapp")
            .unwrap()
            .unwrap();
        assert_eq!(updated.next_action_index, 1);
        assert_eq!(updated.status, ReconcileSessionStatus::Active);

        // Complete session.
        store.complete_reconcile_session("rs-1000").unwrap();

        // Active session should now be gone.
        let none = store.load_active_reconcile_session("myapp").unwrap();
        assert!(none.is_none());

        // List sessions should show completed.
        let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].status, ReconcileSessionStatus::Completed);
        assert!(sessions[0].completed_at.is_some());
    }

    #[test]
    fn phase1_validation_reconcile_session_supersession() {
        let store = StateStore::in_memory().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        // Create first session.
        let session1 = ReconcileSession {
            session_id: "rs-first".to_string(),
            stack_name: "myapp".to_string(),
            operation_id: "op-1".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: "hash-1".to_string(),
            next_action_index: 0,
            total_actions: 1,
            started_at: now,
            updated_at: now,
            completed_at: None,
        };
        store.create_reconcile_session(&session1, &actions).unwrap();

        // Supersede active sessions for the stack.
        let superseded_count = store.supersede_active_sessions("myapp").unwrap();
        assert_eq!(superseded_count, 1);

        // Old session should be superseded.
        let old_active = store.load_active_reconcile_session("myapp").unwrap();
        assert!(old_active.is_none());

        let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
        assert_eq!(sessions[0].status, ReconcileSessionStatus::Superseded);

        // Create new session for same stack.
        let session2 = ReconcileSession {
            session_id: "rs-second".to_string(),
            stack_name: "myapp".to_string(),
            operation_id: "op-2".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: "hash-2".to_string(),
            next_action_index: 0,
            total_actions: 1,
            started_at: now + 1,
            updated_at: now + 1,
            completed_at: None,
        };
        store.create_reconcile_session(&session2, &actions).unwrap();

        // New session is active.
        let active = store
            .load_active_reconcile_session("myapp")
            .unwrap()
            .unwrap();
        assert_eq!(active.session_id, "rs-second");
    }

    #[test]
    fn phase1_validation_event_cursor_coherence_after_simulated_restart() {
        let store = StateStore::in_memory().unwrap();

        // Emit a batch of events (simulating pre-restart state).
        let events_batch1 = vec![
            StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 2,
            },
            StackEvent::ServiceCreating {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
            StackEvent::ServiceReady {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
                runtime_id: "ctr-1".to_string(),
            },
        ];

        for event in &events_batch1 {
            store.emit_event("myapp", event).unwrap();
        }

        // Record the cursor (simulating what a consumer would save before restart).
        let all_records = store.load_event_records("myapp").unwrap();
        assert_eq!(all_records.len(), 3);
        let cursor = all_records[1].id; // After ServiceCreating

        // Emit more events (simulating post-restart activity).
        let events_batch2 = vec![
            StackEvent::ServiceCreating {
                stack_name: "myapp".to_string(),
                service_name: "db".to_string(),
            },
            StackEvent::ServiceReady {
                stack_name: "myapp".to_string(),
                service_name: "db".to_string(),
                runtime_id: "ctr-2".to_string(),
            },
            StackEvent::StackApplyCompleted {
                stack_name: "myapp".to_string(),
                succeeded: 2,
                failed: 0,
            },
        ];

        for event in &events_batch2 {
            store.emit_event("myapp", event).unwrap();
        }

        // Load events since cursor (simulating restart recovery).
        let since_cursor = store.load_events_since("myapp", cursor).unwrap();

        // Should get: ServiceReady(web), ServiceCreating(db), ServiceReady(db), StackApplyCompleted
        assert_eq!(since_cursor.len(), 4);

        // Verify ordering: IDs must be strictly monotonically increasing.
        for window in since_cursor.windows(2) {
            assert!(
                window[1].id > window[0].id,
                "event IDs must be monotonically increasing"
            );
        }

        // All events since cursor must have id > cursor.
        for record in &since_cursor {
            assert!(record.id > cursor);
        }

        // Verify completeness: total events = batch1 + batch2.
        let total = store.load_event_records("myapp").unwrap();
        assert_eq!(total.len(), 6);

        // Verify cursor-based loading gives exact complement.
        let from_start = store.load_events_since("myapp", 0).unwrap();
        assert_eq!(from_start.len(), 6);
    }

    // ── Phase 2: Schema/version migration tests (from agent-a80ffa89) ──

    #[test]
    fn phase2_control_metadata_crud() {
        let store = StateStore::in_memory().unwrap();

        // Read non-existent key.
        assert!(store.get_control_metadata("nonexistent").unwrap().is_none());

        // Set and read.
        store
            .set_control_metadata("test_key", "test_value")
            .unwrap();
        let value = store.get_control_metadata("test_key").unwrap().unwrap();
        assert_eq!(value, "test_value");

        // Update (upsert).
        store
            .set_control_metadata("test_key", "updated_value")
            .unwrap();
        let value = store.get_control_metadata("test_key").unwrap().unwrap();
        assert_eq!(value, "updated_value");
    }

    #[test]
    fn phase2_schema_version_defaults_to_1() {
        let store = StateStore::in_memory().unwrap();
        let version = store.schema_version().unwrap();
        assert_eq!(version, 1);
    }

    #[test]
    fn phase2_schema_version_set_and_get() {
        let store = StateStore::in_memory().unwrap();

        store.set_schema_version(2).unwrap();
        assert_eq!(store.schema_version().unwrap(), 2);

        store.set_schema_version(42).unwrap();
        assert_eq!(store.schema_version().unwrap(), 42);
    }

    #[test]
    fn phase2_created_at_metadata_set_on_init() {
        let store = StateStore::in_memory().unwrap();
        let created_at = store.get_control_metadata("created_at").unwrap();
        assert!(created_at.is_some());
        // Should be a parseable integer.
        let secs: u64 = created_at.unwrap().parse().unwrap();
        assert!(secs > 0);
    }

    #[test]
    fn phase2_multiple_metadata_keys_independent() {
        let store = StateStore::in_memory().unwrap();

        store.set_control_metadata("key_a", "value_a").unwrap();
        store.set_control_metadata("key_b", "value_b").unwrap();

        assert_eq!(
            store.get_control_metadata("key_a").unwrap().unwrap(),
            "value_a"
        );
        assert_eq!(
            store.get_control_metadata("key_b").unwrap().unwrap(),
            "value_b"
        );

        // Updating one doesn't affect the other.
        store.set_control_metadata("key_a", "new_a").unwrap();
        assert_eq!(
            store.get_control_metadata("key_a").unwrap().unwrap(),
            "new_a"
        );
        assert_eq!(
            store.get_control_metadata("key_b").unwrap().unwrap(),
            "value_b"
        );
    }

    // ── Phase 3: Startup drift verification tests (from agent-a80ffa89) ──

    #[test]
    fn phase3_drift_desired_without_observed() {
        let store = StateStore::in_memory().unwrap();

        // Save desired state but no observed state.
        store.save_desired_state("myapp", &sample_spec()).unwrap();

        let findings = store.verify_startup_drift("myapp").unwrap();
        assert!(
            findings
                .iter()
                .any(|f| f.category == "desired_state"
                    && f.description.contains("without observations")),
            "expected desired_state drift finding, got: {findings:?}"
        );
    }

    #[test]
    fn phase3_drift_orphaned_observed_state() {
        let store = StateStore::in_memory().unwrap();

        // Save desired state with only "web" service.
        let mut spec = sample_spec();
        spec.services.retain(|s| s.name == "web");
        store.save_desired_state("myapp", &spec).unwrap();

        // Save observed state for "web" (expected) and "cache" (orphaned).
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
        store
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    service_name: "cache".to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some("ctr-2".to_string()),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();

        let findings = store.verify_startup_drift("myapp").unwrap();
        let orphaned: Vec<_> = findings
            .iter()
            .filter(|f| f.category == "observed_state" && f.description.contains("cache"))
            .collect();
        assert_eq!(orphaned.len(), 1);
        assert!(matches!(orphaned[0].severity, DriftSeverity::Warning));
    }

    #[test]
    fn phase3_drift_stale_reconcile_session() {
        let store = StateStore::in_memory().unwrap();

        // Create an active session with updated_at far in the past (> 5 min ago).
        let old_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 600; // 10 minutes ago

        let actions = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];

        let session = ReconcileSession {
            session_id: "rs-stale".to_string(),
            stack_name: "myapp".to_string(),
            operation_id: "op-stale".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: "hash-stale".to_string(),
            next_action_index: 0,
            total_actions: 1,
            started_at: old_time,
            updated_at: old_time,
            completed_at: None,
        };
        store.create_reconcile_session(&session, &actions).unwrap();

        let findings = store.verify_startup_drift("myapp").unwrap();
        let stale: Vec<_> = findings
            .iter()
            .filter(|f| f.category == "reconcile" && f.description.contains("stale"))
            .collect();
        assert_eq!(stale.len(), 1);
        assert!(matches!(stale[0].severity, DriftSeverity::Warning));
    }

    #[test]
    fn phase3_drift_orphaned_health_state() {
        let store = StateStore::in_memory().unwrap();

        // Save health state but no desired state.
        let mut health = HashMap::new();
        health.insert(
            "web".to_string(),
            HealthPollState {
                service_name: "web".to_string(),
                consecutive_passes: 1,
                consecutive_failures: 0,
                last_check_millis: Some(1_700_000_000_000),
                start_time_millis: None,
            },
        );
        store.save_health_poller_state("myapp", &health).unwrap();

        let findings = store.verify_startup_drift("myapp").unwrap();
        let orphaned: Vec<_> = findings
            .iter()
            .filter(|f| f.category == "health" && f.description.contains("orphaned"))
            .collect();
        assert_eq!(orphaned.len(), 1);
        assert!(matches!(orphaned[0].severity, DriftSeverity::Info));
    }

    #[test]
    fn phase3_drift_clean_state_returns_no_findings() {
        let store = StateStore::in_memory().unwrap();

        // Save desired state with matching observed state.
        store.save_desired_state("myapp", &sample_spec()).unwrap();
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
        store
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    service_name: "db".to_string(),
                    phase: ServicePhase::Running,
                    container_id: Some("ctr-2".to_string()),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();

        let findings = store.verify_startup_drift("myapp").unwrap();
        assert!(
            findings.is_empty(),
            "expected no drift findings in clean state, got: {findings:?}"
        );
    }

    #[test]
    fn phase3_drift_nonexistent_stack_returns_no_findings() {
        let store = StateStore::in_memory().unwrap();
        let findings = store.verify_startup_drift("nonexistent").unwrap();
        assert!(findings.is_empty());
    }

    #[test]
    fn phase3_drift_finding_serialization_round_trip() {
        let finding = DriftFinding {
            category: "observed_state".to_string(),
            description: "orphaned service".to_string(),
            severity: DriftSeverity::Warning,
        };

        let json = serde_json::to_string(&finding).unwrap();
        let loaded: DriftFinding = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.category, "observed_state");
        assert_eq!(loaded.description, "orphaned service");
        assert!(matches!(loaded.severity, DriftSeverity::Warning));
    }

    #[test]
    fn phase3_drift_event_emission() {
        let store = StateStore::in_memory().unwrap();

        // Create a drift finding and emit as event.
        let finding = DriftFinding {
            category: "desired_state".to_string(),
            description: "desired state without observations".to_string(),
            severity: DriftSeverity::Warning,
        };

        let event = StackEvent::DriftDetected {
            stack_name: "myapp".to_string(),
            category: finding.category.clone(),
            description: finding.description.clone(),
            severity: finding.severity.as_str().to_string(),
        };

        store.emit_event("myapp", &event).unwrap();

        let events = store.load_events("myapp").unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], StackEvent::DriftDetected { .. }));
    }

    // ── Part 1: Audit log CRUD tests (vz-v2n.3.1) ──

    fn make_audit_entry(
        session_id: &str,
        stack_name: &str,
        action_index: usize,
        action_kind: &str,
        service_name: &str,
    ) -> ReconcileAuditEntry {
        ReconcileAuditEntry {
            id: 0, // auto-generated on insert
            session_id: session_id.to_string(),
            stack_name: stack_name.to_string(),
            action_index,
            action_kind: action_kind.to_string(),
            service_name: service_name.to_string(),
            action_hash: format!("hash-{action_index}"),
            status: "started".to_string(),
            started_at: 1_700_000_000 + action_index as u64,
            completed_at: None,
            error_message: None,
        }
    }

    #[test]
    fn audit_log_start_and_load() {
        let store = StateStore::in_memory().unwrap();

        let entry = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
        let id = store.log_reconcile_action_start(&entry).unwrap();
        assert!(id > 0);

        let log = store.load_audit_log_for_session("sess-1").unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].session_id, "sess-1");
        assert_eq!(log[0].action_kind, "service_create");
        assert_eq!(log[0].service_name, "web");
        assert_eq!(log[0].status, "started");
        assert!(log[0].completed_at.is_none());
        assert!(log[0].error_message.is_none());
    }

    #[test]
    fn audit_log_complete_success() {
        let store = StateStore::in_memory().unwrap();

        let entry = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
        let id = store.log_reconcile_action_start(&entry).unwrap();
        store.log_reconcile_action_complete(id, None).unwrap();

        let log = store.load_audit_log_for_session("sess-1").unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].status, "completed");
        assert!(log[0].completed_at.is_some());
        assert!(log[0].error_message.is_none());
    }

    #[test]
    fn audit_log_complete_failure() {
        let store = StateStore::in_memory().unwrap();

        let entry = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
        let id = store.log_reconcile_action_start(&entry).unwrap();
        store
            .log_reconcile_action_complete(id, Some("container start failed"))
            .unwrap();

        let log = store.load_audit_log_for_session("sess-1").unwrap();
        assert_eq!(log[0].status, "failed");
        assert!(log[0].completed_at.is_some());
        assert_eq!(
            log[0].error_message.as_deref(),
            Some("container start failed")
        );
    }

    #[test]
    fn audit_log_multiple_entries_ordered_by_action_index() {
        let store = StateStore::in_memory().unwrap();

        // Insert out of order to verify ORDER BY
        let e2 = make_audit_entry("sess-1", "myapp", 2, "service_remove", "cache");
        let e0 = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
        let e1 = make_audit_entry("sess-1", "myapp", 1, "service_create", "db");

        store.log_reconcile_action_start(&e2).unwrap();
        store.log_reconcile_action_start(&e0).unwrap();
        store.log_reconcile_action_start(&e1).unwrap();

        let log = store.load_audit_log_for_session("sess-1").unwrap();
        assert_eq!(log.len(), 3);
        assert_eq!(log[0].action_index, 0);
        assert_eq!(log[1].action_index, 1);
        assert_eq!(log[2].action_index, 2);
    }

    #[test]
    fn audit_log_scoped_by_session() {
        let store = StateStore::in_memory().unwrap();

        let e1 = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
        let e2 = make_audit_entry("sess-2", "myapp", 0, "service_create", "api");

        store.log_reconcile_action_start(&e1).unwrap();
        store.log_reconcile_action_start(&e2).unwrap();

        let log1 = store.load_audit_log_for_session("sess-1").unwrap();
        assert_eq!(log1.len(), 1);
        assert_eq!(log1[0].service_name, "web");

        let log2 = store.load_audit_log_for_session("sess-2").unwrap();
        assert_eq!(log2.len(), 1);
        assert_eq!(log2[0].service_name, "api");
    }

    #[test]
    fn audit_log_recent_by_stack() {
        let store = StateStore::in_memory().unwrap();

        for i in 0..5 {
            let entry = make_audit_entry(
                &format!("sess-{i}"),
                "myapp",
                0,
                "service_create",
                &format!("svc-{i}"),
            );
            store.log_reconcile_action_start(&entry).unwrap();
        }

        // Other stack should not appear
        let other = make_audit_entry("sess-other", "otherapp", 0, "service_create", "web");
        store.log_reconcile_action_start(&other).unwrap();

        let recent = store.load_recent_audit_log("myapp", 3).unwrap();
        assert_eq!(recent.len(), 3);
        // Newest first (DESC)
        assert!(recent[0].id > recent[1].id);
        assert!(recent[1].id > recent[2].id);
    }

    #[test]
    fn audit_log_empty_session_returns_empty() {
        let store = StateStore::in_memory().unwrap();
        let log = store.load_audit_log_for_session("nonexistent").unwrap();
        assert!(log.is_empty());
    }

    // ── Part 2: Recovery fault-injection tests (vz-v2n.3.2) ──

    #[test]
    fn recovery_crash_during_apply_actions_partially_persisted() {
        let store = StateStore::in_memory().unwrap();

        // Create session with 3 actions
        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
            Action::ServiceCreate {
                service_name: "cache".to_string(),
            },
        ];
        let session = ReconcileSession {
            session_id: "rs-crash-1".to_string(),
            stack_name: "myapp".to_string(),
            operation_id: "op-1".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: crate::compute_actions_hash(&actions),
            next_action_index: 0,
            total_actions: 3,
            started_at: 1_700_000_000,
            updated_at: 1_700_000_000,
            completed_at: None,
        };
        store.create_reconcile_session(&session, &actions).unwrap();

        // Action 0: started + completed
        let e0 = make_audit_entry("rs-crash-1", "myapp", 0, "service_create", "web");
        let id0 = store.log_reconcile_action_start(&e0).unwrap();
        store.log_reconcile_action_complete(id0, None).unwrap();

        // Action 1: started + completed
        let e1 = make_audit_entry("rs-crash-1", "myapp", 1, "service_create", "db");
        let id1 = store.log_reconcile_action_start(&e1).unwrap();
        store.log_reconcile_action_complete(id1, None).unwrap();

        // Action 2: started but NOT completed (crash simulation)
        let e2 = make_audit_entry("rs-crash-1", "myapp", 2, "service_create", "cache");
        store.log_reconcile_action_start(&e2).unwrap();

        // Update progress to reflect that we were partway through
        store
            .update_reconcile_session_progress("rs-crash-1", 2, &ReconcileSessionStatus::Active)
            .unwrap();

        // Verify: session is still active (crash recovery)
        let active = store
            .load_active_reconcile_session("myapp")
            .unwrap()
            .unwrap();
        assert_eq!(active.session_id, "rs-crash-1");
        assert_eq!(active.status, ReconcileSessionStatus::Active);

        // Verify: audit log shows 2 completed, 1 started
        let log = store.load_audit_log_for_session("rs-crash-1").unwrap();
        assert_eq!(log.len(), 3);
        assert_eq!(log[0].status, "completed");
        assert_eq!(log[1].status, "completed");
        assert_eq!(log[2].status, "started"); // crash point

        // Verify: next_action_index points to the right place
        assert_eq!(active.next_action_index, 2);
    }

    #[test]
    fn recovery_restart_with_partial_batch_resumes_from_cursor() {
        let store = StateStore::in_memory().unwrap();

        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
            Action::ServiceCreate {
                service_name: "cache".to_string(),
            },
        ];
        let session = ReconcileSession {
            session_id: "rs-resume-1".to_string(),
            stack_name: "myapp".to_string(),
            operation_id: "op-2".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: crate::compute_actions_hash(&actions),
            next_action_index: 0,
            total_actions: 3,
            started_at: 1_700_000_000,
            updated_at: 1_700_000_000,
            completed_at: None,
        };
        store.create_reconcile_session(&session, &actions).unwrap();

        // Complete action 0, advance cursor
        let e0 = make_audit_entry("rs-resume-1", "myapp", 0, "service_create", "web");
        let id0 = store.log_reconcile_action_start(&e0).unwrap();
        store.log_reconcile_action_complete(id0, None).unwrap();
        store
            .update_reconcile_session_progress("rs-resume-1", 1, &ReconcileSessionStatus::Active)
            .unwrap();

        // Simulate restart: load active session
        let resumed = store
            .load_active_reconcile_session("myapp")
            .unwrap()
            .unwrap();
        assert_eq!(resumed.next_action_index, 1);
        assert_eq!(resumed.total_actions, 3);

        // Verify remaining actions via audit log
        let log = store.load_audit_log_for_session("rs-resume-1").unwrap();
        let completed_count = log.iter().filter(|e| e.status == "completed").count();
        assert_eq!(completed_count, 1);
        // Remaining = total - cursor
        let remaining = resumed.total_actions - resumed.next_action_index;
        assert_eq!(remaining, 2);
    }

    #[test]
    fn recovery_crash_during_health_polling_state_preserved() {
        let store = StateStore::in_memory().unwrap();

        let mut health_state = HashMap::new();
        health_state.insert(
            "web".to_string(),
            HealthPollState {
                service_name: "web".to_string(),
                consecutive_passes: 3,
                consecutive_failures: 0,
                last_check_millis: Some(1_700_000_000_000),
                start_time_millis: Some(1_700_000_000_100),
            },
        );
        health_state.insert(
            "db".to_string(),
            HealthPollState {
                service_name: "db".to_string(),
                consecutive_passes: 1,
                consecutive_failures: 2,
                last_check_millis: Some(1_700_000_000_500),
                start_time_millis: Some(1_700_000_000_200),
            },
        );
        store
            .save_health_poller_state("myapp", &health_state)
            .unwrap();

        // Simulate crash: just reload from store (in-memory is still there)
        let restored = store.load_health_poller_state("myapp").unwrap();
        assert_eq!(restored.len(), 2);
        let web = restored.get("web").unwrap();
        assert_eq!(web.consecutive_passes, 3);
        assert_eq!(web.consecutive_failures, 0);
        let db = restored.get("db").unwrap();
        assert_eq!(db.consecutive_passes, 1);
        assert_eq!(db.consecutive_failures, 2);
    }

    #[test]
    fn recovery_port_conflict_replay_after_restart() {
        let store = StateStore::in_memory().unwrap();

        let mut ports = HashMap::new();
        ports.insert(
            "web".to_string(),
            vec![PublishedPort {
                host_port: 8080,
                container_port: 80,
                protocol: "tcp".to_string(),
            }],
        );
        ports.insert(
            "api".to_string(),
            vec![PublishedPort {
                host_port: 3000,
                container_port: 3000,
                protocol: "tcp".to_string(),
            }],
        );
        let snapshot = AllocatorSnapshot {
            ports: ports.clone(),
            service_ips: HashMap::from([
                ("web".to_string(), "10.0.0.2".to_string()),
                ("api".to_string(), "10.0.0.3".to_string()),
            ]),
            mount_tag_offsets: HashMap::from([("web".to_string(), 0), ("api".to_string(), 1)]),
        };
        store.save_allocator_state("myapp", &snapshot).unwrap();

        // Simulate restart: reload
        let restored = store.load_allocator_state("myapp").unwrap().unwrap();
        assert_eq!(restored.ports, snapshot.ports);
        assert_eq!(restored.service_ips, snapshot.service_ips);
        assert_eq!(restored.mount_tag_offsets, snapshot.mount_tag_offsets);
    }

    #[test]
    fn recovery_dependency_blocked_replay_after_restart() {
        let store = StateStore::in_memory().unwrap();

        let spec = StackSpec {
            name: "myapp".to_string(),
            services: vec![
                ServiceSpec {
                    name: "web".to_string(),
                    kind: ServiceKind::Service,
                    image: "nginx:latest".to_string(),
                    depends_on: vec![crate::spec::ServiceDependency {
                        service: "db".to_string(),
                        condition: crate::spec::DependencyCondition::ServiceHealthy,
                    }],
                    command: None,
                    entrypoint: None,
                    environment: HashMap::new(),
                    working_dir: None,
                    user: None,
                    mounts: vec![],
                    ports: vec![],
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
                    depends_on: vec![],
                    command: None,
                    entrypoint: None,
                    environment: HashMap::new(),
                    working_dir: None,
                    user: None,
                    mounts: vec![],
                    ports: vec![],
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
        };
        store.save_desired_state("myapp", &spec).unwrap();

        // Simulate restart: reload desired state and verify dependencies
        let restored = store.load_desired_state("myapp").unwrap().unwrap();
        assert_eq!(restored.services.len(), 2);
        let web = restored.services.iter().find(|s| s.name == "web").unwrap();
        assert_eq!(web.depends_on.len(), 1);
        assert_eq!(web.depends_on[0].service, "db");
        assert_eq!(
            web.depends_on[0].condition,
            crate::spec::DependencyCondition::ServiceHealthy
        );
    }

    #[test]
    fn recovery_superseded_session_cleanup() {
        let store = StateStore::in_memory().unwrap();

        // First session
        let actions1 = vec![Action::ServiceCreate {
            service_name: "web".to_string(),
        }];
        let session1 = ReconcileSession {
            session_id: "rs-old-1".to_string(),
            stack_name: "myapp".to_string(),
            operation_id: "op-old".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: crate::compute_actions_hash(&actions1),
            next_action_index: 0,
            total_actions: 1,
            started_at: 1_700_000_000,
            updated_at: 1_700_000_000,
            completed_at: None,
        };
        store
            .create_reconcile_session(&session1, &actions1)
            .unwrap();

        // Audit entries for old session
        let e_old = make_audit_entry("rs-old-1", "myapp", 0, "service_create", "web");
        store.log_reconcile_action_start(&e_old).unwrap();

        // Supersede the old session
        let superseded_count = store.supersede_active_sessions("myapp").unwrap();
        assert_eq!(superseded_count, 1);

        // Create new session
        let actions2 = vec![Action::ServiceRecreate {
            service_name: "web".to_string(),
        }];
        let session2 = ReconcileSession {
            session_id: "rs-new-1".to_string(),
            stack_name: "myapp".to_string(),
            operation_id: "op-new".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: crate::compute_actions_hash(&actions2),
            next_action_index: 0,
            total_actions: 1,
            started_at: 1_700_001_000,
            updated_at: 1_700_001_000,
            completed_at: None,
        };
        store
            .create_reconcile_session(&session2, &actions2)
            .unwrap();

        // Verify old audit entries are still queryable
        let old_log = store.load_audit_log_for_session("rs-old-1").unwrap();
        assert_eq!(old_log.len(), 1);
        assert_eq!(old_log[0].service_name, "web");

        // Verify only new session is active
        let active = store
            .load_active_reconcile_session("myapp")
            .unwrap()
            .unwrap();
        assert_eq!(active.session_id, "rs-new-1");

        // Verify old session is superseded
        let all_sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
        assert_eq!(all_sessions.len(), 2);
        let old_sess = all_sessions
            .iter()
            .find(|s| s.session_id == "rs-old-1")
            .unwrap();
        assert_eq!(old_sess.status, ReconcileSessionStatus::Superseded);
    }

    // ── Part 3: Phase 3 recovery proof validation (vz-v2n.3.3) ──

    #[test]
    fn phase3_validation_full_recovery_lifecycle() {
        let store = StateStore::in_memory().unwrap();

        // 1. Create stack with desired state
        let spec = sample_spec();
        store.save_desired_state("myapp", &spec).unwrap();

        // 2. Create reconcile session with 3 actions
        let actions = vec![
            Action::ServiceCreate {
                service_name: "web".to_string(),
            },
            Action::ServiceCreate {
                service_name: "db".to_string(),
            },
            Action::ServiceRemove {
                service_name: "old-svc".to_string(),
            },
        ];
        let session = ReconcileSession {
            session_id: "rs-full-1".to_string(),
            stack_name: "myapp".to_string(),
            operation_id: "op-full".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: crate::compute_actions_hash(&actions),
            next_action_index: 0,
            total_actions: 3,
            started_at: 1_700_000_000,
            updated_at: 1_700_000_000,
            completed_at: None,
        };
        store.create_reconcile_session(&session, &actions).unwrap();

        // 3. Log action starts and completions with audit entries
        for (idx, action) in actions.iter().enumerate() {
            let kind = match action {
                Action::ServiceCreate { .. } => "service_create",
                Action::ServiceRecreate { .. } => "service_recreate",
                Action::ServiceRemove { .. } => "service_remove",
            };
            let entry = make_audit_entry("rs-full-1", "myapp", idx, kind, action.service_name());
            let id = store.log_reconcile_action_start(&entry).unwrap();
            store.log_reconcile_action_complete(id, None).unwrap();
            store
                .update_reconcile_session_progress(
                    "rs-full-1",
                    idx + 1,
                    &ReconcileSessionStatus::Active,
                )
                .unwrap();
        }

        // 4. Mark session completed
        store.complete_reconcile_session("rs-full-1").unwrap();

        // 5. Verify: audit log is complete and ordered
        let log = store.load_audit_log_for_session("rs-full-1").unwrap();
        assert_eq!(log.len(), 3);
        for (idx, entry) in log.iter().enumerate() {
            assert_eq!(entry.action_index, idx);
            assert_eq!(entry.status, "completed");
            assert!(entry.completed_at.is_some());
        }
        assert_eq!(log[0].action_kind, "service_create");
        assert_eq!(log[0].service_name, "web");
        assert_eq!(log[1].action_kind, "service_create");
        assert_eq!(log[1].service_name, "db");
        assert_eq!(log[2].action_kind, "service_remove");
        assert_eq!(log[2].service_name, "old-svc");

        // 6. Verify: session has correct completed_at
        let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
        let completed_sess = sessions
            .iter()
            .find(|s| s.session_id == "rs-full-1")
            .unwrap();
        assert_eq!(completed_sess.status, ReconcileSessionStatus::Completed);
        assert!(completed_sess.completed_at.is_some());

        // 7. Create second session (simulating next apply)
        store.supersede_active_sessions("myapp").unwrap(); // no-op: already completed
        let actions2 = vec![Action::ServiceRecreate {
            service_name: "web".to_string(),
        }];
        let session2 = ReconcileSession {
            session_id: "rs-full-2".to_string(),
            stack_name: "myapp".to_string(),
            operation_id: "op-full-2".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: crate::compute_actions_hash(&actions2),
            next_action_index: 0,
            total_actions: 1,
            started_at: 1_700_001_000,
            updated_at: 1_700_001_000,
            completed_at: None,
        };
        store
            .create_reconcile_session(&session2, &actions2)
            .unwrap();

        // 8. Verify: old session is completed (not superseded since it was already done),
        //    new session is active
        let all = store.list_reconcile_sessions("myapp", 10).unwrap();
        assert_eq!(all.len(), 2);
        let old = all.iter().find(|s| s.session_id == "rs-full-1").unwrap();
        assert_eq!(old.status, ReconcileSessionStatus::Completed);
        let new = all.iter().find(|s| s.session_id == "rs-full-2").unwrap();
        assert_eq!(new.status, ReconcileSessionStatus::Active);

        // 9. Verify: drift check returns clean for correct state
        //    Save observed state matching desired state
        for svc in &spec.services {
            store
                .save_observed_state(
                    "myapp",
                    &ServiceObservedState {
                        service_name: svc.name.clone(),
                        phase: ServicePhase::Running,
                        container_id: Some(format!("ctr-{}", svc.name)),
                        last_error: None,
                        ready: true,
                    },
                )
                .unwrap();
        }
        let findings = store.verify_startup_drift("myapp").unwrap();
        // The only finding should be about the active session (if stale).
        // Since the new session was just created, no stale session warning.
        // Both desired services have observed state, so no orphan warnings.
        let non_stale: Vec<_> = findings
            .iter()
            .filter(|f| f.category != "reconcile")
            .collect();
        assert!(
            non_stale.is_empty(),
            "unexpected drift findings: {non_stale:?}"
        );
    }

    // ── Part 4: Phase 2 schema/drift validation (vz-v2n.2.3) ──

    #[test]
    fn phase2_validation_schema_version_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test-state.db");

        {
            let store = StateStore::open(&db_path).unwrap();
            store.set_schema_version(2).unwrap();
            assert_eq!(store.schema_version().unwrap(), 2);
        }
        // Drop store (close connection), reopen
        {
            let store = StateStore::open(&db_path).unwrap();
            assert_eq!(store.schema_version().unwrap(), 2);
        }
    }

    #[test]
    fn phase2_validation_drift_desired_without_observed() {
        let store = StateStore::in_memory().unwrap();

        // Save desired state, don't save observed state
        let spec = sample_spec();
        store.save_desired_state("myapp", &spec).unwrap();

        let findings = store.verify_startup_drift("myapp").unwrap();
        let desired_drift: Vec<_> = findings
            .iter()
            .filter(|f| f.category == "desired_state")
            .collect();
        assert_eq!(desired_drift.len(), 1);
        assert!(
            desired_drift[0]
                .description
                .contains("desired state without observations")
        );
        assert_eq!(desired_drift[0].severity, DriftSeverity::Warning);
    }

    #[test]
    fn phase2_validation_drift_orphaned_observed() {
        let store = StateStore::in_memory().unwrap();

        // Save desired state (only "web" and "db")
        let spec = sample_spec();
        store.save_desired_state("myapp", &spec).unwrap();

        // Save observed state for services including one not in desired state
        for name in &["web", "db", "orphaned-svc"] {
            store
                .save_observed_state(
                    "myapp",
                    &ServiceObservedState {
                        service_name: name.to_string(),
                        phase: ServicePhase::Running,
                        container_id: Some(format!("ctr-{name}")),
                        last_error: None,
                        ready: true,
                    },
                )
                .unwrap();
        }

        let findings = store.verify_startup_drift("myapp").unwrap();
        let orphaned: Vec<_> = findings
            .iter()
            .filter(|f| f.category == "observed_state")
            .collect();
        assert_eq!(orphaned.len(), 1);
        assert!(
            orphaned[0]
                .description
                .contains("orphaned observed state for service 'orphaned-svc'")
        );
        assert_eq!(orphaned[0].severity, DriftSeverity::Warning);
    }

    #[test]
    fn phase2_validation_event_queries_after_migration() {
        let store = StateStore::in_memory().unwrap();

        // Emit events
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
                &StackEvent::ServiceCreating {
                    stack_name: "myapp".to_string(),
                    service_name: "web".to_string(),
                },
            )
            .unwrap();

        // Verify load_events works
        let events = store.load_events("myapp").unwrap();
        assert_eq!(events.len(), 2);

        // Verify load_events_since works
        let records = store.load_event_records("myapp").unwrap();
        let since = store.load_events_since("myapp", records[0].id).unwrap();
        assert_eq!(since.len(), 1);
        assert!(matches!(since[0].event, StackEvent::ServiceCreating { .. }));

        // Set schema version and verify queries still work
        store.set_schema_version(3).unwrap();
        assert_eq!(store.schema_version().unwrap(), 3);

        let events_after = store.load_events("myapp").unwrap();
        assert_eq!(events_after.len(), 2);

        let since_after = store.load_events_since("myapp", records[0].id).unwrap();
        assert_eq!(since_after.len(), 1);
    }

    // ── Capacity and regression tests (vz-lbg) ─────────────────────

    fn make_service(name: &str) -> ServiceSpec {
        ServiceSpec {
            name: name.to_string(),
            kind: ServiceKind::Service,
            image: format!("{name}:latest"),
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
        }
    }

    /// Insert 10,000 events into a single stack and verify that cursor-based
    /// queries remain performant (complete within a generous wall-clock bound).
    #[test]
    fn capacity_10k_events_query_performance() {
        let store = StateStore::in_memory().unwrap();

        // Insert 10,000 events.
        let start_insert = std::time::Instant::now();
        for i in 0..10_000 {
            store
                .emit_event(
                    "perf-app",
                    &StackEvent::ServiceCreating {
                        stack_name: "perf-app".to_string(),
                        service_name: format!("svc-{i}"),
                    },
                )
                .unwrap();
        }
        let insert_elapsed = start_insert.elapsed();
        // Generous bound: 10,000 inserts should complete within 10 seconds on CI.
        assert!(
            insert_elapsed.as_secs() < 10,
            "10,000 event inserts took {insert_elapsed:?} (>10s budget)"
        );

        // Count should be exact.
        assert_eq!(store.event_count("perf-app").unwrap(), 10_000);

        // Cursor-based query from midpoint should be fast.
        let start_query = std::time::Instant::now();
        let page = store
            .load_events_since_limited("perf-app", 5000, 100)
            .unwrap();
        let query_elapsed = start_query.elapsed();
        assert_eq!(page.len(), 100);
        // Query should complete in well under 1 second.
        assert!(
            query_elapsed.as_millis() < 1000,
            "cursor query after 10k events took {query_elapsed:?} (>1s budget)"
        );

        // Full-table scan should also be bounded.
        let start_all = std::time::Instant::now();
        let _all_records = store.load_event_records("perf-app").unwrap();
        let all_elapsed = start_all.elapsed();
        assert!(
            all_elapsed.as_secs() < 5,
            "full load of 10k event records took {all_elapsed:?} (>5s budget)"
        );
    }

    /// Verify that 100 concurrent stacks maintain isolation and perform
    /// adequately for save/load operations.
    #[test]
    fn capacity_100_concurrent_stacks_isolation() {
        let store = StateStore::in_memory().unwrap();

        let start = std::time::Instant::now();

        // Create 100 stacks, each with a unique spec.
        for i in 0..100 {
            let name = format!("stack-{i}");
            let spec = StackSpec {
                name: name.clone(),
                services: vec![make_service(&format!("svc-{i}"))],
                networks: vec![],
                volumes: vec![],
                secrets: vec![],
                disk_size_mb: None,
            };
            store.save_desired_state(&name, &spec).unwrap();

            // Emit a couple events per stack.
            store
                .emit_event(
                    &name,
                    &StackEvent::StackApplyStarted {
                        stack_name: name.clone(),
                        services_count: 1,
                    },
                )
                .unwrap();
            store
                .emit_event(
                    &name,
                    &StackEvent::StackApplyCompleted {
                        stack_name: name.clone(),
                        succeeded: 1,
                        failed: 0,
                    },
                )
                .unwrap();

            // Save observed state.
            store
                .save_observed_state(
                    &name,
                    &ServiceObservedState {
                        service_name: format!("svc-{i}"),
                        phase: ServicePhase::Running,
                        container_id: Some(format!("ctr-{i}")),
                        last_error: None,
                        ready: true,
                    },
                )
                .unwrap();
        }

        let setup_elapsed = start.elapsed();
        assert!(
            setup_elapsed.as_secs() < 10,
            "setting up 100 stacks took {setup_elapsed:?} (>10s budget)"
        );

        // Verify isolation: each stack has its own events.
        for i in 0..100 {
            let name = format!("stack-{i}");
            let events = store.load_events(&name).unwrap();
            assert_eq!(events.len(), 2, "stack-{i} should have exactly 2 events");

            let observed = store.load_observed_state(&name).unwrap();
            assert_eq!(
                observed.len(),
                1,
                "stack-{i} should have exactly 1 observed state"
            );
            assert_eq!(observed[0].service_name, format!("svc-{i}"));
        }

        // Verify load for a random stack in the middle is fast.
        let start_load = std::time::Instant::now();
        let loaded = store.load_desired_state("stack-50").unwrap().unwrap();
        let load_elapsed = start_load.elapsed();
        assert_eq!(loaded.name, "stack-50");
        assert!(
            load_elapsed.as_millis() < 100,
            "loading stack-50 among 100 stacks took {load_elapsed:?} (>100ms budget)"
        );
    }

    /// Verify that a large desired state (50+ services) round-trips
    /// correctly through save/load with acceptable performance.
    #[test]
    fn capacity_large_desired_state_50_services() {
        let store = StateStore::in_memory().unwrap();

        let services: Vec<ServiceSpec> =
            (0..50).map(|i| make_service(&format!("svc-{i}"))).collect();
        let spec = StackSpec {
            name: "large-app".to_string(),
            services,
            networks: vec![NetworkSpec {
                name: "default".to_string(),
                driver: "bridge".to_string(),
                subnet: None,
            }],
            volumes: vec![VolumeSpec {
                name: "data".to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            }],
            secrets: vec![],
            disk_size_mb: Some(20480),
        };

        let start = std::time::Instant::now();
        store.save_desired_state("large-app", &spec).unwrap();
        let loaded = store.load_desired_state("large-app").unwrap().unwrap();
        let elapsed = start.elapsed();

        assert_eq!(loaded, spec);
        assert_eq!(loaded.services.len(), 50);
        assert!(
            elapsed.as_millis() < 500,
            "large spec (50 services) save+load took {elapsed:?} (>500ms budget)"
        );

        // Upsert to verify update path is also performant.
        let start_upsert = std::time::Instant::now();
        store.save_desired_state("large-app", &spec).unwrap();
        let upsert_elapsed = start_upsert.elapsed();
        assert!(
            upsert_elapsed.as_millis() < 500,
            "large spec upsert took {upsert_elapsed:?} (>500ms budget)"
        );
    }

    /// Regression: 1,000 event inserts must complete within 500ms.
    #[test]
    fn regression_1000_event_inserts_under_500ms() {
        let store = StateStore::in_memory().unwrap();

        let start = std::time::Instant::now();
        for i in 0..1_000 {
            store
                .emit_event(
                    "regression-app",
                    &StackEvent::ServiceCreating {
                        stack_name: "regression-app".to_string(),
                        service_name: format!("svc-{i}"),
                    },
                )
                .unwrap();
        }
        let elapsed = start.elapsed();

        assert!(
            elapsed.as_millis() < 500,
            "1,000 event inserts took {elapsed:?} — exceeds 500ms regression gate"
        );
    }

    /// Regression: idempotency key lookup among 500 keys must be under 50ms.
    #[test]
    fn regression_idempotency_lookup_under_50ms() {
        let store = StateStore::in_memory().unwrap();

        for i in 0..500 {
            let record = IdempotencyRecord {
                key: format!("idem-key-{i}"),
                operation: "create_sandbox".to_string(),
                request_hash: format!("hash-{i}"),
                response_json: r#"{"sandbox_id":"sb-1"}"#.to_string(),
                status_code: 201,
                created_at: 1_700_000_000,
                expires_at: 1_700_000_000 + IDEMPOTENCY_TTL_SECS,
            };
            store.save_idempotency_result(&record).unwrap();
        }

        let start = std::time::Instant::now();
        let result = store.find_idempotency_result("idem-key-250").unwrap();
        let elapsed = start.elapsed();

        assert!(result.is_some());
        assert!(
            elapsed.as_millis() < 50,
            "idempotency lookup among 500 keys took {elapsed:?} — exceeds 50ms regression gate"
        );
    }

    /// Regression: saving and loading observed state for 20 services
    /// must complete within 200ms.
    #[test]
    fn regression_observed_state_20_services_under_200ms() {
        let store = StateStore::in_memory().unwrap();

        let start = std::time::Instant::now();
        for i in 0..20 {
            let state = ServiceObservedState {
                service_name: format!("svc-{i}"),
                phase: ServicePhase::Running,
                container_id: Some(format!("ctr-{i}")),
                last_error: None,
                ready: true,
            };
            store.save_observed_state("regression-app", &state).unwrap();
        }
        let loaded = store.load_observed_state("regression-app").unwrap();
        let elapsed = start.elapsed();

        assert_eq!(loaded.len(), 20);
        assert!(
            elapsed.as_millis() < 200,
            "20 observed state save+load took {elapsed:?} — exceeds 200ms regression gate"
        );
    }
}
