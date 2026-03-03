//! SQLite-backed state store for desired and observed stack state.
//!
//! Provides durable persistence for the reconciliation loop:
//! - **Desired state**: the user-specified [`StackSpec`](crate::StackSpec)
//! - **Observed state**: per-service runtime state from the reconciler
//! - **Events**: structured lifecycle events for observability

use std::collections::HashMap;
use std::path::Path;
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use vz_runtime_contract::{
    Build, BuildSpec, BuildState, Checkpoint, CheckpointClass, CheckpointFileEntry,
    CheckpointState, Container, ContainerSpec, ContainerState, Execution, ExecutionSpec,
    ExecutionState, Lease, LeaseState, MachineErrorCode, Sandbox, SandboxBackend, SandboxSpec,
    SandboxState,
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

/// Deterministic reason why a record is selected for retention GC.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RetentionGcReason {
    /// Exceeded age-based retention threshold.
    AgeLimit,
    /// Exceeded count-based retention threshold.
    CountLimit,
    /// Cascaded from lineage-parent deletion.
    LineageCascade,
}

impl RetentionGcReason {
    /// Stable wire/storage string.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AgeLimit => "age_limit",
            Self::CountLimit => "count_limit",
            Self::LineageCascade => "lineage_cascade",
        }
    }
}

/// Retention policy for checkpoints.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointRetentionPolicy {
    /// Maximum retained untagged checkpoints.
    pub max_untagged_count: usize,
    /// Maximum age for untagged checkpoints in seconds.
    pub max_age_secs: u64,
}

impl Default for CheckpointRetentionPolicy {
    fn default() -> Self {
        Self {
            max_untagged_count: 128,
            max_age_secs: 30 * 24 * 3600,
        }
    }
}

/// Retention policy for receipts.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptRetentionPolicy {
    /// Maximum retained receipts.
    pub max_count: usize,
    /// Maximum age for receipts in seconds.
    pub max_age_secs: u64,
}

impl Default for ReceiptRetentionPolicy {
    fn default() -> Self {
        Self {
            max_count: 20_000,
            max_age_secs: 14 * 24 * 3600,
        }
    }
}

/// Effective retention state for a checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointRetentionState {
    /// Tag value when checkpoint is explicitly retained.
    pub tag: Option<String>,
    /// Whether the checkpoint is protected from policy GC.
    pub protected: bool,
    /// Age-based retention deadline for untagged checkpoints.
    pub expires_at: Option<u64>,
    /// Current GC eligibility reason, if any.
    pub gc_reason: Option<RetentionGcReason>,
}

/// Effective retention state for a receipt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptRetentionState {
    /// Age-based retention deadline.
    pub expires_at: u64,
    /// Current GC eligibility reason, if any.
    pub gc_reason: Option<RetentionGcReason>,
}

/// Checkpoint GC result summary.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointGcReport {
    /// Checkpoints deleted by age policy.
    pub deleted_by_age: Vec<String>,
    /// Checkpoints deleted by count policy.
    pub deleted_by_count: Vec<String>,
    /// Checkpoints deleted due to lineage cascading from an already-selected ancestor.
    pub deleted_by_lineage: Vec<String>,
}

impl CheckpointGcReport {
    /// Total deleted checkpoints.
    pub fn total_deleted(&self) -> usize {
        self.deleted_by_age.len() + self.deleted_by_count.len() + self.deleted_by_lineage.len()
    }

    /// Whether no records were deleted.
    pub fn is_empty(&self) -> bool {
        self.total_deleted() == 0
    }
}

/// Receipt GC result summary.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptGcReport {
    /// Receipts deleted by age policy.
    pub deleted_by_age: Vec<String>,
    /// Receipts deleted by count policy.
    pub deleted_by_count: Vec<String>,
}

impl ReceiptGcReport {
    /// Total deleted receipts.
    pub fn total_deleted(&self) -> usize {
        self.deleted_by_age.len() + self.deleted_by_count.len()
    }

    /// Whether no records were deleted.
    pub fn is_empty(&self) -> bool {
        self.total_deleted() == 0
    }
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

/// SQLite PRAGMA policy applied during state-store startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StateStorePragmas {
    /// Enable WAL journal mode.
    pub journal_mode_wal: bool,
    /// Busy timeout in milliseconds.
    pub busy_timeout_ms: Option<u64>,
    /// Enable foreign-key enforcement.
    pub foreign_keys: bool,
}

impl StateStorePragmas {
    /// Daemon runtime defaults for single-writer, contention-tolerant startup.
    pub const fn daemon_defaults() -> Self {
        Self {
            journal_mode_wal: true,
            busy_timeout_ms: Some(5_000),
            foreign_keys: true,
        }
    }
}

impl StateStore {
    /// Open or create a state store at the given path.
    pub fn open(path: &Path) -> Result<Self, StackError> {
        Self::open_with_pragmas(path, StateStorePragmas::default())
    }

    /// Open or create a state store with explicit SQLite pragma policy.
    pub fn open_with_pragmas(path: &Path, pragmas: StateStorePragmas) -> Result<Self, StackError> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn,
            event_sender: None,
        };
        store.apply_pragmas(pragmas)?;
        store.init_schema()?;
        Ok(store)
    }

    /// Create an in-memory state store (useful for testing).
    pub fn in_memory() -> Result<Self, StackError> {
        Self::in_memory_with_pragmas(StateStorePragmas::default())
    }

    /// Create an in-memory state store with explicit SQLite pragma policy.
    pub fn in_memory_with_pragmas(pragmas: StateStorePragmas) -> Result<Self, StackError> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn,
            event_sender: None,
        };
        store.apply_pragmas(pragmas)?;
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

    fn apply_pragmas(&self, pragmas: StateStorePragmas) -> Result<(), StackError> {
        if pragmas.journal_mode_wal {
            self.conn.pragma_update(None, "journal_mode", "WAL")?;
        }
        if let Some(timeout_ms) = pragmas.busy_timeout_ms {
            self.conn.busy_timeout(Duration::from_millis(timeout_ms))?;
        }
        if pragmas.foreign_keys {
            self.conn.pragma_update(None, "foreign_keys", "ON")?;
        }
        Ok(())
    }

    /// Execute a closure within an immediate SQLite transaction boundary.
    ///
    /// The transaction is committed on success and rolled back on error.
    pub fn with_immediate_transaction<T>(
        &self,
        f: impl FnOnce(&Self) -> Result<T, StackError>,
    ) -> Result<T, StackError> {
        self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;
        match f(self) {
            Ok(value) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    /// Return effective SQLite journal mode for this connection.
    pub fn journal_mode(&self) -> Result<String, StackError> {
        self.conn
            .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
            .map_err(Into::into)
    }

    /// Return effective busy timeout (milliseconds) for this connection.
    pub fn busy_timeout_ms(&self) -> Result<u64, StackError> {
        self.conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get::<_, u64>(0))
            .map_err(Into::into)
    }

    /// Return whether foreign-key enforcement is enabled for this connection.
    pub fn foreign_keys_enabled(&self) -> Result<bool, StackError> {
        self.conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
            .map(|value| value != 0)
            .map_err(Into::into)
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
            CREATE INDEX IF NOT EXISTS idx_checkpoint_created_at ON checkpoint_state(created_at);

            CREATE TABLE IF NOT EXISTS checkpoint_retention_tags (
                checkpoint_id TEXT PRIMARY KEY,
                tag TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_checkpoint_retention_tag
                ON checkpoint_retention_tags(tag);

            CREATE TABLE IF NOT EXISTS checkpoint_file_entries (
                checkpoint_id TEXT NOT NULL,
                path TEXT NOT NULL,
                digest_sha256 TEXT NOT NULL,
                size INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (checkpoint_id, path)
            );
            CREATE INDEX IF NOT EXISTS idx_checkpoint_file_entries_checkpoint
                ON checkpoint_file_entries(checkpoint_id);

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
            CREATE INDEX IF NOT EXISTS idx_receipt_created_at ON receipt_state(created_at);

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

    /// Resolve compose `tty` default for a runtime container identifier.
    ///
    /// Returns `Some(tty)` when the container can be mapped to a service in
    /// observed state and that service exists in desired state.
    pub fn resolve_service_tty_for_container(
        &self,
        container_id: &str,
    ) -> Result<Option<bool>, StackError> {
        let container_id = container_id.trim();
        if container_id.is_empty() {
            return Ok(None);
        }

        let mut stmt = self
            .conn
            .prepare("SELECT stack_name, state_json FROM observed_state")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        for row in rows {
            let (stack_name, state_json) = row?;
            let observed: ServiceObservedState = serde_json::from_str(&state_json)?;
            if observed.container_id.as_deref() != Some(container_id) {
                continue;
            }

            let Some(desired) = self.load_desired_state(&stack_name)? else {
                continue;
            };
            let Some(service) = desired
                .services
                .iter()
                .find(|service| service.name == observed.service_name)
            else {
                continue;
            };

            return Ok(Some(service.tty));
        }

        Ok(None)
    }

    /// Resolve inherited execution PTY default for a runtime container identifier.
    ///
    /// Returns `Some(true)` when the mapped service requests interactive I/O
    /// via either `tty` or `stdin_open`.
    pub fn resolve_service_exec_pty_default_for_container(
        &self,
        container_id: &str,
    ) -> Result<Option<bool>, StackError> {
        let container_id = container_id.trim();
        if container_id.is_empty() {
            return Ok(None);
        }

        let mut stmt = self
            .conn
            .prepare("SELECT stack_name, state_json FROM observed_state")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        for row in rows {
            let (stack_name, state_json) = row?;
            let observed: ServiceObservedState = serde_json::from_str(&state_json)?;
            if observed.container_id.as_deref() != Some(container_id) {
                continue;
            }

            let Some(desired) = self.load_desired_state(&stack_name)? else {
                continue;
            };
            let Some(service) = desired
                .services
                .iter()
                .find(|service| service.name == observed.service_name)
            else {
                continue;
            };

            return Ok(Some(service.tty || service.stdin_open));
        }

        Ok(None)
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
}

mod drift;
mod persistence;

#[cfg(test)]
mod tests;
