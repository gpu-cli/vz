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

use crate::StackSpec;
use crate::error::StackError;
use crate::events::{EventRecord, StackEvent};
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

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stack_name TEXT NOT NULL,
                event_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
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
}
