//! SQLite-backed state store for desired and observed stack state.
//!
//! Provides durable persistence for the reconciliation loop:
//! - **Desired state**: the user-specified [`StackSpec`](crate::StackSpec)
//! - **Observed state**: per-service runtime state from the reconciler
//! - **Events**: structured lifecycle events for observability

use std::path::Path;

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::StackSpec;
use crate::error::StackError;
use crate::events::{EventRecord, StackEvent};

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

/// Durable state store backed by a single SQLite database file.
pub struct StateStore {
    conn: Connection,
}

impl StateStore {
    /// Open or create a state store at the given path.
    pub fn open(path: &Path) -> Result<Self, StackError> {
        let conn = Connection::open(path)?;
        let store = Self { conn };
        store.init_schema()?;
        Ok(store)
    }

    /// Create an in-memory state store (useful for testing).
    pub fn in_memory() -> Result<Self, StackError> {
        let conn = Connection::open_in_memory()?;
        let store = Self { conn };
        store.init_schema()?;
        Ok(store)
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
    pub fn emit_event(&self, stack_name: &str, event: &StackEvent) -> Result<(), StackError> {
        let json = serde_json::to_string(event)?;
        self.conn.execute(
            "INSERT INTO events (stack_name, event_json) VALUES (?1, ?2)",
            params![stack_name, json],
        )?;
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
    use crate::spec::{NetworkSpec, ServiceSpec, VolumeSpec};
    use std::collections::HashMap;

    fn sample_spec() -> StackSpec {
        StackSpec {
            name: "myapp".to_string(),
            services: vec![
                ServiceSpec {
                    name: "web".to_string(),
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
                },
                ServiceSpec {
                    name: "db".to_string(),
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
                },
            ],
            networks: vec![],
            volumes: vec![],
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
            }],
            volumes: vec![VolumeSpec {
                name: "vol1".to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            }],
        };

        store.save_desired_state("myapp", &spec2).unwrap();
        let loaded = store.load_desired_state("myapp").unwrap().unwrap();
        assert_eq!(loaded, spec2);
        assert!(loaded.services.is_empty());
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
        };
        let spec2 = StackSpec {
            name: "app2".to_string(),
            services: vec![],
            networks: vec![],
            volumes: vec![],
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
}
