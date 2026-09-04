use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use vz_runtime_contract::types::{
    EndpointInstance, EnvironmentInstance, EnvironmentLifecycleKind, EnvironmentLifecycleOperation,
    EnvironmentLifecycleStatus, EnvironmentSelection, EnvironmentSelectionContext,
    EnvironmentState, EnvironmentTombstone, EnvironmentUpDecision, LegacyMigrationError,
    LifecycleOperationId, LifecycleStepResult, LifecycleStepStatus, MachineInstance,
    MachineLifecycleStepAcknowledgement, NetworkInstance, OwnedResourceKind,
    OwnershipCleanupStepAcknowledgement, OwnershipRecord, ProjectDefinition, ProjectState,
    TOPOLOGY_SCHEMA_VERSION, TopologyLifecycleError, TopologyResolutionError, WorkspaceBinding,
    migrate_legacy_developer_sandbox,
};

use super::StateStore;
use crate::StackError;
use crate::error::OwnedResourceCollisionError;

pub(super) const STORE_SCHEMA_VERSION: u32 = 4;

/// Result of atomically selecting or reserving an Environment for `up`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvironmentUpReservation {
    Existing {
        selection: EnvironmentSelection,
        environment: EnvironmentInstance,
    },
    Created {
        environment: EnvironmentInstance,
    },
}

/// Topology tables common to state-store schema versions 2 and 3.
///
/// Keep version-specific ownership constraints out of this fragment so an
/// existing v2 database can be fingerprinted exactly before it is migrated.
pub(super) const TOPOLOGY_SCHEMA_COMMON_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS project_definitions (
    project_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    name TEXT NOT NULL CHECK(length(trim(name)) BETWEEN 1 AND 128),
    definition_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK(created_at >= 0 AND updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS environment_instances (
    environment_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    name TEXT NOT NULL CHECK(length(trim(name)) BETWEEN 1 AND 128),
    definition_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    instance_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    legacy_sandbox_id TEXT UNIQUE,
    UNIQUE(project_id, name),
    UNIQUE(environment_id, project_id),
    FOREIGN KEY(project_id) REFERENCES project_definitions(project_id) ON DELETE CASCADE,
    CHECK(created_at >= 0 AND updated_at >= created_at)
);
CREATE INDEX IF NOT EXISTS idx_environment_project
    ON environment_instances(project_id, created_at, environment_id);

CREATE TABLE IF NOT EXISTS workspace_bindings (
    binding_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    name TEXT NOT NULL CHECK(length(trim(name)) BETWEEN 1 AND 128),
    workspace_key TEXT NOT NULL CHECK(length(trim(workspace_key)) BETWEEN 1 AND 128),
    path_hint TEXT,
    binding_json TEXT NOT NULL,
    UNIQUE(environment_id, name),
    UNIQUE(environment_id, workspace_key),
    FOREIGN KEY(environment_id, project_id)
        REFERENCES environment_instances(environment_id, project_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_workspace_binding_selector
    ON workspace_bindings(project_id, workspace_key, environment_id);

CREATE TABLE IF NOT EXISTS machine_instances (
    machine_id TEXT PRIMARY KEY,
    environment_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    name TEXT NOT NULL CHECK(length(trim(name)) BETWEEN 1 AND 128),
    state TEXT NOT NULL,
    instance_json TEXT NOT NULL,
    legacy_sandbox_id TEXT UNIQUE,
    UNIQUE(environment_id, name),
    UNIQUE(environment_id, machine_id),
    FOREIGN KEY(environment_id) REFERENCES environment_instances(environment_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_machine_environment
    ON machine_instances(environment_id, name);

CREATE TABLE IF NOT EXISTS environment_networks (
    network_id TEXT PRIMARY KEY,
    environment_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    name TEXT NOT NULL CHECK(length(trim(name)) BETWEEN 1 AND 128),
    instance_json TEXT NOT NULL,
    UNIQUE(environment_id, name),
    UNIQUE(environment_id, network_id),
    FOREIGN KEY(environment_id) REFERENCES environment_instances(environment_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS environment_endpoints (
    endpoint_id TEXT PRIMARY KEY,
    environment_id TEXT NOT NULL,
    machine_id TEXT NOT NULL,
    network_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    name TEXT NOT NULL CHECK(length(trim(name)) BETWEEN 1 AND 128),
    instance_json TEXT NOT NULL,
    UNIQUE(environment_id, name),
    FOREIGN KEY(environment_id, machine_id)
        REFERENCES machine_instances(environment_id, machine_id) ON DELETE CASCADE,
    FOREIGN KEY(environment_id, network_id)
        REFERENCES environment_networks(environment_id, network_id) ON DELETE CASCADE
);
"#;

/// Canonical ownership table shipped by state-store schema v2.
pub(super) const TOPOLOGY_OWNERSHIP_V2_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS topology_ownership (
    resource_kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    machine_id TEXT,
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    record_json TEXT NOT NULL,
    PRIMARY KEY(resource_kind, resource_id),
    FOREIGN KEY(environment_id) REFERENCES environment_instances(environment_id) ON DELETE CASCADE,
    FOREIGN KEY(environment_id, machine_id)
        REFERENCES machine_instances(environment_id, machine_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_topology_ownership_environment
    ON topology_ownership(environment_id, machine_id);
"#;

/// Normalized Environment fencing projections added after the canonical v2
/// Environment table has been created or validated.
pub(super) const TOPOLOGY_ENVIRONMENT_V3_DDL: &str = r#"
ALTER TABLE environment_instances
    ADD COLUMN lifecycle_generation INTEGER NOT NULL DEFAULT 0
        CHECK(lifecycle_generation >= 0);
ALTER TABLE environment_instances
    ADD COLUMN active_operation_id TEXT
        CHECK(active_operation_id IS NULL OR length(trim(active_operation_id)) BETWEEN 1 AND 128);
CREATE UNIQUE INDEX idx_environment_active_operation
    ON environment_instances(active_operation_id)
    WHERE active_operation_id IS NOT NULL;
"#;

/// Canonical ownership table for state-store schema v3.
///
/// Ownership rows are deletion authority. Restricting parent deletion ensures
/// an active Environment or Machine cannot disappear while cleanup evidence is
/// still present.
pub(super) const TOPOLOGY_OWNERSHIP_V3_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS topology_ownership (
    resource_kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    machine_id TEXT,
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    record_json TEXT NOT NULL,
    PRIMARY KEY(resource_kind, resource_id),
    FOREIGN KEY(environment_id) REFERENCES environment_instances(environment_id) ON DELETE RESTRICT,
    FOREIGN KEY(environment_id, machine_id)
        REFERENCES machine_instances(environment_id, machine_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS idx_topology_ownership_environment
    ON topology_ownership(environment_id, machine_id);
"#;

/// Durable lifecycle intent and deletion history introduced in schema v3.
///
/// These records deliberately do not reference the active Environment tables:
/// an operation and its tombstone must survive final deletion of the aggregate.
/// The JSON columns are opaque in this migration slice; later lifecycle code
/// must cross-check them against every normalized projection before acting.
pub(super) const TOPOLOGY_LIFECYCLE_V3_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS environment_lifecycle_operations (
    operation_id TEXT PRIMARY KEY CHECK(length(trim(operation_id)) BETWEEN 1 AND 128),
    idempotency_key TEXT NOT NULL UNIQUE
        CHECK(length(trim(idempotency_key)) BETWEEN 1 AND 256),
    request_id TEXT NOT NULL CHECK(length(trim(request_id)) BETWEEN 1 AND 256),
    project_id TEXT NOT NULL CHECK(length(trim(project_id)) BETWEEN 1 AND 128),
    environment_id TEXT NOT NULL CHECK(length(trim(environment_id)) BETWEEN 1 AND 128),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    generation INTEGER NOT NULL CHECK(generation > 0),
    kind TEXT NOT NULL CHECK(kind IN ('up', 'stop', 'delete')),
    status TEXT NOT NULL
        CHECK(status IN ('planned', 'running', 'blocked', 'succeeded', 'failed', 'superseded')),
    request_hash TEXT NOT NULL CHECK(length(trim(request_hash)) > 0),
    definition_digest TEXT NOT NULL CHECK(length(trim(definition_digest)) > 0),
    initial_state TEXT NOT NULL CHECK(length(trim(initial_state)) > 0),
    requested_target TEXT NOT NULL CHECK(length(trim(requested_target)) > 0),
    operation_json TEXT NOT NULL CHECK(json_valid(operation_json)),
    created_at INTEGER NOT NULL CHECK(created_at >= 0),
    updated_at INTEGER NOT NULL CHECK(updated_at >= created_at),
    completed_at INTEGER,
    UNIQUE(environment_id, generation),
    CHECK(completed_at IS NULL OR completed_at >= updated_at),
    CHECK(
        (status IN ('planned', 'running', 'blocked') AND completed_at IS NULL) OR
        (status IN ('succeeded', 'failed', 'superseded') AND completed_at IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_environment_lifecycle_project
    ON environment_lifecycle_operations(project_id, environment_id, generation);
CREATE INDEX IF NOT EXISTS idx_environment_lifecycle_status
    ON environment_lifecycle_operations(status, updated_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_environment_lifecycle_one_active
    ON environment_lifecycle_operations(environment_id)
    WHERE status IN ('planned', 'running', 'blocked');

CREATE TRIGGER IF NOT EXISTS environment_lifecycle_idempotency_key_immutable
BEFORE UPDATE OF idempotency_key ON environment_lifecycle_operations
WHEN NEW.idempotency_key <> OLD.idempotency_key
BEGIN
    SELECT RAISE(ABORT, 'environment lifecycle idempotency key is immutable');
END;

CREATE TRIGGER IF NOT EXISTS environment_lifecycle_intent_immutable
BEFORE UPDATE OF
    operation_id, idempotency_key, request_id, project_id, environment_id,
    schema_version, generation, kind, request_hash, definition_digest,
    initial_state, requested_target
ON environment_lifecycle_operations
WHEN
    NEW.operation_id <> OLD.operation_id OR
    NEW.idempotency_key <> OLD.idempotency_key OR
    NEW.request_id <> OLD.request_id OR
    NEW.project_id <> OLD.project_id OR
    NEW.environment_id <> OLD.environment_id OR
    NEW.schema_version <> OLD.schema_version OR
    NEW.generation <> OLD.generation OR
    NEW.kind <> OLD.kind OR
    NEW.request_hash <> OLD.request_hash OR
    NEW.definition_digest <> OLD.definition_digest OR
    NEW.initial_state <> OLD.initial_state OR
    NEW.requested_target <> OLD.requested_target
BEGIN
    SELECT RAISE(ABORT, 'environment lifecycle intent projections are immutable');
END;

CREATE TABLE IF NOT EXISTS environment_tombstones (
    environment_id TEXT PRIMARY KEY
        CHECK(length(trim(environment_id)) BETWEEN 1 AND 128),
    project_id TEXT NOT NULL CHECK(length(trim(project_id)) BETWEEN 1 AND 128),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    name TEXT NOT NULL CHECK(length(trim(name)) BETWEEN 1 AND 128),
    definition_digest TEXT NOT NULL CHECK(length(trim(definition_digest)) > 0),
    delete_operation_id TEXT NOT NULL UNIQUE
        CHECK(length(trim(delete_operation_id)) BETWEEN 1 AND 128),
    lifecycle_generation INTEGER NOT NULL CHECK(lifecycle_generation > 0),
    ownership_digest TEXT NOT NULL CHECK(length(trim(ownership_digest)) > 0),
    deleted_at INTEGER NOT NULL CHECK(deleted_at >= 0),
    tombstone_json TEXT NOT NULL CHECK(json_valid(tombstone_json))
);
CREATE INDEX IF NOT EXISTS idx_environment_tombstone_project
    ON environment_tombstones(project_id, deleted_at, environment_id);
"#;

const TOPOLOGY_OWNERSHIP_V2_TO_V3_DDL: &str = r#"
DROP INDEX idx_topology_ownership_environment;
ALTER TABLE topology_ownership RENAME TO topology_ownership_v2;

CREATE TABLE topology_ownership (
    resource_kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    machine_id TEXT,
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    record_json TEXT NOT NULL,
    PRIMARY KEY(resource_kind, resource_id),
    FOREIGN KEY(environment_id) REFERENCES environment_instances(environment_id) ON DELETE RESTRICT,
    FOREIGN KEY(environment_id, machine_id)
        REFERENCES machine_instances(environment_id, machine_id) ON DELETE RESTRICT
);

INSERT INTO topology_ownership
    (resource_kind, resource_id, environment_id, machine_id, schema_version, record_json)
SELECT resource_kind, resource_id, environment_id, machine_id, schema_version, record_json
FROM topology_ownership_v2;

DROP TABLE topology_ownership_v2;
CREATE INDEX idx_topology_ownership_environment
    ON topology_ownership(environment_id, machine_id);
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyMigrationStage {
    TopologySchemaCreated,
    ProjectWritten(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopologyV3MigrationStage {
    OwnershipRebuilt,
    LifecycleSchemaCreated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StackJournalV4MigrationStage {
    JournalSchemaCreated,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LegacyMigrationFailpoint {
    AfterFirstProjectWrite,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TopologyV3MigrationFailpoint {
    AfterOwnershipRebuild,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StackJournalV4MigrationFailpoint {
    AfterJournalSchemaCreated,
}

fn normalized_schema_sql(sql: Option<String>) -> Option<String> {
    sql.map(|sql| sql.split_whitespace().collect::<Vec<_>>().join(" "))
}

type SchemaObjectKey = (String, String);
type SchemaObjectDefinition = (String, Option<String>);
type StateSchemaShape = BTreeMap<SchemaObjectKey, SchemaObjectDefinition>;

fn state_schema_shape(connection: &Connection) -> Result<StateSchemaShape, StackError> {
    let mut statement = connection.prepare(
        "SELECT type, name, tbl_name, sql
         FROM sqlite_master
         WHERE name NOT LIKE 'sqlite_%'
         ORDER BY type, name",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;

    let mut shape = BTreeMap::new();
    for row in rows {
        let (object_type, name, table_name, sql) = row?;
        shape.insert(
            (object_type, name),
            (table_name, normalized_schema_sql(sql)),
        );
    }
    Ok(shape)
}

fn schema_shape_mismatch(
    version: u32,
    expected: &StateSchemaShape,
    actual: &StateSchemaShape,
) -> StackError {
    let missing = expected
        .keys()
        .filter(|key| !actual.contains_key(*key))
        .map(|(kind, name)| format!("{kind}:{name}"))
        .collect::<Vec<_>>();
    let unexpected = actual
        .keys()
        .filter(|key| !expected.contains_key(*key))
        .map(|(kind, name)| format!("{kind}:{name}"))
        .collect::<Vec<_>>();
    let mismatched = expected
        .iter()
        .filter_map(|(key, definition)| {
            actual
                .get(key)
                .filter(|actual_definition| *actual_definition != definition)
                .map(|_| format!("{}:{}", key.0, key.1))
        })
        .collect::<Vec<_>>();
    StackError::InvalidSpec(format!(
        "state schema v{version} shape mismatch: \
         missing={missing:?}, unexpected={unexpected:?}, mismatched={mismatched:?}"
    ))
}

fn persisted_projection_mismatch(table: &str, key: &str, field: &str) -> StackError {
    StackError::InvalidSpec(format!(
        "persisted topology projection mismatch: table={table}, key={key}, field={field}"
    ))
}

fn parse_persisted_json<T: serde::de::DeserializeOwned>(
    table: &str,
    key: &str,
    field: &str,
    json: &str,
) -> Result<T, StackError> {
    serde_json::from_str(json).map_err(|error| {
        StackError::InvalidSpec(format!(
            "persisted topology projection mismatch: table={table}, key={key}, field={field}: {error}"
        ))
    })
}

fn require_projection(
    matches: bool,
    table: &str,
    key: &str,
    field: &str,
) -> Result<(), StackError> {
    if matches {
        Ok(())
    } else {
        Err(persisted_projection_mismatch(table, key, field))
    }
}

fn require_u64_projection(
    sql_value: i64,
    json_value: u64,
    table: &str,
    key: &str,
    field: &str,
) -> Result<(), StackError> {
    require_projection(
        u64::try_from(sql_value).ok() == Some(json_value),
        table,
        key,
        field,
    )
}

fn serialized_string_projection<T: serde::Serialize>(value: &T) -> Result<String, StackError> {
    serde_json::to_value(value)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| {
            StackError::InvalidSpec("expected string-valued serialized projection".to_string())
        })
}

fn sqlite_u64(value: u64, field: &str) -> Result<i64, StackError> {
    i64::try_from(value).map_err(|_| {
        StackError::InvalidSpec(format!(
            "lifecycle {field} value {value} exceeds SQLite INTEGER range"
        ))
    })
}

fn environment_not_found(environment_id: &str) -> StackError {
    TopologyResolutionError::NotFound {
        kind: "environment".to_string(),
        selector: environment_id.to_string(),
    }
    .into()
}

fn operation_not_found(operation_id: &str) -> StackError {
    StackError::Machine {
        code: vz_runtime_contract::MachineErrorCode::NotFound,
        message: format!("lifecycle operation `{operation_id}` not found"),
    }
}

fn lifecycle_ownership_digest(
    ownership: impl IntoIterator<Item = OwnershipRecord>,
) -> Result<String, StackError> {
    fn kind_identity(kind: &OwnedResourceKind) -> String {
        match kind {
            OwnedResourceKind::Machine => "machine".to_string(),
            OwnedResourceKind::Incarnation => "incarnation".to_string(),
            OwnedResourceKind::Disk => "disk".to_string(),
            OwnedResourceKind::Socket => "socket".to_string(),
            OwnedResourceKind::DockerContext => "docker_context".to_string(),
            OwnedResourceKind::Network => "network".to_string(),
            OwnedResourceKind::Endpoint => "endpoint".to_string(),
            OwnedResourceKind::Credential => "credential".to_string(),
            OwnedResourceKind::Fault => "fault".to_string(),
            OwnedResourceKind::LegacySandbox => "legacy_sandbox".to_string(),
            OwnedResourceKind::Other(value) => format!("other:{value}"),
        }
    }

    let mut ownership = ownership.into_iter().collect::<Vec<_>>();
    ownership.sort_by_key(|record| {
        (
            kind_identity(&record.resource_kind),
            record.resource_id.clone(),
            record
                .machine_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
        )
    });
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&ownership)?)
    ))
}

fn semantic_collections_match<T: PartialEq>(
    expected: &[T],
    actual: &[T],
    identity: impl Fn(&T) -> String,
) -> bool {
    let mut expected = expected
        .iter()
        .map(|value| (identity(value), value))
        .collect::<Vec<_>>();
    let mut actual = actual
        .iter()
        .map(|value| (identity(value), value))
        .collect::<Vec<_>>();
    expected.sort_by(|left, right| left.0.cmp(&right.0));
    actual.sort_by(|left, right| left.0.cmp(&right.0));
    expected == actual
}

impl StateStore {
    pub(super) fn create_topology_schema_v2(&self) -> Result<(), StackError> {
        self.conn.execute_batch(TOPOLOGY_SCHEMA_COMMON_DDL)?;
        self.conn.execute_batch(TOPOLOGY_OWNERSHIP_V2_DDL)?;
        Ok(())
    }

    pub(super) fn create_topology_schema_v3(&self) -> Result<(), StackError> {
        self.conn.execute_batch(TOPOLOGY_SCHEMA_COMMON_DDL)?;
        self.conn.execute_batch(TOPOLOGY_ENVIRONMENT_V3_DDL)?;
        self.conn.execute_batch(TOPOLOGY_OWNERSHIP_V3_DDL)?;
        self.conn.execute_batch(TOPOLOGY_LIFECYCLE_V3_DDL)?;
        Ok(())
    }

    pub(super) fn validate_legacy_v1_schema(&self) -> Result<(), StackError> {
        let reference = Connection::open_in_memory()?;
        // Production validation is anchored to the immutable SQL copied from the
        // official v0.3.20 tag, not today's mutable legacy-table constructor.
        // The fixture source checksum is independently frozen by StateStore tests.
        reference.execute_batch(include_str!("../../tests/fixtures/v0.3.20-state.sql"))?;

        let expected = state_schema_shape(&reference)?;
        let actual = state_schema_shape(&self.conn)?;
        if actual != expected {
            return Err(schema_shape_mismatch(1, &expected, &actual));
        }
        Ok(())
    }

    pub(super) fn validate_v2_schema(&self) -> Result<(), StackError> {
        // Build the complete canonical store in a private reference database, then
        // compare every application-owned schema object. This validates legacy
        // execution/build/checkpoint state alongside the topology tables and rejects
        // missing, modified, and injected objects before recovery can mutate state.
        let reference = StateStore {
            conn: Connection::open_in_memory()?,
            event_sender: None,
        };
        reference.create_legacy_schema()?;
        reference.create_topology_schema_v2()?;

        self.validate_schema_against(2, &reference.conn)
    }

    pub(super) fn validate_v3_schema(&self) -> Result<(), StackError> {
        let reference = StateStore {
            conn: Connection::open_in_memory()?,
            event_sender: None,
        };
        reference.create_legacy_schema()?;
        reference.create_topology_schema_v3()?;

        self.validate_schema_against(3, &reference.conn)
    }

    pub(super) fn validate_v4_schema(&self) -> Result<(), StackError> {
        let reference = StateStore {
            conn: Connection::open_in_memory()?,
            event_sender: None,
        };
        reference.create_legacy_schema()?;
        reference.create_topology_schema_v3()?;
        reference.create_stack_journal_schema_v4()?;

        self.validate_schema_against(4, &reference.conn)
    }

    fn validate_schema_against(
        &self,
        version: u32,
        reference: &Connection,
    ) -> Result<(), StackError> {
        let expected = state_schema_shape(reference)?;
        let actual = state_schema_shape(&self.conn)?;
        if actual != expected {
            return Err(schema_shape_mismatch(version, &expected, &actual));
        }

        // Schema declarations are not enough when a database was externally
        // modified with foreign-key enforcement disabled. Reject dangling topology
        // ownership/child records before the daemon can perform recovery writes.
        let violation = self
            .conn
            .query_row("PRAGMA foreign_key_check", [], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .optional()?;
        if let Some((table, row_id, parent, foreign_key)) = violation {
            return Err(StackError::InvalidSpec(format!(
                "state schema v{version} contains a foreign-key violation: \
                 table={table}, row_id={row_id:?}, parent={parent}, foreign_key={foreign_key}"
            )));
        }
        Ok(())
    }

    /// Bootstrap one complete Project aggregate atomically.
    ///
    /// Existing projects must be changed through the narrow locked mutation APIs.
    /// Rejecting replacement here prevents a caller holding a stale aggregate from
    /// erasing a concurrently-created Environment or reserved owned resource.
    pub fn save_project_state(&self, state: &ProjectState) -> Result<(), StackError> {
        state
            .validate()
            .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
        self.with_immediate_transaction(|store| {
            if store
                .load_project_state(state.definition.project_id.as_str())?
                .is_some()
            {
                return Err(StackError::Machine {
                    code: vz_runtime_contract::MachineErrorCode::StateConflict,
                    message: format!(
                        "project `{}` already exists; use a locked topology mutation API",
                        state.definition.project_id
                    ),
                });
            }
            store.save_project_state_in_transaction(state)
        })
    }

    pub(super) fn save_project_state_in_transaction(
        &self,
        state: &ProjectState,
    ) -> Result<(), StackError> {
        let project_id = state.definition.project_id.as_str();
        let definition_json = serde_json::to_string(&state.definition)?;
        let created_at = state
            .environments
            .iter()
            .map(|environment| environment.created_at)
            .min()
            .unwrap_or(0);
        let updated_at = state
            .environments
            .iter()
            .map(|environment| environment.updated_at)
            .max()
            .unwrap_or(created_at);
        self.conn.execute(
            "INSERT INTO project_definitions
                (project_id, schema_version, name, definition_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(project_id) DO UPDATE SET
                schema_version = excluded.schema_version,
                name = excluded.name,
                definition_json = excluded.definition_json,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            params![
                project_id,
                state.definition.schema_version,
                state.definition.name,
                definition_json,
                created_at as i64,
                updated_at as i64,
            ],
        )?;

        self.delete_project_environments(project_id)?;
        for environment in &state.environments {
            self.insert_environment(environment)?;
        }
        Ok(())
    }

    fn delete_project_environments(&self, project_id: &str) -> Result<(), StackError> {
        for table in [
            "topology_ownership",
            "environment_endpoints",
            "environment_networks",
            "machine_instances",
            "workspace_bindings",
        ] {
            self.conn.execute(
                &format!(
                    "DELETE FROM {table} WHERE environment_id IN
                     (SELECT environment_id FROM environment_instances WHERE project_id = ?1)"
                ),
                params![project_id],
            )?;
        }
        self.conn.execute(
            "DELETE FROM environment_instances WHERE project_id = ?1",
            params![project_id],
        )?;
        Ok(())
    }

    fn insert_environment(&self, environment: &EnvironmentInstance) -> Result<(), StackError> {
        let legacy_sandbox_id = environment
            .legacy_migration
            .as_ref()
            .map(|provenance| provenance.legacy_sandbox_id.as_str());
        let has_lifecycle_projection: bool = self.conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM pragma_table_info('environment_instances')
                WHERE name = 'lifecycle_generation'
             )",
            [],
            |row| row.get(0),
        )?;
        if has_lifecycle_projection {
            self.conn.execute(
                "INSERT INTO environment_instances
                    (environment_id, project_id, schema_version, name, definition_digest, state,
                     instance_json, created_at, updated_at, legacy_sandbox_id,
                     lifecycle_generation, active_operation_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    environment.environment_id.as_str(),
                    environment.project_id.as_str(),
                    environment.schema_version,
                    environment.name,
                    environment.definition_digest,
                    serde_json::to_string(&environment.state)?,
                    serde_json::to_string(environment)?,
                    environment.created_at as i64,
                    environment.updated_at as i64,
                    legacy_sandbox_id,
                    environment.lifecycle_generation as i64,
                    environment
                        .active_operation_id
                        .as_ref()
                        .map(|id| id.as_str()),
                ],
            )?;
        } else {
            self.conn.execute(
                "INSERT INTO environment_instances
                    (environment_id, project_id, schema_version, name, definition_digest, state,
                     instance_json, created_at, updated_at, legacy_sandbox_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    environment.environment_id.as_str(),
                    environment.project_id.as_str(),
                    environment.schema_version,
                    environment.name,
                    environment.definition_digest,
                    serde_json::to_string(&environment.state)?,
                    serde_json::to_string(environment)?,
                    environment.created_at as i64,
                    environment.updated_at as i64,
                    legacy_sandbox_id,
                ],
            )?;
        }

        for binding in &environment.bindings {
            self.insert_workspace_binding(binding)?;
        }
        for machine in &environment.machines {
            self.conn.execute(
                "INSERT INTO machine_instances
                    (machine_id, environment_id, schema_version, name, state, instance_json,
                     legacy_sandbox_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    machine.machine_id.as_str(),
                    machine.environment_id.as_str(),
                    machine.schema_version,
                    machine.name,
                    serde_json::to_string(&machine.state)?,
                    serde_json::to_string(machine)?,
                    machine.legacy_sandbox_id,
                ],
            )?;
        }
        for network in &environment.networks {
            self.conn.execute(
                "INSERT INTO environment_networks
                    (network_id, environment_id, schema_version, name, instance_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    network.network_id.as_str(),
                    network.environment_id.as_str(),
                    network.schema_version,
                    network.name,
                    serde_json::to_string(network)?,
                ],
            )?;
        }
        for endpoint in &environment.endpoints {
            self.conn.execute(
                "INSERT INTO environment_endpoints
                    (endpoint_id, environment_id, machine_id, network_id, schema_version, name,
                     instance_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    endpoint.endpoint_id.as_str(),
                    endpoint.environment_id.as_str(),
                    endpoint.machine_id.as_str(),
                    endpoint.network_id.as_str(),
                    endpoint.schema_version,
                    endpoint.name,
                    serde_json::to_string(endpoint)?,
                ],
            )?;
        }
        for record in &environment.ownership {
            self.conn.execute(
                "INSERT INTO topology_ownership
                    (resource_kind, resource_id, environment_id, machine_id, schema_version,
                     record_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    serde_json::to_string(&record.resource_kind)?,
                    record.resource_id,
                    record.environment_id.as_str(),
                    record.machine_id.as_ref().map(|id| id.as_str()),
                    record.schema_version,
                    serde_json::to_string(record)?,
                ],
            )?;
        }
        Ok(())
    }

    fn insert_workspace_binding(&self, binding: &WorkspaceBinding) -> Result<(), StackError> {
        self.conn.execute(
            "INSERT INTO workspace_bindings
                (binding_id, project_id, environment_id, schema_version, name,
                 workspace_key, path_hint, binding_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                binding.binding_id.as_str(),
                binding.project_id.as_str(),
                binding.environment_id.as_str(),
                binding.schema_version,
                binding.name,
                binding.workspace_key,
                binding.path_hint,
                serde_json::to_string(binding)?,
            ],
        )?;
        Ok(())
    }

    fn update_workspace_binding_cas(
        &self,
        before: &WorkspaceBinding,
        after: &WorkspaceBinding,
    ) -> Result<(), StackError> {
        let affected = self.conn.execute(
            "UPDATE workspace_bindings
             SET workspace_key = ?1, path_hint = ?2, binding_json = ?3
             WHERE binding_id = ?4 AND project_id = ?5 AND environment_id = ?6
               AND name = ?7 AND workspace_key = ?8 AND path_hint IS ?9
               AND binding_json = ?10",
            params![
                after.workspace_key,
                after.path_hint,
                serde_json::to_string(after)?,
                before.binding_id.as_str(),
                before.project_id.as_str(),
                before.environment_id.as_str(),
                before.name,
                before.workspace_key,
                before.path_hint,
                serde_json::to_string(before)?,
            ],
        )?;
        if affected == 1 {
            Ok(())
        } else {
            Err(StackError::Machine {
                code: vz_runtime_contract::MachineErrorCode::StateConflict,
                message: format!(
                    "workspace binding `{}` changed during compare-and-swap",
                    before.binding_id
                ),
            })
        }
    }

    /// Load one complete Project aggregate by stable identity.
    pub fn load_project_state(&self, project_id: &str) -> Result<Option<ProjectState>, StackError> {
        let row: Option<(String, i64, String, String, i64, i64)> = self
            .conn
            .query_row(
                "SELECT project_id, schema_version, name, definition_json, created_at, updated_at
                 FROM project_definitions WHERE project_id = ?1",
                params![project_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((sql_project_id, schema_version, name, definition_json, created_at, updated_at)) =
            row
        else {
            return Ok(None);
        };
        let table = "project_definitions";
        let definition: ProjectDefinition =
            parse_persisted_json(table, &sql_project_id, "definition_json", &definition_json)?;
        require_projection(
            sql_project_id == definition.project_id.as_str(),
            table,
            &sql_project_id,
            "project_id",
        )?;
        require_projection(
            schema_version == i64::from(definition.schema_version),
            table,
            &sql_project_id,
            "schema_version",
        )?;
        require_projection(name == definition.name, table, &sql_project_id, "name")?;

        let environments = self.load_environments_for_project(project_id)?;
        let derived_created_at = environments
            .iter()
            .map(|environment| environment.created_at)
            .min()
            .unwrap_or(0);
        let derived_updated_at = environments
            .iter()
            .map(|environment| environment.updated_at)
            .max()
            .unwrap_or(derived_created_at);
        require_u64_projection(
            created_at,
            derived_created_at,
            table,
            &sql_project_id,
            "created_at",
        )?;
        require_u64_projection(
            updated_at,
            derived_updated_at,
            table,
            &sql_project_id,
            "updated_at",
        )?;

        let state = ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments,
        };
        state
            .validate()
            .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
        Ok(Some(state))
    }

    /// List all complete Project aggregates in stable ID order.
    pub fn list_project_states(&self) -> Result<Vec<ProjectState>, StackError> {
        let mut stmt = self
            .conn
            .prepare("SELECT project_id FROM project_definitions ORDER BY project_id")?;
        let project_ids = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        project_ids
            .iter()
            .map(|project_id| {
                self.load_project_state(project_id)?.ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "project `{project_id}` disappeared while listing persisted topology"
                    ))
                })
            })
            .collect()
    }

    /// Resolve one existing Environment in a project without mutating state.
    pub fn resolve_environment(
        &self,
        project_id: &str,
        context: &EnvironmentSelectionContext,
    ) -> Result<EnvironmentSelection, StackError> {
        let project = self.load_project_state(project_id)?.ok_or_else(|| {
            TopologyResolutionError::NotFound {
                kind: "project".to_string(),
                selector: project_id.to_string(),
            }
        })?;
        project.resolve_environment(context).map_err(Into::into)
    }

    /// Atomically select an existing Environment or reserve a fresh immutable instance.
    ///
    /// The project is re-read only after `BEGIN IMMEDIATE`, so concurrent creators of
    /// one name converge and creators of different names retain both sibling instances.
    pub fn resolve_or_reserve_environment_for_up(
        &self,
        definition: &ProjectDefinition,
        context: &EnvironmentSelectionContext,
        now: u64,
    ) -> Result<EnvironmentUpReservation, StackError> {
        definition
            .validate()
            .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
        self.with_immediate_transaction(|store| {
            let (mut project, project_exists) = match store
                .load_project_state(definition.project_id.as_str())?
            {
                Some(project) => {
                    if project.definition != *definition {
                        return Err(StackError::InvalidSpec(format!(
                            "project definition drift for `{}`; persisted digest={}, requested digest={}",
                            definition.project_id,
                            project.definition.digest().map_err(|error| StackError::InvalidSpec(error.to_string()))?,
                            definition.digest().map_err(|error| StackError::InvalidSpec(error.to_string()))?,
                        )));
                    }
                    (project, true)
                }
                None => (
                    ProjectState {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        definition: definition.clone(),
                        environments: Vec::new(),
                    },
                    false,
                ),
            };

            match project.resolve_environment_for_up(context)? {
                EnvironmentUpDecision::Existing { selection } => {
                    let environment = project
                        .environments
                        .iter()
                        .find(|environment| environment.environment_id == selection.environment_id)
                        .cloned()
                        .ok_or_else(|| {
                            StackError::InvalidSpec(format!(
                                "selected Environment `{}` disappeared from its locked project aggregate",
                                selection.environment_id
                            ))
                        })?;
                    Ok(EnvironmentUpReservation::Existing {
                        selection,
                        environment,
                    })
                }
                EnvironmentUpDecision::Create { name } => {
                    let environment = definition
                        .instantiate_environment(name, now)
                        .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
                    project.environments.push(environment.clone());
                    project
                        .validate()
                        .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
                    if !project_exists {
                        store.conn.execute(
                            "INSERT INTO project_definitions
                                (project_id, schema_version, name, definition_json, created_at,
                                 updated_at)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
                            params![
                                definition.project_id.as_str(),
                                definition.schema_version,
                                definition.name,
                                serde_json::to_string(definition)?,
                                sqlite_u64(now, "Environment created_at")?,
                            ],
                        )?;
                    }
                    store.insert_environment(&environment)?;
                    store.refresh_project_timestamps(definition.project_id.as_str())?;
                    Ok(EnvironmentUpReservation::Created { environment })
                }
            }
        })
    }

    /// Reserve a declared workspace slot while an Environment is still Creating.
    ///
    /// This is the pre-reconciliation half of workspace setup: it validates the
    /// exact Project/Environment owner and declared symbolic slot, then persists
    /// the binding while holding an immediate transaction. Repeating the exact
    /// reservation is idempotent and does not advance the Environment timestamp.
    pub fn reserve_workspace_binding_for_environment(
        &self,
        requested: &WorkspaceBinding,
        now: u64,
    ) -> Result<WorkspaceBinding, StackError> {
        self.with_immediate_transaction(|store| {
            let mut project = store
                .load_project_state(requested.project_id.as_str())?
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "project `{}` not found while reserving workspace binding",
                        requested.project_id
                    ))
                })?;
            let slot_is_declared = project.definition.environment.machines.iter().any(|machine| {
                machine
                    .workspace
                    .as_ref()
                    .is_some_and(|workspace| workspace.binding == requested.name)
            });
            if !slot_is_declared {
                return Err(StackError::InvalidSpec(format!(
                    "workspace binding slot `{}` is not declared by project `{}`",
                    requested.name, requested.project_id
                )));
            }
            let environment = project
                .environments
                .iter_mut()
                .find(|environment| environment.environment_id == requested.environment_id)
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "Environment `{}` is not owned by project `{}`",
                        requested.environment_id, requested.project_id
                    ))
                })?;
            if environment.state != EnvironmentState::Creating {
                return Err(StackError::InvalidSpec(format!(
                    "Environment `{}` must be creating while reserving its workspace binding",
                    requested.environment_id
                )));
            }
            if let Some(existing) = environment.bindings.iter().find(|binding| {
                binding.name == requested.name || binding.workspace_key == requested.workspace_key
            }) {
                if existing == requested {
                    return Ok(existing.clone());
                }
                return Err(StackError::Machine {
                    code: vz_runtime_contract::MachineErrorCode::StateConflict,
                    message: format!(
                        "workspace binding slot `{}` or key `{}` is already reserved in Environment `{}`",
                        requested.name, requested.workspace_key, requested.environment_id
                    ),
                });
            }

            let before = environment.clone();
            environment.bindings.push(requested.clone());
            environment.updated_at = environment.updated_at.max(now);
            let after = environment.clone();
            project
                .validate()
                .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
            store.insert_workspace_binding(requested)?;
            store.update_environment_parent_cas(&before, &after)?;
            Ok(requested.clone())
        })
    }

    /// Refresh one workspace slot only after its exact owning Environment is Ready.
    ///
    /// `path_hint` is diagnostic: it may change without changing the binding identity.
    /// Existing Machine/resource identities are preserved by re-reading and writing the
    /// latest aggregate while holding an immediate transaction.
    pub fn refresh_workspace_binding(
        &self,
        requested: &WorkspaceBinding,
        now: u64,
    ) -> Result<WorkspaceBinding, StackError> {
        self.with_immediate_transaction(|store| {
            let mut project = store
                .load_project_state(requested.project_id.as_str())?
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "project `{}` not found while refreshing workspace binding",
                        requested.project_id
                    ))
                })?;
            let environment = project
                .environments
                .iter_mut()
                .find(|environment| environment.environment_id == requested.environment_id)
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "Environment `{}` is not owned by project `{}`",
                        requested.environment_id, requested.project_id
                    ))
                })?;
            if environment.state != EnvironmentState::Ready {
                return Err(StackError::InvalidSpec(format!(
                    "Environment `{}` must be ready before refreshing a workspace binding",
                    requested.environment_id
                )));
            }

            let before = environment.clone();
            let (refreshed, previous) = if let Some(existing) = environment
                .bindings
                .iter_mut()
                .find(|binding| binding.name == requested.name)
            {
                // The symbolic slot owns the immutable binding identity. A successful
                // reconcile may move that slot to a new opaque workspace key and may
                // refresh its diagnostic path without replacing any other resource.
                let previous = existing.clone();
                existing.workspace_key = requested.workspace_key.clone();
                existing.path_hint = requested.path_hint.clone();
                (existing.clone(), Some(previous))
            } else {
                if let Some(existing) = environment
                    .bindings
                    .iter()
                    .find(|binding| binding.workspace_key == requested.workspace_key)
                {
                    return Err(StackError::InvalidSpec(format!(
                        "workspace key `{}` is already bound to slot `{}` in Environment `{}`",
                        requested.workspace_key, existing.name, requested.environment_id
                    )));
                }
                environment.bindings.push(requested.clone());
                (requested.clone(), None)
            };
            environment.updated_at = environment.updated_at.max(now);
            let after = environment.clone();
            project
                .validate()
                .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
            if let Some(previous) = previous {
                store.update_workspace_binding_cas(&previous, &refreshed)?;
            } else {
                store.insert_workspace_binding(&refreshed)?;
            }
            store.update_environment_parent_cas(&before, &after)?;
            Ok(refreshed)
        })
    }

    /// Atomically reserve a physical/runtime resource for its exact topology owner.
    ///
    /// Repeating the exact reservation is idempotent. A reservation held by any
    /// different Environment or Machine fails before the aggregate is changed.
    pub fn reserve_owned_resource(
        &self,
        requested: &OwnershipRecord,
        now: u64,
    ) -> Result<OwnershipRecord, StackError> {
        self.with_immediate_transaction(|store| {
            let encoded_kind = serde_json::to_string(&requested.resource_kind)?;
            let existing: Option<(String, String, String, Option<String>, i64, String)> = store
                .conn
                .query_row(
                    "SELECT resource_kind, resource_id, environment_id, machine_id,
                            schema_version, record_json
                     FROM topology_ownership
                     WHERE resource_kind = ?1 AND resource_id = ?2",
                    params![encoded_kind, requested.resource_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    },
                )
                .optional()?;
            if let Some((
                resource_kind,
                resource_id,
                sql_environment_id,
                machine_id,
                schema_version,
                json,
            )) = existing
            {
                let table = "topology_ownership";
                let key = format!("{resource_kind}:{resource_id}");
                let existing: OwnershipRecord =
                    parse_persisted_json(table, &key, "record_json", &json)?;
                require_projection(
                    resource_kind == serde_json::to_string(&existing.resource_kind)?,
                    table,
                    &key,
                    "resource_kind",
                )?;
                require_projection(
                    resource_id == existing.resource_id,
                    table,
                    &key,
                    "resource_id",
                )?;
                require_projection(
                    sql_environment_id == existing.environment_id.as_str(),
                    table,
                    &key,
                    "environment_id",
                )?;
                require_projection(
                    machine_id.as_deref() == existing.machine_id.as_ref().map(|id| id.as_str()),
                    table,
                    &key,
                    "machine_id",
                )?;
                require_projection(
                    schema_version == i64::from(existing.schema_version),
                    table,
                    &key,
                    "schema_version",
                )?;
                if existing == *requested {
                    let environment = store
                        .load_environment_instance(requested.environment_id.as_str())?
                        .ok_or_else(|| {
                            StackError::InvalidSpec(format!(
                                "Environment `{}` not found while reserving resource `{}`",
                                requested.environment_id, requested.resource_id
                            ))
                        })?;
                    store.ensure_resource_reservation_allowed(&environment)?;
                    return Ok(existing);
                }
                return Err(StackError::OwnedResourceCollision(Box::new(
                    OwnedResourceCollisionError {
                        resource_kind: encoded_kind,
                        resource_id: requested.resource_id.clone(),
                        existing_environment_id: existing.environment_id.to_string(),
                        existing_machine_id: existing.machine_id.map(|id| id.to_string()),
                    },
                )));
            }

            let mut environment = store
                .load_environment_instance(requested.environment_id.as_str())?
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "Environment `{}` not found while reserving resource `{}`",
                        requested.environment_id, requested.resource_id
                    ))
                })?;
            store.ensure_resource_reservation_allowed(&environment)?;
            let before = environment.clone();
            environment.ownership.push(requested.clone());
            environment.updated_at = environment.updated_at.max(now);
            environment
                .validate()
                .map_err(|error| StackError::InvalidSpec(error.to_string()))?;

            store.conn.execute(
                "INSERT INTO topology_ownership
                    (resource_kind, resource_id, environment_id, machine_id, schema_version,
                     record_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    encoded_kind,
                    requested.resource_id,
                    requested.environment_id.as_str(),
                    requested.machine_id.as_ref().map(|id| id.as_str()),
                    requested.schema_version,
                    serde_json::to_string(requested)?,
                ],
            )?;
            store.update_environment_parent_cas(&before, &environment)?;
            Ok(requested.clone())
        })
    }

    /// Persist and begin one generation-fenced Environment lifecycle operation.
    ///
    /// The idempotency key is global and immutable. Replaying the exact request
    /// returns the stored journal even after the Environment has been deleted.
    pub fn begin_environment_lifecycle(
        &self,
        environment_id: &str,
        kind: EnvironmentLifecycleKind,
        request_id: &str,
        idempotency_key: &str,
        request_hash: &str,
        now: u64,
    ) -> Result<EnvironmentLifecycleOperation, StackError> {
        self.with_immediate_transaction(|store| {
            if let Some(existing) =
                store.load_environment_lifecycle_by_idempotency_key(idempotency_key)?
            {
                store.require_exact_lifecycle_replay(
                    &existing,
                    environment_id,
                    kind,
                    request_id,
                    request_hash,
                )?;
                return Ok(existing);
            }

            let mut environment = store
                .load_environment_instance(environment_id)?
                .ok_or_else(|| environment_not_found(environment_id))?;
            let before = environment.clone();

            if let Some(active_operation_id) = environment.active_operation_id.clone() {
                let mut active = store
                    .load_environment_lifecycle(active_operation_id.as_str())?
                    .ok_or_else(|| {
                        StackError::InvalidSpec(format!(
                            "Environment `{environment_id}` references missing lifecycle operation `{active_operation_id}`"
                        ))
                    })?;
                if kind != EnvironmentLifecycleKind::Delete
                    || active.kind == EnvironmentLifecycleKind::Delete
                {
                    return Err(TopologyLifecycleError::OperationConflict {
                        environment_id: environment_id.to_string(),
                        active_operation_id: active_operation_id.to_string(),
                    }
                    .into());
                }
                let active_before = active.clone();
                active.supersede_for_delete(&mut environment, now)?;
                store.update_environment_lifecycle_cas(&active_before, &active)?;
            }

            let mut operation = EnvironmentLifecycleOperation::plan(
                &environment,
                LifecycleOperationId::generate(),
                kind,
                request_id,
                idempotency_key,
                request_hash,
                now,
            )?;
            store.insert_environment_lifecycle(&operation)?;
            let planned_operation = operation.clone();
            operation.begin(&mut environment, now)?;

            if kind != EnvironmentLifecycleKind::Delete {
                for step in operation.machine_steps.clone() {
                    if step.target_state == Some(step.initial_state) {
                        operation.apply_machine_step_acknowledgement(
                            &mut environment,
                            &MachineLifecycleStepAcknowledgement {
                                operation_id: operation.operation_id.clone(),
                                generation: operation.generation,
                                machine_id: step.machine_id,
                                initial_state: step.initial_state,
                                target_state: step.target_state,
                                expected_incarnation: step.expected_incarnation.clone(),
                                resulting_incarnation: if kind == EnvironmentLifecycleKind::Up {
                                    step.expected_incarnation
                                } else {
                                    None
                                },
                                result: LifecycleStepResult::Succeeded,
                            },
                            now,
                        )?;
                    }
                }
                if operation
                    .machine_steps
                    .iter()
                    .all(|step| step.status == LifecycleStepStatus::Succeeded)
                {
                    operation.finish_live_transition(&mut environment, now)?;
                }
            }

            store.update_environment_parent_cas(&before, &environment)?;
            store.update_environment_lifecycle_cas(&planned_operation, &operation)?;
            Ok(operation)
        })
    }

    /// Load one lifecycle journal by immutable operation ID.
    pub fn load_environment_lifecycle(
        &self,
        operation_id: &str,
    ) -> Result<Option<EnvironmentLifecycleOperation>, StackError> {
        self.load_environment_lifecycle_where("operation_id = ?1", operation_id)
    }

    /// Load the operation currently fencing an Environment, if any.
    pub fn load_current_environment_lifecycle(
        &self,
        environment_id: &str,
    ) -> Result<Option<EnvironmentLifecycleOperation>, StackError> {
        let Some(environment) = self.load_environment_instance(environment_id)? else {
            return Ok(None);
        };
        let Some(operation_id) = environment.active_operation_id.as_ref() else {
            return Ok(None);
        };
        let operation = self
            .load_environment_lifecycle(operation_id.as_str())?
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "Environment `{environment_id}` references missing lifecycle operation `{operation_id}`"
                ))
            })?;
        operation.validate_against_environment(&environment)?;
        Ok(Some(operation))
    }

    /// Alias emphasizing that blocked and partially-applied journals are resumable.
    pub fn load_resumable_environment_lifecycle(
        &self,
        environment_id: &str,
    ) -> Result<Option<EnvironmentLifecycleOperation>, StackError> {
        self.load_current_environment_lifecycle(environment_id)
    }

    /// Apply an exact Machine acknowledgement and persist only that Machine,
    /// its selected Environment parent, and the operation journal.
    pub fn acknowledge_environment_machine_step(
        &self,
        acknowledgement: &MachineLifecycleStepAcknowledgement,
        now: u64,
    ) -> Result<EnvironmentLifecycleOperation, StackError> {
        self.with_immediate_transaction(|store| {
            let mut operation = store
                .load_environment_lifecycle(acknowledgement.operation_id.as_str())?
                .ok_or_else(|| operation_not_found(acknowledgement.operation_id.as_str()))?;
            if store.machine_ack_is_terminal_replay(&operation, acknowledgement)? {
                return Ok(operation);
            }
            let mut environment = store
                .load_environment_instance(operation.environment_id.as_str())?
                .ok_or_else(|| environment_not_found(operation.environment_id.as_str()))?;
            operation.validate_against_environment(&environment)?;
            let environment_before = environment.clone();
            let operation_before = operation.clone();
            let machine_before = environment
                .machines
                .iter()
                .find(|machine| machine.machine_id == acknowledgement.machine_id)
                .cloned();

            operation.apply_machine_step_acknowledgement(&mut environment, acknowledgement, now)?;
            if operation == operation_before && environment == environment_before {
                return Ok(operation);
            }
            let machine_after = environment
                .machines
                .iter()
                .find(|machine| machine.machine_id == acknowledgement.machine_id)
                .cloned();
            store.update_machine_incarnation_ownership_cas(
                &environment_before,
                &environment,
                &acknowledgement.machine_id,
            )?;
            if let (Some(before), Some(after)) = (machine_before, machine_after)
                && before != after
            {
                store.update_machine_cas(&before, &after)?;
            }
            store.update_environment_parent_cas(&environment_before, &environment)?;
            store.update_environment_lifecycle_cas(&operation_before, &operation)?;
            Ok(operation)
        })
    }

    /// Apply an exact ownership-cleanup acknowledgement without deleting its row.
    pub fn acknowledge_environment_cleanup_step(
        &self,
        acknowledgement: &OwnershipCleanupStepAcknowledgement,
        now: u64,
    ) -> Result<EnvironmentLifecycleOperation, StackError> {
        self.with_immediate_transaction(|store| {
            let mut operation = store
                .load_environment_lifecycle(acknowledgement.operation_id.as_str())?
                .ok_or_else(|| operation_not_found(acknowledgement.operation_id.as_str()))?;
            if store.cleanup_ack_is_terminal_replay(&operation, acknowledgement)? {
                return Ok(operation);
            }
            let environment = store
                .load_environment_instance(operation.environment_id.as_str())?
                .ok_or_else(|| environment_not_found(operation.environment_id.as_str()))?;
            operation.validate_against_environment(&environment)?;
            let before = operation.clone();
            operation.apply_cleanup_step_acknowledgement(&environment, acknowledgement, now)?;
            if operation == before {
                return Ok(operation);
            }
            store.update_environment_lifecycle_cas(&before, &operation)?;
            Ok(operation)
        })
    }

    /// Finish a non-delete operation and publish its stable aggregate state.
    pub fn finish_environment_lifecycle(
        &self,
        operation_id: &str,
        generation: u64,
        now: u64,
    ) -> Result<EnvironmentLifecycleOperation, StackError> {
        self.with_immediate_transaction(|store| {
            let mut operation = store
                .load_environment_lifecycle(operation_id)?
                .ok_or_else(|| operation_not_found(operation_id))?;
            store.require_operation_generation(&operation, generation)?;
            if operation.kind == EnvironmentLifecycleKind::Delete {
                return Err(TopologyLifecycleError::DeleteRequired {
                    operation_id: operation_id.to_string(),
                }
                .into());
            }
            if matches!(
                operation.status,
                EnvironmentLifecycleStatus::Succeeded | EnvironmentLifecycleStatus::Failed
            ) {
                return Ok(operation);
            }
            let mut environment = store
                .load_environment_instance(operation.environment_id.as_str())?
                .ok_or_else(|| environment_not_found(operation.environment_id.as_str()))?;
            let environment_before = environment.clone();
            let operation_before = operation.clone();
            operation.finish_live_transition(&mut environment, now)?;
            store.update_environment_parent_cas(&environment_before, &environment)?;
            store.update_environment_lifecycle_cas(&operation_before, &operation)?;
            Ok(operation)
        })
    }

    /// Finish an exact delete, retaining the journal and tombstone outside the
    /// active aggregate while removing only the selected Environment's rows.
    pub fn finish_environment_delete(
        &self,
        operation_id: &str,
        generation: u64,
        now: u64,
    ) -> Result<(EnvironmentLifecycleOperation, EnvironmentTombstone), StackError> {
        self.with_immediate_transaction(|store| {
            let mut operation = store
                .load_environment_lifecycle(operation_id)?
                .ok_or_else(|| operation_not_found(operation_id))?;
            store.require_operation_generation(&operation, generation)?;
            if let Some(tombstone) =
                store.load_environment_tombstone(operation.environment_id.as_str())?
            {
                if tombstone.delete_operation_id == operation.operation_id {
                    return Ok((operation, tombstone));
                }
                return Err(StackError::InvalidSpec(format!(
                    "Environment `{}` has a tombstone for different delete operation `{}`",
                    operation.environment_id, tombstone.delete_operation_id
                )));
            }
            let environment = store
                .load_environment_instance(operation.environment_id.as_str())?
                .ok_or_else(|| environment_not_found(operation.environment_id.as_str()))?;
            store.require_no_nonterminal_stack_container_creates(
                operation.environment_id.as_str(),
            )?;
            let operation_before = operation.clone();
            let tombstone = operation.finish_delete(&environment, now)?;
            store.update_environment_lifecycle_cas(&operation_before, &operation)?;
            store.insert_environment_tombstone(&tombstone)?;
            store.delete_exact_environment(&environment, &operation)?;
            Ok((operation, tombstone))
        })
    }

    /// Load durable deletion history by the old immutable Environment ID.
    pub fn load_environment_tombstone(
        &self,
        environment_id: &str,
    ) -> Result<Option<EnvironmentTombstone>, StackError> {
        self.load_environment_tombstone_where("environment_id = ?1", environment_id)
    }

    /// List deletion history for one Project in stable deletion/identity order.
    pub fn list_environment_tombstones(
        &self,
        project_id: &str,
    ) -> Result<Vec<EnvironmentTombstone>, StackError> {
        let mut statement = self.conn.prepare(
            "SELECT environment_id FROM environment_tombstones
             WHERE project_id = ?1 ORDER BY deleted_at, environment_id",
        )?;
        statement
            .query_map(params![project_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|environment_id| {
                self.load_environment_tombstone(&environment_id)?
                    .ok_or_else(|| {
                        StackError::InvalidSpec(format!(
                            "Environment tombstone `{environment_id}` disappeared while listing"
                        ))
                    })
            })
            .collect()
    }

    pub(super) fn load_environment_instance(
        &self,
        environment_id: &str,
    ) -> Result<Option<EnvironmentInstance>, StackError> {
        let row = self
            .conn
            .query_row(
                "SELECT environment_id, project_id, schema_version, name, definition_digest,
                        state, instance_json, created_at, updated_at, legacy_sandbox_id,
                        lifecycle_generation, active_operation_id
                 FROM environment_instances WHERE environment_id = ?1",
                params![environment_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, Option<String>>(9)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, Option<String>>(11)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            sql_environment_id,
            project_id,
            schema_version,
            name,
            definition_digest,
            state,
            json,
            created_at,
            updated_at,
            legacy_sandbox_id,
            lifecycle_generation,
            active_operation_id,
        )) = row
        else {
            return Ok(None);
        };
        let table = "environment_instances";
        let mut environment: EnvironmentInstance =
            parse_persisted_json(table, &sql_environment_id, "instance_json", &json)?;
        require_projection(
            sql_environment_id == environment.environment_id.as_str(),
            table,
            &sql_environment_id,
            "environment_id",
        )?;
        require_projection(
            project_id == environment.project_id.as_str(),
            table,
            &sql_environment_id,
            "project_id",
        )?;
        require_projection(
            schema_version == i64::from(environment.schema_version),
            table,
            &sql_environment_id,
            "schema_version",
        )?;
        require_projection(name == environment.name, table, &sql_environment_id, "name")?;
        require_projection(
            definition_digest == environment.definition_digest,
            table,
            &sql_environment_id,
            "definition_digest",
        )?;
        require_projection(
            state == serde_json::to_string(&environment.state)?,
            table,
            &sql_environment_id,
            "state",
        )?;
        require_u64_projection(
            created_at,
            environment.created_at,
            table,
            &sql_environment_id,
            "created_at",
        )?;
        require_u64_projection(
            updated_at,
            environment.updated_at,
            table,
            &sql_environment_id,
            "updated_at",
        )?;
        require_projection(
            legacy_sandbox_id.as_deref()
                == environment
                    .legacy_migration
                    .as_ref()
                    .map(|provenance| provenance.legacy_sandbox_id.as_str()),
            table,
            &sql_environment_id,
            "legacy_sandbox_id",
        )?;
        require_u64_projection(
            lifecycle_generation,
            environment.lifecycle_generation,
            table,
            &sql_environment_id,
            "lifecycle_generation",
        )?;
        require_projection(
            active_operation_id.as_deref()
                == environment
                    .active_operation_id
                    .as_ref()
                    .map(|id| id.as_str()),
            table,
            &sql_environment_id,
            "active_operation_id",
        )?;
        self.load_environment_children(&sql_environment_id, &mut environment)?;
        environment
            .validate()
            .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
        Ok(Some(environment))
    }

    fn load_environment_lifecycle_by_idempotency_key(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<EnvironmentLifecycleOperation>, StackError> {
        self.load_environment_lifecycle_where("idempotency_key = ?1", idempotency_key)
    }

    fn load_environment_lifecycle_where(
        &self,
        predicate: &str,
        value: &str,
    ) -> Result<Option<EnvironmentLifecycleOperation>, StackError> {
        let sql = format!(
            "SELECT operation_id, idempotency_key, request_id, project_id, environment_id,
                    schema_version, generation, kind, status, request_hash, definition_digest,
                    initial_state, requested_target, operation_json, created_at, updated_at,
                    completed_at
             FROM environment_lifecycle_operations WHERE {predicate}"
        );
        let row = self
            .conn
            .query_row(&sql, params![value], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, String>(13)?,
                    row.get::<_, i64>(14)?,
                    row.get::<_, i64>(15)?,
                    row.get::<_, Option<i64>>(16)?,
                ))
            })
            .optional()?;
        let Some((
            operation_id,
            idempotency_key,
            request_id,
            project_id,
            environment_id,
            schema_version,
            generation,
            kind,
            status,
            request_hash,
            definition_digest,
            initial_state,
            requested_target,
            operation_json,
            created_at,
            updated_at,
            completed_at,
        )) = row
        else {
            return Ok(None);
        };
        let table = "environment_lifecycle_operations";
        let operation: EnvironmentLifecycleOperation =
            parse_persisted_json(table, &operation_id, "operation_json", &operation_json)?;
        for (matches, field) in [
            (
                operation_id == operation.operation_id.as_str(),
                "operation_id",
            ),
            (
                idempotency_key == operation.idempotency_key,
                "idempotency_key",
            ),
            (request_id == operation.request_id, "request_id"),
            (project_id == operation.project_id.as_str(), "project_id"),
            (
                environment_id == operation.environment_id.as_str(),
                "environment_id",
            ),
            (request_hash == operation.request_hash, "request_hash"),
            (
                definition_digest == operation.definition_digest,
                "definition_digest",
            ),
        ] {
            require_projection(matches, table, &operation_id, field)?;
        }
        require_projection(
            schema_version == i64::from(operation.schema_version),
            table,
            &operation_id,
            "schema_version",
        )?;
        require_u64_projection(
            generation,
            operation.generation,
            table,
            &operation_id,
            "generation",
        )?;
        require_projection(
            kind == serialized_string_projection(&operation.kind)?,
            table,
            &operation_id,
            "kind",
        )?;
        require_projection(
            status == serialized_string_projection(&operation.status)?,
            table,
            &operation_id,
            "status",
        )?;
        require_projection(
            initial_state == serialized_string_projection(&operation.initial_state)?,
            table,
            &operation_id,
            "initial_state",
        )?;
        require_projection(
            requested_target == serialized_string_projection(&operation.requested_target)?,
            table,
            &operation_id,
            "requested_target",
        )?;
        require_u64_projection(
            created_at,
            operation.created_at,
            table,
            &operation_id,
            "created_at",
        )?;
        require_u64_projection(
            updated_at,
            operation.updated_at,
            table,
            &operation_id,
            "updated_at",
        )?;
        require_projection(
            completed_at.and_then(|value| u64::try_from(value).ok()) == operation.completed_at,
            table,
            &operation_id,
            "completed_at",
        )?;
        operation.validate_structure()?;
        Ok(Some(operation))
    }

    fn insert_environment_lifecycle(
        &self,
        operation: &EnvironmentLifecycleOperation,
    ) -> Result<(), StackError> {
        operation.validate_structure()?;
        self.conn.execute(
            "INSERT INTO environment_lifecycle_operations
                (operation_id, idempotency_key, request_id, project_id, environment_id,
                 schema_version, generation, kind, status, request_hash, definition_digest,
                 initial_state, requested_target, operation_json, created_at, updated_at,
                 completed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                     ?14, ?15, ?16, ?17)",
            params![
                operation.operation_id.as_str(),
                operation.idempotency_key,
                operation.request_id,
                operation.project_id.as_str(),
                operation.environment_id.as_str(),
                operation.schema_version,
                sqlite_u64(operation.generation, "generation")?,
                serialized_string_projection(&operation.kind)?,
                serialized_string_projection(&operation.status)?,
                operation.request_hash,
                operation.definition_digest,
                serialized_string_projection(&operation.initial_state)?,
                serialized_string_projection(&operation.requested_target)?,
                serde_json::to_string(operation)?,
                sqlite_u64(operation.created_at, "created_at")?,
                sqlite_u64(operation.updated_at, "updated_at")?,
                operation
                    .completed_at
                    .map(|value| sqlite_u64(value, "completed_at"))
                    .transpose()?,
            ],
        )?;
        Ok(())
    }

    fn update_environment_lifecycle_cas(
        &self,
        before: &EnvironmentLifecycleOperation,
        after: &EnvironmentLifecycleOperation,
    ) -> Result<(), StackError> {
        after.validate_structure()?;
        let affected = self.conn.execute(
            "UPDATE environment_lifecycle_operations
             SET status = ?1, operation_json = ?2, updated_at = ?3, completed_at = ?4
             WHERE operation_id = ?5 AND generation = ?6 AND status = ?7
               AND operation_json = ?8",
            params![
                serialized_string_projection(&after.status)?,
                serde_json::to_string(after)?,
                sqlite_u64(after.updated_at, "updated_at")?,
                after
                    .completed_at
                    .map(|value| sqlite_u64(value, "completed_at"))
                    .transpose()?,
                before.operation_id.as_str(),
                sqlite_u64(before.generation, "generation")?,
                serialized_string_projection(&before.status)?,
                serde_json::to_string(before)?,
            ],
        )?;
        if affected == 1 {
            Ok(())
        } else {
            Err(StackError::Machine {
                code: vz_runtime_contract::MachineErrorCode::StateConflict,
                message: format!(
                    "lifecycle operation `{}` changed during compare-and-swap",
                    before.operation_id
                ),
            })
        }
    }

    fn require_exact_lifecycle_replay(
        &self,
        operation: &EnvironmentLifecycleOperation,
        environment_id: &str,
        kind: EnvironmentLifecycleKind,
        request_id: &str,
        request_hash: &str,
    ) -> Result<(), StackError> {
        if operation.environment_id.as_str() == environment_id
            && operation.kind == kind
            && operation.request_id == request_id
            && operation.request_hash == request_hash
        {
            return Ok(());
        }
        Err(TopologyLifecycleError::InvalidOperation {
            reason: format!(
                "idempotency key `{}` was already used by a different lifecycle request",
                operation.idempotency_key
            ),
        }
        .into())
    }

    fn require_operation_generation(
        &self,
        operation: &EnvironmentLifecycleOperation,
        generation: u64,
    ) -> Result<(), StackError> {
        if operation.generation == generation {
            Ok(())
        } else {
            Err(TopologyLifecycleError::GenerationMismatch {
                operation_id: operation.operation_id.to_string(),
                expected: operation.generation,
                found: generation,
            }
            .into())
        }
    }

    fn machine_ack_is_terminal_replay(
        &self,
        operation: &EnvironmentLifecycleOperation,
        acknowledgement: &MachineLifecycleStepAcknowledgement,
    ) -> Result<bool, StackError> {
        if !matches!(
            operation.status,
            EnvironmentLifecycleStatus::Succeeded | EnvironmentLifecycleStatus::Failed
        ) {
            return Ok(false);
        }
        if acknowledgement.operation_id != operation.operation_id {
            return Err(TopologyLifecycleError::OperationMismatch {
                environment_id: operation.environment_id.to_string(),
                expected: operation.operation_id.to_string(),
                found: acknowledgement.operation_id.to_string(),
            }
            .into());
        }
        self.require_operation_generation(operation, acknowledgement.generation)?;
        let step = operation
            .machine_steps
            .iter()
            .find(|step| step.machine_id == acknowledgement.machine_id)
            .ok_or_else(|| TopologyLifecycleError::MachineStepNotFound {
                operation_id: operation.operation_id.to_string(),
                machine_id: acknowledgement.machine_id.to_string(),
            })?;
        if step.initial_state != acknowledgement.initial_state
            || step.target_state != acknowledgement.target_state
            || step.expected_incarnation != acknowledgement.expected_incarnation
            || step.resulting_incarnation != acknowledgement.resulting_incarnation
        {
            return Err(TopologyLifecycleError::MachineStepMismatch {
                machine_id: acknowledgement.machine_id.to_string(),
            }
            .into());
        }
        let exact = matches!(
            (&step.status, &step.failure_reason, &acknowledgement.result),
            (
                LifecycleStepStatus::Succeeded,
                None,
                LifecycleStepResult::Succeeded
            )
        ) || matches!(
            (&step.status, &step.failure_reason, &acknowledgement.result),
            (
                LifecycleStepStatus::Failed,
                Some(existing),
                LifecycleStepResult::Failed { reason }
            ) if existing == reason
        );
        if exact {
            Ok(true)
        } else {
            Err(TopologyLifecycleError::MachineStepMismatch {
                machine_id: acknowledgement.machine_id.to_string(),
            }
            .into())
        }
    }

    fn cleanup_ack_is_terminal_replay(
        &self,
        operation: &EnvironmentLifecycleOperation,
        acknowledgement: &OwnershipCleanupStepAcknowledgement,
    ) -> Result<bool, StackError> {
        if operation.status != EnvironmentLifecycleStatus::Succeeded {
            return Ok(false);
        }
        if acknowledgement.operation_id != operation.operation_id {
            return Err(TopologyLifecycleError::OperationMismatch {
                environment_id: operation.environment_id.to_string(),
                expected: operation.operation_id.to_string(),
                found: acknowledgement.operation_id.to_string(),
            }
            .into());
        }
        self.require_operation_generation(operation, acknowledgement.generation)?;
        let step = operation
            .cleanup_steps
            .iter()
            .find(|step| step.ownership == acknowledgement.ownership)
            .ok_or_else(|| TopologyLifecycleError::OwnershipStepMismatch {
                operation_id: operation.operation_id.to_string(),
                resource_kind: serialized_string_projection(
                    &acknowledgement.ownership.resource_kind,
                )
                .unwrap_or_else(|_| "unknown".to_string()),
                resource_id: acknowledgement.ownership.resource_id.clone(),
            })?;
        if step.status == LifecycleStepStatus::Succeeded
            && matches!(acknowledgement.result, LifecycleStepResult::Succeeded)
        {
            Ok(true)
        } else {
            Err(TopologyLifecycleError::OwnershipStepMismatch {
                operation_id: operation.operation_id.to_string(),
                resource_kind: serialized_string_projection(
                    &acknowledgement.ownership.resource_kind,
                )?,
                resource_id: acknowledgement.ownership.resource_id.clone(),
            }
            .into())
        }
    }

    fn load_environment_tombstone_where(
        &self,
        predicate: &str,
        value: &str,
    ) -> Result<Option<EnvironmentTombstone>, StackError> {
        let sql = format!(
            "SELECT environment_id, project_id, schema_version, name, definition_digest,
                    delete_operation_id, lifecycle_generation, ownership_digest, deleted_at,
                    tombstone_json
             FROM environment_tombstones WHERE {predicate}"
        );
        let row = self
            .conn
            .query_row(&sql, params![value], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, String>(9)?,
                ))
            })
            .optional()?;
        let Some((
            environment_id,
            project_id,
            schema_version,
            name,
            definition_digest,
            delete_operation_id,
            lifecycle_generation,
            ownership_digest,
            deleted_at,
            tombstone_json,
        )) = row
        else {
            return Ok(None);
        };
        let table = "environment_tombstones";
        let tombstone: EnvironmentTombstone =
            parse_persisted_json(table, &environment_id, "tombstone_json", &tombstone_json)?;
        for (matches, field) in [
            (
                environment_id == tombstone.environment_id.as_str(),
                "environment_id",
            ),
            (project_id == tombstone.project_id.as_str(), "project_id"),
            (name == tombstone.name, "name"),
            (
                definition_digest == tombstone.definition_digest,
                "definition_digest",
            ),
            (
                delete_operation_id == tombstone.delete_operation_id.as_str(),
                "delete_operation_id",
            ),
            (
                ownership_digest == tombstone.ownership_digest,
                "ownership_digest",
            ),
        ] {
            require_projection(matches, table, &environment_id, field)?;
        }
        require_projection(
            schema_version == i64::from(tombstone.schema_version),
            table,
            &environment_id,
            "schema_version",
        )?;
        require_u64_projection(
            lifecycle_generation,
            tombstone.lifecycle_generation,
            table,
            &environment_id,
            "lifecycle_generation",
        )?;
        require_u64_projection(
            deleted_at,
            tombstone.deleted_at,
            table,
            &environment_id,
            "deleted_at",
        )?;
        tombstone.validate()?;
        let operation = self
            .load_environment_lifecycle(tombstone.delete_operation_id.as_str())?
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "tombstone `{environment_id}` references missing delete operation `{}`",
                    tombstone.delete_operation_id
                ))
            })?;
        require_projection(
            operation.kind == EnvironmentLifecycleKind::Delete
                && operation.status == EnvironmentLifecycleStatus::Succeeded
                && operation.project_id == tombstone.project_id
                && operation.environment_id == tombstone.environment_id
                && operation.definition_digest == tombstone.definition_digest
                && operation.generation == tombstone.lifecycle_generation,
            table,
            &environment_id,
            "delete_operation",
        )?;
        require_projection(
            lifecycle_ownership_digest(
                operation
                    .cleanup_steps
                    .iter()
                    .map(|step| step.ownership.clone()),
            )? == tombstone.ownership_digest,
            table,
            &environment_id,
            "cleanup_ownership_digest",
        )?;
        Ok(Some(tombstone))
    }

    fn insert_environment_tombstone(
        &self,
        tombstone: &EnvironmentTombstone,
    ) -> Result<(), StackError> {
        tombstone.validate()?;
        self.conn.execute(
            "INSERT INTO environment_tombstones
                (environment_id, project_id, schema_version, name, definition_digest,
                 delete_operation_id, lifecycle_generation, ownership_digest, deleted_at,
                 tombstone_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                tombstone.environment_id.as_str(),
                tombstone.project_id.as_str(),
                tombstone.schema_version,
                tombstone.name,
                tombstone.definition_digest,
                tombstone.delete_operation_id.as_str(),
                sqlite_u64(tombstone.lifecycle_generation, "tombstone generation")?,
                tombstone.ownership_digest,
                sqlite_u64(tombstone.deleted_at, "deleted_at")?,
                serde_json::to_string(tombstone)?,
            ],
        )?;
        Ok(())
    }

    fn delete_exact_environment(
        &self,
        environment: &EnvironmentInstance,
        operation: &EnvironmentLifecycleOperation,
    ) -> Result<(), StackError> {
        let current_ownership = self.load_ownership_records(environment.environment_id.as_str())?;
        let planned_ownership = operation
            .cleanup_steps
            .iter()
            .map(|step| step.ownership.clone())
            .collect::<Vec<_>>();
        if !semantic_collections_match(&planned_ownership, &current_ownership, |record| {
            format!("{:?}:{}", record.resource_kind, record.resource_id)
        }) {
            return Err(TopologyLifecycleError::OperationIncomplete {
                operation_id: operation.operation_id.to_string(),
            }
            .into());
        }
        for record in &planned_ownership {
            let affected = self.conn.execute(
                "DELETE FROM topology_ownership
                 WHERE resource_kind = ?1 AND resource_id = ?2 AND environment_id = ?3
                   AND machine_id IS ?4",
                params![
                    serde_json::to_string(&record.resource_kind)?,
                    record.resource_id,
                    record.environment_id.as_str(),
                    record.machine_id.as_ref().map(|id| id.as_str()),
                ],
            )?;
            if affected != 1 {
                return Err(StackError::Machine {
                    code: vz_runtime_contract::MachineErrorCode::StateConflict,
                    message: format!(
                        "owned resource `{:?}:{}` changed during exact delete",
                        record.resource_kind, record.resource_id
                    ),
                });
            }
        }
        for (table, expected) in [
            ("environment_endpoints", environment.endpoints.len()),
            ("environment_networks", environment.networks.len()),
            ("workspace_bindings", environment.bindings.len()),
            ("machine_instances", environment.machines.len()),
        ] {
            let affected = self.conn.execute(
                &format!("DELETE FROM {table} WHERE environment_id = ?1"),
                params![environment.environment_id.as_str()],
            )?;
            if affected != expected {
                return Err(StackError::Machine {
                    code: vz_runtime_contract::MachineErrorCode::StateConflict,
                    message: format!(
                        "Environment `{}` expected {expected} exact rows in `{table}` during delete, found {affected}",
                        environment.environment_id
                    ),
                });
            }
        }
        let affected = self.conn.execute(
            "DELETE FROM environment_instances
             WHERE environment_id = ?1 AND lifecycle_generation = ?2
               AND active_operation_id = ?3",
            params![
                environment.environment_id.as_str(),
                sqlite_u64(operation.generation, "generation")?,
                operation.operation_id.as_str(),
            ],
        )?;
        if affected != 1 {
            return Err(StackError::Machine {
                code: vz_runtime_contract::MachineErrorCode::StateConflict,
                message: format!(
                    "Environment `{}` changed during exact delete",
                    environment.environment_id
                ),
            });
        }
        self.refresh_project_timestamps(environment.project_id.as_str())
    }

    fn ensure_resource_reservation_allowed(
        &self,
        environment: &EnvironmentInstance,
    ) -> Result<(), StackError> {
        if let Some(active_operation_id) = &environment.active_operation_id {
            return Err(TopologyLifecycleError::OperationConflict {
                environment_id: environment.environment_id.to_string(),
                active_operation_id: active_operation_id.to_string(),
            }
            .into());
        }
        if matches!(
            environment.state,
            EnvironmentState::Deleting | EnvironmentState::Stopped
        ) {
            return Err(StackError::Machine {
                code: vz_runtime_contract::MachineErrorCode::StateConflict,
                message: format!(
                    "Environment `{}` cannot reserve resources while {:?}",
                    environment.environment_id, environment.state
                ),
            });
        }
        Ok(())
    }

    fn update_environment_parent_cas(
        &self,
        before: &EnvironmentInstance,
        after: &EnvironmentInstance,
    ) -> Result<(), StackError> {
        after
            .validate()
            .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
        let affected = self.conn.execute(
            "UPDATE environment_instances
             SET state = ?1, instance_json = ?2, updated_at = ?3,
                 lifecycle_generation = ?4, active_operation_id = ?5
             WHERE environment_id = ?6
               AND state = ?7
               AND lifecycle_generation = ?8
               AND active_operation_id IS ?9",
            params![
                serde_json::to_string(&after.state)?,
                serde_json::to_string(after)?,
                sqlite_u64(after.updated_at, "Environment updated_at")?,
                sqlite_u64(after.lifecycle_generation, "Environment generation")?,
                after.active_operation_id.as_ref().map(|id| id.as_str()),
                before.environment_id.as_str(),
                serde_json::to_string(&before.state)?,
                sqlite_u64(before.lifecycle_generation, "Environment generation")?,
                before.active_operation_id.as_ref().map(|id| id.as_str()),
            ],
        )?;
        if affected != 1 {
            return Err(StackError::Machine {
                code: vz_runtime_contract::MachineErrorCode::StateConflict,
                message: format!(
                    "Environment `{}` changed while applying lifecycle mutation",
                    before.environment_id
                ),
            });
        }
        self.refresh_project_timestamps(after.project_id.as_str())
    }

    fn update_machine_cas(
        &self,
        before: &MachineInstance,
        after: &MachineInstance,
    ) -> Result<(), StackError> {
        let affected = self.conn.execute(
            "UPDATE machine_instances
             SET state = ?1, instance_json = ?2
             WHERE machine_id = ?3 AND environment_id = ?4
               AND state = ?5 AND instance_json = ?6",
            params![
                serde_json::to_string(&after.state)?,
                serde_json::to_string(after)?,
                before.machine_id.as_str(),
                before.environment_id.as_str(),
                serde_json::to_string(&before.state)?,
                serde_json::to_string(before)?,
            ],
        )?;
        if affected == 1 {
            Ok(())
        } else {
            Err(StackError::Machine {
                code: vz_runtime_contract::MachineErrorCode::StateConflict,
                message: format!(
                    "Machine `{}` changed while applying lifecycle acknowledgement",
                    before.machine_id
                ),
            })
        }
    }

    fn update_machine_incarnation_ownership_cas(
        &self,
        before: &EnvironmentInstance,
        after: &EnvironmentInstance,
        machine_id: &vz_runtime_contract::types::MachineId,
    ) -> Result<(), StackError> {
        let incarnation_for = |environment: &EnvironmentInstance| {
            environment
                .ownership
                .iter()
                .find(|record| {
                    record.resource_kind == OwnedResourceKind::Incarnation
                        && record.machine_id.as_ref() == Some(machine_id)
                })
                .cloned()
        };
        let old = incarnation_for(before);
        let new = incarnation_for(after);
        if old == new {
            return Ok(());
        }

        if let Some(old) = &old {
            let affected = self.conn.execute(
                "DELETE FROM topology_ownership
                 WHERE resource_kind = ?1 AND resource_id = ?2 AND environment_id = ?3
                   AND machine_id = ?4 AND record_json = ?5",
                params![
                    serde_json::to_string(&old.resource_kind)?,
                    old.resource_id,
                    old.environment_id.as_str(),
                    machine_id.as_str(),
                    serde_json::to_string(old)?,
                ],
            )?;
            if affected != 1 {
                return Err(StackError::Machine {
                    code: vz_runtime_contract::MachineErrorCode::StateConflict,
                    message: format!(
                        "Machine `{machine_id}` incarnation ownership changed during acknowledgement"
                    ),
                });
            }
        }

        if let Some(new) = &new {
            let encoded_kind = serde_json::to_string(&new.resource_kind)?;
            let collision: Option<(String, Option<String>)> = self
                .conn
                .query_row(
                    "SELECT environment_id, machine_id FROM topology_ownership
                     WHERE resource_kind = ?1 AND resource_id = ?2",
                    params![encoded_kind, new.resource_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if let Some((existing_environment_id, existing_machine_id)) = collision {
                return Err(StackError::OwnedResourceCollision(Box::new(
                    OwnedResourceCollisionError {
                        resource_kind: encoded_kind,
                        resource_id: new.resource_id.clone(),
                        existing_environment_id,
                        existing_machine_id,
                    },
                )));
            }
            self.conn.execute(
                "INSERT INTO topology_ownership
                    (resource_kind, resource_id, environment_id, machine_id, schema_version,
                     record_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    encoded_kind,
                    new.resource_id,
                    new.environment_id.as_str(),
                    new.machine_id.as_ref().map(|id| id.as_str()),
                    new.schema_version,
                    serde_json::to_string(new)?,
                ],
            )?;
        }
        Ok(())
    }

    fn refresh_project_timestamps(&self, project_id: &str) -> Result<(), StackError> {
        self.conn.execute(
            "UPDATE project_definitions
             SET created_at = COALESCE(
                    (SELECT MIN(created_at) FROM environment_instances WHERE project_id = ?1), 0),
                 updated_at = COALESCE(
                    (SELECT MAX(updated_at) FROM environment_instances WHERE project_id = ?1), 0)
             WHERE project_id = ?1",
            params![project_id],
        )?;
        Ok(())
    }

    fn load_environments_for_project(
        &self,
        project_id: &str,
    ) -> Result<Vec<EnvironmentInstance>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT environment_id FROM environment_instances
             WHERE project_id = ?1 ORDER BY created_at, environment_id",
        )?;
        let environment_ids = stmt
            .query_map(params![project_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        environment_ids
            .into_iter()
            .map(|environment_id| {
                self.load_environment_instance(&environment_id)?
                    .ok_or_else(|| environment_not_found(&environment_id))
            })
            .collect()
    }

    fn load_environment_children(
        &self,
        environment_id: &str,
        environment: &mut EnvironmentInstance,
    ) -> Result<(), StackError> {
        let bindings = self.load_workspace_bindings(environment_id)?;
        require_projection(
            semantic_collections_match(&environment.bindings, &bindings, |binding| {
                binding.binding_id.to_string()
            }),
            "environment_instances",
            environment_id,
            "bindings",
        )?;

        let machines = self.load_machine_instances(environment_id)?;
        require_projection(
            semantic_collections_match(&environment.machines, &machines, |machine| {
                machine.machine_id.to_string()
            }),
            "environment_instances",
            environment_id,
            "machines",
        )?;

        let networks = self.load_network_instances(environment_id)?;
        require_projection(
            semantic_collections_match(&environment.networks, &networks, |network| {
                network.network_id.to_string()
            }),
            "environment_instances",
            environment_id,
            "networks",
        )?;

        let endpoints = self.load_endpoint_instances(environment_id)?;
        require_projection(
            semantic_collections_match(&environment.endpoints, &endpoints, |endpoint| {
                endpoint.endpoint_id.to_string()
            }),
            "environment_instances",
            environment_id,
            "endpoints",
        )?;

        let ownership = self.load_ownership_records(environment_id)?;
        require_projection(
            semantic_collections_match(&environment.ownership, &ownership, |record| {
                format!("{:?}:{}", record.resource_kind, record.resource_id)
            }),
            "environment_instances",
            environment_id,
            "ownership",
        )?;

        // Preserve the established normalized-row ordering after proving the parent
        // snapshot and child records describe the same aggregate.
        environment.bindings = bindings;
        environment.machines = machines;
        environment.networks = networks;
        environment.endpoints = endpoints;
        environment.ownership = ownership;
        Ok(())
    }

    fn load_workspace_bindings(
        &self,
        environment_id: &str,
    ) -> Result<Vec<WorkspaceBinding>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT binding_id, project_id, environment_id, schema_version, name,
                    workspace_key, path_hint, binding_json
             FROM workspace_bindings WHERE environment_id = ?1 ORDER BY binding_id",
        )?;
        let rows = stmt
            .query_map(params![environment_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, String>(7)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        rows.into_iter()
            .map(
                |(
                    binding_id,
                    project_id,
                    sql_environment_id,
                    schema_version,
                    name,
                    workspace_key,
                    path_hint,
                    json,
                )| {
                    let table = "workspace_bindings";
                    let binding: WorkspaceBinding =
                        parse_persisted_json(table, &binding_id, "binding_json", &json)?;
                    require_projection(
                        binding_id == binding.binding_id.as_str(),
                        table,
                        &binding_id,
                        "binding_id",
                    )?;
                    require_projection(
                        project_id == binding.project_id.as_str(),
                        table,
                        &binding_id,
                        "project_id",
                    )?;
                    require_projection(
                        sql_environment_id == binding.environment_id.as_str(),
                        table,
                        &binding_id,
                        "environment_id",
                    )?;
                    require_projection(
                        schema_version == i64::from(binding.schema_version),
                        table,
                        &binding_id,
                        "schema_version",
                    )?;
                    require_projection(name == binding.name, table, &binding_id, "name")?;
                    require_projection(
                        workspace_key == binding.workspace_key,
                        table,
                        &binding_id,
                        "workspace_key",
                    )?;
                    require_projection(
                        path_hint == binding.path_hint,
                        table,
                        &binding_id,
                        "path_hint",
                    )?;
                    Ok(binding)
                },
            )
            .collect()
    }

    fn load_machine_instances(
        &self,
        environment_id: &str,
    ) -> Result<Vec<MachineInstance>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT machine_id, environment_id, schema_version, name, state, instance_json,
                    legacy_sandbox_id
             FROM machine_instances WHERE environment_id = ?1 ORDER BY machine_id",
        )?;
        let rows = stmt
            .query_map(params![environment_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        rows.into_iter()
            .map(
                |(
                    machine_id,
                    sql_environment_id,
                    schema_version,
                    name,
                    state,
                    json,
                    legacy_sandbox_id,
                )| {
                    let table = "machine_instances";
                    let machine: MachineInstance =
                        parse_persisted_json(table, &machine_id, "instance_json", &json)?;
                    require_projection(
                        machine_id == machine.machine_id.as_str(),
                        table,
                        &machine_id,
                        "machine_id",
                    )?;
                    require_projection(
                        sql_environment_id == machine.environment_id.as_str(),
                        table,
                        &machine_id,
                        "environment_id",
                    )?;
                    require_projection(
                        schema_version == i64::from(machine.schema_version),
                        table,
                        &machine_id,
                        "schema_version",
                    )?;
                    require_projection(name == machine.name, table, &machine_id, "name")?;
                    require_projection(
                        state == serde_json::to_string(&machine.state)?,
                        table,
                        &machine_id,
                        "state",
                    )?;
                    require_projection(
                        legacy_sandbox_id == machine.legacy_sandbox_id,
                        table,
                        &machine_id,
                        "legacy_sandbox_id",
                    )?;
                    Ok(machine)
                },
            )
            .collect()
    }

    fn load_network_instances(
        &self,
        environment_id: &str,
    ) -> Result<Vec<NetworkInstance>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT network_id, environment_id, schema_version, name, instance_json
             FROM environment_networks WHERE environment_id = ?1 ORDER BY network_id",
        )?;
        let rows = stmt
            .query_map(params![environment_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        rows.into_iter()
            .map(
                |(network_id, sql_environment_id, schema_version, name, json)| {
                    let table = "environment_networks";
                    let network: NetworkInstance =
                        parse_persisted_json(table, &network_id, "instance_json", &json)?;
                    require_projection(
                        network_id == network.network_id.as_str(),
                        table,
                        &network_id,
                        "network_id",
                    )?;
                    require_projection(
                        sql_environment_id == network.environment_id.as_str(),
                        table,
                        &network_id,
                        "environment_id",
                    )?;
                    require_projection(
                        schema_version == i64::from(network.schema_version),
                        table,
                        &network_id,
                        "schema_version",
                    )?;
                    require_projection(name == network.name, table, &network_id, "name")?;
                    Ok(network)
                },
            )
            .collect()
    }

    fn load_endpoint_instances(
        &self,
        environment_id: &str,
    ) -> Result<Vec<EndpointInstance>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT endpoint_id, environment_id, machine_id, network_id, schema_version, name,
                    instance_json
             FROM environment_endpoints WHERE environment_id = ?1 ORDER BY endpoint_id",
        )?;
        let rows = stmt
            .query_map(params![environment_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        rows.into_iter()
            .map(
                |(
                    endpoint_id,
                    sql_environment_id,
                    machine_id,
                    network_id,
                    schema_version,
                    name,
                    json,
                )| {
                    let table = "environment_endpoints";
                    let endpoint: EndpointInstance =
                        parse_persisted_json(table, &endpoint_id, "instance_json", &json)?;
                    require_projection(
                        endpoint_id == endpoint.endpoint_id.as_str(),
                        table,
                        &endpoint_id,
                        "endpoint_id",
                    )?;
                    require_projection(
                        sql_environment_id == endpoint.environment_id.as_str(),
                        table,
                        &endpoint_id,
                        "environment_id",
                    )?;
                    require_projection(
                        machine_id == endpoint.machine_id.as_str(),
                        table,
                        &endpoint_id,
                        "machine_id",
                    )?;
                    require_projection(
                        network_id == endpoint.network_id.as_str(),
                        table,
                        &endpoint_id,
                        "network_id",
                    )?;
                    require_projection(
                        schema_version == i64::from(endpoint.schema_version),
                        table,
                        &endpoint_id,
                        "schema_version",
                    )?;
                    require_projection(name == endpoint.name, table, &endpoint_id, "name")?;
                    Ok(endpoint)
                },
            )
            .collect()
    }

    fn load_ownership_records(
        &self,
        environment_id: &str,
    ) -> Result<Vec<OwnershipRecord>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT resource_kind, resource_id, environment_id, machine_id, schema_version,
                    record_json
             FROM topology_ownership
             WHERE environment_id = ?1 ORDER BY resource_kind, resource_id",
        )?;
        let rows = stmt
            .query_map(params![environment_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        rows.into_iter()
            .map(
                |(
                    resource_kind,
                    resource_id,
                    sql_environment_id,
                    machine_id,
                    schema_version,
                    json,
                )| {
                    let table = "topology_ownership";
                    let key = format!("{resource_kind}:{resource_id}");
                    let record: OwnershipRecord =
                        parse_persisted_json(table, &key, "record_json", &json)?;
                    require_projection(
                        resource_kind == serde_json::to_string(&record.resource_kind)?,
                        table,
                        &key,
                        "resource_kind",
                    )?;
                    require_projection(
                        resource_id == record.resource_id,
                        table,
                        &key,
                        "resource_id",
                    )?;
                    require_projection(
                        sql_environment_id == record.environment_id.as_str(),
                        table,
                        &key,
                        "environment_id",
                    )?;
                    require_projection(
                        machine_id.as_deref() == record.machine_id.as_ref().map(|id| id.as_str()),
                        table,
                        &key,
                        "machine_id",
                    )?;
                    require_projection(
                        schema_version == i64::from(record.schema_version),
                        table,
                        &key,
                        "schema_version",
                    )?;
                    Ok(record)
                },
            )
            .collect()
    }

    pub(super) fn migrate_legacy_v1_to_v2(&self) -> Result<(), StackError> {
        self.migrate_legacy_v1_to_v2_with_hook(|_| Ok(()))
    }

    fn migrate_legacy_v1_to_v2_with_hook(
        &self,
        mut hook: impl FnMut(LegacyMigrationStage) -> Result<(), StackError>,
    ) -> Result<(), StackError> {
        self.with_immediate_transaction(|store| {
            // Hold one SQLite snapshot/write reservation across fingerprinting,
            // materialization, classification, and writes. No other connection can
            // replace validated input between the barrier and the v2 marker.
            let schema_version = store.schema_version()?;
            if schema_version != 1 {
                return Err(StackError::InvalidSpec(format!(
                    "legacy migration requires state schema version 1, found {schema_version}"
                )));
            }
            store.validate_legacy_v1_schema()?;

            let executions = store.list_executions()?;
            let builds = store.list_builds()?;
            let containers = store.list_containers()?;
            let sandboxes = store.list_sandboxes()?;
            for execution in &executions {
                execution
                    .ensure_lifecycle_consistency()
                    .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
            }
            for build in &builds {
                build
                    .ensure_lifecycle_consistency()
                    .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
            }
            for container in &containers {
                container
                    .ensure_lifecycle_consistency()
                    .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
            }
            for sandbox in &sandboxes {
                sandbox
                    .ensure_lifecycle_consistency()
                    .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
            }

            let mut migrated = Vec::new();
            for sandbox in sandboxes {
                match migrate_legacy_developer_sandbox(&sandbox) {
                    Ok(state) => migrated.push(state),
                    Err(LegacyMigrationError::NotDeveloper { .. }) => {}
                    Err(error) => return Err(StackError::InvalidSpec(error.to_string())),
                }
            }

            store.create_topology_schema_v2()?;
            hook(LegacyMigrationStage::TopologySchemaCreated)?;
            for (index, state) in migrated.iter().enumerate() {
                store.save_project_state_in_transaction(state)?;
                hook(LegacyMigrationStage::ProjectWritten(index))?;
            }
            store.validate_v2_schema()?;
            // The version marker is deliberately the final write in the transaction.
            store.set_schema_version(2)?;
            Ok(())
        })
    }

    pub(super) fn migrate_topology_v2_to_v3(&self) -> Result<(), StackError> {
        self.migrate_topology_v2_to_v3_with_hook(|_| Ok(()))
    }

    pub(super) fn migrate_stack_journal_v3_to_v4(&self) -> Result<(), StackError> {
        self.migrate_stack_journal_v3_to_v4_with_hook(|_| Ok(()))
    }

    fn migrate_stack_journal_v3_to_v4_with_hook(
        &self,
        mut hook: impl FnMut(StackJournalV4MigrationStage) -> Result<(), StackError>,
    ) -> Result<(), StackError> {
        self.with_immediate_transaction(|store| {
            let schema_version = store.schema_version()?;
            if schema_version != 3 {
                return Err(StackError::InvalidSpec(format!(
                    "stack journal migration requires state schema version 3, found {schema_version}"
                )));
            }
            store.validate_v3_schema()?;
            store.create_stack_journal_schema_v4()?;
            hook(StackJournalV4MigrationStage::JournalSchemaCreated)?;
            store.validate_v4_schema()?;
            store.set_schema_version(STORE_SCHEMA_VERSION)?;
            Ok(())
        })
    }

    fn migrate_topology_v2_to_v3_with_hook(
        &self,
        mut hook: impl FnMut(TopologyV3MigrationStage) -> Result<(), StackError>,
    ) -> Result<(), StackError> {
        self.with_immediate_transaction(|store| {
            let schema_version = store.schema_version()?;
            if schema_version != 2 {
                return Err(StackError::InvalidSpec(format!(
                    "topology migration requires state schema version 2, found {schema_version}"
                )));
            }
            store.validate_v2_schema()?;

            store.conn.execute_batch(TOPOLOGY_ENVIRONMENT_V3_DDL)?;
            store.conn.execute_batch(TOPOLOGY_OWNERSHIP_V2_TO_V3_DDL)?;
            hook(TopologyV3MigrationStage::OwnershipRebuilt)?;
            store.conn.execute_batch(TOPOLOGY_LIFECYCLE_V3_DDL)?;
            hook(TopologyV3MigrationStage::LifecycleSchemaCreated)?;

            store.validate_v3_schema()?;
            // The version marker is deliberately the final write in the transaction.
            store.set_schema_version(3)?;
            Ok(())
        })
    }

    #[cfg(test)]
    pub(super) fn migrate_legacy_v1_to_v2_with_failpoint(
        &self,
        failpoint: LegacyMigrationFailpoint,
    ) -> Result<(), StackError> {
        self.migrate_legacy_v1_to_v2_with_hook(|stage| {
            if matches!(
                (failpoint, stage),
                (
                    LegacyMigrationFailpoint::AfterFirstProjectWrite,
                    LegacyMigrationStage::ProjectWritten(0)
                )
            ) {
                return Err(StackError::InvalidSpec(
                    "injected v1-to-v2 migration failure after first project write".to_string(),
                ));
            }
            Ok(())
        })
    }

    #[cfg(test)]
    pub(super) fn migrate_topology_v2_to_v3_with_failpoint(
        &self,
        failpoint: TopologyV3MigrationFailpoint,
    ) -> Result<(), StackError> {
        self.migrate_topology_v2_to_v3_with_hook(|stage| {
            if matches!(
                (failpoint, stage),
                (
                    TopologyV3MigrationFailpoint::AfterOwnershipRebuild,
                    TopologyV3MigrationStage::OwnershipRebuilt
                )
            ) {
                return Err(StackError::InvalidSpec(
                    "injected v2-to-v3 migration failure after ownership rebuild".to_string(),
                ));
            }
            Ok(())
        })
    }

    #[cfg(test)]
    pub(super) fn migrate_stack_journal_v3_to_v4_with_failpoint(
        &self,
        failpoint: StackJournalV4MigrationFailpoint,
    ) -> Result<(), StackError> {
        self.migrate_stack_journal_v3_to_v4_with_hook(|stage| {
            if matches!(
                (failpoint, stage),
                (
                    StackJournalV4MigrationFailpoint::AfterJournalSchemaCreated,
                    StackJournalV4MigrationStage::JournalSchemaCreated
                )
            ) {
                return Err(StackError::InvalidSpec(
                    "injected v3-to-v4 migration failure after journal schema creation".to_string(),
                ));
            }
            Ok(())
        })
    }
}
