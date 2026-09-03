use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension, params};
use vz_runtime_contract::types::{
    EnvironmentInstance, LegacyMigrationError, ProjectState, TOPOLOGY_SCHEMA_VERSION,
    migrate_legacy_developer_sandbox,
};

use super::StateStore;
use crate::StackError;

pub(super) const STORE_SCHEMA_VERSION: u32 = 2;

pub(super) const TOPOLOGY_SCHEMA_DDL: &str = r#"
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

const REQUIRED_TOPOLOGY_TABLES: &[&str] = &[
    "project_definitions",
    "environment_instances",
    "workspace_bindings",
    "machine_instances",
    "environment_networks",
    "environment_endpoints",
    "topology_ownership",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyMigrationStage {
    TopologySchemaCreated,
    ProjectWritten(usize),
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LegacyMigrationFailpoint {
    AfterFirstProjectWrite,
}

fn normalized_schema_sql(sql: Option<String>) -> Option<String> {
    sql.map(|sql| sql.split_whitespace().collect::<Vec<_>>().join(" "))
}

type SchemaObjectKey = (String, String);
type SchemaObjectDefinition = (String, Option<String>);
type TopologySchemaShape = BTreeMap<SchemaObjectKey, SchemaObjectDefinition>;

fn topology_schema_shape(connection: &Connection) -> Result<TopologySchemaShape, StackError> {
    let mut statement = connection.prepare(
        "SELECT type, name, tbl_name, sql
         FROM sqlite_master
         WHERE type IN ('table', 'index')
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
        if (object_type == "table" && REQUIRED_TOPOLOGY_TABLES.contains(&name.as_str()))
            || (object_type == "index" && REQUIRED_TOPOLOGY_TABLES.contains(&table_name.as_str()))
        {
            shape.insert(
                (object_type, name),
                (table_name, normalized_schema_sql(sql)),
            );
        }
    }
    Ok(shape)
}

impl StateStore {
    pub(super) fn create_topology_schema(&self) -> Result<(), StackError> {
        self.conn.execute_batch(TOPOLOGY_SCHEMA_DDL)?;
        Ok(())
    }

    pub(super) fn validate_topology_schema(&self) -> Result<(), StackError> {
        // Build the canonical schema in a private reference database, then compare
        // every topology table and index definition. This validates the full set of
        // columns, CHECK/UNIQUE constraints, primary keys, indexes, and foreign-key
        // declarations rather than accepting a database that merely reused the
        // expected table names.
        let reference = Connection::open_in_memory()?;
        reference.execute_batch(TOPOLOGY_SCHEMA_DDL)?;
        let expected = topology_schema_shape(&reference)?;
        let actual = topology_schema_shape(&self.conn)?;
        if actual != expected {
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
            return Err(StackError::InvalidSpec(format!(
                "state schema v{STORE_SCHEMA_VERSION} topology shape mismatch: \
                 missing={missing:?}, unexpected={unexpected:?}, mismatched={mismatched:?}"
            )));
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
                "state schema v{STORE_SCHEMA_VERSION} contains a foreign-key violation: \
                 table={table}, row_id={row_id:?}, parent={parent}, foreign_key={foreign_key}"
            )));
        }
        Ok(())
    }

    /// Persist one complete Project aggregate atomically.
    pub fn save_project_state(&self, state: &ProjectState) -> Result<(), StackError> {
        state
            .validate()
            .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
        self.with_immediate_transaction(|store| store.save_project_state_in_transaction(state))
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

        for binding in &environment.bindings {
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

    /// Load one complete Project aggregate by stable identity.
    pub fn load_project_state(&self, project_id: &str) -> Result<Option<ProjectState>, StackError> {
        let definition_json: Option<String> = self
            .conn
            .query_row(
                "SELECT definition_json FROM project_definitions WHERE project_id = ?1",
                params![project_id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(definition_json) = definition_json else {
            return Ok(None);
        };
        let definition = serde_json::from_str(&definition_json)?;
        let environments = self.load_environments_for_project(project_id)?;
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

    fn load_environments_for_project(
        &self,
        project_id: &str,
    ) -> Result<Vec<EnvironmentInstance>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT environment_id, instance_json FROM environment_instances
             WHERE project_id = ?1 ORDER BY created_at, environment_id",
        )?;
        let rows = stmt
            .query_map(params![project_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(environment_id, json)| self.load_environment_children(&environment_id, &json))
            .collect()
    }

    fn load_environment_children(
        &self,
        environment_id: &str,
        environment_json: &str,
    ) -> Result<EnvironmentInstance, StackError> {
        let mut environment: EnvironmentInstance = serde_json::from_str(environment_json)?;
        environment.bindings = self.load_json_rows(
            "SELECT binding_json FROM workspace_bindings WHERE environment_id = ?1 ORDER BY binding_id",
            environment_id,
        )?;
        environment.machines = self.load_json_rows(
            "SELECT instance_json FROM machine_instances WHERE environment_id = ?1 ORDER BY machine_id",
            environment_id,
        )?;
        environment.networks = self.load_json_rows(
            "SELECT instance_json FROM environment_networks WHERE environment_id = ?1 ORDER BY network_id",
            environment_id,
        )?;
        environment.endpoints = self.load_json_rows(
            "SELECT instance_json FROM environment_endpoints WHERE environment_id = ?1 ORDER BY endpoint_id",
            environment_id,
        )?;
        environment.ownership = self.load_json_rows(
            "SELECT record_json FROM topology_ownership WHERE environment_id = ?1 ORDER BY resource_kind, resource_id",
            environment_id,
        )?;
        Ok(environment)
    }

    fn load_json_rows<T: serde::de::DeserializeOwned>(
        &self,
        query: &str,
        environment_id: &str,
    ) -> Result<Vec<T>, StackError> {
        let mut stmt = self.conn.prepare(query)?;
        let rows = stmt
            .query_map(params![environment_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|json| serde_json::from_str(&json).map_err(Into::into))
            .collect()
    }

    pub(super) fn migrate_legacy_v1_to_v2(&self) -> Result<(), StackError> {
        self.migrate_legacy_v1_to_v2_with_hook(|_| Ok(()))
    }

    fn migrate_legacy_v1_to_v2_with_hook(
        &self,
        mut hook: impl FnMut(LegacyMigrationStage) -> Result<(), StackError>,
    ) -> Result<(), StackError> {
        // Parse and classify every row before creating a v2 table. A malformed or
        // ambiguous legacy database therefore fails without any mutation.
        let mut migrated = Vec::new();
        for sandbox in self.list_sandboxes()? {
            match migrate_legacy_developer_sandbox(&sandbox) {
                Ok(state) => migrated.push(state),
                Err(LegacyMigrationError::NotDeveloper { .. }) => {}
                Err(error) => return Err(StackError::InvalidSpec(error.to_string())),
            }
        }

        self.with_immediate_transaction(|store| {
            store.create_topology_schema()?;
            hook(LegacyMigrationStage::TopologySchemaCreated)?;
            for (index, state) in migrated.iter().enumerate() {
                store.save_project_state_in_transaction(state)?;
                hook(LegacyMigrationStage::ProjectWritten(index))?;
            }
            // The version marker is deliberately the final write in the transaction.
            store.set_schema_version(STORE_SCHEMA_VERSION)?;
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
}
