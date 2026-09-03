use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension, params};
use vz_runtime_contract::types::{
    EndpointInstance, EnvironmentInstance, EnvironmentSelection, EnvironmentSelectionContext,
    EnvironmentState, EnvironmentUpDecision, LegacyMigrationError, MachineInstance,
    NetworkInstance, OwnershipRecord, ProjectDefinition, ProjectState, TOPOLOGY_SCHEMA_VERSION,
    TopologyResolutionError, WorkspaceBinding, migrate_legacy_developer_sandbox,
};

use super::StateStore;
use crate::StackError;
use crate::error::OwnedResourceCollisionError;

pub(super) const STORE_SCHEMA_VERSION: u32 = 2;

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
    pub(super) fn create_topology_schema(&self) -> Result<(), StackError> {
        self.conn.execute_batch(TOPOLOGY_SCHEMA_DDL)?;
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
        reference.create_topology_schema()?;

        let expected = state_schema_shape(&reference.conn)?;
        let actual = state_schema_shape(&self.conn)?;
        if actual != expected {
            return Err(schema_shape_mismatch(
                STORE_SCHEMA_VERSION,
                &expected,
                &actual,
            ));
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
            let mut project = match store.load_project_state(definition.project_id.as_str())? {
                Some(project) => {
                    if project.definition != *definition {
                        return Err(StackError::InvalidSpec(format!(
                            "project definition drift for `{}`; persisted digest={}, requested digest={}",
                            definition.project_id,
                            project.definition.digest().map_err(|error| StackError::InvalidSpec(error.to_string()))?,
                            definition.digest().map_err(|error| StackError::InvalidSpec(error.to_string()))?,
                        )));
                    }
                    project
                }
                None => ProjectState {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    definition: definition.clone(),
                    environments: Vec::new(),
                },
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
                    store.save_project_state_in_transaction(&project)?;
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

            environment.bindings.push(requested.clone());
            environment.updated_at = environment.updated_at.max(now);
            project
                .validate()
                .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
            store.save_project_state_in_transaction(&project)?;
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

            let refreshed = if let Some(existing) = environment
                .bindings
                .iter_mut()
                .find(|binding| binding.name == requested.name)
            {
                // The symbolic slot owns the immutable binding identity. A successful
                // reconcile may move that slot to a new opaque workspace key and may
                // refresh its diagnostic path without replacing any other resource.
                existing.workspace_key = requested.workspace_key.clone();
                existing.path_hint = requested.path_hint.clone();
                existing.clone()
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
                requested.clone()
            };
            environment.updated_at = environment.updated_at.max(now);
            project
                .validate()
                .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
            store.save_project_state_in_transaction(&project)?;
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

            let project_id: Option<String> = store
                .conn
                .query_row(
                    "SELECT project_id FROM environment_instances WHERE environment_id = ?1",
                    params![requested.environment_id.as_str()],
                    |row| row.get(0),
                )
                .optional()?;
            let project_id = project_id.ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "Environment `{}` not found while reserving resource `{}`",
                    requested.environment_id, requested.resource_id
                ))
            })?;
            let mut project = store.load_project_state(&project_id)?.ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "project `{project_id}` disappeared while reserving resource `{}`",
                    requested.resource_id
                ))
            })?;
            let environment = project
                .environments
                .iter_mut()
                .find(|environment| environment.environment_id == requested.environment_id)
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "Environment `{}` is not owned by project `{project_id}`",
                        requested.environment_id
                    ))
                })?;
            environment.ownership.push(requested.clone());
            environment.updated_at = environment.updated_at.max(now);
            project
                .validate()
                .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
            store.save_project_state_in_transaction(&project)?;
            Ok(requested.clone())
        })
    }

    fn load_environments_for_project(
        &self,
        project_id: &str,
    ) -> Result<Vec<EnvironmentInstance>, StackError> {
        let mut stmt = self.conn.prepare(
            "SELECT environment_id, project_id, schema_version, name, definition_digest, state,
                    instance_json, created_at, updated_at, legacy_sandbox_id
             FROM environment_instances
             WHERE project_id = ?1 ORDER BY created_at, environment_id",
        )?;
        let rows = stmt
            .query_map(params![project_id], |row| {
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
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(
                |(
                    environment_id,
                    sql_project_id,
                    schema_version,
                    name,
                    definition_digest,
                    state,
                    json,
                    created_at,
                    updated_at,
                    legacy_sandbox_id,
                )| {
                    let table = "environment_instances";
                    let mut environment: EnvironmentInstance =
                        parse_persisted_json(table, &environment_id, "instance_json", &json)?;
                    require_projection(
                        environment_id == environment.environment_id.as_str(),
                        table,
                        &environment_id,
                        "environment_id",
                    )?;
                    require_projection(
                        sql_project_id == environment.project_id.as_str(),
                        table,
                        &environment_id,
                        "project_id",
                    )?;
                    require_projection(
                        schema_version == i64::from(environment.schema_version),
                        table,
                        &environment_id,
                        "schema_version",
                    )?;
                    require_projection(name == environment.name, table, &environment_id, "name")?;
                    require_projection(
                        definition_digest == environment.definition_digest,
                        table,
                        &environment_id,
                        "definition_digest",
                    )?;
                    require_projection(
                        state == serde_json::to_string(&environment.state)?,
                        table,
                        &environment_id,
                        "state",
                    )?;
                    require_u64_projection(
                        created_at,
                        environment.created_at,
                        table,
                        &environment_id,
                        "created_at",
                    )?;
                    require_u64_projection(
                        updated_at,
                        environment.updated_at,
                        table,
                        &environment_id,
                        "updated_at",
                    )?;
                    require_projection(
                        legacy_sandbox_id.as_deref()
                            == environment
                                .legacy_migration
                                .as_ref()
                                .map(|provenance| provenance.legacy_sandbox_id.as_str()),
                        table,
                        &environment_id,
                        "legacy_sandbox_id",
                    )?;
                    self.load_environment_children(&environment_id, &mut environment)?;
                    Ok(environment)
                },
            )
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

            store.create_topology_schema()?;
            hook(LegacyMigrationStage::TopologySchemaCreated)?;
            for (index, state) in migrated.iter().enumerate() {
                store.save_project_state_in_transaction(state)?;
                hook(LegacyMigrationStage::ProjectWritten(index))?;
            }
            store.validate_v2_schema()?;
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
