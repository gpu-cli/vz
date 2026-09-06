use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use vz_runtime_contract::types::{
    EndpointInstance, EnvironmentId, EnvironmentInstance, EnvironmentLifecycleKind,
    EnvironmentLifecycleOperation, EnvironmentLifecycleStatus, EnvironmentSelection,
    EnvironmentSelectionContext, EnvironmentState, EnvironmentTombstone, EnvironmentUpDecision,
    LegacyMigrationError, LifecycleOperationId, LifecycleStepResult, LifecycleStepStatus,
    MachineInstance, MachineLifecycleStep, MachineLifecycleStepAcknowledgement, MachineState,
    NetworkInstance, OwnedResourceKind, OwnershipCleanupStepAcknowledgement, OwnershipRecord,
    ProjectDefinition, ProjectState, TOPOLOGY_SCHEMA_VERSION, TopologyLifecycleError,
    TopologyResolutionError, WorkspaceBinding, migrate_legacy_developer_sandbox,
};

use super::{ServiceObservedState, ServiceReplicaKey, StateStore};
use crate::StackError;
use crate::error::OwnedResourceCollisionError;

pub(super) const STORE_SCHEMA_VERSION: u32 = 9;
const STACK_JOURNAL_SCHEMA_VERSION: u32 = 4;
const REPLICA_SCHEMA_VERSION: u32 = 5;
const CLAIM_SCHEMA_VERSION: u32 = 7;

const REPLICA_SCHEMA_V5_DDL: &str = r#"
ALTER TABLE stack_container_create_intents
    ADD COLUMN applied_config_digest TEXT
        CHECK(applied_config_digest IS NULL OR length(trim(applied_config_digest)) > 0);
UPDATE stack_container_create_intents
SET applied_config_digest = json_extract(intent_json, '$.applied_config_digest')
WHERE json_type(intent_json, '$.applied_config_digest') IS 'text';
DROP TRIGGER stack_container_create_intent_immutable;
CREATE TRIGGER stack_container_create_intent_immutable
BEFORE UPDATE OF
    reservation_id, schema_version, project_id, environment_id, machine_id,
    machine_incarnation_id, environment_generation, stack_id, service_name,
    replica_index, service_generation, requested_container_id, definition_digest,
    action_digest, applied_config_digest, created_at
ON stack_container_create_intents
WHEN
    NEW.reservation_id <> OLD.reservation_id OR
    NEW.schema_version <> OLD.schema_version OR
    NEW.project_id <> OLD.project_id OR
    NEW.environment_id <> OLD.environment_id OR
    NEW.machine_id <> OLD.machine_id OR
    NEW.machine_incarnation_id <> OLD.machine_incarnation_id OR
    NEW.environment_generation <> OLD.environment_generation OR
    NEW.stack_id <> OLD.stack_id OR
    NEW.service_name <> OLD.service_name OR
    NEW.replica_index <> OLD.replica_index OR
    NEW.service_generation <> OLD.service_generation OR
    NEW.requested_container_id <> OLD.requested_container_id OR
    NEW.definition_digest <> OLD.definition_digest OR
    NEW.action_digest <> OLD.action_digest OR
    NEW.applied_config_digest IS NOT OLD.applied_config_digest OR
    NEW.created_at <> OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'stack container create intent projections are immutable');
END;

ALTER TABLE observed_state RENAME TO observed_state_v4;
CREATE TABLE observed_state (
    id INTEGER PRIMARY KEY,
    stack_name TEXT NOT NULL,
    service_name TEXT NOT NULL CHECK(length(trim(service_name)) BETWEEN 1 AND 128),
    replica_index INTEGER NOT NULL CHECK(replica_index > 0),
    state_json TEXT NOT NULL CHECK(json_valid(state_json)),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(stack_name, service_name, replica_index),
    CHECK(json_type(state_json, '$.replica.service_name') IS 'text'),
    CHECK(json_type(state_json, '$.replica.replica_index') IS 'integer'),
    CHECK(json_extract(state_json, '$.replica.service_name') IS service_name),
    CHECK(json_extract(state_json, '$.replica.replica_index') IS replica_index)
);
CREATE TABLE legacy_observed_state_quarantine_v5 (
    legacy_id INTEGER PRIMARY KEY,
    stack_name TEXT NOT NULL,
    service_name TEXT NOT NULL,
    replica_index INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    quarantined_at TEXT NOT NULL DEFAULT (datetime('now'))
);

DROP INDEX idx_reconcile_session_stack;
DROP INDEX idx_reconcile_session_status;
ALTER TABLE reconcile_sessions RENAME TO reconcile_sessions_v4;
CREATE TABLE legacy_reconcile_sessions_quarantine_v5 (
    session_id TEXT PRIMARY KEY,
    stack_name TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    status TEXT NOT NULL,
    actions_json TEXT NOT NULL,
    actions_hash TEXT NOT NULL,
    next_action_index INTEGER NOT NULL,
    total_actions INTEGER NOT NULL,
    started_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER,
    reason TEXT NOT NULL
);
INSERT INTO legacy_reconcile_sessions_quarantine_v5 (
    session_id, stack_name, operation_id, status, actions_json, actions_hash,
    next_action_index, total_actions, started_at, updated_at, completed_at, reason
)
SELECT session_id, stack_name, operation_id, status, actions_json, actions_hash,
       next_action_index, total_actions, started_at, updated_at, completed_at,
       'terminal legacy aggregate action session'
FROM reconcile_sessions_v4;
CREATE TABLE reconcile_sessions (
    session_id TEXT PRIMARY KEY,
    stack_name TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK(status IN ('active', 'completed', 'failed', 'superseded')),
    action_schema_version INTEGER NOT NULL CHECK(action_schema_version = 2),
    actions_json TEXT NOT NULL CHECK(json_valid(actions_json)),
    actions_hash TEXT NOT NULL,
    next_action_index INTEGER NOT NULL CHECK(next_action_index >= 0),
    total_actions INTEGER NOT NULL CHECK(total_actions >= 0),
    started_at INTEGER NOT NULL CHECK(started_at >= 0),
    updated_at INTEGER NOT NULL CHECK(updated_at >= started_at),
    completed_at INTEGER,
    CHECK(next_action_index <= total_actions),
    CHECK(status <> 'completed' OR next_action_index = total_actions),
    CHECK(
        (status = 'active' AND completed_at IS NULL) OR
        (status <> 'active' AND completed_at IS NOT NULL AND completed_at >= updated_at)
    )
);
DROP TABLE reconcile_sessions_v4;
CREATE INDEX idx_reconcile_session_stack ON reconcile_sessions(stack_name);
CREATE INDEX idx_reconcile_session_status ON reconcile_sessions(status);

ALTER TABLE reconcile_progress RENAME TO reconcile_progress_v4;
CREATE TABLE legacy_reconcile_progress_quarantine_v5 (
    legacy_id INTEGER PRIMARY KEY,
    stack_name TEXT NOT NULL UNIQUE,
    operation_id TEXT NOT NULL,
    actions_json TEXT NOT NULL,
    next_action_index INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    reason TEXT NOT NULL
);
INSERT INTO legacy_reconcile_progress_quarantine_v5 (
    legacy_id, stack_name, operation_id, actions_json,
    next_action_index, updated_at, reason
)
SELECT id, stack_name, operation_id, actions_json,
       next_action_index, updated_at, 'completed legacy aggregate progress marker'
FROM reconcile_progress_v4;
CREATE TABLE reconcile_progress (
    id INTEGER PRIMARY KEY,
    stack_name TEXT NOT NULL UNIQUE,
    operation_id TEXT NOT NULL,
    action_schema_version INTEGER NOT NULL CHECK(action_schema_version = 2),
    actions_json TEXT NOT NULL CHECK(json_valid(actions_json)),
    actions_hash TEXT NOT NULL,
    next_action_index INTEGER NOT NULL CHECK(next_action_index >= 0),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
DROP TABLE reconcile_progress_v4;

DROP INDEX idx_audit_session;
DROP INDEX idx_audit_stack;
ALTER TABLE reconcile_audit_log RENAME TO reconcile_audit_log_v4;
CREATE TABLE legacy_reconcile_audit_quarantine_v5 (
    legacy_id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL,
    stack_name TEXT NOT NULL,
    action_index INTEGER NOT NULL,
    action_kind TEXT NOT NULL,
    service_name TEXT NOT NULL,
    action_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    completed_at INTEGER,
    error_message TEXT,
    reason TEXT NOT NULL
);
INSERT INTO legacy_reconcile_audit_quarantine_v5 (
    legacy_id, session_id, stack_name, action_index, action_kind,
    service_name, action_hash, status, started_at, completed_at,
    error_message, reason
)
SELECT id, session_id, stack_name, action_index, action_kind,
       service_name, action_hash, status, started_at, completed_at,
       error_message, 'legacy aggregate action identity'
FROM reconcile_audit_log_v4;
DROP TABLE reconcile_audit_log_v4;
CREATE TABLE reconcile_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    stack_name TEXT NOT NULL,
    action_index INTEGER NOT NULL CHECK(action_index >= 0),
    action_kind TEXT NOT NULL
        CHECK(action_kind IN ('service_create', 'service_recreate', 'service_remove')),
    service_name TEXT NOT NULL CHECK(length(trim(service_name)) BETWEEN 1 AND 128),
    replica_index INTEGER NOT NULL CHECK(replica_index > 0),
    action_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('started', 'completed', 'failed')),
    started_at INTEGER NOT NULL CHECK(started_at >= 0),
    completed_at INTEGER CHECK(completed_at IS NULL OR completed_at >= started_at),
    error_message TEXT,
    UNIQUE(session_id, action_index),
    CHECK(
        (status = 'started' AND completed_at IS NULL AND error_message IS NULL) OR
        (status = 'completed' AND completed_at IS NOT NULL AND error_message IS NULL) OR
        (status = 'failed' AND completed_at IS NOT NULL AND error_message IS NOT NULL)
    )
);
CREATE INDEX idx_audit_session ON reconcile_audit_log(session_id);
CREATE INDEX idx_audit_stack ON reconcile_audit_log(stack_name);
"#;

const RECONCILE_SCHEMA_V6_ARCHIVE_DDL: &str = r#"
CREATE TABLE legacy_reconcile_sessions_quarantine_v6 (
    session_id TEXT PRIMARY KEY,
    stack_name TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    status TEXT NOT NULL,
    action_schema_version INTEGER NOT NULL CHECK(action_schema_version = 2),
    actions_json TEXT NOT NULL CHECK(json_valid(actions_json)),
    actions_hash TEXT NOT NULL,
    next_action_index INTEGER NOT NULL,
    total_actions INTEGER NOT NULL,
    started_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER NOT NULL,
    reason TEXT NOT NULL,
    quarantined_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE legacy_reconcile_progress_quarantine_v6 (
    legacy_id INTEGER PRIMARY KEY,
    stack_name TEXT NOT NULL UNIQUE,
    operation_id TEXT NOT NULL,
    action_schema_version INTEGER NOT NULL CHECK(action_schema_version = 2),
    actions_json TEXT NOT NULL CHECK(json_valid(actions_json)),
    actions_hash TEXT NOT NULL,
    next_action_index INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    quarantined_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE legacy_reconcile_audit_quarantine_v6 (
    legacy_id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL,
    stack_name TEXT NOT NULL,
    action_index INTEGER NOT NULL,
    action_kind TEXT NOT NULL,
    service_name TEXT NOT NULL,
    replica_index INTEGER NOT NULL,
    action_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    completed_at INTEGER NOT NULL,
    error_message TEXT,
    reason TEXT NOT NULL,
    quarantined_at TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT INTO legacy_reconcile_sessions_quarantine_v6 (
    session_id, stack_name, operation_id, status, action_schema_version,
    actions_json, actions_hash, next_action_index, total_actions, started_at,
    updated_at, completed_at, reason
)
SELECT session_id, stack_name, operation_id, status, action_schema_version,
       actions_json, actions_hash, next_action_index, total_actions, started_at,
       updated_at, completed_at, 'terminal action schema v2 session'
FROM reconcile_sessions;
INSERT INTO legacy_reconcile_progress_quarantine_v6 (
    legacy_id, stack_name, operation_id, action_schema_version, actions_json,
    actions_hash, next_action_index, updated_at, reason
)
SELECT id, stack_name, operation_id, action_schema_version, actions_json,
       actions_hash, next_action_index, updated_at,
       'terminal action schema v2 progress marker'
FROM reconcile_progress;
INSERT INTO legacy_reconcile_audit_quarantine_v6 (
    legacy_id, session_id, stack_name, action_index, action_kind, service_name,
    replica_index, action_hash, status, started_at, completed_at, error_message,
    reason
)
SELECT id, session_id, stack_name, action_index, action_kind, service_name,
       replica_index, action_hash, status, started_at, completed_at, error_message,
       'terminal action schema v2 audit'
FROM reconcile_audit_log;
DELETE FROM reconcile_audit_log;
"#;

const RECONCILE_SCHEMA_V6_ACTION_TABLES_DDL: &str = r#"
DROP INDEX idx_audit_session;
DROP INDEX idx_audit_stack;
ALTER TABLE reconcile_audit_log RENAME TO reconcile_audit_log_v5;
DROP INDEX idx_reconcile_session_stack;
DROP INDEX idx_reconcile_session_status;
ALTER TABLE reconcile_sessions RENAME TO reconcile_sessions_v5;
CREATE TABLE reconcile_sessions (
    session_id TEXT PRIMARY KEY,
    stack_name TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK(status IN ('active', 'completed', 'failed', 'superseded')),
    action_schema_version INTEGER NOT NULL CHECK(action_schema_version = 3),
    actions_json TEXT NOT NULL CHECK(json_valid(actions_json)),
    actions_hash TEXT NOT NULL,
    next_action_index INTEGER NOT NULL CHECK(next_action_index >= 0),
    total_actions INTEGER NOT NULL CHECK(total_actions >= 0),
    started_at INTEGER NOT NULL CHECK(started_at >= 0),
    updated_at INTEGER NOT NULL CHECK(updated_at >= started_at),
    completed_at INTEGER,
    CHECK(next_action_index <= total_actions),
    CHECK(status <> 'completed' OR next_action_index = total_actions),
    CHECK(
        (status = 'active' AND completed_at IS NULL) OR
        (status <> 'active' AND completed_at IS NOT NULL AND completed_at >= updated_at)
    )
);
CREATE TABLE reconcile_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    stack_name TEXT NOT NULL,
    action_index INTEGER NOT NULL CHECK(action_index >= 0),
    action_kind TEXT NOT NULL
        CHECK(action_kind IN ('service_create', 'service_recreate', 'service_remove')),
    service_name TEXT NOT NULL CHECK(length(trim(service_name)) BETWEEN 1 AND 128),
    replica_index INTEGER NOT NULL CHECK(replica_index > 0),
    action_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('started', 'completed', 'failed')),
    started_at INTEGER NOT NULL CHECK(started_at >= 0),
    completed_at INTEGER CHECK(completed_at IS NULL OR completed_at >= started_at),
    error_message TEXT,
    UNIQUE(session_id, action_index),
    CHECK(
        (status = 'started' AND completed_at IS NULL AND error_message IS NULL) OR
        (status = 'completed' AND completed_at IS NOT NULL AND error_message IS NULL) OR
        (status = 'failed' AND completed_at IS NOT NULL AND error_message IS NOT NULL)
    ),
    FOREIGN KEY(session_id) REFERENCES reconcile_sessions(session_id) ON DELETE RESTRICT
);
DROP TABLE reconcile_audit_log_v5;
DROP TABLE reconcile_sessions_v5;
CREATE INDEX idx_reconcile_session_stack ON reconcile_sessions(stack_name);
CREATE INDEX idx_reconcile_session_status ON reconcile_sessions(status);
CREATE INDEX idx_audit_session ON reconcile_audit_log(session_id);
CREATE INDEX idx_audit_stack ON reconcile_audit_log(stack_name);

ALTER TABLE reconcile_progress RENAME TO reconcile_progress_v5;
CREATE TABLE reconcile_progress (
    id INTEGER PRIMARY KEY,
    stack_name TEXT NOT NULL UNIQUE,
    operation_id TEXT NOT NULL,
    action_schema_version INTEGER NOT NULL CHECK(action_schema_version = 3),
    actions_json TEXT NOT NULL CHECK(json_valid(actions_json)),
    actions_hash TEXT NOT NULL,
    next_action_index INTEGER NOT NULL CHECK(next_action_index >= 0),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
DROP TABLE reconcile_progress_v5;
"#;

const RECONCILE_SCHEMA_V6_CLAIM_INDEX_DDL: &str = r#"
CREATE UNIQUE INDEX reconcile_one_started_replica
ON reconcile_audit_log(stack_name, service_name, replica_index)
WHERE status = 'started';
"#;

const CLAIM_SCHEMA_V7_DDL: &str = r#"
CREATE TRIGGER reconcile_session_identity_immutable
BEFORE UPDATE OF
    session_id, stack_name, operation_id, action_schema_version, actions_json,
    actions_hash, total_actions, started_at
ON reconcile_sessions
WHEN
    NEW.session_id IS NOT OLD.session_id OR
    NEW.stack_name IS NOT OLD.stack_name OR
    NEW.operation_id IS NOT OLD.operation_id OR
    NEW.action_schema_version IS NOT OLD.action_schema_version OR
    NEW.actions_json IS NOT OLD.actions_json OR
    NEW.actions_hash IS NOT OLD.actions_hash OR
    NEW.total_actions IS NOT OLD.total_actions OR
    NEW.started_at IS NOT OLD.started_at
BEGIN
    SELECT RAISE(ABORT, 'reconcile session action identity is immutable');
END;

CREATE TRIGGER reconcile_audit_identity_immutable
BEFORE UPDATE OF
    id, session_id, stack_name, action_index, action_kind, service_name,
    replica_index, action_hash, started_at
ON reconcile_audit_log
WHEN
    NEW.id IS NOT OLD.id OR
    NEW.session_id IS NOT OLD.session_id OR
    NEW.stack_name IS NOT OLD.stack_name OR
    NEW.action_index IS NOT OLD.action_index OR
    NEW.action_kind IS NOT OLD.action_kind OR
    NEW.service_name IS NOT OLD.service_name OR
    NEW.replica_index IS NOT OLD.replica_index OR
    NEW.action_hash IS NOT OLD.action_hash OR
    NEW.started_at IS NOT OLD.started_at
BEGIN
    SELECT RAISE(ABORT, 'reconcile audit claim identity is immutable');
END;

CREATE TRIGGER reconcile_started_audit_delete_restricted
BEFORE DELETE ON reconcile_audit_log
WHEN OLD.status = 'started'
BEGIN
    SELECT RAISE(ABORT, 'started reconcile claim cannot be deleted');
END;
"#;

const TEARDOWN_FINALIZER_SCHEMA_V8_DDL: &str = r#"
CREATE TABLE teardown_finalizers (
    operation_key TEXT PRIMARY KEY CHECK(length(trim(operation_key)) BETWEEN 1 AND 512),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    request_id TEXT NOT NULL CHECK(length(trim(request_id)) BETWEEN 1 AND 256),
    idempotency_key TEXT UNIQUE
        CHECK(idempotency_key IS NULL OR length(trim(idempotency_key)) BETWEEN 1 AND 256),
    request_digest TEXT NOT NULL CHECK(length(trim(request_digest)) > 0),
    session_id TEXT NOT NULL UNIQUE CHECK(length(trim(session_id)) BETWEEN 1 AND 256),
    reconcile_operation_id TEXT NOT NULL CHECK(length(trim(reconcile_operation_id)) > 0),
    project_id TEXT NOT NULL CHECK(length(trim(project_id)) BETWEEN 1 AND 128),
    environment_id TEXT NOT NULL CHECK(length(trim(environment_id)) BETWEEN 1 AND 128),
    machine_id TEXT NOT NULL CHECK(length(trim(machine_id)) BETWEEN 1 AND 128),
    machine_incarnation_id TEXT NOT NULL
        CHECK(length(trim(machine_incarnation_id)) BETWEEN 1 AND 128),
    stack_name TEXT NOT NULL CHECK(length(trim(stack_name)) BETWEEN 1 AND 128),
    remove_volumes INTEGER NOT NULL CHECK(remove_volumes IN (0, 1)),
    changed_actions INTEGER NOT NULL CHECK(changed_actions >= 0),
    actions_hash TEXT NOT NULL CHECK(length(trim(actions_hash)) > 0),
    desired_state_digest TEXT NOT NULL CHECK(length(trim(desired_state_digest)) > 0),
    initial_volumes_json TEXT NOT NULL CHECK(json_valid(initial_volumes_json)),
    initial_disk_image INTEGER NOT NULL CHECK(initial_disk_image IN (0, 1)),
    initial_runtime_present INTEGER NOT NULL CHECK(initial_runtime_present IN (0, 1)),
    runtime_shutdown INTEGER NOT NULL CHECK(runtime_shutdown IN (0, 1)),
    staged_volumes_json TEXT NOT NULL CHECK(json_valid(staged_volumes_json)),
    purged_volumes_json TEXT NOT NULL CHECK(json_valid(purged_volumes_json)),
    disk_staged INTEGER NOT NULL CHECK(disk_staged IN (0, 1)),
    disk_purged INTEGER NOT NULL CHECK(disk_purged IN (0, 1)),
    status TEXT NOT NULL CHECK(status IN ('prepared', 'completed')),
    receipt_id TEXT UNIQUE,
    finalizer_json TEXT NOT NULL CHECK(json_valid(finalizer_json)),
    created_at INTEGER NOT NULL CHECK(created_at >= 0),
    updated_at INTEGER NOT NULL CHECK(updated_at >= created_at),
    completed_at INTEGER,
    FOREIGN KEY(receipt_id) REFERENCES receipt_state(receipt_id) ON DELETE RESTRICT,
    CHECK(
        (status = 'prepared' AND receipt_id IS NULL AND completed_at IS NULL) OR
        (status = 'completed' AND receipt_id IS NOT NULL AND completed_at IS NOT NULL
            AND completed_at >= updated_at)
    )
);
CREATE INDEX idx_teardown_finalizer_stack
    ON teardown_finalizers(project_id, environment_id, machine_id, stack_name, status);
CREATE UNIQUE INDEX teardown_one_active_workload
    ON teardown_finalizers(
        project_id, environment_id, machine_id, machine_incarnation_id, stack_name
    )
    WHERE status = 'prepared';

CREATE TRIGGER teardown_finalizer_identity_immutable
BEFORE UPDATE OF
    operation_key, schema_version, request_id, idempotency_key, request_digest,
    session_id, reconcile_operation_id, project_id, environment_id, machine_id,
    machine_incarnation_id, stack_name, remove_volumes, changed_actions,
    actions_hash, desired_state_digest, initial_volumes_json, initial_disk_image,
    initial_runtime_present, created_at
ON teardown_finalizers
WHEN
    NEW.operation_key IS NOT OLD.operation_key OR
    NEW.schema_version IS NOT OLD.schema_version OR
    NEW.request_id IS NOT OLD.request_id OR
    NEW.idempotency_key IS NOT OLD.idempotency_key OR
    NEW.request_digest IS NOT OLD.request_digest OR
    NEW.session_id IS NOT OLD.session_id OR
    NEW.reconcile_operation_id IS NOT OLD.reconcile_operation_id OR
    NEW.project_id IS NOT OLD.project_id OR
    NEW.environment_id IS NOT OLD.environment_id OR
    NEW.machine_id IS NOT OLD.machine_id OR
    NEW.machine_incarnation_id IS NOT OLD.machine_incarnation_id OR
    NEW.stack_name IS NOT OLD.stack_name OR
    NEW.remove_volumes IS NOT OLD.remove_volumes OR
    NEW.changed_actions IS NOT OLD.changed_actions OR
    NEW.actions_hash IS NOT OLD.actions_hash OR
    NEW.desired_state_digest IS NOT OLD.desired_state_digest OR
    NEW.initial_volumes_json IS NOT OLD.initial_volumes_json OR
    NEW.initial_disk_image IS NOT OLD.initial_disk_image OR
    NEW.initial_runtime_present IS NOT OLD.initial_runtime_present OR
    NEW.created_at IS NOT OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'teardown finalizer identity and original inventory are immutable');
END;

CREATE TRIGGER teardown_finalizer_completed_immutable
BEFORE UPDATE ON teardown_finalizers
WHEN OLD.status = 'completed'
BEGIN
    SELECT RAISE(ABORT, 'completed teardown finalizer is immutable');
END;

CREATE TRIGGER teardown_finalizer_delete_restricted
BEFORE DELETE ON teardown_finalizers
BEGIN
    SELECT RAISE(ABORT, 'teardown finalizer is durable replay evidence');
END;

CREATE TRIGGER teardown_finalizer_receipt_update_restricted
BEFORE UPDATE ON receipt_state
WHEN EXISTS (
    SELECT 1 FROM teardown_finalizers WHERE receipt_id = OLD.receipt_id
)
BEGIN
    SELECT RAISE(ABORT, 'teardown finalizer receipt is immutable');
END;

CREATE TRIGGER teardown_finalizer_receipt_delete_restricted
BEFORE DELETE ON receipt_state
WHEN EXISTS (
    SELECT 1 FROM teardown_finalizers WHERE receipt_id = OLD.receipt_id
)
BEGIN
    SELECT RAISE(ABORT, 'teardown finalizer receipt is durable replay evidence');
END;
"#;

const TEARDOWN_RUNTIME_IDENTITY_SCHEMA_V9_DDL: &str = r#"
ALTER TABLE teardown_finalizers
    ADD COLUMN initial_runtime_identity_json TEXT
        CHECK(initial_runtime_identity_json IS NULL OR json_valid(initial_runtime_identity_json));
DROP TRIGGER teardown_finalizer_identity_immutable;
CREATE TRIGGER teardown_finalizer_identity_immutable
BEFORE UPDATE OF
    operation_key, schema_version, request_id, idempotency_key, request_digest,
    session_id, reconcile_operation_id, project_id, environment_id, machine_id,
    machine_incarnation_id, stack_name, remove_volumes, changed_actions,
    actions_hash, desired_state_digest, initial_volumes_json, initial_disk_image,
    initial_runtime_present, initial_runtime_identity_json, created_at
ON teardown_finalizers
WHEN
    NEW.operation_key IS NOT OLD.operation_key OR
    NEW.schema_version IS NOT OLD.schema_version OR
    NEW.request_id IS NOT OLD.request_id OR
    NEW.idempotency_key IS NOT OLD.idempotency_key OR
    NEW.request_digest IS NOT OLD.request_digest OR
    NEW.session_id IS NOT OLD.session_id OR
    NEW.reconcile_operation_id IS NOT OLD.reconcile_operation_id OR
    NEW.project_id IS NOT OLD.project_id OR
    NEW.environment_id IS NOT OLD.environment_id OR
    NEW.machine_id IS NOT OLD.machine_id OR
    NEW.machine_incarnation_id IS NOT OLD.machine_incarnation_id OR
    NEW.stack_name IS NOT OLD.stack_name OR
    NEW.remove_volumes IS NOT OLD.remove_volumes OR
    NEW.changed_actions IS NOT OLD.changed_actions OR
    NEW.actions_hash IS NOT OLD.actions_hash OR
    NEW.desired_state_digest IS NOT OLD.desired_state_digest OR
    NEW.initial_volumes_json IS NOT OLD.initial_volumes_json OR
    NEW.initial_disk_image IS NOT OLD.initial_disk_image OR
    NEW.initial_runtime_present IS NOT OLD.initial_runtime_present OR
    NEW.initial_runtime_identity_json IS NOT OLD.initial_runtime_identity_json OR
    NEW.created_at IS NOT OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'teardown finalizer identity and original inventory are immutable');
END;
"#;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicaV5MigrationStage {
    DurableActionsRebuilt,
    ObservedStateRebuilt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileV6MigrationStage {
    TerminalHistoryArchived,
    DurableActionsRebuilt,
    ReplicaClaimIndexCreated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimV7MigrationStage {
    ImmutabilityGuardsCreated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TeardownFinalizerV8MigrationStage {
    FinalizerSchemaCreated,
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

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplicaV5MigrationFailpoint {
    AfterDurableActionsRebuilt,
    AfterObservedStateRebuilt,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReconcileV6MigrationFailpoint {
    AfterTerminalHistoryArchived,
    AfterDurableActionsRebuilt,
    AfterReplicaClaimIndexCreated,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ClaimV7MigrationFailpoint {
    AfterImmutabilityGuardsCreated,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TeardownFinalizerV8MigrationFailpoint {
    AfterFinalizerSchemaCreated,
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

    pub(super) fn validate_v5_schema(&self) -> Result<(), StackError> {
        let reference = StateStore {
            conn: Connection::open_in_memory()?,
            event_sender: None,
        };
        reference.create_legacy_schema()?;
        reference.create_topology_schema_v3()?;
        reference.create_stack_journal_schema_v4()?;
        reference.create_replica_schema_v5()?;

        self.validate_schema_against(5, &reference.conn)
    }

    pub(super) fn validate_v6_schema(&self) -> Result<(), StackError> {
        let reference = StateStore {
            conn: Connection::open_in_memory()?,
            event_sender: None,
        };
        reference.create_legacy_schema()?;
        reference.create_topology_schema_v3()?;
        reference.create_stack_journal_schema_v4()?;
        reference.create_replica_schema_v5()?;
        reference.create_reconcile_schema_v6()?;

        self.validate_schema_against(6, &reference.conn)
    }

    pub(super) fn validate_v7_schema(&self) -> Result<(), StackError> {
        let reference = StateStore {
            conn: Connection::open_in_memory()?,
            event_sender: None,
        };
        reference.create_legacy_schema()?;
        reference.create_topology_schema_v3()?;
        reference.create_stack_journal_schema_v4()?;
        reference.create_replica_schema_v5()?;
        reference.create_reconcile_schema_v6()?;
        reference.create_claim_schema_v7()?;

        self.validate_schema_against(7, &reference.conn)
    }

    pub(super) fn validate_v8_schema(&self) -> Result<(), StackError> {
        let reference = StateStore {
            conn: Connection::open_in_memory()?,
            event_sender: None,
        };
        reference.create_legacy_schema()?;
        reference.create_topology_schema_v3()?;
        reference.create_stack_journal_schema_v4()?;
        reference.create_replica_schema_v5()?;
        reference.create_reconcile_schema_v6()?;
        reference.create_claim_schema_v7()?;
        reference.create_teardown_finalizer_schema_v8()?;

        self.validate_schema_against(8, &reference.conn)
    }

    pub(super) fn validate_v9_schema(&self) -> Result<(), StackError> {
        let reference = StateStore {
            conn: Connection::open_in_memory()?,
            event_sender: None,
        };
        reference.create_legacy_schema()?;
        reference.create_topology_schema_v3()?;
        reference.create_stack_journal_schema_v4()?;
        reference.create_replica_schema_v5()?;
        reference.create_reconcile_schema_v6()?;
        reference.create_claim_schema_v7()?;
        reference.create_teardown_finalizer_schema_v8()?;
        reference.create_teardown_runtime_identity_schema_v9()?;

        self.validate_schema_against(9, &reference.conn)
    }

    pub(super) fn create_reconcile_schema_v6(&self) -> Result<(), StackError> {
        self.conn.execute_batch(RECONCILE_SCHEMA_V6_ARCHIVE_DDL)?;
        self.conn
            .execute_batch(RECONCILE_SCHEMA_V6_ACTION_TABLES_DDL)?;
        self.conn
            .execute_batch(RECONCILE_SCHEMA_V6_CLAIM_INDEX_DDL)?;
        Ok(())
    }

    pub(super) fn create_claim_schema_v7(&self) -> Result<(), StackError> {
        self.conn.execute_batch(CLAIM_SCHEMA_V7_DDL)?;
        Ok(())
    }

    pub(super) fn create_teardown_finalizer_schema_v8(&self) -> Result<(), StackError> {
        self.conn.execute_batch(TEARDOWN_FINALIZER_SCHEMA_V8_DDL)?;
        Ok(())
    }

    pub(super) fn create_teardown_runtime_identity_schema_v9(&self) -> Result<(), StackError> {
        self.conn
            .execute_batch(TEARDOWN_RUNTIME_IDENTITY_SCHEMA_V9_DDL)?;
        Ok(())
    }

    pub(super) fn create_replica_schema_v5(&self) -> Result<(), StackError> {
        self.create_replica_schema_v5_with_hook(|| Ok(()))
    }

    fn create_replica_schema_v5_with_hook(
        &self,
        mut after_durable_actions: impl FnMut() -> Result<(), StackError>,
    ) -> Result<(), StackError> {
        let mut statement = self.conn.prepare(
            "SELECT id, stack_name, service_name, replica_index, state_json, updated_at
             FROM observed_state ORDER BY id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;
        let legacy_rows = rows.collect::<Result<Vec<_>, _>>()?;
        drop(statement);

        self.conn.execute_batch(REPLICA_SCHEMA_V5_DDL)?;
        after_durable_actions()?;

        for (id, stack_name, service_name, replica_index, state_json, updated_at) in legacy_rows {
            let replica_index_u32 = u32::try_from(replica_index).ok();
            let authoritative_reservation =
                if let Some(index) = replica_index_u32.filter(|index| *index > 0) {
                    self.conn
                        .query_row(
                            "SELECT reservation_id
                     FROM stack_container_create_intents
                     WHERE stack_id = ?1 AND service_name = ?2 AND replica_index = ?3
                     ORDER BY
                        CASE status
                          WHEN 'intent' THEN 0 WHEN 'reserved' THEN 0
                          WHEN 'running' THEN 0 WHEN 'cleanup_pending' THEN 0
                          WHEN 'blocked' THEN 0 ELSE 1
                        END,
                        service_generation DESC
                     LIMIT 1",
                            params![stack_name, service_name, index],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()?
                } else {
                    None
                };

            let Some(reservation_id) = authoritative_reservation else {
                let reason = if replica_index == 0 {
                    "legacy replica-zero identity"
                } else {
                    "replica row lacks exact journal authority"
                };
                self.conn.execute(
                    "INSERT INTO legacy_observed_state_quarantine_v5 (
                        legacy_id, stack_name, service_name, replica_index,
                        state_json, updated_at, reason
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        id,
                        stack_name,
                        service_name,
                        replica_index,
                        state_json,
                        updated_at,
                        reason
                    ],
                )?;
                continue;
            };

            let index = replica_index_u32.ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "journal-backed v4 observed row {id} has invalid replica index {replica_index}"
                ))
            })?;
            let expected_legacy_name = if index == 1 {
                service_name.clone()
            } else {
                format!("{service_name}-{index}")
            };
            let mut value: serde_json::Value = serde_json::from_str(&state_json).map_err(|error| {
                StackError::InvalidSpec(format!(
                    "v4 observed row {id} for `{stack_name}/{service_name}/{index}` has invalid JSON: {error}"
                ))
            })?;
            let object = value.as_object_mut().ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "v4 observed row {id} for `{stack_name}/{service_name}/{index}` is not an object"
                ))
            })?;
            let legacy_name = object
                .remove("service_name")
                .and_then(|value| value.as_str().map(str::to_owned))
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "v4 observed row {id} for `{stack_name}/{service_name}/{index}` lacks its legacy service name"
                    ))
                })?;
            if legacy_name != expected_legacy_name {
                return Err(StackError::InvalidSpec(format!(
                    "v4 observed row {id} identity mismatch: SQL `{service_name}/{index}` but JSON `{legacy_name}`"
                )));
            }
            let replica = ServiceReplicaKey::new(service_name.clone(), index)?;
            object.insert("replica".to_string(), serde_json::to_value(&replica)?);
            let observed: ServiceObservedState = serde_json::from_value(value).map_err(|error| {
                StackError::InvalidSpec(format!(
                    "v4 observed row {id} for `{stack_name}/{service_name}/{index}` is corrupt: {error}"
                ))
            })?;
            let canonical_json = serde_json::to_string(&observed)?;
            self.conn.execute(
                "INSERT INTO observed_state (
                    id, stack_name, service_name, replica_index, state_json, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    id,
                    stack_name,
                    service_name,
                    index,
                    canonical_json,
                    updated_at
                ],
            )?;
            let intent = self
                .load_stack_container_create_intent(&reservation_id)?
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "authoritative reservation `{reservation_id}` disappeared during replica migration"
                    ))
                })?;
            self.require_journal_observed_consistent(&intent)?;
        }
        self.conn.execute_batch("DROP TABLE observed_state_v4")?;
        Ok(())
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
            self.require_exclusive_machine_runtime_identity(machine)?;
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

    /// Load one complete Project aggregate from a single deferred read snapshot.
    ///
    /// Unlike [`Self::load_project_state`], this entry point owns its transaction
    /// boundary so separate SQLite writers cannot commit between the aggregate's
    /// definition and Environment reads.
    pub fn load_project_state_snapshot(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectState>, StackError> {
        let transaction = self.conn.unchecked_transaction()?;
        let state = self.load_project_state(project_id)?;
        transaction.commit()?;
        Ok(state)
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
        self.with_immediate_transaction(|store| {
            store.resolve_or_reserve_environment_for_up_in_transaction(
                definition,
                context,
                now,
                &|_| Ok(()),
            )
        })
    }

    /// Internal transaction body shared by trusted reservation and authorized,
    /// durable request admission. Authorization sees exact prospective IDs
    /// before any Project or Environment insertion.
    pub(super) fn resolve_or_reserve_environment_for_up_in_transaction(
        &self,
        definition: &ProjectDefinition,
        context: &EnvironmentSelectionContext,
        now: u64,
        authorize: &impl Fn(&EnvironmentInstance) -> Result<(), StackError>,
    ) -> Result<EnvironmentUpReservation, StackError> {
        definition
            .validate()
            .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
        let store = self;
        let (mut project, project_exists) = match store
            .load_project_state(definition.project_id.as_str())?
        {
            Some(project) => {
                if project.definition != *definition {
                    return Err(StackError::InvalidSpec(format!(
                        "project definition drift for `{}`; persisted digest={}, requested digest={}",
                        definition.project_id,
                        project
                            .definition
                            .digest()
                            .map_err(|error| StackError::InvalidSpec(error.to_string()))?,
                        definition
                            .digest()
                            .map_err(|error| StackError::InvalidSpec(error.to_string()))?,
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
                authorize(&environment)?;
                Ok(EnvironmentUpReservation::Existing {
                    selection,
                    environment,
                })
            }
            EnvironmentUpDecision::Create { name } => {
                let environment = definition
                    .instantiate_environment(name, now)
                    .map_err(|error| StackError::InvalidSpec(error.to_string()))?;
                authorize(&environment)?;
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

    /// Require the exact, never-started Environment snapshot before admission.
    ///
    /// The complete Project aggregate and absence of any lifecycle history are
    /// checked in one deferred read snapshot. A previously started Environment
    /// cannot become eligible by resetting its visible state to Creating.
    ///
    /// This read assertion does not authorize later effects. The caller must
    /// hold its per-Environment controller serialization across this check and
    /// admission, and re-read after its own reservations change the aggregate.
    /// Supply a persisted snapshot: child collections returned by the state
    /// loader have canonical ordering, unlike an unpersisted instantiation.
    pub fn require_environment_admission_fence(
        &self,
        expected: &EnvironmentInstance,
    ) -> Result<EnvironmentInstance, StackError> {
        let conflict = |reason: &str| StackError::Machine {
            code: vz_runtime_contract::MachineErrorCode::StateConflict,
            message: format!(
                "Environment `{}` admission fence refused: {reason}",
                expected.environment_id
            ),
        };
        expected
            .validate()
            .map_err(|error| TopologyLifecycleError::InvalidOperation {
                reason: format!("invalid expected admission snapshot: {error}"),
            })?;
        let transaction = self.conn.unchecked_transaction()?;
        let project = self
            .load_project_state(expected.project_id.as_str())?
            .ok_or_else(|| conflict("owning Project is absent"))?;
        let environment = project
            .environments
            .into_iter()
            .find(|environment| environment.environment_id == expected.environment_id)
            .ok_or_else(|| conflict("Environment is absent from its owning Project"))?;
        if environment != *expected {
            return Err(conflict(
                "persisted aggregate differs from the expected snapshot",
            ));
        }
        if environment.state != EnvironmentState::Creating
            || environment.lifecycle_generation != 0
            || environment.active_operation_id.is_some()
            || environment.legacy_migration.is_some()
        {
            return Err(conflict(
                "Environment is not a never-started Creating instance",
            ));
        }
        if environment.machines.iter().any(|machine| {
            machine.state != MachineState::Creating
                || machine.backend.is_some()
                || machine.incarnation.is_some()
                || machine.runtime_identity.is_some()
                || machine.legacy_sandbox_id.is_some()
                || !machine.negotiated_capabilities.capabilities.is_empty()
                || !machine.negotiated_capabilities.unsupported.is_empty()
        }) {
            return Err(conflict(
                "a Machine retains activation or previous lifecycle state",
            ));
        }
        let has_history: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM environment_lifecycle_operations
             WHERE environment_id = ?1)",
            params![environment.environment_id.as_str()],
            |row| row.get(0),
        )?;
        if has_history {
            return Err(conflict("Environment has persisted lifecycle history"));
        }
        transaction.commit()?;
        Ok(environment)
    }

    /// Require one exact persisted ownership edge without reserving or mutating it.
    ///
    /// The ownership row, its Environment aggregate, and any attached lifecycle
    /// journal are validated from one deferred SQLite snapshot. This is only a
    /// read-side identity assertion: it grants no authority to perform effects.
    /// Callers must independently fence lifecycle operation and generation before
    /// mutating a physical resource.
    pub fn require_owned_resource(
        &self,
        expected: &OwnershipRecord,
    ) -> Result<OwnershipRecord, StackError> {
        let transaction = self.conn.unchecked_transaction()?;
        let existing = self.require_exact_owned_resource_row(expected)?;
        self.require_owned_resource_parent(expected)?;

        transaction.commit()?;
        Ok(existing)
    }

    /// Require an exact current Machine lifecycle fence and all supplied resources.
    ///
    /// The Environment aggregate, active operation journal, exact Machine step,
    /// and every ownership row are checked in one deferred SQLite read snapshot.
    /// `expected_ownership` must be nonempty, duplicate-free, and entirely scoped
    /// to the Machine in `expected_step`.
    ///
    /// This is an instantaneous read assertion, not authority for a later effect.
    /// The caller must hold its per-Environment controller serialization across
    /// this check and the corresponding physical operation.
    pub fn require_current_machine_lifecycle_fence(
        &self,
        expected_operation: &EnvironmentLifecycleOperation,
        expected_step: &MachineLifecycleStep,
        expected_ownership: &[OwnershipRecord],
    ) -> Result<(EnvironmentInstance, EnvironmentLifecycleOperation), StackError> {
        let transaction = self.conn.unchecked_transaction()?;
        if expected_operation.status != EnvironmentLifecycleStatus::Running {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: format!(
                    "expected lifecycle operation `{}` must be Running, found {:?}",
                    expected_operation.operation_id, expected_operation.status
                ),
            }
            .into());
        }
        expected_operation.validate_structure()?;
        let expected_operation_step = expected_operation
            .machine_steps
            .iter()
            .find(|step| step.machine_id == expected_step.machine_id)
            .ok_or_else(|| TopologyLifecycleError::MachineStepNotFound {
                operation_id: expected_operation.operation_id.to_string(),
                machine_id: expected_step.machine_id.to_string(),
            })?;
        if expected_operation_step != expected_step
            || !matches!(
                expected_step.status,
                LifecycleStepStatus::Pending | LifecycleStepStatus::Running
            )
        {
            return Err(TopologyLifecycleError::MachineStepMismatch {
                machine_id: expected_step.machine_id.to_string(),
            }
            .into());
        }
        if expected_ownership.is_empty() {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: format!(
                    "Machine `{}` lifecycle fence requires at least one owned resource",
                    expected_step.machine_id
                ),
            }
            .into());
        }
        let mut ownership_keys = BTreeSet::new();
        for ownership in expected_ownership {
            if ownership.environment_id != expected_operation.environment_id
                || ownership.machine_id.as_ref() != Some(&expected_step.machine_id)
            {
                return Err(TopologyLifecycleError::InvalidOperation {
                    reason: format!(
                        "owned resource `{:?}:{}` is not scoped to Machine `{}` in Environment `{}`",
                        ownership.resource_kind,
                        ownership.resource_id,
                        expected_step.machine_id,
                        expected_operation.environment_id
                    ),
                }
                .into());
            }
            if !ownership_keys.insert((
                ownership.resource_kind.clone(),
                ownership.resource_id.as_str(),
            )) {
                return Err(TopologyLifecycleError::InvalidOperation {
                    reason: format!(
                        "duplicate owned resource `{:?}:{}` in Machine `{}` lifecycle fence",
                        ownership.resource_kind, ownership.resource_id, expected_step.machine_id
                    ),
                }
                .into());
            }
        }

        let environment = self
            .load_environment_instance(expected_operation.environment_id.as_str())?
            .ok_or_else(|| environment_not_found(expected_operation.environment_id.as_str()))?;
        let active_operation_id = environment.active_operation_id.as_ref().ok_or_else(|| {
            TopologyLifecycleError::OperationMismatch {
                environment_id: environment.environment_id.to_string(),
                expected: "active operation".to_string(),
                found: expected_operation.operation_id.to_string(),
            }
        })?;
        if active_operation_id != &expected_operation.operation_id {
            return Err(TopologyLifecycleError::OperationMismatch {
                environment_id: environment.environment_id.to_string(),
                expected: active_operation_id.to_string(),
                found: expected_operation.operation_id.to_string(),
            }
            .into());
        }
        let current_operation = self
            .load_environment_lifecycle(active_operation_id.as_str())?
            .ok_or_else(|| operation_not_found(active_operation_id.as_str()))?;
        current_operation.validate_against_environment(&environment)?;
        if current_operation.status != EnvironmentLifecycleStatus::Running {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: format!(
                    "current lifecycle operation `{}` must be Running, found {:?}",
                    current_operation.operation_id, current_operation.status
                ),
            }
            .into());
        }
        if current_operation.generation != expected_operation.generation {
            return Err(TopologyLifecycleError::GenerationMismatch {
                operation_id: current_operation.operation_id.to_string(),
                expected: current_operation.generation,
                found: expected_operation.generation,
            }
            .into());
        }
        if current_operation.kind != expected_operation.kind
            || current_operation.project_id != expected_operation.project_id
            || current_operation.environment_id != expected_operation.environment_id
            || current_operation.definition_digest != expected_operation.definition_digest
        {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: format!(
                    "expected lifecycle operation `{}` does not match the current Project/Environment/kind/definition fence",
                    expected_operation.operation_id
                ),
            }
            .into());
        }
        let current_step = current_operation
            .machine_steps
            .iter()
            .find(|step| step.machine_id == expected_step.machine_id)
            .ok_or_else(|| TopologyLifecycleError::MachineStepNotFound {
                operation_id: current_operation.operation_id.to_string(),
                machine_id: expected_step.machine_id.to_string(),
            })?;
        if current_step != expected_step
            || !matches!(
                current_step.status,
                LifecycleStepStatus::Pending | LifecycleStepStatus::Running
            )
        {
            return Err(TopologyLifecycleError::MachineStepMismatch {
                machine_id: expected_step.machine_id.to_string(),
            }
            .into());
        }
        for ownership in expected_ownership {
            self.require_exact_owned_resource_row(ownership)?;
            if !environment
                .ownership
                .iter()
                .any(|record| record == ownership)
            {
                return Err(StackError::Machine {
                    code: vz_runtime_contract::MachineErrorCode::StateConflict,
                    message: format!(
                        "owned resource `{:?}:{}` is absent from Environment `{}`",
                        ownership.resource_kind, ownership.resource_id, ownership.environment_id
                    ),
                });
            }
        }

        transaction.commit()?;
        Ok((environment, current_operation))
    }

    fn require_exact_owned_resource_row(
        &self,
        expected: &OwnershipRecord,
    ) -> Result<OwnershipRecord, StackError> {
        let encoded_kind = serde_json::to_string(&expected.resource_kind)?;
        let row: Option<(String, String, String, Option<String>, i64, String)> = self
            .conn
            .query_row(
                "SELECT resource_kind, resource_id, environment_id, machine_id,
                        schema_version, record_json
                 FROM topology_ownership
                 WHERE resource_kind = ?1 AND resource_id = ?2",
                params![encoded_kind, expected.resource_id],
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
        let Some((
            resource_kind,
            resource_id,
            sql_environment_id,
            sql_machine_id,
            schema_version,
            json,
        )) = row
        else {
            return Err(StackError::Machine {
                code: vz_runtime_contract::MachineErrorCode::NotFound,
                message: format!(
                    "owned resource `{:?}:{}` not found",
                    expected.resource_kind, expected.resource_id
                ),
            });
        };

        let table = "topology_ownership";
        let key = format!("{resource_kind}:{resource_id}");
        let existing: OwnershipRecord = parse_persisted_json(table, &key, "record_json", &json)?;
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
            sql_machine_id.as_deref() == existing.machine_id.as_ref().map(|id| id.as_str()),
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

        if existing != *expected {
            if existing.environment_id != expected.environment_id
                || existing.machine_id != expected.machine_id
            {
                return Err(StackError::OwnedResourceCollision(Box::new(
                    OwnedResourceCollisionError {
                        resource_kind: encoded_kind,
                        resource_id: expected.resource_id.clone(),
                        existing_environment_id: existing.environment_id.to_string(),
                        existing_machine_id: existing.machine_id.map(|id| id.to_string()),
                    },
                )));
            }
            return Err(StackError::Machine {
                code: vz_runtime_contract::MachineErrorCode::StateConflict,
                message: format!(
                    "owned resource `{:?}:{}` does not match its exact persisted record",
                    expected.resource_kind, expected.resource_id
                ),
            });
        }

        Ok(existing)
    }

    fn require_owned_resource_parent(
        &self,
        expected: &OwnershipRecord,
    ) -> Result<EnvironmentInstance, StackError> {
        let environment = self
            .load_environment_instance(expected.environment_id.as_str())?
            .ok_or_else(|| environment_not_found(expected.environment_id.as_str()))?;
        if !environment
            .ownership
            .iter()
            .any(|record| record == expected)
        {
            return Err(StackError::Machine {
                code: vz_runtime_contract::MachineErrorCode::StateConflict,
                message: format!(
                    "owned resource `{:?}:{}` is absent from Environment `{}`",
                    expected.resource_kind, expected.resource_id, expected.environment_id
                ),
            });
        }
        if let Some(machine_id) = &expected.machine_id
            && !environment
                .machines
                .iter()
                .any(|machine| &machine.machine_id == machine_id)
        {
            return Err(StackError::Machine {
                code: vz_runtime_contract::MachineErrorCode::StateConflict,
                message: format!(
                    "owned resource `{:?}:{}` references Machine `{machine_id}` outside Environment `{}`",
                    expected.resource_kind, expected.resource_id, expected.environment_id
                ),
            });
        }
        if let Some(operation_id) = environment.active_operation_id.as_ref() {
            let operation = self
                .load_environment_lifecycle(operation_id.as_str())?
                .ok_or_else(|| operation_not_found(operation_id.as_str()))?;
            operation.validate_against_environment(&environment)?;
        }
        Ok(environment)
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

            // A persisted Ready state is not proof that a backend is still
            // running. Every new Up requires an explicit activation receipt,
            // even when preserving the current incarnation.
            if kind == EnvironmentLifecycleKind::Stop {
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
                                resulting_incarnation: None,
                                resulting_activation: None,
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

    /// A generation lookup is read-only and never supplies physical absence
    /// authority by itself. Callers must validate the exact owner, operation,
    /// successful Machine step and retained controller fence before effects.
    pub fn load_environment_lifecycle_at_generation(
        &self,
        environment_id: &EnvironmentId,
        generation: u64,
    ) -> Result<Option<EnvironmentLifecycleOperation>, StackError> {
        let operation_id: Option<String> = self.conn.query_row(
            "SELECT operation_id FROM environment_lifecycle_operations WHERE environment_id = ?1 AND generation = ?2",
            params![environment_id.as_str(), sqlite_u64(generation, "generation")?],
            |row| row.get(0),
        ).optional()?;
        operation_id
            .map(|id| {
                let operation = self
                    .load_environment_lifecycle(&id)?
                    .ok_or_else(|| operation_not_found(&id))?;
                if &operation.environment_id != environment_id || operation.generation != generation
                {
                    return Err(StackError::InvalidSpec(
                        "lifecycle generation lookup changed identity".into(),
                    ));
                }
                Ok(operation)
            })
            .transpose()
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
            if let Some(machine) = machine_after.as_ref() {
                store.require_exclusive_machine_runtime_identity(machine)?;
            }
            store.update_machine_incarnation_ownership_cas(
                &environment_before,
                &environment,
                &acknowledgement.machine_id,
            )?;
            if let Some(machine) = machine_after.as_ref() {
                store.persist_machine_docker_context_ownership(&environment_before, machine)?;
            }
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

    /// Read a validated lifecycle journal before performing admission effects.
    ///
    /// Journals survive Environment deletion, so callers must check this before
    /// reserving a replacement by name. This lookup does not authorize replay:
    /// compare the complete request identity and owning Project/Environment, then
    /// fence any resumed effects under the Environment controller lock.
    pub fn load_environment_lifecycle_by_idempotency_key(
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

    /// A complete backend token is owned by one logical Machine, including
    /// while that Machine is stopped. Check inside the acknowledgement's
    /// immediate transaction, before any persisted state is changed.
    fn require_exclusive_machine_runtime_identity(
        &self,
        machine: &MachineInstance,
    ) -> Result<(), StackError> {
        let Some(identity) = &machine.runtime_identity else {
            return Ok(());
        };
        let mut statement = self.conn.prepare(
            "SELECT instance_json FROM machine_instances
             WHERE machine_id != ?1
               AND json_extract(instance_json, '$.runtime_identity.opaque_id') = ?2",
        )?;
        let candidates = statement.query_map(
            params![machine.machine_id.as_str(), identity.opaque_id],
            |row| row.get::<_, String>(0),
        )?;
        for candidate in candidates {
            let candidate: MachineInstance = serde_json::from_str(&candidate?)?;
            if candidate.backend == machine.backend
                && candidate.runtime_identity.as_ref() == Some(identity)
            {
                return Err(StackError::InvalidSpec(format!(
                    "backend runtime identity is already owned by Machine `{}`",
                    candidate.machine_id
                )));
            }
        }
        Ok(())
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
            || step.resulting_activation != acknowledgement.resulting_activation
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

    /// Insert context ownership within the same transaction as its activation.
    /// Historical descriptor-less context rows are neither adopted nor removed.
    fn persist_machine_docker_context_ownership(
        &self,
        before: &EnvironmentInstance,
        machine: &MachineInstance,
    ) -> Result<(), StackError> {
        let Some(context) = &machine.docker_context else {
            return Ok(());
        };
        let requested = OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::DockerContext,
            resource_id: context.name.clone(),
            environment_id: machine.environment_id.clone(),
            machine_id: Some(machine.machine_id.clone()),
        };
        if before.ownership.contains(&requested) {
            self.require_exact_owned_resource_row(&requested)?;
            return Ok(());
        }
        let kind = serde_json::to_string(&requested.resource_kind)?;
        let collision: Option<(String, Option<String>)> = self.conn.query_row(
            "SELECT environment_id, machine_id FROM topology_ownership WHERE resource_kind = ?1 AND resource_id = ?2",
            params![kind, requested.resource_id], |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;
        if let Some((existing_environment_id, existing_machine_id)) = collision {
            return Err(StackError::OwnedResourceCollision(Box::new(
                OwnedResourceCollisionError {
                    resource_kind: kind,
                    resource_id: requested.resource_id,
                    existing_environment_id,
                    existing_machine_id,
                },
            )));
        }
        self.conn.execute(
            "INSERT INTO topology_ownership (resource_kind, resource_id, environment_id, machine_id, schema_version, record_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![kind,requested.resource_id,requested.environment_id.as_str(),machine.machine_id.as_str(),requested.schema_version,serde_json::to_string(&requested)?],
        )?;
        Ok(())
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

    pub(super) fn migrate_replica_v4_to_v5(&self) -> Result<(), StackError> {
        self.migrate_replica_v4_to_v5_with_hook(|_| Ok(()))
    }

    pub(super) fn migrate_reconcile_v5_to_v6(&self) -> Result<(), StackError> {
        self.migrate_reconcile_v5_to_v6_with_hook(|_| Ok(()))
    }

    pub(super) fn migrate_claim_v6_to_v7(&self) -> Result<(), StackError> {
        self.migrate_claim_v6_to_v7_with_hook(|_| Ok(()))
    }

    pub(super) fn migrate_teardown_finalizer_v7_to_v8(&self) -> Result<(), StackError> {
        self.migrate_teardown_finalizer_v7_to_v8_with_hook(|_| Ok(()))
    }

    fn migrate_teardown_finalizer_v7_to_v8_with_hook(
        &self,
        mut hook: impl FnMut(TeardownFinalizerV8MigrationStage) -> Result<(), StackError>,
    ) -> Result<(), StackError> {
        self.with_immediate_transaction(|store| {
            let schema_version = store.schema_version()?;
            if schema_version != 7 {
                return Err(StackError::InvalidSpec(format!(
                    "teardown-finalizer migration requires state schema version 7, found {schema_version}"
                )));
            }
            store.validate_v7_schema()?;
            store.validate_v7_teardown_finalizer_migration()?;
            store.create_teardown_finalizer_schema_v8()?;
            hook(TeardownFinalizerV8MigrationStage::FinalizerSchemaCreated)?;
            store.validate_v8_schema()?;
            store.set_schema_version(8)?;
            Ok(())
        })
    }

    pub(super) fn migrate_teardown_runtime_identity_v8_to_v9(&self) -> Result<(), StackError> {
        self.with_immediate_transaction(|store| {
            let schema_version = store.schema_version()?;
            if schema_version != 8 {
                return Err(StackError::InvalidSpec(format!(
                    "teardown-runtime-identity migration requires state schema version 8, found {schema_version}"
                )));
            }
            store.validate_v8_schema()?;
            let prepared: Option<String> = store
                .conn
                .query_row(
                    "SELECT operation_key FROM teardown_finalizers WHERE status = 'prepared' ORDER BY operation_key LIMIT 1",
                    [],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(operation_key) = prepared {
                return Err(StackError::InvalidSpec(format!(
                    "v8 prepared teardown finalizer `{operation_key}` lacks exact runtime identity evidence; explicit recovery is required before v9 migration"
                )));
            }
            store.create_teardown_runtime_identity_schema_v9()?;
            store.validate_v9_schema()?;
            store.set_schema_version(STORE_SCHEMA_VERSION)?;
            Ok(())
        })
    }

    fn validate_v7_teardown_finalizer_migration(&self) -> Result<(), StackError> {
        let mut statement = self.conn.prepare(
            "SELECT session_id, operation_id, status
             FROM reconcile_sessions
             WHERE operation_id LIKE ?1 AND status = 'active'
             ORDER BY session_id",
        )?;
        let pattern = format!("{}%", super::CLAIMED_TEARDOWN_OPERATION_PREFIX);
        let mut rows = statement.query_map(params![pattern], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        if let Some(row) = rows.next() {
            let (session_id, _operation_id, status) = row?;
            return Err(StackError::InvalidSpec(format!(
                "v7 teardown session `{session_id}` is {status} without reconstructable finalizer evidence; explicit recovery is required before v8 migration"
            )));
        }
        Ok(())
    }

    fn migrate_claim_v6_to_v7_with_hook(
        &self,
        mut hook: impl FnMut(ClaimV7MigrationStage) -> Result<(), StackError>,
    ) -> Result<(), StackError> {
        self.with_immediate_transaction(|store| {
            let schema_version = store.schema_version()?;
            if schema_version != 6 {
                return Err(StackError::InvalidSpec(format!(
                    "started-claim migration requires state schema version 6, found {schema_version}"
                )));
            }
            store.validate_v6_schema()?;
            store.validate_v6_reconcile_claim_migration()?;
            store.create_claim_schema_v7()?;
            hook(ClaimV7MigrationStage::ImmutabilityGuardsCreated)?;
            store.validate_v7_schema()?;
            store.set_schema_version(CLAIM_SCHEMA_VERSION)?;
            Ok(())
        })
    }

    fn migrate_reconcile_v5_to_v6_with_hook(
        &self,
        mut hook: impl FnMut(ReconcileV6MigrationStage) -> Result<(), StackError>,
    ) -> Result<(), StackError> {
        self.with_immediate_transaction(|store| {
            let schema_version = store.schema_version()?;
            if schema_version != REPLICA_SCHEMA_VERSION {
                return Err(StackError::InvalidSpec(format!(
                    "reconcile action migration requires state schema version 5, found {schema_version}"
                )));
            }
            store.validate_v5_schema()?;

            let mut progress_statement = store.conn.prepare(
                "SELECT stack_name, action_schema_version, actions_json, next_action_index
                 FROM reconcile_progress ORDER BY id",
            )?;
            let progress_rows = progress_statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?;
            for row in progress_rows {
                let (stack_name, action_schema_version, actions_json, cursor_raw) = row?;
                if action_schema_version != 2 {
                    return Err(StackError::InvalidSpec(format!(
                        "reconcile progress for `{stack_name}` uses unexpected action schema {action_schema_version}"
                    )));
                }
                let action_count = serde_json::from_str::<serde_json::Value>(&actions_json)
                    .ok()
                    .and_then(|value| value.as_array().map(Vec::len))
                    .ok_or_else(|| {
                        StackError::InvalidSpec(format!(
                            "action schema v2 reconcile progress for `{stack_name}` is not an action array"
                        ))
                    })?;
                let cursor = usize::try_from(cursor_raw).map_err(|_| {
                    StackError::InvalidSpec(format!(
                        "action schema v2 reconcile progress for `{stack_name}` has invalid cursor {cursor_raw}"
                    ))
                })?;
                if cursor > action_count {
                    return Err(StackError::InvalidSpec(format!(
                        "action schema v2 reconcile progress for `{stack_name}` has cursor {cursor} beyond {action_count} actions"
                    )));
                }
                if cursor < action_count {
                    return Err(StackError::InvalidSpec(format!(
                        "state schema v5 contains pending action schema v2 reconcile progress for `{stack_name}`; explicit recovery or quarantine is required before reconcile action migration"
                    )));
                }
            }
            drop(progress_statement);

            let mut session_statement = store.conn.prepare(
                "SELECT session_id, status, action_schema_version, actions_json,
                        next_action_index, total_actions, started_at, updated_at, completed_at
                 FROM reconcile_sessions ORDER BY session_id",
            )?;
            let session_rows = session_statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                ))
            })?;
            for row in session_rows {
                let (id, status, action_schema_version, actions_json, cursor, total, started, updated, completed) = row?;
                if action_schema_version != 2 {
                    return Err(StackError::InvalidSpec(format!(
                        "reconcile session `{id}` uses unexpected action schema {action_schema_version}"
                    )));
                }
                if status == "active" {
                    return Err(StackError::InvalidSpec(format!(
                        "state schema v5 contains active action schema v2 reconcile session `{id}`; explicit recovery or quarantine is required before reconcile action migration"
                    )));
                }
                let action_count = serde_json::from_str::<serde_json::Value>(&actions_json)
                    .ok()
                    .and_then(|value| value.as_array().map(Vec::len))
                    .ok_or_else(|| {
                        StackError::InvalidSpec(format!(
                            "action schema v2 reconcile session `{id}` is not an action array"
                        ))
                    })?;
                let valid = matches!(status.as_str(), "completed" | "failed" | "superseded")
                    && cursor >= 0
                    && total >= 0
                    && usize::try_from(total).ok() == Some(action_count)
                    && cursor <= total
                    && (status != "completed" || cursor == total)
                    && started >= 0
                    && updated >= started
                    && completed.is_some_and(|value| value >= updated);
                if !valid {
                    return Err(StackError::InvalidSpec(format!(
                        "action schema v2 reconcile session `{id}` has inconsistent terminal metadata"
                    )));
                }
            }
            drop(session_statement);

            let mut audit_statement = store.conn.prepare(
                "SELECT id, status, action_index, action_kind, service_name,
                        replica_index, started_at, completed_at, error_message
                 FROM reconcile_audit_log ORDER BY id",
            )?;
            let audit_rows = audit_statement.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                ))
            })?;
            for row in audit_rows {
                let (id, status, index, kind, service, replica, started, completed, error) = row?;
                if status == "started" {
                    return Err(StackError::InvalidSpec(format!(
                        "state schema v5 contains started action schema v2 reconcile audit row {id}; explicit recovery or quarantine is required before reconcile action migration"
                    )));
                }
                let valid = index >= 0
                    && matches!(kind.as_str(), "service_create" | "service_recreate" | "service_remove")
                    && !service.trim().is_empty()
                    && replica > 0
                    && started >= 0
                    && completed.is_some_and(|value| value >= started)
                    && ((status == "completed" && error.is_none())
                        || (status == "failed" && error.is_some()));
                if !valid {
                    return Err(StackError::InvalidSpec(format!(
                        "action schema v2 reconcile audit row {id} has inconsistent terminal metadata"
                    )));
                }
            }
            drop(audit_statement);

            store.conn.execute_batch(RECONCILE_SCHEMA_V6_ARCHIVE_DDL)?;
            hook(ReconcileV6MigrationStage::TerminalHistoryArchived)?;
            store
                .conn
                .execute_batch(RECONCILE_SCHEMA_V6_ACTION_TABLES_DDL)?;
            hook(ReconcileV6MigrationStage::DurableActionsRebuilt)?;
            store
                .conn
                .execute_batch(RECONCILE_SCHEMA_V6_CLAIM_INDEX_DDL)?;
            hook(ReconcileV6MigrationStage::ReplicaClaimIndexCreated)?;
            store.validate_v6_schema()?;
            store.set_schema_version(6)?;
            Ok(())
        })
    }

    fn migrate_replica_v4_to_v5_with_hook(
        &self,
        mut hook: impl FnMut(ReplicaV5MigrationStage) -> Result<(), StackError>,
    ) -> Result<(), StackError> {
        self.with_immediate_transaction(|store| {
            let schema_version = store.schema_version()?;
            if schema_version != STACK_JOURNAL_SCHEMA_VERSION {
                return Err(StackError::InvalidSpec(format!(
                    "replica identity migration requires state schema version 4, found {schema_version}"
                )));
            }
            store.validate_v4_schema()?;

            let mut progress_statement = store.conn.prepare(
                "SELECT stack_name, actions_json, next_action_index FROM reconcile_progress",
            )?;
            let progress_rows = progress_statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
            for row in progress_rows {
                let (stack_name, actions_json, cursor_raw) = row?;
                let actions = serde_json::from_str::<serde_json::Value>(&actions_json)
                    .map_err(|error| {
                        StackError::InvalidSpec(format!(
                            "legacy reconcile progress for `{stack_name}` is malformed: {error}"
                        ))
                    })?;
                let action_count = actions.as_array().map(Vec::len).ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "legacy reconcile progress for `{stack_name}` is not an action array"
                    ))
                })?;
                let cursor = usize::try_from(cursor_raw).map_err(|_| {
                    StackError::InvalidSpec(format!(
                        "legacy reconcile progress for `{stack_name}` has invalid cursor {cursor_raw}"
                    ))
                })?;
                if cursor > action_count {
                    return Err(StackError::InvalidSpec(format!(
                        "legacy reconcile progress for `{stack_name}` has cursor {cursor} beyond {action_count} actions"
                    )));
                }
                if cursor < action_count {
                    return Err(StackError::InvalidSpec(format!(
                        "state schema v4 contains pending aggregate reconcile progress for `{stack_name}`; explicit recovery or quarantine is required before replica identity migration"
                    )));
                }
            }
            drop(progress_statement);
            let mut session_statement = store.conn.prepare(
                "SELECT session_id, status, actions_json, next_action_index, total_actions,
                        started_at, updated_at, completed_at
                 FROM reconcile_sessions",
            )?;
            let session_rows = session_statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?, row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?, row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?, row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?, row.get::<_, Option<i64>>(7)?,
                ))
            })?;
            for row in session_rows {
                let (id, status, actions_json, cursor, total, started, updated, completed) = row?;
                if !matches!(status.as_str(), "active" | "completed" | "failed" | "superseded") {
                    return Err(StackError::InvalidSpec(format!(
                        "legacy reconcile session `{id}` has invalid status `{status}`"
                    )));
                }
                let action_count = serde_json::from_str::<serde_json::Value>(&actions_json)
                    .ok()
                    .and_then(|value| value.as_array().map(Vec::len))
                    .ok_or_else(|| StackError::InvalidSpec(format!(
                        "legacy reconcile session `{id}` has malformed actions"
                    )))?;
                let valid_numbers = cursor >= 0
                    && total >= 0
                    && usize::try_from(total).ok() == Some(action_count)
                    && cursor <= total
                    && started >= 0
                    && updated >= started
                    && (status != "completed" || cursor == total);
                let valid_completion = if status == "active" {
                    completed.is_none()
                } else {
                    completed.is_some_and(|value| value >= updated)
                };
                if !valid_numbers || !valid_completion {
                    return Err(StackError::InvalidSpec(format!(
                        "legacy reconcile session `{id}` has inconsistent metadata"
                    )));
                }
                if status == "active" {
                    return Err(StackError::InvalidSpec(format!(
                        "state schema v4 contains active aggregate reconcile session `{id}`; explicit recovery or quarantine is required before replica identity migration"
                    )));
                }
            }
            drop(session_statement);

            let mut audit_statement = store.conn.prepare(
                "SELECT id, status, action_index, action_kind, service_name,
                        started_at, completed_at, error_message
                 FROM reconcile_audit_log",
            )?;
            let audit_rows = audit_statement.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?, row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?, row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?, row.get::<_, i64>(5)?,
                    row.get::<_, Option<i64>>(6)?, row.get::<_, Option<String>>(7)?,
                ))
            })?;
            for row in audit_rows {
                let (id, status, index, kind, service, started, completed, error) = row?;
                if status == "started" {
                    return Err(StackError::InvalidSpec(format!(
                        "legacy reconcile audit row {id} is an in-flight aggregate action"
                    )));
                }
                let valid = index >= 0
                    && matches!(
                        kind.as_str(),
                        "service_create" | "service_recreate" | "service_remove"
                    )
                    && !service.trim().is_empty()
                    && started >= 0
                    && completed.is_some_and(|value| value >= started)
                    && ((status == "completed" && error.is_none())
                        || (status == "failed" && error.is_some()));
                if !valid {
                    return Err(StackError::InvalidSpec(format!(
                        "legacy reconcile audit row {id} has inconsistent metadata"
                    )));
                }
            }
            drop(audit_statement);

            store.create_replica_schema_v5_with_hook(|| {
                hook(ReplicaV5MigrationStage::DurableActionsRebuilt)
            })?;
            hook(ReplicaV5MigrationStage::ObservedStateRebuilt)?;
            store.validate_v5_schema()?;
            store.set_schema_version(REPLICA_SCHEMA_VERSION)?;
            Ok(())
        })
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
            store.set_schema_version(STACK_JOURNAL_SCHEMA_VERSION)?;
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

    #[cfg(test)]
    pub(super) fn migrate_replica_v4_to_v5_with_failpoint(
        &self,
        failpoint: ReplicaV5MigrationFailpoint,
    ) -> Result<(), StackError> {
        self.migrate_replica_v4_to_v5_with_hook(|stage| {
            if matches!(
                (failpoint, stage),
                (
                    ReplicaV5MigrationFailpoint::AfterDurableActionsRebuilt,
                    ReplicaV5MigrationStage::DurableActionsRebuilt
                ) | (
                    ReplicaV5MigrationFailpoint::AfterObservedStateRebuilt,
                    ReplicaV5MigrationStage::ObservedStateRebuilt
                )
            ) {
                return Err(StackError::InvalidSpec(format!(
                    "injected v4-to-v5 migration failure at {stage:?}"
                )));
            }
            Ok(())
        })
    }

    #[cfg(test)]
    pub(super) fn migrate_reconcile_v5_to_v6_with_failpoint(
        &self,
        failpoint: ReconcileV6MigrationFailpoint,
    ) -> Result<(), StackError> {
        self.migrate_reconcile_v5_to_v6_with_hook(|stage| {
            if matches!(
                (failpoint, stage),
                (
                    ReconcileV6MigrationFailpoint::AfterTerminalHistoryArchived,
                    ReconcileV6MigrationStage::TerminalHistoryArchived
                ) | (
                    ReconcileV6MigrationFailpoint::AfterDurableActionsRebuilt,
                    ReconcileV6MigrationStage::DurableActionsRebuilt
                ) | (
                    ReconcileV6MigrationFailpoint::AfterReplicaClaimIndexCreated,
                    ReconcileV6MigrationStage::ReplicaClaimIndexCreated
                )
            ) {
                return Err(StackError::InvalidSpec(format!(
                    "injected v5-to-v6 migration failure at {stage:?}"
                )));
            }
            Ok(())
        })
    }

    #[cfg(test)]
    pub(super) fn migrate_claim_v6_to_v7_with_failpoint(
        &self,
        failpoint: ClaimV7MigrationFailpoint,
    ) -> Result<(), StackError> {
        self.migrate_claim_v6_to_v7_with_hook(|stage| {
            if matches!(
                (failpoint, stage),
                (
                    ClaimV7MigrationFailpoint::AfterImmutabilityGuardsCreated,
                    ClaimV7MigrationStage::ImmutabilityGuardsCreated
                )
            ) {
                return Err(StackError::InvalidSpec(
                    "injected v6-to-v7 migration failure after immutability guards".to_string(),
                ));
            }
            Ok(())
        })
    }

    #[cfg(test)]
    pub(super) fn migrate_teardown_finalizer_v7_to_v8_with_failpoint(
        &self,
        failpoint: TeardownFinalizerV8MigrationFailpoint,
    ) -> Result<(), StackError> {
        self.migrate_teardown_finalizer_v7_to_v8_with_hook(|stage| {
            if matches!(
                (failpoint, stage),
                (
                    TeardownFinalizerV8MigrationFailpoint::AfterFinalizerSchemaCreated,
                    TeardownFinalizerV8MigrationStage::FinalizerSchemaCreated
                )
            ) {
                return Err(StackError::InvalidSpec(
                    "injected v7-to-v8 migration failure after finalizer schema creation"
                        .to_string(),
                ));
            }
            Ok(())
        })
    }
}

impl StateStore {
    /// Revalidate recovery of a first-generation Delete which can have no prior
    /// VM dispatch: the sole lifecycle journal is this exact Delete, and every
    /// Machine began Creating without an incarnation. Hold the controller fence.
    pub fn require_never_started_delete_fence(
        &self,
        expected: &EnvironmentInstance,
        operation: &EnvironmentLifecycleOperation,
    ) -> Result<(), StackError> {
        let conflict = || StackError::Machine {
            code: vz_runtime_contract::MachineErrorCode::StateConflict,
            message: "Delete lacks exact never-started admission authority".into(),
        };
        let transaction = self.conn.unchecked_transaction()?;
        let actual = self
            .load_project_state(expected.project_id.as_str())?
            .ok_or_else(conflict)?
            .environments
            .into_iter()
            .find(|e| e.environment_id == expected.environment_id)
            .ok_or_else(conflict)?;
        if actual != *expected
            || expected.legacy_migration.is_some()
            || operation.kind != EnvironmentLifecycleKind::Delete
            || operation.generation != 1
            || expected.lifecycle_generation != 1
            || expected.active_operation_id.as_ref() != Some(&operation.operation_id)
            || self
                .load_environment_lifecycle(operation.operation_id.as_str())?
                .as_ref()
                != Some(operation)
            || operation.machine_steps.iter().any(|s| {
                s.initial_state != MachineState::Creating || s.expected_incarnation.is_some()
            })
            || expected.machines.iter().any(|m| {
                m.backend.is_some()
                    || m.incarnation.is_some()
                    || m.runtime_identity.is_some()
                    || m.legacy_sandbox_id.is_some()
            })
        {
            return Err(conflict());
        }
        operation
            .validate_against_environment(expected)
            .map_err(|_| conflict())?;
        let count: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM environment_lifecycle_operations WHERE environment_id = ?1",
            params![expected.environment_id.as_str()],
            |r| r.get(0),
        )?;
        if count != 1 {
            return Err(conflict());
        }
        transaction.commit()?;
        Ok(())
    }
}
