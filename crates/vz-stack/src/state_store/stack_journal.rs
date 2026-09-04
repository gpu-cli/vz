use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use vz_runtime_contract::{
    ContainerGenerationOwnership, ContainerGenerationScope, EnvironmentId,
    MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION, MachineErrorCode, MachineId, MachineWorkloadScope,
    ProjectId,
};

use super::{ServiceObservedState, ServicePhase, ServiceReplicaKey, StateStore};
use crate::StackError;
use crate::reconcile::{Action, ActionDraft, ExpectedJournalHead, ReplicaPrecondition};

pub(super) const STACK_JOURNAL_SCHEMA_V4_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS stack_workload_owners (
    stack_id TEXT PRIMARY KEY CHECK(length(trim(stack_id)) BETWEEN 1 AND 128),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    project_id TEXT NOT NULL CHECK(length(trim(project_id)) BETWEEN 1 AND 128),
    environment_id TEXT NOT NULL CHECK(length(trim(environment_id)) BETWEEN 1 AND 128),
    machine_id TEXT NOT NULL CHECK(length(trim(machine_id)) BETWEEN 1 AND 128),
    owner_json TEXT NOT NULL CHECK(json_valid(owner_json)),
    created_at INTEGER NOT NULL CHECK(created_at >= 0),
    UNIQUE(stack_id, project_id, environment_id, machine_id)
);
CREATE INDEX IF NOT EXISTS idx_stack_workload_owner_machine
    ON stack_workload_owners(project_id, environment_id, machine_id, stack_id);
CREATE TRIGGER IF NOT EXISTS stack_workload_owner_immutable
BEFORE UPDATE ON stack_workload_owners
BEGIN
    SELECT RAISE(ABORT, 'stack workload ownership is immutable');
END;
CREATE TRIGGER IF NOT EXISTS stack_workload_owner_delete_restricted
BEFORE DELETE ON stack_workload_owners
BEGIN
    SELECT RAISE(ABORT, 'stack workload ownership is immutable');
END;

CREATE TABLE IF NOT EXISTS stack_container_create_intents (
    reservation_id TEXT PRIMARY KEY
        CHECK(length(trim(reservation_id)) BETWEEN 1 AND 128),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    project_id TEXT NOT NULL CHECK(length(trim(project_id)) BETWEEN 1 AND 128),
    environment_id TEXT NOT NULL CHECK(length(trim(environment_id)) BETWEEN 1 AND 128),
    machine_id TEXT NOT NULL CHECK(length(trim(machine_id)) BETWEEN 1 AND 128),
    machine_incarnation_id TEXT NOT NULL
        CHECK(length(trim(machine_incarnation_id)) BETWEEN 1 AND 128),
    environment_generation INTEGER NOT NULL CHECK(environment_generation >= 0),
    stack_id TEXT NOT NULL CHECK(length(trim(stack_id)) BETWEEN 1 AND 128),
    service_name TEXT NOT NULL CHECK(length(trim(service_name)) BETWEEN 1 AND 128),
    replica_index INTEGER NOT NULL CHECK(replica_index > 0),
    service_generation INTEGER NOT NULL CHECK(service_generation > 0),
    requested_container_id TEXT NOT NULL
        CHECK(length(trim(requested_container_id)) BETWEEN 1 AND 128),
    definition_digest TEXT NOT NULL CHECK(length(trim(definition_digest)) > 0),
    action_digest TEXT NOT NULL CHECK(length(trim(action_digest)) > 0),
    status TEXT NOT NULL CHECK(status IN (
        'intent', 'reserved', 'running', 'cleanup_pending', 'blocked', 'cleaned', 'failed'
    )),
    intent_json TEXT NOT NULL CHECK(json_valid(intent_json)),
    last_error TEXT,
    created_at INTEGER NOT NULL CHECK(created_at >= 0),
    updated_at INTEGER NOT NULL CHECK(updated_at >= created_at),
    completed_at INTEGER,
    UNIQUE(
        machine_incarnation_id, stack_id, service_name, replica_index, service_generation
    ),
    CHECK(completed_at IS NULL OR completed_at >= updated_at),
    CHECK(
        (status IN ('cleaned', 'failed') AND completed_at IS NOT NULL) OR
        (status NOT IN ('cleaned', 'failed') AND completed_at IS NULL)
    )
);
DROP INDEX IF EXISTS idx_stack_create_one_active_service;
CREATE UNIQUE INDEX idx_stack_create_one_active_service
    ON stack_container_create_intents(
        project_id, environment_id, machine_id, stack_id, service_name, replica_index
    )
    WHERE status IN ('intent', 'reserved', 'running', 'cleanup_pending', 'blocked');
CREATE INDEX IF NOT EXISTS idx_stack_create_resumable
    ON stack_container_create_intents(status, updated_at, reservation_id);
CREATE INDEX IF NOT EXISTS idx_stack_create_environment
    ON stack_container_create_intents(
        project_id, environment_id, machine_id, machine_incarnation_id, stack_id
    );

CREATE TRIGGER IF NOT EXISTS stack_container_create_owner_guard
BEFORE INSERT ON stack_container_create_intents
WHEN NOT EXISTS (
    SELECT 1 FROM stack_workload_owners owner
    WHERE owner.stack_id = NEW.stack_id
      AND owner.project_id = NEW.project_id
      AND owner.environment_id = NEW.environment_id
      AND owner.machine_id = NEW.machine_id
)
BEGIN
    SELECT RAISE(ABORT, 'stack container create has no exact stable workload owner');
END;

DROP TRIGGER IF EXISTS stack_container_create_stack_scope_guard;
CREATE TRIGGER stack_container_create_stack_scope_guard
BEFORE INSERT ON stack_container_create_intents
WHEN EXISTS (
    SELECT 1 FROM stack_container_create_intents existing
    WHERE existing.stack_id = NEW.stack_id
      AND (
        existing.project_id <> NEW.project_id OR
        existing.environment_id <> NEW.environment_id OR
        existing.machine_id <> NEW.machine_id
      )
)
BEGIN
    SELECT RAISE(ABORT, 'stack container create stack_id belongs to another Machine scope');
END;

CREATE TRIGGER IF NOT EXISTS stack_container_create_intent_immutable
BEFORE UPDATE OF
    reservation_id, schema_version, project_id, environment_id, machine_id,
    machine_incarnation_id, environment_generation, stack_id, service_name,
    replica_index, service_generation, requested_container_id, definition_digest,
    action_digest, created_at
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
    NEW.created_at <> OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'stack container create intent projections are immutable');
END;

CREATE TABLE IF NOT EXISTS stack_container_generation_bindings (
    reservation_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL CHECK(length(trim(project_id)) BETWEEN 1 AND 128),
    environment_id TEXT NOT NULL CHECK(length(trim(environment_id)) BETWEEN 1 AND 128),
    machine_id TEXT NOT NULL CHECK(length(trim(machine_id)) BETWEEN 1 AND 128),
    machine_incarnation_id TEXT NOT NULL
        CHECK(length(trim(machine_incarnation_id)) BETWEEN 1 AND 128),
    stack_id TEXT NOT NULL CHECK(length(trim(stack_id)) BETWEEN 1 AND 128),
    service_name TEXT NOT NULL CHECK(length(trim(service_name)) BETWEEN 1 AND 128),
    requested_container_id TEXT NOT NULL
        CHECK(length(trim(requested_container_id)) BETWEEN 1 AND 128),
    runtime_generation INTEGER NOT NULL CHECK(runtime_generation > 0),
    ownership_json TEXT NOT NULL CHECK(json_valid(ownership_json)),
    bound_at INTEGER NOT NULL CHECK(bound_at >= 0),
    UNIQUE(machine_incarnation_id, requested_container_id, runtime_generation),
    FOREIGN KEY(reservation_id)
        REFERENCES stack_container_create_intents(reservation_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS idx_stack_generation_binding_scope
    ON stack_container_generation_bindings(
        project_id, environment_id, machine_id, machine_incarnation_id, stack_id
    );

CREATE TRIGGER IF NOT EXISTS stack_container_generation_binding_immutable
BEFORE UPDATE ON stack_container_generation_bindings
BEGIN
    SELECT RAISE(ABORT, 'stack container generation bindings are immutable');
END;
CREATE TRIGGER IF NOT EXISTS stack_container_generation_binding_delete_restricted
BEFORE DELETE ON stack_container_generation_bindings
BEGIN
    SELECT RAISE(ABORT, 'stack container generation bindings are immutable');
END;
"#;

const OBSERVED_STATE_V4_DDL: &str = r#"
ALTER TABLE observed_state RENAME TO observed_state_v3;
CREATE TABLE observed_state (
    id INTEGER PRIMARY KEY,
    stack_name TEXT NOT NULL,
    service_name TEXT NOT NULL,
    replica_index INTEGER NOT NULL DEFAULT 0 CHECK(replica_index >= 0),
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(stack_name, service_name, replica_index)
);
INSERT INTO observed_state
    (id, stack_name, service_name, replica_index, state_json, updated_at)
SELECT id, stack_name, service_name, 0, state_json, updated_at
FROM observed_state_v3;
DROP TABLE observed_state_v3;
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StackContainerCreateStatus {
    Intent,
    Reserved,
    Running,
    CleanupPending,
    Blocked,
    Cleaned,
    Failed,
}

/// Immutable stable ownership of the global stack-state namespace.
///
/// Machine incarnation is deliberately absent: replacing a Machine incarnation
/// does not transfer its stack namespace to another Project, Environment, or
/// Machine. Rows are durable namespace tombstones, not live runtime ownership,
/// so they neither authorize activation nor independently fence Environment
/// deletion. Exact runtime cleanup remains fenced by container journal rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackWorkloadOwner {
    pub schema_version: u32,
    pub stack_id: String,
    pub project_id: ProjectId,
    pub environment_id: EnvironmentId,
    pub machine_id: MachineId,
    pub created_at: u64,
}

impl StackWorkloadOwner {
    pub const SCHEMA_VERSION: u32 = 1;

    fn from_scope(scope: &MachineWorkloadScope, created_at: u64) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            stack_id: scope.stack_id.clone(),
            project_id: scope.project_id.clone(),
            environment_id: scope.environment_id.clone(),
            machine_id: scope.machine_id.clone(),
            created_at,
        }
    }

    fn validate(&self) -> Result<(), StackError> {
        if self.schema_version != Self::SCHEMA_VERSION {
            return invalid(format!(
                "stack workload owner schema version must be {}",
                Self::SCHEMA_VERSION
            ));
        }
        validate_text("stack_id", &self.stack_id)?;
        validate_text("project_id", self.project_id.as_str())?;
        validate_text("environment_id", self.environment_id.as_str())?;
        validate_text("machine_id", self.machine_id.as_str())?;
        Ok(())
    }

    fn matches_scope(&self, scope: &MachineWorkloadScope) -> bool {
        self.stack_id == scope.stack_id
            && self.project_id == scope.project_id
            && self.environment_id == scope.environment_id
            && self.machine_id == scope.machine_id
    }
}

impl StackContainerCreateStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Intent => "intent",
            Self::Reserved => "reserved",
            Self::Running => "running",
            Self::CleanupPending => "cleanup_pending",
            Self::Blocked => "blocked",
            Self::Cleaned => "cleaned",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self, StackError> {
        match value {
            "intent" => Ok(Self::Intent),
            "reserved" => Ok(Self::Reserved),
            "running" => Ok(Self::Running),
            "cleanup_pending" => Ok(Self::CleanupPending),
            "blocked" => Ok(Self::Blocked),
            "cleaned" => Ok(Self::Cleaned),
            "failed" => Ok(Self::Failed),
            other => Err(StackError::InvalidSpec(format!(
                "unknown stack container create status `{other}`"
            ))),
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Cleaned | Self::Failed)
    }

    pub fn is_resumable(self) -> bool {
        !self.is_terminal()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackContainerCreateIntent {
    pub schema_version: u32,
    pub scope: ContainerGenerationScope,
    pub environment_generation: u64,
    pub service_name: String,
    pub replica_index: u32,
    pub service_generation: u64,
    pub requested_container_id: String,
    pub definition_digest: String,
    pub action_digest: String,
    /// Full service configuration this generation will apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_config_digest: Option<String>,
    pub status: StackContainerCreateStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u64>,
}

impl StackContainerCreateIntent {
    pub const SCHEMA_VERSION: u32 = 1;

    pub fn validate(&self) -> Result<(), StackError> {
        if self.schema_version != Self::SCHEMA_VERSION {
            return invalid(format!(
                "stack container create intent schema version must be {}",
                Self::SCHEMA_VERSION
            ));
        }
        self.scope.validate().map_err(StackError::InvalidSpec)?;
        if self.scope.machine_incarnation_id.is_none() {
            return invalid("stack container create intent requires a Machine incarnation");
        }
        validate_text("service_name", &self.service_name)?;
        validate_text("requested_container_id", &self.requested_container_id)?;
        validate_digest("definition_digest", &self.definition_digest)?;
        validate_digest("action_digest", &self.action_digest)?;
        if let Some(digest) = &self.applied_config_digest {
            validate_digest("applied_config_digest", digest)?;
        }
        if self.replica_index == 0 || self.service_generation == 0 {
            return invalid("replica_index and service_generation must be non-zero");
        }
        if self.updated_at < self.created_at {
            return invalid("stack container create intent updated_at precedes created_at");
        }
        if self.status.is_terminal() != self.completed_at.is_some() {
            return invalid("stack container create terminal status/completed_at mismatch");
        }
        if self
            .completed_at
            .is_some_and(|value| value < self.updated_at)
        {
            return invalid("stack container create completed_at precedes updated_at");
        }
        Ok(())
    }

    fn same_immutable_identity(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.scope == other.scope
            && self.environment_generation == other.environment_generation
            && self.service_name == other.service_name
            && self.replica_index == other.replica_index
            && self.service_generation == other.service_generation
            && self.requested_container_id == other.requested_container_id
            && self.definition_digest == other.definition_digest
            && self.action_digest == other.action_digest
            && self.applied_config_digest == other.applied_config_digest
            && self.created_at == other.created_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackContainerGenerationBinding {
    pub reservation_id: String,
    pub service_name: String,
    pub ownership: ContainerGenerationOwnership,
    pub bound_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StackContainerRecoveryDisposition {
    Activatable,
    CleanupOnly { stale_reason: String },
    Abandonable { stale_reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackContainerRecoveryRecord {
    pub intent: StackContainerCreateIntent,
    pub binding: Option<StackContainerGenerationBinding>,
    pub disposition: StackContainerRecoveryDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackContainerCreateSelector {
    pub project_id: vz_runtime_contract::ProjectId,
    pub environment_id: vz_runtime_contract::EnvironmentId,
    pub machine_id: vz_runtime_contract::MachineId,
    pub machine_incarnation_id: vz_runtime_contract::MachineIncarnationId,
    pub environment_generation: u64,
    pub stack_id: String,
    pub service_name: String,
    pub replica_index: u32,
    pub requested_container_id: String,
    pub definition_digest: String,
    pub action_digest: String,
    pub applied_config_digest: String,
}

impl StackContainerCreateSelector {
    fn to_intent(&self, service_generation: u64, now: u64) -> StackContainerCreateIntent {
        let reservation_id = deterministic_reservation_id(self, service_generation);
        StackContainerCreateIntent {
            schema_version: StackContainerCreateIntent::SCHEMA_VERSION,
            scope: ContainerGenerationScope {
                reservation_id,
                project_id: self.project_id.clone(),
                environment_id: self.environment_id.clone(),
                machine_id: self.machine_id.clone(),
                machine_incarnation_id: Some(self.machine_incarnation_id.clone()),
                stack_id: self.stack_id.clone(),
            },
            environment_generation: self.environment_generation,
            service_name: self.service_name.clone(),
            replica_index: self.replica_index,
            service_generation,
            requested_container_id: self.requested_container_id.clone(),
            definition_digest: self.definition_digest.clone(),
            action_digest: self.action_digest.clone(),
            applied_config_digest: Some(self.applied_config_digest.clone()),
            status: StackContainerCreateStatus::Intent,
            last_error: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
        }
    }

    fn matches(&self, intent: &StackContainerCreateIntent) -> bool {
        intent.scope.project_id == self.project_id
            && intent.scope.environment_id == self.environment_id
            && intent.scope.machine_id == self.machine_id
            && intent.scope.machine_incarnation_id.as_ref() == Some(&self.machine_incarnation_id)
            && intent.scope.stack_id == self.stack_id
            && intent.environment_generation == self.environment_generation
            && intent.service_name == self.service_name
            && intent.replica_index == self.replica_index
            && intent.requested_container_id == self.requested_container_id
            && intent.definition_digest == self.definition_digest
            && intent.action_digest == self.action_digest
            && intent.applied_config_digest.as_deref() == Some(self.applied_config_digest.as_str())
    }
}

impl StackContainerGenerationBinding {
    pub fn validate(&self) -> Result<(), StackError> {
        validate_text("reservation_id", &self.reservation_id)?;
        validate_text("service_name", &self.service_name)?;
        self.ownership.validate().map_err(StackError::InvalidSpec)?;
        let scope = self.ownership.scope.as_deref().ok_or_else(|| {
            StackError::InvalidSpec(
                "stack container generation binding is legacy-unscoped".to_string(),
            )
        })?;
        if scope.reservation_id != self.reservation_id {
            return invalid("generation binding reservation_id disagrees with ownership scope");
        }
        Ok(())
    }

    fn same_immutable_authority(&self, other: &Self) -> bool {
        self.reservation_id == other.reservation_id
            && self.service_name == other.service_name
            && self.ownership == other.ownership
    }
}

impl StateStore {
    /// Revalidate one persisted Action-v3 fence immediately before its started
    /// claim is made durable.
    ///
    /// Callers must hold the same `BEGIN IMMEDIATE` transaction used to insert
    /// the audit claim. This method is deliberately read-only: a failed fence
    /// never repairs topology, journal, or observed projections.
    pub(super) fn validate_reconcile_action_claim_precondition(
        &self,
        action: &Action,
    ) -> Result<(), StackError> {
        self.validate_reconcile_action_claim_precondition_inner(action)
            .map_err(|error| match error {
                StackError::Machine {
                    code: MachineErrorCode::StateConflict,
                    ..
                } => error,
                other => StackError::Machine {
                    code: MachineErrorCode::StateConflict,
                    message: format!(
                        "reconcile claim precondition for `{}` is malformed or stale: {other}",
                        action.target().display_name()
                    ),
                },
            })
    }

    fn validate_reconcile_action_claim_precondition_inner(
        &self,
        action: &Action,
    ) -> Result<(), StackError> {
        action.validate()?;
        let precondition = action.precondition();
        let workload = precondition.workload();

        self.validate_stack_workload_owner(workload)?;
        self.validate_current_runnable_workload_scope(workload)?;
        let environment = self
            .load_environment_instance(workload.environment_id.as_str())?
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "Environment `{}` was not found",
                    workload.environment_id
                ))
            })?;
        if environment.lifecycle_generation != precondition.environment_generation() {
            return conflict(format!(
                "reconcile claim for `{}` has stale Environment generation",
                action.target().display_name()
            ));
        }

        let mut statement = self.conn.prepare(
            "SELECT reservation_id, service_generation
             FROM stack_container_create_intents
             WHERE stack_id = ?1 AND service_name = ?2 AND replica_index = ?3
             ORDER BY service_generation DESC, reservation_id ASC
             LIMIT 2",
        )?;
        let rows = statement.query_map(
            params![
                workload.stack_id,
                action.target().service_name,
                i64::from(action.target().index()),
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )?;
        let mut heads = Vec::new();
        for row in rows {
            let (reservation_id, service_generation) = row?;
            heads.push((
                reservation_id,
                persisted_u64("service_generation", service_generation)?,
            ));
        }

        match precondition.journal_head() {
            ExpectedJournalHead::NeverJournaled => {
                if !heads.is_empty() {
                    return conflict(format!(
                        "reconcile claim for `{}` expected no journal predecessor",
                        action.target().display_name()
                    ));
                }
                if self
                    .load_service_observed(
                        &workload.stack_id,
                        &action.target().service_name,
                        action.target().index(),
                    )?
                    .is_some()
                {
                    return conflict(format!(
                        "reconcile claim for `{}` found observed state without a journal predecessor",
                        action.target().display_name()
                    ));
                }
            }
            ExpectedJournalHead::Exact {
                reservation_id,
                service_generation,
                ownership,
            } => {
                let Some((latest_reservation, latest_generation)) = heads.first() else {
                    return conflict(format!(
                        "reconcile claim for `{}` is missing exact journal predecessor `{reservation_id}`",
                        action.target().display_name()
                    ));
                };
                if latest_reservation != reservation_id
                    || latest_generation != service_generation
                    || heads
                        .get(1)
                        .is_some_and(|(_, generation)| generation == service_generation)
                {
                    return conflict(format!(
                        "reconcile claim for `{}` no longer names the unique latest journal predecessor",
                        action.target().display_name()
                    ));
                }

                let intent = self.require_stack_container_create_intent(reservation_id)?;
                if intent.scope.project_id != workload.project_id
                    || intent.scope.environment_id != workload.environment_id
                    || intent.scope.machine_id != workload.machine_id
                    || intent.scope.stack_id != workload.stack_id
                    || intent.service_name != action.target().service_name
                    || intent.replica_index != action.target().index()
                    || intent.service_generation != *service_generation
                {
                    return conflict(format!(
                        "reconcile claim for `{}` journal predecessor has foreign identity",
                        action.target().display_name()
                    ));
                }
                self.validate_journal_workload_owner(&intent)?;
                if !intent.status.is_terminal() {
                    self.validate_intent_topology(&intent)?;
                    if intent.scope.machine_incarnation_id.as_ref()
                        != Some(&workload.machine_incarnation_id)
                    {
                        return conflict(format!(
                            "reconcile claim for `{}` has a nonterminal predecessor from a stale Machine incarnation",
                            action.target().display_name()
                        ));
                    }
                }
                let binding = self.load_stack_container_generation_binding(reservation_id)?;
                if let Some(binding) = &binding {
                    self.validate_binding_against_intent(binding, &intent)?;
                }
                if binding.as_ref().map(|binding| &binding.ownership) != ownership.as_ref() {
                    return conflict(format!(
                        "reconcile claim for `{}` runtime generation binding changed",
                        action.target().display_name()
                    ));
                }
                let legal_pre_effect_status =
                    legal_fresh_claim_predecessor(action, intent.status, binding.is_some());
                if !legal_pre_effect_status {
                    return conflict(format!(
                        "reconcile claim for `{}` found journal status `{}` after action effects had already begun",
                        action.target().display_name(),
                        intent.status.as_str()
                    ));
                }
                self.require_journal_observed_consistent(&intent)?;
            }
        }
        Ok(())
    }

    /// Validate replay of an already durable exact claim. Replay may observe
    /// effects owned by that claim, but never arbitrary topology drift or an
    /// unrelated successor journal generation.
    pub(super) fn validate_reconcile_action_claim_replay(
        &self,
        session_id: &str,
        operation_id: &str,
        absolute_action_index: usize,
        action: &Action,
    ) -> Result<(), StackError> {
        if self
            .validate_reconcile_action_claim_precondition_inner(action)
            .is_ok()
        {
            return Ok(());
        }
        self.validate_reconcile_action_claim_replay_inner(
            session_id,
            operation_id,
            absolute_action_index,
            action,
        )
        .map_err(|error| match error {
            StackError::Machine {
                code: MachineErrorCode::StateConflict,
                ..
            } => error,
            other => StackError::Machine {
                code: MachineErrorCode::StateConflict,
                message: format!(
                    "reconcile claim replay for `{}` is malformed or stale: {other}",
                    action.target().display_name()
                ),
            },
        })
    }

    fn validate_reconcile_action_claim_replay_inner(
        &self,
        session_id: &str,
        operation_id: &str,
        absolute_action_index: usize,
        action: &Action,
    ) -> Result<(), StackError> {
        action.validate()?;
        let precondition = action.precondition();
        let workload = precondition.workload();
        self.validate_stack_workload_owner(workload)?;
        self.validate_current_runnable_workload_scope(workload)?;
        let environment = self
            .load_environment_instance(workload.environment_id.as_str())?
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "Environment `{}` was not found",
                    workload.environment_id
                ))
            })?;
        if environment.lifecycle_generation != precondition.environment_generation() {
            return conflict("reconcile claim replay has stale Environment generation");
        }

        let mut statement = self.conn.prepare(
            "SELECT reservation_id, service_generation
             FROM stack_container_create_intents
             WHERE stack_id = ?1 AND service_name = ?2 AND replica_index = ?3
             ORDER BY service_generation DESC, reservation_id ASC LIMIT 2",
        )?;
        let mut rows = statement.query(params![
            workload.stack_id,
            action.target().service_name,
            i64::from(action.target().index()),
        ])?;
        let Some(first) = rows.next()? else {
            return conflict("claimed action journal predecessor disappeared");
        };
        let latest_reservation = first.get::<_, String>(0)?;
        let latest_generation = persisted_u64("service_generation", first.get::<_, i64>(1)?)?;
        if let Some(second) = rows.next()? {
            let second_generation = persisted_u64("service_generation", second.get::<_, i64>(1)?)?;
            if second_generation == latest_generation {
                return conflict(
                    "claimed action replay found an ambiguous latest journal generation",
                );
            }
        }
        drop(rows);
        drop(statement);
        let intent = self.require_stack_container_create_intent(&latest_reservation)?;
        if intent.scope.project_id != workload.project_id
            || intent.scope.environment_id != workload.environment_id
            || intent.scope.machine_id != workload.machine_id
            || intent.scope.stack_id != workload.stack_id
            || intent.service_name != action.target().service_name
            || intent.replica_index != action.target().index()
        {
            return conflict("claimed action replay found a foreign journal head");
        }
        self.validate_journal_workload_owner(&intent)?;
        let current_binding =
            self.load_stack_container_generation_binding(&intent.scope.reservation_id)?;
        if let Some(binding) = &current_binding {
            self.validate_binding_against_intent(binding, &intent)?;
        }
        if !status_binding_is_structurally_valid(intent.status, current_binding.is_some()) {
            return conflict(
                "claimed action replay found an impossible journal status/binding shape",
            );
        }

        let linked_successor = crate::reconcile::ReconcileActionExecutionKey::new(
            session_id,
            operation_id,
            absolute_action_index,
            action,
        )?
        .matches_activation_digest(&intent.action_digest)?;
        let expected_progression = match precondition.journal_head() {
            ExpectedJournalHead::NeverJournaled => {
                matches!(action, Action::ServiceCreate { .. })
                    && intent.service_generation == 1
                    && linked_successor
            }
            ExpectedJournalHead::Exact {
                reservation_id,
                service_generation,
                ownership,
            } if intent.scope.reservation_id == *reservation_id
                && intent.service_generation == *service_generation =>
            {
                if current_binding.as_ref().map(|binding| &binding.ownership) != ownership.as_ref()
                {
                    return conflict(
                        "claimed action replay predecessor binding no longer matches its fence",
                    );
                }
                matches!(
                    action,
                    Action::ServiceCreate { .. }
                        | Action::ServiceRecreate { .. }
                        | Action::ServiceRemove { .. }
                ) && matches!(
                    intent.status,
                    StackContainerCreateStatus::CleanupPending
                        | StackContainerCreateStatus::Cleaned
                        | StackContainerCreateStatus::Blocked
                )
            }
            ExpectedJournalHead::Exact {
                service_generation, ..
            } => {
                matches!(
                    action,
                    Action::ServiceCreate { .. } | Action::ServiceRecreate { .. }
                ) && service_generation
                    .checked_add(1)
                    .is_some_and(|generation| intent.service_generation == generation)
                    && linked_successor
            }
        };
        if !expected_progression {
            return conflict("claimed action replay found unlinked journal progression");
        }

        if linked_successor {
            if intent.scope.machine_incarnation_id.as_ref()
                != Some(&workload.machine_incarnation_id)
                || intent.environment_generation != precondition.environment_generation()
            {
                return conflict(
                    "claimed action replay successor is outside the current topology generation",
                );
            }
            if !intent.status.is_terminal() {
                self.validate_intent_topology(&intent)?;
            }
        }
        self.require_journal_observed_consistent(&intent)
    }

    /// Capture exact predecessor state for a set of planned replica targets.
    ///
    /// The complete batch is read under one SQLite snapshot. This is planning
    /// evidence only; execution must revalidate it while acquiring its durable
    /// action claim.
    pub(crate) fn capture_action_preconditions(
        &self,
        stack_id: &str,
        drafts: &[ActionDraft],
    ) -> Result<Vec<ReplicaPrecondition>, StackError> {
        validate_text("stack_id", stack_id)?;
        self.with_immediate_transaction(|store| {
            let owner = store.load_stack_workload_owner(stack_id)?;
            let owner = owner.ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "stack_id `{stack_id}` has no stable workload owner for exact planning"
                ))
            })?;
            let environment = store
                .load_environment_instance(owner.environment_id.as_str())?
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "Environment `{}` was not found for stack `{stack_id}`",
                        owner.environment_id
                    ))
                })?;
            if environment.project_id != owner.project_id {
                return conflict("stack workload Project does not own the Environment");
            }
            if environment.state != vz_runtime_contract::EnvironmentState::Ready {
                return conflict(format!(
                    "stack workload Environment `{}` is not runnable ({:?})",
                    environment.environment_id, environment.state
                ));
            }
            let machine = environment
                .machines
                .iter()
                .find(|machine| machine.machine_id == owner.machine_id)
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "Machine `{}` was not found in Environment `{}`",
                        owner.machine_id, owner.environment_id
                    ))
                })?;
            if machine.environment_id != environment.environment_id {
                return conflict("stack workload Environment does not own the Machine");
            }
            if machine.state != vz_runtime_contract::MachineState::Ready {
                return conflict(format!(
                    "stack workload Machine `{}` is not runnable ({:?})",
                    machine.machine_id, machine.state
                ));
            }
            let incarnation = machine.incarnation.as_ref().ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "Machine `{}` has no current incarnation",
                    machine.machine_id
                ))
            })?;
            let workload = MachineWorkloadScope {
                schema_version: MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
                project_id: owner.project_id.clone(),
                environment_id: owner.environment_id.clone(),
                machine_id: owner.machine_id.clone(),
                machine_incarnation_id: incarnation.incarnation_id.clone(),
                stack_id: stack_id.to_string(),
            };
            store.validate_stack_workload_owner(&workload)?;

            let mut preconditions = Vec::with_capacity(drafts.len());
            for draft in drafts {
                let target = draft.target();
                let current_observed =
                    store.load_service_observed(stack_id, &target.service_name, target.index())?;
                if current_observed.as_ref() != draft.observed() {
                    return conflict(format!(
                        "replica `{}` changed after action planning",
                        target.display_name()
                    ));
                }
                let reservation_id = store
                    .conn
                    .query_row(
                        "SELECT reservation_id FROM stack_container_create_intents
                         WHERE project_id = ?1 AND environment_id = ?2 AND machine_id = ?3
                           AND stack_id = ?4 AND service_name = ?5 AND replica_index = ?6
                         ORDER BY service_generation DESC LIMIT 1",
                        params![
                            owner.project_id.as_str(),
                            owner.environment_id.as_str(),
                            owner.machine_id.as_str(),
                            stack_id,
                            target.service_name,
                            i64::from(target.index()),
                        ],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                let journal_head = match reservation_id {
                    None => {
                        if current_observed.is_some() {
                            return conflict(format!(
                                "replica `{}` has observed state without an exact journal head",
                                target.display_name()
                            ));
                        }
                        ExpectedJournalHead::NeverJournaled
                    }
                    Some(reservation_id) => {
                        let intent =
                            store.require_stack_container_create_intent(&reservation_id)?;
                        if intent.service_name != target.service_name
                            || intent.replica_index != target.index()
                        {
                            return conflict(format!(
                                "replica `{}` latest journal head targets another replica",
                                target.display_name()
                            ));
                        }
                        store.validate_journal_workload_owner(&intent)?;
                        if !matches!(
                            intent.status,
                            StackContainerCreateStatus::Cleaned
                                | StackContainerCreateStatus::Failed
                        ) {
                            store.validate_intent_topology(&intent)?;
                        }
                        store.require_journal_observed_consistent(&intent)?;
                        let binding =
                            store.load_stack_container_generation_binding(&reservation_id)?;
                        if let Some(binding) = &binding {
                            store.validate_binding_against_intent(binding, &intent)?;
                        }
                        let ownership = binding.map(|binding| binding.ownership);
                        ExpectedJournalHead::exact(
                            reservation_id,
                            intent.service_generation,
                            ownership,
                        )?
                    }
                };
                preconditions.push(ReplicaPrecondition::new(
                    workload.clone(),
                    environment.lifecycle_generation,
                    journal_head,
                )?);
            }
            Ok(preconditions)
        })
    }

    pub(super) fn create_stack_journal_schema_v4(&self) -> Result<(), StackError> {
        let replica_qualified: bool = self.conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM pragma_table_info('observed_state')
                WHERE name = 'replica_index'
             )",
            [],
            |row| row.get(0),
        )?;
        if !replica_qualified {
            self.conn.execute_batch(OBSERVED_STATE_V4_DDL)?;
        }
        self.conn.execute_batch(STACK_JOURNAL_SCHEMA_V4_DDL)?;
        Ok(())
    }

    /// Atomically reserve the globally keyed stack namespace for one stable Machine owner.
    ///
    /// New admission requires the exact current Ready Environment, Ready Machine, and
    /// Machine incarnation. Exact stable ownership replays across incarnation replacement;
    /// another Project/Environment/Machine owner is a permanent conflict. The immutable
    /// row is a namespace tombstone, not runtime authority, so it neither authorizes
    /// activation nor independently fences Environment deletion.
    pub fn reserve_stack_workload_owner(
        &self,
        scope: &MachineWorkloadScope,
        now: u64,
    ) -> Result<StackWorkloadOwner, StackError> {
        scope.validate().map_err(StackError::InvalidSpec)?;
        self.with_immediate_transaction(|store| {
            store.validate_stack_workload_owner_claim_inner(scope)?;
            if let Some(existing) = store.load_stack_workload_owner(&scope.stack_id)? {
                return Ok(existing);
            }
            let owner = StackWorkloadOwner::from_scope(scope, now);
            owner.validate()?;
            store.insert_stack_workload_owner(&owner)?;
            Ok(owner)
        })
    }

    /// Validate a prospective first claim without creating ownership or reading legacy state.
    ///
    /// An absent owner is admissible only when every globally stack-keyed namespace is empty.
    /// This prevents a current Ready Machine from adopting or observing a pre-registry stack.
    pub fn validate_stack_workload_owner_claim(
        &self,
        scope: &MachineWorkloadScope,
    ) -> Result<(), StackError> {
        scope.validate().map_err(StackError::InvalidSpec)?;
        self.validate_stack_workload_owner_claim_inner(scope)
    }

    fn validate_stack_workload_owner_claim_inner(
        &self,
        scope: &MachineWorkloadScope,
    ) -> Result<(), StackError> {
        self.validate_current_runnable_workload_scope(scope)?;
        if let Some(existing) = self.load_stack_workload_owner(&scope.stack_id)? {
            if existing.matches_scope(scope) {
                return Ok(());
            }
            return conflict(format!(
                "stack_id `{}` belongs to another stable Machine owner",
                scope.stack_id
            ));
        }

        let occupied: bool = self.conn.query_row(
            "SELECT
                 EXISTS(SELECT 1 FROM desired_state WHERE stack_name = ?1)
              OR EXISTS(SELECT 1 FROM observed_state WHERE stack_name = ?1)
              OR EXISTS(SELECT 1 FROM service_mount_digests WHERE stack_name = ?1)
              OR EXISTS(SELECT 1 FROM reconcile_progress WHERE stack_name = ?1)
              OR EXISTS(SELECT 1 FROM health_poller_state WHERE stack_name = ?1)
              OR EXISTS(SELECT 1 FROM events WHERE stack_name = ?1)
              OR EXISTS(SELECT 1 FROM sandbox_state WHERE stack_name = ?1)
              OR EXISTS(SELECT 1 FROM allocator_state WHERE stack_name = ?1)
              OR EXISTS(SELECT 1 FROM reconcile_sessions WHERE stack_name = ?1)
              OR EXISTS(SELECT 1 FROM reconcile_audit_log WHERE stack_name = ?1)
              OR EXISTS(
                    SELECT 1 FROM receipt_state
                    WHERE entity_type = 'stack' AND entity_id = ?1
                 )
              OR EXISTS(
                    SELECT 1 FROM stack_container_create_intents WHERE stack_id = ?1
                 )
              OR EXISTS(
                    SELECT 1 FROM stack_container_generation_bindings WHERE stack_id = ?1
                 )",
            params![scope.stack_id],
            |row| row.get(0),
        )?;
        let quarantined = if self.schema_version()? >= 5 {
            let legacy_v5: bool = self.conn.query_row(
                "SELECT
                     EXISTS(SELECT 1 FROM legacy_observed_state_quarantine_v5 WHERE stack_name = ?1)
                  OR EXISTS(SELECT 1 FROM legacy_reconcile_progress_quarantine_v5 WHERE stack_name = ?1)
                  OR EXISTS(SELECT 1 FROM legacy_reconcile_sessions_quarantine_v5 WHERE stack_name = ?1)
                  OR EXISTS(SELECT 1 FROM legacy_reconcile_audit_quarantine_v5 WHERE stack_name = ?1)",
                params![scope.stack_id],
                |row| row.get(0),
            )?;
            let legacy_v6 = if self.schema_version()? >= 6 {
                self.conn.query_row(
                    "SELECT
                         EXISTS(SELECT 1 FROM legacy_reconcile_sessions_quarantine_v6 WHERE stack_name = ?1)
                      OR EXISTS(SELECT 1 FROM legacy_reconcile_progress_quarantine_v6 WHERE stack_name = ?1)
                      OR EXISTS(SELECT 1 FROM legacy_reconcile_audit_quarantine_v6 WHERE stack_name = ?1)",
                    params![scope.stack_id],
                    |row| row.get::<_, bool>(0),
                )?
            } else {
                false
            };
            legacy_v5 || legacy_v6
        } else {
            false
        };
        if occupied || quarantined {
            return conflict(format!(
                "stack_id `{}` has unowned legacy state; explicit ownership migration is required",
                scope.stack_id
            ));
        }
        Ok(())
    }

    /// Load and projection-validate the immutable owner of a stack namespace.
    pub fn load_stack_workload_owner(
        &self,
        stack_id: &str,
    ) -> Result<Option<StackWorkloadOwner>, StackError> {
        validate_text("stack_id", stack_id)?;
        let row = self
            .conn
            .query_row(
                "SELECT stack_id, schema_version, project_id, environment_id, machine_id,
                        owner_json, created_at
                 FROM stack_workload_owners WHERE stack_id = ?1",
                params![stack_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            projected_stack_id,
            schema_version,
            project_id,
            environment_id,
            machine_id,
            owner_json,
            created_at,
        )) = row
        else {
            return Ok(None);
        };
        let owner: StackWorkloadOwner = serde_json::from_str(&owner_json)?;
        owner.validate()?;
        require_projection(
            owner.stack_id == projected_stack_id,
            "stack_workload_owners",
            stack_id,
            "stack_id",
        )?;
        require_projection(
            i64::from(owner.schema_version) == schema_version,
            "stack_workload_owners",
            stack_id,
            "schema_version",
        )?;
        require_projection(
            owner.project_id.as_str() == project_id,
            "stack_workload_owners",
            stack_id,
            "project_id",
        )?;
        require_projection(
            owner.environment_id.as_str() == environment_id,
            "stack_workload_owners",
            stack_id,
            "environment_id",
        )?;
        require_projection(
            owner.machine_id.as_str() == machine_id,
            "stack_workload_owners",
            stack_id,
            "machine_id",
        )?;
        require_projection(
            owner.created_at == persisted_u64("created_at", created_at)?,
            "stack_workload_owners",
            stack_id,
            "created_at",
        )?;
        Ok(Some(owner))
    }

    /// Require exact stable Project/Environment/Machine ownership for read or cleanup.
    ///
    /// This deliberately accepts non-Ready and historical incarnations after the stable
    /// tuple matches. Callers must apply their separate read/cleanup lifecycle fence; this
    /// method never grants activation authority.
    pub fn validate_stack_workload_owner(
        &self,
        scope: &MachineWorkloadScope,
    ) -> Result<StackWorkloadOwner, StackError> {
        scope.validate().map_err(StackError::InvalidSpec)?;
        let owner = self
            .load_stack_workload_owner(&scope.stack_id)?
            .ok_or_else(|| StackError::Machine {
                code: MachineErrorCode::StateConflict,
                message: format!("stack_id `{}` has no stable owner", scope.stack_id),
            })?;
        if !owner.matches_scope(scope) {
            return conflict(format!(
                "stack_id `{}` belongs to another stable Machine owner",
                scope.stack_id
            ));
        }
        Ok(owner)
    }

    pub fn begin_stack_container_create(
        &self,
        intent: &StackContainerCreateIntent,
    ) -> Result<StackContainerCreateIntent, StackError> {
        intent.validate()?;
        if intent.status != StackContainerCreateStatus::Intent
            || intent.last_error.is_some()
            || intent.completed_at.is_some()
            || intent.updated_at != intent.created_at
        {
            return invalid("new stack container create intent must be pristine");
        }
        self.with_immediate_transaction(|store| {
            if let Some(existing) =
                store.load_stack_container_create_intent(&intent.scope.reservation_id)?
            {
                if existing.same_immutable_identity(intent) {
                    store.validate_journal_workload_owner(&existing)?;
                    store.validate_intent_topology(&existing)?;
                    store.require_journal_observed_consistent(&existing)?;
                    return Ok(existing);
                }
                return conflict(format!(
                    "reservation `{}` was already used by a different stack container create intent",
                    intent.scope.reservation_id
                ));
            }
            if store.schema_version()? >= 5 && intent.applied_config_digest.is_none() {
                return invalid(
                    "new v5 stack container create intent requires applied_config_digest",
                );
            }
            store.validate_intent_topology(intent)?;
            store.validate_journal_workload_owner(intent)?;
            // `observed_state` is keyed by stack name and service only. Production
            // stack IDs are therefore globally unique topology-derived workload IDs,
            // even though container and service names may repeat across Machines.
            let foreign_stack_reservation = store
                .conn
                .query_row(
                    "SELECT reservation_id FROM stack_container_create_intents
                     WHERE stack_id = ?1
                       AND (project_id <> ?2 OR environment_id <> ?3 OR machine_id <> ?4)
                     LIMIT 1",
                    params![
                        intent.scope.stack_id,
                        intent.scope.project_id.as_str(),
                        intent.scope.environment_id.as_str(),
                        intent.scope.machine_id.as_str(),
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(foreign_stack_reservation) = foreign_stack_reservation {
                return conflict(format!(
                    "stack_id `{}` is already journaled under Machine scope reservation `{foreign_stack_reservation}`",
                    intent.scope.stack_id
                ));
            }
            let active_reservation = store
                .conn
                .query_row(
                    "SELECT reservation_id FROM stack_container_create_intents
                     WHERE project_id = ?1 AND environment_id = ?2 AND machine_id = ?3
                       AND stack_id = ?4 AND service_name = ?5 AND replica_index = ?6
                       AND status IN ('intent', 'reserved', 'running', 'cleanup_pending', 'blocked')
                     LIMIT 1",
                    params![
                        intent.scope.project_id.as_str(),
                        intent.scope.environment_id.as_str(),
                        intent.scope.machine_id.as_str(),
                        intent.scope.stack_id,
                        intent.service_name,
                        intent.replica_index,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(active_reservation) = active_reservation {
                return conflict(format!(
                    "stack service already has active create reservation `{active_reservation}`"
                ));
            }
            let generation_reservation = store
                .conn
                .query_row(
                    "SELECT reservation_id FROM stack_container_create_intents
                     WHERE project_id = ?1 AND environment_id = ?2 AND machine_id = ?3
                       AND stack_id = ?4 AND service_name = ?5 AND replica_index = ?6
                       AND service_generation = ?7
                     LIMIT 1",
                    params![
                        intent.scope.project_id.as_str(),
                        intent.scope.environment_id.as_str(),
                        intent.scope.machine_id.as_str(),
                        intent.scope.stack_id,
                        intent.service_name,
                        intent.replica_index,
                        sqlite_u64(intent.service_generation, "service_generation")?,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(generation_reservation) = generation_reservation {
                return conflict(format!(
                    "stack service generation is already reserved by `{generation_reservation}`"
                ));
            }
            store.insert_stack_container_create_intent(intent)?;
            store.save_journal_observed_state(intent, &creating_observed_state(intent)?)?;
            Ok(intent.clone())
        })
    }

    pub fn resolve_or_begin_stack_container_create(
        &self,
        selector: &StackContainerCreateSelector,
        now: u64,
    ) -> Result<
        (
            StackContainerCreateIntent,
            Option<StackContainerGenerationBinding>,
        ),
        StackError,
    > {
        let probe = selector.to_intent(1, now);
        probe.validate()?;
        self.with_immediate_transaction(|store| {
            let active_id = store
                .conn
                .query_row(
                    "SELECT reservation_id FROM stack_container_create_intents
                     WHERE project_id = ?1 AND environment_id = ?2 AND machine_id = ?3
                       AND stack_id = ?4 AND service_name = ?5 AND replica_index = ?6
                       AND status IN ('intent', 'reserved', 'running', 'cleanup_pending', 'blocked')
                     LIMIT 1",
                    params![
                        selector.project_id.as_str(),
                        selector.environment_id.as_str(),
                        selector.machine_id.as_str(),
                        selector.stack_id,
                        selector.service_name,
                        selector.replica_index,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(active_id) = active_id {
                let existing = store.require_stack_container_create_intent(&active_id)?;
                if !selector.matches(&existing) {
                    return conflict(format!(
                        "active stack container create reservation `{active_id}` does not match selector"
                    ));
                }
                store.validate_journal_workload_owner(&existing)?;
                store.validate_intent_topology(&existing)?;
                store.require_journal_observed_consistent(&existing)?;
                let binding = store.load_stack_container_generation_binding(&active_id)?;
                return Ok((existing, binding));
            }
            let previous = store.conn.query_row(
                "SELECT MAX(service_generation) FROM stack_container_create_intents
                 WHERE project_id = ?1 AND environment_id = ?2 AND machine_id = ?3
                   AND stack_id = ?4 AND service_name = ?5 AND replica_index = ?6",
                params![
                    selector.project_id.as_str(),
                    selector.environment_id.as_str(),
                    selector.machine_id.as_str(),
                    selector.stack_id,
                    selector.service_name,
                    selector.replica_index,
                ],
                |row| row.get::<_, Option<i64>>(0),
            )?;
            let generation = match previous {
                Some(previous) => persisted_u64("service_generation", previous)?
                    .checked_add(1)
                    .filter(|value| i64::try_from(*value).is_ok())
                    .ok_or_else(|| {
                        StackError::InvalidSpec(
                            "stack container service generation overflow".to_string(),
                        )
                    })?,
                None => 1,
            };
            let intent = selector.to_intent(generation, now);
            intent.validate()?;
            store.validate_intent_topology(&intent)?;
            store.validate_journal_workload_owner(&intent)?;
            let foreign_scope: Option<String> = store.conn.query_row(
                "SELECT reservation_id FROM stack_container_create_intents
                 WHERE stack_id = ?1 AND (project_id <> ?2 OR environment_id <> ?3
                    OR machine_id <> ?4) LIMIT 1",
                params![
                    selector.stack_id,
                    selector.project_id.as_str(),
                    selector.environment_id.as_str(),
                    selector.machine_id.as_str(),
                ],
                |row| row.get(0),
            ).optional()?;
            if let Some(owner) = foreign_scope {
                return conflict(format!(
                    "stack_id `{}` is already journaled under Machine scope reservation `{owner}`",
                    selector.stack_id
                ));
            }
            store.insert_stack_container_create_intent(&intent)?;
            store.save_journal_observed_state(&intent, &creating_observed_state(&intent)?)?;
            Ok((intent, None))
        })
    }

    pub fn bind_stack_container_generation(
        &self,
        binding: &StackContainerGenerationBinding,
    ) -> Result<StackContainerGenerationBinding, StackError> {
        binding.validate()?;
        self.with_immediate_transaction(|store| {
            let mut intent = store
                .load_stack_container_create_intent(&binding.reservation_id)?
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "stack container create reservation `{}` was not found",
                        binding.reservation_id
                    ))
                })?;
            store.validate_binding_against_intent(binding, &intent)?;
            if let Some(existing) =
                store.load_stack_container_generation_binding(&binding.reservation_id)?
            {
                if existing.same_immutable_authority(binding) {
                    if intent.status != StackContainerCreateStatus::Reserved {
                        return conflict(format!(
                            "reservation `{}` cannot replay activation binding from status `{}`",
                            binding.reservation_id,
                            intent.status.as_str()
                        ));
                    }
                    store.validate_intent_topology(&intent)?;
                    return Ok(existing);
                }
                return conflict(format!(
                    "reservation `{}` is already bound to a different container generation",
                    binding.reservation_id
                ));
            }
            store.validate_intent_topology(&intent)?;
            if intent.status != StackContainerCreateStatus::Intent {
                return conflict(format!(
                    "reservation `{}` cannot bind from status `{}`",
                    binding.reservation_id,
                    intent.status.as_str()
                ));
            }
            if binding.bound_at < intent.updated_at {
                return invalid("generation binding bound_at precedes its create intent");
            }
            let owner = store
                .conn
                .query_row(
                    "SELECT reservation_id FROM stack_container_generation_bindings
                     WHERE machine_incarnation_id = ?1
                       AND requested_container_id = ?2 AND runtime_generation = ?3
                     LIMIT 1",
                    params![
                        binding
                            .ownership
                            .scope
                            .as_ref()
                            .and_then(|scope| scope.machine_incarnation_id.as_ref())
                            .ok_or_else(|| {
                                StackError::InvalidSpec(
                                    "generation binding is missing its validated Machine incarnation"
                                        .to_string(),
                                )
                            })?
                            .as_str(),
                        binding.ownership.container_id,
                        sqlite_u64(binding.ownership.generation, "runtime_generation")?,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(owner) = owner {
                return conflict(format!(
                    "container generation is already bound to reservation `{owner}`"
                ));
            }
            store.insert_stack_container_generation_binding(binding, &intent)?;
            let before = intent.clone();
            intent.status = StackContainerCreateStatus::Reserved;
            intent.updated_at = intent.updated_at.max(binding.bound_at);
            store.update_stack_container_create_intent_cas(&before, &intent)?;
            Ok(binding.clone())
        })
    }

    /// Record runtime ownership discovered after activation authority became stale.
    ///
    /// Unlike the activation binding path, this never adopts the generation into
    /// `Reserved`. It atomically records exact cleanup authority and moves the
    /// journal directly to `CleanupPending`, without requiring current topology.
    pub fn bind_stack_container_generation_for_cleanup(
        &self,
        binding: &StackContainerGenerationBinding,
    ) -> Result<StackContainerGenerationBinding, StackError> {
        binding.validate()?;
        self.with_immediate_transaction(|store| {
            let mut intent =
                store.require_stack_container_create_intent(&binding.reservation_id)?;
            store.validate_binding_against_intent(binding, &intent)?;
            if let Some(existing) =
                store.load_stack_container_generation_binding(&binding.reservation_id)?
            {
                if !existing.same_immutable_authority(binding) {
                    return conflict(format!(
                        "reservation `{}` is already bound to a different container generation",
                        binding.reservation_id
                    ));
                }
                if intent.status != StackContainerCreateStatus::CleanupPending {
                    return conflict(format!(
                        "reservation `{}` already has activation binding status `{}`",
                        binding.reservation_id,
                        intent.status.as_str()
                    ));
                }
                store.require_journal_observed_consistent(&intent)?;
                return Ok(existing);
            }
            if !matches!(
                intent.status,
                StackContainerCreateStatus::Intent | StackContainerCreateStatus::Blocked
            ) {
                return conflict(format!(
                    "reservation `{}` cannot bind for cleanup from status `{}`",
                    binding.reservation_id,
                    intent.status.as_str()
                ));
            }
            if intent.status == StackContainerCreateStatus::Intent
                && store.validate_intent_topology(&intent).is_ok()
            {
                return conflict(format!(
                    "reservation `{}` is still current and cannot bind cleanup-only authority",
                    binding.reservation_id
                ));
            }
            if binding.bound_at < intent.updated_at {
                return invalid("generation binding bound_at precedes its create intent");
            }
            let incarnation_id = binding
                .ownership
                .scope
                .as_ref()
                .and_then(|scope| scope.machine_incarnation_id.as_ref())
                .ok_or_else(|| {
                    StackError::InvalidSpec(
                        "generation binding is missing its validated Machine incarnation"
                            .to_string(),
                    )
                })?;
            let owner = store
                .conn
                .query_row(
                    "SELECT reservation_id FROM stack_container_generation_bindings
                     WHERE machine_incarnation_id = ?1
                       AND requested_container_id = ?2 AND runtime_generation = ?3
                     LIMIT 1",
                    params![
                        incarnation_id.as_str(),
                        binding.ownership.container_id,
                        sqlite_u64(binding.ownership.generation, "runtime_generation")?,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(owner) = owner {
                return conflict(format!(
                    "container generation is already bound to reservation `{owner}`"
                ));
            }
            store.require_journal_observed_consistent(&intent)?;
            let observed = ServiceObservedState {
                replica: replica_key(&intent)?,
                applied_config_digest: None,
                phase: ServicePhase::Stopping,
                container_id: Some(binding.ownership.container_id.clone()),
                failed_create_ownership: Some(binding.ownership.clone()),
                last_error: intent.last_error.clone(),
                ready: false,
            };
            store.insert_stack_container_generation_binding(binding, &intent)?;
            let before = intent.clone();
            intent.status = StackContainerCreateStatus::CleanupPending;
            intent.updated_at = binding.bound_at;
            store.save_journal_observed_state(&intent, &observed)?;
            store.update_stack_container_create_intent_cas(&before, &intent)?;
            Ok(binding.clone())
        })
    }

    pub fn load_stack_container_create_intent(
        &self,
        reservation_id: &str,
    ) -> Result<Option<StackContainerCreateIntent>, StackError> {
        self.load_stack_container_create_intent_where("reservation_id = ?1", reservation_id)
    }

    pub fn load_stack_container_generation_binding(
        &self,
        reservation_id: &str,
    ) -> Result<Option<StackContainerGenerationBinding>, StackError> {
        let row = self
            .conn
            .query_row(
                "SELECT reservation_id, project_id, environment_id, machine_id,
                        machine_incarnation_id, stack_id, service_name,
                        requested_container_id, runtime_generation, ownership_json, bound_at
                 FROM stack_container_generation_bindings WHERE reservation_id = ?1",
                params![reservation_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, i64>(10)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            sql_reservation_id,
            project_id,
            environment_id,
            machine_id,
            incarnation_id,
            stack_id,
            service_name,
            container_id,
            generation,
            ownership_json,
            bound_at,
        )) = row
        else {
            return Ok(None);
        };
        let ownership: ContainerGenerationOwnership = serde_json::from_str(&ownership_json)?;
        let binding = StackContainerGenerationBinding {
            reservation_id: sql_reservation_id.clone(),
            service_name: service_name.clone(),
            ownership,
            bound_at: persisted_u64("bound_at", bound_at)?,
        };
        binding.validate()?;
        let scope = binding.ownership.scope.as_deref().ok_or_else(|| {
            StackError::InvalidSpec(
                "persisted generation binding is missing its validated scope".to_string(),
            )
        })?;
        require_projection(
            binding.reservation_id == sql_reservation_id,
            "stack_container_generation_bindings",
            reservation_id,
            "reservation_id",
        )?;
        require_projection(
            scope.project_id.as_str() == project_id,
            "stack_container_generation_bindings",
            reservation_id,
            "project_id",
        )?;
        require_projection(
            scope.environment_id.as_str() == environment_id,
            "stack_container_generation_bindings",
            reservation_id,
            "environment_id",
        )?;
        require_projection(
            scope.machine_id.as_str() == machine_id,
            "stack_container_generation_bindings",
            reservation_id,
            "machine_id",
        )?;
        require_projection(
            scope.machine_incarnation_id.as_ref().map(|id| id.as_str())
                == Some(incarnation_id.as_str()),
            "stack_container_generation_bindings",
            reservation_id,
            "machine_incarnation_id",
        )?;
        require_projection(
            scope.stack_id == stack_id,
            "stack_container_generation_bindings",
            reservation_id,
            "stack_id",
        )?;
        require_projection(
            binding.service_name == service_name,
            "stack_container_generation_bindings",
            reservation_id,
            "service_name",
        )?;
        require_projection(
            binding.ownership.container_id == container_id,
            "stack_container_generation_bindings",
            reservation_id,
            "requested_container_id",
        )?;
        require_projection(
            binding.ownership.generation == persisted_u64("runtime_generation", generation)?,
            "stack_container_generation_bindings",
            reservation_id,
            "runtime_generation",
        )?;
        Ok(Some(binding))
    }

    pub fn list_resumable_stack_container_creates(
        &self,
    ) -> Result<
        Vec<(
            StackContainerCreateIntent,
            Option<StackContainerGenerationBinding>,
        )>,
        StackError,
    > {
        let mut statement = self.conn.prepare(
            "SELECT reservation_id FROM stack_container_create_intents
             WHERE status IN ('intent', 'reserved', 'running', 'cleanup_pending', 'blocked')
             ORDER BY created_at, reservation_id",
        )?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| {
                let intent = self
                    .load_stack_container_create_intent(&id)?
                    .ok_or_else(|| {
                        StackError::InvalidSpec(format!("missing create intent `{id}`"))
                    })?;
                self.validate_intent_topology(&intent)?;
                let binding = self.load_stack_container_generation_binding(&id)?;
                if let Some(binding) = &binding {
                    self.validate_binding_against_intent(binding, &intent)?;
                }
                self.require_journal_observed_consistent(&intent)?;
                Ok((intent, binding))
            })
            .collect()
    }

    pub fn list_stack_container_recovery_records(
        &self,
    ) -> Result<Vec<StackContainerRecoveryRecord>, StackError> {
        let mut statement = self.conn.prepare(
            "SELECT reservation_id FROM stack_container_create_intents
             WHERE status NOT IN ('cleaned', 'failed') ORDER BY created_at, reservation_id",
        )?;
        let ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| self.stack_container_recovery_record(&id))
            .collect()
    }

    /// Load current and stale-incarnation recovery records for one stable Machine workload.
    ///
    /// Project, Environment, Machine, and stack remain exact. Incarnation is
    /// intentionally not a query predicate: old bindings are cleanup proof that
    /// the current incarnation must discover, while recovery disposition prevents
    /// stale work from being activated.
    pub fn list_stack_container_recovery_records_for_machine_workload(
        &self,
        scope: &MachineWorkloadScope,
    ) -> Result<Vec<StackContainerRecoveryRecord>, StackError> {
        scope.validate().map_err(StackError::InvalidSpec)?;
        let mut statement = self.conn.prepare(
            "SELECT reservation_id FROM stack_container_create_intents
             WHERE project_id = ?1 AND environment_id = ?2 AND machine_id = ?3
               AND stack_id = ?4
               AND status NOT IN ('cleaned', 'failed')
             ORDER BY created_at, reservation_id",
        )?;
        let ids = statement
            .query_map(
                params![
                    scope.project_id.as_str(),
                    scope.environment_id.as_str(),
                    scope.machine_id.as_str(),
                    scope.stack_id,
                ],
                |row| row.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| self.stack_container_recovery_record(&id))
            .collect()
    }

    /// Terminalize an unbound intent that can no longer activate.
    ///
    /// A pristine `Intent` must be stale against current topology. An explicitly
    /// quarantined `Blocked` intent is already non-activatable, so its lack of a
    /// generation binding is sufficient proof that no runtime cleanup is owned.
    pub fn abandon_stale_stack_container_create(
        &self,
        reservation_id: &str,
        reason: &str,
        now: u64,
    ) -> Result<ServiceObservedState, StackError> {
        validate_digest("reason", reason)?;
        self.with_immediate_transaction(|store| {
            let mut intent = store.require_stack_container_create_intent(reservation_id)?;
            let observed = ServiceObservedState {
                replica: replica_key(&intent)?,
                applied_config_digest: None,
                phase: ServicePhase::Failed,
                container_id: None,
                failed_create_ownership: None,
                last_error: Some(reason.to_string()),
                ready: false,
            };
            if intent.status == StackContainerCreateStatus::Failed
                && intent.last_error.as_deref() == Some(reason)
            {
                store.require_exact_observed(&intent, &observed)?;
                return Ok(observed);
            }
            if !matches!(
                intent.status,
                StackContainerCreateStatus::Intent | StackContainerCreateStatus::Blocked
            ) {
                return conflict(format!(
                    "reservation `{reservation_id}` cannot be abandoned from status `{}`",
                    intent.status.as_str()
                ));
            }
            if store
                .load_stack_container_generation_binding(reservation_id)?
                .is_some()
            {
                return conflict(format!(
                    "reservation `{reservation_id}` has cleanup authority and cannot be abandoned"
                ));
            }
            if intent.status == StackContainerCreateStatus::Intent
                && store.validate_intent_topology(&intent).is_ok()
            {
                return conflict(format!(
                    "reservation `{reservation_id}` is still current and cannot be abandoned"
                ));
            }
            store.require_journal_observed_consistent(&intent)?;
            let before = intent.clone();
            intent.status = StackContainerCreateStatus::Failed;
            intent.last_error = Some(reason.to_string());
            intent.updated_at = now;
            intent.completed_at = Some(now);
            store.save_journal_observed_state(&intent, &observed)?;
            store.update_stack_container_create_intent_cas(&before, &intent)?;
            Ok(observed)
        })
    }

    pub fn publish_stack_container_create_success(
        &self,
        reservation_id: &str,
        ready: bool,
        now: u64,
    ) -> Result<ServiceObservedState, StackError> {
        self.with_immediate_transaction(|store| {
            let mut intent = store.require_stack_container_create_intent(reservation_id)?;
            let binding = store
                .load_stack_container_generation_binding(reservation_id)?
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "reservation `{reservation_id}` has no generation binding"
                    ))
                })?;
            store.validate_binding_against_intent(&binding, &intent)?;
            store.validate_intent_topology(&intent)?;
            let applied_config_digest = intent.applied_config_digest.clone().ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "reservation `{reservation_id}` has no exact applied configuration digest"
                ))
            })?;
            let observed = ServiceObservedState {
                replica: replica_key(&intent)?,
                applied_config_digest: Some(applied_config_digest),
                phase: ServicePhase::Running,
                container_id: Some(intent.requested_container_id.clone()),
                failed_create_ownership: Some(binding.ownership),
                last_error: None,
                ready,
            };
            if intent.status == StackContainerCreateStatus::Running {
                store.require_exact_observed(&intent, &observed)?;
                return Ok(observed);
            }
            if intent.status != StackContainerCreateStatus::Reserved {
                return conflict(format!(
                    "reservation `{reservation_id}` cannot publish success from status `{}`",
                    intent.status.as_str()
                ));
            }
            store.require_journal_observed_consistent(&intent)?;
            let before = intent.clone();
            intent.status = StackContainerCreateStatus::Running;
            intent.updated_at = now;
            store.save_journal_observed_state(&intent, &observed)?;
            store.update_stack_container_create_intent_cas(&before, &intent)?;
            Ok(observed)
        })
    }

    /// Monotonically publish readiness for an exact Running journal generation.
    ///
    /// The reservation and expected runtime identity fence stale health results;
    /// all authority-bearing fields are reconstructed from the durable journal.
    pub fn publish_stack_container_ready(
        &self,
        expected_target: &ServiceReplicaKey,
        expected_ownership: &ContainerGenerationOwnership,
    ) -> Result<ServiceObservedState, StackError> {
        expected_ownership
            .validate()
            .map_err(StackError::InvalidSpec)?;
        let reservation_id = &expected_ownership
            .scope
            .as_deref()
            .ok_or_else(|| {
                StackError::InvalidSpec(
                    "readiness ownership is missing its exact reservation scope".to_string(),
                )
            })?
            .reservation_id;
        self.with_immediate_transaction(|store| {
            let intent = store.require_stack_container_create_intent(reservation_id)?;
            if intent.status != StackContainerCreateStatus::Running {
                return conflict(format!(
                    "reservation `{reservation_id}` cannot publish readiness from status `{}`",
                    intent.status.as_str()
                ));
            }
            store.validate_journal_workload_owner(&intent)?;
            store.validate_intent_topology(&intent)?;
            let target = replica_key(&intent)?;
            if &target != expected_target {
                return conflict(format!(
                    "stale readiness result does not match reservation `{reservation_id}`"
                ));
            }
            let binding = store
                .load_stack_container_generation_binding(reservation_id)?
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "running reservation `{reservation_id}` has no generation binding"
                    ))
                })?;
            store.validate_binding_against_intent(&binding, &intent)?;
            if &binding.ownership != expected_ownership {
                return conflict(format!(
                    "stale readiness generation does not match reservation `{reservation_id}`"
                ));
            }
            store.require_journal_observed_consistent(&intent)?;
            let applied_config_digest = intent.applied_config_digest.clone().ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "reservation `{reservation_id}` has no exact applied configuration digest"
                ))
            })?;
            let observed = ServiceObservedState {
                replica: target,
                applied_config_digest: Some(applied_config_digest),
                phase: ServicePhase::Running,
                container_id: Some(intent.requested_container_id.clone()),
                failed_create_ownership: Some(binding.ownership),
                last_error: None,
                ready: true,
            };
            if store
                .load_service_observed(
                    &intent.scope.stack_id,
                    &intent.service_name,
                    intent.replica_index,
                )?
                .as_ref()
                == Some(&observed)
            {
                return Ok(observed);
            }
            store.save_journal_observed_state(&intent, &observed)?;
            Ok(observed)
        })
    }

    pub fn publish_stack_container_create_failure(
        &self,
        reservation_id: &str,
        error: &str,
        now: u64,
    ) -> Result<ServiceObservedState, StackError> {
        validate_digest("error", error)?;
        self.with_immediate_transaction(|store| {
            let mut intent = store.require_stack_container_create_intent(reservation_id)?;
            if !matches!(
                intent.status,
                StackContainerCreateStatus::Intent | StackContainerCreateStatus::Reserved
            ) {
                return conflict(format!(
                    "reservation `{reservation_id}` cannot publish create failure from status `{}`",
                    intent.status.as_str()
                ));
            }
            store.validate_intent_topology(&intent)?;
            let binding = store.load_stack_container_generation_binding(reservation_id)?;
            if let Some(binding) = &binding {
                store.validate_binding_against_intent(binding, &intent)?;
            }
            store.require_journal_observed_consistent(&intent)?;
            let observed = ServiceObservedState {
                replica: replica_key(&intent)?,
                applied_config_digest: None,
                phase: ServicePhase::Failed,
                container_id: binding
                    .as_ref()
                    .map(|binding| binding.ownership.container_id.clone()),
                failed_create_ownership: binding.map(|binding| binding.ownership),
                last_error: Some(error.to_string()),
                ready: false,
            };
            let before = intent.clone();
            intent.status = if observed.failed_create_ownership.is_some() {
                StackContainerCreateStatus::CleanupPending
            } else {
                StackContainerCreateStatus::Failed
            };
            intent.last_error = Some(error.to_string());
            intent.updated_at = now;
            intent.completed_at = intent.status.is_terminal().then_some(now);
            store.save_journal_observed_state(&intent, &observed)?;
            store.update_stack_container_create_intent_cas(&before, &intent)?;
            Ok(observed)
        })
    }

    /// Quarantine a nonterminal create whose runtime state cannot be safely adopted or cleaned.
    pub fn publish_stack_container_blocked(
        &self,
        reservation_id: &str,
        reason: &str,
        now: u64,
    ) -> Result<ServiceObservedState, StackError> {
        validate_digest("reason", reason)?;
        self.with_immediate_transaction(|store| {
            let mut intent = store.require_stack_container_create_intent(reservation_id)?;
            let binding = store.load_stack_container_generation_binding(reservation_id)?;
            if let Some(binding) = &binding {
                store.validate_binding_against_intent(binding, &intent)?;
            }
            let observed = ServiceObservedState {
                replica: replica_key(&intent)?,
                applied_config_digest: None,
                phase: ServicePhase::Failed,
                container_id: binding
                    .as_ref()
                    .map(|binding| binding.ownership.container_id.clone()),
                failed_create_ownership: binding.map(|binding| binding.ownership),
                last_error: Some(reason.to_string()),
                ready: false,
            };
            if intent.status == StackContainerCreateStatus::Blocked
                && intent.last_error.as_deref() == Some(reason)
            {
                store.require_exact_observed(&intent, &observed)?;
                return Ok(observed);
            }
            if intent.status.is_terminal() {
                return conflict(format!(
                    "reservation `{reservation_id}` cannot be blocked from terminal status `{}`",
                    intent.status.as_str()
                ));
            }
            store.require_journal_observed_consistent(&intent)?;
            let before = intent.clone();
            intent.status = StackContainerCreateStatus::Blocked;
            intent.last_error = Some(reason.to_string());
            intent.updated_at = now;
            intent.completed_at = None;
            store.save_journal_observed_state(&intent, &observed)?;
            store.update_stack_container_create_intent_cas(&before, &intent)?;
            Ok(observed)
        })
    }

    /// Admit cleanup using only the exact immutable generation binding.
    ///
    /// This deliberately does not revalidate current topology: stale and blocked
    /// generations remain cleanup proof but can never regain activation authority.
    pub fn begin_stack_container_cleanup(
        &self,
        reservation_id: &str,
        now: u64,
    ) -> Result<ServiceObservedState, StackError> {
        self.with_immediate_transaction(|store| {
            let mut intent = store.require_stack_container_create_intent(reservation_id)?;
            let binding = store
                .load_stack_container_generation_binding(reservation_id)?
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "reservation `{reservation_id}` has no generation binding"
                    ))
                })?;
            store.validate_binding_against_intent(&binding, &intent)?;
            let observed = ServiceObservedState {
                replica: replica_key(&intent)?,
                applied_config_digest: None,
                phase: ServicePhase::Stopping,
                container_id: Some(binding.ownership.container_id.clone()),
                failed_create_ownership: Some(binding.ownership),
                last_error: intent.last_error.clone(),
                ready: false,
            };
            store.require_journal_observed_consistent(&intent)?;
            if intent.status == StackContainerCreateStatus::CleanupPending {
                let actual = store.load_service_observed(
                    &intent.scope.stack_id,
                    &intent.service_name,
                    intent.replica_index,
                )?;
                if actual.as_ref() == Some(&observed) {
                    return Ok(observed);
                }
            } else if !matches!(
                intent.status,
                StackContainerCreateStatus::Reserved
                    | StackContainerCreateStatus::Running
                    | StackContainerCreateStatus::Blocked
            ) {
                return conflict(format!(
                    "reservation `{reservation_id}` cannot begin cleanup from status `{}`",
                    intent.status.as_str()
                ));
            }
            let before = intent.clone();
            intent.status = StackContainerCreateStatus::CleanupPending;
            intent.updated_at = now;
            store.save_journal_observed_state(&intent, &observed)?;
            store.update_stack_container_create_intent_cas(&before, &intent)?;
            Ok(observed)
        })
    }

    pub fn publish_stack_container_cleanup_success(
        &self,
        reservation_id: &str,
        now: u64,
    ) -> Result<ServiceObservedState, StackError> {
        self.with_immediate_transaction(|store| {
            let mut intent = store.require_stack_container_create_intent(reservation_id)?;
            let binding = store
                .load_stack_container_generation_binding(reservation_id)?
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "reservation `{reservation_id}` has no generation binding"
                    ))
                })?;
            store.validate_binding_against_intent(&binding, &intent)?;
            let observed = ServiceObservedState {
                replica: replica_key(&intent)?,
                applied_config_digest: None,
                phase: ServicePhase::Stopped,
                container_id: None,
                failed_create_ownership: None,
                last_error: None,
                ready: false,
            };
            if intent.status == StackContainerCreateStatus::Cleaned {
                store.require_exact_observed(&intent, &observed)?;
                return Ok(observed);
            }
            if !matches!(
                intent.status,
                StackContainerCreateStatus::Running | StackContainerCreateStatus::CleanupPending
            ) {
                return conflict(format!(
                    "reservation `{reservation_id}` cannot complete cleanup from status `{}`",
                    intent.status.as_str()
                ));
            }
            store.require_journal_observed_consistent(&intent)?;
            let before = intent.clone();
            intent.status = StackContainerCreateStatus::Cleaned;
            intent.last_error = None;
            intent.updated_at = now;
            intent.completed_at = Some(now);
            store.save_journal_observed_state(&intent, &observed)?;
            store.update_stack_container_create_intent_cas(&before, &intent)?;
            Ok(observed)
        })
    }

    fn stack_container_recovery_record(
        &self,
        reservation_id: &str,
    ) -> Result<StackContainerRecoveryRecord, StackError> {
        let intent = self.require_stack_container_create_intent(reservation_id)?;
        let binding = self.load_stack_container_generation_binding(reservation_id)?;
        if let Some(binding) = &binding {
            self.validate_binding_against_intent(binding, &intent)?;
        }
        self.require_journal_observed_consistent(&intent)?;
        let topology = self.validate_intent_topology(&intent);
        let disposition = match (topology, binding.is_some(), intent.status) {
            (_, true, StackContainerCreateStatus::Blocked) => {
                StackContainerRecoveryDisposition::CleanupOnly {
                    stale_reason: "blocked journal retains exact cleanup authority".to_string(),
                }
            }
            (_, false, StackContainerCreateStatus::Blocked) => {
                StackContainerRecoveryDisposition::Abandonable {
                    stale_reason: "blocked journal has no runtime ownership to clean up"
                        .to_string(),
                }
            }
            (Ok(()), _, StackContainerCreateStatus::CleanupPending) => {
                StackContainerRecoveryDisposition::CleanupOnly {
                    stale_reason: "journal status requires cleanup".to_string(),
                }
            }
            (Ok(()), _, _) => StackContainerRecoveryDisposition::Activatable,
            (Err(error), true, _) => StackContainerRecoveryDisposition::CleanupOnly {
                stale_reason: error.to_string(),
            },
            (Err(error), false, _) => StackContainerRecoveryDisposition::Abandonable {
                stale_reason: error.to_string(),
            },
        };
        Ok(StackContainerRecoveryRecord {
            intent,
            binding,
            disposition,
        })
    }

    pub(super) fn require_no_nonterminal_stack_container_creates(
        &self,
        environment_id: &str,
    ) -> Result<(), StackError> {
        let mut statement = self.conn.prepare(
            "SELECT reservation_id FROM stack_container_create_intents
             ORDER BY created_at, reservation_id",
        )?;
        let reservation_ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for reservation_id in reservation_ids {
            let intent = self
                .load_stack_container_create_intent(&reservation_id)?
                .ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "missing create intent `{reservation_id}` during deletion fencing"
                    ))
                })?;
            if intent.scope.environment_id.as_str() == environment_id
                && !intent.status.is_terminal()
            {
                return conflict(format!(
                    "Environment `{environment_id}` has nonterminal stack container create reservation `{reservation_id}`"
                ));
            }
        }
        Ok(())
    }

    fn validate_current_runnable_workload_scope(
        &self,
        scope: &MachineWorkloadScope,
    ) -> Result<(), StackError> {
        let environment = self
            .load_environment_instance(scope.environment_id.as_str())?
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "Environment `{}` was not found",
                    scope.environment_id
                ))
            })?;
        if environment.project_id != scope.project_id {
            return conflict("stack workload Project does not own the Environment");
        }
        if environment.state != vz_runtime_contract::EnvironmentState::Ready {
            return conflict(format!(
                "stack workload Environment `{}` is not runnable ({:?})",
                environment.environment_id, environment.state
            ));
        }
        let machine = environment
            .machines
            .iter()
            .find(|machine| machine.machine_id == scope.machine_id)
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "Machine `{}` was not found in Environment `{}`",
                    scope.machine_id, scope.environment_id
                ))
            })?;
        if machine.environment_id != scope.environment_id {
            return conflict("stack workload Environment does not own the Machine");
        }
        if machine.state != vz_runtime_contract::MachineState::Ready {
            return conflict(format!(
                "stack workload Machine `{}` is not runnable ({:?})",
                machine.machine_id, machine.state
            ));
        }
        let current = machine.incarnation.as_ref().ok_or_else(|| {
            StackError::InvalidSpec(format!(
                "Machine `{}` has no current incarnation",
                machine.machine_id
            ))
        })?;
        if current.incarnation_id != scope.machine_incarnation_id {
            return conflict("stack workload Machine incarnation is stale");
        }
        Ok(())
    }

    fn validate_journal_workload_owner(
        &self,
        intent: &StackContainerCreateIntent,
    ) -> Result<StackWorkloadOwner, StackError> {
        let scope = workload_scope_for_intent(intent)?;
        self.validate_stack_workload_owner(&scope)
    }

    fn insert_stack_workload_owner(&self, owner: &StackWorkloadOwner) -> Result<(), StackError> {
        self.conn.execute(
            "INSERT INTO stack_workload_owners (
                stack_id, schema_version, project_id, environment_id, machine_id,
                owner_json, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                owner.stack_id,
                owner.schema_version,
                owner.project_id.as_str(),
                owner.environment_id.as_str(),
                owner.machine_id.as_str(),
                serde_json::to_string(owner)?,
                sqlite_u64(owner.created_at, "created_at")?,
            ],
        )?;
        Ok(())
    }

    fn validate_intent_topology(
        &self,
        intent: &StackContainerCreateIntent,
    ) -> Result<(), StackError> {
        let environment = self
            .load_environment_instance(intent.scope.environment_id.as_str())?
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "Environment `{}` was not found",
                    intent.scope.environment_id
                ))
            })?;
        if environment.project_id != intent.scope.project_id {
            return conflict("stack container create Project does not own the Environment");
        }
        if environment.state != vz_runtime_contract::EnvironmentState::Ready {
            return conflict(format!(
                "stack container create Environment `{}` is not runnable ({:?})",
                environment.environment_id, environment.state
            ));
        }
        if environment.definition_digest != intent.definition_digest {
            return conflict("stack container create definition digest is stale");
        }
        if environment.lifecycle_generation != intent.environment_generation {
            return conflict("stack container create Environment generation is stale");
        }
        let machine = environment
            .machines
            .iter()
            .find(|machine| machine.machine_id == intent.scope.machine_id)
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "Machine `{}` was not found in Environment `{}`",
                    intent.scope.machine_id, intent.scope.environment_id
                ))
            })?;
        if machine.state != vz_runtime_contract::MachineState::Ready {
            return conflict(format!(
                "stack container create Machine `{}` is not runnable ({:?})",
                machine.machine_id, machine.state
            ));
        }
        let current = machine.incarnation.as_ref().ok_or_else(|| {
            StackError::InvalidSpec(format!(
                "Machine `{}` has no current incarnation",
                machine.machine_id
            ))
        })?;
        if Some(&current.incarnation_id) != intent.scope.machine_incarnation_id.as_ref() {
            return conflict("stack container create Machine incarnation is stale");
        }
        Ok(())
    }

    fn validate_binding_against_intent(
        &self,
        binding: &StackContainerGenerationBinding,
        intent: &StackContainerCreateIntent,
    ) -> Result<(), StackError> {
        binding.validate()?;
        let scope = binding.ownership.scope.as_deref().ok_or_else(|| {
            StackError::InvalidSpec("generation binding is missing its validated scope".to_string())
        })?;
        if binding.reservation_id != intent.scope.reservation_id
            || binding.service_name != intent.service_name
            || binding.ownership.container_id != intent.requested_container_id
            || scope != &intent.scope
        {
            return conflict(format!(
                "generation binding does not match reservation `{}`",
                intent.scope.reservation_id
            ));
        }
        Ok(())
    }

    fn insert_stack_container_create_intent(
        &self,
        intent: &StackContainerCreateIntent,
    ) -> Result<(), StackError> {
        let incarnation_id = intent
            .scope
            .machine_incarnation_id
            .as_ref()
            .ok_or_else(|| {
                StackError::InvalidSpec(
                    "stack container create intent is missing its validated Machine incarnation"
                        .to_string(),
                )
            })?;
        let has_applied_config_digest: bool = self.conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM pragma_table_info('stack_container_create_intents')
                WHERE name = 'applied_config_digest'
             )",
            [],
            |row| row.get(0),
        )?;
        let environment_generation =
            sqlite_u64(intent.environment_generation, "environment_generation")?;
        let service_generation = sqlite_u64(intent.service_generation, "service_generation")?;
        let intent_json = serde_json::to_string(intent)?;
        let created_at = sqlite_u64(intent.created_at, "created_at")?;
        let updated_at = sqlite_u64(intent.updated_at, "updated_at")?;
        let completed_at = intent
            .completed_at
            .map(|value| sqlite_u64(value, "completed_at"))
            .transpose()?;
        if has_applied_config_digest {
            self.conn.execute(
                "INSERT INTO stack_container_create_intents (
                    reservation_id, schema_version, project_id, environment_id, machine_id,
                    machine_incarnation_id, environment_generation, stack_id, service_name,
                    replica_index, service_generation, requested_container_id, definition_digest,
                    action_digest, status, intent_json, last_error, created_at, updated_at,
                    completed_at, applied_config_digest
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                           ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
                params![
                    intent.scope.reservation_id,
                    intent.schema_version,
                    intent.scope.project_id.as_str(),
                    intent.scope.environment_id.as_str(),
                    intent.scope.machine_id.as_str(),
                    incarnation_id.as_str(),
                    environment_generation,
                    intent.scope.stack_id,
                    intent.service_name,
                    intent.replica_index,
                    service_generation,
                    intent.requested_container_id,
                    intent.definition_digest,
                    intent.action_digest,
                    intent.status.as_str(),
                    intent_json,
                    intent.last_error,
                    created_at,
                    updated_at,
                    completed_at,
                    intent.applied_config_digest,
                ],
            )?;
        } else {
            self.conn.execute(
                "INSERT INTO stack_container_create_intents (
                    reservation_id, schema_version, project_id, environment_id, machine_id,
                    machine_incarnation_id, environment_generation, stack_id, service_name,
                    replica_index, service_generation, requested_container_id, definition_digest,
                    action_digest, status, intent_json, last_error, created_at, updated_at,
                    completed_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                           ?14, ?15, ?16, ?17, ?18, ?19, ?20)",
                params![
                    intent.scope.reservation_id,
                    intent.schema_version,
                    intent.scope.project_id.as_str(),
                    intent.scope.environment_id.as_str(),
                    intent.scope.machine_id.as_str(),
                    incarnation_id.as_str(),
                    environment_generation,
                    intent.scope.stack_id,
                    intent.service_name,
                    intent.replica_index,
                    service_generation,
                    intent.requested_container_id,
                    intent.definition_digest,
                    intent.action_digest,
                    intent.status.as_str(),
                    intent_json,
                    intent.last_error,
                    created_at,
                    updated_at,
                    completed_at,
                ],
            )?;
        }
        Ok(())
    }

    fn insert_stack_container_generation_binding(
        &self,
        binding: &StackContainerGenerationBinding,
        intent: &StackContainerCreateIntent,
    ) -> Result<(), StackError> {
        let scope = binding.ownership.scope.as_deref().ok_or_else(|| {
            StackError::InvalidSpec("generation binding is missing its validated scope".to_string())
        })?;
        let incarnation_id = scope.machine_incarnation_id.as_ref().ok_or_else(|| {
            StackError::InvalidSpec(
                "generation binding is missing its validated Machine incarnation".to_string(),
            )
        })?;
        self.conn.execute(
            "INSERT INTO stack_container_generation_bindings (
                reservation_id, project_id, environment_id, machine_id,
                machine_incarnation_id, stack_id, service_name, requested_container_id,
                runtime_generation, ownership_json, bound_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                binding.reservation_id,
                scope.project_id.as_str(),
                scope.environment_id.as_str(),
                scope.machine_id.as_str(),
                incarnation_id.as_str(),
                scope.stack_id,
                intent.service_name,
                binding.ownership.container_id,
                sqlite_u64(binding.ownership.generation, "runtime_generation")?,
                serde_json::to_string(&binding.ownership)?,
                sqlite_u64(binding.bound_at, "bound_at")?,
            ],
        )?;
        Ok(())
    }

    fn load_stack_container_create_intent_where(
        &self,
        predicate: &str,
        value: &str,
    ) -> Result<Option<StackContainerCreateIntent>, StackError> {
        let has_applied_config_digest: bool = self.conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM pragma_table_info('stack_container_create_intents')
                WHERE name = 'applied_config_digest'
             )",
            [],
            |row| row.get(0),
        )?;
        let applied_config_projection = if has_applied_config_digest {
            "applied_config_digest"
        } else {
            "NULL"
        };
        let sql = format!(
            "SELECT reservation_id, schema_version, project_id, environment_id, machine_id,
                    machine_incarnation_id, environment_generation, stack_id, service_name,
                    replica_index, service_generation, requested_container_id,
                    definition_digest, action_digest, {applied_config_projection},
                    status, intent_json, last_error,
                    created_at, updated_at, completed_at
             FROM stack_container_create_intents WHERE {predicate}"
        );
        let row = self
            .conn
            .query_row(&sql, params![value], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, String>(13)?,
                    row.get::<_, Option<String>>(14)?,
                    row.get::<_, String>(15)?,
                    row.get::<_, String>(16)?,
                    row.get::<_, Option<String>>(17)?,
                    row.get::<_, i64>(18)?,
                    row.get::<_, i64>(19)?,
                    row.get::<_, Option<i64>>(20)?,
                ))
            })
            .optional()?;
        let Some((
            reservation_id,
            schema_version,
            project_id,
            environment_id,
            machine_id,
            incarnation_id,
            environment_generation,
            stack_id,
            service_name,
            replica_index,
            service_generation,
            container_id,
            definition_digest,
            action_digest,
            applied_config_digest,
            status,
            intent_json,
            last_error,
            created_at,
            updated_at,
            completed_at,
        )) = row
        else {
            return Ok(None);
        };
        let intent: StackContainerCreateIntent = serde_json::from_str(&intent_json)?;
        intent.validate()?;
        let key = intent.scope.reservation_id.as_str();
        require_projection(
            intent.scope.reservation_id == reservation_id,
            "stack_container_create_intents",
            key,
            "reservation_id",
        )?;
        require_projection(
            i64::from(intent.schema_version) == schema_version,
            "stack_container_create_intents",
            key,
            "schema_version",
        )?;
        require_projection(
            intent.scope.project_id.as_str() == project_id,
            "stack_container_create_intents",
            key,
            "project_id",
        )?;
        require_projection(
            intent.scope.environment_id.as_str() == environment_id,
            "stack_container_create_intents",
            key,
            "environment_id",
        )?;
        require_projection(
            intent.scope.machine_id.as_str() == machine_id,
            "stack_container_create_intents",
            key,
            "machine_id",
        )?;
        require_projection(
            intent
                .scope
                .machine_incarnation_id
                .as_ref()
                .map(|id| id.as_str())
                == Some(incarnation_id.as_str()),
            "stack_container_create_intents",
            key,
            "machine_incarnation_id",
        )?;
        require_projection(
            intent.environment_generation
                == persisted_u64("environment_generation", environment_generation)?,
            "stack_container_create_intents",
            key,
            "environment_generation",
        )?;
        require_projection(
            intent.scope.stack_id == stack_id,
            "stack_container_create_intents",
            key,
            "stack_id",
        )?;
        require_projection(
            intent.service_name == service_name,
            "stack_container_create_intents",
            key,
            "service_name",
        )?;
        require_projection(
            i64::from(intent.replica_index) == replica_index,
            "stack_container_create_intents",
            key,
            "replica_index",
        )?;
        require_projection(
            intent.service_generation == persisted_u64("service_generation", service_generation)?,
            "stack_container_create_intents",
            key,
            "service_generation",
        )?;
        require_projection(
            intent.requested_container_id == container_id,
            "stack_container_create_intents",
            key,
            "requested_container_id",
        )?;
        require_projection(
            intent.definition_digest == definition_digest,
            "stack_container_create_intents",
            key,
            "definition_digest",
        )?;
        require_projection(
            intent.action_digest == action_digest,
            "stack_container_create_intents",
            key,
            "action_digest",
        )?;
        require_projection(
            !has_applied_config_digest || intent.applied_config_digest == applied_config_digest,
            "stack_container_create_intents",
            key,
            "applied_config_digest",
        )?;
        require_projection(
            intent.status == StackContainerCreateStatus::parse(&status)?,
            "stack_container_create_intents",
            key,
            "status",
        )?;
        require_projection(
            intent.last_error == last_error,
            "stack_container_create_intents",
            key,
            "last_error",
        )?;
        require_projection(
            intent.created_at == persisted_u64("created_at", created_at)?,
            "stack_container_create_intents",
            key,
            "created_at",
        )?;
        require_projection(
            intent.updated_at == persisted_u64("updated_at", updated_at)?,
            "stack_container_create_intents",
            key,
            "updated_at",
        )?;
        require_projection(
            intent.completed_at
                == completed_at
                    .map(|value| persisted_u64("completed_at", value))
                    .transpose()?,
            "stack_container_create_intents",
            key,
            "completed_at",
        )?;
        Ok(Some(intent))
    }

    fn update_stack_container_create_intent_cas(
        &self,
        before: &StackContainerCreateIntent,
        after: &StackContainerCreateIntent,
    ) -> Result<(), StackError> {
        after.validate()?;
        if !before.same_immutable_identity(after) {
            return invalid("stack container create update changed immutable intent identity");
        }
        let affected = self.conn.execute(
            "UPDATE stack_container_create_intents
             SET status = ?1, intent_json = ?2, last_error = ?3,
                 updated_at = ?4, completed_at = ?5
             WHERE reservation_id = ?6 AND status = ?7 AND intent_json = ?8",
            params![
                after.status.as_str(),
                serde_json::to_string(after)?,
                after.last_error,
                sqlite_u64(after.updated_at, "updated_at")?,
                after
                    .completed_at
                    .map(|value| sqlite_u64(value, "completed_at"))
                    .transpose()?,
                before.scope.reservation_id,
                before.status.as_str(),
                serde_json::to_string(before)?,
            ],
        )?;
        if affected == 1 {
            Ok(())
        } else {
            conflict(format!(
                "stack container create reservation `{}` changed during compare-and-swap",
                before.scope.reservation_id
            ))
        }
    }

    fn require_stack_container_create_intent(
        &self,
        reservation_id: &str,
    ) -> Result<StackContainerCreateIntent, StackError> {
        self.load_stack_container_create_intent(reservation_id)?
            .ok_or_else(|| {
                StackError::InvalidSpec(format!(
                    "stack container create reservation `{reservation_id}` was not found"
                ))
            })
    }

    fn save_journal_observed_state(
        &self,
        intent: &StackContainerCreateIntent,
        state: &ServiceObservedState,
    ) -> Result<(), StackError> {
        let expected = ServiceReplicaKey::new(intent.service_name.clone(), intent.replica_index)?;
        if state.replica != expected {
            return conflict(format!(
                "observed state replica `{}` does not match journal replica `{}`",
                state.replica.display_name(),
                expected.display_name()
            ));
        }
        let json = serde_json::to_string(state)?;
        self.conn.execute(
            "INSERT INTO observed_state (stack_name, service_name, replica_index, state_json)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(stack_name, service_name, replica_index) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = datetime('now')",
            params![
                intent.scope.stack_id,
                intent.service_name,
                intent.replica_index,
                json,
            ],
        )?;
        Ok(())
    }

    fn require_exact_observed(
        &self,
        intent: &StackContainerCreateIntent,
        expected: &ServiceObservedState,
    ) -> Result<(), StackError> {
        let actual = self.load_service_observed(
            &intent.scope.stack_id,
            &intent.service_name,
            intent.replica_index,
        )?;
        if actual.as_ref() == Some(expected) {
            Ok(())
        } else {
            conflict(format!(
                "observed state for `{}/{}` disagrees with its create journal",
                intent.scope.stack_id,
                expected.replica.display_name()
            ))
        }
    }

    fn load_service_observed(
        &self,
        stack_id: &str,
        service_name: &str,
        replica_index: u32,
    ) -> Result<Option<ServiceObservedState>, StackError> {
        self.load_observed_state_for_replica(stack_id, service_name, replica_index)
    }

    pub(super) fn require_journal_observed_consistent(
        &self,
        intent: &StackContainerCreateIntent,
    ) -> Result<(), StackError> {
        match intent.status {
            StackContainerCreateStatus::Intent | StackContainerCreateStatus::Reserved => {
                self.require_exact_observed(intent, &creating_observed_state(intent)?)
            }
            StackContainerCreateStatus::Running => {
                let binding = self
                    .load_stack_container_generation_binding(&intent.scope.reservation_id)?
                    .ok_or_else(|| {
                        StackError::InvalidSpec(format!(
                            "running reservation `{}` has no generation binding",
                            intent.scope.reservation_id
                        ))
                    })?;
                self.validate_binding_against_intent(&binding, intent)?;
                let actual = self.load_service_observed(
                    &intent.scope.stack_id,
                    &intent.service_name,
                    intent.replica_index,
                )?;
                let valid = actual.as_ref().is_some_and(|state| {
                    state.replica.service_name == intent.service_name
                        && state.replica.index() == intent.replica_index
                        && state.phase == ServicePhase::Running
                        && state.container_id.as_deref()
                            == Some(intent.requested_container_id.as_str())
                        && state.failed_create_ownership.as_ref() == Some(&binding.ownership)
                        && state.applied_config_digest == intent.applied_config_digest
                        && state.last_error.is_none()
                });
                require_projection(
                    valid,
                    "observed_state",
                    &intent.scope.reservation_id,
                    "running journal authority",
                )
            }
            StackContainerCreateStatus::CleanupPending => {
                let binding = self
                    .load_stack_container_generation_binding(&intent.scope.reservation_id)?
                    .ok_or_else(|| {
                        StackError::InvalidSpec(format!(
                            "cleanup-pending reservation `{}` has no generation binding",
                            intent.scope.reservation_id
                        ))
                    })?;
                self.validate_binding_against_intent(&binding, intent)?;
                let actual = self.load_service_observed(
                    &intent.scope.stack_id,
                    &intent.service_name,
                    intent.replica_index,
                )?;
                let valid = actual.as_ref().is_some_and(|state| {
                    matches!(state.phase, ServicePhase::Failed | ServicePhase::Stopping)
                        && state.container_id.as_deref()
                            == Some(intent.requested_container_id.as_str())
                        && state.failed_create_ownership.as_ref() == Some(&binding.ownership)
                        && state.last_error == intent.last_error
                        && !state.ready
                });
                require_projection(
                    valid,
                    "observed_state",
                    &intent.scope.reservation_id,
                    "cleanup journal authority",
                )
            }
            StackContainerCreateStatus::Blocked => {
                let binding =
                    self.load_stack_container_generation_binding(&intent.scope.reservation_id)?;
                if let Some(binding) = &binding {
                    self.validate_binding_against_intent(binding, intent)?;
                }
                self.require_exact_observed(
                    intent,
                    &ServiceObservedState {
                        replica: ServiceReplicaKey::new(
                            intent.service_name.clone(),
                            intent.replica_index,
                        )?,
                        applied_config_digest: None,
                        phase: ServicePhase::Failed,
                        container_id: binding
                            .as_ref()
                            .map(|binding| binding.ownership.container_id.clone()),
                        failed_create_ownership: binding.map(|binding| binding.ownership),
                        last_error: intent.last_error.clone(),
                        ready: false,
                    },
                )
            }
            StackContainerCreateStatus::Cleaned => self.require_exact_observed(
                intent,
                &ServiceObservedState {
                    replica: ServiceReplicaKey::new(
                        intent.service_name.clone(),
                        intent.replica_index,
                    )?,
                    applied_config_digest: None,
                    phase: ServicePhase::Stopped,
                    container_id: None,
                    failed_create_ownership: None,
                    last_error: None,
                    ready: false,
                },
            ),
            StackContainerCreateStatus::Failed => self.require_exact_observed(
                intent,
                &ServiceObservedState {
                    replica: ServiceReplicaKey::new(
                        intent.service_name.clone(),
                        intent.replica_index,
                    )?,
                    applied_config_digest: None,
                    phase: ServicePhase::Failed,
                    container_id: None,
                    failed_create_ownership: None,
                    last_error: intent.last_error.clone(),
                    ready: false,
                },
            ),
        }
    }
}

/// The complete admission table for a fresh Action-v3 claim. A durable binding
/// is historical proof after `Cleaned`, so a create successor must preserve it;
/// `Failed` and `Intent` are strictly unbound, while `Reserved` and later
/// ownership-bearing states remain bound.
pub(super) fn legal_fresh_claim_predecessor(
    action: &Action,
    status: StackContainerCreateStatus,
    bound: bool,
) -> bool {
    if !status_binding_is_structurally_valid(status, bound) {
        return false;
    }
    match action {
        Action::ServiceCreate { .. } => match status {
            StackContainerCreateStatus::Blocked => true,
            StackContainerCreateStatus::Cleaned | StackContainerCreateStatus::Failed => true,
            StackContainerCreateStatus::Intent
            | StackContainerCreateStatus::Reserved
            | StackContainerCreateStatus::Running
            | StackContainerCreateStatus::CleanupPending => false,
        },
        Action::ServiceRecreate { .. } => status == StackContainerCreateStatus::Running,
        Action::ServiceRemove { .. } => match status {
            StackContainerCreateStatus::Intent
            | StackContainerCreateStatus::Reserved
            | StackContainerCreateStatus::Running => true,
            StackContainerCreateStatus::Blocked => true,
            StackContainerCreateStatus::Failed => true,
            StackContainerCreateStatus::CleanupPending | StackContainerCreateStatus::Cleaned => {
                false
            }
        },
    }
}

pub(super) fn status_binding_is_structurally_valid(
    status: StackContainerCreateStatus,
    bound: bool,
) -> bool {
    match status {
        StackContainerCreateStatus::Intent | StackContainerCreateStatus::Failed => !bound,
        StackContainerCreateStatus::Reserved
        | StackContainerCreateStatus::Running
        | StackContainerCreateStatus::CleanupPending
        | StackContainerCreateStatus::Cleaned => bound,
        StackContainerCreateStatus::Blocked => true,
    }
}

fn creating_observed_state(
    intent: &StackContainerCreateIntent,
) -> Result<ServiceObservedState, StackError> {
    Ok(ServiceObservedState {
        replica: replica_key(intent)?,
        applied_config_digest: None,
        phase: ServicePhase::Creating,
        container_id: None,
        failed_create_ownership: None,
        last_error: None,
        ready: false,
    })
}

fn replica_key(intent: &StackContainerCreateIntent) -> Result<ServiceReplicaKey, StackError> {
    ServiceReplicaKey::new(intent.service_name.clone(), intent.replica_index)
}

fn deterministic_reservation_id(
    selector: &StackContainerCreateSelector,
    service_generation: u64,
) -> String {
    let mut digest = Sha256::new();
    fn frame(digest: &mut Sha256, field: &[u8]) {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    frame(&mut digest, b"vz-stack-create-reservation-v1");
    frame(&mut digest, selector.project_id.as_str().as_bytes());
    frame(&mut digest, selector.environment_id.as_str().as_bytes());
    frame(&mut digest, selector.machine_id.as_str().as_bytes());
    frame(
        &mut digest,
        selector.machine_incarnation_id.as_str().as_bytes(),
    );
    frame(&mut digest, selector.stack_id.as_bytes());
    frame(&mut digest, selector.service_name.as_bytes());
    frame(&mut digest, &selector.replica_index.to_be_bytes());
    frame(&mut digest, &selector.environment_generation.to_be_bytes());
    frame(&mut digest, &service_generation.to_be_bytes());
    frame(&mut digest, selector.requested_container_id.as_bytes());
    frame(&mut digest, selector.definition_digest.as_bytes());
    frame(&mut digest, selector.action_digest.as_bytes());
    frame(&mut digest, selector.applied_config_digest.as_bytes());
    let bytes = digest.finalize();
    let mut encoded = String::with_capacity(78);
    encoded.push_str("vzscr1-sha256:");
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn sqlite_u64(value: u64, field: &str) -> Result<i64, StackError> {
    i64::try_from(value)
        .map_err(|_| StackError::InvalidSpec(format!("{field} exceeds SQLite INTEGER range")))
}

fn workload_scope_for_intent(
    intent: &StackContainerCreateIntent,
) -> Result<MachineWorkloadScope, StackError> {
    let machine_incarnation_id = intent.scope.machine_incarnation_id.clone().ok_or_else(|| {
        StackError::InvalidSpec("journal intent has no Machine incarnation".into())
    })?;
    let scope = MachineWorkloadScope {
        schema_version: MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
        project_id: intent.scope.project_id.clone(),
        environment_id: intent.scope.environment_id.clone(),
        machine_id: intent.scope.machine_id.clone(),
        machine_incarnation_id,
        stack_id: intent.scope.stack_id.clone(),
    };
    scope.validate().map_err(StackError::InvalidSpec)?;
    Ok(scope)
}

fn persisted_u64(field: &str, value: i64) -> Result<u64, StackError> {
    u64::try_from(value)
        .map_err(|_| StackError::InvalidSpec(format!("persisted {field} is negative")))
}

fn validate_text(field: &str, value: &str) -> Result<(), StackError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 128 {
        return invalid(format!("{field} must contain 1..=128 non-blank bytes"));
    }
    Ok(())
}

fn validate_digest(field: &str, value: &str) -> Result<(), StackError> {
    if value.trim().is_empty() {
        return invalid(format!("{field} must not be blank"));
    }
    Ok(())
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
        invalid(format!(
            "persisted {table} `{key}` has mismatched `{field}` projection"
        ))
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T, StackError> {
    Err(StackError::InvalidSpec(message.into()))
}

fn conflict<T>(message: impl Into<String>) -> Result<T, StackError> {
    Err(StackError::Machine {
        code: MachineErrorCode::StateConflict,
        message: message.into(),
    })
}
