//! Stack reconciliation: diff planner and ordered executor.
//!
//! The [`apply`] function compares desired [`StackSpec`] against
//! observed state, computes a deterministic action plan, and
//! persists all state transitions. Actions are ordered by service
//! dependency graph (topological sort with name-based tie-break).

use std::collections::{HashMap, HashSet, VecDeque};

use sha2::{Digest, Sha256};
use vz_runtime_contract::{ContainerGenerationOwnership, MachineWorkloadScope};

use crate::error::StackError;
use crate::events::StackEvent;
use crate::health::{DependencyCheck, HealthStatus, check_dependencies};
use crate::spec::{ServiceSpec, StackSpec};
use crate::state_store::{ServiceObservedState, ServicePhase, ServiceReplicaKey, StateStore};

/// Compute a deterministic digest of all config-affecting fields for a service.
///
/// The versioned digest covers the canonical normalized service activation
/// projection. Replica count is topology, not per-replica runtime config.
mod planning;
mod topo;

use self::planning::compute_actions_with_mount_digests;
pub use self::planning::service_config_digest;

#[cfg(test)]
mod tests;

/// A reconciliation action to converge observed state toward desired state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Create and start a new service.
    ServiceCreate {
        /// Exact service replica.
        target: ServiceReplicaKey,
        /// Immutable topology and predecessor state observed by the planner.
        precondition: ReplicaPrecondition,
    },
    /// Recreate a service whose configuration changed.
    ServiceRecreate {
        /// Exact service replica.
        target: ServiceReplicaKey,
        /// Immutable topology and predecessor state observed by the planner.
        precondition: ReplicaPrecondition,
    },
    /// Remove a service that is no longer in the desired spec.
    ServiceRemove {
        /// Exact service replica.
        target: ServiceReplicaKey,
        /// Immutable topology and predecessor state observed by the planner.
        precondition: ReplicaPrecondition,
    },
}

/// Caller intent for one exact service-replica lifecycle action.
///
/// This enum deliberately carries no predecessor evidence. [`StateStore`]
/// captures that evidence from its authoritative snapshot before returning an
/// executable [`Action`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetedActionKind {
    /// Create a missing or stopped exact replica.
    Create,
    /// Replace an existing exact replica.
    Recreate,
    /// Remove an existing exact replica.
    Remove,
}

/// Effect-free action kind and target before StateStore attaches authority.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ActionDraft {
    Create {
        target: ServiceReplicaKey,
        observed: Option<ServiceObservedState>,
    },
    Recreate {
        target: ServiceReplicaKey,
        observed: ServiceObservedState,
    },
    Remove {
        target: ServiceReplicaKey,
        observed: ServiceObservedState,
    },
}

impl ActionDraft {
    fn service_name(&self) -> &str {
        &self.target().service_name
    }

    pub(crate) fn target(&self) -> &ServiceReplicaKey {
        match self {
            Self::Create { target, .. }
            | Self::Recreate { target, .. }
            | Self::Remove { target, .. } => target,
        }
    }

    pub(crate) fn observed(&self) -> Option<&ServiceObservedState> {
        match self {
            Self::Create { observed, .. } => observed.as_ref(),
            Self::Recreate { observed, .. } | Self::Remove { observed, .. } => Some(observed),
        }
    }

    pub(crate) fn into_action(self, precondition: ReplicaPrecondition) -> Action {
        match self {
            Self::Create { target, .. } => Action::ServiceCreate {
                target,
                precondition,
            },
            Self::Recreate { target, .. } => Action::ServiceRecreate {
                target,
                precondition,
            },
            Self::Remove { target, .. } => Action::ServiceRemove {
                target,
                precondition,
            },
        }
    }
}

pub(crate) fn attach_action_preconditions(
    stack_id: &str,
    store: &StateStore,
    drafts: Vec<ActionDraft>,
) -> Result<Vec<Action>, StackError> {
    let preconditions = store.capture_action_preconditions(stack_id, &drafts)?;
    Ok(drafts
        .into_iter()
        .zip(preconditions)
        .map(|(draft, precondition)| draft.into_action(precondition))
        .collect())
}

impl StateStore {
    /// Plan one exact targeted action and attach authoritative predecessor evidence.
    ///
    /// The caller supplies only lifecycle intent and an exact replica key. The
    /// observed state and journal head are loaded and validated by the store in
    /// the same snapshot used to capture the action precondition.
    pub fn plan_targeted_action(
        &self,
        stack_id: &str,
        kind: TargetedActionKind,
        target: ServiceReplicaKey,
    ) -> Result<Action, StackError> {
        target.validate()?;
        let observed =
            self.load_observed_state_for_replica(stack_id, &target.service_name, target.index())?;
        let draft = match kind {
            TargetedActionKind::Create => ActionDraft::Create { target, observed },
            TargetedActionKind::Recreate => ActionDraft::Recreate {
                target: target.clone(),
                observed: observed.ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "cannot recreate missing replica `{}`",
                        target.display_name()
                    ))
                })?,
            },
            TargetedActionKind::Remove => ActionDraft::Remove {
                target: target.clone(),
                observed: observed.ok_or_else(|| {
                    StackError::InvalidSpec(format!(
                        "cannot remove missing replica `{}`",
                        target.display_name()
                    ))
                })?,
            },
        };
        let mut actions = attach_action_preconditions(stack_id, self, vec![draft])?;
        actions.pop().ok_or_else(|| {
            StackError::InvalidSpec("targeted action planning returned no action".to_string())
        })
    }
}

#[cfg(test)]
pub(crate) fn test_replica_precondition() -> ReplicaPrecondition {
    TEST_ACTION_STACK.with(|stack_id| test_replica_precondition_for_stack(&stack_id.borrow()))
}

#[cfg(test)]
std::thread_local! {
    static TEST_ACTION_STACK: std::cell::RefCell<String> =
        std::cell::RefCell::new("myapp".to_string());
}

#[cfg(test)]
pub(crate) fn set_test_action_stack(stack_id: &str) {
    TEST_ACTION_STACK.with(|current| *current.borrow_mut() = stack_id.to_string());
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test-only fixture helper")]
pub(crate) fn install_test_planning_authority(store: &StateStore, stack_id: &str) {
    use vz_runtime_contract::{
        Architecture, CapabilitySet, EnvironmentId, EnvironmentInstance, EnvironmentSpec,
        EnvironmentState, MachineCapability, MachineId, MachineIncarnation, MachineIncarnationId,
        MachineInstance, MachineProfile, MachineResources, MachineSpec, MachineState,
        OperatingSystem, OwnedResourceKind, OwnershipRecord, ProjectDefinition, ProjectId,
        ProjectState, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
    };

    if store.load_stack_workload_owner(stack_id).unwrap().is_some() {
        return;
    }
    let project_id = ProjectId::new("prj_stack_unit_fixture").unwrap();
    let environment_id = EnvironmentId::new("env_stack_unit_fixture").unwrap();
    let machine_id = MachineId::new("mch_stack_unit_fixture").unwrap();
    let incarnation_id = MachineIncarnationId::new("inc_stack_unit_fixture").unwrap();
    if store
        .load_project_state(project_id.as_str())
        .unwrap()
        .is_none()
    {
        let capabilities = CapabilitySet::new([
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ]);
        let target = TargetSpec {
            os: OperatingSystem::Linux,
            arch: Architecture::Aarch64,
            image: "fixture:latest".to_string(),
            version: None,
            channel: None,
            digest: Some("sha256:stack-unit-fixture".to_string()),
        };
        let definition = ProjectDefinition {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: project_id.clone(),
            name: "stack-unit-fixture".to_string(),
            environment: EnvironmentSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                default_machine: None,
                machines: vec![MachineSpec {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    name: "linux".to_string(),
                    profile: MachineProfile::Developer,
                    target: target.clone(),
                    resources: MachineResources::default(),
                    requested_capabilities: capabilities.clone(),
                    workspace: None,
                }],
                networks: vec![],
                endpoints: vec![],
            },
        };
        let environment = EnvironmentInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            environment_id: environment_id.clone(),
            project_id: project_id.clone(),
            name: "test".to_string(),
            definition_digest: definition.digest().unwrap(),
            state: EnvironmentState::Ready,
            lifecycle_generation: 0,
            active_operation_id: None,
            bindings: vec![],
            machines: vec![MachineInstance {
                docker_context: None,
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                machine_id: machine_id.clone(),
                environment_id: environment_id.clone(),
                name: "linux".to_string(),
                profile: MachineProfile::Developer,
                target,
                resources: MachineResources::default(),
                requested_capabilities: capabilities.clone(),
                negotiated_capabilities: capabilities,
                backend: None,
                incarnation: Some(MachineIncarnation {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    incarnation_id: incarnation_id.clone(),
                    machine_id: machine_id.clone(),
                    generation: 1,
                    created_at: 1,
                }),
                state: MachineState::Ready,
                runtime_identity: None,
                legacy_sandbox_id: None,
            }],
            networks: vec![],
            endpoints: vec![],
            ownership: vec![
                OwnershipRecord {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    resource_kind: OwnedResourceKind::Incarnation,
                    resource_id: incarnation_id.to_string(),
                    environment_id: environment_id.clone(),
                    machine_id: Some(machine_id.clone()),
                },
                OwnershipRecord {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    resource_kind: OwnedResourceKind::Machine,
                    resource_id: machine_id.to_string(),
                    environment_id: environment_id.clone(),
                    machine_id: Some(machine_id.clone()),
                },
            ],
            legacy_migration: None,
            created_at: 1,
            updated_at: 1,
        };
        store
            .save_project_state(&ProjectState {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                definition,
                environments: vec![environment],
            })
            .unwrap();
    }
    store
        .reserve_stack_workload_owner(
            &MachineWorkloadScope {
                schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
                project_id,
                environment_id,
                machine_id,
                machine_incarnation_id: incarnation_id,
                stack_id: stack_id.to_string(),
            },
            1,
        )
        .unwrap();
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test-only fixture helper")]
pub(crate) fn test_planning_scope(store: &StateStore, stack_id: &str) -> MachineWorkloadScope {
    install_test_planning_authority(store, stack_id);
    let owner = store.load_stack_workload_owner(stack_id).unwrap().unwrap();
    let project = store
        .load_project_state(owner.project_id.as_str())
        .unwrap()
        .unwrap();
    let environment = project
        .environments
        .iter()
        .find(|candidate| candidate.environment_id == owner.environment_id)
        .unwrap();
    let machine = environment
        .machines
        .iter()
        .find(|candidate| candidate.machine_id == owner.machine_id)
        .unwrap();
    MachineWorkloadScope {
        schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
        project_id: owner.project_id,
        environment_id: owner.environment_id,
        machine_id: owner.machine_id,
        machine_incarnation_id: machine.incarnation.as_ref().unwrap().incarnation_id.clone(),
        stack_id: stack_id.to_string(),
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test-only fixture helper")]
fn test_create_selector(
    store: &StateStore,
    stack_id: &str,
    target: &ServiceReplicaKey,
    applied_config_digest: &str,
) -> crate::state_store::StackContainerCreateSelector {
    install_test_planning_authority(store, stack_id);
    let owner = store.load_stack_workload_owner(stack_id).unwrap().unwrap();
    let project = store
        .load_project_state(owner.project_id.as_str())
        .unwrap()
        .unwrap();
    let environment = project
        .environments
        .iter()
        .find(|candidate| candidate.environment_id == owner.environment_id)
        .unwrap();
    let machine = environment
        .machines
        .iter()
        .find(|candidate| candidate.machine_id == owner.machine_id)
        .unwrap();
    crate::state_store::StackContainerCreateSelector {
        project_id: owner.project_id,
        environment_id: owner.environment_id,
        machine_id: owner.machine_id,
        machine_incarnation_id: machine.incarnation.as_ref().unwrap().incarnation_id.clone(),
        environment_generation: environment.lifecycle_generation,
        stack_id: stack_id.to_string(),
        service_name: target.service_name.clone(),
        replica_index: target.index(),
        requested_container_id: format!(
            "ctr-test-{stack_id}-{}-{}",
            target.service_name,
            target.index()
        ),
        definition_digest: environment.definition_digest.clone(),
        action_digest: format!(
            "test-action-{stack_id}-{}-{}",
            target.service_name,
            target.index()
        ),
        applied_config_digest: applied_config_digest.to_string(),
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test-only fixture helper")]
pub(crate) fn begin_test_container_create(
    store: &StateStore,
    stack_id: &str,
    target: &ServiceReplicaKey,
    applied_config_digest: &str,
) -> crate::state_store::StackContainerCreateIntent {
    store
        .resolve_or_begin_stack_container_create(
            &test_create_selector(store, stack_id, target, applied_config_digest),
            10,
        )
        .unwrap()
        .0
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test-only fixture helper")]
pub(crate) fn publish_test_container_unbound_failure(
    store: &StateStore,
    stack_id: &str,
    target: &ServiceReplicaKey,
    applied_config_digest: &str,
) -> ServiceObservedState {
    let intent = begin_test_container_create(store, stack_id, target, applied_config_digest);
    store
        .publish_stack_container_create_failure(
            &intent.scope.reservation_id,
            "test activation was interrupted",
            11,
        )
        .unwrap()
}

#[cfg(test)]
pub(crate) fn publish_test_container_running(
    store: &StateStore,
    stack_id: &str,
    target: &ServiceReplicaKey,
    applied_config_digest: &str,
) -> ServiceObservedState {
    publish_test_container_running_with_ready(store, stack_id, target, applied_config_digest, true)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test-only fixture helper")]
pub(crate) fn publish_test_container_running_with_ready(
    store: &StateStore,
    stack_id: &str,
    target: &ServiceReplicaKey,
    applied_config_digest: &str,
    ready: bool,
) -> ServiceObservedState {
    let (intent, existing_binding) = store
        .resolve_or_begin_stack_container_create(
            &test_create_selector(store, stack_id, target, applied_config_digest),
            10,
        )
        .unwrap();
    if existing_binding.is_none() {
        let ownership = ContainerGenerationOwnership {
            container_id: intent.requested_container_id.clone(),
            generation: intent.service_generation,
            stack_id: stack_id.to_string(),
            scope: Some(Box::new(intent.scope.clone())),
        };
        store
            .bind_stack_container_generation(&crate::state_store::StackContainerGenerationBinding {
                reservation_id: intent.scope.reservation_id.clone(),
                service_name: target.service_name.clone(),
                ownership,
                bound_at: 11,
            })
            .unwrap();
    }
    store
        .publish_stack_container_create_success(&intent.scope.reservation_id, ready, 12)
        .unwrap()
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test-only fixture helper")]
pub(crate) fn test_replica_precondition_for_stack(stack_id: &str) -> ReplicaPrecondition {
    let workload = MachineWorkloadScope {
        schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
        project_id: vz_runtime_contract::ProjectId::new("prj_action_test").unwrap(),
        environment_id: vz_runtime_contract::EnvironmentId::new("env_action_test").unwrap(),
        machine_id: vz_runtime_contract::MachineId::new("mch_action_test").unwrap(),
        machine_incarnation_id: vz_runtime_contract::MachineIncarnationId::new("inc_action_test")
            .unwrap(),
        stack_id: stack_id.to_string(),
    };
    let scope = workload
        .container_generation_scope("reservation-action-test")
        .unwrap();
    ReplicaPrecondition::new(
        workload,
        0,
        ExpectedJournalHead::exact(
            "reservation-action-test",
            1,
            Some(ContainerGenerationOwnership {
                container_id: "ctr-action-test".to_string(),
                generation: 1,
                stack_id: stack_id.to_string(),
                scope: Some(Box::new(scope)),
            }),
        )
        .unwrap(),
    )
    .unwrap()
}

/// Immutable state that must still precede one planned replica action.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaPrecondition {
    workload: MachineWorkloadScope,
    environment_generation: u64,
    journal_head: ExpectedJournalHead,
}

/// Exact latest journal state observed when an action was planned.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "state", deny_unknown_fields)]
pub enum ExpectedJournalHead {
    /// The exact workload and replica have never had a journal generation.
    NeverJournaled,
    /// The exact immutable journal head and optional runtime generation binding.
    Exact {
        reservation_id: String,
        service_generation: u64,
        ownership: Option<ContainerGenerationOwnership>,
    },
}

impl<'de> serde::Deserialize<'de> for ReplicaPrecondition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WirePrecondition {
            workload: WireWorkload,
            environment_generation: u64,
            journal_head: ExpectedJournalHead,
        }

        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireWorkload {
            schema_version: u32,
            project_id: vz_runtime_contract::ProjectId,
            environment_id: vz_runtime_contract::EnvironmentId,
            machine_id: vz_runtime_contract::MachineId,
            machine_incarnation_id: vz_runtime_contract::MachineIncarnationId,
            stack_id: String,
        }

        let wire = WirePrecondition::deserialize(deserializer)?;
        Self::new(
            MachineWorkloadScope {
                schema_version: wire.workload.schema_version,
                project_id: wire.workload.project_id,
                environment_id: wire.workload.environment_id,
                machine_id: wire.workload.machine_id,
                machine_incarnation_id: wire.workload.machine_incarnation_id,
                stack_id: wire.workload.stack_id,
            },
            wire.environment_generation,
            wire.journal_head,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl<'de> serde::Deserialize<'de> for ExpectedJournalHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "snake_case", tag = "state", deny_unknown_fields)]
        enum WireJournalHead {
            NeverJournaled {},
            Exact {
                reservation_id: String,
                service_generation: u64,
                ownership: Option<ContainerGenerationOwnership>,
            },
        }

        let head = match WireJournalHead::deserialize(deserializer)? {
            WireJournalHead::NeverJournaled {} => Self::NeverJournaled,
            WireJournalHead::Exact {
                reservation_id,
                service_generation,
                ownership,
            } => Self::Exact {
                reservation_id,
                service_generation,
                ownership,
            },
        };
        head.validate_identity().map_err(serde::de::Error::custom)?;
        Ok(head)
    }
}

impl ReplicaPrecondition {
    /// Construct and validate an exact planned-replica precondition.
    pub fn new(
        workload: MachineWorkloadScope,
        environment_generation: u64,
        journal_head: ExpectedJournalHead,
    ) -> Result<Self, StackError> {
        workload.validate().map_err(StackError::InvalidSpec)?;
        journal_head.validate_against(&workload)?;
        Ok(Self {
            workload,
            environment_generation,
            journal_head,
        })
    }

    /// Exact topology scope observed by the planner.
    pub fn workload(&self) -> &MachineWorkloadScope {
        &self.workload
    }

    /// Environment lifecycle generation observed by the planner.
    pub fn environment_generation(&self) -> u64 {
        self.environment_generation
    }

    /// Exact latest journal state observed by the planner.
    pub fn journal_head(&self) -> &ExpectedJournalHead {
        &self.journal_head
    }

    fn validate(&self) -> Result<(), StackError> {
        self.workload.validate().map_err(StackError::InvalidSpec)?;
        self.journal_head.validate_against(&self.workload)
    }
}

impl ExpectedJournalHead {
    /// Construct and validate an exact journal-head identity.
    pub fn exact(
        reservation_id: impl Into<String>,
        service_generation: u64,
        ownership: Option<ContainerGenerationOwnership>,
    ) -> Result<Self, StackError> {
        let head = Self::Exact {
            reservation_id: reservation_id.into(),
            service_generation,
            ownership,
        };
        head.validate_identity()?;
        Ok(head)
    }

    fn validate_identity(&self) -> Result<(), StackError> {
        if let Self::Exact {
            reservation_id,
            service_generation,
            ownership,
        } = self
        {
            if reservation_id.is_empty()
                || reservation_id.len() > 128
                || reservation_id
                    .chars()
                    .any(|value| value.is_whitespace() || value.is_control() || value == '\0')
            {
                return Err(StackError::InvalidSpec(
                    "replica precondition reservation_id must contain 1..=128 bytes without whitespace or control characters"
                        .to_string(),
                ));
            }
            if *service_generation == 0 {
                return Err(StackError::InvalidSpec(
                    "replica precondition service_generation must be non-zero".to_string(),
                ));
            }
            if let Some(ownership) = ownership {
                ownership.validate().map_err(StackError::InvalidSpec)?;
                let scope = ownership.scope.as_deref().ok_or_else(|| {
                    StackError::InvalidSpec(
                        "replica precondition ownership is missing exact scope".to_string(),
                    )
                })?;
                if scope.machine_incarnation_id.is_none() {
                    return Err(StackError::InvalidSpec(
                        "replica precondition ownership is missing exact machine incarnation"
                            .to_string(),
                    ));
                }
                if scope.reservation_id != reservation_id.as_str() {
                    return Err(StackError::InvalidSpec(
                        "replica precondition reservation disagrees with runtime ownership"
                            .to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_against(&self, workload: &MachineWorkloadScope) -> Result<(), StackError> {
        self.validate_identity()?;
        let Self::Exact {
            ownership: Some(ownership),
            ..
        } = self
        else {
            return Ok(());
        };
        let scope = ownership.scope.as_deref().ok_or_else(|| {
            StackError::InvalidSpec(
                "replica precondition ownership is missing exact scope".to_string(),
            )
        })?;
        if scope.project_id != workload.project_id
            || scope.environment_id != workload.environment_id
            || scope.machine_id != workload.machine_id
            || scope.stack_id != workload.stack_id
        {
            return Err(StackError::InvalidSpec(
                "replica precondition runtime ownership disagrees with workload scope".to_string(),
            ));
        }
        Ok(())
    }
}

impl Action {
    /// Service name this action targets.
    pub fn service_name(&self) -> &str {
        match self {
            Self::ServiceCreate { target, .. }
            | Self::ServiceRecreate { target, .. }
            | Self::ServiceRemove { target, .. } => &target.service_name,
        }
    }

    /// Exact replica targeted by this action.
    pub fn target(&self) -> &ServiceReplicaKey {
        match self {
            Self::ServiceCreate { target, .. }
            | Self::ServiceRecreate { target, .. }
            | Self::ServiceRemove { target, .. } => target,
        }
    }

    /// Immutable topology and predecessor state required by this action.
    pub fn precondition(&self) -> &ReplicaPrecondition {
        match self {
            Self::ServiceCreate { precondition, .. }
            | Self::ServiceRecreate { precondition, .. }
            | Self::ServiceRemove { precondition, .. } => precondition,
        }
    }

    /// Validate all persisted action identity.
    pub fn validate(&self) -> Result<(), StackError> {
        self.target().validate()?;
        self.precondition().validate()?;
        match self {
            Self::ServiceCreate { .. } => Ok(()),
            Self::ServiceRecreate { precondition, .. }
                if matches!(
                    precondition.journal_head(),
                    ExpectedJournalHead::Exact {
                        ownership: Some(_),
                        ..
                    }
                ) =>
            {
                Ok(())
            }
            Self::ServiceRecreate { .. } => Err(StackError::InvalidSpec(
                "service recreate requires an exact bound predecessor".to_string(),
            )),
            Self::ServiceRemove { precondition, .. }
                if matches!(
                    precondition.journal_head(),
                    ExpectedJournalHead::Exact { .. }
                ) =>
            {
                Ok(())
            }
            Self::ServiceRemove { .. } => Err(StackError::InvalidSpec(
                "service remove requires an exact journal predecessor".to_string(),
            )),
        }
    }
}

/// Canonical identity shared by durable started claims and scoped execution
/// payloads. This is an identity value, not mutation authority; only an opaque
/// [`crate::state_store::ReconcileActionClaim`] proves admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReconcileActionExecutionKey {
    session_id: String,
    operation_id: String,
    absolute_action_index: usize,
    action_hash: String,
    action_kind: &'static str,
    target: ServiceReplicaKey,
}

impl ReconcileActionExecutionKey {
    pub(crate) fn new(
        session_id: &str,
        operation_id: &str,
        absolute_action_index: usize,
        action: &Action,
    ) -> Result<Self, StackError> {
        action.validate()?;
        if session_id.trim().is_empty() || operation_id.trim().is_empty() {
            return Err(StackError::InvalidSpec(
                "reconcile action execution identity requires non-blank session and operation IDs"
                    .to_string(),
            ));
        }
        let action_kind = match action {
            Action::ServiceCreate { .. } => "create",
            Action::ServiceRecreate { .. } => "recreate",
            Action::ServiceRemove { .. } => "remove",
        };
        Ok(Self {
            session_id: session_id.to_string(),
            operation_id: operation_id.to_string(),
            absolute_action_index,
            action_hash: compute_actions_hash(std::slice::from_ref(action)),
            action_kind,
            target: action.target().clone(),
        })
    }

    pub(crate) fn activation_digest_prefix(&self) -> Result<String, StackError> {
        fn hash_field(hasher: &mut Sha256, name: &[u8], value: &[u8]) {
            hasher.update((name.len() as u64).to_le_bytes());
            hasher.update(name);
            hasher.update((value.len() as u64).to_le_bytes());
            hasher.update(value);
        }

        let action_index = u64::try_from(self.absolute_action_index)
            .map_err(|_| StackError::InvalidSpec("action index exceeds u64".to_string()))?;
        let mut hasher = Sha256::new();
        hash_field(&mut hasher, b"schema", b"vz.stack.action-identity.v3");
        hash_field(&mut hasher, b"session_id", self.session_id.as_bytes());
        hash_field(&mut hasher, b"operation_id", self.operation_id.as_bytes());
        hash_field(
            &mut hasher,
            b"absolute_action_index",
            &action_index.to_le_bytes(),
        );
        hash_field(&mut hasher, b"action_kind", self.action_kind.as_bytes());
        hash_field(
            &mut hasher,
            b"service_name",
            self.target.service_name.as_bytes(),
        );
        hash_field(
            &mut hasher,
            b"replica_index",
            &self.target.index().to_le_bytes(),
        );
        hash_field(&mut hasher, b"action_hash", self.action_hash.as_bytes());
        Ok(format!("vzsad3:{:x}:", hasher.finalize()))
    }

    /// Match the complete persisted activation digest, including exactly one
    /// canonical lowercase SHA-256 payload component and no trailing bytes.
    pub(crate) fn matches_activation_digest(&self, candidate: &str) -> Result<bool, StackError> {
        let prefix = self.activation_digest_prefix()?;
        let Some(payload) = candidate.strip_prefix(&prefix) else {
            return Ok(false);
        };
        Ok(payload.len() == 64
            && payload
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn operation_id(&self) -> &str {
        &self.operation_id
    }

    pub(crate) fn absolute_action_index(&self) -> usize {
        self.absolute_action_index
    }

    pub(crate) fn matches_action(&self, action: &Action) -> Result<bool, StackError> {
        Ok(Self::new(
            &self.session_id,
            &self.operation_id,
            self.absolute_action_index,
            action,
        )? == *self)
    }
}

/// Compute a deterministic hash of an action list for identity tracking.
///
/// The versioned, length-framed digest covers action order, kind, exact target,
/// topology authority, journal generation, and complete runtime ownership.
pub fn compute_actions_hash(actions: &[Action]) -> String {
    fn frame(hasher: &mut Sha256, value: &[u8]) {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }

    fn frame_u32(hasher: &mut Sha256, value: u32) {
        frame(hasher, &value.to_be_bytes());
    }

    fn frame_u64(hasher: &mut Sha256, value: u64) {
        frame(hasher, &value.to_be_bytes());
    }

    let mut hasher = Sha256::new();
    frame(&mut hasher, b"vz-reconcile-actions-v2");
    frame(&mut hasher, &(actions.len() as u64).to_be_bytes());
    for action in actions {
        let (kind, target, precondition) = match action {
            Action::ServiceCreate {
                target,
                precondition,
            } => (b"create".as_slice(), target, precondition),
            Action::ServiceRecreate {
                target,
                precondition,
            } => (b"recreate".as_slice(), target, precondition),
            Action::ServiceRemove {
                target,
                precondition,
            } => (b"remove".as_slice(), target, precondition),
        };
        frame(&mut hasher, kind);
        frame(&mut hasher, target.service_name.as_bytes());
        frame(&mut hasher, &target.index().to_be_bytes());
        let workload = precondition.workload();
        frame_u32(&mut hasher, workload.schema_version);
        frame(&mut hasher, workload.project_id.as_str().as_bytes());
        frame(&mut hasher, workload.environment_id.as_str().as_bytes());
        frame(&mut hasher, workload.machine_id.as_str().as_bytes());
        frame(
            &mut hasher,
            workload.machine_incarnation_id.as_str().as_bytes(),
        );
        frame(&mut hasher, workload.stack_id.as_bytes());
        frame_u64(&mut hasher, precondition.environment_generation());
        match precondition.journal_head() {
            ExpectedJournalHead::NeverJournaled => frame(&mut hasher, b"never-journaled"),
            ExpectedJournalHead::Exact {
                reservation_id,
                service_generation,
                ownership,
            } => {
                frame(&mut hasher, b"exact");
                frame(&mut hasher, reservation_id.as_bytes());
                frame_u64(&mut hasher, *service_generation);
                match ownership {
                    None => frame(&mut hasher, b"unbound"),
                    Some(ownership) => {
                        frame(&mut hasher, b"bound");
                        frame(&mut hasher, ownership.container_id.as_bytes());
                        frame_u64(&mut hasher, ownership.generation);
                        frame(&mut hasher, ownership.stack_id.as_bytes());
                        match ownership.scope.as_deref() {
                            None => frame(&mut hasher, b"scope-absent"),
                            Some(scope) => {
                                frame(&mut hasher, b"scope-present");
                                frame(&mut hasher, scope.reservation_id.as_bytes());
                                frame(&mut hasher, scope.project_id.as_str().as_bytes());
                                frame(&mut hasher, scope.environment_id.as_str().as_bytes());
                                frame(&mut hasher, scope.machine_id.as_str().as_bytes());
                                match scope.machine_incarnation_id.as_ref() {
                                    None => frame(&mut hasher, b"incarnation-absent"),
                                    Some(incarnation) => {
                                        frame(&mut hasher, b"incarnation-present");
                                        frame(&mut hasher, incarnation.as_str().as_bytes());
                                    }
                                }
                                frame(&mut hasher, scope.stack_id.as_bytes());
                            }
                        }
                    }
                }
            }
        }
    }
    format!("vzrah2-sha256:{:x}", hasher.finalize())
}

/// Result of an [`apply`] call.
#[derive(Debug, Clone, Default)]
pub struct ApplyResult {
    /// Actions that were planned (and would be executed by a real runtime).
    ///
    /// This is the reconciler's explicit convergence claim for the round:
    /// if this list is empty and no services are deferred, reconcile has no
    /// further work for the current desired/observed state.
    pub actions: Vec<Action>,
    /// Services deferred because their dependencies are not ready.
    ///
    /// Deferred services are part of the convergence claim and must be empty
    /// before the orchestrator can declare the stack converged.
    pub deferred: Vec<DeferredService>,
}

/// A service whose creation was deferred due to unready dependencies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredService {
    /// Service name that was deferred.
    pub service_name: String,
    /// Dependencies that are not yet ready.
    pub waiting_on: Vec<String>,
}

/// Compute an apply plan without persisting desired/observed state or events.
pub fn plan_apply(
    spec: &StackSpec,
    store: &StateStore,
    health_statuses: &HashMap<String, HealthStatus>,
) -> Result<ApplyResult, StackError> {
    let previous_desired = store.load_desired_state(&spec.name)?;
    let observed = store.load_observed_state(&spec.name)?;
    let stored_mount_digests = store.load_service_mount_digests(&spec.name)?;
    let (drafts, deferred) = compute_actions_with_mount_digests(
        &spec.services,
        &observed,
        health_statuses,
        previous_desired
            .as_ref()
            .map(|stack| stack.services.as_slice()),
        &stored_mount_digests,
    )?;
    let actions = attach_action_preconditions(&spec.name, store, drafts)?;
    Ok(ApplyResult { actions, deferred })
}

/// Persist desired state, compute action plan, and update observed state.
///
/// The reconciler:
/// 1. Persists the desired spec in the state store.
/// 2. Loads current observed state.
/// 3. Computes a deterministic, dependency-ordered action plan.
/// 4. Gates service creation on dependency readiness.
/// 5. Updates observed state for each action (create/remove).
/// 6. Emits lifecycle events for observability.
///
/// Services whose dependencies are not ready are deferred and
/// reported in [`ApplyResult::deferred`]. Re-applying the same
/// spec after dependencies become ready will create them. This makes
/// `apply` idempotent and restart-safe: convergence is driven by the
/// persisted desired/observed state and deterministic action planning.
pub fn apply(
    spec: &StackSpec,
    store: &StateStore,
    health_statuses: &HashMap<String, HealthStatus>,
) -> Result<ApplyResult, StackError> {
    // 1. Load previous desired state (for reverse-dep teardown ordering).
    let previous_desired = store.load_desired_state(&spec.name)?;
    let previous_config_digests: HashMap<String, String> = previous_desired
        .as_ref()
        .map(|stack| {
            stack
                .services
                .iter()
                .map(|svc| (svc.name.clone(), service_config_digest(svc)))
                .collect()
        })
        .unwrap_or_default();
    let desired_service_map: HashMap<&str, &ServiceSpec> = spec
        .services
        .iter()
        .map(|svc| (svc.name.as_str(), svc))
        .collect();

    // 2. Persist desired state.
    store.save_desired_state(&spec.name, spec)?;

    // 3. Emit start event.
    store.emit_event(
        &spec.name,
        &StackEvent::StackApplyStarted {
            stack_name: spec.name.clone(),
            services_count: spec.services.len(),
        },
    )?;

    // 4. Load current observed state.
    let observed = store.load_observed_state(&spec.name)?;
    let stored_mount_digests = store.load_service_mount_digests(&spec.name)?;

    // 5. Compute action plan with dependency gating.
    let (drafts, deferred) = compute_actions_with_mount_digests(
        &spec.services,
        &observed,
        health_statuses,
        previous_desired.as_ref().map(|s| s.services.as_slice()),
        &stored_mount_digests,
    )?;

    let actions = attach_action_preconditions(&spec.name, store, drafts)?;

    // 5. Emit events for deferred services.
    for d in &deferred {
        store.emit_event(
            &spec.name,
            &StackEvent::DependencyBlocked {
                stack_name: spec.name.clone(),
                service_name: d.service_name.clone(),
                waiting_on: d.waiting_on.clone(),
            },
        )?;
    }

    // 6. Execute action plan (update observed state).
    let mut succeeded = 0;
    let failed = 0;
    for action in &actions {
        match action {
            Action::ServiceCreate {
                target,
                precondition,
            } => {
                let service_name = &target.service_name;
                if matches!(
                    precondition.journal_head(),
                    ExpectedJournalHead::NeverJournaled
                ) {
                    if let Some(service) = desired_service_map.get(service_name.as_str()) {
                        let digest = service_config_digest(service);
                        store.save_service_mount_digest(&spec.name, service_name, &digest)?;
                    }
                    let failed_create_ownership = observed
                        .iter()
                        .find(|state| state.replica == *target)
                        .and_then(|state| state.failed_create_ownership.clone());
                    let existing_cid = observed
                        .iter()
                        .find(|state| state.replica == *target)
                        .and_then(|state| state.container_id.clone());
                    store.save_observed_state(
                        &spec.name,
                        &ServiceObservedState {
                            replica: target.clone(),
                            applied_config_digest: None,
                            phase: ServicePhase::Pending,
                            container_id: existing_cid,
                            failed_create_ownership,
                            last_error: None,
                            ready: false,
                        },
                    )?;
                    store.emit_event(
                        &spec.name,
                        &StackEvent::ServiceCreating {
                            stack_name: spec.name.clone(),
                            service_name: service_name.clone(),
                        },
                    )?;
                    succeeded += 1;
                }
            }
            Action::ServiceRecreate { target, .. } => {
                let service_name = &target.service_name;
                let desired_digest = desired_service_map
                    .get(service_name.as_str())
                    .map(|service| service_config_digest(service))
                    .unwrap_or_default();
                let previous_digest = stored_mount_digests
                    .get(service_name)
                    .cloned()
                    .or_else(|| previous_config_digests.get(service_name).cloned());
                store.emit_event(
                    &spec.name,
                    &StackEvent::MountTopologyRecreateRequired {
                        stack_name: spec.name.clone(),
                        service_name: service_name.clone(),
                        previous_digest,
                        desired_digest: desired_digest.clone(),
                    },
                )?;
            }
            Action::ServiceRemove { .. } => {}
        }
    }

    // 7. Emit completion event.
    store.emit_event(
        &spec.name,
        &StackEvent::StackApplyCompleted {
            stack_name: spec.name.clone(),
            succeeded,
            failed,
        },
    )?;

    Ok(ApplyResult { actions, deferred })
}
