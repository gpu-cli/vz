//! Cleanup authority for an exact registered restart that never reached Ready.
use super::*;
use vz_runtime_contract::{
    EnvironmentLifecycleStatus, LifecycleStepStatus, MachineIncarnation, MachineInstance,
    MachineRuntimeIdentity,
};

/// Created only while the original Up controller owns the registered activation.
/// Persisted only together with positive Delete quiescence for that activation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FailedUpRuntime {
    owner: ResourceOwner,
    operation_id: String,
    generation: u64,
    previous_incarnation: Option<MachineIncarnation>,
    previous_runtime_identity: Option<MachineRuntimeIdentity>,
    runtime_identity: StackRuntimeIdentity,
}

impl FailedUpRuntime {
    pub(super) fn capture(
        operation: &EnvironmentLifecycleOperation,
        machine: &MachineInstance,
        session: &Session,
    ) -> Result<Self, MachineLiveSessionError> {
        let proof = Self {
            owner: session.owner.clone(),
            operation_id: operation.operation_id.to_string(),
            generation: operation.generation,
            previous_incarnation: machine.incarnation.clone(),
            previous_runtime_identity: machine.runtime_identity.clone(),
            runtime_identity: session.identity.clone(),
        };
        proof.validate_journal(operation, false)?;
        proof.validate_binding(machine, &session.identity)?;
        Ok(proof)
    }

    fn validate_journal(
        &self,
        operation: &EnvironmentLifecycleOperation,
        terminal: bool,
    ) -> Result<(), MachineLiveSessionError> {
        operation.validate_structure().map_err(error)?;
        let step = operation
            .machine_steps
            .iter()
            .find(|step| self.owner.machine_id.as_ref() == Some(&step.machine_id))
            .ok_or_else(|| error("failed Up cleanup Machine absent from journal"))?;
        if operation.operation_id.as_str() != self.operation_id
            || operation.generation != self.generation
            || operation.project_id != self.owner.project_id
            || operation.environment_id != self.owner.environment_id
            || operation.kind != EnvironmentLifecycleKind::Up
            || (terminal && operation.status != EnvironmentLifecycleStatus::Failed)
            || step.status != LifecycleStepStatus::Failed
            || step.expected_incarnation != self.previous_incarnation
            || step.resulting_incarnation.is_some()
            || step.resulting_activation.is_some()
        {
            return Err(error("failed Up cleanup journal changed"));
        }
        Ok(())
    }

    fn validate_binding(
        &self,
        machine: &MachineInstance,
        identity: &StackRuntimeIdentity,
    ) -> Result<(), MachineLiveSessionError> {
        let reservation = MachineRuntimeEntry::<MacosRuntimeBackend>::vm_reservation(&self.owner)
            .map_err(error)?;
        if self.owner.machine_id.as_ref() != Some(&machine.machine_id)
            || self.owner.environment_id != machine.environment_id
            || machine.incarnation != self.previous_incarnation
            || machine.runtime_identity != self.previous_runtime_identity
            || identity != &self.runtime_identity
            || identity.stack_id != reservation.resource_id
        {
            return Err(error("failed Up cleanup activation binding changed"));
        }
        Ok(())
    }

    pub(super) fn require<S: EnvironmentStateStore>(
        &self,
        state: &S,
        machine: &MachineInstance,
        identity: &StackRuntimeIdentity,
    ) -> Result<(), MachineLiveSessionError> {
        self.validate_binding(machine, identity)?;
        let operation = state
            .access(|store| store.load_environment_lifecycle(&self.operation_id))
            .map_err(error)?
            .ok_or_else(|| error("failed Up cleanup journal missing"))?;
        self.validate_journal(&operation, true)
    }
}

pub(super) fn require_session_identity<S: EnvironmentStateStore>(
    state: &S,
    machine: &MachineInstance,
    session: &Session,
) -> Result<(), MachineLiveSessionError> {
    require_identity(
        state,
        machine,
        &session.identity,
        session.failed_up.lock().map_err(error)?.as_ref(),
    )
}

pub(super) fn preflight_session_identity(
    machine: &MachineInstance,
    session: &Session,
) -> Result<(), MachineLiveSessionError> {
    if let Some(proof) = session.failed_up.lock().map_err(error)?.as_ref() {
        return proof.validate_binding(machine, &session.identity);
    }
    if mismatched_identity(machine, &session.identity)? {
        return Err(error(
            "persisted Machine activation differs from registered runtime",
        ));
    }
    Ok(())
}

pub(super) fn require_identity<S: EnvironmentStateStore>(
    state: &S,
    machine: &MachineInstance,
    identity: &StackRuntimeIdentity,
    failed_up: Option<&FailedUpRuntime>,
) -> Result<(), MachineLiveSessionError> {
    if let Some(proof) = failed_up {
        return proof.require(state, machine, identity);
    }
    if mismatched_identity(machine, identity)? {
        return Err(error(
            "persisted Machine activation differs from registered runtime",
        ));
    }
    Ok(())
}

fn mismatched_identity(
    machine: &MachineInstance,
    identity: &StackRuntimeIdentity,
) -> Result<bool, MachineLiveSessionError> {
    if let Some(persisted) = &machine.runtime_identity {
        let exact: StackRuntimeIdentity =
            serde_json::from_str(&persisted.opaque_id).map_err(error)?;
        return Ok(&exact != identity);
    } else if machine.incarnation.is_some() {
        return Err(error("persisted incarnation has no exact runtime identity"));
    }
    Ok(false)
}
