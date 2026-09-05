//! Typed, lossless Machine execution wire conversion.
use super::*;
use vz_runtime_contract::{
    MachineExecutionReceipt, MachineExecutionScope, MachineExecutionSpec, MachineExecutionState,
    MachineExecutionTerminal,
};

fn invalid(value: String) -> TranslationError {
    TranslationError::InvalidValue {
        field: "machine_execution",
        value,
    }
}

pub fn machine_execution_terminal_from_proto(
    value: &runtime_v2::MachineExecutionTerminal,
) -> Result<MachineExecutionTerminal, TranslationError> {
    let rows = u16::try_from(value.rows).map_err(|_| invalid("terminal rows exceed u16".into()))?;
    let columns =
        u16::try_from(value.columns).map_err(|_| invalid("terminal columns exceed u16".into()))?;
    if rows == 0 || columns == 0 {
        return Err(invalid("terminal dimensions must be positive".into()));
    }
    Ok(MachineExecutionTerminal { rows, columns })
}
pub fn machine_execution_spec_to_proto(
    value: &MachineExecutionSpec,
) -> runtime_v2::MachineExecutionSpec {
    runtime_v2::MachineExecutionSpec {
        argv: value.argv.clone(),
        environment: value.environment.clone().into_iter().collect(),
        working_directory: value.working_directory.clone(),
        user: value.user.clone(),
        terminal: value
            .terminal
            .map(|terminal| runtime_v2::MachineExecutionTerminal {
                rows: terminal.rows.into(),
                columns: terminal.columns.into(),
            }),
        timeout_millis: value.timeout_millis,
    }
}
pub fn machine_execution_spec_from_proto(
    value: &runtime_v2::MachineExecutionSpec,
) -> Result<MachineExecutionSpec, TranslationError> {
    let decoded = MachineExecutionSpec {
        argv: value.argv.clone(),
        environment: value.environment.clone().into_iter().collect(),
        working_directory: value.working_directory.clone(),
        user: value.user.clone(),
        terminal: value
            .terminal
            .as_ref()
            .map(machine_execution_terminal_from_proto)
            .transpose()?,
        timeout_millis: value.timeout_millis,
    };
    decoded.validate().map_err(invalid)?;
    Ok(decoded)
}
pub fn machine_execution_scope_to_proto(
    value: &MachineExecutionScope,
) -> runtime_v2::MachineExecutionScope {
    runtime_v2::MachineExecutionScope {
        schema_version: value.schema_version,
        execution_id: value.execution_id.clone(),
        request_id: value.request_id.clone(),
        idempotency_key: value.idempotency_key.clone(),
        request_hash: value.request_hash.clone(),
        project_id: value.project_id.to_string(),
        environment_id: value.environment_id.to_string(),
        machine_id: value.machine_id.to_string(),
        environment_generation: value.environment_generation,
        incarnation: Some(machine_incarnation_to_proto(&value.incarnation)),
        runtime_identity: Some(machine_runtime_identity_to_proto(&value.runtime_identity)),
        definition_digest: value.definition_digest.clone(),
    }
}
pub fn machine_execution_scope_from_proto(
    value: &runtime_v2::MachineExecutionScope,
) -> Result<MachineExecutionScope, TranslationError> {
    let machine_id = MachineId::new(value.machine_id.clone())?;
    let decoded = MachineExecutionScope {
        schema_version: value.schema_version,
        execution_id: value.execution_id.clone(),
        request_id: value.request_id.clone(),
        idempotency_key: value.idempotency_key.clone(),
        request_hash: value.request_hash.clone(),
        project_id: ProjectId::new(value.project_id.clone())?,
        environment_id: EnvironmentId::new(value.environment_id.clone())?,
        incarnation: machine_incarnation_from_proto(required(
            value.incarnation.as_ref(),
            "machine_execution.incarnation",
        )?)?,
        runtime_identity: machine_runtime_identity_from_proto(
            required(
                value.runtime_identity.as_ref(),
                "machine_execution.runtime_identity",
            )?,
            &machine_id,
        )?,
        machine_id,
        environment_generation: value.environment_generation,
        definition_digest: value.definition_digest.clone(),
    };
    decoded.validate().map_err(invalid)?;
    Ok(decoded)
}
pub fn machine_execution_receipt_to_proto(
    value: &MachineExecutionReceipt,
) -> runtime_v2::MachineExecutionReceipt {
    use runtime_v2::MachineExecutionState as Wire;
    runtime_v2::MachineExecutionReceipt {
        scope: Some(machine_execution_scope_to_proto(&value.scope)),
        state: match value.state {
            MachineExecutionState::Admitted => Wire::Admitted,
            MachineExecutionState::Completed => Wire::Completed,
            MachineExecutionState::Quiesced => Wire::Quiesced,
            MachineExecutionState::Uncertain => Wire::Uncertain,
        } as i32,
        exit_code: value.exit_code,
        failure: value.failure.clone(),
        output_replay_available: value.output_replay_available,
        created_at: value.created_at,
        updated_at: value.updated_at,
    }
}
pub fn machine_execution_receipt_from_proto(
    value: &runtime_v2::MachineExecutionReceipt,
) -> Result<MachineExecutionReceipt, TranslationError> {
    use runtime_v2::MachineExecutionState as Wire;
    let state = match Wire::try_from(value.state) {
        Ok(Wire::Admitted) => MachineExecutionState::Admitted,
        Ok(Wire::Completed) => MachineExecutionState::Completed,
        Ok(Wire::Quiesced) => MachineExecutionState::Quiesced,
        Ok(Wire::Uncertain) => MachineExecutionState::Uncertain,
        _ => return Err(invalid("unknown Machine execution state".into())),
    };
    let decoded = MachineExecutionReceipt {
        scope: machine_execution_scope_from_proto(required(
            value.scope.as_ref(),
            "machine_execution.scope",
        )?)?,
        state,
        exit_code: value.exit_code,
        failure: value.failure.clone(),
        output_replay_available: value.output_replay_available,
        created_at: value.created_at,
        updated_at: value.updated_at,
    };
    decoded.validate().map_err(invalid)?;
    Ok(decoded)
}
