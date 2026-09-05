use super::*;
use vz_runtime_contract::{
    EnvironmentSelectionContext, EnvironmentSelector, EnvironmentUpAdmission,
    EnvironmentUpCompletion, EnvironmentUpProgress, EnvironmentUpRequest,
};

pub fn environment_up_request_from_proto(
    value: &runtime_v2::UpEnvironmentRequest,
) -> Result<EnvironmentUpRequest, String> {
    let definition = project_definition_from_proto(
        value
            .definition
            .as_ref()
            .ok_or("Up omitted ProjectDefinition")?,
    )
    .map_err(|error| error.to_string())?;
    let process_environment_id = if value.environment.is_some() {
        None
    } else {
        value
            .process_environment_id
            .as_ref()
            .map(|id| EnvironmentId::new(id.clone()))
            .transpose()
            .map_err(|error| error.to_string())?
    };
    let request = EnvironmentUpRequest {
        definition,
        selection: EnvironmentSelectionContext {
            explicit: value.environment.clone().map(EnvironmentSelector::NameOrId),
            process_environment_id,
            workspace_key: value.workspace_key.clone(),
        },
        path_hint: value.path_hint.clone(),
        timeout_millis: value.timeout_millis,
    };
    request.request_hash()?;
    Ok(request)
}

fn admission_to_proto(value: &EnvironmentUpAdmission) -> runtime_v2::EnvironmentUpAdmission {
    runtime_v2::EnvironmentUpAdmission {
        schema_version: value.schema_version,
        project_id: value.project_id.to_string(),
        environment_id: value.environment_id.to_string(),
        machine_ids: value.machine_ids.iter().map(ToString::to_string).collect(),
        definition_digest: value.definition_digest.clone(),
        request_id: value.request_id.clone(),
        idempotency_key: value.idempotency_key.clone(),
        request_hash: value.request_hash.clone(),
        workspace_key: value.workspace_key.clone(),
        created_at: value.created_at,
    }
}
fn admission_from_proto(
    value: &runtime_v2::EnvironmentUpAdmission,
) -> Result<EnvironmentUpAdmission, String> {
    let admission = EnvironmentUpAdmission {
        schema_version: value.schema_version,
        project_id: ProjectId::new(value.project_id.clone()).map_err(|error| error.to_string())?,
        environment_id: EnvironmentId::new(value.environment_id.clone())
            .map_err(|error| error.to_string())?,
        machine_ids: value
            .machine_ids
            .iter()
            .map(|id| MachineId::new(id.clone()))
            .collect::<Result<_, _>>()
            .map_err(|error| error.to_string())?,
        definition_digest: value.definition_digest.clone(),
        request_id: value.request_id.clone(),
        idempotency_key: value.idempotency_key.clone(),
        request_hash: value.request_hash.clone(),
        workspace_key: value.workspace_key.clone(),
        created_at: value.created_at,
    };
    admission.validate()?;
    Ok(admission)
}
pub fn environment_up_progress_to_proto(
    value: &EnvironmentUpProgress,
) -> runtime_v2::UpEnvironmentEvent {
    runtime_v2::UpEnvironmentEvent {
        schema_version: value.schema_version,
        sequence: value.sequence,
        admission: Some(admission_to_proto(&value.admission)),
        phase: value.phase.clone(),
        operation: value
            .operation
            .as_ref()
            .map(environment_lifecycle_operation_to_proto),
        completion: value.completion.as_ref().map(|completion| {
            runtime_v2::EnvironmentUpCompletion {
                admission: Some(admission_to_proto(&completion.admission)),
                operation: completion
                    .operation
                    .as_ref()
                    .map(environment_lifecycle_operation_to_proto),
                workspace_binding: completion
                    .workspace_binding
                    .as_ref()
                    .map(workspace_binding_to_proto),
                error: completion.error.as_ref().map(machine_error_to_proto_detail),
                completed_at: completion.completed_at,
            }
        }),
    }
}
pub fn environment_up_progress_from_proto(
    value: &runtime_v2::UpEnvironmentEvent,
) -> Result<EnvironmentUpProgress, String> {
    let admission = admission_from_proto(
        value
            .admission
            .as_ref()
            .ok_or("Up progress omitted admission")?,
    )?;
    let operation = value
        .operation
        .as_ref()
        .map(environment_lifecycle_operation_from_proto)
        .transpose()
        .map_err(|error| error.to_string())?;
    let completion = value
        .completion
        .as_ref()
        .map(|completion| -> Result<EnvironmentUpCompletion, String> {
            let completion = EnvironmentUpCompletion {
                admission: admission_from_proto(
                    completion
                        .admission
                        .as_ref()
                        .ok_or("Up completion omitted admission")?,
                )?,
                operation: completion
                    .operation
                    .as_ref()
                    .map(environment_lifecycle_operation_from_proto)
                    .transpose()
                    .map_err(|error| error.to_string())?,
                workspace_binding: completion
                    .workspace_binding
                    .as_ref()
                    .map(workspace_binding_from_proto)
                    .transpose()
                    .map_err(|error| error.to_string())?,
                error: completion
                    .error
                    .as_ref()
                    .map(machine_error_from_proto_detail)
                    .transpose()
                    .map_err(|error| error.to_string())?,
                completed_at: completion.completed_at,
            };
            completion.validate()?;
            if completion.admission != admission || completion.operation != operation {
                return Err("Up terminal scope differs from progress".into());
            }
            Ok(completion)
        })
        .transpose()?;
    if value.schema_version != 1
        || ![
            "admitted",
            "preparing",
            "starting",
            "machine_acknowledged",
            "terminal",
        ]
        .contains(&value.phase.as_str())
        || (value.phase == "terminal") != completion.is_some()
    {
        return Err("Up progress schema or phase invalid".into());
    }
    if let Some(operation) = &operation {
        let validation = EnvironmentUpCompletion {
            admission: admission.clone(),
            operation: Some(operation.clone()),
            workspace_binding: None,
            error: Some(MachineError::new(
                MachineErrorCode::StateConflict,
                "progress scope validation".into(),
                Some(admission.request_id.clone()),
                BTreeMap::new(),
            )),
            completed_at: admission.created_at,
        };
        validation.validate()?;
    }
    Ok(EnvironmentUpProgress {
        schema_version: 1,
        sequence: value.sequence,
        admission,
        phase: value.phase.clone(),
        operation,
        completion,
    })
}
