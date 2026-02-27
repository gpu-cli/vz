use vz_runtime_contract::{MachineError, MachineErrorCode, RequestMetadata};
use vz_stack::StackError;

use tonic::Status;

pub(in crate::grpc) fn status_from_stack_error(error: StackError, request_id: &str) -> Status {
    status_from_machine_error(
        error.to_machine_error(&RequestMetadata::from_optional_refs(Some(request_id), None)),
    )
}

pub(in crate::grpc) fn status_from_machine_error(error: MachineError) -> Status {
    let request_fragment = error
        .request_id
        .as_ref()
        .map(|request_id| format!(" request_id={request_id}"))
        .unwrap_or_default();
    let message = format!(
        "{}: {}{}",
        error.code.as_str(),
        error.message,
        request_fragment
    );

    match error.code {
        MachineErrorCode::ValidationError => Status::invalid_argument(message),
        MachineErrorCode::NotFound => Status::not_found(message),
        MachineErrorCode::StateConflict => Status::failed_precondition(message),
        MachineErrorCode::PolicyDenied => Status::permission_denied(message),
        MachineErrorCode::Timeout => Status::deadline_exceeded(message),
        MachineErrorCode::BackendUnavailable => Status::unavailable(message),
        MachineErrorCode::UnsupportedOperation => Status::unimplemented(message),
        MachineErrorCode::InternalError => Status::internal(message),
    }
}
