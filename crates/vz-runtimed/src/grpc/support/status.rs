use prost::Message;
use vz_runtime_contract::{MachineError, MachineErrorCode, RequestMetadata};
use vz_runtime_translate::machine_error_to_proto_detail;
use vz_stack::StackError;

use tonic::{Code, Status};

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

    let code = match error.code {
        MachineErrorCode::ValidationError => Code::InvalidArgument,
        MachineErrorCode::NotFound => Code::NotFound,
        MachineErrorCode::StateConflict => Code::FailedPrecondition,
        MachineErrorCode::PolicyDenied => Code::PermissionDenied,
        MachineErrorCode::Timeout => Code::DeadlineExceeded,
        MachineErrorCode::BackendUnavailable => Code::Unavailable,
        MachineErrorCode::UnsupportedOperation => Code::Unimplemented,
        MachineErrorCode::InternalError => Code::Internal,
    };
    let details = machine_error_to_proto_detail(&error).encode_to_vec();
    Status::with_details(code, message, details.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use vz_runtime_proto::runtime_v2;
    use vz_runtime_translate::machine_error_from_proto_detail;

    #[test]
    fn tonic_status_carries_decodable_machine_error_details() {
        let status = status_from_machine_error(MachineError::new(
            MachineErrorCode::StateConflict,
            "replacement is owned by another incarnation".to_string(),
            Some("req-typed".to_string()),
            BTreeMap::from([
                ("action".to_string(), "api#1".to_string()),
                ("incarnation_id".to_string(), "inc-2".to_string()),
            ]),
        ));

        assert_eq!(status.code(), Code::FailedPrecondition);
        let wire = runtime_v2::ErrorDetail::decode(status.details())
            .expect("status details must contain ErrorDetail protobuf bytes");
        let decoded = machine_error_from_proto_detail(&wire)
            .expect("machine error detail must round-trip through translation");
        assert_eq!(decoded.code, MachineErrorCode::StateConflict);
        assert_eq!(decoded.request_id.as_deref(), Some("req-typed"));
        assert_eq!(
            decoded.details.get("action").map(String::as_str),
            Some("api#1")
        );
        assert_eq!(
            decoded.details.get("incarnation_id").map(String::as_str),
            Some("inc-2")
        );
    }
}
