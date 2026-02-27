use std::collections::BTreeMap;

use tonic::Status;
use vz_runtime_contract::{MachineError, MachineErrorCode, Sandbox};
use vz_runtime_proto::runtime_v2;
use vz_stack::EventRecord;

use super::status::status_from_machine_error;

pub(in crate::grpc) fn event_record_to_runtime_event(
    record: &EventRecord,
) -> Result<runtime_v2::RuntimeEvent, Status> {
    let event_json = serde_json::to_string(&record.event).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!("failed to serialize event record {}: {error}", record.id),
            None,
            BTreeMap::new(),
        ))
    })?;

    Ok(runtime_v2::RuntimeEvent {
        id: record.id,
        stack_name: record.stack_name.clone(),
        created_at: record.created_at.clone(),
        event_json,
    })
}

pub(in crate::grpc) fn sandbox_stack_name(sandbox: &Sandbox) -> String {
    sandbox
        .labels
        .get("stack_name")
        .cloned()
        .unwrap_or_else(|| sandbox.sandbox_id.clone())
}
