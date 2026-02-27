use tonic::Status;
use vz_runtime_contract::{
    MachineErrorCode, RequestMetadata, RuntimeOperation, runtime_error_machine_error,
};
use vz_stack::{Receipt, StackError};

use crate::RuntimeDaemon;

use super::ids::{current_unix_secs, generate_receipt_id};
use super::receipt_policy_preflight_metadata;
use super::status::{status_from_machine_error, status_from_stack_error};

pub(in crate::grpc) fn enforce_mutation_policy_preflight(
    daemon: &RuntimeDaemon,
    operation: RuntimeOperation,
    metadata: &RequestMetadata,
    request_id: &str,
) -> Result<(), Status> {
    match daemon.enforce_policy_preflight(operation, metadata) {
        Ok(()) => {
            persist_policy_audit_receipt(
                daemon, operation, metadata, request_id, "allow", None, None,
            )
            .map_err(|error| status_from_stack_error(error, request_id))?;
            Ok(())
        }
        Err(error) => {
            let machine_error = runtime_error_machine_error(&error, metadata);
            let decision = if machine_error.code == MachineErrorCode::PolicyDenied {
                "deny"
            } else {
                "error"
            };
            let reason = machine_error
                .details
                .get("reason")
                .cloned()
                .unwrap_or_else(|| machine_error.message.clone());
            persist_policy_audit_receipt(
                daemon,
                operation,
                metadata,
                request_id,
                decision,
                Some(machine_error.code.as_str()),
                Some(reason.as_str()),
            )
            .map_err(|persist_error| status_from_stack_error(persist_error, request_id))?;
            Err(status_from_machine_error(machine_error))
        }
    }
}

fn persist_policy_audit_receipt(
    daemon: &RuntimeDaemon,
    operation: RuntimeOperation,
    metadata: &RequestMetadata,
    request_id: &str,
    decision: &str,
    machine_code: Option<&str>,
    reason: Option<&str>,
) -> Result<(), StackError> {
    let now = current_unix_secs();
    let audit_metadata = receipt_policy_preflight_metadata(
        operation.as_str(),
        decision,
        machine_code,
        reason,
        metadata.trace_id.as_deref(),
        metadata.idempotency_key.as_deref(),
        daemon.policy_hash(),
    )?;
    let receipt = Receipt {
        receipt_id: generate_receipt_id(),
        operation: format!("policy_preflight:{}", operation.as_str()),
        entity_id: request_id.to_string(),
        entity_type: "policy".to_string(),
        request_id: request_id.to_string(),
        status: decision.to_string(),
        created_at: now,
        metadata: audit_metadata,
    };

    daemon.with_state_store(|store| {
        store.with_immediate_transaction(|tx| {
            tx.save_receipt(&receipt)?;
            Ok(())
        })
    })
}
