use serde::Serialize;
use serde_json::Value;
use vz_stack::StackError;

#[derive(Debug, Serialize)]
struct EventReceiptMetadata<'a> {
    event_type: &'a str,
}

#[derive(Debug, Serialize)]
struct IdempotentMutationReceiptMetadata<'a> {
    event_type: &'a str,
    request_hash: &'a str,
    idempotency_key: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct ExecutionStdinReceiptMetadata {
    event_type: &'static str,
    bytes: usize,
}

#[derive(Debug, Serialize)]
struct ExecutionResizedReceiptMetadata {
    event_type: &'static str,
    cols: u16,
    rows: u16,
}

#[derive(Debug, Serialize)]
struct ExecutionSignaledReceiptMetadata<'a> {
    event_type: &'static str,
    signal: &'a str,
}

#[derive(Debug, Serialize)]
struct PolicyPreflightReceiptMetadata<'a> {
    operation: &'a str,
    decision: &'a str,
    machine_code: Option<&'a str>,
    reason: Option<&'a str>,
    trace_id: Option<&'a str>,
    idempotency_key: Option<&'a str>,
    policy_hash: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct StackApplyReceiptMetadata {
    event_type: &'static str,
    changed_actions: u32,
    converged: bool,
    services_ready: u32,
    services_failed: u32,
}

#[derive(Debug, Serialize)]
struct StackTeardownReceiptMetadata {
    event_type: &'static str,
    changed_actions: u32,
    removed_volumes: u32,
}

#[derive(Debug, Serialize)]
struct FileMutationReceiptMetadata<'a> {
    event_type: &'static str,
    sandbox_id: &'a str,
    path: &'a str,
    destination_path: Option<&'a str>,
}

fn metadata_value<T: Serialize>(metadata: T) -> Result<Value, StackError> {
    serde_json::to_value(metadata).map_err(StackError::from)
}

pub(in crate::grpc) fn receipt_event_metadata(
    event_type: &'static str,
) -> Result<Value, StackError> {
    metadata_value(EventReceiptMetadata { event_type })
}

pub(in crate::grpc) fn receipt_idempotent_mutation_metadata(
    event_type: &'static str,
    request_hash: &str,
    idempotency_key: Option<&str>,
) -> Result<Value, StackError> {
    metadata_value(IdempotentMutationReceiptMetadata {
        event_type,
        request_hash,
        idempotency_key,
    })
}

pub(in crate::grpc) fn receipt_execution_stdin_metadata(bytes: usize) -> Result<Value, StackError> {
    metadata_value(ExecutionStdinReceiptMetadata {
        event_type: "execution_running",
        bytes,
    })
}

pub(in crate::grpc) fn receipt_execution_resized_metadata(
    cols: u16,
    rows: u16,
) -> Result<Value, StackError> {
    metadata_value(ExecutionResizedReceiptMetadata {
        event_type: "execution_resized",
        cols,
        rows,
    })
}

pub(in crate::grpc) fn receipt_execution_signaled_metadata(
    signal: &str,
) -> Result<Value, StackError> {
    metadata_value(ExecutionSignaledReceiptMetadata {
        event_type: "execution_signaled",
        signal,
    })
}

pub(in crate::grpc) fn receipt_policy_preflight_metadata(
    operation: &str,
    decision: &str,
    machine_code: Option<&str>,
    reason: Option<&str>,
    trace_id: Option<&str>,
    idempotency_key: Option<&str>,
    policy_hash: Option<&str>,
) -> Result<Value, StackError> {
    metadata_value(PolicyPreflightReceiptMetadata {
        operation,
        decision,
        machine_code,
        reason,
        trace_id,
        idempotency_key,
        policy_hash,
    })
}

pub(in crate::grpc) fn receipt_stack_apply_metadata(
    changed_actions: u32,
    converged: bool,
    services_ready: u32,
    services_failed: u32,
) -> Result<Value, StackError> {
    metadata_value(StackApplyReceiptMetadata {
        event_type: "stack_apply_completed",
        changed_actions,
        converged,
        services_ready,
        services_failed,
    })
}

pub(in crate::grpc) fn receipt_stack_teardown_metadata(
    changed_actions: u32,
    removed_volumes: u32,
) -> Result<Value, StackError> {
    metadata_value(StackTeardownReceiptMetadata {
        event_type: "stack_destroyed",
        changed_actions,
        removed_volumes,
    })
}

pub(in crate::grpc) fn receipt_file_mutation_metadata(
    event_type: &'static str,
    sandbox_id: &str,
    path: &str,
    destination_path: Option<&str>,
) -> Result<Value, StackError> {
    metadata_value(FileMutationReceiptMetadata {
        event_type,
        sandbox_id,
        path,
        destination_path,
    })
}
