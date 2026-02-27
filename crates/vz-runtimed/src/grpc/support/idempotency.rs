use std::collections::BTreeMap;

use tonic::Status;
use vz_runtime_contract::{
    Checkpoint, Container, Execution, Lease, MachineError, MachineErrorCode, Sandbox,
};

use crate::RuntimeDaemon;

use super::status::{status_from_machine_error, status_from_stack_error};

pub(in crate::grpc) fn load_idempotent_sandbox_replay(
    daemon: &RuntimeDaemon,
    key: &str,
    operation: &str,
    request_hash: &str,
    request_id: &str,
) -> Result<Option<Sandbox>, Status> {
    let record = daemon
        .with_state_store(|store| store.find_idempotency_result(key))
        .map_err(|error| status_from_stack_error(error, request_id))?;
    let Some(record) = record else {
        return Ok(None);
    };

    if record.operation != operation || record.request_hash != request_hash {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::StateConflict,
            format!("idempotency key conflict for key={key}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let Some(cached_sandbox) = daemon
        .with_state_store(|store| store.load_sandbox(&record.response_json))
        .map_err(|error| status_from_stack_error(error, request_id))?
    else {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            "idempotency record references missing sandbox".to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    };

    Ok(Some(cached_sandbox))
}

pub(in crate::grpc) fn load_idempotent_lease_replay(
    daemon: &RuntimeDaemon,
    key: &str,
    operation: &str,
    request_hash: &str,
    request_id: &str,
) -> Result<Option<Lease>, Status> {
    let record = daemon
        .with_state_store(|store| store.find_idempotency_result(key))
        .map_err(|error| status_from_stack_error(error, request_id))?;
    let Some(record) = record else {
        return Ok(None);
    };

    if record.operation != operation || record.request_hash != request_hash {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::StateConflict,
            format!("idempotency key conflict for key={key}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let Some(cached_lease) = daemon
        .with_state_store(|store| store.load_lease(&record.response_json))
        .map_err(|error| status_from_stack_error(error, request_id))?
    else {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            "idempotency record references missing lease".to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    };

    Ok(Some(cached_lease))
}

pub(in crate::grpc) fn load_idempotent_container_replay(
    daemon: &RuntimeDaemon,
    key: &str,
    operation: &str,
    request_hash: &str,
    request_id: &str,
) -> Result<Option<Container>, Status> {
    let record = daemon
        .with_state_store(|store| store.find_idempotency_result(key))
        .map_err(|error| status_from_stack_error(error, request_id))?;
    let Some(record) = record else {
        return Ok(None);
    };

    if record.operation != operation || record.request_hash != request_hash {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::StateConflict,
            format!("idempotency key conflict for key={key}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let Some(cached_container) = daemon
        .with_state_store(|store| store.load_container(&record.response_json))
        .map_err(|error| status_from_stack_error(error, request_id))?
    else {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            "idempotency record references missing container".to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    };

    Ok(Some(cached_container))
}

pub(in crate::grpc) fn load_idempotent_execution_replay(
    daemon: &RuntimeDaemon,
    key: &str,
    operation: &str,
    request_hash: &str,
    request_id: &str,
) -> Result<Option<Execution>, Status> {
    let record = daemon
        .with_state_store(|store| store.find_idempotency_result(key))
        .map_err(|error| status_from_stack_error(error, request_id))?;
    let Some(record) = record else {
        return Ok(None);
    };

    if record.operation != operation || record.request_hash != request_hash {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::StateConflict,
            format!("idempotency key conflict for key={key}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let Some(cached_execution) = daemon
        .with_state_store(|store| store.load_execution(&record.response_json))
        .map_err(|error| status_from_stack_error(error, request_id))?
    else {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            "idempotency record references missing execution".to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    };

    Ok(Some(cached_execution))
}

pub(in crate::grpc) fn load_idempotent_checkpoint_replay(
    daemon: &RuntimeDaemon,
    key: &str,
    operation: &str,
    request_hash: &str,
    request_id: &str,
) -> Result<Option<Checkpoint>, Status> {
    let record = daemon
        .with_state_store(|store| store.find_idempotency_result(key))
        .map_err(|error| status_from_stack_error(error, request_id))?;
    let Some(record) = record else {
        return Ok(None);
    };

    if record.operation != operation || record.request_hash != request_hash {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::StateConflict,
            format!("idempotency key conflict for key={key}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let Some(cached_checkpoint) = daemon
        .with_state_store(|store| store.load_checkpoint(&record.response_json))
        .map_err(|error| status_from_stack_error(error, request_id))?
    else {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            "idempotency record references missing checkpoint".to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    };

    Ok(Some(cached_checkpoint))
}

pub(in crate::grpc) fn normalize_idempotency_key(value: Option<&str>) -> Option<&str> {
    let raw = value?.trim();
    if raw.is_empty() { None } else { Some(raw) }
}
