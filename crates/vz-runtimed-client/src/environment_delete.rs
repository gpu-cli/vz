//! Correlated, bounded/coalescing Environment Delete observation.
#[cfg(test)]
#[path = "environment_delete_tests.rs"]
mod tests;

use std::time::Duration;

use tonic::{Request, Status};
use vz_runtime_contract::{
    EnvironmentId, EnvironmentLifecycleKind, EnvironmentLifecycleOperation,
    EnvironmentLifecycleStatus, EnvironmentSelectionContext, EnvironmentSelector,
    EnvironmentTombstone, MachineError, ProjectId, environment_delete_request_hash,
};
use vz_runtime_proto::runtime_v2;

use crate::topology::immutable_stop_scope;
use crate::transport::status_to_client_error;
use crate::{DaemonClient, DaemonClientError, Result};

#[derive(Debug, Clone)]
pub struct EnvironmentDeleteEvent {
    pub request_id: String,
    pub sequence: u64,
    pub operation: EnvironmentLifecycleOperation,
    pub terminal: bool,
    pub error: Option<MachineError>,
    pub tombstone: Option<EnvironmentTombstone>,
}

pub struct EnvironmentDeleteStream {
    stream: tonic::Streaming<runtime_v2::DeleteEnvironmentEvent>,
    validation: DeleteValidator,
    idle_timeout: Duration,
}

struct DeleteValidator {
    project_id: ProjectId,
    request_id: String,
    idempotency_key: String,
    expected_environment: Option<EnvironmentId>,
    selection: EnvironmentSelectionContext,
    machine_timeout_millis: u64,
    scope: Option<EnvironmentLifecycleOperation>,
    last_sequence: Option<u64>,
    terminal: bool,
}

fn invalid(reason: impl Into<String>) -> DaemonClientError {
    DaemonClientError::IncompatibleProtocol {
        reason: reason.into(),
    }
}

impl DeleteValidator {
    fn event(
        &mut self,
        wire: runtime_v2::DeleteEnvironmentEvent,
    ) -> Result<EnvironmentDeleteEvent> {
        if self.terminal
            || wire.schema_version != 1
            || wire.request_id != self.request_id
            || self
                .last_sequence
                .is_some_and(|previous| wire.sequence <= previous)
        {
            return Err(invalid(
                "Delete schema, correlation, monotonic sequence or terminal mismatch",
            ));
        }
        let operation = vz_runtime_translate::environment_lifecycle_operation_from_proto(
            wire.operation
                .as_ref()
                .ok_or_else(|| invalid("Delete omitted operation"))?,
        )
        .map_err(|error| invalid(error.to_string()))?;
        let expected_hash = environment_delete_request_hash(
            &self.project_id,
            &operation.environment_id,
            &self.selection,
            u128::from(self.machine_timeout_millis),
        )
        .map_err(|error| invalid(error.to_string()))?;
        if operation.kind != EnvironmentLifecycleKind::Delete
            || operation.project_id != self.project_id
            || operation.request_id != self.request_id
            || operation.idempotency_key != self.idempotency_key
            || operation.request_hash != expected_hash
            || operation.machine_steps.len() > 128
            || operation.cleanup_steps.len() > 4096
            || self
                .expected_environment
                .as_ref()
                .is_some_and(|id| id != &operation.environment_id)
            || self
                .scope
                .as_ref()
                .is_some_and(|scope| scope != &immutable_stop_scope(&operation))
        {
            return Err(invalid(
                "Delete changed exact operation ownership, plan or request",
            ));
        }
        let error = wire
            .error
            .as_ref()
            .map(vz_runtime_translate::machine_error_from_proto_detail)
            .transpose()
            .map_err(|error| invalid(error.to_string()))?;
        let tombstone = wire
            .tombstone
            .as_ref()
            .map(vz_runtime_translate::environment_tombstone_from_proto)
            .transpose()
            .map_err(|error| invalid(error.to_string()))?;
        let succeeded = operation.status == EnvironmentLifecycleStatus::Succeeded;
        // Delete failure retains the aggregate/journal for reconciliation;
        // contract-valid failed Delete plans are Blocked, never Failed.
        let failed = operation.status == EnvironmentLifecycleStatus::Blocked;
        if wire.terminal != (succeeded || failed)
            || tombstone.is_some() != succeeded
            || error.is_some() != failed
            || error
                .as_ref()
                .is_some_and(|error| error.request_id.as_deref() != Some(&self.request_id))
        {
            return Err(invalid("Delete terminal state/error/tombstone mismatch"));
        }
        if let Some(tombstone) = &tombstone {
            tombstone
                .validate_for_operation(&operation)
                .map_err(|error| invalid(error.to_string()))?;
        }
        self.scope = Some(immutable_stop_scope(&operation));
        self.last_sequence = Some(wire.sequence);
        self.terminal = wire.terminal;
        Ok(EnvironmentDeleteEvent {
            request_id: wire.request_id,
            sequence: wire.sequence,
            operation,
            terminal: wire.terminal,
            error,
            tombstone,
        })
    }
}

fn request_selection(
    request: &runtime_v2::DeleteEnvironmentRequest,
) -> Result<EnvironmentSelectionContext> {
    // Match server-side precedence exactly. Explicit strings stay NameOrId;
    // an ignored process selector is neither parsed nor added to the hash.
    let process_environment_id = if request.environment.is_some() {
        None
    } else {
        request
            .process_environment_id
            .as_ref()
            .map(|id| EnvironmentId::new(id.clone()))
            .transpose()
            .map_err(|error| invalid(error.to_string()))?
    };
    Ok(EnvironmentSelectionContext {
        explicit: request
            .environment
            .clone()
            .map(EnvironmentSelector::NameOrId),
        process_environment_id,
        workspace_key: request.workspace_key.clone(),
    })
}

impl EnvironmentDeleteStream {
    /// Observer timeout/disconnect never implies cancellation of admitted Delete.
    pub async fn next_event(&mut self) -> Result<Option<EnvironmentDeleteEvent>> {
        let next = tokio::time::timeout(self.idle_timeout, self.stream.message()).await
            .map_err(|_| DaemonClientError::Grpc(Box::new(Status::deadline_exceeded(format!(
                "Delete observation timed out; effects may continue; replay request_id={} idempotency_key={}",
                self.validation.request_id, self.validation.idempotency_key)))))?
            .map_err(|error| DaemonClientError::Grpc(Box::new(error)))?;
        match next {
            Some(wire) => self.validation.event(wire).map(Some),
            None if self.validation.terminal => Ok(None),
            None => Err(invalid("Delete stream ended without terminal receipt")),
        }
    }
}

impl DaemonClient {
    /// Begin or replay one exact Delete. Retain request IDs across disconnects.
    pub async fn delete_environment_stream(
        &mut self,
        request: runtime_v2::DeleteEnvironmentRequest,
    ) -> Result<EnvironmentDeleteStream> {
        let project_id =
            ProjectId::new(request.project_id.clone()).map_err(|e| invalid(e.to_string()))?;
        let metadata = request
            .metadata
            .as_ref()
            .ok_or_else(|| invalid("Delete requires request metadata"))?;
        for text in [&metadata.request_id, &metadata.idempotency_key] {
            if text.is_empty()
                || text.len() > 256
                || text.trim() != text
                || text.chars().any(char::is_control)
            {
                return Err(invalid(
                    "Delete requires bounded stable request and idempotency IDs",
                ));
            }
        }
        if !(1..=300_000).contains(&request.machine_timeout_millis) {
            return Err(invalid(
                "Delete Machine timeout must be in 1..300000 milliseconds",
            ));
        }
        let selection = request_selection(&request)?;
        let expected_environment = selection.process_environment_id.clone();
        let validation = DeleteValidator {
            project_id,
            request_id: metadata.request_id.clone(),
            idempotency_key: metadata.idempotency_key.clone(),
            expected_environment,
            selection,
            machine_timeout_millis: request.machine_timeout_millis,
            scope: None,
            last_sequence: None,
            terminal: false,
        };
        // Each physical step is bounded; filesystem retirement may span several
        // bounded substeps. This is observation only, not an effect deadline.
        let idle_timeout = Duration::from_secs(305);
        let stream = tokio::time::timeout(
            Duration::from_secs(35),
            self.topology_client
                .delete_environment(Request::new(request)),
        )
        .await
        .map_err(|_| {
            DaemonClientError::Grpc(Box::new(Status::deadline_exceeded(format!(
                "Delete admission observation timed out; replay request_id={} idempotency_key={}",
                validation.request_id, validation.idempotency_key
            ))))
        })?
        .map_err(|status| status_to_client_error(&self.config.socket_path, status))?
        .into_inner();
        Ok(EnvironmentDeleteStream {
            stream,
            validation,
            idle_timeout,
        })
    }
}
