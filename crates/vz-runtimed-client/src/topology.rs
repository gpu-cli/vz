use tonic::{Code, Request, Status};
use vz_runtime_contract::{ProjectId, ProjectState};
use vz_runtime_proto::runtime_v2;
use vz_runtime_translate::project_state_from_proto;

use crate::transport::status_to_client_error;
use crate::{DaemonClient, DaemonClientError, Result};

/// Preserve the daemon's original typed failure and exact request correlation.
pub fn environment_stop_error_detail(
    error: &DaemonClientError,
) -> Option<vz_runtime_contract::MachineError> {
    use prost::Message;
    let DaemonClientError::Grpc(status) = error else {
        return None;
    };
    let detail = runtime_v2::ErrorDetail::decode(status.details()).ok()?;
    vz_runtime_translate::machine_error_from_proto_detail(&detail).ok()
}

/// Validated, correlated progress from the daemon-owned Environment Stop.
#[derive(Debug, Clone)]
pub struct EnvironmentStopEvent {
    pub request_id: String,
    pub sequence: u64,
    pub operation: vz_runtime_contract::EnvironmentLifecycleOperation,
    pub terminal: bool,
    pub error: Option<vz_runtime_contract::MachineError>,
}

pub struct EnvironmentStopStream {
    stream: tonic::Streaming<runtime_v2::StopEnvironmentEvent>,
    validation: StopEventValidator,
    idle_timeout: std::time::Duration,
}

struct StopEventValidator {
    request_id: String,
    idempotency_key: String,
    project_id: ProjectId,
    sequence: u64,
    operation_scope: Option<vz_runtime_contract::EnvironmentLifecycleOperation>,
    expected_environment_id: Option<vz_runtime_contract::EnvironmentId>,
    terminal: bool,
}

fn stop_protocol_error(reason: impl Into<String>) -> DaemonClientError {
    DaemonClientError::IncompatibleProtocol {
        reason: reason.into(),
    }
}

// Retain all immutable journal fields, including every exact Machine step.
// Only progress and acknowledgement evidence may evolve between frames.
fn immutable_stop_scope(
    operation: &vz_runtime_contract::EnvironmentLifecycleOperation,
) -> vz_runtime_contract::EnvironmentLifecycleOperation {
    let mut scope = operation.clone();
    scope.status = vz_runtime_contract::EnvironmentLifecycleStatus::Running;
    scope.updated_at = scope.created_at;
    scope.completed_at = None;
    for step in &mut scope.machine_steps {
        step.status = vz_runtime_contract::LifecycleStepStatus::Pending;
        step.resulting_incarnation = None;
        step.resulting_activation = None;
        step.failure_reason = None;
    }
    for step in &mut scope.cleanup_steps {
        step.status = vz_runtime_contract::LifecycleStepStatus::Pending;
        step.failure_reason = None;
    }
    scope
}

impl EnvironmentStopStream {
    /// A closed stream without one terminal event is failure. Dropping this
    /// observer does not cancel the daemon's admitted lifecycle operation.
    pub async fn next_event(&mut self) -> Result<Option<EnvironmentStopEvent>> {
        let wire = tokio::time::timeout(self.idle_timeout, self.stream.message()).await
            .map_err(|_| DaemonClientError::Grpc(Box::new(Status::deadline_exceeded(
                format!("Stop observation timed out; operation may continue; replay request_id={} idempotency_key={}", self.validation.request_id, self.validation.idempotency_key)
            ))))?
            .map_err(|error| DaemonClientError::Grpc(Box::new(error)))?;
        let Some(wire) = wire else {
            if !self.validation.terminal {
                return Err(stop_protocol_error(
                    "Stop stream ended without a terminal receipt",
                ));
            }
            return Ok(None);
        };
        self.validation.validate_event(wire).map(Some)
    }
}

impl StopEventValidator {
    fn validate_event(
        &mut self,
        wire: runtime_v2::StopEnvironmentEvent,
    ) -> Result<EnvironmentStopEvent> {
        if self.terminal
            || wire.schema_version != 1
            || wire.request_id != self.request_id
            || wire.sequence != self.sequence
        {
            return Err(stop_protocol_error(
                "Stop stream schema, correlation, sequence or terminal ordering mismatch",
            ));
        }
        let operation = vz_runtime_translate::environment_lifecycle_operation_from_proto(
            wire.operation
                .as_ref()
                .ok_or_else(|| stop_protocol_error("Stop event omitted operation"))?,
        )
        .map_err(|error| stop_protocol_error(error.to_string()))?;
        if operation.kind != vz_runtime_contract::EnvironmentLifecycleKind::Stop
            || operation.project_id != self.project_id
            || operation.request_id != self.request_id
            || operation.idempotency_key != self.idempotency_key
            || operation.machine_steps.len() > 128
            || self
                .expected_environment_id
                .as_ref()
                .is_some_and(|id| id != &operation.environment_id)
            || self
                .operation_scope
                .as_ref()
                .is_some_and(|scope| scope != &immutable_stop_scope(&operation))
        {
            return Err(stop_protocol_error(
                "Stop stream changed its exact operation ownership or request",
            ));
        }
        let error = wire
            .error
            .as_ref()
            .map(vz_runtime_translate::machine_error_from_proto_detail)
            .transpose()
            .map_err(|error| stop_protocol_error(error.to_string()))?;
        let succeeded =
            operation.status == vz_runtime_contract::EnvironmentLifecycleStatus::Succeeded;
        let failed = operation.status == vz_runtime_contract::EnvironmentLifecycleStatus::Failed;
        if wire.terminal != (succeeded || failed)
            || error.is_some() != (wire.terminal && failed)
            || error
                .as_ref()
                .is_some_and(|error| error.request_id.as_deref() != Some(self.request_id.as_str()))
        {
            return Err(stop_protocol_error(
                "Stop terminal state/error contract mismatch",
            ));
        }
        self.sequence += 1;
        self.operation_scope = Some(immutable_stop_scope(&operation));
        self.terminal = wire.terminal;
        Ok(EnvironmentStopEvent {
            request_id: wire.request_id,
            sequence: wire.sequence,
            operation,
            terminal: wire.terminal,
            error,
        })
    }
}

/// Canonically validated Project topology returned by the runtime daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectStateSnapshot {
    pub request_id: String,
    pub project: ProjectState,
}

impl DaemonClient {
    /// Begin or replay a streamed, selected-Environment Stop. The caller owns
    /// stable request/idempotency IDs and must retain them across retries.
    pub async fn stop_environment_stream(
        &mut self,
        request: runtime_v2::StopEnvironmentRequest,
    ) -> Result<EnvironmentStopStream> {
        let project_id = validated_project_id(&request.project_id)?;
        let metadata = request
            .metadata
            .as_ref()
            .ok_or_else(|| stop_protocol_error("Stop requires request metadata"))?;
        let request_id = metadata.request_id.clone();
        let idempotency_key = metadata.idempotency_key.clone();
        if request_id.is_empty()
            || request_id.len() > 256
            || idempotency_key.len() > 256
            || request_id.chars().any(char::is_control)
            || idempotency_key.chars().any(char::is_control)
            || request_id.trim() != request_id
            || idempotency_key.trim() != idempotency_key
            || idempotency_key.is_empty()
            || !(1..=300_000).contains(&request.machine_timeout_millis)
        {
            return Err(stop_protocol_error(
                "Stop requires stable request/idempotency IDs and a bounded Machine timeout",
            ));
        }
        let idle_timeout = std::time::Duration::from_millis(request.machine_timeout_millis)
            + std::time::Duration::from_secs(5);
        // Explicit NameOrId is intentionally not reinterpreted as typed Id:
        // even an ID-shaped string can name an Environment. Process selection
        // is an immutable typed ID, and must constrain the very first frame.
        let expected_environment_id = if request.environment.is_some() {
            None
        } else {
            request
                .process_environment_id
                .as_ref()
                .map(|id| vz_runtime_contract::EnvironmentId::new(id.clone()))
                .transpose()
                .map_err(|error| stop_protocol_error(error.to_string()))?
        };
        let stream = tokio::time::timeout(std::time::Duration::from_secs(35),
            self.topology_client.stop_environment(Request::new(request)))
            .await.map_err(|_| DaemonClientError::Grpc(Box::new(Status::deadline_exceeded(
                format!("Stop admission observation exceeded 35 seconds; replay request_id={request_id} idempotency_key={idempotency_key}")
            ))))?
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))?.into_inner();
        Ok(EnvironmentStopStream {
            stream,
            validation: StopEventValidator {
                request_id,
                idempotency_key,
                project_id,
                sequence: 0,
                operation_scope: None,
                expected_environment_id,
                terminal: false,
            },
            idle_timeout,
        })
    }

    /// Load one complete Project topology aggregate by stable identity.
    pub async fn get_project_state(
        &mut self,
        request: runtime_v2::GetProjectStateRequest,
    ) -> Result<ProjectStateSnapshot> {
        let requested_project_id = validated_project_id(&request.project_id)?;
        let expected_request_id = request
            .metadata
            .as_ref()
            .map(|metadata| metadata.request_id.trim())
            .filter(|request_id| !request_id.is_empty())
            .map(str::to_string);
        let response = self.get_project_state_with_metadata(request).await?;
        let response = response.into_inner();
        validate_response_request_id(expected_request_id.as_deref(), &response.request_id)?;
        let wire_project =
            response
                .project
                .ok_or_else(|| DaemonClientError::IncompatibleProtocol {
                    reason: "GetProjectState response omitted required project aggregate"
                        .to_string(),
                })?;
        let project = project_state_from_proto(&wire_project).map_err(|error| {
            DaemonClientError::IncompatibleProtocol {
                reason: format!("GetProjectState returned invalid project aggregate: {error}"),
            }
        })?;
        if project.definition.project_id != requested_project_id {
            return Err(DaemonClientError::IncompatibleProtocol {
                reason: format!(
                    "GetProjectState returned project {} for requested {}",
                    project.definition.project_id, requested_project_id
                ),
            });
        }
        Ok(ProjectStateSnapshot {
            request_id: response.request_id,
            project,
        })
    }

    /// Load one Project aggregate while preserving gRPC response metadata.
    pub async fn get_project_state_with_metadata(
        &mut self,
        mut request: runtime_v2::GetProjectStateRequest,
    ) -> Result<tonic::Response<runtime_v2::GetProjectStateResponse>> {
        validated_project_id(&request.project_id)?;
        Self::ensure_metadata(&mut request.metadata);
        tokio::time::timeout(
            self.config.request_timeout,
            self.topology_client
                .get_project_state(Request::new(request)),
        )
        .await
        .map_err(|_| DaemonClientError::Unavailable {
            socket_path: self.config.socket_path.clone(),
            reason: format!(
                "get_project_state timed out after {}ms",
                self.config.request_timeout.as_millis()
            ),
        })?
        .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }
}

fn validated_project_id(project_id: &str) -> Result<ProjectId> {
    ProjectId::new(project_id).map_err(|error| {
        DaemonClientError::Grpc(Box::new(Status::new(
            Code::InvalidArgument,
            error.to_string(),
        )))
    })
}

fn validate_response_request_id(expected: Option<&str>, returned: &str) -> Result<()> {
    if returned.trim().is_empty() {
        return Err(DaemonClientError::IncompatibleProtocol {
            reason: "GetProjectState response omitted a nonempty request_id".to_string(),
        });
    }
    if let Some(expected) = expected
        && returned != expected
    {
        return Err(DaemonClientError::IncompatibleProtocol {
            reason: format!(
                "GetProjectState response request_id mismatch: requested {expected}, returned {returned}"
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    fn stop_operation() -> vz_runtime_contract::EnvironmentLifecycleOperation {
        use vz_runtime_contract::*;
        EnvironmentLifecycleOperation {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            operation_id: LifecycleOperationId::generate(),
            project_id: ProjectId::generate(),
            environment_id: EnvironmentId::generate(),
            kind: EnvironmentLifecycleKind::Stop,
            generation: 1,
            request_id: "req-test".into(),
            idempotency_key: "idem-test".into(),
            request_hash: "sha256:request".into(),
            definition_digest: "sha256:definition".into(),
            initial_state: EnvironmentState::Ready,
            requested_target: EnvironmentState::Stopped,
            status: EnvironmentLifecycleStatus::Running,
            machine_steps: vec![MachineLifecycleStep {
                machine_id: MachineId::generate(),
                initial_state: MachineState::Ready,
                target_state: Some(MachineState::Stopped),
                expected_incarnation: None,
                resulting_incarnation: None,
                resulting_activation: None,
                status: LifecycleStepStatus::Pending,
                failure_reason: None,
            }],
            cleanup_steps: vec![],
            created_at: 1,
            updated_at: 1,
            completed_at: None,
        }
    }

    fn validator(
        operation: &vz_runtime_contract::EnvironmentLifecycleOperation,
    ) -> StopEventValidator {
        StopEventValidator {
            request_id: operation.request_id.clone(),
            idempotency_key: operation.idempotency_key.clone(),
            project_id: operation.project_id.clone(),
            sequence: 0,
            operation_scope: None,
            expected_environment_id: None,
            terminal: false,
        }
    }

    fn frame(
        operation: &vz_runtime_contract::EnvironmentLifecycleOperation,
        sequence: u64,
    ) -> runtime_v2::StopEnvironmentEvent {
        runtime_v2::StopEnvironmentEvent {
            schema_version: 1,
            request_id: operation.request_id.clone(),
            sequence,
            operation: Some(
                vz_runtime_translate::environment_lifecycle_operation_to_proto(operation),
            ),
            terminal: false,
            error: None,
        }
    }

    #[test]
    fn stop_second_frame_cannot_change_immutable_scope() {
        use vz_runtime_contract::*;
        let original = stop_operation();
        let mut mutations = Vec::new();
        let mut changed = original.clone();
        changed.environment_id = EnvironmentId::generate();
        mutations.push(changed);
        let mut changed = original.clone();
        changed.generation += 1;
        mutations.push(changed);
        let mut changed = original.clone();
        changed.request_hash.push('x');
        mutations.push(changed);
        let mut changed = original.clone();
        changed.definition_digest.push('x');
        mutations.push(changed);
        let mut changed = original.clone();
        changed.machine_steps[0].machine_id = MachineId::generate();
        mutations.push(changed);
        let mut changed = original.clone();
        changed.machine_steps[0].initial_state = MachineState::Failed;
        mutations.push(changed);
        let mut changed = original.clone();
        changed.created_at = 0;
        mutations.push(changed);
        for changed in mutations {
            changed.validate_structure().unwrap();
            let mut validator = validator(&original);
            validator.validate_event(frame(&original, 0)).unwrap();
            assert!(validator.validate_event(frame(&changed, 1)).is_err());
        }
    }

    #[test]
    fn stop_progress_can_evolve_and_process_selection_pins_first_frame() {
        let original = stop_operation();
        let mut exact = validator(&original);
        exact.expected_environment_id = Some(original.environment_id.clone());
        exact.validate_event(frame(&original, 0)).unwrap();
        let mut progressed = original.clone();
        progressed.updated_at = 2;
        progressed.machine_steps[0].status = vz_runtime_contract::LifecycleStepStatus::Running;
        exact.validate_event(frame(&progressed, 1)).unwrap();
        let mut wrong = validator(&original);
        wrong.expected_environment_id = Some(vz_runtime_contract::EnvironmentId::generate());
        assert!(wrong.validate_event(frame(&original, 0)).is_err());
    }

    #[test]
    fn response_requires_correlation_even_when_request_metadata_was_omitted() {
        for returned in ["", " ", "\t\n"] {
            assert!(matches!(
                validate_response_request_id(None, returned),
                Err(DaemonClientError::IncompatibleProtocol { .. })
            ));
        }
        assert!(validate_response_request_id(None, "req-server-generated").is_ok());
    }

    #[test]
    fn explicit_response_correlation_must_match_exactly() {
        assert!(validate_response_request_id(Some("req-exact"), "req-exact").is_ok());
        for returned in ["", "req-other", " req-exact "] {
            assert!(matches!(
                validate_response_request_id(Some("req-exact"), returned),
                Err(DaemonClientError::IncompatibleProtocol { .. })
            ));
        }
    }
}
