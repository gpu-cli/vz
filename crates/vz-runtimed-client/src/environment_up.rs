//! Exact request-correlated Up snapshots. Slow observers may skip progress, not terminal receipts.
use crate::{DaemonClient, DaemonClientError, Result};
use std::time::Duration;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use vz_runtime_contract::*;
    fn fixture() -> (Validator, runtime_v2::UpEnvironmentEvent) {
        let definition:ProjectDefinition=serde_json::from_value(serde_json::json!({"schema_version":1,"project_id":ProjectId::generate(),"name":"up-client","environment":{"schema_version":1,"machines":[{"schema_version":1,"name":"app","profile":"hardened","target":{"os":"linux","arch":"aarch64","image":"fixture"}}]}})).unwrap();
        let request = EnvironmentUpRequest {
            definition,
            selection: EnvironmentSelectionContext::default(),
            path_hint: None,
            timeout_millis: 1000,
        };
        let hash = request.request_hash().unwrap();
        let admission = EnvironmentUpAdmission {
            schema_version: 1,
            project_id: request.definition.project_id.clone(),
            environment_id: EnvironmentId::generate(),
            machine_ids: vec![MachineId::generate()],
            definition_digest: request.definition.digest().unwrap(),
            request_id: "request".into(),
            idempotency_key: "key".into(),
            request_hash: hash.clone(),
            workspace_key: None,
            created_at: 1,
        };
        let event = EnvironmentUpProgress {
            schema_version: 1,
            sequence: 0,
            admission,
            phase: "admitted".into(),
            operation: None,
            completion: None,
        };
        let validator = Validator {
            request,
            metadata: runtime_v2::RequestMetadata {
                request_id: "request".into(),
                idempotency_key: "key".into(),
                trace_id: String::new(),
            },
            hash,
            admission: None,
            operation: None,
            sequence: None,
            terminal: false,
        };
        (
            validator,
            vz_runtime_translate::environment_up_progress_to_proto(&event),
        )
    }
    #[test]
    fn coalesced_sequences_are_allowed_but_duplicate_or_foreign_scope_is_not() {
        let (mut validator, event) = fixture();
        validator.event(event.clone()).unwrap();
        let mut second = event.clone();
        second.sequence = 4;
        second.phase = "preparing".into();
        validator.event(second.clone()).unwrap();
        assert!(validator.event(second.clone()).is_err());
        second.sequence = 5;
        second.admission.as_mut().unwrap().machine_ids = vec![MachineId::generate().to_string()];
        assert!(validator.event(second).is_err());
    }
    #[test]
    fn request_hash_and_process_environment_constrain_first_frame() {
        let (mut validator, mut event) = fixture();
        event.admission.as_mut().unwrap().request_hash = format!("sha256:{}", "a".repeat(64));
        assert!(validator.event(event).is_err());
        let (mut validator, event) = fixture();
        validator.request.selection.process_environment_id = Some(EnvironmentId::generate());
        assert!(validator.event(event).is_err());
    }
    #[test]
    fn typed_terminal_failure_roundtrips_once_and_cannot_become_success() {
        let (mut validator, event) = fixture();
        let mut progress =
            vz_runtime_translate::environment_up_progress_from_proto(&event).unwrap();
        progress.phase = "terminal".into();
        progress.completion = Some(EnvironmentUpCompletion {
            admission: progress.admission.clone(),
            operation: None,
            workspace_binding: None,
            error: Some(MachineError::new(
                MachineErrorCode::UnsupportedOperation,
                "no readiness evidence".into(),
                Some("request".into()),
                Default::default(),
            )),
            completed_at: 2,
        });
        let wire = vz_runtime_translate::environment_up_progress_to_proto(&progress);
        assert_eq!(validator.event(wire.clone()).unwrap(), progress);
        assert!(validator.event(wire).is_err());
        progress.completion.as_mut().unwrap().error = None;
        assert!(
            vz_runtime_translate::environment_up_progress_from_proto(
                &vz_runtime_translate::environment_up_progress_to_proto(&progress)
            )
            .is_err()
        );
    }
}
use vz_runtime_contract::{
    EnvironmentId, EnvironmentLifecycleOperation, EnvironmentUpAdmission, EnvironmentUpProgress,
    EnvironmentUpRequest,
};
use vz_runtime_proto::runtime_v2;

fn invalid(reason: impl Into<String>) -> DaemonClientError {
    DaemonClientError::IncompatibleProtocol {
        reason: reason.into(),
    }
}

struct Validator {
    request: EnvironmentUpRequest,
    metadata: runtime_v2::RequestMetadata,
    hash: String,
    admission: Option<EnvironmentUpAdmission>,
    operation: Option<EnvironmentLifecycleOperation>,
    sequence: Option<u64>,
    terminal: bool,
}
impl Validator {
    fn event(&mut self, event: runtime_v2::UpEnvironmentEvent) -> Result<EnvironmentUpProgress> {
        let event =
            vz_runtime_translate::environment_up_progress_from_proto(&event).map_err(invalid)?;
        let admission = &event.admission;
        let expected_environment = if self.request.selection.explicit.is_none() {
            self.request.selection.process_environment_id.as_ref()
        } else {
            None
        };
        if self.terminal
            || self
                .sequence
                .is_some_and(|sequence| event.sequence <= sequence)
            || admission.project_id != self.request.definition.project_id
            || admission.request_id != self.metadata.request_id
            || admission.idempotency_key != self.metadata.idempotency_key
            || admission.request_hash != self.hash
            || admission.definition_digest
                != self
                    .request
                    .definition
                    .digest()
                    .map_err(|error| invalid(error.to_string()))?
            || admission.workspace_key != self.request.selection.workspace_key
            || expected_environment.is_some_and(|id| id != &admission.environment_id)
            || self.admission.as_ref().is_some_and(|old| old != admission)
        {
            return Err(invalid(
                "Up stream immutable admission, selector, sequence or request correlation mismatch",
            ));
        }
        if let Some(operation) = &event.operation {
            let scope = crate::topology::immutable_stop_scope(operation);
            if self.operation.as_ref().is_some_and(|old| old != &scope) {
                return Err(invalid("Up stream immutable lifecycle scope changed"));
            }
            self.operation = Some(scope);
        } else if self.operation.is_some()
            && event
                .completion
                .as_ref()
                .is_none_or(|completion| completion.error.is_none())
        {
            return Err(invalid("Up stream silently lost its admitted lifecycle"));
        }
        self.sequence = Some(event.sequence);
        self.admission = Some(admission.clone());
        self.terminal = event.completion.is_some();
        Ok(event)
    }
}

pub struct EnvironmentUpStream {
    stream: tonic::Streaming<runtime_v2::UpEnvironmentEvent>,
    validator: Validator,
    timeout: Duration,
}
impl EnvironmentUpStream {
    /// Cancellation only ends observation. Replay the same IDs to learn the
    /// retained supervisor's receipt; do not generate a replacement mutation.
    pub async fn next_event(&mut self) -> Result<Option<EnvironmentUpProgress>> {
        let wire = tokio::time::timeout(self.timeout, self.stream.message())
            .await
            .map_err(|_| {
                DaemonClientError::Grpc(Box::new(tonic::Status::deadline_exceeded(
                    "Up observation deadline elapsed; original supervisor may continue",
                )))
            })?
            .map_err(|error| DaemonClientError::Grpc(Box::new(error)))?;
        let Some(wire) = wire else {
            if !self.validator.terminal {
                return Err(invalid("Up stream closed without a terminal receipt"));
            }
            return Ok(None);
        };
        self.validator.event(wire).map(Some)
    }
}

impl DaemonClient {
    pub async fn up_environment_stream(
        &mut self,
        request: runtime_v2::UpEnvironmentRequest,
    ) -> Result<EnvironmentUpStream> {
        let input =
            vz_runtime_translate::environment_up_request_from_proto(&request).map_err(invalid)?;
        let metadata = request
            .metadata
            .clone()
            .ok_or_else(|| invalid("Up requires request metadata"))?;
        if [&metadata.request_id, &metadata.idempotency_key]
            .iter()
            .any(|value| {
                value.is_empty()
                    || value.len() > 256
                    || value.trim() != value.as_str()
                    || value.chars().any(char::is_control)
            })
        {
            return Err(invalid(
                "Up requires bounded nonempty request/idempotency IDs",
            ));
        }
        if request.environment.is_none() {
            if let Some(id) = &request.process_environment_id {
                EnvironmentId::new(id.clone()).map_err(|error| invalid(error.to_string()))?;
            }
        }
        let hash = input.request_hash().map_err(invalid)?;
        let timeout = Duration::from_millis(input.timeout_millis) + Duration::from_secs(5);
        let response = tokio::time::timeout(
            Duration::from_secs(35),
            self.topology_client
                .up_environment(tonic::Request::new(request)),
        )
        .await
        .map_err(|_| {
            DaemonClientError::Grpc(Box::new(tonic::Status::deadline_exceeded(
                "Up admission observation timed out; replay exact request IDs",
            )))
        })?
        .map_err(|status| {
            crate::transport::status_to_client_error(&self.config.socket_path, status)
        })?;
        Ok(EnvironmentUpStream {
            stream: response.into_inner(),
            timeout,
            validator: Validator {
                request: input,
                metadata,
                hash,
                admission: None,
                operation: None,
                sequence: None,
                terminal: false,
            },
        })
    }
}
