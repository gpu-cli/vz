use tonic::{Code, Request, Status};
use vz_runtime_contract::{ProjectId, ProjectState};
use vz_runtime_proto::runtime_v2;
use vz_runtime_translate::project_state_from_proto;

use crate::transport::status_to_client_error;
use crate::{DaemonClient, DaemonClientError, Result};

/// Canonically validated Project topology returned by the runtime daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectStateSnapshot {
    pub request_id: String,
    pub project: ProjectState,
}

impl DaemonClient {
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
    use super::*;

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
