use tonic::Request;
use vz_runtime_proto::runtime_v2;

use crate::transport::status_to_client_error;
use crate::{DaemonClient, Result};

impl DaemonClient {
    /// Call Runtime V2 `StartBuild`.
    pub async fn start_build(
        &mut self,
        request: runtime_v2::StartBuildRequest,
    ) -> Result<runtime_v2::BuildResponse> {
        let response = self.start_build_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `StartBuild` and preserve gRPC response metadata.
    pub async fn start_build_with_metadata(
        &mut self,
        mut request: runtime_v2::StartBuildRequest,
    ) -> Result<tonic::Response<runtime_v2::BuildResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.build_client
            .start_build(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetBuild`.
    pub async fn get_build(
        &mut self,
        request: runtime_v2::GetBuildRequest,
    ) -> Result<runtime_v2::BuildResponse> {
        let response = self.get_build_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetBuild` and preserve gRPC response metadata.
    pub async fn get_build_with_metadata(
        &mut self,
        mut request: runtime_v2::GetBuildRequest,
    ) -> Result<tonic::Response<runtime_v2::BuildResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.build_client
            .get_build(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListBuilds`.
    pub async fn list_builds(
        &mut self,
        request: runtime_v2::ListBuildsRequest,
    ) -> Result<runtime_v2::ListBuildsResponse> {
        let response = self.list_builds_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListBuilds` and preserve gRPC response metadata.
    pub async fn list_builds_with_metadata(
        &mut self,
        mut request: runtime_v2::ListBuildsRequest,
    ) -> Result<tonic::Response<runtime_v2::ListBuildsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.build_client
            .list_builds(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CancelBuild`.
    pub async fn cancel_build(
        &mut self,
        request: runtime_v2::CancelBuildRequest,
    ) -> Result<runtime_v2::BuildResponse> {
        let response = self.cancel_build_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CancelBuild` and preserve gRPC response metadata.
    pub async fn cancel_build_with_metadata(
        &mut self,
        mut request: runtime_v2::CancelBuildRequest,
    ) -> Result<tonic::Response<runtime_v2::BuildResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.build_client
            .cancel_build(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `StreamBuildEvents`.
    pub async fn stream_build_events(
        &mut self,
        mut request: runtime_v2::StreamBuildEventsRequest,
    ) -> Result<tonic::Streaming<runtime_v2::BuildEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.build_client
            .stream_build_events(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }
}
