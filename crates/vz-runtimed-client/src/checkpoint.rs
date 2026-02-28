use tonic::Request;
use vz_runtime_proto::runtime_v2;

use crate::transport::status_to_client_error;
use crate::{DaemonClient, Result};

impl DaemonClient {
    /// Call Runtime V2 `CreateCheckpoint`.
    pub async fn create_checkpoint(
        &mut self,
        request: runtime_v2::CreateCheckpointRequest,
    ) -> Result<runtime_v2::CheckpointResponse> {
        let response = self.create_checkpoint_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CreateCheckpoint` and preserve gRPC response metadata.
    pub async fn create_checkpoint_with_metadata(
        &mut self,
        mut request: runtime_v2::CreateCheckpointRequest,
    ) -> Result<tonic::Response<runtime_v2::CheckpointResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .create_checkpoint(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetCheckpoint`.
    pub async fn get_checkpoint(
        &mut self,
        request: runtime_v2::GetCheckpointRequest,
    ) -> Result<runtime_v2::CheckpointResponse> {
        let response = self.get_checkpoint_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetCheckpoint` and preserve gRPC response metadata.
    pub async fn get_checkpoint_with_metadata(
        &mut self,
        mut request: runtime_v2::GetCheckpointRequest,
    ) -> Result<tonic::Response<runtime_v2::CheckpointResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .get_checkpoint(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListCheckpoints`.
    pub async fn list_checkpoints(
        &mut self,
        request: runtime_v2::ListCheckpointsRequest,
    ) -> Result<runtime_v2::ListCheckpointsResponse> {
        let response = self.list_checkpoints_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListCheckpoints` and preserve gRPC response metadata.
    pub async fn list_checkpoints_with_metadata(
        &mut self,
        mut request: runtime_v2::ListCheckpointsRequest,
    ) -> Result<tonic::Response<runtime_v2::ListCheckpointsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .list_checkpoints(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `RestoreCheckpoint`.
    pub async fn restore_checkpoint(
        &mut self,
        request: runtime_v2::RestoreCheckpointRequest,
    ) -> Result<runtime_v2::CheckpointResponse> {
        let response = self.restore_checkpoint_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `RestoreCheckpoint` and preserve gRPC response metadata.
    pub async fn restore_checkpoint_with_metadata(
        &mut self,
        mut request: runtime_v2::RestoreCheckpointRequest,
    ) -> Result<tonic::Response<runtime_v2::CheckpointResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .restore_checkpoint(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ForkCheckpoint`.
    pub async fn fork_checkpoint(
        &mut self,
        request: runtime_v2::ForkCheckpointRequest,
    ) -> Result<runtime_v2::CheckpointResponse> {
        let response = self.fork_checkpoint_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ForkCheckpoint` and preserve gRPC response metadata.
    pub async fn fork_checkpoint_with_metadata(
        &mut self,
        mut request: runtime_v2::ForkCheckpointRequest,
    ) -> Result<tonic::Response<runtime_v2::CheckpointResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .fork_checkpoint(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `DiffCheckpoints`.
    pub async fn diff_checkpoints(
        &mut self,
        request: runtime_v2::DiffCheckpointsRequest,
    ) -> Result<runtime_v2::DiffCheckpointsResponse> {
        let response = self.diff_checkpoints_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `DiffCheckpoints` and preserve gRPC response metadata.
    pub async fn diff_checkpoints_with_metadata(
        &mut self,
        mut request: runtime_v2::DiffCheckpointsRequest,
    ) -> Result<tonic::Response<runtime_v2::DiffCheckpointsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .diff_checkpoints(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }
}
