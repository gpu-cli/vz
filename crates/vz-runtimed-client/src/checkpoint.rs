use tonic::Request;
use tonic::Status;
use tonic::metadata::MetadataValue;
use vz_runtime_proto::runtime_v2;

use crate::stream_completion::{
    read_export_checkpoint_completion, read_import_checkpoint_completion,
};
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

    /// Call Runtime V2 `ExportCheckpoint`.
    pub async fn export_checkpoint(
        &mut self,
        request: runtime_v2::ExportCheckpointRequest,
    ) -> Result<runtime_v2::ExportCheckpointCompletion> {
        let response = self.export_checkpoint_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ExportCheckpoint` and preserve gRPC response metadata.
    pub async fn export_checkpoint_with_metadata(
        &mut self,
        request: runtime_v2::ExportCheckpointRequest,
    ) -> Result<tonic::Response<runtime_v2::ExportCheckpointCompletion>> {
        let response = self.export_checkpoint_stream_with_metadata(request).await?;
        let mut stream = response.into_inner();
        let completion =
            read_export_checkpoint_completion(&self.config.socket_path, &mut stream).await?;
        Ok(tonic::Response::new(completion))
    }

    /// Call Runtime V2 `ExportCheckpoint` as a server stream.
    pub async fn export_checkpoint_stream(
        &mut self,
        request: runtime_v2::ExportCheckpointRequest,
    ) -> Result<tonic::Streaming<runtime_v2::ExportCheckpointEvent>> {
        let response = self.export_checkpoint_stream_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ExportCheckpoint` as a server stream and preserve gRPC response metadata.
    pub async fn export_checkpoint_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::ExportCheckpointRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::ExportCheckpointEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .export_checkpoint(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ImportCheckpoint`.
    pub async fn import_checkpoint(
        &mut self,
        request: runtime_v2::ImportCheckpointRequest,
    ) -> Result<runtime_v2::CheckpointResponse> {
        let response = self.import_checkpoint_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ImportCheckpoint` and preserve gRPC response metadata.
    pub async fn import_checkpoint_with_metadata(
        &mut self,
        request: runtime_v2::ImportCheckpointRequest,
    ) -> Result<tonic::Response<runtime_v2::CheckpointResponse>> {
        let response = self.import_checkpoint_stream_with_metadata(request).await?;
        let mut stream = response.into_inner();
        let completion =
            read_import_checkpoint_completion(&self.config.socket_path, &mut stream).await?;
        let checkpoint_response = completion.response.ok_or_else(|| {
            status_to_client_error(
                &self.config.socket_path,
                Status::internal("import_checkpoint completion missing response payload"),
            )
        })?;
        let mut grpc_response = tonic::Response::new(checkpoint_response);
        if !completion.receipt_id.trim().is_empty()
            && let Ok(value) = MetadataValue::try_from(completion.receipt_id.as_str())
        {
            grpc_response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(grpc_response)
    }

    /// Call Runtime V2 `ImportCheckpoint` as a server stream.
    pub async fn import_checkpoint_stream(
        &mut self,
        request: runtime_v2::ImportCheckpointRequest,
    ) -> Result<tonic::Streaming<runtime_v2::ImportCheckpointEvent>> {
        let response = self.import_checkpoint_stream_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ImportCheckpoint` as a server stream and preserve gRPC response metadata.
    pub async fn import_checkpoint_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::ImportCheckpointRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::ImportCheckpointEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.checkpoint_client
            .import_checkpoint(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }
}
