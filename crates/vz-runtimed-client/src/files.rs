use tonic::Request;
use vz_runtime_proto::runtime_v2;

use crate::transport::status_to_client_error;
use crate::{DaemonClient, Result};

impl DaemonClient {
    /// Call Runtime V2 `ReadFile`.
    pub async fn read_file(
        &mut self,
        request: runtime_v2::ReadFileRequest,
    ) -> Result<runtime_v2::ReadFileResponse> {
        let response = self.read_file_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ReadFile` and preserve gRPC response metadata.
    pub async fn read_file_with_metadata(
        &mut self,
        mut request: runtime_v2::ReadFileRequest,
    ) -> Result<tonic::Response<runtime_v2::ReadFileResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .read_file(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `WriteFile`.
    pub async fn write_file(
        &mut self,
        request: runtime_v2::WriteFileRequest,
    ) -> Result<runtime_v2::WriteFileResponse> {
        let response = self.write_file_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `WriteFile` and preserve gRPC response metadata.
    pub async fn write_file_with_metadata(
        &mut self,
        mut request: runtime_v2::WriteFileRequest,
    ) -> Result<tonic::Response<runtime_v2::WriteFileResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .write_file(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListFiles`.
    pub async fn list_files(
        &mut self,
        request: runtime_v2::ListFilesRequest,
    ) -> Result<runtime_v2::ListFilesResponse> {
        let response = self.list_files_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListFiles` and preserve gRPC response metadata.
    pub async fn list_files_with_metadata(
        &mut self,
        mut request: runtime_v2::ListFilesRequest,
    ) -> Result<tonic::Response<runtime_v2::ListFilesResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .list_files(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `MakeDir`.
    pub async fn make_dir(
        &mut self,
        request: runtime_v2::MakeDirRequest,
    ) -> Result<runtime_v2::FileMutationResponse> {
        let response = self.make_dir_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `MakeDir` and preserve gRPC response metadata.
    pub async fn make_dir_with_metadata(
        &mut self,
        mut request: runtime_v2::MakeDirRequest,
    ) -> Result<tonic::Response<runtime_v2::FileMutationResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .make_dir(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `RemovePath`.
    pub async fn remove_path(
        &mut self,
        request: runtime_v2::RemovePathRequest,
    ) -> Result<runtime_v2::FileMutationResponse> {
        let response = self.remove_path_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `RemovePath` and preserve gRPC response metadata.
    pub async fn remove_path_with_metadata(
        &mut self,
        mut request: runtime_v2::RemovePathRequest,
    ) -> Result<tonic::Response<runtime_v2::FileMutationResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .remove_path(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `MovePath`.
    pub async fn move_path(
        &mut self,
        request: runtime_v2::MovePathRequest,
    ) -> Result<runtime_v2::FileMutationResponse> {
        let response = self.move_path_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `MovePath` and preserve gRPC response metadata.
    pub async fn move_path_with_metadata(
        &mut self,
        mut request: runtime_v2::MovePathRequest,
    ) -> Result<tonic::Response<runtime_v2::FileMutationResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .move_path(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CopyPath`.
    pub async fn copy_path(
        &mut self,
        request: runtime_v2::CopyPathRequest,
    ) -> Result<runtime_v2::FileMutationResponse> {
        let response = self.copy_path_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CopyPath` and preserve gRPC response metadata.
    pub async fn copy_path_with_metadata(
        &mut self,
        mut request: runtime_v2::CopyPathRequest,
    ) -> Result<tonic::Response<runtime_v2::FileMutationResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .copy_path(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ChmodPath`.
    pub async fn chmod_path(
        &mut self,
        request: runtime_v2::ChmodPathRequest,
    ) -> Result<runtime_v2::FileMutationResponse> {
        let response = self.chmod_path_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ChmodPath` and preserve gRPC response metadata.
    pub async fn chmod_path_with_metadata(
        &mut self,
        mut request: runtime_v2::ChmodPathRequest,
    ) -> Result<tonic::Response<runtime_v2::FileMutationResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .chmod_path(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ChownPath`.
    pub async fn chown_path(
        &mut self,
        request: runtime_v2::ChownPathRequest,
    ) -> Result<runtime_v2::FileMutationResponse> {
        let response = self.chown_path_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ChownPath` and preserve gRPC response metadata.
    pub async fn chown_path_with_metadata(
        &mut self,
        mut request: runtime_v2::ChownPathRequest,
    ) -> Result<tonic::Response<runtime_v2::FileMutationResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.file_client
            .chown_path(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }
}
