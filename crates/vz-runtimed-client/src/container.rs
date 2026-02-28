use tonic::Request;
use vz_runtime_proto::runtime_v2;

use crate::transport::status_to_client_error;
use crate::{DaemonClient, Result};

impl DaemonClient {
    /// Call Runtime V2 `CreateContainer`.
    pub async fn create_container(
        &mut self,
        request: runtime_v2::CreateContainerRequest,
    ) -> Result<runtime_v2::ContainerResponse> {
        let response = self.create_container_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CreateContainer` and preserve gRPC response metadata.
    pub async fn create_container_with_metadata(
        &mut self,
        mut request: runtime_v2::CreateContainerRequest,
    ) -> Result<tonic::Response<runtime_v2::ContainerResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.container_client
            .create_container(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetContainer`.
    pub async fn get_container(
        &mut self,
        request: runtime_v2::GetContainerRequest,
    ) -> Result<runtime_v2::ContainerResponse> {
        let response = self.get_container_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetContainer` and preserve gRPC response metadata.
    pub async fn get_container_with_metadata(
        &mut self,
        mut request: runtime_v2::GetContainerRequest,
    ) -> Result<tonic::Response<runtime_v2::ContainerResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.container_client
            .get_container(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListContainers`.
    pub async fn list_containers(
        &mut self,
        request: runtime_v2::ListContainersRequest,
    ) -> Result<runtime_v2::ListContainersResponse> {
        let response = self.list_containers_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListContainers` and preserve gRPC response metadata.
    pub async fn list_containers_with_metadata(
        &mut self,
        mut request: runtime_v2::ListContainersRequest,
    ) -> Result<tonic::Response<runtime_v2::ListContainersResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.container_client
            .list_containers(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `RemoveContainer`.
    pub async fn remove_container(
        &mut self,
        request: runtime_v2::RemoveContainerRequest,
    ) -> Result<runtime_v2::ContainerResponse> {
        let response = self.remove_container_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `RemoveContainer` and preserve gRPC response metadata.
    pub async fn remove_container_with_metadata(
        &mut self,
        mut request: runtime_v2::RemoveContainerRequest,
    ) -> Result<tonic::Response<runtime_v2::ContainerResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.container_client
            .remove_container(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }
}
