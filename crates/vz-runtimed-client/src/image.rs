use tonic::Request;
use vz_runtime_proto::runtime_v2;

use crate::transport::status_to_client_error;
use crate::{DaemonClient, Result};

impl DaemonClient {
    /// Call Runtime V2 `GetImage`.
    pub async fn get_image(
        &mut self,
        request: runtime_v2::GetImageRequest,
    ) -> Result<runtime_v2::ImageResponse> {
        let response = self.get_image_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetImage` and preserve gRPC response metadata.
    pub async fn get_image_with_metadata(
        &mut self,
        mut request: runtime_v2::GetImageRequest,
    ) -> Result<tonic::Response<runtime_v2::ImageResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.image_client
            .get_image(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListImages`.
    pub async fn list_images(
        &mut self,
        request: runtime_v2::ListImagesRequest,
    ) -> Result<runtime_v2::ListImagesResponse> {
        let response = self.list_images_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListImages` and preserve gRPC response metadata.
    pub async fn list_images_with_metadata(
        &mut self,
        mut request: runtime_v2::ListImagesRequest,
    ) -> Result<tonic::Response<runtime_v2::ListImagesResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.image_client
            .list_images(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `PullImage` as a server stream.
    pub async fn pull_image(
        &mut self,
        mut request: runtime_v2::PullImageRequest,
    ) -> Result<tonic::Streaming<runtime_v2::PullImageEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.image_client
            .pull_image(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `PruneImages` as a server stream.
    pub async fn prune_images(
        &mut self,
        mut request: runtime_v2::PruneImagesRequest,
    ) -> Result<tonic::Streaming<runtime_v2::PruneImagesEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.image_client
            .prune_images(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }
}
