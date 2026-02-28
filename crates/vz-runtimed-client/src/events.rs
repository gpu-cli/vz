use tonic::Request;
use vz_runtime_proto::runtime_v2;

use crate::transport::status_to_client_error;
use crate::{DaemonClient, Result};

impl DaemonClient {
    /// Call Runtime V2 `ListEvents`.
    pub async fn list_events(
        &mut self,
        request: runtime_v2::ListEventsRequest,
    ) -> Result<runtime_v2::ListEventsResponse> {
        let response = self.list_events_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListEvents` and preserve gRPC response metadata.
    pub async fn list_events_with_metadata(
        &mut self,
        mut request: runtime_v2::ListEventsRequest,
    ) -> Result<tonic::Response<runtime_v2::ListEventsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.event_client
            .list_events(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `StreamEvents`.
    pub async fn stream_events(
        &mut self,
        mut request: runtime_v2::StreamEventsRequest,
    ) -> Result<tonic::Streaming<runtime_v2::RuntimeEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.event_client
            .stream_events(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetReceipt`.
    pub async fn get_receipt(
        &mut self,
        request: runtime_v2::GetReceiptRequest,
    ) -> Result<runtime_v2::ReceiptResponse> {
        let response = self.get_receipt_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetReceipt` and preserve gRPC response metadata.
    pub async fn get_receipt_with_metadata(
        &mut self,
        mut request: runtime_v2::GetReceiptRequest,
    ) -> Result<tonic::Response<runtime_v2::ReceiptResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.receipt_client
            .get_receipt(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }
}
