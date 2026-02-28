use tonic::metadata::MetadataValue;
use tonic::{Request, Status};
use vz_runtime_proto::runtime_v2;

use crate::stream_completion::{
    read_apply_stack_completion, read_stack_service_action_completion,
    read_teardown_stack_completion,
};
use crate::transport::status_to_client_error;
use crate::{DaemonClient, Result};

impl DaemonClient {
    /// Call Runtime V2 `ApplyStack`.
    pub async fn apply_stack(
        &mut self,
        request: runtime_v2::ApplyStackRequest,
    ) -> Result<runtime_v2::ApplyStackResponse> {
        let response = self.apply_stack_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ApplyStack` and preserve gRPC response metadata.
    pub async fn apply_stack_with_metadata(
        &mut self,
        request: runtime_v2::ApplyStackRequest,
    ) -> Result<tonic::Response<runtime_v2::ApplyStackResponse>> {
        let response = self.apply_stack_stream_with_metadata(request).await?;
        let mut stream = response.into_inner();
        let completion = read_apply_stack_completion(&self.config.socket_path, &mut stream).await?;
        let apply_response = completion.response.ok_or_else(|| {
            status_to_client_error(
                &self.config.socket_path,
                Status::internal("apply_stack completion missing response payload"),
            )
        })?;

        let mut grpc_response = tonic::Response::new(apply_response);
        if !completion.receipt_id.trim().is_empty()
            && let Ok(value) = MetadataValue::try_from(completion.receipt_id.as_str())
        {
            grpc_response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(grpc_response)
    }

    /// Call Runtime V2 `ApplyStack` as a server stream.
    pub async fn apply_stack_stream(
        &mut self,
        request: runtime_v2::ApplyStackRequest,
    ) -> Result<tonic::Streaming<runtime_v2::ApplyStackEvent>> {
        let response = self.apply_stack_stream_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ApplyStack` as a server stream and preserve gRPC response metadata.
    pub async fn apply_stack_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::ApplyStackRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::ApplyStackEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .apply_stack(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `TeardownStack`.
    pub async fn teardown_stack(
        &mut self,
        request: runtime_v2::TeardownStackRequest,
    ) -> Result<runtime_v2::TeardownStackResponse> {
        let response = self.teardown_stack_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `TeardownStack` and preserve gRPC response metadata.
    pub async fn teardown_stack_with_metadata(
        &mut self,
        request: runtime_v2::TeardownStackRequest,
    ) -> Result<tonic::Response<runtime_v2::TeardownStackResponse>> {
        let response = self.teardown_stack_stream_with_metadata(request).await?;
        let mut stream = response.into_inner();
        let completion =
            read_teardown_stack_completion(&self.config.socket_path, &mut stream).await?;
        let teardown_response = completion.response.ok_or_else(|| {
            status_to_client_error(
                &self.config.socket_path,
                Status::internal("teardown_stack completion missing response payload"),
            )
        })?;

        let mut grpc_response = tonic::Response::new(teardown_response);
        if !completion.receipt_id.trim().is_empty()
            && let Ok(value) = MetadataValue::try_from(completion.receipt_id.as_str())
        {
            grpc_response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(grpc_response)
    }

    /// Call Runtime V2 `TeardownStack` as a server stream.
    pub async fn teardown_stack_stream(
        &mut self,
        request: runtime_v2::TeardownStackRequest,
    ) -> Result<tonic::Streaming<runtime_v2::TeardownStackEvent>> {
        let response = self.teardown_stack_stream_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `TeardownStack` as a server stream and preserve gRPC response metadata.
    pub async fn teardown_stack_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::TeardownStackRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::TeardownStackEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .teardown_stack(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetStackStatus`.
    pub async fn get_stack_status(
        &mut self,
        request: runtime_v2::GetStackStatusRequest,
    ) -> Result<runtime_v2::GetStackStatusResponse> {
        let response = self.get_stack_status_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetStackStatus` and preserve gRPC response metadata.
    pub async fn get_stack_status_with_metadata(
        &mut self,
        mut request: runtime_v2::GetStackStatusRequest,
    ) -> Result<tonic::Response<runtime_v2::GetStackStatusResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .get_stack_status(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListStackEvents`.
    pub async fn list_stack_events(
        &mut self,
        request: runtime_v2::ListStackEventsRequest,
    ) -> Result<runtime_v2::ListStackEventsResponse> {
        let response = self.list_stack_events_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListStackEvents` and preserve gRPC response metadata.
    pub async fn list_stack_events_with_metadata(
        &mut self,
        mut request: runtime_v2::ListStackEventsRequest,
    ) -> Result<tonic::Response<runtime_v2::ListStackEventsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .list_stack_events(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetStackLogs`.
    pub async fn get_stack_logs(
        &mut self,
        request: runtime_v2::GetStackLogsRequest,
    ) -> Result<runtime_v2::GetStackLogsResponse> {
        let response = self.get_stack_logs_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetStackLogs` and preserve gRPC response metadata.
    pub async fn get_stack_logs_with_metadata(
        &mut self,
        mut request: runtime_v2::GetStackLogsRequest,
    ) -> Result<tonic::Response<runtime_v2::GetStackLogsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .get_stack_logs(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `StopStackService`.
    pub async fn stop_stack_service(
        &mut self,
        request: runtime_v2::StackServiceActionRequest,
    ) -> Result<runtime_v2::StackServiceActionResponse> {
        let response = self.stop_stack_service_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `StopStackService` and preserve gRPC response metadata.
    pub async fn stop_stack_service_with_metadata(
        &mut self,
        request: runtime_v2::StackServiceActionRequest,
    ) -> Result<tonic::Response<runtime_v2::StackServiceActionResponse>> {
        let response = self
            .stop_stack_service_stream_with_metadata(request)
            .await?;
        let mut stream = response.into_inner();
        let completion =
            read_stack_service_action_completion(&self.config.socket_path, &mut stream).await?;
        let service_response = completion.response.ok_or_else(|| {
            status_to_client_error(
                &self.config.socket_path,
                Status::internal("stop_stack_service completion missing response payload"),
            )
        })?;

        let mut grpc_response = tonic::Response::new(service_response);
        if !completion.receipt_id.trim().is_empty()
            && let Ok(value) = MetadataValue::try_from(completion.receipt_id.as_str())
        {
            grpc_response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(grpc_response)
    }

    /// Call Runtime V2 `StopStackService` as a server stream.
    pub async fn stop_stack_service_stream(
        &mut self,
        request: runtime_v2::StackServiceActionRequest,
    ) -> Result<tonic::Streaming<runtime_v2::StackServiceActionEvent>> {
        let response = self
            .stop_stack_service_stream_with_metadata(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `StopStackService` as a server stream and preserve gRPC response metadata.
    pub async fn stop_stack_service_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::StackServiceActionRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::StackServiceActionEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .stop_stack_service(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `StartStackService`.
    pub async fn start_stack_service(
        &mut self,
        request: runtime_v2::StackServiceActionRequest,
    ) -> Result<runtime_v2::StackServiceActionResponse> {
        let response = self.start_stack_service_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `StartStackService` and preserve gRPC response metadata.
    pub async fn start_stack_service_with_metadata(
        &mut self,
        request: runtime_v2::StackServiceActionRequest,
    ) -> Result<tonic::Response<runtime_v2::StackServiceActionResponse>> {
        let response = self
            .start_stack_service_stream_with_metadata(request)
            .await?;
        let mut stream = response.into_inner();
        let completion =
            read_stack_service_action_completion(&self.config.socket_path, &mut stream).await?;
        let service_response = completion.response.ok_or_else(|| {
            status_to_client_error(
                &self.config.socket_path,
                Status::internal("start_stack_service completion missing response payload"),
            )
        })?;

        let mut grpc_response = tonic::Response::new(service_response);
        if !completion.receipt_id.trim().is_empty()
            && let Ok(value) = MetadataValue::try_from(completion.receipt_id.as_str())
        {
            grpc_response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(grpc_response)
    }

    /// Call Runtime V2 `StartStackService` as a server stream.
    pub async fn start_stack_service_stream(
        &mut self,
        request: runtime_v2::StackServiceActionRequest,
    ) -> Result<tonic::Streaming<runtime_v2::StackServiceActionEvent>> {
        let response = self
            .start_stack_service_stream_with_metadata(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `StartStackService` as a server stream and preserve gRPC response metadata.
    pub async fn start_stack_service_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::StackServiceActionRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::StackServiceActionEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .start_stack_service(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `RestartStackService`.
    pub async fn restart_stack_service(
        &mut self,
        request: runtime_v2::StackServiceActionRequest,
    ) -> Result<runtime_v2::StackServiceActionResponse> {
        let response = self.restart_stack_service_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `RestartStackService` and preserve gRPC response metadata.
    pub async fn restart_stack_service_with_metadata(
        &mut self,
        request: runtime_v2::StackServiceActionRequest,
    ) -> Result<tonic::Response<runtime_v2::StackServiceActionResponse>> {
        let response = self
            .restart_stack_service_stream_with_metadata(request)
            .await?;
        let mut stream = response.into_inner();
        let completion =
            read_stack_service_action_completion(&self.config.socket_path, &mut stream).await?;
        let service_response = completion.response.ok_or_else(|| {
            status_to_client_error(
                &self.config.socket_path,
                Status::internal("restart_stack_service completion missing response payload"),
            )
        })?;

        let mut grpc_response = tonic::Response::new(service_response);
        if !completion.receipt_id.trim().is_empty()
            && let Ok(value) = MetadataValue::try_from(completion.receipt_id.as_str())
        {
            grpc_response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(grpc_response)
    }

    /// Call Runtime V2 `RestartStackService` as a server stream.
    pub async fn restart_stack_service_stream(
        &mut self,
        request: runtime_v2::StackServiceActionRequest,
    ) -> Result<tonic::Streaming<runtime_v2::StackServiceActionEvent>> {
        let response = self
            .restart_stack_service_stream_with_metadata(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `RestartStackService` as a server stream and preserve gRPC response metadata.
    pub async fn restart_stack_service_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::StackServiceActionRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::StackServiceActionEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .restart_stack_service(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CreateStackRunContainer`.
    pub async fn create_stack_run_container(
        &mut self,
        request: runtime_v2::StackRunContainerRequest,
    ) -> Result<runtime_v2::StackRunContainerResponse> {
        let response = self
            .create_stack_run_container_with_metadata(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CreateStackRunContainer` and preserve gRPC response metadata.
    pub async fn create_stack_run_container_with_metadata(
        &mut self,
        mut request: runtime_v2::StackRunContainerRequest,
    ) -> Result<tonic::Response<runtime_v2::StackRunContainerResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .create_stack_run_container(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `RemoveStackRunContainer`.
    pub async fn remove_stack_run_container(
        &mut self,
        request: runtime_v2::StackRunContainerRequest,
    ) -> Result<runtime_v2::StackRunContainerResponse> {
        let response = self
            .remove_stack_run_container_with_metadata(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `RemoveStackRunContainer` and preserve gRPC response metadata.
    pub async fn remove_stack_run_container_with_metadata(
        &mut self,
        mut request: runtime_v2::StackRunContainerRequest,
    ) -> Result<tonic::Response<runtime_v2::StackRunContainerResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.stack_client
            .remove_stack_run_container(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }
}
