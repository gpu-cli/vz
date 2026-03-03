use std::collections::HashMap;

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};
use vz_runtime_contract::RequestMetadata;
use vz_runtime_proto::runtime_v2;
use vz_runtime_translate::request_metadata_to_proto;

use crate::stream_completion::{
    read_create_sandbox_completion, read_prepare_space_cache_completion,
    read_terminate_sandbox_completion,
};
use crate::transport::status_to_client_error;
use crate::{DaemonClient, Result};

impl DaemonClient {
    /// Call Runtime V2 `CreateSandbox`.
    pub async fn create_sandbox(
        &mut self,
        request: runtime_v2::CreateSandboxRequest,
    ) -> Result<runtime_v2::SandboxResponse> {
        let response = self.create_sandbox_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CreateSandbox` and preserve gRPC response metadata.
    pub async fn create_sandbox_with_metadata(
        &mut self,
        request: runtime_v2::CreateSandboxRequest,
    ) -> Result<tonic::Response<runtime_v2::SandboxResponse>> {
        let response = self.create_sandbox_stream_with_metadata(request).await?;
        let mut stream = response.into_inner();
        let completion =
            read_create_sandbox_completion(&self.config.socket_path, &mut stream).await?;
        let sandbox_response = completion.response.ok_or_else(|| {
            status_to_client_error(
                &self.config.socket_path,
                Status::internal("create_sandbox completion missing response payload"),
            )
        })?;

        let mut grpc_response = tonic::Response::new(sandbox_response);
        if !completion.receipt_id.trim().is_empty()
            && let Ok(value) = MetadataValue::try_from(completion.receipt_id.as_str())
        {
            grpc_response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(grpc_response)
    }

    /// Call Runtime V2 `CreateSandbox` as a server stream.
    pub async fn create_sandbox_stream(
        &mut self,
        request: runtime_v2::CreateSandboxRequest,
    ) -> Result<tonic::Streaming<runtime_v2::CreateSandboxEvent>> {
        let response = self.create_sandbox_stream_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CreateSandbox` as a server stream and preserve gRPC response metadata.
    pub async fn create_sandbox_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::CreateSandboxRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::CreateSandboxEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .create_sandbox(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetSandbox`.
    pub async fn get_sandbox(
        &mut self,
        request: runtime_v2::GetSandboxRequest,
    ) -> Result<runtime_v2::SandboxResponse> {
        let response = self.get_sandbox_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetSandbox` and preserve gRPC response metadata.
    pub async fn get_sandbox_with_metadata(
        &mut self,
        mut request: runtime_v2::GetSandboxRequest,
    ) -> Result<tonic::Response<runtime_v2::SandboxResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .get_sandbox(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `PrepareSpaceCache`.
    pub async fn prepare_space_cache(
        &mut self,
        request: runtime_v2::PrepareSpaceCacheRequest,
    ) -> Result<runtime_v2::PrepareSpaceCacheCompletion> {
        let response = self.prepare_space_cache_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `PrepareSpaceCache` and preserve gRPC response metadata.
    pub async fn prepare_space_cache_with_metadata(
        &mut self,
        request: runtime_v2::PrepareSpaceCacheRequest,
    ) -> Result<tonic::Response<runtime_v2::PrepareSpaceCacheCompletion>> {
        let response = self
            .prepare_space_cache_stream_with_metadata(request)
            .await?;
        let mut stream = response.into_inner();
        let completion =
            read_prepare_space_cache_completion(&self.config.socket_path, &mut stream).await?;
        let receipt_id = completion.receipt_id.clone();
        let mut grpc_response = tonic::Response::new(completion);
        if !receipt_id.trim().is_empty()
            && let Ok(value) = MetadataValue::try_from(receipt_id.as_str())
        {
            grpc_response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(grpc_response)
    }

    /// Call Runtime V2 `PrepareSpaceCache` as a server stream.
    pub async fn prepare_space_cache_stream(
        &mut self,
        request: runtime_v2::PrepareSpaceCacheRequest,
    ) -> Result<tonic::Streaming<runtime_v2::PrepareSpaceCacheEvent>> {
        let response = self
            .prepare_space_cache_stream_with_metadata(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `PrepareSpaceCache` as a server stream and preserve metadata.
    pub async fn prepare_space_cache_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::PrepareSpaceCacheRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::PrepareSpaceCacheEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .prepare_space_cache(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListSandboxes`.
    pub async fn list_sandboxes(
        &mut self,
        request: runtime_v2::ListSandboxesRequest,
    ) -> Result<runtime_v2::ListSandboxesResponse> {
        let response = self.list_sandboxes_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListSandboxes` and preserve gRPC response metadata.
    pub async fn list_sandboxes_with_metadata(
        &mut self,
        mut request: runtime_v2::ListSandboxesRequest,
    ) -> Result<tonic::Response<runtime_v2::ListSandboxesResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .list_sandboxes(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `TerminateSandbox`.
    pub async fn terminate_sandbox(
        &mut self,
        request: runtime_v2::TerminateSandboxRequest,
    ) -> Result<runtime_v2::SandboxResponse> {
        let response = self.terminate_sandbox_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `TerminateSandbox` and preserve gRPC response metadata.
    pub async fn terminate_sandbox_with_metadata(
        &mut self,
        request: runtime_v2::TerminateSandboxRequest,
    ) -> Result<tonic::Response<runtime_v2::SandboxResponse>> {
        let response = self.terminate_sandbox_stream_with_metadata(request).await?;
        let mut stream = response.into_inner();
        let completion =
            read_terminate_sandbox_completion(&self.config.socket_path, &mut stream).await?;
        let sandbox_response = completion.response.ok_or_else(|| {
            status_to_client_error(
                &self.config.socket_path,
                Status::internal("terminate_sandbox completion missing response payload"),
            )
        })?;

        let mut grpc_response = tonic::Response::new(sandbox_response);
        if !completion.receipt_id.trim().is_empty()
            && let Ok(value) = MetadataValue::try_from(completion.receipt_id.as_str())
        {
            grpc_response.metadata_mut().insert("x-receipt-id", value);
        }
        Ok(grpc_response)
    }

    /// Call Runtime V2 `TerminateSandbox` as a server stream.
    pub async fn terminate_sandbox_stream(
        &mut self,
        request: runtime_v2::TerminateSandboxRequest,
    ) -> Result<tonic::Streaming<runtime_v2::TerminateSandboxEvent>> {
        let response = self.terminate_sandbox_stream_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `TerminateSandbox` as a server stream and preserve gRPC response metadata.
    pub async fn terminate_sandbox_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::TerminateSandboxRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::TerminateSandboxEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .terminate_sandbox(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `OpenSandboxShell` as a server stream.
    pub async fn open_sandbox_shell(
        &mut self,
        mut request: runtime_v2::OpenSandboxShellRequest,
    ) -> Result<tonic::Streaming<runtime_v2::OpenSandboxShellEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .open_sandbox_shell(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CloseSandboxShell` as a server stream.
    pub async fn close_sandbox_shell(
        &mut self,
        mut request: runtime_v2::CloseSandboxShellRequest,
    ) -> Result<tonic::Streaming<runtime_v2::CloseSandboxShellEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.sandbox_client
            .close_sandbox_shell(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Convenience helper for `CreateSandbox`.
    pub async fn create_sandbox_for_stack(&mut self, stack_name: impl Into<String>) -> Result<()> {
        let request = runtime_v2::CreateSandboxRequest {
            metadata: Some(request_metadata_to_proto(&RequestMetadata::default())),
            stack_name: stack_name.into(),
            cpus: 0,
            memory_mb: 0,
            labels: HashMap::new(),
        };
        let _ = self.create_sandbox(request).await?;
        Ok(())
    }

    /// Call Runtime V2 `OpenLease`.
    pub async fn open_lease(
        &mut self,
        request: runtime_v2::OpenLeaseRequest,
    ) -> Result<runtime_v2::LeaseResponse> {
        let response = self.open_lease_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `OpenLease` and preserve gRPC response metadata.
    pub async fn open_lease_with_metadata(
        &mut self,
        mut request: runtime_v2::OpenLeaseRequest,
    ) -> Result<tonic::Response<runtime_v2::LeaseResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.lease_client
            .open_lease(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetLease`.
    pub async fn get_lease(
        &mut self,
        request: runtime_v2::GetLeaseRequest,
    ) -> Result<runtime_v2::LeaseResponse> {
        let response = self.get_lease_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetLease` and preserve gRPC response metadata.
    pub async fn get_lease_with_metadata(
        &mut self,
        mut request: runtime_v2::GetLeaseRequest,
    ) -> Result<tonic::Response<runtime_v2::LeaseResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.lease_client
            .get_lease(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListLeases`.
    pub async fn list_leases(
        &mut self,
        request: runtime_v2::ListLeasesRequest,
    ) -> Result<runtime_v2::ListLeasesResponse> {
        let response = self.list_leases_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListLeases` and preserve gRPC response metadata.
    pub async fn list_leases_with_metadata(
        &mut self,
        mut request: runtime_v2::ListLeasesRequest,
    ) -> Result<tonic::Response<runtime_v2::ListLeasesResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.lease_client
            .list_leases(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `HeartbeatLease`.
    pub async fn heartbeat_lease(
        &mut self,
        request: runtime_v2::HeartbeatLeaseRequest,
    ) -> Result<runtime_v2::LeaseResponse> {
        let response = self.heartbeat_lease_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `HeartbeatLease` and preserve gRPC response metadata.
    pub async fn heartbeat_lease_with_metadata(
        &mut self,
        mut request: runtime_v2::HeartbeatLeaseRequest,
    ) -> Result<tonic::Response<runtime_v2::LeaseResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.lease_client
            .heartbeat_lease(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CloseLease`.
    pub async fn close_lease(
        &mut self,
        request: runtime_v2::CloseLeaseRequest,
    ) -> Result<runtime_v2::LeaseResponse> {
        let response = self.close_lease_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CloseLease` and preserve gRPC response metadata.
    pub async fn close_lease_with_metadata(
        &mut self,
        mut request: runtime_v2::CloseLeaseRequest,
    ) -> Result<tonic::Response<runtime_v2::LeaseResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.lease_client
            .close_lease(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }
}
