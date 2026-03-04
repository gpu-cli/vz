use tonic::Request;
use vz_runtime_proto::runtime_v2;

use crate::transport::status_to_client_error;
use crate::{DaemonClient, Result};

impl DaemonClient {
    /// Call Runtime V2 `ValidateLinuxVm` as a server stream.
    pub async fn validate_linux_vm_stream(
        &mut self,
        request: runtime_v2::ValidateLinuxVmRequest,
    ) -> Result<tonic::Streaming<runtime_v2::ValidateLinuxVmEvent>> {
        let response = self.validate_linux_vm_stream_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ValidateLinuxVm` as a server stream and preserve gRPC response metadata.
    pub async fn validate_linux_vm_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::ValidateLinuxVmRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::ValidateLinuxVmEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.linux_vm_client
            .validate_linux_vm(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListLinuxVmBases`.
    pub async fn list_linux_vm_bases(
        &mut self,
        request: runtime_v2::ListLinuxVmBasesRequest,
    ) -> Result<runtime_v2::ListLinuxVmBasesResponse> {
        let response = self.list_linux_vm_bases_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListLinuxVmBases` and preserve gRPC response metadata.
    pub async fn list_linux_vm_bases_with_metadata(
        &mut self,
        mut request: runtime_v2::ListLinuxVmBasesRequest,
    ) -> Result<tonic::Response<runtime_v2::ListLinuxVmBasesResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.linux_vm_client
            .list_linux_vm_bases(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetLinuxVmBase`.
    pub async fn get_linux_vm_base(
        &mut self,
        request: runtime_v2::GetLinuxVmBaseRequest,
    ) -> Result<runtime_v2::LinuxVmBaseResponse> {
        let response = self.get_linux_vm_base_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetLinuxVmBase` and preserve gRPC response metadata.
    pub async fn get_linux_vm_base_with_metadata(
        &mut self,
        mut request: runtime_v2::GetLinuxVmBaseRequest,
    ) -> Result<tonic::Response<runtime_v2::LinuxVmBaseResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.linux_vm_client
            .get_linux_vm_base(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `UpsertLinuxVmBase` as a server stream.
    pub async fn upsert_linux_vm_base_stream(
        &mut self,
        request: runtime_v2::UpsertLinuxVmBaseRequest,
    ) -> Result<tonic::Streaming<runtime_v2::UpsertLinuxVmBaseEvent>> {
        let response = self
            .upsert_linux_vm_base_stream_with_metadata(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `UpsertLinuxVmBase` as a server stream and preserve metadata.
    pub async fn upsert_linux_vm_base_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::UpsertLinuxVmBaseRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::UpsertLinuxVmBaseEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.linux_vm_client
            .upsert_linux_vm_base(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `DeleteLinuxVmBase` as a server stream.
    pub async fn delete_linux_vm_base_stream(
        &mut self,
        request: runtime_v2::DeleteLinuxVmBaseRequest,
    ) -> Result<tonic::Streaming<runtime_v2::DeleteLinuxVmBaseEvent>> {
        let response = self
            .delete_linux_vm_base_stream_with_metadata(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `DeleteLinuxVmBase` as a server stream and preserve metadata.
    pub async fn delete_linux_vm_base_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::DeleteLinuxVmBaseRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::DeleteLinuxVmBaseEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.linux_vm_client
            .delete_linux_vm_base(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ApplyLinuxVmPatch` as a server stream.
    pub async fn apply_linux_vm_patch_stream(
        &mut self,
        request: runtime_v2::ApplyLinuxVmPatchRequest,
    ) -> Result<tonic::Streaming<runtime_v2::ApplyLinuxVmPatchEvent>> {
        let response = self
            .apply_linux_vm_patch_stream_with_metadata(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ApplyLinuxVmPatch` as a server stream and preserve metadata.
    pub async fn apply_linux_vm_patch_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::ApplyLinuxVmPatchRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::ApplyLinuxVmPatchEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.linux_vm_client
            .apply_linux_vm_patch(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `RollbackLinuxVmPatch` as a server stream.
    pub async fn rollback_linux_vm_patch_stream(
        &mut self,
        request: runtime_v2::RollbackLinuxVmPatchRequest,
    ) -> Result<tonic::Streaming<runtime_v2::RollbackLinuxVmPatchEvent>> {
        let response = self
            .rollback_linux_vm_patch_stream_with_metadata(request)
            .await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `RollbackLinuxVmPatch` as a server stream and preserve metadata.
    pub async fn rollback_linux_vm_patch_stream_with_metadata(
        &mut self,
        mut request: runtime_v2::RollbackLinuxVmPatchRequest,
    ) -> Result<tonic::Response<tonic::Streaming<runtime_v2::RollbackLinuxVmPatchEvent>>> {
        Self::ensure_metadata(&mut request.metadata);
        self.linux_vm_client
            .rollback_linux_vm_patch(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }
}
