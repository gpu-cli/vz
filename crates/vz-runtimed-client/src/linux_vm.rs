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
}
