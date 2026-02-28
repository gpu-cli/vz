use tonic::Request;
use vz_runtime_proto::runtime_v2;

use crate::transport::status_to_client_error;
use crate::{DaemonClient, Result};

impl DaemonClient {
    /// Call Runtime V2 `CreateExecution`.
    pub async fn create_execution(
        &mut self,
        request: runtime_v2::CreateExecutionRequest,
    ) -> Result<runtime_v2::ExecutionResponse> {
        let response = self.create_execution_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CreateExecution` and preserve gRPC response metadata.
    pub async fn create_execution_with_metadata(
        &mut self,
        mut request: runtime_v2::CreateExecutionRequest,
    ) -> Result<tonic::Response<runtime_v2::ExecutionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .create_execution(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `GetExecution`.
    pub async fn get_execution(
        &mut self,
        request: runtime_v2::GetExecutionRequest,
    ) -> Result<runtime_v2::ExecutionResponse> {
        let response = self.get_execution_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `GetExecution` and preserve gRPC response metadata.
    pub async fn get_execution_with_metadata(
        &mut self,
        mut request: runtime_v2::GetExecutionRequest,
    ) -> Result<tonic::Response<runtime_v2::ExecutionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .get_execution(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ListExecutions`.
    pub async fn list_executions(
        &mut self,
        request: runtime_v2::ListExecutionsRequest,
    ) -> Result<runtime_v2::ListExecutionsResponse> {
        let response = self.list_executions_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ListExecutions` and preserve gRPC response metadata.
    pub async fn list_executions_with_metadata(
        &mut self,
        mut request: runtime_v2::ListExecutionsRequest,
    ) -> Result<tonic::Response<runtime_v2::ListExecutionsResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .list_executions(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `CancelExecution`.
    pub async fn cancel_execution(
        &mut self,
        request: runtime_v2::CancelExecutionRequest,
    ) -> Result<runtime_v2::ExecutionResponse> {
        let response = self.cancel_execution_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `CancelExecution` and preserve gRPC response metadata.
    pub async fn cancel_execution_with_metadata(
        &mut self,
        mut request: runtime_v2::CancelExecutionRequest,
    ) -> Result<tonic::Response<runtime_v2::ExecutionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .cancel_execution(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `StreamExecOutput`.
    pub async fn stream_exec_output(
        &mut self,
        mut request: runtime_v2::StreamExecOutputRequest,
    ) -> Result<tonic::Streaming<runtime_v2::ExecOutputEvent>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .stream_exec_output(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `WriteExecStdin`.
    pub async fn write_exec_stdin(
        &mut self,
        request: runtime_v2::WriteExecStdinRequest,
    ) -> Result<runtime_v2::ExecutionResponse> {
        let response = self.write_exec_stdin_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `WriteExecStdin` and preserve gRPC response metadata.
    pub async fn write_exec_stdin_with_metadata(
        &mut self,
        mut request: runtime_v2::WriteExecStdinRequest,
    ) -> Result<tonic::Response<runtime_v2::ExecutionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .write_exec_stdin(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `ResizeExecPty`.
    pub async fn resize_exec_pty(
        &mut self,
        request: runtime_v2::ResizeExecPtyRequest,
    ) -> Result<runtime_v2::ExecutionResponse> {
        let response = self.resize_exec_pty_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `ResizeExecPty` and preserve gRPC response metadata.
    pub async fn resize_exec_pty_with_metadata(
        &mut self,
        mut request: runtime_v2::ResizeExecPtyRequest,
    ) -> Result<tonic::Response<runtime_v2::ExecutionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .resize_exec_pty(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }

    /// Call Runtime V2 `SignalExec`.
    pub async fn signal_exec(
        &mut self,
        request: runtime_v2::SignalExecRequest,
    ) -> Result<runtime_v2::ExecutionResponse> {
        let response = self.signal_exec_with_metadata(request).await?;
        Ok(response.into_inner())
    }

    /// Call Runtime V2 `SignalExec` and preserve gRPC response metadata.
    pub async fn signal_exec_with_metadata(
        &mut self,
        mut request: runtime_v2::SignalExecRequest,
    ) -> Result<tonic::Response<runtime_v2::ExecutionResponse>> {
        Self::ensure_metadata(&mut request.metadata);
        self.execution_client
            .signal_exec(Request::new(request))
            .await
            .map_err(|status| status_to_client_error(&self.config.socket_path, status))
    }
}
