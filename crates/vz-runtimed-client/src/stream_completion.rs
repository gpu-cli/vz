use std::path::Path;

use tonic::Status;
use vz_runtime_proto::runtime_v2;

use crate::Result;
use crate::transport::status_to_client_error;

pub(crate) async fn read_create_sandbox_completion(
    socket_path: &Path,
    stream: &mut tonic::Streaming<runtime_v2::CreateSandboxEvent>,
) -> Result<runtime_v2::CreateSandboxCompletion> {
    while let Some(event) = stream
        .message()
        .await
        .map_err(|status| status_to_client_error(socket_path, status))?
    {
        if let Some(runtime_v2::create_sandbox_event::Payload::Completion(completion)) =
            event.payload
        {
            return Ok(completion);
        }
    }
    Err(status_to_client_error(
        socket_path,
        Status::internal("create_sandbox stream ended without terminal completion event"),
    ))
}

pub(crate) async fn read_terminate_sandbox_completion(
    socket_path: &Path,
    stream: &mut tonic::Streaming<runtime_v2::TerminateSandboxEvent>,
) -> Result<runtime_v2::TerminateSandboxCompletion> {
    while let Some(event) = stream
        .message()
        .await
        .map_err(|status| status_to_client_error(socket_path, status))?
    {
        if let Some(runtime_v2::terminate_sandbox_event::Payload::Completion(completion)) =
            event.payload
        {
            return Ok(completion);
        }
    }
    Err(status_to_client_error(
        socket_path,
        Status::internal("terminate_sandbox stream ended without terminal completion event"),
    ))
}

pub(crate) async fn read_prepare_space_cache_completion(
    socket_path: &Path,
    stream: &mut tonic::Streaming<runtime_v2::PrepareSpaceCacheEvent>,
) -> Result<runtime_v2::PrepareSpaceCacheCompletion> {
    while let Some(event) = stream
        .message()
        .await
        .map_err(|status| status_to_client_error(socket_path, status))?
    {
        if let Some(runtime_v2::prepare_space_cache_event::Payload::Completion(completion)) =
            event.payload
        {
            return Ok(completion);
        }
    }
    Err(status_to_client_error(
        socket_path,
        Status::internal("prepare_space_cache stream ended without terminal completion event"),
    ))
}

pub(crate) async fn read_export_space_cache_completion(
    socket_path: &Path,
    stream: &mut tonic::Streaming<runtime_v2::ExportSpaceCacheEvent>,
) -> Result<runtime_v2::ExportSpaceCacheCompletion> {
    while let Some(event) = stream
        .message()
        .await
        .map_err(|status| status_to_client_error(socket_path, status))?
    {
        if let Some(runtime_v2::export_space_cache_event::Payload::Completion(completion)) =
            event.payload
        {
            return Ok(completion);
        }
    }
    Err(status_to_client_error(
        socket_path,
        Status::internal("export_space_cache stream ended without terminal completion event"),
    ))
}

pub(crate) async fn read_import_space_cache_completion(
    socket_path: &Path,
    stream: &mut tonic::Streaming<runtime_v2::ImportSpaceCacheEvent>,
) -> Result<runtime_v2::ImportSpaceCacheCompletion> {
    while let Some(event) = stream
        .message()
        .await
        .map_err(|status| status_to_client_error(socket_path, status))?
    {
        if let Some(runtime_v2::import_space_cache_event::Payload::Completion(completion)) =
            event.payload
        {
            return Ok(completion);
        }
    }
    Err(status_to_client_error(
        socket_path,
        Status::internal("import_space_cache stream ended without terminal completion event"),
    ))
}

pub(crate) async fn read_apply_stack_completion(
    socket_path: &Path,
    stream: &mut tonic::Streaming<runtime_v2::ApplyStackEvent>,
) -> Result<runtime_v2::ApplyStackCompletion> {
    while let Some(event) = stream
        .message()
        .await
        .map_err(|status| status_to_client_error(socket_path, status))?
    {
        if let Some(runtime_v2::apply_stack_event::Payload::Completion(completion)) = event.payload
        {
            return Ok(completion);
        }
    }
    Err(status_to_client_error(
        socket_path,
        Status::internal("apply_stack stream ended without terminal completion event"),
    ))
}

pub(crate) async fn read_teardown_stack_completion(
    socket_path: &Path,
    stream: &mut tonic::Streaming<runtime_v2::TeardownStackEvent>,
) -> Result<runtime_v2::TeardownStackCompletion> {
    while let Some(event) = stream
        .message()
        .await
        .map_err(|status| status_to_client_error(socket_path, status))?
    {
        if let Some(runtime_v2::teardown_stack_event::Payload::Completion(completion)) =
            event.payload
        {
            return Ok(completion);
        }
    }
    Err(status_to_client_error(
        socket_path,
        Status::internal("teardown_stack stream ended without terminal completion event"),
    ))
}

pub(crate) async fn read_stack_service_action_completion(
    socket_path: &Path,
    stream: &mut tonic::Streaming<runtime_v2::StackServiceActionEvent>,
) -> Result<runtime_v2::StackServiceActionCompletion> {
    while let Some(event) = stream
        .message()
        .await
        .map_err(|status| status_to_client_error(socket_path, status))?
    {
        if let Some(runtime_v2::stack_service_action_event::Payload::Completion(completion)) =
            event.payload
        {
            return Ok(completion);
        }
    }
    Err(status_to_client_error(
        socket_path,
        Status::internal("stack_service_action stream ended without terminal completion event"),
    ))
}

pub(crate) async fn read_export_checkpoint_completion(
    socket_path: &Path,
    stream: &mut tonic::Streaming<runtime_v2::ExportCheckpointEvent>,
) -> Result<runtime_v2::ExportCheckpointCompletion> {
    while let Some(event) = stream
        .message()
        .await
        .map_err(|status| status_to_client_error(socket_path, status))?
    {
        if let Some(runtime_v2::export_checkpoint_event::Payload::Completion(completion)) =
            event.payload
        {
            return Ok(completion);
        }
    }
    Err(status_to_client_error(
        socket_path,
        Status::internal("export_checkpoint stream ended without terminal completion event"),
    ))
}

pub(crate) async fn read_import_checkpoint_completion(
    socket_path: &Path,
    stream: &mut tonic::Streaming<runtime_v2::ImportCheckpointEvent>,
) -> Result<runtime_v2::ImportCheckpointCompletion> {
    while let Some(event) = stream
        .message()
        .await
        .map_err(|status| status_to_client_error(socket_path, status))?
    {
        if let Some(runtime_v2::import_checkpoint_event::Payload::Completion(completion)) =
            event.payload
        {
            return Ok(completion);
        }
    }
    Err(status_to_client_error(
        socket_path,
        Status::internal("import_checkpoint stream ended without terminal completion event"),
    ))
}
