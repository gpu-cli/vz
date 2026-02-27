use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Request, Status};
use vz_runtime_contract::{RequestMetadata, SandboxBackend};
use vz_runtime_proto::runtime_v2;
use vz_runtime_translate::request_metadata_from_proto;

use crate::DaemonHealth;

use super::ids::generate_request_id;

#[derive(Debug, Clone)]
struct RequestContext {
    request_id: String,
}

pub(in crate::grpc) fn daemon_backend(name: &str) -> SandboxBackend {
    match name {
        "macos_vz" | "macos-vz" => SandboxBackend::MacosVz,
        "linux_firecracker" | "linux-firecracker" => SandboxBackend::LinuxFirecracker,
        other => SandboxBackend::Other(other.to_string()),
    }
}

pub(in crate::grpc) fn normalize_metadata(
    wire_metadata: Option<&runtime_v2::RequestMetadata>,
    fallback_request_id: Option<String>,
) -> RequestMetadata {
    let mut metadata = wire_metadata
        .map(request_metadata_from_proto)
        .unwrap_or_default();
    if metadata.request_id.is_none() {
        metadata.request_id = fallback_request_id;
    }
    if metadata.request_id.is_none() {
        metadata.request_id = Some(generate_request_id());
    }
    metadata
}

pub(in crate::grpc) fn request_id_from_extensions<T>(request: &Request<T>) -> Option<String> {
    request
        .extensions()
        .get::<RequestContext>()
        .map(|context| context.request_id.clone())
}

pub(in crate::grpc) fn request_metadata_interceptor(
    mut request: Request<()>,
) -> Result<Request<()>, Status> {
    let request_id = request
        .metadata()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .unwrap_or_else(generate_request_id);

    request
        .extensions_mut()
        .insert(RequestContext { request_id });
    Ok(request)
}

pub(in crate::grpc) fn insert_health_headers(
    metadata: &mut MetadataMap,
    health: &DaemonHealth,
) -> Result<(), Status> {
    let daemon_id = MetadataValue::try_from(health.daemon_id.as_str())
        .map_err(|_| Status::internal("failed to encode daemon_id metadata header"))?;
    let daemon_version = MetadataValue::try_from(health.daemon_version.as_str())
        .map_err(|_| Status::internal("failed to encode daemon_version metadata header"))?;
    let backend_name = MetadataValue::try_from(health.backend_name.as_str())
        .map_err(|_| Status::internal("failed to encode backend metadata header"))?;
    let started_at = MetadataValue::try_from(health.started_at_unix_secs.to_string())
        .map_err(|_| Status::internal("failed to encode started_at metadata header"))?;

    metadata.insert("x-vz-runtimed-id", daemon_id);
    metadata.insert("x-vz-runtimed-version", daemon_version);
    metadata.insert("x-vz-runtimed-backend", backend_name);
    metadata.insert("x-vz-runtimed-started-at", started_at);
    Ok(())
}
