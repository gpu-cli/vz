use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

mod handlers;
mod support;

use thiserror::Error;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};
use vz_runtime_contract::{
    Build, BuildSpec, BuildState, Checkpoint, CheckpointClass, CheckpointState, Container,
    ContainerSpec, ContainerState, Execution, ExecutionSpec, ExecutionState, Lease, LeaseState,
    MachineError, MachineErrorCode, RuntimeOperation, SANDBOX_LABEL_BASE_IMAGE_DEFAULT_SOURCE,
    SANDBOX_LABEL_BASE_IMAGE_REF, SANDBOX_LABEL_MAIN_CONTAINER,
    SANDBOX_LABEL_MAIN_CONTAINER_DEFAULT_SOURCE, SANDBOX_LABEL_PROJECT_DIR,
    SANDBOX_LABEL_SPACE_MODE, SANDBOX_LABEL_SPACE_SECRET_ENV_PREFIX, SANDBOX_SPACE_MODE_REQUIRED,
    Sandbox, SandboxSpec, SandboxState,
};
use vz_runtime_proto::runtime_v2;
use vz_runtime_translate::{
    build_to_proto_payload, checkpoint_to_proto_payload, container_to_proto_payload,
    execution_to_proto_payload, lease_to_proto_payload, runtime_capabilities_to_proto,
    sandbox_to_proto_payload,
};
use vz_stack::{IDEMPOTENCY_TTL_SECS, IdempotencyRecord, Receipt, StackError, StackEvent};

use crate::RuntimeDaemon;
use handlers::build::BuildServiceImpl;
use handlers::capability::CapabilityServiceImpl;
use handlers::checkpoint::CheckpointServiceImpl;
use handlers::container::ContainerServiceImpl;
use handlers::event::EventServiceImpl;
use handlers::execution::ExecutionServiceImpl;
use handlers::file::FileServiceImpl;
use handlers::image::ImageServiceImpl;
use handlers::lease::LeaseServiceImpl;
use handlers::receipt::ReceiptServiceImpl;
use handlers::sandbox::SandboxServiceImpl;
use handlers::stack::StackServiceImpl;
use support::*;

#[derive(Debug, Error)]
pub enum RuntimedServerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

#[cfg(test)]
const IDEMPOTENCY_CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const IDEMPOTENCY_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
#[cfg(test)]
const ORPHAN_SHELL_EXECUTION_GRACE_SECS: u64 = 1;
#[cfg(not(test))]
const ORPHAN_SHELL_EXECUTION_GRACE_SECS: u64 = 300;

fn reconcile_orphaned_shell_executions(daemon: &RuntimeDaemon) -> Result<u64, StackError> {
    let now = current_unix_secs();
    let executions = daemon.with_state_store(|store| store.list_executions())?;
    let mut reconciled = 0;

    for mut execution in executions {
        if execution.state.is_terminal() {
            continue;
        }
        if !execution.exec_spec.pty {
            continue;
        }
        if execution
            .exec_spec
            .env_override
            .get(SANDBOX_SHELL_SESSION_ENV_KEY)
            .is_none_or(|value| value != "1")
        {
            continue;
        }

        let has_session = daemon
            .execution_sessions()
            .contains(&execution.execution_id)
            .map_err(|_| StackError::Machine {
                code: MachineErrorCode::InternalError,
                message: "execution session registry lock poisoned".to_string(),
            })?;
        if has_session {
            continue;
        }

        let started_at = execution.started_at.unwrap_or(now);
        if now.saturating_sub(started_at) < ORPHAN_SHELL_EXECUTION_GRACE_SECS {
            continue;
        }

        if execution.started_at.is_none() {
            execution.started_at = Some(now);
        }
        execution.ended_at = Some(now);
        execution.exit_code = Some(130);
        execution
            .transition_to(ExecutionState::Canceled)
            .map_err(|error| StackError::Machine {
                code: MachineErrorCode::StateConflict,
                message: error.to_string(),
            })?;

        daemon.with_state_store(|store| {
            store.with_immediate_transaction(|tx| {
                tx.save_execution(&execution)?;
                tx.emit_event(
                    "daemon",
                    &StackEvent::ExecutionCanceled {
                        execution_id: execution.execution_id.clone(),
                    },
                )?;
                Ok(())
            })
        })?;
        let _ = daemon.execution_sessions().remove(&execution.execution_id);
        reconciled += 1;
    }

    Ok(reconciled)
}

async fn run_maintenance_loop(daemon: Arc<RuntimeDaemon>, shutdown: Arc<tokio::sync::Notify>) {
    let mut ticker = tokio::time::interval(IDEMPOTENCY_CLEANUP_INTERVAL);
    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            _ = ticker.tick() => {
                match daemon.refresh_placement_snapshot() {
                    Ok(snapshot) => {
                        debug!(
                            active_sandboxes = snapshot.active_sandboxes,
                            active_containers = snapshot.active_containers,
                            active_executions = snapshot.active_executions,
                            placement_snapshot_updated_at = snapshot.updated_at_unix_secs,
                            "daemon maintenance: refreshed placement snapshot"
                        );
                    }
                    Err(error) => {
                        warn!(error = %error, "daemon maintenance: failed to refresh placement snapshot");
                    }
                }
                match daemon.with_state_store(|store| store.cleanup_expired_idempotency_keys()) {
                    Ok(deleted) => {
                        if deleted > 0 {
                            debug!(deleted_idempotency_records = deleted, "daemon maintenance: cleaned expired idempotency keys");
                        }
                    }
                    Err(error) => {
                        warn!(error = %error, "daemon maintenance: failed to clean expired idempotency keys");
                    }
                }
                match reconcile_orphaned_shell_executions(daemon.as_ref()) {
                    Ok(reconciled) => {
                        if reconciled > 0 {
                            debug!(
                                reconciled_executions = reconciled,
                                "daemon maintenance: reconciled orphaned shell executions"
                            );
                        }
                    }
                    Err(error) => {
                        warn!(error = %error, "daemon maintenance: failed to reconcile orphaned shell executions");
                    }
                }
            }
        }
    }
}

/// Run Runtime V2 gRPC services on a Unix socket with graceful shutdown.
pub async fn serve_runtime_uds_with_shutdown<F>(
    daemon: Arc<RuntimeDaemon>,
    socket_path: impl AsRef<Path>,
    shutdown: F,
) -> Result<(), RuntimedServerError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let socket_path = socket_path.as_ref();

    if socket_path.exists() {
        std::fs::remove_file(socket_path)?;
    }

    let listener = UnixListener::bind(socket_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))?;
    }

    let incoming = UnixListenerStream::new(listener);
    let maintenance_shutdown = Arc::new(tokio::sync::Notify::new());
    let maintenance_task = tokio::spawn(run_maintenance_loop(
        daemon.clone(),
        maintenance_shutdown.clone(),
    ));

    let sandbox_service =
        runtime_v2::sandbox_service_server::SandboxServiceServer::with_interceptor(
            SandboxServiceImpl::new(daemon.clone()),
            request_metadata_interceptor,
        );
    let lease_service = runtime_v2::lease_service_server::LeaseServiceServer::with_interceptor(
        LeaseServiceImpl::new(daemon.clone()),
        request_metadata_interceptor,
    );
    let container_service =
        runtime_v2::container_service_server::ContainerServiceServer::with_interceptor(
            ContainerServiceImpl::new(daemon.clone()),
            request_metadata_interceptor,
        );
    let image_service = runtime_v2::image_service_server::ImageServiceServer::with_interceptor(
        ImageServiceImpl::new(daemon.clone()),
        request_metadata_interceptor,
    );
    let file_service = runtime_v2::file_service_server::FileServiceServer::with_interceptor(
        FileServiceImpl::new(daemon.clone()),
        request_metadata_interceptor,
    );
    let build_service = runtime_v2::build_service_server::BuildServiceServer::with_interceptor(
        BuildServiceImpl::new(daemon.clone()),
        request_metadata_interceptor,
    );
    let execution_service =
        runtime_v2::execution_service_server::ExecutionServiceServer::with_interceptor(
            ExecutionServiceImpl::new(daemon.clone()),
            request_metadata_interceptor,
        );
    let checkpoint_service =
        runtime_v2::checkpoint_service_server::CheckpointServiceServer::with_interceptor(
            CheckpointServiceImpl::new(daemon.clone()),
            request_metadata_interceptor,
        );
    let event_service = runtime_v2::event_service_server::EventServiceServer::with_interceptor(
        EventServiceImpl::new(daemon.clone()),
        request_metadata_interceptor,
    );
    let receipt_service =
        runtime_v2::receipt_service_server::ReceiptServiceServer::with_interceptor(
            ReceiptServiceImpl::new(daemon.clone()),
            request_metadata_interceptor,
        );
    let capability_service =
        runtime_v2::capability_service_server::CapabilityServiceServer::with_interceptor(
            CapabilityServiceImpl::new(daemon.clone()),
            request_metadata_interceptor,
        );
    let stack_service = runtime_v2::stack_service_server::StackServiceServer::with_interceptor(
        StackServiceImpl::new(daemon.clone()),
        request_metadata_interceptor,
    );

    debug!(socket_path = %socket_path.display(), "starting runtime UDS gRPC server");
    let server_result = Server::builder()
        .add_service(sandbox_service)
        .add_service(lease_service)
        .add_service(container_service)
        .add_service(image_service)
        .add_service(file_service)
        .add_service(build_service)
        .add_service(execution_service)
        .add_service(checkpoint_service)
        .add_service(event_service)
        .add_service(receipt_service)
        .add_service(stack_service)
        .add_service(capability_service)
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await;

    maintenance_shutdown.notify_waiters();
    let _ = maintenance_task.await;

    server_result?;

    Ok(())
}

#[cfg(test)]
mod tests;
