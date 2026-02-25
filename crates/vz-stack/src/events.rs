//! Structured event types for stack lifecycle observability.
//!
//! Events are emitted by the reconciler and persisted in the
//! [`StateStore`](crate::StateStore). API consumers can stream
//! events incrementally using [`StateStore::load_events_since`].

use serde::{Deserialize, Serialize};
use vz_runtime_contract::{
    RequestMetadata, RuntimeExtensionFailureKind, RuntimeExtensionPoint,
    map_runtime_extension_failure,
};

use crate::error::StackError;

/// Structured event emitted during stack lifecycle operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum StackEvent {
    /// Reconciliation started for a stack.
    #[serde(rename = "stack_apply_started")]
    StackApplyStarted {
        /// Stack name.
        stack_name: String,
        /// Number of services in the spec.
        services_count: usize,
    },
    /// Reconciliation completed for a stack.
    #[serde(rename = "stack_apply_completed")]
    StackApplyCompleted {
        /// Stack name.
        stack_name: String,
        /// Number of actions that succeeded.
        succeeded: usize,
        /// Number of actions that failed.
        failed: usize,
    },
    /// Reconciliation failed for a stack.
    #[serde(rename = "stack_apply_failed")]
    StackApplyFailed {
        /// Stack name.
        stack_name: String,
        /// Error description.
        error: String,
    },
    /// A service is being created.
    #[serde(rename = "service_creating")]
    ServiceCreating {
        /// Stack name.
        stack_name: String,
        /// Service name.
        service_name: String,
    },
    /// A service is ready and running.
    #[serde(rename = "service_ready")]
    ServiceReady {
        /// Stack name.
        stack_name: String,
        /// Service name.
        service_name: String,
        /// Runtime container identifier.
        runtime_id: String,
    },
    /// A service is being stopped.
    #[serde(rename = "service_stopping")]
    ServiceStopping {
        /// Stack name.
        stack_name: String,
        /// Service name.
        service_name: String,
    },
    /// A service has stopped.
    #[serde(rename = "service_stopped")]
    ServiceStopped {
        /// Stack name.
        stack_name: String,
        /// Service name.
        service_name: String,
        /// Exit code from the container process.
        exit_code: i32,
    },
    /// A service failed to start or crashed.
    #[serde(rename = "service_failed")]
    ServiceFailed {
        /// Stack name.
        stack_name: String,
        /// Service name.
        service_name: String,
        /// Error description.
        error: String,
    },
    /// A host port conflict was detected.
    #[serde(rename = "port_conflict")]
    PortConflict {
        /// Stack name.
        stack_name: String,
        /// Service name that requested the port.
        service_name: String,
        /// Conflicting host port.
        port: u16,
    },
    /// A named volume was created or mounted.
    #[serde(rename = "volume_created")]
    VolumeCreated {
        /// Stack name.
        stack_name: String,
        /// Volume name.
        volume_name: String,
    },
    /// A stack is being destroyed (all services torn down).
    #[serde(rename = "stack_destroyed")]
    StackDestroyed {
        /// Stack name.
        stack_name: String,
    },
    /// A service health check passed.
    #[serde(rename = "health_check_passed")]
    HealthCheckPassed {
        /// Stack name.
        stack_name: String,
        /// Service name.
        service_name: String,
    },
    /// A service health check failed.
    #[serde(rename = "health_check_failed")]
    HealthCheckFailed {
        /// Stack name.
        stack_name: String,
        /// Service name.
        service_name: String,
        /// Consecutive failure count.
        attempt: u32,
        /// Error description.
        error: String,
    },
    /// A service is blocked waiting on dependencies.
    #[serde(rename = "dependency_blocked")]
    DependencyBlocked {
        /// Stack name.
        stack_name: String,
        /// Service that is waiting.
        service_name: String,
        /// Dependencies not yet ready.
        waiting_on: Vec<String>,
    },
    /// A running service requires recreate because mount topology changed.
    #[serde(rename = "mount_topology_recreate_required")]
    MountTopologyRecreateRequired {
        /// Stack name.
        stack_name: String,
        /// Service requiring recreate.
        service_name: String,
        /// Previously persisted mount digest, if available.
        previous_digest: Option<String>,
        /// Desired mount digest from the current spec.
        desired_digest: String,
    },
    /// A sandbox is being created for a stack.
    #[serde(rename = "sandbox_creating")]
    SandboxCreating {
        stack_name: String,
        sandbox_id: String,
    },
    /// A sandbox is ready and accepting workloads.
    #[serde(rename = "sandbox_ready")]
    SandboxReady {
        stack_name: String,
        sandbox_id: String,
    },
    /// A sandbox is draining (no new work accepted).
    #[serde(rename = "sandbox_draining")]
    SandboxDraining {
        stack_name: String,
        sandbox_id: String,
    },
    /// A sandbox has been terminated.
    #[serde(rename = "sandbox_terminated")]
    SandboxTerminated {
        stack_name: String,
        sandbox_id: String,
    },
    /// A sandbox failed irrecoverably.
    #[serde(rename = "sandbox_failed")]
    SandboxFailed {
        stack_name: String,
        sandbox_id: String,
        error: String,
    },
    /// A lease is being opened.
    #[serde(rename = "lease_opened")]
    LeaseOpened {
        sandbox_id: String,
        lease_id: String,
    },
    /// A lease heartbeat was received.
    #[serde(rename = "lease_heartbeat")]
    LeaseHeartbeat { lease_id: String },
    /// A lease expired due to missed heartbeat.
    #[serde(rename = "lease_expired")]
    LeaseExpired { lease_id: String },
    /// A lease was explicitly closed.
    #[serde(rename = "lease_closed")]
    LeaseClosed { lease_id: String },
    /// A lease failed.
    #[serde(rename = "lease_failed")]
    LeaseFailed { lease_id: String, error: String },
    /// An execution was queued for a container.
    #[serde(rename = "execution_queued")]
    ExecutionQueued {
        /// Target container identifier.
        container_id: String,
        /// Execution identifier.
        execution_id: String,
    },
    /// An execution started running.
    #[serde(rename = "execution_running")]
    ExecutionRunning {
        /// Execution identifier.
        execution_id: String,
    },
    /// An execution exited naturally.
    #[serde(rename = "execution_exited")]
    ExecutionExited {
        /// Execution identifier.
        execution_id: String,
        /// Process exit code.
        exit_code: i32,
    },
    /// An execution failed unexpectedly.
    #[serde(rename = "execution_failed")]
    ExecutionFailed {
        /// Execution identifier.
        execution_id: String,
        /// Error description.
        error: String,
    },
    /// An execution was canceled by the caller.
    #[serde(rename = "execution_canceled")]
    ExecutionCanceled {
        /// Execution identifier.
        execution_id: String,
    },
    /// A checkpoint is being created for a sandbox.
    #[serde(rename = "checkpoint_creating")]
    CheckpointCreating {
        /// Owning sandbox identifier.
        sandbox_id: String,
        /// Checkpoint identifier.
        checkpoint_id: String,
        /// Checkpoint class (e.g. "fs_quick", "vm_full").
        class: String,
    },
    /// A checkpoint is ready for restore/fork.
    #[serde(rename = "checkpoint_ready")]
    CheckpointReady {
        /// Checkpoint identifier.
        checkpoint_id: String,
    },
    /// A checkpoint operation failed.
    #[serde(rename = "checkpoint_failed")]
    CheckpointFailed {
        /// Checkpoint identifier.
        checkpoint_id: String,
        /// Error description.
        error: String,
    },
    /// A checkpoint was restored to a sandbox.
    #[serde(rename = "checkpoint_restored")]
    CheckpointRestored {
        /// Checkpoint identifier.
        checkpoint_id: String,
        /// Sandbox that was restored to.
        sandbox_id: String,
    },
    /// A checkpoint was forked into a new sandbox/checkpoint.
    #[serde(rename = "checkpoint_forked")]
    CheckpointForked {
        /// Source checkpoint identifier.
        parent_checkpoint_id: String,
        /// New checkpoint identifier.
        new_checkpoint_id: String,
        /// New sandbox identifier.
        new_sandbox_id: String,
    },
}

/// Persisted event record with metadata from the store.
#[derive(Debug, Clone, PartialEq)]
pub struct EventRecord {
    /// Auto-incremented event identifier used for streaming cursors.
    pub id: i64,
    /// Stack name this event belongs to.
    pub stack_name: String,
    /// Timestamp when the event was stored (ISO 8601).
    pub created_at: String,
    /// The structured event payload.
    pub event: StackEvent,
}

/// Event sink error type for extension adapters.
pub type StackEventSinkError = Box<dyn std::error::Error + Send + Sync>;

/// Generic sink interface for forwarding stack events to integrations.
pub trait StackEventSink: Send + Sync {
    fn emit(
        &self,
        event: &StackEvent,
        metadata: &RequestMetadata,
    ) -> Result<(), StackEventSinkError>;
}

/// Closure-backed sink adapter for simple integration points.
pub struct FnStackEventSink<F>
where
    F: Fn(&StackEvent, &RequestMetadata) -> Result<(), StackEventSinkError> + Send + Sync,
{
    emit_fn: F,
}

impl<F> FnStackEventSink<F>
where
    F: Fn(&StackEvent, &RequestMetadata) -> Result<(), StackEventSinkError> + Send + Sync,
{
    pub fn new(emit_fn: F) -> Self {
        Self { emit_fn }
    }
}

impl<F> StackEventSink for FnStackEventSink<F>
where
    F: Fn(&StackEvent, &RequestMetadata) -> Result<(), StackEventSinkError> + Send + Sync,
{
    fn emit(
        &self,
        event: &StackEvent,
        metadata: &RequestMetadata,
    ) -> Result<(), StackEventSinkError> {
        (self.emit_fn)(event, metadata)
    }
}

/// Emit a stack event through an extension sink with stable error mapping.
pub fn emit_event_to_sink(
    sink: &dyn StackEventSink,
    event: &StackEvent,
    metadata: &RequestMetadata,
) -> Result<(), StackError> {
    sink.emit(event, metadata).map_err(|error| {
        StackError::from(map_runtime_extension_failure(
            RuntimeExtensionPoint::EventSink,
            "stack.emit_event",
            RuntimeExtensionFailureKind::Transport,
            error.to_string(),
        ))
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::io;
    use std::sync::{Arc, Mutex};

    use super::*;
    use vz_runtime_contract::MachineErrorCode;

    fn sample_events() -> Vec<StackEvent> {
        vec![
            StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 3,
            },
            StackEvent::StackApplyCompleted {
                stack_name: "myapp".to_string(),
                succeeded: 2,
                failed: 1,
            },
            StackEvent::StackApplyFailed {
                stack_name: "myapp".to_string(),
                error: "boom".to_string(),
            },
            StackEvent::ServiceCreating {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
            StackEvent::ServiceReady {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
                runtime_id: "ctr-123".to_string(),
            },
            StackEvent::ServiceStopping {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
            StackEvent::ServiceStopped {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
                exit_code: 0,
            },
            StackEvent::ServiceFailed {
                stack_name: "myapp".to_string(),
                service_name: "db".to_string(),
                error: "oom".to_string(),
            },
            StackEvent::PortConflict {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
                port: 8080,
            },
            StackEvent::VolumeCreated {
                stack_name: "myapp".to_string(),
                volume_name: "dbdata".to_string(),
            },
            StackEvent::StackDestroyed {
                stack_name: "myapp".to_string(),
            },
            StackEvent::HealthCheckPassed {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
            StackEvent::HealthCheckFailed {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
                attempt: 3,
                error: "connection refused".to_string(),
            },
            StackEvent::DependencyBlocked {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
                waiting_on: vec!["db".to_string()],
            },
            StackEvent::MountTopologyRecreateRequired {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
                previous_digest: Some("old".to_string()),
                desired_digest: "new".to_string(),
            },
            StackEvent::SandboxCreating {
                stack_name: "myapp".to_string(),
                sandbox_id: "sb-1".to_string(),
            },
            StackEvent::SandboxReady {
                stack_name: "myapp".to_string(),
                sandbox_id: "sb-1".to_string(),
            },
            StackEvent::SandboxDraining {
                stack_name: "myapp".to_string(),
                sandbox_id: "sb-1".to_string(),
            },
            StackEvent::SandboxTerminated {
                stack_name: "myapp".to_string(),
                sandbox_id: "sb-1".to_string(),
            },
            StackEvent::SandboxFailed {
                stack_name: "myapp".to_string(),
                sandbox_id: "sb-1".to_string(),
                error: "vm crashed".to_string(),
            },
            StackEvent::LeaseOpened {
                sandbox_id: "sb-1".to_string(),
                lease_id: "ls-1".to_string(),
            },
            StackEvent::LeaseHeartbeat {
                lease_id: "ls-1".to_string(),
            },
            StackEvent::LeaseExpired {
                lease_id: "ls-1".to_string(),
            },
            StackEvent::LeaseClosed {
                lease_id: "ls-2".to_string(),
            },
            StackEvent::LeaseFailed {
                lease_id: "ls-3".to_string(),
                error: "timeout".to_string(),
            },
            StackEvent::ExecutionQueued {
                container_id: "ctr-123".to_string(),
                execution_id: "exec-1".to_string(),
            },
            StackEvent::ExecutionRunning {
                execution_id: "exec-1".to_string(),
            },
            StackEvent::ExecutionExited {
                execution_id: "exec-1".to_string(),
                exit_code: 0,
            },
            StackEvent::ExecutionFailed {
                execution_id: "exec-2".to_string(),
                error: "command not found".to_string(),
            },
            StackEvent::ExecutionCanceled {
                execution_id: "exec-3".to_string(),
            },
            StackEvent::CheckpointCreating {
                sandbox_id: "sb-1".to_string(),
                checkpoint_id: "ckpt-1".to_string(),
                class: "fs_quick".to_string(),
            },
            StackEvent::CheckpointReady {
                checkpoint_id: "ckpt-1".to_string(),
            },
            StackEvent::CheckpointFailed {
                checkpoint_id: "ckpt-2".to_string(),
                error: "disk full".to_string(),
            },
            StackEvent::CheckpointRestored {
                checkpoint_id: "ckpt-1".to_string(),
                sandbox_id: "sb-1".to_string(),
            },
            StackEvent::CheckpointForked {
                parent_checkpoint_id: "ckpt-1".to_string(),
                new_checkpoint_id: "ckpt-3".to_string(),
                new_sandbox_id: "sb-2".to_string(),
            },
        ]
    }

    #[test]
    fn event_round_trip_all_variants() {
        let events = sample_events();
        for event in events {
            let json = serde_json::to_string(&event).unwrap();
            let deserialized: StackEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, event);
        }
    }

    #[test]
    fn event_tags_forbid_product_domain_primitives() {
        const FORBIDDEN: [&str; 5] = [
            "identity_provider",
            "memory_provider",
            "tool_gateway",
            "mission",
            "workflow",
        ];

        for event in sample_events() {
            let json = serde_json::to_value(&event).unwrap();
            let event_type = json
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap();
            let normalized = event_type.to_ascii_lowercase();
            for forbidden in FORBIDDEN {
                assert!(
                    !normalized.contains(forbidden),
                    "stack event type `{event_type}` must not contain forbidden primitive `{forbidden}`"
                );
            }
        }
    }

    #[test]
    fn event_tag_serialization() {
        let cases = vec![
            (
                StackEvent::StackApplyStarted {
                    stack_name: "t".to_string(),
                    services_count: 1,
                },
                "stack_apply_started",
            ),
            (
                StackEvent::ServiceStopping {
                    stack_name: "t".to_string(),
                    service_name: "w".to_string(),
                },
                "service_stopping",
            ),
            (
                StackEvent::ServiceStopped {
                    stack_name: "t".to_string(),
                    service_name: "w".to_string(),
                    exit_code: 0,
                },
                "service_stopped",
            ),
            (
                StackEvent::PortConflict {
                    stack_name: "t".to_string(),
                    service_name: "w".to_string(),
                    port: 80,
                },
                "port_conflict",
            ),
            (
                StackEvent::VolumeCreated {
                    stack_name: "t".to_string(),
                    volume_name: "v".to_string(),
                },
                "volume_created",
            ),
            (
                StackEvent::StackDestroyed {
                    stack_name: "t".to_string(),
                },
                "stack_destroyed",
            ),
            (
                StackEvent::MountTopologyRecreateRequired {
                    stack_name: "t".to_string(),
                    service_name: "w".to_string(),
                    previous_digest: Some("old".to_string()),
                    desired_digest: "new".to_string(),
                },
                "mount_topology_recreate_required",
            ),
            (
                StackEvent::SandboxCreating {
                    stack_name: "t".to_string(),
                    sandbox_id: "sb".to_string(),
                },
                "sandbox_creating",
            ),
            (
                StackEvent::SandboxReady {
                    stack_name: "t".to_string(),
                    sandbox_id: "sb".to_string(),
                },
                "sandbox_ready",
            ),
            (
                StackEvent::SandboxDraining {
                    stack_name: "t".to_string(),
                    sandbox_id: "sb".to_string(),
                },
                "sandbox_draining",
            ),
            (
                StackEvent::SandboxTerminated {
                    stack_name: "t".to_string(),
                    sandbox_id: "sb".to_string(),
                },
                "sandbox_terminated",
            ),
            (
                StackEvent::SandboxFailed {
                    stack_name: "t".to_string(),
                    sandbox_id: "sb".to_string(),
                    error: "e".to_string(),
                },
                "sandbox_failed",
            ),
            (
                StackEvent::LeaseOpened {
                    sandbox_id: "sb".to_string(),
                    lease_id: "ls".to_string(),
                },
                "lease_opened",
            ),
            (
                StackEvent::LeaseHeartbeat {
                    lease_id: "ls".to_string(),
                },
                "lease_heartbeat",
            ),
            (
                StackEvent::LeaseExpired {
                    lease_id: "ls".to_string(),
                },
                "lease_expired",
            ),
            (
                StackEvent::LeaseClosed {
                    lease_id: "ls".to_string(),
                },
                "lease_closed",
            ),
            (
                StackEvent::LeaseFailed {
                    lease_id: "ls".to_string(),
                    error: "e".to_string(),
                },
                "lease_failed",
            ),
            (
                StackEvent::ExecutionQueued {
                    container_id: "ctr".to_string(),
                    execution_id: "ex".to_string(),
                },
                "execution_queued",
            ),
            (
                StackEvent::ExecutionRunning {
                    execution_id: "ex".to_string(),
                },
                "execution_running",
            ),
            (
                StackEvent::ExecutionExited {
                    execution_id: "ex".to_string(),
                    exit_code: 0,
                },
                "execution_exited",
            ),
            (
                StackEvent::ExecutionFailed {
                    execution_id: "ex".to_string(),
                    error: "e".to_string(),
                },
                "execution_failed",
            ),
            (
                StackEvent::ExecutionCanceled {
                    execution_id: "ex".to_string(),
                },
                "execution_canceled",
            ),
            (
                StackEvent::CheckpointCreating {
                    sandbox_id: "sb".to_string(),
                    checkpoint_id: "ck".to_string(),
                    class: "fs_quick".to_string(),
                },
                "checkpoint_creating",
            ),
            (
                StackEvent::CheckpointReady {
                    checkpoint_id: "ck".to_string(),
                },
                "checkpoint_ready",
            ),
            (
                StackEvent::CheckpointFailed {
                    checkpoint_id: "ck".to_string(),
                    error: "e".to_string(),
                },
                "checkpoint_failed",
            ),
            (
                StackEvent::CheckpointRestored {
                    checkpoint_id: "ck".to_string(),
                    sandbox_id: "sb".to_string(),
                },
                "checkpoint_restored",
            ),
            (
                StackEvent::CheckpointForked {
                    parent_checkpoint_id: "ck".to_string(),
                    new_checkpoint_id: "ck2".to_string(),
                    new_sandbox_id: "sb2".to_string(),
                },
                "checkpoint_forked",
            ),
        ];

        for (event, expected_tag) in cases {
            let json = serde_json::to_string(&event).unwrap();
            let expected = format!("\"type\":\"{expected_tag}\"");
            assert!(json.contains(&expected), "tag mismatch for {json}");
        }
    }

    #[test]
    fn event_sink_adapter_forwards_event_and_metadata() {
        let captured = Arc::new(Mutex::new(None::<(StackEvent, Option<String>)>));
        let captured_clone = Arc::clone(&captured);
        let sink = FnStackEventSink::new(move |event, metadata| {
            *captured_clone.lock().unwrap() = Some((event.clone(), metadata.request_id.clone()));
            Ok(())
        });

        let event = StackEvent::StackDestroyed {
            stack_name: "myapp".to_string(),
        };
        let metadata = RequestMetadata::from_optional_refs(Some("req-55"), None);
        emit_event_to_sink(&sink, &event, &metadata).unwrap();

        let stored = captured.lock().unwrap().clone().unwrap();
        assert_eq!(stored.0, event);
        assert_eq!(stored.1.as_deref(), Some("req-55"));
    }

    #[test]
    fn event_sink_adapter_maps_transport_failures_to_machine_code() {
        let sink = FnStackEventSink::new(|_, _| {
            Err::<(), StackEventSinkError>(Box::new(io::Error::other("sink disconnected")))
        });
        let event = StackEvent::StackDestroyed {
            stack_name: "myapp".to_string(),
        };
        let metadata = RequestMetadata::default();

        let error = emit_event_to_sink(&sink, &event, &metadata).unwrap_err();
        assert_eq!(error.machine_code(), MachineErrorCode::BackendUnavailable);
        let message = error.to_string();
        assert!(message.contains("extension_failure:"));
        assert!(message.contains("extension=event_sink"));
        assert!(message.contains("operation=stack.emit_event"));
    }
}
