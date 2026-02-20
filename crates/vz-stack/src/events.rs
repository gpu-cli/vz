//! Structured event types for stack lifecycle observability.
//!
//! Events are emitted by the reconciler and persisted in the
//! [`StateStore`](crate::StateStore). API consumers can stream
//! events incrementally using [`StateStore::load_events_since`].

use serde::{Deserialize, Serialize};

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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn event_round_trip_all_variants() {
        let events: Vec<StackEvent> = vec![
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
        ];

        for event in events {
            let json = serde_json::to_string(&event).unwrap();
            let deserialized: StackEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, event);
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
        ];

        for (event, expected_tag) in cases {
            let json = serde_json::to_string(&event).unwrap();
            let expected = format!("\"type\":\"{expected_tag}\"");
            assert!(json.contains(&expected), "tag mismatch for {json}");
        }
    }
}
