//! Structured event types for stack lifecycle observability.

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
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn event_round_trip() {
        let events = vec![
            StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 3,
            },
            StackEvent::StackApplyCompleted {
                stack_name: "myapp".to_string(),
                succeeded: 2,
                failed: 1,
            },
            StackEvent::ServiceReady {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
                runtime_id: "ctr-123".to_string(),
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
        let event = StackEvent::StackApplyStarted {
            stack_name: "test".to_string(),
            services_count: 1,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"stack_apply_started\""));
    }
}
