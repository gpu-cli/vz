use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::ContractInvariantError;

/// Event stream scope.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventScope {
    /// Sandbox-scoped event.
    Sandbox,
    /// Lease-scoped event.
    Lease,
    /// Build-scoped event.
    Build,
    /// Container-scoped event.
    Container,
    /// Execution-scoped event.
    Execution,
    /// Checkpoint-scoped event.
    Checkpoint,
    /// System-scoped event.
    System,
}

/// Append-only runtime operation event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    /// Monotonic event identifier in stream.
    pub event_id: u64,
    /// Event timestamp in unix epoch seconds.
    pub ts: u64,
    /// Event scope class.
    pub scope: EventScope,
    /// Scoped entity identifier.
    pub scope_id: String,
    /// Event type identifier.
    pub event_type: String,
    /// Structured payload fields.
    pub payload: BTreeMap<String, String>,
    /// Optional trace identifier.
    pub trace_id: Option<String>,
}

/// Receipt result classification.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptResultClassification {
    /// Request completed successfully.
    Success,
    /// Request failed validation.
    ValidationError,
    /// Request failed due to policy.
    PolicyDenied,
    /// Request failed due to state conflict.
    StateConflict,
    /// Request failed due to timeout.
    Timeout,
    /// Request failed with internal runtime error.
    InternalError,
}

/// Inclusive event range linked to a receipt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventRange {
    /// First event ID included.
    pub start_event_id: u64,
    /// Last event ID included.
    pub end_event_id: u64,
}

/// Immutable operation summary for audit/replay metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    /// Receipt identifier.
    pub receipt_id: String,
    /// Scope class for the operation.
    pub scope: EventScope,
    /// Scoped entity identifier.
    pub scope_id: String,
    /// Hash of request payload/input.
    pub request_hash: String,
    /// Optional policy hash evaluated during the request.
    pub policy_hash: Option<String>,
    /// Result classification.
    pub result_classification: ReceiptResultClassification,
    /// Artifact references emitted by the operation.
    pub artifacts: Vec<String>,
    /// Structured resource usage summary.
    pub resource_summary: BTreeMap<String, String>,
    /// Event range associated with this operation.
    pub event_range: EventRange,
}

impl Receipt {
    /// Validate that receipt event range ordering is correct.
    pub fn ensure_event_range_ordered(&self) -> Result<(), ContractInvariantError> {
        if self.event_range.start_event_id > self.event_range.end_event_id {
            return Err(ContractInvariantError::ReceiptEventRangeInvalid {
                receipt_id: self.receipt_id.clone(),
                start_event_id: self.event_range.start_event_id,
                end_event_id: self.event_range.end_event_id,
            });
        }

        Ok(())
    }
}
