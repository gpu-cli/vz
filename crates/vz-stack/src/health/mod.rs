//! Health and dependency gating for service readiness.
//!
//! Evaluates whether services are ready based on their lifecycle
//! phase, health check configuration, and health check results.
//! Provides dependency readiness checking so the reconciler can
//! defer service creation until all dependencies are satisfied.
//!
//! The [`HealthPoller`] runs one health check cycle across all
//! running services, updating observed state and emitting events.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tracing::{debug, info};

use crate::error::StackError;
use crate::events::StackEvent;
use crate::executor::ContainerRuntime;
use crate::spec::{DependencyCondition, HealthCheckSpec, ServiceSpec, StackSpec};
use crate::state_store::{ServiceObservedState, ServicePhase, StateStore};

/// Result of checking whether a service's dependencies are satisfied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DependencyCheck {
    /// All dependencies are ready.
    Ready,
    /// Some dependencies are not yet ready.
    Blocked {
        /// Names of dependencies that are not ready.
        waiting_on: Vec<String>,
    },
}

/// Health status for a service's health check executions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthStatus {
    /// Service name.
    pub service_name: String,
    /// Number of consecutive passed health checks.
    pub consecutive_passes: u32,
    /// Number of consecutive failed health checks.
    pub consecutive_failures: u32,
    /// When the last health check was executed.
    pub last_check: Option<Instant>,
}

impl HealthStatus {
    /// Create a new health status with zero counts.
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
            consecutive_passes: 0,
            consecutive_failures: 0,
            last_check: None,
        }
    }

    /// Record a passed health check.
    pub fn record_pass(&mut self) {
        self.consecutive_passes += 1;
        self.consecutive_failures = 0;
    }

    /// Record a failed health check.
    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.consecutive_passes = 0;
    }
}

/// Evaluate whether a service should be considered ready.
///
/// A service is ready when:
/// - It is in the `Running` phase, AND
/// - Either no health check is defined, OR the health check
///   has at least one consecutive pass.
mod deps;
mod poller;

pub use self::deps::{check_dependencies, is_service_ready};

#[cfg(test)]
mod tests;

/// Polls health checks for running services in a stack.
///
/// Call [`poll_all`](HealthPoller::poll_all) periodically (at the
/// smallest configured interval) to run one cycle of health checks.
/// The poller respects `start_period_secs` grace periods and marks
/// services as `Failed` when consecutive failures exceed the
/// `retries` threshold.
pub struct HealthPoller {
    /// Health status per service name.
    statuses: HashMap<String, HealthStatus>,
    /// When each service was first observed as Running (for start_period grace).
    start_times: HashMap<String, Instant>,
}

/// Result of a single health poll cycle.
#[derive(Debug, Clone, Default)]
pub struct HealthPollResult {
    /// Services that became ready this cycle.
    pub newly_ready: Vec<String>,
    /// Services that exceeded retries and were marked failed.
    pub newly_failed: Vec<String>,
    /// Number of health checks executed.
    pub checks_run: usize,
}

impl Default for HealthPoller {
    fn default() -> Self {
        Self::new()
    }
}
