//! Structured pass/fail reporting for validation runs.
//!
//! Produces JSON-serializable reports with per-image, per-scenario
//! outcomes, timing metrics, and artifact references.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::cohort::{ImageRef, Tier};
use crate::scenario::ScenarioKind;

/// Outcome of evaluating a single expectation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ScenarioOutcome {
    /// All expectations met.
    Pass,
    /// One or more expectations failed.
    Fail {
        /// Description of each failed expectation.
        failures: Vec<String>,
    },
    /// Scenario could not execute.
    Error {
        /// Error description.
        message: String,
    },
    /// Scenario was skipped (e.g., not applicable to this image).
    Skipped {
        /// Reason for skipping.
        reason: String,
    },
}

impl ScenarioOutcome {
    /// Whether this outcome counts as a pass.
    pub fn is_pass(&self) -> bool {
        matches!(self, Self::Pass)
    }

    /// Whether this outcome counts as a failure (not pass, not skipped).
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Fail { .. } | Self::Error { .. })
    }
}

/// Result of running a single scenario against a single image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    /// Image that was tested.
    pub image: ImageRef,
    /// Scenario that was executed.
    pub scenario_id: String,
    /// Scenario category.
    pub scenario_kind: ScenarioKind,
    /// Pass/fail outcome.
    pub outcome: ScenarioOutcome,
    /// Process exit code (if available).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Duration from scenario start to completion.
    #[serde(with = "duration_serde")]
    pub duration: Duration,
    /// Captured stdout (truncated if large).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    /// Captured stderr (truncated if large).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
}

/// Aggregate report for a validation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestReport {
    /// CI tier this run belongs to.
    pub tier: Tier,
    /// Timestamp when the run started (ISO 8601).
    pub started_at: String,
    /// Total run duration.
    #[serde(with = "duration_serde")]
    pub duration: Duration,
    /// Individual test results.
    pub results: Vec<TestResult>,
}

impl TestReport {
    /// Create a new empty report.
    pub fn new(tier: Tier, started_at: &str) -> Self {
        Self {
            tier,
            started_at: started_at.to_string(),
            duration: Duration::ZERO,
            results: Vec::new(),
        }
    }

    /// Add a test result.
    pub fn add_result(&mut self, result: TestResult) {
        self.results.push(result);
    }

    /// Finalize the report with total duration.
    pub fn finalize(&mut self, duration: Duration) {
        self.duration = duration;
    }

    /// Count of passed tests.
    pub fn passed(&self) -> usize {
        self.results.iter().filter(|r| r.outcome.is_pass()).count()
    }

    /// Count of failed tests.
    pub fn failed(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.outcome.is_failure())
            .count()
    }

    /// Count of skipped tests.
    pub fn skipped(&self) -> usize {
        self.results
            .iter()
            .filter(|r| matches!(r.outcome, ScenarioOutcome::Skipped { .. }))
            .count()
    }

    /// Total number of tests.
    pub fn total(&self) -> usize {
        self.results.len()
    }

    /// Whether all non-skipped tests passed.
    pub fn all_passed(&self) -> bool {
        self.failed() == 0
    }

    /// Generate a summary line for CLI output.
    pub fn summary_line(&self) -> String {
        format!(
            "{}: {} passed, {} failed, {} skipped ({}ms)",
            self.tier.label(),
            self.passed(),
            self.failed(),
            self.skipped(),
            self.duration.as_millis(),
        )
    }
}

mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    #[derive(Serialize, Deserialize)]
    struct DurationMs {
        ms: u64,
    }

    pub fn serialize<S: Serializer>(duration: &Duration, s: S) -> Result<S::Ok, S::Error> {
        DurationMs {
            ms: duration.as_millis() as u64,
        }
        .serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let ms = DurationMs::deserialize(d)?;
        Ok(Duration::from_millis(ms.ms))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn sample_result(passed: bool) -> TestResult {
        TestResult {
            image: ImageRef {
                reference: "alpine:3.20".to_string(),
                digest: None,
                label: "Alpine".to_string(),
            },
            scenario_id: "s1-test".to_string(),
            scenario_kind: ScenarioKind::EntrypointCmd,
            outcome: if passed {
                ScenarioOutcome::Pass
            } else {
                ScenarioOutcome::Fail {
                    failures: vec!["exit code mismatch".to_string()],
                }
            },
            exit_code: Some(if passed { 0 } else { 1 }),
            duration: Duration::from_millis(150),
            stdout: Some("output".to_string()),
            stderr: None,
        }
    }

    #[test]
    fn outcome_pass_classification() {
        assert!(ScenarioOutcome::Pass.is_pass());
        assert!(!ScenarioOutcome::Pass.is_failure());
    }

    #[test]
    fn outcome_fail_classification() {
        let fail = ScenarioOutcome::Fail {
            failures: vec!["x".into()],
        };
        assert!(!fail.is_pass());
        assert!(fail.is_failure());
    }

    #[test]
    fn outcome_error_classification() {
        let err = ScenarioOutcome::Error {
            message: "boom".into(),
        };
        assert!(!err.is_pass());
        assert!(err.is_failure());
    }

    #[test]
    fn outcome_skipped_classification() {
        let skip = ScenarioOutcome::Skipped {
            reason: "n/a".into(),
        };
        assert!(!skip.is_pass());
        assert!(!skip.is_failure());
    }

    #[test]
    fn test_result_round_trip() {
        let result = sample_result(true);
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: TestResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.scenario_id, "s1-test");
        assert!(deserialized.outcome.is_pass());
    }

    #[test]
    fn report_counts() {
        let mut report = TestReport::new(Tier::Tier1, "2026-02-20T00:00:00Z");
        report.add_result(sample_result(true));
        report.add_result(sample_result(true));
        report.add_result(sample_result(false));
        report.add_result(TestResult {
            outcome: ScenarioOutcome::Skipped {
                reason: "n/a".into(),
            },
            ..sample_result(true)
        });

        assert_eq!(report.total(), 4);
        assert_eq!(report.passed(), 2);
        assert_eq!(report.failed(), 1);
        assert_eq!(report.skipped(), 1);
        assert!(!report.all_passed());
    }

    #[test]
    fn report_all_passed() {
        let mut report = TestReport::new(Tier::Tier1, "2026-02-20T00:00:00Z");
        report.add_result(sample_result(true));
        report.add_result(sample_result(true));
        assert!(report.all_passed());
    }

    #[test]
    fn report_round_trip() {
        let mut report = TestReport::new(Tier::Tier1, "2026-02-20T00:00:00Z");
        report.add_result(sample_result(true));
        report.finalize(Duration::from_secs(5));

        let json = serde_json::to_string_pretty(&report).unwrap();
        let deserialized: TestReport = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.tier, Tier::Tier1);
        assert_eq!(deserialized.results.len(), 1);
        assert_eq!(deserialized.duration, Duration::from_secs(5));
    }

    #[test]
    fn summary_line_format() {
        let mut report = TestReport::new(Tier::Tier1, "2026-02-20T00:00:00Z");
        report.add_result(sample_result(true));
        report.add_result(sample_result(false));
        report.finalize(Duration::from_millis(1234));

        let line = report.summary_line();
        assert!(line.contains("tier-1-smoke"));
        assert!(line.contains("1 passed"));
        assert!(line.contains("1 failed"));
        assert!(line.contains("1234ms"));
    }
}
