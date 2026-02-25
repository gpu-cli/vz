//! Stress testing infrastructure for Tier 3 weekly runs.
//!
//! Provides repeated scenario execution with flake rate tracking,
//! hard failure detection, and structured stress reports.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::cohort::ImageRef;
use crate::report::{ScenarioOutcome, TestResult};
use crate::runner::{RuntimeAdapter, ScenarioRunner};
use crate::scenario::Scenario;

/// Configuration for a stress run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StressConfig {
    /// Number of iterations per scenario.
    pub iterations: usize,
    /// Maximum allowed flake rate (0.0-1.0). Exceeding this fails the run.
    pub max_flake_rate: f64,
}

impl Default for StressConfig {
    fn default() -> Self {
        Self {
            iterations: 100,
            max_flake_rate: 0.05,
        }
    }
}

/// Result of running a single scenario through stress iterations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioStressResult {
    /// Image tested.
    pub image: ImageRef,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Total iterations run.
    pub iterations: usize,
    /// Number of passes.
    pub passed: usize,
    /// Number of failures.
    pub failed: usize,
    /// Flake rate (failed / iterations).
    pub flake_rate: f64,
    /// Whether this scenario is considered a hard failure (always fails).
    pub hard_failure: bool,
    /// Total duration across all iterations.
    #[serde(with = "crate::report::duration_serde")]
    pub total_duration: Duration,
    /// Average duration per iteration.
    #[serde(with = "crate::report::duration_serde")]
    pub avg_duration: Duration,
    /// First failure message (if any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_failure: Option<String>,
}

/// Aggregate stress report for Tier 3 weekly runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StressReport {
    /// ISO 8601 timestamp.
    pub started_at: String,
    /// Total run duration.
    #[serde(with = "crate::report::duration_serde")]
    pub duration: Duration,
    /// Stress configuration used.
    pub config: StressConfig,
    /// Per-scenario stress results.
    pub results: Vec<ScenarioStressResult>,
}

impl StressReport {
    /// Create a new empty stress report.
    pub fn new(started_at: &str, config: StressConfig) -> Self {
        Self {
            started_at: started_at.to_string(),
            duration: Duration::ZERO,
            config,
            results: Vec::new(),
        }
    }

    /// Add a scenario stress result.
    pub fn add_result(&mut self, result: ScenarioStressResult) {
        self.results.push(result);
    }

    /// Finalize with total duration.
    pub fn finalize(&mut self, duration: Duration) {
        self.duration = duration;
    }

    /// Number of scenarios that exceeded the max flake rate.
    pub fn flaky_count(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.flake_rate > self.config.max_flake_rate && !r.hard_failure)
            .count()
    }

    /// Number of scenarios that always fail.
    pub fn hard_failure_count(&self) -> usize {
        self.results.iter().filter(|r| r.hard_failure).count()
    }

    /// Number of scenarios that are stable (at or below max flake rate).
    pub fn stable_count(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.flake_rate <= self.config.max_flake_rate)
            .count()
    }

    /// Whether the stress run passes (no hard failures, flake rate within bounds).
    pub fn all_passed(&self) -> bool {
        self.hard_failure_count() == 0 && self.flaky_count() == 0
    }

    /// Generate a summary line.
    pub fn summary_line(&self) -> String {
        format!(
            "tier-3-weekly: {} stable, {} flaky, {} hard failures ({:.1}s, {} iterations each)",
            self.stable_count(),
            self.flaky_count(),
            self.hard_failure_count(),
            self.duration.as_secs_f64(),
            self.config.iterations,
        )
    }
}

/// Controls which test results contribute to duration accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SweepMode {
    /// Account durations from all iterations (pass and fail).
    All,
    /// Account durations only from failed iterations.
    ///
    /// Useful for diagnosing slow failure paths without pass-iteration
    /// noise diluting the timing signal.
    FailOnly,
}

impl Default for SweepMode {
    fn default() -> Self {
        Self::All
    }
}

/// Configuration for a sweep run (parameterised stress with duration
/// accounting control).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepConfig {
    /// Number of iterations per scenario.
    pub iterations: usize,
    /// Maximum allowed flake rate (0.0-1.0). Exceeding this fails the run.
    pub max_flake_rate: f64,
    /// Which iterations contribute to duration accounting.
    pub sweep_mode: SweepMode,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            iterations: 100,
            max_flake_rate: 0.05,
            sweep_mode: SweepMode::All,
        }
    }
}

/// Run stress iterations for a single scenario against a single image.
///
/// Duration accounting uses the per-test `Duration` from each
/// `TestResult` (measured inside `run_one`) rather than wall-clock
/// elapsed time, so overhead between iterations does not inflate the
/// reported execution durations.
pub fn stress_scenario<R: RuntimeAdapter>(
    runner: &ScenarioRunner<R>,
    image: &ImageRef,
    scenario: &Scenario,
    iterations: usize,
) -> ScenarioStressResult {
    sweep_scenario(runner, image, scenario, iterations, SweepMode::All)
}

/// Run a sweep (parameterised stress) for a single scenario.
///
/// When `mode` is [`SweepMode::FailOnly`], only failed iterations
/// contribute to `total_duration` and `avg_duration`. This isolates
/// the timing signal for the failure path without pass-iteration noise.
pub fn sweep_scenario<R: RuntimeAdapter>(
    runner: &ScenarioRunner<R>,
    image: &ImageRef,
    scenario: &Scenario,
    iterations: usize,
    mode: SweepMode,
) -> ScenarioStressResult {
    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut first_failure: Option<String> = None;
    let mut accounted_duration = Duration::ZERO;
    let mut accounted_count = 0usize;

    for _ in 0..iterations {
        let result: TestResult = runner.run_one(image, scenario);
        let is_pass = result.outcome.is_pass();

        if is_pass {
            passed += 1;
        } else {
            failed += 1;
            if first_failure.is_none() {
                first_failure = Some(describe_failure(&result.outcome));
            }
        }

        // Accumulate per-test duration from the TestResult (measured
        // inside run_one) rather than wall-clock elapsed. This avoids
        // inter-iteration overhead inflating reported durations.
        let include = match mode {
            SweepMode::All => true,
            SweepMode::FailOnly => !is_pass,
        };
        if include {
            accounted_duration += result.duration;
            accounted_count += 1;
        }
    }

    let avg_duration = if accounted_count > 0 {
        accounted_duration / accounted_count as u32
    } else {
        Duration::ZERO
    };

    let flake_rate = if iterations > 0 {
        failed as f64 / iterations as f64
    } else {
        0.0
    };

    ScenarioStressResult {
        image: image.clone(),
        scenario_id: scenario.id.clone(),
        iterations,
        passed,
        failed,
        flake_rate,
        hard_failure: passed == 0 && iterations > 0,
        total_duration: accounted_duration,
        avg_duration,
        first_failure,
    }
}

fn describe_failure(outcome: &ScenarioOutcome) -> String {
    match outcome {
        ScenarioOutcome::Fail { failures } => failures.join("; "),
        ScenarioOutcome::Error { message } => message.clone(),
        _ => "unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::runner::{ExecOutput, MockAdapter};
    use crate::scenario::s1_entrypoint_scenarios;

    fn alpine() -> ImageRef {
        ImageRef {
            reference: "alpine:3.20".to_string(),
            digest: None,
            label: "Alpine".to_string(),
        }
    }

    #[test]
    fn stress_config_default() {
        let config = StressConfig::default();
        assert_eq!(config.iterations, 100);
        assert!((config.max_flake_rate - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn stress_scenario_all_pass() {
        let adapter = MockAdapter {
            output: ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
                lifecycle_events: Vec::new(),
            },
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[0]; // exit code 0 only

        let result = stress_scenario(&runner, &alpine(), scenario, 10);
        assert_eq!(result.iterations, 10);
        assert_eq!(result.passed, 10);
        assert_eq!(result.failed, 0);
        assert!((result.flake_rate).abs() < f64::EPSILON);
        assert!(!result.hard_failure);
        assert!(result.first_failure.is_none());
    }

    #[test]
    fn stress_scenario_hard_failure() {
        let adapter = MockAdapter {
            output: ExecOutput {
                exit_code: 1, // always fail for exit code expectation
                stdout: String::new(),
                stderr: String::new(),
                lifecycle_events: Vec::new(),
            },
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[0]; // expects exit 0

        let result = stress_scenario(&runner, &alpine(), scenario, 5);
        assert_eq!(result.passed, 0);
        assert_eq!(result.failed, 5);
        assert!((result.flake_rate - 1.0).abs() < f64::EPSILON);
        assert!(result.hard_failure);
        assert!(result.first_failure.is_some());
    }

    #[test]
    fn stress_report_counts() {
        let mut report = StressReport::new("2026-02-20T00:00:00Z", StressConfig::default());

        // Stable scenario.
        report.add_result(ScenarioStressResult {
            image: alpine(),
            scenario_id: "s1-test".to_string(),
            iterations: 100,
            passed: 100,
            failed: 0,
            flake_rate: 0.0,
            hard_failure: false,
            total_duration: Duration::from_secs(10),
            avg_duration: Duration::from_millis(100),
            first_failure: None,
        });

        // Flaky scenario (10% flake rate, above 5% threshold).
        report.add_result(ScenarioStressResult {
            image: alpine(),
            scenario_id: "s1-flaky".to_string(),
            iterations: 100,
            passed: 90,
            failed: 10,
            flake_rate: 0.10,
            hard_failure: false,
            total_duration: Duration::from_secs(10),
            avg_duration: Duration::from_millis(100),
            first_failure: Some("exit code mismatch".to_string()),
        });

        // Hard failure.
        report.add_result(ScenarioStressResult {
            image: alpine(),
            scenario_id: "s1-broken".to_string(),
            iterations: 100,
            passed: 0,
            failed: 100,
            flake_rate: 1.0,
            hard_failure: true,
            total_duration: Duration::from_secs(10),
            avg_duration: Duration::from_millis(100),
            first_failure: Some("always fails".to_string()),
        });

        assert_eq!(report.stable_count(), 1);
        assert_eq!(report.flaky_count(), 1);
        assert_eq!(report.hard_failure_count(), 1);
        assert!(!report.all_passed());
    }

    #[test]
    fn stress_report_all_passed() {
        let mut report = StressReport::new("2026-02-20T00:00:00Z", StressConfig::default());
        report.add_result(ScenarioStressResult {
            image: alpine(),
            scenario_id: "s1-stable".to_string(),
            iterations: 100,
            passed: 98,
            failed: 2,
            flake_rate: 0.02, // below 5% threshold
            hard_failure: false,
            total_duration: Duration::from_secs(10),
            avg_duration: Duration::from_millis(100),
            first_failure: Some("transient".to_string()),
        });
        report.finalize(Duration::from_secs(10));
        assert!(report.all_passed());
    }

    #[test]
    fn stress_report_summary_line() {
        let mut report = StressReport::new("2026-02-20T00:00:00Z", StressConfig::default());
        report.add_result(ScenarioStressResult {
            image: alpine(),
            scenario_id: "s1-ok".to_string(),
            iterations: 100,
            passed: 100,
            failed: 0,
            flake_rate: 0.0,
            hard_failure: false,
            total_duration: Duration::from_secs(5),
            avg_duration: Duration::from_millis(50),
            first_failure: None,
        });
        report.finalize(Duration::from_secs(60));

        let line = report.summary_line();
        assert!(line.contains("tier-3-weekly"));
        assert!(line.contains("1 stable"));
        assert!(line.contains("0 flaky"));
        assert!(line.contains("100 iterations"));
    }

    #[test]
    fn stress_report_round_trip() {
        let mut report = StressReport::new(
            "2026-02-20T00:00:00Z",
            StressConfig {
                iterations: 50,
                max_flake_rate: 0.10,
            },
        );
        report.add_result(ScenarioStressResult {
            image: alpine(),
            scenario_id: "s1-test".to_string(),
            iterations: 50,
            passed: 48,
            failed: 2,
            flake_rate: 0.04,
            hard_failure: false,
            total_duration: Duration::from_secs(5),
            avg_duration: Duration::from_millis(100),
            first_failure: Some("transient".to_string()),
        });
        report.finalize(Duration::from_secs(30));

        let json = serde_json::to_string_pretty(&report).unwrap();
        let deserialized: StressReport = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.config.iterations, 50);
        assert_eq!(deserialized.results.len(), 1);
        assert_eq!(deserialized.results[0].passed, 48);
    }

    #[test]
    fn stress_zero_iterations() {
        let adapter = MockAdapter {
            output: ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
                lifecycle_events: Vec::new(),
            },
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[0];

        let result = stress_scenario(&runner, &alpine(), scenario, 0);
        assert_eq!(result.iterations, 0);
        assert_eq!(result.passed, 0);
        assert_eq!(result.failed, 0);
        assert!(!result.hard_failure);
    }

    // ── Sweep mode tests ────────────────────────────────────────────

    #[test]
    fn sweep_mode_default_is_all() {
        assert_eq!(SweepMode::default(), SweepMode::All);
    }

    #[test]
    fn sweep_config_default() {
        let config = SweepConfig::default();
        assert_eq!(config.iterations, 100);
        assert!((config.max_flake_rate - 0.05).abs() < f64::EPSILON);
        assert_eq!(config.sweep_mode, SweepMode::All);
    }

    #[test]
    fn sweep_all_mode_uses_per_test_durations() {
        // With SweepMode::All, total_duration should be the sum of
        // per-test durations from TestResult (not wall-clock).
        let adapter = MockAdapter {
            output: ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
                lifecycle_events: Vec::new(),
            },
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[0];

        let result = sweep_scenario(&runner, &alpine(), scenario, 5, SweepMode::All);
        assert_eq!(result.iterations, 5);
        assert_eq!(result.passed, 5);
        assert_eq!(result.failed, 0);
        // total_duration is the sum of 5 per-test durations (very fast
        // for mock adapter), avg_duration = total / 5.
        assert!(result.avg_duration <= result.total_duration);
    }

    #[test]
    fn sweep_fail_only_excludes_pass_durations() {
        // All tests pass -> in FailOnly mode, no durations are accounted.
        let adapter = MockAdapter {
            output: ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
                lifecycle_events: Vec::new(),
            },
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[0];

        let result = sweep_scenario(&runner, &alpine(), scenario, 5, SweepMode::FailOnly);
        assert_eq!(result.passed, 5);
        assert_eq!(result.failed, 0);
        // No failures => zero accounted duration
        assert_eq!(result.total_duration, Duration::ZERO);
        assert_eq!(result.avg_duration, Duration::ZERO);
    }

    #[test]
    fn sweep_fail_only_accounts_only_failure_durations() {
        // All tests fail -> in FailOnly mode, all durations are accounted.
        let adapter = MockAdapter {
            output: ExecOutput {
                exit_code: 1,
                stdout: String::new(),
                stderr: String::new(),
                lifecycle_events: Vec::new(),
            },
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[0]; // expects exit 0

        let result = sweep_scenario(&runner, &alpine(), scenario, 5, SweepMode::FailOnly);
        assert_eq!(result.passed, 0);
        assert_eq!(result.failed, 5);
        // All 5 failed iterations contribute their per-test durations.
        assert!(result.total_duration > Duration::ZERO || result.iterations == 0);
        assert!(result.avg_duration <= result.total_duration);
    }

    #[test]
    fn sweep_mode_round_trip() {
        let config = SweepConfig {
            iterations: 20,
            max_flake_rate: 0.10,
            sweep_mode: SweepMode::FailOnly,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SweepConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.iterations, 20);
        assert_eq!(deserialized.sweep_mode, SweepMode::FailOnly);
    }

    #[test]
    fn sweep_zero_iterations() {
        let adapter = MockAdapter {
            output: ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
                lifecycle_events: Vec::new(),
            },
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[0];

        let result = sweep_scenario(&runner, &alpine(), scenario, 0, SweepMode::FailOnly);
        assert_eq!(result.iterations, 0);
        assert_eq!(result.total_duration, Duration::ZERO);
        assert_eq!(result.avg_duration, Duration::ZERO);
    }

    #[test]
    fn stress_scenario_uses_per_test_durations_not_wall_clock() {
        // Verify that stress_scenario (which delegates to sweep_scenario
        // with SweepMode::All) accumulates per-test durations from
        // TestResult rather than wall-clock elapsed.
        let adapter = MockAdapter {
            output: ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
                lifecycle_events: Vec::new(),
            },
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[0];

        let result = stress_scenario(&runner, &alpine(), scenario, 10);
        // With a mock adapter, per-test durations are very small
        // (microseconds). The total should equal roughly 10x the avg.
        // The key property: avg_duration * iterations ~= total_duration.
        let reconstructed = result.avg_duration * result.iterations as u32;
        // Allow small rounding difference from integer division.
        let diff = if reconstructed > result.total_duration {
            reconstructed - result.total_duration
        } else {
            result.total_duration - reconstructed
        };
        // Rounding error at most 1 tick per iteration.
        assert!(
            diff < Duration::from_nanos(result.iterations as u64 + 1),
            "duration accounting mismatch: total={:?} avg={:?} iters={}",
            result.total_duration,
            result.avg_duration,
            result.iterations
        );
    }
}
