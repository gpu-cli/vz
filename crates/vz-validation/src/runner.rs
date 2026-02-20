//! Scenario runner with a pluggable runtime adapter.
//!
//! The [`ScenarioRunner`] executes scenarios against a
//! [`RuntimeAdapter`], producing structured [`TestResult`]s.
//! Callers provide a concrete adapter (real runtime for CI,
//! mock for unit testing).

use std::time::Instant;

use crate::cohort::ImageRef;
use crate::report::{ScenarioOutcome, TestResult};
use crate::scenario::{Expectation, Scenario};

/// Output captured from a scenario execution.
#[derive(Debug, Clone)]
pub struct ExecOutput {
    /// Process exit code.
    pub exit_code: i32,
    /// Captured stdout.
    pub stdout: String,
    /// Captured stderr.
    pub stderr: String,
    /// Lifecycle events recorded during execution.
    pub lifecycle_events: Vec<String>,
}

/// Trait for runtime backends that can execute scenarios.
///
/// Implement this for real runtimes (vz-oci Runtime) or mocks.
pub trait RuntimeAdapter {
    /// Execute a scenario and return captured output.
    ///
    /// The adapter is responsible for:
    /// 1. Pulling/preparing the image
    /// 2. Creating and running the container
    /// 3. Capturing stdout/stderr
    /// 4. Returning the exit code and lifecycle events
    fn execute(
        &self,
        image: &ImageRef,
        scenario: &Scenario,
    ) -> Result<ExecOutput, String>;
}

/// Runs scenarios against a runtime adapter and collects results.
pub struct ScenarioRunner<R: RuntimeAdapter> {
    adapter: R,
}

impl<R: RuntimeAdapter> ScenarioRunner<R> {
    /// Create a new runner with the given adapter.
    pub fn new(adapter: R) -> Self {
        Self { adapter }
    }

    /// Run a single scenario against a single image.
    pub fn run_one(&self, image: &ImageRef, scenario: &Scenario) -> TestResult {
        let start = Instant::now();

        let (outcome, exit_code, stdout, stderr) =
            match self.adapter.execute(image, scenario) {
                Ok(output) => {
                    let failures = evaluate_expectations(&scenario.expectations, &output);
                    let outcome = if failures.is_empty() {
                        ScenarioOutcome::Pass
                    } else {
                        ScenarioOutcome::Fail { failures }
                    };
                    (
                        outcome,
                        Some(output.exit_code),
                        Some(output.stdout),
                        Some(output.stderr),
                    )
                }
                Err(message) => (
                    ScenarioOutcome::Error { message },
                    None,
                    None,
                    None,
                ),
            };

        let duration = start.elapsed();

        TestResult {
            image: image.clone(),
            scenario_id: scenario.id.clone(),
            scenario_kind: scenario.kind,
            outcome,
            exit_code,
            duration,
            stdout,
            stderr,
        }
    }

    /// Run all given scenarios against a single image.
    pub fn run_image(
        &self,
        image: &ImageRef,
        scenarios: &[Scenario],
    ) -> Vec<TestResult> {
        scenarios
            .iter()
            .map(|s| self.run_one(image, s))
            .collect()
    }

    /// Run all scenarios against all images in a cohort.
    pub fn run_cohort(
        &self,
        images: &[ImageRef],
        scenarios: &[Scenario],
    ) -> Vec<TestResult> {
        images
            .iter()
            .flat_map(|img| self.run_image(img, scenarios))
            .collect()
    }
}

/// Evaluate all expectations against captured output.
fn evaluate_expectations(
    expectations: &[Expectation],
    output: &ExecOutput,
) -> Vec<String> {
    let mut failures = Vec::new();

    for exp in expectations {
        match exp {
            Expectation::ExitCode { code } => {
                if output.exit_code != *code {
                    failures.push(format!(
                        "expected exit code {code}, got {}",
                        output.exit_code
                    ));
                }
            }
            Expectation::StdoutContains { substring } => {
                if !output.stdout.contains(substring.as_str()) {
                    failures.push(format!(
                        "stdout does not contain `{substring}`"
                    ));
                }
            }
            Expectation::StderrContains { substring } => {
                if !output.stderr.contains(substring.as_str()) {
                    failures.push(format!(
                        "stderr does not contain `{substring}`"
                    ));
                }
            }
            Expectation::StdoutMatches { pattern } => {
                // Simple substring match as regex support is optional.
                // Full regex can be added when the `regex` crate is available.
                if !output.stdout.contains(pattern.as_str()) {
                    failures.push(format!(
                        "stdout does not match pattern `{pattern}`"
                    ));
                }
            }
            Expectation::LifecycleSequence { events } => {
                if output.lifecycle_events != *events {
                    failures.push(format!(
                        "lifecycle sequence mismatch: expected {events:?}, got {:?}",
                        output.lifecycle_events
                    ));
                }
            }
        }
    }

    failures
}

/// A mock runtime adapter for unit testing.
pub struct MockAdapter {
    /// Fixed output to return for any execution.
    pub output: ExecOutput,
}

impl RuntimeAdapter for MockAdapter {
    fn execute(
        &self,
        _image: &ImageRef,
        _scenario: &Scenario,
    ) -> Result<ExecOutput, String> {
        Ok(self.output.clone())
    }
}

/// A mock adapter that always returns an error.
pub struct FailingAdapter {
    /// Error message to return.
    pub message: String,
}

impl RuntimeAdapter for FailingAdapter {
    fn execute(
        &self,
        _image: &ImageRef,
        _scenario: &Scenario,
    ) -> Result<ExecOutput, String> {
        Err(self.message.clone())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use std::time::Duration;

    use crate::cohort::ImageRef;
    use crate::scenario::s1_entrypoint_scenarios;

    fn alpine() -> ImageRef {
        ImageRef {
            reference: "alpine:3.20".to_string(),
            digest: None,
            label: "Alpine".to_string(),
        }
    }

    fn passing_output() -> ExecOutput {
        ExecOutput {
            exit_code: 0,
            stdout: "hello-from-override\n".to_string(),
            stderr: String::new(),
            lifecycle_events: vec![
                "create".to_string(),
                "start".to_string(),
                "exec".to_string(),
                "delete".to_string(),
            ],
        }
    }

    #[test]
    fn runner_pass_scenario() {
        let adapter = MockAdapter {
            output: passing_output(),
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[1]; // s1-cmd-override

        let result = runner.run_one(&alpine(), scenario);
        assert!(
            result.outcome.is_pass(),
            "should pass: {:?}",
            result.outcome
        );
        assert_eq!(result.exit_code, Some(0));
    }

    #[test]
    fn runner_fail_exit_code() {
        let adapter = MockAdapter {
            output: ExecOutput {
                exit_code: 1,
                ..passing_output()
            },
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[0]; // expects exit 0

        let result = runner.run_one(&alpine(), scenario);
        assert!(result.outcome.is_failure());
        if let ScenarioOutcome::Fail { failures } = &result.outcome {
            assert!(failures[0].contains("exit code"));
        }
    }

    #[test]
    fn runner_fail_stdout_missing() {
        let adapter = MockAdapter {
            output: ExecOutput {
                stdout: "wrong output".to_string(),
                ..passing_output()
            },
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[1]; // expects "hello-from-override"

        let result = runner.run_one(&alpine(), scenario);
        assert!(result.outcome.is_failure());
        if let ScenarioOutcome::Fail { failures } = &result.outcome {
            assert!(failures.iter().any(|f| f.contains("stdout")));
        }
    }

    #[test]
    fn runner_error_adapter() {
        let adapter = FailingAdapter {
            message: "connection refused".to_string(),
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[0];

        let result = runner.run_one(&alpine(), scenario);
        assert!(result.outcome.is_failure());
        if let ScenarioOutcome::Error { message } = &result.outcome {
            assert_eq!(message, "connection refused");
        }
    }

    #[test]
    fn runner_run_image_all_scenarios() {
        let adapter = MockAdapter {
            output: passing_output(),
        };
        let runner = ScenarioRunner::new(adapter);
        let scenarios = s1_entrypoint_scenarios();

        let results = runner.run_image(&alpine(), &scenarios);
        assert_eq!(results.len(), scenarios.len());
    }

    #[test]
    fn runner_run_cohort() {
        let adapter = MockAdapter {
            output: passing_output(),
        };
        let runner = ScenarioRunner::new(adapter);
        let images = vec![alpine(), ImageRef {
            reference: "python:3.12-slim".to_string(),
            digest: None,
            label: "Python".to_string(),
        }];
        let scenarios = s1_entrypoint_scenarios();

        let results = runner.run_cohort(&images, &scenarios);
        assert_eq!(results.len(), images.len() * scenarios.len());
    }

    #[test]
    fn lifecycle_expectation_pass() {
        let output = passing_output();
        let expectations = vec![Expectation::LifecycleSequence {
            events: vec![
                "create".to_string(),
                "start".to_string(),
                "exec".to_string(),
                "delete".to_string(),
            ],
        }];
        let failures = evaluate_expectations(&expectations, &output);
        assert!(failures.is_empty());
    }

    #[test]
    fn lifecycle_expectation_fail() {
        let output = ExecOutput {
            lifecycle_events: vec!["create".to_string(), "start".to_string()],
            ..passing_output()
        };
        let expectations = vec![Expectation::LifecycleSequence {
            events: vec![
                "create".to_string(),
                "start".to_string(),
                "exec".to_string(),
                "delete".to_string(),
            ],
        }];
        let failures = evaluate_expectations(&expectations, &output);
        assert!(!failures.is_empty());
        assert!(failures[0].contains("lifecycle sequence mismatch"));
    }

    #[test]
    fn result_has_duration() {
        let adapter = MockAdapter {
            output: passing_output(),
        };
        let runner = ScenarioRunner::new(adapter);
        let scenario = &s1_entrypoint_scenarios()[0];

        let result = runner.run_one(&alpine(), scenario);
        // Duration should be non-negative (can be zero on fast machines).
        assert!(result.duration >= Duration::ZERO);
    }

    #[test]
    fn end_to_end_with_report() {
        use crate::cohort::Tier;
        use crate::report::TestReport;

        // Use an output that satisfies the first scenario (s1-image-defaults: exit 0 only).
        let adapter = MockAdapter {
            output: ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
                lifecycle_events: vec![],
            },
        };
        let runner = ScenarioRunner::new(adapter);
        // Use only the first scenario (exit code check only) for the end-to-end test.
        let scenarios = vec![s1_entrypoint_scenarios().into_iter().next().unwrap()];

        let mut report = TestReport::new(Tier::Tier1, "2026-02-20T00:00:00Z");
        for result in runner.run_image(&alpine(), &scenarios) {
            report.add_result(result);
        }
        report.finalize(Duration::from_millis(100));

        assert_eq!(report.total(), 1);
        assert!(report.all_passed());

        // Verify JSON serialization roundtrips.
        let json = serde_json::to_string_pretty(&report).unwrap();
        let deserialized: TestReport = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.tier, Tier::Tier1);
        assert_eq!(deserialized.results.len(), 1);
    }
}
