//! Validation harness for OCI image and compose scenario testing.
//!
//! Provides scenario definitions, a pluggable runner, and structured
//! pass/fail reporting. Used by CI tiers (PR smoke, nightly, weekly)
//! to validate image compatibility and lifecycle conformance.

#![forbid(unsafe_code)]

mod cohort;
mod manifest;
mod report;
mod runner;
mod scenario;
mod stress;

pub use cohort::{ImageCohort, ImageRef, Tier, tier1_smoke, tier2_nightly, tier3_weekly};
pub use manifest::{
    CohortManifest, DefaultBehavior, ImageProfile, MANIFEST_VERSION, ProbeProtocol, ServiceInfo,
    default_manifest,
};
pub use report::{ImageSummary, ScenarioOutcome, TestReport, TestResult};
pub use runner::{ExecOutput, FailingAdapter, MockAdapter, RuntimeAdapter, ScenarioRunner};
pub use scenario::{
    Expectation, Scenario, ScenarioKind, s1_entrypoint_scenarios, s1_env_cwd_scenarios,
    s2_user_scenarios, s3_mount_scenarios, s4_signal_scenarios, s5_service_scenarios,
    s6_compose_scenarios,
};
pub use stress::{ScenarioStressResult, StressConfig, StressReport, stress_scenario};
