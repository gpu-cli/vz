//! Validation harness for OCI image and compose scenario testing.
//!
//! Provides scenario definitions, a pluggable runner, and structured
//! pass/fail reporting. Used by CI tiers (PR smoke, nightly, weekly)
//! to validate image compatibility and lifecycle conformance.

#![forbid(unsafe_code)]

mod cohort;
mod manifest;
#[cfg(feature = "oci")]
mod oci_adapter;
mod report;
mod runner;
mod scenario;
mod stress;

pub use cohort::{ImageCohort, ImageRef, Tier, tier1_smoke, tier2_nightly, tier3_weekly};
pub use manifest::{
    CohortManifest, DefaultBehavior, ImageProfile, MANIFEST_VERSION, ProbeProtocol, ServiceInfo,
    default_manifest,
};
#[cfg(feature = "oci")]
pub use oci_adapter::OciRuntimeAdapter;
pub use report::{
    FailureCategory, ImageSummary, ScenarioOutcome, TestReport, TestResult,
    classify_failure_category,
};
pub use runner::{ExecOutput, FailingAdapter, MockAdapter, RuntimeAdapter, ScenarioRunner};
pub use scenario::{
    ComposeServiceSpec, ConnectivityCheck, Expectation, Scenario, ScenarioKind,
    s1_entrypoint_scenarios, s1_env_cwd_scenarios, s2_user_scenarios, s3_mount_scenarios,
    s4_signal_scenarios, s5_service_scenarios, s6_compose_scenarios,
};
pub use stress::{
    ScenarioStressResult, StressConfig, StressReport, SweepConfig, SweepMode, stress_scenario,
    sweep_scenario,
};
