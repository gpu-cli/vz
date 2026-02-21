//! `vz validate` — Run validation suites against image cohorts.
//!
//! Subcommands:
//! - `run`      — Execute a tier's validation suite and produce a report.
//! - `manifest` — Dump the current cohort manifest as JSON.
//! - `list`     — List images, scenarios, and matrix sizes per tier.

use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use tracing::info;

use vz_validation::{
    CohortManifest, ExecOutput, MockAdapter, OciRuntimeAdapter, RuntimeAdapter, ScenarioRunner,
    StressConfig, StressReport, TestReport, default_manifest, stress_scenario,
};

/// Validate OCI image cohorts with tiered test suites.
#[derive(Debug, Args)]
pub struct ValidateArgs {
    #[command(subcommand)]
    pub action: ValidateCommand,
}

#[derive(Debug, Subcommand)]
pub enum ValidateCommand {
    /// Run validation scenarios for a specific tier.
    Run(RunArgs),
    /// Print the cohort manifest as JSON.
    Manifest(ManifestArgs),
    /// List images, scenarios, and test matrix for each tier.
    List(ListArgs),
}

/// Arguments for `vz validate run`.
#[derive(Debug, Args)]
pub struct RunArgs {
    /// CI tier to run (1, 2, or 3).
    #[arg(long, default_value = "1")]
    pub tier: u8,

    /// Output report to a file (JSON).
    #[arg(long)]
    pub output: Option<PathBuf>,

    /// Dry-run mode: log what would execute without running containers.
    #[arg(long, default_value = "true")]
    pub dry_run: bool,

    /// Output report as JSON to stdout.
    #[arg(long)]
    pub json: bool,

    /// Number of stress iterations (Tier 3 only).
    #[arg(long, default_value = "100")]
    pub iterations: usize,

    /// Maximum allowed flake rate (0.0-1.0, Tier 3 only).
    #[arg(long, default_value = "0.05")]
    pub max_flake_rate: f64,
}

/// Arguments for `vz validate manifest`.
#[derive(Debug, Args)]
pub struct ManifestArgs {
    /// Write manifest to a file instead of stdout.
    #[arg(long)]
    pub output: Option<PathBuf>,
}

/// Arguments for `vz validate list`.
#[derive(Debug, Args)]
pub struct ListArgs {
    /// Filter by tier (1, 2, or 3). If omitted, show all tiers.
    #[arg(long)]
    pub tier: Option<u8>,

    /// Output as JSON.
    #[arg(long)]
    pub json: bool,
}

/// Entry point for `vz validate`.
pub async fn run(args: ValidateArgs) -> Result<()> {
    match args.action {
        ValidateCommand::Run(args) => cmd_run(args),
        ValidateCommand::Manifest(args) => cmd_manifest(args),
        ValidateCommand::List(args) => cmd_list(args),
    }
}

fn tier_from_num(num: u8) -> Result<vz_validation::Tier> {
    match num {
        1 => Ok(vz_validation::Tier::Tier1),
        2 => Ok(vz_validation::Tier::Tier2),
        3 => Ok(vz_validation::Tier::Tier3),
        _ => bail!("invalid tier {num}: must be 1, 2, or 3"),
    }
}

fn now_iso8601() -> String {
    // Simple timestamp without chrono dependency.
    format!(
        "run-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    )
}

fn cmd_run(args: RunArgs) -> Result<()> {
    let tier = tier_from_num(args.tier)?;
    let manifest = default_manifest(&now_iso8601());

    let cohort = manifest
        .cohort_for_tier(tier)
        .context(format!("no cohort defined for tier {}", args.tier))?;

    info!(
        "Running {} validation: {} images",
        tier.label(),
        cohort.images.len()
    );

    // Tier 3 uses stress mode with repeated iterations.
    if args.tier == 3 {
        return cmd_run_stress(&args, &manifest, cohort);
    }

    let start = Instant::now();
    let timestamp = now_iso8601();
    let mut report = TestReport::new(tier, &timestamp);

    if args.dry_run {
        let adapter = DryRunAdapter;
        let runner = ScenarioRunner::new(adapter);
        run_cohort(&runner, &manifest, cohort, &mut report);
    } else {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        let data_dir = std::path::PathBuf::from(home).join(".vz").join("oci");
        let adapter = OciRuntimeAdapter::new(&data_dir);
        let runner = ScenarioRunner::new(adapter);
        run_cohort(&runner, &manifest, cohort, &mut report);
    }

    let duration = start.elapsed();
    report.finalize(duration);

    // Output results.
    if args.json {
        let json = serde_json::to_string_pretty(&report).context("failed to serialize report")?;
        println!("{json}");
    } else {
        print_report_summary(&report);
        if args.tier >= 2 {
            print_per_image_summary(&report);
        }
    }

    // Write to file if requested.
    if let Some(output) = args.output {
        let json = serde_json::to_string_pretty(&report).context("failed to serialize report")?;
        std::fs::write(&output, &json)
            .with_context(|| format!("failed to write report to {}", output.display()))?;
        info!("Report written to {}", output.display());
    }

    if !report.all_passed() {
        bail!(
            "validation failed: {} of {} tests failed",
            report.failed(),
            report.total()
        );
    }

    Ok(())
}

fn cmd_run_stress(
    args: &RunArgs,
    manifest: &CohortManifest,
    cohort: &vz_validation::ImageCohort,
) -> Result<()> {
    let config = StressConfig {
        iterations: args.iterations,
        max_flake_rate: args.max_flake_rate,
    };

    let start = Instant::now();
    let timestamp = now_iso8601();
    let mut report = StressReport::new(&timestamp, config.clone());

    // Both dry-run and real mode use a mock for now (real adapter in future beads).
    let mock = MockAdapter {
        output: ExecOutput {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
            lifecycle_events: Vec::new(),
        },
    };
    let runner = ScenarioRunner::new(mock);

    for image in &cohort.images {
        let scenarios = manifest.scenarios_for_image(&image.reference);
        for scenario in &scenarios {
            info!(
                "Stress testing {} / {} ({} iterations)",
                image.label, scenario.id, config.iterations
            );
            let result = stress_scenario(&runner, image, scenario, config.iterations);
            report.add_result(result);
        }
    }

    let duration = start.elapsed();
    report.finalize(duration);

    // Output results.
    if args.json {
        let json =
            serde_json::to_string_pretty(&report).context("failed to serialize stress report")?;
        println!("{json}");
    } else {
        print_stress_summary(&report);
    }

    // Write to file if requested.
    if let Some(ref output) = args.output {
        let json =
            serde_json::to_string_pretty(&report).context("failed to serialize stress report")?;
        std::fs::write(output, &json)
            .with_context(|| format!("failed to write report to {}", output.display()))?;
        info!("Stress report written to {}", output.display());
    }

    if !report.all_passed() {
        bail!(
            "stress test failed: {} hard failures, {} flaky (>{:.0}% rate)",
            report.hard_failure_count(),
            report.flaky_count(),
            report.config.max_flake_rate * 100.0,
        );
    }

    Ok(())
}

fn print_stress_summary(report: &StressReport) {
    println!("{}", report.summary_line());
    println!();

    for result in &report.results {
        let status = if result.hard_failure {
            "HARD-FAIL"
        } else if result.flake_rate > report.config.max_flake_rate {
            "FLAKY"
        } else {
            "STABLE"
        };

        println!(
            "  [{status:<9}] {image} / {scenario}: {passed}/{total} passed ({flake:.1}% flake, {avg}ms avg)",
            image = result.image.label,
            scenario = result.scenario_id,
            passed = result.passed,
            total = result.iterations,
            flake = result.flake_rate * 100.0,
            avg = result.avg_duration.as_millis(),
        );

        if let Some(ref failure) = result.first_failure {
            println!("            first failure: {failure}");
        }
    }
}

fn run_cohort<R: RuntimeAdapter>(
    runner: &ScenarioRunner<R>,
    manifest: &CohortManifest,
    cohort: &vz_validation::ImageCohort,
    report: &mut TestReport,
) {
    for image in &cohort.images {
        let scenarios = manifest.scenarios_for_image(&image.reference);
        if scenarios.is_empty() {
            info!("Skipping {} (no applicable scenarios)", image.label);
            continue;
        }

        info!("Testing {} ({} scenarios)", image.label, scenarios.len());

        let results = runner.run_image(image, &scenarios);
        for result in results {
            report.add_result(result);
        }
    }
}

fn print_report_summary(report: &TestReport) {
    println!("{}", report.summary_line());
    println!();

    for result in &report.results {
        let status = if result.outcome.is_pass() {
            "PASS"
        } else {
            "FAIL"
        };
        println!(
            "  [{status}] {image} / {scenario} ({ms}ms)",
            image = result.image.label,
            scenario = result.scenario_id,
            ms = result.duration.as_millis(),
        );

        if let vz_validation::ScenarioOutcome::Fail { failures } = &result.outcome {
            for f in failures {
                println!("         {f}");
            }
        }
        if let vz_validation::ScenarioOutcome::Error { message } = &result.outcome {
            println!("         error: {message}");
        }
    }
}

fn print_per_image_summary(report: &TestReport) {
    let summaries = report.per_image_summary();
    if summaries.is_empty() {
        return;
    }

    println!();
    println!("Per-image outcomes:");
    println!(
        "  {:<30} {:>6} {:>6} {:>7}",
        "IMAGE", "PASS", "FAIL", "SKIP"
    );
    for s in &summaries {
        let status = if s.failed > 0 { "FAIL" } else { "OK" };
        println!(
            "  {:<30} {:>6} {:>6} {:>7}  [{}]",
            s.reference, s.passed, s.failed, s.skipped, status
        );
    }
}

fn cmd_manifest(args: ManifestArgs) -> Result<()> {
    let manifest = default_manifest(&now_iso8601());
    let json = manifest.to_json().context("failed to serialize manifest")?;

    if let Some(output) = args.output {
        std::fs::write(&output, &json)
            .with_context(|| format!("failed to write manifest to {}", output.display()))?;
        println!("Manifest written to {}", output.display());
    } else {
        println!("{json}");
    }

    Ok(())
}

fn cmd_list(args: ListArgs) -> Result<()> {
    let manifest = default_manifest(&now_iso8601());

    let tiers_to_show: Vec<vz_validation::Tier> = if let Some(num) = args.tier {
        vec![tier_from_num(num)?]
    } else {
        vec![
            vz_validation::Tier::Tier1,
            vz_validation::Tier::Tier2,
            vz_validation::Tier::Tier3,
        ]
    };

    if args.json {
        let info: Vec<TierInfo> = tiers_to_show
            .iter()
            .filter_map(|t| {
                manifest.cohort_for_tier(*t).map(|c| TierInfo {
                    tier: t.label().to_string(),
                    images: c.images.iter().map(|i| i.reference.clone()).collect(),
                    scenarios: c.scenarios.iter().map(|s| s.label().to_string()).collect(),
                    matrix_size: manifest.test_matrix_size(*t),
                })
            })
            .collect();
        let json = serde_json::to_string_pretty(&info).context("failed to serialize tier info")?;
        println!("{json}");
    } else {
        for tier in &tiers_to_show {
            let Some(cohort) = manifest.cohort_for_tier(*tier) else {
                println!("{}: (not defined)", tier.label());
                continue;
            };
            let matrix_size = manifest.test_matrix_size(*tier);

            println!("{}", tier.label());
            println!("  Images ({}):", cohort.images.len());
            for img in &cohort.images {
                let scenario_count = manifest.scenarios_for_image(&img.reference).len();
                println!("    {} ({} scenarios)", img.reference, scenario_count);
            }
            println!("  Scenario kinds:");
            for kind in &cohort.scenarios {
                println!("    {}", kind.label());
            }
            println!("  Total matrix: {} test cases", matrix_size);
            println!();
        }
    }

    Ok(())
}

#[derive(serde::Serialize)]
struct TierInfo {
    tier: String,
    images: Vec<String>,
    scenarios: Vec<String>,
    matrix_size: usize,
}

/// Dry-run adapter that logs scenario execution without running anything.
struct DryRunAdapter;

impl RuntimeAdapter for DryRunAdapter {
    fn execute(
        &self,
        image: &vz_validation::ImageRef,
        scenario: &vz_validation::Scenario,
    ) -> Result<ExecOutput, String> {
        info!(
            "[dry-run] {} / {}: {}",
            image.label, scenario.id, scenario.description
        );
        // Return a synthetic passing output for dry-run mode.
        Ok(ExecOutput {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
            lifecycle_events: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn tier_from_num_valid() {
        assert_eq!(tier_from_num(1).unwrap(), vz_validation::Tier::Tier1);
        assert_eq!(tier_from_num(2).unwrap(), vz_validation::Tier::Tier2);
        assert_eq!(tier_from_num(3).unwrap(), vz_validation::Tier::Tier3);
    }

    #[test]
    fn tier_from_num_invalid() {
        assert!(tier_from_num(0).is_err());
        assert!(tier_from_num(4).is_err());
    }

    #[test]
    fn dry_run_adapter_returns_pass() {
        let adapter = DryRunAdapter;
        let image = vz_validation::ImageRef {
            reference: "alpine:3.20".to_string(),
            digest: None,
            label: "Alpine".to_string(),
        };
        let scenarios = vz_validation::s1_entrypoint_scenarios();
        let result = adapter.execute(&image, &scenarios[0]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().exit_code, 0);
    }

    #[test]
    fn report_summary_format() {
        use std::time::Duration;

        let manifest = default_manifest("2026-02-20T00:00:00Z");
        let tier = vz_validation::Tier::Tier1;
        let cohort = manifest.cohort_for_tier(tier).unwrap();

        let adapter = DryRunAdapter;
        let runner = ScenarioRunner::new(adapter);
        let mut report = TestReport::new(tier, "2026-02-20T00:00:00Z");
        run_cohort(&runner, &manifest, cohort, &mut report);
        report.finalize(Duration::from_millis(100));

        assert!(report.total() > 0);
        // Dry-run returns exit 0 for all; first scenario expects exit 0, so passes.
        // But s1-cmd-override expects stdout to contain "hello-from-override" which
        // dry-run doesn't produce, so some will fail.
        let line = report.summary_line();
        assert!(line.contains("tier-1-smoke"));
        assert!(line.contains("100ms"));
    }

    #[test]
    fn manifest_json_output() {
        let manifest = default_manifest("2026-02-20T00:00:00Z");
        let json = manifest.to_json().unwrap();
        assert!(json.contains("1.0.0"));
        assert!(json.contains("alpine:3.20"));
    }

    #[test]
    fn tier_info_serialization() {
        let info = TierInfo {
            tier: "tier-1-smoke".to_string(),
            images: vec!["alpine:3.20".to_string()],
            scenarios: vec!["entrypoint-cmd".to_string()],
            matrix_size: 6,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("tier-1-smoke"));
        assert!(json.contains("matrix_size"));
    }
}
