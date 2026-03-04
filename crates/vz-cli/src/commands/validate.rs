//! `vz validate` — Run validation suites against image cohorts.
//!
//! Subcommands:
//! - `run`      — Execute a tier's validation suite and produce a report.
//! - `manifest` — Dump the current cohort manifest as JSON.
//! - `list`     — List images, scenarios, and matrix sizes per tier.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use tracing::info;

use vz_validation::{
    CohortManifest, ExecOutput, FailureCategory, MockAdapter, OciRuntimeAdapter, RuntimeAdapter,
    ScenarioRunner, StressConfig, StressReport, TestReport, classify_failure_category,
    default_manifest, stress_scenario,
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
    /// Run external Dockerfile compatibility build sweep cases.
    SweepBuild(SweepBuildArgs),
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

/// Arguments for `vz validate sweep-build`.
#[derive(Debug, Args)]
pub struct SweepBuildArgs {
    /// Path to sweep case manifest JSON.
    #[arg(long)]
    pub manifest: PathBuf,

    /// Repository root used for relative case paths (defaults to cwd).
    #[arg(long)]
    pub repo_root: Option<PathBuf>,

    /// Only resolve and report context/dockerfile mapping, do not run builds.
    #[arg(long, default_value = "false")]
    pub dry_run: bool,

    /// Continue processing all cases after failures.
    #[arg(long, default_value = "true")]
    pub continue_on_error: bool,

    /// Output report as JSON.
    #[arg(long)]
    pub json: bool,
}

/// Entry point for `vz validate`.
pub async fn run(args: ValidateArgs) -> Result<()> {
    match args.action {
        ValidateCommand::Run(args) => cmd_run(args),
        ValidateCommand::Manifest(args) => cmd_manifest(args),
        ValidateCommand::List(args) => cmd_list(args),
        ValidateCommand::SweepBuild(args) => cmd_sweep_build(args).await,
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
        print_failure_category_summary(&report);
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
        let category_suffix = result
            .failure_category
            .map(|category| format!(" [{}]", category.label()))
            .unwrap_or_default();
        println!(
            "  [{status}] {image} / {scenario} ({ms}ms){category_suffix}",
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

fn print_failure_category_summary(report: &TestReport) {
    let counts = report.failure_category_counts();
    if counts.is_empty() {
        return;
    }
    println!();
    println!("Failure categories:");
    for (category, count) in counts {
        println!("  {:<30} {}", category.label(), count);
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

#[derive(Debug, serde::Deserialize)]
struct BuildSweepManifest {
    cases: Vec<BuildSweepCase>,
}

#[derive(Debug, serde::Deserialize)]
struct BuildSweepCase {
    id: String,
    dockerfile: PathBuf,
    #[serde(default)]
    context: Option<PathBuf>,
    #[serde(default)]
    repo_root: Option<PathBuf>,
    #[serde(default)]
    tag: Option<String>,
    #[serde(default)]
    build_args: BTreeMap<String, String>,
    #[serde(default)]
    target: Option<String>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum BuildSweepStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Debug, serde::Serialize)]
struct BuildSweepCaseResult {
    id: String,
    status: BuildSweepStatus,
    context_dir: String,
    dockerfile: String,
    tag: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure_category: Option<FailureCategory>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct BuildSweepReport {
    total: usize,
    passed: usize,
    failed: usize,
    skipped: usize,
    results: Vec<BuildSweepCaseResult>,
}

#[derive(Debug)]
struct ResolvedBuildSweepCase {
    id: String,
    context_dir: PathBuf,
    dockerfile: PathBuf,
    tag: String,
    build_args: Vec<String>,
    target: Option<String>,
}

async fn cmd_sweep_build(args: SweepBuildArgs) -> Result<()> {
    let manifest_text = std::fs::read_to_string(&args.manifest)
        .with_context(|| format!("failed to read manifest {}", args.manifest.display()))?;
    let manifest: BuildSweepManifest =
        serde_json::from_str(&manifest_text).context("failed to parse sweep manifest JSON")?;
    if manifest.cases.is_empty() {
        bail!("sweep manifest contains no cases");
    }

    let default_repo_root = if let Some(path) = args.repo_root {
        canonicalize_existing_dir(&path)?
    } else {
        std::env::current_dir().context("failed to resolve current directory")?
    };

    let mut report = BuildSweepReport {
        total: manifest.cases.len(),
        passed: 0,
        failed: 0,
        skipped: 0,
        results: Vec::with_capacity(manifest.cases.len()),
    };

    for case in manifest.cases {
        let resolved = resolve_build_sweep_case(&case, &default_repo_root);
        let result = match resolved {
            Ok(resolved) => run_build_sweep_case(resolved, args.dry_run).await,
            Err(error) => BuildSweepCaseResult {
                id: case.id.clone(),
                status: BuildSweepStatus::Failed,
                context_dir: String::new(),
                dockerfile: String::new(),
                tag: case.tag.unwrap_or_else(|| sweep_case_tag(case.id.as_str())),
                failure_category: Some(classify_failure_category(&error.to_string())),
                message: Some(error.to_string()),
            },
        };

        match result.status {
            BuildSweepStatus::Passed => report.passed += 1,
            BuildSweepStatus::Failed => report.failed += 1,
            BuildSweepStatus::Skipped => report.skipped += 1,
        }

        if matches!(result.status, BuildSweepStatus::Failed) && !args.continue_on_error {
            report.results.push(result);
            break;
        }

        report.results.push(result);
    }

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).context("failed to serialize sweep report")?
        );
    } else {
        print_build_sweep_report(&report);
    }

    if report.failed > 0 {
        bail!(
            "build sweep failed: {} of {} cases failed",
            report.failed,
            report.results.len()
        );
    }
    Ok(())
}

fn resolve_build_sweep_case(
    case: &BuildSweepCase,
    default_repo_root: &Path,
) -> Result<ResolvedBuildSweepCase> {
    let repo_root = if let Some(root) = &case.repo_root {
        if root.is_absolute() {
            canonicalize_existing_dir(root)?
        } else {
            canonicalize_existing_dir(&default_repo_root.join(root))?
        }
    } else {
        canonicalize_existing_dir(default_repo_root)?
    };

    let dockerfile_abs = if case.dockerfile.is_absolute() {
        case.dockerfile.clone()
    } else {
        repo_root.join(&case.dockerfile)
    };
    let dockerfile_abs = canonicalize_existing_file(&dockerfile_abs)?;

    let context_input = case.context.as_ref().map(PathBuf::from).unwrap_or_else(|| {
        case.dockerfile
            .parent()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
    });
    let context_abs = if context_input.is_absolute() {
        canonicalize_existing_dir(&context_input)?
    } else {
        canonicalize_existing_dir(&repo_root.join(context_input))?
    };

    let dockerfile_relative = dockerfile_abs.strip_prefix(&context_abs).map_err(|_| {
        anyhow::anyhow!(
            "dockerfile {} must be within context {}",
            dockerfile_abs.display(),
            context_abs.display()
        )
    })?;

    let tag = case
        .tag
        .clone()
        .unwrap_or_else(|| sweep_case_tag(case.id.as_str()));
    let build_args = case
        .build_args
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();

    Ok(ResolvedBuildSweepCase {
        id: case.id.clone(),
        context_dir: context_abs,
        dockerfile: dockerfile_relative.to_path_buf(),
        tag,
        build_args,
        target: case.target.clone(),
    })
}

async fn run_build_sweep_case(case: ResolvedBuildSweepCase, dry_run: bool) -> BuildSweepCaseResult {
    if dry_run {
        return BuildSweepCaseResult {
            id: case.id,
            status: BuildSweepStatus::Skipped,
            context_dir: case.context_dir.display().to_string(),
            dockerfile: case.dockerfile.display().to_string(),
            tag: case.tag,
            failure_category: None,
            message: Some("dry-run".to_string()),
        };
    }

    let build_args = super::build::BuildArgs {
        subcommand: None,
        context: case.context_dir.clone(),
        tag: Some(case.tag.clone()),
        dockerfile: case.dockerfile.clone(),
        target: case.target.clone(),
        build_args: case.build_args.clone(),
        secrets: Vec::new(),
        no_cache: false,
        push: false,
        output: None,
        progress: super::build::ProgressArg::Plain,
        opts: super::oci::ContainerOpts::default(),
    };

    let result = super::build::run(build_args).await;
    match result {
        Ok(()) => BuildSweepCaseResult {
            id: case.id,
            status: BuildSweepStatus::Passed,
            context_dir: case.context_dir.display().to_string(),
            dockerfile: case.dockerfile.display().to_string(),
            tag: case.tag,
            failure_category: None,
            message: None,
        },
        Err(error) => {
            let message = error.to_string();
            BuildSweepCaseResult {
                id: case.id,
                status: BuildSweepStatus::Failed,
                context_dir: case.context_dir.display().to_string(),
                dockerfile: case.dockerfile.display().to_string(),
                tag: case.tag,
                failure_category: Some(classify_failure_category(&message)),
                message: Some(message),
            }
        }
    }
}

fn print_build_sweep_report(report: &BuildSweepReport) {
    println!(
        "Build sweep: total={} passed={} failed={} skipped={}",
        report.total, report.passed, report.failed, report.skipped
    );
    for result in &report.results {
        let status = match result.status {
            BuildSweepStatus::Passed => "PASS",
            BuildSweepStatus::Failed => "FAIL",
            BuildSweepStatus::Skipped => "SKIP",
        };
        println!(
            "  [{status}] {id} context={} dockerfile={} tag={}",
            result.context_dir,
            result.dockerfile,
            result.tag,
            id = result.id
        );
        if let Some(category) = result.failure_category {
            println!("         category={}", category.label());
        }
        if let Some(message) = &result.message {
            println!("         {message}");
        }
    }
}

fn canonicalize_existing_dir(path: &Path) -> Result<PathBuf> {
    let canonical = std::fs::canonicalize(path)
        .with_context(|| format!("path does not exist: {}", path.display()))?;
    if !canonical.is_dir() {
        bail!("path is not a directory: {}", canonical.display());
    }
    Ok(canonical)
}

fn canonicalize_existing_file(path: &Path) -> Result<PathBuf> {
    let canonical = std::fs::canonicalize(path)
        .with_context(|| format!("path does not exist: {}", path.display()))?;
    if !canonical.is_file() {
        bail!("path is not a file: {}", canonical.display());
    }
    Ok(canonical)
}

fn sweep_case_tag(id: &str) -> String {
    let mut sanitized = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    sanitized.truncate(64);
    if sanitized.trim_matches('-').is_empty() {
        "sweep-case".to_string()
    } else {
        format!("vz-sweep:{sanitized}")
    }
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

    #[test]
    fn resolve_build_sweep_case_defaults_context_to_dockerfile_parent() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        let docker_dir = repo.join("docker-stacks").join("binder");
        std::fs::create_dir_all(&docker_dir).unwrap();
        std::fs::write(docker_dir.join("Dockerfile"), "FROM alpine:3.20\n").unwrap();

        let case = BuildSweepCase {
            id: "binder-default".to_string(),
            dockerfile: PathBuf::from("docker-stacks/binder/Dockerfile"),
            context: None,
            repo_root: None,
            tag: None,
            build_args: BTreeMap::new(),
            target: None,
        };

        let resolved = resolve_build_sweep_case(&case, &repo).unwrap();
        assert_eq!(resolved.context_dir, docker_dir.canonicalize().unwrap());
        assert_eq!(resolved.dockerfile, PathBuf::from("Dockerfile"));
    }

    #[test]
    fn resolve_build_sweep_case_honors_explicit_context_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        let docker_dir = repo.join("docker-stacks").join("binder");
        std::fs::create_dir_all(repo.join("binder")).unwrap();
        std::fs::create_dir_all(&docker_dir).unwrap();
        std::fs::write(repo.join("binder").join("README.ipynb"), "notebook").unwrap();
        std::fs::write(docker_dir.join("Dockerfile"), "FROM alpine:3.20\n").unwrap();

        let case = BuildSweepCase {
            id: "binder-root-context".to_string(),
            dockerfile: PathBuf::from("docker-stacks/binder/Dockerfile"),
            context: Some(PathBuf::from(".")),
            repo_root: None,
            tag: None,
            build_args: BTreeMap::new(),
            target: None,
        };

        let resolved = resolve_build_sweep_case(&case, &repo).unwrap();
        assert_eq!(resolved.context_dir, repo.canonicalize().unwrap());
        assert_eq!(
            resolved.dockerfile,
            PathBuf::from("docker-stacks/binder/Dockerfile")
        );
    }
}
