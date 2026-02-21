//! `vz stack` — multi-service stack lifecycle commands.
//!
//! Provides `up`, `down`, `ps`, and `events` subcommands backed by
//! the `vz-stack` control plane. The [`OciContainerRuntime`] bridges
//! the async `vz_oci::Runtime` to the sync [`ContainerRuntime`] trait
//! using `block_in_place` + `block_on`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use tracing::info;

use vz_stack::{
    ApplyResult, ContainerRuntime, EventRecord, ExecutionResult, OrchestrationConfig, RoundReport,
    ServiceObservedState, ServicePhase, StackError, StackEvent, StackExecutor, StackOrchestrator,
    StackSpec, StateStore, parse_compose_with_dir,
};

/// Manage multi-service stacks from Compose files.
#[derive(Args, Debug)]
pub struct StackArgs {
    #[command(subcommand)]
    pub action: StackCommand,
}

#[derive(Subcommand, Debug)]
pub enum StackCommand {
    /// Start services defined in a compose file.
    Up(UpArgs),

    /// Stop and remove all services in a stack.
    Down(DownArgs),

    /// List services and their current status.
    Ps(PsArgs),

    /// Show stack lifecycle events.
    Events(EventsArgs),

    /// Show service logs (event history and container output).
    Logs(LogsArgs),
}

#[derive(Args, Debug)]
pub struct UpArgs {
    /// Path to compose YAML file.
    #[arg(short, long, default_value = "compose.yaml")]
    pub file: PathBuf,

    /// Stack name (defaults to parent directory name).
    #[arg(short = 'n', long)]
    pub name: Option<String>,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// Show planned actions without executing them.
    #[arg(long)]
    pub dry_run: bool,

    /// Start services and return immediately without waiting for
    /// health checks to converge.
    #[arg(short, long)]
    pub detach: bool,
}

#[derive(Args, Debug)]
pub struct DownArgs {
    /// Stack name to stop.
    pub name: String,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// Show planned actions without executing them.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Args, Debug)]
pub struct PsArgs {
    /// Stack name to inspect.
    pub name: String,

    /// Output as JSON.
    #[arg(long)]
    pub json: bool,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct EventsArgs {
    /// Stack name to inspect.
    pub name: String,

    /// Show events since this event ID.
    #[arg(long, default_value_t = 0)]
    pub since: i64,

    /// Output as JSON.
    #[arg(long)]
    pub json: bool,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct LogsArgs {
    /// Stack name to show logs for.
    pub name: String,

    /// Filter logs to a specific service.
    #[arg(short, long)]
    pub service: Option<String>,

    /// Follow log output (poll for new events).
    #[arg(short, long)]
    pub follow: bool,

    /// Number of recent events to show (0 = all).
    #[arg(short = 'n', long, default_value_t = 50)]
    pub tail: usize,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

pub async fn run(args: StackArgs) -> anyhow::Result<()> {
    match args.action {
        StackCommand::Up(args) => cmd_up(args).await,
        StackCommand::Down(args) => cmd_down(args).await,
        StackCommand::Ps(args) => cmd_ps(args).await,
        StackCommand::Events(args) => cmd_events(args).await,
        StackCommand::Logs(args) => cmd_logs(args).await,
    }
}

// ── OCI container runtime bridge ──────────────────────────────────

/// Bridges the async `vz_oci::Runtime` to the sync [`ContainerRuntime`] trait.
///
/// Each method uses `tokio::task::block_in_place` + `Handle::block_on`
/// to call async OCI runtime methods from within the synchronous
/// executor context.
struct OciContainerRuntime {
    runtime: vz_oci::Runtime,
    handle: tokio::runtime::Handle,
}

impl OciContainerRuntime {
    fn new(oci_data_dir: &Path) -> anyhow::Result<Self> {
        let config = vz_oci::RuntimeConfig {
            data_dir: oci_data_dir.to_path_buf(),
            ..Default::default()
        };
        let runtime = vz_oci::Runtime::new(config);
        let handle = tokio::runtime::Handle::current();
        Ok(Self { runtime, handle })
    }
}

impl ContainerRuntime for OciContainerRuntime {
    fn pull(&self, image: &str) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.pull(image))
                .map(|id| id.0)
                .map_err(|e| StackError::Network(format!("pull failed: {e}")))
        })
    }

    fn create(&self, image: &str, config: vz_oci::RunConfig) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.create_container(image, config))
                .map_err(|e| StackError::Network(format!("create failed: {e}")))
        })
    }

    fn stop(&self, container_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.stop_container(container_id, false))
                .map(|_| ())
                .map_err(|e| StackError::Network(format!("stop failed: {e}")))
        })
    }

    fn remove(&self, container_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.remove_container(container_id))
                .map_err(|e| StackError::Network(format!("remove failed: {e}")))
        })
    }

    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError> {
        tokio::task::block_in_place(|| {
            let exec_config = vz_oci::ExecConfig {
                cmd: command.to_vec(),
                ..Default::default()
            };
            self.handle
                .block_on(self.runtime.exec_container(container_id, exec_config))
                .map(|output| output.exit_code)
                .map_err(|e| StackError::Network(format!("exec failed: {e}")))
        })
    }

    fn boot_shared_vm(
        &self,
        stack_id: &str,
        config: vz_oci::StackVmConfig,
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.boot_shared_vm(stack_id, config))
                .map_err(|e| StackError::Network(format!("boot_shared_vm failed: {e}")))
        })
    }

    fn create_in_stack(
        &self,
        stack_id: &str,
        image: &str,
        config: vz_oci::RunConfig,
    ) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.runtime
                        .create_container_in_stack(stack_id, image, config),
                )
                .map_err(|e| StackError::Network(format!("create_in_stack failed: {e}")))
        })
    }

    fn shutdown_shared_vm(&self, stack_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.runtime.shutdown_shared_vm(stack_id))
                .map_err(|e| StackError::Network(format!("shutdown_shared_vm failed: {e}")))
        })
    }

    fn has_shared_vm(&self, stack_id: &str) -> bool {
        tokio::task::block_in_place(|| {
            self.handle.block_on(self.runtime.has_shared_vm(stack_id))
        })
    }
}

// ── up ─────────────────────────────────────────────────────────────

async fn cmd_up(args: UpArgs) -> anyhow::Result<()> {
    let yaml = std::fs::read_to_string(&args.file)
        .with_context(|| format!("failed to read compose file: {}", args.file.display()))?;

    let compose_dir = args
        .file
        .canonicalize()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."));

    let stack_name = resolve_stack_name(args.name.as_deref(), &args.file)?;
    let spec = parse_compose_with_dir(&yaml, &stack_name, &compose_dir)
        .with_context(|| "failed to parse compose file")?;

    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &spec.name)?;
    std::fs::create_dir_all(&state_dir)
        .with_context(|| format!("failed to create state directory: {}", state_dir.display()))?;

    let db_path = state_dir.join("state.db");

    info!(
        stack = %spec.name,
        services = spec.services.len(),
        "applying stack"
    );

    if args.dry_run {
        let store = StateStore::open(&db_path)
            .with_context(|| format!("failed to open state store: {}", db_path.display()))?;
        let health_statuses = HashMap::new();
        let result = vz_stack::apply(&spec, &store, &health_statuses)
            .with_context(|| "stack apply failed")?;
        print_apply_result(&result);
        println!("\n--dry-run: skipping execution");
        return Ok(());
    }

    // Set up runtime, executor, and orchestrator.
    let oci_runtime =
        OciContainerRuntime::new(&state_dir).with_context(|| "failed to initialize OCI runtime")?;

    let exec_store =
        StateStore::open(&db_path).with_context(|| "failed to open execution state store")?;
    let reconcile_store =
        StateStore::open(&db_path).with_context(|| "failed to open reconciliation state store")?;

    let executor = StackExecutor::new(oci_runtime, exec_store, &state_dir);

    if args.detach {
        // Detach mode: single apply+execute pass, return immediately.
        let mut orchestrator = StackOrchestrator::new(
            executor,
            reconcile_store,
            OrchestrationConfig {
                max_rounds: 1,
                ..Default::default()
            },
        );

        let result = orchestrator
            .run(
                &spec,
                Some(&mut |report: &RoundReport| {
                    if let Some(ref apply) = report.apply_result.actions.first() {
                        let _ = apply; // suppress unused warning
                        print_apply_result(&report.apply_result);
                    }
                    if let Some(ref exec) = report.exec_result {
                        print_execution_result(exec);
                    }
                }),
            )
            .with_context(|| "stack up failed")?;

        println!(
            "\nDetached: {} ready, {} failed.",
            result.services_ready, result.services_failed
        );
    } else {
        // Foreground mode: full orchestration loop until convergence.
        let mut orchestrator =
            StackOrchestrator::new(executor, reconcile_store, OrchestrationConfig::default());

        let result = orchestrator
            .run(
                &spec,
                Some(&mut |report: &RoundReport| {
                    print_round_report(report);
                }),
            )
            .with_context(|| "stack orchestration failed")?;

        if result.converged {
            println!(
                "\nStack converged in {} round(s): {} ready, {} failed.",
                result.rounds, result.services_ready, result.services_failed
            );
        } else {
            println!(
                "\nStack did not converge after {} rounds: {} ready, {} failed.",
                result.rounds, result.services_ready, result.services_failed
            );
        }

        if result.services_failed > 0 {
            bail!("{} service(s) failed", result.services_failed);
        }

        if !result.converged {
            bail!("stack did not converge");
        }
    }

    Ok(())
}

// ── down ───────────────────────────────────────────────────────────

async fn cmd_down(args: DownArgs) -> anyhow::Result<()> {
    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &args.name)?;
    let db_path = state_dir.join("state.db");

    if !db_path.exists() {
        bail!("no state found for stack `{}`", args.name);
    }

    let store = StateStore::open(&db_path).with_context(|| "failed to open state store")?;

    // Load current desired state to get the stack name, then apply empty spec.
    let current = store
        .load_desired_state(&args.name)
        .with_context(|| "failed to load desired state")?;

    let stack_name = current
        .as_ref()
        .map(|s| s.name.clone())
        .unwrap_or_else(|| args.name.clone());

    // Apply an empty spec to trigger removal of all services.
    let empty_spec = StackSpec {
        name: stack_name.clone(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
    };

    info!(stack = %stack_name, "tearing down stack");

    let health_statuses = HashMap::new();
    let result = vz_stack::apply(&empty_spec, &store, &health_statuses)
        .with_context(|| "stack teardown failed")?;

    print_apply_result(&result);

    if result.actions.is_empty() {
        return Ok(());
    }

    if args.dry_run {
        println!("\n--dry-run: skipping execution");
        return Ok(());
    }

    // Execute removal actions through the OCI runtime.
    let oci_runtime =
        OciContainerRuntime::new(&state_dir).with_context(|| "failed to initialize OCI runtime")?;

    let exec_store =
        StateStore::open(&db_path).with_context(|| "failed to open execution state store")?;

    let mut executor = StackExecutor::new(oci_runtime, exec_store, &state_dir);
    let exec_result = executor
        .execute(&empty_spec, &result.actions)
        .with_context(|| "teardown execution failed")?;

    print_execution_result(&exec_result);

    Ok(())
}

// ── ps ─────────────────────────────────────────────────────────────

async fn cmd_ps(args: PsArgs) -> anyhow::Result<()> {
    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &args.name)?;
    let db_path = state_dir.join("state.db");

    if !db_path.exists() {
        bail!("no state found for stack `{}`", args.name);
    }

    let store = StateStore::open(&db_path).with_context(|| "failed to open state store")?;

    let observed = store
        .load_observed_state(&args.name)
        .with_context(|| "failed to load observed state")?;

    if args.json {
        let json = serde_json::to_string_pretty(&observed)
            .with_context(|| "failed to serialize observed state")?;
        println!("{json}");
    } else {
        print_ps_table(&observed);
    }

    Ok(())
}

// ── events ─────────────────────────────────────────────────────────

async fn cmd_events(args: EventsArgs) -> anyhow::Result<()> {
    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &args.name)?;
    let db_path = state_dir.join("state.db");

    if !db_path.exists() {
        bail!("no state found for stack `{}`", args.name);
    }

    let store = StateStore::open(&db_path).with_context(|| "failed to open state store")?;

    let records = if args.since > 0 {
        store
            .load_events_since(&args.name, args.since)
            .with_context(|| "failed to load events")?
    } else {
        store
            .load_event_records(&args.name)
            .with_context(|| "failed to load events")?
    };

    if args.json {
        for record in &records {
            let json = serde_json::to_string(&record.event)
                .with_context(|| "failed to serialize event")?;
            println!("{json}");
        }
    } else {
        print_events_table(&records);
    }

    Ok(())
}

// ── logs ──────────────────────────────────────────────────────────

async fn cmd_logs(args: LogsArgs) -> anyhow::Result<()> {
    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &args.name)?;
    let db_path = state_dir.join("state.db");

    if !db_path.exists() {
        bail!("no state found for stack `{}`", args.name);
    }

    let store = StateStore::open(&db_path).with_context(|| "failed to open state store")?;

    // Load initial events.
    let records = store
        .load_event_records(&args.name)
        .with_context(|| "failed to load events")?;

    // Filter by service if specified.
    let filtered: Vec<&EventRecord> = records
        .iter()
        .filter(|r| match &args.service {
            Some(svc) => event_service_name(&r.event).is_some_and(|n| n == svc),
            None => true,
        })
        .collect();

    // Apply tail: show only the last N events (0 = all).
    let display = if args.tail > 0 && filtered.len() > args.tail {
        &filtered[filtered.len() - args.tail..]
    } else {
        &filtered
    };

    for record in display {
        print_log_line(record);
    }

    if !args.follow {
        return Ok(());
    }

    // Follow mode: poll for new events using the cursor.
    let mut cursor = records.last().map_or(0, |r| r.id);

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let new_records = store
            .load_events_since(&args.name, cursor)
            .with_context(|| "failed to poll events")?;

        if new_records.is_empty() {
            continue;
        }

        for record in &new_records {
            if let Some(svc) = &args.service {
                if event_service_name(&record.event).is_none_or(|n| n != svc) {
                    continue;
                }
            }
            print_log_line(record);
        }

        cursor = new_records.last().map_or(cursor, |r| r.id);
    }
}

/// Print a single log line with timestamp, service, and event summary.
fn print_log_line(record: &EventRecord) {
    let service = event_service_name(&record.event).unwrap_or("-");
    let summary = format_event_summary(&record.event);
    println!("{} [{}] {}", record.created_at, service, summary);
}

/// Extract the service name from a stack event, if applicable.
fn event_service_name(event: &StackEvent) -> Option<&str> {
    match event {
        StackEvent::ServiceCreating { service_name, .. }
        | StackEvent::ServiceReady { service_name, .. }
        | StackEvent::ServiceStopping { service_name, .. }
        | StackEvent::ServiceStopped { service_name, .. }
        | StackEvent::ServiceFailed { service_name, .. }
        | StackEvent::PortConflict { service_name, .. }
        | StackEvent::HealthCheckPassed { service_name, .. }
        | StackEvent::HealthCheckFailed { service_name, .. }
        | StackEvent::DependencyBlocked { service_name, .. } => Some(service_name),
        StackEvent::StackApplyStarted { .. }
        | StackEvent::StackApplyCompleted { .. }
        | StackEvent::StackApplyFailed { .. }
        | StackEvent::VolumeCreated { .. }
        | StackEvent::StackDestroyed { .. } => None,
    }
}

// ── Helpers ────────────────────────────────────────────────────────

/// Resolve the stack name from explicit flag or parent directory of compose file.
fn resolve_stack_name(
    explicit: Option<&str>,
    compose_path: &std::path::Path,
) -> anyhow::Result<String> {
    if let Some(name) = explicit {
        return Ok(name.to_string());
    }

    // Use the parent directory name of the compose file.
    let parent = compose_path
        .canonicalize()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()));

    parent.ok_or_else(|| anyhow::anyhow!("cannot determine stack name; use --name"))
}

/// Resolve the state directory for a stack.
fn resolve_state_dir(
    explicit: Option<&std::path::Path>,
    stack_name: &str,
) -> anyhow::Result<PathBuf> {
    if let Some(dir) = explicit {
        return Ok(dir.to_path_buf());
    }

    // Default: ~/.vz/stacks/<stack_name>/
    let home = std::env::var("HOME").with_context(|| "HOME not set")?;
    Ok(PathBuf::from(home)
        .join(".vz")
        .join("stacks")
        .join(stack_name))
}

fn print_round_report(report: &RoundReport) {
    if !report.apply_result.actions.is_empty() {
        print_apply_result(&report.apply_result);
    }

    if let Some(ref exec) = report.exec_result {
        print_execution_result(exec);
    }

    if let Some(ref health) = report.health_result {
        for name in &health.newly_ready {
            println!("  health ok  {name}");
        }
        for name in &health.newly_failed {
            println!("  health fail  {name}");
        }
    }

    if report.services_pending > 0 {
        println!(
            "  [{}/{} ready, {} pending]",
            report.services_ready,
            report.services_ready + report.services_pending + report.services_failed,
            report.services_pending
        );
    }
}

fn print_apply_result(result: &ApplyResult) {
    if result.actions.is_empty() && result.deferred.is_empty() {
        println!("No changes needed.");
        return;
    }

    for action in &result.actions {
        let verb = match action {
            vz_stack::Action::ServiceCreate { .. } => "create",
            vz_stack::Action::ServiceRecreate { .. } => "recreate",
            vz_stack::Action::ServiceRemove { .. } => "remove",
        };
        println!("  {verb:>10}  {}", action.service_name());
    }

    for deferred in &result.deferred {
        println!(
            "  deferred  {} (waiting on: {})",
            deferred.service_name,
            deferred.waiting_on.join(", "),
        );
    }

    println!(
        "\n{} action(s), {} deferred",
        result.actions.len(),
        result.deferred.len(),
    );
}

fn print_execution_result(result: &ExecutionResult) {
    if result.all_succeeded() {
        println!("\nAll {} action(s) succeeded.", result.succeeded);
    } else {
        println!(
            "\n{} succeeded, {} failed.",
            result.succeeded, result.failed
        );
        for (service, error) in &result.errors {
            println!("  error: {service}: {error}");
        }
    }
}

fn print_ps_table(observed: &[ServiceObservedState]) {
    if observed.is_empty() {
        println!("No services found.");
        return;
    }

    // Header.
    println!("{:<20} {:<14} {:<40}", "SERVICE", "STATUS", "CONTAINER ID");
    println!("{}", "-".repeat(74));

    for svc in observed {
        let status = match svc.phase {
            ServicePhase::Pending => "pending".to_string(),
            ServicePhase::Creating => "creating".to_string(),
            ServicePhase::Running if svc.ready => "running (ready)".to_string(),
            ServicePhase::Running => "running".to_string(),
            ServicePhase::Stopping => "stopping".to_string(),
            ServicePhase::Stopped => "stopped".to_string(),
            ServicePhase::Failed => "failed".to_string(),
        };
        let cid = svc.container_id.as_deref().unwrap_or("-");
        println!("{:<20} {:<14} {:<40}", svc.service_name, status, cid);
    }
}

fn print_events_table(records: &[EventRecord]) {
    if records.is_empty() {
        println!("No events found.");
        return;
    }

    println!("{:>6}  {:<24} EVENT", "ID", "TIME");
    println!("{}", "-".repeat(72));

    for record in records {
        let summary = format_event_summary(&record.event);
        println!("{:>6}  {:<24} {}", record.id, record.created_at, summary);
    }
}

fn format_event_summary(event: &StackEvent) -> String {
    match event {
        StackEvent::StackApplyStarted {
            stack_name,
            services_count,
        } => format!("apply started: {stack_name} ({services_count} services)"),
        StackEvent::StackApplyCompleted {
            succeeded, failed, ..
        } => format!("apply completed: {succeeded} ok, {failed} failed"),
        StackEvent::StackApplyFailed { error, .. } => format!("apply failed: {error}"),
        StackEvent::ServiceCreating { service_name, .. } => {
            format!("creating: {service_name}")
        }
        StackEvent::ServiceReady {
            service_name,
            runtime_id,
            ..
        } => format!("ready: {service_name} ({runtime_id})"),
        StackEvent::ServiceStopping { service_name, .. } => {
            format!("stopping: {service_name}")
        }
        StackEvent::ServiceStopped {
            service_name,
            exit_code,
            ..
        } => format!("stopped: {service_name} (exit {exit_code})"),
        StackEvent::ServiceFailed {
            service_name,
            error,
            ..
        } => format!("failed: {service_name}: {error}"),
        StackEvent::PortConflict {
            service_name, port, ..
        } => format!("port conflict: {service_name} port {port}"),
        StackEvent::VolumeCreated { volume_name, .. } => {
            format!("volume created: {volume_name}")
        }
        StackEvent::StackDestroyed { stack_name } => {
            format!("destroyed: {stack_name}")
        }
        StackEvent::HealthCheckPassed { service_name, .. } => {
            format!("health ok: {service_name}")
        }
        StackEvent::HealthCheckFailed {
            service_name,
            attempt,
            error,
            ..
        } => format!("health fail: {service_name} (attempt {attempt}): {error}"),
        StackEvent::DependencyBlocked {
            service_name,
            waiting_on,
            ..
        } => format!(
            "blocked: {service_name} waiting on {}",
            waiting_on.join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn resolve_stack_name_explicit() {
        let name = resolve_stack_name(Some("myapp"), &PathBuf::from("compose.yaml")).unwrap();
        assert_eq!(name, "myapp");
    }

    #[test]
    fn format_event_summary_covers_all_variants() {
        let events = vec![
            StackEvent::StackApplyStarted {
                stack_name: "s".into(),
                services_count: 2,
            },
            StackEvent::StackApplyCompleted {
                stack_name: "s".into(),
                succeeded: 1,
                failed: 0,
            },
            StackEvent::StackApplyFailed {
                stack_name: "s".into(),
                error: "e".into(),
            },
            StackEvent::ServiceCreating {
                stack_name: "s".into(),
                service_name: "web".into(),
            },
            StackEvent::ServiceReady {
                stack_name: "s".into(),
                service_name: "web".into(),
                runtime_id: "ctr-1".into(),
            },
            StackEvent::ServiceStopping {
                stack_name: "s".into(),
                service_name: "web".into(),
            },
            StackEvent::ServiceStopped {
                stack_name: "s".into(),
                service_name: "web".into(),
                exit_code: 0,
            },
            StackEvent::ServiceFailed {
                stack_name: "s".into(),
                service_name: "web".into(),
                error: "oom".into(),
            },
            StackEvent::PortConflict {
                stack_name: "s".into(),
                service_name: "web".into(),
                port: 80,
            },
            StackEvent::VolumeCreated {
                stack_name: "s".into(),
                volume_name: "v".into(),
            },
            StackEvent::StackDestroyed {
                stack_name: "s".into(),
            },
            StackEvent::HealthCheckPassed {
                stack_name: "s".into(),
                service_name: "web".into(),
            },
            StackEvent::HealthCheckFailed {
                stack_name: "s".into(),
                service_name: "web".into(),
                attempt: 3,
                error: "timeout".into(),
            },
            StackEvent::DependencyBlocked {
                stack_name: "s".into(),
                service_name: "web".into(),
                waiting_on: vec!["db".into()],
            },
        ];

        for event in events {
            let summary = format_event_summary(&event);
            assert!(!summary.is_empty(), "empty summary for {event:?}");
        }
    }

    #[test]
    fn print_ps_table_empty() {
        // Just verify it doesn't panic.
        print_ps_table(&[]);
    }

    #[test]
    fn print_ps_table_with_services() {
        let observed = vec![
            ServiceObservedState {
                service_name: "web".into(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-abc".into()),
                last_error: None,
                ready: true,
            },
            ServiceObservedState {
                service_name: "db".into(),
                phase: ServicePhase::Pending,
                container_id: None,
                last_error: None,
                ready: false,
            },
        ];
        // Just verify it doesn't panic.
        print_ps_table(&observed);
    }

    #[test]
    fn print_events_table_empty() {
        print_events_table(&[]);
    }

    #[test]
    fn print_apply_result_empty() {
        let result = ApplyResult {
            actions: vec![],
            deferred: vec![],
        };
        print_apply_result(&result);
    }

    #[test]
    fn event_service_name_returns_name_for_service_events() {
        let event = StackEvent::ServiceCreating {
            stack_name: "s".into(),
            service_name: "web".into(),
        };
        assert_eq!(event_service_name(&event), Some("web"));

        let event = StackEvent::HealthCheckFailed {
            stack_name: "s".into(),
            service_name: "db".into(),
            attempt: 1,
            error: "timeout".into(),
        };
        assert_eq!(event_service_name(&event), Some("db"));
    }

    #[test]
    fn event_service_name_returns_none_for_stack_events() {
        let event = StackEvent::StackApplyStarted {
            stack_name: "s".into(),
            services_count: 2,
        };
        assert_eq!(event_service_name(&event), None);

        let event = StackEvent::VolumeCreated {
            stack_name: "s".into(),
            volume_name: "v".into(),
        };
        assert_eq!(event_service_name(&event), None);

        let event = StackEvent::StackDestroyed {
            stack_name: "s".into(),
        };
        assert_eq!(event_service_name(&event), None);
    }

    #[test]
    fn print_apply_result_with_actions() {
        let result = ApplyResult {
            actions: vec![
                vz_stack::Action::ServiceCreate {
                    service_name: "web".into(),
                },
                vz_stack::Action::ServiceRemove {
                    service_name: "old".into(),
                },
            ],
            deferred: vec![vz_stack::DeferredService {
                service_name: "app".into(),
                waiting_on: vec!["db".into()],
            }],
        };
        print_apply_result(&result);
    }
}
