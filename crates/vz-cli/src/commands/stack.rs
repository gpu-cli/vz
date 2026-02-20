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
    ApplyResult, ContainerRuntime, EventRecord, ExecutionResult, ServiceObservedState,
    ServicePhase, StackError, StackEvent, StackExecutor, StackSpec, StateStore, parse_compose,
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

pub async fn run(args: StackArgs) -> anyhow::Result<()> {
    match args.action {
        StackCommand::Up(args) => cmd_up(args).await,
        StackCommand::Down(args) => cmd_down(args).await,
        StackCommand::Ps(args) => cmd_ps(args).await,
        StackCommand::Events(args) => cmd_events(args).await,
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
}

// ── up ─────────────────────────────────────────────────────────────

async fn cmd_up(args: UpArgs) -> anyhow::Result<()> {
    let yaml = std::fs::read_to_string(&args.file)
        .with_context(|| format!("failed to read compose file: {}", args.file.display()))?;

    let stack_name = resolve_stack_name(args.name.as_deref(), &args.file)?;
    let spec = parse_compose(&yaml, &stack_name)
        .with_context(|| "failed to parse compose file")?;

    let state_dir = resolve_state_dir(args.state_dir.as_deref(), &spec.name)?;
    std::fs::create_dir_all(&state_dir)
        .with_context(|| format!("failed to create state directory: {}", state_dir.display()))?;

    let db_path = state_dir.join("state.db");
    let store = StateStore::open(&db_path)
        .with_context(|| format!("failed to open state store: {}", db_path.display()))?;

    info!(
        stack = %spec.name,
        services = spec.services.len(),
        "applying stack"
    );

    let health_statuses = HashMap::new();
    let result = vz_stack::apply(&spec, &store, &health_statuses)
        .with_context(|| "stack apply failed")?;

    print_apply_result(&result);

    if result.actions.is_empty() {
        return Ok(());
    }

    if args.dry_run {
        println!("\n--dry-run: skipping execution");
        return Ok(());
    }

    // Execute actions through the OCI runtime.
    let oci_runtime = OciContainerRuntime::new(&state_dir)
        .with_context(|| "failed to initialize OCI runtime")?;

    let exec_store = StateStore::open(&db_path)
        .with_context(|| "failed to open execution state store")?;

    let mut executor = StackExecutor::new(oci_runtime, exec_store, &state_dir);
    let exec_result = executor
        .execute(&spec, &result.actions)
        .with_context(|| "execution failed")?;

    print_execution_result(&exec_result);

    if !exec_result.all_succeeded() {
        bail!(
            "{} action(s) failed",
            exec_result.failed,
        );
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

    let store = StateStore::open(&db_path)
        .with_context(|| "failed to open state store")?;

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
    let oci_runtime = OciContainerRuntime::new(&state_dir)
        .with_context(|| "failed to initialize OCI runtime")?;

    let exec_store = StateStore::open(&db_path)
        .with_context(|| "failed to open execution state store")?;

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

    let store = StateStore::open(&db_path)
        .with_context(|| "failed to open state store")?;

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

    let store = StateStore::open(&db_path)
        .with_context(|| "failed to open state store")?;

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

// ── Helpers ────────────────────────────────────────────────────────

/// Resolve the stack name from explicit flag or parent directory of compose file.
fn resolve_stack_name(explicit: Option<&str>, compose_path: &std::path::Path) -> anyhow::Result<String> {
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
fn resolve_state_dir(explicit: Option<&std::path::Path>, stack_name: &str) -> anyhow::Result<PathBuf> {
    if let Some(dir) = explicit {
        return Ok(dir.to_path_buf());
    }

    // Default: ~/.vz/stacks/<stack_name>/
    let home = std::env::var("HOME").with_context(|| "HOME not set")?;
    Ok(PathBuf::from(home).join(".vz").join("stacks").join(stack_name))
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
    println!(
        "{:<20} {:<14} {:<40}",
        "SERVICE", "STATUS", "CONTAINER ID"
    );
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

    println!(
        "{:>6}  {:<24} EVENT",
        "ID", "TIME"
    );
    println!("{}", "-".repeat(72));

    for record in records {
        let summary = format_event_summary(&record.event);
        println!(
            "{:>6}  {:<24} {}",
            record.id, record.created_at, summary
        );
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
            service_name,
            port,
            ..
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
        let name =
            resolve_stack_name(Some("myapp"), &PathBuf::from("compose.yaml")).unwrap();
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
