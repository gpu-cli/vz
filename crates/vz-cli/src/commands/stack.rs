//! `vz stack` — multi-service stack lifecycle commands.
//!
//! Runtime-mutating stack operations are daemon-owned.
//! Command paths without daemon parity fail closed.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use serde::Serialize;
use tracing::debug;

use vz_runtime_proto::runtime_v2;
use vz_stack::{
    EventRecord, ServiceObservedState, ServicePhase, StackEvent, StackSpec, parse_compose_with_dir,
};

use super::runtime_daemon::{connect_control_plane_for_state_db, default_state_db_path};

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

    /// List all known stacks.
    Ls(LsArgs),

    /// Validate and print the resolved compose configuration.
    Config(ConfigArgs),

    /// Show stack lifecycle events.
    Events(EventsArgs),

    /// Show service logs (event history and container output).
    Logs(LogsArgs),

    /// Execute a command in a running service container.
    Exec(ExecArgs),

    /// Run a one-off command in a service container.
    Run(RunArgs),

    /// Stop an individual service in a running stack.
    Stop(ServiceArgs),

    /// Start (recreate) an individual service in a running stack.
    Start(ServiceArgs),

    /// Restart an individual service in a running stack.
    Restart(ServiceArgs),

    /// Open TUI dashboard for a running stack.
    Dashboard(DashboardArgs),
}

#[derive(Args, Debug)]
pub struct UpArgs {
    /// Path to compose YAML file.
    ///
    /// When omitted, auto-discovers the first existing file from:
    /// `compose.yaml`, `compose.yml`, `docker-compose.yml`, `docker-compose.yaml`.
    #[arg(short, long)]
    pub file: Option<PathBuf>,

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

    /// Disable TUI dashboard (use plain text output).
    #[arg(long)]
    pub no_tui: bool,

    #[command(flatten)]
    pub auth: StackRegistryAuthOpts,
}

/// Registry authentication options for image pulls during stack startup.
#[derive(Args, Debug, Clone, Default)]
pub struct StackRegistryAuthOpts {
    /// Use credentials from local Docker credential configuration.
    #[arg(long, conflicts_with_all = ["username", "password"])]
    pub docker_config: bool,

    /// Registry username when using basic auth.
    #[arg(long, requires = "password", conflicts_with = "docker_config")]
    pub username: Option<String>,

    /// Registry password when using basic auth.
    #[arg(long, requires = "username", conflicts_with = "docker_config")]
    pub password: Option<String>,
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

    /// Remove named volumes declared in the compose file.
    #[arg(long)]
    pub volumes: bool,
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

#[derive(Args, Debug)]
pub struct ExecArgs {
    /// Stack name.
    pub name: String,

    /// Service to execute the command in.
    pub service: String,

    /// Command and arguments to execute.
    #[arg(last = true, required = true)]
    pub command: Vec<String>,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct ServiceArgs {
    /// Stack name.
    pub name: String,

    /// Service to act on.
    pub service: String,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct LsArgs {
    /// Output as JSON.
    #[arg(long)]
    pub json: bool,

    /// State directory root (overrides default ~/.vz/stacks/).
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct ConfigArgs {
    /// Path to compose YAML file (auto-discovers if omitted).
    #[arg(short, long)]
    pub file: Option<PathBuf>,

    /// Stack name (defaults to parent directory name).
    #[arg(short = 'n', long)]
    pub name: Option<String>,

    /// Only validate, don't print.
    #[arg(long)]
    pub quiet: bool,
}

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Stack name.
    pub name: String,

    /// Service to run the command in.
    pub service: String,

    /// Command and arguments to execute.
    #[arg(last = true, required = true)]
    pub command: Vec<String>,

    /// Path to compose YAML file (auto-discovers if omitted).
    #[arg(short, long)]
    pub file: Option<PathBuf>,

    /// State directory for stack persistence.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// Remove the container after the command exits.
    #[arg(long, default_value = "true")]
    pub rm: bool,
}

/// Arguments for the `vz stack dashboard` subcommand.
#[derive(Args, Debug)]
pub struct DashboardArgs {
    /// Stack name.
    pub name: String,

    /// Path to compose file (for service metadata).
    #[arg(short, long)]
    pub file: Option<PathBuf>,

    /// State directory.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
}

/// Action types for control socket requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlAction {
    Stop,
    Start,
    Restart,
}

pub async fn run(args: StackArgs) -> anyhow::Result<()> {
    match args.action {
        StackCommand::Up(args) => cmd_up(args).await,
        StackCommand::Down(args) => cmd_down(args).await,
        StackCommand::Ps(args) => cmd_ps(args).await,
        StackCommand::Ls(args) => cmd_ls(args).await,
        StackCommand::Config(args) => cmd_config(args).await,
        StackCommand::Events(args) => cmd_events(args).await,
        StackCommand::Logs(args) => cmd_logs(args).await,
        StackCommand::Exec(args) => cmd_exec(args).await,
        StackCommand::Run(args) => cmd_run(args).await,
        StackCommand::Stop(args) => cmd_service_action(args, ControlAction::Stop).await,
        StackCommand::Start(args) => cmd_service_action(args, ControlAction::Start).await,
        StackCommand::Restart(args) => cmd_service_action(args, ControlAction::Restart).await,
        StackCommand::Dashboard(args) => cmd_dashboard(args).await,
    }
}

fn stack_state_db_path(explicit_state_dir: Option<&Path>) -> PathBuf {
    explicit_state_dir
        .map(|state_dir| state_dir.join("state.db"))
        .unwrap_or_else(default_state_db_path)
}

fn service_phase_from_stack_status(phase: &str) -> ServicePhase {
    match phase.trim().to_ascii_lowercase().as_str() {
        "pending" => ServicePhase::Pending,
        "creating" => ServicePhase::Creating,
        "running" => ServicePhase::Running,
        "stopping" => ServicePhase::Stopping,
        "stopped" => ServicePhase::Stopped,
        "failed" => ServicePhase::Failed,
        _ => ServicePhase::Pending,
    }
}

fn observed_from_stack_statuses(
    services: &[runtime_v2::StackServiceStatus],
) -> Vec<ServiceObservedState> {
    services
        .iter()
        .map(|service| ServiceObservedState {
            service_name: service.service_name.clone(),
            phase: service_phase_from_stack_status(&service.phase),
            container_id: if service.container_id.trim().is_empty() {
                None
            } else {
                Some(service.container_id.clone())
            },
            last_error: if service.last_error.trim().is_empty() {
                None
            } else {
                Some(service.last_error.clone())
            },
            ready: service.ready,
        })
        .collect()
}

fn resolve_service_container_id(
    stack_name: &str,
    service_name: &str,
    services: &[runtime_v2::StackServiceStatus],
) -> anyhow::Result<String> {
    let service = services
        .iter()
        .find(|service| service.service_name == service_name)
        .ok_or_else(|| {
            anyhow::anyhow!("service `{service_name}` not found in stack `{stack_name}`")
        })?;
    let container_id = service.container_id.trim();
    if container_id.is_empty() {
        let phase = if service.phase.trim().is_empty() {
            "unknown"
        } else {
            service.phase.as_str()
        };
        bail!("service `{service_name}` in stack `{stack_name}` is not running (phase: {phase})");
    }
    Ok(container_id.to_string())
}

fn split_exec_command(command: &[String]) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let Some((head, tail)) = command.split_first() else {
        bail!("command cannot be empty");
    };
    Ok((vec![head.clone()], tail.to_vec()))
}

async fn execute_stack_container_command(
    client: &mut vz_runtimed_client::DaemonClient,
    container_id: String,
    command: &[String],
) -> anyhow::Result<()> {
    let (cmd, cmd_args) = split_exec_command(command)?;
    let execution_response = client
        .create_execution(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id,
            cmd,
            args: cmd_args,
            env_override: HashMap::new(),
            timeout_secs: 0,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Inherit as i32,
        })
        .await
        .context("failed to create execution")?;
    let execution = execution_response
        .execution
        .ok_or_else(|| anyhow::anyhow!("daemon returned missing execution payload"))?;
    let execution_id = execution.execution_id.clone();

    let mut stream = client
        .stream_exec_output(runtime_v2::StreamExecOutputRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        })
        .await
        .with_context(|| format!("failed to stream output for execution `{execution_id}`"))?;

    let mut terminal_exit_code: Option<i32> = None;
    while let Some(event) = stream
        .message()
        .await
        .with_context(|| format!("failed reading output stream for `{execution_id}`"))?
    {
        match event.payload {
            Some(runtime_v2::exec_output_event::Payload::Stdout(chunk)) => {
                if !chunk.is_empty() {
                    let mut stdout = std::io::stdout().lock();
                    stdout
                        .write_all(&chunk)
                        .context("failed writing execution stdout")?;
                    stdout.flush().context("failed flushing execution stdout")?;
                }
            }
            Some(runtime_v2::exec_output_event::Payload::Stderr(chunk)) => {
                if !chunk.is_empty() {
                    let mut stderr = std::io::stderr().lock();
                    stderr
                        .write_all(&chunk)
                        .context("failed writing execution stderr")?;
                    stderr.flush().context("failed flushing execution stderr")?;
                }
            }
            Some(runtime_v2::exec_output_event::Payload::ExitCode(code)) => {
                terminal_exit_code = Some(code);
                break;
            }
            Some(runtime_v2::exec_output_event::Payload::Error(message)) => {
                bail!("execution `{execution_id}` reported error: {message}");
            }
            None => {}
        }
    }

    let exit_code = match terminal_exit_code {
        Some(code) => code,
        None => {
            let execution = client
                .get_execution(runtime_v2::GetExecutionRequest {
                    execution_id: execution_id.clone(),
                    metadata: None,
                })
                .await
                .with_context(|| {
                    format!("failed to load terminal execution state for `{execution_id}`")
                })?
                .execution
                .ok_or_else(|| anyhow::anyhow!("daemon returned missing execution payload"))?;
            if execution.state.eq_ignore_ascii_case("failed") {
                bail!("execution `{execution_id}` ended in failed state");
            }
            execution.exit_code
        }
    };

    if exit_code != 0 {
        bail!("stack command exited with status {exit_code}");
    }

    Ok(())
}

fn stack_status_from_sandbox_states(
    states: &[String],
    ready_count: usize,
    total_count: usize,
) -> (String, Option<String>) {
    if total_count == 0 {
        return ("\u{25cb} stopped".to_string(), None);
    }

    let failed = states
        .iter()
        .any(|state| state.eq_ignore_ascii_case("failed"));
    let ready = states
        .iter()
        .filter(|state| state.eq_ignore_ascii_case("ready"))
        .count();
    let creating_or_draining = states.iter().any(|state| {
        state.eq_ignore_ascii_case("creating") || state.eq_ignore_ascii_case("draining")
    });
    let terminated = states
        .iter()
        .filter(|state| state.eq_ignore_ascii_case("terminated"))
        .count();

    if failed {
        return (
            "\u{2717} failed".to_string(),
            Some("one or more sandboxes are failed".to_string()),
        );
    }

    if terminated == total_count {
        return ("\u{25cb} stopped".to_string(), None);
    }

    if ready == total_count && ready_count == total_count {
        return ("\u{2713} running".to_string(), None);
    }

    if creating_or_draining {
        return ("\u{25d0} starting".to_string(), None);
    }

    ("\u{25d0} partial".to_string(), None)
}
// ── up ─────────────────────────────────────────────────────────────

fn resolve_stack_registry_auth(
    opts: &StackRegistryAuthOpts,
) -> anyhow::Result<Option<vz_image::Auth>> {
    if opts.username.is_some() && opts.password.is_none() {
        bail!("--username requires --password");
    }
    if opts.password.is_some() && opts.username.is_none() {
        bail!("--password requires --username");
    }

    let auth = match (&opts.docker_config, &opts.username, &opts.password) {
        (true, _, _) => Some(vz_image::Auth::DockerConfig),
        (false, Some(username), Some(password)) => Some(vz_image::Auth::Basic {
            username: username.clone(),
            password: password.clone(),
        }),
        _ => None,
    };

    Ok(auth)
}

async fn cmd_up(args: UpArgs) -> anyhow::Result<()> {
    let registry_auth = resolve_stack_registry_auth(&args.auth)?;
    if registry_auth.is_some() {
        bail!("registry auth flags are not supported for daemon stack apply yet");
    }
    if !args.no_tui {
        debug!("daemon stack mode ignores TUI control socket flow");
    }

    let file = resolve_compose_file(args.file)?;
    let yaml = std::fs::read_to_string(&file)
        .with_context(|| format!("failed to read compose file: {}", file.display()))?;

    let compose_dir = file
        .canonicalize()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."));

    let stack_name = resolve_stack_name(args.name.as_deref(), &file)?;
    let state_db = stack_state_db_path(args.state_dir.as_deref());
    let mut client = connect_control_plane_for_state_db(&state_db).await?;
    let mut stream = client
        .apply_stack_stream(runtime_v2::ApplyStackRequest {
            metadata: None,
            stack_name: stack_name.clone(),
            compose_yaml: yaml,
            compose_dir: compose_dir.to_string_lossy().to_string(),
            dry_run: args.dry_run,
            detach: args.detach,
        })
        .await
        .with_context(|| format!("failed to apply stack `{stack_name}` via daemon"))?;
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .with_context(|| format!("failed to read apply stack stream for `{stack_name}`"))?
    {
        match event.payload {
            Some(runtime_v2::apply_stack_event::Payload::Progress(progress)) => {
                println!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::apply_stack_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            None => {}
        }
    }
    let response = completion
        .ok_or_else(|| anyhow::anyhow!("daemon apply_stack stream ended without completion"))?
        .response
        .ok_or_else(|| anyhow::anyhow!("daemon apply_stack completion missing response payload"))?;

    let observed = observed_from_stack_statuses(&response.services);

    if args.dry_run {
        println!(
            "Plan for stack `{}`: {} action(s) would change.",
            response.stack_name, response.changed_actions
        );
        if !observed.is_empty() {
            print_ps_table(&observed, None);
        }
        println!("\n--dry-run: skipping execution");
        return Ok(());
    }

    if observed.is_empty() && response.changed_actions == 0 {
        println!("No changes needed.");
    } else {
        print_ps_table(&observed, None);
        println!();
        println!(
            "Applied stack `{}` with {} changed action(s).",
            response.stack_name, response.changed_actions
        );
    }

    if response.services_failed > 0 {
        bail!("{} service(s) failed", response.services_failed);
    }
    if !args.detach && !response.converged {
        bail!("stack did not converge");
    }

    Ok(())
}

// ── exec ──────────────────────────────────────────────────────────

/// Connect to a running `vz stack up` session and execute a command.
async fn cmd_exec(args: ExecArgs) -> anyhow::Result<()> {
    let state_db = stack_state_db_path(args.state_dir.as_deref());
    let mut client = connect_control_plane_for_state_db(&state_db).await?;
    let status = client
        .get_stack_status(runtime_v2::GetStackStatusRequest {
            metadata: None,
            stack_name: args.name.clone(),
        })
        .await
        .with_context(|| format!("failed to load stack status for `{}` via daemon", args.name))?;
    let container_id = resolve_service_container_id(&args.name, &args.service, &status.services)?;
    execute_stack_container_command(&mut client, container_id, &args.command)
        .await
        .with_context(|| {
            format!(
                "failed to execute command for stack `{}` service `{}`",
                args.name, args.service
            )
        })
}

// ── service start/stop/restart ─────────────────────────────────────

/// Send a daemon-backed service-level action (stop/start/restart).
async fn cmd_service_action(args: ServiceArgs, action: ControlAction) -> anyhow::Result<()> {
    let state_db = stack_state_db_path(args.state_dir.as_deref());
    let mut client = connect_control_plane_for_state_db(&state_db).await?;
    let request = runtime_v2::StackServiceActionRequest {
        metadata: None,
        stack_name: args.name.clone(),
        service_name: args.service.clone(),
    };
    let response = match action {
        ControlAction::Stop => client.stop_stack_service(request).await.with_context(|| {
            format!(
                "failed to stop service `{}` in stack `{}` via daemon",
                args.service, args.name
            )
        })?,
        ControlAction::Start => client.start_stack_service(request).await.with_context(|| {
            format!(
                "failed to start service `{}` in stack `{}` via daemon",
                args.service, args.name
            )
        })?,
        ControlAction::Restart => {
            client
                .restart_stack_service(request)
                .await
                .with_context(|| {
                    format!(
                        "failed to restart service `{}` in stack `{}` via daemon",
                        args.service, args.name
                    )
                })?
        }
    };

    let service = response
        .service
        .ok_or_else(|| anyhow::anyhow!("daemon returned missing stack service payload"))?;
    let phase = if service.phase.trim().is_empty() {
        "unknown"
    } else {
        service.phase.as_str()
    };
    println!(
        "Service `{}` in stack `{}` now reports phase `{}`.",
        service.service_name, response.stack_name, phase
    );
    if phase.eq_ignore_ascii_case("failed") {
        if service.last_error.trim().is_empty() {
            bail!(
                "service `{}` in stack `{}` entered failed state",
                service.service_name,
                response.stack_name
            );
        }
        bail!(
            "service `{}` in stack `{}` entered failed state: {}",
            service.service_name,
            response.stack_name,
            service.last_error
        );
    }

    Ok(())
}

// ── down ───────────────────────────────────────────────────────────

async fn cmd_down(args: DownArgs) -> anyhow::Result<()> {
    let state_db = stack_state_db_path(args.state_dir.as_deref());
    let mut client = connect_control_plane_for_state_db(&state_db).await?;
    let mut stream = client
        .teardown_stack_stream(runtime_v2::TeardownStackRequest {
            metadata: None,
            stack_name: args.name.clone(),
            dry_run: args.dry_run,
            remove_volumes: args.volumes,
        })
        .await
        .with_context(|| format!("failed to teardown stack `{}` via daemon", args.name))?;
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .with_context(|| format!("failed to read teardown stack stream for `{}`", args.name))?
    {
        match event.payload {
            Some(runtime_v2::teardown_stack_event::Payload::Progress(progress)) => {
                println!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::teardown_stack_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            None => {}
        }
    }
    let response = completion
        .ok_or_else(|| anyhow::anyhow!("daemon teardown_stack stream ended without completion"))?
        .response
        .ok_or_else(|| {
            anyhow::anyhow!("daemon teardown_stack completion missing response payload")
        })?;

    if args.dry_run {
        println!(
            "Plan for stack `{}`: {} action(s) would change.",
            response.stack_name, response.changed_actions
        );
        println!("\n--dry-run: skipping execution");
        return Ok(());
    }

    if response.changed_actions == 0 && response.removed_volumes == 0 {
        println!("No changes needed.");
        return Ok(());
    }

    println!(
        "Teardown complete for stack `{}` ({} changed action(s)).",
        response.stack_name, response.changed_actions
    );
    if args.volumes {
        if response.removed_volumes == 0 {
            println!("No volumes to remove.");
        } else {
            println!("Removed {} volume(s).", response.removed_volumes);
        }
    }

    Ok(())
}

// ── ps ─────────────────────────────────────────────────────────────

async fn cmd_ps(args: PsArgs) -> anyhow::Result<()> {
    let state_db = stack_state_db_path(args.state_dir.as_deref());
    let mut client = connect_control_plane_for_state_db(&state_db).await?;
    let response = client
        .get_stack_status(runtime_v2::GetStackStatusRequest {
            metadata: None,
            stack_name: args.name.clone(),
        })
        .await
        .with_context(|| format!("failed to get stack status for `{}` via daemon", args.name))?;

    let observed = observed_from_stack_statuses(&response.services);
    if args.json {
        let json = serde_json::to_string_pretty(&observed)
            .with_context(|| "failed to serialize observed state")?;
        println!("{json}");
    } else {
        print_ps_table(&observed, None);
    }

    Ok(())
}

// ── events ─────────────────────────────────────────────────────────

async fn cmd_events(args: EventsArgs) -> anyhow::Result<()> {
    let state_db = stack_state_db_path(args.state_dir.as_deref());
    let mut client = connect_control_plane_for_state_db(&state_db).await?;

    let mut cursor = args.since.max(0);
    let mut events = Vec::new();
    loop {
        let response = client
            .list_stack_events(runtime_v2::ListStackEventsRequest {
                metadata: None,
                stack_name: args.name.clone(),
                after: cursor,
                limit: 1000,
            })
            .await
            .with_context(|| {
                format!("failed to list stack events for `{}` via daemon", args.name)
            })?;
        if response.events.is_empty() {
            break;
        }
        events.extend(response.events);
        if response.next_cursor <= cursor {
            break;
        }
        cursor = response.next_cursor;
    }

    if args.json {
        for event in &events {
            println!("{}", event.event_json);
        }
        return Ok(());
    }

    let mut records = Vec::with_capacity(events.len());
    for event in events {
        let parsed: StackEvent = serde_json::from_str(&event.event_json)
            .with_context(|| format!("failed to parse stack event payload {}", event.id))?;
        records.push(EventRecord {
            id: event.id,
            stack_name: event.stack_name,
            created_at: event.created_at,
            event: parsed,
        });
    }
    print_events_table(&records);
    Ok(())
}

// ── logs ──────────────────────────────────────────────────────────

async fn cmd_logs(args: LogsArgs) -> anyhow::Result<()> {
    let state_db = stack_state_db_path(args.state_dir.as_deref());
    let mut client = connect_control_plane_for_state_db(&state_db).await?;
    let service_filter = args.service.unwrap_or_default();
    let tail_limit = u32::try_from(args.tail).unwrap_or(u32::MAX);

    let mut previous_outputs: HashMap<String, String> = HashMap::new();
    let mut first_iteration = true;

    loop {
        let response = client
            .get_stack_logs(runtime_v2::GetStackLogsRequest {
                metadata: None,
                stack_name: args.name.clone(),
                service: service_filter.clone(),
                tail: if first_iteration { tail_limit } else { 0 },
            })
            .await
            .with_context(|| format!("failed to get stack logs for `{}` via daemon", args.name))?;

        if response.logs.is_empty() {
            bail!("no running services in stack `{}`", args.name);
        }

        let multi = response.logs.len() > 1 || service_filter.is_empty();
        for log in response.logs {
            let previous = previous_outputs
                .get(&log.service_name)
                .map(String::as_str)
                .unwrap_or_default();
            let delta = log
                .output
                .strip_prefix(previous)
                .map(str::to_string)
                .unwrap_or_else(|| log.output.clone());
            print_log_output(&delta, &log.service_name, multi);
            previous_outputs.insert(log.service_name, log.output);
        }

        if !args.follow {
            return Ok(());
        }

        first_iteration = false;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
    }
}

/// Print log output, prefixing each line with `[service]` for multi-service stacks.
fn print_log_output(output: &str, service: &str, multi: bool) {
    if output.is_empty() {
        return;
    }
    if multi {
        for line in output.lines() {
            println!("[{service}] {line}");
        }
    } else {
        print!("{output}");
    }
}

/// Extract the service name from a stack event, if applicable.
#[cfg(test)]
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
        | StackEvent::DependencyBlocked { service_name, .. }
        | StackEvent::MountTopologyRecreateRequired { service_name, .. } => Some(service_name),
        StackEvent::StackApplyStarted { .. }
        | StackEvent::StackApplyCompleted { .. }
        | StackEvent::StackApplyFailed { .. }
        | StackEvent::VolumeCreated { .. }
        | StackEvent::StackDestroyed { .. }
        | StackEvent::SandboxCreating { .. }
        | StackEvent::SandboxReady { .. }
        | StackEvent::SandboxDraining { .. }
        | StackEvent::SandboxTerminated { .. }
        | StackEvent::SandboxFailed { .. }
        | StackEvent::LeaseOpened { .. }
        | StackEvent::LeaseHeartbeat { .. }
        | StackEvent::LeaseExpired { .. }
        | StackEvent::LeaseClosed { .. }
        | StackEvent::LeaseFailed { .. }
        | StackEvent::ExecutionQueued { .. }
        | StackEvent::ExecutionRunning { .. }
        | StackEvent::ExecutionExited { .. }
        | StackEvent::ExecutionFailed { .. }
        | StackEvent::ExecutionCanceled { .. }
        | StackEvent::ExecutionResized { .. }
        | StackEvent::ExecutionSignaled { .. }
        | StackEvent::CheckpointCreating { .. }
        | StackEvent::CheckpointReady { .. }
        | StackEvent::CheckpointFailed { .. }
        | StackEvent::CheckpointRestored { .. }
        | StackEvent::CheckpointForked { .. }
        | StackEvent::BuildQueued { .. }
        | StackEvent::BuildRunning { .. }
        | StackEvent::BuildSucceeded { .. }
        | StackEvent::BuildFailed { .. }
        | StackEvent::BuildCanceled { .. }
        | StackEvent::ContainerCreated { .. }
        | StackEvent::ContainerStarting { .. }
        | StackEvent::ContainerRunning { .. }
        | StackEvent::ContainerStopping { .. }
        | StackEvent::ContainerExited { .. }
        | StackEvent::ContainerFailed { .. }
        | StackEvent::ContainerRemoved { .. }
        | StackEvent::DriftDetected { .. }
        | StackEvent::OrphanCleaned { .. } => None,
    }
}

// ── ls ─────────────────────────────────────────────────────────────

/// Stack entry for the `ls` listing.
#[derive(Debug, Serialize)]
struct StackListEntry {
    name: String,
    status: String,
    ready: usize,
    total: usize,
    error_summary: Option<String>,
}

async fn cmd_ls(args: LsArgs) -> anyhow::Result<()> {
    if args.state_dir.is_some() {
        bail!(
            "`vz stack ls --state-dir` is not supported in daemon mode; use the daemon default state db"
        );
    }

    #[derive(Default)]
    struct StackAggregate {
        states: Vec<String>,
        ready: usize,
        total: usize,
    }

    let state_db = default_state_db_path();
    let mut client = connect_control_plane_for_state_db(&state_db).await?;
    let response = client
        .list_sandboxes(runtime_v2::ListSandboxesRequest { metadata: None })
        .await
        .with_context(|| "failed to list sandboxes via daemon for stack listing")?;

    let mut grouped: HashMap<String, StackAggregate> = HashMap::new();
    for sandbox in response.sandboxes {
        let stack_name = sandbox
            .labels
            .get("stack_name")
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| sandbox.sandbox_id.clone());
        let aggregate = grouped.entry(stack_name).or_default();
        aggregate.total += 1;
        if sandbox.state.eq_ignore_ascii_case("ready") {
            aggregate.ready += 1;
        }
        aggregate.states.push(sandbox.state);
    }

    let mut entries: Vec<StackListEntry> = grouped
        .into_iter()
        .map(|(name, aggregate)| {
            let (status, error_summary) = stack_status_from_sandbox_states(
                &aggregate.states,
                aggregate.ready,
                aggregate.total,
            );
            StackListEntry {
                name,
                status,
                ready: aggregate.ready,
                total: aggregate.total,
                error_summary,
            }
        })
        .collect();
    entries.sort_by(|a, b| a.name.cmp(&b.name));

    if args.json {
        let json = serde_json::to_string_pretty(&entries)
            .with_context(|| "failed to serialize stack list")?;
        println!("{json}");
        return Ok(());
    }

    if entries.is_empty() {
        println!("No stacks found.");
        return Ok(());
    }

    let name_width = entries
        .iter()
        .map(|entry| entry.name.len())
        .max()
        .unwrap_or(10)
        .max(10);
    let status_width = 14;
    let ready_width = 11;

    println!(
        "{:<width$} {:<status_width$} {:<ready_width$}",
        "STACK NAME",
        "STATUS",
        "READY/TOTAL",
        width = name_width
    );
    println!(
        "{}",
        "-".repeat(name_width + status_width + ready_width + 2)
    );

    for entry in &entries {
        let ready_str = format!("{}/{}", entry.ready, entry.total);
        println!(
            "{:<width$} {:<status_width$} {:<ready_width$}",
            entry.name,
            entry.status,
            ready_str,
            width = name_width
        );
        if let Some(ref err) = entry.error_summary {
            let summary = if err.len() > 50 {
                format!("{}...", &err[..47])
            } else {
                err.clone()
            };
            println!("  └─ {}", summary);
        }
    }

    println!();
    let running = entries
        .iter()
        .filter(|entry| entry.status.contains("running"))
        .count();
    let starting = entries
        .iter()
        .filter(|entry| entry.status.contains("starting"))
        .count();
    let failed = entries
        .iter()
        .filter(|entry| entry.status.contains("failed"))
        .count();

    if failed > 0 {
        println!(
            "Showing {} stacks ({} running, {} starting, {} failed)",
            entries.len(),
            running,
            starting,
            failed
        );
    } else {
        println!("Showing {} stacks", entries.len());
    }

    Ok(())
}

// ── config ─────────────────────────────────────────────────────────

async fn cmd_config(args: ConfigArgs) -> anyhow::Result<()> {
    let file = resolve_compose_file(args.file)?;
    let yaml = std::fs::read_to_string(&file)
        .with_context(|| format!("failed to read compose file: {}", file.display()))?;

    let compose_dir = file
        .canonicalize()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."));

    let stack_name = resolve_stack_name(args.name.as_deref(), &file)?;
    let spec = parse_compose_with_dir(&yaml, &stack_name, &compose_dir)
        .with_context(|| "failed to parse compose file")?;

    if args.quiet {
        println!("Valid.");
    } else {
        let json = serde_json::to_string_pretty(&spec)
            .with_context(|| "failed to serialize stack spec")?;
        println!("{json}");
    }

    Ok(())
}

// ── run ────────────────────────────────────────────────────────────

async fn cmd_run(args: RunArgs) -> anyhow::Result<()> {
    let state_db = stack_state_db_path(args.state_dir.as_deref());
    let mut client = connect_control_plane_for_state_db(&state_db).await?;
    let _ = &args.file;

    let run_container = client
        .create_stack_run_container(runtime_v2::StackRunContainerRequest {
            metadata: None,
            stack_name: args.name.clone(),
            service_name: args.service.clone(),
            run_service_name: String::new(),
        })
        .await
        .with_context(|| {
            format!(
                "failed to create StackExecutor-backed run container for stack `{}` service `{}`",
                args.name, args.service
            )
        })?;
    let container_id = run_container.container_id.clone();
    if container_id.trim().is_empty() {
        bail!(
            "daemon returned empty container id for one-off run on service `{}`",
            args.service
        );
    }
    let run_service_name = run_container.run_service_name;
    if run_service_name.trim().is_empty() {
        bail!(
            "daemon returned empty run service name for stack `{}` service `{}`",
            args.name,
            args.service
        );
    }

    let command_result =
        execute_stack_container_command(&mut client, container_id.clone(), &args.command)
            .await
            .with_context(|| {
                format!(
                    "failed to run one-off command for stack `{}` service `{}`",
                    args.name, args.service
                )
            });

    let cleanup_result = if args.rm {
        client
            .remove_stack_run_container(runtime_v2::StackRunContainerRequest {
                metadata: None,
                stack_name: args.name.clone(),
                service_name: args.service.clone(),
                run_service_name: run_service_name.clone(),
            })
            .await
            .with_context(|| {
                format!(
                    "failed to remove one-off run service `{}` (container `{}`) for stack `{}` service `{}`",
                    run_service_name, container_id, args.name, args.service
                )
            })
            .map(|_| ())
    } else {
        Ok(())
    };

    match (command_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(command_error), Err(cleanup_error)) => Err(anyhow::anyhow!(
            "{command_error}; cleanup failed: {cleanup_error}"
        )),
    }
}

// ── dashboard ─────────────────────────────────────────────────────

/// Open the TUI dashboard for an existing (running or stopped) stack.
async fn cmd_dashboard(args: DashboardArgs) -> anyhow::Result<()> {
    let _ = args;
    bail!(
        "`vz stack dashboard` is deprecated and removed in daemon mode. Use `vz stack ps`, `vz stack logs`, and `vz stack events` instead."
    )
}

// ── Helpers ────────────────────────────────────────────────────────

/// Standard compose file names in Docker Compose discovery order.
const COMPOSE_FILE_CANDIDATES: &[&str] = &[
    "compose.yaml",
    "compose.yml",
    "docker-compose.yml",
    "docker-compose.yaml",
];

/// Resolve the compose file path from an explicit `-f` flag or auto-discovery.
///
/// When no explicit path is given, searches the current directory for the first
/// existing file from [`COMPOSE_FILE_CANDIDATES`] (matching Docker Compose's
/// discovery behaviour).
fn resolve_compose_file(explicit: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }

    for candidate in COMPOSE_FILE_CANDIDATES {
        let p = PathBuf::from(candidate);
        if p.exists() {
            return Ok(p);
        }
    }

    bail!(
        "no compose file found. Searched for: {}.\n\
         Use -f to specify one explicitly.",
        COMPOSE_FILE_CANDIDATES.join(", ")
    );
}

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

fn print_ps_table(observed: &[ServiceObservedState], desired: Option<&StackSpec>) {
    if observed.is_empty() {
        println!("No services found.");
        return;
    }

    // Create a map of service name to ports for quick lookup
    let ports_map: std::collections::HashMap<&str, Vec<String>> = desired
        .map(|spec| {
            spec.services
                .iter()
                .map(|s| {
                    let ports = s
                        .ports
                        .iter()
                        .map(|p| {
                            if let Some(hp) = p.host_port {
                                format!("{}:{}", hp, p.container_port)
                            } else {
                                format!("{}", p.container_port)
                            }
                        })
                        .collect();
                    (s.name.as_str(), ports)
                })
                .collect()
        })
        .unwrap_or_default();

    // Header.
    let name_width = 14;
    let status_width = 14;
    let health_width = 8;
    let cpu_width = 8;
    let mem_width = 10;
    let ports_width = 16;
    let container_width = 20;

    println!(
        "{:<wn$} {:<ws$} {:<wh$} {:<wc$} {:<wm$} {:<wp$} {:<wcid$}",
        "SERVICE",
        "STATUS",
        "HEALTH",
        "CPU",
        "MEMORY",
        "PORTS",
        "CONTAINER",
        wn = name_width,
        ws = status_width,
        wh = health_width,
        wc = cpu_width,
        wm = mem_width,
        wp = ports_width,
        wcid = container_width
    );
    println!(
        "{}",
        "-".repeat(
            name_width
                + status_width
                + health_width
                + cpu_width
                + mem_width
                + ports_width
                + container_width
                + 6
        )
    );

    for svc in observed {
        let status = match svc.phase {
            ServicePhase::Pending => "pending".to_string(),
            ServicePhase::Creating => "creating".to_string(),
            ServicePhase::Running if svc.ready => "running".to_string(),
            ServicePhase::Running => "running".to_string(),
            ServicePhase::Stopping => "stopping".to_string(),
            ServicePhase::Stopped => "stopped".to_string(),
            ServicePhase::Failed => "failed".to_string(),
        };

        let health = if svc.phase == ServicePhase::Failed {
            "\u{2717} fail".to_string()
        } else if svc.ready {
            "\u{2713} ok".to_string()
        } else if svc.phase == ServicePhase::Running {
            "-".to_string()
        } else {
            "-".to_string()
        };

        // Resource usage: not yet available from the runtime backend.
        let cpu = "N/A";
        let mem = "N/A";

        let ports = ports_map
            .get(svc.service_name.as_str())
            .map(|p| p.join(", "))
            .unwrap_or_else(|| "-".to_string());

        let cid = svc.container_id.as_deref().unwrap_or("-");
        println!(
            "{:<wn$} {:<ws$} {:<wh$} {:<wc$} {:<wm$} {:<wp$} {:<wcid$}",
            svc.service_name,
            status,
            health,
            cpu,
            mem,
            ports,
            cid,
            wn = name_width,
            ws = status_width,
            wh = health_width,
            wc = cpu_width,
            wm = mem_width,
            wp = ports_width,
            wcid = container_width
        );
    }

    // Note about resource usage
    println!();
    println!("Note: CPU/Memory usage requires runtime metrics (not yet available)");
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
        StackEvent::MountTopologyRecreateRequired {
            service_name,
            previous_digest,
            desired_digest,
            ..
        } => format!(
            "mount recreate: {service_name} ({:?} -> {desired_digest})",
            previous_digest.as_deref().unwrap_or("<none>")
        ),
        StackEvent::SandboxCreating { sandbox_id, .. } => {
            format!("sandbox creating: {sandbox_id}")
        }
        StackEvent::SandboxReady { sandbox_id, .. } => {
            format!("sandbox ready: {sandbox_id}")
        }
        StackEvent::SandboxDraining { sandbox_id, .. } => {
            format!("sandbox draining: {sandbox_id}")
        }
        StackEvent::SandboxTerminated { sandbox_id, .. } => {
            format!("sandbox terminated: {sandbox_id}")
        }
        StackEvent::SandboxFailed {
            sandbox_id, error, ..
        } => format!("sandbox failed: {sandbox_id}: {error}"),
        StackEvent::LeaseOpened { lease_id, .. } => {
            format!("lease opened: {lease_id}")
        }
        StackEvent::LeaseHeartbeat { lease_id } => {
            format!("lease heartbeat: {lease_id}")
        }
        StackEvent::LeaseExpired { lease_id } => {
            format!("lease expired: {lease_id}")
        }
        StackEvent::LeaseClosed { lease_id } => {
            format!("lease closed: {lease_id}")
        }
        StackEvent::LeaseFailed { lease_id, error } => {
            format!("lease failed: {lease_id}: {error}")
        }
        StackEvent::ExecutionQueued {
            execution_id,
            container_id,
        } => format!("execution queued: {execution_id} for {container_id}"),
        StackEvent::ExecutionRunning { execution_id } => {
            format!("execution running: {execution_id}")
        }
        StackEvent::ExecutionExited {
            execution_id,
            exit_code,
        } => format!("execution exited: {execution_id} (code {exit_code})"),
        StackEvent::ExecutionFailed {
            execution_id,
            error,
        } => format!("execution failed: {execution_id}: {error}"),
        StackEvent::ExecutionCanceled { execution_id } => {
            format!("execution canceled: {execution_id}")
        }
        StackEvent::ExecutionResized {
            execution_id,
            cols,
            rows,
        } => format!("execution resized: {execution_id} ({cols}x{rows})"),
        StackEvent::ExecutionSignaled {
            execution_id,
            signal,
        } => format!("execution signaled: {execution_id} ({signal})"),
        StackEvent::CheckpointCreating {
            checkpoint_id,
            class,
            ..
        } => format!("checkpoint creating: {checkpoint_id} ({class})"),
        StackEvent::CheckpointReady { checkpoint_id } => {
            format!("checkpoint ready: {checkpoint_id}")
        }
        StackEvent::CheckpointFailed {
            checkpoint_id,
            error,
        } => format!("checkpoint failed: {checkpoint_id}: {error}"),
        StackEvent::CheckpointRestored {
            checkpoint_id,
            sandbox_id,
        } => format!("checkpoint restored: {checkpoint_id} -> {sandbox_id}"),
        StackEvent::CheckpointForked {
            parent_checkpoint_id,
            new_checkpoint_id,
            ..
        } => format!("checkpoint forked: {parent_checkpoint_id} -> {new_checkpoint_id}"),
        StackEvent::BuildQueued {
            sandbox_id,
            build_id,
        } => format!("build queued: {build_id} for {sandbox_id}"),
        StackEvent::BuildRunning { build_id } => {
            format!("build running: {build_id}")
        }
        StackEvent::BuildSucceeded {
            build_id,
            result_digest,
        } => format!("build succeeded: {build_id} ({result_digest})"),
        StackEvent::BuildFailed { build_id, error } => {
            format!("build failed: {build_id}: {error}")
        }
        StackEvent::BuildCanceled { build_id } => {
            format!("build canceled: {build_id}")
        }
        StackEvent::ContainerCreated {
            container_id,
            sandbox_id,
        } => format!("container created: {container_id} in {sandbox_id}"),
        StackEvent::ContainerStarting { container_id } => {
            format!("container starting: {container_id}")
        }
        StackEvent::ContainerRunning { container_id } => {
            format!("container running: {container_id}")
        }
        StackEvent::ContainerStopping { container_id } => {
            format!("container stopping: {container_id}")
        }
        StackEvent::ContainerExited {
            container_id,
            exit_code,
        } => format!("container exited: {container_id} (code {exit_code})"),
        StackEvent::ContainerFailed {
            container_id,
            error,
        } => format!("container failed: {container_id}: {error}"),
        StackEvent::ContainerRemoved { container_id } => {
            format!("container removed: {container_id}")
        }
        StackEvent::DriftDetected {
            category,
            description,
            severity,
            ..
        } => format!("drift [{severity}] {category}: {description}"),
        StackEvent::OrphanCleaned { container_id, .. } => {
            format!("orphan cleaned: {container_id}")
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn cwd_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn resolve_stack_registry_auth_defaults_to_none() {
        let opts = StackRegistryAuthOpts::default();
        let auth = resolve_stack_registry_auth(&opts).unwrap();
        assert!(auth.is_none());
    }

    #[test]
    fn resolve_stack_registry_auth_supports_docker_config() {
        let opts = StackRegistryAuthOpts {
            docker_config: true,
            ..Default::default()
        };
        let auth = resolve_stack_registry_auth(&opts).unwrap();
        assert_eq!(auth, Some(vz_image::Auth::DockerConfig));
    }

    #[test]
    fn resolve_stack_registry_auth_supports_basic_credentials() {
        let opts = StackRegistryAuthOpts {
            username: Some("alice".to_string()),
            password: Some("s3cr3t".to_string()),
            ..Default::default()
        };
        let auth = resolve_stack_registry_auth(&opts).unwrap();
        assert_eq!(
            auth,
            Some(vz_image::Auth::Basic {
                username: "alice".to_string(),
                password: "s3cr3t".to_string(),
            })
        );
    }

    #[test]
    fn resolve_stack_name_explicit() {
        let name = resolve_stack_name(Some("myapp"), &PathBuf::from("compose.yaml")).unwrap();
        assert_eq!(name, "myapp");
    }

    #[test]
    fn split_exec_command_separates_head_and_args() {
        let command = vec![
            "/bin/echo".to_string(),
            "hello".to_string(),
            "world".to_string(),
        ];
        let (cmd, args) = split_exec_command(&command).unwrap();
        assert_eq!(cmd, vec!["/bin/echo".to_string()]);
        assert_eq!(args, vec!["hello".to_string(), "world".to_string()]);
    }

    #[test]
    fn split_exec_command_rejects_empty_input() {
        let result = split_exec_command(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn resolve_service_container_id_returns_container_for_service() {
        let services = vec![runtime_v2::StackServiceStatus {
            service_name: "web".to_string(),
            phase: "running".to_string(),
            ready: true,
            container_id: "ctr-web-1".to_string(),
            last_error: String::new(),
        }];

        let container_id = resolve_service_container_id("demo", "web", &services).unwrap();
        assert_eq!(container_id, "ctr-web-1");
    }

    #[test]
    fn resolve_service_container_id_errors_when_service_not_running() {
        let services = vec![runtime_v2::StackServiceStatus {
            service_name: "web".to_string(),
            phase: "creating".to_string(),
            ready: false,
            container_id: String::new(),
            last_error: String::new(),
        }];

        let error = resolve_service_container_id("demo", "web", &services).unwrap_err();
        assert!(error.to_string().contains("not running"));
    }

    #[test]
    fn resolve_service_container_id_errors_when_service_missing() {
        let services = vec![runtime_v2::StackServiceStatus {
            service_name: "db".to_string(),
            phase: "running".to_string(),
            ready: true,
            container_id: "ctr-db-1".to_string(),
            last_error: String::new(),
        }];

        let error = resolve_service_container_id("demo", "web", &services).unwrap_err();
        assert!(error.to_string().contains("not found"));
    }

    #[test]
    fn resolve_compose_file_explicit_path() {
        let p = resolve_compose_file(Some(PathBuf::from("/tmp/my-compose.yml"))).unwrap();
        assert_eq!(p, PathBuf::from("/tmp/my-compose.yml"));
    }

    #[test]
    fn resolve_compose_file_discovery_in_tempdir() {
        let _guard = cwd_lock().lock().unwrap();
        let dir = std::env::temp_dir().join("vz-test-compose-discovery");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Write a docker-compose.yml (not compose.yaml).
        let target = dir.join("docker-compose.yml");
        std::fs::write(&target, "services: {}").unwrap();

        // Discovery should find it even though compose.yaml doesn't exist.
        let saved = std::env::current_dir().unwrap();
        std::env::set_current_dir(&dir).unwrap();
        let found = resolve_compose_file(None);
        std::env::set_current_dir(&saved).unwrap();

        assert_eq!(found.unwrap(), PathBuf::from("docker-compose.yml"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_compose_file_no_file_errors() {
        let _guard = cwd_lock().lock().unwrap();
        let dir = std::env::temp_dir().join("vz-test-compose-empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let saved = std::env::current_dir().unwrap();
        std::env::set_current_dir(&dir).unwrap();
        let result = resolve_compose_file(None);
        std::env::set_current_dir(&saved).unwrap();

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no compose file found")
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cmd_dashboard_returns_deprecation_message() {
        let error = cmd_dashboard(DashboardArgs {
            name: "demo".to_string(),
            file: None,
            state_dir: None,
        })
        .await
        .expect_err("dashboard should be deprecated");
        assert!(error.to_string().contains("deprecated and removed"));
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
        print_ps_table(&[], None);
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
        print_ps_table(&observed, None);
    }

    #[test]
    fn print_events_table_empty() {
        print_events_table(&[]);
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
}
