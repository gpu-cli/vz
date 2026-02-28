//! `vz stack` — multi-service stack lifecycle commands.
//!
//! Runtime-mutating stack operations are daemon-owned.
//! Command paths without daemon parity fail closed.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use clap::{Args, Subcommand};
use reqwest::StatusCode as HttpStatusCode;
use serde::{Deserialize, Serialize};
use tracing::debug;

use vz_runtime_proto::runtime_v2;
use vz_stack::{
    EventRecord, ServiceObservedState, ServicePhase, StackEvent, StackSpec, parse_compose_with_dir,
};

use super::runtime_daemon::{
    ControlPlaneTransport, connect_control_plane_for_state_db, control_plane_transport,
    default_state_db_path, runtime_api_base_url,
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
    ///
    /// Host-facing port publishing requires explicit host bindings in Compose
    /// (`HOST:CONTAINER`). Container-only ports remain internal to stack networking.
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
mod api;
mod commands;
mod helpers;
mod output;
#[cfg(test)]
mod tests;

pub async fn run(args: StackArgs) -> anyhow::Result<()> {
    match args.action {
        StackCommand::Up(args) => commands::cmd_up(args).await,
        StackCommand::Down(args) => commands::cmd_down(args).await,
        StackCommand::Ps(args) => commands::cmd_ps(args).await,
        StackCommand::Ls(args) => commands::cmd_ls(args).await,
        StackCommand::Config(args) => commands::cmd_config(args).await,
        StackCommand::Events(args) => commands::cmd_events(args).await,
        StackCommand::Logs(args) => commands::cmd_logs(args).await,
        StackCommand::Exec(args) => commands::cmd_exec(args).await,
        StackCommand::Run(args) => commands::cmd_run(args).await,
        StackCommand::Stop(args) => {
            commands::cmd_service_action(args, commands::ControlAction::Stop).await
        }
        StackCommand::Start(args) => {
            commands::cmd_service_action(args, commands::ControlAction::Start).await
        }
        StackCommand::Restart(args) => {
            commands::cmd_service_action(args, commands::ControlAction::Restart).await
        }
        StackCommand::Dashboard(args) => commands::cmd_dashboard(args).await,
    }
}
