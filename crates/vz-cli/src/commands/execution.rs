//! `vz execution` -- execution lifecycle management commands.
//!
//! Provides `list`, `inspect`, and `cancel` subcommands backed by the
//! `vz-stack` state store for execution persistence.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use vz_runtime_contract::ExecutionState;
use vz_stack::StateStore;

/// Manage container executions.
#[derive(Args, Debug)]
pub struct ExecutionArgs {
    #[command(subcommand)]
    pub action: ExecutionCommand,
}

#[derive(Subcommand, Debug)]
pub enum ExecutionCommand {
    /// List all executions.
    List(ExecutionListArgs),

    /// Show detailed execution information.
    Inspect(ExecutionInspectArgs),

    /// Cancel a running or queued execution.
    Cancel(ExecutionCancelArgs),
}

/// Arguments for `vz execution list`.
#[derive(Args, Debug)]
pub struct ExecutionListArgs {
    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,

    /// Filter by container identifier.
    #[arg(long)]
    container_id: Option<String>,

    /// Output as JSON.
    #[arg(long)]
    json: bool,
}

/// Arguments for `vz execution inspect`.
#[derive(Args, Debug)]
pub struct ExecutionInspectArgs {
    /// Execution identifier.
    pub execution_id: String,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Arguments for `vz execution cancel`.
#[derive(Args, Debug)]
pub struct ExecutionCancelArgs {
    /// Execution identifier.
    pub execution_id: String,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Run the execution subcommand.
pub async fn run(args: ExecutionArgs) -> anyhow::Result<()> {
    match args.action {
        ExecutionCommand::List(list_args) => cmd_list(list_args),
        ExecutionCommand::Inspect(inspect_args) => cmd_inspect(inspect_args),
        ExecutionCommand::Cancel(cancel_args) => cmd_cancel(cancel_args),
    }
}

fn cmd_list(args: ExecutionListArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;

    let executions = if let Some(ref container_id) = args.container_id {
        store
            .list_executions_for_container(container_id)
            .context("failed to list executions for container")?
    } else {
        store
            .list_executions()
            .context("failed to list executions")?
    };

    if args.json {
        let json =
            serde_json::to_string_pretty(&executions).context("failed to serialize executions")?;
        println!("{json}");
        return Ok(());
    }

    if executions.is_empty() {
        println!("No executions found.");
        return Ok(());
    }

    println!(
        "{:<40} {:<20} {:<10} {:<10} {:<12}",
        "EXECUTION ID", "CONTAINER ID", "STATE", "EXIT CODE", "CMD"
    );
    for execution in &executions {
        let state = serde_json::to_string(&execution.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let exit_code = execution
            .exit_code
            .map(|c| c.to_string())
            .unwrap_or_else(|| "-".to_string());
        let cmd = execution.exec_spec.cmd.join(" ");
        let cmd_display = if cmd.len() > 30 {
            format!("{}...", &cmd[..27])
        } else {
            cmd
        };
        println!(
            "{:<40} {:<20} {:<10} {:<10} {:<12}",
            execution.execution_id, execution.container_id, state, exit_code, cmd_display
        );
    }

    Ok(())
}

fn cmd_inspect(args: ExecutionInspectArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;
    let execution = store
        .load_execution(&args.execution_id)
        .context("failed to load execution")?;

    match execution {
        Some(e) => {
            let json = serde_json::to_string_pretty(&e).context("failed to serialize execution")?;
            println!("{json}");
        }
        None => bail!("execution {} not found", args.execution_id),
    }

    Ok(())
}

fn cmd_cancel(args: ExecutionCancelArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;
    let mut execution = store
        .load_execution(&args.execution_id)
        .context("failed to load execution")?
        .ok_or_else(|| anyhow::anyhow!("execution {} not found", args.execution_id))?;

    if execution.state.is_terminal() {
        println!(
            "Execution {} is already in terminal state.",
            args.execution_id
        );
        return Ok(());
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // Ensure started_at is set for transition consistency.
    if execution.started_at.is_none() {
        execution.started_at = Some(now);
    }
    execution.ended_at = Some(now);

    execution
        .transition_to(ExecutionState::Canceled)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    store
        .save_execution(&execution)
        .context("failed to save execution")?;

    println!("Execution {} canceled.", args.execution_id);

    Ok(())
}
