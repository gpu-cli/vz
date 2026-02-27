//! `vz execution` -- execution lifecycle management commands.
//!
//! Provides `list`, `inspect`, and `cancel` subcommands backed by the
//! runtime daemon control plane.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;

use anyhow::{Context, anyhow, bail};
use clap::{Args, Subcommand};
use tonic::Code;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClientError;

use super::runtime_daemon::connect_control_plane_for_state_db;

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
        ExecutionCommand::List(list_args) => cmd_list(list_args).await,
        ExecutionCommand::Inspect(inspect_args) => cmd_inspect(inspect_args).await,
        ExecutionCommand::Cancel(cancel_args) => cmd_cancel(cancel_args).await,
    }
}

fn execution_json(payload: &runtime_v2::ExecutionPayload) -> serde_json::Value {
    serde_json::json!({
        "execution_id": payload.execution_id,
        "container_id": payload.container_id,
        "state": payload.state,
        "exit_code": if payload.exit_code == 0 {
            serde_json::Value::Null
        } else {
            serde_json::Value::Number(payload.exit_code.into())
        },
        "started_at": if payload.started_at == 0 {
            serde_json::Value::Null
        } else {
            serde_json::Value::Number(payload.started_at.into())
        },
        "ended_at": if payload.ended_at == 0 {
            serde_json::Value::Null
        } else {
            serde_json::Value::Number(payload.ended_at.into())
        },
    })
}

async fn cmd_list(args: ExecutionListArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let mut executions = client
        .list_executions(runtime_v2::ListExecutionsRequest { metadata: None })
        .await
        .context("failed to list executions via daemon")?
        .executions;

    if let Some(container_id) = args.container_id.as_deref() {
        executions.retain(|execution| execution.container_id == container_id);
    }

    if args.json {
        let payload: Vec<_> = executions.iter().map(execution_json).collect();
        let json =
            serde_json::to_string_pretty(&payload).context("failed to serialize executions")?;
        println!("{json}");
        return Ok(());
    }

    if executions.is_empty() {
        println!("No executions found.");
        return Ok(());
    }

    println!(
        "{:<40} {:<20} {:<10} {:<10}",
        "EXECUTION ID", "CONTAINER ID", "STATE", "EXIT CODE"
    );
    for payload in &executions {
        let state = if payload.state.trim().is_empty() {
            "unknown"
        } else {
            payload.state.as_str()
        };
        let exit_code = if payload.exit_code == 0 {
            "-".to_string()
        } else {
            payload.exit_code.to_string()
        };
        println!(
            "{:<40} {:<20} {:<10} {:<10}",
            payload.execution_id, payload.container_id, state, exit_code
        );
    }

    Ok(())
}

async fn cmd_inspect(args: ExecutionInspectArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .get_execution(runtime_v2::GetExecutionRequest {
            execution_id: args.execution_id.clone(),
            metadata: None,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("execution {} not found", args.execution_id)
        }
        Err(error) => return Err(anyhow!(error).context("failed to load execution via daemon")),
    };

    let payload = response
        .execution
        .ok_or_else(|| anyhow!("daemon returned missing execution payload"))?;
    let json = serde_json::to_string_pretty(&execution_json(&payload))
        .context("failed to serialize execution")?;
    println!("{json}");

    Ok(())
}

async fn cmd_cancel(args: ExecutionCancelArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .cancel_execution(runtime_v2::CancelExecutionRequest {
            execution_id: args.execution_id.clone(),
            metadata: None,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("execution {} not found", args.execution_id)
        }
        Err(error) => return Err(anyhow!(error).context("failed to cancel execution via daemon")),
    };

    let payload = response
        .execution
        .ok_or_else(|| anyhow!("daemon returned missing execution payload"))?;
    let state = if payload.state.trim().is_empty() {
        "unknown"
    } else {
        payload.state.as_str()
    };
    println!("Execution {} state: {}.", payload.execution_id, state);

    Ok(())
}
