//! `vz checkpoint` — checkpoint lifecycle management commands.
//!
//! Provides `list`, `inspect`, and `create` subcommands backed by the
//! runtime daemon control plane.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;

use anyhow::{Context, anyhow, bail};
use clap::{Args, Subcommand};
use tonic::Code;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClientError;

use super::runtime_daemon::connect_control_plane_for_state_db;

/// Manage checkpoint fingerprints and lineage.
#[derive(Args, Debug)]
pub struct CheckpointArgs {
    #[command(subcommand)]
    pub action: CheckpointCommand,
}

#[derive(Subcommand, Debug)]
pub enum CheckpointCommand {
    /// List all checkpoints.
    List(CheckpointListArgs),

    /// Show detailed checkpoint information.
    Inspect(CheckpointInspectArgs),

    /// Create a new checkpoint.
    Create(CheckpointCreateArgs),
}

/// Arguments for `vz checkpoint list`.
#[derive(Args, Debug)]
pub struct CheckpointListArgs {
    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,

    /// Filter by sandbox identifier.
    #[arg(long)]
    sandbox_id: Option<String>,

    /// Output as JSON.
    #[arg(long)]
    json: bool,
}

/// Arguments for `vz checkpoint inspect`.
#[derive(Args, Debug)]
pub struct CheckpointInspectArgs {
    /// Checkpoint identifier.
    pub checkpoint_id: String,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Arguments for `vz checkpoint create`.
#[derive(Args, Debug)]
pub struct CheckpointCreateArgs {
    /// Owning sandbox identifier.
    pub sandbox_id: String,

    /// Checkpoint class: fs_quick or vm_full.
    #[arg(long, default_value = "fs_quick")]
    class: String,

    /// Compatibility fingerprint string.
    #[arg(long, default_value = "unset")]
    fingerprint: String,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Run the checkpoint subcommand.
pub async fn run(args: CheckpointArgs) -> anyhow::Result<()> {
    match args.action {
        CheckpointCommand::List(list_args) => cmd_list(list_args).await,
        CheckpointCommand::Inspect(inspect_args) => cmd_inspect(inspect_args).await,
        CheckpointCommand::Create(create_args) => cmd_create(create_args).await,
    }
}

fn checkpoint_json(payload: &runtime_v2::CheckpointPayload) -> serde_json::Value {
    serde_json::json!({
        "checkpoint_id": payload.checkpoint_id,
        "sandbox_id": payload.sandbox_id,
        "parent_checkpoint_id": if payload.parent_checkpoint_id.trim().is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(payload.parent_checkpoint_id.clone())
        },
        "class": payload.checkpoint_class,
        "state": payload.state,
        "compatibility_fingerprint": payload.compatibility_fingerprint,
        "created_at": payload.created_at,
    })
}

async fn cmd_list(args: CheckpointListArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let mut checkpoints = client
        .list_checkpoints(runtime_v2::ListCheckpointsRequest { metadata: None })
        .await
        .context("failed to list checkpoints via daemon")?
        .checkpoints;

    if let Some(sandbox_id) = args.sandbox_id.as_deref() {
        checkpoints.retain(|checkpoint| checkpoint.sandbox_id == sandbox_id);
    }

    if args.json {
        let payload: Vec<_> = checkpoints.iter().map(checkpoint_json).collect();
        let json =
            serde_json::to_string_pretty(&payload).context("failed to serialize checkpoints")?;
        println!("{json}");
        return Ok(());
    }

    if checkpoints.is_empty() {
        println!("No checkpoints found.");
        return Ok(());
    }

    println!(
        "{:<40} {:<40} {:<12} {:<10} {:<20}",
        "CHECKPOINT ID", "SANDBOX ID", "CLASS", "STATE", "FINGERPRINT"
    );
    for payload in &checkpoints {
        let class = if payload.checkpoint_class.trim().is_empty() {
            "unknown"
        } else {
            payload.checkpoint_class.as_str()
        };
        let state = if payload.state.trim().is_empty() {
            "unknown"
        } else {
            payload.state.as_str()
        };
        let fingerprint = if payload.compatibility_fingerprint.len() > 18 {
            format!("{}...", &payload.compatibility_fingerprint[..18])
        } else {
            payload.compatibility_fingerprint.clone()
        };
        println!(
            "{:<40} {:<40} {:<12} {:<10} {:<20}",
            payload.checkpoint_id, payload.sandbox_id, class, state, fingerprint
        );
    }

    Ok(())
}

async fn cmd_inspect(args: CheckpointInspectArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .get_checkpoint(runtime_v2::GetCheckpointRequest {
            checkpoint_id: args.checkpoint_id.clone(),
            metadata: None,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("checkpoint {} not found", args.checkpoint_id)
        }
        Err(error) => return Err(anyhow!(error).context("failed to load checkpoint via daemon")),
    };

    let payload = response
        .checkpoint
        .ok_or_else(|| anyhow!("daemon returned missing checkpoint payload"))?;
    let json = serde_json::to_string_pretty(&checkpoint_json(&payload))
        .context("failed to serialize checkpoint")?;
    println!("{json}");

    Ok(())
}

fn normalize_checkpoint_class(class: &str) -> anyhow::Result<String> {
    match class.trim().to_ascii_lowercase().as_str() {
        "fs_quick" | "fs-quick" => Ok("fs_quick".to_string()),
        "vm_full" | "vm-full" => Ok("vm_full".to_string()),
        other => bail!("unknown checkpoint class: {other}"),
    }
}

async fn cmd_create(args: CheckpointCreateArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let checkpoint_class = normalize_checkpoint_class(&args.class)?;
    let response = client
        .create_checkpoint(runtime_v2::CreateCheckpointRequest {
            metadata: None,
            sandbox_id: args.sandbox_id,
            checkpoint_class,
            compatibility_fingerprint: args.fingerprint,
        })
        .await
        .context("failed to create checkpoint via daemon")?;

    let payload = response
        .checkpoint
        .ok_or_else(|| anyhow!("daemon returned missing checkpoint payload"))?;
    let state = if payload.state.trim().is_empty() {
        "unknown"
    } else {
        payload.state.as_str()
    };
    println!(
        "Checkpoint {} created (state: {state}).",
        payload.checkpoint_id
    );

    Ok(())
}
