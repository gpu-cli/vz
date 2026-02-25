//! `vz checkpoint` — checkpoint lifecycle management commands.
//!
//! Provides `list`, `inspect`, and `create` subcommands backed by the
//! `vz-stack` state store for checkpoint persistence.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use vz_runtime_contract::{Checkpoint, CheckpointClass, CheckpointState};
use vz_stack::StateStore;

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
        CheckpointCommand::List(list_args) => cmd_list(list_args),
        CheckpointCommand::Inspect(inspect_args) => cmd_inspect(inspect_args),
        CheckpointCommand::Create(create_args) => cmd_create(create_args),
    }
}

fn cmd_list(args: CheckpointListArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;

    let checkpoints = if let Some(ref sandbox_id) = args.sandbox_id {
        store
            .list_checkpoints_for_sandbox(sandbox_id)
            .context("failed to list checkpoints for sandbox")?
    } else {
        store
            .list_checkpoints()
            .context("failed to list checkpoints")?
    };

    if args.json {
        let json = serde_json::to_string_pretty(&checkpoints)
            .context("failed to serialize checkpoints")?;
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
    for checkpoint in &checkpoints {
        let class = serde_json::to_string(&checkpoint.class)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let state = serde_json::to_string(&checkpoint.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let fingerprint = if checkpoint.compatibility_fingerprint.len() > 18 {
            format!("{}...", &checkpoint.compatibility_fingerprint[..18])
        } else {
            checkpoint.compatibility_fingerprint.clone()
        };
        println!(
            "{:<40} {:<40} {:<12} {:<10} {:<20}",
            checkpoint.checkpoint_id, checkpoint.sandbox_id, class, state, fingerprint
        );
    }

    Ok(())
}

fn cmd_inspect(args: CheckpointInspectArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;
    let checkpoint = store
        .load_checkpoint(&args.checkpoint_id)
        .context("failed to load checkpoint")?;

    match checkpoint {
        Some(c) => {
            let json =
                serde_json::to_string_pretty(&c).context("failed to serialize checkpoint")?;
            println!("{json}");
        }
        None => bail!("checkpoint {} not found", args.checkpoint_id),
    }

    Ok(())
}

fn cmd_create(args: CheckpointCreateArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;

    let class = match args.class.as_str() {
        "fs_quick" => CheckpointClass::FsQuick,
        "vm_full" => CheckpointClass::VmFull,
        other => bail!("unknown checkpoint class: {other}"),
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let mut checkpoint = Checkpoint {
        checkpoint_id: format!("ckpt-{}", uuid::Uuid::new_v4()),
        sandbox_id: args.sandbox_id,
        parent_checkpoint_id: None,
        class,
        state: CheckpointState::Creating,
        created_at: now,
        compatibility_fingerprint: args.fingerprint,
    };

    store
        .save_checkpoint(&checkpoint)
        .context("failed to save checkpoint")?;

    checkpoint
        .transition_to(CheckpointState::Ready)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    store
        .save_checkpoint(&checkpoint)
        .context("failed to update checkpoint state")?;

    let state = serde_json::to_string(&checkpoint.state)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string();
    println!(
        "Checkpoint {} created (state: {state}).",
        checkpoint.checkpoint_id
    );

    Ok(())
}
