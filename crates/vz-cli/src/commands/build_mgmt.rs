//! `vz build-mgmt` -- build entity lifecycle management commands.
//!
//! Provides `list`, `inspect`, and `cancel` subcommands backed by the
//! `vz-stack` state store for build persistence.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use vz_runtime_contract::BuildState;
use vz_stack::StateStore;

/// Manage asynchronous build operations.
#[derive(Args, Debug)]
pub struct BuildMgmtArgs {
    #[command(subcommand)]
    pub action: BuildMgmtCommand,
}

#[derive(Subcommand, Debug)]
pub enum BuildMgmtCommand {
    /// List all builds.
    List(BuildMgmtListArgs),

    /// Show detailed build information.
    Inspect(BuildMgmtInspectArgs),

    /// Cancel a running or queued build.
    Cancel(BuildMgmtCancelArgs),
}

/// Arguments for `vz build-mgmt list`.
#[derive(Args, Debug)]
pub struct BuildMgmtListArgs {
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

/// Arguments for `vz build-mgmt inspect`.
#[derive(Args, Debug)]
pub struct BuildMgmtInspectArgs {
    /// Build identifier.
    pub build_id: String,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Arguments for `vz build-mgmt cancel`.
#[derive(Args, Debug)]
pub struct BuildMgmtCancelArgs {
    /// Build identifier.
    pub build_id: String,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Run the build management subcommand.
pub async fn run(args: BuildMgmtArgs) -> anyhow::Result<()> {
    match args.action {
        BuildMgmtCommand::List(list_args) => cmd_list(list_args),
        BuildMgmtCommand::Inspect(inspect_args) => cmd_inspect(inspect_args),
        BuildMgmtCommand::Cancel(cancel_args) => cmd_cancel(cancel_args),
    }
}

fn cmd_list(args: BuildMgmtListArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;

    let builds = if let Some(ref sandbox_id) = args.sandbox_id {
        store
            .list_builds_for_sandbox(sandbox_id)
            .context("failed to list builds for sandbox")?
    } else {
        store.list_builds().context("failed to list builds")?
    };

    if args.json {
        let json = serde_json::to_string_pretty(&builds).context("failed to serialize builds")?;
        println!("{json}");
        return Ok(());
    }

    if builds.is_empty() {
        println!("No builds found.");
        return Ok(());
    }

    println!(
        "{:<40} {:<20} {:<12} {:<20} {:<12}",
        "BUILD ID", "SANDBOX ID", "STATE", "CONTEXT", "DIGEST"
    );
    for build in &builds {
        let state = serde_json::to_string(&build.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let context_display = if build.build_spec.context.len() > 18 {
            format!("{}...", &build.build_spec.context[..15])
        } else {
            build.build_spec.context.clone()
        };
        let digest = build.result_digest.as_deref().unwrap_or("-");
        let digest_display = if digest.len() > 10 {
            format!("{}...", &digest[..7])
        } else {
            digest.to_string()
        };
        println!(
            "{:<40} {:<20} {:<12} {:<20} {:<12}",
            build.build_id, build.sandbox_id, state, context_display, digest_display
        );
    }

    Ok(())
}

fn cmd_inspect(args: BuildMgmtInspectArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;
    let build = store
        .load_build(&args.build_id)
        .context("failed to load build")?;

    match build {
        Some(b) => {
            let json = serde_json::to_string_pretty(&b).context("failed to serialize build")?;
            println!("{json}");
        }
        None => bail!("build {} not found", args.build_id),
    }

    Ok(())
}

fn cmd_cancel(args: BuildMgmtCancelArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;
    let mut build = store
        .load_build(&args.build_id)
        .context("failed to load build")?
        .ok_or_else(|| anyhow::anyhow!("build {} not found", args.build_id))?;

    if build.state.is_terminal() {
        println!("Build {} is already in terminal state.", args.build_id);
        return Ok(());
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    build.ended_at = Some(now);

    build
        .transition_to(BuildState::Canceled)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    store.save_build(&build).context("failed to save build")?;

    println!("Build {} canceled.", args.build_id);

    Ok(())
}
