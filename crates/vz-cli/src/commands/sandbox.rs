//! `vz sandbox` — sandbox lifecycle management commands.
//!
//! Provides `list`, `inspect`, and `terminate` subcommands backed by the
//! `vz-stack` state store for sandbox persistence.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use vz_runtime_contract::SandboxState;
use vz_stack::StateStore;

/// Manage sandbox runtime boundaries.
#[derive(Args, Debug)]
pub struct SandboxArgs {
    #[command(subcommand)]
    pub action: SandboxCommand,
}

#[derive(Subcommand, Debug)]
pub enum SandboxCommand {
    /// List all sandboxes.
    List(SandboxListArgs),

    /// Show detailed sandbox information.
    Inspect(SandboxInspectArgs),

    /// Terminate a sandbox.
    Terminate(SandboxTerminateArgs),
}

/// Arguments for `vz sandbox list`.
#[derive(Args, Debug)]
pub struct SandboxListArgs {
    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,

    /// Output as JSON.
    #[arg(long)]
    json: bool,
}

/// Arguments for `vz sandbox inspect`.
#[derive(Args, Debug)]
pub struct SandboxInspectArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Arguments for `vz sandbox terminate`.
#[derive(Args, Debug)]
pub struct SandboxTerminateArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Run the sandbox subcommand.
pub async fn run(args: SandboxArgs) -> anyhow::Result<()> {
    match args.action {
        SandboxCommand::List(list_args) => cmd_list(list_args),
        SandboxCommand::Inspect(inspect_args) => cmd_inspect(inspect_args),
        SandboxCommand::Terminate(terminate_args) => cmd_terminate(terminate_args),
    }
}

fn cmd_list(args: SandboxListArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;
    let sandboxes = store.list_sandboxes().context("failed to list sandboxes")?;

    if args.json {
        let json =
            serde_json::to_string_pretty(&sandboxes).context("failed to serialize sandboxes")?;
        println!("{json}");
        return Ok(());
    }

    if sandboxes.is_empty() {
        println!("No sandboxes found.");
        return Ok(());
    }

    println!(
        "{:<40} {:<12} {:<6} {:<10} {:<12}",
        "SANDBOX ID", "STATE", "CPUS", "MEMORY MB", "BACKEND"
    );
    for sandbox in &sandboxes {
        let cpus = sandbox
            .spec
            .cpus
            .map(|c| c.to_string())
            .unwrap_or_else(|| "-".to_string());
        let memory = sandbox
            .spec
            .memory_mb
            .map(|m| m.to_string())
            .unwrap_or_else(|| "-".to_string());
        let backend = serde_json::to_string(&sandbox.backend)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let state = serde_json::to_string(&sandbox.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        println!(
            "{:<40} {:<12} {:<6} {:<10} {:<12}",
            sandbox.sandbox_id, state, cpus, memory, backend
        );
    }

    Ok(())
}

fn cmd_inspect(args: SandboxInspectArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;
    let sandbox = store
        .load_sandbox(&args.sandbox_id)
        .context("failed to load sandbox")?;

    match sandbox {
        Some(s) => {
            let json = serde_json::to_string_pretty(&s).context("failed to serialize sandbox")?;
            println!("{json}");
        }
        None => bail!("sandbox {} not found", args.sandbox_id),
    }

    Ok(())
}

fn cmd_terminate(args: SandboxTerminateArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;
    let mut sandbox = store
        .load_sandbox(&args.sandbox_id)
        .context("failed to load sandbox")?
        .ok_or_else(|| anyhow::anyhow!("sandbox {} not found", args.sandbox_id))?;

    if sandbox.state.is_terminal() {
        println!("Sandbox {} is already in terminal state.", args.sandbox_id);
        return Ok(());
    }

    match sandbox.state {
        SandboxState::Creating => {
            sandbox
                .transition_to(SandboxState::Failed)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        SandboxState::Ready => {
            sandbox
                .transition_to(SandboxState::Draining)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            sandbox
                .transition_to(SandboxState::Terminated)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        SandboxState::Draining => {
            sandbox
                .transition_to(SandboxState::Terminated)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        _ => {}
    }

    store
        .save_sandbox(&sandbox)
        .context("failed to save sandbox")?;

    let state = serde_json::to_string(&sandbox.state)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string();
    println!("Sandbox {} terminated (state: {state}).", args.sandbox_id);

    Ok(())
}
