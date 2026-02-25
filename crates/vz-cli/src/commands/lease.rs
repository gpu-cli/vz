//! `vz lease` -- lease lifecycle management commands.
//!
//! Provides `list`, `inspect`, and `close` subcommands backed by the
//! `vz-stack` state store for lease persistence.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use vz_runtime_contract::LeaseState;
use vz_stack::StateStore;

/// Manage lease access grants.
#[derive(Args, Debug)]
pub struct LeaseArgs {
    #[command(subcommand)]
    pub action: LeaseCommand,
}

#[derive(Subcommand, Debug)]
pub enum LeaseCommand {
    /// List all leases.
    List(LeaseListArgs),

    /// Show detailed lease information.
    Inspect(LeaseInspectArgs),

    /// Close a lease.
    Close(LeaseCloseArgs),
}

/// Arguments for `vz lease list`.
#[derive(Args, Debug)]
pub struct LeaseListArgs {
    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,

    /// Output as JSON.
    #[arg(long)]
    json: bool,
}

/// Arguments for `vz lease inspect`.
#[derive(Args, Debug)]
pub struct LeaseInspectArgs {
    /// Lease identifier.
    pub lease_id: String,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Arguments for `vz lease close`.
#[derive(Args, Debug)]
pub struct LeaseCloseArgs {
    /// Lease identifier.
    pub lease_id: String,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Run the lease subcommand.
pub async fn run(args: LeaseArgs) -> anyhow::Result<()> {
    match args.action {
        LeaseCommand::List(list_args) => cmd_list(list_args),
        LeaseCommand::Inspect(inspect_args) => cmd_inspect(inspect_args),
        LeaseCommand::Close(close_args) => cmd_close(close_args),
    }
}

fn cmd_list(args: LeaseListArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;
    let leases = store.list_leases().context("failed to list leases")?;

    if args.json {
        let json = serde_json::to_string_pretty(&leases).context("failed to serialize leases")?;
        println!("{json}");
        return Ok(());
    }

    if leases.is_empty() {
        println!("No leases found.");
        return Ok(());
    }

    println!(
        "{:<40} {:<40} {:<10} {:<10} {:<20}",
        "LEASE ID", "SANDBOX ID", "STATE", "TTL", "LAST HEARTBEAT"
    );
    for lease in &leases {
        let state = serde_json::to_string(&lease.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        println!(
            "{:<40} {:<40} {:<10} {:<10} {:<20}",
            lease.lease_id, lease.sandbox_id, state, lease.ttl_secs, lease.last_heartbeat_at
        );
    }

    Ok(())
}

fn cmd_inspect(args: LeaseInspectArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;
    let lease = store
        .load_lease(&args.lease_id)
        .context("failed to load lease")?;

    match lease {
        Some(l) => {
            let json = serde_json::to_string_pretty(&l).context("failed to serialize lease")?;
            println!("{json}");
        }
        None => bail!("lease {} not found", args.lease_id),
    }

    Ok(())
}

fn cmd_close(args: LeaseCloseArgs) -> anyhow::Result<()> {
    let store = StateStore::open(&args.state_db).context("failed to open state store")?;
    let mut lease = store
        .load_lease(&args.lease_id)
        .context("failed to load lease")?
        .ok_or_else(|| anyhow::anyhow!("lease {} not found", args.lease_id))?;

    if lease.state == LeaseState::Closed {
        println!("Lease {} is already closed.", args.lease_id);
        return Ok(());
    }

    lease
        .transition_to(LeaseState::Closed)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    store.save_lease(&lease).context("failed to save lease")?;

    println!("Lease {} closed.", args.lease_id);

    Ok(())
}
