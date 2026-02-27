//! `vz lease` -- lease lifecycle management commands.
//!
//! Provides `list`, `inspect`, and `close` subcommands backed by the
//! runtime daemon control plane.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;

use anyhow::{Context, anyhow, bail};
use clap::{Args, Subcommand};
use tonic::Code;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClientError;

use super::runtime_daemon::connect_control_plane_for_state_db;

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
        LeaseCommand::List(list_args) => cmd_list(list_args).await,
        LeaseCommand::Inspect(inspect_args) => cmd_inspect(inspect_args).await,
        LeaseCommand::Close(close_args) => cmd_close(close_args).await,
    }
}

fn lease_json(payload: &runtime_v2::LeasePayload) -> serde_json::Value {
    serde_json::json!({
        "lease_id": payload.lease_id,
        "sandbox_id": payload.sandbox_id,
        "ttl_secs": payload.ttl_secs,
        "last_heartbeat_at": payload.last_heartbeat_at,
        "state": payload.state,
    })
}

async fn cmd_list(args: LeaseListArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let leases = client
        .list_leases(runtime_v2::ListLeasesRequest { metadata: None })
        .await
        .context("failed to list leases via daemon")?
        .leases;

    if args.json {
        let payload: Vec<_> = leases.iter().map(lease_json).collect();
        let json = serde_json::to_string_pretty(&payload).context("failed to serialize leases")?;
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
    for payload in &leases {
        let state = if payload.state.trim().is_empty() {
            "unknown"
        } else {
            payload.state.as_str()
        };
        println!(
            "{:<40} {:<40} {:<10} {:<10} {:<20}",
            payload.lease_id,
            payload.sandbox_id,
            state,
            payload.ttl_secs,
            payload.last_heartbeat_at
        );
    }

    Ok(())
}

async fn cmd_inspect(args: LeaseInspectArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .get_lease(runtime_v2::GetLeaseRequest {
            lease_id: args.lease_id.clone(),
            metadata: None,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("lease {} not found", args.lease_id)
        }
        Err(error) => return Err(anyhow!(error).context("failed to load lease via daemon")),
    };

    let payload = response
        .lease
        .ok_or_else(|| anyhow!("daemon returned missing lease payload"))?;
    let json =
        serde_json::to_string_pretty(&lease_json(&payload)).context("failed to serialize lease")?;
    println!("{json}");

    Ok(())
}

async fn cmd_close(args: LeaseCloseArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .close_lease(runtime_v2::CloseLeaseRequest {
            lease_id: args.lease_id.clone(),
            metadata: None,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("lease {} not found", args.lease_id)
        }
        Err(error) => return Err(anyhow!(error).context("failed to close lease via daemon")),
    };

    let payload = response
        .lease
        .ok_or_else(|| anyhow!("daemon returned missing lease payload"))?;
    let state = if payload.state.trim().is_empty() {
        "unknown"
    } else {
        payload.state.as_str()
    };
    println!("Lease {} state: {}.", payload.lease_id, state);

    Ok(())
}
