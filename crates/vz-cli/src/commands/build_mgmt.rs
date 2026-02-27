//! `vz build-mgmt` -- build entity lifecycle management commands.
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
        BuildMgmtCommand::List(list_args) => cmd_list(list_args).await,
        BuildMgmtCommand::Inspect(inspect_args) => cmd_inspect(inspect_args).await,
        BuildMgmtCommand::Cancel(cancel_args) => cmd_cancel(cancel_args).await,
    }
}

fn build_json(payload: &runtime_v2::BuildPayload) -> serde_json::Value {
    serde_json::json!({
        "build_id": payload.build_id,
        "sandbox_id": payload.sandbox_id,
        "state": payload.state,
        "result_digest": if payload.result_digest.trim().is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(payload.result_digest.clone())
        },
        "started_at": payload.started_at,
        "ended_at": if payload.ended_at == 0 {
            serde_json::Value::Null
        } else {
            serde_json::Value::Number(payload.ended_at.into())
        },
    })
}

async fn cmd_list(args: BuildMgmtListArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let mut builds = client
        .list_builds(runtime_v2::ListBuildsRequest { metadata: None })
        .await
        .context("failed to list builds via daemon")?
        .builds;

    if let Some(sandbox_id) = args.sandbox_id.as_deref() {
        builds.retain(|build| build.sandbox_id == sandbox_id);
    }

    if args.json {
        let payload: Vec<_> = builds.iter().map(build_json).collect();
        let json = serde_json::to_string_pretty(&payload).context("failed to serialize builds")?;
        println!("{json}");
        return Ok(());
    }

    if builds.is_empty() {
        println!("No builds found.");
        return Ok(());
    }

    println!(
        "{:<40} {:<20} {:<12} {:<12} {:<12}",
        "BUILD ID", "SANDBOX ID", "STATE", "DIGEST", "STARTED"
    );
    for payload in &builds {
        let state = if payload.state.trim().is_empty() {
            "unknown"
        } else {
            payload.state.as_str()
        };
        let digest = if payload.result_digest.trim().is_empty() {
            "-"
        } else {
            payload.result_digest.as_str()
        };
        let digest_display = if digest.len() > 10 {
            format!("{}...", &digest[..7])
        } else {
            digest.to_string()
        };
        let started = if payload.started_at == 0 {
            "-".to_string()
        } else {
            payload.started_at.to_string()
        };
        println!(
            "{:<40} {:<20} {:<12} {:<12} {:<12}",
            payload.build_id, payload.sandbox_id, state, digest_display, started
        );
    }

    Ok(())
}

async fn cmd_inspect(args: BuildMgmtInspectArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .get_build(runtime_v2::GetBuildRequest {
            build_id: args.build_id.clone(),
            metadata: None,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("build {} not found", args.build_id)
        }
        Err(error) => return Err(anyhow!(error).context("failed to load build via daemon")),
    };

    let payload = response
        .build
        .ok_or_else(|| anyhow!("daemon returned missing build payload"))?;
    let json =
        serde_json::to_string_pretty(&build_json(&payload)).context("failed to serialize build")?;
    println!("{json}");

    Ok(())
}

async fn cmd_cancel(args: BuildMgmtCancelArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .cancel_build(runtime_v2::CancelBuildRequest {
            build_id: args.build_id.clone(),
            metadata: None,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("build {} not found", args.build_id)
        }
        Err(error) => return Err(anyhow!(error).context("failed to cancel build via daemon")),
    };

    let payload = response
        .build
        .ok_or_else(|| anyhow!("daemon returned missing build payload"))?;
    let state = if payload.state.trim().is_empty() {
        "unknown"
    } else {
        payload.state.as_str()
    };
    println!("Build {} state: {}.", payload.build_id, state);

    Ok(())
}
