//! `vz lease` -- lease lifecycle management commands.
//!
//! Provides `list`, `inspect`, and `close` subcommands backed by the
//! runtime daemon control plane.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;

use anyhow::{Context, anyhow, bail};
use clap::{Args, Subcommand};
use reqwest::StatusCode as HttpStatusCode;
use serde::Deserialize;
use tonic::Code;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClientError;

use super::runtime_daemon::{
    ControlPlaneTransport, connect_control_plane_for_state_db, control_plane_transport,
    runtime_api_base_url,
};

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

#[derive(Debug, Deserialize)]
struct ApiErrorPayload {
    code: String,
    message: String,
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct ApiErrorEnvelope {
    error: ApiErrorPayload,
}

#[derive(Debug, Deserialize)]
struct ApiLeasePayload {
    lease_id: String,
    sandbox_id: String,
    ttl_secs: u64,
    last_heartbeat_at: u64,
    state: String,
}

#[derive(Debug, Deserialize)]
struct ApiLeaseResponse {
    lease: ApiLeasePayload,
}

#[derive(Debug, Deserialize)]
struct ApiLeaseListResponse {
    leases: Vec<ApiLeasePayload>,
}

fn lease_payload_from_api(payload: ApiLeasePayload) -> runtime_v2::LeasePayload {
    runtime_v2::LeasePayload {
        lease_id: payload.lease_id,
        sandbox_id: payload.sandbox_id,
        ttl_secs: payload.ttl_secs,
        last_heartbeat_at: payload.last_heartbeat_at,
        state: payload.state,
    }
}

fn runtime_api_url(path: &str) -> anyhow::Result<String> {
    let base = runtime_api_base_url()?;
    Ok(format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    ))
}

async fn api_error_response(response: reqwest::Response, context: &str) -> anyhow::Error {
    let status = response.status();
    let body = response.bytes().await.unwrap_or_default();
    if let Ok(error) = serde_json::from_slice::<ApiErrorEnvelope>(&body) {
        return anyhow!(
            "{context}: api error {} {} (request_id={})",
            error.error.code,
            error.error.message,
            error.error.request_id
        );
    }
    let snippet = String::from_utf8_lossy(&body);
    anyhow!("{context}: api status {status} body={snippet}")
}

async fn api_list_leases() -> anyhow::Result<Vec<runtime_v2::LeasePayload>> {
    let url = runtime_api_url("/v1/leases")?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api list leases")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to list leases via api").await);
    }
    let payload: ApiLeaseListResponse = response
        .json()
        .await
        .context("failed to decode api list leases response")?;
    Ok(payload
        .leases
        .into_iter()
        .map(lease_payload_from_api)
        .collect())
}

async fn api_get_lease(lease_id: &str) -> anyhow::Result<Option<runtime_v2::LeasePayload>> {
    let url = runtime_api_url(&format!("/v1/leases/{lease_id}"))?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api get lease")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to get lease via api").await);
    }
    let payload: ApiLeaseResponse = response
        .json()
        .await
        .context("failed to decode api get lease response")?;
    Ok(Some(lease_payload_from_api(payload.lease)))
}

async fn api_close_lease(lease_id: &str) -> anyhow::Result<Option<runtime_v2::LeasePayload>> {
    let url = runtime_api_url(&format!("/v1/leases/{lease_id}"))?;
    let response = reqwest::Client::new()
        .delete(url)
        .send()
        .await
        .context("failed to call api close lease")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to close lease via api").await);
    }
    let payload: ApiLeaseResponse = response
        .json()
        .await
        .context("failed to decode api close lease response")?;
    Ok(Some(lease_payload_from_api(payload.lease)))
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
    let leases = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            client
                .list_leases(runtime_v2::ListLeasesRequest { metadata: None })
                .await
                .context("failed to list leases via daemon")?
                .leases
        }
        ControlPlaneTransport::ApiHttp => api_list_leases().await?,
    };

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
    let payload = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
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
                Err(error) => {
                    return Err(anyhow!(error).context("failed to load lease via daemon"));
                }
            };
            response
                .lease
                .ok_or_else(|| anyhow!("daemon returned missing lease payload"))?
        }
        ControlPlaneTransport::ApiHttp => api_get_lease(&args.lease_id)
            .await?
            .ok_or_else(|| anyhow!("lease {} not found", args.lease_id))?,
    };
    let json =
        serde_json::to_string_pretty(&lease_json(&payload)).context("failed to serialize lease")?;
    println!("{json}");

    Ok(())
}

async fn cmd_close(args: LeaseCloseArgs) -> anyhow::Result<()> {
    let payload = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
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
                Err(error) => {
                    return Err(anyhow!(error).context("failed to close lease via daemon"));
                }
            };
            response
                .lease
                .ok_or_else(|| anyhow!("daemon returned missing lease payload"))?
        }
        ControlPlaneTransport::ApiHttp => api_close_lease(&args.lease_id)
            .await?
            .ok_or_else(|| anyhow!("lease {} not found", args.lease_id))?,
    };
    let state = if payload.state.trim().is_empty() {
        "unknown"
    } else {
        payload.state.as_str()
    };
    println!("Lease {} state: {}.", payload.lease_id, state);

    Ok(())
}
