//! `vz execution` -- execution lifecycle management commands.
//!
//! Provides `list`, `inspect`, and `cancel` subcommands backed by the
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
struct ApiExecutionPayload {
    execution_id: String,
    container_id: String,
    state: String,
    exit_code: Option<i32>,
    started_at: Option<u64>,
    ended_at: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ApiExecutionResponse {
    execution: ApiExecutionPayload,
}

#[derive(Debug, Deserialize)]
struct ApiExecutionListResponse {
    executions: Vec<ApiExecutionPayload>,
}

fn execution_payload_from_api(payload: ApiExecutionPayload) -> runtime_v2::ExecutionPayload {
    runtime_v2::ExecutionPayload {
        execution_id: payload.execution_id,
        container_id: payload.container_id,
        state: payload.state,
        exit_code: payload.exit_code.unwrap_or(0),
        started_at: payload.started_at.unwrap_or(0),
        ended_at: payload.ended_at.unwrap_or(0),
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

async fn api_list_executions() -> anyhow::Result<Vec<runtime_v2::ExecutionPayload>> {
    let url = runtime_api_url("/v1/executions")?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api list executions")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to list executions via api").await);
    }
    let payload: ApiExecutionListResponse = response
        .json()
        .await
        .context("failed to decode api list executions response")?;
    Ok(payload
        .executions
        .into_iter()
        .map(execution_payload_from_api)
        .collect())
}

async fn api_get_execution(
    execution_id: &str,
) -> anyhow::Result<Option<runtime_v2::ExecutionPayload>> {
    let url = runtime_api_url(&format!("/v1/executions/{execution_id}"))?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api get execution")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to get execution via api").await);
    }
    let payload: ApiExecutionResponse = response
        .json()
        .await
        .context("failed to decode api get execution response")?;
    Ok(Some(execution_payload_from_api(payload.execution)))
}

async fn api_cancel_execution(
    execution_id: &str,
) -> anyhow::Result<Option<runtime_v2::ExecutionPayload>> {
    let url = runtime_api_url(&format!("/v1/executions/{execution_id}"))?;
    let response = reqwest::Client::new()
        .delete(url)
        .send()
        .await
        .context("failed to call api cancel execution")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to cancel execution via api").await);
    }
    let payload: ApiExecutionResponse = response
        .json()
        .await
        .context("failed to decode api cancel execution response")?;
    Ok(Some(execution_payload_from_api(payload.execution)))
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
    let mut executions = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            client
                .list_executions(runtime_v2::ListExecutionsRequest { metadata: None })
                .await
                .context("failed to list executions via daemon")?
                .executions
        }
        ControlPlaneTransport::ApiHttp => api_list_executions().await?,
    };

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
    let payload = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
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
                Err(error) => {
                    return Err(anyhow!(error).context("failed to load execution via daemon"));
                }
            };
            response
                .execution
                .ok_or_else(|| anyhow!("daemon returned missing execution payload"))?
        }
        ControlPlaneTransport::ApiHttp => api_get_execution(&args.execution_id)
            .await?
            .ok_or_else(|| anyhow!("execution {} not found", args.execution_id))?,
    };
    let json = serde_json::to_string_pretty(&execution_json(&payload))
        .context("failed to serialize execution")?;
    println!("{json}");

    Ok(())
}

async fn cmd_cancel(args: ExecutionCancelArgs) -> anyhow::Result<()> {
    let payload = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
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
                Err(error) => {
                    return Err(anyhow!(error).context("failed to cancel execution via daemon"));
                }
            };
            response
                .execution
                .ok_or_else(|| anyhow!("daemon returned missing execution payload"))?
        }
        ControlPlaneTransport::ApiHttp => api_cancel_execution(&args.execution_id)
            .await?
            .ok_or_else(|| anyhow!("execution {} not found", args.execution_id))?,
    };
    let state = if payload.state.trim().is_empty() {
        "unknown"
    } else {
        payload.state.as_str()
    };
    println!("Execution {} state: {}.", payload.execution_id, state);

    Ok(())
}
