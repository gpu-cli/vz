//! `vz checkpoint` — checkpoint lifecycle management commands.
//!
//! Provides `list`, `inspect`, and `create` subcommands backed by the
//! runtime daemon control plane.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;

use anyhow::{Context, anyhow, bail};
use clap::{Args, Subcommand};
use reqwest::StatusCode as HttpStatusCode;
use serde::{Deserialize, Serialize};
use tonic::Code;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClientError;

use super::runtime_daemon::{
    ControlPlaneTransport, connect_control_plane_for_state_db, control_plane_transport,
    runtime_api_base_url,
};

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

    /// Restore a checkpoint.
    Restore(CheckpointRestoreArgs),

    /// Fork a checkpoint into a new sandbox snapshot lineage.
    Fork(CheckpointForkArgs),

    /// Export a checkpoint to a btrfs send stream file.
    Export(CheckpointExportArgs),

    /// Import a checkpoint from a btrfs send stream file.
    Import(CheckpointImportArgs),
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

    /// Optional retention tag. Tagged checkpoints are protected from policy GC.
    #[arg(long)]
    tag: Option<String>,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Arguments for `vz checkpoint restore`.
#[derive(Args, Debug)]
pub struct CheckpointRestoreArgs {
    /// Checkpoint identifier.
    pub checkpoint_id: String,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Arguments for `vz checkpoint fork`.
#[derive(Args, Debug)]
pub struct CheckpointForkArgs {
    /// Parent checkpoint identifier.
    pub checkpoint_id: String,

    /// Optional explicit sandbox identifier for the fork target.
    #[arg(long)]
    pub new_sandbox_id: Option<String>,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Arguments for `vz checkpoint export`.
#[derive(Args, Debug)]
pub struct CheckpointExportArgs {
    /// Checkpoint identifier.
    pub checkpoint_id: String,

    /// Absolute destination path for the exported stream file.
    #[arg(long)]
    pub stream_path: PathBuf,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    state_db: PathBuf,
}

/// Arguments for `vz checkpoint import`.
#[derive(Args, Debug)]
pub struct CheckpointImportArgs {
    /// Owning sandbox identifier.
    pub sandbox_id: String,

    /// Absolute source path for the import stream file.
    #[arg(long)]
    pub stream_path: PathBuf,

    /// Checkpoint class: fs_quick or vm_full.
    #[arg(long, default_value = "fs_quick")]
    class: String,

    /// Compatibility fingerprint string.
    #[arg(long, default_value = "unset")]
    fingerprint: String,

    /// Optional retention tag. Tagged checkpoints are protected from policy GC.
    #[arg(long)]
    tag: Option<String>,

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
        CheckpointCommand::Restore(restore_args) => cmd_restore(restore_args).await,
        CheckpointCommand::Fork(fork_args) => cmd_fork(fork_args).await,
        CheckpointCommand::Export(export_args) => cmd_export(export_args).await,
        CheckpointCommand::Import(import_args) => cmd_import(import_args).await,
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
struct ApiCheckpointPayload {
    checkpoint_id: String,
    sandbox_id: String,
    parent_checkpoint_id: Option<String>,
    class: String,
    state: String,
    compatibility_fingerprint: String,
    created_at: u64,
    retention_tag: Option<String>,
    retention_protected: Option<bool>,
    retention_gc_reason: Option<String>,
    retention_expires_at: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ApiCheckpointResponse {
    checkpoint: ApiCheckpointPayload,
}

#[derive(Debug, Deserialize)]
struct ApiCheckpointListResponse {
    checkpoints: Vec<ApiCheckpointPayload>,
}

#[derive(Debug, Serialize)]
struct ApiCreateCheckpointRequest {
    sandbox_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compatibility_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retention_tag: Option<String>,
}

#[derive(Debug, Serialize)]
struct ApiForkCheckpointRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    new_sandbox_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct ApiExportCheckpointRequest {
    stream_path: String,
}

#[derive(Debug, Deserialize)]
struct ApiExportCheckpointResponse {
    checkpoint_id: String,
    stream_path: String,
}

#[derive(Debug, Serialize)]
struct ApiImportCheckpointRequest {
    sandbox_id: String,
    stream_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compatibility_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retention_tag: Option<String>,
}

fn checkpoint_payload_from_api(payload: ApiCheckpointPayload) -> runtime_v2::CheckpointPayload {
    runtime_v2::CheckpointPayload {
        checkpoint_id: payload.checkpoint_id,
        sandbox_id: payload.sandbox_id,
        parent_checkpoint_id: payload.parent_checkpoint_id.unwrap_or_default(),
        checkpoint_class: payload.class,
        state: payload.state,
        compatibility_fingerprint: payload.compatibility_fingerprint,
        created_at: payload.created_at,
        retention_tag: payload.retention_tag.unwrap_or_default(),
        retention_protected: payload.retention_protected.unwrap_or(false),
        retention_gc_reason: payload.retention_gc_reason.unwrap_or_default(),
        retention_expires_at: payload.retention_expires_at.unwrap_or_default(),
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

async fn api_list_checkpoints() -> anyhow::Result<Vec<runtime_v2::CheckpointPayload>> {
    let url = runtime_api_url("/v1/checkpoints")?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api list checkpoints")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to list checkpoints via api").await);
    }
    let payload: ApiCheckpointListResponse = response
        .json()
        .await
        .context("failed to decode api list checkpoints response")?;
    Ok(payload
        .checkpoints
        .into_iter()
        .map(checkpoint_payload_from_api)
        .collect())
}

async fn api_get_checkpoint(
    checkpoint_id: &str,
) -> anyhow::Result<Option<runtime_v2::CheckpointPayload>> {
    let url = runtime_api_url(&format!("/v1/checkpoints/{checkpoint_id}"))?;
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .context("failed to call api get checkpoint")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to get checkpoint via api").await);
    }
    let payload: ApiCheckpointResponse = response
        .json()
        .await
        .context("failed to decode api get checkpoint response")?;
    Ok(Some(checkpoint_payload_from_api(payload.checkpoint)))
}

async fn api_create_checkpoint(
    sandbox_id: String,
    checkpoint_class: String,
    fingerprint: String,
    retention_tag: Option<String>,
) -> anyhow::Result<runtime_v2::CheckpointPayload> {
    let url = runtime_api_url("/v1/checkpoints")?;
    let body = ApiCreateCheckpointRequest {
        sandbox_id,
        class: Some(checkpoint_class),
        compatibility_fingerprint: Some(fingerprint),
        retention_tag,
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api create checkpoint")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to create checkpoint via api").await);
    }
    let payload: ApiCheckpointResponse = response
        .json()
        .await
        .context("failed to decode api create checkpoint response")?;
    Ok(checkpoint_payload_from_api(payload.checkpoint))
}

async fn api_restore_checkpoint(
    checkpoint_id: &str,
) -> anyhow::Result<runtime_v2::CheckpointPayload> {
    let url = runtime_api_url(&format!("/v1/checkpoints/{checkpoint_id}/restore"))?;
    let response = reqwest::Client::new()
        .post(url)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("failed to call api restore checkpoint")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to restore checkpoint via api").await);
    }
    let payload: ApiCheckpointResponse = response
        .json()
        .await
        .context("failed to decode api restore checkpoint response")?;
    Ok(checkpoint_payload_from_api(payload.checkpoint))
}

async fn api_fork_checkpoint(
    checkpoint_id: &str,
    new_sandbox_id: Option<String>,
) -> anyhow::Result<runtime_v2::CheckpointPayload> {
    let url = runtime_api_url(&format!("/v1/checkpoints/{checkpoint_id}/fork"))?;
    let body = ApiForkCheckpointRequest { new_sandbox_id };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api fork checkpoint")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to fork checkpoint via api").await);
    }
    let payload: ApiCheckpointResponse = response
        .json()
        .await
        .context("failed to decode api fork checkpoint response")?;
    Ok(checkpoint_payload_from_api(payload.checkpoint))
}

async fn api_export_checkpoint(
    checkpoint_id: &str,
    stream_path: &std::path::Path,
) -> anyhow::Result<runtime_v2::ExportCheckpointCompletion> {
    let url = runtime_api_url(&format!("/v1/checkpoints/{checkpoint_id}/export"))?;
    let body = ApiExportCheckpointRequest {
        stream_path: stream_path.display().to_string(),
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api export checkpoint")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to export checkpoint via api").await);
    }
    let payload: ApiExportCheckpointResponse = response
        .json()
        .await
        .context("failed to decode api export checkpoint response")?;
    Ok(runtime_v2::ExportCheckpointCompletion {
        checkpoint_id: payload.checkpoint_id,
        stream_path: payload.stream_path,
    })
}

async fn api_import_checkpoint(
    sandbox_id: String,
    stream_path: &std::path::Path,
    checkpoint_class: String,
    fingerprint: String,
    retention_tag: Option<String>,
) -> anyhow::Result<runtime_v2::CheckpointPayload> {
    let url = runtime_api_url("/v1/checkpoints/import")?;
    let body = ApiImportCheckpointRequest {
        sandbox_id,
        stream_path: stream_path.display().to_string(),
        class: Some(checkpoint_class),
        compatibility_fingerprint: Some(fingerprint),
        retention_tag,
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api import checkpoint")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to import checkpoint via api").await);
    }
    let payload: ApiCheckpointResponse = response
        .json()
        .await
        .context("failed to decode api import checkpoint response")?;
    Ok(checkpoint_payload_from_api(payload.checkpoint))
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
        "retention_tag": if payload.retention_tag.trim().is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(payload.retention_tag.clone())
        },
        "retention_protected": payload.retention_protected,
        "retention_gc_reason": if payload.retention_gc_reason.trim().is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(payload.retention_gc_reason.clone())
        },
        "retention_expires_at": if payload.retention_expires_at == 0 {
            serde_json::Value::Null
        } else {
            serde_json::Value::Number(payload.retention_expires_at.into())
        },
    })
}

async fn cmd_list(args: CheckpointListArgs) -> anyhow::Result<()> {
    let mut checkpoints = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            client
                .list_checkpoints(runtime_v2::ListCheckpointsRequest { metadata: None })
                .await
                .context("failed to list checkpoints via daemon")?
                .checkpoints
        }
        ControlPlaneTransport::ApiHttp => api_list_checkpoints().await?,
    };

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
        "{:<34} {:<22} {:<10} {:<9} {:<20} {:<10} {:<12}",
        "CHECKPOINT ID", "SANDBOX ID", "CLASS", "STATE", "FINGERPRINT", "TAG", "GC REASON"
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
        let tag = if payload.retention_tag.trim().is_empty() {
            "-"
        } else {
            payload.retention_tag.as_str()
        };
        let gc_reason = if payload.retention_gc_reason.trim().is_empty() {
            "-"
        } else {
            payload.retention_gc_reason.as_str()
        };
        println!(
            "{:<34} {:<22} {:<10} {:<9} {:<20} {:<10} {:<12}",
            payload.checkpoint_id, payload.sandbox_id, class, state, fingerprint, tag, gc_reason
        );
    }

    Ok(())
}

async fn cmd_inspect(args: CheckpointInspectArgs) -> anyhow::Result<()> {
    let payload = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
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
                Err(error) => {
                    return Err(anyhow!(error).context("failed to load checkpoint via daemon"));
                }
            };
            response
                .checkpoint
                .ok_or_else(|| anyhow!("daemon returned missing checkpoint payload"))?
        }
        ControlPlaneTransport::ApiHttp => api_get_checkpoint(&args.checkpoint_id)
            .await?
            .ok_or_else(|| anyhow!("checkpoint {} not found", args.checkpoint_id))?,
    };
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
    let checkpoint_class = normalize_checkpoint_class(&args.class)?;
    let payload = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            let response = client
                .create_checkpoint(runtime_v2::CreateCheckpointRequest {
                    metadata: None,
                    sandbox_id: args.sandbox_id,
                    checkpoint_class,
                    compatibility_fingerprint: args.fingerprint,
                    retention_tag: args.tag.unwrap_or_default(),
                })
                .await
                .context("failed to create checkpoint via daemon")?;
            response
                .checkpoint
                .ok_or_else(|| anyhow!("daemon returned missing checkpoint payload"))?
        }
        ControlPlaneTransport::ApiHttp => {
            api_create_checkpoint(
                args.sandbox_id,
                checkpoint_class,
                args.fingerprint,
                args.tag,
            )
            .await?
        }
    };
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

async fn cmd_restore(args: CheckpointRestoreArgs) -> anyhow::Result<()> {
    let payload = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            let response = client
                .restore_checkpoint(runtime_v2::RestoreCheckpointRequest {
                    checkpoint_id: args.checkpoint_id.clone(),
                    metadata: None,
                })
                .await
                .context("failed to restore checkpoint via daemon")?;
            response
                .checkpoint
                .ok_or_else(|| anyhow!("daemon returned missing checkpoint payload"))?
        }
        ControlPlaneTransport::ApiHttp => api_restore_checkpoint(&args.checkpoint_id).await?,
    };
    println!(
        "Checkpoint {} restored for sandbox {}.",
        payload.checkpoint_id, payload.sandbox_id
    );
    Ok(())
}

async fn cmd_fork(args: CheckpointForkArgs) -> anyhow::Result<()> {
    let payload = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            let response = client
                .fork_checkpoint(runtime_v2::ForkCheckpointRequest {
                    checkpoint_id: args.checkpoint_id.clone(),
                    new_sandbox_id: args.new_sandbox_id.unwrap_or_default(),
                    metadata: None,
                })
                .await
                .context("failed to fork checkpoint via daemon")?;
            response
                .checkpoint
                .ok_or_else(|| anyhow!("daemon returned missing checkpoint payload"))?
        }
        ControlPlaneTransport::ApiHttp => {
            api_fork_checkpoint(&args.checkpoint_id, args.new_sandbox_id).await?
        }
    };
    println!(
        "Checkpoint {} forked to sandbox {} as checkpoint {}.",
        args.checkpoint_id, payload.sandbox_id, payload.checkpoint_id
    );
    Ok(())
}

async fn cmd_export(args: CheckpointExportArgs) -> anyhow::Result<()> {
    let stream_path = args.stream_path.display().to_string();
    let exported = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            client
                .export_checkpoint(runtime_v2::ExportCheckpointRequest {
                    checkpoint_id: args.checkpoint_id.clone(),
                    stream_path: stream_path.clone(),
                    metadata: None,
                })
                .await
                .context("failed to export checkpoint via daemon")?
        }
        ControlPlaneTransport::ApiHttp => {
            api_export_checkpoint(&args.checkpoint_id, &args.stream_path).await?
        }
    };
    println!(
        "Checkpoint {} exported to {}.",
        exported.checkpoint_id, exported.stream_path
    );
    Ok(())
}

async fn cmd_import(args: CheckpointImportArgs) -> anyhow::Result<()> {
    let checkpoint_class = normalize_checkpoint_class(&args.class)?;
    let payload = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            let response = client
                .import_checkpoint(runtime_v2::ImportCheckpointRequest {
                    sandbox_id: args.sandbox_id,
                    stream_path: args.stream_path.display().to_string(),
                    checkpoint_class,
                    compatibility_fingerprint: args.fingerprint,
                    retention_tag: args.tag.unwrap_or_default(),
                    metadata: None,
                })
                .await
                .context("failed to import checkpoint via daemon")?;
            response
                .checkpoint
                .ok_or_else(|| anyhow!("daemon returned missing checkpoint payload"))?
        }
        ControlPlaneTransport::ApiHttp => {
            api_import_checkpoint(
                args.sandbox_id,
                &args.stream_path,
                checkpoint_class,
                args.fingerprint,
                args.tag,
            )
            .await?
        }
    };
    println!(
        "Checkpoint imported for sandbox {} as checkpoint {}.",
        payload.sandbox_id, payload.checkpoint_id
    );
    Ok(())
}
