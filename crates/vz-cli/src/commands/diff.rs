//! `vz diff` — deterministic diff contract over checkpoint metadata.
//!
//! This provides the first versioned diff contract while backend file-level
//! checkpoint deltas are still unavailable. Default mode emits a deterministic
//! file-summary projection; expanded modes expose structured system/patch
//! evidence from checkpoint metadata differences.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;

use anyhow::{Context, anyhow, bail};
use clap::{Args, ValueEnum};
use reqwest::StatusCode as HttpStatusCode;
use serde::{Deserialize, Serialize};
use tonic::Code;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClientError;

use super::runtime_daemon::{
    ControlPlaneTransport, connect_control_plane_for_state_db, control_plane_transport,
    runtime_api_base_url,
};

const DIFF_SCHEMA_VERSION: u16 = 1;

/// Compare two checkpoints using the versioned diff contract.
#[derive(Args, Debug)]
pub struct DiffArgs {
    /// Base checkpoint identifier.
    pub from_checkpoint_id: String,

    /// Target checkpoint identifier.
    pub to_checkpoint_id: String,

    /// Output mode.
    #[arg(long, value_enum, default_value = "file_summary")]
    pub mode: DiffMode,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    pub state_db: PathBuf,

    /// Output JSON envelope.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
#[value(rename_all = "snake_case")]
pub enum DiffMode {
    FileSummary,
    Patch,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ApiErrorPayload {
    code: String,
    message: String,
    request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ApiErrorEnvelope {
    error: ApiErrorPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ApiCheckpointPayload {
    checkpoint_id: String,
    sandbox_id: String,
    parent_checkpoint_id: Option<String>,
    class: String,
    state: String,
    compatibility_fingerprint: String,
    created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ApiCheckpointResponse {
    checkpoint: ApiCheckpointPayload,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DiffEnvelopeV1 {
    schema_version: u16,
    mode: String,
    evidence_source: String,
    from_checkpoint_id: String,
    to_checkpoint_id: String,
    file_summary: Vec<FileSummaryEntry>,
    patch: Vec<PatchEntry>,
    system: Vec<SystemDiffEntry>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct FileSummaryEntry {
    path: String,
    change: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct PatchEntry {
    path: String,
    before: String,
    after: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SystemDiffEntry {
    field: String,
    before: String,
    after: String,
}

pub async fn run(args: DiffArgs) -> anyhow::Result<()> {
    let from = load_checkpoint(args.state_db.as_path(), args.from_checkpoint_id.as_str()).await?;
    let to = load_checkpoint(args.state_db.as_path(), args.to_checkpoint_id.as_str()).await?;
    let envelope = build_diff_envelope(&from, &to, args.mode);

    if args.json {
        let json =
            serde_json::to_string_pretty(&envelope).context("failed to serialize diff envelope")?;
        println!("{json}");
        return Ok(());
    }

    println!(
        "Diff v{} {} -> {} ({})",
        envelope.schema_version,
        envelope.from_checkpoint_id,
        envelope.to_checkpoint_id,
        envelope.mode
    );
    if envelope.file_summary.is_empty() {
        println!("No differences.");
        return Ok(());
    }

    println!("Files:");
    for entry in &envelope.file_summary {
        println!("  {} {}", entry.change, entry.path);
    }
    if matches!(args.mode, DiffMode::Patch) {
        println!("Patch:");
        for entry in &envelope.patch {
            println!("  {}", entry.path);
            println!("    - {}", entry.before);
            println!("    + {}", entry.after);
        }
    }
    if matches!(args.mode, DiffMode::System | DiffMode::Patch) {
        println!("System:");
        for entry in &envelope.system {
            println!("  {}: {} -> {}", entry.field, entry.before, entry.after);
        }
    }

    Ok(())
}

fn checkpoint_from_api(payload: ApiCheckpointPayload) -> runtime_v2::CheckpointPayload {
    runtime_v2::CheckpointPayload {
        checkpoint_id: payload.checkpoint_id,
        sandbox_id: payload.sandbox_id,
        parent_checkpoint_id: payload.parent_checkpoint_id.unwrap_or_default(),
        checkpoint_class: payload.class,
        state: payload.state,
        compatibility_fingerprint: payload.compatibility_fingerprint,
        created_at: payload.created_at,
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
    Ok(Some(checkpoint_from_api(payload.checkpoint)))
}

async fn load_checkpoint(
    state_db: &std::path::Path,
    checkpoint_id: &str,
) -> anyhow::Result<runtime_v2::CheckpointPayload> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(state_db).await?;
            match client
                .get_checkpoint(runtime_v2::GetCheckpointRequest {
                    checkpoint_id: checkpoint_id.to_string(),
                    metadata: None,
                })
                .await
            {
                Ok(response) => response
                    .checkpoint
                    .ok_or_else(|| anyhow!("daemon returned missing checkpoint payload")),
                Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
                    bail!("checkpoint {} not found", checkpoint_id)
                }
                Err(error) => Err(anyhow!(error).context("failed to load checkpoint via daemon")),
            }
        }
        ControlPlaneTransport::ApiHttp => api_get_checkpoint(checkpoint_id)
            .await?
            .ok_or_else(|| anyhow!("checkpoint {} not found", checkpoint_id)),
    }
}

fn normalize_opt(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "<none>".to_string()
    } else {
        trimmed.to_string()
    }
}

fn collect_system_diffs(
    from: &runtime_v2::CheckpointPayload,
    to: &runtime_v2::CheckpointPayload,
) -> Vec<SystemDiffEntry> {
    let mut diffs = Vec::new();
    let candidates = [
        (
            "compatibility_fingerprint",
            from.compatibility_fingerprint.clone(),
            to.compatibility_fingerprint.clone(),
        ),
        (
            "created_at",
            from.created_at.to_string(),
            to.created_at.to_string(),
        ),
        (
            "parent_checkpoint_id",
            normalize_opt(from.parent_checkpoint_id.as_str()),
            normalize_opt(to.parent_checkpoint_id.as_str()),
        ),
        ("sandbox_id", from.sandbox_id.clone(), to.sandbox_id.clone()),
        (
            "checkpoint_class",
            from.checkpoint_class.clone(),
            to.checkpoint_class.clone(),
        ),
        ("state", from.state.clone(), to.state.clone()),
    ];

    for (field, before, after) in candidates {
        if before != after {
            diffs.push(SystemDiffEntry {
                field: field.to_string(),
                before,
                after,
            });
        }
    }
    diffs.sort_by(|a, b| a.field.cmp(&b.field));
    diffs
}

fn build_diff_envelope(
    from: &runtime_v2::CheckpointPayload,
    to: &runtime_v2::CheckpointPayload,
    mode: DiffMode,
) -> DiffEnvelopeV1 {
    let system = collect_system_diffs(from, to);
    let file_summary: Vec<FileSummaryEntry> = system
        .iter()
        .map(|entry| FileSummaryEntry {
            path: format!("/.system/checkpoint/{}", entry.field),
            change: "M".to_string(),
        })
        .collect();
    let patch: Vec<PatchEntry> = system
        .iter()
        .map(|entry| PatchEntry {
            path: format!("/.system/checkpoint/{}", entry.field),
            before: entry.before.clone(),
            after: entry.after.clone(),
        })
        .collect();

    DiffEnvelopeV1 {
        schema_version: DIFF_SCHEMA_VERSION,
        mode: match mode {
            DiffMode::FileSummary => "file_summary".to_string(),
            DiffMode::Patch => "patch".to_string(),
            DiffMode::System => "system".to_string(),
        },
        evidence_source: "checkpoint_metadata".to_string(),
        from_checkpoint_id: from.checkpoint_id.clone(),
        to_checkpoint_id: to.checkpoint_id.clone(),
        file_summary,
        patch: if matches!(mode, DiffMode::Patch) {
            patch
        } else {
            Vec::new()
        },
        system: if matches!(mode, DiffMode::System | DiffMode::Patch) {
            system
        } else {
            Vec::new()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(
        id: &str,
        class: &str,
        state: &str,
        fingerprint: &str,
    ) -> runtime_v2::CheckpointPayload {
        runtime_v2::CheckpointPayload {
            checkpoint_id: id.to_string(),
            sandbox_id: "sandbox-a".to_string(),
            parent_checkpoint_id: "cp-parent".to_string(),
            checkpoint_class: class.to_string(),
            state: state.to_string(),
            compatibility_fingerprint: fingerprint.to_string(),
            created_at: 1,
        }
    }

    #[test]
    fn diff_envelope_schema_version_is_stable() {
        let from = payload("cp-a", "fs_quick", "ready", "fp-1");
        let to = payload("cp-b", "vm_full", "ready", "fp-2");
        let envelope = build_diff_envelope(&from, &to, DiffMode::FileSummary);
        assert_eq!(envelope.schema_version, DIFF_SCHEMA_VERSION);
        assert_eq!(envelope.mode, "file_summary");
        assert_eq!(envelope.evidence_source, "checkpoint_metadata");
    }

    #[test]
    fn file_summary_order_is_deterministic() {
        let from = payload("cp-a", "fs_quick", "creating", "fp-1");
        let mut to = payload("cp-b", "vm_full", "ready", "fp-2");
        to.sandbox_id = "sandbox-z".to_string();
        to.parent_checkpoint_id = "".to_string();
        to.created_at = 5;

        let envelope = build_diff_envelope(&from, &to, DiffMode::FileSummary);
        let paths: Vec<_> = envelope
            .file_summary
            .iter()
            .map(|e| e.path.as_str())
            .collect();
        assert_eq!(
            paths,
            vec![
                "/.system/checkpoint/checkpoint_class",
                "/.system/checkpoint/compatibility_fingerprint",
                "/.system/checkpoint/created_at",
                "/.system/checkpoint/parent_checkpoint_id",
                "/.system/checkpoint/sandbox_id",
                "/.system/checkpoint/state",
            ]
        );
    }

    #[test]
    fn system_mode_emits_structured_field_diffs() {
        let from = payload("cp-a", "fs_quick", "creating", "fp-1");
        let to = payload("cp-b", "vm_full", "ready", "fp-2");
        let envelope = build_diff_envelope(&from, &to, DiffMode::System);
        assert!(!envelope.system.is_empty());
        assert!(envelope.patch.is_empty());
        assert_eq!(envelope.system[0].field, "checkpoint_class".to_string());
    }
}
