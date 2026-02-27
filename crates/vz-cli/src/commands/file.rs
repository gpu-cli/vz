//! `vz file` -- sandbox filesystem primitives through runtime daemon.
//!
//! Provides read/write/list and path mutation operations backed by
//! Runtime V2 FileService.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{Context, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
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

/// Sandbox filesystem management.
#[derive(Args, Debug)]
pub struct FileArgs {
    #[command(subcommand)]
    pub action: FileCommand,
}

#[derive(Subcommand, Debug)]
pub enum FileCommand {
    /// List files/directories under a sandbox path.
    List(FileListArgs),

    /// Read file bytes from a sandbox path.
    Read(FileReadArgs),

    /// Write file bytes to a sandbox path.
    Write(FileWriteArgs),

    /// Create a directory.
    Mkdir(FileMkdirArgs),

    /// Remove a file or directory.
    Rm(FileRemoveArgs),

    /// Move or rename a path.
    Mv(FileMoveArgs),

    /// Copy a path.
    Cp(FileCopyArgs),

    /// Change mode bits for a path.
    Chmod(FileChmodArgs),

    /// Change owner/group for a path.
    Chown(FileChownArgs),
}

/// Arguments for `vz debug file list`.
#[derive(Args, Debug)]
pub struct FileListArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Relative path under sandbox root.
    #[arg(default_value = "")]
    pub path: String,

    /// Recurse into directories.
    #[arg(long)]
    pub recursive: bool,

    /// Maximum entries to return.
    #[arg(long)]
    pub limit: Option<u32>,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    pub state_db: PathBuf,

    /// Output as JSON.
    #[arg(long)]
    pub json: bool,
}

/// Arguments for `vz debug file read`.
#[derive(Args, Debug)]
pub struct FileReadArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Relative path under sandbox root.
    pub path: String,

    /// Byte offset to start reading from.
    #[arg(long, default_value = "0")]
    pub offset: u64,

    /// Maximum bytes to read.
    #[arg(long)]
    pub limit: Option<u64>,

    /// Output file path. When omitted, writes to stdout.
    #[arg(long)]
    pub output: Option<PathBuf>,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    pub state_db: PathBuf,
}

/// Arguments for `vz debug file write`.
#[derive(Args, Debug)]
pub struct FileWriteArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Relative path under sandbox root.
    pub path: String,

    /// Host file path to upload.
    #[arg(long)]
    pub from: Option<PathBuf>,

    /// Inline text bytes to write.
    #[arg(long)]
    pub text: Option<String>,

    /// Append to existing file.
    #[arg(long)]
    pub append: bool,

    /// Create parent directories if needed.
    #[arg(long)]
    pub create_parents: bool,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    pub state_db: PathBuf,
}

/// Arguments for `vz debug file mkdir`.
#[derive(Args, Debug)]
pub struct FileMkdirArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Relative directory path under sandbox root.
    pub path: String,

    /// Create parent directories if needed.
    #[arg(long)]
    pub parents: bool,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    pub state_db: PathBuf,
}

/// Arguments for `vz debug file rm`.
#[derive(Args, Debug)]
pub struct FileRemoveArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Relative path under sandbox root.
    pub path: String,

    /// Remove directories recursively.
    #[arg(long)]
    pub recursive: bool,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    pub state_db: PathBuf,
}

/// Arguments for `vz debug file mv`.
#[derive(Args, Debug)]
pub struct FileMoveArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Source relative path.
    pub src_path: String,

    /// Destination relative path.
    pub dst_path: String,

    /// Overwrite destination when present.
    #[arg(long)]
    pub overwrite: bool,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    pub state_db: PathBuf,
}

/// Arguments for `vz debug file cp`.
#[derive(Args, Debug)]
pub struct FileCopyArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Source relative path.
    pub src_path: String,

    /// Destination relative path.
    pub dst_path: String,

    /// Overwrite destination when present.
    #[arg(long)]
    pub overwrite: bool,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    pub state_db: PathBuf,
}

/// Arguments for `vz debug file chmod`.
#[derive(Args, Debug)]
pub struct FileChmodArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Relative path under sandbox root.
    pub path: String,

    /// Mode bits (octal), for example 755 or 0644.
    pub mode: String,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    pub state_db: PathBuf,
}

/// Arguments for `vz debug file chown`.
#[derive(Args, Debug)]
pub struct FileChownArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Relative path under sandbox root.
    pub path: String,

    /// User ID.
    pub uid: u32,

    /// Group ID.
    pub gid: u32,

    /// Path to the state database.
    #[arg(long, default_value = "stack-state.db")]
    pub state_db: PathBuf,
}

#[derive(Debug, Serialize)]
struct FileEntryView {
    path: String,
    is_dir: bool,
    size: u64,
    modified_at: u64,
}

impl From<runtime_v2::FileEntry> for FileEntryView {
    fn from(value: runtime_v2::FileEntry) -> Self {
        Self {
            path: value.path,
            is_dir: value.is_dir,
            size: value.size,
            modified_at: value.modified_at,
        }
    }
}

fn parse_mode_octal(input: &str) -> anyhow::Result<u32> {
    let raw = input.trim();
    let octal = raw
        .strip_prefix("0o")
        .or_else(|| raw.strip_prefix("0O"))
        .unwrap_or(raw);
    if octal.is_empty() {
        bail!("mode cannot be empty");
    }
    u32::from_str_radix(octal, 8).with_context(|| format!("invalid octal mode `{input}`"))
}

fn read_write_payload(args: &FileWriteArgs) -> anyhow::Result<Vec<u8>> {
    if args.from.is_some() && args.text.is_some() {
        bail!("choose one input source: either `--from` or `--text`");
    }

    if let Some(path) = args.from.as_ref() {
        return fs::read(path).with_context(|| format!("failed to read {}", path.display()));
    }

    if let Some(text) = args.text.as_ref() {
        return Ok(text.as_bytes().to_vec());
    }

    let mut data = Vec::new();
    std::io::stdin()
        .read_to_end(&mut data)
        .context("failed to read stdin")?;
    if data.is_empty() {
        bail!("no input data provided; use `--from`, `--text`, or pipe bytes to stdin");
    }
    Ok(data)
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

#[derive(Debug, Serialize)]
struct ApiReadFileRequest {
    sandbox_id: String,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ApiReadFileResponse {
    data_base64: String,
    truncated: bool,
}

#[derive(Debug, Serialize)]
struct ApiWriteFileRequest {
    sandbox_id: String,
    path: String,
    data_base64: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    append: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    create_parents: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ApiWriteFileResponse {
    bytes_written: u64,
}

#[derive(Debug, Serialize)]
struct ApiListFilesRequest {
    sandbox_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recursive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ApiFileEntry {
    path: String,
    is_dir: bool,
    size: u64,
    modified_at: u64,
}

#[derive(Debug, Deserialize)]
struct ApiListFilesResponse {
    entries: Vec<ApiFileEntry>,
}

#[derive(Debug, Serialize)]
struct ApiMakeDirRequest {
    sandbox_id: String,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parents: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ApiRemovePathRequest {
    sandbox_id: String,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    recursive: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ApiMovePathRequest {
    sandbox_id: String,
    src_path: String,
    dst_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    overwrite: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ApiCopyPathRequest {
    sandbox_id: String,
    src_path: String,
    dst_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    overwrite: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ApiChmodPathRequest {
    sandbox_id: String,
    path: String,
    mode: u32,
}

#[derive(Debug, Serialize)]
struct ApiChownPathRequest {
    sandbox_id: String,
    path: String,
    uid: u32,
    gid: u32,
}

#[derive(Debug, Deserialize)]
struct ApiFileMutationResponse {
    path: String,
    status: String,
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

async fn api_list_files(args: &FileListArgs) -> anyhow::Result<Vec<runtime_v2::FileEntry>> {
    let url = runtime_api_url("/v1/files/list")?;
    let body = ApiListFilesRequest {
        sandbox_id: args.sandbox_id.clone(),
        path: if args.path.is_empty() {
            None
        } else {
            Some(args.path.clone())
        },
        recursive: Some(args.recursive),
        limit: args.limit,
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api list files")?;
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to list files via api").await);
    }
    let payload: ApiListFilesResponse = response
        .json()
        .await
        .context("failed to decode api list files response")?;
    Ok(payload
        .entries
        .into_iter()
        .map(|entry| runtime_v2::FileEntry {
            path: entry.path,
            is_dir: entry.is_dir,
            size: entry.size,
            modified_at: entry.modified_at,
        })
        .collect())
}

async fn api_read_file(
    args: &FileReadArgs,
) -> anyhow::Result<Option<runtime_v2::ReadFileResponse>> {
    let url = runtime_api_url("/v1/files/read")?;
    let body = ApiReadFileRequest {
        sandbox_id: args.sandbox_id.clone(),
        path: args.path.clone(),
        offset: Some(args.offset),
        limit: args.limit,
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api read file")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to read file via api").await);
    }
    let payload: ApiReadFileResponse = response
        .json()
        .await
        .context("failed to decode api read file response")?;
    let data = BASE64_STANDARD
        .decode(payload.data_base64)
        .context("failed to decode api read file payload bytes")?;
    Ok(Some(runtime_v2::ReadFileResponse {
        request_id: String::new(),
        data,
        truncated: payload.truncated,
    }))
}

async fn api_write_file(
    args: &FileWriteArgs,
    payload: Vec<u8>,
) -> anyhow::Result<Option<runtime_v2::WriteFileResponse>> {
    let url = runtime_api_url("/v1/files/write")?;
    let body = ApiWriteFileRequest {
        sandbox_id: args.sandbox_id.clone(),
        path: args.path.clone(),
        data_base64: BASE64_STANDARD.encode(payload),
        append: Some(args.append),
        create_parents: Some(args.create_parents),
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api write file")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to write file via api").await);
    }
    let payload: ApiWriteFileResponse = response
        .json()
        .await
        .context("failed to decode api write file response")?;
    Ok(Some(runtime_v2::WriteFileResponse {
        request_id: String::new(),
        bytes_written: payload.bytes_written,
    }))
}

async fn api_make_dir(
    args: &FileMkdirArgs,
) -> anyhow::Result<Option<runtime_v2::FileMutationResponse>> {
    let url = runtime_api_url("/v1/files/mkdir")?;
    let body = ApiMakeDirRequest {
        sandbox_id: args.sandbox_id.clone(),
        path: args.path.clone(),
        parents: Some(args.parents),
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api make dir")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to make dir via api").await);
    }
    let payload: ApiFileMutationResponse = response
        .json()
        .await
        .context("failed to decode api make dir response")?;
    Ok(Some(runtime_v2::FileMutationResponse {
        request_id: String::new(),
        path: payload.path,
        status: payload.status,
    }))
}

async fn api_remove_path(
    args: &FileRemoveArgs,
) -> anyhow::Result<Option<runtime_v2::FileMutationResponse>> {
    let url = runtime_api_url("/v1/files/remove")?;
    let body = ApiRemovePathRequest {
        sandbox_id: args.sandbox_id.clone(),
        path: args.path.clone(),
        recursive: Some(args.recursive),
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api remove path")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to remove path via api").await);
    }
    let payload: ApiFileMutationResponse = response
        .json()
        .await
        .context("failed to decode api remove path response")?;
    Ok(Some(runtime_v2::FileMutationResponse {
        request_id: String::new(),
        path: payload.path,
        status: payload.status,
    }))
}

async fn api_move_path(
    args: &FileMoveArgs,
) -> anyhow::Result<Option<runtime_v2::FileMutationResponse>> {
    let url = runtime_api_url("/v1/files/move")?;
    let body = ApiMovePathRequest {
        sandbox_id: args.sandbox_id.clone(),
        src_path: args.src_path.clone(),
        dst_path: args.dst_path.clone(),
        overwrite: Some(args.overwrite),
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api move path")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to move path via api").await);
    }
    let payload: ApiFileMutationResponse = response
        .json()
        .await
        .context("failed to decode api move path response")?;
    Ok(Some(runtime_v2::FileMutationResponse {
        request_id: String::new(),
        path: payload.path,
        status: payload.status,
    }))
}

async fn api_copy_path(
    args: &FileCopyArgs,
) -> anyhow::Result<Option<runtime_v2::FileMutationResponse>> {
    let url = runtime_api_url("/v1/files/copy")?;
    let body = ApiCopyPathRequest {
        sandbox_id: args.sandbox_id.clone(),
        src_path: args.src_path.clone(),
        dst_path: args.dst_path.clone(),
        overwrite: Some(args.overwrite),
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api copy path")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to copy path via api").await);
    }
    let payload: ApiFileMutationResponse = response
        .json()
        .await
        .context("failed to decode api copy path response")?;
    Ok(Some(runtime_v2::FileMutationResponse {
        request_id: String::new(),
        path: payload.path,
        status: payload.status,
    }))
}

async fn api_chmod_path(
    args: &FileChmodArgs,
    mode: u32,
) -> anyhow::Result<Option<runtime_v2::FileMutationResponse>> {
    let url = runtime_api_url("/v1/files/chmod")?;
    let body = ApiChmodPathRequest {
        sandbox_id: args.sandbox_id.clone(),
        path: args.path.clone(),
        mode,
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api chmod path")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to chmod path via api").await);
    }
    let payload: ApiFileMutationResponse = response
        .json()
        .await
        .context("failed to decode api chmod path response")?;
    Ok(Some(runtime_v2::FileMutationResponse {
        request_id: String::new(),
        path: payload.path,
        status: payload.status,
    }))
}

async fn api_chown_path(
    args: &FileChownArgs,
) -> anyhow::Result<Option<runtime_v2::FileMutationResponse>> {
    let url = runtime_api_url("/v1/files/chown")?;
    let body = ApiChownPathRequest {
        sandbox_id: args.sandbox_id.clone(),
        path: args.path.clone(),
        uid: args.uid,
        gid: args.gid,
    };
    let response = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to call api chown path")?;
    if response.status() == HttpStatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(api_error_response(response, "failed to chown path via api").await);
    }
    let payload: ApiFileMutationResponse = response
        .json()
        .await
        .context("failed to decode api chown path response")?;
    Ok(Some(runtime_v2::FileMutationResponse {
        request_id: String::new(),
        path: payload.path,
        status: payload.status,
    }))
}

/// Run the file subcommand.
pub async fn run(args: FileArgs) -> anyhow::Result<()> {
    match args.action {
        FileCommand::List(list_args) => cmd_list(list_args).await,
        FileCommand::Read(read_args) => cmd_read(read_args).await,
        FileCommand::Write(write_args) => cmd_write(write_args).await,
        FileCommand::Mkdir(mkdir_args) => cmd_mkdir(mkdir_args).await,
        FileCommand::Rm(remove_args) => cmd_remove(remove_args).await,
        FileCommand::Mv(move_args) => cmd_move(move_args).await,
        FileCommand::Cp(copy_args) => cmd_copy(copy_args).await,
        FileCommand::Chmod(chmod_args) => cmd_chmod(chmod_args).await,
        FileCommand::Chown(chown_args) => cmd_chown(chown_args).await,
    }
}

async fn cmd_list(args: FileListArgs) -> anyhow::Result<()> {
    let entries = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            client
                .list_files(runtime_v2::ListFilesRequest {
                    metadata: None,
                    sandbox_id: args.sandbox_id.clone(),
                    path: args.path.clone(),
                    recursive: args.recursive,
                    limit: args.limit.unwrap_or(0),
                })
                .await
                .context("failed to list files via daemon")?
                .entries
        }
        ControlPlaneTransport::ApiHttp => api_list_files(&args).await?,
    };

    let entries: Vec<FileEntryView> = entries.into_iter().map(FileEntryView::from).collect();
    if args.json {
        let json = serde_json::to_string_pretty(&entries).context("serialize entries")?;
        println!("{json}");
        return Ok(());
    }

    if entries.is_empty() {
        println!("No entries found.");
        return Ok(());
    }

    println!(
        "{:<50} {:<5} {:<12} {:<12}",
        "PATH", "TYPE", "SIZE", "MODIFIED"
    );
    for entry in entries {
        let entry_type = if entry.is_dir { "dir" } else { "file" };
        println!(
            "{:<50} {:<5} {:<12} {:<12}",
            entry.path, entry_type, entry.size, entry.modified_at
        );
    }
    Ok(())
}

async fn cmd_read(args: FileReadArgs) -> anyhow::Result<()> {
    let response = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            match client
                .read_file(runtime_v2::ReadFileRequest {
                    metadata: None,
                    sandbox_id: args.sandbox_id.clone(),
                    path: args.path.clone(),
                    offset: args.offset,
                    limit: args.limit.unwrap_or(0),
                })
                .await
            {
                Ok(response) => response,
                Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
                    bail!("sandbox or path not found")
                }
                Err(error) => return Err(anyhow!(error).context("failed to read file via daemon")),
            }
        }
        ControlPlaneTransport::ApiHttp => api_read_file(&args)
            .await?
            .ok_or_else(|| anyhow!("sandbox or path not found"))?,
    };

    if let Some(path) = args.output {
        fs::write(&path, &response.data)
            .with_context(|| format!("failed to write {}", path.display()))?;
        println!("Wrote {} bytes to {}.", response.data.len(), path.display());
    } else {
        let mut stdout = std::io::stdout();
        stdout
            .write_all(&response.data)
            .context("failed to write read bytes to stdout")?;
        stdout.flush().context("failed to flush stdout")?;
    }

    if response.truncated {
        eprintln!("warning: output truncated by daemon read limit");
    }
    Ok(())
}

async fn cmd_write(args: FileWriteArgs) -> anyhow::Result<()> {
    let payload = read_write_payload(&args)?;
    let response = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            match client
                .write_file(runtime_v2::WriteFileRequest {
                    metadata: None,
                    sandbox_id: args.sandbox_id.clone(),
                    path: args.path.clone(),
                    data: payload.clone(),
                    append: args.append,
                    create_parents: args.create_parents,
                })
                .await
            {
                Ok(response) => response,
                Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
                    bail!("sandbox or parent path not found")
                }
                Err(error) => {
                    return Err(anyhow!(error).context("failed to write file via daemon"));
                }
            }
        }
        ControlPlaneTransport::ApiHttp => api_write_file(&args, payload)
            .await?
            .ok_or_else(|| anyhow!("sandbox or parent path not found"))?,
    };

    println!("Wrote {} bytes to {}.", response.bytes_written, args.path);
    Ok(())
}

async fn cmd_mkdir(args: FileMkdirArgs) -> anyhow::Result<()> {
    let response = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            match client
                .make_dir(runtime_v2::MakeDirRequest {
                    metadata: None,
                    sandbox_id: args.sandbox_id.clone(),
                    path: args.path.clone(),
                    parents: args.parents,
                })
                .await
            {
                Ok(response) => response,
                Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
                    bail!("sandbox not found")
                }
                Err(error) => {
                    return Err(anyhow!(error).context("failed to create directory via daemon"));
                }
            }
        }
        ControlPlaneTransport::ApiHttp => api_make_dir(&args)
            .await?
            .ok_or_else(|| anyhow!("sandbox not found"))?,
    };
    println!(
        "Directory operation status for {}: {}.",
        response.path, response.status
    );
    Ok(())
}

async fn cmd_remove(args: FileRemoveArgs) -> anyhow::Result<()> {
    let response = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            match client
                .remove_path(runtime_v2::RemovePathRequest {
                    metadata: None,
                    sandbox_id: args.sandbox_id.clone(),
                    path: args.path.clone(),
                    recursive: args.recursive,
                })
                .await
            {
                Ok(response) => response,
                Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
                    bail!("sandbox or path not found")
                }
                Err(error) => {
                    return Err(anyhow!(error).context("failed to remove path via daemon"));
                }
            }
        }
        ControlPlaneTransport::ApiHttp => api_remove_path(&args)
            .await?
            .ok_or_else(|| anyhow!("sandbox or path not found"))?,
    };
    println!(
        "Remove operation status for {}: {}.",
        response.path, response.status
    );
    Ok(())
}

async fn cmd_move(args: FileMoveArgs) -> anyhow::Result<()> {
    let response = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            match client
                .move_path(runtime_v2::MovePathRequest {
                    metadata: None,
                    sandbox_id: args.sandbox_id.clone(),
                    src_path: args.src_path.clone(),
                    dst_path: args.dst_path.clone(),
                    overwrite: args.overwrite,
                })
                .await
            {
                Ok(response) => response,
                Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
                    bail!("sandbox or source path not found")
                }
                Err(error) => return Err(anyhow!(error).context("failed to move path via daemon")),
            }
        }
        ControlPlaneTransport::ApiHttp => api_move_path(&args)
            .await?
            .ok_or_else(|| anyhow!("sandbox or source path not found"))?,
    };
    println!(
        "Move operation status for {}: {}.",
        response.path, response.status
    );
    Ok(())
}

async fn cmd_copy(args: FileCopyArgs) -> anyhow::Result<()> {
    let response = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            match client
                .copy_path(runtime_v2::CopyPathRequest {
                    metadata: None,
                    sandbox_id: args.sandbox_id.clone(),
                    src_path: args.src_path.clone(),
                    dst_path: args.dst_path.clone(),
                    overwrite: args.overwrite,
                })
                .await
            {
                Ok(response) => response,
                Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
                    bail!("sandbox or source path not found")
                }
                Err(error) => return Err(anyhow!(error).context("failed to copy path via daemon")),
            }
        }
        ControlPlaneTransport::ApiHttp => api_copy_path(&args)
            .await?
            .ok_or_else(|| anyhow!("sandbox or source path not found"))?,
    };
    println!(
        "Copy operation status for {}: {}.",
        response.path, response.status
    );
    Ok(())
}

async fn cmd_chmod(args: FileChmodArgs) -> anyhow::Result<()> {
    let mode = parse_mode_octal(&args.mode)?;
    let response = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            match client
                .chmod_path(runtime_v2::ChmodPathRequest {
                    metadata: None,
                    sandbox_id: args.sandbox_id.clone(),
                    path: args.path.clone(),
                    mode,
                })
                .await
            {
                Ok(response) => response,
                Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
                    bail!("sandbox or path not found")
                }
                Err(error) => return Err(anyhow!(error).context("failed to chmod path via daemon")),
            }
        }
        ControlPlaneTransport::ApiHttp => api_chmod_path(&args, mode)
            .await?
            .ok_or_else(|| anyhow!("sandbox or path not found"))?,
    };
    println!(
        "Chmod operation status for {}: {}.",
        response.path, response.status
    );
    Ok(())
}

async fn cmd_chown(args: FileChownArgs) -> anyhow::Result<()> {
    let response = match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
            match client
                .chown_path(runtime_v2::ChownPathRequest {
                    metadata: None,
                    sandbox_id: args.sandbox_id.clone(),
                    path: args.path.clone(),
                    uid: args.uid,
                    gid: args.gid,
                })
                .await
            {
                Ok(response) => response,
                Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
                    bail!("sandbox or path not found")
                }
                Err(error) => return Err(anyhow!(error).context("failed to chown path via daemon")),
            }
        }
        ControlPlaneTransport::ApiHttp => api_chown_path(&args)
            .await?
            .ok_or_else(|| anyhow!("sandbox or path not found"))?,
    };
    println!(
        "Chown operation status for {}: {}.",
        response.path, response.status
    );
    Ok(())
}
