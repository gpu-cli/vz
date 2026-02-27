//! `vz file` -- sandbox filesystem primitives through runtime daemon.
//!
//! Provides read/write/list and path mutation operations backed by
//! Runtime V2 FileService.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{Context, anyhow, bail};
use clap::{Args, Subcommand};
use serde::Serialize;
use tonic::Code;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClientError;

use super::runtime_daemon::connect_control_plane_for_state_db;

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
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = client
        .list_files(runtime_v2::ListFilesRequest {
            metadata: None,
            sandbox_id: args.sandbox_id,
            path: args.path,
            recursive: args.recursive,
            limit: args.limit.unwrap_or(0),
        })
        .await
        .context("failed to list files via daemon")?;

    let entries: Vec<FileEntryView> = response
        .entries
        .into_iter()
        .map(FileEntryView::from)
        .collect();
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
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .read_file(runtime_v2::ReadFileRequest {
            metadata: None,
            sandbox_id: args.sandbox_id,
            path: args.path,
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
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .write_file(runtime_v2::WriteFileRequest {
            metadata: None,
            sandbox_id: args.sandbox_id,
            path: args.path.clone(),
            data: payload,
            append: args.append,
            create_parents: args.create_parents,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("sandbox or parent path not found")
        }
        Err(error) => return Err(anyhow!(error).context("failed to write file via daemon")),
    };

    println!("Wrote {} bytes to {}.", response.bytes_written, args.path);
    Ok(())
}

async fn cmd_mkdir(args: FileMkdirArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .make_dir(runtime_v2::MakeDirRequest {
            metadata: None,
            sandbox_id: args.sandbox_id,
            path: args.path,
            parents: args.parents,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("sandbox not found")
        }
        Err(error) => return Err(anyhow!(error).context("failed to create directory via daemon")),
    };
    println!(
        "Directory operation status for {}: {}.",
        response.path, response.status
    );
    Ok(())
}

async fn cmd_remove(args: FileRemoveArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .remove_path(runtime_v2::RemovePathRequest {
            metadata: None,
            sandbox_id: args.sandbox_id,
            path: args.path,
            recursive: args.recursive,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("sandbox or path not found")
        }
        Err(error) => return Err(anyhow!(error).context("failed to remove path via daemon")),
    };
    println!(
        "Remove operation status for {}: {}.",
        response.path, response.status
    );
    Ok(())
}

async fn cmd_move(args: FileMoveArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .move_path(runtime_v2::MovePathRequest {
            metadata: None,
            sandbox_id: args.sandbox_id,
            src_path: args.src_path,
            dst_path: args.dst_path,
            overwrite: args.overwrite,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("sandbox or source path not found")
        }
        Err(error) => return Err(anyhow!(error).context("failed to move path via daemon")),
    };
    println!(
        "Move operation status for {}: {}.",
        response.path, response.status
    );
    Ok(())
}

async fn cmd_copy(args: FileCopyArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .copy_path(runtime_v2::CopyPathRequest {
            metadata: None,
            sandbox_id: args.sandbox_id,
            src_path: args.src_path,
            dst_path: args.dst_path,
            overwrite: args.overwrite,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("sandbox or source path not found")
        }
        Err(error) => return Err(anyhow!(error).context("failed to copy path via daemon")),
    };
    println!(
        "Copy operation status for {}: {}.",
        response.path, response.status
    );
    Ok(())
}

async fn cmd_chmod(args: FileChmodArgs) -> anyhow::Result<()> {
    let mode = parse_mode_octal(&args.mode)?;
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .chmod_path(runtime_v2::ChmodPathRequest {
            metadata: None,
            sandbox_id: args.sandbox_id,
            path: args.path,
            mode,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("sandbox or path not found")
        }
        Err(error) => return Err(anyhow!(error).context("failed to chmod path via daemon")),
    };
    println!(
        "Chmod operation status for {}: {}.",
        response.path, response.status
    );
    Ok(())
}

async fn cmd_chown(args: FileChownArgs) -> anyhow::Result<()> {
    let mut client = connect_control_plane_for_state_db(&args.state_db).await?;
    let response = match client
        .chown_path(runtime_v2::ChownPathRequest {
            metadata: None,
            sandbox_id: args.sandbox_id,
            path: args.path,
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
    };
    println!(
        "Chown operation status for {}: {}.",
        response.path, response.status
    );
    Ok(())
}
