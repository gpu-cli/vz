//! `vz run` / `vz stop` — run commands in a project's Linux VM.
//!
//! Reads `vz.json` from the project directory, boots (or reuses) a Linux VM
//! via the daemon, mounts the project directory via VirtioFS, and executes
//! commands inside the VM. The VM stays alive between runs until `vz stop`.

use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow, bail};
use clap::Args;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tracing::debug;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClient;

use super::runtime_daemon::{connect_control_plane_for_state_db, default_state_db_path};

// ── CLI args ───────────────────────────────────────────────────────

/// Run a command in the project's Linux VM.
#[derive(Args, Debug)]
pub struct DevRunArgs {
    /// Command to execute inside the VM.
    #[arg(trailing_var_arg = true, required = true)]
    pub command: Vec<String>,

    /// Path to vz.json (default: search cwd and parents).
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Override number of CPUs.
    #[arg(long)]
    pub cpus: Option<u8>,

    /// Override memory (e.g., "8G", "4096M", or raw MB).
    #[arg(long)]
    pub memory: Option<String>,

    /// Interactive mode (allocate PTY).
    #[arg(short, long)]
    pub interactive: bool,

    /// Additional environment variables (KEY=VALUE).
    #[arg(short, long)]
    pub env: Vec<String>,

    /// Force fresh VM (stop existing, re-provision).
    #[arg(long)]
    pub fresh: bool,
}

/// Stop the Linux VM for the current project.
#[derive(Args, Debug)]
pub struct DevStopArgs {
    /// Path to vz.json (default: search cwd and parents).
    #[arg(long)]
    pub config: Option<PathBuf>,
}

// ── vz.json schema ─────────────────────────────────────────────────

const VZ_CONFIG_FILE: &str = "vz.json";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VzConfig {
    /// Base container image for the VM rootfs.
    #[serde(default = "default_image")]
    image: String,

    /// Working directory inside the VM for exec.
    #[serde(default = "default_workspace")]
    workspace: String,

    /// VirtioFS mounts from host into the VM.
    #[serde(default)]
    mounts: Vec<MountEntry>,

    /// Setup commands run once after first boot (cached by hash on disk).
    #[serde(default)]
    setup: Vec<String>,

    /// Environment variables injected into every exec.
    #[serde(default)]
    env: BTreeMap<String, String>,

    /// Resource limits.
    #[serde(default)]
    resources: ResourceConfig,
}

fn default_image() -> String {
    "ubuntu:24.04".to_string()
}

fn default_workspace() -> String {
    "/workspace".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MountEntry {
    source: String,
    target: String,
    #[serde(default)]
    read_only: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourceConfig {
    #[serde(default)]
    cpus: Option<u8>,
    #[serde(default)]
    memory: Option<String>,
}

// ── Handlers ───────────────────────────────────────────────────────

pub async fn cmd_run(args: DevRunArgs) -> anyhow::Result<()> {
    let (config, project_dir) = load_config(args.config.as_deref())?;
    let sandbox_id = sandbox_id_for_project(&project_dir);

    let cpus = args.cpus.or(config.resources.cpus).unwrap_or(4);
    let memory_mb = parse_memory(
        args.memory
            .as_deref()
            .or(config.resources.memory.as_deref()),
    )?;

    let volume_mounts = build_volume_mounts(&config, &project_dir)?;
    let disk_image_path = ensure_project_disk(&sandbox_id)?;

    let state_db = default_state_db_path();
    let mut client = connect_control_plane_for_state_db(&state_db).await?;

    // Check if sandbox already exists and is running.
    let needs_boot = match client
        .get_sandbox(runtime_v2::GetSandboxRequest {
            sandbox_id: sandbox_id.clone(),
            metadata: None,
        })
        .await
    {
        Ok(response) => {
            let state = response
                .sandbox
                .as_ref()
                .map(|s| s.state.as_str())
                .unwrap_or("");
            match state {
                // VM is running — reuse it (unless --fresh).
                "ready" | "active" if !args.fresh => false,
                // VM exists but we want fresh, or it's in a terminal/unknown state.
                // Terminate so we can recreate.
                _ if !state.is_empty() => {
                    if args.fresh {
                        eprintln!("Stopping existing VM for fresh start...");
                    }
                    let _ = terminate_sandbox(&mut client, &sandbox_id).await;
                    true
                }
                // Empty state means not found.
                _ => true,
            }
        }
        Err(_) => true,
    };

    if needs_boot {
        eprintln!("Booting Linux VM...");

        let proto_mounts: Vec<runtime_v2::VolumeMount> = volume_mounts
            .iter()
            .map(|m| runtime_v2::VolumeMount {
                tag: m.tag.clone(),
                host_path: m.host_path.clone(),
                guest_path: m.guest_path.clone(),
                read_only: m.read_only,
            })
            .collect();

        let mut labels = HashMap::from([(
            "vz.sandbox.base_image_ref".to_string(),
            config.image.clone(),
        )]);

        // Store mount target paths as labels so the container gets bind mounts.
        for mount in &volume_mounts {
            labels.insert(
                format!("vz.run.mount.{}", mount.tag),
                mount.guest_path.clone(),
            );
        }
        labels.insert(
            "vz.run.workspace".to_string(),
            config.workspace.clone(),
        );

        let mut stream = client
            .create_sandbox_stream(runtime_v2::CreateSandboxRequest {
                metadata: None,
                stack_name: sandbox_id.clone(),
                cpus: u32::from(cpus),
                memory_mb,
                labels,
                volume_mounts: proto_mounts,
                disk_image_path: disk_image_path.to_string_lossy().to_string(),
            })
            .await
            .context("failed to create sandbox via daemon")?;

        while let Some(event) = stream
            .message()
            .await
            .context("failed reading sandbox creation stream")?
        {
            if let Some(payload) = event.payload {
                match payload {
                    runtime_v2::create_sandbox_event::Payload::Progress(p) => {
                        debug!(phase = %p.phase, detail = %p.detail, "boot progress");
                    }
                    runtime_v2::create_sandbox_event::Payload::Completion(c) => {
                        if c.response.is_none() {
                            bail!("sandbox creation failed (no response in completion)");
                        }
                    }
                }
            }
        }

        eprintln!("VM ready.");

        // Run setup commands if needed.
        run_setup_if_needed(&mut client, &sandbox_id, &config).await?;
    }

    // Resolve the container for this sandbox.
    let container_id = resolve_container(&mut client, &sandbox_id).await?;

    // Build the shell command with env vars and working directory.
    let shell_command = args.command.join(" ");
    let mut env_map = config.env.clone();
    for entry in &args.env {
        if let Some((key, value)) = entry.split_once('=') {
            env_map.insert(key.to_string(), value.to_string());
        }
    }

    let env_prefix: String = env_map
        .iter()
        .map(|(k, v)| format!("export {}={}", k, shell_escape(v)))
        .collect::<Vec<_>>()
        .join("; ");

    let workspace = &config.workspace;
    let full_command = if env_prefix.is_empty() {
        format!("cd {workspace} && {shell_command}")
    } else {
        format!("cd {workspace} && {env_prefix}; {shell_command}")
    };

    let pty_mode = if args.interactive {
        runtime_v2::create_execution_request::PtyMode::Enabled as i32
    } else {
        runtime_v2::create_execution_request::PtyMode::Disabled as i32
    };

    let execution = client
        .create_execution(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id,
            cmd: vec!["/bin/sh".to_string()],
            args: vec!["-c".to_string(), full_command],
            env_override: HashMap::new(),
            timeout_secs: 3600,
            pty_mode,
        })
        .await
        .context("failed to create execution")?;

    let execution_payload = execution
        .execution
        .ok_or_else(|| anyhow!("daemon missing execution payload"))?;
    let execution_id = execution_payload.execution_id;

    let mut stream = client
        .stream_exec_output(runtime_v2::StreamExecOutputRequest {
            execution_id,
            metadata: None,
        })
        .await
        .context("failed to stream execution output")?;

    let mut exit_code = 0i32;
    while let Some(event) = stream
        .message()
        .await
        .context("failed reading execution stream")?
    {
        match event.payload {
            Some(runtime_v2::exec_output_event::Payload::Stdout(bytes)) => {
                let _ = std::io::stdout().write_all(&bytes);
                let _ = std::io::stdout().flush();
            }
            Some(runtime_v2::exec_output_event::Payload::Stderr(bytes)) => {
                // Filter the harmless getcwd() warning from the shell.
                // The kernel's getcwd() syscall fails with stacked
                // overlay+VirtioFS mounts but CWD is actually correct.
                write_filtered_stderr(&bytes);
            }
            Some(runtime_v2::exec_output_event::Payload::ExitCode(code)) => {
                exit_code = code;
            }
            Some(runtime_v2::exec_output_event::Payload::Error(error)) => {
                bail!("execution error: {error}");
            }
            None => {}
        }
    }

    if exit_code != 0 {
        std::process::exit(exit_code);
    }

    Ok(())
}

pub async fn cmd_stop(args: DevStopArgs) -> anyhow::Result<()> {
    let (_config, project_dir) = load_config(args.config.as_deref())?;
    let sandbox_id = sandbox_id_for_project(&project_dir);

    let state_db = default_state_db_path();
    let mut client = connect_control_plane_for_state_db(&state_db).await?;

    terminate_sandbox(&mut client, &sandbox_id).await?;
    eprintln!("Stopped VM for {}", project_dir.display());
    Ok(())
}

// ── Config discovery ───────────────────────────────────────────────

fn load_config(explicit_path: Option<&Path>) -> anyhow::Result<(VzConfig, PathBuf)> {
    let config_path = match explicit_path {
        Some(path) => path.to_path_buf(),
        None => find_config_file()?,
    };

    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let config: VzConfig =
        serde_json::from_str(&raw).with_context(|| format!("invalid {}", config_path.display()))?;

    let project_dir = config_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    Ok((config, project_dir))
}

fn find_config_file() -> anyhow::Result<PathBuf> {
    let mut dir = std::env::current_dir().context("failed to get current directory")?;
    loop {
        let candidate = dir.join(VZ_CONFIG_FILE);
        if candidate.is_file() {
            return Ok(candidate);
        }
        if !dir.pop() {
            bail!(
                "no {VZ_CONFIG_FILE} found in current directory or any parent; \
                 create one or use --config"
            );
        }
    }
}

// ── Sandbox naming ─────────────────────────────────────────────────

fn sandbox_id_for_project(project_dir: &Path) -> String {
    let canonical = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());
    let mut hasher = sha2::Sha256::new();
    hasher.update(canonical.to_string_lossy().as_bytes());
    let hash = hasher.finalize();
    let short_hash: String = hash[..6].iter().map(|b| format!("{b:02x}")).collect();

    let dir_name = canonical
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("project");
    format!("vz-run-{dir_name}-{short_hash}")
}

// ── Mount building ─────────────────────────────────────────────────

struct ResolvedMount {
    tag: String,
    host_path: String,
    guest_path: String,
    read_only: bool,
}

fn build_volume_mounts(
    config: &VzConfig,
    project_dir: &Path,
) -> anyhow::Result<Vec<ResolvedMount>> {
    let mounts: Vec<MountEntry> = if config.mounts.is_empty() {
        vec![MountEntry {
            source: ".".to_string(),
            target: config.workspace.clone(),
            read_only: false,
        }]
    } else {
        config.mounts.clone()
    };

    mounts
        .iter()
        .enumerate()
        .map(|(idx, entry)| {
            let host_path = if entry.source == "." {
                project_dir.to_path_buf()
            } else if Path::new(&entry.source).is_absolute() {
                PathBuf::from(&entry.source)
            } else {
                project_dir.join(&entry.source)
            };

            let host_path = host_path
                .canonicalize()
                .with_context(|| format!("mount source does not exist: {}", entry.source))?;

            Ok(ResolvedMount {
                tag: format!("vz-mount-{idx}"),
                host_path: host_path.to_string_lossy().to_string(),
                guest_path: entry.target.clone(),
                read_only: entry.read_only,
            })
        })
        .collect()
}

// ── Persistent disk ────────────────────────────────────────────────

fn ensure_project_disk(sandbox_id: &str) -> anyhow::Result<PathBuf> {
    let run_dir = home_dir()?.join(".vz").join("run").join(sandbox_id);
    std::fs::create_dir_all(&run_dir)
        .with_context(|| format!("failed to create {}", run_dir.display()))?;

    let disk_path = run_dir.join("disk.img");
    if !disk_path.exists() {
        let disk_size: u64 = 20 * 1024 * 1024 * 1024;
        let file = std::fs::File::create(&disk_path)
            .with_context(|| format!("failed to create disk image {}", disk_path.display()))?;
        file.set_len(disk_size)
            .context("failed to set disk image size")?;
        eprintln!("Created 20 GiB persistent disk at {}", disk_path.display());
    }

    Ok(disk_path)
}

// ── Setup caching ──────────────────────────────────────────────────

async fn run_setup_if_needed(
    client: &mut DaemonClient,
    sandbox_id: &str,
    config: &VzConfig,
) -> anyhow::Result<()> {
    if config.setup.is_empty() {
        return Ok(());
    }

    let setup_hash = {
        let mut hasher = Sha256::new();
        for cmd in &config.setup {
            hasher.update(cmd.as_bytes());
            hasher.update(b"\n");
        }
        hasher.finalize()[..8]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    };

    // Check if setup already ran (marker on persistent disk).
    let container_id = resolve_container(client, sandbox_id).await?;
    let check_result = exec_quiet(
        client,
        &container_id,
        "cat /run/vz-oci/volumes/.vz-setup-hash 2>/dev/null",
    )
    .await;

    if let Ok(output) = &check_result {
        if output.trim() == setup_hash {
            debug!(hash = %setup_hash, "setup already complete, skipping");
            return Ok(());
        }
    }

    eprintln!("Running setup commands...");
    for (i, cmd) in config.setup.iter().enumerate() {
        eprintln!("  [{}/{}] {}", i + 1, config.setup.len(), cmd);
        let exit_code = exec_streaming(client, &container_id, cmd).await?;
        if exit_code != 0 {
            bail!("setup command failed with exit code {exit_code}: {cmd}");
        }
    }

    // Write the hash marker.
    exec_quiet(
        client,
        &container_id,
        &format!(
            "mkdir -p /run/vz-oci/volumes && printf '%s' '{setup_hash}' > /run/vz-oci/volumes/.vz-setup-hash"
        ),
    )
    .await?;

    eprintln!("Setup complete.");
    Ok(())
}

/// Filter out the harmless `getcwd() failed` warning from stderr.
///
/// The Linux kernel's `getcwd()` syscall cannot resolve the dentry path
/// through stacked overlay + VirtioFS mount boundaries. The shell prints
/// this warning at startup, but the CWD is actually correct (verified via
/// `/proc/self/cwd`). This is a kernel limitation, not a real error.
fn write_filtered_stderr(bytes: &[u8]) {
    let text = String::from_utf8_lossy(bytes);
    for line in text.split_inclusive('\n') {
        if !line.contains("getcwd() failed") {
            let _ = std::io::stderr().write_all(line.as_bytes());
        }
    }
    let _ = std::io::stderr().flush();
}

// ── Daemon helpers ─────────────────────────────────────────────────

async fn resolve_container(
    client: &mut DaemonClient,
    sandbox_id: &str,
) -> anyhow::Result<String> {
    let mut stream = client
        .open_sandbox_shell(runtime_v2::OpenSandboxShellRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
        .context("failed to open sandbox shell for container resolution")?;

    let mut container_id = None;
    while let Some(event) = stream
        .message()
        .await
        .context("failed reading open_sandbox_shell stream")?
    {
        if let Some(runtime_v2::open_sandbox_shell_event::Payload::Completion(done)) =
            event.payload
        {
            container_id = Some(done.container_id);
            break;
        }
    }

    container_id
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| anyhow!("no container found in sandbox {sandbox_id}"))
}

async fn terminate_sandbox(
    client: &mut DaemonClient,
    sandbox_id: &str,
) -> anyhow::Result<()> {
    let mut stream = client
        .terminate_sandbox_stream(runtime_v2::TerminateSandboxRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
        .context("failed to terminate sandbox")?;

    while let Some(_event) = stream
        .message()
        .await
        .context("error reading terminate stream")?
    {}

    Ok(())
}

async fn exec_quiet(
    client: &mut DaemonClient,
    container_id: &str,
    command: &str,
) -> anyhow::Result<String> {
    let execution = client
        .create_execution(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id: container_id.to_string(),
            cmd: vec!["/bin/sh".to_string()],
            args: vec!["-c".to_string(), command.to_string()],
            env_override: HashMap::new(),
            timeout_secs: 60,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Disabled as i32,
        })
        .await
        .context("exec failed")?;

    let execution_id = execution
        .execution
        .ok_or_else(|| anyhow!("missing execution payload"))?
        .execution_id;

    let mut stream = client
        .stream_exec_output(runtime_v2::StreamExecOutputRequest {
            execution_id,
            metadata: None,
        })
        .await?;

    let mut stdout = String::new();
    while let Some(event) = stream.message().await? {
        if let Some(runtime_v2::exec_output_event::Payload::Stdout(bytes)) = event.payload {
            stdout.push_str(&String::from_utf8_lossy(&bytes));
        }
    }
    Ok(stdout)
}

async fn exec_streaming(
    client: &mut DaemonClient,
    container_id: &str,
    command: &str,
) -> anyhow::Result<i32> {
    let execution = client
        .create_execution(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id: container_id.to_string(),
            cmd: vec!["/bin/sh".to_string()],
            args: vec!["-lc".to_string(), command.to_string()],
            env_override: HashMap::new(),
            timeout_secs: 3600,
            pty_mode: runtime_v2::create_execution_request::PtyMode::Disabled as i32,
        })
        .await
        .context("exec failed")?;

    let execution_id = execution
        .execution
        .ok_or_else(|| anyhow!("missing execution payload"))?
        .execution_id;

    let mut stream = client
        .stream_exec_output(runtime_v2::StreamExecOutputRequest {
            execution_id,
            metadata: None,
        })
        .await?;

    let mut exit_code: Option<i32> = None;
    while let Some(event) = stream.message().await? {
        match event.payload {
            Some(runtime_v2::exec_output_event::Payload::Stdout(bytes)) => {
                let _ = std::io::stdout().write_all(&bytes);
                let _ = std::io::stdout().flush();
            }
            Some(runtime_v2::exec_output_event::Payload::Stderr(bytes)) => {
                let _ = std::io::stderr().write_all(&bytes);
                let _ = std::io::stderr().flush();
            }
            Some(runtime_v2::exec_output_event::Payload::ExitCode(code)) => {
                exit_code = Some(code);
            }
            _ => {}
        }
    }
    // If no exit code event was received (e.g., timeout or disconnection),
    // treat as failure rather than silently succeeding.
    Ok(exit_code.unwrap_or(1))
}

// ── Utilities ──────────────────────────────────────────────────────

fn home_dir() -> anyhow::Result<PathBuf> {
    std::env::var("HOME")
        .map(PathBuf::from)
        .context("HOME environment variable not set")
}

fn parse_memory(raw: Option<&str>) -> anyhow::Result<u64> {
    match raw {
        None => Ok(8192),
        Some(s) => {
            let s = s.trim();
            if let Some(gb) = s.strip_suffix('G').or_else(|| s.strip_suffix("GB")) {
                let n: u64 = gb.trim().parse().context("invalid memory value")?;
                Ok(n * 1024)
            } else if let Some(mb) = s.strip_suffix('M').or_else(|| s.strip_suffix("MB")) {
                let n: u64 = mb.trim().parse().context("invalid memory value")?;
                Ok(n)
            } else {
                s.parse::<u64>()
                    .context("invalid memory value (use e.g., '8G' or '4096M')")
            }
        }
    }
}

fn shell_escape(s: &str) -> String {
    if s.chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '/' || c == '.' || c == ':')
    {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}
