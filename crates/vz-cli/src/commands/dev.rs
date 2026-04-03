//! `vz run` / `vz stop` — run commands in a project's Linux VM.
//!
//! Reads `vz.json` from the project directory, boots (or reuses) a Linux VM
//! via the daemon, mounts the project directory via VirtioFS, and executes
//! commands inside the VM. The VM stays alive between runs until `vz stop`.

use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, anyhow, bail};
use clap::Args;
use crossterm::terminal;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tracing::debug;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::{DaemonClient, DaemonClientError};

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

    /// Publish a container port to the host (HOST:CONTAINER[/PROTO]).
    #[arg(short = 'p', long = "publish")]
    pub publish: Vec<String>,

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

    /// Stop all running `vz run` sandboxes (not just current project).
    #[arg(long)]
    pub all: bool,
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

    /// Port mappings (HOST:CONTAINER or HOST:CONTAINER/PROTO).
    #[serde(default)]
    ports: Vec<String>,

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

    // Merge ports from vz.json and CLI -p flags.
    let mut all_port_specs = config.ports.clone();
    all_port_specs.extend(args.publish.iter().cloned());
    let port_mappings = parse_port_mappings(&all_port_specs)?;

    // --fresh: delete persistent disk so the container starts with a clean filesystem.
    if args.fresh {
        let run_dir = home_dir()?.join(".vz").join("run").join(&sandbox_id);
        let disk_path = run_dir.join("disk.img");
        if disk_path.exists() {
            let _ = std::fs::remove_file(&disk_path);
        }
    }

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
                port_mappings: port_mappings.clone(),
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
        for pm in &port_mappings {
            eprintln!(
                "  Port {} -> {} ({})",
                pm.host_port, pm.container_port, pm.protocol
            );
        }

        // Run setup commands if needed.
        // When --fresh, force re-run by clearing any cached hashes.
        if args.fresh {
            if let Ok(path) = host_setup_hash_path(&sandbox_id) {
                let _ = std::fs::remove_file(path);
            }
            let container_id = resolve_container(&mut client, &sandbox_id).await?;
            let _ = exec_quiet(
                &mut client,
                &container_id,
                "rm -f /run/vz-oci/volumes/.vz-setup-hash",
            )
            .await;
        }
        run_setup_if_needed(&mut client, &sandbox_id, &config).await?;
    }

    // Resolve the container for this sandbox.
    let container_id = resolve_container(&mut client, &sandbox_id).await?;

    // Build the shell command with env vars and working directory.
    // Each argument is shell-quoted so that multi-word args (e.g. `sh -c 'echo hello'`)
    // are preserved when the command string is passed through `/bin/sh -c`.
    let shell_command = args
        .command
        .iter()
        .map(|arg| shell_words::quote(arg).into_owned())
        .collect::<Vec<_>>()
        .join(" ");
    let mut env_map = config.env.clone();

    // Ensure HOME is always set — many tools (rustup, npm, etc.) depend on it.
    if !env_map.contains_key("HOME") {
        env_map.insert("HOME".to_string(), "/root".to_string());
    }

    // Auto-detect Rust projects and set CARGO_TARGET_DIR to persistent disk
    // so build artifacts survive VM restarts.
    if !env_map.contains_key("CARGO_TARGET_DIR") && project_dir.join("Cargo.toml").exists() {
        env_map.insert(
            "CARGO_TARGET_DIR".to_string(),
            "/run/vz-oci/volumes/cargo-target".to_string(),
        );
    }

    for entry in &args.env {
        if let Some((key, value)) = entry.split_once('=') {
            env_map.insert(key.to_string(), value.to_string());
        }
    }

    let env_prefix: String = env_map
        .iter()
        .map(|(k, v)| format!("export {}={}", k, shell_words::quote(v)))
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

    // Keep copies for potential retry on terminal state conflict.
    let retry_container_id = container_id.clone();
    let retry_full_command = full_command.clone();

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
    let execution_id = execution_payload.execution_id.clone();

    // For interactive mode: enable raw terminal and forward stdin to the PTY.
    let stdin_stop = Arc::new(AtomicBool::new(false));
    let stdin_handle = if args.interactive {
        terminal::enable_raw_mode().context("failed to enable raw mode")?;

        let stop = Arc::clone(&stdin_stop);
        let exec_id = execution_id.clone();
        let mut stdin_client = client.clone();
        Some(tokio::task::spawn_blocking(move || {
            use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
            while !stop.load(Ordering::Relaxed) {
                if !event::poll(std::time::Duration::from_millis(100)).unwrap_or(false) {
                    continue;
                }
                let Ok(ev) = event::read() else { break };
                let bytes = match ev {
                    Event::Key(key)
                        if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) =>
                    {
                        match key.code {
                            KeyCode::Char(c)
                                if key.modifiers.contains(KeyModifiers::CONTROL) =>
                            {
                                vec![c as u8 & 0x1f]
                            }
                            KeyCode::Char(c) => {
                                let mut buf = [0u8; 4];
                                c.encode_utf8(&mut buf);
                                buf[..c.len_utf8()].to_vec()
                            }
                            KeyCode::Enter => vec![b'\r'],
                            KeyCode::Backspace => vec![0x7f],
                            KeyCode::Tab => vec![b'\t'],
                            KeyCode::Esc => vec![0x1b],
                            KeyCode::Up => vec![0x1b, b'[', b'A'],
                            KeyCode::Down => vec![0x1b, b'[', b'B'],
                            KeyCode::Right => vec![0x1b, b'[', b'C'],
                            KeyCode::Left => vec![0x1b, b'[', b'D'],
                            KeyCode::Home => vec![0x1b, b'[', b'H'],
                            KeyCode::End => vec![0x1b, b'[', b'F'],
                            KeyCode::Delete => vec![0x1b, b'[', b'3', b'~'],
                            _ => continue,
                        }
                    }
                    Event::Paste(text) => text.into_bytes(),
                    _ => continue,
                };

                let rt = tokio::runtime::Handle::current();
                let _ = rt.block_on(stdin_client.write_exec_stdin(
                    runtime_v2::WriteExecStdinRequest {
                        execution_id: exec_id.clone(),
                        data: bytes,
                        metadata: None,
                    },
                ));
            }
        }))
    } else {
        None
    };

    let stream_result = client
        .stream_exec_output(runtime_v2::StreamExecOutputRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        })
        .await;

    // If the execution is already in a terminal state (e.g., from a previous failed run),
    // create a fresh execution and retry. This recovers from stale state_conflict errors
    // without requiring a manual daemon kill.
    let is_terminal_err = stream_result.as_ref().err().is_some_and(is_terminal_state_error);
    let mut stream = match stream_result {
        Ok(s) => s,
        Err(_) if is_terminal_err => {
            debug!(execution_id = %execution_id, "execution in terminal state, creating fresh execution");
            let retry = client
                .create_execution(runtime_v2::CreateExecutionRequest {
                    metadata: None,
                    container_id: retry_container_id,
                    cmd: vec!["/bin/sh".to_string()],
                    args: vec!["-c".to_string(), retry_full_command],
                    env_override: HashMap::new(),
                    timeout_secs: 3600,
                    pty_mode,
                })
                .await
                .context("failed to create retry execution")?;

            let retry_id = retry
                .execution
                .ok_or_else(|| anyhow!("daemon missing execution payload on retry"))?
                .execution_id;

            client
                .stream_exec_output(runtime_v2::StreamExecOutputRequest {
                    execution_id: retry_id,
                    metadata: None,
                })
                .await
                .context("failed to stream retry execution output")?
        }
        Err(e) => return Err(e).context("failed to stream execution output"),
    };

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
                write_filtered_stderr(&bytes);
            }
            Some(runtime_v2::exec_output_event::Payload::ExitCode(code)) => {
                exit_code = code;
            }
            Some(runtime_v2::exec_output_event::Payload::Error(error)) => {
                if args.interactive {
                    stdin_stop.store(true, Ordering::Relaxed);
                    let _ = terminal::disable_raw_mode();
                }
                bail!("execution error: {error}");
            }
            None => {}
        }
    }

    // Clean up interactive mode.
    if args.interactive {
        stdin_stop.store(true, Ordering::Relaxed);
        let _ = terminal::disable_raw_mode();
        if let Some(handle) = stdin_handle {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        }
    }

    if exit_code != 0 {
        std::process::exit(exit_code);
    }

    Ok(())
}

pub async fn cmd_stop(args: DevStopArgs) -> anyhow::Result<()> {
    let state_db = default_state_db_path();
    let mut client = connect_control_plane_for_state_db(&state_db).await?;

    if args.all {
        // Stop all vz-run sandboxes.
        let response = client
            .list_sandboxes(runtime_v2::ListSandboxesRequest { metadata: None })
            .await
            .context("failed to list sandboxes")?;

        let run_sandboxes: Vec<_> = response
            .sandboxes
            .iter()
            .filter(|s| s.sandbox_id.starts_with("vz-run-"))
            .filter(|s| s.state == "ready" || s.state == "active")
            .collect();

        if run_sandboxes.is_empty() {
            eprintln!("No running `vz run` VMs found.");
            return Ok(());
        }

        for sandbox in &run_sandboxes {
            let _ = terminate_sandbox(&mut client, &sandbox.sandbox_id).await;
            eprintln!("Stopped {}", sandbox.sandbox_id);
        }
        eprintln!("Stopped {} VM(s).", run_sandboxes.len());
    } else {
        let (_config, project_dir) = load_config(args.config.as_deref())?;
        let sandbox_id = sandbox_id_for_project(&project_dir);
        terminate_sandbox(&mut client, &sandbox_id).await?;
        eprintln!("Stopped VM for {}", project_dir.display());
    }

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
                "no {VZ_CONFIG_FILE} found in current directory or any parent.\n\n\
                 Run `vz init` to generate one, or use `--config <path>`."
            );
        }
    }
}

// ── Sandbox naming ─────────────────────────────────────────────────

pub(crate) fn sandbox_id_for_project(project_dir: &Path) -> String {
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

/// Compute a setup hash over the full vz.json config.
///
/// Includes image, setup commands, and resources so that
/// changes to any of these trigger re-execution of setup.
fn compute_setup_hash(config: &VzConfig) -> String {
    let mut hasher = Sha256::new();
    hasher.update(config.image.as_bytes());
    hasher.update(b"\n");
    for cmd in &config.setup {
        hasher.update(cmd.as_bytes());
        hasher.update(b"\n");
    }
    if let Some(cpus) = config.resources.cpus {
        hasher.update(format!("cpus:{cpus}\n").as_bytes());
    }
    if let Some(ref mem) = config.resources.memory {
        hasher.update(format!("mem:{mem}\n").as_bytes());
    }
    hasher.finalize()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
}

/// Host-side path for the setup hash fallback.
fn host_setup_hash_path(sandbox_id: &str) -> anyhow::Result<PathBuf> {
    Ok(home_dir()?
        .join(".vz")
        .join("run")
        .join(sandbox_id)
        .join(".vz-setup-hash"))
}

/// Check if setup hash matches on host (fallback) or guest (primary).
fn check_host_setup_hash(sandbox_id: &str, expected: &str) -> bool {
    host_setup_hash_path(sandbox_id)
        .ok()
        .and_then(|path| std::fs::read_to_string(path).ok())
        .is_some_and(|content| content.trim() == expected)
}

/// Write setup hash to the host-side fallback location.
fn write_host_setup_hash(sandbox_id: &str, hash: &str) {
    if let Ok(path) = host_setup_hash_path(sandbox_id) {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(path, hash);
    }
}

async fn run_setup_if_needed(
    client: &mut DaemonClient,
    sandbox_id: &str,
    config: &VzConfig,
) -> anyhow::Result<()> {
    if config.setup.is_empty() {
        return Ok(());
    }

    let setup_hash = compute_setup_hash(config);

    // Check guest-side hash first (persistent disk), then host-side fallback.
    let container_id = resolve_container(client, sandbox_id).await?;
    let guest_match = exec_quiet(
        client,
        &container_id,
        "cat /run/vz-oci/volumes/.vz-setup-hash 2>/dev/null",
    )
    .await
    .is_ok_and(|output| output.trim() == setup_hash);

    if guest_match {
        debug!(hash = %setup_hash, "setup already complete (guest hash match), skipping");
        return Ok(());
    }

    if check_host_setup_hash(sandbox_id, &setup_hash) {
        debug!(hash = %setup_hash, "setup already complete (host hash match), skipping");
        // Re-write guest hash so future checks are fast.
        let _ = exec_quiet(
            client,
            &container_id,
            &format!(
                "mkdir -p /run/vz-oci/volumes && printf '%s' '{setup_hash}' > /run/vz-oci/volumes/.vz-setup-hash"
            ),
        )
        .await;
        return Ok(());
    }

    eprintln!("Running setup commands...");
    for (i, cmd) in config.setup.iter().enumerate() {
        eprintln!("  [{}/{}] {}", i + 1, config.setup.len(), cmd);
        let exit_code = exec_streaming(client, &container_id, cmd).await?;
        if exit_code != 0 {
            bail!("setup command failed with exit code {exit_code}: {cmd}");
        }
    }

    // Write the hash marker to both guest and host.
    exec_quiet(
        client,
        &container_id,
        &format!(
            "mkdir -p /run/vz-oci/volumes && printf '%s' '{setup_hash}' > /run/vz-oci/volumes/.vz-setup-hash"
        ),
    )
    .await?;
    write_host_setup_hash(sandbox_id, &setup_hash);

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
            args: vec![
                "-c".to_string(),
                format!("cd / && {command}"),
            ],
            env_override: HashMap::from([
                ("HOME".to_string(), "/root".to_string()),
            ]),
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
                write_filtered_stderr(&bytes);
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

/// Check if a daemon client error is a "terminal state" conflict
/// that can be recovered by creating a new execution.
fn is_terminal_state_error(error: &DaemonClientError) -> bool {
    matches!(
        error,
        DaemonClientError::Grpc(status)
            if status.code() == tonic::Code::FailedPrecondition
                && status.message().contains("terminal state")
    )
}

fn parse_port_mappings(
    specs: &[String],
) -> anyhow::Result<Vec<runtime_v2::PortMapping>> {
    specs.iter().map(|s| parse_port_mapping(s)).collect()
}

fn parse_port_mapping(spec: &str) -> anyhow::Result<runtime_v2::PortMapping> {
    let (ports_part, protocol) = match spec.split_once('/') {
        Some((ports, proto)) => (ports, proto.to_ascii_lowercase()),
        None => (spec, "tcp".to_string()),
    };

    if protocol != "tcp" && protocol != "udp" {
        bail!(
            "invalid -p protocol '{protocol}' in '{spec}', expected tcp or udp"
        );
    }

    let mut parts = ports_part.split(':');
    let host_str = parts
        .next()
        .context("invalid -p value, expected HOST:CONTAINER[/PROTO]")?;
    let container_str = parts
        .next()
        .with_context(|| format!("invalid -p value '{spec}', expected HOST:CONTAINER[/PROTO]"))?;

    if parts.next().is_some() {
        bail!(
            "invalid -p value '{spec}', host IP is not supported yet; expected HOST:CONTAINER[/PROTO]"
        );
    }

    let host_port = host_str
        .parse::<u32>()
        .with_context(|| format!("invalid host port '{host_str}' in -p '{spec}'"))?;
    let container_port = container_str
        .parse::<u32>()
        .with_context(|| format!("invalid container port '{container_str}' in -p '{spec}'"))?;

    Ok(runtime_v2::PortMapping {
        host_port,
        container_port,
        protocol,
    })
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

