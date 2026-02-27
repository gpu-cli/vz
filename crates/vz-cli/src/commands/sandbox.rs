//! `vz sandbox` — sandbox lifecycle management commands.
//!
//! Provides sandbox CRUD and the default `vz` instant-sandbox experience.
//! Sandbox state persistence is routed through `vz-runtimed`.

#![allow(clippy::print_stdout)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow, bail};
use clap::Args;
use tonic::Code;
use vz_runtime_contract::{
    SANDBOX_LABEL_BASE_IMAGE_REF, SANDBOX_LABEL_MAIN_CONTAINER, Sandbox, SandboxBackend,
    SandboxSpec, SandboxState,
};
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClientError;

use super::runtime_daemon::{connect_control_plane_for_state_db, default_state_db_path};

const SANDBOX_PROJECT_DIR_LABEL: &str = "project_dir";
const DEFAULT_SANDBOX_BASE_IMAGE_REF: &str = "debian:bookworm";

fn sandbox_backend_from_wire(backend: &str) -> SandboxBackend {
    match backend.trim().to_ascii_lowercase().as_str() {
        "macos_vz" | "macos-vz" => SandboxBackend::MacosVz,
        "linux_firecracker" | "linux-firecracker" => SandboxBackend::LinuxFirecracker,
        other => SandboxBackend::Other(other.to_string()),
    }
}

fn sandbox_state_from_wire(state: &str) -> anyhow::Result<SandboxState> {
    match state.trim().to_ascii_lowercase().as_str() {
        "creating" => Ok(SandboxState::Creating),
        "ready" => Ok(SandboxState::Ready),
        "draining" => Ok(SandboxState::Draining),
        "terminated" => Ok(SandboxState::Terminated),
        "failed" => Ok(SandboxState::Failed),
        other => Err(anyhow!("unsupported sandbox state from daemon: {other}")),
    }
}

fn normalize_optional_label(value: Option<&String>) -> Option<String> {
    let raw = value?.trim();
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

fn sandbox_from_proto(payload: runtime_v2::SandboxPayload) -> anyhow::Result<Sandbox> {
    let labels: BTreeMap<String, String> = payload.labels.into_iter().collect();
    let base_image_ref = normalize_optional_label(labels.get(SANDBOX_LABEL_BASE_IMAGE_REF));
    let main_container = normalize_optional_label(labels.get(SANDBOX_LABEL_MAIN_CONTAINER));
    Ok(Sandbox {
        sandbox_id: payload.sandbox_id,
        backend: sandbox_backend_from_wire(&payload.backend),
        spec: SandboxSpec {
            cpus: if payload.cpus == 0 {
                None
            } else {
                Some(payload.cpus as u8)
            },
            memory_mb: if payload.memory_mb == 0 {
                None
            } else {
                Some(payload.memory_mb)
            },
            base_image_ref,
            main_container,
            network_profile: None,
            volume_mounts: Vec::new(),
        },
        state: sandbox_state_from_wire(&payload.state)?,
        created_at: payload.created_at,
        updated_at: payload.updated_at,
        labels,
    })
}

async fn daemon_list_sandboxes(state_db: &Path) -> anyhow::Result<Vec<Sandbox>> {
    let mut client = connect_control_plane_for_state_db(state_db).await?;
    let response = client
        .list_sandboxes(runtime_v2::ListSandboxesRequest { metadata: None })
        .await
        .context("failed to list sandboxes via daemon")?;
    response
        .sandboxes
        .into_iter()
        .map(sandbox_from_proto)
        .collect()
}

async fn daemon_get_sandbox(state_db: &Path, sandbox_id: &str) -> anyhow::Result<Option<Sandbox>> {
    let mut client = connect_control_plane_for_state_db(state_db).await?;
    match client
        .get_sandbox(runtime_v2::GetSandboxRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
    {
        Ok(response) => {
            let payload = response
                .sandbox
                .ok_or_else(|| anyhow!("daemon get_sandbox returned missing payload"))?;
            Ok(Some(sandbox_from_proto(payload)?))
        }
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => Ok(None),
        Err(error) => Err(anyhow!(error).context("failed to get sandbox via daemon")),
    }
}

async fn daemon_create_sandbox(
    state_db: &Path,
    sandbox_id: &str,
    cpus: u8,
    memory: u64,
    labels: BTreeMap<String, String>,
) -> anyhow::Result<Sandbox> {
    let mut client = connect_control_plane_for_state_db(state_db).await?;
    let mut stream = client
        .create_sandbox_stream(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: sandbox_id.to_string(),
            cpus: u32::from(cpus),
            memory_mb: memory,
            labels: labels.into_iter().collect(),
        })
        .await
        .context("failed to create sandbox via daemon")?;
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .context("failed reading create sandbox stream")?
    {
        match event.payload {
            Some(runtime_v2::create_sandbox_event::Payload::Progress(progress)) => {
                println!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::create_sandbox_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            None => {}
        }
    }
    let completion = completion
        .ok_or_else(|| anyhow!("daemon create_sandbox stream ended without completion"))?;
    let response = completion
        .response
        .ok_or_else(|| anyhow!("daemon create_sandbox completion missing response payload"))?;
    let payload = response
        .sandbox
        .ok_or_else(|| anyhow!("daemon create_sandbox returned missing payload"))?;
    sandbox_from_proto(payload)
}

async fn daemon_terminate_sandbox(
    state_db: &Path,
    sandbox_id: &str,
) -> anyhow::Result<Option<Sandbox>> {
    let mut client = connect_control_plane_for_state_db(state_db).await?;
    match client
        .terminate_sandbox_stream(runtime_v2::TerminateSandboxRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
    {
        Ok(mut stream) => {
            let mut completion = None;
            while let Some(event) = stream
                .message()
                .await
                .context("failed reading terminate sandbox stream")?
            {
                match event.payload {
                    Some(runtime_v2::terminate_sandbox_event::Payload::Progress(progress)) => {
                        println!("[{}] {}", progress.phase, progress.detail);
                    }
                    Some(runtime_v2::terminate_sandbox_event::Payload::Completion(done)) => {
                        completion = Some(done);
                    }
                    None => {}
                }
            }
            let completion = completion.ok_or_else(|| {
                anyhow!("daemon terminate_sandbox stream ended without completion")
            })?;
            let response = completion.response.ok_or_else(|| {
                anyhow!("daemon terminate_sandbox completion missing response payload")
            })?;
            let payload = response
                .sandbox
                .ok_or_else(|| anyhow!("daemon terminate_sandbox returned missing payload"))?;
            Ok(Some(sandbox_from_proto(payload)?))
        }
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => Ok(None),
        Err(error) => Err(anyhow!(error).context("failed to terminate sandbox via daemon")),
    }
}

async fn daemon_open_sandbox_shell(
    state_db: &Path,
    sandbox_id: &str,
) -> anyhow::Result<runtime_v2::OpenSandboxShellResponse> {
    let mut client = connect_control_plane_for_state_db(state_db).await?;
    let mut stream = client
        .open_sandbox_shell(runtime_v2::OpenSandboxShellRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
        .context("failed to open sandbox shell via daemon")?;
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .context("failed reading open sandbox shell stream")?
    {
        match event.payload {
            Some(runtime_v2::open_sandbox_shell_event::Payload::Progress(progress)) => {
                println!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::open_sandbox_shell_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            None => {}
        }
    }
    completion.ok_or_else(|| anyhow!("daemon open_sandbox_shell stream ended without completion"))
}

async fn daemon_close_sandbox_shell(
    state_db: &Path,
    sandbox_id: &str,
    execution_id: Option<&str>,
) -> anyhow::Result<runtime_v2::CloseSandboxShellResponse> {
    let mut client = connect_control_plane_for_state_db(state_db).await?;
    let mut stream = client
        .close_sandbox_shell(runtime_v2::CloseSandboxShellRequest {
            sandbox_id: sandbox_id.to_string(),
            execution_id: execution_id.unwrap_or_default().to_string(),
            metadata: None,
        })
        .await
        .context("failed to close sandbox shell via daemon")?;
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .context("failed reading close sandbox shell stream")?
    {
        match event.payload {
            Some(runtime_v2::close_sandbox_shell_event::Payload::Progress(progress)) => {
                println!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::close_sandbox_shell_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            None => {}
        }
    }
    completion.ok_or_else(|| anyhow!("daemon close_sandbox_shell stream ended without completion"))
}

// ── Top-level argument types ────────────────────────────────────

/// Arguments for `vz ls`.
#[derive(Args, Debug)]
pub struct SandboxListArgs {
    /// Path to the state database.
    #[arg(long)]
    state_db: Option<PathBuf>,

    /// Output as JSON.
    #[arg(long)]
    json: bool,
}

/// Arguments for `vz inspect`.
#[derive(Args, Debug)]
pub struct SandboxInspectArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Path to the state database.
    #[arg(long)]
    state_db: Option<PathBuf>,
}

/// Arguments for `vz rm`.
#[derive(Args, Debug)]
pub struct SandboxTerminateArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Path to the state database.
    #[arg(long)]
    state_db: Option<PathBuf>,
}

/// Arguments for `vz attach`.
#[derive(Args, Debug)]
pub struct SandboxAttachArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Path to the state database.
    #[arg(long)]
    state_db: Option<PathBuf>,
}

/// Arguments for `vz close-shell`.
#[derive(Args, Debug)]
pub struct SandboxCloseShellArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,

    /// Explicit execution identifier to close (defaults to active shell session).
    #[arg(long)]
    pub execution_id: Option<String>,

    /// Path to the state database.
    #[arg(long)]
    state_db: Option<PathBuf>,
}

// ── Default sandbox command (no subcommand) ─────────────────────

/// Handle the default `vz` command — create or resume a sandbox.
///
/// When invoked with no subcommand:
/// - `vz -c`: continue most recent sandbox for the current directory
/// - `vz -r <name>`: resume a specific sandbox by name or ID
/// - `vz`: create a new sandbox bound to the current directory
pub async fn cmd_default_sandbox(
    continue_last: bool,
    resume: Option<String>,
    name: Option<String>,
    cpus: u8,
    memory: u64,
    base_image_ref: Option<String>,
    main_container: Option<String>,
) -> anyhow::Result<()> {
    let state_db = default_state_db_path();
    let cwd = std::env::current_dir().context("failed to get current directory")?;

    if (continue_last || resume.is_some()) && (base_image_ref.is_some() || main_container.is_some())
    {
        bail!("--base-image and --main-container are only valid when creating a new sandbox");
    }

    if continue_last {
        return cmd_continue_sandbox(&state_db, &cwd).await;
    }

    if let Some(ref target) = resume {
        return cmd_resume_sandbox(&state_db, target).await;
    }

    // Create a new sandbox.
    cmd_create_sandbox(
        &state_db,
        &cwd,
        name,
        cpus,
        memory,
        base_image_ref,
        main_container,
    )
    .await
}

/// Continue the most recent sandbox for the current directory.
async fn cmd_continue_sandbox(state_db: &Path, cwd: &Path) -> anyhow::Result<()> {
    let sandboxes = daemon_list_sandboxes(state_db).await?;
    let cwd_str = cwd.to_string_lossy();

    // Find sandbox matching this directory.
    let matching: Vec<_> = sandboxes
        .iter()
        .filter(|s| {
            s.labels.get(SANDBOX_PROJECT_DIR_LABEL).map(|d| d.as_str()) == Some(&*cwd_str)
                && !s.state.is_terminal()
        })
        .collect();

    if let Some(sandbox) = matching.last() {
        println!("Resuming sandbox {}...", sandbox.sandbox_id);
        return attach_to_sandbox_by_id(state_db, &sandbox.sandbox_id).await;
    }

    // Fall back to most recent non-terminal sandbox.
    let most_recent = sandboxes.iter().rev().find(|s| !s.state.is_terminal());

    match most_recent {
        Some(sandbox) => {
            println!("Resuming sandbox {}...", sandbox.sandbox_id);
            attach_to_sandbox_by_id(state_db, &sandbox.sandbox_id).await
        }
        None => bail!("no active sandboxes found; run `vz` to create one"),
    }
}

/// Resume a specific sandbox by name or ID.
async fn cmd_resume_sandbox(state_db: &Path, target: &str) -> anyhow::Result<()> {
    // Try exact ID match first.
    if let Some(sandbox) = daemon_get_sandbox(state_db, target).await? {
        if sandbox.state.is_terminal() {
            bail!("sandbox {target} is in terminal state");
        }
        println!("Resuming sandbox {target}...");
        return attach_to_sandbox_by_id(state_db, target).await;
    }

    // Try name label match.
    let sandboxes = daemon_list_sandboxes(state_db).await?;
    let by_name: Vec<_> = sandboxes
        .iter()
        .filter(|s| {
            s.labels.get("name").map(|n| n.as_str()) == Some(target) && !s.state.is_terminal()
        })
        .collect();

    match by_name.last() {
        Some(sandbox) => {
            println!("Resuming sandbox {} ({target})...", sandbox.sandbox_id);
            attach_to_sandbox_by_id(state_db, &sandbox.sandbox_id).await
        }
        None => bail!("sandbox {target} not found"),
    }
}

/// Create a new sandbox and attach to it.
async fn cmd_create_sandbox(
    state_db: &Path,
    cwd: &Path,
    name: Option<String>,
    cpus: u8,
    memory: u64,
    base_image_ref: Option<String>,
    main_container: Option<String>,
) -> anyhow::Result<()> {
    let sandbox_id = generate_sandbox_id();
    let display_name = name.as_deref().unwrap_or(&sandbox_id);
    let resolved_base_image = base_image_ref
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| DEFAULT_SANDBOX_BASE_IMAGE_REF.to_string());

    let mut labels = BTreeMap::new();
    labels.insert(
        SANDBOX_PROJECT_DIR_LABEL.to_string(),
        cwd.to_string_lossy().to_string(),
    );
    labels.insert("source".to_string(), "standalone".to_string());
    if let Some(ref n) = name {
        labels.insert("name".to_string(), n.clone());
    }
    labels.insert(
        SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
        resolved_base_image.clone(),
    );
    if let Some(main_container) = main_container.as_deref().map(str::trim)
        && !main_container.is_empty()
    {
        labels.insert(
            SANDBOX_LABEL_MAIN_CONTAINER.to_string(),
            main_container.to_string(),
        );
    }

    // Ensure state directory exists.
    if let Some(parent) = state_db.parent() {
        std::fs::create_dir_all(parent).ok();
    }

    println!("Booting sandbox {display_name}...");
    println!("Mounting {} → /workspace", cwd.display());

    let sandbox =
        daemon_create_sandbox(state_db, &sandbox_id, cpus, memory, labels.clone()).await?;
    match attach_to_sandbox_by_id(state_db, &sandbox.sandbox_id).await {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = daemon_terminate_sandbox(state_db, &sandbox.sandbox_id).await;
            Err(error)
        }
    }
}

enum AttachInputEvent {
    Bytes(Vec<u8>),
    Resize { cols: u16, rows: u16 },
    Detach,
}

async fn attach_to_execution_interactive(
    state_db: &Path,
    execution_id: &str,
) -> anyhow::Result<()> {
    use crossterm::event::{self, Event};
    use crossterm::terminal;
    use std::io::Write;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let mut client = connect_control_plane_for_state_db(state_db).await?;
    let execution_id = execution_id.to_string();
    let mut stream = client
        .stream_exec_output(runtime_v2::StreamExecOutputRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        })
        .await
        .with_context(|| {
            format!("failed to stream sandbox execution output for `{execution_id}`")
        })?;

    terminal::enable_raw_mode().context("failed to enable raw mode")?;
    let (input_tx, mut input_rx) = tokio::sync::mpsc::unbounded_channel::<AttachInputEvent>();
    let stop_input = Arc::new(AtomicBool::new(false));
    let stop_input_worker = Arc::clone(&stop_input);
    let input_handle = tokio::task::spawn_blocking(move || {
        let mut detach_prefix_pending = false;
        loop {
            if stop_input_worker.load(Ordering::Relaxed) {
                break;
            }

            match event::poll(std::time::Duration::from_millis(100)) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(_) => break,
            }

            match event::read() {
                Ok(Event::Key(key_event)) => {
                    let bytes = key_event_to_bytes(&key_event);
                    if detach_prefix_pending {
                        detach_prefix_pending = false;
                        if is_detach_confirm(bytes.as_slice()) {
                            if input_tx.send(AttachInputEvent::Detach).is_err() {
                                break;
                            }
                            continue;
                        }
                        if input_tx.send(AttachInputEvent::Bytes(vec![0x10])).is_err() {
                            break;
                        }
                    } else if is_detach_prefix(bytes.as_slice()) {
                        detach_prefix_pending = true;
                        continue;
                    }

                    if bytes.is_empty() {
                        continue;
                    }
                    if input_tx.send(AttachInputEvent::Bytes(bytes)).is_err() {
                        break;
                    }
                }
                Ok(Event::Resize(new_cols, new_rows)) => {
                    if input_tx
                        .send(AttachInputEvent::Resize {
                            cols: new_cols,
                            rows: new_rows,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    let interaction_result = async {
        let mut stdout = std::io::stdout();
        let mut stderr = std::io::stderr();
        let mut detached = false;
        let mut terminal_exit_code: Option<i32> = None;
        loop {
            tokio::select! {
                maybe_input = input_rx.recv() => {
                    let Some(input) = maybe_input else {
                        continue;
                    };
                    match input {
                        AttachInputEvent::Bytes(bytes) => {
                            client
                                .write_exec_stdin(runtime_v2::WriteExecStdinRequest {
                                    execution_id: execution_id.clone(),
                                    data: bytes,
                                    metadata: None,
                                })
                                .await
                                .with_context(|| format!("failed to write stdin to `{execution_id}`"))?;
                        }
                        AttachInputEvent::Resize { cols, rows } => {
                            client
                                .resize_exec_pty(runtime_v2::ResizeExecPtyRequest {
                                    execution_id: execution_id.clone(),
                                    cols: u32::from(cols),
                                    rows: u32::from(rows),
                                    metadata: None,
                                })
                                .await
                                .with_context(|| format!("failed to resize PTY for `{execution_id}`"))?;
                        }
                        AttachInputEvent::Detach => {
                            detached = true;
                            break;
                        }
                    }
                }
                maybe_event = stream.message() => {
                    let maybe_event = maybe_event
                        .with_context(|| format!("failed reading stream for `{execution_id}`"))?;
                    let Some(event) = maybe_event else {
                        break;
                    };
                    match event.payload {
                        Some(runtime_v2::exec_output_event::Payload::Stdout(chunk)) => {
                            if !chunk.is_empty() {
                                stdout
                                    .write_all(&chunk)
                                    .context("failed writing sandbox stdout")?;
                                stdout.flush().context("failed flushing sandbox stdout")?;
                            }
                        }
                        Some(runtime_v2::exec_output_event::Payload::Stderr(chunk)) => {
                            if !chunk.is_empty() {
                                stderr
                                    .write_all(&chunk)
                                    .context("failed writing sandbox stderr")?;
                                stderr.flush().context("failed flushing sandbox stderr")?;
                            }
                        }
                        Some(runtime_v2::exec_output_event::Payload::ExitCode(code)) => {
                            terminal_exit_code = Some(code);
                            break;
                        }
                        Some(runtime_v2::exec_output_event::Payload::Error(message)) => {
                            bail!("sandbox execution `{execution_id}` reported error: {message}");
                        }
                        None => {}
                    }
                }
            }
        }

        if terminal_exit_code.is_none() && !detached {
            if let Ok(response) = client
                .get_execution(runtime_v2::GetExecutionRequest {
                    execution_id: execution_id.clone(),
                    metadata: None,
                })
                .await
                && let Some(execution) = response.execution
            {
                terminal_exit_code = Some(execution.exit_code);
            }
        }

        Ok::<_, anyhow::Error>((detached, terminal_exit_code))
    }
    .await;

    terminal::disable_raw_mode().ok();
    stop_input.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), input_handle).await;
    let (detached, terminal_exit_code) = interaction_result?;

    if detached {
        eprintln!("\nDetached (Ctrl-P Ctrl-Q). Session remains active.");
        return Ok(());
    }

    if let Some(exit_code) = terminal_exit_code
        && exit_code != 0
    {
        bail!("sandbox shell exited with status {exit_code}");
    }

    println!();
    Ok(())
}

/// Convert a crossterm key event to the byte sequence the terminal expects.
fn key_event_to_bytes(key: &crossterm::event::KeyEvent) -> Vec<u8> {
    use crossterm::event::{KeyCode, KeyModifiers};

    match key.code {
        KeyCode::Char(c) => {
            if key.modifiers.contains(KeyModifiers::CONTROL) {
                // Ctrl+A = 0x01, Ctrl+B = 0x02, etc.
                let ctrl_byte = (c as u8).wrapping_sub(b'a').wrapping_add(1);
                if ctrl_byte <= 26 {
                    return vec![ctrl_byte];
                }
            }
            if key.modifiers.contains(KeyModifiers::ALT) {
                // Alt+key sends ESC prefix followed by the key byte.
                let mut buf = [0u8; 4];
                let s = c.encode_utf8(&mut buf);
                let mut out = vec![0x1b];
                out.extend_from_slice(s.as_bytes());
                return out;
            }
            let mut buf = [0u8; 4];
            let s = c.encode_utf8(&mut buf);
            s.as_bytes().to_vec()
        }
        KeyCode::Enter => vec![b'\r'],
        KeyCode::Backspace => vec![0x7f],
        KeyCode::Tab => vec![b'\t'],
        KeyCode::Esc => vec![0x1b],
        KeyCode::Up => b"\x1b[A".to_vec(),
        KeyCode::Down => b"\x1b[B".to_vec(),
        KeyCode::Right => b"\x1b[C".to_vec(),
        KeyCode::Left => b"\x1b[D".to_vec(),
        KeyCode::Home => b"\x1b[H".to_vec(),
        KeyCode::End => b"\x1b[F".to_vec(),
        KeyCode::PageUp => b"\x1b[5~".to_vec(),
        KeyCode::PageDown => b"\x1b[6~".to_vec(),
        KeyCode::Delete => b"\x1b[3~".to_vec(),
        KeyCode::Insert => b"\x1b[2~".to_vec(),
        KeyCode::F(n) => match n {
            1 => b"\x1bOP".to_vec(),
            2 => b"\x1bOQ".to_vec(),
            3 => b"\x1bOR".to_vec(),
            4 => b"\x1bOS".to_vec(),
            5 => b"\x1b[15~".to_vec(),
            6 => b"\x1b[17~".to_vec(),
            7 => b"\x1b[18~".to_vec(),
            8 => b"\x1b[19~".to_vec(),
            9 => b"\x1b[20~".to_vec(),
            10 => b"\x1b[21~".to_vec(),
            11 => b"\x1b[23~".to_vec(),
            12 => b"\x1b[24~".to_vec(),
            _ => vec![],
        },
        _ => vec![],
    }
}

fn is_detach_prefix(bytes: &[u8]) -> bool {
    bytes == [0x10]
}

fn is_detach_confirm(bytes: &[u8]) -> bool {
    matches!(bytes, [0x11] | [b'q'] | [b'Q'])
}

// ── Top-level sandbox commands ──────────────────────────────────

/// List all sandboxes (`vz ls`).
pub async fn cmd_list(args: SandboxListArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let sandboxes = daemon_list_sandboxes(&state_db).await?;

    if args.json {
        let json =
            serde_json::to_string_pretty(&sandboxes).context("failed to serialize sandboxes")?;
        println!("{json}");
        return Ok(());
    }

    if sandboxes.is_empty() {
        println!("No sandboxes found.");
        return Ok(());
    }

    println!(
        "{:<16} {:<12} {:<6} {:<10} {:<30} {:<12}",
        "SANDBOX", "STATE", "CPUS", "MEMORY MB", "DIR", "SOURCE"
    );
    for sandbox in &sandboxes {
        let cpus = sandbox
            .spec
            .cpus
            .map(|c| c.to_string())
            .unwrap_or_else(|| "-".to_string());
        let memory = sandbox
            .spec
            .memory_mb
            .map(|m| m.to_string())
            .unwrap_or_else(|| "-".to_string());
        let state = serde_json::to_string(&sandbox.state)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let dir = sandbox
            .labels
            .get(SANDBOX_PROJECT_DIR_LABEL)
            .map(|d| {
                // Shorten home dir.
                if let Ok(home) = std::env::var("HOME") {
                    if let Some(rest) = d.strip_prefix(&home) {
                        return format!("~{rest}");
                    }
                }
                d.clone()
            })
            .unwrap_or_else(|| "-".to_string());
        let source = sandbox
            .labels
            .get("source")
            .cloned()
            .unwrap_or_else(|| "-".to_string());

        // Use name label if available, otherwise truncate sandbox_id.
        let display_id = sandbox.labels.get("name").cloned().unwrap_or_else(|| {
            if sandbox.sandbox_id.len() > 14 {
                format!("{}…", &sandbox.sandbox_id[..13])
            } else {
                sandbox.sandbox_id.clone()
            }
        });

        println!(
            "{:<16} {:<12} {:<6} {:<10} {:<30} {:<12}",
            display_id, state, cpus, memory, dir, source
        );
    }

    Ok(())
}

/// Show detailed sandbox information (`vz inspect`).
pub async fn cmd_inspect(args: SandboxInspectArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let sandbox = daemon_get_sandbox(&state_db, &args.sandbox_id).await?;

    match sandbox {
        Some(s) => {
            let json = serde_json::to_string_pretty(&s).context("failed to serialize sandbox")?;
            println!("{json}");
        }
        None => bail!("sandbox {} not found", args.sandbox_id),
    }

    Ok(())
}

/// Terminate (remove) a sandbox (`vz rm`).
pub async fn cmd_terminate(args: SandboxTerminateArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let existing = daemon_get_sandbox(&state_db, &args.sandbox_id)
        .await?
        .ok_or_else(|| anyhow!("sandbox {} not found", args.sandbox_id))?;

    if existing.state.is_terminal() {
        println!("Sandbox {} is already in terminal state.", args.sandbox_id);
        return Ok(());
    }

    let sandbox = daemon_terminate_sandbox(&state_db, &args.sandbox_id)
        .await?
        .ok_or_else(|| anyhow!("sandbox {} not found", args.sandbox_id))?;

    let state = serde_json::to_string(&sandbox.state)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string();

    println!("Sandbox {} terminated (state: {state}).", args.sandbox_id);

    Ok(())
}

/// Attach to an existing sandbox (`vz attach`).
pub async fn cmd_attach(args: SandboxAttachArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    attach_to_sandbox_by_id(&state_db, &args.sandbox_id).await
}

/// Close an active sandbox shell session (`vz close-shell`).
pub async fn cmd_close_shell(args: SandboxCloseShellArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let response =
        daemon_close_sandbox_shell(&state_db, &args.sandbox_id, args.execution_id.as_deref())
            .await?;
    println!(
        "Closed sandbox shell session {} for sandbox {}.",
        response.execution_id, response.sandbox_id
    );
    Ok(())
}

/// Attach to a sandbox by its ID (shared helper).
async fn attach_to_sandbox_by_id(state_db: &Path, sandbox_id: &str) -> anyhow::Result<()> {
    let opened = daemon_open_sandbox_shell(state_db, sandbox_id).await?;
    let execution_id = opened.execution_id.trim();
    if execution_id.is_empty() {
        bail!("daemon open_sandbox_shell returned empty execution_id");
    }
    attach_to_execution_interactive(state_db, execution_id).await
}

/// Generate a short sandbox ID.
fn generate_sandbox_id() -> String {
    let id = uuid::Uuid::new_v4();
    let hex = id.as_simple().to_string();
    format!("vz-{}", &hex[..4])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detach_prefix_matches_ctrl_p_byte() {
        assert!(is_detach_prefix(&[0x10]));
        assert!(!is_detach_prefix(&[0x11]));
        assert!(!is_detach_prefix(b"p"));
    }

    #[test]
    fn detach_confirm_accepts_ctrl_q_and_q_fallback() {
        assert!(is_detach_confirm(&[0x11]));
        assert!(is_detach_confirm(b"q"));
        assert!(is_detach_confirm(b"Q"));
        assert!(!is_detach_confirm(&[0x10]));
        assert!(!is_detach_confirm(b"x"));
    }
}
