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
    let response = client
        .create_sandbox(runtime_v2::CreateSandboxRequest {
            metadata: None,
            stack_name: sandbox_id.to_string(),
            cpus: u32::from(cpus),
            memory_mb: memory,
            labels: labels.into_iter().collect(),
        })
        .await
        .context("failed to create sandbox via daemon")?;
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
        .terminate_sandbox(runtime_v2::TerminateSandboxRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
    {
        Ok(response) => {
            let payload = response
                .sandbox
                .ok_or_else(|| anyhow!("daemon terminate_sandbox returned missing payload"))?;
            Ok(Some(sandbox_from_proto(payload)?))
        }
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => Ok(None),
        Err(error) => Err(anyhow!(error).context("failed to terminate sandbox via daemon")),
    }
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
            s.labels.get("project_dir").map(|d| d.as_str()) == Some(&*cwd_str)
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

    let mut labels = BTreeMap::new();
    labels.insert("project_dir".to_string(), cwd.to_string_lossy().to_string());
    labels.insert("source".to_string(), "standalone".to_string());
    if let Some(ref n) = name {
        labels.insert("name".to_string(), n.clone());
    }
    if let Some(base_image_ref) = base_image_ref.as_deref().map(str::trim)
        && !base_image_ref.is_empty()
    {
        labels.insert(
            SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
            base_image_ref.to_string(),
        );
    }
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

    // Boot the VM and attach.
    #[cfg(target_os = "macos")]
    {
        let snapshot_path = sandbox_snapshot_path(state_db, &sandbox_id);
        let sandbox =
            daemon_create_sandbox(state_db, &sandbox_id, cpus, memory, labels.clone()).await?;
        match boot_and_attach(&sandbox.spec, cwd, &snapshot_path).await {
            Ok(()) => {}
            Err(e) => {
                let _ = daemon_terminate_sandbox(state_db, &sandbox_id).await;
                return Err(e);
            }
        }
    }

    #[cfg(not(target_os = "macos"))]
    {
        bail!("sandbox creation requires macOS with Apple Silicon");
    }

    Ok(())
}

/// Boot a standalone VM and attach interactively.
#[cfg(target_os = "macos")]
async fn boot_and_attach(
    spec: &SandboxSpec,
    project_dir: &Path,
    snapshot_path: &Path,
) -> anyhow::Result<()> {
    use std::sync::Arc;
    use vz::SharedDirConfig;
    use vz_linux::{LinuxVm, LinuxVmConfig, ensure_kernel};

    let kernel = ensure_kernel()
        .await
        .context("failed to ensure Linux kernel")?;

    let mut vm_config = LinuxVmConfig::new(kernel.kernel, kernel.initramfs);
    vm_config.cpus = spec.cpus.unwrap_or(2);
    vm_config.memory_mb = spec.memory_mb.unwrap_or(2048);
    let machine_identifier_path = sandbox_machine_identifier_path(snapshot_path);
    let (machine_identifier, machine_identifier_created) =
        load_or_create_machine_identifier(&machine_identifier_path)?;
    if snapshot_path.exists() && machine_identifier_created {
        eprintln!(
            "warning: snapshot exists but machine identifier was missing; discarding stale snapshot"
        );
        let _ = std::fs::remove_file(snapshot_path);
    }
    vm_config.machine_identifier = Some(machine_identifier);

    // Add VirtioFS share for project directory.
    vm_config.shared_dirs.push(SharedDirConfig {
        tag: "workspace".to_string(),
        source: project_dir.to_path_buf(),
        read_only: false,
    });

    // Configure serial port (required for boot — without it, kernel blocks on console writes).
    vm_config.serial_log_file = Some(PathBuf::from("/dev/null"));

    let vm = LinuxVm::create(vm_config)
        .await
        .context("failed to create VM")?;

    let agent_timeout = std::time::Duration::from_secs(60);
    let boot_time = if snapshot_path.exists() {
        eprintln!("Restoring sandbox snapshot {}...", snapshot_path.display());
        match vm
            .restore_and_wait_for_agent(snapshot_path, agent_timeout)
            .await
        {
            Ok(duration) => duration,
            Err(error) => {
                eprintln!(
                    "warning: failed to restore snapshot ({}), falling back to cold boot",
                    error
                );
                let _ = std::fs::remove_file(snapshot_path);
                vm.start_and_wait_for_agent_with_progress(agent_timeout, |attempts, last_err| {
                    if attempts % 5 == 0 {
                        eprintln!("  waiting for guest agent (attempt {attempts}: {last_err})...");
                    }
                })
                .await
                .context("failed to boot VM")?
            }
        }
    } else {
        vm.start_and_wait_for_agent_with_progress(agent_timeout, |attempts, last_err| {
            if attempts % 5 == 0 {
                eprintln!("  waiting for guest agent (attempt {attempts}: {last_err})...");
            }
        })
        .await
        .context("failed to boot VM")?
    };

    eprintln!("Sandbox ready ({:.1}s)", boot_time.as_secs_f64());

    // Mount devpts for PTY support (required before interactive shell).
    let timeout = std::time::Duration::from_secs(10);
    let devpts_result = vm
        .exec_capture(
            "/bin/busybox".to_string(),
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "/bin/busybox mkdir -p /dev/pts && ( /bin/busybox grep -q '^devpts /dev/pts devpts ' /proc/mounts || /bin/busybox mount -t devpts devpts /dev/pts )".to_string(),
            ],
            timeout,
        )
        .await;

    if let Ok(output) = &devpts_result {
        if output.exit_code != 0 {
            eprintln!(
                "warning: failed to mount devpts: {}{}",
                output.stdout, output.stderr
            );
        }
    }

    // Mount workspace inside guest.
    let mount_result = vm
        .exec_capture(
            "/bin/busybox".to_string(),
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "/bin/busybox mkdir -p /workspace && ( /bin/busybox grep -q '^workspace /workspace virtiofs ' /proc/mounts || /bin/busybox mount -t virtiofs workspace /workspace )".to_string(),
            ],
            timeout,
        )
        .await;

    match &mount_result {
        Ok(output) if output.exit_code != 0 => {
            eprintln!(
                "warning: failed to mount workspace: {}{}",
                output.stdout, output.stderr
            );
        }
        Err(e) => {
            eprintln!("warning: failed to mount workspace: {e}");
        }
        _ => {}
    }

    let (shell, shell_args) = startup_command_from_spec(spec)?;
    let vm = Arc::new(vm);
    attach_interactive(vm, &shell, &shell_args, "/workspace", snapshot_path).await
}

#[cfg(any(test, target_os = "macos"))]
fn startup_command_from_spec(spec: &SandboxSpec) -> anyhow::Result<(String, Vec<String>)> {
    if let Some(main_container) = spec.main_container.as_deref().map(str::trim)
        && !main_container.is_empty()
        && let Some(command) = parse_main_container_startup_command(main_container)?
    {
        return Ok(command);
    }

    Ok((
        default_shell_for_base_image(spec.base_image_ref.as_deref()).to_string(),
        Vec::new(),
    ))
}

#[cfg(any(test, target_os = "macos"))]
fn parse_main_container_startup_command(
    main_container: &str,
) -> anyhow::Result<Option<(String, Vec<String>)>> {
    let command_hint = main_container.trim();
    if command_hint.is_empty() {
        return Ok(None);
    }

    // Keep backward compatibility for ID-style workload names like
    // "workspace-main": treat command-looking values as entrypoint overrides.
    let looks_like_command = command_hint.contains(char::is_whitespace)
        || command_hint.starts_with('/')
        || command_hint.contains('/')
        || matches!(command_hint, "sh" | "bash" | "zsh" | "fish" | "nu");
    if !looks_like_command {
        return Ok(None);
    }

    let words = shell_words::split(command_hint)
        .map_err(|error| anyhow!("invalid sandbox main_container command: {error}"))?;
    if words.is_empty() {
        return Ok(None);
    }

    let mut words = words.into_iter();
    let command = match words.next() {
        Some(command) => command,
        None => return Ok(None),
    };
    let args = words.collect();
    Ok(Some((command, args)))
}

#[cfg(any(test, target_os = "macos"))]
fn default_shell_for_base_image(base_image_ref: Option<&str>) -> &'static str {
    let Some(base_image_ref) = base_image_ref else {
        return "/bin/sh";
    };
    let normalized = base_image_ref.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return "/bin/sh";
    }

    if [
        "ubuntu", "debian", "fedora", "centos", "rocky", "alma", "arch",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
    {
        "/bin/bash"
    } else {
        "/bin/sh"
    }
}

/// Attach interactively to a VM with a PTY session.
#[cfg(target_os = "macos")]
async fn attach_interactive(
    vm: std::sync::Arc<vz_linux::LinuxVm>,
    shell: &str,
    shell_args: &[String],
    working_dir: &str,
    snapshot_path: &Path,
) -> anyhow::Result<()> {
    use crossterm::event::{self, Event};
    use crossterm::terminal;
    use std::io::Write;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let (cols, rows) = terminal::size().unwrap_or((80, 24));

    let wd = if working_dir.is_empty() {
        None
    } else {
        Some(working_dir)
    };
    let shell_args_refs: Vec<&str> = shell_args.iter().map(String::as_str).collect();
    let (mut stream, exec_id) = vm
        .exec_interactive(shell, &shell_args_refs, wd, rows as u32, cols as u32)
        .await
        .context("failed to start interactive session")?;

    terminal::enable_raw_mode().context("failed to enable raw mode")?;

    let vm_input = vm.clone();
    let input_exec_id = exec_id;
    let stop_input = Arc::new(AtomicBool::new(false));
    let stop_input_worker = stop_input.clone();
    let detach_notify = Arc::new(tokio::sync::Notify::new());
    let detach_notify_worker = detach_notify.clone();

    // Input task: read terminal events and forward to guest.
    let input_handle = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
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
                            detach_notify_worker.notify_one();
                            break;
                        }
                        if rt
                            .block_on(vm_input.stdin_write(input_exec_id, &[0x10]))
                            .is_err()
                        {
                            break;
                        }
                    } else if is_detach_prefix(bytes.as_slice()) {
                        detach_prefix_pending = true;
                        continue;
                    }

                    if bytes.is_empty() {
                        continue;
                    }
                    if rt
                        .block_on(vm_input.stdin_write(input_exec_id, &bytes))
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(Event::Resize(new_cols, new_rows)) => {
                    let _ = rt.block_on(vm_input.resize_exec_pty(
                        input_exec_id,
                        new_rows as u32,
                        new_cols as u32,
                    ));
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    // Output task: read from guest stream and write to stdout.
    let mut stdout = std::io::stdout();
    use vz::protocol::ExecEvent;
    let mut detached = false;
    loop {
        tokio::select! {
            _ = detach_notify.notified() => {
                detached = true;
                break;
            }
            maybe_ev = stream.next() => {
                let Some(ev) = maybe_ev else {
                    break;
                };
                match ev {
                    ExecEvent::Stdout(data) | ExecEvent::Stderr(data) => {
                        stdout.write_all(&data).ok();
                        stdout.flush().ok();
                    }
                    ExecEvent::Exit(_code) => {
                        break;
                    }
                }
            }
        }
    }

    if detached {
        eprintln!("\nDetached (Ctrl-P Ctrl-Q). Saving sandbox snapshot...");
    }

    terminal::disable_raw_mode().ok();
    stop_input.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), input_handle).await;

    if let Some(parent) = snapshot_path.parent()
        && let Err(error) = std::fs::create_dir_all(parent)
    {
        eprintln!(
            "warning: failed to prepare snapshot directory {}: {}",
            parent.display(),
            error
        );
    }
    if let Err(error) = vm.save_state_snapshot(snapshot_path).await {
        eprintln!(
            "warning: failed to save sandbox snapshot {}: {}",
            snapshot_path.display(),
            error
        );
    }
    if let Err(error) = vm.stop().await {
        eprintln!("warning: failed to stop VM after snapshot save: {error}");
    }

    println!();
    Ok(())
}

/// Convert a crossterm key event to the byte sequence the terminal expects.
#[cfg(target_os = "macos")]
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

#[cfg(any(test, target_os = "macos"))]
fn is_detach_prefix(bytes: &[u8]) -> bool {
    bytes == [0x10]
}

#[cfg(any(test, target_os = "macos"))]
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
            .get("project_dir")
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

    let snapshot_path = sandbox_snapshot_path(&state_db, &args.sandbox_id);
    if snapshot_path.exists()
        && let Err(error) = std::fs::remove_file(&snapshot_path)
    {
        eprintln!(
            "warning: failed to remove sandbox snapshot {}: {}",
            snapshot_path.display(),
            error
        );
    }
    let machine_identifier_path = sandbox_machine_identifier_path(&snapshot_path);
    if machine_identifier_path.exists()
        && let Err(error) = std::fs::remove_file(&machine_identifier_path)
    {
        eprintln!(
            "warning: failed to remove sandbox machine identifier {}: {}",
            machine_identifier_path.display(),
            error
        );
    }

    println!("Sandbox {} terminated (state: {state}).", args.sandbox_id);

    Ok(())
}

/// Attach to an existing sandbox (`vz attach`).
pub async fn cmd_attach(_args: SandboxAttachArgs) -> anyhow::Result<()> {
    // Attachment restores/boots a VM from the sandbox snapshot path and opens
    // a fresh interactive session.
    #[cfg(target_os = "macos")]
    {
        let state_db = _args.state_db.unwrap_or_else(default_state_db_path);
        return attach_to_sandbox_by_id(&state_db, &_args.sandbox_id).await;
    }

    #[cfg(not(target_os = "macos"))]
    bail!("sandbox attach requires macOS with Apple Silicon");
}

/// Attach to a sandbox by its ID (shared helper).
async fn attach_to_sandbox_by_id(state_db: &Path, sandbox_id: &str) -> anyhow::Result<()> {
    let sandbox = daemon_get_sandbox(state_db, sandbox_id)
        .await?
        .ok_or_else(|| anyhow!("sandbox {sandbox_id} not found"))?;

    if sandbox.state.is_terminal() {
        bail!("sandbox {sandbox_id} is in terminal state");
    }

    let project_dir = sandbox.labels.get("project_dir").cloned();

    #[cfg(target_os = "macos")]
    {
        // Re-boot the VM for this sandbox (VM handles are ephemeral).
        // Persist/restore snapshot state across attach sessions.
        let spec = &sandbox.spec;
        let cwd = project_dir
            .as_deref()
            .map(std::path::Path::new)
            .unwrap_or_else(|| std::path::Path::new("/"));
        let snapshot_path = sandbox_snapshot_path(state_db, sandbox_id);
        boot_and_attach(spec, cwd, &snapshot_path).await
    }

    #[cfg(not(target_os = "macos"))]
    {
        let _ = project_dir;
        bail!("sandbox attach requires macOS with Apple Silicon");
    }
}

/// Generate a short sandbox ID.
fn generate_sandbox_id() -> String {
    let id = uuid::Uuid::new_v4();
    let hex = id.as_simple().to_string();
    format!("vz-{}", &hex[..4])
}

fn sandbox_snapshot_path(state_db: &Path, sandbox_id: &str) -> PathBuf {
    let base = state_db
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    base.join(".vz-runtime")
        .join("sandboxes")
        .join(format!("{sandbox_id}.state"))
}

fn sandbox_machine_identifier_path(snapshot_path: &Path) -> PathBuf {
    let mut path = snapshot_path.to_path_buf();
    path.set_extension("machine-id");
    path
}

#[cfg(target_os = "macos")]
fn load_or_create_machine_identifier(path: &Path) -> anyhow::Result<(Vec<u8>, bool)> {
    match std::fs::read(path) {
        Ok(existing) if !existing.is_empty() => return Ok((existing, false)),
        Ok(_) => {
            eprintln!(
                "warning: machine identifier file {} was empty; regenerating",
                path.display()
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(anyhow!(
                "failed to read machine identifier {}: {error}",
                path.display()
            ));
        }
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create machine identifier directory {}",
                parent.display()
            )
        })?;
    }

    let payload = vz::generate_generic_machine_identifier_data()
        .context("failed to generate Linux VM machine identifier")?;
    std::fs::write(path, &payload)
        .with_context(|| format!("failed to persist machine identifier {}", path.display()))?;
    Ok((payload, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_shell_prefers_bash_for_debian_style_images() {
        assert_eq!(
            default_shell_for_base_image(Some("ubuntu:24.04")),
            "/bin/bash"
        );
        assert_eq!(
            default_shell_for_base_image(Some("debian:bookworm")),
            "/bin/bash"
        );
    }

    #[test]
    fn default_shell_falls_back_to_sh_for_alpine_and_empty() {
        assert_eq!(default_shell_for_base_image(Some("alpine:3.20")), "/bin/sh");
        assert_eq!(default_shell_for_base_image(Some("   ")), "/bin/sh");
        assert_eq!(default_shell_for_base_image(None), "/bin/sh");
    }

    #[test]
    fn parse_main_container_treats_identifier_as_non_command() {
        let parsed = parse_main_container_startup_command("workspace-main");
        assert!(matches!(parsed, Ok(None)));
    }

    #[test]
    fn parse_main_container_parses_shell_command() {
        let parsed = parse_main_container_startup_command("bash -lc \"echo ready\"");
        assert_eq!(
            parsed.ok().flatten(),
            Some((
                "bash".to_string(),
                vec!["-lc".to_string(), "echo ready".to_string()],
            )),
        );
    }

    #[test]
    fn startup_command_prefers_main_container_override() {
        let spec = SandboxSpec {
            cpus: Some(2),
            memory_mb: Some(2048),
            base_image_ref: Some("alpine:3.20".to_string()),
            main_container: Some("bash -lc \"echo hi\"".to_string()),
            network_profile: None,
            volume_mounts: Vec::new(),
        };
        match startup_command_from_spec(&spec) {
            Ok((command, args)) => {
                assert_eq!(command, "bash");
                assert_eq!(args, vec!["-lc".to_string(), "echo hi".to_string()]);
            }
            Err(error) => panic!("startup command should resolve: {error}"),
        }
    }

    #[test]
    fn sandbox_snapshot_path_is_namespaced_under_runtime_dir() {
        let state_db = PathBuf::from("/tmp/vz/state/stack-state.db");
        let snapshot = sandbox_snapshot_path(&state_db, "vz-abcd");
        assert_eq!(
            snapshot,
            PathBuf::from("/tmp/vz/state/.vz-runtime/sandboxes/vz-abcd.state")
        );
    }

    #[test]
    fn sandbox_machine_identifier_path_replaces_snapshot_extension() {
        let snapshot = PathBuf::from("/tmp/vz/state/.vz-runtime/sandboxes/vz-abcd.state");
        let machine_identifier = sandbox_machine_identifier_path(&snapshot);
        assert_eq!(
            machine_identifier,
            PathBuf::from("/tmp/vz/state/.vz-runtime/sandboxes/vz-abcd.machine-id")
        );
    }

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
