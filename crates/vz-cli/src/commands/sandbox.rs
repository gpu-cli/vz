//! `vz sandbox` — sandbox lifecycle management commands.
//!
//! Provides sandbox CRUD and the default `vz` instant-sandbox experience.
//! Backed by `vz-stack` StateStore for persistence.

#![allow(clippy::print_stdout)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use clap::Args;
use vz_runtime_contract::{Sandbox, SandboxBackend, SandboxSpec, SandboxState};
use vz_stack::StateStore;

/// Default state database path.
const DEFAULT_STATE_DB: &str = "stack-state.db";

/// Default state database path in home directory.
fn default_state_db() -> PathBuf {
    if let Some(home) = dirs_path() {
        home.join("stack-state.db")
    } else {
        PathBuf::from(DEFAULT_STATE_DB)
    }
}

fn dirs_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".vz"))
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
) -> anyhow::Result<()> {
    let state_db = default_state_db();
    let cwd = std::env::current_dir().context("failed to get current directory")?;

    if continue_last {
        return cmd_continue_sandbox(&state_db, &cwd).await;
    }

    if let Some(ref target) = resume {
        return cmd_resume_sandbox(&state_db, target).await;
    }

    // Create a new sandbox.
    cmd_create_sandbox(&state_db, &cwd, name, cpus, memory).await
}

/// Continue the most recent sandbox for the current directory.
async fn cmd_continue_sandbox(
    state_db: &std::path::Path,
    cwd: &std::path::Path,
) -> anyhow::Result<()> {
    let store = StateStore::open(state_db).context("failed to open state store")?;
    let sandboxes = store.list_sandboxes().context("failed to list sandboxes")?;
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
async fn cmd_resume_sandbox(state_db: &std::path::Path, target: &str) -> anyhow::Result<()> {
    let store = StateStore::open(state_db).context("failed to open state store")?;

    // Try exact ID match first.
    if let Some(sandbox) = store
        .load_sandbox(target)
        .context("failed to load sandbox")?
    {
        if sandbox.state.is_terminal() {
            bail!("sandbox {target} is in terminal state");
        }
        println!("Resuming sandbox {target}...");
        return attach_to_sandbox_by_id(state_db, target).await;
    }

    // Try name label match.
    let sandboxes = store.list_sandboxes().context("failed to list sandboxes")?;
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
    state_db: &std::path::Path,
    cwd: &std::path::Path,
    name: Option<String>,
    cpus: u8,
    memory: u64,
) -> anyhow::Result<()> {
    let sandbox_id = generate_sandbox_id();
    let display_name = name.as_deref().unwrap_or(&sandbox_id);

    let mut labels = BTreeMap::new();
    labels.insert("project_dir".to_string(), cwd.to_string_lossy().to_string());
    labels.insert("source".to_string(), "standalone".to_string());
    if let Some(ref n) = name {
        labels.insert("name".to_string(), n.clone());
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let sandbox = Sandbox {
        sandbox_id: sandbox_id.clone(),
        backend: SandboxBackend::LinuxFirecracker,
        spec: SandboxSpec {
            cpus: Some(cpus),
            memory_mb: Some(memory),
            ..SandboxSpec::default()
        },
        state: SandboxState::Creating,
        created_at: now,
        updated_at: now,
        labels,
    };

    // Ensure state directory exists.
    if let Some(parent) = state_db.parent() {
        std::fs::create_dir_all(parent).ok();
    }

    let store = StateStore::open(state_db).context("failed to open state store")?;
    store
        .save_sandbox(&sandbox)
        .context("failed to save sandbox")?;

    println!("Booting sandbox {display_name}...");
    println!("Mounting {} → /workspace", cwd.display());

    // Boot the VM and attach.
    #[cfg(target_os = "macos")]
    {
        match boot_and_attach(&sandbox_id, &sandbox.spec, cwd, &store).await {
            Ok(()) => {}
            Err(e) => {
                // Mark sandbox as failed on error.
                if let Ok(Some(mut s)) = store.load_sandbox(&sandbox_id) {
                    let _ = s.transition_to(SandboxState::Failed);
                    let _ = store.save_sandbox(&s);
                }
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
    sandbox_id: &str,
    spec: &SandboxSpec,
    project_dir: &std::path::Path,
    store: &StateStore,
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

    let boot_time = vm
        .start_and_wait_for_agent_with_progress(
            std::time::Duration::from_secs(60),
            |attempts, last_err| {
                if attempts % 5 == 0 {
                    eprintln!("  waiting for guest agent (attempt {attempts}: {last_err})...");
                }
            },
        )
        .await
        .context("failed to boot VM")?;

    eprintln!("Sandbox ready ({:.1}s)", boot_time.as_secs_f64());

    // Mount devpts for PTY support (required before interactive shell).
    let timeout = std::time::Duration::from_secs(10);
    let devpts_result = vm
        .exec_capture(
            "/bin/busybox".to_string(),
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "/bin/busybox mkdir -p /dev/pts && /bin/busybox mount -t devpts devpts /dev/pts"
                    .to_string(),
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
                "/bin/busybox mkdir -p /workspace && /bin/busybox mount -t virtiofs workspace /workspace".to_string(),
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

    // Mark sandbox as ready.
    if let Ok(Some(mut s)) = store.load_sandbox(sandbox_id) {
        let _ = s.transition_to(SandboxState::Ready);
        s.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let _ = store.save_sandbox(&s);
    }

    let vm = Arc::new(vm);
    attach_interactive(vm, "/bin/sh", "/workspace").await
}

/// Attach interactively to a VM with a PTY session.
#[cfg(target_os = "macos")]
async fn attach_interactive(
    vm: std::sync::Arc<vz_linux::LinuxVm>,
    shell: &str,
    working_dir: &str,
) -> anyhow::Result<()> {
    use crossterm::event::{self, Event};
    use crossterm::terminal;
    use std::io::Write;

    let (cols, rows) = terminal::size().unwrap_or((80, 24));

    let wd = if working_dir.is_empty() {
        None
    } else {
        Some(working_dir)
    };
    let (mut stream, exec_id) = vm
        .exec_interactive(shell, &[], wd, rows as u32, cols as u32)
        .await
        .context("failed to start interactive session")?;

    terminal::enable_raw_mode().context("failed to enable raw mode")?;

    let vm_input = vm.clone();
    let input_exec_id = exec_id;

    // Input task: read terminal events and forward to guest.
    let input_handle = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        loop {
            match event::read() {
                Ok(Event::Key(key_event)) => {
                    let bytes = key_event_to_bytes(&key_event);
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
    while let Some(ev) = stream.next().await {
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

    terminal::disable_raw_mode().ok();
    input_handle.abort();
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

// ── Top-level sandbox commands ──────────────────────────────────

/// List all sandboxes (`vz ls`).
pub fn cmd_list(args: SandboxListArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db);
    let store = StateStore::open(&state_db).context("failed to open state store")?;
    let sandboxes = store.list_sandboxes().context("failed to list sandboxes")?;

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
pub fn cmd_inspect(args: SandboxInspectArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db);
    let store = StateStore::open(&state_db).context("failed to open state store")?;
    let sandbox = store
        .load_sandbox(&args.sandbox_id)
        .context("failed to load sandbox")?;

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
pub fn cmd_terminate(args: SandboxTerminateArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db);
    let store = StateStore::open(&state_db).context("failed to open state store")?;
    let mut sandbox = store
        .load_sandbox(&args.sandbox_id)
        .context("failed to load sandbox")?
        .ok_or_else(|| anyhow::anyhow!("sandbox {} not found", args.sandbox_id))?;

    if sandbox.state.is_terminal() {
        println!("Sandbox {} is already in terminal state.", args.sandbox_id);
        return Ok(());
    }

    match sandbox.state {
        SandboxState::Creating => {
            sandbox
                .transition_to(SandboxState::Failed)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        SandboxState::Ready => {
            sandbox
                .transition_to(SandboxState::Draining)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            sandbox
                .transition_to(SandboxState::Terminated)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        SandboxState::Draining => {
            sandbox
                .transition_to(SandboxState::Terminated)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        _ => {}
    }

    store
        .save_sandbox(&sandbox)
        .context("failed to save sandbox")?;

    let state = serde_json::to_string(&sandbox.state)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string();
    println!("Sandbox {} terminated (state: {state}).", args.sandbox_id);

    Ok(())
}

/// Attach to an existing sandbox (`vz attach`).
pub async fn cmd_attach(_args: SandboxAttachArgs) -> anyhow::Result<()> {
    // Attachment requires looking up the running VM — for now, we support
    // attach by re-opening a new interactive session on the sandbox's VM.
    // Full VM handle reattach requires runtime persistence (tracked separately).
    #[cfg(target_os = "macos")]
    {
        let state_db = _args.state_db.unwrap_or_else(default_state_db);
        return attach_to_sandbox_by_id(&state_db, &_args.sandbox_id).await;
    }

    #[cfg(not(target_os = "macos"))]
    bail!("sandbox attach requires macOS with Apple Silicon");
}

/// Attach to a sandbox by its ID (shared helper).
async fn attach_to_sandbox_by_id(
    state_db: &std::path::Path,
    sandbox_id: &str,
) -> anyhow::Result<()> {
    let store = StateStore::open(state_db).context("failed to open state store")?;
    let sandbox = store
        .load_sandbox(sandbox_id)
        .context("failed to load sandbox")?
        .ok_or_else(|| anyhow::anyhow!("sandbox {sandbox_id} not found"))?;

    if sandbox.state.is_terminal() {
        bail!("sandbox {sandbox_id} is in terminal state");
    }

    let project_dir = sandbox.labels.get("project_dir").cloned();

    #[cfg(target_os = "macos")]
    {
        // Re-boot the VM for this sandbox (VM handles are ephemeral).
        // In the future, we'll support persistent VM handles via runtime state.
        let spec = &sandbox.spec;
        let cwd = project_dir
            .as_deref()
            .map(std::path::Path::new)
            .unwrap_or_else(|| std::path::Path::new("/"));
        boot_and_attach(sandbox_id, spec, cwd, &store).await
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
