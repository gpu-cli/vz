//! `vz vm` -- VM command namespaces.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use anyhow::{Context, anyhow, bail};
use clap::{Args, Subcommand};
use serde::Serialize;
use tonic::Code;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::{DaemonClient, DaemonClientError};

use super::runtime_daemon::{
    ControlPlaneTransport, connect_control_plane_for_state_db, control_plane_transport,
    default_state_db_path,
};
use super::sandbox;

/// Manage VM command namespaces.
#[derive(Args, Debug)]
pub struct VmArgs {
    #[command(subcommand)]
    pub action: VmCommand,
}

/// VM namespace operations.
#[derive(Subcommand, Debug)]
pub enum VmCommand {
    /// macOS VM management commands (Virtualization.framework).
    #[cfg(target_os = "macos")]
    Mac(MacVmArgs),

    /// Linux VM orchestration/testing workflows.
    Linux(LinuxVmArgs),
}

/// Entry point for `vz vm`.
pub async fn run(args: VmArgs) -> anyhow::Result<()> {
    match args.action {
        #[cfg(target_os = "macos")]
        VmCommand::Mac(mac_args) => run_mac(mac_args).await,
        VmCommand::Linux(linux_args) => run_linux(linux_args).await,
    }
}

/// `vz vm mac` arguments.
#[cfg(target_os = "macos")]
#[derive(Args, Debug)]
pub struct MacVmArgs {
    #[command(subcommand)]
    pub action: MacVmCommand,
}

/// macOS VM management operations.
#[cfg(target_os = "macos")]
#[derive(Subcommand, Debug)]
pub enum MacVmCommand {
    /// Create a golden macOS VM image from an IPSW.
    Init(super::init::InitArgs),
    /// Start a VM with optional mounts.
    Run(super::run::RunArgs),
    /// Execute a command inside a running VM.
    Exec(super::exec::ExecArgs),
    /// Save VM state for fast restore.
    Save(super::save::SaveArgs),
    /// Restore VM from saved state.
    Restore(super::restore::RestoreArgs),
    /// List running VMs.
    List(super::list::ListArgs),
    /// Stop a running VM.
    Stop(super::stop::StopArgs),
    /// Remove VM runtime metadata (and optionally image artifacts).
    Rm(super::rm::RmArgs),
    /// Manage cached files (IPSWs, downloads).
    Cache(super::cache::CacheArgs),
    /// Provision a disk image (user account, guest agent, auto-login).
    Provision(super::provision::ProvisionArgs),
    /// Detect and clean up orphaned VMs.
    Cleanup(super::cleanup::CleanupArgs),
    /// Ad-hoc sign the vz binary with required entitlements.
    SelfSign(super::self_sign::SelfSignArgs),
    /// Run validation suites against image cohorts.
    Validate(super::validate::ValidateArgs),
    /// Manage supported base image definitions.
    Base(super::vm_base::VmBaseArgs),
    /// Create/apply patch bundles and image deltas.
    Patch(super::vm_patch::VmPatchArgs),
}

/// Entry point for `vz vm mac` and legacy `vz debug vm`.
#[cfg(target_os = "macos")]
pub async fn run_mac(args: MacVmArgs) -> anyhow::Result<()> {
    match args.action {
        MacVmCommand::Init(a) => super::init::run(a).await,
        MacVmCommand::Run(a) => super::run::run(a).await,
        MacVmCommand::Exec(a) => super::exec::run(a).await,
        MacVmCommand::Save(a) => super::save::run(a).await,
        MacVmCommand::Restore(a) => super::restore::run(a).await,
        MacVmCommand::List(a) => super::list::run(a).await,
        MacVmCommand::Stop(a) => super::stop::run(a).await,
        MacVmCommand::Rm(a) => super::rm::run(a).await,
        MacVmCommand::Cache(a) => super::cache::run(a).await,
        MacVmCommand::Provision(a) => super::provision::run(a).await,
        MacVmCommand::Cleanup(a) => super::cleanup::run(a).await,
        MacVmCommand::SelfSign(a) => super::self_sign::run(a).await,
        MacVmCommand::Validate(a) => super::validate::run(a).await,
        MacVmCommand::Base(a) => super::vm_base::run(a).await,
        MacVmCommand::Patch(a) => super::vm_patch::run(a).await,
    }
}

/// `vz vm linux` arguments.
#[derive(Args, Debug)]
pub struct LinuxVmArgs {
    #[command(subcommand)]
    pub action: LinuxVmCommand,
}

/// Linux VM workflow operations.
#[derive(Subcommand, Debug)]
pub enum LinuxVmCommand {
    /// Initialize Linux guest image metadata and persistent disk artifacts.
    Init(LinuxVmInitArgs),
    /// Create/start a Linux space without attaching.
    Run(LinuxVmRunArgs),
    /// List Linux spaces.
    List(LinuxVmListArgs),
    /// Show Linux space details.
    Inspect(LinuxVmInspectArgs),
    /// Attach interactive shell to a Linux space.
    Attach(LinuxVmAttachArgs),
    /// Execute a command in a Linux space with streamed output.
    Exec(LinuxVmExecArgs),
    /// Stop a Linux space.
    Stop(LinuxVmStopArgs),
    /// Remove (terminate) a Linux space.
    Rm(LinuxVmRmArgs),
    /// Linux VM test workflows.
    Test(LinuxVmTestArgs),
    /// Run local no-SSH Linux VM E2E harness orchestration (legacy alias).
    #[command(hide = true)]
    E2e(LinuxVmE2eArgs),
}

/// Arguments for `vz vm linux init`.
#[derive(Args, Debug)]
pub struct LinuxVmInitArgs {
    /// Logical Linux guest image name.
    #[arg(long)]
    pub name: String,
    /// Output directory for Linux guest image metadata and disk.
    #[arg(long, default_value = "~/.vz/images")]
    pub output_dir: String,
    /// Linux guest persistent disk size in GiB.
    #[arg(long, default_value_t = 64)]
    pub disk_size_gb: u64,
    /// Override kernel artifact path (defaults to ~/.vz/linux/vmlinux).
    #[arg(long)]
    pub kernel: Option<PathBuf>,
    /// Override initramfs artifact path (defaults to ~/.vz/linux/initramfs.img).
    #[arg(long)]
    pub initramfs: Option<PathBuf>,
    /// Replace existing image artifacts when target already exists.
    #[arg(long)]
    pub force: bool,
}

/// Arguments for `vz vm linux run`.
#[derive(Args, Debug)]
pub struct LinuxVmRunArgs {
    /// Optional display name.
    #[arg(long)]
    pub name: Option<String>,
    /// Number of virtual CPUs.
    #[arg(long, default_value = "2")]
    pub cpus: u8,
    /// Memory in MB.
    #[arg(long, default_value = "2048")]
    pub memory: u64,
    /// Default image reference for startup workload.
    #[arg(long)]
    pub base_image: Option<String>,
    /// Main workload/container identifier for startup.
    #[arg(long)]
    pub main_container: Option<String>,
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
    /// Output created space payload as JSON.
    #[arg(long)]
    pub json: bool,
}

/// Arguments for `vz vm linux list`.
#[derive(Args, Debug)]
pub struct LinuxVmListArgs {
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
    /// Output payload as JSON.
    #[arg(long)]
    pub json: bool,
    /// Include non-linux backends.
    #[arg(long)]
    pub all: bool,
}

/// Arguments for `vz vm linux inspect`.
#[derive(Args, Debug)]
pub struct LinuxVmInspectArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
}

/// Arguments for `vz vm linux attach`.
#[derive(Args, Debug)]
pub struct LinuxVmAttachArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
}

/// Arguments for `vz vm linux exec`.
#[derive(Args, Debug)]
pub struct LinuxVmExecArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,
    /// Command and arguments to run.
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
    /// Timeout in seconds (0 disables timeout).
    #[arg(long, default_value_t = 0)]
    pub timeout_secs: u64,
    /// Force PTY allocation for the command.
    #[arg(long)]
    pub pty: bool,
}

/// Arguments for `vz vm linux stop`.
#[derive(Args, Debug)]
pub struct LinuxVmStopArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
}

/// Arguments for `vz vm linux rm`.
#[derive(Args, Debug)]
pub struct LinuxVmRmArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
}

/// `vz vm linux test` arguments.
#[derive(Args, Debug)]
pub struct LinuxVmTestArgs {
    #[command(subcommand)]
    pub action: LinuxVmTestCommand,
}

/// Linux VM test workflow operations.
#[derive(Subcommand, Debug)]
pub enum LinuxVmTestCommand {
    /// Run local no-SSH Linux VM E2E harness orchestration.
    E2e(LinuxVmE2eArgs),
}

/// Arguments for `vz vm linux test e2e`.
#[derive(Args, Debug)]
pub struct LinuxVmE2eArgs {
    /// VM name used by `vz vm mac`.
    #[arg(long)]
    pub vm_name: String,
    /// Repo path inside guest.
    #[arg(long)]
    pub guest_repo: String,
    /// In-guest btrfs workspace path.
    #[arg(long, default_value = "/mnt/vz-btrfs")]
    pub workspace: String,
    /// Harness profile.
    #[arg(long, default_value = "debug")]
    pub profile: String,
    /// Auto-start VM if not already running.
    #[arg(long)]
    pub auto_start: bool,
    /// VM image path for auto-start.
    #[arg(long)]
    pub vm_image: Option<PathBuf>,
    /// CPUs for auto-started VM.
    #[arg(long, default_value_t = 4)]
    pub vm_cpus: u32,
    /// Memory GB for auto-started VM.
    #[arg(long, default_value_t = 8)]
    pub vm_memory_gb: u64,
    /// Max wait seconds for VM availability after auto-start.
    #[arg(long, default_value_t = 90)]
    pub wait_secs: u64,
    /// VirtioFS mount (TAG:HOST_PATH). Repeatable.
    #[arg(long = "mount")]
    pub mounts: Vec<String>,
    /// Skip guest btrfs provisioning.
    #[arg(long)]
    pub no_provision_btrfs: bool,
    /// In-guest loopback btrfs image path.
    #[arg(long, default_value = "/var/lib/vz-btrfs-workspace.img")]
    pub btrfs_image: String,
    /// In-guest loopback btrfs image size in GiB.
    #[arg(long, default_value_t = 64)]
    pub btrfs_size_gb: u64,
}

async fn run_linux(args: LinuxVmArgs) -> anyhow::Result<()> {
    match args.action {
        LinuxVmCommand::Init(init_args) => run_linux_init(init_args).await,
        LinuxVmCommand::Run(run_args) => run_linux_run(run_args).await,
        LinuxVmCommand::List(list_args) => run_linux_list(list_args).await,
        LinuxVmCommand::Inspect(inspect_args) => run_linux_inspect(inspect_args).await,
        LinuxVmCommand::Attach(attach_args) => run_linux_attach(attach_args).await,
        LinuxVmCommand::Exec(exec_args) => run_linux_exec(exec_args).await,
        LinuxVmCommand::Stop(stop_args) => run_linux_stop(stop_args).await,
        LinuxVmCommand::Rm(rm_args) => run_linux_rm(rm_args).await,
        LinuxVmCommand::Test(test_args) => run_linux_test(test_args).await,
        LinuxVmCommand::E2e(e2e_args) => {
            eprintln!(
                "warning: `vz vm linux e2e` is deprecated; use `vz vm linux test e2e` instead"
            );
            run_linux_e2e(e2e_args)
        }
    }
}

async fn run_linux_init(args: LinuxVmInitArgs) -> anyhow::Result<()> {
    let _ = args;
    bail!(
        "vz vm linux init contract is reserved; implementation is tracked in vz-t8zg.2/vz-t8zg.3/vz-t8zg.4"
    );
}

async fn run_linux_run(args: LinuxVmRunArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let _ = connect_linux_daemon(&state_db).await?;
    sandbox::cmd_create(sandbox::SandboxCreateArgs {
        name: args.name,
        cpus: args.cpus,
        memory: args.memory,
        base_image: args.base_image,
        main_container: args.main_container,
        state_db: Some(state_db),
        json: args.json,
    })
    .await
}

#[derive(Debug, Serialize)]
struct LinuxVmListItem {
    sandbox_id: String,
    backend: String,
    state: String,
    cpus: u32,
    memory_mb: u64,
    created_at: u64,
    updated_at: u64,
}

async fn run_linux_list(args: LinuxVmListArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let mut client = connect_linux_daemon(&state_db).await?;
    let mut sandboxes = client
        .list_sandboxes(runtime_v2::ListSandboxesRequest { metadata: None })
        .await
        .context("failed to list sandboxes via daemon")?
        .sandboxes;

    if !args.all {
        sandboxes.retain(|sandbox| is_linux_backend_name(&sandbox.backend));
    }

    let items: Vec<LinuxVmListItem> = sandboxes
        .into_iter()
        .map(|sandbox| LinuxVmListItem {
            sandbox_id: sandbox.sandbox_id,
            backend: sandbox.backend,
            state: sandbox.state,
            cpus: sandbox.cpus,
            memory_mb: sandbox.memory_mb,
            created_at: sandbox.created_at,
            updated_at: sandbox.updated_at,
        })
        .collect();

    if args.json {
        let json = serde_json::to_string_pretty(&items).context("failed to serialize payload")?;
        println!("{json}");
        return Ok(());
    }

    if items.is_empty() {
        if args.all {
            println!("No spaces found.");
        } else {
            println!("No linux spaces found.");
        }
        return Ok(());
    }

    println!(
        "{:<16} {:<20} {:<12} {:<6} {:<10}",
        "SANDBOX ID", "BACKEND", "STATE", "CPUS", "MEMORY_MB"
    );
    for item in &items {
        println!(
            "{:<16} {:<20} {:<12} {:<6} {:<10}",
            item.sandbox_id, item.backend, item.state, item.cpus, item.memory_mb
        );
    }
    Ok(())
}

async fn run_linux_inspect(args: LinuxVmInspectArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let mut client = connect_linux_daemon(&state_db).await?;
    let response = match client
        .get_sandbox(runtime_v2::GetSandboxRequest {
            sandbox_id: args.sandbox_id.clone(),
            metadata: None,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("linux space {} not found", args.sandbox_id)
        }
        Err(error) => return Err(anyhow!(error).context("failed to inspect linux space")),
    };
    let payload = response
        .sandbox
        .ok_or_else(|| anyhow!("daemon inspect response missing sandbox payload"))?;
    if !is_linux_backend_name(&payload.backend) {
        bail!(
            "sandbox {} is backend {}, not linux",
            args.sandbox_id,
            payload.backend
        );
    }
    let inspect = LinuxVmListItem {
        sandbox_id: payload.sandbox_id,
        backend: payload.backend,
        state: payload.state,
        cpus: payload.cpus,
        memory_mb: payload.memory_mb,
        created_at: payload.created_at,
        updated_at: payload.updated_at,
    };
    let json =
        serde_json::to_string_pretty(&inspect).context("failed to serialize sandbox payload")?;
    println!("{json}");
    Ok(())
}

async fn run_linux_attach(args: LinuxVmAttachArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    ensure_linux_sandbox(&state_db, &args.sandbox_id).await?;
    sandbox::cmd_attach(sandbox::SandboxAttachArgs {
        sandbox_id: args.sandbox_id,
        state_db: Some(state_db),
    })
    .await
}

async fn run_linux_exec(args: LinuxVmExecArgs) -> anyhow::Result<()> {
    if args.command.is_empty() {
        bail!("exec requires a command");
    }
    let (cmd, tail) = args
        .command
        .split_first()
        .ok_or_else(|| anyhow!("exec requires a command"))?;
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let mut client = connect_linux_daemon(&state_db).await?;
    let (container_id, shell_execution_id) =
        resolve_linux_container_for_sandbox(&mut client, &args.sandbox_id).await?;
    if let Some(execution_id) = shell_execution_id.as_deref() {
        close_shell_session(&mut client, &args.sandbox_id, execution_id).await?;
    }

    let execution = client
        .create_execution(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id,
            cmd: vec![cmd.to_string()],
            args: tail.iter().map(|arg| arg.to_string()).collect(),
            env_override: HashMap::new(),
            timeout_secs: args.timeout_secs,
            pty_mode: if args.pty {
                runtime_v2::create_execution_request::PtyMode::Enabled as i32
            } else {
                runtime_v2::create_execution_request::PtyMode::Disabled as i32
            },
        })
        .await
        .context("failed to create linux execution")?;
    let execution_payload = execution
        .execution
        .ok_or_else(|| anyhow!("daemon create_execution missing execution payload"))?;
    let execution_id = execution_payload.execution_id;

    let mut stream = client
        .stream_exec_output(runtime_v2::StreamExecOutputRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        })
        .await
        .context("failed to stream linux execution output")?;

    let mut exit_code = None;
    while let Some(event) = stream
        .message()
        .await
        .context("failed to read linux execution stream")?
    {
        match event.payload {
            Some(runtime_v2::exec_output_event::Payload::Stdout(bytes)) => {
                std::io::stdout()
                    .write_all(&bytes)
                    .context("failed writing stdout payload")?;
                std::io::stdout()
                    .flush()
                    .context("failed flushing stdout payload")?;
            }
            Some(runtime_v2::exec_output_event::Payload::Stderr(bytes)) => {
                std::io::stderr()
                    .write_all(&bytes)
                    .context("failed writing stderr payload")?;
                std::io::stderr()
                    .flush()
                    .context("failed flushing stderr payload")?;
            }
            Some(runtime_v2::exec_output_event::Payload::ExitCode(code)) => {
                exit_code = Some(code);
            }
            Some(runtime_v2::exec_output_event::Payload::Error(error)) => {
                bail!("execution stream error: {error}");
            }
            None => {}
        }
    }

    let code = match exit_code {
        Some(code) => code,
        None => {
            let response = client
                .get_execution(runtime_v2::GetExecutionRequest {
                    execution_id: execution_id.clone(),
                    metadata: None,
                })
                .await
                .context("failed to inspect execution after stream completion")?;
            let payload = response
                .execution
                .ok_or_else(|| anyhow!("daemon get_execution missing payload"))?;
            payload.exit_code
        }
    };

    if code != 0 {
        std::process::exit(code);
    }
    Ok(())
}

async fn run_linux_stop(args: LinuxVmStopArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    ensure_linux_sandbox(&state_db, &args.sandbox_id).await?;
    sandbox::cmd_terminate(sandbox::SandboxTerminateArgs {
        sandbox_id: args.sandbox_id,
        state_db: Some(state_db),
    })
    .await
}

async fn run_linux_rm(args: LinuxVmRmArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    ensure_linux_sandbox(&state_db, &args.sandbox_id).await?;
    sandbox::cmd_terminate(sandbox::SandboxTerminateArgs {
        sandbox_id: args.sandbox_id,
        state_db: Some(state_db),
    })
    .await
}

async fn run_linux_test(args: LinuxVmTestArgs) -> anyhow::Result<()> {
    match args.action {
        LinuxVmTestCommand::E2e(e2e_args) => run_linux_e2e(e2e_args),
    }
}

async fn connect_linux_daemon(state_db: &Path) -> anyhow::Result<DaemonClient> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {}
        ControlPlaneTransport::ApiHttp => {
            bail!("vz vm linux commands require daemon-grpc transport")
        }
    }
    let client = connect_control_plane_for_state_db(state_db).await?;
    let backend = client.handshake().backend_name.trim().to_ascii_lowercase();
    if !is_linux_backend_name(&backend) {
        bail!(
            "connected daemon backend `{backend}` is not linux; point VZ_RUNTIME_DAEMON_SOCKET at a linux daemon"
        );
    }
    Ok(client)
}

fn is_linux_backend_name(name: &str) -> bool {
    matches!(
        name.trim().to_ascii_lowercase().as_str(),
        "linux_firecracker" | "linux-firecracker" | "firecracker"
    )
}

async fn ensure_linux_sandbox(state_db: &Path, sandbox_id: &str) -> anyhow::Result<()> {
    let mut client = connect_linux_daemon(state_db).await?;
    let response = match client
        .get_sandbox(runtime_v2::GetSandboxRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("linux space {sandbox_id} not found")
        }
        Err(error) => return Err(anyhow!(error).context("failed to resolve linux space")),
    };
    let payload = response
        .sandbox
        .ok_or_else(|| anyhow!("daemon get_sandbox missing payload"))?;
    if !is_linux_backend_name(payload.backend.as_str()) {
        bail!(
            "sandbox {sandbox_id} is backend `{}`, not linux",
            payload.backend
        );
    }
    Ok(())
}

async fn resolve_linux_container_for_sandbox(
    client: &mut DaemonClient,
    sandbox_id: &str,
) -> anyhow::Result<(String, Option<String>)> {
    let mut stream = client
        .open_sandbox_shell(runtime_v2::OpenSandboxShellRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
        .context("failed to open sandbox shell for container resolution")?;
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .context("failed reading open_sandbox_shell stream")?
    {
        match event.payload {
            Some(runtime_v2::open_sandbox_shell_event::Payload::Progress(progress)) => {
                eprintln!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::open_sandbox_shell_event::Payload::Completion(done)) => {
                completion = Some(done);
                break;
            }
            None => {}
        }
    }
    let completion = completion
        .ok_or_else(|| anyhow!("open_sandbox_shell stream ended without completion event"))?;
    if completion.container_id.trim().is_empty() {
        bail!("open_sandbox_shell completion missing container_id");
    }
    let shell_execution_id = if completion.execution_id.trim().is_empty() {
        None
    } else {
        Some(completion.execution_id)
    };
    Ok((completion.container_id, shell_execution_id))
}

async fn close_shell_session(
    client: &mut DaemonClient,
    sandbox_id: &str,
    execution_id: &str,
) -> anyhow::Result<()> {
    let mut stream = client
        .close_sandbox_shell(runtime_v2::CloseSandboxShellRequest {
            sandbox_id: sandbox_id.to_string(),
            execution_id: execution_id.to_string(),
            metadata: None,
        })
        .await
        .context("failed to close bootstrap shell session")?;
    while let Some(event) = stream
        .message()
        .await
        .context("failed reading close_sandbox_shell stream")?
    {
        match event.payload {
            Some(runtime_v2::close_sandbox_shell_event::Payload::Progress(progress)) => {
                eprintln!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::close_sandbox_shell_event::Payload::Completion(_)) => break,
            None => {}
        }
    }
    Ok(())
}

fn run_linux_e2e(args: LinuxVmE2eArgs) -> anyhow::Result<()> {
    let script = PathBuf::from("scripts/run-vz-linux-vm-e2e-local.sh");
    if !script.is_file() {
        bail!(
            "missing harness wrapper {}; run from repository root",
            script.display()
        );
    }

    let mut cmd = Command::new(&script);
    cmd.arg("--vm-name")
        .arg(&args.vm_name)
        .arg("--guest-repo")
        .arg(&args.guest_repo)
        .arg("--workspace")
        .arg(&args.workspace)
        .arg("--profile")
        .arg(&args.profile)
        .arg("--vm-cpus")
        .arg(args.vm_cpus.to_string())
        .arg("--vm-memory-gb")
        .arg(args.vm_memory_gb.to_string())
        .arg("--wait-secs")
        .arg(args.wait_secs.to_string())
        .arg("--btrfs-image")
        .arg(&args.btrfs_image)
        .arg("--btrfs-size-gb")
        .arg(args.btrfs_size_gb.to_string());

    if args.auto_start {
        cmd.arg("--auto-start");
    }
    if let Some(vm_image) = args.vm_image {
        cmd.arg("--vm-image").arg(vm_image);
    }
    if args.no_provision_btrfs {
        cmd.arg("--no-provision-btrfs");
    }
    for mount in args.mounts {
        cmd.arg("--mount").arg(mount);
    }

    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let status = cmd
        .status()
        .context("failed to launch local Linux VM E2E wrapper")?;
    if !status.success() {
        match status.code() {
            Some(code) => bail!("local Linux VM E2E wrapper failed with exit code {code}"),
            None => bail!("local Linux VM E2E wrapper terminated by signal"),
        }
    }
    Ok(())
}
