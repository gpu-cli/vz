//! `vz vm` -- VM command namespaces.

use std::path::PathBuf;
use std::process::{Command, Stdio};

use anyhow::{Context, bail};
use clap::{Args, Subcommand};

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
    /// Run local no-SSH Linux VM E2E harness orchestration.
    E2e(LinuxVmE2eArgs),
}

/// Arguments for `vz vm linux e2e`.
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
        LinuxVmCommand::E2e(e2e_args) => run_linux_e2e(e2e_args),
    }
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
