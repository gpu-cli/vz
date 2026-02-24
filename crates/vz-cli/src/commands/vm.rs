//! `vz vm` -- macOS VM management commands.

use clap::{Args, Subcommand};

/// Manage virtual machines.
#[derive(Args, Debug)]
pub struct VmArgs {
    #[command(subcommand)]
    pub action: VmCommand,
}

/// VM management operations.
#[derive(Subcommand, Debug)]
pub enum VmCommand {
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
}

/// Entry point for `vz vm`.
pub async fn run(args: VmArgs) -> anyhow::Result<()> {
    match args.action {
        VmCommand::Init(a) => super::init::run(a).await,
        VmCommand::Run(a) => super::run::run(a).await,
        VmCommand::Exec(a) => super::exec::run(a).await,
        VmCommand::Save(a) => super::save::run(a).await,
        VmCommand::Restore(a) => super::restore::run(a).await,
        VmCommand::List(a) => super::list::run(a).await,
        VmCommand::Stop(a) => super::stop::run(a).await,
        VmCommand::Rm(a) => super::rm::run(a).await,
        VmCommand::Cache(a) => super::cache::run(a).await,
        VmCommand::Provision(a) => super::provision::run(a).await,
        VmCommand::Cleanup(a) => super::cleanup::run(a).await,
        VmCommand::SelfSign(a) => super::self_sign::run(a).await,
        VmCommand::Validate(a) => super::validate::run(a).await,
    }
}
