//! vz -- macOS VM sandbox CLI.
//!
//! Manages macOS virtual machines for sandboxed coding agent execution.
//! Built on Apple's Virtualization.framework via the `vz` crate.

#![cfg(target_os = "macos")]
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod commands;
mod control;
mod ipsw;
mod provision;
mod registry;

use clap::Parser;
use tracing::error;

/// vz -- macOS VM sandbox for coding agents.
///
/// Create, run, and manage macOS virtual machines for isolated
/// command execution. Uses Apple's Virtualization.framework.
#[derive(Parser, Debug)]
#[command(name = "vz", version, about, long_about = None)]
struct Cli {
    /// Increase log verbosity (-v for debug, -vv for trace).
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    /// Suppress non-error output.
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Output as JSON (for scripting).
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    /// Create a golden macOS VM image from an IPSW.
    Init(commands::init::InitArgs),

    /// Start a VM with optional mounts.
    Run(commands::run::RunArgs),

    /// Execute a command inside a running VM.
    Exec(commands::exec::ExecArgs),

    /// Save VM state for fast restore.
    Save(commands::save::SaveArgs),

    /// Restore VM from saved state.
    Restore(commands::restore::RestoreArgs),

    /// List running VMs.
    List(commands::list::ListArgs),

    /// Stop a running VM.
    Stop(commands::stop::StopArgs),

    /// Manage cached files (IPSWs, downloads).
    Cache(commands::cache::CacheArgs),

    /// Provision a disk image (user account, guest agent, auto-login).
    Provision(commands::provision::ProvisionArgs),

    /// Detect and clean up orphaned VMs.
    Cleanup(commands::cleanup::CleanupArgs),

    /// Ad-hoc sign the vz binary with required entitlements.
    SelfSign(commands::self_sign::SelfSignArgs),
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Set up tracing based on verbosity
    let filter = if cli.quiet {
        "error"
    } else {
        match cli.verbose {
            0 => "info",
            1 => "debug",
            _ => "trace",
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter)),
        )
        .with_target(false)
        .init();

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let result = match cli.command {
            Commands::Init(args) => commands::init::run(args).await,
            Commands::Run(args) => commands::run::run(args).await,
            Commands::Exec(args) => commands::exec::run(args).await,
            Commands::Save(args) => commands::save::run(args).await,
            Commands::Restore(args) => commands::restore::run(args).await,
            Commands::List(args) => commands::list::run(args).await,
            Commands::Stop(args) => commands::stop::run(args).await,
            Commands::Cache(args) => commands::cache::run(args).await,
            Commands::Provision(args) => commands::provision::run(args).await,
            Commands::Cleanup(args) => commands::cleanup::run(args).await,
            Commands::SelfSign(args) => commands::self_sign::run(args).await,
        };

        if let Err(ref e) = result {
            error!("{e:#}");
        }
        result
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_debug_assert() {
        // Verify the CLI definition is valid (catches clap config errors)
        Cli::command().debug_assert();
    }

    #[test]
    fn parse_verbose_flag() {
        let cli = Cli::try_parse_from(["vz", "-v", "list"]).expect("parse");
        assert_eq!(cli.verbose, 1);
    }

    #[test]
    fn parse_quiet_flag() {
        let cli = Cli::try_parse_from(["vz", "--quiet", "list"]).expect("parse");
        assert!(cli.quiet);
    }

    #[test]
    fn parse_json_flag() {
        let cli = Cli::try_parse_from(["vz", "--json", "list"]).expect("parse");
        assert!(cli.json);
    }

    #[test]
    fn parse_init_subcommand() {
        let cli = Cli::try_parse_from(["vz", "init", "--disk-size", "64G"]).expect("parse");
        assert!(matches!(cli.command, Commands::Init(_)));
    }

    #[test]
    fn parse_run_subcommand() {
        let cli =
            Cli::try_parse_from(["vz", "run", "--image", "base.img", "--headless"]).expect("parse");
        assert!(matches!(cli.command, Commands::Run(_)));
    }

    #[test]
    fn parse_exec_subcommand() {
        let cli =
            Cli::try_parse_from(["vz", "exec", "my-vm", "--", "cargo", "build"]).expect("parse");
        assert!(matches!(cli.command, Commands::Exec(_)));
    }

    #[test]
    fn parse_stop_subcommand() {
        let cli = Cli::try_parse_from(["vz", "stop", "my-vm", "--force"]).expect("parse");
        assert!(matches!(cli.command, Commands::Stop(_)));
    }

    #[test]
    fn parse_save_subcommand() {
        let cli =
            Cli::try_parse_from(["vz", "save", "my-vm", "--output", "state.bin"]).expect("parse");
        assert!(matches!(cli.command, Commands::Save(_)));
    }

    #[test]
    fn parse_restore_subcommand() {
        let cli = Cli::try_parse_from([
            "vz",
            "restore",
            "--state",
            "state.bin",
            "--image",
            "base.img",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Commands::Restore(_)));
    }

    #[test]
    fn parse_cache_list() {
        let cli = Cli::try_parse_from(["vz", "cache", "list"]).expect("parse");
        assert!(matches!(cli.command, Commands::Cache(_)));
    }

    #[test]
    fn parse_cache_clean() {
        let cli = Cli::try_parse_from(["vz", "cache", "clean", "--all"]).expect("parse");
        assert!(matches!(cli.command, Commands::Cache(_)));
    }

    #[test]
    fn parse_cleanup() {
        let cli = Cli::try_parse_from(["vz", "cleanup"]).expect("parse");
        assert!(matches!(cli.command, Commands::Cleanup(_)));
    }

    #[test]
    fn parse_self_sign() {
        let cli = Cli::try_parse_from(["vz", "self-sign"]).expect("parse");
        assert!(matches!(cli.command, Commands::SelfSign(_)));
    }
}
