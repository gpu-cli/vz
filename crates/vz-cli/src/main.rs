//! vz -- macOS VM sandbox CLI.
//!
//! Manages macOS virtual machines for sandboxed coding agent execution.
//! Built on Apple's Virtualization.framework via the `vz` crate.

#![cfg(target_os = "macos")]
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod commands;
mod control;
#[allow(unsafe_code)]
mod gui;
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

    /// OCI container runtime operations.
    Oci(Box<commands::oci::OciArgs>),

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

    /// Manage multi-service stacks from Compose files.
    Stack(commands::stack::StackArgs),

    /// Run validation suites against image cohorts.
    Validate(commands::validate::ValidateArgs),
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

    // GUI mode: `vz run` without --headless needs AppKit on the main thread.
    if let Commands::Run(ref args) = cli.command {
        if !args.headless {
            let Commands::Run(args) = cli.command else {
                unreachable!()
            };
            return gui::run_with_gui(args);
        }
    }

    // Headless path: normal tokio runtime.
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let result = match cli.command {
            Commands::Init(args) => commands::init::run(args).await,
            Commands::Run(args) => commands::run::run(args).await,
            Commands::Oci(args) => commands::oci::run(*args).await,
            Commands::Exec(args) => commands::exec::run(args).await,
            Commands::Save(args) => commands::save::run(args).await,
            Commands::Restore(args) => commands::restore::run(args).await,
            Commands::List(args) => commands::list::run(args).await,
            Commands::Stop(args) => commands::stop::run(args).await,
            Commands::Cache(args) => commands::cache::run(args).await,
            Commands::Provision(args) => commands::provision::run(args).await,
            Commands::Cleanup(args) => commands::cleanup::run(args).await,
            Commands::SelfSign(args) => commands::self_sign::run(args).await,
            Commands::Stack(args) => commands::stack::run(args).await,
            Commands::Validate(args) => commands::validate::run(args).await,
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
    fn parse_oci_pull_subcommand() {
        let cli = Cli::try_parse_from(["vz", "oci", "pull", "alpine:latest"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Oci(args) if matches!(args.action, commands::oci::OciCommand::Pull(_))
        ));
    }

    #[test]
    fn parse_oci_run_subcommand() {
        let cli = Cli::try_parse_from(["vz", "oci", "run", "alpine:latest", "--", "echo", "hello"])
            .expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Oci(args) if matches!(args.action, commands::oci::OciCommand::Run(_))
        ));
    }

    #[test]
    fn parse_oci_run_with_publish_flag() {
        let cli = Cli::try_parse_from(["vz", "oci", "run", "nginx:alpine", "--publish", "8080:80"])
            .expect("parse");

        match cli.command {
            Commands::Oci(args) => match args.action {
                commands::oci::OciCommand::Run(run) => {
                    assert_eq!(run.publish, vec!["8080:80".to_string()]);
                }
                other => panic!("unexpected OCI action variant: {other:?}"),
            },
            other => panic!("unexpected command variant: {other:?}"),
        }
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

    #[test]
    fn parse_stack_up() {
        let cli =
            Cli::try_parse_from(["vz", "stack", "up", "--file", "docker-compose.yaml"])
                .expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Stack(ref args)
                if matches!(args.action, commands::stack::StackCommand::Up(_))
        ));
    }

    #[test]
    fn parse_stack_up_with_name() {
        let cli = Cli::try_parse_from([
            "vz", "stack", "up", "--file", "compose.yaml", "--name", "myapp",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Commands::Stack(_)));
    }

    #[test]
    fn parse_stack_down() {
        let cli = Cli::try_parse_from(["vz", "stack", "down", "myapp"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Stack(ref args)
                if matches!(args.action, commands::stack::StackCommand::Down(_))
        ));
    }

    #[test]
    fn parse_stack_ps() {
        let cli = Cli::try_parse_from(["vz", "stack", "ps", "myapp"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Stack(ref args)
                if matches!(args.action, commands::stack::StackCommand::Ps(_))
        ));
    }

    #[test]
    fn parse_stack_ps_json() {
        let cli =
            Cli::try_parse_from(["vz", "stack", "ps", "myapp", "--json"]).expect("parse");
        assert!(matches!(cli.command, Commands::Stack(_)));
    }

    #[test]
    fn parse_stack_events() {
        let cli =
            Cli::try_parse_from(["vz", "stack", "events", "myapp"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Stack(ref args)
                if matches!(args.action, commands::stack::StackCommand::Events(_))
        ));
    }

    #[test]
    fn parse_stack_events_with_since() {
        let cli = Cli::try_parse_from([
            "vz", "stack", "events", "myapp", "--since", "10",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Commands::Stack(_)));
    }

    #[test]
    fn parse_validate_run() {
        let cli = Cli::try_parse_from(["vz", "validate", "run", "--tier", "1"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Validate(ref args)
                if matches!(args.action, commands::validate::ValidateCommand::Run(_))
        ));
    }

    #[test]
    fn parse_validate_run_with_output() {
        let cli = Cli::try_parse_from([
            "vz", "validate", "run", "--tier", "2", "--output", "report.json",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Commands::Validate(_)));
    }

    #[test]
    fn parse_validate_manifest() {
        let cli = Cli::try_parse_from(["vz", "validate", "manifest"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Validate(ref args)
                if matches!(args.action, commands::validate::ValidateCommand::Manifest(_))
        ));
    }

    #[test]
    fn parse_validate_list() {
        let cli = Cli::try_parse_from(["vz", "validate", "list"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Validate(ref args)
                if matches!(args.action, commands::validate::ValidateCommand::List(_))
        ));
    }

    #[test]
    fn parse_validate_list_with_tier() {
        let cli =
            Cli::try_parse_from(["vz", "validate", "list", "--tier", "2", "--json"]).expect("parse");
        assert!(matches!(cli.command, Commands::Validate(_)));
    }
}
