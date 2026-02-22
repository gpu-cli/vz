//! vz -- container runtime and macOS VM sandbox CLI.
//!
//! Manages OCI containers and macOS virtual machines for sandboxed
//! coding agent execution. On macOS, uses Apple's Virtualization.framework
//! via the `vz` crate. On Linux, uses native OCI runtimes directly.

#![allow(clippy::print_stdout, clippy::print_stderr)]

mod commands;
#[cfg(target_os = "macos")]
mod control;
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
mod gui;
#[cfg(target_os = "macos")]
mod ipsw;
#[cfg(target_os = "macos")]
mod provision;
mod registry;

use clap::Parser;
use tracing::error;

/// vz -- container runtime and macOS VM sandbox.
///
/// Run OCI containers and manage macOS virtual machines for isolated
/// command execution.
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
    /// OCI container runtime operations.
    Oci(Box<commands::oci::OciArgs>),

    /// Manage multi-service stacks from Compose files.
    Stack(commands::stack::StackArgs),

    // ── macOS-only VM commands ────────────────────────────────────
    /// Create a golden macOS VM image from an IPSW.
    #[cfg(target_os = "macos")]
    Init(commands::init::InitArgs),

    /// Start a VM with optional mounts.
    #[cfg(target_os = "macos")]
    Run(commands::run::RunArgs),

    /// Execute a command inside a running VM.
    #[cfg(target_os = "macos")]
    Exec(commands::exec::ExecArgs),

    /// Save VM state for fast restore.
    #[cfg(target_os = "macos")]
    Save(commands::save::SaveArgs),

    /// Restore VM from saved state.
    #[cfg(target_os = "macos")]
    Restore(commands::restore::RestoreArgs),

    /// List running VMs.
    #[cfg(target_os = "macos")]
    List(commands::list::ListArgs),

    /// Stop a running VM.
    #[cfg(target_os = "macos")]
    Stop(commands::stop::StopArgs),

    /// Manage cached files (IPSWs, downloads).
    #[cfg(target_os = "macos")]
    Cache(commands::cache::CacheArgs),

    /// Provision a disk image (user account, guest agent, auto-login).
    #[cfg(target_os = "macos")]
    Provision(commands::provision::ProvisionArgs),

    /// Detect and clean up orphaned VMs.
    #[cfg(target_os = "macos")]
    Cleanup(commands::cleanup::CleanupArgs),

    /// Ad-hoc sign the vz binary with required entitlements.
    #[cfg(target_os = "macos")]
    SelfSign(commands::self_sign::SelfSignArgs),

    /// Run validation suites against image cohorts.
    #[cfg(target_os = "macos")]
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
    #[cfg(target_os = "macos")]
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
            // Cross-platform commands
            Commands::Oci(args) => commands::oci::run(*args).await,
            Commands::Stack(args) => commands::stack::run(args).await,

            // macOS-only VM commands
            #[cfg(target_os = "macos")]
            Commands::Init(args) => commands::init::run(args).await,
            #[cfg(target_os = "macos")]
            Commands::Run(args) => commands::run::run(args).await,
            #[cfg(target_os = "macos")]
            Commands::Exec(args) => commands::exec::run(args).await,
            #[cfg(target_os = "macos")]
            Commands::Save(args) => commands::save::run(args).await,
            #[cfg(target_os = "macos")]
            Commands::Restore(args) => commands::restore::run(args).await,
            #[cfg(target_os = "macos")]
            Commands::List(args) => commands::list::run(args).await,
            #[cfg(target_os = "macos")]
            Commands::Stop(args) => commands::stop::run(args).await,
            #[cfg(target_os = "macos")]
            Commands::Cache(args) => commands::cache::run(args).await,
            #[cfg(target_os = "macos")]
            Commands::Provision(args) => commands::provision::run(args).await,
            #[cfg(target_os = "macos")]
            Commands::Cleanup(args) => commands::cleanup::run(args).await,
            #[cfg(target_os = "macos")]
            Commands::SelfSign(args) => commands::self_sign::run(args).await,
            #[cfg(target_os = "macos")]
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
        let cli = Cli::try_parse_from(["vz", "-v", "oci", "images"]).expect("parse");
        assert_eq!(cli.verbose, 1);
    }

    #[test]
    fn parse_quiet_flag() {
        let cli = Cli::try_parse_from(["vz", "--quiet", "oci", "images"]).expect("parse");
        assert!(cli.quiet);
    }

    #[test]
    fn parse_json_flag() {
        let cli = Cli::try_parse_from(["vz", "--json", "oci", "images"]).expect("parse");
        assert!(cli.json);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_init_subcommand() {
        let cli = Cli::try_parse_from(["vz", "init", "--disk-size", "64G"]).expect("parse");
        assert!(matches!(cli.command, Commands::Init(_)));
    }

    #[cfg(target_os = "macos")]
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
            Commands::Oci(ref args) if matches!(args.action, commands::oci::OciCommand::Pull(_))
        ));
    }

    #[test]
    fn parse_oci_run_subcommand() {
        let cli = Cli::try_parse_from(["vz", "oci", "run", "alpine:latest", "--", "echo", "hello"])
            .expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Oci(ref args) if matches!(args.action, commands::oci::OciCommand::Run(_))
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

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_exec_subcommand() {
        let cli =
            Cli::try_parse_from(["vz", "exec", "my-vm", "--", "cargo", "build"]).expect("parse");
        assert!(matches!(cli.command, Commands::Exec(_)));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_stop_subcommand() {
        let cli = Cli::try_parse_from(["vz", "stop", "my-vm", "--force"]).expect("parse");
        assert!(matches!(cli.command, Commands::Stop(_)));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_save_subcommand() {
        let cli =
            Cli::try_parse_from(["vz", "save", "my-vm", "--output", "state.bin"]).expect("parse");
        assert!(matches!(cli.command, Commands::Save(_)));
    }

    #[cfg(target_os = "macos")]
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

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_cache_list() {
        let cli = Cli::try_parse_from(["vz", "cache", "list"]).expect("parse");
        assert!(matches!(cli.command, Commands::Cache(_)));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_cache_clean() {
        let cli = Cli::try_parse_from(["vz", "cache", "clean", "--all"]).expect("parse");
        assert!(matches!(cli.command, Commands::Cache(_)));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_cleanup() {
        let cli = Cli::try_parse_from(["vz", "cleanup"]).expect("parse");
        assert!(matches!(cli.command, Commands::Cleanup(_)));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_self_sign() {
        let cli = Cli::try_parse_from(["vz", "self-sign"]).expect("parse");
        assert!(matches!(cli.command, Commands::SelfSign(_)));
    }

    #[test]
    fn parse_stack_up() {
        let cli = Cli::try_parse_from(["vz", "stack", "up", "--file", "docker-compose.yaml"])
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
            "vz",
            "stack",
            "up",
            "--file",
            "compose.yaml",
            "--name",
            "myapp",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Commands::Stack(_)));
    }

    #[test]
    fn parse_stack_up_dry_run() {
        let cli = Cli::try_parse_from(["vz", "stack", "up", "--file", "compose.yaml", "--dry-run"])
            .expect("parse");
        if let Commands::Stack(ref args) = cli.command {
            if let commands::stack::StackCommand::Up(ref up) = args.action {
                assert!(up.dry_run);
            } else {
                panic!("expected Up");
            }
        } else {
            panic!("expected Stack");
        }
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
        let cli = Cli::try_parse_from(["vz", "stack", "ps", "myapp", "--json"]).expect("parse");
        assert!(matches!(cli.command, Commands::Stack(_)));
    }

    #[test]
    fn parse_stack_events() {
        let cli = Cli::try_parse_from(["vz", "stack", "events", "myapp"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Stack(ref args)
                if matches!(args.action, commands::stack::StackCommand::Events(_))
        ));
    }

    #[test]
    fn parse_stack_events_with_since() {
        let cli = Cli::try_parse_from(["vz", "stack", "events", "myapp", "--since", "10"])
            .expect("parse");
        assert!(matches!(cli.command, Commands::Stack(_)));
    }

    #[test]
    fn parse_stack_exec() {
        let cli = Cli::try_parse_from([
            "vz", "stack", "exec", "myapp", "db", "--", "psql", "-U", "app",
        ])
        .expect("parse");
        if let Commands::Stack(ref args) = cli.command {
            if let commands::stack::StackCommand::Exec(ref exec) = args.action {
                assert_eq!(exec.name, "myapp");
                assert_eq!(exec.service, "db");
                assert_eq!(exec.command, vec!["psql", "-U", "app"]);
            } else {
                panic!("expected Exec");
            }
        } else {
            panic!("expected Stack");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_validate_run() {
        let cli = Cli::try_parse_from(["vz", "validate", "run", "--tier", "1"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Validate(ref args)
                if matches!(args.action, commands::validate::ValidateCommand::Run(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_validate_run_with_output() {
        let cli = Cli::try_parse_from([
            "vz",
            "validate",
            "run",
            "--tier",
            "2",
            "--output",
            "report.json",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Commands::Validate(_)));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_validate_manifest() {
        let cli = Cli::try_parse_from(["vz", "validate", "manifest"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Validate(ref args)
                if matches!(args.action, commands::validate::ValidateCommand::Manifest(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_validate_list() {
        let cli = Cli::try_parse_from(["vz", "validate", "list"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Validate(ref args)
                if matches!(args.action, commands::validate::ValidateCommand::List(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_validate_list_with_tier() {
        let cli = Cli::try_parse_from(["vz", "validate", "list", "--tier", "2", "--json"])
            .expect("parse");
        assert!(matches!(cli.command, Commands::Validate(_)));
    }
}
