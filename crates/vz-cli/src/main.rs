//! Developer Environment lifecycle CLI.
//!
//! Topology-owned execution, status, and lifecycle operations use the typed
//! daemon API. Retired infrastructure commands have no parser or dispatch path.

#![allow(clippy::print_stdout, clippy::print_stderr)]

mod commands;

use clap::{CommandFactory, Parser};
use tracing::error;
use vz_cli::legacy_cli::{LEGACY_COMMAND_REMOVED_EXIT_CODE, rejection_for_args};

const CLI_WORKFLOW_EXAMPLES: &str = "\
Examples:
  vz                  Show this help without touching runtime state
  vz status --json    Inspect the selected Environment topology
  vz exec -- uname -s  Execute in the selected Ready Machine

Implementation status: DEV. The complete 0.4 five-verb lifecycle is not yet shipped.";

/// vz — reproducible, parallel Developer Environments.
///
/// Run `vz` without arguments to print top-level help without accessing runtime
/// state. Legacy bare-mode mutation and configuration flags are rejected.
#[derive(Parser, Debug)]
#[command(
    name = "vz",
    version,
    about = "vz — reproducible, parallel Developer Environments",
    after_help = CLI_WORKFLOW_EXAMPLES,
    long_about = None
)]
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
    command: Option<Commands>,
}

#[derive(clap::Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
enum Commands {
    /// Create/reconcile the selected Environment (Linux-on-macOS DEV adapter).
    ///
    /// Developer boots retain private Engine endpoints but are not Ready until
    /// complete Docker/Compose/buildx readiness evidence is available.
    Up(commands::dev_up::DevUpArgs),
    /// Stop the selected Developer Environment, preserving identities and state.
    Stop(commands::dev_stop::DevStopArgs),

    /// Execute in one selected Ready Machine (Linux-on-macOS DEV adapter).
    ///
    /// Automatic startup/reconciliation and native-target execution are not yet
    /// available. Unknown runtime ownership fails closed; no legacy Run fallback.
    Exec(commands::dev_exec::DevExecArgs),

    /// Show the project's persisted Developer Environment topology.
    Status(commands::dev_status::DevStatusArgs),
}

fn main() -> anyhow::Result<()> {
    let args = std::env::args_os().collect::<Vec<_>>();
    if args.len() == 1 {
        Cli::command().print_help()?;
        return Ok(());
    }
    if let Some(rejection) = rejection_for_args(args.iter().cloned()) {
        eprintln!("{}", rejection.to_json());
        std::process::exit(LEGACY_COMMAND_REMOVED_EXIT_CODE);
    }
    let cli = Cli::parse_from(args);
    if cli.command.is_none() {
        Cli::command().print_help()?;
        return Ok(());
    }

    let json = cli.json;
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

    // Only explicit topology commands reach runtime setup.
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let result = match cli.command {
            // Bare and read-only-global-only invocations returned before setup.
            None => unreachable!("command absence handled before runtime setup"),

            Some(Commands::Up(args)) => match commands::dev_up::cmd_dev_up(args, json).await {
                Ok(()) => Ok(()),
                Err(error) => {
                    if !error.already_emitted() {
                        eprintln!("{}", error.to_json());
                    }
                    std::process::exit(error.exit_code());
                }
            },
            Some(Commands::Stop(args)) => {
                match commands::dev_stop::cmd_dev_stop(args, json).await {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        if !error.already_emitted() {
                            eprintln!("{}", error.to_json());
                        }
                        std::process::exit(error.exit_code());
                    }
                }
            }
            Some(Commands::Exec(args)) => {
                // cmd_dev_exec restores terminal state and flushes output before
                // returning. Exit directly: a pending Tokio stdin blocking read
                // must not make a completed guest process hang runtime teardown.
                let code = match commands::dev_exec::cmd_dev_exec(args, json).await {
                    Ok(code) => code,
                    Err(error) => {
                        eprintln!("{}", error.to_json());
                        error.exit_code()
                    }
                };
                std::process::exit(code);
            }
            Some(Commands::Status(args)) => {
                match commands::dev_status::cmd_dev_status(args, json).await {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        eprintln!("{}", error.to_json());
                        std::process::exit(error.exit_code());
                    }
                }
            }
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
    fn help_includes_bare_nonmutating_workflow_example() {
        let mut command = Cli::command();
        let mut help = Vec::new();
        command
            .write_long_help(&mut help)
            .expect("render help text");
        let text = String::from_utf8(help).expect("help should be valid utf-8");

        assert!(text.contains("Examples:"));
        assert!(text.contains("vz                  Show this help without touching runtime state"));
        assert!(!text.contains("--continue"));
        assert!(!text.contains("--resume"));
    }

    #[test]
    fn parse_no_subcommand_has_no_command() {
        let cli = Cli::try_parse_from(["vz"]).expect("parse");
        assert!(cli.command.is_none());
    }

    #[test]
    fn parse_status_topology_selectors() {
        let cli = Cli::try_parse_from(["vz", "status", "--environment", "dev", "--machine", "app"])
            .expect("parse");
        assert!(matches!(
            cli.command,
            Some(Commands::Status(commands::dev_status::DevStatusArgs {
                environment: Some(ref environment),
                machine: Some(ref machine),
                all: false,
            })) if environment == "dev" && machine == "app"
        ));
    }

    #[test]
    fn parse_status_all_and_reject_mixed_selectors() {
        let cli = Cli::try_parse_from(["vz", "status", "--all"]).expect("parse");
        assert!(matches!(
            cli.command,
            Some(Commands::Status(commands::dev_status::DevStatusArgs {
                all: true,
                ..
            }))
        ));
        assert!(Cli::try_parse_from(["vz", "status", "--all", "--environment", "dev"]).is_err());
        assert!(Cli::try_parse_from(["vz", "status", "--all", "--machine", "app"]).is_err());
    }

    #[test]
    fn parse_read_only_global_controls() {
        let cli = Cli::try_parse_from(["vz", "-vv", "--quiet", "--json", "status"]).expect("parse");
        assert_eq!(cli.verbose, 2);
        assert!(cli.quiet);
        assert!(cli.json);
    }

    #[test]
    fn dev_parser_inventory_contains_only_implemented_lifecycle_commands() {
        let mut command = Cli::command();
        command.build();
        let names = command
            .get_subcommands()
            .map(|child| child.get_name())
            .collect::<Vec<_>>();
        // Transitional DEV assertion, not the five-verb release acceptance gate.
        assert_eq!(names, ["up", "stop", "exec", "status", "help"]);
        for child in command.get_subcommands() {
            assert!(!child.is_hide_set());
            assert_eq!(child.get_all_aliases().count(), 0);
        }
    }

    #[test]
    fn removed_roots_have_no_parser_even_without_preflight_rejection() {
        for root in vz_cli::legacy_cli::REMOVED_ROOT_COMMANDS {
            for suffix in [&[][..], &["--help"][..], &["arbitrary", "--unknown"][..]] {
                let args = std::iter::once("vz")
                    .chain(std::iter::once(*root))
                    .chain(suffix.iter().copied());
                let error = Cli::try_parse_from(args).expect_err("removed parser must not exist");
                assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
            }
            let error =
                Cli::try_parse_from(["vz", "help", root]).expect_err("no hidden help parser");
            assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
        }
    }

    #[test]
    fn all_hidden_help_contains_no_retired_command_or_alias() {
        let mut command = Cli::command();
        command.build();
        // Explicitly unhide every parser node. A hidden compatibility family
        // must not evade a public-help snapshot.
        let names = command
            .get_subcommands()
            .map(|child| child.get_name().to_owned())
            .collect::<Vec<_>>();
        for name in names {
            command = command.mut_subcommand(name, |child| child.hide(false));
        }
        let text = command.render_long_help().to_string();
        for root in vz_cli::legacy_cli::REMOVED_ROOT_COMMANDS {
            assert!(
                !text
                    .lines()
                    .any(|line| line.trim_start().starts_with(&format!("{root} ")))
            );
        }
    }
}
