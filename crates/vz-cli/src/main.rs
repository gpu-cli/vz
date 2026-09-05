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
use vz_macos_provision as provision;
mod registry;

use clap::{CommandFactory, Parser};
use tracing::error;
use vz_cli::legacy_cli::{LEGACY_COMMAND_REMOVED_EXIT_CODE, rejection_for_args};

const CLI_WORKFLOW_EXAMPLES: &str = "\
Examples:
  vz                  Show this help without touching runtime state
  vz ls               List sandboxes
  vz <COMMAND>        Run an explicit subcommand";

/// vz — instant sandboxed Linux environments.
///
/// Run `vz` without arguments to print top-level help without accessing runtime
/// state. Legacy bare-mode mutation and configuration flags are rejected.
#[derive(Parser, Debug)]
#[command(
    name = "vz",
    version,
    about = "vz — instant sandboxed Linux environments",
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
    /// Create a new sandbox without attaching an interactive shell.
    Create(commands::sandbox::SandboxCreateArgs),

    // ── Sandbox management (top-level) ──
    /// List sandboxes.
    Ls(commands::sandbox::SandboxListArgs),

    /// Remove a sandbox.
    Rm(commands::sandbox::SandboxTerminateArgs),

    /// Show detailed sandbox information (JSON).
    Inspect(commands::sandbox::SandboxInspectArgs),

    /// Attach to a running sandbox.
    Attach(commands::sandbox::SandboxAttachArgs),

    /// Close an active shell session for a sandbox.
    CloseShell(commands::sandbox::SandboxCloseShellArgs),

    // ── Dev environment ──
    /// Generate a vz.json configuration for the current project.
    Init(commands::dev_init::DevInitArgs),

    /// Run a command in the project's Linux VM (reads vz.json).
    #[cfg(target_os = "macos")]
    Run(commands::dev::DevRunArgs),

    /// Stop the Linux VM for the current project.
    #[cfg(target_os = "macos")]
    Stop(commands::dev::DevStopArgs),

    /// Show the project's persisted Developer Environment topology.
    Status(commands::dev_status::DevStatusArgs),

    /// Show daemon logs for debugging.
    Logs(commands::dev_logs::DevLogsArgs),

    // ── Image management ──
    /// OCI image management (pull, build, list, prune).
    Image(commands::image::ImageArgs),

    // ── Diff contract ──
    /// Compare two checkpoints with the versioned diff contract.
    Diff(commands::diff::DiffArgs),

    /// Checkpoint lifecycle management (list, inspect, create, restore, fork).
    Checkpoint(commands::checkpoint::CheckpointArgs),

    /// VM command namespaces (`mac`, `linux`).
    Vm(commands::vm::VmArgs),

    // ── Setup ──
    /// Ad-hoc sign the vz binary with required entitlements.
    ///
    /// Required after `cargo install vz-cli` to enable Virtualization.framework.
    SelfSign(commands::self_sign::SelfSignArgs),

    // ── Debug/advanced (hidden) ──
    /// Advanced debugging and low-level operations.
    #[command(hide = true)]
    Debug(Box<commands::debug::DebugArgs>),
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

    // GUI mode: `vz vm mac run` (and legacy `vz debug vm run`) without
    // --headless needs AppKit on the main thread.
    #[cfg(target_os = "macos")]
    if let Some(Commands::Vm(ref vm_args)) = cli.command
        && let commands::vm::VmCommand::Mac(ref mac_args) = vm_args.action
        && let commands::vm::MacVmCommand::Run(ref args) = mac_args.action
        && !args.headless
    {
        let Some(Commands::Vm(vm_args)) = cli.command else {
            unreachable!()
        };
        let commands::vm::VmCommand::Mac(mac_args) = vm_args.action else {
            unreachable!()
        };
        let commands::vm::MacVmCommand::Run(args) = mac_args.action else {
            unreachable!()
        };
        return gui::run_with_gui(args);
    }

    #[cfg(target_os = "macos")]
    if let Some(Commands::Debug(ref debug_args)) = cli.command {
        if let commands::debug::DebugCommand::Vm(ref vm_args) = debug_args.action {
            if let commands::vm::MacVmCommand::Run(ref args) = vm_args.action {
                if !args.headless {
                    let Some(Commands::Debug(debug_args)) = cli.command else {
                        unreachable!()
                    };
                    let commands::debug::DebugCommand::Vm(mac_args) = debug_args.action else {
                        unreachable!()
                    };
                    let commands::vm::MacVmCommand::Run(args) = mac_args.action else {
                        unreachable!()
                    };
                    return gui::run_with_gui(args);
                }
            }
        }
    }

    // Headless path: normal tokio runtime.
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let result = match cli.command {
            // Bare and read-only-global-only invocations returned before setup.
            None => unreachable!("command absence handled before runtime setup"),

            // Sandbox management
            Some(Commands::Create(args)) => commands::sandbox::cmd_create(args).await,
            Some(Commands::Ls(args)) => commands::sandbox::cmd_list(args).await,
            Some(Commands::Rm(args)) => commands::sandbox::cmd_terminate(args).await,
            Some(Commands::Inspect(args)) => commands::sandbox::cmd_inspect(args).await,
            Some(Commands::Attach(args)) => commands::sandbox::cmd_attach(args).await,
            Some(Commands::CloseShell(args)) => commands::sandbox::cmd_close_shell(args).await,

            // Dev environment
            Some(Commands::Init(args)) => commands::dev_init::cmd_dev_init(args).await,
            #[cfg(target_os = "macos")]
            Some(Commands::Run(args)) => commands::dev::cmd_run(args).await,
            #[cfg(target_os = "macos")]
            Some(Commands::Stop(args)) => commands::dev::cmd_stop(args).await,
            Some(Commands::Status(args)) => {
                match commands::dev_status::cmd_dev_status(args, json).await {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        eprintln!("{}", error.to_json());
                        std::process::exit(error.exit_code());
                    }
                }
            }
            Some(Commands::Logs(args)) => commands::dev_logs::cmd_dev_logs(args).await,

            // Image management
            Some(Commands::Image(args)) => commands::image::run(args).await,

            // Diff contract
            Some(Commands::Diff(args)) => commands::diff::run(args).await,

            // Checkpoint lifecycle
            Some(Commands::Checkpoint(args)) => commands::checkpoint::run(args).await,

            // VM command namespaces
            Some(Commands::Vm(args)) => commands::vm::run(args).await,

            // Setup
            Some(Commands::SelfSign(args)) => commands::self_sign::run(args).await,

            // Debug/advanced
            Some(Commands::Debug(args)) => commands::debug::run(*args).await,
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
    fn parse_close_shell_subcommand() {
        let cli = Cli::try_parse_from(["vz", "close-shell", "sandbox-a"]).expect("parse");
        assert!(matches!(
            cli.command,
            Some(Commands::CloseShell(commands::sandbox::SandboxCloseShellArgs {
                sandbox_id,
                ..
            })) if sandbox_id == "sandbox-a"
        ));
    }

    #[test]
    fn parse_verbose_flag() {
        let cli = Cli::try_parse_from(["vz", "-v", "ls"]).expect("parse");
        assert_eq!(cli.verbose, 1);
    }

    #[test]
    fn parse_quiet_flag() {
        let cli = Cli::try_parse_from(["vz", "--quiet", "ls"]).expect("parse");
        assert!(cli.quiet);
    }

    #[test]
    fn parse_json_flag() {
        let cli = Cli::try_parse_from(["vz", "--json", "ls"]).expect("parse");
        assert!(cli.json);
    }

    #[test]
    fn parse_ls_subcommand() {
        let cli = Cli::try_parse_from(["vz", "ls"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Ls(_))));
    }

    #[test]
    fn parse_rm_subcommand() {
        let cli = Cli::try_parse_from(["vz", "rm", "sbx-123"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Rm(_))));
    }

    #[test]
    fn parse_inspect_subcommand() {
        let cli = Cli::try_parse_from(["vz", "inspect", "sbx-123"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Inspect(_))));
    }

    #[test]
    fn parse_attach_subcommand() {
        let cli = Cli::try_parse_from(["vz", "attach", "sbx-123"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Attach(_))));
    }

    #[test]
    fn parse_diff_subcommand() {
        let cli = Cli::try_parse_from(["vz", "diff", "cp-001", "cp-002", "--mode", "system"])
            .expect("parse");
        assert!(matches!(cli.command, Some(Commands::Diff(_))));
    }

    #[test]
    fn parse_image_pull() {
        let cli = Cli::try_parse_from(["vz", "image", "pull", "alpine:latest"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Image(_))));
    }

    #[test]
    fn parse_image_ls() {
        let cli = Cli::try_parse_from(["vz", "image", "ls"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Image(_))));
    }

    #[test]
    fn parse_image_prune() {
        let cli = Cli::try_parse_from(["vz", "image", "prune"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Image(_))));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_image_build() {
        let cli = Cli::try_parse_from(["vz", "image", "build"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Image(_))));
    }

    #[test]
    fn parse_debug_docker() {
        let cli = Cli::try_parse_from(["vz", "debug", "docker", "ps"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Debug(_))));
    }

    #[test]
    fn parse_debug_container_run() {
        let cli = Cli::try_parse_from([
            "vz",
            "debug",
            "container",
            "run",
            "alpine:latest",
            "--",
            "echo",
            "hello",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Some(Commands::Debug(_))));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_debug_vm_init() {
        let cli = Cli::try_parse_from(["vz", "debug", "vm", "init", "--disk-size", "64G"])
            .expect("parse");
        assert!(matches!(cli.command, Some(Commands::Debug(_))));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_mac_init() {
        let cli =
            Cli::try_parse_from(["vz", "vm", "mac", "init", "--disk-size", "64G"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Vm(_))));
    }

    #[test]
    fn parse_vm_linux_e2e() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "linux",
            "test",
            "e2e",
            "--vm-name",
            "linux-e2e",
            "--guest-repo",
            "/workspaces/vz",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Some(Commands::Vm(_))));
    }

    #[test]
    fn parse_vm_linux_run() {
        let cli = Cli::try_parse_from([
            "vz", "vm", "linux", "run", "--name", "space-a", "--cpus", "4", "--memory", "4096",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Some(Commands::Vm(_))));
    }

    #[test]
    fn parse_vm_linux_init() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "linux",
            "init",
            "--name",
            "linux-test",
            "--disk-size-gb",
            "80",
            "--force",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Some(Commands::Vm(_))));
    }

    #[test]
    fn parse_vm_linux_list() {
        let cli = Cli::try_parse_from(["vz", "vm", "linux", "list"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Vm(_))));
    }

    #[test]
    fn parse_vm_linux_inspect() {
        let cli = Cli::try_parse_from(["vz", "vm", "linux", "inspect", "vz-1234"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Vm(_))));
    }

    #[test]
    fn parse_vm_linux_attach() {
        let cli = Cli::try_parse_from(["vz", "vm", "linux", "attach", "vz-1234"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Vm(_))));
    }

    #[test]
    fn parse_vm_linux_exec() {
        let cli = Cli::try_parse_from([
            "vz", "vm", "linux", "exec", "vz-1234", "--", "echo", "hello",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Some(Commands::Vm(_))));
    }

    #[test]
    fn parse_vm_linux_stop() {
        let cli = Cli::try_parse_from(["vz", "vm", "linux", "stop", "vz-1234"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Vm(_))));
    }

    #[test]
    fn parse_vm_linux_rm() {
        let cli = Cli::try_parse_from(["vz", "vm", "linux", "rm", "vz-1234"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Vm(_))));
    }

    #[test]
    fn parse_debug_lease() {
        let cli = Cli::try_parse_from(["vz", "debug", "lease", "list"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Debug(_))));
    }
}
