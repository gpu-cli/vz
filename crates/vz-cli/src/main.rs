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
pub mod tui;

use clap::Parser;
use tracing::error;

/// vz — instant sandboxed Linux environments.
///
/// Run `vz` to create and attach to a new sandbox. Use `vz -c` to continue
/// the most recent sandbox, or `vz -r <name>` to resume a specific one.
#[derive(Parser, Debug)]
#[command(
    name = "vz",
    version,
    about = "vz — instant sandboxed Linux environments",
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

    /// Continue most recent sandbox for this directory.
    #[arg(short = 'c', long = "continue", conflicts_with_all = ["resume", "name"])]
    continue_last: bool,

    /// Resume a specific sandbox by name or ID.
    #[arg(short = 'r', long = "resume", conflicts_with_all = ["continue_last", "name"])]
    resume: Option<String>,

    /// Name the new sandbox.
    #[arg(long, conflicts_with_all = ["continue_last", "resume"])]
    name: Option<String>,

    /// Number of virtual CPUs for new sandboxes.
    #[arg(long, default_value = "2")]
    cpus: u8,

    /// Memory in MB for new sandboxes.
    #[arg(long, default_value = "2048")]
    memory: u64,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    // ── Sandbox management (top-level) ──
    /// List sandboxes.
    Ls(commands::sandbox::SandboxListArgs),

    /// Remove a sandbox.
    Rm(commands::sandbox::SandboxTerminateArgs),

    /// Show detailed sandbox information (JSON).
    Inspect(commands::sandbox::SandboxInspectArgs),

    /// Attach to a running sandbox.
    Attach(commands::sandbox::SandboxAttachArgs),

    // ── Stack orchestration ──
    /// Multi-service stack orchestration from Compose files.
    Stack(commands::stack::StackArgs),

    // ── Image management ──
    /// OCI image management (pull, build, list, prune).
    Image(commands::image::ImageArgs),

    // ── Debug/advanced (hidden) ──
    /// Advanced debugging and low-level operations.
    #[command(hide = true)]
    Debug(Box<commands::debug::DebugArgs>),
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Suppress info-level tracing noise for stack up/down — the StackOutput
    // abstraction handles user-facing progress display instead.
    let is_stack_progress = matches!(
        cli.command,
        Some(Commands::Stack(ref args)) if matches!(
            args.action,
            commands::stack::StackCommand::Up(_)
            | commands::stack::StackCommand::Down(_)
        )
    );

    // Default sandbox mode should suppress info logs too.
    let is_sandbox_mode = cli.command.is_none();

    let filter = if cli.quiet || ((is_stack_progress || is_sandbox_mode) && cli.verbose == 0) {
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

    // GUI mode: `vz debug vm run` without --headless needs AppKit on the main thread.
    #[cfg(target_os = "macos")]
    if let Some(Commands::Debug(ref debug_args)) = cli.command {
        if let commands::debug::DebugCommand::Vm(ref vm_args) = debug_args.action {
            if let commands::vm::VmCommand::Run(ref args) = vm_args.action {
                if !args.headless {
                    let Some(Commands::Debug(debug_args)) = cli.command else {
                        unreachable!()
                    };
                    let commands::debug::DebugCommand::Vm(vm_args) = debug_args.action else {
                        unreachable!()
                    };
                    let commands::vm::VmCommand::Run(args) = vm_args.action else {
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
            // No subcommand = default sandbox mode.
            None => {
                commands::sandbox::cmd_default_sandbox(
                    cli.continue_last,
                    cli.resume,
                    cli.name,
                    cli.cpus,
                    cli.memory,
                )
                .await
            }

            // Sandbox management
            Some(Commands::Ls(args)) => commands::sandbox::cmd_list(args),
            Some(Commands::Rm(args)) => commands::sandbox::cmd_terminate(args),
            Some(Commands::Inspect(args)) => commands::sandbox::cmd_inspect(args),
            Some(Commands::Attach(args)) => commands::sandbox::cmd_attach(args).await,

            // Stack orchestration
            Some(Commands::Stack(args)) => commands::stack::run(args).await,

            // Image management
            Some(Commands::Image(args)) => commands::image::run(args).await,

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
    fn parse_no_subcommand_creates_sandbox() {
        let cli = Cli::try_parse_from(["vz"]).expect("parse");
        assert!(cli.command.is_none());
        assert!(!cli.continue_last);
        assert!(cli.resume.is_none());
        assert!(cli.name.is_none());
    }

    #[test]
    fn parse_continue_flag() {
        let cli = Cli::try_parse_from(["vz", "-c"]).expect("parse");
        assert!(cli.continue_last);
        assert!(cli.command.is_none());
    }

    #[test]
    fn parse_resume_flag() {
        let cli = Cli::try_parse_from(["vz", "-r", "my-box"]).expect("parse");
        assert_eq!(cli.resume.as_deref(), Some("my-box"));
    }

    #[test]
    fn parse_named_sandbox() {
        let cli = Cli::try_parse_from(["vz", "--name", "my-project"]).expect("parse");
        assert_eq!(cli.name.as_deref(), Some("my-project"));
    }

    #[test]
    fn parse_sandbox_resources() {
        let cli = Cli::try_parse_from(["vz", "--cpus", "4", "--memory", "4096"]).expect("parse");
        assert_eq!(cli.cpus, 4);
        assert_eq!(cli.memory, 4096);
    }

    #[test]
    fn parse_continue_conflicts_with_resume() {
        let result = Cli::try_parse_from(["vz", "-c", "-r", "foo"]);
        assert!(result.is_err());
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

    #[test]
    fn parse_stack_up() {
        let cli = Cli::try_parse_from(["vz", "stack", "up", "--file", "docker-compose.yaml"])
            .expect("parse");
        assert!(matches!(
            cli.command,
            Some(Commands::Stack(ref args))
                if matches!(args.action, commands::stack::StackCommand::Up(_))
        ));
    }

    #[test]
    fn parse_stack_down() {
        let cli = Cli::try_parse_from(["vz", "stack", "down", "myapp"]).expect("parse");
        assert!(matches!(
            cli.command,
            Some(Commands::Stack(ref args))
                if matches!(args.action, commands::stack::StackCommand::Down(_))
        ));
    }

    #[test]
    fn parse_stack_exec() {
        let cli = Cli::try_parse_from([
            "vz", "stack", "exec", "myapp", "db", "--", "psql", "-U", "app",
        ])
        .expect("parse");
        if let Some(Commands::Stack(ref args)) = cli.command {
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

    #[test]
    fn parse_debug_lease() {
        let cli = Cli::try_parse_from(["vz", "debug", "lease", "list"]).expect("parse");
        assert!(matches!(cli.command, Some(Commands::Debug(_))));
    }
}
