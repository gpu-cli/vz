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
    // ── Container commands (cross-platform, top-level) ──
    /// Pull and cache an OCI image locally.
    Pull(commands::oci::PullArgs),

    /// Run a container from an OCI image.
    Run(Box<commands::oci::RunArgs>),

    /// Create and start a long-lived container.
    Create(Box<commands::oci::CreateArgs>),

    /// Execute a command in a running container.
    Exec(commands::oci::ExecArgs),

    /// List cached OCI images.
    Images(commands::oci::ImagesArgs),

    /// Remove stale image and layer artifacts.
    Prune(commands::oci::PruneArgs),

    /// List containers.
    Ps(commands::oci::PsArgs),

    /// Stop a running container.
    Stop(commands::oci::StopArgs),

    /// Remove container metadata and rootfs.
    Rm(commands::oci::RmArgs),

    /// Show container logs.
    Logs(commands::oci::LogsArgs),

    // ── Stack orchestration (cross-platform) ──
    /// Manage multi-service stacks from Compose files.
    Stack(commands::stack::StackArgs),

    // ── VM management (macOS only) ──
    /// Manage virtual machines.
    #[cfg(target_os = "macos")]
    Vm(commands::vm::VmArgs),
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

    // GUI mode: `vz vm run` without --headless needs AppKit on the main thread.
    #[cfg(target_os = "macos")]
    if let Commands::Vm(ref vm_args) = cli.command {
        if let commands::vm::VmCommand::Run(ref args) = vm_args.action {
            if !args.headless {
                let Commands::Vm(vm_args) = cli.command else {
                    unreachable!()
                };
                let commands::vm::VmCommand::Run(args) = vm_args.action else {
                    unreachable!()
                };
                return gui::run_with_gui(args);
            }
        }
    }

    // Headless path: normal tokio runtime.
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let result = match cli.command {
            // Container commands
            Commands::Pull(args) => commands::oci::run_pull(args).await,
            Commands::Run(args) => commands::oci::run_container(*args).await,
            Commands::Create(args) => commands::oci::run_create(*args).await,
            Commands::Exec(args) => commands::oci::run_exec(args).await,
            Commands::Images(args) => commands::oci::run_images(args).await,
            Commands::Prune(args) => commands::oci::run_prune(args).await,
            Commands::Ps(args) => commands::oci::run_ps(args).await,
            Commands::Stop(args) => commands::oci::run_stop(args).await,
            Commands::Rm(args) => commands::oci::run_rm(args).await,
            Commands::Logs(args) => commands::oci::run_logs(args).await,

            // Stack orchestration
            Commands::Stack(args) => commands::stack::run(args).await,

            // VM management (macOS only)
            #[cfg(target_os = "macos")]
            Commands::Vm(args) => commands::vm::run(args).await,
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
        let cli = Cli::try_parse_from(["vz", "-v", "images"]).expect("parse");
        assert_eq!(cli.verbose, 1);
    }

    #[test]
    fn parse_quiet_flag() {
        let cli = Cli::try_parse_from(["vz", "--quiet", "images"]).expect("parse");
        assert!(cli.quiet);
    }

    #[test]
    fn parse_json_flag() {
        let cli = Cli::try_parse_from(["vz", "--json", "images"]).expect("parse");
        assert!(cli.json);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_init_subcommand() {
        let cli =
            Cli::try_parse_from(["vz", "vm", "init", "--disk-size", "64G"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Init(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_run_subcommand() {
        let cli = Cli::try_parse_from(["vz", "vm", "run", "--image", "base.img", "--headless"])
            .expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Run(_))
        ));
    }

    #[test]
    fn parse_pull_subcommand() {
        let cli = Cli::try_parse_from(["vz", "pull", "alpine:latest"]).expect("parse");
        assert!(matches!(cli.command, Commands::Pull(_)));
    }

    #[test]
    fn parse_logs_subcommand() {
        let cli = Cli::try_parse_from(["vz", "logs", "ctr-123"]).expect("parse");
        assert!(matches!(cli.command, Commands::Logs(_)));
    }

    #[test]
    fn parse_logs_with_follow_and_tail() {
        let cli =
            Cli::try_parse_from(["vz", "logs", "ctr-123", "--follow", "--tail", "50"])
                .expect("parse");
        match cli.command {
            Commands::Logs(args) => {
                assert_eq!(args.id, "ctr-123");
                assert!(args.follow);
                assert_eq!(args.tail, 50);
            }
            other => panic!("unexpected command variant: {other:?}"),
        }
    }

    #[test]
    fn parse_container_run_subcommand() {
        let cli =
            Cli::try_parse_from(["vz", "run", "alpine:latest", "--", "echo", "hello"])
                .expect("parse");
        assert!(matches!(cli.command, Commands::Run(_)));
    }

    #[test]
    fn parse_run_with_publish_flag() {
        let cli =
            Cli::try_parse_from(["vz", "run", "nginx:alpine", "--publish", "8080:80"])
                .expect("parse");

        match cli.command {
            Commands::Run(args) => {
                assert_eq!(args.publish, vec!["8080:80".to_string()]);
            }
            other => panic!("unexpected command variant: {other:?}"),
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_exec_subcommand() {
        let cli = Cli::try_parse_from(["vz", "vm", "exec", "my-vm", "--", "cargo", "build"])
            .expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Exec(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_stop_subcommand() {
        let cli =
            Cli::try_parse_from(["vz", "vm", "stop", "my-vm", "--force"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Stop(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_save_subcommand() {
        let cli = Cli::try_parse_from(["vz", "vm", "save", "my-vm", "--output", "state.bin"])
            .expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Save(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_restore_subcommand() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "restore",
            "--state",
            "state.bin",
            "--image",
            "base.img",
        ])
        .expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Restore(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_cache_list() {
        let cli = Cli::try_parse_from(["vz", "vm", "cache", "list"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Cache(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_cache_clean() {
        let cli =
            Cli::try_parse_from(["vz", "vm", "cache", "clean", "--all"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Cache(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_cleanup() {
        let cli = Cli::try_parse_from(["vz", "vm", "cleanup"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Cleanup(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_self_sign() {
        let cli = Cli::try_parse_from(["vz", "vm", "self-sign"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::SelfSign(_))
        ));
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
    fn parse_vm_validate_run() {
        let cli =
            Cli::try_parse_from(["vz", "vm", "validate", "run", "--tier", "1"]).expect("parse");
        if let Commands::Vm(ref vm_args) = cli.command {
            assert!(matches!(
                vm_args.action,
                commands::vm::VmCommand::Validate(ref args)
                    if matches!(args.action, commands::validate::ValidateCommand::Run(_))
            ));
        } else {
            panic!("expected Vm");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_validate_run_with_output() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "validate",
            "run",
            "--tier",
            "2",
            "--output",
            "report.json",
        ])
        .expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Validate(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_validate_manifest() {
        let cli =
            Cli::try_parse_from(["vz", "vm", "validate", "manifest"]).expect("parse");
        if let Commands::Vm(ref vm_args) = cli.command {
            assert!(matches!(
                vm_args.action,
                commands::vm::VmCommand::Validate(ref args)
                    if matches!(args.action, commands::validate::ValidateCommand::Manifest(_))
            ));
        } else {
            panic!("expected Vm");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_validate_list() {
        let cli = Cli::try_parse_from(["vz", "vm", "validate", "list"]).expect("parse");
        if let Commands::Vm(ref vm_args) = cli.command {
            assert!(matches!(
                vm_args.action,
                commands::vm::VmCommand::Validate(ref args)
                    if matches!(args.action, commands::validate::ValidateCommand::List(_))
            ));
        } else {
            panic!("expected Vm");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_validate_list_with_tier() {
        let cli = Cli::try_parse_from(["vz", "vm", "validate", "list", "--tier", "2", "--json"])
            .expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Validate(_))
        ));
    }
}
