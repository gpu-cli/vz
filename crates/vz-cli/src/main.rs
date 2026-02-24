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

    /// Build a Dockerfile into the local vz image store.
    #[cfg(target_os = "macos")]
    Build(commands::build::BuildArgs),

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

    // Suppress info-level tracing noise for stack up/down — the StackOutput
    // abstraction handles user-facing progress display instead.
    let is_stack_progress = matches!(
        cli.command,
        Commands::Stack(ref args) if matches!(
            args.action,
            commands::stack::StackCommand::Up(_)
            | commands::stack::StackCommand::Down(_)
        )
    );

    let filter = if cli.quiet {
        "error"
    } else if is_stack_progress && cli.verbose == 0 {
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
            #[cfg(target_os = "macos")]
            Commands::Build(args) => commands::build::run(args).await,
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
    use std::path::PathBuf;

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
        let cli = Cli::try_parse_from(["vz", "vm", "init", "--disk-size", "64G"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Init(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_init_with_pinned_base() {
        let cli =
            Cli::try_parse_from(["vz", "vm", "init", "--base", "macos-15.3.1-24D70-arm64-64g"])
                .expect("parse");

        match cli.command {
            Commands::Vm(args) => match args.action {
                commands::vm::VmCommand::Init(init) => {
                    assert_eq!(init.base.as_deref(), Some("macos-15.3.1-24D70-arm64-64g"));
                    assert!(!init.allow_unpinned);
                }
                other => panic!("unexpected vm command: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_init_with_channel_alias() {
        let cli = Cli::try_parse_from(["vz", "vm", "init", "--base", "stable"]).expect("parse");

        match cli.command {
            Commands::Vm(args) => match args.action {
                commands::vm::VmCommand::Init(init) => {
                    assert_eq!(init.base.as_deref(), Some("stable"));
                    assert!(!init.allow_unpinned);
                }
                other => panic!("unexpected vm command: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_init_with_previous_channel_alias() {
        let cli = Cli::try_parse_from(["vz", "vm", "init", "--base", "previous"]).expect("parse");

        match cli.command {
            Commands::Vm(args) => match args.action {
                commands::vm::VmCommand::Init(init) => {
                    assert_eq!(init.base.as_deref(), Some("previous"));
                    assert!(!init.allow_unpinned);
                }
                other => panic!("unexpected vm command: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_init_unpinned_escape_hatch() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "init",
            "--allow-unpinned",
            "--ipsw",
            "/tmp/restore.ipsw",
        ])
        .expect("parse");

        match cli.command {
            Commands::Vm(args) => match args.action {
                commands::vm::VmCommand::Init(init) => {
                    assert!(init.allow_unpinned);
                    assert_eq!(init.ipsw, Some("/tmp/restore.ipsw".into()));
                }
                other => panic!("unexpected vm command: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_init_rejects_base_and_ipsw_together() {
        let err = Cli::try_parse_from([
            "vz",
            "vm",
            "init",
            "--base",
            "macos-15.3.1-24D70-arm64-64g",
            "--ipsw",
            "/tmp/restore.ipsw",
        ])
        .expect_err("expected clap conflict");
        let msg = err.to_string();
        assert!(msg.contains("--base"));
        assert!(msg.contains("--ipsw"));
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

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_build_subcommand() {
        let cli = Cli::try_parse_from([
            "vz",
            "build",
            "-t",
            "demo:latest",
            "-f",
            "Dockerfile.dev",
            "--build-arg",
            "A=1",
            ".",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Commands::Build(_)));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_build_cache_du_subcommand() {
        let cli = Cli::try_parse_from(["vz", "build", "cache", "du"]).expect("parse");
        assert!(matches!(cli.command, Commands::Build(_)));
    }

    #[test]
    fn parse_logs_subcommand() {
        let cli = Cli::try_parse_from(["vz", "logs", "ctr-123"]).expect("parse");
        assert!(matches!(cli.command, Commands::Logs(_)));
    }

    #[test]
    fn parse_logs_with_follow_and_tail() {
        let cli = Cli::try_parse_from(["vz", "logs", "ctr-123", "--follow", "--tail", "50"])
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
        let cli = Cli::try_parse_from(["vz", "run", "alpine:latest", "--", "echo", "hello"])
            .expect("parse");
        assert!(matches!(cli.command, Commands::Run(_)));
    }

    #[test]
    fn parse_run_with_publish_flag() {
        let cli = Cli::try_parse_from(["vz", "run", "nginx:alpine", "--publish", "8080:80"])
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
        let cli = Cli::try_parse_from(["vz", "vm", "stop", "my-vm", "--force"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Stop(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_rm_subcommand() {
        let cli = Cli::try_parse_from(["vz", "vm", "rm", "my-vm", "--force", "--delete-image"])
            .expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Rm(_))
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
        let cli = Cli::try_parse_from(["vz", "vm", "cache", "clean", "--all"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args) if matches!(args.action, commands::vm::VmCommand::Cache(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_base_list() {
        let cli = Cli::try_parse_from(["vz", "vm", "base", "list"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Vm(ref args)
                if matches!(
                    args.action,
                    commands::vm::VmCommand::Base(ref base_args)
                        if matches!(base_args.action, commands::vm_base::VmBaseCommand::List)
                )
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_base_verify() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "base",
            "verify",
            "--image",
            "/tmp/base.img",
            "--base-id",
            "macos-15.3.1-24D70-arm64-64g",
        ])
        .expect("parse");

        match cli.command {
            Commands::Vm(args) => match args.action {
                commands::vm::VmCommand::Base(base_args) => match base_args.action {
                    commands::vm_base::VmBaseCommand::Verify(verify) => {
                        assert_eq!(verify.image, PathBuf::from("/tmp/base.img"));
                        assert_eq!(verify.base_id, "macos-15.3.1-24D70-arm64-64g");
                    }
                    other => panic!("unexpected vm base action: {other:?}"),
                },
                other => panic!("unexpected vm command: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_base_verify_with_channel_alias() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "base",
            "verify",
            "--image",
            "/tmp/base.img",
            "--base-id",
            "previous",
        ])
        .expect("parse");

        match cli.command {
            Commands::Vm(args) => match args.action {
                commands::vm::VmCommand::Base(base_args) => match base_args.action {
                    commands::vm_base::VmBaseCommand::Verify(verify) => {
                        assert_eq!(verify.image, PathBuf::from("/tmp/base.img"));
                        assert_eq!(verify.base_id, "previous");
                    }
                    other => panic!("unexpected vm base action: {other:?}"),
                },
                other => panic!("unexpected vm command: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_patch_create() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "patch",
            "create",
            "--bundle",
            "/tmp/patch-bundle.vzpatch",
            "--base-id",
            "stable",
            "--operations",
            "/tmp/operations.json",
            "--payload-dir",
            "/tmp/payload",
            "--signing-key",
            "/tmp/signing-key.pem",
        ])
        .expect("parse");

        match cli.command {
            Commands::Vm(args) => match args.action {
                commands::vm::VmCommand::Patch(patch_args) => match patch_args.action {
                    commands::vm_patch::VmPatchCommand::Create(create) => {
                        assert_eq!(create.bundle, PathBuf::from("/tmp/patch-bundle.vzpatch"));
                        assert_eq!(create.base_id, "stable");
                        assert_eq!(create.operations, PathBuf::from("/tmp/operations.json"));
                        assert_eq!(create.payload_dir, PathBuf::from("/tmp/payload"));
                        assert_eq!(create.signing_key, PathBuf::from("/tmp/signing-key.pem"));
                        assert_eq!(create.patch_version, "1.0.0");
                        assert!(create.post_state_hashes.is_none());
                        assert!(create.bundle_id.is_none());
                        assert!(create.created_at.is_none());
                    }
                    other => panic!("unexpected vm patch action: {other:?}"),
                },
                other => panic!("unexpected vm command: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_patch_verify() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "patch",
            "verify",
            "--bundle",
            "/tmp/patch-bundle.vzpatch",
        ])
        .expect("parse");

        match cli.command {
            Commands::Vm(args) => match args.action {
                commands::vm::VmCommand::Patch(patch_args) => match patch_args.action {
                    commands::vm_patch::VmPatchCommand::Verify(verify) => {
                        assert_eq!(verify.bundle, PathBuf::from("/tmp/patch-bundle.vzpatch"));
                    }
                    other => panic!("unexpected vm patch action: {other:?}"),
                },
                other => panic!("unexpected vm command: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_patch_apply() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "patch",
            "apply",
            "--bundle",
            "/tmp/patch-bundle.vzpatch",
            "--root",
            "/tmp/mounted-root",
        ])
        .expect("parse");

        match cli.command {
            Commands::Vm(args) => match args.action {
                commands::vm::VmCommand::Patch(patch_args) => match patch_args.action {
                    commands::vm_patch::VmPatchCommand::Apply(apply) => {
                        assert_eq!(apply.bundle, PathBuf::from("/tmp/patch-bundle.vzpatch"));
                        assert_eq!(apply.root, PathBuf::from("/tmp/mounted-root"));
                    }
                    other => panic!("unexpected vm patch action: {other:?}"),
                },
                other => panic!("unexpected vm command: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
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

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_provision_user_agent_mode() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "provision",
            "--image",
            "base.img",
            "--base-id",
            "macos-15.3.1-24D70-arm64-64g",
            "--agent-mode",
            "user",
        ])
        .expect("parse");

        if let Commands::Vm(ref vm_args) = cli.command {
            if let commands::vm::VmCommand::Provision(ref provision) = vm_args.action {
                assert_eq!(
                    provision.base_id.as_deref(),
                    Some("macos-15.3.1-24D70-arm64-64g")
                );
                assert!(!provision.allow_unpinned);
                assert!(matches!(
                    provision.agent_mode,
                    commands::provision::AgentModeArg::User
                ));
            } else {
                panic!("expected Provision");
            }
        } else {
            panic!("expected Vm");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_provision_with_channel_alias() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "provision",
            "--image",
            "base.img",
            "--base-id",
            "stable",
        ])
        .expect("parse");

        if let Commands::Vm(ref vm_args) = cli.command {
            if let commands::vm::VmCommand::Provision(ref provision) = vm_args.action {
                assert_eq!(provision.base_id.as_deref(), Some("stable"));
                assert!(!provision.allow_unpinned);
                assert!(matches!(
                    provision.agent_mode,
                    commands::provision::AgentModeArg::System
                ));
            } else {
                panic!("expected Provision");
            }
        } else {
            panic!("expected Vm");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_provision_defaults_to_system_agent_mode() {
        let cli =
            Cli::try_parse_from(["vz", "vm", "provision", "--image", "base.img"]).expect("parse");

        if let Commands::Vm(ref vm_args) = cli.command {
            if let commands::vm::VmCommand::Provision(ref provision) = vm_args.action {
                assert_eq!(provision.base_id, None);
                assert!(!provision.allow_unpinned);
                assert!(matches!(
                    provision.agent_mode,
                    commands::provision::AgentModeArg::System
                ));
            } else {
                panic!("expected Provision");
            }
        } else {
            panic!("expected Vm");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_provision_unpinned_escape_hatch() {
        let cli = Cli::try_parse_from([
            "vz",
            "vm",
            "provision",
            "--image",
            "base.img",
            "--allow-unpinned",
        ])
        .expect("parse");

        if let Commands::Vm(ref vm_args) = cli.command {
            if let commands::vm::VmCommand::Provision(ref provision) = vm_args.action {
                assert_eq!(provision.base_id, None);
                assert!(provision.allow_unpinned);
            } else {
                panic!("expected Provision");
            }
        } else {
            panic!("expected Vm");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_vm_provision_rejects_base_id_with_allow_unpinned() {
        let err = Cli::try_parse_from([
            "vz",
            "vm",
            "provision",
            "--image",
            "base.img",
            "--base-id",
            "macos-15.3.1-24D70-arm64-64g",
            "--allow-unpinned",
        ])
        .expect_err("expected clap conflict");
        let msg = err.to_string();
        assert!(msg.contains("--base-id"));
        assert!(msg.contains("--allow-unpinned"));
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

    #[test]
    fn parse_stack_dashboard() {
        let cli = Cli::try_parse_from(["vz", "stack", "dashboard", "myapp"]).expect("parse");
        assert!(matches!(
            cli.command,
            Commands::Stack(ref args)
                if matches!(args.action, commands::stack::StackCommand::Dashboard(_))
        ));
    }

    #[test]
    fn parse_stack_dashboard_with_file() {
        let cli = Cli::try_parse_from(["vz", "stack", "dashboard", "myapp", "-f", "compose.yaml"])
            .expect("parse");
        if let Commands::Stack(ref args) = cli.command {
            if let commands::stack::StackCommand::Dashboard(ref d) = args.action {
                assert_eq!(d.name, "myapp");
                assert_eq!(
                    d.file.as_deref(),
                    Some(std::path::Path::new("compose.yaml"))
                );
            } else {
                panic!("expected Dashboard");
            }
        } else {
            panic!("expected Stack");
        }
    }

    #[test]
    fn parse_stack_up_no_tui() {
        let cli = Cli::try_parse_from(["vz", "stack", "up", "--file", "compose.yaml", "--no-tui"])
            .expect("parse");
        if let Commands::Stack(ref args) = cli.command {
            if let commands::stack::StackCommand::Up(ref up) = args.action {
                assert!(up.no_tui);
            } else {
                panic!("expected Up");
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
        let cli = Cli::try_parse_from(["vz", "vm", "validate", "manifest"]).expect("parse");
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
