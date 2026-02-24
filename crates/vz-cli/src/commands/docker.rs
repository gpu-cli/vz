//! Docker-compat translation layer over Runtime V2 primitives.

use anyhow::{Result, anyhow};
use clap::{Args, Subcommand};
use vz_runtime_contract::DockerShimCommand;

#[cfg(target_os = "macos")]
use super::build;
use super::oci;

/// Docker-compatible command entrypoint (`vz docker ...`).
#[derive(Args, Debug)]
pub struct DockerArgs {
    #[command(subcommand)]
    pub action: DockerCommand,
}

/// Supported Docker V1 shim commands.
#[derive(Subcommand, Debug)]
pub enum DockerCommand {
    /// Translate `docker run` to Runtime V2 container operations.
    Run(Box<DockerRunArgs>),
    /// Translate `docker exec` to Runtime V2 exec operation.
    Exec(DockerExecArgs),
    /// Translate `docker ps` to Runtime V2 container listing.
    Ps(oci::PsArgs),
    /// Translate `docker logs` to Runtime V2 log retrieval.
    Logs(oci::LogsArgs),
    /// Translate `docker pull` to Runtime V2 image pull.
    Pull(oci::PullArgs),
    /// Translate `docker build` to Runtime V2 build operation.
    #[cfg(target_os = "macos")]
    Build(DockerBuildArgs),
    /// Translate `docker stop` to Runtime V2 stop operation.
    Stop(oci::StopArgs),
    /// Translate `docker rm` to Runtime V2 remove operation.
    Rm(oci::RmArgs),
}

/// Docker-flavored `run` args with strict compatibility guardrails.
#[derive(Args, Debug)]
pub struct DockerRunArgs {
    #[command(flatten)]
    pub inner: oci::RunArgs,

    /// Remove container after command exits (required by shim v1).
    #[arg(long)]
    pub rm: bool,

    /// Docker platform override (unsupported in shim v1).
    #[arg(long)]
    pub platform: Option<String>,
}

/// Docker-flavored `exec` args with strict compatibility guardrails.
#[derive(Args, Debug)]
pub struct DockerExecArgs {
    #[command(flatten)]
    pub inner: oci::ExecArgs,

    /// Docker privileged exec mode (unsupported in shim v1).
    #[arg(long)]
    pub privileged: bool,
}

/// Docker-flavored `build` args with strict compatibility guardrails.
#[cfg(target_os = "macos")]
#[derive(Args, Debug)]
pub struct DockerBuildArgs {
    #[command(flatten)]
    pub inner: build::BuildArgs,

    /// Docker BuildKit SSH mount forwarding (unsupported in shim v1).
    #[arg(long = "ssh", value_name = "SPEC")]
    pub ssh: Vec<String>,
}

/// Dispatch docker-shim commands into Runtime V2 command handlers.
pub async fn run(args: DockerArgs) -> Result<()> {
    match args.action {
        DockerCommand::Run(args) => {
            validate_run_args(&args)?;
            oci::run_container(args.inner).await
        }
        DockerCommand::Exec(args) => {
            validate_exec_args(&args)?;
            oci::run_exec(args.inner).await
        }
        DockerCommand::Ps(args) => oci::run_ps(args).await,
        DockerCommand::Logs(args) => oci::run_logs(args).await,
        DockerCommand::Pull(args) => oci::run_pull(args).await,
        #[cfg(target_os = "macos")]
        DockerCommand::Build(args) => {
            validate_build_args(&args)?;
            build::run(args.inner).await
        }
        DockerCommand::Stop(args) => oci::run_stop(args).await,
        DockerCommand::Rm(args) => oci::run_rm(args).await,
    }
}

fn validate_run_args(args: &DockerRunArgs) -> Result<()> {
    if !args.rm {
        return Err(unsupported_operation(
            DockerShimCommand::Run,
            "requires `--rm` in shim v1",
        ));
    }
    if args.inner.detach {
        return Err(unsupported_flag(
            DockerShimCommand::Run,
            "--detach",
            "foreground execution only in shim v1",
        ));
    }
    if args.platform.is_some() {
        return Err(unsupported_flag(
            DockerShimCommand::Run,
            "--platform",
            "platform selection is backend-managed",
        ));
    }
    Ok(())
}

fn validate_exec_args(args: &DockerExecArgs) -> Result<()> {
    if args.privileged {
        return Err(unsupported_flag(
            DockerShimCommand::Exec,
            "--privileged",
            "privileged exec is not supported",
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn validate_build_args(args: &DockerBuildArgs) -> Result<()> {
    if !args.ssh.is_empty() {
        return Err(unsupported_flag(
            DockerShimCommand::Build,
            "--ssh",
            "ssh forwarding is not supported",
        ));
    }
    Ok(())
}

fn unsupported_operation(command: DockerShimCommand, reason: &str) -> anyhow::Error {
    anyhow!(
        "unsupported_operation: surface=docker; operation={}; reason={}",
        command.as_str(),
        reason,
    )
}

fn unsupported_flag(command: DockerShimCommand, flag: &str, reason: &str) -> anyhow::Error {
    unsupported_operation(
        command,
        &format!("flag `{flag}` is not supported: {reason}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_args() -> DockerRunArgs {
        DockerRunArgs {
            inner: oci::RunArgs {
                image: "alpine:latest".to_string(),
                command: vec!["echo".to_string(), "hi".to_string()],
                env: Vec::new(),
                publish: Vec::new(),
                workdir: None,
                user: None,
                cpus: None,
                memory_mb: None,
                no_network: false,
                timeout_secs: None,
                serial_log_file: None,
                detach: false,
                internal_detached_child: false,
                internal_container_id: None,
                volume: Vec::new(),
                execution_mode: oci::ExecutionModeArg::GuestExec,
                opts: oci::ContainerOpts::default(),
            },
            rm: true,
            platform: None,
        }
    }

    fn exec_args() -> DockerExecArgs {
        DockerExecArgs {
            inner: oci::ExecArgs {
                id: "ctr-1".to_string(),
                command: vec!["true".to_string()],
                env: Vec::new(),
                workdir: None,
                user: None,
                timeout_secs: None,
                opts: oci::ContainerOpts::default(),
            },
            privileged: false,
        }
    }

    #[test]
    fn validate_run_requires_rm() {
        let mut args = run_args();
        args.rm = false;
        let err = validate_run_args(&args).unwrap_err();
        assert!(err.to_string().contains("unsupported_operation"));
        assert!(err.to_string().contains("requires `--rm`"));
    }

    #[test]
    fn validate_run_rejects_platform_flag() {
        let mut args = run_args();
        args.platform = Some("linux/amd64".to_string());
        let err = validate_run_args(&args).unwrap_err();
        assert!(err.to_string().contains("--platform"));
    }

    #[test]
    fn validate_exec_rejects_privileged_flag() {
        let mut args = exec_args();
        args.privileged = true;
        let err = validate_exec_args(&args).unwrap_err();
        assert!(err.to_string().contains("--privileged"));
    }

    #[test]
    fn unsupported_operation_message_shape_is_stable() {
        let err = unsupported_operation(DockerShimCommand::Run, "requires `--rm` in shim v1");
        let message = err.to_string();
        assert!(message.starts_with("unsupported_operation:"));
        assert!(message.contains("surface=docker"));
        assert!(message.contains("operation=run"));
    }
}
