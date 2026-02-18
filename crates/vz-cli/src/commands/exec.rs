//! `vz exec` -- Execute a command inside a running VM.

use clap::Args;
use tracing::info;

/// Execute a command inside a running VM via the guest agent.
#[derive(Args, Debug)]
pub struct ExecArgs {
    /// Name of the VM to execute in.
    pub name: String,

    /// Command and arguments to execute.
    #[arg(last = true, required = true)]
    pub command: Vec<String>,

    /// Run as this user inside the VM (default: "dev").
    #[arg(long, default_value = "dev")]
    pub user: String,

    /// Working directory inside the VM.
    #[arg(long)]
    pub workdir: Option<String>,
}

pub async fn run(args: ExecArgs) -> anyhow::Result<()> {
    info!(
        name = %args.name,
        command = ?args.command,
        user = %args.user,
        "executing command in VM"
    );

    // Look up VM in registry to verify it's running
    let registry = crate::registry::Registry::load()?;
    let entry = registry.get(&args.name).ok_or_else(|| {
        anyhow::anyhow!("VM '{}' not found in registry. Is it running?", args.name)
    })?;

    if !crate::registry::is_pid_alive(entry.pid) {
        anyhow::bail!(
            "VM '{}' appears to have crashed (PID {} is not running).\n\
             Run `vz cleanup` to clean up stale entries.",
            args.name,
            entry.pid
        );
    }

    // Connect to control socket
    let mut stream = crate::control::connect(&args.name).await?;

    // Build and send exec request
    let request = crate::control::ControlRequest::Exec {
        command: args.command.clone(),
        user: Some(args.user.clone()),
        workdir: args.workdir.clone(),
    };

    let response = crate::control::request(&mut stream, &request).await?;

    match response {
        crate::control::ControlResponse::ExecResult {
            exit_code,
            stdout,
            stderr,
        } => {
            if !stdout.is_empty() {
                print!("{stdout}");
            }
            if !stderr.is_empty() {
                eprint!("{stderr}");
            }
            if exit_code != 0 {
                std::process::exit(exit_code);
            }
            Ok(())
        }
        crate::control::ControlResponse::Error { message } => {
            anyhow::bail!("exec failed: {message}");
        }
        other => {
            anyhow::bail!("unexpected response: {other:?}");
        }
    }
}
