//! `vz stop` -- Stop a running VM.

use clap::Args;
use tracing::{info, warn};

/// Stop a running VM.
#[derive(Args, Debug)]
pub struct StopArgs {
    /// Name of the VM to stop.
    pub name: String,

    /// Force immediate termination (skip graceful shutdown).
    #[arg(long)]
    pub force: bool,
}

pub async fn run(args: StopArgs) -> anyhow::Result<()> {
    info!(
        name = %args.name,
        force = args.force,
        "stopping VM"
    );

    // Look up VM in registry
    let registry = crate::registry::Registry::load()?;
    let entry = registry
        .get(&args.name)
        .ok_or_else(|| anyhow::anyhow!("VM '{}' not found in registry", args.name))?;

    if !crate::registry::is_pid_alive(entry.pid) {
        // VM process is already dead, just clean up the registry
        let mut registry = crate::registry::Registry::load()?;
        registry.remove(&args.name);
        registry.save()?;
        println!(
            "VM '{}' was already stopped (cleaned up registry)",
            args.name
        );
        return Ok(());
    }

    // Try control socket first for graceful shutdown
    match crate::control::connect(&args.name).await {
        Ok(mut stream) => {
            let request = crate::control::ControlRequest::Stop { force: args.force };
            match crate::control::request(&mut stream, &request).await {
                Ok(crate::control::ControlResponse::Stopped) => {
                    println!("VM '{}' stopped", args.name);
                }
                Ok(crate::control::ControlResponse::Error { message }) => {
                    anyhow::bail!("stop failed: {message}");
                }
                Ok(other) => {
                    anyhow::bail!("unexpected response: {other:?}");
                }
                Err(e) => {
                    warn!(error = %e, "control request failed, falling back to SIGTERM");
                    send_signal(
                        entry.pid,
                        if args.force {
                            libc::SIGKILL
                        } else {
                            libc::SIGTERM
                        },
                    );
                    println!("Signal sent to VM '{}' (PID {})", args.name, entry.pid);
                }
            }
        }
        Err(_) => {
            // No control socket, fall back to signals
            let signal = if args.force {
                libc::SIGKILL
            } else {
                libc::SIGTERM
            };
            send_signal(entry.pid, signal);
            println!(
                "Sent {} to VM '{}' (PID {})",
                if args.force { "SIGKILL" } else { "SIGTERM" },
                args.name,
                entry.pid
            );
        }
    }

    Ok(())
}

fn send_signal(pid: u32, signal: i32) {
    #[allow(unsafe_code)]
    unsafe {
        libc::kill(pid as libc::pid_t, signal);
    }
}
