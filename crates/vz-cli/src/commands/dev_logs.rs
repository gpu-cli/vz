//! `vz logs` — show daemon and VM logs for debugging.

use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::Args;

use super::runtime_daemon::default_state_db_path;

/// Show daemon logs for debugging.
#[derive(Args, Debug)]
pub struct DevLogsArgs {
    /// Number of lines to show (default: 50, use 0 for all).
    #[arg(short = 'n', long, default_value_t = 50)]
    pub lines: usize,

    /// Follow log output (like tail -f).
    #[arg(short, long)]
    pub follow: bool,
}

pub async fn cmd_dev_logs(args: DevLogsArgs) -> anyhow::Result<()> {
    let log_path = resolve_log_path()?;

    if !log_path.exists() {
        // Check if the daemon socket exists — if so, the daemon was started
        // before log-file support was added.
        let socket_path = log_path.with_extension("sock");
        if socket_path.exists() {
            bail!(
                "no log file found (daemon was started before log support).\n\
                 Restart the daemon to enable logging:\n\
                 \n  vz stop && vz run <command>"
            );
        }
        bail!(
            "no daemon log file found at {}\n\
             The daemon has not been started yet. Run `vz run` first.",
            log_path.display()
        );
    }

    if args.follow {
        let status = std::process::Command::new("tail")
            .arg("-f")
            .arg("-n")
            .arg(args.lines.to_string())
            .arg(&log_path)
            .status()
            .context("failed to run tail -f")?;

        if !status.success() {
            bail!("tail exited with status {status}");
        }
    } else {
        let content =
            std::fs::read_to_string(&log_path).context("failed to read daemon log file")?;

        let output_lines: Vec<&str> = content.lines().collect();
        let start = if args.lines == 0 || args.lines >= output_lines.len() {
            0
        } else {
            output_lines.len() - args.lines
        };

        for line in &output_lines[start..] {
            println!("{line}");
        }
    }

    Ok(())
}

fn resolve_log_path() -> anyhow::Result<PathBuf> {
    let state_db = default_state_db_path();
    // Log file lives next to the socket: <state_db_parent>/.vz-runtime/runtimed.log
    let runtime_dir = state_db
        .parent()
        .map(|p| p.join(".vz-runtime"))
        .unwrap_or_else(|| PathBuf::from(".vz-runtime"));
    Ok(runtime_dir.join("runtimed.log"))
}
