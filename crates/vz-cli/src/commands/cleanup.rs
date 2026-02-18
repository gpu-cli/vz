//! `vz cleanup` -- Detect and clean up orphaned VMs.

use clap::Args;
use tracing::{info, warn};

use crate::registry;

/// Detect and clean up orphaned VMs.
#[derive(Args, Debug)]
pub struct CleanupArgs {}

pub async fn run(_args: CleanupArgs) -> anyhow::Result<()> {
    let mut reg = registry::Registry::load()?;
    let orphaned: Vec<String> = reg
        .entries()
        .iter()
        .filter(|(_, entry)| entry.state == "running" && !registry::is_pid_alive(entry.pid))
        .map(|(name, _)| name.clone())
        .collect();

    if orphaned.is_empty() {
        info!("no orphaned VMs found");
        println!("No orphaned VMs found.");
        return Ok(());
    }

    println!("Found {} orphaned VM(s):", orphaned.len());

    for name in &orphaned {
        warn!(name, "cleaning up orphaned VM");
        println!("  Removing: {name}");
        reg.remove(name);
    }

    reg.save()?;

    // Clean up stale PID/lock/socket files
    let run_dir = registry::vz_home().join("run");
    for name in &orphaned {
        let pid_file = run_dir.join(format!("{name}.pid"));
        let lock_file = run_dir.join(format!("{name}.lock"));
        let sock_file = run_dir.join(format!("{name}.sock"));
        if pid_file.exists() {
            let _ = std::fs::remove_file(&pid_file);
        }
        if lock_file.exists() {
            let _ = std::fs::remove_file(&lock_file);
        }
        if sock_file.exists() {
            let _ = std::fs::remove_file(&sock_file);
        }
    }

    println!("Cleanup complete.");
    Ok(())
}
