//! `vz list` -- List running VMs.

use clap::Args;
use tracing::info;

use crate::registry;

/// List running VMs.
#[derive(Args, Debug)]
pub struct ListArgs {}

pub async fn run(_args: ListArgs) -> anyhow::Result<()> {
    let reg = registry::Registry::load()?;
    let entries = reg.entries();

    if entries.is_empty() {
        info!("no VMs registered");
        return Ok(());
    }

    // Table header
    println!(
        "{:<15} {:<10} {:<5} {:<8} {:<6}",
        "NAME", "STATE", "CPUS", "MEMORY", "PID"
    );
    println!("{}", "-".repeat(50));

    for (name, entry) in entries {
        let state = if registry::is_pid_alive(entry.pid) {
            &entry.state
        } else {
            "orphaned"
        };

        println!(
            "{:<15} {:<10} {:<5} {:<8} {:<6}",
            name,
            state,
            entry.cpus.unwrap_or(0),
            entry.memory_gb.map(|m| format!("{m}G")).unwrap_or_default(),
            entry.pid,
        );
    }

    Ok(())
}
