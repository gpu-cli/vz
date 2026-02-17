//! vz — macOS VM sandbox CLI.
//!
//! Commands:
//!   vz init                    Download IPSW and create a golden macOS image
//!   vz run                     Start a VM with project mounts
//!   vz exec <name> -- <cmd>    Execute a command inside a running VM
//!   vz save <name>             Save VM state for fast restore
//!   vz restore <name>          Restore VM from saved state
//!   vz list                    List running VMs
//!   vz stop <name>             Stop a running VM

fn main() {
    // TODO: Phase 3 — implement CLI with clap
    eprintln!("vz CLI — not yet implemented. See planning/README.md for the design.");
    std::process::exit(1);
}
