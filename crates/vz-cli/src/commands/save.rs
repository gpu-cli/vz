//! `vz save` -- Save VM state for fast restore.

use std::path::PathBuf;

use clap::Args;
use tracing::info;

/// Save VM state for fast restore.
#[derive(Args, Debug)]
pub struct SaveArgs {
    /// Name of the VM to save.
    pub name: String,

    /// Output path for the state file.
    #[arg(long)]
    pub output: Option<PathBuf>,

    /// Stop the VM after saving (instead of resuming).
    #[arg(long)]
    pub stop: bool,
}

pub async fn run(args: SaveArgs) -> anyhow::Result<()> {
    info!(
        name = %args.name,
        output = ?args.output,
        stop_after = args.stop,
        "saving VM state"
    );

    // Look up VM
    let registry = crate::registry::Registry::load()?;
    let entry = registry
        .get(&args.name)
        .ok_or_else(|| anyhow::anyhow!("VM '{}' not found in registry", args.name))?;

    if !crate::registry::is_pid_alive(entry.pid) {
        anyhow::bail!("VM '{}' is not running (PID {} dead)", args.name, entry.pid);
    }

    // Determine output path
    let save_path = args.output.unwrap_or_else(|| {
        let image = PathBuf::from(&entry.image);
        image.with_extension("state")
    });

    // Connect to control socket
    let mut stream = crate::control::connect(&args.name).await?;

    let request = crate::control::ControlRequest::Save {
        path: save_path.to_string_lossy().to_string(),
        stop_after: args.stop,
    };

    let response = crate::control::request(&mut stream, &request).await?;

    match response {
        crate::control::ControlResponse::SaveComplete { path } => {
            println!("VM state saved to: {path}");
            if args.stop {
                println!("VM stopped after save.");
            }
            Ok(())
        }
        crate::control::ControlResponse::Error { message } => {
            anyhow::bail!("save failed: {message}");
        }
        other => {
            anyhow::bail!("unexpected response: {other:?}");
        }
    }
}
