//! `vz restore` -- Restore VM from saved state.

use std::path::PathBuf;

use clap::Args;
use tracing::info;

/// Restore VM from saved state.
#[derive(Args, Debug)]
pub struct RestoreArgs {
    /// Path to the saved state file.
    #[arg(long)]
    pub state: PathBuf,

    /// Path to the disk image.
    #[arg(long)]
    pub image: PathBuf,

    /// VirtioFS mount in tag:host-path format. Can be repeated.
    #[arg(long, value_name = "TAG:PATH")]
    pub mount: Vec<String>,

    /// VM name for registry tracking.
    #[arg(long)]
    pub name: Option<String>,
}

pub async fn run(args: RestoreArgs) -> anyhow::Result<()> {
    let name = args.name.clone().unwrap_or_else(|| "default".to_string());

    info!(
        name = %name,
        state = %args.state.display(),
        image = %args.image.display(),
        "restoring VM from state"
    );

    // Delegate to `vz run --restore <state>`
    let run_args = super::run::RunArgs {
        image: args.image,
        mount: args.mount,
        cpus: 4,
        memory: 8,
        headless: true,
        restore: Some(args.state),
        name: Some(name),
    };

    super::run::run(run_args).await
}
