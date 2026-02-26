//! `vz image` — OCI image management subcommands.

use clap::{Args, Subcommand};

/// Manage OCI images.
#[derive(Args, Debug)]
pub struct ImageArgs {
    #[command(subcommand)]
    pub action: ImageCommand,
}

#[derive(Subcommand, Debug)]
pub enum ImageCommand {
    /// Pull and cache an OCI image locally.
    Pull(super::oci::PullArgs),

    /// Build a Dockerfile into the local vz image store.
    #[cfg(target_os = "macos")]
    Build(Box<super::build::BuildArgs>),

    /// List cached OCI images.
    Ls(super::oci::ImagesArgs),

    /// Remove stale image and layer artifacts.
    Prune(super::oci::PruneArgs),
}

/// Run the image subcommand.
pub async fn run(args: ImageArgs) -> anyhow::Result<()> {
    match args.action {
        ImageCommand::Pull(pull_args) => super::oci::run_pull(pull_args).await,
        #[cfg(target_os = "macos")]
        ImageCommand::Build(build_args) => super::build::run(*build_args).await,
        ImageCommand::Ls(images_args) => super::oci::run_images(images_args).await,
        ImageCommand::Prune(prune_args) => super::oci::run_prune(prune_args).await,
    }
}
