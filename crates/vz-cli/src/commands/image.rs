//! `vz image` — OCI image management subcommands.

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use vz_runtime_proto::runtime_v2;

use super::runtime_daemon::{connect_control_plane_for_state_db, default_state_db_path};

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

fn pull_auth_overrides_requested(opts: &super::oci::ContainerOpts) -> bool {
    opts.docker_config || opts.username.is_some() || opts.password.is_some()
}

async fn run_pull_stream(args: super::oci::PullArgs) -> anyhow::Result<()> {
    if pull_auth_overrides_requested(&args.opts) {
        bail!("registry auth flags are not supported for daemon-backed `vz image pull` yet");
    }

    let state_db = default_state_db_path();
    let mut client = connect_control_plane_for_state_db(&state_db).await?;
    let mut stream = client
        .pull_image(runtime_v2::PullImageRequest {
            image_ref: args.image.clone(),
            metadata: None,
        })
        .await?;

    let mut completion: Option<runtime_v2::PullImageResponse> = None;
    while let Some(event) = stream.message().await? {
        match event.payload {
            Some(runtime_v2::pull_image_event::Payload::Progress(progress)) => {
                println!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::pull_image_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            None => {}
        }
    }

    let done = completion.context("daemon pull stream ended without terminal completion event")?;
    let image = done
        .image
        .context("daemon pull completion was missing image payload")?;
    println!(
        "Pulled {image_ref} as {digest}",
        image_ref = image.image_ref,
        digest = image.resolved_digest
    );
    if !done.receipt_id.trim().is_empty() {
        println!("Receipt: {}", done.receipt_id);
    }
    Ok(())
}

async fn run_prune_stream(_args: super::oci::PruneArgs) -> anyhow::Result<()> {
    let state_db = default_state_db_path();
    let mut client = connect_control_plane_for_state_db(&state_db).await?;
    let mut stream = client
        .prune_images(runtime_v2::PruneImagesRequest { metadata: None })
        .await?;

    let mut completion: Option<runtime_v2::PruneImagesResponse> = None;
    while let Some(event) = stream.message().await? {
        match event.payload {
            Some(runtime_v2::prune_images_event::Payload::Progress(progress)) => {
                println!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::prune_images_event::Payload::Completion(done)) => {
                completion = Some(done);
            }
            None => {}
        }
    }

    let done = completion.context("daemon prune stream ended without terminal completion event")?;
    println!(
        "Pruned images: refs={refs}, manifests={manifests}, configs={configs}, layer_dirs={layer_dirs}, remaining={remaining}",
        refs = done.removed_refs,
        manifests = done.removed_manifests,
        configs = done.removed_configs,
        layer_dirs = done.removed_layer_dirs,
        remaining = done.remaining_images
    );
    if !done.receipt_id.trim().is_empty() {
        println!("Receipt: {}", done.receipt_id);
    }
    Ok(())
}

/// Run the image subcommand.
pub async fn run(args: ImageArgs) -> anyhow::Result<()> {
    match args.action {
        ImageCommand::Pull(pull_args) => run_pull_stream(pull_args).await,
        #[cfg(target_os = "macos")]
        ImageCommand::Build(_build_args) => {
            bail!(
                "`vz image build` local-runtime path has been removed in daemon-only mode; \
                 use daemon-backed stack/build flows"
            )
        }
        ImageCommand::Ls(_images_args) => {
            let state_db = default_state_db_path();
            let mut client = connect_control_plane_for_state_db(&state_db).await?;
            let response = client
                .list_images(runtime_v2::ListImagesRequest { metadata: None })
                .await?;
            let images = response.images;
            if images.is_empty() {
                println!("No images found.");
                return Ok(());
            }

            println!(
                "{:<40}  {:<14}  {:<18}  REF",
                "DIGEST", "PLATFORM", "REGISTRY"
            );
            for image in images {
                let digest = if image.resolved_digest.len() > 40 {
                    image.resolved_digest[..40].to_string()
                } else {
                    image.resolved_digest.clone()
                };
                println!(
                    "{:<40}  {:<14}  {:<18}  {}",
                    digest, image.platform, image.source_registry, image.image_ref
                );
            }
            Ok(())
        }
        ImageCommand::Prune(prune_args) => run_prune_stream(prune_args).await,
    }
}
