//! `vz image` — OCI image management subcommands.

use std::path::Path;

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClient;

use super::runtime_daemon::{daemon_client_config, default_state_db_path};

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

async fn connect_image_daemon(
    state_db: &Path,
    auto_spawn_override: Option<bool>,
) -> anyhow::Result<DaemonClient> {
    let mut config = daemon_client_config(state_db)?;
    if let Some(auto_spawn) = auto_spawn_override {
        config.auto_spawn = auto_spawn;
    }

    DaemonClient::connect_with_config(config)
        .await
        .with_context(|| {
            format!(
                "failed to connect to vz-runtimed for state db {}",
                state_db.display()
            )
        })
}

async fn run_pull_stream_for_state_db(
    args: super::oci::PullArgs,
    state_db: &Path,
    auto_spawn_override: Option<bool>,
) -> anyhow::Result<()> {
    if pull_auth_overrides_requested(&args.opts) {
        bail!("registry auth flags are not supported for daemon-backed `vz image pull` yet");
    }

    let mut client = connect_image_daemon(state_db, auto_spawn_override).await?;
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

pub(crate) async fn run_pull_stream(args: super::oci::PullArgs) -> anyhow::Result<()> {
    let state_db = default_state_db_path();
    run_pull_stream_for_state_db(args, &state_db, None).await
}

async fn run_prune_stream_for_state_db(
    _args: super::oci::PruneArgs,
    state_db: &Path,
    auto_spawn_override: Option<bool>,
) -> anyhow::Result<()> {
    let mut client = connect_image_daemon(state_db, auto_spawn_override).await?;
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

async fn run_prune_stream(args: super::oci::PruneArgs) -> anyhow::Result<()> {
    let state_db = default_state_db_path();
    run_prune_stream_for_state_db(args, &state_db, None).await
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
            let mut client = connect_image_daemon(&state_db, None).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::oci::{ContainerOpts, PruneArgs, PullArgs};

    #[tokio::test]
    async fn pull_stream_reports_unreachable_daemon_when_autostart_disabled() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let state_db = tmp.path().join("state").join("stack-state.db");
        let error = run_pull_stream_for_state_db(
            PullArgs {
                image: "alpine:3.20".to_string(),
                opts: ContainerOpts::default(),
            },
            &state_db,
            Some(false),
        )
        .await
        .expect_err("missing daemon socket should fail");

        assert!(
            error
                .to_string()
                .contains("failed to connect to vz-runtimed for state db"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn prune_stream_reports_unreachable_daemon_when_autostart_disabled() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let state_db = tmp.path().join("state").join("stack-state.db");
        let error = run_prune_stream_for_state_db(
            PruneArgs {
                opts: ContainerOpts::default(),
            },
            &state_db,
            Some(false),
        )
        .await
        .expect_err("missing daemon socket should fail");

        assert!(
            error
                .to_string()
                .contains("failed to connect to vz-runtimed for state db"),
            "unexpected error: {error:#}"
        );
    }
}
