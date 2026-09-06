//! Exact macOS base/patch preparation, independent of catalog and VM lifecycle.
//!
//! [`BootstrapCache::prepare`] accepts a manifest pin from an authenticated catalog.
//! Persist that pin in Environment state first: this layer never follows `latest`.
//! Ready means verified immutable template files, **not** a booted native Machine.
//! The caller owns a private cache root and must keep templates immutable; mutable
//! Machines receive separate copy-on-write disks and private platform state.

use std::fs::{self, File};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::artifact_cache::{self, Artifact, ArtifactCache, preparation_lock, private_directory};
use crate::image_delta;

mod manifest;
mod template;
pub use manifest::{ImageIdentity, Platform, ReleaseManifest};
pub use template::PreparedTemplate;

const MAX_MANIFEST_BYTES: u64 = 64 * 1024;

/// Artifact being acquired. Names are stable for structured progress consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Component {
    /// Authenticated release manifest.
    Manifest,
    /// Exact base disk.
    Base,
    /// Matching disk patch.
    Patch,
    /// Serialized hardware model.
    HardwareModel,
    /// Immutable auxiliary-storage seed.
    AuxiliaryStorageSeed,
}

/// Operation events; nested progress retains units (patch application uses
/// chunks, downloads and other delta phases use bytes). A ready template event
/// does not assert that a guest agent or a Machine is ready.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum Progress {
    /// Download/cache verification progress for one exact artifact.
    Artifact {
        /// Artifact role.
        component: Component,
        /// Per-artifact progress and phase.
        progress: artifact_cache::Progress,
    },
    /// Another operation holds the template preparation lock.
    WaitingForTemplate,
    /// Disk preparation progress.
    PreparingImage {
        /// Per-delta progress and phase.
        progress: image_delta::Progress,
    },
    /// Copy and verify immutable native platform resources.
    PreparingPlatform {
        /// Platform artifact role.
        component: Component,
        /// Bytes copied and hashed.
        completed: u64,
        /// Pinned artifact size.
        total: u64,
    },
    /// Validate and publish the completed template receipt.
    PublishingTemplate,
    /// The immutable template is available.
    TemplateReady {
        /// True only when preparation reused an existing completion receipt.
        reused: bool,
    },
}

/// Cache of downloads and verified immutable templates, keyed by manifest digest.
/// No Machine identity, mutable runtime disk, or Environment state is stored here.
pub struct BootstrapCache {
    downloads: ArtifactCache,
    templates: PathBuf,
}

impl BootstrapCache {
    /// Open an absolute caller-owned 0700 root with no symlink ancestry. Its
    /// parent must already exist. Separate mutable Machine storage from this root.
    pub fn new(root: PathBuf) -> Result<Self> {
        private_directory(&root)?;
        let downloads = ArtifactCache::new(root.join("downloads"))?;
        let templates = root.join("templates");
        private_directory(&templates)?;
        Ok(Self {
            downloads,
            templates,
        })
    }

    /// Prepare an exact authenticated release once. Concurrent callers share
    /// per-manifest work; valid prepared hits never read/hash the large blobs.
    ///
    /// The pin must come from a trusted catalog, not an untrusted project URL.
    /// The small manifest is reverified locally on reuse. Immutable image stamps
    /// detect ordinary modification/replacement without hashing entire disks.
    /// This trusts the owning account and storage, not a hostile same-UID writer.
    ///
    /// Returning an error from `progress` cancels and drains a disk worker before
    /// returning. Dropping this future closes its bounded event channel; the disk
    /// worker cancels at its next checkpoint and removes unpublished staging.
    /// It may finish an already committed valid template, never a partial one.
    pub async fn prepare(
        &self,
        manifest_pin: &Artifact,
        progress: impl FnMut(Progress) -> Result<()>,
    ) -> Result<PreparedTemplate> {
        self.prepare_source(manifest_pin, None, progress).await
    }

    /// Prepare from an explicitly trusted installation bundle. Each input is
    /// named by its SHA-256 within this directory and independently verified.
    /// Existing completed templates need only their small manifest and receipt.
    pub async fn prepare_installed(
        &self,
        manifest_pin: &Artifact,
        bundle: &std::path::Path,
        progress: impl FnMut(Progress) -> Result<()>,
    ) -> Result<PreparedTemplate> {
        self.prepare_source(manifest_pin, Some(bundle), progress)
            .await
    }

    async fn acquire(
        &self,
        pin: &Artifact,
        bundle: Option<&std::path::Path>,
        progress: impl FnMut(artifact_cache::Progress) -> Result<()>,
    ) -> Result<PathBuf> {
        if let Some(bundle) = bundle {
            self.downloads
                .ensure_installed(pin, &bundle.join(&pin.sha256), progress)
                .await
        } else {
            self.downloads.ensure(pin, progress).await
        }
    }

    async fn prepare_source(
        &self,
        manifest_pin: &Artifact,
        bundle: Option<&std::path::Path>,
        mut progress: impl FnMut(Progress) -> Result<()>,
    ) -> Result<PreparedTemplate> {
        manifest_pin.validate()?;
        ensure!(
            manifest_pin.size_bytes <= MAX_MANIFEST_BYTES,
            "manifest exceeds size limit"
        );
        let manifest_path = self
            .acquire(manifest_pin, bundle, |p| {
                progress(Progress::Artifact {
                    component: Component::Manifest,
                    progress: p,
                })
            })
            .await?;
        let bytes = template::read_small(&manifest_path, MAX_MANIFEST_BYTES)?;
        // Recheck the bytes being parsed, rather than relying on a pathname check.
        template::verify_bytes(&bytes, &manifest_pin.sha256)?;
        let manifest: ReleaseManifest =
            serde_json::from_slice(&bytes).context("parse bootstrap manifest")?;
        manifest.validate()?;
        let key = &manifest_pin.sha256;
        let lock = preparation_lock(&self.templates.join(format!("{key}.lock")))?;
        loop {
            match lock.try_lock_exclusive() {
                Ok(()) => break,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    progress(Progress::WaitingForTemplate)?;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => return Err(e).context("lock template preparation"),
            }
        }
        let destination = self.templates.join(key);
        match fs::symlink_metadata(&destination) {
            Ok(_) => {
                let ready = PreparedTemplate::load(destination, key, manifest)?;
                progress(Progress::TemplateReady { reused: true })?;
                return Ok(ready);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).context("inspect prepared template"),
        }
        // Leftover staging from a killed process is never considered ready.
        // The lock guarantees no live operation still owns this release's stage.
        let staging = self.templates.join(format!("{key}.staging"));
        template::remove_stale_stage(&staging)?;
        let mut inputs = Vec::new();
        for (component, artifact) in [
            (Component::Base, &manifest.base),
            (Component::Patch, &manifest.patch),
            (Component::HardwareModel, &manifest.platform.hardware_model),
            (
                Component::AuxiliaryStorageSeed,
                &manifest.platform.auxiliary_storage_seed,
            ),
        ] {
            inputs.push(
                self.acquire(artifact, bundle, |p| {
                    progress(Progress::Artifact {
                        component,
                        progress: p,
                    })
                })
                .await?,
            );
        }
        let [base, patch, hardware, auxiliary]: [PathBuf; 4] = inputs
            .try_into()
            .map_err(|_| anyhow::anyhow!("missing bootstrap inputs"))?;
        let key = key.clone();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let worker = tokio::task::spawn_blocking(move || {
            // Keep the template lock in the worker even when its observer drops.
            let _lock: File = lock;
            template::build(
                &staging,
                destination,
                &key,
                manifest,
                &bytes,
                [&base, &patch, &hardware, &auxiliary],
                |p| {
                    let (ack, accepted) = tokio::sync::oneshot::channel();
                    sender
                        .blocking_send((p, ack))
                        .context("template preparation cancelled")?;
                    accepted
                        .blocking_recv()
                        .context("template preparation cancelled")
                },
            )
        });
        while let Some((event, accepted)) = receiver.recv().await {
            if let Err(error) = progress(event) {
                drop(accepted);
                drop(receiver);
                let _ = worker.await;
                return Err(error);
            }
            let _ = accepted.send(());
        }
        let prepared = worker.await.context("template worker failed")??;
        progress(Progress::TemplateReady { reused: false })?;
        Ok(prepared)
    }
}

#[cfg(test)]
mod tests;
