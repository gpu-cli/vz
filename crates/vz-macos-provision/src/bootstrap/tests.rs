use super::*;
use sha2::{Digest, Sha256};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tempfile::TempDir;

struct Fixture {
    _directory: TempDir,
    root: PathBuf,
    cache: BootstrapCache,
    manifest: ReleaseManifest,
    pin: Artifact,
    expected: Vec<u8>,
}

fn artifact(bytes: &[u8]) -> Artifact {
    Artifact {
        url: "https://example.invalid/never-requested".into(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        size_bytes: bytes.len() as u64,
    }
}

impl Fixture {
    fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let cache = BootstrapCache::new(root.join("cache"))?;
        let base = vec![0; 3 * 1024 * 1024];
        let mut expected = base.clone();
        expected[1024..2048].fill(13);
        fs::write(root.join("base"), &base)?;
        fs::write(root.join("output"), &expected)?;
        image_delta::create(
            &root.join("base"),
            &root.join("output"),
            &root.join("delta"),
            65536,
            |_| Ok(()),
        )?;
        let patch = fs::read(root.join("delta"))?;
        let hardware = b"test hardware model";
        let auxiliary = b"test auxiliary seed";
        let manifest = ReleaseManifest {
            development: false,
            schema_version: 1,
            macos_version: "26.6.2".into(),
            macos_build: "25G83".into(),
            base: Some(artifact(&base)),
            patch: Some(artifact(&patch)),
            local_image: None,
            prepared_image: ImageIdentity {
                sha256: artifact(&expected).sha256,
                size_bytes: expected.len() as u64,
            },
            platform: Platform {
                architecture: "aarch64".into(),
                minimum_host_version: "26.3.1".into(),
                minimum_cpu_count: 2,
                minimum_memory_bytes: 4 * 1024 * 1024 * 1024,
                hardware_model: artifact(hardware),
                auxiliary_storage_seed: artifact(auxiliary),
            },
            guest_agent_sha256: artifact(b"test agent").sha256,
            toolchain_sha256: artifact(b"test toolchain").sha256,
        };
        for bytes in [&base[..], &patch, hardware, auxiliary] {
            fs::write(
                root.join("cache/downloads").join(artifact(bytes).sha256),
                bytes,
            )?;
        }
        let pin = artifact(&serde_json::to_vec(&manifest)?);
        let mut f = Self {
            _directory: directory,
            root,
            cache,
            manifest,
            pin,
            expected,
        };
        f.save_manifest()?;
        Ok(f)
    }

    fn save_manifest(&mut self) -> Result<()> {
        let bytes = serde_json::to_vec(&self.manifest)?;
        self.pin = artifact(&bytes);
        fs::write(
            self.root.join("cache/downloads").join(&self.pin.sha256),
            bytes,
        )?;
        Ok(())
    }

    fn template(&self) -> PathBuf {
        self.root.join("cache/templates").join(&self.pin.sha256)
    }
    fn stage(&self) -> PathBuf {
        self.root
            .join("cache/templates")
            .join(format!("{}.staging", self.pin.sha256))
    }
}

#[tokio::test]
async fn concurrent_preparation_commits_once_and_warm_hit_needs_no_large_blobs() -> Result<()> {
    let f = Fixture::new()?;
    let preparations = AtomicUsize::new(0);
    let callback = |p| {
        if p == Progress::PublishingTemplate {
            preparations.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    };
    let (a, b) = tokio::join!(
        f.cache.prepare(&f.pin, callback),
        f.cache.prepare(&f.pin, callback)
    );
    let a = a?;
    let b = b?;
    assert_eq!(a.manifest_sha256(), b.manifest_sha256());
    assert_eq!(preparations.load(Ordering::SeqCst), 1);
    assert_eq!(fs::read(f.template().join("disk.img"))?, f.expected);
    for pin in f.manifest.artifacts() {
        fs::remove_file(f.root.join("cache/downloads").join(&pin.sha256))?;
    }
    let mut events = Vec::new();
    let warm = f
        .cache
        .prepare(&f.pin, |p| {
            events.push(p);
            Ok(())
        })
        .await?;
    warm.validate_cached()?;
    assert!(events.contains(&Progress::TemplateReady { reused: true }));
    assert!(events.iter().all(|p| matches!(
        p,
        Progress::Artifact {
            component: Component::Manifest,
            ..
        } | Progress::TemplateReady { reused: true }
    )));
    Ok(())
}

#[tokio::test]
async fn mismatched_output_is_rejected_before_image_preparation() -> Result<()> {
    let mut f = Fixture::new()?;
    f.manifest.prepared_image.sha256 = artifact(b"different").sha256;
    f.save_manifest()?;
    let mut prepared = false;
    let result = f
        .cache
        .prepare(&f.pin, |p| {
            prepared |= matches!(p, Progress::PreparingImage { .. });
            Ok(())
        })
        .await;
    assert!(result.is_err());
    assert!(!prepared && !f.template().exists() && !f.stage().exists());
    Ok(())
}

#[tokio::test]
async fn cancellation_at_each_worker_phase_discards_stage_and_allows_retry() -> Result<()> {
    for phase in ["verify", "copy", "patch", "output", "platform", "publish"] {
        let f = Fixture::new()?;
        let mut cancelled = false;
        let result = f
            .cache
            .prepare(&f.pin, |p| {
                let current = match p {
                    Progress::PreparingImage { progress } => match progress.phase {
                        image_delta::Phase::VerifyingBase => "verify",
                        image_delta::Phase::CopyingBase => "copy",
                        image_delta::Phase::ApplyingPatch => "patch",
                        image_delta::Phase::VerifyingOutput => "output",
                        _ => "",
                    },
                    Progress::PreparingPlatform { .. } => "platform",
                    Progress::PublishingTemplate => "publish",
                    _ => "",
                };
                if current == phase {
                    cancelled = true;
                    anyhow::bail!("test cancellation");
                }
                Ok(())
            })
            .await;
        assert!(result.is_err() && cancelled, "{phase}");
        assert!(!f.template().exists() && !f.stage().exists(), "{phase}");
        f.cache
            .prepare(&f.pin, |_| Ok(()))
            .await?
            .validate_cached()?;
    }
    Ok(())
}

#[tokio::test]
async fn dropping_operation_stops_worker_and_releases_lock() -> Result<()> {
    let f = Arc::new(Fixture::new()?);
    let started = Arc::new(tokio::sync::Notify::new());
    let signal = started.clone();
    let fixture = f.clone();
    let operation = tokio::spawn(async move {
        fixture
            .cache
            .prepare(&fixture.pin, |p| {
                if matches!(p, Progress::PreparingImage { .. }) {
                    signal.notify_one();
                }
                Ok(())
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), started.notified()).await?;
    operation.abort();
    assert!(operation.await.is_err_and(|e| e.is_cancelled()));
    // A subsequent caller waits for worker cleanup rather than stealing its lock.
    tokio::time::timeout(Duration::from_secs(10), f.cache.prepare(&f.pin, |_| Ok(()))).await??;
    assert!(!f.stage().exists());
    Ok(())
}

#[tokio::test]
async fn stale_stage_is_recovered_but_corrupt_completed_template_is_preserved() -> Result<()> {
    let f = Fixture::new()?;
    private_directory(&f.stage())?;
    fs::write(f.stage().join("partial"), b"abandoned")?;
    f.cache.prepare(&f.pin, |_| Ok(())).await?;
    assert!(!f.stage().exists());
    let disk = f.template().join("disk.img");
    fs::set_permissions(&disk, fs::Permissions::from_mode(0o600))?;
    fs::write(&disk, vec![5; f.expected.len()])?;
    fs::set_permissions(&disk, fs::Permissions::from_mode(0o400))?;
    assert!(f.cache.prepare(&f.pin, |_| Ok(())).await.is_err());
    assert_eq!(fs::read(disk)?[0], 5);
    Ok(())
}

#[tokio::test]
async fn incomplete_or_symlink_template_never_counts_as_ready() -> Result<()> {
    for link in [true, false] {
        let f = Fixture::new()?;
        if link {
            symlink(&f.root, f.template())?;
        } else {
            private_directory(&f.template())?;
        }
        assert!(f.cache.prepare(&f.pin, |_| Ok(())).await.is_err());
        assert!(fs::symlink_metadata(f.template()).is_ok());
    }
    Ok(())
}

#[tokio::test]
async fn manifest_version_pins_and_size_fail_before_image_work() -> Result<()> {
    let mut f = Fixture::new()?;
    f.manifest.schema_version = 2;
    f.save_manifest()?;
    assert!(f.cache.prepare(&f.pin, |_| Ok(())).await.is_err());
    f.manifest.schema_version = 1;
    f.manifest.macos_version = "latest".into();
    assert!(f.manifest.validate().is_err());
    f.manifest.macos_version = "15.6.1".into();
    assert!(f.manifest.validate().is_err());
    let mut oversized = f.pin.clone();
    oversized.size_bytes = MAX_MANIFEST_BYTES + 1;
    assert!(f.cache.prepare(&oversized, |_| Ok(())).await.is_err());
    let mut value = serde_json::to_value(&f.manifest)?;
    value["unexpected"] = true.into();
    assert!(serde_json::from_value::<ReleaseManifest>(value).is_err());
    assert!(!f.template().exists());
    Ok(())
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn apfs_clones_have_private_writable_inodes_and_no_identity_sidecars() -> Result<()> {
    let f = Fixture::new()?;
    let ready = f.cache.prepare(&f.pin, |_| Ok(())).await?;
    let machines = f.root.join("machines");
    private_directory(&machines)?;
    let a = machines.join("a.img");
    let b = machines.join("b.img");
    assert!(ready.clone_disk(&f.template().join("mutable.img")).is_err());
    assert!(
        ready
            .clone_disk(&machines.join("../cache/templates/mutable.img"))
            .is_err()
    );
    ready.clone_disk(&a)?;
    ready.clone_disk(&b)?;
    assert_ne!(fs::metadata(&a)?.ino(), fs::metadata(&b)?.ino());
    assert_ne!(
        fs::metadata(&a)?.ino(),
        fs::metadata(f.template().join("disk.img"))?.ino()
    );
    assert_eq!(fs::metadata(&a)?.mode() & 0o777, 0o600);
    assert_eq!(fs::metadata(&a)?.nlink(), 1);
    fs::write(&a, b"private mutation")?;
    assert_eq!(fs::read(&b)?, f.expected);
    ready.validate_cached()?;
    assert!(ready.clone_disk(&a).is_err());
    assert_eq!(fs::read(&a)?, b"private mutation");
    assert_eq!(fs::read_dir(&machines)?.count(), 2);
    fs::remove_file(&a)?;
    assert_eq!(fs::read(&b)?, f.expected);
    ready.validate_cached()?;
    Ok(())
}

#[tokio::test]
async fn prepared_handle_does_not_recreate_a_deleted_template() -> Result<()> {
    let f = Fixture::new()?;
    let ready = f.cache.prepare(&f.pin, |_| Ok(())).await?;
    fs::remove_dir_all(f.template())?;
    assert!(ready.validate_cached().is_err());
    assert!(!f.template().exists());
    Ok(())
}

#[tokio::test]
async fn cancelling_waiter_does_not_interrupt_lock_owner() -> Result<()> {
    let f = Fixture::new()?;
    let lock = preparation_lock(&f.cache.templates.join(format!("{}.lock", f.pin.sha256)))?;
    lock.lock_exclusive()?;
    let mut waited = false;
    let result = f
        .cache
        .prepare(&f.pin, |event| {
            if event == Progress::WaitingForTemplate {
                waited = true;
                anyhow::bail!("cancel waiter");
            }
            Ok(())
        })
        .await;
    assert!(result.is_err() && waited);
    let other = preparation_lock(&f.cache.templates.join(format!("{}.lock", f.pin.sha256)))?;
    assert!(other.try_lock_exclusive().is_err());
    drop(lock);
    f.cache.prepare(&f.pin, |_| Ok(())).await?;
    Ok(())
}

#[tokio::test]
async fn different_release_pins_keep_existing_template_independent() -> Result<()> {
    let mut f = Fixture::new()?;
    let first_pin = f.pin.clone();
    let first = f.cache.prepare(&first_pin, |_| Ok(())).await?;
    // Even identical disk bytes must not erase different agent/platform contracts.
    f.manifest.guest_agent_sha256 = artifact(b"next agent").sha256;
    f.save_manifest()?;
    let second = f.cache.prepare(&f.pin, |_| Ok(())).await?;
    assert_ne!(first.manifest_sha256(), second.manifest_sha256());
    assert_ne!(
        first.manifest().guest_agent_sha256,
        second.manifest().guest_agent_sha256
    );
    first.validate_cached()?;
    second.validate_cached()?;
    let restarted = BootstrapCache::new(f.root.join("cache"))?;
    let original = restarted.prepare(&first_pin, |_| Ok(())).await?;
    assert_eq!(original.manifest(), first.manifest());
    Ok(())
}

#[test]
fn cache_root_rejects_parent_traversal_before_creation() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    assert!(BootstrapCache::new(root.join("unused/../cache")).is_err());
    assert!(!root.join("cache").exists());
    Ok(())
}

#[tokio::test]
async fn installed_bundle_verifies_then_reuses_template_without_source_blobs() -> Result<()> {
    let mut f = Fixture::new()?;
    f.manifest.development = true;
    f.manifest.toolchain_sha256.clear();
    for a in [
        f.manifest.base.as_mut().context("base")?,
        f.manifest.patch.as_mut().context("patch")?,
        &mut f.manifest.platform.hardware_model,
        &mut f.manifest.platform.auxiliary_storage_seed,
    ] {
        a.url = format!("bundle:{}", a.sha256);
    }
    f.save_manifest()?;
    f.pin.url = format!("bundle:{}", f.pin.sha256);
    let bundle = f.root.join("bundle");
    fs::rename(f.root.join("cache/downloads"), &bundle)?;
    let cache = BootstrapCache::new(f.root.join("consumer"))?;
    let prepared = cache.prepare_installed(&f.pin, &bundle, |_| Ok(())).await?;
    assert_eq!(prepared.manifest(), &f.manifest);
    fs::remove_dir_all(&bundle)?;
    let warm = cache.prepare_installed(&f.pin, &bundle, |_| Ok(())).await?;
    assert_eq!(prepared.manifest_sha256(), warm.manifest_sha256());
    Ok(())
}

#[test]
fn missing_toolchain_is_only_accepted_for_explicit_development() -> Result<()> {
    let mut f = Fixture::new()?;
    f.manifest.toolchain_sha256.clear();
    assert!(f.manifest.validate().is_err());
    f.manifest.development = true;
    f.manifest.validate()?;
    f.manifest.toolchain_sha256 = "unverified".into();
    assert!(f.manifest.validate().is_err());
    Ok(())
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn local_image_needs_no_delta_and_warm_reuse_needs_no_source_disk() -> Result<()> {
    let mut f = Fixture::new()?;
    f.manifest.development = true;
    f.manifest.toolchain_sha256.clear();
    let bundle = f.root.join("local-inputs");
    private_directory(&bundle)?;
    f.manifest.schema_version = 2;
    f.manifest.base = None;
    f.manifest.patch = None;
    let mut image = artifact(&f.expected);
    image.url = format!("bundle:{}", image.sha256);
    f.manifest.local_image = Some(image.clone());
    f.manifest.validate()?;
    fs::write(bundle.join(&image.sha256), &f.expected)?;
    f.save_manifest()?;
    for a in [
        &f.manifest.platform.hardware_model,
        &f.manifest.platform.auxiliary_storage_seed,
    ] {
        fs::copy(
            f.root.join("cache/downloads").join(&a.sha256),
            bundle.join(&a.sha256),
        )?;
    }
    let mut delta_seen = false;
    let mut commits = 0;
    let template = f
        .cache
        .prepare_installed(&f.pin, &bundle, |p| {
            delta_seen |= matches!(p, Progress::PreparingImage { .. });
            if p == Progress::PublishingTemplate {
                commits += 1;
            }
            Ok(())
        })
        .await?;
    assert!(!delta_seen);
    assert_eq!(commits, 1);
    assert_eq!(fs::read(f.template().join("disk.img"))?, f.expected);
    fs::set_permissions(&f.root, fs::Permissions::from_mode(0o700))?;
    let first = f.root.join("first.img");
    let second = f.root.join("second.img");
    template.clone_disk(&first)?;
    template.clone_disk(&second)?;
    assert_ne!(fs::metadata(&first)?.ino(), fs::metadata(&second)?.ino());
    fs::write(&first, b"private mutation")?;
    assert_eq!(fs::read(&second)?, f.expected);
    fs::remove_dir_all(bundle)?;
    f.cache
        .prepare_installed(&f.pin, &f.root.join("removed"), |_| Ok(()))
        .await?
        .validate_cached()?;
    Ok(())
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn corrupted_local_image_never_publishes_and_retry_succeeds() -> Result<()> {
    let mut f = Fixture::new()?;
    let bundle = f.root.join("local-inputs");
    private_directory(&bundle)?;
    f.manifest.schema_version = 2;
    f.manifest.base = None;
    f.manifest.patch = None;
    let mut image = artifact(&f.expected);
    image.url = format!("bundle:{}", image.sha256);
    f.manifest.local_image = Some(image.clone());
    f.save_manifest()?;
    for a in [
        &f.manifest.platform.hardware_model,
        &f.manifest.platform.auxiliary_storage_seed,
    ] {
        fs::copy(
            f.root.join("cache/downloads").join(&a.sha256),
            bundle.join(&a.sha256),
        )?;
    }
    fs::write(bundle.join(&image.sha256), vec![55; f.expected.len()])?;
    let error = f
        .cache
        .prepare_installed(&f.pin, &bundle, |_| Ok(()))
        .await
        .err()
        .context("corruption rejected")?;
    assert!(error.to_string().contains("checksum mismatch"));
    assert!(!f.template().exists() && !f.stage().exists());
    fs::write(bundle.join(&image.sha256), &f.expected)?;
    let error = f
        .cache
        .prepare_installed(&f.pin, &bundle, |p| {
            if matches!(
                p,
                Progress::Artifact {
                    component: Component::LocalImage,
                    ..
                }
            ) {
                anyhow::bail!("cancel local verification");
            }
            Ok(())
        })
        .await
        .err()
        .context("cancelled")?;
    assert!(error.to_string().contains("cancel local verification"));
    assert!(!f.template().exists() && !f.stage().exists());
    f.cache
        .prepare_installed(&f.pin, &bundle, |_| Ok(()))
        .await?
        .validate_cached()?;
    Ok(())
}

#[test]
fn local_and_delta_sources_cannot_be_mixed_or_downloaded_as_local_images() -> Result<()> {
    let mut f = Fixture::new()?;
    f.manifest.schema_version = 2;
    let mut image = artifact(&f.expected);
    image.url = format!("bundle:{}", image.sha256);
    f.manifest.local_image = Some(image);
    assert!(f.manifest.validate().is_err());
    f.manifest.base = None;
    f.manifest.patch = None;
    f.manifest.validate()?;
    f.manifest.local_image.as_mut().context("local image")?.url =
        "https://example.invalid/image".into();
    assert!(f.manifest.validate().is_err());
    Ok(())
}
