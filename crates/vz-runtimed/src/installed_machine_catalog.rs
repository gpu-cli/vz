//! Offline installer entry point: verify exact installed profiles before atomic publication.

use std::collections::BTreeSet;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use vz_linux::{KernelProfile, verify_kernel_bundle_read_only};
use vz_runtime_contract::MachineProfile;

use crate::machine_target_resolver::{
    LINUX_APPLIANCE_IMAGE, LinuxTargetCatalogEntry, MachineTargetCatalog,
};

/// Verify only explicitly named profiles from this installation transaction.
/// No legacy root aliases, stale optional profile discovery, channels or VM effects.
pub async fn write_installed_catalog(
    prefix: &Path,
    version: &str,
    profiles: &[String],
) -> Result<PathBuf> {
    write_installed_catalog_with_native(prefix, version, profiles, None).await
}

/// Publish a DEV native bundle alongside the explicitly installed Linux profiles.
/// The manifest digest is operator supplied; project definitions cannot select paths.
pub async fn write_installed_catalog_with_native(
    prefix: &Path,
    version: &str,
    profiles: &[String],
    native: Option<(&Path, &str)>,
) -> Result<PathBuf> {
    write_catalog(prefix, version, profiles, native, false).await
}

/// Register a prepared local image while retaining all installed Linux profiles
/// and older native pins. A single catalog lock serializes installer writes.
pub async fn register_local_catalog(
    prefix: &Path,
    version: &str,
    native: (&Path, &str),
) -> Result<PathBuf> {
    write_catalog(prefix, version, &[], Some(native), true).await
}

async fn write_catalog(
    prefix: &Path,
    version: &str,
    profiles: &[String],
    native: Option<(&Path, &str)>,
    preserve: bool,
) -> Result<PathBuf> {
    ensure!(
        prefix.is_absolute() && prefix.canonicalize()? == prefix,
        "installation prefix must be canonical and absolute"
    );
    trusted_metadata(prefix, true)?;
    ensure!(
        (!profiles.is_empty() || native.is_some()) && profiles.len() <= 2,
        "explicitly installed Linux profiles or a native bundle required"
    );
    let mut seen = BTreeSet::new();
    use std::os::unix::fs::OpenOptionsExt;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(prefix.join("machine-target-catalog.lock"))?;
    ensure!(
        lock.metadata()?.is_file() && lock.metadata()?.nlink() == 1,
        "invalid catalog lock"
    );
    loop {
        match fs2::FileExt::try_lock_exclusive(&lock) {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await
            }
            Err(error) => return Err(error.into()),
        }
    }
    let mut catalog = if prefix.join("machine-target-catalog.json").exists() {
        MachineTargetCatalog::from_file(&prefix.join("machine-target-catalog.json"))?
    } else {
        MachineTargetCatalog::default()
    };
    if !preserve {
        catalog.linux.clear();
    }
    for name in profiles {
        ensure!(seen.insert(name), "duplicate installed profile");
        let (kernel_profile, profile) = match name.as_str() {
            "developer" => (KernelProfile::Developer, MachineProfile::Developer),
            "container" => (KernelProfile::Container, MachineProfile::Hardened),
            _ => anyhow::bail!("unsupported installed Linux profile {name}"),
        };
        let bundle_dir = prefix.join("linux").join(name);
        ensure!(
            bundle_dir.canonicalize()? == bundle_dir,
            "installed profile ancestry must not contain symlinks"
        );
        trusted_metadata(&prefix.join("linux"), true)?;
        trusted_metadata(&bundle_dir, true)?;
        let verified = verify_kernel_bundle_read_only(&bundle_dir, kernel_profile)
            .await
            .with_context(|| format!("verify installed {name} bundle"))?;
        catalog.linux.push(LinuxTargetCatalogEntry {
            image: LINUX_APPLIANCE_IMAGE.into(),
            version: version.into(),
            profile,
            bundle_dir,
            digest: verified.artifact_identity.digest,
            channels: BTreeSet::new(),
        });
    }
    if let Some((bundle, digest)) = native {
        use sha2::{Digest, Sha256};
        use vz_macos_provision::{artifact_cache::Artifact, bootstrap::ReleaseManifest};
        ensure!(
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "expected native manifest SHA-256"
        );
        ensure!(
            bundle.is_absolute() && bundle.canonicalize()? == bundle,
            "native bundle must be canonical and absolute"
        );
        trusted_metadata(bundle, true)?;
        let source = bundle.join(digest);
        trusted_metadata(&source, false)?;
        let bytes = crate::native_macos::artifacts::read_regular(&source, 64 * 1024)?;
        ensure!(
            format!("{:x}", Sha256::digest(&bytes)) == digest,
            "native manifest checksum mismatch"
        );
        let release: ReleaseManifest = serde_json::from_slice(&bytes)?;
        release.validate()?;
        ensure!(
            release.development,
            "installed native bundles are explicitly DEV until release qualification"
        );
        for artifact in release.artifacts() {
            let source = bundle.join(&artifact.sha256);
            trusted_metadata(&source, false)?;
            ensure!(
                std::fs::metadata(&source)?.len() == artifact.size_bytes,
                "installed native input size mismatch"
            );
        }
        if preserve {
            ensure!(
                release.schema_version == 2,
                "local setup requires a local-image manifest"
            );
        }
        let variant = if release.toolchain_sha256.is_empty() {
            "clean"
        } else {
            "xcode"
        };
        for entry in &mut catalog.macos {
            entry.channels.remove("latest");
            entry.channels.remove(variant);
        }
        catalog
            .macos
            .retain(|entry| entry.manifest.sha256 != digest);
        catalog
            .macos
            .push(crate::machine_target_resolver::NativeMacosCatalogEntry {
                image: "vz-macos".into(),
                version: release.macos_version,
                manifest: Artifact {
                    url: format!("bundle:{digest}"),
                    sha256: digest.into(),
                    size_bytes: bytes.len() as u64,
                },
                installed_bundle: Some(bundle.into()),
                channels: BTreeSet::from(["latest".into(), variant.into()]),
            });
    }
    catalog.validate()?;
    let destination = prefix.join("machine-target-catalog.json");
    match std::fs::symlink_metadata(&destination) {
        Ok(_) => trusted_metadata(&destination, false)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    let mut staging = tempfile::NamedTempFile::new_in(prefix)?;
    serde_json::to_writer_pretty(&mut staging, &catalog)?;
    staging.write_all(b"\n")?;
    staging.as_file().sync_all()?;
    staging.persist(&destination).map_err(|error| error.error)?;
    std::fs::File::open(prefix)?.sync_all()?;
    Ok(destination)
}

fn trusted_metadata(path: &Path, directory: bool) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(
        if directory {
            metadata.is_dir()
        } else {
            metadata.is_file() && metadata.nlink() == 1
        },
        "unexpected installed catalog path type: {}",
        path.display()
    );
    ensure!(
        [0, rustix::process::geteuid().as_raw()].contains(&metadata.uid())
            && metadata.mode() & 0o022 == 0,
        "installed catalog path must be owned by root/current user and not writable by others: {}",
        path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use sha2::{Digest, Sha256};

    fn fixture(prefix: &Path, profile: KernelProfile) {
        let directory = prefix.join("linux").join(profile.as_str());
        std::fs::create_dir_all(&directory).expect("profile directory");
        for name in ["vmlinux", "initramfs.img", "youki"] {
            std::fs::write(directory.join(name), name).expect("artifact");
        }
        let hash = |value: &str| format!("{:x}", Sha256::digest(value.as_bytes()));
        let metadata = serde_json::json!({"kernel":"test-kernel", "busybox":"test-busybox", "agent":env!("CARGO_PKG_VERSION"),
            "agent_protocol_revision":vz_agent_proto::AGENT_PROTOCOL_REVISION, "youki":"test-youki",
            "profile":profile.as_str(), "security_profile":profile.security_profile(),
            "capabilities":profile.default_capabilities(), "sha256_vmlinux":hash("vmlinux"),
            "sha256_initramfs":hash("initramfs.img"), "sha256_youki":hash("youki")});
        std::fs::write(
            directory.join("version.json"),
            serde_json::to_vec(&metadata).expect("metadata"),
        )
        .expect("version");
    }

    #[tokio::test]
    async fn both_exact_profiles_are_verified_and_failed_upgrade_preserves_catalog() {
        let root = tempfile::tempdir().expect("root");
        let prefix = root.path().canonicalize().expect("canonical");
        fixture(&prefix, KernelProfile::Developer);
        fixture(&prefix, KernelProfile::Container);
        let profiles = vec!["developer".into(), "container".into()];
        let path = write_installed_catalog(&prefix, "0.4.0", &profiles)
            .await
            .expect("publish");
        let original = std::fs::read(&path).expect("catalog bytes");
        let catalog = MachineTargetCatalog::from_file(&path).expect("authoritative read");
        assert_eq!(catalog.linux.len(), 2);
        assert_eq!(catalog.linux[0].profile, MachineProfile::Developer);
        assert_eq!(catalog.linux[1].profile, MachineProfile::Hardened);
        assert_eq!(
            std::fs::metadata(&path).expect("mode").mode() & 0o777,
            0o600
        );
        for (entry, profile) in catalog
            .linux
            .iter()
            .zip([KernelProfile::Developer, KernelProfile::Container])
        {
            let verified = verify_kernel_bundle_read_only(&entry.bundle_dir, profile)
                .await
                .expect("verify");
            assert_eq!(entry.digest, verified.artifact_identity.digest);
            assert_eq!(entry.version, "0.4.0");
            assert!(entry.channels.is_empty());
        }
        std::fs::write(prefix.join("linux/container/youki"), "changed").expect("corrupt");
        assert!(
            write_installed_catalog(&prefix, "0.4.1", &profiles)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&path).expect("retained"), original);
        // Unselected stale/corrupt profiles cannot contaminate or expand the new catalog.
        write_installed_catalog(&prefix, "0.4.1", &["developer".into()])
            .await
            .expect("selected only");
        assert_eq!(
            MachineTargetCatalog::from_file(&path)
                .expect("read")
                .linux
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn publication_rejects_symlink_or_hardlinked_foreign_catalog() {
        let root = tempfile::tempdir().expect("root");
        let prefix = root.path().canonicalize().expect("canonical");
        fixture(&prefix, KernelProfile::Developer);
        let target = prefix.join("decoy");
        std::fs::write(&target, "preserve").expect("decoy");
        let catalog = prefix.join("machine-target-catalog.json");
        std::os::unix::fs::symlink(&target, &catalog).expect("symlink");
        assert!(
            write_installed_catalog(&prefix, "0.4.0", &["developer".into()])
                .await
                .is_err()
        );
        std::fs::remove_file(&catalog).expect("remove fixture symlink");
        std::fs::hard_link(&target, &catalog).expect("hardlink");
        assert!(
            write_installed_catalog(&prefix, "0.4.0", &["developer".into()])
                .await
                .is_err()
        );
        assert_eq!(std::fs::read_to_string(target).expect("decoy"), "preserve");
    }
    #[tokio::test]
    async fn missing_or_unselected_profile_never_publishes_catalog() {
        let root = tempfile::tempdir().expect("root");
        let prefix = root.path().canonicalize().expect("canonical");
        for profiles in [vec![], vec!["developer".into()], vec!["legacy".into()]] {
            assert!(
                write_installed_catalog(&prefix, "0.4.0", &profiles)
                    .await
                    .is_err()
            );
            assert!(!prefix.join("machine-target-catalog.json").exists());
        }
    }
    #[tokio::test]
    async fn local_registration_preserves_linux_old_pins_and_survives_reinstallation() {
        use vz_macos_provision::{
            artifact_cache::Artifact,
            bootstrap::{ImageIdentity, Platform, ReleaseManifest},
        };
        let root = tempfile::tempdir().unwrap();
        let prefix = root.path().canonicalize().unwrap();
        fixture(&prefix, KernelProfile::Developer);
        write_installed_catalog(&prefix, "0.4.0", &["developer".into()])
            .await
            .unwrap();
        let bundle = prefix.join("local");
        std::fs::create_dir(&bundle).unwrap();
        let blob = |bytes: &[u8]| {
            let sha256 = format!("{:x}", Sha256::digest(bytes));
            std::fs::write(bundle.join(&sha256), bytes).unwrap();
            Artifact {
                url: format!("bundle:{sha256}"),
                sha256,
                size_bytes: bytes.len() as u64,
            }
        };
        let image = blob(b"local image");
        let manifest = ReleaseManifest {
            schema_version: 2,
            development: true,
            macos_version: "26.3.1".into(),
            macos_build: "25D2128".into(),
            base: None,
            patch: None,
            local_image: Some(image.clone()),
            prepared_image: ImageIdentity {
                sha256: image.sha256,
                size_bytes: image.size_bytes,
            },
            platform: Platform {
                architecture: "aarch64".into(),
                minimum_host_version: "26.3.1".into(),
                minimum_cpu_count: 2,
                minimum_memory_bytes: 4096,
                hardware_model: blob(b"hardware"),
                auxiliary_storage_seed: blob(b"auxiliary"),
            },
            guest_agent_sha256: "a".repeat(64),
            toolchain_sha256: "b".repeat(64),
        };
        let first = blob(&serde_json::to_vec(&manifest).unwrap());
        let path = register_local_catalog(&prefix, "0.4.0", (&bundle, &first.sha256))
            .await
            .unwrap();
        let first_catalog = MachineTargetCatalog::from_file(&path).unwrap();
        assert_eq!(first_catalog.linux.len(), 1);
        assert_eq!(first_catalog.macos.len(), 1);
        let mut updated = manifest;
        updated.toolchain_sha256 = "c".repeat(64);
        let second = blob(&serde_json::to_vec(&updated).unwrap());
        register_local_catalog(&prefix, "0.4.0", (&bundle, &second.sha256))
            .await
            .unwrap();
        let catalog = MachineTargetCatalog::from_file(&path).unwrap();
        assert_eq!(catalog.linux, first_catalog.linux);
        assert_eq!(catalog.macos.len(), 2);
        assert!(catalog.macos[0].channels.is_empty());
        assert!(catalog.macos[1].channels.contains("latest"));
        updated.toolchain_sha256.clear();
        let clean = blob(&serde_json::to_vec(&updated).unwrap());
        register_local_catalog(&prefix, "0.4.0", (&bundle, &clean.sha256))
            .await
            .unwrap();
        let catalog = MachineTargetCatalog::from_file(&path).unwrap();
        assert_eq!(catalog.macos.len(), 3);
        assert_eq!(catalog.macos[1].channels, BTreeSet::from(["xcode".into()]));
        assert_eq!(
            catalog.macos[2].channels,
            BTreeSet::from(["latest".into(), "clean".into()])
        );
        write_installed_catalog(&prefix, "0.4.1", &["developer".into()])
            .await
            .unwrap();
        let reinstalled = MachineTargetCatalog::from_file(&path).unwrap();
        assert_eq!(reinstalled.macos, catalog.macos);
        assert_eq!(reinstalled.linux[0].version, "0.4.1");
        let before = std::fs::read(&path).unwrap();
        std::fs::write(bundle.join(&second.sha256), b"corrupted").unwrap();
        assert!(
            register_local_catalog(&prefix, "0.4.1", (&bundle, &second.sha256))
                .await
                .is_err()
        );
        assert_eq!(before, std::fs::read(path).unwrap());
    }
}
