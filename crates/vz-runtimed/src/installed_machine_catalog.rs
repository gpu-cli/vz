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
    ensure!(
        prefix.is_absolute() && prefix.canonicalize()? == prefix,
        "installation prefix must be canonical and absolute"
    );
    trusted_metadata(prefix, true)?;
    ensure!(
        !profiles.is_empty() && profiles.len() <= 2,
        "one or two explicitly installed Linux profiles required"
    );
    let mut seen = BTreeSet::new();
    let mut catalog = MachineTargetCatalog::default();
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
}
