//! Exact, verified fixture copies; no installation or ambient artifact lookup.
use anyhow::{Context, Result, ensure};
use rustix::fs::{Mode, OFlags, open};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::Path;
use vz_linux::{DEVELOPER_PROBE_ARCHIVE, KernelProfile, KernelVersion};

pub fn artifact_names(version: &KernelVersion) -> Result<Vec<&'static str>> {
    let mut names = vec!["vmlinux", "initramfs.img", "youki", "version.json"];
    if let Some(probe) = &version.developer_probe {
        probe.validate()?;
        ensure!(version.profile.as_deref() == Some("developer"));
        ensure!(probe.busybox_version == version.busybox);
        names.push(DEVELOPER_PROBE_ARCHIVE);
    }
    Ok(names)
}

pub async fn copy_fixture_bundle(
    source: &Path,
    destination: &Path,
    profile: KernelProfile,
) -> Result<()> {
    // Verify before creating any destination. In particular, an undeclared,
    // redirected, missing, or tampered probe is never copied or invented.
    let verified = vz_linux::verify_kernel_bundle_read_only(source, profile)
        .await
        .with_context(|| format!("verify source fixture bundle {}", source.display()))?;
    fs::DirBuilder::new().mode(0o700).create(destination)?;
    for name in artifact_names(&verified.paths.version)? {
        let source_path = source.join(name);
        let mut input = File::from(open(
            &source_path,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        let metadata = input.metadata()?;
        ensure!(metadata.is_file() && metadata.nlink() == 1);
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(destination.join(name))?;
        let copied = io::copy(&mut input, &mut output)?;
        ensure!(copied == metadata.len());
        output.sync_all()?;
    }
    File::open(destination)?.sync_all()?;
    let copied = vz_linux::verify_kernel_bundle_read_only(destination, profile)
        .await
        .with_context(|| format!("verify copied fixture bundle {}", destination.display()))?;
    ensure!(copied.artifact_identity == verified.artifact_identity);
    ensure!(copied.paths.version == verified.paths.version);
    Ok(())
}
