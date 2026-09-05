//! Digest-bound offline rootfs input for the bounded Developer startup probe.
//!
//! These bytes carry no Docker compatibility certification. The exact version
//! metadata is part of the appliance digest; the archive is checked against that
//! metadata before callers receive its path.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::LinuxError;
use crate::kernel::KernelVersion;

/// Fixed bundle filename; metadata cannot redirect this outside the bundle.
pub const DEVELOPER_PROBE_ARCHIVE: &str = "developer-probe-rootfs.tar";
/// Versioned public marker contained in the rootfs archive.
pub const DEVELOPER_PROBE_MARKER: &[u8] = b"vz-developer-probe-v1\n";
/// Bound for the tiny static BusyBox startup rootfs archive.
pub const MAX_DEVELOPER_PROBE_BYTES: u64 = 32 * 1024 * 1024;

/// Provenance emitted by the normal pinned BusyBox packaging recipe.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeveloperProbeMetadata {
    /// Packaging protocol version, currently one.
    pub schema_version: u32,
    /// Exact fixed archive filename.
    pub archive: String,
    /// SHA256 of the complete deterministic rootfs tar bytes.
    pub sha256: String,
    /// SHA256 of the static Linux/arm64 BusyBox binary within that rootfs.
    pub busybox_sha256: String,
    /// Pinned BusyBox source version.
    pub busybox_version: String,
    /// SHA256 of the pinned upstream BusyBox source archive.
    pub source_archive_sha256: String,
    /// SHA256 of its case-preserving source inventory.
    pub source_inventory_sha256: String,
    /// SHA256 of the exact verified BusyBox build-provenance sidecar.
    pub build_provenance_sha256: String,
    /// SHA256 of the versioned public marker, never a secret.
    pub marker_sha256: String,
}

impl DeveloperProbeMetadata {
    /// Reject unknown versions, redirected filenames and malformed provenance.
    pub fn validate(&self) -> Result<(), LinuxError> {
        if self.schema_version != 1 || self.archive != DEVELOPER_PROBE_ARCHIVE {
            return Err(invalid(
                "unsupported startup-probe schema or archive filename",
            ));
        }
        for hash in [
            &self.sha256,
            &self.busybox_sha256,
            &self.source_archive_sha256,
            &self.source_inventory_sha256,
            &self.build_provenance_sha256,
            &self.marker_sha256,
        ] {
            if hash.len() != 64
                || !hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            {
                return Err(invalid(
                    "startup-probe SHA256 must be canonical lowercase hex",
                ));
            }
        }
        if self.marker_sha256 != format!("{:x}", Sha256::digest(DEVELOPER_PROBE_MARKER))
            || self.busybox_version.split('.').count() != 3
            || self
                .busybox_version
                .split('.')
                .any(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
        {
            return Err(invalid("startup-probe marker or BusyBox version mismatch"));
        }
        Ok(())
    }
}

/// A verified archive path and the exact digest/provenance bound by its bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedDeveloperProbe {
    /// Fixed archive beneath the verified caller-owned bundle directory.
    pub archive: PathBuf,
    /// Verified declaration from the digest-bound version metadata.
    pub metadata: DeveloperProbeMetadata,
}

pub(crate) async fn verify_developer_probe(
    bundle_dir: &Path,
    version: &KernelVersion,
) -> Result<Option<VerifiedDeveloperProbe>, LinuxError> {
    let archive = bundle_dir.join(DEVELOPER_PROBE_ARCHIVE);
    let Some(declaration) = version.developer_probe.as_ref() else {
        match tokio::fs::symlink_metadata(&archive).await {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
            Ok(_) => {
                return Err(invalid(
                    "startup-probe archive has no digest-bound declaration",
                ));
            }
        }
    };
    declaration.validate()?;
    if version.profile.as_deref() != Some("developer")
        || declaration.busybox_version != version.busybox
    {
        return Err(invalid(
            "startup-probe is not bound to this Developer BusyBox profile",
        ));
    }
    let metadata = tokio::fs::symlink_metadata(&archive).await?;
    use std::os::unix::fs::MetadataExt;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.len() == 0
        || metadata.len() > MAX_DEVELOPER_PROBE_BYTES
    {
        return Err(invalid(
            "startup-probe must be a bounded single-link regular archive",
        ));
    }
    let mut file = tokio::fs::File::open(&archive).await?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 65536];
    let mut count = 0u64;
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        count += read as u64;
        if count > MAX_DEVELOPER_PROBE_BYTES {
            return Err(invalid("startup-probe grew beyond its size bound"));
        }
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata().await?;
    if count != metadata.len()
        || after.len() != count
        || !after.is_file()
        || after.nlink() != 1
        || metadata.dev() != after.dev()
        || metadata.ino() != after.ino()
        || metadata.mtime() != after.mtime()
        || metadata.mtime_nsec() != after.mtime_nsec()
        || metadata.ctime() != after.ctime()
        || metadata.ctime_nsec() != after.ctime_nsec()
    {
        return Err(invalid("startup-probe changed during verification"));
    }
    let found = format!("{:x}", hasher.finalize());
    if found != declaration.sha256 {
        return Err(LinuxError::ArtifactChecksumMismatch {
            artifact: DEVELOPER_PROBE_ARCHIVE.to_owned(),
            path: archive.display().to_string(),
            expected: declaration.sha256.clone(),
            found,
        });
    }
    Ok(Some(VerifiedDeveloperProbe {
        archive,
        metadata: declaration.clone(),
    }))
}

fn invalid(message: &str) -> LinuxError {
    LinuxError::InvalidConfig(message.to_owned())
}
