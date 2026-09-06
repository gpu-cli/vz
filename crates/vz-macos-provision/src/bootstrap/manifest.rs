use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

use crate::artifact_cache::Artifact;

/// Exact disk content identity, independent of runtime Machine identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageIdentity {
    /// Lowercase SHA-256 of the complete uncompressed image.
    pub sha256: String,
    /// Exact logical image size, including sparse extents.
    pub size_bytes: u64,
}

impl ImageIdentity {
    pub(super) fn validate(&self) -> Result<()> {
        digest(&self.sha256)?;
        ensure!(self.size_bytes > 0, "empty prepared disk is unsupported");
        Ok(())
    }
}

/// Platform resources retained as immutable preparation inputs. The native
/// adapter must check host support and create private runtime platform state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Platform {
    /// Required architecture; currently only `aarch64` is accepted.
    pub architecture: String,
    /// Minimum host OS version, checked by the native adapter before boot.
    pub minimum_host_version: String,
    /// Minimum virtual CPU count.
    pub minimum_cpu_count: u32,
    /// Minimum guest RAM in bytes.
    pub minimum_memory_bytes: u64,
    /// Serialized Virtualization.framework hardware model for this release.
    pub hardware_model: Artifact,
    /// Quiescent auxiliary-storage seed. Never attach this shared file to a VM;
    /// compatibility with freshly allocated Machine identity needs native proof.
    pub auxiliary_storage_seed: Artifact,
}

/// Version 1 of the exact base + delta delivery contract.
///
/// Authenticate a pin for these exact JSON bytes using the release catalog,
/// then persist that pin in Environment state before invoking preparation.
/// `latest` resolution and signing authorities belong to the catalog adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseManifest {
    /// Format version; currently 1.
    pub schema_version: u32,
    /// Exact macOS guest version, with major version at least 26.
    pub macos_version: String,
    /// Exact Apple build identifier.
    pub macos_build: String,
    /// Uncompressed exact base disk bytes downloaded by consumers.
    pub base: Artifact,
    /// VZDELTA1 patch bound to that base and the expected prepared image.
    pub patch: Artifact,
    /// Expected patched output, checked against the header before application.
    pub prepared_image: ImageIdentity,
    /// Native platform requirements and immutable seed artifacts.
    pub platform: Platform,
    /// Agent content pin; readiness/handshake verification belongs to the adapter.
    pub guest_agent_sha256: String,
    /// Native toolchain content pin; the installed native gate verifies execution.
    pub toolchain_sha256: String,
}

impl ReleaseManifest {
    /// Validate the complete contract before acquiring image artifacts.
    /// This establishes syntax and pin consistency, not host or guest readiness.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == 1,
            "unsupported bootstrap manifest version"
        );
        ensure!(version(&self.macos_version)? >= 26, "macOS 26+ is required");
        ensure!(
            !self.macos_build.is_empty()
                && self.macos_build.len() <= 32
                && self.macos_build.bytes().all(|b| b.is_ascii_alphanumeric()),
            "invalid macOS build"
        );
        self.base.validate()?;
        self.patch.validate()?;
        self.prepared_image.validate()?;
        ensure!(
            self.platform.architecture == "aarch64",
            "unsupported macOS architecture"
        );
        version(&self.platform.minimum_host_version)?;
        ensure!(
            self.platform.minimum_cpu_count > 0 && self.platform.minimum_memory_bytes > 0,
            "missing platform resource requirements"
        );
        self.platform.hardware_model.validate()?;
        self.platform.auxiliary_storage_seed.validate()?;
        digest(&self.guest_agent_sha256)?;
        digest(&self.toolchain_sha256)?;
        Ok(())
    }
}

fn digest(value: &str) -> Result<()> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "expected lowercase SHA-256"
    );
    Ok(())
}

fn version(value: &str) -> Result<u32> {
    let parts: Vec<_> = value.split('.').collect();
    ensure!(
        (2..=3).contains(&parts.len())
            && parts
                .iter()
                .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit())),
        "expected numeric OS version"
    );
    for part in &parts {
        part.parse::<u32>()?;
    }
    Ok(parts[0].parse()?)
}
