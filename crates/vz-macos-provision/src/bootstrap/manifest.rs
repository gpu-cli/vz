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
    /// Explicit unqualified DEV inputs. A missing toolchain pin is permitted
    /// only here; this flag never establishes release conformance.
    #[serde(default)]
    pub development: bool,
    /// Format version; currently 1.
    pub schema_version: u32,
    /// Exact macOS guest version, with major version at least 26.
    pub macos_version: String,
    /// Exact Apple build identifier.
    pub macos_build: String,
    /// Uncompressed exact base disk bytes downloaded by consumers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<Artifact>,
    /// VZDELTA1 patch bound to that base and the expected prepared image.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub patch: Option<Artifact>,
    /// Locally provisioned quiescent disk. Schema 2 never requires a block patch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_image: Option<Artifact>,
    /// Expected patched output, checked against the header before application.
    pub prepared_image: ImageIdentity,
    /// Native platform requirements and immutable seed artifacts.
    pub platform: Platform,
    /// Agent content pin; readiness/handshake verification belongs to the adapter.
    pub guest_agent_sha256: String,
    /// SHA-256 of exact `toolchain::ToolchainManifest` receipt bytes embedded in
    /// the image. Native readiness verifies the pinned compiler/SDK anchors.
    pub toolchain_sha256: String,
}

impl ReleaseManifest {
    /// Exact image and platform inputs in preparation order.
    pub fn artifacts(&self) -> Vec<&Artifact> {
        self.base
            .iter()
            .chain(self.patch.iter())
            .chain(self.local_image.iter())
            .chain([
                &self.platform.hardware_model,
                &self.platform.auxiliary_storage_seed,
            ])
            .collect()
    }

    /// Validate the complete contract before acquiring image artifacts.
    /// This establishes syntax and pin consistency, not host or guest readiness.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            matches!(self.schema_version, 1 | 2),
            "unsupported bootstrap manifest version"
        );
        ensure!(version(&self.macos_version)? >= 26, "macOS 26+ is required");
        ensure!(
            !self.macos_build.is_empty()
                && self.macos_build.len() <= 32
                && self.macos_build.bytes().all(|b| b.is_ascii_alphanumeric()),
            "invalid macOS build"
        );
        match (
            &self.base,
            &self.patch,
            &self.local_image,
            self.schema_version,
        ) {
            (Some(base), Some(patch), None, 1) => {
                base.validate()?;
                patch.validate()?;
            }
            (None, None, Some(image), 2) => {
                image.validate()?;
                ensure!(
                    image.url == format!("bundle:{}", image.sha256),
                    "local image requires an installed bundle"
                );
                ensure!(
                    image.sha256 == self.prepared_image.sha256
                        && image.size_bytes == self.prepared_image.size_bytes,
                    "local image differs from prepared identity"
                );
                ensure!(
                    !self.toolchain_sha256.is_empty(),
                    "local setup requires a validated toolchain"
                );
            }
            _ => anyhow::bail!("manifest must select exactly one versioned preparation source"),
        }
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
        if !self.development || !self.toolchain_sha256.is_empty() {
            digest(&self.toolchain_sha256)?;
        }
        if !self.development && self.schema_version == 1 {
            ensure!(
                self.artifacts()
                    .iter()
                    .all(|a| a.url.starts_with("https://")),
                "qualified release artifacts require HTTPS locations"
            );
        }
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
