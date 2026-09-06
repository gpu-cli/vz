//! Pinned Swift/SDK receipt embedded by local setup or maintainer preparation.
//!
//! The prepared-image identity binds the full initial installation. This small
//! receipt binds its input archive and the compiler/SDK anchors checked on boot;
//! it is not an attestation of every mutable file in a running Machine.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Fixed guest location for the release-authenticated receipt bytes.
pub const RECEIPT_PATH: &str = "/usr/local/share/vz/toolchain.json";
/// Fixed native developer directory; consumer requests cannot choose host paths.
pub const DEVELOPER_DIR: &str = "/Library/Developer/CommandLineTools";
/// Fixed location for an explicitly supplied complete Xcode candidate.
pub const XCODE_DEVELOPER_DIR: &str = "/Applications/Xcode.app/Contents/Developer";
/// Maximum receipt size accepted before JSON deserialization.
pub const MAX_RECEIPT_BYTES: usize = 32 * 1024;

/// Supported fixed installation layouts; arbitrary guest or host paths are excluded.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolchainLayout {
    /// Apple Command Line Tools; the default for legacy receipt bytes.
    #[default]
    Clt,
    /// A complete, explicitly supplied Xcode application bundle.
    Xcode,
}

/// Exact toolchain input identity, independent of its installation location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveIdentity {
    /// SHA-256 of the complete maintainer input archive.
    pub sha256: String,
    /// Archive length in bytes.
    pub size_bytes: u64,
}

/// Versioned recipe receipt whose exact JSON SHA-256 is `toolchain_sha256` in
/// the release manifest. Empty legacy DEV pins bypass this additional gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolchainManifest {
    /// Format version, currently one.
    pub schema_version: u32,
    /// Fixed installation layout. Missing legacy fields select Command Line Tools.
    #[serde(default)]
    pub layout: ToolchainLayout,
    /// Complete normalized combined output of `xcrun swift --version`.
    pub swift_version: String,
    /// Exact selected macOS SDK version.
    pub sdk_version: String,
    /// Required compiler/linker/package-manager/SDK anchors, relative to the
    /// fixed developer directory, mapped to their SHA-256 digests.
    pub files: BTreeMap<String, String>,
    /// Input archive identity checked before maintainer extraction.
    pub archive: ArchiveIdentity,
}

fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl ToolchainManifest {
    /// Fixed developer directory selected by the authenticated layout.
    pub fn developer_dir(&self) -> &'static str {
        match self.layout {
            ToolchainLayout::Clt => DEVELOPER_DIR,
            ToolchainLayout::Xcode => XCODE_DEVELOPER_DIR,
        }
    }

    /// Relative compiler directory within the selected developer directory.
    pub fn binary_dir(&self) -> &'static str {
        match self.layout {
            ToolchainLayout::Clt => "usr/bin",
            ToolchainLayout::Xcode => "Toolchains/XcodeDefault.xctoolchain/usr/bin",
        }
    }

    /// Relative SDK settings anchor for the exact selected version.
    pub fn sdk_settings_path(&self) -> String {
        let prefix = match self.layout {
            ToolchainLayout::Clt => "SDKs",
            ToolchainLayout::Xcode => "Platforms/MacOSX.platform/Developer/SDKs",
        };
        format!("{prefix}/MacOSX{}.sdk/SDKSettings.json", self.sdk_version)
    }
    /// Check bounds, required tools, and literal shell-safe relative paths.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == 1,
            "unsupported native toolchain receipt"
        );
        ensure!(
            digest(&self.archive.sha256) && self.archive.size_bytes > 0,
            "invalid toolchain archive identity"
        );
        ensure!(
            !self.swift_version.is_empty()
                && self.swift_version.len() <= 1024
                && self.swift_version.trim() == self.swift_version
                && !self
                    .swift_version
                    .chars()
                    .any(|c| c.is_control() && c != '\n'),
            "invalid Swift version"
        );
        ensure!(
            !self.sdk_version.is_empty()
                && self.sdk_version.len() <= 32
                && self
                    .sdk_version
                    .split('.')
                    .all(|v| !v.is_empty() && v.bytes().all(|c| c.is_ascii_digit())),
            "invalid SDK version"
        );
        ensure!(
            (6..=16).contains(&self.files.len()),
            "incomplete or oversized toolchain anchors"
        );
        for (path, sha) in &self.files {
            ensure!(
                path.len() <= 256
                    && path
                        .split('/')
                        .all(|c| !c.is_empty() && c != "." && c != "..")
                    && path
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"/._-+".contains(&c))
                    && digest(sha),
                "invalid toolchain anchor"
            );
        }
        for name in [
            "swift-frontend",
            "swift-driver",
            "swift-package",
            "clang",
            "ld",
        ] {
            ensure!(
                self.files
                    .contains_key(&format!("{}/{name}", self.binary_dir())),
                "missing native toolchain binary: {name}"
            );
        }
        ensure!(
            self.files.contains_key(&self.sdk_settings_path()),
            "missing selected SDK anchor"
        );
        Ok(())
    }

    /// Authenticate bounded receipt bytes before using any parsed path or value.
    pub fn from_verified_bytes(bytes: &[u8], expected_sha256: &str) -> Result<Self> {
        ensure!(
            bytes.len() <= MAX_RECEIPT_BYTES && digest(expected_sha256),
            "invalid toolchain receipt bounds or pin"
        );
        ensure!(
            format!("{:x}", Sha256::digest(bytes)) == expected_sha256,
            "native toolchain receipt SHA-256 mismatch"
        );
        let manifest: Self = serde_json::from_slice(bytes)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Build a fixed-location guest verification command and exact expected
    /// stdout. Validate paths before interpolation; no shell syntax is admitted.
    pub fn verification(&self) -> Result<(String, String)> {
        self.validate()?;
        let developer = self.developer_dir();
        let mut script = format!("set -eu; export DEVELOPER_DIR='{developer}'; ");
        let mut expected = String::new();
        for (path, hash) in &self.files {
            script.push_str(&format!("/usr/bin/shasum -a 256 '{developer}/{path}'; "));
            expected.push_str(&format!("{hash}  {developer}/{path}\n"));
        }
        script.push_str("/usr/bin/xcrun --find swift; /usr/bin/xcrun swift --version 2>&1; /usr/bin/xcrun --sdk macosx --show-sdk-version");
        expected.push_str(&format!(
            "{developer}/{}/swift\n{}\n{}\n",
            self.binary_dir(),
            self.swift_version,
            self.sdk_version
        ));
        Ok((script, expected))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> ToolchainManifest {
        let mut files = [
            "swift-frontend",
            "swift-driver",
            "swift-package",
            "clang",
            "ld",
        ]
        .into_iter()
        .map(|name| (format!("usr/bin/{name}"), "a".repeat(64)))
        .collect::<BTreeMap<_, _>>();
        files.insert(
            "SDKs/MacOSX26.1.sdk/SDKSettings.json".into(),
            "b".repeat(64),
        );
        ToolchainManifest {
            schema_version: 1,
            layout: ToolchainLayout::Clt,
            swift_version: "Apple Swift version 6.2.1\nTarget: arm64-apple-macosx26.0".into(),
            sdk_version: "26.1".into(),
            files,
            archive: ArchiveIdentity {
                sha256: "c".repeat(64),
                size_bytes: 1024,
            },
        }
    }
    #[test]
    fn receipt_requires_exact_bytes_and_complete_compiler_sdk_identity() -> Result<()> {
        let original = fixture();
        let bytes = serde_json::to_vec(&original)?;
        let pin = format!("{:x}", Sha256::digest(&bytes));
        assert_eq!(
            ToolchainManifest::from_verified_bytes(&bytes, &pin)?,
            original
        );
        let mut changed = bytes.clone();
        changed.push(b' ');
        assert!(ToolchainManifest::from_verified_bytes(&changed, &pin).is_err());
        let mut incomplete = original;
        incomplete.files.remove("usr/bin/swift-frontend");
        assert!(incomplete.validate().is_err());
        assert!(
            ToolchainManifest::from_verified_bytes(&vec![0; MAX_RECEIPT_BYTES + 1], &pin).is_err()
        );
        Ok(())
    }
    #[test]
    fn xcode_layout_binds_its_own_compiler_and_sdk_paths() -> Result<()> {
        let mut manifest = fixture();
        manifest.layout = ToolchainLayout::Xcode;
        assert!(manifest.validate().is_err());
        manifest.files = manifest
            .files
            .into_iter()
            .map(|(path, hash)| {
                let path = if path.starts_with("usr/") {
                    format!("Toolchains/XcodeDefault.xctoolchain/{path}")
                } else {
                    format!("Platforms/MacOSX.platform/Developer/{path}")
                };
                (path, hash)
            })
            .collect();
        let (script, expected) = manifest.verification()?;
        assert!(script.contains(XCODE_DEVELOPER_DIR));
        assert!(expected.contains("/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/swift\n"));
        assert!(!expected.contains(DEVELOPER_DIR));
        let mut legacy = serde_json::to_value(fixture())?;
        legacy
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("fixture is not an object"))?
            .remove("layout");
        let bytes = serde_json::to_vec(&legacy)?;
        let restored = ToolchainManifest::from_verified_bytes(
            &bytes,
            &format!("{:x}", Sha256::digest(&bytes)),
        )?;
        assert_eq!(restored.layout, ToolchainLayout::Clt);
        Ok(())
    }

    #[test]
    fn receipt_rejects_escape_paths_shell_syntax_and_unbound_sdk() {
        for path in [
            "../escape",
            "/absolute",
            "usr/../escape",
            "usr/$(id)",
            "usr/'bad",
            "usr//bin",
            "usr/./bin",
        ] {
            let mut manifest = fixture();
            manifest.files.insert(path.into(), "a".repeat(64));
            assert!(manifest.verification().is_err(), "{path}");
        }
        let mut manifest = fixture();
        manifest.sdk_version = "26.2".into();
        assert!(manifest.verification().is_err());
    }
}
