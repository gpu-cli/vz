use std::ffi::OsString;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::LinuxError;

const KERNEL_FILE: &str = "vmlinux";
const INITRAMFS_FILE: &str = "initramfs.img";
const YOUKI_FILE: &str = "youki";
const VERSION_FILE: &str = "version.json";

/// Installed kernel artifact paths and metadata.
#[derive(Debug, Clone)]
pub struct KernelPaths {
    /// Linux kernel image path.
    pub kernel: PathBuf,
    /// Initramfs image path.
    pub initramfs: PathBuf,
    /// Pinned Linux/arm64 `youki` runtime binary path.
    pub youki: PathBuf,
    /// Parsed artifact metadata from `version.json`.
    pub version: KernelVersion,
}

/// Serialized metadata for bundled Linux kernel artifacts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KernelVersion {
    /// Linux kernel version.
    pub kernel: String,
    /// BusyBox version used in initramfs.
    pub busybox: String,
    /// Guest-agent version used in initramfs.
    pub agent: String,
    /// Pinned youki runtime version.
    pub youki: String,
    /// Build timestamp (optional).
    pub built: Option<String>,
    /// Optional SHA256 of `vmlinux`.
    pub sha256_vmlinux: Option<String>,
    /// Optional SHA256 of `initramfs.img`.
    pub sha256_initramfs: Option<String>,
    /// Optional SHA256 of `youki`.
    pub sha256_youki: Option<String>,
}

/// Options for resolving kernel artifacts.
#[derive(Debug, Clone)]
pub struct EnsureKernelOptions {
    /// Install/cache directory (defaults to `~/.vz/linux`).
    pub install_dir: Option<PathBuf>,
    /// Optional predownloaded bundle directory to install from.
    ///
    /// If unset, `VZ_LINUX_BUNDLE_DIR` is used when present.
    pub bundle_dir: Option<PathBuf>,
    /// Require `version.json.agent == CARGO_PKG_VERSION`.
    pub require_exact_agent_version: bool,
}

impl Default for EnsureKernelOptions {
    fn default() -> Self {
        Self {
            install_dir: None,
            bundle_dir: None,
            require_exact_agent_version: true,
        }
    }
}

/// Resolve the default Linux artifact directory (`~/.vz/linux`).
pub fn default_linux_dir() -> Result<PathBuf, LinuxError> {
    let home = std::env::var_os("HOME").ok_or(LinuxError::HomeDirectoryUnavailable)?;
    Ok(PathBuf::from(home).join(".vz").join("linux"))
}

/// Ensure Linux kernel artifacts are installed and compatible.
pub async fn ensure_kernel() -> Result<KernelPaths, LinuxError> {
    ensure_kernel_with_options(EnsureKernelOptions::default()).await
}

/// Ensure Linux kernel artifacts are installed and compatible.
///
/// Resolution order:
/// 1. Install from `bundle_dir` / `VZ_LINUX_BUNDLE_DIR` when provided.
/// 2. Existing files in `install_dir` (or `~/.vz/linux`).
pub async fn ensure_kernel_with_options(
    options: EnsureKernelOptions,
) -> Result<KernelPaths, LinuxError> {
    let should_probe_workspace_bundle = options.install_dir.is_none();
    let install_dir = match options.install_dir {
        Some(path) => path,
        None => default_linux_dir()?,
    };
    let expected_agent = env!("CARGO_PKG_VERSION").to_string();
    let mut bundle_dir = options
        .bundle_dir
        .or_else(|| std::env::var_os("VZ_LINUX_BUNDLE_DIR").map(PathBuf::from));
    if bundle_dir.is_none() && should_probe_workspace_bundle {
        bundle_dir = workspace_bundle_dir();
    }

    if let Some(bundle_dir) = bundle_dir {
        let bundle = read_kernel_paths(&bundle_dir).await?;
        validate_agent_version(
            &bundle.version,
            &expected_agent,
            options.require_exact_agent_version,
        )?;
        validate_artifact_checksums(&bundle).await?;

        if let Ok(installed) = read_kernel_paths(&install_dir).await {
            let version_ok = validate_agent_version(
                &installed.version,
                &expected_agent,
                options.require_exact_agent_version,
            )
            .is_ok();
            let checksum_ok = validate_artifact_checksums(&installed).await.is_ok();

            if version_ok && checksum_ok && installed.version == bundle.version {
                return Ok(installed);
            }
        }

        install_from_bundle(&bundle_dir, &install_dir).await?;
        let installed = read_kernel_paths(&install_dir).await?;
        validate_agent_version(
            &installed.version,
            &expected_agent,
            options.require_exact_agent_version,
        )?;
        validate_artifact_checksums(&installed).await?;
        return Ok(installed);
    }

    if let Ok(installed) = read_kernel_paths(&install_dir).await {
        validate_agent_version(
            &installed.version,
            &expected_agent,
            options.require_exact_agent_version,
        )?;
        validate_artifact_checksums(&installed).await?;
        return Ok(installed);
    }

    Err(LinuxError::MissingKernelArtifacts { dir: install_dir })
}

fn workspace_bundle_dir() -> Option<PathBuf> {
    workspace_bundle_dir_from_manifest_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
}

fn workspace_bundle_dir_from_manifest_dir(manifest_dir: &Path) -> Option<PathBuf> {
    let candidate = manifest_dir.join("../../linux/out");
    if looks_like_kernel_bundle_dir(&candidate) {
        std::fs::canonicalize(&candidate).ok().or(Some(candidate))
    } else {
        None
    }
}

fn looks_like_kernel_bundle_dir(dir: &Path) -> bool {
    [KERNEL_FILE, INITRAMFS_FILE, YOUKI_FILE, VERSION_FILE]
        .into_iter()
        .all(|name| dir.join(name).is_file())
}

async fn install_from_bundle(bundle_dir: &Path, install_dir: &Path) -> Result<(), LinuxError> {
    let bundle = read_kernel_paths(bundle_dir).await?;
    let version_path = bundle_dir.join(VERSION_FILE);

    if let Some(parent) = install_dir.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut staging_name = OsString::from(
        install_dir
            .file_name()
            .ok_or_else(|| LinuxError::InvalidConfig("invalid install directory".to_string()))?,
    );
    staging_name.push(".staging");
    let staging = install_dir.with_file_name(staging_name);

    if tokio::fs::metadata(&staging).await.is_ok() {
        tokio::fs::remove_dir_all(&staging).await?;
    }

    tokio::fs::create_dir_all(&staging).await?;
    tokio::fs::copy(&bundle.kernel, staging.join(KERNEL_FILE)).await?;
    tokio::fs::copy(&bundle.initramfs, staging.join(INITRAMFS_FILE)).await?;
    tokio::fs::copy(&bundle.youki, staging.join(YOUKI_FILE)).await?;
    tokio::fs::copy(version_path, staging.join(VERSION_FILE)).await?;

    if tokio::fs::metadata(install_dir).await.is_ok() {
        tokio::fs::remove_dir_all(install_dir).await?;
    }

    tokio::fs::rename(&staging, install_dir).await?;
    Ok(())
}

fn validate_agent_version(
    version: &KernelVersion,
    expected_agent: &str,
    require_exact_agent_version: bool,
) -> Result<(), LinuxError> {
    if !require_exact_agent_version {
        return Ok(());
    }
    if version.agent != expected_agent {
        return Err(LinuxError::VersionMismatch {
            expected: expected_agent.to_string(),
            found: version.agent.clone(),
        });
    }
    Ok(())
}

async fn validate_artifact_checksums(paths: &KernelPaths) -> Result<(), LinuxError> {
    if let Some(expected) = paths.version.sha256_vmlinux.as_deref() {
        validate_file_checksum(&paths.kernel, KERNEL_FILE, expected).await?;
    }

    if let Some(expected) = paths.version.sha256_initramfs.as_deref() {
        validate_file_checksum(&paths.initramfs, INITRAMFS_FILE, expected).await?;
    }

    if let Some(expected) = paths.version.sha256_youki.as_deref() {
        validate_file_checksum(&paths.youki, YOUKI_FILE, expected).await?;
    }

    Ok(())
}

async fn validate_file_checksum(
    path: &Path,
    artifact: &str,
    expected_sha256: &str,
) -> Result<(), LinuxError> {
    let found = sha256_file(path).await?;
    let expected = expected_sha256.trim().to_ascii_lowercase();

    if found != expected {
        return Err(LinuxError::ArtifactChecksumMismatch {
            artifact: artifact.to_string(),
            path: path.display().to_string(),
            expected,
            found,
        });
    }

    Ok(())
}

async fn sha256_file(path: &Path) -> Result<String, LinuxError> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];

    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

async fn read_kernel_paths(dir: &Path) -> Result<KernelPaths, LinuxError> {
    let kernel = dir.join(KERNEL_FILE);
    let initramfs = dir.join(INITRAMFS_FILE);
    let youki = dir.join(YOUKI_FILE);
    let version_path = dir.join(VERSION_FILE);

    if tokio::fs::metadata(&kernel).await.is_err()
        || tokio::fs::metadata(&initramfs).await.is_err()
        || tokio::fs::metadata(&youki).await.is_err()
        || tokio::fs::metadata(&version_path).await.is_err()
    {
        return Err(LinuxError::MissingKernelArtifacts {
            dir: dir.to_path_buf(),
        });
    }

    let version_text = tokio::fs::read_to_string(version_path).await?;
    let version: KernelVersion = serde_json::from_str(&version_text)?;

    Ok(KernelPaths {
        kernel,
        initramfs,
        youki,
        version,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::*;

    fn sample_version(agent: String) -> KernelVersion {
        KernelVersion {
            kernel: "6.12.11".to_string(),
            busybox: "1.37.0".to_string(),
            agent,
            youki: "0.5.7".to_string(),
            built: None,
            sha256_vmlinux: None,
            sha256_initramfs: None,
            sha256_youki: None,
        }
    }

    fn sha256(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }

    async fn write_artifacts(dir: &Path, agent_version: String) {
        write_artifacts_with_checksums(dir, agent_version, false).await;
    }

    async fn write_artifacts_with_checksums(
        dir: &Path,
        agent_version: String,
        include_checksums: bool,
    ) {
        const KERNEL_BYTES: &[u8] = b"kernel";
        const INITRAMFS_BYTES: &[u8] = b"initramfs";
        const YOUKI_BYTES: &[u8] = b"youki";

        tokio::fs::create_dir_all(dir).await.expect("mkdir");
        tokio::fs::write(dir.join(KERNEL_FILE), KERNEL_BYTES)
            .await
            .expect("kernel");
        tokio::fs::write(dir.join(INITRAMFS_FILE), INITRAMFS_BYTES)
            .await
            .expect("initramfs");
        tokio::fs::write(dir.join(YOUKI_FILE), YOUKI_BYTES)
            .await
            .expect("youki");

        let mut version = sample_version(agent_version);
        if include_checksums {
            version.sha256_vmlinux = Some(sha256(KERNEL_BYTES));
            version.sha256_initramfs = Some(sha256(INITRAMFS_BYTES));
            version.sha256_youki = Some(sha256(YOUKI_BYTES));
        }

        let json = serde_json::to_string_pretty(&version).expect("json");
        tokio::fs::write(dir.join(VERSION_FILE), json)
            .await
            .expect("version");
    }

    #[tokio::test]
    async fn ensure_kernel_uses_installed_artifacts() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("linux");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts(&install, expected.clone()).await;

        let paths = ensure_kernel_with_options(EnsureKernelOptions {
            install_dir: Some(install.clone()),
            bundle_dir: None,
            require_exact_agent_version: true,
        })
        .await
        .expect("ensure kernel");

        assert_eq!(paths.version.agent, expected);
        assert_eq!(paths.kernel, install.join(KERNEL_FILE));
        assert_eq!(paths.initramfs, install.join(INITRAMFS_FILE));
        assert_eq!(paths.youki, install.join(YOUKI_FILE));
    }

    #[tokio::test]
    async fn ensure_kernel_installs_from_bundle_when_missing() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("install");
        let bundle = temp.path().join("bundle");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts(&bundle, expected.clone()).await;

        let paths = ensure_kernel_with_options(EnsureKernelOptions {
            install_dir: Some(install.clone()),
            bundle_dir: Some(bundle),
            require_exact_agent_version: true,
        })
        .await
        .expect("ensure kernel from bundle");

        assert_eq!(paths.version.agent, expected);
        assert!(install.join(KERNEL_FILE).exists());
        assert!(install.join(INITRAMFS_FILE).exists());
        assert!(install.join(YOUKI_FILE).exists());
        assert!(install.join(VERSION_FILE).exists());
    }

    #[tokio::test]
    async fn ensure_kernel_rejects_mismatched_agent_version() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("linux");
        write_artifacts(&install, "0.0.0".to_string()).await;

        let err = ensure_kernel_with_options(EnsureKernelOptions {
            install_dir: Some(install),
            bundle_dir: None,
            require_exact_agent_version: true,
        })
        .await
        .expect_err("must fail version mismatch");

        assert!(matches!(err, LinuxError::VersionMismatch { .. }));
    }

    #[tokio::test]
    async fn ensure_kernel_reinstalls_when_bundle_version_differs() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("install");
        let bundle = temp.path().join("bundle");
        let expected = env!("CARGO_PKG_VERSION").to_string();

        write_artifacts(&install, expected.clone()).await;
        write_artifacts(&bundle, expected.clone()).await;

        let mut bundle_version = sample_version(expected);
        bundle_version.built = Some("2026-02-18T00:00:00Z".to_string());
        let bundle_json = serde_json::to_string_pretty(&bundle_version).expect("json");
        tokio::fs::write(bundle.join(VERSION_FILE), bundle_json)
            .await
            .expect("write bundle version");

        let paths = ensure_kernel_with_options(EnsureKernelOptions {
            install_dir: Some(install.clone()),
            bundle_dir: Some(bundle),
            require_exact_agent_version: true,
        })
        .await
        .expect("ensure kernel from newer bundle");

        assert_eq!(paths.version.built.as_deref(), Some("2026-02-18T00:00:00Z"));
    }

    #[tokio::test]
    async fn ensure_kernel_rejects_bad_checksum_without_bundle() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("linux");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts_with_checksums(&install, expected, true).await;

        let mut version: KernelVersion = serde_json::from_str(
            &tokio::fs::read_to_string(install.join(VERSION_FILE))
                .await
                .expect("read version"),
        )
        .expect("parse version");
        version.sha256_vmlinux = Some("deadbeef".to_string());
        tokio::fs::write(
            install.join(VERSION_FILE),
            serde_json::to_string_pretty(&version).expect("version json"),
        )
        .await
        .expect("write version");

        let err = ensure_kernel_with_options(EnsureKernelOptions {
            install_dir: Some(install),
            bundle_dir: None,
            require_exact_agent_version: true,
        })
        .await
        .expect_err("must fail checksum mismatch");

        assert!(matches!(err, LinuxError::ArtifactChecksumMismatch { .. }));
    }

    #[tokio::test]
    async fn ensure_kernel_rejects_bad_youki_checksum_without_bundle() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("linux");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts_with_checksums(&install, expected, true).await;

        let mut version: KernelVersion = serde_json::from_str(
            &tokio::fs::read_to_string(install.join(VERSION_FILE))
                .await
                .expect("read version"),
        )
        .expect("parse version");
        version.sha256_youki = Some("beadfeed".to_string());
        tokio::fs::write(
            install.join(VERSION_FILE),
            serde_json::to_string_pretty(&version).expect("version json"),
        )
        .await
        .expect("write version");

        let err = ensure_kernel_with_options(EnsureKernelOptions {
            install_dir: Some(install),
            bundle_dir: None,
            require_exact_agent_version: true,
        })
        .await
        .expect_err("must fail checksum mismatch");

        assert!(matches!(
            err,
            LinuxError::ArtifactChecksumMismatch { ref artifact, .. } if artifact == YOUKI_FILE
        ));
    }

    #[tokio::test]
    async fn ensure_kernel_reinstalls_when_installed_checksum_is_bad() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("install");
        let bundle = temp.path().join("bundle");
        let expected = env!("CARGO_PKG_VERSION").to_string();

        write_artifacts_with_checksums(&bundle, expected.clone(), true).await;
        write_artifacts_with_checksums(&install, expected, true).await;

        tokio::fs::write(install.join(KERNEL_FILE), b"corrupt-kernel")
            .await
            .expect("corrupt installed kernel");

        let paths = ensure_kernel_with_options(EnsureKernelOptions {
            install_dir: Some(install.clone()),
            bundle_dir: Some(bundle),
            require_exact_agent_version: true,
        })
        .await
        .expect("ensure kernel should reinstall from valid bundle");

        let installed_kernel = tokio::fs::read(install.join(KERNEL_FILE))
            .await
            .expect("read installed kernel");
        assert_eq!(installed_kernel, b"kernel");
        assert_eq!(paths.version.sha256_vmlinux, Some(sha256(b"kernel")));
        assert_eq!(paths.version.sha256_youki, Some(sha256(b"youki")));
    }

    #[tokio::test]
    async fn workspace_bundle_dir_discovery_uses_manifest_relative_linux_out() {
        let temp = tempdir().expect("tempdir");
        let manifest_dir = temp.path().join("crates/vz-linux");
        let bundle = temp.path().join("linux/out");
        tokio::fs::create_dir_all(&manifest_dir)
            .await
            .expect("manifest dir");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts(&bundle, expected).await;

        let discovered = workspace_bundle_dir_from_manifest_dir(&manifest_dir);
        assert_eq!(
            discovered
                .as_deref()
                .and_then(|path| path.canonicalize().ok()),
            bundle.canonicalize().ok()
        );
    }

    #[tokio::test]
    async fn workspace_bundle_dir_discovery_ignores_incomplete_bundle_dir() {
        let temp = tempdir().expect("tempdir");
        let manifest_dir = temp.path().join("crates/vz-linux");
        let bundle = temp.path().join("linux/out");
        tokio::fs::create_dir_all(&manifest_dir)
            .await
            .expect("manifest dir");
        tokio::fs::create_dir_all(&bundle)
            .await
            .expect("bundle dir");
        tokio::fs::write(bundle.join(KERNEL_FILE), b"kernel")
            .await
            .expect("kernel");

        let discovered = workspace_bundle_dir_from_manifest_dir(&manifest_dir);
        assert!(discovered.is_none());
    }
}
