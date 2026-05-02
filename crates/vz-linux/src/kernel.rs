use std::collections::BTreeSet;
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
    /// Kernel build profile, such as `developer` or `container`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// Security posture descriptor for this artifact profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_profile: Option<String>,
    /// BusyBox version used in initramfs.
    pub busybox: String,
    /// Guest-agent version used in initramfs.
    pub agent: String,
    /// Guest-agent protocol compatibility revision used for host startup gating.
    pub agent_protocol_revision: Option<u32>,
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
    /// Optional capability declarations for this kernel bundle.
    ///
    /// Older bundles predate this field; callers that use
    /// [`ensure_kernel_bundle`] fall back to the capability set implied by the
    /// requested [`KernelFlavor`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<BTreeSet<KernelCapability>>,
}

/// Kernel feature that external callers may require before booting a guest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KernelCapability {
    /// Virtio-vsock device support.
    Vsock,
    /// VirtioFS filesystem support.
    Virtiofs,
    /// `console=hvc0` serial console support.
    Hvc0Serial,
    /// Ext4 root filesystem support.
    Ext4Root,
    /// OverlayFS support for writable container roots.
    Overlayfs,
    /// Network namespace support.
    Netns,
    /// Seccomp syscall filtering support.
    Seccomp,
    /// `io_uring` asynchronous I/O interface support.
    IoUring,
    /// Nested virtualization support through `/dev/kvm`.
    NestedVirt,
    /// TUN/TAP support through `/dev/net/tun`.
    Tun,
    /// Btrfs subvolume/snapshot support for sandbox checkpointing.
    BtrfsSnapshots,
    /// Hardened container sandbox kernel profile.
    ContainerSandbox,
}

impl KernelCapability {
    /// Stable string identifier for diagnostics and metadata.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Vsock => "vsock",
            Self::Virtiofs => "virtiofs",
            Self::Hvc0Serial => "hvc0_serial",
            Self::Ext4Root => "ext4_root",
            Self::Overlayfs => "overlayfs",
            Self::Netns => "netns",
            Self::Seccomp => "seccomp",
            Self::IoUring => "io_uring",
            Self::NestedVirt => "nested_virt",
            Self::Tun => "tun",
            Self::BtrfsSnapshots => "btrfs_snapshots",
            Self::ContainerSandbox => "container_sandbox",
        }
    }
}

/// Versioned kernel flavor provided by `vz-linux`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelFlavor {
    /// Apple Virtualization.framework compatible Linux/aarch64 kernel.
    LinuxAarch64Vz,
}

/// Caller-controlled options for resolving a versioned kernel bundle.
#[derive(Debug, Clone)]
pub struct KernelBundleOptions {
    /// Kernel flavor to resolve.
    pub flavor: KernelFlavor,
    /// Install/cache directory. When unset, defaults to [`default_linux_dir`].
    pub install_dir: Option<PathBuf>,
    /// Optional predownloaded bundle directory to install from.
    ///
    /// If unset, `VZ_LINUX_BUNDLE_DIR` and workspace-relative `linux/out`
    /// discovery behave the same as [`ensure_kernel_with_options`].
    pub bundle_dir: Option<PathBuf>,
    /// Require strict `vz-guest-agent` version/protocol compatibility.
    ///
    /// Callers that only need the kernel image, such as direct-rootfs guests,
    /// can set this to `false`.
    pub require_exact_agent_version: bool,
    /// Capabilities the resolved kernel bundle must declare.
    pub required_capabilities: BTreeSet<KernelCapability>,
}

impl Default for KernelBundleOptions {
    fn default() -> Self {
        Self {
            flavor: KernelFlavor::LinuxAarch64Vz,
            install_dir: None,
            bundle_dir: None,
            require_exact_agent_version: true,
            required_capabilities: default_vz_linux_kernel_capabilities(),
        }
    }
}

/// Resolved kernel bundle with caller-facing metadata.
#[derive(Debug, Clone)]
pub struct KernelBundle {
    /// Kernel flavor that was resolved.
    pub flavor: KernelFlavor,
    /// Linux kernel image path.
    pub kernel: PathBuf,
    /// Optional initramfs path from the bundle.
    pub initramfs: Option<PathBuf>,
    /// Optional pinned `youki` runtime path from the bundle.
    pub youki: Option<PathBuf>,
    /// Parsed artifact metadata from `version.json`.
    pub version: KernelVersion,
    /// Declared kernel capabilities after flavor fallback.
    pub capabilities: BTreeSet<KernelCapability>,
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
    /// Require strict host/guest compatibility checks from `version.json`.
    ///
    /// Enforces both:
    /// - `agent == CARGO_PKG_VERSION`
    /// - `agent_protocol_revision == vz_agent_proto::AGENT_PROTOCOL_REVISION`
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

/// Ensure a versioned Linux kernel bundle is installed and satisfies caller requirements.
///
/// This is the public resolver for consumers that need explicit control over
/// where `vz`'s Linux kernel artifacts land. The returned bundle can be passed
/// directly to `VmConfigBuilder::boot_linux`; callers that boot their own rootfs
/// may ignore the optional initramfs and `youki` paths.
pub async fn ensure_kernel_bundle(
    options: KernelBundleOptions,
) -> Result<KernelBundle, LinuxError> {
    let KernelBundleOptions {
        flavor,
        install_dir,
        bundle_dir,
        require_exact_agent_version,
        required_capabilities,
    } = options;

    let paths = ensure_kernel_with_options(EnsureKernelOptions {
        install_dir,
        bundle_dir,
        require_exact_agent_version,
    })
    .await?;
    let capabilities = capabilities_for_version(&paths.version, flavor);
    validate_required_capabilities(&capabilities, &required_capabilities)?;

    Ok(KernelBundle {
        flavor,
        kernel: paths.kernel,
        initramfs: Some(paths.initramfs),
        youki: Some(paths.youki),
        version: paths.version,
        capabilities,
    })
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
    let expected_protocol_revision = vz_agent_proto::AGENT_PROTOCOL_REVISION;
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
            expected_protocol_revision,
            options.require_exact_agent_version,
        )?;
        validate_artifact_checksums(&bundle).await?;

        if let Ok(installed) = read_kernel_paths(&install_dir).await {
            let version_ok = validate_agent_version(
                &installed.version,
                &expected_agent,
                expected_protocol_revision,
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
            expected_protocol_revision,
            options.require_exact_agent_version,
        )?;
        validate_artifact_checksums(&installed).await?;
        return Ok(installed);
    }

    if let Ok(installed) = read_kernel_paths(&install_dir).await {
        validate_agent_version(
            &installed.version,
            &expected_agent,
            expected_protocol_revision,
            options.require_exact_agent_version,
        )?;
        validate_artifact_checksums(&installed).await?;
        return Ok(installed);
    }

    Err(LinuxError::MissingKernelArtifacts { dir: install_dir })
}

/// Capabilities expected from the current `vz-linux` Apple VZ kernel flavor.
pub fn default_vz_linux_kernel_capabilities() -> BTreeSet<KernelCapability> {
    [
        KernelCapability::Vsock,
        KernelCapability::Virtiofs,
        KernelCapability::Hvc0Serial,
        KernelCapability::Ext4Root,
    ]
    .into_iter()
    .collect()
}

fn capabilities_for_version(
    version: &KernelVersion,
    flavor: KernelFlavor,
) -> BTreeSet<KernelCapability> {
    version
        .capabilities
        .clone()
        .unwrap_or_else(|| match flavor {
            KernelFlavor::LinuxAarch64Vz => default_vz_linux_kernel_capabilities(),
        })
}

fn validate_required_capabilities(
    capabilities: &BTreeSet<KernelCapability>,
    required: &BTreeSet<KernelCapability>,
) -> Result<(), LinuxError> {
    let missing = required
        .difference(capabilities)
        .map(|capability| capability.as_str().to_string())
        .collect::<Vec<_>>();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(LinuxError::MissingKernelCapabilities { missing })
    }
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
    expected_protocol_revision: u32,
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
    let found_protocol_revision =
        version
            .agent_protocol_revision
            .ok_or(LinuxError::MissingProtocolRevision {
                expected: expected_protocol_revision,
            })?;
    if found_protocol_revision != expected_protocol_revision {
        return Err(LinuxError::ProtocolRevisionMismatch {
            expected: expected_protocol_revision,
            found: found_protocol_revision,
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
            profile: None,
            security_profile: None,
            busybox: "1.37.0".to_string(),
            agent,
            agent_protocol_revision: Some(vz_agent_proto::AGENT_PROTOCOL_REVISION),
            youki: "0.5.7".to_string(),
            built: None,
            sha256_vmlinux: None,
            sha256_initramfs: None,
            sha256_youki: None,
            capabilities: None,
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
    async fn ensure_kernel_bundle_uses_caller_install_dir_and_returns_capabilities() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("virgil-controlled/linux/vz-0.1.0");
        let bundle = temp.path().join("bundle");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts_with_checksums(&bundle, expected.clone(), true).await;

        let resolved = ensure_kernel_bundle(KernelBundleOptions {
            install_dir: Some(install.clone()),
            bundle_dir: Some(bundle),
            ..KernelBundleOptions::default()
        })
        .await
        .expect("ensure kernel bundle");

        assert_eq!(resolved.flavor, KernelFlavor::LinuxAarch64Vz);
        assert_eq!(resolved.version.agent, expected);
        assert_eq!(resolved.kernel, install.join(KERNEL_FILE));
        assert_eq!(
            resolved.initramfs.as_deref(),
            Some(install.join(INITRAMFS_FILE).as_path())
        );
        assert_eq!(
            resolved.youki.as_deref(),
            Some(install.join(YOUKI_FILE).as_path())
        );
        assert!(resolved.capabilities.contains(&KernelCapability::Virtiofs));
        assert!(resolved.capabilities.contains(&KernelCapability::Vsock));
        assert!(
            resolved
                .capabilities
                .contains(&KernelCapability::Hvc0Serial)
        );
        assert!(resolved.capabilities.contains(&KernelCapability::Ext4Root));
    }

    #[tokio::test]
    async fn ensure_kernel_bundle_returns_declared_profile_metadata_and_capabilities() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("container/linux/vz-0.1.0");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts_with_checksums(&install, expected, true).await;

        let declared_capabilities = [
            KernelCapability::Vsock,
            KernelCapability::Virtiofs,
            KernelCapability::Hvc0Serial,
            KernelCapability::Ext4Root,
            KernelCapability::Overlayfs,
            KernelCapability::Netns,
            KernelCapability::Seccomp,
            KernelCapability::IoUring,
            KernelCapability::BtrfsSnapshots,
            KernelCapability::ContainerSandbox,
        ]
        .into_iter()
        .collect();

        let mut version: KernelVersion = serde_json::from_str(
            &tokio::fs::read_to_string(install.join(VERSION_FILE))
                .await
                .expect("read version"),
        )
        .expect("parse version");
        version.profile = Some("container".to_string());
        version.security_profile = Some("container-hardened".to_string());
        version.capabilities = Some(declared_capabilities);
        tokio::fs::write(
            install.join(VERSION_FILE),
            serde_json::to_string_pretty(&version).expect("version json"),
        )
        .await
        .expect("write version");

        let required_capabilities = [
            KernelCapability::Overlayfs,
            KernelCapability::Netns,
            KernelCapability::Seccomp,
            KernelCapability::IoUring,
            KernelCapability::BtrfsSnapshots,
            KernelCapability::ContainerSandbox,
        ]
        .into_iter()
        .collect();

        let resolved = ensure_kernel_bundle(KernelBundleOptions {
            install_dir: Some(install),
            bundle_dir: None,
            required_capabilities,
            ..KernelBundleOptions::default()
        })
        .await
        .expect("ensure profile kernel bundle");

        assert_eq!(resolved.version.profile.as_deref(), Some("container"));
        assert_eq!(
            resolved.version.security_profile.as_deref(),
            Some("container-hardened")
        );
        assert!(
            resolved
                .capabilities
                .contains(&KernelCapability::ContainerSandbox)
        );
        assert!(
            resolved
                .capabilities
                .contains(&KernelCapability::BtrfsSnapshots)
        );
        assert!(resolved.capabilities.contains(&KernelCapability::IoUring));
    }

    #[tokio::test]
    async fn ensure_kernel_bundle_can_skip_guest_agent_version_validation() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("install");
        write_artifacts(&install, "not-this-crate".to_string()).await;

        let resolved = ensure_kernel_bundle(KernelBundleOptions {
            install_dir: Some(install.clone()),
            bundle_dir: None,
            require_exact_agent_version: false,
            ..KernelBundleOptions::default()
        })
        .await
        .expect("direct-rootfs callers can opt out of guest-agent version checks");

        assert_eq!(resolved.kernel, install.join(KERNEL_FILE));
        assert_eq!(resolved.version.agent, "not-this-crate");
    }

    #[tokio::test]
    async fn ensure_kernel_bundle_rejects_missing_required_capability() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("linux");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts(&install, expected).await;

        let mut version: KernelVersion = serde_json::from_str(
            &tokio::fs::read_to_string(install.join(VERSION_FILE))
                .await
                .expect("read version"),
        )
        .expect("parse version");
        version.capabilities = Some([KernelCapability::Vsock].into_iter().collect());
        tokio::fs::write(
            install.join(VERSION_FILE),
            serde_json::to_string_pretty(&version).expect("version json"),
        )
        .await
        .expect("write version");

        let err = ensure_kernel_bundle(KernelBundleOptions {
            install_dir: Some(install),
            bundle_dir: None,
            ..KernelBundleOptions::default()
        })
        .await
        .expect_err("must fail missing virtiofs/hvc0/ext4 capabilities");

        assert!(
            matches!(err, LinuxError::MissingKernelCapabilities { ref missing }
                if missing.contains(&KernelCapability::Virtiofs.as_str().to_string()))
        );
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
    async fn ensure_kernel_rejects_missing_protocol_revision() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("linux");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts(&install, expected).await;

        let mut version: KernelVersion = serde_json::from_str(
            &tokio::fs::read_to_string(install.join(VERSION_FILE))
                .await
                .expect("read version"),
        )
        .expect("parse version");
        version.agent_protocol_revision = None;
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
        .expect_err("must fail missing protocol revision");

        assert!(matches!(err, LinuxError::MissingProtocolRevision { .. }));
    }

    #[tokio::test]
    async fn ensure_kernel_rejects_mismatched_protocol_revision() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("linux");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts(&install, expected).await;

        let mut version: KernelVersion = serde_json::from_str(
            &tokio::fs::read_to_string(install.join(VERSION_FILE))
                .await
                .expect("read version"),
        )
        .expect("parse version");
        version.agent_protocol_revision = Some(vz_agent_proto::AGENT_PROTOCOL_REVISION + 1);
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
        .expect_err("must fail protocol revision mismatch");

        assert!(matches!(err, LinuxError::ProtocolRevisionMismatch { .. }));
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
