use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::LinuxError;
use crate::developer_probe::{
    DeveloperProbeMetadata, VerifiedDeveloperProbe, verify_developer_probe,
};

const KERNEL_FILE: &str = "vmlinux";
const INITRAMFS_FILE: &str = "initramfs.img";
const YOUKI_FILE: &str = "youki";
const VERSION_FILE: &str = "version.json";
const MAX_VERSION_METADATA_BYTES: u64 = 64 * 1024;
const MAX_VERSION_VALUE_BYTES: usize = 256;
const VERIFIED_BUNDLE_DIGEST_DOMAIN: &[u8] = b"vz.linux.kernel-bundle.v1\0";

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
    /// Optional digest-bound offline Developer startup rootfs; absent in legacy
    /// and Hardened bundles. Absence never certifies Developer readiness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub developer_probe: Option<DeveloperProbeMetadata>,
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
    /// Cgroup BPF attachment backed by the BPF syscall.
    CgroupBpf,
    /// User namespace support, required by youki's runtime preflight.
    UserNs,
    /// Nested virtualization support through `/dev/kvm`.
    NestedVirt,
    /// TUN/TAP support through `/dev/net/tun`.
    Tun,
    /// Btrfs subvolume/snapshot support for sandbox checkpointing.
    BtrfsSnapshots,
    /// Device-mapper core support for mapped block devices.
    DeviceMapper,
    /// dm-crypt target support for LUKS-backed volumes.
    DmCrypt,
    /// Kernel NFS server support for workspace/frontend exports.
    Nfsd,
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
            Self::CgroupBpf => "cgroup_bpf",
            Self::UserNs => "user_ns",
            Self::NestedVirt => "nested_virt",
            Self::Tun => "tun",
            Self::BtrfsSnapshots => "btrfs_snapshots",
            Self::DeviceMapper => "device_mapper",
            Self::DmCrypt => "dm_crypt",
            Self::Nfsd => "nfsd",
            Self::ContainerSandbox => "container_sandbox",
        }
    }
}

/// Security and capability profile for bundled `vz` Linux kernels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KernelProfile {
    /// Broad developer kernel with nested virtualization and TUN/TAP support.
    Developer,
    /// Constrained container/sandbox kernel without nested virtualization.
    Container,
}

impl KernelProfile {
    /// Stable string identifier used in release artifact names and metadata.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Developer => "developer",
            Self::Container => "container",
        }
    }

    /// Stable profile-specific bundle directory environment variable.
    pub const fn bundle_dir_env_var(self) -> &'static str {
        match self {
            Self::Developer => "VZ_LINUX_DEVELOPER_BUNDLE_DIR",
            Self::Container => "VZ_LINUX_CONTAINER_BUNDLE_DIR",
        }
    }

    /// Security posture descriptor written to `version.json`.
    pub const fn security_profile(self) -> &'static str {
        match self {
            Self::Developer => "developer-nested-virt",
            Self::Container => "container-hardened",
        }
    }

    /// Expected capability contract for this profile.
    pub fn default_capabilities(self) -> BTreeSet<KernelCapability> {
        default_vz_linux_kernel_profile_capabilities(self)
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
    /// Optional kernel profile to select and validate.
    ///
    /// When set, profile-specific bundle env vars and workspace discovery are
    /// used, and the resolved metadata must declare the requested profile.
    pub profile: Option<KernelProfile>,
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
            profile: None,
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

/// Content identity for a read-only verified Linux appliance bundle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KernelBundleArtifactIdentity {
    /// SHA256 of the Linux kernel image.
    pub kernel_sha256: String,
    /// SHA256 of the initramfs image.
    pub initramfs_sha256: String,
    /// SHA256 of the pinned `youki` executable.
    pub youki_sha256: String,
    /// SHA256 of the exact, unparsed `version.json` bytes.
    pub version_sha256: String,
    /// Versioned, domain-separated aggregate digest of all four hashes.
    ///
    /// This is SHA256 over `vz.linux.kernel-bundle.v1\0`, followed in order by
    /// `kernel`, `initramfs`, `youki`, and `version`; each field is framed as
    /// its ASCII name, NUL, its 64-byte lowercase hexadecimal hash, then NUL.
    /// An optional Developer startup-probe archive is verified against its
    /// checksum in those exact version bytes, so it is transitively bound
    /// without changing the identity framing of legacy four-file bundles.
    pub digest: String,
}

/// Explicit, verified Linux appliance bundle selected without installation or discovery.
#[derive(Debug, Clone)]
pub struct VerifiedKernelBundle {
    /// Caller-supplied bundle directory. It is deliberately not canonicalized.
    pub bundle_dir: PathBuf,
    /// Exact artifact paths and parsed metadata from the supplied directory.
    pub paths: KernelPaths,
    /// Explicit capability declarations validated for the selected profile.
    pub capabilities: BTreeSet<KernelCapability>,
    /// Artifact-level and aggregate content identity.
    pub artifact_identity: KernelBundleArtifactIdentity,
    /// Optional extra archive authenticated by the exact version metadata hash.
    pub developer_probe: Option<VerifiedDeveloperProbe>,
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

/// Resolve the default install directory for a kernel profile.
pub fn default_linux_profile_dir(profile: KernelProfile) -> Result<PathBuf, LinuxError> {
    Ok(default_linux_dir()?.join(profile.as_str()))
}

/// Ensure Linux kernel artifacts are installed and compatible.
pub async fn ensure_kernel() -> Result<KernelPaths, LinuxError> {
    ensure_kernel_with_options(EnsureKernelOptions::default()).await
}

/// Ensure Linux kernel artifacts for a specific kernel profile are installed and compatible.
pub async fn ensure_kernel_profile(profile: KernelProfile) -> Result<KernelPaths, LinuxError> {
    ensure_kernel_profile_with_options(profile, EnsureKernelOptions::default()).await
}

/// Ensure Linux kernel artifacts for a specific profile are installed and compatible.
///
/// This resolver validates both profile metadata and the default capability
/// contract for the selected profile. Use `ensure_kernel_bundle` with
/// `KernelBundleOptions::profile` when a caller also needs the declared
/// capability set returned.
pub async fn ensure_kernel_profile_with_options(
    profile: KernelProfile,
    options: EnsureKernelOptions,
) -> Result<KernelPaths, LinuxError> {
    let use_default_install_dir = options.install_dir.is_none();
    let install_dir = match options.install_dir {
        Some(path) => path,
        None => default_linux_profile_dir(profile)?,
    };
    let bundle_dir = profile_bundle_dir(profile, options.bundle_dir);
    let selected_bundle_dir = bundle_dir.is_some();
    let require_exact_agent_version = options.require_exact_agent_version;

    let paths = ensure_kernel_with_resolved_options(ResolvedKernelOptions {
        install_dir: install_dir.clone(),
        bundle_dir: bundle_dir.clone(),
        workspace_profile: use_default_install_dir.then_some(profile),
        expected_profile: Some(profile),
        require_exact_agent_version,
    })
    .await;

    let paths = match paths {
        Err(LinuxError::MissingKernelArtifacts { .. })
            if use_default_install_dir
                && !selected_bundle_dir
                && profile == KernelProfile::Developer =>
        {
            ensure_kernel_with_resolved_options(ResolvedKernelOptions {
                install_dir: default_linux_dir()?,
                bundle_dir,
                workspace_profile: use_default_install_dir.then_some(profile),
                expected_profile: Some(profile),
                require_exact_agent_version,
            })
            .await?
        }
        other => other?,
    };

    let capabilities = capabilities_for_version(&paths.version, KernelFlavor::LinuxAarch64Vz);
    validate_required_capabilities(&capabilities, &profile.default_capabilities())?;
    Ok(paths)
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
        profile,
        require_exact_agent_version,
        mut required_capabilities,
    } = options;

    let ensure_options = EnsureKernelOptions {
        install_dir,
        bundle_dir,
        require_exact_agent_version,
    };
    let paths = match profile {
        Some(profile) => {
            required_capabilities.extend(profile.default_capabilities());
            ensure_kernel_profile_with_options(profile, ensure_options).await?
        }
        None => ensure_kernel_with_options(ensure_options).await?,
    };
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

/// Verify one explicitly selected Linux appliance bundle without writing to disk.
///
/// This function performs no environment lookup, workspace fallback, cache install,
/// or filesystem mutation. The supplied directory must be from a trusted read-only
/// catalog: metadata checks and symlink rejection do not make a same-UID, mutable
/// directory immune to races after this function returns.
pub async fn verify_kernel_bundle_read_only(
    bundle_dir: &Path,
    profile: KernelProfile,
) -> Result<VerifiedKernelBundle, LinuxError> {
    require_directory_without_symlink(bundle_dir).await?;

    let kernel = bundle_dir.join(KERNEL_FILE);
    let initramfs = bundle_dir.join(INITRAMFS_FILE);
    let youki = bundle_dir.join(YOUKI_FILE);
    let version_path = bundle_dir.join(VERSION_FILE);
    for (artifact, path) in [
        (KERNEL_FILE, kernel.as_path()),
        (INITRAMFS_FILE, initramfs.as_path()),
        (YOUKI_FILE, youki.as_path()),
        (VERSION_FILE, version_path.as_path()),
    ] {
        require_regular_file_without_symlink(bundle_dir, artifact, path).await?;
    }

    let raw_version = read_bounded_version_metadata(&version_path).await?;
    let version: KernelVersion = serde_json::from_slice(&raw_version)?;
    validate_verified_version_metadata(&version, profile)?;
    let capabilities = version.capabilities.clone().ok_or_else(|| {
        LinuxError::InvalidConfig(format!(
            "{VERSION_FILE} must explicitly declare capabilities for profile `{}`",
            profile.as_str()
        ))
    })?;
    validate_required_capabilities(&capabilities, &profile.default_capabilities())?;

    let expected_kernel =
        require_canonical_checksum(KERNEL_FILE, version.sha256_vmlinux.as_deref())?;
    let expected_initramfs =
        require_canonical_checksum(INITRAMFS_FILE, version.sha256_initramfs.as_deref())?;
    let expected_youki = require_canonical_checksum(YOUKI_FILE, version.sha256_youki.as_deref())?;

    let kernel_sha256 = sha256_file(&kernel).await?;
    require_matching_checksum(KERNEL_FILE, &kernel, expected_kernel, &kernel_sha256)?;
    let initramfs_sha256 = sha256_file(&initramfs).await?;
    require_matching_checksum(
        INITRAMFS_FILE,
        &initramfs,
        expected_initramfs,
        &initramfs_sha256,
    )?;
    let youki_sha256 = sha256_file(&youki).await?;
    require_matching_checksum(YOUKI_FILE, &youki, expected_youki, &youki_sha256)?;
    let version_sha256 = sha256_bytes(&raw_version);
    let developer_probe = verify_developer_probe(bundle_dir, &version).await?;
    let digest = verified_bundle_digest(
        &kernel_sha256,
        &initramfs_sha256,
        &youki_sha256,
        &version_sha256,
    );

    Ok(VerifiedKernelBundle {
        bundle_dir: bundle_dir.to_path_buf(),
        paths: KernelPaths {
            kernel,
            initramfs,
            youki,
            version,
        },
        capabilities,
        artifact_identity: KernelBundleArtifactIdentity {
            kernel_sha256,
            initramfs_sha256,
            youki_sha256,
            version_sha256,
            digest,
        },
        developer_probe,
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
    ensure_kernel_with_resolved_options(ResolvedKernelOptions {
        install_dir,
        bundle_dir: generic_bundle_dir(options.bundle_dir),
        workspace_profile: should_probe_workspace_bundle.then_some(KernelProfile::Developer),
        expected_profile: None,
        require_exact_agent_version: options.require_exact_agent_version,
    })
    .await
}

struct ResolvedKernelOptions {
    install_dir: PathBuf,
    bundle_dir: Option<PathBuf>,
    workspace_profile: Option<KernelProfile>,
    expected_profile: Option<KernelProfile>,
    require_exact_agent_version: bool,
}

async fn ensure_kernel_with_resolved_options(
    options: ResolvedKernelOptions,
) -> Result<KernelPaths, LinuxError> {
    let ResolvedKernelOptions {
        install_dir,
        mut bundle_dir,
        workspace_profile,
        expected_profile,
        require_exact_agent_version,
    } = options;
    let expected_agent = env!("CARGO_PKG_VERSION").to_string();
    let expected_protocol_revision = vz_agent_proto::AGENT_PROTOCOL_REVISION;
    if bundle_dir.is_none() {
        bundle_dir = workspace_profile.and_then(workspace_bundle_dir);
    }

    if let Some(bundle_dir) = bundle_dir {
        let bundle = read_kernel_paths(&bundle_dir).await?;
        validate_kernel_metadata(
            &bundle.version,
            &expected_agent,
            expected_protocol_revision,
            require_exact_agent_version,
            expected_profile,
        )?;
        validate_artifact_checksums(&bundle).await?;

        if let Ok(installed) = read_kernel_paths(&install_dir).await {
            let version_ok = validate_kernel_metadata(
                &installed.version,
                &expected_agent,
                expected_protocol_revision,
                require_exact_agent_version,
                expected_profile,
            )
            .is_ok();
            let checksum_ok = validate_artifact_checksums(&installed).await.is_ok();

            if version_ok && checksum_ok && installed.version == bundle.version {
                return Ok(installed);
            }
        }

        install_from_bundle(&bundle_dir, &install_dir).await?;
        let installed = read_kernel_paths(&install_dir).await?;
        validate_kernel_metadata(
            &installed.version,
            &expected_agent,
            expected_protocol_revision,
            require_exact_agent_version,
            expected_profile,
        )?;
        validate_artifact_checksums(&installed).await?;
        return Ok(installed);
    }

    if let Ok(installed) = read_kernel_paths(&install_dir).await {
        validate_kernel_metadata(
            &installed.version,
            &expected_agent,
            expected_protocol_revision,
            require_exact_agent_version,
            expected_profile,
        )?;
        validate_artifact_checksums(&installed).await?;
        return Ok(installed);
    }

    Err(LinuxError::MissingKernelArtifacts { dir: install_dir })
}

async fn require_directory_without_symlink(dir: &Path) -> Result<(), LinuxError> {
    let metadata = match tokio::fs::symlink_metadata(dir).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(LinuxError::MissingKernelArtifacts {
                dir: dir.to_path_buf(),
            });
        }
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(LinuxError::InvalidConfig(format!(
            "explicit kernel bundle directory must be a non-symlink directory: {}",
            dir.display()
        )));
    }
    Ok(())
}

async fn require_regular_file_without_symlink(
    bundle_dir: &Path,
    artifact: &str,
    path: &Path,
) -> Result<(), LinuxError> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(LinuxError::MissingKernelArtifacts {
                dir: bundle_dir.to_path_buf(),
            });
        }
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(LinuxError::InvalidConfig(format!(
            "kernel bundle artifact `{artifact}` must be a non-symlink regular file: {}",
            path.display()
        )));
    }
    Ok(())
}

async fn read_bounded_version_metadata(path: &Path) -> Result<Vec<u8>, LinuxError> {
    let metadata = tokio::fs::symlink_metadata(path).await?;
    if metadata.len() == 0 || metadata.len() > MAX_VERSION_METADATA_BYTES {
        return Err(LinuxError::InvalidConfig(format!(
            "{VERSION_FILE} must contain 1..={MAX_VERSION_METADATA_BYTES} bytes"
        )));
    }
    let file = tokio::fs::File::open(path).await?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_VERSION_METADATA_BYTES + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_VERSION_METADATA_BYTES {
        return Err(LinuxError::InvalidConfig(format!(
            "{VERSION_FILE} must contain 1..={MAX_VERSION_METADATA_BYTES} bytes"
        )));
    }
    Ok(bytes)
}

fn validate_verified_version_metadata(
    version: &KernelVersion,
    profile: KernelProfile,
) -> Result<(), LinuxError> {
    validate_kernel_metadata(
        version,
        env!("CARGO_PKG_VERSION"),
        vz_agent_proto::AGENT_PROTOCOL_REVISION,
        true,
        Some(profile),
    )?;
    let security_profile = version.security_profile.as_deref().unwrap_or("<missing>");
    if security_profile != profile.security_profile() {
        return Err(LinuxError::InvalidConfig(format!(
            "kernel artifact security profile mismatch: expected {}, found {security_profile}",
            profile.security_profile()
        )));
    }
    for (field, value) in [
        ("kernel", version.kernel.as_str()),
        ("busybox", version.busybox.as_str()),
        ("agent", version.agent.as_str()),
        ("youki", version.youki.as_str()),
    ] {
        if value.trim().is_empty()
            || value.len() > MAX_VERSION_VALUE_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(LinuxError::InvalidConfig(format!(
                "kernel artifact metadata `{field}` must be nonblank, control-free, and at most {MAX_VERSION_VALUE_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

fn require_canonical_checksum<'a>(
    artifact: &str,
    checksum: Option<&'a str>,
) -> Result<&'a str, LinuxError> {
    let checksum = checksum.ok_or_else(|| {
        LinuxError::InvalidConfig(format!(
            "{VERSION_FILE} must declare sha256 for `{artifact}`"
        ))
    })?;
    if checksum.len() != 64
        || !checksum
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(LinuxError::InvalidConfig(format!(
            "{VERSION_FILE} sha256 for `{artifact}` must be exactly 64 lowercase hexadecimal characters"
        )));
    }
    Ok(checksum)
}

fn require_matching_checksum(
    artifact: &str,
    path: &Path,
    expected: &str,
    found: &str,
) -> Result<(), LinuxError> {
    if found == expected {
        Ok(())
    } else {
        Err(LinuxError::ArtifactChecksumMismatch {
            artifact: artifact.to_string(),
            path: path.display().to_string(),
            expected: expected.to_string(),
            found: found.to_string(),
        })
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Hash framing is exactly the domain bytes followed by four repetitions of
/// `field-name`, NUL, the fixed 64-byte lowercase hash, NUL, in the order below.
fn verified_bundle_digest(
    kernel_sha256: &str,
    initramfs_sha256: &str,
    youki_sha256: &str,
    version_sha256: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(VERIFIED_BUNDLE_DIGEST_DOMAIN);
    for (field, hash) in [
        ("kernel", kernel_sha256),
        ("initramfs", initramfs_sha256),
        ("youki", youki_sha256),
        ("version", version_sha256),
    ] {
        hasher.update(field.as_bytes());
        hasher.update([0]);
        hasher.update(hash.as_bytes());
        hasher.update([0]);
    }
    format!("sha256:{:x}", hasher.finalize())
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

/// Capabilities expected from a named `vz-linux` kernel profile.
pub fn default_vz_linux_kernel_profile_capabilities(
    profile: KernelProfile,
) -> BTreeSet<KernelCapability> {
    let mut capabilities = default_vz_linux_kernel_capabilities();
    capabilities.extend([
        KernelCapability::Overlayfs,
        KernelCapability::Netns,
        KernelCapability::Seccomp,
        KernelCapability::IoUring,
        KernelCapability::BtrfsSnapshots,
        KernelCapability::DeviceMapper,
        KernelCapability::DmCrypt,
    ]);
    match profile {
        KernelProfile::Developer => {
            capabilities.extend([
                KernelCapability::CgroupBpf,
                KernelCapability::UserNs,
                KernelCapability::NestedVirt,
                KernelCapability::Tun,
            ]);
        }
        KernelProfile::Container => {
            capabilities.insert(KernelCapability::CgroupBpf);
            capabilities.insert(KernelCapability::Nfsd);
            capabilities.insert(KernelCapability::ContainerSandbox);
        }
    }
    capabilities
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

fn generic_bundle_dir(bundle_dir: Option<PathBuf>) -> Option<PathBuf> {
    bundle_dir.or_else(|| std::env::var_os("VZ_LINUX_BUNDLE_DIR").map(PathBuf::from))
}

fn profile_bundle_dir(profile: KernelProfile, bundle_dir: Option<PathBuf>) -> Option<PathBuf> {
    bundle_dir
        .or_else(|| std::env::var_os(profile.bundle_dir_env_var()).map(PathBuf::from))
        .or_else(|| std::env::var_os("VZ_LINUX_BUNDLE_DIR").map(PathBuf::from))
}

fn workspace_bundle_dir(profile: KernelProfile) -> Option<PathBuf> {
    workspace_bundle_dir_from_manifest_dir(Path::new(env!("CARGO_MANIFEST_DIR")), profile)
}

fn workspace_bundle_dir_from_manifest_dir(
    manifest_dir: &Path,
    profile: KernelProfile,
) -> Option<PathBuf> {
    let candidate = match profile {
        KernelProfile::Developer => manifest_dir.join("../../linux/out"),
        KernelProfile::Container => manifest_dir.join("../../linux/out/container"),
    };
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
    if let Some(probe) = verify_developer_probe(bundle_dir, &bundle.version).await? {
        tokio::fs::copy(probe.archive, staging.join(probe.metadata.archive)).await?;
    }
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

fn validate_kernel_metadata(
    version: &KernelVersion,
    expected_agent: &str,
    expected_protocol_revision: u32,
    require_exact_agent_version: bool,
    expected_profile: Option<KernelProfile>,
) -> Result<(), LinuxError> {
    validate_agent_version(
        version,
        expected_agent,
        expected_protocol_revision,
        require_exact_agent_version,
    )?;
    if let Some(expected_profile) = expected_profile {
        validate_kernel_profile(version, expected_profile)?;
    }
    Ok(())
}

fn validate_kernel_profile(
    version: &KernelVersion,
    expected_profile: KernelProfile,
) -> Result<(), LinuxError> {
    let found = version.profile.as_deref().unwrap_or("<missing>");
    if found == expected_profile.as_str() {
        Ok(())
    } else {
        Err(LinuxError::KernelProfileMismatch {
            expected: expected_profile.as_str().to_string(),
            found: found.to_string(),
        })
    }
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
    let directory = paths.kernel.parent().ok_or_else(|| {
        LinuxError::InvalidConfig("kernel artifact lacks a bundle directory".to_string())
    })?;
    verify_developer_probe(directory, &paths.version).await?;

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
    #![allow(clippy::unwrap_used, clippy::expect_used)]

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
            developer_probe: None,
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

    async fn write_artifact_profile(dir: &Path, profile: KernelProfile) {
        let mut version: KernelVersion = serde_json::from_str(
            &tokio::fs::read_to_string(dir.join(VERSION_FILE))
                .await
                .expect("read version"),
        )
        .expect("parse version");
        version.profile = Some(profile.as_str().to_string());
        version.security_profile = Some(profile.security_profile().to_string());
        version.capabilities = Some(profile.default_capabilities());
        tokio::fs::write(
            dir.join(VERSION_FILE),
            serde_json::to_string_pretty(&version).expect("version json"),
        )
        .await
        .expect("write version");
    }

    async fn write_verified_bundle(dir: &Path, profile: KernelProfile) {
        write_artifacts_with_checksums(dir, env!("CARGO_PKG_VERSION").to_string(), true).await;
        write_artifact_profile(dir, profile).await;
    }

    async fn version_value(dir: &Path) -> serde_json::Value {
        serde_json::from_slice(&tokio::fs::read(dir.join(VERSION_FILE)).await.unwrap()).unwrap()
    }

    async fn write_version_value(dir: &Path, value: &serde_json::Value) {
        tokio::fs::write(
            dir.join(VERSION_FILE),
            serde_json::to_vec_pretty(value).unwrap(),
        )
        .await
        .unwrap();
    }

    fn directory_names(dir: &Path) -> BTreeSet<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect()
    }

    async fn add_developer_probe(dir: &Path) {
        let archive = b"fixture-only-rootfs-tar-bytes";
        tokio::fs::write(dir.join(crate::DEVELOPER_PROBE_ARCHIVE), archive)
            .await
            .unwrap();
        let mut version = version_value(dir).await;
        version["developer_probe"] = serde_json::json!({
            "schema_version": 1, "archive": crate::DEVELOPER_PROBE_ARCHIVE,
            "sha256": sha256(archive), "busybox_sha256": "a".repeat(64),
            "busybox_version": version["busybox"], "source_archive_sha256": "b".repeat(64),
            "source_inventory_sha256": "c".repeat(64), "build_provenance_sha256": "d".repeat(64),
            "marker_sha256": sha256(crate::DEVELOPER_PROBE_MARKER)
        });
        write_version_value(dir, &version).await;
    }

    #[tokio::test]
    async fn developer_probe_is_digest_bound_verified_and_copied_on_install() {
        let root = tempdir().unwrap();
        let source = root.path().join("source");
        write_verified_bundle(&source, KernelProfile::Developer).await;
        let before = verify_kernel_bundle_read_only(&source, KernelProfile::Developer)
            .await
            .unwrap();
        assert!(before.developer_probe.is_none());
        add_developer_probe(&source).await;
        let verified = verify_kernel_bundle_read_only(&source, KernelProfile::Developer)
            .await
            .unwrap();
        let probe = verified.developer_probe.as_ref().unwrap();
        assert_eq!(probe.archive, source.join(crate::DEVELOPER_PROBE_ARCHIVE));
        assert_ne!(
            before.artifact_identity.digest,
            verified.artifact_identity.digest
        );
        assert_eq!(
            before.artifact_identity.kernel_sha256,
            verified.artifact_identity.kernel_sha256
        );
        let installed = root.path().join("installed");
        install_from_bundle(&source, &installed).await.unwrap();
        let copied = verify_kernel_bundle_read_only(&installed, KernelProfile::Developer)
            .await
            .unwrap();
        assert_eq!(verified.artifact_identity, copied.artifact_identity);
        assert_eq!(copied.developer_probe.unwrap().metadata, probe.metadata);
    }

    #[tokio::test]
    async fn developer_probe_missing_tampered_or_redirected_inputs_fail_closed() {
        for kind in [
            "missing",
            "tampered",
            "redirected",
            "uppercase",
            "marker",
            "hardened",
            "undeclared",
        ] {
            let root = tempdir().unwrap();
            write_verified_bundle(root.path(), KernelProfile::Developer).await;
            add_developer_probe(root.path()).await;
            let archive = root.path().join(crate::DEVELOPER_PROBE_ARCHIVE);
            let mut version = version_value(root.path()).await;
            match kind {
                "missing" => tokio::fs::remove_file(&archive).await.unwrap(),
                "tampered" => tokio::fs::write(&archive, b"foreign").await.unwrap(),
                "redirected" => version["developer_probe"]["archive"] = "../foreign.tar".into(),
                "uppercase" => version["developer_probe"]["sha256"] = "A".repeat(64).into(),
                "marker" => version["developer_probe"]["marker_sha256"] = "e".repeat(64).into(),
                "hardened" => version["profile"] = "container".into(),
                "undeclared" => {
                    version.as_object_mut().unwrap().remove("developer_probe");
                }
                _ => unreachable!(),
            }
            write_version_value(root.path(), &version).await;
            assert!(
                verify_kernel_bundle_read_only(root.path(), KernelProfile::Developer)
                    .await
                    .is_err(),
                "{kind}"
            );
        }
    }

    #[tokio::test]
    async fn developer_probe_symlink_and_hardlink_are_rejected() {
        for hard in [false, true] {
            let root = tempdir().unwrap();
            write_verified_bundle(root.path(), KernelProfile::Developer).await;
            add_developer_probe(root.path()).await;
            let archive = root.path().join(crate::DEVELOPER_PROBE_ARCHIVE);
            let original = root.path().join("original.tar");
            tokio::fs::rename(&archive, &original).await.unwrap();
            if hard {
                std::fs::hard_link(&original, &archive).unwrap();
            } else {
                std::os::unix::fs::symlink(&original, &archive).unwrap();
            }
            assert!(
                verify_kernel_bundle_read_only(root.path(), KernelProfile::Developer)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn read_only_verifier_accepts_both_profiles_and_is_path_independent() {
        for profile in [KernelProfile::Developer, KernelProfile::Container] {
            let first = tempdir().unwrap();
            let second = tempdir().unwrap();
            write_verified_bundle(first.path(), profile).await;
            write_verified_bundle(second.path(), profile).await;
            let expected_names = BTreeSet::from([
                KERNEL_FILE.to_string(),
                INITRAMFS_FILE.to_string(),
                YOUKI_FILE.to_string(),
                VERSION_FILE.to_string(),
            ]);

            let first_verified = verify_kernel_bundle_read_only(first.path(), profile)
                .await
                .unwrap();
            let second_verified = verify_kernel_bundle_read_only(second.path(), profile)
                .await
                .unwrap();

            assert_eq!(first_verified.bundle_dir, first.path());
            assert_eq!(first_verified.paths.kernel, first.path().join(KERNEL_FILE));
            assert_eq!(
                first_verified.paths.version.profile.as_deref(),
                Some(profile.as_str())
            );
            assert_eq!(first_verified.capabilities, profile.default_capabilities());
            assert_eq!(
                first_verified.artifact_identity,
                second_verified.artifact_identity
            );
            assert!(
                first_verified
                    .artifact_identity
                    .digest
                    .starts_with("sha256:")
            );
            assert_eq!(first_verified.artifact_identity.digest.len(), 71);
            assert_eq!(
                serde_json::from_str::<KernelBundleArtifactIdentity>(
                    &serde_json::to_string(&first_verified.artifact_identity).unwrap()
                )
                .unwrap(),
                first_verified.artifact_identity
            );
            assert_eq!(directory_names(first.path()), expected_names);
            assert_eq!(directory_names(second.path()), expected_names);
        }
    }

    #[tokio::test]
    async fn read_only_verifier_requires_every_explicit_metadata_declaration() {
        for field in [
            "profile",
            "security_profile",
            "capabilities",
            "sha256_vmlinux",
            "sha256_initramfs",
            "sha256_youki",
            "agent",
            "agent_protocol_revision",
            "kernel",
            "youki",
        ] {
            let bundle = tempdir().unwrap();
            write_verified_bundle(bundle.path(), KernelProfile::Developer).await;
            let mut version = version_value(bundle.path()).await;
            version.as_object_mut().unwrap().remove(field);
            write_version_value(bundle.path(), &version).await;

            assert!(
                verify_kernel_bundle_read_only(bundle.path(), KernelProfile::Developer)
                    .await
                    .is_err(),
                "missing `{field}` must fail closed"
            );
        }
    }

    #[tokio::test]
    async fn read_only_verifier_rejects_metadata_mismatch_and_missing_capabilities() {
        let cases = [
            ("agent", serde_json::json!("other-host-version")),
            (
                "agent_protocol_revision",
                serde_json::json!(vz_agent_proto::AGENT_PROTOCOL_REVISION + 1),
            ),
            ("profile", serde_json::json!("container")),
            ("security_profile", serde_json::json!("container-hardened")),
            ("kernel", serde_json::json!("")),
            ("youki", serde_json::json!("\n")),
        ];
        for (field, replacement) in cases {
            let bundle = tempdir().unwrap();
            write_verified_bundle(bundle.path(), KernelProfile::Developer).await;
            let mut version = version_value(bundle.path()).await;
            version[field] = replacement;
            write_version_value(bundle.path(), &version).await;
            assert!(
                verify_kernel_bundle_read_only(bundle.path(), KernelProfile::Developer)
                    .await
                    .is_err(),
                "mismatched `{field}` must fail closed"
            );
        }

        let bundle = tempdir().unwrap();
        write_verified_bundle(bundle.path(), KernelProfile::Developer).await;
        let mut version = version_value(bundle.path()).await;
        version["capabilities"] = serde_json::json!(["vsock"]);
        write_version_value(bundle.path(), &version).await;
        assert!(matches!(
            verify_kernel_bundle_read_only(bundle.path(), KernelProfile::Developer).await,
            Err(LinuxError::MissingKernelCapabilities { .. })
        ));
    }

    #[tokio::test]
    async fn read_only_verifier_requires_canonical_checksums_and_detects_tampering() {
        let bundle = tempdir().unwrap();
        write_verified_bundle(bundle.path(), KernelProfile::Developer).await;
        let mut version = version_value(bundle.path()).await;
        version["sha256_vmlinux"] = serde_json::json!(
            version["sha256_vmlinux"]
                .as_str()
                .unwrap()
                .to_ascii_uppercase()
        );
        write_version_value(bundle.path(), &version).await;
        assert!(matches!(
            verify_kernel_bundle_read_only(bundle.path(), KernelProfile::Developer).await,
            Err(LinuxError::InvalidConfig(_))
        ));

        write_verified_bundle(bundle.path(), KernelProfile::Developer).await;
        tokio::fs::write(bundle.path().join(INITRAMFS_FILE), b"tampered")
            .await
            .unwrap();
        assert!(matches!(
            verify_kernel_bundle_read_only(bundle.path(), KernelProfile::Developer).await,
            Err(LinuxError::ArtifactChecksumMismatch { ref artifact, .. })
                if artifact == INITRAMFS_FILE
        ));
    }

    #[tokio::test]
    async fn read_only_verifier_binds_unknown_raw_version_metadata() {
        let bundle = tempdir().unwrap();
        write_verified_bundle(bundle.path(), KernelProfile::Developer).await;
        let before = verify_kernel_bundle_read_only(bundle.path(), KernelProfile::Developer)
            .await
            .unwrap()
            .artifact_identity;

        let mut version = version_value(bundle.path()).await;
        version["iptables"] = serde_json::json!("1.8.13");
        write_version_value(bundle.path(), &version).await;
        let after = verify_kernel_bundle_read_only(bundle.path(), KernelProfile::Developer)
            .await
            .unwrap()
            .artifact_identity;

        assert_eq!(before.kernel_sha256, after.kernel_sha256);
        assert_eq!(before.initramfs_sha256, after.initramfs_sha256);
        assert_eq!(before.youki_sha256, after.youki_sha256);
        assert_ne!(before.version_sha256, after.version_sha256);
        assert_ne!(before.digest, after.digest);
    }

    #[tokio::test]
    async fn read_only_verifier_rejects_missing_symlinked_and_nonregular_inputs() {
        let missing = tempdir().unwrap();
        write_verified_bundle(missing.path(), KernelProfile::Developer).await;
        tokio::fs::remove_file(missing.path().join(YOUKI_FILE))
            .await
            .unwrap();
        assert!(matches!(
            verify_kernel_bundle_read_only(missing.path(), KernelProfile::Developer).await,
            Err(LinuxError::MissingKernelArtifacts { .. })
        ));

        let symlinked_artifact = tempdir().unwrap();
        write_verified_bundle(symlinked_artifact.path(), KernelProfile::Developer).await;
        tokio::fs::remove_file(symlinked_artifact.path().join(YOUKI_FILE))
            .await
            .unwrap();
        std::os::unix::fs::symlink(
            symlinked_artifact.path().join(KERNEL_FILE),
            symlinked_artifact.path().join(YOUKI_FILE),
        )
        .unwrap();
        assert!(matches!(
            verify_kernel_bundle_read_only(symlinked_artifact.path(), KernelProfile::Developer)
                .await,
            Err(LinuxError::InvalidConfig(_))
        ));

        let nonregular = tempdir().unwrap();
        write_verified_bundle(nonregular.path(), KernelProfile::Developer).await;
        tokio::fs::remove_file(nonregular.path().join(INITRAMFS_FILE))
            .await
            .unwrap();
        tokio::fs::create_dir(nonregular.path().join(INITRAMFS_FILE))
            .await
            .unwrap();
        assert!(matches!(
            verify_kernel_bundle_read_only(nonregular.path(), KernelProfile::Developer).await,
            Err(LinuxError::InvalidConfig(_))
        ));

        let parent = tempdir().unwrap();
        let real = parent.path().join("real");
        write_verified_bundle(&real, KernelProfile::Developer).await;
        let linked = parent.path().join("linked");
        std::os::unix::fs::symlink(&real, &linked).unwrap();
        assert!(matches!(
            verify_kernel_bundle_read_only(&linked, KernelProfile::Developer).await,
            Err(LinuxError::InvalidConfig(_))
        ));
    }

    #[tokio::test]
    async fn read_only_verifier_bounds_raw_version_metadata() {
        let bundle = tempdir().unwrap();
        write_verified_bundle(bundle.path(), KernelProfile::Developer).await;
        tokio::fs::write(
            bundle.path().join(VERSION_FILE),
            vec![b' '; MAX_VERSION_METADATA_BYTES as usize + 1],
        )
        .await
        .unwrap();
        assert!(matches!(
            verify_kernel_bundle_read_only(bundle.path(), KernelProfile::Developer).await,
            Err(LinuxError::InvalidConfig(_))
        ));
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
            KernelCapability::CgroupBpf,
            KernelCapability::BtrfsSnapshots,
            KernelCapability::DeviceMapper,
            KernelCapability::DmCrypt,
            KernelCapability::Nfsd,
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
            KernelCapability::CgroupBpf,
            KernelCapability::BtrfsSnapshots,
            KernelCapability::DeviceMapper,
            KernelCapability::DmCrypt,
            KernelCapability::Nfsd,
            KernelCapability::ContainerSandbox,
        ]
        .into_iter()
        .collect();

        let resolved = ensure_kernel_bundle(KernelBundleOptions {
            install_dir: Some(install),
            bundle_dir: None,
            profile: Some(KernelProfile::Container),
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
        assert!(resolved.capabilities.contains(&KernelCapability::Nfsd));
        assert!(resolved.capabilities.contains(&KernelCapability::CgroupBpf));
        assert!(resolved.capabilities.contains(&KernelCapability::IoUring));
        assert!(
            resolved
                .capabilities
                .contains(&KernelCapability::DeviceMapper)
        );
        assert!(resolved.capabilities.contains(&KernelCapability::DmCrypt));
    }

    #[tokio::test]
    async fn ensure_kernel_profile_installs_and_validates_requested_profile() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("linux/container");
        let bundle = temp.path().join("bundle/container");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts_with_checksums(&bundle, expected.clone(), true).await;
        write_artifact_profile(&bundle, KernelProfile::Container).await;

        let paths = ensure_kernel_profile_with_options(
            KernelProfile::Container,
            EnsureKernelOptions {
                install_dir: Some(install.clone()),
                bundle_dir: Some(bundle),
                require_exact_agent_version: true,
            },
        )
        .await
        .expect("ensure container profile");

        assert_eq!(paths.version.agent, expected);
        assert_eq!(paths.version.profile.as_deref(), Some("container"));
        assert_eq!(paths.kernel, install.join(KERNEL_FILE));
    }

    #[tokio::test]
    async fn ensure_kernel_profile_rejects_wrong_profile_metadata() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("linux/developer");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts_with_checksums(&install, expected, true).await;
        write_artifact_profile(&install, KernelProfile::Developer).await;

        let err = ensure_kernel_profile_with_options(
            KernelProfile::Container,
            EnsureKernelOptions {
                install_dir: Some(install),
                bundle_dir: None,
                require_exact_agent_version: true,
            },
        )
        .await
        .expect_err("must reject developer metadata for container profile");

        assert!(matches!(
            err,
            LinuxError::KernelProfileMismatch { ref expected, ref found }
                if expected == "container" && found == "developer"
        ));
    }

    #[tokio::test]
    async fn ensure_kernel_bundle_profile_requires_profile_capabilities() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("linux/container");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts_with_checksums(&install, expected, true).await;

        let mut version: KernelVersion = serde_json::from_str(
            &tokio::fs::read_to_string(install.join(VERSION_FILE))
                .await
                .expect("read version"),
        )
        .expect("parse version");
        version.profile = Some("container".to_string());
        version.security_profile = Some("container-hardened".to_string());
        version.capabilities = Some(default_vz_linux_kernel_capabilities());
        tokio::fs::write(
            install.join(VERSION_FILE),
            serde_json::to_string_pretty(&version).expect("version json"),
        )
        .await
        .expect("write version");

        let err = ensure_kernel_bundle(KernelBundleOptions {
            install_dir: Some(install),
            bundle_dir: None,
            profile: Some(KernelProfile::Container),
            ..KernelBundleOptions::default()
        })
        .await
        .expect_err("must reject missing profile capabilities");

        assert!(matches!(
            err,
            LinuxError::MissingKernelCapabilities { ref missing }
                if missing.contains(&KernelCapability::ContainerSandbox.as_str().to_string())
                    && missing.contains(&KernelCapability::Nfsd.as_str().to_string())
                    && missing.contains(&KernelCapability::CgroupBpf.as_str().to_string())
        ));
    }

    #[tokio::test]
    async fn ensure_developer_profile_rejects_bundle_without_cgroup_bpf() {
        let temp = tempdir().expect("tempdir");
        let install = temp.path().join("linux/developer");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts_with_checksums(&install, expected, true).await;
        write_artifact_profile(&install, KernelProfile::Developer).await;

        let mut version: KernelVersion = serde_json::from_str(
            &tokio::fs::read_to_string(install.join(VERSION_FILE))
                .await
                .expect("read version"),
        )
        .expect("parse version");
        version
            .capabilities
            .as_mut()
            .expect("developer capabilities")
            .remove(&KernelCapability::CgroupBpf);
        tokio::fs::write(
            install.join(VERSION_FILE),
            serde_json::to_string_pretty(&version).expect("version json"),
        )
        .await
        .expect("write version");

        let err = ensure_kernel_profile_with_options(
            KernelProfile::Developer,
            EnsureKernelOptions {
                install_dir: Some(install),
                bundle_dir: None,
                require_exact_agent_version: true,
            },
        )
        .await
        .expect_err("must reject a stale developer bundle before boot");

        assert!(matches!(
            err,
            LinuxError::MissingKernelCapabilities { ref missing }
                if missing == &[KernelCapability::CgroupBpf.as_str().to_string()]
        ));
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

        let discovered =
            workspace_bundle_dir_from_manifest_dir(&manifest_dir, KernelProfile::Developer);
        assert_eq!(
            discovered
                .as_deref()
                .and_then(|path| path.canonicalize().ok()),
            bundle.canonicalize().ok()
        );
    }

    #[tokio::test]
    async fn workspace_bundle_dir_discovery_uses_profile_relative_linux_out() {
        let temp = tempdir().expect("tempdir");
        let manifest_dir = temp.path().join("crates/vz-linux");
        let bundle = temp.path().join("linux/out/container");
        tokio::fs::create_dir_all(&manifest_dir)
            .await
            .expect("manifest dir");
        let expected = env!("CARGO_PKG_VERSION").to_string();
        write_artifacts(&bundle, expected).await;

        let discovered =
            workspace_bundle_dir_from_manifest_dir(&manifest_dir, KernelProfile::Container);
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

        let discovered =
            workspace_bundle_dir_from_manifest_dir(&manifest_dir, KernelProfile::Developer);
        assert!(discovered.is_none());
    }
}
