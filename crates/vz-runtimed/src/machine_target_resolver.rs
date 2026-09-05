//! Explicit, read-only Machine artifact resolution before topology admission.
//!
//! This resolves artifacts, not runtime capabilities or an Environment execution
//! plan. It neither reserves resources nor authorizes booting. Catalog paths are
//! trusted operator inputs, never workspace hints or ambient discovery. A future
//! controller must pin verified source bytes in private immutable storage before
//! activation; a returned pathname alone does not prevent later source mutation.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use vz_linux::{KernelBundleArtifactIdentity, KernelProfile, verify_kernel_bundle_read_only};
use vz_runtime_contract::{
    Architecture, HostSpec, MachineBackend, MachineCapability, MachineProfile, MachineSpec,
    OperatingSystem, ProjectDefinition,
};

pub const MACHINE_TARGET_CATALOG_SCHEMA_VERSION: u32 = 1;
pub const LINUX_APPLIANCE_IMAGE: &str = "vz-linux-appliance";

/// Explicit installation catalog. No entry is inferred from the local system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MachineTargetCatalog {
    pub schema_version: u32,
    pub linux: Vec<LinuxTargetCatalogEntry>,
}

impl Default for MachineTargetCatalog {
    fn default() -> Self {
        Self {
            schema_version: MACHINE_TARGET_CATALOG_SCHEMA_VERSION,
            linux: Vec::new(),
        }
    }
}

/// One immutable appliance release/profile and its explicitly declared channels.
/// `version` is the catalog release version, not the kernel's version number.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LinuxTargetCatalogEntry {
    pub image: String,
    pub version: String,
    pub profile: MachineProfile,
    pub bundle_dir: PathBuf,
    pub digest: String,
    #[serde(default)]
    pub channels: BTreeSet<String>,
}

#[derive(Debug, Error)]
pub enum TargetResolutionError {
    #[error("invalid Machine target catalog: {0}")]
    InvalidCatalog(String),
    #[error("invalid ProjectDefinition: {0}")]
    InvalidDefinition(String),
    #[error("unsupported target for Machine `{machine}`: {reason}")]
    UnsupportedTarget { machine: String, reason: String },
    #[error("target backend not implemented for Machine `{machine}`: {reason}")]
    BackendNotImplemented { machine: String, reason: String },
    #[error("Machine `{machine}` requires a canonical pinned SHA-256 artifact digest")]
    UnpinnedTarget { machine: String },
    #[error("no exact catalog target matches Machine `{machine}`")]
    TargetNotFound { machine: String },
    #[error("catalog selection for Machine `{machine}` is ambiguous")]
    AmbiguousTarget { machine: String },
    #[error("artifact verification failed for Machine `{machine}`: {reason}")]
    ArtifactVerification { machine: String, reason: String },
}

impl MachineTargetCatalog {
    /// Load only this explicit file, bounded and without following a final symlink.
    /// Parent ancestry and ACLs belong to the trusted operator configuration boundary.
    pub fn from_file(path: &Path) -> Result<Self, TargetResolutionError> {
        if !absolute_without_traversal(path) {
            return Err(TargetResolutionError::InvalidCatalog(
                "catalog path must be absolute without traversal".into(),
            ));
        }
        let invalid =
            |error: std::io::Error| TargetResolutionError::InvalidCatalog(error.to_string());
        let descriptor = rustix::fs::open(
            path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|error| invalid(error.into()))?;
        let file = File::from(descriptor);
        let metadata = file.metadata().map_err(invalid)?;
        use std::os::unix::fs::MetadataExt;
        let uid = rustix::process::geteuid().as_raw();
        if !metadata.is_file()
            || metadata.nlink() != 1
            || ![0, uid].contains(&metadata.uid())
            || metadata.mode() & 0o022 != 0
        {
            return Err(TargetResolutionError::InvalidCatalog(
                "catalog must be a trusted, non-writable-by-others, single-link regular file"
                    .into(),
            ));
        }
        const LIMIT: u64 = 1024 * 1024;
        let mut bytes = Vec::new();
        file.take(LIMIT + 1)
            .read_to_end(&mut bytes)
            .map_err(invalid)?;
        if bytes.len() as u64 > LIMIT {
            return Err(TargetResolutionError::InvalidCatalog(
                "catalog exceeds 1 MiB".into(),
            ));
        }
        let catalog: Self = serde_json::from_slice(&bytes)
            .map_err(|error| TargetResolutionError::InvalidCatalog(error.to_string()))?;
        catalog.validate()?;
        Ok(catalog)
    }

    pub fn validate(&self) -> Result<(), TargetResolutionError> {
        if self.schema_version != MACHINE_TARGET_CATALOG_SCHEMA_VERSION || self.linux.len() > 1024 {
            return Err(TargetResolutionError::InvalidCatalog(
                "unsupported schema or oversized entry set".into(),
            ));
        }
        let mut releases = BTreeSet::new();
        let mut channels = BTreeSet::new();
        for entry in &self.linux {
            if entry.image != LINUX_APPLIANCE_IMAGE
                || !label(&entry.version)
                || !canonical_digest(&entry.digest)
                || !absolute_without_traversal(&entry.bundle_dir)
                || entry.channels.iter().any(|channel| !label(channel))
            {
                return Err(TargetResolutionError::InvalidCatalog(
                    "invalid appliance identity, version, digest, channel or absolute bundle path"
                        .into(),
                ));
            }
            if !releases.insert((entry.profile, entry.version.clone())) {
                return Err(TargetResolutionError::InvalidCatalog(
                    "duplicate appliance release/profile".into(),
                ));
            }
            for channel in &entry.channels {
                if !channels.insert((entry.profile, channel.clone())) {
                    return Err(TargetResolutionError::InvalidCatalog(
                        "a channel selects multiple releases of one profile".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResolvedMachineResources {
    pub cpus: u8,
    pub memory_mb: u64,
}

/// Canonical, path-independent requested and resolved configuration identity.
/// This contains no negotiated Machine capabilities: artifact metadata is not
/// Docker/Compose/buildx or native-backend conformance evidence.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResolvedMachineConfiguration {
    pub schema_version: u32,
    pub host: HostSpec,
    pub backend: MachineBackend,
    pub machine: MachineSpec,
    pub release_version: String,
    pub kernel_profile: KernelProfile,
    pub artifact: KernelBundleArtifactIdentity,
    pub resources: ResolvedMachineResources,
}

#[derive(Debug, Clone)]
pub struct ResolvedLinuxMachineTarget {
    bundle_dir: PathBuf,
    configuration: ResolvedMachineConfiguration,
    configuration_digest: String,
}

impl ResolvedLinuxMachineTarget {
    pub fn bundle_dir(&self) -> &Path {
        &self.bundle_dir
    }
    pub fn profile(&self) -> KernelProfile {
        self.configuration.kernel_profile
    }
    pub fn configuration(&self) -> &ResolvedMachineConfiguration {
        &self.configuration
    }
    pub fn configuration_digest(&self) -> &str {
        &self.configuration_digest
    }
}

#[derive(Debug)]
pub struct ResolvedProjectTargets {
    pub definition_digest: String,
    pub machines: BTreeMap<String, ResolvedLinuxMachineTarget>,
}

#[derive(Debug, Clone)]
pub struct MachineTargetResolver {
    host: HostSpec,
    catalog: MachineTargetCatalog,
}

impl MachineTargetResolver {
    /// Construction validates only the supplied catalog, without filesystem access.
    pub fn new(
        host: HostSpec,
        catalog: MachineTargetCatalog,
    ) -> Result<Self, TargetResolutionError> {
        catalog.validate()?;
        Ok(Self { host, catalog })
    }

    /// Resolve every Machine or return no plan. There is deliberately no store,
    /// factory or mutation callback in this API. Network/workspace planning and
    /// lifecycle capability negotiation remain separate controller obligations.
    pub async fn resolve_project(
        &self,
        definition: &ProjectDefinition,
    ) -> Result<ResolvedProjectTargets, TargetResolutionError> {
        definition
            .validate()
            .map_err(|error| TargetResolutionError::InvalidDefinition(error.to_string()))?;
        // Pure selection for all siblings precedes even the first artifact read.
        let selected = definition
            .environment
            .machines
            .iter()
            .map(|machine| self.select(machine).map(|entry| (machine, entry)))
            .collect::<Result<Vec<_>, _>>()?;
        let mut machines = BTreeMap::new();
        for (machine, entry) in selected {
            let target = self.verify_selected(machine, entry).await?;
            machines.insert(machine.name.clone(), target);
        }
        Ok(ResolvedProjectTargets {
            definition_digest: definition
                .digest()
                .map_err(|error| TargetResolutionError::InvalidDefinition(error.to_string()))?,
            machines,
        })
    }

    fn select(
        &self,
        machine: &MachineSpec,
    ) -> Result<&LinuxTargetCatalogEntry, TargetResolutionError> {
        let unsupported = |reason: &str| TargetResolutionError::UnsupportedTarget {
            machine: machine.name.clone(),
            reason: reason.into(),
        };
        if self.host.os != OperatingSystem::Macos || self.host.arch != Architecture::Aarch64 {
            return Err(unsupported(
                "this adapter currently supports Apple-silicon macOS hosts only",
            ));
        }
        if machine.target.os == OperatingSystem::Macos
            && machine.target.arch == Architecture::Aarch64
        {
            return Err(TargetResolutionError::BackendNotImplemented {
                machine: machine.name.clone(), reason: "native macOS requires its private disk/auxiliary/identity adapter and verified catalog".into(),
            });
        }
        if machine.target.os != OperatingSystem::Linux
            || machine.target.arch != Architecture::Aarch64
        {
            return Err(unsupported(
                "no adapter for the requested host/target/architecture pair",
            ));
        }
        if machine
            .requested_capabilities
            .contains(MachineCapability::WindowsConsole)
            || machine
                .requested_capabilities
                .contains(MachineCapability::Gui)
        {
            return Err(unsupported(
                "this Linux appliance has no Windows console or GUI adapter",
            ));
        }
        let cpus = machine.resources.cpus.unwrap_or(2);
        let memory = machine
            .resources
            .memory_mb
            .unwrap_or(default_memory(machine.profile));
        if cpus == 0
            || memory < 512
            || memory.checked_mul(1024 * 1024).is_none()
            || machine.resources.disk_bytes.is_some()
        {
            return Err(unsupported(
                "invalid compute resources or unsupported explicit Machine disk sizing",
            ));
        }
        let digest = machine
            .target
            .digest
            .as_deref()
            .filter(|digest| canonical_digest(digest))
            .ok_or_else(|| TargetResolutionError::UnpinnedTarget {
                machine: machine.name.clone(),
            })?;
        let entries = self
            .catalog
            .linux
            .iter()
            .filter(|entry| {
                entry.image == machine.target.image
                    && entry.profile == machine.profile
                    && entry.digest == digest
                    && machine
                        .target
                        .version
                        .as_ref()
                        .is_none_or(|version| version == &entry.version)
                    && machine
                        .target
                        .channel
                        .as_ref()
                        .is_none_or(|channel| entry.channels.contains(channel))
            })
            .collect::<Vec<_>>();
        match entries.as_slice() {
            [entry] => Ok(entry),
            [] => Err(TargetResolutionError::TargetNotFound {
                machine: machine.name.clone(),
            }),
            _ => Err(TargetResolutionError::AmbiguousTarget {
                machine: machine.name.clone(),
            }),
        }
    }

    async fn verify_selected(
        &self,
        machine: &MachineSpec,
        entry: &LinuxTargetCatalogEntry,
    ) -> Result<ResolvedLinuxMachineTarget, TargetResolutionError> {
        let profile = match machine.profile {
            MachineProfile::Developer => KernelProfile::Developer,
            MachineProfile::Hardened => KernelProfile::Container,
        };
        let verified = verify_kernel_bundle_read_only(&entry.bundle_dir, profile)
            .await
            .map_err(|error| TargetResolutionError::ArtifactVerification {
                machine: machine.name.clone(),
                reason: error.to_string(),
            })?;
        if verified.artifact_identity.digest != entry.digest {
            return Err(TargetResolutionError::ArtifactVerification {
                machine: machine.name.clone(),
                reason:
                    "verified appliance bytes do not match the pinned catalog/TargetSpec digest"
                        .into(),
            });
        }
        let configuration = ResolvedMachineConfiguration {
            schema_version: 1,
            host: self.host,
            backend: MachineBackend::MacosVirtualizationLinux,
            machine: machine.clone(),
            release_version: entry.version.clone(),
            kernel_profile: profile,
            artifact: verified.artifact_identity,
            resources: ResolvedMachineResources {
                cpus: machine.resources.cpus.unwrap_or(2),
                memory_mb: machine
                    .resources
                    .memory_mb
                    .unwrap_or(default_memory(machine.profile)),
            },
        };
        // Sorted JSON gives non-Rust evidence validators an exact portable encoding.
        let canonical = serde_json::to_value(&configuration)
            .and_then(|value| serde_json::to_vec(&value))
            .map_err(|error| TargetResolutionError::InvalidDefinition(error.to_string()))?;
        let mut hasher = Sha256::new();
        hasher.update(b"vz.machine-configuration.v1\0");
        hasher.update(canonical);
        Ok(ResolvedLinuxMachineTarget {
            bundle_dir: verified.bundle_dir,
            configuration,
            configuration_digest: format!("sha256:{:x}", hasher.finalize()),
        })
    }
}

fn default_memory(profile: MachineProfile) -> u64 {
    match profile {
        MachineProfile::Developer => 4096,
        MachineProfile::Hardened => 1024,
    }
}

fn canonical_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|value| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
}

fn absolute_without_traversal(path: &Path) -> bool {
    path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use vz_runtime_contract::{
        CapabilitySet, EnvironmentSpec, MachineResources, ProjectId, TOPOLOGY_SCHEMA_VERSION,
    };

    fn host() -> HostSpec {
        HostSpec {
            os: OperatingSystem::Macos,
            arch: Architecture::Aarch64,
        }
    }

    async fn bundle(root: &Path, profile: MachineProfile) -> LinuxTargetCatalogEntry {
        fs::create_dir_all(root).unwrap();
        let kernel_profile = match profile {
            MachineProfile::Developer => KernelProfile::Developer,
            MachineProfile::Hardened => KernelProfile::Container,
        };
        for name in ["vmlinux", "initramfs.img", "youki"] {
            fs::write(root.join(name), name).unwrap();
        }
        let hash = |value: &str| format!("{:x}", Sha256::digest(value.as_bytes()));
        let metadata = json!({"kernel":"test-kernel", "busybox":"test-busybox", "agent":env!("CARGO_PKG_VERSION"),
            "agent_protocol_revision":vz_agent_proto::AGENT_PROTOCOL_REVISION, "youki":"test-youki",
            "profile":kernel_profile.as_str(), "security_profile":kernel_profile.security_profile(),
            "capabilities":kernel_profile.default_capabilities(), "sha256_vmlinux":hash("vmlinux"),
            "sha256_initramfs":hash("initramfs.img"), "sha256_youki":hash("youki")});
        fs::write(
            root.join("version.json"),
            serde_json::to_vec(&metadata).unwrap(),
        )
        .unwrap();
        let verified = verify_kernel_bundle_read_only(root, kernel_profile)
            .await
            .unwrap();
        LinuxTargetCatalogEntry {
            image: LINUX_APPLIANCE_IMAGE.into(),
            version: "0.4.0-test".into(),
            profile,
            bundle_dir: root.to_path_buf(),
            digest: verified.artifact_identity.digest,
            channels: BTreeSet::from(["test".into()]),
        }
    }

    fn definition(entries: &[LinuxTargetCatalogEntry]) -> ProjectDefinition {
        ProjectDefinition {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: ProjectId::new("prj_resolution").unwrap(),
            name: "resolution".into(),
            environment: EnvironmentSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                networks: Vec::new(),
                endpoints: Vec::new(),
                machines: entries
                    .iter()
                    .enumerate()
                    .map(|(index, entry)| MachineSpec {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        name: format!("machine-{index}"),
                        profile: entry.profile,
                        target: vz_runtime_contract::TargetSpec {
                            os: OperatingSystem::Linux,
                            arch: Architecture::Aarch64,
                            image: entry.image.clone(),
                            version: Some(entry.version.clone()),
                            channel: Some("test".into()),
                            digest: Some(entry.digest.clone()),
                        },
                        resources: MachineResources::default(),
                        requested_capabilities: CapabilitySet::new([MachineCapability::PosixExec]),
                        workspace: None,
                    })
                    .collect(),
            },
        }
    }

    fn resolver(entries: Vec<LinuxTargetCatalogEntry>) -> MachineTargetResolver {
        MachineTargetResolver::new(
            host(),
            MachineTargetCatalog {
                schema_version: 1,
                linux: entries,
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn exact_profile_targets_resolve_without_state_or_installation() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let dev = bundle(&root.join("developer"), MachineProfile::Developer).await;
        let hard = bundle(&root.join("hardened"), MachineProfile::Hardened).await;
        let definition = definition(&[dev.clone(), hard.clone()]);
        let targets = resolver(vec![dev, hard])
            .resolve_project(&definition)
            .await
            .unwrap();
        assert_eq!(targets.machines.len(), 2);
        assert_eq!(targets.definition_digest, definition.digest().unwrap());
        for (name, target) in targets.machines {
            assert_eq!(target.configuration().machine.name, name);
            assert!(canonical_digest(target.configuration_digest()));
            assert_eq!(fs::read_dir(target.bundle_dir()).unwrap().count(), 4);
            assert_eq!(target.configuration().resources.cpus, 2);
            assert_eq!(
                target.configuration().resources.memory_mb,
                default_memory(target.configuration().machine.profile)
            );
        }
        assert_eq!(fs::read_dir(root).unwrap().count(), 2);
    }

    #[tokio::test]
    async fn unsupported_sibling_is_rejected_before_any_artifact_read_or_state() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let entry = bundle(&root.join("bundle"), MachineProfile::Developer).await;
        let mut definition = definition(&[entry.clone(), entry.clone()]);
        let resolver = resolver(vec![entry.clone()]);
        fs::remove_file(entry.bundle_dir.join("vmlinux")).unwrap();
        definition.environment.machines[1].target.image = "ubuntu:24.04".into();
        assert!(matches!(resolver.resolve_project(&definition).await,
            Err(TargetResolutionError::TargetNotFound { machine }) if machine == "machine-1"));
        assert_eq!(fs::read_dir(root).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn every_target_selector_is_consumed_without_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let entry = bundle(&root, MachineProfile::Developer).await;
        let base = definition(std::slice::from_ref(&entry));
        let resolver = resolver(vec![entry]);
        for case in [
            "image", "version", "channel", "digest", "unpinned", "profile", "disk", "cpus",
            "memory",
        ] {
            let mut changed = base.clone();
            let machine = &mut changed.environment.machines[0];
            match case {
                "image" => machine.target.image = "ubuntu:24.04".into(),
                "version" => machine.target.version = Some("other-release".into()),
                "channel" => machine.target.channel = Some("undeclared".into()),
                "digest" => machine.target.digest = Some(format!("sha256:{}", "f".repeat(64))),
                "unpinned" => machine.target.digest = None,
                "profile" => machine.profile = MachineProfile::Hardened,
                "disk" => machine.resources.disk_bytes = Some(1),
                "cpus" => machine.resources.cpus = Some(0),
                "memory" => machine.resources.memory_mb = Some(u64::MAX),
                _ => unreachable!(),
            }
            assert!(
                resolver.resolve_project(&changed).await.is_err(),
                "{case} silently ignored"
            );
        }
        let mut digest_only = base;
        digest_only.environment.machines[0].target.version = None;
        digest_only.environment.machines[0].target.channel = None;
        resolver.resolve_project(&digest_only).await.unwrap();
        assert_eq!(fs::read_dir(root).unwrap().count(), 4);
    }

    #[tokio::test]
    async fn host_target_matrix_never_infers_an_unimplemented_backend() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let entry = bundle(&root, MachineProfile::Developer).await;
        let base = definition(std::slice::from_ref(&entry));
        for host_os in [
            OperatingSystem::Macos,
            OperatingSystem::Linux,
            OperatingSystem::Windows,
        ] {
            for host_arch in [Architecture::Aarch64, Architecture::X86_64] {
                for target_os in [
                    OperatingSystem::Macos,
                    OperatingSystem::Linux,
                    OperatingSystem::Windows,
                ] {
                    for target_arch in [Architecture::Aarch64, Architecture::X86_64] {
                        let mut definition = base.clone();
                        definition.environment.machines[0].target.os = target_os;
                        definition.environment.machines[0].target.arch = target_arch;
                        let resolver = MachineTargetResolver::new(
                            HostSpec {
                                os: host_os,
                                arch: host_arch,
                            },
                            MachineTargetCatalog {
                                schema_version: 1,
                                linux: vec![entry.clone()],
                            },
                        )
                        .unwrap();
                        let result = resolver.resolve_project(&definition).await;
                        assert_eq!(
                            result.is_ok(),
                            host_os == OperatingSystem::Macos
                                && host_arch == Architecture::Aarch64
                                && target_os == OperatingSystem::Linux
                                && target_arch == Architecture::Aarch64,
                            "unexpected result for {host_os:?}/{host_arch:?} -> {target_os:?}/{target_arch:?}"
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn configuration_identity_binds_inputs_but_not_artifact_location() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let first = bundle(&root.join("first"), MachineProfile::Developer).await;
        let second = bundle(&root.join("second"), MachineProfile::Developer).await;
        let definition = definition(std::slice::from_ref(&first));
        let one = resolver(vec![first.clone()])
            .resolve_project(&definition)
            .await
            .unwrap();
        let two = resolver(vec![second])
            .resolve_project(&definition)
            .await
            .unwrap();
        assert_eq!(
            one.machines["machine-0"].configuration_digest(),
            two.machines["machine-0"].configuration_digest()
        );
        let mut changed = definition.clone();
        changed.environment.machines[0].resources.cpus = Some(3);
        let three = resolver(vec![first.clone()])
            .resolve_project(&changed)
            .await
            .unwrap();
        assert_ne!(
            one.machines["machine-0"].configuration_digest(),
            three.machines["machine-0"].configuration_digest()
        );
        fs::write(first.bundle_dir.join("youki"), "tampered").unwrap();
        assert!(matches!(
            resolver(vec![first]).resolve_project(&definition).await,
            Err(TargetResolutionError::ArtifactVerification { .. })
        ));
    }

    #[tokio::test]
    async fn intact_bundle_rejects_canonical_but_wrong_catalog_and_target_digest() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let mut entry = bundle(&root, MachineProfile::Developer).await;
        entry.digest = format!("sha256:{}", "f".repeat(64));
        let definition = definition(std::slice::from_ref(&entry));

        assert!(matches!(
            resolver(vec![entry]).resolve_project(&definition).await,
            Err(TargetResolutionError::ArtifactVerification { machine, reason })
                if machine == "machine-0" && reason.contains("pinned catalog/TargetSpec digest")
        ));
    }

    #[tokio::test]
    async fn unsupported_runtime_capabilities_and_subminimum_memory_fail_selection() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let entry = bundle(&root, MachineProfile::Developer).await;
        let base = definition(std::slice::from_ref(&entry));
        let resolver = resolver(vec![entry]);

        for capability in [MachineCapability::Gui, MachineCapability::WindowsConsole] {
            let mut changed = base.clone();
            changed.environment.machines[0]
                .requested_capabilities
                .capabilities
                .insert(capability);
            assert!(matches!(
                resolver.resolve_project(&changed).await,
                Err(TargetResolutionError::UnsupportedTarget { machine, .. })
                    if machine == "machine-0"
            ));
        }

        let mut too_small = base;
        too_small.environment.machines[0].resources.memory_mb = Some(511);
        assert!(matches!(
            resolver.resolve_project(&too_small).await,
            Err(TargetResolutionError::UnsupportedTarget { machine, .. })
                if machine == "machine-0"
        ));
    }

    #[tokio::test]
    async fn selected_bundle_profile_and_security_mismatch_fail_in_resolver() {
        for (field, value) in [
            ("profile", json!("container")),
            ("security_profile", json!("container-hardened")),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().canonicalize().unwrap();
            let entry = bundle(&root, MachineProfile::Developer).await;
            let definition = definition(std::slice::from_ref(&entry));
            let mut metadata: serde_json::Value =
                serde_json::from_slice(&fs::read(root.join("version.json")).unwrap()).unwrap();
            metadata[field] = value;
            fs::write(
                root.join("version.json"),
                serde_json::to_vec(&metadata).unwrap(),
            )
            .unwrap();

            assert!(matches!(
                resolver(vec![entry]).resolve_project(&definition).await,
                Err(TargetResolutionError::ArtifactVerification { machine, .. })
                    if machine == "machine-0"
            ));
        }
    }

    #[tokio::test]
    async fn catalog_rejects_ambiguous_release_channels_and_untrusted_files() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let entry = bundle(&root.join("bundle"), MachineProfile::Developer).await;
        let catalog = MachineTargetCatalog {
            schema_version: 1,
            linux: vec![entry.clone()],
        };
        let file = root.join("catalog.json");
        fs::write(&file, serde_json::to_vec(&catalog).unwrap()).unwrap();
        assert_eq!(MachineTargetCatalog::from_file(&file).unwrap(), catalog);
        let duplicate = MachineTargetCatalog {
            schema_version: 1,
            linux: vec![entry.clone(), entry.clone()],
        };
        assert!(duplicate.validate().is_err());
        let mut other = entry;
        other.version = "other-version".into();
        let channels = MachineTargetCatalog {
            schema_version: 1,
            linux: vec![catalog.linux[0].clone(), other],
        };
        assert!(channels.validate().is_err());
        fs::set_permissions(&file, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(MachineTargetCatalog::from_file(&file).is_err());
        let alias = root.join("alias.json");
        std::os::unix::fs::symlink(&file, &alias).unwrap();
        assert!(MachineTargetCatalog::from_file(&alias).is_err());
        assert!(MachineTargetCatalog::from_file(Path::new("catalog.json")).is_err());
    }
}
