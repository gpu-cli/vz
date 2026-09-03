use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use super::{
    SANDBOX_LABEL_PROJECT_DIR, SANDBOX_LABEL_SPACE_MODE, SANDBOX_SPACE_MODE_REQUIRED, Sandbox,
    SandboxBackend, SandboxState,
};

/// Current schema version for Developer Environment topology records.
pub const TOPOLOGY_SCHEMA_VERSION: u32 = 1;

const MAX_ID_LENGTH: usize = 128;
const MAX_NAME_LENGTH: usize = 128;
const LEGACY_DEVELOPER_MARKER: &str = "vz.run.workspace";
/// Maximum number of candidates returned by a topology selection error.
pub const MAX_TOPOLOGY_SELECTION_CANDIDATES: usize = 20;
const RESOURCE_NAME_VERSION_PREFIX: &str = "vzr1";
const RESOURCE_NAME_DIGEST_HEX_LENGTH: usize = 32;

macro_rules! topology_id {
    ($name:ident, $label:literal, $prefix:literal) => {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Create a validated identifier.
            pub fn new(value: impl Into<String>) -> Result<Self, TopologyValidationError> {
                let value = value.into();
                validate_identifier($label, &value)?;
                Ok(Self(value))
            }

            /// Generate a fresh opaque identity with a stable, type-specific prefix.
            pub fn generate() -> Self {
                Self(format!("{}{}", $prefix, Uuid::new_v4().simple()))
            }

            /// Borrow the stable wire representation.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            fn validate(&self) -> Result<(), TopologyValidationError> {
                validate_identifier($label, &self.0)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl TryFrom<String> for $name {
            type Error = TopologyValidationError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = TopologyValidationError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
    };
}

topology_id!(ProjectId, "project_id", "prj_");
topology_id!(EnvironmentId, "environment_id", "env_");
topology_id!(MachineId, "machine_id", "mch_");
topology_id!(MachineIncarnationId, "machine_incarnation_id", "inc_");
topology_id!(WorkspaceBindingId, "workspace_binding_id", "wsp_");
topology_id!(NetworkId, "network_id", "net_");
topology_id!(EndpointId, "endpoint_id", "end_");

/// Host or Machine operating system.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum OperatingSystem {
    Linux,
    Macos,
    Windows,
}

/// Host or Machine CPU architecture.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Architecture {
    Aarch64,
    X86_64,
}

/// Host tuple used only for explicit target compatibility validation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostSpec {
    pub os: OperatingSystem,
    pub arch: Architecture,
}

/// Immutable target artifact and platform requested for one Machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TargetSpec {
    pub os: OperatingSystem,
    pub arch: Architecture,
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

/// Target-qualified capability advertised or requested by a Machine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum MachineCapability {
    PosixExec,
    PosixPty,
    Signals,
    Files,
    Ports,
    DockerEngine,
    Compose,
    Buildx,
    Snapshot,
    Suspend,
    Checkpoint,
    Gui,
    WindowsConsole,
}

/// Explicit operating profile for one Machine.
///
/// Developer Linux Machines implicitly provide their own private Docker stack.
/// Hardened Machines are Linux-only and must never expose Docker, Compose, or
/// buildx capabilities.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum MachineProfile {
    Developer,
    Hardened,
}

/// Deterministically ordered set of Machine capabilities.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilitySet {
    #[serde(default)]
    pub capabilities: BTreeSet<MachineCapability>,
    /// Requested capabilities the backend could not negotiate, with a stable reason.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub unsupported: BTreeMap<MachineCapability, String>,
}

impl CapabilitySet {
    pub fn new(capabilities: impl IntoIterator<Item = MachineCapability>) -> Self {
        Self {
            capabilities: capabilities.into_iter().collect(),
            unsupported: BTreeMap::new(),
        }
    }

    pub fn contains(&self, capability: MachineCapability) -> bool {
        self.capabilities.contains(&capability)
    }

    pub fn unaccounted_by(&self, negotiated: &Self) -> Vec<MachineCapability> {
        self.capabilities
            .difference(&negotiated.capabilities)
            .filter(|capability| !negotiated.unsupported.contains_key(capability))
            .copied()
            .collect()
    }
}

/// Desired compute resources for one Machine.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MachineResources {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpus: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_bytes: Option<u64>,
}

/// How a workspace is projected into one Machine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceProjectionMode {
    ReadWrite,
    ReadOnly,
    Snapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceProjection {
    pub binding: String,
    pub target_path: String,
    pub mode: WorkspaceProjectionMode,
}

/// Desired Machine within the reusable Environment topology.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MachineSpec {
    pub schema_version: u32,
    pub name: String,
    pub profile: MachineProfile,
    pub target: TargetSpec,
    #[serde(default)]
    pub resources: MachineResources,
    #[serde(default)]
    pub requested_capabilities: CapabilitySet,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<WorkspaceProjection>,
}

/// Desired network kind. Data-plane behavior is implemented by later slices.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NetworkKind {
    Private,
    SimulatedPublic,
}

/// Desired Environment-local network.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkSpec {
    pub schema_version: u32,
    pub name: String,
    pub kind: NetworkKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cidr: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EndpointProtocol {
    Tcp,
    Udp,
    Http,
    Https,
}

/// Desired endpoint reference using topology-local Machine/network names.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EndpointSpec {
    pub schema_version: u32,
    pub name: String,
    pub machine: String,
    pub network: String,
    pub protocol: EndpointProtocol,
    pub port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
}

/// Reusable desired topology instantiated by each EnvironmentInstance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentSpec {
    pub schema_version: u32,
    pub machines: Vec<MachineSpec>,
    #[serde(default)]
    pub networks: Vec<NetworkSpec>,
    #[serde(default)]
    pub endpoints: Vec<EndpointSpec>,
}

/// Versioned, portable project definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectDefinition {
    pub schema_version: u32,
    pub project_id: ProjectId,
    pub name: String,
    pub environment: EnvironmentSpec,
}

/// A path-independent workspace association.
///
/// `workspace_key` is the authoritative opaque worktree token. `path_hint` is
/// non-authorizing diagnostic metadata and must never be used to adopt an
/// Environment or derive persistent identity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceBinding {
    pub schema_version: u32,
    pub binding_id: WorkspaceBindingId,
    pub project_id: ProjectId,
    pub environment_id: EnvironmentId,
    /// Symbolic slot referenced by `WorkspaceProjection.binding` in the definition.
    pub name: String,
    pub workspace_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_hint: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentState {
    Creating,
    Reconciling,
    Ready,
    Stopped,
    Deleting,
    Deleted,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MachineState {
    Creating,
    Ready,
    Stopped,
    Failed,
}

/// Resolved implementation serving a Machine. It is never part of TargetSpec identity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MachineBackend {
    MacosVirtualizationLinux,
    MacosNative,
    LinuxNative,
    WindowsLinux,
    WindowsNative,
    Other(String),
}

/// Replaceable runtime incarnation of a stable logical Machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MachineIncarnation {
    pub schema_version: u32,
    pub incarnation_id: MachineIncarnationId,
    pub machine_id: MachineId,
    pub generation: u64,
    pub created_at: u64,
}

/// Persisted logical Machine identity and negotiated target state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MachineInstance {
    pub schema_version: u32,
    pub machine_id: MachineId,
    pub environment_id: EnvironmentId,
    pub name: String,
    pub profile: MachineProfile,
    pub target: TargetSpec,
    #[serde(default)]
    pub resources: MachineResources,
    #[serde(default)]
    pub requested_capabilities: CapabilitySet,
    #[serde(default)]
    pub negotiated_capabilities: CapabilitySet,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<MachineBackend>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incarnation: Option<MachineIncarnation>,
    pub state: MachineState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_sandbox_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkInstance {
    pub schema_version: u32,
    pub network_id: NetworkId,
    pub environment_id: EnvironmentId,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EndpointInstance {
    pub schema_version: u32,
    pub endpoint_id: EndpointId,
    pub environment_id: EnvironmentId,
    pub machine_id: MachineId,
    pub network_id: NetworkId,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum OwnedResourceKind {
    Machine,
    Incarnation,
    Disk,
    Socket,
    DockerContext,
    Network,
    Endpoint,
    Credential,
    Fault,
    LegacySandbox,
    Other(String),
}

/// Minimal persisted ownership edge. Machine-owned resources always carry machine_id.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnershipRecord {
    pub schema_version: u32,
    pub resource_kind: OwnedResourceKind,
    pub resource_id: String,
    pub environment_id: EnvironmentId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_id: Option<MachineId>,
}

/// Stable ownership tuple for a physical resource.
///
/// Host paths and human names are intentionally absent so relocation cannot
/// change a resource name or cause cross-Environment adoption.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceOwner {
    pub project_id: ProjectId,
    pub environment_id: EnvironmentId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_id: Option<MachineId>,
}

impl ResourceOwner {
    /// Build a deterministic, bounded physical name for an owned resource.
    ///
    /// Callers must still collision-check this name in the physical backend
    /// namespace before mutation. A matching name may be reused only when the
    /// persisted owner tuple and logical identity are identical.
    pub fn bounded_resource_name(
        &self,
        resource_kind: &OwnedResourceKind,
        logical_identity: &str,
        max_bytes: usize,
    ) -> Result<String, TopologyValidationError> {
        if logical_identity.trim().is_empty() {
            return Err(TopologyValidationError::InvalidIdentifier {
                kind: "resource.logical_identity".to_string(),
                value: logical_identity.to_string(),
                reason: "must not be empty".to_string(),
            });
        }

        let minimum = RESOURCE_NAME_VERSION_PREFIX.len() + 1 + RESOURCE_NAME_DIGEST_HEX_LENGTH;
        if max_bytes < minimum {
            return Err(TopologyValidationError::InvalidIdentifier {
                kind: "resource.max_name_bytes".to_string(),
                value: max_bytes.to_string(),
                reason: format!("must be at least {minimum}"),
            });
        }

        let kind = resource_kind_identity(resource_kind);
        let machine_id = self
            .machine_id
            .as_ref()
            .map(MachineId::as_str)
            .unwrap_or("");
        let mut hasher = Sha256::new();
        hasher.update(b"vz.resource-name.v1\0");
        for field in [
            self.project_id.as_str(),
            self.environment_id.as_str(),
            machine_id,
            kind.as_str(),
            logical_identity,
        ] {
            hasher.update((field.len() as u64).to_le_bytes());
            hasher.update(field.as_bytes());
        }
        let digest = format!("{:x}", hasher.finalize());
        let digest = &digest[..RESOURCE_NAME_DIGEST_HEX_LENGTH];
        let readable = resource_name_slug(&format!("{kind}-{logical_identity}"));
        let readable_budget = max_bytes - minimum;
        if readable_budget == 0 {
            return Ok(format!("{RESOURCE_NAME_VERSION_PREFIX}-{digest}"));
        }
        let readable = &readable[..readable.len().min(readable_budget.saturating_sub(1))];
        if readable.is_empty() {
            Ok(format!("{RESOURCE_NAME_VERSION_PREFIX}-{digest}"))
        } else {
            Ok(format!(
                "{RESOURCE_NAME_VERSION_PREFIX}-{readable}-{digest}"
            ))
        }
    }
}

/// Provenance retained when a v0.3.20 Sandbox becomes a Developer Environment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LegacyMigrationProvenance {
    pub source_version: String,
    pub legacy_sandbox_id: String,
    #[serde(default)]
    pub unresolved_resources: Vec<String>,
}

/// Persisted Environment aggregate. Runtime mutation is implemented by follow-on issues.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentInstance {
    pub schema_version: u32,
    pub environment_id: EnvironmentId,
    pub project_id: ProjectId,
    pub name: String,
    pub definition_digest: String,
    pub state: EnvironmentState,
    #[serde(default)]
    pub bindings: Vec<WorkspaceBinding>,
    pub machines: Vec<MachineInstance>,
    #[serde(default)]
    pub networks: Vec<NetworkInstance>,
    #[serde(default)]
    pub endpoints: Vec<EndpointInstance>,
    #[serde(default)]
    pub ownership: Vec<OwnershipRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_migration: Option<LegacyMigrationProvenance>,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Definition plus all currently persisted instances for one project.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectState {
    pub schema_version: u32,
    pub definition: ProjectDefinition,
    #[serde(default)]
    pub environments: Vec<EnvironmentInstance>,
}

/// An explicit Environment selector. `NameOrId` exists for the single CLI
/// spelling and deliberately checks both namespaces rather than guessing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum EnvironmentSelector {
    Id(EnvironmentId),
    Name(String),
    NameOrId(String),
}

/// All process-local inputs used to select one Environment.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentSelectionContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explicit: Option<EnvironmentSelector>,
    /// Immutable ID received from the process-scoped `VZ_ENVIRONMENT_ID` selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_environment_id: Option<EnvironmentId>,
    /// Opaque token read from the resolved worktree's private Git metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_key: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentSelectionSource {
    Explicit,
    Process,
    Workspace,
}

/// Stable result of selecting an existing Environment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentSelection {
    pub project_id: ProjectId,
    pub environment_id: EnvironmentId,
    pub name: String,
    pub source: EnvironmentSelectionSource,
}

/// Read-only decision made before `up` starts any reconciliation or binding mutation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum EnvironmentUpDecision {
    Existing { selection: EnvironmentSelection },
    Create { name: String },
}

/// A structured resolution candidate; callers render these without parsing prose.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopologyCandidate {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Error)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum TopologyResolutionError {
    #[error("invalid {kind} selector `{selector}`: {reason}")]
    InvalidSelector {
        kind: String,
        selector: String,
        reason: String,
    },
    #[error("no {kind} matched selector `{selector}`")]
    NotFound { kind: String, selector: String },
    #[error("selector `{selector}` matched multiple {kind} candidates")]
    Ambiguous {
        kind: String,
        selector: String,
        candidates: Vec<TopologyCandidate>,
    },
    #[error("select a {kind} explicitly; {selector} does not identify an existing binding")]
    SelectionRequired {
        kind: String,
        selector: String,
        candidates: Vec<TopologyCandidate>,
    },
}

impl TopologyResolutionError {
    pub fn ambiguous(
        kind: impl Into<String>,
        selector: impl Into<String>,
        candidates: impl IntoIterator<Item = TopologyCandidate>,
    ) -> Self {
        let candidates = bounded_candidates(candidates);
        Self::Ambiguous {
            kind: kind.into(),
            selector: selector.into(),
            candidates,
        }
    }

    pub fn selection_required(
        kind: impl Into<String>,
        selector: impl Into<String>,
        candidates: impl IntoIterator<Item = TopologyCandidate>,
    ) -> Self {
        Self::SelectionRequired {
            kind: kind.into(),
            selector: selector.into(),
            candidates: bounded_candidates(candidates),
        }
    }
}

fn bounded_candidates(
    candidates: impl IntoIterator<Item = TopologyCandidate>,
) -> Vec<TopologyCandidate> {
    let mut candidates: Vec<_> = candidates.into_iter().collect();
    candidates.sort();
    candidates.dedup();
    candidates.truncate(MAX_TOPOLOGY_SELECTION_CANDIDATES);
    candidates
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Error)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum TopologyValidationError {
    #[error("unsupported topology schema version {found}; supported version is {supported}")]
    UnsupportedSchemaVersion { found: u32, supported: u32 },
    #[error("invalid {kind} `{value}`: {reason}")]
    InvalidIdentifier {
        kind: String,
        value: String,
        reason: String,
    },
    #[error("invalid {kind} name `{value}`")]
    InvalidName { kind: String, value: String },
    #[error("duplicate {kind} `{value}`")]
    Duplicate { kind: String, value: String },
    #[error("missing {kind} reference `{value}`")]
    MissingReference { kind: String, value: String },
    #[error("ownership mismatch for {kind} `{value}`")]
    OwnershipMismatch { kind: String, value: String },
    #[error("environment `{environment_id}` must contain at least one Machine")]
    MissingMachines { environment_id: String },
    #[error("Machine `{machine_id}` is missing required capability `{capability:?}`")]
    MissingCapability {
        machine_id: String,
        capability: MachineCapability,
    },
    #[error(
        "Machine `{machine_id}` reports capability `{capability:?}` as both supported and unsupported"
    )]
    ContradictoryCapability {
        machine_id: String,
        capability: MachineCapability,
    },
    #[error("invalid capability declaration for Machine `{machine_id}`: {reason}")]
    InvalidCapabilityDeclaration { machine_id: String, reason: String },
    #[error("invalid {profile:?} profile for Machine `{machine_id}`: {reason}")]
    InvalidMachineProfile {
        machine_id: String,
        profile: MachineProfile,
        reason: String,
    },
    #[error(
        "Environment `{environment_id}` definition digest mismatch: expected `{expected}`, found `{found}`"
    )]
    DefinitionDigestMismatch {
        environment_id: String,
        expected: String,
        found: String,
    },
    #[error("Environment `{environment_id}` does not instantiate its definition: {details}")]
    DefinitionTopologyMismatch {
        environment_id: String,
        details: String,
    },
    #[error("failed to serialize canonical project definition: {details}")]
    CanonicalSerialization { details: String },
    #[error(
        "unsupported host/target tuple: host={host_os:?}/{host_arch:?}, target={target_os:?}/{target_arch:?}"
    )]
    UnsupportedTarget {
        host_os: OperatingSystem,
        host_arch: Architecture,
        target_os: OperatingSystem,
        target_arch: Architecture,
        requested_capabilities: Vec<MachineCapability>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Error)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum LegacyMigrationError {
    #[error("legacy Sandbox `{sandbox_id}` is not a Developer Environment record")]
    NotDeveloper { sandbox_id: String },
    #[error("legacy Sandbox `{sandbox_id}` has both Developer and Hardened markers")]
    AmbiguousClassification { sandbox_id: String },
    #[error(
        "legacy Developer Sandbox `{sandbox_id}` backend `{backend:?}` has no authoritative v0.3.20 target architecture"
    )]
    UnresolvedTargetArchitecture {
        sandbox_id: String,
        backend: SandboxBackend,
    },
    #[error(transparent)]
    InvalidTopology(#[from] TopologyValidationError),
    #[error("failed to serialize migrated definition: {details}")]
    Serialization { details: String },
}

impl ProjectDefinition {
    pub fn validate(&self) -> Result<(), TopologyValidationError> {
        validate_schema(self.schema_version)?;
        self.project_id.validate()?;
        validate_name("project", &self.name)?;
        self.environment.validate()
    }

    /// Validate only the explicit host/target support matrix; never substitute a backend.
    pub fn validate_for_host(&self, host: HostSpec) -> Result<(), TopologyValidationError> {
        self.validate()?;
        for machine in &self.environment.machines {
            if !supported_target(host, &machine.target, &machine.requested_capabilities) {
                return Err(TopologyValidationError::UnsupportedTarget {
                    host_os: host.os,
                    host_arch: host.arch,
                    target_os: machine.target.os,
                    target_arch: machine.target.arch,
                    requested_capabilities: machine
                        .requested_capabilities
                        .capabilities
                        .iter()
                        .copied()
                        .collect(),
                });
            }
        }
        Ok(())
    }

    /// Return the stable digest persisted by each Environment instance.
    pub fn digest(&self) -> Result<String, TopologyValidationError> {
        let bytes = serde_json::to_vec(self).map_err(|error| {
            TopologyValidationError::CanonicalSerialization {
                details: error.to_string(),
            }
        })?;
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }

    /// Materialize a fresh, unbound Environment identity from this definition.
    ///
    /// Runtime negotiation and workspace authorization happen during later
    /// reconciliation. This constructor never derives identity from a path or
    /// human selector.
    pub fn instantiate_environment(
        &self,
        name: impl Into<String>,
        now: u64,
    ) -> Result<EnvironmentInstance, TopologyValidationError> {
        self.validate()?;
        let name = name.into();
        validate_name("environment", &name)?;
        let environment_id = EnvironmentId::generate();

        let machines: Vec<_> = self
            .environment
            .machines
            .iter()
            .map(|machine| MachineInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                machine_id: MachineId::generate(),
                environment_id: environment_id.clone(),
                name: machine.name.clone(),
                profile: machine.profile,
                target: machine.target.clone(),
                resources: machine.resources.clone(),
                requested_capabilities: machine.requested_capabilities.clone(),
                negotiated_capabilities: CapabilitySet::default(),
                backend: None,
                incarnation: None,
                state: MachineState::Creating,
                legacy_sandbox_id: None,
            })
            .collect();
        let machine_ids: BTreeMap<_, _> = machines
            .iter()
            .map(|machine| (machine.name.as_str(), machine.machine_id.clone()))
            .collect();

        let networks: Vec<_> = self
            .environment
            .networks
            .iter()
            .map(|network| NetworkInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                network_id: NetworkId::generate(),
                environment_id: environment_id.clone(),
                name: network.name.clone(),
            })
            .collect();
        let network_ids: BTreeMap<_, _> = networks
            .iter()
            .map(|network| (network.name.as_str(), network.network_id.clone()))
            .collect();

        let endpoints: Vec<_> = self
            .environment
            .endpoints
            .iter()
            .map(|endpoint| EndpointInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                endpoint_id: EndpointId::generate(),
                environment_id: environment_id.clone(),
                machine_id: machine_ids[endpoint.machine.as_str()].clone(),
                network_id: network_ids[endpoint.network.as_str()].clone(),
                name: endpoint.name.clone(),
            })
            .collect();

        let mut ownership: Vec<_> = machines
            .iter()
            .map(|machine| OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::Machine,
                resource_id: machine.machine_id.to_string(),
                environment_id: environment_id.clone(),
                machine_id: Some(machine.machine_id.clone()),
            })
            .collect();
        ownership.extend(networks.iter().map(|network| OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::Network,
            resource_id: network.network_id.to_string(),
            environment_id: environment_id.clone(),
            machine_id: None,
        }));
        ownership.extend(endpoints.iter().map(|endpoint| OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::Endpoint,
            resource_id: endpoint.endpoint_id.to_string(),
            environment_id: environment_id.clone(),
            machine_id: Some(endpoint.machine_id.clone()),
        }));

        let environment = EnvironmentInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            environment_id,
            project_id: self.project_id.clone(),
            name,
            definition_digest: self.digest()?,
            state: EnvironmentState::Creating,
            bindings: Vec::new(),
            machines,
            networks,
            endpoints,
            ownership,
            legacy_migration: None,
            created_at: now,
            updated_at: now,
        };
        environment.validate()?;
        validate_definition_instance(&self.environment, &environment)?;
        Ok(environment)
    }
}

impl EnvironmentSpec {
    pub fn validate(&self) -> Result<(), TopologyValidationError> {
        validate_schema(self.schema_version)?;
        if self.machines.is_empty() {
            return Err(TopologyValidationError::MissingMachines {
                environment_id: "definition".to_string(),
            });
        }
        let machine_names =
            validate_unique_names("machine", self.machines.iter().map(|m| &m.name))?;
        let network_names =
            validate_unique_names("network", self.networks.iter().map(|n| &n.name))?;
        validate_unique_names("endpoint", self.endpoints.iter().map(|e| &e.name))?;
        for machine in &self.machines {
            validate_schema(machine.schema_version)?;
            validate_name("machine", &machine.name)?;
            validate_target(&machine.target)?;
            validate_requested_capabilities(&machine.name, &machine.requested_capabilities)?;
            validate_machine_profile(
                &machine.name,
                machine.profile,
                &machine.target,
                &machine.requested_capabilities,
                None,
            )?;
        }
        for network in &self.networks {
            validate_schema(network.schema_version)?;
        }
        for endpoint in &self.endpoints {
            validate_schema(endpoint.schema_version)?;
            if !machine_names.contains(endpoint.machine.as_str()) {
                return Err(TopologyValidationError::MissingReference {
                    kind: "endpoint.machine".to_string(),
                    value: endpoint.machine.clone(),
                });
            }
            if !network_names.contains(endpoint.network.as_str()) {
                return Err(TopologyValidationError::MissingReference {
                    kind: "endpoint.network".to_string(),
                    value: endpoint.network.clone(),
                });
            }
            if endpoint.port == 0 {
                return Err(TopologyValidationError::InvalidIdentifier {
                    kind: "endpoint.port".to_string(),
                    value: "0".to_string(),
                    reason: "port must be non-zero".to_string(),
                });
            }
        }
        Ok(())
    }
}

impl ProjectState {
    pub fn validate(&self) -> Result<(), TopologyValidationError> {
        validate_schema(self.schema_version)?;
        self.definition.validate()?;
        let definition_digest = self.definition.digest()?;
        let mut environment_ids = BTreeSet::new();
        let mut environment_names = BTreeSet::new();
        let mut binding_ids = BTreeSet::new();
        for environment in &self.environments {
            environment.validate()?;
            if environment.project_id != self.definition.project_id {
                return Err(TopologyValidationError::OwnershipMismatch {
                    kind: "environment.project".to_string(),
                    value: environment.environment_id.to_string(),
                });
            }
            if environment.definition_digest != definition_digest {
                return Err(TopologyValidationError::DefinitionDigestMismatch {
                    environment_id: environment.environment_id.to_string(),
                    expected: definition_digest.clone(),
                    found: environment.definition_digest.clone(),
                });
            }
            validate_definition_instance(&self.definition.environment, environment)?;
            if !environment_ids.insert(environment.environment_id.as_str()) {
                return Err(TopologyValidationError::Duplicate {
                    kind: "environment_id".to_string(),
                    value: environment.environment_id.to_string(),
                });
            }
            if !environment_names.insert(environment.name.as_str()) {
                return Err(TopologyValidationError::Duplicate {
                    kind: "environment_name".to_string(),
                    value: environment.name.clone(),
                });
            }
            for binding in &environment.bindings {
                if !binding_ids.insert(binding.binding_id.as_str()) {
                    return Err(TopologyValidationError::Duplicate {
                        kind: "workspace_binding_id".to_string(),
                        value: binding.binding_id.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Resolve one existing Environment without mutating bindings or topology.
    pub fn resolve_environment(
        &self,
        context: &EnvironmentSelectionContext,
    ) -> Result<EnvironmentSelection, TopologyResolutionError> {
        if let Some(selector) = &context.explicit {
            return self
                .resolve_explicit_environment(selector)?
                .map(|environment| {
                    environment_selection(environment, EnvironmentSelectionSource::Explicit)
                })
                .ok_or_else(|| TopologyResolutionError::NotFound {
                    kind: "environment".to_string(),
                    selector: environment_selector_value(selector).to_string(),
                });
        }

        if let Some(environment_id) = &context.process_environment_id {
            environment_id.validate().map_err(|error| {
                TopologyResolutionError::InvalidSelector {
                    kind: "environment".to_string(),
                    selector: environment_id.to_string(),
                    reason: error.to_string(),
                }
            })?;
            return self
                .environments
                .iter()
                .find(|environment| {
                    environment.project_id == self.definition.project_id
                        && environment.environment_id == *environment_id
                })
                .map(|environment| {
                    environment_selection(environment, EnvironmentSelectionSource::Process)
                })
                .ok_or_else(|| TopologyResolutionError::NotFound {
                    kind: "environment".to_string(),
                    selector: environment_id.to_string(),
                });
        }

        if let Some(workspace_key) = context.workspace_key.as_deref() {
            validate_name("workspace_key", workspace_key).map_err(|error| {
                TopologyResolutionError::InvalidSelector {
                    kind: "workspace".to_string(),
                    selector: "workspace binding".to_string(),
                    reason: error.to_string(),
                }
            })?;
            let bound: Vec<_> = self
                .environments
                .iter()
                .filter(|environment| {
                    environment.project_id == self.definition.project_id
                        && environment.bindings.iter().any(|binding| {
                            binding.project_id == self.definition.project_id
                                && binding.workspace_key == workspace_key
                        })
                })
                .collect();
            match bound.as_slice() {
                [environment] => {
                    return Ok(environment_selection(
                        environment,
                        EnvironmentSelectionSource::Workspace,
                    ));
                }
                [] => {}
                _ => {
                    return Err(TopologyResolutionError::ambiguous(
                        "environment",
                        "workspace binding",
                        bound.into_iter().map(environment_candidate),
                    ));
                }
            }
        }

        Err(TopologyResolutionError::selection_required(
            "environment",
            "workspace binding",
            self.environments
                .iter()
                .filter(|environment| environment.project_id == self.definition.project_id)
                .map(environment_candidate),
        ))
    }

    /// Decide whether `up` selects an existing Environment or may create one.
    ///
    /// Only a missing explicit name (including a non-colliding `NameOrId`) or
    /// the empty-project `default` rule can create. A stale explicit/process ID
    /// never falls through to another tier or to creation.
    pub fn resolve_environment_for_up(
        &self,
        context: &EnvironmentSelectionContext,
    ) -> Result<EnvironmentUpDecision, TopologyResolutionError> {
        if let Some(selector) = &context.explicit {
            if let Some(environment) = self.resolve_explicit_environment(selector)? {
                return Ok(EnvironmentUpDecision::Existing {
                    selection: environment_selection(
                        environment,
                        EnvironmentSelectionSource::Explicit,
                    ),
                });
            }
            return match selector {
                EnvironmentSelector::Name(name) => {
                    validate_name("environment", name).map_err(|error| {
                        TopologyResolutionError::InvalidSelector {
                            kind: "environment".to_string(),
                            selector: name.clone(),
                            reason: error.to_string(),
                        }
                    })?;
                    Ok(EnvironmentUpDecision::Create { name: name.clone() })
                }
                EnvironmentSelector::NameOrId(value) => {
                    validate_name("environment", value).map_err(|error| {
                        TopologyResolutionError::InvalidSelector {
                            kind: "environment".to_string(),
                            selector: value.clone(),
                            reason: error.to_string(),
                        }
                    })?;
                    if is_generated_environment_id(value) {
                        Err(TopologyResolutionError::NotFound {
                            kind: "environment".to_string(),
                            selector: value.clone(),
                        })
                    } else {
                        Ok(EnvironmentUpDecision::Create {
                            name: value.clone(),
                        })
                    }
                }
                EnvironmentSelector::Id(environment_id) => Err(TopologyResolutionError::NotFound {
                    kind: "environment".to_string(),
                    selector: environment_id.to_string(),
                }),
            };
        }

        if context.process_environment_id.is_some() {
            return self
                .resolve_environment(context)
                .map(|selection| EnvironmentUpDecision::Existing { selection });
        }

        match self.resolve_environment(context) {
            Ok(selection) => Ok(EnvironmentUpDecision::Existing { selection }),
            Err(TopologyResolutionError::SelectionRequired { .. })
                if self.environments.is_empty() =>
            {
                Ok(EnvironmentUpDecision::Create {
                    name: "default".to_string(),
                })
            }
            Err(error) => Err(error),
        }
    }

    fn resolve_explicit_environment(
        &self,
        selector: &EnvironmentSelector,
    ) -> Result<Option<&EnvironmentInstance>, TopologyResolutionError> {
        match selector {
            EnvironmentSelector::Id(environment_id) => environment_id
                .validate()
                .map_err(|error| TopologyResolutionError::InvalidSelector {
                    kind: "environment".to_string(),
                    selector: environment_id.to_string(),
                    reason: error.to_string(),
                })
                .map(|()| {
                    self.environments.iter().find(|environment| {
                        environment.project_id == self.definition.project_id
                            && environment.environment_id == *environment_id
                    })
                }),
            EnvironmentSelector::Name(name) => {
                validate_name("environment", name).map_err(|error| {
                    TopologyResolutionError::InvalidSelector {
                        kind: "environment".to_string(),
                        selector: name.clone(),
                        reason: error.to_string(),
                    }
                })?;
                Ok(self.environments.iter().find(|environment| {
                    environment.project_id == self.definition.project_id
                        && environment.name == *name
                }))
            }
            EnvironmentSelector::NameOrId(value) => {
                validate_name("environment", value).map_err(|error| {
                    TopologyResolutionError::InvalidSelector {
                        kind: "environment".to_string(),
                        selector: value.clone(),
                        reason: error.to_string(),
                    }
                })?;
                let matches: Vec<_> = self
                    .environments
                    .iter()
                    .filter(|environment| {
                        environment.project_id == self.definition.project_id
                            && (environment.environment_id.as_str() == value
                                || environment.name == *value)
                    })
                    .collect();
                match matches.as_slice() {
                    [] => Ok(None),
                    [environment] => Ok(Some(environment)),
                    _ => Err(TopologyResolutionError::ambiguous(
                        "environment",
                        value,
                        matches.into_iter().map(environment_candidate),
                    )),
                }
            }
        }
    }
}

fn is_generated_environment_id(value: &str) -> bool {
    value.strip_prefix("env_").is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn environment_selector_value(selector: &EnvironmentSelector) -> &str {
    match selector {
        EnvironmentSelector::Id(id) => id.as_str(),
        EnvironmentSelector::Name(name) | EnvironmentSelector::NameOrId(name) => name,
    }
}

fn environment_candidate(environment: &EnvironmentInstance) -> TopologyCandidate {
    TopologyCandidate {
        id: environment.environment_id.to_string(),
        name: environment.name.clone(),
    }
}

fn environment_selection(
    environment: &EnvironmentInstance,
    source: EnvironmentSelectionSource,
) -> EnvironmentSelection {
    EnvironmentSelection {
        project_id: environment.project_id.clone(),
        environment_id: environment.environment_id.clone(),
        name: environment.name.clone(),
        source,
    }
}

impl EnvironmentInstance {
    pub fn validate(&self) -> Result<(), TopologyValidationError> {
        validate_schema(self.schema_version)?;
        self.environment_id.validate()?;
        self.project_id.validate()?;
        validate_name("environment", &self.name)?;
        if self.machines.is_empty() {
            return Err(TopologyValidationError::MissingMachines {
                environment_id: self.environment_id.to_string(),
            });
        }
        let mut machine_ids = BTreeSet::new();
        let mut machine_names = BTreeSet::new();
        for machine in &self.machines {
            machine.validate()?;
            if machine.environment_id != self.environment_id {
                return Err(TopologyValidationError::OwnershipMismatch {
                    kind: "machine.environment".to_string(),
                    value: machine.machine_id.to_string(),
                });
            }
            if !machine_ids.insert(machine.machine_id.as_str()) {
                return Err(TopologyValidationError::Duplicate {
                    kind: "machine_id".to_string(),
                    value: machine.machine_id.to_string(),
                });
            }
            if !machine_names.insert(machine.name.as_str()) {
                return Err(TopologyValidationError::Duplicate {
                    kind: "machine_name".to_string(),
                    value: machine.name.clone(),
                });
            }
        }
        for binding in &self.bindings {
            validate_schema(binding.schema_version)?;
            binding.binding_id.validate()?;
            if binding.project_id != self.project_id
                || binding.environment_id != self.environment_id
            {
                return Err(TopologyValidationError::OwnershipMismatch {
                    kind: "workspace_binding".to_string(),
                    value: binding.binding_id.to_string(),
                });
            }
            validate_name("workspace_binding", &binding.name)?;
            validate_name("workspace_key", &binding.workspace_key)?;
        }
        validate_unique_names(
            "workspace_binding",
            self.bindings.iter().map(|binding| &binding.name),
        )?;
        let mut network_ids = BTreeSet::new();
        let mut network_names = BTreeSet::new();
        for network in &self.networks {
            validate_schema(network.schema_version)?;
            network.network_id.validate()?;
            validate_name("network", &network.name)?;
            if network.environment_id != self.environment_id {
                return Err(TopologyValidationError::OwnershipMismatch {
                    kind: "network.environment".to_string(),
                    value: network.network_id.to_string(),
                });
            }
            if !network_ids.insert(network.network_id.as_str()) {
                return Err(TopologyValidationError::Duplicate {
                    kind: "network_id".to_string(),
                    value: network.network_id.to_string(),
                });
            }
            if !network_names.insert(network.name.as_str()) {
                return Err(TopologyValidationError::Duplicate {
                    kind: "network_name".to_string(),
                    value: network.name.clone(),
                });
            }
        }
        let mut endpoint_ids = BTreeSet::new();
        let mut endpoint_names = BTreeSet::new();
        for endpoint in &self.endpoints {
            validate_schema(endpoint.schema_version)?;
            endpoint.endpoint_id.validate()?;
            validate_name("endpoint", &endpoint.name)?;
            if endpoint.environment_id != self.environment_id {
                return Err(TopologyValidationError::OwnershipMismatch {
                    kind: "endpoint.environment".to_string(),
                    value: endpoint.endpoint_id.to_string(),
                });
            }
            if !machine_ids.contains(endpoint.machine_id.as_str()) {
                return Err(TopologyValidationError::MissingReference {
                    kind: "endpoint.machine_id".to_string(),
                    value: endpoint.machine_id.to_string(),
                });
            }
            if !network_ids.contains(endpoint.network_id.as_str()) {
                return Err(TopologyValidationError::MissingReference {
                    kind: "endpoint.network_id".to_string(),
                    value: endpoint.network_id.to_string(),
                });
            }
            if !endpoint_ids.insert(endpoint.endpoint_id.as_str()) {
                return Err(TopologyValidationError::Duplicate {
                    kind: "endpoint_id".to_string(),
                    value: endpoint.endpoint_id.to_string(),
                });
            }
            if !endpoint_names.insert(endpoint.name.as_str()) {
                return Err(TopologyValidationError::Duplicate {
                    kind: "endpoint_name".to_string(),
                    value: endpoint.name.clone(),
                });
            }
        }
        let mut ownership_keys = BTreeSet::new();
        for record in &self.ownership {
            validate_schema(record.schema_version)?;
            if record.environment_id != self.environment_id {
                return Err(TopologyValidationError::OwnershipMismatch {
                    kind: "resource.environment".to_string(),
                    value: record.resource_id.clone(),
                });
            }
            if let Some(machine_id) = &record.machine_id
                && !machine_ids.contains(machine_id.as_str())
            {
                return Err(TopologyValidationError::MissingReference {
                    kind: "resource.machine_id".to_string(),
                    value: machine_id.to_string(),
                });
            }
            if resource_kind_requires_machine(&record.resource_kind) && record.machine_id.is_none()
            {
                return Err(TopologyValidationError::OwnershipMismatch {
                    kind: "resource.machine".to_string(),
                    value: record.resource_id.clone(),
                });
            }
            if !ownership_keys.insert((&record.resource_kind, record.resource_id.as_str())) {
                return Err(TopologyValidationError::Duplicate {
                    kind: "ownership_resource".to_string(),
                    value: format!(
                        "{}:{}",
                        resource_kind_identity(&record.resource_kind),
                        record.resource_id
                    ),
                });
            }
        }
        Ok(())
    }
}

impl MachineInstance {
    pub fn validate(&self) -> Result<(), TopologyValidationError> {
        validate_schema(self.schema_version)?;
        self.machine_id.validate()?;
        self.environment_id.validate()?;
        validate_name("machine", &self.name)?;
        validate_target(&self.target)?;
        validate_requested_capabilities(&self.name, &self.requested_capabilities)?;
        // Creation can fail before capability negotiation completes. Persist that
        // failure record so reconciliation can diagnose or resume it; operational
        // Ready/Stopped Machines still require a complete negotiation result.
        let negotiation_complete =
            matches!(self.state, MachineState::Ready | MachineState::Stopped);
        validate_machine_profile(
            self.machine_id.as_str(),
            self.profile,
            &self.target,
            &self.requested_capabilities,
            negotiation_complete.then_some(&self.negotiated_capabilities),
        )?;
        if let Some(incarnation) = &self.incarnation {
            validate_schema(incarnation.schema_version)?;
            incarnation.incarnation_id.validate()?;
            if incarnation.machine_id != self.machine_id {
                return Err(TopologyValidationError::OwnershipMismatch {
                    kind: "incarnation.machine".to_string(),
                    value: incarnation.incarnation_id.to_string(),
                });
            }
        }
        if let Some(capability) =
            self.negotiated_capabilities
                .capabilities
                .iter()
                .find(|capability| {
                    self.negotiated_capabilities
                        .unsupported
                        .contains_key(capability)
                })
        {
            return Err(TopologyValidationError::ContradictoryCapability {
                machine_id: self.machine_id.to_string(),
                capability: *capability,
            });
        }
        for (capability, reason) in &self.negotiated_capabilities.unsupported {
            if reason.trim().is_empty() {
                return Err(TopologyValidationError::InvalidCapabilityDeclaration {
                    machine_id: self.machine_id.to_string(),
                    reason: format!("unsupported capability `{capability:?}` has an empty reason"),
                });
            }
            if !self.requested_capabilities.contains(*capability) {
                return Err(TopologyValidationError::InvalidCapabilityDeclaration {
                    machine_id: self.machine_id.to_string(),
                    reason: format!("unsupported capability `{capability:?}` was not requested"),
                });
            }
        }
        if negotiation_complete
            && let Some(capability) = self
                .requested_capabilities
                .unaccounted_by(&self.negotiated_capabilities)
                .into_iter()
                .next()
        {
            return Err(TopologyValidationError::MissingCapability {
                machine_id: self.machine_id.to_string(),
                capability,
            });
        }
        Ok(())
    }
}

fn validate_definition_instance(
    spec: &EnvironmentSpec,
    environment: &EnvironmentInstance,
) -> Result<(), TopologyValidationError> {
    let environment_id = environment.environment_id.to_string();
    let machines: BTreeMap<_, _> = environment
        .machines
        .iter()
        .map(|machine| (machine.name.as_str(), machine))
        .collect();
    if machines.len() != spec.machines.len() {
        return definition_topology_mismatch(
            &environment_id,
            "Machine names/count differ from the project definition",
        );
    }
    let binding_names: BTreeSet<_> = environment
        .bindings
        .iter()
        .map(|binding| binding.name.as_str())
        .collect();
    for desired in &spec.machines {
        let Some(actual) = machines.get(desired.name.as_str()) else {
            return definition_topology_mismatch(
                &environment_id,
                format!("missing Machine `{}`", desired.name),
            );
        };
        if actual.target != desired.target {
            return definition_topology_mismatch(
                &environment_id,
                format!("Machine `{}` target differs", desired.name),
            );
        }
        if actual.profile != desired.profile {
            return definition_topology_mismatch(
                &environment_id,
                format!("Machine `{}` profile differs", desired.name),
            );
        }
        if actual.requested_capabilities != desired.requested_capabilities {
            return definition_topology_mismatch(
                &environment_id,
                format!("Machine `{}` requested capabilities differ", desired.name),
            );
        }
        // A failed Environment may be the durable result of creation failing
        // before its workspace slot was reserved. All states reached after
        // successful creation keep the binding requirement, including Stopped
        // and Deleting, so lifecycle operations cannot shed selector authority.
        if !matches!(
            environment.state,
            EnvironmentState::Creating | EnvironmentState::Failed
        ) && let Some(workspace) = &desired.workspace
            && !binding_names.contains(workspace.binding.as_str())
        {
            return definition_topology_mismatch(
                &environment_id,
                format!(
                    "Machine `{}` references missing workspace binding `{}`",
                    desired.name, workspace.binding
                ),
            );
        }
    }

    let networks: BTreeMap<_, _> = environment
        .networks
        .iter()
        .map(|network| (network.name.as_str(), network))
        .collect();
    if networks.len() != spec.networks.len()
        || spec
            .networks
            .iter()
            .any(|network| !networks.contains_key(network.name.as_str()))
    {
        return definition_topology_mismatch(
            &environment_id,
            "Network names/count differ from the project definition",
        );
    }

    let machine_names_by_id: BTreeMap<_, _> = environment
        .machines
        .iter()
        .map(|machine| (machine.machine_id.as_str(), machine.name.as_str()))
        .collect();
    let network_names_by_id: BTreeMap<_, _> = environment
        .networks
        .iter()
        .map(|network| (network.network_id.as_str(), network.name.as_str()))
        .collect();
    let endpoints: BTreeMap<_, _> = environment
        .endpoints
        .iter()
        .map(|endpoint| (endpoint.name.as_str(), endpoint))
        .collect();
    if endpoints.len() != spec.endpoints.len() {
        return definition_topology_mismatch(
            &environment_id,
            "Endpoint names/count differ from the project definition",
        );
    }
    for desired in &spec.endpoints {
        let Some(actual) = endpoints.get(desired.name.as_str()) else {
            return definition_topology_mismatch(
                &environment_id,
                format!("missing Endpoint `{}`", desired.name),
            );
        };
        let actual_machine = machine_names_by_id.get(actual.machine_id.as_str()).copied();
        let actual_network = network_names_by_id.get(actual.network_id.as_str()).copied();
        if actual_machine != Some(desired.machine.as_str())
            || actual_network != Some(desired.network.as_str())
        {
            return definition_topology_mismatch(
                &environment_id,
                format!("Endpoint `{}` attachment differs", desired.name),
            );
        }
    }
    Ok(())
}

fn definition_topology_mismatch<T>(
    environment_id: &str,
    details: impl Into<String>,
) -> Result<T, TopologyValidationError> {
    Err(TopologyValidationError::DefinitionTopologyMismatch {
        environment_id: environment_id.to_string(),
        details: details.into(),
    })
}

fn validate_requested_capabilities(
    machine: &str,
    capabilities: &CapabilitySet,
) -> Result<(), TopologyValidationError> {
    if capabilities.unsupported.is_empty() {
        return Ok(());
    }
    Err(TopologyValidationError::InvalidCapabilityDeclaration {
        machine_id: machine.to_string(),
        reason: "requested capabilities cannot contain unsupported results".to_string(),
    })
}

fn validate_machine_profile(
    machine: &str,
    profile: MachineProfile,
    target: &TargetSpec,
    requested: &CapabilitySet,
    negotiated: Option<&CapabilitySet>,
) -> Result<(), TopologyValidationError> {
    const DOCKER_CAPABILITIES: [MachineCapability; 3] = [
        MachineCapability::DockerEngine,
        MachineCapability::Compose,
        MachineCapability::Buildx,
    ];

    if target.os != OperatingSystem::Linux && profile != MachineProfile::Developer {
        return Err(TopologyValidationError::InvalidMachineProfile {
            machine_id: machine.to_string(),
            profile,
            reason: "native targets support only the Developer profile".to_string(),
        });
    }

    match profile {
        MachineProfile::Developer if target.os == OperatingSystem::Linux => {
            if let Some(negotiated) = negotiated {
                for capability in DOCKER_CAPABILITIES {
                    if negotiated.contains(capability) {
                        continue;
                    }
                    return Err(TopologyValidationError::MissingCapability {
                        machine_id: machine.to_string(),
                        capability,
                    });
                }
            }
        }
        MachineProfile::Developer => {
            for capabilities in std::iter::once(requested).chain(negotiated) {
                if let Some(capability) = DOCKER_CAPABILITIES.into_iter().find(|capability| {
                    capabilities.contains(*capability)
                        || capabilities.unsupported.contains_key(capability)
                }) {
                    return Err(TopologyValidationError::InvalidCapabilityDeclaration {
                        machine_id: machine.to_string(),
                        reason: format!(
                            "non-Linux target cannot declare implicit capability `{capability:?}`"
                        ),
                    });
                }
            }
        }
        MachineProfile::Hardened => {
            for capabilities in std::iter::once(requested).chain(negotiated) {
                if let Some(capability) = DOCKER_CAPABILITIES.into_iter().find(|capability| {
                    capabilities.contains(*capability)
                        || capabilities.unsupported.contains_key(capability)
                }) {
                    return Err(TopologyValidationError::InvalidMachineProfile {
                        machine_id: machine.to_string(),
                        profile,
                        reason: format!(
                            "Hardened Machines cannot declare capability `{capability:?}`"
                        ),
                    });
                }
            }
        }
    }
    Ok(())
}

/// Deterministically migrate one persisted v0.3.20 Developer Sandbox.
pub fn migrate_legacy_developer_sandbox(
    sandbox: &Sandbox,
) -> Result<ProjectState, LegacyMigrationError> {
    let developer = sandbox.labels.contains_key(LEGACY_DEVELOPER_MARKER);
    let hardened = sandbox
        .labels
        .get(SANDBOX_LABEL_SPACE_MODE)
        .is_some_and(|value| value == SANDBOX_SPACE_MODE_REQUIRED);
    if developer && hardened {
        return Err(LegacyMigrationError::AmbiguousClassification {
            sandbox_id: sandbox.sandbox_id.clone(),
        });
    }
    if !developer || hardened {
        return Err(LegacyMigrationError::NotDeveloper {
            sandbox_id: sandbox.sandbox_id.clone(),
        });
    }

    // The official v0.3.20 macOS release was Apple-Silicon-only, so its
    // Virtualization.framework backend is authoritative evidence for a Linux
    // aarch64 target. Legacy Linux/custom records did not persist architecture;
    // never invent one from the host that happens to open the relocated DB.
    let backend = match &sandbox.backend {
        SandboxBackend::MacosVz => MachineBackend::MacosVirtualizationLinux,
        SandboxBackend::LinuxFirecracker | SandboxBackend::Other(_) => {
            return Err(LegacyMigrationError::UnresolvedTargetArchitecture {
                sandbox_id: sandbox.sandbox_id.clone(),
                backend: sandbox.backend.clone(),
            });
        }
    };

    let project_id = ProjectId::new(legacy_id("prj", &sandbox.sandbox_id))?;
    let environment_id = EnvironmentId::new(legacy_id("env", &sandbox.sandbox_id))?;
    let machine_id = MachineId::new(legacy_id("mac", &sandbox.sandbox_id))?;
    let binding_id = WorkspaceBindingId::new(legacy_id("wsp", &sandbox.sandbox_id))?;
    let image = sandbox
        .spec
        .base_image_ref
        .clone()
        .unwrap_or_else(|| "legacy-unresolved".to_string());
    let target = TargetSpec {
        os: OperatingSystem::Linux,
        arch: Architecture::Aarch64,
        image,
        version: None,
        channel: None,
        digest: None,
    };
    let requested = CapabilitySet::new([
        MachineCapability::PosixExec,
        MachineCapability::PosixPty,
        MachineCapability::Signals,
        MachineCapability::Files,
        MachineCapability::Ports,
        MachineCapability::DockerEngine,
        MachineCapability::Compose,
        MachineCapability::Buildx,
    ]);
    let (environment_state, machine_state) = legacy_state(sandbox.state);
    let machine_spec = MachineSpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        name: "linux".to_string(),
        profile: MachineProfile::Developer,
        target: target.clone(),
        resources: MachineResources {
            cpus: sandbox.spec.cpus,
            memory_mb: sandbox.spec.memory_mb,
            disk_bytes: None,
        },
        requested_capabilities: requested.clone(),
        workspace: Some(WorkspaceProjection {
            binding: "workspace".to_string(),
            target_path: sandbox
                .labels
                .get(LEGACY_DEVELOPER_MARKER)
                .cloned()
                .unwrap_or_else(|| "/workspace".to_string()),
            mode: WorkspaceProjectionMode::ReadWrite,
        }),
    };
    let environment_spec = EnvironmentSpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        machines: vec![machine_spec],
        networks: Vec::new(),
        endpoints: Vec::new(),
    };
    let definition = ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: project_id.clone(),
        name: sandbox.sandbox_id.clone(),
        environment: environment_spec,
    };
    let definition_digest = canonical_digest(&definition)?;
    let binding = WorkspaceBinding {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        binding_id,
        project_id: project_id.clone(),
        environment_id: environment_id.clone(),
        name: "workspace".to_string(),
        workspace_key: format!("legacy:{}", sandbox.sandbox_id),
        path_hint: sandbox.labels.get(SANDBOX_LABEL_PROJECT_DIR).cloned(),
    };
    let machine = MachineInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        machine_id: machine_id.clone(),
        environment_id: environment_id.clone(),
        name: "linux".to_string(),
        profile: MachineProfile::Developer,
        target,
        resources: MachineResources {
            cpus: sandbox.spec.cpus,
            memory_mb: sandbox.spec.memory_mb,
            disk_bytes: None,
        },
        requested_capabilities: requested.clone(),
        negotiated_capabilities: requested,
        backend: Some(backend),
        incarnation: None,
        state: machine_state,
        legacy_sandbox_id: Some(sandbox.sandbox_id.clone()),
    };
    let environment = EnvironmentInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        environment_id: environment_id.clone(),
        project_id: project_id.clone(),
        name: "default".to_string(),
        definition_digest,
        state: environment_state,
        bindings: vec![binding],
        machines: vec![machine],
        networks: Vec::new(),
        endpoints: Vec::new(),
        ownership: vec![OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::LegacySandbox,
            resource_id: sandbox.sandbox_id.clone(),
            environment_id,
            machine_id: Some(machine_id),
        }],
        legacy_migration: Some(LegacyMigrationProvenance {
            source_version: "v0.3.20".to_string(),
            legacy_sandbox_id: sandbox.sandbox_id.clone(),
            unresolved_resources: vec![
                "host_mount_sources".to_string(),
                "persistent_disk_path".to_string(),
                "published_ports".to_string(),
                "target_image_digest".to_string(),
            ],
        }),
        created_at: sandbox.created_at,
        updated_at: sandbox.updated_at,
    };
    let state = ProjectState {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        definition,
        environments: vec![environment],
    };
    state.validate()?;
    Ok(state)
}

fn supported_target(host: HostSpec, target: &TargetSpec, capabilities: &CapabilitySet) -> bool {
    if host.arch != target.arch {
        return false;
    }
    let os_supported = matches!(
        (host.os, target.os),
        (OperatingSystem::Macos, OperatingSystem::Linux)
            | (OperatingSystem::Macos, OperatingSystem::Macos)
            | (OperatingSystem::Linux, OperatingSystem::Linux)
            | (OperatingSystem::Windows, OperatingSystem::Linux)
            | (OperatingSystem::Windows, OperatingSystem::Windows)
    );
    if !os_supported {
        return false;
    }
    if target.os != OperatingSystem::Linux
        && (capabilities.contains(MachineCapability::DockerEngine)
            || capabilities.contains(MachineCapability::Compose)
            || capabilities.contains(MachineCapability::Buildx))
    {
        return false;
    }
    true
}

fn resource_kind_requires_machine(kind: &OwnedResourceKind) -> bool {
    matches!(
        kind,
        OwnedResourceKind::Machine
            | OwnedResourceKind::Incarnation
            | OwnedResourceKind::Disk
            | OwnedResourceKind::Socket
            | OwnedResourceKind::DockerContext
            | OwnedResourceKind::Endpoint
            | OwnedResourceKind::LegacySandbox
    )
}

fn validate_schema(version: u32) -> Result<(), TopologyValidationError> {
    if version != TOPOLOGY_SCHEMA_VERSION {
        return Err(TopologyValidationError::UnsupportedSchemaVersion {
            found: version,
            supported: TOPOLOGY_SCHEMA_VERSION,
        });
    }
    Ok(())
}

fn validate_identifier(kind: &str, value: &str) -> Result<(), TopologyValidationError> {
    let valid = !value.is_empty()
        && value.len() <= MAX_ID_LENGTH
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'));
    if !valid {
        return Err(TopologyValidationError::InvalidIdentifier {
            kind: kind.to_string(),
            value: value.to_string(),
            reason: format!(
                "must be 1..={MAX_ID_LENGTH} ASCII alphanumeric, '-', '_', '.', or ':' characters"
            ),
        });
    }
    Ok(())
}

fn resource_kind_identity(kind: &OwnedResourceKind) -> String {
    match kind {
        OwnedResourceKind::Machine => "machine".to_string(),
        OwnedResourceKind::Incarnation => "incarnation".to_string(),
        OwnedResourceKind::Disk => "disk".to_string(),
        OwnedResourceKind::Socket => "socket".to_string(),
        OwnedResourceKind::DockerContext => "docker_context".to_string(),
        OwnedResourceKind::Network => "network".to_string(),
        OwnedResourceKind::Endpoint => "endpoint".to_string(),
        OwnedResourceKind::Credential => "credential".to_string(),
        OwnedResourceKind::Fault => "fault".to_string(),
        OwnedResourceKind::LegacySandbox => "legacy_sandbox".to_string(),
        OwnedResourceKind::Other(value) => format!("other:{value}"),
    }
}

fn resource_name_slug(value: &str) -> String {
    let mut slug = String::with_capacity(value.len());
    let mut previous_was_dash = false;
    for byte in value.bytes() {
        let mapped = if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.') {
            byte as char
        } else {
            '-'
        };
        if mapped == '-' && previous_was_dash {
            continue;
        }
        previous_was_dash = mapped == '-';
        slug.push(mapped);
    }
    slug.trim_matches('-').to_string()
}

fn validate_name(kind: &str, value: &str) -> Result<(), TopologyValidationError> {
    if value.trim().is_empty() || value.len() > MAX_NAME_LENGTH {
        return Err(TopologyValidationError::InvalidName {
            kind: kind.to_string(),
            value: value.to_string(),
        });
    }
    Ok(())
}

fn validate_unique_names<'a>(
    kind: &str,
    names: impl IntoIterator<Item = &'a String>,
) -> Result<BTreeSet<&'a str>, TopologyValidationError> {
    let mut unique = BTreeSet::new();
    for name in names {
        validate_name(kind, name)?;
        if !unique.insert(name.as_str()) {
            return Err(TopologyValidationError::Duplicate {
                kind: format!("{kind}_name"),
                value: name.clone(),
            });
        }
    }
    Ok(unique)
}

fn validate_target(target: &TargetSpec) -> Result<(), TopologyValidationError> {
    if target.image.trim().is_empty() {
        return Err(TopologyValidationError::InvalidIdentifier {
            kind: "target.image".to_string(),
            value: target.image.clone(),
            reason: "image must not be empty".to_string(),
        });
    }
    Ok(())
}

fn legacy_id(prefix: &str, legacy_sandbox_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"vz.topology.v1\0");
    hasher.update(prefix.as_bytes());
    hasher.update(b"\0");
    hasher.update(legacy_sandbox_id.as_bytes());
    let digest = hasher.finalize();
    let suffix: String = digest[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("{prefix}_{suffix}")
}

fn canonical_digest(definition: &ProjectDefinition) -> Result<String, LegacyMigrationError> {
    definition
        .digest()
        .map_err(|error| LegacyMigrationError::Serialization {
            details: error.to_string(),
        })
}

fn legacy_state(state: SandboxState) -> (EnvironmentState, MachineState) {
    match state {
        SandboxState::Creating => (EnvironmentState::Creating, MachineState::Creating),
        SandboxState::Ready => (EnvironmentState::Ready, MachineState::Ready),
        SandboxState::Draining | SandboxState::Terminated => {
            (EnvironmentState::Stopped, MachineState::Stopped)
        }
        SandboxState::Failed => (EnvironmentState::Failed, MachineState::Failed),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::{SandboxSpec, SandboxVolumeMount};

    fn linux_spec(name: &str) -> MachineSpec {
        MachineSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            name: name.to_string(),
            profile: MachineProfile::Developer,
            target: TargetSpec {
                os: OperatingSystem::Linux,
                arch: Architecture::Aarch64,
                image: "ubuntu:24.04".to_string(),
                version: Some("24.04".to_string()),
                channel: Some("stable".to_string()),
                digest: Some("sha256:linux".to_string()),
            },
            resources: MachineResources {
                cpus: Some(4),
                memory_mb: Some(8192),
                disk_bytes: Some(20 * 1024 * 1024 * 1024),
            },
            requested_capabilities: CapabilitySet::new([
                MachineCapability::PosixExec,
                MachineCapability::DockerEngine,
                MachineCapability::Compose,
                MachineCapability::Buildx,
            ]),
            workspace: Some(WorkspaceProjection {
                binding: "source".to_string(),
                target_path: "/workspace".to_string(),
                mode: WorkspaceProjectionMode::ReadWrite,
            }),
        }
    }

    fn project_definition() -> ProjectDefinition {
        ProjectDefinition {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: ProjectId::new("prj_shop").unwrap(),
            name: "shop".to_string(),
            environment: EnvironmentSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                machines: vec![
                    linux_spec("api"),
                    MachineSpec {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        name: "ios".to_string(),
                        profile: MachineProfile::Developer,
                        target: TargetSpec {
                            os: OperatingSystem::Macos,
                            arch: Architecture::Aarch64,
                            image: "macos-26".to_string(),
                            version: Some("26.0".to_string()),
                            channel: None,
                            digest: Some("sha256:macos".to_string()),
                        },
                        resources: MachineResources::default(),
                        requested_capabilities: CapabilitySet::new([
                            MachineCapability::PosixExec,
                            MachineCapability::Gui,
                        ]),
                        workspace: None,
                    },
                ],
                networks: vec![NetworkSpec {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    name: "private".to_string(),
                    kind: NetworkKind::Private,
                    cidr: Some("10.42.0.0/24".to_string()),
                }],
                endpoints: vec![EndpointSpec {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    name: "api".to_string(),
                    machine: "api".to_string(),
                    network: "private".to_string(),
                    protocol: EndpointProtocol::Https,
                    port: 443,
                    hostname: Some("api.shop.test".to_string()),
                }],
            },
        }
    }

    fn legacy_sandbox(path: &str) -> Sandbox {
        Sandbox {
            sandbox_id: "vz-run-shop-deadbeef0001".to_string(),
            backend: SandboxBackend::MacosVz,
            spec: SandboxSpec {
                cpus: Some(4),
                memory_mb: Some(8192),
                base_image_ref: Some("ubuntu:24.04".to_string()),
                main_container: None,
                network_profile: None,
                volume_mounts: vec![SandboxVolumeMount {
                    volume_id: "legacy-volume".to_string(),
                    target: "/workspace".to_string(),
                    read_only: false,
                }],
            },
            state: SandboxState::Ready,
            created_at: 100,
            updated_at: 200,
            labels: BTreeMap::from([
                (
                    LEGACY_DEVELOPER_MARKER.to_string(),
                    "/workspace".to_string(),
                ),
                (SANDBOX_LABEL_PROJECT_DIR.to_string(), path.to_string()),
            ]),
        }
    }

    #[test]
    fn project_definition_json_round_trip_is_lossless() {
        let definition = project_definition();
        definition.validate().unwrap();
        let json = serde_json::to_string_pretty(&definition).unwrap();
        let decoded: ProjectDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, definition);
        assert_eq!(
            serde_json::to_value(&definition).unwrap()["environment"]["machines"][0]["profile"],
            "developer"
        );
    }

    #[test]
    fn profile_is_required_in_definition_and_instance_json() {
        let mut definition = serde_json::to_value(project_definition()).unwrap();
        definition["environment"]["machines"][0]
            .as_object_mut()
            .unwrap()
            .remove("profile");
        assert!(serde_json::from_value::<ProjectDefinition>(definition).is_err());

        let migrated = migrate_legacy_developer_sandbox(&legacy_sandbox("/shop")).unwrap();
        let mut machine = serde_json::to_value(&migrated.environments[0].machines[0]).unwrap();
        machine.as_object_mut().unwrap().remove("profile");
        assert!(serde_json::from_value::<MachineInstance>(machine).is_err());
    }

    #[test]
    fn machine_profiles_enforce_target_and_docker_contract() {
        let mut implicit_docker = project_definition();
        for capability in [
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ] {
            implicit_docker.environment.machines[0]
                .requested_capabilities
                .capabilities
                .remove(&capability);
        }
        implicit_docker.validate().unwrap();

        let mut hardened = project_definition();
        let hardened_machine = &mut hardened.environment.machines[0];
        hardened_machine.profile = MachineProfile::Hardened;
        for capability in [
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ] {
            hardened_machine
                .requested_capabilities
                .capabilities
                .remove(&capability);
        }
        hardened.validate().unwrap();

        hardened.environment.machines[0]
            .requested_capabilities
            .capabilities
            .insert(MachineCapability::DockerEngine);
        assert!(matches!(
            hardened.validate(),
            Err(TopologyValidationError::InvalidMachineProfile {
                profile: MachineProfile::Hardened,
                ..
            })
        ));

        let mut native_hardened = project_definition();
        native_hardened.environment.machines[1].profile = MachineProfile::Hardened;
        assert!(matches!(
            native_hardened.validate(),
            Err(TopologyValidationError::InvalidMachineProfile {
                profile: MachineProfile::Hardened,
                ..
            })
        ));
    }

    #[test]
    fn machine_instance_profiles_enforce_negotiation_and_definition_match() {
        let migrated = migrate_legacy_developer_sandbox(&legacy_sandbox("/shop")).unwrap();
        let mut developer = migrated.environments[0].machines[0].clone();
        for capability in [
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ] {
            developer
                .requested_capabilities
                .capabilities
                .remove(&capability);
        }
        developer.validate().unwrap();
        developer
            .negotiated_capabilities
            .capabilities
            .remove(&MachineCapability::Compose);
        assert!(matches!(
            developer.validate(),
            Err(TopologyValidationError::MissingCapability {
                capability: MachineCapability::Compose,
                ..
            })
        ));

        let mut hardened = migrated.environments[0].machines[0].clone();
        hardened.profile = MachineProfile::Hardened;
        for capability in [
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ] {
            hardened
                .requested_capabilities
                .capabilities
                .remove(&capability);
            hardened
                .negotiated_capabilities
                .capabilities
                .remove(&capability);
        }
        hardened.validate().unwrap();

        let mut drift = migrated;
        drift.environments[0].machines[0] = hardened;
        assert!(matches!(
            drift.validate(),
            Err(TopologyValidationError::DefinitionTopologyMismatch { details, .. })
                if details.contains("profile differs")
        ));
    }

    #[test]
    fn macos_arm64_accepts_linux_and_macos_targets() {
        project_definition()
            .validate_for_host(HostSpec {
                os: OperatingSystem::Macos,
                arch: Architecture::Aarch64,
            })
            .unwrap();
    }

    #[test]
    fn unsupported_target_is_structured_and_never_substituted() {
        let mut definition = project_definition();
        definition.environment.machines[0].target.os = OperatingSystem::Windows;
        for capability in [
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ] {
            definition.environment.machines[0]
                .requested_capabilities
                .capabilities
                .remove(&capability);
        }
        let error = definition
            .validate_for_host(HostSpec {
                os: OperatingSystem::Macos,
                arch: Architecture::Aarch64,
            })
            .unwrap_err();
        assert!(matches!(
            error,
            TopologyValidationError::UnsupportedTarget {
                host_os: OperatingSystem::Macos,
                target_os: OperatingSystem::Windows,
                ..
            }
        ));
    }

    #[test]
    fn native_macos_cannot_request_implicit_docker() {
        let mut definition = project_definition();
        definition.environment.machines[1].requested_capabilities =
            CapabilitySet::new([MachineCapability::DockerEngine]);
        assert!(matches!(
            definition.validate_for_host(HostSpec {
                os: OperatingSystem::Macos,
                arch: Architecture::Aarch64,
            }),
            Err(TopologyValidationError::InvalidCapabilityDeclaration { .. })
        ));
    }

    #[test]
    fn duplicate_machine_names_and_missing_endpoint_references_fail() {
        let mut definition = project_definition();
        definition.environment.machines[1].name = "api".to_string();
        assert!(matches!(
            definition.validate(),
            Err(TopologyValidationError::Duplicate { .. })
        ));

        let mut definition = project_definition();
        definition.environment.endpoints[0].network = "missing".to_string();
        assert!(matches!(
            definition.validate(),
            Err(TopologyValidationError::MissingReference { .. })
        ));
    }

    #[test]
    fn schema_zero_and_malformed_ids_fail() {
        let mut definition = project_definition();
        definition.schema_version = 0;
        assert!(matches!(
            definition.validate(),
            Err(TopologyValidationError::UnsupportedSchemaVersion { found: 0, .. })
        ));
        assert!(ProjectId::new("not valid/id").is_err());
    }

    #[test]
    fn ambiguous_candidates_are_sorted_and_serialize_structurally() {
        let error = TopologyResolutionError::ambiguous(
            "environment",
            "shop",
            [
                TopologyCandidate {
                    id: "env_b".to_string(),
                    name: "b".to_string(),
                },
                TopologyCandidate {
                    id: "env_a".to_string(),
                    name: "a".to_string(),
                },
            ],
        );
        let json = serde_json::to_value(&error).unwrap();
        assert_eq!(json["code"], "ambiguous");
        assert_eq!(json["candidates"][0]["id"], "env_a");
    }

    #[test]
    fn legacy_migration_is_deterministic_and_path_independent() {
        let first = migrate_legacy_developer_sandbox(&legacy_sandbox("/old/shop")).unwrap();
        let second = migrate_legacy_developer_sandbox(&legacy_sandbox("/new/shop")).unwrap();
        assert_eq!(first.definition.project_id, second.definition.project_id);
        assert_eq!(
            first.environments[0].environment_id,
            second.environments[0].environment_id
        );
        assert_eq!(
            first.environments[0].machines[0].machine_id,
            second.environments[0].machines[0].machine_id
        );
        assert_ne!(
            first.environments[0].bindings[0].path_hint,
            second.environments[0].bindings[0].path_hint
        );
        let machine = &first.environments[0].machines[0];
        assert_eq!(
            first.definition.environment.machines[0].profile,
            MachineProfile::Developer
        );
        assert_eq!(machine.profile, MachineProfile::Developer);
        assert_eq!(machine.target.version, None);
        assert!(
            machine
                .negotiated_capabilities
                .contains(MachineCapability::DockerEngine)
        );
        assert!(
            first.environments[0]
                .legacy_migration
                .as_ref()
                .unwrap()
                .unresolved_resources
                .contains(&"target_image_digest".to_string())
        );
    }

    #[test]
    fn legacy_developer_target_architecture_requires_authoritative_backend_provenance() {
        let macos = migrate_legacy_developer_sandbox(&legacy_sandbox("/shop")).unwrap();
        let machine = &macos.environments[0].machines[0];
        assert_eq!(machine.target.os, OperatingSystem::Linux);
        assert_eq!(machine.target.arch, Architecture::Aarch64);
        assert_eq!(
            machine.backend,
            Some(MachineBackend::MacosVirtualizationLinux)
        );

        for backend in [
            SandboxBackend::LinuxFirecracker,
            SandboxBackend::Other("custom".to_string()),
        ] {
            let mut legacy = legacy_sandbox("/shop");
            legacy.backend = backend.clone();
            assert!(matches!(
                migrate_legacy_developer_sandbox(&legacy),
                Err(LegacyMigrationError::UnresolvedTargetArchitecture {
                    sandbox_id,
                    backend: unresolved,
                }) if sandbox_id == legacy.sandbox_id && unresolved == backend
            ));
        }
    }

    #[test]
    fn hardened_and_ambiguous_legacy_records_are_never_adopted() {
        let mut generic = legacy_sandbox("/shop");
        generic.labels.remove(LEGACY_DEVELOPER_MARKER);
        assert!(matches!(
            migrate_legacy_developer_sandbox(&generic),
            Err(LegacyMigrationError::NotDeveloper { .. })
        ));

        let mut hardened = legacy_sandbox("/shop");
        hardened.labels.remove(LEGACY_DEVELOPER_MARKER);
        hardened.labels.insert(
            SANDBOX_LABEL_SPACE_MODE.to_string(),
            SANDBOX_SPACE_MODE_REQUIRED.to_string(),
        );
        assert!(matches!(
            migrate_legacy_developer_sandbox(&hardened),
            Err(LegacyMigrationError::NotDeveloper { .. })
        ));

        hardened.labels.insert(
            LEGACY_DEVELOPER_MARKER.to_string(),
            "/workspace".to_string(),
        );
        assert!(matches!(
            migrate_legacy_developer_sandbox(&hardened),
            Err(LegacyMigrationError::AmbiguousClassification { .. })
        ));
    }

    #[test]
    fn missing_negotiated_capability_is_machine_qualified() {
        let migrated = migrate_legacy_developer_sandbox(&legacy_sandbox("/shop")).unwrap();
        let mut machine = migrated.environments[0].machines[0].clone();
        machine.negotiated_capabilities = CapabilitySet::default();
        assert!(matches!(
            machine.validate(),
            Err(TopologyValidationError::MissingCapability { machine_id, .. })
                if machine_id == machine.machine_id.as_str()
        ));
    }

    #[test]
    fn unsupported_capability_reason_accounts_for_request_without_contradiction() {
        let migrated = migrate_legacy_developer_sandbox(&legacy_sandbox("/shop")).unwrap();
        let mut machine = migrated.environments[0].machines[0].clone();
        let capability = MachineCapability::Ports;
        machine
            .negotiated_capabilities
            .capabilities
            .remove(&capability);
        machine
            .negotiated_capabilities
            .unsupported
            .insert(capability, "backend does not expose host ports".to_string());
        machine.validate().unwrap();

        machine
            .negotiated_capabilities
            .capabilities
            .insert(capability);
        assert!(matches!(
            machine.validate(),
            Err(TopologyValidationError::ContradictoryCapability {
                capability: MachineCapability::Ports,
                ..
            })
        ));
    }

    #[test]
    fn capability_declarations_reject_empty_unrequested_and_non_linux_docker() {
        let migrated = migrate_legacy_developer_sandbox(&legacy_sandbox("/shop")).unwrap();
        let mut machine = migrated.environments[0].machines[0].clone();
        machine
            .negotiated_capabilities
            .unsupported
            .insert(MachineCapability::Checkpoint, String::new());
        assert!(matches!(
            machine.validate(),
            Err(TopologyValidationError::InvalidCapabilityDeclaration { .. })
        ));

        let mut machine = migrated.environments[0].machines[0].clone();
        machine
            .negotiated_capabilities
            .unsupported
            .insert(MachineCapability::Checkpoint, "not requested".to_string());
        assert!(matches!(
            machine.validate(),
            Err(TopologyValidationError::InvalidCapabilityDeclaration { .. })
        ));

        let mut machine = migrated.environments[0].machines[0].clone();
        machine.target.os = OperatingSystem::Macos;
        assert!(matches!(
            machine.validate(),
            Err(TopologyValidationError::InvalidCapabilityDeclaration { .. })
        ));
    }

    #[test]
    fn project_state_rejects_definition_drift_and_missing_workspace_slot() {
        let mut state = migrate_legacy_developer_sandbox(&legacy_sandbox("/shop")).unwrap();
        state.environments[0].definition_digest = "sha256:wrong".to_string();
        assert!(matches!(
            state.validate(),
            Err(TopologyValidationError::DefinitionDigestMismatch { .. })
        ));

        let mut state = migrate_legacy_developer_sandbox(&legacy_sandbox("/shop")).unwrap();
        state.environments[0].bindings.clear();
        assert!(matches!(
            state.validate(),
            Err(TopologyValidationError::DefinitionTopologyMismatch { .. })
        ));

        let mut state = migrate_legacy_developer_sandbox(&legacy_sandbox("/shop")).unwrap();
        state.environments[0].machines[0].target.image = "different:image".to_string();
        assert!(matches!(
            state.validate(),
            Err(TopologyValidationError::DefinitionTopologyMismatch { .. })
        ));
    }

    #[test]
    fn instance_names_follow_the_same_contract_as_persistence() {
        let mut state = migrate_legacy_developer_sandbox(&legacy_sandbox("/shop")).unwrap();
        let environment_id = state.environments[0].environment_id.clone();
        state.environments[0].networks.push(NetworkInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            network_id: NetworkId::new("net_empty").unwrap(),
            environment_id,
            name: String::new(),
        });
        assert!(matches!(
            state.environments[0].validate(),
            Err(TopologyValidationError::InvalidName { .. })
        ));
    }

    #[test]
    fn one_workspace_can_bind_multiple_named_environment_instances() {
        let mut state = migrate_legacy_developer_sandbox(&legacy_sandbox("/shop")).unwrap();
        let mut second = state.environments[0].clone();
        second.environment_id = EnvironmentId::new("env_parallel").unwrap();
        second.name = "parallel".to_string();
        second.bindings[0].binding_id = WorkspaceBindingId::new("wsp_parallel").unwrap();
        second.bindings[0].environment_id = second.environment_id.clone();
        second.machines[0].machine_id = MachineId::new("mac_parallel").unwrap();
        second.machines[0].environment_id = second.environment_id.clone();
        second.ownership[0].environment_id = second.environment_id.clone();
        second.ownership[0].machine_id = Some(second.machines[0].machine_id.clone());
        second.legacy_migration = None;
        state.environments.push(second);

        state.validate().unwrap();
        assert_eq!(state.environments.len(), 2);
        assert_eq!(
            state.environments[0].bindings[0].workspace_key,
            state.environments[1].bindings[0].workspace_key
        );
    }

    fn unbound_project_state(names: &[&str]) -> ProjectState {
        let definition = project_definition();
        let environments = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                definition
                    .instantiate_environment(*name, 1_000 + index as u64)
                    .unwrap()
            })
            .collect();
        ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments,
        }
    }

    fn bind(environment: &mut EnvironmentInstance, workspace_key: &str) {
        environment.bindings.push(WorkspaceBinding {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            binding_id: WorkspaceBindingId::generate(),
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            name: "source".to_string(),
            workspace_key: workspace_key.to_string(),
            path_hint: Some("/diagnostic/only".to_string()),
        });
    }

    #[test]
    fn generated_topology_ids_are_fresh_valid_and_type_prefixed() {
        macro_rules! assert_generated {
            ($type:ty, $prefix:literal) => {{
                let first = <$type>::generate();
                let second = <$type>::generate();
                assert!(first.as_str().starts_with($prefix));
                assert_ne!(first, second);
                first.validate().unwrap();
                second.validate().unwrap();
            }};
        }

        assert_generated!(ProjectId, "prj_");
        assert_generated!(EnvironmentId, "env_");
        assert_generated!(MachineId, "mch_");
        assert_generated!(MachineIncarnationId, "inc_");
        assert_generated!(WorkspaceBindingId, "wsp_");
        assert_generated!(NetworkId, "net_");
        assert_generated!(EndpointId, "end_");
    }

    #[test]
    fn instantiate_environment_creates_fresh_unbound_topology() {
        let definition = project_definition();
        let first = definition.instantiate_environment("agent", 42).unwrap();
        let second = definition.instantiate_environment("agent-2", 42).unwrap();

        assert_ne!(first.environment_id, second.environment_id);
        assert!(first.bindings.is_empty());
        assert_eq!(
            first.ownership.len(),
            first.machines.len() + first.networks.len() + first.endpoints.len()
        );
        assert_eq!(first.state, EnvironmentState::Creating);
        assert_eq!(first.created_at, 42);
        assert_eq!(first.updated_at, 42);
        assert!(first.machines.iter().all(|machine| {
            machine.state == MachineState::Creating
                && machine.backend.is_none()
                && machine.incarnation.is_none()
                && machine.negotiated_capabilities == CapabilitySet::default()
        }));
        let endpoint = &first.endpoints[0];
        assert_eq!(
            first
                .machines
                .iter()
                .find(|machine| machine.machine_id == endpoint.machine_id)
                .unwrap()
                .name,
            "api"
        );
        first.validate().unwrap();
        ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments: vec![first],
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn creating_allows_pending_negotiation_and_binding_but_ready_is_strict() {
        let definition = project_definition();
        let mut environment = definition.instantiate_environment("agent", 42).unwrap();
        environment.validate().unwrap();

        environment.state = EnvironmentState::Ready;
        for machine in &mut environment.machines {
            machine.state = MachineState::Ready;
        }
        assert!(matches!(
            environment.validate(),
            Err(TopologyValidationError::MissingCapability { .. })
        ));

        for machine in &mut environment.machines {
            machine.negotiated_capabilities = machine.requested_capabilities.clone();
        }
        environment.validate().unwrap();
        assert!(matches!(
            ProjectState {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                definition,
                environments: vec![environment],
            }
            .validate(),
            Err(TopologyValidationError::DefinitionTopologyMismatch { .. })
        ));
    }

    #[test]
    fn partial_creation_failure_persists_but_post_creation_states_remain_strict() {
        let definition = project_definition();
        let creating = definition.instantiate_environment("agent", 42).unwrap();

        let mut failed = creating.clone();
        failed.state = EnvironmentState::Failed;
        for machine in &mut failed.machines {
            machine.state = MachineState::Failed;
        }
        ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition: definition.clone(),
            environments: vec![failed],
        }
        .validate()
        .expect("a partial Creating -> Failed snapshot must remain persistable");

        let mut stopped = creating.clone();
        stopped.state = EnvironmentState::Stopped;
        for machine in &mut stopped.machines {
            machine.state = MachineState::Stopped;
        }
        assert!(matches!(
            stopped.validate(),
            Err(TopologyValidationError::MissingCapability { .. })
        ));

        for machine in &mut stopped.machines {
            machine.negotiated_capabilities = machine.requested_capabilities.clone();
        }
        for state in [EnvironmentState::Stopped, EnvironmentState::Deleting] {
            let mut environment = stopped.clone();
            environment.state = state;
            assert!(matches!(
                ProjectState {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    definition: definition.clone(),
                    environments: vec![environment],
                }
                .validate(),
                Err(TopologyValidationError::DefinitionTopologyMismatch { .. })
            ));
        }
    }

    #[test]
    fn environment_selection_uses_strict_explicit_process_workspace_precedence() {
        let mut state = unbound_project_state(&["explicit", "process", "workspace"]);
        bind(&mut state.environments[2], "worktree-token");
        let context = EnvironmentSelectionContext {
            explicit: Some(EnvironmentSelector::Name("explicit".to_string())),
            process_environment_id: Some(state.environments[1].environment_id.clone()),
            workspace_key: Some("worktree-token".to_string()),
        };

        let selection = state.resolve_environment(&context).unwrap();
        assert_eq!(selection.name, "explicit");
        assert_eq!(selection.source, EnvironmentSelectionSource::Explicit);

        let selection = state
            .resolve_environment(&EnvironmentSelectionContext {
                explicit: None,
                ..context.clone()
            })
            .unwrap();
        assert_eq!(selection.name, "process");
        assert_eq!(selection.source, EnvironmentSelectionSource::Process);

        let selection = state
            .resolve_environment(&EnvironmentSelectionContext {
                explicit: None,
                process_environment_id: None,
                ..context
            })
            .unwrap();
        assert_eq!(selection.name, "workspace");
        assert_eq!(selection.source, EnvironmentSelectionSource::Workspace);
    }

    #[test]
    fn stale_explicit_and_process_selectors_never_fall_through() {
        let mut state = unbound_project_state(&["bound"]);
        bind(&mut state.environments[0], "worktree-token");

        for context in [
            EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::Id(
                    EnvironmentId::new("env_missing").unwrap(),
                )),
                process_environment_id: None,
                workspace_key: Some("worktree-token".to_string()),
            },
            EnvironmentSelectionContext {
                explicit: None,
                process_environment_id: Some(EnvironmentId::new("env_missing").unwrap()),
                workspace_key: Some("worktree-token".to_string()),
            },
        ] {
            assert!(matches!(
                state.resolve_environment(&context),
                Err(TopologyResolutionError::NotFound { .. })
            ));
            assert!(matches!(
                state.resolve_environment_for_up(&context),
                Err(TopologyResolutionError::NotFound { .. })
            ));
        }
    }

    #[test]
    fn malformed_present_selectors_fail_validation_without_fallthrough() {
        let mut state = unbound_project_state(&["bound"]);
        bind(&mut state.environments[0], "worktree-token");
        let invalid_id: EnvironmentId = serde_json::from_str("\"invalid/id\"").unwrap();

        for context in [
            EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::Id(invalid_id.clone())),
                process_environment_id: None,
                workspace_key: Some("worktree-token".to_string()),
            },
            EnvironmentSelectionContext {
                explicit: None,
                process_environment_id: Some(invalid_id),
                workspace_key: Some("worktree-token".to_string()),
            },
            EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::Name("   ".to_string())),
                process_environment_id: None,
                workspace_key: Some("worktree-token".to_string()),
            },
            EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::NameOrId("".to_string())),
                process_environment_id: None,
                workspace_key: Some("worktree-token".to_string()),
            },
        ] {
            assert!(matches!(
                state.resolve_environment(&context),
                Err(TopologyResolutionError::InvalidSelector { .. })
            ));
            assert!(matches!(
                state.resolve_environment_for_up(&context),
                Err(TopologyResolutionError::InvalidSelector { .. })
            ));
        }
    }

    #[test]
    fn missing_generated_id_in_name_or_id_never_creates() {
        let state = unbound_project_state(&[]);
        let missing = EnvironmentId::generate().to_string();
        let context = EnvironmentSelectionContext {
            explicit: Some(EnvironmentSelector::NameOrId(missing.clone())),
            ..EnvironmentSelectionContext::default()
        };

        assert!(matches!(
            state.resolve_environment(&context),
            Err(TopologyResolutionError::NotFound { selector, .. }) if selector == missing
        ));
        assert!(matches!(
            state.resolve_environment_for_up(&context),
            Err(TopologyResolutionError::NotFound { selector, .. }) if selector == missing
        ));

        let uppercase_lookalike = format!("env_{}", "A".repeat(32));
        assert_eq!(
            state
                .resolve_environment_for_up(&EnvironmentSelectionContext {
                    explicit: Some(EnvironmentSelector::NameOrId(uppercase_lookalike.clone())),
                    ..EnvironmentSelectionContext::default()
                })
                .unwrap(),
            EnvironmentUpDecision::Create {
                name: uppercase_lookalike
            }
        );
    }

    #[test]
    fn name_or_id_collision_is_ambiguous_instead_of_guessing() {
        let mut state = unbound_project_state(&["first", "second"]);
        let colliding = state.environments[0].environment_id.to_string();
        state.environments[1].name = colliding.clone();
        let error = state
            .resolve_environment(&EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::NameOrId(colliding)),
                ..EnvironmentSelectionContext::default()
            })
            .unwrap_err();
        assert!(matches!(
            error,
            TopologyResolutionError::Ambiguous { candidates, .. } if candidates.len() == 2
        ));
    }

    #[test]
    fn selection_contract_round_trips_as_structured_json() {
        let context = EnvironmentSelectionContext {
            explicit: Some(EnvironmentSelector::NameOrId("agent".to_string())),
            process_environment_id: Some(EnvironmentId::new("env_process").unwrap()),
            workspace_key: Some("opaque-worktree-token".to_string()),
        };
        let decoded: EnvironmentSelectionContext =
            serde_json::from_value(serde_json::to_value(&context).unwrap()).unwrap();
        assert_eq!(decoded, context);

        let error = TopologyResolutionError::selection_required(
            "environment",
            "workspace binding",
            [TopologyCandidate {
                id: "env_agent".to_string(),
                name: "agent".to_string(),
            }],
        );
        let decoded: TopologyResolutionError =
            serde_json::from_value(serde_json::to_value(&error).unwrap()).unwrap();
        assert_eq!(decoded, error);
    }

    #[test]
    fn workspace_ambiguity_is_sorted_bounded_and_path_independent() {
        let names: Vec<_> = (0..(MAX_TOPOLOGY_SELECTION_CANDIDATES + 5))
            .map(|index| format!("agent-{index:02}"))
            .collect();
        let refs: Vec<_> = names.iter().map(String::as_str).collect();
        let mut state = unbound_project_state(&refs);
        for (index, environment) in state.environments.iter_mut().enumerate() {
            bind(environment, "shared-token");
            environment.bindings[0].path_hint = Some(format!("/different/path/{index}"));
        }
        let error = state
            .resolve_environment(&EnvironmentSelectionContext {
                workspace_key: Some("shared-token".to_string()),
                ..EnvironmentSelectionContext::default()
            })
            .unwrap_err();
        let TopologyResolutionError::Ambiguous { candidates, .. } = error else {
            panic!("expected ambiguity");
        };
        assert_eq!(candidates.len(), MAX_TOPOLOGY_SELECTION_CANDIDATES);
        assert!(candidates.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn up_creation_rules_are_fail_closed() {
        let empty = unbound_project_state(&[]);
        assert_eq!(
            empty
                .resolve_environment_for_up(&EnvironmentSelectionContext::default())
                .unwrap(),
            EnvironmentUpDecision::Create {
                name: "default".to_string()
            }
        );

        let existing = unbound_project_state(&["agent"]);
        let no_selector = existing
            .resolve_environment_for_up(&EnvironmentSelectionContext::default())
            .unwrap_err();
        assert!(matches!(
            no_selector,
            TopologyResolutionError::SelectionRequired { candidates, .. }
                if candidates.len() == 1
        ));

        assert_eq!(
            existing
                .resolve_environment_for_up(&EnvironmentSelectionContext {
                    explicit: Some(EnvironmentSelector::Name("new-agent".to_string())),
                    ..EnvironmentSelectionContext::default()
                })
                .unwrap(),
            EnvironmentUpDecision::Create {
                name: "new-agent".to_string()
            }
        );
        assert_eq!(
            existing
                .resolve_environment_for_up(&EnvironmentSelectionContext {
                    explicit: Some(EnvironmentSelector::NameOrId("new-agent-2".to_string())),
                    ..EnvironmentSelectionContext::default()
                })
                .unwrap(),
            EnvironmentUpDecision::Create {
                name: "new-agent-2".to_string()
            }
        );
        assert!(matches!(
            existing.resolve_environment_for_up(&EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::Id(
                    EnvironmentId::new("env_missing").unwrap()
                )),
                ..EnvironmentSelectionContext::default()
            }),
            Err(TopologyResolutionError::NotFound { .. })
        ));
        assert!(matches!(
            existing.resolve_environment_for_up(&EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::Name("   ".to_string())),
                ..EnvironmentSelectionContext::default()
            }),
            Err(TopologyResolutionError::InvalidSelector { .. })
        ));
        assert!(matches!(
            empty.resolve_environment_for_up(&EnvironmentSelectionContext {
                workspace_key: Some(String::new()),
                ..EnvironmentSelectionContext::default()
            }),
            Err(TopologyResolutionError::InvalidSelector { .. })
        ));
    }

    #[test]
    fn resource_names_are_bounded_deterministic_and_owner_scoped() {
        let owner = ResourceOwner {
            project_id: ProjectId::new("prj_shop").unwrap(),
            environment_id: EnvironmentId::new("env_agent").unwrap(),
            machine_id: Some(MachineId::new("mch_linux").unwrap()),
        };
        let first = owner
            .bounded_resource_name(&OwnedResourceKind::Socket, "docker/socket", 64)
            .unwrap();
        assert_eq!(
            first,
            owner
                .bounded_resource_name(&OwnedResourceKind::Socket, "docker/socket", 64)
                .unwrap()
        );
        assert!(first.is_ascii());
        assert!(first.len() <= 64);
        assert_ne!(
            first,
            ResourceOwner {
                environment_id: EnvironmentId::new("env_sibling").unwrap(),
                ..owner.clone()
            }
            .bounded_resource_name(&OwnedResourceKind::Socket, "docker/socket", 64)
            .unwrap()
        );
        assert_ne!(
            first,
            owner
                .bounded_resource_name(&OwnedResourceKind::Disk, "docker/socket", 64)
                .unwrap()
        );
        assert_ne!(
            first,
            owner
                .bounded_resource_name(&OwnedResourceKind::Socket, "docker/other", 64)
                .unwrap()
        );
        assert_ne!(
            first,
            ResourceOwner {
                project_id: ProjectId::new("prj_other").unwrap(),
                ..owner.clone()
            }
            .bounded_resource_name(&OwnedResourceKind::Socket, "docker/socket", 64)
            .unwrap()
        );
        assert_ne!(
            first,
            ResourceOwner {
                machine_id: None,
                ..owner.clone()
            }
            .bounded_resource_name(&OwnedResourceKind::Socket, "docker/socket", 64)
            .unwrap()
        );
        let minimum = RESOURCE_NAME_VERSION_PREFIX.len() + 1 + RESOURCE_NAME_DIGEST_HEX_LENGTH;
        assert_eq!(
            owner
                .bounded_resource_name(&OwnedResourceKind::Socket, &"x".repeat(1_000), minimum)
                .unwrap()
                .len(),
            minimum
        );
        assert!(
            owner
                .bounded_resource_name(&OwnedResourceKind::Socket, "socket", minimum - 1)
                .is_err()
        );
    }

    #[test]
    fn duplicate_ownership_keys_are_rejected() {
        let definition = project_definition();
        let mut environment = definition.instantiate_environment("agent", 42).unwrap();
        let record = OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::Network,
            resource_id: "shared-logical-id".to_string(),
            environment_id: environment.environment_id.clone(),
            machine_id: None,
        };
        environment.ownership = vec![record.clone(), record];
        assert!(matches!(
            environment.validate(),
            Err(TopologyValidationError::Duplicate { kind, .. })
                if kind == "ownership_resource"
        ));
    }
}
