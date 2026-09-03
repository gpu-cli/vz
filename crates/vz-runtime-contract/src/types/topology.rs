use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{
    SANDBOX_LABEL_PROJECT_DIR, SANDBOX_LABEL_SPACE_MODE, SANDBOX_SPACE_MODE_REQUIRED, Sandbox,
    SandboxBackend, SandboxState,
};

/// Current schema version for Developer Environment topology records.
pub const TOPOLOGY_SCHEMA_VERSION: u32 = 1;

const MAX_ID_LENGTH: usize = 128;
const MAX_NAME_LENGTH: usize = 128;
const LEGACY_DEVELOPER_MARKER: &str = "vz.run.workspace";

macro_rules! topology_id {
    ($name:ident, $label:literal) => {
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

topology_id!(ProjectId, "project_id");
topology_id!(EnvironmentId, "environment_id");
topology_id!(MachineId, "machine_id");
topology_id!(MachineIncarnationId, "machine_incarnation_id");
topology_id!(WorkspaceBindingId, "workspace_binding_id");
topology_id!(NetworkId, "network_id");
topology_id!(EndpointId, "endpoint_id");

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

/// A path-independent workspace association. `path_hint` is a relocatable selector only.
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

/// A structured resolution candidate; callers render these without parsing prose.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopologyCandidate {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Error)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum TopologyResolutionError {
    #[error("no {kind} matched selector `{selector}`")]
    NotFound { kind: String, selector: String },
    #[error("selector `{selector}` matched multiple {kind} candidates")]
    Ambiguous {
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
        let mut candidates: Vec<_> = candidates.into_iter().collect();
        candidates.sort();
        Self::Ambiguous {
            kind: kind.into(),
            selector: selector.into(),
            candidates,
        }
    }
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
        reject_non_linux_docker(&self.name, &self.target, &self.negotiated_capabilities)?;
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
        if let Some(capability) = self
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
        if self.target.os == OperatingSystem::Linux {
            for capability in [
                MachineCapability::DockerEngine,
                MachineCapability::Compose,
                MachineCapability::Buildx,
            ] {
                if !self.negotiated_capabilities.contains(capability) {
                    return Err(TopologyValidationError::MissingCapability {
                        machine_id: self.machine_id.to_string(),
                        capability,
                    });
                }
            }
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
        if actual.requested_capabilities != desired.requested_capabilities {
            return definition_topology_mismatch(
                &environment_id,
                format!("Machine `{}` requested capabilities differ", desired.name),
            );
        }
        if let Some(workspace) = &desired.workspace
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

fn reject_non_linux_docker(
    machine: &str,
    target: &TargetSpec,
    capabilities: &CapabilitySet,
) -> Result<(), TopologyValidationError> {
    if target.os == OperatingSystem::Linux {
        return Ok(());
    }
    if let Some(capability) = [
        MachineCapability::DockerEngine,
        MachineCapability::Compose,
        MachineCapability::Buildx,
    ]
    .into_iter()
    .find(|capability| capabilities.contains(*capability))
    {
        return Err(TopologyValidationError::InvalidCapabilityDeclaration {
            machine_id: machine.to_string(),
            reason: format!("non-Linux target cannot provide implicit capability `{capability:?}`"),
        });
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
    let backend = match &sandbox.backend {
        SandboxBackend::MacosVz => MachineBackend::MacosVirtualizationLinux,
        SandboxBackend::LinuxFirecracker => MachineBackend::LinuxNative,
        SandboxBackend::Other(value) => MachineBackend::Other(value.clone()),
    };
    let (environment_state, machine_state) = legacy_state(sandbox.state);
    let machine_spec = MachineSpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        name: "linux".to_string(),
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
            Err(TopologyValidationError::UnsupportedTarget { .. })
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
    fn hardened_and_ambiguous_legacy_records_are_never_adopted() {
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
}
