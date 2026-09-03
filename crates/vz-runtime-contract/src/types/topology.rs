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
topology_id!(LifecycleOperationId, "lifecycle_operation_id", "lop_");

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
    Degraded,
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

/// Aggregate lifecycle mutation represented by a durable operation journal.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentLifecycleKind {
    Up,
    Stop,
    Delete,
}

/// Durable status of one aggregate lifecycle operation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentLifecycleStatus {
    Planned,
    Running,
    Blocked,
    Succeeded,
    Failed,
    Superseded,
}

/// Durable status shared by Machine and ownership-cleanup steps.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleStepStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
}

/// Exact acknowledgement accepted for a lifecycle step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum LifecycleStepResult {
    Succeeded,
    Failed { reason: String },
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

/// One deterministic Machine action within an aggregate lifecycle operation.
///
/// The Machine record is deliberately passive: the operation and its generation
/// are the transition authority. `target_state` is absent for delete because the
/// Machine row is removed only after its exact ownership graph is clean.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MachineLifecycleStep {
    pub machine_id: MachineId,
    pub initial_state: MachineState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_state: Option<MachineState>,
    /// Exact runtime incarnation observed when the operation was planned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_incarnation: Option<MachineIncarnation>,
    /// Durable incarnation established by a successful Up acknowledgement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resulting_incarnation: Option<MachineIncarnation>,
    pub status: LifecycleStepStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

/// Exact caller acknowledgement for one Machine lifecycle step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MachineLifecycleStepAcknowledgement {
    pub operation_id: LifecycleOperationId,
    pub generation: u64,
    pub machine_id: MachineId,
    pub initial_state: MachineState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_state: Option<MachineState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_incarnation: Option<MachineIncarnation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resulting_incarnation: Option<MachineIncarnation>,
    pub result: LifecycleStepResult,
}

/// One exact-owner cleanup action in a canonical delete plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnershipCleanupStep {
    pub ownership: OwnershipRecord,
    pub status: LifecycleStepStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

/// Exact caller acknowledgement for an owned-resource cleanup step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnershipCleanupStepAcknowledgement {
    pub operation_id: LifecycleOperationId,
    pub generation: u64,
    pub ownership: OwnershipRecord,
    pub result: LifecycleStepResult,
}

/// Generation-fenced durable operation for one Environment aggregate.
///
/// Physical reconciliation is intentionally outside this contract. Backends
/// consume the deterministic Machine and cleanup steps, then acknowledge the
/// exact step against this operation ID and generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentLifecycleOperation {
    pub schema_version: u32,
    pub operation_id: LifecycleOperationId,
    pub project_id: ProjectId,
    pub environment_id: EnvironmentId,
    pub kind: EnvironmentLifecycleKind,
    pub generation: u64,
    pub request_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub definition_digest: String,
    pub initial_state: EnvironmentState,
    pub requested_target: EnvironmentState,
    pub status: EnvironmentLifecycleStatus,
    pub machine_steps: Vec<MachineLifecycleStep>,
    #[serde(default)]
    pub cleanup_steps: Vec<OwnershipCleanupStep>,
    pub created_at: u64,
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u64>,
}

/// Durable proof that an Environment's exact ownership graph was deleted.
///
/// Tombstones live outside the active aggregate, allowing the same human name
/// to be instantiated later with a fresh immutable Environment ID.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentTombstone {
    pub schema_version: u32,
    pub project_id: ProjectId,
    pub environment_id: EnvironmentId,
    pub name: String,
    pub definition_digest: String,
    pub delete_operation_id: LifecycleOperationId,
    pub lifecycle_generation: u64,
    pub ownership_digest: String,
    pub deleted_at: u64,
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
    /// Monotonic fencing generation for aggregate lifecycle mutations.
    #[serde(default)]
    pub lifecycle_generation: u64,
    /// The only operation permitted to acknowledge lifecycle work at this generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_operation_id: Option<LifecycleOperationId>,
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

/// Structured lifecycle failures suitable for stable API and transport details.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Error)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum TopologyLifecycleError {
    #[error("Environment `{environment_id}` cannot begin {operation:?} from state {state:?}")]
    InvalidTransition {
        environment_id: String,
        operation: EnvironmentLifecycleKind,
        state: EnvironmentState,
    },
    #[error(
        "Environment `{environment_id}` already has lifecycle operation `{active_operation_id}`"
    )]
    OperationConflict {
        environment_id: String,
        active_operation_id: String,
    },
    #[error(
        "lifecycle operation `{operation_id}` generation mismatch: expected {expected}, found {found}"
    )]
    GenerationMismatch {
        operation_id: String,
        expected: u64,
        found: u64,
    },
    #[error(
        "Environment `{environment_id}` is fenced by lifecycle operation `{expected}`, not `{found}`"
    )]
    OperationMismatch {
        environment_id: String,
        expected: String,
        found: String,
    },
    #[error("Machine `{machine_id}` is not part of lifecycle operation `{operation_id}`")]
    MachineStepNotFound {
        operation_id: String,
        machine_id: String,
    },
    #[error("Machine step acknowledgement does not match the planned step for `{machine_id}`")]
    MachineStepMismatch { machine_id: String },
    #[error(
        "owned resource `{resource_kind}:{resource_id}` is not an exact cleanup step in operation `{operation_id}`"
    )]
    OwnershipStepMismatch {
        operation_id: String,
        resource_kind: String,
        resource_id: String,
    },
    #[error("lifecycle operation `{operation_id}` still has incomplete steps")]
    OperationIncomplete { operation_id: String },
    #[error("lifecycle operation `{operation_id}` has failed steps")]
    OperationFailed { operation_id: String },
    #[error("lifecycle operation `{operation_id}` is not a delete operation")]
    DeleteRequired { operation_id: String },
    #[error("Environment `{environment_id}` is deleted and cannot remain in the live aggregate")]
    DeletedEnvironmentIsNotLive { environment_id: String },
    #[error("lifecycle operation is invalid: {reason}")]
    InvalidOperation { reason: String },
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
    #[error("invalid lifecycle state for Environment `{environment_id}`: {reason}")]
    InvalidLifecycleState {
        environment_id: String,
        reason: String,
    },
    #[error("invalid incarnation for Machine `{machine_id}`: {reason}")]
    InvalidMachineIncarnation { machine_id: String, reason: String },
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
            lifecycle_generation: 0,
            active_operation_id: None,
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
        match self.state {
            EnvironmentState::Deleted => {
                return Err(TopologyValidationError::InvalidLifecycleState {
                    environment_id: self.environment_id.to_string(),
                    reason:
                        "Deleted is represented by an EnvironmentTombstone, not a live aggregate"
                            .to_string(),
                });
            }
            EnvironmentState::Reconciling | EnvironmentState::Deleting => {
                if self.lifecycle_generation == 0 || self.active_operation_id.is_none() {
                    return Err(TopologyValidationError::InvalidLifecycleState {
                        environment_id: self.environment_id.to_string(),
                        reason: format!(
                            "state {:?} requires a positive lifecycle generation and active operation",
                            self.state
                        ),
                    });
                }
            }
            EnvironmentState::Creating
            | EnvironmentState::Ready
            | EnvironmentState::Degraded
            | EnvironmentState::Stopped
            | EnvironmentState::Failed => {
                if self.active_operation_id.is_some() {
                    return Err(TopologyValidationError::InvalidLifecycleState {
                        environment_id: self.environment_id.to_string(),
                        reason: format!(
                            "stable state {:?} cannot retain an active lifecycle operation",
                            self.state
                        ),
                    });
                }
            }
        }
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
        match self.state {
            EnvironmentState::Ready
                if self
                    .machines
                    .iter()
                    .any(|machine| machine.state != MachineState::Ready) =>
            {
                return Err(TopologyValidationError::InvalidLifecycleState {
                    environment_id: self.environment_id.to_string(),
                    reason: "Ready requires every Machine to be Ready".to_string(),
                });
            }
            EnvironmentState::Stopped
                if self
                    .machines
                    .iter()
                    .any(|machine| machine.state != MachineState::Stopped) =>
            {
                return Err(TopologyValidationError::InvalidLifecycleState {
                    environment_id: self.environment_id.to_string(),
                    reason: "Stopped requires every Machine to be Stopped".to_string(),
                });
            }
            EnvironmentState::Degraded
                if !self
                    .machines
                    .iter()
                    .any(|machine| machine.state == MachineState::Ready)
                    || !self
                        .machines
                        .iter()
                        .any(|machine| machine.state == MachineState::Failed) =>
            {
                return Err(TopologyValidationError::InvalidLifecycleState {
                    environment_id: self.environment_id.to_string(),
                    reason: "Degraded requires at least one Ready and one Failed Machine"
                        .to_string(),
                });
            }
            _ => {}
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
            validate_identifier("ownership_resource_id", &record.resource_id)?;
            if let OwnedResourceKind::Other(kind) = &record.resource_kind {
                validate_identifier("ownership_resource_kind", kind)?;
            }
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
        validate_exact_topology_ownership(self)?;
        Ok(())
    }
}

impl EnvironmentLifecycleOperation {
    /// Build a deterministic operation plan without mutating the Environment.
    pub fn plan(
        environment: &EnvironmentInstance,
        operation_id: LifecycleOperationId,
        kind: EnvironmentLifecycleKind,
        request_id: impl Into<String>,
        idempotency_key: impl Into<String>,
        request_hash: impl Into<String>,
        now: u64,
    ) -> Result<Self, TopologyLifecycleError> {
        environment
            .validate()
            .map_err(|error| TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            })?;
        ensure_lifecycle_transition_allowed(environment, kind)?;
        let generation = environment
            .lifecycle_generation
            .checked_add(1)
            .ok_or_else(|| TopologyLifecycleError::InvalidOperation {
                reason: "lifecycle generation overflow".to_string(),
            })?;
        let requested_target = match kind {
            EnvironmentLifecycleKind::Up => EnvironmentState::Ready,
            EnvironmentLifecycleKind::Stop => EnvironmentState::Stopped,
            EnvironmentLifecycleKind::Delete => EnvironmentState::Deleted,
        };
        let target_state = match kind {
            EnvironmentLifecycleKind::Up => Some(MachineState::Ready),
            EnvironmentLifecycleKind::Stop => Some(MachineState::Stopped),
            EnvironmentLifecycleKind::Delete => None,
        };
        let mut machine_steps = environment
            .machines
            .iter()
            .map(|machine| MachineLifecycleStep {
                machine_id: machine.machine_id.clone(),
                initial_state: machine.state,
                target_state,
                expected_incarnation: machine.incarnation.clone(),
                resulting_incarnation: None,
                status: LifecycleStepStatus::Pending,
                failure_reason: None,
            })
            .collect::<Vec<_>>();
        machine_steps.sort_by(|left, right| left.machine_id.cmp(&right.machine_id));

        let mut cleanup_steps = if kind == EnvironmentLifecycleKind::Delete {
            environment
                .ownership
                .iter()
                .cloned()
                .map(|ownership| OwnershipCleanupStep {
                    ownership,
                    status: LifecycleStepStatus::Pending,
                    failure_reason: None,
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        cleanup_steps.sort_by(|left, right| {
            ownership_sort_key(&left.ownership).cmp(&ownership_sort_key(&right.ownership))
        });

        let operation = Self {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            operation_id,
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            kind,
            generation,
            request_id: request_id.into(),
            idempotency_key: idempotency_key.into(),
            request_hash: request_hash.into(),
            definition_digest: environment.definition_digest.clone(),
            initial_state: environment.state,
            requested_target,
            status: EnvironmentLifecycleStatus::Planned,
            machine_steps,
            cleanup_steps,
            created_at: now,
            updated_at: now,
            completed_at: None,
        };
        operation.validate_structure()?;
        operation.validate_against_environment(environment)?;
        Ok(operation)
    }

    /// Validate all operation-local invariants independent of a store.
    pub fn validate_structure(&self) -> Result<(), TopologyLifecycleError> {
        validate_schema(self.schema_version).map_err(|error| {
            TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            }
        })?;
        self.operation_id
            .validate()
            .map_err(|error| TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            })?;
        self.project_id
            .validate()
            .map_err(|error| TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            })?;
        self.environment_id.validate().map_err(|error| {
            TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            }
        })?;
        for (field, value) in [
            ("request_id", self.request_id.as_str()),
            ("idempotency_key", self.idempotency_key.as_str()),
            ("request_hash", self.request_hash.as_str()),
            ("definition_digest", self.definition_digest.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(TopologyLifecycleError::InvalidOperation {
                    reason: format!("{field} must not be empty"),
                });
            }
        }
        if self.generation == 0 {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: "generation must be greater than zero".to_string(),
            });
        }
        if self.updated_at < self.created_at
            || self
                .completed_at
                .is_some_and(|completed_at| completed_at < self.updated_at)
        {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: "operation timestamps are not monotonic".to_string(),
            });
        }
        let terminal = matches!(
            self.status,
            EnvironmentLifecycleStatus::Succeeded
                | EnvironmentLifecycleStatus::Failed
                | EnvironmentLifecycleStatus::Superseded
        );
        if terminal != self.completed_at.is_some() {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: format!(
                    "operation status {:?} and completed_at disagree",
                    self.status
                ),
            });
        }
        let expected_target = match self.kind {
            EnvironmentLifecycleKind::Up => EnvironmentState::Ready,
            EnvironmentLifecycleKind::Stop => EnvironmentState::Stopped,
            EnvironmentLifecycleKind::Delete => EnvironmentState::Deleted,
        };
        if self.requested_target != expected_target {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: format!(
                    "{:?} must request target {:?}, found {:?}",
                    self.kind, expected_target, self.requested_target
                ),
            });
        }
        if !lifecycle_transition_allowed_from_state(self.initial_state, self.kind) {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: format!(
                    "{:?} cannot have initial Environment state {:?}",
                    self.kind, self.initial_state
                ),
            });
        }

        let expected_machine_target = match self.kind {
            EnvironmentLifecycleKind::Up => Some(MachineState::Ready),
            EnvironmentLifecycleKind::Stop => Some(MachineState::Stopped),
            EnvironmentLifecycleKind::Delete => None,
        };
        if self.machine_steps.is_empty() {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: "a lifecycle operation requires at least one Machine step".to_string(),
            });
        }
        let mut prior_machine_id: Option<&MachineId> = None;
        for step in &self.machine_steps {
            step.machine_id.validate().map_err(|error| {
                TopologyLifecycleError::InvalidOperation {
                    reason: error.to_string(),
                }
            })?;
            if prior_machine_id.is_some_and(|prior| prior >= &step.machine_id) {
                return Err(TopologyLifecycleError::InvalidOperation {
                    reason: "Machine steps must be strictly ordered by Machine ID".to_string(),
                });
            }
            prior_machine_id = Some(&step.machine_id);
            if step.target_state != expected_machine_target {
                return Err(TopologyLifecycleError::InvalidOperation {
                    reason: format!(
                        "Machine `{}` has a target inconsistent with {:?}",
                        step.machine_id, self.kind
                    ),
                });
            }
            validate_lifecycle_incarnation(&step.machine_id, step.expected_incarnation.as_ref())?;
            validate_lifecycle_incarnation(&step.machine_id, step.resulting_incarnation.as_ref())?;
            match (self.kind, step.status, step.resulting_incarnation.as_ref()) {
                (EnvironmentLifecycleKind::Up, LifecycleStepStatus::Succeeded, Some(resulting)) => {
                    validate_up_incarnation_transition(
                        &step.machine_id,
                        step.expected_incarnation.as_ref(),
                        resulting,
                    )?;
                }
                (EnvironmentLifecycleKind::Up, LifecycleStepStatus::Succeeded, None) => {
                    return Err(TopologyLifecycleError::InvalidOperation {
                        reason: format!(
                            "successful Up step for Machine `{}` requires a resulting incarnation",
                            step.machine_id
                        ),
                    });
                }
                (_, _, None) => {}
                (EnvironmentLifecycleKind::Up, _, Some(_)) => {
                    return Err(TopologyLifecycleError::InvalidOperation {
                        reason: format!(
                            "only a successful Up step may carry a resulting incarnation for Machine `{}`",
                            step.machine_id
                        ),
                    });
                }
                (_, _, Some(_)) => {
                    return Err(TopologyLifecycleError::InvalidOperation {
                        reason: format!(
                            "{:?} step for Machine `{}` cannot carry a resulting incarnation",
                            self.kind, step.machine_id
                        ),
                    });
                }
            }
            validate_step_status(step.status, step.failure_reason.as_deref())?;
        }

        if self.kind != EnvironmentLifecycleKind::Delete && !self.cleanup_steps.is_empty() {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: "only delete operations may contain ownership cleanup steps".to_string(),
            });
        }
        if self.kind == EnvironmentLifecycleKind::Delete && self.cleanup_steps.is_empty() {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: "a delete operation requires an exact ownership cleanup plan".to_string(),
            });
        }
        let mut prior_ownership_key: Option<(String, String, String)> = None;
        for step in &self.cleanup_steps {
            validate_schema(step.ownership.schema_version).map_err(|error| {
                TopologyLifecycleError::InvalidOperation {
                    reason: format!("invalid cleanup ownership schema: {error}"),
                }
            })?;
            validate_identifier("ownership_resource_id", &step.ownership.resource_id).map_err(
                |error| TopologyLifecycleError::InvalidOperation {
                    reason: format!("invalid cleanup ownership resource: {error}"),
                },
            )?;
            if let OwnedResourceKind::Other(kind) = &step.ownership.resource_kind {
                validate_identifier("ownership_resource_kind", kind).map_err(|error| {
                    TopologyLifecycleError::InvalidOperation {
                        reason: format!("invalid cleanup ownership resource kind: {error}"),
                    }
                })?;
            }
            step.ownership.environment_id.validate().map_err(|error| {
                TopologyLifecycleError::InvalidOperation {
                    reason: format!("invalid cleanup ownership Environment: {error}"),
                }
            })?;
            if step.ownership.environment_id != self.environment_id {
                return Err(TopologyLifecycleError::InvalidOperation {
                    reason: format!(
                        "cleanup ownership `{}` belongs to Environment `{}`, not operation Environment `{}`",
                        step.ownership.resource_id,
                        step.ownership.environment_id,
                        self.environment_id
                    ),
                });
            }
            match &step.ownership.machine_id {
                Some(machine_id) => {
                    machine_id.validate().map_err(|error| {
                        TopologyLifecycleError::InvalidOperation {
                            reason: format!("invalid cleanup ownership Machine: {error}"),
                        }
                    })?;
                    if !self
                        .machine_steps
                        .iter()
                        .any(|machine_step| machine_step.machine_id == *machine_id)
                    {
                        return Err(TopologyLifecycleError::InvalidOperation {
                            reason: format!(
                                "cleanup ownership `{}` belongs to Machine `{machine_id}` outside the operation plan",
                                step.ownership.resource_id
                            ),
                        });
                    }
                }
                None if resource_kind_requires_machine(&step.ownership.resource_kind) => {
                    return Err(TopologyLifecycleError::InvalidOperation {
                        reason: format!(
                            "cleanup ownership `{}` of kind `{}` requires a Machine owner",
                            step.ownership.resource_id,
                            resource_kind_identity(&step.ownership.resource_kind)
                        ),
                    });
                }
                None => {}
            }
            let key = ownership_sort_key(&step.ownership);
            if prior_ownership_key
                .as_ref()
                .is_some_and(|prior| prior >= &key)
            {
                return Err(TopologyLifecycleError::InvalidOperation {
                    reason: "cleanup steps must be strictly ordered by exact ownership key"
                        .to_string(),
                });
            }
            prior_ownership_key = Some(key);
            validate_step_status(step.status, step.failure_reason.as_deref())?;
        }
        validate_operation_status_coherence(self)?;
        Ok(())
    }

    /// Validate the operation plan or in-flight journal against its exact aggregate.
    pub fn validate_against_environment(
        &self,
        environment: &EnvironmentInstance,
    ) -> Result<(), TopologyLifecycleError> {
        self.validate_structure()?;
        if self.project_id != environment.project_id
            || self.environment_id != environment.environment_id
            || self.definition_digest != environment.definition_digest
        {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: "operation Project/Environment/definition ownership does not match"
                    .to_string(),
            });
        }
        match &environment.active_operation_id {
            None => {
                let expected =
                    environment
                        .lifecycle_generation
                        .checked_add(1)
                        .ok_or_else(|| TopologyLifecycleError::InvalidOperation {
                            reason: "lifecycle generation overflow".to_string(),
                        })?;
                if self.generation != expected {
                    return Err(TopologyLifecycleError::GenerationMismatch {
                        operation_id: self.operation_id.to_string(),
                        expected,
                        found: self.generation,
                    });
                }
                if environment.state != self.initial_state {
                    return Err(TopologyLifecycleError::InvalidOperation {
                        reason: "planned operation initial state does not match Environment"
                            .to_string(),
                    });
                }
            }
            Some(active_operation_id) => {
                if active_operation_id != &self.operation_id {
                    return Err(TopologyLifecycleError::OperationMismatch {
                        environment_id: environment.environment_id.to_string(),
                        expected: active_operation_id.to_string(),
                        found: self.operation_id.to_string(),
                    });
                }
                if environment.lifecycle_generation != self.generation {
                    return Err(TopologyLifecycleError::GenerationMismatch {
                        operation_id: self.operation_id.to_string(),
                        expected: environment.lifecycle_generation,
                        found: self.generation,
                    });
                }
                let expected_state = if self.kind == EnvironmentLifecycleKind::Delete {
                    EnvironmentState::Deleting
                } else {
                    EnvironmentState::Reconciling
                };
                if environment.state != expected_state
                    || !matches!(
                        self.status,
                        EnvironmentLifecycleStatus::Running | EnvironmentLifecycleStatus::Blocked
                    )
                {
                    return Err(TopologyLifecycleError::InvalidOperation {
                        reason: format!(
                            "attached {:?} operation with status {:?} requires Environment state {:?}",
                            self.kind, self.status, expected_state
                        ),
                    });
                }
            }
        }

        if self.machine_steps.len() != environment.machines.len() {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: "operation Machine set does not match Environment".to_string(),
            });
        }
        for step in &self.machine_steps {
            let machine = environment
                .machines
                .iter()
                .find(|machine| machine.machine_id == step.machine_id)
                .ok_or_else(|| TopologyLifecycleError::MachineStepNotFound {
                    operation_id: self.operation_id.to_string(),
                    machine_id: step.machine_id.to_string(),
                })?;
            let expected_current = match step.status {
                LifecycleStepStatus::Pending | LifecycleStepStatus::Running => step.initial_state,
                LifecycleStepStatus::Succeeded => step.target_state.unwrap_or(machine.state),
                LifecycleStepStatus::Failed if step.target_state.is_some() => MachineState::Failed,
                LifecycleStepStatus::Failed => machine.state,
            };
            let expected_incarnation = if self.kind == EnvironmentLifecycleKind::Up
                && step.status == LifecycleStepStatus::Succeeded
            {
                step.resulting_incarnation.as_ref()
            } else {
                step.expected_incarnation.as_ref()
            };
            if machine.state != expected_current
                || machine.incarnation.as_ref() != expected_incarnation
            {
                return Err(TopologyLifecycleError::MachineStepMismatch {
                    machine_id: machine.machine_id.to_string(),
                });
            }
        }

        let planned_ownership = self
            .cleanup_steps
            .iter()
            .map(|step| step.ownership.clone())
            .collect::<Vec<_>>();
        if self.kind == EnvironmentLifecycleKind::Delete {
            let mut expected = environment.ownership.clone();
            expected.sort_by_key(ownership_sort_key);
            if planned_ownership != expected {
                return Err(TopologyLifecycleError::InvalidOperation {
                    reason: "delete cleanup plan does not exactly match Environment ownership"
                        .to_string(),
                });
            }
        }
        Ok(())
    }

    /// Atomically-enterable in-memory transition used by persistence adapters.
    pub fn begin(
        &mut self,
        environment: &mut EnvironmentInstance,
        now: u64,
    ) -> Result<(), TopologyLifecycleError> {
        if self.status != EnvironmentLifecycleStatus::Planned {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: "only a planned operation may begin".to_string(),
            });
        }
        self.validate_against_environment(environment)?;
        ensure_lifecycle_transition_allowed(environment, self.kind)?;
        environment.lifecycle_generation = self.generation;
        environment.active_operation_id = Some(self.operation_id.clone());
        environment.state = if self.kind == EnvironmentLifecycleKind::Delete {
            EnvironmentState::Deleting
        } else {
            EnvironmentState::Reconciling
        };
        environment.updated_at = environment.updated_at.max(now);
        self.status = EnvironmentLifecycleStatus::Running;
        self.updated_at = self.updated_at.max(now);
        Ok(())
    }

    /// Apply an exact, generation-fenced Machine acknowledgement.
    pub fn apply_machine_step_acknowledgement(
        &mut self,
        environment: &mut EnvironmentInstance,
        acknowledgement: &MachineLifecycleStepAcknowledgement,
        now: u64,
    ) -> Result<(), TopologyLifecycleError> {
        self.ensure_fence(
            environment,
            &acknowledgement.operation_id,
            acknowledgement.generation,
        )?;
        let step = self
            .machine_steps
            .iter_mut()
            .find(|step| step.machine_id == acknowledgement.machine_id)
            .ok_or_else(|| TopologyLifecycleError::MachineStepNotFound {
                operation_id: self.operation_id.to_string(),
                machine_id: acknowledgement.machine_id.to_string(),
            })?;
        if step.initial_state != acknowledgement.initial_state
            || step.target_state != acknowledgement.target_state
            || step.expected_incarnation != acknowledgement.expected_incarnation
        {
            return Err(TopologyLifecycleError::MachineStepMismatch {
                machine_id: acknowledgement.machine_id.to_string(),
            });
        }
        validate_machine_acknowledgement_incarnation(self.kind, step, acknowledgement)?;
        if step_is_exact_terminal_replay(
            step.status,
            step.failure_reason.as_deref(),
            &acknowledgement.result,
        ) {
            let machine = environment
                .machines
                .iter()
                .find(|machine| machine.machine_id == acknowledgement.machine_id)
                .ok_or_else(|| TopologyLifecycleError::MachineStepNotFound {
                    operation_id: self.operation_id.to_string(),
                    machine_id: acknowledgement.machine_id.to_string(),
                })?;
            let expected_state = match step.status {
                LifecycleStepStatus::Succeeded => step.target_state.unwrap_or(machine.state),
                LifecycleStepStatus::Failed if step.target_state.is_some() => MachineState::Failed,
                LifecycleStepStatus::Failed => step.initial_state,
                LifecycleStepStatus::Pending | LifecycleStepStatus::Running => unreachable!(),
            };
            let expected_incarnation = if self.kind == EnvironmentLifecycleKind::Up
                && step.status == LifecycleStepStatus::Succeeded
            {
                step.resulting_incarnation.as_ref()
            } else {
                step.expected_incarnation.as_ref()
            };
            return if step.resulting_incarnation == acknowledgement.resulting_incarnation
                && machine.state == expected_state
                && machine.incarnation.as_ref() == expected_incarnation
            {
                Ok(())
            } else {
                Err(TopologyLifecycleError::MachineStepMismatch {
                    machine_id: acknowledgement.machine_id.to_string(),
                })
            };
        }
        if matches!(step.status, LifecycleStepStatus::Succeeded)
            || (matches!(step.status, LifecycleStepStatus::Failed)
                && self.kind != EnvironmentLifecycleKind::Delete)
        {
            return Err(TopologyLifecycleError::MachineStepMismatch {
                machine_id: acknowledgement.machine_id.to_string(),
            });
        }

        let machine = environment
            .machines
            .iter_mut()
            .find(|machine| machine.machine_id == acknowledgement.machine_id)
            .ok_or_else(|| TopologyLifecycleError::MachineStepNotFound {
                operation_id: self.operation_id.to_string(),
                machine_id: acknowledgement.machine_id.to_string(),
            })?;
        if matches!(
            step.status,
            LifecycleStepStatus::Pending | LifecycleStepStatus::Running
        ) && (machine.state != step.initial_state
            || machine.incarnation != step.expected_incarnation)
        {
            return Err(TopologyLifecycleError::MachineStepMismatch {
                machine_id: acknowledgement.machine_id.to_string(),
            });
        }
        match &acknowledgement.result {
            LifecycleStepResult::Succeeded => {
                if self.kind == EnvironmentLifecycleKind::Up {
                    let Some(resulting) = acknowledgement.resulting_incarnation.as_ref() else {
                        return Err(TopologyLifecycleError::MachineStepMismatch {
                            machine_id: acknowledgement.machine_id.to_string(),
                        });
                    };
                    apply_up_incarnation(
                        machine,
                        &mut environment.ownership,
                        step.expected_incarnation.as_ref(),
                        resulting,
                    )?;
                    step.resulting_incarnation = Some(resulting.clone());
                }
                step.status = LifecycleStepStatus::Succeeded;
                step.failure_reason = None;
                if let Some(target_state) = step.target_state {
                    machine.state = target_state;
                }
            }
            LifecycleStepResult::Failed { reason } => {
                if reason.trim().is_empty() {
                    return Err(TopologyLifecycleError::InvalidOperation {
                        reason: "failed Machine acknowledgement requires a reason".to_string(),
                    });
                }
                step.status = LifecycleStepStatus::Failed;
                step.failure_reason = Some(reason.clone());
                step.resulting_incarnation = None;
                if step.target_state.is_some() {
                    machine.state = MachineState::Failed;
                }
            }
        }
        environment.updated_at = environment.updated_at.max(now);
        self.updated_at = self.updated_at.max(now);
        self.refresh_status(now);
        Ok(())
    }

    /// Apply an exact-owner, generation-fenced delete-cleanup acknowledgement.
    pub fn apply_cleanup_step_acknowledgement(
        &mut self,
        environment: &EnvironmentInstance,
        acknowledgement: &OwnershipCleanupStepAcknowledgement,
        now: u64,
    ) -> Result<(), TopologyLifecycleError> {
        if self.kind != EnvironmentLifecycleKind::Delete {
            return Err(TopologyLifecycleError::DeleteRequired {
                operation_id: self.operation_id.to_string(),
            });
        }
        self.ensure_fence(
            environment,
            &acknowledgement.operation_id,
            acknowledgement.generation,
        )?;
        let step = self
            .cleanup_steps
            .iter_mut()
            .find(|step| step.ownership == acknowledgement.ownership)
            .ok_or_else(|| TopologyLifecycleError::OwnershipStepMismatch {
                operation_id: self.operation_id.to_string(),
                resource_kind: resource_kind_identity(&acknowledgement.ownership.resource_kind),
                resource_id: acknowledgement.ownership.resource_id.clone(),
            })?;
        if step_is_exact_terminal_replay(
            step.status,
            step.failure_reason.as_deref(),
            &acknowledgement.result,
        ) {
            return Ok(());
        }
        if step.status == LifecycleStepStatus::Succeeded {
            return Err(TopologyLifecycleError::OwnershipStepMismatch {
                operation_id: self.operation_id.to_string(),
                resource_kind: resource_kind_identity(&acknowledgement.ownership.resource_kind),
                resource_id: acknowledgement.ownership.resource_id.clone(),
            });
        }
        match &acknowledgement.result {
            LifecycleStepResult::Succeeded => {
                step.status = LifecycleStepStatus::Succeeded;
                step.failure_reason = None;
            }
            LifecycleStepResult::Failed { reason } => {
                if reason.trim().is_empty() {
                    return Err(TopologyLifecycleError::InvalidOperation {
                        reason: "failed cleanup acknowledgement requires a reason".to_string(),
                    });
                }
                step.status = LifecycleStepStatus::Failed;
                step.failure_reason = Some(reason.clone());
            }
        }
        self.updated_at = self.updated_at.max(now);
        self.refresh_status(now);
        Ok(())
    }

    /// Compute the coherent terminal aggregate state after every step finishes.
    pub fn final_environment_state(&self) -> Result<EnvironmentState, TopologyLifecycleError> {
        let all_steps = self
            .machine_steps
            .iter()
            .map(|step| step.status)
            .chain(self.cleanup_steps.iter().map(|step| step.status));
        let statuses = all_steps.collect::<Vec<_>>();
        if statuses.iter().any(|status| {
            matches!(
                status,
                LifecycleStepStatus::Pending | LifecycleStepStatus::Running
            )
        }) {
            return Err(TopologyLifecycleError::OperationIncomplete {
                operation_id: self.operation_id.to_string(),
            });
        }
        let failed = statuses
            .iter()
            .filter(|status| **status == LifecycleStepStatus::Failed)
            .count();
        match self.kind {
            EnvironmentLifecycleKind::Up if failed == 0 => Ok(EnvironmentState::Ready),
            EnvironmentLifecycleKind::Up if failed < self.machine_steps.len() => {
                Ok(EnvironmentState::Degraded)
            }
            EnvironmentLifecycleKind::Up => Ok(EnvironmentState::Failed),
            EnvironmentLifecycleKind::Stop if failed == 0 => Ok(EnvironmentState::Stopped),
            EnvironmentLifecycleKind::Stop => Ok(EnvironmentState::Failed),
            EnvironmentLifecycleKind::Delete if failed == 0 => Ok(EnvironmentState::Deleted),
            EnvironmentLifecycleKind::Delete => Err(TopologyLifecycleError::OperationFailed {
                operation_id: self.operation_id.to_string(),
            }),
        }
    }

    /// Commit a non-delete terminal state to the in-memory aggregate.
    pub fn finish_live_transition(
        &mut self,
        environment: &mut EnvironmentInstance,
        now: u64,
    ) -> Result<EnvironmentState, TopologyLifecycleError> {
        self.ensure_fence(environment, &self.operation_id.clone(), self.generation)?;
        if self.kind == EnvironmentLifecycleKind::Delete {
            return Err(TopologyLifecycleError::DeleteRequired {
                operation_id: self.operation_id.to_string(),
            });
        }
        let target = self.final_environment_state()?;
        environment.state = target;
        environment.active_operation_id = None;
        environment.updated_at = environment.updated_at.max(now);
        self.status = if target == self.requested_target {
            EnvironmentLifecycleStatus::Succeeded
        } else {
            EnvironmentLifecycleStatus::Failed
        };
        self.updated_at = self.updated_at.max(now);
        self.completed_at = Some(self.updated_at);
        self.validate_structure()?;
        environment
            .validate()
            .map_err(|error| TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            })?;
        Ok(target)
    }

    /// Produce the terminal tombstone after every exact delete step succeeds.
    pub fn finish_delete(
        &mut self,
        environment: &EnvironmentInstance,
        now: u64,
    ) -> Result<EnvironmentTombstone, TopologyLifecycleError> {
        self.ensure_fence(environment, &self.operation_id.clone(), self.generation)?;
        if self.kind != EnvironmentLifecycleKind::Delete {
            return Err(TopologyLifecycleError::DeleteRequired {
                operation_id: self.operation_id.to_string(),
            });
        }
        self.validate_against_environment(environment)?;
        if self.final_environment_state()? != EnvironmentState::Deleted {
            return Err(TopologyLifecycleError::OperationIncomplete {
                operation_id: self.operation_id.to_string(),
            });
        }
        let ownership_digest = canonical_ownership_digest(
            self.cleanup_steps
                .iter()
                .map(|step| step.ownership.clone())
                .collect(),
        )?;
        self.status = EnvironmentLifecycleStatus::Succeeded;
        self.updated_at = self.updated_at.max(now);
        self.completed_at = Some(self.updated_at);
        self.validate_structure()?;
        let tombstone = EnvironmentTombstone {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            name: environment.name.clone(),
            definition_digest: environment.definition_digest.clone(),
            delete_operation_id: self.operation_id.clone(),
            lifecycle_generation: self.generation,
            ownership_digest,
            deleted_at: self.updated_at,
        };
        tombstone.validate()?;
        Ok(tombstone)
    }

    /// Fence an attached non-delete operation so a Delete can take the next generation.
    ///
    /// Persistence adapters must commit this mutation and the replacement Delete begin
    /// atomically. `Failed` is the only stable aggregate state that can faithfully retain
    /// an interrupted mixture of child Machine states without inventing success.
    pub fn supersede_for_delete(
        &mut self,
        environment: &mut EnvironmentInstance,
        now: u64,
    ) -> Result<(), TopologyLifecycleError> {
        if self.kind == EnvironmentLifecycleKind::Delete
            || self.status != EnvironmentLifecycleStatus::Running
        {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason:
                    "only an attached running Up or Stop operation may be superseded for delete"
                        .to_string(),
            });
        }
        self.ensure_fence(environment, &self.operation_id.clone(), self.generation)?;
        if environment.state != EnvironmentState::Reconciling {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: "a superseded Up or Stop must own a Reconciling Environment".to_string(),
            });
        }
        environment
            .validate()
            .map_err(|error| TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            })?;
        self.status = EnvironmentLifecycleStatus::Superseded;
        self.updated_at = self.updated_at.max(now);
        self.completed_at = Some(self.updated_at);
        environment.state = EnvironmentState::Failed;
        environment.active_operation_id = None;
        environment.updated_at = environment.updated_at.max(now);
        self.validate_structure()?;
        environment
            .validate()
            .map_err(|error| TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            })
    }

    fn ensure_fence(
        &self,
        environment: &EnvironmentInstance,
        operation_id: &LifecycleOperationId,
        generation: u64,
    ) -> Result<(), TopologyLifecycleError> {
        if operation_id != &self.operation_id {
            return Err(TopologyLifecycleError::OperationMismatch {
                environment_id: environment.environment_id.to_string(),
                expected: self.operation_id.to_string(),
                found: operation_id.to_string(),
            });
        }
        if generation != self.generation {
            return Err(TopologyLifecycleError::GenerationMismatch {
                operation_id: self.operation_id.to_string(),
                expected: self.generation,
                found: generation,
            });
        }
        if environment.lifecycle_generation != self.generation {
            return Err(TopologyLifecycleError::GenerationMismatch {
                operation_id: self.operation_id.to_string(),
                expected: environment.lifecycle_generation,
                found: self.generation,
            });
        }
        match &environment.active_operation_id {
            Some(active) if active == &self.operation_id => Ok(()),
            Some(active) => Err(TopologyLifecycleError::OperationMismatch {
                environment_id: environment.environment_id.to_string(),
                expected: active.to_string(),
                found: self.operation_id.to_string(),
            }),
            None => Err(TopologyLifecycleError::OperationMismatch {
                environment_id: environment.environment_id.to_string(),
                expected: "active operation".to_string(),
                found: self.operation_id.to_string(),
            }),
        }
    }

    fn refresh_status(&mut self, now: u64) {
        let statuses = self
            .machine_steps
            .iter()
            .map(|step| step.status)
            .chain(self.cleanup_steps.iter().map(|step| step.status))
            .collect::<Vec<_>>();
        let delete_is_blocked = self.kind == EnvironmentLifecycleKind::Delete
            && statuses.iter().all(|status| {
                matches!(
                    status,
                    LifecycleStepStatus::Succeeded | LifecycleStepStatus::Failed
                )
            })
            && statuses.contains(&LifecycleStepStatus::Failed);
        if delete_is_blocked {
            self.status = EnvironmentLifecycleStatus::Blocked;
        } else {
            // Step completion is not aggregate completion. Keep the operation
            // active and resumable until finish_live_transition or
            // finish_delete atomically publishes the terminal aggregate state.
            self.status = EnvironmentLifecycleStatus::Running;
        }
        self.updated_at = self.updated_at.max(now);
        self.completed_at = None;
    }
}

impl EnvironmentTombstone {
    pub fn validate(&self) -> Result<(), TopologyLifecycleError> {
        validate_schema(self.schema_version).map_err(|error| {
            TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            }
        })?;
        self.project_id
            .validate()
            .map_err(|error| TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            })?;
        self.environment_id.validate().map_err(|error| {
            TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            }
        })?;
        self.delete_operation_id.validate().map_err(|error| {
            TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            }
        })?;
        validate_name("environment", &self.name).map_err(|error| {
            TopologyLifecycleError::InvalidOperation {
                reason: error.to_string(),
            }
        })?;
        if self.lifecycle_generation == 0
            || self.definition_digest.trim().is_empty()
            || self.ownership_digest.trim().is_empty()
        {
            return Err(TopologyLifecycleError::InvalidOperation {
                reason: "tombstone requires positive generation and non-empty digests".to_string(),
            });
        }
        Ok(())
    }
}

fn ensure_lifecycle_transition_allowed(
    environment: &EnvironmentInstance,
    kind: EnvironmentLifecycleKind,
) -> Result<(), TopologyLifecycleError> {
    if let Some(operation_id) = &environment.active_operation_id {
        return Err(TopologyLifecycleError::OperationConflict {
            environment_id: environment.environment_id.to_string(),
            active_operation_id: operation_id.to_string(),
        });
    }
    let allowed = lifecycle_transition_allowed_from_state(environment.state, kind);
    if allowed {
        Ok(())
    } else {
        Err(TopologyLifecycleError::InvalidTransition {
            environment_id: environment.environment_id.to_string(),
            operation: kind,
            state: environment.state,
        })
    }
}

fn lifecycle_transition_allowed_from_state(
    state: EnvironmentState,
    kind: EnvironmentLifecycleKind,
) -> bool {
    match kind {
        EnvironmentLifecycleKind::Up => matches!(
            state,
            EnvironmentState::Creating
                | EnvironmentState::Ready
                | EnvironmentState::Degraded
                | EnvironmentState::Stopped
                | EnvironmentState::Failed
        ),
        EnvironmentLifecycleKind::Stop => matches!(
            state,
            EnvironmentState::Ready
                | EnvironmentState::Degraded
                | EnvironmentState::Stopped
                | EnvironmentState::Failed
        ),
        EnvironmentLifecycleKind::Delete => matches!(
            state,
            EnvironmentState::Creating
                | EnvironmentState::Ready
                | EnvironmentState::Degraded
                | EnvironmentState::Stopped
                | EnvironmentState::Failed
        ),
    }
}

fn validate_operation_status_coherence(
    operation: &EnvironmentLifecycleOperation,
) -> Result<(), TopologyLifecycleError> {
    let statuses = operation
        .machine_steps
        .iter()
        .map(|step| step.status)
        .chain(operation.cleanup_steps.iter().map(|step| step.status))
        .collect::<Vec<_>>();
    let all_pending = statuses
        .iter()
        .all(|status| *status == LifecycleStepStatus::Pending);
    let all_terminal = statuses.iter().all(|status| {
        matches!(
            status,
            LifecycleStepStatus::Succeeded | LifecycleStepStatus::Failed
        )
    });
    let all_succeeded = statuses
        .iter()
        .all(|status| *status == LifecycleStepStatus::Succeeded);
    let any_failed = statuses.contains(&LifecycleStepStatus::Failed);
    let coherent = match operation.status {
        EnvironmentLifecycleStatus::Planned => all_pending,
        EnvironmentLifecycleStatus::Running => {
            !(operation.kind == EnvironmentLifecycleKind::Delete && all_terminal && any_failed)
        }
        EnvironmentLifecycleStatus::Blocked => {
            operation.kind == EnvironmentLifecycleKind::Delete && all_terminal && any_failed
        }
        EnvironmentLifecycleStatus::Succeeded => all_succeeded,
        EnvironmentLifecycleStatus::Failed => {
            operation.kind != EnvironmentLifecycleKind::Delete && all_terminal && any_failed
        }
        EnvironmentLifecycleStatus::Superseded => {
            operation.kind != EnvironmentLifecycleKind::Delete
        }
    };
    if coherent {
        Ok(())
    } else {
        Err(TopologyLifecycleError::InvalidOperation {
            reason: format!(
                "operation status {:?} is incoherent with {:?} step results",
                operation.status, operation.kind
            ),
        })
    }
}

fn validate_lifecycle_incarnation(
    machine_id: &MachineId,
    incarnation: Option<&MachineIncarnation>,
) -> Result<(), TopologyLifecycleError> {
    let Some(incarnation) = incarnation else {
        return Ok(());
    };
    validate_schema(incarnation.schema_version).map_err(|error| {
        TopologyLifecycleError::InvalidOperation {
            reason: error.to_string(),
        }
    })?;
    incarnation.incarnation_id.validate().map_err(|error| {
        TopologyLifecycleError::InvalidOperation {
            reason: error.to_string(),
        }
    })?;
    if incarnation.machine_id != *machine_id || incarnation.generation == 0 {
        return Err(TopologyLifecycleError::InvalidOperation {
            reason: format!(
                "incarnation for Machine `{machine_id}` must have the exact Machine owner and positive generation"
            ),
        });
    }
    Ok(())
}

fn validate_up_incarnation_transition(
    machine_id: &MachineId,
    expected: Option<&MachineIncarnation>,
    resulting: &MachineIncarnation,
) -> Result<(), TopologyLifecycleError> {
    validate_lifecycle_incarnation(machine_id, Some(resulting))?;
    let valid = match expected {
        None => resulting.generation == 1,
        Some(expected) if expected == resulting => true,
        Some(expected) => {
            resulting.incarnation_id != expected.incarnation_id
                && resulting.generation > expected.generation
                && resulting.created_at >= expected.created_at
        }
    };
    if valid {
        Ok(())
    } else {
        Err(TopologyLifecycleError::InvalidOperation {
            reason: format!(
                "Up result for Machine `{machine_id}` must preserve its incarnation or replace it with a distinct, newer generation"
            ),
        })
    }
}

fn validate_machine_acknowledgement_incarnation(
    kind: EnvironmentLifecycleKind,
    step: &MachineLifecycleStep,
    acknowledgement: &MachineLifecycleStepAcknowledgement,
) -> Result<(), TopologyLifecycleError> {
    validate_lifecycle_incarnation(
        &acknowledgement.machine_id,
        acknowledgement.expected_incarnation.as_ref(),
    )?;
    validate_lifecycle_incarnation(
        &acknowledgement.machine_id,
        acknowledgement.resulting_incarnation.as_ref(),
    )?;
    match (
        &acknowledgement.result,
        kind,
        acknowledgement.resulting_incarnation.as_ref(),
    ) {
        (LifecycleStepResult::Succeeded, EnvironmentLifecycleKind::Up, Some(resulting)) => {
            validate_up_incarnation_transition(
                &step.machine_id,
                step.expected_incarnation.as_ref(),
                resulting,
            )
        }
        (LifecycleStepResult::Succeeded, EnvironmentLifecycleKind::Up, None) => {
            Err(TopologyLifecycleError::MachineStepMismatch {
                machine_id: step.machine_id.to_string(),
            })
        }
        (LifecycleStepResult::Failed { .. }, EnvironmentLifecycleKind::Up, None)
        | (_, EnvironmentLifecycleKind::Stop | EnvironmentLifecycleKind::Delete, None) => Ok(()),
        _ => Err(TopologyLifecycleError::MachineStepMismatch {
            machine_id: step.machine_id.to_string(),
        }),
    }
}

fn apply_up_incarnation(
    machine: &mut MachineInstance,
    ownership: &mut Vec<OwnershipRecord>,
    expected: Option<&MachineIncarnation>,
    resulting: &MachineIncarnation,
) -> Result<(), TopologyLifecycleError> {
    if machine.incarnation.as_ref() != expected {
        return Err(TopologyLifecycleError::MachineStepMismatch {
            machine_id: machine.machine_id.to_string(),
        });
    }
    if expected == Some(resulting) {
        let exact_count = ownership
            .iter()
            .filter(|record| {
                record.resource_kind == OwnedResourceKind::Incarnation
                    && record.resource_id == resulting.incarnation_id.as_str()
                    && record.machine_id.as_ref() == Some(&machine.machine_id)
            })
            .count();
        if exact_count != 1 {
            return Err(TopologyLifecycleError::MachineStepMismatch {
                machine_id: machine.machine_id.to_string(),
            });
        }
        return Ok(());
    }

    let old_index = if let Some(expected) = expected {
        let matching = ownership
            .iter()
            .enumerate()
            .filter(|(_, record)| {
                record.resource_kind == OwnedResourceKind::Incarnation
                    && record.resource_id == expected.incarnation_id.as_str()
                    && record.machine_id.as_ref() == Some(&machine.machine_id)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Err(TopologyLifecycleError::MachineStepMismatch {
                machine_id: machine.machine_id.to_string(),
            });
        }
        Some(matching[0])
    } else {
        None
    };
    if ownership.iter().any(|record| {
        record.resource_kind == OwnedResourceKind::Incarnation
            && record.resource_id == resulting.incarnation_id.as_str()
    }) {
        return Err(TopologyLifecycleError::MachineStepMismatch {
            machine_id: machine.machine_id.to_string(),
        });
    }
    if let Some(old_index) = old_index {
        ownership.remove(old_index);
    }
    ownership.push(OwnershipRecord {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        resource_kind: OwnedResourceKind::Incarnation,
        resource_id: resulting.incarnation_id.to_string(),
        environment_id: machine.environment_id.clone(),
        machine_id: Some(machine.machine_id.clone()),
    });
    machine.incarnation = Some(resulting.clone());
    Ok(())
}

fn validate_step_status(
    status: LifecycleStepStatus,
    failure_reason: Option<&str>,
) -> Result<(), TopologyLifecycleError> {
    match (status, failure_reason) {
        (LifecycleStepStatus::Failed, Some(reason)) if !reason.trim().is_empty() => Ok(()),
        (LifecycleStepStatus::Failed, _) => Err(TopologyLifecycleError::InvalidOperation {
            reason: "failed lifecycle step requires a non-empty reason".to_string(),
        }),
        (_, None) => Ok(()),
        (_, Some(_)) => Err(TopologyLifecycleError::InvalidOperation {
            reason: "only a failed lifecycle step may carry a failure reason".to_string(),
        }),
    }
}

fn step_is_exact_terminal_replay(
    status: LifecycleStepStatus,
    failure_reason: Option<&str>,
    result: &LifecycleStepResult,
) -> bool {
    match (status, failure_reason, result) {
        (LifecycleStepStatus::Succeeded, None, LifecycleStepResult::Succeeded) => true,
        (LifecycleStepStatus::Failed, Some(existing), LifecycleStepResult::Failed { reason }) => {
            existing == reason
        }
        _ => false,
    }
}

fn ownership_sort_key(record: &OwnershipRecord) -> (String, String, String) {
    (
        resource_kind_identity(&record.resource_kind),
        record.resource_id.clone(),
        record
            .machine_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default(),
    )
}

fn canonical_ownership_digest(
    mut ownership: Vec<OwnershipRecord>,
) -> Result<String, TopologyLifecycleError> {
    ownership.sort_by_key(ownership_sort_key);
    let bytes = serde_json::to_vec(&ownership).map_err(|error| {
        TopologyLifecycleError::InvalidOperation {
            reason: format!("failed to serialize canonical ownership: {error}"),
        }
    })?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
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
            if incarnation.generation == 0 {
                return Err(TopologyValidationError::InvalidMachineIncarnation {
                    machine_id: self.machine_id.to_string(),
                    reason: "generation must be greater than zero".to_string(),
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

fn validate_exact_topology_ownership(
    environment: &EnvironmentInstance,
) -> Result<(), TopologyValidationError> {
    let ownership_mismatch =
        |kind: &str, value: String| TopologyValidationError::OwnershipMismatch {
            kind: kind.to_string(),
            value,
        };

    for machine in &environment.machines {
        let exact_machine = environment.ownership.iter().filter(|record| {
            record.resource_kind == OwnedResourceKind::Machine
                && record.resource_id == machine.machine_id.as_str()
                && record.machine_id.as_ref() == Some(&machine.machine_id)
        });
        let exact_machine_count = exact_machine.count();
        let exact_legacy_count = machine
            .legacy_sandbox_id
            .as_deref()
            .map(|legacy_id| {
                environment
                    .ownership
                    .iter()
                    .filter(|record| {
                        record.resource_kind == OwnedResourceKind::LegacySandbox
                            && record.resource_id == legacy_id
                            && record.machine_id.as_ref() == Some(&machine.machine_id)
                    })
                    .count()
            })
            .unwrap_or(0);
        let has_exact_legacy_proof =
            machine
                .legacy_sandbox_id
                .as_deref()
                .is_some_and(|legacy_id| {
                    exact_legacy_count == 1
                        && environment
                            .legacy_migration
                            .as_ref()
                            .is_some_and(|provenance| {
                                provenance.legacy_sandbox_id == legacy_id
                                    && provenance.source_version == "v0.3.20"
                            })
                });
        if !((exact_machine_count == 1 && !has_exact_legacy_proof)
            || (exact_machine_count == 0 && has_exact_legacy_proof))
        {
            return Err(ownership_mismatch(
                "machine",
                machine.machine_id.to_string(),
            ));
        }

        match &machine.incarnation {
            Some(incarnation) => {
                let exact = environment.ownership.iter().filter(|record| {
                    record.resource_kind == OwnedResourceKind::Incarnation
                        && record.resource_id == incarnation.incarnation_id.as_str()
                        && record.machine_id.as_ref() == Some(&machine.machine_id)
                });
                if exact.count() != 1 {
                    return Err(ownership_mismatch(
                        "incarnation",
                        incarnation.incarnation_id.to_string(),
                    ));
                }
            }
            None if matches!(machine.state, MachineState::Ready | MachineState::Stopped)
                && !has_exact_legacy_proof =>
            {
                return Err(TopologyValidationError::InvalidMachineIncarnation {
                    machine_id: machine.machine_id.to_string(),
                    reason: format!("state {:?} requires a current incarnation", machine.state),
                });
            }
            None => {}
        }
    }

    for network in &environment.networks {
        let exact = environment.ownership.iter().filter(|record| {
            record.resource_kind == OwnedResourceKind::Network
                && record.resource_id == network.network_id.as_str()
                && record.machine_id.is_none()
        });
        if exact.count() != 1 {
            return Err(ownership_mismatch(
                "network",
                network.network_id.to_string(),
            ));
        }
    }

    for endpoint in &environment.endpoints {
        let exact = environment.ownership.iter().filter(|record| {
            record.resource_kind == OwnedResourceKind::Endpoint
                && record.resource_id == endpoint.endpoint_id.as_str()
                && record.machine_id.as_ref() == Some(&endpoint.machine_id)
        });
        if exact.count() != 1 {
            return Err(ownership_mismatch(
                "endpoint",
                endpoint.endpoint_id.to_string(),
            ));
        }
    }

    for record in &environment.ownership {
        let known = match record.resource_kind {
            OwnedResourceKind::Machine => environment.machines.iter().any(|machine| {
                record.resource_id == machine.machine_id.as_str()
                    && record.machine_id.as_ref() == Some(&machine.machine_id)
                    && machine.legacy_sandbox_id.is_none()
            }),
            OwnedResourceKind::Incarnation => environment.machines.iter().any(|machine| {
                record.machine_id.as_ref() == Some(&machine.machine_id)
                    && machine.incarnation.as_ref().is_some_and(|incarnation| {
                        record.resource_id == incarnation.incarnation_id.as_str()
                    })
            }),
            OwnedResourceKind::Network => environment.networks.iter().any(|network| {
                record.resource_id == network.network_id.as_str() && record.machine_id.is_none()
            }),
            OwnedResourceKind::Endpoint => environment.endpoints.iter().any(|endpoint| {
                record.resource_id == endpoint.endpoint_id.as_str()
                    && record.machine_id.as_ref() == Some(&endpoint.machine_id)
            }),
            OwnedResourceKind::LegacySandbox => {
                environment
                    .legacy_migration
                    .as_ref()
                    .is_some_and(|provenance| {
                        provenance.legacy_sandbox_id == record.resource_id
                            && provenance.source_version == "v0.3.20"
                    })
                    && environment.machines.iter().any(|machine| {
                        machine.legacy_sandbox_id.as_deref() == Some(record.resource_id.as_str())
                            && record.machine_id.as_ref() == Some(&machine.machine_id)
                    })
            }
            _ => true,
        };
        if !known {
            return Err(ownership_mismatch(
                "resource",
                format!(
                    "{}:{}",
                    resource_kind_identity(&record.resource_kind),
                    record.resource_id
                ),
            ));
        }
    }
    Ok(())
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
        lifecycle_generation: 0,
        active_operation_id: None,
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

    fn add_current_incarnations(environment: &mut EnvironmentInstance) {
        let additions = environment
            .machines
            .iter_mut()
            .enumerate()
            .map(|(index, machine)| {
                let incarnation = MachineIncarnation {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    incarnation_id: MachineIncarnationId::new(format!("inc_test_{index}")).unwrap(),
                    machine_id: machine.machine_id.clone(),
                    generation: 1,
                    created_at: 50,
                };
                machine.incarnation = Some(incarnation.clone());
                OwnershipRecord {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    resource_kind: OwnedResourceKind::Incarnation,
                    resource_id: incarnation.incarnation_id.to_string(),
                    environment_id: environment.environment_id.clone(),
                    machine_id: Some(machine.machine_id.clone()),
                }
            })
            .collect::<Vec<_>>();
        environment.ownership.extend(additions);
    }

    fn operational_environment(state: EnvironmentState) -> EnvironmentInstance {
        let definition = project_definition();
        let mut environment = definition.instantiate_environment("agent", 42).unwrap();
        bind(&mut environment, "workspace-token");
        for machine in &mut environment.machines {
            machine.negotiated_capabilities = machine.requested_capabilities.clone();
            machine.state = match state {
                EnvironmentState::Stopped => MachineState::Stopped,
                _ => MachineState::Ready,
            };
        }
        add_current_incarnations(&mut environment);
        environment.state = state;
        environment.validate().unwrap();
        environment
    }

    fn machine_acknowledgement(
        operation: &EnvironmentLifecycleOperation,
        step: &MachineLifecycleStep,
        result: LifecycleStepResult,
        resulting_incarnation: Option<MachineIncarnation>,
    ) -> MachineLifecycleStepAcknowledgement {
        MachineLifecycleStepAcknowledgement {
            operation_id: operation.operation_id.clone(),
            generation: operation.generation,
            machine_id: step.machine_id.clone(),
            initial_state: step.initial_state,
            target_state: step.target_state,
            expected_incarnation: step.expected_incarnation.clone(),
            resulting_incarnation,
            result,
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
        second.machines[0].legacy_sandbox_id = None;
        second.ownership[0].resource_kind = OwnedResourceKind::Machine;
        second.ownership[0].resource_id = second.machines[0].machine_id.to_string();
        second.ownership[0].environment_id = second.environment_id.clone();
        second.ownership[0].machine_id = Some(second.machines[0].machine_id.clone());
        second.legacy_migration = None;
        add_current_incarnations(&mut second);
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
        add_current_incarnations(&mut environment);
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
        add_current_incarnations(&mut stopped);
        for state in [EnvironmentState::Stopped, EnvironmentState::Deleting] {
            let mut environment = stopped.clone();
            environment.state = state;
            if state == EnvironmentState::Deleting {
                environment.lifecycle_generation = 1;
                environment.active_operation_id = Some(LifecycleOperationId::generate());
            }
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

        let mut invalid_resource = operational_environment(EnvironmentState::Ready);
        invalid_resource.ownership[0].resource_id.clear();
        assert!(matches!(
            invalid_resource.validate(),
            Err(TopologyValidationError::InvalidIdentifier { kind, .. })
                if kind == "ownership_resource_id"
        ));

        let mut invalid_custom_kind = operational_environment(EnvironmentState::Ready);
        invalid_custom_kind.ownership.push(OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::Other(String::new()),
            resource_id: "custom-resource".to_string(),
            environment_id: invalid_custom_kind.environment_id.clone(),
            machine_id: None,
        });
        assert!(matches!(
            invalid_custom_kind.validate(),
            Err(TopologyValidationError::InvalidIdentifier { kind, .. })
                if kind == "ownership_resource_kind"
        ));
    }

    #[test]
    fn lifecycle_transition_matrix_is_exhaustive_for_stable_states() {
        let creating = project_definition()
            .instantiate_environment("creating", 1)
            .unwrap();
        let ready = operational_environment(EnvironmentState::Ready);
        let stopped = operational_environment(EnvironmentState::Stopped);
        let mut degraded = ready.clone();
        degraded.state = EnvironmentState::Degraded;
        degraded.machines[0].state = MachineState::Failed;
        degraded.validate().unwrap();
        let mut failed = ready.clone();
        failed.state = EnvironmentState::Failed;
        for machine in &mut failed.machines {
            machine.state = MachineState::Failed;
        }
        failed.validate().unwrap();

        let cases = [
            (&creating, true, false, true),
            (&ready, true, true, true),
            (&degraded, true, true, true),
            (&stopped, true, true, true),
            (&failed, true, true, true),
        ];
        for (environment, up, stop, delete) in cases {
            assert_eq!(
                ensure_lifecycle_transition_allowed(environment, EnvironmentLifecycleKind::Up)
                    .is_ok(),
                up,
                "up from {:?}",
                environment.state
            );
            assert_eq!(
                ensure_lifecycle_transition_allowed(environment, EnvironmentLifecycleKind::Stop)
                    .is_ok(),
                stop,
                "stop from {:?}",
                environment.state
            );
            assert_eq!(
                ensure_lifecycle_transition_allowed(environment, EnvironmentLifecycleKind::Delete)
                    .is_ok(),
                delete,
                "delete from {:?}",
                environment.state
            );
        }

        let mut reconciling = ready.clone();
        let mut operation = EnvironmentLifecycleOperation::plan(
            &reconciling,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Up,
            "req-transition",
            "idem-transition",
            "sha256:transition",
            2,
        )
        .unwrap();
        operation.begin(&mut reconciling, 3).unwrap();
        for kind in [
            EnvironmentLifecycleKind::Up,
            EnvironmentLifecycleKind::Stop,
            EnvironmentLifecycleKind::Delete,
        ] {
            assert!(matches!(
                ensure_lifecycle_transition_allowed(&reconciling, kind),
                Err(TopologyLifecycleError::OperationConflict { .. })
            ));
        }
    }

    #[test]
    fn lifecycle_terminal_coherence_rejects_mixed_and_live_deleted_aggregates() {
        let ready = operational_environment(EnvironmentState::Ready);

        let mut mixed_ready = ready.clone();
        mixed_ready.machines[0].state = MachineState::Failed;
        assert!(matches!(
            mixed_ready.validate(),
            Err(TopologyValidationError::InvalidLifecycleState { .. })
        ));

        let mut degraded = ready.clone();
        degraded.state = EnvironmentState::Degraded;
        assert!(matches!(
            degraded.validate(),
            Err(TopologyValidationError::InvalidLifecycleState { .. })
        ));
        degraded.machines[0].state = MachineState::Failed;
        degraded.validate().unwrap();

        let mut deleted = ready;
        deleted.state = EnvironmentState::Deleted;
        assert!(matches!(
            deleted.validate(),
            Err(TopologyValidationError::InvalidLifecycleState { .. })
        ));
    }

    #[test]
    fn lifecycle_ownership_completeness_rejects_missing_and_forged_nodes() {
        let environment = operational_environment(EnvironmentState::Ready);

        for kind in [
            OwnedResourceKind::Machine,
            OwnedResourceKind::Network,
            OwnedResourceKind::Endpoint,
            OwnedResourceKind::Incarnation,
        ] {
            let mut missing = environment.clone();
            let index = missing
                .ownership
                .iter()
                .position(|record| record.resource_kind == kind)
                .unwrap();
            missing.ownership.remove(index);
            assert!(matches!(
                missing.validate(),
                Err(TopologyValidationError::OwnershipMismatch { .. })
                    | Err(TopologyValidationError::InvalidMachineIncarnation { .. })
            ));
        }

        let mut forged = environment.clone();
        let machine = &forged.machines[0];
        forged.ownership.push(OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::Incarnation,
            resource_id: "inc_forged".to_string(),
            environment_id: forged.environment_id.clone(),
            machine_id: Some(machine.machine_id.clone()),
        });
        assert!(matches!(
            forged.validate(),
            Err(TopologyValidationError::OwnershipMismatch { .. })
        ));

        let mut zero_generation = environment;
        zero_generation.machines[0]
            .incarnation
            .as_mut()
            .unwrap()
            .generation = 0;
        assert!(matches!(
            zero_generation.validate(),
            Err(TopologyValidationError::InvalidMachineIncarnation { .. })
        ));
    }

    #[test]
    fn lifecycle_stop_preserves_identity_incarnation_definition_and_ownership() {
        let mut environment = operational_environment(EnvironmentState::Ready);
        let before_environment_id = environment.environment_id.clone();
        let before_machine_ids = environment
            .machines
            .iter()
            .map(|machine| machine.machine_id.clone())
            .collect::<Vec<_>>();
        let before_incarnations = environment
            .machines
            .iter()
            .map(|machine| machine.incarnation.clone())
            .collect::<Vec<_>>();
        let before_definition = environment.definition_digest.clone();
        let before_ownership = environment.ownership.clone();
        let mut operation = EnvironmentLifecycleOperation::plan(
            &environment,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Stop,
            "req-stop",
            "idem-stop",
            "sha256:stop",
            100,
        )
        .unwrap();
        operation.begin(&mut environment, 101).unwrap();
        for step in operation.machine_steps.clone() {
            operation
                .apply_machine_step_acknowledgement(
                    &mut environment,
                    &MachineLifecycleStepAcknowledgement {
                        operation_id: operation.operation_id.clone(),
                        generation: operation.generation,
                        machine_id: step.machine_id,
                        initial_state: step.initial_state,
                        target_state: step.target_state,
                        expected_incarnation: step.expected_incarnation,
                        resulting_incarnation: None,
                        result: LifecycleStepResult::Succeeded,
                    },
                    102,
                )
                .unwrap();
        }
        assert_eq!(operation.status, EnvironmentLifecycleStatus::Running);
        assert_eq!(operation.completed_at, None);
        assert_eq!(
            environment.active_operation_id.as_ref(),
            Some(&operation.operation_id)
        );
        assert_eq!(
            operation
                .finish_live_transition(&mut environment, 103)
                .unwrap(),
            EnvironmentState::Stopped
        );
        assert_eq!(environment.environment_id, before_environment_id);
        assert_eq!(
            environment
                .machines
                .iter()
                .map(|machine| machine.machine_id.clone())
                .collect::<Vec<_>>(),
            before_machine_ids
        );
        assert_eq!(
            environment
                .machines
                .iter()
                .map(|machine| machine.incarnation.clone())
                .collect::<Vec<_>>(),
            before_incarnations
        );
        assert_eq!(environment.definition_digest, before_definition);
        assert_eq!(environment.ownership, before_ownership);
    }

    #[test]
    fn lifecycle_delete_plan_is_canonical_and_requires_exact_owner_acknowledgements() {
        let mut environment = operational_environment(EnvironmentState::Ready);
        let mut operation = EnvironmentLifecycleOperation::plan(
            &environment,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Delete,
            "req-delete",
            "idem-delete",
            "sha256:delete",
            200,
        )
        .unwrap();
        assert!(
            operation
                .cleanup_steps
                .windows(2)
                .all(|pair| ownership_sort_key(&pair[0].ownership)
                    < ownership_sort_key(&pair[1].ownership))
        );
        operation.begin(&mut environment, 201).unwrap();

        for step in operation.machine_steps.clone() {
            operation
                .apply_machine_step_acknowledgement(
                    &mut environment,
                    &MachineLifecycleStepAcknowledgement {
                        operation_id: operation.operation_id.clone(),
                        generation: operation.generation,
                        machine_id: step.machine_id,
                        initial_state: step.initial_state,
                        target_state: step.target_state,
                        expected_incarnation: step.expected_incarnation,
                        resulting_incarnation: None,
                        result: LifecycleStepResult::Succeeded,
                    },
                    202,
                )
                .unwrap();
        }

        let mut forged = operation.cleanup_steps[0].ownership.clone();
        forged.resource_id.push_str("-foreign");
        assert!(matches!(
            operation.apply_cleanup_step_acknowledgement(
                &environment,
                &OwnershipCleanupStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    ownership: forged,
                    result: LifecycleStepResult::Succeeded,
                },
                203,
            ),
            Err(TopologyLifecycleError::OwnershipStepMismatch { .. })
        ));

        for step in operation.cleanup_steps.clone() {
            let acknowledgement = OwnershipCleanupStepAcknowledgement {
                operation_id: operation.operation_id.clone(),
                generation: operation.generation,
                ownership: step.ownership,
                result: LifecycleStepResult::Succeeded,
            };
            operation
                .apply_cleanup_step_acknowledgement(&environment, &acknowledgement, 204)
                .unwrap();
            operation
                .apply_cleanup_step_acknowledgement(&environment, &acknowledgement, 205)
                .expect("exact terminal acknowledgement is idempotent");
        }
        assert_eq!(operation.status, EnvironmentLifecycleStatus::Running);
        assert_eq!(operation.completed_at, None);
        assert_eq!(
            environment.active_operation_id.as_ref(),
            Some(&operation.operation_id)
        );
        let tombstone = operation.finish_delete(&environment, 206).unwrap();
        assert_eq!(tombstone.environment_id, environment.environment_id);
        assert_eq!(tombstone.delete_operation_id, operation.operation_id);
        tombstone.validate().unwrap();
    }

    #[test]
    fn lifecycle_structure_rejects_malformed_and_foreign_cleanup_ownership() {
        let environment = operational_environment(EnvironmentState::Ready);
        let operation = EnvironmentLifecycleOperation::plan(
            &environment,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Delete,
            "req-invalid-cleanup",
            "idem-invalid-cleanup",
            "sha256:invalid-cleanup",
            250,
        )
        .unwrap();
        let machine_owned_index = operation
            .cleanup_steps
            .iter()
            .position(|step| step.ownership.machine_id.is_some())
            .unwrap();

        let assert_invalid = |candidate: EnvironmentLifecycleOperation| {
            assert!(matches!(
                candidate.validate_structure(),
                Err(TopologyLifecycleError::InvalidOperation { .. })
            ));
        };

        let mut malformed_schema = operation.clone();
        malformed_schema.cleanup_steps[0].ownership.schema_version = TOPOLOGY_SCHEMA_VERSION + 1;
        assert_invalid(malformed_schema);

        let mut malformed_resource_id = operation.clone();
        malformed_resource_id.cleanup_steps[0].ownership.resource_id =
            "invalid/resource".to_string();
        assert_invalid(malformed_resource_id);

        let mut malformed_other_kind = operation.clone();
        malformed_other_kind.cleanup_steps[0]
            .ownership
            .resource_kind = OwnedResourceKind::Other("invalid/kind".to_string());
        assert_invalid(malformed_other_kind);

        let mut foreign_environment = operation.clone();
        foreign_environment.cleanup_steps[0]
            .ownership
            .environment_id = EnvironmentId::new("env_foreign_cleanup").unwrap();
        assert_invalid(foreign_environment);

        let mut foreign_machine = operation.clone();
        foreign_machine.cleanup_steps[machine_owned_index]
            .ownership
            .machine_id = Some(MachineId::new("mch_foreign_cleanup").unwrap());
        assert_invalid(foreign_machine);

        let mut missing_machine = operation;
        missing_machine.cleanup_steps[machine_owned_index]
            .ownership
            .machine_id = None;
        assert_invalid(missing_machine);
    }

    #[test]
    fn lifecycle_machine_failure_is_bounded_and_computes_degraded_terminal_state() {
        let mut environment = operational_environment(EnvironmentState::Stopped);
        let mut operation = EnvironmentLifecycleOperation::plan(
            &environment,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Up,
            "req-up",
            "idem-up",
            "sha256:up",
            300,
        )
        .unwrap();
        operation.begin(&mut environment, 301).unwrap();
        let failed = operation.machine_steps[0].clone();
        let sibling = operation.machine_steps[1].clone();
        let sibling_before = environment
            .machines
            .iter()
            .find(|machine| machine.machine_id == sibling.machine_id)
            .unwrap()
            .clone();
        operation
            .apply_machine_step_acknowledgement(
                &mut environment,
                &MachineLifecycleStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    machine_id: failed.machine_id.clone(),
                    initial_state: failed.initial_state,
                    target_state: failed.target_state,
                    expected_incarnation: failed.expected_incarnation.clone(),
                    resulting_incarnation: None,
                    result: LifecycleStepResult::Failed {
                        reason: "guest boot failed".to_string(),
                    },
                },
                302,
            )
            .unwrap();
        assert_eq!(
            environment
                .machines
                .iter()
                .find(|machine| machine.machine_id == sibling.machine_id)
                .unwrap(),
            &sibling_before
        );
        operation
            .apply_machine_step_acknowledgement(
                &mut environment,
                &MachineLifecycleStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    machine_id: sibling.machine_id.clone(),
                    initial_state: sibling.initial_state,
                    target_state: sibling.target_state,
                    expected_incarnation: sibling.expected_incarnation.clone(),
                    resulting_incarnation: sibling.expected_incarnation.clone(),
                    result: LifecycleStepResult::Succeeded,
                },
                303,
            )
            .unwrap();
        assert_eq!(operation.status, EnvironmentLifecycleStatus::Running);
        assert_eq!(operation.completed_at, None);
        assert_eq!(
            operation.final_environment_state().unwrap(),
            EnvironmentState::Degraded
        );
        assert_eq!(
            operation
                .finish_live_transition(&mut environment, 304)
                .unwrap(),
            EnvironmentState::Degraded
        );
        assert_eq!(
            environment
                .machines
                .iter()
                .find(|machine| machine.machine_id == failed.machine_id)
                .unwrap()
                .state,
            MachineState::Failed
        );
        assert_eq!(
            environment
                .machines
                .iter()
                .find(|machine| machine.machine_id == sibling_before.machine_id)
                .unwrap()
                .state,
            MachineState::Ready
        );
    }

    #[test]
    fn lifecycle_acknowledgements_are_operation_and_generation_fenced() {
        let mut environment = operational_environment(EnvironmentState::Ready);
        let mut operation = EnvironmentLifecycleOperation::plan(
            &environment,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Stop,
            "req-fence",
            "idem-fence",
            "sha256:fence",
            350,
        )
        .unwrap();
        operation.begin(&mut environment, 351).unwrap();
        let step = operation.machine_steps[0].clone();
        let mut acknowledgement = MachineLifecycleStepAcknowledgement {
            operation_id: operation.operation_id.clone(),
            generation: operation.generation - 1,
            machine_id: step.machine_id,
            initial_state: step.initial_state,
            target_state: step.target_state,
            expected_incarnation: step.expected_incarnation,
            resulting_incarnation: None,
            result: LifecycleStepResult::Succeeded,
        };
        assert!(matches!(
            operation.apply_machine_step_acknowledgement(&mut environment, &acknowledgement, 352),
            Err(TopologyLifecycleError::GenerationMismatch { .. })
        ));
        acknowledgement.generation = operation.generation;
        acknowledgement.operation_id = LifecycleOperationId::generate();
        assert!(matches!(
            operation.apply_machine_step_acknowledgement(&mut environment, &acknowledgement, 353),
            Err(TopologyLifecycleError::OperationMismatch { .. })
        ));
    }

    #[test]
    fn lifecycle_first_up_establishes_generation_one_incarnations_and_ownership() {
        let mut environment = project_definition()
            .instantiate_environment("first-up", 500)
            .unwrap();
        let mut operation = EnvironmentLifecycleOperation::plan(
            &environment,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Up,
            "req-first-up",
            "idem-first-up",
            "sha256:first-up",
            501,
        )
        .unwrap();
        assert!(
            operation
                .machine_steps
                .iter()
                .all(|step| step.expected_incarnation.is_none())
        );
        operation.begin(&mut environment, 502).unwrap();
        for machine in &mut environment.machines {
            machine.negotiated_capabilities = machine.requested_capabilities.clone();
        }
        for (index, step) in operation.machine_steps.clone().into_iter().enumerate() {
            let resulting = MachineIncarnation {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                incarnation_id: MachineIncarnationId::new(format!("inc_first_up_{index}")).unwrap(),
                machine_id: step.machine_id.clone(),
                generation: 1,
                created_at: 503,
            };
            let acknowledgement = machine_acknowledgement(
                &operation,
                &step,
                LifecycleStepResult::Succeeded,
                Some(resulting.clone()),
            );
            operation
                .apply_machine_step_acknowledgement(&mut environment, &acknowledgement, 503)
                .unwrap();
            assert_eq!(
                environment
                    .machines
                    .iter()
                    .find(|machine| machine.machine_id == step.machine_id)
                    .unwrap()
                    .incarnation
                    .as_ref(),
                Some(&resulting)
            );
            assert!(environment.ownership.iter().any(|record| {
                record.resource_kind == OwnedResourceKind::Incarnation
                    && record.resource_id == resulting.incarnation_id.as_str()
                    && record.machine_id.as_ref() == Some(&step.machine_id)
            }));
        }
        assert_eq!(
            operation
                .finish_live_transition(&mut environment, 504)
                .unwrap(),
            EnvironmentState::Ready
        );
        environment.validate().unwrap();
    }

    #[test]
    fn lifecycle_stop_and_delete_reject_stale_incarnation_acknowledgements() {
        for kind in [
            EnvironmentLifecycleKind::Stop,
            EnvironmentLifecycleKind::Delete,
        ] {
            let mut environment = operational_environment(EnvironmentState::Ready);
            let mut operation = EnvironmentLifecycleOperation::plan(
                &environment,
                LifecycleOperationId::generate(),
                kind,
                format!("req-stale-{kind:?}"),
                format!("idem-stale-{kind:?}"),
                "sha256:stale-incarnation",
                510,
            )
            .unwrap();
            operation.begin(&mut environment, 511).unwrap();
            let step = operation.machine_steps[0].clone();
            let mut acknowledgement =
                machine_acknowledgement(&operation, &step, LifecycleStepResult::Succeeded, None);
            acknowledgement
                .expected_incarnation
                .as_mut()
                .unwrap()
                .generation += 1;
            assert!(matches!(
                operation.apply_machine_step_acknowledgement(
                    &mut environment,
                    &acknowledgement,
                    512
                ),
                Err(TopologyLifecycleError::MachineStepMismatch { .. })
            ));
            assert_eq!(
                operation.machine_steps[0].status,
                LifecycleStepStatus::Pending
            );
        }
    }

    #[test]
    fn lifecycle_up_restart_preserves_the_exact_incarnation() {
        let mut environment = operational_environment(EnvironmentState::Stopped);
        let before = environment.clone();
        let mut operation = EnvironmentLifecycleOperation::plan(
            &environment,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Up,
            "req-restart",
            "idem-restart",
            "sha256:restart",
            520,
        )
        .unwrap();
        operation.begin(&mut environment, 521).unwrap();
        for step in operation.machine_steps.clone() {
            let acknowledgement = machine_acknowledgement(
                &operation,
                &step,
                LifecycleStepResult::Succeeded,
                step.expected_incarnation.clone(),
            );
            operation
                .apply_machine_step_acknowledgement(&mut environment, &acknowledgement, 522)
                .unwrap();
        }
        operation
            .finish_live_transition(&mut environment, 523)
            .unwrap();
        assert_eq!(
            environment
                .machines
                .iter()
                .map(|machine| machine.incarnation.clone())
                .collect::<Vec<_>>(),
            before
                .machines
                .iter()
                .map(|machine| machine.incarnation.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(environment.ownership, before.ownership);
    }

    #[test]
    fn lifecycle_up_rebuild_replaces_exact_incarnation_ownership() {
        let mut environment = operational_environment(EnvironmentState::Stopped);
        let mut operation = EnvironmentLifecycleOperation::plan(
            &environment,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Up,
            "req-rebuild",
            "idem-rebuild",
            "sha256:rebuild",
            530,
        )
        .unwrap();
        operation.begin(&mut environment, 531).unwrap();
        let rebuilt_step = operation.machine_steps[0].clone();
        let old = rebuilt_step.expected_incarnation.clone().unwrap();
        let replacement = MachineIncarnation {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            incarnation_id: MachineIncarnationId::new("inc_rebuilt").unwrap(),
            machine_id: rebuilt_step.machine_id.clone(),
            generation: old.generation + 1,
            created_at: old.created_at + 1,
        };
        let acknowledgement = machine_acknowledgement(
            &operation,
            &rebuilt_step,
            LifecycleStepResult::Succeeded,
            Some(replacement.clone()),
        );
        operation
            .apply_machine_step_acknowledgement(&mut environment, &acknowledgement, 532)
            .unwrap();
        assert_eq!(
            operation.machine_steps[0].resulting_incarnation.as_ref(),
            Some(&replacement)
        );
        assert!(!environment.ownership.iter().any(|record| {
            record.resource_kind == OwnedResourceKind::Incarnation
                && record.resource_id == old.incarnation_id.as_str()
        }));
        assert!(environment.ownership.iter().any(|record| {
            record.resource_kind == OwnedResourceKind::Incarnation
                && record.resource_id == replacement.incarnation_id.as_str()
                && record.machine_id.as_ref() == Some(&rebuilt_step.machine_id)
        }));
        environment.validate().unwrap();
    }

    #[test]
    fn lifecycle_delete_supersedes_non_delete_and_fences_old_acknowledgements() {
        let mut environment = operational_environment(EnvironmentState::Ready);
        let mut old = EnvironmentLifecycleOperation::plan(
            &environment,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Stop,
            "req-old-stop",
            "idem-old-stop",
            "sha256:old-stop",
            540,
        )
        .unwrap();
        old.begin(&mut environment, 541).unwrap();
        let old_step = old.machine_steps[0].clone();
        let stale = machine_acknowledgement(&old, &old_step, LifecycleStepResult::Succeeded, None);
        old.supersede_for_delete(&mut environment, 542).unwrap();
        assert_eq!(old.status, EnvironmentLifecycleStatus::Superseded);
        assert_eq!(environment.state, EnvironmentState::Failed);

        let mut delete = EnvironmentLifecycleOperation::plan(
            &environment,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Delete,
            "req-new-delete",
            "idem-new-delete",
            "sha256:new-delete",
            543,
        )
        .unwrap();
        assert_eq!(delete.generation, old.generation + 1);
        delete.begin(&mut environment, 544).unwrap();
        assert!(matches!(
            old.apply_machine_step_acknowledgement(&mut environment, &stale, 545),
            Err(TopologyLifecycleError::GenerationMismatch { .. })
                | Err(TopologyLifecycleError::OperationMismatch { .. })
        ));
        assert!(matches!(
            delete.supersede_for_delete(&mut environment, 546),
            Err(TopologyLifecycleError::InvalidOperation { .. })
        ));
    }

    #[test]
    fn lifecycle_journal_status_direction_and_terminal_matrix_is_coherent() {
        let ready = operational_environment(EnvironmentState::Ready);
        let mut operation = EnvironmentLifecycleOperation::plan(
            &ready,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Up,
            "req-coherence",
            "idem-coherence",
            "sha256:coherence",
            550,
        )
        .unwrap();
        operation.status = EnvironmentLifecycleStatus::Succeeded;
        operation.completed_at = Some(551);
        assert!(operation.validate_structure().is_err());

        operation.status = EnvironmentLifecycleStatus::Blocked;
        operation.completed_at = None;
        assert!(operation.validate_structure().is_err());

        let mut attached = EnvironmentLifecycleOperation::plan(
            &ready,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Stop,
            "req-direction",
            "idem-direction",
            "sha256:direction",
            552,
        )
        .unwrap();
        let mut environment = ready;
        attached.begin(&mut environment, 553).unwrap();
        environment.state = EnvironmentState::Deleting;
        assert!(attached.validate_against_environment(&environment).is_err());

        let mut delete = EnvironmentLifecycleOperation::plan(
            &operational_environment(EnvironmentState::Ready),
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Delete,
            "req-delete-coherence",
            "idem-delete-coherence",
            "sha256:delete-coherence",
            554,
        )
        .unwrap();
        delete.status = EnvironmentLifecycleStatus::Superseded;
        delete.completed_at = Some(555);
        assert!(delete.validate_structure().is_err());
    }

    #[test]
    fn lifecycle_operation_tombstone_and_error_json_round_trip_losslessly() {
        let environment = operational_environment(EnvironmentState::Ready);
        let operation = EnvironmentLifecycleOperation::plan(
            &environment,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Delete,
            "req-json",
            "idem-json",
            "sha256:json",
            400,
        )
        .unwrap();
        let decoded: EnvironmentLifecycleOperation =
            serde_json::from_str(&serde_json::to_string(&operation).unwrap()).unwrap();
        assert_eq!(decoded, operation);

        let mut superseded = EnvironmentLifecycleOperation::plan(
            &environment,
            LifecycleOperationId::generate(),
            EnvironmentLifecycleKind::Stop,
            "req-superseded-json",
            "idem-superseded-json",
            "sha256:superseded-json",
            400,
        )
        .unwrap();
        superseded.status = EnvironmentLifecycleStatus::Superseded;
        assert!(superseded.validate_structure().is_err());
        superseded.completed_at = Some(superseded.updated_at);
        superseded.validate_structure().unwrap();

        let tombstone = EnvironmentTombstone {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            name: environment.name.clone(),
            definition_digest: environment.definition_digest.clone(),
            delete_operation_id: operation.operation_id.clone(),
            lifecycle_generation: operation.generation,
            ownership_digest: "sha256:ownership".to_string(),
            deleted_at: 401,
        };
        let decoded: EnvironmentTombstone =
            serde_json::from_str(&serde_json::to_string(&tombstone).unwrap()).unwrap();
        assert_eq!(decoded, tombstone);

        let error = TopologyLifecycleError::GenerationMismatch {
            operation_id: operation.operation_id.to_string(),
            expected: 2,
            found: 1,
        };
        let decoded: TopologyLifecycleError =
            serde_json::from_str(&serde_json::to_string(&error).unwrap()).unwrap();
        assert_eq!(decoded, error);
    }
}
