#![forbid(unsafe_code)]

//! Deterministic translation between Runtime V2 proto messages and runtime-domain types.
//!
//! Semantics:
//! - Unknown enum/state/capability strings are rejected with [`TranslationError`].
//! - Proto fields that encode optional values as empty strings/zero numbers are normalized
//!   back into `Option` domain fields.
//! - Event payloads are encoded/decoded through a stable JSON envelope in
//!   `runtime_v2::RuntimeEvent.event_json`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use vz_runtime_contract::{
    Architecture, Build, BuildSpec, BuildState, CapabilitySet, Checkpoint, CheckpointClass,
    CheckpointState, Container, ContainerSpec, ContainerState, EndpointId, EndpointInstance,
    EndpointProtocol, EndpointSpec, EnvironmentId, EnvironmentInstance, EnvironmentSpec,
    EnvironmentState, Event, EventScope, Execution, ExecutionSpec, ExecutionState, HostSpec, Lease,
    LeaseState, LegacyMigrationProvenance, MachineBackend, MachineCapability, MachineError,
    MachineErrorCode, MachineId, MachineIncarnation, MachineIncarnationId, MachineInstance,
    MachineProfile, MachineResources, MachineSpec, MachineState, NetworkId, NetworkInstance,
    NetworkKind, NetworkSpec, OperatingSystem, OwnedResourceKind, OwnershipRecord,
    ProjectDefinition, ProjectId, ProjectState, RequestMetadata, RuntimeCapabilities,
    SANDBOX_LABEL_BASE_IMAGE_REF, SANDBOX_LABEL_MAIN_CONTAINER, Sandbox, SandboxBackend,
    SandboxSpec, SandboxState, TargetSpec, TopologyCandidate, TopologyResolutionError,
    TopologyValidationError, WorkspaceBinding, WorkspaceBindingId, WorkspaceProjection,
    WorkspaceProjectionMode,
};
use vz_runtime_proto::runtime_v2;

/// Conversion failures between Runtime V2 wire messages and domain entities.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TranslationError {
    #[error("invalid enum value for `{field}`: `{value}`")]
    InvalidEnumValue { field: &'static str, value: String },
    #[error("invalid value for `{field}`: `{value}`")]
    InvalidValue { field: &'static str, value: String },
    #[error("duplicate capability entry: `{name}`")]
    DuplicateCapability { name: String },
    #[error("unknown capability entry: `{name}`")]
    UnknownCapability { name: String },
    #[error("invalid json for `{field}`: {details}")]
    InvalidJson {
        field: &'static str,
        details: String,
    },
    #[error("missing required message field `{field}`")]
    MissingRequiredField { field: &'static str },
    #[error("decoded topology violates its canonical contract: {0}")]
    InvalidTopology(#[from] TopologyValidationError),
}

/// Convert a complete project topology aggregate to its lossless wire form.
pub fn project_state_to_proto(state: &ProjectState) -> runtime_v2::ProjectState {
    runtime_v2::ProjectState {
        schema_version: state.schema_version,
        definition: Some(project_definition_to_proto(&state.definition)),
        environments: state
            .environments
            .iter()
            .map(environment_instance_to_proto)
            .collect(),
    }
}

/// Decode and canonically validate a complete project topology aggregate.
pub fn project_state_from_proto(
    state: &runtime_v2::ProjectState,
) -> Result<ProjectState, TranslationError> {
    let decoded = ProjectState {
        schema_version: state.schema_version,
        definition: project_definition_from_proto(required(
            state.definition.as_ref(),
            "project_state.definition",
        )?)?,
        environments: state
            .environments
            .iter()
            .map(environment_instance_from_proto)
            .collect::<Result<_, _>>()?,
    };
    decoded.validate()?;
    Ok(decoded)
}

/// Convert a project definition to its wire representation.
pub fn project_definition_to_proto(
    definition: &ProjectDefinition,
) -> runtime_v2::ProjectDefinition {
    runtime_v2::ProjectDefinition {
        schema_version: definition.schema_version,
        project_id: definition.project_id.to_string(),
        name: definition.name.clone(),
        environment: Some(environment_spec_to_proto(&definition.environment)),
    }
}

/// Decode a project definition, including its validated project identifier.
pub fn project_definition_from_proto(
    definition: &runtime_v2::ProjectDefinition,
) -> Result<ProjectDefinition, TranslationError> {
    Ok(ProjectDefinition {
        schema_version: definition.schema_version,
        project_id: ProjectId::new(definition.project_id.clone())?,
        name: definition.name.clone(),
        environment: environment_spec_from_proto(required(
            definition.environment.as_ref(),
            "project_definition.environment",
        )?)?,
    })
}

/// Convert an Environment desired topology to wire form.
pub fn environment_spec_to_proto(spec: &EnvironmentSpec) -> runtime_v2::EnvironmentSpec {
    runtime_v2::EnvironmentSpec {
        schema_version: spec.schema_version,
        machines: spec.machines.iter().map(machine_spec_to_proto).collect(),
        networks: spec.networks.iter().map(network_spec_to_proto).collect(),
        endpoints: spec.endpoints.iter().map(endpoint_spec_to_proto).collect(),
    }
}

/// Decode an Environment desired topology.
pub fn environment_spec_from_proto(
    spec: &runtime_v2::EnvironmentSpec,
) -> Result<EnvironmentSpec, TranslationError> {
    Ok(EnvironmentSpec {
        schema_version: spec.schema_version,
        machines: spec
            .machines
            .iter()
            .map(machine_spec_from_proto)
            .collect::<Result<_, _>>()?,
        networks: spec
            .networks
            .iter()
            .map(network_spec_from_proto)
            .collect::<Result<_, _>>()?,
        endpoints: spec
            .endpoints
            .iter()
            .map(endpoint_spec_from_proto)
            .collect::<Result<_, _>>()?,
    })
}

/// Convert one desired Machine to wire form.
pub fn machine_spec_to_proto(spec: &MachineSpec) -> runtime_v2::MachineSpec {
    runtime_v2::MachineSpec {
        schema_version: spec.schema_version,
        name: spec.name.clone(),
        profile: machine_profile_to_proto(spec.profile) as i32,
        target: Some(target_spec_to_proto(&spec.target)),
        resources: Some(machine_resources_to_proto(&spec.resources)),
        requested_capabilities: Some(capability_set_to_proto(&spec.requested_capabilities)),
        workspace: spec.workspace.as_ref().map(workspace_projection_to_proto),
    }
}

/// Decode one desired Machine and all required nested records.
pub fn machine_spec_from_proto(
    spec: &runtime_v2::MachineSpec,
) -> Result<MachineSpec, TranslationError> {
    Ok(MachineSpec {
        schema_version: spec.schema_version,
        name: spec.name.clone(),
        profile: machine_profile_from_proto(spec.profile, "machine_spec.profile")?,
        target: target_spec_from_proto(required(spec.target.as_ref(), "machine_spec.target")?)?,
        resources: machine_resources_from_proto(required(
            spec.resources.as_ref(),
            "machine_spec.resources",
        )?)?,
        requested_capabilities: capability_set_from_proto(required(
            spec.requested_capabilities.as_ref(),
            "machine_spec.requested_capabilities",
        )?)?,
        workspace: spec
            .workspace
            .as_ref()
            .map(workspace_projection_from_proto)
            .transpose()?,
    })
}

/// Convert a host tuple to wire form.
pub fn host_spec_to_proto(host: HostSpec) -> runtime_v2::HostSpec {
    runtime_v2::HostSpec {
        os: operating_system_to_proto(host.os) as i32,
        arch: architecture_to_proto(host.arch) as i32,
    }
}

/// Decode a host tuple, rejecting unknown and unspecified values.
pub fn host_spec_from_proto(host: &runtime_v2::HostSpec) -> Result<HostSpec, TranslationError> {
    Ok(HostSpec {
        os: operating_system_from_proto(host.os, "host_spec.os")?,
        arch: architecture_from_proto(host.arch, "host_spec.arch")?,
    })
}

/// Convert a Machine target to wire form.
pub fn target_spec_to_proto(target: &TargetSpec) -> runtime_v2::TargetSpec {
    runtime_v2::TargetSpec {
        os: operating_system_to_proto(target.os) as i32,
        arch: architecture_to_proto(target.arch) as i32,
        image: target.image.clone(),
        version: target.version.clone(),
        channel: target.channel.clone(),
        digest: target.digest.clone(),
    }
}

/// Decode a Machine target, rejecting unknown and unspecified platform values.
pub fn target_spec_from_proto(
    target: &runtime_v2::TargetSpec,
) -> Result<TargetSpec, TranslationError> {
    Ok(TargetSpec {
        os: operating_system_from_proto(target.os, "target_spec.os")?,
        arch: architecture_from_proto(target.arch, "target_spec.arch")?,
        image: target.image.clone(),
        version: target.version.clone(),
        channel: target.channel.clone(),
        digest: target.digest.clone(),
    })
}

/// Convert a deterministically ordered capability set to wire form.
pub fn capability_set_to_proto(set: &CapabilitySet) -> runtime_v2::CapabilitySet {
    runtime_v2::CapabilitySet {
        capabilities: set
            .capabilities
            .iter()
            .copied()
            .map(|capability| machine_capability_to_proto(capability) as i32)
            .collect(),
        unsupported: set
            .unsupported
            .iter()
            .map(
                |(capability, reason)| runtime_v2::UnsupportedMachineCapability {
                    capability: machine_capability_to_proto(*capability) as i32,
                    reason: reason.clone(),
                },
            )
            .collect(),
    }
}

/// Decode a capability set without losing unsupported-capability reasons.
pub fn capability_set_from_proto(
    set: &runtime_v2::CapabilitySet,
) -> Result<CapabilitySet, TranslationError> {
    let mut capabilities = BTreeSet::new();
    for raw in &set.capabilities {
        let capability = machine_capability_from_proto(*raw, "capability_set.capabilities")?;
        if !capabilities.insert(capability) {
            return Err(TranslationError::DuplicateCapability {
                name: machine_capability_name(capability).to_string(),
            });
        }
    }

    let mut unsupported = BTreeMap::new();
    for entry in &set.unsupported {
        let capability = machine_capability_from_proto(
            entry.capability,
            "capability_set.unsupported.capability",
        )?;
        if unsupported
            .insert(capability, entry.reason.clone())
            .is_some()
        {
            return Err(TranslationError::DuplicateCapability {
                name: machine_capability_name(capability).to_string(),
            });
        }
    }

    Ok(CapabilitySet {
        capabilities,
        unsupported,
    })
}

/// Convert optional Machine resources to their exact wire representation.
pub fn machine_resources_to_proto(resources: &MachineResources) -> runtime_v2::MachineResources {
    runtime_v2::MachineResources {
        cpus: resources.cpus.map(u32::from),
        memory_mb: resources.memory_mb,
        disk_bytes: resources.disk_bytes,
    }
}

/// Decode Machine resources, rejecting CPU values outside the domain width.
pub fn machine_resources_from_proto(
    resources: &runtime_v2::MachineResources,
) -> Result<MachineResources, TranslationError> {
    Ok(MachineResources {
        cpus: resources
            .cpus
            .map(|cpus| {
                u8::try_from(cpus).map_err(|_| TranslationError::InvalidValue {
                    field: "machine_resources.cpus",
                    value: cpus.to_string(),
                })
            })
            .transpose()?,
        memory_mb: resources.memory_mb,
        disk_bytes: resources.disk_bytes,
    })
}

/// Convert a workspace projection to wire form.
pub fn workspace_projection_to_proto(
    projection: &WorkspaceProjection,
) -> runtime_v2::WorkspaceProjection {
    runtime_v2::WorkspaceProjection {
        binding: projection.binding.clone(),
        target_path: projection.target_path.clone(),
        mode: workspace_projection_mode_to_proto(projection.mode) as i32,
    }
}

/// Decode a workspace projection.
pub fn workspace_projection_from_proto(
    projection: &runtime_v2::WorkspaceProjection,
) -> Result<WorkspaceProjection, TranslationError> {
    Ok(WorkspaceProjection {
        binding: projection.binding.clone(),
        target_path: projection.target_path.clone(),
        mode: workspace_projection_mode_from_proto(projection.mode)?,
    })
}

/// Convert a desired network to wire form.
pub fn network_spec_to_proto(spec: &NetworkSpec) -> runtime_v2::NetworkSpec {
    runtime_v2::NetworkSpec {
        schema_version: spec.schema_version,
        name: spec.name.clone(),
        kind: network_kind_to_proto(spec.kind) as i32,
        cidr: spec.cidr.clone(),
    }
}

/// Decode a desired network.
pub fn network_spec_from_proto(
    spec: &runtime_v2::NetworkSpec,
) -> Result<NetworkSpec, TranslationError> {
    Ok(NetworkSpec {
        schema_version: spec.schema_version,
        name: spec.name.clone(),
        kind: network_kind_from_proto(spec.kind)?,
        cidr: spec.cidr.clone(),
    })
}

/// Convert a desired endpoint to wire form.
pub fn endpoint_spec_to_proto(spec: &EndpointSpec) -> runtime_v2::EndpointSpec {
    runtime_v2::EndpointSpec {
        schema_version: spec.schema_version,
        name: spec.name.clone(),
        machine: spec.machine.clone(),
        network: spec.network.clone(),
        protocol: endpoint_protocol_to_proto(spec.protocol) as i32,
        port: u32::from(spec.port),
        hostname: spec.hostname.clone(),
    }
}

/// Decode a desired endpoint, rejecting ports outside the domain width.
pub fn endpoint_spec_from_proto(
    spec: &runtime_v2::EndpointSpec,
) -> Result<EndpointSpec, TranslationError> {
    Ok(EndpointSpec {
        schema_version: spec.schema_version,
        name: spec.name.clone(),
        machine: spec.machine.clone(),
        network: spec.network.clone(),
        protocol: endpoint_protocol_from_proto(spec.protocol)?,
        port: u16::try_from(spec.port).map_err(|_| TranslationError::InvalidValue {
            field: "endpoint_spec.port",
            value: spec.port.to_string(),
        })?,
        hostname: spec.hostname.clone(),
    })
}

/// Convert a workspace binding to wire form.
pub fn workspace_binding_to_proto(binding: &WorkspaceBinding) -> runtime_v2::WorkspaceBinding {
    runtime_v2::WorkspaceBinding {
        schema_version: binding.schema_version,
        binding_id: binding.binding_id.to_string(),
        project_id: binding.project_id.to_string(),
        environment_id: binding.environment_id.to_string(),
        name: binding.name.clone(),
        workspace_key: binding.workspace_key.clone(),
        path_hint: binding.path_hint.clone(),
    }
}

/// Decode a workspace binding and validate every typed identifier.
pub fn workspace_binding_from_proto(
    binding: &runtime_v2::WorkspaceBinding,
) -> Result<WorkspaceBinding, TranslationError> {
    Ok(WorkspaceBinding {
        schema_version: binding.schema_version,
        binding_id: WorkspaceBindingId::new(binding.binding_id.clone())?,
        project_id: ProjectId::new(binding.project_id.clone())?,
        environment_id: EnvironmentId::new(binding.environment_id.clone())?,
        name: binding.name.clone(),
        workspace_key: binding.workspace_key.clone(),
        path_hint: binding.path_hint.clone(),
    })
}

/// Convert a Machine incarnation to wire form.
pub fn machine_incarnation_to_proto(
    incarnation: &MachineIncarnation,
) -> runtime_v2::MachineIncarnation {
    runtime_v2::MachineIncarnation {
        schema_version: incarnation.schema_version,
        incarnation_id: incarnation.incarnation_id.to_string(),
        machine_id: incarnation.machine_id.to_string(),
        generation: incarnation.generation,
        created_at: incarnation.created_at,
    }
}

/// Decode a Machine incarnation and validate its typed identifiers.
pub fn machine_incarnation_from_proto(
    incarnation: &runtime_v2::MachineIncarnation,
) -> Result<MachineIncarnation, TranslationError> {
    Ok(MachineIncarnation {
        schema_version: incarnation.schema_version,
        incarnation_id: MachineIncarnationId::new(incarnation.incarnation_id.clone())?,
        machine_id: MachineId::new(incarnation.machine_id.clone())?,
        generation: incarnation.generation,
        created_at: incarnation.created_at,
    })
}

/// Convert a persisted Machine to wire form.
pub fn machine_instance_to_proto(machine: &MachineInstance) -> runtime_v2::MachineInstance {
    let (backend, other_backend) = machine_backend_to_proto(machine.backend.as_ref());
    runtime_v2::MachineInstance {
        schema_version: machine.schema_version,
        machine_id: machine.machine_id.to_string(),
        environment_id: machine.environment_id.to_string(),
        name: machine.name.clone(),
        profile: machine_profile_to_proto(machine.profile) as i32,
        target: Some(target_spec_to_proto(&machine.target)),
        resources: Some(machine_resources_to_proto(&machine.resources)),
        requested_capabilities: Some(capability_set_to_proto(&machine.requested_capabilities)),
        negotiated_capabilities: Some(capability_set_to_proto(&machine.negotiated_capabilities)),
        backend,
        other_backend,
        incarnation: machine
            .incarnation
            .as_ref()
            .map(machine_incarnation_to_proto),
        state: machine_state_to_proto(machine.state) as i32,
        legacy_sandbox_id: machine.legacy_sandbox_id.clone(),
    }
}

/// Decode a persisted Machine and reject malformed companion fields.
pub fn machine_instance_from_proto(
    machine: &runtime_v2::MachineInstance,
) -> Result<MachineInstance, TranslationError> {
    Ok(MachineInstance {
        schema_version: machine.schema_version,
        machine_id: MachineId::new(machine.machine_id.clone())?,
        environment_id: EnvironmentId::new(machine.environment_id.clone())?,
        name: machine.name.clone(),
        profile: machine_profile_from_proto(machine.profile, "machine_instance.profile")?,
        target: target_spec_from_proto(required(
            machine.target.as_ref(),
            "machine_instance.target",
        )?)?,
        resources: machine_resources_from_proto(required(
            machine.resources.as_ref(),
            "machine_instance.resources",
        )?)?,
        requested_capabilities: capability_set_from_proto(required(
            machine.requested_capabilities.as_ref(),
            "machine_instance.requested_capabilities",
        )?)?,
        negotiated_capabilities: capability_set_from_proto(required(
            machine.negotiated_capabilities.as_ref(),
            "machine_instance.negotiated_capabilities",
        )?)?,
        backend: machine_backend_from_proto(machine.backend, machine.other_backend.as_deref())?,
        incarnation: machine
            .incarnation
            .as_ref()
            .map(machine_incarnation_from_proto)
            .transpose()?,
        state: machine_state_from_proto(machine.state)?,
        legacy_sandbox_id: machine.legacy_sandbox_id.clone(),
    })
}

/// Convert a persisted network identity to wire form.
pub fn network_instance_to_proto(network: &NetworkInstance) -> runtime_v2::NetworkInstance {
    runtime_v2::NetworkInstance {
        schema_version: network.schema_version,
        network_id: network.network_id.to_string(),
        environment_id: network.environment_id.to_string(),
        name: network.name.clone(),
    }
}

/// Decode a persisted network identity.
pub fn network_instance_from_proto(
    network: &runtime_v2::NetworkInstance,
) -> Result<NetworkInstance, TranslationError> {
    Ok(NetworkInstance {
        schema_version: network.schema_version,
        network_id: NetworkId::new(network.network_id.clone())?,
        environment_id: EnvironmentId::new(network.environment_id.clone())?,
        name: network.name.clone(),
    })
}

/// Convert a persisted endpoint identity to wire form.
pub fn endpoint_instance_to_proto(endpoint: &EndpointInstance) -> runtime_v2::EndpointInstance {
    runtime_v2::EndpointInstance {
        schema_version: endpoint.schema_version,
        endpoint_id: endpoint.endpoint_id.to_string(),
        environment_id: endpoint.environment_id.to_string(),
        machine_id: endpoint.machine_id.to_string(),
        network_id: endpoint.network_id.to_string(),
        name: endpoint.name.clone(),
    }
}

/// Decode a persisted endpoint identity.
pub fn endpoint_instance_from_proto(
    endpoint: &runtime_v2::EndpointInstance,
) -> Result<EndpointInstance, TranslationError> {
    Ok(EndpointInstance {
        schema_version: endpoint.schema_version,
        endpoint_id: EndpointId::new(endpoint.endpoint_id.clone())?,
        environment_id: EnvironmentId::new(endpoint.environment_id.clone())?,
        machine_id: MachineId::new(endpoint.machine_id.clone())?,
        network_id: NetworkId::new(endpoint.network_id.clone())?,
        name: endpoint.name.clone(),
    })
}

/// Convert one aggregate ownership edge to wire form.
pub fn ownership_record_to_proto(record: &OwnershipRecord) -> runtime_v2::OwnershipRecord {
    let (resource_kind, other_resource_kind) = owned_resource_kind_to_proto(&record.resource_kind);
    runtime_v2::OwnershipRecord {
        schema_version: record.schema_version,
        resource_kind: resource_kind as i32,
        other_resource_kind,
        resource_id: record.resource_id.clone(),
        environment_id: record.environment_id.to_string(),
        machine_id: record.machine_id.as_ref().map(ToString::to_string),
    }
}

/// Decode one aggregate ownership edge and validate companion fields and IDs.
pub fn ownership_record_from_proto(
    record: &runtime_v2::OwnershipRecord,
) -> Result<OwnershipRecord, TranslationError> {
    Ok(OwnershipRecord {
        schema_version: record.schema_version,
        resource_kind: owned_resource_kind_from_proto(
            record.resource_kind,
            record.other_resource_kind.as_deref(),
        )?,
        resource_id: record.resource_id.clone(),
        environment_id: EnvironmentId::new(record.environment_id.clone())?,
        machine_id: record
            .machine_id
            .as_ref()
            .map(|id| MachineId::new(id.clone()))
            .transpose()?,
    })
}

/// Convert legacy migration provenance to wire form.
pub fn legacy_migration_provenance_to_proto(
    provenance: &LegacyMigrationProvenance,
) -> runtime_v2::LegacyMigrationProvenance {
    runtime_v2::LegacyMigrationProvenance {
        source_version: provenance.source_version.clone(),
        legacy_sandbox_id: provenance.legacy_sandbox_id.clone(),
        unresolved_resources: provenance.unresolved_resources.clone(),
    }
}

/// Decode legacy migration provenance.
pub fn legacy_migration_provenance_from_proto(
    provenance: &runtime_v2::LegacyMigrationProvenance,
) -> LegacyMigrationProvenance {
    LegacyMigrationProvenance {
        source_version: provenance.source_version.clone(),
        legacy_sandbox_id: provenance.legacy_sandbox_id.clone(),
        unresolved_resources: provenance.unresolved_resources.clone(),
    }
}

/// Convert a persisted Environment aggregate to wire form.
pub fn environment_instance_to_proto(
    environment: &EnvironmentInstance,
) -> runtime_v2::EnvironmentInstance {
    runtime_v2::EnvironmentInstance {
        schema_version: environment.schema_version,
        environment_id: environment.environment_id.to_string(),
        project_id: environment.project_id.to_string(),
        name: environment.name.clone(),
        definition_digest: environment.definition_digest.clone(),
        state: environment_state_to_proto(environment.state) as i32,
        bindings: environment
            .bindings
            .iter()
            .map(workspace_binding_to_proto)
            .collect(),
        machines: environment
            .machines
            .iter()
            .map(machine_instance_to_proto)
            .collect(),
        networks: environment
            .networks
            .iter()
            .map(network_instance_to_proto)
            .collect(),
        endpoints: environment
            .endpoints
            .iter()
            .map(endpoint_instance_to_proto)
            .collect(),
        ownership: environment
            .ownership
            .iter()
            .map(ownership_record_to_proto)
            .collect(),
        legacy_migration: environment
            .legacy_migration
            .as_ref()
            .map(legacy_migration_provenance_to_proto),
        created_at: environment.created_at,
        updated_at: environment.updated_at,
    }
}

/// Decode a persisted Environment aggregate.
pub fn environment_instance_from_proto(
    environment: &runtime_v2::EnvironmentInstance,
) -> Result<EnvironmentInstance, TranslationError> {
    Ok(EnvironmentInstance {
        schema_version: environment.schema_version,
        environment_id: EnvironmentId::new(environment.environment_id.clone())?,
        project_id: ProjectId::new(environment.project_id.clone())?,
        name: environment.name.clone(),
        definition_digest: environment.definition_digest.clone(),
        state: environment_state_from_proto(environment.state)?,
        bindings: environment
            .bindings
            .iter()
            .map(workspace_binding_from_proto)
            .collect::<Result<_, _>>()?,
        machines: environment
            .machines
            .iter()
            .map(machine_instance_from_proto)
            .collect::<Result<_, _>>()?,
        networks: environment
            .networks
            .iter()
            .map(network_instance_from_proto)
            .collect::<Result<_, _>>()?,
        endpoints: environment
            .endpoints
            .iter()
            .map(endpoint_instance_from_proto)
            .collect::<Result<_, _>>()?,
        ownership: environment
            .ownership
            .iter()
            .map(ownership_record_from_proto)
            .collect::<Result<_, _>>()?,
        legacy_migration: environment
            .legacy_migration
            .as_ref()
            .map(legacy_migration_provenance_from_proto),
        created_at: environment.created_at,
        updated_at: environment.updated_at,
    })
}

/// Convert a structured topology resolution failure to wire detail.
pub fn topology_resolution_error_to_proto(
    error: &TopologyResolutionError,
) -> runtime_v2::TopologyErrorDetail {
    use runtime_v2::topology_error_detail::Detail;

    let detail = match error {
        TopologyResolutionError::NotFound { kind, selector } => {
            Detail::NotFound(runtime_v2::TopologyNotFoundDetail {
                kind: kind.clone(),
                selector: selector.clone(),
            })
        }
        TopologyResolutionError::Ambiguous {
            kind,
            selector,
            candidates,
        } => Detail::Ambiguous(runtime_v2::TopologyAmbiguityDetail {
            kind: kind.clone(),
            selector: selector.clone(),
            candidates: candidates
                .iter()
                .map(|candidate| runtime_v2::TopologyCandidate {
                    id: candidate.id.clone(),
                    name: candidate.name.clone(),
                })
                .collect(),
        }),
        TopologyResolutionError::SelectionRequired {
            kind,
            selector,
            candidates,
        } => Detail::SelectionRequired(runtime_v2::TopologySelectionRequiredDetail {
            kind: kind.clone(),
            selector: selector.clone(),
            candidates: candidates
                .iter()
                .map(|candidate| runtime_v2::TopologyCandidate {
                    id: candidate.id.clone(),
                    name: candidate.name.clone(),
                })
                .collect(),
        }),
        TopologyResolutionError::InvalidSelector {
            kind,
            selector,
            reason,
        } => Detail::InvalidSelector(runtime_v2::TopologyInvalidSelectorDetail {
            kind: kind.clone(),
            selector: selector.clone(),
            reason: reason.clone(),
        }),
    };
    runtime_v2::TopologyErrorDetail {
        detail: Some(detail),
    }
}

/// Decode a structured topology resolution failure.
pub fn topology_resolution_error_from_proto(
    error: &runtime_v2::TopologyErrorDetail,
) -> Result<TopologyResolutionError, TranslationError> {
    use runtime_v2::topology_error_detail::Detail;

    match required(error.detail.as_ref(), "topology_error_detail.detail")? {
        Detail::NotFound(detail) => Ok(TopologyResolutionError::NotFound {
            kind: detail.kind.clone(),
            selector: detail.selector.clone(),
        }),
        Detail::Ambiguous(detail) => Ok(TopologyResolutionError::ambiguous(
            detail.kind.clone(),
            detail.selector.clone(),
            detail.candidates.iter().map(|candidate| TopologyCandidate {
                id: candidate.id.clone(),
                name: candidate.name.clone(),
            }),
        )),
        Detail::SelectionRequired(detail) => Ok(TopologyResolutionError::selection_required(
            detail.kind.clone(),
            detail.selector.clone(),
            detail.candidates.iter().map(|candidate| TopologyCandidate {
                id: candidate.id.clone(),
                name: candidate.name.clone(),
            }),
        )),
        Detail::InvalidSelector(detail) => Ok(TopologyResolutionError::InvalidSelector {
            kind: detail.kind.clone(),
            selector: detail.selector.clone(),
            reason: detail.reason.clone(),
        }),
        Detail::UnsupportedTarget(_)
        | Detail::MissingCapability(_)
        | Detail::InvalidMachineProfile(_)
        | Detail::InvalidCapabilityDeclaration(_)
        | Detail::ContradictoryCapability(_) => Err(TranslationError::InvalidValue {
            field: "topology_error_detail.detail",
            value: "validation_error".to_string(),
        }),
    }
}

/// Convert a topology validation failure when the wire schema can represent it.
pub fn topology_validation_error_to_proto(
    error: &TopologyValidationError,
) -> Option<runtime_v2::TopologyErrorDetail> {
    use runtime_v2::topology_error_detail::Detail;

    let detail = match error {
        TopologyValidationError::UnsupportedTarget {
            host_os,
            host_arch,
            target_os,
            target_arch,
            requested_capabilities,
        } => Detail::UnsupportedTarget(runtime_v2::TopologyUnsupportedTargetDetail {
            host_os: operating_system_to_proto(*host_os) as i32,
            host_arch: architecture_to_proto(*host_arch) as i32,
            target_os: operating_system_to_proto(*target_os) as i32,
            target_arch: architecture_to_proto(*target_arch) as i32,
            requested_capabilities: requested_capabilities
                .iter()
                .copied()
                .map(|capability| machine_capability_to_proto(capability) as i32)
                .collect(),
        }),
        TopologyValidationError::MissingCapability {
            machine_id,
            capability,
        } => Detail::MissingCapability(runtime_v2::TopologyMissingCapabilityDetail {
            machine_id: machine_id.clone(),
            capability: machine_capability_to_proto(*capability) as i32,
        }),
        TopologyValidationError::InvalidMachineProfile {
            machine_id,
            profile,
            reason,
        } => Detail::InvalidMachineProfile(runtime_v2::TopologyInvalidMachineProfileDetail {
            machine_id: machine_id.clone(),
            profile: machine_profile_to_proto(*profile) as i32,
            reason: reason.clone(),
        }),
        TopologyValidationError::InvalidCapabilityDeclaration { machine_id, reason } => {
            Detail::InvalidCapabilityDeclaration(
                runtime_v2::TopologyInvalidCapabilityDeclarationDetail {
                    machine_id: machine_id.clone(),
                    reason: reason.clone(),
                },
            )
        }
        TopologyValidationError::ContradictoryCapability {
            machine_id,
            capability,
        } => Detail::ContradictoryCapability(runtime_v2::TopologyContradictoryCapabilityDetail {
            machine_id: machine_id.clone(),
            capability: machine_capability_to_proto(*capability) as i32,
        }),
        _ => return None,
    };
    Some(runtime_v2::TopologyErrorDetail {
        detail: Some(detail),
    })
}

/// Decode a representable topology validation failure.
pub fn topology_validation_error_from_proto(
    error: &runtime_v2::TopologyErrorDetail,
) -> Result<TopologyValidationError, TranslationError> {
    use runtime_v2::topology_error_detail::Detail;

    match required(error.detail.as_ref(), "topology_error_detail.detail")? {
        Detail::UnsupportedTarget(detail) => {
            let requested_capabilities = decode_capability_list(
                &detail.requested_capabilities,
                "topology_unsupported_target.requested_capabilities",
            )?;
            Ok(TopologyValidationError::UnsupportedTarget {
                host_os: operating_system_from_proto(
                    detail.host_os,
                    "topology_unsupported_target.host_os",
                )?,
                host_arch: architecture_from_proto(
                    detail.host_arch,
                    "topology_unsupported_target.host_arch",
                )?,
                target_os: operating_system_from_proto(
                    detail.target_os,
                    "topology_unsupported_target.target_os",
                )?,
                target_arch: architecture_from_proto(
                    detail.target_arch,
                    "topology_unsupported_target.target_arch",
                )?,
                requested_capabilities,
            })
        }
        Detail::MissingCapability(detail) => {
            let machine_id = MachineId::new(detail.machine_id.clone())?;
            Ok(TopologyValidationError::MissingCapability {
                machine_id: machine_id.to_string(),
                capability: machine_capability_from_proto(
                    detail.capability,
                    "topology_missing_capability.capability",
                )?,
            })
        }
        Detail::InvalidMachineProfile(detail) => {
            Ok(TopologyValidationError::InvalidMachineProfile {
                machine_id: detail.machine_id.clone(),
                profile: machine_profile_from_proto(
                    detail.profile,
                    "topology_invalid_machine_profile.profile",
                )?,
                reason: detail.reason.clone(),
            })
        }
        Detail::InvalidCapabilityDeclaration(detail) => {
            Ok(TopologyValidationError::InvalidCapabilityDeclaration {
                machine_id: detail.machine_id.clone(),
                reason: detail.reason.clone(),
            })
        }
        Detail::ContradictoryCapability(detail) => {
            Ok(TopologyValidationError::ContradictoryCapability {
                machine_id: detail.machine_id.clone(),
                capability: machine_capability_from_proto(
                    detail.capability,
                    "topology_contradictory_capability.capability",
                )?,
            })
        }
        Detail::NotFound(_)
        | Detail::Ambiguous(_)
        | Detail::SelectionRequired(_)
        | Detail::InvalidSelector(_) => Err(TranslationError::InvalidValue {
            field: "topology_error_detail.detail",
            value: "resolution_error".to_string(),
        }),
    }
}

fn required<'a, T>(value: Option<&'a T>, field: &'static str) -> Result<&'a T, TranslationError> {
    value.ok_or(TranslationError::MissingRequiredField { field })
}

fn invalid_enum(field: &'static str, value: i32) -> TranslationError {
    TranslationError::InvalidEnumValue {
        field,
        value: value.to_string(),
    }
}

fn operating_system_to_proto(value: OperatingSystem) -> runtime_v2::OperatingSystem {
    match value {
        OperatingSystem::Linux => runtime_v2::OperatingSystem::Linux,
        OperatingSystem::Macos => runtime_v2::OperatingSystem::Macos,
        OperatingSystem::Windows => runtime_v2::OperatingSystem::Windows,
    }
}

fn operating_system_from_proto(
    raw: i32,
    field: &'static str,
) -> Result<OperatingSystem, TranslationError> {
    match runtime_v2::OperatingSystem::try_from(raw).map_err(|_| invalid_enum(field, raw))? {
        runtime_v2::OperatingSystem::Linux => Ok(OperatingSystem::Linux),
        runtime_v2::OperatingSystem::Macos => Ok(OperatingSystem::Macos),
        runtime_v2::OperatingSystem::Windows => Ok(OperatingSystem::Windows),
        runtime_v2::OperatingSystem::Unspecified => Err(invalid_enum(field, raw)),
    }
}

fn architecture_to_proto(value: Architecture) -> runtime_v2::Architecture {
    match value {
        Architecture::Aarch64 => runtime_v2::Architecture::Aarch64,
        Architecture::X86_64 => runtime_v2::Architecture::X8664,
    }
}

fn architecture_from_proto(
    raw: i32,
    field: &'static str,
) -> Result<Architecture, TranslationError> {
    match runtime_v2::Architecture::try_from(raw).map_err(|_| invalid_enum(field, raw))? {
        runtime_v2::Architecture::Aarch64 => Ok(Architecture::Aarch64),
        runtime_v2::Architecture::X8664 => Ok(Architecture::X86_64),
        runtime_v2::Architecture::Unspecified => Err(invalid_enum(field, raw)),
    }
}

fn machine_capability_to_proto(value: MachineCapability) -> runtime_v2::MachineCapability {
    match value {
        MachineCapability::PosixExec => runtime_v2::MachineCapability::PosixExec,
        MachineCapability::PosixPty => runtime_v2::MachineCapability::PosixPty,
        MachineCapability::Signals => runtime_v2::MachineCapability::Signals,
        MachineCapability::Files => runtime_v2::MachineCapability::Files,
        MachineCapability::Ports => runtime_v2::MachineCapability::Ports,
        MachineCapability::DockerEngine => runtime_v2::MachineCapability::DockerEngine,
        MachineCapability::Compose => runtime_v2::MachineCapability::Compose,
        MachineCapability::Buildx => runtime_v2::MachineCapability::Buildx,
        MachineCapability::Snapshot => runtime_v2::MachineCapability::Snapshot,
        MachineCapability::Suspend => runtime_v2::MachineCapability::Suspend,
        MachineCapability::Checkpoint => runtime_v2::MachineCapability::Checkpoint,
        MachineCapability::Gui => runtime_v2::MachineCapability::Gui,
        MachineCapability::WindowsConsole => runtime_v2::MachineCapability::WindowsConsole,
    }
}

fn machine_capability_from_proto(
    raw: i32,
    field: &'static str,
) -> Result<MachineCapability, TranslationError> {
    match runtime_v2::MachineCapability::try_from(raw).map_err(|_| invalid_enum(field, raw))? {
        runtime_v2::MachineCapability::PosixExec => Ok(MachineCapability::PosixExec),
        runtime_v2::MachineCapability::PosixPty => Ok(MachineCapability::PosixPty),
        runtime_v2::MachineCapability::Signals => Ok(MachineCapability::Signals),
        runtime_v2::MachineCapability::Files => Ok(MachineCapability::Files),
        runtime_v2::MachineCapability::Ports => Ok(MachineCapability::Ports),
        runtime_v2::MachineCapability::DockerEngine => Ok(MachineCapability::DockerEngine),
        runtime_v2::MachineCapability::Compose => Ok(MachineCapability::Compose),
        runtime_v2::MachineCapability::Buildx => Ok(MachineCapability::Buildx),
        runtime_v2::MachineCapability::Snapshot => Ok(MachineCapability::Snapshot),
        runtime_v2::MachineCapability::Suspend => Ok(MachineCapability::Suspend),
        runtime_v2::MachineCapability::Checkpoint => Ok(MachineCapability::Checkpoint),
        runtime_v2::MachineCapability::Gui => Ok(MachineCapability::Gui),
        runtime_v2::MachineCapability::WindowsConsole => Ok(MachineCapability::WindowsConsole),
        runtime_v2::MachineCapability::Unspecified => Err(invalid_enum(field, raw)),
    }
}

fn machine_capability_name(value: MachineCapability) -> &'static str {
    match value {
        MachineCapability::PosixExec => "posix_exec",
        MachineCapability::PosixPty => "posix_pty",
        MachineCapability::Signals => "signals",
        MachineCapability::Files => "files",
        MachineCapability::Ports => "ports",
        MachineCapability::DockerEngine => "docker_engine",
        MachineCapability::Compose => "compose",
        MachineCapability::Buildx => "buildx",
        MachineCapability::Snapshot => "snapshot",
        MachineCapability::Suspend => "suspend",
        MachineCapability::Checkpoint => "checkpoint",
        MachineCapability::Gui => "gui",
        MachineCapability::WindowsConsole => "windows_console",
    }
}

fn decode_capability_list(
    raw: &[i32],
    field: &'static str,
) -> Result<Vec<MachineCapability>, TranslationError> {
    let mut seen = BTreeSet::new();
    let mut decoded = Vec::with_capacity(raw.len());
    for value in raw {
        let capability = machine_capability_from_proto(*value, field)?;
        if !seen.insert(capability) {
            return Err(TranslationError::DuplicateCapability {
                name: machine_capability_name(capability).to_string(),
            });
        }
        decoded.push(capability);
    }
    Ok(decoded)
}

fn workspace_projection_mode_to_proto(
    value: WorkspaceProjectionMode,
) -> runtime_v2::WorkspaceProjectionMode {
    match value {
        WorkspaceProjectionMode::ReadWrite => runtime_v2::WorkspaceProjectionMode::ReadWrite,
        WorkspaceProjectionMode::ReadOnly => runtime_v2::WorkspaceProjectionMode::ReadOnly,
        WorkspaceProjectionMode::Snapshot => runtime_v2::WorkspaceProjectionMode::Snapshot,
    }
}

fn workspace_projection_mode_from_proto(
    raw: i32,
) -> Result<WorkspaceProjectionMode, TranslationError> {
    let field = "workspace_projection.mode";
    match runtime_v2::WorkspaceProjectionMode::try_from(raw)
        .map_err(|_| invalid_enum(field, raw))?
    {
        runtime_v2::WorkspaceProjectionMode::ReadWrite => Ok(WorkspaceProjectionMode::ReadWrite),
        runtime_v2::WorkspaceProjectionMode::ReadOnly => Ok(WorkspaceProjectionMode::ReadOnly),
        runtime_v2::WorkspaceProjectionMode::Snapshot => Ok(WorkspaceProjectionMode::Snapshot),
        runtime_v2::WorkspaceProjectionMode::Unspecified => Err(invalid_enum(field, raw)),
    }
}

fn network_kind_to_proto(value: NetworkKind) -> runtime_v2::NetworkKind {
    match value {
        NetworkKind::Private => runtime_v2::NetworkKind::Private,
        NetworkKind::SimulatedPublic => runtime_v2::NetworkKind::SimulatedPublic,
    }
}

fn network_kind_from_proto(raw: i32) -> Result<NetworkKind, TranslationError> {
    let field = "network_spec.kind";
    match runtime_v2::NetworkKind::try_from(raw).map_err(|_| invalid_enum(field, raw))? {
        runtime_v2::NetworkKind::Private => Ok(NetworkKind::Private),
        runtime_v2::NetworkKind::SimulatedPublic => Ok(NetworkKind::SimulatedPublic),
        runtime_v2::NetworkKind::Unspecified => Err(invalid_enum(field, raw)),
    }
}

fn endpoint_protocol_to_proto(value: EndpointProtocol) -> runtime_v2::EndpointProtocol {
    match value {
        EndpointProtocol::Tcp => runtime_v2::EndpointProtocol::Tcp,
        EndpointProtocol::Udp => runtime_v2::EndpointProtocol::Udp,
        EndpointProtocol::Http => runtime_v2::EndpointProtocol::Http,
        EndpointProtocol::Https => runtime_v2::EndpointProtocol::Https,
    }
}

fn endpoint_protocol_from_proto(raw: i32) -> Result<EndpointProtocol, TranslationError> {
    let field = "endpoint_spec.protocol";
    match runtime_v2::EndpointProtocol::try_from(raw).map_err(|_| invalid_enum(field, raw))? {
        runtime_v2::EndpointProtocol::Tcp => Ok(EndpointProtocol::Tcp),
        runtime_v2::EndpointProtocol::Udp => Ok(EndpointProtocol::Udp),
        runtime_v2::EndpointProtocol::Http => Ok(EndpointProtocol::Http),
        runtime_v2::EndpointProtocol::Https => Ok(EndpointProtocol::Https),
        runtime_v2::EndpointProtocol::Unspecified => Err(invalid_enum(field, raw)),
    }
}

fn environment_state_to_proto(value: EnvironmentState) -> runtime_v2::EnvironmentState {
    match value {
        EnvironmentState::Creating => runtime_v2::EnvironmentState::Creating,
        EnvironmentState::Reconciling => runtime_v2::EnvironmentState::Reconciling,
        EnvironmentState::Ready => runtime_v2::EnvironmentState::Ready,
        EnvironmentState::Stopped => runtime_v2::EnvironmentState::Stopped,
        EnvironmentState::Deleting => runtime_v2::EnvironmentState::Deleting,
        EnvironmentState::Deleted => runtime_v2::EnvironmentState::Deleted,
        EnvironmentState::Failed => runtime_v2::EnvironmentState::Failed,
    }
}

fn environment_state_from_proto(raw: i32) -> Result<EnvironmentState, TranslationError> {
    let field = "environment_instance.state";
    match runtime_v2::EnvironmentState::try_from(raw).map_err(|_| invalid_enum(field, raw))? {
        runtime_v2::EnvironmentState::Creating => Ok(EnvironmentState::Creating),
        runtime_v2::EnvironmentState::Reconciling => Ok(EnvironmentState::Reconciling),
        runtime_v2::EnvironmentState::Ready => Ok(EnvironmentState::Ready),
        runtime_v2::EnvironmentState::Stopped => Ok(EnvironmentState::Stopped),
        runtime_v2::EnvironmentState::Deleting => Ok(EnvironmentState::Deleting),
        runtime_v2::EnvironmentState::Deleted => Ok(EnvironmentState::Deleted),
        runtime_v2::EnvironmentState::Failed => Ok(EnvironmentState::Failed),
        runtime_v2::EnvironmentState::Unspecified => Err(invalid_enum(field, raw)),
    }
}

fn machine_state_to_proto(value: MachineState) -> runtime_v2::MachineState {
    match value {
        MachineState::Creating => runtime_v2::MachineState::Creating,
        MachineState::Ready => runtime_v2::MachineState::Ready,
        MachineState::Stopped => runtime_v2::MachineState::Stopped,
        MachineState::Failed => runtime_v2::MachineState::Failed,
    }
}

fn machine_state_from_proto(raw: i32) -> Result<MachineState, TranslationError> {
    let field = "machine_instance.state";
    match runtime_v2::MachineState::try_from(raw).map_err(|_| invalid_enum(field, raw))? {
        runtime_v2::MachineState::Creating => Ok(MachineState::Creating),
        runtime_v2::MachineState::Ready => Ok(MachineState::Ready),
        runtime_v2::MachineState::Stopped => Ok(MachineState::Stopped),
        runtime_v2::MachineState::Failed => Ok(MachineState::Failed),
        runtime_v2::MachineState::Unspecified => Err(invalid_enum(field, raw)),
    }
}

fn machine_profile_to_proto(value: MachineProfile) -> runtime_v2::MachineProfile {
    match value {
        MachineProfile::Developer => runtime_v2::MachineProfile::Developer,
        MachineProfile::Hardened => runtime_v2::MachineProfile::Hardened,
    }
}

fn machine_profile_from_proto(
    raw: i32,
    field: &'static str,
) -> Result<MachineProfile, TranslationError> {
    match runtime_v2::MachineProfile::try_from(raw).map_err(|_| invalid_enum(field, raw))? {
        runtime_v2::MachineProfile::Developer => Ok(MachineProfile::Developer),
        runtime_v2::MachineProfile::Hardened => Ok(MachineProfile::Hardened),
        runtime_v2::MachineProfile::Unspecified => Err(invalid_enum(field, raw)),
    }
}

fn machine_backend_to_proto(value: Option<&MachineBackend>) -> (Option<i32>, Option<String>) {
    match value {
        None => (None, None),
        Some(MachineBackend::MacosVirtualizationLinux) => (
            Some(runtime_v2::MachineBackend::MacosVirtualizationLinux as i32),
            None,
        ),
        Some(MachineBackend::MacosNative) => {
            (Some(runtime_v2::MachineBackend::MacosNative as i32), None)
        }
        Some(MachineBackend::LinuxNative) => {
            (Some(runtime_v2::MachineBackend::LinuxNative as i32), None)
        }
        Some(MachineBackend::WindowsLinux) => {
            (Some(runtime_v2::MachineBackend::WindowsLinux as i32), None)
        }
        Some(MachineBackend::WindowsNative) => {
            (Some(runtime_v2::MachineBackend::WindowsNative as i32), None)
        }
        Some(MachineBackend::Other(value)) => (
            Some(runtime_v2::MachineBackend::Other as i32),
            Some(value.clone()),
        ),
    }
}

fn machine_backend_from_proto(
    raw: Option<i32>,
    other: Option<&str>,
) -> Result<Option<MachineBackend>, TranslationError> {
    let field = "machine_instance.backend";
    let Some(raw) = raw else {
        if let Some(other) = other {
            return Err(inconsistent_other(field, other));
        }
        return Ok(None);
    };
    match runtime_v2::MachineBackend::try_from(raw).map_err(|_| invalid_enum(field, raw))? {
        runtime_v2::MachineBackend::Unspecified => Err(invalid_enum(field, raw)),
        runtime_v2::MachineBackend::MacosVirtualizationLinux => {
            reject_other(field, other)?;
            Ok(Some(MachineBackend::MacosVirtualizationLinux))
        }
        runtime_v2::MachineBackend::MacosNative => {
            reject_other(field, other)?;
            Ok(Some(MachineBackend::MacosNative))
        }
        runtime_v2::MachineBackend::LinuxNative => {
            reject_other(field, other)?;
            Ok(Some(MachineBackend::LinuxNative))
        }
        runtime_v2::MachineBackend::WindowsLinux => {
            reject_other(field, other)?;
            Ok(Some(MachineBackend::WindowsLinux))
        }
        runtime_v2::MachineBackend::WindowsNative => {
            reject_other(field, other)?;
            Ok(Some(MachineBackend::WindowsNative))
        }
        runtime_v2::MachineBackend::Other => {
            Ok(Some(MachineBackend::Other(require_other(field, other)?)))
        }
    }
}

fn owned_resource_kind_to_proto(
    value: &OwnedResourceKind,
) -> (runtime_v2::OwnedResourceKind, Option<String>) {
    match value {
        OwnedResourceKind::Machine => (runtime_v2::OwnedResourceKind::Machine, None),
        OwnedResourceKind::Incarnation => (runtime_v2::OwnedResourceKind::Incarnation, None),
        OwnedResourceKind::Disk => (runtime_v2::OwnedResourceKind::Disk, None),
        OwnedResourceKind::Socket => (runtime_v2::OwnedResourceKind::Socket, None),
        OwnedResourceKind::DockerContext => (runtime_v2::OwnedResourceKind::DockerContext, None),
        OwnedResourceKind::Network => (runtime_v2::OwnedResourceKind::Network, None),
        OwnedResourceKind::Endpoint => (runtime_v2::OwnedResourceKind::Endpoint, None),
        OwnedResourceKind::Credential => (runtime_v2::OwnedResourceKind::Credential, None),
        OwnedResourceKind::Fault => (runtime_v2::OwnedResourceKind::Fault, None),
        OwnedResourceKind::LegacySandbox => (runtime_v2::OwnedResourceKind::LegacySandbox, None),
        OwnedResourceKind::Other(value) => {
            (runtime_v2::OwnedResourceKind::Other, Some(value.clone()))
        }
    }
}

fn owned_resource_kind_from_proto(
    raw: i32,
    other: Option<&str>,
) -> Result<OwnedResourceKind, TranslationError> {
    let field = "ownership_record.resource_kind";
    match runtime_v2::OwnedResourceKind::try_from(raw).map_err(|_| invalid_enum(field, raw))? {
        runtime_v2::OwnedResourceKind::Unspecified => Err(invalid_enum(field, raw)),
        runtime_v2::OwnedResourceKind::Machine => {
            reject_other(field, other)?;
            Ok(OwnedResourceKind::Machine)
        }
        runtime_v2::OwnedResourceKind::Incarnation => {
            reject_other(field, other)?;
            Ok(OwnedResourceKind::Incarnation)
        }
        runtime_v2::OwnedResourceKind::Disk => {
            reject_other(field, other)?;
            Ok(OwnedResourceKind::Disk)
        }
        runtime_v2::OwnedResourceKind::Socket => {
            reject_other(field, other)?;
            Ok(OwnedResourceKind::Socket)
        }
        runtime_v2::OwnedResourceKind::DockerContext => {
            reject_other(field, other)?;
            Ok(OwnedResourceKind::DockerContext)
        }
        runtime_v2::OwnedResourceKind::Network => {
            reject_other(field, other)?;
            Ok(OwnedResourceKind::Network)
        }
        runtime_v2::OwnedResourceKind::Endpoint => {
            reject_other(field, other)?;
            Ok(OwnedResourceKind::Endpoint)
        }
        runtime_v2::OwnedResourceKind::Credential => {
            reject_other(field, other)?;
            Ok(OwnedResourceKind::Credential)
        }
        runtime_v2::OwnedResourceKind::Fault => {
            reject_other(field, other)?;
            Ok(OwnedResourceKind::Fault)
        }
        runtime_v2::OwnedResourceKind::LegacySandbox => {
            reject_other(field, other)?;
            Ok(OwnedResourceKind::LegacySandbox)
        }
        runtime_v2::OwnedResourceKind::Other => {
            Ok(OwnedResourceKind::Other(require_other(field, other)?))
        }
    }
}

fn reject_other(field: &'static str, other: Option<&str>) -> Result<(), TranslationError> {
    match other {
        None => Ok(()),
        Some(value) => Err(inconsistent_other(field, value)),
    }
}

fn require_other(field: &'static str, other: Option<&str>) -> Result<String, TranslationError> {
    match other.map(str::trim) {
        Some(value) if !value.is_empty() => Ok(value.to_string()),
        Some(value) => Err(inconsistent_other(field, value)),
        None => Err(inconsistent_other(field, "<missing>")),
    }
}

fn inconsistent_other(field: &'static str, value: &str) -> TranslationError {
    TranslationError::InvalidValue {
        field,
        value: format!("inconsistent Other companion `{value}`"),
    }
}

/// Convert domain request metadata into wire metadata.
pub fn request_metadata_to_proto(metadata: &RequestMetadata) -> runtime_v2::RequestMetadata {
    runtime_v2::RequestMetadata {
        request_id: metadata.request_id.clone().unwrap_or_default(),
        idempotency_key: metadata.idempotency_key.clone().unwrap_or_default(),
        trace_id: metadata.trace_id.clone().unwrap_or_default(),
    }
}

/// Convert wire request metadata into domain metadata.
pub fn request_metadata_from_proto(metadata: &runtime_v2::RequestMetadata) -> RequestMetadata {
    RequestMetadata::from_optional_refs(
        normalize_optional_wire_field(&metadata.request_id).as_deref(),
        normalize_optional_wire_field(&metadata.idempotency_key).as_deref(),
    )
    .with_trace_id(normalize_optional_wire_field(&metadata.trace_id))
}

/// Convert a domain machine error into wire error detail.
pub fn machine_error_to_proto_detail(error: &MachineError) -> runtime_v2::ErrorDetail {
    runtime_v2::ErrorDetail {
        code: error.code.as_str().to_string(),
        message: error.message.clone(),
        request_id: error.request_id.clone().unwrap_or_default(),
    }
}

/// Convert a wire error detail into a domain machine error.
pub fn machine_error_from_proto_detail(
    detail: &runtime_v2::ErrorDetail,
) -> Result<MachineError, TranslationError> {
    let code = machine_error_code_from_wire(&detail.code)?;
    Ok(MachineError::new(
        code,
        detail.message.clone(),
        normalize_optional_wire_field(&detail.request_id),
        BTreeMap::new(),
    ))
}

/// Convert domain sandbox into wire payload.
pub fn sandbox_to_proto_payload(sandbox: &Sandbox) -> runtime_v2::SandboxPayload {
    let mut labels = sandbox.labels.clone();
    if let Some(base_image_ref) = sandbox.spec.base_image_ref.as_ref() {
        labels
            .entry(SANDBOX_LABEL_BASE_IMAGE_REF.to_string())
            .or_insert_with(|| base_image_ref.clone());
    }
    if let Some(main_container) = sandbox.spec.main_container.as_ref() {
        labels
            .entry(SANDBOX_LABEL_MAIN_CONTAINER.to_string())
            .or_insert_with(|| main_container.clone());
    }

    runtime_v2::SandboxPayload {
        sandbox_id: sandbox.sandbox_id.clone(),
        backend: sandbox_backend_to_wire(&sandbox.backend).to_string(),
        state: sandbox_state_to_wire(sandbox.state).to_string(),
        cpus: sandbox.spec.cpus.map(u32::from).unwrap_or_default(),
        memory_mb: sandbox.spec.memory_mb.unwrap_or_default(),
        created_at: sandbox.created_at,
        updated_at: sandbox.updated_at,
        labels: btree_to_hash_map(&labels),
    }
}

/// Convert wire sandbox payload into domain sandbox.
pub fn sandbox_from_proto_payload(
    payload: &runtime_v2::SandboxPayload,
) -> Result<Sandbox, TranslationError> {
    let cpus = if payload.cpus == 0 {
        None
    } else {
        Some(
            u8::try_from(payload.cpus).map_err(|_| TranslationError::InvalidValue {
                field: "sandbox.cpus",
                value: payload.cpus.to_string(),
            })?,
        )
    };
    let labels = hash_to_btree_map(&payload.labels);
    let base_image_ref = labels
        .get(SANDBOX_LABEL_BASE_IMAGE_REF)
        .and_then(|value| normalize_optional_wire_field(value));
    let main_container = labels
        .get(SANDBOX_LABEL_MAIN_CONTAINER)
        .and_then(|value| normalize_optional_wire_field(value));

    Ok(Sandbox {
        sandbox_id: payload.sandbox_id.clone(),
        backend: sandbox_backend_from_wire(&payload.backend),
        spec: SandboxSpec {
            cpus,
            memory_mb: none_if_zero(payload.memory_mb),
            base_image_ref,
            main_container,
            network_profile: None,
            volume_mounts: Vec::new(),
        },
        state: sandbox_state_from_wire(&payload.state)?,
        created_at: payload.created_at,
        updated_at: payload.updated_at,
        labels,
    })
}

/// Convert domain lease into wire payload.
pub fn lease_to_proto_payload(lease: &Lease) -> runtime_v2::LeasePayload {
    runtime_v2::LeasePayload {
        lease_id: lease.lease_id.clone(),
        sandbox_id: lease.sandbox_id.clone(),
        ttl_secs: lease.ttl_secs,
        last_heartbeat_at: lease.last_heartbeat_at,
        state: lease_state_to_wire(lease.state).to_string(),
    }
}

/// Convert wire lease payload into domain lease.
pub fn lease_from_proto_payload(
    payload: &runtime_v2::LeasePayload,
) -> Result<Lease, TranslationError> {
    Ok(Lease {
        lease_id: payload.lease_id.clone(),
        sandbox_id: payload.sandbox_id.clone(),
        ttl_secs: payload.ttl_secs,
        last_heartbeat_at: payload.last_heartbeat_at,
        state: lease_state_from_wire(&payload.state)?,
    })
}

/// Convert domain container into wire payload.
pub fn container_to_proto_payload(container: &Container) -> runtime_v2::ContainerPayload {
    runtime_v2::ContainerPayload {
        container_id: container.container_id.clone(),
        sandbox_id: container.sandbox_id.clone(),
        image_digest: container.image_digest.clone(),
        state: container_state_to_wire(container.state).to_string(),
        created_at: container.created_at,
        started_at: container.started_at.unwrap_or_default(),
        ended_at: container.ended_at.unwrap_or_default(),
    }
}

/// Convert wire container payload into domain container.
pub fn container_from_proto_payload(
    payload: &runtime_v2::ContainerPayload,
) -> Result<Container, TranslationError> {
    Ok(Container {
        container_id: payload.container_id.clone(),
        sandbox_id: payload.sandbox_id.clone(),
        image_digest: payload.image_digest.clone(),
        container_spec: ContainerSpec::default(),
        state: container_state_from_wire(&payload.state)?,
        created_at: payload.created_at,
        started_at: none_if_zero(payload.started_at),
        ended_at: none_if_zero(payload.ended_at),
    })
}

/// Convert domain execution into wire payload.
pub fn execution_to_proto_payload(execution: &Execution) -> runtime_v2::ExecutionPayload {
    runtime_v2::ExecutionPayload {
        execution_id: execution.execution_id.clone(),
        container_id: execution.container_id.clone(),
        state: execution_state_to_wire(execution.state).to_string(),
        exit_code: execution.exit_code.unwrap_or_default(),
        started_at: execution.started_at.unwrap_or_default(),
        ended_at: execution.ended_at.unwrap_or_default(),
    }
}

/// Convert wire execution payload into domain execution.
pub fn execution_from_proto_payload(
    payload: &runtime_v2::ExecutionPayload,
) -> Result<Execution, TranslationError> {
    let state = execution_state_from_wire(&payload.state)?;
    let exit_code = match state {
        ExecutionState::Queued | ExecutionState::Running => None,
        ExecutionState::Exited => Some(payload.exit_code),
        ExecutionState::Failed | ExecutionState::Canceled => {
            if payload.exit_code == 0 {
                None
            } else {
                Some(payload.exit_code)
            }
        }
    };

    Ok(Execution {
        execution_id: payload.execution_id.clone(),
        container_id: payload.container_id.clone(),
        exec_spec: ExecutionSpec::default(),
        state,
        exit_code,
        started_at: none_if_zero(payload.started_at),
        ended_at: none_if_zero(payload.ended_at),
    })
}

/// Convert domain checkpoint into wire payload.
pub fn checkpoint_to_proto_payload(checkpoint: &Checkpoint) -> runtime_v2::CheckpointPayload {
    runtime_v2::CheckpointPayload {
        checkpoint_id: checkpoint.checkpoint_id.clone(),
        sandbox_id: checkpoint.sandbox_id.clone(),
        parent_checkpoint_id: checkpoint.parent_checkpoint_id.clone().unwrap_or_default(),
        checkpoint_class: checkpoint_class_to_wire(checkpoint.class).to_string(),
        state: checkpoint_state_to_wire(checkpoint.state).to_string(),
        compatibility_fingerprint: checkpoint.compatibility_fingerprint.clone(),
        created_at: checkpoint.created_at,
        retention_tag: String::new(),
        retention_protected: false,
        retention_gc_reason: String::new(),
        retention_expires_at: 0,
    }
}

/// Convert wire checkpoint payload into domain checkpoint.
pub fn checkpoint_from_proto_payload(
    payload: &runtime_v2::CheckpointPayload,
) -> Result<Checkpoint, TranslationError> {
    Ok(Checkpoint {
        checkpoint_id: payload.checkpoint_id.clone(),
        sandbox_id: payload.sandbox_id.clone(),
        parent_checkpoint_id: normalize_optional_wire_field(&payload.parent_checkpoint_id),
        class: checkpoint_class_from_wire(&payload.checkpoint_class)?,
        state: checkpoint_state_from_wire(&payload.state)?,
        created_at: payload.created_at,
        compatibility_fingerprint: payload.compatibility_fingerprint.clone(),
    })
}

/// Convert domain build into wire payload.
pub fn build_to_proto_payload(build: &Build) -> runtime_v2::BuildPayload {
    runtime_v2::BuildPayload {
        build_id: build.build_id.clone(),
        sandbox_id: build.sandbox_id.clone(),
        state: build_state_to_wire(build.state).to_string(),
        result_digest: build.result_digest.clone().unwrap_or_default(),
        started_at: build.started_at,
        ended_at: build.ended_at.unwrap_or_default(),
    }
}

/// Convert wire build payload into domain build.
pub fn build_from_proto_payload(
    payload: &runtime_v2::BuildPayload,
) -> Result<Build, TranslationError> {
    Ok(Build {
        build_id: payload.build_id.clone(),
        sandbox_id: payload.sandbox_id.clone(),
        build_spec: BuildSpec::default(),
        state: build_state_from_wire(&payload.state)?,
        result_digest: normalize_optional_wire_field(&payload.result_digest),
        started_at: payload.started_at,
        ended_at: none_if_zero(payload.ended_at),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct WireRuntimeEvent {
    scope: String,
    scope_id: String,
    event_type: String,
    payload: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_id: Option<String>,
    ts: u64,
}

/// Convert domain runtime event into wire runtime event payload.
pub fn event_to_proto_runtime_event(
    event: &Event,
) -> Result<runtime_v2::RuntimeEvent, TranslationError> {
    let id = i64::try_from(event.event_id).map_err(|_| TranslationError::InvalidValue {
        field: "event.event_id",
        value: event.event_id.to_string(),
    })?;

    let envelope = WireRuntimeEvent {
        scope: event_scope_to_wire(event.scope).to_string(),
        scope_id: event.scope_id.clone(),
        event_type: event.event_type.clone(),
        payload: event.payload.clone(),
        trace_id: event.trace_id.clone(),
        ts: event.ts,
    };

    let event_json =
        serde_json::to_string(&envelope).map_err(|error| TranslationError::InvalidJson {
            field: "runtime_event.event_json",
            details: error.to_string(),
        })?;

    Ok(runtime_v2::RuntimeEvent {
        id,
        // Keep legacy field populated for compatibility surfaces.
        stack_name: event.scope_id.clone(),
        created_at: event.ts.to_string(),
        event_json,
    })
}

/// Convert wire runtime event payload into domain runtime event.
pub fn event_from_proto_runtime_event(
    event: &runtime_v2::RuntimeEvent,
) -> Result<Event, TranslationError> {
    if event.id < 0 {
        return Err(TranslationError::InvalidValue {
            field: "runtime_event.id",
            value: event.id.to_string(),
        });
    }

    let created_at_ts = parse_u64_string_field("runtime_event.created_at", &event.created_at)?;
    let envelope: WireRuntimeEvent =
        serde_json::from_str(&event.event_json).map_err(|error| TranslationError::InvalidJson {
            field: "runtime_event.event_json",
            details: error.to_string(),
        })?;

    let scope_id = if envelope.scope_id.trim().is_empty() {
        event.stack_name.clone()
    } else {
        envelope.scope_id
    };

    if scope_id.trim().is_empty() {
        return Err(TranslationError::InvalidValue {
            field: "runtime_event.scope_id",
            value: String::new(),
        });
    }

    Ok(Event {
        event_id: event.id as u64,
        ts: if envelope.ts == 0 {
            created_at_ts
        } else {
            envelope.ts
        },
        scope: event_scope_from_wire(&envelope.scope)?,
        scope_id,
        event_type: envelope.event_type,
        payload: envelope.payload,
        trace_id: normalize_optional_owned(envelope.trace_id),
    })
}

/// Convert runtime capability flags into wire capability entries.
pub fn runtime_capabilities_to_proto(
    capabilities: RuntimeCapabilities,
) -> Vec<runtime_v2::Capability> {
    vec![
        proto_capability("vm_full_checkpoint", capabilities.vm_full_checkpoint),
        proto_capability("checkpoint_fork", capabilities.checkpoint_fork),
        proto_capability("docker_compat", capabilities.docker_compat),
        proto_capability("compose_adapter", capabilities.compose_adapter),
        proto_capability("build_cache_export", capabilities.build_cache_export),
        proto_capability("gpu_passthrough", capabilities.gpu_passthrough),
        proto_capability("fs_quick_checkpoint", capabilities.fs_quick_checkpoint),
        proto_capability("shared_vm", capabilities.shared_vm),
        proto_capability("stack_networking", capabilities.stack_networking),
        proto_capability("container_logs", capabilities.container_logs),
        proto_capability("live_resize", capabilities.live_resize),
    ]
}

/// Convert wire capability entries into runtime capability flags.
pub fn runtime_capabilities_from_proto(
    capabilities: &[runtime_v2::Capability],
) -> Result<RuntimeCapabilities, TranslationError> {
    let mut seen = BTreeSet::new();
    let mut result = RuntimeCapabilities::default();

    for capability in capabilities {
        let name = capability.name.trim();
        if name.is_empty() {
            return Err(TranslationError::InvalidValue {
                field: "capability.name",
                value: capability.name.clone(),
            });
        }

        if !seen.insert(name.to_string()) {
            return Err(TranslationError::DuplicateCapability {
                name: name.to_string(),
            });
        }

        match name {
            "vm_full_checkpoint" => result.vm_full_checkpoint = capability.enabled,
            "checkpoint_fork" => result.checkpoint_fork = capability.enabled,
            "docker_compat" => result.docker_compat = capability.enabled,
            "compose_adapter" => result.compose_adapter = capability.enabled,
            "build_cache_export" => result.build_cache_export = capability.enabled,
            "gpu_passthrough" => result.gpu_passthrough = capability.enabled,
            "fs_quick_checkpoint" => result.fs_quick_checkpoint = capability.enabled,
            "shared_vm" => result.shared_vm = capability.enabled,
            "stack_networking" => result.stack_networking = capability.enabled,
            "container_logs" => result.container_logs = capability.enabled,
            "live_resize" => result.live_resize = capability.enabled,
            other => {
                return Err(TranslationError::UnknownCapability {
                    name: other.to_string(),
                });
            }
        }
    }

    Ok(result)
}

fn machine_error_code_from_wire(code: &str) -> Result<MachineErrorCode, TranslationError> {
    match code {
        "validation_error" => Ok(MachineErrorCode::ValidationError),
        "not_found" => Ok(MachineErrorCode::NotFound),
        "state_conflict" => Ok(MachineErrorCode::StateConflict),
        "policy_denied" => Ok(MachineErrorCode::PolicyDenied),
        "timeout" => Ok(MachineErrorCode::Timeout),
        "backend_unavailable" => Ok(MachineErrorCode::BackendUnavailable),
        "unsupported_operation" => Ok(MachineErrorCode::UnsupportedOperation),
        "internal_error" => Ok(MachineErrorCode::InternalError),
        other => Err(TranslationError::InvalidEnumValue {
            field: "error.code",
            value: other.to_string(),
        }),
    }
}

fn sandbox_backend_to_wire(backend: &SandboxBackend) -> &str {
    match backend {
        SandboxBackend::MacosVz => "macos_vz",
        SandboxBackend::LinuxFirecracker => "linux_firecracker",
        SandboxBackend::Other(name) => name.as_str(),
    }
}

fn sandbox_backend_from_wire(backend: &str) -> SandboxBackend {
    match backend {
        "macos_vz" => SandboxBackend::MacosVz,
        "linux_firecracker" => SandboxBackend::LinuxFirecracker,
        other if other.trim().is_empty() => SandboxBackend::Other("unknown".to_string()),
        other => SandboxBackend::Other(other.to_string()),
    }
}

fn sandbox_state_to_wire(state: SandboxState) -> &'static str {
    match state {
        SandboxState::Creating => "creating",
        SandboxState::Ready => "ready",
        SandboxState::Draining => "draining",
        SandboxState::Terminated => "terminated",
        SandboxState::Failed => "failed",
    }
}

fn sandbox_state_from_wire(state: &str) -> Result<SandboxState, TranslationError> {
    match state {
        "creating" => Ok(SandboxState::Creating),
        "ready" => Ok(SandboxState::Ready),
        "draining" => Ok(SandboxState::Draining),
        "terminated" => Ok(SandboxState::Terminated),
        "failed" => Ok(SandboxState::Failed),
        other => Err(TranslationError::InvalidEnumValue {
            field: "sandbox.state",
            value: other.to_string(),
        }),
    }
}

fn lease_state_to_wire(state: LeaseState) -> &'static str {
    match state {
        LeaseState::Opening => "opening",
        LeaseState::Active => "active",
        LeaseState::Expired => "expired",
        LeaseState::Closed => "closed",
        LeaseState::Failed => "failed",
    }
}

fn lease_state_from_wire(state: &str) -> Result<LeaseState, TranslationError> {
    match state {
        "opening" => Ok(LeaseState::Opening),
        "active" => Ok(LeaseState::Active),
        "expired" => Ok(LeaseState::Expired),
        "closed" => Ok(LeaseState::Closed),
        "failed" => Ok(LeaseState::Failed),
        other => Err(TranslationError::InvalidEnumValue {
            field: "lease.state",
            value: other.to_string(),
        }),
    }
}

fn container_state_to_wire(state: ContainerState) -> &'static str {
    match state {
        ContainerState::Created => "created",
        ContainerState::Starting => "starting",
        ContainerState::Running => "running",
        ContainerState::Stopping => "stopping",
        ContainerState::Exited => "exited",
        ContainerState::Failed => "failed",
        ContainerState::Removed => "removed",
    }
}

fn container_state_from_wire(state: &str) -> Result<ContainerState, TranslationError> {
    match state {
        "created" => Ok(ContainerState::Created),
        "starting" => Ok(ContainerState::Starting),
        "running" => Ok(ContainerState::Running),
        "stopping" => Ok(ContainerState::Stopping),
        "exited" => Ok(ContainerState::Exited),
        "failed" => Ok(ContainerState::Failed),
        "removed" => Ok(ContainerState::Removed),
        other => Err(TranslationError::InvalidEnumValue {
            field: "container.state",
            value: other.to_string(),
        }),
    }
}

fn execution_state_to_wire(state: ExecutionState) -> &'static str {
    match state {
        ExecutionState::Queued => "queued",
        ExecutionState::Running => "running",
        ExecutionState::Exited => "exited",
        ExecutionState::Failed => "failed",
        ExecutionState::Canceled => "canceled",
    }
}

fn execution_state_from_wire(state: &str) -> Result<ExecutionState, TranslationError> {
    match state {
        "queued" => Ok(ExecutionState::Queued),
        "running" => Ok(ExecutionState::Running),
        "exited" => Ok(ExecutionState::Exited),
        "failed" => Ok(ExecutionState::Failed),
        "canceled" => Ok(ExecutionState::Canceled),
        other => Err(TranslationError::InvalidEnumValue {
            field: "execution.state",
            value: other.to_string(),
        }),
    }
}

fn checkpoint_class_to_wire(class: CheckpointClass) -> &'static str {
    match class {
        CheckpointClass::FsQuick => "fs_quick",
        CheckpointClass::VmFull => "vm_full",
    }
}

fn checkpoint_class_from_wire(class: &str) -> Result<CheckpointClass, TranslationError> {
    match class {
        "fs_quick" => Ok(CheckpointClass::FsQuick),
        "vm_full" => Ok(CheckpointClass::VmFull),
        other => Err(TranslationError::InvalidEnumValue {
            field: "checkpoint.class",
            value: other.to_string(),
        }),
    }
}

fn checkpoint_state_to_wire(state: CheckpointState) -> &'static str {
    match state {
        CheckpointState::Creating => "creating",
        CheckpointState::Ready => "ready",
        CheckpointState::Failed => "failed",
    }
}

fn checkpoint_state_from_wire(state: &str) -> Result<CheckpointState, TranslationError> {
    match state {
        "creating" => Ok(CheckpointState::Creating),
        "ready" => Ok(CheckpointState::Ready),
        "failed" => Ok(CheckpointState::Failed),
        other => Err(TranslationError::InvalidEnumValue {
            field: "checkpoint.state",
            value: other.to_string(),
        }),
    }
}

fn build_state_to_wire(state: BuildState) -> &'static str {
    match state {
        BuildState::Queued => "queued",
        BuildState::Running => "running",
        BuildState::Succeeded => "succeeded",
        BuildState::Failed => "failed",
        BuildState::Canceled => "canceled",
    }
}

fn build_state_from_wire(state: &str) -> Result<BuildState, TranslationError> {
    match state {
        "queued" => Ok(BuildState::Queued),
        "running" => Ok(BuildState::Running),
        "succeeded" => Ok(BuildState::Succeeded),
        "failed" => Ok(BuildState::Failed),
        "canceled" => Ok(BuildState::Canceled),
        other => Err(TranslationError::InvalidEnumValue {
            field: "build.state",
            value: other.to_string(),
        }),
    }
}

fn event_scope_to_wire(scope: EventScope) -> &'static str {
    match scope {
        EventScope::Sandbox => "sandbox",
        EventScope::Lease => "lease",
        EventScope::Build => "build",
        EventScope::Container => "container",
        EventScope::Execution => "execution",
        EventScope::Checkpoint => "checkpoint",
        EventScope::System => "system",
    }
}

fn event_scope_from_wire(scope: &str) -> Result<EventScope, TranslationError> {
    match scope {
        "sandbox" => Ok(EventScope::Sandbox),
        "lease" => Ok(EventScope::Lease),
        "build" => Ok(EventScope::Build),
        "container" => Ok(EventScope::Container),
        "execution" => Ok(EventScope::Execution),
        "checkpoint" => Ok(EventScope::Checkpoint),
        "system" => Ok(EventScope::System),
        other => Err(TranslationError::InvalidEnumValue {
            field: "event.scope",
            value: other.to_string(),
        }),
    }
}

fn proto_capability(name: &str, enabled: bool) -> runtime_v2::Capability {
    runtime_v2::Capability {
        name: name.to_string(),
        enabled,
    }
}

fn parse_u64_string_field(field: &'static str, value: &str) -> Result<u64, TranslationError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(0);
    }

    trimmed
        .parse::<u64>()
        .map_err(|_| TranslationError::InvalidValue {
            field,
            value: value.to_string(),
        })
}

fn normalize_optional_wire_field(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn normalize_optional_owned(value: Option<String>) -> Option<String> {
    value.and_then(|value| normalize_optional_wire_field(&value))
}

fn none_if_zero(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

fn btree_to_hash_map(
    input: &BTreeMap<String, String>,
) -> std::collections::HashMap<String, String> {
    input.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn hash_to_btree_map(
    input: &std::collections::HashMap<String, String>,
) -> BTreeMap<String, String> {
    input.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;
    use prost::Message;

    const V: u32 = vz_runtime_contract::TOPOLOGY_SCHEMA_VERSION;

    fn project_id(value: &str) -> ProjectId {
        ProjectId::new(value).expect("valid project ID")
    }

    fn environment_id(value: &str) -> EnvironmentId {
        EnvironmentId::new(value).expect("valid environment ID")
    }

    fn machine_id(value: &str) -> MachineId {
        MachineId::new(value).expect("valid machine ID")
    }

    fn network_id(value: &str) -> NetworkId {
        NetworkId::new(value).expect("valid network ID")
    }

    fn endpoint_id(value: &str) -> EndpointId {
        EndpointId::new(value).expect("valid endpoint ID")
    }

    fn requested_linux_capabilities() -> CapabilitySet {
        CapabilitySet::new([
            MachineCapability::PosixExec,
            MachineCapability::PosixPty,
            MachineCapability::Signals,
            MachineCapability::Files,
            MachineCapability::Ports,
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
            MachineCapability::Checkpoint,
        ])
    }

    fn negotiated_linux_capabilities() -> CapabilitySet {
        let mut set = CapabilitySet::new([
            MachineCapability::PosixExec,
            MachineCapability::PosixPty,
            MachineCapability::Signals,
            MachineCapability::Files,
            MachineCapability::Ports,
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ]);
        set.unsupported.insert(
            MachineCapability::Checkpoint,
            "kernel profile does not expose checkpoint restore".to_string(),
        );
        set
    }

    fn macos_capabilities() -> CapabilitySet {
        CapabilitySet::new([
            MachineCapability::PosixExec,
            MachineCapability::PosixPty,
            MachineCapability::Signals,
            MachineCapability::Files,
            MachineCapability::Ports,
            MachineCapability::Snapshot,
            MachineCapability::Suspend,
            MachineCapability::Gui,
        ])
    }

    fn target(os: OperatingSystem, image: &str) -> TargetSpec {
        TargetSpec {
            os,
            arch: Architecture::Aarch64,
            image: image.to_string(),
            version: Some("15.6.1".to_string()),
            channel: Some("stable".to_string()),
            digest: Some(format!("sha256:{image}")),
        }
    }

    fn machine_spec(name: &str, os: OperatingSystem) -> MachineSpec {
        let (requested_capabilities, workspace) = match os {
            OperatingSystem::Linux => (
                requested_linux_capabilities(),
                Some(WorkspaceProjection {
                    binding: "source".to_string(),
                    target_path: "/workspace".to_string(),
                    mode: WorkspaceProjectionMode::ReadWrite,
                }),
            ),
            OperatingSystem::Macos => (
                macos_capabilities(),
                Some(WorkspaceProjection {
                    binding: "source".to_string(),
                    target_path: "/Users/vz/workspace".to_string(),
                    mode: WorkspaceProjectionMode::Snapshot,
                }),
            ),
            OperatingSystem::Windows => (CapabilitySet::default(), None),
        };
        MachineSpec {
            schema_version: V,
            name: name.to_string(),
            profile: MachineProfile::Developer,
            target: target(os, name),
            resources: MachineResources {
                cpus: Some(12),
                memory_mb: Some(65_536),
                disk_bytes: Some(536_870_912_000),
            },
            requested_capabilities,
            workspace,
        }
    }

    fn project_definition() -> ProjectDefinition {
        ProjectDefinition {
            schema_version: V,
            project_id: project_id("prj-roundtrip"),
            name: "roundtrip".to_string(),
            environment: EnvironmentSpec {
                schema_version: V,
                machines: vec![
                    machine_spec("linux", OperatingSystem::Linux),
                    machine_spec("macos", OperatingSystem::Macos),
                ],
                networks: vec![
                    NetworkSpec {
                        schema_version: V,
                        name: "private".to_string(),
                        kind: NetworkKind::Private,
                        cidr: Some("10.44.0.0/24".to_string()),
                    },
                    NetworkSpec {
                        schema_version: V,
                        name: "public-like".to_string(),
                        kind: NetworkKind::SimulatedPublic,
                        cidr: Some("198.18.44.0/24".to_string()),
                    },
                ],
                endpoints: vec![EndpointSpec {
                    schema_version: V,
                    name: "web".to_string(),
                    machine: "linux".to_string(),
                    network: "public-like".to_string(),
                    protocol: EndpointProtocol::Https,
                    port: 8443,
                    hostname: Some("web.test".to_string()),
                }],
            },
        }
    }

    fn environment(suffix: &str, path_hint: &str) -> EnvironmentInstance {
        let definition = project_definition();
        let definition_digest = definition.digest().expect("definition digest");
        let linux_spec = &definition.environment.machines[0];
        let macos_spec = &definition.environment.machines[1];
        let project_id = definition.project_id.clone();
        let environment_id = environment_id(&format!("env-{suffix}"));
        let linux_id = machine_id(&format!("mac-{suffix}-linux"));
        let macos_id = machine_id(&format!("mac-{suffix}-macos"));
        let public_network_id = network_id(&format!("net-{suffix}-public"));
        let private_network_id = network_id(&format!("net-{suffix}-private"));
        EnvironmentInstance {
            schema_version: V,
            environment_id: environment_id.clone(),
            project_id: project_id.clone(),
            name: suffix.to_string(),
            definition_digest,
            state: EnvironmentState::Ready,
            bindings: vec![WorkspaceBinding {
                schema_version: V,
                binding_id: WorkspaceBindingId::new(format!("wsp-{suffix}"))
                    .expect("valid binding ID"),
                project_id,
                environment_id: environment_id.clone(),
                name: "source".to_string(),
                workspace_key: "shared-worktree-key".to_string(),
                path_hint: Some(path_hint.to_string()),
            }],
            machines: vec![
                MachineInstance {
                    schema_version: V,
                    machine_id: linux_id.clone(),
                    environment_id: environment_id.clone(),
                    name: "linux".to_string(),
                    profile: MachineProfile::Developer,
                    target: linux_spec.target.clone(),
                    resources: MachineResources {
                        cpus: Some(16),
                        memory_mb: Some(131_072),
                        disk_bytes: Some(1_099_511_627_776),
                    },
                    requested_capabilities: linux_spec.requested_capabilities.clone(),
                    negotiated_capabilities: negotiated_linux_capabilities(),
                    backend: Some(MachineBackend::MacosVirtualizationLinux),
                    incarnation: Some(MachineIncarnation {
                        schema_version: V,
                        incarnation_id: MachineIncarnationId::new(format!("inc-{suffix}-linux"))
                            .expect("valid incarnation ID"),
                        machine_id: linux_id.clone(),
                        generation: 7,
                        created_at: 1_725_000_001,
                    }),
                    state: MachineState::Ready,
                    legacy_sandbox_id: Some(format!("legacy-{suffix}")),
                },
                MachineInstance {
                    schema_version: V,
                    machine_id: macos_id.clone(),
                    environment_id: environment_id.clone(),
                    name: "macos".to_string(),
                    profile: MachineProfile::Developer,
                    target: macos_spec.target.clone(),
                    resources: MachineResources {
                        cpus: Some(8),
                        memory_mb: Some(65_536),
                        disk_bytes: Some(536_870_912_000),
                    },
                    requested_capabilities: macos_spec.requested_capabilities.clone(),
                    negotiated_capabilities: macos_capabilities(),
                    backend: Some(MachineBackend::MacosNative),
                    incarnation: Some(MachineIncarnation {
                        schema_version: V,
                        incarnation_id: MachineIncarnationId::new(format!("inc-{suffix}-macos"))
                            .expect("valid incarnation ID"),
                        machine_id: macos_id,
                        generation: 3,
                        created_at: 1_725_000_002,
                    }),
                    state: MachineState::Stopped,
                    legacy_sandbox_id: None,
                },
            ],
            networks: vec![
                NetworkInstance {
                    schema_version: V,
                    network_id: private_network_id,
                    environment_id: environment_id.clone(),
                    name: "private".to_string(),
                },
                NetworkInstance {
                    schema_version: V,
                    network_id: public_network_id.clone(),
                    environment_id: environment_id.clone(),
                    name: "public-like".to_string(),
                },
            ],
            endpoints: vec![EndpointInstance {
                schema_version: V,
                endpoint_id: endpoint_id(&format!("ep-{suffix}-web")),
                environment_id: environment_id.clone(),
                machine_id: linux_id.clone(),
                network_id: public_network_id.clone(),
                name: "web".to_string(),
            }],
            ownership: vec![
                OwnershipRecord {
                    schema_version: V,
                    resource_kind: OwnedResourceKind::Disk,
                    resource_id: format!("disk-{suffix}-linux"),
                    environment_id: environment_id.clone(),
                    machine_id: Some(linux_id),
                },
                OwnershipRecord {
                    schema_version: V,
                    resource_kind: OwnedResourceKind::Network,
                    resource_id: public_network_id.to_string(),
                    environment_id: environment_id.clone(),
                    machine_id: None,
                },
                OwnershipRecord {
                    schema_version: V,
                    resource_kind: OwnedResourceKind::Other("audit_bundle".to_string()),
                    resource_id: format!("audit-{suffix}"),
                    environment_id,
                    machine_id: None,
                },
            ],
            legacy_migration: Some(LegacyMigrationProvenance {
                source_version: "0.3.20".to_string(),
                legacy_sandbox_id: format!("legacy-{suffix}"),
                unresolved_resources: vec!["external-cache:old".to_string()],
            }),
            created_at: 1_725_000_000,
            updated_at: 1_725_000_999,
        }
    }

    fn topology_fixture() -> ProjectState {
        ProjectState {
            schema_version: V,
            definition: project_definition(),
            environments: vec![
                environment("alpha", "/Users/alice/project"),
                environment("beta", "/Volumes/worktrees/project"),
            ],
        }
    }

    #[test]
    fn topology_project_state_round_trips_through_protobuf_bytes() {
        let domain = topology_fixture();
        domain.validate().expect("fixture is canonically valid");

        let wire = project_state_to_proto(&domain);
        let bytes = wire.encode_to_vec();
        let decoded_wire =
            runtime_v2::ProjectState::decode(bytes.as_slice()).expect("protobuf bytes decode");
        let decoded = project_state_from_proto(&decoded_wire).expect("topology decode");

        assert_eq!(decoded, domain);
        assert_eq!(decoded.environments.len(), 2);
        let definition_digest = decoded.definition.digest().expect("definition digest");
        assert!(
            decoded
                .environments
                .iter()
                .all(|environment| environment.definition_digest == definition_digest)
        );
        assert_eq!(decoded.environments[0].bindings[0].name, "source");
        assert!(
            decoded
                .environments
                .iter()
                .flat_map(|environment| &environment.machines)
                .all(|machine| machine.profile == MachineProfile::Developer)
        );
        assert_eq!(
            decoded.environments[0].bindings[0].workspace_key,
            decoded.environments[1].bindings[0].workspace_key
        );
        assert_eq!(
            decoded.environments[0].machines[0].target,
            decoded.definition.environment.machines[0].target
        );
        assert_eq!(
            decoded.environments[0].machines[1].target,
            decoded.definition.environment.machines[1].target
        );
        let negotiated = &decoded.environments[0].machines[0].negotiated_capabilities;
        for capability in [
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ] {
            assert!(negotiated.contains(capability));
        }
        assert_eq!(
            negotiated.unsupported.get(&MachineCapability::Checkpoint),
            Some(&"kernel profile does not expose checkpoint restore".to_string())
        );
    }

    #[test]
    fn topology_project_state_round_trips_through_json() {
        let domain = topology_fixture();
        domain.validate().expect("fixture is canonically valid");

        let json = serde_json::to_string_pretty(&domain).expect("topology JSON encode");
        let decoded: ProjectState = serde_json::from_str(&json).expect("topology JSON decode");
        decoded.validate().expect("decoded topology remains valid");

        assert_eq!(decoded, domain);
        assert_eq!(decoded.environments.len(), 2);
        assert!(decoded.environments.iter().all(|environment| {
            environment.machines.iter().any(|machine| {
                machine.target.os == OperatingSystem::Linux
                    && machine.profile == MachineProfile::Developer
            }) && environment
                .machines
                .iter()
                .any(|machine| machine.target.os == OperatingSystem::Macos)
        }));
    }

    #[test]
    fn topology_decode_rejects_missing_required_messages() {
        let mut wire = project_state_to_proto(&topology_fixture());
        wire.definition = None;
        assert_eq!(
            project_state_from_proto(&wire),
            Err(TranslationError::MissingRequiredField {
                field: "project_state.definition"
            })
        );

        let mut machine =
            machine_instance_to_proto(&topology_fixture().environments[0].machines[0]);
        machine.target = None;
        assert_eq!(
            machine_instance_from_proto(&machine),
            Err(TranslationError::MissingRequiredField {
                field: "machine_instance.target"
            })
        );
    }

    #[test]
    fn topology_decode_rejects_unspecified_and_unknown_enums() {
        let mut target = target_spec_to_proto(&target(OperatingSystem::Linux, "linux"));
        target.os = runtime_v2::OperatingSystem::Unspecified as i32;
        assert!(matches!(
            target_spec_from_proto(&target),
            Err(TranslationError::InvalidEnumValue {
                field: "target_spec.os",
                ..
            })
        ));
        target.os = 9_999;
        assert!(matches!(
            target_spec_from_proto(&target),
            Err(TranslationError::InvalidEnumValue {
                field: "target_spec.os",
                ..
            })
        ));

        let mut spec = machine_spec_to_proto(&machine_spec("linux", OperatingSystem::Linux));
        spec.profile = runtime_v2::MachineProfile::Unspecified as i32;
        assert!(matches!(
            machine_spec_from_proto(&spec),
            Err(TranslationError::InvalidEnumValue {
                field: "machine_spec.profile",
                ..
            })
        ));
        spec.profile = 9_999;
        assert!(matches!(
            machine_spec_from_proto(&spec),
            Err(TranslationError::InvalidEnumValue {
                field: "machine_spec.profile",
                ..
            })
        ));

        let mut machine =
            machine_instance_to_proto(&topology_fixture().environments[0].machines[0]);
        machine.profile = runtime_v2::MachineProfile::Unspecified as i32;
        assert!(matches!(
            machine_instance_from_proto(&machine),
            Err(TranslationError::InvalidEnumValue {
                field: "machine_instance.profile",
                ..
            })
        ));
        machine.profile = 9_999;
        assert!(matches!(
            machine_instance_from_proto(&machine),
            Err(TranslationError::InvalidEnumValue {
                field: "machine_instance.profile",
                ..
            })
        ));

        let invalid_profile = runtime_v2::TopologyErrorDetail {
            detail: Some(
                runtime_v2::topology_error_detail::Detail::InvalidMachineProfile(
                    runtime_v2::TopologyInvalidMachineProfileDetail {
                        machine_id: "mac_native".to_string(),
                        profile: runtime_v2::MachineProfile::Unspecified as i32,
                        reason: "unsupported profile".to_string(),
                    },
                ),
            ),
        };
        assert!(matches!(
            topology_validation_error_from_proto(&invalid_profile),
            Err(TranslationError::InvalidEnumValue {
                field: "topology_invalid_machine_profile.profile",
                ..
            })
        ));

        let mut unknown_profile = invalid_profile;
        let Some(runtime_v2::topology_error_detail::Detail::InvalidMachineProfile(detail)) =
            unknown_profile.detail.as_mut()
        else {
            unreachable!("fixture detail is an invalid Machine profile");
        };
        detail.profile = 9_999;
        assert!(matches!(
            topology_validation_error_from_proto(&unknown_profile),
            Err(TranslationError::InvalidEnumValue {
                field: "topology_invalid_machine_profile.profile",
                ..
            })
        ));

        let contradictory = runtime_v2::TopologyErrorDetail {
            detail: Some(
                runtime_v2::topology_error_detail::Detail::ContradictoryCapability(
                    runtime_v2::TopologyContradictoryCapabilityDetail {
                        machine_id: "mac_linux".to_string(),
                        capability: runtime_v2::MachineCapability::Unspecified as i32,
                    },
                ),
            ),
        };
        assert!(matches!(
            topology_validation_error_from_proto(&contradictory),
            Err(TranslationError::InvalidEnumValue {
                field: "topology_contradictory_capability.capability",
                ..
            })
        ));

        let mut unknown_capability = contradictory;
        let Some(runtime_v2::topology_error_detail::Detail::ContradictoryCapability(detail)) =
            unknown_capability.detail.as_mut()
        else {
            unreachable!("fixture detail is a contradictory capability");
        };
        detail.capability = 9_999;
        assert!(matches!(
            topology_validation_error_from_proto(&unknown_capability),
            Err(TranslationError::InvalidEnumValue {
                field: "topology_contradictory_capability.capability",
                ..
            })
        ));
    }

    #[test]
    fn hardened_machine_profile_round_trips_losslessly() {
        let mut spec = machine_spec("linux", OperatingSystem::Linux);
        spec.profile = MachineProfile::Hardened;
        for capability in [
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ] {
            spec.requested_capabilities.capabilities.remove(&capability);
        }
        let decoded = machine_spec_from_proto(&machine_spec_to_proto(&spec))
            .expect("Hardened MachineSpec decode");
        assert_eq!(decoded, spec);

        let mut machine = topology_fixture().environments[0].machines[0].clone();
        machine.profile = MachineProfile::Hardened;
        for capability in [
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ] {
            machine
                .requested_capabilities
                .capabilities
                .remove(&capability);
            machine
                .negotiated_capabilities
                .capabilities
                .remove(&capability);
        }
        machine.validate().expect("valid Hardened Machine");
        let decoded = machine_instance_from_proto(&machine_instance_to_proto(&machine))
            .expect("Hardened MachineInstance decode");
        assert_eq!(decoded, machine);
    }

    #[test]
    fn topology_decode_rejects_malformed_typed_ids() {
        let mut wire = project_definition_to_proto(&project_definition());
        wire.project_id = "../../project".to_string();
        assert!(matches!(
            project_definition_from_proto(&wire),
            Err(TranslationError::InvalidTopology(
                TopologyValidationError::InvalidIdentifier { .. }
            ))
        ));
    }

    #[test]
    fn topology_decode_rejects_inconsistent_other_companions() {
        let domain = topology_fixture();
        let mut machine = machine_instance_to_proto(&domain.environments[0].machines[0]);
        machine.other_backend = Some("custom".to_string());
        assert!(matches!(
            machine_instance_from_proto(&machine),
            Err(TranslationError::InvalidValue {
                field: "machine_instance.backend",
                ..
            })
        ));

        machine.backend = Some(runtime_v2::MachineBackend::Other as i32);
        machine.other_backend = None;
        assert!(matches!(
            machine_instance_from_proto(&machine),
            Err(TranslationError::InvalidValue {
                field: "machine_instance.backend",
                ..
            })
        ));

        let mut ownership = ownership_record_to_proto(&domain.environments[0].ownership[0]);
        ownership.other_resource_kind = Some("custom".to_string());
        assert!(matches!(
            ownership_record_from_proto(&ownership),
            Err(TranslationError::InvalidValue {
                field: "ownership_record.resource_kind",
                ..
            })
        ));
    }

    #[test]
    fn topology_other_variants_round_trip_with_exact_companions() {
        let mut machine = topology_fixture().environments[0].machines[0].clone();
        machine.backend = Some(MachineBackend::Other("remote_lab_backend".to_string()));
        let wire = machine_instance_to_proto(&machine);
        let decoded = machine_instance_from_proto(&wire).expect("custom backend decode");
        assert_eq!(decoded, machine);

        let ownership = topology_fixture().environments[0].ownership[2].clone();
        let wire = ownership_record_to_proto(&ownership);
        let decoded = ownership_record_from_proto(&wire).expect("custom ownership kind decode");
        assert_eq!(decoded, ownership);
    }

    #[test]
    fn topology_decode_rejects_duplicate_and_unknown_capabilities() {
        let duplicate = runtime_v2::CapabilitySet {
            capabilities: vec![
                runtime_v2::MachineCapability::Files as i32,
                runtime_v2::MachineCapability::Files as i32,
            ],
            unsupported: Vec::new(),
        };
        assert_eq!(
            capability_set_from_proto(&duplicate),
            Err(TranslationError::DuplicateCapability {
                name: "files".to_string()
            })
        );

        let unknown = runtime_v2::CapabilitySet {
            capabilities: vec![8_888],
            unsupported: Vec::new(),
        };
        assert!(matches!(
            capability_set_from_proto(&unknown),
            Err(TranslationError::InvalidEnumValue {
                field: "capability_set.capabilities",
                ..
            })
        ));
    }

    #[test]
    fn topology_decode_rejects_endpoint_port_above_u16() {
        let mut endpoint = endpoint_spec_to_proto(&project_definition().environment.endpoints[0]);
        endpoint.port = u32::from(u16::MAX) + 1;
        assert_eq!(
            endpoint_spec_from_proto(&endpoint),
            Err(TranslationError::InvalidValue {
                field: "endpoint_spec.port",
                value: "65536".to_string()
            })
        );
    }

    #[test]
    fn topology_decode_rejects_aggregate_that_fails_canonical_validation() {
        let mut wire = project_state_to_proto(&topology_fixture());
        wire.environments[1].environment_id = wire.environments[0].environment_id.clone();
        let error = project_state_from_proto(&wire).expect_err("invalid aggregate ownership");
        assert!(matches!(
            error,
            TranslationError::InvalidTopology(TopologyValidationError::OwnershipMismatch {
                kind,
                ..
            }) if kind == "machine.environment"
        ));
    }

    #[test]
    fn topology_decode_rejects_definition_digest_or_target_drift() {
        let mut digest_drift = project_state_to_proto(&topology_fixture());
        digest_drift.environments[0].definition_digest = "sha256:stale".to_string();
        assert!(matches!(
            project_state_from_proto(&digest_drift),
            Err(TranslationError::InvalidTopology(
                TopologyValidationError::DefinitionDigestMismatch { .. }
            ))
        ));

        let mut target_drift = project_state_to_proto(&topology_fixture());
        let target = target_drift.environments[0].machines[0]
            .target
            .as_mut()
            .expect("required fixture target");
        target.image = "different-image".to_string();
        assert!(matches!(
            project_state_from_proto(&target_drift),
            Err(TranslationError::InvalidTopology(
                TopologyValidationError::DefinitionTopologyMismatch { .. }
            ))
        ));
    }

    #[test]
    fn topology_resolution_errors_round_trip_and_restore_candidate_order() {
        let error = TopologyResolutionError::ambiguous(
            "environment",
            "dev",
            [
                TopologyCandidate {
                    id: "env-z".to_string(),
                    name: "zeta".to_string(),
                },
                TopologyCandidate {
                    id: "env-a".to_string(),
                    name: "alpha".to_string(),
                },
            ],
        );
        let mut wire = topology_resolution_error_to_proto(&error);
        if let Some(runtime_v2::topology_error_detail::Detail::Ambiguous(detail)) =
            wire.detail.as_mut()
        {
            detail.candidates.reverse();
        }
        let decoded = topology_resolution_error_from_proto(&wire).expect("resolution error");
        assert_eq!(decoded, error);

        let selection_required = TopologyResolutionError::selection_required(
            "environment",
            "workspace:worktree-new",
            [
                TopologyCandidate {
                    id: "env-z".to_string(),
                    name: "zeta".to_string(),
                },
                TopologyCandidate {
                    id: "env-a".to_string(),
                    name: "alpha".to_string(),
                },
            ],
        );
        let wire = topology_resolution_error_to_proto(&selection_required);
        let decoded = topology_resolution_error_from_proto(&wire).expect("selection required");
        assert_eq!(decoded, selection_required);

        let invalid_selector = TopologyResolutionError::InvalidSelector {
            kind: "environment".to_string(),
            selector: " ".to_string(),
            reason: "name must not be blank".to_string(),
        };
        let wire = topology_resolution_error_to_proto(&invalid_selector);
        let decoded = topology_resolution_error_from_proto(&wire).expect("invalid selector");
        assert_eq!(decoded, invalid_selector);
    }

    #[test]
    fn representable_topology_validation_errors_round_trip() {
        let errors = [
            TopologyValidationError::UnsupportedTarget {
                host_os: OperatingSystem::Macos,
                host_arch: Architecture::Aarch64,
                target_os: OperatingSystem::Windows,
                target_arch: Architecture::X86_64,
                requested_capabilities: vec![
                    MachineCapability::WindowsConsole,
                    MachineCapability::Gui,
                ],
            },
            TopologyValidationError::MissingCapability {
                machine_id: "mac_linux".to_string(),
                capability: MachineCapability::Compose,
            },
            TopologyValidationError::InvalidMachineProfile {
                machine_id: "mac_native".to_string(),
                profile: MachineProfile::Hardened,
                reason: "native targets support only the Developer profile".to_string(),
            },
            TopologyValidationError::InvalidCapabilityDeclaration {
                machine_id: "mac_native".to_string(),
                reason: "non-Linux target cannot declare implicit capability `DockerEngine`"
                    .to_string(),
            },
            TopologyValidationError::ContradictoryCapability {
                machine_id: "mac_linux".to_string(),
                capability: MachineCapability::Buildx,
            },
        ];
        for error in errors {
            let wire = topology_validation_error_to_proto(&error).expect("representable error");
            let decoded = topology_validation_error_from_proto(&wire).expect("validation error");
            assert_eq!(decoded, error);
        }

        let unrepresentable = TopologyValidationError::InvalidName {
            kind: "machine".to_string(),
            value: "".to_string(),
        };
        assert!(topology_validation_error_to_proto(&unrepresentable).is_none());
    }

    fn sample_request_metadata() -> RequestMetadata {
        RequestMetadata::new(Some(" req-1 ".to_string()), Some(" idem-1 ".to_string()))
            .with_trace_id(Some(" trace-1 ".to_string()))
    }

    #[test]
    fn request_metadata_round_trip_normalizes_fields() {
        let metadata = sample_request_metadata();
        let proto = request_metadata_to_proto(&metadata);
        let decoded = request_metadata_from_proto(&proto);

        assert_eq!(decoded.request_id.as_deref(), Some("req-1"));
        assert_eq!(decoded.idempotency_key.as_deref(), Some("idem-1"));
        assert_eq!(decoded.trace_id.as_deref(), Some("trace-1"));
        assert!(decoded.passthrough.is_empty());
    }

    #[test]
    fn request_metadata_from_proto_drops_empty_values() {
        let proto = runtime_v2::RequestMetadata {
            request_id: " ".to_string(),
            idempotency_key: "".to_string(),
            trace_id: "\n".to_string(),
        };

        let decoded = request_metadata_from_proto(&proto);
        assert!(decoded.request_id.is_none());
        assert!(decoded.idempotency_key.is_none());
        assert!(decoded.trace_id.is_none());
    }

    #[test]
    fn machine_error_code_round_trip_for_all_known_values() {
        for code in MachineErrorCode::ALL {
            let error = MachineError::new(
                code,
                "oops".to_string(),
                Some("req-2".to_string()),
                BTreeMap::new(),
            );
            let proto = machine_error_to_proto_detail(&error);
            let decoded = machine_error_from_proto_detail(&proto).expect("decode should succeed");
            assert_eq!(decoded.code, code);
            assert_eq!(decoded.message, "oops");
            assert_eq!(decoded.request_id.as_deref(), Some("req-2"));
            assert!(decoded.details.is_empty());
        }
    }

    #[test]
    fn machine_error_unknown_code_is_rejected() {
        let detail = runtime_v2::ErrorDetail {
            code: "made_up".to_string(),
            message: "x".to_string(),
            request_id: String::new(),
        };

        let err = machine_error_from_proto_detail(&detail).expect_err("should reject unknown code");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "error.code",
                value: "made_up".to_string(),
            }
        );
    }

    #[test]
    fn sandbox_payload_round_trip_preserves_representable_fields() {
        let sandbox = Sandbox {
            sandbox_id: "sbx-1".to_string(),
            backend: SandboxBackend::MacosVz,
            spec: SandboxSpec {
                cpus: Some(4),
                memory_mb: Some(4096),
                base_image_ref: Some("alpine:3.20".to_string()),
                main_container: Some("workspace-main".to_string()),
                network_profile: None,
                volume_mounts: Vec::new(),
            },
            state: SandboxState::Ready,
            created_at: 11,
            updated_at: 12,
            labels: BTreeMap::from([
                ("env".to_string(), "dev".to_string()),
                (
                    SANDBOX_LABEL_BASE_IMAGE_REF.to_string(),
                    "alpine:3.20".to_string(),
                ),
                (
                    SANDBOX_LABEL_MAIN_CONTAINER.to_string(),
                    "workspace-main".to_string(),
                ),
            ]),
        };

        let payload = sandbox_to_proto_payload(&sandbox);
        let decoded = sandbox_from_proto_payload(&payload).expect("sandbox decode should succeed");
        assert_eq!(decoded, sandbox);
    }

    #[test]
    fn sandbox_payload_rejects_unknown_state() {
        let payload = runtime_v2::SandboxPayload {
            sandbox_id: "sbx".to_string(),
            backend: "macos_vz".to_string(),
            state: "booting".to_string(),
            cpus: 0,
            memory_mb: 0,
            created_at: 0,
            updated_at: 0,
            labels: std::collections::HashMap::new(),
        };

        let err = sandbox_from_proto_payload(&payload).expect_err("unknown state should fail");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "sandbox.state",
                value: "booting".to_string(),
            }
        );
    }

    #[test]
    fn lease_payload_round_trip() {
        let lease = Lease {
            lease_id: "lease-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            ttl_secs: 60,
            last_heartbeat_at: 123,
            state: LeaseState::Active,
        };

        let payload = lease_to_proto_payload(&lease);
        let decoded = lease_from_proto_payload(&payload).expect("lease decode should succeed");
        assert_eq!(decoded, lease);
    }

    #[test]
    fn lease_payload_rejects_unknown_state() {
        let payload = runtime_v2::LeasePayload {
            lease_id: "lease".to_string(),
            sandbox_id: "sbx".to_string(),
            ttl_secs: 10,
            last_heartbeat_at: 1,
            state: "paused".to_string(),
        };

        let err = lease_from_proto_payload(&payload).expect_err("unknown state should fail");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "lease.state",
                value: "paused".to_string(),
            }
        );
    }

    #[test]
    fn container_payload_round_trip() {
        let container = Container {
            container_id: "ctr-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            image_digest: "sha256:abc".to_string(),
            container_spec: ContainerSpec::default(),
            state: ContainerState::Running,
            created_at: 10,
            started_at: Some(11),
            ended_at: None,
        };

        let payload = container_to_proto_payload(&container);
        let decoded =
            container_from_proto_payload(&payload).expect("container decode should succeed");
        assert_eq!(decoded, container);
    }

    #[test]
    fn execution_payload_round_trip_for_exited_state() {
        let execution = Execution {
            execution_id: "exec-1".to_string(),
            container_id: "ctr-1".to_string(),
            exec_spec: ExecutionSpec::default(),
            state: ExecutionState::Exited,
            exit_code: Some(0),
            started_at: Some(20),
            ended_at: Some(30),
        };

        let payload = execution_to_proto_payload(&execution);
        let decoded =
            execution_from_proto_payload(&payload).expect("execution decode should succeed");
        assert_eq!(decoded, execution);
    }

    #[test]
    fn execution_payload_rejects_unknown_state() {
        let payload = runtime_v2::ExecutionPayload {
            execution_id: "exec".to_string(),
            container_id: "ctr".to_string(),
            state: "completed".to_string(),
            exit_code: 0,
            started_at: 0,
            ended_at: 0,
        };

        let err = execution_from_proto_payload(&payload).expect_err("unknown state should fail");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "execution.state",
                value: "completed".to_string(),
            }
        );
    }

    #[test]
    fn checkpoint_payload_round_trip() {
        let checkpoint = Checkpoint {
            checkpoint_id: "ckpt-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            parent_checkpoint_id: Some("ckpt-parent".to_string()),
            class: CheckpointClass::FsQuick,
            state: CheckpointState::Ready,
            created_at: 44,
            compatibility_fingerprint: "kernel-6.1".to_string(),
        };

        let payload = checkpoint_to_proto_payload(&checkpoint);
        let decoded =
            checkpoint_from_proto_payload(&payload).expect("checkpoint decode should succeed");
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    fn checkpoint_payload_rejects_unknown_class() {
        let payload = runtime_v2::CheckpointPayload {
            checkpoint_id: "ckpt".to_string(),
            sandbox_id: "sbx".to_string(),
            parent_checkpoint_id: String::new(),
            checkpoint_class: "snapshot".to_string(),
            state: "ready".to_string(),
            compatibility_fingerprint: String::new(),
            created_at: 0,
            retention_tag: String::new(),
            retention_protected: false,
            retention_gc_reason: String::new(),
            retention_expires_at: 0,
        };

        let err = checkpoint_from_proto_payload(&payload).expect_err("unknown class should fail");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "checkpoint.class",
                value: "snapshot".to_string(),
            }
        );
    }

    #[test]
    fn checkpoint_payload_rejects_unknown_state() {
        let payload = runtime_v2::CheckpointPayload {
            checkpoint_id: "ckpt".to_string(),
            sandbox_id: "sbx".to_string(),
            parent_checkpoint_id: String::new(),
            checkpoint_class: "fs_quick".to_string(),
            state: "restoring".to_string(),
            compatibility_fingerprint: String::new(),
            created_at: 0,
            retention_tag: String::new(),
            retention_protected: false,
            retention_gc_reason: String::new(),
            retention_expires_at: 0,
        };

        let err = checkpoint_from_proto_payload(&payload).expect_err("unknown state should fail");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "checkpoint.state",
                value: "restoring".to_string(),
            }
        );
    }

    #[test]
    fn build_payload_round_trip() {
        let build = Build {
            build_id: "build-1".to_string(),
            sandbox_id: "sbx-1".to_string(),
            build_spec: BuildSpec::default(),
            state: BuildState::Succeeded,
            result_digest: Some("sha256:def".to_string()),
            started_at: 70,
            ended_at: Some(80),
        };

        let payload = build_to_proto_payload(&build);
        let decoded = build_from_proto_payload(&payload).expect("build decode should succeed");
        assert_eq!(decoded, build);
    }

    #[test]
    fn build_payload_rejects_unknown_state() {
        let payload = runtime_v2::BuildPayload {
            build_id: "build".to_string(),
            sandbox_id: "sbx".to_string(),
            state: "done".to_string(),
            result_digest: String::new(),
            started_at: 0,
            ended_at: 0,
        };

        let err = build_from_proto_payload(&payload).expect_err("unknown state should fail");
        assert_eq!(
            err,
            TranslationError::InvalidEnumValue {
                field: "build.state",
                value: "done".to_string(),
            }
        );
    }

    #[test]
    fn runtime_event_round_trip() {
        let event = Event {
            event_id: 42,
            ts: 1_700_000_001,
            scope: EventScope::Container,
            scope_id: "ctr-1".to_string(),
            event_type: "container.started".to_string(),
            payload: BTreeMap::from([("key".to_string(), "value".to_string())]),
            trace_id: Some("trace-1".to_string()),
        };

        let wire = event_to_proto_runtime_event(&event).expect("event encode should succeed");
        let decoded = event_from_proto_runtime_event(&wire).expect("event decode should succeed");
        assert_eq!(decoded, event);
    }

    #[test]
    fn runtime_event_rejects_negative_identifier() {
        let wire = runtime_v2::RuntimeEvent {
            id: -1,
            stack_name: "stack".to_string(),
            created_at: "0".to_string(),
            event_json: "{}".to_string(),
        };

        let err = event_from_proto_runtime_event(&wire).expect_err("negative id should fail");
        assert_eq!(
            err,
            TranslationError::InvalidValue {
                field: "runtime_event.id",
                value: "-1".to_string(),
            }
        );
    }

    #[test]
    fn runtime_event_rejects_invalid_json() {
        let wire = runtime_v2::RuntimeEvent {
            id: 1,
            stack_name: "stack".to_string(),
            created_at: "1700".to_string(),
            event_json: "not-json".to_string(),
        };

        let err = event_from_proto_runtime_event(&wire).expect_err("invalid json should fail");
        match err {
            TranslationError::InvalidJson {
                field: "runtime_event.event_json",
                ..
            } => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn runtime_capabilities_round_trip() {
        let capabilities = RuntimeCapabilities {
            fs_quick_checkpoint: true,
            vm_full_checkpoint: false,
            checkpoint_fork: true,
            docker_compat: true,
            compose_adapter: true,
            build_cache_export: false,
            gpu_passthrough: false,
            live_resize: true,
            shared_vm: true,
            stack_networking: true,
            container_logs: true,
        };

        let wire = runtime_capabilities_to_proto(capabilities);
        let decoded =
            runtime_capabilities_from_proto(&wire).expect("capability decode should succeed");
        assert_eq!(decoded, capabilities);
    }

    #[test]
    fn runtime_capabilities_reject_unknown_capability() {
        let wire = vec![runtime_v2::Capability {
            name: "future_capability".to_string(),
            enabled: true,
        }];

        let err =
            runtime_capabilities_from_proto(&wire).expect_err("unknown capability should fail");
        assert_eq!(
            err,
            TranslationError::UnknownCapability {
                name: "future_capability".to_string(),
            }
        );
    }

    #[test]
    fn runtime_capabilities_reject_duplicate_entries() {
        let wire = vec![
            runtime_v2::Capability {
                name: "docker_compat".to_string(),
                enabled: true,
            },
            runtime_v2::Capability {
                name: "docker_compat".to_string(),
                enabled: false,
            },
        ];

        let err =
            runtime_capabilities_from_proto(&wire).expect_err("duplicate capability should fail");
        assert_eq!(
            err,
            TranslationError::DuplicateCapability {
                name: "docker_compat".to_string(),
            }
        );
    }
}
