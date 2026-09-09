//! Retained Up supervisor. Observation cancellation never drops boot effects.
//! A deadline is an observation/next-effect bound, not permission to abandon an
//! in-flight backend future. Uncertain effects keep their original ownership.
use crate::machine_runtime_activation::MachineRuntimeActivation;
use crate::{RuntimeDaemon, current_unix_secs};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedMutexGuard, watch};
use vz_runtime_contract::*;
use vz_stack::StackError;

mod native_readiness;
mod readiness;
mod supervisor;
#[cfg(test)]
mod tests;
pub mod workspace_projection;

/// Exact, authorized boot boundary for trusted backend instrumentation. This
/// observer can delay dispatch but cannot supply activation/readiness evidence.
#[derive(Debug, Clone)]
pub struct EnvironmentUpBootBoundary {
    pub admission: EnvironmentUpAdmission,
    pub operation: EnvironmentLifecycleOperation,
    pub machine_id: MachineId,
    pub owner: ResourceOwner,
}

#[tonic::async_trait]
pub trait EnvironmentUpBootObserver: Send + Sync {
    async fn before_dispatch(&self, boundary: &EnvironmentUpBootBoundary);
}

#[derive(Default)]
pub(crate) struct EnvironmentUpRuns(Mutex<HashMap<String, Arc<UpRun>>>);

struct UpRun {
    admission: EnvironmentUpAdmission,
    progress: watch::Sender<EnvironmentUpProgress>,
    fence: Mutex<Option<Arc<OwnedMutexGuard<()>>>>,
    // Failed registration must not drop the only activation reader.
    uncertain: Mutex<Vec<Arc<MachineRuntimeActivation>>>,
}

impl UpRun {
    fn preparing(&self, preparation: EnvironmentPreparationProgress) {
        self.progress.send_modify(|event| {
            if event.completion.is_some() {
                return;
            }
            event.sequence += 1;
            event.phase = "preparing".into();
            event.preparation = Some(preparation);
        });
    }

    fn publish(
        &self,
        phase: &str,
        operation: Option<EnvironmentLifecycleOperation>,
        completion: Option<EnvironmentUpCompletion>,
    ) {
        self.progress.send_modify(|event| {
            if event.completion.is_some() {
                return;
            }
            event.sequence += 1;
            event.phase = phase.into();
            event.preparation = None;
            event.operation = operation;
            event.completion = completion;
        });
    }
}

fn failure(
    metadata: &RequestMetadata,
    code: MachineErrorCode,
    message: impl ToString,
) -> MachineError {
    MachineError::new(
        code,
        message.to_string().chars().take(2048).collect(),
        metadata.request_id.clone(),
        BTreeMap::from([("operation".into(), "up_environment".into())]),
    )
}

impl RuntimeDaemon {
    /// Install trusted instrumentation before publishing this daemon owner.
    /// There is no RPC/CLI setting and no alternate/fake readiness provider.
    pub fn with_environment_up_boot_observer(
        mut self,
        observer: Arc<dyn EnvironmentUpBootObserver>,
    ) -> Self {
        self.environment_up_observer = Some(observer);
        self
    }
    /// Admission authorizes exact generated IDs in the transaction that creates
    /// them. Exact retries observe one retained run or its immutable receipt.
    pub async fn up_environment(
        self: &Arc<Self>,
        mut request: EnvironmentUpRequest,
        metadata: RequestMetadata,
    ) -> Result<watch::Receiver<EnvironmentUpProgress>, MachineError> {
        if request.selection.explicit.is_some() {
            request.selection.process_environment_id = None;
        }
        let hash = request
            .request_hash()
            .map_err(|error| failure(&metadata, MachineErrorCode::ValidationError, error))?;
        validate_supported(&request, &metadata)?;
        let request_id = metadata.request_id.as_deref().unwrap_or_default();
        let key = metadata.idempotency_key.as_deref().unwrap_or_default();
        if [request_id, key].iter().any(|value| {
            value.is_empty()
                || value.len() > 256
                || value.trim() != *value
                || value.chars().any(char::is_control)
        }) {
            return Err(failure(
                &metadata,
                MachineErrorCode::ValidationError,
                "Up requires bounded request and idempotency IDs without control characters",
            ));
        }
        let mut runs = self
            .environment_up_runs
            .0
            .lock()
            .map_err(|error| failure(&metadata, MachineErrorCode::InternalError, error))?;
        if runs.len() >= 1024 && !runs.contains_key(key) {
            return Err(failure(
                &metadata,
                MachineErrorCode::BackendUnavailable,
                "Up supervisor capacity exhausted; no admission performed",
            ));
        }
        let admission = self
            .with_state_store(|store| {
                store.reserve_environment_up_admission(
                    &request.definition,
                    &request.selection,
                    request_id,
                    key,
                    &hash,
                    current_unix_secs(),
                    |environment| self.authorize_up(&metadata, environment),
                )
            })
            .map_err(|error| error.to_machine_error(&metadata))?;
        if let Some(existing) = runs.get(key) {
            if existing.admission != admission {
                return Err(failure(
                    &metadata,
                    MachineErrorCode::StateConflict,
                    "Up run admission mismatch",
                ));
            }
            return Ok(existing.progress.subscribe());
        }
        let completion = self
            .with_state_store(|store| store.load_environment_up_completion(key))
            .map_err(|error| error.to_machine_error(&metadata))?;
        let initial = EnvironmentUpProgress {
            preparation: None,
            schema_version: 1,
            sequence: 0,
            admission: admission.clone(),
            phase: if completion.is_some() {
                "terminal"
            } else {
                "admitted"
            }
            .into(),
            operation: completion
                .as_ref()
                .and_then(|value| value.operation.clone()),
            completion,
        };
        let (progress, receiver) = watch::channel(initial);
        if receiver.borrow().completion.is_some() {
            return Ok(receiver);
        }
        let run = Arc::new(UpRun {
            admission,
            progress,
            fence: Mutex::new(None),
            uncertain: Mutex::new(Vec::new()),
        });
        runs.insert(key.into(), Arc::clone(&run));
        let daemon = Arc::clone(self);
        tokio::spawn(async move {
            daemon.supervise_up(request, metadata, run).await;
        });
        Ok(receiver)
    }

    fn authorize_up(
        &self,
        metadata: &RequestMetadata,
        environment: &EnvironmentInstance,
    ) -> Result<(), StackError> {
        if environment.legacy_migration.is_some() {
            return Err(StackError::Machine {
                code: MachineErrorCode::UnsupportedOperation,
                message: "Up cannot apply a legacy Sandbox migration".into(),
            });
        }
        authorize_ownership(environment)?;
        let mut machine_ids = environment
            .machines
            .iter()
            .map(|machine| machine.machine_id.clone())
            .collect::<Vec<_>>();
        machine_ids.sort();
        let scope = TopologyAuthorization {
            operation: TopologyOperation::Up,
            project_id: environment.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            definition_digest: environment.definition_digest.clone(),
            machine_ids,
        };
        match self.policy_hook.evaluate_topology(&scope, metadata) {
            Ok(PolicyDecision::Allow) => Ok(()),
            Ok(PolicyDecision::Deny { reason }) => Err(StackError::Machine {
                code: MachineErrorCode::PolicyDenied,
                message: reason,
            }),
            Err(error) => Err(StackError::Machine {
                code: MachineErrorCode::BackendUnavailable,
                message: format!("topology policy failed: {error}"),
            }),
        }
    }
}

/// Refuse an Environment whose persisted ownership graph names a resource this
/// Up cannot serve, or a declared-fabric record with no instance behind it.
///
/// Declared `Network`, `Endpoint` and `NetworkAttachment` ownership is admitted
/// here (vz-9vv.7), which is only sound because those records are minted once by
/// `ProjectDefinition::instantiate_environment` alongside the instances they
/// name and are never added afterwards: the switch registry that
/// `install_environment_fabric` writes to is process-local and persists no
/// ownership. The fabric half of the graph is therefore comparable as an exact
/// set at admission — the same comparison `environment_delete` makes before it
/// reclaims. Making it here as well, and not only in Delete, is what stops Up
/// booting every Machine of an Environment that could then never be deleted.
///
/// Records with no adapter behind them stay refused: `HostExport`, `HostImport`,
/// `Socket`, `PortRange`, `Credential`, `Fault`, `LegacySandbox` and any
/// unrecognised `Other` kind.
fn authorize_ownership(environment: &EnvironmentInstance) -> Result<(), StackError> {
    fn unsupported(message: &str) -> StackError {
        StackError::Machine {
            code: MachineErrorCode::UnsupportedOperation,
            message: message.into(),
        }
    }
    let mut expected: Vec<OwnershipRecord> = environment
        .networks
        .iter()
        .map(|network| OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::Network,
            resource_id: network.network_id.to_string(),
            environment_id: environment.environment_id.clone(),
            machine_id: None,
        })
        .collect();
    expected.extend(
        environment
            .endpoints
            .iter()
            .map(|endpoint| OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::Endpoint,
                resource_id: endpoint.endpoint_id.to_string(),
                environment_id: environment.environment_id.clone(),
                machine_id: Some(endpoint.machine_id.clone()),
            }),
    );
    expected.extend(
        environment
            .network_attachments
            .iter()
            .map(|attachment| OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::NetworkAttachment,
                resource_id: attachment.attachment_id.to_string(),
                environment_id: environment.environment_id.clone(),
                machine_id: Some(attachment.machine_id.clone()),
            }),
    );
    // Two instances sharing one identity would emit one record twice. That is
    // state corruption, and it is refused rather than deduplicated: a silent
    // dedup would leave the slot the duplicate vacated free for an unaccounted
    // resource to occupy, exactly as `environment_delete` reasons.
    let expected_set = expected.iter().collect::<BTreeSet<_>>();
    if expected_set.len() != expected.len() {
        return Err(unsupported(
            "Up fabric ownership plan minted one resource identity twice; no effects admitted",
        ));
    }
    let mut declared = BTreeSet::new();
    for record in &environment.ownership {
        let supported = match &record.resource_kind {
            OwnedResourceKind::Machine
            | OwnedResourceKind::Incarnation
            | OwnedResourceKind::Disk => true,
            OwnedResourceKind::Network
            | OwnedResourceKind::Endpoint
            | OwnedResourceKind::NetworkAttachment => declared.insert(record),
            OwnedResourceKind::DockerContext => environment.machines.iter().any(|machine| {
                machine.docker_context.as_ref().is_some_and(|context| {
                    context.name == record.resource_id
                        && context.owner.environment_id == record.environment_id
                        && context.owner.machine_id == record.machine_id
                        && context.owner.project_id == environment.project_id
                        && context.owner.environment_id == environment.environment_id
                        && context.owner.machine_id.as_ref() == Some(&machine.machine_id)
                })
            }),
            OwnedResourceKind::Other(kind) => {
                kind == "machine_runtime_store" || kind == "runtime_vm"
            }
            _ => false,
        };
        if !supported {
            return Err(unsupported(
                "Up cannot apply unknown, unsupported, or repeated existing topology resources",
            ));
        }
    }
    if declared != expected_set {
        return Err(unsupported(
            "Up declared-fabric ownership does not account for exactly the persisted network, endpoint and attachment instances; no effects admitted",
        ));
    }
    Ok(())
}

fn validate_supported(
    request: &EnvironmentUpRequest,
    metadata: &RequestMetadata,
) -> Result<(), MachineError> {
    let spec = &request.definition.environment;
    if spec.machines.is_empty() || spec.machines.len() > 128 {
        return Err(failure(
            metadata,
            MachineErrorCode::ValidationError,
            "Up requires 1..128 Machines",
        ));
    }
    // Declared networks, endpoints and workspace projections are applied:
    // `install_environment_fabric` starts every switch and mints every port, and
    // `workspace_projection` resolves every share, both before the boot loop.
    // What is refused below is only what no adapter implements, named one case
    // at a time so the refusal says which declaration it cannot serve.
    //
    // `NetworkKind::SimulatedPublic` is a private fabric plus external egress,
    // and no egress path off the fabric exists. The shared vmnet NAT segment is
    // disqualified by the product contract (a NAT alias is not authorization),
    // so a per-Environment gateway has to be built first (vz-9vv.6). The switch
    // planner already reserves the address such a gateway would take, but
    // nothing answers on it.
    if let Some(network) = spec
        .networks
        .iter()
        .find(|network| network.kind == NetworkKind::SimulatedPublic)
    {
        return Err(failure(
            metadata,
            MachineErrorCode::UnsupportedOperation,
            format!(
                "network `{}` declares kind `simulated_public`, whose per-Environment egress gateway is not implemented; this Up applies `private` networks only and performs no admission",
                network.name
            ),
        ));
    }
    for machine in &spec.machines {
        let Some(workspace) = &machine.workspace else {
            continue;
        };
        // Snapshot has neither a directory-tree copy primitive (only the
        // single-file `clone_file` in `vz-macos-provision`) nor an
        // `OwnedResourceKind` variant, so a snapshot Delete could neither
        // reclaim nor account for would leak on the first successful Up.
        if workspace.mode == WorkspaceProjectionMode::Snapshot {
            return Err(failure(
                metadata,
                MachineErrorCode::UnsupportedOperation,
                format!(
                    "Machine `{}` requests `snapshot` workspace projection, which has no directory-copy primitive and no owned-resource kind; this Up applies `read_write` and `read_only` only and performs no admission",
                    machine.name
                ),
            ));
        }
        // A projection is carried by a VirtioFS share whose `vz-mount-{N}` tag
        // `linux/initramfs/init` bind-mounts. `boot_or_inspect_machine` hands
        // the native macOS backend no `StackResourceHint` at all, so admitting
        // one there would boot a Machine whose declared workspace silently
        // never appears. Hardened is the restricted profile and declares none
        // of this topology, matching the contract's refusal of network
        // attachments on it.
        if machine.target.os != OperatingSystem::Linux
            || machine.profile != MachineProfile::Developer
        {
            return Err(failure(
                metadata,
                MachineErrorCode::UnsupportedOperation,
                format!(
                    "Machine `{}` declares a workspace projection, which only a Developer Linux Machine carries; this Up performs no admission",
                    machine.name
                ),
            ));
        }
    }
    // Two Machines sharing one host source with either of them writable is the
    // "no silent multi-attach" rule of the workspace and storage policy. It is
    // enforced here, at admission, because the supervisor reserves a durable
    // workspace binding before it resolves shares: refusing only at resolution
    // would already have mutated state. `resolve_environment_workspace_mounts`
    // repeats the rule over canonicalised paths to catch two declarations that
    // become one directory through a symlink.
    if let Err(error) =
        workspace_projection::refuse_declared_writable_multi_attach(&request.definition.environment)
    {
        return Err(failure(
            metadata,
            MachineErrorCode::ValidationError,
            error.to_string(),
        ));
    }
    // Host relays and non-offline egress are declarable but not yet applied by
    // any adapter. Admitting them would start a Machine that silently lacks the
    // boundary its definition asks for, so they are refused here until the
    // relay and egress adapters exist.
    if !spec.host_exports.is_empty()
        || !spec.host_imports.is_empty()
        || spec
            .machines
            .iter()
            .any(|machine| machine.egress != EgressPolicy::Offline)
    {
        return Err(failure(
            metadata,
            MachineErrorCode::UnsupportedOperation,
            "declared host import/export and non-offline egress adapters remain required; this Up cannot apply them and performs no admission",
        ));
    }
    for machine in &spec.machines {
        if !matches!(
            machine.target.os,
            OperatingSystem::Linux | OperatingSystem::Macos
        ) || machine.target.arch != Architecture::Aarch64
        {
            return Err(failure(
                metadata,
                MachineErrorCode::UnsupportedOperation,
                "this Up adapter supports Linux and native macOS ARM64 on Apple silicon only",
            ));
        }
    }
    if request
        .path_hint
        .as_ref()
        .is_some_and(|value| value.len() > 4096 || value.chars().any(char::is_control))
    {
        return Err(failure(
            metadata,
            MachineErrorCode::ValidationError,
            "invalid bounded workspace diagnostic path",
        ));
    }
    Ok(())
}
