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

pub mod host_exports;
pub mod host_imports;
mod native_readiness;
mod readiness;
mod supervisor;
#[cfg(test)]
mod tests;
pub mod volumes;
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
    failure_with_details(metadata, code, message, BTreeMap::new())
}

/// A failure whose cause has fields worth reading, not only prose.
///
/// `operation` is always present, so a caller can tell an Up failure from any
/// other; `details` adds whatever the refusal itself can name — the contended
/// host port, the declaration that wanted it — and never overwrites it.
fn failure_with_details(
    metadata: &RequestMetadata,
    code: MachineErrorCode,
    message: impl ToString,
    details: BTreeMap<String, String>,
) -> MachineError {
    let mut all = details;
    all.insert("operation".into(), "up_environment".into());
    MachineError::new(
        code,
        message.to_string().chars().take(2048).collect(),
        metadata.request_id.clone(),
        all,
    )
}

/// Refuse one requested capability the checked-in matrix does not advertise.
///
/// The message names the Machine, the capability and the matrix status so the
/// refusal reads without a lookup, and `details` carry the same facts in wire
/// names so a caller never has to parse the prose.
fn unsupported_capability_failure(
    metadata: &RequestMetadata,
    host: HostSpec,
    machine: &MachineSpec,
    unadvertised: capability_matrix::UnadvertisedCapability,
) -> MachineError {
    let host_key = capability_matrix::host_key(host);
    let capability = unadvertised.capability.as_str();
    let status = unadvertised.status.as_str();
    let target = machine.target.os.as_str();
    let profile = machine.profile.as_str();
    MachineError::new(
        MachineErrorCode::UnsupportedOperation,
        format!(
            "Machine `{}` requests the `{capability}` capability, which {} marks {status} for {host_key} × {target} × {profile}; that pair has no negotiation path for it, so this Up performs no admission",
            machine.name,
            capability_matrix::MATRIX_PATH,
        ),
        metadata.request_id.clone(),
        BTreeMap::from([
            ("operation".into(), "up_environment".into()),
            ("machine".into(), machine.name.clone()),
            ("capability".into(), capability.into()),
            ("capability_status".into(), status.into()),
            ("host".into(), host_key),
            ("target".into(), target.into()),
            ("profile".into(), profile.into()),
        ]),
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
        validate_supported(&request, self.machine_target_resolver.host(), &metadata)?;
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
        // Minting happens BEFORE admission so the fork is one of the exact
        // `machine_ids` the admission records, and therefore one of the Machines
        // this Up boots. It is idempotent by address: a replay of the same
        // `--as` finds the fork already there and reconciles it rather than
        // minting a second warm copy nobody asked for.
        if let Some(fork) = request.fork.clone() {
            self.mint_machine_fork(&fork, &request, &metadata)?;
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

    /// Mint one fork of a declared Machine inside the selected Environment.
    ///
    /// Read-only resolution first, then one exact store mutation. Nothing here
    /// touches a disk: seeding happens once the fork's runtime store is pinned,
    /// in `supervise_up`.
    fn mint_machine_fork(
        &self,
        fork: &MachineForkRequest,
        request: &EnvironmentUpRequest,
        metadata: &RequestMetadata,
    ) -> Result<(), MachineError> {
        let (address, label) = fork
            .resolve()
            .map_err(|reason| failure(metadata, MachineErrorCode::ValidationError, reason))?;
        let project = self
            .with_state_store(|store| {
                store.load_project_state_snapshot(request.definition.project_id.as_str())
            })
            .map_err(|error| error.to_machine_error(metadata))?
            .ok_or_else(|| {
                failure(
                    metadata,
                    MachineErrorCode::NotFound,
                    "a fork needs an Environment to fork inside; run `vz up` once first",
                )
            })?;
        let selection = project
            .resolve_environment(&request.selection)
            .map_err(|error| failure(metadata, MachineErrorCode::NotFound, error.to_string()))?;
        let environment = project
            .environments
            .into_iter()
            .find(|environment| environment.environment_id == selection.environment_id)
            .ok_or_else(|| {
                failure(
                    metadata,
                    MachineErrorCode::StateConflict,
                    "selected Environment disappeared while resolving a fork",
                )
            })?;
        // Name or immutable id, exactly as `vz exec --machine` accepts, and
        // ambiguity fails closed listing the candidates rather than guessing.
        let candidates: Vec<_> = environment
            .machines
            .iter()
            .filter(|machine| {
                machine.machine_id.as_str() == fork.fork_from || machine.name == fork.fork_from
            })
            .collect();
        let parent = match candidates.as_slice() {
            [parent] => *parent,
            [] => {
                return Err(failure(
                    metadata,
                    MachineErrorCode::NotFound,
                    format!("no Machine named `{}` to fork from", fork.fork_from),
                ));
            }
            _ => {
                return Err(failure(
                    metadata,
                    MachineErrorCode::ValidationError,
                    format!(
                        "fork source `{}` is ambiguous (candidates: {})",
                        fork.fork_from,
                        candidates
                            .iter()
                            .take(32)
                            .map(|machine| format!("{} ({})", machine.name, machine.machine_id))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                ));
            }
        };
        if parent.name != address.machine {
            return Err(failure(
                metadata,
                MachineErrorCode::ValidationError,
                format!(
                    "`--as {}` does not name a fork of `{}`; expected `{}@{label}`",
                    fork.fork_as, parent.name, parent.name
                ),
            ));
        }
        crate::machine_fork::require_forkable(parent)
            .map_err(|error| failure(metadata, MachineErrorCode::UnsupportedOperation, error))?;
        // Already minted by an earlier Up with this address: reconcile it rather
        // than refusing, so `vz up --fork-from ... --as ...` is idempotent the
        // way every other Up is.
        if environment.machine_by_address(&address).is_some() {
            return Ok(());
        }
        self.with_state_store(|store| {
            store
                .fork_machine_in_environment(
                    environment.environment_id.as_str(),
                    &parent.machine_id,
                    &label,
                    current_unix_secs(),
                )
                .map(|_| ())
        })
        .map_err(|error| error.to_machine_error(metadata))
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
/// Declared `Network`, `Endpoint`, `NetworkAttachment`, `HostExport` and
/// `Volume` ownership is admitted here, which is only sound because those records are
/// minted once by `ProjectDefinition::instantiate_environment` alongside the
/// instances they name and are never added afterwards: the switch registry that
/// `install_environment_fabric` writes to, and the port-forward registry that
/// `start_port_forwarding` writes to, are both process-local and persist no
/// ownership. The declared half of the graph is therefore comparable as an exact
/// set at admission — the same comparison `environment_delete` makes before it
/// reclaims. Making it here as well, and not only in Delete, is what stops Up
/// booting every Machine of an Environment that could then never be deleted.
///
/// Records with no adapter behind them stay refused: `Socket`, `PortRange`,
/// `Credential`, `Fault`, `LegacySandbox` and any unrecognised `Other` kind.
/// `HostImport` is admitted now that the authenticated relay serves it; its
/// instances are minted by the same `instantiate_environment` call as the rest
/// of the declared fabric and are never added afterwards, so the exact-set
/// comparison below holds for them too.
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
    expected.extend(
        environment
            .host_exports
            .iter()
            .map(|export| OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::HostExport,
                resource_id: export.export_id.to_string(),
                environment_id: environment.environment_id.clone(),
                machine_id: Some(export.machine_id.clone()),
            }),
    );
    // Environment-scoped: `machine_id: None` is part of the expected record, so
    // a volume record that acquired a Machine does not match and is refused
    // rather than admitted with the wrong owner.
    expected.extend(environment.volumes.iter().map(|volume| OwnershipRecord {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        resource_kind: OwnedResourceKind::Volume,
        resource_id: volume.volume_id.to_string(),
        environment_id: environment.environment_id.clone(),
        machine_id: None,
    }));
    expected.extend(
        environment
            .host_imports
            .iter()
            .map(|import| OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::HostImport,
                resource_id: import.import_id.to_string(),
                environment_id: environment.environment_id.clone(),
                machine_id: Some(import.machine_id.clone()),
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
            // A fork's record is not part of the DECLARED set compared below,
            // for the reason it exists: the definition never names a fork, so a
            // fork can never be one of the exact instances that set accounts
            // for. It is admitted by kind and checked against its Machine by
            // `EnvironmentInstance::validate`, which requires exactly one such
            // record per forked Machine and none for a declared one.
            //
            // This is where "reconcile must not prune forks" is enforced for
            // Up: without this arm the `_ => false` default below would make
            // every Up of an Environment that has ever been forked refuse the
            // whole Environment, which is a harsher failure than pruning and
            // just as wrong.
            OwnedResourceKind::MachineFork => environment.machines.iter().any(|machine| {
                machine.fork.is_some()
                    && record.resource_id == machine.machine_id.as_str()
                    && record.machine_id.as_ref() == Some(&machine.machine_id)
            }),
            OwnedResourceKind::Network
            | OwnedResourceKind::Endpoint
            | OwnedResourceKind::NetworkAttachment
            | OwnedResourceKind::HostExport
            | OwnedResourceKind::Volume
            | OwnedResourceKind::HostImport => declared.insert(record),
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
            "Up declared-topology ownership does not account for exactly the persisted network, endpoint, attachment, host export and host import instances; no effects admitted",
        ));
    }
    Ok(())
}

fn validate_supported(
    request: &EnvironmentUpRequest,
    host: HostSpec,
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
    // Capability negotiation is the checked-in matrix's answer, not the
    // request's. `config/host-target-capabilities-v0.4.json` is the source of
    // truth for what is ACTIVE/DEV on this host × target × profile, and a
    // capability it marks PLANNED or NA has no negotiation path at all. Echoing
    // the request back would make discovery, `vz status`, help and the site copy
    // that read it all claim a capability the runtime does not have.
    //
    // It is refused HERE, before `reserve_environment_up_admission`, for the
    // same reason the storage rules above are: an Up that cannot be honoured
    // must not reserve identity or leave any durable trace behind it. The
    // refusal names the Machine and the exact capability, and carries both in
    // machine-readable `details`, so a caller can act on it without parsing
    // prose.
    for machine in &spec.machines {
        if let Some(unadvertised) = capability_matrix::first_unadvertised(
            host,
            machine.target.os,
            machine.profile,
            &machine.requested_capabilities,
        ) {
            return Err(unsupported_capability_failure(
                metadata,
                host,
                machine,
                unadvertised,
            ));
        }
    }
    // Declared networks, endpoints and workspace projections are applied:
    // `install_environment_fabric` starts every switch and mints every port, and
    // `workspace_projection` resolves every share, both before the boot loop.
    // What is refused below is only what no adapter implements, named one case
    // at a time so the refusal says which declaration it cannot serve.
    for machine in &spec.machines {
        if machine.workspace.is_none() {
            continue;
        }
        // Every mode is applied. `snapshot` is a private per-Machine clone of
        // the source, made with `clonefile` inside that Machine's own runtime
        // store; see `workspace_projection` for why that needs no
        // `OwnedResourceKind` of its own.
        //
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
    // Declared Environment-owned storage. A writable block volume attached to
    // more than one Machine is refused HERE, before
    // `reserve_environment_up_admission` runs, so no identity is reserved and no
    // image is allocated when the refusal fires: the "rejected before mutation"
    // half of the workspace-and-storage policy. The rule itself is the same one
    // `refuse_declared_writable_multi_attach` applies to workspace sources, and
    // both call `workspace_projection::first_writable_multi_attach`.
    if let Err(error) = volumes::refuse_unsupported_volumes(&request.definition.environment) {
        let code = match error {
            // A multi-attach is a declaration the product contract forbids, not
            // a capability the runtime lacks, so it is a validation failure and
            // will still be one when every adapter exists.
            volumes::VolumeError::WritableBlockMultiAttach { .. }
            | volumes::VolumeError::UnknownMachine { .. } => MachineErrorCode::ValidationError,
            _ => MachineErrorCode::UnsupportedOperation,
        };
        return Err(failure(metadata, code, error.to_string()));
    }
    // Host EXPORTS are applied: `resolve_environment_host_exports` joins each
    // persisted export identity to its declared ports and the boot loop hands
    // them to `start_port_forwarding`, whose listener is loopback-only by
    // construction. What cannot be served is refused here, one case at a time,
    // so the refusal says which declaration it could not carry.
    if let Err(error) =
        host_exports::refuse_unsupported_host_exports(&request.definition.environment)
    {
        let details = error.details();
        return Err(failure_with_details(
            metadata,
            MachineErrorCode::UnsupportedOperation,
            error.to_string(),
            details,
        ));
    }
    // Host IMPORTS are applied too, by the opposite mechanism. An import is a
    // guest-initiated stream that the host terminates against exactly one
    // stored `127.0.0.1` service: `host_imports::boot_import_grants` mints a
    // per-declaration credential, the boot loop installs a vsock terminator on
    // that Machine's own socket device and the matching loopback listeners in
    // its agent, and the host destination never crosses to the guest at all.
    // What cannot be served is refused here, one case at a time, so the refusal
    // says which declaration it could not carry.
    if let Err(error) =
        host_imports::refuse_unsupported_host_imports(&request.definition.environment)
    {
        return Err(failure(
            metadata,
            MachineErrorCode::UnsupportedOperation,
            error.to_string(),
        ));
    }
    // Egress is not applied. `EgressPolicy::Offline` is the only policy this Up
    // can honour, and the Environment edge does not change that: the edge
    // translates addresses between a client and an origin that are both inside
    // one Environment's fabric, and never towards a host outside it. A
    // non-offline Machine needs a translation towards the host's own network
    // and a policy deciding which destinations it may reach, and neither
    // exists. Refusing here rather than admitting a Machine whose declared
    // reachability is silently absent is the same rule the imports below follow.
    if let Some(machine) = spec
        .machines
        .iter()
        .find(|machine| machine.egress != EgressPolicy::Offline)
    {
        return Err(failure(
            metadata,
            MachineErrorCode::UnsupportedOperation,
            format!(
                "Machine `{}` declares a non-offline egress policy, whose host-facing translation and destination policy are not implemented; the Environment edge translates only between an Environment's own client and its own declared origin. This Up applies `offline` only and performs no admission",
                machine.name
            ),
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
