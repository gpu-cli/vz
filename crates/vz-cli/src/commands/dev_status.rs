//! `vz status` — read one project's persisted Developer Environment topology.

use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::fmt;

use clap::Args;
use serde::Serialize;
use tonic::Code;
use vz_cli::developer_environment_context::{
    VZ_ENVIRONMENT_ID, VZ_MACHINE_ID, discover_existing_git_workspace,
};
use vz_cli::project_definition::{DefinitionDiscoveryError, discover_project_definition};
use vz_runtime_contract::{
    CapabilitySet, EndpointProtocol, EnvironmentId, EnvironmentInstance,
    EnvironmentSelectionContext, EnvironmentSelectionSource, EnvironmentSelector, EnvironmentState,
    MAX_TOPOLOGY_SELECTION_CANDIDATES, MachineBackend, MachineCapability,
    MachineDockerContextDescriptor, MachineHealth, MachineId, MachineProfile, MachineState,
    NetworkKind, TargetSpec, TopologyCandidate, TopologyResolutionError,
};
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::{DaemonClientError, ProjectStateSnapshot};

use super::runtime_daemon::{connect_existing_daemon_for_state_db, default_state_db_path};

const STATUS_ERROR_EXIT_CODE: i32 = 2;
const STATUS_OUTPUT_SCHEMA_VERSION: u32 = 1;

/// Select and report persisted Developer Environment topology without mutation.
#[derive(Args, Debug)]
pub struct DevStatusArgs {
    /// Environment name or immutable ID.
    #[arg(long, value_name = "NAME_OR_ID", conflicts_with = "all")]
    pub environment: Option<String>,

    /// Machine name or immutable ID within the selected Environment.
    #[arg(long, value_name = "NAME_OR_ID", conflicts_with = "all")]
    pub machine: Option<String>,

    /// Report every Environment belonging to the discovered project.
    #[arg(long, conflicts_with_all = ["environment", "machine"])]
    pub all: bool,
}

#[derive(Debug, Serialize)]
pub struct StatusCommandError {
    code: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    candidates: Vec<TopologyCandidate>,
}

#[derive(Serialize)]
struct StatusErrorEnvelope<'a> {
    error: &'a StatusCommandError,
}

#[derive(Debug, Serialize)]
struct StatusOutput {
    schema_version: u32,
    request_id: String,
    topology_state_source: &'static str,
    definition_path: String,
    project_id: String,
    project_name: String,
    host: StatusHost,
    daemon: StatusDaemon,
    desired_definition_digest: String,
    persisted_definition_digest: String,
    definition_drift: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    selection_source: Option<EnvironmentSelectionSource>,
    environments: Vec<EnvironmentStatus>,
}

#[derive(Debug, Serialize)]
struct StatusHost {
    os: &'static str,
    arch: &'static str,
}

#[derive(Debug, Serialize)]
struct StatusDaemon {
    backend_name: String,
    version: String,
}

/// A bounded persisted-state projection, now including the Environment's shape.
///
/// The previous position was that networks, attachments and endpoints were
/// "endpoint material" and belonged outside routine status alongside workspace
/// paths and ownership internals. That reading does not survive the release
/// contract: `docs/developer-environments.md` says `status` reports "topology,
/// identities, targets, capabilities, health, endpoints, and a Docker context
/// for each Developer-profile Linux Machine", and an Environment whose networks
/// and endpoints are invisible cannot be read for what it is. The identity, the
/// shape and the declared service coordinates are the answer to "what is this
/// Environment"; they are declared by the user in `vz.json` and tell a reader
/// nothing they did not write.
///
/// What stays out, and why it is not the same category:
///
/// * `bindings` — host workspace paths. Filesystem layout of the machine the
///   command ran on, not Environment shape.
/// * `ownership` — the internal owned-resource graph: store paths, socket
///   paths, context names keyed by resource id. This is how the runtime finds
///   and reclaims what it made, and printing it invites reading a private
///   implementation path as a public address.
/// * `host_exports` / `host_imports` — the host boundary. Their persisted form
///   is identity and a name only: the bound loopback port and the import
///   credential are runtime state that is deliberately never persisted. Routine
///   status could therefore print a grant's name while being structurally
///   unable to say whether the grant is live or what it reaches, and a named
///   grant reads as an authorized one.
/// * `egress` — external reachability policy, and a Machine property rather
///   than Environment topology.
/// * `active_operation_id`, `legacy_migration` — lifecycle fencing and
///   migration provenance internals.
#[derive(Debug, Serialize)]
struct EnvironmentStatus {
    environment_id: String,
    name: String,
    state: EnvironmentState,
    definition_digest: String,
    lifecycle_generation: u64,
    machines: Vec<MachineStatus>,
    networks: Vec<NetworkStatus>,
    network_attachments: Vec<NetworkAttachmentStatus>,
    endpoints: Vec<EndpointStatus>,
}

/// One Environment-owned network, by identity and declared shape.
#[derive(Debug, Serialize)]
struct NetworkStatus {
    network_id: String,
    name: String,
    kind: NetworkKind,
    /// Absent means the runtime chose the range rather than the declaration.
    #[serde(skip_serializing_if = "Option::is_none")]
    cidr: Option<String>,
}

/// Which Machine holds a port on which network. This is the topology proper:
/// networks alone say what exists, and attachments say what is connected.
#[derive(Debug, Serialize)]
struct NetworkAttachmentStatus {
    attachment_id: String,
    machine_id: String,
    network_id: String,
}

/// One declared service coordinate inside the Environment.
///
/// The persisted record carries the protocol and port precisely so that "which
/// port is endpoint `api` on" is answerable from durable state; reporting them
/// is reporting the declaration, not a live listener or any credential.
#[derive(Debug, Serialize)]
struct EndpointStatus {
    endpoint_id: String,
    name: String,
    machine_id: String,
    network_id: String,
    protocol: EndpointProtocol,
    port: u16,
    /// The in-Environment hostname, when the declaration named one that is not
    /// simply the endpoint `name`.
    #[serde(skip_serializing_if = "Option::is_none")]
    hostname: Option<String>,
}

#[derive(Debug, Serialize)]
struct MachineStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    docker_context: Option<MachineDockerContextDescriptor>,
    /// A persisted lifecycle/capability projection, never a live Engine probe.
    #[serde(skip_serializing_if = "Option::is_none")]
    docker_context_availability: Option<&'static str>,
    machine_id: String,
    name: String,
    state: MachineState,
    profile: MachineProfile,
    target: TargetSpec,
    /// Persisted requests and negotiation results, never inferred from profile
    /// or the capabilities of a neighboring Machine or the daemon itself.
    requested_capabilities: CapabilitySet,
    negotiated_capabilities: CapabilitySet,
    /// What the answering daemon could see of this Machine's supervision when
    /// it replied: whether it still holds the live session it registered when
    /// it booted the Machine, and whether that session names the persisted
    /// runtime identity. It is the one field here that is not read out of the
    /// persisted record, and it is deliberately not a guest probe -- nothing is
    /// asked of the Machine itself, so a `supervised` Machine is one the daemon
    /// is still running, not one whose workload is known to be serving.
    health: MachineHealth,
    #[serde(skip_serializing_if = "Option::is_none")]
    backend: Option<MachineBackend>,
    #[serde(skip_serializing_if = "Option::is_none")]
    incarnation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    incarnation_generation: Option<u64>,
    /// Present exactly when this Machine is a fork of another in the same
    /// Environment.
    ///
    /// `vz status` is how an agent discovers what exists before it addresses
    /// anything, and a fork is only addressable as `<machine>@<label>`. Carrying
    /// the lineage rather than only the composed name means the parent is
    /// answerable too, which is what lets an agent tell "a fork of the Machine I
    /// wanted" from "a Machine whose name happens to contain an @".
    #[serde(skip_serializing_if = "Option::is_none")]
    fork: Option<MachineForkStatus>,
}

/// Where a forked Machine came from, as `vz status` reports it.
#[derive(Debug, Serialize)]
struct MachineForkStatus {
    parent_machine_id: String,
    parent_name: String,
    label: String,
}

impl EnvironmentStatus {
    /// Project one persisted Environment, joining each Machine to the health
    /// reading the daemon took for it in the same reply.
    ///
    /// A Machine the daemon returned no reading for becomes `Unobservable`
    /// rather than being silently omitted or defaulted to a healthy value. In
    /// practice the client refuses such a response outright; this is the second
    /// door on the same rule.
    fn project(
        environment: EnvironmentInstance,
        health: &BTreeMap<(String, String), MachineHealth>,
    ) -> Self {
        let environment_key = environment.environment_id.to_string();
        Self {
            environment_id: environment.environment_id.to_string(),
            name: environment.name,
            state: environment.state,
            definition_digest: environment.definition_digest,
            lifecycle_generation: environment.lifecycle_generation,
            machines: environment
                .machines
                .into_iter()
                .map(|machine| MachineStatus {
                    health: health
                        .get(&(environment_key.clone(), machine.machine_id.to_string()))
                        .copied()
                        .unwrap_or(MachineHealth::Unobservable),
                    docker_context_availability: machine.docker_context.as_ref().map(|_| {
                        if machine.state == MachineState::Stopped {
                            "stopped_unavailable"
                        } else if machine.state == MachineState::Ready
                            && machine
                                .negotiated_capabilities
                                .contains(MachineCapability::DockerEngine)
                        {
                            "persisted_ready_not_live_probed"
                        } else {
                            "persisted_unavailable"
                        }
                    }),
                    docker_context: machine.docker_context,
                    machine_id: machine.machine_id.to_string(),
                    name: machine.name,
                    state: machine.state,
                    profile: machine.profile,
                    target: machine.target,
                    requested_capabilities: machine.requested_capabilities,
                    negotiated_capabilities: machine.negotiated_capabilities,
                    backend: machine.backend,
                    incarnation_id: machine
                        .incarnation
                        .as_ref()
                        .map(|incarnation| incarnation.incarnation_id.to_string()),
                    incarnation_generation: machine
                        .incarnation
                        .map(|incarnation| incarnation.generation),
                    fork: machine.fork.map(|origin| MachineForkStatus {
                        parent_machine_id: origin.parent_machine_id.to_string(),
                        parent_name: origin.parent_name,
                        label: origin.label,
                    }),
                })
                .collect(),
            networks: environment
                .networks
                .into_iter()
                .map(|network| NetworkStatus {
                    network_id: network.network_id.to_string(),
                    name: network.name,
                    kind: network.kind,
                    cidr: network.cidr,
                })
                .collect(),
            network_attachments: environment
                .network_attachments
                .into_iter()
                .map(|attachment| NetworkAttachmentStatus {
                    attachment_id: attachment.attachment_id.to_string(),
                    machine_id: attachment.machine_id.to_string(),
                    network_id: attachment.network_id.to_string(),
                })
                .collect(),
            endpoints: environment
                .endpoints
                .into_iter()
                .map(|endpoint| EndpointStatus {
                    endpoint_id: endpoint.endpoint_id.to_string(),
                    name: endpoint.name,
                    machine_id: endpoint.machine_id.to_string(),
                    network_id: endpoint.network_id.to_string(),
                    protocol: endpoint.protocol,
                    port: endpoint.port,
                    hostname: endpoint.hostname,
                })
                .collect(),
        }
    }
}

#[derive(Debug)]
struct SelectedStatus {
    request_id: String,
    project_name: String,
    selection_source: Option<EnvironmentSelectionSource>,
    environments: Vec<EnvironmentInstance>,
    /// The daemon's readings for the whole reply, keyed by
    /// `(environment_id, machine_id)`. Selection narrows the Environments that
    /// are reported; it never re-takes or re-interprets an observation.
    machine_health: BTreeMap<(String, String), MachineHealth>,
}

impl StatusCommandError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            candidates: Vec::new(),
        }
    }

    fn with_candidates(
        code: &'static str,
        message: impl Into<String>,
        candidates: Vec<TopologyCandidate>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            candidates,
        }
    }

    pub const fn exit_code(&self) -> i32 {
        STATUS_ERROR_EXIT_CODE
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&StatusErrorEnvelope { error: self }).unwrap_or_else(|_| {
            "{\"error\":{\"code\":\"status_error\",\"message\":\"failed to serialize status error\"}}"
                .to_string()
        })
    }
}

impl fmt::Display for StatusCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StatusCommandError {}

pub async fn cmd_dev_status(args: DevStatusArgs, json: bool) -> Result<(), StatusCommandError> {
    let cwd = env::current_dir().map_err(|error| {
        StatusCommandError::new(
            "definition_read_failed",
            format!("cannot inspect the current directory: {error}"),
        )
    })?;
    let discovered = discover_project_definition(&cwd).map_err(definition_error)?;
    let desired_definition_digest = discovered.definition.digest().map_err(|error| {
        StatusCommandError::new(
            "invalid_definition",
            format!("cannot digest the discovered project definition: {error}"),
        )
    })?;

    reject_all_with_process_selectors(&args)?;
    let process_environment_id = process_environment_id(args.environment.is_some())?;
    let process_machine_id = process_machine_id(args.machine.is_some())?;
    let workspace_key = if args.all
        || args.environment.is_some()
        || process_environment_id.is_some()
    {
        None
    } else {
        discover_existing_git_workspace(&cwd)
            .map_err(|error| StatusCommandError::new("workspace_read_failed", error.to_string()))?
            .map(|workspace| workspace.workspace_key)
    };

    let state_db = default_state_db_path();
    let mut client = connect_existing_daemon_for_state_db(&state_db)
        .await
        .map_err(|_| {
            StatusCommandError::new(
                "daemon_unavailable",
                "no compatible runtime daemon is listening on the configured socket",
            )
        })?;
    let daemon = StatusDaemon {
        backend_name: client.handshake().backend_name.clone(),
        version: client.handshake().daemon_version.clone(),
    };
    let snapshot = client
        .get_project_state(runtime_v2::GetProjectStateRequest {
            metadata: None,
            project_id: discovered.definition.project_id.to_string(),
        })
        .await
        .map_err(project_state_error)?;

    if snapshot.project.definition.project_id != discovered.definition.project_id {
        return Err(StatusCommandError::new(
            "invalid_daemon_response",
            "daemon returned a different project than requested",
        ));
    }
    let persisted_definition_digest = snapshot.project.definition.digest().map_err(|_| {
        StatusCommandError::new(
            "invalid_daemon_response",
            "daemon returned a project definition that cannot be digested",
        )
    })?;

    let selected = select_status_environments(
        snapshot,
        &args,
        process_environment_id,
        process_machine_id,
        workspace_key,
    )?;
    let output = StatusOutput {
        schema_version: STATUS_OUTPUT_SCHEMA_VERSION,
        request_id: selected.request_id,
        topology_state_source: "persisted",
        definition_path: discovered.path.to_string_lossy().into_owned(),
        project_id: discovered.definition.project_id.to_string(),
        project_name: selected.project_name,
        host: StatusHost {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
        },
        daemon,
        definition_drift: desired_definition_digest != persisted_definition_digest,
        desired_definition_digest,
        persisted_definition_digest,
        selection_source: selected.selection_source,
        environments: selected
            .environments
            .into_iter()
            .map(|environment| EnvironmentStatus::project(environment, &selected.machine_health))
            .collect(),
    };

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&output).map_err(|error| {
                StatusCommandError::new(
                    "status_serialization_failed",
                    format!("cannot serialize status output: {error}"),
                )
            })?
        );
    } else {
        print_text_status(&output);
    }
    Ok(())
}

fn select_status_environments(
    snapshot: ProjectStateSnapshot,
    args: &DevStatusArgs,
    process_environment_id: Option<EnvironmentId>,
    process_machine_id: Option<MachineId>,
    workspace_key: Option<String>,
) -> Result<SelectedStatus, StatusCommandError> {
    let ProjectStateSnapshot {
        request_id,
        project,
        machine_health,
    } = snapshot;
    let machine_health: BTreeMap<(String, String), MachineHealth> = machine_health
        .into_iter()
        .map(|observation| {
            (
                (
                    observation.environment_id.to_string(),
                    observation.machine_id.to_string(),
                ),
                observation.health,
            )
        })
        .collect();
    let project_name = project.definition.name.clone();
    if args.all {
        let mut environments = project.environments;
        sort_environments(&mut environments);
        return Ok(SelectedStatus {
            request_id,
            project_name,
            selection_source: None,
            environments,
            machine_health,
        });
    }

    let context = EnvironmentSelectionContext {
        explicit: args.environment.clone().map(EnvironmentSelector::NameOrId),
        process_environment_id,
        workspace_key,
    };
    let selection = project
        .resolve_environment(&context)
        .map_err(environment_selection_error)?;
    let mut environment = project
        .environments
        .into_iter()
        .find(|environment| environment.environment_id == selection.environment_id)
        .ok_or_else(|| {
            StatusCommandError::new(
                "invalid_daemon_response",
                "selected Environment is absent from the returned project",
            )
        })?;

    if let Some(machine_id) =
        resolve_machine(&environment, args.machine.as_deref(), process_machine_id)?
    {
        environment
            .machines
            .retain(|machine| machine.machine_id == machine_id);
        environment
            .endpoints
            .retain(|endpoint| endpoint.machine_id == machine_id);
        // Attachments are per-Machine exactly as endpoints are, so a
        // Machine-scoped report must narrow them the same way; the networks
        // themselves stay whole because they belong to the Environment.
        environment
            .network_attachments
            .retain(|attachment| attachment.machine_id == machine_id);
        environment.ownership.retain(|record| {
            record
                .machine_id
                .as_ref()
                .is_none_or(|owner| owner == &machine_id)
        });
    }
    sort_environment_children(&mut environment);

    Ok(SelectedStatus {
        request_id,
        project_name,
        selection_source: Some(selection.source),
        environments: vec![environment],
        machine_health,
    })
}

fn reject_all_with_process_selectors(args: &DevStatusArgs) -> Result<(), StatusCommandError> {
    if !args.all {
        return Ok(());
    }
    let present = [VZ_ENVIRONMENT_ID, VZ_MACHINE_ID]
        .into_iter()
        .filter(|name| env::var_os(name).is_some())
        .collect::<Vec<_>>();
    if present.is_empty() {
        return Ok(());
    }
    Err(StatusCommandError::new(
        "selector_conflict",
        format!("--all cannot be combined with {}", present.join(" or ")),
    ))
}

fn process_environment_id(
    explicit_environment: bool,
) -> Result<Option<EnvironmentId>, StatusCommandError> {
    if explicit_environment {
        return Ok(None);
    }
    parse_process_id(VZ_ENVIRONMENT_ID, EnvironmentId::new)
}

fn process_machine_id(explicit_machine: bool) -> Result<Option<MachineId>, StatusCommandError> {
    if explicit_machine {
        return Ok(None);
    }
    parse_process_id(VZ_MACHINE_ID, MachineId::new)
}

fn parse_process_id<T>(
    variable: &'static str,
    parse: impl FnOnce(String) -> Result<T, vz_runtime_contract::TopologyValidationError>,
) -> Result<Option<T>, StatusCommandError> {
    let Some(raw) = env::var_os(variable) else {
        return Ok(None);
    };
    let value = os_string(raw, variable)?;
    parse(value).map(Some).map_err(|_| {
        StatusCommandError::new(
            "invalid_selector",
            format!("{variable} does not contain a valid immutable ID"),
        )
    })
}

fn os_string(value: OsString, variable: &str) -> Result<String, StatusCommandError> {
    value.into_string().map_err(|_| {
        StatusCommandError::new(
            "invalid_selector",
            format!("{variable} must contain valid UTF-8"),
        )
    })
}

fn resolve_machine(
    environment: &EnvironmentInstance,
    explicit: Option<&str>,
    process: Option<MachineId>,
) -> Result<Option<MachineId>, StatusCommandError> {
    if let Some(selector) = explicit {
        if selector.is_empty() || selector.trim() != selector {
            return Err(StatusCommandError::new(
                "invalid_selector",
                format!("invalid Machine selector `{selector}`"),
            ));
        }
        let matches = environment
            .machines
            .iter()
            .filter(|machine| {
                machine.machine_id.as_str() == selector || machine.name.as_str() == selector
            })
            .collect::<Vec<_>>();
        return match matches.as_slice() {
            [] => Err(StatusCommandError::new(
                "machine_not_found",
                format!("no Machine matched selector `{selector}`"),
            )),
            [machine] => Ok(Some(machine.machine_id.clone())),
            _ => Err(StatusCommandError::with_candidates(
                "ambiguous_machine",
                format!("selector `{selector}` matched multiple Machines"),
                bounded_machine_candidates(matches),
            )),
        };
    }

    if let Some(machine_id) = process {
        return environment
            .machines
            .iter()
            .find(|machine| machine.machine_id == machine_id)
            .map(|machine| Some(machine.machine_id.clone()))
            .ok_or_else(|| {
                StatusCommandError::new(
                    "machine_not_found",
                    format!(
                        "no Machine owned by Environment `{}` matched {} `{}`",
                        environment.environment_id, VZ_MACHINE_ID, machine_id
                    ),
                )
            });
    }

    Ok(None)
}

fn bounded_machine_candidates(
    machines: Vec<&vz_runtime_contract::MachineInstance>,
) -> Vec<TopologyCandidate> {
    let mut candidates = machines
        .into_iter()
        .map(|machine| TopologyCandidate {
            id: machine.machine_id.to_string(),
            name: machine.name.clone(),
        })
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.dedup();
    candidates.truncate(MAX_TOPOLOGY_SELECTION_CANDIDATES);
    candidates
}

fn definition_error(error: DefinitionDiscoveryError) -> StatusCommandError {
    StatusCommandError::new(error.code(), error.to_string())
}

fn environment_selection_error(error: TopologyResolutionError) -> StatusCommandError {
    let message = error.to_string();
    match error {
        TopologyResolutionError::InvalidSelector { .. } => {
            StatusCommandError::new("invalid_selector", message)
        }
        TopologyResolutionError::NotFound { .. } => {
            StatusCommandError::new("environment_not_found", message)
        }
        TopologyResolutionError::Ambiguous { candidates, .. } => {
            StatusCommandError::with_candidates("ambiguous_environment", message, candidates)
        }
        TopologyResolutionError::SelectionRequired { candidates, .. } => {
            StatusCommandError::with_candidates(
                "environment_selection_required",
                message,
                candidates,
            )
        }
    }
}

fn project_state_error(error: DaemonClientError) -> StatusCommandError {
    match &error {
        DaemonClientError::Grpc(status) if status.code() == Code::NotFound => {
            StatusCommandError::new(
                "project_not_found",
                "no persisted topology exists for the discovered project",
            )
        }
        DaemonClientError::IncompatibleProtocol { .. } => StatusCommandError::new(
            "invalid_daemon_response",
            "the runtime daemon returned invalid project topology",
        ),
        _ => StatusCommandError::new(
            "status_unavailable",
            "the runtime daemon could not return project topology",
        ),
    }
}

fn sort_environments(environments: &mut [EnvironmentInstance]) {
    environments.sort_by(|left, right| left.environment_id.cmp(&right.environment_id));
    for environment in environments {
        sort_environment_children(environment);
    }
}

fn sort_environment_children(environment: &mut EnvironmentInstance) {
    environment
        .bindings
        .sort_by(|left, right| left.binding_id.cmp(&right.binding_id));
    environment
        .machines
        .sort_by(|left, right| left.machine_id.cmp(&right.machine_id));
    environment
        .networks
        .sort_by(|left, right| left.network_id.cmp(&right.network_id));
    environment
        .endpoints
        .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
    environment
        .network_attachments
        .sort_by(|left, right| left.attachment_id.cmp(&right.attachment_id));
    environment.ownership.sort_by(|left, right| {
        left.resource_id
            .cmp(&right.resource_id)
            .then_with(|| left.machine_id.cmp(&right.machine_id))
    });
}

fn print_text_status(output: &StatusOutput) {
    println!("Project: {} ({})", output.project_name, output.project_id);
    println!(
        "Host: {}/{}; daemon backend={} version={}",
        output.host.os, output.host.arch, output.daemon.backend_name, output.daemon.version
    );
    println!("Topology state: {} snapshot", output.topology_state_source);
    println!("Definition: {}", output.definition_path);
    println!(
        "Definition digest: desired={} persisted={} drift={}",
        output.desired_definition_digest,
        output.persisted_definition_digest,
        output.definition_drift
    );
    if output.environments.is_empty() {
        println!("Environments: none");
        return;
    }
    for environment in &output.environments {
        println!(
            "Environment: {} ({}) [{:?}]",
            environment.name, environment.environment_id, environment.state
        );
        if environment.machines.is_empty() {
            println!("  Machines: none");
        }
        for machine in &environment.machines {
            println!(
                "  Machine: {} ({}) [{:?}] {:?}/{:?} profile={:?}",
                machine.name,
                machine.machine_id,
                machine.state,
                machine.target.os,
                machine.target.arch,
                machine.profile
            );
            if let Some(fork) = &machine.fork {
                println!(
                    "    Fork of {} ({}), label {}",
                    fork.parent_name, fork.parent_machine_id, fork.label
                );
            }
            println!(
                "    Supervision health (this daemon, not a guest probe): {}",
                machine.health
            );
            println!(
                "    Capabilities (persisted): requested={:?} negotiated={:?}",
                machine.requested_capabilities.capabilities,
                machine.negotiated_capabilities.capabilities
            );
            for (capability, reason) in &machine.negotiated_capabilities.unsupported {
                println!("    Unsupported {capability:?}: {reason}");
            }
            if let Some(context) = &machine.docker_context {
                println!(
                    "    Docker context (persisted, not live health): {} [{}]",
                    context.name,
                    machine
                        .docker_context_availability
                        .unwrap_or("persisted_unavailable")
                );
                println!(
                    "    Docker configuration: {} endpoint={}",
                    context.config_dir, context.endpoint
                );
            }
        }
        for network in &environment.networks {
            println!(
                "  Network: {} ({}) [{:?}] cidr={}",
                network.name,
                network.network_id,
                network.kind,
                network.cidr.as_deref().unwrap_or("runtime-derived")
            );
        }
        for attachment in &environment.network_attachments {
            println!(
                "  Attachment: {} machine={} network={}",
                attachment.attachment_id, attachment.machine_id, attachment.network_id
            );
        }
        for endpoint in &environment.endpoints {
            println!(
                "  Endpoint: {} ({}) machine={} network={} {:?}/{} hostname={}",
                endpoint.name,
                endpoint.endpoint_id,
                endpoint.machine_id,
                endpoint.network_id,
                endpoint.protocol,
                endpoint.port,
                endpoint.hostname.as_deref().unwrap_or(&endpoint.name)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use vz_runtime_contract::{
        Architecture, CapabilitySet, EnvironmentSpec, EnvironmentState, MachineHealthObservation,
        MachineInstance, MachineProfile, MachineResources, MachineSpec, MachineState,
        OperatingSystem, ProjectDefinition, ProjectId, ProjectState, TOPOLOGY_SCHEMA_VERSION,
        TargetSpec, WorkspaceBinding, WorkspaceBindingId,
    };

    fn environment_with_machines(id: &str, name: &str) -> EnvironmentInstance {
        let environment_id = EnvironmentId::new(id).unwrap();
        let machine = |id: &str, name: &str| MachineInstance {
            fork: None,
            docker_context: None,
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            machine_id: MachineId::new(id).unwrap(),
            environment_id: environment_id.clone(),
            name: name.to_string(),
            profile: MachineProfile::Developer,
            target: TargetSpec {
                os: OperatingSystem::Linux,
                arch: Architecture::Aarch64,
                image: "ubuntu:24.04".to_string(),
                version: None,
                channel: None,
                digest: None,
            },
            resources: MachineResources::default(),
            requested_capabilities: CapabilitySet::default(),
            negotiated_capabilities: CapabilitySet::default(),
            backend: None,
            incarnation: None,
            state: MachineState::Ready,
            runtime_identity: None,
            legacy_sandbox_id: None,
        };
        EnvironmentInstance {
            volumes: Vec::new(),
            network_attachments: Vec::new(),
            host_exports: Vec::new(),
            host_imports: Vec::new(),
            egress: Vec::new(),
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            environment_id: environment_id.clone(),
            project_id: ProjectId::new("prj-status").unwrap(),
            name: name.to_string(),
            definition_digest: "sha256:status".to_string(),
            state: EnvironmentState::Ready,
            lifecycle_generation: 1,
            active_operation_id: None,
            bindings: Vec::new(),
            machines: vec![machine("mch-two", "worker"), machine("mch-one", "app")],
            networks: Vec::new(),
            endpoints: Vec::new(),
            ownership: Vec::new(),
            legacy_migration: None,
            created_at: 1,
            updated_at: 1,
        }
    }

    /// One Environment with a declared private network, both Machines attached
    /// to it, and one endpoint on the first.
    fn environment_with_shape(id: &str, name: &str) -> EnvironmentInstance {
        let mut environment = environment_with_machines(id, name);
        let network_id = vz_runtime_contract::NetworkId::new("net-backend").unwrap();
        environment
            .networks
            .push(vz_runtime_contract::NetworkInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                network_id: network_id.clone(),
                environment_id: environment.environment_id.clone(),
                name: "backend".to_string(),
                kind: NetworkKind::Private,
                cidr: Some("10.85.0.0/24".to_string()),
            });
        for (index, machine) in environment.machines.iter().enumerate() {
            environment
                .network_attachments
                .push(vz_runtime_contract::NetworkAttachmentInstance {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    attachment_id: vz_runtime_contract::NetworkAttachmentId::new(format!(
                        "att-{index}"
                    ))
                    .unwrap(),
                    environment_id: environment.environment_id.clone(),
                    machine_id: machine.machine_id.clone(),
                    network_id: network_id.clone(),
                });
        }
        let served_by = environment.machines[0].machine_id.clone();
        environment
            .endpoints
            .push(vz_runtime_contract::EndpointInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                endpoint_id: vz_runtime_contract::EndpointId::new("end-probe").unwrap(),
                environment_id: environment.environment_id.clone(),
                machine_id: served_by,
                network_id,
                name: "probe".to_string(),
                protocol: EndpointProtocol::Tcp,
                port: 8080,
                hostname: None,
            });
        environment
    }

    #[test]
    fn status_reports_the_declared_networks_attachments_and_endpoints() {
        // Criterion 2 reads topology and endpoints out of `vz status --json`,
        // and every value below is one the definition declared: the projection
        // must carry them across rather than merely emit the keys.
        let environment = environment_with_shape("env-shape", "shape");
        let expected_machine = environment.machines[0].machine_id.to_string();
        let json = serde_json::to_value(project_without_readings(environment)).unwrap();
        assert_eq!(json["networks"].as_array().map(Vec::len), Some(1));
        assert_eq!(json["networks"][0]["network_id"], "net-backend");
        assert_eq!(json["networks"][0]["name"], "backend");
        assert_eq!(json["networks"][0]["kind"], "private");
        assert_eq!(json["networks"][0]["cidr"], "10.85.0.0/24");
        assert_eq!(
            json["network_attachments"].as_array().map(Vec::len),
            Some(2)
        );
        for attachment in json["network_attachments"].as_array().unwrap() {
            assert_eq!(attachment["network_id"], "net-backend");
        }
        assert_eq!(json["endpoints"].as_array().map(Vec::len), Some(1));
        assert_eq!(json["endpoints"][0]["endpoint_id"], "end-probe");
        assert_eq!(json["endpoints"][0]["name"], "probe");
        assert_eq!(
            json["endpoints"][0]["machine_id"],
            expected_machine.as_str()
        );
        assert_eq!(json["endpoints"][0]["network_id"], "net-backend");
        assert_eq!(json["endpoints"][0]["protocol"], "tcp");
        assert_eq!(json["endpoints"][0]["port"], 8080);
        // An absent hostname is skipped rather than emitted as null: the
        // endpoint resolves under its own name, which is already reported.
        assert!(json["endpoints"][0].get("hostname").is_none());
    }

    #[test]
    fn status_keeps_ownership_bindings_and_host_boundary_records_out_of_routine_output() {
        // The deliberate exclusions, asserted rather than left to the doc
        // comment: an aggregate field that starts being projected by accident
        // has to fail here.
        let environment = environment_with_shape("env-shape", "shape");
        let json = serde_json::to_value(project_without_readings(environment)).unwrap();
        let object = json.as_object().unwrap();
        for excluded in [
            "bindings",
            "ownership",
            "host_exports",
            "host_imports",
            "egress",
            "active_operation_id",
            "legacy_migration",
            "project_id",
            "created_at",
            "updated_at",
        ] {
            assert!(
                !object.contains_key(excluded),
                "{excluded} reached routine status output"
            );
        }
    }

    #[test]
    fn each_machine_reports_the_reading_taken_for_it_and_nothing_reaches_the_record() {
        // Health is a per-Machine join, so a single environment-wide value or a
        // reading applied to the wrong Machine has to be visible here.
        let environment = environment_with_shape("env-health", "health");
        let first = environment.machines[0].machine_id.to_string();
        let second = environment.machines[1].machine_id.to_string();
        let readings = BTreeMap::from([
            (
                (environment.environment_id.to_string(), first.clone()),
                MachineHealth::Supervised,
            ),
            (
                (environment.environment_id.to_string(), second.clone()),
                MachineHealth::Diverged,
            ),
        ]);
        let output = EnvironmentStatus::project(environment, &readings);
        let json = serde_json::to_value(&output).unwrap();
        for (index, machine) in output.machines.iter().enumerate() {
            let expected = if machine.machine_id == first {
                "supervised"
            } else {
                "diverged"
            };
            assert_eq!(json["machines"][index]["health"], expected);
        }
        // The reading is answer-time only. Nothing about it may appear on the
        // Machine's persisted projection under another name.
        assert!(json["machines"][0].get("runtime_identity").is_none());
    }

    #[test]
    fn a_machine_with_no_reading_is_unobservable_rather_than_assumed_healthy() {
        let environment = environment_with_shape("env-health", "health");
        let first = environment.machines[0].machine_id.to_string();
        let readings = BTreeMap::from([(
            (environment.environment_id.to_string(), first.clone()),
            MachineHealth::Supervised,
        )]);
        let output = EnvironmentStatus::project(environment, &readings);
        for machine in &output.machines {
            let expected = if machine.machine_id == first {
                MachineHealth::Supervised
            } else {
                MachineHealth::Unobservable
            };
            assert_eq!(machine.health, expected, "machine {}", machine.machine_id);
        }
    }

    #[test]
    fn a_reading_taken_in_another_environment_is_never_borrowed() {
        // The join is keyed by (environment_id, machine_id) because machine
        // ids are unique only within an Environment. A key that dropped the
        // Environment would let a sibling's reading answer for this one.
        let environment = environment_with_shape("env-health", "health");
        let machine = environment.machines[0].machine_id.to_string();
        let readings = BTreeMap::from([(
            ("env-somewhere-else".to_string(), machine.clone()),
            MachineHealth::Supervised,
        )]);
        let output = EnvironmentStatus::project(environment, &readings);
        for row in &output.machines {
            assert_eq!(row.health, MachineHealth::Unobservable);
        }
    }

    #[test]
    fn docker_context_status_is_exact_and_explicitly_persisted_not_live_health() {
        let mut environment = environment_with_machines("env-context", "one");
        let machine = &mut environment.machines[0];
        machine.docker_context = Some(MachineDockerContextDescriptor {
            schema_version: 1,
            owner: vz_runtime_contract::ResourceOwner {
                project_id: environment.project_id.clone(),
                environment_id: environment.environment_id.clone(),
                machine_id: Some(machine.machine_id.clone()),
            },
            name: "exact-context".into(),
            endpoint: "unix:///private/exact.sock".into(),
            config_dir: "/private/exact-client".into(),
            engine_id: "exact-engine".into(),
            incarnation_id: vz_runtime_contract::MachineIncarnationId::generate(),
            incarnation_generation: 1,
        });
        machine.negotiated_capabilities = CapabilitySet::new([MachineCapability::DockerEngine]);
        let ready = serde_json::to_value(project_without_readings(environment.clone())).unwrap();
        assert_eq!(
            ready["machines"][0]["docker_context"]["engine_id"],
            "exact-engine"
        );
        assert_eq!(
            ready["machines"][0]["docker_context_availability"],
            "persisted_ready_not_live_probed"
        );
        assert!(ready["machines"][1].get("docker_context").is_none());
        environment.machines[0].state = MachineState::Stopped;
        let stopped = serde_json::to_value(project_without_readings(environment.clone())).unwrap();
        assert_eq!(
            stopped["machines"][0]["docker_context"],
            ready["machines"][0]["docker_context"]
        );
        assert_eq!(
            stopped["machines"][0]["docker_context_availability"],
            "stopped_unavailable"
        );
        environment.machines[0].state = MachineState::Ready;
        environment.machines[0].negotiated_capabilities = CapabilitySet::default();
        let unverified = serde_json::to_value(project_without_readings(environment)).unwrap();
        assert_eq!(
            unverified["machines"][0]["docker_context_availability"],
            "persisted_unavailable"
        );
    }

    /// Project without any daemon reading, for the tests that are about the
    /// persisted projection alone. Every Machine then reports `unobservable`,
    /// which is exactly what "nobody took a reading" has to mean.
    fn project_without_readings(environment: EnvironmentInstance) -> EnvironmentStatus {
        EnvironmentStatus::project(environment, &BTreeMap::new())
    }

    fn status_args(environment: Option<&str>, machine: Option<&str>, all: bool) -> DevStatusArgs {
        DevStatusArgs {
            environment: environment.map(str::to_string),
            machine: machine.map(str::to_string),
            all,
        }
    }

    fn status_snapshot() -> ProjectStateSnapshot {
        let target = TargetSpec {
            os: OperatingSystem::Linux,
            arch: Architecture::Aarch64,
            image: "fixture:latest".to_string(),
            version: None,
            channel: None,
            digest: None,
        };
        let definition = ProjectDefinition {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: ProjectId::new("prj-status").unwrap(),
            name: "status-project".to_string(),
            environment: EnvironmentSpec {
                volumes: Vec::new(),
                host_exports: Vec::new(),
                host_imports: Vec::new(),
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                default_machine: None,
                machines: ["app", "worker"]
                    .into_iter()
                    .map(|name| MachineSpec {
                        networks: Vec::new(),
                        egress: Default::default(),
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        name: name.to_string(),
                        profile: MachineProfile::Developer,
                        target: target.clone(),
                        resources: MachineResources::default(),
                        requested_capabilities: CapabilitySet::default(),
                        workspace: None,
                    })
                    .collect(),
                networks: Vec::new(),
                endpoints: Vec::new(),
            },
        };
        let mut dev = environment_with_machines("env-zed", "dev");
        dev.bindings.push(WorkspaceBinding {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            binding_id: WorkspaceBindingId::new("wsp-dev").unwrap(),
            project_id: definition.project_id.clone(),
            environment_id: dev.environment_id.clone(),
            name: "dev-worktree".to_string(),
            workspace_key: "workspace-dev".to_string(),
            path_hint: None,
            slots: std::collections::BTreeSet::new(),
        });
        let mut staging = environment_with_machines("env-alpha", "staging");
        staging.bindings.push(WorkspaceBinding {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            binding_id: WorkspaceBindingId::new("wsp-staging").unwrap(),
            project_id: definition.project_id.clone(),
            environment_id: staging.environment_id.clone(),
            name: "staging-worktree".to_string(),
            workspace_key: "workspace-staging".to_string(),
            path_hint: None,
            slots: std::collections::BTreeSet::new(),
        });
        let project = ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments: vec![dev, staging],
        };
        let machine_health = project
            .environments
            .iter()
            .flat_map(|environment| {
                environment.machines.iter().map(|machine| {
                    MachineHealthObservation::new(
                        environment.environment_id.clone(),
                        machine.machine_id.clone(),
                        MachineHealth::Supervised,
                    )
                })
            })
            .collect();
        ProjectStateSnapshot {
            request_id: "req-status".to_string(),
            project,
            machine_health,
        }
    }

    #[test]
    fn explicit_machine_matches_name_or_id_and_unfiltered_reports_all() {
        let environment = environment_with_machines("env-status", "dev");
        assert_eq!(
            resolve_machine(&environment, Some("app"), None).unwrap(),
            Some(MachineId::new("mch-one").unwrap())
        );
        assert_eq!(
            resolve_machine(&environment, Some("mch-two"), None).unwrap(),
            Some(MachineId::new("mch-two").unwrap())
        );
        assert_eq!(resolve_machine(&environment, None, None).unwrap(), None);
    }

    #[test]
    fn status_preserves_per_machine_negotiation_without_inference_or_sibling_fallback() {
        use vz_runtime_contract::MachineCapability;

        let mut environment = environment_with_machines("env-status", "dev");
        let developer = &mut environment.machines[0];
        developer.requested_capabilities =
            CapabilitySet::new([MachineCapability::PosixExec, MachineCapability::Suspend]);
        developer.negotiated_capabilities = CapabilitySet::new([
            MachineCapability::PosixExec,
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ]);
        developer.negotiated_capabilities.unsupported.insert(
            MachineCapability::Suspend,
            "shared devices cannot be suspended atomically".to_string(),
        );
        let requested = developer.requested_capabilities.clone();
        let negotiated = developer.negotiated_capabilities.clone();

        let mut native = environment.machines[1].clone();
        native.machine_id = MachineId::new("mch-native").unwrap();
        native.name = "native".to_string();
        native.target.os = OperatingSystem::Macos;
        native.negotiated_capabilities = CapabilitySet::new([MachineCapability::PosixExec]);
        environment.machines.push(native);
        environment.machines[1].profile = MachineProfile::Hardened;
        environment.machines[1].negotiated_capabilities =
            CapabilitySet::new([MachineCapability::PosixExec]);

        let output = project_without_readings(environment);
        assert_eq!(output.machines[0].requested_capabilities, requested);
        assert_eq!(output.machines[0].negotiated_capabilities, negotiated);
        for machine in &output.machines[1..] {
            assert_eq!(
                machine.negotiated_capabilities,
                CapabilitySet::new([MachineCapability::PosixExec])
            );
        }
        let json = serde_json::to_value(&output).unwrap();
        assert_eq!(
            json["machines"][0]["negotiated_capabilities"]["unsupported"]["suspend"],
            "shared devices cannot be suspended atomically"
        );
        assert!(json["machines"][1].get("docker_context").is_none());
        assert!(json["machines"][2].get("docker_context").is_none());
    }

    #[test]
    fn status_does_not_claim_implicit_docker_before_negotiation() {
        let mut environment = environment_with_machines("env-status", "dev");
        for machine in &mut environment.machines {
            machine.state = MachineState::Creating;
        }
        let output = project_without_readings(environment);
        for machine in &output.machines {
            assert_eq!(machine.profile, MachineProfile::Developer);
            assert_eq!(machine.target.os, OperatingSystem::Linux);
            assert_eq!(machine.negotiated_capabilities, CapabilitySet::default());
        }
    }

    #[test]
    fn process_machine_must_belong_to_selected_environment() {
        let environment = environment_with_machines("env-status", "dev");
        let error = resolve_machine(
            &environment,
            None,
            Some(MachineId::new("mch-foreign").unwrap()),
        )
        .unwrap_err();
        assert_eq!(error.code, "machine_not_found");
    }

    #[test]
    fn status_error_json_is_stable_and_machine_readable() {
        let error = StatusCommandError::with_candidates(
            "ambiguous_environment",
            "choose one Environment",
            vec![TopologyCandidate {
                id: "env-one".to_string(),
                name: "one".to_string(),
            }],
        );
        assert_eq!(
            error.to_json(),
            "{\"error\":{\"code\":\"ambiguous_environment\",\"message\":\"choose one Environment\",\"candidates\":[{\"id\":\"env-one\",\"name\":\"one\"}]}}"
        );
        assert_eq!(error.exit_code(), STATUS_ERROR_EXIT_CODE);
    }

    #[test]
    fn incompatible_protocol_is_not_reported_as_transient_or_leaked() {
        let error = project_state_error(DaemonClientError::IncompatibleProtocol {
            reason: "malformed response containing sensitive-source-value".to_string(),
        });
        assert_eq!(error.code, "invalid_daemon_response");
        assert_eq!(
            error.message,
            "the runtime daemon returned invalid project topology"
        );
        assert!(!error.to_json().contains("sensitive-source-value"));
    }

    #[test]
    fn adapter_preserves_explicit_process_workspace_precedence() {
        let explicit = select_status_environments(
            status_snapshot(),
            &status_args(Some("dev"), None, false),
            Some(EnvironmentId::new("env-alpha").unwrap()),
            None,
            Some("workspace-staging".to_string()),
        )
        .unwrap();
        assert_eq!(
            explicit.selection_source,
            Some(EnvironmentSelectionSource::Explicit)
        );
        assert_eq!(explicit.environments[0].environment_id.as_str(), "env-zed");

        let process = select_status_environments(
            status_snapshot(),
            &status_args(None, None, false),
            Some(EnvironmentId::new("env-alpha").unwrap()),
            None,
            Some("workspace-dev".to_string()),
        )
        .unwrap();
        assert_eq!(
            process.selection_source,
            Some(EnvironmentSelectionSource::Process)
        );
        assert_eq!(process.environments[0].environment_id.as_str(), "env-alpha");

        let workspace = select_status_environments(
            status_snapshot(),
            &status_args(None, None, false),
            None,
            None,
            Some("workspace-dev".to_string()),
        )
        .unwrap();
        assert_eq!(
            workspace.selection_source,
            Some(EnvironmentSelectionSource::Workspace)
        );
        assert_eq!(workspace.environments[0].environment_id.as_str(), "env-zed");
    }

    #[test]
    fn stale_explicit_or_process_environment_never_falls_back() {
        let explicit_error = select_status_environments(
            status_snapshot(),
            &status_args(Some("missing"), None, false),
            Some(EnvironmentId::new("env-alpha").unwrap()),
            None,
            Some("workspace-dev".to_string()),
        )
        .unwrap_err();
        assert_eq!(explicit_error.code, "environment_not_found");

        let process_error = select_status_environments(
            status_snapshot(),
            &status_args(None, None, false),
            Some(EnvironmentId::new("env-missing").unwrap()),
            None,
            Some("workspace-dev".to_string()),
        )
        .unwrap_err();
        assert_eq!(process_error.code, "environment_not_found");
    }

    #[test]
    fn ambiguous_workspace_is_bounded_and_does_not_pick_a_candidate() {
        let mut snapshot = status_snapshot();
        let staging = &mut snapshot.project.environments[1];
        staging.bindings.push(WorkspaceBinding {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            binding_id: WorkspaceBindingId::new("wsp-ambiguous").unwrap(),
            project_id: snapshot.project.definition.project_id.clone(),
            environment_id: staging.environment_id.clone(),
            name: "ambiguous-worktree".to_string(),
            workspace_key: "workspace-dev".to_string(),
            path_hint: None,
            slots: std::collections::BTreeSet::new(),
        });
        let error = select_status_environments(
            snapshot,
            &status_args(None, None, false),
            None,
            None,
            Some("workspace-dev".to_string()),
        )
        .unwrap_err();
        assert_eq!(error.code, "ambiguous_environment");
        assert_eq!(error.candidates.len(), 2);
        assert!(error.candidates.len() <= MAX_TOPOLOGY_SELECTION_CANDIDATES);
    }

    #[test]
    fn all_lists_environments_and_children_deterministically() {
        let selected = select_status_environments(
            status_snapshot(),
            &status_args(None, None, true),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(selected.selection_source, None);
        assert_eq!(
            selected.environments[0].environment_id.as_str(),
            "env-alpha"
        );
        assert_eq!(selected.environments[1].environment_id.as_str(), "env-zed");
        for environment in selected.environments {
            assert_eq!(environment.machines[0].machine_id.as_str(), "mch-one");
            assert_eq!(environment.machines[1].machine_id.as_str(), "mch-two");
        }
    }

    #[test]
    fn machine_name_id_collision_is_rejected_as_ambiguous() {
        let mut snapshot = status_snapshot();
        snapshot.project.environments[0].machines[0].name = "mch-one".to_string();
        let error = select_status_environments(
            snapshot,
            &status_args(Some("dev"), Some("mch-one"), false),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, "ambiguous_machine");
        assert_eq!(error.candidates.len(), 2);
    }
}
