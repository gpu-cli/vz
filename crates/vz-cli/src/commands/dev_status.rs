//! `vz status` — read one project's persisted Developer Environment topology.

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
    CapabilitySet, EnvironmentId, EnvironmentInstance, EnvironmentSelectionContext,
    EnvironmentSelectionSource, EnvironmentSelector, EnvironmentState,
    MAX_TOPOLOGY_SELECTION_CANDIDATES, MachineBackend, MachineId, MachineProfile, MachineState,
    TargetSpec, TopologyCandidate, TopologyResolutionError,
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

/// Deliberately bounded persisted-state projection. Workspace paths, ownership
/// internals, and endpoint material are not part of routine CLI status output.
#[derive(Debug, Serialize)]
struct EnvironmentStatus {
    environment_id: String,
    name: String,
    state: EnvironmentState,
    definition_digest: String,
    lifecycle_generation: u64,
    machines: Vec<MachineStatus>,
}

#[derive(Debug, Serialize)]
struct MachineStatus {
    machine_id: String,
    name: String,
    state: MachineState,
    profile: MachineProfile,
    target: TargetSpec,
    /// Persisted requests and negotiation results, never inferred from profile
    /// or the capabilities of a neighboring Machine or the daemon itself.
    requested_capabilities: CapabilitySet,
    negotiated_capabilities: CapabilitySet,
    #[serde(skip_serializing_if = "Option::is_none")]
    backend: Option<MachineBackend>,
    #[serde(skip_serializing_if = "Option::is_none")]
    incarnation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    incarnation_generation: Option<u64>,
}

impl From<EnvironmentInstance> for EnvironmentStatus {
    fn from(environment: EnvironmentInstance) -> Self {
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
            .map(EnvironmentStatus::from)
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
    } = snapshot;
    let project_name = project.definition.name.clone();
    if args.all {
        let mut environments = project.environments;
        sort_environments(&mut environments);
        return Ok(SelectedStatus {
            request_id,
            project_name,
            selection_source: None,
            environments,
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
            println!(
                "    Capabilities (persisted): requested={:?} negotiated={:?}",
                machine.requested_capabilities.capabilities,
                machine.negotiated_capabilities.capabilities
            );
            for (capability, reason) in &machine.negotiated_capabilities.unsupported {
                println!("    Unsupported {capability:?}: {reason}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vz_runtime_contract::{
        Architecture, CapabilitySet, EnvironmentSpec, EnvironmentState, MachineInstance,
        MachineProfile, MachineResources, MachineSpec, MachineState, OperatingSystem,
        ProjectDefinition, ProjectId, ProjectState, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
        WorkspaceBinding, WorkspaceBindingId,
    };

    fn environment_with_machines(id: &str, name: &str) -> EnvironmentInstance {
        let environment_id = EnvironmentId::new(id).unwrap();
        let machine = |id: &str, name: &str| MachineInstance {
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
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                machines: ["app", "worker"]
                    .into_iter()
                    .map(|name| MachineSpec {
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
        });
        ProjectStateSnapshot {
            request_id: "req-status".to_string(),
            project: ProjectState {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                definition,
                environments: vec![dev, staging],
            },
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

        let output = EnvironmentStatus::from(environment);
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
        let output = EnvironmentStatus::from(environment);
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
