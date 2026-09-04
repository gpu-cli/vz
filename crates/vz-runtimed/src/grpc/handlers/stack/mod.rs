//! Stack gRPC handler support code shared by stack endpoint RPC methods.
//!
//! Consolidates request parsing/mapping helpers and runtime bridge adapters used by
//! `stack::rpc` endpoint implementations.

use super::super::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use vz_stack::{
    Action, ComposeBuildSpec, ContainerLogs, ContainerRuntime, ExecutionResult,
    OrchestrationConfig, ServiceObservedState, ServicePhase, StackExecutor, StackOrchestrator,
    StackSpec, VolumeManager, apply, collect_compose_build_specs_with_dir, parse_compose_with_dir,
    plan_apply,
};

const STACK_BUILD_POLL_INTERVAL: Duration = Duration::from_millis(250);
const STACK_BUILD_TIMEOUT: Duration = Duration::from_secs(60 * 60);

#[tonic::async_trait]
trait ComposeBuildRunner {
    async fn start_build(
        &self,
        sandbox_id: &str,
        build_spec: BuildSpec,
    ) -> Result<vz_runtime_contract::Build, vz_runtime_contract::RuntimeError>;
    async fn get_build(
        &self,
        build_id: &str,
    ) -> Result<vz_runtime_contract::Build, vz_runtime_contract::RuntimeError>;
    async fn cancel_build(
        &self,
        build_id: &str,
    ) -> Result<vz_runtime_contract::Build, vz_runtime_contract::RuntimeError>;
}

struct DaemonBuildRunner {
    daemon: Arc<RuntimeDaemon>,
}

impl DaemonBuildRunner {
    fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

#[tonic::async_trait]
impl ComposeBuildRunner for DaemonBuildRunner {
    async fn start_build(
        &self,
        sandbox_id: &str,
        build_spec: BuildSpec,
    ) -> Result<vz_runtime_contract::Build, vz_runtime_contract::RuntimeError> {
        self.daemon
            .manager()
            .start_build(sandbox_id, build_spec, None)
            .await
    }

    async fn get_build(
        &self,
        build_id: &str,
    ) -> Result<vz_runtime_contract::Build, vz_runtime_contract::RuntimeError> {
        self.daemon.manager().get_build(build_id).await
    }

    async fn cancel_build(
        &self,
        build_id: &str,
    ) -> Result<vz_runtime_contract::Build, vz_runtime_contract::RuntimeError> {
        self.daemon.manager().cancel_build(build_id).await
    }
}

pub(in crate::grpc) struct StackServiceImpl {
    daemon: Arc<RuntimeDaemon>,
}

impl StackServiceImpl {
    pub(in crate::grpc) fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self { daemon }
    }
}

struct DaemonContainerRuntime {
    daemon: Arc<RuntimeDaemon>,
    handle: tokio::runtime::Handle,
}

impl DaemonContainerRuntime {
    fn new(daemon: Arc<RuntimeDaemon>) -> Self {
        Self {
            daemon,
            handle: tokio::runtime::Handle::current(),
        }
    }

    fn capabilities(&self) -> vz_runtime_contract::RuntimeCapabilities {
        self.daemon.manager().capabilities()
    }

    /// Drive a daemon future from the executor's synchronous runtime adapter.
    ///
    /// Stack execution calls this adapter both directly on Tokio multi-thread
    /// workers and from plain `std::thread` workers used for parallel scoped
    /// activation. `block_in_place` is valid only in the former context; a
    /// plain worker can use the captured daemon runtime handle directly.
    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future,
    {
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.handle.block_on(future))
        } else {
            self.handle.block_on(future)
        }
    }

    fn ensure_capability(
        &self,
        operation: &str,
        capability_name: &str,
        enabled: bool,
    ) -> Result<(), StackError> {
        if enabled {
            return Ok(());
        }
        Err(unsupported_operation_error(
            operation,
            format!(
                "backend={} missing capability {}",
                self.daemon.manager().name(),
                capability_name
            ),
        ))
    }

    /// Reserve one exact topology-scoped generation without starting it.
    ///
    /// These inherent adapters are kept separate from `ContainerRuntime` until
    /// the stack executor grows the matching two-phase trait surface. Keeping
    /// the bridge here lets that change reuse the exact daemon error mapping
    /// without routing production creates through the legacy synthetic scope.
    #[allow(dead_code)]
    fn reserve_container_generation(
        &self,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        container_id: &str,
    ) -> Result<vz_runtime_contract::ContainerGenerationOwnership, StackError> {
        self.block_on(
            self.daemon
                .manager()
                .reserve_stack_container_generation(scope, container_id),
        )
        .map_err(|error| map_runtime_error("reserve_container_generation", error))
    }

    /// Inspect a reservation by caller-stable scope after reopen or response loss.
    #[allow(dead_code)]
    fn inspect_container_reservation(
        &self,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        container_id: &str,
    ) -> Result<vz_runtime_contract::ContainerGenerationInspection, StackError> {
        self.block_on(
            self.daemon
                .manager()
                .inspect_stack_container_reservation(scope, container_id),
        )
        .map_err(|error| map_runtime_error("inspect_container_reservation", error))
    }

    /// Inspect one exact generation, including replacement classification.
    #[allow(dead_code)]
    fn inspect_container_generation(
        &self,
        ownership: &vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::ContainerGenerationInspection, StackError> {
        self.block_on(
            self.daemon
                .manager()
                .inspect_stack_container_generation(ownership),
        )
        .map_err(|error| map_runtime_error("inspect_container_generation", error))
    }

    /// Activate only the generation named by an exact ownership proof.
    #[allow(dead_code, clippy::result_large_err)]
    fn activate_container_generation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<
        vz_runtime_contract::ContainerCreateReceipt,
        vz_runtime_contract::OwnedCreateError<StackError>,
    > {
        self.block_on(
            self.daemon
                .manager()
                .activate_stack_container_generation(ownership, image, config),
        )
        .map_err(|failure| map_owned_runtime_error("activate_container_generation", failure))
    }

    /// Release only an exact unpublished reservation.
    #[allow(dead_code)]
    fn release_container_reservation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::ContainerGenerationReleaseOutcome, StackError> {
        self.block_on(
            self.daemon
                .manager()
                .release_stack_container_reservation(ownership),
        )
        .map_err(|error| map_runtime_error("release_container_reservation", error))
    }
}

/// Decode the required topology authority for a mutating stack request.
///
/// Missing scope is rejected instead of falling through to stack-name or
/// process-local identity. Callers must run this before any filesystem, journal,
/// OCI, guest, or network mutation.
fn required_machine_workload_scope(
    scope: Option<&runtime_v2::MachineWorkloadScope>,
) -> Result<vz_runtime_contract::MachineWorkloadScope, StackError> {
    let scope = scope.ok_or_else(|| StackError::Machine {
        code: vz_runtime_contract::MachineErrorCode::ValidationError,
        message: "unscoped legacy stack access is disabled; use the topology-scoped API and provide MachineWorkloadScope"
            .to_string(),
    })?;
    vz_runtime_translate::machine_workload_scope_from_proto(scope).map_err(|error| {
        StackError::Machine {
            code: vz_runtime_contract::MachineErrorCode::ValidationError,
            message: format!("invalid MachineWorkloadScope: {error}"),
        }
    })
}

/// Decode and fence all topology authority needed by a mutating stack handler.
///
/// This is the single entry point intended for RPC integration: a missing or
/// malformed wire scope and a stale persisted scope all return before the
/// caller receives any authority it can pass to a mutating runtime method.
#[allow(dead_code)]
pub(in crate::grpc) fn validated_machine_workload_scope_for_mutation(
    store: &vz_stack::StateStore,
    wire_scope: Option<&runtime_v2::MachineWorkloadScope>,
    requested_stack_id: &str,
) -> Result<vz_runtime_contract::MachineWorkloadScope, StackError> {
    let scope = required_machine_workload_scope(wire_scope)?;
    validate_machine_workload_scope_for_mutation(store, &scope, requested_stack_id)?;
    store.validate_stack_workload_owner(&scope)?;
    Ok(scope)
}

/// Validate topology for the one first-apply path before its owner claim.
pub(in crate::grpc) fn validated_machine_workload_scope_for_claim(
    store: &vz_stack::StateStore,
    wire_scope: Option<&runtime_v2::MachineWorkloadScope>,
    requested_stack_id: &str,
) -> Result<vz_runtime_contract::MachineWorkloadScope, StackError> {
    let scope = required_machine_workload_scope(wire_scope)?;
    if scope.stack_id != requested_stack_id {
        return Err(StackError::Machine {
            code: vz_runtime_contract::MachineErrorCode::StateConflict,
            message: format!(
                "workload scope stack `{}` does not match requested stack `{requested_stack_id}`",
                scope.stack_id
            ),
        });
    }
    store.validate_stack_workload_owner_claim(&scope)?;
    Ok(scope)
}

/// Decode and fence topology authority for a read or stream without requiring
/// the Environment and Machine to be runnable.
pub(in crate::grpc) fn validated_machine_workload_scope_for_read(
    store: &vz_stack::StateStore,
    wire_scope: Option<&runtime_v2::MachineWorkloadScope>,
    requested_stack_id: &str,
) -> Result<vz_runtime_contract::MachineWorkloadScope, StackError> {
    let scope = required_machine_workload_scope(wire_scope)?;
    validate_machine_workload_scope_ownership(store, &scope, requested_stack_id, false)?;
    store.validate_stack_workload_owner(&scope)?;
    Ok(scope)
}

/// Decode cleanup authority for the current Machine incarnation.
///
/// Historical incarnation cleanup is an internal recovery operation over an
/// exact journal reservation. A public stack-wide teardown or service action
/// must never use an old incarnation to mutate the current stack projection.
pub(in crate::grpc) fn validated_machine_workload_scope_for_cleanup(
    store: &vz_stack::StateStore,
    wire_scope: Option<&runtime_v2::MachineWorkloadScope>,
    requested_stack_id: &str,
) -> Result<vz_runtime_contract::MachineWorkloadScope, StackError> {
    let scope = required_machine_workload_scope(wire_scope)?;
    validate_machine_workload_scope_ownership(store, &scope, requested_stack_id, false)?;
    store.validate_stack_workload_owner(&scope)?;
    Ok(scope)
}

#[allow(clippy::result_large_err)]
fn validate_stack_request_scope(
    daemon: &RuntimeDaemon,
    wire_scope: Option<&runtime_v2::MachineWorkloadScope>,
    stack_id: &str,
    require_runnable: bool,
    request_id: &str,
) -> Result<vz_runtime_contract::MachineWorkloadScope, Status> {
    daemon
        .with_state_store(|store| {
            if require_runnable {
                validated_machine_workload_scope_for_mutation(store, wire_scope, stack_id)
            } else {
                validated_machine_workload_scope_for_read(store, wire_scope, stack_id)
            }
        })
        .map_err(|error| status_from_stack_error(error, request_id))
}

#[allow(clippy::result_large_err)]
fn validate_stack_cleanup_request_scope(
    daemon: &RuntimeDaemon,
    wire_scope: Option<&runtime_v2::MachineWorkloadScope>,
    stack_id: &str,
    request_id: &str,
) -> Result<vz_runtime_contract::MachineWorkloadScope, Status> {
    daemon
        .with_state_store(|store| {
            validated_machine_workload_scope_for_cleanup(store, wire_scope, stack_id)
        })
        .map_err(|error| status_from_stack_error(error, request_id))
}

#[allow(clippy::result_large_err)]
fn validate_stack_apply_request_scope(
    daemon: &RuntimeDaemon,
    wire_scope: Option<&runtime_v2::MachineWorkloadScope>,
    stack_id: &str,
    request_id: &str,
) -> Result<vz_runtime_contract::MachineWorkloadScope, Status> {
    let scope = daemon
        .with_state_store(|store| {
            validated_machine_workload_scope_for_claim(store, wire_scope, stack_id)
        })
        .map_err(|error| status_from_stack_error(error, request_id))?;
    if daemon
        .runtime_data_dir()
        .join("stacks")
        .join(stack_id)
        .exists()
        && daemon
            .with_state_store(|store| store.load_stack_workload_owner(stack_id))
            .map_err(|error| status_from_stack_error(error, request_id))?
            .is_none()
    {
        return Err(status_from_stack_error(
            StackError::Machine {
                code: vz_runtime_contract::MachineErrorCode::StateConflict,
                message: format!(
                    "stack_id `{stack_id}` has an unowned runtime namespace; explicit ownership migration is required"
                ),
            },
            request_id,
        ));
    }
    Ok(scope)
}

#[allow(clippy::result_large_err)]
fn enforce_stack_policy_preflight_read_only(
    daemon: &RuntimeDaemon,
    operation: RuntimeOperation,
    metadata: &vz_runtime_contract::RequestMetadata,
    _request_id: &str,
) -> Result<(), Status> {
    daemon
        .enforce_policy_preflight(operation, metadata)
        .map_err(|error| {
            status_from_machine_error(vz_runtime_contract::runtime_error_machine_error(
                &error, metadata,
            ))
        })
}

#[allow(clippy::result_large_err)]
fn reject_primary_service_run_alias(
    service_name: &str,
    run_service_name: &str,
    request_id: &str,
) -> Result<(), Status> {
    if run_service_name == service_name {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "run_service_name `{run_service_name}` must not alias primary service `{service_name}`"
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }
    Ok(())
}

/// Fence a decoded workload scope against the current persisted topology.
///
/// This is deliberately read-only. A mutating handler must complete it before
/// creating a stack directory or invoking a runtime method, and then pass the
/// returned exact scope into the scoped executor rather than re-resolving names.
fn validate_machine_workload_scope_for_mutation(
    store: &vz_stack::StateStore,
    scope: &vz_runtime_contract::MachineWorkloadScope,
    requested_stack_id: &str,
) -> Result<(), StackError> {
    validate_machine_workload_scope_ownership(store, scope, requested_stack_id, true)
}

fn validate_machine_workload_scope_ownership(
    store: &vz_stack::StateStore,
    scope: &vz_runtime_contract::MachineWorkloadScope,
    requested_stack_id: &str,
    require_runnable: bool,
) -> Result<(), StackError> {
    scope.validate().map_err(|reason| StackError::Machine {
        code: vz_runtime_contract::MachineErrorCode::ValidationError,
        message: format!("invalid MachineWorkloadScope: {reason}"),
    })?;
    if scope.stack_id != requested_stack_id {
        return Err(scope_conflict(format!(
            "request stack `{requested_stack_id}` does not match scoped stack `{}`",
            scope.stack_id
        )));
    }

    let project = store
        .load_project_state(scope.project_id.as_str())?
        .ok_or_else(|| {
            scope_conflict(format!(
                "scoped Project `{}` is not present",
                scope.project_id
            ))
        })?;
    let environment = project
        .environments
        .iter()
        .find(|environment| environment.environment_id == scope.environment_id)
        .ok_or_else(|| {
            scope_conflict(format!(
                "scoped Environment `{}` is not present in Project `{}`",
                scope.environment_id, scope.project_id
            ))
        })?;
    if environment.project_id != scope.project_id {
        return Err(scope_conflict(
            "scoped Project does not own the Environment",
        ));
    }
    if require_runnable && environment.state != vz_runtime_contract::EnvironmentState::Ready {
        return Err(scope_conflict(format!(
            "Environment `{}` is not Ready for stack mutation ({:?})",
            environment.environment_id, environment.state
        )));
    }
    let machine = environment
        .machines
        .iter()
        .find(|machine| machine.machine_id == scope.machine_id)
        .ok_or_else(|| {
            scope_conflict(format!(
                "scoped Machine `{}` is not present in Environment `{}`",
                scope.machine_id, scope.environment_id
            ))
        })?;
    if machine.environment_id != scope.environment_id {
        return Err(scope_conflict(
            "scoped Environment does not own the Machine",
        ));
    }
    if require_runnable && machine.state != vz_runtime_contract::MachineState::Ready {
        return Err(scope_conflict(format!(
            "Machine `{}` is not Ready for stack mutation ({:?})",
            machine.machine_id, machine.state
        )));
    }
    let current_incarnation = machine.incarnation.as_ref().ok_or_else(|| {
        scope_conflict(format!(
            "scoped Machine `{}` has no current incarnation",
            scope.machine_id
        ))
    })?;
    if current_incarnation.incarnation_id != scope.machine_incarnation_id {
        return Err(scope_conflict(format!(
            "scoped Machine incarnation `{}` is stale; current incarnation is `{}`",
            scope.machine_incarnation_id, current_incarnation.incarnation_id
        )));
    }
    Ok(())
}

fn scope_conflict(message: impl Into<String>) -> StackError {
    StackError::Machine {
        code: vz_runtime_contract::MachineErrorCode::StateConflict,
        message: message.into(),
    }
}

fn unsupported_operation_error(operation: &str, reason: impl Into<String>) -> StackError {
    StackError::Network(format!(
        "unsupported_operation: surface=stack; operation={operation}; reason={}",
        reason.into()
    ))
}

fn map_runtime_error(operation: &str, error: vz_runtime_contract::RuntimeError) -> StackError {
    match error {
        vz_runtime_contract::RuntimeError::UnsupportedOperation {
            operation: backend_operation,
            reason,
        } => unsupported_operation_error(
            operation,
            format!("backend_operation={backend_operation}; {reason}"),
        ),
        other => StackError::Machine {
            code: other.machine_code(),
            message: format!("{operation} failed: {other}"),
        },
    }
}

fn map_owned_runtime_error(
    operation: &str,
    failure: vz_runtime_contract::OwnedCreateError<vz_runtime_contract::RuntimeError>,
) -> vz_runtime_contract::OwnedCreateError<StackError> {
    failure.map_error(|error| map_runtime_error(operation, error))
}

#[cfg(test)]
mod runtime_error_mapping_tests {
    use super::*;
    use vz_runtime_contract::{
        Architecture, CapabilitySet, EnvironmentId, EnvironmentInstance, EnvironmentLifecycleKind,
        EnvironmentSpec, EnvironmentState, MachineCapability, MachineId, MachineIncarnation,
        MachineIncarnationId, MachineInstance, MachineProfile, MachineResources, MachineSpec,
        MachineState, MachineWorkloadScope, OperatingSystem, OwnedResourceKind, OwnershipRecord,
        ProjectDefinition, ProjectId, ProjectState, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
    };

    fn topology_fixture() -> (ProjectState, MachineWorkloadScope) {
        let project_id = ProjectId::new("prj-daemon-scope").unwrap();
        let environment_id = EnvironmentId::new("env-daemon-scope").unwrap();
        let machine_id = MachineId::new("mch-daemon-scope").unwrap();
        let incarnation_id = MachineIncarnationId::new("inc-daemon-scope").unwrap();
        let target = TargetSpec {
            os: OperatingSystem::Linux,
            arch: Architecture::Aarch64,
            image: "ubuntu:24.04".to_string(),
            version: None,
            channel: None,
            digest: Some("sha256:daemon-scope".to_string()),
        };
        let capabilities = CapabilitySet::new([
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ]);
        let definition = ProjectDefinition {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: project_id.clone(),
            name: "daemon-scope".to_string(),
            environment: EnvironmentSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                machines: vec![MachineSpec {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    name: "linux".to_string(),
                    profile: MachineProfile::Developer,
                    target: target.clone(),
                    resources: MachineResources::default(),
                    requested_capabilities: capabilities.clone(),
                    workspace: None,
                }],
                networks: Vec::new(),
                endpoints: Vec::new(),
            },
        };
        let environment = EnvironmentInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            environment_id: environment_id.clone(),
            project_id: project_id.clone(),
            name: "developer".to_string(),
            definition_digest: definition.digest().unwrap(),
            state: EnvironmentState::Ready,
            lifecycle_generation: 1,
            active_operation_id: None,
            bindings: Vec::new(),
            machines: vec![MachineInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                machine_id: machine_id.clone(),
                environment_id: environment_id.clone(),
                name: "linux".to_string(),
                profile: MachineProfile::Developer,
                target,
                resources: MachineResources::default(),
                requested_capabilities: capabilities.clone(),
                negotiated_capabilities: capabilities,
                backend: None,
                incarnation: Some(MachineIncarnation {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    incarnation_id: incarnation_id.clone(),
                    machine_id: machine_id.clone(),
                    generation: 1,
                    created_at: 1,
                }),
                state: MachineState::Ready,
                legacy_sandbox_id: None,
            }],
            networks: Vec::new(),
            endpoints: Vec::new(),
            ownership: vec![
                OwnershipRecord {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    resource_kind: OwnedResourceKind::Machine,
                    resource_id: machine_id.to_string(),
                    environment_id: environment_id.clone(),
                    machine_id: Some(machine_id.clone()),
                },
                OwnershipRecord {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    resource_kind: OwnedResourceKind::Incarnation,
                    resource_id: incarnation_id.to_string(),
                    environment_id: environment_id.clone(),
                    machine_id: Some(machine_id.clone()),
                },
            ],
            legacy_migration: None,
            created_at: 1,
            updated_at: 1,
        };
        let scope = MachineWorkloadScope {
            schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
            project_id,
            environment_id,
            machine_id,
            machine_incarnation_id: incarnation_id,
            stack_id: "stack-daemon-scope".to_string(),
        };
        (
            ProjectState {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                definition,
                environments: vec![environment],
            },
            scope,
        )
    }

    #[test]
    fn runtime_state_conflict_keeps_its_machine_classification() {
        let mapped = map_runtime_error(
            "cleanup_container_generation",
            vz_runtime_contract::RuntimeError::ContainerFailed {
                id: "owned-id".to_string(),
                reason: "generation changed".to_string(),
            },
        );

        assert_eq!(
            mapped.machine_code(),
            vz_runtime_contract::MachineErrorCode::StateConflict
        );
        assert!(mapped.to_string().contains("generation changed"));
    }

    #[test]
    fn runtime_unsupported_operation_keeps_stack_surface_context() {
        let mapped = map_runtime_error(
            "cleanup_container_generation",
            vz_runtime_contract::RuntimeError::UnsupportedOperation {
                operation: "cleanup_container_generation".to_string(),
                reason: "backend lacks generation cleanup".to_string(),
            },
        );

        assert_eq!(
            mapped.machine_code(),
            vz_runtime_contract::MachineErrorCode::UnsupportedOperation
        );
        assert!(mapped.to_string().contains("surface=stack"));
    }

    #[test]
    fn owned_runtime_error_mapping_preserves_exact_cleanup_proof() {
        let (_, workload) = topology_fixture();
        let scope = workload
            .container_generation_scope("reservation-owned-error")
            .unwrap();
        let ownership = vz_runtime_contract::ContainerGenerationOwnership {
            container_id: "ctr-owned-error".to_string(),
            generation: 9,
            stack_id: scope.stack_id.clone(),
            scope: Some(Box::new(scope)),
        };
        let mapped = map_owned_runtime_error(
            "activate_container_generation",
            vz_runtime_contract::OwnedCreateError {
                error: vz_runtime_contract::RuntimeError::ContainerFailed {
                    id: ownership.container_id.clone(),
                    reason: "activation failed after reservation".to_string(),
                },
                cleanup: Some(ownership.clone()),
            },
        );

        assert_eq!(mapped.cleanup, Some(ownership));
        assert_eq!(
            mapped.error.machine_code(),
            vz_runtime_contract::MachineErrorCode::StateConflict
        );
    }

    #[test]
    fn required_scope_rejects_absence_and_invalid_wire_authority() {
        let store = vz_stack::StateStore::in_memory().unwrap();
        let missing =
            validated_machine_workload_scope_for_mutation(&store, None, "stack-daemon-scope")
                .unwrap_err();
        assert_eq!(
            missing.machine_code(),
            vz_runtime_contract::MachineErrorCode::ValidationError
        );

        let invalid = runtime_v2::MachineWorkloadScope {
            schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION + 1,
            project_id: "prj-daemon-scope".to_string(),
            environment_id: "env-daemon-scope".to_string(),
            machine_id: "mch-daemon-scope".to_string(),
            machine_incarnation_id: "inc-daemon-scope".to_string(),
            stack_id: "stack-daemon-scope".to_string(),
        };
        let invalid = validated_machine_workload_scope_for_mutation(
            &store,
            Some(&invalid),
            "stack-daemon-scope",
        )
        .unwrap_err();
        assert_eq!(
            invalid.machine_code(),
            vz_runtime_contract::MachineErrorCode::ValidationError
        );
    }

    #[test]
    fn topology_scope_accepts_only_current_exact_machine_incarnation() {
        let store = vz_stack::StateStore::in_memory().unwrap();
        let (project, scope) = topology_fixture();
        store.save_project_state(&project).unwrap();
        store.reserve_stack_workload_owner(&scope, 1).unwrap();

        let wire = vz_runtime_translate::machine_workload_scope_to_proto(&scope);
        assert_eq!(
            validated_machine_workload_scope_for_mutation(
                &store,
                Some(&wire),
                "stack-daemon-scope",
            )
            .unwrap(),
            scope
        );

        let mut wrong_stack = scope.clone();
        wrong_stack.stack_id = "another-stack".to_string();
        let error = validate_machine_workload_scope_for_mutation(
            &store,
            &wrong_stack,
            "stack-daemon-scope",
        )
        .unwrap_err();
        assert_eq!(
            error.machine_code(),
            vz_runtime_contract::MachineErrorCode::StateConflict
        );

        let mut stale = scope;
        stale.machine_incarnation_id = MachineIncarnationId::new("inc-replaced").unwrap();
        let error =
            validate_machine_workload_scope_for_mutation(&store, &stale, "stack-daemon-scope")
                .unwrap_err();
        assert_eq!(
            error.machine_code(),
            vz_runtime_contract::MachineErrorCode::StateConflict
        );
    }

    #[test]
    fn stopping_topology_denies_mutation_but_preserves_exact_read_authority() {
        let store = vz_stack::StateStore::in_memory().unwrap();
        let (project, scope) = topology_fixture();
        store.save_project_state(&project).unwrap();
        store.reserve_stack_workload_owner(&scope, 1).unwrap();
        store
            .begin_environment_lifecycle(
                scope.environment_id.as_str(),
                EnvironmentLifecycleKind::Stop,
                "req-stop-scope-test",
                "idem-stop-scope-test",
                "hash-stop-scope-test",
                2,
            )
            .unwrap();
        let wire = vz_runtime_translate::machine_workload_scope_to_proto(&scope);

        let mutation = validated_machine_workload_scope_for_mutation(
            &store,
            Some(&wire),
            "stack-daemon-scope",
        )
        .unwrap_err();
        assert_eq!(
            mutation.machine_code(),
            vz_runtime_contract::MachineErrorCode::StateConflict
        );
        assert_eq!(
            validated_machine_workload_scope_for_read(&store, Some(&wire), "stack-daemon-scope",)
                .unwrap(),
            scope
        );
        assert_eq!(
            validated_machine_workload_scope_for_cleanup(
                &store,
                Some(&wire),
                "stack-daemon-scope",
            )
            .unwrap(),
            scope
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn daemon_two_phase_adapters_preserve_operation_and_owned_error_shape() {
        let temp = tempfile::tempdir().unwrap();
        let daemon = Arc::new(
            RuntimeDaemon::start(crate::RuntimedConfig {
                state_store_path: temp.path().join("state").join("stack-state.db"),
                runtime_data_dir: temp.path().join("runtime"),
                socket_path: temp.path().join("runtime").join("runtimed.sock"),
            })
            .unwrap(),
        );
        let adapter = DaemonContainerRuntime::new(daemon);
        let (_, workload) = topology_fixture();
        let scope = workload
            .container_generation_scope("reservation-daemon-bridge")
            .unwrap();
        let ownership = vz_runtime_contract::ContainerGenerationOwnership {
            container_id: "ctr-daemon-bridge".to_string(),
            generation: 1,
            stack_id: scope.stack_id.clone(),
            scope: Some(Box::new(scope.clone())),
        };

        let reserve = adapter
            .reserve_container_generation(&scope, &ownership.container_id)
            .unwrap_err();
        assert_eq!(
            reserve.machine_code(),
            vz_runtime_contract::MachineErrorCode::UnsupportedOperation
        );
        assert!(reserve.to_string().contains("reserve_container_generation"));

        let inspect_reservation = adapter
            .inspect_container_reservation(&scope, &ownership.container_id)
            .unwrap_err();
        assert_eq!(
            inspect_reservation.machine_code(),
            vz_runtime_contract::MachineErrorCode::UnsupportedOperation
        );
        assert!(
            inspect_reservation
                .to_string()
                .contains("inspect_container_reservation")
        );

        let inspect_generation = adapter
            .inspect_container_generation(&ownership)
            .unwrap_err();
        assert_eq!(
            inspect_generation.machine_code(),
            vz_runtime_contract::MachineErrorCode::UnsupportedOperation
        );
        assert!(
            inspect_generation
                .to_string()
                .contains("inspect_container_generation")
        );

        let activation = adapter
            .activate_container_generation(
                ownership.clone(),
                "alpine:latest",
                vz_runtime_contract::RunConfig::default(),
            )
            .unwrap_err();
        assert_eq!(
            activation.error.machine_code(),
            vz_runtime_contract::MachineErrorCode::UnsupportedOperation
        );
        assert!(
            activation
                .error
                .to_string()
                .contains("activate_container_generation")
        );
        assert!(activation.cleanup.is_none());

        let release = adapter
            .release_container_reservation(ownership)
            .unwrap_err();
        assert_eq!(
            release.machine_code(),
            vz_runtime_contract::MachineErrorCode::UnsupportedOperation
        );
        assert!(
            release
                .to_string()
                .contains("release_container_reservation")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn daemon_activation_adapter_runs_from_plain_worker_without_losing_owned_error() {
        let temp = tempfile::tempdir().unwrap();
        let daemon = Arc::new(
            RuntimeDaemon::start(crate::RuntimedConfig {
                state_store_path: temp.path().join("state").join("stack-state.db"),
                runtime_data_dir: temp.path().join("runtime"),
                socket_path: temp.path().join("runtime").join("runtimed.sock"),
            })
            .unwrap(),
        );
        let adapter = DaemonContainerRuntime::new(daemon);
        let (_, workload) = topology_fixture();
        let scope = workload
            .container_generation_scope("reservation-daemon-plain-worker")
            .unwrap();
        let ownership = vz_runtime_contract::ContainerGenerationOwnership {
            container_id: "ctr-daemon-plain-worker".to_string(),
            generation: 1,
            stack_id: scope.stack_id.clone(),
            scope: Some(Box::new(scope)),
        };

        let activation = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    adapter.activate_container_generation(
                        ownership,
                        "alpine:latest",
                        vz_runtime_contract::RunConfig::default(),
                    )
                })
                .join()
                .expect("plain activation worker must not panic")
        })
        .unwrap_err();

        assert_eq!(
            activation.error.machine_code(),
            vz_runtime_contract::MachineErrorCode::UnsupportedOperation
        );
        assert!(
            activation
                .error
                .to_string()
                .contains("activate_container_generation")
        );
        assert!(activation.cleanup.is_none());
    }
}

async fn shutdown_stack_runtime_for_teardown(
    daemon: Arc<RuntimeDaemon>,
    stack_id: String,
) -> Result<(), StackError> {
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        handle.block_on(daemon.manager().shutdown_stack_runtime(&stack_id))
    })
    .await
    .map_err(|error| StackError::Network(format!("shutdown_sandbox task failed: {error}")))?
    .map_err(|error| map_runtime_error("shutdown_sandbox", error))
}

impl ContainerRuntime for DaemonContainerRuntime {
    fn pull(&self, image: &str) -> Result<String, StackError> {
        self.block_on(self.daemon.manager().pull_image(image))
            .map_err(|error| map_runtime_error("pull", error))
    }

    fn create(
        &self,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<String, StackError> {
        self.block_on(self.daemon.manager().create_container(image, config))
            .map_err(|error| map_runtime_error("create", error))
    }

    fn stop(
        &self,
        container_id: &str,
        signal: Option<&str>,
        grace_period: Option<std::time::Duration>,
    ) -> Result<(), StackError> {
        self.block_on(self.daemon.manager().stop_container(
            container_id,
            false,
            signal,
            grace_period,
        ))
        .map(|_| ())
        .map_err(|error| map_runtime_error("stop", error))
    }

    fn remove(&self, container_id: &str) -> Result<(), StackError> {
        self.block_on(self.daemon.manager().remove_container(container_id))
            .map_err(|error| map_runtime_error("remove", error))
    }

    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError> {
        let (exit_code, _, _) = self.exec_with_output(container_id, command)?;
        Ok(exit_code)
    }

    fn exec_with_output(
        &self,
        container_id: &str,
        command: &[String],
    ) -> Result<(i32, String, String), StackError> {
        let exec_config = vz_runtime_contract::ExecConfig {
            cmd: command.to_vec(),
            ..Default::default()
        };
        self.block_on(
            self.daemon
                .manager()
                .exec_container(container_id, exec_config),
        )
        .map(|output| (output.exit_code, output.stdout, output.stderr))
        .map_err(|error| map_runtime_error("exec", error))
    }

    fn create_sandbox(
        &self,
        sandbox_id: &str,
        ports: Vec<vz_runtime_contract::PortMapping>,
        resources: vz_runtime_contract::StackResourceHint,
    ) -> Result<(), StackError> {
        let capabilities = self.capabilities();
        self.ensure_capability("create_sandbox", "shared_vm", capabilities.shared_vm)?;
        self.block_on(
            self.daemon
                .manager()
                .ensure_stack_runtime(sandbox_id, ports, resources),
        )
        .map_err(|error| map_runtime_error("create_sandbox", error))
    }

    fn create_in_sandbox(
        &self,
        sandbox_id: &str,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<String, StackError> {
        let capabilities = self.capabilities();
        self.ensure_capability("create_in_sandbox", "shared_vm", capabilities.shared_vm)?;
        self.block_on(
            self.daemon
                .manager()
                .create_stack_container(sandbox_id, image, config),
        )
        .map_err(|error| map_runtime_error("create_in_sandbox", error))
    }

    #[allow(clippy::result_large_err)]
    fn create_in_sandbox_owned(
        &self,
        sandbox_id: &str,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<
        vz_runtime_contract::ContainerCreateReceipt,
        vz_runtime_contract::OwnedCreateError<StackError>,
    > {
        self.block_on(
            self.daemon
                .manager()
                .create_legacy_stack_container_owned(sandbox_id, image, config),
        )
        .map_err(|failure| map_owned_runtime_error("create_in_sandbox", failure))
    }

    fn reserve_container_generation(
        &self,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        container_id: &str,
    ) -> Result<vz_runtime_contract::ContainerGenerationOwnership, StackError> {
        DaemonContainerRuntime::reserve_container_generation(self, scope, container_id)
    }

    fn inspect_container_reservation(
        &self,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        container_id: &str,
    ) -> Result<vz_runtime_contract::ContainerGenerationInspection, StackError> {
        DaemonContainerRuntime::inspect_container_reservation(self, scope, container_id)
    }

    fn inspect_container_generation(
        &self,
        ownership: &vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::ContainerGenerationInspection, StackError> {
        DaemonContainerRuntime::inspect_container_generation(self, ownership)
    }

    #[allow(clippy::result_large_err)]
    fn activate_container_generation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<
        vz_runtime_contract::ContainerCreateReceipt,
        vz_runtime_contract::OwnedCreateError<StackError>,
    > {
        DaemonContainerRuntime::activate_container_generation(self, ownership, image, config)
    }

    fn release_container_reservation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::ContainerGenerationReleaseOutcome, StackError> {
        DaemonContainerRuntime::release_container_reservation(self, ownership)
    }

    fn cleanup_container_generation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::GenerationCleanupOutcome, StackError> {
        self.block_on(
            self.daemon
                .manager()
                .cleanup_container_generation(ownership),
        )
        .map_err(|error| map_runtime_error("cleanup_container_generation", error))
    }

    fn stop_and_remove_container_generation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
        signal: Option<&str>,
        grace_period: Option<std::time::Duration>,
    ) -> Result<vz_runtime_contract::GenerationCleanupOutcome, StackError> {
        self.block_on(self.daemon.manager().stop_and_remove_container_generation(
            ownership,
            signal.map(str::to_string),
            grace_period,
        ))
        .map_err(|error| map_runtime_error("stop_and_remove_container_generation", error))
    }

    fn setup_sandbox_network(
        &self,
        sandbox_id: &str,
        services: Vec<vz_runtime_contract::NetworkServiceConfig>,
    ) -> Result<(), StackError> {
        let capabilities = self.capabilities();
        self.ensure_capability("setup_sandbox_network", "shared_vm", capabilities.shared_vm)?;
        self.ensure_capability(
            "setup_sandbox_network",
            "stack_networking",
            capabilities.stack_networking,
        )?;
        self.block_on(
            self.daemon
                .manager()
                .setup_stack_network(sandbox_id, services),
        )
        .map_err(|error| map_runtime_error("setup_sandbox_network", error))
    }

    fn teardown_sandbox_network(
        &self,
        sandbox_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), StackError> {
        let capabilities = self.capabilities();
        self.ensure_capability(
            "teardown_sandbox_network",
            "shared_vm",
            capabilities.shared_vm,
        )?;
        self.ensure_capability(
            "teardown_sandbox_network",
            "stack_networking",
            capabilities.stack_networking,
        )?;
        self.block_on(
            self.daemon
                .manager()
                .teardown_stack_network(sandbox_id, service_names),
        )
        .map_err(|error| map_runtime_error("teardown_sandbox_network", error))
    }

    fn shutdown_sandbox(&self, sandbox_id: &str) -> Result<(), StackError> {
        self.block_on(self.daemon.manager().shutdown_stack_runtime(sandbox_id))
            .map_err(|error| map_runtime_error("shutdown_sandbox", error))
    }

    fn has_sandbox(&self, sandbox_id: &str) -> bool {
        if !self.capabilities().shared_vm {
            return false;
        }
        self.daemon.manager().has_stack_runtime(sandbox_id)
    }

    fn logs(&self, container_id: &str) -> Result<ContainerLogs, StackError> {
        let capabilities = self.capabilities();
        self.ensure_capability("logs", "container_logs", capabilities.container_logs)?;
        let logs = self
            .daemon
            .manager()
            .container_logs(container_id)
            .map_err(|error| map_runtime_error("logs", error))?;
        Ok(ContainerLogs {
            output: logs.output,
        })
    }
}

fn stack_runtime_dir(daemon: &RuntimeDaemon, stack_name: &str) -> PathBuf {
    daemon.runtime_data_dir().join("stacks").join(stack_name)
}

fn stack_status_from_observed(status: &ServiceObservedState) -> runtime_v2::StackServiceStatus {
    runtime_v2::StackServiceStatus {
        service_name: status.service_name.clone(),
        phase: match status.phase {
            ServicePhase::Pending => "pending".to_string(),
            ServicePhase::Creating => "creating".to_string(),
            ServicePhase::Running => "running".to_string(),
            ServicePhase::Stopping => "stopping".to_string(),
            ServicePhase::Stopped => "stopped".to_string(),
            ServicePhase::Failed => "failed".to_string(),
        },
        ready: status.ready,
        container_id: status.container_id.clone().unwrap_or_default(),
        last_error: status.last_error.clone().unwrap_or_default(),
    }
}

fn default_stopped_service(service_name: &str) -> ServiceObservedState {
    ServiceObservedState {
        service_name: service_name.to_string(),
        phase: ServicePhase::Stopped,
        container_id: None,
        failed_create_ownership: None,
        last_error: None,
        ready: false,
    }
}

#[expect(
    clippy::result_large_err,
    reason = "this helper feeds tonic service methods whose error type is fixed to tonic::Status"
)]
fn load_stack_service_action_context(
    daemon: &RuntimeDaemon,
    stack_name: &str,
    service_name: &str,
    request_id: &str,
) -> Result<(StackSpec, ServiceObservedState), Status> {
    let (desired, observed) = daemon
        .with_state_store(|store| {
            Ok((
                store.load_desired_state(stack_name)?,
                store.load_observed_state(stack_name)?,
            ))
        })
        .map_err(|error| status_from_stack_error(error, request_id))?;

    if desired.is_none() && observed.is_empty() {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::NotFound,
            format!("stack not found: {stack_name}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let spec = desired.ok_or_else(|| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::StateConflict,
            format!("desired stack state missing for: {stack_name}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    if !spec
        .services
        .iter()
        .any(|service| service.name == service_name)
    {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::NotFound,
            format!("service not found in stack {stack_name}: {service_name}"),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let observed_state = observed
        .iter()
        .find(|service| service.service_name == service_name)
        .cloned()
        .unwrap_or_else(|| default_stopped_service(service_name));

    Ok((spec, observed_state))
}

fn stack_service_action_response(
    request_id: String,
    stack_name: String,
    service_state: ServiceObservedState,
) -> runtime_v2::StackServiceActionResponse {
    runtime_v2::StackServiceActionResponse {
        request_id,
        stack_name,
        service: Some(stack_status_from_observed(&service_state)),
    }
}

fn stack_run_container_response(
    request_id: String,
    stack_name: String,
    service_name: String,
    run_service_name: String,
    container_id: String,
) -> runtime_v2::StackRunContainerResponse {
    runtime_v2::StackRunContainerResponse {
        request_id,
        stack_name,
        service_name,
        run_service_name,
        container_id,
    }
}

fn generated_stack_run_service_name(service_name: &str) -> String {
    let suffix = generate_request_id().replace("req_", "");
    format!("{service_name}-run-{suffix}")
}

#[expect(
    clippy::result_large_err,
    reason = "this helper feeds tonic service methods whose error type is fixed to tonic::Status"
)]
fn clone_stack_spec_with_run_service(
    spec: &StackSpec,
    service_name: &str,
    run_service_name: &str,
    request_id: &str,
) -> Result<StackSpec, Status> {
    let source_service = spec
        .services
        .iter()
        .find(|service| service.name == service_name)
        .cloned()
        .ok_or_else(|| {
            status_from_machine_error(MachineError::new(
                MachineErrorCode::NotFound,
                format!("service not found in stack {}: {service_name}", spec.name),
                Some(request_id.to_string()),
                BTreeMap::new(),
            ))
        })?;

    if spec
        .services
        .iter()
        .any(|service| service.name == run_service_name && service.name != service_name)
    {
        return Err(status_from_machine_error(MachineError::new(
            MachineErrorCode::ValidationError,
            format!(
                "run service name already exists in stack {}: {run_service_name}",
                spec.name
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    let mut run_service = source_service;
    run_service.name = run_service_name.to_string();
    run_service.container_name = None;

    let mut run_spec = spec.clone();
    run_spec.services.push(run_service);
    Ok(run_spec)
}

#[expect(
    clippy::result_large_err,
    reason = "this helper feeds tonic service methods whose error type is fixed to tonic::Status"
)]
fn load_observed_stack_service(
    daemon: &RuntimeDaemon,
    stack_name: &str,
    service_name: &str,
    request_id: &str,
) -> Result<ServiceObservedState, Status> {
    daemon
        .with_state_store(|store| {
            Ok(store
                .load_observed_state(stack_name)?
                .into_iter()
                .find(|service| service.service_name == service_name)
                .unwrap_or_else(|| default_stopped_service(service_name)))
        })
        .map_err(|error| status_from_stack_error(error, request_id))
}

#[expect(
    clippy::result_large_err,
    reason = "this helper feeds tonic service methods whose error type is fixed to tonic::Status"
)]
fn execute_stack_service_action(
    daemon: Arc<RuntimeDaemon>,
    spec: &StackSpec,
    action: Action,
    workload_scope: vz_runtime_contract::MachineWorkloadScope,
    request_id: &str,
    failure_code: MachineErrorCode,
) -> Result<(), Status> {
    let stack_dir = stack_runtime_dir(daemon.as_ref(), &spec.name);
    std::fs::create_dir_all(&stack_dir).map_err(|error| {
        status_from_machine_error(MachineError::new(
            MachineErrorCode::InternalError,
            format!(
                "failed to create stack runtime directory {}: {error}",
                stack_dir.display()
            ),
            Some(request_id.to_string()),
            BTreeMap::new(),
        ))
    })?;

    let exec_store = daemon
        .open_dedicated_state_store()
        .map_err(|error| status_from_stack_error(error, request_id))?;
    let runtime = DaemonContainerRuntime::new(daemon);
    let mut executor = if matches!(&action, Action::ServiceRemove { .. }) {
        StackExecutor::new_scoped_for_cleanup(runtime, exec_store, &stack_dir, workload_scope)
    } else {
        StackExecutor::new_scoped(runtime, exec_store, &stack_dir, workload_scope)
    }
    .map_err(|error| status_from_stack_error(error, request_id))?;
    let result = executor
        .execute_with_operation(spec, &[action], request_id, 0)
        .map_err(|error| status_from_stack_error(error, request_id))?;
    if result.failed > 0 {
        let first_error = result
            .errors
            .first()
            .map(|(_, message)| message.as_str())
            .unwrap_or("unknown stack service action failure");
        return Err(status_from_machine_error(MachineError::new(
            failure_code,
            first_error.to_string(),
            Some(request_id.to_string()),
            BTreeMap::new(),
        )));
    }

    Ok(())
}

fn teardown_execution_failure_response(
    result: &ExecutionResult,
    request_id: &str,
    events: &mut Vec<Result<runtime_v2::TeardownStackEvent, Status>>,
) -> Option<Response<TeardownStackEventStream>> {
    if result.failed == 0 {
        return None;
    }

    let errors = result
        .errors
        .iter()
        .map(|(action, message)| format!("{action}: {message}"))
        .collect::<Vec<_>>()
        .join("; ");
    let message = if errors.is_empty() {
        format!(
            "stack teardown execution failed for {} action(s)",
            result.failed
        )
    } else {
        format!(
            "stack teardown execution failed for {} action(s): {errors}",
            result.failed
        )
    };

    let status = status_from_machine_error(MachineError::new(
        MachineErrorCode::BackendUnavailable,
        message,
        Some(request_id.to_string()),
        BTreeMap::from([
            ("failed_actions".to_string(), result.failed.to_string()),
            (
                "succeeded_actions".to_string(),
                result.succeeded.to_string(),
            ),
        ]),
    ));
    events.push(Err(status));
    Some(stack_stream_response(std::mem::take(events), None))
}

fn parse_stack_spec(
    stack_name: &str,
    compose_yaml: &str,
    compose_dir: &str,
) -> Result<StackSpec, StackError> {
    let base_dir = if compose_dir.trim().is_empty() {
        PathBuf::from(".")
    } else {
        PathBuf::from(compose_dir)
    };
    parse_compose_with_dir(compose_yaml, stack_name, &base_dir)
        .map_err(|error| StackError::ComposeValidation(error.to_string()))
}

fn parse_stack_build_specs(
    compose_yaml: &str,
    compose_dir: &str,
) -> Result<Vec<ComposeBuildSpec>, StackError> {
    let base_dir = if compose_dir.trim().is_empty() {
        PathBuf::from(".")
    } else {
        PathBuf::from(compose_dir)
    };
    collect_compose_build_specs_with_dir(compose_yaml, &base_dir)
        .map(|builds| builds.into_values().collect())
        .map_err(|error| StackError::ComposeValidation(error.to_string()))
}

fn resolve_build_context_path(compose_dir: &Path, context: &str) -> PathBuf {
    let context_path = PathBuf::from(context);
    if context_path.is_absolute() {
        context_path
    } else {
        compose_dir.join(context_path)
    }
}

async fn run_compose_builds(
    daemon: Arc<RuntimeDaemon>,
    stack_spec: &StackSpec,
    compose_yaml: &str,
    compose_dir: &str,
) -> Result<(), StackError> {
    run_compose_builds_with_runner(
        &DaemonBuildRunner::new(daemon),
        stack_spec,
        compose_yaml,
        compose_dir,
        STACK_BUILD_POLL_INTERVAL,
        STACK_BUILD_TIMEOUT,
    )
    .await
}

async fn run_compose_builds_with_runner(
    runner: &(impl ComposeBuildRunner + ?Sized),
    stack_spec: &StackSpec,
    compose_yaml: &str,
    compose_dir: &str,
    poll_interval: Duration,
    timeout: Duration,
) -> Result<(), StackError> {
    let compose_dir_path = if compose_dir.trim().is_empty() {
        PathBuf::from(".")
    } else {
        PathBuf::from(compose_dir)
    };
    let build_specs = parse_stack_build_specs(compose_yaml, compose_dir)?;
    if build_specs.is_empty() {
        return Ok(());
    }

    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return Err(StackError::Network(
            "stack build timeout overflowed instant range".to_string(),
        ));
    };

    for build_spec in build_specs {
        let service = stack_spec
            .services
            .iter()
            .find(|service| service.name == build_spec.service_name)
            .ok_or_else(|| {
                StackError::ComposeValidation(format!(
                    "service `{}` not found while preparing build directives",
                    build_spec.service_name
                ))
            })?;

        let context_path = resolve_build_context_path(&compose_dir_path, &build_spec.context);
        let mut build = runner
            .start_build(
                &stack_spec.name,
                BuildSpec {
                    context: context_path.to_string_lossy().to_string(),
                    dockerfile: build_spec.dockerfile.clone(),
                    target: build_spec.target.clone(),
                    args: build_spec.args.clone(),
                    cache_from: build_spec.cache_from.clone(),
                    image_tag: Some(service.image.clone()),
                    secrets: Vec::new(),
                    no_cache: false,
                    push: false,
                    output_oci_tar_dest: None,
                },
            )
            .await
            .map_err(|error| map_runtime_error("start_build", error))?;

        while !build.state.is_terminal() {
            if Instant::now() >= deadline {
                let _ = runner.cancel_build(&build.build_id).await;
                return Err(StackError::Network(format!(
                    "timed out waiting for build {} for service {}",
                    build.build_id, build_spec.service_name
                )));
            }

            tokio::time::sleep(poll_interval).await;
            build = runner
                .get_build(&build.build_id)
                .await
                .map_err(|error| map_runtime_error("get_build", error))?;
        }

        if build.state != BuildState::Succeeded {
            return Err(StackError::Network(format!(
                "build {} for service {} finished in state {}",
                build.build_id,
                build_spec.service_name,
                build_state_label(build.state)
            )));
        }
    }

    Ok(())
}

fn build_state_label(state: BuildState) -> &'static str {
    match state {
        BuildState::Queued => "queued",
        BuildState::Running => "running",
        BuildState::Succeeded => "succeeded",
        BuildState::Failed => "failed",
        BuildState::Canceled => "canceled",
    }
}

fn tail_output(raw: &str, tail: usize) -> String {
    if tail == 0 {
        return raw.to_string();
    }
    let mut lines: Vec<&str> = raw.lines().collect();
    if lines.len() > tail {
        let start = lines.len() - tail;
        lines = lines.split_off(start);
    }
    let mut output = lines.join("\n");
    if !output.is_empty() {
        output.push('\n');
    }
    output
}

type ApplyStackEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::ApplyStackEvent, Status>>;
type TeardownStackEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::TeardownStackEvent, Status>>;
type StackServiceActionEventStream =
    tokio_stream::wrappers::ReceiverStream<Result<runtime_v2::StackServiceActionEvent, Status>>;

fn stack_stream_from_events<T>(
    events: Vec<Result<T, Status>>,
) -> tokio_stream::wrappers::ReceiverStream<Result<T, Status>>
where
    T: Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(events.len().max(1));
    for event in events {
        if tx.try_send(event).is_err() {
            break;
        }
    }
    drop(tx);
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

fn stack_stream_response<T>(
    events: Vec<Result<T, Status>>,
    receipt_id: Option<&str>,
) -> Response<tokio_stream::wrappers::ReceiverStream<Result<T, Status>>>
where
    T: Send + 'static,
{
    let mut response = Response::new(stack_stream_from_events(events));
    if let Some(receipt_id) = receipt_id
        && !receipt_id.trim().is_empty()
        && let Ok(value) = MetadataValue::try_from(receipt_id)
    {
        response.metadata_mut().insert("x-receipt-id", value);
    }
    response
}

fn apply_stack_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::ApplyStackEvent {
    runtime_v2::ApplyStackEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::apply_stack_event::Payload::Progress(
            runtime_v2::StackMutationProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn apply_stack_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::ApplyStackResponse,
    receipt_id: &str,
) -> runtime_v2::ApplyStackEvent {
    runtime_v2::ApplyStackEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::apply_stack_event::Payload::Completion(
            runtime_v2::ApplyStackCompletion {
                response: Some(response),
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

fn teardown_stack_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::TeardownStackEvent {
    runtime_v2::TeardownStackEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::teardown_stack_event::Payload::Progress(
            runtime_v2::StackMutationProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn teardown_stack_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::TeardownStackResponse,
    receipt_id: &str,
) -> runtime_v2::TeardownStackEvent {
    runtime_v2::TeardownStackEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::teardown_stack_event::Payload::Completion(
            runtime_v2::TeardownStackCompletion {
                response: Some(response),
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

fn stack_service_action_progress_event(
    request_id: &str,
    sequence: u64,
    phase: &str,
    detail: &str,
) -> runtime_v2::StackServiceActionEvent {
    runtime_v2::StackServiceActionEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::stack_service_action_event::Payload::Progress(
            runtime_v2::StackMutationProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
            },
        )),
    }
}

fn stack_service_action_completion_event(
    request_id: &str,
    sequence: u64,
    response: runtime_v2::StackServiceActionResponse,
    receipt_id: &str,
) -> runtime_v2::StackServiceActionEvent {
    runtime_v2::StackServiceActionEvent {
        request_id: request_id.to_string(),
        sequence,
        payload: Some(runtime_v2::stack_service_action_event::Payload::Completion(
            runtime_v2::StackServiceActionCompletion {
                response: Some(response),
                receipt_id: receipt_id.to_string(),
            },
        )),
    }
}

mod rpc;
