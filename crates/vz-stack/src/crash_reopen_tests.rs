#![allow(clippy::unwrap_used)]

use std::env;
use std::fs::{self, File};
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use vz_runtime_contract::{
    Architecture, CapabilitySet, ContainerCreateReceipt, ContainerGenerationInspection,
    ContainerGenerationOwnership, ContainerGenerationReleaseOutcome, EnvironmentId,
    EnvironmentInstance, EnvironmentSpec, EnvironmentState, GenerationCleanupOutcome,
    MachineCapability, MachineId, MachineIncarnation, MachineIncarnationId, MachineInstance,
    MachineProfile, MachineResources, MachineSpec, MachineState, MachineWorkloadScope,
    OperatingSystem, OwnedCreateError, OwnedResourceKind, OwnershipRecord, ProjectDefinition,
    ProjectId, ProjectState, RunConfig, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};

use crate::{
    Action, ClaimedAllocatorTarget, ClaimedCreateInput, ContainerRuntime, ReconcileSession,
    ReconcileSessionStatus, ServicePhase, StackContainerCreateStatus,
    StackContainerGenerationBinding, StackError, StackEvent, StackExecutor, StackSpec, StateStore,
    compute_actions_hash, parse_compose, plan_apply,
};

const CHILD_TEST: &str = "crash_reopen_tests::action_v3_state_store_crash_child";
const BOUNDARY_ENV: &str = "VZ_STACK_CRASH_CHILD_BOUNDARY";
const ROOT_ENV: &str = "VZ_STACK_CRASH_CHILD_ROOT";
const MARKER_ENV: &str = "VZ_STACK_CRASH_CHILD_MARKER";

const BOUNDARIES: [&str; 4] = [
    "successor_bound_before_activation",
    "runtime_published_before_receipt",
    "observed_upsert_before_intent_cas",
    "running_committed_before_batch_commit",
];

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
struct RuntimeCounters {
    reserve: u64,
    activate: u64,
    cleanup: u64,
}

impl RuntimeCounters {
    fn delta(self, before: Self) -> Self {
        Self {
            reserve: self.reserve.checked_sub(before.reserve).unwrap(),
            activate: self.activate.checked_sub(before.activate).unwrap(),
            cleanup: self.cleanup.checked_sub(before.cleanup).unwrap(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DurableRuntimeState {
    ownership: Option<ContainerGenerationOwnership>,
    reserved: bool,
    published: bool,
    last_generation: u64,
    counters: RuntimeCounters,
}

#[derive(Clone)]
struct DurableCrashRuntime {
    path: PathBuf,
}

impl DurableCrashRuntime {
    fn new(path: PathBuf) -> Self {
        if !path.exists() {
            write_json_durable(&path, &DurableRuntimeState::default());
        }
        Self { path }
    }

    fn load(&self) -> DurableRuntimeState {
        serde_json::from_slice(&fs::read(&self.path).unwrap()).unwrap()
    }

    fn save(&self, state: &DurableRuntimeState) {
        write_json_durable(&self.path, state);
    }

    fn inspection_for(
        &self,
        expected: &ContainerGenerationOwnership,
    ) -> ContainerGenerationInspection {
        let state = self.load();
        if !state.reserved {
            return ContainerGenerationInspection::Absent;
        }
        let Some(found) = state.ownership else {
            return ContainerGenerationInspection::Malformed(
                "durable test runtime lost active ownership".to_string(),
            );
        };
        if found == *expected {
            if state.published {
                ContainerGenerationInspection::Published(found)
            } else {
                ContainerGenerationInspection::ReservedUnpublished(found)
            }
        } else if found.container_id == expected.container_id
            && found.generation != expected.generation
        {
            ContainerGenerationInspection::Replacement
        } else {
            ContainerGenerationInspection::Foreign
        }
    }

    fn snapshot(&self, ownership: &ContainerGenerationOwnership) -> Value {
        let state = self.load();
        json!({
            "inspection": inspection_name(&self.inspection_for(ownership)),
            "counters": state.counters,
        })
    }
}

impl ContainerRuntime for DurableCrashRuntime {
    fn pull(&self, image: &str) -> Result<String, StackError> {
        Ok(format!("sha256:{:x}", Sha256::digest(image.as_bytes())))
    }

    fn create(&self, _image: &str, _config: RunConfig) -> Result<String, StackError> {
        Err(StackError::InvalidSpec(
            "crash companion requires exact generation activation".to_string(),
        ))
    }

    fn stop(
        &self,
        _container_id: &str,
        _signal: Option<&str>,
        _grace_period: Option<Duration>,
    ) -> Result<(), StackError> {
        Ok(())
    }

    fn remove(&self, _container_id: &str) -> Result<(), StackError> {
        Ok(())
    }

    fn exec(&self, _container_id: &str, _command: &[String]) -> Result<i32, StackError> {
        Ok(0)
    }

    fn reserve_container_generation(
        &self,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        container_id: &str,
    ) -> Result<ContainerGenerationOwnership, StackError> {
        let mut state = self.load();
        if state.reserved {
            let existing = state.ownership.clone().unwrap();
            if existing.container_id == container_id && existing.scope.as_deref() == Some(scope) {
                return Ok(existing);
            }
            return Err(state_conflict("foreign durable runtime reservation"));
        }
        state.last_generation += 1;
        state.counters.reserve += 1;
        let ownership = ContainerGenerationOwnership {
            container_id: container_id.to_string(),
            generation: state.last_generation,
            stack_id: scope.stack_id.clone(),
            scope: Some(Box::new(scope.clone())),
        };
        state.ownership = Some(ownership.clone());
        state.reserved = true;
        state.published = false;
        self.save(&state);
        Ok(ownership)
    }

    fn inspect_container_reservation(
        &self,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        container_id: &str,
    ) -> Result<ContainerGenerationInspection, StackError> {
        let state = self.load();
        if !state.reserved {
            return Ok(ContainerGenerationInspection::Absent);
        }
        let found = state.ownership.unwrap();
        if found.container_id != container_id || found.scope.as_deref() != Some(scope) {
            return Ok(ContainerGenerationInspection::Foreign);
        }
        Ok(if state.published {
            ContainerGenerationInspection::Published(found)
        } else {
            ContainerGenerationInspection::ReservedUnpublished(found)
        })
    }

    fn inspect_container_generation(
        &self,
        ownership: &ContainerGenerationOwnership,
    ) -> Result<ContainerGenerationInspection, StackError> {
        Ok(self.inspection_for(ownership))
    }

    fn activate_container_generation(
        &self,
        ownership: ContainerGenerationOwnership,
        _image: &str,
        config: RunConfig,
    ) -> Result<ContainerCreateReceipt, OwnedCreateError<StackError>> {
        pause_if_selected("successor_bound_before_activation");
        let mut state = self.load();
        if !state.reserved || state.published || state.ownership.as_ref() != Some(&ownership) {
            return Err(OwnedCreateError::unowned(state_conflict(
                "exact durable runtime generation is not activatable",
            )));
        }
        if config.container_id.as_deref() != Some(ownership.container_id.as_str()) {
            return Err(OwnedCreateError::unowned(state_conflict(
                "activation container ID changed",
            )));
        }
        state.published = true;
        state.counters.activate += 1;
        self.save(&state);
        pause_if_selected("runtime_published_before_receipt");
        Ok(ContainerCreateReceipt {
            container_id: ownership.container_id.clone(),
            ownership: Some(ownership),
        })
    }

    fn release_container_reservation(
        &self,
        ownership: ContainerGenerationOwnership,
    ) -> Result<ContainerGenerationReleaseOutcome, StackError> {
        let mut state = self.load();
        if !state.reserved {
            return Ok(ContainerGenerationReleaseOutcome::AlreadyAbsent);
        }
        if state.published || state.ownership.as_ref() != Some(&ownership) {
            return Err(state_conflict("reservation release lost exact authority"));
        }
        state.reserved = false;
        self.save(&state);
        Ok(ContainerGenerationReleaseOutcome::Released)
    }

    fn cleanup_container_generation(
        &self,
        ownership: ContainerGenerationOwnership,
    ) -> Result<GenerationCleanupOutcome, StackError> {
        let mut state = self.load();
        if !state.reserved {
            return Ok(GenerationCleanupOutcome::AlreadyAbsent);
        }
        if state.ownership.as_ref() != Some(&ownership) {
            return Err(state_conflict("cleanup lost exact generation authority"));
        }
        state.reserved = false;
        state.published = false;
        state.counters.cleanup += 1;
        self.save(&state);
        Ok(GenerationCleanupOutcome::Removed)
    }

    fn stop_and_remove_container_generation(
        &self,
        ownership: ContainerGenerationOwnership,
        _signal: Option<&str>,
        _grace_period: Option<Duration>,
    ) -> Result<GenerationCleanupOutcome, StackError> {
        self.cleanup_container_generation(ownership)
    }
}

fn state_conflict(message: impl Into<String>) -> StackError {
    StackError::Machine {
        code: vz_runtime_contract::MachineErrorCode::StateConflict,
        message: message.into(),
    }
}

fn inspection_name(value: &ContainerGenerationInspection) -> &'static str {
    match value {
        ContainerGenerationInspection::Absent => "absent",
        ContainerGenerationInspection::ReservedUnpublished(_) => "reserved_unpublished",
        ContainerGenerationInspection::Published(_) => "published",
        ContainerGenerationInspection::Foreign => "foreign",
        ContainerGenerationInspection::Replacement => "replacement",
        ContainerGenerationInspection::LegacyUnscoped => "legacy_unscoped",
        ContainerGenerationInspection::Malformed(_) => "malformed",
    }
}

fn phase_name(value: &ServicePhase) -> &'static str {
    match value {
        ServicePhase::Pending => "pending",
        ServicePhase::Creating => "creating",
        ServicePhase::Running => "running",
        ServicePhase::Stopping => "stopping",
        ServicePhase::Stopped => "stopped",
        ServicePhase::Failed => "failed",
    }
}

fn session_status_name(value: &ReconcileSessionStatus) -> &'static str {
    match value {
        ReconcileSessionStatus::Active => "active",
        ReconcileSessionStatus::Completed => "completed",
        ReconcileSessionStatus::Failed => "failed",
        ReconcileSessionStatus::Superseded => "superseded",
    }
}

fn intent_status_name(value: StackContainerCreateStatus) -> &'static str {
    match value {
        StackContainerCreateStatus::Intent => "intent",
        StackContainerCreateStatus::Reserved => "reserved",
        StackContainerCreateStatus::Running => "running",
        StackContainerCreateStatus::CleanupPending => "cleanup_pending",
        StackContainerCreateStatus::Blocked => "blocked",
        StackContainerCreateStatus::Cleaned => "cleaned",
        StackContainerCreateStatus::Failed => "failed",
    }
}

fn unique_private_dir(name: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = env::temp_dir().join(format!("vz-stack-{name}-{}-{stamp}", std::process::id()));
    fs::create_dir_all(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path
}

fn sync_parent(path: &Path) {
    File::open(path.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
}

fn write_json_durable(path: &Path, value: &impl Serialize) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let temporary = path.with_extension("json.tmp");
    let mut file = File::create(&temporary).unwrap();
    file.write_all(&serde_json::to_vec_pretty(value).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    fs::rename(&temporary, path).unwrap();
    sync_parent(path);
}

pub(crate) fn pause_if_selected(boundary: &str) {
    if env::var(BOUNDARY_ENV).ok().as_deref() != Some(boundary) {
        return;
    }
    let marker = PathBuf::from(env::var_os(MARKER_ENV).unwrap());
    let temporary = marker.with_extension("ready.tmp");
    let mut file = File::create(&temporary).unwrap();
    file.write_all(boundary.as_bytes()).unwrap();
    file.write_all(b"\n").unwrap();
    file.sync_all().unwrap();
    fs::rename(&temporary, &marker).unwrap();
    sync_parent(&marker);
    loop {
        thread::park_timeout(Duration::from_secs(60));
    }
}

fn authority_scope(stack_id: &str) -> MachineWorkloadScope {
    MachineWorkloadScope {
        schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
        project_id: ProjectId::new(format!("prj_{stack_id}")).unwrap(),
        environment_id: EnvironmentId::new(format!("env_{stack_id}")).unwrap(),
        machine_id: MachineId::new(format!("mch_{stack_id}")).unwrap(),
        machine_incarnation_id: MachineIncarnationId::new(format!("inc_{stack_id}")).unwrap(),
        stack_id: stack_id.to_string(),
    }
}

fn install_authority(store: &StateStore, stack_id: &str) -> MachineWorkloadScope {
    let scope = authority_scope(stack_id);
    if store.load_stack_workload_owner(stack_id).unwrap().is_some() {
        store.validate_stack_workload_owner(&scope).unwrap();
        return scope;
    }
    let capabilities = CapabilitySet::new([
        MachineCapability::DockerEngine,
        MachineCapability::Compose,
        MachineCapability::Buildx,
    ]);
    let target = TargetSpec {
        os: OperatingSystem::Linux,
        arch: Architecture::Aarch64,
        image: "crash-fixture:latest".to_string(),
        version: None,
        channel: None,
        digest: Some("sha256:crash-fixture".to_string()),
    };
    let definition = ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: scope.project_id.clone(),
        name: format!("project-{stack_id}"),
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
            networks: vec![],
            endpoints: vec![],
        },
    };
    let environment = EnvironmentInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        environment_id: scope.environment_id.clone(),
        project_id: scope.project_id.clone(),
        name: "test".to_string(),
        definition_digest: definition.digest().unwrap(),
        state: EnvironmentState::Ready,
        lifecycle_generation: 0,
        active_operation_id: None,
        bindings: vec![],
        machines: vec![MachineInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            machine_id: scope.machine_id.clone(),
            environment_id: scope.environment_id.clone(),
            name: "linux".to_string(),
            profile: MachineProfile::Developer,
            target,
            resources: MachineResources::default(),
            requested_capabilities: capabilities.clone(),
            negotiated_capabilities: capabilities,
            backend: None,
            incarnation: Some(MachineIncarnation {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                incarnation_id: scope.machine_incarnation_id.clone(),
                machine_id: scope.machine_id.clone(),
                generation: 1,
                created_at: 1,
            }),
            state: MachineState::Ready,
            legacy_sandbox_id: None,
        }],
        networks: vec![],
        endpoints: vec![],
        ownership: vec![
            OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::Incarnation,
                resource_id: scope.machine_incarnation_id.to_string(),
                environment_id: scope.environment_id.clone(),
                machine_id: Some(scope.machine_id.clone()),
            },
            OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::Machine,
                resource_id: scope.machine_id.to_string(),
                environment_id: scope.environment_id.clone(),
                machine_id: Some(scope.machine_id.clone()),
            },
        ],
        legacy_migration: None,
        created_at: 1,
        updated_at: 1,
    };
    store
        .save_project_state(&ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments: vec![environment],
        })
        .unwrap();
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    scope
}

fn stack_spec(stack_id: &str) -> StackSpec {
    parse_compose(
        r#"services:
  worker:
    image: alpine:latest
    command: ["sleep", "300"]
    healthcheck:
      test: ["CMD", "true"]
"#,
        stack_id,
    )
    .unwrap()
}

fn executor_for(root: &Path, stack_id: &str) -> StackExecutor<DurableCrashRuntime> {
    let store = StateStore::open(&root.join("state.db")).unwrap();
    let scope = install_authority(&store, stack_id);
    let data_dir = root.join("executor");
    fs::create_dir_all(&data_dir).unwrap();
    fs::set_permissions(&data_dir, fs::Permissions::from_mode(0o700)).unwrap();
    StackExecutor::new_scoped(
        DurableCrashRuntime::new(root.join("runtime.json")),
        store,
        &data_dir,
        scope,
    )
    .unwrap()
}

fn initial_actions(store: &StateStore, spec: &StackSpec) -> Vec<Action> {
    let actions = plan_apply(spec, store, &std::collections::HashMap::new())
        .unwrap()
        .actions;
    assert_eq!(actions.len(), 1);
    assert!(matches!(actions[0], Action::ServiceCreate { .. }));
    actions
}

fn session_id(boundary: &str) -> String {
    format!("rs-{boundary}")
}

fn operation_id(boundary: &str) -> String {
    format!("op-{boundary}")
}

fn run_child(root: &Path, boundary: &str) {
    let stack_id = format!("crash-{boundary}");
    let spec = stack_spec(&stack_id);
    let mut executor = executor_for(root, &stack_id);
    let actions = initial_actions(executor.store(), &spec);
    let _ = executor.execute_claimed_batch(
        &spec,
        &actions,
        &session_id(boundary),
        &operation_id(boundary),
        0,
    );
    panic!("crash child escaped boundary {boundary}");
}

#[test]
#[ignore = "subprocess helper for Action-v3 crash/reopen evidence"]
fn action_v3_state_store_crash_child() {
    let Ok(boundary) = env::var(BOUNDARY_ENV) else {
        return;
    };
    assert!(BOUNDARIES.contains(&boundary.as_str()));
    run_child(&PathBuf::from(env::var_os(ROOT_ENV).unwrap()), &boundary);
}

fn wait_then_kill(root: &Path, boundary: &str) {
    let marker = root.join("boundary.ready");
    let mut child = Command::new(env::current_exe().unwrap())
        .args(["--ignored", "--nocapture", "--exact", CHILD_TEST])
        .env(BOUNDARY_ENV, boundary)
        .env(ROOT_ENV, root)
        .env(MARKER_ENV, &marker)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !marker.is_file() {
        assert!(
            std::time::Instant::now() < deadline,
            "child did not reach {boundary}"
        );
        assert!(
            child.try_wait().unwrap().is_none(),
            "child exited before {boundary}"
        );
        thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert_eq!(status.signal(), Some(9));
}

fn store_snapshot(
    store: &StateStore,
    session_id: &str,
    ownership: &ContainerGenerationOwnership,
) -> Value {
    let session = store.load_reconcile_session(session_id).unwrap().unwrap();
    let audits = store.load_audit_log_for_session(session_id).unwrap();
    let reservation_id = &ownership.scope.as_deref().unwrap().reservation_id;
    let intent = store
        .load_stack_container_create_intent(reservation_id)
        .unwrap()
        .unwrap();
    let binding = store
        .load_stack_container_generation_binding(reservation_id)
        .unwrap();
    let observed = store
        .load_observed_state_for_replica(
            &intent.scope.stack_id,
            &intent.service_name,
            intent.replica_index,
        )
        .unwrap()
        .unwrap();
    let events = store.load_events(&intent.scope.stack_id).unwrap();
    let count = |predicate: fn(&StackEvent) -> bool| events.iter().filter(|e| predicate(e)).count();
    json!({
        "schema_version": store.schema_version().unwrap(),
        "action_schema_version": store
            .reconcile_action_schema_version_for_test(session_id)
            .unwrap(),
        "session_status": session_status_name(&session.status),
        "session_cursor": session.next_action_index,
        "session_actions_hash": session.actions_hash,
        "audit_rows": audits.len(),
        "audit_status": audits.first().map(|entry| entry.status.as_str()).unwrap_or("missing"),
        "audit_action_hash": audits.first().map(|entry| entry.action_hash.as_str()).unwrap_or("missing"),
        "intent_status": intent_status_name(intent.status),
        "observed_phase": phase_name(&observed.phase),
        "ready": observed.ready,
        "binding": binding,
        "event_counts": {
            "creating": count(|event| matches!(event, StackEvent::ServiceCreating { .. })),
            "ready": count(|event| matches!(event, StackEvent::ServiceReady { .. })),
            "failed": count(|event| matches!(event, StackEvent::ServiceFailed { .. })),
            "stopping": count(|event| matches!(event, StackEvent::ServiceStopping { .. })),
            "stopped": count(|event| matches!(event, StackEvent::ServiceStopped { .. })),
        },
    })
}

fn assert_boundary_contract(evidence: &Value) {
    let boundary = evidence["boundary"].as_str().unwrap();
    let pre_store = &evidence["pre_replay"]["store"];
    let pre_runtime = &evidence["pre_replay"]["runtime"];
    let replay = &evidence["replay"];
    let post_store = &evidence["post_replay"]["store"];
    let post_runtime = &evidence["post_replay"]["runtime"];
    assert_eq!(pre_store["schema_version"], 9);
    assert_eq!(pre_store["action_schema_version"], 3);
    assert_eq!(pre_store["session_status"], "active");
    assert_eq!(pre_store["session_cursor"], 0);
    assert_eq!(pre_store["audit_rows"], 1);
    assert_eq!(pre_store["audit_status"], "started");
    assert_eq!(
        pre_store["session_actions_hash"],
        pre_store["audit_action_hash"]
    );
    assert_eq!(pre_store["ready"], false);
    assert_eq!(post_store["schema_version"], 9);
    assert_eq!(post_store["action_schema_version"], 3);
    assert_eq!(post_store["audit_rows"], 1);
    assert_eq!(
        post_store["session_actions_hash"],
        post_store["audit_action_hash"]
    );
    assert_eq!(post_store["ready"], false);

    match boundary {
        "successor_bound_before_activation" => {
            assert_eq!(pre_store["intent_status"], "reserved");
            assert_eq!(pre_store["observed_phase"], "creating");
            assert_eq!(pre_runtime["inspection"], "reserved_unpublished");
            assert_eq!(
                pre_runtime["counters"],
                json!({"reserve":1,"activate":0,"cleanup":0})
            );
            assert_eq!(replay["succeeded"], 1);
            assert_eq!(replay["failed"], 0);
            assert_eq!(
                replay["runtime_deltas"],
                json!({"reserve":0,"activate":1,"cleanup":0})
            );
            assert_eq!(post_store["session_status"], "completed");
            assert_eq!(post_store["session_cursor"], 1);
            assert_eq!(post_store["audit_status"], "completed");
            assert_eq!(post_store["intent_status"], "running");
            assert_eq!(post_store["observed_phase"], "running");
            assert_eq!(
                post_store["event_counts"],
                json!({"creating":1,"ready":0,"failed":0,"stopping":0,"stopped":0})
            );
            assert_eq!(post_runtime["inspection"], "published");
            assert_eq!(
                post_runtime["counters"],
                json!({"reserve":1,"activate":1,"cleanup":0})
            );
        }
        "runtime_published_before_receipt" | "observed_upsert_before_intent_cas" => {
            assert_eq!(pre_store["intent_status"], "reserved");
            assert_eq!(pre_store["observed_phase"], "creating");
            assert_eq!(pre_runtime["inspection"], "published");
            assert_eq!(
                pre_runtime["counters"],
                json!({"reserve":1,"activate":1,"cleanup":0})
            );
            assert_eq!(replay["succeeded"], 0);
            assert_eq!(replay["failed"], 1);
            assert_eq!(
                replay["runtime_deltas"],
                json!({"reserve":0,"activate":0,"cleanup":1})
            );
            assert_eq!(post_store["session_status"], "failed");
            assert_eq!(post_store["session_cursor"], 0);
            assert_eq!(post_store["audit_status"], "failed");
            assert_eq!(post_store["intent_status"], "cleaned");
            assert_eq!(post_store["observed_phase"], "stopped");
            assert_eq!(
                post_store["event_counts"],
                json!({"creating":1,"ready":0,"failed":1,"stopping":1,"stopped":1})
            );
            assert_eq!(post_runtime["inspection"], "absent");
            assert_eq!(
                post_runtime["counters"],
                json!({"reserve":1,"activate":1,"cleanup":1})
            );
        }
        "running_committed_before_batch_commit" => {
            assert_eq!(pre_store["intent_status"], "running");
            assert_eq!(pre_store["observed_phase"], "running");
            assert_eq!(
                pre_store["event_counts"],
                json!({"creating":1,"ready":0,"failed":0,"stopping":0,"stopped":0})
            );
            assert_eq!(pre_runtime["inspection"], "published");
            assert_eq!(
                pre_runtime["counters"],
                json!({"reserve":1,"activate":1,"cleanup":0})
            );
            assert_eq!(replay["succeeded"], 1);
            assert_eq!(replay["failed"], 0);
            assert_eq!(
                replay["runtime_deltas"],
                json!({"reserve":0,"activate":0,"cleanup":0})
            );
            assert_eq!(post_store["session_status"], "completed");
            assert_eq!(post_store["session_cursor"], 1);
            assert_eq!(post_store["audit_status"], "completed");
            assert_eq!(post_store["intent_status"], "running");
            assert_eq!(post_store["observed_phase"], "running");
            assert_eq!(post_store["event_counts"], pre_store["event_counts"]);
            assert_eq!(post_runtime["inspection"], "published");
            assert_eq!(post_runtime["counters"], pre_runtime["counters"]);
        }
        other => panic!("unexpected crash boundary {other}"),
    }
}

fn logical_sha256(value: &Value) -> String {
    format!("{:x}", Sha256::digest(serde_json::to_vec(value).unwrap()))
}

fn run_boundary(root: &Path, boundary: &str) -> Value {
    fs::create_dir_all(root).unwrap();
    fs::set_permissions(root, fs::Permissions::from_mode(0o700)).unwrap();
    wait_then_kill(root, boundary);

    let runtime = DurableCrashRuntime::new(root.join("runtime.json"));
    let runtime_state = runtime.load();
    let ownership = runtime_state.ownership.clone().unwrap();
    let stack_id = ownership.stack_id.clone();
    let spec = stack_spec(&stack_id);
    let mut executor = executor_for(root, &stack_id);
    let actions = executor
        .store()
        .load_reconcile_session_actions(&session_id(boundary))
        .unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(
        compute_actions_hash(&actions),
        executor
            .store()
            .load_reconcile_session(&session_id(boundary))
            .unwrap()
            .unwrap()
            .actions_hash
    );
    let pre_store = store_snapshot(executor.store(), &session_id(boundary), &ownership);
    let pre_runtime = runtime.snapshot(&ownership);
    let before_counters = runtime.load().counters;
    let replay = executor
        .execute_claimed_batch(
            &spec,
            &actions,
            &session_id(boundary),
            &operation_id(boundary),
            0,
        )
        .unwrap();
    let after_counters = runtime.load().counters;
    let post_store = store_snapshot(executor.store(), &session_id(boundary), &ownership);
    let post_runtime = runtime.snapshot(&ownership);

    let evidence = json!({
        "boundary": boundary,
        "child": {"signal": "SIGKILL", "expected_exit_code": 137},
        "ownership": ownership,
        "pre_replay": {"store": pre_store, "runtime": pre_runtime},
        "replay": {
            "succeeded": replay.succeeded,
            "failed": replay.failed,
            "runtime_deltas": after_counters.delta(before_counters),
        },
        "post_replay": {"store": post_store, "runtime": post_runtime},
    });
    assert_boundary_contract(&evidence);
    evidence
}

fn foreign_receipt_zero_write(root: &Path) -> Value {
    fs::create_dir_all(root).unwrap();
    fs::set_permissions(root, fs::Permissions::from_mode(0o700)).unwrap();
    let stack_id = "crash-foreign-receipt";
    let spec = stack_spec(stack_id);
    let executor = executor_for(root, stack_id);
    let actions = initial_actions(executor.store(), &spec);
    let session = ReconcileSession {
        session_id: "rs-foreign-receipt".to_string(),
        stack_name: stack_id.to_string(),
        operation_id: "op-foreign-receipt".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 1,
        started_at: 1,
        updated_at: 1,
        completed_at: None,
    };
    executor
        .store()
        .create_reconcile_batch(&session, &actions)
        .unwrap();
    let claim = executor
        .store()
        .start_reconcile_batch(
            &session.session_id,
            stack_id,
            &session.operation_id,
            0,
            &actions,
        )
        .unwrap()
        .remove(0);
    let scope = authority_scope(stack_id);
    let project = executor
        .store()
        .load_project_state(scope.project_id.as_str())
        .unwrap()
        .unwrap();
    let environment = project
        .environments
        .iter()
        .find(|environment| environment.environment_id == scope.environment_id)
        .unwrap();
    let input = ClaimedCreateInput {
        requested_container_id: "ctr-foreign-receipt".to_string(),
        definition_digest: environment.definition_digest.clone(),
        applied_config_digest: "vzsc1-sha256:foreign-receipt".to_string(),
        activation_payload_sha256: "c".repeat(64),
    };
    let intent = executor
        .store()
        .resolve_or_begin_claimed_successor(
            &claim,
            &input,
            &ClaimedAllocatorTarget {
                ports: vec![],
                service_ip: None,
                service_network_ips: vec![],
                mount_tag_offset: None,
            },
            2,
        )
        .unwrap();
    let runtime = DurableCrashRuntime::new(root.join("runtime.json"));
    let ownership = runtime
        .reserve_container_generation(&intent.scope, &intent.requested_container_id)
        .unwrap();
    let binding = StackContainerGenerationBinding {
        reservation_id: intent.scope.reservation_id.clone(),
        service_name: intent.service_name.clone(),
        ownership: ownership.clone(),
        bound_at: 3,
    };
    executor
        .store()
        .bind_claimed_successor_generation(&claim, &binding)
        .unwrap();
    let before = store_snapshot(executor.store(), &session.session_id, &ownership);
    let before_hash = logical_sha256(&before);
    let before_changes = executor.store().total_changes_for_test();
    let before_counters = runtime.load().counters;
    let mut foreign = ownership.clone();
    foreign.generation += 1;
    let error = executor
        .store()
        .publish_claimed_successor_success(
            &claim,
            &intent.scope.reservation_id,
            &ContainerCreateReceipt {
                container_id: ownership.container_id.clone(),
                ownership: Some(foreign),
            },
            false,
            4,
        )
        .unwrap_err();
    let after = store_snapshot(executor.store(), &session.session_id, &ownership);
    let after_changes = executor.store().total_changes_for_test();
    let after_counters = runtime.load().counters;
    json!({
        "machine_code": error.machine_code().as_str(),
        "total_changes_delta": after_changes - before_changes,
        "logical_sha256_before": before_hash,
        "logical_sha256_after": logical_sha256(&after),
        "runtime_deltas": after_counters.delta(before_counters),
    })
}

#[test]
#[ignore = "release-built local-process SIGKILL/reopen companion evidence"]
fn action_v3_state_store_sigkill_crash_reopen() {
    let root = unique_private_dir("action-v3-crash-reopen");
    let boundaries = BOUNDARIES
        .iter()
        .map(|boundary| run_boundary(&root.join(boundary), boundary))
        .collect::<Vec<_>>();
    let receipt = foreign_receipt_zero_write(&root.join("foreign-receipt"));
    assert_eq!(receipt["machine_code"], "state_conflict");
    assert_eq!(receipt["total_changes_delta"], 0);
    assert_eq!(
        receipt["logical_sha256_before"],
        receipt["logical_sha256_after"]
    );

    let binary = env::current_exe().unwrap();
    let binary_sha256 = format!("{:x}", Sha256::digest(fs::read(&binary).unwrap()));
    let evidence = json!({
        "schema_version": 7,
        "scenario": "runtime-generation-state-store-v7",
        "coverage_classification": "action_v3_executor_state_store_atomicity",
        "action_schema_version": 3,
        "build_identity": {
            "profile": env::var("VZ_STACK_CRASH_BUILD_PROFILE").unwrap_or_else(|_| "debug".to_string()),
            "test_binary_sha256": env::var("VZ_STACK_CRASH_TEST_BINARY_SHA256").unwrap_or(binary_sha256),
        },
        "runtime_store_companion": {
            "scenario": "runtime-generation-crash-reopen",
            "sha256": env::var("VZ_RUNTIME_CRASH_REOPEN_SHA256_VALUE").unwrap_or_else(|_| "0".repeat(64)),
        },
        "controls": {
            "harness_invocations": 1,
            "child_processes": 4,
            "sigkills": 4,
            "reopen_replays": 4,
            "fallbacks": 0,
            "skips": 0,
        },
        "boundaries": boundaries,
        "foreign_receipt_zero_write": receipt,
    });
    let rendered = serde_json::to_string_pretty(&evidence).unwrap();
    eprintln!("VZ_STACK_CRASH_REOPEN_EVIDENCE={rendered}");
    if let Some(path) = env::var_os("VZ_STACK_CRASH_REOPEN_EVIDENCE") {
        write_json_durable(&PathBuf::from(path), &evidence);
    }
}
