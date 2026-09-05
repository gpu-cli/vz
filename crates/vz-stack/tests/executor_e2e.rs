//! End-to-end stack integration tests.
//!
//! Exercises the full pipeline: parse compose YAML → reconcile →
//! execute via mock runtime → verify container state, health checks,
//! restart policies, and port allocation.

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};

use vz_runtime_contract::{
    Architecture, CapabilitySet, EnvironmentId, EnvironmentInstance, EnvironmentSpec,
    EnvironmentState, MachineCapability, MachineId, MachineIncarnation, MachineIncarnationId,
    MachineInstance, MachineProfile, MachineResources, MachineSpec, MachineState,
    MachineWorkloadScope, OperatingSystem, OwnedResourceKind, OwnershipRecord, ProjectDefinition,
    ProjectId, ProjectState, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};
use vz_stack::{
    Action, ContainerRuntime, HealthPoller, OrchestrationConfig, RestartTracker,
    ServiceObservedState, ServicePhase, StackError, StackEvent, StackExecutor, StackOrchestrator,
    StackSpec, StateStore, compute_restart_targets, parse_compose,
};

fn install_planning_authority(store: &StateStore, stack_id: &str) -> MachineWorkloadScope {
    let project_id = ProjectId::new("prj_executor_fixture").unwrap();
    let environment_id = EnvironmentId::new("env_executor_fixture").unwrap();
    let machine_id = MachineId::new("mch_executor_fixture").unwrap();
    let incarnation_id = MachineIncarnationId::new("inc_executor_fixture").unwrap();
    if store.load_stack_workload_owner(stack_id).unwrap().is_some() {
        return MachineWorkloadScope {
            schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
            project_id,
            environment_id,
            machine_id,
            machine_incarnation_id: incarnation_id,
            stack_id: stack_id.to_string(),
        };
    }
    if store
        .load_project_state(project_id.as_str())
        .unwrap()
        .is_none()
    {
        let capabilities = CapabilitySet::new([
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ]);
        let target = TargetSpec {
            os: OperatingSystem::Linux,
            arch: Architecture::Aarch64,
            image: "fixture:latest".to_string(),
            version: None,
            channel: None,
            digest: Some("sha256:executor-fixture".to_string()),
        };
        let definition = ProjectDefinition {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: project_id.clone(),
            name: "executor-fixture".to_string(),
            environment: EnvironmentSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                default_machine: None,
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
            environment_id: environment_id.clone(),
            project_id: project_id.clone(),
            name: "test".to_string(),
            definition_digest: definition.digest().unwrap(),
            state: EnvironmentState::Ready,
            lifecycle_generation: 0,
            active_operation_id: None,
            bindings: vec![],
            machines: vec![MachineInstance {
                docker_context: None,
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
                runtime_identity: None,
                legacy_sandbox_id: None,
            }],
            networks: vec![],
            endpoints: vec![],
            ownership: vec![
                OwnershipRecord {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    resource_kind: OwnedResourceKind::Incarnation,
                    resource_id: incarnation_id.to_string(),
                    environment_id: environment_id.clone(),
                    machine_id: Some(machine_id.clone()),
                },
                OwnershipRecord {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    resource_kind: OwnedResourceKind::Machine,
                    resource_id: machine_id.to_string(),
                    environment_id: environment_id.clone(),
                    machine_id: Some(machine_id.clone()),
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
    }
    let scope = MachineWorkloadScope {
        schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
        project_id,
        environment_id,
        machine_id,
        machine_incarnation_id: incarnation_id,
        stack_id: stack_id.to_string(),
    };
    store.reserve_stack_workload_owner(&scope, 1).unwrap();
    scope
}

fn apply_test(
    spec: &StackSpec,
    store: &StateStore,
    health: &HashMap<String, vz_stack::HealthStatus>,
) -> Result<vz_stack::ApplyResult, StackError> {
    install_planning_authority(store, &spec.name);
    vz_stack::apply(spec, store, health)
}

// ── Mock runtime for integration tests ───────────────────────────

struct MockRuntime {
    container_ids: Vec<String>,
    exec_exit_code: AtomicI32,
    calls: Mutex<Vec<(String, String)>>,
    create_counter: AtomicUsize,
    scoped_generations:
        Mutex<HashMap<String, (vz_runtime_contract::ContainerGenerationOwnership, bool)>>,
    next_scoped_generation: AtomicU64,
}

impl MockRuntime {
    fn new(ids: Vec<&str>) -> Self {
        Self {
            container_ids: ids.into_iter().map(String::from).collect(),
            exec_exit_code: AtomicI32::new(0),
            calls: Mutex::new(Vec::new()),
            create_counter: AtomicUsize::new(0),
            scoped_generations: Mutex::new(HashMap::new()),
            next_scoped_generation: AtomicU64::new(1),
        }
    }

    fn call_log(&self) -> Vec<(String, String)> {
        self.calls.lock().unwrap().clone()
    }

    fn set_exec_exit_code(&self, code: i32) {
        self.exec_exit_code.store(code, Ordering::SeqCst);
    }
}

impl ContainerRuntime for MockRuntime {
    fn pull(&self, image: &str) -> Result<String, StackError> {
        self.calls
            .lock()
            .unwrap()
            .push(("pull".into(), image.into()));
        Ok(format!("sha256:{image}"))
    }

    fn create(
        &self,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<String, StackError> {
        self.calls
            .lock()
            .unwrap()
            .push(("create".into(), image.into()));
        let id = config
            .container_id
            .as_ref()
            .map(|name| format!("ctr-{name}"))
            .unwrap_or_else(|| {
                let idx = self.create_counter.fetch_add(1, Ordering::SeqCst);
                self.container_ids[idx % self.container_ids.len()].clone()
            });
        Ok(id)
    }

    fn stop(
        &self,
        container_id: &str,
        _signal: Option<&str>,
        _grace_period: Option<std::time::Duration>,
    ) -> Result<(), StackError> {
        self.calls
            .lock()
            .unwrap()
            .push(("stop".into(), container_id.into()));
        Ok(())
    }

    fn remove(&self, container_id: &str) -> Result<(), StackError> {
        self.calls
            .lock()
            .unwrap()
            .push(("remove".into(), container_id.into()));
        Ok(())
    }

    fn create_in_sandbox_owned(
        &self,
        sandbox_id: &str,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<
        vz_runtime_contract::ContainerCreateReceipt,
        vz_runtime_contract::OwnedCreateError<StackError>,
    > {
        self.create_in_sandbox(sandbox_id, image, config)
            .map(|container_id| vz_runtime_contract::ContainerCreateReceipt {
                ownership: Some(vz_runtime_contract::ContainerGenerationOwnership {
                    container_id: container_id.clone(),
                    generation: 1,
                    stack_id: sandbox_id.to_string(),
                    scope: Some(Box::new(
                        vz_runtime_contract::ContainerGenerationScope::synthetic_legacy_stack(
                            sandbox_id,
                        )
                        .expect("test sandbox ID must form a valid legacy scope"),
                    )),
                }),
                container_id,
            })
            .map_err(|error| vz_runtime_contract::OwnedCreateError {
                error,
                cleanup: None,
            })
    }

    fn reserve_container_generation(
        &self,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        container_id: &str,
    ) -> Result<vz_runtime_contract::ContainerGenerationOwnership, StackError> {
        let mut generations = self.scoped_generations.lock().unwrap();
        if let Some((ownership, _)) = generations.get(container_id) {
            if ownership.scope.as_deref() == Some(scope) {
                return Ok(ownership.clone());
            }
            return Err(StackError::InvalidSpec(
                "mock container ID has foreign ownership".to_string(),
            ));
        }
        let ownership = vz_runtime_contract::ContainerGenerationOwnership {
            container_id: container_id.to_string(),
            generation: self.next_scoped_generation.fetch_add(1, Ordering::SeqCst),
            stack_id: scope.stack_id.clone(),
            scope: Some(Box::new(scope.clone())),
        };
        generations.insert(container_id.to_string(), (ownership.clone(), false));
        self.calls
            .lock()
            .unwrap()
            .push(("reserve_scoped".into(), container_id.to_string()));
        Ok(ownership)
    }

    fn inspect_container_reservation(
        &self,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        container_id: &str,
    ) -> Result<vz_runtime_contract::ContainerGenerationInspection, StackError> {
        let generations = self.scoped_generations.lock().unwrap();
        let Some((ownership, published)) = generations.get(container_id) else {
            return Ok(vz_runtime_contract::ContainerGenerationInspection::Absent);
        };
        if ownership.scope.as_deref() != Some(scope) {
            return Ok(vz_runtime_contract::ContainerGenerationInspection::Foreign);
        }
        Ok(if *published {
            vz_runtime_contract::ContainerGenerationInspection::Published(ownership.clone())
        } else {
            vz_runtime_contract::ContainerGenerationInspection::ReservedUnpublished(
                ownership.clone(),
            )
        })
    }

    fn inspect_container_generation(
        &self,
        ownership: &vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::ContainerGenerationInspection, StackError> {
        let generations = self.scoped_generations.lock().unwrap();
        let Some((found, published)) = generations.get(&ownership.container_id) else {
            return Ok(vz_runtime_contract::ContainerGenerationInspection::Absent);
        };
        if found.scope != ownership.scope {
            return Ok(vz_runtime_contract::ContainerGenerationInspection::Foreign);
        }
        if found.generation != ownership.generation {
            return Ok(vz_runtime_contract::ContainerGenerationInspection::Replacement);
        }
        Ok(if *published {
            vz_runtime_contract::ContainerGenerationInspection::Published(found.clone())
        } else {
            vz_runtime_contract::ContainerGenerationInspection::ReservedUnpublished(found.clone())
        })
    }

    fn activate_container_generation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
        image: &str,
        _config: vz_runtime_contract::RunConfig,
    ) -> Result<
        vz_runtime_contract::ContainerCreateReceipt,
        vz_runtime_contract::OwnedCreateError<StackError>,
    > {
        let mut generations = self.scoped_generations.lock().unwrap();
        let Some((found, published)) = generations.get_mut(&ownership.container_id) else {
            return Err(vz_runtime_contract::OwnedCreateError {
                error: StackError::InvalidSpec(
                    "mock activation lacks exact reservation".to_string(),
                ),
                cleanup: None,
            });
        };
        if found != &ownership || *published {
            return Err(vz_runtime_contract::OwnedCreateError {
                error: StackError::InvalidSpec(
                    "mock activation lacks exact unpublished reservation".to_string(),
                ),
                cleanup: None,
            });
        }
        *published = true;
        self.calls
            .lock()
            .unwrap()
            .push(("activate_scoped".into(), image.to_string()));
        Ok(vz_runtime_contract::ContainerCreateReceipt {
            container_id: ownership.container_id.clone(),
            ownership: Some(ownership),
        })
    }

    fn release_container_reservation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::ContainerGenerationReleaseOutcome, StackError> {
        let mut generations = self.scoped_generations.lock().unwrap();
        match generations.get(&ownership.container_id) {
            None => Ok(vz_runtime_contract::ContainerGenerationReleaseOutcome::AlreadyAbsent),
            Some((found, false)) if found == &ownership => {
                generations.remove(&ownership.container_id);
                Ok(vz_runtime_contract::ContainerGenerationReleaseOutcome::Released)
            }
            _ => Err(StackError::InvalidSpec(
                "mock release lacks exact unpublished ownership".to_string(),
            )),
        }
    }

    fn cleanup_container_generation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::GenerationCleanupOutcome, StackError> {
        self.calls.lock().unwrap().push((
            "cleanup_container_generation".into(),
            ownership.container_id,
        ));
        Ok(vz_runtime_contract::GenerationCleanupOutcome::Removed)
    }

    fn stop_and_remove_container_generation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
        _signal: Option<&str>,
        _grace_period: Option<std::time::Duration>,
    ) -> Result<vz_runtime_contract::GenerationCleanupOutcome, StackError> {
        self.scoped_generations
            .lock()
            .unwrap()
            .remove(&ownership.container_id);
        self.calls.lock().unwrap().push((
            "stop_and_remove_container_generation".into(),
            ownership.container_id,
        ));
        Ok(vz_runtime_contract::GenerationCleanupOutcome::Removed)
    }

    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError> {
        self.calls.lock().unwrap().push((
            "exec".into(),
            format!("{container_id}:{}", command.join(" ")),
        ));
        Ok(self.exec_exit_code.load(Ordering::SeqCst))
    }
}

// ── Helpers ──────────────────────────────────────────────────────

// ── Full pipeline: parse → reconcile → execute ──────────────────

const SIMPLE_COMPOSE: &str = r#"
services:
  web:
    image: nginx:latest
    ports:
      - "8080:80"

  api:
    image: node:20
    ports:
      - "3000:3000"
    depends_on:
      - web
"#;

#[test]
fn full_pipeline_parse_apply_execute() {
    let spec = parse_compose(SIMPLE_COMPOSE, "myapp").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(&dir.path().join("state.db")).unwrap();

    // Step 1: Reconcile first round (strict dependency gating starts roots first).
    let health = HashMap::new();
    let first = apply_test(&spec, &store, &health).unwrap();
    assert_eq!(first.actions.len(), 1);
    assert!(matches!(
        &first.actions[0],
        Action::ServiceCreate { target, .. } if target.service_name == "web"
    ));

    // Step 2: Execute first round through mock runtime.
    let runtime = MockRuntime::new(vec!["ctr-web", "ctr-api"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());

    let first_exec = executor.execute(&spec, &first.actions).unwrap();
    assert!(first_exec.all_succeeded());
    assert_eq!(first_exec.succeeded, 1);

    // Step 3: Reconcile + execute second round (api unblocked once web is running).
    let second = apply_test(&spec, &store, &health).unwrap();
    assert_eq!(second.actions.len(), 1);
    assert!(matches!(
        &second.actions[0],
        Action::ServiceCreate { target, .. } if target.service_name == "api"
    ));
    let second_exec = executor.execute(&spec, &second.actions).unwrap();
    assert!(second_exec.all_succeeded());
    assert_eq!(second_exec.succeeded, 1);

    // Step 4: Verify observed state.
    let observed = executor.store().load_observed_state("myapp").unwrap();
    assert_eq!(observed.len(), 2);

    let web = observed
        .iter()
        .find(|o| o.replica.service_name == "web")
        .unwrap();
    assert_eq!(web.phase, ServicePhase::Running);
    assert!(
        web.container_id
            .as_deref()
            .is_some_and(|id| id.starts_with("ctr-vzs1-myapp-web-"))
    );

    let api = observed
        .iter()
        .find(|o| o.replica.service_name == "api")
        .unwrap();
    assert_eq!(api.phase, ServicePhase::Running);
    assert!(
        api.container_id
            .as_deref()
            .is_some_and(|id| id.starts_with("ctr-vzs1-myapp-api-"))
    );

    // Step 5: Verify events emitted.
    // Note: apply() also emits ServiceCreating events, so we get 2 from apply + 2 from executor = 4.
    // ServiceReady events are only emitted by the executor (2 total).
    let events = executor.store().load_events("myapp").unwrap();
    let creating_count = events
        .iter()
        .filter(|e| matches!(e, StackEvent::ServiceCreating { .. }))
        .count();
    assert_eq!(creating_count, 4);

    let ready_count = events
        .iter()
        .filter(|e| matches!(e, StackEvent::ServiceReady { .. }))
        .count();
    assert_eq!(ready_count, 2);
}

#[test]
fn full_pipeline_up_then_down() {
    let spec = parse_compose(SIMPLE_COMPOSE, "myapp").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let scope = install_planning_authority(&store, &spec.name);
    let runtime = MockRuntime::new(vec!["ctr-web", "ctr-api"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let stack_data_dir = dir.path().join("runtime").join("stacks").join(&spec.name);
    assert!(!stack_data_dir.exists());
    let executor = StackExecutor::new_scoped(runtime, exec_store, &stack_data_dir, scope).unwrap();
    let mut orchestrator = StackOrchestrator::new(
        executor,
        store,
        OrchestrationConfig {
            poll_interval: Some(0),
            max_rounds: 4,
            ..OrchestrationConfig::default()
        },
    );
    let up = orchestrator.run(&spec, None).unwrap();
    assert!(up.converged);
    assert_eq!(up.rounds, 2);

    // Verify running.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("myapp")
        .unwrap();
    assert_eq!(observed.len(), 2);
    assert!(observed.iter().all(|o| o.phase == ServicePhase::Running));

    // DOWN: the orchestrator captures the exact journal predecessors and
    // executes cleanup under the persisted reconcile session.
    let empty = StackSpec {
        name: "myapp".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&empty, None).unwrap();
    assert!(down.converged);

    // Verify stopped.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("myapp")
        .unwrap();
    assert!(
        observed.iter().all(|o| o.phase == ServicePhase::Stopped),
        "all services should be stopped: {observed:?}"
    );

    // Verify runtime calls use exact generation-qualified cleanup.
    let calls = orchestrator.executor().runtime().call_log();
    let cleanup_count = calls
        .iter()
        .filter(|(op, _)| op == "stop_and_remove_container_generation")
        .count();
    assert_eq!(cleanup_count, 2);
    assert!(!calls.iter().any(|(op, _)| op == "stop" || op == "remove"));
}

// ── Health check integration ────────────────────────────────────

const HEALTHCHECK_COMPOSE: &str = r#"
services:
  db:
    image: postgres:16
    healthcheck:
      test: ["CMD", "pg_isready"]
      interval: 0s
      retries: 2

  app:
    image: myapp:latest
    depends_on:
      db:
        condition: service_healthy
"#;

#[test]
fn health_check_gates_dependent_service() {
    let spec = parse_compose(HEALTHCHECK_COMPOSE, "hc-test").unwrap();
    let dir = tempfile::tempdir().unwrap();

    // Initial apply creates only db; app is gated on db health.
    let store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let health = HashMap::new();
    let first = apply_test(&spec, &store, &health).unwrap();
    assert_eq!(first.actions.len(), 1);
    assert!(matches!(
        &first.actions[0],
        Action::ServiceCreate { target, .. } if target.service_name == "db"
    ));

    // Execute first round: db starts running.
    let runtime = MockRuntime::new(vec!["ctr-db", "ctr-app"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());
    executor.execute(&spec, &first.actions).unwrap();

    // Health check: db returns healthy.
    let mut poller = HealthPoller::new();
    let poll_result = poller
        .poll_all(executor.runtime(), executor.store(), &spec)
        .unwrap();
    assert_eq!(poll_result.newly_ready, vec!["db".to_string()]);

    // Verify db is now ready.
    let observed = executor.store().load_observed_state("hc-test").unwrap();
    let db = observed
        .iter()
        .find(|o| o.replica.service_name == "db")
        .unwrap();
    assert!(db.ready);

    // Reconcile again: app is now unblocked by healthy db.
    let second = apply_test(&spec, &store, poller.statuses()).unwrap();
    assert_eq!(second.actions.len(), 1);
    assert!(matches!(
        &second.actions[0],
        Action::ServiceCreate { target, .. } if target.service_name == "app"
    ));
    executor.execute(&spec, &second.actions).unwrap();
}

#[test]
fn health_check_failure_marks_service_failed() {
    let spec = parse_compose(HEALTHCHECK_COMPOSE, "hc-fail").unwrap();
    let dir = tempfile::tempdir().unwrap();

    let store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let health = HashMap::new();
    let result = apply_test(&spec, &store, &health).unwrap();

    let runtime = MockRuntime::new(vec!["ctr-db", "ctr-app"]);
    runtime.set_exec_exit_code(1); // Health checks will fail.

    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());
    executor.execute(&spec, &result.actions).unwrap();

    // Poll twice with failures (retries=2).
    let mut poller = HealthPoller::new();
    poller
        .poll_all(executor.runtime(), executor.store(), &spec)
        .unwrap();
    let poll2 = poller
        .poll_all(executor.runtime(), executor.store(), &spec)
        .unwrap();

    // Retries exhausted → reported as newly_failed.
    assert_eq!(poll2.newly_failed, vec!["db".to_string()]);

    // Docker semantics: container stays Running (unhealthy), not killed.
    let observed = executor.store().load_observed_state("hc-fail").unwrap();
    let db = observed
        .iter()
        .find(|o| o.replica.service_name == "db")
        .unwrap();
    assert_eq!(db.phase, ServicePhase::Running);

    // Counter is reset so health checks continue — a subsequent pass
    // can still promote the service to ready.
    assert_eq!(poller.statuses()["db"].consecutive_failures, 0);
}

// ── Restart policy integration ──────────────────────────────────

const RESTART_COMPOSE: &str = r#"
services:
  worker:
    image: worker:latest
    restart: always

  cron:
    image: cron:latest
    restart: "no"
"#;

#[test]
fn restart_policy_generates_actions_for_failed_services() {
    let spec = parse_compose(RESTART_COMPOSE, "restart-test").unwrap();
    let dir = tempfile::tempdir().unwrap();

    let store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let health = HashMap::new();
    let result = apply_test(&spec, &store, &health).unwrap();

    let runtime = MockRuntime::new(vec!["ctr-worker", "ctr-cron"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());
    executor.execute(&spec, &result.actions).unwrap();

    // Simulate both services failing.
    let observed_states = vec![
        ServiceObservedState {
            replica: vz_stack::ServiceReplicaKey::first("worker".to_string()).unwrap(),
            applied_config_digest: None,
            phase: ServicePhase::Failed,
            container_id: None,
            failed_create_ownership: None,
            last_error: Some("crash".to_string()),
            ready: false,
        },
        ServiceObservedState {
            replica: vz_stack::ServiceReplicaKey::first("cron".to_string()).unwrap(),
            applied_config_digest: None,
            phase: ServicePhase::Failed,
            container_id: None,
            failed_create_ownership: None,
            last_error: Some("crash".to_string()),
            ready: false,
        },
    ];

    for obs in &observed_states {
        executor
            .store()
            .save_observed_state("restart-test", obs)
            .unwrap();
    }

    // Compute restarts.
    let tracker = RestartTracker::new();
    let restart_targets = compute_restart_targets(&spec, &observed_states, &tracker);

    // Only worker should restart (policy=always). cron has policy=no.
    assert_eq!(restart_targets.len(), 1);
    assert_eq!(restart_targets[0].service_name, "worker");
}

#[test]
fn restart_with_max_retries_stops_after_limit() {
    let compose = r#"
services:
  worker:
    image: worker:latest
    restart: on-failure:2
"#;

    let spec = parse_compose(compose, "retry-test").unwrap();

    let observed = vec![ServiceObservedState {
        replica: vz_stack::ServiceReplicaKey::first("worker".to_string()).unwrap(),
        applied_config_digest: None,
        phase: ServicePhase::Failed,
        container_id: None,
        failed_create_ownership: None,
        last_error: Some("crash".to_string()),
        ready: false,
    }];

    let mut tracker = RestartTracker::new();

    // First restart: ok.
    let r1 = compute_restart_targets(&spec, &observed, &tracker);
    assert_eq!(r1.len(), 1);
    tracker.record_restart("worker");

    // Second restart: ok.
    let r2 = compute_restart_targets(&spec, &observed, &tracker);
    assert_eq!(r2.len(), 1);
    tracker.record_restart("worker");

    // Third restart: blocked (max_retries=2).
    let r3 = compute_restart_targets(&spec, &observed, &tracker);
    assert!(r3.is_empty());
}

// ── Port allocation integration ─────────────────────────────────

const PORT_COMPOSE: &str = r#"
services:
  web:
    image: nginx:latest
    ports:
      - "8080:80"

  api:
    image: node:20
    ports:
      - "3000:3000"
"#;

#[test]
fn port_allocation_tracked_through_lifecycle() {
    let spec = parse_compose(PORT_COMPOSE, "port-test").unwrap();
    let dir = tempfile::tempdir().unwrap();

    let store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let scope = install_planning_authority(&store, &spec.name);
    let runtime = MockRuntime::new(vec!["ctr-web", "ctr-api"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let stack_data_dir = dir.path().join("runtime").join("stacks").join(&spec.name);
    assert!(!stack_data_dir.exists());
    let executor = StackExecutor::new_scoped(runtime, exec_store, &stack_data_dir, scope).unwrap();
    let mut orchestrator = StackOrchestrator::new(
        executor,
        store,
        OrchestrationConfig {
            poll_interval: Some(0),
            max_rounds: 2,
            ..OrchestrationConfig::default()
        },
    );
    assert!(orchestrator.run(&spec, None).unwrap().converged);

    // Verify ports are tracked.
    let web_ports = orchestrator.executor().ports().ports_for("web").unwrap();
    assert_eq!(web_ports.len(), 1);
    assert_eq!(web_ports[0].host_port, 8080);
    assert_eq!(web_ports[0].container_port, 80);

    let api_ports = orchestrator.executor().ports().ports_for("api").unwrap();
    assert_eq!(api_ports.len(), 1);
    assert_eq!(api_ports[0].host_port, 3000);

    // Down: remove services and verify ports released.
    let empty = StackSpec {
        name: "port-test".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    assert!(orchestrator.run(&empty, None).unwrap().converged);

    assert!(orchestrator.executor().ports().ports_for("web").is_none());
    assert!(orchestrator.executor().ports().ports_for("api").is_none());
    assert!(orchestrator.executor().ports().in_use().is_empty());
}

// ── Volume lifecycle ────────────────────────────────────────────

const VOLUME_COMPOSE: &str = r#"
services:
  db:
    image: postgres:16
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
"#;

#[test]
fn volumes_created_and_used_in_full_pipeline() {
    let spec = parse_compose(VOLUME_COMPOSE, "vol-test").unwrap();
    let dir = tempfile::tempdir().unwrap();

    let store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let health = HashMap::new();
    let result = apply_test(&spec, &store, &health).unwrap();

    let runtime = MockRuntime::new(vec!["ctr-db"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());
    let exec_result = executor.execute(&spec, &result.actions).unwrap();
    assert!(exec_result.all_succeeded());

    // Volume directory should exist.
    assert!(executor.volumes().volumes_dir().join("pgdata").is_dir());

    // VolumeCreated event emitted.
    let events = executor.store().load_events("vol-test").unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::VolumeCreated { .. }))
    );
}

// ── Idempotent re-apply ─────────────────────────────────────────

#[test]
fn re_apply_after_execution_is_idempotent() {
    let spec = parse_compose(SIMPLE_COMPOSE, "idem-test").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(&dir.path().join("state.db")).unwrap();

    // First apply + execute (starts dependency root only).
    let health = HashMap::new();
    let result = apply_test(&spec, &store, &health).unwrap();
    assert_eq!(result.actions.len(), 1);
    assert!(matches!(
        &result.actions[0],
        Action::ServiceCreate { target, .. } if target.service_name == "web"
    ));

    let runtime = MockRuntime::new(vec!["ctr-web", "ctr-api"]);
    let exec_store = StateStore::open(&dir.path().join("state.db")).unwrap();
    let mut executor = StackExecutor::new(runtime, exec_store, dir.path());
    executor.execute(&spec, &result.actions).unwrap();

    // Second apply + execute starts dependent service.
    let result2 = apply_test(&spec, &store, &health).unwrap();
    assert_eq!(result2.actions.len(), 1);
    assert!(matches!(
        &result2.actions[0],
        Action::ServiceCreate { target, .. } if target.service_name == "api"
    ));
    executor.execute(&spec, &result2.actions).unwrap();

    // Third apply: should be idempotent after both services are running.
    let result3 = apply_test(&spec, &store, &health).unwrap();
    assert!(
        result3.actions.is_empty(),
        "third apply should be idempotent after staged execution: {:?}",
        result3.actions
    );
}
