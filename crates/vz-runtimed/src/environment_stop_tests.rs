#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use crate::RuntimedConfig;
use tempfile::TempDir;
use vz_runtime_contract::{
    CapabilitySet, EndpointProtocol, EndpointSpec, EnvironmentSelector, EnvironmentSpec,
    EnvironmentState, MachineProfile, MachineResources, MachineSpec, MachineState, NetworkKind,
    NetworkSpec, ProjectDefinition, ProjectState, RuntimeOperation, RuntimePolicyHook,
    TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};

struct Fixture {
    _root: TempDir,
    daemon: Arc<RuntimeDaemon>,
    project: ProjectState,
}

impl Fixture {
    fn new(stopped: bool, hook: Option<Arc<dyn RuntimePolicyHook>>) -> Self {
        Self::build(stopped, hook, false)
    }

    /// A fixture whose definition declares one network, one endpoint and one
    /// attachment per Machine, so `instantiate_environment` emits the three
    /// fabric ownership kinds Stop must account for.
    fn networked(stopped: bool) -> Self {
        Self::build(stopped, None, true)
    }

    fn build(stopped: bool, hook: Option<Arc<dyn RuntimePolicyHook>>, networked: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let config = RuntimedConfig {
            state_store_path: root.path().join("state.db"),
            runtime_data_dir: root.path().join("runtime"),
            socket_path: root.path().join("daemon.sock"),
        };
        let definition = ProjectDefinition {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            project_id: ProjectId::generate(),
            name: "stop-tests".into(),
            environment: EnvironmentSpec {
                host_exports: Vec::new(),
                host_imports: Vec::new(),
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                default_machine: None,
                machines: ["app", "worker"]
                    .map(|name| MachineSpec {
                        networks: if networked {
                            vec!["private".to_string()]
                        } else {
                            Vec::new()
                        },
                        egress: Default::default(),
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        name: name.into(),
                        profile: MachineProfile::Developer,
                        target: TargetSpec {
                            os: OperatingSystem::Linux,
                            arch: Architecture::Aarch64,
                            image: "linux-appliance".into(),
                            version: None,
                            channel: None,
                            digest: None,
                        },
                        resources: MachineResources::default(),
                        requested_capabilities: CapabilitySet::default(),
                        workspace: None,
                    })
                    .to_vec(),
                networks: if networked {
                    vec![NetworkSpec {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        name: "private".into(),
                        kind: NetworkKind::Private,
                        cidr: None,
                    }]
                } else {
                    vec![]
                },
                endpoints: if networked {
                    vec![EndpointSpec {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        name: "api".into(),
                        machine: "app".into(),
                        network: "private".into(),
                        protocol: EndpointProtocol::Tcp,
                        port: 8080,
                        hostname: None,
                    }]
                } else {
                    vec![]
                },
            },
        };
        let mut environments = ["first", "sibling"]
            .map(|name| definition.instantiate_environment(name, 1).unwrap())
            .to_vec();
        if stopped {
            for environment in &mut environments {
                environment.state = EnvironmentState::Stopped;
                for machine in &mut environment.machines {
                    machine.state = MachineState::Stopped;
                }
            }
        } else {
            for environment in &mut environments {
                environment.state = EnvironmentState::Failed;
                for machine in &mut environment.machines {
                    machine.state = MachineState::Failed;
                }
            }
        }
        let project = ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments,
        };
        vz_stack::StateStore::open(&config.state_store_path)
            .unwrap()
            .save_project_state(&project)
            .unwrap();
        let daemon = Arc::new(match hook {
            Some(hook) => RuntimeDaemon::start_with_policy_hook(config, hook, None).unwrap(),
            None => RuntimeDaemon::start(config).unwrap(),
        });
        let project = daemon
            .with_state_store(|store| {
                store.load_project_state_snapshot(project.definition.project_id.as_str())
            })
            .unwrap()
            .unwrap();
        Self {
            _root: root,
            daemon,
            project,
        }
    }

    fn input(&self) -> StopEnvironmentInput {
        StopEnvironmentInput {
            project_id: self.project.definition.project_id.clone(),
            selection: EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::NameOrId("first".into())),
                ..Default::default()
            },
            metadata: RequestMetadata::new(
                Some("req-stop-test".into()),
                Some("idem-stop-test".into()),
            ),
            machine_timeout: Duration::from_secs(1),
        }
    }

    fn snapshot(&self) -> ProjectState {
        self.daemon
            .with_state_store(|store| {
                store.load_project_state_snapshot(self.project.definition.project_id.as_str())
            })
            .unwrap()
            .unwrap()
    }
}

async fn collect(
    mut receiver: mpsc::Receiver<Result<StopEnvironmentProgress, MachineError>>,
) -> Vec<StopEnvironmentProgress> {
    let mut events = Vec::new();
    while let Some(event) = receiver.recv().await {
        events.push(event.unwrap());
    }
    events
}

#[tokio::test]
async fn stopped_environment_is_real_idempotent_transition_without_touching_sibling() {
    let fixture = Fixture::new(true, None);
    let events = collect(
        fixture
            .daemon
            .stop_environment(fixture.input())
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(events.len(), 1);
    assert!(events[0].terminal && events[0].error.is_none());
    assert_eq!(
        events[0].operation.status,
        EnvironmentLifecycleStatus::Succeeded
    );
    let after = fixture.snapshot();
    let selected = after
        .environments
        .iter()
        .find(|environment| environment.name == "first")
        .unwrap();
    let before = fixture
        .project
        .environments
        .iter()
        .find(|environment| environment.name == "first")
        .unwrap();
    assert_eq!(selected.environment_id, before.environment_id);
    assert_eq!(selected.machines, before.machines);
    assert_eq!(selected.ownership, before.ownership);
    assert_eq!(selected.lifecycle_generation, 1);
    assert_eq!(
        after
            .environments
            .iter()
            .find(|environment| environment.name == "sibling"),
        fixture
            .project
            .environments
            .iter()
            .find(|environment| environment.name == "sibling")
    );
}

#[tokio::test]
async fn unknown_live_sessions_fail_before_durable_begin() {
    let fixture = Fixture::new(false, None);
    let error = fixture
        .daemon
        .stop_environment(fixture.input())
        .await
        .unwrap_err();
    assert_eq!(error.code, MachineErrorCode::StateConflict);
    assert!(error.message.contains("restart recovery is uncertain"));
    assert_eq!(fixture.snapshot(), fixture.project);
    assert!(
        fixture
            .daemon
            .with_state_store(
                |store| store.load_environment_lifecycle_by_idempotency_key("idem-stop-test")
            )
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn concurrent_exact_replay_has_one_generation_and_changed_hash_is_rejected() {
    let fixture = Fixture::new(true, None);
    let (first, second) = tokio::join!(
        fixture.daemon.stop_environment(fixture.input()),
        fixture.daemon.stop_environment(fixture.input())
    );
    let first = collect(first.unwrap()).await;
    let second = collect(second.unwrap()).await;
    assert_eq!(first[0].operation, second[0].operation);
    let before = fixture.snapshot();
    let mut changed = fixture.input();
    changed.machine_timeout += Duration::from_secs(1);
    let error = fixture.daemon.stop_environment(changed).await.unwrap_err();
    assert_eq!(error.code, MachineErrorCode::StateConflict);
    assert_eq!(fixture.snapshot(), before);
}

struct SandboxOnlyPolicy;
impl RuntimePolicyHook for SandboxOnlyPolicy {
    fn evaluate(
        &self,
        _: RuntimeOperation,
        _: &RequestMetadata,
    ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
        Ok(PolicyDecision::Allow)
    }
}

#[tokio::test]
async fn sandbox_authority_does_not_implicitly_authorize_environment_stop() {
    let fixture = Fixture::new(true, Some(Arc::new(SandboxOnlyPolicy)));
    let error = fixture
        .daemon
        .stop_environment(fixture.input())
        .await
        .unwrap_err();
    assert_eq!(error.code, MachineErrorCode::PolicyDenied);
    assert_eq!(error.request_id.as_deref(), Some("req-stop-test"));
    assert_eq!(fixture.snapshot(), fixture.project);
}

#[tokio::test]
async fn stale_explicit_selector_does_not_fall_back_and_disconnect_keeps_receipt() {
    let fixture = Fixture::new(true, None);
    let mut stale = fixture.input();
    stale.selection.explicit = Some(EnvironmentSelector::NameOrId("missing".into()));
    stale.selection.process_environment_id =
        Some(fixture.project.environments[0].environment_id.clone());
    assert!(fixture.daemon.stop_environment(stale).await.is_err());
    assert_eq!(fixture.snapshot(), fixture.project);
    drop(
        fixture
            .daemon
            .stop_environment(fixture.input())
            .await
            .unwrap(),
    );
    let replay = collect(
        fixture
            .daemon
            .stop_environment(fixture.input())
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(replay.len(), 1);
    assert!(replay[0].terminal);
    assert_eq!(replay[0].operation.generation, 1);
}

#[test]
fn unsupported_live_resource_handler_fails_before_any_transition() {
    let fixture = Fixture::new(true, None);
    let mut environment = fixture.project.environments[0].clone();
    environment
        .ownership
        .push(vz_runtime_contract::OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::Fault,
            resource_id: "fault-unhandled".into(),
            environment_id: environment.environment_id.clone(),
            machine_id: None,
        });
    assert_eq!(
        validate_supported_topology(&fixture.input(), &environment)
            .unwrap_err()
            .code,
        MachineErrorCode::UnsupportedOperation
    );
}

/// The three declared-fabric ownership kinds are accounted for rather than
/// refused, and a Stop of an Environment carrying them completes.
#[tokio::test(flavor = "multi_thread")]
async fn declared_fabric_ownership_is_accounted_and_stop_completes() {
    let fixture = Fixture::networked(true);
    let environment = fixture
        .project
        .environments
        .iter()
        .find(|candidate| candidate.name == "first")
        .unwrap();
    let kinds = environment
        .ownership
        .iter()
        .map(|record| record.resource_kind.clone())
        .collect::<Vec<_>>();
    for kind in [
        OwnedResourceKind::Network,
        OwnedResourceKind::Endpoint,
        OwnedResourceKind::NetworkAttachment,
    ] {
        assert!(kinds.contains(&kind), "fixture must emit {kind:?}");
    }
    validate_supported_topology(&fixture.input(), environment).unwrap();

    // A switch this daemon owns for the selected Environment must be joined and
    // released by the Stop, leaving no task or port behind.
    let environment_id = environment.environment_id.clone();
    let owner = vz_runtime_contract::ResourceOwner {
        project_id: fixture.project.definition.project_id.clone(),
        environment_id: environment_id.clone(),
        machine_id: None,
    };
    let lease = fixture
        .daemon
        .acquire_environment_controller(&owner.project_id, &environment_id)
        .await
        .unwrap();
    let members = environment
        .machines
        .iter()
        .enumerate()
        .map(|(index, machine)| {
            (
                crate::environment_switch::PortId(index as u32 + 1),
                crate::environment_switch::MacAddress::derive(
                    environment_id.as_str(),
                    machine.machine_id.as_str(),
                    "private",
                ),
            )
        })
        .collect::<Vec<_>>();
    let (switch, guests) =
        crate::environment_switch::runtime::NetworkSwitch::start(members).unwrap();
    drop(guests);
    fixture
        .daemon
        .environment_switches()
        .install(&lease, &owner, "private", switch)
        .await
        .unwrap();
    drop(lease);
    assert_eq!(
        fixture
            .daemon
            .environment_switches()
            .networks(&environment_id)
            .await,
        vec!["private".to_string()]
    );

    let events = collect(
        fixture
            .daemon
            .stop_environment(fixture.input())
            .await
            .unwrap(),
    )
    .await;
    let last = events.last().unwrap();
    assert!(last.terminal && last.error.is_none());
    assert_eq!(last.operation.status, EnvironmentLifecycleStatus::Succeeded);
    assert!(
        fixture
            .daemon
            .environment_switches()
            .networks(&environment_id)
            .await
            .is_empty(),
        "Stop must reclaim every switch it owned"
    );
    // Stop reclaims the runtime switch, never the declared identity, so the
    // fabric records survive for the next Up to rebind.
    let stopped = fixture
        .snapshot()
        .environments
        .into_iter()
        .find(|candidate| candidate.name == "first")
        .unwrap();
    assert_eq!(stopped.ownership, environment.ownership);
    assert_eq!(stopped.networks, environment.networks);
    assert_eq!(stopped.endpoints, environment.endpoints);
    assert_eq!(stopped.network_attachments, environment.network_attachments);
}

/// Stop's accounting is a per-record kind whitelist, not an expected-set
/// comparison, so it has no length arithmetic a duplicate could unbalance: a
/// repeated record is simply tested twice and an unaccounted one still refused.
#[test]
fn a_duplicated_record_cannot_absorb_an_unaccounted_resource() {
    let fixture = Fixture::networked(true);
    let mut environment = fixture
        .project
        .environments
        .iter()
        .find(|candidate| candidate.name == "first")
        .unwrap()
        .clone();
    let duplicate = environment
        .ownership
        .iter()
        .find(|record| record.resource_kind == OwnedResourceKind::Network)
        .unwrap()
        .clone();
    environment.ownership.push(duplicate);
    validate_supported_topology(&fixture.input(), &environment).unwrap();
    environment
        .ownership
        .push(vz_runtime_contract::OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::Fault,
            resource_id: "unaccounted-behind-a-duplicate".into(),
            environment_id: environment.environment_id.clone(),
            machine_id: None,
        });
    assert_eq!(
        validate_supported_topology(&fixture.input(), &environment)
            .unwrap_err()
            .code,
        MachineErrorCode::UnsupportedOperation
    );
}

/// The accounting is a fixed set, not a widening: a kind outside it is still a
/// hard refusal even though three kinds were added to that set.
#[test]
fn ownership_kinds_outside_the_accounted_set_remain_refused() {
    let fixture = Fixture::networked(true);
    let selected = fixture
        .project
        .environments
        .iter()
        .find(|candidate| candidate.name == "first")
        .unwrap()
        .clone();
    for kind in [
        OwnedResourceKind::HostExport,
        OwnedResourceKind::HostImport,
        OwnedResourceKind::PortRange,
        OwnedResourceKind::Credential,
        OwnedResourceKind::Fault,
        OwnedResourceKind::Socket,
        OwnedResourceKind::LegacySandbox,
        OwnedResourceKind::Other("unimplemented-adapter".into()),
    ] {
        let mut environment = selected.clone();
        environment
            .ownership
            .push(vz_runtime_contract::OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: kind.clone(),
                resource_id: "unaccounted".into(),
                environment_id: environment.environment_id.clone(),
                machine_id: None,
            });
        assert_eq!(
            validate_supported_topology(&fixture.input(), &environment)
                .unwrap_err()
                .code,
            MachineErrorCode::UnsupportedOperation,
            "{kind:?} must remain unsupported by Stop"
        );
    }
}

#[tokio::test]
async fn failed_terminal_receipt_replays_without_waiting_for_retained_effect_fence() {
    let fixture = Fixture::new(false, None);
    let input = fixture.input();
    let environment = fixture.daemon.selected_stop_environment(&input).unwrap();
    let _retained_fence = fixture
        .daemon
        .acquire_environment_controller(&input.project_id, &environment.environment_id)
        .await
        .unwrap();
    let mut operation = fixture
        .daemon
        .with_state_store(|store| {
            store.begin_environment_lifecycle(
                environment.environment_id.as_str(),
                EnvironmentLifecycleKind::Stop,
                input.metadata.request_id.as_deref().unwrap(),
                input.metadata.idempotency_key.as_deref().unwrap(),
                &request_hash(&input, &environment).unwrap(),
                2,
            )
        })
        .unwrap();
    for step in operation.machine_steps.clone() {
        operation = fixture
            .daemon
            .with_state_store(|store| {
                store.acknowledge_environment_machine_step(
                    &MachineLifecycleStepAcknowledgement {
                        operation_id: operation.operation_id.clone(),
                        generation: operation.generation,
                        machine_id: step.machine_id,
                        initial_state: step.initial_state,
                        target_state: step.target_state,
                        expected_incarnation: step.expected_incarnation,
                        resulting_incarnation: None,
                        resulting_activation: None,
                        result: LifecycleStepResult::Failed {
                            reason: "exact physical teardown remains uncertain".into(),
                        },
                    },
                    3,
                )
            })
            .unwrap();
    }
    let operation = fixture
        .daemon
        .with_state_store(|store| {
            store.finish_environment_lifecycle(
                operation.operation_id.as_str(),
                operation.generation,
                4,
            )
        })
        .unwrap();
    let before = fixture.snapshot();
    let receiver = tokio::time::timeout(
        Duration::from_millis(500),
        fixture.daemon.stop_environment(input),
    )
    .await
    .unwrap()
    .unwrap();
    let events = collect(receiver).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].operation, operation);
    assert!(events[0].terminal && events[0].error.is_some());
    assert_eq!(fixture.snapshot(), before);
}

#[tokio::test]
async fn policy_revoked_while_waiting_for_controller_prevents_admission() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct Revocable {
        allow: AtomicBool,
        evaluations: AtomicUsize,
    }
    impl RuntimePolicyHook for Revocable {
        fn evaluate(
            &self,
            _: RuntimeOperation,
            _: &RequestMetadata,
        ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
            Ok(PolicyDecision::Allow)
        }
        fn evaluate_topology(
            &self,
            _: &TopologyAuthorization,
            _: &RequestMetadata,
        ) -> Result<PolicyDecision, Box<dyn std::error::Error + Send + Sync>> {
            self.evaluations.fetch_add(1, Ordering::SeqCst);
            Ok(if self.allow.load(Ordering::SeqCst) {
                PolicyDecision::Allow
            } else {
                PolicyDecision::Deny {
                    reason: "Stop authority revoked".into(),
                }
            })
        }
    }
    let hook = Arc::new(Revocable {
        allow: AtomicBool::new(true),
        evaluations: AtomicUsize::new(0),
    });
    let fixture = Fixture::new(true, Some(hook.clone()));
    let input = fixture.input();
    let environment = fixture.daemon.selected_stop_environment(&input).unwrap();
    let fence = fixture
        .daemon
        .acquire_environment_controller(&input.project_id, &environment.environment_id)
        .await
        .unwrap();
    let daemon = fixture.daemon.clone();
    let request = tokio::spawn(async move { daemon.stop_environment(input).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while hook.evaluations.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    hook.allow.store(false, Ordering::SeqCst);
    drop(fence);
    let error = request.await.unwrap().unwrap_err();
    assert_eq!(error.code, MachineErrorCode::PolicyDenied);
    assert!(hook.evaluations.load(Ordering::SeqCst) >= 2);
    assert_eq!(fixture.snapshot(), fixture.project);
}
