#![allow(clippy::unwrap_used)]

use super::topology::{ClaimV7MigrationFailpoint, TeardownFinalizerV8MigrationFailpoint};
use super::*;
use crate::spec::{NetworkSpec, ServiceKind, ServiceSpec, VolumeSpec};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use vz_runtime_contract::types::{
    Architecture, CapabilitySet, EndpointId, EndpointInstance, EndpointProtocol,
    EndpointSpec as TopologyEndpointSpec, EnvironmentId, EnvironmentInstance,
    EnvironmentLifecycleKind, EnvironmentLifecycleOperation, EnvironmentLifecycleStatus,
    EnvironmentSelectionContext, EnvironmentSelectionSource, EnvironmentSelector, EnvironmentSpec,
    EnvironmentState, LifecycleOperationId, LifecycleStepResult, LifecycleStepStatus,
    MachineActivationEvidence, MachineBackend, MachineCapability, MachineId, MachineIncarnation,
    MachineIncarnationId, MachineInstance, MachineLifecycleStepAcknowledgement, MachineProfile,
    MachineResources, MachineRuntimeIdentity, MachineSpec, MachineState, NetworkId,
    NetworkInstance, NetworkKind, NetworkSpec as TopologyNetworkSpec, OperatingSystem,
    OwnedResourceKind, OwnershipCleanupStepAcknowledgement, OwnershipRecord, ProjectDefinition,
    ProjectId, ProjectState, ResourceOwner, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
    TopologyResolutionError, WorkspaceBinding, WorkspaceBindingId, WorkspaceProjection,
    WorkspaceProjectionMode,
};
use vz_runtime_contract::{ContainerCreateReceipt, MachineErrorCode};

const V0_3_20_FIXTURE: &str = include_str!("../../tests/fixtures/v0.3.20-state.sql");
const V0_3_20_AMBIGUOUS_FIXTURE: &str = include_str!("../../tests/fixtures/v0.3.20-ambiguous.sql");
const V0_3_20_MALFORMED_FIXTURE: &str = include_str!("../../tests/fixtures/v0.3.20-malformed.sql");
const V0_3_20_FIXTURE_SHA256: &str =
    "51b7f3cbe9d7e1ad1219e819d862fdb4c832d6ece32842267d87fefb8b2f5529";
const V0_3_20_AMBIGUOUS_FIXTURE_SHA256: &str =
    "a591d2e0af4578d94d96fe66423c1d59979d33648d10f8f0a69087d1f5ba2ad7";
const V0_3_20_MALFORMED_FIXTURE_SHA256: &str =
    "e99a5c6bd2a82c9ef2389ffe12fc00cd637a4f341def67e37e741e7b0b27db38";

// Unit-test backend receipt, deliberately not release/runtime evidence.
fn test_activation(incarnation: MachineIncarnation) -> MachineActivationEvidence {
    MachineActivationEvidence {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        backend: MachineBackend::MacosVirtualizationLinux,
        negotiated_capabilities: CapabilitySet::new([
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ]),
        runtime_identity: MachineRuntimeIdentity {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            opaque_id: format!("unit-test-runtime:{}", incarnation.incarnation_id),
        },
        incarnation,
    }
}

fn fixture_sha256(fixture: &str) -> String {
    format!("{:x}", Sha256::digest(fixture.as_bytes()))
}

fn seed_v0_3_20_fixture(path: &Path, extension: Option<&str>) {
    let conn = Connection::open(path).expect("open legacy fixture database");
    conn.execute_batch(V0_3_20_FIXTURE)
        .expect("seed exact v0.3.20 state fixture");
    if let Some(extension) = extension {
        conn.execute_batch(extension)
            .expect("apply legacy negative fixture extension");
    }
}

fn create_v2_store(path: &Path) -> StateStore {
    let connection = Connection::open(path).expect("open v2 state database");
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .expect("enable foreign keys for v2 state database");
    let store = StateStore {
        conn: connection,
        event_sender: None,
    };
    store
        .with_immediate_transaction(|store| {
            store.create_legacy_schema()?;
            store.create_topology_schema_v2()?;
            store.validate_v2_schema()?;
            store.set_schema_version(2)
        })
        .expect("create canonical v2 state database");
    store
}

fn create_v3_store(path: &Path) -> StateStore {
    let connection = Connection::open(path).expect("open v3 state database");
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .expect("enable foreign keys for v3 state database");
    let store = StateStore {
        conn: connection,
        event_sender: None,
    };
    store
        .with_immediate_transaction(|store| {
            store.create_legacy_schema()?;
            store.create_topology_schema_v3()?;
            store.validate_v3_schema()?;
            store.set_schema_version(3)
        })
        .expect("create canonical v3 state database");
    store
}

fn create_v4_store(path: &Path) -> StateStore {
    let store = create_v3_store(path);
    store
        .migrate_stack_journal_v3_to_v4()
        .expect("create canonical v4 state database");
    store
}

fn create_v5_store(path: &Path) -> StateStore {
    let store = create_v4_store(path);
    store
        .migrate_replica_v4_to_v5()
        .expect("create canonical v5 state database");
    store
}

fn create_v8_store(path: &Path) -> StateStore {
    let store = create_v5_store(path);
    store
        .migrate_reconcile_v5_to_v6()
        .expect("create canonical v6 state database");
    store
        .migrate_claim_v6_to_v7()
        .expect("create canonical v7 state database");
    store
        .migrate_teardown_finalizer_v7_to_v8()
        .expect("create canonical v8 state database");
    store
}

fn application_schema_snapshot(connection: &Connection) -> Vec<(String, String, String, String)> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, COALESCE(sql, '')
             FROM sqlite_master
             WHERE name NOT LIKE 'sqlite_%'
             ORDER BY type, name",
        )
        .unwrap();
    statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn ownership_snapshot(
    connection: &Connection,
) -> Vec<(String, String, String, Option<String>, i64, String)> {
    let mut statement = connection
        .prepare(
            "SELECT resource_kind, resource_id, environment_id, machine_id,
                    schema_version, record_json
             FROM topology_ownership
             ORDER BY resource_kind, resource_id",
        )
        .unwrap();
    statement
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn legacy_non_developer_rows(
    path: &Path,
) -> Vec<(String, String, String, String, String, String, i64, i64)> {
    let conn = Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT sandbox_id, stack_name, state, backend, spec_json, labels_json,
                    created_at, updated_at
             FROM sandbox_state
             WHERE sandbox_id != 'vz-run-shop-a1b2c3d4e5f6'
             ORDER BY sandbox_id",
        )
        .unwrap();
    stmt.query_map([], |row| {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
            row.get(7)?,
        ))
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

fn topology_project_state(
    project_id: &str,
    environment_names: &[&str],
    path_hint: &str,
) -> ProjectState {
    let project_id = ProjectId::new(project_id).unwrap();
    let target = TargetSpec {
        os: OperatingSystem::Linux,
        arch: Architecture::Aarch64,
        image: "ubuntu:24.04".to_string(),
        version: Some("24.04".to_string()),
        channel: Some("stable".to_string()),
        digest: Some("sha256:pinned-linux".to_string()),
    };
    let linux_capabilities = CapabilitySet::new([
        MachineCapability::DockerEngine,
        MachineCapability::Compose,
        MachineCapability::Buildx,
    ]);
    let machine_spec = MachineSpec {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        name: "linux".to_string(),
        profile: MachineProfile::Developer,
        target: target.clone(),
        resources: MachineResources::default(),
        requested_capabilities: linux_capabilities.clone(),
        workspace: Some(WorkspaceProjection {
            binding: "workspace".to_string(),
            target_path: "/workspace".to_string(),
            mode: WorkspaceProjectionMode::ReadWrite,
        }),
    };
    let definition = ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: project_id.clone(),
        name: "shop".to_string(),
        environment: EnvironmentSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            machines: vec![machine_spec],
            networks: vec![TopologyNetworkSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                name: "private".to_string(),
                kind: NetworkKind::Private,
                cidr: Some("10.20.0.0/24".to_string()),
            }],
            endpoints: vec![TopologyEndpointSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                name: "web".to_string(),
                machine: "linux".to_string(),
                network: "private".to_string(),
                protocol: EndpointProtocol::Https,
                port: 443,
                hostname: Some("web.shop.test".to_string()),
            }],
        },
    };
    let definition_digest = definition.digest().unwrap();
    let environments = environment_names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let environment_id = EnvironmentId::new(format!("env_{name}")).unwrap();
            let machine_id = MachineId::new(format!("mac_{name}")).unwrap();
            let incarnation_id = MachineIncarnationId::new(format!("inc_{name}")).unwrap();
            let network_id = NetworkId::new(format!("net_{name}")).unwrap();
            let endpoint_id = EndpointId::new(format!("end_{name}")).unwrap();
            EnvironmentInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                environment_id: environment_id.clone(),
                project_id: project_id.clone(),
                name: (*name).to_string(),
                definition_digest: definition_digest.clone(),
                state: EnvironmentState::Ready,
                lifecycle_generation: 0,
                active_operation_id: None,
                bindings: vec![WorkspaceBinding {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    binding_id: WorkspaceBindingId::new(format!("wsp_{name}")).unwrap(),
                    project_id: project_id.clone(),
                    environment_id: environment_id.clone(),
                    name: "workspace".to_string(),
                    workspace_key: "same-worktree-key".to_string(),
                    path_hint: Some(path_hint.to_string()),
                }],
                machines: vec![MachineInstance {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    machine_id: machine_id.clone(),
                    environment_id: environment_id.clone(),
                    name: "linux".to_string(),
                    profile: MachineProfile::Developer,
                    target: target.clone(),
                    resources: MachineResources::default(),
                    requested_capabilities: linux_capabilities.clone(),
                    negotiated_capabilities: linux_capabilities.clone(),
                    backend: None,
                    incarnation: Some(MachineIncarnation {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        incarnation_id: incarnation_id.clone(),
                        machine_id: machine_id.clone(),
                        generation: 1,
                        created_at: 50,
                    }),
                    state: MachineState::Ready,
                    runtime_identity: None,
                    legacy_sandbox_id: None,
                }],
                networks: vec![NetworkInstance {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    network_id: network_id.clone(),
                    environment_id: environment_id.clone(),
                    name: "private".to_string(),
                }],
                endpoints: vec![EndpointInstance {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    endpoint_id: endpoint_id.clone(),
                    environment_id: environment_id.clone(),
                    machine_id: machine_id.clone(),
                    network_id: network_id.clone(),
                    name: "web".to_string(),
                }],
                ownership: vec![
                    OwnershipRecord {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        resource_kind: OwnedResourceKind::Endpoint,
                        resource_id: endpoint_id.to_string(),
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
                    OwnershipRecord {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        resource_kind: OwnedResourceKind::Machine,
                        resource_id: machine_id.to_string(),
                        environment_id: environment_id.clone(),
                        machine_id: Some(machine_id.clone()),
                    },
                    OwnershipRecord {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        resource_kind: OwnedResourceKind::Network,
                        resource_id: network_id.to_string(),
                        environment_id,
                        machine_id: None,
                    },
                ],
                legacy_migration: None,
                created_at: 100 + index as u64,
                updated_at: 200 + index as u64,
            }
        })
        .collect();
    ProjectState {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        definition,
        environments,
    }
}

fn hardened_topology_project_state(project_id: &str, environment_name: &str) -> ProjectState {
    let mut state = topology_project_state(project_id, &[environment_name], "/checkout");
    let machine_spec = &mut state.definition.environment.machines[0];
    machine_spec.profile = MachineProfile::Hardened;
    for capability in [
        MachineCapability::DockerEngine,
        MachineCapability::Compose,
        MachineCapability::Buildx,
    ] {
        machine_spec
            .requested_capabilities
            .capabilities
            .remove(&capability);
    }
    let definition_digest = state.definition.digest().unwrap();
    let machine = &mut state.environments[0].machines[0];
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
    state.environments[0].definition_digest = definition_digest;
    state
}

fn sample_spec() -> StackSpec {
    StackSpec {
        name: "myapp".to_string(),
        services: vec![
            ServiceSpec {
                name: "web".to_string(),
                kind: ServiceKind::Service,
                image: "nginx:latest".to_string(),
                command: None,
                entrypoint: None,
                environment: HashMap::from([("PORT".to_string(), "80".to_string())]),
                working_dir: None,
                user: None,
                mounts: vec![],
                ports: vec![],
                depends_on: vec![],
                healthcheck: None,
                restart_policy: None,
                resources: Default::default(),
                extra_hosts: vec![],
                secrets: vec![],
                networks: vec![],
                cap_add: vec![],
                cap_drop: vec![],
                privileged: false,
                read_only: false,
                sysctls: HashMap::new(),
                ulimits: vec![],
                container_name: None,
                hostname: None,
                domainname: None,
                labels: HashMap::new(),
                stop_signal: None,
                stop_grace_period_secs: None,
                expose: vec![],
                stdin_open: false,
                tty: false,
                logging: None,
            },
            ServiceSpec {
                name: "db".to_string(),
                kind: ServiceKind::Service,
                image: "postgres:16".to_string(),
                command: None,
                entrypoint: None,
                environment: HashMap::from([(
                    "POSTGRES_PASSWORD".to_string(),
                    "secret".to_string(),
                )]),
                working_dir: None,
                user: None,
                mounts: vec![],
                ports: vec![],
                depends_on: vec![],
                healthcheck: None,
                restart_policy: None,
                resources: Default::default(),
                extra_hosts: vec![],
                secrets: vec![],
                networks: vec![],
                cap_add: vec![],
                cap_drop: vec![],
                privileged: false,
                read_only: false,
                sysctls: HashMap::new(),
                ulimits: vec![],
                container_name: None,
                hostname: None,
                domainname: None,
                labels: HashMap::new(),
                stop_signal: None,
                stop_grace_period_secs: None,
                expose: vec![],
                stdin_open: false,
                tty: false,
                logging: None,
            },
        ],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    }
}

#[test]
fn desired_state_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let spec = sample_spec();

    store.save_desired_state("myapp", &spec).unwrap();
    let loaded = store.load_desired_state("myapp").unwrap();
    assert_eq!(loaded, Some(spec));
}

#[test]
fn desired_state_identical_save_is_a_zero_write_replay() {
    let store = StateStore::in_memory().unwrap();
    let spec = sample_spec();

    store.save_desired_state("myapp", &spec).unwrap();
    let changes_before_replay = store.conn.total_changes();
    store.save_desired_state("myapp", &spec).unwrap();

    assert_eq!(store.conn.total_changes(), changes_before_replay);
    assert_eq!(store.load_desired_state("myapp").unwrap(), Some(spec));
}

#[test]
fn desired_state_missing_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.load_desired_state("nonexistent").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn desired_state_upsert_replaces() {
    let store = StateStore::in_memory().unwrap();
    let spec1 = sample_spec();

    store.save_desired_state("myapp", &spec1).unwrap();

    let spec2 = StackSpec {
        name: "myapp".to_string(),
        services: vec![],
        networks: vec![NetworkSpec {
            name: "net1".to_string(),
            driver: "bridge".to_string(),
            subnet: None,
        }],
        volumes: vec![VolumeSpec {
            name: "vol1".to_string(),
            driver: "local".to_string(),
            driver_opts: None,
        }],
        secrets: vec![],
        disk_size_mb: None,
    };

    store.save_desired_state("myapp", &spec2).unwrap();
    let loaded = store.load_desired_state("myapp").unwrap().unwrap();
    assert_eq!(loaded, spec2);
    assert!(loaded.services.is_empty());
}

#[test]
fn service_mount_digest_round_trip_and_delete() {
    let store = StateStore::in_memory().unwrap();

    store
        .save_service_mount_digest("myapp", "web", "digest-web-v1")
        .unwrap();
    store
        .save_service_mount_digest("myapp", "db", "digest-db-v1")
        .unwrap();

    let digests = store.load_service_mount_digests("myapp").unwrap();
    assert_eq!(digests.len(), 2);
    assert_eq!(digests.get("web"), Some(&"digest-web-v1".to_string()));
    assert_eq!(digests.get("db"), Some(&"digest-db-v1".to_string()));

    store
        .save_service_mount_digest("myapp", "web", "digest-web-v2")
        .unwrap();
    let digests = store.load_service_mount_digests("myapp").unwrap();
    assert_eq!(digests.get("web"), Some(&"digest-web-v2".to_string()));

    store.delete_service_mount_digest("myapp", "db").unwrap();
    let digests = store.load_service_mount_digests("myapp").unwrap();
    assert_eq!(digests.len(), 1);
    assert!(digests.get("db").is_none());
}

#[test]
fn reconcile_progress_round_trip_and_clear() {
    let store = StateStore::in_memory().unwrap();
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("api".to_string()).unwrap(),
        },
    ];

    store
        .save_reconcile_progress("myapp", "op-1", &actions, 0)
        .unwrap();

    let progress = store.load_reconcile_progress("myapp").unwrap().unwrap();
    assert_eq!(progress.operation_id, "op-1");
    assert_eq!(progress.next_action_index, 0);
    assert_eq!(progress.actions, actions);

    store
        .save_reconcile_progress("myapp", "op-1", &progress.actions, 1)
        .unwrap();
    let updated = store.load_reconcile_progress("myapp").unwrap().unwrap();
    assert_eq!(updated.next_action_index, 1);
    assert_eq!(updated.actions.len(), 2);

    store.clear_reconcile_progress("myapp").unwrap();
    assert!(store.load_reconcile_progress("myapp").unwrap().is_none());
}

#[test]
fn observed_state_round_trip() {
    let store = StateStore::in_memory().unwrap();

    let ownership = ContainerGenerationOwnership {
        container_id: "ctr-abc".to_string(),
        generation: 17,
        stack_id: "myapp".to_string(),
        scope: Some(Box::new(
            vz_runtime_contract::ContainerGenerationScope::synthetic_legacy_stack("myapp").unwrap(),
        )),
    };

    let state1 = ServiceObservedState {
        replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        applied_config_digest: None,
        phase: ServicePhase::Failed,
        container_id: Some("ctr-abc".to_string()),
        failed_create_ownership: Some(ownership.clone()),
        last_error: Some("create failed after admission".to_string()),
        ready: false,
    };

    let state2 = ServiceObservedState {
        replica: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        applied_config_digest: None,
        phase: ServicePhase::Pending,
        container_id: None,
        failed_create_ownership: None,
        last_error: None,
        ready: false,
    };

    store.save_observed_state("myapp", &state1).unwrap();
    store.save_observed_state("myapp", &state2).unwrap();

    let states = store.load_observed_state("myapp").unwrap();
    assert_eq!(states.len(), 2);
    let web = states
        .iter()
        .find(|s| s.replica.service_name == "web")
        .unwrap();
    assert_eq!(web.failed_create_ownership, Some(ownership));
    assert!(states.iter().any(|s| s.replica.service_name == "db"));
}

#[test]
fn running_container_ownership_survives_file_backed_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("state.db");
    let ownership = ContainerGenerationOwnership {
        container_id: "ctr-running".to_string(),
        generation: 23,
        stack_id: "myapp".to_string(),
        scope: Some(Box::new(
            vz_runtime_contract::ContainerGenerationScope::synthetic_legacy_stack("myapp").unwrap(),
        )),
    };
    {
        let store = StateStore::open(&path).unwrap();
        store
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    replica: crate::state_store::ServiceReplicaKey::first("web".to_string())
                        .unwrap(),
                    applied_config_digest: None,
                    phase: ServicePhase::Running,
                    container_id: Some(ownership.container_id.clone()),
                    failed_create_ownership: Some(ownership.clone()),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();
    }

    let reopened = StateStore::open(&path).unwrap();
    let states = reopened.load_observed_state("myapp").unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].phase, ServicePhase::Running);
    assert_eq!(states[0].failed_create_ownership, Some(ownership));
}

#[test]
fn observed_state_rejects_legacy_replica_unqualified_json() {
    let legacy = r#"{
        "service_name":"web",
        "phase":"Failed",
        "container_id":"ctr-abc",
        "last_error":"legacy create failure",
        "ready":false
    }"#;

    let error = serde_json::from_str::<ServiceObservedState>(legacy).unwrap_err();
    assert!(error.to_string().contains("replica"));
}

#[test]
fn observed_state_rejects_legacy_unscoped_ownership_json_without_replica() {
    let legacy = r#"{
        "service_name":"web",
        "phase":"Running",
        "container_id":"ctr-abc",
        "failed_create_ownership":{
            "container_id":"ctr-abc",
            "generation":3,
            "stack_id":"myapp"
        },
        "ready":true
    }"#;

    let error = serde_json::from_str::<ServiceObservedState>(legacy).unwrap_err();
    assert!(error.to_string().contains("replica"));
}

#[test]
fn resolve_service_tty_for_container_returns_desired_service_tty() {
    let store = StateStore::in_memory().unwrap();

    let mut spec = sample_spec();
    spec.services[0].tty = true;
    store.save_desired_state("myapp", &spec).unwrap();
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-web-1".to_string()),
                failed_create_ownership: None,
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let resolved = store
        .resolve_service_tty_for_container("ctr-web-1")
        .unwrap();
    assert_eq!(resolved, Some(true));
}

#[test]
fn resolve_service_tty_for_container_returns_none_when_unmapped() {
    let store = StateStore::in_memory().unwrap();
    store.save_desired_state("myapp", &sample_spec()).unwrap();
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-web-1".to_string()),
                failed_create_ownership: None,
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let resolved_missing = store
        .resolve_service_tty_for_container("ctr-missing")
        .unwrap();
    assert!(resolved_missing.is_none());
}

#[test]
fn resolve_service_exec_pty_default_for_container_uses_stdin_open_or_tty() {
    let store = StateStore::in_memory().unwrap();

    let mut spec = sample_spec();
    spec.services[0].tty = false;
    spec.services[0].stdin_open = true;
    store.save_desired_state("myapp", &spec).unwrap();
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-web-stdin".to_string()),
                failed_create_ownership: None,
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let resolved = store
        .resolve_service_exec_pty_default_for_container("ctr-web-stdin")
        .unwrap();
    assert_eq!(resolved, Some(true));
}

#[test]
fn resolve_service_exec_pty_default_for_container_returns_none_when_unmapped() {
    let store = StateStore::in_memory().unwrap();
    store.save_desired_state("myapp", &sample_spec()).unwrap();
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-web-1".to_string()),
                failed_create_ownership: None,
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let resolved_missing = store
        .resolve_service_exec_pty_default_for_container("ctr-missing")
        .unwrap();
    assert!(resolved_missing.is_none());
}

#[test]
fn observed_state_upsert_updates_service() {
    let store = StateStore::in_memory().unwrap();

    let initial = ServiceObservedState {
        replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        applied_config_digest: None,
        phase: ServicePhase::Creating,
        container_id: None,
        failed_create_ownership: None,
        last_error: None,
        ready: false,
    };

    store.save_observed_state("myapp", &initial).unwrap();

    let updated = ServiceObservedState {
        replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        applied_config_digest: None,
        phase: ServicePhase::Running,
        container_id: Some("ctr-xyz".to_string()),
        failed_create_ownership: None,
        last_error: None,
        ready: true,
    };

    store.save_observed_state("myapp", &updated).unwrap();

    let states = store.load_observed_state("myapp").unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].phase, ServicePhase::Running);
    assert_eq!(states[0].container_id, Some("ctr-xyz".to_string()));
}

#[test]
fn observed_state_empty_returns_empty_vec() {
    let store = StateStore::in_memory().unwrap();
    let states = store.load_observed_state("empty").unwrap();
    assert!(states.is_empty());
}

#[test]
fn health_poller_state_round_trip_and_clear() {
    let store = StateStore::in_memory().unwrap();
    let mut state = HashMap::new();
    state.insert(
        "web".to_string(),
        HealthPollState {
            service_name: "web".to_string(),
            consecutive_passes: 2,
            consecutive_failures: 1,
            last_check_millis: Some(1_700_000_000_000),
            start_time_millis: Some(1_700_000_000_123),
        },
    );

    store.save_health_poller_state("myapp", &state).unwrap();
    let loaded = store.load_health_poller_state("myapp").unwrap();
    assert_eq!(loaded.get("web").unwrap(), state.get("web").unwrap());

    store.clear_health_poller_state("myapp").unwrap();
    let cleared = store.load_health_poller_state("myapp").unwrap();
    assert!(cleared.is_empty());
}

#[test]
fn events_emit_and_load() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 2,
            },
        )
        .unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyCompleted {
                stack_name: "myapp".to_string(),
                succeeded: 2,
                failed: 0,
            },
        )
        .unwrap();

    let events = store.load_events("myapp").unwrap();
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0], StackEvent::StackApplyStarted { .. }));
    assert!(matches!(events[1], StackEvent::StackApplyCompleted { .. }));
}

#[test]
fn events_empty_returns_empty_vec() {
    let store = StateStore::in_memory().unwrap();
    let events = store.load_events("empty").unwrap();
    assert!(events.is_empty());
}

#[test]
fn events_scoped_by_stack_name() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "app1",
            &StackEvent::StackApplyStarted {
                stack_name: "app1".to_string(),
                services_count: 1,
            },
        )
        .unwrap();

    store
        .emit_event(
            "app2",
            &StackEvent::StackApplyStarted {
                stack_name: "app2".to_string(),
                services_count: 5,
            },
        )
        .unwrap();

    let app1_events = store.load_events("app1").unwrap();
    assert_eq!(app1_events.len(), 1);
    let app2_events = store.load_events("app2").unwrap();
    assert_eq!(app2_events.len(), 1);
}

#[test]
fn multiple_stacks_isolated() {
    let store = StateStore::in_memory().unwrap();

    let spec1 = StackSpec {
        name: "app1".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let spec2 = StackSpec {
        name: "app2".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };

    store.save_desired_state("app1", &spec1).unwrap();
    store.save_desired_state("app2", &spec2).unwrap();

    let loaded1 = store.load_desired_state("app1").unwrap().unwrap();
    let loaded2 = store.load_desired_state("app2").unwrap().unwrap();

    assert_eq!(loaded1.name, "app1");
    assert_eq!(loaded2.name, "app2");
}

// ── B17: Event pipeline tests ──

#[test]
fn event_records_include_id_and_timestamp() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::ServiceCreating {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
        )
        .unwrap();

    let records = store.load_event_records("myapp").unwrap();
    assert_eq!(records.len(), 1);
    assert!(records[0].id > 0);
    assert!(!records[0].created_at.is_empty());
    assert_eq!(records[0].stack_name, "myapp");
    assert!(matches!(
        records[0].event,
        StackEvent::ServiceCreating { .. }
    ));
}

#[test]
fn load_events_since_returns_only_newer_events() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 1,
            },
        )
        .unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::ServiceCreating {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
        )
        .unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::ServiceReady {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
                runtime_id: "ctr-1".to_string(),
            },
        )
        .unwrap();

    let all = store.load_event_records("myapp").unwrap();
    assert_eq!(all.len(), 3);

    // Stream from after the first event.
    let cursor = all[0].id;
    let newer = store.load_events_since("myapp", cursor).unwrap();
    assert_eq!(newer.len(), 2);
    assert!(matches!(newer[0].event, StackEvent::ServiceCreating { .. }));
    assert!(matches!(newer[1].event, StackEvent::ServiceReady { .. }));

    // Stream from after the second event.
    let cursor2 = newer[0].id;
    let newest = store.load_events_since("myapp", cursor2).unwrap();
    assert_eq!(newest.len(), 1);
    assert!(matches!(newest[0].event, StackEvent::ServiceReady { .. }));
}

#[test]
fn load_events_since_with_zero_returns_all() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 1,
            },
        )
        .unwrap();

    let all = store.load_events_since("myapp", 0).unwrap();
    assert_eq!(all.len(), 1);
}

#[test]
fn load_events_since_with_future_cursor_returns_empty() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 1,
            },
        )
        .unwrap();

    let empty = store.load_events_since("myapp", 999_999).unwrap();
    assert!(empty.is_empty());
}

#[test]
fn load_events_since_limited_applies_limit_and_order() {
    let store = StateStore::in_memory().unwrap();
    for index in 0..3 {
        store
            .emit_event(
                "myapp",
                &StackEvent::ServiceCreating {
                    stack_name: "myapp".to_string(),
                    service_name: format!("svc-{index}"),
                },
            )
            .unwrap();
    }

    let first_page = store.load_events_since_limited("myapp", 0, 2).unwrap();
    assert_eq!(first_page.len(), 2);
    assert!(first_page[0].id < first_page[1].id);

    let second_page = store
        .load_events_since_limited("myapp", first_page[1].id, 2)
        .unwrap();
    assert_eq!(second_page.len(), 1);
    assert!(second_page[0].id > first_page[1].id);
}

#[test]
fn event_count_returns_correct_total() {
    let store = StateStore::in_memory().unwrap();

    assert_eq!(store.event_count("myapp").unwrap(), 0);

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 1,
            },
        )
        .unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyCompleted {
                stack_name: "myapp".to_string(),
                succeeded: 1,
                failed: 0,
            },
        )
        .unwrap();

    assert_eq!(store.event_count("myapp").unwrap(), 2);
    assert_eq!(store.event_count("other").unwrap(), 0);
}

#[test]
fn event_records_ids_are_monotonically_increasing() {
    let store = StateStore::in_memory().unwrap();

    for i in 0..5 {
        store
            .emit_event(
                "myapp",
                &StackEvent::ServiceCreating {
                    stack_name: "myapp".to_string(),
                    service_name: format!("svc-{i}"),
                },
            )
            .unwrap();
    }

    let records = store.load_event_records("myapp").unwrap();
    assert_eq!(records.len(), 5);
    for window in records.windows(2) {
        assert!(window[1].id > window[0].id);
    }
}

#[test]
fn new_event_variants_persist_and_load() {
    let store = StateStore::in_memory().unwrap();

    let events = vec![
        StackEvent::ServiceStopping {
            stack_name: "myapp".to_string(),
            service_name: "web".to_string(),
        },
        StackEvent::ServiceStopped {
            stack_name: "myapp".to_string(),
            service_name: "web".to_string(),
            exit_code: 137,
        },
        StackEvent::PortConflict {
            stack_name: "myapp".to_string(),
            service_name: "web".to_string(),
            port: 8080,
        },
        StackEvent::VolumeCreated {
            stack_name: "myapp".to_string(),
            volume_name: "dbdata".to_string(),
        },
        StackEvent::StackDestroyed {
            stack_name: "myapp".to_string(),
        },
    ];

    for event in &events {
        store.emit_event("myapp", event).unwrap();
    }

    let loaded = store.load_events("myapp").unwrap();
    assert_eq!(loaded, events);
}

// ── Real-time event streaming tests ──

#[test]
fn emit_event_sends_to_channel() {
    use std::sync::mpsc;

    let mut store = StateStore::in_memory().unwrap();
    let (tx, rx) = mpsc::channel();
    store.set_event_sender(tx);

    store
        .emit_event(
            "test",
            &StackEvent::StackDestroyed {
                stack_name: "test".to_string(),
            },
        )
        .unwrap();

    let received = rx.try_recv().unwrap();
    assert!(matches!(received, StackEvent::StackDestroyed { .. }));
}

#[test]
fn emit_event_without_sender_works() {
    let store = StateStore::in_memory().unwrap();
    // No sender set — should not error.
    store
        .emit_event(
            "test",
            &StackEvent::StackDestroyed {
                stack_name: "test".to_string(),
            },
        )
        .unwrap();
}

#[test]
fn emit_event_ignores_dropped_receiver() {
    use std::sync::mpsc;

    let mut store = StateStore::in_memory().unwrap();
    let (tx, rx) = mpsc::channel();
    store.set_event_sender(tx);

    // Drop the receiver so sends fail.
    drop(rx);

    // Should not error even though receiver is gone.
    store
        .emit_event(
            "test",
            &StackEvent::StackDestroyed {
                stack_name: "test".to_string(),
            },
        )
        .unwrap();

    // Event should still be persisted to SQLite.
    let events = store.load_events("test").unwrap();
    assert_eq!(events.len(), 1);
}

// ── Event compaction tests ──

fn emit_n_events(store: &StateStore, stack_name: &str, n: usize) {
    for i in 0..n {
        store
            .emit_event(
                stack_name,
                &StackEvent::ServiceCreating {
                    stack_name: stack_name.to_string(),
                    service_name: format!("svc-{i}"),
                },
            )
            .unwrap();
    }
}

#[test]
fn compact_events_by_count_keeps_recent() {
    let store = StateStore::in_memory().unwrap();
    emit_n_events(&store, "myapp", 20);

    assert_eq!(store.event_count("myapp").unwrap(), 20);

    let deleted = store.compact_events_by_count("myapp", 10).unwrap();
    assert_eq!(deleted, 10);
    assert_eq!(store.event_count("myapp").unwrap(), 10);

    // The kept events should be the most recent 10 (IDs 11..=20).
    let records = store.load_event_records("myapp").unwrap();
    assert_eq!(records.len(), 10);
    // Verify ordering is ascending by id and that the oldest kept is > 10.
    assert!(records[0].id > 10);
}

#[test]
fn compact_events_by_count_noop_when_under_limit() {
    let store = StateStore::in_memory().unwrap();
    emit_n_events(&store, "myapp", 5);

    let deleted = store.compact_events_by_count("myapp", 10).unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(store.event_count("myapp").unwrap(), 5);
}

#[test]
fn compact_events_by_count_scoped_to_stack() {
    let store = StateStore::in_memory().unwrap();
    emit_n_events(&store, "app-a", 15);
    emit_n_events(&store, "app-b", 5);

    let deleted = store.compact_events_by_count("app-a", 10).unwrap();
    assert_eq!(deleted, 5);
    assert_eq!(store.event_count("app-a").unwrap(), 10);
    // app-b is untouched.
    assert_eq!(store.event_count("app-b").unwrap(), 5);
}

#[test]
fn compact_events_by_age_deletes_old() {
    let store = StateStore::in_memory().unwrap();
    emit_n_events(&store, "myapp", 5);

    // Back-date all events to 2 hours ago so they are clearly old.
    store
        .conn
        .execute(
            "UPDATE events SET created_at = datetime('now', '-7200 seconds') WHERE stack_name = 'myapp'",
            [],
        )
        .unwrap();

    // Delete events older than 1 hour (3600 seconds). All 5 should be removed.
    let deleted = store.compact_events("myapp", 3600).unwrap();
    assert_eq!(deleted, 5);
    assert_eq!(store.event_count("myapp").unwrap(), 0);
}

#[test]
fn compact_events_by_age_keeps_recent() {
    let store = StateStore::in_memory().unwrap();
    emit_n_events(&store, "myapp", 5);

    // With a generous window (1 hour), nothing should be deleted
    // because the events were just created.
    let deleted = store.compact_events("myapp", 3600).unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(store.event_count("myapp").unwrap(), 5);
}

#[test]
fn compact_events_by_age_partial_delete() {
    let store = StateStore::in_memory().unwrap();
    emit_n_events(&store, "myapp", 5);

    // Back-date 3 events to 2 hours ago, leave 2 at current time.
    store
        .conn
        .execute(
            "UPDATE events SET created_at = datetime('now', '-7200 seconds')
             WHERE stack_name = 'myapp' AND id IN (
                 SELECT id FROM events WHERE stack_name = 'myapp' ORDER BY id ASC LIMIT 3
             )",
            [],
        )
        .unwrap();

    let deleted = store.compact_events("myapp", 3600).unwrap();
    assert_eq!(deleted, 3);
    assert_eq!(store.event_count("myapp").unwrap(), 2);
}

#[test]
fn compact_events_default_applies_both_policies() {
    let store = StateStore::in_memory().unwrap();
    // Emit more than the default max (10,000).
    emit_n_events(&store, "myapp", 10_050);
    assert_eq!(store.event_count("myapp").unwrap(), 10_050);

    let deleted = store.compact_events_default("myapp").unwrap();
    // Age-based deletes 0 (all recent), count-based deletes 50.
    assert_eq!(deleted, 50);
    assert_eq!(store.event_count("myapp").unwrap(), 10_000);
}

#[test]
fn event_count_empty_stack() {
    let store = StateStore::in_memory().unwrap();
    assert_eq!(store.event_count("nonexistent").unwrap(), 0);
}

#[test]
fn compact_events_empty_stack() {
    let store = StateStore::in_memory().unwrap();
    let deleted = store.compact_events("nonexistent", 0).unwrap();
    assert_eq!(deleted, 0);
    let deleted = store.compact_events_by_count("nonexistent", 10).unwrap();
    assert_eq!(deleted, 0);
}

// ── Sandbox persistence tests ──

fn sample_sandbox(id: &str, stack_name: &str) -> Sandbox {
    use std::collections::BTreeMap;
    let mut labels = BTreeMap::new();
    labels.insert("stack_name".to_string(), stack_name.to_string());
    Sandbox {
        sandbox_id: id.to_string(),
        backend: SandboxBackend::MacosVz,
        spec: SandboxSpec::default(),
        state: SandboxState::Creating,
        created_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        labels,
    }
}

#[test]
fn sandbox_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let sandbox = sample_sandbox("sb-1", "myapp");

    store.save_sandbox(&sandbox).unwrap();
    let loaded = store.load_sandbox("sb-1").unwrap().unwrap();
    assert_eq!(loaded, sandbox);
}

#[test]
fn sandbox_for_stack_lookup() {
    let store = StateStore::in_memory().unwrap();
    let sandbox = sample_sandbox("sb-2", "myapp");

    store.save_sandbox(&sandbox).unwrap();
    let loaded = store.load_sandbox_for_stack("myapp").unwrap().unwrap();
    assert_eq!(loaded.sandbox_id, "sb-2");
}

#[test]
fn sandbox_list_returns_all() {
    let store = StateStore::in_memory().unwrap();
    let sb1 = sample_sandbox("sb-a", "app1");
    let mut sb2 = sample_sandbox("sb-b", "app2");
    sb2.created_at = 1_700_000_001;

    store.save_sandbox(&sb1).unwrap();
    store.save_sandbox(&sb2).unwrap();

    let all = store.list_sandboxes().unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn sandbox_delete_removes() {
    let store = StateStore::in_memory().unwrap();
    let sandbox = sample_sandbox("sb-del", "myapp");

    store.save_sandbox(&sandbox).unwrap();
    store.delete_sandbox("sb-del").unwrap();
    let loaded = store.load_sandbox("sb-del").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn sandbox_upsert_updates_state() {
    let store = StateStore::in_memory().unwrap();
    let mut sandbox = sample_sandbox("sb-up", "myapp");

    store.save_sandbox(&sandbox).unwrap();

    sandbox.state = SandboxState::Ready;
    sandbox.updated_at = 1_700_000_100;
    store.save_sandbox(&sandbox).unwrap();

    let loaded = store.load_sandbox("sb-up").unwrap().unwrap();
    assert_eq!(loaded.state, SandboxState::Ready);
    assert_eq!(loaded.updated_at, 1_700_000_100);
}

#[test]
fn allocator_state_round_trip() {
    let store = StateStore::in_memory().unwrap();

    let snapshot = AllocatorSnapshot {
        schema_version: 2,
        ports: vec![AllocatorPortLease {
            target: ServiceReplicaKey::first("web").unwrap(),
            ports: vec![PublishedPort {
                protocol: "tcp".to_string(),
                container_port: 80,
                host_port: 8080,
            }],
        }],
        service_ips: vec![AllocatorIpLease {
            target: ServiceReplicaKey::first("web").unwrap(),
            ip: "10.0.0.2".to_string(),
        }],
        service_network_ips: vec![],
        mount_tag_offsets: HashMap::from([("web".to_string(), 3)]),
    };

    store.save_allocator_state("myapp", &snapshot).unwrap();
    let loaded = store.load_allocator_state("myapp").unwrap().unwrap();
    assert_eq!(loaded, snapshot);
}

#[test]
fn allocator_state_exact_aliases_and_network_leases_survive_round_trip() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("allocator.db");
    let store = StateStore::open(&path).unwrap();
    let api_2 = ServiceReplicaKey::new("api", 2).unwrap();
    let api_dash_2 = ServiceReplicaKey::new("api-2", 1).unwrap();
    let snapshot = AllocatorSnapshot {
        schema_version: 2,
        ports: vec![
            AllocatorPortLease {
                target: api_2.clone(),
                ports: vec![PublishedPort {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: 8080,
                }],
            },
            AllocatorPortLease {
                target: api_dash_2.clone(),
                ports: vec![PublishedPort {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: 8081,
                }],
            },
        ],
        service_ips: vec![
            AllocatorIpLease {
                target: api_2.clone(),
                ip: "10.0.0.2".to_string(),
            },
            AllocatorIpLease {
                target: api_dash_2.clone(),
                ip: "10.0.0.3".to_string(),
            },
        ],
        service_network_ips: vec![
            AllocatorNetworkIpLease {
                target: api_2,
                network_name: "dev".to_string(),
                ip: "10.0.0.2".to_string(),
            },
            AllocatorNetworkIpLease {
                target: api_dash_2,
                network_name: "dev".to_string(),
                ip: "10.0.0.3".to_string(),
            },
        ],
        mount_tag_offsets: HashMap::new(),
    };
    store.save_allocator_state("stack", &snapshot).unwrap();
    drop(store);
    let reopened = StateStore::open(&path).unwrap();
    assert_eq!(
        reopened.load_allocator_state("stack").unwrap(),
        Some(snapshot)
    );
}

#[test]
fn allocator_state_rejects_duplicate_exact_targets_and_host_ports() {
    let store = StateStore::in_memory().unwrap();
    let target = ServiceReplicaKey::first("api").unwrap();
    let duplicated = AllocatorSnapshot {
        schema_version: 2,
        ports: vec![
            AllocatorPortLease {
                target: target.clone(),
                ports: vec![PublishedPort {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: 8080,
                }],
            },
            AllocatorPortLease {
                target,
                ports: vec![PublishedPort {
                    protocol: "tcp".to_string(),
                    container_port: 81,
                    host_port: 8080,
                }],
            },
        ],
        service_ips: vec![],
        service_network_ips: vec![],
        mount_tag_offsets: HashMap::new(),
    };
    assert!(store.save_allocator_state("stack", &duplicated).is_err());
    assert!(store.load_allocator_state("stack").unwrap().is_none());
}

// ── Reconcile session tests ──

fn sample_session(id: &str, stack: &str) -> ReconcileSession {
    let actions = sample_actions_for_stack(stack);
    ReconcileSession {
        session_id: id.to_string(),
        stack_name: stack.to_string(),
        operation_id: "op-1".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 2,
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    }
}

fn sample_actions() -> Vec<Action> {
    sample_actions_for_stack("myapp")
}

fn sample_actions_for_stack(stack: &str) -> Vec<Action> {
    vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack(stack),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack(stack),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
    ]
}

#[test]
fn reconcile_session_create_and_load_active() {
    let store = StateStore::in_memory().unwrap();
    let session = sample_session("rs-1", "myapp");
    let actions = sample_actions();

    store.create_reconcile_session(&session, &actions).unwrap();

    let loaded = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(loaded.session_id, "rs-1");
    assert_eq!(loaded.stack_name, "myapp");
    assert_eq!(loaded.status, ReconcileSessionStatus::Active);
    assert_eq!(
        loaded.actions_hash,
        crate::reconcile::compute_actions_hash(&actions)
    );
    assert_eq!(loaded.next_action_index, 0);
    assert_eq!(loaded.total_actions, 2);
}

#[test]
fn reconcile_session_exact_lookup_covers_missing_active_and_terminal_states() {
    let store = StateStore::in_memory().unwrap();
    assert!(store.load_reconcile_session("missing").unwrap().is_none());

    let active = sample_session("rs-exact-active", "myapp");
    store
        .create_reconcile_session(&active, &sample_actions())
        .unwrap();
    let loaded = store
        .load_reconcile_session("rs-exact-active")
        .unwrap()
        .unwrap();
    assert_eq!(loaded.session_id, active.session_id);
    assert_eq!(loaded.stack_name, active.stack_name);
    assert_eq!(loaded.operation_id, active.operation_id);
    assert_eq!(loaded.status, active.status);
    assert_eq!(loaded.actions_hash, active.actions_hash);
    assert_eq!(loaded.next_action_index, active.next_action_index);
    assert_eq!(loaded.total_actions, active.total_actions);
    assert_eq!(loaded.started_at, active.started_at);
    assert_eq!(loaded.updated_at, active.updated_at);
    assert_eq!(loaded.completed_at, active.completed_at);

    store
        .update_reconcile_session_progress("rs-exact-active", 2, &ReconcileSessionStatus::Active)
        .unwrap();
    store.complete_reconcile_session("rs-exact-active").unwrap();
    let completed = store
        .load_reconcile_session("rs-exact-active")
        .unwrap()
        .unwrap();
    assert_eq!(completed.status, ReconcileSessionStatus::Completed);
    assert_eq!(completed.next_action_index, completed.total_actions);
    assert!(completed.completed_at.is_some());

    let failed = sample_session("rs-exact-failed", "other");
    store
        .create_reconcile_session(&failed, &sample_actions_for_stack("other"))
        .unwrap();
    store.fail_reconcile_session("rs-exact-failed").unwrap();
    let failed = store
        .load_reconcile_session("rs-exact-failed")
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, ReconcileSessionStatus::Failed);
    assert!(failed.completed_at.is_some());
}

#[test]
fn reconcile_session_exact_lookup_rejects_malformed_action_metadata() {
    let store = StateStore::in_memory().unwrap();
    let session = sample_session("rs-exact-malformed", "myapp");
    store
        .create_reconcile_session(&session, &sample_actions())
        .unwrap();
    store
        .conn
        .execute("DROP TRIGGER reconcile_session_identity_immutable", [])
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE reconcile_sessions SET actions_hash = 'tampered' WHERE session_id = ?1",
            params![session.session_id],
        )
        .unwrap();

    let error = store
        .load_reconcile_session("rs-exact-malformed")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("action metadata is inconsistent")
    );
}

#[test]
fn reconcile_session_exact_lookup_rejects_impossible_status_shapes() {
    struct Case {
        id: &'static str,
        status: &'static str,
        cursor: i64,
        completed_at: Option<i64>,
    }
    let cases = [
        Case {
            id: "active-at-end",
            status: "active",
            cursor: 2,
            completed_at: None,
        },
        Case {
            id: "active-completed",
            status: "active",
            cursor: 0,
            completed_at: Some(10),
        },
        Case {
            id: "completed-partial",
            status: "completed",
            cursor: 1,
            completed_at: Some(10),
        },
        Case {
            id: "completed-without-time",
            status: "completed",
            cursor: 2,
            completed_at: None,
        },
        Case {
            id: "failed-without-time",
            status: "failed",
            cursor: 0,
            completed_at: None,
        },
        Case {
            id: "superseded-without-time",
            status: "superseded",
            cursor: 0,
            completed_at: None,
        },
    ];

    for case in cases {
        let store = StateStore::in_memory().unwrap();
        let session_id = format!("rs-shape-{}", case.id);
        let session = sample_session(&session_id, "myapp");
        store
            .create_reconcile_session(&session, &sample_actions())
            .unwrap();
        store
            .conn
            .execute_batch("PRAGMA ignore_check_constraints = ON;")
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE reconcile_sessions
                 SET status = ?1, next_action_index = ?2, completed_at = ?3
                 WHERE session_id = ?4",
                params![case.status, case.cursor, case.completed_at, session_id],
            )
            .unwrap();
        store
            .conn
            .execute_batch("PRAGMA ignore_check_constraints = OFF;")
            .unwrap();

        let error = store.load_reconcile_session(&session_id).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("status, cursor, and completion metadata are inconsistent"),
            "unexpected error for {}: {error}",
            case.id
        );
    }
}

#[test]
fn reconcile_session_exact_lookup_rejects_blank_persisted_identity() {
    for (column, value) in [("stack_name", "   "), ("operation_id", "")] {
        let store = StateStore::in_memory().unwrap();
        let session = sample_session("rs-blank-identity", "myapp");
        store
            .create_reconcile_session(&session, &sample_actions())
            .unwrap();
        store
            .conn
            .execute("DROP TRIGGER reconcile_session_identity_immutable", [])
            .unwrap();
        store
            .conn
            .execute(
                &format!("UPDATE reconcile_sessions SET {column} = ?1 WHERE session_id = ?2"),
                params![value, session.session_id],
            )
            .unwrap();

        assert!(store.load_reconcile_session("rs-blank-identity").is_err());
    }
    assert!(
        StateStore::in_memory()
            .unwrap()
            .load_reconcile_session("  ")
            .is_err()
    );
}

#[test]
fn reconcile_session_update_progress() {
    let store = StateStore::in_memory().unwrap();
    let session = sample_session("rs-2", "myapp");
    store
        .create_reconcile_session(&session, &sample_actions())
        .unwrap();

    store
        .update_reconcile_session_progress("rs-2", 1, &ReconcileSessionStatus::Active)
        .unwrap();

    let loaded = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(loaded.next_action_index, 1);
    assert_eq!(loaded.status, ReconcileSessionStatus::Active);
}

#[test]
fn reconcile_session_complete() {
    let store = StateStore::in_memory().unwrap();
    let session = sample_session("rs-3", "myapp");
    store
        .create_reconcile_session(&session, &sample_actions())
        .unwrap();

    store
        .update_reconcile_session_progress("rs-3", 2, &ReconcileSessionStatus::Active)
        .unwrap();

    store.complete_reconcile_session("rs-3").unwrap();

    // Active load should return None since it's completed now.
    let active = store.load_active_reconcile_session("myapp").unwrap();
    assert!(active.is_none());

    // List should show it as completed.
    let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Completed);
    assert!(sessions[0].completed_at.is_some());
}

#[test]
fn reconcile_session_fail() {
    let store = StateStore::in_memory().unwrap();
    let session = sample_session("rs-4", "myapp");
    store
        .create_reconcile_session(&session, &sample_actions())
        .unwrap();

    store.fail_reconcile_session("rs-4").unwrap();

    let active = store.load_active_reconcile_session("myapp").unwrap();
    assert!(active.is_none());

    let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Failed);
    assert!(sessions[0].completed_at.is_some());
}

#[test]
fn reconcile_session_supersede_active() {
    let store = StateStore::in_memory().unwrap();

    let session1 = sample_session("rs-5", "myapp");
    store
        .create_reconcile_session(&session1, &sample_actions())
        .unwrap();

    let count = store.supersede_active_sessions("myapp").unwrap();
    assert_eq!(count, 1);

    let active = store.load_active_reconcile_session("myapp").unwrap();
    assert!(active.is_none());

    let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Superseded);
}

#[test]
fn reconcile_session_list_respects_limit_and_ordering() {
    let store = StateStore::in_memory().unwrap();

    for i in 0..5 {
        let mut session = sample_session(&format!("rs-{i}"), "myapp");
        session.started_at = 1_700_000_000 + i as u64;
        session.updated_at = session.started_at;
        store.complete_reconcile_session(&format!("rs-{i}")).ok();
        store
            .create_reconcile_session(&session, &sample_actions())
            .unwrap();
        store
            .update_reconcile_session_progress(
                &format!("rs-{i}"),
                2,
                &ReconcileSessionStatus::Active,
            )
            .unwrap();
        store
            .complete_reconcile_session(&format!("rs-{i}"))
            .unwrap();
    }

    let all = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(all.len(), 5);
    // Ordered by started_at DESC.
    assert!(all[0].started_at >= all[1].started_at);

    let limited = store.list_reconcile_sessions("myapp", 2).unwrap();
    assert_eq!(limited.len(), 2);
}

#[test]
fn reconcile_session_no_active_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let active = store.load_active_reconcile_session("nonexistent").unwrap();
    assert!(active.is_none());
}

#[test]
fn reconcile_session_stacks_are_isolated() {
    let store = StateStore::in_memory().unwrap();

    let s1 = sample_session("rs-a1", "app1");
    let s2 = sample_session("rs-b1", "app2");
    store
        .create_reconcile_session(&s1, &sample_actions_for_stack("app1"))
        .unwrap();
    store
        .create_reconcile_session(&s2, &sample_actions_for_stack("app2"))
        .unwrap();

    let active1 = store
        .load_active_reconcile_session("app1")
        .unwrap()
        .unwrap();
    assert_eq!(active1.session_id, "rs-a1");

    let active2 = store
        .load_active_reconcile_session("app2")
        .unwrap()
        .unwrap();
    assert_eq!(active2.session_id, "rs-b1");

    // Supersede only app1.
    store.supersede_active_sessions("app1").unwrap();
    assert!(
        store
            .load_active_reconcile_session("app1")
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_active_reconcile_session("app2")
            .unwrap()
            .is_some()
    );
}

// ── Idempotency key persistence tests ──

fn sample_idempotency_record(key: &str) -> IdempotencyRecord {
    IdempotencyRecord {
        key: key.to_string(),
        operation: "create_sandbox".to_string(),
        request_hash: "abc123".to_string(),
        response_json: r#"{"sandbox_id":"sbx-1"}"#.to_string(),
        status_code: 201,
        created_at: 1_700_000_000,
        expires_at: 1_700_000_000 + IDEMPOTENCY_TTL_SECS,
    }
}

#[test]
fn idempotency_save_and_find_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let record = sample_idempotency_record("ik-1");

    store.save_idempotency_result(&record).unwrap();
    let loaded = store.find_idempotency_result("ik-1").unwrap().unwrap();
    assert_eq!(loaded.key, "ik-1");
    assert_eq!(loaded.operation, "create_sandbox");
    assert_eq!(loaded.request_hash, "abc123");
    assert_eq!(loaded.response_json, r#"{"sandbox_id":"sbx-1"}"#);
    assert_eq!(loaded.status_code, 201);
    assert_eq!(loaded.created_at, 1_700_000_000);
    assert_eq!(loaded.expires_at, 1_700_000_000 + IDEMPOTENCY_TTL_SECS);
}

#[test]
fn idempotency_missing_key_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.find_idempotency_result("nonexistent").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn idempotency_cleanup_removes_expired_keys() {
    let store = StateStore::in_memory().unwrap();

    // Record with expires_at in the past (epoch 0 + TTL = ~1 day).
    let expired = IdempotencyRecord {
        key: "ik-expired".to_string(),
        operation: "create_sandbox".to_string(),
        request_hash: "hash1".to_string(),
        response_json: "{}".to_string(),
        status_code: 201,
        created_at: 0,
        expires_at: 1, // Far in the past
    };
    store.save_idempotency_result(&expired).unwrap();

    // Record with expires_at far in the future.
    let fresh = IdempotencyRecord {
        key: "ik-fresh".to_string(),
        operation: "create_sandbox".to_string(),
        request_hash: "hash2".to_string(),
        response_json: "{}".to_string(),
        status_code: 201,
        created_at: 1_700_000_000,
        expires_at: u64::MAX / 2, // Far in the future
    };
    store.save_idempotency_result(&fresh).unwrap();

    let deleted = store.cleanup_expired_idempotency_keys().unwrap();
    assert_eq!(deleted, 1);

    // Expired key is gone.
    assert!(
        store
            .find_idempotency_result("ik-expired")
            .unwrap()
            .is_none()
    );
    // Fresh key is still present.
    assert!(store.find_idempotency_result("ik-fresh").unwrap().is_some());
}

// ── Lease persistence tests ──

fn sample_lease(id: &str, sandbox_id: &str) -> Lease {
    Lease {
        lease_id: id.to_string(),
        sandbox_id: sandbox_id.to_string(),
        ttl_secs: 300,
        last_heartbeat_at: 1_700_000_000,
        state: LeaseState::Opening,
    }
}

#[test]
fn lease_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let lease = sample_lease("ls-1", "sb-1");

    store.save_lease(&lease).unwrap();
    let loaded = store.load_lease("ls-1").unwrap().unwrap();
    assert_eq!(loaded, lease);
}

#[test]
fn lease_list_for_sandbox() {
    let store = StateStore::in_memory().unwrap();
    let lease1 = sample_lease("ls-a", "sb-1");
    let lease2 = sample_lease("ls-b", "sb-1");
    let lease3 = sample_lease("ls-c", "sb-2");

    store.save_lease(&lease1).unwrap();
    store.save_lease(&lease2).unwrap();
    store.save_lease(&lease3).unwrap();

    let sb1_leases = store.list_leases_for_sandbox("sb-1").unwrap();
    assert_eq!(sb1_leases.len(), 2);

    let sb2_leases = store.list_leases_for_sandbox("sb-2").unwrap();
    assert_eq!(sb2_leases.len(), 1);
}

#[test]
fn lease_list_returns_all() {
    let store = StateStore::in_memory().unwrap();
    let lease1 = sample_lease("ls-x", "sb-1");
    let lease2 = sample_lease("ls-y", "sb-2");

    store.save_lease(&lease1).unwrap();
    store.save_lease(&lease2).unwrap();

    let all = store.list_leases().unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn lease_delete_removes() {
    let store = StateStore::in_memory().unwrap();
    let lease = sample_lease("ls-del", "sb-1");

    store.save_lease(&lease).unwrap();
    store.delete_lease("ls-del").unwrap();
    let loaded = store.load_lease("ls-del").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn lease_upsert_updates_state() {
    let store = StateStore::in_memory().unwrap();
    let mut lease = sample_lease("ls-up", "sb-1");

    store.save_lease(&lease).unwrap();

    lease.state = LeaseState::Active;
    lease.last_heartbeat_at = 1_700_000_100;
    store.save_lease(&lease).unwrap();

    let loaded = store.load_lease("ls-up").unwrap().unwrap();
    assert_eq!(loaded.state, LeaseState::Active);
    assert_eq!(loaded.last_heartbeat_at, 1_700_000_100);
}

// ── Execution persistence tests ──

fn sample_execution(id: &str, container_id: &str) -> Execution {
    Execution {
        execution_id: id.to_string(),
        container_id: container_id.to_string(),
        exec_spec: ExecutionSpec {
            cmd: vec!["echo".to_string(), "hello".to_string()],
            args: vec![],
            env_override: std::collections::BTreeMap::new(),
            pty: false,
            timeout_secs: None,
        },
        state: ExecutionState::Queued,
        exit_code: None,
        started_at: None,
        ended_at: None,
    }
}

#[test]
fn execution_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let execution = sample_execution("exec-1", "ctr-abc");

    store.save_execution(&execution).unwrap();
    let loaded = store.load_execution("exec-1").unwrap().unwrap();
    assert_eq!(loaded.execution_id, "exec-1");
    assert_eq!(loaded.container_id, "ctr-abc");
    assert_eq!(loaded.state, ExecutionState::Queued);
    assert_eq!(loaded.exec_spec.cmd, vec!["echo", "hello"]);
}

#[test]
fn execution_list_returns_all() {
    let store = StateStore::in_memory().unwrap();
    store
        .save_execution(&sample_execution("exec-a", "ctr-1"))
        .unwrap();
    store
        .save_execution(&sample_execution("exec-b", "ctr-2"))
        .unwrap();

    let all = store.list_executions().unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn execution_list_for_container() {
    let store = StateStore::in_memory().unwrap();
    store
        .save_execution(&sample_execution("exec-a", "ctr-1"))
        .unwrap();
    store
        .save_execution(&sample_execution("exec-b", "ctr-1"))
        .unwrap();
    store
        .save_execution(&sample_execution("exec-c", "ctr-2"))
        .unwrap();

    let for_ctr1 = store.list_executions_for_container("ctr-1").unwrap();
    assert_eq!(for_ctr1.len(), 2);
    assert!(for_ctr1.iter().all(|e| e.container_id == "ctr-1"));

    let for_ctr2 = store.list_executions_for_container("ctr-2").unwrap();
    assert_eq!(for_ctr2.len(), 1);
}

#[test]
fn execution_delete_removes() {
    let store = StateStore::in_memory().unwrap();
    store
        .save_execution(&sample_execution("exec-del", "ctr-1"))
        .unwrap();
    store.delete_execution("exec-del").unwrap();
    let loaded = store.load_execution("exec-del").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn execution_upsert_updates_state() {
    let store = StateStore::in_memory().unwrap();
    let mut execution = sample_execution("exec-up", "ctr-1");

    store.save_execution(&execution).unwrap();

    execution.state = ExecutionState::Running;
    execution.started_at = Some(1_700_000_000);
    store.save_execution(&execution).unwrap();

    let loaded = store.load_execution("exec-up").unwrap().unwrap();
    assert_eq!(loaded.state, ExecutionState::Running);
    assert_eq!(loaded.started_at, Some(1_700_000_000));
}

#[test]
fn execution_missing_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.load_execution("nonexistent").unwrap();
    assert!(loaded.is_none());
}

// ── Checkpoint persistence tests ──

fn sample_checkpoint(id: &str, sandbox_id: &str) -> Checkpoint {
    Checkpoint {
        checkpoint_id: id.to_string(),
        sandbox_id: sandbox_id.to_string(),
        parent_checkpoint_id: None,
        class: CheckpointClass::FsQuick,
        state: CheckpointState::Creating,
        created_at: 1_700_000_000,
        compatibility_fingerprint: "fp-abc123".to_string(),
    }
}

#[test]
fn checkpoint_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let checkpoint = sample_checkpoint("ckpt-1", "sb-1");

    store.save_checkpoint(&checkpoint).unwrap();
    let loaded = store.load_checkpoint("ckpt-1").unwrap().unwrap();
    assert_eq!(loaded, checkpoint);
}

#[test]
fn checkpoint_missing_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.load_checkpoint("nonexistent").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn checkpoint_upsert_updates_state() {
    let store = StateStore::in_memory().unwrap();
    let mut checkpoint = sample_checkpoint("ckpt-up", "sb-1");

    store.save_checkpoint(&checkpoint).unwrap();

    checkpoint.state = CheckpointState::Ready;
    store.save_checkpoint(&checkpoint).unwrap();

    let loaded = store.load_checkpoint("ckpt-up").unwrap().unwrap();
    assert_eq!(loaded.state, CheckpointState::Ready);
}

#[test]
fn checkpoint_list_returns_all_ordered() {
    let store = StateStore::in_memory().unwrap();
    let ckpt1 = sample_checkpoint("ckpt-a", "sb-1");
    let mut ckpt2 = sample_checkpoint("ckpt-b", "sb-2");
    ckpt2.created_at = 1_700_000_001;

    store.save_checkpoint(&ckpt1).unwrap();
    store.save_checkpoint(&ckpt2).unwrap();

    let all = store.list_checkpoints().unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].checkpoint_id, "ckpt-a");
    assert_eq!(all[1].checkpoint_id, "ckpt-b");
}

#[test]
fn checkpoint_list_for_sandbox_filters() {
    let store = StateStore::in_memory().unwrap();
    let ckpt1 = sample_checkpoint("ckpt-1", "sb-1");
    let ckpt2 = sample_checkpoint("ckpt-2", "sb-2");
    let mut ckpt3 = sample_checkpoint("ckpt-3", "sb-1");
    ckpt3.created_at = 1_700_000_001;

    store.save_checkpoint(&ckpt1).unwrap();
    store.save_checkpoint(&ckpt2).unwrap();
    store.save_checkpoint(&ckpt3).unwrap();

    let sb1 = store.list_checkpoints_for_sandbox("sb-1").unwrap();
    assert_eq!(sb1.len(), 2);
    assert!(sb1.iter().all(|c| c.sandbox_id == "sb-1"));
}

#[test]
fn checkpoint_children_returns_direct_children() {
    let store = StateStore::in_memory().unwrap();
    let parent = sample_checkpoint("ckpt-parent", "sb-1");
    let mut child1 = sample_checkpoint("ckpt-child1", "sb-2");
    child1.parent_checkpoint_id = Some("ckpt-parent".to_string());
    let mut child2 = sample_checkpoint("ckpt-child2", "sb-3");
    child2.parent_checkpoint_id = Some("ckpt-parent".to_string());
    child2.created_at = 1_700_000_001;
    let unrelated = sample_checkpoint("ckpt-other", "sb-4");

    store.save_checkpoint(&parent).unwrap();
    store.save_checkpoint(&child1).unwrap();
    store.save_checkpoint(&child2).unwrap();
    store.save_checkpoint(&unrelated).unwrap();

    let children = store.list_checkpoint_children("ckpt-parent").unwrap();
    assert_eq!(children.len(), 2);
    assert!(
        children
            .iter()
            .all(|c| c.parent_checkpoint_id.as_deref() == Some("ckpt-parent"))
    );
}

#[test]
fn checkpoint_delete_removes() {
    let store = StateStore::in_memory().unwrap();
    let checkpoint = sample_checkpoint("ckpt-del", "sb-1");

    store.save_checkpoint(&checkpoint).unwrap();
    store.delete_checkpoint("ckpt-del").unwrap();
    let loaded = store.load_checkpoint("ckpt-del").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn checkpoint_file_entries_round_trip_replaces_and_orders_by_path() {
    let store = StateStore::in_memory().unwrap();
    let checkpoint = sample_checkpoint("ckpt-files", "sb-1");
    store.save_checkpoint(&checkpoint).unwrap();

    store
        .replace_checkpoint_file_entries(
            "ckpt-files",
            &[
                CheckpointFileEntry {
                    path: "z.txt".to_string(),
                    digest_sha256: "digest-z".to_string(),
                    size: 3,
                },
                CheckpointFileEntry {
                    path: "a.txt".to_string(),
                    digest_sha256: "digest-a".to_string(),
                    size: 1,
                },
            ],
        )
        .unwrap();

    let loaded = store.load_checkpoint_file_entries("ckpt-files").unwrap();
    let paths: Vec<_> = loaded.iter().map(|entry| entry.path.as_str()).collect();
    assert_eq!(paths, vec!["a.txt", "z.txt"]);

    store
        .replace_checkpoint_file_entries(
            "ckpt-files",
            &[CheckpointFileEntry {
                path: "m.txt".to_string(),
                digest_sha256: "digest-m".to_string(),
                size: 2,
            }],
        )
        .unwrap();
    let replaced = store.load_checkpoint_file_entries("ckpt-files").unwrap();
    assert_eq!(replaced.len(), 1);
    assert_eq!(replaced[0].path, "m.txt");
}

#[test]
fn checkpoint_delete_cascades_checkpoint_file_entries() {
    let store = StateStore::in_memory().unwrap();
    let checkpoint = sample_checkpoint("ckpt-del-files", "sb-1");
    store.save_checkpoint(&checkpoint).unwrap();
    store
        .replace_checkpoint_file_entries(
            "ckpt-del-files",
            &[CheckpointFileEntry {
                path: "artifact.bin".to_string(),
                digest_sha256: "digest-artifact".to_string(),
                size: 42,
            }],
        )
        .unwrap();

    store.delete_checkpoint("ckpt-del-files").unwrap();
    let loaded = store
        .load_checkpoint_file_entries("ckpt-del-files")
        .unwrap();
    assert!(loaded.is_empty());
}

#[test]
fn checkpoint_null_parent_round_trips() {
    let store = StateStore::in_memory().unwrap();
    let checkpoint = sample_checkpoint("ckpt-null-parent", "sb-1");
    assert!(checkpoint.parent_checkpoint_id.is_none());

    store.save_checkpoint(&checkpoint).unwrap();
    let loaded = store.load_checkpoint("ckpt-null-parent").unwrap().unwrap();
    assert!(loaded.parent_checkpoint_id.is_none());
}

#[test]
fn checkpoint_vm_full_class_persists() {
    let store = StateStore::in_memory().unwrap();
    let mut checkpoint = sample_checkpoint("ckpt-vm", "sb-1");
    checkpoint.class = CheckpointClass::VmFull;

    store.save_checkpoint(&checkpoint).unwrap();
    let loaded = store.load_checkpoint("ckpt-vm").unwrap().unwrap();
    assert_eq!(loaded.class, CheckpointClass::VmFull);
}

#[test]
fn checkpoint_retention_tag_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let checkpoint = sample_checkpoint("ckpt-tagged", "sb-1");
    store.save_checkpoint(&checkpoint).unwrap();

    store
        .save_checkpoint_retention_tag("ckpt-tagged", "pre-session")
        .unwrap();
    let loaded = store
        .load_checkpoint_retention_tag("ckpt-tagged")
        .unwrap()
        .unwrap();
    assert_eq!(loaded, "pre-session");

    store
        .delete_checkpoint_retention_tag("ckpt-tagged")
        .unwrap();
    assert!(
        store
            .load_checkpoint_retention_tag("ckpt-tagged")
            .unwrap()
            .is_none()
    );
}

#[test]
fn checkpoint_gc_respects_tags_and_is_idempotent() {
    let store = StateStore::in_memory().unwrap();

    let mut old_age = sample_checkpoint("ckpt-age", "sb-1");
    old_age.created_at = 10;
    let mut old_count = sample_checkpoint("ckpt-count", "sb-1");
    old_count.created_at = 61;
    let mut tagged = sample_checkpoint("ckpt-tagged", "sb-1");
    tagged.created_at = 20;
    let mut newest = sample_checkpoint("ckpt-keep", "sb-1");
    newest.created_at = 62;

    store.save_checkpoint(&old_age).unwrap();
    store.save_checkpoint(&old_count).unwrap();
    store.save_checkpoint(&tagged).unwrap();
    store.save_checkpoint(&newest).unwrap();
    store
        .save_checkpoint_retention_tag("ckpt-tagged", "golden")
        .unwrap();

    let policy = CheckpointRetentionPolicy {
        max_untagged_count: 1,
        max_age_secs: 40,
    };
    let state_map = store.checkpoint_retention_state_map(policy, 100).unwrap();
    assert_eq!(
        state_map.get("ckpt-age").and_then(|s| s.gc_reason),
        Some(RetentionGcReason::AgeLimit)
    );
    assert_eq!(
        state_map.get("ckpt-count").and_then(|s| s.gc_reason),
        Some(RetentionGcReason::CountLimit)
    );
    assert_eq!(state_map.get("ckpt-tagged").and_then(|s| s.gc_reason), None);
    assert_eq!(
        state_map.get("ckpt-tagged").map(|s| s.protected),
        Some(true)
    );

    let report = store
        .compact_checkpoints_with_policy_at(policy, 100)
        .unwrap();
    assert_eq!(report.deleted_by_age, vec!["ckpt-age".to_string()]);
    assert_eq!(report.deleted_by_count, vec!["ckpt-count".to_string()]);
    assert!(report.deleted_by_lineage.is_empty());

    let remaining_ids: Vec<_> = store
        .list_checkpoints()
        .unwrap()
        .into_iter()
        .map(|checkpoint| checkpoint.checkpoint_id)
        .collect();
    assert_eq!(
        remaining_ids,
        vec!["ckpt-tagged".to_string(), "ckpt-keep".to_string()]
    );

    let second = store
        .compact_checkpoints_with_policy_at(policy, 100)
        .unwrap();
    assert!(second.is_empty());
}

#[test]
fn checkpoint_gc_preserves_tagged_lineage_ancestors() {
    let store = StateStore::in_memory().unwrap();

    let mut root = sample_checkpoint("ckpt-root", "sb-1");
    root.created_at = 10;
    let mut child = sample_checkpoint("ckpt-child", "sb-1");
    child.parent_checkpoint_id = Some("ckpt-root".to_string());
    child.created_at = 20;
    let mut tagged_leaf = sample_checkpoint("ckpt-tagged-leaf", "sb-1");
    tagged_leaf.parent_checkpoint_id = Some("ckpt-child".to_string());
    tagged_leaf.created_at = 30;
    let mut old_unrelated = sample_checkpoint("ckpt-old-unrelated", "sb-1");
    old_unrelated.created_at = 5;

    store.save_checkpoint(&root).unwrap();
    store.save_checkpoint(&child).unwrap();
    store.save_checkpoint(&tagged_leaf).unwrap();
    store.save_checkpoint(&old_unrelated).unwrap();
    store
        .save_checkpoint_retention_tag("ckpt-tagged-leaf", "golden")
        .unwrap();

    let policy = CheckpointRetentionPolicy {
        max_untagged_count: 1,
        max_age_secs: 50,
    };

    let state_map = store.checkpoint_retention_state_map(policy, 100).unwrap();
    assert_eq!(state_map["ckpt-root"].gc_reason, None);
    assert_eq!(state_map["ckpt-child"].gc_reason, None);
    assert_eq!(state_map["ckpt-tagged-leaf"].gc_reason, None);

    let report = store
        .compact_checkpoints_with_policy_at(policy, 100)
        .unwrap();
    assert_eq!(
        report.deleted_by_age,
        vec!["ckpt-old-unrelated".to_string()]
    );
    assert!(report.deleted_by_count.is_empty());
    assert!(report.deleted_by_lineage.is_empty());

    assert!(store.load_checkpoint("ckpt-root").unwrap().is_some());
    assert!(store.load_checkpoint("ckpt-child").unwrap().is_some());
    assert!(store.load_checkpoint("ckpt-tagged-leaf").unwrap().is_some());
}

#[test]
fn checkpoint_gc_cascades_fork_descendants_with_lineage_reason() {
    let store = StateStore::in_memory().unwrap();

    let mut root = sample_checkpoint("ckpt-root", "sb-1");
    root.created_at = 10;
    let mut left = sample_checkpoint("ckpt-left", "sb-1");
    left.parent_checkpoint_id = Some("ckpt-root".to_string());
    left.created_at = 100;
    let mut right = sample_checkpoint("ckpt-right", "sb-1");
    right.parent_checkpoint_id = Some("ckpt-root".to_string());
    right.created_at = 101;
    let mut newest = sample_checkpoint("ckpt-newest", "sb-1");
    newest.created_at = 111;

    store.save_checkpoint(&root).unwrap();
    store.save_checkpoint(&left).unwrap();
    store.save_checkpoint(&right).unwrap();
    store.save_checkpoint(&newest).unwrap();

    let policy = CheckpointRetentionPolicy {
        max_untagged_count: 16,
        max_age_secs: 100,
    };
    let state_map = store.checkpoint_retention_state_map(policy, 120).unwrap();
    assert_eq!(
        state_map["ckpt-root"].gc_reason,
        Some(RetentionGcReason::AgeLimit)
    );
    assert_eq!(
        state_map["ckpt-left"].gc_reason,
        Some(RetentionGcReason::LineageCascade)
    );
    assert_eq!(
        state_map["ckpt-right"].gc_reason,
        Some(RetentionGcReason::LineageCascade)
    );
    assert_eq!(state_map["ckpt-newest"].gc_reason, None);

    let report = store
        .compact_checkpoints_with_policy_at(policy, 120)
        .unwrap();
    assert_eq!(report.deleted_by_age, vec!["ckpt-root".to_string()]);
    assert!(report.deleted_by_count.is_empty());
    assert_eq!(
        report.deleted_by_lineage,
        vec!["ckpt-left".to_string(), "ckpt-right".to_string()]
    );

    assert!(store.load_checkpoint("ckpt-root").unwrap().is_none());
    assert!(store.load_checkpoint("ckpt-left").unwrap().is_none());
    assert!(store.load_checkpoint("ckpt-right").unwrap().is_none());
    assert!(store.load_checkpoint("ckpt-newest").unwrap().is_some());
}

// ── Receipt persistence tests (from agent-a03881b1) ──

fn sample_receipt(receipt_id: &str, entity_id: &str) -> Receipt {
    Receipt {
        receipt_id: receipt_id.to_string(),
        operation: "create_sandbox".to_string(),
        entity_id: entity_id.to_string(),
        entity_type: "sandbox".to_string(),
        request_id: "req-1".to_string(),
        status: "completed".to_string(),
        created_at: 1_700_000_000,
        metadata: serde_json::Value::Object(serde_json::Map::new()),
    }
}

#[test]
fn receipt_save_and_load() {
    let store = StateStore::in_memory().unwrap();
    let receipt = sample_receipt("rcp-1", "sbx-1");

    store.save_receipt(&receipt).unwrap();
    let loaded = store.load_receipt("rcp-1").unwrap().unwrap();
    assert_eq!(loaded.receipt_id, "rcp-1");
    assert_eq!(loaded.operation, "create_sandbox");
    assert_eq!(loaded.entity_id, "sbx-1");
    assert_eq!(loaded.entity_type, "sandbox");
    assert_eq!(loaded.request_id, "req-1");
    assert_eq!(loaded.status, "completed");
    assert_eq!(loaded.created_at, 1_700_000_000);
}

#[test]
fn receipt_load_missing_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.load_receipt("nonexistent").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn receipt_load_by_request_id() {
    let store = StateStore::in_memory().unwrap();
    let receipt = sample_receipt("rcp-2", "sbx-2");

    store.save_receipt(&receipt).unwrap();
    let loaded = store.load_receipt_by_request_id("req-1").unwrap().unwrap();
    assert_eq!(loaded.receipt_id, "rcp-2");
}

#[test]
fn receipt_load_by_request_id_missing_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.load_receipt_by_request_id("nonexistent").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn receipt_list_for_entity() {
    let store = StateStore::in_memory().unwrap();

    let r1 = Receipt {
        receipt_id: "rcp-a".to_string(),
        operation: "create_sandbox".to_string(),
        entity_id: "sbx-1".to_string(),
        entity_type: "sandbox".to_string(),
        request_id: "req-a".to_string(),
        status: "completed".to_string(),
        created_at: 1_700_000_000,
        metadata: serde_json::Value::Object(serde_json::Map::new()),
    };
    let r2 = Receipt {
        receipt_id: "rcp-b".to_string(),
        operation: "terminate_sandbox".to_string(),
        entity_id: "sbx-1".to_string(),
        entity_type: "sandbox".to_string(),
        request_id: "req-b".to_string(),
        status: "completed".to_string(),
        created_at: 1_700_000_001,
        metadata: serde_json::Value::Object(serde_json::Map::new()),
    };
    let r3 = Receipt {
        receipt_id: "rcp-c".to_string(),
        operation: "create_sandbox".to_string(),
        entity_id: "sbx-2".to_string(),
        entity_type: "sandbox".to_string(),
        request_id: "req-c".to_string(),
        status: "completed".to_string(),
        created_at: 1_700_000_002,
        metadata: serde_json::Value::Object(serde_json::Map::new()),
    };

    store.save_receipt(&r1).unwrap();
    store.save_receipt(&r2).unwrap();
    store.save_receipt(&r3).unwrap();

    let sbx1_receipts = store.list_receipts_for_entity("sandbox", "sbx-1").unwrap();
    assert_eq!(sbx1_receipts.len(), 2);
    assert_eq!(sbx1_receipts[0].receipt_id, "rcp-a");
    assert_eq!(sbx1_receipts[1].receipt_id, "rcp-b");

    let sbx2_receipts = store.list_receipts_for_entity("sandbox", "sbx-2").unwrap();
    assert_eq!(sbx2_receipts.len(), 1);
    assert_eq!(sbx2_receipts[0].receipt_id, "rcp-c");

    let empty = store.list_receipts_for_entity("lease", "ls-1").unwrap();
    assert!(empty.is_empty());
}

#[test]
fn receipt_upsert_updates() {
    let store = StateStore::in_memory().unwrap();
    let mut receipt = sample_receipt("rcp-upsert", "sbx-1");
    receipt.status = "pending".to_string();
    store.save_receipt(&receipt).unwrap();

    receipt.status = "completed".to_string();
    store.save_receipt(&receipt).unwrap();

    let loaded = store.load_receipt("rcp-upsert").unwrap().unwrap();
    assert_eq!(loaded.status, "completed");
}

#[test]
fn receipt_gc_applies_age_then_count_and_is_idempotent() {
    let store = StateStore::in_memory().unwrap();

    let mut r1 = sample_receipt("rcp-age", "sbx-1");
    r1.created_at = 10;
    let mut r2 = sample_receipt("rcp-count", "sbx-1");
    r2.created_at = 20;
    let mut r3 = sample_receipt("rcp-keep", "sbx-1");
    r3.created_at = 30;

    store.save_receipt(&r1).unwrap();
    store.save_receipt(&r2).unwrap();
    store.save_receipt(&r3).unwrap();

    let policy = ReceiptRetentionPolicy {
        max_count: 1,
        max_age_secs: 60,
    };
    let state_map = store.receipt_retention_state_map(policy, 70).unwrap();
    assert_eq!(
        state_map.get("rcp-age").and_then(|s| s.gc_reason),
        Some(RetentionGcReason::AgeLimit)
    );
    assert_eq!(
        state_map.get("rcp-count").and_then(|s| s.gc_reason),
        Some(RetentionGcReason::CountLimit)
    );
    assert_eq!(state_map.get("rcp-keep").and_then(|s| s.gc_reason), None);

    let report = store.compact_receipts_with_policy_at(policy, 70).unwrap();
    assert_eq!(report.deleted_by_age, vec!["rcp-age".to_string()]);
    assert_eq!(report.deleted_by_count, vec!["rcp-count".to_string()]);

    let remaining_ids: Vec<_> = store
        .list_receipts()
        .unwrap()
        .into_iter()
        .map(|receipt| receipt.receipt_id)
        .collect();
    assert_eq!(remaining_ids, vec!["rcp-keep".to_string()]);

    let second = store.compact_receipts_with_policy_at(policy, 70).unwrap();
    assert!(second.is_empty());
}

// ── Scoped event listing tests (from agent-a03881b1) ──

#[test]
fn events_by_scope_filters_on_type_prefix() {
    let store = StateStore::in_memory().unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::SandboxCreating {
                stack_name: "myapp".to_string(),
                sandbox_id: "sb-1".to_string(),
            },
        )
        .unwrap();
    store
        .emit_event(
            "myapp",
            &StackEvent::LeaseOpened {
                sandbox_id: "sb-1".to_string(),
                lease_id: "ls-1".to_string(),
            },
        )
        .unwrap();
    store
        .emit_event(
            "myapp",
            &StackEvent::SandboxReady {
                stack_name: "myapp".to_string(),
                sandbox_id: "sb-1".to_string(),
            },
        )
        .unwrap();
    store
        .emit_event(
            "myapp",
            &StackEvent::ExecutionQueued {
                container_id: "ctr-1".to_string(),
                execution_id: "exec-1".to_string(),
            },
        )
        .unwrap();

    let sandbox_events = store
        .load_events_by_scope("myapp", "sandbox_", None, 100)
        .unwrap();
    assert_eq!(sandbox_events.len(), 2);

    let lease_events = store
        .load_events_by_scope("myapp", "lease_", None, 100)
        .unwrap();
    assert_eq!(lease_events.len(), 1);

    let exec_events = store
        .load_events_by_scope("myapp", "execution_", None, 100)
        .unwrap();
    assert_eq!(exec_events.len(), 1);
}

#[test]
fn events_by_scope_respects_cursor_and_limit() {
    let store = StateStore::in_memory().unwrap();

    for i in 0..5 {
        store
            .emit_event(
                "myapp",
                &StackEvent::SandboxCreating {
                    stack_name: "myapp".to_string(),
                    sandbox_id: format!("sb-{i}"),
                },
            )
            .unwrap();
    }

    let first_page = store
        .load_events_by_scope("myapp", "sandbox_", None, 2)
        .unwrap();
    assert_eq!(first_page.len(), 2);

    let cursor = first_page.last().map(|r| r.id);
    let second_page = store
        .load_events_by_scope("myapp", "sandbox_", cursor, 2)
        .unwrap();
    assert_eq!(second_page.len(), 2);

    // IDs should be strictly greater than the cursor
    assert!(second_page[0].id > first_page[1].id);
}

#[test]
fn events_by_scope_empty_scope_returns_nothing() {
    let store = StateStore::in_memory().unwrap();
    store
        .emit_event(
            "myapp",
            &StackEvent::SandboxCreating {
                stack_name: "myapp".to_string(),
                sandbox_id: "sb-1".to_string(),
            },
        )
        .unwrap();

    let events = store
        .load_events_by_scope("myapp", "nonexistent_", None, 100)
        .unwrap();
    assert!(events.is_empty());
}

// ── Build persistence tests (from agent-af0c4a41) ──

fn sample_build(id: &str, sandbox_id: &str) -> Build {
    Build {
        build_id: id.to_string(),
        sandbox_id: sandbox_id.to_string(),
        build_spec: BuildSpec {
            context: "/tmp/ctx".to_string(),
            dockerfile: Some("Dockerfile".to_string()),
            target: None,
            args: std::collections::BTreeMap::new(),
            cache_from: Vec::new(),
            image_tag: None,
            secrets: Vec::new(),
            no_cache: false,
            push: false,
            output_oci_tar_dest: None,
        },
        state: BuildState::Queued,
        result_digest: None,
        started_at: 1_700_000_000,
        ended_at: None,
    }
}

#[test]
fn build_round_trip() {
    let store = StateStore::in_memory().unwrap();
    let build = sample_build("bld-1", "sb-1");

    store.save_build(&build).unwrap();
    let loaded = store.load_build("bld-1").unwrap().unwrap();
    assert_eq!(loaded.build_id, "bld-1");
    assert_eq!(loaded.sandbox_id, "sb-1");
    assert_eq!(loaded.state, BuildState::Queued);
    assert_eq!(loaded.build_spec.context, "/tmp/ctx");
}

#[test]
fn build_list_returns_all() {
    let store = StateStore::in_memory().unwrap();
    store.save_build(&sample_build("bld-a", "sb-1")).unwrap();
    store.save_build(&sample_build("bld-b", "sb-2")).unwrap();

    let all = store.list_builds().unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn build_list_for_sandbox() {
    let store = StateStore::in_memory().unwrap();
    store.save_build(&sample_build("bld-a", "sb-1")).unwrap();
    store.save_build(&sample_build("bld-b", "sb-1")).unwrap();
    store.save_build(&sample_build("bld-c", "sb-2")).unwrap();

    let for_sb1 = store.list_builds_for_sandbox("sb-1").unwrap();
    assert_eq!(for_sb1.len(), 2);
    assert!(for_sb1.iter().all(|b| b.sandbox_id == "sb-1"));

    let for_sb2 = store.list_builds_for_sandbox("sb-2").unwrap();
    assert_eq!(for_sb2.len(), 1);
}

#[test]
fn build_delete_removes() {
    let store = StateStore::in_memory().unwrap();
    store.save_build(&sample_build("bld-del", "sb-1")).unwrap();
    store.delete_build("bld-del").unwrap();
    let loaded = store.load_build("bld-del").unwrap();
    assert!(loaded.is_none());
}

#[test]
fn build_upsert_updates_state() {
    let store = StateStore::in_memory().unwrap();
    let mut build = sample_build("bld-up", "sb-1");

    store.save_build(&build).unwrap();

    build.state = BuildState::Running;
    store.save_build(&build).unwrap();

    let loaded = store.load_build("bld-up").unwrap().unwrap();
    assert_eq!(loaded.state, BuildState::Running);
}

#[test]
fn build_missing_returns_none() {
    let store = StateStore::in_memory().unwrap();
    let loaded = store.load_build("nonexistent").unwrap();
    assert!(loaded.is_none());
}

// ── Phase 1 validation tests (from agent-a80ffa89) ──

#[test]
fn phase1_validation_health_state_persistence_round_trip() {
    let store = StateStore::in_memory().unwrap();

    let mut original_state = HashMap::new();
    original_state.insert(
        "web".to_string(),
        HealthPollState {
            service_name: "web".to_string(),
            consecutive_passes: 5,
            consecutive_failures: 0,
            last_check_millis: Some(1_700_000_000_000),
            start_time_millis: Some(1_700_000_000_123),
        },
    );
    original_state.insert(
        "db".to_string(),
        HealthPollState {
            service_name: "db".to_string(),
            consecutive_passes: 0,
            consecutive_failures: 3,
            last_check_millis: Some(1_700_000_001_000),
            start_time_millis: Some(1_700_000_000_456),
        },
    );

    // Save to store.
    store
        .save_health_poller_state("myapp", &original_state)
        .unwrap();

    // Load from a fresh perspective (same store, simulating reload).
    let loaded = store.load_health_poller_state("myapp").unwrap();

    assert_eq!(loaded.len(), 2);
    assert_eq!(
        loaded.get("web").unwrap(),
        original_state.get("web").unwrap()
    );
    assert_eq!(loaded.get("db").unwrap(), original_state.get("db").unwrap());

    // Verify counters survived the round-trip.
    let web = loaded.get("web").unwrap();
    assert_eq!(web.consecutive_passes, 5);
    assert_eq!(web.consecutive_failures, 0);

    let db = loaded.get("db").unwrap();
    assert_eq!(db.consecutive_passes, 0);
    assert_eq!(db.consecutive_failures, 3);
}

#[test]
fn phase1_validation_allocator_state_persistence_round_trip() {
    let store = StateStore::in_memory().unwrap();

    let ports = vec![
        AllocatorPortLease {
            target: ServiceReplicaKey::first("web").unwrap(),
            ports: vec![PublishedPort {
                host_port: 8080,
                container_port: 80,
                protocol: "tcp".to_string(),
            }],
        },
        AllocatorPortLease {
            target: ServiceReplicaKey::first("db").unwrap(),
            ports: vec![PublishedPort {
                host_port: 5432,
                container_port: 5432,
                protocol: "tcp".to_string(),
            }],
        },
    ];

    let service_ips = vec![
        AllocatorIpLease {
            target: ServiceReplicaKey::first("web").unwrap(),
            ip: "10.0.0.2".to_string(),
        },
        AllocatorIpLease {
            target: ServiceReplicaKey::first("db").unwrap(),
            ip: "10.0.0.3".to_string(),
        },
    ];

    let mut mount_tag_offsets = HashMap::new();
    mount_tag_offsets.insert("web".to_string(), 0);
    mount_tag_offsets.insert("db".to_string(), 3);

    let snapshot = AllocatorSnapshot {
        schema_version: 2,
        ports: ports.clone(),
        service_ips: service_ips.clone(),
        service_network_ips: vec![],
        mount_tag_offsets: mount_tag_offsets.clone(),
    };

    store.save_allocator_state("myapp", &snapshot).unwrap();

    // Reload and verify all fields.
    let loaded = store.load_allocator_state("myapp").unwrap().unwrap();
    assert_eq!(loaded.ports, ports);
    assert_eq!(loaded.service_ips, service_ips);
    assert_eq!(loaded.mount_tag_offsets, mount_tag_offsets);

    // Verify specific port allocations survived.
    let web_ports = &loaded
        .ports
        .iter()
        .find(|lease| lease.target == ServiceReplicaKey::first("web").unwrap())
        .unwrap()
        .ports;
    assert_eq!(web_ports.len(), 1);
    assert_eq!(web_ports[0].host_port, 8080);
    assert_eq!(web_ports[0].container_port, 80);

    // Verify IPs survived.
    assert!(loaded.service_ips.iter().any(|lease| {
        lease.target == ServiceReplicaKey::first("web").unwrap() && lease.ip == "10.0.0.2"
    }));
    assert!(loaded.service_ips.iter().any(|lease| {
        lease.target == ServiceReplicaKey::first("db").unwrap() && lease.ip == "10.0.0.3"
    }));

    // Verify mount tag offsets survived.
    assert_eq!(loaded.mount_tag_offsets.get("web"), Some(&0));
    assert_eq!(loaded.mount_tag_offsets.get("db"), Some(&3));
}

#[test]
fn phase1_validation_reconcile_session_lifecycle() {
    let store = StateStore::in_memory().unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
    ];

    let session = ReconcileSession {
        session_id: "rs-1000".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-1".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 2,
        started_at: now,
        updated_at: now,
        completed_at: None,
    };

    // Create session.
    store.create_reconcile_session(&session, &actions).unwrap();

    // Load active session.
    let loaded = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(loaded.session_id, "rs-1000");
    assert_eq!(loaded.status, ReconcileSessionStatus::Active);
    assert_eq!(loaded.next_action_index, 0);

    // Update progress.
    store
        .update_reconcile_session_progress("rs-1000", 1, &ReconcileSessionStatus::Active)
        .unwrap();

    let updated = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(updated.next_action_index, 1);
    assert_eq!(updated.status, ReconcileSessionStatus::Active);

    store
        .update_reconcile_session_progress("rs-1000", 2, &ReconcileSessionStatus::Active)
        .unwrap();

    // Complete session.
    store.complete_reconcile_session("rs-1000").unwrap();

    // Active session should now be gone.
    let none = store.load_active_reconcile_session("myapp").unwrap();
    assert!(none.is_none());

    // List sessions should show completed.
    let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Completed);
    assert!(sessions[0].completed_at.is_some());
}

#[test]
fn phase1_validation_reconcile_session_supersession() {
    let store = StateStore::in_memory().unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    // Create first session.
    let session1 = ReconcileSession {
        session_id: "rs-first".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-1".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 1,
        started_at: now,
        updated_at: now,
        completed_at: None,
    };
    store.create_reconcile_session(&session1, &actions).unwrap();

    // Supersede active sessions for the stack.
    let superseded_count = store.supersede_active_sessions("myapp").unwrap();
    assert_eq!(superseded_count, 1);

    // Old session should be superseded.
    let old_active = store.load_active_reconcile_session("myapp").unwrap();
    assert!(old_active.is_none());

    let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Superseded);

    // Create new session for same stack.
    let session2 = ReconcileSession {
        session_id: "rs-second".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-2".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 1,
        started_at: now + 1,
        updated_at: now + 1,
        completed_at: None,
    };
    store.create_reconcile_session(&session2, &actions).unwrap();

    // New session is active.
    let active = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(active.session_id, "rs-second");
}

#[test]
fn phase1_validation_event_cursor_coherence_after_simulated_restart() {
    let store = StateStore::in_memory().unwrap();

    // Emit a batch of events (simulating pre-restart state).
    let events_batch1 = vec![
        StackEvent::StackApplyStarted {
            stack_name: "myapp".to_string(),
            services_count: 2,
        },
        StackEvent::ServiceCreating {
            stack_name: "myapp".to_string(),
            service_name: "web".to_string(),
        },
        StackEvent::ServiceReady {
            stack_name: "myapp".to_string(),
            service_name: "web".to_string(),
            runtime_id: "ctr-1".to_string(),
        },
    ];

    for event in &events_batch1 {
        store.emit_event("myapp", event).unwrap();
    }

    // Record the cursor (simulating what a consumer would save before restart).
    let all_records = store.load_event_records("myapp").unwrap();
    assert_eq!(all_records.len(), 3);
    let cursor = all_records[1].id; // After ServiceCreating

    // Emit more events (simulating post-restart activity).
    let events_batch2 = vec![
        StackEvent::ServiceCreating {
            stack_name: "myapp".to_string(),
            service_name: "db".to_string(),
        },
        StackEvent::ServiceReady {
            stack_name: "myapp".to_string(),
            service_name: "db".to_string(),
            runtime_id: "ctr-2".to_string(),
        },
        StackEvent::StackApplyCompleted {
            stack_name: "myapp".to_string(),
            succeeded: 2,
            failed: 0,
        },
    ];

    for event in &events_batch2 {
        store.emit_event("myapp", event).unwrap();
    }

    // Load events since cursor (simulating restart recovery).
    let since_cursor = store.load_events_since("myapp", cursor).unwrap();

    // Should get: ServiceReady(web), ServiceCreating(db), ServiceReady(db), StackApplyCompleted
    assert_eq!(since_cursor.len(), 4);

    // Verify ordering: IDs must be strictly monotonically increasing.
    for window in since_cursor.windows(2) {
        assert!(
            window[1].id > window[0].id,
            "event IDs must be monotonically increasing"
        );
    }

    // All events since cursor must have id > cursor.
    for record in &since_cursor {
        assert!(record.id > cursor);
    }

    // Verify completeness: total events = batch1 + batch2.
    let total = store.load_event_records("myapp").unwrap();
    assert_eq!(total.len(), 6);

    // Verify cursor-based loading gives exact complement.
    let from_start = store.load_events_since("myapp", 0).unwrap();
    assert_eq!(from_start.len(), 6);
}

// ── Phase 2: Schema/version migration tests (from agent-a80ffa89) ──

#[test]
fn phase2_control_metadata_crud() {
    let store = StateStore::in_memory().unwrap();

    // Read non-existent key.
    assert!(store.get_control_metadata("nonexistent").unwrap().is_none());

    // Set and read.
    store
        .set_control_metadata("test_key", "test_value")
        .unwrap();
    let value = store.get_control_metadata("test_key").unwrap().unwrap();
    assert_eq!(value, "test_value");

    // Update (upsert).
    store
        .set_control_metadata("test_key", "updated_value")
        .unwrap();
    let value = store.get_control_metadata("test_key").unwrap().unwrap();
    assert_eq!(value, "updated_value");
}

#[test]
fn phase2_schema_version_defaults_to_current() {
    let store = StateStore::in_memory().unwrap();
    let version = store.schema_version().unwrap();
    assert_eq!(version, 9);
}

#[test]
fn phase2_schema_version_set_and_get() {
    let store = StateStore::in_memory().unwrap();

    store.set_schema_version(2).unwrap();
    assert_eq!(store.schema_version().unwrap(), 2);

    store.set_schema_version(42).unwrap();
    assert_eq!(store.schema_version().unwrap(), 42);
}

#[test]
fn phase2_created_at_metadata_set_on_init() {
    let store = StateStore::in_memory().unwrap();
    let created_at = store.get_control_metadata("created_at").unwrap();
    assert!(created_at.is_some());
    // Should be a parseable integer.
    let secs: u64 = created_at.unwrap().parse().unwrap();
    assert!(secs > 0);
}

#[test]
fn phase2_multiple_metadata_keys_independent() {
    let store = StateStore::in_memory().unwrap();

    store.set_control_metadata("key_a", "value_a").unwrap();
    store.set_control_metadata("key_b", "value_b").unwrap();

    assert_eq!(
        store.get_control_metadata("key_a").unwrap().unwrap(),
        "value_a"
    );
    assert_eq!(
        store.get_control_metadata("key_b").unwrap().unwrap(),
        "value_b"
    );

    // Updating one doesn't affect the other.
    store.set_control_metadata("key_a", "new_a").unwrap();
    assert_eq!(
        store.get_control_metadata("key_a").unwrap().unwrap(),
        "new_a"
    );
    assert_eq!(
        store.get_control_metadata("key_b").unwrap().unwrap(),
        "value_b"
    );
}

// ── Phase 3: Startup drift verification tests (from agent-a80ffa89) ──

#[test]
fn phase3_drift_desired_without_observed() {
    let store = StateStore::in_memory().unwrap();

    // Save desired state but no observed state.
    store.save_desired_state("myapp", &sample_spec()).unwrap();

    let findings = store.verify_startup_drift("myapp").unwrap();
    assert!(
        findings.iter().any(
            |f| f.category == "desired_state" && f.description.contains("without observations")
        ),
        "expected desired_state drift finding, got: {findings:?}"
    );
}

#[test]
fn phase3_drift_orphaned_observed_state() {
    let store = StateStore::in_memory().unwrap();

    // Save desired state with only "web" service.
    let mut spec = sample_spec();
    spec.services.retain(|s| s.name == "web");
    store.save_desired_state("myapp", &spec).unwrap();

    // Save observed state for "web" (expected) and "cache" (orphaned).
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-1".to_string()),
                failed_create_ownership: None,
                last_error: None,
                ready: true,
            },
        )
        .unwrap();
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("cache".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-2".to_string()),
                failed_create_ownership: None,
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let findings = store.verify_startup_drift("myapp").unwrap();
    let orphaned: Vec<_> = findings
        .iter()
        .filter(|f| f.category == "observed_state" && f.description.contains("cache"))
        .collect();
    assert_eq!(orphaned.len(), 1);
    assert!(matches!(orphaned[0].severity, DriftSeverity::Warning));
}

#[test]
fn phase3_drift_stale_reconcile_session() {
    let store = StateStore::in_memory().unwrap();

    // Create an active session with updated_at far in the past (> 5 min ago).
    let old_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 600; // 10 minutes ago

    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];

    let session = ReconcileSession {
        session_id: "rs-stale".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-stale".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 1,
        started_at: old_time,
        updated_at: old_time,
        completed_at: None,
    };
    store.create_reconcile_session(&session, &actions).unwrap();

    let findings = store.verify_startup_drift("myapp").unwrap();
    let stale: Vec<_> = findings
        .iter()
        .filter(|f| f.category == "reconcile" && f.description.contains("stale"))
        .collect();
    assert_eq!(stale.len(), 1);
    assert!(matches!(stale[0].severity, DriftSeverity::Warning));
}

#[test]
fn phase3_drift_orphaned_health_state() {
    let store = StateStore::in_memory().unwrap();

    // Save health state but no desired state.
    let mut health = HashMap::new();
    health.insert(
        "web".to_string(),
        HealthPollState {
            service_name: "web".to_string(),
            consecutive_passes: 1,
            consecutive_failures: 0,
            last_check_millis: Some(1_700_000_000_000),
            start_time_millis: None,
        },
    );
    store.save_health_poller_state("myapp", &health).unwrap();

    let findings = store.verify_startup_drift("myapp").unwrap();
    let orphaned: Vec<_> = findings
        .iter()
        .filter(|f| f.category == "health" && f.description.contains("orphaned"))
        .collect();
    assert_eq!(orphaned.len(), 1);
    assert!(matches!(orphaned[0].severity, DriftSeverity::Info));
}

#[test]
fn phase3_drift_clean_state_returns_no_findings() {
    let store = StateStore::in_memory().unwrap();

    // Save desired state with matching observed state.
    store.save_desired_state("myapp", &sample_spec()).unwrap();
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-1".to_string()),
                failed_create_ownership: None,
                last_error: None,
                ready: true,
            },
        )
        .unwrap();
    store
        .save_observed_state(
            "myapp",
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Running,
                container_id: Some("ctr-2".to_string()),
                failed_create_ownership: None,
                last_error: None,
                ready: true,
            },
        )
        .unwrap();

    let findings = store.verify_startup_drift("myapp").unwrap();
    assert!(
        findings.is_empty(),
        "expected no drift findings in clean state, got: {findings:?}"
    );
}

#[test]
fn phase3_drift_nonexistent_stack_returns_no_findings() {
    let store = StateStore::in_memory().unwrap();
    let findings = store.verify_startup_drift("nonexistent").unwrap();
    assert!(findings.is_empty());
}

#[test]
fn phase3_drift_finding_serialization_round_trip() {
    let finding = DriftFinding {
        category: "observed_state".to_string(),
        description: "orphaned service".to_string(),
        severity: DriftSeverity::Warning,
    };

    let json = serde_json::to_string(&finding).unwrap();
    let loaded: DriftFinding = serde_json::from_str(&json).unwrap();
    assert_eq!(loaded.category, "observed_state");
    assert_eq!(loaded.description, "orphaned service");
    assert!(matches!(loaded.severity, DriftSeverity::Warning));
}

#[test]
fn phase3_drift_event_emission() {
    let store = StateStore::in_memory().unwrap();

    // Create a drift finding and emit as event.
    let finding = DriftFinding {
        category: "desired_state".to_string(),
        description: "desired state without observations".to_string(),
        severity: DriftSeverity::Warning,
    };

    let event = StackEvent::DriftDetected {
        stack_name: "myapp".to_string(),
        category: finding.category.clone(),
        description: finding.description.clone(),
        severity: finding.severity.as_str().to_string(),
    };

    store.emit_event("myapp", &event).unwrap();

    let events = store.load_events("myapp").unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], StackEvent::DriftDetected { .. }));
}

// ── Part 1: Audit log CRUD tests (vz-v2n.3.1) ──

fn make_audit_entry(
    session_id: &str,
    stack_name: &str,
    action_index: usize,
    action_kind: &str,
    service_name: &str,
) -> ReconcileAuditEntry {
    let target = ServiceReplicaKey::first(service_name).unwrap();
    let action = match action_kind {
        "service_create" => Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack(stack_name),
            target: target.clone(),
        },
        "service_recreate" => Action::ServiceRecreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: target.clone(),
        },
        "service_remove" => Action::ServiceRemove {
            precondition: crate::reconcile::test_replica_precondition(),
            target: target.clone(),
        },
        other => panic!("unsupported audit test action kind {other}"),
    };
    ReconcileAuditEntry {
        id: 0, // auto-generated on insert
        session_id: session_id.to_string(),
        stack_name: stack_name.to_string(),
        action_index,
        action_kind: action_kind.to_string(),
        target,
        action_hash: crate::reconcile::compute_actions_hash(&[action]),
        status: "started".to_string(),
        started_at: 1_700_000_000 + action_index as u64,
        completed_at: None,
        error_message: None,
    }
}

fn save_audit_session(store: &StateStore, id: &str, stack: &str, actions: &[Action]) {
    let session = ReconcileSession {
        session_id: id.to_string(),
        stack_name: stack.to_string(),
        operation_id: format!("op-{id}"),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store.create_reconcile_session(&session, actions).unwrap();
}

#[test]
fn audit_log_start_and_load() {
    let store = StateStore::in_memory().unwrap();

    let entry = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
    save_audit_session(
        &store,
        "sess-1",
        "myapp",
        &[Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: entry.target.clone(),
        }],
    );
    let id = store.log_reconcile_action_start(&entry).unwrap();
    assert!(id > 0);

    let log = store.load_audit_log_for_session("sess-1").unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].session_id, "sess-1");
    assert_eq!(log[0].action_kind, "service_create");
    assert_eq!(log[0].target.service_name, "web");
    assert_eq!(log[0].status, "started");
    assert!(log[0].completed_at.is_none());
    assert!(log[0].error_message.is_none());
}

#[test]
fn audit_log_complete_success() {
    let store = StateStore::in_memory().unwrap();

    let entry = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
    save_audit_session(
        &store,
        "sess-1",
        "myapp",
        &[Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: entry.target.clone(),
        }],
    );
    let id = store.log_reconcile_action_start(&entry).unwrap();
    store.log_reconcile_action_complete(id, None).unwrap();

    let log = store.load_audit_log_for_session("sess-1").unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].status, "completed");
    assert!(log[0].completed_at.is_some());
    assert!(log[0].error_message.is_none());
}

#[test]
fn audit_log_complete_failure() {
    let store = StateStore::in_memory().unwrap();

    let entry = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
    save_audit_session(
        &store,
        "sess-1",
        "myapp",
        &[Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: entry.target.clone(),
        }],
    );
    let id = store.log_reconcile_action_start(&entry).unwrap();
    store
        .log_reconcile_action_complete(id, Some("container start failed"))
        .unwrap();

    let log = store.load_audit_log_for_session("sess-1").unwrap();
    assert_eq!(log[0].status, "failed");
    assert!(log[0].completed_at.is_some());
    assert_eq!(
        log[0].error_message.as_deref(),
        Some("container start failed")
    );
}

#[test]
fn audit_log_multiple_entries_ordered_by_action_index() {
    let store = StateStore::in_memory().unwrap();

    // Insert out of order to verify ORDER BY
    let e2 = make_audit_entry("sess-1", "myapp", 2, "service_remove", "cache");
    let e0 = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
    let e1 = make_audit_entry("sess-1", "myapp", 1, "service_create", "db");

    save_audit_session(
        &store,
        "sess-1",
        "myapp",
        &[
            Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: e0.target.clone(),
            },
            Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: e1.target.clone(),
            },
            Action::ServiceRemove {
                precondition: crate::reconcile::test_replica_precondition(),
                target: e2.target.clone(),
            },
        ],
    );

    store.log_reconcile_action_start(&e2).unwrap();
    store.log_reconcile_action_start(&e0).unwrap();
    store.log_reconcile_action_start(&e1).unwrap();

    let log = store.load_audit_log_for_session("sess-1").unwrap();
    assert_eq!(log.len(), 3);
    assert_eq!(log[0].action_index, 0);
    assert_eq!(log[1].action_index, 1);
    assert_eq!(log[2].action_index, 2);
}

#[test]
fn audit_log_scoped_by_session() {
    let store = StateStore::in_memory().unwrap();

    let e1 = make_audit_entry("sess-1", "myapp", 0, "service_create", "web");
    let e2 = make_audit_entry("sess-2", "myapp", 0, "service_create", "api");

    save_audit_session(
        &store,
        "sess-1",
        "myapp",
        &[Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: e1.target.clone(),
        }],
    );
    save_audit_session(
        &store,
        "sess-2",
        "myapp",
        &[Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: e2.target.clone(),
        }],
    );

    store.log_reconcile_action_start(&e1).unwrap();
    store.log_reconcile_action_start(&e2).unwrap();

    let log1 = store.load_audit_log_for_session("sess-1").unwrap();
    assert_eq!(log1.len(), 1);
    assert_eq!(log1[0].target.service_name, "web");

    let log2 = store.load_audit_log_for_session("sess-2").unwrap();
    assert_eq!(log2.len(), 1);
    assert_eq!(log2[0].target.service_name, "api");
}

#[test]
fn audit_log_recent_by_stack() {
    let store = StateStore::in_memory().unwrap();

    for i in 0..5 {
        let entry = make_audit_entry(
            &format!("sess-{i}"),
            "myapp",
            0,
            "service_create",
            &format!("svc-{i}"),
        );
        save_audit_session(
            &store,
            &format!("sess-{i}"),
            "myapp",
            &[Action::ServiceCreate {
                precondition: crate::reconcile::test_replica_precondition(),
                target: entry.target.clone(),
            }],
        );
        store.log_reconcile_action_start(&entry).unwrap();
    }

    // Other stack should not appear
    let other = make_audit_entry("sess-other", "otherapp", 0, "service_create", "web");
    save_audit_session(
        &store,
        "sess-other",
        "otherapp",
        &[Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("otherapp"),
            target: other.target.clone(),
        }],
    );
    store.log_reconcile_action_start(&other).unwrap();

    let recent = store.load_recent_audit_log("myapp", 3).unwrap();
    assert_eq!(recent.len(), 3);
    // Newest first (DESC)
    assert!(recent[0].id > recent[1].id);
    assert!(recent[1].id > recent[2].id);
}

#[test]
fn audit_log_empty_session_returns_empty() {
    let store = StateStore::in_memory().unwrap();
    let log = store.load_audit_log_for_session("nonexistent").unwrap();
    assert!(log.is_empty());
}

// ── Part 2: Recovery fault-injection tests (vz-v2n.3.2) ──

#[test]
fn recovery_crash_during_apply_actions_partially_persisted() {
    let store = StateStore::in_memory().unwrap();

    // Create session with 3 actions
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("cache".to_string()).unwrap(),
        },
    ];
    let session = ReconcileSession {
        session_id: "rs-crash-1".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-1".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 3,
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store.create_reconcile_session(&session, &actions).unwrap();

    // Action 0: started + completed
    let e0 = make_audit_entry("rs-crash-1", "myapp", 0, "service_create", "web");
    let id0 = store.log_reconcile_action_start(&e0).unwrap();
    store.log_reconcile_action_complete(id0, None).unwrap();

    // Action 1: started + completed
    let e1 = make_audit_entry("rs-crash-1", "myapp", 1, "service_create", "db");
    let id1 = store.log_reconcile_action_start(&e1).unwrap();
    store.log_reconcile_action_complete(id1, None).unwrap();

    // Action 2: started but NOT completed (crash simulation)
    let e2 = make_audit_entry("rs-crash-1", "myapp", 2, "service_create", "cache");
    store.log_reconcile_action_start(&e2).unwrap();

    // Update progress to reflect that we were partway through
    store
        .update_reconcile_session_progress("rs-crash-1", 2, &ReconcileSessionStatus::Active)
        .unwrap();

    // Verify: session is still active (crash recovery)
    let active = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(active.session_id, "rs-crash-1");
    assert_eq!(active.status, ReconcileSessionStatus::Active);

    // Verify: audit log shows 2 completed, 1 started
    let log = store.load_audit_log_for_session("rs-crash-1").unwrap();
    assert_eq!(log.len(), 3);
    assert_eq!(log[0].status, "completed");
    assert_eq!(log[1].status, "completed");
    assert_eq!(log[2].status, "started"); // crash point

    // Verify: next_action_index points to the right place
    assert_eq!(active.next_action_index, 2);
}

#[test]
fn recovery_restart_with_partial_batch_resumes_from_cursor() {
    let store = StateStore::in_memory().unwrap();

    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("cache".to_string()).unwrap(),
        },
    ];
    let session = ReconcileSession {
        session_id: "rs-resume-1".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-2".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 3,
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store.create_reconcile_session(&session, &actions).unwrap();

    // Complete action 0, advance cursor
    let e0 = make_audit_entry("rs-resume-1", "myapp", 0, "service_create", "web");
    let id0 = store.log_reconcile_action_start(&e0).unwrap();
    store.log_reconcile_action_complete(id0, None).unwrap();
    store
        .update_reconcile_session_progress("rs-resume-1", 1, &ReconcileSessionStatus::Active)
        .unwrap();

    // Simulate restart: load active session
    let resumed = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(resumed.next_action_index, 1);
    assert_eq!(resumed.total_actions, 3);

    // Verify remaining actions via audit log
    let log = store.load_audit_log_for_session("rs-resume-1").unwrap();
    let completed_count = log.iter().filter(|e| e.status == "completed").count();
    assert_eq!(completed_count, 1);
    // Remaining = total - cursor
    let remaining = resumed.total_actions - resumed.next_action_index;
    assert_eq!(remaining, 2);
}

#[test]
fn recovery_crash_during_health_polling_state_preserved() {
    let store = StateStore::in_memory().unwrap();

    let mut health_state = HashMap::new();
    health_state.insert(
        "web".to_string(),
        HealthPollState {
            service_name: "web".to_string(),
            consecutive_passes: 3,
            consecutive_failures: 0,
            last_check_millis: Some(1_700_000_000_000),
            start_time_millis: Some(1_700_000_000_100),
        },
    );
    health_state.insert(
        "db".to_string(),
        HealthPollState {
            service_name: "db".to_string(),
            consecutive_passes: 1,
            consecutive_failures: 2,
            last_check_millis: Some(1_700_000_000_500),
            start_time_millis: Some(1_700_000_000_200),
        },
    );
    store
        .save_health_poller_state("myapp", &health_state)
        .unwrap();

    // Simulate crash: just reload from store (in-memory is still there)
    let restored = store.load_health_poller_state("myapp").unwrap();
    assert_eq!(restored.len(), 2);
    let web = restored.get("web").unwrap();
    assert_eq!(web.consecutive_passes, 3);
    assert_eq!(web.consecutive_failures, 0);
    let db = restored.get("db").unwrap();
    assert_eq!(db.consecutive_passes, 1);
    assert_eq!(db.consecutive_failures, 2);
}

#[test]
fn recovery_port_conflict_replay_after_restart() {
    let store = StateStore::in_memory().unwrap();

    let ports = vec![
        AllocatorPortLease {
            target: ServiceReplicaKey::first("web").unwrap(),
            ports: vec![PublishedPort {
                host_port: 8080,
                container_port: 80,
                protocol: "tcp".to_string(),
            }],
        },
        AllocatorPortLease {
            target: ServiceReplicaKey::first("api").unwrap(),
            ports: vec![PublishedPort {
                host_port: 3000,
                container_port: 3000,
                protocol: "tcp".to_string(),
            }],
        },
    ];
    let snapshot = AllocatorSnapshot {
        schema_version: 2,
        ports: ports.clone(),
        service_ips: vec![
            AllocatorIpLease {
                target: ServiceReplicaKey::first("web").unwrap(),
                ip: "10.0.0.2".to_string(),
            },
            AllocatorIpLease {
                target: ServiceReplicaKey::first("api").unwrap(),
                ip: "10.0.0.3".to_string(),
            },
        ],
        service_network_ips: vec![],
        mount_tag_offsets: HashMap::from([("web".to_string(), 0), ("api".to_string(), 1)]),
    };
    store.save_allocator_state("myapp", &snapshot).unwrap();

    // Simulate restart: reload
    let restored = store.load_allocator_state("myapp").unwrap().unwrap();
    assert_eq!(restored.ports, snapshot.ports);
    assert_eq!(restored.service_ips, snapshot.service_ips);
    assert_eq!(restored.mount_tag_offsets, snapshot.mount_tag_offsets);
}

#[test]
fn recovery_dependency_blocked_replay_after_restart() {
    let store = StateStore::in_memory().unwrap();

    let spec = StackSpec {
        name: "myapp".to_string(),
        services: vec![
            ServiceSpec {
                name: "web".to_string(),
                kind: ServiceKind::Service,
                image: "nginx:latest".to_string(),
                depends_on: vec![crate::spec::ServiceDependency {
                    service: "db".to_string(),
                    condition: crate::spec::DependencyCondition::ServiceHealthy,
                }],
                command: None,
                entrypoint: None,
                environment: HashMap::new(),
                working_dir: None,
                user: None,
                mounts: vec![],
                ports: vec![],
                healthcheck: None,
                restart_policy: None,
                resources: Default::default(),
                extra_hosts: vec![],
                secrets: vec![],
                networks: vec![],
                cap_add: vec![],
                cap_drop: vec![],
                privileged: false,
                read_only: false,
                sysctls: HashMap::new(),
                ulimits: vec![],
                container_name: None,
                hostname: None,
                domainname: None,
                labels: HashMap::new(),
                stop_signal: None,
                stop_grace_period_secs: None,
                expose: vec![],
                stdin_open: false,
                tty: false,
                logging: None,
            },
            ServiceSpec {
                name: "db".to_string(),
                kind: ServiceKind::Service,
                image: "postgres:16".to_string(),
                depends_on: vec![],
                command: None,
                entrypoint: None,
                environment: HashMap::new(),
                working_dir: None,
                user: None,
                mounts: vec![],
                ports: vec![],
                healthcheck: None,
                restart_policy: None,
                resources: Default::default(),
                extra_hosts: vec![],
                secrets: vec![],
                networks: vec![],
                cap_add: vec![],
                cap_drop: vec![],
                privileged: false,
                read_only: false,
                sysctls: HashMap::new(),
                ulimits: vec![],
                container_name: None,
                hostname: None,
                domainname: None,
                labels: HashMap::new(),
                stop_signal: None,
                stop_grace_period_secs: None,
                expose: vec![],
                stdin_open: false,
                tty: false,
                logging: None,
            },
        ],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    store.save_desired_state("myapp", &spec).unwrap();

    // Simulate restart: reload desired state and verify dependencies
    let restored = store.load_desired_state("myapp").unwrap().unwrap();
    assert_eq!(restored.services.len(), 2);
    let web = restored.services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(web.depends_on.len(), 1);
    assert_eq!(web.depends_on[0].service, "db");
    assert_eq!(
        web.depends_on[0].condition,
        crate::spec::DependencyCondition::ServiceHealthy
    );
}

#[test]
fn recovery_superseded_session_cleanup() {
    let store = StateStore::in_memory().unwrap();

    // First session
    let actions1 = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    let session1 = ReconcileSession {
        session_id: "rs-old-1".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-old".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::compute_actions_hash(&actions1),
        next_action_index: 0,
        total_actions: 1,
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store
        .create_reconcile_session(&session1, &actions1)
        .unwrap();

    // Audit entries for old session
    let e_old = make_audit_entry("rs-old-1", "myapp", 0, "service_create", "web");
    let old_audit_id = store.log_reconcile_action_start(&e_old).unwrap();

    // A durable started claim prevents unsafe supersession.
    let superseded_count = store.supersede_active_sessions("myapp").unwrap();
    assert_eq!(superseded_count, 0);
    store
        .log_reconcile_action_complete(old_audit_id, None)
        .unwrap();

    // Once no started claim remains, the legacy cleanup helper may supersede.
    let superseded_count = store.supersede_active_sessions("myapp").unwrap();
    assert_eq!(superseded_count, 1);

    // Create new session
    let actions2 = vec![Action::ServiceRecreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    let session2 = ReconcileSession {
        session_id: "rs-new-1".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-new".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::compute_actions_hash(&actions2),
        next_action_index: 0,
        total_actions: 1,
        started_at: 1_700_001_000,
        updated_at: 1_700_001_000,
        completed_at: None,
    };
    store
        .create_reconcile_session(&session2, &actions2)
        .unwrap();

    // Verify old audit entries are still queryable
    let old_log = store.load_audit_log_for_session("rs-old-1").unwrap();
    assert_eq!(old_log.len(), 1);
    assert_eq!(old_log[0].target.service_name, "web");

    // Verify only new session is active
    let active = store
        .load_active_reconcile_session("myapp")
        .unwrap()
        .unwrap();
    assert_eq!(active.session_id, "rs-new-1");

    // Verify old session is superseded
    let all_sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(all_sessions.len(), 2);
    let old_sess = all_sessions
        .iter()
        .find(|s| s.session_id == "rs-old-1")
        .unwrap();
    assert_eq!(old_sess.status, ReconcileSessionStatus::Superseded);
}

// ── Part 3: Phase 3 recovery proof validation (vz-v2n.3.3) ──

#[test]
fn phase3_validation_full_recovery_lifecycle() {
    let store = StateStore::in_memory().unwrap();

    // 1. Create stack with desired state
    let spec = sample_spec();
    store.save_desired_state("myapp", &spec).unwrap();

    // 2. Create reconcile session with 3 actions
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("db".to_string()).unwrap(),
        },
        Action::ServiceRemove {
            precondition: crate::reconcile::test_replica_precondition(),
            target: crate::state_store::ServiceReplicaKey::first("old-svc".to_string()).unwrap(),
        },
    ];
    let session = ReconcileSession {
        session_id: "rs-full-1".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-full".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 3,
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store.create_reconcile_session(&session, &actions).unwrap();

    // 3. Log action starts and completions with audit entries
    for (idx, action) in actions.iter().enumerate() {
        let kind = match action {
            Action::ServiceCreate { .. } => "service_create",
            Action::ServiceRecreate { .. } => "service_recreate",
            Action::ServiceRemove { .. } => "service_remove",
        };
        let entry = make_audit_entry("rs-full-1", "myapp", idx, kind, action.service_name());
        let id = store.log_reconcile_action_start(&entry).unwrap();
        store.log_reconcile_action_complete(id, None).unwrap();
        store
            .update_reconcile_session_progress(
                "rs-full-1",
                idx + 1,
                &ReconcileSessionStatus::Active,
            )
            .unwrap();
    }

    // 4. Mark session completed
    store.complete_reconcile_session("rs-full-1").unwrap();

    // 5. Verify: audit log is complete and ordered
    let log = store.load_audit_log_for_session("rs-full-1").unwrap();
    assert_eq!(log.len(), 3);
    for (idx, entry) in log.iter().enumerate() {
        assert_eq!(entry.action_index, idx);
        assert_eq!(entry.status, "completed");
        assert!(entry.completed_at.is_some());
    }
    assert_eq!(log[0].action_kind, "service_create");
    assert_eq!(log[0].target.service_name, "web");
    assert_eq!(log[1].action_kind, "service_create");
    assert_eq!(log[1].target.service_name, "db");
    assert_eq!(log[2].action_kind, "service_remove");
    assert_eq!(log[2].target.service_name, "old-svc");

    // 6. Verify: session has correct completed_at
    let sessions = store.list_reconcile_sessions("myapp", 10).unwrap();
    let completed_sess = sessions
        .iter()
        .find(|s| s.session_id == "rs-full-1")
        .unwrap();
    assert_eq!(completed_sess.status, ReconcileSessionStatus::Completed);
    assert!(completed_sess.completed_at.is_some());

    // 7. Create second session (simulating next apply)
    store.supersede_active_sessions("myapp").unwrap(); // no-op: already completed
    let actions2 = vec![Action::ServiceRecreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
    }];
    let session2 = ReconcileSession {
        session_id: "rs-full-2".to_string(),
        stack_name: "myapp".to_string(),
        operation_id: "op-full-2".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::compute_actions_hash(&actions2),
        next_action_index: 0,
        total_actions: 1,
        started_at: 1_700_001_000,
        updated_at: 1_700_001_000,
        completed_at: None,
    };
    store
        .create_reconcile_session(&session2, &actions2)
        .unwrap();

    // 8. Verify: old session is completed (not superseded since it was already done),
    //    new session is active
    let all = store.list_reconcile_sessions("myapp", 10).unwrap();
    assert_eq!(all.len(), 2);
    let old = all.iter().find(|s| s.session_id == "rs-full-1").unwrap();
    assert_eq!(old.status, ReconcileSessionStatus::Completed);
    let new = all.iter().find(|s| s.session_id == "rs-full-2").unwrap();
    assert_eq!(new.status, ReconcileSessionStatus::Active);

    // 9. Verify: drift check returns clean for correct state
    //    Save observed state matching desired state
    for svc in &spec.services {
        store
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    replica: crate::state_store::ServiceReplicaKey::first(svc.name.clone())
                        .unwrap(),
                    applied_config_digest: None,
                    phase: ServicePhase::Running,
                    container_id: Some(format!("ctr-{}", svc.name)),
                    failed_create_ownership: None,
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();
    }
    let findings = store.verify_startup_drift("myapp").unwrap();
    // The only finding should be about the active session (if stale).
    // Since the new session was just created, no stale session warning.
    // Both desired services have observed state, so no orphan warnings.
    let non_stale: Vec<_> = findings
        .iter()
        .filter(|f| f.category != "reconcile")
        .collect();
    assert!(
        non_stale.is_empty(),
        "unexpected drift findings: {non_stale:?}"
    );
}

// ── Part 4: Phase 2 schema/drift validation (vz-v2n.2.3) ──

#[test]
fn phase2_validation_schema_version_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test-state.db");

    {
        let store = StateStore::open(&db_path).unwrap();
        store.set_schema_version(9).unwrap();
        assert_eq!(store.schema_version().unwrap(), 9);
    }
    // Drop store (close connection), reopen
    {
        let store = StateStore::open(&db_path).unwrap();
        assert_eq!(store.schema_version().unwrap(), 9);
    }
}

#[test]
fn phase2_validation_drift_desired_without_observed() {
    let store = StateStore::in_memory().unwrap();

    // Save desired state, don't save observed state
    let spec = sample_spec();
    store.save_desired_state("myapp", &spec).unwrap();

    let findings = store.verify_startup_drift("myapp").unwrap();
    let desired_drift: Vec<_> = findings
        .iter()
        .filter(|f| f.category == "desired_state")
        .collect();
    assert_eq!(desired_drift.len(), 1);
    assert!(
        desired_drift[0]
            .description
            .contains("desired state without observations")
    );
    assert_eq!(desired_drift[0].severity, DriftSeverity::Warning);
}

#[test]
fn phase2_validation_drift_orphaned_observed() {
    let store = StateStore::in_memory().unwrap();

    // Save desired state (only "web" and "db")
    let spec = sample_spec();
    store.save_desired_state("myapp", &spec).unwrap();

    // Save observed state for services including one not in desired state
    for name in &["web", "db", "orphaned-svc"] {
        store
            .save_observed_state(
                "myapp",
                &ServiceObservedState {
                    replica: crate::state_store::ServiceReplicaKey::first(name.to_string())
                        .unwrap(),
                    applied_config_digest: None,
                    phase: ServicePhase::Running,
                    container_id: Some(format!("ctr-{name}")),
                    failed_create_ownership: None,
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();
    }

    let findings = store.verify_startup_drift("myapp").unwrap();
    let orphaned: Vec<_> = findings
        .iter()
        .filter(|f| f.category == "observed_state")
        .collect();
    assert_eq!(orphaned.len(), 1);
    assert!(
        orphaned[0]
            .description
            .contains("orphaned observed state for service 'orphaned-svc'")
    );
    assert_eq!(orphaned[0].severity, DriftSeverity::Warning);
}

#[test]
fn phase2_validation_event_queries_after_migration() {
    let store = StateStore::in_memory().unwrap();

    // Emit events
    store
        .emit_event(
            "myapp",
            &StackEvent::StackApplyStarted {
                stack_name: "myapp".to_string(),
                services_count: 2,
            },
        )
        .unwrap();
    store
        .emit_event(
            "myapp",
            &StackEvent::ServiceCreating {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
        )
        .unwrap();

    // Verify load_events works
    let events = store.load_events("myapp").unwrap();
    assert_eq!(events.len(), 2);

    // Verify load_events_since works
    let records = store.load_event_records("myapp").unwrap();
    let since = store.load_events_since("myapp", records[0].id).unwrap();
    assert_eq!(since.len(), 1);
    assert!(matches!(since[0].event, StackEvent::ServiceCreating { .. }));

    // Set schema version and verify queries still work
    store.set_schema_version(4).unwrap();
    assert_eq!(store.schema_version().unwrap(), 4);

    let events_after = store.load_events("myapp").unwrap();
    assert_eq!(events_after.len(), 2);

    let since_after = store.load_events_since("myapp", records[0].id).unwrap();
    assert_eq!(since_after.len(), 1);
}

// ── Capacity and regression tests (vz-lbg) ─────────────────────

fn make_service(name: &str) -> ServiceSpec {
    ServiceSpec {
        name: name.to_string(),
        kind: ServiceKind::Service,
        image: format!("{name}:latest"),
        command: None,
        entrypoint: None,
        environment: HashMap::from([("PORT".to_string(), "80".to_string())]),
        working_dir: None,
        user: None,
        mounts: vec![],
        ports: vec![],
        depends_on: vec![],
        healthcheck: None,
        restart_policy: None,
        resources: Default::default(),
        extra_hosts: vec![],
        secrets: vec![],
        networks: vec![],
        cap_add: vec![],
        cap_drop: vec![],
        privileged: false,
        read_only: false,
        sysctls: HashMap::new(),
        ulimits: vec![],
        container_name: None,
        hostname: None,
        domainname: None,
        labels: HashMap::new(),
        stop_signal: None,
        stop_grace_period_secs: None,
        expose: vec![],
        stdin_open: false,
        tty: false,
        logging: None,
    }
}

/// Insert 10,000 events into a single stack and verify that cursor-based
/// queries remain performant (complete within a generous wall-clock bound).
#[test]
fn capacity_10k_events_query_performance() {
    let store = StateStore::in_memory().unwrap();

    // Insert 10,000 events.
    let start_insert = std::time::Instant::now();
    for i in 0..10_000 {
        store
            .emit_event(
                "perf-app",
                &StackEvent::ServiceCreating {
                    stack_name: "perf-app".to_string(),
                    service_name: format!("svc-{i}"),
                },
            )
            .unwrap();
    }
    let insert_elapsed = start_insert.elapsed();
    // Generous bound: 10,000 inserts should complete within 10 seconds on CI.
    assert!(
        insert_elapsed.as_secs() < 10,
        "10,000 event inserts took {insert_elapsed:?} (>10s budget)"
    );

    // Count should be exact.
    assert_eq!(store.event_count("perf-app").unwrap(), 10_000);

    // Cursor-based query from midpoint should be fast.
    let start_query = std::time::Instant::now();
    let page = store
        .load_events_since_limited("perf-app", 5000, 100)
        .unwrap();
    let query_elapsed = start_query.elapsed();
    assert_eq!(page.len(), 100);
    // Query should complete in well under 1 second.
    assert!(
        query_elapsed.as_millis() < 1000,
        "cursor query after 10k events took {query_elapsed:?} (>1s budget)"
    );

    // Full-table scan should also be bounded.
    let start_all = std::time::Instant::now();
    let _all_records = store.load_event_records("perf-app").unwrap();
    let all_elapsed = start_all.elapsed();
    assert!(
        all_elapsed.as_secs() < 5,
        "full load of 10k event records took {all_elapsed:?} (>5s budget)"
    );
}

/// Verify that 100 concurrent stacks maintain isolation and perform
/// adequately for save/load operations.
#[test]
fn capacity_100_concurrent_stacks_isolation() {
    let store = StateStore::in_memory().unwrap();

    let start = std::time::Instant::now();

    // Create 100 stacks, each with a unique spec.
    for i in 0..100 {
        let name = format!("stack-{i}");
        let spec = StackSpec {
            name: name.clone(),
            services: vec![make_service(&format!("svc-{i}"))],
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
            disk_size_mb: None,
        };
        store.save_desired_state(&name, &spec).unwrap();

        // Emit a couple events per stack.
        store
            .emit_event(
                &name,
                &StackEvent::StackApplyStarted {
                    stack_name: name.clone(),
                    services_count: 1,
                },
            )
            .unwrap();
        store
            .emit_event(
                &name,
                &StackEvent::StackApplyCompleted {
                    stack_name: name.clone(),
                    succeeded: 1,
                    failed: 0,
                },
            )
            .unwrap();

        // Save observed state.
        store
            .save_observed_state(
                &name,
                &ServiceObservedState {
                    replica: crate::state_store::ServiceReplicaKey::first(format!("svc-{i}"))
                        .unwrap(),
                    applied_config_digest: None,
                    phase: ServicePhase::Running,
                    container_id: Some(format!("ctr-{i}")),
                    failed_create_ownership: None,
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();
    }

    let setup_elapsed = start.elapsed();
    assert!(
        setup_elapsed.as_secs() < 10,
        "setting up 100 stacks took {setup_elapsed:?} (>10s budget)"
    );

    // Verify isolation: each stack has its own events.
    for i in 0..100 {
        let name = format!("stack-{i}");
        let events = store.load_events(&name).unwrap();
        assert_eq!(events.len(), 2, "stack-{i} should have exactly 2 events");

        let observed = store.load_observed_state(&name).unwrap();
        assert_eq!(
            observed.len(),
            1,
            "stack-{i} should have exactly 1 observed state"
        );
        assert_eq!(observed[0].replica.service_name, format!("svc-{i}"));
    }

    // Verify load for a random stack in the middle is fast.
    let start_load = std::time::Instant::now();
    let loaded = store.load_desired_state("stack-50").unwrap().unwrap();
    let load_elapsed = start_load.elapsed();
    assert_eq!(loaded.name, "stack-50");
    assert!(
        load_elapsed.as_millis() < 100,
        "loading stack-50 among 100 stacks took {load_elapsed:?} (>100ms budget)"
    );
}

/// Verify that a large desired state (50+ services) round-trips
/// correctly through save/load with acceptable performance.
#[test]
fn capacity_large_desired_state_50_services() {
    let store = StateStore::in_memory().unwrap();

    let services: Vec<ServiceSpec> = (0..50).map(|i| make_service(&format!("svc-{i}"))).collect();
    let spec = StackSpec {
        name: "large-app".to_string(),
        services,
        networks: vec![NetworkSpec {
            name: "default".to_string(),
            driver: "bridge".to_string(),
            subnet: None,
        }],
        volumes: vec![VolumeSpec {
            name: "data".to_string(),
            driver: "local".to_string(),
            driver_opts: None,
        }],
        secrets: vec![],
        disk_size_mb: Some(20480),
    };

    let start = std::time::Instant::now();
    store.save_desired_state("large-app", &spec).unwrap();
    let loaded = store.load_desired_state("large-app").unwrap().unwrap();
    let elapsed = start.elapsed();

    assert_eq!(loaded, spec);
    assert_eq!(loaded.services.len(), 50);
    assert!(
        elapsed.as_millis() < 500,
        "large spec (50 services) save+load took {elapsed:?} (>500ms budget)"
    );

    // Upsert to verify update path is also performant.
    let start_upsert = std::time::Instant::now();
    store.save_desired_state("large-app", &spec).unwrap();
    let upsert_elapsed = start_upsert.elapsed();
    assert!(
        upsert_elapsed.as_millis() < 500,
        "large spec upsert took {upsert_elapsed:?} (>500ms budget)"
    );
}

/// Regression: 1,000 event inserts must complete within 500ms.
#[test]
fn regression_1000_event_inserts_under_500ms() {
    let store = StateStore::in_memory().unwrap();

    let start = std::time::Instant::now();
    for i in 0..1_000 {
        store
            .emit_event(
                "regression-app",
                &StackEvent::ServiceCreating {
                    stack_name: "regression-app".to_string(),
                    service_name: format!("svc-{i}"),
                },
            )
            .unwrap();
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 500,
        "1,000 event inserts took {elapsed:?} — exceeds 500ms regression gate"
    );
}

/// Regression: idempotency key lookup among 500 keys must be under 50ms.
#[test]
fn regression_idempotency_lookup_under_50ms() {
    let store = StateStore::in_memory().unwrap();

    for i in 0..500 {
        let record = IdempotencyRecord {
            key: format!("idem-key-{i}"),
            operation: "create_sandbox".to_string(),
            request_hash: format!("hash-{i}"),
            response_json: r#"{"sandbox_id":"sb-1"}"#.to_string(),
            status_code: 201,
            created_at: 1_700_000_000,
            expires_at: 1_700_000_000 + IDEMPOTENCY_TTL_SECS,
        };
        store.save_idempotency_result(&record).unwrap();
    }

    let start = std::time::Instant::now();
    let result = store.find_idempotency_result("idem-key-250").unwrap();
    let elapsed = start.elapsed();

    assert!(result.is_some());
    assert!(
        elapsed.as_millis() < 50,
        "idempotency lookup among 500 keys took {elapsed:?} — exceeds 50ms regression gate"
    );
}

/// Regression: saving and loading observed state for 20 services
/// must complete within 200ms.
#[test]
fn regression_observed_state_20_services_under_200ms() {
    let store = StateStore::in_memory().unwrap();

    let start = std::time::Instant::now();
    for i in 0..20 {
        let state = ServiceObservedState {
            replica: crate::state_store::ServiceReplicaKey::first(format!("svc-{i}")).unwrap(),
            applied_config_digest: None,
            phase: ServicePhase::Running,
            container_id: Some(format!("ctr-{i}")),
            failed_create_ownership: None,
            last_error: None,
            ready: true,
        };
        store.save_observed_state("regression-app", &state).unwrap();
    }
    let loaded = store.load_observed_state("regression-app").unwrap();
    let elapsed = start.elapsed();

    assert_eq!(loaded.len(), 20);
    assert!(
        elapsed.as_millis() < 200,
        "20 observed state save+load took {elapsed:?} — exceeds 200ms regression gate"
    );
}

// ── Migration compatibility tests (vz-4g0) ──

#[test]
fn v0_3_20_fixture_content_and_checksums_are_stable() {
    assert_eq!(fixture_sha256(V0_3_20_FIXTURE), V0_3_20_FIXTURE_SHA256);
    assert_eq!(
        fixture_sha256(V0_3_20_AMBIGUOUS_FIXTURE),
        V0_3_20_AMBIGUOUS_FIXTURE_SHA256
    );
    assert_eq!(
        fixture_sha256(V0_3_20_MALFORMED_FIXTURE),
        V0_3_20_MALFORMED_FIXTURE_SHA256
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("v0.3.20.db");
    seed_v0_3_20_fixture(&db_path, None);
    let conn = Connection::open(&db_path).unwrap();

    let version: String = conn
        .query_row(
            "SELECT value FROM control_metadata WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "1");

    let sandbox_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM sandbox_state", [], |row| row.get(0))
        .unwrap();
    assert_eq!(sandbox_count, 3);

    let developer_labels: String = conn
        .query_row(
            "SELECT labels_json FROM sandbox_state WHERE sandbox_id = 'vz-run-shop-a1b2c3d4e5f6'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let developer_labels: serde_json::Value = serde_json::from_str(&developer_labels).unwrap();
    assert_eq!(
        developer_labels["vz.run.workspace"].as_str(),
        Some("/workspace")
    );
    assert!(
        developer_labels.get("project_dir").is_none(),
        "v0.3.20 vz run did not persist the host workspace source"
    );

    let hardened_labels: String = conn
        .query_row(
            "SELECT labels_json FROM sandbox_state WHERE sandbox_id = 'sbx-hardened-001'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let hardened_labels: serde_json::Value = serde_json::from_str(&hardened_labels).unwrap();
    assert_eq!(hardened_labels["vz.space.mode"].as_str(), Some("required"));

    let dependent_rows: i64 = conn
        .query_row(
            "SELECT
                (SELECT COUNT(*) FROM container_state WHERE sandbox_id = 'vz-run-shop-a1b2c3d4e5f6') +
                (SELECT COUNT(*) FROM checkpoint_state WHERE sandbox_id = 'vz-run-shop-a1b2c3d4e5f6') +
                (SELECT COUNT(*) FROM receipt_state WHERE entity_id = 'vz-run-shop-a1b2c3d4e5f6') +
                (SELECT COUNT(*) FROM events WHERE stack_name = 'vz-run-shop-a1b2c3d4e5f6')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(dependent_rows, 4);
}

#[test]
fn v0_3_20_negative_fixture_extensions_are_distinct_and_loadable() {
    for (name, extension, expected_id) in [
        (
            "ambiguous",
            V0_3_20_AMBIGUOUS_FIXTURE,
            "vz-run-ambiguous-deadbeef0001",
        ),
        (
            "malformed",
            V0_3_20_MALFORMED_FIXTURE,
            "vz-run-malformed-deadbeef0002",
        ),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join(format!("v0.3.20-{name}.db"));
        seed_v0_3_20_fixture(&db_path, Some(extension));
        let conn = Connection::open(db_path).unwrap();
        let row_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sandbox_state WHERE sandbox_id = ?1",
                params![expected_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(row_count, 1);
    }
}

#[test]
fn topology_complete_aggregate_round_trips_after_database_relocation() {
    let first_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let first_path = first_dir.path().join("state.db");
    let relocated_path = second_dir.path().join("renamed-state.db");
    let expected =
        topology_project_state("prj_shop", &["agent-a", "agent-b"], "/old/checkout/path");

    {
        let store = StateStore::open(&first_path).unwrap();
        store.save_project_state(&expected).unwrap();
        assert_eq!(store.list_project_states().unwrap(), vec![expected.clone()]);
        assert_eq!(store.schema_version().unwrap(), 9);
        let definition_json: String = store
            .conn
            .query_row(
                "SELECT definition_json FROM project_definitions WHERE project_id = 'prj_shop'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let definition_json: serde_json::Value = serde_json::from_str(&definition_json).unwrap();
        assert_eq!(
            definition_json["environment"]["machines"][0]["profile"],
            "developer"
        );
        let machine_json: String = store
            .conn
            .query_row(
                "SELECT instance_json FROM machine_instances WHERE machine_id = 'mac_agent-a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let machine_json: serde_json::Value = serde_json::from_str(&machine_json).unwrap();
        assert_eq!(machine_json["profile"], "developer");
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM environment_instances", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            2
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM workspace_bindings WHERE name = 'workspace'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            2
        );
        assert!(
            store
                .conn
                .execute("UPDATE workspace_bindings SET name = ''", [])
                .is_err(),
            "the normalized binding name CHECK must reject blank names"
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM machine_instances", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            2
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM environment_networks", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            2
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM environment_endpoints", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            2
        );
    }

    std::fs::copy(&first_path, &relocated_path).unwrap();
    let reopened = StateStore::open(&relocated_path).unwrap();
    let actual = reopened.load_project_state("prj_shop").unwrap().unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.environments.len(), 2);
    assert_eq!(
        actual.definition.environment.machines[0].profile,
        MachineProfile::Developer
    );
    assert!(actual.environments.iter().all(|environment| {
        environment.machines[0].name == "linux"
            && environment.machines[0].profile == MachineProfile::Developer
    }));
    assert!(
        actual
            .environments
            .iter()
            .all(|environment| { environment.bindings[0].workspace_key == "same-worktree-key" })
    );
}

#[test]
fn hardened_machine_profile_round_trips_durably_without_docker_capabilities() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("hardened.db");
    let expected = hardened_topology_project_state("prj_hardened", "secure");

    {
        let store = StateStore::open(&db_path).unwrap();
        store.save_project_state(&expected).unwrap();
        let definition_json: String = store
            .conn
            .query_row(
                "SELECT definition_json FROM project_definitions WHERE project_id = 'prj_hardened'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let definition_json: serde_json::Value = serde_json::from_str(&definition_json).unwrap();
        assert_eq!(
            definition_json["environment"]["machines"][0]["profile"],
            "hardened"
        );
        let machine_json: String = store
            .conn
            .query_row(
                "SELECT instance_json FROM machine_instances WHERE machine_id = 'mac_secure'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let machine_json: serde_json::Value = serde_json::from_str(&machine_json).unwrap();
        assert_eq!(machine_json["profile"], "hardened");
    }

    let reopened = StateStore::open(&db_path).unwrap();
    let actual = reopened
        .load_project_state("prj_hardened")
        .unwrap()
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        actual.definition.environment.machines[0].profile,
        MachineProfile::Hardened
    );
    assert_eq!(
        actual.environments[0].machines[0].profile,
        MachineProfile::Hardened
    );
}

#[test]
fn stored_definition_without_machine_profile_fails_closed() {
    let store = StateStore::in_memory().unwrap();
    let state = topology_project_state("prj_missing_definition_profile", &["agent"], "/checkout");
    store.save_project_state(&state).unwrap();

    let definition_json: String = store
        .conn
        .query_row(
            "SELECT definition_json FROM project_definitions
             WHERE project_id = 'prj_missing_definition_profile'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut definition_json: serde_json::Value = serde_json::from_str(&definition_json).unwrap();
    definition_json["environment"]["machines"][0]
        .as_object_mut()
        .unwrap()
        .remove("profile");
    store
        .conn
        .execute(
            "UPDATE project_definitions SET definition_json = ?1
             WHERE project_id = 'prj_missing_definition_profile'",
            params![serde_json::to_string(&definition_json).unwrap()],
        )
        .unwrap();

    let error = store
        .load_project_state("prj_missing_definition_profile")
        .expect_err("missing persisted definition profile must fail closed")
        .to_string();
    assert!(error.contains("profile"), "unexpected error: {error}");
}

#[test]
fn stored_machine_instance_without_profile_fails_closed() {
    let store = StateStore::in_memory().unwrap();
    let state = topology_project_state("prj_missing_instance_profile", &["agent"], "/checkout");
    store.save_project_state(&state).unwrap();

    let machine_json: String = store
        .conn
        .query_row(
            "SELECT instance_json FROM machine_instances WHERE machine_id = 'mac_agent'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut machine_json: serde_json::Value = serde_json::from_str(&machine_json).unwrap();
    machine_json.as_object_mut().unwrap().remove("profile");
    store
        .conn
        .execute(
            "UPDATE machine_instances SET instance_json = ?1 WHERE machine_id = 'mac_agent'",
            params![serde_json::to_string(&machine_json).unwrap()],
        )
        .unwrap();

    let error = store
        .load_project_state("prj_missing_instance_profile")
        .expect_err("missing persisted Machine instance profile must fail closed")
        .to_string();
    assert!(error.contains("profile"), "unexpected error: {error}");
}

#[test]
fn topology_save_is_all_or_nothing_on_cross_environment_id_collision() {
    let store = StateStore::in_memory_with_pragmas(StateStorePragmas::daemon_defaults()).unwrap();
    let baseline = topology_project_state("prj_baseline", &["stable"], "/checkout");
    store.save_project_state(&baseline).unwrap();

    let mut conflicting =
        topology_project_state("prj_conflict", &["agent-a", "agent-b"], "/checkout");
    let duplicate_machine_id = conflicting.environments[0].machines[0].machine_id.clone();
    let second = &mut conflicting.environments[1];
    second.machines[0].machine_id = duplicate_machine_id.clone();
    second.machines[0].incarnation.as_mut().unwrap().machine_id = duplicate_machine_id.clone();
    second.endpoints[0].machine_id = duplicate_machine_id.clone();
    for ownership in &mut second.ownership {
        if ownership.machine_id.is_some() {
            ownership.machine_id = Some(duplicate_machine_id.clone());
        }
        if ownership.resource_kind == OwnedResourceKind::Machine {
            ownership.resource_id = duplicate_machine_id.to_string();
        }
    }
    conflicting.validate().unwrap();

    assert!(store.save_project_state(&conflicting).is_err());
    assert!(store.load_project_state("prj_conflict").unwrap().is_none());
    assert_eq!(
        store.load_project_state("prj_baseline").unwrap(),
        Some(baseline)
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM environment_instances", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
}

#[test]
fn persisted_topology_rejects_normalized_column_json_drift_without_mutation() {
    for (case, mutation, table, field) in [
        (
            "project",
            "UPDATE project_definitions SET name = 'drifted-project'",
            "project_definitions",
            "name",
        ),
        (
            "project-created-at",
            "UPDATE project_definitions SET created_at = 99",
            "project_definitions",
            "created_at",
        ),
        (
            "project-updated-at",
            "UPDATE project_definitions SET updated_at = 201",
            "project_definitions",
            "updated_at",
        ),
        (
            "environment",
            r#"UPDATE environment_instances SET state = '"stopped"'"#,
            "environment_instances",
            "state",
        ),
        (
            "binding",
            "UPDATE workspace_bindings SET path_hint = '/drifted/checkout'",
            "workspace_bindings",
            "path_hint",
        ),
        (
            "machine",
            r#"UPDATE machine_instances SET state = '"stopped"'"#,
            "machine_instances",
            "state",
        ),
        (
            "network",
            "UPDATE environment_networks SET name = 'drifted-network'",
            "environment_networks",
            "name",
        ),
        (
            "endpoint",
            "UPDATE environment_endpoints SET name = 'drifted-endpoint'",
            "environment_endpoints",
            "name",
        ),
        (
            "ownership",
            r#"UPDATE topology_ownership SET resource_kind = '"disk"'"#,
            "topology_ownership",
            "resource_kind",
        ),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join(format!("projection-{case}.db"));
        let store = StateStore::open(&db_path).unwrap();
        let state = topology_project_state("prj_projection_drift", &["agent"], "/checkout");
        store.save_project_state(&state).unwrap();
        store.conn.execute(mutation, []).unwrap();

        let bytes_before = std::fs::read(&db_path).unwrap();
        let error = store
            .load_project_state("prj_projection_drift")
            .expect_err("normalized SQL drift must fail closed")
            .to_string();
        let bytes_after = std::fs::read(&db_path).unwrap();

        assert!(
            error.contains("persisted topology projection mismatch"),
            "unexpected error for {case}: {error}"
        );
        assert!(
            error.contains(&format!("table={table}")),
            "missing table for {case}: {error}"
        );
        assert!(
            error.contains(&format!("field={field}")),
            "missing field for {case}: {error}"
        );
        assert_eq!(
            bytes_after, bytes_before,
            "failed projection read mutated the database for {case}"
        );
    }
}

#[test]
fn persisted_topology_parent_child_comparison_is_stable_identity_ordered() {
    let store = StateStore::in_memory().unwrap();
    let mut state = topology_project_state("prj_child_order", &["agent"], "/checkout");
    let environment = &mut state.environments[0];
    environment.bindings.push(WorkspaceBinding {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        binding_id: WorkspaceBindingId::new("wsp_000").unwrap(),
        project_id: state.definition.project_id.clone(),
        environment_id: environment.environment_id.clone(),
        name: "secondary".to_string(),
        workspace_key: "secondary-worktree-key".to_string(),
        path_hint: Some("/secondary".to_string()),
    });
    state.validate().unwrap();
    store.save_project_state(&state).unwrap();

    let loaded = store
        .load_project_state("prj_child_order")
        .unwrap()
        .unwrap();
    assert_eq!(loaded.environments[0].bindings.len(), 2);
    assert_eq!(
        loaded.environments[0].bindings[0].binding_id.as_str(),
        "wsp_000"
    );
    assert_eq!(
        loaded.environments[0].bindings[1].binding_id.as_str(),
        "wsp_agent"
    );
}

#[test]
fn persisted_topology_rejects_self_consistent_child_that_diverges_from_parent_snapshot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("parent-child-drift.db");
    let store = StateStore::open(&db_path).unwrap();
    let state = topology_project_state("prj_parent_child_drift", &["agent"], "/checkout");
    store.save_project_state(&state).unwrap();

    let machine_json: String = store
        .conn
        .query_row(
            "SELECT instance_json FROM machine_instances WHERE machine_id = 'mac_agent'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut machine: MachineInstance = serde_json::from_str(&machine_json).unwrap();
    machine.state = MachineState::Stopped;
    store
        .conn
        .execute(
            "UPDATE machine_instances SET state = ?1, instance_json = ?2
             WHERE machine_id = 'mac_agent'",
            params![
                serde_json::to_string(&machine.state).unwrap(),
                serde_json::to_string(&machine).unwrap()
            ],
        )
        .unwrap();

    let bytes_before = std::fs::read(&db_path).unwrap();
    let error = store
        .load_project_state("prj_parent_child_drift")
        .expect_err("parent and normalized child snapshots must agree")
        .to_string();
    let bytes_after = std::fs::read(&db_path).unwrap();
    assert!(error.contains("table=environment_instances"));
    assert!(error.contains("key=env_agent"));
    assert!(error.contains("field=machines"));
    assert_eq!(
        bytes_after, bytes_before,
        "failed read must not mutate state"
    );
}

#[test]
fn v3_schema_has_durable_lifecycle_shape_and_constraints() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("canonical-v3.db");
    let store = create_v3_store(&db_path);
    assert_eq!(store.schema_version().unwrap(), 3);
    store.validate_v3_schema().unwrap();

    let object_names = application_schema_snapshot(&store.conn)
        .into_iter()
        .map(|(kind, name, _, _)| (kind, name))
        .collect::<Vec<_>>();
    for expected in [
        ("table", "environment_lifecycle_operations"),
        ("table", "environment_tombstones"),
        ("index", "idx_environment_active_operation"),
        ("index", "idx_environment_lifecycle_project"),
        ("index", "idx_environment_lifecycle_status"),
        ("index", "idx_environment_lifecycle_one_active"),
        ("index", "idx_environment_tombstone_project"),
        ("trigger", "environment_lifecycle_idempotency_key_immutable"),
        ("trigger", "environment_lifecycle_intent_immutable"),
    ] {
        assert!(
            object_names
                .iter()
                .any(|(kind, name)| kind == expected.0 && name == expected.1),
            "missing canonical schema object {expected:?}"
        );
    }

    let lifecycle_foreign_keys: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_foreign_key_list('environment_lifecycle_operations')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let tombstone_foreign_keys: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_foreign_key_list('environment_tombstones')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!((lifecycle_foreign_keys, tombstone_foreign_keys), (0, 0));

    let insert_operation = |operation_id: &str,
                            idempotency_key: &str,
                            environment_id: &str,
                            generation: i64,
                            status: &str| {
        store.conn.execute(
            "INSERT INTO environment_lifecycle_operations
                (operation_id, idempotency_key, request_id, project_id, environment_id,
                 schema_version, generation, kind, status, request_hash, definition_digest,
                 initial_state, requested_target, operation_json, created_at, updated_at,
                 completed_at)
             VALUES (?1, ?2, 'req_fixture', 'prj_missing', ?3, 1, ?4, 'up', ?5,
                     'sha256:request', 'sha256:definition', 'stopped', 'ready', '{}', 10, 10,
                     CASE WHEN ?5 IN ('succeeded', 'failed', 'superseded') THEN 10 ELSE NULL END)",
            params![
                operation_id,
                idempotency_key,
                environment_id,
                generation,
                status
            ],
        )
    };

    insert_operation("lop_done", "idem_done", "env_absent", 1, "succeeded").unwrap();
    insert_operation("lop_active", "idem_active", "env_absent", 2, "planned").unwrap();
    assert!(
        insert_operation("lop_blocked", "idem_blocked", "env_absent", 3, "blocked").is_err(),
        "only one planned/running/blocked operation may exist per Environment"
    );
    assert!(
        insert_operation(
            "lop_generation",
            "idem_generation",
            "env_absent",
            2,
            "failed"
        )
        .is_err(),
        "Environment generations must be unique"
    );
    insert_operation(
        "lop_superseded",
        "idem_superseded",
        "env_absent",
        3,
        "superseded",
    )
    .unwrap();
    assert!(
        insert_operation(
            "lop_idempotency",
            "idem_active",
            "env_other",
            1,
            "succeeded"
        )
        .is_err(),
        "idempotency keys must be globally unique"
    );
    let immutable_error = store
        .conn
        .execute(
            "UPDATE environment_lifecycle_operations
             SET idempotency_key = 'idem_changed'
             WHERE operation_id = 'lop_active'",
            [],
        )
        .expect_err("idempotency keys must be immutable")
        .to_string();
    assert!(immutable_error.contains("immutable"));
    let immutable_hash_error = store
        .conn
        .execute(
            "UPDATE environment_lifecycle_operations
             SET request_hash = 'sha256:changed'
             WHERE operation_id = 'lop_active'",
            [],
        )
        .expect_err("request hashes and other intent projections must be immutable")
        .to_string();
    assert!(immutable_hash_error.contains("immutable"));
    assert!(
        store
            .conn
            .execute(
                "UPDATE environment_lifecycle_operations
                 SET completed_at = 11
                 WHERE operation_id = 'lop_active'",
                [],
            )
            .is_err(),
        "active lifecycle rows cannot advertise terminal completion"
    );

    store
        .conn
        .execute(
            "INSERT INTO environment_tombstones
                (environment_id, project_id, schema_version, name, definition_digest,
                 delete_operation_id, lifecycle_generation, ownership_digest, deleted_at,
                 tombstone_json)
             VALUES ('env_absent', 'prj_missing', 1, 'gone', 'sha256:definition',
                     'lop_delete', 4, 'sha256:ownership', 20, '{}')",
            [],
        )
        .unwrap();
}

#[test]
fn lifecycle_stop_up_requires_ready_activation_and_preserves_identity() {
    let store = StateStore::in_memory().unwrap();
    let expected = topology_project_state("prj_lifecycle", &["agent"], "/checkout");
    let original = expected.environments[0].clone();
    store.save_project_state(&expected).unwrap();

    let ready_up = store
        .begin_environment_lifecycle(
            "env_agent",
            EnvironmentLifecycleKind::Up,
            "req-ready-up",
            "idem-ready-up",
            "sha256:ready-up",
            300,
        )
        .unwrap();
    assert_eq!(ready_up.status, EnvironmentLifecycleStatus::Running);
    assert!(
        ready_up
            .machine_steps
            .iter()
            .all(|step| step.status == LifecycleStepStatus::Pending)
    );
    for step in ready_up.machine_steps.clone() {
        store
            .acknowledge_environment_machine_step(
                &MachineLifecycleStepAcknowledgement {
                    operation_id: ready_up.operation_id.clone(),
                    generation: ready_up.generation,
                    machine_id: step.machine_id,
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: step.expected_incarnation.clone(),
                    resulting_incarnation: step.expected_incarnation.clone(),
                    resulting_activation: Some(test_activation(step.expected_incarnation.unwrap())),
                    result: LifecycleStepResult::Succeeded,
                },
                300,
            )
            .unwrap();
    }
    store
        .finish_environment_lifecycle(ready_up.operation_id.as_str(), ready_up.generation, 300)
        .unwrap();

    let stop = store
        .begin_environment_lifecycle(
            "env_agent",
            EnvironmentLifecycleKind::Stop,
            "req-stop",
            "idem-stop",
            "sha256:stop",
            301,
        )
        .unwrap();
    assert_eq!(stop.status, EnvironmentLifecycleStatus::Running);
    let mut stop = stop;
    for step in stop.machine_steps.clone() {
        stop = store
            .acknowledge_environment_machine_step(
                &MachineLifecycleStepAcknowledgement {
                    operation_id: stop.operation_id.clone(),
                    generation: stop.generation,
                    machine_id: step.machine_id,
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: step.expected_incarnation,
                    resulting_incarnation: None,
                    resulting_activation: None,
                    result: LifecycleStepResult::Succeeded,
                },
                302,
            )
            .unwrap();
    }
    assert_eq!(stop.status, EnvironmentLifecycleStatus::Running);
    let stop = store
        .finish_environment_lifecycle(stop.operation_id.as_str(), stop.generation, 303)
        .unwrap();
    assert_eq!(stop.status, EnvironmentLifecycleStatus::Succeeded);

    let stopped_stop = store
        .begin_environment_lifecycle(
            "env_agent",
            EnvironmentLifecycleKind::Stop,
            "req-stopped-stop",
            "idem-stopped-stop",
            "sha256:stopped-stop",
            304,
        )
        .unwrap();
    assert_eq!(stopped_stop.status, EnvironmentLifecycleStatus::Succeeded);
    assert!(
        stopped_stop
            .machine_steps
            .iter()
            .all(|step| step.status == LifecycleStepStatus::Succeeded)
    );

    let mut up = store
        .begin_environment_lifecycle(
            "env_agent",
            EnvironmentLifecycleKind::Up,
            "req-up",
            "idem-up",
            "sha256:up",
            305,
        )
        .unwrap();
    for step in up.machine_steps.clone() {
        up = store
            .acknowledge_environment_machine_step(
                &MachineLifecycleStepAcknowledgement {
                    operation_id: up.operation_id.clone(),
                    generation: up.generation,
                    machine_id: step.machine_id,
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: step.expected_incarnation.clone(),
                    resulting_incarnation: step.expected_incarnation.clone(),
                    resulting_activation: Some(test_activation(step.expected_incarnation.unwrap())),
                    result: LifecycleStepResult::Succeeded,
                },
                306,
            )
            .unwrap();
    }
    store
        .finish_environment_lifecycle(up.operation_id.as_str(), up.generation, 307)
        .unwrap();

    let actual = store
        .load_project_state("prj_lifecycle")
        .unwrap()
        .unwrap()
        .environments
        .remove(0);
    assert_eq!(actual.state, EnvironmentState::Ready);
    assert_eq!(actual.environment_id, original.environment_id);
    assert_eq!(
        actual.machines[0].machine_id,
        original.machines[0].machine_id
    );
    assert_eq!(
        actual.machines[0].incarnation,
        original.machines[0].incarnation
    );
    assert_eq!(actual.definition_digest, original.definition_digest);
    assert_eq!(actual.ownership, original.ownership);
}

#[test]
fn lifecycle_idempotency_and_two_connection_admission_are_atomic() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("lifecycle-admission.db");
    let first =
        StateStore::open_with_pragmas(&db_path, StateStorePragmas::daemon_defaults()).unwrap();
    first
        .save_project_state(&topology_project_state(
            "prj_admission",
            &["first", "second"],
            "/checkout",
        ))
        .unwrap();
    let second =
        StateStore::open_with_pragmas(&db_path, StateStorePragmas::daemon_defaults()).unwrap();

    let operation = first
        .begin_environment_lifecycle(
            "env_first",
            EnvironmentLifecycleKind::Stop,
            "req-admit",
            "idem-admit",
            "sha256:admit",
            400,
        )
        .unwrap();
    let replay = second
        .begin_environment_lifecycle(
            "env_first",
            EnvironmentLifecycleKind::Stop,
            "req-admit",
            "idem-admit",
            "sha256:admit",
            999,
        )
        .unwrap();
    assert_eq!(replay, operation);
    let row_count_before: i64 = first
        .conn
        .query_row(
            "SELECT COUNT(*) FROM environment_lifecycle_operations",
            [],
            |row| row.get(0),
        )
        .unwrap();

    for (environment_id, kind, request_id, request_hash) in [
        (
            "env_first",
            EnvironmentLifecycleKind::Stop,
            "req-other",
            "sha256:admit",
        ),
        (
            "env_first",
            EnvironmentLifecycleKind::Up,
            "req-admit",
            "sha256:admit",
        ),
        (
            "env_second",
            EnvironmentLifecycleKind::Stop,
            "req-admit",
            "sha256:admit",
        ),
        (
            "env_first",
            EnvironmentLifecycleKind::Stop,
            "req-admit",
            "sha256:other",
        ),
    ] {
        assert!(
            second
                .begin_environment_lifecycle(
                    environment_id,
                    kind,
                    request_id,
                    "idem-admit",
                    request_hash,
                    401,
                )
                .is_err()
        );
    }
    assert!(
        second
            .begin_environment_lifecycle(
                "env_first",
                EnvironmentLifecycleKind::Up,
                "req-conflict",
                "idem-conflict",
                "sha256:conflict",
                402,
            )
            .is_err()
    );
    assert_eq!(
        first
            .conn
            .query_row(
                "SELECT COUNT(*) FROM environment_lifecycle_operations",
                [],
                |row| { row.get::<_, i64>(0) }
            )
            .unwrap(),
        row_count_before
    );
}

#[test]
fn lifecycle_simultaneous_two_connection_admission_serializes_exactly() {
    fn run_pair(
        db_path: &Path,
        idempotency_keys: [&'static str; 2],
    ) -> [Result<EnvironmentLifecycleOperation, StackError>; 2] {
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let handles = idempotency_keys.map(|idempotency_key| {
            let db_path = db_path.to_path_buf();
            let barrier = std::sync::Arc::clone(&barrier);
            std::thread::spawn(move || {
                let store =
                    StateStore::open_with_pragmas(&db_path, StateStorePragmas::daemon_defaults())
                        .unwrap();
                barrier.wait();
                store.begin_environment_lifecycle(
                    "env_agent",
                    EnvironmentLifecycleKind::Stop,
                    "req-race",
                    idempotency_key,
                    "sha256:race",
                    1_000,
                )
            })
        });
        handles.map(|handle| handle.join().expect("admission thread must not panic"))
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let conflict_path = temp_dir.path().join("lifecycle-race-conflict.db");
    let seed = StateStore::open_with_pragmas(&conflict_path, StateStorePragmas::daemon_defaults())
        .unwrap();
    seed.save_project_state(&topology_project_state(
        "prj_race_conflict",
        &["agent"],
        "/checkout",
    ))
    .unwrap();
    drop(seed);

    let conflict_results = run_pair(&conflict_path, ["idem-race-a", "idem-race-b"]);
    assert_eq!(
        conflict_results
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        1
    );
    assert_eq!(
        conflict_results
            .iter()
            .filter(|result| result.is_err())
            .count(),
        1
    );
    let accepted = conflict_results
        .into_iter()
        .find_map(Result::ok)
        .expect("one competing request must own the Environment fence");
    let inspect = StateStore::open(&conflict_path).unwrap();
    assert_eq!(
        inspect
            .conn
            .query_row(
                "SELECT COUNT(*) FROM environment_lifecycle_operations",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    let environment = inspect
        .load_project_state("prj_race_conflict")
        .unwrap()
        .unwrap()
        .environments
        .remove(0);
    assert_eq!(environment.lifecycle_generation, accepted.generation);
    assert_eq!(
        environment.active_operation_id.as_ref(),
        Some(&accepted.operation_id)
    );

    let replay_path = temp_dir.path().join("lifecycle-race-replay.db");
    let seed =
        StateStore::open_with_pragmas(&replay_path, StateStorePragmas::daemon_defaults()).unwrap();
    seed.save_project_state(&topology_project_state(
        "prj_race_replay",
        &["agent"],
        "/checkout",
    ))
    .unwrap();
    drop(seed);

    let replay_results = run_pair(&replay_path, ["idem-race-shared", "idem-race-shared"]);
    let [first, second] = replay_results.map(Result::unwrap);
    assert_eq!(first, second);
    let inspect = StateStore::open(&replay_path).unwrap();
    assert_eq!(
        inspect
            .conn
            .query_row(
                "SELECT COUNT(*) FROM environment_lifecycle_operations",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    let environment = inspect
        .load_project_state("prj_race_replay")
        .unwrap()
        .unwrap()
        .environments
        .remove(0);
    assert_eq!(environment.lifecycle_generation, first.generation);
    assert_eq!(
        environment.active_operation_id.as_ref(),
        Some(&first.operation_id)
    );
}

#[test]
fn lifecycle_ack_is_fenced_resumable_and_terminal_replay_is_exact() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("lifecycle-resume.db");
    let store = StateStore::open(&db_path).unwrap();
    store
        .save_project_state(&topology_project_state(
            "prj_resume",
            &["agent"],
            "/checkout",
        ))
        .unwrap();
    let operation = store
        .begin_environment_lifecycle(
            "env_agent",
            EnvironmentLifecycleKind::Stop,
            "req-resume",
            "idem-resume",
            "sha256:resume",
            500,
        )
        .unwrap();
    let step = operation.machine_steps[0].clone();
    let exact = MachineLifecycleStepAcknowledgement {
        operation_id: operation.operation_id.clone(),
        generation: operation.generation,
        machine_id: step.machine_id.clone(),
        initial_state: step.initial_state,
        target_state: step.target_state,
        expected_incarnation: step.expected_incarnation,
        resulting_incarnation: None,
        resulting_activation: None,
        result: LifecycleStepResult::Succeeded,
    };
    let mut stale = exact.clone();
    stale.generation -= 1;
    assert!(
        store
            .acknowledge_environment_machine_step(&stale, 501)
            .is_err()
    );
    let mut foreign = exact.clone();
    foreign.machine_id = MachineId::new("mac_foreign").unwrap();
    assert!(
        store
            .acknowledge_environment_machine_step(&foreign, 501)
            .is_err()
    );
    let mut stale_incarnation = exact.clone();
    stale_incarnation.expected_incarnation = None;
    assert!(
        store
            .acknowledge_environment_machine_step(&stale_incarnation, 501)
            .is_err()
    );
    assert_eq!(
        store
            .load_current_environment_lifecycle("env_agent")
            .unwrap(),
        Some(operation.clone()),
        "a stale incarnation acknowledgement must roll back without advancing the journal"
    );
    let acknowledged = store
        .acknowledge_environment_machine_step(&exact, 502)
        .unwrap();
    assert_eq!(acknowledged.status, EnvironmentLifecycleStatus::Running);
    drop(store);

    let reopened = StateStore::open(&db_path).unwrap();
    assert_eq!(
        reopened
            .load_resumable_environment_lifecycle("env_agent")
            .unwrap(),
        Some(acknowledged.clone())
    );
    let finished = reopened
        .finish_environment_lifecycle(
            acknowledged.operation_id.as_str(),
            acknowledged.generation,
            503,
        )
        .unwrap();
    assert_eq!(finished.status, EnvironmentLifecycleStatus::Succeeded);
    assert_eq!(
        reopened
            .acknowledge_environment_machine_step(&exact, 999)
            .unwrap(),
        finished
    );
    let mut different = exact;
    different.result = LifecycleStepResult::Failed {
        reason: "different".to_string(),
    };
    assert!(
        reopened
            .acknowledge_environment_machine_step(&different, 999)
            .is_err()
    );
}

#[test]
fn lifecycle_delete_is_exact_durable_replayable_and_releases_name() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("lifecycle-delete-resume.db");
    let store = StateStore::open(&db_path).unwrap();
    let state = topology_project_state("prj_delete", &["target", "sibling"], "/checkout");
    let old_environment_id = state.environments[0].environment_id.clone();
    store.save_project_state(&state).unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER forbid_sibling_update
             BEFORE UPDATE ON environment_instances
             WHEN OLD.environment_id = 'env_sibling'
             BEGIN SELECT RAISE(ABORT, 'sibling update forbidden'); END;
             CREATE TRIGGER forbid_sibling_delete
             BEFORE DELETE ON environment_instances
             WHEN OLD.environment_id = 'env_sibling'
             BEGIN SELECT RAISE(ABORT, 'sibling delete forbidden'); END;",
        )
        .unwrap();
    let sibling_before: (String, String) = store
        .conn
        .query_row(
            "SELECT state, instance_json FROM environment_instances
             WHERE environment_id = 'env_sibling'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();

    let mut operation = store
        .begin_environment_lifecycle(
            old_environment_id.as_str(),
            EnvironmentLifecycleKind::Delete,
            "req-delete",
            "idem-delete",
            "sha256:delete",
            600,
        )
        .unwrap();
    for step in operation.machine_steps.clone() {
        operation = store
            .acknowledge_environment_machine_step(
                &MachineLifecycleStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    machine_id: step.machine_id,
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: step.expected_incarnation,
                    resulting_incarnation: None,
                    resulting_activation: None,
                    result: LifecycleStepResult::Succeeded,
                },
                601,
            )
            .unwrap();
    }
    store
        .conn
        .execute_batch("DROP TRIGGER forbid_sibling_update; DROP TRIGGER forbid_sibling_delete;")
        .unwrap();
    drop(store);
    let store = StateStore::open(&db_path).unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER forbid_sibling_update
             BEFORE UPDATE ON environment_instances
             WHEN OLD.environment_id = 'env_sibling'
             BEGIN SELECT RAISE(ABORT, 'sibling update forbidden'); END;
             CREATE TRIGGER forbid_sibling_delete
             BEFORE DELETE ON environment_instances
             WHEN OLD.environment_id = 'env_sibling'
             BEGIN SELECT RAISE(ABORT, 'sibling delete forbidden'); END;",
        )
        .unwrap();
    assert_eq!(
        store
            .load_resumable_environment_lifecycle(old_environment_id.as_str())
            .unwrap(),
        Some(operation.clone()),
        "a partially acknowledged delete must resume from its durable journal"
    );
    assert!(
        store
            .finish_environment_delete(operation.operation_id.as_str(), operation.generation, 602,)
            .is_err(),
        "delete cannot finish before exact ownership cleanup succeeds"
    );
    let ownership_count_before: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM topology_ownership WHERE environment_id = ?1",
            params![old_environment_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    for step in operation.cleanup_steps.clone() {
        operation = store
            .acknowledge_environment_cleanup_step(
                &OwnershipCleanupStepAcknowledgement {
                    operation_id: operation.operation_id.clone(),
                    generation: operation.generation,
                    ownership: step.ownership,
                    result: LifecycleStepResult::Succeeded,
                },
                603,
            )
            .unwrap();
    }
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM topology_ownership WHERE environment_id = ?1",
                params![old_environment_id.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        ownership_count_before,
        "cleanup acknowledgements retain ownership evidence until final delete"
    );

    let (finished, tombstone) = store
        .finish_environment_delete(operation.operation_id.as_str(), operation.generation, 604)
        .unwrap();
    assert_eq!(finished.status, EnvironmentLifecycleStatus::Succeeded);
    assert_eq!(tombstone.environment_id, old_environment_id);
    let changes_before_lookup = store.total_changes_for_test();
    assert_eq!(
        store
            .load_environment_lifecycle_by_idempotency_key("idem-delete")
            .unwrap(),
        Some(finished.clone()),
        "read-only replay lookup must retain the deleted Environment identity"
    );
    assert_eq!(store.total_changes_for_test(), changes_before_lookup);
    assert!(
        store
            .load_project_state("prj_delete")
            .unwrap()
            .unwrap()
            .environments
            .iter()
            .all(|environment| environment.environment_id != old_environment_id)
    );
    assert_eq!(
        store
            .load_environment_lifecycle(finished.operation_id.as_str())
            .unwrap(),
        Some(finished.clone())
    );
    assert_eq!(
        store
            .load_environment_tombstone(old_environment_id.as_str())
            .unwrap(),
        Some(tombstone.clone())
    );
    assert_eq!(
        store
            .finish_environment_delete(finished.operation_id.as_str(), finished.generation, 999)
            .unwrap(),
        (finished, tombstone.clone())
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state, instance_json FROM environment_instances
                 WHERE environment_id = 'env_sibling'",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap(),
        sibling_before
    );
    store
        .conn
        .execute_batch("DROP TRIGGER forbid_sibling_update; DROP TRIGGER forbid_sibling_delete;")
        .unwrap();

    let definition = state.definition;
    let created = store
        .resolve_or_reserve_environment_for_up(
            &definition,
            &EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::Name("target".to_string())),
                ..EnvironmentSelectionContext::default()
            },
            605,
        )
        .unwrap();
    let EnvironmentUpReservation::Created { environment } = created else {
        panic!("deleted Environment name should be reusable")
    };
    assert_ne!(environment.environment_id, old_environment_id);

    let mut corrupted_tombstone = tombstone;
    corrupted_tombstone.ownership_digest = "sha256:corrupted".to_string();
    store
        .conn
        .execute(
            "UPDATE environment_tombstones
             SET ownership_digest = ?1, tombstone_json = ?2
             WHERE environment_id = ?3",
            params![
                corrupted_tombstone.ownership_digest,
                serde_json::to_string(&corrupted_tombstone).unwrap(),
                old_environment_id.as_str(),
            ],
        )
        .unwrap();
    let error = store
        .load_environment_tombstone(old_environment_id.as_str())
        .expect_err("tombstone ownership must match the succeeded Delete cleanup plan")
        .to_string();
    assert!(error.contains("field=cleanup_ownership_digest"));
}

#[test]
fn blocked_delete_reopens_retries_and_replays_exact_steps_after_deletion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("blocked-delete-retry.db");
    let store = StateStore::open(&db_path).unwrap();
    store
        .save_project_state(&topology_project_state(
            "prj_blocked_delete",
            &["agent"],
            "/checkout",
        ))
        .unwrap();
    let operation = store
        .begin_environment_lifecycle(
            "env_agent",
            EnvironmentLifecycleKind::Delete,
            "req-blocked-delete",
            "idem-blocked-delete",
            "sha256:blocked-delete",
            650,
        )
        .unwrap();
    let step = operation.machine_steps[0].clone();
    let failed_ack = MachineLifecycleStepAcknowledgement {
        operation_id: operation.operation_id.clone(),
        generation: operation.generation,
        machine_id: step.machine_id,
        initial_state: step.initial_state,
        target_state: step.target_state,
        expected_incarnation: step.expected_incarnation,
        resulting_incarnation: None,
        resulting_activation: None,
        result: LifecycleStepResult::Failed {
            reason: "transient backend failure".to_string(),
        },
    };
    let mut blocked = store
        .acknowledge_environment_machine_step(&failed_ack, 651)
        .unwrap();
    let cleanup_acks = blocked
        .cleanup_steps
        .iter()
        .map(|step| OwnershipCleanupStepAcknowledgement {
            operation_id: blocked.operation_id.clone(),
            generation: blocked.generation,
            ownership: step.ownership.clone(),
            result: LifecycleStepResult::Succeeded,
        })
        .collect::<Vec<_>>();
    for acknowledgement in &cleanup_acks {
        blocked = store
            .acknowledge_environment_cleanup_step(acknowledgement, 651)
            .unwrap();
    }
    assert_eq!(blocked.status, EnvironmentLifecycleStatus::Blocked);
    drop(store);

    let store = StateStore::open(&db_path).unwrap();
    assert_eq!(
        store
            .load_resumable_environment_lifecycle("env_agent")
            .unwrap(),
        Some(blocked.clone())
    );
    assert_eq!(
        store
            .acknowledge_environment_machine_step(&failed_ack, 999)
            .unwrap(),
        blocked,
        "an exact failed-step replay must not advance the journal"
    );
    let mut succeeded_ack = failed_ack;
    succeeded_ack.result = LifecycleStepResult::Succeeded;
    let retried = store
        .acknowledge_environment_machine_step(&succeeded_ack, 652)
        .unwrap();
    for acknowledgement in &cleanup_acks {
        assert_eq!(
            store
                .acknowledge_environment_cleanup_step(acknowledgement, 999)
                .unwrap(),
            retried,
            "an exact successful cleanup replay must not advance the journal"
        );
    }
    let (finished, _) = store
        .finish_environment_delete(retried.operation_id.as_str(), retried.generation, 654)
        .unwrap();
    drop(store);

    let store = StateStore::open(&db_path).unwrap();
    assert_eq!(
        store
            .acknowledge_environment_machine_step(&succeeded_ack, 999)
            .unwrap(),
        finished
    );
    for acknowledgement in &cleanup_acks {
        assert_eq!(
            store
                .acknowledge_environment_cleanup_step(acknowledgement, 999)
                .unwrap(),
            finished
        );
    }
}

#[test]
fn delete_supersedes_non_delete_and_resource_reservation_obeys_fence() {
    let store = StateStore::in_memory().unwrap();
    let state = topology_project_state("prj_supersede", &["agent"], "/checkout");
    store.save_project_state(&state).unwrap();
    let stop = store
        .begin_environment_lifecycle(
            "env_agent",
            EnvironmentLifecycleKind::Stop,
            "req-stop-before-delete",
            "idem-stop-before-delete",
            "sha256:stop-before-delete",
            700,
        )
        .unwrap();
    let delete = store
        .begin_environment_lifecycle(
            "env_agent",
            EnvironmentLifecycleKind::Delete,
            "req-delete-after-stop",
            "idem-delete-after-stop",
            "sha256:delete-after-stop",
            701,
        )
        .unwrap();
    assert_eq!(delete.generation, stop.generation + 1);
    assert_eq!(
        store
            .load_environment_lifecycle(stop.operation_id.as_str())
            .unwrap()
            .unwrap()
            .status,
        EnvironmentLifecycleStatus::Superseded
    );
    assert_eq!(
        store
            .load_current_environment_lifecycle("env_agent")
            .unwrap(),
        Some(delete.clone())
    );
    let requested = OwnershipRecord {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        resource_kind: OwnedResourceKind::Disk,
        resource_id: "vzr1-active-disk".to_string(),
        environment_id: EnvironmentId::new("env_agent").unwrap(),
        machine_id: Some(MachineId::new("mac_agent").unwrap()),
    };
    assert!(store.reserve_owned_resource(&requested, 702).is_err());
    assert!(
        store
            .conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM topology_ownership WHERE resource_id = 'vzr1-active-disk'
                 )",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap()
            == false
    );
}

#[test]
fn failed_first_up_can_stop_without_activation_reopen_and_retry() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("failed-first-up-stop.db");
    let template = topology_project_state("prj_failed_first_up", &["template"], "/checkout");
    let mut definition = template.definition;
    definition.environment.machines[0].workspace = None;
    let environment = definition.instantiate_environment("fresh", 780).unwrap();
    let environment_id = environment.environment_id.clone();
    let machine_id = environment.machines[0].machine_id.clone();

    let store = StateStore::open(&path).unwrap();
    store
        .save_project_state(&ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments: vec![environment],
        })
        .unwrap();

    let failed_up = store
        .begin_environment_lifecycle(
            environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "req-failed-first-up",
            "idem-failed-first-up",
            "sha256:failed-first-up",
            781,
        )
        .unwrap();
    let step = failed_up.machine_steps[0].clone();
    let failed_up = store
        .acknowledge_environment_machine_step(
            &MachineLifecycleStepAcknowledgement {
                operation_id: failed_up.operation_id.clone(),
                generation: failed_up.generation,
                machine_id: machine_id.clone(),
                initial_state: step.initial_state,
                target_state: step.target_state,
                expected_incarnation: None,
                resulting_incarnation: None,
                resulting_activation: None,
                result: LifecycleStepResult::Failed {
                    reason: "backend refused activation".to_string(),
                },
            },
            782,
        )
        .unwrap();
    store
        .finish_environment_lifecycle(failed_up.operation_id.as_str(), failed_up.generation, 783)
        .unwrap();

    let stop = store
        .begin_environment_lifecycle(
            environment_id.as_str(),
            EnvironmentLifecycleKind::Stop,
            "req-stop-never-activated",
            "idem-stop-never-activated",
            "sha256:stop-never-activated",
            784,
        )
        .unwrap();
    let step = stop.machine_steps[0].clone();
    let stop = store
        .acknowledge_environment_machine_step(
            &MachineLifecycleStepAcknowledgement {
                operation_id: stop.operation_id.clone(),
                generation: stop.generation,
                machine_id: machine_id.clone(),
                initial_state: step.initial_state,
                target_state: step.target_state,
                expected_incarnation: None,
                resulting_incarnation: None,
                resulting_activation: None,
                result: LifecycleStepResult::Succeeded,
            },
            785,
        )
        .unwrap();
    store
        .finish_environment_lifecycle(stop.operation_id.as_str(), stop.generation, 786)
        .unwrap();
    drop(store);

    let store = StateStore::open(&path).unwrap();
    let stopped = store
        .load_project_state("prj_failed_first_up")
        .unwrap()
        .unwrap()
        .environments
        .remove(0);
    assert_eq!(stopped.state, EnvironmentState::Stopped);
    assert_eq!(stopped.machines[0].state, MachineState::Stopped);
    assert_eq!(stopped.machines[0].backend, None);
    assert_eq!(stopped.machines[0].incarnation, None);
    assert_eq!(stopped.machines[0].runtime_identity, None);
    assert_eq!(
        stopped.machines[0].negotiated_capabilities,
        CapabilitySet::default()
    );

    let retry_up = store
        .begin_environment_lifecycle(
            environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "req-retry-first-up",
            "idem-retry-first-up",
            "sha256:retry-first-up",
            787,
        )
        .unwrap();
    let step = retry_up.machine_steps[0].clone();
    let incarnation = MachineIncarnation {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        incarnation_id: MachineIncarnationId::new("inc_retry_first_up").unwrap(),
        machine_id,
        generation: 1,
        created_at: 788,
    };
    let retry_up = store
        .acknowledge_environment_machine_step(
            &MachineLifecycleStepAcknowledgement {
                operation_id: retry_up.operation_id.clone(),
                generation: retry_up.generation,
                machine_id: incarnation.machine_id.clone(),
                initial_state: step.initial_state,
                target_state: step.target_state,
                expected_incarnation: None,
                resulting_incarnation: Some(incarnation.clone()),
                resulting_activation: Some(test_activation(incarnation)),
                result: LifecycleStepResult::Succeeded,
            },
            788,
        )
        .unwrap();
    store
        .finish_environment_lifecycle(retry_up.operation_id.as_str(), retry_up.generation, 789)
        .unwrap();
    let ready = store
        .load_project_state("prj_failed_first_up")
        .unwrap()
        .unwrap()
        .environments
        .remove(0);
    assert_eq!(ready.state, EnvironmentState::Ready);
    assert_eq!(ready.machines[0].state, MachineState::Ready);
    assert!(ready.machines[0].runtime_identity.is_some());
}

#[test]
fn successful_up_persists_first_and_replacement_incarnation_ownership_exactly() {
    let store = StateStore::in_memory().unwrap();
    let template = topology_project_state("prj_incarnation", &["template"], "/checkout");
    let definition = template.definition;
    let environment = definition.instantiate_environment("fresh", 800).unwrap();
    let environment_id = environment.environment_id.clone();
    let machine_id = environment.machines[0].machine_id.clone();
    store
        .save_project_state(&ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments: vec![environment],
        })
        .unwrap();

    let mut up = store
        .begin_environment_lifecycle(
            environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "req-first-up",
            "idem-first-up",
            "sha256:first-up",
            801,
        )
        .unwrap();
    let step = up.machine_steps[0].clone();
    assert_eq!(step.expected_incarnation, None);
    let first_incarnation = MachineIncarnation {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        incarnation_id: MachineIncarnationId::new("inc_first").unwrap(),
        machine_id: machine_id.clone(),
        generation: 1,
        created_at: 802,
    };
    up = store
        .acknowledge_environment_machine_step(
            &MachineLifecycleStepAcknowledgement {
                operation_id: up.operation_id.clone(),
                generation: up.generation,
                machine_id: machine_id.clone(),
                initial_state: step.initial_state,
                target_state: step.target_state,
                expected_incarnation: None,
                resulting_incarnation: Some(first_incarnation.clone()),
                resulting_activation: Some(test_activation(first_incarnation.clone())),
                result: LifecycleStepResult::Succeeded,
            },
            802,
        )
        .unwrap();
    store
        .finish_environment_lifecycle(up.operation_id.as_str(), up.generation, 803)
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM topology_ownership
                 WHERE resource_kind = '\"incarnation\"' AND resource_id = 'inc_first'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );

    let mut stop = store
        .begin_environment_lifecycle(
            environment_id.as_str(),
            EnvironmentLifecycleKind::Stop,
            "req-stop-incarnation",
            "idem-stop-incarnation",
            "sha256:stop-incarnation",
            804,
        )
        .unwrap();
    let step = stop.machine_steps[0].clone();
    stop = store
        .acknowledge_environment_machine_step(
            &MachineLifecycleStepAcknowledgement {
                operation_id: stop.operation_id.clone(),
                generation: stop.generation,
                machine_id: machine_id.clone(),
                initial_state: step.initial_state,
                target_state: step.target_state,
                expected_incarnation: step.expected_incarnation,
                resulting_incarnation: None,
                resulting_activation: None,
                result: LifecycleStepResult::Succeeded,
            },
            805,
        )
        .unwrap();
    store
        .finish_environment_lifecycle(stop.operation_id.as_str(), stop.generation, 806)
        .unwrap();

    let mut rebuild = store
        .begin_environment_lifecycle(
            environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "req-rebuild",
            "idem-rebuild",
            "sha256:rebuild",
            807,
        )
        .unwrap();
    let step = rebuild.machine_steps[0].clone();
    let replacement = MachineIncarnation {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        incarnation_id: MachineIncarnationId::new("inc_replacement").unwrap(),
        machine_id,
        generation: 2,
        created_at: 808,
    };
    rebuild = store
        .acknowledge_environment_machine_step(
            &MachineLifecycleStepAcknowledgement {
                operation_id: rebuild.operation_id.clone(),
                generation: rebuild.generation,
                machine_id: replacement.machine_id.clone(),
                initial_state: step.initial_state,
                target_state: step.target_state,
                expected_incarnation: Some(first_incarnation),
                resulting_incarnation: Some(replacement.clone()),
                resulting_activation: Some(test_activation(replacement.clone())),
                result: LifecycleStepResult::Succeeded,
            },
            808,
        )
        .unwrap();
    store
        .finish_environment_lifecycle(rebuild.operation_id.as_str(), rebuild.generation, 809)
        .unwrap();
    let ids = store
        .conn
        .prepare(
            "SELECT resource_id FROM topology_ownership
             WHERE resource_kind = '\"incarnation\"' AND environment_id = ?1",
        )
        .unwrap()
        .query_map(params![environment_id.as_str()], |row| {
            row.get::<_, String>(0)
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(ids, vec![replacement.incarnation_id.to_string()]);
}

#[test]
fn activation_receipt_is_durable_exact_and_exclusive_across_environments() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("activation.db");
    let store = StateStore::open(&path).unwrap();
    let mut definition =
        topology_project_state("prj_activation", &["template"], "/checkout").definition;
    definition.environment.machines[0].workspace = None;
    let first = definition.instantiate_environment("first", 100).unwrap();
    let second = definition.instantiate_environment("second", 100).unwrap();
    let first_id = first.environment_id.clone();
    let second_id = second.environment_id.clone();
    assert!(first.machines[0].backend.is_none());
    assert!(first.machines[0].runtime_identity.is_none());
    store
        .save_project_state(&ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments: vec![first, second],
        })
        .unwrap();
    let begin = |store: &StateStore, id: &EnvironmentId, key: &str| {
        store
            .begin_environment_lifecycle(
                id.as_str(),
                EnvironmentLifecycleKind::Up,
                key,
                key,
                key,
                101,
            )
            .unwrap()
    };
    let ack_for = |operation: &EnvironmentLifecycleOperation, id: &str| {
        let step = &operation.machine_steps[0];
        let incarnation = MachineIncarnation {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            incarnation_id: MachineIncarnationId::new(id).unwrap(),
            machine_id: step.machine_id.clone(),
            generation: 1,
            created_at: 102,
        };
        MachineLifecycleStepAcknowledgement {
            operation_id: operation.operation_id.clone(),
            generation: operation.generation,
            machine_id: step.machine_id.clone(),
            initial_state: step.initial_state,
            target_state: step.target_state,
            expected_incarnation: None,
            resulting_incarnation: Some(incarnation.clone()),
            resulting_activation: Some(test_activation(incarnation)),
            result: LifecycleStepResult::Succeeded,
        }
    };
    let first_up = begin(&store, &first_id, "first-up");
    let exact = ack_for(&first_up, "inc_activation_first");
    let before = store.load_project_state("prj_activation").unwrap();
    let writes = store.total_changes_for_test();
    let mut missing = exact.clone();
    missing.resulting_activation = None;
    assert!(
        store
            .acknowledge_environment_machine_step(&missing, 102)
            .is_err()
    );
    assert_eq!(store.total_changes_for_test(), writes);
    assert_eq!(store.load_project_state("prj_activation").unwrap(), before);
    store
        .acknowledge_environment_machine_step(&exact, 102)
        .unwrap();
    drop(store);

    let store = StateStore::open(&path).unwrap();
    let persisted = store.load_project_state("prj_activation").unwrap().unwrap();
    let machine = &persisted
        .environments
        .iter()
        .find(|env| env.environment_id == first_id)
        .unwrap()
        .machines[0];
    let evidence = exact.resulting_activation.as_ref().unwrap();
    assert_eq!(machine.state, MachineState::Ready);
    assert_eq!(machine.backend.as_ref(), Some(&evidence.backend));
    assert_eq!(
        machine.runtime_identity.as_ref(),
        Some(&evidence.runtime_identity)
    );
    assert_eq!(
        machine.negotiated_capabilities,
        evidence.negotiated_capabilities
    );
    assert_eq!(machine.incarnation.as_ref(), Some(&evidence.incarnation));
    store
        .finish_environment_lifecycle(first_up.operation_id.as_str(), first_up.generation, 103)
        .unwrap();
    let writes = store.total_changes_for_test();
    store
        .acknowledge_environment_machine_step(&exact, 104)
        .unwrap();
    assert_eq!(store.total_changes_for_test(), writes);

    let mut changed_receipts = Vec::new();
    let mut changed = exact.clone();
    changed
        .resulting_activation
        .as_mut()
        .unwrap()
        .runtime_identity
        .opaque_id
        .push_str("-replacement");
    changed_receipts.push(changed);
    let mut changed = exact.clone();
    changed.resulting_activation.as_mut().unwrap().backend = MachineBackend::LinuxNative;
    changed_receipts.push(changed);
    let mut changed = exact.clone();
    changed
        .resulting_activation
        .as_mut()
        .unwrap()
        .negotiated_capabilities = CapabilitySet::default();
    changed_receipts.push(changed);
    let mut changed = exact.clone();
    changed
        .resulting_activation
        .as_mut()
        .unwrap()
        .schema_version += 1;
    changed_receipts.push(changed);
    let mut changed = exact.clone();
    changed.resulting_activation = None;
    changed_receipts.push(changed);
    for changed in changed_receipts {
        assert!(
            store
                .acknowledge_environment_machine_step(&changed, 105)
                .is_err()
        );
        assert_eq!(store.total_changes_for_test(), writes);
    }

    let second_up = begin(&store, &second_id, "second-up");
    let mut collision = ack_for(&second_up, "inc_activation_second");
    collision
        .resulting_activation
        .as_mut()
        .unwrap()
        .runtime_identity = evidence.runtime_identity.clone();
    let writes = store.total_changes_for_test();
    let before = store.load_project_state("prj_activation").unwrap();
    let error = store
        .acknowledge_environment_machine_step(&collision, 106)
        .unwrap_err();
    assert!(error.to_string().contains("already owned"), "{error}");
    assert_eq!(store.total_changes_for_test(), writes);
    assert_eq!(store.load_project_state("prj_activation").unwrap(), before);
    let valid_second = ack_for(&second_up, "inc_activation_second");
    store
        .acknowledge_environment_machine_step(&valid_second, 107)
        .unwrap();
}

#[test]
fn historical_terminal_up_without_activation_is_durable_exact_replay_after_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("historical-activation.db");
    let store = StateStore::open(&path).unwrap();
    let mut definition =
        topology_project_state("prj_historical_activation", &["template"], "/checkout").definition;
    definition.environment.machines[0].workspace = None;
    let environment = definition
        .instantiate_environment("historical", 200)
        .unwrap();
    let environment_id = environment.environment_id.clone();
    store
        .save_project_state(&ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments: vec![environment],
        })
        .unwrap();

    let operation = store
        .begin_environment_lifecycle(
            environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "req-historical-activation",
            "idem-historical-activation",
            "sha256:historical-activation",
            201,
        )
        .unwrap();
    let step = &operation.machine_steps[0];
    let incarnation = MachineIncarnation {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        incarnation_id: MachineIncarnationId::new("inc_historical_activation").unwrap(),
        machine_id: step.machine_id.clone(),
        generation: 1,
        created_at: 202,
    };
    let acknowledgement = MachineLifecycleStepAcknowledgement {
        operation_id: operation.operation_id.clone(),
        generation: operation.generation,
        machine_id: step.machine_id.clone(),
        initial_state: step.initial_state,
        target_state: step.target_state,
        expected_incarnation: None,
        resulting_incarnation: Some(incarnation.clone()),
        resulting_activation: Some(test_activation(incarnation)),
        result: LifecycleStepResult::Succeeded,
    };
    store
        .acknowledge_environment_machine_step(&acknowledgement, 202)
        .unwrap();
    store
        .finish_environment_lifecycle(operation.operation_id.as_str(), operation.generation, 203)
        .unwrap();

    // Downgrade only the newly-added optional fields to the exact shape written
    // by a pre-activation-evidence release, in both normalized and parent JSON.
    let mut historical_environment = store
        .load_environment_instance(environment_id.as_str())
        .unwrap()
        .unwrap();
    historical_environment.machines[0].runtime_identity = None;
    let historical_machine = historical_environment.machines[0].clone();
    let mut historical_operation = store
        .load_environment_lifecycle(operation.operation_id.as_str())
        .unwrap()
        .unwrap();
    historical_operation.machine_steps[0].resulting_activation = None;
    store
        .conn
        .execute(
            "UPDATE environment_instances SET instance_json = ?1 WHERE environment_id = ?2",
            params![
                serde_json::to_string(&historical_environment).unwrap(),
                environment_id.as_str()
            ],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE machine_instances SET instance_json = ?1 WHERE machine_id = ?2",
            params![
                serde_json::to_string(&historical_machine).unwrap(),
                historical_machine.machine_id.as_str()
            ],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE environment_lifecycle_operations SET operation_json = ?1 WHERE operation_id = ?2",
            params![
                serde_json::to_string(&historical_operation).unwrap(),
                historical_operation.operation_id.as_str()
            ],
        )
        .unwrap();
    drop(store);

    let store = StateStore::open(&path).unwrap();
    let reopened_project = store
        .load_project_state("prj_historical_activation")
        .unwrap()
        .unwrap();
    assert!(
        reopened_project.environments[0].machines[0]
            .runtime_identity
            .is_none()
    );
    let reopened_operation = store
        .load_environment_lifecycle(operation.operation_id.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(reopened_operation, historical_operation);
    let mut historical_acknowledgement = acknowledgement;
    historical_acknowledgement.resulting_activation = None;
    let writes = store.total_changes_for_test();
    let replayed = store
        .acknowledge_environment_machine_step(&historical_acknowledgement, 204)
        .unwrap();
    assert_eq!(replayed, historical_operation);
    assert_eq!(store.total_changes_for_test(), writes);
    assert_eq!(
        store
            .load_project_state("prj_historical_activation")
            .unwrap()
            .unwrap(),
        reopened_project
    );
}

#[test]
fn activation_late_journal_failure_rolls_back_machine_parent_and_ownership() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("activation-late-rollback.db");
    let store = StateStore::open(&path).unwrap();
    let mut definition =
        topology_project_state("prj_activation_rollback", &["template"], "/checkout").definition;
    definition.environment.machines[0].workspace = None;
    let environment = definition.instantiate_environment("rollback", 300).unwrap();
    let environment_id = environment.environment_id.clone();
    store
        .save_project_state(&ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments: vec![environment],
        })
        .unwrap();
    let operation = store
        .begin_environment_lifecycle(
            environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "req-activation-rollback",
            "idem-activation-rollback",
            "sha256:activation-rollback",
            301,
        )
        .unwrap();
    let step = &operation.machine_steps[0];
    let incarnation = MachineIncarnation {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        incarnation_id: MachineIncarnationId::new("inc_activation_rollback").unwrap(),
        machine_id: step.machine_id.clone(),
        generation: 1,
        created_at: 302,
    };
    let evidence = test_activation(incarnation.clone());
    let acknowledgement = MachineLifecycleStepAcknowledgement {
        operation_id: operation.operation_id.clone(),
        generation: operation.generation,
        machine_id: step.machine_id.clone(),
        initial_state: step.initial_state,
        target_state: step.target_state,
        expected_incarnation: None,
        resulting_incarnation: Some(incarnation.clone()),
        resulting_activation: Some(evidence.clone()),
        result: LifecycleStepResult::Succeeded,
    };
    let project_before = store
        .load_project_state("prj_activation_rollback")
        .unwrap()
        .unwrap();
    let operation_before = store
        .load_environment_lifecycle(operation.operation_id.as_str())
        .unwrap()
        .unwrap();
    let ownership_before: Vec<(String, String, String, Option<String>, String)> = store
        .conn
        .prepare(
            "SELECT resource_kind, resource_id, environment_id, machine_id, record_json
             FROM topology_ownership ORDER BY resource_kind, resource_id",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TEMP TRIGGER abort_activation_journal_update
             BEFORE UPDATE ON environment_lifecycle_operations
             BEGIN SELECT RAISE(ABORT, 'injected activation journal failure'); END;",
        )
        .unwrap();

    let error = store
        .acknowledge_environment_machine_step(&acknowledgement, 302)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected activation journal failure"),
        "{error}"
    );
    assert_eq!(
        store
            .load_project_state("prj_activation_rollback")
            .unwrap()
            .unwrap(),
        project_before
    );
    assert_eq!(
        store
            .load_environment_lifecycle(operation.operation_id.as_str())
            .unwrap()
            .unwrap(),
        operation_before
    );
    let ownership_after: Vec<(String, String, String, Option<String>, String)> = store
        .conn
        .prepare(
            "SELECT resource_kind, resource_id, environment_id, machine_id, record_json
             FROM topology_ownership ORDER BY resource_kind, resource_id",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(ownership_after, ownership_before);
    assert!(
        !ownership_after.iter().any(|(_, resource_id, _, _, _)| {
            resource_id == incarnation.incarnation_id.as_str()
        })
    );
    store
        .conn
        .execute("DROP TRIGGER abort_activation_journal_update", [])
        .unwrap();
    drop(store);

    let store = StateStore::open(&path).unwrap();
    assert_eq!(
        store
            .load_project_state("prj_activation_rollback")
            .unwrap()
            .unwrap(),
        project_before
    );
    assert_eq!(
        store
            .load_environment_lifecycle(operation.operation_id.as_str())
            .unwrap()
            .unwrap(),
        operation_before
    );
    store
        .acknowledge_environment_machine_step(&acknowledgement, 303)
        .unwrap();
    let project = store
        .load_project_state("prj_activation_rollback")
        .unwrap()
        .unwrap();
    let machine = &project.environments[0].machines[0];
    assert_eq!(
        machine.runtime_identity.as_ref(),
        Some(&evidence.runtime_identity)
    );
    assert_eq!(machine.incarnation.as_ref(), Some(&incarnation));
    let incarnation_owner: (String, String) = store
        .conn
        .query_row(
            "SELECT environment_id, machine_id FROM topology_ownership
             WHERE resource_kind = ?1 AND resource_id = ?2",
            params![
                serde_json::to_string(&OwnedResourceKind::Incarnation).unwrap(),
                incarnation.incarnation_id.as_str()
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        incarnation_owner,
        (environment_id.to_string(), machine.machine_id.to_string())
    );
}

#[test]
fn lifecycle_partial_up_requires_evidence_even_for_ready_sibling() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("partial-up-degraded.db");
    let store = StateStore::open(&db_path).unwrap();
    let mut state = topology_project_state("prj_partial_up", &["agent"], "/checkout");
    let environment = &mut state.environments[0];
    let mut second_spec = state.definition.environment.machines[0].clone();
    second_spec.name = "worker".to_string();
    state.definition.environment.machines.push(second_spec);

    let mut second_machine = environment.machines[0].clone();
    second_machine.machine_id = MachineId::new("mac_worker").unwrap();
    second_machine.name = "worker".to_string();
    second_machine.state = MachineState::Stopped;
    let second_incarnation = MachineIncarnation {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        incarnation_id: MachineIncarnationId::new("inc_worker").unwrap(),
        machine_id: second_machine.machine_id.clone(),
        generation: 1,
        created_at: 50,
    };
    second_machine.incarnation = Some(second_incarnation.clone());
    environment.ownership.extend([
        OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::Incarnation,
            resource_id: second_incarnation.incarnation_id.to_string(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(second_machine.machine_id.clone()),
        },
        OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::Machine,
            resource_id: second_machine.machine_id.to_string(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(second_machine.machine_id.clone()),
        },
    ]);
    environment.machines.push(second_machine);
    environment.state = EnvironmentState::Failed;
    environment.definition_digest = state.definition.digest().unwrap();
    store.save_project_state(&state).unwrap();

    let operation = store
        .begin_environment_lifecycle(
            "env_agent",
            EnvironmentLifecycleKind::Up,
            "req-partial-up",
            "idem-partial-up",
            "sha256:partial-up",
            900,
        )
        .unwrap();
    assert_eq!(operation.status, EnvironmentLifecycleStatus::Running);
    assert_eq!(
        operation
            .machine_steps
            .iter()
            .filter(|step| step.status == LifecycleStepStatus::Succeeded)
            .count(),
        0
    );
    assert_eq!(
        operation
            .machine_steps
            .iter()
            .filter(|step| step.status == LifecycleStepStatus::Pending)
            .count(),
        2
    );
    let ready_sibling_before: (String, String) = store
        .conn
        .query_row(
            "SELECT state, instance_json FROM machine_instances
             WHERE machine_id = 'mac_agent'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER forbid_ready_sibling_machine_update
             BEFORE UPDATE ON machine_instances
             WHEN OLD.machine_id = 'mac_agent'
             BEGIN SELECT RAISE(ABORT, 'ready sibling Machine update forbidden'); END;",
        )
        .unwrap();
    let pending = operation
        .machine_steps
        .iter()
        .find(|step| step.initial_state == MachineState::Stopped)
        .unwrap();
    let acknowledged = store
        .acknowledge_environment_machine_step(
            &MachineLifecycleStepAcknowledgement {
                operation_id: operation.operation_id.clone(),
                generation: operation.generation,
                machine_id: pending.machine_id.clone(),
                initial_state: pending.initial_state,
                target_state: pending.target_state,
                expected_incarnation: pending.expected_incarnation.clone(),
                resulting_incarnation: None,
                resulting_activation: None,
                result: LifecycleStepResult::Failed {
                    reason: "guest failed to start".to_string(),
                },
            },
            901,
        )
        .unwrap();
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state, instance_json FROM machine_instances
                 WHERE machine_id = 'mac_agent'",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap(),
        ready_sibling_before
    );
    store
        .conn
        .execute("DROP TRIGGER forbid_ready_sibling_machine_update", [])
        .unwrap();
    let ready = operation
        .machine_steps
        .iter()
        .find(|step| step.initial_state == MachineState::Ready)
        .unwrap();
    store
        .acknowledge_environment_machine_step(
            &MachineLifecycleStepAcknowledgement {
                operation_id: operation.operation_id.clone(),
                generation: operation.generation,
                machine_id: ready.machine_id.clone(),
                initial_state: ready.initial_state,
                target_state: ready.target_state,
                expected_incarnation: ready.expected_incarnation.clone(),
                resulting_incarnation: ready.expected_incarnation.clone(),
                resulting_activation: Some(test_activation(
                    ready.expected_incarnation.clone().unwrap(),
                )),
                result: LifecycleStepResult::Succeeded,
            },
            901,
        )
        .unwrap();
    let finished = store
        .finish_environment_lifecycle(
            acknowledged.operation_id.as_str(),
            acknowledged.generation,
            902,
        )
        .unwrap();
    assert_eq!(finished.status, EnvironmentLifecycleStatus::Failed);
    drop(store);
    let environment = StateStore::open(&db_path)
        .unwrap()
        .load_project_state("prj_partial_up")
        .unwrap()
        .unwrap()
        .environments
        .remove(0);
    assert_eq!(environment.state, EnvironmentState::Degraded);
}

#[test]
fn lifecycle_bulk_and_journal_projection_drift_fail_closed() {
    let store = StateStore::in_memory().unwrap();
    store
        .save_project_state(&topology_project_state(
            "prj_projection",
            &["agent"],
            "/checkout",
        ))
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE environment_instances SET lifecycle_generation = 99
             WHERE environment_id = 'env_agent'",
            [],
        )
        .unwrap();
    let error = store
        .load_project_state("prj_projection")
        .expect_err("bulk project load must validate normalized lifecycle generation")
        .to_string();
    assert!(error.contains("field=lifecycle_generation"));
    store
        .conn
        .execute(
            "UPDATE environment_instances SET lifecycle_generation = 0
             WHERE environment_id = 'env_agent'",
            [],
        )
        .unwrap();

    let operation = store
        .begin_environment_lifecycle(
            "env_agent",
            EnvironmentLifecycleKind::Stop,
            "req-projection",
            "idem-projection",
            "sha256:projection",
            910,
        )
        .unwrap();
    let operation_json: String = store
        .conn
        .query_row(
            "SELECT operation_json FROM environment_lifecycle_operations
             WHERE operation_id = ?1",
            params![operation.operation_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let mut operation_json: serde_json::Value = serde_json::from_str(&operation_json).unwrap();
    operation_json["request_hash"] = serde_json::Value::String("sha256:drift".to_string());
    store
        .conn
        .execute(
            "UPDATE environment_lifecycle_operations SET operation_json = ?1
             WHERE operation_id = ?2",
            params![
                serde_json::to_string(&operation_json).unwrap(),
                operation.operation_id.as_str()
            ],
        )
        .unwrap();
    let error = store
        .load_environment_lifecycle(operation.operation_id.as_str())
        .expect_err("journal JSON must agree with immutable normalized request hash")
        .to_string();
    assert!(error.contains("field=request_hash"));
    let changes = store.total_changes_for_test();
    let error = store
        .load_environment_lifecycle_by_idempotency_key("idem-projection")
        .expect_err("pre-admission replay lookup must validate the same immutable projections")
        .to_string();
    assert!(error.contains("field=request_hash"));
    assert_eq!(store.total_changes_for_test(), changes);
}

#[test]
fn lifecycle_current_load_and_ack_validate_the_attached_aggregate() {
    let store = StateStore::in_memory().unwrap();
    store
        .save_project_state(&topology_project_state(
            "prj_attachment",
            &["agent"],
            "/checkout",
        ))
        .unwrap();
    let operation = store
        .begin_environment_lifecycle(
            "env_agent",
            EnvironmentLifecycleKind::Stop,
            "req-attachment",
            "idem-attachment",
            "sha256:attachment",
            920,
        )
        .unwrap();
    let step = operation.machine_steps[0].clone();
    let instance_json: String = store
        .conn
        .query_row(
            "SELECT instance_json FROM environment_instances
             WHERE environment_id = 'env_agent'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut environment: EnvironmentInstance = serde_json::from_str(&instance_json).unwrap();
    environment.definition_digest = "sha256:foreign-definition".to_string();
    store
        .conn
        .execute(
            "UPDATE environment_instances
             SET definition_digest = ?1, instance_json = ?2
             WHERE environment_id = 'env_agent'",
            params![
                environment.definition_digest,
                serde_json::to_string(&environment).unwrap(),
            ],
        )
        .unwrap();

    assert!(
        store
            .load_current_environment_lifecycle("env_agent")
            .is_err(),
        "current-operation lookup must validate the journal attachment"
    );
    assert!(
        store
            .acknowledge_environment_machine_step(
                &MachineLifecycleStepAcknowledgement {
                    operation_id: operation.operation_id,
                    generation: operation.generation,
                    machine_id: step.machine_id,
                    initial_state: step.initial_state,
                    target_state: step.target_state,
                    expected_incarnation: step.expected_incarnation,
                    resulting_incarnation: None,
                    resulting_activation: None,
                    result: LifecycleStepResult::Succeeded,
                },
                921,
            )
            .is_err(),
        "acknowledgement must reject a coherently encoded but foreign aggregate"
    );
}

#[test]
fn stopped_environment_rejects_narrow_resource_reservation_without_mutation() {
    let store = StateStore::in_memory().unwrap();
    let mut state = topology_project_state("prj_stopped_reserve", &["agent"], "/checkout");
    state.environments[0].state = EnvironmentState::Stopped;
    state.environments[0].machines[0].state = MachineState::Stopped;
    store.save_project_state(&state).unwrap();
    let before = ownership_snapshot(&store.conn);
    let requested = OwnershipRecord {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        resource_kind: OwnedResourceKind::Disk,
        resource_id: "vzr1-stopped-disk".to_string(),
        environment_id: EnvironmentId::new("env_agent").unwrap(),
        machine_id: Some(MachineId::new("mac_agent").unwrap()),
    };
    assert!(store.reserve_owned_resource(&requested, 999).is_err());
    assert_eq!(ownership_snapshot(&store.conn), before);
}

#[test]
fn v2_to_v3_migration_preserves_topology_and_restricts_owned_parent_deletion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("v2-to-v3.db");
    let store = create_v2_store(&db_path);
    let expected = topology_project_state("prj_v2_migration", &["agent"], "/checkout");
    store.save_project_state(&expected).unwrap();
    let ownership_before = ownership_snapshot(&store.conn);
    assert!(!ownership_before.is_empty());

    store.migrate_topology_v2_to_v3().unwrap();

    assert_eq!(store.schema_version().unwrap(), 3);
    assert_eq!(
        store.load_project_state("prj_v2_migration").unwrap(),
        Some(expected)
    );
    assert_eq!(ownership_snapshot(&store.conn), ownership_before);
    let lifecycle_rows: i64 = store
        .conn
        .query_row(
            "SELECT
                (SELECT COUNT(*) FROM environment_lifecycle_operations) +
                (SELECT COUNT(*) FROM environment_tombstones)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(lifecycle_rows, 0);

    let mut foreign_keys = store
        .conn
        .prepare("PRAGMA foreign_key_list('topology_ownership')")
        .unwrap();
    let delete_actions = foreign_keys
        .query_map([], |row| row.get::<_, String>(6))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(delete_actions, vec!["RESTRICT", "RESTRICT", "RESTRICT"]);
    assert!(
        store
            .conn
            .execute(
                "DELETE FROM environment_instances WHERE environment_id = 'env_agent'",
                [],
            )
            .is_err(),
        "ownership must block Environment deletion until cleanup removes its record"
    );
    assert_eq!(ownership_snapshot(&store.conn), ownership_before);
    let (lifecycle_generation, active_operation_id): (i64, Option<String>) = store
        .conn
        .query_row(
            "SELECT lifecycle_generation, active_operation_id
             FROM environment_instances WHERE environment_id = 'env_agent'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(lifecycle_generation, 0);
    assert_eq!(active_operation_id, None);
}

#[test]
fn v2_to_v3_failure_rolls_back_schema_rows_and_version_then_retries() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("v2-to-v3-failpoint.db");
    let store = create_v2_store(&db_path);
    let expected = topology_project_state("prj_v2_failpoint", &["agent"], "/checkout");
    store.save_project_state(&expected).unwrap();
    let schema_before = application_schema_snapshot(&store.conn);
    let ownership_before = ownership_snapshot(&store.conn);

    let error = store
        .migrate_topology_v2_to_v3_with_failpoint(
            topology::TopologyV3MigrationFailpoint::AfterOwnershipRebuild,
        )
        .expect_err("injected migration failure must abort the transaction")
        .to_string();
    assert!(error.contains("after ownership rebuild"));
    assert_eq!(store.schema_version().unwrap(), 2);
    assert_eq!(application_schema_snapshot(&store.conn), schema_before);
    assert_eq!(ownership_snapshot(&store.conn), ownership_before);
    drop(store);

    let retried = StateStore::open(&db_path).expect("v2-to-v3 migration retry must succeed");
    assert_eq!(retried.schema_version().unwrap(), 9);
    assert_eq!(
        retried.load_project_state("prj_v2_failpoint").unwrap(),
        Some(expected)
    );
    assert_eq!(ownership_snapshot(&retried.conn), ownership_before);
}

#[test]
fn incomplete_v2_schema_is_rejected_before_v3_mutation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("incomplete-v2.db");
    let store = create_v2_store(&db_path);
    store
        .conn
        .execute("DROP TABLE environment_endpoints", [])
        .unwrap();
    drop(store);

    let error = StateStore::open(&db_path)
        .err()
        .expect("incomplete v2 schema must fail before migration")
        .to_string();
    assert!(error.contains("state schema v2 shape mismatch"));
    assert!(error.contains("table:environment_endpoints"));
    let connection = Connection::open(&db_path).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT value FROM control_metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "2"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE name IN ('environment_lifecycle_operations', 'environment_tombstones')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn v0_3_20_developer_migration_is_atomic_idempotent_and_preserves_legacy_rows() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("legacy.db");
    seed_v0_3_20_fixture(&db_path, None);
    let untouched_legacy_rows = legacy_non_developer_rows(&db_path);

    let migrated = {
        let store = StateStore::open(&db_path).unwrap();
        assert_eq!(store.schema_version().unwrap(), 9);
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table'
                       AND name IN (
                           'environment_lifecycle_operations',
                           'environment_tombstones'
                       )",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2,
            "opening exact v1 state must chain through v2, v3, and the complete v4 schema"
        );
        let projects = store.list_project_states().unwrap();
        assert_eq!(projects.len(), 1);
        let project = projects.into_iter().next().unwrap();
        let environment = &project.environments[0];
        assert_eq!(environment.created_at, 1_779_100_100);
        assert_eq!(environment.updated_at, 1_779_100_200);
        assert_eq!(
            environment.machines[0].legacy_sandbox_id.as_deref(),
            Some("vz-run-shop-a1b2c3d4e5f6")
        );
        assert_eq!(environment.bindings[0].path_hint, None);
        assert_eq!(environment.bindings[0].name, "workspace");
        assert_eq!(
            project.definition.environment.machines[0].profile,
            MachineProfile::Developer
        );
        assert_eq!(environment.machines[0].profile, MachineProfile::Developer);
        assert_eq!(
            project.definition.environment.machines[0]
                .workspace
                .as_ref()
                .unwrap()
                .binding,
            environment.bindings[0].name
        );
        assert_eq!(
            environment.definition_digest,
            project.definition.digest().unwrap()
        );
        let machine = &environment.machines[0];
        assert_eq!(machine.target.version, None);
        assert_eq!(machine.target.digest, None);
        for capability in [
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
        ] {
            assert!(machine.requested_capabilities.contains(capability));
            assert!(machine.negotiated_capabilities.contains(capability));
        }
        let provenance = environment.legacy_migration.as_ref().unwrap();
        assert_eq!(provenance.source_version, "v0.3.20");
        assert_eq!(
            provenance.unresolved_resources,
            [
                "host_mount_sources",
                "persistent_disk_path",
                "published_ports",
                "target_image_digest"
            ]
        );
        assert_eq!(store.list_sandboxes().unwrap().len(), 3);
        assert_eq!(
            store
                .load_sandbox("sbx-hardened-001")
                .unwrap()
                .unwrap()
                .labels["vz.space.mode"],
            "required"
        );
        project
    };

    let reopened = StateStore::open(&db_path).unwrap();
    let reopened_projects = reopened.list_project_states().unwrap();
    assert_eq!(reopened_projects, vec![migrated]);
    assert_eq!(
        reopened_projects[0].definition.environment.machines[0].profile,
        MachineProfile::Developer
    );
    assert_eq!(
        reopened_projects[0].environments[0].machines[0].profile,
        MachineProfile::Developer
    );
    assert_eq!(reopened.list_sandboxes().unwrap().len(), 3);
    assert_eq!(legacy_non_developer_rows(&db_path), untouched_legacy_rows);
    let dependent_rows: i64 = reopened
        .conn
        .query_row(
            "SELECT
                (SELECT COUNT(*) FROM container_state WHERE sandbox_id = 'vz-run-shop-a1b2c3d4e5f6') +
                (SELECT COUNT(*) FROM checkpoint_state WHERE sandbox_id = 'vz-run-shop-a1b2c3d4e5f6') +
                (SELECT COUNT(*) FROM receipt_state WHERE entity_id = 'vz-run-shop-a1b2c3d4e5f6') +
                (SELECT COUNT(*) FROM events WHERE stack_name = 'vz-run-shop-a1b2c3d4e5f6')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(dependent_rows, 4);
}

#[test]
fn v0_3_20_migration_identity_uses_persisted_key_not_path_hint() {
    let migrate_with_path = |path_hint: &str| {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("legacy.db");
        seed_v0_3_20_fixture(&db_path, None);
        {
            let conn = Connection::open(&db_path).unwrap();
            let labels: String = conn
                .query_row(
                    "SELECT labels_json FROM sandbox_state
                     WHERE sandbox_id = 'vz-run-shop-a1b2c3d4e5f6'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let mut labels: serde_json::Value = serde_json::from_str(&labels).unwrap();
            labels["project_dir"] = serde_json::Value::String(path_hint.to_string());
            conn.execute(
                "UPDATE sandbox_state SET labels_json = ?1
                 WHERE sandbox_id = 'vz-run-shop-a1b2c3d4e5f6'",
                params![serde_json::to_string(&labels).unwrap()],
            )
            .unwrap();
        }
        StateStore::open(&db_path)
            .unwrap()
            .list_project_states()
            .unwrap()
            .remove(0)
    };

    let old_path = migrate_with_path("/old/worktrees/shop");
    let new_path = migrate_with_path("/relocated/worktrees/shop");
    assert_eq!(
        old_path.definition.project_id,
        new_path.definition.project_id
    );
    assert_eq!(
        old_path.environments[0].environment_id,
        new_path.environments[0].environment_id
    );
    assert_eq!(
        old_path.environments[0].machines[0].machine_id,
        new_path.environments[0].machines[0].machine_id
    );
    assert_ne!(
        old_path.environments[0].bindings[0].path_hint,
        new_path.environments[0].bindings[0].path_hint
    );
}

#[test]
fn v0_3_20_migration_failure_after_partial_write_rolls_back_and_retries() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("legacy-failpoint.db");
    seed_v0_3_20_fixture(&db_path, None);

    let schema_snapshot = |connection: &Connection| {
        let mut statement = connection
            .prepare(
                "SELECT type, name, tbl_name, COALESCE(sql, '')
                 FROM sqlite_master
                 WHERE name NOT LIKE 'sqlite_%'
                 ORDER BY type, name",
            )
            .unwrap();
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };

    let connection = Connection::open(&db_path).unwrap();
    let schema_before = schema_snapshot(&connection);
    let store = StateStore {
        conn: connection,
        event_sender: None,
    };
    let error = store
        .migrate_legacy_v1_to_v2_with_failpoint(
            topology::LegacyMigrationFailpoint::AfterFirstProjectWrite,
        )
        .expect_err("injected migration failure must abort the transaction")
        .to_string();
    assert!(error.contains("after first project write"));
    drop(store);

    let connection = Connection::open(&db_path).unwrap();
    assert_eq!(schema_snapshot(&connection), schema_before);
    assert_eq!(
        connection
            .query_row(
                "SELECT value FROM control_metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "1"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE name IN (
                     'project_definitions', 'environment_instances', 'workspace_bindings',
                     'machine_instances', 'environment_networks', 'environment_endpoints',
                     'topology_ownership'
                 )",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    drop(connection);

    let retried = StateStore::open(&db_path).expect("migration retry must succeed");
    assert_eq!(retried.schema_version().unwrap(), 9);
    let projects = retried.list_project_states().unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(
        projects[0].environments[0]
            .legacy_migration
            .as_ref()
            .map(|provenance| provenance.legacy_sandbox_id.as_str()),
        Some("vz-run-shop-a1b2c3d4e5f6")
    );
    assert_eq!(retried.list_sandboxes().unwrap().len(), 3);
}

#[test]
fn v0_3_20_ambiguous_and_malformed_state_fail_before_schema_mutation() {
    for (name, extension, message) in [
        (
            "ambiguous",
            V0_3_20_AMBIGUOUS_FIXTURE,
            "both Developer and Hardened markers",
        ),
        (
            "malformed",
            V0_3_20_MALFORMED_FIXTURE,
            "serialization error",
        ),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join(format!("{name}.db"));
        seed_v0_3_20_fixture(&db_path, Some(extension));
        let error = StateStore::open(&db_path)
            .err()
            .expect("legacy migration must fail")
            .to_string();
        assert!(error.contains(message), "unexpected error: {error}");

        let conn = Connection::open(&db_path).unwrap();
        let version: String = conn
            .query_row(
                "SELECT value FROM control_metadata WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, "1");
        let topology_tables: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'project_definitions'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(topology_tables, 0);
    }
}

#[test]
fn daemon_pragmas_do_not_change_journal_mode_before_legacy_validation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("ambiguous-daemon-open.db");
    seed_v0_3_20_fixture(&db_path, Some(V0_3_20_AMBIGUOUS_FIXTURE));
    let journal_mode = |path: &Path| {
        Connection::open(path)
            .unwrap()
            .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
            .unwrap()
    };
    assert_eq!(journal_mode(&db_path), "delete");

    let error = StateStore::open_with_pragmas(&db_path, StateStorePragmas::daemon_defaults())
        .err()
        .expect("ambiguous legacy data must fail daemon-style open")
        .to_string();
    assert!(error.contains("both Developer and Hardened markers"));

    assert_eq!(
        journal_mode(&db_path),
        "delete",
        "failed validation must not durably switch the legacy DB to WAL"
    );
    let connection = Connection::open(&db_path).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT value FROM control_metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "1"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'project_definitions'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn future_and_incomplete_v4_schemas_are_rejected_without_repair() {
    let future_dir = tempfile::tempdir().unwrap();
    let future_path = future_dir.path().join("future.db");
    seed_v0_3_20_fixture(&future_path, None);
    {
        let conn = Connection::open(&future_path).unwrap();
        conn.execute(
            "UPDATE control_metadata SET value = '99' WHERE key = 'schema_version'",
            [],
        )
        .unwrap();
    }
    let error = StateStore::open(&future_path)
        .err()
        .expect("future schema must fail")
        .to_string();
    assert!(error.contains("newer than supported"));
    let conn = Connection::open(&future_path).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'project_definitions'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );

    let incomplete_dir = tempfile::tempdir().unwrap();
    let incomplete_path = incomplete_dir.path().join("incomplete.db");
    {
        let store = StateStore::open(&incomplete_path).unwrap();
        store
            .conn
            .execute("DROP TABLE environment_endpoints", [])
            .unwrap();
    }
    let error = StateStore::open(&incomplete_path)
        .err()
        .expect("incomplete v4 schema must fail")
        .to_string();
    assert!(error.contains("state schema v9 shape mismatch"));
    assert!(error.contains("table:environment_endpoints"));
    let conn = Connection::open(&incomplete_path).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'environment_endpoints'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}

#[test]
fn malformed_current_columns_and_foreign_key_data_are_rejected() {
    let column_dir = tempfile::tempdir().unwrap();
    let column_path = column_dir.path().join("unexpected-column.db");
    {
        let store = StateStore::open(&column_path).unwrap();
        store
            .conn
            .execute(
                "ALTER TABLE project_definitions ADD COLUMN unexpected TEXT",
                [],
            )
            .unwrap();
    }
    let error = StateStore::open(&column_path).err().unwrap().to_string();
    assert!(error.contains("state schema v9 shape mismatch"));
    assert!(error.contains("table:project_definitions"));

    let constraint_dir = tempfile::tempdir().unwrap();
    let constraint_path = constraint_dir.path().join("missing-constraints.db");
    seed_v0_3_20_fixture(&constraint_path, None);
    {
        let connection = Connection::open(&constraint_path).unwrap();
        let malformed_ddl = topology::TOPOLOGY_SCHEMA_COMMON_DDL.replace(
            "schema_version INTEGER NOT NULL CHECK(schema_version = 1)",
            "schema_version INTEGER NOT NULL",
        );
        connection.execute_batch(&malformed_ddl).unwrap();
        connection
            .execute_batch(topology::TOPOLOGY_OWNERSHIP_V2_DDL)
            .unwrap();
        connection
            .execute(
                "UPDATE control_metadata SET value = '2' WHERE key = 'schema_version'",
                [],
            )
            .unwrap();
    }
    let error = StateStore::open(&constraint_path)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("state schema v2 shape mismatch"));
    assert!(error.contains("table:project_definitions"));

    let foreign_key_dir = tempfile::tempdir().unwrap();
    let foreign_key_path = foreign_key_dir.path().join("foreign-key-violation.db");
    {
        let store = StateStore::open(&foreign_key_path).unwrap();
        drop(store);
        let connection = Connection::open(&foreign_key_path).unwrap();
        connection
            .pragma_update(None, "foreign_keys", "OFF")
            .unwrap();
        connection
            .execute(
                "INSERT INTO environment_instances
                    (environment_id, project_id, schema_version, name, definition_digest, state,
                     instance_json, created_at, updated_at, legacy_sandbox_id)
                 VALUES (
                    'env_orphan', 'prj_missing', 1, 'orphan', 'sha256:orphan',
                    '\"stopped\"', '{}', 1, 1, NULL
                 )",
                [],
            )
            .unwrap();
    }
    let error = StateStore::open(&foreign_key_path)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("foreign-key violation"));
    assert!(error.contains("table=environment_instances"));
    assert!(error.contains("parent=project_definitions"));
}

#[test]
fn v4_open_rejects_noncanonical_legacy_schema_objects_without_repair() {
    for (name, mutation, expected, verification_sql) in [
        (
            "missing-table",
            "DROP TABLE execution_state",
            "table:execution_state",
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'execution_state'",
        ),
        (
            "malformed-table",
            "ALTER TABLE checkpoint_state ADD COLUMN unexpected TEXT",
            "table:checkpoint_state",
            "SELECT COUNT(*) FROM pragma_table_info('checkpoint_state') WHERE name = 'unexpected'",
        ),
        (
            "missing-index",
            "DROP INDEX idx_build_sandbox",
            "index:idx_build_sandbox",
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_build_sandbox'",
        ),
        (
            "unexpected-trigger",
            "CREATE TRIGGER injected_metadata_trigger
             AFTER UPDATE ON control_metadata
             BEGIN
                 DELETE FROM build_state;
             END",
            "trigger:injected_metadata_trigger",
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = 'injected_metadata_trigger'",
        ),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join(format!("{name}.db"));
        {
            let store = StateStore::open(&db_path).unwrap();
            store.conn.execute_batch(mutation).unwrap();
        }

        let error = StateStore::open(&db_path)
            .err()
            .expect("noncanonical v4 schema must fail")
            .to_string();
        assert!(
            error.contains("state schema v9 shape mismatch"),
            "unexpected error for {name}: {error}"
        );
        assert!(
            error.contains(expected),
            "missing object diagnostic for {name}: {error}"
        );

        let connection = Connection::open(&db_path).unwrap();
        let observed: i64 = connection
            .query_row(verification_sql, [], |row| row.get(0))
            .unwrap();
        let expected_count = i64::from(name == "malformed-table" || name == "unexpected-trigger");
        assert_eq!(
            observed, expected_count,
            "failed v3 open repaired the {name} mutation"
        );
    }
}

#[test]
fn v0_3_20_migration_rejects_noncanonical_schema_before_mutation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("noncanonical-v0.3.20.db");
    seed_v0_3_20_fixture(&db_path, None);
    Connection::open(&db_path)
        .unwrap()
        .execute(
            "ALTER TABLE checkpoint_state ADD COLUMN unexpected TEXT",
            [],
        )
        .unwrap();

    let error = StateStore::open(&db_path)
        .err()
        .expect("noncanonical v0.3.20 schema must not migrate")
        .to_string();
    assert!(error.contains("state schema v1 shape mismatch"));
    assert!(error.contains("table:checkpoint_state"));

    let connection = Connection::open(&db_path).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT value FROM control_metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "1"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE name IN (
                     'project_definitions', 'environment_instances', 'workspace_bindings',
                     'machine_instances', 'environment_networks', 'environment_endpoints',
                     'topology_ownership'
                 )",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0,
        "schema fingerprint failure must occur before topology mutation"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('checkpoint_state')
                 WHERE name = 'unexpected'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1,
        "failed migration must not repair the legacy schema"
    );
}

#[test]
fn v0_3_20_migration_rejects_missing_legacy_index_before_mutation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("missing-index-v0.3.20.db");
    seed_v0_3_20_fixture(&db_path, None);
    Connection::open(&db_path)
        .unwrap()
        .execute("DROP INDEX idx_execution_container", [])
        .unwrap();

    let error = StateStore::open(&db_path)
        .err()
        .expect("v0.3.20 with a missing index must not migrate")
        .to_string();
    assert!(error.contains("state schema v1 shape mismatch"));
    assert!(error.contains("index:idx_execution_container"));

    let connection = Connection::open(&db_path).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT value FROM control_metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "1"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'project_definitions'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn legacy_migration_rechecks_schema_version_inside_transaction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("changed-version-v0.3.20.db");
    seed_v0_3_20_fixture(&db_path, None);
    let connection = Connection::open(&db_path).unwrap();
    connection
        .execute(
            "UPDATE control_metadata SET value = '3' WHERE key = 'schema_version'",
            [],
        )
        .unwrap();
    let store = StateStore {
        conn: connection,
        event_sender: None,
    };

    let error = store
        .migrate_legacy_v1_to_v2()
        .expect_err("migration must recheck its source version under the write reservation")
        .to_string();
    assert!(error.contains("requires state schema version 1, found 3"));
    drop(store);

    let connection = Connection::open(&db_path).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT value FROM control_metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "3"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'project_definitions'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn v0_3_20_migration_validates_all_recovery_rows_before_mutation() {
    for (name, mutation, expected) in [
        (
            "malformed-execution",
            "INSERT INTO execution_state
                (execution_id, container_id, spec_json, state, exit_code, started_at, ended_at,
                 created_at, updated_at)
             VALUES ('exec-corrupt', 'ctr-legacy-workspace', '{not-json', '\"queued\"',
                     NULL, NULL, NULL, 1, 1)",
            "serialization error",
        ),
        (
            "malformed-build",
            "INSERT INTO build_state
                (build_id, sandbox_id, spec_json, state, result_digest, started_at, ended_at,
                 created_at, updated_at)
             VALUES ('build-corrupt', 'vz-run-shop-a1b2c3d4e5f6', '{not-json', '\"queued\"',
                     NULL, 1, NULL, 1, 1)",
            "serialization error",
        ),
        (
            "inconsistent-execution",
            "INSERT INTO execution_state
                (execution_id, container_id, spec_json, state, exit_code, started_at, ended_at,
                 created_at, updated_at)
             VALUES ('exec-inconsistent', 'ctr-legacy-workspace',
                     '{\"cmd\":[],\"args\":[],\"env_override\":{},\"pty\":false,\"timeout_secs\":null}',
                     '\"queued\"', NULL, 1, NULL, 1, 1)",
            "queued executions cannot include start/end/exit metadata",
        ),
        (
            "inconsistent-container",
            "UPDATE container_state SET state = '\"created\"'
             WHERE container_id = 'ctr-legacy-workspace'",
            "created containers cannot include start/end metadata",
        ),
        (
            "negative-execution-timestamp",
            "INSERT INTO execution_state
                (execution_id, container_id, spec_json, state, exit_code, started_at, ended_at,
                 created_at, updated_at)
             VALUES ('exec-negative', 'ctr-legacy-workspace',
                     '{\"cmd\":[],\"args\":[],\"env_override\":{},\"pty\":false,\"timeout_secs\":null}',
                     '\"running\"', NULL, -1, NULL, 1, 1)",
            "persisted execution `exec-negative` has negative `started_at` timestamp -1",
        ),
        (
            "negative-container-timestamp",
            "UPDATE container_state SET started_at = -1
             WHERE container_id = 'ctr-legacy-workspace'",
            "persisted container `ctr-legacy-workspace` has negative `started_at` timestamp -1",
        ),
        (
            "negative-build-timestamp",
            "INSERT INTO build_state
                (build_id, sandbox_id, spec_json, state, result_digest, started_at, ended_at,
                 created_at, updated_at)
             VALUES ('build-negative', 'vz-run-shop-a1b2c3d4e5f6',
                     '{\"context\":\".\",\"dockerfile\":\"Dockerfile\",\"target\":null,\"args\":{},\"cache_from\":[],\"image_tag\":null,\"secrets\":[],\"no_cache\":false,\"push\":false,\"output_oci_tar_dest\":null}',
                     '\"running\"', NULL, -1, NULL, 1, 1)",
            "persisted build `build-negative` has negative `started_at` timestamp -1",
        ),
        (
            "inconsistent-sandbox-timestamp",
            "UPDATE sandbox_state SET updated_at = created_at - 1
             WHERE sandbox_id = 'vz-run-shop-a1b2c3d4e5f6'",
            "update time cannot precede creation time",
        ),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join(format!("{name}.db"));
        seed_v0_3_20_fixture(&db_path, None);
        Connection::open(&db_path)
            .unwrap()
            .execute_batch(mutation)
            .unwrap();

        let error = StateStore::open(&db_path)
            .err()
            .expect("invalid recovery state must block migration")
            .to_string();
        assert!(error.contains(expected), "unexpected error for {name}: {error}");

        let connection = Connection::open(&db_path).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT value FROM control_metadata WHERE key = 'schema_version'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "1"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name = 'project_definitions'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0,
            "invalid recovery state must fail before topology creation"
        );
    }
}

#[test]
fn v0_3_20_migration_rejects_developer_backend_without_architecture_provenance() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("unresolved-target.db");
    seed_v0_3_20_fixture(&db_path, None);
    Connection::open(&db_path)
        .unwrap()
        .execute(
            "UPDATE sandbox_state SET backend = '\"linux_firecracker\"'
             WHERE sandbox_id = 'vz-run-shop-a1b2c3d4e5f6'",
            [],
        )
        .unwrap();

    let error = StateStore::open(&db_path)
        .err()
        .expect("unresolved legacy architecture must block migration")
        .to_string();
    assert!(error.contains("no authoritative v0.3.20 target architecture"));

    let connection = Connection::open(&db_path).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT value FROM control_metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "1"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'project_definitions'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn missing_and_malformed_schema_versions_are_rejected_before_mutation() {
    for (name, mutation, expected) in [
        (
            "missing",
            "DELETE FROM control_metadata WHERE key = 'schema_version'",
            "missing state schema version",
        ),
        (
            "malformed",
            "UPDATE control_metadata SET value = 'v1-ish' WHERE key = 'schema_version'",
            "malformed state schema version",
        ),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join(format!("{name}.db"));
        seed_v0_3_20_fixture(&db_path, None);
        Connection::open(&db_path)
            .unwrap()
            .execute(mutation, [])
            .unwrap();

        let error = StateStore::open(&db_path).err().unwrap().to_string();
        assert!(error.contains(expected), "unexpected error: {error}");
        let conn = Connection::open(&db_path).unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'project_definitions'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    }
}

/// Verify that fresh state stores advertise the current v8 schema.
#[test]
fn migration_v4_schema_detectable() {
    let store = StateStore::in_memory().unwrap();

    // Schema version must be present and equal to "9" after initial init.
    let version_str = store
        .get_control_metadata("schema_version")
        .unwrap()
        .expect("schema_version should be set on first init");
    assert_eq!(version_str, "9");

    // The typed accessor must agree.
    assert_eq!(store.schema_version().unwrap(), 9);

    // created_at must also be set.
    assert!(
        store.get_control_metadata("created_at").unwrap().is_some(),
        "created_at should be populated on first init"
    );
}

/// Pre-populate a database with v1 format data, then re-run `init_schema`
/// (which would add any new tables in a migration scenario). Verify that
/// previously-stored data is still readable.
#[test]
fn migration_old_data_readable_after_schema_update() {
    // Open an in-memory store — this runs init_schema once.
    let store = StateStore::in_memory().unwrap();

    // Pre-populate with v1 data: desired_state, observed_state, events.
    let spec = sample_spec();
    store.save_desired_state("myapp", &spec).unwrap();

    let obs = ServiceObservedState {
        replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        applied_config_digest: None,
        phase: ServicePhase::Running,
        container_id: Some("ctr-web-001".to_string()),
        failed_create_ownership: None,
        last_error: None,
        ready: true,
    };
    store.save_observed_state("myapp", &obs).unwrap();

    store
        .emit_event(
            "myapp",
            &StackEvent::ServiceCreating {
                stack_name: "myapp".to_string(),
                service_name: "web".to_string(),
            },
        )
        .unwrap();

    // Simulate a "migration" by running init_schema again — this calls
    // CREATE TABLE IF NOT EXISTS for every table, including any new ones.
    store.init_schema().unwrap();

    // Verify old data is still readable after the schema re-init.
    let loaded_spec = store.load_desired_state("myapp").unwrap().unwrap();
    assert_eq!(loaded_spec, spec);

    let loaded_obs = store.load_observed_state("myapp").unwrap();
    assert_eq!(loaded_obs.len(), 1);
    assert_eq!(loaded_obs[0].replica.service_name, "web");
    assert_eq!(loaded_obs[0].phase, ServicePhase::Running);

    let loaded_events = store.load_events("myapp").unwrap();
    assert_eq!(loaded_events.len(), 1);

    // Schema version must not have been overwritten by re-init
    // (INSERT OR IGNORE preserves original value).
    assert_eq!(store.schema_version().unwrap(), 9);
}

/// Verify that all existing queries continue to work correctly after new
/// tables are added to the schema. This exercises the full query surface
/// against a freshly-initialized store.
#[test]
fn migration_new_tables_dont_break_old_queries() {
    let store = StateStore::in_memory().unwrap();

    // Exercise every major query path to ensure none are broken.
    // Desired state
    assert!(store.load_desired_state("nonexistent").unwrap().is_none());
    store.save_desired_state("s1", &sample_spec()).unwrap();
    assert!(store.load_desired_state("s1").unwrap().is_some());

    // Observed state
    assert!(store.load_observed_state("s1").unwrap().is_empty());
    let obs = ServiceObservedState {
        replica: crate::state_store::ServiceReplicaKey::first("svc".to_string()).unwrap(),
        applied_config_digest: None,
        phase: ServicePhase::Pending,
        container_id: None,
        failed_create_ownership: None,
        last_error: None,
        ready: false,
    };
    store.save_observed_state("s1", &obs).unwrap();
    assert_eq!(store.load_observed_state("s1").unwrap().len(), 1);

    // Events
    store
        .emit_event(
            "s1",
            &StackEvent::StackApplyStarted {
                stack_name: "s1".to_string(),
                services_count: 1,
            },
        )
        .unwrap();
    assert_eq!(store.load_events("s1").unwrap().len(), 1);

    // Control metadata
    store.set_control_metadata("test_k", "test_v").unwrap();
    assert_eq!(
        store.get_control_metadata("test_k").unwrap().unwrap(),
        "test_v"
    );

    // Mount digests
    store
        .save_service_mount_digest("s1", "svc", "abc123")
        .unwrap();
    let digests = store.load_service_mount_digests("s1").unwrap();
    assert_eq!(digests.get("svc").unwrap(), "abc123");

    // Reconcile progress
    let actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition_for_stack("s1"),
        target: crate::state_store::ServiceReplicaKey::first("svc".to_string()).unwrap(),
    }];
    store
        .save_reconcile_progress("s1", "op-1", &actions, 0)
        .unwrap();
    let progress = store.load_reconcile_progress("s1").unwrap().unwrap();
    assert_eq!(progress.operation_id, "op-1");

    // Checkpoint state (via entity CRUD)
    let checkpoint = Checkpoint {
        checkpoint_id: "ckpt-1".to_string(),
        sandbox_id: "sbx-1".to_string(),
        parent_checkpoint_id: None,
        class: CheckpointClass::FsQuick,
        state: CheckpointState::Ready,
        created_at: 1_700_000_000,
        compatibility_fingerprint: "fp-abc".to_string(),
    };
    store.save_checkpoint(&checkpoint).unwrap();
    let loaded = store.load_checkpoint("ckpt-1").unwrap().unwrap();
    assert_eq!(loaded.checkpoint_id, "ckpt-1");

    // Schema version still intact.
    assert_eq!(store.schema_version().unwrap(), 9);
}

fn journal_fixture(
    reservation_id: &str,
) -> (
    ProjectState,
    StackContainerCreateIntent,
    StackContainerGenerationBinding,
) {
    let project = topology_project_state("prj_journal", &["journal"], "/checkout");
    let (intent, binding) = journal_records_for_environment(
        &project,
        0,
        reservation_id,
        "stack-journal",
        "web",
        "ctr-journal-web",
    );
    (project, intent, binding)
}

fn journal_records_for_environment(
    project: &ProjectState,
    environment_index: usize,
    reservation_id: &str,
    stack_id: &str,
    service_name: &str,
    requested_container_id: &str,
) -> (StackContainerCreateIntent, StackContainerGenerationBinding) {
    let environment = &project.environments[environment_index];
    let machine = &environment.machines[0];
    let scope = vz_runtime_contract::ContainerGenerationScope {
        reservation_id: reservation_id.to_string(),
        project_id: project.definition.project_id.clone(),
        environment_id: environment.environment_id.clone(),
        machine_id: machine.machine_id.clone(),
        machine_incarnation_id: Some(machine.incarnation.as_ref().unwrap().incarnation_id.clone()),
        stack_id: stack_id.to_string(),
    };
    let intent = StackContainerCreateIntent {
        schema_version: StackContainerCreateIntent::SCHEMA_VERSION,
        scope: scope.clone(),
        environment_generation: environment.lifecycle_generation,
        service_name: service_name.to_string(),
        replica_index: 1,
        service_generation: 1,
        requested_container_id: requested_container_id.to_string(),
        definition_digest: environment.definition_digest.clone(),
        action_digest: "sha256:action-journal".to_string(),
        applied_config_digest: Some("vzsc1-sha256:test-config".to_string()),
        status: StackContainerCreateStatus::Intent,
        last_error: None,
        created_at: 100,
        updated_at: 100,
        completed_at: None,
    };
    let binding = StackContainerGenerationBinding {
        reservation_id: reservation_id.to_string(),
        service_name: intent.service_name.clone(),
        ownership: ContainerGenerationOwnership {
            container_id: intent.requested_container_id.clone(),
            generation: 7,
            stack_id: scope.stack_id.clone(),
            scope: Some(Box::new(scope)),
        },
        bound_at: 101,
    };
    (intent, binding)
}

fn overwrite_journal_fixture_environment(store: &StateStore, environment: &EnvironmentInstance) {
    store
        .conn
        .execute(
            "UPDATE environment_instances
             SET state = ?1, instance_json = ?2, updated_at = ?3,
                 lifecycle_generation = ?4, active_operation_id = ?5
             WHERE environment_id = ?6",
            params![
                serde_json::to_string(&environment.state).unwrap(),
                serde_json::to_string(environment).unwrap(),
                environment.updated_at as i64,
                environment.lifecycle_generation as i64,
                environment
                    .active_operation_id
                    .as_ref()
                    .map(|id| id.as_str()),
                environment.environment_id.as_str(),
            ],
        )
        .unwrap();
    for machine in &environment.machines {
        store
            .conn
            .execute(
                "UPDATE machine_instances SET state = ?1, instance_json = ?2
                 WHERE machine_id = ?3",
                params![
                    serde_json::to_string(&machine.state).unwrap(),
                    serde_json::to_string(machine).unwrap(),
                    machine.machine_id.as_str(),
                ],
            )
            .unwrap();
    }
}

fn selector_for_intent(intent: &StackContainerCreateIntent) -> StackContainerCreateSelector {
    StackContainerCreateSelector {
        project_id: intent.scope.project_id.clone(),
        environment_id: intent.scope.environment_id.clone(),
        machine_id: intent.scope.machine_id.clone(),
        machine_incarnation_id: intent.scope.machine_incarnation_id.clone().unwrap(),
        environment_generation: intent.environment_generation,
        stack_id: intent.scope.stack_id.clone(),
        service_name: intent.service_name.clone(),
        replica_index: intent.replica_index,
        requested_container_id: intent.requested_container_id.clone(),
        definition_digest: intent.definition_digest.clone(),
        action_digest: intent.action_digest.clone(),
        applied_config_digest: intent
            .applied_config_digest
            .clone()
            .unwrap_or_else(|| "vzsc1-sha256:test-config".to_string()),
    }
}

fn workload_scope_for_journal_intent(
    intent: &StackContainerCreateIntent,
) -> vz_runtime_contract::MachineWorkloadScope {
    vz_runtime_contract::MachineWorkloadScope {
        schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
        project_id: intent.scope.project_id.clone(),
        environment_id: intent.scope.environment_id.clone(),
        machine_id: intent.scope.machine_id.clone(),
        machine_incarnation_id: intent.scope.machine_incarnation_id.clone().unwrap(),
        stack_id: intent.scope.stack_id.clone(),
    }
}

fn reserve_journal_owner(store: &StateStore, intent: &StackContainerCreateIntent) {
    store
        .reserve_stack_workload_owner(
            &workload_scope_for_journal_intent(intent),
            intent.created_at,
        )
        .unwrap();
}

fn reserve_selector_owner(store: &StateStore, selector: &StackContainerCreateSelector) {
    store
        .reserve_stack_workload_owner(
            &vz_runtime_contract::MachineWorkloadScope {
                schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
                project_id: selector.project_id.clone(),
                environment_id: selector.environment_id.clone(),
                machine_id: selector.machine_id.clone(),
                machine_incarnation_id: selector.machine_incarnation_id.clone(),
                stack_id: selector.stack_id.clone(),
            },
            0,
        )
        .unwrap();
}

#[test]
fn action_precondition_capture_uses_exact_topology_and_never_journaled_head() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, _) = journal_fixture("capture-never");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    let drafts = vec![crate::reconcile::ActionDraft::Create {
        target: ServiceReplicaKey::first(intent.service_name.clone()).unwrap(),
        observed: None,
    }];

    let captured = store
        .capture_action_preconditions(&intent.scope.stack_id, &drafts)
        .unwrap();

    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0].workload(),
        &workload_scope_for_journal_intent(&intent)
    );
    assert_eq!(
        captured[0].environment_generation(),
        intent.environment_generation
    );
    assert_eq!(
        captured[0].journal_head(),
        &crate::reconcile::ExpectedJournalHead::NeverJournaled
    );
}

#[test]
fn action_precondition_capture_rejects_observed_state_changed_after_draft() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, _) = journal_fixture("capture-stale");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    let drafts = vec![crate::reconcile::ActionDraft::Create {
        target: ServiceReplicaKey::first(intent.service_name.clone()).unwrap(),
        observed: None,
    }];
    store
        .save_observed_state(
            &intent.scope.stack_id,
            &ServiceObservedState {
                replica: ServiceReplicaKey::first(intent.service_name.clone()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Pending,
                container_id: None,
                failed_create_ownership: None,
                last_error: None,
                ready: false,
            },
        )
        .unwrap();

    let error = store
        .capture_action_preconditions(&intent.scope.stack_id, &drafts)
        .unwrap_err();
    assert!(error.to_string().contains("changed after action planning"));
}

#[test]
fn action_precondition_capture_includes_exact_bound_journal_head() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, binding) = journal_fixture("capture-bound");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    store.bind_stack_container_generation(&binding).unwrap();
    let observed = store
        .load_observed_state_for_replica(
            &intent.scope.stack_id,
            &intent.service_name,
            intent.replica_index,
        )
        .unwrap()
        .unwrap();
    let drafts = vec![crate::reconcile::ActionDraft::Recreate {
        target: ServiceReplicaKey::first(intent.service_name.clone()).unwrap(),
        observed,
    }];

    let captured = store
        .capture_action_preconditions(&intent.scope.stack_id, &drafts)
        .unwrap();

    assert_eq!(
        captured[0].journal_head(),
        &crate::reconcile::ExpectedJournalHead::Exact {
            reservation_id: intent.scope.reservation_id.clone(),
            service_generation: intent.service_generation,
            ownership: Some(binding.ownership),
        }
    );
}

#[test]
fn action_precondition_capture_preserves_cleaned_head_across_machine_incarnation() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, binding) = journal_fixture("capture-cleaned-old-incarnation");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    store.bind_stack_container_generation(&binding).unwrap();
    store
        .begin_stack_container_cleanup(&intent.scope.reservation_id, 102)
        .unwrap();
    let stopped = store
        .publish_stack_container_cleanup_success(&intent.scope.reservation_id, 103)
        .unwrap();

    let mut current_environment = project.environments[0].clone();
    current_environment.lifecycle_generation += 1;
    current_environment.updated_at += 1;
    let current_machine = &mut current_environment.machines[0];
    let current_incarnation_id = MachineIncarnationId::new("inc_journal_successor").unwrap();
    current_machine.incarnation = Some(MachineIncarnation {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        incarnation_id: current_incarnation_id.clone(),
        machine_id: current_machine.machine_id.clone(),
        generation: 2,
        created_at: 200,
    });
    let incarnation_ownership = current_environment
        .ownership
        .iter_mut()
        .find(|record| record.resource_kind == OwnedResourceKind::Incarnation)
        .unwrap();
    let previous_incarnation_id = incarnation_ownership.resource_id.clone();
    incarnation_ownership.resource_id = current_incarnation_id.to_string();
    store
        .conn
        .execute(
            "UPDATE topology_ownership SET resource_id = ?1, record_json = ?2
             WHERE resource_kind = ?3 AND resource_id = ?4",
            params![
                incarnation_ownership.resource_id,
                serde_json::to_string(incarnation_ownership).unwrap(),
                serde_json::to_string(&OwnedResourceKind::Incarnation).unwrap(),
                previous_incarnation_id,
            ],
        )
        .unwrap();
    overwrite_journal_fixture_environment(&store, &current_environment);
    let drafts = vec![crate::reconcile::ActionDraft::Create {
        target: ServiceReplicaKey::first(intent.service_name.clone()).unwrap(),
        observed: Some(stopped),
    }];

    let captured = store
        .capture_action_preconditions(&intent.scope.stack_id, &drafts)
        .unwrap();

    assert_eq!(
        captured[0].workload().machine_incarnation_id.as_str(),
        "inc_journal_successor"
    );
    assert_eq!(
        captured[0].environment_generation(),
        intent.environment_generation + 1
    );
    assert_eq!(
        captured[0].journal_head(),
        &crate::reconcile::ExpectedJournalHead::Exact {
            reservation_id: intent.scope.reservation_id.clone(),
            service_generation: intent.service_generation,
            ownership: Some(binding.ownership),
        }
    );
    let actions = vec![Action::ServiceCreate {
        target: ServiceReplicaKey::first(intent.service_name.clone()).unwrap(),
        precondition: captured[0].clone(),
    }];
    install_unstarted_batch(
        &store,
        "rs-old-terminal-new-incarnation",
        "op-old-terminal-new-incarnation",
        &actions,
    );
    assert_eq!(
        store
            .start_reconcile_batch(
                "rs-old-terminal-new-incarnation",
                &intent.scope.stack_id,
                "op-old-terminal-new-incarnation",
                0,
                &actions,
            )
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn v3_to_v4_stack_journal_migration_preserves_rows_without_backfilling_authority() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v3-to-v4.db");
    let store = create_v3_store(&path);
    let (project, _, binding) = journal_fixture("reservation-legacy-observed");
    store.save_project_state(&project).unwrap();
    let observed = serde_json::json!({
        "service_name": "web",
        "phase": "Running",
        "container_id": binding.ownership.container_id,
        "failed_create_ownership": binding.ownership,
        "ready": true
    });
    store
        .conn
        .execute(
            "INSERT INTO observed_state (stack_name, service_name, state_json)
             VALUES ('stack-journal', 'web', ?1)",
            params![observed.to_string()],
        )
        .unwrap();

    store.migrate_stack_journal_v3_to_v4().unwrap();

    assert_eq!(store.schema_version().unwrap(), 4);
    assert_eq!(
        store.load_project_state("prj_journal").unwrap(),
        Some(project)
    );
    let migrated: (i64, String) = store
        .conn
        .query_row(
            "SELECT replica_index, state_json FROM observed_state
             WHERE stack_name = 'stack-journal' AND service_name = 'web'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(migrated.0, 0);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&migrated.1).unwrap(),
        observed
    );
    assert!(
        store
            .list_resumable_stack_container_creates()
            .unwrap()
            .is_empty()
    );
    let journal_rows: i64 = store
        .conn
        .query_row(
            "SELECT
                (SELECT COUNT(*) FROM stack_container_create_intents) +
                (SELECT COUNT(*) FROM stack_container_generation_bindings)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        journal_rows, 0,
        "legacy observed JSON must not mint authority"
    );
    let owner_rows: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM stack_workload_owners", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(
        owner_rows, 0,
        "legacy desired/observed rows must not mint stable stack ownership"
    );
}

#[test]
fn v3_to_v4_stack_journal_migration_rolls_back_and_retries() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v3-to-v4-failpoint.db");
    let store = create_v3_store(&path);
    let (project, _, _) = journal_fixture("reservation-migrate");
    store.save_project_state(&project).unwrap();
    let before = application_schema_snapshot(&store.conn);

    let error = store
        .migrate_stack_journal_v3_to_v4_with_failpoint(
            topology::StackJournalV4MigrationFailpoint::AfterJournalSchemaCreated,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("after journal schema creation"));
    assert_eq!(store.schema_version().unwrap(), 3);
    assert_eq!(application_schema_snapshot(&store.conn), before);
    drop(store);

    let reopened = StateStore::open(&path).unwrap();
    assert_eq!(reopened.schema_version().unwrap(), 9);
    assert_eq!(
        reopened.load_project_state("prj_journal").unwrap(),
        Some(project)
    );
}

#[test]
fn v4_to_v5_replica_migration_rolls_back_then_reopens_and_quarantines_zero() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v4-to-v5-failpoint.db");
    let store = create_v4_store(&path);
    let legacy = serde_json::json!({
        "service_name": "api-2",
        "phase": "Pending",
        "ready": false
    });
    store
        .conn
        .execute(
            "INSERT INTO observed_state
                (stack_name, service_name, replica_index, state_json)
             VALUES ('legacy-stack', 'api-2', 0, ?1)",
            params![legacy.to_string()],
        )
        .unwrap();
    let before = application_schema_snapshot(&store.conn);

    for failpoint in [
        topology::ReplicaV5MigrationFailpoint::AfterDurableActionsRebuilt,
        topology::ReplicaV5MigrationFailpoint::AfterObservedStateRebuilt,
    ] {
        let error = store
            .migrate_replica_v4_to_v5_with_failpoint(failpoint)
            .unwrap_err()
            .to_string();
        assert!(error.contains("injected v4-to-v5 migration failure"));
        assert_eq!(store.schema_version().unwrap(), 4);
        assert_eq!(application_schema_snapshot(&store.conn), before);
    }
    drop(store);

    let reopened = StateStore::open(&path).unwrap();
    assert_eq!(reopened.schema_version().unwrap(), 9);
    assert!(
        reopened
            .load_observed_state("legacy-stack")
            .unwrap()
            .is_empty()
    );
    let quarantined: (i64, String, String) = reopened
        .conn
        .query_row(
            "SELECT replica_index, state_json, reason
             FROM legacy_observed_state_quarantine_v5
             WHERE stack_name = 'legacy-stack'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(quarantined.0, 0);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&quarantined.1).unwrap(),
        legacy
    );
    assert!(quarantined.2.contains("replica-zero"));
    assert!(ServiceReplicaKey::new("api", 0).is_err());
    let zero_state_json = serde_json::json!({
        "replica": {"service_name": "api", "replica_index": 0},
        "phase": "Pending",
        "container_id": null,
        "failed_create_ownership": null,
        "last_error": null,
        "ready": false
    })
    .to_string();
    let raw_zero = reopened.conn.execute(
        "INSERT INTO observed_state
            (stack_name, service_name, replica_index, state_json)
         VALUES ('legacy-stack', 'api', 0, ?1)",
        params![zero_state_json],
    );
    assert!(raw_zero.is_err());
}

#[test]
fn v4_to_v5_rejects_pending_aggregate_progress_but_archives_completed_marker() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v4-progress.db");
    let store = create_v4_store(&path);
    let legacy_actions = r#"[{"kind":"service_create","service_name":"api"}]"#;
    store
        .conn
        .execute(
            "INSERT INTO reconcile_progress
                (stack_name, operation_id, actions_json, next_action_index)
             VALUES ('progress-stack', 'op-legacy', ?1, 0)",
            params![legacy_actions],
        )
        .unwrap();
    let before = application_schema_snapshot(&store.conn);
    let error = store.migrate_replica_v4_to_v5().unwrap_err().to_string();
    assert!(error.contains("pending aggregate reconcile progress"));
    assert_eq!(store.schema_version().unwrap(), 4);
    assert_eq!(application_schema_snapshot(&store.conn), before);

    store
        .conn
        .execute(
            "UPDATE reconcile_progress SET next_action_index = 1
             WHERE stack_name = 'progress-stack'",
            [],
        )
        .unwrap();
    store.migrate_replica_v4_to_v5().unwrap();
    assert_eq!(store.schema_version().unwrap(), 5);
    assert!(
        store
            .load_reconcile_progress("progress-stack")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM legacy_reconcile_progress_quarantine_v5
                 WHERE stack_name = 'progress-stack'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

#[test]
fn v4_to_v5_rewrites_only_fully_consistent_journal_backed_replica() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v4-authoritative-observed.db");
    let store = create_v4_store(&path);
    let (project, mut intent, _) = journal_fixture("reservation-api-two");
    intent.service_name = "api".to_string();
    intent.replica_index = 2;
    intent.requested_container_id = "ctr-api-2".to_string();
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    let (mut similarly_named_intent, _) = journal_records_for_environment(
        &project,
        0,
        "reservation-api-dash-two",
        "stack-journal",
        "api-2",
        "ctr-api-dash-two-1",
    );
    similarly_named_intent.action_digest = "sha256:action-api-dash-two".to_string();
    store
        .begin_stack_container_create(&similarly_named_intent)
        .unwrap();
    let similarly_named_exact = store
        .load_observed_state_for_replica("stack-journal", "api-2", 1)
        .unwrap()
        .unwrap();
    let mut similarly_named_legacy = serde_json::to_value(similarly_named_exact).unwrap();
    let object = similarly_named_legacy.as_object_mut().unwrap();
    object.remove("replica");
    object.insert("service_name".to_string(), serde_json::json!("api-2"));
    store
        .conn
        .execute(
            "UPDATE observed_state SET state_json = ?1
             WHERE stack_name = 'stack-journal' AND service_name = 'api-2'
               AND replica_index = 1",
            params![similarly_named_legacy.to_string()],
        )
        .unwrap();
    let decorated_zero = serde_json::json!({
        "service_name": "api-2",
        "phase": "Pending",
        "ready": false
    });
    store
        .conn
        .execute(
            "INSERT INTO observed_state
                (stack_name, service_name, replica_index, state_json)
             VALUES ('stack-journal', 'api-2', 0, ?1)",
            params![decorated_zero.to_string()],
        )
        .unwrap();
    let orphan = serde_json::json!({
        "service_name": "orphan-3",
        "phase": "Pending",
        "ready": false
    });
    store
        .conn
        .execute(
            "INSERT INTO observed_state
                (stack_name, service_name, replica_index, state_json)
             VALUES ('stack-journal', 'orphan', 3, ?1)",
            params![orphan.to_string()],
        )
        .unwrap();
    let exact = store
        .load_observed_state_for_replica("stack-journal", "api", 2)
        .unwrap()
        .unwrap();
    let mut legacy = serde_json::to_value(exact).unwrap();
    let object = legacy.as_object_mut().unwrap();
    object.remove("replica");
    object.insert("service_name".to_string(), serde_json::json!("api-3"));
    store
        .conn
        .execute(
            "UPDATE observed_state SET state_json = ?1
             WHERE stack_name = 'stack-journal' AND service_name = 'api' AND replica_index = 2",
            params![legacy.to_string()],
        )
        .unwrap();

    let before = application_schema_snapshot(&store.conn);
    let error = store.migrate_replica_v4_to_v5().unwrap_err().to_string();
    assert!(error.contains("identity mismatch"));
    assert_eq!(store.schema_version().unwrap(), 4);
    assert_eq!(application_schema_snapshot(&store.conn), before);
    legacy
        .as_object_mut()
        .unwrap()
        .insert("service_name".to_string(), serde_json::json!("api-2"));
    store
        .conn
        .execute(
            "UPDATE observed_state SET state_json = ?1
             WHERE stack_name = 'stack-journal' AND service_name = 'api' AND replica_index = 2",
            params![legacy.to_string()],
        )
        .unwrap();

    store.migrate_replica_v4_to_v5().unwrap();
    let migrated = store
        .load_observed_state_for_replica("stack-journal", "api", 2)
        .unwrap()
        .unwrap();
    assert_eq!(migrated.replica, ServiceReplicaKey::new("api", 2).unwrap());
    assert_eq!(migrated.phase, ServicePhase::Creating);
    let similarly_named = store
        .load_observed_state_for_replica("stack-journal", "api-2", 1)
        .unwrap()
        .unwrap();
    assert_eq!(
        similarly_named.replica,
        ServiceReplicaKey::new("api-2", 1).unwrap()
    );
    assert_ne!(migrated.replica, similarly_named.replica);
    let quarantined = store
        .conn
        .prepare(
            "SELECT service_name, replica_index, state_json, reason
             FROM legacy_observed_state_quarantine_v5
             WHERE stack_name = 'stack-journal'
             ORDER BY service_name, replica_index",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(quarantined.len(), 2);
    assert_eq!(quarantined[0].0, "api-2");
    assert_eq!(quarantined[0].1, 0);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&quarantined[0].2).unwrap(),
        decorated_zero
    );
    assert!(quarantined[0].3.contains("replica-zero"));
    assert_eq!(quarantined[1].0, "orphan");
    assert_eq!(quarantined[1].1, 3);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&quarantined[1].2).unwrap(),
        orphan
    );
    assert!(quarantined[1].3.contains("lacks exact journal authority"));
}

#[test]
fn v4_running_without_applied_digest_migrates_and_replans_conservatively() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v4-running-unknown-digest.db");
    let store = create_v4_store(&path);
    let (project, mut intent, binding) = journal_fixture("reservation-legacy-running");
    intent.applied_config_digest = None;
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    store.bind_stack_container_generation(&binding).unwrap();
    let mut running_intent = store
        .load_stack_container_create_intent(&intent.scope.reservation_id)
        .unwrap()
        .unwrap();
    running_intent.status = StackContainerCreateStatus::Running;
    running_intent.updated_at = 102;
    store
        .conn
        .execute(
            "UPDATE stack_container_create_intents
             SET status = 'running', intent_json = ?1, updated_at = 102
             WHERE reservation_id = ?2",
            params![
                serde_json::to_string(&running_intent).unwrap(),
                intent.scope.reservation_id,
            ],
        )
        .unwrap();
    let legacy_running = serde_json::json!({
        "service_name": "web",
        "phase": "Running",
        "container_id": intent.requested_container_id,
        "failed_create_ownership": binding.ownership,
        "ready": true
    });
    store
        .conn
        .execute(
            "UPDATE observed_state SET state_json = ?1
             WHERE stack_name = ?2 AND service_name = 'web' AND replica_index = 1",
            params![legacy_running.to_string(), intent.scope.stack_id],
        )
        .unwrap();

    store.migrate_replica_v4_to_v5().unwrap();
    let migrated = store
        .load_observed_state_for_replica("stack-journal", "web", 1)
        .unwrap()
        .unwrap();
    assert_eq!(migrated.phase, ServicePhase::Running);
    assert_eq!(migrated.applied_config_digest, None);

    let mut desired = sample_spec();
    desired.name = "stack-journal".to_string();
    desired.services.truncate(1);
    let plan = crate::reconcile::plan_apply(&desired, &store, &HashMap::new()).unwrap();
    assert_eq!(plan.actions.len(), 1);
    assert!(matches!(plan.actions[0], Action::ServiceRecreate { .. }));
    assert_eq!(
        plan.actions[0].target(),
        &ServiceReplicaKey::first("web").unwrap()
    );
}

#[test]
fn fresh_v5_journal_intent_without_applied_digest_is_rejected_without_writes() {
    let store = StateStore::in_memory().unwrap();
    let (project, mut intent, _) = journal_fixture("reservation-missing-config");
    intent.applied_config_digest = None;
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    let error = store.begin_stack_container_create(&intent).unwrap_err();
    assert!(error.to_string().contains("requires applied_config_digest"));
    assert!(
        store
            .load_stack_container_create_intent(&intent.scope.reservation_id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_observed_state(&intent.scope.stack_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn observed_state_v5_requires_complete_typed_replica_projection() {
    let store = StateStore::in_memory().unwrap();
    let cases = [
        serde_json::json!({"phase": "Pending", "ready": false}),
        serde_json::json!({"replica": null, "phase": "Pending", "ready": false}),
        serde_json::json!({
            "replica": {"service_name": null, "replica_index": 1},
            "phase": "Pending",
            "ready": false
        }),
        serde_json::json!({
            "replica": {"service_name": "api", "replica_index": null},
            "phase": "Pending",
            "ready": false
        }),
        serde_json::json!({
            "replica": {"service_name": 7, "replica_index": 1},
            "phase": "Pending",
            "ready": false
        }),
        serde_json::json!({
            "replica": {"service_name": "api", "replica_index": "1"},
            "phase": "Pending",
            "ready": false
        }),
    ];
    for (case, state_json) in cases.into_iter().enumerate() {
        let result = store.conn.execute(
            "INSERT INTO observed_state
                (stack_name, service_name, replica_index, state_json)
             VALUES (?1, 'api', 1, ?2)",
            params![format!("invalid-{case}"), state_json.to_string()],
        );
        assert!(result.is_err(), "case {case} unexpectedly passed");
    }

    let valid = ServiceObservedState {
        replica: ServiceReplicaKey::new("api", 1).unwrap(),
        applied_config_digest: None,
        phase: ServicePhase::Pending,
        container_id: None,
        failed_create_ownership: None,
        last_error: None,
        ready: false,
    };
    store.save_observed_state("valid", &valid).unwrap();
    assert_eq!(
        store
            .load_observed_state_for_replica("valid", "api", 1)
            .unwrap(),
        Some(valid)
    );
}

#[test]
fn exact_action_storage_rejects_hash_cursor_and_legacy_corruption() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("exact-action-storage.db");
    let store = StateStore::open(&path).unwrap();
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("exact"),
            target: ServiceReplicaKey::new("api", 2).unwrap(),
        },
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("exact"),
            target: ServiceReplicaKey::new("api-2", 1).unwrap(),
        },
    ];
    store
        .save_reconcile_progress("exact", "op-exact", &actions, 1)
        .unwrap();
    drop(store);
    let store = StateStore::open(&path).unwrap();
    assert_eq!(
        store
            .load_reconcile_progress("exact")
            .unwrap()
            .unwrap()
            .actions,
        actions
    );

    store
        .conn
        .execute(
            "UPDATE reconcile_progress SET actions_hash = 'tampered'
             WHERE stack_name = 'exact'",
            [],
        )
        .unwrap();
    assert!(store.load_reconcile_progress("exact").is_err());
    store
        .conn
        .execute(
            "UPDATE reconcile_progress SET actions_json = ?1, actions_hash = 'legacy'
             WHERE stack_name = 'exact'",
            params![r#"[{"kind":"service_create","service_name":"api"}]"#],
        )
        .unwrap();
    assert!(store.load_reconcile_progress("exact").is_err());

    let session = ReconcileSession {
        session_id: "exact-session".to_string(),
        stack_name: "exact".to_string(),
        operation_id: "op-session".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 1,
        total_actions: actions.len(),
        started_at: 100,
        updated_at: 100,
        completed_at: None,
    };
    store.create_reconcile_session(&session, &actions).unwrap();
    assert_eq!(
        store
            .load_reconcile_session_actions("exact-session")
            .unwrap(),
        actions
    );
    let audit = ReconcileAuditEntry {
        id: 0,
        session_id: "exact-session".to_string(),
        stack_name: "exact".to_string(),
        action_index: 1,
        action_kind: "service_create".to_string(),
        target: ServiceReplicaKey::new("api-2", 1).unwrap(),
        action_hash: crate::reconcile::compute_actions_hash(&[actions[1].clone()]),
        status: "started".to_string(),
        started_at: 101,
        completed_at: None,
        error_message: None,
    };
    store.log_reconcile_action_start(&audit).unwrap();
    drop(store);
    let store = StateStore::open(&path).unwrap();
    assert_eq!(
        store
            .load_reconcile_session_actions("exact-session")
            .unwrap(),
        actions
    );
    assert_eq!(
        store.load_audit_log_for_session("exact-session").unwrap()[0].target,
        ServiceReplicaKey::new("api-2", 1).unwrap()
    );
}

#[test]
fn v4_to_v5_rejects_active_legacy_session_atomically() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v4-active-session.db");
    let store = create_v4_store(&path);
    let actions = r#"[{"kind":"service_create","service_name":"api"}]"#;
    store
        .conn
        .execute(
            "INSERT INTO reconcile_sessions (
            session_id, stack_name, operation_id, status, actions_json, actions_hash,
            next_action_index, total_actions, started_at, updated_at, completed_at
         ) VALUES ('legacy-active', 'legacy-stack', 'op', 'active', ?1, 'legacy',
                   0, 1, 10, 10, NULL)",
            params![actions],
        )
        .unwrap();
    let before = application_schema_snapshot(&store.conn);
    let error = store.migrate_replica_v4_to_v5().unwrap_err().to_string();
    assert!(error.contains("active aggregate reconcile session"));
    assert_eq!(store.schema_version().unwrap(), 4);
    assert_eq!(application_schema_snapshot(&store.conn), before);
}

#[test]
fn v4_to_v5_rejects_inflight_legacy_audit_atomically() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v4-inflight-audit.db");
    let store = create_v4_store(&path);
    store
        .conn
        .execute(
            "INSERT INTO reconcile_audit_log (
            session_id, stack_name, action_index, action_kind, service_name,
            action_hash, status, started_at, completed_at, error_message
         ) VALUES ('legacy', 'legacy-stack', 0, 'service_create', 'api',
                   'legacy', 'started', 10, NULL, NULL)",
            [],
        )
        .unwrap();
    let before = application_schema_snapshot(&store.conn);
    let error = store.migrate_replica_v4_to_v5().unwrap_err().to_string();
    assert!(error.contains("in-flight aggregate action"));
    assert_eq!(store.schema_version().unwrap(), 4);
    assert_eq!(application_schema_snapshot(&store.conn), before);
}

#[test]
fn v4_to_v5_rejects_partial_completed_legacy_session_atomically() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v4-partial-completed-session.db");
    let store = create_v4_store(&path);
    let actions = r#"[{"kind":"service_create","service_name":"api"}]"#;
    store
        .conn
        .execute(
            "INSERT INTO reconcile_sessions (
            session_id, stack_name, operation_id, status, actions_json, actions_hash,
            next_action_index, total_actions, started_at, updated_at, completed_at
         ) VALUES ('legacy-completed', 'legacy-stack', 'op', 'completed', ?1, 'legacy',
                   0, 1, 10, 10, 10)",
            params![actions],
        )
        .unwrap();
    let before = application_schema_snapshot(&store.conn);
    let error = store.migrate_replica_v4_to_v5().unwrap_err().to_string();
    assert!(error.contains("inconsistent metadata"));
    assert_eq!(store.schema_version().unwrap(), 4);
    assert_eq!(application_schema_snapshot(&store.conn), before);
}

#[test]
fn v4_to_v5_quarantines_terminal_legacy_history_and_preserves_namespace_fences() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v4-terminal-history.db");
    let store = create_v4_store(&path);
    let (project, mut scope_fixture, _) = journal_fixture("terminal-history-scope");
    store.save_project_state(&project).unwrap();
    let actions = r#"[{"kind":"service_create","service_name":"api"}]"#;
    store
        .conn
        .execute(
            "INSERT INTO reconcile_sessions (
                session_id, stack_name, operation_id, status, actions_json, actions_hash,
                next_action_index, total_actions, started_at, updated_at, completed_at
             ) VALUES ('legacy-terminal', 'session-only-stack', 'op', 'completed',
                       ?1, 'legacy', 1, 1, 10, 11, 12)",
            params![actions],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO reconcile_audit_log (
                session_id, stack_name, action_index, action_kind, service_name,
                action_hash, status, started_at, completed_at, error_message
             ) VALUES ('orphan-audit', 'audit-only-stack', 0, 'service_remove', 'api',
                       'legacy', 'completed', 20, 21, NULL)",
            [],
        )
        .unwrap();

    store.migrate_replica_v4_to_v5().unwrap();
    assert_eq!(store.schema_version().unwrap(), 5);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM legacy_reconcile_sessions_quarantine_v5
                 WHERE stack_name = 'session-only-stack' AND status = 'completed'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM legacy_reconcile_audit_quarantine_v5
                 WHERE stack_name = 'audit-only-stack' AND status = 'completed'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM reconcile_sessions", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM reconcile_audit_log", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );

    for stack_id in ["session-only-stack", "audit-only-stack"] {
        scope_fixture.scope.stack_id = stack_id.to_string();
        let error = store
            .validate_stack_workload_owner_claim(&workload_scope_for_journal_intent(&scope_fixture))
            .unwrap_err()
            .to_string();
        assert!(error.contains("unowned legacy state"));
    }
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM stack_workload_owners", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
}

#[test]
fn fresh_store_uses_v7_reconcile_claim_schema_and_replica_claim_index() {
    let store = StateStore::in_memory().unwrap();
    assert_eq!(store.schema_version().unwrap(), 9);

    for table in ["reconcile_sessions", "reconcile_progress"] {
        let sql: String = store
            .conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params![table],
                |row| row.get(0),
            )
            .unwrap();
        let normalized = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            normalized.contains("CHECK(action_schema_version = 3)"),
            "unexpected {table} declaration: {normalized}"
        );
    }
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index' AND name = 'reconcile_one_started_replica'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

#[test]
fn v5_to_v6_archives_only_terminal_v2_history() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v5-terminal-reconcile.db");
    let store = create_v5_store(&path);
    let (project, mut scope_fixture, _) = journal_fixture("v6-archive-scope");
    store.save_project_state(&project).unwrap();
    let actions =
        r#"[{"kind":"service_remove","target":{"service_name":"api","replica_index":2}}]"#;
    store
        .conn
        .execute(
            "INSERT INTO reconcile_sessions (
                session_id, stack_name, operation_id, status, action_schema_version,
                actions_json, actions_hash, next_action_index, total_actions,
                started_at, updated_at, completed_at
             ) VALUES ('terminal-v2', 'archive-stack', 'op-v2', 'completed', 2,
                       ?1, 'hash-v2', 1, 1, 10, 11, 12)",
            params![actions],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO reconcile_audit_log (
                session_id, stack_name, action_index, action_kind, service_name,
                replica_index, action_hash, status, started_at, completed_at, error_message
             ) VALUES ('terminal-v2', 'archive-stack', 0, 'service_remove', 'api',
                       2, 'action-hash-v2', 'completed', 10, 12, NULL)",
            [],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO reconcile_progress (
                stack_name, operation_id, action_schema_version, actions_json,
                actions_hash, next_action_index
             ) VALUES ('completed-marker', 'op-complete', 2, ?1, 'hash-v2', 1)",
            params![actions],
        )
        .unwrap();

    store.migrate_reconcile_v5_to_v6().unwrap();

    assert_eq!(store.schema_version().unwrap(), 6);
    let archived_session: (String, i64, String) = store
        .conn
        .query_row(
            "SELECT status, action_schema_version, reason
             FROM legacy_reconcile_sessions_quarantine_v6
             WHERE session_id = 'terminal-v2'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(archived_session.0, "completed");
    assert_eq!(archived_session.1, 2);
    assert!(archived_session.2.contains("schema v2"));
    let archived_audit: (String, i64, String) = store
        .conn
        .query_row(
            "SELECT status, replica_index, reason
             FROM legacy_reconcile_audit_quarantine_v6
             WHERE session_id = 'terminal-v2'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(archived_audit.0, "completed");
    assert_eq!(archived_audit.1, 2);
    assert!(archived_audit.2.contains("schema v2"));
    let archived_progress: (i64, String) = store
        .conn
        .query_row(
            "SELECT action_schema_version, reason
             FROM legacy_reconcile_progress_quarantine_v6
             WHERE stack_name = 'completed-marker'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(archived_progress.0, 2);
    assert!(archived_progress.1.contains("terminal"));
    for table in [
        "reconcile_sessions",
        "reconcile_progress",
        "reconcile_audit_log",
    ] {
        assert_eq!(
            store
                .conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }
    for stack_id in ["archive-stack", "completed-marker"] {
        scope_fixture.scope.stack_id = stack_id.to_string();
        let error = store
            .validate_stack_workload_owner_claim(&workload_scope_for_journal_intent(&scope_fixture))
            .unwrap_err()
            .to_string();
        assert!(error.contains("unowned legacy state"));
    }
}

#[test]
fn v5_to_v6_preflight_refuses_live_v2_state_without_writes() {
    for case in ["active-session", "pending-progress", "started-audit"] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(format!("v5-{case}.db"));
        let store = create_v5_store(&path);
        let actions =
            r#"[{"kind":"service_create","target":{"service_name":"api","replica_index":1}}]"#;
        match case {
            "active-session" => {
                store
                    .conn
                    .execute(
                        "INSERT INTO reconcile_sessions (
                            session_id, stack_name, operation_id, status, action_schema_version,
                            actions_json, actions_hash, next_action_index, total_actions,
                            started_at, updated_at, completed_at
                         ) VALUES ('active-v2', 'live-stack', 'op-v2', 'active', 2,
                                   ?1, 'hash-v2', 0, 1, 10, 10, NULL)",
                        params![actions],
                    )
                    .unwrap();
            }
            "pending-progress" => {
                store
                    .conn
                    .execute(
                        "INSERT INTO reconcile_progress (
                            stack_name, operation_id, action_schema_version, actions_json,
                            actions_hash, next_action_index
                         ) VALUES ('live-stack', 'op-v2', 2, ?1, 'hash-v2', 0)",
                        params![actions],
                    )
                    .unwrap();
            }
            "started-audit" => {
                store
                    .conn
                    .execute(
                        "INSERT INTO reconcile_audit_log (
                            session_id, stack_name, action_index, action_kind, service_name,
                            replica_index, action_hash, status, started_at,
                            completed_at, error_message
                         ) VALUES ('claim-v2', 'live-stack', 0, 'service_create', 'api',
                                   1, 'action-hash-v2', 'started', 10, NULL, NULL)",
                        [],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let schema_before = application_schema_snapshot(&store.conn);

        let error = store.migrate_reconcile_v5_to_v6().unwrap_err().to_string();

        assert!(
            error.contains(case.split('-').next().unwrap()),
            "{case}: {error}"
        );
        assert_eq!(store.schema_version().unwrap(), 5);
        assert_eq!(application_schema_snapshot(&store.conn), schema_before);
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT
                         (SELECT COUNT(*) FROM reconcile_sessions) +
                         (SELECT COUNT(*) FROM reconcile_progress) +
                         (SELECT COUNT(*) FROM reconcile_audit_log)",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE name IN (
                        'legacy_reconcile_sessions_quarantine_v6',
                        'legacy_reconcile_progress_quarantine_v6',
                        'legacy_reconcile_audit_quarantine_v6',
                        'reconcile_one_started_replica'
                     )",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }
}

#[test]
fn v5_to_v6_failpoints_roll_back_then_reopen_and_retry() {
    for (suffix, failpoint) in [
        (
            "archive",
            topology::ReconcileV6MigrationFailpoint::AfterTerminalHistoryArchived,
        ),
        (
            "tables",
            topology::ReconcileV6MigrationFailpoint::AfterDurableActionsRebuilt,
        ),
        (
            "index",
            topology::ReconcileV6MigrationFailpoint::AfterReplicaClaimIndexCreated,
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(format!("v5-to-v6-{suffix}.db"));
        let store = create_v5_store(&path);
        store
            .conn
            .execute(
                "INSERT INTO reconcile_sessions (
                    session_id, stack_name, operation_id, status, action_schema_version,
                    actions_json, actions_hash, next_action_index, total_actions,
                    started_at, updated_at, completed_at
                 ) VALUES ('terminal-v2', 'archive-stack', 'op-v2', 'failed', 2,
                           '[]', 'hash-v2', 0, 0, 10, 11, 12)",
                [],
            )
            .unwrap();
        let before = application_schema_snapshot(&store.conn);

        let error = store
            .migrate_reconcile_v5_to_v6_with_failpoint(failpoint)
            .unwrap_err()
            .to_string();
        assert!(error.contains("injected v5-to-v6 migration failure"));
        assert_eq!(store.schema_version().unwrap(), 5);
        assert_eq!(application_schema_snapshot(&store.conn), before);
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM reconcile_sessions WHERE session_id = 'terminal-v2'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        drop(store);

        let reopened = StateStore::open(&path).unwrap();
        assert_eq!(reopened.schema_version().unwrap(), 9);
        assert_eq!(
            reopened
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM legacy_reconcile_sessions_quarantine_v6
                     WHERE session_id = 'terminal-v2'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }
}

fn downgrade_claim_fixture_to_v6(store: &StateStore) {
    downgrade_teardown_finalizer_fixture_to_v7(store);
    store
        .conn
        .execute_batch(
            "DROP TRIGGER reconcile_session_identity_immutable;
             DROP TRIGGER reconcile_audit_identity_immutable;
             DROP TRIGGER reconcile_started_audit_delete_restricted;",
        )
        .unwrap();
    store.set_schema_version(6).unwrap();
    store.validate_v6_schema().unwrap();
}

fn downgrade_teardown_finalizer_fixture_to_v7(store: &StateStore) {
    store
        .conn
        .execute_batch(
            "DROP TRIGGER teardown_finalizer_delete_restricted;
             DROP TRIGGER teardown_finalizer_receipt_delete_restricted;
             DROP TRIGGER teardown_finalizer_receipt_update_restricted;
             DROP TRIGGER teardown_finalizer_completed_immutable;
             DROP TRIGGER teardown_finalizer_identity_immutable;
             DROP INDEX teardown_one_active_workload;
             DROP INDEX idx_teardown_finalizer_stack;
             DROP TABLE teardown_finalizers;",
        )
        .unwrap();
    store.set_schema_version(7).unwrap();
    store.validate_v7_schema().unwrap();
}

#[test]
fn v7_to_v8_teardown_finalizer_migration_rolls_back_then_reopens() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v7-to-v8-teardown-finalizer.db");
    let store = StateStore::open(&path).unwrap();
    downgrade_teardown_finalizer_fixture_to_v7(&store);
    let before = application_schema_snapshot(&store.conn);

    let error = store
        .migrate_teardown_finalizer_v7_to_v8_with_failpoint(
            TeardownFinalizerV8MigrationFailpoint::AfterFinalizerSchemaCreated,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("injected v7-to-v8 migration failure"));
    assert_eq!(store.schema_version().unwrap(), 7);
    assert_eq!(application_schema_snapshot(&store.conn), before);
    drop(store);

    let reopened = StateStore::open(&path).unwrap();
    assert_eq!(reopened.schema_version().unwrap(), 9);
    reopened.validate_v9_schema().unwrap();
    for object in [
        "teardown_finalizers",
        "teardown_one_active_workload",
        "teardown_finalizer_identity_immutable",
        "teardown_finalizer_completed_immutable",
        "teardown_finalizer_delete_restricted",
    ] {
        assert_eq!(
            reopened
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name = ?1",
                    params![object],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }
}

#[test]
fn v7_to_v8_refuses_legacy_claimed_teardown_without_finalizer_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v7-active-teardown.db");
    let store = StateStore::open(&path).unwrap();
    install_claimed_teardown_batch(&store, "rs-v7-active-teardown");
    downgrade_teardown_finalizer_fixture_to_v7(&store);
    let before = application_schema_snapshot(&store.conn);
    drop(store);

    let error = match StateStore::open(&path) {
        Ok(_) => panic!("v8 migration must reject unreconstructable active teardown evidence"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("without reconstructable finalizer evidence"));
    let raw = Connection::open(&path).unwrap();
    assert_eq!(application_schema_snapshot(&raw), before);
    assert_eq!(
        raw.query_row(
            "SELECT value FROM control_metadata WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        "7"
    );
}

#[test]
fn v7_to_v8_preserves_terminal_claimed_teardown_history() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v7-terminal-teardown-without-receipt.db");
    let store = StateStore::open(&path).unwrap();
    let session_id = "rs-v7-terminal-teardown";
    let (actions, claims, operation_id) = install_claimed_teardown_batch(&store, session_id);
    store
        .commit_claimed_teardown_batch(ClaimedTeardownCommit {
            claims: &claims,
            session_id,
            stack_name: "exact-batch",
            operation_id: &operation_id,
            expected_cursor: 0,
            actions: &actions,
            outcomes: &exact_outcomes(&actions, None),
        })
        .unwrap();
    downgrade_teardown_finalizer_fixture_to_v7(&store);
    drop(store);

    let reopened = StateStore::open(&path).unwrap();
    assert_eq!(reopened.schema_version().unwrap(), 9);
    let session = reopened
        .load_reconcile_session(session_id)
        .unwrap()
        .expect("terminal teardown session must survive migration");
    assert_eq!(session.status, ReconcileSessionStatus::Completed);
}

#[test]
fn v8_to_v9_adds_exact_runtime_identity_projection() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v8-runtime-identity.db");
    let store = create_v8_store(&path);
    assert_eq!(store.schema_version().unwrap(), 8);
    drop(store);

    let reopened = StateStore::open(&path).unwrap();
    assert_eq!(reopened.schema_version().unwrap(), 9);
    reopened.validate_v9_schema().unwrap();
    let column_count: i64 = reopened
        .conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('teardown_finalizers') WHERE name = 'initial_runtime_identity_json'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(column_count, 1);
}

#[test]
fn v8_to_v9_rejects_prepared_teardown_without_exact_runtime_identity() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v8-prepared-runtime-identity.db");
    let store = create_v8_store(&path);
    store
        .conn
        .execute(
            "INSERT INTO teardown_finalizers (
                operation_key, schema_version, request_id, idempotency_key,
                request_digest, session_id, reconcile_operation_id, project_id,
                environment_id, machine_id, machine_incarnation_id, stack_name,
                remove_volumes, changed_actions, actions_hash, desired_state_digest,
                initial_volumes_json, initial_disk_image, initial_runtime_present,
                runtime_shutdown, staged_volumes_json, purged_volumes_json,
                disk_staged, disk_purged, status, receipt_id, finalizer_json,
                created_at, updated_at, completed_at
             ) VALUES (
                'req:legacy-runtime', 1, 'legacy-runtime', NULL,
                'digest', 'session-legacy-runtime', 'teardown:req-legacy-runtime', 'project',
                'environment', 'machine', 'incarnation', 'stack',
                0, 0, 'actions', 'desired',
                '[]', 0, 1,
                0, '[]', '[]',
                0, 0, 'prepared', NULL, '{}',
                1, 1, NULL
             )",
            [],
        )
        .unwrap();
    drop(store);

    let error = match StateStore::open(&path) {
        Ok(_) => panic!("v9 migration must reject an unqualified prepared teardown"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("lacks exact runtime identity evidence"));
    let raw = Connection::open(&path).unwrap();
    assert_eq!(
        raw.query_row(
            "SELECT value FROM control_metadata WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        "8"
    );
    let missing_column: i64 = raw
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('teardown_finalizers') WHERE name = 'initial_runtime_identity_json'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(missing_column, 0, "failed migration must roll back DDL");
}

fn teardown_finalizer_fixture(operation_key: &str) -> TeardownFinalizer {
    let (_, intent, _) = journal_fixture("teardown-finalizer-fixture");
    let scope = workload_scope_for_journal_intent(&intent);
    let initial_runtime_identity =
        Some(vz_runtime_contract::StackRuntimeIdentity::new(scope.stack_id.clone()).unwrap());
    TeardownFinalizer {
        schema_version: TEARDOWN_FINALIZER_SCHEMA_VERSION,
        operation_key: operation_key.to_string(),
        request_id: operation_key
            .strip_prefix("req:")
            .unwrap_or("original-request")
            .to_string(),
        idempotency_key: operation_key.strip_prefix("idem:").map(ToString::to_string),
        request_digest: "vztr3-sha256:fixture".to_string(),
        session_id: format!("rs-{}", operation_key.replace(':', "-")),
        reconcile_operation_id: format!("{CLAIMED_TEARDOWN_OPERATION_PREFIX}original-request"),
        scope,
        remove_volumes: true,
        changed_actions: 2,
        actions_hash: "vza3-sha256:fixture".to_string(),
        desired_state_digest: "vzs1-sha256:fixture".to_string(),
        initial_volumes: vec!["cache".to_string(), "database".to_string()],
        initial_disk_image: true,
        initial_runtime_present: true,
        initial_runtime_identity,
        runtime_shutdown: false,
        staged_volumes: Vec::new(),
        purged_volumes: Vec::new(),
        disk_staged: false,
        disk_purged: false,
        status: TeardownFinalizerStatus::Prepared,
        receipt: None,
        response_json: None,
        created_at: 100,
        updated_at: 100,
        completed_at: None,
    }
}

fn teardown_policy_audit_fixture(receipt_id: &str, request_id: &str, created_at: u64) -> Receipt {
    Receipt {
        receipt_id: receipt_id.to_string(),
        operation: "policy_preflight:remove_container".to_string(),
        entity_id: request_id.to_string(),
        entity_type: "policy".to_string(),
        request_id: request_id.to_string(),
        status: "allow".to_string(),
        created_at,
        metadata: serde_json::json!({"decision": "allow"}),
    }
}

fn completed_teardown_finalizer_fixture(operation_key: &str) -> TeardownFinalizer {
    let mut record = teardown_finalizer_fixture(operation_key);
    let removed_volumes = u32::try_from(record.initial_volumes.len()).unwrap();
    record.runtime_shutdown = true;
    record.staged_volumes = record.initial_volumes.clone();
    record.purged_volumes = record.initial_volumes.clone();
    record.disk_staged = true;
    record.disk_purged = true;
    record.status = TeardownFinalizerStatus::Completed;
    record.updated_at = 101;
    record.completed_at = Some(101);
    record.response_json = Some(
        canonical_teardown_response_json(
            &record.request_id,
            &record.scope.stack_id,
            record.changed_actions,
            removed_volumes,
        )
        .unwrap(),
    );
    record.receipt = Some(Receipt {
        receipt_id: teardown_receipt_id(&record.operation_key, &record.request_digest),
        operation: "teardown_stack".to_string(),
        entity_id: record.scope.stack_id.clone(),
        entity_type: "stack".to_string(),
        request_id: record.request_id.clone(),
        status: "success".to_string(),
        created_at: 101,
        metadata: canonical_teardown_receipt_metadata(
            &record.request_digest,
            record.changed_actions,
            removed_volumes,
        ),
    });
    record
}

#[test]
fn teardown_policy_audit_reservation_replay_and_conflict_are_atomic() {
    let store = StateStore::in_memory().unwrap();
    let original = teardown_finalizer_fixture("req:policy-atomic-original");
    let original_audit =
        teardown_policy_audit_fixture("rcp-policy-original", &original.request_id, 100);

    assert_eq!(
        store
            .reserve_teardown_finalizer_with_policy_audit(&original, &original_audit)
            .unwrap(),
        original
    );
    assert_eq!(
        store.load_receipt(&original_audit.receipt_id).unwrap(),
        Some(original_audit.clone())
    );

    let replay_audit =
        teardown_policy_audit_fixture("rcp-policy-replay", &original.request_id, 101);
    let receipts_before_replay = store.list_receipts().unwrap();
    assert_eq!(
        store
            .reserve_teardown_finalizer_with_policy_audit(&original, &replay_audit)
            .unwrap(),
        original
    );
    assert_eq!(store.list_receipts().unwrap(), receipts_before_replay);
    assert!(
        store
            .load_receipt(&replay_audit.receipt_id)
            .unwrap()
            .is_none()
    );

    let conflicting = teardown_finalizer_fixture("req:policy-atomic-conflict");
    let conflicting_audit =
        teardown_policy_audit_fixture("rcp-policy-conflict", &conflicting.request_id, 102);
    let receipts_before_conflict = store.list_receipts().unwrap();
    let error = store
        .reserve_teardown_finalizer_with_policy_audit(&conflicting, &conflicting_audit)
        .unwrap_err();
    assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
    assert_eq!(store.list_receipts().unwrap(), receipts_before_conflict);
    assert!(
        store
            .load_receipt(&conflicting_audit.receipt_id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_teardown_finalizer(&conflicting.operation_key)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .load_teardown_finalizer(&original.operation_key)
            .unwrap(),
        Some(original)
    );
}

#[test]
fn teardown_policy_audit_failure_rolls_back_finalizer_and_idempotency_claim() {
    let store = StateStore::in_memory().unwrap();
    let record = teardown_finalizer_fixture("idem:policy-audit-rollback");
    let audit = teardown_policy_audit_fixture("rcp-policy-abort", &record.request_id, 100);
    store
        .conn
        .execute_batch(
            "CREATE TEMP TRIGGER abort_policy_audit
             BEFORE INSERT ON receipt_state
             WHEN NEW.receipt_id = 'rcp-policy-abort'
             BEGIN
                 SELECT RAISE(ABORT, 'injected policy audit failure');
             END;",
        )
        .unwrap();

    let error = store
        .reserve_teardown_finalizer_with_policy_audit(&record, &audit)
        .unwrap_err();
    assert!(error.to_string().contains("injected policy audit failure"));
    assert!(
        store
            .load_teardown_finalizer(&record.operation_key)
            .unwrap()
            .is_none()
    );
    assert!(store.load_receipt(&audit.receipt_id).unwrap().is_none());
    assert!(
        store
            .find_idempotency_result(record.idempotency_key.as_deref().unwrap())
            .unwrap()
            .is_none()
    );
}

#[test]
fn teardown_terminal_output_requires_canonical_response_bytes_and_exact_metadata() {
    let store = StateStore::in_memory().unwrap();
    let canonical = completed_teardown_finalizer_fixture("req:canonical-terminal-output");
    let valid_error = store.reserve_teardown_finalizer(&canonical).unwrap_err();
    assert!(
        valid_error
            .to_string()
            .contains("new teardown finalizer must be prepared"),
        "canonical terminal output should pass output validation: {valid_error}"
    );

    let mut noncanonical_response = canonical.clone();
    noncanonical_response.response_json = Some(
        serde_json::to_string_pretty(
            &serde_json::from_str::<serde_json::Value>(
                noncanonical_response.response_json.as_deref().unwrap(),
            )
            .unwrap(),
        )
        .unwrap(),
    );
    assert_ne!(
        noncanonical_response.response_json, canonical.response_json,
        "test fixture must preserve semantics while changing response bytes"
    );
    let error = store
        .reserve_teardown_finalizer(&noncanonical_response)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("completed teardown finalizer output does not match its identity")
    );

    for metadata in [
        serde_json::json!({
            "event_type": "wrong_event",
            "request_digest": canonical.request_digest.clone(),
            "changed_actions": canonical.changed_actions,
            "removed_volumes": canonical.initial_volumes.len(),
        }),
        {
            let mut metadata = canonical.receipt.as_ref().unwrap().metadata.clone();
            metadata
                .as_object_mut()
                .unwrap()
                .insert("unexpected".to_string(), serde_json::json!(true));
            metadata
        },
    ] {
        let mut malformed = canonical.clone();
        malformed.receipt.as_mut().unwrap().metadata = metadata;
        let error = store.reserve_teardown_finalizer(&malformed).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("completed teardown finalizer output does not match its identity")
        );
    }
}

#[test]
fn teardown_finalizer_reservation_replays_exact_identity_and_rejects_key_reuse() {
    let store = StateStore::in_memory().unwrap();
    let record = teardown_finalizer_fixture("req:request-1");
    assert_eq!(store.reserve_teardown_finalizer(&record).unwrap(), record);
    assert_eq!(store.reserve_teardown_finalizer(&record).unwrap(), record);

    let mut conflicting = record.clone();
    conflicting.remove_volumes = false;
    conflicting.initial_volumes.clear();
    conflicting.initial_disk_image = false;
    let error = store.reserve_teardown_finalizer(&conflicting).unwrap_err();
    assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
    assert_eq!(
        store
            .load_teardown_finalizer(&record.operation_key)
            .unwrap(),
        Some(record)
    );

    let active_conflict = teardown_finalizer_fixture("req:request-2");
    assert_eq!(
        store
            .reserve_teardown_finalizer(&active_conflict)
            .unwrap_err()
            .machine_code(),
        MachineErrorCode::StateConflict
    );
}

#[test]
fn teardown_idempotency_key_replays_original_request_and_response_identity() {
    let store = StateStore::in_memory().unwrap();
    let original = teardown_finalizer_fixture("idem:stable-delete-key");
    store.reserve_teardown_finalizer(&original).unwrap();

    let mut retry = original.clone();
    retry.request_id = "transport-retry-request".to_string();
    retry.created_at = 900;
    retry.updated_at = 900;
    let replayed = store.reserve_teardown_finalizer(&retry).unwrap();
    assert_eq!(replayed, original);
    assert_eq!(replayed.request_id, "original-request");
    assert_eq!(store.cleanup_expired_idempotency_keys().unwrap(), 0);
    assert!(
        store
            .find_idempotency_result("stable-delete-key")
            .unwrap()
            .is_some()
    );
    let overwrite = IdempotencyRecord {
        key: "stable-delete-key".to_string(),
        operation: "foreign_operation".to_string(),
        request_hash: "foreign-hash".to_string(),
        response_json: "foreign-response".to_string(),
        status_code: 200,
        created_at: 901,
        expires_at: 902,
    };
    assert_eq!(
        store
            .save_idempotency_result(&overwrite)
            .unwrap_err()
            .machine_code(),
        MachineErrorCode::StateConflict
    );
}

#[test]
fn teardown_finalizer_progress_is_monotonic_and_projection_tamper_fails_closed() {
    let store = StateStore::in_memory().unwrap();
    let mut record = teardown_finalizer_fixture("idem:delete-stack-once");
    store.reserve_teardown_finalizer(&record).unwrap();

    record.runtime_shutdown = true;
    record.staged_volumes.push("cache".to_string());
    record.updated_at = 101;
    store.save_teardown_finalizer_progress(&record).unwrap();
    assert_eq!(
        store
            .load_teardown_finalizer(&record.operation_key)
            .unwrap(),
        Some(record.clone())
    );

    let mut stale = record.clone();
    stale.runtime_shutdown = false;
    stale.updated_at = 102;
    assert_eq!(
        store
            .save_teardown_finalizer_progress(&stale)
            .unwrap_err()
            .machine_code(),
        MachineErrorCode::StateConflict
    );

    store
        .conn
        .execute(
            "UPDATE teardown_finalizers SET runtime_shutdown = 0 WHERE operation_key = ?1",
            params![record.operation_key],
        )
        .unwrap();
    assert!(
        store
            .load_teardown_finalizer(&record.operation_key)
            .unwrap_err()
            .to_string()
            .contains("JSON/projection mismatch")
    );
}

#[test]
fn prepared_teardown_fences_normal_reconcile_after_reservation_crash() {
    let store = StateStore::in_memory().unwrap();
    let actions = exact_batch_actions_for_claim(&store);
    let mut finalizer = teardown_finalizer_fixture("req:crash-after-reservation");
    finalizer.scope = actions[0].precondition().workload().clone();
    finalizer.initial_runtime_identity = Some(
        vz_runtime_contract::StackRuntimeIdentity::new(finalizer.scope.stack_id.clone()).unwrap(),
    );
    finalizer.session_id = "rs-reserved-teardown-owner".to_string();
    finalizer.reconcile_operation_id =
        format!("{CLAIMED_TEARDOWN_OPERATION_PREFIX}reserved-teardown-owner");
    store.reserve_teardown_finalizer(&finalizer).unwrap();

    let session = ReconcileSession {
        session_id: "rs-normal-apply-after-crash".to_string(),
        stack_name: "exact-batch".to_string(),
        operation_id: "normal-apply".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 300,
        updated_at: 300,
        completed_at: None,
    };
    assert_eq!(
        store
            .create_reconcile_batch(&session, &actions)
            .unwrap_err()
            .machine_code(),
        MachineErrorCode::StateConflict
    );
    assert!(
        store
            .load_reconcile_session(&session.session_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn prepared_teardown_fences_desired_state_even_for_zero_action_apply() {
    let store = StateStore::in_memory().unwrap();
    let finalizer = teardown_finalizer_fixture("req:desired-state-fence");
    let mut spec = sample_spec();
    spec.name = finalizer.scope.stack_id.clone();
    store.reserve_teardown_finalizer(&finalizer).unwrap();

    let error = store
        .save_desired_state_unless_prepared_teardown(&finalizer.scope, &spec)
        .unwrap_err();

    assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
    assert!(store.load_desired_state(&spec.name).unwrap().is_none());
}

#[test]
fn prepared_teardown_desired_state_fence_is_exact_scope_only() {
    let store = StateStore::in_memory().unwrap();
    let finalizer = teardown_finalizer_fixture("req:desired-state-sibling");
    let mut sibling = finalizer.scope.clone();
    sibling.machine_id = MachineId::new("mac_sibling").unwrap();
    sibling.machine_incarnation_id = MachineIncarnationId::new("inc_sibling_001").unwrap();
    let mut spec = sample_spec();
    spec.name = sibling.stack_id.clone();
    store.reserve_teardown_finalizer(&finalizer).unwrap();

    store
        .save_desired_state_unless_prepared_teardown(&sibling, &spec)
        .unwrap();

    assert_eq!(store.load_desired_state(&spec.name).unwrap(), Some(spec));
}

#[test]
fn prepared_teardown_does_not_fence_same_stack_name_on_sibling_machine() {
    let store = StateStore::in_memory().unwrap();
    let actions = exact_batch_actions_for_claim(&store);
    let mut sibling = teardown_finalizer_fixture("req:sibling-machine-teardown");
    sibling.scope = actions[0].precondition().workload().clone();
    sibling.scope.machine_id = MachineId::new("mac_sibling").unwrap();
    sibling.scope.machine_incarnation_id = MachineIncarnationId::new("inc_sibling_001").unwrap();
    sibling.initial_runtime_identity = Some(
        vz_runtime_contract::StackRuntimeIdentity::new(sibling.scope.stack_id.clone()).unwrap(),
    );
    sibling.session_id = "rs-sibling-teardown".to_string();
    sibling.reconcile_operation_id = format!("{CLAIMED_TEARDOWN_OPERATION_PREFIX}sibling-teardown");
    store.reserve_teardown_finalizer(&sibling).unwrap();

    let session = ReconcileSession {
        session_id: "rs-original-machine-apply".to_string(),
        stack_name: "exact-batch".to_string(),
        operation_id: "normal-apply".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 301,
        updated_at: 301,
        completed_at: None,
    };
    store.create_reconcile_batch(&session, &actions).unwrap();
    assert!(
        store
            .load_reconcile_session(&session.session_id)
            .unwrap()
            .is_some()
    );
}

#[test]
fn v6_to_v7_claim_migration_rolls_back_then_reopens_with_immutable_identity() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v6-to-v7-claims.db");
    let store = StateStore::open(&path).unwrap();
    downgrade_claim_fixture_to_v6(&store);
    let before = application_schema_snapshot(&store.conn);

    let error = store
        .migrate_claim_v6_to_v7_with_failpoint(
            ClaimV7MigrationFailpoint::AfterImmutabilityGuardsCreated,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("injected v6-to-v7 migration failure"));
    assert_eq!(store.schema_version().unwrap(), 6);
    assert_eq!(application_schema_snapshot(&store.conn), before);
    drop(store);

    let reopened = StateStore::open(&path).unwrap();
    assert_eq!(reopened.schema_version().unwrap(), 9);
    for trigger in [
        "reconcile_session_identity_immutable",
        "reconcile_audit_identity_immutable",
        "reconcile_started_audit_delete_restricted",
    ] {
        assert_eq!(
            reopened
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = ?1",
                    params![trigger],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }
    reopened.validate_v9_schema().unwrap();
}

#[test]
fn v6_to_v7_refuses_untrusted_started_claims_without_writes_across_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v6-started-claim-refused.db");
    let store = StateStore::open(&path).unwrap();
    install_exact_batch(&store, "rs-v6-started-refused");
    downgrade_claim_fixture_to_v6(&store);
    let before = application_schema_snapshot(&store.conn);
    drop(store);

    let error = match StateStore::open(&path) {
        Ok(_) => panic!("v7 migration must refuse untrusted v6 started claims"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("untrusted started reconcile claims")
    );

    let raw = Connection::open(&path).unwrap();
    assert_eq!(
        raw.query_row(
            "SELECT value FROM control_metadata WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        "6"
    );
    assert_eq!(application_schema_snapshot(&raw), before);
    assert_eq!(
        raw.query_row(
            "SELECT COUNT(*) FROM reconcile_audit_log WHERE status = 'started'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        3
    );
}

#[test]
fn v6_to_v7_preserves_effect_free_active_session_and_terminal_history() {
    for terminal in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(format!("v6-valid-{terminal}.db"));
        let store = StateStore::open(&path).unwrap();
        let actions = exact_batch_actions_for_claim(&store);
        let session_id = format!("rs-v6-valid-{terminal}");
        install_unstarted_batch(&store, &session_id, "op-v6-valid", &actions);
        if terminal {
            store
                .start_reconcile_batch(&session_id, "exact-batch", "op-v6-valid", 0, &actions)
                .unwrap();
            store
                .commit_reconcile_batch(
                    &session_id,
                    "exact-batch",
                    "op-v6-valid",
                    0,
                    &actions,
                    &exact_outcomes(&actions, None),
                )
                .unwrap();
        }
        downgrade_claim_fixture_to_v6(&store);
        drop(store);

        let reopened = StateStore::open(&path).unwrap();
        assert_eq!(reopened.schema_version().unwrap(), 9);
        assert_eq!(
            reopened
                .load_reconcile_session_actions(&session_id)
                .unwrap(),
            actions
        );
        if terminal {
            assert_eq!(
                reopened
                    .load_audit_log_for_session(&session_id)
                    .unwrap()
                    .len(),
                3
            );
            assert!(
                reopened
                    .load_reconcile_progress("exact-batch")
                    .unwrap()
                    .is_none()
            );
        } else {
            assert!(
                reopened
                    .load_audit_log_for_session(&session_id)
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                reopened
                    .load_reconcile_progress("exact-batch")
                    .unwrap()
                    .unwrap()
                    .actions,
                actions
            );
        }
    }
}

#[test]
fn v6_to_v7_preserves_terminal_history_beside_one_effect_free_active_session() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v6-mixed-history-active.db");
    let store = StateStore::open(&path).unwrap();
    let actions = exact_batch_actions_for_claim(&store);
    install_unstarted_batch(&store, "rs-v6-terminal", "op-v6-terminal", &actions);
    store
        .start_reconcile_batch(
            "rs-v6-terminal",
            "exact-batch",
            "op-v6-terminal",
            0,
            &actions,
        )
        .unwrap();
    store
        .commit_reconcile_batch(
            "rs-v6-terminal",
            "exact-batch",
            "op-v6-terminal",
            0,
            &actions,
            &exact_outcomes(&actions, None),
        )
        .unwrap();
    install_unstarted_batch(&store, "rs-v6-active", "op-v6-active", &actions);
    downgrade_claim_fixture_to_v6(&store);
    drop(store);

    let reopened = StateStore::open(&path).unwrap();
    assert_eq!(reopened.schema_version().unwrap(), 9);
    assert_eq!(
        reopened
            .load_audit_log_for_session("rs-v6-terminal")
            .unwrap()
            .len(),
        actions.len()
    );
    assert!(
        reopened
            .load_audit_log_for_session("rs-v6-active")
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reopened
            .load_reconcile_progress("exact-batch")
            .unwrap()
            .unwrap()
            .operation_id,
        "op-v6-active"
    );
}

#[test]
fn v6_to_v7_refuses_active_session_with_terminal_audit_without_writes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v6-active-with-effects.db");
    let store = StateStore::open(&path).unwrap();
    let actions = exact_batch_actions_for_claim(&store);
    install_unstarted_batch(&store, "rs-v6-active-effects", "op-v6-effects", &actions);
    store
        .start_reconcile_batch(
            "rs-v6-active-effects",
            "exact-batch",
            "op-v6-effects",
            0,
            &actions,
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE reconcile_audit_log
             SET status = 'completed', completed_at = started_at
             WHERE session_id = 'rs-v6-active-effects'",
            [],
        )
        .unwrap();
    downgrade_claim_fixture_to_v6(&store);
    let before = application_schema_snapshot(&store.conn);
    drop(store);

    let error = match StateStore::open(&path) {
        Ok(_) => panic!("v7 migration must refuse an active session with effects"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("is not effect-free"));
    let raw = Connection::open(&path).unwrap();
    assert_eq!(application_schema_snapshot(&raw), before);
}

#[test]
fn v6_to_v7_refuses_active_nonzero_cursor_without_audit_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v6-active-hidden-effects.db");
    let store = StateStore::open(&path).unwrap();
    let actions = exact_batch_actions_for_claim(&store);
    install_unstarted_batch(&store, "rs-v6-hidden-effects", "op-v6-hidden", &actions);
    store
        .conn
        .execute(
            "UPDATE reconcile_sessions SET next_action_index = 1
             WHERE session_id = 'rs-v6-hidden-effects'",
            [],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE reconcile_progress SET next_action_index = 1
             WHERE stack_name = 'exact-batch'",
            [],
        )
        .unwrap();
    downgrade_claim_fixture_to_v6(&store);
    let before = application_schema_snapshot(&store.conn);
    drop(store);

    let error = match StateStore::open(&path) {
        Ok(_) => panic!("v7 migration must refuse hidden active effects"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("nonzero cursor"));
    let raw = Connection::open(&path).unwrap();
    assert_eq!(application_schema_snapshot(&raw), before);
}

#[test]
fn v6_to_v7_preserves_length_framed_whitespace_ids_for_atomic_claim() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v6-whitespace-identities.db");
    let session_id = "rs v6 whitespace";
    let operation_id = "op v6 whitespace";
    let (actions, digest_before) = {
        let store = StateStore::open(&path).unwrap();
        let actions = exact_batch_actions_for_claim(&store);
        install_unstarted_batch(&store, session_id, operation_id, &actions);
        let digest = crate::reconcile::ReconcileActionExecutionKey::new(
            session_id,
            operation_id,
            0,
            &actions[0],
        )
        .unwrap()
        .activation_digest_prefix()
        .unwrap();
        downgrade_claim_fixture_to_v6(&store);
        (actions, digest)
    };

    let reopened = StateStore::open(&path).unwrap();
    assert_eq!(reopened.schema_version().unwrap(), 9);
    assert_eq!(
        crate::reconcile::ReconcileActionExecutionKey::new(
            session_id,
            operation_id,
            0,
            &actions[0],
        )
        .unwrap()
        .activation_digest_prefix()
        .unwrap(),
        digest_before
    );
    assert_eq!(
        reopened
            .start_reconcile_batch(session_id, "exact-batch", operation_id, 0, &actions,)
            .unwrap()
            .len(),
        actions.len()
    );
}

#[test]
fn v7_reopen_preserves_v3_actions_and_started_claim_uniqueness() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v6-reopen-v3.db");
    let store = StateStore::open(&path).unwrap();
    let (project, intent, _) = journal_fixture("v3-reopen-scope");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    let precondition = crate::reconcile::ReplicaPrecondition::new(
        workload_scope_for_journal_intent(&intent),
        intent.environment_generation,
        crate::reconcile::ExpectedJournalHead::NeverJournaled,
    )
    .unwrap();
    let actions = vec![Action::ServiceCreate {
        target: ServiceReplicaKey::new("api", 1).unwrap(),
        precondition,
    }];
    let session = ReconcileSession {
        session_id: "session-v3".to_string(),
        stack_name: intent.scope.stack_id.clone(),
        operation_id: "op-v3".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 10,
        updated_at: 10,
        completed_at: None,
    };
    store.create_reconcile_batch(&session, &actions).unwrap();
    store
        .start_reconcile_batch(
            &session.session_id,
            &session.stack_name,
            &session.operation_id,
            0,
            &actions,
        )
        .unwrap();
    drop(store);

    let reopened = StateStore::open(&path).unwrap();
    assert_eq!(reopened.schema_version().unwrap(), 9);
    assert_eq!(
        reopened
            .load_reconcile_session_actions(&session.session_id)
            .unwrap(),
        actions
    );
    assert_eq!(
        reopened
            .load_reconcile_progress(&session.stack_name)
            .unwrap()
            .unwrap()
            .actions,
        actions
    );
    let audits = reopened
        .load_audit_log_for_session(&session.session_id)
        .unwrap();
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].target, actions[0].target().clone());
    assert_eq!(audits[0].status, "started");

    let session_tamper = reopened.conn.execute(
        "UPDATE reconcile_sessions
             SET actions_json = ?1
             WHERE session_id = 'session-v3'",
        params![r#"[{"schema_version":3}]"#],
    );
    assert!(
        session_tamper
            .unwrap_err()
            .to_string()
            .contains("immutable")
    );

    let audit_tamper = reopened.conn.execute(
        "UPDATE reconcile_audit_log SET replica_index = 2
             WHERE session_id = 'session-v3'",
        [],
    );
    assert!(audit_tamper.unwrap_err().to_string().contains("immutable"));

    let hash_tamper = reopened.conn.execute(
        "UPDATE reconcile_sessions SET actions_hash = 'tampered'
             WHERE session_id = 'session-v3'",
        [],
    );
    assert!(hash_tamper.unwrap_err().to_string().contains("immutable"));

    let delete_claim = reopened.conn.execute(
        "DELETE FROM reconcile_audit_log WHERE session_id = 'session-v3'",
        [],
    );
    assert!(
        delete_claim
            .unwrap_err()
            .to_string()
            .contains("cannot be deleted")
    );

    let duplicate = reopened.conn.execute(
        "INSERT INTO reconcile_audit_log (
            session_id, stack_name, action_index, action_kind, service_name,
            replica_index, action_hash, status, started_at, completed_at, error_message
         ) VALUES ('foreign-v3', 'stack-journal', 0, 'service_create', 'api',
                   1, 'foreign-action-v3', 'started', 12, NULL, NULL)",
        [],
    );
    assert!(duplicate.is_err());
}

#[test]
fn stack_workload_owner_reservation_is_exact_and_rejects_foreign_machine_scope() {
    let store = StateStore::in_memory().unwrap();
    let project = topology_project_state("prj_stack_owner", &["owner_a", "owner_b"], "/checkout");
    let (first_intent, _) = journal_records_for_environment(
        &project,
        0,
        "reservation-owner-a",
        "global-stack-owner",
        "web",
        "ctr-owner-a",
    );
    let (foreign_intent, _) = journal_records_for_environment(
        &project,
        1,
        "reservation-owner-b",
        "global-stack-owner",
        "web",
        "ctr-owner-b",
    );
    store.save_project_state(&project).unwrap();
    let first_scope = workload_scope_for_journal_intent(&first_intent);
    let owner = store
        .reserve_stack_workload_owner(&first_scope, 100)
        .unwrap();
    assert_eq!(
        store
            .reserve_stack_workload_owner(&first_scope, 999)
            .unwrap(),
        owner,
        "exact replay returns the original immutable timestamp"
    );
    assert_eq!(
        store.validate_stack_workload_owner(&first_scope).unwrap(),
        owner
    );
    assert_eq!(
        store
            .load_stack_workload_owner("global-stack-owner")
            .unwrap(),
        Some(owner)
    );

    let error = store
        .reserve_stack_workload_owner(&workload_scope_for_journal_intent(&foreign_intent), 101)
        .unwrap_err();
    assert!(matches!(
        error,
        StackError::Machine {
            code: MachineErrorCode::StateConflict,
            ..
        }
    ));
}

#[test]
fn stack_workload_owner_claim_rejects_unowned_legacy_namespace_atomically() {
    let store = StateStore::in_memory().unwrap();
    let project = topology_project_state("prj_stack_legacy", &["owner"], "/checkout");
    let (intent, _) = journal_records_for_environment(
        &project,
        0,
        "reservation-legacy-claim",
        "legacy-global-stack",
        "web",
        "ctr-legacy-claim",
    );
    let scope = workload_scope_for_journal_intent(&intent);
    store.save_project_state(&project).unwrap();
    let mut legacy_spec = sample_spec();
    legacy_spec.name = scope.stack_id.clone();
    store
        .save_desired_state(&scope.stack_id, &legacy_spec)
        .unwrap();
    let legacy_observed = ServiceObservedState {
        replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
        applied_config_digest: None,
        phase: ServicePhase::Running,
        container_id: Some("legacy-container".to_string()),
        failed_create_ownership: None,
        last_error: None,
        ready: true,
    };
    store
        .save_observed_state(&scope.stack_id, &legacy_observed)
        .unwrap();

    let prospective = store
        .validate_stack_workload_owner_claim(&scope)
        .unwrap_err();
    assert!(
        prospective
            .to_string()
            .contains("explicit ownership migration")
    );
    let reserve = store.reserve_stack_workload_owner(&scope, 100).unwrap_err();
    assert!(reserve.to_string().contains("explicit ownership migration"));
    assert!(store.begin_stack_container_create(&intent).is_err());
    assert!(
        store
            .resolve_or_begin_stack_container_create(&selector_for_intent(&intent), 100)
            .is_err()
    );
    assert!(
        store
            .load_stack_workload_owner(&scope.stack_id)
            .unwrap()
            .is_none(),
        "a rejected legacy namespace must remain unowned"
    );
    assert_eq!(
        store
            .load_desired_state(&scope.stack_id)
            .unwrap()
            .expect("legacy desired state remains")
            .name,
        scope.stack_id
    );
    assert_eq!(
        store.load_observed_state(&scope.stack_id).unwrap(),
        vec![legacy_observed]
    );
    assert!(
        store
            .load_stack_container_create_intent(&intent.scope.reservation_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn direct_stack_journal_admission_requires_preexisting_exact_owner() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, _) = journal_fixture("reservation-owner-required");
    store.save_project_state(&project).unwrap();

    assert!(store.begin_stack_container_create(&intent).is_err());
    assert!(
        store
            .resolve_or_begin_stack_container_create(&selector_for_intent(&intent), 100)
            .is_err()
    );
    assert!(
        store
            .load_stack_workload_owner(&intent.scope.stack_id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_stack_container_create_intent(&intent.scope.reservation_id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_observed_state(&intent.scope.stack_id)
            .unwrap()
            .is_empty()
    );

    reserve_journal_owner(&store, &intent);
    assert_eq!(store.begin_stack_container_create(&intent).unwrap(), intent);
}

#[test]
fn stack_workload_owner_survives_machine_incarnation_replacement() {
    let store = StateStore::in_memory().unwrap();
    let mut project = topology_project_state("prj_owner_replacement", &["owner"], "/checkout");
    let (intent, _) = journal_records_for_environment(
        &project,
        0,
        "reservation-owner-replacement",
        "stable-stack-owner",
        "web",
        "ctr-owner",
    );
    store.save_project_state(&project).unwrap();
    let old_scope = workload_scope_for_journal_intent(&intent);
    let owner = store.reserve_stack_workload_owner(&old_scope, 100).unwrap();

    let replacement = MachineIncarnationId::new("inc_owner_replacement").unwrap();
    let environment = &mut project.environments[0];
    let incarnation = environment.machines[0].incarnation.as_mut().unwrap();
    incarnation.incarnation_id = replacement.clone();
    incarnation.generation += 1;
    let incarnation_ownership = environment
        .ownership
        .iter_mut()
        .find(|record| record.resource_kind == OwnedResourceKind::Incarnation)
        .unwrap();
    let previous_incarnation_id = incarnation_ownership.resource_id.clone();
    incarnation_ownership.resource_id = replacement.to_string();
    store
        .conn
        .execute(
            "UPDATE topology_ownership SET resource_id = ?1, record_json = ?2
             WHERE resource_kind = ?3 AND resource_id = ?4",
            params![
                incarnation_ownership.resource_id,
                serde_json::to_string(incarnation_ownership).unwrap(),
                serde_json::to_string(&OwnedResourceKind::Incarnation).unwrap(),
                previous_incarnation_id,
            ],
        )
        .unwrap();
    environment.updated_at += 1;
    overwrite_journal_fixture_environment(&store, environment);

    let replacement_scope = vz_runtime_contract::MachineWorkloadScope {
        machine_incarnation_id: replacement,
        ..old_scope.clone()
    };
    assert_eq!(
        store
            .reserve_stack_workload_owner(&replacement_scope, 200)
            .unwrap(),
        owner
    );
    assert_eq!(
        store.validate_stack_workload_owner(&old_scope).unwrap(),
        owner,
        "stable-owner validation remains usable by exact historical cleanup"
    );
}

#[test]
fn stack_workload_owner_projection_drift_fails_closed() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, _) = journal_fixture("reservation-owner-corrupt");
    store.save_project_state(&project).unwrap();
    let scope = workload_scope_for_journal_intent(&intent);
    store.reserve_stack_workload_owner(&scope, 100).unwrap();
    store
        .conn
        .execute_batch("DROP TRIGGER stack_workload_owner_immutable")
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE stack_workload_owners SET project_id = 'prj_corrupt'
             WHERE stack_id = ?1",
            params![scope.stack_id],
        )
        .unwrap();
    let error = store
        .load_stack_workload_owner("stack-journal")
        .unwrap_err()
        .to_string();
    assert!(error.contains("projection"));
    assert!(store.validate_stack_workload_owner(&scope).is_err());
}

#[test]
fn stack_create_intent_exact_replay_and_active_collision_are_stable() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, _) = journal_fixture("reservation-a");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);

    assert_eq!(store.begin_stack_container_create(&intent).unwrap(), intent);
    assert_eq!(store.begin_stack_container_create(&intent).unwrap(), intent);
    assert_eq!(
        store.load_observed_state("stack-journal").unwrap(),
        vec![ServiceObservedState {
            replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
            applied_config_digest: None,
            phase: ServicePhase::Creating,
            container_id: None,
            failed_create_ownership: None,
            last_error: None,
            ready: false,
        }]
    );

    let mut collision = intent.clone();
    collision.scope.reservation_id = "reservation-b".to_string();
    collision.service_generation = 2;
    let error = store.begin_stack_container_create(&collision).unwrap_err();
    assert!(matches!(
        error,
        StackError::Machine {
            code: MachineErrorCode::StateConflict,
            ..
        }
    ));
    assert!(
        store
            .load_stack_container_create_intent("reservation-b")
            .unwrap()
            .is_none()
    );
}

#[test]
fn stack_create_intent_and_creating_observed_state_are_atomic_and_replay_checked() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, _) = journal_fixture("reservation-atomic-begin");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_journal_creating
             BEFORE INSERT ON observed_state
             WHEN NEW.stack_name = 'stack-journal'
             BEGIN SELECT RAISE(ABORT, 'injected observed failure'); END;",
        )
        .unwrap();

    assert!(store.begin_stack_container_create(&intent).is_err());
    assert!(
        store
            .load_stack_container_create_intent("reservation-atomic-begin")
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_stack_workload_owner("stack-journal")
            .unwrap()
            .is_some(),
        "failed journal insertion must not remove the pre-reserved stable owner"
    );
    store
        .conn
        .execute_batch("DROP TRIGGER reject_journal_creating")
        .unwrap();
    store.begin_stack_container_create(&intent).unwrap();
    let generic_error = store
        .save_observed_state_for_replica(
            "stack-journal",
            intent.replica_index,
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web".to_string()).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Pending,
                container_id: None,
                failed_create_ownership: None,
                last_error: None,
                ready: false,
            },
        )
        .unwrap_err()
        .to_string();
    assert!(generic_error.contains("journal-owned"));
    assert_eq!(store.begin_stack_container_create(&intent).unwrap(), intent);
}

#[test]
fn stack_create_intent_rejects_foreign_project_and_stale_incarnation() {
    for mutation in ["project", "incarnation"] {
        let store = StateStore::in_memory().unwrap();
        let (project, mut intent, _) = journal_fixture("reservation-invalid");
        store.save_project_state(&project).unwrap();
        match mutation {
            "project" => {
                intent.scope.project_id = ProjectId::new("prj_foreign").unwrap();
            }
            "incarnation" => {
                intent.scope.machine_incarnation_id =
                    Some(MachineIncarnationId::new("inc_replacement").unwrap());
            }
            _ => unreachable!(),
        }
        let error = store.begin_stack_container_create(&intent).unwrap_err();
        assert!(matches!(
            error,
            StackError::Machine {
                code: MachineErrorCode::StateConflict,
                ..
            }
        ));
        assert!(
            store
                .list_resumable_stack_container_creates()
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn stack_generation_binding_is_exact_immutable_and_resumable() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, binding) = journal_fixture("reservation-bind");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();

    assert_eq!(
        store.bind_stack_container_generation(&binding).unwrap(),
        binding
    );
    let mut retry = binding.clone();
    retry.bound_at = 999;
    assert_eq!(
        store.bind_stack_container_generation(&retry).unwrap(),
        binding
    );
    assert_eq!(
        store.begin_stack_container_create(&intent).unwrap().status,
        StackContainerCreateStatus::Reserved
    );
    let loaded = store
        .load_stack_container_generation_binding("reservation-bind")
        .unwrap();
    assert_eq!(loaded, Some(binding.clone()));
    let resumable = store.list_resumable_stack_container_creates().unwrap();
    assert_eq!(resumable.len(), 1);
    assert_eq!(resumable[0].1, Some(binding));

    assert!(
        store
            .conn
            .execute(
                "UPDATE stack_container_generation_bindings
                 SET runtime_generation = 8 WHERE reservation_id = 'reservation-bind'",
                [],
            )
            .is_err()
    );
    assert!(
        store
            .conn
            .execute(
                "DELETE FROM stack_container_generation_bindings
                 WHERE reservation_id = 'reservation-bind'",
                [],
            )
            .is_err()
    );
}

#[test]
fn stack_generation_binding_uniqueness_is_scoped_to_machine_incarnation() {
    let store = StateStore::in_memory().unwrap();
    let project = topology_project_state(
        "prj_binding_scope",
        &["binding_a", "binding_b"],
        "/checkout",
    );
    let (first_intent, first_binding) = journal_records_for_environment(
        &project,
        0,
        "reservation-binding-a",
        "stack-binding-a",
        "web",
        "same-container-id",
    );
    let (second_intent, second_binding) = journal_records_for_environment(
        &project,
        1,
        "reservation-binding-b",
        "stack-binding-b",
        "web",
        "same-container-id",
    );
    let (third_intent, third_binding) = journal_records_for_environment(
        &project,
        0,
        "reservation-binding-c",
        "stack-binding-c",
        "worker",
        "same-container-id",
    );
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &first_intent);
    reserve_journal_owner(&store, &second_intent);
    reserve_journal_owner(&store, &third_intent);

    store.begin_stack_container_create(&first_intent).unwrap();
    store
        .bind_stack_container_generation(&first_binding)
        .unwrap();
    store.begin_stack_container_create(&second_intent).unwrap();
    store
        .bind_stack_container_generation(&second_binding)
        .expect("private Machine runtimes may issue the same container generation");

    store.begin_stack_container_create(&third_intent).unwrap();
    let error = store
        .bind_stack_container_generation(&third_binding)
        .unwrap_err();
    assert!(matches!(
        error,
        StackError::Machine {
            code: MachineErrorCode::StateConflict,
            ..
        }
    ));
}

#[test]
fn stack_id_namespace_rejects_cross_machine_observed_state_aliases() {
    let store = StateStore::in_memory().unwrap();
    let project = topology_project_state(
        "prj_stack_namespace",
        &["namespace_a", "namespace_b"],
        "/checkout",
    );
    let (first, _) = journal_records_for_environment(
        &project,
        0,
        "reservation-namespace-a",
        "topology-workload-id",
        "web",
        "ctr-a",
    );
    let (second, _) = journal_records_for_environment(
        &project,
        1,
        "reservation-namespace-b",
        "topology-workload-id",
        "worker",
        "ctr-b",
    );
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &first);
    store.begin_stack_container_create(&first).unwrap();

    let error = store.begin_stack_container_create(&second).unwrap_err();
    assert!(matches!(
        error,
        StackError::Machine {
            code: MachineErrorCode::StateConflict,
            ..
        }
    ));
    assert!(
        store
            .load_stack_container_create_intent("reservation-namespace-b")
            .unwrap()
            .is_none()
    );
}

#[test]
fn stack_observed_state_is_replica_qualified_without_aliasing() {
    let store = StateStore::in_memory().unwrap();
    let (project, first, _) = journal_fixture("reservation-replica-1");
    let mut second = first.clone();
    second.scope.reservation_id = "reservation-replica-2".to_string();
    second.replica_index = 2;
    second.requested_container_id = "ctr-journal-web-2".to_string();
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &first);
    store.begin_stack_container_create(&first).unwrap();
    store.begin_stack_container_create(&second).unwrap();

    assert_eq!(store.load_observed_state("stack-journal").unwrap().len(), 2);
    assert_eq!(
        store
            .load_observed_state_for_replica("stack-journal", "web", 1)
            .unwrap()
            .unwrap()
            .phase,
        ServicePhase::Creating
    );
    let generic_error = store
        .save_observed_state_for_replica(
            "stack-journal",
            2,
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::new("web", 2).unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Failed,
                container_id: None,
                failed_create_ownership: None,
                last_error: Some("replica two only".to_string()),
                ready: false,
            },
        )
        .unwrap_err()
        .to_string();
    assert!(generic_error.contains("journal-owned"));
    assert_eq!(
        store
            .load_observed_state_for_replica("stack-journal", "web", 1)
            .unwrap()
            .unwrap()
            .phase,
        ServicePhase::Creating
    );
}

#[test]
fn stack_selector_exact_replay_mismatch_and_terminal_generation_are_stable() {
    let store = StateStore::in_memory().unwrap();
    let (project, fixture, mut selected_binding) = journal_fixture("ignored-selector-reservation");
    store.save_project_state(&project).unwrap();
    let selector = selector_for_intent(&fixture);
    reserve_selector_owner(&store, &selector);
    let (first, binding) = store
        .resolve_or_begin_stack_container_create(&selector, 100)
        .unwrap();
    assert!(binding.is_none());
    assert_eq!(first.service_generation, 1);
    selected_binding.reservation_id = first.scope.reservation_id.clone();
    selected_binding.ownership.scope = Some(Box::new(first.scope.clone()));
    store
        .bind_stack_container_generation(&selected_binding)
        .unwrap();
    let replay = store
        .resolve_or_begin_stack_container_create(&selector, 999)
        .unwrap();
    assert_eq!(replay.0.scope.reservation_id, first.scope.reservation_id);
    assert_eq!(replay.0.status, StackContainerCreateStatus::Reserved);
    assert_eq!(replay.0.created_at, 100);
    assert_eq!(replay.1, Some(selected_binding.clone()));
    let mut mismatch = selector.clone();
    mismatch.action_digest = "sha256:different-action".to_string();
    assert!(
        store
            .resolve_or_begin_stack_container_create(&mismatch, 999)
            .is_err()
    );
    store
        .publish_stack_container_create_failure(
            first.scope.reservation_id.as_str(),
            "terminal before retry",
            102,
        )
        .unwrap();
    store
        .publish_stack_container_cleanup_success(first.scope.reservation_id.as_str(), 103)
        .unwrap();
    let next_selector = selector;
    let (next, next_binding) = store
        .resolve_or_begin_stack_container_create(&next_selector, 104)
        .unwrap();
    assert!(next_binding.is_none());
    assert_eq!(next.service_generation, 2);
    assert_ne!(next.scope.reservation_id, first.scope.reservation_id);
}

#[test]
fn stack_selector_is_concurrent_length_framed_and_overflow_checked() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("selector-concurrent.db");
    let (project, fixture, _) = journal_fixture("ignored-concurrent");
    let selector = selector_for_intent(&fixture);
    let initial_store = StateStore::open(&path).unwrap();
    initial_store.save_project_state(&project).unwrap();
    reserve_selector_owner(&initial_store, &selector);
    drop(initial_store);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let handles = [100_u64, 999]
        .into_iter()
        .map(|now| {
            let path = path.clone();
            let selector = selector.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let store = StateStore::open(&path).unwrap();
                barrier.wait();
                store
                    .resolve_or_begin_stack_container_create(&selector, now)
                    .unwrap()
                    .0
            })
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        results[0].scope.reservation_id,
        results[1].scope.reservation_id
    );
    assert_eq!(results[0].created_at, results[1].created_at);

    let store_a = StateStore::in_memory().unwrap();
    let store_b = StateStore::in_memory().unwrap();
    store_a.save_project_state(&project).unwrap();
    store_b.save_project_state(&project).unwrap();
    let mut framed_a = selector.clone();
    framed_a.stack_id = "ab".to_string();
    framed_a.service_name = "c".to_string();
    let mut framed_b = selector.clone();
    framed_b.stack_id = "a".to_string();
    framed_b.service_name = "bc".to_string();
    reserve_selector_owner(&store_a, &framed_a);
    reserve_selector_owner(&store_b, &framed_b);
    let id_a = store_a
        .resolve_or_begin_stack_container_create(&framed_a, 100)
        .unwrap()
        .0
        .scope
        .reservation_id;
    let id_b = store_b
        .resolve_or_begin_stack_container_create(&framed_b, 100)
        .unwrap()
        .0
        .scope
        .reservation_id;
    assert_ne!(id_a, id_b);
    assert!(id_a.starts_with("vzscr1-sha256:"));

    let overflow_store = StateStore::in_memory().unwrap();
    overflow_store.save_project_state(&project).unwrap();
    let mut maximum = fixture;
    maximum.scope.reservation_id = "reservation-max-generation".to_string();
    maximum.service_generation = i64::MAX as u64;
    reserve_journal_owner(&overflow_store, &maximum);
    overflow_store
        .begin_stack_container_create(&maximum)
        .unwrap();
    overflow_store
        .publish_stack_container_create_failure(
            maximum.scope.reservation_id.as_str(),
            "terminal maximum",
            101,
        )
        .unwrap();
    assert!(
        overflow_store
            .resolve_or_begin_stack_container_create(&selector, 102)
            .unwrap_err()
            .to_string()
            .contains("generation overflow")
    );
}

#[test]
fn stack_generation_binding_rejects_foreign_scope_without_partial_write() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, mut binding) = journal_fixture("reservation-foreign-bind");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    binding.ownership.scope.as_mut().unwrap().environment_id =
        EnvironmentId::new("env_foreign").unwrap();

    let error = store.bind_stack_container_generation(&binding).unwrap_err();
    assert!(matches!(
        error,
        StackError::Machine {
            code: MachineErrorCode::StateConflict,
            ..
        }
    ));
    assert!(
        store
            .load_stack_container_generation_binding("reservation-foreign-bind")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .load_stack_container_create_intent("reservation-foreign-bind")
            .unwrap()
            .unwrap()
            .status,
        StackContainerCreateStatus::Intent
    );
}

#[test]
fn stack_activation_replays_reject_stopped_or_reincarnated_machine_scope() {
    for mutation in ["stopped", "reincarnated"] {
        let store = StateStore::in_memory().unwrap();
        let (mut project, intent, binding) =
            journal_fixture(&format!("reservation-stale-{mutation}"));
        store.save_project_state(&project).unwrap();
        reserve_journal_owner(&store, &intent);
        store.begin_stack_container_create(&intent).unwrap();
        store.bind_stack_container_generation(&binding).unwrap();
        let observed_before = store.load_observed_state("stack-journal").unwrap();

        let environment = &mut project.environments[0];
        environment.updated_at += 1;
        match mutation {
            "stopped" => {
                environment.lifecycle_generation += 1;
                environment.state = EnvironmentState::Stopped;
                environment.machines[0].state = MachineState::Stopped;
            }
            "reincarnated" => {
                let incarnation = environment.machines[0].incarnation.as_mut().unwrap();
                incarnation.incarnation_id =
                    MachineIncarnationId::new("inc_journal_replacement").unwrap();
                incarnation.generation += 1;
            }
            _ => unreachable!(),
        }
        overwrite_journal_fixture_environment(&store, environment);

        assert!(store.begin_stack_container_create(&intent).is_err());
        assert!(store.bind_stack_container_generation(&binding).is_err());
        assert!(store.list_resumable_stack_container_creates().is_err());
        assert!(
            store
                .publish_stack_container_create_success(
                    intent.scope.reservation_id.as_str(),
                    true,
                    102,
                )
                .is_err()
        );
        assert!(
            store
                .publish_stack_container_create_failure(
                    intent.scope.reservation_id.as_str(),
                    "late stale failure",
                    102,
                )
                .is_err()
        );
        assert_eq!(
            store.load_observed_state("stack-journal").unwrap(),
            observed_before
        );
        assert_eq!(
            store
                .load_stack_container_create_intent(intent.scope.reservation_id.as_str())
                .unwrap()
                .unwrap()
                .status,
            StackContainerCreateStatus::Reserved
        );
        if mutation == "reincarnated" {
            let stopping = store
                .begin_stack_container_cleanup(intent.scope.reservation_id.as_str(), 103)
                .expect("stale exact binding remains cleanup authority");
            assert_eq!(stopping.phase, ServicePhase::Stopping);
            assert_eq!(
                stopping.failed_create_ownership,
                Some(binding.ownership.clone())
            );
            store
                .publish_stack_container_cleanup_success(intent.scope.reservation_id.as_str(), 104)
                .unwrap();
            assert_eq!(
                store
                    .load_stack_container_create_intent(intent.scope.reservation_id.as_str())
                    .unwrap()
                    .unwrap()
                    .status,
                StackContainerCreateStatus::Cleaned
            );
        }
    }
}

#[test]
fn stale_unbound_runtime_ownership_binds_cleanup_only_and_replays_exactly() {
    let store = StateStore::in_memory().unwrap();
    let (mut project, intent, binding) = journal_fixture("reservation-cleanup-only-bind");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();

    let current_error = store
        .bind_stack_container_generation_for_cleanup(&binding)
        .unwrap_err();
    assert!(current_error.to_string().contains("still current"));
    assert!(
        store
            .load_stack_container_generation_binding(&binding.reservation_id)
            .unwrap()
            .is_none()
    );

    let environment = &mut project.environments[0];
    environment.updated_at += 1;
    let incarnation = environment.machines[0].incarnation.as_mut().unwrap();
    incarnation.incarnation_id = MachineIncarnationId::new("inc_cleanup_only_new").unwrap();
    incarnation.generation += 1;
    overwrite_journal_fixture_environment(&store, environment);

    assert!(store.bind_stack_container_generation(&binding).is_err());
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_cleanup_only_observed
             BEFORE UPDATE OF state_json ON observed_state
             WHEN NEW.stack_name = 'stack-journal' AND NEW.state_json LIKE '%\"Stopping\"%'
             BEGIN SELECT RAISE(ABORT, 'injected cleanup-only failure'); END;",
        )
        .unwrap();
    assert!(
        store
            .bind_stack_container_generation_for_cleanup(&binding)
            .is_err()
    );
    assert!(
        store
            .load_stack_container_generation_binding(&binding.reservation_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .load_stack_container_create_intent(&binding.reservation_id)
            .unwrap()
            .unwrap()
            .status,
        StackContainerCreateStatus::Intent
    );
    store
        .conn
        .execute_batch("DROP TRIGGER reject_cleanup_only_observed")
        .unwrap();
    assert_eq!(
        store
            .bind_stack_container_generation_for_cleanup(&binding)
            .unwrap(),
        binding
    );
    let pending = store
        .load_stack_container_create_intent(&binding.reservation_id)
        .unwrap()
        .unwrap();
    assert_eq!(pending.status, StackContainerCreateStatus::CleanupPending);
    assert_eq!(pending.updated_at, binding.bound_at);
    let observed = store
        .load_observed_state_for_replica(
            &intent.scope.stack_id,
            &intent.service_name,
            intent.replica_index,
        )
        .unwrap()
        .unwrap();
    assert_eq!(observed.phase, ServicePhase::Stopping);
    assert_eq!(
        observed.failed_create_ownership,
        Some(binding.ownership.clone())
    );

    let mut retry = binding.clone();
    retry.bound_at = 999;
    assert_eq!(
        store
            .bind_stack_container_generation_for_cleanup(&retry)
            .unwrap(),
        binding
    );
    assert!(
        store
            .publish_stack_container_create_success(&binding.reservation_id, true, 1000)
            .is_err()
    );
    assert!(
        store
            .publish_stack_container_create_failure(
                &binding.reservation_id,
                "late create failure",
                1000,
            )
            .is_err()
    );

    store
        .publish_stack_container_cleanup_success(&binding.reservation_id, 102)
        .unwrap();
    assert!(
        store
            .require_no_nonterminal_stack_container_creates("env_journal")
            .is_ok()
    );
}

#[test]
fn blocked_unbound_runtime_ownership_can_bind_only_for_cleanup() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, mut binding) = journal_fixture("reservation-blocked-cleanup-bind");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    store
        .publish_stack_container_blocked(
            &intent.scope.reservation_id,
            "runtime outcome requires operator cleanup",
            102,
        )
        .unwrap();
    binding.bound_at = 103;

    assert_eq!(
        store
            .bind_stack_container_generation_for_cleanup(&binding)
            .unwrap(),
        binding
    );
    assert_eq!(
        store
            .load_stack_container_create_intent(&binding.reservation_id)
            .unwrap()
            .unwrap()
            .status,
        StackContainerCreateStatus::CleanupPending
    );
    assert!(store.bind_stack_container_generation(&binding).is_err());
    store
        .publish_stack_container_cleanup_success(&binding.reservation_id, 104)
        .unwrap();
}

#[test]
fn blocked_journal_recovery_rejects_every_observed_authority_tamper() {
    for bound in [false, true] {
        for field in ["name", "container", "ownership", "error", "ready"] {
            let store = StateStore::in_memory().unwrap();
            let reservation = format!("reservation-blocked-tamper-{bound}-{field}");
            let (project, mut intent, mut binding) = journal_fixture(&reservation);
            intent.replica_index = 2;
            intent.requested_container_id = format!("ctr-blocked-tamper-{bound}-{field}");
            binding.ownership.container_id = intent.requested_container_id.clone();
            store.save_project_state(&project).unwrap();
            reserve_journal_owner(&store, &intent);
            store.begin_stack_container_create(&intent).unwrap();
            if bound {
                store.bind_stack_container_generation(&binding).unwrap();
            }
            let mut observed = store
                .publish_stack_container_blocked(
                    &intent.scope.reservation_id,
                    "runtime ownership is uncertain",
                    102,
                )
                .unwrap();
            match field {
                "name" => observed.replica.service_name = "web-foreign-replica".to_string(),
                "container" => {
                    observed.container_id = if bound {
                        Some("foreign-container".to_string())
                    } else {
                        Some(intent.requested_container_id.clone())
                    };
                }
                "ownership" => {
                    let mut foreign = binding.ownership.clone();
                    foreign.generation += 1;
                    observed.failed_create_ownership = Some(foreign);
                }
                "error" => observed.last_error = Some("different error".to_string()),
                "ready" => observed.ready = true,
                _ => unreachable!(),
            }
            let tamper = store.conn.execute(
                "UPDATE observed_state SET state_json = ?1
                     WHERE stack_name = ?2 AND service_name = ?3 AND replica_index = ?4",
                params![
                    serde_json::to_string(&observed).unwrap(),
                    intent.scope.stack_id,
                    intent.service_name,
                    intent.replica_index,
                ],
            );
            if field == "name" {
                assert!(
                    tamper.is_err(),
                    "SQL/JSON identity constraint must reject name tamper"
                );
                continue;
            }
            tamper.unwrap();

            assert!(
                store
                    .list_stack_container_recovery_records()
                    .unwrap_err()
                    .to_string()
                    .contains("observed state"),
                "bound={bound} field={field} must fail recovery closed"
            );
            assert!(
                store
                    .publish_stack_container_blocked(
                        &intent.scope.reservation_id,
                        "runtime ownership is uncertain",
                        999,
                    )
                    .is_err(),
                "bound={bound} field={field} must fail exact replay"
            );
            if bound {
                assert!(
                    store
                        .begin_stack_container_cleanup(&intent.scope.reservation_id, 999)
                        .is_err()
                );
            } else {
                assert!(
                    store
                        .abandon_stale_stack_container_create(
                            &intent.scope.reservation_id,
                            "abandon corrupted blocked intent",
                            999,
                        )
                        .is_err()
                );
            }
        }
    }
}

#[test]
fn stack_new_admission_requires_ready_environment_and_machine() {
    let environment_states = [
        EnvironmentState::Reconciling,
        EnvironmentState::Stopped,
        EnvironmentState::Deleting,
    ];
    for state in environment_states {
        let store = StateStore::in_memory().unwrap();
        let (mut project, mut intent, _) =
            journal_fixture(&format!("reservation-environment-{state:?}"));
        if matches!(
            state,
            EnvironmentState::Reconciling | EnvironmentState::Deleting
        ) {
            project.environments[0].lifecycle_generation = 1;
            intent.environment_generation = 1;
        }
        store.save_project_state(&project).unwrap();
        let environment = &mut project.environments[0];
        environment.state = state;
        if matches!(
            state,
            EnvironmentState::Reconciling | EnvironmentState::Deleting
        ) {
            environment.active_operation_id = Some(LifecycleOperationId::generate());
        }
        if state == EnvironmentState::Stopped {
            environment.machines[0].state = MachineState::Stopped;
        }
        environment.updated_at += 1;
        overwrite_journal_fixture_environment(&store, environment);

        let error = store.begin_stack_container_create(&intent).unwrap_err();
        assert!(error.to_string().contains("Environment"));
        assert!(error.to_string().contains("not runnable"));
        assert!(
            store
                .resolve_or_begin_stack_container_create(&selector_for_intent(&intent), 101)
                .is_err()
        );
        assert!(
            store
                .load_stack_container_create_intent(intent.scope.reservation_id.as_str())
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .load_observed_state("stack-journal")
                .unwrap()
                .is_empty()
        );
    }

    for state in [
        MachineState::Creating,
        MachineState::Stopped,
        MachineState::Failed,
    ] {
        let store = StateStore::in_memory().unwrap();
        let (mut project, intent, _) = journal_fixture(&format!("reservation-machine-{state:?}"));
        store.save_project_state(&project).unwrap();
        let environment = &mut project.environments[0];
        environment.machines[0].state = state;
        environment.updated_at += 1;
        overwrite_journal_fixture_environment(&store, environment);

        let error = store.begin_stack_container_create(&intent).unwrap_err();
        assert!(
            error.to_string().contains("Machine")
                || error.to_string().contains("Ready requires every Machine")
        );
        assert!(
            store
                .resolve_or_begin_stack_container_create(&selector_for_intent(&intent), 101)
                .is_err()
        );
        assert!(
            store
                .load_stack_container_create_intent(intent.scope.reservation_id.as_str())
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .load_observed_state("stack-journal")
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn stack_activation_rejects_non_runnable_current_generation_but_cleanup_remains_possible() {
    for mode in [
        "environment_deleting",
        "environment_stopped",
        "machine_stopped",
    ] {
        let store = StateStore::in_memory().unwrap();
        let (mut project, mut intent, binding) =
            journal_fixture(&format!("reservation-current-state-{mode}"));
        if mode == "environment_deleting" {
            project.environments[0].lifecycle_generation = 1;
            intent.environment_generation = 1;
        }
        store.save_project_state(&project).unwrap();
        reserve_journal_owner(&store, &intent);
        store.begin_stack_container_create(&intent).unwrap();
        store.bind_stack_container_generation(&binding).unwrap();
        let observed_before = store.load_observed_state("stack-journal").unwrap();

        let environment = &mut project.environments[0];
        environment.updated_at += 1;
        match mode {
            "environment_deleting" => {
                environment.state = EnvironmentState::Deleting;
                environment.active_operation_id = Some(LifecycleOperationId::generate());
            }
            "environment_stopped" => {
                environment.state = EnvironmentState::Stopped;
                environment.machines[0].state = MachineState::Stopped;
            }
            "machine_stopped" => environment.machines[0].state = MachineState::Stopped,
            _ => unreachable!(),
        }
        // Keep the definition, lifecycle generation, and Machine incarnation exact:
        // lifecycle state alone must fence activation and replay.
        overwrite_journal_fixture_environment(&store, environment);

        assert!(store.begin_stack_container_create(&intent).is_err());
        assert!(store.bind_stack_container_generation(&binding).is_err());
        assert!(store.list_resumable_stack_container_creates().is_err());
        assert!(
            store
                .publish_stack_container_create_success(
                    intent.scope.reservation_id.as_str(),
                    true,
                    102,
                )
                .is_err()
        );
        assert!(
            store
                .publish_stack_container_create_failure(
                    intent.scope.reservation_id.as_str(),
                    "late failure from non-runnable topology",
                    102,
                )
                .is_err()
        );
        assert_eq!(
            store.load_observed_state("stack-journal").unwrap(),
            observed_before
        );

        let recovery = store.list_stack_container_recovery_records().unwrap();
        assert_eq!(recovery.len(), 1);
        assert!(matches!(
            recovery[0].disposition,
            StackContainerRecoveryDisposition::CleanupOnly { .. }
        ));
        store
            .begin_stack_container_cleanup(intent.scope.reservation_id.as_str(), 103)
            .unwrap();
        store
            .publish_stack_container_cleanup_success(intent.scope.reservation_id.as_str(), 104)
            .unwrap();
        assert!(
            store
                .require_no_nonterminal_stack_container_creates("env_journal")
                .is_ok()
        );
    }
}

#[test]
fn stale_recovery_discovery_survives_reopen_for_cleanup_and_abandonment() {
    for bound in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp
            .path()
            .join(if bound { "bound.db" } else { "intent.db" });
        let reservation = if bound {
            "reservation-reopen-bound"
        } else {
            "reservation-reopen-intent"
        };
        let (mut project, intent, binding) = journal_fixture(reservation);
        {
            let store = StateStore::open(&path).unwrap();
            store.save_project_state(&project).unwrap();
            reserve_journal_owner(&store, &intent);
            store.begin_stack_container_create(&intent).unwrap();
            if bound {
                store.bind_stack_container_generation(&binding).unwrap();
            }
            let environment = &mut project.environments[0];
            environment.updated_at += 1;
            let incarnation = environment.machines[0].incarnation.as_mut().unwrap();
            incarnation.incarnation_id =
                MachineIncarnationId::new(format!("inc_reopen_{bound}")).unwrap();
            incarnation.generation += 1;
            overwrite_journal_fixture_environment(&store, environment);
        }

        let reopened = StateStore::open(&path).unwrap();
        let recovery = reopened.list_stack_container_recovery_records().unwrap();
        assert_eq!(recovery.len(), 1);
        if bound {
            assert!(matches!(
                recovery[0].disposition,
                StackContainerRecoveryDisposition::CleanupOnly { .. }
            ));
            reopened
                .begin_stack_container_cleanup(reservation, 103)
                .unwrap();
            reopened
                .publish_stack_container_cleanup_success(reservation, 104)
                .unwrap();
        } else {
            assert!(matches!(
                recovery[0].disposition,
                StackContainerRecoveryDisposition::Abandonable { .. }
            ));
            let abandoned = reopened
                .abandon_stale_stack_container_create(reservation, "stale intent", 103)
                .unwrap();
            assert_eq!(abandoned.phase, ServicePhase::Failed);
            assert_eq!(
                reopened
                    .abandon_stale_stack_container_create(reservation, "stale intent", 999)
                    .unwrap(),
                abandoned
            );
        }
        assert!(
            reopened
                .require_no_nonterminal_stack_container_creates("env_journal")
                .is_ok()
        );
        assert!(
            reopened
                .list_stack_container_recovery_records()
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn blocked_recovery_is_executable_after_reopen_and_clears_deletion_fence() {
    for bound in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(if bound {
            "blocked-bound.db"
        } else {
            "blocked-unbound.db"
        });
        let reservation = if bound {
            "reservation-blocked-bound"
        } else {
            "reservation-blocked-unbound"
        };
        let (mut project, intent, binding) = journal_fixture(reservation);
        {
            let store = StateStore::open(&path).unwrap();
            store.save_project_state(&project).unwrap();
            reserve_journal_owner(&store, &intent);
            store.begin_stack_container_create(&intent).unwrap();
            if bound {
                store.bind_stack_container_generation(&binding).unwrap();
            }
            store
                .publish_stack_container_blocked(reservation, "recovery cannot adopt", 102)
                .unwrap();
            if !bound {
                let environment = &mut project.environments[0];
                environment.state = EnvironmentState::Deleting;
                environment.updated_at += 1;
                overwrite_journal_fixture_environment(&store, environment);
            }
        }

        let reopened = StateStore::open(&path).unwrap();
        assert!(
            reopened
                .require_no_nonterminal_stack_container_creates("env_journal")
                .is_err()
        );
        let recovery = reopened.list_stack_container_recovery_records().unwrap();
        assert_eq!(recovery.len(), 1);
        if bound {
            assert!(matches!(
                recovery[0].disposition,
                StackContainerRecoveryDisposition::CleanupOnly { .. }
            ));
            let stopping = reopened
                .begin_stack_container_cleanup(reservation, 103)
                .unwrap();
            assert_eq!(stopping.phase, ServicePhase::Stopping);
            assert_eq!(stopping.failed_create_ownership, Some(binding.ownership));
            reopened
                .publish_stack_container_cleanup_success(reservation, 104)
                .unwrap();
        } else {
            assert!(matches!(
                recovery[0].disposition,
                StackContainerRecoveryDisposition::Abandonable { .. }
            ));
            let abandoned = reopened
                .abandon_stale_stack_container_create(
                    reservation,
                    "blocked without runtime ownership",
                    103,
                )
                .unwrap();
            assert_eq!(abandoned.phase, ServicePhase::Failed);
            assert!(abandoned.failed_create_ownership.is_none());
            assert_eq!(
                reopened
                    .abandon_stale_stack_container_create(
                        reservation,
                        "blocked without runtime ownership",
                        999,
                    )
                    .unwrap(),
                abandoned
            );
        }
        assert!(
            reopened
                .require_no_nonterminal_stack_container_creates("env_journal")
                .is_ok()
        );
        assert!(
            reopened
                .list_stack_container_recovery_records()
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn machine_workload_recovery_discovers_old_incarnations_without_sibling_leakage() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("machine-workload-recovery.db");
    let mut project =
        topology_project_state("prj_recovery_history", &["journal", "sibling"], "/checkout");
    let (old_bound, binding) = journal_records_for_environment(
        &project,
        0,
        "reservation-old-bound",
        "stack-incarnation-stable",
        "web",
        "ctr-old-web",
    );
    let (old_unbound, _) = journal_records_for_environment(
        &project,
        0,
        "reservation-old-unbound",
        "stack-incarnation-stable",
        "worker",
        "ctr-old-worker",
    );
    let (sibling, _) = journal_records_for_environment(
        &project,
        1,
        "reservation-sibling",
        "stack-sibling",
        "web",
        "ctr-sibling-web",
    );
    let current_incarnation = MachineIncarnationId::new("inc_journal_reopened").unwrap();
    {
        let store = StateStore::open(&path).unwrap();
        store.save_project_state(&project).unwrap();
        reserve_journal_owner(&store, &old_bound);
        reserve_journal_owner(&store, &sibling);
        store.begin_stack_container_create(&old_bound).unwrap();
        store.bind_stack_container_generation(&binding).unwrap();
        store.begin_stack_container_create(&old_unbound).unwrap();
        store.begin_stack_container_create(&sibling).unwrap();

        let environment = &mut project.environments[0];
        let incarnation = environment.machines[0].incarnation.as_mut().unwrap();
        incarnation.incarnation_id = current_incarnation.clone();
        incarnation.generation += 1;
        let incarnation_ownership = environment
            .ownership
            .iter_mut()
            .find(|record| record.resource_kind == OwnedResourceKind::Incarnation)
            .unwrap();
        let previous_incarnation_id = incarnation_ownership.resource_id.clone();
        incarnation_ownership.resource_id = current_incarnation.to_string();
        store
            .conn
            .execute(
                "UPDATE topology_ownership SET resource_id = ?1, record_json = ?2
                 WHERE resource_kind = ?3 AND resource_id = ?4",
                params![
                    incarnation_ownership.resource_id,
                    serde_json::to_string(incarnation_ownership).unwrap(),
                    serde_json::to_string(&OwnedResourceKind::Incarnation).unwrap(),
                    previous_incarnation_id,
                ],
            )
            .unwrap();
        environment.updated_at += 1;
        overwrite_journal_fixture_environment(&store, environment);
    }

    let reopened = StateStore::open(&path).unwrap();
    let scope = vz_runtime_contract::MachineWorkloadScope {
        schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
        project_id: old_bound.scope.project_id.clone(),
        environment_id: old_bound.scope.environment_id.clone(),
        machine_id: old_bound.scope.machine_id.clone(),
        machine_incarnation_id: current_incarnation.clone(),
        stack_id: old_bound.scope.stack_id.clone(),
    };
    let recovery = reopened
        .list_stack_container_recovery_records_for_machine_workload(&scope)
        .unwrap();
    assert_eq!(recovery.len(), 2);
    assert_eq!(
        recovery
            .iter()
            .map(|record| record.intent.scope.reservation_id.as_str())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["reservation-old-bound", "reservation-old-unbound",])
    );
    assert!(recovery.iter().any(|record| {
        record.intent.scope.reservation_id == "reservation-old-bound"
            && matches!(
                record.disposition,
                StackContainerRecoveryDisposition::CleanupOnly { .. }
            )
    }));
    assert!(recovery.iter().any(|record| {
        record.intent.scope.reservation_id == "reservation-old-unbound"
            && matches!(
                record.disposition,
                StackContainerRecoveryDisposition::Abandonable { .. }
            )
    }));
    assert_eq!(
        reopened
            .list_stack_container_recovery_records_for_machine_workload(
                &vz_runtime_contract::MachineWorkloadScope {
                    schema_version: vz_runtime_contract::MACHINE_WORKLOAD_SCOPE_SCHEMA_VERSION,
                    project_id: sibling.scope.project_id.clone(),
                    environment_id: sibling.scope.environment_id.clone(),
                    machine_id: sibling.scope.machine_id.clone(),
                    machine_incarnation_id: sibling.scope.machine_incarnation_id.clone().unwrap(),
                    stack_id: sibling.scope.stack_id.clone(),
                },
            )
            .unwrap()
            .len(),
        1
    );

    reopened
        .begin_stack_container_cleanup("reservation-old-bound", 103)
        .unwrap();
    reopened
        .publish_stack_container_cleanup_success("reservation-old-bound", 104)
        .unwrap();
    reopened
        .abandon_stale_stack_container_create(
            "reservation-old-unbound",
            "old incarnation has no runtime ownership",
            103,
        )
        .unwrap();
    assert!(
        reopened
            .require_no_nonterminal_stack_container_creates("env_journal")
            .is_ok()
    );

    let mut next_selector = selector_for_intent(&old_bound);
    next_selector.machine_incarnation_id = current_incarnation;
    let (next, next_binding) = reopened
        .resolve_or_begin_stack_container_create(&next_selector, 105)
        .unwrap();
    assert!(next_binding.is_none());
    assert_eq!(next.service_generation, 2);
    assert_ne!(
        next.scope.machine_incarnation_id,
        old_bound.scope.machine_incarnation_id
    );
}

#[test]
fn stack_v4_schema_refresh_replaces_incarnation_scoped_history_guards() {
    let store = StateStore::in_memory().unwrap();
    store
        .conn
        .execute_batch(
            "DROP TRIGGER stack_container_create_stack_scope_guard;
             CREATE TRIGGER stack_container_create_stack_scope_guard
             BEFORE INSERT ON stack_container_create_intents
             WHEN EXISTS (
                 SELECT 1 FROM stack_container_create_intents existing
                 WHERE existing.stack_id = NEW.stack_id
                   AND existing.machine_incarnation_id <> NEW.machine_incarnation_id
             )
             BEGIN
                 SELECT RAISE(ABORT, 'legacy incarnation-scoped stack guard');
             END;
             DROP INDEX idx_stack_create_one_active_service;
             CREATE UNIQUE INDEX idx_stack_create_one_active_service
             ON stack_container_create_intents(
                 machine_incarnation_id, stack_id, service_name, replica_index
             ) WHERE status IN ('intent', 'reserved', 'running', 'cleanup_pending', 'blocked');",
        )
        .unwrap();

    store.create_stack_journal_schema_v4().unwrap();
    let trigger_sql: String = store
        .conn
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'trigger' AND name = 'stack_container_create_stack_scope_guard'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!trigger_sql.contains("machine_incarnation_id"));
    let index_sql: String = store
        .conn
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'index' AND name = 'idx_stack_create_one_active_service'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(index_sql.contains("project_id"));
    assert!(!index_sql.contains("machine_incarnation_id"));
    store.validate_v9_schema().unwrap();
}

#[test]
fn stack_create_success_publishes_binding_and_observed_state_atomically() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, binding) = journal_fixture("reservation-success");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    assert!(
        store
            .publish_stack_container_ready(
                &ServiceReplicaKey::first("web").unwrap(),
                &binding.ownership,
            )
            .is_err()
    );
    store.bind_stack_container_generation(&binding).unwrap();

    let failed = store
        .publish_stack_container_create_success("reservation-success", true, 99)
        .unwrap_err();
    assert!(failed.to_string().contains("updated_at precedes"));
    assert!(
        store
            .load_observed_state("stack-journal")
            .unwrap()
            .iter()
            .all(|state| state.phase == ServicePhase::Creating)
    );
    assert_eq!(
        store
            .load_stack_container_create_intent("reservation-success")
            .unwrap()
            .unwrap()
            .status,
        StackContainerCreateStatus::Reserved
    );

    let observed = store
        .publish_stack_container_create_success("reservation-success", true, 102)
        .unwrap();
    assert_eq!(observed.phase, ServicePhase::Running);
    assert_eq!(observed.failed_create_ownership, Some(binding.ownership));
    assert_eq!(
        store.load_observed_state("stack-journal").unwrap(),
        vec![observed]
    );
    assert_eq!(
        store
            .list_resumable_stack_container_creates()
            .unwrap()
            .len(),
        1
    );

    let journal_before = store
        .load_stack_container_create_intent("reservation-success")
        .unwrap()
        .unwrap();
    let observed_before = store.load_observed_state("stack-journal").unwrap();
    assert!(
        store
            .publish_stack_container_create_failure(
                "reservation-success",
                "delayed create error",
                103,
            )
            .is_err()
    );
    assert_eq!(
        store
            .load_stack_container_create_intent("reservation-success")
            .unwrap(),
        Some(journal_before)
    );
    assert_eq!(
        store.load_observed_state("stack-journal").unwrap(),
        observed_before
    );
}

#[test]
fn journal_readiness_update_is_exact_idempotent_and_survives_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("journal-readiness.db");
    let mut store = StateStore::open(&path).unwrap();
    let (event_tx, event_rx) = std::sync::mpsc::channel();
    store.set_event_sender(event_tx);
    let (project, intent, binding) = journal_fixture("reservation-readiness");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    store.bind_stack_container_generation(&binding).unwrap();
    let running = store
        .publish_stack_container_create_success(&intent.scope.reservation_id, false, 102)
        .unwrap();
    assert!(!running.ready);
    assert!(store.load_events("stack-journal").unwrap().is_empty());
    assert_eq!(
        event_rx.try_recv().unwrap_err(),
        std::sync::mpsc::TryRecvError::Empty,
        "Running with ready=false must not emit or stream ServiceReady"
    );
    let target = ServiceReplicaKey::new(&intent.service_name, intent.replica_index).unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_readiness_event
             BEFORE INSERT ON events
             BEGIN SELECT RAISE(ABORT, 'injected readiness event failure'); END;",
        )
        .unwrap();
    assert!(
        store
            .publish_stack_container_ready(&target, &binding.ownership)
            .is_err()
    );
    assert!(
        !store
            .load_observed_state_for_replica("stack-journal", "web", 1)
            .unwrap()
            .unwrap()
            .ready,
        "failed event persistence must roll readiness back"
    );
    assert!(store.load_events("stack-journal").unwrap().is_empty());
    assert_eq!(
        event_rx.try_recv().unwrap_err(),
        std::sync::mpsc::TryRecvError::Empty,
        "rolled-back readiness must not notify subscribers"
    );
    store
        .conn
        .execute_batch("DROP TRIGGER reject_readiness_event;")
        .unwrap();
    let ready = store
        .publish_stack_container_ready(&target, &binding.ownership)
        .unwrap();
    assert!(ready.ready);
    let ready_event = StackEvent::ServiceReady {
        stack_name: "stack-journal".to_string(),
        service_name: "web".to_string(),
        runtime_id: binding.ownership.container_id.clone(),
    };
    assert_eq!(
        store.load_events("stack-journal").unwrap(),
        vec![ready_event.clone()]
    );
    assert_eq!(event_rx.try_recv().unwrap(), ready_event.clone());
    assert_eq!(
        store
            .publish_stack_container_ready(&target, &binding.ownership)
            .unwrap(),
        ready
    );
    assert_eq!(
        store.load_events("stack-journal").unwrap(),
        vec![ready_event.clone()]
    );
    assert_eq!(
        event_rx.try_recv().unwrap_err(),
        std::sync::mpsc::TryRecvError::Empty,
        "readiness replay must not duplicate its durable or live event"
    );
    assert!(
        store
            .publish_stack_container_ready(
                &ServiceReplicaKey::new("web", 2).unwrap(),
                &binding.ownership,
            )
            .is_err()
    );
    let mut wrong_ownership = binding.ownership.clone();
    wrong_ownership.generation += 1;
    assert!(
        store
            .publish_stack_container_ready(&target, &wrong_ownership)
            .is_err()
    );
    drop(store);

    let reopened = StateStore::open(&path).unwrap();
    assert!(
        reopened
            .load_observed_state_for_replica("stack-journal", "web", 1)
            .unwrap()
            .unwrap()
            .ready
    );
    assert_eq!(
        reopened.load_events("stack-journal").unwrap(),
        vec![ready_event],
        "ready state and its single durable event must survive reopen together"
    );
}

#[test]
fn stack_create_failure_atomically_retains_bound_cleanup_authority() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, binding) = journal_fixture("reservation-failure");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    store.bind_stack_container_generation(&binding).unwrap();

    let observed = store
        .publish_stack_container_create_failure("reservation-failure", "setup failed", 102)
        .unwrap();
    assert_eq!(observed.phase, ServicePhase::Failed);
    assert_eq!(observed.failed_create_ownership, Some(binding.ownership));
    assert_eq!(
        store
            .load_stack_container_create_intent("reservation-failure")
            .unwrap()
            .unwrap()
            .status,
        StackContainerCreateStatus::CleanupPending
    );
}

#[test]
fn stack_cleanup_transitions_are_atomic_replayable_and_clear_the_fence() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, binding) = journal_fixture("reservation-cleanup");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    store.bind_stack_container_generation(&binding).unwrap();
    store
        .publish_stack_container_create_success("reservation-cleanup", true, 102)
        .unwrap();
    assert!(
        store
            .require_no_nonterminal_stack_container_creates("env_journal")
            .is_err()
    );

    let stopping = store
        .begin_stack_container_cleanup("reservation-cleanup", 103)
        .unwrap();
    assert_eq!(stopping.phase, ServicePhase::Stopping);
    assert_eq!(
        store
            .begin_stack_container_cleanup("reservation-cleanup", 999)
            .unwrap(),
        stopping
    );
    assert_eq!(
        store
            .load_stack_container_create_intent("reservation-cleanup")
            .unwrap()
            .unwrap()
            .updated_at,
        103
    );

    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_journal_stopped
             BEFORE UPDATE OF state_json ON observed_state
             WHEN NEW.stack_name = 'stack-journal' AND NEW.state_json LIKE '%\"Stopped\"%'
             BEGIN SELECT RAISE(ABORT, 'injected stopped failure'); END;",
        )
        .unwrap();
    assert!(
        store
            .publish_stack_container_cleanup_success("reservation-cleanup", 104)
            .is_err()
    );
    assert_eq!(
        store
            .load_stack_container_create_intent("reservation-cleanup")
            .unwrap()
            .unwrap()
            .status,
        StackContainerCreateStatus::CleanupPending
    );
    store
        .conn
        .execute_batch("DROP TRIGGER reject_journal_stopped")
        .unwrap();

    let stopped = store
        .publish_stack_container_cleanup_success("reservation-cleanup", 104)
        .unwrap();
    assert_eq!(stopped.phase, ServicePhase::Stopped);
    assert_eq!(
        store
            .publish_stack_container_cleanup_success("reservation-cleanup", 999)
            .unwrap(),
        stopped
    );
    let cleaned = store
        .load_stack_container_create_intent("reservation-cleanup")
        .unwrap()
        .unwrap();
    assert_eq!(cleaned.status, StackContainerCreateStatus::Cleaned);
    assert_eq!(cleaned.updated_at, 104);
    assert!(
        store
            .require_no_nonterminal_stack_container_creates("env_journal")
            .is_ok()
    );

    let generic_error = store
        .save_observed_state_for_replica(
            "stack-journal",
            intent.replica_index,
            &ServiceObservedState {
                replica: crate::state_store::ServiceReplicaKey::first("web").unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Pending,
                container_id: None,
                failed_create_ownership: None,
                last_error: None,
                ready: false,
            },
        )
        .unwrap_err()
        .to_string();
    assert!(generic_error.contains("journal-owned"));
    assert!(store.begin_stack_container_create(&intent).is_ok());
    assert!(
        store
            .publish_stack_container_cleanup_success("reservation-cleanup", 999)
            .is_ok()
    );
}

#[test]
fn malformed_stack_intent_projection_fails_closed() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, _) = journal_fixture("reservation-malformed");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    let mut json = serde_json::to_value(&intent).unwrap();
    json["service_name"] = serde_json::Value::String("different".to_string());
    store
        .conn
        .execute(
            "UPDATE stack_container_create_intents SET intent_json = ?1
             WHERE reservation_id = 'reservation-malformed'",
            params![serde_json::to_string(&json).unwrap()],
        )
        .unwrap();

    let error = store
        .load_stack_container_create_intent("reservation-malformed")
        .unwrap_err()
        .to_string();
    assert!(error.contains("mismatched `service_name` projection"));
}

#[test]
fn malformed_stack_binding_projection_fails_closed() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, binding) = journal_fixture("reservation-malformed-binding");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    store.bind_stack_container_generation(&binding).unwrap();
    store
        .conn
        .execute_batch(
            "DROP TRIGGER stack_container_generation_binding_immutable;
             UPDATE stack_container_generation_bindings
             SET project_id = 'prj_foreign'
             WHERE reservation_id = 'reservation-malformed-binding';",
        )
        .unwrap();

    let error = store
        .load_stack_container_generation_binding("reservation-malformed-binding")
        .unwrap_err()
        .to_string();
    assert!(error.contains("mismatched `project_id` projection"));
}

#[test]
fn malformed_stack_journal_projection_cannot_bypass_environment_delete_fence() {
    let store = StateStore::in_memory().unwrap();
    let (project, intent, _) = journal_fixture("reservation-malformed-fence");
    store.save_project_state(&project).unwrap();
    reserve_journal_owner(&store, &intent);
    store.begin_stack_container_create(&intent).unwrap();
    store
        .conn
        .execute_batch(
            "DROP TRIGGER stack_container_create_intent_immutable;
             UPDATE stack_container_create_intents
             SET environment_id = 'env_foreign', status = 'failed', completed_at = updated_at
             WHERE reservation_id = 'reservation-malformed-fence';",
        )
        .unwrap();

    let error = store
        .require_no_nonterminal_stack_container_creates("env_journal")
        .unwrap_err()
        .to_string();
    assert!(error.contains("mismatched `environment_id` projection"));
}

/// Serialize and deserialize a checkpoint through the state store,
/// verifying no data loss in the round trip.
#[test]
fn checkpoint_format_round_trip_stability() {
    let store = StateStore::in_memory().unwrap();

    let original = Checkpoint {
        checkpoint_id: "ckpt-roundtrip-001".to_string(),
        sandbox_id: "sbx-roundtrip".to_string(),
        parent_checkpoint_id: Some("ckpt-parent-000".to_string()),
        class: CheckpointClass::VmFull,
        state: CheckpointState::Ready,
        created_at: 1_700_100_200,
        compatibility_fingerprint: "fp-sha256-deadbeef".to_string(),
    };

    store.save_checkpoint(&original).unwrap();
    let loaded = store
        .load_checkpoint("ckpt-roundtrip-001")
        .unwrap()
        .unwrap();

    assert_eq!(loaded.checkpoint_id, original.checkpoint_id);
    assert_eq!(loaded.sandbox_id, original.sandbox_id);
    assert_eq!(loaded.parent_checkpoint_id, original.parent_checkpoint_id);
    assert_eq!(loaded.class, original.class);
    assert_eq!(loaded.state, original.state);
    assert_eq!(loaded.created_at, original.created_at);
    assert_eq!(
        loaded.compatibility_fingerprint,
        original.compatibility_fingerprint
    );

    // Also test FsQuick class with no parent.
    let original_fs = Checkpoint {
        checkpoint_id: "ckpt-fs-001".to_string(),
        sandbox_id: "sbx-fs".to_string(),
        parent_checkpoint_id: None,
        class: CheckpointClass::FsQuick,
        state: CheckpointState::Creating,
        created_at: 1_700_200_300,
        compatibility_fingerprint: "fp-sha256-cafebabe".to_string(),
    };
    store.save_checkpoint(&original_fs).unwrap();
    let loaded_fs = store.load_checkpoint("ckpt-fs-001").unwrap().unwrap();

    assert_eq!(loaded_fs.checkpoint_id, original_fs.checkpoint_id);
    assert_eq!(loaded_fs.sandbox_id, original_fs.sandbox_id);
    assert_eq!(loaded_fs.parent_checkpoint_id, None);
    assert_eq!(loaded_fs.class, CheckpointClass::FsQuick);
    assert_eq!(loaded_fs.state, CheckpointState::Creating);
}

/// Verify that old event JSON formats (v1 tagged enums) can still be
/// deserialized after code evolution. This guards against accidental
/// serde tag or field renames.
#[test]
fn event_format_backward_compat() {
    // These are the canonical v1 JSON shapes — if serde(rename) or
    // serde(tag) attributes change, this test will catch it.
    let v1_event_jsons = vec![
        r#"{"type":"stack_apply_started","stack_name":"app","services_count":2}"#,
        r#"{"type":"stack_apply_completed","stack_name":"app","succeeded":2,"failed":0}"#,
        r#"{"type":"stack_apply_failed","stack_name":"app","error":"boom"}"#,
        r#"{"type":"service_creating","stack_name":"app","service_name":"web"}"#,
        r#"{"type":"service_ready","stack_name":"app","service_name":"web","runtime_id":"ctr-001"}"#,
        r#"{"type":"service_stopped","stack_name":"app","service_name":"web","exit_code":0}"#,
        r#"{"type":"service_failed","stack_name":"app","service_name":"web","error":"crash"}"#,
        r#"{"type":"stack_destroyed","stack_name":"app"}"#,
    ];

    for (i, json_str) in v1_event_jsons.iter().enumerate() {
        let parsed: Result<StackEvent, _> = serde_json::from_str(json_str);
        assert!(
            parsed.is_ok(),
            "v1 event JSON at index {i} failed to deserialize: {} — input: {json_str}",
            parsed.unwrap_err()
        );

        // Re-serialize and re-deserialize to verify stability.
        let re_serialized = serde_json::to_string(&parsed.unwrap()).unwrap();
        let re_parsed: StackEvent = serde_json::from_str(&re_serialized).unwrap();
        let _ = re_parsed; // Just verify it doesn't panic.
    }

    // Also verify that events stored in the DB can be loaded back.
    let store = StateStore::in_memory().unwrap();
    for json_str in &v1_event_jsons {
        // Directly insert raw JSON into the events table to simulate
        // events written by an older version.
        store
            .conn
            .execute(
                "INSERT INTO events (stack_name, event_json) VALUES ('compat', ?1)",
                params![*json_str],
            )
            .unwrap();
    }
    let loaded = store.load_events("compat").unwrap();
    assert_eq!(loaded.len(), v1_event_jsons.len());
}

#[test]
fn with_immediate_transaction_rolls_back_on_error() {
    let store = StateStore::in_memory().unwrap();

    let _: Result<(), StackError> = store.with_immediate_transaction(|tx| {
        let sandbox = Sandbox {
            sandbox_id: "sbx-rollback".to_string(),
            backend: SandboxBackend::MacosVz,
            spec: SandboxSpec::default(),
            state: SandboxState::Ready,
            created_at: 1,
            updated_at: 1,
            labels: std::collections::BTreeMap::new(),
        };
        tx.save_sandbox(&sandbox)?;
        Err(StackError::InvalidSpec("force rollback".to_string()))
    });

    assert!(store.load_sandbox("sbx-rollback").unwrap().is_none());
}

#[test]
fn daemon_pragmas_busy_timeout_waits_through_write_lock_contention() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("state.db");

    let contender_store =
        StateStore::open_with_pragmas(&db_path, StateStorePragmas::daemon_defaults()).unwrap();

    let (lock_started_tx, lock_started_rx) = std::sync::mpsc::channel();
    let db_path_for_lock_holder = db_path.clone();
    let lock_holder = std::thread::spawn(move || {
        let lock_holder_store = StateStore::open_with_pragmas(
            &db_path_for_lock_holder,
            StateStorePragmas::daemon_defaults(),
        )
        .unwrap();

        lock_holder_store
            .with_immediate_transaction(|tx| {
                tx.save_sandbox(&Sandbox {
                    sandbox_id: "sbx-lock-holder".to_string(),
                    backend: SandboxBackend::MacosVz,
                    spec: SandboxSpec::default(),
                    state: SandboxState::Ready,
                    created_at: 1,
                    updated_at: 1,
                    labels: std::collections::BTreeMap::new(),
                })?;

                lock_started_tx.send(()).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(300));
                Ok(())
            })
            .unwrap();
    });

    lock_started_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("lock holder should enter transaction");

    let start = std::time::Instant::now();
    contender_store
        .with_immediate_transaction(|tx| {
            tx.save_sandbox(&Sandbox {
                sandbox_id: "sbx-contender".to_string(),
                backend: SandboxBackend::MacosVz,
                spec: SandboxSpec::default(),
                state: SandboxState::Ready,
                created_at: 2,
                updated_at: 2,
                labels: std::collections::BTreeMap::new(),
            })?;
            Ok(())
        })
        .unwrap();
    let elapsed = start.elapsed();

    lock_holder.join().expect("lock holder thread should join");

    assert!(
        elapsed >= std::time::Duration::from_millis(200),
        "contender transaction should wait for lock release (elapsed={elapsed:?})"
    );
    assert!(
        contender_store
            .load_sandbox("sbx-lock-holder")
            .unwrap()
            .is_some()
    );
    assert!(
        contender_store
            .load_sandbox("sbx-contender")
            .unwrap()
            .is_some()
    );
}

#[test]
fn workspace_binding_refresh_preserves_identity_and_resources_across_relocation() {
    let store = StateStore::in_memory().unwrap();
    let original = topology_project_state("prj_relocated", &["agent"], "/old/checkout");
    let original_environment = &original.environments[0];
    let original_binding_id = original_environment.bindings[0].binding_id.clone();
    let original_machines = original_environment.machines.clone();
    let original_ownership = original_environment.ownership.clone();
    store.save_project_state(&original).unwrap();

    let mut requested = original_environment.bindings[0].clone();
    requested.binding_id = WorkspaceBindingId::generate();
    requested.path_hint = Some("/new/location/checkout".to_string());
    let refreshed = store.refresh_workspace_binding(&requested, 300).unwrap();

    assert_eq!(refreshed.binding_id, original_binding_id);
    assert_eq!(refreshed.workspace_key, "same-worktree-key");
    assert_eq!(refreshed.path_hint, requested.path_hint);
    let actual = store.load_project_state("prj_relocated").unwrap().unwrap();
    let actual_environment = &actual.environments[0];
    assert_eq!(actual_environment.bindings, vec![refreshed]);
    assert_eq!(actual_environment.machines, original_machines);
    assert_eq!(actual_environment.ownership, original_ownership);
    assert_eq!(
        actual_environment.created_at,
        original_environment.created_at
    );
    assert_eq!(actual_environment.updated_at, 300);
}

#[test]
fn topology_create_and_binding_mutations_never_rewrite_active_sibling_rows() {
    let store = StateStore::in_memory().unwrap();
    let state = topology_project_state("prj_narrow_topology", &["active", "ready"], "/checkout");
    let definition = state.definition.clone();
    let ready_binding = state.environments[1].bindings[0].clone();
    store.save_project_state(&state).unwrap();
    let active = store
        .begin_environment_lifecycle(
            "env_active",
            EnvironmentLifecycleKind::Stop,
            "req-active-sibling",
            "idem-active-sibling",
            "sha256:active-sibling",
            100,
        )
        .unwrap();
    let active_row_before: (String, String, i64, Option<String>) = store
        .conn
        .query_row(
            "SELECT state, instance_json, lifecycle_generation, active_operation_id
             FROM environment_instances WHERE environment_id = 'env_active'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER forbid_active_sibling_update
             BEFORE UPDATE ON environment_instances
             WHEN OLD.environment_id = 'env_active'
             BEGIN SELECT RAISE(ABORT, 'active sibling update forbidden'); END;
             CREATE TRIGGER forbid_active_sibling_delete
             BEFORE DELETE ON environment_instances
             WHEN OLD.environment_id = 'env_active'
             BEGIN SELECT RAISE(ABORT, 'active sibling delete forbidden'); END;
             CREATE TRIGGER forbid_active_owner_update
             BEFORE UPDATE ON topology_ownership
             WHEN OLD.environment_id = 'env_active'
             BEGIN SELECT RAISE(ABORT, 'active owner update forbidden'); END;
             CREATE TRIGGER forbid_active_owner_delete
             BEFORE DELETE ON topology_ownership
             WHEN OLD.environment_id = 'env_active'
             BEGIN SELECT RAISE(ABORT, 'active owner delete forbidden'); END;",
        )
        .unwrap();

    let EnvironmentUpReservation::Created {
        environment: created,
    } = store
        .resolve_or_reserve_environment_for_up(
            &definition,
            &EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::Name("created".to_string())),
                ..EnvironmentSelectionContext::default()
            },
            200,
        )
        .unwrap()
    else {
        panic!("explicit missing name must create an Environment")
    };
    let reserved = WorkspaceBinding {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        binding_id: WorkspaceBindingId::generate(),
        project_id: definition.project_id.clone(),
        environment_id: created.environment_id,
        name: "workspace".to_string(),
        workspace_key: "narrow-created-workspace".to_string(),
        path_hint: Some("/created".to_string()),
    };
    store
        .reserve_workspace_binding_for_environment(&reserved, 201)
        .unwrap();

    let mut refreshed = ready_binding;
    refreshed.workspace_key = "narrow-ready-workspace".to_string();
    refreshed.path_hint = Some("/relocated".to_string());
    store.refresh_workspace_binding(&refreshed, 202).unwrap();

    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state, instance_json, lifecycle_generation, active_operation_id
                 FROM environment_instances WHERE environment_id = 'env_active'",
                [],
                |row| Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?
                )),
            )
            .unwrap(),
        active_row_before
    );
    assert_eq!(
        store
            .load_current_environment_lifecycle("env_active")
            .unwrap(),
        Some(active)
    );
}

#[test]
fn creating_environment_can_reserve_declared_workspace_before_reconciliation() {
    let store = StateStore::in_memory().unwrap();
    let definition = topology_project_state("prj_pre_reconcile", &["fixture"], "/x").definition;
    let created = store
        .resolve_or_reserve_environment_for_up(
            &definition,
            &EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::Name("agent".to_string())),
                ..EnvironmentSelectionContext::default()
            },
            100,
        )
        .unwrap();
    let environment = match created {
        EnvironmentUpReservation::Created { environment } => environment,
        EnvironmentUpReservation::Existing { .. } => panic!("expected a new Environment"),
    };
    assert_eq!(environment.state, EnvironmentState::Creating);
    assert!(environment.bindings.is_empty());

    let requested = WorkspaceBinding {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        binding_id: WorkspaceBindingId::generate(),
        project_id: definition.project_id.clone(),
        environment_id: environment.environment_id.clone(),
        name: "workspace".to_string(),
        workspace_key: "opaque-worktree-token".to_string(),
        path_hint: Some("/diagnostic/checkout".to_string()),
    };
    assert_eq!(
        store
            .reserve_workspace_binding_for_environment(&requested, 200)
            .unwrap(),
        requested
    );
    assert_eq!(
        store
            .reserve_workspace_binding_for_environment(&requested, 999)
            .unwrap(),
        requested
    );

    let persisted = store
        .load_project_state(definition.project_id.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(persisted.environments[0].state, EnvironmentState::Creating);
    assert_eq!(persisted.environments[0].bindings, vec![requested]);
    assert_eq!(persisted.environments[0].updated_at, 200);
}

#[test]
fn workspace_binding_refresh_rejects_non_ready_environment_without_writes() {
    let store = StateStore::in_memory().unwrap();
    let mut original = topology_project_state("prj_stopped", &["agent"], "/checkout");
    original.environments[0].state = EnvironmentState::Stopped;
    original.environments[0].machines[0].state = MachineState::Stopped;
    store.save_project_state(&original).unwrap();
    let mut requested = original.environments[0].bindings[0].clone();
    requested.path_hint = Some("/moved".to_string());

    let error = store
        .refresh_workspace_binding(&requested, 300)
        .expect_err("stopped Environment must reject binding refresh");
    assert!(error.to_string().contains("must be ready"));
    assert_eq!(
        store.load_project_state("prj_stopped").unwrap(),
        Some(original)
    );
}

#[test]
fn owned_resource_reservation_is_idempotent_and_foreign_collision_rolls_back() {
    let store = StateStore::in_memory().unwrap();
    let original = topology_project_state("prj_resources", &["agent-a", "agent-b"], "/checkout");
    store.save_project_state(&original).unwrap();
    let first_environment = &original.environments[0];
    let first_machine = &first_environment.machines[0];
    let reservation = OwnershipRecord {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        resource_kind: OwnedResourceKind::Disk,
        resource_id: "vzr1-shared-looking-key".to_string(),
        environment_id: first_environment.environment_id.clone(),
        machine_id: Some(first_machine.machine_id.clone()),
    };

    assert_eq!(
        store.reserve_owned_resource(&reservation, 300).unwrap(),
        reservation
    );
    assert_eq!(
        store.reserve_owned_resource(&reservation, 999).unwrap(),
        reservation
    );
    let after_idempotent = store.load_project_state("prj_resources").unwrap().unwrap();
    assert_eq!(after_idempotent.environments[0].updated_at, 300);
    assert_eq!(
        after_idempotent.environments[0]
            .ownership
            .iter()
            .filter(|record| record.resource_id == reservation.resource_id)
            .count(),
        1
    );

    let second_environment = &after_idempotent.environments[1];
    let foreign = OwnershipRecord {
        environment_id: second_environment.environment_id.clone(),
        machine_id: Some(second_environment.machines[0].machine_id.clone()),
        ..reservation.clone()
    };
    let error = store
        .reserve_owned_resource(&foreign, 500)
        .expect_err("foreign owner must not adopt a reserved resource");
    assert!(matches!(error, StackError::OwnedResourceCollision(_)));
    assert_eq!(
        store.load_project_state("prj_resources").unwrap(),
        Some(after_idempotent)
    );
}

#[test]
fn exact_owned_resource_requirement_is_read_only_for_live_states_and_reopen() {
    let ready_store = StateStore::in_memory().unwrap();
    let ready = topology_project_state("prj_require_ready", &["ready"], "/checkout");
    let ready_ownership = ready.environments[0]
        .ownership
        .iter()
        .find(|record| record.resource_kind == OwnedResourceKind::Machine)
        .unwrap()
        .clone();
    ready_store.save_project_state(&ready).unwrap();
    let ready_changes = ready_store.total_changes_for_test();
    assert_eq!(
        ready_store
            .require_owned_resource(&ready_ownership)
            .unwrap(),
        ready_ownership
    );
    assert_eq!(ready_store.total_changes_for_test(), ready_changes);

    let creating_store = StateStore::in_memory().unwrap();
    let template = topology_project_state("prj_require_creating", &["template"], "/checkout");
    let definition = template.definition;
    let creating_environment = definition.instantiate_environment("creating", 100).unwrap();
    let creating_ownership = OwnershipRecord {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        resource_kind: OwnedResourceKind::Disk,
        resource_id: "vzr1-creating-private-disk".to_string(),
        environment_id: creating_environment.environment_id.clone(),
        machine_id: Some(creating_environment.machines[0].machine_id.clone()),
    };
    creating_store
        .save_project_state(&ProjectState {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            definition,
            environments: vec![creating_environment],
        })
        .unwrap();
    creating_store
        .reserve_owned_resource(&creating_ownership, 101)
        .unwrap();
    let creating_changes = creating_store.total_changes_for_test();
    assert_eq!(
        creating_store
            .require_owned_resource(&creating_ownership)
            .unwrap(),
        creating_ownership
    );
    assert_eq!(creating_store.total_changes_for_test(), creating_changes);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("require-owned-resource.db");
    let stopped_ownership = {
        let store = StateStore::open(&path).unwrap();
        let mut stopped = topology_project_state("prj_require_stopped", &["stopped"], "/checkout");
        stopped.environments[0].state = EnvironmentState::Stopped;
        stopped.environments[0].machines[0].state = MachineState::Stopped;
        let ownership = stopped.environments[0]
            .ownership
            .iter()
            .find(|record| record.resource_kind == OwnedResourceKind::Machine)
            .unwrap()
            .clone();
        store.save_project_state(&stopped).unwrap();
        let changes = store.total_changes_for_test();
        assert_eq!(store.require_owned_resource(&ownership).unwrap(), ownership);
        assert_eq!(store.total_changes_for_test(), changes);
        ownership
    };

    let reopened = StateStore::open(&path).unwrap();
    let reopened_changes = reopened.total_changes_for_test();
    assert_eq!(
        reopened.require_owned_resource(&stopped_ownership).unwrap(),
        stopped_ownership
    );
    assert_eq!(reopened.total_changes_for_test(), reopened_changes);
}

#[test]
fn exact_owned_resource_requirement_validates_active_up_and_rejects_nonowners_without_writes() {
    let store = StateStore::in_memory().unwrap();
    let project =
        topology_project_state("prj_require_active_up", &["owner", "foreign"], "/checkout");
    let owner = &project.environments[0];
    let ownership = OwnershipRecord {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        resource_kind: OwnedResourceKind::Disk,
        resource_id: "vzr1-active-up-private-disk".to_string(),
        environment_id: owner.environment_id.clone(),
        machine_id: Some(owner.machines[0].machine_id.clone()),
    };
    let foreign_environment = project.environments[1].environment_id.clone();
    let foreign_machine = project.environments[1].machines[0].machine_id.clone();
    store.save_project_state(&project).unwrap();
    store.reserve_owned_resource(&ownership, 299).unwrap();
    let operation = store
        .begin_environment_lifecycle(
            owner.environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "req-require-active-up",
            "idem-require-active-up",
            "sha256:require-active-up",
            300,
        )
        .unwrap();
    assert_eq!(operation.status, EnvironmentLifecycleStatus::Running);

    let changes = store.total_changes_for_test();
    assert_eq!(store.require_owned_resource(&ownership).unwrap(), ownership);
    assert_eq!(store.total_changes_for_test(), changes);

    let foreign = OwnershipRecord {
        environment_id: foreign_environment,
        machine_id: Some(foreign_machine),
        ..ownership.clone()
    };
    let error = store.require_owned_resource(&foreign).unwrap_err();
    assert!(matches!(error, StackError::OwnedResourceCollision(_)));
    assert_eq!(store.total_changes_for_test(), changes);

    let stale = OwnershipRecord {
        schema_version: 0,
        ..ownership.clone()
    };
    let error = store.require_owned_resource(&stale).unwrap_err();
    assert!(matches!(
        error,
        StackError::Machine {
            code: MachineErrorCode::StateConflict,
            ..
        }
    ));
    assert_eq!(store.total_changes_for_test(), changes);

    let missing = OwnershipRecord {
        resource_id: "vzr1-missing-owned-resource".to_string(),
        ..ownership
    };
    let error = store.require_owned_resource(&missing).unwrap_err();
    assert!(matches!(
        error,
        StackError::Machine {
            code: MachineErrorCode::NotFound,
            ..
        }
    ));
    assert_eq!(store.total_changes_for_test(), changes);
}

fn never_started_admission_project() -> ProjectState {
    let mut template = topology_project_state("prj_admission_fence", &["template"], "/checkout");
    template.definition.environment.machines[0].workspace = None;
    template.definition.environment.networks.clear();
    template.definition.environment.endpoints.clear();
    let environment = template
        .definition
        .instantiate_environment("fresh", 100)
        .unwrap();
    ProjectState {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        definition: template.definition,
        environments: vec![environment],
    }
}

#[test]
fn environment_admission_fence_is_exact_read_only_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("environment-admission-fence.db");
    let project = never_started_admission_project();
    let expected = project.environments[0].clone();
    {
        let store = StateStore::open(&path).unwrap();
        store.save_project_state(&project).unwrap();
        let changes = store.total_changes_for_test();
        assert_eq!(
            store
                .require_environment_admission_fence(&expected)
                .unwrap(),
            expected
        );
        assert_eq!(store.total_changes_for_test(), changes);
    }
    let store = StateStore::open(&path).unwrap();
    let changes = store.total_changes_for_test();
    assert_eq!(
        store
            .require_environment_admission_fence(&expected)
            .unwrap(),
        expected
    );
    assert_eq!(store.total_changes_for_test(), changes);
    let mut stale = expected.clone();
    stale.updated_at += 1;
    assert!(matches!(
        store
            .require_environment_admission_fence(&stale)
            .unwrap_err(),
        StackError::Machine {
            code: MachineErrorCode::StateConflict,
            ..
        }
    ));
    assert_eq!(store.total_changes_for_test(), changes);
    assert_eq!(
        store
            .load_project_state(expected.project_id.as_str())
            .unwrap(),
        Some(project)
    );
}

#[test]
fn environment_admission_fence_rejects_new_reservations_and_missing_owner_read_only() {
    let store = StateStore::in_memory().unwrap();
    let project = never_started_admission_project();
    let expected = &project.environments[0];
    let initial_changes = store.total_changes_for_test();
    assert!(store.require_environment_admission_fence(expected).is_err());
    assert_eq!(store.total_changes_for_test(), initial_changes);
    store.save_project_state(&project).unwrap();
    store
        .reserve_owned_resource(
            &OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::Other("machine_runtime_store".into()),
                resource_id: "private-admission-store".into(),
                environment_id: expected.environment_id.clone(),
                machine_id: Some(expected.machines[0].machine_id.clone()),
            },
            101,
        )
        .unwrap();
    let current = store
        .load_project_state(expected.project_id.as_str())
        .unwrap()
        .unwrap();
    let changes = store.total_changes_for_test();
    assert!(store.require_environment_admission_fence(expected).is_err());
    assert_eq!(
        store
            .require_environment_admission_fence(&current.environments[0])
            .unwrap(),
        current.environments[0]
    );
    assert_eq!(store.total_changes_for_test(), changes);
}

#[test]
fn environment_admission_fence_rejects_all_machine_activation_dimensions_read_only() {
    for dimension in [
        "state",
        "backend",
        "incarnation",
        "runtime",
        "capabilities",
        "unsupported",
        "legacy",
    ] {
        let store = StateStore::in_memory().unwrap();
        let mut project = never_started_admission_project();
        let machine = &mut project.environments[0].machines[0];
        match dimension {
            "state" => machine.state = MachineState::Failed,
            "backend" => machine.backend = Some(MachineBackend::MacosVirtualizationLinux),
            "incarnation" | "runtime" => {
                machine.incarnation = Some(MachineIncarnation {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    incarnation_id: MachineIncarnationId::new("inc_previous_admission").unwrap(),
                    machine_id: machine.machine_id.clone(),
                    generation: 1,
                    created_at: 100,
                });
                if dimension == "runtime" {
                    machine.backend = Some(MachineBackend::MacosVirtualizationLinux);
                    machine.runtime_identity = Some(MachineRuntimeIdentity {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        opaque_id: "previous-runtime-token".into(),
                    });
                }
            }
            "capabilities" => {
                machine.negotiated_capabilities =
                    CapabilitySet::new([MachineCapability::DockerEngine])
            }
            "unsupported" => {
                machine.negotiated_capabilities.unsupported.insert(
                    MachineCapability::DockerEngine,
                    "previous negotiation".into(),
                );
            }
            "legacy" => machine.legacy_sandbox_id = Some("legacy-machine".into()),
            _ => unreachable!(),
        }
        if let Some(incarnation) = &machine.incarnation {
            let ownership = OwnershipRecord {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                resource_kind: OwnedResourceKind::Incarnation,
                resource_id: incarnation.incarnation_id.to_string(),
                environment_id: machine.environment_id.clone(),
                machine_id: Some(machine.machine_id.clone()),
            };
            project.environments[0].ownership.push(ownership);
        }
        if dimension == "legacy" {
            // A legacy ID cannot be grafted onto fresh Machine ownership.
            let changes = store.total_changes_for_test();
            assert!(matches!(
                store
                    .require_environment_admission_fence(&project.environments[0])
                    .unwrap_err(),
                StackError::TopologyLifecycle(_)
            ));
            assert_eq!(store.total_changes_for_test(), changes);
            continue;
        }
        store.save_project_state(&project).unwrap();
        let current = store
            .load_project_state(project.definition.project_id.as_str())
            .unwrap()
            .unwrap();
        let changes = store.total_changes_for_test();
        assert!(
            matches!(
                store
                    .require_environment_admission_fence(&current.environments[0])
                    .unwrap_err(),
                StackError::Machine {
                    code: MachineErrorCode::StateConflict,
                    ..
                }
            ),
            "dimension={dimension}"
        );
        assert_eq!(
            store.total_changes_for_test(),
            changes,
            "dimension={dimension}"
        );
    }
}

#[test]
fn environment_admission_fence_rejects_started_and_rolled_back_history_read_only() {
    let store = StateStore::in_memory().unwrap();
    let project = never_started_admission_project();
    let expected = &project.environments[0];
    store.save_project_state(&project).unwrap();
    let operation = store
        .begin_environment_lifecycle(
            expected.environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "req-admission",
            "idem-admission",
            "sha256:admission",
            101,
        )
        .unwrap();
    let current = store
        .load_project_state(expected.project_id.as_str())
        .unwrap()
        .unwrap();
    let changes = store.total_changes_for_test();
    assert!(store.require_environment_admission_fence(expected).is_err());
    assert!(
        store
            .require_environment_admission_fence(&current.environments[0])
            .is_err()
    );
    assert_eq!(store.total_changes_for_test(), changes);

    let step = &operation.machine_steps[0];
    store
        .acknowledge_environment_machine_step(
            &MachineLifecycleStepAcknowledgement {
                operation_id: operation.operation_id.clone(),
                generation: operation.generation,
                machine_id: step.machine_id.clone(),
                initial_state: step.initial_state,
                target_state: step.target_state,
                expected_incarnation: None,
                resulting_incarnation: None,
                resulting_activation: None,
                result: LifecycleStepResult::Failed {
                    reason: "admission fixture failure".into(),
                },
            },
            102,
        )
        .unwrap();
    let operation = store
        .finish_environment_lifecycle(operation.operation_id.as_str(), operation.generation, 103)
        .unwrap();
    assert_eq!(operation.status, EnvironmentLifecycleStatus::Failed);

    // Adversarially restore every aggregate projection while retaining even
    // terminal history. Normal mutation APIs deliberately forbid this reset.
    store
        .with_immediate_transaction(|store| store.save_project_state_in_transaction(&project))
        .unwrap();
    assert_eq!(
        store
            .load_project_state(expected.project_id.as_str())
            .unwrap(),
        Some(project.clone())
    );
    let changes = store.total_changes_for_test();
    let error = store
        .require_environment_admission_fence(expected)
        .unwrap_err();
    assert!(error.to_string().contains("persisted lifecycle history"));
    assert_eq!(
        store
            .load_environment_lifecycle_by_idempotency_key("idem-admission")
            .unwrap(),
        Some(operation)
    );
    assert_eq!(
        store
            .load_environment_lifecycle_by_idempotency_key("missing-key")
            .unwrap(),
        None
    );
    assert_eq!(store.total_changes_for_test(), changes);
}

#[test]
fn current_machine_lifecycle_fence_is_exact_read_only_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("current-machine-lifecycle-fence.db");
    let (operation, step, ownership, foreign_collision, missing) = {
        let store = StateStore::open(&path).unwrap();
        let project = topology_project_state(
            "prj_machine_lifecycle_fence",
            &["owner", "foreign"],
            "/checkout",
        );
        let owner = &project.environments[0];
        let machine_ownership = owner
            .ownership
            .iter()
            .find(|record| record.resource_kind == OwnedResourceKind::Machine)
            .unwrap()
            .clone();
        let disk_ownership = OwnershipRecord {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            resource_kind: OwnedResourceKind::Disk,
            resource_id: "vzr1-machine-lifecycle-fence-disk".to_string(),
            environment_id: owner.environment_id.clone(),
            machine_id: Some(owner.machines[0].machine_id.clone()),
        };
        let foreign_existing = project.environments[1]
            .ownership
            .iter()
            .find(|record| record.resource_kind == OwnedResourceKind::Machine)
            .unwrap()
            .clone();
        let foreign_collision = OwnershipRecord {
            environment_id: owner.environment_id.clone(),
            machine_id: Some(owner.machines[0].machine_id.clone()),
            ..foreign_existing
        };
        let missing = OwnershipRecord {
            resource_kind: OwnedResourceKind::Socket,
            resource_id: "vzr1-missing-machine-lifecycle-socket".to_string(),
            ..disk_ownership.clone()
        };
        store.save_project_state(&project).unwrap();
        store.reserve_owned_resource(&disk_ownership, 299).unwrap();
        let operation = store
            .begin_environment_lifecycle(
                owner.environment_id.as_str(),
                EnvironmentLifecycleKind::Up,
                "req-machine-lifecycle-fence",
                "idem-machine-lifecycle-fence",
                "sha256:machine-lifecycle-fence",
                300,
            )
            .unwrap();
        let step = operation.machine_steps[0].clone();
        (
            operation,
            step,
            vec![machine_ownership, disk_ownership],
            foreign_collision,
            missing,
        )
    };

    let store = StateStore::open(&path).unwrap();
    let changes = store.total_changes_for_test();
    let (environment, current) = store
        .require_current_machine_lifecycle_fence(&operation, &step, &ownership)
        .unwrap();
    assert_eq!(current, operation);
    assert_eq!(environment.environment_id, operation.environment_id);
    assert_eq!(
        environment.active_operation_id,
        Some(operation.operation_id.clone())
    );
    assert_eq!(store.total_changes_for_test(), changes);

    let mut stale_generation = operation.clone();
    stale_generation.generation += 1;
    let error = store
        .require_current_machine_lifecycle_fence(&stale_generation, &step, &ownership)
        .unwrap_err();
    assert!(matches!(
        error,
        StackError::TopologyLifecycle(error)
            if matches!(*error, vz_runtime_contract::types::TopologyLifecycleError::GenerationMismatch { .. })
    ));
    assert_eq!(store.total_changes_for_test(), changes);

    let mut wrong_kind = operation.clone();
    wrong_kind.kind = EnvironmentLifecycleKind::Stop;
    wrong_kind.requested_target = EnvironmentState::Stopped;
    wrong_kind.machine_steps[0].target_state = Some(MachineState::Stopped);
    let wrong_kind_step = wrong_kind.machine_steps[0].clone();
    assert!(
        store
            .require_current_machine_lifecycle_fence(&wrong_kind, &wrong_kind_step, &ownership)
            .is_err()
    );
    assert_eq!(store.total_changes_for_test(), changes);

    let mut wrong_definition = operation.clone();
    wrong_definition.definition_digest = "sha256:foreign-definition".to_string();
    assert!(
        store
            .require_current_machine_lifecycle_fence(&wrong_definition, &step, &ownership)
            .is_err()
    );
    assert_eq!(store.total_changes_for_test(), changes);

    assert!(
        store
            .require_current_machine_lifecycle_fence(&operation, &step, &[])
            .is_err()
    );
    assert_eq!(store.total_changes_for_test(), changes);

    let duplicate = [ownership[0].clone(), ownership[0].clone()];
    assert!(
        store
            .require_current_machine_lifecycle_fence(&operation, &step, &duplicate)
            .is_err()
    );
    assert_eq!(store.total_changes_for_test(), changes);

    let with_foreign_second = [ownership[0].clone(), foreign_collision];
    assert!(matches!(
        store
            .require_current_machine_lifecycle_fence(&operation, &step, &with_foreign_second)
            .unwrap_err(),
        StackError::OwnedResourceCollision(_)
    ));
    assert_eq!(store.total_changes_for_test(), changes);

    let with_missing_second = [ownership[0].clone(), missing];
    assert!(matches!(
        store
            .require_current_machine_lifecycle_fence(&operation, &step, &with_missing_second)
            .unwrap_err(),
        StackError::Machine {
            code: MachineErrorCode::NotFound,
            ..
        }
    ));
    assert_eq!(store.total_changes_for_test(), changes);
}

#[test]
fn current_machine_lifecycle_fence_rejects_advanced_terminal_and_detached_state_read_only() {
    let store = StateStore::in_memory().unwrap();
    let project = topology_project_state("prj_machine_fence_advanced", &["owner"], "/checkout");
    let ownership = project.environments[0]
        .ownership
        .iter()
        .filter(|record| record.machine_id.is_some())
        .cloned()
        .collect::<Vec<_>>();
    store.save_project_state(&project).unwrap();
    let operation = store
        .begin_environment_lifecycle(
            "env_owner",
            EnvironmentLifecycleKind::Up,
            "req-machine-fence-advanced",
            "idem-machine-fence-advanced",
            "sha256:machine-fence-advanced",
            300,
        )
        .unwrap();
    let pending_step = operation.machine_steps[0].clone();
    let incarnation = pending_step.expected_incarnation.clone().unwrap();
    let acknowledged = store
        .acknowledge_environment_machine_step(
            &MachineLifecycleStepAcknowledgement {
                operation_id: operation.operation_id.clone(),
                generation: operation.generation,
                machine_id: pending_step.machine_id.clone(),
                initial_state: pending_step.initial_state,
                target_state: pending_step.target_state,
                expected_incarnation: pending_step.expected_incarnation.clone(),
                resulting_incarnation: Some(incarnation.clone()),
                resulting_activation: Some(test_activation(incarnation)),
                result: LifecycleStepResult::Succeeded,
            },
            301,
        )
        .unwrap();
    let changes = store.total_changes_for_test();

    let error = store
        .require_current_machine_lifecycle_fence(&operation, &pending_step, &ownership)
        .unwrap_err();
    assert!(matches!(
        error,
        StackError::TopologyLifecycle(error)
            if matches!(*error, vz_runtime_contract::types::TopologyLifecycleError::MachineStepMismatch { .. })
    ));
    assert_eq!(store.total_changes_for_test(), changes);

    let acknowledged_step = acknowledged.machine_steps[0].clone();
    assert!(
        store
            .require_current_machine_lifecycle_fence(&acknowledged, &acknowledged_step, &ownership)
            .is_err()
    );
    assert_eq!(store.total_changes_for_test(), changes);

    let terminal = store
        .finish_environment_lifecycle(
            acknowledged.operation_id.as_str(),
            acknowledged.generation,
            302,
        )
        .unwrap();
    let terminal_changes = store.total_changes_for_test();
    assert!(
        store
            .require_current_machine_lifecycle_fence(
                &terminal,
                &terminal.machine_steps[0],
                &ownership,
            )
            .is_err()
    );
    assert_eq!(store.total_changes_for_test(), terminal_changes);

    assert!(
        store
            .require_current_machine_lifecycle_fence(&operation, &pending_step, &ownership)
            .is_err()
    );
    assert_eq!(store.total_changes_for_test(), terminal_changes);
}

#[test]
fn owned_resource_reservation_rejects_normalized_json_owner_drift_without_adoption() {
    let store = StateStore::in_memory().unwrap();
    let original = topology_project_state(
        "prj_resource_projection",
        &["owner", "foreign"],
        "/checkout",
    );
    store.save_project_state(&original).unwrap();
    let owner = &original.environments[0];
    let foreign = &original.environments[1];
    let reserved = OwnershipRecord {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        resource_kind: OwnedResourceKind::Disk,
        resource_id: "vzr1-projection-drift".to_string(),
        environment_id: owner.environment_id.clone(),
        machine_id: Some(owner.machines[0].machine_id.clone()),
    };
    store.reserve_owned_resource(&reserved, 300).unwrap();

    let corrupted_json_owner = OwnershipRecord {
        environment_id: foreign.environment_id.clone(),
        machine_id: Some(foreign.machines[0].machine_id.clone()),
        ..reserved.clone()
    };
    store
        .conn
        .execute(
            "UPDATE topology_ownership SET record_json = ?1
             WHERE resource_kind = ?2 AND resource_id = ?3",
            params![
                serde_json::to_string(&corrupted_json_owner).unwrap(),
                serde_json::to_string(&reserved.resource_kind).unwrap(),
                reserved.resource_id,
            ],
        )
        .unwrap();
    let row_before: (String, String, String, Option<String>, i64, String) = store
        .conn
        .query_row(
            "SELECT resource_kind, resource_id, environment_id, machine_id,
                    schema_version, record_json
             FROM topology_ownership
             WHERE resource_kind = ?1 AND resource_id = ?2",
            params![
                serde_json::to_string(&reserved.resource_kind).unwrap(),
                reserved.resource_id,
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();

    let error = store
        .reserve_owned_resource(&corrupted_json_owner, 999)
        .expect_err("JSON owner drift must not be accepted as an idempotent reservation");
    assert!(
        error
            .to_string()
            .contains("persisted topology projection mismatch")
    );
    assert!(error.to_string().contains("field=environment_id"));
    let row_after: (String, String, String, Option<String>, i64, String) = store
        .conn
        .query_row(
            "SELECT resource_kind, resource_id, environment_id, machine_id,
                    schema_version, record_json
             FROM topology_ownership
             WHERE resource_kind = ?1 AND resource_id = ?2",
            params![
                serde_json::to_string(&reserved.resource_kind).unwrap(),
                reserved.resource_id,
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(row_after, row_before);
    assert_eq!(row_after.2, owner.environment_id.as_str());
    assert_eq!(
        row_after.3.as_deref(),
        Some(owner.machines[0].machine_id.as_str())
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT updated_at FROM environment_instances WHERE environment_id = ?1",
                params![owner.environment_id.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        300
    );
}

#[test]
fn store_environment_resolution_uses_shared_explicit_process_workspace_precedence() {
    let store = StateStore::in_memory().unwrap();
    let mut state = topology_project_state(
        "prj_selection",
        &["explicit", "process", "workspace"],
        "/checkout",
    );
    for (index, environment) in state.environments.iter_mut().enumerate() {
        environment.bindings[0].workspace_key = format!("workspace-{index}");
    }
    store.save_project_state(&state).unwrap();

    let context = EnvironmentSelectionContext {
        explicit: Some(EnvironmentSelector::Id(
            state.environments[0].environment_id.clone(),
        )),
        process_environment_id: Some(state.environments[1].environment_id.clone()),
        workspace_key: Some("workspace-2".to_string()),
    };
    let explicit = store
        .resolve_environment("prj_selection", &context)
        .unwrap();
    assert_eq!(explicit.project_id, state.definition.project_id);
    assert_eq!(explicit.name, "explicit");
    assert_eq!(explicit.source, EnvironmentSelectionSource::Explicit);

    let process = store
        .resolve_environment(
            "prj_selection",
            &EnvironmentSelectionContext {
                explicit: None,
                ..context.clone()
            },
        )
        .unwrap();
    assert_eq!(process.name, "process");
    assert_eq!(process.source, EnvironmentSelectionSource::Process);

    let workspace = store
        .resolve_environment(
            "prj_selection",
            &EnvironmentSelectionContext {
                explicit: None,
                process_environment_id: None,
                ..context
            },
        )
        .unwrap();
    assert_eq!(workspace.name, "workspace");
    assert_eq!(workspace.source, EnvironmentSelectionSource::Workspace);
}

#[test]
fn store_environment_resolution_reports_id_name_collision_without_writes() {
    let store = StateStore::in_memory().unwrap();
    let mut state = topology_project_state("prj_selector_collision", &["first", "second"], "/x");
    let colliding = state.environments[0].environment_id.to_string();
    state.environments[1].name = colliding.clone();
    store.save_project_state(&state).unwrap();

    let error = store
        .resolve_environment(
            "prj_selector_collision",
            &EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::NameOrId(colliding)),
                ..EnvironmentSelectionContext::default()
            },
        )
        .expect_err("cross-namespace ID/name collision must be ambiguous");
    assert!(matches!(
        error,
        StackError::TopologyResolution(error)
            if matches!(error.as_ref(), TopologyResolutionError::Ambiguous { candidates, .. } if candidates.len() == 2)
    ));
    assert_eq!(
        store.load_project_state("prj_selector_collision").unwrap(),
        Some(state)
    );
}

#[test]
fn same_path_with_new_workspace_key_does_not_adopt_or_write() {
    let store = StateStore::in_memory().unwrap();
    let state = topology_project_state("prj_no_adoption", &["agent"], "/same/path");
    store.save_project_state(&state).unwrap();

    let error = store
        .resolve_environment(
            "prj_no_adoption",
            &EnvironmentSelectionContext {
                workspace_key: Some("new-opaque-worktree-key".to_string()),
                ..EnvironmentSelectionContext::default()
            },
        )
        .expect_err("an unbound key must not adopt by matching path_hint");
    assert!(matches!(
        error,
        StackError::TopologyResolution(error)
            if matches!(error.as_ref(), TopologyResolutionError::SelectionRequired { candidates, .. } if candidates.len() == 1)
    ));
    assert_eq!(
        store.load_project_state("prj_no_adoption").unwrap(),
        Some(state)
    );
}

fn race_environment_up_reservations(
    db_path: &Path,
    definition: ProjectDefinition,
    names: [&str; 2],
) -> [EnvironmentUpReservation; 2] {
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let handles: Vec<_> = names
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            let db_path = db_path.to_path_buf();
            let definition = definition.clone();
            let barrier = barrier.clone();
            let name = name.to_string();
            std::thread::spawn(move || {
                let store =
                    StateStore::open_with_pragmas(&db_path, StateStorePragmas::daemon_defaults())
                        .unwrap();
                barrier.wait();
                store
                    .resolve_or_reserve_environment_for_up(
                        &definition,
                        &EnvironmentSelectionContext {
                            explicit: Some(EnvironmentSelector::Name(name)),
                            ..EnvironmentSelectionContext::default()
                        },
                        1_000 + index as u64,
                    )
                    .unwrap()
            })
        })
        .collect();
    barrier.wait();
    let mut results = handles.into_iter().map(|handle| handle.join().unwrap());
    [results.next().unwrap(), results.next().unwrap()]
}

#[test]
fn concurrent_same_name_up_reservations_converge_on_one_immutable_id() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("same-name.db");
    let definition = topology_project_state("prj_same_name", &["fixture"], "/x").definition;
    StateStore::open_with_pragmas(&db_path, StateStorePragmas::daemon_defaults()).unwrap();

    let results =
        race_environment_up_reservations(&db_path, definition.clone(), ["parallel", "parallel"]);
    let ids: Vec<_> = results
        .iter()
        .map(|reservation| match reservation {
            EnvironmentUpReservation::Existing { environment, .. }
            | EnvironmentUpReservation::Created { environment } => {
                environment.environment_id.clone()
            }
        })
        .collect();
    assert_eq!(ids[0], ids[1]);
    assert_eq!(
        results
            .iter()
            .filter(|reservation| matches!(reservation, EnvironmentUpReservation::Created { .. }))
            .count(),
        1
    );
    let final_state = StateStore::open(&db_path)
        .unwrap()
        .load_project_state(definition.project_id.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(final_state.environments.len(), 1);
    assert_eq!(final_state.environments[0].name, "parallel");
}

#[test]
fn concurrent_different_name_up_reservations_preserve_both_siblings() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("different-name.db");
    let definition = topology_project_state("prj_different_names", &["fixture"], "/x").definition;
    StateStore::open_with_pragmas(&db_path, StateStorePragmas::daemon_defaults()).unwrap();

    let results =
        race_environment_up_reservations(&db_path, definition.clone(), ["agent-a", "agent-b"]);
    assert!(
        results
            .iter()
            .all(|result| matches!(result, EnvironmentUpReservation::Created { .. }))
    );
    let final_state = StateStore::open(&db_path)
        .unwrap()
        .load_project_state(definition.project_id.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(
        final_state
            .environments
            .iter()
            .map(|environment| environment.name.as_str())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["agent-a", "agent-b"])
    );
}

#[test]
fn stale_aggregate_save_cannot_erase_concurrent_sibling_or_owned_resource() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("stale-aggregate.db");
    let initial = topology_project_state("prj_stale", &["existing"], "/x");
    let first =
        StateStore::open_with_pragmas(&db_path, StateStorePragmas::daemon_defaults()).unwrap();
    first.save_project_state(&initial).unwrap();
    let stale = first.load_project_state("prj_stale").unwrap().unwrap();

    let second =
        StateStore::open_with_pragmas(&db_path, StateStorePragmas::daemon_defaults()).unwrap();
    let created = second
        .resolve_or_reserve_environment_for_up(
            &initial.definition,
            &EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::Name("sibling".to_string())),
                ..EnvironmentSelectionContext::default()
            },
            300,
        )
        .unwrap();
    assert!(matches!(created, EnvironmentUpReservation::Created { .. }));
    let resource = OwnershipRecord {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        resource_kind: OwnedResourceKind::Disk,
        resource_id: "vzr1-concurrent-disk".to_string(),
        environment_id: initial.environments[0].environment_id.clone(),
        machine_id: Some(initial.environments[0].machines[0].machine_id.clone()),
    };
    second.reserve_owned_resource(&resource, 400).unwrap();

    let error = first
        .save_project_state(&stale)
        .expect_err("bootstrap save must reject replacement of an existing Project");
    assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
    let final_state = first.load_project_state("prj_stale").unwrap().unwrap();
    assert_eq!(final_state.environments.len(), 2);
    assert!(
        final_state
            .environments
            .iter()
            .any(|environment| environment.name == "sibling")
    );
    assert!(
        final_state
            .environments
            .iter()
            .flat_map(|environment| &environment.ownership)
            .any(|record| record == &resource)
    );
}

#[test]
fn up_reservation_rejects_definition_drift_before_mutation() {
    let store = StateStore::in_memory().unwrap();
    let state = topology_project_state("prj_drift_up", &["existing"], "/x");
    store.save_project_state(&state).unwrap();
    let mut drifted = state.definition.clone();
    drifted.name = "different-project-name".to_string();

    let error = store
        .resolve_or_reserve_environment_for_up(
            &drifted,
            &EnvironmentSelectionContext {
                explicit: Some(EnvironmentSelector::Name("new".to_string())),
                ..EnvironmentSelectionContext::default()
            },
            500,
        )
        .expect_err("definition drift must reject before creating an Environment");
    assert!(error.to_string().contains("project definition drift"));
    assert_eq!(
        store.load_project_state("prj_drift_up").unwrap(),
        Some(state)
    );
}

#[test]
fn two_worktree_three_environment_layout_resolves_and_persists_distinct_owned_resources() {
    let store = StateStore::in_memory().unwrap();
    let mut state = topology_project_state(
        "prj_acceptance_layout",
        &["agent-a", "agent-b", "integration"],
        "/diagnostic/path",
    );
    state.environments[0].bindings[0].workspace_key = "worktree-token-a".to_string();
    state.environments[1].bindings[0].workspace_key = "worktree-token-a".to_string();
    state.environments[2].bindings[0].workspace_key = "worktree-token-b".to_string();
    assert!(
        state
            .environments
            .iter()
            .all(|environment| environment.machines[0].name == "linux")
    );
    store.save_project_state(&state).unwrap();

    let ambiguity = store
        .resolve_environment(
            "prj_acceptance_layout",
            &EnvironmentSelectionContext {
                workspace_key: Some("worktree-token-a".to_string()),
                ..EnvironmentSelectionContext::default()
            },
        )
        .expect_err("one worktree bound to two named Environments must be ambiguous");
    assert!(matches!(
        ambiguity,
        StackError::TopologyResolution(error)
            if matches!(error.as_ref(), TopologyResolutionError::Ambiguous { candidates, .. }
                if candidates.iter().map(|candidate| candidate.name.as_str()).collect::<std::collections::BTreeSet<_>>()
                    == std::collections::BTreeSet::from(["agent-a", "agent-b"]))
    ));
    let selected = store
        .resolve_environment(
            "prj_acceptance_layout",
            &EnvironmentSelectionContext {
                workspace_key: Some("worktree-token-b".to_string()),
                ..EnvironmentSelectionContext::default()
            },
        )
        .unwrap();
    assert_eq!(selected.name, "integration");

    let resource_kinds = [
        OwnedResourceKind::Disk,
        OwnedResourceKind::Socket,
        OwnedResourceKind::DockerContext,
        OwnedResourceKind::Credential,
        OwnedResourceKind::Other("state".to_string()),
    ];
    let mut expected_ids = std::collections::BTreeSet::new();
    for environment in &state.environments {
        let machine_id = environment.machines[0].machine_id.clone();
        let owner = ResourceOwner {
            project_id: state.definition.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: Some(machine_id.clone()),
        };
        for kind in &resource_kinds {
            let resource_id = owner
                .bounded_resource_name(kind, "linux-primary", 96)
                .unwrap();
            assert!(expected_ids.insert(resource_id.clone()));
            store
                .reserve_owned_resource(
                    &OwnershipRecord {
                        schema_version: TOPOLOGY_SCHEMA_VERSION,
                        resource_kind: kind.clone(),
                        resource_id,
                        environment_id: environment.environment_id.clone(),
                        machine_id: Some(machine_id.clone()),
                    },
                    300,
                )
                .unwrap();
        }
    }
    assert_eq!(
        expected_ids.len(),
        state.environments.len() * resource_kinds.len()
    );

    let persisted = store
        .load_project_state("prj_acceptance_layout")
        .unwrap()
        .unwrap();
    let persisted_ids: std::collections::BTreeSet<_> = persisted
        .environments
        .iter()
        .flat_map(|environment| &environment.ownership)
        .filter(|record| resource_kinds.contains(&record.resource_kind))
        .map(|record| record.resource_id.clone())
        .collect();
    assert_eq!(persisted_ids, expected_ids);
}

fn exact_batch_actions() -> Vec<Action> {
    vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("exact-batch"),
            target: ServiceReplicaKey::new("api", 2).unwrap(),
        },
        Action::ServiceRecreate {
            precondition: crate::reconcile::test_replica_precondition_for_stack("exact-batch"),
            target: ServiceReplicaKey::new("api-2", 1).unwrap(),
        },
        Action::ServiceRemove {
            precondition: crate::reconcile::test_replica_precondition_for_stack("exact-batch"),
            target: ServiceReplicaKey::new("worker", 1).unwrap(),
        },
    ]
}

#[test]
fn fresh_claim_kind_status_binding_matrix_is_complete_and_fail_closed() {
    use super::stack_journal::{
        legal_fresh_claim_predecessor, status_binding_is_structurally_valid,
    };

    let actions = exact_batch_actions();
    let statuses = [
        StackContainerCreateStatus::Intent,
        StackContainerCreateStatus::Reserved,
        StackContainerCreateStatus::Running,
        StackContainerCreateStatus::CleanupPending,
        StackContainerCreateStatus::Blocked,
        StackContainerCreateStatus::Cleaned,
        StackContainerCreateStatus::Failed,
    ];
    let expected = [
        // Create: only retryable blocked or terminal predecessors.
        [false, false, false, false, true, true, true],
        // Recreate: exactly a running, bound predecessor.
        [false, false, true, false, false, false, false],
        // Remove: incomplete unbound, running bound, blocked either way, or
        // terminal unbound failure. Cleanup progression is replay-only.
        [true, true, true, false, true, false, true],
    ];

    let mut checked = 0;
    for (action_index, action) in actions.iter().enumerate() {
        for (status_index, status) in statuses.into_iter().enumerate() {
            for bound in [false, true] {
                let binding_permitted = match action_index {
                    0 => {
                        expected[action_index][status_index]
                            && (status != StackContainerCreateStatus::Blocked || bound)
                    }
                    1 => expected[action_index][status_index],
                    2 => {
                        expected[action_index][status_index]
                            && match status {
                                StackContainerCreateStatus::Intent
                                | StackContainerCreateStatus::Failed => !bound,
                                StackContainerCreateStatus::Reserved
                                | StackContainerCreateStatus::Running => bound,
                                StackContainerCreateStatus::Blocked => true,
                                StackContainerCreateStatus::CleanupPending
                                | StackContainerCreateStatus::Cleaned => false,
                            }
                    }
                    _ => unreachable!(),
                };
                let structurally_valid = match status {
                    StackContainerCreateStatus::Intent | StackContainerCreateStatus::Failed => {
                        !bound
                    }
                    StackContainerCreateStatus::Reserved
                    | StackContainerCreateStatus::Running
                    | StackContainerCreateStatus::CleanupPending
                    | StackContainerCreateStatus::Cleaned => bound,
                    StackContainerCreateStatus::Blocked => true,
                };
                assert_eq!(
                    status_binding_is_structurally_valid(status, bound),
                    structurally_valid,
                    "unexpected structural shape result for status={status:?}, bound={bound}"
                );
                assert_eq!(
                    legal_fresh_claim_predecessor(action, status, bound),
                    binding_permitted && structurally_valid,
                    "unexpected fresh-claim admission for action={action:?}, status={status:?}, bound={bound}"
                );
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 42);
}

#[test]
fn impossible_action_kind_and_head_pairs_never_cross_persistence_boundary() {
    let store = StateStore::in_memory().unwrap();
    let workload = workload_scope_for_journal_intent(&journal_fixture("kind-head").1);
    let target = ServiceReplicaKey::first("web".to_string()).unwrap();
    let never = crate::reconcile::ReplicaPrecondition::new(
        workload.clone(),
        0,
        crate::reconcile::ExpectedJournalHead::NeverJournaled,
    )
    .unwrap();
    let exact_unbound = crate::reconcile::ReplicaPrecondition::new(
        workload.clone(),
        0,
        crate::reconcile::ExpectedJournalHead::exact("kind-head", 1, None).unwrap(),
    )
    .unwrap();
    let invalid_actions = [
        Action::ServiceRecreate {
            target: target.clone(),
            precondition: never.clone(),
        },
        Action::ServiceRecreate {
            target: target.clone(),
            precondition: exact_unbound,
        },
        Action::ServiceRemove {
            target,
            precondition: never,
        },
    ];

    for (index, action) in invalid_actions.into_iter().enumerate() {
        let actions = vec![action];
        assert!(actions[0].validate().is_err());
        let session = ReconcileSession {
            session_id: format!("rs-invalid-kind-head-{index}"),
            stack_name: workload.stack_id.clone(),
            operation_id: format!("op-invalid-kind-head-{index}"),
            status: ReconcileSessionStatus::Active,
            actions_hash: crate::reconcile::compute_actions_hash(&actions),
            next_action_index: 0,
            total_actions: 1,
            started_at: 1,
            updated_at: 1,
            completed_at: None,
        };
        assert!(store.create_reconcile_session(&session, &actions).is_err());
        assert!(
            store
                .save_reconcile_progress(&session.stack_name, &session.operation_id, &actions, 0)
                .is_err()
        );
        assert!(store.create_reconcile_batch(&session, &actions).is_err());
    }
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM reconcile_sessions", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM reconcile_progress", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn batch_rejects_valid_action_scoped_to_another_stack_without_writes() {
    let store = StateStore::in_memory().unwrap();
    let (_, intent, _) = journal_fixture("cross-stack-action");
    let action = Action::ServiceCreate {
        target: ServiceReplicaKey::first("web".to_string()).unwrap(),
        precondition: crate::reconcile::ReplicaPrecondition::new(
            workload_scope_for_journal_intent(&intent),
            intent.environment_generation,
            crate::reconcile::ExpectedJournalHead::NeverJournaled,
        )
        .unwrap(),
    };
    let actions = vec![action];
    let session = ReconcileSession {
        session_id: "rs-cross-stack".to_string(),
        stack_name: "different-stack".to_string(),
        operation_id: "op-cross-stack".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: 1,
        started_at: 1,
        updated_at: 1,
        completed_at: None,
    };

    assert!(store.create_reconcile_batch(&session, &actions).is_err());
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM reconcile_sessions", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM reconcile_progress", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn invalid_action_target_is_rejected_at_every_persistence_entrypoint() {
    let store = StateStore::in_memory().unwrap();
    let valid_actions = exact_batch_actions();
    let mut invalid_target = ServiceReplicaKey::new("api", 2).unwrap();
    invalid_target.service_name = " api".to_string();
    let invalid_actions = vec![Action::ServiceCreate {
        precondition: crate::reconcile::test_replica_precondition(),
        target: invalid_target,
    }];
    let invalid_session = ReconcileSession {
        session_id: "rs-invalid-target".to_string(),
        stack_name: "invalid-target".to_string(),
        operation_id: "invalid-target-operation".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&invalid_actions),
        next_action_index: 0,
        total_actions: invalid_actions.len(),
        started_at: 1,
        updated_at: 1,
        completed_at: None,
    };

    assert!(
        store
            .save_reconcile_progress(
                &invalid_session.stack_name,
                &invalid_session.operation_id,
                &invalid_actions,
                0,
            )
            .is_err()
    );
    assert!(
        store
            .create_reconcile_session(&invalid_session, &invalid_actions)
            .is_err()
    );
    assert!(
        store
            .create_reconcile_batch(&invalid_session, &invalid_actions)
            .is_err()
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM reconcile_progress", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT COUNT(*) FROM reconcile_sessions", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );

    let valid_session = ReconcileSession {
        session_id: "rs-valid-before-invalid-start".to_string(),
        stack_name: "exact-batch".to_string(),
        operation_id: "valid-operation".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&valid_actions),
        next_action_index: 0,
        total_actions: valid_actions.len(),
        started_at: 2,
        updated_at: 2,
        completed_at: None,
    };
    store
        .create_reconcile_batch(&valid_session, &valid_actions)
        .unwrap();
    assert!(
        store
            .start_reconcile_batch(
                &valid_session.session_id,
                &valid_session.stack_name,
                &valid_session.operation_id,
                0,
                &invalid_actions,
            )
            .is_err()
    );
    assert!(
        store
            .load_audit_log_for_session(&valid_session.session_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn persisted_actions_reject_unknown_nested_target_and_workload_fields() {
    let store = StateStore::in_memory().unwrap();
    let actions = exact_batch_actions();
    store
        .save_reconcile_progress("exact-batch", "nested-unknown-operation", &actions, 0)
        .unwrap();
    let actions_json: String = store
        .conn
        .query_row(
            "SELECT actions_json FROM reconcile_progress WHERE stack_name = 'exact-batch'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut target_tamper: serde_json::Value = serde_json::from_str(&actions_json).unwrap();
    target_tamper[0]["target"]["unexpected"] = serde_json::json!(true);
    store
        .conn
        .execute(
            "UPDATE reconcile_progress SET actions_json = ?1 WHERE stack_name = 'exact-batch'",
            params![target_tamper.to_string()],
        )
        .unwrap();
    assert!(store.load_reconcile_progress("exact-batch").is_err());

    let mut legacy_ownership: serde_json::Value = serde_json::from_str(&actions_json).unwrap();
    legacy_ownership[0]["precondition"]["journal_head"]["ownership"]["scope"]["machine_incarnation_id"] =
        serde_json::Value::Null;
    store
        .conn
        .execute(
            "UPDATE reconcile_progress SET actions_json = ?1 WHERE stack_name = 'exact-batch'",
            params![legacy_ownership.to_string()],
        )
        .unwrap();
    let error = store.load_reconcile_progress("exact-batch").unwrap_err();
    assert!(error.to_string().contains("machine incarnation"));

    store
        .save_reconcile_progress("exact-batch", "op", &actions, 0)
        .unwrap();
    let progress_json: String = store
        .conn
        .query_row(
            "SELECT actions_json FROM reconcile_progress WHERE stack_name = 'exact-batch'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut workload_tamper: serde_json::Value = serde_json::from_str(&progress_json).unwrap();
    workload_tamper[0]["precondition"]["workload"]["unexpected"] = serde_json::json!(true);
    store
        .conn
        .execute(
            "UPDATE reconcile_progress SET actions_json = ?1 WHERE stack_name = 'exact-batch'",
            params![workload_tamper.to_string()],
        )
        .unwrap();
    assert!(store.load_reconcile_progress("exact-batch").is_err());
}

fn exact_batch_actions_for_claim(store: &StateStore) -> Vec<Action> {
    let project = topology_project_state("prj_exact_batch", &["machine"], "/checkout");
    let (mut recreate_intent, mut recreate_binding) = journal_records_for_environment(
        &project,
        0,
        "reservation-exact-recreate",
        "exact-batch",
        "api-2",
        "ctr-exact-recreate",
    );
    let (mut remove_intent, mut remove_binding) = journal_records_for_environment(
        &project,
        0,
        "reservation-exact-remove",
        "exact-batch",
        "worker",
        "ctr-exact-remove",
    );
    recreate_intent.action_digest = "sha256:exact-recreate".to_string();
    recreate_binding.ownership.scope = Some(Box::new(recreate_intent.scope.clone()));
    remove_intent.action_digest = "sha256:exact-remove".to_string();
    remove_binding.ownership.scope = Some(Box::new(remove_intent.scope.clone()));

    store.save_project_state(&project).unwrap();
    reserve_journal_owner(store, &recreate_intent);
    for (intent, binding) in [
        (&recreate_intent, &recreate_binding),
        (&remove_intent, &remove_binding),
    ] {
        store.begin_stack_container_create(intent).unwrap();
        store.bind_stack_container_generation(binding).unwrap();
        store
            .publish_stack_container_create_success(&intent.scope.reservation_id, true, 102)
            .unwrap();
    }

    let workload = workload_scope_for_journal_intent(&recreate_intent);
    let environment_generation = recreate_intent.environment_generation;
    vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::ReplicaPrecondition::new(
                workload.clone(),
                environment_generation,
                crate::reconcile::ExpectedJournalHead::NeverJournaled,
            )
            .unwrap(),
            target: ServiceReplicaKey::new("api", 2).unwrap(),
        },
        Action::ServiceRecreate {
            precondition: crate::reconcile::ReplicaPrecondition::new(
                workload.clone(),
                environment_generation,
                crate::reconcile::ExpectedJournalHead::exact(
                    &recreate_intent.scope.reservation_id,
                    recreate_intent.service_generation,
                    Some(recreate_binding.ownership.clone()),
                )
                .unwrap(),
            )
            .unwrap(),
            target: ServiceReplicaKey::new("api-2", 1).unwrap(),
        },
        Action::ServiceRemove {
            precondition: crate::reconcile::ReplicaPrecondition::new(
                workload,
                environment_generation,
                crate::reconcile::ExpectedJournalHead::exact(
                    &remove_intent.scope.reservation_id,
                    remove_intent.service_generation,
                    Some(remove_binding.ownership.clone()),
                )
                .unwrap(),
            )
            .unwrap(),
            target: ServiceReplicaKey::new("worker", 1).unwrap(),
        },
    ]
}

fn install_exact_batch(store: &StateStore, session_id: &str) -> Vec<Action> {
    let actions = exact_batch_actions_for_claim(store);
    let session = ReconcileSession {
        session_id: session_id.to_string(),
        stack_name: "exact-batch".to_string(),
        operation_id: "exact-operation".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store.create_reconcile_batch(&session, &actions).unwrap();
    store
        .start_reconcile_batch(session_id, "exact-batch", "exact-operation", 0, &actions)
        .unwrap();
    actions
}

fn install_unstarted_batch(
    store: &StateStore,
    session_id: &str,
    operation_id: &str,
    actions: &[Action],
) {
    let session = ReconcileSession {
        session_id: session_id.to_string(),
        stack_name: actions[0].precondition().workload().stack_id.clone(),
        operation_id: operation_id.to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 1,
        updated_at: 1,
        completed_at: None,
    };
    store.create_reconcile_batch(&session, actions).unwrap();
}

fn claimed_create_input(store: &StateStore, action: &Action, suffix: &str) -> ClaimedCreateInput {
    let environment = store
        .load_environment_instance(action.precondition().workload().environment_id.as_str())
        .unwrap()
        .unwrap();
    ClaimedCreateInput {
        requested_container_id: format!("ctr-claimed-{suffix}"),
        definition_digest: environment.definition_digest,
        applied_config_digest: format!("vzsc1-sha256:claimed-{suffix}"),
        activation_payload_sha256: "c".repeat(64),
    }
}

fn empty_claimed_allocator_target() -> ClaimedAllocatorTarget {
    ClaimedAllocatorTarget {
        ports: Vec::new(),
        service_ip: None,
        service_network_ips: Vec::new(),
        mount_tag_offset: None,
    }
}

fn binding_for_claimed_intent(
    intent: &StackContainerCreateIntent,
    runtime_generation: u64,
    bound_at: u64,
) -> StackContainerGenerationBinding {
    StackContainerGenerationBinding {
        reservation_id: intent.scope.reservation_id.clone(),
        service_name: intent.service_name.clone(),
        ownership: ContainerGenerationOwnership {
            container_id: intent.requested_container_id.clone(),
            generation: runtime_generation,
            stack_id: intent.scope.stack_id.clone(),
            scope: Some(Box::new(intent.scope.clone())),
        },
        bound_at,
    }
}

fn receipt_for_claimed_binding(
    binding: &StackContainerGenerationBinding,
) -> ContainerCreateReceipt {
    ContainerCreateReceipt {
        container_id: binding.ownership.container_id.clone(),
        ownership: Some(binding.ownership.clone()),
    }
}

fn inject_journal_intent_for_test(store: &StateStore, intent: &StackContainerCreateIntent) {
    intent.validate().unwrap();
    let (persisted_status, observed) = match intent.status {
        StackContainerCreateStatus::Intent => (
            "intent",
            ServiceObservedState {
                replica: ServiceReplicaKey::new(intent.service_name.clone(), intent.replica_index)
                    .unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Creating,
                container_id: None,
                failed_create_ownership: None,
                last_error: None,
                ready: false,
            },
        ),
        StackContainerCreateStatus::Failed => (
            "failed",
            ServiceObservedState {
                replica: ServiceReplicaKey::new(intent.service_name.clone(), intent.replica_index)
                    .unwrap(),
                applied_config_digest: None,
                phase: ServicePhase::Failed,
                container_id: None,
                failed_create_ownership: None,
                last_error: intent.last_error.clone(),
                ready: false,
            },
        ),
        other => panic!("test journal injector cannot synthesize {other:?}"),
    };
    store
        .conn
        .execute(
            "INSERT INTO stack_container_create_intents (
                reservation_id, schema_version, project_id, environment_id, machine_id,
                machine_incarnation_id, environment_generation, stack_id, service_name,
                replica_index, service_generation, requested_container_id, definition_digest,
                action_digest, applied_config_digest, status, intent_json, last_error, created_at, updated_at,
                completed_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                       ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
            params![
                intent.scope.reservation_id,
                intent.schema_version,
                intent.scope.project_id.as_str(),
                intent.scope.environment_id.as_str(),
                intent.scope.machine_id.as_str(),
                intent
                    .scope
                    .machine_incarnation_id
                    .as_ref()
                    .unwrap()
                    .as_str(),
                i64::try_from(intent.environment_generation).unwrap(),
                intent.scope.stack_id,
                intent.service_name,
                i64::from(intent.replica_index),
                i64::try_from(intent.service_generation).unwrap(),
                intent.requested_container_id,
                intent.definition_digest,
                intent.action_digest,
                intent.applied_config_digest,
                persisted_status,
                serde_json::to_string(intent).unwrap(),
                intent.last_error,
                i64::try_from(intent.created_at).unwrap(),
                i64::try_from(intent.updated_at).unwrap(),
                intent
                    .completed_at
                    .map(|value| i64::try_from(value).unwrap()),
            ],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO observed_state (stack_name, service_name, replica_index, state_json)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(stack_name, service_name, replica_index) DO UPDATE SET
                state_json = excluded.state_json",
            params![
                intent.scope.stack_id,
                intent.service_name,
                i64::from(intent.replica_index),
                serde_json::to_string(&observed).unwrap(),
            ],
        )
        .unwrap();
}

fn exact_outcomes(
    actions: &[Action],
    failed_index: Option<usize>,
) -> Vec<crate::executor::IndexedActionOutcome> {
    actions
        .iter()
        .enumerate()
        .map(
            |(absolute_index, action)| crate::executor::IndexedActionOutcome {
                absolute_index,
                action_hash: crate::reconcile::compute_actions_hash(std::slice::from_ref(action)),
                action_kind: crate::executor::ReconcileActionKind::from_action(action),
                target: action.target().clone(),
                result: if failed_index == Some(absolute_index) {
                    crate::executor::ActionOutcomeResult::Failed {
                        error: format!("failure-{absolute_index}"),
                    }
                } else {
                    crate::executor::ActionOutcomeResult::Succeeded
                },
            },
        )
        .collect()
}

fn assert_uncommitted_exact_batch(store: &StateStore, session_id: &str) {
    let active = store
        .load_active_reconcile_session("exact-batch")
        .unwrap()
        .unwrap();
    assert_eq!(active.session_id, session_id);
    assert_eq!(active.next_action_index, 0);
    let progress = store
        .load_reconcile_progress("exact-batch")
        .unwrap()
        .unwrap();
    assert_eq!(progress.next_action_index, 0);
    let audits = store.load_audit_log_for_session(session_id).unwrap();
    assert_eq!(audits.len(), 3);
    assert!(audits.iter().all(|entry| entry.status == "started"));
}

fn install_claimed_teardown_batch(
    store: &StateStore,
    session_id: &str,
) -> (Vec<Action>, Vec<ReconcileActionClaim>, String) {
    let source = exact_batch_actions_for_claim(store);
    let actions = vec![
        match &source[1] {
            Action::ServiceRecreate {
                precondition,
                target,
            } => Action::ServiceRemove {
                precondition: precondition.clone(),
                target: target.clone(),
            },
            other => panic!("expected recreate fixture, got {other:?}"),
        },
        source[2].clone(),
    ];
    let operation_id = format!("{CLAIMED_TEARDOWN_OPERATION_PREFIX}req-state-store-test");
    let session = ReconcileSession {
        session_id: session_id.to_string(),
        stack_name: "exact-batch".to_string(),
        operation_id: operation_id.clone(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store.create_reconcile_batch(&session, &actions).unwrap();
    let claims = store
        .start_reconcile_batch(session_id, "exact-batch", &operation_id, 0, &actions)
        .unwrap();
    (actions, claims, operation_id)
}

#[test]
fn generic_commit_rejects_reserved_teardown_without_mutation() {
    let store = StateStore::in_memory().unwrap();
    let session_id = "rs-reserved-generic-denial";
    let (actions, _claims, operation_id) = install_claimed_teardown_batch(&store, session_id);

    let error = store
        .commit_reconcile_batch(
            session_id,
            "exact-batch",
            &operation_id,
            0,
            &actions,
            &exact_outcomes(&actions, None),
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("claim-qualified teardown commit")
    );
    let active = store
        .load_active_reconcile_session("exact-batch")
        .unwrap()
        .unwrap();
    assert_eq!(active.session_id, session_id);
    assert_eq!(active.next_action_index, 0);
    let audits = store.load_audit_log_for_session(session_id).unwrap();
    assert_eq!(audits.len(), 2);
    assert!(audits.iter().all(|audit| audit.status == "started"));
}

#[test]
fn claimed_teardown_commit_rejects_swapped_claims_without_mutation() {
    let store = StateStore::in_memory().unwrap();
    let session_id = "rs-swapped-teardown-claims";
    let (actions, mut claims, operation_id) = install_claimed_teardown_batch(&store, session_id);
    claims.swap(0, 1);

    let error = store
        .commit_claimed_teardown_batch(ClaimedTeardownCommit {
            claims: &claims,
            session_id,
            stack_name: "exact-batch",
            operation_id: &operation_id,
            expected_cursor: 0,
            actions: &actions,
            outcomes: &exact_outcomes(&actions, None),
        })
        .unwrap_err();
    assert!(error.to_string().contains("does not match exact action"));
    let active = store
        .load_active_reconcile_session("exact-batch")
        .unwrap()
        .unwrap();
    assert_eq!(active.session_id, session_id);
    assert_eq!(active.next_action_index, 0);
    let audits = store.load_audit_log_for_session(session_id).unwrap();
    assert_eq!(audits.len(), 2);
    assert!(audits.iter().all(|audit| audit.status == "started"));
}

#[test]
fn terminal_claimed_teardown_reconstructs_one_atomic_finalizer_result() {
    let store = StateStore::in_memory().unwrap();
    let session_id = "rs-terminal-finalizer-reconstruction";
    let (actions, claims, operation_id) = install_claimed_teardown_batch(&store, session_id);
    let (_, intent, _) = journal_fixture("terminal-finalizer-reconstruction");
    let mut scope = workload_scope_for_journal_intent(&intent);
    scope.stack_id = "exact-batch".to_string();
    let initial_runtime_identity =
        Some(vz_runtime_contract::StackRuntimeIdentity::new(scope.stack_id.clone()).unwrap());
    let mut finalizer = TeardownFinalizer {
        schema_version: TEARDOWN_FINALIZER_SCHEMA_VERSION,
        operation_key: "req:req-state-store-test".to_string(),
        request_id: "req-state-store-test".to_string(),
        idempotency_key: None,
        request_digest: "vztr3-sha256:terminal-reconstruction".to_string(),
        session_id: session_id.to_string(),
        reconcile_operation_id: operation_id.clone(),
        scope,
        remove_volumes: false,
        changed_actions: u32::try_from(actions.len()).unwrap(),
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        desired_state_digest: "vzs1-sha256:terminal-reconstruction".to_string(),
        initial_volumes: Vec::new(),
        initial_disk_image: false,
        initial_runtime_present: true,
        initial_runtime_identity,
        runtime_shutdown: false,
        staged_volumes: Vec::new(),
        purged_volumes: Vec::new(),
        disk_staged: false,
        disk_purged: false,
        status: TeardownFinalizerStatus::Prepared,
        receipt: None,
        response_json: None,
        created_at: 200,
        updated_at: 200,
        completed_at: None,
    };
    store.reserve_teardown_finalizer(&finalizer).unwrap();
    finalizer.runtime_shutdown = true;
    finalizer.updated_at = 201;
    store.save_teardown_finalizer_progress(&finalizer).unwrap();

    store
        .commit_claimed_teardown_batch(ClaimedTeardownCommit {
            claims: &claims,
            session_id,
            stack_name: "exact-batch",
            operation_id: &operation_id,
            expected_cursor: 0,
            actions: &actions,
            outcomes: &exact_outcomes(&actions, None),
        })
        .unwrap();
    assert!(store.list_receipts().unwrap().is_empty());

    finalizer.status = TeardownFinalizerStatus::Completed;
    finalizer.updated_at = 202;
    finalizer.completed_at = Some(202);
    finalizer.response_json = Some(
        serde_json::json!({
            "request_id": "req-state-store-test",
            "stack_name": "exact-batch",
            "changed_actions": 2,
            "removed_volumes": 0,
        })
        .to_string(),
    );
    finalizer.receipt = Some(Receipt {
        receipt_id: teardown_receipt_id(&finalizer.operation_key, &finalizer.request_digest),
        operation: "teardown_stack".to_string(),
        entity_id: "exact-batch".to_string(),
        entity_type: "stack".to_string(),
        request_id: "req-state-store-test".to_string(),
        status: "success".to_string(),
        created_at: 202,
        metadata: serde_json::json!({
            "event_type": "stack_destroyed",
            "request_digest": finalizer.request_digest.clone(),
            "changed_actions": 2,
            "removed_volumes": 0
        }),
    });
    let event = StackEvent::StackDestroyed {
        stack_name: "exact-batch".to_string(),
    };
    store
        .complete_terminal_teardown_finalizer(&finalizer, None, &event)
        .unwrap();

    assert_eq!(
        store
            .load_teardown_finalizer("req:req-state-store-test")
            .unwrap(),
        Some(finalizer.clone())
    );
    assert_eq!(store.list_receipts().unwrap().len(), 1);
    let receipt_id = finalizer.receipt.as_ref().unwrap().receipt_id.clone();
    let policy = ReceiptRetentionPolicy {
        max_count: 0,
        max_age_secs: 0,
    };
    assert_eq!(
        store
            .receipt_retention_state_map(policy, 1_000)
            .unwrap()
            .get(&receipt_id)
            .and_then(|state| state.gc_reason),
        None,
        "retention inspection must agree that durable teardown replay receipts are protected"
    );
    assert!(
        store
            .compact_receipts_with_policy_at(policy, 1_000)
            .unwrap()
            .is_empty()
    );
    assert!(store.load_receipt(&receipt_id).unwrap().is_some());
    assert_eq!(
        store
            .load_events_since("exact-batch", 0)
            .unwrap()
            .into_iter()
            .filter(|record| matches!(record.event, StackEvent::StackDestroyed { .. }))
            .count(),
        1
    );
}

#[test]
fn audit_claim_prevents_session_payload_deletion_across_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("audit-session-fk.db");
    let store = StateStore::open(&path).unwrap();
    let actions = install_exact_batch(&store, "rs-audit-session-fk");
    assert!(store.foreign_keys_enabled().unwrap());
    let error = store
        .conn
        .execute(
            "DELETE FROM reconcile_sessions WHERE session_id = 'rs-audit-session-fk'",
            [],
        )
        .unwrap_err();
    assert!(error.to_string().contains("FOREIGN KEY constraint failed"));
    drop(store);

    let reopened = StateStore::open(&path).unwrap();
    assert!(reopened.foreign_keys_enabled().unwrap());
    assert_eq!(
        reopened
            .load_reconcile_session_actions("rs-audit-session-fk")
            .unwrap(),
        actions
    );
    assert_eq!(
        reopened
            .load_audit_log_for_session("rs-audit-session-fk")
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn competing_exact_batch_install_preserves_active_session_progress_and_audits() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("exact-batch-competing-install.db");
    let first = StateStore::open_with_pragmas(&path, StateStorePragmas::daemon_defaults()).unwrap();
    let actions = install_exact_batch(&first, "rs-exact-owner");
    let before_progress = first
        .load_reconcile_progress("exact-batch")
        .unwrap()
        .unwrap();
    let before_audits =
        serde_json::to_value(first.load_audit_log_for_session("rs-exact-owner").unwrap()).unwrap();

    let competitor =
        StateStore::open_with_pragmas(&path, StateStorePragmas::daemon_defaults()).unwrap();
    let competing_session = ReconcileSession {
        session_id: "rs-exact-competitor".to_string(),
        stack_name: "exact-batch".to_string(),
        operation_id: "competing-operation".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 1_700_000_001,
        updated_at: 1_700_000_001,
        completed_at: None,
    };
    let error = competitor
        .create_reconcile_batch(&competing_session, &actions)
        .unwrap_err()
        .to_string();
    assert!(error.contains("already has active reconcile session"));

    let active = first
        .load_active_reconcile_session("exact-batch")
        .unwrap()
        .unwrap();
    assert_eq!(active.session_id, "rs-exact-owner");
    assert_eq!(active.operation_id, "exact-operation");
    assert_eq!(active.next_action_index, 0);
    assert_eq!(
        first.load_reconcile_progress("exact-batch").unwrap(),
        Some(before_progress)
    );
    assert_eq!(
        serde_json::to_value(first.load_audit_log_for_session("rs-exact-owner").unwrap()).unwrap(),
        before_audits
    );
    assert_eq!(
        first
            .conn
            .query_row(
                "SELECT COUNT(*) FROM reconcile_sessions WHERE session_id = ?1",
                params![competing_session.session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn duplicate_exact_target_plan_is_rejected_before_batch_mutation() {
    let store = StateStore::in_memory().unwrap();
    let target = ServiceReplicaKey::new("api", 2).unwrap();
    let actions = vec![
        Action::ServiceCreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target: target.clone(),
        },
        Action::ServiceRecreate {
            precondition: crate::reconcile::test_replica_precondition(),
            target,
        },
    ];
    let session = ReconcileSession {
        session_id: "rs-exact-duplicate-target".to_string(),
        stack_name: "duplicate-target".to_string(),
        operation_id: "duplicate-target-operation".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };

    let error = store
        .create_reconcile_batch(&session, &actions)
        .unwrap_err()
        .to_string();
    assert!(error.contains("duplicate exact replica target"));
    assert!(
        store
            .load_reconcile_progress("duplicate-target")
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_active_reconcile_session("duplicate-target")
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .load_audit_log_for_session(&session.session_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn exact_batch_all_success_completes_and_clears_progress_atomically() {
    let store = StateStore::in_memory().unwrap();
    let actions = install_exact_batch(&store, "rs-exact-success");
    let commit = store
        .commit_reconcile_batch(
            "rs-exact-success",
            "exact-batch",
            "exact-operation",
            0,
            &actions,
            &exact_outcomes(&actions, None),
        )
        .unwrap();
    assert_eq!(commit.next_action_index, 3);
    assert_eq!(commit.status, ReconcileSessionStatus::Completed);
    assert!(
        store
            .load_reconcile_progress("exact-batch")
            .unwrap()
            .is_none()
    );
    let sessions = store.list_reconcile_sessions("exact-batch", 10).unwrap();
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Completed);
    assert_eq!(sessions[0].next_action_index, 3);
    let audits = store
        .load_audit_log_for_session("rs-exact-success")
        .unwrap();
    assert!(audits.iter().all(|entry| entry.status == "completed"));
    assert_eq!(audits[0].target, ServiceReplicaKey::new("api", 2).unwrap());
    assert_eq!(
        audits[1].target,
        ServiceReplicaKey::new("api-2", 1).unwrap()
    );
}

#[test]
fn exact_batch_partial_failure_advances_only_successful_prefix_and_forces_replan() {
    let store = StateStore::in_memory().unwrap();
    let actions = install_exact_batch(&store, "rs-exact-partial");
    let outcomes = exact_outcomes(&actions, Some(1));
    let commit = store
        .commit_reconcile_batch(
            "rs-exact-partial",
            "exact-batch",
            "exact-operation",
            0,
            &actions,
            &outcomes,
        )
        .unwrap();
    assert_eq!(commit.next_action_index, 1);
    assert_eq!(commit.status, ReconcileSessionStatus::Failed);
    assert!(
        store
            .load_reconcile_progress("exact-batch")
            .unwrap()
            .is_none()
    );
    let sessions = store.list_reconcile_sessions("exact-batch", 10).unwrap();
    assert_eq!(sessions[0].status, ReconcileSessionStatus::Failed);
    assert_eq!(sessions[0].next_action_index, 1);
    let audits = store
        .load_audit_log_for_session("rs-exact-partial")
        .unwrap();
    assert_eq!(
        audits
            .iter()
            .map(|entry| entry.status.as_str())
            .collect::<Vec<_>>(),
        vec!["completed", "failed", "completed"]
    );
    assert_eq!(audits[1].error_message.as_deref(), Some("failure-1"));
    assert_eq!(
        store
            .commit_reconcile_batch(
                "rs-exact-partial",
                "exact-batch",
                "exact-operation",
                0,
                &actions,
                &outcomes,
            )
            .unwrap(),
        commit
    );
    let mut conflicting = outcomes;
    conflicting[1].result = crate::executor::ActionOutcomeResult::Failed {
        error: "different failure".to_string(),
    };
    assert!(
        store
            .commit_reconcile_batch(
                "rs-exact-partial",
                "exact-batch",
                "exact-operation",
                0,
                &actions,
                &conflicting,
            )
            .is_err()
    );
}

#[test]
fn malformed_exact_outcomes_roll_back_without_mutation() {
    enum Mutation {
        Missing,
        Duplicate,
        WrongIndex,
        WrongHash,
        WrongTarget,
        WrongKind,
    }
    for (case, mutation) in [
        ("missing", Mutation::Missing),
        ("duplicate", Mutation::Duplicate),
        ("index", Mutation::WrongIndex),
        ("hash", Mutation::WrongHash),
        ("target", Mutation::WrongTarget),
        ("kind", Mutation::WrongKind),
    ] {
        let store = StateStore::in_memory().unwrap();
        let session_id = format!("rs-malformed-{case}");
        let actions = install_exact_batch(&store, &session_id);
        let mut outcomes = exact_outcomes(&actions, None);
        match mutation {
            Mutation::Missing => {
                outcomes.pop();
            }
            Mutation::Duplicate => outcomes[1] = outcomes[0].clone(),
            Mutation::WrongIndex => outcomes[1].absolute_index = 99,
            Mutation::WrongHash => outcomes[1].action_hash = "wrong".to_string(),
            Mutation::WrongTarget => {
                outcomes[1].target = ServiceReplicaKey::new("api", 2).unwrap();
            }
            Mutation::WrongKind => {
                outcomes[1].action_kind = crate::executor::ReconcileActionKind::Remove;
            }
        }
        assert!(
            store
                .commit_reconcile_batch(
                    &session_id,
                    "exact-batch",
                    "exact-operation",
                    0,
                    &actions,
                    &outcomes,
                )
                .is_err(),
            "case {case} unexpectedly committed"
        );
        assert_uncommitted_exact_batch(&store, &session_id);
    }
}

#[test]
fn exact_batch_commit_failpoints_roll_back_audit_session_and_progress() {
    for (suffix, failpoint) in [
        (
            "audit",
            ReconcileBatchCommitFailpoint::AfterAuditTerminalization,
        ),
        ("session", ReconcileBatchCommitFailpoint::AfterSessionCas),
    ] {
        let store = StateStore::in_memory().unwrap();
        let session_id = format!("rs-failpoint-{suffix}");
        let actions = install_exact_batch(&store, &session_id);
        assert!(
            store
                .commit_reconcile_batch_with_failpoint(
                    &session_id,
                    "exact-batch",
                    "exact-operation",
                    0,
                    &actions,
                    &exact_outcomes(&actions, None),
                    failpoint,
                )
                .is_err()
        );
        assert_uncommitted_exact_batch(&store, &session_id);
    }
}

#[test]
fn exact_batch_commit_cas_is_idempotent_only_for_identical_outcomes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("exact-batch-cas.db");
    let first = StateStore::open_with_pragmas(&path, StateStorePragmas::daemon_defaults()).unwrap();
    let actions = install_exact_batch(&first, "rs-exact-cas");
    let second =
        StateStore::open_with_pragmas(&path, StateStorePragmas::daemon_defaults()).unwrap();
    let outcomes = exact_outcomes(&actions, None);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let (won, identical) = std::thread::scope(|scope| {
        let first_barrier = barrier.clone();
        let second_barrier = barrier.clone();
        let first_actions = actions.clone();
        let second_actions = actions.clone();
        let first_outcomes = outcomes.clone();
        let second_outcomes = outcomes.clone();
        let first_commit = scope.spawn(move || {
            first_barrier.wait();
            first.commit_reconcile_batch(
                "rs-exact-cas",
                "exact-batch",
                "exact-operation",
                0,
                &first_actions,
                &first_outcomes,
            )
        });
        let second_commit = scope.spawn(move || {
            second_barrier.wait();
            second.commit_reconcile_batch(
                "rs-exact-cas",
                "exact-batch",
                "exact-operation",
                0,
                &second_actions,
                &second_outcomes,
            )
        });
        (
            first_commit.join().unwrap().unwrap(),
            second_commit.join().unwrap().unwrap(),
        )
    });
    assert_eq!(won, identical);

    let mut conflicting = outcomes;
    conflicting[1].result = crate::executor::ActionOutcomeResult::Failed {
        error: "different".to_string(),
    };
    assert!(
        StateStore::open_with_pragmas(&path, StateStorePragmas::daemon_defaults())
            .unwrap()
            .commit_reconcile_batch(
                "rs-exact-cas",
                "exact-batch",
                "exact-operation",
                0,
                &actions,
                &conflicting,
            )
            .is_err()
    );
}

#[test]
fn exact_batch_prevents_audit_stack_misattribution_and_preserves_idempotent_replay() {
    let store = StateStore::in_memory().unwrap();
    let actions = install_exact_batch(&store, "rs-exact-audit-stack");
    let outcomes = exact_outcomes(&actions, None);
    store
        .commit_reconcile_batch(
            "rs-exact-audit-stack",
            "exact-batch",
            "exact-operation",
            0,
            &actions,
            &outcomes,
        )
        .unwrap();
    let tamper = store.conn.execute(
        "UPDATE reconcile_audit_log SET stack_name = 'misattributed-stack'
             WHERE session_id = 'rs-exact-audit-stack' AND action_index = 1",
        [],
    );
    assert!(tamper.unwrap_err().to_string().contains("immutable"));
    store
        .commit_reconcile_batch(
            "rs-exact-audit-stack",
            "exact-batch",
            "exact-operation",
            0,
            &actions,
            &outcomes,
        )
        .unwrap();
}

#[test]
fn started_exact_batch_reopens_with_old_cursor_for_exact_replay() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("exact-batch-reopen.db");
    let actions = {
        let store =
            StateStore::open_with_pragmas(&path, StateStorePragmas::daemon_defaults()).unwrap();
        install_exact_batch(&store, "rs-exact-reopen")
    };
    let reopened =
        StateStore::open_with_pragmas(&path, StateStorePragmas::daemon_defaults()).unwrap();
    assert_uncommitted_exact_batch(&reopened, "rs-exact-reopen");
    reopened
        .start_reconcile_batch(
            "rs-exact-reopen",
            "exact-batch",
            "exact-operation",
            0,
            &actions,
        )
        .unwrap();
    assert_uncommitted_exact_batch(&reopened, "rs-exact-reopen");
}

#[test]
fn exact_batch_start_mismatch_rolls_back_without_audit_rows() {
    let actions = exact_batch_actions();
    for mismatch in ["slice", "cursor"] {
        let store = StateStore::in_memory().unwrap();
        let session_id = format!("rs-start-{mismatch}");
        let session = ReconcileSession {
            session_id: session_id.clone(),
            stack_name: "exact-batch".to_string(),
            operation_id: "exact-operation".to_string(),
            status: ReconcileSessionStatus::Active,
            actions_hash: crate::reconcile::compute_actions_hash(&actions),
            next_action_index: 0,
            total_actions: actions.len(),
            started_at: 1_700_000_000,
            updated_at: 1_700_000_000,
            completed_at: None,
        };
        store.create_reconcile_batch(&session, &actions).unwrap();
        if mismatch == "cursor" {
            store
                .conn
                .execute(
                    "UPDATE reconcile_progress SET next_action_index = 1
                     WHERE stack_name = 'exact-batch'",
                    [],
                )
                .unwrap();
        }
        let dispatched = if mismatch == "slice" {
            &actions[1..]
        } else {
            &actions[..]
        };
        assert!(
            store
                .start_reconcile_batch(
                    &session_id,
                    "exact-batch",
                    "exact-operation",
                    0,
                    dispatched,
                )
                .is_err()
        );
        assert!(
            store
                .load_audit_log_for_session(&session_id)
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn exact_batch_start_failpoint_rolls_back_all_started_claims() {
    let store = StateStore::in_memory().unwrap();
    let actions = exact_batch_actions_for_claim(&store);
    let session = ReconcileSession {
        session_id: "rs-start-rollback".to_string(),
        stack_name: "exact-batch".to_string(),
        operation_id: "exact-operation".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 1,
        updated_at: 1,
        completed_at: None,
    };
    store.create_reconcile_batch(&session, &actions).unwrap();

    let error = store
        .start_reconcile_batch_with_failpoint(
            &session.session_id,
            &session.stack_name,
            &session.operation_id,
            0,
            &actions,
            ReconcileBatchStartFailpoint::AfterFirstAuditInsert,
        )
        .unwrap_err();
    assert!(error.to_string().contains("injected reconcile batch start"));
    assert!(
        store
            .load_audit_log_for_session(&session.session_id)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .load_reconcile_progress(&session.stack_name)
            .unwrap()
            .unwrap()
            .next_action_index,
        0
    );
}

#[test]
fn exact_batch_later_stale_precondition_claims_nothing() {
    let store = StateStore::in_memory().unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let stale = Action::ServiceRecreate {
        target: seeded[1].target().clone(),
        precondition: crate::reconcile::ReplicaPrecondition::new(
            seeded[1].precondition().workload().clone(),
            seeded[1].precondition().environment_generation() + 1,
            seeded[1].precondition().journal_head().clone(),
        )
        .unwrap(),
    };
    let actions = vec![seeded[0].clone(), stale];
    let session = ReconcileSession {
        session_id: "rs-start-later-stale".to_string(),
        stack_name: "exact-batch".to_string(),
        operation_id: "exact-operation".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 1,
        updated_at: 1,
        completed_at: None,
    };
    store.create_reconcile_batch(&session, &actions).unwrap();

    let error = store
        .start_reconcile_batch(
            &session.session_id,
            &session.stack_name,
            &session.operation_id,
            0,
            &actions,
        )
        .unwrap_err();
    assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
    assert!(
        store
            .load_audit_log_for_session(&session.session_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn claim_admission_rejects_every_stale_precondition_dimension_without_mutation() {
    for dimension in [
        "project",
        "environment",
        "machine",
        "machine_incarnation",
        "stack",
        "environment_generation",
        "reservation",
        "service_generation",
        "binding_absent",
        "binding_present_for_unbound",
        "container_id",
        "runtime_generation",
        "ownership_incarnation",
        "target_service",
        "target_replica",
        "never_for_journaled",
        "exact_for_never",
    ] {
        let store = StateStore::in_memory().unwrap();
        let seeded = exact_batch_actions_for_claim(&store);
        let mut target = seeded[1].target().clone();
        let mut workload = seeded[1].precondition().workload().clone();
        let mut environment_generation = seeded[1].precondition().environment_generation();
        let (mut reservation_id, mut service_generation, mut ownership) =
            match seeded[1].precondition().journal_head() {
                crate::reconcile::ExpectedJournalHead::Exact {
                    reservation_id,
                    service_generation,
                    ownership,
                } => (
                    reservation_id.clone(),
                    *service_generation,
                    ownership.clone(),
                ),
                crate::reconcile::ExpectedJournalHead::NeverJournaled => unreachable!(),
            };
        let mut kind = "recreate";

        match dimension {
            "project" => {
                workload.project_id = ProjectId::new("prj_foreign_claim").unwrap();
                ownership
                    .as_mut()
                    .unwrap()
                    .scope
                    .as_mut()
                    .unwrap()
                    .project_id = workload.project_id.clone();
            }
            "environment" => {
                workload.environment_id = EnvironmentId::new("env_foreign_claim").unwrap();
                ownership
                    .as_mut()
                    .unwrap()
                    .scope
                    .as_mut()
                    .unwrap()
                    .environment_id = workload.environment_id.clone();
            }
            "machine" => {
                workload.machine_id = MachineId::new("mch_foreign_claim").unwrap();
                ownership
                    .as_mut()
                    .unwrap()
                    .scope
                    .as_mut()
                    .unwrap()
                    .machine_id = workload.machine_id.clone();
            }
            "machine_incarnation" => {
                workload.machine_incarnation_id =
                    MachineIncarnationId::new("inc_foreign_claim").unwrap();
            }
            "stack" => {
                workload.stack_id = "foreign-claim-stack".to_string();
                let ownership = ownership.as_mut().unwrap();
                ownership.stack_id = workload.stack_id.clone();
                ownership.scope.as_mut().unwrap().stack_id = workload.stack_id.clone();
            }
            "environment_generation" => environment_generation += 1,
            "reservation" => {
                reservation_id = "reservation-foreign-claim".to_string();
                ownership
                    .as_mut()
                    .unwrap()
                    .scope
                    .as_mut()
                    .unwrap()
                    .reservation_id = reservation_id.clone();
            }
            "service_generation" => service_generation += 1,
            "binding_absent" => {
                ownership = None;
                kind = "remove";
            }
            "binding_present_for_unbound" => {
                target = seeded[0].target().clone();
                let project = store
                    .load_project_state(workload.project_id.as_str())
                    .unwrap()
                    .unwrap();
                let environment = project
                    .environments
                    .iter()
                    .find(|environment| environment.environment_id == workload.environment_id)
                    .unwrap();
                let selector = StackContainerCreateSelector {
                    project_id: workload.project_id.clone(),
                    environment_id: workload.environment_id.clone(),
                    machine_id: workload.machine_id.clone(),
                    machine_incarnation_id: workload.machine_incarnation_id.clone(),
                    environment_generation,
                    stack_id: workload.stack_id.clone(),
                    service_name: target.service_name.clone(),
                    replica_index: target.index(),
                    requested_container_id: "ctr-unbound-claim".to_string(),
                    definition_digest: environment.definition_digest.clone(),
                    action_digest: "sha256:unbound-claim".to_string(),
                    applied_config_digest: "vzsc1-sha256:unbound-claim".to_string(),
                };
                let intent = store
                    .resolve_or_begin_stack_container_create(&selector, 10)
                    .unwrap()
                    .0;
                reservation_id = intent.scope.reservation_id.clone();
                service_generation = intent.service_generation;
                ownership = Some(ContainerGenerationOwnership {
                    container_id: intent.requested_container_id,
                    generation: 99,
                    stack_id: intent.scope.stack_id.clone(),
                    scope: Some(Box::new(intent.scope)),
                });
                kind = "remove";
            }
            "container_id" => {
                ownership.as_mut().unwrap().container_id = "ctr-foreign-claim".to_string()
            }
            "runtime_generation" => ownership.as_mut().unwrap().generation += 1,
            "ownership_incarnation" => {
                ownership
                    .as_mut()
                    .unwrap()
                    .scope
                    .as_mut()
                    .unwrap()
                    .machine_incarnation_id =
                    Some(MachineIncarnationId::new("inc_ownership_foreign").unwrap());
            }
            "target_service" => target = ServiceReplicaKey::new("api-foreign", 1).unwrap(),
            "target_replica" => target = ServiceReplicaKey::new("api-2", 2).unwrap(),
            "never_for_journaled" => {
                kind = "create-never";
            }
            "exact_for_never" => {
                target = seeded[0].target().clone();
                reservation_id = "reservation-missing-claim".to_string();
                service_generation = 1;
                ownership = None;
                kind = "create";
            }
            _ => unreachable!(),
        }

        let journal_head = if kind == "create-never" {
            crate::reconcile::ExpectedJournalHead::NeverJournaled
        } else {
            crate::reconcile::ExpectedJournalHead::exact(
                &reservation_id,
                service_generation,
                ownership,
            )
            .unwrap()
        };
        let precondition = crate::reconcile::ReplicaPrecondition::new(
            workload,
            environment_generation,
            journal_head,
        )
        .unwrap();
        let action = match kind {
            "recreate" => Action::ServiceRecreate {
                target,
                precondition,
            },
            "remove" => Action::ServiceRemove {
                target,
                precondition,
            },
            "create" | "create-never" => Action::ServiceCreate {
                target,
                precondition,
            },
            _ => unreachable!(),
        };
        let actions = vec![action];
        let session_id = format!("rs-claim-dimension-{dimension}");
        install_unstarted_batch(&store, &session_id, "op-claim-dimension", &actions);

        let workload = seeded[1].precondition().workload();
        let project_before = store
            .load_project_state(workload.project_id.as_str())
            .unwrap();
        let observed_before = store.load_observed_state("exact-batch").unwrap();
        let journal_before = store.list_resumable_stack_container_creates().unwrap();
        let error = store
            .start_reconcile_batch(
                &session_id,
                actions[0].precondition().workload().stack_id.as_str(),
                "op-claim-dimension",
                0,
                &actions,
            )
            .unwrap_err();
        assert_eq!(
            error.machine_code(),
            MachineErrorCode::StateConflict,
            "dimension {dimension} returned {error}"
        );
        assert!(
            store
                .load_audit_log_for_session(&session_id)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .load_project_state(workload.project_id.as_str())
                .unwrap(),
            project_before
        );
        assert_eq!(
            store.load_observed_state("exact-batch").unwrap(),
            observed_before
        );
        assert_eq!(
            store.list_resumable_stack_container_creates().unwrap(),
            journal_before
        );
        assert_eq!(
            store.load_reconcile_session_actions(&session_id).unwrap(),
            actions,
            "session action/hash identity must remain self-consistent for {dimension}"
        );
    }
}

#[test]
fn claim_admission_rejects_never_journaled_after_failed_or_cleaned_aba() {
    for terminal in ["failed", "cleaned"] {
        let store = StateStore::in_memory().unwrap();
        let seeded = exact_batch_actions_for_claim(&store);
        let actions = vec![seeded[0].clone()];
        let session_id = format!("rs-never-aba-{terminal}");
        install_unstarted_batch(&store, &session_id, "op-never-aba", &actions);

        let workload = actions[0].precondition().workload();
        let project = store
            .load_project_state(workload.project_id.as_str())
            .unwrap()
            .unwrap();
        let environment = project
            .environments
            .iter()
            .find(|environment| environment.environment_id == workload.environment_id)
            .unwrap();
        let selector = StackContainerCreateSelector {
            project_id: workload.project_id.clone(),
            environment_id: workload.environment_id.clone(),
            machine_id: workload.machine_id.clone(),
            machine_incarnation_id: workload.machine_incarnation_id.clone(),
            environment_generation: environment.lifecycle_generation,
            stack_id: workload.stack_id.clone(),
            service_name: "api".to_string(),
            replica_index: 2,
            requested_container_id: format!("ctr-never-aba-{terminal}"),
            definition_digest: environment.definition_digest.clone(),
            action_digest: format!("sha256:never-aba-{terminal}"),
            applied_config_digest: "vzsc1-sha256:never-aba".to_string(),
        };
        let intent = store
            .resolve_or_begin_stack_container_create(&selector, 10)
            .unwrap()
            .0;
        if terminal == "failed" {
            store
                .publish_stack_container_create_failure(
                    &intent.scope.reservation_id,
                    "failed before stale claim",
                    11,
                )
                .unwrap();
        } else {
            let ownership = ContainerGenerationOwnership {
                container_id: intent.requested_container_id.clone(),
                generation: 19,
                stack_id: intent.scope.stack_id.clone(),
                scope: Some(Box::new(intent.scope.clone())),
            };
            store
                .bind_stack_container_generation(&StackContainerGenerationBinding {
                    reservation_id: intent.scope.reservation_id.clone(),
                    service_name: intent.service_name.clone(),
                    ownership,
                    bound_at: 11,
                })
                .unwrap();
            store
                .publish_stack_container_create_success(&intent.scope.reservation_id, true, 12)
                .unwrap();
            store
                .begin_stack_container_cleanup(&intent.scope.reservation_id, 13)
                .unwrap();
            store
                .publish_stack_container_cleanup_success(&intent.scope.reservation_id, 14)
                .unwrap();
        }

        let error = store
            .start_reconcile_batch(&session_id, &workload.stack_id, "op-never-aba", 0, &actions)
            .unwrap_err();
        assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
        assert!(
            store
                .load_audit_log_for_session(&session_id)
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn started_claim_replay_accepts_own_cleanup_progression_and_rejects_foreign_session() {
    let store = StateStore::in_memory().unwrap();
    let actions = exact_batch_actions_for_claim(&store);
    install_unstarted_batch(&store, "rs-claim-owner", "op-claim-owner", &actions);
    let first_claims = store
        .start_reconcile_batch(
            "rs-claim-owner",
            "exact-batch",
            "op-claim-owner",
            0,
            &actions,
        )
        .unwrap();
    store
        .begin_claimed_predecessor_cleanup(&first_claims[1], 200)
        .unwrap();

    let replayed = store
        .start_reconcile_batch(
            "rs-claim-owner",
            "exact-batch",
            "op-claim-owner",
            0,
            &actions,
        )
        .unwrap();
    assert_eq!(replayed, first_claims);
    assert_eq!(
        store
            .load_audit_log_for_session("rs-claim-owner")
            .unwrap()
            .len(),
        actions.len()
    );

    let foreign = ReconcileSession {
        session_id: "rs-claim-foreign".to_string(),
        stack_name: "exact-batch".to_string(),
        operation_id: "op-claim-foreign".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 2,
        updated_at: 2,
        completed_at: None,
    };
    store.create_reconcile_session(&foreign, &actions).unwrap();
    store
        .save_reconcile_progress("exact-batch", "op-claim-foreign", &actions, 0)
        .unwrap();
    let error = store
        .start_reconcile_batch(
            &foreign.session_id,
            &foreign.stack_name,
            &foreign.operation_id,
            0,
            &actions,
        )
        .unwrap_err();
    assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
    assert!(
        store
            .load_audit_log_for_session(&foreign.session_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn started_create_claim_replays_exact_predecessor_cleanup_before_successor_intent() {
    let store = StateStore::in_memory().unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let reservation_id = match seeded[1].precondition().journal_head() {
        crate::reconcile::ExpectedJournalHead::Exact { reservation_id, .. } => {
            reservation_id.clone()
        }
        crate::reconcile::ExpectedJournalHead::NeverJournaled => unreachable!(),
    };
    store
        .publish_stack_container_blocked(
            &reservation_id,
            "claimed create must clean exact predecessor",
            200,
        )
        .unwrap();
    let actions = vec![Action::ServiceCreate {
        target: seeded[1].target().clone(),
        precondition: seeded[1].precondition().clone(),
    }];
    install_unstarted_batch(
        &store,
        "rs-create-cleanup-replay",
        "op-create-cleanup",
        &actions,
    );
    let first = store
        .start_reconcile_batch(
            "rs-create-cleanup-replay",
            "exact-batch",
            "op-create-cleanup",
            0,
            &actions,
        )
        .unwrap();

    store
        .begin_claimed_predecessor_cleanup(&first[0], 201)
        .unwrap();
    assert_eq!(
        store
            .start_reconcile_batch(
                "rs-create-cleanup-replay",
                "exact-batch",
                "op-create-cleanup",
                0,
                &actions,
            )
            .unwrap(),
        first
    );
    store
        .complete_claimed_predecessor_cleanup(&first[0], 202)
        .unwrap();
    assert_eq!(
        store
            .start_reconcile_batch(
                "rs-create-cleanup-replay",
                "exact-batch",
                "op-create-cleanup",
                0,
                &actions,
            )
            .unwrap(),
        first
    );
}

#[test]
fn started_recreate_claim_rejects_unowned_blocked_predecessor_progression() {
    let store = StateStore::in_memory().unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let actions = vec![seeded[1].clone()];
    install_unstarted_batch(
        &store,
        "rs-recreate-blocked-replay",
        "op-recreate-blocked-replay",
        &actions,
    );
    store
        .start_reconcile_batch(
            "rs-recreate-blocked-replay",
            "exact-batch",
            "op-recreate-blocked-replay",
            0,
            &actions,
        )
        .unwrap();
    let (reservation_id, ownership) = match actions[0].precondition().journal_head() {
        crate::reconcile::ExpectedJournalHead::Exact {
            reservation_id,
            ownership: Some(ownership),
            ..
        } => (reservation_id, ownership),
        _ => unreachable!(),
    };
    let mut intent = store
        .load_stack_container_create_intent(reservation_id)
        .unwrap()
        .unwrap();
    intent.status = StackContainerCreateStatus::Blocked;
    intent.last_error = Some("foreign blocked transition".to_string());
    intent.updated_at += 1;
    let observed = ServiceObservedState {
        replica: actions[0].target().clone(),
        applied_config_digest: None,
        phase: ServicePhase::Failed,
        container_id: Some(ownership.container_id.clone()),
        failed_create_ownership: Some(ownership.clone()),
        last_error: intent.last_error.clone(),
        ready: false,
    };
    store
        .conn
        .execute(
            "UPDATE stack_container_create_intents
             SET status = 'blocked', intent_json = ?1, last_error = ?2, updated_at = ?3
             WHERE reservation_id = ?4",
            params![
                serde_json::to_string(&intent).unwrap(),
                intent.last_error,
                i64::try_from(intent.updated_at).unwrap(),
                reservation_id,
            ],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE observed_state SET state_json = ?1
             WHERE stack_name = 'exact-batch' AND service_name = 'api-2' AND replica_index = 1",
            params![serde_json::to_string(&observed).unwrap()],
        )
        .unwrap();
    assert_eq!(
        store
            .start_reconcile_batch(
                "rs-recreate-blocked-replay",
                "exact-batch",
                "op-recreate-blocked-replay",
                0,
                &actions,
            )
            .unwrap_err()
            .machine_code(),
        MachineErrorCode::StateConflict
    );
}

#[test]
fn claimed_exact_successor_lifecycle_replays_after_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("claimed-successor-reopen.db");
    let store = StateStore::open(&path).unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let actions = vec![seeded[1].clone()];
    install_unstarted_batch(
        &store,
        "rs-claimed-successor",
        "op-claimed-successor",
        &actions,
    );
    let claim = store
        .start_reconcile_batch(
            "rs-claimed-successor",
            "exact-batch",
            "op-claimed-successor",
            0,
            &actions,
        )
        .unwrap()
        .remove(0);

    assert!(matches!(
        store.inspect_claimed_predecessor(&claim).unwrap(),
        ClaimedPredecessorInspection::ExactBoundNeedsCleanup { .. }
    ));
    let stopping = store
        .begin_claimed_predecessor_cleanup(&claim, 200)
        .unwrap();
    assert_eq!(stopping.phase, ServicePhase::Stopping);
    assert_eq!(
        store
            .begin_claimed_predecessor_cleanup(&claim, 200)
            .unwrap(),
        stopping
    );
    assert_eq!(
        store.load_events("exact-batch").unwrap(),
        vec![StackEvent::ServiceStopping {
            stack_name: "exact-batch".to_string(),
            service_name: "api-2".to_string(),
        }],
        "cleanup admission replay must not duplicate ServiceStopping"
    );
    assert!(matches!(
        store.inspect_claimed_predecessor(&claim).unwrap(),
        ClaimedPredecessorInspection::ExactBoundCleanupPending { .. }
    ));
    drop(store);

    let store = StateStore::open(&path).unwrap();
    let stopped = store
        .complete_claimed_predecessor_cleanup(&claim, 201)
        .unwrap();
    assert_eq!(stopped.phase, ServicePhase::Stopped);
    assert_eq!(
        store
            .complete_claimed_predecessor_cleanup(&claim, 201)
            .unwrap(),
        stopped
    );
    assert_eq!(
        store.load_events("exact-batch").unwrap(),
        vec![
            StackEvent::ServiceStopping {
                stack_name: "exact-batch".to_string(),
                service_name: "api-2".to_string(),
            },
            StackEvent::ServiceStopped {
                stack_name: "exact-batch".to_string(),
                service_name: "api-2".to_string(),
                exit_code: 0,
            },
        ],
        "terminal cleanup replay must not duplicate ServiceStopped"
    );
    assert!(matches!(
        store.inspect_claimed_predecessor(&claim).unwrap(),
        ClaimedPredecessorInspection::ExactBoundCleaned { .. }
    ));

    let input = claimed_create_input(&store, &actions[0], "successor-reopen");
    let allocation = empty_claimed_allocator_target();
    let successor = store
        .resolve_or_begin_claimed_successor(&claim, &input, &allocation, 202)
        .unwrap();
    assert_eq!(successor.service_generation, 2);
    assert_eq!(
        store
            .resolve_or_begin_claimed_successor(&claim, &input, &allocation, 202)
            .unwrap(),
        successor
    );
    let binding = binding_for_claimed_intent(&successor, 41, 203);
    assert_eq!(
        store
            .bind_claimed_successor_generation(&claim, &binding)
            .unwrap(),
        binding
    );
    assert_eq!(
        store
            .bind_claimed_successor_generation(&claim, &binding)
            .unwrap(),
        binding
    );
    let receipt = receipt_for_claimed_binding(&binding);
    let running = store
        .publish_claimed_successor_success(
            &claim,
            &successor.scope.reservation_id,
            &receipt,
            true,
            204,
        )
        .unwrap();
    assert_eq!(running.phase, ServicePhase::Running);
    assert!(running.ready);
    assert_eq!(
        store
            .publish_claimed_successor_blocked(
                &claim,
                &successor.scope.reservation_id,
                "late health result",
                205,
            )
            .unwrap_err()
            .machine_code(),
        MachineErrorCode::StateConflict
    );
    assert_eq!(
        store
            .publish_claimed_successor_success(
                &claim,
                &successor.scope.reservation_id,
                &receipt,
                true,
                204,
            )
            .unwrap(),
        running
    );
    assert_eq!(
        store.load_events("exact-batch").unwrap(),
        vec![
            StackEvent::ServiceStopping {
                stack_name: "exact-batch".to_string(),
                service_name: "api-2".to_string(),
            },
            StackEvent::ServiceStopped {
                stack_name: "exact-batch".to_string(),
                service_name: "api-2".to_string(),
                exit_code: 0,
            },
            StackEvent::ServiceCreating {
                stack_name: "exact-batch".to_string(),
                service_name: "api-2".to_string(),
            },
            StackEvent::ServiceReady {
                stack_name: "exact-batch".to_string(),
                service_name: "api-2".to_string(),
                runtime_id: receipt.container_id,
            },
        ],
        "the exact recreate lifecycle is emitted once in journal order"
    );
}

#[test]
fn claimed_successor_success_rejects_foreign_receipts_without_db_changes_and_replays_exactly() {
    let mut store = StateStore::in_memory().unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let actions = vec![seeded[0].clone()];
    install_unstarted_batch(
        &store,
        "rs-claimed-success-receipt",
        "op-claimed-success-receipt",
        &actions,
    );
    let claim = store
        .start_reconcile_batch(
            "rs-claimed-success-receipt",
            "exact-batch",
            "op-claimed-success-receipt",
            0,
            &actions,
        )
        .unwrap()
        .remove(0);
    let (event_tx, event_rx) = std::sync::mpsc::channel();
    store.set_event_sender(event_tx);
    let input = claimed_create_input(&store, &actions[0], "success-receipt");
    let allocation = empty_claimed_allocator_target();
    let intent = store
        .resolve_or_begin_claimed_successor(&claim, &input, &allocation, 300)
        .unwrap();
    assert_eq!(
        store
            .resolve_or_begin_claimed_successor(&claim, &input, &allocation, 300)
            .unwrap(),
        intent,
        "claim-linked intent replay must reuse the exact successor"
    );
    assert_eq!(
        store.load_events("exact-batch").unwrap(),
        vec![StackEvent::ServiceCreating {
            stack_name: "exact-batch".to_string(),
            service_name: "api-2".to_string(),
        }],
        "replica-qualified Creating is emitted exactly once"
    );
    assert_eq!(
        event_rx.try_recv().unwrap(),
        StackEvent::ServiceCreating {
            stack_name: "exact-batch".to_string(),
            service_name: "api-2".to_string(),
        }
    );
    assert_eq!(
        event_rx.try_recv().unwrap_err(),
        std::sync::mpsc::TryRecvError::Empty,
        "intent replay must not notify subscribers twice"
    );
    let binding = binding_for_claimed_intent(&intent, 71, 301);
    store
        .bind_claimed_successor_generation(&claim, &binding)
        .unwrap();

    let reserved_intent = store
        .load_stack_container_create_intent(&intent.scope.reservation_id)
        .unwrap();
    let creating_observed = store
        .load_observed_state_for_replica(
            &intent.scope.stack_id,
            &intent.service_name,
            intent.replica_index,
        )
        .unwrap();

    let mut foreign_ownership = binding.ownership.clone();
    foreign_ownership.generation += 1;
    let ownership_mismatch = ContainerCreateReceipt {
        container_id: binding.ownership.container_id.clone(),
        ownership: Some(foreign_ownership),
    };
    let changes_before = store.conn.total_changes();
    assert_eq!(
        store
            .publish_claimed_successor_success(
                &claim,
                &intent.scope.reservation_id,
                &ownership_mismatch,
                true,
                302,
            )
            .unwrap_err()
            .machine_code(),
        MachineErrorCode::StateConflict
    );
    assert_eq!(store.conn.total_changes(), changes_before);
    assert_eq!(
        store
            .load_stack_container_create_intent(&intent.scope.reservation_id)
            .unwrap(),
        reserved_intent
    );
    assert_eq!(
        store
            .load_observed_state_for_replica(
                &intent.scope.stack_id,
                &intent.service_name,
                intent.replica_index,
            )
            .unwrap(),
        creating_observed
    );

    let container_id_mismatch = ContainerCreateReceipt {
        container_id: "foreign-container-id".to_string(),
        ownership: Some(binding.ownership.clone()),
    };
    let changes_before = store.conn.total_changes();
    assert_eq!(
        store
            .publish_claimed_successor_success(
                &claim,
                &intent.scope.reservation_id,
                &container_id_mismatch,
                true,
                303,
            )
            .unwrap_err()
            .machine_code(),
        MachineErrorCode::StateConflict
    );
    assert_eq!(store.conn.total_changes(), changes_before);
    assert_eq!(
        store
            .load_stack_container_create_intent(&intent.scope.reservation_id)
            .unwrap(),
        reserved_intent
    );
    assert_eq!(
        store
            .load_observed_state_for_replica(
                &intent.scope.stack_id,
                &intent.service_name,
                intent.replica_index,
            )
            .unwrap(),
        creating_observed
    );

    let receipt = receipt_for_claimed_binding(&binding);
    let expected = ServiceObservedState {
        replica: ServiceReplicaKey::new(intent.service_name.clone(), intent.replica_index).unwrap(),
        applied_config_digest: intent.applied_config_digest.clone(),
        phase: ServicePhase::Running,
        container_id: Some(receipt.container_id.clone()),
        failed_create_ownership: receipt.ownership.clone(),
        last_error: None,
        ready: true,
    };
    assert_eq!(
        store
            .publish_claimed_successor_success(
                &claim,
                &intent.scope.reservation_id,
                &receipt,
                true,
                304,
            )
            .unwrap(),
        expected
    );
    assert_eq!(
        store
            .load_observed_state_for_replica(
                &intent.scope.stack_id,
                &intent.service_name,
                intent.replica_index,
            )
            .unwrap(),
        Some(expected.clone())
    );
    let committed_events = vec![
        StackEvent::ServiceCreating {
            stack_name: "exact-batch".to_string(),
            service_name: "api-2".to_string(),
        },
        StackEvent::ServiceReady {
            stack_name: "exact-batch".to_string(),
            service_name: "api-2".to_string(),
            runtime_id: receipt.container_id.clone(),
        },
    ];
    assert_eq!(store.load_events("exact-batch").unwrap(), committed_events);
    assert_eq!(
        event_rx.try_recv().unwrap(),
        committed_events[1],
        "Ready is streamed only after its journal transaction commits"
    );

    let changes_before_replay = store.conn.total_changes();
    assert_eq!(
        store
            .publish_claimed_successor_success(
                &claim,
                &intent.scope.reservation_id,
                &receipt,
                true,
                305,
            )
            .unwrap(),
        expected
    );
    assert_eq!(store.conn.total_changes(), changes_before_replay);
    assert_eq!(
        store.load_events("exact-batch").unwrap(),
        committed_events,
        "terminal success replay must not duplicate lifecycle events"
    );
    assert_eq!(
        event_rx.try_recv().unwrap_err(),
        std::sync::mpsc::TryRecvError::Empty,
        "terminal success replay must not notify subscribers twice"
    );
}

#[test]
fn claimed_successor_post_publication_failure_cleanup_commits_events_once_and_reopens() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("claimed-successor-cleanup-events.db");
    let (claim, reservation_id, stopping) = {
        let mut store = StateStore::open(&path).unwrap();
        let seeded = exact_batch_actions_for_claim(&store);
        let actions = vec![seeded[0].clone()];
        install_unstarted_batch(
            &store,
            "rs-claimed-cleanup-events",
            "op-claimed-cleanup-events",
            &actions,
        );
        let claim = store
            .start_reconcile_batch(
                "rs-claimed-cleanup-events",
                "exact-batch",
                "op-claimed-cleanup-events",
                0,
                &actions,
            )
            .unwrap()
            .remove(0);
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        store.set_event_sender(event_tx);
        let input = claimed_create_input(&store, &actions[0], "cleanup-events");
        let successor = store
            .resolve_or_begin_claimed_successor(
                &claim,
                &input,
                &empty_claimed_allocator_target(),
                400,
            )
            .unwrap();
        assert!(matches!(
            event_rx.try_recv().unwrap(),
            StackEvent::ServiceCreating { .. }
        ));
        let reservation_id = successor.scope.reservation_id.clone();
        let binding = binding_for_claimed_intent(&successor, 81, 401);
        store
            .bind_claimed_successor_generation(&claim, &binding)
            .unwrap();
        let reserved_intent = store
            .load_stack_container_create_intent(&reservation_id)
            .unwrap()
            .unwrap();
        let creating = store
            .load_observed_state_for_replica("exact-batch", "api", 2)
            .unwrap()
            .unwrap();
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER reject_successor_failed_event
                 BEFORE INSERT ON events
                 BEGIN SELECT RAISE(ABORT, 'injected failed event failure'); END;",
            )
            .unwrap();
        assert!(
            store
                .publish_claimed_successor_failure(
                    &claim,
                    &reservation_id,
                    "runtime published but activation acknowledgement failed",
                    402,
                )
                .is_err()
        );
        assert_eq!(
            store
                .load_stack_container_create_intent(&reservation_id)
                .unwrap()
                .unwrap(),
            reserved_intent,
            "failure event persistence error must roll back the journal failure"
        );
        assert_eq!(
            store
                .load_observed_state_for_replica("exact-batch", "api", 2)
                .unwrap()
                .unwrap(),
            creating,
            "failure event persistence error must roll back observed failure"
        );
        assert_eq!(
            event_rx.try_recv().unwrap_err(),
            std::sync::mpsc::TryRecvError::Empty,
            "rolled-back failure must not notify subscribers"
        );
        store
            .conn
            .execute_batch("DROP TRIGGER reject_successor_failed_event;")
            .unwrap();
        let failed = store
            .publish_claimed_successor_failure(
                &claim,
                &reservation_id,
                "runtime published but activation acknowledgement failed",
                402,
            )
            .unwrap();
        assert_eq!(failed.phase, ServicePhase::Failed);
        let failed_event = StackEvent::ServiceFailed {
            stack_name: "exact-batch".to_string(),
            service_name: "api-2".to_string(),
            error: "runtime published but activation acknowledgement failed".to_string(),
        };
        assert_eq!(event_rx.try_recv().unwrap(), failed_event.clone());
        assert_eq!(
            store
                .publish_claimed_successor_failure(
                    &claim,
                    &reservation_id,
                    "runtime published but activation acknowledgement failed",
                    402,
                )
                .unwrap(),
            failed
        );
        assert_eq!(
            event_rx.try_recv().unwrap_err(),
            std::sync::mpsc::TryRecvError::Empty,
            "failure replay must not notify subscribers twice"
        );
        let blocked_intent = store
            .load_stack_container_create_intent(&reservation_id)
            .unwrap()
            .unwrap();

        store
            .conn
            .execute_batch(
                "CREATE TRIGGER reject_successor_stopping_event
                 BEFORE INSERT ON events
                 BEGIN SELECT RAISE(ABORT, 'injected stopping event failure'); END;",
            )
            .unwrap();
        assert!(
            store
                .begin_claimed_successor_cleanup(&claim, &reservation_id, 403)
                .is_err()
        );
        assert_eq!(
            store
                .load_stack_container_create_intent(&reservation_id)
                .unwrap()
                .unwrap(),
            blocked_intent,
            "event persistence failure must roll back CleanupPending"
        );
        assert_eq!(
            store
                .load_observed_state_for_replica("exact-batch", "api", 2)
                .unwrap(),
            Some(failed),
            "event persistence failure must roll back Stopping observation"
        );
        assert_eq!(
            store.load_events("exact-batch").unwrap(),
            vec![
                StackEvent::ServiceCreating {
                    stack_name: "exact-batch".to_string(),
                    service_name: "api-2".to_string(),
                },
                failed_event.clone(),
            ]
        );
        assert_eq!(
            event_rx.try_recv().unwrap_err(),
            std::sync::mpsc::TryRecvError::Empty,
            "rolled-back events must not reach subscribers"
        );
        store
            .conn
            .execute_batch("DROP TRIGGER reject_successor_stopping_event;")
            .unwrap();

        let stopping = store
            .begin_claimed_successor_cleanup(&claim, &reservation_id, 404)
            .unwrap();
        assert_eq!(stopping.phase, ServicePhase::Stopping);
        let stopping_event = StackEvent::ServiceStopping {
            stack_name: "exact-batch".to_string(),
            service_name: "api-2".to_string(),
        };
        assert_eq!(event_rx.try_recv().unwrap(), stopping_event);
        assert_eq!(
            store
                .begin_claimed_successor_cleanup(&claim, &reservation_id, 405)
                .unwrap(),
            stopping
        );
        assert_eq!(
            event_rx.try_recv().unwrap_err(),
            std::sync::mpsc::TryRecvError::Empty,
            "cleanup admission replay must not notify subscribers twice"
        );
        assert_eq!(
            store.load_events("exact-batch").unwrap(),
            vec![
                StackEvent::ServiceCreating {
                    stack_name: "exact-batch".to_string(),
                    service_name: "api-2".to_string(),
                },
                failed_event,
                stopping_event,
            ]
        );
        (claim, reservation_id, stopping)
    };

    let mut store = StateStore::open(&path).unwrap();
    assert_eq!(
        store
            .load_observed_state_for_replica("exact-batch", "api", 2)
            .unwrap(),
        Some(stopping.clone()),
        "Stopping observation and event must survive reopen together"
    );
    let (event_tx, event_rx) = std::sync::mpsc::channel();
    store.set_event_sender(event_tx);
    let cleanup_pending_intent = store
        .load_stack_container_create_intent(&reservation_id)
        .unwrap()
        .unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_successor_stopped_event
             BEFORE INSERT ON events
             BEGIN SELECT RAISE(ABORT, 'injected stopped event failure'); END;",
        )
        .unwrap();
    assert!(
        store
            .complete_claimed_successor_cleanup(&claim, &reservation_id, 406)
            .is_err()
    );
    assert_eq!(
        store
            .load_stack_container_create_intent(&reservation_id)
            .unwrap()
            .unwrap(),
        cleanup_pending_intent,
        "event persistence failure must roll back Cleaned"
    );
    assert_eq!(
        store
            .load_observed_state_for_replica("exact-batch", "api", 2)
            .unwrap(),
        Some(stopping),
        "event persistence failure must roll back Stopped observation"
    );
    assert_eq!(
        event_rx.try_recv().unwrap_err(),
        std::sync::mpsc::TryRecvError::Empty,
        "rolled-back terminal events must not reach subscribers"
    );
    store
        .conn
        .execute_batch("DROP TRIGGER reject_successor_stopped_event;")
        .unwrap();

    let stopped = store
        .complete_claimed_successor_cleanup(&claim, &reservation_id, 407)
        .unwrap();
    assert_eq!(stopped.phase, ServicePhase::Stopped);
    let stopped_event = StackEvent::ServiceStopped {
        stack_name: "exact-batch".to_string(),
        service_name: "api-2".to_string(),
        exit_code: 0,
    };
    assert_eq!(event_rx.try_recv().unwrap(), stopped_event);
    assert_eq!(
        store
            .complete_claimed_successor_cleanup(&claim, &reservation_id, 408)
            .unwrap(),
        stopped
    );
    assert_eq!(
        event_rx.try_recv().unwrap_err(),
        std::sync::mpsc::TryRecvError::Empty,
        "terminal cleanup replay must not notify subscribers twice"
    );
    assert_eq!(
        store.load_events("exact-batch").unwrap(),
        vec![
            StackEvent::ServiceCreating {
                stack_name: "exact-batch".to_string(),
                service_name: "api-2".to_string(),
            },
            StackEvent::ServiceFailed {
                stack_name: "exact-batch".to_string(),
                service_name: "api-2".to_string(),
                error: "runtime published but activation acknowledgement failed".to_string(),
            },
            StackEvent::ServiceStopping {
                stack_name: "exact-batch".to_string(),
                service_name: "api-2".to_string(),
            },
            stopped_event,
        ],
        "post-publication failure cleanup events are durable and emitted exactly once"
    );
}

#[test]
fn claimed_successor_failure_is_replayable_and_bound_cleanup_is_explicit() {
    for bound in [false, true] {
        let store = StateStore::in_memory().unwrap();
        let seeded = exact_batch_actions_for_claim(&store);
        let actions = vec![seeded[0].clone()];
        let session_id = if bound {
            "rs-claimed-bound-failure"
        } else {
            "rs-claimed-unbound-failure"
        };
        install_unstarted_batch(&store, session_id, "op-claimed-failure", &actions);
        let claim = store
            .start_reconcile_batch(session_id, "exact-batch", "op-claimed-failure", 0, &actions)
            .unwrap()
            .remove(0);
        let input = claimed_create_input(
            &store,
            &actions[0],
            if bound {
                "bound-failure"
            } else {
                "unbound-failure"
            },
        );
        let allocation = empty_claimed_allocator_target();
        let successor = store
            .resolve_or_begin_claimed_successor(&claim, &input, &allocation, 210)
            .unwrap();
        if bound {
            let binding = binding_for_claimed_intent(&successor, 61, 211);
            store
                .bind_claimed_successor_generation(&claim, &binding)
                .unwrap();
            assert_eq!(
                store
                    .begin_claimed_successor_cleanup(&claim, &successor.scope.reservation_id, 211,)
                    .unwrap_err()
                    .machine_code(),
                MachineErrorCode::StateConflict
            );
            let blocked = store
                .publish_claimed_successor_blocked(
                    &claim,
                    &successor.scope.reservation_id,
                    "activation failed exactly",
                    212,
                )
                .unwrap();
            assert_eq!(
                store
                    .publish_claimed_successor_blocked(
                        &claim,
                        &successor.scope.reservation_id,
                        "activation failed exactly",
                        212,
                    )
                    .unwrap(),
                blocked
            );
        } else {
            let before = store
                .load_stack_container_create_intent(&successor.scope.reservation_id)
                .unwrap()
                .unwrap();
            assert_eq!(
                store
                    .publish_claimed_successor_blocked(
                        &claim,
                        &successor.scope.reservation_id,
                        "unbound intent must remain bindable",
                        212,
                    )
                    .unwrap_err()
                    .machine_code(),
                MachineErrorCode::StateConflict
            );
            assert_eq!(
                store
                    .load_stack_container_create_intent(&successor.scope.reservation_id)
                    .unwrap()
                    .unwrap(),
                before
            );
        }
        let failed = store
            .publish_claimed_successor_failure(
                &claim,
                &successor.scope.reservation_id,
                "activation failed exactly",
                212,
            )
            .unwrap();
        assert_eq!(failed.phase, ServicePhase::Failed);
        assert_eq!(
            store
                .publish_claimed_successor_failure(
                    &claim,
                    &successor.scope.reservation_id,
                    "activation failed exactly",
                    212,
                )
                .unwrap(),
            failed
        );
        assert_eq!(
            store
                .publish_claimed_successor_failure(
                    &claim,
                    &successor.scope.reservation_id,
                    "different activation evidence",
                    212,
                )
                .unwrap_err()
                .machine_code(),
            MachineErrorCode::StateConflict
        );
        let failed_intent = store
            .load_stack_container_create_intent(&successor.scope.reservation_id)
            .unwrap()
            .unwrap();
        if bound {
            assert_eq!(failed_intent.status, StackContainerCreateStatus::Blocked);
            assert_eq!(
                store
                    .publish_claimed_successor_blocked(
                        &claim,
                        &successor.scope.reservation_id,
                        "activation failed exactly",
                        212,
                    )
                    .unwrap(),
                failed
            );
            assert_eq!(
                store
                    .publish_claimed_successor_blocked(
                        &claim,
                        &successor.scope.reservation_id,
                        "different blocked evidence",
                        212,
                    )
                    .unwrap_err()
                    .machine_code(),
                MachineErrorCode::StateConflict
            );
            let stopping = store
                .begin_claimed_successor_cleanup(&claim, &successor.scope.reservation_id, 213)
                .unwrap();
            assert_eq!(stopping.phase, ServicePhase::Stopping);
            assert_eq!(
                store
                    .begin_claimed_successor_cleanup(&claim, &successor.scope.reservation_id, 213,)
                    .unwrap(),
                stopping
            );
            let stopped = store
                .complete_claimed_successor_cleanup(&claim, &successor.scope.reservation_id, 214)
                .unwrap();
            assert_eq!(stopped.phase, ServicePhase::Stopped);
            assert_eq!(
                store
                    .complete_claimed_successor_cleanup(
                        &claim,
                        &successor.scope.reservation_id,
                        214,
                    )
                    .unwrap(),
                stopped
            );
        } else {
            assert_eq!(failed_intent.status, StackContainerCreateStatus::Failed);
        }
    }
}

#[test]
fn claimed_unbound_remove_requires_explicit_absent_or_discovered_cleanup_decision() {
    for initial_status in [
        StackContainerCreateStatus::Intent,
        StackContainerCreateStatus::Blocked,
    ] {
        let status_label = match initial_status {
            StackContainerCreateStatus::Intent => "intent",
            StackContainerCreateStatus::Blocked => "blocked",
            _ => unreachable!(),
        };
        let store = StateStore::in_memory().unwrap();
        let seeded = exact_batch_actions_for_claim(&store);
        let create = &seeded[0];
        let workload = create.precondition().workload();
        let input = claimed_create_input(&store, create, status_label);
        let selector = StackContainerCreateSelector {
            project_id: workload.project_id.clone(),
            environment_id: workload.environment_id.clone(),
            machine_id: workload.machine_id.clone(),
            machine_incarnation_id: workload.machine_incarnation_id.clone(),
            environment_generation: create.precondition().environment_generation(),
            stack_id: workload.stack_id.clone(),
            service_name: create.target().service_name.clone(),
            replica_index: create.target().index(),
            requested_container_id: input.requested_container_id,
            definition_digest: input.definition_digest,
            action_digest: format!("sha256:unbound-{status_label}"),
            applied_config_digest: input.applied_config_digest,
        };
        let intent = store
            .resolve_or_begin_stack_container_create(&selector, 150)
            .unwrap()
            .0;
        if initial_status == StackContainerCreateStatus::Blocked {
            store
                .publish_stack_container_blocked(
                    &intent.scope.reservation_id,
                    "runtime inspection required",
                    151,
                )
                .unwrap();
        }
        let action = Action::ServiceRemove {
            target: create.target().clone(),
            precondition: crate::reconcile::ReplicaPrecondition::new(
                workload.clone(),
                create.precondition().environment_generation(),
                crate::reconcile::ExpectedJournalHead::exact(
                    &intent.scope.reservation_id,
                    intent.service_generation,
                    None,
                )
                .unwrap(),
            )
            .unwrap(),
        };
        let actions = vec![action];
        let session_id = format!("rs-unbound-{status_label}");
        install_unstarted_batch(&store, &session_id, "op-unbound-absent", &actions);
        let claim = store
            .start_reconcile_batch(&session_id, "exact-batch", "op-unbound-absent", 0, &actions)
            .unwrap()
            .remove(0);
        let before = store
            .load_stack_container_create_intent(&intent.scope.reservation_id)
            .unwrap()
            .unwrap();
        assert!(matches!(
            store.inspect_claimed_predecessor(&claim).unwrap(),
            ClaimedPredecessorInspection::ExactUnboundNeedsInspection { .. }
        ));
        assert_eq!(
            store
                .load_stack_container_create_intent(&intent.scope.reservation_id)
                .unwrap()
                .unwrap(),
            before,
            "inspection must not infer runtime absence"
        );
        let failed = store
            .publish_claimed_unbound_predecessor_failure(
                &claim,
                "executor confirmed runtime absence",
                160,
            )
            .unwrap();
        assert_eq!(failed.phase, ServicePhase::Failed);
        assert!(matches!(
            store.inspect_claimed_predecessor(&claim).unwrap(),
            ClaimedPredecessorInspection::ExactUnboundFailed { .. }
        ));
        assert_eq!(
            store
                .publish_claimed_unbound_predecessor_failure(
                    &claim,
                    "executor confirmed runtime absence",
                    160,
                )
                .unwrap(),
            failed
        );
        store.release_claimed_allocator_target(&claim).unwrap();
    }

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("claimed-unbound-bind-reopen.db");
    let store = StateStore::open(&path).unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let create = &seeded[0];
    let workload = create.precondition().workload();
    let input = claimed_create_input(&store, create, "unbound-bind-reopen");
    let selector = StackContainerCreateSelector {
        project_id: workload.project_id.clone(),
        environment_id: workload.environment_id.clone(),
        machine_id: workload.machine_id.clone(),
        machine_incarnation_id: workload.machine_incarnation_id.clone(),
        environment_generation: create.precondition().environment_generation(),
        stack_id: workload.stack_id.clone(),
        service_name: create.target().service_name.clone(),
        replica_index: create.target().index(),
        requested_container_id: input.requested_container_id,
        definition_digest: input.definition_digest,
        action_digest: "sha256:unbound-bind-reopen".to_string(),
        applied_config_digest: input.applied_config_digest,
    };
    let intent = store
        .resolve_or_begin_stack_container_create(&selector, 170)
        .unwrap()
        .0;
    let actions = vec![Action::ServiceRemove {
        target: create.target().clone(),
        precondition: crate::reconcile::ReplicaPrecondition::new(
            workload.clone(),
            create.precondition().environment_generation(),
            crate::reconcile::ExpectedJournalHead::exact(
                &intent.scope.reservation_id,
                intent.service_generation,
                None,
            )
            .unwrap(),
        )
        .unwrap(),
    }];
    install_unstarted_batch(
        &store,
        "rs-unbound-bind-reopen",
        "op-unbound-bind-reopen",
        &actions,
    );
    let claim = store
        .start_reconcile_batch(
            "rs-unbound-bind-reopen",
            "exact-batch",
            "op-unbound-bind-reopen",
            0,
            &actions,
        )
        .unwrap()
        .remove(0);
    let binding = binding_for_claimed_intent(&intent, 52, 171);
    store
        .bind_claimed_predecessor_for_cleanup(&claim, &binding)
        .unwrap();
    assert!(matches!(
        store.inspect_claimed_predecessor(&claim).unwrap(),
        ClaimedPredecessorInspection::ExactBoundCleanupPending { binding: actual, .. }
            if actual == binding
    ));
    drop(store);

    let store = StateStore::open(&path).unwrap();
    assert!(matches!(
        store.inspect_claimed_predecessor(&claim).unwrap(),
        ClaimedPredecessorInspection::ExactBoundCleanupPending { binding: actual, .. }
            if actual == binding
    ));
    let stopped = store
        .complete_claimed_predecessor_cleanup(&claim, 172)
        .unwrap();
    assert_eq!(
        store
            .complete_claimed_predecessor_cleanup(&claim, 172)
            .unwrap(),
        stopped
    );
    store.release_claimed_allocator_target(&claim).unwrap();
}

#[test]
fn raw_journal_mutators_cannot_bypass_a_started_claim_with_matching_authority() {
    let store = StateStore::in_memory().unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let actions = vec![seeded[1].clone()];
    install_unstarted_batch(&store, "rs-raw-fence", "op-raw-fence", &actions);
    let claim = store
        .start_reconcile_batch("rs-raw-fence", "exact-batch", "op-raw-fence", 0, &actions)
        .unwrap()
        .remove(0);
    let reservation_id = match actions[0].precondition().journal_head() {
        crate::reconcile::ExpectedJournalHead::Exact { reservation_id, .. } => reservation_id,
        crate::reconcile::ExpectedJournalHead::NeverJournaled => unreachable!(),
    };
    let before = store
        .load_stack_container_create_intent(reservation_id)
        .unwrap()
        .unwrap();
    let error = store
        .begin_stack_container_cleanup(reservation_id, 200)
        .unwrap_err();
    assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
    assert_eq!(
        store
            .load_stack_container_create_intent(reservation_id)
            .unwrap()
            .unwrap(),
        before
    );
    store
        .begin_claimed_predecessor_cleanup(&claim, 200)
        .unwrap();
}

#[test]
fn claimed_successor_allocator_is_atomic_exact_and_fences_raw_saves() {
    let store = StateStore::in_memory().unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let sibling = ServiceReplicaKey::new("api-2", 1).unwrap();
    let initial = AllocatorSnapshot {
        schema_version: 2,
        ports: vec![AllocatorPortLease {
            target: sibling.clone(),
            ports: vec![PublishedPort {
                protocol: "tcp".to_string(),
                container_port: 80,
                host_port: 18_080,
            }],
        }],
        service_ips: vec![AllocatorIpLease {
            target: sibling.clone(),
            ip: "10.42.0.8".to_string(),
        }],
        service_network_ips: vec![AllocatorNetworkIpLease {
            target: sibling.clone(),
            network_name: "backend".to_string(),
            ip: "10.43.0.8".to_string(),
        }],
        mount_tag_offsets: HashMap::from([("api-2".to_string(), 17), ("api".to_string(), 31)]),
    };
    store.save_allocator_state("exact-batch", &initial).unwrap();
    let actions = vec![seeded[0].clone()];
    install_unstarted_batch(
        &store,
        "rs-allocator-upsert",
        "op-allocator-upsert",
        &actions,
    );
    let claim = store
        .start_reconcile_batch(
            "rs-allocator-upsert",
            "exact-batch",
            "op-allocator-upsert",
            0,
            &actions,
        )
        .unwrap()
        .remove(0);
    store
        .validate_reconcile_action_claim(
            &claim,
            "rs-allocator-upsert",
            "op-allocator-upsert",
            0,
            &actions[0],
        )
        .unwrap();
    for (session, operation, index, action) in [
        ("foreign-session", "op-allocator-upsert", 0, &actions[0]),
        ("rs-allocator-upsert", "foreign-operation", 0, &actions[0]),
        ("rs-allocator-upsert", "op-allocator-upsert", 1, &actions[0]),
        ("rs-allocator-upsert", "op-allocator-upsert", 0, &seeded[1]),
    ] {
        assert_eq!(
            store
                .validate_reconcile_action_claim(&claim, session, operation, index, action)
                .unwrap_err()
                .machine_code(),
            MachineErrorCode::StateConflict
        );
    }

    let allocation = ClaimedAllocatorTarget {
        ports: vec![
            PublishedPort {
                protocol: "tcp".to_string(),
                container_port: 8080,
                host_port: 28_080,
            },
            PublishedPort {
                protocol: "udp".to_string(),
                container_port: 5353,
                host_port: 25_353,
            },
        ],
        service_ip: Some("10.42.0.9".to_string()),
        service_network_ips: vec![
            ClaimedAllocatorNetworkIp {
                network_name: "backend".to_string(),
                ip: "10.43.0.9".to_string(),
            },
            ClaimedAllocatorNetworkIp {
                network_name: "frontend".to_string(),
                ip: "10.46.0.9".to_string(),
            },
        ],
        mount_tag_offset: Some(31),
    };
    let input = claimed_create_input(&store, &actions[0], "allocator-atomic");
    assert!(
        store
            .resolve_or_begin_claimed_successor_after_allocator_failpoint(
                &claim,
                &input,
                &allocation,
                190,
            )
            .is_err()
    );
    assert_eq!(
        store.load_allocator_state("exact-batch").unwrap().unwrap(),
        initial
    );
    let intent = store
        .resolve_or_begin_claimed_successor(&claim, &input, &allocation, 190)
        .unwrap();
    let mut reordered = allocation.clone();
    reordered.ports.reverse();
    reordered.service_network_ips.reverse();
    assert_eq!(
        store
            .resolve_or_begin_claimed_successor(&claim, &input, &reordered, 190)
            .unwrap(),
        intent
    );
    let persisted = store.load_allocator_state("exact-batch").unwrap().unwrap();
    assert_eq!(persisted.mount_tag_offsets.get("api-2"), Some(&17));
    assert_eq!(persisted.mount_tag_offsets.get("api"), Some(&31));
    assert!(persisted.ports.iter().any(|lease| lease.target == sibling));
    assert!(
        persisted
            .service_ips
            .iter()
            .any(|lease| lease.target == sibling)
    );
    assert!(
        persisted
            .service_network_ips
            .iter()
            .any(|lease| lease.target == sibling)
    );

    let before_conflict = persisted.clone();
    let conflict_allocation = ClaimedAllocatorTarget {
        ports: vec![PublishedPort {
            protocol: "tcp".to_string(),
            container_port: 8080,
            host_port: 18_080,
        }],
        service_ip: allocation.service_ip.clone(),
        service_network_ips: allocation.service_network_ips.clone(),
        mount_tag_offset: allocation.mount_tag_offset,
    };
    assert!(
        store
            .resolve_or_begin_claimed_successor(&claim, &input, &conflict_allocation, 190)
            .is_err()
    );
    assert_eq!(
        store.load_allocator_state("exact-batch").unwrap().unwrap(),
        before_conflict
    );
    let mut mount_conflict = allocation.clone();
    mount_conflict.mount_tag_offset = Some(32);
    assert_eq!(
        store
            .resolve_or_begin_claimed_successor(&claim, &input, &mount_conflict, 190)
            .unwrap_err()
            .machine_code(),
        MachineErrorCode::StateConflict
    );
    assert_eq!(
        store
            .load_allocator_state("exact-batch")
            .unwrap()
            .unwrap()
            .mount_tag_offsets
            .get("api"),
        Some(&31)
    );
    assert_eq!(
        store
            .save_allocator_state("exact-batch", &before_conflict)
            .unwrap_err()
            .machine_code(),
        MachineErrorCode::StateConflict
    );
}

#[test]
fn claimed_successor_does_not_mutate_allocator_before_exact_predecessor_is_terminal() {
    let store = StateStore::in_memory().unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let initial = AllocatorSnapshot {
        schema_version: 2,
        ports: Vec::new(),
        service_ips: Vec::new(),
        service_network_ips: Vec::new(),
        mount_tag_offsets: HashMap::from([("api-2".to_string(), 7)]),
    };
    store.save_allocator_state("exact-batch", &initial).unwrap();
    let actions = vec![seeded[1].clone()];
    install_unstarted_batch(
        &store,
        "rs-allocator-predecessor",
        "op-allocator-predecessor",
        &actions,
    );
    let claim = store
        .start_reconcile_batch(
            "rs-allocator-predecessor",
            "exact-batch",
            "op-allocator-predecessor",
            0,
            &actions,
        )
        .unwrap()
        .remove(0);
    let input = claimed_create_input(&store, &actions[0], "predecessor-running");
    let allocation = ClaimedAllocatorTarget {
        ports: vec![PublishedPort {
            protocol: "tcp".to_string(),
            container_port: 8080,
            host_port: 30_080,
        }],
        service_ip: Some("10.50.0.2".to_string()),
        service_network_ips: Vec::new(),
        mount_tag_offset: Some(99),
    };
    assert_eq!(
        store
            .resolve_or_begin_claimed_successor(&claim, &input, &allocation, 400)
            .unwrap_err()
            .machine_code(),
        MachineErrorCode::StateConflict
    );
    assert_eq!(
        store.load_allocator_state("exact-batch").unwrap().unwrap(),
        initial
    );
}

#[test]
fn claimed_allocator_remove_releases_only_exact_target_and_replays_after_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("claimed-allocator-release.db");
    let store = StateStore::open(&path).unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let target = seeded[2].target().clone();
    let sibling = ServiceReplicaKey::new("api-2", 1).unwrap();
    let initial = AllocatorSnapshot {
        schema_version: 2,
        ports: vec![
            AllocatorPortLease {
                target: target.clone(),
                ports: vec![PublishedPort {
                    protocol: "tcp".to_string(),
                    container_port: 9000,
                    host_port: 19_000,
                }],
            },
            AllocatorPortLease {
                target: sibling.clone(),
                ports: vec![PublishedPort {
                    protocol: "tcp".to_string(),
                    container_port: 9001,
                    host_port: 19_001,
                }],
            },
        ],
        service_ips: vec![
            AllocatorIpLease {
                target: target.clone(),
                ip: "10.44.0.10".to_string(),
            },
            AllocatorIpLease {
                target: sibling.clone(),
                ip: "10.44.0.11".to_string(),
            },
        ],
        service_network_ips: vec![
            AllocatorNetworkIpLease {
                target: target.clone(),
                network_name: "backend".to_string(),
                ip: "10.45.0.10".to_string(),
            },
            AllocatorNetworkIpLease {
                target: sibling.clone(),
                network_name: "backend".to_string(),
                ip: "10.45.0.11".to_string(),
            },
        ],
        mount_tag_offsets: HashMap::from([("worker".to_string(), 23), ("api-2".to_string(), 29)]),
    };
    store.save_allocator_state("exact-batch", &initial).unwrap();
    let actions = vec![seeded[2].clone()];
    install_unstarted_batch(
        &store,
        "rs-allocator-release",
        "op-allocator-release",
        &actions,
    );
    let claim = store
        .start_reconcile_batch(
            "rs-allocator-release",
            "exact-batch",
            "op-allocator-release",
            0,
            &actions,
        )
        .unwrap()
        .remove(0);
    store
        .begin_claimed_predecessor_cleanup(&claim, 300)
        .unwrap();
    store
        .complete_claimed_predecessor_cleanup(&claim, 301)
        .unwrap();
    let release = store.release_claimed_allocator_target(&claim).unwrap();
    assert_eq!(release.target, target);
    assert!(!release.already_released);
    assert_eq!(release.released.ports, initial.ports[0].ports);
    assert_eq!(release.released.service_ip.as_deref(), Some("10.44.0.10"));
    drop(store);

    let store = StateStore::open(&path).unwrap();
    let before_replay: (String, String, String) = store
        .conn
        .query_row(
            "SELECT ports_json, service_ips_json, mount_tag_offsets_json
             FROM allocator_state WHERE stack_name = 'exact-batch'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let changes_before_replay = store.conn.total_changes();
    let replay = store.release_claimed_allocator_target(&claim).unwrap();
    assert!(replay.already_released);
    assert_eq!(replay.target, target);
    assert_eq!(store.conn.total_changes(), changes_before_replay);
    let after_replay: (String, String, String) = store
        .conn
        .query_row(
            "SELECT ports_json, service_ips_json, mount_tag_offsets_json
             FROM allocator_state WHERE stack_name = 'exact-batch'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(after_replay, before_replay);
    let persisted = store.load_allocator_state("exact-batch").unwrap().unwrap();
    assert_eq!(persisted.mount_tag_offsets, initial.mount_tag_offsets);
    assert_eq!(persisted.ports, vec![initial.ports[1].clone()]);
    assert_eq!(persisted.service_ips, vec![initial.service_ips[1].clone()]);
    assert_eq!(
        persisted.service_network_ips,
        vec![initial.service_network_ips[1].clone()]
    );
    store
        .commit_reconcile_batch(
            "rs-allocator-release",
            "exact-batch",
            "op-allocator-release",
            0,
            &actions,
            &exact_outcomes(&actions, None),
        )
        .unwrap();
    assert_eq!(
        store
            .release_claimed_allocator_target(&claim)
            .unwrap_err()
            .machine_code(),
        MachineErrorCode::StateConflict
    );
}

#[test]
fn started_claim_replay_rejects_impossible_status_binding_shapes() {
    for malformed_shape in ["reserved-unbound", "cleaned-unbound", "failed-bound"] {
        let store = StateStore::in_memory().unwrap();
        let seeded = exact_batch_actions_for_claim(&store);

        let (actions, reservation_id) = if malformed_shape == "failed-bound" {
            let reservation_id = match seeded[1].precondition().journal_head() {
                crate::reconcile::ExpectedJournalHead::Exact { reservation_id, .. } => {
                    reservation_id.clone()
                }
                crate::reconcile::ExpectedJournalHead::NeverJournaled => unreachable!(),
            };
            store
                .publish_stack_container_blocked(
                    &reservation_id,
                    "bound predecessor awaits cleanup",
                    200,
                )
                .unwrap();
            (
                vec![Action::ServiceCreate {
                    target: seeded[1].target().clone(),
                    precondition: seeded[1].precondition().clone(),
                }],
                reservation_id,
            )
        } else {
            let workload = seeded[0].precondition().workload();
            let environment = store
                .load_environment_instance(workload.environment_id.as_str())
                .unwrap()
                .unwrap();
            let environment_generation = environment.lifecycle_generation;
            let selector = StackContainerCreateSelector {
                project_id: workload.project_id.clone(),
                environment_id: workload.environment_id.clone(),
                machine_id: workload.machine_id.clone(),
                machine_incarnation_id: workload.machine_incarnation_id.clone(),
                environment_generation: environment.lifecycle_generation,
                stack_id: workload.stack_id.clone(),
                service_name: seeded[0].target().service_name.clone(),
                replica_index: seeded[0].target().index(),
                requested_container_id: format!("ctr-{malformed_shape}"),
                definition_digest: environment.definition_digest.clone(),
                action_digest: format!("sha256:{malformed_shape}"),
                applied_config_digest: "vzsc1-sha256:malformed-replay".to_string(),
            };
            let mut intent = store
                .resolve_or_begin_stack_container_create(&selector, 200)
                .unwrap()
                .0;
            if malformed_shape == "cleaned-unbound" {
                store
                    .publish_stack_container_blocked(
                        &intent.scope.reservation_id,
                        "unbound predecessor is retryable",
                        201,
                    )
                    .unwrap();
                intent = store
                    .load_stack_container_create_intent(&intent.scope.reservation_id)
                    .unwrap()
                    .unwrap();
            }
            let precondition = crate::reconcile::ReplicaPrecondition::new(
                workload.clone(),
                environment_generation,
                crate::reconcile::ExpectedJournalHead::exact(
                    &intent.scope.reservation_id,
                    intent.service_generation,
                    None,
                )
                .unwrap(),
            )
            .unwrap();
            let action = if matches!(malformed_shape, "reserved-unbound" | "cleaned-unbound") {
                Action::ServiceRemove {
                    target: seeded[0].target().clone(),
                    precondition,
                }
            } else {
                Action::ServiceCreate {
                    target: seeded[0].target().clone(),
                    precondition,
                }
            };
            (vec![action], intent.scope.reservation_id)
        };

        let session_id = format!("rs-malformed-replay-{malformed_shape}");
        install_unstarted_batch(&store, &session_id, "op-malformed-replay", &actions);
        store
            .start_reconcile_batch(
                &session_id,
                "exact-batch",
                "op-malformed-replay",
                0,
                &actions,
            )
            .unwrap();

        let mut intent = store
            .load_stack_container_create_intent(&reservation_id)
            .unwrap()
            .unwrap();
        let (status, observed) = match malformed_shape {
            "reserved-unbound" => (
                StackContainerCreateStatus::Reserved,
                store
                    .load_observed_state_for_replica(
                        &intent.scope.stack_id,
                        &intent.service_name,
                        intent.replica_index,
                    )
                    .unwrap()
                    .unwrap(),
            ),
            "cleaned-unbound" => (
                StackContainerCreateStatus::Cleaned,
                ServiceObservedState {
                    replica: actions[0].target().clone(),
                    applied_config_digest: None,
                    phase: ServicePhase::Stopped,
                    container_id: None,
                    failed_create_ownership: None,
                    last_error: None,
                    ready: false,
                },
            ),
            "failed-bound" => (
                StackContainerCreateStatus::Failed,
                ServiceObservedState {
                    replica: actions[0].target().clone(),
                    applied_config_digest: None,
                    phase: ServicePhase::Failed,
                    container_id: None,
                    failed_create_ownership: None,
                    last_error: Some("malformed bound failure".to_string()),
                    ready: false,
                },
            ),
            _ => unreachable!(),
        };
        intent.status = status;
        intent.updated_at = 300;
        intent.completed_at = matches!(
            status,
            StackContainerCreateStatus::Cleaned | StackContainerCreateStatus::Failed
        )
        .then_some(300);
        intent.last_error = observed.last_error.clone();
        store
            .conn
            .execute(
                "UPDATE stack_container_create_intents
                 SET status = ?1, intent_json = ?2, last_error = ?3,
                     updated_at = ?4, completed_at = ?5
                 WHERE reservation_id = ?6",
                params![
                    malformed_shape.split('-').next().unwrap(),
                    serde_json::to_string(&intent).unwrap(),
                    intent.last_error,
                    i64::try_from(intent.updated_at).unwrap(),
                    intent
                        .completed_at
                        .map(|value| i64::try_from(value).unwrap()),
                    reservation_id,
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE observed_state SET state_json = ?1
                 WHERE stack_name = ?2 AND service_name = ?3 AND replica_index = ?4",
                params![
                    serde_json::to_string(&observed).unwrap(),
                    intent.scope.stack_id,
                    intent.service_name,
                    i64::from(intent.replica_index),
                ],
            )
            .unwrap();

        let error = store
            .start_reconcile_batch(
                &session_id,
                "exact-batch",
                "op-malformed-replay",
                0,
                &actions,
            )
            .unwrap_err();
        assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
        assert!(
            error
                .to_string()
                .contains("impossible journal status/binding shape")
        );
        assert_eq!(
            store.load_audit_log_for_session(&session_id).unwrap().len(),
            1
        );
    }
}

#[test]
fn started_create_claim_replay_requires_session_linked_successor_digest() {
    for (case, suffix, linked) in [
        ("valid", "a".repeat(64), true),
        ("short", "a".repeat(63), false),
        ("uppercase", "A".repeat(64), false),
        ("nonhex", format!("{}g", "a".repeat(63)), false),
        ("trailing", format!("{}0", "a".repeat(64)), false),
    ] {
        let store = StateStore::in_memory().unwrap();
        let seeded = exact_batch_actions_for_claim(&store);
        let actions = vec![seeded[0].clone()];
        let session_id = format!("rs-successor-{case}");
        install_unstarted_batch(&store, &session_id, "op-successor", &actions);
        let claim = store
            .start_reconcile_batch(&session_id, "exact-batch", "op-successor", 0, &actions)
            .unwrap()
            .remove(0);

        let workload = actions[0].precondition().workload();
        let project = store
            .load_project_state(workload.project_id.as_str())
            .unwrap()
            .unwrap();
        let environment = project
            .environments
            .iter()
            .find(|environment| environment.environment_id == workload.environment_id)
            .unwrap();
        let prefix = crate::reconcile::ReconcileActionExecutionKey::new(
            &session_id,
            "op-successor",
            0,
            &actions[0],
        )
        .unwrap()
        .activation_digest_prefix()
        .unwrap();
        let input = ClaimedCreateInput {
            requested_container_id: format!("ctr-successor-{case}"),
            definition_digest: environment.definition_digest.clone(),
            applied_config_digest: "vzsc1-sha256:successor".to_string(),
            activation_payload_sha256: suffix.clone(),
        };
        if linked {
            store
                .resolve_or_begin_claimed_successor(
                    &claim,
                    &input,
                    &empty_claimed_allocator_target(),
                    10,
                )
                .unwrap();
        } else {
            let intent = StackContainerCreateIntent {
                schema_version: StackContainerCreateIntent::SCHEMA_VERSION,
                scope: vz_runtime_contract::ContainerGenerationScope {
                    reservation_id: format!("reservation-malformed-{case}"),
                    project_id: workload.project_id.clone(),
                    environment_id: workload.environment_id.clone(),
                    machine_id: workload.machine_id.clone(),
                    machine_incarnation_id: Some(workload.machine_incarnation_id.clone()),
                    stack_id: workload.stack_id.clone(),
                },
                environment_generation: environment.lifecycle_generation,
                service_name: actions[0].target().service_name.clone(),
                replica_index: actions[0].target().index(),
                service_generation: 1,
                requested_container_id: input.requested_container_id,
                definition_digest: input.definition_digest,
                action_digest: format!("{prefix}{suffix}"),
                applied_config_digest: Some(input.applied_config_digest),
                status: StackContainerCreateStatus::Intent,
                last_error: None,
                created_at: 10,
                updated_at: 10,
                completed_at: None,
            };
            inject_journal_intent_for_test(&store, &intent);
        }

        let replay =
            store.start_reconcile_batch(&session_id, "exact-batch", "op-successor", 0, &actions);
        if linked {
            assert_eq!(replay.unwrap().len(), 1);
        } else {
            assert_eq!(
                replay.unwrap_err().machine_code(),
                MachineErrorCode::StateConflict
            );
        }
    }
}

#[test]
fn inspect_claimed_predecessor_reopens_every_structurally_legal_linked_successor_state() {
    for status in [
        StackContainerCreateStatus::Intent,
        StackContainerCreateStatus::Reserved,
        StackContainerCreateStatus::Running,
        StackContainerCreateStatus::Blocked,
        StackContainerCreateStatus::CleanupPending,
        StackContainerCreateStatus::Cleaned,
        StackContainerCreateStatus::Failed,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(format!("linked-successor-{status:?}.db"));
        let (claim, reservation_id, expected_binding) = {
            let store = StateStore::open(&path).unwrap();
            let seeded = exact_batch_actions_for_claim(&store);
            let actions = vec![seeded[0].clone()];
            let session_id = format!("inspect-linked-{status:?}");
            install_unstarted_batch(&store, &session_id, "inspect-linked-op", &actions);
            let claim = store
                .start_reconcile_batch(&session_id, "exact-batch", "inspect-linked-op", 0, &actions)
                .unwrap()
                .remove(0);
            let input = claimed_create_input(&store, &actions[0], &format!("{status:?}"));
            let intent = store
                .resolve_or_begin_claimed_successor(
                    &claim,
                    &input,
                    &empty_claimed_allocator_target(),
                    100,
                )
                .unwrap();
            let reservation_id = intent.scope.reservation_id.clone();
            let binding = binding_for_claimed_intent(&intent, 11, 101);

            match status {
                StackContainerCreateStatus::Intent => {}
                StackContainerCreateStatus::Reserved => {
                    store
                        .bind_claimed_successor_generation(&claim, &binding)
                        .unwrap();
                }
                StackContainerCreateStatus::Running => {
                    store
                        .bind_claimed_successor_generation(&claim, &binding)
                        .unwrap();
                    let receipt = receipt_for_claimed_binding(&binding);
                    store
                        .publish_claimed_successor_success(
                            &claim,
                            &reservation_id,
                            &receipt,
                            true,
                            102,
                        )
                        .unwrap();
                }
                StackContainerCreateStatus::Blocked => {
                    store
                        .bind_claimed_successor_generation(&claim, &binding)
                        .unwrap();
                    store
                        .publish_claimed_successor_blocked(
                            &claim,
                            &reservation_id,
                            "activation blocked",
                            102,
                        )
                        .unwrap();
                }
                StackContainerCreateStatus::CleanupPending => {
                    store
                        .bind_claimed_successor_generation(&claim, &binding)
                        .unwrap();
                    store
                        .publish_claimed_successor_blocked(
                            &claim,
                            &reservation_id,
                            "activation blocked",
                            102,
                        )
                        .unwrap();
                    store
                        .begin_claimed_successor_cleanup(&claim, &reservation_id, 103)
                        .unwrap();
                }
                StackContainerCreateStatus::Cleaned => {
                    store
                        .bind_claimed_successor_generation(&claim, &binding)
                        .unwrap();
                    store
                        .publish_claimed_successor_blocked(
                            &claim,
                            &reservation_id,
                            "activation blocked",
                            102,
                        )
                        .unwrap();
                    store
                        .begin_claimed_successor_cleanup(&claim, &reservation_id, 103)
                        .unwrap();
                    store
                        .complete_claimed_successor_cleanup(&claim, &reservation_id, 104)
                        .unwrap();
                }
                StackContainerCreateStatus::Failed => {
                    store
                        .publish_claimed_successor_failure(
                            &claim,
                            &reservation_id,
                            "activation failed before bind",
                            102,
                        )
                        .unwrap();
                }
            }
            (
                claim,
                reservation_id,
                !matches!(
                    status,
                    StackContainerCreateStatus::Intent | StackContainerCreateStatus::Failed
                ),
            )
        };

        let reopened = StateStore::open(&path).unwrap();
        match reopened.inspect_claimed_predecessor(&claim).unwrap() {
            ClaimedPredecessorInspection::ClaimLinkedSuccessor { intent, binding } => {
                assert_eq!(intent.scope.reservation_id, reservation_id);
                assert_eq!(intent.status, status);
                assert_eq!(binding.is_some(), expected_binding);
            }
            other => panic!("expected linked successor after reopen, got {other:?}"),
        }
    }
}

#[test]
fn inspect_claimed_predecessor_rejects_foreign_and_ambiguous_successors_without_writes() {
    for case in ["foreign", "ambiguous"] {
        let store = StateStore::in_memory().unwrap();
        let seeded = exact_batch_actions_for_claim(&store);
        let actions = vec![seeded[0].clone()];
        let session_id = format!("inspect-linked-negative-{case}");
        install_unstarted_batch(&store, &session_id, "inspect-linked-negative-op", &actions);
        let claim = store
            .start_reconcile_batch(
                &session_id,
                "exact-batch",
                "inspect-linked-negative-op",
                0,
                &actions,
            )
            .unwrap()
            .remove(0);
        let workload = actions[0].precondition().workload();
        let environment = store
            .load_environment_instance(workload.environment_id.as_str())
            .unwrap()
            .unwrap();

        let linked = if case == "ambiguous" {
            let intent = store
                .resolve_or_begin_claimed_successor(
                    &claim,
                    &claimed_create_input(&store, &actions[0], case),
                    &empty_claimed_allocator_target(),
                    10,
                )
                .unwrap();
            store
                .publish_claimed_successor_failure(
                    &claim,
                    &intent.scope.reservation_id,
                    "terminalize before ambiguity injection",
                    11,
                )
                .unwrap();
            Some(
                store
                    .load_stack_container_create_intent(&intent.scope.reservation_id)
                    .unwrap()
                    .unwrap(),
            )
        } else {
            None
        };
        let mut foreign = linked.clone().unwrap_or(StackContainerCreateIntent {
            schema_version: StackContainerCreateIntent::SCHEMA_VERSION,
            scope: vz_runtime_contract::ContainerGenerationScope {
                reservation_id: "foreign-linked-successor".to_string(),
                project_id: workload.project_id.clone(),
                environment_id: workload.environment_id.clone(),
                machine_id: workload.machine_id.clone(),
                machine_incarnation_id: Some(workload.machine_incarnation_id.clone()),
                stack_id: workload.stack_id.clone(),
            },
            environment_generation: environment.lifecycle_generation,
            service_name: actions[0].target().service_name.clone(),
            replica_index: actions[0].target().index(),
            service_generation: 1,
            requested_container_id: "ctr-foreign-linked-successor".to_string(),
            definition_digest: environment.definition_digest,
            action_digest: format!("vzsad3:{}:{}", "0".repeat(64), "a".repeat(64)),
            applied_config_digest: Some("vzsc1-sha256:foreign-linked".to_string()),
            status: StackContainerCreateStatus::Intent,
            last_error: None,
            created_at: 10,
            updated_at: 10,
            completed_at: None,
        });
        if case == "ambiguous" {
            foreign.scope.reservation_id = "zz-ambiguous-linked-successor".to_string();
            foreign.scope.machine_incarnation_id =
                Some(MachineIncarnationId::new("inc_ambiguous_linked_successor").unwrap());
            foreign.requested_container_id = "ctr-ambiguous-linked-successor".to_string();
            foreign.action_digest = format!("vzsad3:{}:{}", "0".repeat(64), "b".repeat(64));
            foreign.last_error = Some("foreign terminal failure".to_string());
        }
        inject_journal_intent_for_test(&store, &foreign);

        let before = store.conn.total_changes();
        let error = store.inspect_claimed_predecessor(&claim).unwrap_err();
        assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
        assert_eq!(store.conn.total_changes(), before);
    }
}

#[test]
fn claimed_successor_reservation_preview_is_v2_stable_no_write_and_matches_resolve() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("claimed-successor-preview.db");
    let (claim, action, preview, linked) = {
        let store = StateStore::open(&path).unwrap();
        let seeded = exact_batch_actions_for_claim(&store);
        let actions = vec![seeded[0].clone()];
        install_unstarted_batch(&store, "preview-session", "preview-operation", &actions);
        let claim = store
            .start_reconcile_batch(
                "preview-session",
                "exact-batch",
                "preview-operation",
                0,
                &actions,
            )
            .unwrap()
            .remove(0);

        let before = store.conn.total_changes();
        let preview = store
            .preview_claimed_successor_reservation(&claim, "ctr-preview")
            .unwrap();
        assert!(preview.reservation_id.starts_with("vzscr2-sha256:"));
        assert_eq!(store.conn.total_changes(), before);
        assert_eq!(
            store
                .preview_claimed_successor_reservation(&claim, "ctr-preview")
                .unwrap(),
            preview
        );
        let different_requested = store
            .preview_claimed_successor_reservation(&claim, "ctr-preview-other")
            .unwrap();
        assert_ne!(different_requested.reservation_id, preview.reservation_id);
        assert_eq!(store.conn.total_changes(), before);

        let input = ClaimedCreateInput {
            requested_container_id: "ctr-preview".to_string(),
            ..claimed_create_input(&store, &actions[0], "preview")
        };
        let allocation = ClaimedAllocatorTarget {
            ports: vec![PublishedPort {
                protocol: "tcp".to_string(),
                container_port: 8080,
                host_port: 18080,
            }],
            service_ip: Some("10.55.0.2".to_string()),
            service_network_ips: vec![ClaimedAllocatorNetworkIp {
                network_name: "preview-net".to_string(),
                ip: "10.56.0.2".to_string(),
            }],
            mount_tag_offset: Some(4),
        };
        let linked = store
            .resolve_or_begin_claimed_successor(&claim, &input, &allocation, 50)
            .unwrap();
        assert_eq!(linked.scope, preview);
        let after_resolve = store.conn.total_changes();
        assert_eq!(
            store
                .preview_claimed_successor_reservation(&claim, "ctr-preview")
                .unwrap(),
            preview
        );
        assert_eq!(store.conn.total_changes(), after_resolve);
        (claim, actions[0].clone(), preview, linked)
    };

    let reopened = StateStore::open(&path).unwrap();
    let before = reopened.conn.total_changes();
    assert_eq!(
        reopened
            .preview_claimed_successor_reservation(&claim, "ctr-preview")
            .unwrap(),
        preview
    );
    assert_eq!(reopened.conn.total_changes(), before);
    assert_eq!(
        reopened.inspect_claimed_predecessor(&claim).unwrap(),
        ClaimedPredecessorInspection::ClaimLinkedSuccessor {
            intent: linked,
            binding: None,
        }
    );
    reopened
        .validate_reconcile_action_claim(&claim, "preview-session", "preview-operation", 0, &action)
        .unwrap();
}

#[test]
fn claimed_successor_reservation_preview_fences_foreign_heads_and_derives_exact_generation() {
    let store = StateStore::in_memory().unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let recreate = vec![seeded[1].clone()];
    install_unstarted_batch(&store, "preview-exact", "preview-exact-op", &recreate);
    let recreate_claim = store
        .start_reconcile_batch(
            "preview-exact",
            "exact-batch",
            "preview-exact-op",
            0,
            &recreate,
        )
        .unwrap()
        .remove(0);
    let before = store.conn.total_changes();
    let exact_preview = store
        .preview_claimed_successor_reservation(&recreate_claim, "ctr-preview-exact")
        .unwrap();
    assert!(exact_preview.reservation_id.starts_with("vzscr2-sha256:"));
    assert_eq!(store.conn.total_changes(), before);

    let separate = StateStore::in_memory().unwrap();
    let seeded = exact_batch_actions_for_claim(&separate);
    let create = vec![seeded[0].clone()];
    install_unstarted_batch(&separate, "preview-foreign", "preview-foreign-op", &create);
    let claim = separate
        .start_reconcile_batch(
            "preview-foreign",
            "exact-batch",
            "preview-foreign-op",
            0,
            &create,
        )
        .unwrap()
        .remove(0);
    let workload = create[0].precondition().workload();
    let environment = separate
        .load_environment_instance(workload.environment_id.as_str())
        .unwrap()
        .unwrap();
    let foreign = StackContainerCreateIntent {
        schema_version: StackContainerCreateIntent::SCHEMA_VERSION,
        scope: vz_runtime_contract::ContainerGenerationScope {
            reservation_id: "foreign-preview-successor".to_string(),
            project_id: workload.project_id.clone(),
            environment_id: workload.environment_id.clone(),
            machine_id: workload.machine_id.clone(),
            machine_incarnation_id: Some(workload.machine_incarnation_id.clone()),
            stack_id: workload.stack_id.clone(),
        },
        environment_generation: environment.lifecycle_generation,
        service_name: create[0].target().service_name.clone(),
        replica_index: create[0].target().index(),
        service_generation: 1,
        requested_container_id: "ctr-preview-foreign".to_string(),
        definition_digest: environment.definition_digest,
        action_digest: format!("vzsad3:{}:{}", "0".repeat(64), "a".repeat(64)),
        applied_config_digest: Some("vzsc1-sha256:preview-foreign".to_string()),
        status: StackContainerCreateStatus::Intent,
        last_error: None,
        created_at: 10,
        updated_at: 10,
        completed_at: None,
    };
    inject_journal_intent_for_test(&separate, &foreign);
    let before = separate.conn.total_changes();
    let error = separate
        .preview_claimed_successor_reservation(&claim, "ctr-preview-foreign")
        .unwrap_err();
    assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
    assert_eq!(separate.conn.total_changes(), before);
}

#[test]
fn started_claim_replay_rejects_linked_first_duplicate_latest_generation() {
    let store = StateStore::in_memory().unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let actions = vec![seeded[0].clone()];
    let session_id = "rs-duplicate-replay-head";
    let operation_id = "op-duplicate-replay-head";
    install_unstarted_batch(&store, session_id, operation_id, &actions);
    let claim = store
        .start_reconcile_batch(session_id, "exact-batch", operation_id, 0, &actions)
        .unwrap()
        .remove(0);

    let workload = actions[0].precondition().workload();
    let environment = store
        .load_environment_instance(workload.environment_id.as_str())
        .unwrap()
        .unwrap();
    let execution_key = crate::reconcile::ReconcileActionExecutionKey::new(
        session_id,
        operation_id,
        0,
        &actions[0],
    )
    .unwrap();
    let input = ClaimedCreateInput {
        requested_container_id: "ctr-linked-first".to_string(),
        definition_digest: environment.definition_digest,
        applied_config_digest: "vzsc1-sha256:linked-first".to_string(),
        activation_payload_sha256: "c".repeat(64),
    };
    let linked = store
        .resolve_or_begin_claimed_successor(&claim, &input, &empty_claimed_allocator_target(), 10)
        .unwrap();
    assert!(
        execution_key
            .matches_activation_digest(&linked.action_digest)
            .unwrap()
    );
    assert_ne!(
        linked.action_digest,
        format!(
            "{}{}",
            execution_key.activation_digest_prefix().unwrap(),
            input.activation_payload_sha256
        )
    );
    store
        .publish_claimed_successor_failure(
            &claim,
            &linked.scope.reservation_id,
            "linked predecessor failed",
            11,
        )
        .unwrap();
    let linked = store
        .load_stack_container_create_intent(&linked.scope.reservation_id)
        .unwrap()
        .unwrap();

    let mut foreign = linked.clone();
    foreign.scope.reservation_id = "zz-foreign-duplicate-head".to_string();
    foreign.scope.machine_incarnation_id =
        Some(MachineIncarnationId::new("inc_duplicate_replay_head").unwrap());
    foreign.requested_container_id = "ctr-foreign-duplicate".to_string();
    foreign.action_digest = "sha256:foreign-duplicate-head".to_string();
    foreign.created_at = 12;
    foreign.updated_at = 13;
    foreign.completed_at = Some(13);
    assert!(linked.scope.reservation_id < foreign.scope.reservation_id);
    store
        .conn
        .execute(
            "INSERT INTO stack_container_create_intents (
                reservation_id, schema_version, project_id, environment_id, machine_id,
                machine_incarnation_id, environment_generation, stack_id, service_name,
                replica_index, service_generation, requested_container_id, definition_digest,
                action_digest, status, intent_json, last_error, created_at, updated_at,
                completed_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                       ?14, ?15, ?16, ?17, ?18, ?19, ?20)",
            params![
                foreign.scope.reservation_id,
                foreign.schema_version,
                foreign.scope.project_id.as_str(),
                foreign.scope.environment_id.as_str(),
                foreign.scope.machine_id.as_str(),
                foreign
                    .scope
                    .machine_incarnation_id
                    .as_ref()
                    .unwrap()
                    .as_str(),
                i64::try_from(foreign.environment_generation).unwrap(),
                foreign.scope.stack_id,
                foreign.service_name,
                i64::from(foreign.replica_index),
                i64::try_from(foreign.service_generation).unwrap(),
                foreign.requested_container_id,
                foreign.definition_digest,
                foreign.action_digest,
                "failed",
                serde_json::to_string(&foreign).unwrap(),
                foreign.last_error,
                i64::try_from(foreign.created_at).unwrap(),
                i64::try_from(foreign.updated_at).unwrap(),
                i64::try_from(foreign.completed_at.unwrap()).unwrap(),
            ],
        )
        .unwrap();

    let error = store
        .start_reconcile_batch(session_id, "exact-batch", operation_id, 0, &actions)
        .unwrap_err();
    assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
    assert!(
        error
            .to_string()
            .contains("ambiguous latest journal generation")
    );
    assert_eq!(
        store.load_audit_log_for_session(session_id).unwrap().len(),
        1
    );
}

#[test]
fn started_claim_replay_rejects_generation_gaps_and_remove_successors() {
    for case in ["never-gap", "remove-successor"] {
        let store = StateStore::in_memory().unwrap();
        let seeded = exact_batch_actions_for_claim(&store);
        let action = if case == "never-gap" {
            seeded[0].clone()
        } else {
            seeded[2].clone()
        };
        let actions = vec![action];
        let session_id = format!("rs-replay-{case}");
        install_unstarted_batch(&store, &session_id, "op-replay-generation", &actions);
        let claim = store
            .start_reconcile_batch(
                &session_id,
                "exact-batch",
                "op-replay-generation",
                0,
                &actions,
            )
            .unwrap()
            .remove(0);

        let workload = actions[0].precondition().workload();
        let environment = store
            .load_environment_instance(workload.environment_id.as_str())
            .unwrap()
            .unwrap();
        let action_digest = format!(
            "{}{}",
            crate::reconcile::ReconcileActionExecutionKey::new(
                &session_id,
                "op-replay-generation",
                0,
                &actions[0],
            )
            .unwrap()
            .activation_digest_prefix()
            .unwrap(),
            "b".repeat(64)
        );

        if case == "remove-successor" {
            store
                .begin_claimed_predecessor_cleanup(&claim, 200)
                .unwrap();
            store
                .complete_claimed_predecessor_cleanup(&claim, 201)
                .unwrap();
        }

        let input = ClaimedCreateInput {
            requested_container_id: format!("ctr-replay-{case}"),
            definition_digest: environment.definition_digest.clone(),
            applied_config_digest: "vzsc1-sha256:replay-generation".to_string(),
            activation_payload_sha256: "b".repeat(64),
        };
        let first_successor = if case == "never-gap" {
            store
                .resolve_or_begin_claimed_successor(
                    &claim,
                    &input,
                    &empty_claimed_allocator_target(),
                    222,
                )
                .unwrap()
        } else {
            let predecessor_generation = match actions[0].precondition().journal_head() {
                crate::reconcile::ExpectedJournalHead::Exact {
                    service_generation, ..
                } => *service_generation,
                crate::reconcile::ExpectedJournalHead::NeverJournaled => unreachable!(),
            };
            let intent = StackContainerCreateIntent {
                schema_version: StackContainerCreateIntent::SCHEMA_VERSION,
                scope: vz_runtime_contract::ContainerGenerationScope {
                    reservation_id: format!("reservation-remove-successor-{case}"),
                    project_id: workload.project_id.clone(),
                    environment_id: workload.environment_id.clone(),
                    machine_id: workload.machine_id.clone(),
                    machine_incarnation_id: Some(workload.machine_incarnation_id.clone()),
                    stack_id: workload.stack_id.clone(),
                },
                environment_generation: environment.lifecycle_generation,
                service_name: actions[0].target().service_name.clone(),
                replica_index: actions[0].target().index(),
                service_generation: predecessor_generation + 1,
                requested_container_id: input.requested_container_id.clone(),
                definition_digest: input.definition_digest.clone(),
                action_digest: action_digest.clone(),
                applied_config_digest: Some(input.applied_config_digest.clone()),
                status: StackContainerCreateStatus::Intent,
                last_error: None,
                created_at: 222,
                updated_at: 222,
                completed_at: None,
            };
            inject_journal_intent_for_test(&store, &intent);
            intent
        };
        if case == "never-gap" {
            assert_eq!(first_successor.service_generation, 1);
            store
                .publish_claimed_successor_failure(
                    &claim,
                    &first_successor.scope.reservation_id,
                    "force a generation gap",
                    223,
                )
                .unwrap();
            let mut gap = first_successor.clone();
            gap.scope.reservation_id = "reservation-never-gap-2".to_string();
            gap.service_generation = 2;
            gap.created_at = 224;
            gap.updated_at = 224;
            gap.status = StackContainerCreateStatus::Intent;
            gap.last_error = None;
            gap.completed_at = None;
            inject_journal_intent_for_test(&store, &gap);
            assert_eq!(
                store
                    .load_stack_container_create_intent(&gap.scope.reservation_id)
                    .unwrap()
                    .unwrap()
                    .service_generation,
                2
            );
        } else {
            assert_eq!(first_successor.service_generation, 2);
        }

        let error = store
            .start_reconcile_batch(
                &session_id,
                "exact-batch",
                "op-replay-generation",
                0,
                &actions,
            )
            .unwrap_err();
        assert_eq!(error.machine_code(), MachineErrorCode::StateConflict);
    }
}

#[test]
fn claim_admission_holds_atomic_snapshot_against_concurrent_topology_change() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("claim-topology-race.db");
    let store = StateStore::open_with_pragmas(&path, StateStorePragmas::daemon_defaults()).unwrap();
    let seeded = exact_batch_actions_for_claim(&store);
    let actions = vec![seeded[0].clone()];
    install_unstarted_batch(&store, "rs-claim-race", "op-claim-race", &actions);

    let workload = actions[0].precondition().workload().clone();
    let mut changed_environment = store
        .load_project_state(workload.project_id.as_str())
        .unwrap()
        .unwrap()
        .environments
        .into_iter()
        .find(|environment| environment.environment_id == workload.environment_id)
        .unwrap();
    changed_environment.lifecycle_generation += 1;
    changed_environment.updated_at += 1;

    let validation_barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let claim_barrier = validation_barrier.clone();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let claim_path = path.clone();
    let claim_actions = actions.clone();
    let claim = std::thread::spawn(move || {
        let claim_store =
            StateStore::open_with_pragmas(&claim_path, StateStorePragmas::daemon_defaults())
                .unwrap();
        claim_store.start_reconcile_batch_after_validation(
            "rs-claim-race",
            "exact-batch",
            "op-claim-race",
            0,
            &claim_actions,
            Box::new(move || {
                claim_barrier.wait();
                release_rx.recv().unwrap();
            }),
        )
    });
    validation_barrier.wait();

    let mutation_path = path.clone();
    let (attempted_tx, attempted_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let mutation = std::thread::spawn(move || {
        let mutation_store =
            StateStore::open_with_pragmas(&mutation_path, StateStorePragmas::daemon_defaults())
                .unwrap();
        attempted_tx.send(()).unwrap();
        let result = mutation_store.conn.execute(
            "UPDATE environment_instances
             SET lifecycle_generation = ?1, updated_at = ?2, instance_json = ?3
             WHERE environment_id = ?4",
            params![
                i64::try_from(changed_environment.lifecycle_generation).unwrap(),
                i64::try_from(changed_environment.updated_at).unwrap(),
                serde_json::to_string(&changed_environment).unwrap(),
                changed_environment.environment_id.as_str(),
            ],
        );
        finished_tx.send(()).unwrap();
        result.map(|affected| assert_eq!(affected, 1))
    });
    attempted_rx.recv().unwrap();
    assert!(
        finished_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "topology writer must wait behind the claim transaction"
    );
    release_tx.send(()).unwrap();
    assert_eq!(claim.join().unwrap().unwrap().len(), 1);
    mutation.join().unwrap().unwrap();
    finished_rx.recv().unwrap();

    assert_eq!(
        store
            .load_audit_log_for_session("rs-claim-race")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .load_environment_instance(workload.environment_id.as_str())
            .unwrap()
            .unwrap()
            .lifecycle_generation,
        actions[0].precondition().environment_generation() + 1
    );
}

#[test]
fn exact_batch_successful_subsets_advance_monotonically_until_completion() {
    let store = StateStore::in_memory().unwrap();
    let actions = exact_batch_actions_for_claim(&store);
    let session = ReconcileSession {
        session_id: "rs-exact-subsets".to_string(),
        stack_name: "exact-batch".to_string(),
        operation_id: "exact-operation".to_string(),
        status: ReconcileSessionStatus::Active,
        actions_hash: crate::reconcile::compute_actions_hash(&actions),
        next_action_index: 0,
        total_actions: actions.len(),
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        completed_at: None,
    };
    store.create_reconcile_batch(&session, &actions).unwrap();
    store
        .start_reconcile_batch(
            &session.session_id,
            &session.stack_name,
            &session.operation_id,
            0,
            &actions[..2],
        )
        .unwrap();
    let first_outcomes = exact_outcomes(&actions[..2], None);
    let first = store
        .commit_reconcile_batch(
            &session.session_id,
            &session.stack_name,
            &session.operation_id,
            0,
            &actions[..2],
            &first_outcomes,
        )
        .unwrap();
    assert_eq!(first.next_action_index, 2);
    assert_eq!(first.status, ReconcileSessionStatus::Active);
    assert_eq!(
        store
            .load_reconcile_progress(&session.stack_name)
            .unwrap()
            .unwrap()
            .next_action_index,
        2
    );

    store
        .start_reconcile_batch(
            &session.session_id,
            &session.stack_name,
            &session.operation_id,
            2,
            &actions[2..],
        )
        .unwrap();
    let mut final_outcomes = exact_outcomes(&actions[2..], None);
    final_outcomes[0].absolute_index = 2;
    let final_commit = store
        .commit_reconcile_batch(
            &session.session_id,
            &session.stack_name,
            &session.operation_id,
            2,
            &actions[2..],
            &final_outcomes,
        )
        .unwrap();
    assert_eq!(final_commit.next_action_index, 3);
    assert_eq!(final_commit.status, ReconcileSessionStatus::Completed);
    assert!(
        store
            .load_reconcile_progress(&session.stack_name)
            .unwrap()
            .is_none()
    );
}
