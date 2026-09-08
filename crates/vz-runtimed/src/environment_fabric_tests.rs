//! Installing a fabric is the point of this module, so every test is about what
//! got started and what a Machine is handed, not about forwarding rules.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::environment_switch::plan::plan_environment_fabric;
use crate::environment_switch::{Disposition, HEADER_LEN, MacAddress, PortId};
use crate::{RuntimeDaemon, RuntimedConfig};
use std::sync::Arc;
use vz_runtime_contract::{
    Architecture, EnvironmentId, EnvironmentState, MachineInstance, MachineProfile, MachineState,
    NetworkAttachmentId, NetworkAttachmentInstance, NetworkId, NetworkInstance, NetworkKind,
    OperatingSystem, ProjectId, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};

const PROJECT: &str = "prj_0123456789abcdef0123456789abcdef";
const ENVIRONMENT: &str = "env_0123456789abcdef0123456789abcdef";
const NETWORK: &str = "net_0123456789abcdef0123456789abcdef";
const OTHER_NETWORK: &str = "net_fedcba9876543210fedcba9876543210";

fn environment_id() -> EnvironmentId {
    EnvironmentId::new(ENVIRONMENT.to_string()).unwrap()
}

fn machine_id(suffix: u8) -> MachineId {
    MachineId::new(format!("mch_{suffix:032x}")).unwrap()
}

fn machine(suffix: u8) -> MachineInstance {
    MachineInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        machine_id: machine_id(suffix),
        environment_id: environment_id(),
        name: format!("machine-{suffix}"),
        profile: MachineProfile::Developer,
        target: TargetSpec {
            os: OperatingSystem::Linux,
            arch: Architecture::Aarch64,
            image: "vz-linux-appliance".to_string(),
            version: None,
            channel: None,
            digest: None,
        },
        resources: Default::default(),
        requested_capabilities: Default::default(),
        negotiated_capabilities: Default::default(),
        backend: None,
        incarnation: None,
        runtime_identity: None,
        docker_context: None,
        state: MachineState::Stopped,
        legacy_sandbox_id: None,
    }
}

fn environment(network: &str, kind: NetworkKind) -> EnvironmentInstance {
    let network_id = NetworkId::new(network.to_string()).unwrap();
    EnvironmentInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        environment_id: environment_id(),
        project_id: ProjectId::new(PROJECT.to_string()).unwrap(),
        name: "fabric".to_string(),
        definition_digest: format!("sha256:{}", "a".repeat(64)),
        state: EnvironmentState::Stopped,
        lifecycle_generation: 0,
        active_operation_id: None,
        bindings: Vec::new(),
        machines: vec![machine(1), machine(2)],
        networks: vec![NetworkInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            network_id: network_id.clone(),
            environment_id: environment_id(),
            name: "private".to_string(),
            kind,
            cidr: Some("10.42.0.0/24".to_string()),
        }],
        endpoints: Vec::new(),
        network_attachments: (1..=2_u8)
            .map(|index| NetworkAttachmentInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                attachment_id: NetworkAttachmentId::new(format!("att_{index:032x}")).unwrap(),
                environment_id: environment_id(),
                machine_id: machine_id(index),
                network_id: network_id.clone(),
            })
            .collect(),
        host_exports: Vec::new(),
        host_imports: Vec::new(),
        egress: Vec::new(),
        ownership: Vec::new(),
        legacy_migration: None,
        created_at: 0,
        updated_at: 0,
    }
}

/// A daemon and a lease over the Environment these fixtures describe.
async fn fixture() -> (
    tempfile::TempDir,
    Arc<RuntimeDaemon>,
    crate::environment_runtime_controller::EnvironmentControllerLease,
) {
    let root = tempfile::Builder::new()
        .prefix("vz-fabric-")
        .tempdir_in("/private/tmp")
        .unwrap();
    let daemon = Arc::new(
        RuntimeDaemon::start(RuntimedConfig {
            state_store_path: root.path().join("state.db"),
            runtime_data_dir: root.path().join("r"),
            socket_path: root.path().join("d.sock"),
        })
        .unwrap(),
    );
    let lease = daemon
        .acquire_environment_controller(
            &ProjectId::new(PROJECT.to_string()).unwrap(),
            &environment_id(),
        )
        .await
        .unwrap();
    (root, daemon, lease)
}

fn nobody() -> BTreeSet<MachineId> {
    BTreeSet::new()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_declared_network_becomes_a_running_switch_with_one_port_per_attached_machine() {
    let (_root, daemon, lease) = fixture().await;
    let environment = environment(NETWORK, NetworkKind::Private);
    let minted = daemon
        .install_environment_fabric(&lease, &environment, &nobody())
        .await
        .unwrap();
    assert_eq!(
        daemon
            .environment_switches()
            .networks(&environment_id())
            .await,
        vec![NETWORK.to_string()]
    );
    assert_eq!(
        minted.keys().collect::<Vec<_>>(),
        vec![&machine_id(1), &machine_id(2)]
    );
    for (machine, attachments) in &minted {
        assert_eq!(attachments.len(), 1);
        let declaration = attachments[0].declaration();
        assert_eq!(declaration.network_id, NETWORK);
        assert_eq!(declaration.mtu, 1500);
        // The address a Machine's NIC presents has to be the one the switch
        // assigned that port: the fabric refuses any frame whose source is not
        // the assigned address, so a NIC configured with anything else would be
        // silently mute rather than misdelivered.
        assert_eq!(
            declaration.address,
            MacAddress::derive(ENVIRONMENT, machine.as_str(), NETWORK).to_string()
        );
    }
    daemon
        .reclaim_environment_switches(
            &lease,
            &ProjectId::new(PROJECT.to_string()).unwrap(),
            &environment_id(),
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_switch_this_installs_is_the_one_stop_and_delete_reclaim() {
    // Ownership is registered before any descriptor is handed out, so a fabric
    // that started is always one the shared reclamation path can join.
    let (_root, daemon, lease) = fixture().await;
    let environment = environment(NETWORK, NetworkKind::Private);
    daemon
        .install_environment_fabric(&lease, &environment, &nobody())
        .await
        .unwrap();
    let owner = ResourceOwner {
        project_id: ProjectId::new(PROJECT.to_string()).unwrap(),
        environment_id: environment_id(),
        machine_id: None,
    };
    let receipt = daemon
        .environment_switches()
        .stop(&lease, &owner)
        .await
        .unwrap();
    assert_eq!(
        receipt.networks.keys().collect::<Vec<_>>(),
        vec![&NETWORK.to_string()]
    );
    assert!(
        daemon
            .environment_switches()
            .networks(&environment_id())
            .await
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_planned_membership_is_a_fabric_two_machines_actually_reach_each_other_on() {
    // Up cannot admit a declared network yet, so this is where the planned
    // membership is proven usable: the plan's addresses are fed to a real switch
    // and one Machine's frame is delivered to its sibling. A planned address that
    // did not match the port it was assigned to would be dropped as
    // `SourceAddressNotAssigned` rather than forwarded.
    let environment = environment(NETWORK, NetworkKind::Private);
    let plan = plan_environment_fabric(&environment).unwrap();
    let network = &plan.networks[0];
    let (mut switch, guests) =
        crate::environment_switch::runtime::NetworkSwitch::start(network.members()).unwrap();
    assert_eq!(guests.len(), 2);
    let mut fabric = crate::environment_switch::Fabric::new();
    for (port, address) in network.members() {
        fabric.attach(port, address).unwrap();
    }
    let mut frame = Vec::with_capacity(HEADER_LEN);
    frame.extend_from_slice(&network.ports[1].mac.bytes());
    frame.extend_from_slice(&network.ports[0].mac.bytes());
    frame.extend_from_slice(&0x0800_u16.to_be_bytes());
    assert_eq!(
        fabric.forward(network.ports[0].port, &frame),
        Disposition::Unicast(network.ports[1].port)
    );
    // A sibling's address on the wrong port is refused, which is what keeps a
    // guest from claiming a neighbour's traffic.
    assert!(matches!(
        fabric.forward(network.ports[1].port, &frame),
        Disposition::Drop(_)
    ));
    assert!(switch.ports().contains_key(&PortId(0)));
    switch.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_up_over_an_intact_running_fabric_mints_nothing() {
    // Every attached Machine is already running and already holds its guest end.
    // A fresh port for it would reach nothing, so the running fabric stands.
    let (_root, daemon, lease) = fixture().await;
    let environment = environment(NETWORK, NetworkKind::Private);
    daemon
        .install_environment_fabric(&lease, &environment, &nobody())
        .await
        .unwrap();
    let live = BTreeSet::from([machine_id(1), machine_id(2)]);
    assert!(
        daemon
            .install_environment_fabric(&lease, &environment, &live)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        daemon
            .environment_switches()
            .networks(&environment_id())
            .await,
        vec![NETWORK.to_string()]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_machine_that_still_needs_a_boot_cannot_join_a_running_fabric() {
    // A switch fixes its membership when it is constructed, so the second
    // Machine cannot be given a port now, and restarting the switch to make room
    // would disconnect the first. The half-established fabric is reported rather
    // than a Machine booted silently unattached.
    let (_root, daemon, lease) = fixture().await;
    let environment = environment(NETWORK, NetworkKind::Private);
    daemon
        .install_environment_fabric(&lease, &environment, &nobody())
        .await
        .unwrap();
    let live = BTreeSet::from([machine_id(1)]);
    assert!(matches!(
        daemon
            .install_environment_fabric(&lease, &environment, &live)
            .await,
        Err(EnvironmentFabricError::Conflict(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_running_machine_cannot_be_handed_a_port_for_a_fabric_that_is_not_there() {
    let (_root, daemon, lease) = fixture().await;
    let environment = environment(NETWORK, NetworkKind::Private);
    let live = BTreeSet::from([machine_id(1)]);
    assert!(matches!(
        daemon
            .install_environment_fabric(&lease, &environment, &live)
            .await,
        Err(EnvironmentFabricError::Conflict(_))
    ));
    assert!(
        daemon
            .environment_switches()
            .networks(&environment_id())
            .await
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_running_fabric_for_different_networks_is_refused_rather_than_re_membered() {
    let (_root, daemon, lease) = fixture().await;
    daemon
        .install_environment_fabric(
            &lease,
            &environment(NETWORK, NetworkKind::Private),
            &nobody(),
        )
        .await
        .unwrap();
    assert!(matches!(
        daemon
            .install_environment_fabric(
                &lease,
                &environment(OTHER_NETWORK, NetworkKind::Private),
                &nobody(),
            )
            .await,
        Err(EnvironmentFabricError::Conflict(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_simulated_public_definition_starts_no_switch_at_all() {
    // SimulatedPublic is this fabric plus external egress. Starting the private
    // half would attach Machines to a network that silently lacks the
    // reachability its definition asked for.
    let (_root, daemon, lease) = fixture().await;
    assert!(matches!(
        daemon
            .install_environment_fabric(
                &lease,
                &environment(NETWORK, NetworkKind::SimulatedPublic),
                &nobody(),
            )
            .await,
        Err(EnvironmentFabricError::Plan(
            crate::environment_switch::plan::FabricPlanError::EgressNotImplemented { .. }
        ))
    ));
    assert!(
        daemon
            .environment_switches()
            .networks(&environment_id())
            .await
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_environment_that_declares_no_network_starts_no_switch_and_mints_nothing() {
    let (_root, daemon, lease) = fixture().await;
    let mut environment = environment(NETWORK, NetworkKind::Private);
    environment.networks.clear();
    environment.network_attachments.clear();
    assert!(
        daemon
            .install_environment_fabric(&lease, &environment, &nobody())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        daemon
            .environment_switches()
            .networks(&environment_id())
            .await
            .is_empty()
    );
}
