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
        fork: None,
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
        volumes: Vec::new(),
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
            declaration.mac,
            MacAddress::derive(ENVIRONMENT, machine.as_str(), NETWORK).to_string()
        );
    }

    // The derived L3 address reaches the Machine that boots. Until this
    // adapter existed the plan computed an address and nothing carried it, so
    // the assertion that matters is that each declaration holds the address
    // the plan assigned that Machine's port, with the range's prefix, rendered
    // as the one kernel argument the guest parses.
    let plan = crate::environment_switch::plan::plan_environment_fabric(&environment)
        .expect("the same Environment plans");
    let planned = &plan.networks[0];
    for port in &planned.ports {
        let declaration = minted[&port.machine_id][0].declaration();
        assert_eq!(declaration.ipv4, port.address);
        assert_eq!(declaration.prefix, planned.cidr.prefix());
        // Nothing routes off a private fabric yet, so no gateway is named.
        assert_eq!(declaration.gateway, None);
        assert_eq!(
            declaration.kernel_argument(0),
            format!(
                "vz.net.0={},{}/{}",
                port.mac,
                port.address,
                planned.cidr.prefix()
            )
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
async fn a_declared_endpoint_name_reaches_every_machine_on_its_network_as_one_kernel_argument() {
    // The last link in the chain: the plan resolves a name to an address and
    // this is what carries it to the Machine that boots. Nothing here binds a
    // listener, probes the endpoint's port or waits for it — the Machine simply
    // boots able to resolve the name.
    let (_root, daemon, lease) = fixture().await;
    let mut environment = environment(NETWORK, NetworkKind::Private);
    environment.endpoints = vec![vz_runtime_contract::EndpointInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        endpoint_id: vz_runtime_contract::EndpointId::new(format!("ept_{:032x}", 1)).unwrap(),
        environment_id: environment_id(),
        // Owned by Machine 2, so Machine 1's copy is the interesting one: it
        // resolves a name to an address that is not its own.
        machine_id: machine_id(2),
        network_id: NetworkId::new(NETWORK.to_string()).unwrap(),
        name: "database".to_string(),
        protocol: vz_runtime_contract::EndpointProtocol::Tcp,
        port: 5432,
        hostname: None,
    }];

    let minted = daemon
        .install_environment_fabric(&lease, &environment, &nobody())
        .await
        .unwrap();
    let plan = plan_environment_fabric(&environment).expect("the same Environment plans");
    let owner_address = plan.networks[0]
        .ports
        .iter()
        .find(|port| port.machine_id == machine_id(2))
        .expect("the owning Machine holds a port")
        .address;

    for machine in [machine_id(1), machine_id(2)] {
        let declaration = minted[&machine][0].declaration();
        assert_eq!(
            declaration.hosts.len(),
            1,
            "every Machine on the network resolves the name, its owner included"
        );
        // The hostname was absent, so the endpoint's own name is what resolves.
        assert_eq!(declaration.hosts[0].name, "database");
        // And it resolves to the address the plan already assigned that port,
        // not to a second derivation that could drift from it.
        assert_eq!(declaration.hosts[0].address, owner_address);
    }
    assert!(
        vz_oci_macos::DeclaredAttachment::kernel_argument(
            minted[&machine_id(1)][0].declaration(),
            0
        )
        .starts_with("vz.net.0="),
        "the port argument is unchanged by the presence of a name"
    );

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
        crate::environment_switch::runtime::NetworkSwitch::start(&network.name, network.members())
            .unwrap();
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
async fn a_simulated_public_definition_starts_a_switch_and_an_edge_on_it() {
    // The switch is the same switch. What a public-like declaration adds is a
    // port the daemon keeps, and a route and resolver the Machines are booted
    // pointing at.
    let (_root, daemon, lease) = fixture().await;
    let environment = environment(NETWORK, NetworkKind::SimulatedPublic);
    let minted = daemon
        .install_environment_fabric(&lease, &environment, &nobody())
        .await
        .expect("a public-like network is applied");
    assert_eq!(
        daemon
            .environment_switches()
            .networks(&environment_id())
            .await,
        vec![NETWORK.to_string()]
    );
    let plan =
        crate::environment_switch::plan::plan_environment_fabric(&environment).expect("plan");
    let edge = plan.networks[0]
        .gateway
        .as_ref()
        .expect("a public-like network has an edge")
        .address;
    assert!(!minted.is_empty());
    for attachments in minted.values() {
        for attachment in attachments {
            // A Machine on a public-like network is booted knowing where its
            // route leads and who answers its names, and both are the edge that
            // is already running by the time this descriptor exists.
            assert_eq!(attachment.declaration().gateway, Some(edge));
            assert_eq!(attachment.declaration().dns, Some(edge));
        }
    }
    // Stopping the Environment joins the edge with its switch and reports what
    // the edge decided, which is the only record of a frame it refused.
    let owner = ResourceOwner {
        project_id: ProjectId::new(PROJECT.to_string()).unwrap(),
        environment_id: environment_id(),
        machine_id: None,
    };
    let receipt = daemon
        .environment_switches()
        .stop(&lease, &owner)
        .await
        .expect("stopped");
    assert!(receipt.edges.contains_key(NETWORK), "{:?}", receipt.edges);
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
