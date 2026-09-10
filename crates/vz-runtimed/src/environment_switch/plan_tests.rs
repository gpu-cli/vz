//! Addressing is the point of this module, so every test is about which address
//! an attachment gets and why, not about forwarding or ownership.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;

use super::*;
use vz_runtime_contract::{
    Architecture, EnvironmentState, MachineInstance, MachineProfile, MachineState,
    NetworkAttachmentInstance, NetworkInstance, OperatingSystem, ProjectId,
    TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};

const PROJECT: &str = "prj_0123456789abcdef0123456789abcdef";
const ENVIRONMENT: &str = "env_0123456789abcdef0123456789abcdef";

fn environment_id() -> vz_runtime_contract::EnvironmentId {
    vz_runtime_contract::EnvironmentId::new(ENVIRONMENT.to_string()).unwrap()
}

fn machine_id(suffix: u8) -> MachineId {
    MachineId::new(format!("mch_{:032x}", suffix)).unwrap()
}

fn network_id(suffix: u8) -> NetworkId {
    NetworkId::new(format!("net_{:032x}", suffix)).unwrap()
}

fn attachment_id(suffix: u32) -> NetworkAttachmentId {
    NetworkAttachmentId::new(format!("att_{:032x}", suffix)).unwrap()
}

/// A Developer Linux Machine, the only profile that may declare an attachment.
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
            image: "vz/linux".to_string(),
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

fn network(suffix: u8, kind: NetworkKind, cidr: Option<&str>) -> NetworkInstance {
    NetworkInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        network_id: network_id(suffix),
        environment_id: environment_id(),
        name: format!("network-{suffix}"),
        kind,
        cidr: cidr.map(str::to_string),
    }
}

fn attachment(id: u32, machine: u8, network: u8) -> NetworkAttachmentInstance {
    NetworkAttachmentInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        attachment_id: attachment_id(id),
        environment_id: environment_id(),
        machine_id: machine_id(machine),
        network_id: network_id(network),
    }
}

fn environment(
    machines: Vec<MachineInstance>,
    networks: Vec<NetworkInstance>,
    network_attachments: Vec<NetworkAttachmentInstance>,
) -> EnvironmentInstance {
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
        machines,
        networks,
        endpoints: Vec::new(),
        network_attachments,
        host_exports: Vec::new(),
        host_imports: Vec::new(),
        egress: Vec::new(),
        ownership: Vec::new(),
        legacy_migration: None,
        created_at: 0,
        updated_at: 0,
    }
}

/// Two Developer Machines, both on one declared /24.
fn pair_on_one_network() -> EnvironmentInstance {
    environment(
        vec![machine(1), machine(2)],
        vec![network(1, NetworkKind::Private, Some("10.42.0.0/24"))],
        vec![attachment(1, 1, 1), attachment(2, 2, 1)],
    )
}

fn addresses(plan: &FabricPlan) -> Vec<String> {
    plan.networks
        .iter()
        .flat_map(|network| {
            network
                .ports
                .iter()
                .map(|port| format!("{}={}", port.attachment_id, port.address))
        })
        .collect()
}

#[test]
fn every_attached_machine_gets_one_port_on_each_network_it_declares() {
    let environment = environment(
        vec![machine(1), machine(2)],
        vec![
            network(1, NetworkKind::Private, Some("10.42.0.0/24")),
            network(2, NetworkKind::Private, Some("10.43.0.0/24")),
        ],
        vec![
            attachment(1, 1, 1),
            attachment(2, 2, 1),
            attachment(3, 1, 2),
        ],
    );
    let plan = plan_environment_fabric(&environment).unwrap();
    assert_eq!(plan.networks.len(), 2);
    assert_eq!(plan.networks[0].ports.len(), 2);
    assert_eq!(plan.networks[1].ports.len(), 1);
    // A port number is only meaningful inside one switch, so both networks
    // start at zero and each network's numbers are distinct.
    for network in &plan.networks {
        let ports: BTreeSet<_> = network.ports.iter().map(|port| port.port).collect();
        assert_eq!(ports.len(), network.ports.len());
        let macs: BTreeSet<_> = network.ports.iter().map(|port| port.mac).collect();
        assert_eq!(macs.len(), network.ports.len());
        let hosts: BTreeSet<_> = network.ports.iter().map(|port| port.address).collect();
        assert_eq!(hosts.len(), network.ports.len());
        assert_eq!(network.members().len(), network.ports.len());
    }
    assert_eq!(
        plan.attached_machines(),
        BTreeSet::from([machine_id(1), machine_id(2)])
    );
}

#[test]
fn the_same_environment_plans_the_same_addresses_however_its_records_are_ordered() {
    // Determinism has to hold against record order, not just against repetition:
    // nothing promises the store returns networks or attachments in the order the
    // definition listed them, and an address that moved with that order would
    // move under the Machine on an ordinary re-Up.
    let environment = environment(
        vec![machine(2), machine(1)],
        vec![
            network(2, NetworkKind::Private, Some("10.43.0.0/24")),
            network(1, NetworkKind::Private, Some("10.42.0.0/24")),
        ],
        vec![
            attachment(3, 1, 2),
            attachment(2, 2, 1),
            attachment(1, 1, 1),
        ],
    );
    let mut reordered = environment.clone();
    reordered.networks.reverse();
    reordered.network_attachments.reverse();
    reordered.machines.reverse();
    assert_eq!(
        plan_environment_fabric(&environment).unwrap(),
        plan_environment_fabric(&reordered).unwrap()
    );
}

#[test]
fn an_address_is_pinned_to_the_identifiers_it_is_derived_from() {
    // The pinned values are what makes a change to the derivation visible: a
    // Machine that comes back up after a stop must present the address its
    // switch already expects, so the derivation is part of the contract and not
    // an implementation detail free to drift.
    let plan = plan_environment_fabric(&pair_on_one_network()).unwrap();
    assert_eq!(
        addresses(&plan),
        vec![
            "att_00000000000000000000000000000001=10.42.0.40".to_string(),
            "att_00000000000000000000000000000002=10.42.0.142".to_string(),
        ]
    );
    // And to the network and Environment, not only to the attachment.
    let mut moved = pair_on_one_network();
    moved.networks[0].network_id = network_id(9);
    moved.network_attachments[0].network_id = network_id(9);
    moved.network_attachments[1].network_id = network_id(9);
    assert_ne!(
        addresses(&plan_environment_fabric(&moved).unwrap()),
        addresses(&plan)
    );
}

#[test]
fn a_derived_offset_a_sibling_already_holds_probes_forward_by_one() {
    // Two attachments on one network can hash into one offset. Handing both the
    // same address would make delivery ambiguous in exactly the way the fabric
    // refuses, so the loser probes forward, deterministically, and the winner
    // keeps the offset it derived.
    let capacity = Ipv4Cidr::parse("10.42.0.0/24").unwrap().host_capacity();
    let colliding = (1..4096_u32)
        .map(|id| {
            let offset = derive_u32(
                HOST_DERIVATION_DOMAIN,
                &[
                    ENVIRONMENT,
                    network_id(1).as_str(),
                    attachment_id(id).as_str(),
                ],
            ) % capacity;
            (offset, id)
        })
        .fold(
            std::collections::BTreeMap::<u32, Vec<u32>>::new(),
            |mut found, (offset, id)| {
                found.entry(offset).or_default().push(id);
                found
            },
        )
        .into_values()
        .find(|ids| ids.len() >= 2)
        .expect("two attachment identities that derive one offset");
    let (first, second) = (colliding[0], colliding[1]);
    assert!(first < second, "the probe order is attachment_id order");
    let environment = environment(
        vec![machine(1), machine(2)],
        vec![network(1, NetworkKind::Private, Some("10.42.0.0/24"))],
        vec![attachment(first, 1, 1), attachment(second, 2, 1)],
    );
    let plan = plan_environment_fabric(&environment).unwrap();
    let ports = &plan.networks[0].ports;
    assert_eq!(ports.len(), 2);
    assert_ne!(ports[0].address, ports[1].address);
    assert_eq!(
        u32::from(ports[1].address),
        u32::from(ports[0].address) + 1,
        "the second attachment takes the next offset, not a fresh derivation"
    );
    // The winner is unmoved: alone it would derive the same address.
    let alone = environment.clone();
    let mut alone = alone;
    alone.network_attachments.truncate(1);
    assert_eq!(
        plan_environment_fabric(&alone).unwrap().networks[0].ports[0].address,
        ports[0].address
    );
}

#[test]
fn the_gateway_offset_is_never_handed_to_a_machine() {
    // A /29 has six offsets and five assignable hosts, so every host address in
    // the range is used and the reserved one is the only survivor.
    let environment = environment(
        (1..=5).map(machine).collect(),
        vec![network(1, NetworkKind::Private, Some("10.42.0.0/29"))],
        (1..=5_u32)
            .map(|index| attachment(index, u8::try_from(index).unwrap(), 1))
            .collect(),
    );
    let plan = plan_environment_fabric(&environment).unwrap();
    let assigned: BTreeSet<_> = plan.networks[0]
        .ports
        .iter()
        .map(|port| u32::from(port.address) & 0x7)
        .collect();
    assert_eq!(assigned, BTreeSet::from([2, 3, 4, 5, 6]));
}

#[test]
fn a_network_with_fewer_host_addresses_than_attachments_is_refused() {
    let environment = environment(
        vec![machine(1), machine(2)],
        vec![network(1, NetworkKind::Private, Some("10.42.0.0/30"))],
        vec![attachment(1, 1, 1), attachment(2, 2, 1)],
    );
    assert_eq!(
        plan_environment_fabric(&environment),
        Err(FabricPlanError::NetworkTooSmall {
            network: "network-1".to_string(),
            capacity: 1,
            attached: 2,
        })
    );
}

#[test]
fn a_network_without_a_declared_range_gets_a_derived_one_that_nothing_else_covers() {
    let environment = environment(
        vec![machine(1)],
        vec![
            network(1, NetworkKind::Private, None),
            network(2, NetworkKind::Private, None),
        ],
        vec![attachment(1, 1, 1), attachment(2, 1, 2)],
    );
    let plan = plan_environment_fabric(&environment).unwrap();
    let ranges: Vec<_> = plan.networks.iter().map(|network| network.cidr).collect();
    for range in &ranges {
        assert!(
            range.to_string().starts_with("10."),
            "derived ranges live in 10/8, got {range}"
        );
        assert_eq!(range.prefix, DERIVED_SUBNET_PREFIX);
    }
    assert!(!ranges[0].overlaps(&ranges[1]));
    // Derivation, not allocation: the same Environment derives the same ranges.
    assert_eq!(
        plan_environment_fabric(&environment).unwrap().networks[0].cidr,
        ranges[0]
    );
}

#[test]
fn a_derived_range_moves_off_a_range_a_sibling_declared() {
    let mut environment = environment(
        vec![machine(1)],
        vec![network(1, NetworkKind::Private, None)],
        vec![attachment(1, 1, 1)],
    );
    let derived = plan_environment_fabric(&environment).unwrap().networks[0].cidr;
    // Declaring exactly the range network-1 would derive forces it elsewhere:
    // two networks covering one range would leave a Machine on both unable to
    // route between them.
    environment
        .networks
        .push(network(2, NetworkKind::Private, Some(&derived.to_string())));
    let plan = plan_environment_fabric(&environment).unwrap();
    let moved = plan
        .networks
        .iter()
        .find(|network| network.network_id == network_id(1))
        .unwrap()
        .cidr;
    assert_ne!(moved, derived);
    assert!(!moved.overlaps(&derived));
    assert_eq!(
        moved.base - derived.base,
        1 << (32 - u32::from(DERIVED_SUBNET_PREFIX)),
        "the probe steps one /24 forward rather than re-deriving"
    );
}

#[test]
fn two_declared_ranges_that_overlap_are_refused_rather_than_moved() {
    let environment = environment(
        vec![machine(1)],
        vec![
            network(1, NetworkKind::Private, Some("10.42.0.0/16")),
            network(2, NetworkKind::Private, Some("10.42.7.0/24")),
        ],
        vec![attachment(1, 1, 1)],
    );
    assert_eq!(
        plan_environment_fabric(&environment),
        Err(FabricPlanError::OverlappingNetworks {
            first: "network-1".to_string(),
            second: "network-2".to_string(),
        })
    );
}

#[test]
fn a_range_this_fabric_cannot_address_is_refused_with_its_reason() {
    for (declared, fragment) in [
        ("10.42.0.0", "expected `A.B.C.D/len`"),
        ("10.42.0.0/33", "is outside"),
        ("10.42.0.0/31", "is outside"),
        ("10.42.0.0/4", "is outside"),
        ("10.42.0.5/24", "host bits set"),
        ("fd00::/64", "not an IPv4 address"),
        ("10.42.0.0/x", "not a prefix length"),
    ] {
        let environment = environment(
            vec![machine(1)],
            vec![network(1, NetworkKind::Private, Some(declared))],
            vec![attachment(1, 1, 1)],
        );
        let Err(FabricPlanError::InvalidCidr { cidr, reason, .. }) =
            plan_environment_fabric(&environment)
        else {
            panic!("`{declared}` was accepted");
        };
        assert_eq!(cidr, declared);
        assert!(
            reason.contains(fragment),
            "`{declared}` gave `{reason}`, expected `{fragment}`"
        );
    }
}

#[test]
fn a_simulated_public_network_plans_an_edge_and_a_private_one_does_not() {
    // A public-like network is this same fabric plus one more port, which the
    // daemon keeps rather than attaches to a Machine. It is the only difference
    // between the two kinds at plan time, and it is visible here as a port that
    // no attachment claimed.
    let private = plan_environment_fabric(&pair_on_one_network()).unwrap();
    assert_eq!(private.networks[0].gateway, None);
    assert_eq!(
        private.networks[0].members().len(),
        private.networks[0].ports.len()
    );

    let mut environment = pair_on_one_network();
    environment.networks[0].kind = NetworkKind::SimulatedPublic;
    let plan = plan_environment_fabric(&environment).unwrap();
    let network = &plan.networks[0];
    let edge = network
        .gateway
        .as_ref()
        .expect("a public-like network has an edge");
    // The reserved offset, which `assign_host_offset` never hands out on any
    // network: a network that gains an edge does not move a Machine.
    assert_eq!(
        edge.address,
        "10.42.0.1".parse::<std::net::Ipv4Addr>().unwrap()
    );
    assert_eq!(
        private.networks[0]
            .ports
            .iter()
            .map(|port| port.address)
            .collect::<Vec<_>>(),
        network
            .ports
            .iter()
            .map(|port| port.address)
            .collect::<Vec<_>>(),
        "the same attachments plan to the same addresses either way"
    );
    for port in &network.ports {
        assert_ne!(port.address, edge.address);
        assert_ne!(port.mac, edge.mac);
    }
    // A member of the switch like any other station, numbered after every
    // Machine so no Machine's port number moved.
    assert_eq!(network.members().len(), network.ports.len() + 1);
    assert_eq!(
        edge.port,
        PortId(u32::try_from(network.ports.len()).unwrap())
    );
    assert!(edge.mac.is_locally_administered() && !edge.mac.is_group());

    // Derived, so the same persisted Environment plans the same edge every Up.
    let again = plan_environment_fabric(&environment).unwrap();
    assert_eq!(again.networks[0].gateway.as_ref(), Some(edge));
}

#[test]
fn a_native_machine_on_a_public_like_network_is_refused_rather_than_left_unable_to_resolve() {
    // A public-like network publishes no static host entry, so its names exist
    // only through its edge's resolver. The native addressing channel can give
    // a macOS Machine the address and the route but not the resolver, and a
    // Machine on that network that could not resolve its names would be exactly
    // the silent half-configuration that admitting a declaration nothing serves
    // produces everywhere else.
    let mut environment = pair_on_one_network();
    environment.networks[0].kind = NetworkKind::SimulatedPublic;
    environment.machines[1].target.os = OperatingSystem::Macos;
    assert_eq!(
        plan_environment_fabric(&environment),
        Err(FabricPlanError::UnresolvedPublicMachine {
            machine: "machine-2".to_string(),
            os: OperatingSystem::Macos,
            network: "network-1".to_string(),
        })
    );
    // The same Machine on a private network is fine: its names travel as a
    // static table that needs no resolver at all.
    environment.networks[0].kind = NetworkKind::Private;
    assert!(plan_environment_fabric(&environment).is_ok());
}

#[test]
fn an_attachment_that_names_an_absent_machine_or_network_is_refused() {
    let mut orphan_machine = pair_on_one_network();
    orphan_machine
        .machines
        .retain(|machine| machine.machine_id == machine_id(1));
    assert_eq!(
        plan_environment_fabric(&orphan_machine),
        Err(FabricPlanError::DanglingAttachment {
            attachment: attachment_id(2).to_string(),
            kind: "Machine",
            id: machine_id(2).to_string(),
        })
    );
    let mut orphan_network = pair_on_one_network();
    orphan_network.network_attachments[0].network_id = network_id(7);
    assert_eq!(
        plan_environment_fabric(&orphan_network),
        Err(FabricPlanError::DanglingAttachment {
            attachment: attachment_id(1).to_string(),
            kind: "network",
            id: network_id(7).to_string(),
        })
    );
}

#[test]
fn a_machine_that_cannot_hold_a_fabric_port_is_refused_rather_than_planned_one() {
    // Hardened is the restricted profile and declares none of this topology;
    // native Windows is PLANNED rather than shipped and has no NIC a switch
    // could attach to. The topology contract refuses either declaration, so a
    // persisted record like this is corruption; planning a port for it would
    // build a switch whose member never appears.
    //
    // Windows was not covered here before — the old loop paired Hardened Linux
    // with Developer macOS, and macOS is now admitted — so this stands beside
    // `a_developer_macos_machine_holds_a_fabric_port_like_a_linux_one` rather
    // than being weakened by it: one refused shape was replaced by a refused
    // shape that had no coverage at all, and Hardened is asserted on both
    // targets instead of one.
    for (profile, os) in [
        (MachineProfile::Hardened, OperatingSystem::Linux),
        (MachineProfile::Hardened, OperatingSystem::Macos),
        (MachineProfile::Developer, OperatingSystem::Windows),
        (MachineProfile::Hardened, OperatingSystem::Windows),
    ] {
        let mut environment = pair_on_one_network();
        environment.machines[1].profile = profile;
        environment.machines[1].target.os = os;
        assert_eq!(
            plan_environment_fabric(&environment),
            Err(FabricPlanError::UnsupportedMachine {
                machine: "machine-2".to_string(),
                profile,
                os,
            })
        );
    }
}

#[test]
fn a_developer_macos_machine_holds_a_fabric_port_like_a_linux_one() {
    // Criterion 5 needs a service path that crosses between a Linux Machine and
    // a native macOS Machine, so a macOS Machine must be planned a real port —
    // "no longer refused" would not say the fabric was built.
    //
    // The plan for the mixed pair is asserted to be byte-identical to the plan
    // for the all-Linux pair. That is the strongest available statement of the
    // module's own rule that addresses are derived and never leased: the
    // Machine's target is not an input to any derivation, so flipping it moves
    // no port number, no MAC and no address, and a persisted Environment whose
    // Machine is macOS plans the fabric its saved guests already believe in.
    let linux_plan = plan_environment_fabric(&pair_on_one_network()).unwrap();

    let mut mixed = pair_on_one_network();
    mixed.machines[1].target.os = OperatingSystem::Macos;
    mixed.machines[1].target.image = "macos-26".to_string();
    let macos_plan = plan_environment_fabric(&mixed).unwrap();

    assert_eq!(macos_plan, linux_plan);

    // Stated positively as well, so this test still says what the fabric IS if
    // the Linux baseline it is compared against ever changes.
    assert_eq!(macos_plan.networks.len(), 1);
    let ports = &macos_plan.networks[0].ports;
    assert_eq!(ports.len(), 2);
    assert_eq!(
        macos_plan.attached_machines(),
        BTreeSet::from([machine_id(1), machine_id(2)])
    );
    let macos_port = ports
        .iter()
        .find(|port| port.machine_id == machine_id(2))
        .expect("the macOS Machine holds a port on the network it declared");
    assert_eq!(macos_port.attachment_id, attachment_id(2));
    let octets = macos_port.address.octets();
    assert_eq!(
        [octets[0], octets[1], octets[2]],
        [10, 42, 0],
        "{} is outside the declared range {}",
        macos_port.address,
        macos_plan.networks[0].cidr
    );
    // Offsets 0 and 1 are the subnet address and the reserved gateway; neither
    // is assignable, and 255 is the broadcast address.
    assert!((2..255).contains(&octets[3]), "{}", macos_port.address);
    // One address per port: a switch that assigned the same address twice would
    // drop one member's frames rather than forward them.
    assert_ne!(ports[0].address, ports[1].address);
    assert_ne!(ports[0].mac, ports[1].mac);
    assert_eq!(
        macos_port.mac,
        MacAddress::derive(ENVIRONMENT, machine_id(2).as_str(), network_id(1).as_str()),
        "the macOS port's MAC must be the derivation over its own identifiers"
    );
}

#[test]
fn an_all_macos_environment_plans_a_whole_fabric() {
    // The mixed pair above could pass with a rule that admitted macOS only
    // alongside a Linux Machine. Every Machine here is macOS, and the plan is
    // still the same fabric, because the target is not an input at all.
    let linux_plan = plan_environment_fabric(&pair_on_one_network()).unwrap();
    let mut all_macos = pair_on_one_network();
    for machine in &mut all_macos.machines {
        machine.target.os = OperatingSystem::Macos;
        machine.target.image = "macos-26".to_string();
    }
    assert_eq!(plan_environment_fabric(&all_macos).unwrap(), linux_plan);
}

#[test]
fn an_environment_with_no_declared_network_plans_no_fabric() {
    let plan =
        plan_environment_fabric(&environment(vec![machine(1)], Vec::new(), Vec::new())).unwrap();
    assert!(plan.is_empty());
    assert!(plan.attached_machines().is_empty());
}

#[test]
fn a_declared_network_with_no_attachment_still_gets_a_switch() {
    // The switch is the network's, not the Machines'. One with no members
    // forwards nothing, which is what an unattached network should do; skipping
    // it would make Stop and Delete reclaim a different set than Up installed.
    let plan = plan_environment_fabric(&environment(
        vec![machine(1)],
        vec![network(1, NetworkKind::Private, Some("10.42.0.0/24"))],
        Vec::new(),
    ))
    .unwrap();
    assert_eq!(plan.networks.len(), 1);
    assert!(plan.networks[0].ports.is_empty());
}

// --- Endpoint name resolution -------------------------------------------
//
// An endpoint is declaration and resolution only. Every test below asks which
// name resolves to which address and from where; none asserts that anything is
// listening, because nothing in this path binds, probes or waits.

fn endpoint_id(suffix: u32) -> vz_runtime_contract::EndpointId {
    vz_runtime_contract::EndpointId::new(format!("ept_{suffix:032x}")).unwrap()
}

fn endpoint(
    id: u32,
    name: &str,
    hostname: Option<&str>,
    machine: u8,
    network: u8,
) -> vz_runtime_contract::EndpointInstance {
    vz_runtime_contract::EndpointInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        endpoint_id: endpoint_id(id),
        environment_id: environment_id(),
        machine_id: machine_id(machine),
        network_id: network_id(network),
        name: name.to_string(),
        protocol: vz_runtime_contract::EndpointProtocol::Tcp,
        port: 5432,
        hostname: hostname.map(str::to_string),
    }
}

/// Every name that resolves on every network, as `network:name=address`.
fn resolved(plan: &FabricPlan) -> Vec<String> {
    plan.networks
        .iter()
        .flat_map(|network| {
            network
                .hosts()
                .into_iter()
                .map(move |(name, address)| format!("{}:{name}={address}", network.name))
        })
        .collect()
}

#[test]
fn a_declared_endpoint_resolves_to_the_fabric_address_of_the_machine_that_owns_it() {
    let mut environment = pair_on_one_network();
    environment.endpoints = vec![endpoint(1, "api", Some("api.internal"), 2, 1)];
    let plan = plan_environment_fabric(&environment).unwrap();

    // The address is the one the owning Machine's port already holds, read out
    // of the plan rather than derived a second time. If resolution ever grew a
    // derivation of its own the two would drift and the name would answer with
    // an address the switch does not forward to.
    let owner = plan.networks[0]
        .ports
        .iter()
        .find(|port| port.machine_id == machine_id(2))
        .expect("the owning Machine holds a port");
    assert_eq!(
        resolved(&plan),
        vec![format!("network-1:api.internal={}", owner.address)]
    );
}

#[test]
fn an_endpoint_that_declares_no_hostname_resolves_under_its_own_name() {
    // The default is the endpoint's name and not, say, the Machine's: the
    // declaration named the endpoint, so that is the identifier it already
    // asked to be known by.
    let mut environment = pair_on_one_network();
    environment.endpoints = vec![endpoint(1, "postgres", None, 1, 1)];
    let plan = plan_environment_fabric(&environment).unwrap();
    let owner = plan.networks[0]
        .ports
        .iter()
        .find(|port| port.machine_id == machine_id(1))
        .expect("the owning Machine holds a port");
    assert_eq!(
        resolved(&plan),
        vec![format!("network-1:postgres={}", owner.address)]
    );
}

#[test]
fn a_declared_hostname_is_used_verbatim_and_the_endpoint_name_is_not() {
    let mut environment = pair_on_one_network();
    environment.endpoints = vec![endpoint(1, "postgres", Some("db"), 1, 1)];
    let plan = plan_environment_fabric(&environment).unwrap();
    let names: Vec<&str> = plan.networks[0]
        .endpoints
        .iter()
        .map(|endpoint| endpoint.name.as_str())
        .collect();
    assert_eq!(names, vec!["db"]);
}

#[test]
fn every_machine_on_the_network_resolves_the_name_including_the_one_that_owns_it() {
    // The table is the network's, not the caller's: a service that reaches a
    // sibling by name must reach itself by its own name too, or the name would
    // mean one thing from outside the Machine and nothing from inside it.
    let mut environment = pair_on_one_network();
    environment.endpoints = vec![endpoint(1, "api", None, 2, 1)];
    let plan = plan_environment_fabric(&environment).unwrap();
    assert_eq!(plan.networks[0].ports.len(), 2);
    assert_eq!(plan.networks[0].endpoints.len(), 1);
}

#[test]
fn a_name_is_published_only_to_the_network_it_was_declared_on() {
    // A Machine with no port on the endpoint's network cannot route to its
    // address at all. Publishing the name there would turn a clear "unknown
    // host" into a connection that hangs, which is strictly worse.
    let environment = {
        let mut environment = environment(
            vec![machine(1), machine(2), machine(3)],
            vec![
                network(1, NetworkKind::Private, Some("10.42.0.0/24")),
                network(2, NetworkKind::Private, Some("10.43.0.0/24")),
            ],
            vec![
                attachment(1, 1, 1),
                attachment(2, 2, 1),
                attachment(3, 3, 2),
            ],
        );
        // One endpoint on each network, so this catches a resolution that
        // published Environment-wide *and* one that always attributed a name to
        // whichever network happened to be planned first.
        environment.endpoints = vec![
            endpoint(1, "api", None, 2, 1),
            endpoint(2, "worker", None, 3, 2),
        ];
        environment
    };
    let plan = plan_environment_fabric(&environment).unwrap();
    let by_name: BTreeMap<&str, Vec<&str>> = plan
        .networks
        .iter()
        .map(|network| {
            (
                network.name.as_str(),
                network
                    .endpoints
                    .iter()
                    .map(|endpoint| endpoint.name.as_str())
                    .collect(),
            )
        })
        .collect();
    assert_eq!(by_name["network-1"], vec!["api"]);
    assert_eq!(by_name["network-2"], vec!["worker"]);
}

#[test]
fn an_endpoint_whose_machine_holds_no_port_on_its_network_is_refused() {
    // Not given an address of its own: minting one here would put a Machine on
    // a network its declaration never attached it to.
    let mut environment = pair_on_one_network();
    environment.machines.push(machine(3));
    environment.endpoints = vec![endpoint(1, "api", None, 3, 1)];
    assert_eq!(
        plan_environment_fabric(&environment),
        Err(FabricPlanError::UnattachedEndpoint {
            endpoint: "api".to_string(),
            network: "network-1".to_string(),
        })
    );
}

#[test]
fn an_endpoint_naming_a_network_or_machine_the_environment_does_not_have_is_refused() {
    let mut environment = pair_on_one_network();
    environment.endpoints = vec![endpoint(1, "api", None, 2, 9)];
    assert_eq!(
        plan_environment_fabric(&environment),
        Err(FabricPlanError::DanglingEndpoint {
            endpoint: "api".to_string(),
            kind: "network",
            id: network_id(9).to_string(),
        })
    );

    let mut environment = pair_on_one_network();
    environment.endpoints = vec![endpoint(1, "api", None, 9, 1)];
    assert_eq!(
        plan_environment_fabric(&environment),
        Err(FabricPlanError::DanglingEndpoint {
            endpoint: "api".to_string(),
            kind: "Machine",
            id: machine_id(9).to_string(),
        })
    );
}

#[test]
fn two_endpoints_that_resolve_one_name_are_refused_rather_than_ordered() {
    // Whichever `/etc/hosts` line a resolver read first would decide which
    // Machine the name meant, and the declaration said nothing about which.
    let mut environment = pair_on_one_network();
    environment.endpoints = vec![
        endpoint(1, "api", None, 1, 1),
        endpoint(2, "other", Some("api"), 2, 1),
    ];
    assert_eq!(
        plan_environment_fabric(&environment),
        Err(FabricPlanError::AmbiguousEndpointName {
            name: "api".to_string(),
            first: "api".to_string(),
            second: "other".to_string(),
        })
    );
}

#[test]
fn resolution_depends_only_on_persisted_records_and_not_on_declaration_order() {
    let mut forward = pair_on_one_network();
    forward.endpoints = vec![
        endpoint(1, "api", None, 1, 1),
        endpoint(2, "db", None, 2, 1),
        endpoint(3, "cache", None, 1, 1),
    ];
    let mut reversed = forward.clone();
    reversed.endpoints.reverse();
    assert_eq!(
        resolved(&plan_environment_fabric(&forward).unwrap()),
        resolved(&plan_environment_fabric(&reversed).unwrap()),
    );
    // And the published order is the resolved name's, not the declaration's.
    let plan = plan_environment_fabric(&forward).unwrap();
    let names: Vec<&str> = plan.networks[0]
        .endpoints
        .iter()
        .map(|endpoint| endpoint.name.as_str())
        .collect();
    assert_eq!(names, vec!["api", "cache", "db"]);
}

#[test]
fn an_environment_that_declares_no_endpoint_resolves_no_name() {
    let plan = plan_environment_fabric(&pair_on_one_network()).unwrap();
    assert!(resolved(&plan).is_empty());
}
