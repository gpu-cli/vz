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
fn a_simulated_public_network_is_refused_until_an_egress_path_exists() {
    // SimulatedPublic is this private fabric plus external egress. Planning the
    // private half and calling it done would start Machines that silently lack
    // the boundary their definition asked for.
    let mut environment = pair_on_one_network();
    environment.networks[0].kind = NetworkKind::SimulatedPublic;
    assert_eq!(
        plan_environment_fabric(&environment),
        Err(FabricPlanError::EgressNotImplemented {
            network: "network-1".to_string(),
        })
    );
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
    // Hardened and native-target Machines have no NIC a switch could attach to.
    // The topology contract refuses the declaration, so a persisted record like
    // this is corruption; planning a port for it would build a switch whose
    // member never appears.
    for (profile, os) in [
        (MachineProfile::Hardened, OperatingSystem::Linux),
        (MachineProfile::Developer, OperatingSystem::Macos),
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
