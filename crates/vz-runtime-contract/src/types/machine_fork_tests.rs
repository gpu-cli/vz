#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::topology::{
    Architecture, CapabilitySet, EgressPolicy, EndpointProtocol, EndpointSpec, EnvironmentSpec,
    MachineCapability, MachineProfile, MachineResources, MachineSpec, NetworkKind, NetworkSpec,
    OperatingSystem, ProjectDefinition, ProjectId, TargetSpec, TopologyValidationError,
};

/// One Linux Machine on one private network, reachable, with one endpoint.
///
/// Deliberately the smallest topology that exercises everything a fork mints:
/// a network attachment, an egress record, and an endpoint it must NOT copy.
pub(super) fn definition() -> ProjectDefinition {
    ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: ProjectId::new("prj_fork").unwrap(),
        name: "fork".to_string(),
        environment: EnvironmentSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            default_machine: None,
            host_exports: Vec::new(),
            host_imports: Vec::new(),
            volumes: Vec::new(),
            machines: vec![MachineSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                name: "backend".to_string(),
                profile: MachineProfile::Developer,
                target: TargetSpec {
                    os: OperatingSystem::Linux,
                    arch: Architecture::Aarch64,
                    image: "vz-linux-appliance".to_string(),
                    version: Some("1.0".to_string()),
                    channel: None,
                    digest: Some(format!("sha256:{}", "a".repeat(64))),
                },
                resources: MachineResources::default(),
                requested_capabilities: CapabilitySet::new([MachineCapability::PosixExec]),
                workspace: None,
                networks: vec!["private".to_string()],
                egress: EgressPolicy::Allowed,
            }],
            networks: vec![NetworkSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                name: "private".to_string(),
                kind: NetworkKind::Private,
                cidr: Some("10.42.0.0/24".to_string()),
            }],
            endpoints: vec![EndpointSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                name: "api".to_string(),
                machine: "backend".to_string(),
                network: "private".to_string(),
                protocol: EndpointProtocol::Https,
                port: 443,
                hostname: None,
            }],
        },
    }
}

#[test]
fn a_fork_is_a_new_machine_in_the_same_environment_with_its_own_identity() {
    let definition = definition();
    let mut environment = definition.instantiate_environment("agent", 7).unwrap();
    let parent = environment.machines[0].machine_id.clone();

    let plan = environment.plan_machine_fork(&parent, "feat-x").unwrap();

    // Same Environment, new Machine, addressed `<machine>@<label>`.
    assert_eq!(plan.machine.environment_id, environment.environment_id);
    assert_ne!(*plan.machine_id(), parent);
    assert_eq!(plan.machine.name, "backend@feat-x");
    assert_eq!(plan.parent_machine_id(), Some(&parent));

    // Shape is inherited; runtime bindings are not. A context bound to the
    // parent's incarnation must never arrive with the disk.
    assert_eq!(plan.machine.profile, environment.machines[0].profile);
    assert_eq!(plan.machine.target, environment.machines[0].target);
    assert_eq!(plan.machine.docker_context, None);
    assert_eq!(plan.machine.incarnation, None);
    assert_eq!(plan.machine.runtime_identity, None);
    assert_eq!(plan.machine.backend, None);
    assert_eq!(plan.machine.state, MachineState::Creating);

    // A fresh attachment per parent network: this is what re-derives the
    // address. The network membership is copied; the attachment id is not.
    assert_eq!(plan.network_attachments.len(), 1);
    let attachment = &plan.network_attachments[0];
    assert_eq!(
        attachment.network_id,
        environment.network_attachments[0].network_id
    );
    assert_ne!(
        attachment.attachment_id,
        environment.network_attachments[0].attachment_id
    );
    assert_eq!(attachment.machine_id, *plan.machine_id());

    // Egress mirrors the parent verbatim.
    assert_eq!(
        plan.egress.as_ref().map(|egress| egress.policy),
        Some(EgressPolicy::Allowed)
    );

    plan.apply(&mut environment);
    environment.validate().unwrap();
    // The whole point: the aggregate still instantiates its definition even
    // though it now holds a Machine the definition never declared.
    crate::types::topology::validate_definition_instance_for_test(
        &definition.environment,
        &environment,
    )
    .unwrap();
}

#[test]
fn a_fork_mints_exactly_one_machine_fork_ownership_record_and_a_machine_record() {
    let definition = definition();
    let mut environment = definition.instantiate_environment("agent", 7).unwrap();
    let parent = environment.machines[0].machine_id.clone();
    let plan = environment.plan_machine_fork(&parent, "feat-x").unwrap();
    let forked = plan.machine_id().clone();
    plan.apply(&mut environment);

    for kind in [OwnedResourceKind::Machine, OwnedResourceKind::MachineFork] {
        assert_eq!(
            environment
                .ownership
                .iter()
                .filter(|record| record.resource_kind == kind
                    && record.resource_id == forked.as_str()
                    && record.machine_id.as_ref() == Some(&forked))
                .count(),
            1,
            "exactly one {kind:?} record for the fork"
        );
    }
    // The declared parent must NOT acquire a fork record: that record is the
    // only thing exempting a Machine from reconciliation.
    assert!(!environment.ownership.iter().any(|record| {
        record.resource_kind == OwnedResourceKind::MachineFork
            && record.resource_id == parent.as_str()
    }));
    environment.validate().unwrap();
}

#[test]
fn a_forks_ownership_graph_is_refused_when_its_fork_record_is_missing_or_misplaced() {
    let definition = definition();
    let mut environment = definition.instantiate_environment("agent", 7).unwrap();
    let parent = environment.machines[0].machine_id.clone();
    let plan = environment.plan_machine_fork(&parent, "feat-x").unwrap();
    let forked = plan.machine_id().clone();
    plan.apply(&mut environment);
    environment.validate().unwrap();

    // Dropping the record leaves a Machine that Delete cannot account for and
    // that reconciliation would treat as declared.
    let mut missing = environment.clone();
    missing
        .ownership
        .retain(|record| record.resource_kind != OwnedResourceKind::MachineFork);
    assert!(matches!(
        missing.validate(),
        Err(TopologyValidationError::OwnershipMismatch { ref kind, .. })
            if kind == "machine_fork"
    ));

    // Giving a DECLARED Machine a fork record would silently exempt it.
    let mut misplaced = environment.clone();
    for record in &mut misplaced.ownership {
        if record.resource_kind == OwnedResourceKind::MachineFork {
            record.resource_id = parent.to_string();
            record.machine_id = Some(parent.clone());
        }
    }
    assert!(misplaced.validate().is_err());

    // A record naming a Machine that does not exist at all is refused by the
    // reverse arm rather than admitted by the `_ => true` default.
    let mut dangling = environment.clone();
    for record in &mut dangling.ownership {
        if record.resource_kind == OwnedResourceKind::MachineFork {
            record.resource_id = "mch_00000000000000000000000000000000".to_string();
        }
    }
    assert!(dangling.validate().is_err());
    let _ = forked;
}

#[test]
fn reconciliation_leaves_a_fork_alone_but_still_refuses_real_definition_drift() {
    let definition = definition();
    let mut environment = definition.instantiate_environment("agent", 7).unwrap();
    let parent = environment.machines[0].machine_id.clone();
    for label in ["feat-x", "feat-y"] {
        let plan = environment.plan_machine_fork(&parent, label).unwrap();
        plan.apply(&mut environment);
    }
    environment.validate().unwrap();
    assert_eq!(environment.machines.len(), 3);
    crate::types::topology::validate_definition_instance_for_test(
        &definition.environment,
        &environment,
    )
    .unwrap();

    // The exemption is exactly "is a fork", not "is an extra Machine". An
    // unaccounted extra Machine is still drift and is still refused.
    let mut smuggled = environment.clone();
    smuggled.machines[2].fork = None;
    smuggled
        .ownership
        .retain(|record| record.resource_kind != OwnedResourceKind::MachineFork);
    assert!(
        crate::types::topology::validate_definition_instance_for_test(
            &definition.environment,
            &smuggled,
        )
        .is_err()
    );

    // And a definition that really did change a declared Machine is still drift.
    let mut changed = definition.clone();
    changed.environment.machines[0].name = "renamed".to_string();
    assert!(
        crate::types::topology::validate_definition_instance_for_test(
            &changed.environment,
            &environment,
        )
        .is_err()
    );
}

#[test]
fn a_fork_does_not_republish_its_parents_declared_service_coordinates() {
    let definition = definition();
    let mut environment = definition.instantiate_environment("agent", 7).unwrap();
    let parent = environment.machines[0].machine_id.clone();
    let endpoints_before = environment.endpoints.len();
    let plan = environment.plan_machine_fork(&parent, "feat-x").unwrap();
    let forked = plan.machine_id().clone();
    plan.apply(&mut environment);

    // Endpoint, host-export and host-import names are Environment-unique, and a
    // host export additionally owns a host port. Two forks cannot both publish
    // `api` on one port, so a fork publishes none of them.
    assert_eq!(environment.endpoints.len(), endpoints_before);
    assert!(
        !environment
            .endpoints
            .iter()
            .any(|endpoint| endpoint.machine_id == forked)
    );
    assert!(
        !environment
            .host_exports
            .iter()
            .any(|export| export.machine_id == forked)
    );
    environment.validate().unwrap();
}

#[test]
fn forking_refuses_an_unknown_parent_a_fork_parent_a_bad_label_and_a_taken_label() {
    let definition = definition();
    let mut environment = definition.instantiate_environment("agent", 7).unwrap();
    let parent = environment.machines[0].machine_id.clone();

    assert!(matches!(
        environment.plan_machine_fork(&MachineId::generate(), "feat-x"),
        Err(TopologyValidationError::MissingReference { ref kind, .. }) if kind == "machine"
    ));
    for label in ["", "-leading", "has space", "has/slash", &"x".repeat(65)] {
        assert!(
            environment.plan_machine_fork(&parent, label).is_err(),
            "label {label:?} must be refused"
        );
    }

    let plan = environment.plan_machine_fork(&parent, "feat-x").unwrap();
    let forked = plan.machine_id().clone();
    plan.apply(&mut environment);

    // The same label twice is a duplicate name, refused rather than made unique.
    assert!(matches!(
        environment.plan_machine_fork(&parent, "feat-x"),
        Err(TopologyValidationError::Duplicate { ref kind, .. }) if kind == "machine_name"
    ));
    // A fork of a fork would need a two-label address this release does not mint.
    assert!(matches!(
        environment.plan_machine_fork(&forked, "feat-y"),
        Err(TopologyValidationError::InvalidIdentifier { ref kind, .. })
            if kind == "machine_fork_parent"
    ));
}

#[test]
fn fork_addresses_parse_exactly_and_round_trip() {
    let plain = MachineForkAddress::parse("backend").unwrap();
    assert_eq!(plain.machine, "backend");
    assert_eq!(plain.label, None);
    assert_eq!(plain.to_selector(), "backend");

    let forked = MachineForkAddress::parse("backend@feat-x").unwrap();
    assert_eq!(forked.machine, "backend");
    assert_eq!(forked.label.as_deref(), Some("feat-x"));
    assert_eq!(forked.to_selector(), "backend@feat-x");

    for bad in [
        "@feat-x",
        "backend@",
        "backend@a@b",
        "backend@-x",
        "backend@a b",
    ] {
        assert!(
            MachineForkAddress::parse(bad).is_err(),
            "selector {bad:?} must be refused"
        );
    }
}

#[test]
fn a_branch_normalises_into_a_label_by_a_rule_an_agent_can_apply_itself() {
    // The default label is the worktree's branch, so the mapping has to be a
    // rule rather than a lookup: an agent must be able to predict the name it
    // will target before the fork exists.
    for (branch, expected) in [
        ("feat-x", "feat-x"),
        ("feat/third-environment", "feat-third-environment"),
        ("james/gpu-mesh", "james-gpu-mesh"),
        ("release/1.2.3", "release-1.2.3"),
        ("--weird--", "weird"),
    ] {
        assert_eq!(fork_label_from_branch(branch).as_deref(), Some(expected));
        assert!(is_valid_fork_label(expected));
    }
    assert_eq!(fork_label_from_branch("///"), None);
    assert_eq!(fork_label_from_branch(""), None);
    // Truncation keeps the result inside the Machine-name budget.
    let long = fork_label_from_branch(&"a".repeat(200)).unwrap();
    assert_eq!(long.len(), MAX_FORK_LABEL_LENGTH);
}

#[test]
fn forks_are_discoverable_by_parent_and_resolvable_by_address() {
    let definition = definition();
    let mut environment = definition.instantiate_environment("agent", 7).unwrap();
    let parent = environment.machines[0].machine_id.clone();
    for label in ["feat-y", "feat-x"] {
        let plan = environment.plan_machine_fork(&parent, label).unwrap();
        plan.apply(&mut environment);
    }

    // `vz status` lists forks with their labels so an agent can discover what
    // exists; the order is by name so successive listings agree.
    let forks = environment.forks_of(&parent);
    assert_eq!(
        forks
            .iter()
            .map(|fork| fork.name.as_str())
            .collect::<Vec<_>>(),
        vec!["backend@feat-x", "backend@feat-y"]
    );

    let address = MachineForkAddress::parse("backend@feat-y").unwrap();
    let resolved = environment.machine_by_address(&address).unwrap();
    assert_eq!(resolved.name, "backend@feat-y");
    assert_eq!(
        resolved.fork.as_ref().map(|origin| origin.label.as_str()),
        Some("feat-y")
    );
    assert_eq!(
        environment
            .machine_by_address(&MachineForkAddress::parse("backend").unwrap())
            .map(|machine| machine.machine_id.clone()),
        Some(parent)
    );
    assert!(
        environment
            .machine_by_address(&MachineForkAddress::parse("backend@absent").unwrap())
            .is_none()
    );
}
