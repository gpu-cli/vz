#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use super::*;
use vz_runtime_contract::{
    Architecture, EnvironmentId, HostImportId, MachineState, TargetSpec, TransportProtocol,
};

fn machine_spec(name: &str, profile: &str, os: &str) -> vz_runtime_contract::MachineSpec {
    let mut spec: vz_runtime_contract::MachineSpec = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "name": name,
        "profile": profile,
        "target": {"os": os, "arch": "aarch64", "image": "sha256:fixture"}
    }))
    .expect("a Machine spec");
    spec.name = name.to_string();
    spec
}

fn developer_linux(name: &str) -> vz_runtime_contract::MachineSpec {
    machine_spec(name, "developer", "linux")
}

fn import_spec(
    name: &str,
    machine: &str,
    host_port: u16,
    guest_port: Option<u16>,
) -> HostImportSpec {
    HostImportSpec {
        schema_version: 1,
        name: name.to_string(),
        machine: machine.to_string(),
        protocol: TransportProtocol::Tcp,
        host_port,
        guest_port,
        alias: None,
    }
}

fn spec_with(
    imports: Vec<HostImportSpec>,
    machines: Vec<vz_runtime_contract::MachineSpec>,
) -> EnvironmentSpec {
    EnvironmentSpec {
        secret_bindings: Vec::new(),
        schema_version: 1,
        default_machine: None,
        machines,
        networks: Vec::new(),
        endpoints: Vec::new(),
        host_exports: Vec::new(),
        host_imports: imports,
        // Criterion 17 added volumes to the spec; a host import declares none.
        volumes: Vec::new(),
    }
}

fn machine_instance(
    name: &str,
    machine_id: &MachineId,
    environment_id: &EnvironmentId,
) -> MachineInstance {
    MachineInstance {
        fork: None,
        schema_version: 1,
        machine_id: machine_id.clone(),
        environment_id: environment_id.clone(),
        name: name.to_string(),
        profile: MachineProfile::Developer,
        target: TargetSpec {
            os: OperatingSystem::Linux,
            arch: Architecture::Aarch64,
            image: "sha256:fixture".to_string(),
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
        state: MachineState::Creating,
        legacy_sandbox_id: None,
    }
}

fn import_instance(
    name: &str,
    machine_id: &MachineId,
    environment_id: &EnvironmentId,
) -> HostImportInstance {
    HostImportInstance {
        schema_version: 1,
        import_id: HostImportId::generate(),
        environment_id: environment_id.clone(),
        machine_id: machine_id.clone(),
        name: name.to_string(),
    }
}

/// The positive case. Every denial below is evidence only because this one
/// resolves: a declared import of a Developer Linux Machine becomes exactly one
/// grant naming one host loopback port, held by exactly that Machine.
#[test]
fn a_declared_import_resolves_to_one_grant_for_exactly_its_machine() {
    let environment_id = EnvironmentId::generate();
    let machine_id = MachineId::generate();
    let spec = spec_with(
        vec![import_spec("db", "machine-0", 5432, Some(15432))],
        vec![developer_linux("machine-0")],
    );
    refuse_unsupported_host_imports(&spec).expect("a Developer Linux import is admitted");
    let resolved = resolve_environment_host_imports(
        &spec,
        &[machine_instance("machine-0", &machine_id, &environment_id)],
        &[import_instance("db", &machine_id, &environment_id)],
    )
    .expect("the declared import joins its instance");
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].name, "db");
    assert_eq!(resolved[0].host_port, 5432);
    assert_eq!(resolved[0].guest_port, 15432);
    assert_eq!(resolved[0].machine_id, machine_id);

    let grants = boot_import_grants(&resolved).expect("credentials");
    assert_eq!(grants.len(), 1);
    let machine_grants = grants.get(&machine_id).expect("grants for this Machine");
    assert_eq!(machine_grants.len(), 1);
    assert_eq!(machine_grants[0].host_port, 5432);
    assert_eq!(machine_grants[0].guest_port, 15432);
    // The projection that crosses to the guest cannot carry the host port.
    let view = machine_grants[0].guest_view();
    assert_eq!(view.guest_port, 15432);
    assert_eq!(view.credential, machine_grants[0].credential);
}

#[test]
fn an_absent_guest_port_reuses_the_host_port() {
    let spec = spec_with(
        vec![import_spec("db", "machine-0", 5432, None)],
        vec![developer_linux("machine-0")],
    );
    refuse_unsupported_host_imports(&spec).expect("admitted");
    let environment_id = EnvironmentId::generate();
    let machine_id = MachineId::generate();
    let resolved = resolve_environment_host_imports(
        &spec,
        &[machine_instance("machine-0", &machine_id, &environment_id)],
        &[import_instance("db", &machine_id, &environment_id)],
    )
    .expect("joined");
    assert_eq!(resolved[0].guest_port, 5432);
}

#[test]
fn an_import_naming_a_machine_the_environment_does_not_declare_is_refused() {
    let spec = spec_with(
        vec![import_spec("db", "absent", 5432, None)],
        vec![developer_linux("machine-0")],
    );
    assert_eq!(
        refuse_unsupported_host_imports(&spec),
        Err(HostImportError::UnknownMachine {
            import: "db".to_string(),
            machine: "absent".to_string()
        })
    );
}

#[test]
fn a_hardened_or_native_machine_cannot_hold_an_import() {
    for (profile, os) in [("hardened", "linux"), ("developer", "macos")] {
        let spec = spec_with(
            vec![import_spec("db", "machine-x", 5432, None)],
            vec![machine_spec("machine-x", profile, os)],
        );
        assert_eq!(
            refuse_unsupported_host_imports(&spec),
            Err(HostImportError::UnsupportedMachine {
                import: "db".to_string(),
                machine: "machine-x".to_string()
            }),
            "{profile}/{os} must not hold a host import"
        );
    }
}

#[test]
fn port_zero_is_refused_on_both_sides_because_it_names_no_service() {
    let host_zero = spec_with(
        vec![import_spec("db", "machine-0", 0, None)],
        vec![developer_linux("machine-0")],
    );
    assert_eq!(
        refuse_unsupported_host_imports(&host_zero),
        Err(HostImportError::ZeroHostPort {
            import: "db".to_string()
        })
    );
    let guest_zero = spec_with(
        vec![import_spec("db", "machine-0", 5432, Some(0))],
        vec![developer_linux("machine-0")],
    );
    assert_eq!(
        refuse_unsupported_host_imports(&guest_zero),
        Err(HostImportError::ZeroGuestPort {
            import: "db".to_string()
        })
    );
}

#[test]
fn two_imports_of_one_name_are_refused_because_one_name_is_one_service() {
    let spec = spec_with(
        vec![
            import_spec("db", "machine-0", 5432, Some(15432)),
            import_spec("db", "machine-0", 5433, Some(15433)),
        ],
        vec![developer_linux("machine-0")],
    );
    assert_eq!(
        refuse_unsupported_host_imports(&spec),
        Err(HostImportError::DuplicateName {
            import: "db".to_string()
        })
    );
}

#[test]
fn two_imports_on_one_machines_guest_port_are_refused_and_across_machines_are_not() {
    let colliding = spec_with(
        vec![
            import_spec("db", "machine-0", 5432, Some(15432)),
            import_spec("cache", "machine-0", 6379, Some(15432)),
        ],
        vec![developer_linux("machine-0")],
    );
    assert_eq!(
        refuse_unsupported_host_imports(&colliding),
        Err(HostImportError::DuplicateGuestPort {
            first: "db".to_string(),
            second: "cache".to_string(),
            machine: "machine-0".to_string(),
            port: 15432
        })
    );
    // The same guest port on two Machines is two separate loopbacks; and two
    // Machines granted the same host service is two grants, not a collision.
    let separate = spec_with(
        vec![
            import_spec("db-a", "machine-0", 5432, Some(15432)),
            import_spec("db-b", "machine-1", 5432, Some(15432)),
        ],
        vec![developer_linux("machine-0"), developer_linux("machine-1")],
    );
    refuse_unsupported_host_imports(&separate).expect("two Machines may each be granted");
}

#[test]
fn a_name_the_open_frame_cannot_carry_is_refused_at_admission() {
    let long = "n".repeat(MAX_NAME_BYTES + 1);
    let spec = spec_with(
        vec![import_spec(&long, "machine-0", 5432, None)],
        vec![developer_linux("machine-0")],
    );
    assert_eq!(
        refuse_unsupported_host_imports(&spec),
        Err(HostImportError::NameTooLong {
            import: long,
            length: MAX_NAME_BYTES + 1
        })
    );
}

#[test]
fn an_instance_with_no_declaration_and_a_declaration_with_no_instance_are_both_refused() {
    let environment_id = EnvironmentId::generate();
    let machine_id = MachineId::generate();
    let machines = [machine_instance("machine-0", &machine_id, &environment_id)];
    let spec = spec_with(
        vec![import_spec("db", "machine-0", 5432, None)],
        vec![developer_linux("machine-0")],
    );
    assert_eq!(
        resolve_environment_host_imports(
            &spec,
            &machines,
            &[import_instance("orphan", &machine_id, &environment_id)]
        ),
        Err(HostImportError::UndeclaredInstance {
            import: "orphan".to_string()
        })
    );
    assert_eq!(
        resolve_environment_host_imports(&spec, &machines, &[]),
        Err(HostImportError::MissingInstance {
            import: "db".to_string()
        })
    );
}

#[test]
fn a_persisted_instance_pointing_at_another_machine_is_refused() {
    let environment_id = EnvironmentId::generate();
    let machine_id = MachineId::generate();
    let sibling_id = MachineId::generate();
    let spec = spec_with(
        vec![import_spec("db", "machine-0", 5432, None)],
        vec![developer_linux("machine-0"), developer_linux("machine-1")],
    );
    let machines = [
        machine_instance("machine-0", &machine_id, &environment_id),
        machine_instance("machine-1", &sibling_id, &environment_id),
    ];
    // The instance says machine-1; the declaration says machine-0. Serving it
    // would hand a host service to a Machine nothing granted it to.
    assert_eq!(
        resolve_environment_host_imports(
            &spec,
            &machines,
            &[import_instance("db", &sibling_id, &environment_id)]
        ),
        Err(HostImportError::UnknownMachine {
            import: "db".to_string(),
            machine: "machine-0".to_string()
        })
    );
    // And an instance naming a Machine id this Environment does not hold at all.
    assert!(matches!(
        resolve_environment_host_imports(
            &spec,
            &machines,
            &[import_instance(
                "db",
                &MachineId::generate(),
                &environment_id
            )]
        ),
        Err(HostImportError::UnknownInstanceMachine { .. })
    ));
}

#[test]
fn an_environment_that_declares_no_import_resolves_to_no_grant_at_all() {
    let spec = spec_with(Vec::new(), vec![developer_linux("machine-0")]);
    refuse_unsupported_host_imports(&spec).expect("nothing to refuse");
    let resolved = resolve_environment_host_imports(&spec, &[], &[]).expect("nothing to join");
    assert!(resolved.is_empty());
    assert!(
        boot_import_grants(&resolved)
            .expect("no credentials")
            .is_empty()
    );
}

#[test]
fn every_grant_gets_its_own_credential_and_two_machines_never_share_one() {
    let environment_id = EnvironmentId::generate();
    let first_id = MachineId::generate();
    let second_id = MachineId::generate();
    let spec = spec_with(
        vec![
            import_spec("db", "machine-0", 5432, Some(15432)),
            import_spec("cache", "machine-0", 6379, Some(16379)),
            // Deliberately the same host service, granted to a second Machine.
            import_spec("db-mirror", "machine-1", 5432, Some(15432)),
        ],
        vec![developer_linux("machine-0"), developer_linux("machine-1")],
    );
    refuse_unsupported_host_imports(&spec).expect("admitted");
    let resolved = resolve_environment_host_imports(
        &spec,
        &[
            machine_instance("machine-0", &first_id, &environment_id),
            machine_instance("machine-1", &second_id, &environment_id),
        ],
        &[
            import_instance("db", &first_id, &environment_id),
            import_instance("cache", &first_id, &environment_id),
            import_instance("db-mirror", &second_id, &environment_id),
        ],
    )
    .expect("joined");
    let grants = boot_import_grants(&resolved).expect("credentials");
    assert_eq!(grants.len(), 2);
    let mut credentials: Vec<[u8; CREDENTIAL_BYTES]> = grants
        .values()
        .flat_map(|machine| machine.iter().map(|grant| grant.credential))
        .collect();
    assert_eq!(credentials.len(), 3);
    credentials.sort_unstable();
    credentials.dedup();
    assert_eq!(
        credentials.len(),
        3,
        "each grant must hold its own secret, including two grants to one host service"
    );
    // No credential is the all-zero array a forgotten fill would leave behind.
    assert!(
        credentials
            .iter()
            .all(|credential| credential != &[0u8; CREDENTIAL_BYTES])
    );
    // A second minting of the same resolution produces different secrets, which
    // is what makes a stopped Machine's credential unreplayable.
    let again = boot_import_grants(&resolved).expect("credentials");
    let reminted: Vec<[u8; CREDENTIAL_BYTES]> = again
        .values()
        .flat_map(|machine| machine.iter().map(|grant| grant.credential))
        .collect();
    for credential in &reminted {
        assert!(
            !credentials.contains(credential),
            "a re-minted credential must not repeat a previous boot's"
        );
    }
}

#[test]
fn resolution_order_is_stable_so_two_ups_present_the_same_request() {
    let environment_id = EnvironmentId::generate();
    let machine_id = MachineId::generate();
    let spec = spec_with(
        vec![
            import_spec("zeta", "machine-0", 5432, Some(15432)),
            import_spec("alpha", "machine-0", 6379, Some(16379)),
        ],
        vec![developer_linux("machine-0")],
    );
    let machines = [machine_instance("machine-0", &machine_id, &environment_id)];
    let forward = resolve_environment_host_imports(
        &spec,
        &machines,
        &[
            import_instance("zeta", &machine_id, &environment_id),
            import_instance("alpha", &machine_id, &environment_id),
        ],
    )
    .expect("joined");
    let reversed = resolve_environment_host_imports(
        &spec,
        &machines,
        &[
            import_instance("alpha", &machine_id, &environment_id),
            import_instance("zeta", &machine_id, &environment_id),
        ],
    )
    .expect("joined");
    assert_eq!(forward, reversed);
}
