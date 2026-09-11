#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use super::*;
use vz_runtime_contract::{Architecture, EnvironmentId, HostExportId, MachineState, TargetSpec};

fn machine_spec(name: &str) -> vz_runtime_contract::MachineSpec {
    let mut spec: vz_runtime_contract::MachineSpec = serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "name": name,
        "profile": "developer",
        "target": {"os": "linux", "arch": "aarch64", "image": "sha256:fixture"}
    }))
    .expect("a Developer Linux Machine spec");
    spec.name = name.to_string();
    spec
}

fn export_spec(name: &str, machine: &str, host_port: Option<u16>) -> HostExportSpec {
    HostExportSpec {
        schema_version: 1,
        name: name.to_string(),
        machine: machine.to_string(),
        protocol: TransportProtocol::Tcp,
        machine_port: 8080,
        host_port,
    }
}

fn spec_with(
    exports: Vec<HostExportSpec>,
    machines: Vec<vz_runtime_contract::MachineSpec>,
) -> EnvironmentSpec {
    EnvironmentSpec {
        secret_bindings: Vec::new(),
        volumes: Vec::new(),
        schema_version: 1,
        default_machine: None,
        machines,
        networks: Vec::new(),
        endpoints: Vec::new(),
        host_exports: exports,
        host_imports: Vec::new(),
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

fn export_instance(
    name: &str,
    machine_id: &MachineId,
    environment_id: &EnvironmentId,
) -> HostExportInstance {
    HostExportInstance {
        schema_version: 1,
        export_id: HostExportId::generate(),
        environment_id: environment_id.clone(),
        machine_id: machine_id.clone(),
        name: name.to_string(),
    }
}

/// The positive this module exists to serve. Every denial below is only evidence
/// because this case resolves: an export of a Developer Linux Machine becomes
/// exactly one loopback mapping whose destination is the guest's own loopback
/// and never an address.
#[test]
fn a_declared_export_resolves_to_one_loopback_mapping_with_no_destination() {
    let environment_id = EnvironmentId::generate();
    let machine_id = MachineId::generate();
    let spec = spec_with(
        vec![export_spec("api", "machine-0", Some(18080))],
        vec![machine_spec("machine-0")],
    );
    refuse_unsupported_host_exports(&spec).expect("a Developer Linux export is admitted");
    let resolved = resolve_environment_host_exports(
        &spec,
        &[machine_instance("machine-0", &machine_id, &environment_id)],
        &[export_instance("api", &machine_id, &environment_id)],
    )
    .expect("the declared export joins its instance");
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].mapping.host, 18080);
    assert_eq!(resolved[0].mapping.container, 8080);
    assert_eq!(resolved[0].mapping.protocol, PortProtocol::Tcp);
    // The load-bearing assertion: no address ever crosses this boundary, so the
    // guest resolves the destination to its own loopback and an export cannot be
    // pointed at a third party.
    assert_eq!(resolved[0].mapping.target_service, None);
    let grouped = boot_port_mappings(&resolved);
    assert_eq!(grouped.len(), 1);
    assert_eq!(grouped[&machine_id].len(), 1);
}

/// Paired with the positive above: the same definition, one Machine changed, is
/// refused rather than booted with a listener that would never carry traffic.
#[test]
fn an_export_of_a_non_developer_linux_machine_is_refused() {
    let mut hardened = machine_spec("machine-0");
    hardened.profile = MachineProfile::Hardened;
    let error = refuse_unsupported_host_exports(&spec_with(
        vec![export_spec("api", "machine-0", Some(18080))],
        vec![hardened],
    ))
    .expect_err("a Hardened Machine carries no relay");
    assert!(matches!(error, HostExportError::UnsupportedMachine { .. }));

    let mut native = machine_spec("machine-0");
    native.target.os = OperatingSystem::Macos;
    let error = refuse_unsupported_host_exports(&spec_with(
        vec![export_spec("api", "machine-0", Some(18080))],
        vec![native],
    ))
    .expect_err("a native macOS Machine is booted with no ports at all");
    assert!(matches!(error, HostExportError::UnsupportedMachine { .. }));
}

#[test]
fn an_export_naming_an_undeclared_machine_is_refused() {
    let error = refuse_unsupported_host_exports(&spec_with(
        vec![export_spec("api", "machine-9", Some(18080))],
        vec![machine_spec("machine-0")],
    ))
    .expect_err("the named Machine is not declared");
    assert!(matches!(error, HostExportError::UnknownMachine { .. }));
}

/// The intra-Environment half of "loopback exports work without collisions".
/// Two declarations of one loopback port is a definition defect and is refused
/// at admission, before any Machine of the Environment has been started.
#[test]
fn two_exports_declaring_one_loopback_port_are_refused_before_any_effect() {
    let error = refuse_unsupported_host_exports(&spec_with(
        vec![
            export_spec("api", "machine-0", Some(18080)),
            export_spec("web", "machine-0", Some(18080)),
        ],
        vec![machine_spec("machine-0")],
    ))
    .expect_err("one loopback port carries at most one export");
    match error {
        HostExportError::DuplicateHostPort { port, .. } => assert_eq!(port, 18080),
        other => panic!("expected a duplicate host port, got {other:?}"),
    }
    // Paired positive: two exports on two DIFFERENT ports are admitted, so the
    // refusal above is about the collision and not about declaring two exports.
    refuse_unsupported_host_exports(&spec_with(
        vec![
            export_spec("api", "machine-0", Some(18080)),
            export_spec("web", "machine-0", Some(18081)),
        ],
        vec![machine_spec("machine-0")],
    ))
    .expect("two exports on distinct loopback ports are admitted");
}

#[test]
fn a_dynamically_allocated_export_port_is_refused_rather_than_silently_chosen() {
    let error = refuse_unsupported_host_exports(&spec_with(
        vec![export_spec("api", "machine-0", None)],
        vec![machine_spec("machine-0")],
    ))
    .expect_err("no surface reports a dynamically allocated port back");
    assert!(matches!(
        error,
        HostExportError::DynamicPortUnsupported { .. }
    ));
}

/// An instance with no declaration is an owned resource nothing reclaims; a
/// declaration with no instance is a boundary the caller asked for and did not
/// get. Both are refused rather than skipped.
#[test]
fn the_instance_and_declaration_sets_must_match_exactly() {
    let environment_id = EnvironmentId::generate();
    let machine_id = MachineId::generate();
    let machines = [machine_instance("machine-0", &machine_id, &environment_id)];

    let error = resolve_environment_host_exports(
        &spec_with(Vec::new(), vec![machine_spec("machine-0")]),
        &machines,
        &[export_instance("api", &machine_id, &environment_id)],
    )
    .expect_err("a persisted export with no declaration is unaccounted");
    assert!(matches!(error, HostExportError::UndeclaredInstance { .. }));

    let error = resolve_environment_host_exports(
        &spec_with(
            vec![export_spec("api", "machine-0", Some(18080))],
            vec![machine_spec("machine-0")],
        ),
        &machines,
        &[],
    )
    .expect_err("a declared export with no instance was never minted");
    assert!(matches!(error, HostExportError::MissingInstance { .. }));
}

/// The instance's Machine is the authority Delete accounts against, so a drift
/// between it and the declaration is refused rather than resolved to either side.
#[test]
fn an_instance_attributed_to_the_wrong_machine_is_refused() {
    let environment_id = EnvironmentId::generate();
    let first = MachineId::generate();
    let second = MachineId::generate();
    let spec = spec_with(
        vec![export_spec("api", "machine-0", Some(18080))],
        vec![machine_spec("machine-0"), machine_spec("machine-1")],
    );
    let machines = [
        machine_instance("machine-0", &first, &environment_id),
        machine_instance("machine-1", &second, &environment_id),
    ];
    let error = resolve_environment_host_exports(
        &spec,
        &machines,
        &[export_instance("api", &second, &environment_id)],
    )
    .expect_err("the instance names a Machine the declaration does not");
    assert!(matches!(error, HostExportError::UnknownMachine { .. }));

    // Paired positive: the same export attributed to the declared Machine resolves.
    resolve_environment_host_exports(
        &spec,
        &machines,
        &[export_instance("api", &first, &environment_id)],
    )
    .expect("the correctly attributed export resolves");
}

#[test]
fn an_instance_naming_a_machine_this_environment_does_not_hold_is_refused() {
    let environment_id = EnvironmentId::generate();
    let held = MachineId::generate();
    let absent = MachineId::generate();
    let error = resolve_environment_host_exports(
        &spec_with(
            vec![export_spec("api", "machine-0", Some(18080))],
            vec![machine_spec("machine-0")],
        ),
        &[machine_instance("machine-0", &held, &environment_id)],
        &[export_instance("api", &absent, &environment_id)],
    )
    .expect_err("the instance names an absent Machine");
    assert!(matches!(
        error,
        HostExportError::UnknownInstanceMachine { .. }
    ));
}

/// The cross-Environment half of "without collisions": a sibling Environment's
/// listener is invisible to this definition, so the static duplicate check
/// cannot see it and the probe is what fails the Up before any Machine boots.
#[tokio::test]
async fn a_port_another_listener_already_holds_fails_before_any_boot() {
    let machine_id = MachineId::generate();
    let squatter = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("a loopback squatter");
    let taken = squatter.local_addr().expect("bound address").port();
    let resolved = vec![ResolvedHostExport {
        name: "api".to_string(),
        machine_id: machine_id.clone(),
        mapping: PortMapping {
            host: taken,
            container: 8080,
            protocol: PortProtocol::Tcp,
            target_service: None,
        },
    }];
    let error = probe_exportable_host_ports(&resolved, &BTreeSet::new())
        .await
        .expect_err("the port is already held");
    match &error {
        HostExportError::HostPortUnavailable { port, export, .. } => {
            assert_eq!(*port, taken);
            assert_eq!(export, "api");
        }
        other => panic!("expected an unavailable host port, got {other:?}"),
    }
    // The refusal a caller reads is the error envelope, not this enum. It has
    // to name the contended port as a field: an agent that can only match on
    // the sentence cannot tell a port collision from any other Up refusal, and
    // the port it must free is the whole content of the failure.
    let details = error.details();
    assert_eq!(details.get("host_export").map(String::as_str), Some("api"));
    assert_eq!(
        details.get("host_port").map(String::as_str),
        Some(taken.to_string().as_str())
    );
    assert!(
        error.to_string().contains("already held on this host"),
        "{error}"
    );

    // Paired positive one: a Machine already booted holds its own listener, so
    // its port is not probed against itself on a re-Up.
    probe_exportable_host_ports(&resolved, &BTreeSet::from([machine_id.clone()]))
        .await
        .expect("a running Machine's own listener is not a collision");

    // Paired positive two: once the squatter is gone the identical probe passes,
    // so the refusal above was the held port and not the probe always failing.
    drop(squatter);
    probe_exportable_host_ports(&resolved, &BTreeSet::new())
        .await
        .expect("a free loopback port probes clean");
}

/// The criterion's "wrong protocol is denied" clause holds by construction
/// rather than by a check: `TransportProtocol` has exactly one variant, so a
/// non-TCP export cannot be declared. This pins that fact, so adding UDP breaks
/// here rather than silently reaching a TCP-only relay.
#[test]
fn host_export_protocol_is_tcp_only() {
    let all = [TransportProtocol::Tcp];
    assert_eq!(all.len(), 1);
    assert!(all.into_iter().all(protocol_is_relayable));
    assert_eq!(
        serde_json::to_value(TransportProtocol::Tcp).expect("serializable"),
        serde_json::json!("tcp")
    );
    assert!(serde_json::from_value::<TransportProtocol>(serde_json::json!("udp")).is_err());
}
