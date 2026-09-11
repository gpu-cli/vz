//! The redaction claim, measured rather than asserted.
//!
//! Criterion 18's last group sweeps seven artifact groups for the exact planted
//! bytes. The audit log is one of them, and it is the one the daemon writes
//! deliberately -- so its record is built by a pure function and searched here
//! for a value it was never given.

use super::supervisor::secret_audit_record;
use vz_runtime_contract::{
    EnvironmentId, EnvironmentInstance, EnvironmentState, MachineId, MachineInstance,
    MachineProfile, MachineState, ProjectId, RequestMetadata, SecretBindingId,
    SecretBindingInstance, TOPOLOGY_SCHEMA_VERSION,
};

/// A value no field of the record could legitimately contain.
const PLANTED: &str = "vz-secret-planted-8e41c0b2f7d94a16";

fn binding(source_env: Option<&str>, source_command: Option<Vec<String>>) -> SecretBindingInstance {
    SecretBindingInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        binding_id: SecretBindingId::new("sec_0123456789abcdef".to_string()).unwrap(),
        environment_id: EnvironmentId::new("env_0123456789abcdef".to_string()).unwrap(),
        machine_id: MachineId::new("mch_0123456789abcdef".to_string()).unwrap(),
        name: "gate-secret".to_string(),
        target_path: "/run/vz-secrets/gate-secret".to_string(),
        source_env: source_env.map(ToString::to_string),
        source_command,
    }
}

fn environment() -> EnvironmentInstance {
    EnvironmentInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        environment_id: EnvironmentId::new("env_0123456789abcdef".to_string()).unwrap(),
        project_id: ProjectId::new("prj_0123456789abcdef".to_string()).unwrap(),
        name: "default".to_string(),
        definition_digest: format!("sha256:{}", "0".repeat(64)),
        state: EnvironmentState::Ready,
        lifecycle_generation: 1,
        active_operation_id: None,
        bindings: Vec::new(),
        machines: Vec::new(),
        networks: Vec::new(),
        endpoints: Vec::new(),
        network_attachments: Vec::new(),
        host_exports: Vec::new(),
        host_imports: Vec::new(),
        egress: Vec::new(),
        volumes: Vec::new(),
        secret_bindings: Vec::new(),
        ownership: Vec::new(),
        legacy_migration: None,
        created_at: 1,
        updated_at: 1,
    }
}

fn machine() -> MachineInstance {
    MachineInstance {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        machine_id: MachineId::new("mch_0123456789abcdef".to_string()).unwrap(),
        environment_id: EnvironmentId::new("env_0123456789abcdef".to_string()).unwrap(),
        name: "machine-0".to_string(),
        profile: MachineProfile::Developer,
        target: vz_runtime_contract::TargetSpec {
            os: vz_runtime_contract::OperatingSystem::Linux,
            arch: vz_runtime_contract::Architecture::Aarch64,
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
        state: MachineState::Ready,
        legacy_sandbox_id: None,
        fork: None,
    }
}

fn metadata() -> RequestMetadata {
    RequestMetadata {
        request_id: Some("req-secret-audit".to_string()),
        idempotency_key: Some("key-secret-audit".to_string()),
        trace_id: None,
        passthrough: Default::default(),
    }
}

#[test]
fn an_audit_record_names_the_binding_and_never_its_value() {
    let record = secret_audit_record(
        &environment(),
        &machine(),
        &binding(Some("VZ_GATE_SECRET_VALUE"), None),
        &metadata(),
    );
    let encoded = serde_json::to_string(&record).unwrap();
    // The identity criterion 18 requires the record to carry.
    assert_eq!(record["event"], "secret_binding_used");
    // The exact field names a reader correlates on.
    assert_eq!(record["binding"], "gate-secret");
    assert_eq!(record["binding_id"], "sec_0123456789abcdef");
    for needle in [
        "env_0123456789abcdef",
        "mch_0123456789abcdef",
        "sec_0123456789abcdef",
        "gate-secret",
        "/run/vz-secrets/gate-secret",
    ] {
        assert!(
            encoded.contains(needle),
            "audit must name {needle}: {encoded}"
        );
    }
    // And the value it must not: the record is never handed one, so there is no
    // field it could arrive through.
    assert!(
        !encoded.contains(PLANTED),
        "audit record carried the value: {encoded}"
    );
}

#[test]
fn an_audit_record_names_the_exact_argv_a_command_source_runs() {
    // The mitigation promised for `source_command`: a definition that runs
    // something unexpected is visible afterwards, so the argv is recorded even
    // though the value it produced is not.
    let record = secret_audit_record(
        &environment(),
        &machine(),
        &binding(
            None,
            Some(vec![
                "op".to_string(),
                "read".to_string(),
                "op://vault/item/field".to_string(),
            ]),
        ),
        &metadata(),
    );
    let encoded = serde_json::to_string(&record).unwrap();
    assert!(encoded.contains("op://vault/item/field"), "{encoded}");
    assert!(!encoded.contains(PLANTED), "{encoded}");
}
