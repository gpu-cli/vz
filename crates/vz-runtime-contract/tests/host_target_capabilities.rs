//! Checks that `config/host-target-capabilities-v0.4.json` agrees with the
//! typed contract: its vocabularies are exactly the serde wire names of the
//! Rust enums, every pair labels every Machine capability exactly once, and
//! the ACTIVE/DEV/PLANNED/NA evidence rules hold.
#![allow(clippy::expect_used)]

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;
use vz_runtime_contract::{MachineBackend, MachineCapability, MachineProfile};

const MATRIX: &str = include_str!("../../../config/host-target-capabilities-v0.4.json");
const SCHEMA: &str = include_str!("../../../schemas/host-target-capabilities-v0.4.schema.json");

const STATUSES: [&str; 4] = ["ACTIVE", "DEV", "PLANNED", "NA"];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Matrix {
    #[serde(rename = "$schema")]
    schema: String,
    schema_version: u32,
    target_release: String,
    contract_state: String,
    normative_source: String,
    source_head: String,
    status_definitions: BTreeMap<String, String>,
    unlisted_capability_status: String,
    vocabularies: Vocabularies,
    hosts: BTreeMap<String, Host>,
    targets: BTreeMap<String, Value>,
    evidence_notes: Vec<String>,
    generated_surfaces: Vec<String>,
    pairs: Vec<Pair>,
}

#[derive(Deserialize)]
struct Vocabularies {
    machine_capabilities: Vec<String>,
    topology_capabilities: Vec<String>,
    profiles: AliasTable,
    backends: AliasTable,
}

#[derive(Deserialize)]
struct AliasTable {
    wire_names: Vec<String>,
    aliases: BTreeMap<String, String>,
    alias_sources: Vec<String>,
}

#[derive(Deserialize)]
struct Host {
    status: String,
    #[serde(default)]
    rejected_by: Vec<String>,
    #[serde(default)]
    minimum_os: Option<String>,
}

#[derive(Deserialize)]
struct Pair {
    host: String,
    target: String,
    profile: String,
    pair_status: String,
    backend: Option<String>,
    negotiated_by: Vec<String>,
    rejected_by: Vec<String>,
    // Duplicate JSON keys are detected by scripts/helpers/installed_capability_matrix.py
    // (object_pairs_hook); here a key set equal to the vocabulary proves each is listed once.
    machine_capabilities: BTreeMap<String, CapabilityEntry>,
    topology_capabilities: BTreeMap<String, CapabilityEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityEntry {
    status: String,
    negotiated_by: Vec<String>,
    rejected_by: Vec<String>,
    evidence: Vec<Evidence>,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Evidence {
    lane: String,
    suite: String,
    run_id: String,
    outcome: String,
    result_path: String,
    result_sha256: String,
    checksums_sha256: Option<String>,
    release_certified: bool,
    #[serde(default)]
    source_head: Option<String>,
    #[serde(default)]
    checks_cited: Option<Vec<String>>,
}

/// Exhaustive by construction: adding a `MachineCapability` variant fails to
/// compile here until the matrix vocabulary is revisited.
fn every_machine_capability() -> Vec<MachineCapability> {
    use MachineCapability::*;
    let all = [
        PosixExec,
        PosixPty,
        Signals,
        Files,
        Ports,
        DockerEngine,
        Compose,
        Buildx,
        Snapshot,
        Suspend,
        Checkpoint,
        Gui,
        WindowsConsole,
    ];
    for capability in all {
        match capability {
            PosixExec | PosixPty | Signals | Files | Ports | DockerEngine | Compose | Buildx
            | Snapshot | Suspend | Checkpoint | Gui | WindowsConsole => {}
        }
    }
    all.to_vec()
}

fn every_profile() -> Vec<MachineProfile> {
    let all = [MachineProfile::Developer, MachineProfile::Hardened];
    for profile in all {
        match profile {
            MachineProfile::Developer | MachineProfile::Hardened => {}
        }
    }
    all.to_vec()
}

/// Named backends only; `Other(String)` is an escape hatch without a wire name.
fn every_named_backend() -> Vec<MachineBackend> {
    let all = [
        MachineBackend::MacosVirtualizationLinux,
        MachineBackend::MacosNative,
        MachineBackend::LinuxNative,
        MachineBackend::WindowsLinux,
        MachineBackend::WindowsNative,
    ];
    for backend in &all {
        match backend {
            MachineBackend::MacosVirtualizationLinux
            | MachineBackend::MacosNative
            | MachineBackend::LinuxNative
            | MachineBackend::WindowsLinux
            | MachineBackend::WindowsNative
            | MachineBackend::Other(_) => {}
        }
    }
    all.to_vec()
}

fn wire_name<T: serde::Serialize>(value: &T) -> String {
    match serde_json::to_value(value).expect("enum serializes") {
        Value::String(name) => name,
        other => panic!("expected a bare string wire name, got {other}"),
    }
}

fn matrix() -> Matrix {
    serde_json::from_str(MATRIX).expect("matrix parses into the typed shape")
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn is_source_ref(value: &str) -> bool {
    let Some((path, lines)) = value.rsplit_once(':') else {
        return false;
    };
    let path_ok = !path.is_empty()
        && !path.starts_with('/')
        && path.contains('.')
        && path
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_./-".contains(&b));
    let lines_ok = match lines.split_once('-') {
        Some((start, end)) => {
            let start: u64 = start.parse().unwrap_or(0);
            let end: u64 = end.parse().unwrap_or(0);
            start > 0 && end >= start
        }
        None => lines.parse::<u64>().map(|n| n > 0).unwrap_or(false),
    };
    path_ok && lines_ok
}

fn check_entry(context: &str, entry: &CapabilityEntry) {
    assert!(
        STATUSES.contains(&entry.status.as_str()),
        "{context}: unknown status {}",
        entry.status
    );
    for reference in entry.negotiated_by.iter().chain(&entry.rejected_by) {
        assert!(
            is_source_ref(reference),
            "{context}: bad source ref {reference}"
        );
    }
    match entry.status.as_str() {
        "ACTIVE" | "DEV" => {
            assert!(
                !entry.evidence.is_empty(),
                "{context}: {} requires evidence",
                entry.status
            );
            assert!(
                !entry.negotiated_by.is_empty(),
                "{context}: {} requires negotiated_by",
                entry.status
            );
        }
        "NA" => assert!(
            !entry.rejected_by.is_empty(),
            "{context}: NA requires rejected_by"
        ),
        _ => {
            assert!(
                entry.negotiated_by.is_empty(),
                "{context}: PLANNED must not negotiate"
            );
            assert!(
                entry.rejected_by.is_empty(),
                "{context}: PLANNED must not cite rejections"
            );
            assert!(
                entry.evidence.is_empty(),
                "{context}: PLANNED must not carry evidence"
            );
        }
    }
    if let Some(note) = &entry.note {
        assert!(!note.trim().is_empty(), "{context}: empty note");
    }
    for evidence in &entry.evidence {
        let run = format!("{context} evidence {}", evidence.run_id);
        assert!(
            !evidence.lane.is_empty() && !evidence.suite.is_empty(),
            "{run}: lane/suite"
        );
        assert!(!evidence.run_id.is_empty(), "{context}: empty run_id");
        assert!(
            evidence.outcome.starts_with("passed"),
            "{run}: outcome {}",
            evidence.outcome
        );
        assert!(
            evidence.result_path.ends_with(".json"),
            "{run}: result_path"
        );
        assert!(is_sha256(&evidence.result_sha256), "{run}: result_sha256");
        if let Some(checksums) = &evidence.checksums_sha256 {
            assert!(is_sha256(checksums), "{run}: checksums_sha256");
        }
        assert!(
            !evidence.release_certified,
            "{run}: nothing is release certified yet"
        );
        if let Some(head) = &evidence.source_head {
            assert!(
                head.len() >= 7 && head.bytes().all(|b| b.is_ascii_hexdigit()),
                "{run}: head"
            );
        }
        if let Some(checks) = &evidence.checks_cited {
            assert!(
                !checks.is_empty(),
                "{run}: checks_cited must not be empty when present"
            );
        }
    }
}

#[test]
fn matrix_parses_and_header_is_frozen() {
    let matrix = matrix();
    assert_eq!(
        matrix.schema,
        "../schemas/host-target-capabilities-v0.4.schema.json"
    );
    assert_eq!(matrix.schema_version, 1);
    assert_eq!(matrix.target_release, "0.4.0");
    assert!(matches!(
        matrix.contract_state.as_str(),
        "draft_unverified" | "inputs_frozen"
    ));
    assert!(
        matrix
            .normative_source
            .starts_with("planning/developer-environments/GOAL-0.4.0.md#")
    );
    assert!(
        matrix.source_head.len() >= 7 && matrix.source_head.bytes().all(|b| b.is_ascii_hexdigit())
    );
    assert_eq!(matrix.unlisted_capability_status, "PLANNED");
    assert_eq!(
        matrix
            .status_definitions
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        STATUSES.into_iter().collect::<BTreeSet<_>>()
    );
    assert!(!matrix.evidence_notes.is_empty());
    assert!(!matrix.targets.is_empty());
    let schema: Value = serde_json::from_str(SCHEMA).expect("schema parses");
    assert_eq!(
        schema["$schema"],
        "https://json-schema.org/draft/2020-12/schema"
    );
}

#[test]
fn machine_capability_vocabulary_matches_enum_wire_names() {
    let matrix = matrix();
    let expected: BTreeSet<String> = every_machine_capability().iter().map(wire_name).collect();
    let listed: Vec<&str> = matrix
        .vocabularies
        .machine_capabilities
        .iter()
        .map(String::as_str)
        .collect();
    let listed_set: BTreeSet<String> = listed.iter().map(|s| (*s).to_string()).collect();
    assert_eq!(
        listed.len(),
        listed_set.len(),
        "duplicate machine capability in vocabulary"
    );
    assert_eq!(
        listed_set, expected,
        "vocabulary must equal MachineCapability wire names"
    );
    let schema: Value = serde_json::from_str(SCHEMA).expect("schema parses");
    let schema_enum: BTreeSet<String> = schema["$defs"]["machine_capability"]["enum"]
        .as_array()
        .expect("schema machine_capability enum")
        .iter()
        .map(|v| v.as_str().expect("string").to_string())
        .collect();
    assert_eq!(
        schema_enum, expected,
        "schema enum must equal MachineCapability wire names"
    );
}

#[test]
fn topology_vocabulary_is_unique_and_snake_case() {
    let matrix = matrix();
    let names = &matrix.vocabularies.topology_capabilities;
    let unique: BTreeSet<&String> = names.iter().collect();
    assert_eq!(names.len(), unique.len(), "duplicate topology capability");
    for name in names {
        assert!(
            name.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
            "topology capability {name} is not snake_case"
        );
    }
}

#[test]
fn profile_and_backend_alias_tables_cover_enum_wire_names() {
    let matrix = matrix();
    let profiles = &matrix.vocabularies.profiles;
    let expected: BTreeSet<String> = every_profile().iter().map(wire_name).collect();
    assert_eq!(
        profiles.wire_names.iter().cloned().collect::<BTreeSet<_>>(),
        expected
    );
    for (alias, target) in &profiles.aliases {
        assert!(
            expected.contains(target),
            "profile alias {alias} -> unknown {target}"
        );
        assert!(
            !expected.contains(alias),
            "profile alias {alias} shadows a wire name"
        );
    }
    assert_eq!(
        profiles.aliases.get("container").map(String::as_str),
        Some("hardened")
    );
    assert!(!profiles.alias_sources.is_empty());
    for reference in &profiles.alias_sources {
        assert!(
            is_source_ref(reference),
            "bad profile alias source {reference}"
        );
    }

    let backends = &matrix.vocabularies.backends;
    let expected: BTreeSet<String> = every_named_backend().iter().map(wire_name).collect();
    assert_eq!(
        backends.wire_names.iter().cloned().collect::<BTreeSet<_>>(),
        expected
    );
    for (alias, target) in &backends.aliases {
        assert!(
            expected.contains(target),
            "backend alias {alias} -> unknown {target}"
        );
        assert!(
            !expected.contains(alias),
            "backend alias {alias} shadows a wire name"
        );
    }
    assert_eq!(
        backends.aliases.get("macos-vz").map(String::as_str),
        Some("macos_virtualization_linux"),
        "daemon backend_name must map onto the Machine backend wire name"
    );
    assert!(!backends.alias_sources.is_empty());
    for reference in &backends.alias_sources {
        assert!(
            is_source_ref(reference),
            "bad backend alias source {reference}"
        );
    }
}

#[test]
fn every_pair_lists_every_machine_capability_exactly_once() {
    let matrix = matrix();
    let expected: BTreeSet<String> = every_machine_capability().iter().map(wire_name).collect();
    let topology: BTreeSet<&String> = matrix.vocabularies.topology_capabilities.iter().collect();
    assert!(!matrix.pairs.is_empty());
    let mut seen_pairs = BTreeSet::new();
    for pair in &matrix.pairs {
        let context = format!("{}/{}/{}", pair.host, pair.target, pair.profile);
        assert!(
            seen_pairs.insert((pair.host.clone(), pair.target.clone(), pair.profile.clone())),
            "{context}: duplicate pair"
        );
        let listed: BTreeSet<String> = pair.machine_capabilities.keys().cloned().collect();
        assert_eq!(
            listed, expected,
            "{context}: machine capabilities must be listed exactly once each"
        );
        let listed_topology: BTreeSet<&String> = pair.topology_capabilities.keys().collect();
        assert_eq!(
            listed_topology, topology,
            "{context}: topology capabilities must match the vocabulary"
        );
    }
}

#[test]
fn pair_statuses_and_evidence_rules_hold() {
    let matrix = matrix();
    let profiles: BTreeSet<String> = matrix
        .vocabularies
        .profiles
        .wire_names
        .iter()
        .cloned()
        .collect();
    let backends: BTreeSet<String> = matrix
        .vocabularies
        .backends
        .wire_names
        .iter()
        .cloned()
        .collect();
    let targets: BTreeSet<&String> = matrix.targets.keys().collect();
    for pair in &matrix.pairs {
        let context = format!("{}/{}/{}", pair.host, pair.target, pair.profile);
        assert!(
            matrix.hosts.contains_key(&pair.host),
            "{context}: unknown host"
        );
        assert!(targets.contains(&pair.target), "{context}: unknown target");
        assert!(
            profiles.contains(&pair.profile),
            "{context}: unknown profile"
        );
        assert!(
            STATUSES.contains(&pair.pair_status.as_str()),
            "{context}: bad pair_status"
        );
        if let Some(backend) = &pair.backend {
            assert!(
                backends.contains(backend),
                "{context}: unknown backend {backend}"
            );
        }
        for reference in pair.negotiated_by.iter().chain(&pair.rejected_by) {
            assert!(
                is_source_ref(reference),
                "{context}: bad pair source ref {reference}"
            );
        }
        match pair.pair_status.as_str() {
            "NA" => {
                assert!(
                    !pair.rejected_by.is_empty(),
                    "{context}: NA pair requires rejected_by"
                );
                for (name, entry) in pair
                    .machine_capabilities
                    .iter()
                    .chain(&pair.topology_capabilities)
                {
                    assert_eq!(
                        entry.status, "NA",
                        "{context}/{name}: NA pair must label every capability NA"
                    );
                }
            }
            "ACTIVE" | "DEV" => {
                assert!(
                    !pair.negotiated_by.is_empty(),
                    "{context}: live pair requires negotiated_by"
                );
                assert!(
                    pair.backend.is_some(),
                    "{context}: live pair requires a backend"
                );
                assert!(
                    pair.machine_capabilities["posix_exec"].status == pair.pair_status,
                    "{context}: posix_exec must carry the pair status"
                );
            }
            _ => {
                for (name, entry) in pair
                    .machine_capabilities
                    .iter()
                    .chain(&pair.topology_capabilities)
                {
                    assert_eq!(
                        entry.status, "PLANNED",
                        "{context}/{name}: PLANNED pair must label every capability PLANNED"
                    );
                }
            }
        }
        for (name, entry) in pair
            .machine_capabilities
            .iter()
            .chain(&pair.topology_capabilities)
        {
            check_entry(&format!("{context}/{name}"), entry);
            if pair.pair_status != "NA" {
                assert!(
                    entry.status != "ACTIVE",
                    "{context}/{name}: nothing is ACTIVE before a published 0.4 release"
                );
            }
        }
    }
}

#[test]
fn hosts_carry_baseline_and_rejections() {
    let matrix = matrix();
    let macos = matrix.hosts.get("macos-arm64").expect("macos-arm64 host");
    assert_eq!(macos.minimum_os.as_deref(), Some("14.0"));
    assert_ne!(macos.status, "NA");
    for (name, host) in &matrix.hosts {
        assert!(
            STATUSES.contains(&host.status.as_str()),
            "{name}: bad host status"
        );
        if host.status == "NA" {
            assert!(
                !host.rejected_by.is_empty(),
                "{name}: NA host requires rejected_by"
            );
        }
        for reference in &host.rejected_by {
            assert!(
                is_source_ref(reference),
                "{name}: bad host source ref {reference}"
            );
        }
    }
    let intel = matrix.hosts.get("macos-x86_64").expect("macos-x86_64 host");
    assert_eq!(intel.status, "NA");
    for surface in &matrix.generated_surfaces {
        assert!(
            !surface.is_empty() && !surface.starts_with('/'),
            "bad surface {surface}"
        );
    }
}
