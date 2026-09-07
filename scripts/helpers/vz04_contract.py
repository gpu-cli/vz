"""Frozen gate inputs: load, schema-validate, digest, and derive the required
scenario inventory (22 `gate.*` IDs from the contract plus the 63 `docker.*`
IDs from `docker_compatibility_contract.REQUIRED_IDS`).

Draft state and unresolved pins are reported as findings (unmet requirements),
never silently accepted and never blockers to running the gate.
"""
from __future__ import annotations

from pathlib import Path

import docker_compatibility_contract as docker_contract
import vz04_schema as schema
from vz04_common import (CONFIG_FILES, DECISION_SIGNATURES_DIR, LANE_PHASES, REPO_ROOT, SCHEMAS_DIR, GateError,
                         canonical_digest, digest_file, files_digest, load_json, relative_components, require,
                         tree_digest)

CONTRACT_SCHEMA_BY_KEY = {
    "e2e_contract": "e2e-contract",
    "docker_contract": "docker-compatibility",
    "migration_barriers": "migration-barriers",
    "decisions": "decisions",
    "decision_authorities": "decision-authorities",
    "host_target_capabilities": "host-target-capabilities",
}


def load_contract(repo_root: Path = REPO_ROOT) -> dict:
    contract = load_json(repo_root / CONFIG_FILES["e2e_contract"])
    schema.require_valid("e2e-contract", contract, repo_root)
    return contract


def load_docker_contract(repo_root: Path = REPO_ROOT) -> dict:
    value = load_json(repo_root / CONFIG_FILES["docker_contract"])
    schema.require_valid("docker-compatibility", value, repo_root)
    return value


def lane_by_name(contract: dict) -> dict:
    lanes = {lane["name"]: lane for lane in contract["lanes"]}
    require(len(lanes) == len(contract["lanes"]), "duplicate lane names in contract")
    return lanes


def required_scenarios(contract: dict, docker: dict) -> list:
    """[{id, lane, phase}] in contract order; gate.* first, then docker.* sorted."""
    lanes = lane_by_name(contract)
    rows = []
    seen = set()
    for scenario in contract["scenarios"]:
        require(scenario["id"] not in seen, f"duplicate scenario id {scenario['id']}")
        require(scenario["phase"] in lanes[scenario["lane"]]["phases"],
                f"{scenario['id']} assigned to phase {scenario['phase']} its lane does not run")
        seen.add(scenario["id"])
        rows.append({"id": scenario["id"], "lane": scenario["lane"], "phase": scenario["phase"]})
    criteria = sorted(scenario["criterion"] for scenario in contract["scenarios"])
    require(criteria == list(range(1, 23)), "contract must map every acceptance criterion 1..22 exactly once")
    docker_lane = contract["docker_contract"]["lane"]
    docker_ids = {scenario["id"]: scenario for scenario in docker["scenarios"]}
    require(set(docker_ids) == set(docker_contract.REQUIRED_IDS) and len(docker["scenarios"]) == len(docker_contract.REQUIRED_IDS),
            "docker contract scenarios differ from docker_compatibility_contract.REQUIRED_IDS")
    for identifier in sorted(docker_contract.REQUIRED_IDS):
        require(identifier not in seen, f"docker id collides with gate id {identifier}")
        seen.add(identifier)
        phase = docker_ids[identifier]["phase"]
        if phase == "persisted-recovery":
            phase = "persisted-recovery/pre-sleep"
        require(phase in LANE_PHASES and phase in lanes[docker_lane]["phases"], f"docker scenario {identifier} phase {phase} unknown")
        rows.append({"id": identifier, "lane": docker_lane, "phase": phase})
    return rows


def _input_entry(repo_root: Path, relative: str, schema_name, findings: list, label: str) -> dict:
    relative_components(relative)
    path = repo_root / relative
    if not path.is_file() or path.is_symlink():
        findings.append(("input.missing", label, f"{relative} is absent"))
        return {"path": relative, "sha256": None, "present": False}
    try:
        value = load_json(path)
        digest = digest_file(path)
    except GateError as error:
        findings.append(("input.unreadable", label, str(error)))
        return {"path": relative, "sha256": None, "present": True}
    if schema_name is not None:
        if schema.schema_available(schema_name, repo_root):
            for problem in schema.validate(schema_name, value, repo_root):
                findings.append(("input.schema", label, problem))
        else:
            findings.append(("input.schema_missing", label, f"schema {schema_name} is absent"))
        state = value.get("contract_state") if isinstance(value, dict) else None
        if state is not None and state != "inputs_frozen":
            findings.append(("input.draft", label, f"{relative} contract_state is {state}, not inputs_frozen"))
    return {"path": relative, "sha256": digest, "present": True}


def frozen_inputs(contract: dict, repo_root: Path = REPO_ROOT) -> dict:
    """Digest every frozen input. Returns {inputs, digests, findings}.

    `findings` are (code, subject, detail) triples describing unmet
    requirements (draft state, null pins, absent optional inputs).
    """
    findings = []
    inputs = {}
    for key, relative in CONFIG_FILES.items():
        inputs[key] = _input_entry(repo_root, relative, CONTRACT_SCHEMA_BY_KEY[key], findings, key)
    inputs["ipsw_pin"] = _input_entry(repo_root, contract["native_macos"]["ipsw_pin"], None, findings, "ipsw_pin")
    inputs["buildkit_pin"] = _input_entry(repo_root, contract["pins"]["buildkit"], None, findings, "buildkit_pin")
    inputs["cli_removal"] = _input_entry(repo_root, contract["pins"]["cli_removal"], None, findings, "cli_removal")

    signatures = repo_root / DECISION_SIGNATURES_DIR
    if signatures.is_dir() and not signatures.is_symlink():
        signatures_tree_sha256 = tree_digest(signatures, allow_empty=True)
    else:
        signatures_tree_sha256 = None
        findings.append(("input.missing", "decision_signatures", f"{DECISION_SIGNATURES_DIR} is absent"))

    schemas_tree_sha256 = tree_digest(repo_root / SCHEMAS_DIR)

    fixture_digests = {}
    for relative in contract["fixtures"]["required_dirs"]:
        relative_components(relative)
        directory = repo_root / relative
        if directory.is_dir() and not directory.is_symlink():
            try:
                fixture_digests[relative] = tree_digest(directory)
            except GateError as error:
                fixture_digests[relative] = None
                findings.append(("fixture.unreadable", relative, str(error)))
        else:
            fixture_digests[relative] = None
            findings.append(("fixture.missing", relative, "required fixture directory is absent"))
    fixtures_tree_sha256 = canonical_digest(sorted(fixture_digests.items()))

    harness_files = {}
    for relative in contract["harness"]["files"]:
        relative_components(relative)
        path = repo_root / relative
        if path.is_file() and not path.is_symlink():
            harness_files[relative] = digest_file(path)
        else:
            harness_files[relative] = None
            findings.append(("harness.missing", relative, "declared harness file is absent"))
    harness_tree_sha256 = files_digest(repo_root, [k for k, v in harness_files.items() if v is not None])["sha256"] \
        if any(v is not None for v in harness_files.values()) else None

    for null_field, label in ((contract["native_macos"]["guest_agent_sha256"], "native_macos.guest_agent_sha256"),
                              (contract["native_macos"]["prepared_image_sha256"], "native_macos.prepared_image_sha256"),
                              (contract["native_macos"]["xcode_toolchain_sha256"], "native_macos.xcode_toolchain_sha256"),
                              (contract["native_macos"]["setup_recipe"], "native_macos.setup_recipe"),
                              (contract["migration"]["legacy_artifact_sha256"], "migration.legacy_artifact_sha256"),
                              (contract["migration"]["legacy_state_fixture"], "migration.legacy_state_fixture"),
                              (contract["host_envelope"]["minimum_mac_model"], "host_envelope.minimum_mac_model"),
                              (contract["host_envelope"]["minimum_free_memory_bytes"], "host_envelope.minimum_free_memory_bytes"),
                              (contract["host_envelope"]["minimum_free_disk_bytes"], "host_envelope.minimum_free_disk_bytes"),
                              (contract["host_envelope"]["docker_client_ranges"], "host_envelope.docker_client_ranges")):
        if null_field is None:
            findings.append(("contract.unpinned", label, "contract value is null; requirement unmet under draft_unverified"))
    if not contract["native_macos"]["privilege_steps"] or not contract["native_macos"]["license_steps"]:
        findings.append(("contract.unpinned", "native_macos.privilege_steps/license_steps",
                         "explicit privilege and license steps are not recorded"))

    return {
        "inputs": inputs,
        "digests": {
            "schemas_tree_sha256": schemas_tree_sha256,
            "fixtures_tree_sha256": fixtures_tree_sha256,
            "fixture_dirs": fixture_digests,
            "harness_tree_sha256": harness_tree_sha256,
            "harness_files": harness_files,
            "signatures_tree_sha256": signatures_tree_sha256,
        },
        "findings": findings,
    }
