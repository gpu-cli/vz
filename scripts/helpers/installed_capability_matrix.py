"""Validate config/host-target-capabilities-v0.4.json without jsonschema.

Hand-written structural and status/evidence rules for the host x Machine-target
x profile capability matrix. Evidence file digests are verified when the cited
file exists locally; a missing file is reported as "unverifiable locally" and is
never a failure, because .artifacts/ is gitignored. Nothing here is release
evidence: passing means the matrix is well-formed and internally consistent.

  python3 -B scripts/check-host-target-capabilities.py
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[2]
MATRIX = "config/host-target-capabilities-v0.4.json"
SCHEMA = "schemas/host-target-capabilities-v0.4.schema.json"
MAX_JSON = 16 * 1024 * 1024

STATUSES = ("ACTIVE", "DEV", "PLANNED", "NA")
LIVE = ("ACTIVE", "DEV")
OSES = ("linux", "macos", "windows")
ARCHES = ("aarch64", "x86_64")
PROFILES = ("developer", "hardened")
BACKENDS = ("macos_virtualization_linux", "macos_native", "linux_native", "windows_linux", "windows_native")
MACHINE_CAPABILITIES = (
    "posix_exec", "posix_pty", "signals", "files", "ports", "docker_engine", "compose", "buildx",
    "snapshot", "suspend", "checkpoint", "gui", "windows_console",
)
TOP_LEVEL = (
    "$schema", "schema_version", "target_release", "contract_state", "normative_source", "source_head",
    "status_definitions", "unlisted_capability_status", "vocabularies", "hosts", "targets",
    "evidence_notes", "generated_surfaces", "pairs",
)
ENTRY_KEYS = ("status", "negotiated_by", "rejected_by", "evidence")
ENTRY_OPTIONAL = ("note",)
EVIDENCE_KEYS = ("lane", "suite", "run_id", "outcome", "result_path", "result_sha256",
                 "checksums_sha256", "release_certified")
EVIDENCE_OPTIONAL = ("source_head", "checks_cited")
PAIR_KEYS = ("host", "target", "profile", "pair_status", "backend", "note", "negotiated_by",
             "rejected_by", "machine_capabilities", "topology_capabilities")
HOST_KEYS = ("os", "arch", "status")
HOST_OPTIONAL = ("minimum_os", "minimum_os_source", "rejected_by", "note")

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
GIT_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")
SNAKE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
HOST_ID_RE = re.compile(r"^(linux|macos|windows)-(arm64|x86_64)$")
SOURCE_REF_RE = re.compile(r"^[A-Za-z0-9_./-]+\.(rs|py|md|json|sh|toml):([0-9]+)(?:-([0-9]+))?$")
SURFACE_RE = re.compile(r"^[A-Za-z0-9_./-]+$")
VERSION_RE = re.compile(r"^[0-9]+(\.[0-9]+){1,2}$")
HOST_ARCH = {"arm64": "aarch64", "x86_64": "x86_64"}


class InvalidMatrix(Exception):
    """Raised when the matrix violates a structural or status rule."""


class DuplicateKey(ValueError):
    pass


def _unique_object(pairs: Sequence[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKey(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def load_json(root: Path, relative: str) -> Any:
    path = root / relative
    if not path.is_file():
        raise InvalidMatrix(f"{relative}: missing regular file")
    if path.stat().st_size > MAX_JSON:
        raise InvalidMatrix(f"{relative}: exceeds {MAX_JSON} bytes")
    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    except (DuplicateKey, ValueError) as error:
        raise InvalidMatrix(f"{relative}: {error}") from error


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Checker:
    def __init__(self) -> None:
        self.violations: List[str] = []

    def fail(self, message: str) -> None:
        self.violations.append(message)

    def require(self, condition: bool, message: str) -> bool:
        if not condition:
            self.fail(message)
        return condition

    def keys(self, value: Any, context: str, required: Sequence[str], optional: Sequence[str] = ()) -> bool:
        if not self.require(isinstance(value, dict), f"{context}: expected object"):
            return False
        present = set(value)
        for key in required:
            self.require(key in present, f"{context}: missing {key}")
        unknown = sorted(present - set(required) - set(optional))
        self.require(not unknown, f"{context}: unknown keys {unknown}")
        return True

    def string_list(self, value: Any, context: str, pattern: Optional[re.Pattern] = None,
                    unique: bool = True) -> List[str]:
        if not self.require(isinstance(value, list), f"{context}: expected array"):
            return []
        items: List[str] = []
        for index, item in enumerate(value):
            if not self.require(isinstance(item, str) and item, f"{context}[{index}]: expected non-empty string"):
                continue
            if pattern is not None:
                self.require(bool(pattern.match(item)), f"{context}[{index}]: {item!r} does not match {pattern.pattern}")
            items.append(item)
        if unique:
            self.require(len(items) == len(set(items)), f"{context}: duplicate entries")
        return items

    def source_refs(self, value: Any, context: str) -> List[str]:
        refs = self.string_list(value, context, SOURCE_REF_RE)
        for ref in refs:
            match = SOURCE_REF_RE.match(ref)
            if match and match.group(3) is not None:
                self.require(int(match.group(3)) >= int(match.group(2)) > 0, f"{context}: inverted line range {ref}")
        return refs


def check_evidence(checker: Checker, evidence: Any, context: str) -> None:
    if not checker.keys(evidence, context, EVIDENCE_KEYS, EVIDENCE_OPTIONAL):
        return
    for key in ("lane", "suite", "run_id", "outcome", "result_path"):
        checker.require(isinstance(evidence.get(key), str) and bool(evidence.get(key)),
                        f"{context}: {key} must be a non-empty string")
    if isinstance(evidence.get("outcome"), str):
        checker.require(evidence["outcome"].startswith("passed"), f"{context}: outcome must start with 'passed'")
    if isinstance(evidence.get("result_path"), str):
        checker.require(evidence["result_path"].endswith(".json") and not evidence["result_path"].startswith("/"),
                        f"{context}: result_path must be a repository-relative .json path")
    checker.require(isinstance(evidence.get("result_sha256"), str) and bool(SHA256_RE.match(evidence["result_sha256"])),
                    f"{context}: result_sha256 must be 64 lowercase hex characters")
    checksums = evidence.get("checksums_sha256")
    checker.require(checksums is None or (isinstance(checksums, str) and bool(SHA256_RE.match(checksums))),
                    f"{context}: checksums_sha256 must be null or 64 lowercase hex characters")
    checker.require(evidence.get("release_certified") is False, f"{context}: release_certified must be false")
    if "source_head" in evidence:
        checker.require(isinstance(evidence["source_head"], str) and bool(GIT_SHA_RE.match(evidence["source_head"])),
                        f"{context}: source_head must be a git sha")
    if "checks_cited" in evidence:
        checks = checker.string_list(evidence["checks_cited"], f"{context}.checks_cited", unique=False)
        checker.require(bool(checks), f"{context}: checks_cited must not be empty")


def check_entry(checker: Checker, entry: Any, context: str) -> Optional[str]:
    if not checker.keys(entry, context, ENTRY_KEYS, ENTRY_OPTIONAL):
        return None
    status = entry.get("status")
    if not checker.require(status in STATUSES, f"{context}: status {status!r} not in {STATUSES}"):
        return None
    negotiated = checker.source_refs(entry.get("negotiated_by"), f"{context}.negotiated_by")
    rejected = checker.source_refs(entry.get("rejected_by"), f"{context}.rejected_by")
    evidence = entry.get("evidence")
    if checker.require(isinstance(evidence, list), f"{context}: evidence must be an array"):
        for index, item in enumerate(evidence):
            check_evidence(checker, item, f"{context}.evidence[{index}]")
    else:
        evidence = []
    if "note" in entry:
        checker.require(isinstance(entry["note"], str) and bool(entry["note"].strip()), f"{context}: note must be non-empty")
    if status in LIVE:
        checker.require(bool(evidence), f"{context}: {status} requires non-empty evidence")
        checker.require(bool(negotiated), f"{context}: {status} requires non-empty negotiated_by")
    elif status == "NA":
        checker.require(bool(rejected), f"{context}: NA requires non-empty rejected_by")
    else:
        checker.require(not negotiated and not rejected and not evidence,
                        f"{context}: PLANNED requires empty negotiated_by, rejected_by and evidence")
    return status


def check_alias_table(checker: Checker, table: Any, context: str, wire_names: Sequence[str],
                      alias_pattern: re.Pattern, extra_optional: Sequence[str] = ()) -> None:
    if not checker.keys(table, context, ("wire_names", "aliases", "alias_sources"), extra_optional):
        return
    listed = checker.string_list(table.get("wire_names"), f"{context}.wire_names")
    checker.require(set(listed) == set(wire_names),
                    f"{context}.wire_names must equal the enum wire names {sorted(wire_names)}; got {sorted(listed)}")
    aliases = table.get("aliases")
    if checker.require(isinstance(aliases, dict), f"{context}.aliases: expected object"):
        for alias, target in aliases.items():
            checker.require(bool(alias_pattern.match(alias)), f"{context}.aliases: bad alias name {alias!r}")
            checker.require(alias not in wire_names, f"{context}.aliases: {alias!r} shadows a wire name")
            checker.require(target in wire_names, f"{context}.aliases: {alias!r} -> unknown wire name {target!r}")
    sources = checker.source_refs(table.get("alias_sources"), f"{context}.alias_sources")
    checker.require(bool(sources), f"{context}.alias_sources must not be empty")


def check_hosts(checker: Checker, hosts: Any) -> Dict[str, Dict[str, Any]]:
    if not checker.require(isinstance(hosts, dict) and hosts, "hosts: expected non-empty object"):
        return {}
    for name, host in hosts.items():
        context = f"hosts.{name}"
        match = HOST_ID_RE.match(name)
        if not checker.require(bool(match), f"{context}: host id must match {HOST_ID_RE.pattern}"):
            continue
        if not checker.keys(host, context, HOST_KEYS, HOST_OPTIONAL):
            continue
        checker.require(host.get("os") == match.group(1), f"{context}: os must equal the host id prefix")
        checker.require(host.get("arch") == HOST_ARCH[match.group(2)], f"{context}: arch must match the host id suffix")
        checker.require(host.get("status") in STATUSES, f"{context}: bad status")
        rejected = checker.source_refs(host.get("rejected_by", []), f"{context}.rejected_by")
        if host.get("status") == "NA":
            checker.require(bool(rejected), f"{context}: NA host requires rejected_by")
        if "minimum_os" in host:
            checker.require(isinstance(host["minimum_os"], str) and bool(VERSION_RE.match(host["minimum_os"])),
                            f"{context}: minimum_os must be a dotted version")
        if "minimum_os_source" in host:
            checker.source_refs([host["minimum_os_source"]], f"{context}.minimum_os_source")
    return hosts


def check_pair(checker: Checker, pair: Any, index: int, hosts: Dict[str, Any], targets: Sequence[str],
               topology: Sequence[str]) -> Optional[Tuple[str, str, str]]:
    context = f"pairs[{index}]"
    if not checker.keys(pair, context, PAIR_KEYS):
        return None
    host, target, profile = pair.get("host"), pair.get("target"), pair.get("profile")
    context = f"pairs[{index}] {host}/{target}/{profile}"
    checker.require(host in hosts, f"{context}: unknown host")
    checker.require(target in targets, f"{context}: unknown target")
    checker.require(profile in PROFILES, f"{context}: unknown profile")
    pair_status = pair.get("pair_status")
    checker.require(pair_status in STATUSES, f"{context}: bad pair_status")
    backend = pair.get("backend")
    checker.require(backend is None or backend in BACKENDS, f"{context}: unknown backend {backend!r}")
    checker.require(isinstance(pair.get("note"), str) and bool(pair["note"].strip()), f"{context}: note must be non-empty")
    negotiated = checker.source_refs(pair.get("negotiated_by"), f"{context}.negotiated_by")
    rejected = checker.source_refs(pair.get("rejected_by"), f"{context}.rejected_by")

    statuses: Dict[str, str] = {}
    machine = pair.get("machine_capabilities")
    if checker.require(isinstance(machine, dict), f"{context}: machine_capabilities must be an object"):
        checker.require(set(machine) == set(MACHINE_CAPABILITIES),
                        f"{context}: machine_capabilities must list every machine capability exactly once; "
                        f"missing {sorted(set(MACHINE_CAPABILITIES) - set(machine))}, "
                        f"unknown {sorted(set(machine) - set(MACHINE_CAPABILITIES))}")
        for name, entry in machine.items():
            status = check_entry(checker, entry, f"{context}.machine_capabilities.{name}")
            if status:
                statuses[name] = status
    topo = pair.get("topology_capabilities")
    if checker.require(isinstance(topo, dict), f"{context}: topology_capabilities must be an object"):
        checker.require(set(topo) == set(topology),
                        f"{context}: topology_capabilities must match vocabularies.topology_capabilities; "
                        f"missing {sorted(set(topology) - set(topo))}, unknown {sorted(set(topo) - set(topology))}")
        for name, entry in topo.items():
            status = check_entry(checker, entry, f"{context}.topology_capabilities.{name}")
            if status:
                statuses[f"topology:{name}"] = status

    if pair_status == "NA":
        checker.require(bool(rejected), f"{context}: NA pair requires rejected_by")
        for name, status in statuses.items():
            checker.require(status == "NA", f"{context}: NA pair must label {name} NA, got {status}")
    elif pair_status in LIVE:
        checker.require(bool(negotiated), f"{context}: {pair_status} pair requires negotiated_by")
        checker.require(backend is not None, f"{context}: {pair_status} pair requires a backend")
        checker.require(statuses.get("posix_exec") == pair_status,
                        f"{context}: posix_exec must carry the pair status {pair_status}")
    elif pair_status == "PLANNED":
        for name, status in statuses.items():
            checker.require(status == "PLANNED", f"{context}: PLANNED pair must label {name} PLANNED, got {status}")
    return (host, target, profile) if isinstance(host, str) and isinstance(target, str) and isinstance(profile, str) else None


def validate_matrix(document: Any) -> List[str]:
    """Return every rule violation; an empty list means the matrix is well-formed."""
    checker = Checker()
    if not checker.keys(document, "matrix", TOP_LEVEL):
        return checker.violations
    checker.require(document.get("$schema") == "../schemas/host-target-capabilities-v0.4.schema.json", "matrix: $schema")
    checker.require(document.get("schema_version") == 1, "matrix: schema_version must be 1")
    checker.require(document.get("target_release") == "0.4.0", "matrix: target_release must be 0.4.0")
    checker.require(document.get("contract_state") in ("draft_unverified", "inputs_frozen"), "matrix: contract_state")
    checker.require(document.get("normative_source") == "planning/developer-environments/GOAL-0.4.0.md#versioned-gate-inputs",
                    "matrix: normative_source")
    checker.require(isinstance(document.get("source_head"), str) and bool(GIT_SHA_RE.match(document["source_head"])),
                    "matrix: source_head must be a git sha")
    checker.require(document.get("unlisted_capability_status") == "PLANNED", "matrix: unlisted_capability_status must be PLANNED")
    definitions = document.get("status_definitions")
    if checker.keys(definitions, "status_definitions", STATUSES):
        for status in STATUSES:
            checker.require(isinstance(definitions.get(status), str) and bool(definitions[status].strip()),
                            f"status_definitions.{status} must be non-empty")
    checker.string_list(document.get("evidence_notes"), "evidence_notes", unique=False)
    surfaces = checker.string_list(document.get("generated_surfaces"), "generated_surfaces", SURFACE_RE)
    checker.require(bool(surfaces), "generated_surfaces must not be empty")

    vocabularies = document.get("vocabularies")
    topology: List[str] = []
    if checker.keys(vocabularies, "vocabularies", ("machine_capabilities", "topology_capabilities", "profiles", "backends")):
        listed = checker.string_list(vocabularies.get("machine_capabilities"), "vocabularies.machine_capabilities", SNAKE_RE)
        checker.require(listed == list(MACHINE_CAPABILITIES),
                        f"vocabularies.machine_capabilities must equal the 13 MachineCapability wire names in declaration order; got {listed}")
        topology = checker.string_list(vocabularies.get("topology_capabilities"), "vocabularies.topology_capabilities", SNAKE_RE)
        checker.require(bool(topology), "vocabularies.topology_capabilities must not be empty")
        check_alias_table(checker, vocabularies.get("profiles"), "vocabularies.profiles", PROFILES, SNAKE_RE)
        check_alias_table(checker, vocabularies.get("backends"), "vocabularies.backends", BACKENDS,
                          re.compile(r"^[a-z][a-z0-9_-]*$"), ("note",))

    hosts = check_hosts(checker, document.get("hosts"))
    targets = document.get("targets")
    target_names: List[str] = []
    if checker.require(isinstance(targets, dict) and targets, "targets: expected non-empty object"):
        for name, target in targets.items():
            checker.require(name in OSES, f"targets.{name}: unknown target os")
            if checker.keys(target, f"targets.{name}", ("os",), ("note",)):
                checker.require(target.get("os") == name, f"targets.{name}: os must equal the key")
            target_names.append(name)

    pairs = document.get("pairs")
    if checker.require(isinstance(pairs, list) and pairs, "pairs: expected non-empty array"):
        seen: set = set()
        for index, pair in enumerate(pairs):
            key = check_pair(checker, pair, index, hosts, target_names, topology)
            if key is not None:
                checker.require(key not in seen, f"pairs[{index}]: duplicate pair {key}")
                seen.add(key)
        for pair in pairs:
            if isinstance(pair, dict) and pair.get("pair_status") != "NA":
                for group in ("machine_capabilities", "topology_capabilities"):
                    entries = pair.get(group)
                    if isinstance(entries, dict):
                        for name, entry in entries.items():
                            if isinstance(entry, dict) and entry.get("status") == "ACTIVE":
                                checker.fail(f"{pair.get('host')}/{pair.get('target')}/{pair.get('profile')}.{group}.{name}: "
                                             "ACTIVE requires a published 0.4 release; none exists")
    return checker.violations


def iter_evidence(document: Any):
    for pair in document.get("pairs", []):
        for group in ("machine_capabilities", "topology_capabilities"):
            for name, entry in pair.get(group, {}).items():
                for evidence in entry.get("evidence", []):
                    yield f"{pair['host']}/{pair['target']}/{pair['profile']}.{group}.{name}", evidence


def verify_evidence_digests(document: Any, root: Path) -> Tuple[List[str], List[str], List[str]]:
    """Return (verified, unverifiable, mismatched) descriptions for cited evidence files."""
    verified: List[str] = []
    unverifiable: List[str] = []
    mismatched: List[str] = []
    checked: Dict[Tuple[str, str], Optional[str]] = {}
    for context, evidence in iter_evidence(document):
        result_path = evidence["result_path"]
        for label, relative, expected in (
            ("result", result_path, evidence["result_sha256"]),
            ("checksums", str(Path(result_path).parent / "checksums.sha256"), evidence.get("checksums_sha256")),
        ):
            if expected is None:
                continue
            key = (relative, expected)
            if key in checked:
                continue
            path = root / relative
            if not path.is_file():
                checked[key] = None
                unverifiable[len(unverifiable):] = [f"{relative}: unverifiable locally ({label} for {context})"]
                continue
            actual = sha256_file(path)
            checked[key] = actual
            if actual == expected:
                verified.append(f"{relative}: sha256 verified")
            else:
                mismatched.append(f"{relative}: expected {expected} got {actual} ({label} for {context})")
    return verified, unverifiable, mismatched


def run(root: Path = ROOT, relative: str = MATRIX, out: Callable[[str], None] = print) -> int:
    try:
        document = load_json(root, relative)
    except InvalidMatrix as error:
        out(f"FAIL {error}")
        return 1
    violations = validate_matrix(document)
    for violation in violations:
        out(f"FAIL {violation}")
    verified, unverifiable, mismatched = verify_evidence_digests(document, root)
    for line in verified:
        out(f"ok   {line}")
    for line in unverifiable:
        out(f"skip {line}")
    for line in mismatched:
        out(f"FAIL {line}")
    pairs = document.get("pairs", []) if isinstance(document, dict) else []
    out(f"{relative}: {len(pairs)} pairs, {len(violations)} rule violations, "
        f"{len(verified)} evidence digests verified, {len(unverifiable)} unverifiable locally, {len(mismatched)} mismatched")
    return 1 if violations or mismatched else 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=ROOT, help="repository root (default: this checkout)")
    parser.add_argument("--matrix", default=MATRIX, help="matrix path relative to --root")
    args = parser.parse_args(argv)
    return run(args.root.resolve(), args.matrix)


if __name__ == "__main__":
    sys.exit(main())
