"""Strict Docker requirement-input validation; never Docker coverage or PASS evidence.

Run with the pinned dependency:
  uv run --with jsonschema==4.23.0 python scripts/helpers/docker_compatibility_contract.py
Default mode rejects drafts/unresolved pins. --check-draft checks only structure.
No Docker commands, fixture workloads, or harnesses are executed by this module.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import sys

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = "config/docker-compatibility-v0.4.json"
SCHEMA = "schemas/docker-compatibility-v0.4.schema.json"
GOAL = "planning/developer-environments/GOAL-0.4.0.md"
MAX_JSON = 4 * 1024 * 1024
MAX_FIXTURE_FILE = 16 * 1024 * 1024
MAX_FIXTURE_BYTES = 512 * 1024 * 1024
MAX_FIXTURE_ENTRIES = 4096
MINIMUM_SECTION_START = "At minimum the manifest covers, from the Mac's supported unmodified clients:\n"
MINIMUM_SECTION_END = "release candidate; silence is not compatibility.\n"
REQUIRED_IDS = frozenset([
  "docker.engine.version",
  "docker.engine.info",
  "docker.engine.context",
  "docker.engine.api_negotiation",
  "docker.image.registry_login",
  "docker.image.pull",
  "docker.image.push",
  "docker.image.tag",
  "docker.image.inspect",
  "docker.image.save_load",
  "docker.image.remove",
  "docker.container.create",
  "docker.container.start",
  "docker.container.stop",
  "docker.container.restart",
  "docker.container.kill",
  "docker.container.wait",
  "docker.container.remove",
  "docker.container.health_checks",
  "docker.container.logs",
  "docker.container.events",
  "docker.container.attach",
  "docker.container.exec",
  "docker.container.stdin",
  "docker.container.tty",
  "docker.container.signals",
  "docker.container.exact_exit_results",
  "docker.storage.bind_mounts",
  "docker.storage.named_volumes",
  "docker.storage.tmpfs",
  "docker.storage.read_only_mounts",
  "docker.storage.ownership",
  "docker.storage.persistence",
  "docker.network.user_defined_networks",
  "docker.network.dns",
  "docker.network.published_ports",
  "docker.network.cleanup",
  "docker.build.multi_stage",
  "docker.build.parallel",
  "docker.build.build_arguments",
  "docker.build.secrets",
  "docker.build.cache_reuse",
  "docker.build.cache_export",
  "docker.build.cache_isolation",
  "docker.build.ssh_mounts",
  "docker.build.output_export",
  "docker.compose.create",
  "docker.compose.up",
  "docker.compose.down",
  "docker.compose.dependency_ordering",
  "docker.compose.health_ordering",
  "docker.compose.networks",
  "docker.compose.volumes",
  "docker.compose.logs",
  "docker.compose.exec",
  "docker.compose.scaling",
  "docker.compose.failure_propagation",
  "docker.operation.resource_limits",
  "docker.operation.oom",
  "docker.operation.daemon_restart_recovery",
  "docker.operation.concurrent_clients",
  "docker.operation.same_environment_isolation",
  "docker.operation.sibling_environment_isolation"
])


class InvalidContract(ValueError):
    """Invalid/unresolved requirement inputs, not a workload test result."""


def require(condition, message):
    if not condition:
        raise InvalidContract(message)


def _components(relative):
    require(type(relative) is str, "path must be text")
    path = PurePosixPath(relative)
    require(not path.is_absolute() and str(path) == relative and path.parts
            and all(part not in (".", "..") for part in path.parts), "path is not canonical and repository-relative")
    return path.parts


def _open_relative(root, relative, directory=False):
    parts = _components(relative)
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for index, part in enumerate(parts):
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
            if index != len(parts) - 1 or directory:
                flags |= os.O_DIRECTORY
            opened = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = opened
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_descriptor(descriptor, limit):
    before = os.fstat(descriptor)
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1,
            "input is not a single-link regular file")
    require(0 <= before.st_size <= limit, "input exceeds byte bound")
    chunks = []
    remaining = limit + 1
    while remaining:
        chunk = os.read(descriptor, min(remaining, 65536))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    data = b"".join(chunks)
    after = os.fstat(descriptor)
    require(len(data) == before.st_size <= limit and
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
            "input changed during bounded read")
    return data, stat.S_IMODE(before.st_mode)


def read_regular(root, relative, limit=MAX_JSON):
    try:
        descriptor = _open_relative(root, relative)
        try:
            return _read_descriptor(descriptor, limit)[0]
        finally:
            os.close(descriptor)
    except OSError as error:
        raise InvalidContract(f"cannot read regular pinned input: {relative} ({error.strerror})") from error


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON object member")
        result[key] = value
    return result


def _invalid_constant(_value):
    raise InvalidContract("non-finite JSON number")


def load_json(root, relative):
    try:
        return json.loads(read_regular(root, relative).decode("utf-8"),
                          object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidContract("input is not strict UTF-8 JSON") from error


def minimum_section_digest(root):
    text = read_regular(root, GOAL).decode("utf-8")
    require(text.count(MINIMUM_SECTION_START) == 1 and text.count(MINIMUM_SECTION_END) == 1,
            "normative Docker minimum section changed; explicit catalog review required")
    start = text.index(MINIMUM_SECTION_START)
    end = text.index(MINIMUM_SECTION_END, start) + len(MINIMUM_SECTION_END)
    return hashlib.sha256(text[start:end].encode()).hexdigest()


def fixture_tree_digest(root, relative):
    """SHA-256 of sorted [relative-path, POSIX-mode, size, content-SHA256] JSON.

    Real directories only; no symlinks, special files, hardlinks, empty trees,
    traversal, unbounded inventories, or unbounded reads are accepted.
    """
    entries = []
    total_bytes = 0
    visited_entries = 0

    def walk(descriptor, prefix, depth):
        nonlocal total_bytes, visited_entries
        require(depth <= 32, "fixture directory depth exceeds bound")
        before = os.fstat(descriptor)
        with os.scandir(descriptor) as scan:
            names = sorted(entry.name for entry in scan)
        for name in names:
            visited_entries += 1
            require(visited_entries <= MAX_FIXTURE_ENTRIES, "fixture inventory exceeds entry bound")
            relative_name = "/".join((*prefix, name))
            metadata = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISDIR(metadata.st_mode):
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
                try:
                    walk(child, (*prefix, name), depth + 1)
                finally:
                    os.close(child)
            else:
                require(stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1,
                        "fixture contains a symlink, special file, or hardlink")
                child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
                try:
                    data, mode = _read_descriptor(child, MAX_FIXTURE_FILE)
                finally:
                    os.close(child)
                total_bytes += len(data)
                require(total_bytes <= MAX_FIXTURE_BYTES, "fixture total bytes exceed bound")
                entries.append([relative_name, mode, len(data), hashlib.sha256(data).hexdigest()])
        after = os.fstat(descriptor)
        require((before.st_mtime_ns, before.st_ctime_ns) == (after.st_mtime_ns, after.st_ctime_ns),
                "fixture directory changed during inventory")

    try:
        descriptor = _open_relative(root, relative, directory=True)
        try:
            walk(descriptor, (), 0)
        finally:
            os.close(descriptor)
    except OSError as error:
        raise InvalidContract("fixture tree cannot be read without following links") from error
    require(entries, "fixture inventory must not be empty")
    return hashlib.sha256(json.dumps(sorted(entries), ensure_ascii=False,
                                    separators=(",", ":")).encode()).hexdigest()


def validate_contract(value, root=ROOT, *, check_draft=False):
    schema = load_json(root, SCHEMA)
    Draft202012Validator.check_schema(schema)
    problems = list(Draft202012Validator(schema).iter_errors(value))
    if problems:
        locations = [".".join(map(str, error.absolute_path)) or "<root>" for error in problems[:4]]
        raise InvalidContract("schema rejection at " + ", ".join(locations))
    canonical = schema["properties"]["scenarios"]["items"]["enum"]
    require({entry["id"] for entry in canonical} == REQUIRED_IDS and len(canonical) == len(REQUIRED_IDS),
            "schema changed the required minimum scenario inventory")
    ids = [scenario["id"] for scenario in value["scenarios"]]
    require(len(ids) == len(REQUIRED_IDS) and set(ids) == REQUIRED_IDS,
            "required Docker behavior is missing, duplicated or unknown")
    require(value["minimum_requirements_sha256"] == minimum_section_digest(root),
            "normative minimum changed; do not silently reuse old coverage requirements")
    observation = value["host_client_observation"]
    require(hashlib.sha256(observation["stdout"].encode()).hexdigest() == observation["sha256"],
            "host client observation digest mismatch")
    require(value["intentional_exclusions"] == [], "no new exclusions or approval claims are authorized")
    unresolved = [name for name in ("fixture_bundle", "harness") if value[name]["state"] != "pinned"]
    if check_draft:
        return {"kind": "draft_structure_only", "required_scenarios": len(ids),
                "unresolved_required_pins": unresolved, "docker_tests_executed": 0,
                "compatibility_certified": False}
    require(value["contract_state"] == "inputs_frozen", "draft contract is not frozen gate input")
    require(not unresolved, "unresolved required pins: " + ", ".join(unresolved))
    fixture = value["fixture_bundle"]
    require(fixture_tree_digest(root, fixture["path"]) == fixture["sha256"],
            "fixture bundle digest mismatch")
    harness = value["harness"]
    descriptor = _open_relative(root, harness["entry_point"])
    try:
        harness_bytes, mode = _read_descriptor(descriptor, MAX_FIXTURE_FILE)
    finally:
        os.close(descriptor)
    require(mode & 0o111, "pinned host Docker harness is not executable")
    require(hashlib.sha256(harness_bytes).hexdigest() == harness["sha256"], "harness digest mismatch")
    return {"kind": "requirement_inputs_valid", "required_scenarios": len(ids),
            "docker_tests_executed": 0, "compatibility_certified": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--contract", default=CONTRACT)
    parser.add_argument("--check-draft", action="store_true",
                        help="structure/completeness lint only; never a release preflight or PASS")
    args = parser.parse_args(argv)
    try:
        result = validate_contract(load_json(args.repo_root, args.contract),
                                   args.repo_root, check_draft=args.check_draft)
    except (InvalidContract, OSError, ValueError) as error:
        print(f"INVALID_REQUIREMENT_INPUTS: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
