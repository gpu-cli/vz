"""Single source of truth: which Docker suite proves which manifest scenario ID, and how completely.

Every Docker Machine module and every `docker_host_driver` recipe declares its
scenario IDs by reading this table (`for_suite`, `for_recipe`), so a claim can
never drift between a module and the lane result. Statuses per (suite, id):

  proven    the suite's assertions cover the manifest `expected` block completely
  partial   the suite exercises the ID but `unproven` lists the `expected` fields
            it does not assert; the lane result reports such an ID as FAIL
  secondary the suite exercises the ID too, but the proof is accounted under
            another suite's primary (proven/partial) claim; never emitted

IDs no suite exercises are listed in `UNCOVERED` with the gap-suite that the
2026-09-07 coverage audit assigned (mounts, netpolicy, concurrency, isolation).
Import-time validation (`check`) rejects: an ID outside the required inventory,
an ID `proven` by two suites, more than one primary claim per ID, a secondary
without a primary, a `partial` without unproven fields, an unproven field that
is not in the manifest `expected` block, and any required ID that is neither
claimed nor explicitly uncovered (or both).

`lane_scenarios` turns one suite run into lane-result `scenarios[]` entries.
PASS there means the DEV run passed and the claim is `proven`; it is never
release certification. The aggregate validator decides.
"""
from __future__ import annotations

from collections import namedtuple
import json
import os
from pathlib import Path
import re

REPO = Path(__file__).resolve().parents[2]
MANIFEST = REPO / "config/docker-compatibility-v0.4.json"
MANIFEST_LIMIT = 4 * 1024 * 1024
ID_PATTERN = re.compile(r"^docker\.[a-z_]+\.[a-z_]+$")
PHASES = {"clean-provision": "clean-provision", "persisted-recovery": "persisted-recovery/pre-sleep",
          "final-cleanup": "final-cleanup"}
GAP_SUITES = ("mounts", "netpolicy", "concurrency", "isolation")
STATUSES = ("proven", "partial", "secondary")

Claim = namedtuple("Claim", "id suite sources status unproven")
Suite = namedtuple("Suite", "name module evidence polls process_scenario")
Poll = namedtuple("Poll", "id deadline_seconds samples")


class CoverageError(ValueError):
    """The coverage table contradicts the required inventory or itself."""


def require(condition, message):
    if not condition:
        raise CoverageError(message)


def _health_samples(item):
    return item["health"]["samples"]


def _limits_samples(item):
    return item["workload"]["sibling_health"]["samples"]


def _recovery_samples(item):
    return sum(len(poll["samples"]) for poll in item["cycle"]["readiness_polls"].values())


SUITES = {suite.name: suite for suite in (
    Suite("compose", "docker_host_driver.py", ("compose-machine-{index}/result.json",), (), "docker.compose.up"),
    Suite("build", "docker_host_driver.py", ("build-machine-{index}/result.json",), (), "docker.build.cache_reuse"),
    Suite("artifacts", "linux_docker_build_artifacts.py", ("artifacts-machine-{index}/machine-artifact-validation.json",), (),
          "docker.build.multi_stage"),
    Suite("parallel", "linux_docker_build_parallel.py", ("parallel-machine-{index}/machine-parallel-validation.json",),
          (Poll("poll.service.health_probe", 60, _health_samples),), "docker.build.parallel"),
    Suite("ssh", "linux_docker_build_ssh.py", ("ssh-machine-{index}/machine-ssh-validation.json",), (), "docker.build.ssh_mounts"),
    Suite("lifecycle", "linux_docker_container_lifecycle.py",
          ("container-machine-{index}/workload.json", "container-machine-{index}/cleanup.json"), (), "docker.container.create"),
    Suite("images", "linux_docker_image_machine.py", ("image-machine-{index}/machine-image-validation.json",), (), "docker.image.inspect"),
    Suite("registry", "linux_docker_registry_machine.py", ("registry-machine-{index}/machine-registry-validation.json",), (),
          "docker.image.registry_login"),
    Suite("handshake", "linux_docker_handshake_machine.py", ("handshake-machine-{index}/machine-handshake-validation.json",), (),
          "docker.engine.version"),
    Suite("limits", "linux_docker_limits_machine.py", ("limits-machine-{index}/machine-limits-validation.json",),
          (Poll("poll.service.health_probe", 60, _limits_samples),), "docker.operation.resource_limits"),
    Suite("recovery", "linux_docker_recovery_machine.py", ("recovery-machine-{index}/machine-recovery-validation.json",),
          (Poll("poll.docker.engine_ready", 60, _recovery_samples),), "docker.operation.daemon_restart_recovery"),
)}


def _c(identifier, suite, sources, status="proven", unproven=()):
    return Claim(identifier, suite, tuple(sources) if isinstance(sources, (tuple, list)) else (sources,), status, tuple(unproven))


TABLE = (
    # handshake: one Machine, Engine version/API handshake, no mutation.
    _c("docker.engine.version", "handshake", "run_machine"),
    _c("docker.engine.info", "handshake", "run_machine", "partial", ("daemon_unique_per_machine",)),
    _c("docker.engine.context", "handshake", "run_machine", "partial", ("global_default_unchanged", "stale_or_foreign_context")),
    _c("docker.engine.api_negotiation", "handshake", "run_machine"),
    # registry: owned private registry Session per Machine.
    _c("docker.image.registry_login", "registry", "Session.authenticate"),
    _c("docker.image.pull", "registry", "Session.roundtrip"),
    _c("docker.image.push", "registry", "Session.roundtrip"),
    # images: source-selected image round trip.
    _c("docker.image.tag", "images", "ImageRoundTrip.exercise"),
    _c("docker.image.inspect", "images", "ImageRoundTrip.exercise"),
    _c("docker.image.save_load", "images", "ImageRoundTrip.exercise"),
    _c("docker.image.remove", "images", "ImageRoundTrip.cleanup"),
    # lifecycle: container I/O, state, signals, processes.
    _c("docker.container.create", "lifecycle", "Lifecycle.exercise"),
    _c("docker.container.start", "lifecycle", "Lifecycle.exercise"),
    _c("docker.container.stop", "lifecycle", "Lifecycle.exercise"),
    _c("docker.container.restart", "lifecycle", "Lifecycle.exercise"),
    _c("docker.container.kill", "lifecycle", "Lifecycle.exercise"),
    _c("docker.container.wait", "lifecycle", "Lifecycle.exercise"),
    _c("docker.container.remove", "lifecycle", "Lifecycle.cleanup"),
    _c("docker.container.health_checks", "lifecycle", "Lifecycle.exercise"),
    _c("docker.container.logs", "lifecycle", "Lifecycle.exercise"),
    _c("docker.container.events", "lifecycle", "Lifecycle.cleanup"),
    _c("docker.container.attach", "lifecycle", "Lifecycle.exercise"),
    _c("docker.container.exec", "lifecycle", "Lifecycle.exercise"),
    _c("docker.container.stdin", "lifecycle", "Lifecycle.exercise", "partial", ("eof_delivered_once",)),
    _c("docker.container.tty", "lifecycle", "Lifecycle.exercise"),
    _c("docker.container.signals", "lifecycle", "Lifecycle.exercise", "partial", ("unrelated_processes_unchanged",)),
    _c("docker.container.exact_exit_results", "lifecycle", "Lifecycle.exercise"),
    # recovery: public Stop/Up of the owning Environment (not an in-place daemon restart).
    _c("docker.storage.persistence", "recovery", "run_machine"),
    _c("docker.operation.daemon_restart_recovery", "recovery", "run_machine"),
    # compose: driver recipes (sources are recipe names) plus the driver cleanup path.
    _c("docker.compose.create", "compose", "compose-create", "partial", ("configuration",)),
    _c("docker.compose.up", "compose", "compose-up-order"),
    _c("docker.compose.dependency_ordering", "compose", "compose-up-order", "partial", ("undeclared_dependency_edges_absent",)),
    _c("docker.compose.health_ordering", "compose", ("compose-up-order", "compose-blocked-health")),
    _c("docker.compose.logs", "compose", "compose-logs"),
    _c("docker.compose.exec", "compose", "compose-exec"),
    _c("docker.compose.networks", "compose", "compose-network-paths"),
    _c("docker.network.user_defined_networks", "compose", "compose-network-paths"),
    _c("docker.network.dns", "compose", "compose-network-paths", "partial", ("foreign_environment_alias", "stale_alias_after_remove")),
    _c("docker.compose.volumes", "compose", "compose-volume-persistence", "partial", ("restart_data_sha256",)),
    _c("docker.compose.scaling", "compose", "compose-scale"),
    _c("docker.compose.failure_propagation", "compose", "compose-failure", "partial", ("failed_dependency_not_reported_healthy",)),
    _c("docker.compose.down", "compose", "Driver.cleanup", "partial", ("external_and_unrelated_resources_unchanged",)),
    # build: driver recipes on the embedded builder.
    _c("docker.build.cache_reuse", "build", "build-cache-reuse"),
    _c("docker.build.cache_isolation", "build", "build-cache-mount", "partial",
       ("sibling_machine_cache_hit_without_import", "sibling_environment_cache_hit_without_import")),
    _c("docker.build.output_export", "build", "build-multi-stage", "partial", ("oci_export_digest",)),
    _c("docker.build.multi_stage", "build", "build-multi-stage", "secondary"),
    _c("docker.build.build_arguments", "build", "build-arguments", "secondary"),
    _c("docker.build.secrets", "build", "build-secret-mount", "secondary"),
    # artifacts: OCI/cache export with blob-level canary scans across builder roles.
    _c("docker.build.multi_stage", "artifacts", "run_machine"),
    _c("docker.build.build_arguments", "artifacts", "run_machine"),
    _c("docker.build.secrets", "artifacts", "run_machine"),
    _c("docker.build.cache_export", "artifacts", "run_machine"),
    _c("docker.build.output_export", "artifacts", "run_machine", "secondary"),
    # parallel / ssh / limits.
    _c("docker.build.parallel", "parallel", "run_machine"),
    _c("docker.build.ssh_mounts", "ssh", "run_machine"),
    _c("docker.operation.resource_limits", "limits", "run_machine"),
    _c("docker.operation.oom", "limits", "run_machine"),
)

UNCOVERED = (
    ("docker.storage.bind_mounts", "mounts"),
    ("docker.storage.named_volumes", "mounts"),
    ("docker.storage.tmpfs", "mounts"),
    ("docker.storage.read_only_mounts", "mounts"),
    ("docker.storage.ownership", "mounts"),
    ("docker.network.published_ports", "netpolicy"),
    ("docker.network.cleanup", "netpolicy"),
    ("docker.operation.concurrent_clients", "concurrency"),
    ("docker.operation.same_environment_isolation", "isolation"),
    ("docker.operation.sibling_environment_isolation", "isolation"),
)


def _read(path, limit):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        size = os.fstat(descriptor).st_size
        require(0 < size <= limit, "manifest size out of bounds")
        data = os.read(descriptor, size + 1)
        require(len(data) == size, "manifest changed during read")
        return data
    finally:
        os.close(descriptor)


def manifest(path=MANIFEST):
    """{id: {'phase': lane phase, 'expected': {...}}} from the frozen manifest."""
    value = json.loads(_read(path, MANIFEST_LIMIT).decode("utf-8"))
    rows = {}
    for scenario in value["scenarios"]:
        identifier = scenario["id"]
        require(ID_PATTERN.match(identifier) and identifier not in rows, "manifest scenario id invalid or duplicated")
        require(scenario["phase"] in PHASES and isinstance(scenario["expected"], dict) and scenario["expected"],
                "manifest scenario lacks phase/expected: " + identifier)
        rows[identifier] = {"phase": PHASES[scenario["phase"]], "expected": dict(scenario["expected"])}
    return rows


def required_ids(rows):
    """The frozen 63-ID inventory; cross-checked against REQUIRED_IDS when that module is importable."""
    ids = frozenset(rows)
    try:
        import docker_compatibility_contract as contract
    except ImportError:  # /usr/bin/python3 without jsonschema: the manifest IDs stand in; tests cross-check.
        return ids
    require(ids == contract.REQUIRED_IDS, "manifest scenario inventory differs from REQUIRED_IDS")
    return contract.REQUIRED_IDS


def check(table=TABLE, uncovered=UNCOVERED, rows=None):
    """Validate the table against the manifest; returns {id: primary Claim}."""
    rows = manifest() if rows is None else rows
    required = required_ids(rows)
    primaries, proven_by, seen_pairs = {}, {}, set()
    for claim in table:
        require(claim.id in required, "unknown scenario id claimed: " + claim.id)
        require(claim.suite in SUITES, "unknown suite: " + claim.suite)
        require(claim.status in STATUSES, "unknown status for " + claim.id)
        require(claim.sources and all(isinstance(s, str) and s for s in claim.sources), "claim without source: " + claim.id)
        require((claim.suite, claim.id) not in seen_pairs, "duplicate claim " + claim.suite + "/" + claim.id)
        seen_pairs.add((claim.suite, claim.id))
        expected = rows[claim.id]["expected"]
        if claim.status == "partial":
            require(claim.unproven and set(claim.unproven) <= set(expected) and len(set(claim.unproven)) == len(claim.unproven),
                    "partial claim must list distinct manifest expected fields: " + claim.id)
            require(set(claim.unproven) != set(expected), "partial claim proves nothing: " + claim.suite + "/" + claim.id)
        else:
            require(not claim.unproven, "only partial claims list unproven fields: " + claim.id)
        if claim.status == "proven":
            require(claim.id not in proven_by, claim.id + " claimed proven by two suites: " + proven_by.get(claim.id, "") + ", " + claim.suite)
            proven_by[claim.id] = claim.suite
        if claim.status in ("proven", "partial"):
            other = primaries.get(claim.id)
            require(other is None, claim.id + " has two primary claims: " + (other.suite if other else "") + ", " + claim.suite)
            primaries[claim.id] = claim
    for claim in table:
        if claim.status == "secondary":
            require(claim.id in primaries and primaries[claim.id].suite != claim.suite,
                    "secondary claim without a primary in another suite: " + claim.suite + "/" + claim.id)
    gaps = {}
    for identifier, gap in uncovered:
        require(identifier in required, "unknown uncovered id: " + identifier)
        require(gap in GAP_SUITES, "unknown gap suite for " + identifier)
        require(identifier not in primaries and identifier not in gaps, identifier + " is both claimed and uncovered")
        gaps[identifier] = gap
    missing = sorted(required - set(primaries) - set(gaps))
    require(not missing, "required ids neither claimed nor uncovered: " + ", ".join(missing))
    for suite in SUITES.values():
        require(suite.process_scenario in primaries and primaries[suite.process_scenario].suite == suite.name,
                "suite process accounting scenario is not its own primary claim: " + suite.name)
    return primaries


def for_suite(suite, *, include_partial=True):
    """IDs a suite is the primary prover of, in table order (module-level SCENARIOS declaration)."""
    require(suite in SUITES, "unknown suite: " + str(suite))
    statuses = ("proven", "partial") if include_partial else ("proven",)
    return tuple(claim.id for claim in TABLE if claim.suite == suite and claim.status in statuses)


def for_recipe(suite, recipe):
    """IDs a driver recipe relates to (primary and secondary), for `Driver.observe(name, related)`."""
    ids = tuple(claim.id for claim in TABLE if claim.suite == suite and recipe in claim.sources)
    require(ids, "recipe declares no scenario: " + suite + "/" + recipe)
    return ids


def claims(suite):
    return tuple(claim for claim in TABLE if claim.suite == suite and claim.status in ("proven", "partial"))


def coverage():
    """[(id, suite or gap suite, status)] over the whole inventory, manifest order."""
    rows = manifest()
    primaries = check(rows=rows)
    gaps = dict(UNCOVERED)
    table = []
    for identifier in rows:
        if identifier in primaries:
            table.append((identifier, primaries[identifier].suite, primaries[identifier].status))
        else:
            table.append((identifier, gaps[identifier], "uncovered"))
    return table


def _timing(slices, key, pick, fallback):
    values = [item[key] for item in slices if isinstance(item.get(key), int) and item[key] >= 0]
    return pick(values) if values else fallback


def lane_scenarios(suite, slices, *, phase, passed, error=None, evidence_prefix="harness", window=None, rows=None):
    """Lane-result `scenarios[]` for one suite run.

    `slices` are the run's `scenario.machine_slices`; only IDs whose manifest
    phase equals `phase` are emitted. Status is PASS only for a `proven` claim
    of a passed run; `partial` claims are FAIL with their unproven fields named.
    """
    rows = manifest() if rows is None else rows
    check(rows=rows)
    descriptor = SUITES[suite]
    started_fallback, ended_fallback = window if window else (0, 0)
    started = _timing(slices, "started_unix_ns", min, started_fallback)
    ended = _timing(slices, "ended_unix_ns", max, ended_fallback)
    evidence = [evidence_prefix + "/" + pattern.format(index=index) for index in range(len(slices)) for pattern in descriptor.evidence]
    polls = []
    for poll in descriptor.polls:
        samples = 0
        for item in slices:
            try:
                samples += int(poll.samples(item))
            except (KeyError, TypeError, ValueError):
                samples = 0
                break
        if samples >= 1:
            polls.append({"id": poll.id, "samples": samples, "deadline_seconds": poll.deadline_seconds, "satisfied": bool(passed)})
    entries = []
    for claim in claims(suite):
        if rows[claim.id]["phase"] != phase:
            continue
        expected = rows[claim.id]["expected"]
        assertions = []
        if slices:
            for field in expected:
                if field in claim.unproven:
                    assertions.append("UNPROVEN expected." + field + " (no assertion in " + suite + ")")
                else:
                    assertions.append("expected." + field + "=" + json.dumps(expected[field], sort_keys=True, separators=(",", ":")) +
                                      " asserted by " + suite + "/" + "+".join(claim.sources) + " on " + str(len(slices)) + " Machine(s)")
        if not passed:
            assertions.append("run failed: " + (error or "harness reported failure"))
        status = "PASS" if passed and claim.status == "proven" and slices else "FAIL"
        entries.append({"id": claim.id, "status": status, "started_unix_ns": started, "ended_unix_ns": ended,
                        "assertions": assertions, "evidence": list(evidence), "readiness_polls": list(polls)})
    return entries


PRIMARY = check()
