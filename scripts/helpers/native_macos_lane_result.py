#!/usr/bin/env python3
"""`native-macos` lane of the vz 0.4 aggregate release gate.

Entry point: `scripts/run-macos-developer-environment-e2e.sh --suite all <lane
argv>` with exactly the argv contract from `vz04_lanes.lane_argv`
(`argv_contract: "vz04"`). A schema-valid `<evidence-dir>/lane-result.json` is
written on every exit path where the identity fields permit one.

The lane owns exactly one required scenario,
`gate.native.target_native_execution` (criterion 4) at `clean-provision`. It
earns that row by *driving the real hardware harness*,
`scripts/run-installed-native-macos-e2e.py --require-swift`, which transfers
`tests/fixtures/vz-0.4/native-macos-swift` into a native macOS Machine over the
`vz exec` stream, runs `swift build -c release` and `swift test` inside the
guest, and runs the release binary to read back its protocol identity. The three
other phases the contract assigns this lane carry no required scenario; they
report `not_implemented` and claim nothing.

What this lane will NOT do
--------------------------
It never provisions a macOS template. `vz-macos-setup` downloads an IPSW from
Apple and takes one administrator authorisation to install the loader into the
new guest disk; that is a maintainer step with a recipe of its own
(`planning/developer-environments/macos-local-setup.md`), not something a gate
lane may trigger. This lane only *reads* an already registered template out of
an installed `machine-target-catalog.json`. When no catalog registers a native
macOS image, the lane reports `failure.reason: prerequisite` and its scenario
FAIL. That is the honest state of a host that has never run setup, and it is
deliberately distinguishable from the harness running and failing.

Failure vocabulary (the schema's, reused, never extended)
---------------------------------------------------------
  argv/contract/release admission rejected      -> input_rejected  (exit 2)
  no registered template, no tmux, unusable
  release components, dirty state root          -> prerequisite    (exit 1)
  harness ran and an assertion did not hold,
  or a prohibited component was observed        -> assertion       (exit 1)
  harness exceeded the lane's deadline          -> timeout         (exit 1)
  harness wrote no summary, or the lane itself
  broke while driving or translating it         -> crash           (exit 1)
  lane could not positively remove what it made -> cleanup         (exit 1)
  phase carries no required scenario            -> not_implemented (exit 3)

PASS is not the harness's exit code
-----------------------------------
`run-installed-native-macos-e2e.py` writes `summary.json` on both paths, and its
`--expect-preparation-failure` mode sets `passed: True` for a run that never
booted a guest. So a zero exit is necessary and not sufficient here. The lane
re-derives the criterion from the retained evidence: the run's scope, the
recorded toolchain identity against the contract pin, the guest's own
`sw_vers`/`hw.model` as reported by the compiled Swift probe, the transferred
fixture sources, and one receipt per Swift command proving it exited 0. A run
that leaves no `swift-build`/`swift-test`/`swift-run` receipt cannot PASS, no
matter what its summary says.
"""
from __future__ import annotations

import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import traceback

import vz04_candidate as candidate
import vz04_lanes as lanes
import vz04_schema as schema
from vz04_common import (DIGEST_PATTERN, LANE_PHASES, MAX_JSON, REPO_ROOT, RUN_ID_PATTERN, GateError, canonical_path,
                         digest_file, document, load_json, now_ns, require, sha256_bytes, tree_digest, which,
                         write_exclusive)

LANE = "native-macos"
ENTRY_POINT = "scripts/run-macos-developer-environment-e2e.sh"
E2E_SCRIPT = "scripts/run-installed-native-macos-e2e.py"
SCENARIO = "gate.native.target_native_execution"
CLEAN_PROVISION = "clean-provision"
EXIT_PASSED, EXIT_FAILED, EXIT_REJECTED, EXIT_NOT_IMPLEMENTED = 0, 1, 2, 3
REQUIRED_COMPONENTS = ("bin/vz", "bin/vz-runtimed")
GATE_OWNED_FILES = frozenset(("lane-result.json", "lane-result.rejected.json", "lane.stdout", "lane.stderr",
                              "invocation.json"))
E2E_SUBDIR = "e2e"
CATALOG_NAME = "machine-target-catalog.json"
CATALOG_LIMIT = 4 * 1024 * 1024
TEMPLATE_CHANNEL = "latest"
TEMPLATE_IMAGE = "vz-macos"
# The harness's own `cold-up` alone allows an hour, and a cold Swift build in a
# guest is not quick. Stay well inside `invoke_lane`'s six-hour lane deadline so
# a hung harness is reported by this lane as `timeout` rather than killed by the
# gate with no lane result at all.
E2E_TIMEOUT_SECONDS = 4 * 3600

# The scope `run-installed-native-macos-e2e.py` records for the run that
# actually boots a guest and exercises it. Its `--expect-preparation-failure`
# mode records `INSTALLED_NATIVE_CORRUPT_INPUT_DELETE` and *also* sets
# `passed: True`; accepting that scope would certify criterion 4 from a run that
# deliberately never started a Machine.
E2E_SCOPE = "INSTALLED_NATIVE_DEV_LOCAL_BUNDLE"
PROBE_PROTOCOL = "vz-native-macos-swift"
PROBE_VERSION = 1
HARDWARE_MODEL = "VirtualMac2,1"
# Every command `swift_build`/`swift_identity`/`swift_probe` runs in the guest,
# by the receipt name the harness writes. Each must exist and have exited 0.
# Cold identity and build/test/run prove the target-native toolchain compiled
# and ran the fixture; the warm pair proves it survived a Stop/Up.
SWIFT_RECEIPTS = ("swift-cold-receipt", "swift-cold-version", "swift-fixture-directory", "swift-fixture-transfer",
                  "swift-build", "swift-test", "swift-run", "swift-warm-receipt", "swift-warm-version",
                  "swift-persisted-run")
# Sources the harness tars into the guest. A run that transferred no test file
# cannot have run `swift test` against the fixture.
REQUIRED_SOURCE = "Package.swift"
PROHIBITED_KEYS = ("docker_desktop", "host_system_daemon", "runc", "crun", "cargo_run", "path_fallback", "ssh_hosts")


class Rejected(Exception):
    """Argument/contract/release admission failure (exit 2)."""


# --------------------------------------------------------------------------------------
# argv
# --------------------------------------------------------------------------------------
def scan_argv(argv: list) -> tuple:
    """{option: value} for `--k v` / `--k=v` pairs plus a list of problems."""
    options, problems = {}, []
    known = ("suite", *lanes.LANE_OPTIONS)
    index = 0
    while index < len(argv):
        item = argv[index]
        if not item.startswith("--"):
            problems.append(f"unexpected positional argument {item!r}")
            index += 1
            continue
        key, value = item[2:], None
        if "=" in key:
            key, value = key.split("=", 1)
        elif index + 1 < len(argv):
            value, index = argv[index + 1], index + 1
        else:
            problems.append(f"option --{key} lacks a value")
        index += 1
        if key not in known:
            problems.append(f"unknown option --{key}")
        elif key in options:
            problems.append(f"duplicate option --{key}")
        else:
            options[key] = value
    for key in known:
        if key not in options:
            problems.append(f"missing required option --{key}")
    return options, problems


def _matches(value, pattern: str) -> bool:
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


def _digest_or_empty(fn):
    try:
        return fn()
    except (GateError, OSError):
        return sha256_bytes(b"")


# --------------------------------------------------------------------------------------
# template discovery (read-only; never provisions)
# --------------------------------------------------------------------------------------
def catalog_locations(release_dir, home, environ=None) -> list:
    """Where a registered installed catalog may live, most explicit first.

    `VZ_MACHINE_TARGET_CATALOG` is the documented operator override
    (`docs/installed-machine-catalog.md`), and it is authoritative: "a present
    invalid override does not fall through to installed discovery". Honouring
    that here keeps the lane's view of which template is registered identical to
    the daemon's, and makes the absent-template path reproducible on a host that
    does have one registered. The gate hands lanes a minimal environment that
    does not carry the override, so under the gate the discoverable locations
    are the release candidate's own catalog and the standard installation prefix.
    """
    environ = os.environ if environ is None else environ
    locations = []
    override = environ.get("VZ_MACHINE_TARGET_CATALOG")
    if isinstance(override, str) and override.startswith("/"):
        return [Path(override)]
    if release_dir is not None:
        locations.append(Path(release_dir) / CATALOG_NAME)
    if home:
        locations.append(Path(home) / ".vz" / CATALOG_NAME)
    seen, unique = set(), []
    for path in locations:
        if str(path) not in seen:
            seen.add(str(path))
            unique.append(path)
    return unique


def template_from_catalog(path, channel=TEMPLATE_CHANNEL) -> tuple:
    """(template, notes) for one catalog path; template is None when it registers none.

    A template is only usable when the catalog entry still points at a bundle
    directory that holds the digest-named manifest it names. A registered entry
    whose bundle has been deleted is reported as a note, never silently skipped,
    because that is a different host state from "setup was never run".
    """
    notes = []
    if not path.is_file() or path.is_symlink():
        return None, [f"{path}: no catalog"]
    try:
        catalog = load_json(path, CATALOG_LIMIT)
    except (GateError, OSError, ValueError) as error:
        return None, [f"{path}: unreadable catalog: {error}"]
    entries = catalog.get("macos")
    if not isinstance(entries, list) or not entries:
        return None, [f"{path}: catalog registers no macOS template"]
    for index, entry in enumerate(entries):
        where = f"{path}[macos/{index}]"
        if not isinstance(entry, dict):
            notes.append(f"{where}: not an object")
            continue
        if entry.get("image") != TEMPLATE_IMAGE:
            notes.append(f"{where}: image is {entry.get('image')!r}, not {TEMPLATE_IMAGE!r}")
            continue
        channels = entry.get("channels")
        if not isinstance(channels, list) or channel not in channels:
            notes.append(f"{where}: channel {channel!r} not registered (channels={channels!r})")
            continue
        manifest = entry.get("manifest") if isinstance(entry.get("manifest"), dict) else {}
        digest = manifest.get("sha256")
        if not _matches(digest, DIGEST_PATTERN):
            notes.append(f"{where}: manifest sha256 is not a digest")
            continue
        bundle = entry.get("installed_bundle")
        if not isinstance(bundle, str) or not bundle.startswith("/"):
            notes.append(f"{where}: installed_bundle is not an absolute path")
            continue
        bundle_path = Path(bundle)
        if not bundle_path.is_dir() or bundle_path.is_symlink():
            notes.append(f"{where}: registered bundle directory is gone: {bundle}")
            continue
        manifest_path = bundle_path / digest
        if not manifest_path.is_file() or manifest_path.is_symlink():
            notes.append(f"{where}: bundle holds no manifest named {digest}")
            continue
        return ({"catalog": str(path), "bundle": str(bundle_path), "manifest_sha256": digest,
                 "version": entry.get("version"), "channels": sorted(channels)}, notes)
    return None, notes


def find_template(release_dir, home, environ=None, channel=TEMPLATE_CHANNEL) -> tuple:
    """(template or None, notes) across every discoverable catalog location."""
    notes = []
    for path in catalog_locations(release_dir, home, environ):
        template, more = template_from_catalog(path, channel)
        notes += more
        if template is not None:
            return template, notes
    return None, notes


# --------------------------------------------------------------------------------------
# harness evidence
# --------------------------------------------------------------------------------------
def read_e2e_evidence(evidence_dir) -> tuple:
    """(summary or None, {receipt name: receipt}) from a harness evidence directory."""
    evidence_dir = Path(evidence_dir)
    summary = None
    summary_path = evidence_dir / "summary.json"
    if summary_path.is_file() and not summary_path.is_symlink():
        try:
            summary = load_json(summary_path, MAX_JSON)
        except (GateError, OSError, ValueError):
            summary = None
    if not isinstance(summary, dict):
        summary = None
    receipts = {}
    if evidence_dir.is_dir():
        for path in sorted(evidence_dir.glob("*.json")):
            if path.name == "summary.json" or path.is_symlink() or not path.is_file():
                continue
            try:
                row = load_json(path, MAX_JSON)
            except (GateError, OSError, ValueError):
                continue
            if isinstance(row, dict) and isinstance(row.get("name"), str) and isinstance(row.get("argv"), list):
                receipts[row["name"]] = row
    return summary, receipts


def prohibited_observed(receipts) -> dict:
    """`prohibited_observed` derived from the harness's own per-command receipts.

    Every command the harness runs writes an fsync'd receipt naming its argv, so
    this reads what actually executed rather than asserting the lane's intent.
    """
    flags = {key: False for key in PROHIBITED_KEYS}
    for row in receipts.values():
        argv = [str(item) for item in row.get("argv") or []]
        if not argv:
            continue
        head = argv[0]
        if not head.startswith("/"):
            flags["path_fallback"] = True
        if Path(head).name == "cargo" and argv[1:2] == ["run"]:
            flags["cargo_run"] = True
        for item in argv:
            name = Path(item).name
            if name == "runc":
                flags["runc"] = True
            if name == "crun":
                flags["crun"] = True
            if item.startswith("ssh://"):
                flags["ssh_hosts"] = True
            lowered = item.lower()
            if "docker.app" in lowered or "/.docker/run/docker.sock" in lowered:
                flags["docker_desktop"] = True
            if item in ("unix:///var/run/docker.sock", "/var/run/docker.sock"):
                flags["host_system_daemon"] = True
    return flags


def _probe_problems(label, probe, native) -> list:
    if not isinstance(probe, dict):
        return [f"{label} is missing from the harness summary"]
    problems = []
    if probe.get("protocol") != PROBE_PROTOCOL:
        problems.append(f"{label}.protocol is {probe.get('protocol')!r}, not {PROBE_PROTOCOL!r}")
    if probe.get("protocol_version") != PROBE_VERSION:
        problems.append(f"{label}.protocol_version is {probe.get('protocol_version')!r}, not {PROBE_VERSION}")
    if probe.get("hardware_model") != HARDWARE_MODEL:
        problems.append(f"{label}.hardware_model is {probe.get('hardware_model')!r}, not {HARDWARE_MODEL!r}")
    pid = probe.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 1:
        problems.append(f"{label}.pid is {probe.get('pid')!r}, not a real guest process id")
    expected_version, expected_build = native.get("guest_version"), native.get("guest_build")
    if expected_version and probe.get("os_version") != expected_version:
        problems.append(f"{label}.os_version is {probe.get('os_version')!r}, not the pinned {expected_version!r}")
    if expected_build and probe.get("os_build") != expected_build:
        problems.append(f"{label}.os_build is {probe.get('os_build')!r}, not the pinned {expected_build!r}")
    return problems


def summary_passed(summary) -> bool:
    """The harness's own verdict, read strictly: `passed` must be exactly `True`.

    Separate from `swift_claims` on purpose. `translate` consults both, so a
    single mistake in the claim derivation cannot on its own manufacture a PASS
    for a run the harness itself said had failed.
    """
    return isinstance(summary, dict) and summary.get("passed") is True


def harness_root(summary):
    """The temporary root the harness kept, as it recorded it in its own summary."""
    if not isinstance(summary, dict):
        return None
    root = summary.get("root")
    return root if isinstance(root, str) and root.startswith("/") else None


def swift_claims(summary, receipts, native) -> tuple:
    """(assertions, problems) for `gate.native.target_native_execution`.

    Re-derives criterion 4 from the retained harness evidence rather than
    trusting the harness's own verdict. `problems` empty is the only thing that
    may become PASS.
    """
    native = native or {}
    if not isinstance(summary, dict):
        return [], ["the harness wrote no readable summary.json"]
    assertions, problems = [], []
    if summary.get("passed") is not True:
        problems.append("harness summary reports passed=" + repr(summary.get("passed")) +
                        (": " + str(summary.get("error"))[:400] if summary.get("error") else ""))
    scope = summary.get("scope")
    if scope != E2E_SCOPE:
        problems.append(f"harness scope is {scope!r}, not the guest-exercising {E2E_SCOPE!r}")
    else:
        assertions.append(f"harness scope {E2E_SCOPE}: a native macOS Machine was booted and exercised")
    if summary.get("aggregate_release_certified") is not False:
        problems.append("harness summary claims aggregate release certification, which no lane may assert")

    swift = summary.get("swift") if isinstance(summary.get("swift"), dict) else None
    if swift is None:
        problems.append("harness summary carries no `swift` block; the run was not --require-swift")
        return assertions, problems

    pinned = native.get("xcode_toolchain_sha256")
    observed = swift.get("toolchain_sha256")
    if not _matches(observed, DIGEST_PATTERN):
        problems.append(f"swift.toolchain_sha256 is {observed!r}, not a digest")
    elif pinned and observed != pinned:
        problems.append(f"swift.toolchain_sha256 {observed} is not the contract-pinned {pinned}")
    elif pinned:
        assertions.append(f"guest toolchain receipt matches the contract pin {pinned}")

    identity = swift.get("identity") if isinstance(swift.get("identity"), dict) else {}
    for field in ("swift_version", "sdk_version"):
        if not isinstance(identity.get(field), str) or not identity[field].strip():
            problems.append(f"swift.identity.{field} is missing")
    if not problems:
        assertions.append("guest reported swift " + " / SDK ".join(
            [identity["swift_version"].splitlines()[0], identity["sdk_version"]]))

    sources = swift.get("source_sha256") if isinstance(swift.get("source_sha256"), dict) else {}
    if REQUIRED_SOURCE not in sources:
        problems.append(f"swift.source_sha256 does not record {REQUIRED_SOURCE}; no fixture was transferred")
    if not any(name.startswith("Sources/") for name in sources):
        problems.append("swift.source_sha256 records no Sources/ file")
    if not any(name.startswith("Tests/") for name in sources):
        problems.append("swift.source_sha256 records no Tests/ file")
    if not problems:
        assertions.append(f"{len(sources)} fixture sources transferred into the guest and digested")

    problems += _probe_problems("swift.probe", swift.get("probe"), native)
    problems += _probe_problems("swift.persisted_probe", swift.get("persisted_probe"), native)
    if not problems:
        assertions.append(f"compiled Swift probe reported {PROBE_PROTOCOL} v{PROBE_VERSION} on {HARDWARE_MODEL}, "
                          "both before and after a Stop/Up cycle")

    missing = [name for name in SWIFT_RECEIPTS if name not in receipts]
    if missing:
        problems.append("harness left no receipt for: " + ", ".join(missing))
    nonzero = sorted(name for name in SWIFT_RECEIPTS
                     if name in receipts and receipts[name].get("exit_code") != 0)
    if nonzero:
        problems.append("guest command receipts report a non-zero exit: " + ", ".join(nonzero))
    if not missing and not nonzero:
        assertions.append("every guest Swift command exited 0: " + ", ".join(SWIFT_RECEIPTS))
    return assertions, problems


# --------------------------------------------------------------------------------------
# translation
# --------------------------------------------------------------------------------------
class E2eOutcome:
    """What the lane observed of one attempt to run the hardware harness.

    Exactly one of the three states is expressible: the harness was never
    invoked because a prerequisite was absent (`prerequisite`), the lane itself
    broke (`harness_error`), or the harness ran (`invoked`, with `exit_code`
    None meaning it exceeded the lane's deadline).
    """

    def __init__(self, *, invoked=False, exit_code=None, summary=None, receipts=None, started_unix_ns=0,
                 ended_unix_ns=0, prerequisite=None, harness_error=None, retained_root=None, evidence_files=(),
                 detail_suffix=""):
        self.invoked = bool(invoked)
        self.exit_code = exit_code
        self.summary = summary
        self.receipts = dict(receipts or {})
        self.started_unix_ns = int(started_unix_ns)
        self.ended_unix_ns = int(ended_unix_ns)
        self.prerequisite = prerequisite
        self.harness_error = harness_error
        self.retained_root = retained_root
        self.evidence_files = list(evidence_files)
        self.detail_suffix = detail_suffix


def scenario_row(outcome, status, assertions) -> dict:
    started = outcome.started_unix_ns if outcome.started_unix_ns > 0 else now_ns()
    ended = outcome.ended_unix_ns if outcome.ended_unix_ns >= started else started
    evidence = sorted(path for path in outcome.evidence_files if path.startswith(E2E_SUBDIR + "/"))
    return {"id": SCENARIO, "status": status, "started_unix_ns": started, "ended_unix_ns": ended,
            "assertions": list(assertions), "evidence": evidence, "readiness_polls": []}


def translate(base, outcome, native) -> tuple:
    """(lane result, exit code) for the `clean-provision` phase.

    `base` is a `vz04_lanes.base_result` skeleton, so identity and digests come
    from the gate rather than from anything the harness wrote.
    """
    result = dict(base)
    retained = outcome.retained_root if isinstance(outcome.retained_root, str) and \
        outcome.retained_root.startswith("/") else harness_root(outcome.summary)
    result["retained_root"] = retained
    result["evidence_files"] = sorted(outcome.evidence_files)
    result["prohibited_observed"] = prohibited_observed(outcome.receipts)

    def fail(reason, detail, exit_code, assertions):
        result["outcome"] = "failed"
        result["scenarios"] = [scenario_row(outcome, "FAIL", assertions)]
        result["failure"] = {"reason": reason, "detail": (detail + outcome.detail_suffix)[:2000] or reason,
                             "exit_code": exit_code}
        return result, EXIT_FAILED

    if outcome.prerequisite is not None:
        return fail("prerequisite", "native macOS harness not run: " + str(outcome.prerequisite), EXIT_FAILED,
                    ["prerequisite absent, so criterion 4 was not exercised: " + str(outcome.prerequisite)])
    if outcome.harness_error is not None:
        return fail("crash", "native-macos lane broke while driving the harness: " + str(outcome.harness_error),
                    EXIT_FAILED, ["lane crashed before criterion 4 could be judged: " + str(outcome.harness_error)])
    if not outcome.invoked:
        return fail("crash", "native-macos lane produced no harness invocation and named no prerequisite",
                    EXIT_FAILED, ["lane reported neither a prerequisite nor a harness run"])
    if outcome.exit_code is None:
        return fail("timeout", f"native macOS harness exceeded the lane deadline of {E2E_TIMEOUT_SECONDS}s",
                    None, ["harness exceeded the lane deadline; criterion 4 unproven"])

    assertions, problems = swift_claims(outcome.summary, outcome.receipts, native)
    # Deliberately redundant with the check inside `swift_claims`: two
    # independent readers of the harness's own verdict, so no single bug in the
    # claim derivation can certify a run the harness reported as failed.
    if not summary_passed(outcome.summary):
        problems.append("harness summary does not report passed=True")
    flagged = sorted(key for key, value in result["prohibited_observed"].items() if value)
    if flagged:
        problems.append("prohibited component observed in harness receipts: " + ", ".join(flagged))
    if outcome.exit_code != 0:
        problems.append(f"harness exited {outcome.exit_code}")
    if problems:
        return fail("assertion", "native macOS harness did not prove criterion 4: " + "; ".join(problems),
                    outcome.exit_code, problems + assertions)
    result["outcome"] = "passed"
    result["failure"] = None
    result["scenarios"] = [scenario_row(outcome, "PASS", assertions)]
    return result, EXIT_PASSED


# --------------------------------------------------------------------------------------
# lane
# --------------------------------------------------------------------------------------
class Lane:
    def __init__(self, argv: list, *, repo_root: Path, codesign_verifier, runner=None, environ=None):
        self.argv = [str(a) for a in argv]
        self.repo_root = Path(repo_root)
        self.codesign_verifier = codesign_verifier
        self.runner = runner or run_harness
        self.environ = os.environ if environ is None else environ
        self.options, self.problems = scan_argv(self.argv)
        self.evidence_dir = None
        self.ctx = None
        self.entry = None
        self.contract = None
        self.release = None
        self.phase = self.options.get("phase")

    def can_emit(self) -> bool:
        o = self.options
        evidence = o.get("evidence-dir")
        return (_matches(o.get("run-id"), RUN_ID_PATTERN) and _matches(o.get("candidate-tuple"), DIGEST_PATTERN) and
                _matches(o.get("fixture-sha256"), DIGEST_PATTERN) and self.phase in LANE_PHASES and
                isinstance(evidence, str) and evidence.startswith("/") and
                not any(c in evidence for c in "\r\n\x00"))

    def prepare_identity(self) -> None:
        o = self.options
        self.evidence_dir = Path(o["evidence-dir"])
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        contract_path = Path(o["contract"]) if isinstance(o.get("contract"), str) else \
            self.repo_root / "config/vz-0.4-e2e-contract.json"
        release_dir = Path(o["release-dir"]) if isinstance(o.get("release-dir"), str) else Path("/nonexistent")
        self.ctx = lanes.LaneContext(
            run_id=o["run-id"], release_dir=release_dir,
            release_dir_sha256=_digest_or_empty(lambda: tree_digest(canonical_path(release_dir))),
            state_root=o.get("state-root") or "/nonexistent", contract_path=contract_path,
            contract_sha256=_digest_or_empty(lambda: digest_file(contract_path)),
            candidate_tuple_sha256=o["candidate-tuple"], fixture_sha256=o["fixture-sha256"],
            clients={"docker": o.get("docker"), "compose_plugin": o.get("compose-plugin"),
                     "buildx_plugin": o.get("buildx-plugin")},
            repo_root=self.repo_root)
        entry_path = self.repo_root / ENTRY_POINT
        self.entry = {"path": ENTRY_POINT,
                      "sha256": digest_file(entry_path) if entry_path.is_file() else sha256_bytes(b""),
                      "argv": self.argv}

    def write_result(self, result: dict) -> None:
        schema.require_valid("lane-result", result, self.repo_root)
        document(self.evidence_dir / "lane-result.json", result, replace=True)

    def failed(self, reason, detail, exit_code, *, scenarios=None, extra=None) -> dict:
        result = lanes.failed_result(LANE, self.phase, self.ctx, self.entry, reason, detail, exit_code)
        result["scenarios"] = scenarios or []
        for key, value in (extra or {}).items():
            result[key] = value
        return result

    def evidence_files(self) -> list:
        files = []
        if self.evidence_dir is None or not self.evidence_dir.is_dir():
            return files
        for path in sorted(self.evidence_dir.rglob("*")):
            if path.is_file() and not path.is_symlink():
                relative = path.relative_to(self.evidence_dir).as_posix()
                if relative not in GATE_OWNED_FILES:
                    files.append(relative)
        return files

    def admit(self) -> None:
        if self.problems:
            raise Rejected("; ".join(self.problems))
        o = self.options
        require(o["suite"] == "all", "only --suite all is accepted")
        contract_path = canonical_path(o["contract"])
        self.contract = load_json(contract_path)
        problems = schema.validate("e2e-contract", self.contract, self.repo_root)
        require(not problems, "contract rejected: " + "; ".join(problems))
        declared = [entry for entry in self.contract["lanes"] if entry["name"] == LANE]
        require(len(declared) == 1, "contract does not declare exactly one native-macos lane")
        require(self.phase in declared[0]["phases"], f"phase {self.phase} is not assigned to the native-macos lane")
        require(declared[0]["entry_point"] == ENTRY_POINT, "contract entry point differs from this script's wrapper")
        state_root = Path(o["state-root"])
        require(state_root.is_absolute() and not any(c in str(state_root) for c in "\r\n\x00"),
                "absolute clean --state-root required")
        state_root.mkdir(parents=True, exist_ok=True)
        require(canonical_path(state_root) == state_root, "canonical --state-root required")
        for key in ("docker", "compose-plugin", "buildx-plugin"):
            require(o[key] == "none" or (o[key].startswith("/") and Path(o[key]).is_file()),
                    f"--{key} must be none or an existing absolute path")
        require(o["handoff"] == "none" or (o["handoff"].startswith("/") and Path(o["handoff"]).is_file()),
                "--handoff must be none or an existing absolute file")
        self.release = candidate.admit_release_dir(o["release-dir"], repo_root=self.repo_root,
                                                   codesign_verifier=self.codesign_verifier)
        for relative in REQUIRED_COMPONENTS:
            require(relative in self.release["components"], f"release manifest lacks component {relative}")
            path = Path(self.release["dir"]) / relative
            require(os.access(path, os.X_OK), f"release component not executable: {relative}")
        self.ctx.release_dir = Path(self.release["dir"])
        self.ctx.release_dir_sha256 = self.release["release_dir_sha256"]
        self.ctx.contract_sha256 = digest_file(contract_path)

    def native_pins(self) -> dict:
        block = (self.contract or {}).get("native_macos")
        return block if isinstance(block, dict) else {}

    def handoff_record(self) -> dict:
        handoff = self.options["handoff"]
        if handoff == "none":
            return {"produced": None, "consumed": None, "consumed_sha256": None}
        path = Path(handoff)
        return {"produced": None, "consumed": path.name, "consumed_sha256": digest_file(path)}

    def run(self) -> int:
        if self.phase != CLEAN_PROVISION:
            return self.run_unassigned_phase()
        return self.run_clean_provision()

    def run_unassigned_phase(self) -> int:
        """No required scenario is assigned to this lane outside clean-provision."""
        result = self.failed(
            "not_implemented",
            f"the native-macos lane owns only {SCENARIO} at {CLEAN_PROVISION}; the contract assigns no scenario to "
            f"{self.phase}, so this phase exercises nothing and claims nothing",
            EXIT_NOT_IMPLEMENTED,
            extra={"handoff": self.handoff_record(), "evidence_files": self.evidence_files()})
        self.write_result(result)
        return EXIT_NOT_IMPLEMENTED

    def preflight(self):
        """The prerequisite that is absent, or (template, harness path, tmux) when all are present."""
        harness = self.repo_root / E2E_SCRIPT
        if not harness.is_file() or harness.is_symlink():
            return f"harness {E2E_SCRIPT} is missing from the source tree", None
        tmux = which("tmux") or shutil.which("tmux", path=os.pathsep.join(
            [self.environ.get("PATH", os.defpath), "/opt/homebrew/bin", "/usr/local/bin"]))
        if not tmux:
            return "tmux is required by the native macOS harness and is not installed", None
        fixture = self.repo_root / "tests/fixtures/vz-0.4/native-macos-swift"
        if not fixture.is_dir():
            return "the Swift fixture tests/fixtures/vz-0.4/native-macos-swift is missing", None
        template, notes = find_template(self.ctx.release_dir, self.environ.get("HOME"), self.environ)
        if template is None:
            return ("no installed machine-target catalog registers a native macOS template; run the maintainer "
                    "recipe planning/developer-environments/macos-local-setup.md (vz-macos-setup) on this host "
                    "first. This lane never provisions one: setup downloads an Apple IPSW and takes an "
                    "administrator authorisation. Inspected: " + ("; ".join(notes) if notes else "nothing")), None
        return None, {"template": template, "harness": harness, "tmux": tmux}

    def run_clean_provision(self) -> int:
        native = self.native_pins()
        harness_evidence = self.evidence_dir / E2E_SUBDIR
        started = now_ns()
        try:
            missing, ready = self.preflight()
        except (GateError, OSError) as error:
            missing, ready = f"prerequisite discovery failed: {error}", None
        if missing is not None:
            write_exclusive(self.evidence_dir / "prerequisite.txt", (missing + "\n").encode())
            outcome = E2eOutcome(prerequisite=missing, started_unix_ns=started, ended_unix_ns=now_ns(),
                                 evidence_files=self.evidence_files())
            result, code = translate(self.base(), outcome, native)
            self.write_result(result)
            print(f"native-macos lane {self.phase}: outcome={result['outcome']} reason=prerequisite {missing}",
                  file=sys.stderr)
            return code

        write_exclusive(self.evidence_dir / "template.txt", (
            "catalog: {catalog}\nbundle: {bundle}\nmanifest_sha256: {manifest_sha256}\n"
            "version: {version}\nchannels: {channels}\n".format(**ready["template"])).encode())
        outcome = self.drive(ready, harness_evidence, started)
        result, code = translate(self.base(), outcome, native)
        self.write_result(result)
        reason = None if result["failure"] is None else result["failure"]["reason"]
        print(f"native-macos lane {self.phase}: outcome={result['outcome']} reason={reason}", file=sys.stderr)
        return code

    def base(self) -> dict:
        result = lanes.base_result(LANE, self.phase, self.ctx, self.entry)
        result["handoff"] = self.handoff_record()
        return result

    def drive(self, ready, harness_evidence: Path, started: int) -> E2eOutcome:
        """Run the hardware harness once and read back what it left."""
        template = ready["template"]
        argv = [sys.executable, str(ready["harness"]),
                "--release-dir", str(self.ctx.release_dir / "bin"),
                "--bundle", template["bundle"],
                "--manifest", template["manifest_sha256"],
                "--channel", TEMPLATE_CHANNEL,
                "--require-swift",
                "--evidence", str(harness_evidence)]
        env = {"PATH": self.environ.get("PATH", "/usr/bin:/bin:/usr/sbin:/sbin"),
               "HOME": self.environ.get("HOME", "/"), "LC_ALL": "C", "NO_COLOR": "1",
               "TMPDIR": self.environ.get("TMPDIR", "/private/tmp")}
        exit_code, harness_error = None, None
        stdout_path, stderr_path = self.evidence_dir / "harness.stdout", self.evidence_dir / "harness.stderr"
        try:
            with open(stdout_path, "xb") as out, open(stderr_path, "xb") as err:
                completed = self.runner(argv, cwd=str(self.repo_root), env=env, stdout=out, stderr=err,
                                        timeout=E2E_TIMEOUT_SECONDS)
            exit_code = None if completed is None else completed.returncode
        except subprocess.TimeoutExpired:
            exit_code = None
        except (Exception, KeyboardInterrupt):  # noqa: BLE001 - recorded as a crash, never swallowed
            harness_error = traceback.format_exc().strip().splitlines()[-1][:400]
            write_exclusive(self.evidence_dir / "crash.txt", traceback.format_exc().encode())
        summary, receipts = read_e2e_evidence(harness_evidence)
        if harness_error is None and exit_code is not None and summary is None:
            harness_error = f"harness exited {exit_code} without writing {E2E_SUBDIR}/summary.json"
        retained = harness_root(summary)
        document(self.evidence_dir / "harness-invocation.json",
                 {"argv": argv, "started_unix_ns": started, "ended_unix_ns": now_ns(), "exit_code": exit_code,
                  "template": template}, replace=True)
        return E2eOutcome(invoked=True, exit_code=exit_code, summary=summary, receipts=receipts,
                          started_unix_ns=started, ended_unix_ns=now_ns(), harness_error=harness_error,
                          retained_root=retained, evidence_files=self.evidence_files())


def run_harness(argv, *, cwd, env, stdout, stderr, timeout):
    return subprocess.run(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                          timeout=timeout, check=False)


def main(argv=None, *, repo_root: Path = REPO_ROOT, codesign_verifier=candidate.run_codesign_verify,
         runner=None, environ=None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    lane = Lane(argv, repo_root=repo_root, codesign_verifier=codesign_verifier, runner=runner, environ=environ)
    if not lane.can_emit():
        print("native-macos lane rejected input before a lane result could be written: " +
              "; ".join(lane.problems or
                        ["run-id/candidate-tuple/fixture-sha256/phase/evidence-dir identity invalid"]),
              file=sys.stderr)
        return EXIT_REJECTED
    lane.prepare_identity()
    try:
        lane.admit()
    except (Rejected, GateError, OSError) as error:
        lane.write_result(lane.failed("input_rejected", f"native-macos lane rejected input: {error}", EXIT_REJECTED))
        print(f"native-macos lane rejected input: {error}", file=sys.stderr)
        return EXIT_REJECTED
    try:
        return lane.run()
    except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - a broken lane must still write a result
        detail = f"{type(error).__name__}: {error}"
        try:
            lane.write_result(lane.failed("crash", f"native-macos lane crashed: {detail}", EXIT_FAILED,
                                          extra={"evidence_files": lane.evidence_files()}))
        except Exception:  # noqa: BLE001 - nothing further can be recorded
            pass
        print(f"native-macos lane crashed: {detail}", file=sys.stderr)
        return EXIT_FAILED


if __name__ == "__main__":
    raise SystemExit(main())
