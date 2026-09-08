"""linux-docker lane-result writer: schema-shaped `lane-result.json` for the aggregate gate.

Only an aggregate-gate invocation of `scripts/run-linux-docker-e2e.sh` writes a
lane result. The gate is recognised by the simultaneous presence of `--run-id`,
`--phase` and `--candidate-tuple` (`gate_context`); a standalone DEV run
(`--suite <name>` without those) writes nothing new and keeps its early-exit
semantics untouched.

The lane result mirrors `schemas/vz-0.4-lane-result.schema.json` field by
field. `scenarios[]` is the DEV per-scenario surface produced by
`linux_docker_scenarios.lane_scenarios`; PASS there is a DEV observation of a
fully proven manifest `expected` block, never release certification -- the
aggregate validator (`vz04_lanes.account`) decides. `release_scenarios_passed`
in the harness `result.json` stays `[]`.

The gate invokes the composed run (`--suite all`); its result carries every
executed suite's Machine slices under `scenario.suite_slices`, and each suite
contributes the scenarios of its own claims to the one lane result.

Failure mapping (`failure_reason`):
  argument/preflight rejection       -> input_rejected
  harness `cleanup_errors` non-empty -> cleanup
  harness `error` mentioning an uncertain mutation -> uncertain_effects
  harness `error` from KeyboardInterrupt/Timeout    -> crash/timeout
  any other harness `error`          -> assertion
`test_case_retries` is the constant 0; `leaks` is always `[]` (the orchestrator
diffs host inventory itself); `prohibited_observed` is derived from the retained
guard proofs (Engine `info` JSON: DefaultRuntime youki, inert runc metadata only,
no crun; context endpoints: private unix sockets, no Desktop/system daemon/ssh;
host receipts: absolute executables, no cargo). A passed run without a single
retained runtime guard proof is downgraded to a failed `assertion` result.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re

import docker_host_driver as driver
import linux_docker_scenarios as scenarios
from vz04_common import LANE_PHASES, REPO_ROOT, digest_file, document, sha256_bytes, tree_digest

LANE = "linux-docker"
ENTRY_POINT = "scripts/run-linux-docker-e2e.sh"
RESULT_NAME = "lane-result.json"
HARNESS_SUBDIR = "harness"
COMPOSED_SUITE = "all"
RUN_DOCUMENTS = ("result.json", "run-info.json", "checksums.sha256")
GATE_TRIO = ("run-id", "phase", "candidate-tuple")
GATE_OPTIONS = ("phase", "contract", "candidate-tuple", "fixture-sha256", "handoff", "state-root")
SCANNED_OPTIONS = (*GATE_TRIO, *GATE_OPTIONS, "evidence-dir", "release-dir", "suite")
RUN_ID = r"[a-z0-9][a-z0-9-]{7,63}"
DIGEST = r"[0-9a-f]{64}"
RECEIPT_LIMIT = 4 * 1024 * 1024
# One line per digested file: a composed run digests tens of thousands of them.
CHECKSUMS_LIMIT = 64 * 1024 * 1024
# A composed run's result carries every suite's evidence at once — the whole
# continuous-sentinel sample series, the SSH cache proofs, the registry session
# records — and runs to tens of megabytes, where a single-suite result is small.
# Bounding it at the per-receipt limit rejected the only result that matters.
RESULT_LIMIT = 128 * 1024 * 1024
STREAM_LIMIT = 8 * 1024 * 1024
HARNESS_RECEIPT = re.compile(r"^\d{3}-.+\.intent\.json$")
DRIVER_RECEIPT = re.compile(r"^command-\d{5}\.intent\.json$")
EXCLUDED_RECEIPT_DIRS = frozenset(("runtime-receipts",))
PROHIBITED_KEYS = ("docker_desktop", "host_system_daemon", "runc", "crun", "cargo_run", "path_fallback", "ssh_hosts")
INERT_RUNTIME_METADATA = {"path": "runc"}
require = driver.require


def scan_options(argv, names=SCANNED_OPTIONS):
    """Raw `--name value` / `--name=value` scan; argparse-independent so rejected argv still yields a context."""
    found = {}
    items = [str(item) for item in argv]
    for index, item in enumerate(items):
        for name in names:
            if item == "--" + name and index + 1 < len(items):
                found.setdefault(name, items[index + 1])
            elif item.startswith("--" + name + "="):
                found.setdefault(name, item[len(name) + 3:])
    return found


class GateContext:
    """Gate-provided identity for one lane invocation, taken only from argv."""

    def __init__(self, options, argv):
        self.argv = [str(item) for item in argv]
        self.run_id = options.get("run-id")
        self.phase = options.get("phase")
        self.candidate_tuple = options.get("candidate-tuple")
        self.contract = options.get("contract")
        self.fixture_sha256 = options.get("fixture-sha256")
        self.handoff = options.get("handoff")
        self.state_root = options.get("state-root")
        self.evidence_dir = options.get("evidence-dir")
        self.release_dir = options.get("release-dir")
        self.suite = options.get("suite")

    def validate(self):
        """All gate options are required together; values are checked before any harness work."""
        missing = [name for name in ("run-id", *GATE_OPTIONS, "evidence-dir", "release-dir")
                   if getattr(self, name.replace("-", "_")) is None]
        require(not missing, "gate options are all-or-nothing; missing: " + ", ".join("--" + name for name in missing))
        driver.checked_text(self.run_id, RUN_ID, "gate run ID")
        require(self.phase in LANE_PHASES, "unknown lane phase: " + repr(self.phase))
        driver.checked_text(self.candidate_tuple, DIGEST, "candidate tuple digest")
        driver.checked_text(self.fixture_sha256, DIGEST, "fixture digest")
        contract = Path(self.contract)
        require(contract.is_absolute() and contract.is_file() and not contract.is_symlink(), "gate contract must be an absolute regular file")
        for name in ("evidence_dir", "state_root", "release_dir"):
            path = Path(getattr(self, name))
            require(path.is_absolute() and not any(c in str(path) for c in "\r\n\x00"), "--" + name.replace("_", "-") + " must be absolute")
        require(self.handoff == "none" or (Path(self.handoff).is_absolute() and Path(self.handoff).is_file()),
                "--handoff must be 'none' or an absolute regular file")
        return self

    def lane_dir(self):
        return Path(self.evidence_dir)

    def harness_dir(self):
        """Fresh child for the harness: the gate pre-creates the lane directory itself."""
        return self.lane_dir() / HARNESS_SUBDIR

    def result_path(self):
        return self.lane_dir() / RESULT_NAME


def gate_context(argv):
    """GateContext when --run-id AND --phase AND --candidate-tuple are present; else None."""
    options = scan_options(argv)
    if not all(name in options for name in GATE_TRIO):
        return None
    return GateContext(options, argv)


def entry_point(ctx, repo_root=REPO_ROOT):
    path = repo_root / ENTRY_POINT
    digest = digest_file(path) if path.is_file() and not path.is_symlink() else sha256_bytes(b"")
    return {"path": ENTRY_POINT, "sha256": digest, "argv": list(ctx.argv)}


def _optional_digest(value):
    return value if isinstance(value, str) and re.fullmatch(DIGEST, value) else sha256_bytes(b"")


def _handoff(ctx):
    if not ctx.handoff or ctx.handoff == "none":
        return {"produced": None, "consumed": None, "consumed_sha256": None}
    path = Path(ctx.handoff)
    digest = digest_file(path) if path.is_file() and not path.is_symlink() else None
    return {"produced": None, "consumed": path.name, "consumed_sha256": digest}


def base(ctx, repo_root=REPO_ROOT):
    """Schema-shaped skeleton: outcome failed, failure null, nothing claimed."""
    release_dir = Path(ctx.release_dir) if ctx.release_dir else None
    release_digest = sha256_bytes(b"")
    if release_dir is not None and release_dir.is_dir() and not release_dir.is_symlink():
        try:
            release_digest = tree_digest(release_dir)
        except Exception:  # noqa: BLE001 -- an unreadable release tree is recorded as the empty digest, never guessed
            release_digest = sha256_bytes(b"")
    contract = Path(ctx.contract) if ctx.contract else None
    contract_digest = digest_file(contract) if contract is not None and contract.is_file() and not contract.is_symlink() else sha256_bytes(b"")
    phase = ctx.phase if ctx.phase in LANE_PHASES else LANE_PHASES[0]
    run_id = ctx.run_id if isinstance(ctx.run_id, str) and re.fullmatch(RUN_ID, ctx.run_id) else "invalid-run-id"
    return {"schema_version": 1, "kind": "vz-0.4-lane-result", "lane": LANE, "phase": phase, "run_id": run_id,
            "candidate_tuple_sha256": _optional_digest(ctx.candidate_tuple), "release_dir_sha256": release_digest,
            "fixture_sha256": _optional_digest(ctx.fixture_sha256), "contract_sha256": contract_digest,
            "entry_point": entry_point(ctx, repo_root), "outcome": "failed", "failure": None, "scenarios": [],
            "test_case_retries": 0, "process_starts": [], "prohibited_observed": {key: False for key in PROHIBITED_KEYS},
            "leaks": [], "cleanup_errors": [], "handoff": _handoff(ctx), "retained_root": None, "evidence_files": [],
            "result_adapter": None}


def failed(ctx, reason, detail, exit_code, repo_root=REPO_ROOT):
    result = base(ctx, repo_root)
    result["failure"] = {"reason": reason, "detail": str(detail)[:2000] or reason, "exit_code": exit_code}
    return result


def failure_reason(error, cleanup_errors):
    """Map the harness result's `error`/`cleanup_errors` to a typed lane failure."""
    if cleanup_errors:
        return "cleanup", "; ".join(cleanup_errors) + ("; error: " + error if error else "")
    text = error or "harness result reports no outcome"
    lowered = text.lower()
    if "uncertain" in lowered:
        return "uncertain_effects", text
    if text.startswith("KeyboardInterrupt"):
        return "crash", text
    if text.startswith("TimeoutExpired") or "timed out" in lowered or "timeout expired" in lowered:
        return "timeout", text
    return "assertion", text


def _read_json(path, limit):
    try:
        return json.loads(driver.regular(path, limit))
    except (driver.Rejected, OSError, ValueError):
        return None


def load_result(harness_dir):
    """The harness `result.json` a completed suite run leaves in its evidence dir.

    A rejected read must say so here. `_read_json` returns None on any failure,
    and passing that on turned "the result exceeded its bound" into an
    AttributeError three frames away, in the one path that only ever runs after
    a suite has actually completed.
    """
    path = Path(harness_dir) / "result.json"
    require(path.is_file() and not path.is_symlink(), "harness result.json is missing: " + str(path))
    result = _read_json(path, RESULT_LIMIT)
    require(isinstance(result, dict),
            "harness result.json is unreadable or exceeds its %d byte bound: %s is %d bytes"
            % (RESULT_LIMIT, path, path.stat().st_size))
    return result


def receipts(harness_dir):
    """Every host process the harness started, from fsync'd intent receipts (label, argv0, executable, start)."""
    rows = []
    if not harness_dir.is_dir():
        return rows
    for root, directories, files in os.walk(harness_dir):
        directories[:] = sorted(d for d in directories if d not in EXCLUDED_RECEIPT_DIRS and not Path(root, d).is_symlink())
        for name in sorted(files):
            if not (HARNESS_RECEIPT.match(name) or DRIVER_RECEIPT.match(name)):
                continue
            path = Path(root) / name
            if path.is_symlink():
                continue
            row = _read_json(path, RECEIPT_LIMIT)
            if not isinstance(row, dict) or not isinstance(row.get("argv0"), str):
                continue
            rows.append({"path": path.relative_to(harness_dir).as_posix(), "label": row.get("label"),
                         "argv0": row["argv0"], "executable": row.get("executable"), "argv": row.get("argv") or [],
                         "started_unix_ns": row.get("started_unix_ns"), "pid": row.get("pid") if isinstance(row.get("pid"), int) else None})
    return rows


def _earliest(rows):
    ordered = sorted(rows, key=lambda row: row["started_unix_ns"] if isinstance(row["started_unix_ns"], int) and
                     row["started_unix_ns"] >= 0 else float("inf"))
    return ordered[0] if ordered else None


def process_starts(rows, suites):
    """One row per suite: the first host process its own Machine evidence recorded.

    A Docker suite starts thousands of host processes for one accounting
    scenario, so listing them all would report that scenario as started many
    times, which the aggregate validator rejects. The suite accounts for its host
    work once, and `prohibited_observed` is what reads every receipt.
    """
    starts = []
    for suite in suites:
        prefix = scenarios.machine_prefix(suite)
        row = _earliest([row for row in rows if row["path"].startswith(prefix)]) or _earliest(rows)
        if row is None:
            continue
        starts.append({"scenario_id": scenarios.SUITES[suite].process_scenario, "argv0": row["argv0"],
                       "pid": row["pid"] if row["pid"] and row["pid"] >= 1 else None})
    return starts


def run_window(rows):
    starts = [row["started_unix_ns"] for row in rows if isinstance(row.get("started_unix_ns"), int) and row["started_unix_ns"] >= 0]
    return (min(starts), max(starts)) if starts else None


def _json_objects(harness_dir):
    for root, directories, files in os.walk(harness_dir):
        directories[:] = sorted(d for d in directories if d not in EXCLUDED_RECEIPT_DIRS and not Path(root, d).is_symlink())
        for name in sorted(files):
            if not name.endswith(".stdout"):
                continue
            path = Path(root) / name
            if path.is_symlink():
                continue
            try:
                raw = driver.regular(path, STREAM_LIMIT)
            except (driver.Rejected, OSError):
                continue
            stripped = raw.lstrip()
            if not stripped[:1] in (b"{", b"["):
                continue
            try:
                value = json.loads(stripped)
            except ValueError:
                continue
            yield value


def prohibited(harness_dir, rows):
    """Derive `prohibited_observed` from retained guard proofs; returns (flags, runtime_guard_proofs)."""
    flags = {key: False for key in PROHIBITED_KEYS}
    guard_proofs = 0
    for value in _json_objects(harness_dir):
        items = value if isinstance(value, list) else [value]
        for item in items:
            if not isinstance(item, dict):
                continue
            if "DefaultRuntime" in item and isinstance(item.get("Runtimes"), dict):
                guard_proofs += 1
                runtimes = item["Runtimes"]
                if item["DefaultRuntime"] != "youki":
                    flags["runc"] = flags["runc"] or item["DefaultRuntime"] == "runc"
                    flags["crun"] = flags["crun"] or item["DefaultRuntime"] == "crun"
                for name, runtime in runtimes.items():
                    if "crun" in name:
                        flags["crun"] = True
                    if "runc" in name and runtime != INERT_RUNTIME_METADATA:
                        flags["runc"] = True
            endpoints = item.get("Endpoints") if isinstance(item.get("Endpoints"), dict) else None
            if endpoints is not None and isinstance(endpoints.get("docker"), dict):
                host = str(endpoints["docker"].get("Host", ""))
                flags["docker_desktop"] = flags["docker_desktop"] or "desktop" in host.lower() or "/.docker/run/docker.sock" in host
                flags["host_system_daemon"] = flags["host_system_daemon"] or host == "unix:///var/run/docker.sock"
                flags["ssh_hosts"] = flags["ssh_hosts"] or host.startswith("ssh://")
    for row in rows:
        executable = row.get("executable") or row["argv0"]
        if Path(executable).name == "cargo" or (len(row["argv"]) > 1 and Path(row["argv"][0]).name == "cargo" and row["argv"][1] == "run"):
            flags["cargo_run"] = True
        if not str(executable).startswith("/"):
            flags["path_fallback"] = True
        if any(str(item).startswith("ssh://") for item in row["argv"]):
            flags["ssh_hosts"] = True
    return flags, guard_proofs


def digested(harness_dir, prefix=HARNESS_SUBDIR):
    """Every relative path the harness `checksums.sha256` commits to, by digest."""
    checksums = harness_dir / "checksums.sha256"
    if not checksums.is_file() or checksums.is_symlink():
        return frozenset()
    paths = set()
    for line in driver.regular(checksums, CHECKSUMS_LIMIT).decode("utf-8", "replace").splitlines():
        digest, separator, relative = line.partition("  ")
        if separator and re.fullmatch(DIGEST, digest) and relative:
            paths.add(prefix + "/" + relative)
    return frozenset(paths)


def evidence_files(committed, cited, prefix=HARNESS_SUBDIR):
    """What the lane declares: the run documents plus every file a scenario cites.

    A composed run digests tens of thousands of files, and declaring all of them
    would push the lane result past the gate's own JSON bound without adding a
    claim -- `checksums.sha256` already commits to each one by digest, and the
    gate scans the whole retained tree itself. So the declaration is the evidence
    the scenarios actually rest on, and every path in it must appear in that
    digest manifest.
    """
    documents = {prefix + "/" + name for name in RUN_DOCUMENTS}
    return sorted((documents | set(cited)) & (committed | {prefix + "/checksums.sha256"}))


def _machine_slices(block):
    return [item for item in block if isinstance(item, dict)] if isinstance(block, list) else []


def executed_suites(result, info):
    """Every suite the run covered, paired with its Machine slices, in run order.

    A single-suite run yields one pair from `scenario.machine_slices`; the
    composed run the gate invokes yields one pair per executed suite from
    `scenario.suite_slices`, which the harness writes only for `--suite all`.
    """
    suite = result.get("suite") or info.get("suite")
    block = result.get("scenario") if isinstance(result.get("scenario"), dict) else {}
    if suite != COMPOSED_SUITE:
        require(suite in scenarios.SUITES, "unknown suite in harness result: " + repr(suite))
        return ((suite, _machine_slices(block.get("machine_slices"))),)
    executed, per_suite = block.get("suites_executed"), block.get("suite_slices")
    require(isinstance(executed, list) and executed and isinstance(per_suite, dict),
            "composed harness result carries no per-suite Machine slices")
    covered = []
    for name in executed:
        require(name in scenarios.SUITES, "unknown suite in harness result: " + repr(name))
        covered.append((name, _machine_slices(per_suite.get(name))))
    return tuple(covered)


def from_run(ctx, result, info, harness_dir, exit_code, *, repo_root=REPO_ROOT, prefix=HARNESS_SUBDIR):
    """Translate one DEV run (`result.json` + `info`) into a lane result."""
    harness_dir = Path(harness_dir)
    covered = executed_suites(result, info)
    lane = base(ctx, repo_root)
    error, cleanup_errors = result.get("error"), list(result.get("cleanup_errors") or [])
    passed = error is None and not cleanup_errors and str(result.get("outcome", "")).startswith("passed_")
    rows = receipts(harness_dir)
    flags, guard_proofs = prohibited(harness_dir, rows)
    if passed and guard_proofs == 0:
        passed, error = False, "no retained Engine runtime guard proof (info DefaultRuntime/Runtimes) in harness evidence"
    if passed and any(flags.values()):
        passed, error = False, "prohibited component observed: " + ", ".join(sorted(k for k, v in flags.items() if v))
    manifest_rows, window = scenarios.manifest(), run_window(rows)
    lane["scenarios"] = [entry for suite, slices in covered
                         for entry in scenarios.lane_scenarios(suite, slices, phase=lane["phase"], passed=passed, error=error,
                                                               evidence_prefix=prefix, window=window, rows=manifest_rows)]
    lane["process_starts"] = process_starts(rows, [suite for suite, _ in covered])
    lane["prohibited_observed"] = flags
    lane["cleanup_errors"] = [] if passed else cleanup_errors
    retained = result.get("retained_root")
    lane["retained_root"] = retained if isinstance(retained, str) and retained.startswith("/") else None
    committed = digested(harness_dir, prefix)
    cited = {path for entry in lane["scenarios"] for path in entry["evidence"]}
    undigested = sorted(path for entry in lane["scenarios"] if entry["status"] == "PASS" for path in entry["evidence"]
                        if path not in committed)
    if passed and undigested:
        passed, error = False, "scenario cites evidence the harness never digested: " + ", ".join(undigested[:5])
        lane["scenarios"] = [entry for suite, slices in covered
                             for entry in scenarios.lane_scenarios(suite, slices, phase=lane["phase"], passed=passed, error=error,
                                                                   evidence_prefix=prefix, window=window, rows=manifest_rows)]
    lane["evidence_files"] = evidence_files(committed, cited, prefix)
    if passed:
        lane["outcome"] = "passed"
    else:
        reason, detail = failure_reason(error, cleanup_errors)
        lane["failure"] = {"reason": reason, "detail": detail[:2000], "exit_code": exit_code}
    return lane


def write(ctx, lane):
    """Exclusive, fsync'd write of `<evidence-dir>/lane-result.json`; the lane directory is the gate's."""
    path = ctx.result_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    require(not os.path.lexists(path), "lane result already exists: " + str(path))
    document(path, lane)
    return path
