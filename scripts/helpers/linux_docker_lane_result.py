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

Failure mapping (`failure_reason`):
  `--suite all`                      -> not_implemented (before any provisioning)
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
GATE_TRIO = ("run-id", "phase", "candidate-tuple")
GATE_OPTIONS = ("phase", "contract", "candidate-tuple", "fixture-sha256", "handoff", "state-root")
SCANNED_OPTIONS = (*GATE_TRIO, *GATE_OPTIONS, "evidence-dir", "release-dir", "suite")
RUN_ID = r"[a-z0-9][a-z0-9-]{7,63}"
DIGEST = r"[0-9a-f]{64}"
RECEIPT_LIMIT = 4 * 1024 * 1024
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
    """The harness `result.json` a completed suite run leaves in its evidence dir."""
    path = Path(harness_dir) / "result.json"
    require(path.is_file() and not path.is_symlink(), "harness result.json is missing: " + str(path))
    return _read_json(path, RECEIPT_LIMIT)


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


def process_starts(rows, suite):
    """Attribute every harness/driver host process start to the suite's declared accounting scenario."""
    scenario_id = scenarios.SUITES[suite].process_scenario
    return [{"scenario_id": scenario_id, "argv0": row["argv0"], "pid": row["pid"] if row["pid"] and row["pid"] >= 1 else None}
            for row in rows]


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


def evidence_files(harness_dir, prefix=HARNESS_SUBDIR):
    """Relative evidence list from the harness `checksums.sha256` (plus that file itself)."""
    checksums = harness_dir / "checksums.sha256"
    if not checksums.is_file() or checksums.is_symlink():
        return []
    files = []
    for line in driver.regular(checksums, RECEIPT_LIMIT).decode("utf-8", "replace").splitlines():
        digest, separator, relative = line.partition("  ")
        if separator and re.fullmatch(DIGEST, digest) and relative:
            files.append(prefix + "/" + relative)
    files.append(prefix + "/checksums.sha256")
    return files


def from_run(ctx, result, info, harness_dir, exit_code, *, repo_root=REPO_ROOT, prefix=HARNESS_SUBDIR):
    """Translate one DEV suite run (`result.json` + `info`) into a lane result."""
    harness_dir = Path(harness_dir)
    suite = result.get("suite") or info.get("suite")
    require(suite in scenarios.SUITES, "unknown suite in harness result: " + repr(suite))
    lane = base(ctx, repo_root)
    error, cleanup_errors = result.get("error"), list(result.get("cleanup_errors") or [])
    passed = error is None and not cleanup_errors and str(result.get("outcome", "")).startswith("passed_")
    rows = receipts(harness_dir)
    flags, guard_proofs = prohibited(harness_dir, rows)
    if passed and guard_proofs == 0:
        passed, error = False, "no retained Engine runtime guard proof (info DefaultRuntime/Runtimes) in harness evidence"
    if passed and any(flags.values()):
        passed, error = False, "prohibited component observed: " + ", ".join(sorted(k for k, v in flags.items() if v))
    slices = []
    scenario_block = result.get("scenario")
    if isinstance(scenario_block, dict) and isinstance(scenario_block.get("machine_slices"), list):
        slices = [item for item in scenario_block["machine_slices"] if isinstance(item, dict)]
    lane["scenarios"] = scenarios.lane_scenarios(suite, slices, phase=lane["phase"], passed=passed, error=error,
                                                 evidence_prefix=prefix, window=run_window(rows))
    lane["process_starts"] = process_starts(rows, suite)
    lane["prohibited_observed"] = flags
    lane["cleanup_errors"] = [] if passed else cleanup_errors
    retained = result.get("retained_root")
    lane["retained_root"] = retained if isinstance(retained, str) and retained.startswith("/") else None
    lane["evidence_files"] = evidence_files(harness_dir, prefix)
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
