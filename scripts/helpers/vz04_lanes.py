"""Lane invocation, lane-result synthesis and required-scenario accounting.

No `skipped` state exists. A lane that does not exist, refuses `--suite all`,
crashes, or writes no schema-valid result yields `outcome: failed` with a
typed `failure.reason`; every scenario assigned to it becomes MISSING.

The sandbox lane writes a native result through
`scripts/helpers/sandbox_lane_result.py`; the contract marks it
`native_result_required`, so a missing result is a `crash` failure. The
`summary.txt` translator survives only for a contract that clears that flag.

CLI (used by the failing lane stubs):
  python vz04_lanes.py stub --lane <name> --entry-point <repo-relative> <lane argv...>
writes a schema-valid `not_implemented` lane result into `--evidence-dir` and
exits 3.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

import frozen_tree
import vz04_schema as schema
from vz04_host import DOCKER_CONFIG_DIRNAME
from vz04_common import (CONFIG_FILES, LANE_PHASES, REPO_ROOT, GateError, canonical_path, digest_file, document,
                         load_json, now_ns, read_regular, require, sha256_bytes, tree_digest, write_exclusive)

STUB_EXIT = 3
LANE_OPTIONS = ("run-id", "phase", "release-dir", "evidence-dir", "state-root", "contract", "candidate-tuple",
                "fixture-sha256", "handoff", "docker", "compose-plugin", "buildx-plugin")
PROHIBITED_NONE = {k: False for k in ("docker_desktop", "host_system_daemon", "runc", "crun", "cargo_run", "path_fallback", "ssh_hosts")}


class LaneContext:
    """Everything a lane receives through argv; never ambient environment."""

    def __init__(self, *, run_id, release_dir, release_dir_sha256, state_root, contract_path, contract_sha256,
                 candidate_tuple_sha256, fixture_sha256, clients, repo_root=REPO_ROOT, linux_docker_context=None,
                 tmux=None, acquired_inputs=None, uv=None):
        self.run_id = run_id
        self.release_dir = Path(release_dir)
        self.release_dir_sha256 = release_dir_sha256
        self.state_root = Path(state_root)
        self.contract_path = Path(contract_path)
        self.contract_sha256 = contract_sha256
        self.candidate_tuple_sha256 = candidate_tuple_sha256
        self.fixture_sha256 = fixture_sha256
        self.clients = clients
        self.repo_root = Path(repo_root)
        self.linux_docker_context = linux_docker_context
        # The terminal the composed Docker lane's lifecycle suite drives. A host
        # tool like the Docker clients, so the gate resolves it and passes the
        # path rather than the contract naming one.
        self.tmux = tmux
        # Run-frozen inputs the gate acquired from their checked-in pins, by the
        # option name the lane passes them as. Absent means nothing was acquired,
        # which a lane that needs one refuses on rather than inventing a path.
        self.acquired_inputs = dict(acquired_inputs or {})
        # Every lane entry point is a `uv run` wrapper, and `uv` is not on the
        # minimal PATH. The gate resolves it explicitly, the way it resolves the
        # Docker clients, rather than the lanes inheriting an ambient one.
        self.uv = uv


def entry_point_record(repo_root: Path, lane: dict, argv: list) -> dict:
    path = repo_root / lane["entry_point"]
    digest = digest_file(path) if path.is_file() and not path.is_symlink() else sha256_bytes(b"")
    return {"path": lane["entry_point"], "sha256": digest, "argv": [str(item) for item in argv]}


def release_components(release_dir: Path) -> dict:
    """The release inputs a candidate directory already holds, by its own layout.

    `scripts/build-vz-0.4-release-candidate.sh` writes `bin/`, `linux/developer`,
    `linux/container`, one `buildkit/vz-buildkit-*.tar` and the manifest that
    names the version. A lane that needs them should read them off the candidate
    it was pointed at rather than have them restated in the contract, where they
    could disagree with the artifacts they claim to describe.
    """
    manifest = release_dir / "release-manifest.json"
    require(manifest.is_file() and not manifest.is_symlink(),
            f"release candidate manifest missing: {manifest}")
    version = load_json(manifest).get("release_version")
    require(isinstance(version, str) and version, "release manifest names no release_version")
    archives = sorted(path for path in (release_dir / "buildkit").glob("vz-buildkit-*.tar")
                      if path.is_file() and not path.is_symlink())
    require(len(archives) == 1, f"exactly one BuildKit archive required, found {len(archives)}")
    components = {"release-version": version, "release-dir": release_dir / "bin",
                  "developer-bundle": release_dir / "linux" / "developer",
                  "hardened-bundle": release_dir / "linux" / "container",
                  "buildkit-archive": archives[0]}
    for name, path in components.items():
        if name != "release-version":
            require(Path(path).exists(), f"release candidate is missing {name}: {path}")
    return components


def lane_argv(lane: dict, ctx: LaneContext, phase: str, evidence_dir: Path, handoff) -> list:
    """The lane's contract argv plus the gate identity options every lane receives.

    `legacy_sandbox` additionally names the harness's own artifact root
    (`--output-dir`), where `run-sandbox-vm-e2e.sh` keeps its timestamped run
    directory; its native `lane-result.json` is written into `--evidence-dir`.

    `linux_docker` receives the release inputs its harness requires, derived from
    the candidate directory rather than restated in the contract, and is pointed
    at `bin/` because `installed_developer_startup` resolves `vz` and
    `vz-runtimed` directly under `--release-dir`.
    """
    argv = list(lane["argv"])
    release_dir = ctx.release_dir
    extra = []
    if lane["argv_contract"] == "linux_docker":
        components = release_components(ctx.release_dir)
        release_dir = components.pop("release-dir")
        for name, value in sorted(components.items()):
            extra += ["--" + name, str(value)]
        require(ctx.tmux, "the composed Docker lane drives a terminal and requires --tmux")
        extra += ["--tmux", str(ctx.tmux)]
        for name in ("registry-archive", "registry-layout", "ssh-packages"):
            value = ctx.acquired_inputs.get(name)
            require(value, f"the composed Docker lane requires an acquired --{name}")
            extra += ["--" + name, str(value)]
    argv += ["--run-id", ctx.run_id, "--phase", phase, "--release-dir", str(release_dir),
             "--evidence-dir", str(evidence_dir), "--state-root", str(ctx.state_root),
             "--contract", str(ctx.contract_path), "--candidate-tuple", ctx.candidate_tuple_sha256,
             "--fixture-sha256", ctx.fixture_sha256, "--handoff", handoff if handoff else "none"]
    for key in ("docker", "compose_plugin", "buildx_plugin"):
        argv += ["--" + key.replace("_", "-"), ctx.clients.get(key) or "none"]
    argv += extra
    if lane["argv_contract"] == "legacy_sandbox":
        argv += ["--output-dir", str(ctx.state_root / "sandbox-vm-output")]
    return argv


def base_result(lane_name: str, phase: str, ctx: LaneContext, entry_point: dict) -> dict:
    require(phase in LANE_PHASES, f"unknown lane phase {phase}")
    return {"schema_version": 1, "kind": "vz-0.4-lane-result", "lane": lane_name, "phase": phase, "run_id": ctx.run_id,
            "candidate_tuple_sha256": ctx.candidate_tuple_sha256, "release_dir_sha256": ctx.release_dir_sha256,
            "fixture_sha256": ctx.fixture_sha256, "contract_sha256": ctx.contract_sha256, "entry_point": entry_point,
            "source_tree": frozen_tree.record(ctx.repo_root), "outcome": "failed", "failure": None, "scenarios": [], "test_case_retries": 0, "process_starts": [],
            "prohibited_observed": dict(PROHIBITED_NONE), "leaks": [], "cleanup_errors": [],
            "handoff": {"produced": None, "consumed": None, "consumed_sha256": None}, "retained_root": None,
            "evidence_files": [], "result_adapter": None}


def failed_result(lane_name, phase, ctx, entry_point, reason, detail, exit_code=None) -> dict:
    result = base_result(lane_name, phase, ctx, entry_point)
    result["failure"] = {"reason": reason, "detail": detail, "exit_code": exit_code}
    return result


def translate_sandbox_summary(lane_name, phase, ctx, entry_point, run_dir: Path, exit_code: int, evidence_dir: Path) -> dict:
    """Translate run-sandbox-vm-e2e.sh summary.txt/run-info.txt into a lane result."""
    result = base_result(lane_name, phase, ctx, entry_point)
    summary_path = run_dir / "summary.txt"
    if not summary_path.is_file() or summary_path.is_symlink():
        result["failure"] = {"reason": "crash", "detail": f"sandbox lane wrote no summary.txt under {run_dir}", "exit_code": exit_code}
        return result
    summary = read_regular(summary_path, 4 * 1024 * 1024)
    fields = {}
    for line in summary.decode("utf-8", "replace").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            fields[key] = value
    passed = [] if fields.get("passed", "none") == "none" else fields.get("passed", "").split()
    failed = [] if fields.get("failed", "none") == "none" else fields.get("failed", "").split()
    write_exclusive(evidence_dir / "sandbox-summary.txt", summary)
    evidence_files = ["sandbox-summary.txt"]
    run_info_sha256 = None
    run_info = run_dir / "run-info.txt"
    if run_info.is_file() and not run_info.is_symlink():
        data = read_regular(run_info, 4 * 1024 * 1024)
        write_exclusive(evidence_dir / "sandbox-run-info.txt", data)
        run_info_sha256 = sha256_bytes(data)
        evidence_files.append("sandbox-run-info.txt")
    result["result_adapter"] = {"kind": "sandbox_summary_txt", "run_dir": str(run_dir), "summary_sha256": sha256_bytes(summary),
                                "run_info_sha256": run_info_sha256, "passed": passed, "failed": failed, "exit_code": exit_code}
    result["retained_root"] = str(run_dir)
    result["evidence_files"] = evidence_files
    if exit_code == 0 and not failed and passed:
        result["outcome"] = "passed"
    else:
        result["failure"] = {"reason": "assertion" if failed else "crash",
                             "detail": f"sandbox summary failed={failed or 'none'} exit={exit_code}", "exit_code": exit_code}
    return result


def minimal_env(ctx: LaneContext) -> dict:
    # TMPDIR is redirected into the run's own state root so nothing a lane
    # creates escapes into the host's shared temp. It has to EXIST before a
    # child is handed it: the gate's prerequisite builds run under this
    # environment too, and anything that reaches for a temp file early fails
    # with a bare errno that names neither the gate nor the directory. sccache,
    # configured as a global `rustc-wrapper` in a developer's ~/.cargo/config.toml,
    # does exactly that and took down `cargo nextest --all-features`:
    #   sccache: error: Failed to create temp dir
    #   caused by: No such file or directory (os error 2) at path ".../tmp/sccacheXXXXXX"
    #   error: could not compile `libc` (lib)
    temp_root = ctx.state_root / "tmp"
    temp_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin:/usr/local/bin", "HOME": os.environ.get("HOME", "/"),
           "TMPDIR": str(temp_root), "LANG": "C.UTF-8", "VZ04_RUN_ID": ctx.run_id,
           "VZ_DOCKER_CONFIG": str(ctx.state_root / DOCKER_CONFIG_DIRNAME)}
    if ctx.linux_docker_context:
        env["LINUX_DOCKER_CONTEXT"] = ctx.linux_docker_context
    cargo_home = os.environ.get("CARGO_HOME")
    if cargo_home:
        env["CARGO_HOME"] = cargo_home
        env["PATH"] = str(Path(cargo_home) / "bin") + ":" + env["PATH"]
    elif (Path(env["HOME"]) / ".cargo" / "bin").is_dir():
        env["PATH"] = str(Path(env["HOME"]) / ".cargo" / "bin") + ":" + env["PATH"]
    if ctx.uv:
        # Prepended last, so a lane runs the interpreter the gate resolved and
        # recorded rather than one that happens to be installed elsewhere —
        # `uv` is commonly present in the Cargo bin directory added above.
        env["PATH"] = str(Path(ctx.uv).parent) + ":" + env["PATH"]
    return env


def invoke_lane(lane: dict, phase: str, ctx: LaneContext, evidence_dir: Path, handoff=None, *, timeout_seconds=6 * 3600,
                dry: bool = False) -> dict:
    """Run one lane for one phase and return its lane-result (written to disk).

    `dry=True` substitutes the lane's `not_implemented` result without starting
    any process (DEV only; the gate records the override and can never PASS).
    """
    evidence_dir.mkdir(parents=True, exist_ok=False)
    result_path = evidence_dir / "lane-result.json"
    unbuildable = None
    try:
        argv = lane_argv(lane, ctx, phase, evidence_dir, handoff)
    except GateError as error:
        # A lane whose argv depends on the candidate or the host can be
        # unbuildable, and that is a finding rather than a crash. A dry run is
        # substituting this lane anyway, so it records why the command could
        # not be built and stays `not_implemented`; a real run rejects it.
        argv, unbuildable = [], str(error)
        if not dry:
            result = failed_result(lane["name"], phase, ctx, entry_point_record(ctx.repo_root, lane, []),
                                   "input_rejected", unbuildable, 2)
            document(result_path, result)
            return result
    entry = entry_point_record(ctx.repo_root, lane, argv)
    if dry:
        detail = "dry-lanes developer substitution: lane not invoked"
        if unbuildable is not None:
            detail += "; argv could not be built: " + unbuildable
        result = failed_result(lane["name"], phase, ctx, entry, "not_implemented", detail, STUB_EXIT)
        document(result_path, result)
        return result
    script = ctx.repo_root / lane["entry_point"]
    if not script.is_file():
        result = failed_result(lane["name"], phase, ctx, entry, "not_implemented", f"lane entry point does not exist: {lane['entry_point']}")
        document(result_path, result)
        return result
    (ctx.state_root / "tmp").mkdir(parents=True, exist_ok=True)
    started = now_ns()
    stdout_path, stderr_path = evidence_dir / "lane.stdout", evidence_dir / "lane.stderr"
    exit_code = None
    with open(stdout_path, "xb") as out, open(stderr_path, "xb") as err:
        try:
            completed = subprocess.run([str(script), *argv], cwd=str(ctx.repo_root), env=minimal_env(ctx), stdin=subprocess.DEVNULL,
                                       stdout=out, stderr=err, timeout=timeout_seconds, check=False)
            exit_code = completed.returncode
        except subprocess.TimeoutExpired:
            exit_code = None
    document(evidence_dir / "invocation.json", {"argv": [str(script), *argv], "started_unix_ns": started, "ended_unix_ns": now_ns(),
                                                "exit_code": exit_code, "cwd": str(ctx.repo_root)})
    if exit_code is None:
        result = failed_result(lane["name"], phase, ctx, entry, "timeout", f"lane exceeded {timeout_seconds}s", None)
        if result_path.exists():
            result_path.unlink()
        document(result_path, result)
        return result
    if not result_path.is_file():
        if lane["result_adapter"] == "sandbox_summary_txt" and not lane["native_result_required"]:
            # Legacy translation, only while the contract still tolerates a lane without a native result.
            output_root = ctx.state_root / "sandbox-vm-output"
            runs = sorted(p for p in output_root.iterdir() if p.is_dir() and not p.is_symlink()) if output_root.is_dir() else []
            if not runs:
                result = failed_result(lane["name"], phase, ctx, entry, "crash", "sandbox lane produced no run directory", exit_code)
            else:
                result = translate_sandbox_summary(lane["name"], phase, ctx, entry, runs[-1], exit_code, evidence_dir)
            document(result_path, result)
            return result
        result = failed_result(lane["name"], phase, ctx, entry, "crash", "lane wrote no lane-result.json", exit_code)
        document(result_path, result)
        return result
    try:
        result = load_json(result_path)
        problems = schema.validate("lane-result", result, ctx.repo_root)
        require(not problems, "; ".join(problems))
        require(result["lane"] == lane["name"] and result["phase"] == phase and result["run_id"] == ctx.run_id and
                result["candidate_tuple_sha256"] == ctx.candidate_tuple_sha256, "lane result identity mismatch")
    except GateError as error:
        result_path.rename(evidence_dir / "lane-result.rejected.json")
        result = failed_result(lane["name"], phase, ctx, entry, "input_rejected", f"lane result rejected: {error}", exit_code)
        document(result_path, result)
    return result


# `developer_environment_checks.SubCheck` names a sub-check `<scenario id>__<slug>`.
SUBCHECK_SEPARATOR = "__"


def account(required: list, results: list) -> dict:
    """Required-scenario accounting. Returns {rows, findings}."""
    by_key = {}
    findings = []
    for result in results:
        key = (result["lane"], result["phase"])
        if key in by_key:
            findings.append(("lane.duplicate_result", f"{key[0]}/{key[1]}", "more than one lane result for the same lane and phase"))
        by_key[key] = result
    assigned = {row["id"]: row for row in required}
    reported = {}
    for result in results:
        for scenario in result["scenarios"]:
            reported.setdefault(scenario["id"], []).append((result["lane"], result["phase"], scenario["status"]))
    for identifier, places in sorted(reported.items()):
        expected = assigned.get(identifier)
        if expected is None:
            parent, separator, slug = identifier.partition(SUBCHECK_SEPARATOR)
            if separator and slug and parent in assigned:
                # A sub-check of a required scenario. The topology lane reports
                # these so its evidence names which assertion of a criterion
                # failed, and the contract deliberately does not require them --
                # they are diagnostics, not claims. Counting one as an unknown
                # scenario made every finding-free verdict unreachable for that
                # lane: `validate.verdict_for` returns PASS only when `findings`
                # is empty, so a lane failed the gate precisely because it
                # explained itself. `scenario.unknown` still catches a lane
                # reporting anything that is not a sub-check of a required
                # scenario, which is the case it exists for.
                continue
            findings.append(("scenario.unknown", identifier, f"reported by {places[0][0]}/{places[0][1]} but not required"))
            continue
        if len(places) > 1:
            findings.append(("scenario.duplicate", identifier, f"reported {len(places)} times across lanes/phases"))
        for lane, phase, _status in places:
            if (lane, phase) != (expected["lane"], expected["phase"]):
                findings.append(("scenario.misassigned", identifier, f"reported by {lane}/{phase}, assigned to {expected['lane']}/{expected['phase']}"))
    rows = []
    for row in required:
        result = by_key.get((row["lane"], row["phase"]))
        status, reason = "MISSING", None
        if result is None:
            reason = "lane_result_absent"
        elif result["outcome"] == "failed":
            reason = result["failure"]["reason"]
        else:
            matches = [s for s in result["scenarios"] if s["id"] == row["id"]]
            if not matches:
                reason = "not_reported"
            elif len(matches) > 1:
                reason = "duplicate_in_lane"
            else:
                status, reason = matches[0]["status"], None
        rows.append({"id": row["id"], "lane": row["lane"], "phase": row["phase"], "status": status, "reason": reason})
        if status != "PASS":
            findings.append((f"scenario.{status.lower()}", row["id"], f"{row['lane']}/{row['phase']}: {reason or 'assertion failed'}"))
    return {"rows": rows, "findings": findings}


def _stub_main(argv) -> int:
    parser = argparse.ArgumentParser(description="write a schema-valid not_implemented lane result", allow_abbrev=False)
    parser.add_argument("--lane", required=True, choices=("topology", "native-macos", "linux-docker", "sandbox-vm"))
    parser.add_argument("--entry-point", required=True)
    parser.add_argument("--suite", required=True)
    for name in LANE_OPTIONS:
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args(argv)
    require(args.suite == "all", "only --suite all is accepted")
    evidence_dir = canonical_path(args.evidence_dir)
    contract_path = canonical_path(args.contract)
    release_dir = canonical_path(args.release_dir)
    ctx = LaneContext(run_id=args.run_id, release_dir=release_dir, release_dir_sha256=tree_digest(release_dir),
                      state_root=args.state_root, contract_path=contract_path, contract_sha256=digest_file(contract_path),
                      candidate_tuple_sha256=args.candidate_tuple, fixture_sha256=args.fixture_sha256,
                      clients={"docker": args.docker, "compose_plugin": args.compose_plugin, "buildx_plugin": args.buildx_plugin})
    entry = {"path": args.entry_point, "sha256": digest_file(REPO_ROOT / args.entry_point), "argv": [str(a) for a in argv]}
    result = failed_result(args.lane, args.phase, ctx, entry, "not_implemented",
                           f"{args.entry_point} is an explicit failing stub; the {args.lane} lane is not implemented", STUB_EXIT)
    schema.require_valid("lane-result", result)
    document(evidence_dir / "lane-result.json", result)
    print(f"{args.lane} lane not implemented; wrote not_implemented lane result", file=sys.stderr)
    return STUB_EXIT


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "stub":
        try:
            return _stub_main(argv[1:])
        except GateError as error:
            print(f"lane stub rejected input: {error}", file=sys.stderr)
            return 2
    print("usage: vz04_lanes.py stub --lane <name> --entry-point <path> <lane argv...>", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
