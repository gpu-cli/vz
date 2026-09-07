"""Native `lane-result.json` writer for the sandbox-vm lane.

`scripts/run-sandbox-vm-e2e.sh` delegates here at the end of its summary writer
and from its EXIT trap on every other exit path, so a lane result exists beside
`summary.txt` for every run that got as far as creating a run directory (and in
the gate's `--evidence-dir` even when it did not). Standard library only,
Python 3.9 (`/usr/bin/python3`): the harness does not carry `jsonschema`, so the
schema check here is opportunistic; the gate validates the document itself.

Field derivations (see docs/sandbox-vm-e2e.md, "Lane result"):

* `run_id`: `--run-id` from the gate, else `sandbox-<run dir name>` lowered and
  restricted to `[a-z0-9-]` (standalone DEV runs); `phase`: `--phase` else
  `clean-provision`.
* `candidate_tuple_sha256` / `fixture_sha256`: the gate's values; when absent
  the digest of the empty byte string (`ABSENT_DIGEST`) marks a standalone
  run, which the gate can therefore never admit.
* `release_dir_sha256`: `vz04_common.tree_digest(<release-dir>)`, the same
  derivation the gate manifest uses (`vz04_candidate`), and only after
  `<release-dir>/release-manifest.json` exists as a regular file; a
  `--release-dir` without that manifest is `input_rejected`. Absent:
  `ABSENT_DIGEST`.
* `contract_sha256`: SHA-256 of `--contract`, else of the repository's
  `config/vz-0.4-e2e-contract.json`, else `ABSENT_DIGEST`.
* `entry_point`: repo-relative script path, its SHA-256, the argv it received.
* `outcome`/`failure`: `passed` only when the summary was written, the exit
  code is 0, `failed=none` and `passed=` is non-empty. Otherwise
  `input_rejected` (gate argument problems), `crash` (a signal interrupted the
  harness, or it died before writing a summary while running suites),
  `assertion` (`failed=` lists suites, scenarios or evidence checks),
  `prerequisite` (preflight, build or provisioning failure).
* `process_starts`: one row per `run_and_log` invocation, recorded by the
  script as `label<TAB>pid<TAB>argv0` in `process-starts.tsv`; the scenario id
  is `gate.sandbox_vm.<label>` with the label reduced to `[a-z_]`.
* `prohibited_observed`: `runc`/`crun` are true when the youki-only BuildKit
  runtime inventory (`buildkit-runtime-inventory.txt`, asserted by
  `validate_buildkit_runtime_inventory_evidence`) lists a runtime path with
  that basename; the other keys are false (the harness never uses `cargo run`,
  Docker Desktop, a host daemon, PATH fallback or SSH hosts).
* `scenarios` is always empty (the lane owns no acceptance IDs), `leaks` and
  `cleanup_errors` empty, `handoff` all null, `result_adapter` null,
  `retained_root` the run directory, `evidence_files` relative to the
  directory holding the lane result.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from vz04_common import (CONFIG_FILES, GateError, LANE_PHASES, digest_file, read_regular,  # noqa: E402
                         tree_digest)

LANE = "sandbox-vm"
ENTRY_POINT = "scripts/run-sandbox-vm-e2e.sh"
ABSENT_DIGEST = hashlib.sha256(b"").hexdigest()
STAGES = ("arguments", "lane-arguments", "preflight", "provision", "suites", "summary")
PROHIBITED_KEYS = ("docker_desktop", "host_system_daemon", "runc", "crun", "cargo_run", "path_fallback", "ssh_hosts")
RUN_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,63}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
RELATIVE_RE = re.compile(r"^[A-Za-z0-9._@+-]+(/[A-Za-z0-9._@+-]+)*$")
PROCESS_STARTS_FILE = "process-starts.tsv"
INVENTORY_FILE = "buildkit-runtime-inventory.txt"
# Files copied from the run directory into a distinct gate evidence directory.
COPIED_EVIDENCE = (("summary.txt", "sandbox-summary.txt"), ("run-info.txt", "sandbox-run-info.txt"),
                   (PROCESS_STARTS_FILE, "sandbox-process-starts.tsv"), (INVENTORY_FILE, "sandbox-buildkit-runtime-inventory.txt"))
MAX_TEXT = 4 * 1024 * 1024


def parse_args(argv):
    parser = argparse.ArgumentParser(description="write the sandbox-vm native lane result", allow_abbrev=False)
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--exit-code", required=True, type=int)
    parser.add_argument("--stage", required=True, choices=STAGES)
    parser.add_argument("--interrupted", default=None, help="signal name when a trap ended the harness")
    parser.add_argument("--run-dir", default=None, help="timestamped run directory (absent before preflight)")
    parser.add_argument("--evidence-dir", default=None, help="gate evidence directory; defaults to the run directory")
    for name in ("run-id", "phase", "release-dir", "state-root", "contract", "candidate-tuple", "fixture-sha256", "handoff",
                 "docker", "compose-plugin", "buildx-plugin"):
        parser.add_argument("--" + name, default=None)
    parser.add_argument("script_argv", nargs="*", help="argv the harness received (after --)")
    return parser.parse_args(argv)


def _regular(path: Path) -> bool:
    return path.is_file() and not path.is_symlink()


def _text(path: Path) -> str:
    return read_regular(path, MAX_TEXT).decode("utf-8", "replace")


def parse_summary(run_dir):
    """`summary.txt` key=value rows; `passed`/`failed` are split into lists, `none` meaning empty."""
    if run_dir is None or not _regular(run_dir / "summary.txt"):
        return None
    fields = {}
    for line in _text(run_dir / "summary.txt").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            fields[key] = value
    for key in ("passed", "failed"):
        value = fields.get(key, "none")
        fields[key] = [] if value in ("", "none") else value.split()
    return fields


def standalone_run_id(run_name) -> str:
    stem = re.sub(r"[^a-z0-9-]", "-", (run_name or "unstarted").lower())
    run_id = ("sandbox-" + stem)[:64]
    if not RUN_ID_RE.match(run_id):
        raise GateError(f"cannot derive a standalone run id from {run_name!r}")
    return run_id


def scenario_id_for_label(label: str) -> str:
    reduced = re.sub(r"[^a-z_]", "", label.lower().replace("-", "_")).strip("_") or "suite"
    return f"gate.sandbox_vm.{reduced}"


def parse_process_starts(run_dir) -> list:
    if run_dir is None or not _regular(run_dir / PROCESS_STARTS_FILE):
        return []
    rows = []
    for line in _text(run_dir / PROCESS_STARTS_FILE).splitlines():
        parts = line.split("\t", 2)
        if len(parts) != 3 or not parts[2]:
            raise GateError(f"malformed process start record: {line!r}")
        label, pid, argv0 = parts
        rows.append({"scenario_id": scenario_id_for_label(label), "argv0": argv0,
                     "pid": int(pid) if pid.isdigit() and int(pid) >= 1 else None})
    return rows


def prohibited_observed(run_dir, summary) -> dict:
    observed = {key: False for key in PROHIBITED_KEYS}
    candidates = []
    if summary and summary.get("buildkit_runtime_inventory", "none") != "none":
        candidates.append(Path(summary["buildkit_runtime_inventory"]))
    if run_dir is not None:
        candidates.append(run_dir / INVENTORY_FILE)
    for path in candidates:
        if not path.is_absolute() or not _regular(path):
            continue
        try:
            inventory = json.loads(_text(path))
        except ValueError:
            continue
        if not isinstance(inventory, dict):
            continue
        paths = []
        for key in ("observed_runtime_paths", "oci_runtime_elf_paths", "forbidden_runtime_paths", "runtime_binary"):
            value = inventory.get(key)
            paths += value if isinstance(value, list) else ([value] if isinstance(value, str) else [])
        names = {os.path.basename(str(p)) for p in paths}
        observed["runc"] = observed["runc"] or "runc" in names
        observed["crun"] = observed["crun"] or "crun" in names
    return observed


def gate_inputs(args, problems: list) -> dict:
    """Resolve the gate-supplied identity fields, collecting input_rejected problems."""
    repo_root = Path(args.repo_root)
    values = {"candidate_tuple_sha256": ABSENT_DIGEST, "fixture_sha256": ABSENT_DIGEST,
              "release_dir_sha256": ABSENT_DIGEST, "contract_sha256": ABSENT_DIGEST}
    for option, key in (("candidate_tuple", "candidate_tuple_sha256"), ("fixture_sha256", "fixture_sha256")):
        value = getattr(args, option)
        if value is not None:
            if DIGEST_RE.match(value):
                values[key] = value
            else:
                problems.append(f"--{option.replace('_', '-')} is not a 64-hex digest")
    if args.release_dir is not None:
        release_dir = Path(args.release_dir)
        manifest = release_dir / "release-manifest.json"
        if not release_dir.is_absolute() or not release_dir.is_dir() or release_dir.is_symlink():
            problems.append(f"--release-dir is not an absolute real directory: {args.release_dir}")
        elif not _regular(manifest):
            problems.append(f"--release-dir lacks release-manifest.json: {args.release_dir}")
        else:
            try:
                values["release_dir_sha256"] = tree_digest(release_dir)
            except GateError as error:
                problems.append(f"release directory digest failed: {error}")
    contract = Path(args.contract) if args.contract is not None else repo_root / CONFIG_FILES["e2e_contract"]
    if _regular(contract):
        values["contract_sha256"] = digest_file(contract)
    elif args.contract is not None:
        problems.append(f"--contract is not a regular file: {args.contract}")
    if args.run_id is not None and not RUN_ID_RE.match(args.run_id):
        problems.append(f"--run-id does not match {RUN_ID_RE.pattern}")
    if args.phase is not None and args.phase != "clean-provision":
        problems.append(f"sandbox-vm runs only clean-provision, got --phase {args.phase}")
    if args.handoff not in (None, "none"):
        problems.append("sandbox-vm consumes no state handoff; --handoff must be none")
    if args.state_root is not None and not Path(args.state_root).is_absolute():
        problems.append("--state-root must be absolute")
    return values


def failure_for(args, summary, problems) -> tuple:
    """(outcome, failure) from the stage, exit code, interruption and summary facts."""
    code = args.exit_code
    if problems:
        return "failed", {"reason": "input_rejected", "detail": "; ".join(problems), "exit_code": code}
    if args.interrupted:
        return "failed", {"reason": "crash", "detail": f"harness interrupted by {args.interrupted} during {args.stage}", "exit_code": code}
    if args.stage == "summary" and summary is not None:
        if summary["failed"]:
            return "failed", {"reason": "assertion", "detail": "failed=" + " ".join(summary["failed"]), "exit_code": code}
        if code == 0 and summary["passed"]:
            return "passed", None
        detail = "summary reports no passed suites" if not summary["passed"] else f"harness exited {code} with failed=none"
        return "failed", {"reason": "crash", "detail": detail, "exit_code": code}
    if args.stage == "summary":
        return "failed", {"reason": "crash", "detail": "summary stage reached without summary.txt", "exit_code": code}
    if args.stage in ("arguments", "lane-arguments"):
        return "failed", {"reason": "input_rejected", "detail": f"harness rejected its arguments (exit {code})", "exit_code": code}
    if args.stage in ("preflight", "provision"):
        return "failed", {"reason": "prerequisite", "detail": f"harness failed during {args.stage} (exit {code})", "exit_code": code}
    return "failed", {"reason": "crash", "detail": f"harness exited {code} while running suites before writing summary.txt", "exit_code": code}


def collect_evidence(run_dir, evidence_dir) -> list:
    """Evidence files relative to `evidence_dir`; copies from a distinct run directory."""
    if run_dir is None:
        return []
    if evidence_dir == run_dir:
        names = sorted(p.name for p in run_dir.iterdir() if _regular(p) and p.name != "lane-result.json" and RELATIVE_RE.match(p.name))
        return names
    copied = []
    for source_name, target_name in COPIED_EVIDENCE:
        source = run_dir / source_name
        if _regular(source):
            with open(evidence_dir / target_name, "xb") as handle:
                handle.write(read_regular(source, MAX_TEXT))
                handle.flush()
                os.fsync(handle.fileno())
            copied.append(target_name)
    return copied


def build_result(args) -> dict:
    repo_root = Path(args.repo_root)
    run_dir = Path(args.run_dir) if args.run_dir else None
    if run_dir is not None and (not run_dir.is_absolute() or not run_dir.is_dir()):
        raise GateError(f"run directory is not an absolute directory: {args.run_dir}")
    evidence_dir = Path(args.evidence_dir) if args.evidence_dir else run_dir
    if evidence_dir is None:
        raise GateError("neither --evidence-dir nor --run-dir given; nowhere to write the lane result")
    if not evidence_dir.is_absolute():
        raise GateError(f"evidence directory must be absolute: {evidence_dir}")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    problems = []
    identity = gate_inputs(args, problems)
    summary = parse_summary(run_dir)
    outcome, failure = failure_for(args, summary, problems)
    phase = args.phase if args.phase in LANE_PHASES else "clean-provision"
    run_id = args.run_id if args.run_id is not None and RUN_ID_RE.match(args.run_id) else standalone_run_id(run_dir.name if run_dir else None)
    script = repo_root / ENTRY_POINT
    entry_sha256 = digest_file(script) if _regular(script) else ABSENT_DIGEST
    result = {"schema_version": 1, "kind": "vz-0.4-lane-result", "lane": LANE, "phase": phase, "run_id": run_id,
              "candidate_tuple_sha256": identity["candidate_tuple_sha256"], "release_dir_sha256": identity["release_dir_sha256"],
              "fixture_sha256": identity["fixture_sha256"], "contract_sha256": identity["contract_sha256"],
              "entry_point": {"path": ENTRY_POINT, "sha256": entry_sha256, "argv": list(args.script_argv)},
              "outcome": outcome, "failure": failure, "scenarios": [], "test_case_retries": 0,
              "process_starts": parse_process_starts(run_dir), "prohibited_observed": prohibited_observed(run_dir, summary),
              "leaks": [], "cleanup_errors": [], "handoff": {"produced": None, "consumed": None, "consumed_sha256": None},
              "retained_root": str(run_dir) if run_dir else None, "evidence_files": collect_evidence(run_dir, evidence_dir),
              "result_adapter": None}
    return result, evidence_dir, run_dir


def optional_schema_check(result, repo_root: Path) -> list:
    """Validate when jsonschema is importable; the gate performs the authoritative check."""
    try:
        import vz04_schema  # noqa: F401  (imports jsonschema)
    except ImportError:
        return []
    return vz04_schema.validate("lane-result", result, repo_root)


def write_document(path: Path, result: dict) -> None:
    data = (json.dumps(result, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with open(path, "xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def main(argv=None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        result, evidence_dir, run_dir = build_result(args)
        problems = optional_schema_check(result, Path(args.repo_root))
        if problems:
            raise GateError("lane result is not schema-valid: " + "; ".join(problems))
        target = evidence_dir / "lane-result.json"
        write_document(target, result)
        if run_dir is not None and run_dir != evidence_dir:
            write_document(run_dir / "lane-result.json", result)
    except FileExistsError as error:
        # A lane result is written exactly once; a second invocation for the same
        # run must never silently replace the retained document.
        print(f"sandbox lane result not written: refusing to overwrite {error.filename}", file=sys.stderr)
        return 2
    except GateError as error:
        print(f"sandbox lane result not written: {error}", file=sys.stderr)
        return 2
    print(str(target))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
