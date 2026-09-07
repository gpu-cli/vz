"""Aggregate vz 0.4 release-gate orchestrator.

    vz04_gate.py --suite all --release-dir <dir> --run-id <id> [--evidence-root]
                 [--state-root] [--docker] [--compose-plugin] [--buildx-plugin]
                 [--linux-docker-context] [--sleep-wake-ack-file] [--dry-lanes]

Only `--suite all` is accepted; anything else exits 2 before touching state.
Admission never mutates inputs. The index entry is written before any phase
runs so an aborted run is retained. The verdict is computed by
`vz04_validate.evaluate`, written, and then reproduced by
`vz04_validate.validate_root` on the finished root; exit 0 only on PASS.

`--dry-lanes` is a DEVELOPER-ONLY flag: it substitutes each lane invocation
with that lane's `not_implemented` result and records the cargo prerequisites
as not executed, so the orchestrator/validator path is exercised without
provisioning. It is refused for `developer-id-notarized` releases, recorded in
`developer_overrides`, and makes PASS impossible.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import vz04_candidate as candidate
import vz04_contract as contract_module
import vz04_host as host
import vz04_lanes as lanes
import vz04_phases as phases
import vz04_schema as schema
import vz04_validate as validate
from vz04_common import (EVIDENCE_ROOT_DEFAULT, REPO_ROOT, RUN_ID_PATTERN, GateError, canonical_path, checked_text,
                         digest_file, document, git_dirty, git_head, load_json, now_ns, require, utc_iso, write_checksums,
                         write_exclusive)

# Injection point for unit tests only (never a CLI flag): tests build fake
# release directories with dummy files and substitute a fake verifier here.
CODESIGN_VERIFIER = candidate.run_codesign_verify

PREREQUISITES = (
    ("cargo-fmt", ["cargo", "fmt", "--manifest-path", "crates/Cargo.toml", "--all", "--", "--check"], 600),
    ("cargo-clippy", ["cargo", "clippy", "--manifest-path", "crates/Cargo.toml", "--workspace", "--all-targets",
                      "--all-features", "--", "-D", "warnings"], 3600),
    ("cargo-nextest", ["cargo", "nextest", "run", "--manifest-path", "crates/Cargo.toml", "--workspace", "--all-features"], 3600),
)


def parse_args(argv):
    for index, item in enumerate(argv):
        if item == "--suite" and index + 1 < len(argv) and argv[index + 1] == "all":
            break
        if item == "--suite=all":
            break
    else:
        print("error: only --suite all is accepted by the aggregate release gate", file=sys.stderr)
        raise SystemExit(2)
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--suite", required=True, choices=("all",))
    parser.add_argument("--release-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--evidence-root", default=str(REPO_ROOT / EVIDENCE_ROOT_DEFAULT))
    parser.add_argument("--state-root", default=None)
    parser.add_argument("--docker", default=None)
    parser.add_argument("--compose-plugin", default=None)
    parser.add_argument("--buildx-plugin", default=None)
    parser.add_argument("--linux-docker-context", default=None)
    parser.add_argument("--sleep-wake-ack-file", default=None)
    parser.add_argument("--dry-lanes", action="store_true", help="DEV ONLY: substitute lanes with not_implemented results")
    return parser.parse_args(argv)


class Receipts:
    def __init__(self, root: Path, run_id: str):
        self.root, self.run_id, self.count, self.paths = root, run_id, 0, []
        root.mkdir(parents=True, exist_ok=True)

    def _row(self, label, argv, cwd, timeout):
        self.count += 1
        return {"schema_version": 1, "kind": "vz-0.4-receipt", "run_id": self.run_id, "index": self.count, "label": label,
                "argv": list(argv), "executable": argv[0], "cwd": str(cwd), "timeout_seconds": timeout, "state": "intent",
                "started_unix_ns": None, "ended_unix_ns": None, "exit_code": None, "stdout_path": None, "stderr_path": None,
                "stdout_sha256": None, "stderr_sha256": None, "error": None, "effects_uncertain": False, "canary_withheld": False,
                "not_executed_reason": None}

    def not_executed(self, label, argv, cwd, timeout, reason) -> None:
        row = self._row(label, argv, cwd, timeout)
        row.update(state="not_executed", not_executed_reason=reason)
        path = self.root / f"{row['index']:03}-{label}.result.json"
        document(path, row)
        self.paths.append(path)

    def run(self, label, argv, cwd, timeout, env) -> int:
        row = self._row(label, argv, cwd, timeout)
        stem = f"{row['index']:03}-{label}"
        document(self.root / f"{stem}.intent.json", row)
        self.paths.append(self.root / f"{stem}.intent.json")
        row["started_unix_ns"] = now_ns()
        stdout_path, stderr_path = self.root / f"{stem}.stdout", self.root / f"{stem}.stderr"
        code = None
        try:
            with open(stdout_path, "xb") as out, open(stderr_path, "xb") as err:
                completed = subprocess.run(argv, cwd=str(cwd), env=env, stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                           timeout=timeout, check=False)
            code = completed.returncode
            row.update(state="completed", exit_code=code, effects_uncertain=code < 0)
        except subprocess.TimeoutExpired:
            row.update(state="error", error="timeout", effects_uncertain=True)
        except OSError as error:
            row.update(state="error", error=f"{type(error).__name__}: {error}")
        row.update(ended_unix_ns=now_ns(), stdout_path=stdout_path.name, stderr_path=stderr_path.name,
                   stdout_sha256=digest_file(stdout_path) if stdout_path.exists() else None,
                   stderr_sha256=digest_file(stderr_path) if stderr_path.exists() else None)
        row["stdout_path"], row["stderr_path"] = f"prerequisites/{stdout_path.name}", f"prerequisites/{stderr_path.name}"
        document(self.root / f"{stem}.result.json", row)
        self.paths.append(self.root / f"{stem}.result.json")
        return code if code is not None else -1


def load_index(path: Path) -> dict:
    if path.exists():
        index = load_json(path)
        schema.require_valid("run-index", index)
        return index
    return {"schema_version": 1, "kind": "vz-0.4-run-index", "entries": []}


def summary_text(manifest: dict, summary: dict) -> str:
    lines = [f"verdict={summary['verdict']}", f"run_id={manifest['run_id']}", f"candidate_tuple_sha256={manifest['candidate_tuple_sha256']}",
             f"release_dir={manifest['release']['dir']}", f"signing_class={manifest['release']['signing_class']}",
             f"developer_overrides={','.join(summary['developer_overrides']) or 'none'}",
             "counts required={required} PASS={PASS} FAIL={FAIL} MISSING={MISSING} findings={findings}".format(**summary["counts"]), "",
             "lanes:"]
    for row in summary["lanes"]:
        lines.append(f"  {row['lane']} {row['phase']} outcome={row['outcome']} failure={row['failure_reason'] or 'none'}")
    lines += ["", "scenarios not PASS:"]
    for row in summary["scenarios"]:
        if row["status"] != "PASS":
            lines.append(f"  {row['status']} {row['id']} lane={row['lane']} phase={row['phase']} reason={row['reason']}")
    lines += ["", "unmet requirements (findings):"]
    for row in summary["findings"]:
        lines.append(f"  {row['code']} {row['subject']}: {row['detail']}")
    return "\n".join(lines) + "\n"


def run(args) -> int:
    repo_root = REPO_ROOT
    run_id = checked_text(args.run_id, RUN_ID_PATTERN, "run-id")
    evidence_root = Path(args.evidence_root)
    require(evidence_root.is_absolute(), "evidence root must be absolute")
    evidence_root.mkdir(parents=True, exist_ok=True)
    evidence_root = canonical_path(evidence_root)
    root = evidence_root / run_id
    require(not os.path.lexists(root), f"evidence directory already exists: {root}")
    index_path = evidence_root / "index.json"
    index = load_index(index_path)
    require(all(entry["run_id"] != run_id for entry in index["entries"]), f"run-id already recorded in {index_path}")

    release = candidate.admit_release_dir(args.release_dir, repo_root=repo_root, codesign_verifier=CODESIGN_VERIFIER)
    overrides = []
    if args.dry_lanes:
        require(release["signing_class"] != "developer-id-notarized", "--dry-lanes is refused for developer-id-notarized releases")
        overrides.append("dry_lanes")
    clients = {}
    for key in ("docker", "compose_plugin", "buildx_plugin"):
        value = getattr(args, key)
        if value is None:
            require(args.dry_lanes, f"--{key.replace('_', '-')} is required unless --dry-lanes")
            clients[key] = None
        else:
            # Clients are invoked by the path given: OrbStack/Docker Desktop ship
            # multi-call binaries dispatched by argv[0], so symlinked plugin paths
            # are admitted as-is and the resolved target is recorded with its digest.
            path = canonical_path(value, must_exist=False)
            require(path.is_file() and os.access(path, os.X_OK), f"client is not an executable file: {path}")
            clients[key] = str(path)

    contract = contract_module.load_contract(repo_root)
    docker = contract_module.load_docker_contract(repo_root)
    required = contract_module.required_scenarios(contract, docker)
    frozen = contract_module.frozen_inputs(contract, repo_root)
    source_commit = git_head(repo_root)
    tuple_value = candidate.candidate_tuple(source_commit=release["source_commit"], source_tree_sha256=release["manifest"]["source"].get("tree_sha256"),
                                           release=release, frozen=frozen)
    if not args.dry_lanes:
        clash = [e for e in index["entries"] if e["candidate_tuple_sha256"] == tuple_value["sha256"] and not e["developer_overrides"]]
        require(not clash, f"candidate tuple already recorded as run {clash[0]['run_id']}; a new tuple is required")

    if args.state_root is None:
        state_root = Path(tempfile.mkdtemp(prefix=f"vz04-{run_id}-")).resolve()
    else:
        state_root = Path(args.state_root)
        require(state_root.is_absolute() and not os.path.lexists(state_root), "state root must be an absolute fresh path")
        state_root.mkdir(mode=0o700)
        state_root = state_root.resolve()
    state_root = canonical_path(state_root)

    root.mkdir(mode=0o700)
    started = utc_iso()
    manifest = {
        "schema_version": 1, "kind": "vz-0.4-gate-manifest", "run_id": run_id, "suite": "all", "verdict": "running",
        "started_at_utc": started, "finished_at_utc": None, "developer_overrides": overrides,
        "source": {"commit": source_commit, "dirty": git_dirty(repo_root), "repo_root": str(repo_root)},
        "host": host.host_facts(state_root), "toolchain": host.toolchain_facts(),
        "release": {"dir": release["dir"], "release_dir_sha256": release["release_dir_sha256"],
                    "release_manifest_sha256": release["release_manifest_sha256"], "signing_class": release["signing_class"],
                    "release_version": release["release_version"], "normalized_content_sha256": release["normalized_content_sha256"],
                    "signed_content_sha256": release["signed_content_sha256"], "source_commit": release["source_commit"],
                    "components": release["components"]},
        "inputs": frozen["inputs"], "candidate_tuple": tuple_value["tuple"], "candidate_tuple_sha256": tuple_value["sha256"],
        "index_path": str(index_path), "state_root": str(state_root), "clients": host.client_facts(clients),
        "prerequisites": {"status": "not_executed", "receipts": []},
        "lanes": {lane["name"]: {"entry_point": lane["entry_point"], "sha256": lanes.entry_point_record(repo_root, lane, [])["sha256"],
                                 "role": lane["role"], "phases": lane["phases"]} for lane in contract["lanes"]},
        "required_scenarios": required, "phases": [], "inventories": {"before": None, "after": None}, "leak_diff": None,
    }
    schema.require_valid("gate-manifest", manifest)
    index["entries"].append({"run_id": run_id, "candidate_tuple_sha256": tuple_value["sha256"], "started_at_utc": started,
                             "evidence_dir": str(root), "verdict": "running", "developer_overrides": overrides})
    document(index_path, index, replace=True)
    document(root / "manifest.json", manifest)
    print(f"==> run {run_id} admitted; evidence {root}; tuple {tuple_value['sha256'][:16]}", flush=True)

    receipts = Receipts(root / "prerequisites", run_id)
    env = lanes.minimal_env(lanes.LaneContext(run_id=run_id, release_dir=release["dir"], release_dir_sha256="", state_root=state_root,
                                              contract_path="", contract_sha256="", candidate_tuple_sha256="", fixture_sha256="", clients=clients))
    prerequisite_status = "passed"
    for label, argv, timeout in PREREQUISITES:
        if args.dry_lanes:
            receipts.not_executed(label, argv, repo_root, timeout, "dry-lanes developer substitution: prerequisite not executed")
            prerequisite_status = "not_executed"
            continue
        print(f"==> prerequisite {label}", flush=True)
        if receipts.run(label, argv, repo_root, timeout, env) != 0:
            prerequisite_status = "failed"
            print(f"==> prerequisite {label} failed; stopping before lanes", flush=True)
            break
    manifest["prerequisites"] = {"status": prerequisite_status, "receipts": [str(p.relative_to(root)) for p in receipts.paths]}
    scope = host.HostScope(run_id=run_id, state_root=state_root, release_dir=Path(release["dir"]), clients=clients)
    manifest["inventories"]["before"] = host.write_inventory(root, scope, "before")
    print(f"==> host inventory before: {manifest['inventories']['before']}", flush=True)

    if prerequisite_status != "failed":
        ctx = lanes.LaneContext(run_id=run_id, release_dir=release["dir"], release_dir_sha256=release["release_dir_sha256"], state_root=state_root,
                                contract_path=repo_root / "config/vz-0.4-e2e-contract.json", contract_sha256=frozen["inputs"]["e2e_contract"]["sha256"],
                                candidate_tuple_sha256=tuple_value["sha256"], fixture_sha256=frozen["digests"]["fixtures_tree_sha256"],
                                clients=clients, repo_root=repo_root, linux_docker_context=args.linux_docker_context)

        def observer(phase, lane, lane_phase, result):
            reason = "" if result["failure"] is None else f" ({result['failure']['reason']})"
            print(f"==> {phase}: {lane} {lane_phase} -> {result['outcome']}{reason}", flush=True)

        outcome = phases.run_phases(root, contract, ctx, dry=args.dry_lanes, scope=scope, before_inventory=manifest["inventories"]["before"],
                                    ack_file=args.sleep_wake_ack_file, observer=observer)
        manifest["phases"] = outcome["phases"]
        manifest["inventories"]["after"] = outcome["after_inventory"]
        manifest["leak_diff"] = outcome["leak_diff"]
    else:
        manifest["inventories"]["after"] = host.write_inventory(root, scope, "after")
        manifest["leak_diff"] = {"performed": False, "survivors": [],
                                 "reason": "prerequisites failed before any lane ran; no final-cleanup phase to diff"}

    evaluation = validate.evaluate(root, manifest, repo_root=repo_root, codesign_verifier=CODESIGN_VERIFIER)
    verdict = validate.verdict_for(evaluation["findings"], evaluation["scenarios"])
    counts = {"required": len(evaluation["scenarios"]), "PASS": sum(r["status"] == "PASS" for r in evaluation["scenarios"]),
              "FAIL": sum(r["status"] == "FAIL" for r in evaluation["scenarios"]),
              "MISSING": sum(r["status"] == "MISSING" for r in evaluation["scenarios"]), "findings": len(evaluation["findings"])}
    summary = {"schema_version": 1, "kind": "vz-0.4-summary", "run_id": run_id, "verdict": verdict,
               "candidate_tuple_sha256": tuple_value["sha256"], "developer_overrides": overrides, "counts": counts,
               "scenarios": evaluation["scenarios"], "lanes": evaluation["lanes"], "findings": evaluation["findings"]}
    schema.require_valid("summary", summary)
    manifest["verdict"], manifest["finished_at_utc"] = verdict, utc_iso()
    schema.require_valid("gate-manifest", manifest)
    document(root / "summary.json", summary)
    write_exclusive(root / "summary.txt", summary_text(manifest, summary).encode("utf-8"))
    document(root / "manifest.json", manifest, replace=True)
    index = load_index(index_path)
    for entry in index["entries"]:
        if entry["run_id"] == run_id:
            entry["verdict"] = verdict
    document(index_path, index, replace=True)
    write_checksums(root)

    report = validate.validate_root(root, repo_root=repo_root, codesign_verifier=CODESIGN_VERIFIER)
    reproduced = report["verdict"] == verdict and report["raw_findings"] == evaluation["findings"]
    print(f"==> verdict={verdict} required={counts['required']} PASS={counts['PASS']} FAIL={counts['FAIL']} MISSING={counts['MISSING']} "
          f"findings={counts['findings']} validator_reproduced={reproduced}", flush=True)
    print(f"==> summary: {root / 'summary.txt'}", flush=True)
    if not reproduced:
        extra = [r for r in report["findings"] if r not in evaluation["findings"]]
        print("==> validator did not reproduce the gate verdict: " + "; ".join(f"{r['code']} {r['subject']}" for r in extra[:5]), file=sys.stderr)
        return 1
    return 0 if verdict == "PASS" else 1


def main(argv=None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        return run(args)
    except GateError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
