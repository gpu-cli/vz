#!/usr/bin/env python3
"""`topology` lane of the vz 0.4 aggregate release gate (skeleton).

Entry point: `scripts/run-developer-environment-e2e.sh --suite all <lane argv>`
with exactly the argv contract from `vz04_lanes.lane_argv`. A schema-valid
`<evidence-dir>/lane-result.json` is written on every exit path where the
identity fields (run-id, digests, phase) permit one.

What is physical today (phase `clean-provision`, installed release binaries,
isolated state under `<state-root>/topology`, no Machine ever provisioned):
criterion 21 sub-checks bare_help, legacy_rejection, clean_up_refuses,
bootstrap_read_only and criterion 15 sub-checks help_surface_exact,
error_envelope_agreement. Everything else is reported FAIL with an explicit
`not_implemented` assertion; the lane outcome is then `failed` with
`failure.reason: not_implemented` (exit 3) so accounting stays honest. A real
regression in an implemented sub-check yields `assertion` (exit 1).

Exit codes: 0 passed, 1 failed (assertion/crash/cleanup/uncertain_effects),
2 input rejected, 3 not_implemented.
"""
from __future__ import annotations

import os
from pathlib import Path
import re
import shutil
import sys
import traceback

import vz04_candidate as candidate
import vz04_lanes as lanes
import vz04_schema as schema
from vz04_common import (DIGEST_PATTERN, LANE_PHASES, REPO_ROOT, RUN_ID_PATTERN, GateError, canonical_path, digest_file,
                         document, load_json, now_ns, require, sha256_bytes, tree_digest, write_exclusive)
import developer_environment_checks as checks
from developer_environment_recorder import (LANE, CleanupError, LaneState, Recorder, inventory, inventory_digest,
                                            processes_referencing, stop_daemons, stray_sockets, write_inventory)

ENTRY_POINT = "scripts/run-developer-environment-e2e.sh"
EXIT_PASSED, EXIT_FAILED, EXIT_REJECTED, EXIT_NOT_IMPLEMENTED = 0, 1, 2, 3
REQUIRED_COMPONENTS = ("bin/vz", "bin/vz-runtimed")
GATE_OWNED_FILES = frozenset(("lane-result.json", "lane-result.rejected.json", "lane.stdout", "lane.stderr", "invocation.json"))
CRITERION_21 = "gate.cli.legacy_removal_and_bootstrap"
CRITERION_15 = "gate.cli_api.agreement"
HANDOFF_SENTINEL = "state-handoff-sentinel.txt"


class Rejected(Exception):
    """Argument/contract/release admission failure (exit 2)."""


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


class Lane:
    def __init__(self, argv: list, *, repo_root: Path, codesign_verifier):
        self.argv = [str(a) for a in argv]
        self.repo_root = Path(repo_root)
        self.codesign_verifier = codesign_verifier
        self.options, self.problems = scan_argv(self.argv)
        self.evidence_dir = None
        self.ctx = None
        self.entry = None
        self.contract = None
        self.release = None
        self.phase = self.options.get("phase")

    # -- identity and result plumbing -------------------------------------------------
    def can_emit(self) -> bool:
        o = self.options
        evidence = o.get("evidence-dir")
        return (_matches(o.get("run-id"), RUN_ID_PATTERN) and _matches(o.get("candidate-tuple"), DIGEST_PATTERN) and
                _matches(o.get("fixture-sha256"), DIGEST_PATTERN) and self.phase in LANE_PHASES and isinstance(evidence, str) and
                evidence.startswith("/") and not any(c in evidence for c in "\r\n\x00"))

    def prepare_identity(self) -> None:
        o = self.options
        self.evidence_dir = Path(o["evidence-dir"])
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        contract_path = Path(o["contract"]) if isinstance(o.get("contract"), str) else self.repo_root / "config/vz-0.4-e2e-contract.json"
        release_dir = Path(o["release-dir"]) if isinstance(o.get("release-dir"), str) else Path("/nonexistent")
        self.ctx = lanes.LaneContext(
            run_id=o["run-id"], release_dir=release_dir,
            release_dir_sha256=_digest_or_empty(lambda: tree_digest(canonical_path(release_dir))),
            state_root=o.get("state-root") or "/nonexistent", contract_path=contract_path,
            contract_sha256=_digest_or_empty(lambda: digest_file(contract_path)),
            candidate_tuple_sha256=o["candidate-tuple"], fixture_sha256=o["fixture-sha256"],
            clients={"docker": o.get("docker"), "compose_plugin": o.get("compose-plugin"), "buildx_plugin": o.get("buildx-plugin")},
            repo_root=self.repo_root)
        entry_path = self.repo_root / ENTRY_POINT
        self.entry = {"path": ENTRY_POINT, "sha256": digest_file(entry_path) if entry_path.is_file() else sha256_bytes(b""),
                      "argv": self.argv}

    def write_result(self, result: dict) -> None:
        schema.require_valid("lane-result", result, self.repo_root)
        document(self.evidence_dir / "lane-result.json", result, replace=True)

    def failed(self, reason: str, detail: str, exit_code: int, *, scenarios=None, extra=None) -> dict:
        result = lanes.failed_result(LANE, self.phase, self.ctx, self.entry, reason, detail, exit_code)
        result["scenarios"] = scenarios or []
        for key, value in (extra or {}).items():
            result[key] = value
        return result

    def evidence_files(self) -> list:
        files = []
        for path in sorted(self.evidence_dir.rglob("*")):
            if path.is_file() and not path.is_symlink():
                relative = path.relative_to(self.evidence_dir).as_posix()
                if relative not in GATE_OWNED_FILES:
                    files.append(relative)
        return files

    # -- admission -------------------------------------------------------------------
    def admit(self) -> None:
        if self.problems:
            raise Rejected("; ".join(self.problems))
        o = self.options
        require(o["suite"] == "all", "only --suite all is accepted")
        contract_path = canonical_path(o["contract"])
        self.contract = load_json(contract_path)
        problems = schema.validate("e2e-contract", self.contract, self.repo_root)
        require(not problems, "contract rejected: " + "; ".join(problems))
        topology = [entry for entry in self.contract["lanes"] if entry["name"] == LANE]
        require(len(topology) == 1, "contract does not declare exactly one topology lane")
        require(self.phase in topology[0]["phases"], f"phase {self.phase} is not assigned to the topology lane")
        require(topology[0]["entry_point"] == ENTRY_POINT, "contract entry point differs from this script's wrapper")
        state_root = Path(o["state-root"])
        require(state_root.is_absolute() and not any(c in str(state_root) for c in "\r\n\x00"), "absolute clean --state-root required")
        state_root.mkdir(parents=True, exist_ok=True)
        require(canonical_path(state_root) == state_root, "canonical --state-root required")
        for key in ("docker", "compose-plugin", "buildx-plugin"):
            require(o[key] == "none" or (o[key].startswith("/") and Path(o[key]).is_file()), f"--{key} must be none or an existing absolute path")
        require(o["handoff"] == "none" or (o["handoff"].startswith("/") and Path(o["handoff"]).is_file()), "--handoff must be none or an existing absolute file")
        self.release = candidate.admit_release_dir(o["release-dir"], repo_root=self.repo_root, codesign_verifier=self.codesign_verifier)
        for relative in REQUIRED_COMPONENTS:
            require(relative in self.release["components"], f"release manifest lacks component {relative}")
            path = Path(self.release["dir"]) / relative
            require(os.access(path, os.X_OK), f"release component not executable: {relative}")
        pin = self.contract["pins"]["cli_removal"]
        self.cli_removal = load_json(self.repo_root / pin)
        require(self.cli_removal.get("schema_version") == 1 and isinstance(self.cli_removal.get("removed_roots"), list),
                f"{pin} is not a CLI-removal inventory")
        self.ctx.release_dir = Path(self.release["dir"])
        self.ctx.release_dir_sha256 = self.release["release_dir_sha256"]
        self.ctx.contract_sha256 = digest_file(contract_path)

    def release_findings(self) -> list:
        return [f"{code} {subject}: {detail}" for code, subject, detail in self.release["findings"] if code == "release.codesign"]

    def assigned(self) -> list:
        return [s for s in self.contract["scenarios"] if s["lane"] == LANE and s["phase"] == self.phase]

    def not_implemented_scenario(self, scenario: dict, moment: int) -> dict:
        return {"id": scenario["id"], "status": "FAIL", "started_unix_ns": moment, "ended_unix_ns": now_ns(),
                "assertions": [f"not_implemented: criterion {scenario['criterion']} ({scenario['title']}) needs provisioned Machines; "
                               "this lane skeleton never provisions"], "evidence": [], "readiness_polls": []}

    # -- phases ----------------------------------------------------------------------
    def run(self) -> int:
        if self.phase == "clean-provision":
            return self.run_clean_provision()
        if self.phase == "final-cleanup":
            return self.run_final_cleanup()
        return self.run_persisted_recovery()

    def handoff_record(self) -> dict:
        handoff = self.options["handoff"]
        if handoff == "none":
            return {"produced": None, "consumed": None, "consumed_sha256": None}
        path = Path(handoff)
        return {"produced": None, "consumed": path.name, "consumed_sha256": digest_file(path)}

    def run_persisted_recovery(self) -> int:
        moment = now_ns()
        state = LaneState(self.ctx.state_root, self.ctx.release_dir / "bin")
        rows, path = write_inventory(self.evidence_dir, "lane-state-root", state.root)
        scenarios = [self.not_implemented_scenario(s, moment) for s in self.assigned()]
        result = self.failed("not_implemented", f"topology lane {self.phase}: no scenario implemented; lane state root inventory retained "
                             f"({len(rows)} entries)", EXIT_NOT_IMPLEMENTED, scenarios=scenarios,
                             extra={"handoff": self.handoff_record(), "retained_root": str(state.root) if state.root.exists() else None,
                                    "evidence_files": self.evidence_files()})
        self.write_result(result)
        return EXIT_NOT_IMPLEMENTED

    def run_final_cleanup(self) -> int:
        moment = now_ns()
        state = LaneState(self.ctx.state_root, self.ctx.release_dir / "bin")
        rows, path = write_inventory(self.evidence_dir, "lane-state-root-before-cleanup", state.root)
        cleanup_errors, leaks, notes = [], [], [f"lane state root: {state.root}", f"entries before cleanup: {len(rows)}"]
        try:
            daemons = stop_daemons(state)
            notes.append(f"daemons stopped: {daemons if daemons else 'none present'}")
        except CleanupError as error:
            cleanup_errors.append(str(error))
        live = processes_referencing(state)
        for pid, command in live:
            leaks.append({"kind": "process", "identifier": f"pid {pid}: {command[:200]}"})
        if not cleanup_errors and not leaks and state.root.exists():
            try:
                shutil.rmtree(state.root)
            except OSError as error:
                cleanup_errors.append(f"cannot remove lane state root: {error}")
        remaining = inventory(state.root)
        for relative, kind, _mode, _size, _digest in remaining:
            leaks.append({"kind": kind, "identifier": f"{state.root}/{relative}"})
        notes.append(f"entries after cleanup: {len(remaining)}")
        write_exclusive(self.evidence_dir / "cleanup.txt", ("\n".join(notes + cleanup_errors) + "\n").encode())
        scenarios = [self.not_implemented_scenario(s, moment) for s in self.assigned()]
        if cleanup_errors or leaks:
            result = self.failed("cleanup", "final-cleanup could not positively remove the lane state root: " +
                                 "; ".join(cleanup_errors or [f"{len(leaks)} survivors"]), EXIT_FAILED, scenarios=scenarios,
                                 extra={"cleanup_errors": cleanup_errors, "leaks": leaks, "handoff": self.handoff_record(),
                                        "retained_root": str(state.root) if state.root.exists() else None, "evidence_files": self.evidence_files()})
            self.write_result(result)
            return EXIT_FAILED
        result = self.failed("not_implemented", "topology lane final-cleanup: scenarios not implemented; lane state root removed, "
                             "nothing of this lane remains outside the retained evidence directory", EXIT_NOT_IMPLEMENTED,
                             scenarios=scenarios, extra={"handoff": self.handoff_record(), "retained_root": None,
                                                        "evidence_files": self.evidence_files()})
        self.write_result(result)
        return EXIT_NOT_IMPLEMENTED

    def run_clean_provision(self) -> int:
        state = LaneState(self.ctx.state_root, self.ctx.release_dir / "bin")
        if os.path.lexists(state.root):
            result = self.failed("prerequisite", f"lane state root already exists before clean-provision: {state.root}", EXIT_FAILED)
            self.write_result(result)
            return EXIT_FAILED
        codesign = self.release_findings()
        if codesign:
            result = self.failed("prerequisite", "release components failed codesign re-verification; refusing to execute: " +
                                 "; ".join(codesign)[:600], EXIT_FAILED)
            self.write_result(result)
            return EXIT_FAILED
        state.create()
        recorder = Recorder(self.evidence_dir, self.ctx.run_id)
        write_exclusive(self.evidence_dir / "lane-facts.txt", (
            f"lane state root: {state.root}\ncli: {state.cli}\ncli sha256: {digest_file(state.cli)}\n"
            f"daemon: {state.daemon}\ndaemon sha256: {digest_file(state.daemon)}\nsocket: {state.socket}\n"
            f"socket path bindable (<= 103 bytes): {state.socket_path_bindable()}\n"
            f"release signing_class: {self.release['signing_class']}\nrelease version: {self.release['release_version']}\n").encode())
        ctx = checks.CheckContext(repo_root=self.repo_root, release_dir=self.ctx.release_dir, state=state, recorder=recorder,
                                  evidence_dir=self.evidence_dir, cli_removal=self.cli_removal)
        subchecks = {CRITERION_21: [], CRITERION_15: []}
        crash = None
        started = now_ns()
        try:
            subchecks[CRITERION_21].append(checks.check_bare_help(ctx, CRITERION_21))
            subchecks[CRITERION_21].append(checks.check_legacy_rejection(ctx, CRITERION_21))
            subchecks[CRITERION_21].append(checks.check_clean_up(ctx, CRITERION_21))
            subchecks[CRITERION_21].append(checks.check_bootstrap_read_only(ctx, CRITERION_21))
            subchecks[CRITERION_21].append(checks.check_bootstrap_creates_default(CRITERION_21))
            subchecks[CRITERION_15].append(checks.check_help_surface(ctx, CRITERION_15))
            subchecks[CRITERION_15].append(checks.check_error_envelope(ctx, CRITERION_15))
            subchecks[CRITERION_15].append(checks.check_status_field_set(CRITERION_15))
            subchecks[CRITERION_15].append(checks.check_grpc_agreement(CRITERION_15))
        except Exception:  # noqa: BLE001 - recorded as a crash, never swallowed
            crash = traceback.format_exc()
            write_exclusive(self.evidence_dir / "crash.txt", crash.encode())
        cleanup_errors, leaks, cleanup_notes = [], [], []
        try:
            daemons = stop_daemons(state)
            cleanup_notes.append(f"autospawned daemons stopped gracefully: {daemons if daemons else 'none observed'}")
        except CleanupError as error:
            cleanup_errors.append(str(error))
        for pid, command in processes_referencing(state):
            leaks.append({"kind": "process", "identifier": f"pid {pid}: {command[:200]}"})
        for path in stray_sockets(state):
            leaks.append({"kind": "socket", "identifier": str(path)})
        write_exclusive(self.evidence_dir / "cleanup.txt", ("\n".join(cleanup_notes + cleanup_errors) + "\n").encode())

        scenarios, summary = [], {"PASS": [], "FAIL": [], "not_implemented": []}
        for scenario in self.assigned():
            subs = subchecks.get(scenario["id"])
            if not subs:
                scenarios.append(self.not_implemented_scenario(scenario, started))
                continue
            status = "PASS" if all(sub.status == "PASS" for sub in subs) else "FAIL"
            assertions = [f"{sub.id}: {sub.status}" + (" (not_implemented)" if sub.not_implemented else "") for sub in subs]
            if crash:
                assertions.append("lane crashed before every sub-check completed; see crash.txt")
                status = "FAIL"
            evidence = sorted({path for sub in subs for path in sub.evidence})
            scenarios.append({"id": scenario["id"], "status": status, "started_unix_ns": min(sub.started for sub in subs),
                              "ended_unix_ns": max(sub.ended or now_ns() for sub in subs), "assertions": assertions,
                              "evidence": evidence, "readiness_polls": []})
        (self.evidence_dir / "checks").mkdir(mode=0o700)
        for subs in subchecks.values():
            for sub in subs:
                scenarios.append(sub.scenario())
                text = "\n".join([f"{sub.id}: {sub.status}", *sub.scenario()["assertions"]]) + "\n"
                write_exclusive(self.evidence_dir / "checks" / f"{sub.slug}.txt", text.encode())
                summary["not_implemented" if sub.not_implemented else sub.status].append(sub.slug)
        rows = inventory(state.root)
        write_exclusive(self.evidence_dir / HANDOFF_SENTINEL, (
            f"run_id: {self.ctx.run_id}\nlane_state_root: {state.root}\nlane_state_root_inventory_sha256: {inventory_digest(rows)}\n"
            f"entries: {len(rows)}\n").encode())

        base = lanes.base_result(LANE, self.phase, self.ctx, self.entry)
        base.update(scenarios=scenarios, process_starts=recorder.process_starts, cleanup_errors=cleanup_errors, leaks=leaks,
                    retained_root=str(state.root), evidence_files=self.evidence_files(),
                    handoff={"produced": HANDOFF_SENTINEL, "consumed": None, "consumed_sha256": None})
        detail = (f"sub-checks PASS={summary['PASS']} FAIL={summary['FAIL']} not_implemented={summary['not_implemented']}; "
                  f"top-level FAIL={[s['id'] for s in scenarios if s['status'] == 'FAIL' and '__' not in s['id']]}")
        if crash:
            base["failure"] = {"reason": "crash", "detail": "lane crashed: " + crash.strip().splitlines()[-1][:300] + "; " + detail, "exit_code": EXIT_FAILED}
            code = EXIT_FAILED
        elif recorder.uncertain:
            names = [receipt.name for receipt in recorder.uncertain]
            base["failure"] = {"reason": "uncertain_effects", "detail": f"observers with uncertain effects: {names[:10]}; " + detail, "exit_code": EXIT_FAILED}
            code = EXIT_FAILED
        elif cleanup_errors or leaks:
            base["failure"] = {"reason": "cleanup", "detail": "; ".join(cleanup_errors) or f"{len(leaks)} live processes reference the lane state root", "exit_code": EXIT_FAILED}
            code = EXIT_FAILED
        elif summary["FAIL"]:
            base["failure"] = {"reason": "assertion", "detail": detail, "exit_code": EXIT_FAILED}
            code = EXIT_FAILED
        elif any(s["status"] == "FAIL" for s in scenarios):
            base["failure"] = {"reason": "not_implemented", "detail": detail, "exit_code": EXIT_NOT_IMPLEMENTED}
            code = EXIT_NOT_IMPLEMENTED
        else:
            base["outcome"], base["failure"] = "passed", None
            code = EXIT_PASSED
        self.write_result(base)
        print(f"topology lane {self.phase}: outcome={base['outcome']} "
              f"reason={None if base['failure'] is None else base['failure']['reason']} {detail}", file=sys.stderr)
        return code


def main(argv=None, *, repo_root: Path = REPO_ROOT, codesign_verifier=candidate.run_codesign_verify) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    lane = Lane(argv, repo_root=repo_root, codesign_verifier=codesign_verifier)
    if not lane.can_emit():
        print("topology lane rejected input before a lane result could be written: " +
              "; ".join(lane.problems or ["run-id/candidate-tuple/fixture-sha256/phase/evidence-dir identity invalid"]), file=sys.stderr)
        return EXIT_REJECTED
    lane.prepare_identity()
    try:
        lane.admit()
    except (Rejected, GateError, OSError) as error:
        lane.write_result(lane.failed("input_rejected", f"topology lane rejected input: {error}", EXIT_REJECTED))
        print(f"topology lane rejected input: {error}", file=sys.stderr)
        return EXIT_REJECTED
    return lane.run()


if __name__ == "__main__":
    raise SystemExit(main())
