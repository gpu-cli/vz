#!/usr/bin/env python3
"""`topology` lane of the vz 0.4 aggregate release gate (skeleton).

Entry point: `scripts/run-developer-environment-e2e.sh --suite all <lane argv>`
with exactly the argv contract from `vz04_lanes.lane_argv`. A schema-valid
`<evidence-dir>/lane-result.json` is written on every exit path where the
identity fields (run-id, digests, phase) permit one.

Isolated state lives under `<state-root>/topology`; the AF_UNIX sockets the
installed binaries bind live in a short root derived from the state root
(`developer_environment_recorder.socket_root_for`), because macOS cannot bind a
103+ byte path and a real `--state-root` is already over that budget. Both roots
are owned by the lane: created here, scanned for daemons/strays, removed at
final-cleanup.

Every sub-check but `grpc_api_live_agreement` is physical, including the ones
that provision real Developer Machines and delete them again. What is not
implemented is reported FAIL with an explicit
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
CRITERION_1 = "gate.instances.three_concurrent_no_collision"
CRITERION_16 = "gate.reproducibility.recreate_from_definition"
CRITERION_11 = "gate.delete.single_environment_safety"
CRITERION_5 = "gate.network.private_topology_paths"
CRITERION_2 = "gate.machines.mixed_profile_topology_status"
CRITERION_6 = "gate.network.public_like_ingress"
CRITERION_7 = "gate.host.import_export_boundaries"
CRITERION_10 = "gate.lifecycle.recovery_including_sleep_wake"
CRITERION_17 = "gate.storage.workspace_projection_policy"
CRITERION_19 = "gate.migration.install_upgrade_rollback_uninstall"
CRITERION_18 = "gate.secrets.snapshots_scoped_redacted"
CRITERION_20 = "gate.network.exhaustive_denial_matrix"
CRITERION_22 = "gate.definition.reconciliation_fencing"
HANDOFF_SENTINEL = "state-handoff-sentinel.txt"
# Run one named sub-check instead of the phase's whole set. A single-claim test
# otherwise runs every other criterion's checks to assert one thing, which is
# most of this suite's runtime. It cannot be used to manufacture evidence: the
# phase always grades every scenario `assigned()` returns, so the ones with no
# sub-check become FAIL and the outcome can never be `passed`. The gate's own
# `lanes.lane_argv` never emits it.
OPTIONAL_OPTIONS = ("only",)
# The Environments pre-sleep leaves running and post-wake must find again.
# Criterion 8 wants three mutually isolated Environments and criterion 10 wants
# their identity preserved across the checkpoint, so three are established once
# and both phases address them by these names.
RECOVERY_ISOLATES = ("rec-a", "rec-b", "rec-c")
RECOVERY_RECORD = "persisted-recovery-environments.json"


class Rejected(Exception):
    """Argument/contract/release admission failure (exit 2)."""


def scan_argv(argv: list) -> tuple:
    """{option: value} for `--k v` / `--k=v` pairs plus a list of problems."""
    options, problems = {}, []
    # `only` is optional and belongs to this lane alone, so it is not in
    # LANE_OPTIONS -- every entry there is required of every lane.
    known = ("suite", *lanes.LANE_OPTIONS, *OPTIONAL_OPTIONS)
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
        if key not in OPTIONAL_OPTIONS and key not in options:
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

    def check_context(self, state: LaneState, recorder: Recorder) -> checks.CheckContext:
        """One CheckContext, built the same way in every phase.

        Three phases used to construct this inline. A phase that assembled it
        differently would hand its checks a different Docker client or plugin
        set, and the difference would surface as a criterion failing in one
        phase and passing in another for no reason a reader could find.
        """
        return checks.CheckContext(repo_root=self.repo_root, release_dir=self.ctx.release_dir, state=state,
                                   recorder=recorder, evidence_dir=self.evidence_dir, cli_removal=self.cli_removal,
                                   docker_client=self.options.get("docker", "none"),
                                   plugins={"compose": self.options.get("compose-plugin"),
                                            "buildx": self.options.get("buildx-plugin")})

    def compose_result(self, subchecks: dict, crash, recorder: Recorder, *, moment: int, extra: dict) -> tuple:
        """Scenario rows and a phase outcome from this phase's sub-checks.

        The rules are `run_clean_provision`'s, in one place so a phase cannot
        quietly grade itself more kindly than its neighbours: a crash or an
        observer with uncertain effects fails; a failing sub-check fails; a
        scenario this lane assigns but does not implement, or a sub-check that
        reported `not_implemented`, keeps the phase at `not_implemented`; and
        only a phase whose every assigned scenario passed is `passed`.
        """
        scenarios, summary = [], {"PASS": [], "FAIL": [], "not_implemented": []}
        for scenario in self.assigned():
            subs = subchecks.get(scenario["id"])
            if not subs:
                scenarios.append(self.not_implemented_scenario(scenario, moment))
                continue
            status = "PASS" if all(sub.status == "PASS" for sub in subs) and not crash else "FAIL"
            assertions = [f"{sub.id}: {sub.status}" + (" (not_implemented)" if sub.not_implemented else "") for sub in subs]
            if crash:
                assertions.append("lane crashed before every sub-check completed; see crash.txt")
            scenarios.append({"id": scenario["id"], "status": status,
                              "started_unix_ns": min(sub.started for sub in subs),
                              "ended_unix_ns": max(sub.ended or now_ns() for sub in subs), "assertions": assertions,
                              "evidence": sorted({item for sub in subs for item in sub.evidence}), "readiness_polls": []})
        checks_dir = self.evidence_dir / "checks"
        checks_dir.mkdir(mode=0o700, exist_ok=True)
        for subs in subchecks.values():
            for sub in subs:
                scenarios.append(sub.scenario())
                text = "\n".join([f"{sub.id}: {sub.status}", *sub.scenario()["assertions"]]) + "\n"
                write_exclusive(checks_dir / f"{sub.slug}.txt", text.encode())
                summary["not_implemented" if sub.not_implemented else sub.status].append(sub.slug)
        detail = (f"sub-checks PASS={summary['PASS']} FAIL={summary['FAIL']} not_implemented={summary['not_implemented']}; "
                  f"top-level FAIL={[s['id'] for s in scenarios if s['status'] == 'FAIL' and '__' not in s['id']]}")
        if crash:
            return scenarios, self.failed("crash", f"topology lane {self.phase} crashed; see crash.txt: " + detail,
                                          EXIT_FAILED, scenarios=scenarios, extra=extra), EXIT_FAILED
        if recorder.uncertain:
            names = [receipt.name for receipt in recorder.uncertain]
            return scenarios, self.failed("uncertain_effects", f"topology lane {self.phase}: observers with uncertain "
                                          f"effects: {names[:10]}; " + detail, EXIT_FAILED, scenarios=scenarios,
                                          extra=extra), EXIT_FAILED
        if summary["FAIL"]:
            return scenarios, self.failed("assertion", f"topology lane {self.phase}: " + detail, EXIT_FAILED,
                                          scenarios=scenarios, extra=extra), EXIT_FAILED
        if any(scenario["status"] == "FAIL" for scenario in scenarios):
            return scenarios, self.failed("not_implemented", f"topology lane {self.phase}: " + detail,
                                          EXIT_NOT_IMPLEMENTED, scenarios=scenarios, extra=extra), EXIT_NOT_IMPLEMENTED
        result = lanes.base_result(LANE, self.phase, self.ctx, self.entry)
        result.update(scenarios=scenarios, outcome="passed", failure=None, **extra)
        return scenarios, result, EXIT_PASSED

    def run_persisted_recovery(self) -> int:
        state = LaneState(self.ctx.state_root, self.ctx.release_dir / "bin")
        if self.phase == "persisted-recovery/pre-sleep":
            return self.run_pre_sleep(state)
        return self.run_post_wake(state)

    def run_pre_sleep(self, state: LaneState) -> int:
        """Provision the Environments the sleep/wake checkpoint has to preserve.

        Every other phase deletes what it creates. This one deliberately does
        not: the Environments it leaves running in the retained state root are
        the subject of the post-wake phase, and criterion 10's claim is about
        exactly those. `socket_root_for` derives the AF_UNIX root from the state
        root so both phases of one run address the same sockets.
        """
        moment = now_ns()
        # In the gate clean-provision already made this root; a standalone
        # pre-sleep makes its own, exactly as final-cleanup does. Establishing
        # Environments is meaningful on a fresh root -- recovering them is not,
        # which is why post-wake refuses instead of creating anything.
        if not state.root.exists():
            state.create()
        recorder = Recorder(self.evidence_dir, self.ctx.run_id)
        ctx = self.check_context(state, recorder)
        subchecks, crash, established, establish = {CRITERION_22: []}, None, None, None
        try:
            established, establish = checks.establish_recovery_environments(ctx, RECOVERY_ISOLATES)
            subchecks[CRITERION_22].extend(checks.check_definition_reconciliation_fencing(ctx, CRITERION_22, established))
        except Exception:  # noqa: BLE001 - recorded as a crash, never swallowed
            crash = traceback.format_exc()
            write_exclusive(self.evidence_dir / "crash.txt", crash.encode())
        if establish is not None:
            (self.evidence_dir / "checks").mkdir(mode=0o700, exist_ok=True)
            text = "\n".join([f"{establish.id}: {establish.status}", *establish.scenario()["assertions"]]) + "\n"
            write_exclusive(self.evidence_dir / "checks" / f"{establish.slug}.txt", text.encode())
        if established is not None:
            document(self.evidence_dir / RECOVERY_RECORD, established)
            document(state.root / RECOVERY_RECORD, established)
        rows, _path = write_inventory(self.evidence_dir, "lane-state-root", state.root)
        extra = {"handoff": self.handoff_record(), "retained_root": str(state.root),
                 "evidence_files": self.evidence_files(), "process_starts": recorder.process_starts}
        # Establishing the Environments is this phase's precondition, not one of
        # its criteria: a pre-sleep that could not bring them up has nothing for
        # post-wake to recover, which is a different failure from a criterion
        # this lane has not implemented.
        if crash is None and established is None:
            detail = "; ".join(establish.scenario()["assertions"][-4:]) if establish is not None else "no result"
            result = self.failed("prerequisite", f"topology lane {self.phase}: could not establish the Environments "
                                 f"post-wake must recover: {detail[:500]}", EXIT_FAILED, scenarios=[], extra=extra)
            self.write_result(result)
            return EXIT_FAILED
        _scenarios, result, code = self.compose_result(subchecks, crash, recorder, moment=moment, extra=extra)
        if result["failure"] is not None:
            result["failure"]["detail"] += (f"; {len(established['environments']) if established else 0} Environment(s) left "
                                            f"running for post-wake, lane state root inventory {len(rows)} entries")
        self.write_result(result)
        print(f"topology lane {self.phase}: outcome={result['outcome']} "
              f"reason={None if result['failure'] is None else result['failure']['reason']}", file=sys.stderr)
        return code

    def run_post_wake(self, state: LaneState) -> int:
        """Address the Environments pre-sleep left, across the sleep/wake edge."""
        moment = now_ns()
        record_path = state.root / RECOVERY_RECORD
        if not record_path.is_file() or record_path.is_symlink():
            result = self.failed("prerequisite", f"pre-sleep left no {RECOVERY_RECORD} in {state.root}; there is no "
                                 "record of what should have survived", EXIT_FAILED)
            self.write_result(result)
            return EXIT_FAILED
        established = load_json(record_path)
        document(self.evidence_dir / RECOVERY_RECORD, established)
        recorder = Recorder(self.evidence_dir, self.ctx.run_id)
        ctx = self.check_context(state, recorder)
        subchecks, crash = {CRITERION_10: [], CRITERION_18: [], CRITERION_20: []}, None
        try:
            subchecks[CRITERION_10].append(checks.check_lifecycle_recovery(ctx, CRITERION_10, established))
            subchecks[CRITERION_18].append(checks.check_secret_bindings_scoped_redacted(ctx, CRITERION_18))
            subchecks[CRITERION_18].append(checks.check_snapshot_restore_capability(ctx, CRITERION_18))
            subchecks[CRITERION_20].append(checks.check_exhaustive_denial_matrix(ctx, CRITERION_20, established))
        except Exception:  # noqa: BLE001 - recorded as a crash, never swallowed
            crash = traceback.format_exc()
            write_exclusive(self.evidence_dir / "crash.txt", crash.encode())
        rows, _path = write_inventory(self.evidence_dir, "lane-state-root", state.root)
        # pre-sleep leaves its daemons running on purpose: post-wake has to find
        # the Environments they supervise. Post-wake has now observed that, so
        # the Machines it woke are stopped here rather than left running through
        # the gap until final-cleanup removes the state root underneath them.
        cleanup_errors = []
        try:
            daemons = stop_daemons(state)
            write_exclusive(self.evidence_dir / "cleanup.txt",
                            (f"daemons stopped after recovery was observed: "
                             f"{daemons if daemons else 'none present'}\n").encode())
        except CleanupError as error:
            cleanup_errors.append(str(error))
        leaks = [{"kind": "process", "identifier": f"pid {pid}: {command[:200]}"}
                 for pid, command in processes_referencing(state)]
        extra = {"handoff": self.handoff_record(), "retained_root": str(state.root),
                 "evidence_files": self.evidence_files(), "process_starts": recorder.process_starts,
                 "cleanup_errors": cleanup_errors, "leaks": leaks}
        _scenarios, result, code = self.compose_result(subchecks, crash, recorder, moment=moment, extra=extra)
        if (cleanup_errors or leaks) and result["failure"] is None:
            result["outcome"] = "failed"
            result["failure"] = {"reason": "cleanup", "detail": "; ".join(cleanup_errors) or
                                 f"{len(leaks)} live processes still reference the lane state root",
                                 "exit_code": EXIT_FAILED}
            code = EXIT_FAILED
        if result["failure"] is not None:
            result["failure"]["detail"] += f"; lane state root inventory {len(rows)} entries"
        self.write_result(result)
        print(f"topology lane {self.phase}: outcome={result['outcome']} "
              f"reason={None if result['failure'] is None else result['failure']['reason']}", file=sys.stderr)
        return code

    def run_final_cleanup(self) -> int:
        moment = now_ns()
        state = LaneState(self.ctx.state_root, self.ctx.release_dir / "bin")
        # Reproducibility is a final-cleanup claim: it recreates the pinned
        # definition from fresh state and must run before the state root is
        # removed. In the gate the earlier phases already created that root; a
        # standalone final-cleanup makes its own.
        if not state.root.exists():
            state.create()
        recorder = Recorder(self.evidence_dir, self.ctx.run_id)
        ctx = checks.CheckContext(repo_root=self.repo_root, release_dir=self.ctx.release_dir, state=state,
                                  recorder=recorder, evidence_dir=self.evidence_dir, cli_removal=self.cli_removal,
                                  docker_client=self.options.get("docker", "none"),
                                  plugins={"compose": self.options.get("compose-plugin"),
                                           "buildx": self.options.get("buildx-plugin")})
        subchecks = {CRITERION_16: [], CRITERION_11: []}
        crash = None
        try:
            subchecks[CRITERION_16].append(checks.check_recreate_from_definition(ctx, CRITERION_16))
            subchecks[CRITERION_11].append(checks.check_delete_single_environment_safety(ctx, CRITERION_11))
        except Exception:  # noqa: BLE001 - recorded as a crash, never swallowed
            crash = traceback.format_exc()
            write_exclusive(self.evidence_dir / "crash.txt", crash.encode())
        rows, path = write_inventory(self.evidence_dir, "lane-state-root-before-cleanup", state.root)
        socket_rows, _socket_path = write_inventory(self.evidence_dir, "lane-socket-root-before-cleanup", state.socket_root)
        cleanup_errors, leaks, notes = [], [], [f"lane state root: {state.root}", f"entries before cleanup: {len(rows)}",
                                                f"lane socket root: {state.socket_root}",
                                                f"socket root entries before cleanup: {len(socket_rows)}"]
        try:
            daemons = stop_daemons(state)
            notes.append(f"daemons stopped: {daemons if daemons else 'none present'}")
        except CleanupError as error:
            cleanup_errors.append(str(error))
        live = processes_referencing(state)
        for pid, command in live:
            leaks.append({"kind": "process", "identifier": f"pid {pid}: {command[:200]}"})
        # The daemon writes its log inside the socket root, which the rmtree below
        # removes. The switch's drop-reason and first-carried-frame diagnostics
        # live there, and an Environment under investigation is exactly the one
        # whose evidence must outlive its runtime directory.
        for root in state.roots():
            for log in sorted(root.rglob("*.log")) if root.exists() else []:
                try:
                    target = self.evidence_dir / "daemon-logs" / log.relative_to(root)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(log, target)
                except OSError as error:
                    cleanup_errors.append(f"cannot retain daemon log {log}: {error}")
        if not cleanup_errors and not leaks:
            for root in state.roots():
                if not root.exists():
                    continue
                try:
                    shutil.rmtree(root)
                except OSError as error:
                    cleanup_errors.append(f"cannot remove lane root {root}: {error}")
        remaining = [(root, row) for root in state.roots() for row in inventory(root)]
        for root, (relative, kind, _mode, _size, _digest) in remaining:
            leaks.append({"kind": kind, "identifier": f"{root}/{relative}"})
        notes.append(f"entries after cleanup: {len(remaining)}")
        write_exclusive(self.evidence_dir / "cleanup.txt", ("\n".join(notes + cleanup_errors) + "\n").encode())
        scenarios, summary = [], {"PASS": [], "FAIL": [], "not_implemented": []}
        for scenario in self.assigned():
            subs = subchecks.get(scenario["id"])
            if not subs:
                scenarios.append(self.not_implemented_scenario(scenario, moment))
                continue
            status = "PASS" if all(sub.status == "PASS" for sub in subs) and not crash else "FAIL"
            assertions = [f"{sub.id}: {sub.status}" + (" (not_implemented)" if sub.not_implemented else "") for sub in subs]
            if crash:
                assertions.append("lane crashed before every sub-check completed; see crash.txt")
                status = "FAIL"
            scenarios.append({"id": scenario["id"], "status": status,
                              "started_unix_ns": min(sub.started for sub in subs),
                              "ended_unix_ns": max(sub.ended or now_ns() for sub in subs), "assertions": assertions,
                              "evidence": sorted({item for sub in subs for item in sub.evidence}), "readiness_polls": []})
        (self.evidence_dir / "checks").mkdir(mode=0o700)
        for subs in subchecks.values():
            for sub in subs:
                scenarios.append(sub.scenario())
                text = "\n".join([f"{sub.id}: {sub.status}", *sub.scenario()["assertions"]]) + "\n"
                write_exclusive(self.evidence_dir / "checks" / f"{sub.slug}.txt", text.encode())
                summary["not_implemented" if sub.not_implemented else sub.status].append(sub.slug)
        if cleanup_errors or leaks:
            result = self.failed("cleanup", "final-cleanup could not positively remove the lane state and socket roots: " +
                                 "; ".join(cleanup_errors or [f"{len(leaks)} survivors"]), EXIT_FAILED, scenarios=scenarios,
                                 extra={"cleanup_errors": cleanup_errors, "leaks": leaks, "handoff": self.handoff_record(),
                                        "retained_root": str(state.root) if state.root.exists() else None, "evidence_files": self.evidence_files()})
            self.write_result(result)
            return EXIT_FAILED
        detail = (f"sub-checks PASS={summary['PASS']} FAIL={summary['FAIL']} not_implemented={summary['not_implemented']}; "
                  "lane state root and socket root removed, nothing of this lane remains outside the retained "
                  "evidence directory")
        # This phase runs Up, exec and delete several times over; reporting an
        # empty process_starts would hide every one of them from the receipt.
        extra = {"handoff": self.handoff_record(), "retained_root": None, "evidence_files": self.evidence_files(),
                 "process_starts": recorder.process_starts}
        if crash:
            result = self.failed("crash", "topology lane final-cleanup crashed; see crash.txt: " + detail,
                                 EXIT_FAILED, scenarios=scenarios, extra=extra)
            self.write_result(result)
            return EXIT_FAILED
        if recorder.uncertain:
            names = [receipt.name for receipt in recorder.uncertain]
            result = self.failed("uncertain_effects", f"topology lane final-cleanup: observers with uncertain effects: "
                                 f"{names[:10]}; " + detail, EXIT_FAILED, scenarios=scenarios, extra=extra)
            self.write_result(result)
            return EXIT_FAILED
        if summary["FAIL"]:
            result = self.failed("assertion", "topology lane final-cleanup: " + detail, EXIT_FAILED,
                                 scenarios=scenarios, extra=extra)
            self.write_result(result)
            return EXIT_FAILED
        # A scenario this lane assigns but does not implement, or a sub-check
        # that reported not_implemented, keeps the phase honest -- both land as
        # a FAIL scenario. When every assigned scenario is implemented and
        # passes, the phase passes: deciding that in advance, as this did while
        # criterion 11 was still unimplemented, denies the rows their own
        # evidence once the implementation arrives.
        if any(s["status"] == "FAIL" for s in scenarios):
            result = self.failed("not_implemented", "topology lane final-cleanup: " + detail, EXIT_NOT_IMPLEMENTED,
                                 scenarios=scenarios, extra=extra)
            self.write_result(result)
            return EXIT_NOT_IMPLEMENTED
        result = lanes.base_result(LANE, self.phase, self.ctx, self.entry)
        result.update(scenarios=scenarios, outcome="passed", failure=None, **extra)
        self.write_result(result)
        return EXIT_PASSED

    def run_clean_provision(self) -> int:
        state = LaneState(self.ctx.state_root, self.ctx.release_dir / "bin")
        if os.path.lexists(state.root):
            result = self.failed("prerequisite", f"lane state root already exists before clean-provision: {state.root}", EXIT_FAILED)
            self.write_result(result)
            return EXIT_FAILED
        if os.path.lexists(state.socket_root):
            result = self.failed("prerequisite", f"lane AF_UNIX socket root already exists before clean-provision: "
                                 f"{state.socket_root}; a previous run of this state root leaked it", EXIT_FAILED)
            self.write_result(result)
            return EXIT_FAILED
        # A socket the installed binaries cannot bind fails every provisioning
        # check for a reason that has nothing to do with the runtime; say so
        # before running anything rather than reporting it as an assertion.
        budget = state.socket_budget()
        if not budget["bindable"]:
            result = self.failed("prerequisite", f"lane AF_UNIX paths exceed the macOS sun_path limit: {budget}", EXIT_FAILED)
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
            f"daemon: {state.daemon}\ndaemon sha256: {digest_file(state.daemon)}\n"
            f"lane socket root: {state.socket_root}\nsocket: {state.socket}\n"
            f"socket budget: {state.socket_budget()}\n"
            f"release signing_class: {self.release['signing_class']}\nrelease version: {self.release['release_version']}\n").encode())
        ctx = checks.CheckContext(repo_root=self.repo_root, release_dir=self.ctx.release_dir, state=state, recorder=recorder,
                                  evidence_dir=self.evidence_dir, cli_removal=self.cli_removal,
                                  docker_client=self.options.get("docker", "none"),
                                  plugins={"compose": self.options.get("compose-plugin"),
                                           "buildx": self.options.get("buildx-plugin")})
        subchecks = {CRITERION_21: [], CRITERION_15: [], CRITERION_1: [], CRITERION_5: [],
                     CRITERION_2: [], CRITERION_6: [], CRITERION_7: [], CRITERION_17: [],
                     CRITERION_19: []}
        crash = None
        started = now_ns()
        # One table, so `--only` selects from exactly the set that would
        # otherwise run, and a slug that names nothing is a rejection rather
        # than a silently empty phase.
        dispatch = [
            ("bare_help", CRITERION_21, checks.check_bare_help),
            ("legacy_rejection", CRITERION_21, checks.check_legacy_rejection),
            ("clean_up_refuses", CRITERION_21, checks.check_clean_up),
            ("bootstrap_read_only", CRITERION_21, checks.check_bootstrap_read_only),
            ("bootstrap_creates_default", CRITERION_21, checks.check_bootstrap_creates_default),
            ("help_surface_exact", CRITERION_15, checks.check_help_surface),
            ("error_envelope_agreement", CRITERION_15, checks.check_error_envelope),
            ("status_json_field_set", CRITERION_15, checks.check_status_field_set),
            ("grpc_api_live_agreement", CRITERION_15, checks.check_grpc_agreement),
            ("three_concurrent_no_collision", CRITERION_1, checks.check_three_concurrent_environments),
            ("private_topology_paths", CRITERION_5, checks.check_private_topology_paths),
            ("public_like_ingress", CRITERION_6, checks.check_public_like_ingress),
            ("host_import_export_boundaries", CRITERION_7, checks.check_host_import_export_boundaries),
            ("mixed_profile_topology_status", CRITERION_2, checks.check_mixed_profile_topology_status),
            ("workspace_storage_policy", CRITERION_17, checks.check_workspace_projection_policy),
            ("install_upgrade_rollback_uninstall", CRITERION_19, checks.check_migration_install_upgrade_rollback_uninstall)
        ]
        # A comma-separated set, because some claims are only meaningful beside a
        # control: "this mode broke X and left Y passing" needs both to have run.
        selected = None if self.options.get("only") is None else set(self.options["only"].split(","))
        known_slugs = {slug for slug, _c, _r in dispatch}
        if selected is not None and not selected <= known_slugs:
            result = self.failed("prerequisite", f"--only names no sub-check of this phase: "
                                 f"{sorted(selected - known_slugs)}; known: {sorted(known_slugs)}",
                                 EXIT_FAILED)
            self.write_result(result)
            return EXIT_FAILED
        if selected is not None:
            # Named in the retained evidence so a partial run is never mistaken
            # for a full one. It cannot pass in any case -- every scenario with
            # no sub-check grades FAIL -- but evidence should say what it was
            # rather than leave it inferred from an absence. The lane-result
            # schema is closed, so this is a file rather than a new field.
            write_exclusive(self.evidence_dir / "subcheck-filter.txt",
                            (f"--only {','.join(sorted(selected))}\nthis phase ran "
                             f"{len(selected)} sub-check(s) of {len(dispatch)}; it is not a full "
                             f"clean-provision run\n").encode())
        try:
            for slug, criterion, run in dispatch:
                if selected is not None and slug not in selected:
                    continue
                subchecks[criterion].append(run(ctx, criterion))
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
