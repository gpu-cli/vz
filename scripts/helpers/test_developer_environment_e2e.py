"""Unit tests for the topology lane skeleton against a fake `vz`.

Run: uv run --no-project --python /usr/bin/python3 --with-requirements scripts/helpers/gate-requirements.txt \
       python -B -m unittest scripts/helpers/test_developer_environment_e2e.py
"""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import unittest
from unittest import mock

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

import developer_environment_checks as checks  # noqa: E402
import developer_environment_e2e as e2e  # noqa: E402
import developer_environment_recorder as recorder  # noqa: E402
import developer_environment_test_support as support  # noqa: E402
import test_vz04_fixtures as fixtures  # noqa: E402
import vz04_common as common  # noqa: E402
import vz04_contract as contract_module  # noqa: E402
import vz04_lanes as lanes  # noqa: E402
import vz04_schema as schema  # noqa: E402

DIGEST = "b" * 64
RUN_ID = "topology-unit-run-1"
TOP21 = e2e.CRITERION_21
TOP15 = e2e.CRITERION_15
TOP1 = "gate.instances.three_concurrent_no_collision"
TOP5 = "gate.network.private_topology_paths"
TOP16 = "gate.reproducibility.recreate_from_definition"
TOP11 = "gate.delete.single_environment_safety"
IMPLEMENTED = {"bare_help", "legacy_rejection", "clean_up_refuses", "bootstrap_read_only", "help_surface_exact",
               "error_envelope_agreement", "bootstrap_creates_default", "three_concurrent_no_collision",
               "private_topology_paths", "status_json_field_set"}
# Both remaining sub-checks belong to criterion 15 and need a live typed API,
# so criterion 21 is the first topology scenario the lane can actually pass.
# Criterion 15's last blocker: agreement must be observed over the daemon's own
# gRPC channel, which needs a pinned client this lane does not have.
NOT_IMPLEMENTED = {"grpc_api_live_agreement"}
# One component that puts the fixture's `--state-root` at the depth a real gate
# run has, so no socket can be bound anywhere under it.
DEEP_STATE_ROOT_PADDING = "private-var-folders-style-gate-state-root-depth-vz04"


class TopologyLaneTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="vztl-", dir="/private/tmp"))
        self.mode_file = self.tmp / "mode"
        self.release = support.build_fake_release(self.tmp / "release", mode_file=self.mode_file)
        # A state root as deep as the gate's own: `vz04_gate` mkdtemps under
        # `/var/folders/<2>/<28>/T` and the observed run's was 115 bytes.
        # Nothing the lane binds may depend on the state root being short --
        # that is exactly what stalled every provisioning sub-check, and a short
        # fixture root hides it.
        self.state_root = self.tmp / DEEP_STATE_ROOT_PADDING / "state"
        self.contract = contract_module.load_contract()
        self.lane = contract_module.lane_by_name(self.contract)["topology"]
        self.counter = 0
        self.socket_root = recorder.socket_root_for(self.state_root)
        self.addCleanup(shutil.rmtree, self.socket_root, ignore_errors=True)

    def tearDown(self):
        fixtures.make_writable(self.release)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_fixture_state_root_reproduces_the_af_unix_constraint(self):
        """The fixture must reproduce the constraint, not dodge it."""
        state = e2e.LaneState(self.state_root, self.release / "bin")
        # Both AF_UNIX paths the old layout produced are over the limit under a
        # state root of gate depth: the daemon socket and, by 45 bytes more, a
        # Machine's Docker endpoint.
        daemon_socket = self.state_root / "topology" / "boot" / "r" / "d.sock"
        endpoint = daemon_socket.parent / ("x" * recorder.ENDPOINT_NAME_BYTES)
        for path in (daemon_socket, endpoint):
            self.assertGreater(len(str(path).encode()), recorder.SOCKET_PATH_LIMIT, path)
        budget = state.socket_budget()
        self.assertTrue(budget["bindable"], budget)
        self.assertFalse(str(state.socket).startswith(str(self.state_root)))

    def set_mode(self, mode: str):
        self.mode_file.write_text(mode)

    def argv(self, phase: str, evidence: Path, handoff=None, **overrides) -> list:
        ctx = lanes.LaneContext(run_id=RUN_ID, release_dir=self.release, release_dir_sha256=DIGEST, state_root=self.state_root,
                                contract_path=common.REPO_ROOT / common.CONFIG_FILES["e2e_contract"], contract_sha256=DIGEST,
                                candidate_tuple_sha256=DIGEST, fixture_sha256=DIGEST, clients={})
        argv = lanes.lane_argv(self.lane, ctx, phase, evidence, handoff)
        for key, value in overrides.items():
            flag = "--" + key.replace("_", "-")
            argv[argv.index(flag) + 1] = value
        return argv

    def evidence(self) -> Path:
        self.counter += 1
        path = self.tmp / f"evidence-{self.counter}"
        path.mkdir()
        return path

    def run_lane(self, argv: list, evidence: Path):
        code = e2e.main(argv, codesign_verifier=fixtures.fake_codesign_verifier)
        result = common.load_json(evidence / "lane-result.json")
        self.assertEqual(schema.validate("lane-result", result), [])
        self.assertEqual(result["test_case_retries"], 0)
        self.assertEqual((result["lane"], result["run_id"], result["candidate_tuple_sha256"]), ("topology", RUN_ID, DIGEST))
        for relative in result["evidence_files"]:
            self.assertTrue((evidence / relative).is_file(), relative)
        return code, result

    def by_slug(self, result: dict) -> dict:
        return {s["id"].split("__", 1)[1]: s for s in result["scenarios"] if "__" in s["id"]}

    def top(self, result: dict, identifier: str) -> dict:
        return next(s for s in result["scenarios"] if s["id"] == identifier)

    # -- argument handling --------------------------------------------------------------
    def test_no_identity_means_exit_2_without_result(self):
        self.assertEqual(e2e.main(["--suite", "lifecycle"], codesign_verifier=fixtures.fake_codesign_verifier), 2)
        evidence = self.evidence()
        argv = self.argv("clean-provision", evidence)
        argv[argv.index("--phase") + 1] = "not-a-phase"
        self.assertEqual(e2e.main(argv, codesign_verifier=fixtures.fake_codesign_verifier), 2)
        self.assertFalse((evidence / "lane-result.json").exists())

    def test_rejections_emit_input_rejected_result(self):
        for overrides in ({"suite": "lifecycle"}, {"release_dir": str(self.tmp)}, {"contract": str(self.tmp / "release/checksums.sha256")},
                          {"docker": "relative/docker"}):
            evidence = self.evidence()
            code, result = self.run_lane(self.argv("clean-provision", evidence, **overrides), evidence)
            self.assertEqual(code, 2, overrides)
            self.assertEqual((result["outcome"], result["failure"]["reason"]), ("failed", "input_rejected"), overrides)
            self.assertEqual(result["scenarios"], [])
        evidence = self.evidence()
        argv = self.argv("clean-provision", evidence) + ["--extra", "x"]
        code, result = self.run_lane(argv, evidence)
        self.assertEqual((code, result["failure"]["reason"]), (2, "input_rejected"))
        self.assertIn("unknown option --extra", result["failure"]["detail"])
        self.assertFalse((self.state_root / "topology").exists())

    def test_scan_argv_contract(self):
        options, problems = e2e.scan_argv(["--suite", "all", "--run-id=x"])
        self.assertEqual((options["suite"], options["run-id"]), ("all", "x"))
        self.assertTrue(any(p.startswith("missing required option --phase") for p in problems))
        _options, problems = e2e.scan_argv(["--suite", "all", "--suite", "all", "positional"])
        self.assertIn("duplicate option --suite", problems)
        self.assertIn("unexpected positional argument 'positional'", problems)

    # -- clean-provision ----------------------------------------------------------------
    def test_conformant_cli_passes_every_implemented_sub_check(self):
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual(code, 3)
        self.assertEqual((result["outcome"], result["failure"]["reason"]), ("failed", "not_implemented"))
        subs = self.by_slug(result)
        self.assertEqual(set(subs), IMPLEMENTED | NOT_IMPLEMENTED)
        for slug in IMPLEMENTED:
            self.assertEqual(subs[slug]["status"], "PASS", (slug, subs[slug]["assertions"]))
            self.assertTrue(subs[slug]["evidence"], slug)
        for slug in NOT_IMPLEMENTED:
            self.assertEqual(subs[slug]["status"], "FAIL", slug)
            self.assertTrue(any(a.startswith("not_implemented:") for a in subs[slug]["assertions"]), slug)
        # Criterion 21's sub-checks are all implemented now, so it is the first
        # topology scenario the lane can pass. Criterion 15 still needs a live
        # typed API for its remaining two.
        self.assertEqual(self.top(result, TOP21)["status"], "PASS")
        self.assertEqual(self.top(result, TOP1)["status"], "PASS")
        # The fake applies declared networks, so the check completes here. The
        # installed 0.4 runtime refuses them and the check reports
        # not_implemented instead; see check_private_topology_paths.
        self.assertEqual(self.top(result, TOP5)["status"], "PASS")
        self.assertEqual(self.top(result, TOP15)["status"], "FAIL")
        assigned = {s["id"] for s in self.contract["scenarios"] if s["lane"] == "topology" and s["phase"] == "clean-provision"}
        tops = {s["id"]: s for s in result["scenarios"] if "__" not in s["id"]}
        self.assertEqual(set(tops), assigned)
        for identifier in assigned - {TOP21, TOP15, TOP1, TOP5}:
            self.assertEqual(tops[identifier]["status"], "FAIL")
            self.assertIn("not_implemented", tops[identifier]["assertions"][0])
        cli_removal = common.load_json(common.REPO_ROOT / self.contract["pins"]["cli_removal"])
        expected = (len(cli_removal["removed_roots"]) * 9 + 18 +
                    (len(cli_removal["dev_baseline"]["help_paths"]) + len(cli_removal["normative_only_paths"])) * 4)
        self.assertTrue(any(a.startswith(f"{expected}/{expected} invocations rejected") for a in subs["legacy_rejection"]["assertions"]),
                        subs["legacy_rejection"]["assertions"][-3:])
        self.assertTrue(any("0 connections" in a for a in subs["legacy_rejection"]["assertions"]))
        starts = [s["scenario_id"] for s in result["process_starts"]]
        self.assertEqual(len(starts), len(set(starts)))
        # bootstrap_creates_default dispatches the CLI too, now that it provisions.
        self.assertEqual(set(starts), {f"{TOP21}__{s}" for s in IMPLEMENTED
                                       if s in ("bare_help", "legacy_rejection", "clean_up_refuses",
                                                "bootstrap_read_only", "bootstrap_creates_default")} |
                         {f"{TOP15}__help_surface_exact", f"{TOP15}__error_envelope_agreement",
                          f"{TOP1}__three_concurrent_no_collision",
                          f"{TOP5}__private_topology_paths", f"{TOP15}__status_json_field_set"})
        receipts = sorted((evidence / "receipts").glob("*.json"))
        self.assertGreater(len(receipts), expected)
        for path in receipts[:5] + receipts[-5:]:
            self.assertEqual(schema.validate("receipt", common.load_json(path)), [], path.name)
        self.assertEqual(result["retained_root"], str(self.state_root / "topology"))
        self.assertTrue((self.state_root / "topology").is_dir())
        self.assertEqual(result["handoff"]["produced"], e2e.HANDOFF_SENTINEL)
        self.assertTrue((evidence / e2e.HANDOFF_SENTINEL).is_file())
        self.assertEqual((result["cleanup_errors"], result["leaks"]), ([], []))
        self.assertEqual((evidence / "bare-help-observed.txt").read_bytes(),
                         (common.REPO_ROOT / "tests/fixtures/vz-0.4/cli/help-snapshot.txt").read_bytes())
        for name in ("clean-state-root-before.txt", "clean-state-root-after.txt", "bare-isolated-before.txt", "bare-isolated-after.txt"):
            self.assertTrue((evidence / "inventories" / name).is_file(), name)
        self.assertEqual((evidence / "inventories/clean-state-root-before.txt").read_bytes().split(b"\n", 2)[2],
                         (evidence / "inventories/clean-state-root-after.txt").read_bytes().split(b"\n", 2)[2])

    def test_existing_lane_state_root_is_a_prerequisite_failure(self):
        (self.state_root / "topology").mkdir(parents=True)
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual((code, result["failure"]["reason"]), (1, "prerequisite"))

    def test_existing_socket_root_is_a_prerequisite_failure(self):
        """A leaked socket root would silently share sockets between runs."""
        self.socket_root.mkdir(mode=0o700)
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual((code, result["failure"]["reason"]), (1, "prerequisite"))
        self.assertIn(str(self.socket_root), result["failure"]["detail"])
        self.assertFalse((self.state_root / "topology").exists())

    def test_unbindable_socket_root_is_a_prerequisite_failure(self):
        """Sockets nothing can bind are said once, not re-derived per sub-check."""
        deep = self.tmp / ("d" * 90)
        deep.mkdir()
        evidence = self.evidence()
        with mock.patch.object(recorder, "SOCKET_ROOT_BASE", deep):
            code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual((code, result["failure"]["reason"]), (1, "prerequisite"))
        self.assertIn("sun_path", result["failure"]["detail"])
        self.assertFalse((self.state_root / "topology").exists())

    def assert_regression(self, mode: str, slug: str, needle: str):
        self.set_mode(mode)
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual((code, result["outcome"], result["failure"]["reason"]), (1, "failed", "assertion"), mode)
        sub = self.by_slug(result)[slug]
        self.assertEqual(sub["status"], "FAIL", mode)
        self.assertTrue(any(needle in a for a in sub["assertions"]), (mode, sub["assertions"]))
        self.assertEqual(self.top(result, TOP21 if slug in ("bare_help", "legacy_rejection", "clean_up_refuses", "bootstrap_read_only") else TOP15)["status"], "FAIL")
        return result

    def test_bare_mutation_fails_bare_help(self):
        result = self.assert_regression("mutate", "bare_help", "isolated root changed: appeared: project/discovered")
        self.assertEqual(self.by_slug(result)["legacy_rejection"]["status"], "PASS")

    def test_snapshot_drift_fails_bare_help(self):
        self.assert_regression("drift", "bare_help", "FAILED: bare vz: stdout == snapshot")

    def test_executable_alias_fails_legacy_rejection(self):
        result = self.assert_regression("alias", "legacy_rejection", "vz create: exit 0 (expected 2)")
        self.assertEqual(self.by_slug(result)["bare_help"]["status"], "PASS")

    def test_provisioning_up_fails_clean_directory_check(self):
        result = self.assert_regression("provisions", "clean_up_refuses", "lane state root changed")
        self.assertTrue(any("exit 0 (expected 2)" in a for a in self.by_slug(result)["clean_up_refuses"]["assertions"]))

    def test_hanging_command_is_uncertain_effects(self):
        self.set_mode("hang")
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual((code, result["failure"]["reason"]), (1, "uncertain_effects"))
        errors = [p for p in (evidence / "receipts").glob("*.json") if common.load_json(p)["state"] == "error"]
        self.assertTrue(errors)
        self.assertTrue(all(common.load_json(p)["effects_uncertain"] for p in errors))

    def test_autospawned_daemon_is_detected_and_stopped_gracefully(self):
        self.set_mode("autospawn")
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual((code, result["failure"]["reason"]), (1, "assertion"))
        sub = self.by_slug(result)["bootstrap_read_only"]
        self.assertEqual(sub["status"], "FAIL")
        # The spawn happens in the lane's socket root, which the state-root
        # inventory cannot see; the check names that directory itself.
        self.assertTrue(any("read-only status created" in a for a in sub["assertions"]), sub["assertions"])
        self.assertEqual((result["cleanup_errors"], result["leaks"]), ([], []))
        cleanup = (evidence / "cleanup.txt").read_text()
        self.assertIn("graceful_shutdown_observed", cleanup)
        self.assertIn(str(self.release / "bin/vz-runtimed"), cleanup)
        for root in (self.state_root / "topology", self.socket_root):
            self.assertEqual(sorted(root.rglob("*.pid")), [], root)
            self.assertFalse(list(root.rglob("*.sock")), root)

    def test_unattributable_pid_file_is_a_cleanup_failure(self):
        self.set_mode("bogus_pid")
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual((code, result["failure"]["reason"]), (1, "cleanup"))
        self.assertTrue(result["cleanup_errors"])
        self.assertIn("no positively identified daemon", result["cleanup_errors"][0])

    # -- later phases -----------------------------------------------------------------
    def test_persisted_recovery_and_final_cleanup_are_honest(self):
        evidence = self.evidence()
        code, _result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual(code, 3)
        handoff = self.tmp / "state-handoff.deadbeef.json"
        handoff.write_bytes(b"{}\n")
        for phase in ("persisted-recovery/pre-sleep", "persisted-recovery/post-wake"):
            evidence = self.evidence()
            code, result = self.run_lane(self.argv(phase, evidence, handoff=str(handoff)), evidence)
            self.assertEqual((code, result["failure"]["reason"], result["phase"]), (3, "not_implemented", phase))
            self.assertEqual(result["handoff"]["consumed"], handoff.name)
            self.assertEqual(result["handoff"]["consumed_sha256"], common.digest_file(handoff))
            self.assertEqual(result["retained_root"], str(self.state_root / "topology"))
            self.assertTrue(all(s["status"] == "FAIL" for s in result["scenarios"]))
            self.assertEqual({s["id"] for s in result["scenarios"]},
                             {s["id"] for s in self.contract["scenarios"] if s["lane"] == "topology" and s["phase"] == phase})
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("final-cleanup", evidence, handoff=str(handoff)), evidence)
        # Reproducibility is implemented and passes; the phase still reports
        # not_implemented because its other scenarios are not.
        self.assertEqual((code, result["failure"]["reason"]), (3, "not_implemented"))
        self.assertEqual(self.top(result, TOP16)["status"], "PASS")
        self.assertEqual(self.top(result, TOP11)["status"], "PASS")
        self.assertIsNone(result["retained_root"])
        self.assertFalse((self.state_root / "topology").exists())
        # The socket root is owned state too: final-cleanup removes it, or the
        # lane leaks every daemon socket it ever bound.
        self.assertFalse(self.socket_root.exists(), self.socket_root)
        self.assertEqual((result["cleanup_errors"], result["leaks"]), ([], []))
        for name in ("lane-state-root-before-cleanup.txt", "lane-socket-root-before-cleanup.txt"):
            self.assertTrue((evidence / "inventories" / name).is_file(), name)

    def test_final_cleanup_reports_live_process_as_leak(self):
        root = self.state_root / "topology"
        root.mkdir(parents=True)
        (root / "leftover").write_text("x")
        # The shell must not exec away, or its argv (which names the lane state
        # root) is replaced by the child's and `ps` can no longer attribute it.
        process = subprocess.Popen(["/bin/sh", "-c", f"while :; do sleep 1; done # {root}"], stdin=subprocess.DEVNULL,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not any(pid == process.pid for pid, _ in e2e.processes_referencing(e2e.LaneState(self.state_root, self.release / "bin"))):
                time.sleep(0.05)
            evidence = self.evidence()
            code, result = self.run_lane(self.argv("final-cleanup", evidence), evidence)
        finally:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        self.assertEqual((code, result["failure"]["reason"]), (1, "cleanup"))
        self.assertTrue(any(f"pid {process.pid}" in leak["identifier"] for leak in result["leaks"]))
        self.assertTrue(root.exists(), "survivors must retain the lane state root")

    def test_wrapper_script_runs_the_lane(self):
        evidence = self.evidence()
        script = common.REPO_ROOT / "scripts/run-developer-environment-e2e.sh"
        argv = self.argv("persisted-recovery/pre-sleep", evidence)
        completed = subprocess.run([str(script), *argv], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=600, check=False,
                                   env={"PATH": os.environ["PATH"], "HOME": os.environ.get("HOME", "/")})
        # The fake release dir's dummy binaries fail the real codesign verifier only as findings; admission itself succeeds.
        self.assertEqual(completed.returncode, 3, completed.stderr.decode())
        result = common.load_json(evidence / "lane-result.json")
        self.assertEqual(schema.validate("lane-result", result), [])
        self.assertEqual(result["entry_point"]["path"], "scripts/run-developer-environment-e2e.sh")
        rejected = subprocess.run([str(script), "--suite", "lifecycle"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=600, check=False)
        self.assertEqual(rejected.returncode, 2)


# One `#[derive(...)]`-preceded struct body out of the Rust source, as
# (all serialized fields, the `skip_serializing_if` subset). The parser is
# deliberately literal: it refuses a `flatten`, `rename` or unconditional `skip`
# attribute rather than silently reporting a field set the wire never carries.
def _rust_serialized_fields(source: str, name: str) -> tuple[set, set]:
    marker = "struct " + name + " {"
    assert marker in source, "no struct " + name
    body = source.split(marker, 1)[1].split("\n}", 1)[0]
    for hostile in ("serde(flatten", "serde(rename", "serde(skip)", "serde(skip_serializing)"):
        assert hostile not in body, name + " carries " + hostile + ", which this comparison cannot model"
    fields, optional = set(), set()
    skipped_next = False
    for line in body.splitlines():
        line = line.strip()
        if line.startswith("#["):
            skipped_next = skipped_next or "skip_serializing_if" in line
            continue
        if not line or line.startswith("//"):
            continue
        field = line.split(":", 1)[0].strip()
        if not field.isidentifier():
            continue
        fields.add(field)
        if skipped_next:
            optional.add(field)
        skipped_next = False
    assert fields, "could not read " + name + " fields"
    return fields, optional


class StatusFieldSetAgreementTests(unittest.TestCase):
    """The harness's declared status field sets must be the CLI's actual ones.

    `check_status_field_set` compares an observed `vz status --json` payload
    against constants in `developer_environment_checks`. Those constants are the
    only thing that says what the document is supposed to contain, so a field
    added to the Rust structs and not to them leaves the gate check passing on a
    document it no longer fully describes -- and, for a `skip_serializing_if`
    field, passing without ever having seen it. Each set is therefore read out of
    `dev_status.rs` here rather than restated, so that drift fails offline.
    """

    SOURCE = "crates/vz-cli/src/commands/dev_status.rs"

    def setUp(self):
        self.source = (common.REPO_ROOT / self.SOURCE).read_text()

    def test_declared_field_sets_match_the_status_command_structs(self):
        for struct, declared, optional in (
                ("StatusOutput", checks.STATUS_FIELDS, checks.STATUS_OPTIONAL_FIELDS),
                ("EnvironmentStatus", checks.ENVIRONMENT_FIELDS, checks.ENVIRONMENT_OPTIONAL_FIELDS),
                ("MachineStatus", checks.MACHINE_FIELDS, checks.MACHINE_OPTIONAL_FIELDS)):
            with self.subTest(struct=struct):
                fields, skipped = _rust_serialized_fields(self.source, struct)
                self.assertEqual(fields, declared)
                self.assertEqual(skipped, optional)
                self.assertLessEqual(optional, declared)

    def test_the_parser_would_notice_an_added_or_newly_optional_field(self):
        """Failing-before, in process: neither comparison above is vacuous."""
        drifted = self.source.replace(
            "    machine_id: String,\n    name: String,",
            "    machine_id: String,\n    topology: Vec<String>,\n    name: String,", 1)
        self.assertNotEqual(drifted, self.source)
        fields, _ = _rust_serialized_fields(drifted, "MachineStatus")
        self.assertEqual(fields - checks.MACHINE_FIELDS, {"topology"})
        newly_optional = self.source.replace(
            "    machine_id: String,",
            '    #[serde(skip_serializing_if = "String::is_empty")]\n    machine_id: String,', 1)
        _, skipped = _rust_serialized_fields(newly_optional, "MachineStatus")
        self.assertEqual(skipped - checks.MACHINE_OPTIONAL_FIELDS, {"machine_id"})


if __name__ == "__main__":
    unittest.main()
