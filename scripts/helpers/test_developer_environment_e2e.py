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

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

import developer_environment_e2e as e2e  # noqa: E402
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
IMPLEMENTED = {"bare_help", "legacy_rejection", "clean_up_refuses", "bootstrap_read_only", "help_surface_exact", "error_envelope_agreement"}
NOT_IMPLEMENTED = {"bootstrap_creates_default", "status_json_field_set", "grpc_api_live_agreement"}


class TopologyLaneTests(unittest.TestCase):
    def setUp(self):
        # Short root: isolated Unix socket paths must stay under macOS's sun_path limit.
        self.tmp = Path(tempfile.mkdtemp(prefix="vztl-", dir="/private/tmp"))
        self.mode_file = self.tmp / "mode"
        self.release = support.build_fake_release(self.tmp / "release", mode_file=self.mode_file)
        self.state_root = self.tmp / "state"
        self.contract = contract_module.load_contract()
        self.lane = contract_module.lane_by_name(self.contract)["topology"]
        self.counter = 0

    def tearDown(self):
        fixtures.make_writable(self.release)
        shutil.rmtree(self.tmp, ignore_errors=True)

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
        self.assertEqual(self.top(result, TOP21)["status"], "FAIL")
        self.assertEqual(self.top(result, TOP15)["status"], "FAIL")
        assigned = {s["id"] for s in self.contract["scenarios"] if s["lane"] == "topology" and s["phase"] == "clean-provision"}
        tops = {s["id"]: s for s in result["scenarios"] if "__" not in s["id"]}
        self.assertEqual(set(tops), assigned)
        for identifier in assigned - {TOP21, TOP15}:
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
        self.assertEqual(set(starts), {f"{TOP21}__{s}" for s in IMPLEMENTED if s in ("bare_help", "legacy_rejection", "clean_up_refuses", "bootstrap_read_only")} |
                         {f"{TOP15}__help_surface_exact", f"{TOP15}__error_envelope_agreement"})
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
        self.assertTrue(any("lane state root changed" in a for a in sub["assertions"]), sub["assertions"])
        self.assertEqual((result["cleanup_errors"], result["leaks"]), ([], []))
        cleanup = (evidence / "cleanup.txt").read_text()
        self.assertIn("graceful_shutdown_observed", cleanup)
        self.assertIn(str(self.release / "bin/vz-runtimed"), cleanup)
        self.assertEqual(sorted((self.state_root / "topology").rglob("*.pid")), [])
        self.assertFalse(list((self.state_root / "topology").rglob("*.sock")))

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
        self.assertEqual((code, result["failure"]["reason"]), (3, "not_implemented"))
        self.assertIsNone(result["retained_root"])
        self.assertFalse((self.state_root / "topology").exists())
        self.assertEqual((result["cleanup_errors"], result["leaks"]), ([], []))
        self.assertTrue((evidence / "inventories/lane-state-root-before-cleanup.txt").is_file())

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


if __name__ == "__main__":
    unittest.main()
