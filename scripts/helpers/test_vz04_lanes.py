from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_vz04_fixtures as fixtures  # noqa: E402
import vz04_common as common  # noqa: E402
import vz04_contract as contract_module  # noqa: E402
import vz04_lanes as lanes  # noqa: E402
import vz04_schema as schema  # noqa: E402

DIGEST = "a" * 64


def _ctx(root):
    return lanes.LaneContext(run_id="gate-test-run-1", release_dir=root, release_dir_sha256=DIGEST, state_root=root / "state",
                             contract_path=root / "c.json", contract_sha256=DIGEST, candidate_tuple_sha256=DIGEST, fixture_sha256=DIGEST,
                             clients={"docker": None, "compose_plugin": None, "buildx_plugin": None})


def _passed(lane, phase, ids, ctx):
    result = lanes.base_result(lane, phase, ctx, {"path": "scripts/x.sh", "sha256": DIGEST, "argv": []})
    result["outcome"] = "passed"
    result["scenarios"] = [{"id": i, "status": "PASS", "started_unix_ns": 1, "ended_unix_ns": 2, "assertions": ["ok"], "evidence": [],
                            "readiness_polls": []} for i in ids]
    return result


class LaneTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="vz04-lanes-")
        self.root = Path(self.tmp.name).resolve()
        self.ctx = _ctx(self.root)
        self.contract = contract_module.load_contract()
        self.lanes = contract_module.lane_by_name(self.contract)

    def tearDown(self):
        self.tmp.cleanup()

    def test_not_implemented_result_is_schema_valid(self):
        result = lanes.failed_result("topology", "clean-provision", self.ctx, {"path": "scripts/x.sh", "sha256": DIGEST, "argv": []},
                                     "not_implemented", "stub", 3)
        self.assertEqual(schema.validate("lane-result", result), [])
        self.assertEqual(result["outcome"], "failed")

    def test_dry_invocation_writes_not_implemented_without_running(self):
        directory = self.root / "topology" / "clean-provision"
        result = lanes.invoke_lane(self.lanes["topology"], "clean-provision", self.ctx, directory, dry=True)
        self.assertEqual(result["failure"]["reason"], "not_implemented")
        self.assertEqual(common.load_json(directory / "lane-result.json"), result)
        self.assertFalse((directory / "lane.stdout").exists())

    def test_lane_argv_carries_everything(self):
        argv = lanes.lane_argv(self.lanes["topology"], self.ctx, "final-cleanup", self.root / "e", "/handoff.json")
        for flag in ("--suite", "--run-id", "--phase", "--release-dir", "--evidence-dir", "--state-root", "--contract",
                     "--candidate-tuple", "--fixture-sha256", "--handoff", "--docker", "--compose-plugin", "--buildx-plugin"):
            self.assertIn(flag, argv)
        sandbox = lanes.lane_argv(self.lanes["sandbox-vm"], self.ctx, "clean-provision", self.root / "e", None)
        self.assertEqual(sandbox[:4], ["--suite", "all", "--profile", "release"])
        self.assertIn("--output-dir", sandbox)

    def test_stub_script_writes_valid_result_and_exits_3(self):
        """The native-macOS lane is still a stub: it must account for itself rather
        than be absent. The topology lane is a real lane and is covered by
        `test_developer_environment_e2e`."""
        release = fixtures.build_fake_release_dir(self.root / "release")
        try:
            evidence = self.root / "evidence"
            evidence.mkdir()
            script = common.REPO_ROOT / "scripts/run-macos-developer-environment-e2e.sh"
            argv = lanes.lane_argv(self.lanes["native-macos"], lanes.LaneContext(
                run_id="gate-test-run-1", release_dir=release, release_dir_sha256=common.tree_digest(release), state_root=self.root / "state",
                contract_path=common.REPO_ROOT / common.CONFIG_FILES["e2e_contract"], contract_sha256=DIGEST, candidate_tuple_sha256=DIGEST,
                fixture_sha256=DIGEST, clients={}), "clean-provision", evidence, None)
            completed = subprocess.run([str(script), *argv], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300, check=False)
            self.assertEqual(completed.returncode, 3, completed.stderr.decode())
            result = common.load_json(evidence / "lane-result.json")
            self.assertEqual(schema.validate("lane-result", result), [])
            self.assertEqual((result["lane"], result["failure"]["reason"], result["failure"]["exit_code"]),
                             ("native-macos", "not_implemented", 3))
            self.assertEqual(result["entry_point"]["path"], "scripts/run-macos-developer-environment-e2e.sh")
            rejected = subprocess.run([str(script), "--suite", "lifecycle"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300, check=False)
            self.assertEqual(rejected.returncode, 2)
        finally:
            fixtures.make_writable(release)

    def test_sandbox_summary_translation(self):
        run_dir = self.root / "run"
        run_dir.mkdir()
        (run_dir / "summary.txt").write_text("passed=runtime stack\nfailed=none\nother=x\n")
        (run_dir / "run-info.txt").write_text("host=x\n")
        evidence = self.root / "evidence-pass"
        evidence.mkdir()
        entry = {"path": "scripts/run-sandbox-vm-e2e.sh", "sha256": DIGEST, "argv": []}
        result = lanes.translate_sandbox_summary("sandbox-vm", "clean-provision", self.ctx, entry, run_dir, 0, evidence)
        self.assertEqual(schema.validate("lane-result", result), [])
        self.assertEqual(result["outcome"], "passed")
        self.assertEqual(result["result_adapter"]["passed"], ["runtime", "stack"])
        self.assertTrue((evidence / "sandbox-summary.txt").exists())
        (run_dir / "summary.txt").write_text("passed=runtime\nfailed=stack:1\n")
        evidence = self.root / "evidence-fail"
        evidence.mkdir()
        result = lanes.translate_sandbox_summary("sandbox-vm", "clean-provision", self.ctx, entry, run_dir, 1, evidence)
        self.assertEqual(schema.validate("lane-result", result), [])
        self.assertEqual((result["outcome"], result["failure"]["reason"]), ("failed", "assertion"))

    def test_accounting_rules(self):
        required = [{"id": "gate.a.b", "lane": "topology", "phase": "clean-provision"},
                    {"id": "gate.c.d", "lane": "topology", "phase": "clean-provision"},
                    {"id": "docker.e.f", "lane": "linux-docker", "phase": "final-cleanup"},
                    {"id": "gate.g.h", "lane": "native-macos", "phase": "clean-provision"}]
        topology = _passed("topology", "clean-provision", ["gate.a.b", "gate.x.y"], self.ctx)
        docker = lanes.failed_result("linux-docker", "final-cleanup", self.ctx, {"path": "s", "sha256": DIGEST, "argv": []}, "not_implemented", "d")
        outcome = lanes.account(required, [topology, docker])
        status = {row["id"]: (row["status"], row["reason"]) for row in outcome["rows"]}
        self.assertEqual(status["gate.a.b"], ("PASS", None))
        self.assertEqual(status["gate.c.d"], ("MISSING", "not_reported"))
        self.assertEqual(status["docker.e.f"], ("MISSING", "not_implemented"))
        self.assertEqual(status["gate.g.h"], ("MISSING", "lane_result_absent"))
        codes = {code for code, _s, _d in outcome["findings"]}
        self.assertEqual(codes, {"scenario.unknown", "scenario.missing"})
        duplicate = _passed("linux-docker", "final-cleanup", ["gate.a.b", "docker.e.f"], self.ctx)
        outcome = lanes.account(required, [topology, duplicate])
        codes = {code for code, _s, _d in outcome["findings"]}
        self.assertIn("scenario.duplicate", codes)
        self.assertIn("scenario.misassigned", codes)


if __name__ == "__main__":
    unittest.main()
