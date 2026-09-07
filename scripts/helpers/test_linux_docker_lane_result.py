"""Lane-result translation for the linux-docker lane; offline, never Docker evidence."""
import json
import os
from pathlib import Path
import tempfile
import unittest

import linux_docker_lane_result as subject
import linux_docker_scenarios as scenarios

try:
    import vz04_schema as schema
except ImportError:  # jsonschema absent: structural assertions still run
    schema = None

REPO = Path(__file__).resolve().parents[2]
CANDIDATE = REPO / ".artifacts/linux-docker-limits-candidate-2"
DIGEST = "a" * 64


class Scratch(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="vz-lane-result-")
        self.root = Path(self.tmp.name).resolve()
        self.lane_dir = self.root / "linux-docker" / "clean-provision"
        self.lane_dir.mkdir(parents=True)
        (self.root / "release").mkdir()
        (self.root / "release" / "vz").write_bytes(b"release")
        (self.root / "contract.json").write_text("{}\n")
        (self.root / "state").mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def argv(self, suite="limits", phase="clean-provision", handoff="none", **extra):
        argv = ["--suite", suite, "--run-id", "gate-run-000001", "--phase", phase, "--release-dir", str(self.root / "release"),
                "--evidence-dir", str(self.lane_dir), "--state-root", str(self.root / "state"),
                "--contract", str(self.root / "contract.json"), "--candidate-tuple", DIGEST, "--fixture-sha256", "b" * 64,
                "--handoff", handoff]
        for key, value in extra.items():
            argv += ["--" + key.replace("_", "-"), value]
        return argv

    def validate(self, lane):
        if schema is not None:
            self.assertEqual(schema.validate("lane-result", lane), [])


class GateContextTests(Scratch):
    def test_context_only_when_run_id_phase_and_candidate_tuple_are_all_present(self):
        self.assertIsNone(subject.gate_context(["--suite", "limits"]))
        self.assertIsNone(subject.gate_context(["--suite", "limits", "--run-id", "x", "--phase", "clean-provision"]))
        self.assertIsNone(subject.gate_context(["--suite", "limits", "--run-id=x", "--candidate-tuple", DIGEST]))
        ctx = subject.gate_context(self.argv())
        self.assertIsNotNone(ctx)
        ctx.validate()
        self.assertEqual(ctx.harness_dir(), self.lane_dir / "harness")
        self.assertEqual(ctx.result_path(), self.lane_dir / "lane-result.json")
        equals = subject.gate_context(["--suite=all", "--run-id=gate-run-000001", "--phase=final-cleanup", "--candidate-tuple=" + DIGEST])
        self.assertEqual((equals.suite, equals.phase), ("all", "final-cleanup"))

    def test_gate_options_are_all_or_nothing_and_checked(self):
        with self.assertRaisesRegex(ValueError, "all-or-nothing.*--contract"):
            subject.gate_context([a for a in self.argv() if not a.startswith(str(self.root / "contract.json"))
                                  and a != "--contract"]).validate()
        for bad in ({"phase": "warm-up"}, {"candidate_tuple": "zz"}, {"fixture_sha256": "short"}, {"handoff": "relative.json"},
                    {"state_root": "relative"}, {"run_id": "UPPER"}):
            argv = self.argv()
            for key, value in bad.items():
                flag = "--" + key.replace("_", "-")
                argv[argv.index(flag) + 1] = value
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                subject.gate_context(argv).validate()


class ResultShapeTests(Scratch):
    def test_not_implemented_and_input_rejected_results_are_schema_valid(self):
        ctx = subject.gate_context(self.argv(suite="all")).validate()
        lane = subject.failed(ctx, "not_implemented", "full 63-scenario --suite all is not implemented", 2)
        self.validate(lane)
        self.assertEqual((lane["lane"], lane["phase"], lane["run_id"], lane["outcome"]), ("linux-docker", "clean-provision", "gate-run-000001", "failed"))
        self.assertEqual(lane["failure"], {"reason": "not_implemented", "detail": "full 63-scenario --suite all is not implemented", "exit_code": 2})
        self.assertEqual(lane["entry_point"]["path"], "scripts/run-linux-docker-e2e.sh")
        self.assertEqual(lane["entry_point"]["argv"], self.argv(suite="all"))
        self.assertEqual(lane["candidate_tuple_sha256"], DIGEST)
        self.assertEqual(lane["fixture_sha256"], "b" * 64)
        self.assertEqual(lane["scenarios"], [])
        self.assertEqual(lane["test_case_retries"], 0)
        self.assertEqual(lane["handoff"], {"produced": None, "consumed": None, "consumed_sha256": None})
        self.assertIsNone(lane["result_adapter"])
        path = subject.write(ctx, lane)
        self.assertEqual(json.loads(path.read_text()), lane)
        with self.assertRaisesRegex(ValueError, "already exists"):
            subject.write(ctx, lane)
        rejected = subject.failed(ctx, "input_rejected", "required option: --release-dir", 2)
        self.validate(rejected)

    def test_handoff_is_recorded_by_name_and_digest(self):
        handoff = self.root / "state-handoff.abc.json"
        handoff.write_text('{"complete": true}\n')
        ctx = subject.gate_context(self.argv(handoff=str(handoff))).validate()
        lane = subject.base(ctx)
        self.assertEqual(lane["handoff"]["consumed"], "state-handoff.abc.json")
        self.assertEqual(len(lane["handoff"]["consumed_sha256"]), 64)
        self.validate(subject.failed(ctx, "crash", "x", None))

    def test_failure_reason_mapping(self):
        self.assertEqual(subject.failure_reason("Rejected: x", ["cleanup proof unresolved"])[0], "cleanup")
        self.assertEqual(subject.failure_reason("Rejected: uncertain mutation: resources retained", [])[0], "uncertain_effects")
        self.assertEqual(subject.failure_reason("KeyboardInterrupt: ", [])[0], "crash")
        self.assertEqual(subject.failure_reason("TimeoutExpired: Command timed out", [])[0], "timeout")
        self.assertEqual(subject.failure_reason("Rejected: sibling limits changed", [])[0], "assertion")
        self.assertEqual(subject.failure_reason(None, [])[0], "assertion")

    def synthetic_harness(self, *, passed=True, guard=True, runc=False):
        harness = self.lane_dir / "harness"
        harness.mkdir()
        info = {"DefaultRuntime": "runc" if runc else "youki", "Runtimes": {"youki": {"path": "/mnt/linux-bin/youki"}, "runc": {"path": "runc"}}}
        rows = [("001-engine-info", ["docker", "info"], json.dumps(info) if guard else "id\n"),
                ("002-context-inspect", ["docker", "context", "inspect"],
                 json.dumps([{"Name": "ctx", "Endpoints": {"docker": {"Host": "unix:///private/tmp/x/r/vzr1.sock"}}}]))]
        for stem, argv, stdout in rows:
            (harness / (stem + ".intent.json")).write_text(json.dumps({"label": stem[4:], "argv": argv, "argv0": argv[0],
                                                                        "executable": "/usr/local/bin/docker", "started_unix_ns": 1000}))
            (harness / (stem + ".stdout")).write_text(stdout)
        (harness / "limits-machine-0").mkdir()
        (harness / "limits-machine-0" / "machine-limits-validation.json").write_text("{}")
        (harness / "checksums.sha256").write_text(DIGEST + "  001-engine-info.stdout\n" + DIGEST + "  limits-machine-0/machine-limits-validation.json\n")
        result = {"suite": "limits", "outcome": "passed_dev_installed_limits_slice" if passed else "failed",
                  "error": None if passed else "Rejected: sibling limits changed", "cleanup_errors": [],
                  "retained_root": "/private/tmp/vzdev-x", "release_scenarios_passed": [],
                  "scenario": {"machine_slices": [{"started_unix_ns": 1500, "ended_unix_ns": 2500,
                                                   "workload": {"sibling_health": {"samples": 60}}}]}}
        return harness, result, {"suite": "limits"}

    def test_passed_run_translates_to_pass_scenarios_receipts_and_evidence(self):
        harness, result, info = self.synthetic_harness()
        ctx = subject.gate_context(self.argv()).validate()
        lane = subject.from_run(ctx, result, info, harness, 0)
        self.validate(lane)
        self.assertEqual(lane["outcome"], "passed")
        self.assertIsNone(lane["failure"])
        self.assertEqual([s["id"] for s in lane["scenarios"]], ["docker.operation.resource_limits", "docker.operation.oom"])
        self.assertEqual({s["status"] for s in lane["scenarios"]}, {"PASS"})
        self.assertEqual(lane["scenarios"][0]["evidence"], ["harness/limits-machine-0/machine-limits-validation.json"])
        self.assertEqual(lane["scenarios"][0]["readiness_polls"][0]["samples"], 60)
        self.assertEqual(lane["process_starts"], [{"scenario_id": "docker.operation.resource_limits", "argv0": "docker", "pid": None}] * 2)
        self.assertEqual(lane["prohibited_observed"], {k: False for k in subject.PROHIBITED_KEYS})
        self.assertEqual(lane["retained_root"], "/private/tmp/vzdev-x")
        self.assertEqual(lane["evidence_files"], ["harness/001-engine-info.stdout", "harness/limits-machine-0/machine-limits-validation.json",
                                                  "harness/checksums.sha256"])
        self.assertEqual(lane["release_dir_sha256"] != DIGEST, True)

    def test_failed_run_maps_reason_and_marks_every_suite_scenario_fail(self):
        harness, result, info = self.synthetic_harness(passed=False)
        ctx = subject.gate_context(self.argv()).validate()
        lane = subject.from_run(ctx, result, info, harness, 1)
        self.validate(lane)
        self.assertEqual(lane["failure"], {"reason": "assertion", "detail": "Rejected: sibling limits changed", "exit_code": 1})
        self.assertEqual({s["status"] for s in lane["scenarios"]}, {"FAIL"})
        result["cleanup_errors"] = ["Rejected: cleanup proof unresolved"]
        lane = subject.from_run(ctx, result, info, harness, 1)
        self.validate(lane)
        self.assertEqual(lane["failure"]["reason"], "cleanup")
        self.assertEqual(lane["cleanup_errors"], ["Rejected: cleanup proof unresolved"])

    def test_passed_run_without_runtime_guard_or_with_prohibited_runtime_cannot_pass(self):
        harness, result, info = self.synthetic_harness(guard=False)
        ctx = subject.gate_context(self.argv()).validate()
        lane = subject.from_run(ctx, result, info, harness, 0)
        self.validate(lane)
        self.assertEqual(lane["outcome"], "failed")
        self.assertIn("runtime guard proof", lane["failure"]["detail"])
        self.tearDown(); self.setUp()
        harness, result, info = self.synthetic_harness(runc=True)
        lane = subject.from_run(subject.gate_context(self.argv()).validate(), result, info, harness, 0)
        self.validate(lane)
        self.assertTrue(lane["prohibited_observed"]["runc"])
        self.assertEqual(lane["outcome"], "failed")
        self.assertIn("prohibited component observed: runc", lane["failure"]["detail"])

    def test_other_phase_yields_no_scenarios_but_stays_valid(self):
        harness, result, info = self.synthetic_harness()
        ctx = subject.gate_context(self.argv(phase="final-cleanup")).validate()
        lane = subject.from_run(ctx, result, info, harness, 0)
        self.validate(lane)
        self.assertEqual(lane["scenarios"], [])
        self.assertEqual(lane["outcome"], "passed")


@unittest.skipUnless((CANDIDATE / "result.json").is_file() and schema is not None, "retained limits candidate or jsonschema unavailable")
class RetainedCandidateTests(Scratch):
    def test_retained_passing_limits_candidate_yields_schema_valid_passed_lane_result(self):
        result = json.loads((CANDIDATE / "result.json").read_text())
        info = json.loads((CANDIDATE / "inputs.json").read_text())
        ctx = subject.gate_context(self.argv()).validate()
        lane = subject.from_run(ctx, result, info, CANDIDATE, 0, prefix="harness")
        self.assertEqual(schema.validate("lane-result", lane), [])
        self.assertEqual(lane["outcome"], "passed", lane["failure"])
        self.assertEqual([s["id"] for s in lane["scenarios"]], list(scenarios.for_suite("limits")))
        self.assertEqual({s["status"] for s in lane["scenarios"]}, {"PASS"})
        for entry in lane["scenarios"]:
            self.assertEqual(len(entry["evidence"]), 3)
            self.assertEqual(entry["readiness_polls"], [{"id": "poll.service.health_probe", "samples": 180, "deadline_seconds": 60, "satisfied": True}])
            self.assertLess(entry["started_unix_ns"], entry["ended_unix_ns"])
        self.assertGreater(len(lane["process_starts"]), 100)
        self.assertTrue(all(p["scenario_id"] == "docker.operation.resource_limits" and p["pid"] is None for p in lane["process_starts"]))
        self.assertEqual(lane["prohibited_observed"], {k: False for k in subject.PROHIBITED_KEYS})
        self.assertEqual(lane["retained_root"], result["retained_root"])
        self.assertIn("harness/result.json", lane["evidence_files"])
        self.assertIn("harness/checksums.sha256", lane["evidence_files"])
        subject.write(ctx, lane)
        self.assertEqual(os.stat(ctx.result_path()).st_size > 1000, True)


if __name__ == "__main__":
    unittest.main()
