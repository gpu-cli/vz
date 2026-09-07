import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import vz04_common as common  # noqa: E402
import vz04_schema as schema  # noqa: E402


def _lane_result():
    digest = "0" * 64
    return {"schema_version": 1, "kind": "vz-0.4-lane-result", "lane": "topology", "phase": "clean-provision",
            "run_id": "gate-test-run-1", "candidate_tuple_sha256": digest, "release_dir_sha256": digest, "fixture_sha256": digest,
            "contract_sha256": digest, "entry_point": {"path": "scripts/x.sh", "sha256": digest, "argv": []}, "outcome": "failed",
            "failure": {"reason": "not_implemented", "detail": "stub", "exit_code": 3}, "scenarios": [], "test_case_retries": 0,
            "process_starts": [], "prohibited_observed": {k: False for k in ("docker_desktop", "host_system_daemon", "runc", "crun",
                                                                            "cargo_run", "path_fallback", "ssh_hosts")},
            "leaks": [], "cleanup_errors": [], "handoff": {"produced": None, "consumed": None, "consumed_sha256": None},
            "retained_root": None, "evidence_files": [], "result_adapter": None}


class SchemaTests(unittest.TestCase):
    def test_every_gate_schema_loads(self):
        for name in schema.INPUT_SCHEMAS + schema.EVIDENCE_SCHEMAS:
            self.assertTrue(schema.load_schema(name)["title"], name)

    def test_checked_in_inputs_validate(self):
        for name, key in (("e2e-contract", "e2e_contract"), ("migration-barriers", "migration_barriers"),
                          ("decisions", "decisions"), ("decision-authorities", "decision_authorities")):
            value = common.load_json(common.REPO_ROOT / common.CONFIG_FILES[key])
            self.assertEqual(schema.validate(name, value), [], name)
        barriers = common.load_json(common.REPO_ROOT / common.CONFIG_FILES["migration_barriers"])
        self.assertGreaterEqual(len(barriers["barriers"]), 1)
        self.assertEqual(len({b["id"] for b in barriers["barriers"]}), len(barriers["barriers"]))

    def test_barriers_inventory_must_not_be_empty(self):
        barriers = common.load_json(common.REPO_ROOT / common.CONFIG_FILES["migration_barriers"])
        barriers["barriers"] = []
        self.assertTrue(schema.validate("migration-barriers", barriers))

    def test_lane_result_if_then_rules(self):
        result = _lane_result()
        self.assertEqual(schema.validate("lane-result", result), [])
        passed = copy.deepcopy(result)
        passed["outcome"] = "passed"
        self.assertTrue(schema.validate("lane-result", passed), "passed with failure object must be rejected")
        passed["failure"] = None
        self.assertEqual(schema.validate("lane-result", passed), [])
        passed["cleanup_errors"] = ["x"]
        self.assertTrue(schema.validate("lane-result", passed))
        retried = copy.deepcopy(result)
        retried["test_case_retries"] = 1
        self.assertTrue(schema.validate("lane-result", retried))
        skipped = copy.deepcopy(result)
        skipped["outcome"] = "skipped"
        self.assertTrue(schema.validate("lane-result", skipped))
        extra = copy.deepcopy(result)
        extra["bonus"] = 1
        self.assertTrue(schema.validate("lane-result", extra))

    def test_summary_pass_requires_zero_findings(self):
        summary = {"schema_version": 1, "kind": "vz-0.4-summary", "run_id": "gate-test-run-1", "verdict": "PASS",
                   "candidate_tuple_sha256": "0" * 64, "developer_overrides": [],
                   "counts": {"required": 1, "PASS": 1, "FAIL": 0, "MISSING": 0, "findings": 0},
                   "scenarios": [{"id": "gate.cli_api.agreement", "lane": "topology", "phase": "clean-provision", "status": "PASS", "reason": None}],
                   "lanes": [], "findings": []}
        self.assertEqual(schema.validate("summary", summary), [])
        summary["findings"] = [{"code": "x.y", "subject": "s", "detail": "d"}]
        self.assertTrue(schema.validate("summary", summary))
        summary["findings"] = []
        summary["developer_overrides"] = ["dry_lanes"]
        self.assertTrue(schema.validate("summary", summary))

    def test_sleep_wake_observed_requires_bindings(self):
        value = {"schema_version": 1, "kind": "vz-0.4-sleep-wake", "run_id": "gate-test-run-1", "minimum_sleep_seconds": 20,
                 "observed": False, "reason": "not_observed_step1", "checkpoint": None, "wake": None}
        self.assertEqual(schema.validate("sleep-wake", value), [])
        value["observed"] = True
        self.assertTrue(schema.validate("sleep-wake", value))

    def test_receipt_not_executed_rules(self):
        receipt = {"schema_version": 1, "kind": "vz-0.4-receipt", "run_id": "gate-test-run-1", "index": 1, "label": "cargo-fmt",
                   "argv": ["cargo", "fmt"], "executable": "cargo", "cwd": "/repo", "timeout_seconds": 10, "state": "not_executed",
                   "started_unix_ns": None, "ended_unix_ns": None, "exit_code": None, "stdout_path": None, "stderr_path": None,
                   "stdout_sha256": None, "stderr_sha256": None, "error": None, "effects_uncertain": False, "canary_withheld": False,
                   "not_executed_reason": "dry"}
        self.assertEqual(schema.validate("receipt", receipt), [])
        receipt["exit_code"] = 0
        self.assertTrue(schema.validate("receipt", receipt))


if __name__ == "__main__":
    unittest.main()
