import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import docker_compatibility_contract as docker_contract  # noqa: E402
import vz04_common as common  # noqa: E402
import vz04_contract as contract_module  # noqa: E402


class ContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = contract_module.load_contract()
        cls.docker = contract_module.load_docker_contract()

    def test_required_inventory_is_85_unique_ids(self):
        rows = contract_module.required_scenarios(self.contract, self.docker)
        self.assertEqual(len(rows), 85)
        self.assertEqual(len({row["id"] for row in rows}), 85)
        self.assertEqual(sum(row["id"].startswith("gate.") for row in rows), 22)
        self.assertEqual({row["id"] for row in rows if row["id"].startswith("docker.")}, set(docker_contract.REQUIRED_IDS))
        self.assertTrue(all(row["lane"] == "linux-docker" for row in rows if row["id"].startswith("docker.")))
        self.assertTrue(all(row["phase"] in common.LANE_PHASES for row in rows))

    def test_gate_ids_match_plan_table(self):
        ids = [s["id"] for s in self.contract["scenarios"]]
        self.assertEqual(ids[0], "gate.instances.three_concurrent_no_collision")
        self.assertEqual(ids[21], "gate.definition.reconciliation_fencing")
        self.assertIn("gate.lifecycle.recovery_including_sleep_wake", ids)
        by_id = {s["id"]: s for s in self.contract["scenarios"]}
        self.assertEqual(by_id["gate.lifecycle.recovery_including_sleep_wake"]["phase"], "persisted-recovery/post-wake")
        self.assertEqual(by_id["gate.runtime.youki_only_provenance"]["lane"], "linux-docker")

    def test_duplicate_or_missing_criterion_rejected(self):
        broken = copy.deepcopy(self.contract)
        broken["scenarios"][1]["id"] = broken["scenarios"][0]["id"]
        with self.assertRaises(common.GateError):
            contract_module.required_scenarios(broken, self.docker)
        broken = copy.deepcopy(self.contract)
        broken["scenarios"][1]["criterion"] = 1
        with self.assertRaises(common.GateError):
            contract_module.required_scenarios(broken, self.docker)
        broken_docker = copy.deepcopy(self.docker)
        broken_docker["scenarios"].pop()
        with self.assertRaises(common.GateError):
            contract_module.required_scenarios(self.contract, broken_docker)

    def test_frozen_inputs_report_draft_state_and_stable_digests(self):
        frozen = contract_module.frozen_inputs(self.contract)
        codes = {code for code, _s, _d in frozen["findings"]}
        self.assertIn("input.draft", codes)
        self.assertIn("contract.unpinned", codes)
        self.assertEqual(frozen["inputs"]["e2e_contract"]["sha256"],
                         common.digest_file(common.REPO_ROOT / common.CONFIG_FILES["e2e_contract"]))
        again = contract_module.frozen_inputs(self.contract)
        self.assertEqual(frozen["digests"], again["digests"])
        self.assertEqual(len(frozen["digests"]["fixture_dirs"]), len(self.contract["fixtures"]["required_dirs"]))
        for key in common.CONFIG_FILES:
            self.assertIn(key, frozen["inputs"])

    def test_missing_harness_file_is_a_finding_not_an_error(self):
        broken = copy.deepcopy(self.contract)
        broken["harness"]["files"].append("scripts/does-not-exist.sh")
        frozen = contract_module.frozen_inputs(broken)
        self.assertTrue(any(code == "harness.missing" and subject == "scripts/does-not-exist.sh" for code, subject, _ in frozen["findings"]))


if __name__ == "__main__":
    unittest.main()
