"""Adversarial contract-input tests. Synthetic files are never Docker evidence.

uv run --with jsonschema==4.23.0 python scripts/helpers/test_docker_compatibility_contract.py
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from jsonschema import Draft202012Validator

import docker_compatibility_contract as contract


class DockerCompatibilityContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = contract.load_json(contract.ROOT, contract.CONTRACT)
        cls.schema = contract.load_json(contract.ROOT, contract.SCHEMA)

    def changed(self):
        return copy.deepcopy(self.manifest)

    def reject(self, value, root=contract.ROOT, draft=True):
        with self.assertRaises(contract.InvalidContract):
            contract.validate_contract(value, root, check_draft=draft)

    def fixture_root(self, path):
        """A synthetic preflight input tree, not a real compatibility harness."""
        root = Path(path)
        for relative in (contract.SCHEMA, contract.GOAL):
            destination = root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(contract.read_regular(contract.ROOT, relative))
        fixtures = root / self.manifest["fixture_bundle"]["path"]
        fixtures.mkdir(parents=True)
        (fixtures / "unit-test-only.txt").write_text("synthetic fixture input, not Docker proof\n")
        harness = root / self.manifest["harness"]["entry_point"]
        harness.parent.mkdir(parents=True, exist_ok=True)
        harness.write_text("#!/bin/sh\n# Synthetic preflight file; never executed by tests.\nexit 99\n")
        harness.chmod(0o700)
        value = self.changed()
        value["contract_state"] = "inputs_frozen"
        value["fixture_bundle"].update(state="pinned", sha256=contract.fixture_tree_digest(root, value["fixture_bundle"]["path"]))
        value["harness"].update(state="pinned", sha256=hashlib.sha256(harness.read_bytes()).hexdigest())
        return root, value, fixtures, harness

    def test_schema_and_truthful_draft_inventory(self):
        Draft202012Validator.check_schema(self.schema)
        result = contract.validate_contract(self.manifest, check_draft=True)
        self.assertEqual(result, {"kind": "draft_structure_only", "required_scenarios": 63,
                                 "unresolved_required_pins": ["fixture_bundle", "harness"],
                                 "docker_tests_executed": 0, "compatibility_certified": False})
        self.assertEqual(len(contract.REQUIRED_IDS), 63)

    def test_default_gate_rejects_draft_and_each_unresolved_pin(self):
        self.reject(self.manifest, draft=False)
        with tempfile.TemporaryDirectory() as directory:
            root, value, _, _ = self.fixture_root(directory)
            for name in ("fixture_bundle", "harness"):
                changed = copy.deepcopy(value)
                changed[name].update(state="pending", sha256=None)
                self.reject(changed, root, draft=False)

    def test_every_minimum_behavior_is_individually_required(self):
        for index, scenario in enumerate(self.manifest["scenarios"]):
            with self.subTest(scenario=scenario["id"]):
                value = self.changed()
                del value["scenarios"][index]
                self.reject(value)
                value = self.changed()
                value["scenarios"][index] = copy.deepcopy(value["scenarios"][(index + 1) % 63])
                self.reject(value)
                value = self.changed()
                value["scenarios"][index]["id"] += "_replacement"
                self.reject(value)

    def test_each_expected_result_command_scope_and_evidence_is_locked(self):
        for index, scenario in enumerate(self.manifest["scenarios"]):
            for field, replacement in (("expected", {}), ("command_paths", [["info"]]),
                                       ("scope", "environment_global"), ("required", False),
                                       ("evidence", ["summary_only"])):
                if scenario[field] == replacement:
                    continue
                with self.subTest(scenario=scenario["id"], field=field):
                    value = self.changed()
                    value["scenarios"][index][field] = replacement
                    self.reject(value)

    def test_no_fallback_guest_client_mock_or_retry_waivers(self):
        for key, current in self.manifest["execution_policy"].items():
            if isinstance(current, bool):
                value = self.changed()
                value["execution_policy"][key] = not current
                self.reject(value)
        value = self.changed()
        value["execution_policy"]["test_case_retries"] = 1
        self.reject(value)
        for name in ("ssh_mounts", "scaling"):
            self.assertTrue(any(scenario["id"].endswith("." + name) and scenario["required"]
                                for scenario in self.manifest["scenarios"]))

    def test_exclusions_approval_fabrication_and_pass_claims_rejected(self):
        for value in ({"intentional_exclusions": ["ssh_mounts"]},
                      {"approval": {"identity": "owner", "approved": True}},
                      {"verdict": "PASS"}, {"coverage_complete": True}):
            changed = self.changed()
            changed.update(value)
            self.reject(changed)
        changed = self.changed()
        changed["scenarios"][0]["observed_result"] = "PASS"
        self.reject(changed)

    def test_exact_candidate_versions_and_primary_source_pin(self):
        for name in self.manifest["candidate_versions"]:
            value = self.changed()
            value["candidate_versions"][name]["minimum"] = "0.0"
            self.reject(value)
        for field, replacement in (("source_state", "pending"), ("source_sha256", "0" * 64),
                                   ("source_url", "https://example.invalid/unverified"),
                                   ("minimum_override_allowed", True), ("minimum", "1.24")):
            value = self.changed()
            value["candidate_versions"]["engine_api"][field] = replacement
            self.reject(value)

    def test_observation_hash_and_unmodified_host_client_origin(self):
        value = self.changed()
        value["host_client_observation"]["stdout"] += "fabricated client\n"
        self.reject(value)
        value = self.changed()
        value["host_client_observation"]["commands"][0] = ["vz", "exec", "docker", "--version"]
        self.reject(value)

    def test_normative_minimum_change_requires_catalog_review(self):
        with tempfile.TemporaryDirectory() as directory:
            root, value, _, _ = self.fixture_root(directory)
            goal = root / contract.GOAL
            goal.write_text(goal.read_text().replace("registry login, pull", "registry login, logout, pull"))
            self.reject(value, root)

    def test_resolved_inputs_are_not_a_compatibility_verdict(self):
        with tempfile.TemporaryDirectory() as directory:
            root, value, _, _ = self.fixture_root(directory)
            result = contract.validate_contract(value, root)
            self.assertEqual(result["kind"], "requirement_inputs_valid")
            self.assertEqual(result["docker_tests_executed"], 0)
            self.assertIs(result["compatibility_certified"], False)
            self.assertNotIn("verdict", result)

    def test_fixture_content_mode_and_inventory_are_bound(self):
        for mutation in ("content", "mode", "add", "remove"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root, value, fixtures, _ = self.fixture_root(directory)
                sample = fixtures / "unit-test-only.txt"
                if mutation == "content":
                    sample.write_text("changed")
                elif mutation == "mode":
                    sample.chmod(0o700)
                elif mutation == "add":
                    (fixtures / "extra.txt").write_text("extra")
                else:
                    sample.unlink()
                self.reject(value, root, draft=False)

    def test_harness_digest_executable_bit_and_missing_input(self):
        for mutation in ("content", "mode", "remove"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root, value, _, harness = self.fixture_root(directory)
                if mutation == "content":
                    harness.write_text("changed")
                elif mutation == "mode":
                    harness.chmod(0o600)
                else:
                    harness.unlink()
                with self.assertRaises((contract.InvalidContract, OSError)):
                    contract.validate_contract(value, root)

    def test_symlink_hardlink_and_fifo_inputs_fail_closed(self):
        for kind in ("symlink", "hardlink", "fifo", "directory_symlink"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root, value, fixtures, _ = self.fixture_root(directory)
                target = fixtures / "forbidden"
                source = fixtures / "unit-test-only.txt"
                if kind == "symlink":
                    target.symlink_to(source)
                elif kind == "hardlink":
                    os.link(source, target)
                elif kind == "fifo":
                    os.mkfifo(target)
                else:
                    target.symlink_to(fixtures, target_is_directory=True)
                self.reject(value, root, draft=False)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.mkfifo(root / "fifo.json")
            with self.assertRaises(contract.InvalidContract):
                contract.load_json(root, "fifo.json")

    def test_strict_json_and_canonical_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for encoded in (b'{"x":1,"x":2}', b'{"x":NaN}', b'\xff'):
                (root / "input.json").write_bytes(encoded)
                with self.assertRaises(contract.InvalidContract):
                    contract.load_json(root, "input.json")
            for path in ("../input.json", "/input.json", "./input.json", "a//input.json"):
                with self.assertRaises(contract.InvalidContract):
                    contract.read_regular(root, path)

    def test_cli_default_rejects_draft_and_lint_never_reports_pass(self):
        script = str(contract.ROOT / "scripts/helpers/docker_compatibility_contract.py")
        rejected = subprocess.run([sys.executable, script], capture_output=True, timeout=10)
        self.assertEqual(rejected.returncode, 1)
        self.assertIn(b"INVALID_REQUIREMENT_INPUTS", rejected.stderr)
        checked = subprocess.run([sys.executable, script, "--check-draft"], capture_output=True, timeout=10)
        self.assertEqual(checked.returncode, 0, checked.stderr)
        result = json.loads(checked.stdout)
        self.assertIs(result["compatibility_certified"], False)
        self.assertEqual(result["docker_tests_executed"], 0)


if __name__ == "__main__":
    unittest.main()
