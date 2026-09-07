"""Run using the pinned jsonschema dependency; no Docker execution."""

import copy
import json
from pathlib import Path
import unittest

from jsonschema import Draft202012Validator, ValidationError

from docker_host_driver import BUILD_RECIPES, COMPOSE_RECIPES, Rejected, validate_result


class ResultSchemaTests(unittest.TestCase):
    def setUp(self):
        self.schema = json.loads(Path(__file__).with_name("docker_host_results.schema.json").read_text())
        Draft202012Validator.check_schema(self.schema)
        self.validator = Draft202012Validator(self.schema)
        self.result = {
            "schema_version": 1, "kind": "docker_host_fixture_subset", "suite": "build", "run_id": "fixture-run-12345678",
            "scope": {"project_id": "p", "environment_id": "e", "machine_id": "m",
                      "machine_incarnation": "i", "runtime_identity": "r", "docker_context": "owned",
                      "docker_endpoint": "unix:///private/owned/machine.sock", "engine_id": "owned-engine"},
            "release_sha256": "a" * 64, "fixture_sha256": "b" * 64,
            "compatibility_certified": False, "release_scenarios_passed": [], "test_case_retries": 0,
            "outcome": "fixture_assertions_passed", "failure": None, "cleanup_errors": [],
            "observations": [{"recipe": recipe, "related_scenario_ids": ["docker.build.multi_stage"],
                              "first_command": index * 2 + 1, "last_command": index * 2 + 2,
                              "outcome": "fixture_assertions_passed", "assertions": ["synthetic schema test only"]}
                             for index, recipe in enumerate(BUILD_RECIPES)],
            "command_count": len(BUILD_RECIPES) * 2, "owned_projects": {}, "remaining": ["physical verification"]}

    def test_fixture_only_success_shape(self):
        self.validator.validate(self.result)
        validate_result(self.result)

    def test_failed_before_first_command_shape(self):
        self.result.update(outcome="failed", failure="context rejected", observations=[], command_count=0)
        self.validator.validate(self.result)
        validate_result(self.result)

    def test_release_certification_retry_unknown_field_missing_owner_rejected(self):
        for field, value in (("compatibility_certified", True), ("release_scenarios_passed", ["docker.compose.up"]),
                             ("test_case_retries", 1), ("unknown", True), ("scope", {})):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                self.validator.validate(self.result | {field: value})

    def test_success_cannot_hide_cleanup_or_recipe_failure(self):
        bad_recipe = copy.deepcopy(self.result["observations"])
        bad_recipe[0]["outcome"] = "failed"
        for changed in ({"cleanup_errors": ["leak"]}, {"failure": "failure"},
                        {"observations": []}, {"observations": bad_recipe}):
            with self.subTest(changed=changed), self.assertRaises(ValidationError):
                self.validator.validate(self.result | changed)

    def test_semantic_bounds_reject_zero_count_overlap_and_unselected_recipes(self):
        overlapping = copy.deepcopy(self.result["observations"])
        overlapping[1]["first_command"] = overlapping[0]["last_command"]
        for changed in ({"command_count": 0}, {"observations": overlapping}, {"suite": "compose"},
                        {"observations": self.result["observations"][:-1]}):
            with self.subTest(changed=changed), self.assertRaises(Rejected):
                validate_result(self.result | changed)

    def test_compose_suite_requires_exactly_nine_recipes_and_no_all_alias(self):
        observations = [{"recipe": recipe, "related_scenario_ids": ["docker.compose.logs" if recipe == "compose-logs" else "docker.compose.up"],
                         "first_command": index * 2 + 1, "last_command": index * 2 + 2,
                         "outcome": "fixture_assertions_passed", "assertions": ["synthetic schema test only"]}
                        for index, recipe in enumerate(COMPOSE_RECIPES)]
        result = self.result | {"suite": "compose", "observations": observations, "command_count": len(observations) * 2}
        self.assertEqual(len(observations), 9)
        self.validator.validate(result)
        validate_result(result)
        with self.assertRaises(ValidationError):
            self.validator.validate(result | {"observations": observations[:-1], "command_count": 16})
        with self.assertRaises(ValidationError):
            self.validator.validate(result | {"suite": "all"})
        union = self.result["observations"] + [x | {"first_command": x["first_command"] + 10, "last_command": x["last_command"] + 10}
                                              for x in observations]
        combined = self.result | {"suite": "build_compose", "observations": union, "command_count": 28}
        self.validator.validate(combined)
        validate_result(combined)
        with self.assertRaises(ValidationError):
            self.validator.validate(combined | {"observations": union[:-1]})

    def test_semantic_rejects_invalid_range_types_and_order(self):
        for first, last in ((True, 2), (1, False), (0, 2), (2, 1), (1, 100)):
            observations = copy.deepcopy(self.result["observations"])
            observations[0].update(first_command=first, last_command=last)
            with self.subTest(first=first, last=last), self.assertRaises(Rejected):
                validate_result(self.result | {"observations": observations})


if __name__ == "__main__":
    unittest.main()
