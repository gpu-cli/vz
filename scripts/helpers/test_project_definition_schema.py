"""Published authoring schema tests.

Run: uv run --with jsonschema==4.23.0 python scripts/helpers/test_project_definition_schema.py
Rust project_definition integration tests independently load the same example
through the production parser and semantic validator.
"""

import copy
import json
from pathlib import Path
import unittest

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[2]
SCHEMA = json.loads((ROOT / "schemas/vz-project-definition-v1.schema.json").read_text())
EXAMPLE = json.loads((ROOT / "examples/developer-environment/vz.json").read_text())


class ProjectDefinitionSchemaTests(unittest.TestCase):
    def setUp(self):
        self.validator = Draft202012Validator(SCHEMA)

    def test_schema_and_example(self):
        Draft202012Validator.check_schema(SCHEMA)
        self.validator.validate(EXAMPLE)

    def test_required_fields_and_versions(self):
        for path in [(), ("environment",), ("environment", "machines", 0)]:
            value = EXAMPLE
            for key in path:
                value = value[key]
            for field in value:
                if field == "resources":
                    continue
                with self.subTest(path=path, field=field):
                    changed = copy.deepcopy(EXAMPLE)
                    target = changed
                    for key in path:
                        target = target[key]
                    del target[field]
                    self.assertFalse(self.validator.is_valid(changed))
            changed = copy.deepcopy(EXAMPLE)
            target = changed
            for key in path:
                target = target[key]
            target["schema_version"] = 2
            self.assertFalse(self.validator.is_valid(changed))

    def test_unknown_fields_at_every_object_boundary(self):
        value = copy.deepcopy(EXAMPLE)
        environment = value["environment"]
        machine = environment["machines"][0]
        machine["workspace"] = {"binding": "source", "target_path": "/workspace", "mode": "read_only"}
        machine["requested_capabilities"] = {"capabilities": ["posix_exec"]}
        environment["networks"] = [{"schema_version": 1, "name": "private", "kind": "private"}]
        environment["endpoints"] = [{"schema_version": 1, "name": "api", "machine": "dev", "network": "private", "protocol": "tcp", "port": 8080}]
        self.validator.validate(value)

        def objects(item):
            if isinstance(item, dict):
                yield item
                for child in list(item.values()):
                    yield from objects(child)
            elif isinstance(item, list):
                for child in item:
                    yield from objects(child)

        for item in objects(value):
            item["typo_must_not_be_ignored"] = True
            self.assertFalse(self.validator.is_valid(value))
            del item["typo_must_not_be_ignored"]

    def test_profile_and_target_docker_rules(self):
        for target in ["linux", "macos", "windows"]:
            for profile in ["developer", "hardened"]:
                for capability in [None, "docker_engine", "compose", "buildx"]:
                    with self.subTest(target=target, profile=profile, capability=capability):
                        value = copy.deepcopy(EXAMPLE)
                        machine = value["environment"]["machines"][0]
                        machine["target"]["os"] = target
                        machine["profile"] = profile
                        machine["requested_capabilities"] = {"capabilities": [] if capability is None else [capability]}
                        expected = not (profile == "hardened" and target != "linux")
                        if capability is not None:
                            expected = expected and target == "linux" and profile == "developer"
                        self.assertEqual(self.validator.is_valid(value), expected)

    def test_invalid_identity_capabilities_and_wire_ranges(self):
        for project_id in ["", "../../project", "a" * 129, "prj_é", "prj_x\n", "prj_x\r", "prj_x\t", "prj_\x00x"]:
            value = copy.deepcopy(EXAMPLE)
            value["project_id"] = project_id
            self.assertFalse(self.validator.is_valid(value))
        for field, invalids in [("cpus", [-1, 256, 1.5, True, "2"]), ("memory_mb", [-1, 2**64]), ("disk_bytes", [-1, 2**64])]:
            for invalid in invalids:
                value = copy.deepcopy(EXAMPLE)
                value["environment"]["machines"][0]["resources"][field] = invalid
                self.assertFalse(self.validator.is_valid(value), (field, invalid))
        for requested in [{"capabilities": ["unknown"]}, {"unsupported": {"compose": "disabled"}}]:
            value = copy.deepcopy(EXAMPLE)
            value["environment"]["machines"][0]["requested_capabilities"] = requested
            self.assertFalse(self.validator.is_valid(value))

    def test_empty_topology_and_invalid_endpoint_ports(self):
        value = copy.deepcopy(EXAMPLE)
        value["environment"]["machines"] = []
        self.assertFalse(self.validator.is_valid(value))
        for port in [0, -1, 65536, True, "8080"]:
            value = copy.deepcopy(EXAMPLE)
            value["environment"]["endpoints"] = [{"schema_version": 1, "name": "api", "machine": "dev", "network": "private", "protocol": "tcp", "port": port}]
            self.assertFalse(self.validator.is_valid(value))


    def test_optional_default_machine_name_shape(self):
        for default, expected in [(None, True), ("dev", True), ("", False), (" ", False), (False, False), (42, False)]:
            value = copy.deepcopy(EXAMPLE)
            value["environment"]["default_machine"] = default
            self.assertEqual(self.validator.is_valid(value), expected)
        # Cross-references are checked by the production semantic validator,
        # covered separately in the Rust definition loader tests.


if __name__ == "__main__":
    unittest.main()
