"""Published authoring schema tests.

Run: uv run --with jsonschema==4.23.0 python scripts/helpers/test_project_definition_schema.py
Rust project_definition integration tests independently load the same example
through the production parser and semantic validator.
"""

import copy
import json
import re
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
        machine["workspace"] = {"binding": "source", "target_path": "/workspace", "mode": "read_only", "source_path": "."}
        machine["requested_capabilities"] = {"capabilities": ["posix_exec"]}
        environment["networks"] = [{"schema_version": 1, "name": "private", "kind": "private"}]
        environment["endpoints"] = [{"schema_version": 1, "name": "api", "machine": "dev", "network": "private", "protocol": "tcp", "port": 8080}]
        environment["volumes"] = [
            {"schema_version": 1, "name": "data", "kind": "block", "size_bytes": 16 * 1024 * 1024,
             "attachments": [{"machine": "dev", "target_path": "/data", "mode": "read_write"}]},
            {"schema_version": 1, "name": "cache", "kind": "shared_cache",
             "consistency": {"model": "bounded_staleness", "staleness_bound_millis": 2000},
             "attachments": [{"machine": "dev", "target_path": "/cache", "mode": "read_write"}]},
        ]
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

    def test_machine_network_membership_is_a_separate_grant_from_implicit_docker(self):
        """Which Machines may declare `networks` and `egress`, cell by cell.

        The third machine conditional used to withhold both from `macos` and
        `windows` alike, which made a native macOS Machine undeclarable on any
        Environment network. Acceptance criterion 5 requires a service path that
        crosses between a Linux Machine and a native macOS Machine in both
        directions, so a Developer macOS Machine now declares them; Hardened and
        native Windows still declare neither. Nothing tested this conditional
        before -- `test_profile_and_target_docker_rules` covers only the Docker
        one -- so every cell below is new coverage.

        The last two cells are the reason the two conditionals had to be split:
        a macOS Machine on a network still gets no implicit Docker, which the
        product contract forbids and which `native_macos_cannot_request_implicit_docker`
        asserts on the Rust side.
        """
        for target in ["linux", "macos", "windows"]:
            for profile in ["developer", "hardened"]:
                # A separate rule pins Hardened to Linux; without it every
                # "expected True" below would be about that rule instead.
                profile_target_ok = not (profile == "hardened" and target != "linux")
                on_the_fabric = profile == "developer" and target in ("linux", "macos")
                for declaration, expected in [
                    ({}, profile_target_ok),
                    ({"networks": []}, profile_target_ok),
                    ({"egress": "offline"}, profile_target_ok),
                    ({"networks": ["private"]}, profile_target_ok and on_the_fabric),
                    ({"networks": ["private", "public"]}, profile_target_ok and on_the_fabric),
                    ({"egress": "allowed"}, profile_target_ok and on_the_fabric),
                    ({"networks": ["private"], "egress": "allowed"},
                     profile_target_ok and on_the_fabric),
                    # Membership is not a Docker grant: the Docker conditional
                    # is untouched and still refuses every non-Linux target.
                    ({"networks": ["private"],
                      "requested_capabilities": {"capabilities": ["docker_engine"]}},
                     profile_target_ok and on_the_fabric and target == "linux"),
                    ({"egress": "allowed",
                      "requested_capabilities": {"capabilities": ["compose"]}},
                     profile_target_ok and on_the_fabric and target == "linux"),
                ]:
                    with self.subTest(target=target, profile=profile, declaration=declaration):
                        value = copy.deepcopy(EXAMPLE)
                        machine = value["environment"]["machines"][0]
                        machine["target"]["os"] = target
                        machine["profile"] = profile
                        machine.update(copy.deepcopy(declaration))
                        self.assertEqual(self.validator.is_valid(value), expected)

    def test_machine_field_set_matches_the_production_parser(self):
        """The authoring schema and `MachineSpec` must agree exactly.

        Same defect class as the `WorkspaceProjection` pin below: a projection
        shipped that no vz.json could declare, because the Rust type gained a
        field the schema never learned about. `networks` and `egress` are the
        two fields this change makes newly declarable on a macOS Machine, so the
        field set is read out of the Rust struct rather than restated here, and
        the next field added on either side fails this test instead of shipping.

        `deny_unknown_fields` plus `additionalProperties: false` means neither
        side may carry a field the other does not, and a field with no
        `#[serde(default)]` has no deserializable absence, so the schema's
        `required` list must name exactly the undefaulted fields.
        """
        source = (ROOT / "crates/vz-runtime-contract/src/types/topology.rs").read_text()
        declaration = source.split("pub struct MachineSpec {", 1)
        self.assertEqual(len(declaration), 2, "MachineSpec is no longer a struct here")
        self.assertIn("#[serde(deny_unknown_fields)]",
                      declaration[0][-400:],
                      "MachineSpec no longer denies unknown fields; this comparison assumes it")
        body = declaration[1].split("\n}", 1)[0]

        rust_fields, required_fields, pending = set(), set(), []
        for line in body.splitlines():
            stripped = line.strip()
            if stripped.startswith("#["):
                pending.append(stripped)
                continue
            match = re.match(r"pub (\w+):", stripped)
            if match is None:
                continue  # doc comments and blank lines
            field = match.group(1)
            rust_fields.add(field)
            if not any("serde(default" in attribute for attribute in pending):
                required_fields.add(field)
            pending = []

        self.assertTrue(rust_fields, "could not read MachineSpec fields")
        self.assertEqual(set(SCHEMA["$defs"]["machine"]["properties"]), rust_fields)
        self.assertEqual(set(SCHEMA["$defs"]["machine"]["required"]), required_fields)
        # Vacuity: the parse above is only evidence if it actually distinguishes
        # a defaulted field from an undefaulted one.
        self.assertIn("networks", rust_fields)
        self.assertIn("egress", rust_fields)
        self.assertNotIn("networks", required_fields)
        self.assertNotIn("egress", required_fields)
        self.assertIn("schema_version", required_fields)

    def test_volume_field_set_matches_the_production_parser(self):
        """The authoring schema and `VolumeSpec` must agree exactly.

        Same defect class as the `MachineSpec` and `WorkspaceProjection` pins
        above. `d416c071` added a required `source_path` to a
        `deny_unknown_fields` struct without touching the schema, and a
        projection shipped that no vz.json could declare: with the field the
        schema rejected it, without it the parser did. A `Volume` has the same
        exposure and more of it, because two of its fields are conditionally
        required, so the field set is read out of the Rust struct rather than
        restated here.
        """
        source = (ROOT / "crates/vz-runtime-contract/src/types/volume.rs").read_text()
        for struct, definition in [("VolumeSpec", "volume"), ("VolumeAttachment", "volumeAttachment"),
                                   ("SharedCacheConsistency", "sharedCacheConsistency")]:
            with self.subTest(struct=struct):
                declaration = source.split(f"pub struct {struct} {{", 1)
                self.assertEqual(len(declaration), 2, f"{struct} is no longer a struct here")
                self.assertIn("#[serde(deny_unknown_fields)]", declaration[0][-400:],
                              f"{struct} no longer denies unknown fields; this comparison assumes it")
                body = declaration[1].split("\n}", 1)[0]
                rust_fields, required_fields, pending = set(), set(), []
                for line in body.splitlines():
                    stripped = line.strip()
                    if stripped.startswith("#["):
                        pending.append(stripped)
                        continue
                    match = re.match(r"pub (\w+):", stripped)
                    if match is None:
                        continue  # doc comments and blank lines
                    field = match.group(1)
                    rust_fields.add(field)
                    if not any("serde(default" in attribute for attribute in pending):
                        required_fields.add(field)
                    pending = []
                self.assertTrue(rust_fields, f"could not read {struct} fields")
                self.assertEqual(set(SCHEMA["$defs"][definition]["properties"]), rust_fields)
                self.assertEqual(set(SCHEMA["$defs"][definition]["required"]), required_fields)
        # Vacuity: the parse is only evidence if it distinguishes a defaulted
        # field from an undefaulted one. `size_bytes` and `consistency` are the
        # two optional fields, and each is required by exactly one `kind` via
        # the schema's `allOf`, which the case below exercises.
        volume = SCHEMA["$defs"]["volume"]
        self.assertNotIn("size_bytes", volume["required"])
        self.assertNotIn("consistency", volume["required"])
        self.assertIn("attachments", volume["required"])

    def test_volume_kind_conditional_fields_and_declarable_shapes(self):
        """Each kind requires its own field and refuses the other's.

        A `size_bytes` on a shared cache would cap nothing and a `consistency`
        on a block volume would advertise a model the single-writer rule makes
        vacuous, so the schema refuses both rather than ignoring them. The
        positive cases come first: a rule that refused everything would pass
        every negative case here.
        """
        block = {"schema_version": 1, "name": "data", "kind": "block", "size_bytes": 16 * 1024 * 1024,
                 "attachments": [{"machine": "dev", "target_path": "/data", "mode": "read_write"}]}
        cache = {"schema_version": 1, "name": "cache", "kind": "shared_cache",
                 "consistency": {"model": "bounded_staleness", "staleness_bound_millis": 2000},
                 "attachments": [{"machine": "dev", "target_path": "/cache", "mode": "read_write"}]}
        for declared in ([block], [cache], [block, cache]):
            value = copy.deepcopy(EXAMPLE)
            value["environment"]["volumes"] = copy.deepcopy(declared)
            self.validator.validate(value)

        def refuse(volume, note):
            value = copy.deepcopy(EXAMPLE)
            value["environment"]["volumes"] = [volume]
            self.assertFalse(self.validator.is_valid(value), note)

        without_size = copy.deepcopy(block)
        del without_size["size_bytes"]
        refuse(without_size, "a block volume must declare size_bytes")
        with_consistency = copy.deepcopy(block)
        with_consistency["consistency"] = cache["consistency"]
        refuse(with_consistency, "a block volume must not declare consistency")
        without_consistency = copy.deepcopy(cache)
        del without_consistency["consistency"]
        refuse(without_consistency, "a shared cache must declare consistency")
        with_size = copy.deepcopy(cache)
        with_size["size_bytes"] = 16 * 1024 * 1024
        refuse(with_size, "a shared cache must not declare size_bytes")
        for size in [1024 * 1024 - 1, 1024 ** 4 + 1, "16MiB", 1.5]:
            sized = copy.deepcopy(block)
            sized["size_bytes"] = size
            refuse(sized, f"size_bytes {size!r} is outside the declarable domain")
        for millis in [0, 60001, "2s"]:
            stale = copy.deepcopy(cache)
            stale["consistency"] = {"model": "bounded_staleness", "staleness_bound_millis": millis}
            refuse(stale, f"staleness_bound_millis {millis!r} is outside the declarable domain")
        refuse({**copy.deepcopy(block), "attachments": []}, "a volume with no attachment is refused")
        for target in ["relative", "/escapes/../..", ""]:
            bad = copy.deepcopy(block)
            bad["attachments"] = [{"machine": "dev", "target_path": target, "mode": "read_write"}]
            refuse(bad, f"target_path {target!r} is not a bounded absolute Machine path")
        for mode in ["snapshot", "rw", ""]:
            bad = copy.deepcopy(block)
            bad["attachments"] = [{"machine": "dev", "target_path": "/data", "mode": mode}]
            refuse(bad, f"access mode {mode!r} is not declarable")
        refuse({**copy.deepcopy(block), "kind": "tmpfs"}, "an unknown volume kind is refused")

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


    def test_workspace_projection_field_set_matches_the_production_parser(self):
        """The authoring schema and `WorkspaceProjection` must agree exactly.

        This drifted once and made the feature unreachable: the Rust type gained
        a required `source_path` while the schema kept
        `additionalProperties: false` without it, so no vz.json declaring a
        workspace could both validate here and deserialize there. The field set
        is read out of the Rust struct rather than restated, so the next field
        added on either side fails this test instead of shipping.
        """
        source = (ROOT / "crates/vz-runtime-contract/src/types/topology.rs").read_text()
        body = source.split("pub struct WorkspaceProjection {", 1)[1].split("\n}", 1)[0]
        rust_fields = set(re.findall(r"^    pub (\w+):", body, re.MULTILINE))
        self.assertTrue(rust_fields, "could not read WorkspaceProjection fields")
        self.assertNotIn(
            "#[serde(default", body,
            "a defaulted field would weaken this comparison; teach the test about it first")
        schema_fields = set(SCHEMA["$defs"]["workspace"]["properties"])
        self.assertEqual(schema_fields, rust_fields)
        # `deny_unknown_fields` plus no serde defaults means every Rust field is
        # required, so the schema's `required` list must name all of them too.
        self.assertEqual(set(SCHEMA["$defs"]["workspace"]["required"]), rust_fields)

    def test_workspace_source_path_is_relative_and_traversal_free(self):
        """The schema half of decision 7's containment rule.

        `validate_workspace_source_path` refuses absolute paths, `..` and empty
        components before the filesystem is touched; the authoring schema must
        refuse the same declarations rather than accept a definition the runtime
        will reject later.
        """
        for source_path, expected in [
            (".", True), ("src", True), ("a/b/c", True), ("..dotfile", True), ("a/..b", True), ("a b/c", True),
            ("", False), ("/abs", False), ("..", False), ("../x", False), ("a/../b", False),
            ("a/..", False), ("a//b", False), ("a/\x00b", False), ("\x01lead", False),
            ("a/", False), ("/", False), ("x" * 1024, True), ("x" * 1025, False),
        ]:
            with self.subTest(source_path=source_path):
                value = copy.deepcopy(EXAMPLE)
                value["environment"]["machines"][0]["workspace"] = {
                    "binding": "source", "target_path": "/workspace", "mode": "read_write",
                    "source_path": source_path}
                self.assertEqual(self.validator.is_valid(value), expected)

    def test_optional_default_machine_name_shape(self):
        for default, expected in [(None, True), ("dev", True), ("", False), (" ", False), (False, False), (42, False)]:
            value = copy.deepcopy(EXAMPLE)
            value["environment"]["default_machine"] = default
            self.assertEqual(self.validator.is_valid(value), expected)
        # Cross-references are checked by the production semantic validator,
        # covered separately in the Rust definition loader tests.


if __name__ == "__main__":
    unittest.main()
