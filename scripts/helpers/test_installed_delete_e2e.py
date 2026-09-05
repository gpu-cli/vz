"""Offline adversarial Delete-driver tests. No Docker, daemon, VM or build."""
import copy
import contextlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import installed_delete_e2e as driver


def fixture():
    environment = {"project_id": "prj_delete_test", "environment_id": "env_delete_test", "name": "primary",
                   "definition_digest": "sha256:" + "a" * 64, "lifecycle_generation": 1, "state": "ready", "machines": []}
    bindings, steps, cleanups = [], [], []
    for index in range(2):
        machine = {"machine_id": "mch_test_" + str(index), "state": "ready"}
        owner = dict(project_id=environment["project_id"], environment_id=environment["environment_id"], machine_id=machine["machine_id"])
        incarnation = {"schema_version": 1, "machine_id": machine["machine_id"], "incarnation_id": "inc_test_" + str(index),
                       "generation": 1, "created_at": 7}
        records = [driver.ownership(owner, "machine", machine["machine_id"]),
                   driver.ownership(owner, {"other": "machine_runtime_store"}, driver.resource_name(owner, "other:machine_runtime_store", "runtime"))]
        environment["machines"].append(machine)
        bindings.append({"machine": machine, "owner": owner, "incarnation": incarnation, "ownership": records})
        steps.append({"machine_id": machine["machine_id"], "initial_state": "ready", "expected_incarnation": incarnation,
                      "status": "succeeded"})
        cleanups.extend({"ownership": x, "status": "succeeded"} for x in records)
    request = "delete-fixture-request"
    operation = {"schema_version": 1, "operation_id": "lop_fixture_delete", "kind": "delete",
        "project_id": environment["project_id"], "environment_id": environment["environment_id"], "request_id": request,
        "idempotency_key": request, "definition_digest": environment["definition_digest"], "generation": 2,
        "initial_state": "ready", "requested_target": "deleted", "status": "succeeded", "completed_at": 20,
        "request_hash": driver.request_hash(environment["project_id"], environment["environment_id"], "primary"),
        "machine_steps": steps, "cleanup_steps": cleanups}
    tombstone = {"schema_version": 1, "project_id": environment["project_id"], "environment_id": environment["environment_id"],
        "definition_digest": environment["definition_digest"], "name": "primary", "delete_operation_id": operation["operation_id"],
        "lifecycle_generation": 2, "deleted_at": 20, "ownership_digest": driver.ownership_digest([x["ownership"] for x in cleanups])}
    rows = [{"schema_version": 1, "record_type": "request_started", "operation": "delete_environment", "request_id": request,
             "idempotency_key": request},
            {"schema_version": 1, "record_type": "operation_progress", "request_id": request, "idempotency_key": request,
             "sequence": 1, "operation": operation, "terminal": True, "error": None, "tombstone": tombstone}]
    return environment, bindings, request, rows


def encode(rows):
    return b"\n".join(json.dumps(row).encode() for row in rows) + b"\n"


class DeleteDriverTests(unittest.TestCase):
    def test_receipt_inventory_stops_before_consuming_or_processing_unbounded_source(self):
        consumed = []
        class Entry:
            def __init__(self, index):
                self.path = "/owned/file-" + str(index)
            def is_symlink(self):
                return False
            def is_dir(self, *, follow_symlinks):
                self_test.assertFalse(follow_symlinks)
                return False
        self_test = self
        @contextlib.contextmanager
        def entries(_):
            def generate():
                for index in range(1000000):
                    consumed.append(index)
                    yield Entry(index)
            yield generate()
        with patch.object(driver.os, "scandir", side_effect=entries):
            paths = driver.bounded_receipt_paths(Path("/owned"), limit=3)
            self.assertEqual([next(paths).name for _ in range(3)], ["file-0", "file-1", "file-2"])
            with self.assertRaises(ValueError):
                next(paths)
        self.assertEqual(consumed, [0, 1, 2, 3])

    def test_receipt_inventory_never_descends_into_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "redirect").symlink_to(root, target_is_directory=True)
            with self.assertRaises(ValueError):
                list(driver.bounded_receipt_paths(root))

    def test_youki_inventory_rejects_missing_duplicate_foreign_or_unbounded_absence_proof(self):
        _, bindings, _, _ = fixture()
        owner, incarnation = bindings[0]["owner"], bindings[0]["incarnation"]
        digest = "a" * 64
        value = {"owner": owner, "incarnation": incarnation, "youki_sha256": digest,
            "scope": "startup_executable_paths_and_pinned_daemon_mounts_not_release_cache_audit",
            "stdout": "vz-startup-runtime-inventory-v1\nyouki-sha256=" + digest +
                      "\nyouki 0.7.0\nalternate-runtime-binaries=absent\n"}
        driver.runtime_inventory(value, owner, incarnation, digest)
        mutations = [
            lambda x: x.update(scope="full_release_cache_audit"),
            lambda x: x.update(youki_sha256="b" * 64),
            lambda x: x["owner"].update(machine_id="mch_foreign"),
            lambda x: x["incarnation"].update(generation=2),
            lambda x: x.update(stdout=x["stdout"].replace(digest, "b" * 64)),
            lambda x: x.update(stdout=x["stdout"].replace("alternate-runtime-binaries=absent\n", "")),
            lambda x: x.update(stdout=x["stdout"] + "alternate-runtime-binaries=absent\n"),
            lambda x: x.update(stdout=x["stdout"].replace("youki 0.7.0\n", "alternate-runtime-binaries=present\n")),
            lambda x: x.update(stdout=x["stdout"] + "unexpected trailing line\n"),
            lambda x: x.update(stdout=x["stdout"].replace("youki 0.7.0", "é" * 4096)),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                changed = copy.deepcopy(value)
                mutate(changed)
                with self.assertRaises(ValueError):
                    driver.runtime_inventory(changed, owner, incarnation, digest)

    def test_exact_terminal_and_original_name_replay(self):
        environment, bindings, request, rows = fixture()
        result = driver.delete_terminal(encode(rows), environment, bindings, request, "primary")
        self.assertEqual(result["operation"], rows[-1]["operation"])
        self.assertEqual(result["tombstone"], rows[-1]["tombstone"])
        # A reused name does not authorize interpreting an old receipt as the
        # replacement's deletion. Caller must keep original immutable scope.
        replacement = dict(environment, environment_id="env_replacement")
        with self.assertRaises(ValueError):
            driver.delete_terminal(encode(rows), replacement, bindings, request, "primary")

    def test_foreign_scope_hash_generation_incarnation_and_tombstone_rejected(self):
        environment, bindings, request, rows = fixture()
        mutations = [
            lambda x: x[-1]["operation"].update(project_id="prj_foreign"),
            lambda x: x[-1]["operation"].update(environment_id="env_foreign"),
            lambda x: x[-1]["operation"].update(request_id="different"),
            lambda x: x[-1]["operation"].update(request_hash="sha256:" + "f" * 64),
            lambda x: x[-1]["operation"].update(definition_digest="sha256:" + "b" * 64),
            lambda x: x[-1]["operation"].update(generation=3),
            lambda x: x[-1]["operation"].update(initial_state="stopped"),
            lambda x: x[-1]["operation"]["machine_steps"][0]["expected_incarnation"].update(created_at=99),
            lambda x: x[-1]["operation"]["machine_steps"][0].update(target_state="stopped"),
            lambda x: x[-1]["operation"]["machine_steps"][0].update(status="pending"),
            lambda x: x[-1]["operation"]["cleanup_steps"][0].update(status="failed"),
            lambda x: x[-1]["tombstone"].update(ownership_digest="sha256:" + "f" * 64),
            lambda x: x[-1]["tombstone"].update(delete_operation_id="lop_foreign"),
            lambda x: x[-1]["tombstone"].update(deleted_at=21),
            lambda x: x[-1].update(error={"code": "backend_unavailable"}),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                changed = copy.deepcopy(rows)
                mutate(changed)
                with self.assertRaises(ValueError):
                    driver.delete_terminal(encode(changed), environment, bindings, request, "primary")

    def test_unknown_duplicate_missing_or_drifting_stream_records_rejected(self):
        environment, bindings, request, rows = fixture()
        variants = [rows[:1], rows[1:], rows + [rows[-1]], rows + [{"record_type": "unknown"}]]
        first = copy.deepcopy(rows[-1])
        first.update(sequence=0, terminal=False, tombstone=None)
        first["operation"]["status"] = "running"
        drift = copy.deepcopy(rows)
        drift.insert(1, first)
        drift[-1]["operation"]["operation_id"] = "lop_other"
        drift[-1]["tombstone"]["delete_operation_id"] = "lop_other"
        variants.append(drift)
        for index, value in enumerate(variants):
            with self.subTest(index=index), self.assertRaises(ValueError):
                driver.delete_terminal(encode(value), environment, bindings, request, "primary")

    def test_cleanup_inventory_cannot_omit_duplicate_or_redirect_resources(self):
        environment, bindings, request, rows = fixture()
        for mode in ("omit", "duplicate", "foreign"):
            changed = copy.deepcopy(rows)
            steps = changed[-1]["operation"]["cleanup_steps"]
            if mode == "omit":
                steps.pop()
            elif mode == "duplicate":
                steps[-1] = steps[0]
            else:
                steps[0]["ownership"]["machine_id"] = "mch_other"
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                driver.delete_terminal(encode(changed), environment, bindings, request, "primary")

    def test_resource_names_and_cleanup_hash_are_order_independent_and_owner_bound(self):
        environment, bindings, _, _ = fixture()
        owner = bindings[0]["owner"]
        name = driver.resource_name(owner, "other:machine_runtime_store", "runtime")
        self.assertEqual(len(name), 64)
        self.assertTrue(name.startswith("vzr1-other-machine_runtime_stor-"))
        self.assertNotEqual(name, driver.resource_name(dict(owner, environment_id="env_other"), "other:machine_runtime_store", "runtime"))
        records = [x for binding in bindings for x in binding["ownership"]]
        self.assertEqual(driver.ownership_digest(records), driver.ownership_digest(list(reversed(records))))
        self.assertNotEqual(driver.request_hash(environment["project_id"], environment["environment_id"], "primary"),
                            driver.request_hash(environment["project_id"], environment["environment_id"], environment["environment_id"]))

    def test_unresolved_delete_withholds_all_automated_lifecycle_cleanup(self):
        harness = object.__new__(driver.DeleteHarness)
        harness.monitor = None
        harness.unresolved_deletes = {"delete-unknown"}
        with patch.object(driver.startup.Harness, "cleanup", side_effect=AssertionError("unsafe cleanup")):
            with self.assertRaises(ValueError):
                harness.cleanup()

    def test_changed_host_project_file_is_not_hidden_by_unchanged_docker_defaults(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "config.json").write_bytes(b"default")
            sentinel = directory / "user-file"
            sentinel.write_bytes(b"original")
            harness = object.__new__(driver.DeleteHarness)
            harness.config, harness.default_digest = directory, driver.startup.digest(directory / "config.json")
            harness.baseline_paths, harness.baseline = set(), {}
            harness.host_files = {str(sentinel): driver.startup.digest(sentinel)}
            harness.check_defaults()
            sentinel.write_bytes(b"changed")
            with self.assertRaises(ValueError):
                harness.check_defaults()

    def test_failed_final_postcondition_still_writes_result_and_checksums(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            class FakeHarness:
                def __init__(self, info):
                    self.evidence = directory
                    self.root = directory
                    self.staged_inputs = {}
                    self.unresolved_deletes = set()
                    self.unresolved_up = set()
                def stage(self):
                    pass
                def scenario(self):
                    return {"fixture": True}
                def cleanup(self):
                    return {"positive_delete_count": 2, "daemon_graceful_shutdown_observed": True}
            with patch.object(driver, "DeleteHarness", FakeHarness), patch.object(driver.startup, "collect_runtime_receipts"), patch("builtins.print"):
                self.assertEqual(driver.run({"inputs": {}}), 1)
            result = json.loads((directory / "result.json").read_bytes())
            self.assertEqual(result["outcome"], "failed")
            self.assertTrue(result["cleanup_errors"])
            self.assertTrue((directory / "checksums.sha256").is_file())


if __name__ == "__main__":
    unittest.main()
