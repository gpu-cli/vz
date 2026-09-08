"""Offline orchestration checks, not evidence of physical Docker execution."""
import copy
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path
import tempfile
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import docker_host_driver as driver
import linux_docker_build_artifacts as artifacts

FIXTURE = Path(__file__).resolve().parents[2] / "tests/fixtures/vz-0.4/docker"


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.fixture_digest = driver.tree_digest(FIXTURE)
        self.inputs = {"run_id": "same-run-token", "fixture_sha256": self.fixture_digest,
                       "builder": {"name": "owned-source"},
                       "images": {"base": {"reference": "python@sha256:" + "a" * 64}}}

    def spec(self, name):
        cache = self.root / "source-alpha/cache" if name == "fresh-import-alpha" else None
        return artifacts.specification(name, self.root / name, FIXTURE, self.fixture_digest, cache)

    def fake_driver(self, name):
        selected = artifacts.ArtifactDriver.__new__(artifacts.ArtifactDriver)
        selected.output, selected.fixture = self.root / name, FIXTURE
        selected.output.mkdir(mode=0o700)
        selected.inputs = SimpleNamespace(raw=self.inputs)
        selected.record = SimpleNamespace(count=0)
        def guard():
            selected.record.count += 4
        def command(*args, **kwargs):
            selected.record.count += 1
            return SimpleNamespace(stdout=b"")
        selected.builder_guard = Mock(side_effect=guard)
        selected.command = Mock(side_effect=command)
        return selected

    def test_source_cold_import_inputs_identical_only_explicit_importer_has_cache_from(self):
        calls = [artifacts.build_arguments(self.inputs, FIXTURE, self.spec(name)) for name in
                 ("source-alpha", "fresh-cold-alpha", "fresh-import-alpha")]
        values = lambda args: [args[n + 1] for n, arg in enumerate(args) if arg == "--build-arg"]
        self.assertEqual(values(calls[0]), values(calls[1]))
        self.assertEqual(values(calls[1]), values(calls[2]))
        for args in calls[:2]:
            self.assertNotIn("--cache-from", args)
        self.assertIn("--cache-from", calls[2])
        for args in calls:
            self.assertNotIn("--no-cache", args)
            self.assertIn("--provenance=false", args)
            self.assertIn("--sbom=false", args)
            self.assertNotIn("--load", args)
            self.assertNotIn("--push", args)

    def test_secret_has_no_owner_variant_args_and_exports_max_cache(self):
        args = artifacts.build_arguments(self.inputs, FIXTURE, self.spec("source-secret"))
        self.assertIn("--secret", args)
        self.assertIn("--no-cache", args)
        self.assertTrue(any(value.startswith("FIXTURE_SECRET_SHA256=") for value in args))
        self.assertFalse(any(value.startswith(("FIXTURE_RUN=", "FIXTURE_VARIANT=")) for value in args))
        self.assertIn(",mode=max,image-manifest=true,oci-mediatypes=true,compression=gzip,force-compression=true",
                      args[args.index("--cache-to") + 1])

    def test_foreign_missing_and_extra_cache_imports_rejected(self):
        for name, cache in (("source-alpha", self.root / "source-alpha/cache"),
                            ("fresh-import-alpha", None), ("fresh-import-alpha", self.root / "foreign")):
            with self.assertRaises(ValueError):
                artifacts.specification(name, self.root / name, FIXTURE, self.fixture_digest, cache)

    def test_exporter_delimiter_paths_rejected_before_host_commands(self):
        for delimiter in (",", "\x00", "\n", "\r"):
            for target in ("output", "fixture"):
                output = self.root / ("source" + delimiter + "alpha") if target == "output" else self.root / "source-alpha"
                fixture = Path(str(FIXTURE) + delimiter + "other") if target == "fixture" else FIXTURE
                with self.subTest(delimiter=repr(delimiter), target=target), \
                        patch.object(artifacts.driver, "regular") as read_fixture:
                    with self.assertRaises(ValueError):
                        artifacts.specification("source-alpha", output, fixture, self.fixture_digest)
                    read_fixture.assert_not_called()

    def test_beta_has_distinct_exact_payload(self):
        alpha, beta = self.spec("source-alpha"), self.spec("source-beta")
        self.assertNotEqual(alpha["payload"]["sha256"], beta["payload"]["sha256"])
        self.assertEqual(alpha["payload"]["path"], beta["payload"]["path"])
        self.assertIsNone(beta["cache_output"])

    def test_one_solve_between_two_guards_scans_final_and_cache(self):
        selected, spec = self.fake_driver("source-alpha"), self.spec("source-alpha")
        with patch.object(artifacts.layout, "validate_oci", return_value={"kind": "oci"}) as oci, \
                patch.object(artifacts.layout, "validate_cache", return_value={"kind": "cache"}) as cache:
            final, proofs = selected.execute(spec)
        self.assertEqual(selected.record.count, 9)
        self.assertEqual(selected.builder_guard.call_count, 2)
        selected.command.assert_called_once_with(artifacts.build_arguments(self.inputs, FIXTURE, spec), timeout=300)
        self.assertEqual(len(oci.call_args.kwargs["canaries"]), 4)
        self.assertEqual(len(cache.call_args.kwargs["canaries"]), 2)
        self.assertEqual(final, spec)
        self.assertEqual(set(proofs), {"oci", "cache"})
        self.assertEqual(json.loads((selected.output / "operation.json").read_text()), final)
        with self.assertRaises(ValueError):
            selected.execute(spec)

    def test_dangling_export_link_rejected_before_host_commands(self):
        selected, spec = self.fake_driver("source-alpha"), self.spec("source-alpha")
        (selected.output / "oci").symlink_to(self.root / "absent")
        with self.assertRaises(ValueError):
            selected.execute(spec)
        self.assertEqual(selected.record.count, 0)

    def test_altered_operation_rejected_before_host_commands(self):
        selected, spec = self.fake_driver("source-alpha"), self.spec("source-alpha")
        spec["payload"]["sha256"] = "b" * 64
        with self.assertRaises(ValueError):
            selected.execute(spec)
        self.assertEqual(selected.record.count, 0)

    def test_imported_cache_mutation_rejected_without_replay_or_success_document(self):
        selected, spec = self.fake_driver("fresh-import-alpha"), self.spec("fresh-import-alpha")
        with patch.object(artifacts.stream, "inventory_tree", side_effect=[{"digest": "before"}, {"digest": "after"}]), \
                patch.object(artifacts.layout, "validate_cache"), patch.object(artifacts.layout, "validate_oci") as oci:
            with self.assertRaises(ValueError):
                selected.execute(spec)
        oci.assert_not_called()
        self.assertTrue((selected.output / "operation.intent.json").exists())
        self.assertFalse((selected.output / "operation.json").exists())

    def test_shared_role_volume_and_foreign_owner_rejected(self):
        descriptor = {"owner": "machine-a"}
        builders = {role: SimpleNamespace(role=role, descriptor=copy.deepcopy(descriptor), prepared=True,
                    name=role, node=role, container_id=role, volume_name=role) for role in artifacts.ROLES}
        artifacts.distinct_roles(builders, descriptor)
        builders["importer"].volume_name = "cold-control"
        with self.assertRaises(ValueError):
            artifacts.distinct_roles(builders, descriptor)
        builders["importer"].volume_name = "importer"
        builders["importer"].descriptor = {"owner": "foreign"}
        with self.assertRaises(ValueError):
            artifacts.distinct_roles(builders, descriptor)

    @contextmanager
    def machine_fixture(self, *, fail_execute=None, fail_replay=None,
                        fail_worker=None, fail_monitor=None, drift_final_cache=False,
                        mutate_operation=False, drift_import_cache=False, fail_cached_worker=False):
        """Only orchestrator control flow is exercised; all Docker boundaries mock."""
        descriptor = {"owner": "exact-machine-id"}
        events, operation_proofs, builders = [], {}, {}
        harness = SimpleNamespace(evidence=self.root, info={"fixture": str(FIXTURE),
            "fixture_sha256": self.fixture_digest}, drivers=[object()], driver_cleanup_verified=[True])

        def prepare(selected, *, role, keep_probe):
            self.assertEqual(selected, descriptor)
            events.append(("prepare", role, keep_probe))
            builder = SimpleNamespace(role=role, descriptor=copy.deepcopy(descriptor), prepared=True,
                name="builder-" + role, node="node-" + role, container_id="cid-" + role,
                volume_name="volume-" + role, mapping={"name": "builder-" + role},
                ownership={"machine_id": "exact-machine-id", "role": role})
            builder.verify = Mock(side_effect=lambda **kwargs: {
                "require_invocation": kwargs["require_invocation"], "role": role})
            builders[role] = builder
            return builder

        harness.prepare_builder = Mock(side_effect=prepare)
        checks = 0

        def monitor():
            nonlocal checks
            checks += 1
            events.append(("monitor", checks))
            self.assertEqual(harness.driver_cleanup_verified, [True] + [False] * len(operation_proofs))
            if checks == fail_monitor:
                raise ValueError("mock monitor failure")

        harness.monitor = SimpleNamespace(check=Mock(side_effect=monitor))

        def admitted(raw, suite):
            self.assertEqual(suite, "build")
            return SimpleNamespace(raw=raw, verify_runtime_evidence=Mock())

        def selected_driver(inputs, fixture, output):
            self.assertEqual(fixture, FIXTURE)
            output.mkdir(mode=0o700)

            def execute(operation):
                name = operation["operation"]
                events.append(("execute", name))
                self.assertEqual(harness.driver_cleanup_verified, [True] + [False] * len(harness.drivers[1:]))
                if name == fail_execute:
                    raise ValueError("mock solve failure")
                variant = "beta" if name == "source-beta" else "secret" if name == "source-secret" else "alpha"
                proof = {"oci": {"layer": {"descriptor": {"digest": "sha256:" + driver.sha256(variant.encode())}},
                                 "payload": {"sha256": operation["payload"]["sha256"]}}}
                if operation["cache_output"] is not None:
                    proof["cache"] = {"inventory_sha256": driver.sha256((name + "-cache").encode())}
                operation_proofs[name] = copy.deepcopy(proof)
                (output / "operation.json").write_text(json.dumps(operation))
                if mutate_operation and name == "fresh-import-alpha":
                    original_path = output.parent / "source-alpha/operation.json"
                    changed = json.loads(original_path.read_text())
                    changed["payload"]["size"] += 1
                    original_path.write_text(json.dumps(changed))
                return copy.deepcopy(operation), proof

            return SimpleNamespace(output=output, inputs=inputs, execute=Mock(side_effect=execute))

        replay_calls = []

        def replay(directory, inputs, expected):
            name = directory.name
            replay_calls.append((name, copy.deepcopy(expected), copy.deepcopy(inputs)))
            events.append(("replay", name))
            self.assertEqual(harness.driver_cleanup_verified, [True] + [False] * len(harness.drivers[1:]))
            if len(replay_calls) == fail_replay:
                raise ValueError("mock replay failure")
            # The real consumer authenticates the persisted operation against
            # caller expectations; this catches re-reading mutable expectations.
            driver.require(json.loads((directory / "operation.json").read_text()) == expected,
                           "mock original operation differs")
            proof = copy.deepcopy(operation_proofs[name])
            if drift_final_cache and len(replay_calls) == 6:
                proof["cache"]["inventory_sha256"] = driver.sha256(b"different-valid-resealed-cache")
            imported_cache = copy.deepcopy(operation_proofs["source-alpha"]["cache"]) if name == "fresh-import-alpha" else None
            if imported_cache is not None and drift_import_cache:
                imported_cache["inventory_sha256"] = driver.sha256(b"valid-but-not-original-source-cache")
            return {"operation": name, "oci": proof["oci"], "cache": proof.get("cache"),
                    "imported_cache": imported_cache,
                    "outcome": "mock-independent-validation"}

        worker_calls = 0

        def worker(builder):
            nonlocal worker_calls
            worker_calls += 1
            events.append(("worker", builder.role))
            if worker_calls == fail_worker:
                raise ValueError("mock worker log failure")
            return {"present": True, "no_runtime_errors": True}

        def cached_worker(builder):
            self.assertEqual(builder.role, "importer")
            events.append(("cached-worker", builder.role))
            if fail_cached_worker:
                raise ValueError("mock cached worker log failure")
            return {"no_worker_execution": True, "invocations": 0, "log_present": False}

        modules = {}
        for name in ("linux_docker_e2e", "linux_docker_artifact_evidence", "linux_docker_buildkit_keep"):
            modules[name] = ModuleType(name)
        modules["linux_docker_e2e"].input_mapping = Mock(side_effect=lambda *args: copy.deepcopy(self.inputs))
        modules["linux_docker_artifact_evidence"].validate = Mock(side_effect=replay)
        modules["linux_docker_buildkit_keep"].verify_worker_log = Mock(side_effect=worker)
        modules["linux_docker_buildkit_keep"].verify_cached_worker_log = Mock(side_effect=cached_worker)
        with ExitStack() as stack:
            stack.enter_context(patch.dict(sys.modules, modules))
            stack.enter_context(patch.object(artifacts.driver, "Inputs", side_effect=admitted))
            stack.enter_context(patch.object(artifacts, "ArtifactDriver", side_effect=selected_driver))
            yield SimpleNamespace(harness=harness, descriptor=descriptor, events=events,
                replay_calls=replay_calls, builders=builders,
                invoke=lambda: artifacts.run_machine(harness, descriptor, {"machine_id": "exact-machine-id"}, {}, {}, 0))

    def test_run_machine_all_five_then_final_replays_before_cleanup_admission(self):
        with self.machine_fixture() as case:
            result = case.invoke()
            self.assertEqual([row["operation"] for row in result["operations"]], list(artifacts.OPERATIONS))
            self.assertEqual([row[0] for row in case.replay_calls], list(artifacts.OPERATIONS) * 2)
            self.assertEqual(case.harness.driver_cleanup_verified, [True] * 6)
            self.assertEqual([event for event in case.events if event[0] == "prepare"],
                             [("prepare", "source", True), ("prepare", "cold-control", False), ("prepare", "importer", False)])
            self.assertLess(case.events.index(("execute", "fresh-cold-alpha")),
                            case.events.index(("prepare", "importer", False)))
            case.builders["importer"].verify.assert_called_once_with(require_invocation=False)
            case.builders["cold-control"].verify.assert_called_once_with(require_invocation=True)
            self.assertEqual(case.builders["source"].verify.call_count, 3)
            self.assertEqual([event for event in case.events if event[0] in ("worker", "cached-worker")],
                             [("worker", "source"), ("worker", "cold-control"), ("cached-worker", "importer")])
            self.assertNotIn("post_workload_log", result["operations"][0]["runtime"])
            self.assertNotIn("post_workload_log", result["operations"][1]["runtime"])
            self.assertTrue(result["operations"][2]["runtime"]["post_workload_log"]["present"])
            self.assertTrue(result["operations"][3]["runtime"]["post_workload_log"]["present"])
            self.assertTrue(result["operations"][4]["runtime"]["no_worker_execution"]["no_worker_execution"])
            self.assertNotIn("post_workload_log", result["operations"][4]["runtime"])
            self.assertEqual(result["test_case_retries"], 0)
            self.assertFalse(result["docker_parity_certified"])
            self.assertTrue((self.root / "artifacts-machine-0/machine-artifact-validation.json").is_file())
            for initial, final in zip(case.replay_calls[:5], case.replay_calls[5:]):
                self.assertEqual(initial, final)

    def test_run_machine_solve_failure_keeps_all_attempted_cleanup_flags_false(self):
        with self.machine_fixture(fail_execute="source-beta") as case:
            with self.assertRaisesRegex(ValueError, "mock solve failure"):
                case.invoke()
            self.assertEqual(case.harness.driver_cleanup_verified, [True, False, False])
            self.assertEqual(set(case.builders), {"source"})
            self.assertEqual(len(case.replay_calls), 1)
            self.assertFalse((self.root / "artifacts-machine-0/machine-artifact-validation.json").exists())

    def test_run_machine_final_replay_failure_never_admits_partial_cleanup(self):
        with self.machine_fixture(fail_replay=10) as case:
            with self.assertRaisesRegex(ValueError, "mock replay failure"):
                case.invoke()
            self.assertEqual(case.harness.driver_cleanup_verified, [True] + [False] * 5)
            self.assertFalse((self.root / "artifacts-machine-0/machine-artifact-validation.json").exists())

    def test_run_machine_resealed_export_cache_drift_rejected_at_final_boundary(self):
        with self.machine_fixture(drift_final_cache=True) as case:
            with self.assertRaises(ValueError):
                case.invoke()
            self.assertEqual(case.harness.driver_cleanup_verified, [True] + [False] * 5)
            self.assertFalse((self.root / "artifacts-machine-0/machine-artifact-validation.json").exists())

    def test_run_machine_original_operation_contract_not_reread_as_authority(self):
        with self.machine_fixture(mutate_operation=True) as case:
            with self.assertRaisesRegex(ValueError, "mock original operation differs"):
                case.invoke()
            self.assertEqual(case.harness.driver_cleanup_verified, [True] + [False] * 5)

    def test_run_machine_import_must_match_original_producer_cache_proof(self):
        with self.machine_fixture(drift_import_cache=True) as case:
            with self.assertRaisesRegex(ValueError, "original source cache proof"):
                case.invoke()
            self.assertEqual(len(case.replay_calls), 5)
            self.assertEqual(case.harness.driver_cleanup_verified, [True] + [False] * 5)
            self.assertFalse((self.root / "artifacts-machine-0/machine-artifact-validation.json").exists())

    def test_run_machine_worker_log_failure_keeps_cleanup_withheld(self):
        with self.machine_fixture(fail_worker=1) as case:
            with self.assertRaisesRegex(ValueError, "mock worker log failure"):
                case.invoke()
            self.assertEqual(case.harness.driver_cleanup_verified, [True] + [False] * 3)

    def test_run_machine_cached_worker_proof_failure_withholds_all_cleanup(self):
        with self.machine_fixture(fail_cached_worker=True) as case:
            with self.assertRaisesRegex(ValueError, "mock cached worker log failure"):
                case.invoke()
            self.assertEqual(case.harness.driver_cleanup_verified, [True] + [False] * 5)
            self.assertEqual(len(case.replay_calls), 5)
            self.assertFalse((self.root / "artifacts-machine-0/machine-artifact-validation.json").exists())

    def test_run_machine_monitor_failure_keeps_cleanup_withheld(self):
        with self.machine_fixture(fail_monitor=5) as case:
            with self.assertRaisesRegex(ValueError, "mock monitor failure"):
                case.invoke()
            self.assertEqual(case.harness.driver_cleanup_verified, [True] + [False] * 5)


if __name__ == "__main__":
    unittest.main()
