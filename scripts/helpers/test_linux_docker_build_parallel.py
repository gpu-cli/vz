"""Offline orchestration checks, never physical concurrency certification."""
from contextlib import contextmanager
import copy
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import docker_host_driver as driver
import linux_docker_build_parallel as parallel

BASE = Path(__file__).resolve().parents[2] / "tests/fixtures/vz-0.4/docker"


class ParallelTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.fixture = self.root / "fixture"
        self.fixture.mkdir(mode=0o700)
        (self.fixture / "input").write_bytes(b"exact parallel fixture")
        self.digest = driver.tree_digest(self.fixture)
        self.inputs = {"run_id": "parallel-test-run", "fixture_sha256": driver.tree_digest(BASE),
                       "builder": {"name": "exact-builder"},
                       "images": {"base": {"reference": "python@sha256:" + "a" * 64}}}

    def operation(self, slot=0):
        return parallel.specification(slot, self.root / ("slot-" + str(slot)), self.fixture,
                                      self.digest, self.inputs["run_id"])

    def test_four_slots_have_distinct_arguments_outputs_and_payloads(self):
        operations = [self.operation(slot) for slot in range(4)]
        self.assertEqual(len({op["payload"]["sha256"] for op in operations}), 4)
        self.assertEqual(len({op["output"] for op in operations}), 4)
        for slot, op in enumerate(operations):
            args = parallel.build_arguments(self.inputs, op)
            self.assertIn("FIXTURE_SLOT=" + str(slot), args)
            self.assertIn("FIXTURE_RUN=parallel-test-run", args)
            # IgnoreCache prunes even active shared cache-mount references in
            # pinned BuildKit. Cold RUN proof comes from fresh ownership and
            # distinct slots, not from destructively resetting the barrier.
            self.assertNotIn("--no-cache", args)
            self.assertIn("--network=none", args)
            for forbidden in ("--cache-from", "--cache-to", "--load", "--push", "--secret", "--ssh"):
                self.assertNotIn(forbidden, args)
            self.assertTrue(args[args.index("--output") + 1].endswith(parallel.OCI_OPTIONS))

    def test_contract_rejects_unknown_timing_types_and_generated_inputs(self):
        fixture = self.root / "contract-fixture"
        fixture.mkdir()
        for name in ("Dockerfile.parallel", "parallel.py", "health.py", "contract.json", ".dockerignore",
                     "test_parallel.py", "README.md"):
            (fixture / name).write_bytes(b"fixture")
        path = fixture / "contract.json"
        path.write_text(json.dumps(parallel.CONTRACT))
        self.assertEqual(parallel.fixture_contract(fixture), parallel.CONTRACT)
        for changed in (parallel.CONTRACT | {"schema_version": True},
                        parallel.CONTRACT | {"extra": 1},
                        parallel.CONTRACT | {"barrier": parallel.CONTRACT["barrier"] | {"workers": 3}}):
            path.write_text(json.dumps(changed))
            with self.assertRaises(ValueError):
                parallel.fixture_contract(fixture)
        path.write_text(json.dumps(parallel.CONTRACT))
        (fixture / "__pycache__").mkdir()
        with self.assertRaises(ValueError):
            parallel.fixture_contract(fixture)

    def test_invalid_slots_and_paths_rejected_before_dispatch(self):
        for slot in (-1, 4, True, "0", None):
            with self.subTest(slot=slot), self.assertRaises(ValueError):
                self.operation(slot)
        for character in (",", "\x00", "\n", "\r"):
            with self.subTest(character=repr(character)), self.assertRaises(ValueError):
                parallel.specification(0, self.root / ("bad" + character), self.fixture, self.digest, "parallel-test-run")

    def test_workers_are_dispatched_concurrently_and_results_ordered(self):
        rendezvous = threading.Barrier(4, timeout=2)
        def execute(op):
            rendezvous.wait()
            return op["slot"]
        workers = [SimpleNamespace(execute=execute) for _ in range(4)]
        self.assertEqual(parallel.execute_slots(workers, [self.operation(i) for i in range(4)]), [0, 1, 2, 3])

    def test_failure_still_joins_every_started_worker(self):
        rendezvous, finished = threading.Barrier(4, timeout=2), []
        lock = threading.Lock()
        def execute(op):
            rendezvous.wait()
            if op["slot"] != 0:
                time.sleep(0.02)
            with lock:
                finished.append(op["slot"])
            if op["slot"] == 0:
                raise ValueError("failed solve")
            return op["slot"]
        workers = [SimpleNamespace(execute=execute) for _ in range(4)]
        with self.assertRaisesRegex(RuntimeError, "slots failed: 0"):
            parallel.execute_slots(workers, [self.operation(i) for i in range(4)])
        self.assertEqual(sorted(finished), [0, 1, 2, 3])

    def test_reused_recorder_and_missing_slot_rejected(self):
        one = SimpleNamespace(execute=Mock())
        with self.assertRaises(ValueError):
            parallel.execute_slots([one] * 4, [self.operation(i) for i in range(4)])
        with self.assertRaises(ValueError):
            parallel.execute_slots([SimpleNamespace(execute=Mock()) for _ in range(4)], [self.operation(0)] * 4)
        one.execute.assert_not_called()

    def fake_driver(self):
        selected = parallel.ParallelDriver.__new__(parallel.ParallelDriver)
        selected.output, selected.fixture = self.root / "slot-0", BASE
        selected.output.mkdir(mode=0o700)
        selected.inputs, selected.record = SimpleNamespace(raw=self.inputs), SimpleNamespace(count=0)
        def guard():
            selected.record.count += 4
        def command(*args, **kwargs):
            selected.record.count += 1
            return SimpleNamespace(stdout=b"")
        selected.builder_guard = Mock(side_effect=guard)
        selected.command = Mock(side_effect=command)
        return selected

    def test_single_slot_has_exact_guards_and_exports(self):
        selected = self.fake_driver()
        operation = self.operation()
        with patch.object(parallel.layout, "validate_oci", return_value={"verified": True}) as validate:
            final, proof = selected.execute(operation)
        self.assertEqual(final, operation)
        self.assertEqual(proof, {"oci": {"verified": True}})
        self.assertEqual(selected.record.count, 9)
        self.assertEqual(selected.builder_guard.call_count, 2)
        self.assertEqual(len(validate.call_args.kwargs["canaries"]), 4)
        self.assertEqual(json.loads((selected.output / "operation.intent.json").read_text()), operation)
        with self.assertRaises(ValueError):
            selected.execute(operation)

    def test_changed_contract_or_preexisting_export_fails_before_commands(self):
        selected = self.fake_driver()
        changed = self.operation() | {"run_id": "foreign-run-id"}
        with self.assertRaises(ValueError):
            selected.execute(changed)
        (selected.output / "oci").symlink_to(self.root / "absent")
        with self.assertRaises(ValueError):
            selected.execute(self.operation())
        self.assertEqual(selected.record.count, 0)

    def test_changed_fixture_after_solve_withholds_success(self):
        selected = self.fake_driver()
        original = selected.command.side_effect
        def mutate(*args, **kwargs):
            result = original(*args, **kwargs)
            (self.fixture / "input").write_bytes(b"changed")
            return result
        selected.command.side_effect = mutate
        with self.assertRaisesRegex(ValueError, "fixture changed during"):
            selected.execute(self.operation())
        self.assertTrue((selected.output / "operation.intent.json").exists())
        self.assertFalse((selected.output / "operation.json").exists())

    @contextmanager
    def machine(self, failure=None):
        harness = SimpleNamespace(evidence=self.root, info={"fixture": str(BASE),
            "parallel_fixture": str(self.fixture), "parallel_fixture_sha256": self.digest},
            drivers=[], driver_cleanup_verified=[], monitor=Mock())
        builder = SimpleNamespace(mapping=self.inputs["builder"], ownership={"owner": "exact"},
                                  verify=Mock(return_value={"runtime": True}))
        harness.prepare_builder = Mock(return_value=builder)
        health = SimpleNamespace(record=Mock(), prepare=Mock(), start=Mock(), finish=Mock(return_value={"health": True}))
        if failure == "prepare":
            health.prepare.side_effect = ValueError("prepare failure")
        if failure == "runtime":
            builder.verify.side_effect = ValueError("runtime failure")
        worker_log = Mock(return_value={"log": True}, side_effect=ValueError("log failure") if failure == "log" else None)
        if failure == "start":
            health.start.side_effect = ValueError("start failure")
        if failure in ("health", "solve-and-health"):
            health.finish.side_effect = ValueError("health failure")
        def make_driver(inputs, fixture, output):
            output.mkdir(mode=0o700)
            return SimpleNamespace(output=output, record=Mock())
        replay_calls = 0
        def replay(output, inputs, operation):
            nonlocal replay_calls
            replay_calls += 1
            if failure == "replay" or (failure == "late-replay" and replay_calls == 5):
                raise ValueError("replay failure")
            return {"slot": operation["slot"],
                    "run_interval": {"started_ns": 100, "completed_ns": 200},
                    "guest_run_envelope": {"started_ns": 80, "completed_ns": 230}}
        def execute(selected, operations):
            self.assertEqual(harness.driver_cleanup_verified, [False] * 5)
            if failure in ("solve", "solve-and-health"):
                raise ValueError("solve failure")
            return [(copy.deepcopy(op), {"oci": True}) for op in operations]
        group = Mock(return_value={"overlap": True})
        if failure == "group":
            group.side_effect = ValueError("group failure")
        with patch.dict("sys.modules", {
                "linux_docker_parallel_evidence": SimpleNamespace(validate_slot=replay, validate_group=group),
                "linux_docker_parallel_health": SimpleNamespace(Health=Mock(return_value=health)),
                "linux_docker_buildkit_keep": SimpleNamespace(verify_worker_log=worker_log)}), \
                patch("linux_docker_e2e.input_mapping", return_value=self.inputs), \
                patch.object(parallel.driver, "Inputs", return_value=SimpleNamespace(verify_runtime_evidence=Mock())), \
                patch.object(parallel, "ParallelDriver", side_effect=make_driver), \
                patch.object(parallel, "execute_slots", side_effect=execute):
            yield harness, builder, health

    def test_machine_admits_cleanup_only_after_all_proofs(self):
        with self.machine() as (harness, builder, health):
            result = parallel.run_machine(harness, {"owner": "exact"}, {"scope": True}, {}, {}, 0)
        self.assertEqual(harness.driver_cleanup_verified, [True] * 5)
        self.assertEqual(len(result["operations"]), 4)
        self.assertFalse(result["docker_parity_certified"])
        # A plausible client-translated interval is not guest-clock evidence.
        health.finish.assert_called_once_with([(80, 230)] * 4)
        builder.verify.assert_called_once_with(require_invocation=True)

    def test_each_failure_withholds_cleanup_and_joins_observer(self):
        for failure in ("start", "solve", "replay", "group", "health", "late-replay", "runtime", "log"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                prior = self.root
                self.root = Path(temporary).resolve()
                try:
                    with self.machine(failure) as (harness, _, health):
                        with self.assertRaises(ValueError):
                            parallel.run_machine(harness, {"owner": "exact"}, {}, {}, {}, 0)
                    self.assertEqual(harness.driver_cleanup_verified, [False] * 5)
                    health.finish.assert_called_once()
                    self.assertFalse((self.root / "parallel-machine-0/machine-parallel-validation.json").exists())
                finally:
                    self.root = prior

    def test_failed_health_validation_cannot_hide_original_solve_failure(self):
        with self.machine("solve-and-health") as (harness, _, health):
            with self.assertRaisesRegex(RuntimeError, "workload failed: solve failure; health verification also failed: health failure"):
                parallel.run_machine(harness, {"owner": "exact"}, {}, {}, {}, 0)
        health.finish.assert_called_once_with([])
        self.assertEqual(harness.driver_cleanup_verified, [False] * 5)

    def test_health_preparation_failure_never_starts_observer_or_admits_cleanup(self):
        with self.machine("prepare") as (harness, _, health):
            with self.assertRaisesRegex(ValueError, "prepare failure"):
                parallel.run_machine(harness, {"owner": "exact"}, {}, {}, {}, 0)
        health.start.assert_not_called()
        health.finish.assert_not_called()
        self.assertEqual(harness.driver_cleanup_verified, [False] * 5)


if __name__ == "__main__":
    unittest.main()
