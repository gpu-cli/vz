"""Real local processes and adversarial filesystem records; no Docker required."""
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("parallel_fixture", HERE / "parallel.py")
parallel = importlib.util.module_from_spec(spec)
spec.loader.exec_module(parallel)
BASE = "python:3@sha256:" + "a" * 64


def environment(slot=0):
    return {"FIXTURE_BASE": BASE, "FIXTURE_RUN": "unit-parallel-v1", "FIXTURE_SLOT": str(slot)}


def record(slot, run="unit-parallel-v1"):
    return {"schema_version": 1, "run_id": run, "slot": slot,
            "started_unix_ns": time.time_ns(), "started_monotonic_ns": parallel.monotonic_ns(),
            "ready_unix_ns": time.time_ns(), "ready_monotonic_ns": parallel.monotonic_ns()}


class ParallelFixtureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.barrier = self.root / "barrier"
        self.barrier.mkdir()

    def run_fixture(self, env=None):
        return parallel.run(self.barrier, self.root / "out", environment() if env is None else env)

    def put(self, slot, value=None):
        directory = self.barrier / ("slot-" + str(slot))
        directory.mkdir()
        (directory / "ready.json").write_bytes(parallel.canonical(record(slot) if value is None else value))
        return directory / "ready.json"

    def observe(self):
        fd = os.open(self.barrier, parallel.DIRECTORY)
        try:
            return parallel.observe(fd, environment()["FIXTURE_RUN"])
        finally:
            os.close(fd)

    def test_contract_and_exact_graph(self):
        contract = json.loads((HERE / "contract.json").read_text())
        self.assertEqual(contract["barrier"], {"workers": parallel.WORKERS,
            "timeout_ns": parallel.TIMEOUT_NS, "poll_interval_ns": parallel.POLL_INTERVAL_NS,
            "release_dwell_ns": parallel.RELEASE_DWELL_NS, "max_samples": parallel.MAX_SAMPLES,
            "max_record_bytes": parallel.MAX_RECORD_BYTES})
        self.assertEqual(contract["health"], {"samples": 60, "interval_ns": 10**9,
            "max_lateness_ns": 250000000, "request_timeout_ns": 500000000,
            "observer_bound_ns": 70000000000})
        self.assertEqual((HERE / ".dockerignore").read_text().splitlines(),
                         ["*", "!Dockerfile.parallel", "!parallel.py"])
        self.assertEqual((HERE / "Dockerfile.parallel").read_text().splitlines(), [
            "ARG FIXTURE_BASE", "FROM ${FIXTURE_BASE} AS build", "ARG FIXTURE_BASE",
            "ARG FIXTURE_RUN", "ARG FIXTURE_SLOT", "COPY parallel.py /fixture/parallel.py",
            "RUN --network=none --mount=type=cache,id=vz04-parallel-barrier-v1,target=/barrier,sharing=shared python3 /fixture/parallel.py",
            "FROM scratch AS output", "COPY --from=build /out/payload.txt /payload.txt"])

    def test_four_real_processes_release_same_generation_and_exact_payloads(self):
        code = ("import importlib.util,json,pathlib,sys; "
                "s=importlib.util.spec_from_file_location('f',sys.argv[1]); "
                "f=importlib.util.module_from_spec(s); s.loader.exec_module(f); "
                "r=f.run(pathlib.Path(sys.argv[2]),pathlib.Path(sys.argv[3])); "
                "print(f.PREFIX+f.canonical(r).decode(),flush=True); "
                "sys.exit(0 if r['outcome']=='released' else 1)")
        children = []
        try:
            for slot in range(4):
                children.append(subprocess.Popen([sys.executable, "-B", "-c", code,
                    str(HERE / "parallel.py"), str(self.barrier), str(self.root / ("out-" + str(slot)))],
                    env=dict(os.environ, **environment(slot)), stdout=subprocess.PIPE, stderr=subprocess.PIPE))
            rows = []
            for slot, child in enumerate(children):
                out, err = child.communicate(timeout=10)
                self.assertEqual(err, b"")
                self.assertEqual(child.returncode, 0, out)
                self.assertEqual(out.count(b"\n"), 1)
                self.assertTrue(out.startswith(parallel.PREFIX.encode()))
                row = json.loads(out[len(parallel.PREFIX):])
                self.assertEqual(row["participants"][slot], {key: row[key] for key in parallel.RECORD_KEYS})
                self.assertEqual(row["samples"][-1]["ready_slots"], [0, 1, 2, 3])
                first = next(sample for sample in row["samples"] if len(sample["ready_slots"]) == 4)
                self.assertEqual(row["all_ready_monotonic_ns"], first["monotonic_ns"])
                self.assertGreaterEqual(row["released_monotonic_ns"] - row["all_ready_monotonic_ns"], 10**9)
                payload = self.root / ("out-" + str(slot)) / "payload.txt"
                self.assertEqual(payload.read_bytes(), ("vz04-parallel-v1\nslot=" + str(slot) + "\n").encode())
                self.assertEqual(stat.S_IMODE(payload.stat().st_mode), 0o644)
                rows.append(row)
            self.assertEqual(len({row["generation_sha256"] for row in rows}), 1)
            self.assertTrue(all(row["participants"] == rows[0]["participants"] for row in rows))
            self.assertGreaterEqual(min(row["released_monotonic_ns"] for row in rows)
                                    - max(row["ready_monotonic_ns"] for row in rows), 10**9)
            self.assertEqual(len(list(self.barrier.iterdir())), 4)
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate()

    def test_invalid_environment_has_no_claims(self):
        for key, value in [("FIXTURE_SLOT", "00"), ("FIXTURE_SLOT", "4"), ("FIXTURE_SLOT", "-1"),
                           ("FIXTURE_RUN", "../foreign"), ("FIXTURE_RUN", ""), ("FIXTURE_BASE", "python:latest")]:
            with self.subTest(key=key, value=value):
                result = self.run_fixture(dict(environment(), **{key: value}))
                self.assertEqual(result["error_code"], "invalid_environment")
                self.assertEqual(list(self.barrier.iterdir()), [])

    def test_duplicate_claim_preserves_bytes(self):
        path = self.put(0)
        before = path.read_bytes()
        result = self.run_fixture()
        self.assertEqual(result["error_code"], "slot_already_claimed")
        self.assertEqual(path.read_bytes(), before)

    def test_foreign_run_schema_and_types(self):
        path = self.put(1)
        for change in [{"run_id": "foreign"}, {"slot": 2}, {"schema_version": True},
                       {"extra": 1}, {"ready_monotonic_ns": True}, {"ready_unix_ns": 0}]:
            with self.subTest(change=change):
                path.write_bytes(parallel.canonical(dict(record(1), **change)))
                with self.assertRaises(parallel.BarrierError):
                    self.observe()

    def test_duplicate_json_and_nonfinite_and_oversize(self):
        path = self.put(1)
        for raw in [b'{"slot":1,"slot":1}', b'{"x":NaN}', b'{"x":Infinity}', b'x' * 1025, b'[]']:
            with self.subTest(raw_length=len(raw)):
                path.write_bytes(raw)
                with self.assertRaises(parallel.BarrierError):
                    self.observe()

    def test_regular_file_symlink_and_hardlink_rejected(self):
        path = self.put(1)
        original = path.read_bytes()
        path.unlink()
        external = self.root / "record"
        external.write_bytes(original)
        path.symlink_to(external)
        with self.assertRaises(OSError):
            self.observe()
        path.unlink()
        os.link(external, path)
        with self.assertRaises(parallel.BarrierError):
            self.observe()
        path.unlink()
        os.mkfifo(path)
        with self.assertRaises(parallel.BarrierError):
            self.observe()

    def test_stale_or_future_record_rejected(self):
        path = self.put(1)
        for change in [{"started_monotonic_ns": 1}, {"ready_monotonic_ns": parallel.monotonic_ns() + 10**12}]:
            path.write_bytes(parallel.canonical(dict(record(1), **change)))
            with self.assertRaisesRegex(parallel.BarrierError, "stale_record"):
                self.observe()

    def test_slot_symlink_and_unexpected_inventory_rejected(self):
        outside = self.root / "outside"
        outside.mkdir()
        (self.barrier / "slot-1").symlink_to(outside)
        self.assertEqual(self.run_fixture()["error_code"], "filesystem_error")
        (self.barrier / "slot-1").unlink()
        (self.barrier / "foreign").write_text("private-marker-do-not-print")
        result = self.run_fixture()
        self.assertEqual(result["error_code"], "unexpected_inventory")
        self.assertNotIn("private-marker", parallel.canonical(result).decode())

    def test_pending_is_not_ready_and_unknown_slot_contents_fail(self):
        slot = self.barrier / "slot-1"
        slot.mkdir()
        (slot / "ready.pending").write_bytes(b"partial")
        self.assertEqual(self.observe(), {})
        (slot / "extra").write_bytes(b"")
        with self.assertRaises(parallel.BarrierError):
            self.observe()

    def test_missing_workers_timeout_keeps_claim_no_payload(self):
        with patch.object(parallel, "TIMEOUT_NS", 20_000_000), patch.object(parallel, "POLL_INTERVAL_NS", 2_000_000):
            result = self.run_fixture()
        self.assertEqual(result["error_code"], "barrier_timeout")
        self.assertEqual(result["outcome"], "failed")
        self.assertTrue((self.barrier / "slot-0" / "ready.json").is_file())
        self.assertFalse((self.root / "out").exists())
        self.assertTrue(all(sample["ready_slots"] == [0] for sample in result["samples"]))

    def test_observed_readiness_cannot_change_or_disappear(self):
        row = {"run_id": "unit-parallel-v1", "samples": []}
        seen = {0: record(0)}
        for changed in [{}, {0: dict(seen[0], run_id="foreign")}]:
            with patch.object(parallel, "observe", return_value=changed):
                with self.assertRaisesRegex(parallel.BarrierError, "readiness_regressed"):
                    parallel.sample(-1, row, seen.copy())

    def test_sample_inventory_bounded(self):
        row = {"run_id": "unit-parallel-v1", "samples": [{}] * parallel.MAX_SAMPLES}
        with patch.object(parallel, "observe", return_value={}):
            with self.assertRaisesRegex(parallel.BarrierError, "sample_limit"):
                parallel.sample(-1, row, {})

    def test_after_dwell_change_fails_without_payload(self):
        for slot in (1, 2, 3):
            self.put(slot)
        def mutate(_seconds):
            (self.barrier / "slot-1" / "ready.json").write_bytes(parallel.canonical(record(1, "foreign")))
        with patch.object(parallel.time, "sleep", side_effect=mutate), patch.object(parallel, "RELEASE_DWELL_NS", 1_000_000):
            result = self.run_fixture()
        self.assertEqual(result["outcome"], "failed")
        self.assertIsNone(result["payload"])
        self.assertFalse((self.root / "out").exists())

    def test_output_mode_independent_of_umask_and_existing_output_preserved(self):
        for slot in (1, 2, 3):
            self.put(slot)
        old = os.umask(0o077)
        try:
            with patch.object(parallel, "RELEASE_DWELL_NS", 0):
                result = self.run_fixture()
        finally:
            os.umask(old)
        self.assertEqual(result["outcome"], "released")
        path = self.root / "out" / "payload.txt"
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o644)
        before = path.read_bytes()
        # New exact barrier claim, same already-existing output: never overwrite.
        fresh = self.root / "fresh"
        fresh.mkdir()
        self.barrier = fresh
        for slot in (1, 2, 3):
            self.put(slot)
        with patch.object(parallel, "RELEASE_DWELL_NS", 0):
            result = self.run_fixture()
        self.assertEqual(result["error_code"], "filesystem_error")
        self.assertEqual(path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
