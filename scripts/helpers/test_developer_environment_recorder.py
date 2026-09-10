"""Inventory digests, offline, against throwaway files.

The topology lane inventories its state root before and after most checks, and
that root holds Machine disks: a Developer Machine's Docker `data.img` is a
64 GiB sparse file. Hashing one whole read 68,719,476,736 bytes to cover the
29 MB of data actually in it, which dominated the tail of a lane run. These
tests pin the two properties that make skipping the holes safe rather than
merely fast: an ordinary file's digest is unchanged, and a sparse file's digest
still moves when anything about it moves.
"""
import hashlib
import os
import stat
import types
from pathlib import Path
import tempfile
import time
import unittest

import developer_environment_checks as checks
import developer_environment_recorder as subject
from vz04_common import GateError

# Large enough that reading it whole would be unmistakably slow, and larger than
# any hole-free file a test would otherwise make.
SPARSE_BYTES = 64 * 1024 ** 3


def sparse(path: Path, writes) -> Path:
    with open(path, "wb") as stream:
        stream.truncate(SPARSE_BYTES)
        for offset, payload in writes:
            stream.seek(offset)
            stream.write(payload)
    return path


class DenseFileTests(unittest.TestCase):
    def test_an_ordinary_file_keeps_its_plain_whole_file_digest(self):
        """The common case must not change value, only cost.

        Rows are compared against each other across phases and a digest that
        silently changed meaning would read as "this file changed".
        """
        with tempfile.TemporaryDirectory(prefix="vz04-inv-") as tmp:
            path = Path(tmp) / "dense.bin"
            payload = b"hello world" * 1000
            path.write_bytes(payload)
            self.assertEqual(subject.file_digest(path, len(payload)), hashlib.sha256(payload).hexdigest())

    def test_an_empty_file_keeps_the_digest_of_no_bytes(self):
        with tempfile.TemporaryDirectory(prefix="vz04-inv-") as tmp:
            path = Path(tmp) / "empty.bin"
            path.write_bytes(b"")
            self.assertEqual(subject.file_digest(path, 0), hashlib.sha256(b"").hexdigest())


class SparseFileTests(unittest.TestCase):
    def test_a_sparse_file_is_digested_without_reading_its_holes(self):
        with tempfile.TemporaryDirectory(prefix="vz04-inv-") as tmp:
            path = sparse(Path(tmp) / "s.img", [(0, b"HEAD" * 256), (32 * 1024 ** 3, b"MIDDLE" * 256),
                                                (SPARSE_BYTES - 4096, b"TAIL" * 256)])
            allocated = os.stat(path).st_blocks * 512
            self.assertLess(allocated, 16 * 1024 * 1024, "fixture is not sparse on this filesystem")
            started = time.monotonic()
            digest = subject.file_digest(path, SPARSE_BYTES)
            elapsed = time.monotonic() - started
            # Reading 64 GiB whole measured 30.5 s on this hardware at 2.25 GB/s.
            # A second is three orders of magnitude away from that and far above
            # what the extent walk needs, so this fails on a regression to
            # whole-file reads without being tight enough to flake under load.
            self.assertLess(elapsed, 1.0, f"digest took {elapsed:.3f}s; holes are being read")
            self.assertTrue(digest.startswith("sparse:"),
                            "a framed digest must not be presentable as a plain whole-file sha256")

    def test_a_change_inside_a_data_extent_changes_the_digest(self):
        with tempfile.TemporaryDirectory(prefix="vz04-inv-") as tmp:
            path = sparse(Path(tmp) / "s.img", [(0, b"HEAD" * 256), (32 * 1024 ** 3, b"MIDDLE" * 256)])
            before = subject.file_digest(path, SPARSE_BYTES)
            with open(path, "r+b") as stream:
                stream.seek(32 * 1024 ** 3)
                stream.write(b"CHANGE")
            self.assertNotEqual(subject.file_digest(path, SPARSE_BYTES), before)

    def test_the_same_bytes_at_a_different_offset_change_the_digest(self):
        """The extent map is hashed, not just the bytes.

        Without framing, moving a payload from one offset to another would leave
        the concatenated data identical and the digest unchanged -- a file whose
        layout changed reading as a file that did not.
        """
        with tempfile.TemporaryDirectory(prefix="vz04-inv-") as tmp:
            here = sparse(Path(tmp) / "a.img", [(0, b"HEAD" * 256), (16 * 1024 ** 3, b"BODY" * 256)])
            there = sparse(Path(tmp) / "b.img", [(0, b"HEAD" * 256), (48 * 1024 ** 3, b"BODY" * 256)])
            self.assertNotEqual(subject.file_digest(here, SPARSE_BYTES), subject.file_digest(there, SPARSE_BYTES))

    def test_a_file_that_is_all_holes_still_gets_a_digest(self):
        with tempfile.TemporaryDirectory(prefix="vz04-inv-") as tmp:
            path = Path(tmp) / "holes.img"
            with open(path, "wb") as stream:
                stream.truncate(SPARSE_BYTES)
            digest = subject.file_digest(path, SPARSE_BYTES)
            self.assertTrue(digest.startswith("sparse:"))
            # Its size is part of the framing, so two all-hole files of
            # different sizes are distinguishable.
            smaller = Path(tmp) / "smaller.img"
            with open(smaller, "wb") as stream:
                stream.truncate(SPARSE_BYTES // 2)
            self.assertNotEqual(subject.file_digest(smaller, SPARSE_BYTES // 2), digest)


class InventoryTests(unittest.TestCase):
    def test_the_inventory_carries_the_digest_and_the_diff_sees_a_change(self):
        with tempfile.TemporaryDirectory(prefix="vz04-inv-") as tmp:
            root = Path(tmp)
            path = sparse(root / "disk.img", [(0, b"HEAD" * 256), (8 * 1024 ** 3, b"BODY" * 256)])
            (root / "plain.txt").write_bytes(b"ordinary")
            before = subject.inventory(root)
            rows = {row[0]: row for row in before}
            self.assertEqual(rows["plain.txt"][4], hashlib.sha256(b"ordinary").hexdigest())
            self.assertTrue(rows["disk.img"][4].startswith("sparse:"))
            self.assertEqual(rows["disk.img"][3], SPARSE_BYTES)
            with open(path, "r+b") as stream:
                stream.seek(8 * 1024 ** 3)
                stream.write(b"MOVED")
            self.assertEqual(subject.inventory_diff(before, subject.inventory(root)), ["changed: disk.img"])


class StopDaemonsTests(unittest.TestCase):
    """One unattributable artifact must not excuse the daemons that can be stopped.

    This is the exact shape that leaked eight daemons on a real run. Criterion
    19's injected migration failure leaves an EMPTY `migf/d.pid`, `migf` sorts
    before `mix`, `net-a`, `stat` and `store-deny`, and the sweep raised on the
    first artifact it could not attribute -- so it stopped nothing. On macOS the
    consequence is not untidiness: the host caps concurrent virtual machines, so
    leaked daemons holding native Machines make every later Environment fail
    with `VZErrorDomain:6`.
    """

    def lane(self, tmp: str, names) -> subject.LaneState:
        state = subject.LaneState(Path(tmp) / "state", Path(tmp) / "bin")
        state.create()
        for name in names:
            runtime = state.socket_root / name
            runtime.mkdir(mode=0o700, parents=True, exist_ok=True)
            # An empty PID file is what the injected migration failure leaves.
            (runtime / "d.pid").write_text("" if name.startswith("bad") else "4242")
            os.mkfifo(runtime / "d.sock")   # a placeholder daemon_artifacts will pair
        return state

    def sweep(self, state):
        """Run the sweep with attribution and stopping stubbed, recording both."""
        attempted, stopped = [], []

        def fingerprint(_state, pidfile, socket_path):
            attempted.append(pidfile.parent.name)
            if not pidfile.read_text().strip():
                raise GateError("invalid daemon PID")
            return {"pid": 4242, "socket": str(socket_path)}

        def stop_one(identity, pidfile, _socket_path):
            stopped.append(pidfile.parent.name)
            return identity

        original = (subject.daemon_fingerprint, subject._stop_one_daemon)
        subject.daemon_fingerprint, subject._stop_one_daemon = fingerprint, stop_one
        try:
            with self.assertRaises(subject.CleanupError) as caught:
                subject.stop_daemons(state)
        finally:
            subject.daemon_fingerprint, subject._stop_one_daemon = original
        return attempted, stopped, str(caught.exception)

    def test_the_daemons_that_can_be_stopped_are_stopped_first(self):
        with tempfile.TemporaryDirectory(prefix="vz04-daemons-") as tmp:
            # "bad" sorts first, exactly as "migf" does among the real isolates.
            state = self.lane(tmp, ["bad-migf", "mix", "net-a"])
            attempted, stopped, message = self.sweep(state)
            self.assertEqual(sorted(attempted), ["bad-migf", "mix", "net-a"],
                             "every artifact must be attempted, not just those before the first failure")
            self.assertEqual(sorted(stopped), ["mix", "net-a"])
            self.assertIn("2 daemon(s) stopped", message)
            self.assertIn("invalid daemon PID", message)

    def test_the_unattributable_artifact_is_still_reported(self):
        """Stopping the rest must not quietly forgive the one that failed."""
        with tempfile.TemporaryDirectory(prefix="vz04-daemons-") as tmp:
            state = self.lane(tmp, ["bad-migf"])
            _attempted, stopped, message = self.sweep(state)
            self.assertEqual(stopped, [])
            self.assertIn("0 daemon(s) stopped", message)
            self.assertIn("1 artifact(s) not attributed", message)

    def test_a_clean_sweep_raises_nothing_and_returns_what_it_stopped(self):
        with tempfile.TemporaryDirectory(prefix="vz04-daemons-") as tmp:
            state = self.lane(tmp, ["mix", "net-a"])
            calls = []

            def fingerprint(_state, pidfile, socket_path):
                return {"pid": 4242, "socket": str(socket_path)}

            def stop_one(identity, pidfile, _socket_path):
                calls.append(pidfile.parent.name)
                return identity

            original = (subject.daemon_fingerprint, subject._stop_one_daemon)
            subject.daemon_fingerprint, subject._stop_one_daemon = fingerprint, stop_one
            try:
                stopped = subject.stop_daemons(state)
            finally:
                subject.daemon_fingerprint, subject._stop_one_daemon = original
            self.assertEqual(sorted(calls), ["mix", "net-a"])
            self.assertEqual(len(stopped), 2)


class LegacyArtifactStagingTests(unittest.TestCase):
    """Criterion 19 EXECUTES the pinned v0.3.20 daemon, so it must be executable.

    The staging instruction the check prints is a plain `curl -o`, which writes
    0644. Executing the operator's own file therefore raised PermissionError
    and crashed the entire lane after every other sub-check had already run --
    following the printed instruction exactly was the way to reproduce it.
    """

    class Ctx:
        def __init__(self, repo_root, tmp):
            self.repo_root = repo_root
            self.state = types.SimpleNamespace(tmp=tmp)

    def stage(self, tmp, mode):
        repo = Path(tmp) / "repo"
        (repo / ".cache" / "vz-0.4-legacy-v0.3.20").mkdir(parents=True)
        artifact = repo / checks.LEGACY_ARTIFACT_CACHE
        artifact.write_bytes(b"#!/bin/sh\nexit 0\n")
        artifact.chmod(mode)
        return repo, artifact, hashlib.sha256(artifact.read_bytes()).hexdigest()

    def test_a_non_executable_staged_artifact_still_yields_a_runnable_copy(self):
        with tempfile.TemporaryDirectory(prefix="vz04-legacy-") as tmp:
            repo, artifact, digest = self.stage(tmp, 0o644)
            scratch = Path(tmp) / "scratch"
            check = checks.SubCheck("gate.migration.install_upgrade_rollback_uninstall", "x")
            runnable = checks._legacy_artifact(self.Ctx(repo, scratch), check, digest, "https://example.invalid/x")
            self.assertIsNotNone(runnable)
            self.assertNotEqual(runnable, artifact, "the operator's cache file must not be what gets executed")
            self.assertTrue(os.access(runnable, os.X_OK), "the copy the lane runs must be executable")
            # The cache is not mutated: it is the operator's file, and the check
            # may run against a directory it has no business writing to.
            self.assertEqual(stat.S_IMODE(artifact.stat().st_mode), 0o644)
            self.assertEqual(check.status, "PASS")

    def test_a_staged_artifact_with_the_wrong_digest_is_refused(self):
        with tempfile.TemporaryDirectory(prefix="vz04-legacy-") as tmp:
            repo, _artifact, _digest = self.stage(tmp, 0o644)
            check = checks.SubCheck("gate.migration.install_upgrade_rollback_uninstall", "x")
            runnable = checks._legacy_artifact(self.Ctx(repo, Path(tmp) / "scratch"), check, "0" * 64,
                                               "https://example.invalid/x")
            self.assertIsNone(runnable)
            self.assertEqual(check.status, "FAIL")

    def test_an_absent_artifact_reports_not_implemented_naming_the_live_checkout(self):
        """The instruction must name a path that still exists after the run.

        A frozen lane's own root is deleted when the run ends, so an
        instruction resolved against it tells the operator to stage the file
        somewhere that is about to disappear.
        """
        with tempfile.TemporaryDirectory(prefix="vz04-legacy-") as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            check = checks.SubCheck("gate.migration.install_upgrade_rollback_uninstall", "x")
            runnable = checks._legacy_artifact(self.Ctx(repo, Path(tmp) / "scratch"), check, "0" * 64,
                                               "https://example.invalid/x")
            self.assertIsNone(runnable)
            self.assertIsNotNone(check.not_implemented)
            self.assertIn(str(repo / checks.LEGACY_ARTIFACT_CACHE), check.not_implemented)


if __name__ == "__main__":
    unittest.main()
