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
import re
import socket
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
            # Three kinds, because they are no longer the same thing: an EMPTY
            # PID file is what criterion 19's injected migration failure leaves
            # on every run and names no process; a GARBAGE one is unreadable and
            # could name anything; the rest are ordinary running daemons.
            if name.startswith("empty"):
                contents = ""
            elif name.startswith("bad"):
                contents = "not-a-pid"
            else:
                contents = "4242"
            (runtime / "d.pid").write_text(contents)
            os.mkfifo(runtime / "d.sock")   # a placeholder daemon_artifacts will pair
        return state

    def sweep(self, state, *, survivors=(), expect_error=True):
        """Run the sweep with attribution, stopping and the live-process
        cross-check stubbed, recording what each did."""
        attempted, stopped = [], []

        def fingerprint(_state, pidfile, socket_path):
            attempted.append(pidfile.parent.name)
            if not re.fullmatch(r"[0-9]+", pidfile.read_text().strip()):
                raise GateError("invalid daemon PID")
            return {"pid": 4242, "socket": str(socket_path)}

        def stop_one(identity, pidfile, _socket_path):
            stopped.append(pidfile.parent.name)
            return identity

        original = (subject.daemon_fingerprint, subject._stop_one_daemon,
                    subject.processes_referencing)
        subject.daemon_fingerprint = fingerprint
        subject._stop_one_daemon = stop_one
        subject.processes_referencing = lambda _state, exclude_pids=(): list(survivors)
        try:
            if expect_error:
                with self.assertRaises(subject.CleanupError) as caught:
                    subject.stop_daemons(state)
                message = str(caught.exception)
            else:
                subject.stop_daemons(state)
                message = ""
        finally:
            (subject.daemon_fingerprint, subject._stop_one_daemon,
             subject.processes_referencing) = original
        return attempted, stopped, message

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

    def test_an_empty_pid_file_with_nothing_alive_is_not_a_cleanup_failure(self):
        """The residue criterion 19 leaves on every run must not fail the lane.

        The injected migration failure dispatches a daemon that refuses to start
        and never writes its PID. An empty file names no process, so it cannot be
        a daemon this sweep failed to stop -- and treating it as one failed the
        whole topology lane on cleanup after all 19 real daemons had been stopped
        and nothing had leaked, which costs every row the lane would have carried.
        """
        with tempfile.TemporaryDirectory(prefix="vz04-daemons-") as tmp:
            state = self.lane(tmp, ["empty-migf", "mix", "net-a"])
            attempted, stopped, _ = self.sweep(state, expect_error=False)
            self.assertEqual(sorted(attempted), ["empty-migf", "mix", "net-a"])
            self.assertEqual(sorted(stopped), ["mix", "net-a"])

    def test_an_empty_pid_file_is_a_failure_when_something_is_still_alive(self):
        """The exemption is evidence, not a special case for a filename."""
        with tempfile.TemporaryDirectory(prefix="vz04-daemons-") as tmp:
            state = self.lane(tmp, ["empty-migf", "mix"])
            _attempted, stopped, message = self.sweep(
                state, survivors=[(9931, "vz-runtimed --state-db ... --socket ...")])
            self.assertEqual(stopped, ["mix"])
            self.assertIn("empty PID file while 1 process(es) still reference", message)
            self.assertIn("9931", message)

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


class MachineExecArgvTests(unittest.TestCase):
    """A probe must be spelled for the Machine it runs on.

    Every Machine exec used to render `/bin/busybox sh -c`. A macOS guest has no
    BusyBox, so every probe aimed at the native Machine failed before running,
    as `backend_unavailable` -> exit 5 -- which read in the lane's output as the
    address claim failing, when the Machine did hold its address and the
    crossing did carry traffic both ways.
    """

    def test_a_linux_machine_runs_through_busybox(self):
        argv = checks.machine_exec_argv("machine-0", "id")
        self.assertEqual(argv[-4:], ["/bin/busybox", "sh", "-c", "id"])

    def test_the_native_macos_machine_runs_through_a_shell_it_has(self):
        argv = checks.machine_exec_argv(checks.MACOS_MACHINE, "id")
        self.assertEqual(argv[-3:], ["/bin/sh", "-c", "id"])
        self.assertNotIn("/bin/busybox", argv)

    def test_both_address_the_same_machine_the_same_way(self):
        """Only the interpreter differs; the selector must not drift with it."""
        linux = checks.machine_exec_argv("machine-0", "id")
        native = checks.machine_exec_argv(checks.MACOS_MACHINE, "id")
        self.assertEqual(linux[:4], ["exec", "--environment", "default", "--machine"])
        self.assertEqual(native[:4], ["exec", "--environment", "default", "--machine"])
        self.assertEqual(native[4], checks.MACOS_MACHINE)


if __name__ == "__main__":
    unittest.main()


class StraySocketTests(unittest.TestCase):
    """A socket file is only a leak while something is answering on it."""

    def lane(self, tmp: str) -> subject.LaneState:
        state = subject.LaneState(Path(tmp) / "state", Path(tmp) / "bin")
        state.create()
        return state

    def test_a_socket_nobody_is_listening_on_is_not_a_leak(self):
        # Criterion 19's migration exercise leaves exactly this behind on every
        # run: a socket with no PID file and nothing serving it. Counting it
        # failed the whole topology lane on cleanup, and the lane is
        # all-or-nothing, so every row it would have carried went with it.
        with tempfile.TemporaryDirectory(prefix="vz04-stray-") as tmp:
            state = self.lane(tmp)
            runtime = state.socket_root / "migr"
            runtime.mkdir(mode=0o700, parents=True, exist_ok=True)
            abandoned = runtime / "d.sock"
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(str(abandoned))
            listener.listen(1)
            listener.close()          # the file survives; nothing serves it
            self.assertTrue(abandoned.exists())
            self.assertEqual(subject.stray_sockets(state), [])

    def test_a_socket_something_is_still_serving_is_a_leak(self):
        """The exemption is evidence, not amnesty for a filename."""
        with tempfile.TemporaryDirectory(prefix="vz04-stray-") as tmp:
            state = self.lane(tmp)
            runtime = state.socket_root / "migr"
            runtime.mkdir(mode=0o700, parents=True, exist_ok=True)
            served = runtime / "d.sock"
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.addCleanup(listener.close)
            listener.bind(str(served))
            listener.listen(1)
            self.assertEqual(subject.stray_sockets(state), [served])
