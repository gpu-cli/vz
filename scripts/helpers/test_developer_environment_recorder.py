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
from pathlib import Path
import tempfile
import time
import unittest

import developer_environment_recorder as subject

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


if __name__ == "__main__":
    unittest.main()
