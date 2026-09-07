import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import vz04_common as common  # noqa: E402


class CommonTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="vz04-common-")
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_canonical_json_is_sorted_and_compact(self):
        self.assertEqual(common.canonical_json({"b": 1, "a": [1, {"z": None, "y": "é"}]}), '{"a":[1,{"y":"é","z":null}],"b":1}')
        self.assertEqual(common.canonical_digest({"a": 1}), common.sha256_bytes(b'{"a":1}'))

    def test_write_exclusive_refuses_overwrite_and_document_round_trips(self):
        path = self.root / "doc.json"
        common.document(path, {"k": [1, 2]})
        self.assertEqual(common.load_json(path), {"k": [1, 2]})
        with self.assertRaises(FileExistsError):
            common.write_exclusive(path, b"x")
        common.document(path, {"k": 3}, replace=True)
        self.assertEqual(common.load_json(path), {"k": 3})

    def test_strict_json_rejects_duplicates_and_nan(self):
        with self.assertRaises(common.GateError):
            common.parse_strict_json(b'{"a":1,"a":2}')
        with self.assertRaises(common.GateError):
            common.parse_strict_json(b'{"a":NaN}')

    def test_digest_rejects_symlink_and_hardlink(self):
        target = self.root / "file"
        target.write_bytes(b"data")
        link = self.root / "link"
        link.symlink_to(target)
        with self.assertRaises(common.GateError):
            common.digest_file(link)
        hard = self.root / "hard"
        os.link(target, hard)
        with self.assertRaises(common.GateError):
            common.digest_file(target)

    def test_tree_entries_skip_pycache_and_reject_symlinks(self):
        (self.root / "a").write_bytes(b"1")
        (self.root / "__pycache__").mkdir()
        (self.root / "__pycache__" / "x.pyc").write_bytes(b"2")
        rows = common.tree_entries(self.root)
        self.assertEqual([row[0] for row in rows], ["a"])
        (self.root / "s").symlink_to(self.root / "a")
        with self.assertRaises(common.GateError):
            common.tree_entries(self.root)

    def test_checksums_roundtrip_detects_tamper_and_uncovered(self):
        (self.root / "one").write_bytes(b"1")
        (self.root / "sub").mkdir()
        (self.root / "sub" / "two").write_bytes(b"2")
        common.write_checksums(self.root)
        self.assertEqual(common.verify_checksums(self.root), [])
        (self.root / "sub" / "two").write_bytes(b"changed")
        (self.root / "three").write_bytes(b"3")
        findings = common.verify_checksums(self.root)
        self.assertTrue(any("digest mismatch sub/two" in f for f in findings))
        self.assertTrue(any("file not covered three" in f for f in findings))

    def test_checked_text_and_relative_components(self):
        self.assertEqual(common.checked_text("gate-dry-smoke-1", common.RUN_ID_PATTERN, "run-id"), "gate-dry-smoke-1")
        with self.assertRaises(common.GateError):
            common.checked_text("short", common.RUN_ID_PATTERN, "run-id")
        with self.assertRaises(common.GateError):
            common.relative_components("../escape")
        with self.assertRaises(common.GateError):
            common.relative_components("/abs")

    def test_canonical_path_rejects_symlinked_component(self):
        real = self.root / "real"
        real.mkdir()
        alias = self.root / "alias"
        alias.symlink_to(real)
        with self.assertRaises(common.GateError):
            common.canonical_path(str(alias))
        self.assertEqual(common.canonical_path(str(real.resolve())), real.resolve())


if __name__ == "__main__":
    unittest.main()
