"""Offline tests for vz04_source_tree: temporary Git repositories, no network or product process."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import vz04_source_tree as source_tree  # noqa: E402

GIT_ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "test",
    "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "test",
    "GIT_COMMITTER_EMAIL": "test@example.invalid",
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_CONFIG_NOSYSTEM": "1",
}


def run_git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, env=GIT_ENV,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def make_repo(root):
    run_git(root, "init", "-q", "--initial-branch=main")
    (root / "crates").mkdir()
    (root / "crates" / "Cargo.lock").write_text("# lock\n")
    (root / "README.md").write_text("hello\n")
    (root / "run.sh").write_text("#!/bin/sh\n")
    (root / "run.sh").chmod(0o755)
    os.symlink("README.md", root / "link.md")
    run_git(root, "add", "-A")
    run_git(root, "commit", "-q", "-m", "initial")


def expected_tree_digest(root):
    entries = [
        ["README.md", "100644", hashlib.sha256(b"hello\n").hexdigest()],
        ["crates/Cargo.lock", "100644", hashlib.sha256(b"# lock\n").hexdigest()],
        ["link.md", "120000", hashlib.sha256(b"README.md").hexdigest()],
        ["run.sh", "100755", hashlib.sha256(b"#!/bin/sh\n").hexdigest()],
    ]
    payload = json.dumps(entries, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    return hashlib.sha256(payload).hexdigest()


class SourceTreeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        make_repo(self.root)

    def tearDown(self):
        self.temporary.cleanup()

    def test_clean_checkout_description(self):
        report = source_tree.describe(self.root)
        self.assertTrue(report["clean"])
        self.assertEqual(report["tracked_changes"], [])
        self.assertEqual(report["untracked_paths"], [])
        self.assertEqual(report["submodules"], [])
        self.assertEqual(report["tracked_file_count"], 4)
        self.assertEqual(report["tree_sha256"], expected_tree_digest(self.root))
        self.assertEqual(report["lockfile"], {"path": "crates/Cargo.lock", "tracked": True, "unchanged": True})
        head = subprocess.check_output(["git", "-C", str(self.root), "rev-parse", "HEAD"], env=GIT_ENV).decode().strip()
        tree = subprocess.check_output(["git", "-C", str(self.root), "rev-parse", "HEAD^{tree}"], env=GIT_ENV).decode().strip()
        self.assertEqual(report["commit"], head)
        self.assertEqual(report["git_tree"], tree)

    def test_digest_is_deterministic_and_content_sensitive(self):
        first = source_tree.describe(self.root)["tree_sha256"]
        self.assertEqual(first, source_tree.describe(self.root)["tree_sha256"])
        (self.root / "README.md").write_text("changed\n")
        run_git(self.root, "commit", "-q", "-am", "change")
        self.assertNotEqual(first, source_tree.describe(self.root)["tree_sha256"])

    def test_modified_tracked_file_is_rejected(self):
        (self.root / "README.md").write_text("dirty\n")
        with self.assertRaisesRegex(source_tree.SourceTreeError, "1 tracked change"):
            source_tree.describe(self.root)

    def test_untracked_file_is_rejected(self):
        (self.root / ".beads").mkdir()
        (self.root / ".beads" / "state.json").write_text("{}")
        with self.assertRaisesRegex(source_tree.SourceTreeError, r"untracked path\(s\): \.beads/state\.json"):
            source_tree.describe(self.root)

    def test_dev_unclean_records_changes_and_hashes_working_tree(self):
        (self.root / "README.md").write_text("dirty\n")
        (self.root / "scratch.txt").write_text("x")
        report = source_tree.describe(self.root, allow_unclean=True)
        self.assertFalse(report["clean"])
        self.assertEqual(report["tracked_changes"], [{"status": " M", "path": "README.md", "original": None}])
        self.assertEqual(report["untracked_paths"], ["scratch.txt"])
        self.assertNotEqual(report["tree_sha256"], expected_tree_digest(self.root))
        tree = subprocess.check_output(["git", "-C", str(self.root), "rev-parse", "HEAD^{tree}"], env=GIT_ENV).decode().strip()
        self.assertEqual(report["git_tree"], tree)

    def test_lockfile_change_rejected_even_when_unclean_allowed(self):
        (self.root / "crates" / "Cargo.lock").write_text("# edited\n")
        with self.assertRaisesRegex(source_tree.SourceTreeError, "Cargo.lock differs"):
            source_tree.describe(self.root, allow_unclean=True)

    def test_untracked_lockfile_rejected(self):
        run_git(self.root, "rm", "-q", "--cached", "crates/Cargo.lock")
        run_git(self.root, "commit", "-q", "-m", "drop lock")
        with self.assertRaisesRegex(source_tree.SourceTreeError, "must be tracked"):
            source_tree.describe(self.root, allow_unclean=True)

    def test_missing_tracked_file_rejected(self):
        (self.root / "run.sh").unlink()
        with self.assertRaisesRegex(source_tree.SourceTreeError, "tracked file missing"):
            source_tree.describe(self.root, allow_unclean=True)

    def test_rename_status_parsing(self):
        run_git(self.root, "mv", "README.md", "RENAMED.md")
        entries = source_tree.status_entries(self.root)
        self.assertEqual(entries, [{"xy": "R ", "path": "RENAMED.md", "original": "README.md"}])

    def test_subdirectory_is_not_a_repo_root(self):
        with self.assertRaisesRegex(source_tree.SourceTreeError, "top level"):
            source_tree.describe(self.root / "crates")

    def test_canonical_json_and_digest(self):
        payload = source_tree.canonical_json({"b": [1, "é"], "a": None})
        self.assertEqual(payload, b'{"a":null,"b":[1,"\\u00e9"]}\n')
        self.assertEqual(source_tree.canonical_sha256([["bin/vz", "a" * 64]]),
                         hashlib.sha256(b'[["bin/vz","' + b"a" * 64 + b'"]]\n').hexdigest())

    def test_cli_describe_and_canonical_sha256(self):
        module = str(Path(source_tree.__file__).resolve())
        out = self.root / "desc.json"
        completed = subprocess.run([sys.executable, module, "describe", "--repo", str(self.root), "--json-out", str(out)],
                                   capture_output=True, text=True, env=GIT_ENV)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertTrue(json.loads(out.read_text())["clean"])
        (self.root / "scratch.txt").write_text("x")
        completed = subprocess.run([sys.executable, module, "describe", "--repo", str(self.root)],
                                   capture_output=True, text=True, env=GIT_ENV)
        self.assertEqual(completed.returncode, 1)
        self.assertIn("fresh checkout required", completed.stderr)
        completed = subprocess.run([sys.executable, module, "describe", "--repo", str(self.root), "--dev-unclean-checkout"],
                                   capture_output=True, text=True, env=GIT_ENV)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertFalse(json.loads(completed.stdout)["clean"])
        self.assertIn("DEVELOPMENT-ONLY", completed.stderr)
        completed = subprocess.run([sys.executable, module, "canonical-sha256"], input='[["p", "q"]]',
                                   capture_output=True, text=True)
        self.assertEqual(completed.stdout.strip(), hashlib.sha256(b'[["p","q"]]\n').hexdigest())


if __name__ == "__main__":
    unittest.main()
